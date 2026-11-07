"""Strict, dated TWSE adapters. HTTP retrieval is deliberately limited to this allowlist."""
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
import json
import math
import os
import urllib.request

@dataclass(frozen=True)
class Source:
    id: str
    label: str
    url: str
    coverage_note: str
    frequency: str = 'daily'
    freshness_days: int = 4
    # False: licensed data kept for local analysis only; web_export leaves it out of public snapshots.
    publish: bool = True
    # Set: the public snapshot keeps only this many years of observations; the local DB keeps everything.
    public_years: int | None = None
    # Attribution the licence requires wherever the data is shown; "{year}" becomes the latest observation year.
    attribution: str | None = None
    license_url: str | None = None
    # Whole-history archives: a changed capture is not itself a changed observation.
    content_revisions_only: bool = False


def twse_website_enabled():
    """TWSE website endpoints (www.twse.com.tw) forbid automated download without consent. They stay
    available for local-only analysis unless EAGLEEYE_TWSE_WEBSITE is off; OpenAPI sources are unaffected."""
    return os.getenv("EAGLEEYE_TWSE_WEBSITE", "on").strip().lower() not in {"off", "0", "false", "no"}


TWSE_OPENAPI_LICENSE = "https://data.gov.tw/license"
TWSE_LOCAL_ONLY_NOTE = "證交所網站端點禁止未經同意以程式自動下載，這裡的資料只供本機分析，不進公開快照；公開版改用 OpenAPI 來源。"

SOURCES = (
    Source("twse_oa_market", "TWSE OpenAPI 集中市場每日市場成交資訊",
           "https://openapi.twse.com.tw/v1/exchangeReport/FMTQIK",
           "官方 OpenAPI 只提供當月逐日資料；歷史由每日擷取逐步累積，不回補。成交金額涵蓋有價證券，不能直接當成上市普通股母體。",
           attribution="臺灣證券交易所 {year}〈集中市場每日市場成交資訊〉（OpenAPI v1）",
           license_url=TWSE_OPENAPI_LICENSE),
    Source("twse_institutions", "TWSE 三大法人買賣金額（網站端點，僅供本機）",
           "https://www.twse.com.tw/fund/BFI82U?response=json",
           "官方當日有價證券統計；外資賣超不等於淨匯出，避險與自行買賣分開。" + TWSE_LOCAL_ONLY_NOTE,
           publish=False),
    Source("twse_margin", "TWSE 信用交易統計（網站端點，僅供本機）",
           "https://www.twse.com.tw/exchangeReport/MI_MARGN?response=json",
           "融資金額由仟元轉新台幣元；當日餘額供參考，官方建議以次日報表的前日餘額修訂值為準。未涵蓋全部股票質借、信貸與場外槓桿。" + TWSE_LOCAL_ONLY_NOTE,
           publish=False),
)
SOURCE_MAP = {s.id: s for s in SOURCES}
SERIES = {
    "taiex": ("發行量加權股價指數", "point"),
    "market_turnover": ("集中市場成交金額", "TWD"),
    "foreign_net": ("外資及陸資買賣超（不含外資自營商）", "TWD"),
    "institutional_net": ("三大法人買賣超", "TWD"),
    "trust_net": ("投信買賣超", "TWD"),
    "dealer_hedge_net": ("自營商避險買賣超", "TWD"),
    "dealer_prop_net": ("自營商自行買賣買賣超", "TWD"),
    "margin_balance": ("集中市場融資金額餘額", "TWD"),
}

class SchemaError(ValueError):
    pass


class ReportNotAvailable(SchemaError):
    """The official endpoint answered, but has no report for the requested session yet.

    This is not evidence of a format change. The engine retries it while the
    session is recent, and escalates it for review only after a waiting window.
    """
    def __init__(self, message, session_date=None):
        super().__init__(message)
        self.session_date = session_date

def number(value):
    if value is None or isinstance(value, bool):
        raise SchemaError("Missing or invalid numeric value")
    s = str(value).replace(",", "").strip()
    if not s or s in {"--", "-", "N/A", "null"}:
        raise SchemaError("Missing numeric value; never impute zero")
    try:
        n = Decimal(s)
    except InvalidOperation as exc:
        raise SchemaError("Non-numeric value") from exc
    if not n.is_finite():
        raise SchemaError("Non-finite numeric value")
    return n

def observation_date(value):
    s = str(value).replace("/", "").replace("-", "")
    try:
        if len(s) == 7 and s.isdigit():
            return date(int(s[:3]) + 1911, int(s[3:5]), int(s[5:])).isoformat()
        if len(s) == 8 and s.isdigit():
            return date(int(s[:4]), int(s[4:6]), int(s[6:])).isoformat()
    except ValueError:
        pass
    raise SchemaError("Missing or invalid observation date; retrieval date is not a substitute")

def point(series_id, observed, value):
    label, unit = SERIES[series_id]
    if not math.isfinite(float(value)):
        raise SchemaError("Numeric value exceeds supported range")
    return {"series_id": series_id, "label": label, "unit": unit,
            "observation_date": observed, "value": float(value)}

def require_fields(fields, expected):
    if fields != expected:
        raise SchemaError("Upstream fields changed; reject rather than guess column positions")

def parse_market(data, today=None):
    """OpenAPI FMTQIK: the current month only, one row per session, dated by the official Date field."""
    if not isinstance(data, list) or not data:
        raise SchemaError("Empty or malformed FMTQIK response")
    today = today or datetime.now(timezone(timedelta(hours=8))).date()
    result, seen = [], set()
    for row in data:
        if not isinstance(row, dict) or not {"Date", "TAIEX", "TradeValue"}.issubset(row):
            raise SchemaError("FMTQIK fields changed")
        observed = observation_date(row["Date"])
        if observed in seen:
            raise SchemaError("Duplicate market date")
        if observed > today.isoformat():
            raise SchemaError("FMTQIK session date is after today; retrieval date is not a substitute")
        if seen and observed[:7] != min(seen)[:7]:
            raise SchemaError("FMTQIK spans more than one month; the OpenAPI publishes the current month only")
        seen.add(observed)
        idx, turnover = number(row["TAIEX"]), number(row["TradeValue"])
        if idx <= 0 or turnover < 0:
            raise SchemaError("Invalid index or turnover")
        result.extend([point("taiex", observed, idx), point("market_turnover", observed, turnover)])
    return result

def parse_institutions(data):
    if not isinstance(data, dict) or data.get("stat") != "OK":
        raise SchemaError("TWSE institutional report is not OK")
    observed = observation_date(data.get("date"))
    require_fields(data.get("fields"), ["單位名稱", "買進金額", "賣出金額", "買賣差額"])
    mapping = {"外資及陸資(不含外資自營商)": "foreign_net", "投信": "trust_net",
               "自營商(避險)": "dealer_hedge_net", "自營商(自行買賣)": "dealer_prop_net",
               "合計": "institutional_net"}
    result, seen = [], set()
    for row in data.get("data", []):
        if not isinstance(row, list) or len(row) != 4:
            raise SchemaError("Institutional row shape changed")
        name = row[0].strip()
        if name not in mapping:
            continue
        if name in seen:
            raise SchemaError("Duplicate institution")
        seen.add(name)
        buy, sell, net = (number(v) for v in row[1:])
        if min(buy, sell) < 0 or buy - sell != net:
            raise SchemaError("Institutional buy minus sell does not equal net")
        result.append(point(mapping[name], observed, net))
    if len(result) != len(mapping):
        raise SchemaError("Required institutional category missing")
    net_by_id = {p["series_id"]: Decimal(str(p["value"])) for p in result}
    if sum(net_by_id[k] for k in ("foreign_net", "trust_net", "dealer_hedge_net", "dealer_prop_net")) != net_by_id["institutional_net"]:
        raise SchemaError("Institutional categories do not reconcile to total")
    return result

def parse_margin(data, *, allow_reported_residual=False):
    if not isinstance(data, dict) or data.get("stat") != "OK":
        raise SchemaError("TWSE margin report is not OK")
    observed = observation_date(data.get("date"))
    target = []
    for table in data.get("tables", []):
        for row in table.get("data", []):
            if row and row[0] == "融資金額(仟元)":
                require_fields(table.get("fields"), ["項目", "買進", "賣出", "現金(券)償還", "前日餘額", "今日餘額"])
                target.append(row)
    if len(target) != 1 or len(target[0]) != 6:
        raise SchemaError("Exactly one dated margin amount summary is required")
    buy, sell, repayment, previous, current = (number(v) for v in target[0][1:])
    if min(buy, sell, repayment, previous, current) < 0:
        raise SchemaError("Negative margin amount")
    expected = previous + buy - sell - repayment
    residual = current - expected
    if residual and not allow_reported_residual:
        raise SchemaError("Margin balance does not reconcile")
    result = point("margin_balance", observed, current * 1000)
    result["metadata"] = {
        "reconciliation": "exact" if not residual else "reported_value_unreconciled",
        "reconciliation_residual_twd": float(residual * 1000),
        "reconstructed_balance_twd": float(expected * 1000),
        "quality_flag": None if not residual else "unreconciled_balance",
        "rounding_possible_not_verified": bool(residual and abs(residual) <= 2),
        "reported_components_thousand_twd": {"buy": float(buy), "sell": float(sell),
            "cash_repayment": float(repayment), "previous": float(previous), "current": float(current)},
        "reconciliation_note": "保留官方今日餘額原值；非零殘差未判定原因，不把驗算不符當成通過。官方指出當日初值與次日調帳可能有差異。",
    }
    return [result]

PARSERS = {"twse_oa_market": parse_market, "twse_institutions": parse_institutions, "twse_margin": parse_margin}


def refresh_sources():
    """Sources the manual /api/refresh may contact: website endpoints only while they are enabled."""
    return tuple(s for s in SOURCES if s.publish or twse_website_enabled())

def fetch(source):
    if source.id not in SOURCE_MAP or source.url != SOURCE_MAP[source.id].url:
        raise ValueError("Source is outside the allowlist")
    request = urllib.request.Request(source.url, headers={"User-Agent": "EagleEyePOC/0.1 local-economic-research",
                                                         "Accept": "application/json"})
    with urllib.request.urlopen(request, timeout=15) as response:
        raw = response.read(5_000_001)
        if len(raw) > 5_000_000:
            raise SchemaError("Response exceeds 5 MB limit")
    return raw

def parse(source_id, raw):
    try:
        data = json.loads(raw)
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise SchemaError("Response is not valid JSON") from exc
    return PARSERS[source_id](data)
