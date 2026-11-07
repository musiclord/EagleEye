"""Official TAIFEX TX institutional positions and settlement-consistent market OI.

No HTTP or database mutations occur in parsers or derived calculations. TXF is
only the institution download form's selector; the product is the full-size TX.
"""
from __future__ import annotations

from calendar import monthrange
from collections import defaultdict
from datetime import date, timedelta
from functools import partial
import csv
import hashlib
import io
import re
from urllib.parse import urlencode

from .connectors import SchemaError, Source, number
from .contracts import Job, make_point

MODEL_VERSION = "taifex-tx-1"
INSTITUTION_URL = "https://www.taifex.com.tw/cht/3/futContractsDateDown"
MARKET_URL = "https://www.taifex.com.tw/cht/3/futDataDown"
TX_MULTIPLIER = 200  # TWD per index point; official full-size TX specification.
SOURCES = (
    Source("taifex_tx_institutions", "期交所 TX 三大法人未平倉歷史", INSTITUTION_URL,
           "僅臺股期貨TX（下載選項TXF）；各到期月份合計，排除MTX/TMF。外資含陸資。"
           "未平倉存量不等於當日交易量、現貨買賣超或匯出入；結算日排除到期契約。"
           "夜盤歸入次一一般交易日，OI約15:00發布，晚於台股收盤。", "daily", 4),
    Source("taifex_tx_market", "期交所 TX 各月份結算價與全市場未平倉", MARKET_URL,
           "僅TX一般盤各單式月份，排除夜盤、價差交易及當日到期契約。"
           "全市場單邊OI每張契約只計一次；名目金額以各月份結算價乘200元及OI加總，非現貨估值。", "daily", 4),
)
DERIVED_SOURCE = Source("taifex_tx_ratios", "TX 法人淨部位相對全市場單邊OI", INSTITUTION_URL,
                        "同日期淨多減淨空口數／全市場單邊OI；有正負號，不是持股占比、現金流或槓桿率。", "daily", 4)
GROUPS = {"外資及陸資": ("foreign", "外資及陸資"), "投信": ("trust", "投信"), "自營商": ("dealer", "自營商")}
SERIES = {
    "tx_market_oi_total": ("TX全市場單邊未平倉口數（排除到期契約）", "contract"),
    "tx_market_oi_notional": ("TX全市場單邊未平倉名目金額", "TWD"),
}
for _group, _label in GROUPS.values():
    for _side, _side_label in (("long", "多方"), ("short", "空方"), ("net", "淨多減淨空")):
        SERIES[f"tx_{_group}_oi_{_side}"] = (f"TX{_label}{_side_label}未平倉口數", "contract")
    SERIES[f"tx_{_group}_oi_net_notional"] = (f"TX{_label}淨未平倉名目金額", "TWD")
    SERIES[f"tx_{_group}_oi_share_pct"] = (f"TX{_label}淨部位／全市場單邊OI", "%")

INSTITUTION_COLUMNS = (
    "日期", "商品名稱", "身份別", "多方交易口數", "多方交易契約金額(千元)",
    "空方交易口數", "空方交易契約金額(千元)", "多空交易口數淨額", "多空交易契約金額淨額(千元)",
    "多方未平倉口數", "多方未平倉契約金額(千元)", "空方未平倉口數",
    "空方未平倉契約金額(千元)", "多空未平倉口數淨額", "多空未平倉契約金額淨額(千元)",
)
MARKET_COLUMNS = ("交易日期", "契約", "到期月份(週別)", "結算價", "未沖銷契約數", "交易時段")


def _rows(raw: bytes, required: tuple[str, ...]) -> list[dict]:
    try:
        text = raw.decode("utf-8-sig")
    except UnicodeDecodeError:
        try:
            text = raw.decode("cp950")
        except UnicodeDecodeError as exc:
            raise SchemaError("TAIFEX CSV is neither UTF-8 nor CP950") from exc
    reader = csv.reader(io.StringIO(text))
    try:
        headers = [x.strip() for x in next(reader)]
    except StopIteration as exc:
        raise SchemaError("Empty TAIFEX download") from exc
    while headers and not headers[-1]:
        headers.pop()
    if len(set(headers)) != len(headers) or not set(required).issubset(headers):
        raise SchemaError("TAIFEX CSV required columns changed or HTML returned")
    result = []
    for row in reader:
        if not row or not any(x.strip() for x in row):
            continue
        if len(row) < len(headers) or any(x.strip() for x in row[len(headers):]):
            raise SchemaError("TAIFEX CSV row width changed")
        result.append(dict(zip(headers, (x.strip() for x in row[:len(headers)]))))
    if not result:
        raise SchemaError("No published TAIFEX rows in requested period")
    return result


def _day(value: str, start: date, end: date) -> str:
    if not re.fullmatch(r"\d{4}/\d{2}/\d{2}", value):
        raise SchemaError("TAIFEX date must be Gregorian YYYY/MM/DD")
    try:
        stamp = date.fromisoformat(value.replace("/", "-"))
    except ValueError as exc:
        raise SchemaError("Invalid TAIFEX calendar date") from exc
    if not start <= stamp <= end:
        raise SchemaError("TAIFEX returned a date outside the requested range")
    return stamp.isoformat()


def _integer(value: str, signed: bool = False) -> int:
    val = number(value)
    if val != val.to_integral_value() or (val < 0 and not signed):
        raise SchemaError("Expected nonnegative integral TAIFEX quantity")
    return int(val)


def _metadata(raw: bytes) -> dict:
    return {
        "method_version": MODEL_VERSION, "raw_sha256": hashlib.sha256(raw).hexdigest(),
        "frequency": "daily", "source_timezone": "Asia/Taipei", "product": "TX",
        "contract_multiplier_twd_per_point": TX_MULTIPLIER,
        "market_calendar": "TAIFEX attributed trading date; overnight belongs to following regular session",
        "observation_time_local": "regular session end 13:45; expiring month ends 13:30",
        "publication_schedule_note": "Institutional OI approximately 15:00 Asia/Taipei; not verified historical release timestamps",
        "publication_time_known": False, "historical_vintage_available": False,
        "same_day_tw_close_available": False,
        "alignment_rule": "strictly_previous_available_observation_date_for_TW_close_prediction",
        "retrospective_same_date_allowed": True,
        "missing_rule": "No zero-fill or carry-forward on holidays or missing dates",
    }


def parse_institutions(raw: bytes, start: date, end: date) -> list[dict]:
    rows = _rows(raw, INSTITUTION_COLUMNS)
    grouped = defaultdict(dict)
    for row in rows:
        stamp = _day(row["日期"], start, end)
        if row["商品名稱"] != "臺股期貨":
            raise SchemaError("Requested full-size TX, but another product was returned")
        identity = row["身份別"]
        if identity not in GROUPS:
            raise SchemaError("TAIFEX institutional identity changed")
        if identity in grouped[stamp]:
            raise SchemaError("Duplicate institutional date and identity")
        values = {key: _integer(row[key], signed=key.startswith("多空")) for key in INSTITUTION_COLUMNS[3:]}
        for category in ("交易", "未平倉"):
            long, short = values[f"多方{category}口數"], values[f"空方{category}口數"]
            if long - short != values[f"多空{category}口數淨額"]:
                raise SchemaError("Institutional long - short does not equal net contracts")
            amount_diff = values[f"多方{category}契約金額(千元)"] - values[f"空方{category}契約金額(千元)"]
            # Independently rounded thousands of TWD may differ by one thousand.
            if abs(amount_diff - values[f"多空{category}契約金額淨額(千元)"]) > 1:
                raise SchemaError("Institutional notional reconciliation exceeds rounding tolerance")
        grouped[stamp][identity] = values
    output, base = [], _metadata(raw)
    for stamp, identities in sorted(grouped.items()):
        if set(identities) != set(GROUPS):
            raise SchemaError("Each TAIFEX date must contain all three institutional categories")
        for identity, values in identities.items():
            group, _ = GROUPS[identity]
            meta = {**base, "identity": identity, "form_selector": "TXF",
                    "measure": "end_of_regular_session_open_interest_stock",
                    "expiry_rule": "Official institutional OI excludes contracts expiring on this date",
                    "interpretation": "Aggregated categories include hedges/arbitrage; not identities or directional forecasts",
                    "trade_volume_contracts": {"long": values["多方交易口數"], "short": values["空方交易口數"], "net": values["多空交易口數淨額"]},
                    "official_notional_thousand_twd": {"long": values["多方未平倉契約金額(千元)"], "short": values["空方未平倉契約金額(千元)"], "net": values["多空未平倉契約金額淨額(千元)"]}}
            for side, col in (("long", "多方未平倉口數"), ("short", "空方未平倉口數"), ("net", "多空未平倉口數淨額")):
                output.append(make_point(SERIES, f"tx_{group}_oi_{side}", stamp, values[col], metadata={**meta, "source_unit": "contracts"}))
            output.append(make_point(SERIES, f"tx_{group}_oi_net_notional", stamp,
                                     values["多空未平倉契約金額淨額(千元)"] * 1000,
                                     metadata={**meta, "source_unit": "thousand TWD", "conversion": "multiply by 1000",
                                               "formula_note": "Official sum across maturities of settlement * 200 * net OI; not spot * OI"}))
    return output


def parse_market(raw: bytes, start: date, end: date) -> list[dict]:
    rows = _rows(raw, MARKET_COLUMNS)
    grouped, expired = defaultdict(dict), defaultdict(list)
    for row in rows:
        stamp = _day(row["交易日期"], start, end)
        if row["契約"] != "TX":
            raise SchemaError("Requested TX but market download contains a different contract")
        if row["交易時段"] not in {"一般", "盤後"}:
            raise SchemaError("Unknown TAIFEX market session")
        maturity = row["到期月份(週別)"]
        if re.fullmatch(r"\d{6}/\d{6}", maturity) or row["交易時段"] == "盤後":
            continue  # Neither calendar-spread volume nor night volume is an additional OI stock.
        if not re.fullmatch(r"\d{6}", maturity) or not 1 <= int(maturity[4:]) <= 12:
            raise SchemaError("Unexpected TX maturity format")
        if maturity in grouped[stamp]:
            raise SchemaError("Duplicate TX regular-session maturity")
        oi = _integer(row["未沖銷契約數"])
        settlement = number(row["結算價"])
        if settlement < 0 or settlement != settlement.to_integral_value():
            raise SchemaError("Invalid TX settlement price")
        if settlement == 0:
            # TAIFEX daily file retains the expiring position count with settlement=0.
            # Institutional report removes it. Do not guess holidays from third Wednesdays.
            if maturity != stamp[:7].replace("-", "") or int(stamp[8:]) < 15:
                raise SchemaError("Zero TX settlement outside identifiable current-month expiry")
            expired[stamp].append({"maturity": maturity, "contracts": oi, "settlement": 0})
        grouped[stamp][maturity] = (oi, int(settlement))
    output, base = [], _metadata(raw)
    for stamp, contracts in sorted(grouped.items()):
        if len(contracts) != 6:
            raise SchemaError("TX must have six listed monthly maturities; incomplete market response")
        omitted = expired[stamp]
        if len(omitted) > 1:
            raise SchemaError("Multiple expiries on one TX session")
        active = {k: v for k, v in contracts.items() if v[1] > 0}
        total = sum(v[0] for v in active.values())
        if total <= 0:
            raise SchemaError("Nonpositive TX market denominator")
        nominal = sum(oi * settlement * TX_MULTIPLIER for oi, settlement in active.values())
        meta = {**base, "measure": "one_sided_market_open_interest",
                "maturities": [{"maturity": m, "contracts": oi, "settlement": s} for m, (oi, s) in sorted(active.items())],
                "excluded_expiring_contracts": omitted,
                "expiry_rule": "Exclude current-month regular row with settlement=0; keep exclusion evidence, no calendar-day guessing",
                "notional_is_margin_or_capital_flow": False, "input_raw_sha256": [base["raw_sha256"]]}
        output.append(make_point(SERIES, "tx_market_oi_total", stamp, total, status="derived",
                                 metadata={**meta, "formula": "sum(TX regular-session single-month OI excluding expired contracts)"}))
        output.append(make_point(SERIES, "tx_market_oi_notional", stamp, nominal, status="derived",
                                 metadata={**meta, "formula": "sum(TX maturity OI * own daily settlement * 200 TWD)"}))
    return output


def derive(store) -> list[tuple[Source, list[dict]]]:
    wanted = {"tx_market_oi_total"} | {f"tx_{g}_oi_{s}" for g, _ in GROUPS.values() for s in ("long", "short", "net")}
    data = defaultdict(dict)
    for p in store.observations():
        sid, stamp = p["series_id"], p["observation_date"]
        if sid not in wanted:
            continue
        if sid in data[stamp]:
            raise SchemaError("Multiple sources for a TX ratio input")
        data[stamp][sid] = p
    output = []
    for stamp, row in sorted(data.items()):
        if set(row) != wanted:
            continue  # Missing denominator or categories remain missing, never filled.
        denominator = row["tx_market_oi_total"]["value"]
        if denominator <= 0:
            raise SchemaError("Nonpositive TX ratio denominator")
        for side in ("long", "short"):
            if sum(row[f"tx_{g}_oi_{side}"]["value"] for g, _ in GROUPS.values()) > denominator:
                raise SchemaError("Institutional OI exceeds market OI; scopes or expiry dates disagree")
        for group, _ in GROUPS.values():
            sid = f"tx_{group}_oi_net"
            net = row[sid]["value"]
            if row[f"tx_{group}_oi_long"]["value"] - row[f"tx_{group}_oi_short"]["value"] != net:
                raise SchemaError("Stored TX positions do not reconcile")
            inputs = [row[sid], row["tx_market_oi_total"]]
            meta = {"method_version": MODEL_VERSION, "frequency": "daily", "source_timezone": "Asia/Taipei",
                    "formula": f"{sid} / tx_market_oi_total * 100",
                    "same_day_tw_close_available": False, "publication_time_known": False,
                    "historical_vintage_available": False,
                    "alignment_rule": "strictly_previous_available_observation_date_for_TW_close_prediction",
                    "input_raw_sha256": sorted({p.get("raw_sha256") or p.get("metadata", {}).get("raw_sha256") for p in inputs} - {None}),
                    "inputs": [{k: p.get(k) for k in ("series_id", "observation_date", "value", "source_id", "raw_sha256", "retrieved_at")} for p in inputs],
                    "interpretation": "Signed net positions / one-sided market OI; not ownership share, invested capital, leverage or directional prediction"}
            output.append(make_point(SERIES, f"tx_{group}_oi_share_pct", stamp, net / denominator * 100,
                                     status="derived", metadata=meta))
    return [(DERIVED_SOURCE, output)] if output else []


def jobs(start: date, end: date) -> list[Job]:
    if start > end:
        raise ValueError("start must not follow end")
    output = []
    headers = {"Content-Type": "application/x-www-form-urlencoded", "Referer": "https://www.taifex.com.tw/cht/3/futContractsDateView"}
    for year in range(start.year, end.year + 1):
        lower, upper = max(start, date(year, 1, 1)), min(end, date(year, 12, 31))
        output.append(Job(key=f"taifex-tx-institutions:{lower}:{upper}", source_id=SOURCES[0].id,
                          url=INSTITUTION_URL, parser=partial(parse_institutions, start=lower, end=upper),
                          parser_version=MODEL_VERSION, refresh_after_hours=24 if (end-upper).days < 60 else 24*30,
                          headers=headers, method="POST", body=urlencode({"queryStartDate": lower.strftime("%Y/%m/%d"),
                          "queryEndDate": upper.strftime("%Y/%m/%d"), "commodityId": "TXF"}).encode("ascii")))
    lower = start
    while lower <= end:
        upper = min(end, date(lower.year, lower.month, monthrange(lower.year, lower.month)[1]))
        output.append(Job(key=f"taifex-tx-market:{lower}:{upper}", source_id=SOURCES[1].id,
                          url=MARKET_URL, parser=partial(parse_market, start=lower, end=upper),
                          parser_version=MODEL_VERSION, refresh_after_hours=24 if (end-upper).days < 60 else 24*30,
                          optional=True, method="POST", headers={**headers, "Referer": "https://www.taifex.com.tw/cht/3/dlFutDailyMarketView"},
                          body=urlencode({"down_type": "1", "commodity_id": "TX", "commodity_id2": "",
                          "queryStartDate": lower.strftime("%Y/%m/%d"), "queryEndDate": upper.strftime("%Y/%m/%d")}).encode("ascii")))
        lower = upper + timedelta(days=1)
    return output
