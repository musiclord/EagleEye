"""Japan Ministry of Finance: weekly cross-border securities transactions and JGB yields.

Both files are Shift_JIS (CP932) CSV. Parsers perform no network or DB writes.
"""
from __future__ import annotations

from datetime import date
import csv
import io
import re
import unicodedata

from .connectors import SchemaError, Source, number
from .contracts import Job, make_point

WEEKLY_URL = ("https://www.mof.go.jp/policy/international_policy/reference/"
              "itn_transactions_in_securities/week.csv")
JGB_ALL_URL = "https://www.mof.go.jp/jgbs/reference/interest_rate/data/jgbcm_all.csv"
# The current-month file lives one directory above jgbcm_all.csv (…/data/jgbcm.csv is 404).
JGB_CURRENT_URL = "https://www.mof.go.jp/jgbs/reference/interest_rate/jgbcm.csv"
MODEL_VERSION = "japan-mof-1"
CLASSIFICATION_BREAK = date(2014, 1, 1)

SOURCES = (
    Source("mof_jp_weekly_securities", "日本財務省 對外及對內證券投資（週，指定報告機關）", WEEKLY_URL,
           "單位億日圓；每期為週日至週六，觀測日用期末日並保留起訖。淨額=取得−處分，正值為淨買入（檔案備註③，"
           "解析器逐列驗算）。2014年1月起投資信託（開放型契約型）由中長期債改列「股票・投資基金持分」，"
           "前後不可直接比較。只分居住者／非居住者，不分投資人所在國家或地區；指定報告機關口徑，非國際收支全體。",
           "weekly", 14, public_years=10),
    Source("mof_jgb_yields", "日本財務省 國債金利情報（2年、10年）", JGB_ALL_URL,
           "財務省依店頭市場氣配值計算的固定年限（曲線）利率，年率百分比；非實際成交殖利率。"
           "日期由和曆轉西元；「-」代表當時未計算，略過不補。歷史檔每月更新，當月另檔。",
           "daily", 6, public_years=10),
)

WEEKLY_COLUMNS = {
    # column index in the CSV -> (series id, label)
    3: ("jp_residents_net_foreign_equity_jpy_100m", "日本投資人淨買外國股票・投資基金持分"),
    6: ("jp_residents_net_foreign_long_debt_jpy_100m", "日本投資人淨買外國中長期債"),
    14: ("jp_nonresidents_net_japan_equity_jpy_100m", "外國投資人淨買日本股票・投資基金持分"),
    17: ("jp_nonresidents_net_japan_long_debt_jpy_100m", "外國投資人淨買日本中長期債"),
    21: ("jp_nonresidents_net_japan_short_debt_jpy_100m", "外國投資人淨買日本短期債"),
}
JGB_COLUMNS = {"2年": "jgb_2y_pct", "10年": "jgb_10y_pct"}
SERIES = {sid: (label, "億JPY") for sid, label in WEEKLY_COLUMNS.values()}
SERIES.update({"jgb_2y_pct": ("日本2年期國債利率（財務省）", "%"),
               "jgb_10y_pct": ("日本10年期國債利率（財務省）", "%")})
ERA_BASE = {"M": 1867, "T": 1911, "S": 1925, "H": 1988, "R": 2018}


def _decode(raw: bytes) -> str:
    if raw.removeprefix(b"\xef\xbb\xbf").lstrip()[:1] == b"<":  # MOF error pages are UTF-8 HTML
        raise SchemaError("Expected MOF CSV, not an HTML page")
    try:
        return raw.decode("cp932")
    except UnicodeDecodeError as exc:
        raise SchemaError("Expected Shift_JIS (CP932) CSV") from exc


def _cell_number(text: str):
    """None for an empty cell; strict number otherwise (never zero-filled)."""
    s = unicodedata.normalize("NFKC", text).strip()
    if s == "":
        return None
    return number(s)


_PERIOD = re.compile(r"(\d{4})\.(\d{1,2})\.(\d{1,2})[~〜](?:(\d{4})\.)?(\d{1,2})\.(\d{1,2})")


def week_period(text: str) -> tuple[date, date]:
    """'2005．12．25～ 12．31' or '2013．12．29～2014．1．4' -> (start, end)."""
    s = re.sub(r"\s+", "", unicodedata.normalize("NFKC", text)).replace("～", "~")
    m = _PERIOD.fullmatch(s)
    if not m:
        raise SchemaError("MOF weekly period format changed: " + text[:30])
    y1, m1, d1, y2, m2, d2 = m.groups()
    try:
        first = date(int(y1), int(m1), int(d1))
        end_year = int(y2) if y2 else int(y1) + (1 if int(m2) < int(m1) else 0)
        last = date(end_year, int(m2), int(d2))
    except ValueError as exc:
        raise SchemaError("Invalid MOF weekly period date") from exc
    if not 0 <= (last - first).days <= 7:
        raise SchemaError("MOF weekly period is not one week")
    return first, last


def parse_weekly_securities(raw: bytes) -> list[dict]:
    rows = list(csv.reader(io.StringIO(_decode(raw))))
    header = next((i for i, r in enumerate(rows) if r and r[0].startswith("期間")), None)
    if header is None or len(rows) < header + 4:
        raise SchemaError("MOF weekly header not found")
    unit_text = " ".join(" ".join(r) for r in rows[:header])
    if "億円" not in unit_text:
        raise SchemaError("MOF weekly unit changed")
    labels = [unicodedata.normalize("NFKC", c).strip() for c in rows[header + 2]]
    expected = ["", "取得", "処分", "ネット", "取得", "処分", "ネット", "ネット", "取得", "処分", "ネット", "ネット",
                "取得", "処分", "ネット", "取得", "処分", "ネット", "ネット", "取得", "処分", "ネット", "ネット"]
    if labels[:23] != expected:
        raise SchemaError("MOF weekly column layout changed")
    sections = " ".join(rows[header - 3]) if header >= 3 else ""
    if "対外証券投資" not in sections or "対内証券投資" not in sections:
        raise SchemaError("MOF weekly section headings changed")
    output, seen = [], set()
    for row in rows[header + 4:]:
        if not row or not row[0].strip():
            if any(c.strip() for c in row[1:]):
                raise SchemaError("MOF weekly data row without a period")
            continue
        if not re.match(r"\s*\d{4}", unicodedata.normalize("NFKC", row[0])):
            break  # footnotes
        first, last = week_period(row[0])
        if last in seen:
            raise SchemaError("Duplicate MOF weekly period")
        seen.add(last)
        if len(row) < 23:
            raise SchemaError("MOF weekly row is too short")
        values = [None] + [_cell_number(c) for c in row[1:23]]
        regime = "2014_onward_investment_funds_in_equity" if first >= CLASSIFICATION_BREAK else \
            "pre_2014_open_end_contractual_funds_in_long_term_debt"
        if first < CLASSIFICATION_BREAK <= last:
            regime = "straddles_2014_reclassification"
        for col, (sid, _) in WEEKLY_COLUMNS.items():
            net, acquired, disposed = values[col], values[col - 2], values[col - 1]
            if net is None:
                continue
            # Sign check: the file defines net = acquisition - disposition (plus = net purchase).
            # Short-term debt sits at columns 19-21, so it shares the col-2/col-1 offsets.
            if acquired is None or disposed is None or abs((acquired - disposed) - net) > 1:
                raise SchemaError("MOF weekly net does not equal acquisition minus disposition")
            output.append(make_point(SERIES, sid, last.isoformat(), net, metadata={
                "frequency": "weekly", "period_start": first.isoformat(), "period_end": last.isoformat(),
                "observation_date_rule": "period_end", "source_unit": "100 million yen",
                "acquisition_jpy_100m": float(acquired), "disposition_jpy_100m": float(disposed),
                # Long explanations live in the source coverage note; rows keep short codes.
                "sign_convention": "net=acquisition-disposition; positive=net purchase; checked per row",
                "investor": "non-residents" if col >= 12 else "residents of Japan",
                "investor_country_breakdown": False,
                "classification_regime": regime,
                "reporting_basis": "designated_major_investors_weekly",
                "publication_time_known": False,
            }))
    if not output:
        raise SchemaError("MOF weekly file contains no data rows")
    return sorted(output, key=lambda p: (p["observation_date"], p["series_id"]))


def wareki_date(text: str) -> date:
    """'R8.11.2' / 'H31.4.30' / 'S49.9.24' -> Gregorian date."""
    m = re.fullmatch(r"([MTSHR])(\d{1,2})\.(\d{1,2})\.(\d{1,2})", unicodedata.normalize("NFKC", text).strip())
    if not m:
        raise SchemaError("JGB date is not a Japanese era date: " + text[:20])
    era, year, month, day = m.group(1), int(m.group(2)), int(m.group(3)), int(m.group(4))
    if year < 1:
        raise SchemaError("Invalid era year")
    try:
        return date(ERA_BASE[era] + year, month, day)
    except ValueError as exc:
        raise SchemaError("Invalid JGB calendar date") from exc


def parse_jgb_yields(raw: bytes) -> list[dict]:
    rows = list(csv.reader(io.StringIO(_decode(raw))))
    if len(rows) < 3 or not rows[0] or "国債金利情報" not in rows[0][0] or "%" not in "".join(rows[0]):
        raise SchemaError("JGB file title or unit changed")
    header = [c.strip() for c in rows[1]]
    if header[:1] != ["基準日"] or any(name not in header for name in JGB_COLUMNS):
        raise SchemaError("JGB header changed")
    index = {name: header.index(name) for name in JGB_COLUMNS}
    meta = {"frequency": "daily", "source_timezone": "Asia/Tokyo", "source_unit": "percent per annum",
            "measure": "constant_maturity_yield_from_otc_quotes", "annualized": True,
            "calendar": "wareki_to_gregorian",
            "publication_time_known": False, "missing_rule": "'-' omitted; no fill",
            "alignment_rule": "strictly_previous_available_observation_date"}
    output, seen = [], set()
    for row in rows[2:]:
        if not row or not row[0].strip():
            continue
        if not re.match(r"[MTSHR]\d", row[0].strip()):
            continue  # trailing notes such as the cache-clearing advice
        day = wareki_date(row[0])
        if day in seen:
            raise SchemaError("Duplicate JGB date")
        seen.add(day)
        for name, sid in JGB_COLUMNS.items():
            cell = row[index[name]].strip() if index[name] < len(row) else ""
            if cell in ("-", ""):
                continue
            output.append(make_point(SERIES, sid, day.isoformat(), number(cell),
                                     metadata={**meta, "tenor": name.replace("年", "Y")}))
    if not output:
        raise SchemaError("JGB file contains no yields")
    return sorted(output, key=lambda p: (p["observation_date"], p["series_id"]))


def jobs(start: date, end: date) -> list[Job]:
    if start > end:
        raise ValueError("start must not follow end")
    return [
        Job(key="mof-jp:weekly-securities", source_id="mof_jp_weekly_securities", url=WEEKLY_URL,
            parser=parse_weekly_securities, parser_version=MODEL_VERSION, refresh_after_hours=24,
            max_bytes=3_000_000),
        Job(key="mof-jp:jgb:all", source_id="mof_jgb_yields", url=JGB_ALL_URL, parser=parse_jgb_yields,
            parser_version=MODEL_VERSION, refresh_after_hours=24 * 7, max_bytes=10_000_000),
        Job(key="mof-jp:jgb:current", source_id="mof_jgb_yields", url=JGB_CURRENT_URL, parser=parse_jgb_yields,
            parser_version=MODEL_VERSION, refresh_after_hours=12, max_bytes=1_000_000),
    ]
