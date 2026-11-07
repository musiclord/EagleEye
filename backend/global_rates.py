"""Daily official global rates/JPY history. Parsers perform no network or DB writes."""
from __future__ import annotations

from calendar import monthrange
from datetime import date
from functools import partial
import hashlib
import json
import re
from urllib.parse import urlencode
import xml.etree.ElementTree as ET

from .connectors import SchemaError, Source, number
from .contracts import Job, make_point

TREASURY_BASE = "https://home.treasury.gov/resource-center/data-chart-center/interest-rates/pages/xml"
BOJ_BASE = "https://www.stat-search.boj.or.jp/api/v1/getDataCode"
MODEL_VERSION = "global-rates-1"
SOURCES = (
    Source("us_treasury_yields", "美國財政部每日公債平價殖利率", TREASURY_BASE,
           "官方2年／10年年率；美東約15:30報價建構，非票息、零息利率或投資報酬。"
           "相對台股使用嚴格前一可得日期；API updated不是每筆歷史首次發布時間。",
           "daily", 6),
    Source("boj_overnight", "日本銀行無擔保隔夜拆款成交加權平均利率",
           BOJ_BASE + "?format=json&lang=en&db=FM01&code=STRDCLUCON",
           "實際市場成交加權平均年率，不是央行政策目標。負利率保留；休市／未發布空值不補零。"
           "時序資料庫有更新落差，不假設觀察日盤中即可取得。", "daily", 6),
    Source("boj_usd_jpy", "日本銀行東京17:00美元／日圓中間價",
           BOJ_BASE + "?format=json&lang=en&db=FM08&code=FXERD04",
           "JPY per USD，增加代表日圓貶值。東京17:00晚於台股收盤；非可成交買賣價或UTC收盤。",
           "daily", 6),
)
SERIES = {
    "us_treasury_2y_pct": ("美國2年期公債平價殖利率", "%"),
    "us_treasury_10y_pct": ("美國10年期公債平價殖利率", "%"),
    "us_yield_curve_10y_2y_bp": ("美國公債10年減2年殖利率差", "bp"),
    "boj_overnight_call_pct": ("日本無擔保隔夜拆款實際平均年率", "%"),
    "usd_jpy": ("美元／日圓東京17:00中間價", "JPY/USD"),
}
BOJ_SPECS = {
    "STRDCLUCON": {
        "series_id": "boj_overnight_call_pct", "db": "FM01",
        "unit": "percent per annum", "measure": "realized_volume_weighted_average",
    },
    "FXERD04": {
        "series_id": "usd_jpy", "db": "FM08",
        "unit": "Yen per U.S. Dollar", "measure": "bid_offer_midpoint_at_17_00",
    },
}
_NS = {
    "a": "http://www.w3.org/2005/Atom",
    "m": "http://schemas.microsoft.com/ado/2007/08/dataservices/metadata",
    "d": "http://schemas.microsoft.com/ado/2007/08/dataservices",
}
FIELDS = {"us_treasury_2y_pct": "BC_2YEAR", "us_treasury_10y_pct": "BC_10YEAR"}


def _calendar_date(value) -> date:
    s = str(value)
    if not re.fullmatch(r"\d{8}", s):
        raise SchemaError("Expected Gregorian YYYYMMDD")
    try:
        return date(int(s[:4]), int(s[4:6]), int(s[6:8]))
    except ValueError as exc:
        raise SchemaError("Invalid observation date") from exc


def _history_start(start: date, end: date) -> date:
    """Minimum three calendar years, including a valid leap-year boundary."""
    earlier = date(end.year - 3, end.month,
                   min(end.day, monthrange(end.year - 3, end.month)[1]))
    return min(start, earlier)


def parse_treasury(raw: bytes, start: date, end: date, year: int) -> list[dict]:
    if b"<!DOCTYPE" in raw.upper() or b"<!ENTITY" in raw.upper():
        raise SchemaError("Treasury XML must not contain external declarations")
    try:
        root = ET.fromstring(raw)
    except ET.ParseError as exc:
        raise SchemaError("Expected Treasury Atom XML") from exc
    if root.tag != "{" + _NS["a"] + "}feed":
        raise SchemaError("Treasury feed envelope changed")
    if root.find("a:title", _NS) is None or root.find("a:title", _NS).text != "DailyTreasuryYieldCurveRateData":
        raise SchemaError("Wrong Treasury feed type")
    if any(link.get("rel") == "next" for link in root.findall("a:link", _NS)):
        raise SchemaError("Unexpected Treasury pagination; refusing truncated year")
    entries = root.findall("a:entry", _NS)
    if not entries:
        raise SchemaError("Treasury annual feed contains no entries")
    raw_hash = hashlib.sha256(raw).hexdigest()
    feed_updated = root.findtext("a:updated", namespaces=_NS)
    daily = {}
    for entry in entries:
        props = entry.find("a:content/m:properties", _NS)
        if props is None:
            raise SchemaError("Treasury properties missing")
        stamp = props.findtext("d:NEW_DATE", namespaces=_NS) or ""
        if not re.fullmatch(r"\d{4}-\d{2}-\d{2}T00:00:00", stamp):
            raise SchemaError("Treasury NEW_DATE format changed")
        try:
            d = date.fromisoformat(stamp[:10])
        except ValueError as exc:
            raise SchemaError("Invalid Treasury observation date") from exc
        if d.year != year:
            raise SchemaError("Treasury annual response does not match requested year")
        if not start <= d <= end:
            continue
        row = {}
        missing = []
        for sid, field in FIELDS.items():
            node = props.find("d:" + field, _NS)
            if node is None:
                raise SchemaError("Required Treasury maturity field missing: " + field)
            if node.get("{" + _NS["m"] + "}null") == "true":
                if node.text and node.text.strip():
                    raise SchemaError("Treasury field is both null and populated")
                missing.append(sid)
                continue
            value = number(node.text)
            row[sid] = value
        observed = d.isoformat()
        if observed in daily and daily[observed][0] != row:
            raise SchemaError("Conflicting duplicate Treasury observation")
        daily[observed] = row, missing
    output = []
    for observed, (row, missing) in sorted(daily.items()):
        meta = {
            "frequency": "daily", "source_timezone": "America/New_York",
            "market_calendar": "US Treasury business days",
            "observation_time_local_approx": "15:30",
            "observation_time_kind": "indicative market quotes, not publication time",
            "publication_time_known": False, "source_feed_updated_at": feed_updated,
            "alignment_rule": "strictly_previous_available_observation_date",
            "same_day_tw_close_available": False,
            "measure": "constant_maturity_par_yield", "annualized": True,
            "source_unit": "percent per annum", "raw_sha256": raw_hash,
            "missing_maturities": missing,
        }
        for sid, value in row.items():
            output.append(make_point(SERIES, sid, observed, value, metadata=meta))
        if all(sid in row for sid in FIELDS):
            spread = (row["us_treasury_10y_pct"] - row["us_treasury_2y_pct"]) * 100
            output.append(make_point(SERIES, "us_yield_curve_10y_2y_bp", observed,
                                     spread, status="derived", metadata={
                **meta, "measure": "yield_spread",
                "source_unit": "basis points",
                "formula": "(us_treasury_10y_pct - us_treasury_2y_pct) * 100",
                "inputs": {sid: float(value) for sid, value in row.items()},
                "input_raw_sha256": [raw_hash],
                "interpretation": "negative means inverted 10Y/2Y curve; not a guaranteed recession forecast",
            }))
    return output


def parse_boj(raw: bytes, code: str, start: date, end: date) -> list[dict]:
    if code not in BOJ_SPECS:
        raise SchemaError("Unapproved BOJ series")
    try:
        payload = json.loads(raw.decode("utf-8-sig"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise SchemaError("Expected BOJ JSON, not HTML error page") from exc
    if not isinstance(payload, dict) or payload.get("STATUS") != 200:
        raise SchemaError("BOJ API returned an error")
    if "NEXTPOSITION" not in payload or payload["NEXTPOSITION"] is not None:
        raise SchemaError("BOJ result is paginated; refusing truncated history")
    spec = BOJ_SPECS[code]
    if payload.get("PARAMETER", {}).get("DB") != spec["db"]:
        raise SchemaError("BOJ database differs from requested series")
    series = payload.get("RESULTSET")
    if not isinstance(series, list) or len(series) != 1:
        raise SchemaError("Expected exactly one BOJ series")
    series = series[0]
    if series.get("SERIES_CODE") != code or series.get("UNIT") != spec["unit"] or series.get("FREQUENCY") != "DAILY":
        raise SchemaError("BOJ series identity, frequency, or unit changed")
    values = series.get("VALUES", {})
    dates, numbers = values.get("SURVEY_DATES"), values.get("VALUES")
    if not isinstance(dates, list) or not isinstance(numbers, list) or len(dates) != len(numbers) or not dates:
        raise SchemaError("BOJ date and value arrays must have equal nonzero lengths")
    raw_hash = hashlib.sha256(raw).hexdigest()
    last_update = _calendar_date(series.get("LAST_UPDATE")).isoformat()
    base_meta = {
        "frequency": "daily", "source_timezone": "Asia/Tokyo",
        "market_calendar": "Japanese banking business days",
        "source_series_code": code, "source_unit": spec["unit"],
        "source_series_name": series.get("NAME_OF_TIME_SERIES"),
        "source_last_update_date": last_update,
        "api_response_generated_at": payload.get("DATE"),
        "publication_time_known": False,
        "alignment_rule": "strictly_previous_available_observation_date",
        "same_day_tw_close_available": False,
        "historical_vintage_available": False,
        "measure": spec["measure"], "raw_sha256": raw_hash,
        "missing_rule": "null remains unavailable; no holiday or missing-day fill",
    }
    if code == "FXERD04":
        base_meta.update({"observation_time_local": "17:00", "quote": "JPY per 1 USD",
                          "rising_means": "JPY depreciation",
                          "publication_note": "17:00 JST is the quote timestamp, not a verified release timestamp"})
    else:
        base_meta.update({"annualized": True, "policy_target": False,
                          "publication_schedule_note": "Provisional around 17:15 JST; final around 10:00 next business day; time-series API can lag",
                          "observation_time_local": "whole business day weighted average"})
    output = []
    seen = set()
    for stamp, value in zip(dates, numbers):
        d = _calendar_date(stamp)
        if d in seen:
            raise SchemaError("Duplicate BOJ calendar date")
        seen.add(d)
        if not start <= d <= end:
            continue
        if value is None:
            continue
        if d.isoformat() > last_update:
            raise SchemaError("BOJ observation lies after the series update date")
        n = number(value)
        if code == "FXERD04" and n <= 0:
            raise SchemaError("JPY/USD exchange rate must be positive")
        output.append(make_point(SERIES, spec["series_id"], d.isoformat(), n, metadata=base_meta))
    return sorted(output, key=lambda p: p["observation_date"])


def jobs(start: date, end: date) -> list[Job]:
    if start > end:
        raise ValueError("start must not follow end")
    lower = _history_start(start, end)
    output = []
    for year in range(lower.year, end.year + 1):
        url = TREASURY_BASE + "?" + urlencode({
            "data": "daily_treasury_yield_curve", "field_tdr_date_value": year})
        output.append(Job(key=f"treasury-yields:{year}:{max(lower, date(year, 1, 1)):%Y%m%d}:{min(end, date(year, 12, 31)):%Y%m%d}", source_id="us_treasury_yields",
                          url=url, parser=partial(parse_treasury, start=lower, end=end, year=year),
                          parser_version=MODEL_VERSION,
                          refresh_after_hours=12 if year == end.year else 24 * 30))
    for code, spec in BOJ_SPECS.items():
        url = BOJ_BASE + "?" + urlencode({
            "format": "json", "lang": "en", "db": spec["db"],
            "startDate": lower.strftime("%Y%m"), "endDate": end.strftime("%Y%m"), "code": code})
        output.append(Job(key=f"boj:{code}:{lower:%Y%m%d}:{end:%Y%m%d}",
                          source_id="boj_overnight" if code == "STRDCLUCON" else "boj_usd_jpy",
                          url=url, parser=partial(parse_boj, code=code, start=lower, end=end),
                          parser_version=MODEL_VERSION, refresh_after_hours=12))
    return output
