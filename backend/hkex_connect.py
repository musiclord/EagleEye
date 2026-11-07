"""HKEX Shanghai/Shenzhen-Hong Kong Stock Connect daily statistics.

The page script is a JavaScript assignment ``tabData = [...]``; the array is parsed as JSON and
never evaluated. Southbound reports buy and sell turnover, so a net buy can be stated.
Northbound reports only total turnover, trade count and the daily quota balance: it is an
activity measure and must never be labelled a net inflow. Parsers perform no network or DB writes.
"""
from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal
from functools import partial
import json
import re

from .connectors import ReportNotAvailable, SchemaError, Source, number
from .contracts import Job, NOT_FOUND_RAW, make_point

BASE = "https://www.hkex.com.hk/eng/csm/DailyStat/data_tab_daily_{:%Y%m%d}e.js"
MODEL_VERSION = "hkex-connect-2"
BACKFILL_DAYS = 365          # ~250 trading days; HKEX keeps only a limited window of daily files
RECENT_DAYS = 7              # recent files are rechecked; older answers (including 404) are kept
DQB_PLACEHOLDER = Decimal("999999999")
# HKEX Terms of Use (last updated 19 August 2025), section 5: without express written permission the
# Information may not be displayed or made available to third parties "whether or not for gain", nor
# accessed by programmatic means. Kept for local analysis only; see docs/WORLD_MODEL.md, 使用條款.
SOURCES = (
    Source("hkex_stock_connect", "港交所 滬深港通每日成交統計（僅供本機）", BASE.format(date(2026, 11, 6)),
           "南向（港股通，滬／深兩條）有買入與賣出成交額（百萬港元），淨買入=買入−賣出；"
           "北向只公布總成交額（百萬人民幣）、筆數與每日額度餘額，只能當活躍度，不是淨流入。"
           "非交易日或超出港交所保留期間回 404，視為當日無檔案，不補零。每日一檔，請求間隔至少 1 秒。"
           "港交所使用條款未經書面許可不得向第三方展示，僅本機分析，公開快照預設排除。",
           "daily", 6, publish=False),
)
MARKETS = {
    "SSE Southbound": ("sse", "south"), "SZSE Southbound": ("szse", "south"),
    "SSE Northbound": ("sse", "north"), "SZSE Northbound": ("szse", "north"),
}
_EXCHANGE = {"sse": "滬", "szse": "深"}
SERIES = {}
for _x, _name in _EXCHANGE.items():
    SERIES[f"hk_connect_{_x}_south_buy_hkd_mn"] = (f"港股通（{_name}）買入成交額", "百萬HKD")
    SERIES[f"hk_connect_{_x}_south_sell_hkd_mn"] = (f"港股通（{_name}）賣出成交額", "百萬HKD")
    SERIES[f"hk_connect_{_x}_south_net_buy_hkd_mn"] = (f"港股通（{_name}）淨買入成交額", "百萬HKD")
    SERIES[f"hk_connect_{_x}_north_turnover_cny_mn"] = (f"{_name}股通北向總成交額（活躍度，非淨流入）", "百萬CNY")
    SERIES[f"hk_connect_{_x}_north_trades"] = (f"{_name}股通北向成交筆數（活躍度）", "筆")
    SERIES[f"hk_connect_{_x}_north_dqb_cny_mn"] = (f"{_name}股通北向每日額度餘額", "百萬CNY")
_ASSIGNMENT = re.compile(r"\s*tabData\s*=\s*(\[.*\])\s*;?\s*", re.S)


def _cell(value):
    """'-' and blank mean not reported; never zero."""
    if isinstance(value, list):
        if len(value) != 1:
            raise SchemaError("HKEX cell shape changed")
        value = value[0]
    s = str(value).strip()
    return None if s in ("", "-") else number(s)


def _trading_table(market: dict) -> dict:
    tables = [c.get("table") for c in market.get("content", []) if isinstance(c, dict)]
    tables = [t for t in tables if isinstance(t, dict) and t.get("classname") == "tradingTable"]
    if len(tables) != 1:
        raise SchemaError("Expected one HKEX tradingTable per market")
    schema, rows = tables[0].get("schema"), tables[0].get("tr")
    if not (isinstance(schema, list) and len(schema) == 1 and isinstance(rows, list)
            and len(schema[0]) == len(rows)):
        raise SchemaError("HKEX tradingTable shape changed")
    values = {}
    for name, row in zip(schema[0], rows):
        td = row.get("td") if isinstance(row, dict) else None
        if not isinstance(td, list) or len(td) != 1:
            raise SchemaError("HKEX tradingTable row changed")
        values[name] = _cell(td[0])
    return values


def parse_connect_day(raw: bytes, day: date) -> list[dict]:
    if raw == NOT_FOUND_RAW:
        return []  # HTTP 404: holiday, weekend or outside HKEX's retention window
    # Observed: 2026-07-03 was served as 8,700 NUL bytes. Empty or all-NUL is an unusable
    # answer for that day, not a format change, and must not suspend the other dates.
    if not raw.strip(b"\x00 \t\r\n"):
        raise ReportNotAvailable("HKEX answered an empty or all-NUL daily statistics file", session_date=day)
    text = raw.decode("utf-8-sig", errors="strict")
    match = _ASSIGNMENT.fullmatch(text)
    if not match:
        raise SchemaError("Expected 'tabData = [...]' assignment")
    try:
        markets = json.loads(match.group(1))
    except json.JSONDecodeError as exc:
        raise SchemaError("HKEX tabData is not a JSON array") from exc
    if not isinstance(markets, list) or sorted(m.get("market") for m in markets if isinstance(m, dict)) != sorted(MARKETS):
        raise SchemaError("HKEX market set changed")
    output = []
    for market in markets:
        if market.get("date") != day.isoformat():
            raise SchemaError("HKEX file returned a different date")
        if market.get("tradingDay") not in (0, 1):
            raise SchemaError("HKEX tradingDay flag changed")
        exchange, direction = MARKETS[market["market"]]
        values = _trading_table(market)
        if market["tradingDay"] == 0:
            if any(v is not None for v in values.values()):
                raise SchemaError("HKEX non-trading market reports values")
            continue
        observed = day.isoformat()
        base = {"frequency": "daily", "source_timezone": "Asia/Hong_Kong", "market": market["market"],
                "direction": "southbound" if direction == "south" else "northbound",
                "publication_time_known": False, "missing_rule": "'-' or 404 means unavailable; never zero"}
        prefix = f"hk_connect_{exchange}_{direction}"
        if direction == "south":
            need = ("Total Turnover", "Buy Turnover", "Sell Turnover")
            if any(k not in values for k in need):
                raise SchemaError("HKEX southbound fields changed")
            total, buy, sell = (values[k] for k in need)
            if None in (total, buy, sell):
                continue
            if min(total, buy, sell) < 0 or abs(buy + sell - total) > Decimal("0.011"):
                raise SchemaError("HKEX southbound buy + sell does not equal total turnover")
            meta = {**base, "source_unit": "HKD million", "total_turnover_hkd_mn": float(total),
                    "unit_basis": "HKEX daily statistics turnover in HK$ million (top-10 table is in HK$)"}
            output += [
                make_point(SERIES, prefix + "_buy_hkd_mn", observed, buy, metadata=meta),
                make_point(SERIES, prefix + "_sell_hkd_mn", observed, sell, metadata=meta),
                make_point(SERIES, prefix + "_net_buy_hkd_mn", observed, buy - sell, status="derived",
                           metadata={**meta, "formula": "Buy Turnover - Sell Turnover",
                                     "interpretation": "turnover-based net buy by southbound investors; "
                                                       "not a settled cash flow or holdings change"}),
            ]
        else:
            need = ("Total Turnover", "Total Trade Count", "DQB")
            if any(k not in values for k in need):
                raise SchemaError("HKEX northbound fields changed")
            if any(k in values for k in ("Buy Turnover", "Sell Turnover")):
                raise SchemaError("HKEX northbound now reports buy/sell; review naming before use")
            meta = {**base, "activity_only": True, "not_net_flow": True,
                    "note": "northbound buy/sell split is not published; total turnover is activity, not inflow"}
            turnover, trades, dqb = (values[k] for k in need)
            if turnover is not None:
                if turnover < 0:
                    raise SchemaError("Negative HKEX turnover")
                output.append(make_point(SERIES, prefix + "_turnover_cny_mn", observed, turnover,
                                         metadata={**meta, "source_unit": "RMB million"}))
            if trades is not None:
                output.append(make_point(SERIES, prefix + "_trades", observed, trades,
                                         metadata={**meta, "source_unit": "trades"}))
            # 999,999,999 is a display placeholder, not a quota balance.
            if dqb is not None and dqb != DQB_PLACEHOLDER:
                output.append(make_point(SERIES, prefix + "_dqb_cny_mn", observed, dqb,
                                         metadata={**meta, "source_unit": "RMB million (as displayed)"}))
    return output


def jobs(start: date, end: date) -> list[Job]:
    if start > end:
        raise ValueError("start must not follow end")
    lower = min(start, end - timedelta(days=BACKFILL_DAYS))
    output = []
    day = lower
    while day <= end:
        if day.weekday() < 5:
            recent = (end - day).days <= RECENT_DAYS
            output.append(Job(key=f"hkex-connect:{day:%Y%m%d}", source_id="hkex_stock_connect",
                              url=BASE.format(day), parser=partial(parse_connect_day, day=day),
                              parser_version=MODEL_VERSION, refresh_after_hours=6 if recent else 24 * 365,
                              max_bytes=500_000, empty_ok=True))
        day += timedelta(days=1)
    return output
