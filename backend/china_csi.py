"""CSI 300 daily close from China Securities Index Co. public JSON. Local analysis only.

CSI index values are the index provider's property; the source is flagged ``publish=False``
so web_export leaves it out of published snapshots. Parsers perform no network or DB writes.
"""
from __future__ import annotations

from datetime import date
from functools import partial
import json
from urllib.parse import urlencode

from .connectors import SchemaError, Source, number
from .contracts import Job, make_point

CSI_PERF = "https://www.csindex.com.cn/csindex-home/perf/index-perf"
MODEL_VERSION = "china-csi-1"
HISTORY_START = date(2008, 1, 1)
INDEX_CODE = "000300"
SOURCES = (
    Source("csi_index_perf", "中證指數公司 滬深300 收盤（僅供本機）",
           CSI_PERF + "?indexCode=000300",
           "中證指數公司官網公開 JSON；指數版權屬中證，僅本機分析，公開快照預設排除。"
           "收盤為上海時間 15:00，早於台股收盤後的美國資料；只取收盤，不取成交量。",
           "daily", 6, publish=False),
)
SERIES = {"csi300_close": ("滬深300 收盤指數", "point")}
_PRICE_FIELDS = ("open", "high", "low", "close", "tradingVol", "tradingValue")


def _trade_date(value) -> date:
    s = str(value)
    if len(s) != 8 or not s.isdigit():
        raise SchemaError("Expected CSI tradeDate YYYYMMDD")
    try:
        return date(int(s[:4]), int(s[4:6]), int(s[6:]))
    except ValueError as exc:
        raise SchemaError("Invalid CSI trade date") from exc


def parse_csi_perf(raw: bytes, requested_start: date | None = None) -> list[dict]:
    try:
        payload = json.loads(raw.decode("utf-8-sig"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise SchemaError("Expected CSI JSON, not an HTML or challenge page") from exc
    if not isinstance(payload, dict) or str(payload.get("code")) != "200" or payload.get("success") is not True:
        raise SchemaError("CSI API did not return success")
    rows = payload.get("data")
    if not isinstance(rows, list) or not rows:
        raise SchemaError("CSI response contains no rows")
    parsed = []
    for row in rows:
        if not isinstance(row, dict) or row.get("indexCode") != INDEX_CODE:
            raise SchemaError("CSI index code changed")
        parsed.append((_trade_date(row.get("tradeDate")), row))
    parsed.sort(key=lambda item: item[0])
    if len({d for d, _ in parsed}) != len(parsed):
        raise SchemaError("Duplicate CSI trade date")
    # Observed: when the requested start is a holiday the API labels a row with that date
    # but copies the next session (2010-01-01 == 2010-01-04). Drop only that placeholder.
    dropped = None
    if (len(parsed) > 1 and requested_start is not None and parsed[0][0] == requested_start
            and all(parsed[0][1].get(k) == parsed[1][1].get(k) for k in _PRICE_FIELDS)):
        dropped = parsed.pop(0)[0].isoformat()
    for (_, a), (_, b) in zip(parsed, parsed[1:]):
        if all(a.get(k) == b.get(k) for k in _PRICE_FIELDS):
            raise SchemaError("Identical consecutive CSI sessions; refusing a possible carried-forward row")
    meta = {"frequency": "daily", "source_timezone": "Asia/Shanghai", "observation_time_local": "15:00",
            "measure": "daily_close", "index_code": INDEX_CODE, "index_name": "CSI 300",
            "publish": False, "license": "index_provider_copyright_local_only",
            "publication_time_known": False,
            "alignment_rule": "strictly_previous_available_observation_date",
            "missing_rule": "non-trading days are absent; no fill"}
    if dropped:
        meta = {**meta, "dropped_placeholder_date": dropped}
    output = []
    for day, row in parsed:
        close = row.get("close")
        if close is None:
            continue
        value = number(close)
        if value <= 0:
            raise SchemaError("CSI close must be positive")
        output.append(make_point(SERIES, "csi300_close", day.isoformat(), value, metadata=meta))
    return output


def jobs(start: date, end: date) -> list[Job]:
    if start > end:
        raise ValueError("start must not follow end")
    windows = []
    archive_end = date(end.year - 1, 12, 31)
    if archive_end >= HISTORY_START:
        windows.append(("archive", HISTORY_START, archive_end, 24 * 30))
    windows.append(("current", date(end.year, 1, 1), end, 12))
    output = []
    for name, lower, upper, ttl in windows:
        url = CSI_PERF + "?" + urlencode({"indexCode": INDEX_CODE, "startDate": lower.strftime("%Y%m%d"),
                                         "endDate": upper.strftime("%Y%m%d")})
        output.append(Job(key=f"csi:{INDEX_CODE}:{name}:{lower:%Y%m%d}" + (f":{upper:%Y%m%d}" if name == "archive" else ""),
                          source_id="csi_index_perf", url=url,
                          parser=partial(parse_csi_perf, requested_start=lower),
                          parser_version=MODEL_VERSION, refresh_after_hours=ttl, max_bytes=5_000_000))
    return output
