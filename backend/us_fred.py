"""US dollar liquidity, foreign official custody holdings and selected equity indexes from FRED.

FRED's public ``fredgraph.csv`` needs no API key. Parsers perform no network or DB writes.
Derived values such as "net liquidity" are deliberately NOT computed here; the front end
combines the published inputs and must disclose their differing measurement conventions.
"""
from __future__ import annotations

from datetime import date
from dataclasses import replace
from functools import partial
from urllib.parse import urlencode

from .connectors import SchemaError, Source, number
from .contracts import Job, make_point

FRED_CSV = "https://fred.stlouisfed.org/graph/fredgraph.csv"
# 2: RRPONTSYD is FRBNY content, not a U.S. government work; its rows carry their own licence code.
MODEL_VERSION = "us-fred-2"
# A long enough window for the post-2008 balance-sheet regime; earlier history is not requested.
HISTORY_START = date(2008, 1, 1)

# Checked 2026-11-07; quotations and URLs are in docs/WORLD_MODEL.md, 使用條款.
LICENSE_PUBLIC = ("Federal Reserve Board data: public domain per the Board's website policy (last update "
                  "2024-08-02), cite the Board. FRED label: Public Domain: Citation Requested; cite the source "
                  "and that the data were obtained via FRED.")
LICENSE_FRBNY = ("Federal Reserve Bank of New York content under its Terms of Use (last updated 2023-06-09): "
                 "may be copied and distributed with the notice '(c) [year] Federal Reserve Bank of New York. "
                 "Content from the New York Fed subject to the Terms of Use at newyorkfed.org.', modifications "
                 "labelled, no implied endorsement. FRED label: Copyrighted: Citation Required.")
LICENSE_LOCAL = ("Index provider copyright. FRED states the series may not be reproduced without "
                 "permission; kept for local analysis only and excluded from published snapshots.")

SOURCES = (
    Source("fred_h41", "聯準會 H.4.1 資產負債表與外國官方保管持有（經 FRED）", FRED_CSV + "?id=WALCL",
           "WALCL、WMTSECL1 為週三水位；WTREGEN 為截至週三的週平均，兩者口徑不同，相減前須先說明。"
           "單位為百萬美元。週四美東下午發布，資料日不是發布日；FRED 的修訂不保留歷史 vintage。",
           "weekly", 10, public_years=10),
    Source("fred_rrp", "紐約聯準銀行隔夜逆回購（經 FRED）", FRED_CSV + "?id=RRPONTSYD",
           "每日操作成交總額，單位十億美元；非營業日沒有觀測，不補零也不沿用前值。", "daily", 6, public_years=10),
    Source("fred_h10", "聯準會 H.10 匯率與廣義美元指數（經 FRED）", FRED_CSV + "?id=DTWEXBGS",
           "H.10 每週一發布前一週日資料；人民幣為紐約中午買入匯率（CNY per USD，上升代表人民幣貶值），"
           "不是在岸 CFETS 中間價或離岸 CNH。廣義美元指數以 2006 年 1 月為 100。", "daily", 12, public_years=10),
    Source("fred_sp500", "S&P 500 收盤指數（經 FRED，僅供本機）", FRED_CSV + "?id=SP500",
           "S&P Dow Jones Indices 版權；FRED 只提供近十年。僅本機分析，公開快照預設排除。", "daily", 6,
           publish=False),
    Source("fred_nikkei225", "日經 225 收盤指數（經 FRED，僅供本機）", FRED_CSV + "?id=NIKKEI225",
           "日經版權；FRED 載明未經許可不得轉載。僅本機分析，公開快照預設排除。東京收盤早於台股收盤後的美國資料。",
           "daily", 6, publish=False),
)
# 2026-11-08: Board series now come directly from FRB XML; the browser already reads NYFed RRP.
# Preserve legacy local observations/parsers, but do not republish FRED-hosted copies.
REPLACED_SOURCES = {'fred_h41', 'fred_h10', 'fred_rrp'}
SOURCES = tuple(replace(s, publish=False, coverage_note=s.coverage_note +
                       ' 此FRED擷取路徑已停用；理事會資料改取FRB原始發布，逆回購由瀏覽器直讀紐約聯準會。')
                if s.id in REPLACED_SOURCES else s for s in SOURCES)
SOURCE_PUBLISH = {s.id: s.publish for s in SOURCES}

# fred id -> series id, source, label, unit, source unit, measure, frequency, value rule
SPECS = {
    "WALCL": dict(series_id="fed_total_assets_usd_mn", source_id="fred_h41",
                  label="聯準會總資產（週三水位）", unit="百萬USD",
                  source_unit="Millions of U.S. Dollars", measure="wednesday_level",
                  frequency="weekly", observation_day="Wednesday", positive=True,
                  original_source="Board of Governors of the Federal Reserve System, H.4.1"),
    "WTREGEN": dict(series_id="us_treasury_general_account_usd_mn", source_id="fred_h41",
                    label="美國財政部一般帳戶 TGA（截至週三的週平均）", unit="百萬USD",
                    source_unit="Millions of U.S. Dollars", measure="week_average_ending_wednesday",
                    frequency="weekly", observation_day="Wednesday (week ending)", positive=False,
                    original_source="Board of Governors of the Federal Reserve System, H.4.1"),
    "WMTSECL1": dict(series_id="fed_custody_foreign_official_ust_usd_mn", source_id="fred_h41",
                     label="聯準會代外國官方與國際機構保管的可交易美國公債（週三水位）", unit="百萬USD",
                     source_unit="Millions of U.S. Dollars", measure="wednesday_level",
                     frequency="weekly", observation_day="Wednesday", positive=True,
                     original_source="Board of Governors of the Federal Reserve System, H.4.1 memorandum item"),
    "RRPONTSYD": dict(series_id="fed_overnight_reverse_repo_usd_bn", source_id="fred_rrp",
                      label="聯準會隔夜逆回購操作總額", unit="十億USD",
                      source_unit="Billions of US Dollars", measure="daily_operation_total",
                      frequency="daily", positive=False, license="frbny_terms_attribution_required",
                      original_source="Federal Reserve Bank of New York, Temporary Open Market Operations"),
    "DTWEXBGS": dict(series_id="usd_broad_index", source_id="fred_h10",
                     label="名目廣義美元指數", unit="index (2006-01=100)",
                     source_unit="Index Jan 2006=100", measure="nominal_broad_trade_weighted",
                     frequency="daily", positive=True,
                     original_source="Board of Governors of the Federal Reserve System, H.10"),
    "DEXCHUS": dict(series_id="usd_cny_noon_ny", source_id="fred_h10",
                    label="人民幣兌美元（紐約中午買入匯率）", unit="CNY/USD",
                    source_unit="Chinese Yuan Renminbi to One U.S. Dollar",
                    measure="noon_buying_rate_new_york", frequency="daily", positive=True,
                    original_source="Board of Governors of the Federal Reserve System, H.10"),
    "SP500": dict(series_id="sp500_close", source_id="fred_sp500", label="S&P 500 收盤指數",
                  unit="point", source_unit="Index", measure="daily_close", frequency="daily",
                  positive=True, original_source="S&P Dow Jones Indices LLC"),
    "NIKKEI225": dict(series_id="nikkei225_close", source_id="fred_nikkei225", label="日經 225 收盤指數",
                      unit="point", source_unit="Index", measure="daily_close", frequency="daily",
                      positive=True, original_source="Nikkei Inc."),
}
SERIES = {spec["series_id"]: (spec["label"], spec["unit"]) for spec in SPECS.values()}
MISSING = {"", "."}


def parse_fred_csv(raw: bytes, fred_id: str) -> list[dict]:
    """Strict two-column CSV. Blank or '.' cells are holidays/unpublished days and are omitted."""
    if fred_id not in SPECS:
        raise SchemaError("Unapproved FRED series")
    spec = SPECS[fred_id]
    try:
        text = raw.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise SchemaError("Expected FRED CSV text") from exc
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    if not lines or lines[0].startswith("<"):
        raise SchemaError("Expected FRED CSV, not an HTML page")
    if lines[0].split(",") not in (["observation_date", fred_id], ["DATE", fred_id]):
        raise SchemaError("FRED CSV header or series id changed")
    publish = SOURCE_PUBLISH[spec["source_id"]]
    meta = {
        "frequency": spec["frequency"], "fred_series_id": fred_id, "source_unit": spec["source_unit"],
        "measure": spec["measure"], "original_source": spec["original_source"],
        "publication_time_known": False, "historical_vintage_available": False, "publish": publish,
        # Full licence wording lives in LICENSE_* and the source note; rows carry a short code.
        "license": spec.get("license") or ("index_provider_copyright_local_only" if fred_id in {"SP500", "NIKKEI225"} else "us_government_public"),
        "missing_rule": "blank or '.' omitted; no fill",
        "source_timezone": "America/New_York" if fred_id != "NIKKEI225" else "Asia/Tokyo",
    }
    if "observation_day" in spec:
        meta["observation_day"] = spec["observation_day"]
    if fred_id == "DEXCHUS":
        meta.update(quote="CNY per 1 USD", rising_means="CNY depreciation",
                    not_equivalent_to="CFETS central parity or offshore CNH")
    if fred_id == "WTREGEN":
        meta["comparability_note"] = ("week average; WALCL is a Wednesday level. Subtracting them mixes "
                                      "conventions (FRED WDTGAL is the Wednesday-level TGA).")
    output, seen = [], set()
    for line in lines[1:]:
        cells = line.split(",")
        if len(cells) != 2:
            raise SchemaError("FRED CSV row shape changed")
        try:
            day = date.fromisoformat(cells[0])
        except ValueError as exc:
            raise SchemaError("Invalid FRED observation date") from exc
        if day in seen:
            raise SchemaError("Duplicate FRED observation date")
        seen.add(day)
        if cells[1].strip() in MISSING:
            continue
        value = number(cells[1])
        if value < 0 or (spec["positive"] and value == 0):
            raise SchemaError("FRED value outside the valid range for " + fred_id)
        output.append(make_point(SERIES, spec["series_id"], day.isoformat(), value, metadata=meta))
    return sorted(output, key=lambda p: p["observation_date"])


def jobs(start: date, end: date) -> list[Job]:
    """Two requests per series: a stable archive (rarely revised) and the current year.

    Splitting keeps a daily refresh from re-versioning every historical row.
    """
    if start > end:
        raise ValueError("start must not follow end")
    output = []
    archive_end = date(end.year - 1, 12, 31)
    for fred_id, spec in SPECS.items():
        if spec["source_id"] in REPLACED_SOURCES:
            continue
        windows = []
        if archive_end >= HISTORY_START:
            windows.append(("archive", HISTORY_START, archive_end, 24 * 30))
        windows.append(("current", date(end.year, 1, 1), None, 12))
        for name, lower, upper, ttl in windows:
            params = {"id": fred_id, "cosd": lower.isoformat()}
            if upper:
                params["coed"] = upper.isoformat()
            output.append(Job(
                key=f"fred:{fred_id}:{name}:{lower:%Y%m%d}" + (f":{upper:%Y%m%d}" if upper else ""),
                source_id=spec["source_id"], url=FRED_CSV + "?" + urlencode(params),
                parser=partial(parse_fred_csv, fred_id=fred_id), parser_version=MODEL_VERSION,
                refresh_after_hours=ttl, max_bytes=2_000_000))
    return output
