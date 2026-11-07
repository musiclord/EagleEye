"""Official CBC FX and FSC/SFB monthly foreign-investor adapters.

No network access in parsers. The engine owns downloads, immutable raw cache,
timeouts, and publication/retrieval provenance. A positive remittance is net
inflow, not gross inflow, and must never be substituted for market purchases.
"""
from __future__ import annotations

from calendar import monthrange
from datetime import date
from decimal import Decimal
from functools import partial
from html import unescape
from io import BytesIO
import json
import re
from urllib.parse import quote, urlencode, urljoin, urlsplit
import xml.etree.ElementTree as ET
from zipfile import ZipFile

from .connectors import Source, SchemaError, number, observation_date
from .contracts import Job, make_point

CBC_URL = "https://cpx.cbc.gov.tw/api/OpenData/FTDOpenData_Day"
FSC_URL = "https://stat.fsc.gov.tw/api/v1/public/datasets/70284558-c657-47c0-9352-5e19bac25479/data?limit=500"
SFB_BASE = "https://www.sfb.gov.tw"
SFB_INDEX = SFB_BASE + "/ch/home.jsp"
SOURCES = (
    Source("cbc_fx", "央行／台北外匯經紀銀行間每日收盤匯率", CBC_URL,
           "TWD per USD。上升代表台幣貶值；為銀行間收盤，不是零售牌告或即時報價。"),
    Source("fsc_foreign_holdings", "金管會上市／上櫃外資持股市值比重", FSC_URL,
           "月末存量市值占比，分上市、上櫃及合計；變動同時受股價與持股影響，非資金流量。"
           "官方金融競爭力資料，重複月份只接受完全一致的持股值。", "monthly", 100),
    Source("sfb_remittances", "證期局外資及陸資投資證券淨匯出入", SFB_INDEX,
           "官方月新聞稿的淨額，含外資與陸資；正為淨匯入、負為淨匯出。"
           "不是gross匯入/匯出、不是外資買賣超，也非全體國際資本流動。", "monthly", 45),
)
SERIES = {
    "usd_twd": ("美元／新台幣銀行間收盤匯率", "TWD/USD"),
    "foreign_holding_listed_pct": ("外資持有上市股票市值占比", "%"),
    "foreign_holding_otc_pct": ("外資持有上櫃股票市值占比", "%"),
    "foreign_holding_total_pct": ("外資持有上市櫃股票市值占比", "%"),
    "foreign_monthly_net_inflow_usd_100m": ("外資及陸資單月淨匯入", "億USD"),
    "foreign_cumulative_net_inflow_usd_100m": ("外資及陸資歷史累積淨匯入", "億USD"),
    "foreign_ytd_net_inflow_usd_100m": ("外資及陸資年初至今淨匯入", "億USD"),
}
HOLDING_FIELDS = {
    "foreign_holding_listed_pct": "上市外資持有股票占市值比重",
    "foreign_holding_otc_pct": "上櫃外資持有股票占市值比重",
    "foreign_holding_total_pct": "外資持有股票占市值比重(上市櫃)",
}
_NS = {"t": "urn:oasis:names:tc:opendocument:xmlns:table:1.0"}


def _month_end(year: int, month: int) -> date:
    return date(year, month, monthrange(year, month)[1])


def _shift_month(d: date, offset: int) -> date:
    absolute = d.year * 12 + d.month - 1 + offset
    return date(absolute // 12, absolute % 12 + 1, 1)


def _json(raw: bytes):
    try:
        return json.loads(raw.decode("utf-8-sig"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise SchemaError("Expected official JSON, not an HTML error page") from exc


def parse_fx(raw: bytes, start: date, end: date) -> list[dict]:
    rows = _json(raw)
    if not isinstance(rows, list) or not rows:
        raise SchemaError("CBC daily array missing")
    values = {}
    for row in rows:
        if not isinstance(row, dict) or not {"日期", "NTD_USD"} <= row.keys():
            raise SchemaError("CBC fields changed")
        observed = observation_date(row["日期"])
        d = date.fromisoformat(observed)
        if not start <= d <= end:
            continue
        value = number(row["NTD_USD"])
        if value <= 0:
            raise SchemaError("Exchange rate must be strictly positive")
        if observed in values and values[observed] != value:
            raise SchemaError("Conflicting CBC observations")
        values[observed] = value
    return [make_point(SERIES, "usd_twd", d, v, metadata={
        "frequency": "daily", "quote": "TWD per 1 USD",
        "rising_means": "TWD depreciation", "price_type": "interbank close",
        "publication_time_known": False,
        "schedule_note": "央行說明每營業日17:00前公布；非本觀察值實際公布時間",
    }) for d, v in sorted(values.items())]


def parse_holdings(raw: bytes, start: date, end: date) -> list[dict]:
    payload = _json(raw)
    if not isinstance(payload, dict) or not isinstance(payload.get("data"), list):
        raise SchemaError("FSC dataset envelope changed")
    rows = payload["data"]
    if payload.get("total", len(rows)) > len(rows):
        raise SchemaError("FSC response is paginated; refusing truncated history")
    values = {}
    anomalies = {}
    for row in rows:
        if not {"年月", *HOLDING_FIELDS.values()} <= row.keys():
            raise SchemaError("FSC holdings fields changed")
        ym = str(row["年月"])
        if not re.fullmatch(r"\d{5}", ym):
            raise SchemaError("Expected ROC YYYMM monthly holdings")
        d = _month_end(int(ym[:3]) + 1911, int(ym[3:]))
        if not start <= d <= end:
            continue
        ratios = {sid: number(row[field]) for sid, field in HOLDING_FIELDS.items()}
        corrected = {}
        for sid, ratio in ratios.items():
            if not 0 <= ratio <= 1:
                if not 1 < ratio <= 100:
                    raise SchemaError("FSC holding ratio outside valid range")
                corrected[sid] = ratio
        if corrected:
            # Accept a source unit anomaly only after the independently reported
            # listed/OTC market-cap weighted total verifies the candidate scale.
            normalized = {sid: value / 100 if sid in corrected else value
                          for sid, value in ratios.items()}
            try:
                listed_cap = number(row["上市股市市值（單位：10億元）"])
                otc_cap = number(row["上櫃股市市值（單位：10億元）"])
            except KeyError as exc:
                raise SchemaError("Cannot validate anomalous source ratio scale") from exc
            if listed_cap <= 0 or otc_cap <= 0:
                raise SchemaError("Invalid market capitalization for ratio validation")
            weighted = (normalized["foreign_holding_listed_pct"] * listed_cap +
                        normalized["foreign_holding_otc_pct"] * otc_cap) / (listed_cap + otc_cap)
            if abs(weighted - normalized["foreign_holding_total_pct"]) > Decimal("0.00015"):
                raise SchemaError("Anomalous source ratio fails market-value cross-check")
            ratios = normalized
        for sid, ratio in ratios.items():
            key = sid, d.isoformat()
            if key in values and values[key] != ratio:
                raise SchemaError("Conflicting duplicate FSC holdings; revision order unknown")
            values[key] = ratio
            if sid in corrected:
                anomalies[key] = {"source_unit_anomaly": True, "raw_source_value": float(corrected[sid]),
                                  "source_scale": "percentage (verified anomaly)",
                                  "conversion": "raw percentage retained after weighted market-value validation",
                                  "normalization": "raw percentage / 100 = fraction",
                                  "validation": "listed/OTC market-cap weighted share matches total within 0.015 percentage points"}
    return [make_point(SERIES, sid, observed, ratio * 100, metadata={
        "frequency": "monthly", "measure": "market_value_share",
        "source_scale": "fraction", "conversion": "source fraction × 100",
        "publication_time_known": False,
        **anomalies.get((sid, observed), {}),
        "coverage": {"foreign_holding_listed_pct": "listed",
                     "foreign_holding_otc_pct": "OTC",
                     "foreign_holding_total_pct": "listed_and_OTC"}[sid],
    }) for (sid, observed), ratio in sorted(values.items())]


def _accounting_number(cell: str) -> Decimal:
    s = (cell or "").replace("\n", "").replace(" ", "").replace("−", "-")
    if re.fullmatch(r"[\d,]+\.\d+\.", s):
        s = s[:-1]  # Official 2024-10 ODT has total "32.91."; reconcile below.
    if re.fullmatch(r"\([\d,]+(?:\.\d+)?\)", s):
        return -number(s[1:-1])
    if not re.fullmatch(r"-?[\d,]+(?:\.\d+)?", s):
        raise SchemaError("Invalid remittance table numeric cell")
    return number(s)


def _period_digits(cell: str) -> list[int]:
    return [int(v) for v in re.findall(r"\d+", cell or "")]


def remittance_points(tables: list, period: date, released: date, source_url: str,
                      format_name: str) -> list[dict]:
    """Verify the two official table grids and their arithmetic before ingestion.

    Some SFB files contain damaged Chinese character maps even in ODT XML.
    Numeric headers, all table dimensions, row sums, and stock/flow reconciliation
    are checked; an unrecognized layout is rejected rather than guessed.
    """
    if released < period or released > _shift_month(period, 3):
        raise SchemaError("Unexpected SFB release/observation relationship")
    roc, month = period.year - 1911, period.month
    previous = _shift_month(period, -1)
    candidates = []
    for tab in tables:
        if len(tab) != 4 or any(len(row) != len(tab[0]) for row in tab):
            continue
        width = len(tab[0])
        if width not in (2, 3, 4):
            continue
        if _period_digits(tab[0][1]) != [roc, month]:
            continue
        try:
            nums = [[_accounting_number(c) for c in row[1:]] for row in tab[1:]]
        except SchemaError:
            continue
        candidates.append((tab, nums, width))
    monthly = [x for x in candidates if x[2] in (2, 3)]
    cumulative = [x for x in candidates if x[2] == 4]
    if len(monthly) != 1 or len(cumulative) != 1:
        raise SchemaError("Cannot uniquely identify SFB monthly/cumulative table")
    mt, mv, mw = monthly[0]
    ct, cv, _ = cumulative[0]
    if mw == 3 and _period_digits(mt[0][2]) != [roc, month]:
        raise SchemaError("YTD heading does not match observation month")
    if _period_digits(ct[0][2]) != [previous.year - 1911, previous.month]:
        raise SchemaError("Previous cumulative heading is not the preceding month")
    tolerance = Decimal("0.02")  # Official values rounded independently to 0.01 億USD.
    for rows in (mv, cv):
        for col in range(len(rows[0])):
            if abs(rows[0][col] + rows[1][col] - rows[2][col]) > tolerance:
                raise SchemaError("Foreign plus mainland total fails arithmetic")
    for row in range(3):
        if abs(cv[row][0] - cv[row][1] - cv[row][2]) > tolerance:
            raise SchemaError("Cumulative change does not reconcile")
        if abs(mv[row][0] - cv[row][2]) > tolerance:
            raise SchemaError("Official monthly net and cumulative change disagree")
    meta = {
        "frequency": "monthly", "coverage": "foreign_and_mainland_securities_investors",
        "measure": "net_remittance", "positive_means": "net inflow",
        "gross_inflow_outflow_available": False, "source_url": source_url,
        "source_format": format_name, "source_release_date": released.isoformat(),
        "release_precision": "date", "source_unit": "USD 100 million",
        "validation": "3 component sums; 3 cumulative differences; 3 monthly reconciliations",
        "rounding_tolerance_usd_100m": float(tolerance),
        "cumulative_previous": float(cv[2][1]),
        "warning": "淨額非gross匯出入；證券投資匯出入與股票交易買賣超口徑不同",
    }
    output = [
        make_point(SERIES, "foreign_monthly_net_inflow_usd_100m", period.isoformat(),
                   mv[2][0], release_at=released.isoformat(), metadata=meta),
        make_point(SERIES, "foreign_cumulative_net_inflow_usd_100m", period.isoformat(),
                   cv[2][0], release_at=released.isoformat(), metadata=meta),
    ]
    ytd = mv[2][1] if mw == 3 else (mv[2][0] if month == 1 else None)
    if ytd is not None:
        output.append(make_point(SERIES, "foreign_ytd_net_inflow_usd_100m",
                                 period.isoformat(), ytd,
                                 release_at=released.isoformat(), metadata=meta))
    return output


def parse_remittance_pdf(raw: bytes, period: date, released: date, source_url: str) -> list[dict]:
    if not raw.startswith(b"%PDF"):
        raise SchemaError("SFB attachment is not PDF")
    import pdfplumber
    with pdfplumber.open(BytesIO(raw)) as doc:
        if not 1 <= len(doc.pages) <= 8:
            raise SchemaError("Unexpected remittance PDF length")
        tables = [t for page in doc.pages for t in page.extract_tables()]
    return remittance_points(tables, period, released, source_url, "PDF")


def parse_remittance_odt(raw: bytes, period: date, released: date, source_url: str) -> list[dict]:
    with ZipFile(BytesIO(raw)) as z:
        entry = z.getinfo("content.xml")
        if entry.file_size > 5_000_000:
            raise SchemaError("Unexpectedly large ODT XML")
        root = ET.fromstring(z.read(entry))
    tables = []
    for tab in root.findall(".//t:table", _NS):
        tables.append([["".join(cell.itertext()) for cell in row.findall("t:table-cell", _NS)]
                       for row in tab.findall("t:table-row", _NS)])
    return remittance_points(tables, period, released, source_url, "ODT")


def _safe_sfb_url(href: str) -> str:
    url = urljoin(SFB_INDEX, unescape(href))
    parts = urlsplit(url)
    if parts.scheme != "https" or parts.netloc != "www.sfb.gov.tw":
        raise SchemaError("SFB index points outside the official allowed host")
    return quote(url, safe=":/?&=%+;,")


def _attachment_job(url: str, period: date, released: date, end: date) -> Job:
    if ".pdf" in url.lower():
        parser = parse_remittance_pdf
    elif ".odt" in url.lower():
        parser = parse_remittance_odt
    elif ".doc" in url.lower():
        parser = parse_remittance_doc
    else:
        raise SchemaError("No supported public PDF/ODT/DOC attachment")
    return Job(key=f"sfb-remittance-{period:%Y-%m}", source_id="sfb_remittances",
               url=url, parser=partial(parser, period=period, released=released, source_url=url),
               parser_version="5", refresh_after_hours=24 if period >= _shift_month(end, -3) else 8760)


def discover_attachment(raw: bytes, period: date, released: date, end: date) -> list[Job]:
    text = raw.decode("utf-8-sig")
    links = [unescape(x) for x in re.findall(r'href=["\x27]([^"\x27]+)', text, re.I)]
    candidates = [x for x in links if "uploaddowndoc" in x and ".pdf" in x.lower()]
    if not candidates:
        candidates = [x for x in links if "uploaddowndoc" in x and ".odt" in x.lower()]
    if not candidates:
        candidates = [x for x in links if "uploaddowndoc" in x and ".doc" in x.lower()]
    if not candidates:
        raise SchemaError("Official detail page has no PDF/ODT/DOC attachment")
    return [_attachment_job(_safe_sfb_url(candidates[0]), period, released, end)]


def discover_remittances(raw: bytes, start: date, end: date) -> list[Job]:
    text = raw.decode("utf-8-sig")
    # These publication-year queries contain at most 12 monthly press releases.
    # A newly paginated response must be explicitly handled, never truncated.
    page = re.search(r'class="page">\s*(\d+)\s*/\s*(\d+)', text)
    if page and int(page[2]) > 1:
        raise SchemaError("SFB annual query unexpectedly paginated")
    blocks = re.split(r'<div\s+class="whitebackground"\s*>', text)[1:]
    output = []
    for block in blocks:
        anchor = re.search(r'<a\b(?P<attrs>[^>]+)>(?P<title>\s*\d{2,3}年\s*\d{1,2}月外資新聞稿\s*)</a>', block)
        if not anchor:
            continue
        y, m = [int(x) for x in re.findall(r"\d+", anchor["title"])]
        period = _month_end(y + 1911, m)
        if not start <= period <= end:
            continue
        release_match = re.search(r'class="pdate1">\s*(\d{4}-\d{2}-\d{2})', block)
        if not release_match:
            raise SchemaError("SFB official publication date missing")
        released = date.fromisoformat(release_match[1])
        if released > end:
            continue
        href = re.search(r'href="([^"]+)"', anchor["attrs"])
        if not href:
            raise SchemaError("SFB monthly article URL missing")
        url = _safe_sfb_url(href[1])
        if any(ext in url.lower() for ext in (".pdf", ".odt", ".doc")):
            output.append(_attachment_job(url, period, released, end))
        else:
            if "uploaddowndoc" in url:  # Legacy direct DOC: resolve its official detail page.
                news_id = re.search(r"'NewsFile','(\d{12})'", anchor["attrs"])
                if not news_id:
                    raise SchemaError("Cannot resolve DOC to public detail page")
                url = SFB_INDEX + "?" + urlencode({
                    "id": 95, "parentpath": "0,2", "mcustomize": "news_view.jsp",
                    "dataserno": news_id[1], "dtable": "News"})
            output.append(Job(key=f"sfb-detail-{period:%Y-%m}", source_id="sfb_remittances",
                              url=url, parser=lambda raw: [],
                              followups=partial(discover_attachment, period=period, released=released, end=end),
                              parser_version="5", refresh_after_hours=8760))
    if not output and "外資新聞稿" not in text:
        raise SchemaError("SFB index content missing")
    return output


def jobs(start: date, end: date) -> list[Job]:
    if start > end:
        raise ValueError("start must not follow end")
    # Keep 27 calendar months to cover publication lags and a complete 24-month
    # comparison window. No forward-fill creates fictitious monthly observations.
    monthly_start = min(start.replace(day=1), _shift_month(end, -27))
    output = [
        Job(key="cbc-usd-twd-history", source_id="cbc_fx", url=CBC_URL,
            parser=partial(parse_fx, start=start, end=end), parser_version="5", refresh_after_hours=12,
            parse_scope=f"{start}:{end}", refresh_at_local="17:00", requested_end=end),
        Job(key="fsc-foreign-holdings-history", source_id="fsc_foreign_holdings", url=FSC_URL,
            parser=partial(parse_holdings, start=monthly_start, end=end),
            parser_version="5", refresh_after_hours=24, parse_scope=f"{monthly_start}:{end}",
            refresh_at_local="16:30", requested_end=end),
    ]
    for year in range(monthly_start.year, end.year + 1):
        begin = max(monthly_start, date(year, 1, 1))
        finish = min(end, date(year, 12, 31))
        url = SFB_INDEX + "?" + urlencode({
            "id": 95, "parentpath": "0,2", "mcustomize": "news_list.jsp",
            "keyword": "外資新聞稿", "qptdate": begin.isoformat(),
            "qdldate": finish.isoformat(), "page": 1,
        })
        output.append(Job(key=f"sfb-remittance-index-{year}", source_id="sfb_remittances",
                          url=url, parser=lambda raw: [],
                          followups=partial(discover_remittances, start=monthly_start, end=end),
                          parser_version="5", refresh_after_hours=24 if year == end.year else 8760))
    return output

def _doc_piece_text(word: bytes, table: bytes) -> str:
    """Read MS-DOC FIB/CLX text pieces only; no automation or macro execution."""
    import struct
    def unpack(fmt, raw, offset):
        if offset < 0 or offset + struct.calcsize(fmt) > len(raw):
            raise SchemaError("Truncated DOC binary structure")
        return struct.unpack_from(fmt, raw, offset)
    ident, version = unpack("<HH", word, 0)
    flags, = unpack("<H", word, 10)
    if ident != 0xA5EC or version < 0xC1 or flags & 0x0100:
        raise SchemaError("Unsupported or encrypted DOC")
    csw, = unpack("<H", word, 32)
    lw_offset = 34 + 2 * csw
    cslw, = unpack("<H", word, lw_offset)
    lw_data = lw_offset + 2
    main_chars, = unpack("<I", word, lw_data + 12)
    if cslw < 4 or not 1 <= main_chars <= 2_000_000:
        raise SchemaError("Invalid DOC main text length")
    pairs_offset = lw_data + cslw * 4
    pair_count, = unpack("<H", word, pairs_offset)
    if pair_count <= 33:
        raise SchemaError("DOC CLX pointer missing")
    fc, size = unpack("<II", word, pairs_offset + 2 + 33 * 8)
    if not 0 < size <= len(table) or fc + size > len(table):
        raise SchemaError("Invalid DOC CLX range")
    clx = table[fc:fc + size]
    offset = 0
    while offset < len(clx) and clx[offset] == 1:
        n, = unpack("<H", clx, offset + 1)
        offset += 3 + n
    if offset >= len(clx) or clx[offset] != 2:
        raise SchemaError("DOC piece table missing")
    plen, = unpack("<I", clx, offset + 1)
    plc = clx[offset + 5:offset + 5 + plen]
    if len(plc) != plen or plen < 16 or (plen - 4) % 12:
        raise SchemaError("Invalid DOC PLC dimensions")
    count = (plen - 4) // 12
    positions = unpack("<" + "I" * (count + 1), plc, 0)
    if positions[0] != 0 or positions[-1] < main_chars:
        raise SchemaError("DOC piece character coverage missing")
    chunks = []
    for i in range(count):
        length = positions[i + 1] - positions[i]
        if length < 0 or length > 2_000_000:
            raise SchemaError("Invalid DOC character positions")
        fc_raw, = unpack("<I", plc, 4 * (count + 1) + i * 8 + 2)
        compressed = bool(fc_raw & 0x40000000)
        position = fc_raw & 0x3FFFFFFF
        if compressed:
            if position % 2:
                raise SchemaError("Invalid compressed DOC offset")
            position //= 2
        byte_length = length * (1 if compressed else 2)
        if position + byte_length > len(word):
            raise SchemaError("DOC text piece outside stream")
        chunks.append(word[position:position + byte_length].decode("cp1252" if compressed else "utf-16le"))
    return "".join(chunks)[:main_chars]


def _read_doc_text(raw: bytes) -> str:
    import olefile
    if not raw.startswith(b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1"):
        raise SchemaError("Attachment is not a Word binary document")
    with olefile.OleFileIO(BytesIO(raw), write_mode=False) as ole:
        word = ole.openstream("WordDocument").read()
        if len(word) < 12 or len(word) > 5_000_000:
            raise SchemaError("Unexpected DOC text stream size")
        table_name = "1Table" if int.from_bytes(word[10:12], "little") & 0x200 else "0Table"
        table = ole.openstream(table_name).read()
    return _doc_piece_text(word, table)



def parse_remittance_doc(raw: bytes, period: date, released: date, source_url: str) -> list[dict]:
    # MS-DOC table cells end with BEL; row end is an additional BEL.
    text = _read_doc_text(raw)
    rows = [[cell.replace("\r", "\n").strip() for cell in row.split("\x07")]
            for row in text.split("\x07\x07")]
    tables = [rows[i:i + 4] for i in range(max(0, len(rows) - 3))]
    return remittance_points(tables, period, released, source_url, "DOC (read-only FIB/CLX)")
