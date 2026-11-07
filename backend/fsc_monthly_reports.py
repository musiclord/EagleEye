"""Bounded FSC/SFB monthly-report discovery and ODS parsing.

HTTP, raw captures and checkpoints belong to the engine. Every announcement and
archive URL comes from source HTML; neither filenames nor release dates are guessed.
Monthly revised history is never relabelled as daily or point-in-time history.
"""
from __future__ import annotations

import calendar
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
import hashlib
from html.parser import HTMLParser
import io
import re
from urllib.parse import urljoin, urlsplit, parse_qs, unquote, urlencode
import xml.etree.ElementTree as ET
import zipfile
import zlib

from .connectors import Source, SchemaError
from .contracts import Job, make_point

INDEX_URL = 'https://www.sfb.gov.tw/ch/home.jsp?id=622&parentpath=0,4,109'
LICENSE_EVIDENCE_URL = 'https://www.sfb.gov.tw/ch/main.jsp?dataserno=201605130008&websitelink=artsublink.jsp'
PARSER_VERSION = 'fsc-monthly-ods-3'
MAX_LISTING_PAGES = 12
MAX_REPORTS_PER_RUN = 4
MAX_BACKFILL_MONTHS = 36
MAX_HTML_BYTES = 1_000_000
MAX_ARCHIVE_BYTES = 6_000_000
ARCHIVE_REFRESH_HOURS = 31 * 24
TAIPEI = timezone(timedelta(hours=8))


def _today():
    return datetime.now(TAIPEI).date()
SOURCES = (Source('fsc_monthly_ods', '證期局：上市市場月度法人、交易結構及融資', INDEX_URL,
    '官方月報ODS，月頻且保留各表口徑。外資與自營商有重疊，不加總為三大法人；'
    '不是日頻資料。原表含年合計與滾動月份，本模組只輸出逐月觀測；歷史是當前修訂版本。',
    'monthly', 65, public_years=10,
    attribution='金融監督管理委員會證券期貨局 {year}〈證券暨期貨市場重要指標〉表14、17、19',
    license_url=LICENSE_EVIDENCE_URL, content_revisions_only=True),)
SERIES = {
    'fsc_twse_monthly_turnover_t14': ('上市市場月成交金額（證期局表14）', 'TWD'),
    'fsc_twse_monthly_margin_balance': ('上市月末融資餘額（證期局）', 'TWD'),
    'fsc_twse_monthly_short_balance': ('上市月末融券餘額（證期局）', 'million_shares'),
}
for _group, _label in (('domestic_juridical', '本國法人'), ('foreign_juridical', '僑外法人'),
                       ('domestic_individual', '本國自然人'), ('foreign_individual', '外國自然人')):
    SERIES[f'fsc_twse_monthly_{_group}_two_sided'] = (f'上市{_label}月買賣合計', 'TWD')
    SERIES[f'fsc_twse_monthly_{_group}_share'] = (f'上市{_label}月雙邊成交占比（官方）', '%')
SERIES['fsc_twse_monthly_foreign_two_sided_share_derived'] = ('上市僑外法人及外國自然人月雙邊成交占比（計算）', '%')
for _group, _label in (('trust', '投信'), ('dealer', '自營商'), ('foreign', '外資')):
    SERIES[f'fsc_twse_monthly_{_group}_net_t19'] = (f'上市{_label}月內買賣超（證期局表19）', 'TWD')
    SERIES[f'fsc_twse_monthly_{_group}_share_t19'] = (f'上市{_label}月成交比重（證期局表19）', '%')

NS = {'table': 'urn:oasis:names:tc:opendocument:xmlns:table:1.0',
      'text': 'urn:oasis:names:tc:opendocument:xmlns:text:1.0',
      'office': 'urn:oasis:names:tc:opendocument:xmlns:office:1.0'}
MONTHS = {x: i for i, x in enumerate(('Jan.', 'Feb.', 'Mar.', 'Apr.', 'May', 'Jun.',
                                    'Jul.', 'Aug.', 'Sep.', 'Oct.', 'Nov.', 'Dec.'), 1)}
TITLES = {14: '上市證券信用交易值分析表', 17: '集中交易市場成交金額投資人類別比例表',
          19: '集中交易市場投資法人交易概況表'}


@dataclass(frozen=True)
class Report:
    month: date
    announcement_date: date
    page_url: str
    archive_url: str | None = None


class _HTML(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.links = []
        self.anchor = None
        self.text = []
        self.dates = []
        self.date_depth = 0
        self.date_text = []

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if tag == 'a':
            self.anchor = [attrs.get('href', '').strip(), attrs.get('title', ''), []]
        if tag == 'div':
            if self.date_depth:
                self.date_depth += 1
            elif attrs.get('class') == 'contentdate':
                self.date_depth = 1
                self.date_text = []

    def handle_data(self, data):
        self.text.append(data)
        if self.anchor is not None:
            self.anchor[2].append(data)
        if self.date_depth:
            self.date_text.append(data)

    def handle_endtag(self, tag):
        if tag == 'a' and self.anchor is not None:
            href, title, text = self.anchor
            self.links.append((href, title, ''.join(text).strip()))
            self.anchor = None
        if tag == 'div' and self.date_depth:
            self.date_depth -= 1
            if not self.date_depth:
                self.dates.append(''.join(self.date_text).strip())


def _html(raw):
    try:
        parser = _HTML()
        parser.feed(raw.decode('utf-8-sig'))
        return parser
    except (UnicodeError, ValueError) as exc:
        raise SchemaError('Invalid SFB HTML') from exc


def _report_month(text):
    match = re.fullmatch(r'證券暨期貨市場(\d{3})年(\d{1,2})月份重要指標', text.strip())
    if not match:
        return None
    try:
        return date(int(match[1]) + 1911, int(match[2]), 1)
    except ValueError as exc:
        raise SchemaError('Invalid report month') from exc


def discover_reports(raw: bytes, today: date | None = None) -> list[dict]:
    """Discover only published month/title links from the supplied index page.

    This does not assert that page 1 includes the whole historical archive.
    Announcement dates must be confirmed on each detail page, not dataserno.
    """
    today = today or _today()
    if len(raw) > MAX_HTML_BYTES:
        raise SchemaError('Oversized SFB listing')
    found = {}
    for href, _, title in _html(raw).links:
        month = _report_month(title)
        if month is None:
            continue
        url = urljoin(INDEX_URL, href)
        validate_fetch_url(url)
        parsed = urlsplit(url)
        query = parse_qs(parsed.query)
        if (parsed.scheme != 'https' or parsed.netloc != 'www.sfb.gov.tw' or parsed.path != '/ch/home.jsp'
                or query.get('id') != ['622'] or query.get('mcustomize') != ['important_view.jsp']
                or len(query.get('dataserno', [])) != 1 or not re.fullmatch(r'\d{12}', query['dataserno'][0])):
            raise SchemaError('Unapproved report link')
        if month > today.replace(day=1):
            raise SchemaError('Report month is in the future')
        if month in found:
            raise SchemaError('Duplicate report month')
        found[month] = {'month': month.isoformat()[:7], 'page_url': url}
    if not found:
        raise SchemaError('No monthly report links found')
    return [found[x] for x in sorted(found, reverse=True)]


def discover_archive(raw: bytes, page_url: str, today: date | None = None) -> Report:
    today = today or _today()
    if len(raw) > MAX_HTML_BYTES:
        raise SchemaError('Oversized SFB announcement')
    validate_fetch_url(page_url)
    page = urlsplit(page_url)
    if page.scheme != 'https' or page.netloc != 'www.sfb.gov.tw' or page.path != '/ch/home.jsp':
        raise SchemaError('Unapproved announcement page')
    parser = _html(raw)
    titles = set(re.findall(r'證券暨期貨市場\d{3}年\d{1,2}月份重要指標', ' '.join(parser.text)))
    if len(titles) != 1 or len(parser.dates) != 1:
        raise SchemaError('Ambiguous report title or announcement date')
    month = _report_month(titles.pop())
    try:
        announced = date.fromisoformat(parser.dates[0])
    except ValueError as exc:
        raise SchemaError('Invalid announcement date') from exc
    if announced > today or month > announced.replace(day=1):
        raise SchemaError('Future or inconsistent report dates')
    links = {urljoin(page_url, href) for href, _, text in parser.links if text == '檔案下載'}
    if len(links) != 1:
        raise SchemaError('Missing or ambiguous report archive')
    archive = links.pop()
    validate_fetch_url(archive)
    parsed = urlsplit(archive)
    if (parsed.scheme != 'https' or parsed.netloc != 'www.fsc.gov.tw'
            or not re.fullmatch(r'/userfiles/file/[A-Za-z0-9_\-\u3400-\u9fff]+[.]zip', unquote(parsed.path))
            or parsed.query or parsed.fragment):
        raise SchemaError('Unapproved archive URL')
    return Report(month, announced, page_url, archive)


def _zip(raw, cap=12_000_000):
    try:
        if len(raw) > cap:
            raise SchemaError('Oversized compressed archive')
        archive = zipfile.ZipFile(io.BytesIO(raw))
        infos = archive.infolist()
        if len(infos) > 100 or len({x.filename for x in infos}) != len(infos):
            raise SchemaError('Too many or duplicate archive members')
        if any(x.orig_filename.startswith(('/', '\\')) or '\\' in x.orig_filename
               or any(part in ('.', '..') for part in x.orig_filename.split('/'))
               or ':' in x.orig_filename or (x.external_attr >> 16) & 0o170000 == 0o120000 for x in infos):
            raise SchemaError('Unsafe archive member path or symbolic link')
        if any(x.compress_type not in (zipfile.ZIP_STORED, zipfile.ZIP_DEFLATED) for x in infos):
            raise SchemaError('Unsupported archive compression')
        if sum(x.file_size for x in infos) > cap or any(x.flag_bits & 1 for x in infos):
            raise SchemaError('Oversized or encrypted archive')
        return archive
    except (zipfile.BadZipFile, OSError) as exc:
        raise SchemaError('Invalid ZIP/ODS') from exc


def _zip_read(archive, member):
    try:
        return archive.read(member)
    except (zipfile.BadZipFile, RuntimeError, NotImplementedError, OSError, zlib.error) as exc:
        raise SchemaError('Corrupt or unsupported ZIP member') from exc


def ods_rows(raw: bytes, sheet: str) -> list[list[str]]:
    """Read stored ODS values and labels. Never execute formulas or external links."""
    archive = _zip(raw, 2_000_000)
    try:
        xml = _zip_read(archive, 'content.xml')
        if b'<!DOCTYPE' in xml.upper() or b'<!ENTITY' in xml.upper() or b'\x00' in xml:
            raise SchemaError('XML entities are not permitted')
        root = ET.fromstring(xml)
    except (KeyError, ET.ParseError) as exc:
        raise SchemaError('Missing or invalid ODS content') from exc
    tables = root.findall('.//table:table', NS)
    if len(tables) != 1 or tables[0].get(f'{{{NS["table"]}}}name') != sheet:
        raise SchemaError('Unexpected ODS worksheet')
    rows = []
    for row in tables[0].findall('table:table-row', NS):
        values = []
        for cell in row:
            if cell.tag not in (f'{{{NS["table"]}}}table-cell', f'{{{NS["table"]}}}covered-table-cell'):
                continue
            value = cell.get(f'{{{NS["office"]}}}value')
            if value is None:
                value = ' '.join(''.join(p.itertext()) for p in cell.findall('text:p', NS)).strip()
            repeat = _bounded_repeat(cell.get(f'{{{NS["table"]}}}number-columns-repeated', '1'))
            if repeat < 1 or (value and len(values) + repeat > 32):
                raise SchemaError('Unexpected ODS repeated cell')
            # LibreOffice may encode vast empty trailing areas. Keep only the table width.
            values.extend([value] * min(repeat, max(0, 32 - len(values))))
        while values and not values[-1]:
            values.pop()
        if not values:
            continue
        repeat = _bounded_repeat(row.get(f'{{{NS["table"]}}}number-rows-repeated', '1'))
        if repeat != 1 or len(rows) >= 200:
            raise SchemaError('Unexpected ODS repeated row or size')
        rows.append(values)
    return rows


def _bounded_repeat(value):
    if not re.fullmatch(r'[0-9]{1,7}', value):
        raise SchemaError('Invalid ODS repeat count')
    return int(value)


def _decimal(text):
    try:
        value = Decimal(text)
    except (InvalidOperation, TypeError) as exc:
        raise SchemaError('Missing or invalid numeric cell') from exc
    if not value.is_finite():
        raise SchemaError('Nonfinite numeric cell')
    return value


def _monthly_rows(rows, report, width):
    year, months, annual = None, {}, {}
    for row in rows:
        label = row[0]
        if re.fullmatch(r'20\d{2}|19\d{2}', label):
            year = int(label)
            if len(row) != width:
                raise SchemaError('Annual row width changed')
            if year in annual:
                raise SchemaError('Duplicate annual anchor')
            annual[year] = row
        elif label in MONTHS:
            if year is None or len(row) != width:
                raise SchemaError('Month missing year anchor or wrong width')
            month = date(year, MONTHS[label], 1)
            if month > report.month or month in months:
                raise SchemaError('Future or duplicate observation month')
            months[month] = row
    if not months or max(months) != report.month:
        raise SchemaError('Report does not contain its stated latest month')
    serial = report.month.year * 12 + report.month.month - 1
    expected = {date(n // 12, n % 12 + 1, 1) for n in range(serial - 11, serial + 1)}
    if set(months) != expected:
        raise SchemaError('Expected twelve consecutive monthly rows')
    current = {date(report.month.year, m, 1) for m in range(1, report.month.month + 1)}
    if not current.issubset(months) or report.month.year not in annual:
        raise SchemaError('Current-year monthly coverage is incomplete')
    return months, annual


def parse_report_tables(tables: dict[int, bytes], report: Report, today: date | None = None) -> list[dict]:
    """Parse t14/t17/t19; retain original units and overlapping populations.

    The API's word '累計' is reconciled against the source annual/YTD row:
    month rows are period net flows, not YTD amounts to difference again.
    """
    today = today or _today()
    if report.month.day != 1 or report.announcement_date > today or report.month > report.announcement_date.replace(day=1):
        raise SchemaError('Invalid report context dates')
    if set(tables) != {14, 17, 19}:
        raise SchemaError('Exactly t14, t17 and t19 are required')
    parsed = {}
    required = {14: ('總成交值', '融資餘額', '百萬股'),
                17: ('本國法人', '僑外法人', '外國自然人', '買賣合計', 'T+4'),
                19: ('累計買賣超金額', '交易金額比重', '外國證券商在台分公司')}
    for table, raw in tables.items():
        rows = ods_rows(raw, f'表{table}')
        joined = re.sub(r'\s+', '', ' '.join(' '.join(row) for row in rows))
        if TITLES[table] not in joined or not all(re.sub(r'\s+', '', x) in joined for x in required[table]):
            raise SchemaError('Source title, units or definitions changed')
        if ('10億元' not in joined and '十億元' not in joined):
            raise SchemaError('Missing original monetary unit')
        if table == 14:
            header = next((r for r in rows if r[0] == '年'), [])
            expected = {1: '總成交值', 11: '融資餘額', 12: '融券餘額'}
        elif table == 17:
            header = next((r for r in rows if r[0] == '年 Year'), [])
            expected = {1: '本國法人', 3: '僑外法人', 5: '本國自然人', 7: '外國自然人'}
        else:
            header = next((r for r in rows if len(r) > 1 and 'Securities Investment Trust Funds' in r[1]), [])
            expected = {1: '投信', 2: '自營商', 3: '外資', 4: '投信', 5: '自營商', 6: '外資'}
        if any(i >= len(header) or not re.sub(r'\s+', '', header[i]).startswith(label) for i, label in expected.items()):
            raise SchemaError('Source column order changed')
        parsed[table] = _monthly_rows(rows, report, {14: 13, 17: 9, 19: 7}[table])
    if len({tuple(sorted(value[0])) for value in parsed.values()}) != 1:
        raise SchemaError('Tables have different month coverage')
    # Verify t19 month amounts sum to the current year row. Never sum its overlapping investor categories.
    t19, totals = parsed[19]
    for column in (1, 2, 3):
        monthly_sum = sum((_decimal(row[column]) for day, row in t19.items() if day.year == report.month.year), Decimal(0))
        if abs(monthly_sum - _decimal(totals[report.month.year][column])) > Decimal('0.00001'):
            raise SchemaError('Monthly net amounts do not reconcile to report YTD row')
    output = []
    for month in sorted(t19):
        end = date(month.year, month.month, calendar.monthrange(month.year, month.month)[1])
        def emit(table, sid, original, factor=Decimal(1), **extra):
            value = _decimal(original)
            metadata = {
                'frequency': 'monthly', 'market': 'TWSE', 'period_start': month.isoformat(),
                'period_end': end.isoformat(), 'observation_date_basis': 'calendar_month_end',
                'report_month': report.month.isoformat()[:7], 'announcement_date': report.announcement_date.isoformat(),
                'release_date_known': False, 'vintage': 'latest_available_revision',
                'archive_revision_date_known': False, 'original_unit': 'NTD_billion' if factor == Decimal('1e9') else SERIES[sid][1],
                'original_value': original, 'source_table': f't{table}', 'source_url': report.archive_url,
                'announcement_url': report.page_url, 'member_sha256': hashlib.sha256(tables[table]).hexdigest(),
                'license': 'OGDL-Taiwan-1.0', 'license_evidence_url': LICENSE_EVIDENCE_URL,
                'classification': 'observed', 'seasonally_adjusted': False, **extra,
            }
            output.append(make_point(SERIES, sid, end.isoformat(), float(value * factor),
                                     status=metadata['classification'], metadata=metadata))
        row = parsed[14][0][month]
        for col in (1, 11, 12):
            if _decimal(row[col]) < 0:
                raise SchemaError('Negative turnover or balance')
        emit(14, 'fsc_twse_monthly_turnover_t14', row[1], Decimal('1e9'), measure='flow_single_sided')
        emit(14, 'fsc_twse_monthly_margin_balance', row[11], Decimal('1e9'), measure='month_end_stock')
        emit(14, 'fsc_twse_monthly_short_balance', row[12], measure='month_end_stock', original_unit='million_shares')
        row = parsed[17][0][month]
        amounts = [_decimal(row[i]) for i in (1, 3, 5, 7)]
        percentages = [_decimal(row[i]) for i in (2, 4, 6, 8)]
        denominator = sum(amounts)
        if denominator <= 0 or min(amounts) < 0 or any(x < 0 or x > 100 for x in percentages):
            raise SchemaError('Invalid investor amounts or percentages')
        deviations = [abs(a / denominator * 100 - p) for a, p in zip(amounts, percentages)]
        # The original 2025-09 report has three monthly rows slightly outside
        # 0.05 pp rounding (max 0.053101 pp). Preserve and flag, never relabel it
        # as exact rounding. A discrepancy over one displayed step is rejected.
        if max(deviations) > Decimal('0.1000001'):
            raise SchemaError('Investor share is inconsistent with two-sided denominator')
        consistency = {
            'published_percentage_max_residual_pp': str(max(deviations)),
            'published_percentage_reconciliation': ('outside_display_rounding'
                if max(deviations) > Decimal('0.0500001') else 'within_display_rounding'),
            'parser_reconciliation_limit_pp': 0.1000001,
        }
        for i, group in enumerate(('domestic_juridical', 'foreign_juridical', 'domestic_individual', 'foreign_individual')):
            meta = {'universe': 'all_securities_excluding_auction_and_bidding', 'measure': 'flow_buy_plus_sell',
                    'adjustment': 'T+4_adjusted_since_2016_10', 'denominator_ntd_billion': str(denominator), **consistency}
            emit(17, f'fsc_twse_monthly_{group}_two_sided', row[1+i*2], Decimal('1e9'), **meta)
            emit(17, f'fsc_twse_monthly_{group}_share', row[2+i*2], **{**meta, 'measure': 'share_buy_plus_sell'})
        emit(17, 'fsc_twse_monthly_foreign_two_sided_share_derived', str((amounts[1] + amounts[3]) / denominator * 100),
             classification='derived', original_unit='%', measure='share_buy_plus_sell',
             formula='100*(foreign_juridical+foreign_individual)/(sum_four_investor_categories)',
             denominator_ntd_billion=str(denominator), universe='all_securities_excluding_auction_and_bidding',
             adjustment='T+4_adjusted_since_2016_10', not_equivalent_to_t19_foreign=True, **consistency)
        row = parsed[19][0][month]
        for i, group in enumerate(('trust', 'dealer', 'foreign')):
            meta = {'measure': 'period_net_flow', 'source_header': '累計買賣超金額',
                    'period_semantics': 'month_sum_reconciled_with_current_year_total',
                    'investor_groups_overlap': True, 'aggregate_three_institutions_allowed': False,
                    'overlap_note': 'dealer_and_foreign_both_include_Taiwan_branches_of_foreign_securities_firms'}
            emit(19, f'fsc_twse_monthly_{group}_net_t19', row[1+i], Decimal('1e9'), **meta)
            share = _decimal(row[4+i])
            if not 0 <= share <= 100:
                raise SchemaError('Invalid t19 participation share')
            emit(19, f'fsc_twse_monthly_{group}_share_t19', row[4+i], **{**meta, 'measure': 'reported_trading_share'})
    return output


def parse_report_archive(raw: bytes, report: Report, today: date | None = None) -> list[dict]:
    """Read the small official outer ZIP + ODF ZIP; no extraction to filesystem."""
    archive = _zip(raw)
    odf = [x for x in archive.namelist() if x.lower().endswith('odf.zip')]
    if len(odf) != 1:
        raise SchemaError('Missing or ambiguous ODF archive')
    prefix = f'{report.month.year - 1911}{report.month.month:02d}'
    if not odf[0].startswith(prefix):
        raise SchemaError('Mismatched archive report month')
    inner = _zip(_zip_read(archive, odf[0]))
    tables = {}
    for info in inner.infolist():
        name = info.filename
        if not info.flag_bits & 0x800:
            # Older SFB ZIPs store Big5/CP950 filenames without a Unicode flag.
            try:
                name = name.encode('cp437').decode('cp950')
            except (UnicodeEncodeError, UnicodeDecodeError):
                raise SchemaError('Unsupported archive filename encoding')
        match = re.search(r'(?:^|/)t(14|17|19)' + r'([^/]+)[.]ods$', name)
        if match and match[2].startswith(TITLES[int(match[1])]):
            table = int(match[1])
            if ('/' in name and not name.startswith(prefix)) or table in tables:
                raise SchemaError('Duplicate table or mismatched archive report month')
            tables[table] = _zip_read(inner, info)
    return parse_report_tables(tables, report, today=today)



def validate_fetch_url(url: str) -> None:
    """Exact HTTPS source families, also applied before following redirects."""
    parsed = urlsplit(url)
    if parsed.scheme != 'https' or parsed.fragment or parsed.netloc not in {'www.sfb.gov.tw', 'www.fsc.gov.tw'}:
        raise SchemaError('Unapproved monthly-report URL')
    if parsed.netloc == 'www.fsc.gov.tw':
        if parsed.query or not re.fullmatch(r'/userfiles/file/[A-Za-z0-9_\-\u3400-\u9fff]+[.]zip', unquote(parsed.path)):
            raise SchemaError('Unapproved archive URL')
        return
    if parsed.path != '/ch/home.jsp':
        raise SchemaError('Unapproved SFB path')
    query = parse_qs(parsed.query, keep_blank_values=True)
    # The real listing form uses POST home.jsp with hidden fields in its body.
    if not query:
        return
    if any(len(v) != 1 for v in query.values()) or set(query) - {'id', 'parentpath', 'mcustomize', 'dataserno'}:
        raise SchemaError('Unapproved SFB query')
    if query.get('id') != ['622'] or query.get('parentpath', ['0,4,109']) != ['0,4,109']:
        raise SchemaError('Unapproved SFB report collection')
    custom = query.get('mcustomize', [''])[0]
    if custom not in ('', 'important_list.jsp', 'important_view.jsp'):
        raise SchemaError('Unapproved SFB report view')
    serial = query.get('dataserno')
    if custom == 'important_view.jsp':
        if serial is None or not re.fullmatch(r'\d{12}', serial[0]):
            raise SchemaError('Unapproved SFB announcement identifier')
    elif serial is not None:
        raise SchemaError('Unexpected announcement identifier on listing')


class _ListingHTML(_HTML):
    def __init__(self):
        super().__init__()
        self.forms = []
        self.form = None
        self.page_text = []
        self.in_page = False
        self.page_options = set()
        self.in_page_select = False

    def handle_starttag(self, tag, attrs):
        super().handle_starttag(tag, attrs)
        attrs = dict(attrs)
        if tag == 'form':
            self.form = {'name': attrs.get('name'), 'action': attrs.get('action', ''),
                         'method': attrs.get('method', '').lower(), 'fields': {}}
        elif tag == 'input' and self.form is not None and attrs.get('type') == 'hidden':
            name = attrs.get('name', '')
            if name in self.form['fields']:
                raise SchemaError('Duplicate listing form field')
            self.form['fields'][name] = attrs.get('value', '')
        elif tag == 'span' and 'page' in attrs.get('class', '').split():
            self.in_page = True
        elif tag == 'select' and attrs.get('name') == 'page':
            self.in_page_select = True
        elif tag == 'option' and self.in_page_select:
            value = attrs.get('value', '')
            if re.fullmatch(r'[0-9]{1,4}', value):
                self.page_options.add(int(value))

    def handle_data(self, data):
        super().handle_data(data)
        if self.in_page:
            self.page_text.append(data)

    def handle_endtag(self, tag):
        super().handle_endtag(tag)
        if tag == 'form' and self.form is not None:
            self.forms.append(self.form)
            self.form = None
        elif tag == 'span':
            self.in_page = False
        elif tag == 'select':
            self.in_page_select = False


def discover_next_listing(raw: bytes, current_page: int) -> tuple[str, bytes] | None:
    """Use the actual HTML form and an advertised next-page option; no JS execution."""
    parser = _ListingHTML()
    parser.feed(raw.decode('utf-8-sig'))
    page_text = ''.join(parser.page_text).strip()
    if not page_text:
        return None
    match = re.fullmatch(r'(\d{1,4})\s*/\s*(\d{1,4})', page_text)
    if not match or int(match[1]) != current_page or not current_page <= int(match[2]):
        raise SchemaError('Unexpected listing pagination')
    if current_page == int(match[2]):
        return None
    advertised = {int(m[1]) for href, _, _ in parser.links
                  if (m := re.fullmatch(r'javascript:list\((\d{1,4})\)', href))}
    if current_page + 1 not in advertised | parser.page_options:
        raise SchemaError('Next listing page is not advertised')
    forms = [x for x in parser.forms if x['name'] == 'bstform']
    if len(forms) != 1 or forms[0]['method'] != 'post':
        raise SchemaError('Missing official listing form')
    form = forms[0]
    fields = form['fields']
    expected = {'id': '622', 'contentid': '622', 'parentpath': '0,4,109',
                'mcustomize': 'important_list.jsp'}
    if any(fields.get(k) != v for k, v in expected.items()) or set(fields) - set(expected) - {'dateid', 'page', 'pagesize'}:
        raise SchemaError('Changed listing form fields')
    if any(fields.get(k, '') for k in ('dateid', 'page', 'pagesize')):
        raise SchemaError('Unexpected filtered listing form')
    url = urljoin(INDEX_URL, form['action'])
    validate_fetch_url(url)
    if urlsplit(url).netloc != 'www.sfb.gov.tw':
        raise SchemaError('Unapproved listing form destination')
    return url, urlencode({**fields, 'page': str(current_page + 1)}).encode('ascii')


def _month_shift(month: date, count: int) -> date:
    n = month.year * 12 + month.month - 1 + count
    return date(n // 12, n % 12 + 1, 1)


def _capture(url: str, raw: bytes, role: str, method='GET', body=None) -> dict:
    result = {'url': url, 'sha256': hashlib.sha256(raw).hexdigest(), 'bytes': len(raw),
              'role': role, 'method': method}
    if body is not None:
        result['request_body_sha256'] = hashlib.sha256(body).hexdigest()
    return result


def _report_job(found, lower, upper, listing_captures, today, store=None, collection_scope=None):
    month = date.fromisoformat(found['month'] + '-01')
    url = found['page_url']
    scope = f'{lower:%Y-%m}:{upper:%Y-%m}'
    def children(raw):
        report = discover_archive(raw, url, today)
        if report.month != month:
            raise SchemaError('Listing and announcement months disagree')
        captures = [*listing_captures, _capture(url, raw, 'announcement')]
        def parse_archive(payload):
            points = parse_report_archive(payload, report, today)
            evidence = [*captures, _capture(report.archive_url, payload, 'archive')]
            # Historical backfill must not replace a newer report vintage already stored.
            newer = {}
            if store is not None:
                for sid in SERIES:
                    for point in store.observations(sid):
                        if point.get('source_id') != SOURCES[0].id:
                            continue
                        meta = point.get('metadata') or {}
                        newer[(sid, point['observation_date'])] = meta.get('report_month', '')
            output = []
            for point in points:
                observed_month = date.fromisoformat(point['observation_date']).replace(day=1)
                if not lower <= observed_month <= upper:
                    continue
                if newer.get((point['series_id'], point['observation_date']), '') > found['month']:
                    continue
                point['metadata'].update({'provenance_chain': evidence,
                    'archive_sha256': evidence[-1]['sha256'], 'parser_version': PARSER_VERSION,
                    'history_coverage': 'rolling_monthly_reports_not_point_in_time',
                    'same_url_revision_recheck_days': 31,
                    'collection_scope': collection_scope or {}})
                output.append(point)
            return output
        key_hash = hashlib.sha256(report.archive_url.encode()).hexdigest()[:16]
        return [Job(f'fsc_monthly_ods:archive:{month:%Y-%m}:{key_hash}:{scope}', SOURCES[0].id,
                    report.archive_url, parse_archive, PARSER_VERSION, ARCHIVE_REFRESH_HOURS,
                    max_bytes=MAX_ARCHIVE_BYTES, parse_scope=scope, empty_ok=True)]
    return Job(f'fsc_monthly_ods:announcement:{month:%Y-%m}:{scope}', SOURCES[0].id, url,
               lambda raw: [], PARSER_VERSION, 24, max_bytes=MAX_HTML_BYTES,
               followups=children, parse_scope=scope)


def _plan(start: date, end: date, *, mode='backfill', complete_months=frozenset(), store=None):
    if start > end:
        raise ValueError('Invalid monthly report interval')
    lower, upper = start.replace(day=1), end.replace(day=1)
    months = (upper.year - lower.year) * 12 + upper.month - lower.month + 1
    limited = mode != 'incremental' and months > MAX_BACKFILL_MONTHS
    if limited:
        lower = _month_shift(upper, -(MAX_BACKFILL_MONTHS - 1))
    if mode not in ('incremental', 'backfill', 'retry_failed'):
        raise ValueError('Unknown monthly report update mode')
    today = _today()
    def listing_job(page=1, url=INDEX_URL, body=None, cursor=None, target_lower=None,
                    used=0, previous_oldest=None, ancestors=(), scope_context=None):
        def children(raw):
            reports = discover_reports(raw, today)
            newest = date.fromisoformat(reports[0]['month'] + '-01')
            oldest = date.fromisoformat(reports[-1]['month'] + '-01')
            if previous_oldest is not None and newest >= previous_oldest:
                raise SchemaError('Listing pagination repeated or overlapped months')
            wanted_upper = cursor
            wanted_lower = target_lower
            if wanted_upper is None:
                wanted_upper = min(upper, newest)
                wanted_lower = lower
                if mode == 'incremental':
                    expected = {_month_shift(wanted_upper, -i) for i in range(24)}
                    missing = expected - set(complete_months)
                    wanted_lower = min(missing | {_month_shift(wanted_upper, -11)})
            collection_scope = scope_context or {'mode': mode, 'requested_start': start.isoformat(), 'requested_end': end.isoformat(),
                                'effective_start': wanted_lower.isoformat(),
                                'effective_end': min(upper, newest).isoformat()[:7],
                                'coverage_limited': limited, 'max_backfill_months': MAX_BACKFILL_MONTHS,
                                'scope_policy': ('initialize_24_months_then_recheck_rolling_12_months'
                                                 if mode == 'incremental' else 'bounded_requested_months')}
            captures = (*ancestors, _capture(url, raw, 'listing', 'POST' if body else 'GET', body))
            planned = []
            while wanted_upper >= wanted_lower:
                eligible = [x for x in reports if _month_shift(date.fromisoformat(x['month'] + '-01'), -11)
                            <= wanted_upper <= date.fromisoformat(x['month'] + '-01')]
                if not eligible:
                    break
                # Prefer a report ending at the cursor: each download adds up to 12 months.
                # The exact fetched report vintage remains explicit on every point.
                chosen = eligible[-1]
                report_month = date.fromisoformat(chosen['month'] + '-01')
                segment_lower = max(wanted_lower, _month_shift(report_month, -11))
                used_here = used + len(planned) + 1
                if used_here > MAX_REPORTS_PER_RUN:
                    raise SchemaError('Monthly report download bound reached; narrow backfill interval')
                planned.append(_report_job(chosen, segment_lower, wanted_upper, captures, today, store, collection_scope))
                wanted_upper = _month_shift(segment_lower, -1)
            if wanted_upper >= wanted_lower:
                if page >= MAX_LISTING_PAGES:
                    raise SchemaError('Monthly listing page bound reached; requested history remains incomplete')
                next_page = discover_next_listing(raw, page)
                if next_page is None:
                    raise SchemaError('Requested monthly history is not covered by discovered reports')
                next_url, next_body = next_page
                planned.append(listing_job(page + 1, next_url, next_body, wanted_upper, wanted_lower,
                                           used + len(planned), oldest, captures, collection_scope))
            return planned
        scope = f'{mode}:{lower:%Y-%m}:{upper:%Y-%m}'
        # Keep parent keys stable across partial initialization. Followups run for cached HTML too.
        return Job(f'fsc_monthly_ods:listing:{page}:{scope}', SOURCES[0].id, url,
                   lambda raw: [], PARSER_VERSION, 24, method='POST' if body else 'GET', body=body,
                   headers={'Content-Type': 'application/x-www-form-urlencoded'} if body else {},
                   max_bytes=MAX_HTML_BYTES, followups=children, parse_scope=scope)
    return [listing_job()]


def jobs(start: date, end: date) -> list[Job]:
    """Standalone bounded backfill; normal engine updates use store_jobs below."""
    return _plan(start, end)


def store_jobs(store, start: date, end: date, mode='incremental') -> list[Job]:
    """Initialize missing rolling 24 months, then recheck the latest report monthly."""
    coverage = []
    for sid in SERIES:
        coverage.append({date.fromisoformat(x['observation_date']).replace(day=1)
                         for x in store.observations(sid) if x.get('source_id') == SOURCES[0].id})
    complete = set.intersection(*coverage) if coverage else set()
    return _plan(start, end, mode=mode, complete_months=complete, store=store)
