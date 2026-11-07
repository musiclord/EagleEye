"""Official Taiwan policy/market rates, CPI and regular wages.

No HTTP or database writes occur here. Event dates, banking days and monthly
period ends remain distinct. Real wages are explicitly local calculations from
same-period official nominal regular wages and the 2021-base all-items CPI.
"""
from __future__ import annotations

import calendar
import io
import re
import xml.etree.ElementTree as ET
from datetime import date
from decimal import Decimal
from functools import partial
from html.parser import HTMLParser

from .connectors import Source, SchemaError, number
from .contracts import Job, make_point

POLICY_URL = 'https://www.cbc.gov.tw/tw/lp-640-1-1-20.html'
OVERNIGHT_URL = 'https://www.cbc.gov.tw/tw/lp-641-1-{page}-60.html'
CPI_URL = 'https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/230555/pr0101a1m.xml'
WAGE_URL = 'https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/230037/mp05002.xml'
METHOD_URL = 'https://earnings.dgbas.gov.tw/replies.aspx'
MODEL_VERSION = 'taiwan-macro-2'
CPI_ITEM = '總指數(指數基期：民國110年=100)'
WAGE_FIELD = '工業及服務業__Industry_and_services_金額_新臺幣元'
WAGE_SCOPE = 'industry_and_services_all_employees'
CPI_SCOPE = 'taiwan_all_items_consumer_price_index'

SOURCES = (
    Source('cbc_policy_rate', '中央銀行：重貼現率調整事件', POLICY_URL,
           '政策利率；觀測日期是生效日，不是每日成交或公告日。最近一次調整仍有效不表示資料過期；需另看成功擷取日期。', 'event', 14),
    Source('cbc_overnight_rate', '中央銀行：金融業隔夜拆款利率', OVERNIGHT_URL.format(page=1),
           '市場隔夜拆款加權平均利率，年率百分比；依實際公布銀行營業日保留，不以台股交易日補值。', 'daily', 5),
    Source('dgbas_cpi', '主計總處：消費者物價總指數及年增率', CPI_URL,
           '月資料，2021年=100、未季調；總指數與官方發布年增率。歷史為最新修訂版本，逐期發布日未知。', 'monthly', 50),
    Source('dgbas_regular_wage', '主計總處：全體受僱員工經常性薪資', WAGE_URL,
           '工業及服務業全體受僱員工，每人每月名目平均、新臺幣元、未季調；含部分工時與外籍，不是本國籍全時、中位數或含年終獎金總薪資。', 'monthly', 100),
)
DERIVED_SOURCE = Source('taiwan_macro_derived', '本地計算：實質經常性薪資及薪資年增率', METHOD_URL,
    '同月名目經常性薪資除以2021基年CPI乘100。使用公開四捨五入值，非直接發布的實質薪資序列；只取同月交集與去年同月，不補資料。', 'monthly', 100)

SERIES = {
    'tw_policy_rate': ('央行重貼現率（政策調整事件）', '%'),
    'tw_overnight_rate': ('金融業隔夜拆款利率（市場年率）', '%'),
    'tw_cpi': ('消費者物價總指數（2021=100，未季調）', 'index'),
    'tw_cpi_yoy': ('消費者物價總指數年增率（官方）', '%'),
    'tw_regular_wage': ('全體受僱員工經常性薪資（名目平均）', 'TWD'),
    'tw_regular_wage_yoy': ('全體受僱員工名目經常性薪資年增率', '%'),
    'tw_real_regular_wage': ('實質經常性薪資（2021年價格，本地計算）', 'TWD'),
    'tw_real_regular_wage_yoy': ('實質經常性薪資年增率（本地計算）', '%'),
}


def _month_end(year: int, month: int) -> date:
    try:
        return date(year, month, calendar.monthrange(year, month)[1])
    except (ValueError, OverflowError) as exc:
        raise SchemaError('Invalid reporting month') from exc


def _monthly(day: date, **extra) -> dict:
    return {'frequency': 'monthly', 'period_start': day.replace(day=1).isoformat(),
            'period_end': day.isoformat(), 'seasonally_adjusted': False,
            'release_date_known': False, 'vintage': 'latest_available_revision', **extra}


class _Tables(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.tables, self.table, self.row, self.cell = [], None, None, None

    def handle_starttag(self, tag, attrs):
        if tag == 'table':
            if self.table is not None:
                raise SchemaError('Unexpected nested source table')
            self.table = []
        elif tag == 'tr' and self.table is not None:
            self.row = []
        elif tag in ('th', 'td') and self.row is not None:
            self.cell = []

    def handle_data(self, data):
        if self.cell is not None:
            self.cell.append(data)

    def handle_endtag(self, tag):
        if tag in ('th', 'td') and self.cell is not None:
            self.row.append(re.sub(r'\s+', '', ''.join(self.cell)))
            self.cell = None
        elif tag == 'tr' and self.row is not None:
            if self.row:
                self.table.append(self.row)
            self.row = None
        elif tag == 'table' and self.table is not None:
            self.tables.append(self.table)
            self.table = None


def _table(raw: bytes, headers: list[str]) -> tuple[str, list[list[str]]]:
    try:
        html = raw.decode('utf-8-sig')
        parser = _Tables()
        parser.feed(html)
    except (UnicodeError, ValueError) as exc:
        raise SchemaError('Invalid CBC HTML') from exc
    matches = [table[1:] for table in parser.tables if table and table[0] == headers]
    if len(matches) != 1 or not matches[0]:
        raise SchemaError('CBC table headers changed or table is empty')
    rows = matches[0]
    if any(len(row) != len(headers) for row in rows):
        raise SchemaError('CBC table width changed')
    return html, rows


def _cbc_date(text: str) -> date:
    match = re.fullmatch(r'(\d{4})/(\d{1,2})/(\d{1,2})', text)
    if not match:
        raise SchemaError('CBC observation date changed')
    try:
        return date(*(int(x) for x in match.groups()))
    except ValueError as exc:
        raise SchemaError('Invalid CBC observation date') from exc


def parse_policy(raw: bytes, start: date, end: date) -> list[dict]:
    _, rows = _table(raw, ['調整日期', '重貼現率', '擔保放款融通利率', '短期融通利率'])
    events = [(_cbc_date(row[0]), number(row[1])) for row in rows]
    if len({day for day, _ in events}) != len(events):
        raise SchemaError('Duplicate policy adjustment date')
    events.sort()
    anchors = [day for day, _ in events if day <= start]
    if not anchors:
        raise SchemaError('Policy page does not contain an effective starting anchor')
    anchor = max(anchors)
    output = []
    for index, (day, value) in enumerate(events):
        if not anchor <= day <= end:
            continue
        until = events[index + 1][0].isoformat() if index + 1 < len(events) else None
        output.append(make_point(SERIES, 'tw_policy_rate', day.isoformat(), value,
            status='observed', release_at=None, metadata={
                'frequency': 'event', 'scope': 'central_bank_rediscount_policy_rate',
                'rate_basis': 'annual_percentage', 'effective_from': day.isoformat(),
                'effective_until_exclusive': until, 'validity_checked_through': end.isoformat(),
                'is_starting_anchor': day == anchor, 'release_date_known': False,
                'display_rule': 'step_after_effective_date; preserve_actual_event_date'}))
    return output


def _overnight_rows(raw: bytes, page: int) -> tuple[list[tuple[date, Decimal]], int]:
    html, rows = _table(raw, ['標題(顯示資料日期)', '利率'])
    pagination = re.search(r'第\s*<span[^>]*>\s*(\d+)\s*/\s*(\d+)\s*</span>\s*頁', html)
    if not pagination or int(pagination[1]) != page:
        raise SchemaError('CBC pagination changed or requested page ignored')
    values = [(_cbc_date(row[0]), number(row[1])) for row in rows]
    if len({day for day, _ in values}) != len(values) or any(
            values[i][0] <= values[i+1][0] for i in range(len(values)-1)):
        raise SchemaError('Duplicate or unordered overnight observations')
    return values, int(pagination[2])


def parse_overnight(raw: bytes, start: date, end: date, page: int = 1) -> list[dict]:
    rows, _ = _overnight_rows(raw, page)
    return [make_point(SERIES, 'tw_overnight_rate', day.isoformat(), value,
        status='observed', release_at=None, metadata={
            'frequency': 'daily', 'scope': 'financial_institutions_overnight_call_market',
            'rate_basis': 'annual_percentage', 'aggregation': 'transaction_weighted_average',
            'calendar': 'published_banking_days', 'release_date_known': False})
        for day, value in rows if start <= day <= end]


def _overnight_followups(raw: bytes, start: date, end: date, page: int) -> list[Job]:
    rows, pages = _overnight_rows(raw, page)
    if min(day for day, _ in rows) <= start or page >= pages:
        return []
    if page >= 200:
        raise SchemaError('Overnight pagination exceeds bounded history retrieval')
    return [_overnight_job(start, end, page + 1)]


def _overnight_job(start: date, end: date, page: int) -> Job:
    return Job(f'taiwan:overnight:{start}:{end}:p{page}', 'cbc_overnight_rate',
        OVERNIGHT_URL.format(page=page), partial(parse_overnight, start=start, end=end, page=page),
        parser_version=MODEL_VERSION, refresh_after_hours=24,
        followups=partial(_overnight_followups, start=start, end=end, page=page))


def _xml(raw: bytes, expected_root: str):
    if b'<!DOCTYPE' in raw.upper() or b'<!ENTITY' in raw.upper():
        raise SchemaError('XML entities or DTD are not supported')
    try:
        iterator = ET.iterparse(io.BytesIO(raw), events=('start', 'end'))
        event, root = next(iterator)
        if event != 'start' or root.tag != expected_root:
            raise SchemaError('Official XML root changed')
        yield root, iterator
    except (ET.ParseError, StopIteration) as exc:
        raise SchemaError('Invalid official XML') from exc


def parse_cpi(raw: bytes, start: date, end: date) -> list[dict]:
    selected = {}
    try:
        for root, iterator in _xml(raw, 'DataSet'):
            if root.attrib.get('Sender_NAME') != '行政院主計總處' or root.attrib.get('Tab_NAME') != '消費者物價基本分類指數':
                raise SchemaError('CPI dataset scope changed')
            for event, elem in iterator:
                if event != 'end' or elem.tag != 'Obs':
                    continue
                row = {child.tag: (child.text or '').strip() for child in elem}
                elem.clear()
                if row.get('Item') != CPI_ITEM:
                    # An altered headline base is not compatible with stored real wages.
                    if row.get('Item', '').startswith('總指數('):
                        raise SchemaError('CPI base year changed; explicit migration required')
                    continue
                if row.get('FREQ') != 'M' or row.get('TYPE') not in ('原始值', '年增率(%)'):
                    raise SchemaError('CPI frequency or value type changed')
                match = re.fullmatch(r'(\d{4})M(\d{2})', row.get('TIME_PERIOD', ''))
                if not match:
                    raise SchemaError('Invalid CPI monthly date')
                day = _month_end(int(match[1]), int(match[2]))
                if day > end or day < date(start.year - 1, start.month, 1):
                    continue
                sid = 'tw_cpi' if row['TYPE'] == '原始值' else 'tw_cpi_yoy'
                value = number(row.get('Item_VALUE'))
                if sid == 'tw_cpi' and value <= 0:
                    raise SchemaError('Nonpositive CPI index')
                key = (sid, day)
                if key in selected:
                    raise SchemaError('Duplicate CPI period/type')
                selected[key] = value
    except ET.ParseError as exc:
        raise SchemaError('Invalid CPI XML') from exc
    output = []
    for (sid, day), value in sorted(selected.items(), key=lambda item: (item[0][1], item[0][0])):
        if day < start:
            continue
        meta = _monthly(day, scope=CPI_SCOPE, base_year=2021, index_base=100,
                        price_basis='consumer_price_index', reported_precision_decimals=2)
        if sid == 'tw_cpi_yoy':
            current = selected.get(('tw_cpi', day))
            prior = selected.get(('tw_cpi', _month_end(day.year - 1, day.month)))
            if current is not None and prior is not None:
                residual = value - (current / prior - 1) * 100
                if abs(residual) > Decimal('0.03'):
                    raise SchemaError('Official CPI YoY fails rounded-index reconciliation')
                meta.update(yoy_check_residual_pp=float(residual), yoy_check_tolerance_pp=0.03,
                            yoy_check='rounded_current_vs_same_month_last_year')
        output.append(make_point(SERIES, sid, day.isoformat(), value,
                                 status='observed', release_at=None, metadata=meta))
    if {p['series_id'] for p in output} != {'tw_cpi', 'tw_cpi_yoy'}:
        raise SchemaError('Missing CPI headline index or official YoY in requested history')
    return output


def parse_wages(raw: bytes, start: date, end: date) -> list[dict]:
    output, seen = [], set()
    try:
        for _, iterator in _xml(raw, 'DataCollection'):
            for event, elem in iterator:
                if event != 'end' or elem.tag != '每人每月經常性薪資':
                    continue
                row = {child.tag: (child.text or '').strip() for child in elem}
                elem.clear()
                period = row.get('年月別_Year_and_month', '')
                if re.fullmatch(r'\d{4}[ⓅⓇ]?', period):
                    continue  # annual employee-weighted averages are not monthly rows
                match = re.fullmatch(r'(\d{4})(\d{2})([ⓅⓇ]?)', period)
                if not match:
                    raise SchemaError('Wage period or revision flag changed')
                day = _month_end(int(match[1]), int(match[2]))
                if not start <= day <= end:
                    continue
                if day in seen:
                    raise SchemaError('Duplicate wage month')
                seen.add(day)
                value = number(row.get(WAGE_FIELD))
                if value <= 0:
                    raise SchemaError('Nonpositive regular wage')
                output.append(make_point(SERIES, 'tw_regular_wage', day.isoformat(), value,
                    status='observed', release_at=None, metadata=_monthly(day,
                        scope=WAGE_SCOPE, population='all_employees_including_part_time_and_foreign',
                        geography='Taiwan_excluding_Kinmen_and_Lienchiang',
                        statistic='arithmetic_mean_per_employee_per_month', price_basis='nominal',
                        wage_component='regular_earnings_excluding_overtime_and_nonmonthly_bonus',
                        industry_classification='ROC_industrial_classification_revision_11',
                        benchmark='2021_industry_and_services_census',
                        provisional=match[3] == 'Ⓟ', revised=match[3] == 'Ⓡ',
                        source_revision_marker=match[3] or None)))
    except ET.ParseError as exc:
        raise SchemaError('Invalid wage XML') from exc
    if not output:
        raise SchemaError('Missing monthly industry/services regular wages')
    return sorted(output, key=lambda p: p['observation_date'])


def jobs(start: date, end: date) -> list[Job]:
    if start > end:
        raise ValueError('History start must not exceed end')
    # Six years of level data support five years of same-month YoY, less release lag.
    lower = min(start.replace(day=1), date(end.year - 6, end.month, 1))
    return [
        Job(f'taiwan:policy:{lower}:{end}', 'cbc_policy_rate', POLICY_URL,
            partial(parse_policy, start=lower, end=end), parser_version=MODEL_VERSION,
            refresh_after_hours=24, refresh_at_local='16:30', requested_end=end),
        _overnight_job(start, end, 1),
        Job(f'taiwan:cpi:headline:{lower}:{end}', 'dgbas_cpi', CPI_URL,
            partial(parse_cpi, start=lower, end=end), parser_version=MODEL_VERSION,
            refresh_after_hours=24, max_bytes=25_000_000, refresh_at_local='16:30', requested_end=end),
        Job(f'taiwan:wages:regular:{lower}:{end}', 'dgbas_regular_wage', WAGE_URL,
            partial(parse_wages, start=lower, end=end), parser_version=MODEL_VERSION,
            refresh_after_hours=24, max_bytes=5_000_000, refresh_at_local='16:30', requested_end=end),
    ]


def _indexed(rows: list[dict], sid: str, scope: str) -> dict[str, dict]:
    indexed = {}
    for row in rows:
        if row.get('series_id') != sid or row.get('unit') != SERIES[sid][1]:
            raise SchemaError('Wage derivation input series or unit mismatch')
        day = date.fromisoformat(row['observation_date'])
        if day != _month_end(day.year, day.month) or day.isoformat() in indexed:
            raise SchemaError('Ambiguous or non-month-end derivation input')
        meta = row.get('metadata') or {}
        if meta.get('scope') != scope or meta.get('seasonally_adjusted') is not False:
            raise SchemaError('Wage derivation scope or seasonal adjustment mismatch')
        if sid == 'tw_cpi' and (meta.get('base_year'), meta.get('index_base')) != (2021, 100):
            raise SchemaError('CPI base year must match real wage basis')
        if sid == 'tw_regular_wage' and meta.get('price_basis') != 'nominal':
            raise SchemaError('Regular wage input is not nominal')
        if number(row.get('value')) <= 0:
            raise SchemaError('Nonpositive derivation input')
        indexed[day.isoformat()] = row
    return indexed


def _inputs(*rows: dict) -> list[dict]:
    return [{key: row.get(key) for key in ('source_id', 'series_id', 'observation_date',
             'value', 'unit', 'raw_sha256', 'retrieved_at')} for row in rows]


def derive(store) -> list[tuple[Source, list[dict]]]:
    wages = _indexed(store.observations('tw_regular_wage'), 'tw_regular_wage', WAGE_SCOPE)
    cpi = _indexed(store.observations('tw_cpi'), 'tw_cpi', CPI_SCOPE)
    output, real = [], {}
    for observed in sorted(wages.keys() & cpi.keys()):
        wage, price = wages[observed], cpi[observed]
        value = number(wage['value']) / number(price['value']) * 100
        real[observed] = value
        output.append(make_point(SERIES, 'tw_real_regular_wage', observed, value,
            status='derived', release_at=None, metadata=_monthly(date.fromisoformat(observed),
                scope=WAGE_SCOPE, price_basis='constant_2021_TWD', base_year=2021,
                formula='tw_regular_wage[t] / tw_cpi[t] * 100', inputs=_inputs(wage, price),
                provisional=bool(wage['metadata'].get('provisional')),
                calculation_basis='published_rounded_values; not_direct_official_real_wage_series')))
    for observed, wage in sorted(wages.items()):
        day = date.fromisoformat(observed)
        previous = _month_end(day.year - 1, day.month).isoformat()
        prior = wages.get(previous)
        if prior is None:
            continue
        nominal = (number(wage['value']) / number(prior['value']) - 1) * 100
        output.append(make_point(SERIES, 'tw_regular_wage_yoy', observed, nominal,
            status='derived', release_at=None, metadata=_monthly(day,
                scope=WAGE_SCOPE, price_basis='nominal',
                formula='100 * (regular_wage[t] / regular_wage[same month last year] - 1)',
                inputs=_inputs(wage, prior))))
        if observed not in real or previous not in real:
            continue
        value = (real[observed] / real[previous] - 1) * 100
        output.append(make_point(SERIES, 'tw_real_regular_wage_yoy', observed, value,
            status='derived', release_at=None, metadata=_monthly(day,
                scope=WAGE_SCOPE, price_basis='constant_2021_TWD', base_year=2021,
                formula='100 * ((wage[t]/CPI[t]) / (wage[same month last year]/CPI[same month last year]) - 1)',
                inputs=_inputs(wage, cpi[observed], prior, cpi[previous]),
                provisional=bool(wage['metadata'].get('provisional')))))
    return [(DERIVED_SOURCE, output)] if output else []
