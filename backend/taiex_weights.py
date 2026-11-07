"""Official TAIEX constituent weights published by TAIFEX (data by Taiwan Index Plus).

Only the 2330 (TSMC) weight is stored. It is the official month-end benchmark for
the ``tsmc_weight_estimated`` proxy in ex_tsmc.py; it does not replace that model.
The page carries one month-end snapshot at a time, so history accrues one capture
per month. Nothing is interpolated or carried forward.
"""
from decimal import Decimal
from html.parser import HTMLParser
import re

from .connectors import Source, SchemaError, number, observation_date
from .contracts import Job, make_point

URL = 'https://www.taifex.com.tw/cht/2/weightedPropertion'
PARSER_VERSION = 'taiex-weights-1'
MODEL_VERSION = PARSER_VERSION
TITLE = '發行量加權股價指數成分股暨市值比重'
TSMC_CODE = '2330'
# Each row is rounded to 4 decimals; ~1,100 rows can drift by at most ~0.06pp.
SUM_TOLERANCE_PP = 0.1
MIN_ROWS = 500
SIDES = ('a', 'b')
FIELDS = ('rank', 'name', 'propertion')

SOURCES = (
    Source('taifex_taiex_weights', '期交所 加權指數成分股市值比重', URL,
           '期交所轉載臺灣指數公司資料，每月月底更新一次；只保存2330「市值佔大盤比重」作為排除台積電估算的官方對照，'
           '頁面一次只列一個月底，不回填歷史月份。', 'monthly', 45),
)
SERIES = {
    'tsmc_weight_official': ('台積電占加權指數市值比重（官方月底）', 'percent'),
}


class _Cells(HTMLParser):
    """Collect <td headers=...> text by table row; tags inside a cell are ignored."""

    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.rows, self.row, self.cell, self.text = [], None, None, []

    def handle_starttag(self, tag, attrs):
        if tag == 'tr':
            self._close_row()
            self.row = []
        elif tag == 'td' and self.row is not None:
            self._close_cell()
            headers = dict(attrs).get('headers')
            self.cell, self.text = (headers, []) if headers else (None, [])

    def handle_endtag(self, tag):
        if tag == 'td':
            self._close_cell()
        elif tag in ('tr', 'table'):
            self._close_row()

    def handle_data(self, data):
        if self.cell is not None:
            self.text.append(data)

    def _close_cell(self):
        if self.cell is not None and self.row is not None:
            self.row.append((self.cell, ' '.join(''.join(self.text).split())))
        self.cell, self.text = None, []

    def _close_row(self):
        self._close_cell()
        if self.row:
            self.rows.append(self.row)
        self.row = None

    def close(self):
        super().close()
        self._close_row()


def _text(page: str) -> str:
    page = re.sub(r'(?is)<(script|style)\b.*?</\1>', ' ', page)
    from html import unescape
    return ' '.join(unescape(re.sub(r'<[^>]+>', ' ', page)).split())


def parse_table(raw: bytes) -> dict:
    """Strictly parse the whole constituent table; any ambiguity is a SchemaError."""
    try:
        page = raw.decode('utf-8')
    except UnicodeDecodeError as exc:
        raise SchemaError('TAIEX weight page is not UTF-8') from exc
    text = _text(page)
    if TITLE not in text:
        raise SchemaError('TAIEX weight page title missing')
    dates = set(re.findall(r'資料日期\s*[：:]\s*(\d{4}/\d{1,2}/\d{1,2})', text))
    if len(dates) != 1:
        raise SchemaError('TAIEX weight data date missing or ambiguous; retrieval date is not a substitute')
    y, m, d = (int(x) for x in dates.pop().split('/'))
    data_date = observation_date(f'{y:04d}{m:02d}{d:02d}')
    parser = _Cells()
    parser.feed(page)
    parser.close()
    entries = []
    for cells in parser.rows:
        by_header = {}
        for header, value in cells:
            by_header.setdefault(header, []).append(value)
        if not any(h.startswith(('rank_', 'name_', 'propertion_')) for h in by_header):
            continue
        for side in SIDES:
            rank = by_header.get('rank_' + side, [])
            name = by_header.get('name_' + side, [])
            weight = by_header.get('propertion_' + side, [])
            if len(rank) != 1 or len(name) != 2 or len(weight) != 1:
                raise SchemaError('TAIEX weight row layout changed')
            values = [rank[0], *name, weight[0]]
            if not any(values):
                continue  # Right-hand half of the final row is blank on the official page.
            if not all(values):
                raise SchemaError('TAIEX weight row has a blank field')
            if not re.fullmatch(r'[0-9][0-9,]*', rank[0]):
                raise SchemaError('Invalid constituent rank')
            if not re.fullmatch(r'[0-9]{4}[0-9A-Z]{0,2}', name[0]):
                raise SchemaError('Invalid constituent code')
            match = re.fullmatch(r'([0-9]+(?:\.[0-9]+)?)%', weight[0])
            if not match:
                raise SchemaError('Invalid constituent weight')
            entries.append({'rank': int(number(rank[0])), 'code': name[0], 'name': name[1],
                            'weight_pct': number(match.group(1))})
    if len(entries) < MIN_ROWS:
        raise SchemaError(f'Too few constituents ({len(entries)}) for a complete TAIEX table')
    ranks = sorted(e['rank'] for e in entries)
    if ranks != list(range(1, len(entries) + 1)):
        raise SchemaError('Constituent ranks are not a complete 1..N sequence')
    codes = [e['code'] for e in entries]
    if len(set(codes)) != len(codes):
        raise SchemaError('Duplicate constituent code')
    tsmc = [e for e in entries if e['code'] == TSMC_CODE]
    if len(tsmc) != 1:
        raise SchemaError('Missing or duplicate 2330 constituent row')
    if not 0 < tsmc[0]['weight_pct'] < 100:
        raise SchemaError('Implausible 2330 weight')
    total = sum((e['weight_pct'] for e in entries), Decimal(0))
    if abs(total - 100) > Decimal(str(SUM_TOLERANCE_PP)):
        raise SchemaError(f'Constituent weights sum to {total}%, not about 100%')
    return {'data_date': data_date, 'entries': sorted(entries, key=lambda e: e['rank']),
            'weight_sum_pct': float(total), 'tsmc': tsmc[0],
            'source_attribution': '臺灣指數公司' if '臺灣指數公司' in text else None,
            'monthly_note': '每月月底更新一次' in text}


def parse(raw: bytes) -> list[dict]:
    table = parse_table(raw)
    tsmc = table['tsmc']
    return [make_point(SERIES, 'tsmc_weight_official', table['data_date'], float(tsmc['weight_pct']),
        status='observed', release_at=None, metadata={
            'parser_version': PARSER_VERSION, 'constituent_code': TSMC_CODE, 'constituent_name': tsmc['name'],
            'constituent_rank': tsmc['rank'], 'constituent_count': len(table['entries']),
            'weight_sum_pct': round(table['weight_sum_pct'], 6), 'sum_tolerance_pp': SUM_TOLERANCE_PP,
            'data_date_basis': '頁尾「資料日期」，非擷取日', 'source_attribution': table['source_attribution'],
            'update_cadence': 'month_end_snapshot', 'publication_time': 'unknown',
            'value_definition': '市值佔大盤比重（%），官方頁四位小數',
            'role': 'official benchmark for tsmc_weight_estimated; not used to build ex_tsmc_estimated'})]


def jobs(start, end) -> list[Job]:
    # One current snapshot regardless of the requested range; HTTP at most once a day.
    if start > end:
        raise ValueError('History start must not exceed end')
    return [Job(key='taifex-taiex-weights:current', source_id='taifex_taiex_weights', url=URL,
                parser=parse, parser_version=PARSER_VERSION, refresh_after_hours=24, max_bytes=3_000_000)]
