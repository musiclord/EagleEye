"""TWSE OpenAPI (openapi.twse.com.tw): the publishable route for Taiwan Stock Exchange data.

The OpenAPI is released under the Open Government Data License, version 1 (data.gov.tw/license):
reuse is free with attribution. The website endpoints (www.twse.com.tw) forbid automated download
without consent, so their sources are publish=False (docs/TWSE_OPENAPI_MIGRATION.md).

No OpenAPI endpoint takes a date. FMTQIK carries the current month; STOCK_DAY_ALL, MI_MARGN and
t187ap03_L carry only the latest session. History therefore accrues one capture at a time; a
session that was never captured stays missing. Nothing is back-filled, interpolated or carried.
"""
from datetime import date, datetime, timedelta, timezone
from functools import partial
import json

from .connectors import (Source, SchemaError, ReportNotAvailable, number, observation_date,
                         TWSE_OPENAPI_LICENSE, SOURCE_MAP as CONNECTOR_SOURCES, SERIES as CONNECTOR_SERIES,
                         parse as parse_connector)
from .contracts import Job, make_point
from .quality import issue, with_quality
from .market_history import SERIES as MARKET_SERIES
from .margin_history import SERIES as CREDIT_SERIES, _metadata as credit_metadata
from .ex_tsmc import SERIES as EX_TSMC_SERIES

BASE = 'https://openapi.twse.com.tw/v1/'
FMTQIK_URL = BASE + 'exchangeReport/FMTQIK'
STOCK_DAY_ALL_URL = BASE + 'exchangeReport/STOCK_DAY_ALL'
MI_MARGN_URL = BASE + 'exchangeReport/MI_MARGN'
COMPANY_URL = BASE + 'opendata/t187ap03_L'
PARSER_VERSION = 'twse-openapi-2'
TAIPEI = timezone(timedelta(hours=8))
TSMC = '2330'
LICENSE = 'OGDL-Taiwan-1.0'
TSMC_JOB_KEY = 'twse_oa_tsmc:latest'
CREDIT_JOB_KEY = 'twse_oa_credit:latest'
# MI_MARGN has no date field. It is dated by the STOCK_DAY_ALL capture of the same sync run, so that
# capture must be recent; an older one says nothing about which session MI_MARGN now carries.
ANCHOR_MAX_AGE = timedelta(hours=3)
# Look back this far for stored balances when checking whether MI_MARGN is still an older session.
STALE_LOOKBACK_DAYS = 45
ACCRUAL_NOTE = 'OpenAPI 只提供最新一個交易日；歷史由每日擷取逐步累積，沒擷取到的交易日保持空白，不回補也不沿用前值。'


def _attribution(name):
    return '臺灣證券交易所 {year}〈' + name + '〉（OpenAPI v1）'


SOURCES = (
    CONNECTOR_SOURCES['twse_oa_market'],
    Source('twse_oa_tsmc', 'TWSE OpenAPI 上市個股日成交資訊（台積電）', STOCK_DAY_ALL_URL,
           '取 2330 未還原收盤價與成交金額；與價格指數比較，不包含股利再投資報酬。' + ACCRUAL_NOTE,
           attribution=_attribution('上市個股日成交資訊'), license_url=TWSE_OPENAPI_LICENSE),
    Source('twse_oa_credit', 'TWSE OpenAPI 集中市場融資融券餘額（交易單位）', MI_MARGN_URL,
           '逐檔交易單位加總成市場合計，OpenAPI 沒有融資金額。回應沒有日期欄，資料日以同一輪擷取的上市個股日成交資訊日期推定；'
           '有同日網站合計時逐欄比對，不符就不收。空白欄位視為 0（2026-11-06 加總與官方合計逐欄相等）。' + ACCRUAL_NOTE,
           attribution=_attribution('集中市場融資融券餘額'), license_url=TWSE_OPENAPI_LICENSE),
    Source('twse_oa_shares', 'TWSE OpenAPI 上市公司基本資料（台積電發行股數）', COMPANY_URL,
           '取 2330「已發行普通股數或TDR原股發行股數」，以出表日期為觀測日；每週擷取一次，樣本之間不內插。',
           'weekly', 14, attribution=_attribution('上市公司基本資料'), license_url=TWSE_OPENAPI_LICENSE),
)
CREDIT_IDS = ('twse_margin_units_balance', 'twse_short_balance', 'twse_short_balance_change',
              'twse_short_sell', 'twse_short_buy', 'twse_short_repayment')
SERIES = {'taiex': CONNECTOR_SERIES['taiex'], 'market_turnover': CONNECTOR_SERIES['market_turnover'],
          'tsmc_close': MARKET_SERIES['tsmc_close'], 'tsmc_turnover': MARKET_SERIES['tsmc_turnover'],
          'tsmc_issued_shares': EX_TSMC_SERIES['tsmc_issued_shares'],
          **{sid: CREDIT_SERIES[sid] for sid in CREDIT_IDS}}

STOCK_FIELDS = {'Date', 'Code', 'Name', 'TradeVolume', 'TradeValue', 'OpeningPrice', 'HighestPrice',
                'LowestPrice', 'ClosingPrice', 'Change', 'Transaction'}
CREDIT_FIELDS = {'股票代號', '股票名稱', '融資買進', '融資賣出', '融資現金償還', '融資前日餘額', '融資今日餘額', '融資限額',
                 '融券買進', '融券賣出', '融券現券償還', '融券前日餘額', '融券今日餘額', '融券限額', '資券互抵', '註記'}
# Same order as margin_history components: (increase, decrease, repayment, previous, current).
# For short sales a sale increases the balance and a purchase decreases it.
MARGIN_COLUMNS = ('融資買進', '融資賣出', '融資現金償還', '融資前日餘額', '融資今日餘額')
SHORT_COLUMNS = ('融券賣出', '融券買進', '融券現券償還', '融券前日餘額', '融券今日餘額')
COMPONENT_KEYS = ('increase', 'decrease', 'cash_or_stock_repayment', 'previous', 'current')
COMPANY_DATE, COMPANY_CODE, COMPANY_SHARES = '出表日期', '公司代號', '已發行普通股數或TDR原股發行股數'


def _load(raw, label):
    try:
        data = json.loads(raw.decode('utf-8-sig'))
    except (ValueError, UnicodeError) as exc:
        raise SchemaError(label + ' is not valid JSON') from exc
    if not isinstance(data, list) or not data or not all(isinstance(r, dict) for r in data):
        raise SchemaError(label + ' is empty or not a list of rows')
    return data


def _rows(raw, fields, label):
    rows = _load(raw, label)
    if any(set(r) != fields for r in rows):
        raise SchemaError(label + ' fields changed; reject rather than guess columns')
    return rows


def _session(value, today=None):
    day = observation_date(value)
    if day > (today or datetime.now(TAIPEI).date()).isoformat():
        raise SchemaError('Session date is after today; retrieval date is not a substitute')
    return day


def _single(rows, field, label):
    values = {r[field] for r in rows}
    if len(values) != 1:
        raise SchemaError(label + ' rows carry more than one date')
    return values.pop()


def _tsmc_row(rows, field, label):
    found = [r for r in rows if r[field] == TSMC]
    if len(found) != 1:
        raise SchemaError('Missing or duplicate 2330 row in ' + label)
    return found[0]


def stock_session(raw, today=None):
    """The one official Date shared by every STOCK_DAY_ALL row, and the rows."""
    rows = _rows(raw, STOCK_FIELDS, 'STOCK_DAY_ALL')
    return _session(_single(rows, 'Date', 'STOCK_DAY_ALL'), today), rows


def parse_stock_day_all(raw, today=None):
    day, rows = stock_session(raw, today)
    row = _tsmc_row(rows, 'Code', 'STOCK_DAY_ALL')
    close, turnover = number(row['ClosingPrice']), number(row['TradeValue'])
    if close <= 0 or turnover < 0:
        raise SchemaError('Invalid 2330 closing price or turnover')
    meta = {'endpoint': 'STOCK_DAY_ALL', 'security': TSMC, 'session_date_basis': 'official_date_field',
            'coverage': 'latest_session_only', 'license': LICENSE}
    return [make_point(SERIES, 'tsmc_close', day, close, metadata=dict(meta)),
            make_point(SERIES, 'tsmc_turnover', day, turnover, metadata=dict(meta))]


def parse_company(raw, today=None):
    rows = _load(raw, 't187ap03_L')
    fields = set(rows[0])
    if not {COMPANY_DATE, COMPANY_CODE, COMPANY_SHARES} <= fields or any(set(r) != fields for r in rows):
        raise SchemaError('t187ap03_L columns changed; reject rather than guess columns')
    day = _session(_single(rows, COMPANY_DATE, 't187ap03_L'), today)
    shares = number(_tsmc_row(rows, COMPANY_CODE, 't187ap03_L')[COMPANY_SHARES])
    if shares <= 0 or shares != shares.to_integral_value():
        raise SchemaError('Invalid issued-share count')
    return [make_point(SERIES, 'tsmc_issued_shares', day, shares, metadata={
        'endpoint': 't187ap03_L', 'security': TSMC, 'sampling': 'weekly_capture_of_latest_report',
        'session_date_basis': 'official_report_date', 'license': LICENSE})]


def credit_totals(raw):
    """Sum the per-security units. A blank cell is the source's zero: on 2026-11-06 these sums,
    with blanks as zero, equalled the website summary in all five columns of both sides."""
    rows = _rows(raw, CREDIT_FIELDS, 'MI_MARGN')
    if len({r['股票代號'] for r in rows}) != len(rows):
        raise SchemaError('Duplicate security in MI_MARGN')
    blanks, totals = 0, {}
    for side, columns in (('margin', MARGIN_COLUMNS), ('short', SHORT_COLUMNS)):
        sums = []
        for column in columns:
            total = 0
            for row in rows:
                text = str(row[column]).strip()
                if not text:
                    blanks += 1
                    continue
                value = number(text)
                if value < 0 or value != value.to_integral_value():
                    raise SchemaError('Credit units must be nonnegative integers')
                total += int(value)
            sums.append(total)
        totals[side] = tuple(sums)
    return totals, {'security_count': len(rows), 'blank_cells_counted_as_zero': blanks}


def parse_credit(raw, expected_date, site=None, known=None, own=None):
    """Date one MI_MARGN capture as expected_date, or refuse.

    expected_date: session of the STOCK_DAY_ALL capture from the same run (the anchor).
    site:  {date: {'margin': components, 'short': components}} from the website summary.
    known: {date: {(margin_current, short_current), ...}} from every stored source.
    own:   {date: (margin_current, short_current)} already stored from this OpenAPI source.
    """
    totals, detail = credit_totals(raw)
    margin, short = totals['margin'], totals['short']
    balances = (margin[4], short[4])
    site, known, own = site or {}, known or {}, own or {}
    if expected_date in site:
        same = site[expected_date]
        if (same['margin'], same['short']) != (margin, short):
            raise ReportNotAvailable('OpenAPI MI_MARGN differs from the same-day website summary; '
                                     'it does not carry this session yet', expected_date)
        basis, inferred = 'anchor_matches_same_day_site_summary', []
    else:
        older = sorted(day for day, pairs in known.items() if day != expected_date and balances in pairs)
        if older:
            raise ReportNotAvailable(f'OpenAPI MI_MARGN still carries the {older[-1]} balances', expected_date)
        if expected_date in own and own[expected_date] != balances:
            # Two different captures for one inferred session: one of them is another session.
            # Keep the first and leave the conflict visible instead of overwriting it.
            raise ReportNotAvailable('OpenAPI MI_MARGN changed after this session was already captured; '
                                     'the session date cannot be confirmed', expected_date)
        basis = 'anchor_from_same_run_stock_day_all'
        inferred = [issue('session_date_inferred', field='observation_date', observed=expected_date,
                          expected=None, residual=None, blocking=True, severity='warning',
                          note='回應沒有日期欄；以同一輪擷取的上市個股日成交資訊日期推定，沒有同日網站合計可比對；日期未確認，不納入分析或公開快照。')]
    extra = {**detail, 'aggregation': 'sum_of_security_rows', 'endpoint': 'MI_MARGN',
             'session_date_basis': basis, 'coverage': 'latest_session_only', 'license': LICENSE}
    margin_meta = credit_metadata('twse', 'margin_purchase_quantity', margin, 'trading_unit', extra)
    short_meta = credit_metadata('twse', 'margin_short_sale_quantity', short, 'trading_unit', extra)
    output = []
    for sid, value, metadata, derived, field in (
            ('twse_margin_units_balance', margin[4], margin_meta, False, 'current'),
            ('twse_short_balance', short[4], short_meta, False, 'current'),
            ('twse_short_balance_change', short[4] - short[3], short_meta, True, None),
            ('twse_short_sell', short[0], short_meta, False, 'increase'),
            ('twse_short_buy', short[1], short_meta, False, 'decrease'),
            ('twse_short_repayment', short[2], short_meta, False, 'repayment')):
        relevant = {'previous', 'current'} if derived else {field}
        issues = [{**i, 'scope': sid, 'blocking': i['field'] == 'current_balance_identity' or i['field'] in relevant}
                  for i in metadata['quality_issues']] + [{**i, 'scope': sid} for i in inferred]
        meta = with_quality(dict(metadata), issues)
        if derived:
            meta.update(formula='reported_current - reported_previous', input_basis='same_raw_report_and_observation_date',
                        inputs=[{'field': 'reported_current', 'value': float(short[4])},
                                {'field': 'reported_previous', 'value': float(short[3])}])
        output.append(make_point(SERIES, sid, expected_date, value, release_at=None,
                                 status='derived' if derived else 'observed', metadata=meta))
    return output


def anchor_session(store, now=None):
    """Session date of a recent successful STOCK_DAY_ALL capture, else None."""
    checkpoint = store.checkpoint(TSMC_JOB_KEY)
    if not checkpoint or checkpoint.get('status') != 'ok' or not checkpoint.get('raw_sha256'):
        return None
    try:
        fetched = datetime.fromisoformat(checkpoint['fetched_at'])
    except (TypeError, ValueError):
        return None
    if fetched.tzinfo is None or not timedelta(0) <= (now or datetime.now(timezone.utc)) - fetched <= ANCHOR_MAX_AGE:
        return None
    path = store.raw_dir / (checkpoint['raw_sha256'] + '.json')
    if not path.exists():
        return None
    try:
        return stock_session(path.read_bytes())[0]
    except SchemaError:
        return None


def stored_credit(store, expected_date):
    """Stored TWSE credit unit balances up to expected_date, split as parse_credit expects."""
    start = (date.fromisoformat(expected_date) - timedelta(days=STALE_LOOKBACK_DAYS)).isoformat()
    margin = store.observations('twse_margin_units_balance', start=start, end=expected_date)
    short = {(p['source_id'], p['observation_date']): p
             for p in store.observations('twse_short_balance', start=start, end=expected_date)}
    site, known, own = {}, {}, {}
    for p in margin:
        q = short.get((p['source_id'], p['observation_date']))
        if q is None:
            continue
        day, pair = p['observation_date'], (int(p['value']), int(q['value']))
        known.setdefault(day, set()).add(pair)
        if p['source_id'] == 'twse_oa_credit':
            own[day] = pair
        elif p['source_id'] == 'twse_margin':
            components = [(r.get('metadata') or {}).get('reported_components') or {} for r in (p, q)]
            if all(set(COMPONENT_KEYS) <= set(c) for c in components):
                site[day] = {side: tuple(int(c[k]) for k in COMPONENT_KEYS)
                             for side, c in zip(('margin', 'short'), components)}
    return site, known, own


def parse_credit_capture(raw, store):
    expected = anchor_session(store)
    if expected is None:
        raise ReportNotAvailable('No STOCK_DAY_ALL capture from this sync run; MI_MARGN has no date of its own')
    site, known, own = stored_credit(store, expected)
    return parse_credit(raw, expected, site, known, own)


def jobs(start, end):
    """Captures of what the OpenAPI publishes now. They ignore the requested range: no endpoint takes a date."""
    return [
        Job('twse_oa_market:current', 'twse_oa_market', FMTQIK_URL, partial(parse_connector, 'twse_oa_market'),
            headers={'Accept': 'application/json'}, parser_version=PARSER_VERSION, refresh_after_hours=2, max_bytes=500_000),
        Job(TSMC_JOB_KEY, 'twse_oa_tsmc', STOCK_DAY_ALL_URL, parse_stock_day_all,
            headers={'Accept': 'application/json'}, parser_version=PARSER_VERSION, refresh_after_hours=2, max_bytes=5_000_000),
        Job('twse_oa_shares:latest', 'twse_oa_shares', COMPANY_URL, parse_company,
            headers={'Accept': 'application/json'}, parser_version=PARSER_VERSION, refresh_after_hours=24 * 7, max_bytes=10_000_000),
    ]


def store_daily_jobs(store, calendar):
    """MI_MARGN runs with the daily jobs, after this run's STOCK_DAY_ALL capture and after the website
    credit summaries (when enabled) for the same sessions, so the parser can compare against them.
    The refresh is shorter than STOCK_DAY_ALL's, so whenever the anchor is re-captured MI_MARGN is too."""
    return [Job(CREDIT_JOB_KEY, 'twse_oa_credit', MI_MARGN_URL, partial(parse_credit_capture, store=store),
                headers={'Accept': 'application/json'}, parser_version=PARSER_VERSION, refresh_after_hours=1, max_bytes=5_000_000)]
