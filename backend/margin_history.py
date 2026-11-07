"""Credit balances and flows from official TWSE / TPEx summary reports.

Keep amounts distinct from quantities and margin short sales distinct from SBL.
Nonzero reconciliation residuals retain the reported value with quality flags.
"""
from datetime import date, datetime, timedelta, timezone
from functools import partial
import json
from decimal import Decimal

from .connectors import Source, SchemaError, ReportNotAvailable, number, observation_date, parse_margin, TWSE_LOCAL_ONLY_NOTE
from .contracts import Job, make_point
from .quality import issue, with_quality

MODEL_VERSION = 'credit-history-2'
TPEX_URL = 'https://www.tpex.org.tw/www/zh-tw/margin/balance'
TWSE_URL = 'https://www.twse.com.tw/exchangeReport/MI_MARGN'
SOURCES = (
    Source('twse_margin', 'TWSE 每日融資融券餘額與增減（網站端點，僅供本機）', TWSE_URL,
           '融資仟元轉TWD；融券保留官方交易單位，不冒稱股數。今日餘額初值與次日前日餘額可能因調帳、轉上市及公司行動而不同。'
           + TWSE_LOCAL_ONLY_NOTE, publish=False),
    Source('tpex_margin', 'TPEx 每日融資融券餘額與增減', TPEX_URL,
           '取官方合計張數及融資金額summary，包含ETF等合格信用標的；融資仟元轉TWD、數量保留張。不是全體場外槓桿或借券餘額。'),
)
SERIES = {'margin_balance': ('集中市場融資金額餘額', 'TWD')}
for market, label, quantity_unit, quantity_label in (
        ('twse', '集中市場', 'trading_unit', '交易單位'), ('tpex', '櫃買市場', 'lot', '張')):
    if market == 'tpex':
        SERIES['tpex_margin_balance'] = (label + '融資金額餘額', 'TWD')
    for suffix, text in (
        ('margin_balance_change', '融資金額增減（本報前餘額基準）'),
        ('margin_buy', '融資買進金額'), ('margin_sell', '融資賣出金額'),
        ('margin_cash_repayment', '融資現金償還金額')):
        SERIES[market + '_' + suffix] = (label + text, 'TWD')
    for suffix, text in (
        ('margin_units_balance', '融資數量餘額'), ('short_balance', '融券餘額'),
        ('short_balance_change', '融券增減（本報前餘額基準）'),
        ('short_sell', '融券賣出'), ('short_buy', '融券買進'), ('short_repayment', '融券現券償還')):
        SERIES[market + '_' + suffix] = (label + text + '（' + quantity_label + '）', quantity_unit)

TWSE_FIELDS = ['項目', '買進', '賣出', '現金(券)償還', '前日餘額', '今日餘額']
TPEX_FIELDS = ['代號', '名稱', '前資餘額(張)', '資買', '資賣', '現償', '資餘額', '資屬證金',
    '資使用率(%)', '資限額', '前券餘額(張)', '券賣', '券買', '券償', '券餘額', '券屬證金',
    '券使用率(%)', '券限額', '資券相抵(張)', '備註']


def _integer_values(values):
    result = tuple(number(v) for v in values)
    if any(v < 0 or v != v.to_integral_value() for v in result):
        raise SchemaError('Credit components must be nonnegative reported integers')
    return result


def _metadata(market, side, components, unit, extra=None):
    increase, decrease, repayment, previous, current = components
    expected = previous + increase - decrease - repayment
    residual = current - expected
    flags = ['unreconciled_balance'] if residual else []
    issues = ([issue('unreconciled_balance', field='current_balance_identity',
        observed=float(current), expected=float(expected), residual=float(residual), scope=side)] if residual else [])
    extra = extra or {}
    if extra.get('summary_detail_residuals'):
        flags.append('summary_detail_mismatch')
        names = ('increase', 'decrease', 'repayment', 'previous', 'current')
        for field, difference in extra['summary_detail_residuals'].items():
            actual = float(components[names.index(field)])
            issues.append(issue('summary_detail_mismatch', field=field, observed=actual,
                expected=actual - difference, residual=difference, scope=side))
    return {
        'frequency': 'daily', 'market': market, 'scope': 'credit_eligible_securities',
        'credit_type': side, 'unit_basis': unit, 'vintage': 'reported_current_balance',
        'calendar': market + '_official_sessions', 'release_date_known': False,
        'previous_reported_balance': float(previous),
        'reported_components': {'increase': float(increase), 'decrease': float(decrease),
            'cash_or_stock_repayment': float(repayment), 'previous': float(previous), 'current': float(current)},
        'reconstructed_balance': float(expected), 'reconciliation_residual': float(residual),
        'reconciliation': 'reported_value_unreconciled' if residual else 'exact',
        'quality_flag': flags[0] if flags else None, 'quality_flags': flags,
        'quality_schema_version': 1, 'quality_issues': issues,
        'reconciliation_formula': 'previous + increase - decrease - cash_or_stock_repayment',
        'change_basis': 'current_minus_previous_in_same_report; not_adjacent_stored_current_difference',
        'limitation': 'Repayments, adjustments, corporate actions and market transfers can change balances; not pure investor buying/selling or total leverage.',
        'short_scope': 'margin_short_sales_only; excludes_securities_borrowing_and_lending', **extra,
    }


def _points(market, day, amount, margin_units, short_units, details=None):
    details = details or {}
    output = []
    amount_meta = _metadata(market, 'margin_purchase_amount', amount, 'TWD')
    amount_meta.update(reconciliation_residual_twd=amount_meta['reconciliation_residual'],
        reconstructed_balance_twd=amount_meta['reconstructed_balance'], reported_unit='TWD thousand',
        rounding_possible_not_verified=bool(0 < abs(amount_meta['reconciliation_residual']) <= 2000))
    unit = 'trading_unit' if market == 'twse' else 'lot'
    margin_meta = _metadata(market, 'margin_purchase_quantity', margin_units, unit,
                           details.get('margin', {}))
    short_meta = _metadata(market, 'margin_short_sale_quantity', short_units, unit,
                          details.get('short', {}))
    for sid, value, metadata, derived in (
        ('margin_balance' if market == 'twse' else 'tpex_margin_balance', amount[4], amount_meta, False),
        (market + '_margin_units_balance', margin_units[4], margin_meta, False),
        (market + '_short_balance', short_units[4], short_meta, False),
        (market + '_margin_balance_change', amount[4] - amount[3], amount_meta, True),
        (market + '_margin_buy', amount[0], amount_meta, False),
        (market + '_margin_sell', amount[1], amount_meta, False),
        (market + '_margin_cash_repayment', amount[2], amount_meta, False),
        (market + '_short_balance_change', short_units[4] - short_units[3], short_meta, True),
        (market + '_short_sell', short_units[0], short_meta, False),
        (market + '_short_buy', short_units[1], short_meta, False),
        (market + '_short_repayment', short_units[2], short_meta, False)):
        metadata = dict(metadata)
        field = ('current' if 'balance' in sid else 'repayment' if 'repayment' in sid
                 else 'increase' if sid.endswith('_margin_buy') or sid.endswith('_short_sell') else 'decrease')
        relevant_fields = {'previous', 'current'} if derived else {field}
        issues = [{**i, 'scope': sid, 'blocking': i['field'] == 'current_balance_identity' or i['field'] in relevant_fields}
                  for i in metadata['quality_issues']]
        metadata = with_quality(metadata, issues)
        if derived:
            metadata.update(formula='reported_current - reported_previous',
                input_basis='same_raw_report_and_observation_date',
                inputs=[{'field': 'reported_current', 'value': float(metadata['reported_components']['current'])},
                        {'field': 'reported_previous', 'value': float(metadata['reported_components']['previous'])}])
        output.append(make_point(SERIES, sid, day, value, release_at=None,
            status='derived' if derived else 'observed', metadata=metadata))
    return output


def parse_twse(raw: bytes, expected_date: str) -> list[dict]:
    try:
        data = json.loads(raw.decode('utf-8-sig'))
    except (ValueError, UnicodeError) as exc:
        raise SchemaError('Invalid TWSE credit JSON') from exc
    if not isinstance(data, dict):
        raise SchemaError('TWSE credit response is not a report object')
    if data.get('stat') != 'OK' or observation_date(data.get('date')) != expected_date:
        raise ReportNotAvailable('TWSE credit report is missing or returned a different session', expected_date)
    tables = [t for t in data.get('tables', []) if t.get('fields') == TWSE_FIELDS]
    if len(tables) != 1:
        raise SchemaError('Exactly one dated TWSE credit summary required')
    rows = tables[0].get('data', [])
    if any(not isinstance(r, list) or len(r) != 6 for r in rows):
        raise SchemaError('TWSE credit summary width changed')
    indexed = {row[0]: row for row in rows}
    if len(indexed) != len(rows) or set(indexed) != {'融資(交易單位)', '融券(交易單位)', '融資金額(仟元)'}:
        raise SchemaError('TWSE credit summary rows changed or duplicated')
    amount = tuple(v * 1000 for v in _integer_values(indexed['融資金額(仟元)'][1:]))
    margin_units = _integer_values(indexed['融資(交易單位)'][1:])
    buy, sell, repay, previous, current = _integer_values(indexed['融券(交易單位)'][1:])
    output = _points('twse', expected_date, amount, margin_units, (sell, buy, repay, previous, current))
    # Keep the original margin_balance metadata contract and residual flags.
    legacy = parse_margin(data, allow_reported_residual=True)[0]
    output[0]['metadata'].update(legacy['metadata'])
    output[0]['metadata'] = with_quality(output[0]['metadata'], output[0]['metadata']['quality_issues'])
    for p in output:
        p['metadata']['source_notes'] = tables[0].get('notes', [])
        p['metadata']['note'] = '當日餘額初值；次日前日餘額可能調整，尤其轉上市時不能把移轉量誤判買賣。'
    return output


def parse_tpex(raw: bytes, expected_date: str) -> list[dict]:
    try:
        data = json.loads(raw.decode('utf-8-sig'))
    except (ValueError, UnicodeError) as exc:
        raise SchemaError('Invalid TPEx credit JSON') from exc
    if not isinstance(data, dict):
        raise SchemaError('TPEx credit response is not a report object')
    if data.get('stat') != 'ok' or observation_date(data.get('date')) != expected_date:
        raise ReportNotAvailable('TPEx credit report is missing or returned a different session', expected_date)
    tables = data.get('tables', [])
    if len(tables) != 1 or tables[0].get('fields') != TPEX_FIELDS:
        raise SchemaError('TPEx credit columns changed')
    table = tables[0]
    if observation_date(table.get('date')) != expected_date:
        raise SchemaError('TPEx credit table date mismatch')
    rows, summaries = table.get('data', []), table.get('summary', [])
    if not rows or table.get('totalCount') != len(rows) or any(len(r) != 20 for r in rows + summaries):
        raise SchemaError('TPEx credit rows truncated or malformed')
    if len({r[0] for r in rows}) != len(rows):
        raise SchemaError('Duplicate TPEx credit security')
    summary = {row[1]: row for row in summaries}
    if len(summary) != len(summaries) or set(summary) != {'合計(張)', '融資金(仟元)'}:
        raise SchemaError('TPEx credit summary labels or units changed')
    amount_row, total = summary['融資金(仟元)'], summary['合計(張)']
    # Source layout is previous, increase, decrease, repayment, current.
    order = (3, 4, 5, 2, 6)
    amount = tuple(v * 1000 for v in _integer_values([amount_row[i] for i in order]))
    margin_units = _integer_values([total[i] for i in order])
    short_order = (11, 12, 13, 10, 14)
    short_units = _integer_values([total[i] for i in short_order])
    details = {}
    for side, columns, aggregate in [('margin', order, margin_units), ('short', short_order, short_units)]:
        sums = [Decimal(0)] * 5
        for row in rows:
            for i, value in enumerate(_integer_values([row[c] for c in columns])):
                sums[i] += value
        residuals = {name: float(actual - summed) for name, actual, summed in
            zip(('increase', 'decrease', 'repayment', 'previous', 'current'), aggregate, sums) if actual != summed}
        details[side] = {'security_count': len(rows), 'summary_detail_residuals': residuals,
                         'summary_detail_check': 'exact' if not residuals else 'reported_summary_retained'}
    return _points('tpex', expected_date, amount, margin_units, short_units, details)


def tpex_daily_jobs(trading_dates: list[str]) -> list[Job]:
    today = datetime.now(timezone(timedelta(hours=8))).date()
    output = []
    for day in sorted(set(trading_dates)):
        d = date.fromisoformat(day)
        output.append(Job(f'tpex_margin:{day}', 'tpex_margin',
            TPEX_URL + '?date=' + d.strftime('%Y/%m/%d') + '&response=json',
            partial(parse_tpex, expected_date=day), parser_version=MODEL_VERSION,
            refresh_after_hours=8760 if d < today - timedelta(days=30) else 24,
            max_bytes=2_000_000))
    return output
