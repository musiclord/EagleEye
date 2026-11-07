"""TPEx monthly session calendar / index / turnover and daily institutional totals."""
from datetime import date, datetime, timedelta, timezone
from functools import partial
import json

from .connectors import Source, SchemaError, ReportNotAvailable, number, observation_date
from .contracts import Job, make_point
from .quality import issue, with_quality
from .margin_history import tpex_daily_jobs

MODEL_VERSION = 'tpex-history-2'
DAILY_CALENDAR = 'tpex_index'
MARKET_URL = 'https://www.tpex.org.tw/www/zh-tw/afterTrading/tradingIndexRpk'
INSTITUTION_URL = 'https://www.tpex.org.tw/www/zh-tw/insti/summary'
SOURCES = (
    Source('tpex_market', 'TPEx 每日櫃買指數與有價證券成交額', MARKET_URL,
        '按月取回日資料建立櫃買自己的交易日。成交額含ETF、權證、TDR、ETN及鉅額，不含可轉債；仟元轉TWD。指數與成交額母體不同。'),
    Source('tpex_institutions', 'TPEx 每日三大法人有價證券交易', INSTITUTION_URL,
        'prod=0有價證券口徑，含ETF等與鉅額；外資不含自營商，避免與自營商重複計入。原始成交統計不依錯帳更正調整；不代表跨境匯出入。'),
)
SERIES = {
    'tpex_index': ('櫃買發行量加權股價指數', 'point'),
    'tpex_turnover': ('上櫃有價證券成交金額（含鉅額）', 'TWD'),
    'tpex_foreign_net': ('上櫃外資及陸資買賣超（不含自營商）', 'TWD'),
    'tpex_foreign_buy': ('上櫃外資及陸資買進金額（不含自營商）', 'TWD'),
    'tpex_foreign_sell': ('上櫃外資及陸資賣出金額（不含自營商）', 'TWD'),
    'tpex_trust_net': ('上櫃投信買賣超', 'TWD'),
    'tpex_dealer_prop_net': ('上櫃自營商自行買賣買賣超', 'TWD'),
    'tpex_dealer_hedge_net': ('上櫃自營商避險買賣超', 'TWD'),
    'tpex_dealer_net': ('上櫃自營商合計買賣超', 'TWD'),
    'tpex_institutional_net': ('上櫃三大法人買賣超', 'TWD'),
}
MARKET_FIELDS = ['日 期', '成交股數（張數）', '金額（仟元）', '筆數', '櫃買指數', '漲/跌']
INSTITUTION_FIELDS = ['單位名稱', '買進金額(元)', '賣出金額(元)', '買賣超(元)']
GROUPS = {'外資及陸資合計', '外資及陸資(不含自營商)', '外資自營商', '投信',
          '自營商合計', '自營商(自行買賣)', '自營商(避險)', '三大法人合計*'}


def _payload(raw, session=None):
    try:
        data = json.loads(raw.decode('utf-8-sig'))
    except (UnicodeError, ValueError) as exc:
        raise SchemaError('Invalid TPEx historical JSON') from exc
    if not isinstance(data, dict):
        raise SchemaError('TPEx response is not a report object')
    if data.get('stat') != 'ok':
        raise ReportNotAvailable('TPEx did not return a successful dated report', session)
    tables = data.get('tables', [])
    if not isinstance(tables, list) or len(tables) != 1:
        raise SchemaError('Expected one TPEx historical table')
    return data, tables[0]


def _meta(**extra):
    return {'frequency': 'daily', 'market': 'tpex', 'calendar': 'tpex_official_sessions',
            'release_date_known': False, 'scope': 'otc_securities_including_block_excluding_convertible_bonds',
            'seasonally_adjusted': False, **extra}


def parse_month(raw: bytes, month: str, start: str, end: str) -> list[dict]:
    data, table = _payload(raw, end)
    if observation_date(data.get('date'))[:7] != month or observation_date(table.get('date'))[:7] != month:
        raise SchemaError('TPEx returned a different month')
    if table.get('fields') != MARKET_FIELDS:
        raise SchemaError('TPEx market columns or turnover units changed')
    rows = table.get('data', [])
    if not rows or table.get('totalCount') != len(rows):
        raise SchemaError('TPEx market history is empty or truncated')
    result, seen = [], set()
    for row in rows:
        if not isinstance(row, list) or len(row) != len(MARKET_FIELDS):
            raise SchemaError('Malformed TPEx market row')
        day = observation_date(row[0])
        if day[:7] != month or day in seen:
            raise SchemaError('Duplicate or out-of-month TPEx session')
        seen.add(day)
        index, turnover = number(row[4]), number(row[2]) * 1000
        if index <= 0 or turnover < 0:
            raise SchemaError('Invalid TPEx index or turnover')
        if start <= day <= end:
            result += [make_point(SERIES, 'tpex_index', day, index, release_at=None, status='observed',
                       metadata=_meta(scope='tpex_stock_price_index', return_basis='price_not_total_return')),
                       make_point(SERIES, 'tpex_turnover', day, turnover, release_at=None, status='observed',
                       metadata=_meta(reported_unit='TWD thousand', reported_precision_twd=1000,
                                      source_notes=table.get('notes', [])))]
    return sorted(result, key=lambda p: (p['observation_date'], p['series_id']))


def parse_institutions(raw: bytes, expected_date: str) -> list[dict]:
    data, table = _payload(raw, expected_date)
    if observation_date(data.get('date')) != expected_date or observation_date(table.get('date')) != expected_date:
        raise ReportNotAvailable('TPEx institutional report returned a different session', expected_date)
    if table.get('fields') != INSTITUTION_FIELDS or table.get('prod') != '0' or '上櫃有價證券' not in table.get('subtitle', ''):
        raise SchemaError('TPEx institutional units or securities scope changed')
    indexed = {}
    for row in table.get('data', []):
        if not isinstance(row, list) or len(row) != 4 or not isinstance(row[0], str):
            raise SchemaError('Malformed TPEx institutional row')
        label = row[0].strip()
        if label in indexed:
            raise SchemaError('Duplicate TPEx institutional category')
        amounts = tuple(number(v) for v in row[1:])
        if min(amounts[:2]) < 0:
            raise SchemaError('Negative institutional gross trading amount')
        indexed[label] = amounts
    if set(indexed) != GROUPS:
        raise SchemaError('TPEx institutional categories changed')
    residuals = {label: float(v[2] - (v[0] - v[1])) for label, v in indexed.items() if v[2] != v[0] - v[1]}
    constraints = [(label, None, 'buy_minus_sell', float(v[2]), float(v[0] - v[1]))
                   for label, v in indexed.items() if v[2] != v[0] - v[1]]
    for label, parts in [
        ('外資及陸資合計', ['外資及陸資(不含自營商)', '外資自營商']),
        ('自營商合計', ['自營商(自行買賣)', '自營商(避險)']),
        ('三大法人合計*', ['外資及陸資(不含自營商)', '投信', '自營商合計'])]:
        for i, kind in enumerate(('buy', 'sell', 'net')):
            residual = indexed[label][i] - sum(indexed[p][i] for p in parts)
            if residual:
                residuals[label + ':' + kind] = float(residual)
                constraints.append((label, i, kind + '_sum', float(indexed[label][i]), float(sum(indexed[p][i] for p in parts))))
    output = []
    for sid, label, col in (
        ('tpex_foreign_net', '外資及陸資(不含自營商)', 2),
        ('tpex_foreign_buy', '外資及陸資(不含自營商)', 0),
        ('tpex_foreign_sell', '外資及陸資(不含自營商)', 1),
        ('tpex_trust_net', '投信', 2), ('tpex_dealer_prop_net', '自營商(自行買賣)', 2),
        ('tpex_dealer_hedge_net', '自營商(避險)', 2), ('tpex_dealer_net', '自營商合計', 2),
        ('tpex_institutional_net', '三大法人合計*', 2)):
        issues = [issue('unreconciled_institutional_total', field=category + ':' + field,
                    observed=actual, expected=expected, residual=actual - expected,
                    blocking=category == label and (column is None or column == col),
                    scope=sid if category == label and (column is None or column == col) else 'report')
                  for category, column, field, actual, expected in constraints]
        output.append(make_point(SERIES, sid, expected_date, indexed[label][col],
            status='observed', release_at=None, metadata=with_quality(_meta(
                investor_category=label, product_selector='0', reported_unit='TWD',
                quality_flag='unreconciled_institutional_total' if residuals else None,
                reconciliation='reported_value_unreconciled' if any(i['blocking'] for i in issues) else 'exact',
                report_reconciliation='reported_value_unreconciled' if residuals else 'exact',
                reconciliation_residuals_twd=residuals, source_notes=table.get('notes', []),
                excludes_foreign_dealer_from_foreign_total=True,
                flow_type='secondary_market_transactions_not_cross_border_remittance'), issues)))
    return output


def jobs(start: date, end: date) -> list[Job]:
    if start > end:
        raise ValueError('History start must not exceed end')
    from .market_history import has_weekday, month_end
    month, output = start.replace(day=1), []
    while month <= end:
        ym = month.strftime('%Y-%m')
        current = ym == end.strftime('%Y-%m')
        if has_weekday(max(start, month), min(end, month_end(month))):
            output.append(Job(f'tpex_market:{ym}:{start}:{end}', 'tpex_market',
                MARKET_URL + '?date=' + month.strftime('%Y/%m/%d') + '&response=json',
                partial(parse_month, month=ym, start=start.isoformat(), end=end.isoformat()),
                parser_version=MODEL_VERSION, refresh_after_hours=24 if current else 720,
                refresh_at_local='17:00' if current else None, requested_end=end if current else None))
        month = date(month.year + (month.month == 12), month.month % 12 + 1, 1)
    return output


def daily_jobs(trading_dates: list[str]) -> list[Job]:
    today = datetime.now(timezone(timedelta(hours=8))).date()
    output = []
    for day in sorted(set(trading_dates)):
        d = date.fromisoformat(day)
        output.append(Job(f'tpex_institutions:{day}', 'tpex_institutions',
            INSTITUTION_URL + '?date=' + d.strftime('%Y/%m/%d') + '&type=Daily&prod=0&response=json',
            partial(parse_institutions, expected_date=day), parser_version=MODEL_VERSION,
            refresh_after_hours=8760 if d < today - timedelta(days=30) else 24))
    return output + tpex_daily_jobs(trading_dates)
