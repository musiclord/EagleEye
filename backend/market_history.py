"""Date-checked TWSE historical adapters. The ingestion engine owns HTTP/cache/rate limits."""
from datetime import date, datetime, timedelta, timezone
from functools import partial
import json
from .connectors import (Source, SchemaError, ReportNotAvailable, number, observation_date, parse_institutions,
                         parse_margin, TWSE_LOCAL_ONLY_NOTE, twse_website_enabled)
from .contracts import Job, make_point
from .margin_history import parse_twse as parse_credit_history, MODEL_VERSION as CREDIT_PARSER_VERSION

SOURCES = (
    Source('twse_market', 'TWSE 集中市場每日成交與加權指數（網站端點，僅供本機）',
           'https://www.twse.com.tw/exchangeReport/FMTQIK',
           '按月回補官方每日資料；成交額包含有價證券，與普通股指數母體不同。' + TWSE_LOCAL_ONLY_NOTE, publish=False),
    Source('twse_tsmc', 'TWSE 台積電每日股價（網站端點，僅供本機）',
           'https://www.twse.com.tw/exchangeReport/STOCK_DAY',
           '2330 未還原收盤價；與價格指數比較，不包含股利再投資報酬。' + TWSE_LOCAL_ONLY_NOTE, publish=False),
    Source('twse_institutions', 'TWSE 三大法人每日買賣金額（網站端點，僅供本機）',
           'https://www.twse.com.tw/fund/BFI82U',
           '外資及陸資不含外資自營商；包含 ETF 等有價證券。買賣超不等於跨境淨匯出入。' + TWSE_LOCAL_ONLY_NOTE, publish=False),
    Source('twse_margin', 'TWSE 每日融資餘額（網站端點，僅供本機）',
           'https://www.twse.com.tw/exchangeReport/MI_MARGN',
           '今日餘額為初值，重抓保留修訂版本；不涵蓋全部股票質借與場外槓桿。' + TWSE_LOCAL_ONLY_NOTE, publish=False),
)
SERIES = {
    'taiex': ('發行量加權股價指數', 'point'),
    'market_turnover': ('集中市場成交金額', 'TWD'),
    'tsmc_close': ('台積電未還原收盤價', 'TWD/share'),
    'tsmc_turnover': ('台積電成交金額', 'TWD'),
    'foreign_net': ('外資及陸資買賣超（不含外資自營商）', 'TWD'),
    'foreign_buy': ('外資及陸資買進金額（不含外資自營商）', 'TWD'),
    'foreign_sell': ('外資及陸資賣出金額（不含外資自營商）', 'TWD'),
    'institutional_net': ('三大法人買賣超', 'TWD'),
    'trust_net': ('投信買賣超', 'TWD'),
    'dealer_hedge_net': ('自營商避險買賣超', 'TWD'),
    'dealer_prop_net': ('自營商自行買賣買賣超', 'TWD'),
    'margin_balance': ('集中市場融資金額餘額', 'TWD'),
}


def _payload(raw: bytes, session: str | None = None) -> dict:
    data = json.loads(raw.decode('utf-8-sig'))
    if not isinstance(data, dict):
        raise SchemaError('TWSE response is not a report object')
    if data.get('stat') != 'OK':
        # An answered "no matching data" is a publication state, not a schema change.
        raise ReportNotAvailable('TWSE did not return a successful report; never impute a missing session', session)
    return data


def parse_month(raw: bytes, month: str, start: str, end: str, stock: bool = False) -> list[dict]:
    data = _payload(raw, end)
    reported = observation_date(data.get('date', ''))
    if reported[:7] != month:
        raise SchemaError('TWSE returned a different month')
    fields = data.get('fields', [])
    expected = (['日期','成交股數','成交金額','開盤價','最高價','最低價','收盤價','漲跌價差','成交筆數']
                if stock else ['日期','成交股數','成交金額','成交筆數','發行量加權股價指數','漲跌點數'])
    if fields != expected and not (stock and fields == expected + ['註記']):
        raise SchemaError('Historical TWSE columns changed')
    rows = data.get('data', [])
    if not isinstance(rows, list):
        raise SchemaError('Historical TWSE rows are malformed')
    if not rows:
        raise ReportNotAvailable('Historical TWSE report is empty', end)
    result, seen = [], set()
    for row in rows:
        if not isinstance(row, list) or len(row) != len(fields):
            raise SchemaError('Malformed historical TWSE row')
        day = observation_date(row[0])
        if day[:7] != month or day in seen:
            raise SchemaError('Duplicate or out-of-month daily observation')
        seen.add(day)
        price, turnover = number(row[6 if stock else 4]), number(row[2])
        if price <= 0 or turnover < 0:
            raise SchemaError('Invalid closing price or turnover')
        if start <= day <= end:
            for sid, value in [('tsmc_close' if stock else 'taiex', price),
                               ('tsmc_turnover' if stock else 'market_turnover', turnover)]:
                result.append(make_point(SERIES, sid, day, value))
    return result


def parse_daily(raw: bytes, expected_date: str, margin: bool = False) -> list[dict]:
    data = _payload(raw, expected_date)
    if observation_date(data.get('date', '')) != expected_date:
        raise ReportNotAvailable('TWSE returned a different session', expected_date)
    if margin:
        return parse_credit_history(raw, expected_date)
    points = parse_institutions(data)
    for row in data['data']:
        if row[0] == '外資及陸資(不含外資自營商)':
            points += [make_point(SERIES, 'foreign_buy', expected_date, number(row[1])),
                       make_point(SERIES, 'foreign_sell', expected_date, number(row[2]))]
    return points


def has_weekday(start: date, end: date) -> bool:
    """A weekend-only slice cannot contain an exchange session; never request it."""
    return any((start + timedelta(days=i)).weekday() < 5 for i in range(min((end - start).days + 1, 7)))


def month_end(month: date) -> date:
    return date(month.year + (month.month == 12), month.month % 12 + 1, 1) - timedelta(days=1)


def jobs(start: date, end: date) -> list[Job]:
    if start > end:
        raise ValueError('start must not exceed end')
    if not twse_website_enabled():
        return []
    month = start.replace(day=1)
    result = []
    while month <= end:
        ym, compact = month.strftime('%Y-%m'), month.strftime('%Y%m%d')
        current = month.year == end.year and month.month == end.month
        if not has_weekday(max(start, month), min(end, month_end(month))):
            month = date(month.year + (month.month == 12), month.month % 12 + 1, 1)
            continue
        for stock, source_id, endpoint in [(False, 'twse_market', 'FMTQIK'), (True, 'twse_tsmc', 'STOCK_DAY')]:
            url = f'https://www.twse.com.tw/exchangeReport/{endpoint}?response=json&date={compact}'
            if stock:
                url += '&stockNo=2330'
            # The current month's URL is fixed while sessions are appended; the
            # requested end and an after-close boundary decide when to recheck.
            result.append(Job(key=f'{source_id}:{ym}:{start}:{end}', source_id=source_id, url=url,
                parser=partial(parse_month, month=ym, start=start.isoformat(), end=end.isoformat(), stock=stock),
                parser_version='market-history-2', refresh_after_hours=24 if current else 24 * 30,
                refresh_at_local='17:00' if current else None, requested_end=end if current else None))
        month = date(month.year + (month.month == 12), month.month % 12 + 1, 1)
    return result


def daily_jobs(trading_dates: list[str]) -> list[Job]:
    if not twse_website_enabled():
        return []
    result = []
    for day in sorted(set(trading_dates)):
        compact = date.fromisoformat(day).strftime('%Y%m%d')
        for margin, sid, url in [
            (False, 'twse_institutions', f'https://www.twse.com.tw/fund/BFI82U?response=json&dayDate={compact}&type=day'),
            (True, 'twse_margin', f'https://www.twse.com.tw/exchangeReport/MI_MARGN?response=json&date={compact}&selectType=MS'),
        ]:
            result.append(Job(key=f'{sid}:{day}', source_id=sid, url=url,
                parser=partial(parse_daily, expected_date=day, margin=margin),
                parser_version=CREDIT_PARSER_VERSION if margin else 'market-history-2', refresh_after_hours=(8760 if
                    date.fromisoformat(day) < datetime.now(timezone(timedelta(hours=8))).date() - timedelta(days=30) else 24)))
    return result
