"""Exact government-registered TWSE CSV delivery of the current month's market data.

This is a separate resource from openapi.twse.com.tw/exchangeReport/FMTQIK.
No date parameter, website JSON fallback, proxy, or historical request is used.
The MI_5MINS_HIST parser below is an offline cross-check helper, not a second job.
"""
import csv
from datetime import date, datetime, timedelta, timezone
import io
import json

from .connectors import Source, SchemaError, number, observation_date, twse_website_enabled
from .contracts import Job, make_point

URL = 'https://www.twse.com.tw/exchangeReport/FMTQIK?response=open_data'
DATASET_URL = 'https://data.gov.tw/dataset/11672'
HIST_URL = 'https://openapi.twse.com.tw/v1/indicesReport/MI_5MINS_HIST'
HIST_DATASET_URL = 'https://data.gov.tw/dataset/11755'
LICENSE_URL = 'https://data.gov.tw/license'
PARSER_VERSION = 'twse-open-data-market-1'
TAIPEI = timezone(timedelta(hours=8))
FIELDS = ['日期', '成交股數', '成交金額', '成交筆數', '發行量加權股價指數', '漲跌點數']
HIST_FIELDS = {'Date', 'OpeningIndex', 'HighestIndex', 'LowestIndex', 'ClosingIndex'}
SERIES = {'taiex': ('發行量加權股價指數', 'point'), 'market_turnover': ('集中市場成交金額', 'TWD')}
SOURCES = (
    Source('twse_od_market', 'TWSE 開放資料 集中市場每日市場成交資訊', URL,
           '政府資料開放平臺11672精確登錄CSV；只取得目前發布月份，沒有歷史日期參數。'
           '加權指數為價格指數；成交金額是官方集中市場成交合計、新臺幣元，不能當作普通股母體或淨資金流。'
           '資料日期取自原表；漏抓前月不回補，不以擷取日冒充交易日。',
           public_years=10, content_revisions_only=True,
           attribution='臺灣證券交易所 {year}〈集中市場每日市場成交資訊〉（政府開放資料11672）',
           license_url=LICENSE_URL),
)


def _date(value, seen, today):
    day = observation_date(value)
    if day > today.isoformat():
        raise SchemaError('TWSE market observation date is after today')
    if day in seen:
        raise SchemaError('Duplicate market date')
    if seen and day[:7] != next(iter(seen))[:7]:
        raise SchemaError('TWSE current-month resource spans more than one month')
    seen.add(day)
    return day


def _integer(value, field):
    result = number(value)
    if result < 0 or result != result.to_integral_value():
        raise SchemaError(field + ' must be a nonnegative integer')
    return result


def parse_market_csv(raw: bytes, today: date | None = None) -> list[dict]:
    """Reject HTML/empty/schema changes; preserve official dates and exact TWD units."""
    try:
        text = raw.decode('utf-8-sig')
        reader = csv.reader(io.StringIO(text, newline=''), strict=True)
        rows = list(reader)
    except (UnicodeError, csv.Error) as exc:
        raise SchemaError('TWSE open-data market response is not valid UTF-8 CSV') from exc
    if not rows or rows[0] != FIELDS:
        raise SchemaError('TWSE open-data market CSV fields changed; HTML is not a report')
    if len(rows) == 1:
        raise SchemaError('TWSE open-data market CSV has no observations')
    today = today or datetime.now(TAIPEI).date()
    seen, output = set(), []
    for row in rows[1:]:
        if len(row) != len(FIELDS):
            raise SchemaError('TWSE open-data market CSV row width changed')
        day = _date(row[0], seen, today)
        volume = _integer(row[1], 'Trade volume')
        turnover = _integer(row[2], 'Trade value in TWD')
        transactions = _integer(row[3], 'Transactions')
        close, change = number(row[4]), number(row[5])
        if close <= 0:
            raise SchemaError('TAIEX close must be positive')
        shared = {
            'endpoint': 'FMTQIK?response=open_data',
            'delivery': 'government_registered_csv',
            'session_date_basis': 'official_date_field',
            'coverage': 'published_month_only',
            'report_month': day[:7],
            'license': 'OGDL-Taiwan-1.0',
            'license_evidence_url': DATASET_URL,
            'reported_components': {'trade_volume': int(volume), 'transactions': int(transactions),
                                    'taiex_change_points': float(change)},
        }
        output.append(make_point(SERIES, 'taiex', day, close, release_at=None, metadata={
            **shared, 'measure': 'price_index', 'universe': 'official_TAIEX_constituents',
        }))
        output.append(make_point(SERIES, 'market_turnover', day, turnover, release_at=None, metadata={
            **shared, 'measure': 'gross_daily_turnover', 'universe': 'official_FMTQIK_market_total',
            'original_unit': 'TWD', 'unit_multiplier': 1,
            'not_common_stock_only': True, 'not_net_capital_flow': True,
            'included_trading_sessions': ['regular', 'odd_lot', 'after_hours_fixed_price', 'block'],
            'excluded_trading_sessions': ['auction', 'tender_purchase'],
            'foreign_currency_conversion': 'TWSE_announced_FX_rate_at_1530',
        }))
    return sorted(output, key=lambda p: (p['observation_date'], p['series_id']))


def parse_taiex_history(raw: bytes, today: date | None = None) -> list[dict]:
    """Pure alternate parser for fixture cross-checks; no registry source/job is added."""
    try:
        rows = json.loads(raw.decode('utf-8-sig'))
    except (UnicodeError, ValueError) as exc:
        raise SchemaError('MI_5MINS_HIST is not valid JSON') from exc
    if not isinstance(rows, list) or not rows:
        raise SchemaError('MI_5MINS_HIST has no observations')
    seen, output = set(), []
    today = today or datetime.now(TAIPEI).date()
    for row in rows:
        if not isinstance(row, dict) or set(row) != HIST_FIELDS:
            raise SchemaError('MI_5MINS_HIST fields changed')
        day = _date(row['Date'], seen, today)
        opening, high, low, close = (number(row[k]) for k in
                                    ('OpeningIndex', 'HighestIndex', 'LowestIndex', 'ClosingIndex'))
        if not 0 < low <= min(opening, close) <= max(opening, close) <= high:
            raise SchemaError('MI_5MINS_HIST OHLC range is inconsistent')
        output.append(make_point(SERIES, 'taiex', day, close, release_at=None, metadata={
            'endpoint': 'MI_5MINS_HIST', 'session_date_basis': 'official_date_field',
            'coverage': 'published_month_only', 'measure': 'price_index',
            'universe': 'official_TAIEX_constituents', 'license': 'OGDL-Taiwan-1.0',
            'license_evidence_url': HIST_DATASET_URL,
        }))
    return sorted(output, key=lambda p: p['observation_date'])


def jobs(start: date, end: date) -> list[Job]:
    # A requested historical range never becomes unauthorized date query parameters.
    # Retain the existing operational promise: WEBSITE=off means no www.twse calls.
    # This switch is separate from this resource's government-open-data licence.
    if not twse_website_enabled():
        return []
    return [Job('twse_od_market:current', SOURCES[0].id, URL, parse_market_csv,
                headers={'Accept': 'text/csv'}, parser_version=PARSER_VERSION,
                refresh_after_hours=2, max_bytes=500_000)]
