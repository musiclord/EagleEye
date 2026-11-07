"""Official TWSE small-cap benchmarks, complementary to (not replacements for) ex-TSMC.

MI_INDEX is the latest dated closing report. It is not a live price feed or a
historical backfill API. This module is pure apart from the engine-owned Job.
Dataset licence: https://data.gov.tw/dataset/11555 (OGDL Taiwan v1).
Method: TWSE IndexS06, April 2023; https://taiwanindex.com.tw/indexes/SC300.
"""
from datetime import date, datetime, timedelta, timezone
import json

from .connectors import Source, SchemaError, number, observation_date, TWSE_OPENAPI_LICENSE
from .contracts import Job, make_point

URL = 'https://openapi.twse.com.tw/v1/exchangeReport/MI_INDEX'
PARSER_VERSION = 'twse-openapi-indices-1'
TAIPEI = timezone(timedelta(hours=8))
DATASET_URL = 'https://data.gov.tw/dataset/11555'
METHOD_URL = 'https://taiwanindex.com.tw/indexes/SC300'
SOURCES = (
    Source('twse_oa_smallcap_indices', 'TWSE OpenAPI 小型股300指數', URL,
           '官方小型股300以加權指數成分股中市值排序第151至450名為採樣，每季審核。'
           '價格與含息報酬分列；不是排除台積電的全市場指數，也不是中小型300指數。'
           '只提供最新交易日收盤值，歷史由擷取累積，漏抓不回填。',
           public_years=10,
           attribution='臺灣證券交易所 {year}〈每日收盤行情-大盤統計資訊〉（OpenAPI v1）',
           license_url=TWSE_OPENAPI_LICENSE),
)
SPECS = {
    '小型股300指數': ('twse_smallcap300_price', 'price'),
    '小型股300報酬指數': ('twse_smallcap300_total_return', 'total_return'),
}
SERIES = {
    'twse_smallcap300_price': ('小型股300價格指數', 'point'),
    'twse_smallcap300_total_return': ('小型股300報酬指數（含息）', 'point'),
}
FIELDS = {'日期', '指數', '收盤指數', '漲跌', '漲跌點數', '漲跌百分比', '特殊處理註記'}


def parse_indices(raw: bytes, today: date | None = None) -> list[dict]:
    """Use the official report date, never the HTTP timestamp or collection date."""
    try:
        rows = json.loads(raw.decode('utf-8-sig'))
    except (UnicodeError, ValueError) as exc:
        raise SchemaError('MI_INDEX is not valid JSON') from exc
    if not isinstance(rows, list) or not rows:
        raise SchemaError('MI_INDEX is empty or not a list of rows')
    if any(not isinstance(row, dict) or set(row) != FIELDS for row in rows):
        raise SchemaError('MI_INDEX fields changed')
    days = {observation_date(row['日期']) for row in rows}
    if len(days) != 1:
        raise SchemaError('MI_INDEX rows carry more than one date')
    day = days.pop()
    if day > (today or datetime.now(TAIPEI).date()).isoformat():
        raise SchemaError('MI_INDEX observation date is after today')
    names = [row['指數'] for row in rows]
    if not all(isinstance(name, str) for name in names) or len(set(names)) != len(names):
        raise SchemaError('MI_INDEX has invalid or duplicate index names')
    selected = {row['指數']: row for row in rows if row['指數'] in SPECS}
    if set(selected) != set(SPECS):
        raise SchemaError('MI_INDEX missing a required small-cap benchmark')
    output = []
    for name, (series_id, measure) in SPECS.items():
        row = selected[name]
        value = number(row['收盤指數'])
        if value <= 0:
            raise SchemaError('MI_INDEX closing index must be positive')
        if row['特殊處理註記'] != '':
            raise SchemaError('MI_INDEX selected index has a special-treatment note requiring review')
        output.append(make_point(SERIES, series_id, day, value, metadata={
            'endpoint': 'MI_INDEX', 'official_index_name': name,
            'session_date_basis': 'official_date_field', 'coverage': 'latest_session_only',
            'measure': measure, 'classification': 'observed', 'license': 'OGDL-Taiwan-1.0',
            'license_evidence_url': DATASET_URL, 'method_url': METHOD_URL,
            'method_version': 'TWSE-SC300-2023-04',
            'universe': 'TAIEX_constituents_ranked_151_to_450_at_quarterly_review',
            'is_ex_tsmc_index': False,
        }))
    return output


def jobs(start: date, end: date) -> list[Job]:
    # Do not attach date parameters or fabricate old observations for a requested range.
    return [Job('twse_oa_smallcap_indices:latest', SOURCES[0].id, URL, parse_indices,
                headers={'Accept': 'application/json'}, parser_version=PARSER_VERSION, refresh_after_hours=2, max_bytes=2_000_000)]
