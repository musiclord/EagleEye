"""Reproducible ex-TSMC estimate with historical weekly capitalization anchors.

This is NOT the official TAIEX ex-TSMC index. Weekly listed-stock capitalization
is a different universe from index-eligible capitalization; see docs/EX_TSMC.md.
"""
from datetime import date, timedelta
from functools import partial
from io import BytesIO
import json
from math import isfinite
from zipfile import ZipFile
from .connectors import Source, SchemaError, ReportNotAvailable, number, observation_date, TWSE_LOCAL_ONLY_NOTE, twse_website_enabled
from .contracts import Job, make_point

WEEK_URL = 'https://www.twse.com.tw/staticFiles/inspection/inspection/week.zip'
METHOD_VERSION = 'weekly-listed-cap-proxy-v1'
MODEL_VERSION = METHOD_VERSION
SOURCES = (
    Source('twse_weekly_cap', 'TWSE 歷史上市股票市值週報', WEEK_URL,
        '每週末初步估計整體上市股票總市值，非完全相同的加權指數成分母體。官方檔含2005年起歷史。' + TWSE_LOCAL_ONLY_NOTE,
        'weekly', 14, publish=False),
    Source('twse_tsmc_shares', 'TWSE 台積電歷史發行股數',
        'https://www.twse.com.tw/fund/MI_QFIIS',
        '從各月末交易日外資持股表取得2330發行股數；樣本間僅向前延用。' + TWSE_LOCAL_ONLY_NOTE, 'monthly', 50, publish=False),
    Source('eagleeye_ex_tsmc', '鷹眼排除台積電估算',
        'https://www.twse.com.tw/downloads/zh/products/indices/IndexS02.pdf',
        '歷史週市值與月度股數估算，非官方指數；未逐筆重建公司行動、指數成分與除數。'
        '輸入來自證交所網站端點，所以只供本機分析，不進公開快照。', publish=False),
)
SERIES = {
    'market_cap_weekly': ('上市股票總市值（週末初估）', 'TWD'),
    'tsmc_issued_shares': ('台積電發行股數（月度抽樣）', 'share'),
    'ex_tsmc_estimated': ('排除台積電（週市值估算）', 'rebased_100'),
    'tsmc_weight_estimated': ('台積電市值權重代理（估算）', 'percent'),
}


def parse_weekly(raw: bytes, start: str, end: str) -> list[dict]:
    import xlrd
    with ZipFile(BytesIO(raw)) as archive:
        members = [x for x in archive.infolist() if x.filename.lower().endswith('.xls')]
        if len(members) != 1 or members[0].file_size > 5_000_000:
            raise SchemaError('Unexpected weekly report archive')
        workbook = xlrd.open_workbook(file_contents=archive.read(members[0]))
    sheet = workbook.sheet_by_index(0)
    if '總市值' not in str(sheet.cell_value(1, 1)) or '億元' not in str(sheet.cell_value(1, 1)):
        raise SchemaError('Weekly capitalization units changed')
    output, seen = [], set()
    for i in range(2, sheet.nrows):
        cell = sheet.cell(i, 0)
        if cell.ctype == xlrd.XL_CELL_DATE:
            day = xlrd.xldate.xldate_as_datetime(cell.value, workbook.datemode).date().isoformat()
        elif cell.ctype == xlrd.XL_CELL_TEXT:
            day = observation_date(cell.value)
        else:
            raise SchemaError('Invalid weekly report date')
        cap = number(sheet.cell_value(i, 1)) * 100_000_000
        if day in seen or cap <= 0:
            raise SchemaError('Duplicate date or invalid weekly capitalization')
        seen.add(day)
        if start <= day <= end:
            output.append(make_point(SERIES, 'market_cap_weekly', day, cap,
                metadata={'publication_time': 'unknown', 'unit_conversion': 'NT$100million * 100000000',
                          'universe': 'listed_stocks_preliminary_not_index_constituents'}))
    if not output:
        raise SchemaError('No weekly capitalization in requested interval')
    return output


def parse_shares(raw: bytes, expected_date: str) -> list[dict]:
    data = json.loads(raw.decode('utf-8-sig'))
    if not isinstance(data, dict):
        raise SchemaError('Issued-share response is not a report object')
    if data.get('stat') != 'OK' or observation_date(data.get('date', '')) != expected_date:
        raise ReportNotAvailable('Issued-share report does not match requested session', expected_date)
    fields = data.get('fields', [])
    if fields[:4] != ['證券代號', '證券名稱', '國際證券編碼', '發行股數']:
        raise SchemaError('Issued-share report fields changed')
    rows = [r for r in data.get('data', []) if r[0] == '2330']
    if len(rows) != 1:
        raise SchemaError('Missing or duplicate 2330 issued-share observation')
    shares = number(rows[0][3])
    if shares <= 0 or shares != int(shares):
        raise SchemaError('Invalid issued-share count')
    return [make_point(SERIES, 'tsmc_issued_shares', expected_date, shares,
        metadata={'sampling': 'monthly_last_available_trading_session', 'publication_time': 'unknown'})]


def jobs(start: date, end: date) -> list[Job]:
    from .market_history import jobs as market_jobs
    if not twse_website_enabled():
        return []
    lower = start - timedelta(days=21)
    # Earlier prices provide an actual historical index level at the prior weekly anchor.
    output = market_jobs(lower, start - timedelta(days=1))
    output.append(Job(key=f'weekly-cap:{lower}:{end}', source_id='twse_weekly_cap', url=WEEK_URL,
        parser=partial(parse_weekly, start=lower.isoformat(), end=end.isoformat()),
        parser_version=METHOD_VERSION, refresh_after_hours=24, requested_end=end))
    return output


def daily_jobs(trading_dates: list[str]) -> list[Job]:
    if not twse_website_enabled():
        return []
    months = {}
    for day in sorted(set(trading_dates)):
        date.fromisoformat(day)
        months[day[:7]] = day
    return [Job(key=f'tsmc-shares:{day}', source_id='twse_tsmc_shares',
        url=f'https://www.twse.com.tw/fund/MI_QFIIS?response=json&date={day.replace("-", "")}&selectType=ALLBUT0999',
        parser=partial(parse_shares, expected_date=day), parser_version=METHOD_VERSION,
        refresh_after_hours=24 * 30) for day in months.values()]


def exclude_return(market_return: float, stock_return: float, lagged_weight: float) -> float:
    """Exact arithmetic for a consistent period's weights/returns; data may be estimates."""
    if not all(isfinite(x) for x in (market_return, stock_return, lagged_weight)):
        raise ValueError('Non-finite return input')
    if not 0 <= lagged_weight < 1:
        raise ValueError('Weight must be in [0, 1)')
    result = (market_return - lagged_weight * stock_return) / (1 - lagged_weight)
    if result <= -1:
        raise ValueError('Impossible ex-stock gross return')
    return result


def build_series(observations: list[dict], start: str, end: str) -> list[dict]:
    lookup, lineage = {}, {}
    for p in observations:
        if p['series_id'] in {'taiex', 'tsmc_close', 'market_cap_weekly', 'tsmc_issued_shares'}:
            series = lookup.setdefault(p['series_id'], {})
            day = p['observation_date']
            value = float(p['value'])
            if value <= 0 or not isfinite(value):
                raise ValueError('Non-positive/non-finite model input')
            if day in series and series[day] != value:
                raise ValueError('Conflicting source observations')
            series[day] = value
            lineage[(p['series_id'], day)] = {
                'series_id': p['series_id'], 'observation_date': day, 'value': value,
                **{field: p.get(field) for field in ('source_id', 'source_url', 'raw_sha256', 'retrieved_at')}}
    index, price, caps, shares = [lookup.get(x, {}) for x in
        ('taiex', 'tsmc_close', 'market_cap_weekly', 'tsmc_issued_shares')]
    days = sorted(d for d in index if start <= d <= end)
    if len(days) < 2 or not caps or not shares:
        return []
    if any(d not in price for d in days):
        raise ValueError('TSMC price missing on a TAIEX session; do not bridge the gap')
    cap_days = sorted(d for d in caps if d in index)
    share_days = sorted(shares)
    output, level = [], 100.0
    base = days[0]
    for i, day in enumerate(days):
        # Only dated anchors at/before this observation; no interpolation from future snapshots.
        eligible_caps = [d for d in cap_days if d <= day]
        eligible_shares = [d for d in share_days if d <= day]
        if not eligible_caps or not eligible_shares:
            raise ValueError(f'No historical capitalization/share anchor at {day}')
        anchor, share_day = eligible_caps[-1], eligible_shares[-1]
        if (date.fromisoformat(day) - date.fromisoformat(anchor)).days > 21:
            raise ValueError('Weekly capitalization anchor is over 21 days old')
        if (date.fromisoformat(day) - date.fromisoformat(share_day)).days > 50:
            raise ValueError('Issued-share anchor is over 50 days old')
        estimated_cap = caps[anchor] * index[day] / index[anchor]
        weight = shares[share_day] * price[day] / estimated_cap
        if not 0 < weight < .95:
            raise ValueError('Implausible estimated TSMC weight')
        if i:
            previous = days[i-1]
            market_return = index[day] / index[previous] - 1
            stock_return = price[day] / price[previous] - 1
            # LAGGED weight: current-day price must never influence its own contribution weight.
            level *= 1 + exclude_return(market_return, stock_return, previous_weight)
        metadata = {'method_version': METHOD_VERSION, 'base_date': base, 'base_value': 100,
            'capitalization_anchor_date': anchor, 'issued_shares_anchor_date': share_day,
            'weight': weight, 'publication_time': 'unknown', 'point_in_time_backtest_eligible': False,
            'input_refs': {
                'current_index': lineage[('taiex', day)],
                'current_tsmc_price': lineage[('tsmc_close', day)],
                'current_capitalization_anchor': lineage[('market_cap_weekly', anchor)],
                'current_anchor_index': lineage[('taiex', anchor)],
                'current_issued_shares_anchor': lineage[('tsmc_issued_shares', share_day)],
                **({'previous_index': lineage[('taiex', previous)],
                    'previous_tsmc_price': lineage[('tsmc_close', previous)],
                    'return_capitalization_anchor': lineage[('market_cap_weekly', previous_anchor)],
                    'return_anchor_index': lineage[('taiex', previous_anchor)],
                    'return_issued_shares_anchor': lineage[('tsmc_issued_shares', previous_share_day)]} if i else {})},
            'coverage': 'weekly listed-stock capitalization; monthly sampled issued shares; no corporate-action/divisor reconstruction',
            'limit': '估算：上市股票母體與TAIEX成分不同；來源目前回溯取得，非當時可得資訊快照。'}
        output.append(make_point(SERIES, 'ex_tsmc_estimated', day, level,
            status='estimated', metadata={**metadata, 'weight_used_for_return': previous_weight if i else None}))
        output.append(make_point(SERIES, 'tsmc_weight_estimated', day, weight * 100,
            status='estimated', metadata=metadata))
        previous_weight, previous_anchor, previous_share_day = weight, anchor, share_day
    return output


def derive(store, start: str | None = None, end: str | None = None):
    from .registry import prefer_publishable
    # TAIEX and the TSMC price arrive from both the website and the OpenAPI; one row per date.
    rows = prefer_publishable(store.observations())
    days = sorted(p['observation_date'] for p in rows if p['series_id'] == 'taiex')
    if not days:
        return []
    end = end or days[-1]
    end_date = date.fromisoformat(end)
    if start is None:
        # Keep the established base when the live window advances. Otherwise stored
        # dates would silently contain index levels calculated from different bases.
        bases = [p.get('metadata', {}).get('base_date') for p in rows
                 if p['series_id'] == 'ex_tsmc_estimated'
                 and p.get('metadata', {}).get('method_version') == METHOD_VERSION]
        start = min((b for b in bases if b), default=None)
    if start is None:
        try:
            start = end_date.replace(year=end_date.year - 1).isoformat()
        except ValueError:
            start = end_date.replace(year=end_date.year - 1, day=28).isoformat()
    points = build_series(rows, start, end)
    return [(SOURCES[-1], points)] if points else []
