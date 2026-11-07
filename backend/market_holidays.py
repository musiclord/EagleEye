"""Read saved TWSE OpenAPI annual calendar; no fetching, schema creation or inferred dates."""
from datetime import date, datetime, timedelta, timezone
import json
from .publication_calendar import TWSE_URL


def build(store, *, as_of=None, year=None):
    reference = (datetime.fromisoformat(as_of.replace('Z', '+00:00')) if as_of
                 else datetime.now(timezone.utc)).astimezone(timezone(timedelta(hours=8)))
    year = year or reference.year
    result = {'markets': {}, 'coverage': {}, 'note':
              '年度休市表；只列平日全日休市，尚未整合颱風等臨時停市。沒有列出的市場或年度僅依週末判斷。'}
    with store.lock, store.connect() as db:
        if not db.execute("SELECT 1 FROM information_schema.tables WHERE table_name='publication_calendars'").fetchone():
            return result
        condition = ' AND CAST(retrieved_at AS TIMESTAMPTZ)<=CAST(? AS TIMESTAMPTZ)' if as_of else ''
        rows = db.execute("SELECT retrieved_at,raw_sha256,source_url,payload_json FROM publication_calendars "
                          "WHERE calendar_key=?" + condition +
                          " ORDER BY CAST(retrieved_at AS TIMESTAMPTZ) DESC LIMIT 1",
                          [f'calendar:twse:{year}'] + ([as_of] if as_of else [])).fetchall()
    if not rows:
        return result
    stamp, digest, url, payload = rows[0]
    obj = json.loads(payload)
    start, end = f'{year}-01-01', f'{year}-12-31'
    if (url != TWSE_URL or obj.get('kind') != 'market_session' or obj.get('market') != 'twse'
            or obj.get('coverage_start') != start or obj.get('coverage_end') != end):
        return result
    holidays = set()
    for event in obj['events']:
        if event.get('market') != 'twse' or event.get('kind') != 'market_session':
            continue
        day = date.fromisoformat(event['date'])
        if day.year != year:
            return result
        if day.weekday() < 5 and event.get('status') == 'closed' and event.get('basis') == 'official_confirmed':
            holidays.add(day.isoformat())
    result['markets']['tw'] = sorted(holidays)
    result['coverage']['tw'] = {'start': start, 'end': end, 'timezone': 'Asia/Taipei',
        'source_url': url, 'retrieved_at': stamp, 'raw_sha256': digest, 'emergency_closures_loaded': False,
        'attribution': f'臺灣證券交易所 {year}〈開休市日期〉（OpenAPI v1）',
        'license_url': 'https://data.gov.tw/license'}
    return result
