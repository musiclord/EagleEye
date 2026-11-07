"""Versioned official release plans and exchange calendars, separate from value vintages.

Scheduled publication is not proof of actual publication or API availability. This
module never writes observations/release_at and never infers an unsaved vintage.
"""
from __future__ import annotations
import calendar
from datetime import date, datetime, timedelta, timezone
from functools import partial
import hashlib
from html import unescape
import json
import re
from urllib.parse import parse_qs, urlparse
from .contracts import Job
from .connectors import SchemaError
from .store import utc_now

STAT_URL = 'https://www.stat.gov.tw/News_NoticeCalendar.aspx?n=3717&Dept=all&year={year}&month=1&_Showtype=0&PageSize=100'
# OpenAPI (Open Government Data License): the current year only, no year parameter.
TWSE_URL = 'https://openapi.twse.com.tw/v1/holidaySchedule/holidaySchedule'
TPEX_URL = 'https://www.tpex.org.tw/www/zh-tw/bulletin/tradingDate?date={year}&response=json'
TARGETS = {
    '166': ('消費者物價指數 CPI', ['dgbas_cpi']),
    '150': ('受僱員工薪資與工時', ['dgbas_regular_wage']),
    '107': ('海關進出口貿易初步統計', ['mof_exports', 'mof_imports']),
}
NOTE = '官方預告可調整；預定發布不等於已發布或資料 API 已更新。歷史回放只使用當時已擷取的日曆版本。'


def ensure_schema(store):
    with store.lock, store.connect() as db:
        db.execute("""CREATE TABLE IF NOT EXISTS publication_calendars (
            calendar_key VARCHAR, retrieved_at VARCHAR, raw_sha256 VARCHAR,
            source_url VARCHAR, payload_json VARCHAR,
            PRIMARY KEY(calendar_key,retrieved_at))""")
        db.execute("""CREATE TABLE IF NOT EXISTS publication_calendar_attempts (
            calendar_key VARCHAR PRIMARY KEY, attempted_at VARCHAR, status VARCHAR,
            error VARCHAR, next_retry_at VARCHAR)""")


def parse_releases(raw: bytes) -> dict:
    text = raw.decode('utf-8-sig')
    marker = re.search(r'var\s+VueData\s*=\s*', text)
    if not marker: raise SchemaError('Official release calendar VueData is missing')
    try: obj = json.JSONDecoder().raw_decode(text[marker.end():])[0]
    except (ValueError, TypeError) as exc: raise SchemaError('Invalid calendar JSON') from exc
    year = int(obj['year']); year = year + 1911 if year < 1911 else year
    month = int(obj['month'])
    if not 1 <= month <= 12: raise SchemaError('Invalid first calendar month')
    rows = obj.get('list')
    if not isinstance(rows, list): raise SchemaError('Calendar list missing')
    found, events = set(), []
    for item in rows:
        key = parse_qs(urlparse(item.get('ContentUrl', '')).query).get('MetaI_D', [''])[0]
        if key not in TARGETS: continue
        found.add(key)
        label, sources = TARGETS[key]
        dates = item.get('timedatas')
        if not isinstance(dates, list) or len(dates) != 12: raise SchemaError('Expected rolling 12-month release plan')
        for offset, cells in enumerate(dates):
            event_year = year + (month - 1 + offset) // 12
            event_month = (month - 1 + offset) % 12 + 1
            for cell in cells:
                day_text, clock = str(cell['date']), str(cell.get('time', ''))
                # "before day 30" is an official window, not an exact release timestamp.
                match = re.fullmatch(r'(\d{1,2})(日以前)?', day_text)
                if not match: raise SchemaError('Unknown calendar day expression: ' + day_text)
                day = date(event_year, event_month, int(match[1]))
                if clock and not re.fullmatch(r'(?:[01]\d|2[0-3]):[0-5]\d', clock): raise SchemaError('Invalid publication time')
                exact = not match[2]
                period = str(cell.get('notice', '')).strip('()（）')
                stamp = day.isoformat() + 'T' + clock + ':00+08:00' if exact and clock else None
                events.append({'id': 'release:' + key + ':' + day.isoformat() + ':' + period,
                    'date': day.isoformat(), 'label': label, 'kind': 'release',
                    'basis': 'official_confirmed' if exact else 'official_window',
                    'status': 'scheduled', 'source_ids': sources, 'period': period,
                    'scheduled_at': stamp, 'window_start': day.isoformat() if exact else day.replace(day=1).isoformat(),
                    'window_end': day.isoformat(), 'source_url': item['ContentUrl'],
                    'note': item.get('notice') or NOTE})
    if found != set(TARGETS): raise SchemaError('Calendar pagination/schema incomplete; missing target IDs: ' + str(sorted(set(TARGETS)-found)))
    last_year = year + (month + 10) // 12; last_month = (month + 10) % 12 + 1
    return {'kind': 'release', 'events': events, 'coverage_start': date(year, month, 1).isoformat(),
        'coverage_end': date(last_year, last_month, calendar.monthrange(last_year, last_month)[1]).isoformat(),
        'note': NOTE, 'parser_version': 'publication-calendar-1'}


def _calendar_payload(market, year, overrides):
    if len(overrides) < 10: raise SchemaError('Annual exchange calendar is incomplete')
    days = []
    current = date(year, 1, 1)
    while current.year == year:
        day = current.isoformat()
        explicit = overrides.get(day)
        opened = explicit['open'] if explicit else current.weekday() < 5
        label = explicit['label'] if explicit else ('預定交易日' if opened else '週末休市')
        days.append({'id': market + ':' + day, 'date': day, 'label': market.upper() + ' ' + label,
            'kind': 'market_session', 'market': market, 'status': 'open' if opened else 'closed',
            'basis': 'official_confirmed' if explicit else 'annual_calendar_rule',
            'source_ids': [], 'scheduled_at': None,
            'note': '年度開休市表與週末規則；尚未自動整合颱風等臨時停市公告，不能據此斷言資料遺漏。'})
        current += timedelta(days=1)
    return {'kind': 'market_session', 'market': market, 'events': days,
        'coverage_start': f'{year}-01-01', 'coverage_end': f'{year}-12-31',
        'parser_version': 'exchange-calendar-1', 'emergency_closures_loaded': False}


WEEKDAYS = '一二三四五六日'


def parse_twse(raw: bytes, year: int) -> dict:
    """OpenAPI holidaySchedule: rows of Name, Date (ROC yyyMMdd), Weekday, Description."""
    try: rows = json.loads(raw.decode('utf-8-sig'))
    except (ValueError, UnicodeError) as exc: raise SchemaError('Invalid TWSE holiday JSON') from exc
    if not isinstance(rows, list) or not rows: raise SchemaError('Unexpected TWSE annual calendar schema')
    overrides = {}
    for row in rows:
        if not isinstance(row, dict) or set(row) != {'Name', 'Date', 'Weekday', 'Description'}:
            raise SchemaError('Unexpected TWSE holiday row')
        text = str(row['Date'])
        if len(text) != 7 or not text.isdigit(): raise SchemaError('TWSE holiday date is not ROC yyyMMdd')
        try: day = date(int(text[:3]) + 1911, int(text[3:5]), int(text[5:]))
        except ValueError as exc: raise SchemaError('Invalid TWSE holiday date') from exc
        if day.year != year: raise SchemaError('TWSE calendar year mismatch')
        if str(row['Weekday']).strip()[-1:] != WEEKDAYS[day.weekday()]: raise SchemaError('TWSE holiday weekday disagrees with its date')
        name = str(row['Name']).strip()
        overrides[day.isoformat()] = {'label': name, 'open': '開始交易' in name or '最後交易' in name}
    return _calendar_payload('twse', year, overrides)


def parse_tpex(raw: bytes, year: int) -> dict:
    obj = json.loads(raw)
    html = obj.get('data', {}).get('html', '')
    if obj.get('stat') != 'ok' or f'中華民國{year-1911}年' not in html: raise SchemaError('TPEx calendar year/schema mismatch')
    overrides, last_name = {}, ''
    for row in re.findall(r'<tr[^>]*>(.*?)</tr>', html, re.S | re.I):
        cells = [unescape(re.sub(r'<[^>]+>', ' ', x)).strip() for x in re.findall(r'<td[^>]*>(.*?)(?:</td>|(?=<td\b)|$)', row, re.S | re.I)]
        if len(cells) == 4: last_name, dates, _, note = cells
        elif len(cells) == 3: dates, _, note = cells
        else: continue
        if '債券' in last_name: continue  # Stock-market calendar, not international bonds.
        opened = '開始交易' in last_name or '最後交易' in last_name
        pairs = re.findall(r'(\d{1,2})月(\d{1,2})日', dates)
        if not pairs: raise SchemaError('TPEx calendar date missing')
        for month, day in pairs:
            overrides[date(year, int(month), int(day)).isoformat()] = {'label': last_name, 'open': opened}
        if '股票交易系統' in last_name and '市場無交易' in note:
            for month, day in re.findall(r'(\d{1,2})月(\d{1,2})日', note):
                overrides[date(year, int(month), int(day)).isoformat()] = {'label': '市場無交易，僅辦理給付結算', 'open': False}
    return _calendar_payload('tpex', year, overrides)


def calendar_jobs(year: int):
    if not 2006 <= year <= date.today().year + 1: raise ValueError('Calendar year outside supported range')
    return [Job('calendar:releases', 'calendar_releases', STAT_URL.format(year=year), parse_releases,
                headers={'User-Agent': 'Mozilla/5.0 EagleEye public-calendar reader'}, refresh_after_hours=24),
            Job(f'calendar:twse:{year}', 'calendar_twse', TWSE_URL, partial(parse_twse, year=year), refresh_after_hours=24),
            Job(f'calendar:tpex:{year}', 'calendar_tpex', TPEX_URL.format(year=year), partial(parse_tpex, year=year), refresh_after_hours=24)]


def refresh_calendar(store, fetch, year=None):
    """At most three official fetches; cooldown survives process restarts."""
    ensure_schema(store)
    year = year or datetime.now(timezone(timedelta(hours=8))).year
    results = []
    for job in calendar_jobs(year):
        now = datetime.now(timezone.utc)
        with store.lock, store.connect() as db:
            previous = db.execute('SELECT attempted_at,status,next_retry_at FROM publication_calendar_attempts WHERE calendar_key=?', [job.key]).fetchone()
        if previous:
            due = datetime.fromisoformat(previous[2]) if previous[2] else datetime.fromisoformat(previous[0])+timedelta(hours=24)
            if now < due:
                results.append({'key': job.key, 'status': 'cached' if previous[1]=='ok' else 'cooldown', 'next_retry_at': due.isoformat()}); continue
        stamp = utc_now()
        try:
            raw = fetch(job); payload = job.parser(raw)
            digest = store.cache_raw(raw)
            with store.lock, store.connect() as db:
                db.execute('INSERT INTO publication_calendars VALUES (?,?,?,?,?)', [job.key, stamp, digest, job.url, json.dumps(payload, ensure_ascii=False)])
                db.execute('INSERT OR REPLACE INTO publication_calendar_attempts VALUES (?,?,?,?,?)', [job.key, stamp, 'ok', None, None])
            results.append({'key': job.key, 'status': 'ok', 'events': len(payload['events']), 'retrieved_at': stamp})
        except Exception as exc:
            # Site authorization/rate limits and format failures never loop immediately.
            retry_at = (now+timedelta(hours=24 if getattr(exc, 'code', None) in (401,403,429) or isinstance(exc,(SchemaError,ValueError)) else 1)).isoformat()
            error = type(exc).__name__ + ': ' + str(exc)[:250]
            with store.lock, store.connect() as db:
                db.execute('INSERT OR REPLACE INTO publication_calendar_attempts VALUES (?,?,?,?,?)', [job.key, stamp, 'error', error, retry_at])
            results.append({'key': job.key, 'status': 'error', 'error': error, 'next_retry_at': retry_at})
    return {'status': 'partial' if any(x['status'] in ('error','cooldown') for x in results) else 'completed', 'results': results}


def _rules():
    from .registry import source_map
    output = []
    for sid, source in source_map().items():
        if 'derived' in sid: continue
        freq = source.frequency
        if freq == 'monthly':
            window = {'dgbas_cpi': (1,15,1), 'dgbas_regular_wage': (10,20,2), 'mof_exports': (1,15,1), 'mof_imports': (1,15,1), 'moea_demand': (20,28,1)}.get(sid, (1,28,1))
            description = f'觀測月後第{window[2]}個月 {window[0]}–{window[1]} 日檢查；本地監測窗口，非官方承諾。'
        elif freq == 'event':
            window = None; description = '事件型資料每週檢查來源；沒有新事件不代表資料過期。'
        elif freq == 'semiannual':
            window = None; description = '國際機構半年發布展望（IMF 約 4／10 月、OECD 約 6／12 月）；每 3 日檢查新版本，非官方發布時間。'
        else:
            window = None; description = '工作日盤後至後兩日檢查；跨國假日、時區和官方更新延遲另計，非精確發布時間。'
        output.append({'source_id': sid, 'label': source.label, 'frequency': freq, 'basis': 'expected_rule', 'description': description, 'month_window': window, 'source_url': source.url})
    return output


def calendar_view(store, start=None, end=None, as_of=None):
    ensure_schema(store)
    reference = datetime.fromisoformat(as_of.replace('Z','+00:00')).astimezone(timezone(timedelta(hours=8))).date() if as_of else datetime.now(timezone(timedelta(hours=8))).date()
    start = date.fromisoformat(start) if isinstance(start,str) else (start or reference-timedelta(days=7))
    end = date.fromisoformat(end) if isinstance(end,str) else (end or reference+timedelta(days=35))
    if start>end or (end-start).days>740: raise ValueError('Calendar range must be 0–740 days')
    with store.lock, store.connect() as db:
        where, params = ('WHERE CAST(retrieved_at AS TIMESTAMPTZ)<=CAST(? AS TIMESTAMPTZ)', [as_of]) if as_of else ('', [])
        rows = db.execute(f'SELECT calendar_key,retrieved_at,raw_sha256,source_url,payload_json FROM publication_calendars {where} QUALIFY row_number() OVER (PARTITION BY calendar_key ORDER BY CAST(retrieved_at AS TIMESTAMPTZ) DESC)=1', params).fetchall()
        attempts = [] if as_of else [dict(zip(['key','attempted_at','status','error','next_retry_at'], row)) for row in db.execute('SELECT * FROM publication_calendar_attempts').fetchall()]
    events, coverage, official_months = [], [], set()
    for key, stamp, digest, url, payload in rows:
        obj = json.loads(payload)
        coverage.append({'key': key, 'retrieved_at': stamp, 'start': obj['coverage_start'], 'end': obj['coverage_end'], 'source_url': url, 'raw_sha256': digest, 'emergency_closures_loaded': obj.get('emergency_closures_loaded')})
        for event in obj['events']:
            # Suppress a local estimate whenever the selected calendar version
            # already has an official plan for that source/month, even if its
            # exact release day lies outside the displayed date range.
            if event['kind']=='release' and event['basis'] in ('official_confirmed','official_window'):
                official_months.update((sid,event['date'][:7]) for sid in event['source_ids'])
            lower=event.get('window_start') or event['date']
            upper=event.get('window_end') or event['date']
            if lower<=end.isoformat() and upper>=start.isoformat():
                events.append({**event, 'source_url': event.get('source_url',url), 'retrieved_at': stamp, 'raw_sha256': digest})
    rules = _rules()
    # Rules are deliberately windows, never synthesized scheduled_at values.
    current = start.replace(day=1)
    while current <= end:
        for rule in rules:
            if not rule['month_window']: continue
            lower, upper, lag = rule['month_window']; day = current.replace(day=min(upper,calendar.monthrange(current.year,current.month)[1]))
            window_start=current.replace(day=lower)
            if window_start>end or day<start: continue
            if (rule['source_id'],day.isoformat()[:7]) in official_months: continue
            idx=current.year*12+current.month-1-lag; period=f'{idx//12:04d}-{idx%12+1:02d}'
            events.append({'id':'rule:'+rule['source_id']+':'+day.isoformat()[:7], 'date':day.isoformat(), 'label':rule['label'],
                'kind':'release', 'basis':'expected_rule', 'status':'expected', 'source_ids':[rule['source_id']], 'period':period,
                'scheduled_at':None, 'window_start':current.replace(day=lower).isoformat(), 'window_end':day.isoformat(),
                'source_url':rule['source_url'], 'retrieved_at':None, 'note':rule['description']})
        idx=current.year*12+current.month; current=date(idx//12,idx%12+1,1)
    return {'events':sorted(events,key=lambda x:(x['date'],x['id'])), 'coverage':coverage, 'rules':rules,
        'attempts':attempts, 'as_of':as_of, 'start':start.isoformat(), 'end':end.isoformat(), 'note':NOTE}
