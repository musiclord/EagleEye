"""Allowlisted, offline publication export. Never opens the original DB for writing."""
from __future__ import annotations
import argparse, gzip, hashlib, json, os, re, shutil, tempfile, sys
from pathlib import Path
from datetime import date, datetime, timezone, timedelta
from .lease import DataDirectoryLease
from .publication_policy import date_unconfirmed

# No private event ledger, acknowledgement, notification inbox, logs or runtime credentials.
PRIVATE_KEYS = {'error','errors','note_text','notes','user_note','acknowledged_at','acknowledged_by',
 'project_root','data_directory','database_path','raw_path','path','token','access_token','api_key',
 'process_id','instance_id','event_id','event_ids','record_id','record_ids','read_at','delivery'}

def public_value(value):
    if isinstance(value, dict):
        return {k: public_value(v) for k,v in value.items() if k.lower() not in PRIVATE_KEYS}
    if isinstance(value, (tuple,list)):
        return [public_value(v) for v in value]
    if isinstance(value, str) and (re.search(r'(?<![A-Za-z])[A-Za-z]:[\\/]',value) or '/Users/' in value or '/home/' in value):
        return '[local path omitted]'
    return value

def _local_only_row(value, hidden):
    # Observation/catalogue rows carry source_id; source status rows carry id plus coverage_note.
    return isinstance(value, dict) and (value.get('source_id') in hidden or
        (value.get('id') in hidden and 'coverage_note' in value))

def strip_local_only(value, hidden):
    """Drop every observation, catalogue and source row of publish=False sources (licensed data)."""
    if isinstance(value, dict):
        output = {}
        for key, item in value.items():
            item = strip_local_only(item, hidden)
            if key == 'histories' and isinstance(item, dict):
                item = {sid: points for sid, points in item.items() if points}
            output[key] = item
        return output
    if isinstance(value, (tuple, list)):
        return [strip_local_only(v, hidden) for v in value if not _local_only_row(v, hidden) and not (isinstance(v, dict) and date_unconfirmed(v))]
    return value

def credit_coverage(value, histories, points=None):
    if points is None:
        points = [p for ps in histories.values() for p in ps if p.get('source_id') == 'twse_oa_credit']
    if isinstance(value, list):
        return [credit_coverage(v, histories, points) for v in value if not (
            isinstance(v, dict) and v.get('source_id') == 'twse_oa_credit'
            and 'count' in v and not histories.get(v.get('series_id')))]
    if not isinstance(value, dict):
        return value
    row = {k: credit_coverage(v, histories, points) for k, v in value.items()}
    if row.get('id') == 'twse_oa_credit' and 'coverage_note' in row:
        days = sorted({p['observation_date'] for p in points})
        row.update(first_observation_date=days[0] if days else None,
                   latest_observation_date=days[-1] if days else None, available_dates=len(days))
        from .twse_openapi import SOURCES
        source = next(s for s in SOURCES if s.id == 'twse_oa_credit')
        today = datetime.now(timezone(timedelta(hours=8))).date()
        age = (today - date.fromisoformat(days[-1])).days if days else None
        row.update(stale=age is not None and age > source.freshness_days, age_calendar_days=age,
                   attribution=source.attribution.format(year=(days[-1] if days else today.isoformat())[:4]))
        if not days:
            row.update(status='date_unconfirmed' if row.get('latest_observation_date') or value.get('available_dates',0) else row.get('status','never'),
                       age_calendar_days=None, rows=0)
        note = '公開快照只收日期已確認的觀測；推定日期資料排除。'
        if note not in row['coverage_note']:
            row['coverage_note'] += ' ' + note
    elif row.get('source_id') == 'twse_oa_credit' and 'count' in row:
        days = sorted({p['observation_date'] for p in points if p['series_id'] == row.get('series_id')})
        row.update(start=days[0] if days else None, end=days[-1] if days else None, count=len(days))
    return row


def years_before(day, years):
    try:return day.replace(year=day.year-years)
    except ValueError:return day.replace(year=day.year-years,day=28)  # 29 Feb

def public_coverage(histories, cutoffs):
    """Coverage of windowed sources and complete published series, after trimming.
    A series may join windowed and unrestricted deliveries; its catalogue must count both."""
    days = {'source': {}, 'series': {}}
    for points in histories.values():
        for p in points:
            if p.get('source_id') in cutoffs:
                days['source'].setdefault(p['source_id'], set()).add(p['observation_date'])
            days['series'].setdefault(p['series_id'], set()).add(p['observation_date'])
    return {kind: {k: (min(d), len(d)) for k, d in found.items()} for kind, found in days.items()}

_DROP = object()

def _windowed_row(row, cutoffs, coverage):
    """Rows of windowed sources: observations outside the window are dropped; status and catalogue rows,
    which summarise the full local history, are restated to the published window (only once `coverage` is known).
    Returns the row to publish, _DROP, or None for rows this window does not concern."""
    if 'public_from' in row:                                     # already restated: export applies this twice
        return row
    if coverage is not None and row.get('id') in cutoffs and 'coverage_note' in row:   # source status row
        cutoff = cutoffs[row['id']]
        first, count = coverage['source'].get(row['id'], (None, 0))
        return {**row, 'first_observation_date': first, 'available_dates': count, 'public_from': cutoff,
                'coverage_note': row['coverage_note'] + f"公開快照只收 {cutoff} 以後的資料。"}
    if row.get('source_id') not in cutoffs:
        return None
    cutoff = cutoffs[row['source_id']]
    if isinstance(row.get('observation_date'), str):             # observation row
        return _DROP if row['observation_date'] < cutoff else None
    if coverage is not None and 'start' in row and 'count' in row:   # catalogue row
        if row.get('series_id') not in coverage['series']:
            return _DROP
        first, count = coverage['series'][row['series_id']]
        return {**row, 'start': first, 'count': count, 'public_from': cutoff}
    return None

def trim_public_history(value, cutoffs, coverage=None):
    """Keep only the public window (source_id -> first kept ISO date) of windowed sources; the local DB is never changed.
    Without `coverage` only observation rows are trimmed, which is how the coverage itself is first measured."""
    if not cutoffs:
        return value
    if isinstance(value, dict):
        restated = _windowed_row(value, cutoffs, coverage)
        if restated is not None and restated is not _DROP:
            return restated
        output = {}
        for key, item in value.items():
            item = trim_public_history(item, cutoffs, coverage)
            if key == 'histories' and isinstance(item, dict):
                item = {sid: points for sid, points in item.items() if points}
            output[key] = item
        return output
    if isinstance(value, (tuple, list)):
        return [trim_public_history(v, cutoffs, coverage) for v in value
                if not (isinstance(v, dict) and _windowed_row(v, cutoffs, coverage) is _DROP)]
    return value

# Default-deny TWSE website resources; exception is the exact government-registered dataset URL.
FORBIDDEN_PUBLIC_HOSTS = re.compile(rb'(?<![A-Za-z0-9.-])www\.twse\.com\.tw')

ALLOWED_TWSE_OPEN_RESOURCES = frozenset({
    'https://www.twse.com.tw/exchangeReport/FMTQIK?response=open_data',  # data.gov.tw/dataset/11672
})

def assert_publishable(blob, label):
    """An approved resource is not blanket permission for its host or arbitrary date parameters."""
    def forbidden(value):
        if isinstance(value, dict):
            return any(forbidden(k) or forbidden(v) for k, v in value.items())
        if isinstance(value, list):
            return any(forbidden(v) for v in value)
        return isinstance(value, str) and 'www.twse.com.tw' in value and value not in ALLOWED_TWSE_OPEN_RESOURCES
    if FORBIDDEN_PUBLIC_HOSTS.search(blob):
        try:
            rejected = forbidden(json.loads(blob))
        except (ValueError, UnicodeError):
            rejected = True
        if rejected:
            raise RuntimeError(f'{label} still contains unapproved TWSE website (www.twse.com.tw) content; refusing to publish')
    return blob


def catalogue_from_histories(histories):
    """Catalogue counts/source match the exact observations selected for public analysis."""
    output=[]
    for sid, points in sorted(histories.items()):
        if not points: continue
        last=points[-1];days=sorted({p['observation_date'] for p in points})
        output.append({'series_id':sid, 'label':last['label'], 'unit':last['unit'],
                       'source_id':last['source_id'], 'start':days[0], 'end':days[-1], 'count':len(days)})
    return output


def write_json(path, value):
    encoded=json.dumps(public_value(value),ensure_ascii=False,allow_nan=False,separators=(',',':')).encode()
    path.parent.mkdir(parents=True,exist_ok=True)
    path.write_bytes(encoded)
    return encoded

def export(source: Path, output: Path):
    if 'backend.app' in sys.modules:
        raise RuntimeError('Export must run in a fresh process; an existing API must not be modified')
    source=source.resolve();output=output.resolve()
    if source==output or source in output.parents:
        raise ValueError('Export output must be outside the data directory')
    database=source/'eagleeye.duckdb'
    if not database.is_file():raise ValueError('Existing observation database required')
    # The application uses the same OS lease: copy only when no writer is running.
    with tempfile.TemporaryDirectory(prefix='eagleeye-publish-') as scratch:
        lease=DataDirectoryLease(source).acquire()
        try:
            if Path(str(database)+'.wal').exists():
                raise RuntimeError('Database has a WAL; cleanly open and exit EagleEye before export')
            shutil.copy2(database,Path(scratch)/database.name)
        finally:lease.close()
        os.environ['EAGLEEYE_DATA_DIR']=scratch
        # Import does not enter ASGI lifespan, therefore starts no scheduler or network.
        from . import app as api
        if api.store.directory.resolve()!=Path(scratch).resolve():
            raise RuntimeError('Export store is not the isolated copy')
        try:
            api.store.set_setting('schedule',{'enabled':False,'network_enabled':False,'auto_recovery':False,'local_time':'18:30'})
            from .transmission import build, COMPARISONS, analyse, DEFAULT_SETTINGS
            from .daily_digest import build as digest
            stamp=datetime.now(timezone.utc).isoformat()
            from .registry import local_only_sources, public_history_years
            # Licensed index data stays on this machine: removed before any model or route sees it.
            hidden=local_only_sources()
            # Every route, including computed positions and breadth, now reads public observations only.
            api.PUBLICATION_HIDDEN=frozenset(hidden)
            # Long official histories (e.g. JGB yields since 1974) are published for the last N years only.
            today=datetime.now(timezone.utc).date()
            cutoffs={sid:years_before(today,n).isoformat() for sid,n in public_history_years().items()}
            raw_dashboard=strip_local_only(api.dashboard(),hidden)
            coverage=public_coverage(trim_public_history(raw_dashboard['histories'],cutoffs),cutoffs)
            public=lambda value:credit_coverage(trim_public_history(strip_local_only(value,hidden),cutoffs,coverage),raw_dashboard['histories'])
            dashboard=public(raw_dashboard);histories=dashboard['histories'];sources=dashboard['sources']
            if not any(histories.values()):raise ValueError('Refusing to publish an empty dataset')
            # Publication always uses documented defaults, never personal settings or notes.
            transmission=build(histories,sources,settings=DEFAULT_SETTINGS,acks={})
            brief=digest(histories,sources,generated_at=stamp,previous=None,alerts=transmission['alerts'],events=[])
            brief.update(id='published-'+brief['content_hash'][:24],version=1,reason='published_snapshot',created_at=stamp,attention_count=sum(t['status']=='watch' for t in brief.get('topics',[])))
            brief['publication_note']='本次發布資料的整理；不代表所有變化都發生在今天。'
            schedule={'enabled':False,'network_enabled':False,'auto_recovery':False,'local_time':'18:30',
                      'timezone':'Asia/Taipei','next_run_at':None,'tracking_paused':True,'published':True}
            routes={}
            def save(value,label='route'):
                blob=json.dumps(public_value(public(value)),ensure_ascii=False,allow_nan=False,separators=(',',':')).encode()
                assert_publishable(blob,label)
                name=hashlib.sha256(blob).hexdigest()[:24]+'.json.gz'
                output.mkdir(parents=True,exist_ok=True);(output/name).write_bytes(gzip.compress(blob,compresslevel=9,mtime=0))
                return name
            def route(key,value):routes[key]=save(value,key)
            snapshot=api.snapshot();snapshot['catalog']=catalogue_from_histories(histories);snapshot.update(as_of=stamp,sync=None,schedule=schedule,operations_semantics='published_readonly')
            dashboard.update(as_of=stamp,sync=None,operations_semantics='published_readonly')
            route('/api/snapshot',snapshot);route('/api/dashboard',dashboard)
            route('/api/partners',api.partners());route('/api/sources',api.sources())
            route('/api/schedule',schedule);route('/api/sync',None)
            route('/api/outlook',api.outlook())
            route('/api/market-holidays',api.market_holidays())
            route('/api/ex-tsmc-open',api.ex_tsmc_open())
            route('/api/briefs/latest',{'brief':brief});route('/api/briefs/'+brief['id'],brief)
            route('/api/briefs',{'briefs':[{k:brief.get(k) for k in ('id','digest_date','created_at','generated_at','title','lead','version','attention_count','content_hash','coverage','model_version')}], 'total':1})
            for period in (20,60,120,252):
                route('/api/positions?period='+str(period),api.positions(period))
                route('/api/breadth?period='+str(period),api.breadth(period))
                for pair in COMPARISONS:
                    for window in (20,60,120):
                        route(f"/api/transmission?pair={pair['id']}&period={period}&window={window}",analyse(transmission['histories'],pair,period,window))
            transmission.pop('analysis',None)
            manifest={'schema_version':1,'generated_at':stamp,'mode':'published_readonly',
               'transmission':save(transmission,'transmission'),'routes':routes,'uses_ai':False,
               'privacy':'No raw database, local settings, private notes, events or notification inbox.',
               'series_count':len(histories),'latest_observation':{k:v[-1]['observation_date'] for k,v in histories.items() if v}}
            # Last write makes the manifest an atomic boundary for a new snapshot build.
            temporary=output/'manifest.next.json';write_json(temporary,manifest);temporary.replace(output/'manifest.json')
            active=set(routes.values())|{manifest['transmission']}
            for old in output.iterdir():
                if re.fullmatch(r'[0-9a-f]{24}\.json(?:\.gz)?',old.name) and old.name not in active:
                    old.unlink()
            return {'routes':len(routes),'series':len(histories),'generated_at':stamp,'files':len(set(routes.values()))+2}
        finally:
            api.engine.close();api.application_lease.close()

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--data-dir',required=True,type=Path)
    parser.add_argument('--output',required=True,type=Path)
    args=parser.parse_args()
    print(json.dumps(export(args.data_dir,args.output),ensure_ascii=False))
if __name__=='__main__':main()
