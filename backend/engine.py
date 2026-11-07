"""Resumable, bounded and rate-limited official-source ingestion in the API's sole process."""
from datetime import date,datetime,timedelta,timezone
from dataclasses import replace
import json
import re
import threading
import time
import urllib.request
import urllib.error
from urllib.parse import urlparse,parse_qs
import uuid
from .registry import modules,source_map
from .store import utc_now
from .contracts import Job,NOT_FOUND_RAW
from .connectors import ReportNotAvailable, twse_website_enabled

TAIPEI=timezone(timedelta(hours=8))
TX_REPAIR_SERIES=tuple(f"tx_{owner}_oi_{leg}" for owner in ("foreign","trust","dealer")
                       for leg in ("long","short","net"))+("tx_market_oi_total",)


def cache_decision(job, cached, raw_exists, now=None):
    """Return independent HTTP freshness and parsing coverage decisions.

    A weekday publication boundary requests at most one additional check after
    that boundary. It is a retrieval policy, NOT a complete bank-holiday calendar
    or a claim that the source must publish a row on that day.
    """
    if not cached or cached.get('status') != 'ok' or not raw_exists:
        return False, False, 'missing_successful_raw'
    signature=cached.get('request_signature')
    legacy_get=(signature is None and job.method.upper()=='GET' and not job.body
                and cached.get('url')==job.url and cached.get('source_id')==job.source_id)
    if signature != job.request_signature and not legacy_get:
        return False, False, 'request_changed'
    now=now or datetime.now(timezone.utc)
    fetched=datetime.fromisoformat(cached['fetched_at'])
    age=(now-fetched).total_seconds()
    if age<0 or age>=job.refresh_after_hours*3600:
        return False, False, 'ttl_expired'
    coverage_end=job.requested_end or _scope_end(job.parse_scope)
    if coverage_end:
        # A capture cannot contain sessions or releases dated after it was taken.
        # Weekends add no request: only the latest weekday up to today counts.
        wanted=min(coverage_end,now.astimezone(TAIPEI).date())
        while wanted.weekday()>4:wanted-=timedelta(days=1)
        if wanted>fetched.astimezone(TAIPEI).date():
            return False, False, 'requested_after_fetch'
    if job.refresh_at_local:
        local=now.astimezone(TAIPEI)
        hour,minute=map(int,job.refresh_at_local.split(':'))
        boundary=local.replace(hour=hour,minute=minute,second=0,microsecond=0)
        if boundary>local:boundary-=timedelta(days=1)
        while boundary.weekday()>4:boundary-=timedelta(days=1)
        # Historical requests do not need a new current-day publication check.
        if (job.requested_end is None or job.requested_end>=boundary.date()) and fetched<boundary:
            return False, False, 'publication_recheck'
    if cached.get('parser_version')!=job.parser_version:
        return True, False, 'parser_changed'
    if (cached.get('parse_scope') or '')!=job.parse_scope:
        return True, False, 'parse_scope_changed'
    # Legacy adapters encode parsing coverage in the job key. Their request
    # identity may still share raw bytes without assuming parsing is complete.
    if cached.get('job_key') and cached['job_key']!=job.key:
        return True, False, 'parse_job_changed'
    return True, True, 'fresh'


def _scope_end(scope):
    try:return date.fromisoformat((scope or '').rsplit(':',1)[1])
    except (IndexError,ValueError):return None


def repair_jobs(jobs,repairs):
    """A known missing TX date must not be hidden by a fresh, incomplete CSV cache."""
    output=[]
    for job in jobs:
        missing=repairs.get(job.source_id,[])
        if missing and job.method=="POST" and job.body:
            params=parse_qs(job.body.decode("ascii"))
            starts=params.get("queryStartDate",[]);ends=params.get("queryEndDate",[])
            if len(starts)==len(ends)==1:
                lower=starts[0].replace("/","-");upper=ends[0].replace("/","-")
                if any(lower<=day<=upper for day in missing):
                    job=replace(job,refresh_after_hours=0)
        output.append(job)
    return output

ALLOWED_HOSTS={"www.twse.com.tw","openapi.twse.com.tw","www.taifex.com.tw","cpx.cbc.gov.tw",
               "stat.fsc.gov.tw","www.fsc.gov.tw","www.sfb.gov.tw","service.mof.gov.tw",
               "service.moea.gov.tw","www.moea.gov.tw","nstatdb.dgbas.gov.tw","www.stat.gov.tw","www.cbc.gov.tw","ws.dgbas.gov.tw","home.treasury.gov","www.stat-search.boj.or.jp","www.tpex.org.tw",
               "api.imf.org","sdmx.oecd.org",
               "www.federalreserve.gov","fred.stlouisfed.org","www.csindex.com.cn","www.mof.go.jp","www.hkex.com.hk","ticdata.treasury.gov"}
# Minimum seconds between request starts to one host; others use the TWSE-family/default rule.
HOST_MIN_INTERVAL={"www.hkex.com.hk":1.1}
def reference_end():
    now=datetime.now(TAIPEI)
    return now.date() if now.hour>=17 else now.date()-timedelta(days=1)


MAX_RECOVERY_ATTEMPTS=3
# An answered "no report for this session yet" is a publication state. Keep
# asking for a few calendar days, then escalate it for review.
PUBLICATION_WAIT_DAYS=4
MAX_AWAITING_ATTEMPTS=8
UNPUBLISHED=('awaiting_publication','official_report_missing')
LEGACY_UNPUBLISHED_ERRORS=('TWSE did not return a successful report','Issued-share report does not match requested session',
    'Breadth report missing or returned a different session','TWSE credit report is missing or returned a different session',
    'TPEx credit report is missing or returned a different session','TPEx did not return a successful dated report',
    'TPEx institutional report returned a different session','TWSE returned a different session','Historical TWSE report is empty')
SCHEDULE_DEFAULTS={'enabled':True,'auto_recovery':True,'network_enabled':True,'local_time':'18:30'}
MAX_DAILY_RESTARTS=3
MAX_RUNTIME_RECOVERIES=8
RECOVERY_INTERVAL_MINUTES=30


def _taipei(now=None):
    value=now or datetime.now(TAIPEI)
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError('排程時間必須包含時區')
    return value.astimezone(TAIPEI)


def _deadline_after(value,now):
    return bool(value and datetime.fromisoformat(value)>now)


def _schedule_settings(store):
    stored=store.setting('schedule',{})
    return {**SCHEDULE_DEFAULTS,**stored}


def _schedule_due(now,settings):
    hour,minute=map(int,settings['local_time'].split(':'))
    today_at=now.replace(hour=hour,minute=minute,second=0,microsecond=0)
    due_date=now.date() if now>=today_at else now.date()-timedelta(days=1)
    next_at=today_at if now<today_at else today_at+timedelta(days=1)
    return due_date.isoformat(),next_at



def _session_date(value):
    if isinstance(value,date):return value
    try:return date.fromisoformat(str(value))
    except (TypeError,ValueError):return None


def _key_session(key):
    return _session_date((key or '').rsplit(':',1)[-1])


def _same_request(row,job):
    request=row.get('request') or {}
    return (request.get('url')==job.url and (request.get('method') or 'GET').upper()==job.method.upper()
            and (request.get('body_hex') or None)==(job.body.hex() if job.body else None))


def reclassify_unpublished(store,now=None):
    """Earlier versions filed an answered "no report yet" as a schema failure and blocked
    the whole source for 24 hours. Re-file those rows; the original error text is kept."""
    ensure_recovery_schema(store)
    today=_taipei(now).date() if now else datetime.now(TAIPEI).date()
    changed=0
    with store.lock,store.connect() as db:
        for key,payload in db.execute('SELECT job_key,payload_json FROM recovery_jobs').fetchall():
            row=json.loads(payload)
            if row.get('category')!='schema' or row.get('status') in ('recovered','superseded'):continue
            if not any(text in (row.get('error') or '') for text in LEGACY_UNPUBLISHED_ERRORS):continue
            session=_key_session(key)
            recent=session is not None and (today-session).days<=PUBLICATION_WAIT_DAYS
            stamp=utc_now()
            row.update(category='awaiting_publication' if recent else 'official_report_missing',retryable=recent,
                session_date=session.isoformat() if session else None,reclassified_from='schema',updated_at=stamp,
                next_retry_at=stamp if recent else row.get('next_retry_at'))
            if recent:row['attempts']=min(row.get('attempts',0),MAX_AWAITING_ATTEMPTS-1)
            db.execute('UPDATE recovery_jobs SET updated_at=?,payload_json=? WHERE job_key=?',[stamp,json.dumps(row,ensure_ascii=False),key])
            changed+=1
        for source_id,payload in db.execute('SELECT source_id,payload_json FROM recovery_sources').fetchall():
            policy=json.loads(payload)
            if policy.get('category')=='schema' and any(text in (policy.get('error') or '') for text in LEGACY_UNPUBLISHED_ERRORS):
                db.execute('DELETE FROM recovery_sources WHERE source_id=?',[source_id])
                changed+=1
    return changed


def ensure_recovery_schema(store):
    with store.lock,store.connect() as db:
        db.execute("CREATE TABLE IF NOT EXISTS recovery_jobs (job_key VARCHAR PRIMARY KEY, source_id VARCHAR, updated_at VARCHAR, payload_json VARCHAR)")
        db.execute("CREATE TABLE IF NOT EXISTS recovery_sources (source_id VARCHAR PRIMARY KEY, updated_at VARCHAR, payload_json VARCHAR)")


def _planned_daily_jobs(store,adapters,start,end):
    dates=sorted({p['observation_date'] for p in store.observations('taiex',start=start.isoformat(),end=end.isoformat())})
    daily=[]
    for module in adapters:
        factory=getattr(module,'daily_jobs',None)
        if factory:
            calendar=sorted({p['observation_date'] for p in store.observations('taiex')}) if module.__name__.endswith('ex_tsmc') else dates
            if hasattr(module,'DAILY_CALENDAR'):
                calendar=sorted({p['observation_date'] for p in store.observations(module.DAILY_CALENDAR,start=start.isoformat(),end=end.isoformat())})
            daily.extend(factory(calendar))
        tpex_factory=getattr(module,'tpex_daily_jobs',None)
        if tpex_factory:
            tpex_dates=sorted({p['observation_date'] for p in store.observations('tpex_index',start=start.isoformat(),end=end.isoformat())})
            daily.extend(tpex_factory(tpex_dates))
        # Jobs whose parser must read what this run already stored (e.g. an undated OpenAPI file).
        store_factory=getattr(module,'store_daily_jobs',None)
        if store_factory:
            daily.extend(store_factory(store,dates))
    return daily



def _adapter_jobs(module, store, start, end, mode='incremental'):
    """Store-aware discovery remains a plan: all actual HTTP still runs through _batch."""
    factory = getattr(module, 'store_jobs', None)
    if factory:
        return list(factory(store, start, end, mode=mode))
    factory = getattr(module, 'jobs', None)
    return list(factory(start, end)) if factory else []


class _SourceRedirectHandler(urllib.request.HTTPRedirectHandler):
    """Check a discovered resource before urllib opens the redirect destination."""
    def __init__(self, validate):
        super().__init__()
        self.validate = validate

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        self.validate(newurl)
        return super().redirect_request(req, fp, code, msg, headers, newurl)


def _source_url_validator(source_id):
    for module in modules():
        validate = getattr(module, 'validate_fetch_url', None)
        if validate and any(s.id == source_id for s in getattr(module, 'SOURCES', ())):
            return validate
    return None


def _planned_lookup(store,adapters,ranges):
    """Rebuild only registered jobs and cached followups, with no HTTP."""
    planned=[]
    for start,end in ranges:
        for module in adapters:
            planned.extend(_adapter_jobs(module,store,start,end,mode='backfill'))
            if hasattr(module,'store_jobs'):
                planned.extend(_adapter_jobs(module,store,start,end,mode='incremental'))
        planned.extend(_planned_daily_jobs(store,adapters,start,end))
    lookup={job.key:job for job in planned}
    with store.lock,store.connect() as db:
        checkpoints=dict(db.execute('SELECT job_key,raw_sha256 FROM fetch_checkpoints WHERE raw_sha256 IS NOT NULL').fetchall())
    queue=list(lookup.values())
    for job in queue:
        if len(lookup)>20000:raise ValueError('重建工作超過安全上限')
        if not job.followups:continue
        digest=checkpoints.get(job.key)
        if not digest:
            capture=store.request_checkpoint(job)
            digest=capture.get('raw_sha256') if capture and capture.get('status')=='ok' else None
        if not digest:continue
        path=store.raw_dir/(digest+'.json')
        if not path.exists():continue
        try:children=job.followups(path.read_bytes())
        except Exception:continue
        for child in children:
            if child.key not in lookup:lookup[child.key]=child;queue.append(child)
    return lookup


def _legacy_disposition(error):
    """Historical error text has no reliable attempt/cooldown history."""
    error=error or ''
    http=re.search(r'(?:HTTP Error|HTTPError:?)\s*(\d{3})',error)
    if http:
        code=int(http[1])
        return ('http_'+str(code) if code!=429 else 'rate_limit'),False
    if any(kind in error for kind in ('SchemaError','ValueError','KeyError','TypeError','格式/日期驗證失敗')):
        return 'schema',False
    # Even transport-looking legacy rows lack a durable retry count/Retry-After.
    # A normal sync can revalidate them; an error-only row must not silently gain
    # automatic retry eligibility after an upgrade.
    return 'legacy_requires_review',False


def _recovery_rows(store):
    with store.lock,store.connect() as db:
        rows=[json.loads(r[0]) for r in db.execute("SELECT payload_json FROM recovery_jobs").fetchall()]
        legacy=db.execute("SELECT job_key,source_id,url,parser_version,fetched_at,error FROM fetch_checkpoints WHERE status='error'").fetchall()
    known={r['job_key'] for r in rows}
    latest=store.get_sync() or {}
    end=latest.get('end') or reference_end().isoformat()
    start=latest.get('start') or (date.fromisoformat(end)-timedelta(days=366)).isoformat()
    pending=[record for record in legacy if record[0] not in known]
    lookup={}
    if pending:
        source_ids={record[1] for record in pending}
        adapters=[module for module in modules() if not hasattr(module,'SOURCES') or any(source.id in source_ids for source in module.SOURCES)]
        try:lookup=_planned_lookup(store,adapters,{(date.fromisoformat(start),date.fromisoformat(end))})
        except Exception:pass  # Remain review-required; malformed plans never authorize HTTP.
    for key,source,url,version,stamp,error in pending:
        category,retryable=_legacy_disposition(error)
        planned=lookup.get(key)
        reconstructable=bool(planned and planned.source_id==source)
        rows.append({'job_key':key,'source_id':source,'status':'requires_review','attempts':0,'retryable':retryable,
            'category':category if reconstructable else 'needs_adapter_update','original_error_category':category,
            'next_retry_at':None,'error':error,'updated_at':stamp,
            'parser_version':version,'current_parser_version':planned.parser_version if reconstructable else None,
            'start':start,'end':end,'request':{'url':url},'reconstructable':reconstructable,
            'requires_review':True,'review_reason':'舊錯誤沒有完整重試紀錄；請由一般同步重新驗證來源。' if reconstructable else
                '目前 adapter 與已快取來源索引無法重建此舊工作；請檢查替代工作或更新來源索引，歷史錯誤仍保留。'})
    return rows


def recovery_view(store):
    ensure_recovery_schema(store)
    now=datetime.now(timezone.utc)
    rows=[]
    for row in _recovery_rows(store):
        if row['status'] in ('recovered','superseded'):continue
        due=not row.get('next_retry_at') or datetime.fromisoformat(row['next_retry_at'])<=now
        limit=MAX_AWAITING_ATTEMPTS if row.get('category')=='awaiting_publication' else MAX_RECOVERY_ATTEMPTS
        rows.append({**row,'eligible':row.get('retryable',False) and row.get('attempts',0)<limit and due})
    with store.lock,store.connect() as db:
        sources=[json.loads(r[0]) for r in db.execute('SELECT payload_json FROM recovery_sources').fetchall()]
    source_states={r['source_id']:r for r in sources}
    for row in rows:
        source=source_states.get(row['source_id'],{})
        row['source_attempts']=source.get('attempts',0)
        source_due=not source.get('next_retry_at') or datetime.fromisoformat(source['next_retry_at'])<=now
        row['eligible']=row['eligible'] and row['source_attempts']<MAX_RECOVERY_ATTEMPTS and source_due and source.get('retryable',True)
    return {'jobs':sorted(rows,key=lambda r:(r['source_id'],r['job_key'])), 'sources':sources,
        'pending':len(rows),'eligible':sum(r['eligible'] for r in rows),'requires_review':sum(bool(r.get('requires_review')) for r in rows),'max_attempts':MAX_RECOVERY_ATTEMPTS,
        'awaiting_publication':sum(r.get('category')=='awaiting_publication' for r in rows),
        'max_awaiting_attempts':MAX_AWAITING_ATTEMPTS,'publication_wait_days':PUBLICATION_WAIT_DAYS,
        'note':'補抓只重建已登錄 adapter 的工作；403／格式錯誤不自動重試，429 遵守冷卻時間。官方回覆「尚無該日報表」時，'
            f'在交易日後 {PUBLICATION_WAIT_DAYS} 個日曆日內稍後重查，不封鎖同來源的其他日期；超過期限才列為官方報表缺漏待查。'}


def _failure_policy(exc,attempts):
    if isinstance(exc,ReportNotAvailable):
        session=_session_date(getattr(exc,'session_date',None))
        if session and (datetime.now(TAIPEI).date()-session).days<=PUBLICATION_WAIT_DAYS:
            return 'awaiting_publication',True,min(2700*2**min(attempts-1,3),21600)
        return 'official_report_missing',False,86400
    code=getattr(exc,'code',None)
    if code in (400,401,403,404):return 'http_'+str(code),False,86400
    if code==429:
        seconds=3600
        header=exc.headers.get('Retry-After') if getattr(exc,'headers',None) else None
        if header:
            try:seconds=max(seconds,int(header))
            except ValueError:
                try:
                    from email.utils import parsedate_to_datetime
                    seconds=max(seconds,int((parsedate_to_datetime(header)-datetime.now(timezone.utc)).total_seconds()))
                except (TypeError,ValueError,OverflowError):pass
        return 'rate_limit',True,min(seconds,7*86400)
    if isinstance(exc,(ValueError,KeyError,TypeError)):return 'schema',False,86400
    return ('http_'+str(code) if code else 'transport'),True,min(300*2**min(attempts-1,6),21600)


class NetworkDisabled(RuntimeError):
    pass


class DeferredSource(Exception):
    def __init__(self,policy):
        self.policy=policy
        super().__init__('來源暫停；'+policy.get('error','等待冷卻或修正格式'))


class Engine:
    def __init__(self,store):
        self.store=store
        ensure_recovery_schema(store)
        reclassify_unpublished(store)
        self.after_sync=None
        self.on_tick=None
        self.schedule_lock=threading.Lock()
        self.lock=threading.Lock()
        self.last_host={}
        self.stop_event=threading.Event()
        self.thread=None
        old=store.get_sync()
        if old and old["status"]=="running":
            old.update(status="interrupted",finished_at=utc_now(),message="服務重啟；已完成的下載檢查點保留，可安全續跑。")
            store.save_sync(old)
            daily=store.setting('runtime_daily',{})
            if daily.get('run_id')==old['id'] or daily.get('status')=='launching':
                daily.update(status='interrupted',finished_at=utc_now())
                store.set_setting('runtime_daily',daily)
    def start(self,mode="incremental",start=None,end=None,source_ids=None,job_keys=None,max_jobs=100,scheduled_for=None,trigger='manual'):
        if self.store.setting('tracking_paused',False):raise RuntimeError('追蹤已暫停，請先恢復追蹤。')
        if self.stop_event.is_set():raise RuntimeError('應用程式正在退出。')
        if not _schedule_settings(self.store)['network_enabled'] or self.store.setting('tracking_paused',False):
            raise RuntimeError('目前為離線模式；僅讀取本機資料，請先允許資料連線。')
        if self._network_block():
            raise RuntimeError('多個來源連線失敗後暫緩下載；請待連線冷卻時間結束。')
        if not self.lock.acquire(blocking=False):
            raise RuntimeError("已有同步正在進行")
        try:
            if mode not in ("incremental","backfill","retry_failed"):raise ValueError("未知同步模式")
            if not 1<=max_jobs<=200:raise ValueError("補抓上限必須介於1與200")
            if mode=="retry_failed":
                return self._start_recovery(source_ids,job_keys,max_jobs,scheduled_for,trigger)
            end=end or reference_end()
            repairs={}
            if start is None:
                start=end-timedelta(days=366 if mode=="backfill" else 14)
                if mode=="incremental":
                    # Repair gaps from interrupted/failed history, and catch up after long shutdowns.
                    calendar={p["observation_date"] for p in self.store.observations("taiex",start=(end-timedelta(days=366)).isoformat(),end=end.isoformat())}
                    if calendar:
                        start=min(start,date.fromisoformat(max(calendar))-timedelta(days=1))
                        # Only the website endpoints can fill an old TWSE gap; the OpenAPI has no dates.
                        for sid in ("tsmc_close","foreign_net","margin_balance","twse_short_balance") if twse_website_enabled() else ():
                            existing={p["observation_date"] for p in self.store.observations(sid)}
                            missing=calendar-existing
                            if missing:start=min(start,date.fromisoformat(min(missing)))
            if mode=="incremental":
                # TX follows the equity calendar. Check every side/category, not
                # just net OI; a new or incomplete source can otherwise look current.
                tx_calendar={p["observation_date"] for p in self.store.observations("taiex",start=(end-timedelta(days=366)).isoformat(),end=end.isoformat())}
                for sid in TX_REPAIR_SERIES:
                    existing={p["observation_date"] for p in self.store.observations(sid)}
                    missing=tx_calendar-existing
                    if missing:
                        start=min(start,date.fromisoformat(min(missing)))
                        source="taifex_tx_market" if sid=="tx_market_oi_total" else "taifex_tx_institutions"
                        repairs[source]=sorted(set(repairs.get(source,[]))|missing)
                tpex_calendar={p["observation_date"] for p in self.store.observations("tpex_index",start=(end-timedelta(days=366)).isoformat(),end=end.isoformat())}
                for sid in ("tpex_foreign_net","tpex_margin_balance","tpex_short_balance"):
                    missing=tpex_calendar-{p["observation_date"] for p in self.store.observations(sid)}
                    if missing:start=min(start,date.fromisoformat(min(missing)))
                if tpex_calendar:start=min(start,date.fromisoformat(max(tpex_calendar))-timedelta(days=1))
            if start>end or (end-start).days>1100:raise ValueError("日期範圍必須為0至1100天")
            if end>datetime.now(TAIPEI).date():raise ValueError("不得要求未來資料")
            state={"id":str(uuid.uuid4()),"mode":mode,"start":start.isoformat(),"end":end.isoformat(),
                   "status":"running","started_at":utc_now(),"finished_at":None,"total":0,"done":0,
                   "downloaded":0,"cached":0,"failed":0,"rows":0,"current":"","errors":[],"message":"準備資料工作","repair_dates":repairs,"scheduled_for":scheduled_for,"trigger":trigger}
            self.store.save_sync(state)
            self._runtime_started(state)
            self.thread=threading.Thread(target=self._run,args=(state,start,end),daemon=True,name="eagleeye-sync")
            self.thread.start()
            return state
        except Exception:
            self.lock.release()
            raise
    def _start_recovery(self,source_ids,job_keys,max_jobs,scheduled_for,trigger):
        view=recovery_view(self.store)
        selected=[r for r in view['jobs'] if r['eligible'] and (not source_ids or r['source_id'] in source_ids) and (not job_keys or r['job_key'] in job_keys)][:max_jobs]
        if not selected:raise ValueError('沒有已到重試時間且可自動補抓的工作；請查看冷卻時間及不可重試原因。')
        start=min(date.fromisoformat(r['start']) for r in selected);end=max(date.fromisoformat(r['end']) for r in selected)
        if (end-start).days>1100:raise ValueError('失敗工作日期跨度過大，請分來源或工作選擇補抓。')
        state={'id':str(uuid.uuid4()),'mode':'retry_failed','start':start.isoformat(),'end':end.isoformat(),
            'status':'running','started_at':utc_now(),'finished_at':None,'total':0,'done':0,'downloaded':0,
            'cached':0,'failed':0,'rows':0,'current':'','errors':[],'message':'重建可補抓工作',
            'repair_dates':{},'retry_keys':[r['job_key'] for r in selected],'scheduled_for':scheduled_for,'trigger':trigger}
        self.store.save_sync(state)
        self._runtime_started(state)
        self.thread=threading.Thread(target=self._run,args=(state,start,end),daemon=True,name='eagleeye-recovery')
        self.thread.start()
        return state

    def _daily_jobs(self,adapters,start,end):
        return _planned_daily_jobs(self.store,adapters,start,end)

    def _calendar_dates(self,adapters,start):
        series={'taiex','tpex_index'}|{module.DAILY_CALENDAR for module in adapters if hasattr(module,'DAILY_CALENDAR')}
        lower=(start-timedelta(days=7)).isoformat()
        return {p['observation_date'] for sid in series for p in self.store.observations(sid,start=lower)}

    def _reconstruct(self,adapters,state,start,end):
        ranges={(start,end)}
        wanted=set(state['retry_keys'])
        records={row['job_key']:row for row in _recovery_rows(self.store)}
        for key in wanted:
            row=records.get(key)
            if row:ranges.add((date.fromisoformat(row['start']),date.fromisoformat(row['end'])))
        lookup=_planned_lookup(self.store,adapters,ranges)
        missing=wanted-set(lookup)
        for key in sorted(missing):
            state['failed']+=1
            state['errors'].append({'key':key,'error':'無法由目前 adapter 與快取父工作重建；請執行一般同步更新索引。'})
            row=records.get(key)
            if row:
                row={**row,'status':'requires_review','retryable':False,'reconstructable':False,
                    'category':'needs_adapter_update','requires_review':True,'updated_at':utc_now(),
                    'review_reason':'無法重建工作；需一般同步更新索引或 adapter。'}
                with self.store.lock,self.store.connect() as db:
                    db.execute('INSERT OR REPLACE INTO recovery_jobs VALUES (?,?,?,?)',[key,row['source_id'],row['updated_at'],json.dumps(row,ensure_ascii=False)])
        return [replace(lookup[key],refresh_after_hours=0) for key in state['retry_keys'] if key in lookup]

    def _remember_failure(self,job,state,exc,attempted,digest):
        with self.store.lock,self.store.connect() as db:
            saved=db.execute('SELECT payload_json FROM recovery_jobs WHERE job_key=?',[job.key]).fetchone()
        previous=json.loads(saved[0]) if saved else None
        attempts=(previous.get('attempts',0) if previous else 0)+int(attempted)
        if isinstance(exc,DeferredSource):
            policy=exc.policy;category=policy['category'];retryable=policy['retryable'];next_retry=policy.get('next_retry_at')
        else:
            category,retryable,seconds=_failure_policy(exc,max(1,attempts))
            next_retry=(datetime.now(timezone.utc)+timedelta(seconds=seconds)).isoformat()
        end=state.get('end') or reference_end().isoformat()
        session=_session_date(getattr(exc,'session_date',None))
        row={'job_key':job.key,'source_id':job.source_id,'status':'failed' if attempted else 'deferred',
            'attempts':attempts,'retryable':retryable,'category':category,'next_retry_at':next_retry,
            'error':type(exc).__name__+': '+str(exc)[:350], 'updated_at':utc_now(), 'parser_version':job.parser_version,
            'start':state.get('start') or (date.fromisoformat(end)-timedelta(days=366)).isoformat(),'end':end,
            'request':{'url':job.url,'method':job.method,'body_hex':job.body.hex() if job.body else None,'headers':job.headers},
            'raw_sha256':digest,'reconstructable':True,'session_date':session.isoformat() if session else None}
        with self.store.lock,self.store.connect() as db:
            db.execute('INSERT OR REPLACE INTO recovery_jobs VALUES (?,?,?,?)',[job.key,job.source_id,row['updated_at'],json.dumps(row,ensure_ascii=False)])
            if category=='awaiting_publication':
                # One live question per request: a newer "not yet" replaces older keys for the same URL.
                for key,payload in db.execute('SELECT job_key,payload_json FROM recovery_jobs WHERE source_id=? AND job_key<>?',[job.source_id,job.key]).fetchall():
                    old=json.loads(payload)
                    if old.get('category')=='awaiting_publication' and old.get('status') not in ('recovered','superseded') and _same_request(old,job):
                        old.update(status='superseded',superseded_by=job.key,updated_at=row['updated_at'],next_retry_at=None)
                        db.execute('UPDATE recovery_jobs SET updated_at=?,payload_json=? WHERE job_key=?',[row['updated_at'],json.dumps(old,ensure_ascii=False),key])
            # A missing report for one session says nothing about the source's other dates.
            if attempted and category not in UNPUBLISHED:
                old=db.execute('SELECT payload_json FROM recovery_sources WHERE source_id=?',[job.source_id]).fetchone()
                policy={k:row[k] for k in ('source_id','category','retryable','next_retry_at','error','parser_version')}
                policy['attempts']=(json.loads(old[0]).get('attempts',0) if old else 0)+1
                db.execute('INSERT OR REPLACE INTO recovery_sources VALUES (?,?,?)',[job.source_id,row['updated_at'],json.dumps(policy,ensure_ascii=False)])
        return row

    def _source_block(self,job):
        with self.store.lock,self.store.connect() as db:
            result=db.execute('SELECT payload_json FROM recovery_sources WHERE source_id=?',[job.source_id]).fetchone()
        if not result:return None
        policy=json.loads(result[0])
        if policy['category']=='schema' and policy.get('parser_version')!=job.parser_version:return None
        if policy.get('next_retry_at') and datetime.fromisoformat(policy['next_retry_at'])>datetime.now(timezone.utc):return policy
        return None

    def _recovered(self,job,http=True):
        """Only a successful HTTP response is evidence that the source works again.
        Re-parsing an older capture satisfies this job, but never lifts a source cooldown."""
        with self.store.lock,self.store.connect() as db:
            row=db.execute('SELECT payload_json FROM recovery_jobs WHERE job_key=?',[job.key]).fetchone()
            if row:
                obj=json.loads(row[0]);obj.update(status='recovered',updated_at=utc_now(),next_retry_at=None)
                db.execute('UPDATE recovery_jobs SET updated_at=?,payload_json=? WHERE job_key=?',[obj['updated_at'],json.dumps(obj,ensure_ascii=False),job.key])
            if not http:return
            db.execute('DELETE FROM recovery_sources WHERE source_id=?',[job.source_id])
            # An older "not yet published" answer for the same request is superseded by this capture.
            for key,payload in db.execute('SELECT job_key,payload_json FROM recovery_jobs WHERE source_id=? AND job_key<>?',[job.source_id,job.key]).fetchall():
                old=json.loads(payload)
                if old.get('category')=='awaiting_publication' and old.get('status') not in ('recovered','superseded') and _same_request(old,job):
                    stamp=utc_now();old.update(status='superseded',superseded_by=job.key,updated_at=stamp,next_retry_at=None)
                    db.execute('UPDATE recovery_jobs SET updated_at=?,payload_json=? WHERE job_key=?',[stamp,json.dumps(old,ensure_ascii=False),key])

    def _fetch(self,job):
        if not _schedule_settings(self.store)['network_enabled'] or self.stop_event.is_set() or self.store.setting('tracking_paused',False):
            raise NetworkDisabled('已停止資料連線；只保留本機快取。')
        circuit=self._network_block()
        if circuit:raise DeferredSource(circuit)
        validator=_source_url_validator(job.source_id)
        if validator:validator(job.url)
        host=urlparse(job.url).hostname
        if host not in ALLOWED_HOSTS:raise ValueError("Unapproved source host: "+str(host))
        if job.method not in ("GET","POST"):raise ValueError("Unsupported method")
        delay=HOST_MIN_INTERVAL.get(host,.7 if "twse" in host or "tpex" in host or "taifex" in host else .3)
        wait=delay-(time.monotonic()-self.last_host.get(host,0))
        if wait>0:self.stop_event.wait(wait)
        if not _schedule_settings(self.store)['network_enabled'] or self.stop_event.is_set() or self.store.setting('tracking_paused',False):
            raise NetworkDisabled('已停止資料連線；只保留本機快取。')
        request=urllib.request.Request(job.url,data=job.body,method=job.method,
            headers={"User-Agent":"EagleEye/0.10 personal macroeconomic research","Accept":"application/json,text/csv,text/html,*/*",**job.headers})
        self.last_host[host]=time.monotonic()
        open_request=(urllib.request.build_opener(_SourceRedirectHandler(validator)).open if validator else urllib.request.urlopen)
        with open_request(request,timeout=20) as response:
            if urlparse(response.url).hostname not in ALLOWED_HOSTS:raise ValueError("Unexpected redirect host")
            if validator:validator(response.url)
            raw=response.read(job.max_bytes+1)
            if len(raw)>job.max_bytes:raise ValueError("Source payload too large")
            return raw
    def _run(self,state,start,end):
        blocked={}
        try:
            adapters=modules()
            if state['mode']=='retry_failed':
                before=self._calendar_dates(adapters,start)
                self._batch(self._reconstruct(adapters,state,start,end),state,blocked)
                # A recovered calendar parent reveals sessions whose per-date reports
                # were never planned; plan them now instead of marking the slot done.
                added=sorted(self._calendar_dates(adapters,start)-before)
                if added and not state.get('network_paused') and not self.stop_event.is_set():
                    state['followup_dates']=added
                    state['followup_daily']=True
                    self._batch(self._daily_jobs(adapters,date.fromisoformat(added[0]),date.fromisoformat(added[-1])),state,blocked)
            else:
                bootstrap=[]
                for module in adapters:
                    bootstrap.extend(_adapter_jobs(module,self.store,start,end,mode=state['mode']))
                self._batch(repair_jobs(bootstrap,state.get('repair_dates',{})),state,blocked)
                self._batch(self._daily_jobs(adapters,start,end),state,blocked)
            state["current"]="重新計算衍生指標"
            self.store.save_sync(state)
            for module in adapters:
                derive=getattr(module,"derive",None)
                if not derive:continue
                try:
                    for source,points in derive(self.store):
                        if not points:continue
                        raw=json.dumps({"model":module.__name__,"version":getattr(module,"MODEL_VERSION","1"),"points":points},ensure_ascii=False,sort_keys=True).encode()
                        digest=self.store.cache_raw(raw)
                        self.store.save(source,points,utc_now(),digest)
                        self.store.record_run(source.id,utc_now(),"ok",len(points),digest=digest)
                except Exception as exc:
                    state["failed"]+=1
                    state["errors"].append({"key":module.__name__+":derive","error":str(exc)[:350]})
            message="已保留來源日期、擷取版本與缺口；最新覆蓋以指標表為準。"
            if state.get('awaiting_publication'):
                message+=f"另有 {state['awaiting_publication']} 份官方報表尚未發布，已排定稍後自動重查。"
            state.update(status="partial" if state["failed"] else "completed",finished_at=utc_now(),
                         current="",message=message)
            if self.stop_event.is_set() or state.get('network_paused'):
                state['status']='interrupted'
                if state.get('network_paused'):state['message']='追蹤已暫停；已取得資料保留，未繼續連線。' if self.store.setting('tracking_paused',False) else '已切換離線模式；已取得資料保留，未繼續連線。' 
        except Exception as exc:
            state.update(status="failed",finished_at=utc_now(),message=type(exc).__name__+": "+str(exc)[:400])
        finally:
            self.store.save_sync(state)
            if state.get('scheduled_for') and state['status']=='completed' and (state.get('mode')!='retry_failed' or not recovery_view(self.store)['pending']):
                self.store.set_setting('scheduled_date',state['scheduled_for'])
            self._runtime_result(state)
            if state.get('trigger')=='scheduled_daily' and state['status']=='interrupted' and (state.get('network_paused') or self.stop_event.is_set()):
                self._refund_daily_attempt(state)
            if self.after_sync:
                try:self.after_sync(dict(state))
                except Exception as exc:
                    state['post_sync_warning']='同步後摘要／事件簿更新失敗：'+str(exc)[:250]
                    self.store.save_sync(state)
            self.lock.release()
    def _batch(self,jobs,state,blocked):
        # No duplicate URLs/keys, and a failed source cannot launch hundreds of retries.
        jobs=list({j.key:j for j in jobs}.values())
        state["total"]+=len(jobs)
        self.store.save_sync(state)
        sources=source_map()
        seen_keys={j.key for j in jobs}
        for job in jobs:
            if self.stop_event.is_set():return
            if not _schedule_settings(self.store)['network_enabled'] or self.store.setting('tracking_paused',False):
                state['network_paused']=True
                return
            state['current']=job.key
            cached=self.store.checkpoint(job.key)
            # Prefer this job's checkpoint to preserve failed-job/recovery
            # semantics. A new parsing key may reuse a successful HTTP capture.
            if cached is None:cached=self.store.request_checkpoint(job)
            digest=None
            attempted=False
            try:
                policy=self._network_block() or blocked.get(job.source_id) or self._source_block(job)
                raw_path=self.store.raw_dir/(cached['raw_sha256']+'.json') if cached and cached.get('raw_sha256') else None
                raw_fresh,fresh,reason=cache_decision(job,cached,bool(raw_path and raw_path.exists()))
                reasons=state.setdefault('cache_reasons',{})
                reasons[reason]=reasons.get(reason,0)+1
                if policy and not raw_fresh:raise DeferredSource(policy)
                if fresh:
                    state["cached"]+=1
                    raw=(self.store.raw_dir/(cached["raw_sha256"]+".json")).read_bytes() if job.followups else None
                else:
                    raw=None
                    attempted=True
                    reparse=raw_fresh
                    if reparse:
                        raw=raw_path.read_bytes()
                    else:
                        for attempt in range(2):
                            if not _schedule_settings(self.store)['network_enabled'] or self.store.setting('tracking_paused',False):
                                state['network_paused']=True
                                return
                            try:
                                raw=self._fetch(job)
                                state['network_successes']=state.get('network_successes',0)+1
                                self._network_success()
                                break
                            except (urllib.error.URLError,TimeoutError,OSError) as exc:
                                code=getattr(exc,'code',None)
                                if code==404 and job.empty_ok:
                                    # An answered "no file for this day"; the parser decides what it means.
                                    raw=NOT_FOUND_RAW
                                    self._network_success()
                                    break
                                if attempt or code in (400,401,403,404,429):raise
                                self.stop_event.wait(1)
                    digest=self.store.cache_raw(raw)
                    points=job.parser(raw)
                    if not points and not job.followups and not job.empty_ok:raise ValueError("來源沒有可驗證的觀測，不補零")
                    source=replace(sources[job.source_id],url=job.url)
                    self.store.save(source,points,utc_now(),digest)
                    # A re-parse is not a new contact with the source; it must not
                    # hide a failure recorded in this or an earlier run.
                    if not reparse:self.store.record_run(source.id,utc_now(),"ok",len(points),digest=digest)
                    self.store.checkpoint_save(job,"ok",digest,len(points),fetched_at=cached["fetched_at"] if reparse else None)
                    self._recovered(job,http=not reparse)
                    if reparse:state["reparsed"]=state.get("reparsed",0)+1
                    else:state["downloaded"]+=1
                    state["rows"]+=len(points)
                if job.followups:
                    extra=[j for j in job.followups(raw) if j.key not in seen_keys and (state.get('mode')!='retry_failed' or state.get('followup_daily') or j.key in state.get('retry_keys',[]))]
                    seen_keys.update(j.key for j in extra)
                    jobs.extend(extra)
                    state["total"]+=len(extra)
            except NetworkDisabled:
                state['network_paused']=True
                return
            except Exception as exc:
                if attempted and isinstance(exc,(urllib.error.URLError,TimeoutError,OSError)) and getattr(exc,'code',None) is None:
                    self._transport_failure(job,state,exc)
                failure=self._remember_failure(job,state,exc,attempted,digest)
                error=failure['error']
                if failure['category']=='awaiting_publication':
                    # Not a source failure: keep the source status and other dates untouched.
                    self.store.checkpoint_save(job,"awaiting",digest=digest,error=error)
                    state['awaiting_publication']=state.get('awaiting_publication',0)+1
                    waiting=state.setdefault('awaiting',[])
                    if len(waiting)<30:waiting.append({'key':job.key,'session_date':failure.get('session_date'),'next_retry_at':failure['next_retry_at']})
                else:
                    if failure['category'] not in UNPUBLISHED:blocked[job.source_id]=failure
                    self.store.checkpoint_save(job,"error",digest=digest,error=error)
                    self.store.record_run(job.source_id,utc_now(),"error",error=error,digest=digest)
                    state["failed"]+=1
                    if len(state["errors"])<30:state["errors"].append({"key":job.key,"error":error})
            state["done"]+=1
            self.store.save_sync(state)
    def set_schedule(self,values):
        """Merge runtime-only preferences; no OS tasks, services or startup entries."""
        if not isinstance(values,dict) or set(values)-set(SCHEDULE_DEFAULTS):
            raise ValueError('未知排程設定')
        for key in ('enabled','auto_recovery','network_enabled'):
            if key in values and not isinstance(values[key],bool):raise ValueError(key+' 必須是布林值')
        if 'local_time' in values and (not isinstance(values['local_time'],str) or
                not re.fullmatch(r'(?:[01][0-9]|2[0-3]):[0-5][0-9]',values['local_time'])):
            raise ValueError('每日時間必須為 HH:MM')
        settings={**_schedule_settings(self.store),**values}
        self.store.set_setting('schedule',settings)
        return self.schedule_view()

    def schedule_view(self,now=None):
        now=_taipei(now);settings=_schedule_settings(self.store)
        due_date,next_at=_schedule_due(now,settings)
        return {**settings,'timezone':'Asia/Taipei','due_date':due_date,'next_daily_at':next_at.isoformat(),
            'last_scheduled_date':self.store.setting('scheduled_date'),
            'daily':self.store.setting('runtime_daily',{}),
            'recovery':self.store.setting('runtime_recovery',{}),
            'network':self.store.setting('runtime_network',{'status':'normal','circuit_count':0}),
            'last_tick':self.store.setting('runtime_last_tick'),
            'running':self.lock.locked(),'no_boot_startup':True,
            'tracking_paused':bool(self.store.setting('tracking_paused',False)),
            'pausing':bool(self.store.setting('tracking_paused',False)) and self.lock.locked(),
            'scope':'僅手動啟動的本機 API 存活期間執行；離線仍可閱讀快取與產生本機摘要。',
            'limits':{'source_attempts':MAX_RECOVERY_ATTEMPTS,'daily_restart_attempts':MAX_DAILY_RESTARTS,
                'recovery_batches_per_day':MAX_RUNTIME_RECOVERIES,'recovery_interval_minutes':RECOVERY_INTERVAL_MINUTES}}

    def _network_block(self,now=None):
        now=now or datetime.now(timezone.utc)
        network=self.store.setting('runtime_network',{})
        if _deadline_after(network.get('cooldown_until'),now):
            return {'category':'transport_circuit','retryable':True,'next_retry_at':network['cooldown_until'],
                'error':'多個來源連線失敗，暫停下載至冷卻結束；這不能單獨證明網際網路中斷。'}
        return None

    def _network_success(self):
        network=self.store.setting('runtime_network',{})
        if network.get('status')!='normal' or network.get('circuit_count',0):
            self.store.set_setting('runtime_network',{'status':'normal','circuit_count':0,
                'last_success_at':utc_now(),'cooldown_until':None})

    def _transport_failure(self,job,state,exc):
        hosts=set(state.get('transport_failure_hosts',[]));hosts.add(urlparse(job.url).hostname)
        state['transport_failure_hosts']=sorted(hosts)
        # A successful HTTP response in this run is positive connectivity evidence.
        # Do not label a few unrelated source failures as a global outage.
        if len(hosts)<3 or state.get('network_successes',0) or self._network_block():return
        prior=self.store.setting('runtime_network',{})
        count=prior.get('circuit_count',0)+1
        now=datetime.now(timezone.utc)
        cooldown=(now+timedelta(minutes=min(30*2**min(count-1,4),360))).isoformat()
        self.store.set_setting('runtime_network',{'status':'cooldown','category':'multiple_transport_failures',
            'cooldown_until':cooldown,'circuit_count':count,'last_failure_at':now.isoformat(),
            'failed_hosts':sorted(hosts),'last_error':str(exc)[:250],
            'note':'三個不同官方主機連線失敗且本批無成功下載；保留快取並暫緩連線，未判定根因。'})
        state['network_cooldown_until']=cooldown

    def _runtime_started(self,state):
        trigger=state.get('trigger','manual')
        if trigger=='scheduled_daily':
            prior=self.store.setting('runtime_daily',{})
            self.store.set_setting('runtime_daily',{**prior,'date':state.get('scheduled_for'),
                'status':'running','run_id':state['id'],'started_at':state['started_at']})
        elif trigger=='scheduled_recovery':
            prior=self.store.setting('runtime_recovery',{})
            self.store.set_setting('runtime_recovery',{**prior,'last_result':{
                'status':'running','run_id':state['id'],'started_at':state['started_at']}})

    def _refund_daily_attempt(self,state):
        """Pause, offline mode or a normal exit is the user's decision, not a failed
        attempt; it must not use up the bounded daily restart budget."""
        attempt=self.store.setting('scheduled_attempt',{})
        if attempt.get('date')==state.get('scheduled_for') and attempt.get('count',0)>0:
            self.store.set_setting('scheduled_attempt',{**attempt,'count':attempt['count']-1,'next_at':None,
                'refunded':attempt.get('refunded',0)+1})

    def _runtime_result(self,state):
        trigger=state.get('trigger','manual')
        if trigger=='scheduled_daily':
            prior=self.store.setting('runtime_daily',{})
            self.store.set_setting('runtime_daily',{**prior,'date':state.get('scheduled_for'),
                'status':state['status'],'run_id':state['id'],'started_at':state['started_at'],
                'finished_at':state.get('finished_at'),'failed':state.get('failed',0),
                'downloaded':state.get('downloaded',0),'message':state.get('message','')})
        elif trigger=='scheduled_recovery':
            prior=self.store.setting('runtime_recovery',{})
            self.store.set_setting('runtime_recovery',{**prior,'last_result':{
                'status':state['status'],'run_id':state['id'],'finished_at':state.get('finished_at'),
                'failed':state.get('failed',0),'downloaded':state.get('downloaded',0)}})

    def _schedule_decision(self,now):
        settings=_schedule_settings(self.store)
        if not settings['enabled'] or not settings['network_enabled'] or self.lock.locked() or self.stop_event.is_set() or self.store.setting('tracking_paused',False):return None
        if self._network_block(now):return None
        due_date,_=_schedule_due(now,settings)
        daily=self.store.setting('runtime_daily',{})
        complete=self.store.setting('scheduled_date')
        attempt=self.store.setting('scheduled_attempt',{})
        daily_due=(not complete or complete<due_date) and (daily.get('date')!=due_date or daily.get('status') in ('failed','interrupted','launching'))
        if daily_due:
            count=attempt.get('count',0) if attempt.get('date')==due_date else 0
            if count>=MAX_DAILY_RESTARTS or (attempt.get('date')==due_date and _deadline_after(attempt.get('next_at'),now)):
                return None
            claim={'date':due_date,'count':count+1,'next_at':(now+timedelta(minutes=30)).isoformat(),
                'refunded':attempt.get('refunded',0) if attempt.get('date')==due_date else 0}
            self.store.set_setting('scheduled_attempt',claim)
            self.store.set_setting('runtime_daily',{'date':due_date,'status':'launching','attempt':count+1,'started_at':now.isoformat()})
            try:
                return self.start('incremental',end=date.fromisoformat(due_date),scheduled_for=due_date,trigger='scheduled_daily')
            except RuntimeError:
                self.store.set_setting('scheduled_attempt',attempt)
                self.store.set_setting('runtime_daily',daily)
                return None
            except Exception as exc:
                self.store.set_setting('runtime_daily',{'date':due_date,'status':'failed','attempt':count+1,
                    'started_at':now.isoformat(),'finished_at':now.isoformat(),'message':str(exc)[:250]})
                return None
        if not settings['auto_recovery']:return None
        recovery=self.store.setting('runtime_recovery',{})
        today=now.date().isoformat()
        count=recovery.get('count',0) if recovery.get('date')==today else 0
        if count>=MAX_RUNTIME_RECOVERIES or _deadline_after(recovery.get('next_at'),now):return None
        view=recovery_view(self.store)
        if not view['eligible']:return None
        claim={**recovery,'date':today,'count':count+1,'next_at':(now+timedelta(minutes=RECOVERY_INTERVAL_MINUTES)).isoformat(),
            'started_at':now.isoformat()}
        self.store.set_setting('runtime_recovery',claim)
        try:
            return self.start('retry_failed',max_jobs=100,scheduled_for=due_date,trigger='scheduled_recovery')
        except (RuntimeError,ValueError):
            self.store.set_setting('runtime_recovery',recovery)
            return None

    def schedule_tick(self,now=None):
        now=_taipei(now)
        if not self.schedule_lock.acquire(blocking=False):return None
        try:
            self.store.set_setting('runtime_last_tick',now.isoformat())
            return self._schedule_decision(now)
        finally:
            self.schedule_lock.release()
            # Local-only summary work must also run in explicitly offline mode.
            if self.on_tick:
                try:self.on_tick(now)
                except Exception as exc:self.store.set_setting('runtime_tick_warning',{'at':now.isoformat(),'error':str(exc)[:250]})

    def schedule(self):
        # Immediate bounded catch-up, then one cheap tick per minute. The user
        # starts this process; nothing is installed at Windows login or boot.
        while not self.stop_event.is_set():
            try:self.schedule_tick()
            except Exception as exc:
                try:self.store.set_setting('runtime_tick_warning',{'at':utc_now(),'error':str(exc)[:250]})
                except Exception:pass
            if self.stop_event.wait(60):break

    def close(self):
        self.stop_event.set()
