"""Local API: a single DuckDB writer with resumable official-source ingestion."""
import os,json,threading,time,atexit,asyncio
from pathlib import Path
from datetime import date,datetime,timezone,timedelta
from contextlib import asynccontextmanager
from fastapi import FastAPI,HTTPException,Query,Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from pydantic import BaseModel,Field,FiniteFloat
from typing import Literal
from .connectors import SERIES,fetch,parse,refresh_sources
from .registry import series_map,prefer_publishable
from .store import Store,utc_now
from .engine import Engine
from .lease import DataDirectoryLease

DATA_DIR=Path(os.getenv("EAGLEEYE_DATA_DIR",str(Path(__file__).parent/"data")))
application_lease=DataDirectoryLease(DATA_DIR).acquire()
atexit.register(application_lease.close)
store=Store(DATA_DIR)
engine=Engine(store)
refresh_lock=threading.Lock()
last_refresh_monotonic=0.0
@asynccontextmanager
async def lifespan(app):
    evaluate_ledger(initial_scan=True,reason="startup")
    try:generate_daily_brief(reason="startup")
    except Exception as exc:
        from .brief_store import notify
        notify(store,dedupe_key="brief-startup:"+datetime.now(timezone(timedelta(hours=8))).date().isoformat(),
            kind="operation",severity="watch",title="啟動時的摘要整理尚未完成",
            body="原有資料與摘要仍保留；可在每日觀測頁重新整理。",
            evidence={"error":type(exc).__name__+": "+str(exc)[:300]})
    thread=threading.Thread(target=engine.schedule,daemon=True,name="eagleeye-schedule")
    thread.start()
    try:yield
    finally:
        engine.close()
        # Drain only our workers. A current source HTTP request has a 20-second timeout.
        deadline=time.monotonic()+60
        for worker in (thread,engine.thread):
            if worker and worker is not threading.current_thread():
                await asyncio.to_thread(worker.join,max(0,deadline-time.monotonic()))
        # The OS lease is also held until process exit if a worker did not drain.
        if not any(worker and worker.is_alive() for worker in (thread,engine.thread)):
            application_lease.close()
app=FastAPI(title="EagleEye local data API",version="0.10.0",lifespan=lifespan)
ALLOWED_ORIGINS=["http://localhost:5173","http://127.0.0.1:5173","http://localhost:4173","http://127.0.0.1:4173"]
app.add_middleware(CORSMiddleware,allow_origins=ALLOWED_ORIGINS,allow_methods=["GET","POST"],allow_headers=["Content-Type"])
app.add_middleware(GZipMiddleware,minimum_size=2000)

def origin_check(request):
    origin=request.headers.get("origin")
    if origin and origin not in ALLOWED_ORIGINS:raise HTTPException(403,"Origin not allowed")

@app.get("/api/health")
def health():
    return {"status":"ok","version":"0.10.0","database":"duckdb","storage":"persistent","writer_processes":1,
            "mode":"official","process_id":os.getpid(),"project_root":str(Path(__file__).parent.parent.resolve()),
            "data_directory":str(store.directory.resolve())}
@app.get("/api/sources")
def sources(as_of:str|None=None):
    as_of=checked_as_of(as_of)
    return {"sources":store.sources(as_of=as_of),"planned":[
        {"id":"fred","status":"partial","note":"Selected series via public fredgraph.csv (no API key); ALFRED vintages not implemented","url":"https://fred.stlouisfed.org/docs/api/fred/"},
        {"id":"bis","status":"not_implemented","note":"Quarterly credit and debt-service ratios","url":"https://data.bis.org/"},
        {"id":"ofr","status":"not_implemented","note":"Financial stress; not a forecast probability","url":"https://www.financialresearch.gov/financial-stress-index/"}]}
@app.get("/api/observations")
def observations(series_id:str=Query(...,min_length=1,max_length=100),start:date|None=None,end:date|None=None,as_of:str|None=None):
    as_of=checked_as_of(as_of)
    if start and end and start>end:raise HTTPException(422,"start exceeds end")
    rows=store.observations(series_id,as_of=as_of,start=start.isoformat() if start else None,end=end.isoformat() if end else None)
    if series_id not in series_map() and not rows:raise HTTPException(404,"Unknown series")
    return {"series_id":series_id,"observations":rows}

def checked_as_of(value):
    if value is None:return None
    try:
        stamp=datetime.fromisoformat(value.replace("Z","+00:00"))
        if stamp.tzinfo is None:raise ValueError("Timezone is required")
        stamp=stamp.astimezone(timezone.utc)
        if stamp>datetime.now(timezone.utc):raise ValueError("Cannot replay a future cutoff")
        return stamp.isoformat()
    except (ValueError,TypeError) as exc:raise HTTPException(422,str(exc))

def replay_metadata(as_of):
    return {"active":bool(as_of),"cutoff":as_of,"semantics":"locally_known",
            "model_semantics":"current_model_on_locally_known_observations",
            "note":"回放本機截至此時已取得且已發布的版本；以目前模型與門檻重算，並非當時已發出的歷史訊號。" if as_of else "最新已取得版本"}

def replay_today(as_of):
    return datetime.fromisoformat(as_of).astimezone(timezone(timedelta(hours=8))).date() if as_of else None

OUTLOOK_PREFIXES=("imf_weo_","oecd_eo_")
# Set only by web_export (a separate process): the publish=False source ids. Every route then computes
# from public observations alone, so no model output in a snapshot is derived from local-only data.
PUBLICATION_HIDDEN=frozenset()
def main_observations(as_of=None):
    # Institution outlooks hold several vintages per series/year; they are served by /api/outlook only.
    rows=[p for p in store.observations(as_of=as_of,exclude_partners=True) if not p["series_id"].startswith(OUTLOOK_PREFIXES)]
    if PUBLICATION_HIDDEN:
        from .publication_policy import date_unconfirmed
        rows=[p for p in rows if p["source_id"] not in PUBLICATION_HIDDEN and not date_unconfirmed(p)]
    from .ex_tsmc_open import filter_current_segments
    return prefer_publishable(filter_current_segments(rows))

LIMITATIONS=[
    "日資料與月資料各依官方發布頻率；不是即時報價。",
    "舊排除台積電採週市值估算；新模型採自建普通股母體與申報股數，固定有價樣本另列。皆非官方指數或當時可得回測訊號。",
    "外資買賣超、持股比率與單月淨匯入是不同口徑；淨匯入不等於流入股票的資金。",
    "逐期發布日期未知時保留空值；擷取日期不能取代發布日期。"]
@app.get("/api/ex-tsmc-open")
def ex_tsmc_open(as_of:str|None=None):
    as_of=checked_as_of(as_of)
    if as_of:
        return {"status":"replay_unavailable", "message":"此模型診斷只保存目前狀態；回放時不帶入現在的診斷或資料。"}
    from .ex_tsmc_open import public_view
    return public_view(store)

@app.get("/api/snapshot")
def snapshot(as_of:str|None=None):
    as_of=checked_as_of(as_of)
    latest={}
    for p in main_observations(as_of):latest[p["series_id"]]=p
    return {"as_of":as_of or utc_now(),"replay":replay_metadata(as_of),"mode":"official","series":list(latest.values()),"sources":store.sources(as_of=as_of),
            "catalog":store.catalog(as_of=as_of,exclude_sources=PUBLICATION_HIDDEN),"sync":store.get_sync(),"schedule":schedule(),"operations_semantics":"current_live_state","limitations":LIMITATIONS}
@app.get("/api/dashboard")
def dashboard(as_of:str|None=None):
    as_of=checked_as_of(as_of)
    histories={}
    for p in main_observations(as_of):histories.setdefault(p["series_id"],[]).append(p)
    all_calendar=[p["observation_date"] for p in histories.get("taiex",[])]
    cutoff="1900-01-01"
    if all_calendar:
        last=date.fromisoformat(all_calendar[-1])
        try:cutoff=last.replace(year=last.year-1).isoformat()
        except ValueError:cutoff=last.replace(year=last.year-1,day=28).isoformat()
    calendar={d for d in all_calendar if d>=cutoff}
    coverage=[]
    required=("taiex","tsmc_close","foreign_net","margin_balance","usd_twd","ex_tsmc_estimated")
    for sid in required:
        points=histories.get(sid,[])
        dates={p["observation_date"] for p in points if p["observation_date"]>=cutoff}
        if PUBLICATION_HIDDEN and not points:
            # Not a data gap: in a public snapshot this series has no publishable source.
            coverage.append({"series_id":sid,"label":series_map().get(sid,(sid,""))[0],"first_date":None,"last_date":None,
                "count":0,"expected":None,"missing_count":None,"missing_dates":[],"published":False,
                "calendar_note":"公開快照不收這個序列：它只來自本機專用來源（例如證交所網站端點）。"})
            continue
        missing=sorted(calendar-dates)
        coverage.append({"series_id":sid,"label":series_map().get(sid,(sid,""))[0],
            "first_date":min(dates) if dates else None,
            "last_date":max(dates) if dates else None,"count":len(dates),
            "expected":None if sid=="usd_twd" else len(calendar),
            "missing_count":None if sid=="usd_twd" else len(missing),"missing_dates":[] if sid=="usd_twd" else missing,
            "calendar_note":"銀行營業日；獨立日曆尚未驗證" if sid=="usd_twd" else "以已取得TAIEX交易日為基準，含期初觀測"})
    return {"as_of":as_of or utc_now(),"replay":replay_metadata(as_of),"histories":histories,"coverage":coverage,"sources":store.sources(as_of=as_of),
            "sync":store.get_sync(),"operations_semantics":"current_live_state","limitations":LIMITATIONS}
@app.get("/api/market-holidays")
def market_holidays(as_of:str|None=None):
    from .market_holidays import build
    return build(store, as_of=checked_as_of(as_of))

@app.get("/api/partners")
def partners(as_of:str|None=None):
    as_of=checked_as_of(as_of)
    rows=[(p["series_id"],p["observation_date"],p["value"],p["metadata"]) for p in store.observations(as_of=as_of) if "_partner_" in p["series_id"]]
    result={}
    for direction in ("exports","imports"):
        points=[(d,v,m) for sid,d,v,m in rows if sid.startswith(direction+"_partner_")]
        if not points:
            result[direction]={"date":None,"total":None,"top5":[],"others":None}
            continue
        latest=max(p[0] for p in points)
        ranked=sorted([{"partner":m["partner"],"value":v} for d,v,m in points if d==latest],key=lambda p:p["value"],reverse=True)[:5]
        totals=store.observations(direction+"_usd",as_of=as_of,start=latest,end=latest)
        total=totals[-1]["value"] if totals else None
        result[direction]={"date":latest,"total":total,"top5":ranked,"others":total-sum(p["value"] for p in ranked) if total else None}
    return result

class SyncRequest(BaseModel):
    mode:Literal["backfill","incremental"]="incremental"
    start:date|None=None
    end:date|None=None
@app.post("/api/sync")
def sync(request:Request,body:SyncRequest):
    origin_check(request)
    require_network()
    try:return engine.start(body.mode,body.start,body.end)
    except RuntimeError as exc:raise HTTPException(409,str(exc))
    except ValueError as exc:raise HTTPException(422,str(exc))
@app.get("/api/sync")
def sync_status():return store.get_sync()
@app.get("/api/sync/{run_id}")
def sync_run(run_id:str):
    state=store.get_sync(run_id)
    if not state:raise HTTPException(404,"Unknown run")
    return state
@app.get("/api/schedule")
def schedule():
    return {**engine.schedule_view(),"shutting_down":engine.stop_event.is_set()}
class ScheduleRequest(BaseModel):
    enabled:bool|None=None
    auto_recovery:bool|None=None
    network_enabled:bool|None=None
    local_time:str|None=Field(default=None,pattern=r"^(?:[01]\d|2[0-3]):[0-5]\d$")
@app.post("/api/schedule")
def change_schedule(request:Request,body:ScheduleRequest):
    origin_check(request)
    values=body.model_dump(exclude_none=True,exclude_unset=True)
    if not values:raise HTTPException(422,"請指定至少一項更新設定")
    try:
        engine.set_schedule(values)
        return schedule()
    except ValueError as exc:raise HTTPException(422,str(exc))

def require_network():
    if store.setting("tracking_paused",False):raise HTTPException(409,"追蹤已暫停；既有資料仍可閱讀。")
    if engine.stop_event.is_set():raise HTTPException(409,"應用程式正在退出。")
    if not store.setting("schedule",{}).get("network_enabled",True):
        raise HTTPException(409,"目前為本機離線模式；既有資料、摘要與事件簿仍可使用。")
    blocked=engine._network_block()
    if blocked:raise HTTPException(429,blocked["error"])


def refresh_all():
    global last_refresh_monotonic
    require_network()
    if not engine.lock.acquire(blocking=False):raise HTTPException(409,"Refresh already in progress")
    if not refresh_lock.acquire(blocking=False):
        engine.lock.release()
        raise HTTPException(409,"Refresh already in progress")
    try:
        if last_refresh_monotonic and time.monotonic()-last_refresh_monotonic<30:raise HTTPException(429,"Wait 30 seconds between refreshes")
        for source in refresh_sources():
            require_network()
            if engine.stop_event.is_set():break
            attempted,digest=utc_now(),None
            try:
                raw=fetch(source);fetched_at=utc_now();digest=store.cache_raw(raw)
                points=parse(source.id,raw)
                store.save(source,points,fetched_at,digest)
                store.record_run(source.id,attempted,"ok",len(points),digest=digest)
            except Exception as exc:store.record_run(source.id,attempted,"error",error=f"{type(exc).__name__}: {str(exc)[:300]}",digest=digest)
        evaluate_ledger(reason="refresh")
        generate_daily_brief(reason="refresh")
        last_refresh_monotonic=time.monotonic()
        return {"refreshed_at":utc_now(),"sources":store.sources()}
    finally:
        refresh_lock.release()
        engine.lock.release()
@app.post("/api/refresh")
def refresh(request:Request):
    origin_check(request)
    return refresh_all()

def transmission_payload(pair="us10y_taiwan",period=252,window=60,as_of=None):
    from .transmission import build
    histories={}
    for p in main_observations(as_of):histories.setdefault(p["series_id"],[]).append(p)
    result=build(histories,store.sources(as_of=as_of),pair,period,window,store.setting("alert_settings"),{} if as_of else store.setting("alert_acks",{}),today=replay_today(as_of))
    result.update(as_of=as_of or result["as_of"],replay=replay_metadata(as_of))
    return result

@app.get("/api/transmission")
def transmission(pair:str="us10y_taiwan",period:int=252,window:int=60,as_of:str|None=None):
    as_of=checked_as_of(as_of)
    try:return transmission_payload(pair,period,window,as_of)
    except ValueError as exc:raise HTTPException(422,str(exc))

class AlertSettings(BaseModel):
    z_threshold:FiniteFloat=Field(default=3.5,ge=1,le=10)
    rate_move_bp:FiniteFloat=Field(default=10,ge=1,le=100)
    fx_move_pct:FiniteFloat=Field(default=1,ge=.1,le=10)
    oi_move_contracts:FiniteFloat=Field(default=5000,ge=100,le=100000)
    margin_move_pct:FiniteFloat=Field(default=2,ge=.1,le=50)
    short_move_pct:FiniteFloat=Field(default=10,ge=.1,le=100)
@app.get("/api/alerts/settings")
def alert_settings():
    from .transmission import DEFAULT_SETTINGS
    return store.setting("alert_settings",DEFAULT_SETTINGS)
@app.post("/api/alerts/settings")
def update_alert_settings(request:Request,body:AlertSettings):
    origin_check(request)
    from .transmission import DEFAULT_SETTINGS
    with store.lock:
        values={**DEFAULT_SETTINGS,**store.setting("alert_settings",{}),**body.model_dump(exclude_unset=True)}
        store.set_setting("alert_settings",values)
    evaluate_ledger(reason="settings_changed")
    generate_daily_brief(reason="settings_changed")
    return values
class AlertAcknowledgement(BaseModel):
    id:str=Field(pattern="^[a-f0-9]{24}$")
    acknowledged:bool
@app.post("/api/alerts/acknowledge")
def acknowledge_alert(request:Request,body:AlertAcknowledgement):
    origin_check(request)
    alerts=transmission_payload()["alerts"]
    if body.id not in {a["id"] for a in alerts}:raise HTTPException(404,"提醒已更新，請重讀目前觀測")
    with store.lock:
        records=store.setting("alert_acks",{})
        if body.acknowledged:records[body.id]=utc_now()
        else:records.pop(body.id,None)
        # Keep acknowledgement state bounded, local and separate from financial observations.
        records=dict(sorted(records.items(),key=lambda p:p[1])[-1000:])
        store.set_setting("alert_acks",records)
    if body.acknowledged:
        from .alert_ledger import list_events,acknowledge
        for episode in list_events(store,limit=500)["events"]:
            if episode.get("alert_instance_id")==body.id and episode.get("state") in ("active","data_unavailable"):
                acknowledge(store,episode["event_id"],note="由觀測頁標記已閱讀")
    return {"id":body.id,"acknowledged":body.acknowledged}


@app.get("/api/positions")
def positions(period:int=252,as_of:str|None=None):
    as_of=checked_as_of(as_of)
    from .positions import overview
    histories={}
    for point in main_observations(as_of):histories.setdefault(point["series_id"],[]).append(point)
    try:
        result=overview(histories,store.sources(as_of=as_of),period,today=replay_today(as_of))
        result.update(as_of=as_of or utc_now(),replay=replay_metadata(as_of))
        return result
    except ValueError as exc:raise HTTPException(422,str(exc))


@app.get("/api/history")
def history_summary():
    from .history import summary
    return summary(store)

@app.get("/api/history/versions")
def history_versions(series_id:str=Query(...,min_length=1,max_length=100),observation_date:date|None=None):
    from .history import versions
    if series_id not in series_map():raise HTTPException(404,"Unknown series")
    return versions(store,series_id,observation_date.isoformat() if observation_date else None)

@app.get("/api/breadth")
def breadth(period:int=252,as_of:str|None=None):
    from .breadth import overview
    as_of=checked_as_of(as_of)
    histories={}
    for p in main_observations(as_of):histories.setdefault(p["series_id"],[]).append(p)
    try:
        result=overview(histories,store.sources(as_of=as_of),period,today=replay_today(as_of))
        result.update(as_of=as_of or utc_now(),replay=replay_metadata(as_of))
        return result
    except ValueError as exc:raise HTTPException(422,str(exc))

@app.get("/api/quality")
def quality(as_of:str|None=None,offset:int=Query(0,ge=0),limit:int=Query(100,ge=1,le=500),blocking_only:bool=False,
            group_by:Literal["issue","report"]="issue"):
    from .governance import quality_view
    as_of=checked_as_of(as_of)
    return quality_view(main_observations(as_of),store.sources(as_of=as_of),offset,limit,blocking_only,group_by)

@app.get("/api/calendar")
def calendar(start:date|None=None,end:date|None=None,as_of:str|None=None):
    from .publication_calendar import calendar_view
    as_of=checked_as_of(as_of)
    start=start or replay_today(as_of) or datetime.now(timezone(timedelta(hours=8))).date()
    end=end or start+timedelta(days=45)
    if start>end or (end-start).days>370:raise HTTPException(422,"Calendar range must be 0–370 days")
    return calendar_view(store,start,end,as_of=as_of)

class CalendarRefresh(BaseModel):
    year:int=Field(default=date.today().year,ge=2000,le=2100)
@app.post("/api/calendar/refresh")
def refresh_publication_calendar(request:Request,body:CalendarRefresh):
    from .publication_calendar import refresh_calendar
    origin_check(request)
    require_network()
    if not engine.lock.acquire(blocking=False):raise HTTPException(409,"資料同步執行中，請完成後再更新發布日曆")
    try:return refresh_calendar(store,engine._fetch,body.year)
    finally:engine.lock.release()

@app.get("/api/recovery")
def recovery():
    from .engine import recovery_view
    return recovery_view(store)

@app.get("/api/freshness")
def freshness():
    """Why the latest value is not newer: publication wait, failure, or no rule. Current state only."""
    from .engine import recovery_view
    from .freshness import freshness_view
    from .publication_calendar import calendar_view
    today=datetime.now(timezone(timedelta(hours=8))).date()
    return freshness_view(store.sources(),recovery_view(store),
        calendar_view(store,today-timedelta(days=62),today+timedelta(days=70)),today)

@app.get("/api/methods")
def methods():
    """Versioned, read-only method registry; generated by scripts/build_method_registry.py."""
    return json.loads((Path(__file__).parent/"reference"/"method_registry.json").read_text(encoding="utf-8"))
@app.get("/api/ex-tsmc/benchmark")
def ex_tsmc_benchmark():
    """Official month-end TSMC weight vs the ex-TSMC proxy; sensitivity only, never rewrites the estimate."""
    from .extsmc_benchmark import benchmark,load_reference
    rows=prefer_publishable([p for sid in ("taiex","tsmc_close","tsmc_weight_estimated","ex_tsmc_estimated","tsmc_weight_official") for p in store.observations(sid)])
    try:return benchmark(rows,load_reference())
    except ValueError as exc:raise HTTPException(409,str(exc))
@app.get("/api/outlook")
def outlook(as_of:str|None=None):
    """IMF WEO / OECD EO outlooks: institution x indicator x economy x year, every captured vintage."""
    from .institution_outlook import load_versions,outlook_view
    as_of=checked_as_of(as_of)
    return outlook_view(load_versions(store,as_of=as_of),as_of=as_of)
class RecoveryRequest(BaseModel):
    source_ids:list[str]|None=Field(default=None,max_length=50)
    job_keys:list[str]|None=Field(default=None,max_length=100)
    max_jobs:int=Field(default=100,ge=1,le=200)
@app.post("/api/recovery/retry")
def retry_failed(request:Request,body:RecoveryRequest):
    origin_check(request)
    require_network()
    try:return engine.start("retry_failed",source_ids=body.source_ids,job_keys=body.job_keys,max_jobs=body.max_jobs)
    except RuntimeError as exc:raise HTTPException(409,str(exc))
    except ValueError as exc:raise HTTPException(422,str(exc))

ledger_lock=threading.Lock()
brief_lock=threading.Lock()

def evaluate_ledger(initial_scan=False,reason="refresh",state=None):
    from .alert_ledger import evaluate
    from .brief_store import capture_events
    with ledger_lock,store.lock:
        result=evaluate(store,transmission_payload(),initial_scan=initial_scan,reason=reason)
        capture_events(store,result)
        return result

def generate_daily_brief(reason="manual",generated_at=None):
    from .daily_digest import build
    from .brief_store import latest,save
    from .alert_ledger import list_events
    with brief_lock,store.lock:
        stamp=generated_at or utc_now()
        histories={}
        for point in main_observations():histories.setdefault(point["series_id"],[]).append(point)
        source_rows=store.sources()
        from .transmission import build as transmission_build
        payload=transmission_build(histories,source_rows,"us10y_taiwan",252,60,store.setting("alert_settings"),{})
        document=build(histories,source_rows,generated_at=stamp,previous=latest(store),
                       alerts=payload["alerts"],events=list_events(store,limit=500)["events"])
        return save(store,document,reason=reason)

def daily_tick(now):
    # Local summaries still run in explicit offline mode. No missed-day fabrication.
    from .brief_store import latest,notify
    settings=store.setting("schedule",{})
    if not settings.get("enabled",True) or engine.lock.locked() or engine.stop_event.is_set() or store.setting("tracking_paused",False):return
    local=now.astimezone(timezone(timedelta(hours=8)))
    if local.strftime("%H:%M")<settings.get("local_time","18:30"):return
    current=latest(store)
    if current and current["digest_date"]>=local.date().isoformat():return
    try:generate_daily_brief(reason="daily_local",generated_at=now.isoformat())
    except Exception as exc:
        notify(store,dedupe_key="brief-error:"+local.date().isoformat(),kind="operation",severity="watch",
               title="每日摘要暫時無法產生",body="本機計算未完成，請查看後端紀錄後重新產生；原有摘要仍保留。",
               evidence={"error":type(exc).__name__+": "+str(exc)[:300]})

def after_sync(state):
    if engine.stop_event.is_set() or store.setting("tracking_paused",False):return
    from .publication_calendar import refresh_calendar
    from .brief_store import capture_sync
    if (state.get("status") in ("completed","partial") and not engine.stop_event.is_set()
            and store.setting("schedule",{}).get("network_enabled",True) and not engine._network_block()):
        state["calendar_refresh"]=refresh_calendar(store,engine._fetch,int(state["end"][:4]))
        store.save_sync(state)
    evaluate_ledger(reason="sync",state=state)
    capture_sync(store,state)
    generate_daily_brief(reason="sync")
engine.after_sync=after_sync
engine.on_tick=daily_tick

@app.get("/api/briefs")
def brief_list(limit:int=Query(60,ge=1,le=365),offset:int=Query(0,ge=0)):
    from .brief_store import list_briefs
    return list_briefs(store,limit=limit,offset=offset)

@app.get("/api/briefs/latest")
def latest_brief():
    from .brief_store import latest
    return {"brief":latest(store)}

@app.post("/api/briefs/generate")
def regenerate_brief(request:Request):
    origin_check(request)
    if engine.lock.locked():raise HTTPException(409,"資料同步中，完成後會自動生成摘要。")
    return generate_daily_brief(reason="manual")

@app.get("/api/briefs/{brief_id}")
def brief_detail(brief_id:str):
    from .brief_store import get
    result=get(store,brief_id)
    if result is None:raise HTTPException(404,"Unknown brief")
    return result

@app.get("/api/notifications")
def notification_list(limit:int=Query(100,ge=1,le=500),offset:int=Query(0,ge=0)):
    from .brief_store import notifications
    return notifications(store,limit,offset)

@app.post("/api/notifications/{notification_id}/read")
def read_notification(request:Request,notification_id:str):
    from .brief_store import mark_read
    origin_check(request)
    try:return mark_read(store,notification_id)
    except KeyError as exc:raise HTTPException(404,str(exc))

@app.get("/api/events")
def event_list(limit:int=Query(100,ge=1,le=500),state:str|None=None):
    from .alert_ledger import list_events
    return list_events(store,limit=limit,state=state)
@app.get("/api/events/{event_id}")
def event_detail(event_id:str):
    from .alert_ledger import detail
    result=detail(store,event_id)
    if not result:raise HTTPException(404,"Unknown event")
    return result
class EventNote(BaseModel):
    note:str=Field(default="",max_length=2000)
@app.post("/api/events/{event_id}/acknowledge")
def event_ack(request:Request,event_id:str,body:EventNote):
    from .alert_ledger import acknowledge
    origin_check(request)
    try:return acknowledge(store,event_id,note=body.note or None)
    except (ValueError,KeyError) as exc:raise HTTPException(404,str(exc))
@app.post("/api/events/{event_id}/notes")
def event_note(request:Request,event_id:str,body:EventNote):
    from .alert_ledger import add_note
    origin_check(request)
    if not body.note.strip():raise HTTPException(422,"Note cannot be empty")
    try:return add_note(store,event_id,body.note.strip())
    except (ValueError,KeyError) as exc:raise HTTPException(404,str(exc))


