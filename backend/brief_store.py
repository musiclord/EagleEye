"""Local immutable daily briefs and a deduplicated notification inbox. No network."""
from datetime import datetime, timezone, timedelta, date
import hashlib
import json
import uuid
from .history import parse_instant

TAIPEI=timezone(timedelta(hours=8))

def encode(value):
    return json.dumps(value,ensure_ascii=False,sort_keys=True,separators=(",",":"),allow_nan=False)

def create_schema(db):
    db.execute("""CREATE TABLE IF NOT EXISTS daily_briefs (
        id VARCHAR PRIMARY KEY,digest_date VARCHAR,version INTEGER,
        content_hash VARCHAR,created_at VARCHAR,payload_json VARCHAR,
        UNIQUE(digest_date,content_hash))""")
    db.execute("""CREATE TABLE IF NOT EXISTS notification_event_deliveries (
        delivery_key VARCHAR PRIMARY KEY,notification_id VARCHAR)""")
    db.execute("""CREATE TABLE IF NOT EXISTS local_notifications (
        id VARCHAR PRIMARY KEY,dedupe_key VARCHAR UNIQUE,kind VARCHAR,
        severity VARCHAR,title VARCHAR,body VARCHAR,created_at VARCHAR,
        read_at VARCHAR,brief_id VARCHAR,event_id VARCHAR,payload_json VARCHAR)""")

def _notification(db,*,dedupe_key,kind,title,body,severity="info",created_at=None,brief_id=None,event_id=None,evidence=None):
    found=db.execute("SELECT payload_json,read_at FROM local_notifications WHERE dedupe_key=?",[dedupe_key]).fetchone()
    if found:return {**json.loads(found[0]),"read_at":found[1]}
    item={"id":str(uuid.uuid4()),"kind":kind,"severity":severity,"title":title,"body":body,
          "created_at":parse_instant(created_at or datetime.now(timezone.utc)).isoformat(),
          "read_at":None,"brief_id":brief_id,"event_id":event_id,"evidence":evidence or {}}
    db.execute("INSERT INTO local_notifications VALUES (?,?,?,?,?,?,?,?,?,?,?)",
               [item["id"],dedupe_key,kind,severity,title,body,item["created_at"],None,brief_id,event_id,encode(item)])
    return item

def notify(store,**values):
    with store.lock,store.connect() as db:
        return _notification(db,**values)

def latest(store):
    with store.lock,store.connect() as db:
        row=db.execute("SELECT payload_json FROM daily_briefs ORDER BY digest_date DESC,version DESC LIMIT 1").fetchone()
    return json.loads(row[0]) if row else None

def get(store,ident):
    with store.lock,store.connect() as db:
        row=db.execute("SELECT payload_json FROM daily_briefs WHERE id=?",[ident]).fetchone()
    return json.loads(row[0]) if row else None

def list_briefs(store,limit=60,offset=0):
    with store.lock,store.connect() as db:
        count=db.execute("SELECT count(*) FROM daily_briefs").fetchone()[0]
        rows=db.execute("SELECT payload_json FROM daily_briefs ORDER BY digest_date DESC,version DESC LIMIT ? OFFSET ?",[limit,offset]).fetchall()
    fields=("id","digest_date","created_at","title","lead","version","attention_count","content_hash","model_version")
    return {"briefs":[{k:p.get(k) for k in fields} for p in (json.loads(r[0]) for r in rows)],"total":count}

def save(store,payload,reason="manual"):
    """A same-day identical semantic input reuses the saved original, including wording."""
    day=date.fromisoformat(payload["digest_date"]).isoformat()
    at=parse_instant(payload["generated_at"])
    if at.astimezone(TAIPEI).date().isoformat()!=day:raise ValueError("摘要日期必須與台北生成日期一致")
    digest=payload["content_hash"]
    if not isinstance(digest,str) or len(digest)!=64 or any(c not in "0123456789abcdef" for c in digest):
        raise ValueError("Invalid digest fingerprint")
    with store.lock,store.connect() as db:
        db.begin()
        try:
            old=db.execute("SELECT payload_json FROM daily_briefs WHERE digest_date=? AND content_hash=?",[day,digest]).fetchone()
            if old:
                db.rollback()
                return {"brief":json.loads(old[0]),"created":False}
            version=db.execute("SELECT coalesce(max(version),0)+1 FROM daily_briefs WHERE digest_date=?",[day]).fetchone()[0]
            item={**payload,"id":str(uuid.uuid4()),"created_at":at.isoformat(),"version":version,"reason":reason,
                  "attention_count":sum(p.get("status") in ("attention","watch","triggered") or p.get("severity") in ("watch","warning","high","critical")
                                        for p in payload.get("topics",payload.get("items",[])))}
            db.execute("INSERT INTO daily_briefs VALUES (?,?,?,?,?,?)",[item["id"],day,version,digest,at.isoformat(),encode(item)])
            # One first daily notice, then only real post-sync content revisions.
            # Identical data/wording refreshes never enqueue a new notification.
            if version==1 or reason=="sync":
                _notification(db,dedupe_key="daily:"+day if version==1 else "daily-revision:"+day+":"+digest,
                    kind="brief",title=day+(" 每日觀測已產生" if version==1 else " 每日觀測已有新資料"),
                    body=item.get("lead") or item["title"],created_at=at,brief_id=item["id"],
                    evidence={"version":version,"content_hash":digest})
            for notice in payload.get("topic_notifications",payload.get("notifications",[])):
                # Ledger rules already notify lifecycle transitions. Only supplemental
                # daily rules enqueue here, once per topic and semantic evidence.
                if notice.get("kind")!="market":continue
                if "rules" in notice:
                    supplemental=[r for r in notice["rules"] if not r.get("event_id")]
                    if not supplemental:continue
                    signature=hashlib.sha256(encode({"topic":notice.get("topic_id"),
                        "rules":sorted(r["id"] for r in supplemental)}).encode()).hexdigest()
                    key="topic-signal:"+signature
                else:
                    if notice.get("event_id"):continue
                    key="signal:"+notice["id"]
                _notification(db,dedupe_key=key,kind="alert",
                    title=notice["title"],body=notice["body"],severity=notice.get("severity","watch"),
                    created_at=at,brief_id=item["id"],evidence=notice)
            issues=payload.get("quality_issues",[])
            if issues:
                signature=hashlib.sha256(encode(sorted(p["id"] for p in issues)).encode()).hexdigest()
                _notification(db,dedupe_key="digest-quality:"+signature,kind="data",severity="watch",
                    title=f"{len(issues)} 項資料或比較條件待查核",
                    body="這些項目暫不作市場判斷："+ "、".join(p["title"] for p in issues[:4])+
                         ("等；完整原因與日期請查看摘要。" if len(issues)>4 else "。完整依據請查看摘要。"),
                    created_at=at,brief_id=item["id"],evidence={"quality_issues":issues})
            db.commit()
        except Exception:
            db.rollback()
            raise
    return {"brief":item,"created":True}

def notifications(store,limit=100,offset=0):
    with store.lock,store.connect() as db:
        total,unread=db.execute("SELECT count(*),count(*) FILTER (WHERE read_at IS NULL) FROM local_notifications").fetchone()
        rows=db.execute("""SELECT payload_json,read_at FROM local_notifications
            ORDER BY CAST(created_at AS TIMESTAMPTZ) DESC,id DESC LIMIT ? OFFSET ?""",[limit,offset]).fetchall()
    return {"items":[{**json.loads(row[0]),"read_at":row[1]} for row in rows],"total":total,"unread":unread,
            "delivery":"local_inbox","note":"本機保存；關閉網頁後仍會記錄，桌面彈出需網頁開啟且允許通知。"}

def mark_read(store,ident,at=None):
    at=parse_instant(at or datetime.now(timezone.utc)).isoformat()
    with store.lock,store.connect() as db:
        row=db.execute("SELECT payload_json,read_at FROM local_notifications WHERE id=?",[ident]).fetchone()
        if not row:raise KeyError("Unknown notification")
        read_at=row[1] or at
        db.execute("UPDATE local_notifications SET read_at=? WHERE id=?",[read_at,ident])
    return {**json.loads(row[0]),"read_at":read_at}

def capture_events(store,evaluation):
    """Bundle a topic's same-day transitions, preserving every immutable event link.

    Delivery keys are tracked individually so replaying a subset or a different
    batch order cannot resend a previously delivered transition. Ledger episodes,
    records and evidence snapshots are never changed here.
    """
    from .daily_digest import TOPIC_SPECS,topic_for_series
    labels={"triggered":"需要注意","reopened":"再次觸發","escalated":"提醒升級",
            "resolved":"條件已解除","retracted":"原判斷已修正","data_unavailable":"資料不足，暫停判讀"}
    titles={t[0]:t[1] for t in TOPIC_SPECS}
    severity_rank={"info":0,"watch":1,"warning":2,"high":3,"critical":4}
    output=[];groups={}
    with store.lock,store.connect() as db:
        db.begin()
        try:
            for record in evaluation.get("events",[]):
                kind=record["event_type"]
                if kind not in labels:continue
                row=db.execute("SELECT episode_json FROM alert_episodes WHERE event_id=?",[record["event_id"]]).fetchone()
                raw=db.execute("SELECT snapshot_json FROM alert_evidence WHERE evidence_id=?",[record.get("evidence_id")]).fetchone()
                if not row:continue
                episode=json.loads(row[0]);snapshot=json.loads(raw[0]) if raw else {}
                alert=snapshot.get("alert",{})
                severity=alert.get("severity",episode.get("severity","watch"))
                key=f"event:{episode['event_id']}:{kind}"
                if kind=="escalated":key+=":"+severity
                # Respect notifications saved by v1 as well as bundled v2 deliveries.
                if db.execute("SELECT 1 FROM notification_event_deliveries WHERE delivery_key=?",[key]).fetchone():continue
                if db.execute("SELECT 1 FROM local_notifications WHERE dedupe_key=?",[key]).fetchone():continue
                topic=topic_for_series(alert.get("source_ids",[]))
                observed=alert.get("observation_date") or record.get("observation_date") or "未確認"
                lead=alert.get("message") or alert.get("explanation") or alert.get("reason") or "請查看事件證據與門檻。"
                if not isinstance(lead,str):lead=encode(lead)
                entry={"record_id":record["record_id"],"evidence_id":record.get("evidence_id"),
                       "event_id":record["event_id"],"rule_id":record["rule_id"],"event_type":kind,
                       "observation_date":observed,"threshold":alert.get("threshold"),"method":alert.get("method"),
                       "baseline":alert.get("baseline"),"alert":alert,"title":episode["title"],"body":lead,
                       "severity":severity,"recorded_at":record["recorded_at"],"delivery_key":key}
                group=(topic or "rule:"+record["rule_id"],kind,observed)
                groups.setdefault(group,{"topic_id":topic,"entries":{}})["entries"][key]=entry
            for (_,kind,observed),group in sorted(groups.items()):
                entries=[group["entries"][key] for key in sorted(group["entries"])]
                first=entries[0];topic=group["topic_id"]
                delivery_keys=[entry["delivery_key"] for entry in entries]
                key="event-topic:"+hashlib.sha256(encode(delivery_keys).encode()).hexdigest()
                headline=titles[topic] if topic and len(entries)>1 else first["title"]
                body=f"觀測日期 {observed}。"+"；".join(entry["body"] for entry in entries[:3])
                if len(entries)>1:body+=f" 共 {len(entries)} 條規則的狀態變化合併顯示，不代表獨立風險數量。"
                if kind=="retracted":body+=" 此為資料或規則重新分類，不能視為市場已恢復。"
                if kind=="data_unavailable":body+=" 保留既有觀測，不用缺值判定安全。"
                evidence={k:first[k] for k in ("record_id","evidence_id","rule_id","event_type","observation_date")}
                evidence.update(topic_id=topic,events=entries,rule_ids=[e["rule_id"] for e in entries],
                                event_ids=[e["event_id"] for e in entries],
                                evidence_ids=[e["evidence_id"] for e in entries])
                note=_notification(db,dedupe_key=key,kind="data" if kind=="data_unavailable" else "alert",
                    title=labels[kind]+"："+headline,body=body,
                    severity="info" if kind in ("resolved","retracted") else "watch" if kind=="data_unavailable" else
                        max((e["severity"] for e in entries),key=lambda value:severity_rank.get(value,0)),
                    created_at=max(e["recorded_at"] for e in entries),event_id=first["event_id"],evidence=evidence)
                for delivery in delivery_keys:
                    db.execute("INSERT INTO notification_event_deliveries VALUES (?,?)",[delivery,note["id"]])
                output.append(note)
            db.commit()
        except Exception:
            db.rollback()
            raise
    return output

def capture_sync(store,state):
    if state.get("status") not in ("partial","failed","error","interrupted"):return None
    errors=state.get("errors") or []
    return notify(store,dedupe_key="sync:"+state["id"],kind="operation",severity="watch",
        title="部分資料尚未更新，先保留最後成功版本",
        body=f"本次完成 {state.get('done',0)} / {state.get('total',0)} 項，失敗 {state.get('failed',0)} 項。暫時錯誤會依冷卻與次數限制補抓；格式或權限問題需查核。",
        created_at=state.get("finished_at") or state.get("started_at"),evidence={
            "run_id":state["id"],"status":state.get("status"),"errors":errors[:10],
            "network_note":state.get("network_note"),"next_step":"時序・品質・事件 → 失敗補抓"})
