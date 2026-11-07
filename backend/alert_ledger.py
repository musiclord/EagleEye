"""Durable, explicit live alert evaluations with immutable evidence and lifecycle records."""
from datetime import datetime, timezone
import hashlib
import json
import uuid
from .history import parse_instant, observation_availability

LIMITATION = "事件時間是本機實際評估時間；初次掃描只記錄目前狀態，不聲稱過去已發生告警。資料過期、缺漏或規則未評估不會自動視為解除。"
SEVERITIES = {"info":0,"watch":1,"warning":2,"high":3,"critical":4}


def create_schema(db):
    """Called transactionally by Store initialization; ledger reads never create tables."""
    db.execute("""CREATE TABLE IF NOT EXISTS alert_rule_state (
        rule_id VARCHAR PRIMARY KEY, state_json VARCHAR)""")
    db.execute("""CREATE TABLE IF NOT EXISTS alert_episodes (
        event_id VARCHAR PRIMARY KEY, rule_id VARCHAR, opened_at VARCHAR,
        updated_at VARCHAR, state VARCHAR, episode_json VARCHAR)""")
    db.execute("""CREATE TABLE IF NOT EXISTS alert_evidence (
        evidence_id VARCHAR PRIMARY KEY, created_at VARCHAR, snapshot_json VARCHAR)""")
    db.execute("""CREATE TABLE IF NOT EXISTS alert_event_records (
        record_id VARCHAR PRIMARY KEY, event_id VARCHAR, rule_id VARCHAR,
        event_type VARCHAR, recorded_at VARCHAR, evidence_id VARCHAR, record_json VARCHAR)""")
    db.execute("""CREATE TABLE IF NOT EXISTS alert_evaluation_batches (
        batch_id VARCHAR PRIMARY KEY, evaluated_at VARCHAR, reason VARCHAR,
        initial_scan BOOLEAN, changed_rules INTEGER, new_records INTEGER)""")


def _json(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",",":"), allow_nan=False)


def _digest(value):
    return hashlib.sha256(_json(value).encode("utf-8")).hexdigest()


def _stamp(value=None):
    return parse_instant(value or datetime.now(timezone.utc)).isoformat()


def _read(db, table, key, value, column):
    row = db.execute(f"SELECT {column} FROM {table} WHERE {key}=?", [value]).fetchone()
    return json.loads(row[0]) if row else None


def _source_rows(payload, alert):
    ids = alert.get("source_ids", [])
    return {sid:[{**p, **observation_availability(p)} for p in payload.get("histories",{}).get(sid, [])]
            for sid in sorted(ids)}


def _snapshot(payload, alert):
    # A generated response time and acknowledgement are not new market evidence.
    rule = {k:v for k,v in alert.items() if k != "acknowledged"}
    ids = set(alert.get("source_ids", []))
    indicators = [p for p in payload.get("indicators",[]) if p.get("id") in ids]
    versions = {"transmission":payload.get("model_version", "unspecified")}
    if str(alert["rule_id"]).startswith("positions:"):
        from .positions import MODEL_VERSION
        versions["positions"] = MODEL_VERSION
    inputs = _source_rows(payload, alert)
    auxiliary = {}
    if str(alert["rule_id"]).startswith("positions:"):
        # Valid-change tests use independently observed venue calendars. Capture
        # those dependencies too, including their provenance, not just target values.
        from .positions import CALENDAR_SERIES
        for target in ids:
            venue = "tx" if target.startswith("tx_") else "tpex" if target.startswith("tpex_") else "twse"
            for dependency in CALENDAR_SERIES[venue]:
                if dependency not in ids:
                    auxiliary[dependency] = [{**p, **observation_availability(p)}
                        for p in payload.get("histories",{}).get(dependency,[])]
    evidence = {"rule_id":alert["rule_id"],"alert_instance_id":alert.get("id"),
                "model_versions":versions,"settings":payload.get("alert_settings",{}),
                "alert":rule,"indicators":indicators,"input_observations":inputs,"auxiliary_observations":auxiliary,
                "input_count":sum(len(rows) for rows in inputs.values())+sum(len(rows) for rows in auxiliary.values()),
                "availability_semantics":"local capture; max(retrieved_at, actual release_at); date-only release uses Taipei end-of-day"}
    evidence["snapshot_id"] = _digest(evidence)
    return evidence


def _quality_status(alert, snapshot):
    status = alert.get("status", "insufficient")
    if status not in ("triggered","normal"):
        return "data_unavailable"
    if any(p.get("freshness",{}).get("stale") or p.get("latest") is None
           for p in snapshot["indicators"]):
        return "data_unavailable"
    # Explicitly missing data must not turn an existing alert into "resolved".
    if any(not rows for rows in snapshot["input_observations"].values()):
        return "data_unavailable"
    return status


def _save_episode(db, episode):
    db.execute("INSERT OR REPLACE INTO alert_episodes VALUES (?,?,?,?,?,?)",
               [episode["event_id"],episode["rule_id"],episode["opened_at"],episode["updated_at"],episode["state"],_json(episode)])


def _record(db, episode, kind, at, evidence_id, reason, note=None, actor="system"):
    item = {"record_id":str(uuid.uuid4()),"event_id":episode["event_id"],"rule_id":episode["rule_id"],
            "event_type":kind,"recorded_at":at,"observation_date":episode.get("observation_date"),
            "evidence_id":evidence_id,"reason":reason,"note":note,"actor":actor}
    db.execute("INSERT INTO alert_event_records VALUES (?,?,?,?,?,?,?)", [item["record_id"],item["event_id"],item["rule_id"],kind,at,evidence_id,_json(item)])
    episode["event_count"] = episode.get("event_count",0) + 1
    episode["latest_event_type"] = kind
    episode["updated_at"] = at
    return item


def _observation_signature(evidence):
    return _digest({key:evidence.get(key,{}) for key in ("input_observations","auxiliary_observations")}) if evidence else None


def _new_episode(alert, snapshot, at, initial, state):
    ident = str(uuid.uuid4())
    return {"event_id":ident,"id":ident,"rule_id":alert["rule_id"],"title":alert.get("title",alert["rule_id"]),
            "group":alert.get("group","global"),"state":state,"opened_at":at,"updated_at":at,
            "closed_at":None,"initial_scan":initial,"event_count":0,"ever_triggered":False,
            "acknowledged":False,"acknowledged_at":None,"evidence_id":snapshot["snapshot_id"]}


def evaluate(store, payload, evaluated_at=None, initial_scan=False, reason="refresh"):
    """Evaluate once after a live ingest/settings change. Never invoke for GET replay."""
    replay = payload.get("replay")
    historical = replay.get("active",False) if isinstance(replay,dict) else bool(replay)
    if historical or payload.get("mode") in ("replay","historical"):
        raise ValueError("歷史回放不得寫入即時告警事件簿")
    at = _stamp(evaluated_at)
    records, touched = [], 0
    with store.lock, store.connect() as db:
        db.begin()
        try:
            previous_rows = db.execute("SELECT rule_id,state_json FROM alert_rule_state").fetchall()
            states = {key:json.loads(value) for key,value in previous_rows}
            first = not states
            initial = bool(initial_scan or first)
            alerts = {a["rule_id"]:dict(a) for a in payload.get("alerts",[])}
            indicators = {i["id"]:i for i in payload.get("indicators",[])}
            for rule_id, previous in states.items():
                if rule_id in alerts or not previous.get("episode_id"):
                    continue
                # A missing rule is not a normal result. Only a previously emitted
                # freshness rule may explicitly recover when its current input is fresh.
                old = previous["alert"]
                ids = old.get("source_ids",[])
                fresh = bool(ids) and all(sid in indicators and indicators[sid].get("latest")
                            and not indicators[sid].get("freshness",{}).get("stale",True) for sid in ids)
                recovered = rule_id.endswith(":freshness") and fresh
                alerts[rule_id] = {**old,"status":"normal" if recovered else "not_evaluated",
                    "explanation":"來源已恢復新鮮資料" if recovered else "本次未評估此規則，不能判定風險解除"}
                if recovered:
                    alerts[rule_id]["observation_date"] = max(indicators[sid]["latest"]["observation_date"] for sid in ids)
            for rule_id, alert in alerts.items():
                snapshot = _snapshot(payload, alert)
                sid = snapshot["snapshot_id"]
                previous = states.get(rule_id)
                if previous and previous["snapshot_id"] == sid:
                    continue
                touched += 1
                db.execute("INSERT INTO alert_evidence VALUES (?,?,?) ON CONFLICT DO NOTHING", [sid,at,_json(snapshot)])
                status = _quality_status(alert,snapshot)
                episode = _read(db,"alert_episodes","event_id",previous["episode_id"],"episode_json") if previous and previous.get("episode_id") else None
                old_evidence = _read(db,"alert_evidence","evidence_id",previous["snapshot_id"],"snapshot_json") if previous else None
                was_closed = episode and episode["state"] in ("resolved","retracted")
                same_date = bool(previous and previous.get("observation_date") == alert.get("observation_date"))
                input_changed = _observation_signature(old_evidence) != _observation_signature(snapshot)
                settings_changed = old_evidence is not None and old_evidence.get("alert",{}).get("threshold") != snapshot.get("alert",{}).get("threshold")
                model_changed = old_evidence is not None and old_evidence.get("model_versions") != snapshot.get("model_versions")
                cause = "settings_changed" if settings_changed else "model_changed" if model_changed else (
                    "source_revision" if same_date and input_changed else "new_observation" if not same_date else "evaluation_changed")
                kind = None
                if status == "triggered":
                    if episode is None or was_closed:
                        episode = _new_episode(alert,snapshot,at,initial,"active")
                        kind = "reopened" if previous and previous.get("episode_id") else "triggered"
                    else:
                        kind = "escalated" if SEVERITIES.get(alert.get("severity","info"),0)>SEVERITIES.get(episode.get("severity","info"),0) else "ongoing"
                    episode["state"] = "active"
                    episode["ever_triggered"] = True
                    episode["trigger_observation_date"] = alert.get("observation_date")
                    episode["closed_at"] = None
                elif status == "data_unavailable":
                    if episode is None or was_closed:
                        episode = _new_episode(alert,snapshot,at,initial,"data_unavailable")
                    episode["state"] = "data_unavailable"
                    kind = "data_unavailable"
                    cause = "missing_stale_or_not_evaluated"
                elif episode and not was_closed:
                    # Same-period reclassification is a correction, not evidence
                    # that the market subsequently recovered. Threshold/model changes
                    # likewise cannot be reported as a market recovery.
                    not_newer = not alert.get("observation_date") or not episode.get("trigger_observation_date") or alert["observation_date"] <= episode["trigger_observation_date"]
                    kind = "retracted" if episode.get("ever_triggered") and (not_newer or settings_changed or model_changed) else "resolved"
                    episode["state"] = kind
                    episode["closed_at"] = at
                    if not episode.get("ever_triggered"):
                        cause = "data_restored"
                if episode and kind:
                    episode.update(title=alert.get("title",rule_id),group=alert.get("group","global"),
                        latest_status=alert.get("status"),severity=alert.get("severity","info"),
                        observation_date=alert.get("observation_date"),value=alert.get("value"),unit=alert.get("unit"),
                        threshold=alert.get("threshold"),evidence_id=sid,alert_instance_id=alert.get("id"),
                        acknowledged=False,acknowledged_at=None,reason=cause)
                    records.append(_record(db,episode,kind,at,sid,"initial_current_scan" if initial and previous is None else cause))
                    _save_episode(db,episode)
                state = {"snapshot_id":sid,"episode_id":episode["event_id"] if episode else None,"status":status,
                         "observation_date":alert.get("observation_date"),"alert":alert,"evaluated_at":at}
                db.execute("INSERT OR REPLACE INTO alert_rule_state VALUES (?,?)",[rule_id,_json(state)])
            if touched:
                db.execute("INSERT INTO alert_evaluation_batches VALUES (?,?,?,?,?,?)", [str(uuid.uuid4()),at,reason,initial,touched,len(records)])
            db.commit()
        except Exception:
            db.rollback()
            raise
    return {"evaluated_at":at,"initial_scan":initial,"changed_rules":touched,"new_records":len(records),"events":records}


def list_events(store, limit=100, state=None):
    limit = max(1,min(int(limit),500))
    if state not in (None,"active","data_unavailable","resolved","retracted","all"):
        raise ValueError("未知的事件狀態")
    clause, params = ("WHERE state=?",[state]) if state and state != "all" else ("",[])
    with store.lock, store.connect() as db:
        count = db.execute(f"SELECT count(*) FROM alert_episodes {clause}",params).fetchone()[0]
        rows = db.execute(f"SELECT episode_json FROM alert_episodes {clause} ORDER BY CAST(updated_at AS TIMESTAMPTZ) DESC,event_id LIMIT ?",params+[limit]).fetchall()
    return {"events":[json.loads(row[0]) for row in rows],"total":count,"limitation":LIMITATION}


def detail(store, event_id):
    with store.lock, store.connect() as db:
        episode = _read(db,"alert_episodes","event_id",event_id,"episode_json")
        if episode is None:
            return None
        rows = db.execute("SELECT record_json FROM alert_event_records WHERE event_id=? ORDER BY CAST(recorded_at AS TIMESTAMPTZ),rowid",[event_id]).fetchall()
        records = [json.loads(row[0]) for row in rows]
        evidence_ids = list(dict.fromkeys(r["evidence_id"] for r in records if r.get("evidence_id")))
        evidence = [_read(db,"alert_evidence","evidence_id",key,"snapshot_json") for key in evidence_ids]
        latest = _read(db,"alert_evidence","evidence_id",episode["evidence_id"],"snapshot_json")
    return {**episode,"records":records,"evidence":latest,"evidence_history":evidence,"limitation":LIMITATION}


def _annotate(store, event_id, kind, note, actor, at):
    if note is not None:
        note = str(note).strip()
        if len(note)>4000:
            raise ValueError("事件註記不得超過 4000 字")
    if kind == "note" and not note:
        raise ValueError("請輸入事件註記")
    at = _stamp(at)
    with store.lock, store.connect() as db:
        db.begin()
        try:
            episode = _read(db,"alert_episodes","event_id",event_id,"episode_json")
            if episode is None:
                raise KeyError("找不到告警事件")
            if kind == "acknowledged" and episode.get("acknowledged") and not note:
                db.rollback()
                return episode
            if kind == "acknowledged":
                episode["acknowledged"] = True
                episode["acknowledged_at"] = at
            _record(db,episode,kind,at,episode["evidence_id"],"user_review",note,actor)
            _save_episode(db,episode)
            db.commit()
        except Exception:
            db.rollback()
            raise
    return episode


def acknowledge(store, event_id, note=None, actor="local", at=None):
    return _annotate(store,event_id,"acknowledged",note,actor,at)


def add_note(store, event_id, note, actor="local", at=None):
    return _annotate(store,event_id,"note",note,actor,at)