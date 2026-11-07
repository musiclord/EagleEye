"""Versioned observations, raw payloads, durable fetch checkpoints and sync runs."""
from datetime import datetime, timezone, date, timedelta
import hashlib
import json
from pathlib import Path
import threading
import uuid
import duckdb
from .connectors import SOURCES
from .history import parse_instant, observation_availability, decode_observation

class Store:
    def __init__(self, directory):
        self.directory = Path(directory)
        self.raw_dir = self.directory / "raw"
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        self.db_path = self.directory / "eagleeye.duckdb"
        self.lock = threading.RLock()
        with self.lock, self.connect() as db:
            db.execute("""CREATE TABLE IF NOT EXISTS observations (
                source_id VARCHAR, series_id VARCHAR, label VARCHAR, unit VARCHAR,
                observation_date VARCHAR, value DOUBLE, retrieved_at VARCHAR,
                source_url VARCHAR, raw_sha256 VARCHAR, release_at VARCHAR,
                PRIMARY KEY(source_id, series_id, observation_date, retrieved_at))""")
            primary_key=db.execute("SELECT constraint_column_names FROM duckdb_constraints() WHERE table_name='observations' AND constraint_type='PRIMARY KEY'").fetchone()
            if primary_key and "raw_sha256" in primary_key[0]:
                db.begin()
                try:
                    db.execute("""CREATE TABLE observations_revision_v2 (
                        source_id VARCHAR, series_id VARCHAR, label VARCHAR, unit VARCHAR,
                        observation_date VARCHAR, value DOUBLE, retrieved_at VARCHAR,
                        source_url VARCHAR, raw_sha256 VARCHAR, release_at VARCHAR,
                        PRIMARY KEY(source_id, series_id, observation_date, retrieved_at))""")
                    db.execute("INSERT INTO observations_revision_v2 SELECT * FROM observations")
                    db.execute("ALTER TABLE observations RENAME TO observations_legacy_hash_key_v1")
                    db.execute("ALTER TABLE observations_revision_v2 RENAME TO observations")
                    db.commit()
                except Exception:
                    db.rollback()
                    raise
            db.execute("ALTER TABLE observations ADD COLUMN IF NOT EXISTS quality VARCHAR DEFAULT 'observed'")
            db.execute("ALTER TABLE observations ADD COLUMN IF NOT EXISTS metadata VARCHAR DEFAULT '{}'")
            db.execute("""CREATE TABLE IF NOT EXISTS ingestion_runs (
                run_id VARCHAR PRIMARY KEY, source_id VARCHAR, attempted_at VARCHAR,
                status VARCHAR, rows INTEGER, error VARCHAR, raw_sha256 VARCHAR)""")
            db.execute("""CREATE TABLE IF NOT EXISTS fetch_checkpoints (
                job_key VARCHAR PRIMARY KEY, source_id VARCHAR, url VARCHAR,
                parser_version VARCHAR, fetched_at VARCHAR, raw_sha256 VARCHAR,
                status VARCHAR, rows INTEGER, error VARCHAR)""")
            db.execute("ALTER TABLE fetch_checkpoints ADD COLUMN IF NOT EXISTS request_signature VARCHAR")
            db.execute("ALTER TABLE fetch_checkpoints ADD COLUMN IF NOT EXISTS parse_scope VARCHAR")
            db.execute("CREATE TABLE IF NOT EXISTS sync_runs (run_id VARCHAR PRIMARY KEY, state_json VARCHAR)")
            db.execute("CREATE TABLE IF NOT EXISTS settings (key VARCHAR PRIMARY KEY, value_json VARCHAR)")
            # Additive phase-5 schema: original raw payloads and observations remain intact.
            db.begin()
            try:
                db.execute("""CREATE TABLE IF NOT EXISTS checkpoint_history (
                    capture_id VARCHAR PRIMARY KEY, job_key VARCHAR, source_id VARCHAR,
                    fetched_at VARCHAR, status VARCHAR, rows INTEGER, error VARCHAR, raw_sha256 VARCHAR)""")
                db.execute("ALTER TABLE checkpoint_history ADD COLUMN IF NOT EXISTS recorded_at VARCHAR")
                db.execute("UPDATE checkpoint_history SET recorded_at=fetched_at WHERE recorded_at IS NULL")
                from .alert_ledger import create_schema
                create_schema(db)
                from .brief_store import create_schema as create_brief_schema
                create_brief_schema(db)
                db.commit()
            except Exception:
                db.rollback()
                raise
    def connect(self):
        return duckdb.connect(str(self.db_path))
    def cache_raw(self, raw):
        digest=hashlib.sha256(raw).hexdigest()
        path=self.raw_dir/(digest+".json")
        with self.lock:
            if not path.exists():
                with path.open("xb") as f:f.write(raw)
        return digest
    def save(self, source, points, fetched_at, digest):
        parse_instant(fetched_at)  # New captures must include an explicit timezone.
        with self.lock,self.connect() as db:
            db.begin()
            try:
                for p in points:
                    observed=p["observation_date"]
                    date.fromisoformat(observed)
                    value=float(p["value"])
                    import math
                    if not math.isfinite(value):raise ValueError("Non-finite value rejected")
                    latest=db.execute("""SELECT raw_sha256,quality,metadata,value,unit,label,release_at,source_url
                        FROM observations WHERE source_id=? AND series_id=? AND observation_date=?
                        ORDER BY TRY_CAST(retrieved_at AS TIMESTAMPTZ) DESC, retrieved_at DESC LIMIT 1""",[source.id,p["series_id"],observed]).fetchone()
                    quality=p.get("status","observed")
                    meta=json.dumps(p.get("metadata",{}),ensure_ascii=False,sort_keys=True)
                    # The same raw payload may be reinterpreted by a corrected parser.
                    # Preserve such revisions even when its content hash is unchanged.
                    interpreted=(digest,quality,meta,value,p["unit"],p["label"],
                                 p.get("release_at"),p.get("source_url",source.url))
                    if latest and (tuple(latest)==interpreted or
                                   (getattr(source, 'content_revisions_only', False)
                                    and tuple(latest[1:])==interpreted[1:])):continue
                    db.execute("""INSERT INTO observations
                        (source_id,series_id,label,unit,observation_date,value,retrieved_at,source_url,raw_sha256,release_at,quality,metadata)
                        VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                        [source.id,p["series_id"],p["label"],p["unit"],observed,value,fetched_at,
                         p.get("source_url",source.url),digest,p.get("release_at"),quality,meta])
                db.commit()
            except Exception:
                db.rollback()
                raise
    def record_run(self, source_id, attempted_at, status, rows=0, error=None, digest=None):
        with self.lock,self.connect() as db:
            db.execute("INSERT INTO ingestion_runs VALUES (?,?,?,?,?,?,?)",
                       [str(uuid.uuid4()),source_id,attempted_at,status,rows,error,digest])
    def observation_versions(self, series_id=None, observation_date=None, start=None, end=None):
        clauses, params = [], []
        for expr, value in [("series_id = ?", series_id), ("observation_date = ?", observation_date),
                            ("observation_date >= ?", start), ("observation_date <= ?", end)]:
            if value is not None:
                clauses.append(expr)
                params.append(value)
        where = "WHERE " + " AND ".join(clauses) if clauses else ""
        with self.lock, self.connect() as db:
            cursor = db.execute(f"SELECT * FROM observations {where}", params)
            keys = [x[0] for x in cursor.description]
            return [decode_observation(dict(zip(keys, row))) for row in cursor.fetchall()]

    def observations(self, series_id=None, as_of=None, start=None, end=None, exclude_partners=False):
        cutoff = parse_instant(as_of) if as_of is not None else None
        clauses, params = [], []
        for expr, value in [("series_id = ?", series_id), ("observation_date >= ?", start),
                            ("observation_date <= ?", end)]:
            if value is not None:
                clauses.append(expr)
                params.append(value)
        where = "WHERE " + " AND ".join(clauses) if clauses else ""
        if exclude_partners:
            clauses.append("NOT contains(series_id, '_partner_')")
            where = "WHERE " + " AND ".join(clauses)
        with self.lock, self.connect() as db:
            # Conversion to TIMESTAMPTZ prevents lexicographic timezone ordering errors.
            # Cutoff mode must filter availability BEFORE choosing the newest revision.
            query = f"SELECT * FROM observations {where}" if cutoff else f"""SELECT * EXCLUDE(version_rank) FROM
                (SELECT *,row_number() OVER(PARTITION BY source_id,series_id,observation_date
                 ORDER BY TRY_CAST(retrieved_at AS TIMESTAMPTZ) DESC,retrieved_at DESC) version_rank
                 FROM observations {where}) WHERE version_rank=1"""
            cursor = db.execute(query, params)
            keys = [x[0] for x in cursor.description]
            rows = [decode_observation(dict(zip(keys, row))) for row in cursor.fetchall()]
        if cutoff:
            selected = {}
            for row in rows:
                availability = observation_availability(row)
                known = availability["known_since"]
                if known is None or parse_instant(known) > cutoff:
                    continue
                key = (row["source_id"], row["series_id"], row["observation_date"])
                previous = selected.get(key)
                if previous is None or parse_instant(row["retrieved_at"]) > parse_instant(previous["retrieved_at"]):
                    selected[key] = row
            rows = list(selected.values())
        return sorted(rows, key=lambda row: (row["observation_date"], row["series_id"], row["source_id"]))

    def catalog(self, as_of=None, exclude_sources=()):
        if as_of is None and not exclude_sources:
            # The catalogue needs no values or metadata. Keep partner histories
            # and revision ranking inside DuckDB instead of decoding every row.
            with self.lock, self.connect() as db:
                rows = db.execute("""WITH latest AS (
                    SELECT source_id,series_id,label,unit,observation_date,
                        row_number() OVER(PARTITION BY source_id,series_id,observation_date
                        ORDER BY TRY_CAST(retrieved_at AS TIMESTAMPTZ) DESC,retrieved_at DESC) AS version_rank
                    FROM observations)
                    SELECT series_id,
                        first(label ORDER BY observation_date DESC,source_id DESC),
                        first(unit ORDER BY observation_date DESC,source_id DESC),
                        first(source_id ORDER BY observation_date,source_id),
                        min(observation_date),max(observation_date),count(DISTINCT observation_date)
                    FROM latest WHERE version_rank=1 GROUP BY series_id ORDER BY series_id""").fetchall()
            keys = ("series_id","label","unit","source_id","start","end","count")
            return [dict(zip(keys,row)) for row in rows]
        grouped = {}
        for row in self.observations(as_of=as_of):
            if row["source_id"] in exclude_sources:
                continue
            sid = row["series_id"]
            if sid not in grouped:
                grouped[sid] = {"series_id":sid,"label":row["label"],"unit":row["unit"],
                    "source_id":row["source_id"],"start":row["observation_date"],"end":row["observation_date"],"dates":set()}
            item = grouped[sid]
            item.update(label=row["label"], unit=row["unit"], end=row["observation_date"])
            item["dates"].add(row["observation_date"])
        return [{**{k:v for k,v in grouped[sid].items() if k != "dates"},"count":len(grouped[sid]["dates"])}
                for sid in sorted(grouped)]

    def sources(self, as_of=None):
        from .registry import source_map
        cutoff = parse_instant(as_of) if as_of is not None else None
        today = (cutoff or datetime.now(timezone.utc)).astimezone(timezone(timedelta(hours=8))).date()
        coverage, latest_runs, successes, failure_counts = {}, {}, {}, {}
        def order(stamp):
            try:
                return parse_instant(stamp)
            except (ValueError, TypeError):
                return datetime.min.replace(tzinfo=timezone.utc)
        if cutoff is None:
            with self.lock, self.connect() as db:
                # Revisions never remove a source/date, so this aggregate does
                # not need value selection, metadata decoding, or a window sort.
                coverage = {row[0]:row[1:] for row in db.execute("""SELECT source_id,
                    min(observation_date),max(observation_date),count(DISTINCT observation_date)
                    FROM observations GROUP BY source_id""").fetchall()}
                runs = db.execute("""SELECT source_id,attempted_at,status,rows,error,raw_sha256 FROM (
                    SELECT *,row_number() OVER(PARTITION BY source_id
                    ORDER BY TRY_CAST(attempted_at AS TIMESTAMPTZ) DESC NULLS LAST,rowid DESC) AS rank
                    FROM ingestion_runs) WHERE rank=1""").fetchall()
                latest_runs = {row[0]:row[1:] for row in runs}
                successes = dict(db.execute("""SELECT source_id,attempted_at FROM (
                    SELECT source_id,attempted_at,row_number() OVER(PARTITION BY source_id
                    ORDER BY TRY_CAST(attempted_at AS TIMESTAMPTZ) DESC NULLS LAST,rowid DESC) AS rank
                    FROM ingestion_runs WHERE status='ok') WHERE rank=1""").fetchall())
                failure_counts = dict(db.execute("""SELECT source_id,count(*) FROM fetch_checkpoints
                    WHERE status='error' GROUP BY source_id""").fetchall())
        else:
            available = {}
            for row in self.observations(as_of=as_of):
                available.setdefault(row["source_id"], set()).add(row["observation_date"])
            coverage = {sid:(min(days),max(days),len(days)) for sid,days in available.items()}
            with self.lock, self.connect() as db:
                runs = db.execute("SELECT source_id,attempted_at,status,rows,error,raw_sha256 FROM ingestion_runs").fetchall()
                checks = db.execute("SELECT job_key,source_id,recorded_at,status FROM checkpoint_history").fetchall()
            def permitted(stamp):
                try:
                    return parse_instant(stamp) <= cutoff
                except (ValueError, TypeError):
                    return False
            for row in runs:
                if not permitted(row[1]):
                    continue
                sid, run = row[0], row[1:]
                if sid not in latest_runs or order(run[0]) >= order(latest_runs[sid][0]):
                    latest_runs[sid] = run
                if run[1] == "ok" and (sid not in successes or order(run[0]) >= order(successes[sid])):
                    successes[sid] = run[0]
            # Current checkpoints cannot describe the past. Only captured
            # processing history participates, with limited coverage disclosed.
            latest_checks = {}
            for row in checks:
                if permitted(row[2]) and (row[0] not in latest_checks or order(row[2]) >= order(latest_checks[row[0]][2])):
                    latest_checks[row[0]] = row
            for row in latest_checks.values():
                if row[3] == "error":
                    failure_counts[row[1]] = failure_counts.get(row[1],0)+1
        output = []
        for source in source_map().values():
            run = latest_runs.get(source.id)
            success = successes.get(source.id)
            first, latest, count = coverage.get(source.id,(None,None,0))
            failures = failure_counts.get(source.id,0)
            age = (today-date.fromisoformat(latest)).days if latest else None
            rule = f"觀測期末超過 {source.freshness_days} 日曆日；請併讀發布日曆"
            if source.frequency == "event":
                age = (today-order(success).astimezone(timezone(timedelta(hours=8))).date()).days if success else None
                rule = f"政策事件持續有效；最近成功查核超過 {source.freshness_days} 日才提示"
            output.append({"id":source.id,"label":source.label,"url":source.url,"status":run[1] if run else "never",
                "last_attempt_at":run[0] if run else None,"last_success_at":success,"first_observation_date":first,
                "latest_observation_date":latest,"stale":age is not None and age>source.freshness_days,"age_calendar_days":age,
                "stale_rule":rule,"rows":run[2] if run else 0,"error":run[3] if run else None,"raw_sha256":run[4] if run else None,
                "available_dates":count,"frequency":source.frequency,"coverage_note":source.coverage_note,"failed_requests":failures,
                "publish":getattr(source,"publish",True),
                # Licence attribution (e.g. Open Government Data License): year of the latest observation shown.
                "attribution":source.attribution.format(year=(latest or today.isoformat())[:4]) if getattr(source,"attribution",None) else None,
                "license_url":getattr(source,"license_url",None),
                "as_of":cutoff.isoformat() if cutoff else None,
                "checkpoint_history_complete":False if cutoff else None})
        return output
    def checkpoint(self,key):
        with self.lock,self.connect() as db:
            c=db.execute("SELECT * FROM fetch_checkpoints WHERE job_key=?",[key])
            keys=[x[0] for x in c.description];row=c.fetchone()
            return dict(zip(keys,row)) if row else None
    def request_checkpoint(self,job):
        """Find raw HTTP cache across parsing-window job keys, without changing them."""
        with self.lock,self.connect() as db:
            c=db.execute("""SELECT * FROM fetch_checkpoints
                WHERE status='ok' AND raw_sha256 IS NOT NULL AND
                (request_signature=? OR (request_signature IS NULL AND source_id=? AND url=? AND ?))
                ORDER BY TRY_CAST(fetched_at AS TIMESTAMPTZ) DESC LIMIT 1""",
                [job.request_signature,job.source_id,job.url,job.method.upper()=='GET' and not job.body])
            keys=[x[0] for x in c.description];row=c.fetchone()
            return dict(zip(keys,row)) if row else None

    def checkpoint_save(self,job,status,digest=None,rows=0,error=None,fetched_at=None):
        recorded = utc_now()
        stamp = fetched_at or recorded
        parse_instant(stamp)
        with self.lock,self.connect() as db:
            db.begin()
            try:
                db.execute("""INSERT OR REPLACE INTO fetch_checkpoints
                    (job_key,source_id,url,parser_version,fetched_at,raw_sha256,status,rows,error,request_signature,parse_scope)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                           [job.key,job.source_id,job.url,job.parser_version,stamp,digest,status,rows,error,
                            getattr(job,'request_signature',None),getattr(job,'parse_scope','')])
                db.execute("""INSERT INTO checkpoint_history
                    (capture_id,job_key,source_id,fetched_at,status,rows,error,raw_sha256,recorded_at) VALUES (?,?,?,?,?,?,?,?,?)""",
                           [str(uuid.uuid4()),job.key,job.source_id,stamp,status,rows,error,digest,recorded])
                db.commit()
            except Exception:
                db.rollback()
                raise
    def save_sync(self,state):
        with self.lock,self.connect() as db:
            db.execute("INSERT OR REPLACE INTO sync_runs VALUES (?,?)",[state["id"],json.dumps(state,ensure_ascii=False)])
    def get_sync(self,run_id=None):
        with self.lock,self.connect() as db:
            row=db.execute("SELECT state_json FROM sync_runs WHERE run_id=?",[run_id]).fetchone() if run_id else db.execute("SELECT state_json FROM sync_runs ORDER BY json_extract_string(state_json,'$.started_at') DESC LIMIT 1").fetchone()
            return json.loads(row[0]) if row else None
    def setting(self,key,default=None):
        with self.lock,self.connect() as db:
            row=db.execute("SELECT value_json FROM settings WHERE key=?",[key]).fetchone()
            return json.loads(row[0]) if row else default
    def set_setting(self,key,value):
        with self.lock,self.connect() as db:
            db.execute("INSERT OR REPLACE INTO settings VALUES (?,?)",[key,json.dumps(value,ensure_ascii=False)])
def utc_now():
    return datetime.now(timezone.utc).isoformat(timespec="microseconds")
