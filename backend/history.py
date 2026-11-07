"""Honest local-capture replay, not an assertion of historical public availability."""
from datetime import date, datetime, time, timedelta, timezone
import json

TAIPEI = timezone(timedelta(hours=8))
LIMITATIONS = [
    "歷史回放顯示本機當時已保存且已知發布的版本；回補取得的舊月份資料不會被當成當時已知。",
    "來源只提供發布日期時，保守採台北時間當日結束；發布時間未知會另外標記。未公布發布時間者以本機取得時間為下限。",
    "回放使用目前的模型程式與設定重算；不代表當年的模型、門檻或當時已產生提醒。實際告警以事件簿保留的評估快照為準。",
    "舊資料沒有保存的修訂版本無法還原；歷史來源描述使用目前資料字典，舊工作檢查點紀錄可能不完整。",
]


def parse_instant(value):
    """Only explicit-offset instants are accepted at availability boundaries."""
    try:
        stamp = value if isinstance(value, datetime) else datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (ValueError, TypeError, AttributeError) as exc:
        raise ValueError("時間必須是含時區的 ISO 8601，例如 2026-11-06T12:00:00+08:00") from exc
    if stamp.tzinfo is None or stamp.utcoffset() is None:
        raise ValueError("時間必須包含時區；不接受無時區或只有日期的回放截止值")
    return stamp.astimezone(timezone.utc)


def observation_availability(row):
    flags = []
    try:
        captured = parse_instant(row.get("retrieved_at"))
    except ValueError:
        return {"known_since": None, "availability_flags": ["capture_time_unknown"], "release_precision": "unknown"}
    released = row.get("release_at")
    publication = None
    precision = "unknown"
    if released:
        try:
            if isinstance(released, str) and len(released) == 10:
                publication = datetime.combine(date.fromisoformat(released), time.max, TAIPEI).astimezone(timezone.utc)
                precision = "date"
                flags.append("publication_time_unknown")
            else:
                publication = parse_instant(released)
                precision = "timestamp"
        except ValueError:
            # Invalid or ambiguous publication timestamps cannot justify replay.
            return {"known_since":None,"availability_flags":["publication_timestamp_invalid"],"release_precision":"unknown"}
    else:
        flags.append("publication_time_unknown")
    known = max(captured, publication) if publication else captured
    return {"known_since":known.isoformat(),"availability_flags":flags,"release_precision":precision}


def decode_observation(row):
    row = dict(row)
    row["status"] = row.pop("quality", row.get("status")) or "observed"
    if isinstance(row.get("metadata"), str):
        row["metadata"] = json.loads(row["metadata"] or "{}")
    row.setdefault("metadata", {})
    row.update(observation_availability(row))
    return row


def summary(store):
    # Replay controls need capture times and counts, never observation values,
    # release metadata, or decoded country/partner histories.
    with store.lock, store.connect() as db:
        version_count, observation_count, revised_count = db.execute("""SELECT
            coalesce(sum(versions),0),count(*),coalesce(sum(CASE WHEN versions>1 THEN 1 ELSE 0 END),0)
            FROM (SELECT count(*) AS versions FROM observations
                  GROUP BY source_id,series_id,observation_date)""").fetchone()
        stamps = db.execute("SELECT DISTINCT retrieved_at FROM observations WHERE retrieved_at IS NOT NULL").fetchall()
    captures = set()
    for (stamp,) in stamps:
        try:
            captures.add(parse_instant(stamp).isoformat())
        except ValueError:
            # Legacy missing/ambiguous timestamps stay in version counts but
            # cannot become trustworthy replay cutoffs.
            continue
    captures = sorted(captures, key=parse_instant)
    # Avoid an unbounded dropdown while retaining both ends of the capture range.
    shown = captures if len(captures) <= 250 else [captures[0]] + captures[-249:]
    return {"mode":"local_capture_history", "first_capture":captures[0] if captures else None,
            "latest_capture":captures[-1] if captures else None,"capture_count":len(captures),
            "captures":shown,"captures_truncated":len(shown)<len(captures),
            "observation_count":observation_count,"version_count":version_count,
            "revised_observation_count":revised_count,
            "model_history_available":False,"limitations":LIMITATIONS}


def versions(store, series_id, observation_date=None):
    if not series_id:
        raise ValueError("請指定 series_id")
    if observation_date is not None:
        date.fromisoformat(observation_date)
    rows = store.observation_versions(series_id, observation_date)
    def key(row):
        try:
            stamp = parse_instant(row["retrieved_at"])
        except ValueError:
            stamp = datetime.min.replace(tzinfo=timezone.utc)
        return row["observation_date"],row["source_id"],stamp
    rows.sort(key=key)
    previous = {}
    counts = {}
    current = {}
    output = []
    fields = ("value","unit","label","status","metadata","release_at","source_url","raw_sha256")
    for row in rows:
        group = (row["source_id"],row["observation_date"])
        prior = previous.get(group)
        counts[group] = counts.get(group, 0) + 1
        changes = {field:{"before":prior.get(field),"after":row.get(field)}
                   for field in fields if prior and prior.get(field) != row.get(field)}
        item = {**row,"revision_number":counts[group],"diff":changes,"is_current":True,
                "revision_kind":"initial_capture" if prior is None else
                    "source_payload_changed" if prior.get("raw_sha256") != row.get("raw_sha256") else "interpretation_changed"}
        if group in current:
            current[group]["is_current"] = False
        current[group] = item
        previous[group] = row
        output.append(item)
    return {"series_id":series_id,"observation_date":observation_date,"versions":output,
            "version_count":len(output),"limitations":LIMITATIONS}