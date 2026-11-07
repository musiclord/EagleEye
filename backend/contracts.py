"""Small immutable contracts shared by source adapters; fetching and storage belong to the engine."""
from dataclasses import dataclass, field
from typing import Callable, Any
from datetime import date
import hashlib, json
from .connectors import Source

# Raw bytes handed to an ``empty_ok`` job's parser when the source answered HTTP 404.
# Stored like any capture so the "no file for this day" answer keeps a hash and a TTL.
NOT_FOUND_RAW = b"EAGLEEYE-HTTP-404-NOT-FOUND\n"

@dataclass(frozen=True)
class Job:
    key: str
    source_id: str
    url: str
    parser: Callable[[bytes], list[dict]]
    parser_version: str = "1"
    refresh_after_hours: float = 24
    optional: bool = False
    headers: dict[str, str] = field(default_factory=dict)
    method: str = "GET"
    body: bytes | None = None
    max_bytes: int = 15_000_000
    followups: Callable[[bytes], list['Job']] | None = None
    # Parsing coverage is separate from the HTTP request identity and its age.
    parse_scope: str = ""
    refresh_at_local: str | None = None
    requested_end: date | None = None
    # Per-day files: HTTP 404 reaches the parser as NOT_FOUND_RAW, and a parser returning no
    # rows is a verified "no session / no file" answer (0 rows, never zeros), not a failure.
    empty_ok: bool = False

    @property
    def request_signature(self):
        payload = [self.source_id, self.method.upper(), self.url,
                   sorted((k.lower(), v) for k, v in self.headers.items()),
                   hashlib.sha256(self.body or b'').hexdigest()]
        return hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()

def make_point(series: dict, series_id: str, observed: str, value: float, **extra: Any) -> dict:
    from math import isfinite
    date.fromisoformat(observed)
    if not isfinite(float(value)):
        raise ValueError("Non-finite observation")
    label, unit = series[series_id]
    return {"series_id":series_id,"label":label,"unit":unit,
            "observation_date":observed,"value":float(value),**extra}
