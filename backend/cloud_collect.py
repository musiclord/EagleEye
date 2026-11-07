"""Bounded GitHub-runner collection using reviewed adapters and temporary Store.

No app import, scheduler, formal data directory, browser feed, deployment or Git.
An ordinary source failure produces a validated degraded candidate; corruption,
privacy, resource limits and programming failures prevent canonical replacement.
"""
from __future__ import annotations
import argparse
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from functools import partial
import hashlib
import json
import math
from pathlib import Path
import shutil
import sys
import time
import urllib.error
import urllib.request
import uuid

from .cloud_history import (CanonicalHistory, adapters, assert_safe_directory,
                            canonical, cutoff_date, instant, _validate_url)
from .contracts import Job
from .connectors import SchemaError
from .engine import Engine, DeferredSource, NetworkDisabled, _adapter_jobs, _SourceRedirectHandler
from .store import Store, utc_now

TAIPEI = timezone(timedelta(hours=8))
CADENCE_HOURS = {'twse_od_market': 2, 'twse_oa_smallcap_indices': 2,
                'fsc_monthly_ods': 24, 'frb_h41': 24, 'frb_h10': 24,
                'calendar_twse': 24,
                'twse_open_common_basic': 2, 'twse_open_common_prices': 2,
                'twse_open_common_actions': 2}
MAX_REQUESTS = 24
MAX_DEADLINE_SECONDS = 480
MAX_RESPONSE_BYTES = 16_000_000


class CloudStore(Store):
    def save(self, source, points, fetched_at, digest):
        """Batch no-op prefilter; changed rows still use the existing Store writer.

        Compare exactly the same fields as Store.save, including source identity,
        quality, release date, metadata and the source's content-revision policy.
        This avoids thousands of repeated SQL lookups for unchanged FRB archives.
        """
        from .history import parse_instant
        parse_instant(fetched_at)
        points = list(points)
        if not points:
            return super().save(source,points,fetched_at,digest)
        lower = min(p['observation_date'] for p in points)
        upper = max(p['observation_date'] for p in points)
        latest = {}
        for row in self.observations(start=lower,end=upper):
            if row['source_id'] == source.id:
                latest[(row['series_id'],row['observation_date'])] = (
                    row['raw_sha256'],row['status'],json.dumps(row['metadata'],ensure_ascii=False,sort_keys=True),
                    row['value'],row['unit'],row['label'],row['release_at'],row['source_url'])
        changed = []
        for point in points:
            date.fromisoformat(point['observation_date'])
            value = float(point['value'])
            if not math.isfinite(value):
                raise ValueError('Non-finite observation')
            key = (point['series_id'],point['observation_date'])
            interpreted = (digest,point.get('status','observed'),
                json.dumps(point.get('metadata',{}),ensure_ascii=False,sort_keys=True),
                value,point['unit'],point['label'],point.get('release_at'),point.get('source_url',source.url))
            old = latest.get(key)
            if old and (old == interpreted or
                    (source.content_revisions_only and old[1:] == interpreted[1:])):
                continue
            changed.append(point)
            latest[key] = interpreted
        if changed:
            return super().save(source,changed,fetched_at,digest)


class CollectionLimit(RuntimeError):
    pass


class BoundedEngine(Engine):
    def __init__(self, store, *, max_requests=MAX_REQUESTS, deadline_seconds=MAX_DEADLINE_SECONDS, fetcher=None):
        super().__init__(store)
        if not 1 <= max_requests <= 48 or not 1 <= deadline_seconds <= 900:
            raise ValueError('Request/deadline budget outside supported limits')
        self.max_requests = max_requests
        self.deadline = time.monotonic() + deadline_seconds
        self.requests = 0
        self.fetcher = fetcher
        self.fatal = None

    def check_budget(self, request=False):
        if time.monotonic() >= self.deadline or (request and self.requests >= self.max_requests):
            self.fatal = 'collection_budget_exceeded'
            raise CollectionLimit(self.fatal)

    def _remember_failure(self, job, state, exc, attempted, digest):
        if not isinstance(exc, (SchemaError, urllib.error.URLError, TimeoutError,
                                DeferredSource, NetworkDisabled)):
            self.fatal = self.fatal or 'unexpected_internal_failure'
        return super()._remember_failure(job, state, exc, attempted, digest)

    def _fetch(self, job):
        self.check_budget(request=True)
        if job.source_id not in CADENCE_HOURS:
            self.fatal = 'fetch_source_not_reviewed'
            raise CollectionLimit(self.fatal)
        try:
            _validate_url(job.source_id, job.url)
        except ValueError:
            self.fatal = 'request_allowlist_violation'
            raise
        if job.method not in {'GET', 'POST'} or job.max_bytes > MAX_RESPONSE_BYTES:
            self.fatal = 'invalid_request_contract'
            raise CollectionLimit(self.fatal)
        self.requests += 1
        if self.fetcher is not None:
            raw = self.fetcher(job)
            self.check_budget()
            if len(raw) > job.max_bytes:
                self.fatal = 'response_size_exceeded'
                raise CollectionLimit(self.fatal)
            return raw
        # Validate before every redirect, not after bytes reached an unknown host.
        validate = partial(_validate_url, job.source_id)
        from urllib.parse import urlsplit
        host = urlsplit(job.url).hostname
        wait = (.7 if 'twse' in host else .3) - (time.monotonic() - self.last_host.get(host, 0))
        if wait > 0:
            time.sleep(min(wait, max(0, self.deadline - time.monotonic())))
        self.check_budget()
        self.last_host[host] = time.monotonic()
        request = urllib.request.Request(job.url, data=job.body, method=job.method,
            headers={'User-Agent': 'EagleEye/0.10 public official statistical research',
                     'Accept': 'application/json,text/csv,text/html,*/*', **job.headers})
        timeout = max(.1, min(20, self.deadline - time.monotonic()))
        def validate_redirect(url):
            validate(url)
            self.check_budget(request=True)
            self.requests += 1
        opener = urllib.request.build_opener(_SourceRedirectHandler(validate_redirect))
        with opener.open(request, timeout=timeout) as response:
            validate(response.url)
            length = response.headers.get('Content-Length')
            if length and int(length) > job.max_bytes:
                self.fatal = 'response_size_exceeded'
                raise CollectionLimit(self.fatal)
            parts, size = [], 0
            while True:
                self.check_budget()
                block = response.read1(min(64 * 1024, job.max_bytes + 1 - size))
                if not block:
                    break
                parts.append(block)
                size += len(block)
                if size > job.max_bytes:
                    self.fatal = 'response_size_exceeded'
                    raise CollectionLimit(self.fatal)
            return b''.join(parts)


def due(source_id, states, now):
    old = states.get(source_id, {})
    if not old.get('last_attempt'):
        return True
    retry = old.get('recovery', {}).get('next_retry_at')
    if old.get('status') != 'ok' and retry and now < instant(retry):
        return False
    elapsed = (now - instant(old['last_attempt'])).total_seconds()
    hours = CADENCE_HOURS[source_id] if old.get('status') == 'ok' else .25
    if hours == 24:
        return now.astimezone(TAIPEI).date() > instant(old['last_attempt']).astimezone(TAIPEI).date()
    return elapsed >= hours * 3600


def instrument_job(job):
    # Store's legacy table lacks a parser-version column. Stamp the actual job
    # used for this parse; historical rows without this evidence remain unknown.
    def parse(raw):
        return [{**p, 'metadata': {**p.get('metadata', {}), 'parser_version': job.parser_version}}
                for p in job.parser(raw)]
    followups = (lambda raw: [instrument_job(child) for child in job.followups(raw)]) if job.followups else None
    return replace(job, parser=parse, followups=followups)


def plan_jobs(store, history_state, end, now):
    """Latest-only TWSE; rolling FSC 12/24 months; one bounded FRB archive each."""
    jobs, skipped = [], []
    for module in adapters():
        if module.__name__.endswith('ex_tsmc_open'):
            selected = list(module.SOURCES) if any(due(s.id, history_state['sources'], now) for s in module.SOURCES) else []
        else:
            selected = [s for s in module.SOURCES if due(s.id, history_state['sources'], now)]
        if not selected:
            skipped.extend(s.id for s in module.SOURCES)
            continue
        selected_ids = {s.id for s in selected}
        planned = _adapter_jobs(module, store, end - timedelta(days=14), end, mode='incremental')
        if module.__name__.endswith('us_federalreserve'):
            # Endpoint is a complete archive; parse/store only the public window.
            lower = cutoff_date(end)
            planned = [replace(j, parser=partial(module.parse_release, release=j.source_id[-3:],
                start=lower, end=end), parse_scope=f'{lower}:{end}') for j in planned]
        jobs.extend(instrument_job(j) for j in planned if j.source_id in selected_ids)
        skipped.extend(s.id for s in module.SOURCES if s.id not in selected_ids)
        if module.__name__.endswith('twse_open_data_market') and not planned:
            raise ValueError('Reviewed TWSE CSV disabled by environment; explicit collection configuration required')
    return jobs, skipped


def _new_state(end):
    return {'id': str(uuid.uuid4()), 'mode': 'incremental', 'start': (end-timedelta(days=14)).isoformat(),
            'end': end.isoformat(), 'status': 'running', 'started_at': utc_now(),
            'total': 0, 'done': 0, 'downloaded': 0, 'cached': 0, 'failed': 0,
            'rows': 0, 'errors': [], 'current': ''}


def _restore_source(store, source_id, previous):
    with store.lock, store.connect() as db:
        db.execute('DELETE FROM observations WHERE source_id=?', [source_id])
        rows = [r for r in previous if r['source_id'] == source_id]
        if rows:
            db.executemany('INSERT INTO observations VALUES (?,?,?,?,?,?,?,?,?,?,?,?)',
                [[r[k] if k != 'metadata' else canonical(r[k]).decode() for k in
                  ('source_id','series_id','label','unit','observation_date','value','retrieved_at',
                   'source_url','raw_sha256','release_at','quality','metadata')] for r in rows])


def _reject_source_group(store, source_id, previous, since):
    _restore_source(store, source_id, previous)
    with store.lock, store.connect() as db:
        db.execute("DELETE FROM ingestion_runs WHERE source_id=? AND status='ok' AND CAST(attempted_at AS TIMESTAMPTZ)>=CAST(? AS TIMESTAMPTZ)", [source_id,since])
        db.execute("UPDATE fetch_checkpoints SET status='error',error='source_group_failed' WHERE source_id=? AND CAST(fetched_at AS TIMESTAMPTZ)>=CAST(? AS TIMESTAMPTZ)", [source_id,since])
    store.record_run(source_id, utc_now(), 'error', error='source_group_failed')


def _calendar(store, engine, year):
    from .publication_calendar import TWSE_URL, parse_twse, ensure_schema
    ensure_schema(store)
    job = Job(f'calendar:twse:{year}', 'calendar_twse', TWSE_URL,
              partial(parse_twse, year=year), max_bytes=1_000_000)
    # Same finite transport retry as Engine; 429/403 never retried immediately.
    for attempt in range(2):
        try:
            raw = engine._fetch(job)
            break
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            if attempt or getattr(exc, 'code', None) in {400,401,403,404,429}:
                raise
            engine.check_budget()
            time.sleep(1)
    payload = parse_twse(raw, year)
    digest, stamp = store.cache_raw(raw), utc_now()
    with store.lock, store.connect() as db:
        previous = db.execute('SELECT raw_sha256 FROM publication_calendars WHERE calendar_key=? ORDER BY CAST(retrieved_at AS TIMESTAMPTZ) DESC LIMIT 1', [job.key]).fetchone()
        if not previous or previous[0] != digest:
            db.execute('INSERT INTO publication_calendars VALUES (?,?,?,?,?)',
                [job.key,stamp,digest,job.url,canonical(payload).decode()])
    store.record_run('calendar_twse', stamp, 'ok', digest=digest)


def collect_history(history_dir, scratch_dir, *, end=None, max_requests=MAX_REQUESTS,
                    deadline_seconds=MAX_DEADLINE_SECONDS, fetcher=None, now=None):
    history = CanonicalHistory(history_dir)
    history.validate()
    scratch = assert_safe_directory(scratch_dir)
    if scratch == history.directory or scratch in history.directory.parents or history.directory in scratch.parents:
        raise ValueError('Scratch and canonical directories must not overlap')
    if scratch.exists() and any(scratch.iterdir()):
        raise ValueError('Scratch directory must be new or empty')
    now = now or datetime.now(timezone.utc)
    if now.utcoffset() is None:
        raise ValueError('now requires timezone')
    end = end or now.astimezone(TAIPEI).date()
    if end > now.astimezone(TAIPEI).date():
        raise ValueError('No future collection date')
    scratch.mkdir(parents=True, exist_ok=True)
    store = CloudStore(scratch / 'data')
    history.hydrate(store)
    previous = list(history.rows())
    engine = BoundedEngine(store, max_requests=max_requests, deadline_seconds=deadline_seconds, fetcher=fetcher)
    state = _new_state(end)
    errors = []
    skipped = []
    try:
        jobs, skipped = plan_jobs(store, history.state(), end, now)
        # Do not invoke Engine._run: it plans the entire local registry.
        engine._batch(jobs, state, {})
        engine.check_budget()
        with store.lock, store.connect() as db:
            failed_ids = {r[0] for r in db.execute("SELECT DISTINCT source_id FROM ingestion_runs WHERE status='error' AND CAST(attempted_at AS TIMESTAMPTZ)>=CAST(? AS TIMESTAMPTZ)", [state['started_at']]).fetchall()}
        from . import ex_tsmc_open
        model_ids = set(ex_tsmc_open.SOURCE_IDS.values())
        if failed_ids & model_ids:
            failed_ids.update(model_ids)
        for sid in failed_ids:
            # Source listings/attachments and the model's three resources are
            # atomic groups; a partial success cannot advance last_success.
            _reject_source_group(store, sid, previous, state['started_at'])
        errors.extend(state['errors'])
        if due('calendar_twse', history.state()['sources'], now):
            try:
                _calendar(store, engine, end.year)
            except CollectionLimit:
                raise
            except (SchemaError, urllib.error.URLError, TimeoutError) as exc:
                errors.append({'key': 'calendar_twse', 'error': type(exc).__name__ + ': ' + str(exc)[:300]})
                from .publication_calendar import TWSE_URL
                job = Job(f'calendar:twse:{end.year}', 'calendar_twse', TWSE_URL, lambda raw: [])
                engine._remember_failure(job,state,exc,True,None)
                store.record_run('calendar_twse', utc_now(), 'error', error='source_update_failed')
        else:
            skipped.append('calendar_twse')
        from . import ex_tsmc_open
        model_ids = set(ex_tsmc_open.SOURCE_IDS.values())
        model_requested = any(j.source_id in model_ids for j in jobs)
        if model_requested and not (failed_ids & model_ids):
            try:
                # Validate alignment before derive can append to the immutable ledger.
                raw, stamps = ex_tsmc_open._current_captures(store)
                ex_tsmc_open.collect(raw, stamps)
            except SchemaError as exc:
                errors.append({'key': 'ex_tsmc_open:alignment', 'error': type(exc).__name__ + ': ' + str(exc)[:300]})
                for sid in model_ids:
                    _reject_source_group(store,sid,previous,state['started_at'])
                store.record_run(ex_tsmc_open.DERIVED_SOURCE.id, utc_now(), 'error', error='source_update_failed')
            else:
                # An error after validated input reaches model persistence is
                # internal/fatal; never publish a partially appended ledger.
                for source, points in ex_tsmc_open.derive(store):
                    digest = hashlib.sha256(canonical({'model': ex_tsmc_open.MODEL_VERSION, 'points': points})).hexdigest()
                    store.save(source, points, utc_now(), digest)
                    store.record_run(source.id, utc_now(), 'ok', len(points), digest=digest)
        elif model_requested:
            store.record_run(ex_tsmc_open.DERIVED_SOURCE.id, utc_now(), 'error', error='source_update_failed')
        engine.check_budget()
        if engine.fatal:
            raise CollectionLimit(engine.fatal)
        # Assemble/validate separately. The designated canonical tree only receives
        # a complete candidate; failed individual sources retain their prior rows.
        candidate = CanonicalHistory(scratch / 'candidate-history')
        if history.directory.exists():
            shutil.copytree(history.directory, candidate.directory)
        result = candidate.capture(store, as_of=end)
        candidate.validate()
        old = history._files()
        new = candidate._files()
        history.directory.mkdir(parents=True, exist_ok=True)
        for name, raw in new.items():
            target = history.directory / name
            target.parent.mkdir(parents=True, exist_ok=True)
            if old.get(name) != raw:
                temp = target.with_suffix(target.suffix + '.pending')
                temp.write_bytes(raw)
                temp.replace(target)
        for name in set(old) - set(new):
            (history.directory / name).unlink()
        source_states = candidate.state()['sources']
        unhealthy = [sid for sid, info in source_states.items()
                     if info.get('collection_policy') != 'seed_only_not_scheduled'
                     and info['status'] in {'error','waiting'}]
        report = {**result, 'status': 'degraded' if errors or unhealthy else 'completed',
                  'unhealthy_sources': sorted(unhealthy),
                  'requests': engine.requests, 'skipped_sources': sorted(skipped),
                  'sources': source_states, 'errors': errors,
                  'raw_retained': False}
    except Exception as exc:
        report = {'status': 'failed', 'changed': False, 'observations_changed': False,
                  'requests': engine.requests, 'skipped_sources': sorted(skipped),
                  'errors': errors + [{'key': 'collector', 'error': type(exc).__name__ + ': ' + str(exc)[:300]}]}
    finally:
        engine.close()
    (scratch / 'collection-report.json').write_bytes(canonical(report) + b'\n')
    return report


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--history-dir', required=True)
    parser.add_argument('--scratch-dir', required=True)
    parser.add_argument('--end', type=date.fromisoformat)
    parser.add_argument('--max-requests', type=int, default=MAX_REQUESTS)
    parser.add_argument('--deadline-seconds', type=int, default=MAX_DEADLINE_SECONDS)
    parser.add_argument('--report')
    args = parser.parse_args(argv)
    try:
        report = collect_history(args.history_dir, args.scratch_dir, end=args.end,
            max_requests=args.max_requests, deadline_seconds=args.deadline_seconds)
    except ValueError as exc:
        print(json.dumps({'status':'rejected', 'error':str(exc)}, ensure_ascii=False))
        return 2
    if args.report:
        target = assert_safe_directory(args.report)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(canonical(report) + b'\n')
    public = {k:v for k,v in report.items() if k != 'errors'}
    public['error_count'] = len(report.get('errors', []))
    public['error_keys'] = [x.get('key') for x in report.get('errors', [])]
    print(json.dumps(public, ensure_ascii=False, indent=2))
    return 0 if report['status'] in {'completed','degraded'} else 1


if __name__ == '__main__':
    sys.exit(main())

