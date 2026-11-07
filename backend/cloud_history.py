"""Compact public canonical history. No application import and no permanent database.

Only reviewed scalar adapters, TWSE holiday facts and the existing model's
recalculation ledger are admitted. Raw HTTP bodies never belong to this tree.
"""
from __future__ import annotations
from collections import defaultdict
from datetime import date, datetime, timezone
import hashlib
import importlib
from functools import lru_cache
import json
import math
import os
from pathlib import Path
import re
import shutil

MODULE_NAMES = ('twse_open_data_market', 'twse_openapi_indices', 'fsc_monthly_reports',
                'us_federalreserve', 'ex_tsmc_open')
SEED_ONLY_SOURCE_IDS = frozenset(('boj_overnight','boj_usd_jpy','cbc_fx','cbc_overnight_rate',
    'cbc_policy_rate','dgbas_cpi','dgbas_regular_wage','fsc_foreign_holdings','macro_derived',
    'moea_demand','mof_exports','mof_imports','sfb_remittances','taifex_taiex_weights',
    'taifex_tx_institutions','taifex_tx_market','taifex_tx_ratios','taiwan_macro_derived',
    'tpex_breadth','tpex_institutions','tpex_margin','tpex_market','us_treasury_yields'))
SCHEMA_VERSION = 1
MAX_SCALAR_BYTES = 128 * 1024 * 1024
MAX_MODEL_BYTES = 512 * 1024 * 1024
MAX_FILE_BYTES = 16 * 1024 * 1024
ID = re.compile(r'^[a-zA-Z0-9_-]+$')
HASH = re.compile(r'^[a-f0-9]{64}$')
OBS_KEYS = ('source_id', 'series_id', 'label', 'unit', 'observation_date', 'value',
            'retrieved_at', 'source_url', 'raw_sha256', 'release_at', 'quality',
            'metadata', 'parser_version', 'model_version')
CP_KEYS = ('job_key', 'source_id', 'url', 'parser_version', 'fetched_at', 'raw_sha256',
           'status', 'rows', 'error', 'request_signature', 'parse_scope')
BANNED_KEYS = {'address', 'phone', 'email', 'password', 'token', 'secret', 'raw_body',
               'html', 'raw_payload', 'local_path', 'data_directory', '電話', '住址', '地址',
               '電子郵件信箱', '公司住址', '營利事業統一編號',
               'note_text','notes','user_note','acknowledged_at','acknowledged_by',
               'project_root','database_path','raw_path','path','access_token','api_key',
               'process_id','instance_id','record_id','record_ids','read_at','authorization'}


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'),
                      allow_nan=False).encode('utf-8')


def adapters():
    return [importlib.import_module('.' + name, __package__) for name in MODULE_NAMES]


@lru_cache(maxsize=1)
def allowed_sources():
    result = {}
    for module in adapters():
        for source in module.SOURCES:
            if not source.publish:
                raise ValueError('Collector source is no longer public: ' + source.id)
            result[source.id] = source
        if hasattr(module, 'DERIVED_SOURCE'):
            if not module.DERIVED_SOURCE.publish:
                raise ValueError('Model source is no longer public')
            result[module.DERIVED_SOURCE.id] = module.DERIVED_SOURCE
    from .registry import source_map
    registry = source_map()
    for sid in SEED_ONLY_SOURCE_IDS:
        if sid not in registry or not registry[sid].publish:
            raise ValueError('Seed-only source is no longer public: ' + sid)
        result[sid] = registry[sid]
    return result


def public_source_ids():
    return frozenset(allowed_sources())


@lru_cache(maxsize=1)
def _versions():
    result = {}
    for module in adapters():
        parser = getattr(module, 'PARSER_VERSION', getattr(module, 'MODEL_VERSION', '1'))
        for source in module.SOURCES:
            result[source.id] = (parser, None)
        if hasattr(module, 'DERIVED_SOURCE'):
            result[module.DERIVED_SOURCE.id] = (parser, module.MODEL_VERSION)
    result.update((sid, ('legacy-parser-not-recorded', None)) for sid in SEED_ONLY_SOURCE_IDS)
    return result


def instant(value):
    stamp = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if stamp.utcoffset() is None:
        raise ValueError('canonical timestamps require timezone')
    return stamp.astimezone(timezone.utc)


def cutoff_date(reference):
    try:
        return reference.replace(year=reference.year - 10)
    except ValueError:
        return reference.replace(year=reference.year - 10, day=28)


def assert_safe_directory(directory):
    path = Path(directory).expanduser().resolve()
    normalized = path.as_posix().lower()
    if '/appdata/local/eagleeye/data' in normalized or normalized.endswith('/backend/data'):
        raise ValueError('Production or legacy data directory is forbidden')
    local = os.environ.get('LOCALAPPDATA')
    production = Path(local, 'EagleEye', 'data').resolve() if local else None
    if production and (path == production or production in path.parents or path in production.parents):
        raise ValueError('Production data directory is forbidden')
    return path


def _privacy(value):
    if isinstance(value, dict):
        for key, item in value.items():
            if str(key).lower() in BANNED_KEYS:
                raise ValueError('Non-public metadata field rejected: ' + str(key))
            _privacy(item)
    elif isinstance(value, list):
        for item in value:
            _privacy(item)
    elif isinstance(value, str):
        if re.search(r'(?<![A-Za-z])[A-Za-z]:[\\/]|file://|AppData[\\/]|/Users/|/home/|/tmp/|<html\b|<!DOCTYPE', value, re.I):
            raise ValueError('Private path or original document rejected')
        if re.search(r'gh[pousr]_[A-Za-z0-9]{20,}|github_pat_[A-Za-z0-9_]{20,}|sk-[A-Za-z0-9_-]{20,}|Bearer\s+[A-Za-z0-9._-]{12,}', value):
            raise ValueError('Credential pattern rejected')
        if value.startswith(('https://','http://')):
            from urllib.parse import urlsplit, parse_qs
            url = urlsplit(value)
            if url.username or url.password or any(k.lower() in
                {'token','access_token','api_key','apikey','password','secret','authorization'} for k in parse_qs(url.query)):
                raise ValueError('Credential URL rejected')


def _validate_url(source_id, value):
    from . import fsc_monthly_reports
    sources = allowed_sources()
    if source_id == 'fsc_monthly_ods':
        fsc_monthly_reports.validate_fetch_url(value)
    elif source_id == 'calendar_twse':
        from .publication_calendar import TWSE_URL
        if value != TWSE_URL:
            raise ValueError('Unapproved calendar URL')
    elif source_id in SEED_ONLY_SOURCE_IDS:
        from urllib.parse import urlsplit
        url = urlsplit(value)
        expected = urlsplit(sources[source_id].url)
        if url.scheme != 'https' or url.hostname != expected.hostname or url.username or url.password or url.port not in (None,443):
            raise ValueError('Unapproved seed-only source URL')
    elif source_id not in sources or value != sources[source_id].url:
        raise ValueError('Unapproved source URL')


def _signature(row):
    # Hash evidence can change when the same archive adds a different date.
    # The first proof stays on each revision; current proof stays in checkpoints.
    value = {k: row.get(k) for k in OBS_KEYS if k not in {'retrieved_at','raw_sha256'}}
    # Discovery pagination, request interval and attachment/container hashes are
    # capture evidence, not a revision to an unchanged monthly financial fact.
    value['metadata'] = {k:v for k,v in (row.get('metadata') or {}).items()
        if k not in {'provenance_chain','archive_sha256','collection_scope','parser_version'}}
    return canonical(value)


def _row(row):
    versions = _versions()
    if row.get('source_id') not in versions or not ID.fullmatch(row.get('series_id', '')):
        raise ValueError('Unknown public source or unsafe series id')
    parser, model = versions[row['source_id']]
    item = {k: row.get(k) for k in OBS_KEYS}
    item['metadata'] = row.get('metadata') or {}
    item['quality'] = row.get('quality', row.get('status', 'observed'))
    item['parser_version'] = row.get('parser_version') or item['metadata'].get('parser_version') or 'legacy-parser-not-recorded'
    item['model_version'] = row.get('model_version') or item['metadata'].get('method_version') or item['metadata'].get('model_version')
    date.fromisoformat(item['observation_date'])
    instant(item['retrieved_at'])
    if not isinstance(item['value'], (int, float)) or isinstance(item['value'], bool) or not math.isfinite(item['value']):
        raise ValueError('Invalid observation value')
    if item['quality'] not in {'observed', 'derived', 'estimated'}:
        raise ValueError('Only real observed/derived/estimated data is public')
    if not HASH.fullmatch(item.get('raw_sha256') or ''):
        raise ValueError('Observation requires its actual evidence SHA256')
    _validate_url(item['source_id'], item['source_url'])
    _privacy(item)
    canonical(item)
    return item


def _calendar(row):
    from .publication_calendar import TWSE_URL
    if row['source_url'] != TWSE_URL or not re.fullmatch(r'calendar:twse:\d{4}', row['calendar_key']):
        raise ValueError('Only TWSE annual calendar facts are permitted')
    payload = row['payload']
    year = int(row['calendar_key'][-4:])
    if payload.get('kind') != 'market_session' or payload.get('market') != 'twse':
        raise ValueError('Wrong calendar kind')
    if payload.get('coverage_start') != f'{year}-01-01' or payload.get('coverage_end') != f'{year}-12-31':
        raise ValueError('Incomplete calendar coverage')
    events = []
    for event in payload['events']:
        d = date.fromisoformat(event['date'])
        if d.year != year or event['status'] not in {'open', 'closed'}:
            raise ValueError('Invalid calendar date/status')
        events.append({k: event[k] for k in ('date', 'kind', 'market', 'status', 'basis')})
    if len({e['date'] for e in events}) != len(events):
        raise ValueError('Duplicate calendar facts')
    if len(events) != (date(year+1,1,1)-date(year,1,1)).days:
        raise ValueError('Incomplete annual calendar facts')
    instant(row['retrieved_at'])
    if not HASH.fullmatch(row['raw_sha256']):
        raise ValueError('Calendar evidence hash missing')
    return {**{k: row[k] for k in ('calendar_key', 'retrieved_at', 'raw_sha256', 'source_url')},
            'payload': {'kind': 'market_session', 'market': 'twse',
                        'coverage_start': f'{year}-01-01', 'coverage_end': f'{year}-12-31',
                        'parser_version': payload.get('parser_version', 'exchange-calendar-1'),
                        'emergency_closures_loaded': False, 'events': sorted(events, key=lambda e: e['date'])}}


def _ledger_path(name):
    return (name in {'ex-tsmc-open/universe.json', 'ex-tsmc-open/verified-events.json'} or
            re.fullmatch(r'ex-tsmc-open/objects/[a-f0-9]{64}\.json', name) or
            re.fullmatch(r'ex-tsmc-open/snapshots/\d{4}-\d{2}/\d{4}-\d{2}-\d{2}-[a-f0-9]{64}(?:-[a-f0-9]{16})?\.json', name))


class CanonicalHistory:
    def __init__(self, directory):
        self.directory = assert_safe_directory(directory)

    def _files(self):
        if not self.directory.exists():
            return {}
        output = {}
        for path in sorted(self.directory.rglob('*')):
            if path.is_symlink():
                raise ValueError('Symlinks are forbidden in canonical history')
            if not path.is_file():
                continue
            name = path.relative_to(self.directory).as_posix()
            if not (name == 'state.json' or _ledger_path(name) or
                    re.fullmatch(r'observations/[a-zA-Z0-9_-]+/[a-zA-Z0-9_-]+/\d{4}\.jsonl', name)):
                raise ValueError('Unexpected canonical history file: ' + name)
            if path.stat().st_size > MAX_FILE_BYTES:
                raise ValueError('Canonical file exceeds budget')
            output[name] = path.read_bytes()
        return output

    def state(self):
        path = self.directory / 'state.json'
        return json.loads(path.read_bytes()) if path.exists() else {
            'schema_version': SCHEMA_VERSION, 'sources': {}, 'checkpoints': [],
            'settings': {}, 'calendars': [], 'public_years': 10,
            'raw_retained': False, 'model_retention': 'retain_all_recalculation_dependencies'}

    def rows(self):
        for path in sorted((self.directory / 'observations').glob('*/*/*.jsonl')):
            for line in path.read_text(encoding='utf-8').splitlines():
                if line:
                    yield _row(json.loads(line))

    def validate(self):
        files = self._files()
        scalar = sum(len(raw) for name, raw in files.items() if not name.startswith('ex-tsmc-open/'))
        model = sum(len(raw) for name, raw in files.items() if name.startswith('ex-tsmc-open/'))
        if scalar > MAX_SCALAR_BYTES or model > MAX_MODEL_BYTES:
            raise ValueError('Canonical capacity exceeded; dependencies were not pruned')
        state = self.state()
        if state.get('schema_version') != SCHEMA_VERSION or state.get('raw_retained') is not False:
            raise ValueError('Unsupported canonical schema')
        if set(state.get('settings', {})) - {'ex_tsmc_open_view'}:
            raise ValueError('Private application settings rejected')
        _privacy(state)
        sources = allowed_sources()
        for sid, value in state['sources'].items():
            if sid not in sources and sid != 'calendar_twse':
                raise ValueError('Unknown source state')
            for key in ('last_attempt', 'last_success'):
                if value.get(key):
                    instant(value[key])
            if value.get('status') not in {'ok', 'error', 'waiting'}:
                raise ValueError('Unknown source status')
        for cp in state['checkpoints']:
            if set(cp) - set(CP_KEYS) or cp['source_id'] not in sources:
                raise ValueError('Invalid checkpoint')
            _validate_url(cp['source_id'], cp['url'])
            instant(cp['fetched_at'])
            if cp.get('raw_sha256') and not HASH.fullmatch(cp['raw_sha256']):
                raise ValueError('Invalid checkpoint hash')
        for item in state.get('calendars', []):
            _calendar(item)
        keys = set()
        count = 0
        for name, raw in files.items():
            if not name.startswith('observations/'):
                continue
            _, sid, series, yearfile = name.split('/')
            previous = None
            for line in raw.splitlines():
                row = _row(json.loads(line))
                if row['source_id'] != sid or row['series_id'] != series or row['observation_date'][:4] + '.jsonl' != yearfile:
                    raise ValueError('Canonical partition mismatch')
                key = (sid, series, row['observation_date'], instant(row['retrieved_at']))
                if key in keys:
                    raise ValueError('Duplicate observation revision')
                keys.add(key)
                sortkey = (row['observation_date'], instant(row['retrieved_at']))
                if previous and sortkey < previous:
                    raise ValueError('Unsorted canonical observations')
                previous = sortkey
                count += 1
        if model:
            from .ex_tsmc_open import CompactLedger, canonical as ledger_canonical
            # Validate content-addressed dependency objects and private-field exclusion.
            for name, raw in files.items():
                if name.startswith('ex-tsmc-open/'):
                    obj = json.loads(raw)
                    _privacy(obj)
                    if '/objects/' in name:
                        if hashlib.sha256(ledger_canonical(obj)).hexdigest() != Path(name).stem:
                            raise ValueError('Model object hash mismatch')
                        share_fields = {'code','shares','private_shares','listing_date'}
                        event_fields = {'Code','Date','Exdividend','StockDividendRatio','SubscriptionRatio',
                            'SubscriptionPricePerShare','CashDividend','SharesOffered','SharesEmpOwner',
                            'SharesholderOwner','StockHoldingRatio','event_date','event_id','confirmed'}
                        if not isinstance(obj, list) or any(not isinstance(x,dict) or not
                            (set(x) <= share_fields or set(x) <= event_fields) for x in obj):
                            raise ValueError('Unexpected model object fields')
                    if '/snapshots/' in name:
                        required = {'method_version','data_date','price_date','shares_report_date',
                            'shares_effective_date','retrieved_at','known_at','excluded','provenance',
                            'original_raw_retained_by_ledger','snapshot_id','shares_object','actions_object','prices'}
                        if set(obj) != required or obj['original_raw_retained_by_ledger'] is not False:
                            raise ValueError('Unexpected compact snapshot fields')
                        date.fromisoformat(obj['data_date'])
                        instant(obj['known_at'])
                        if not HASH.fullmatch(obj['snapshot_id']) or not Path(name).name.startswith(obj['data_date']+'-'+obj['snapshot_id']):
                            raise ValueError('Snapshot identity mismatch')
                        for field in ('shares_object','actions_object'):
                            ref = obj[field]
                            if not isinstance(ref,str) or not HASH.fullmatch(ref) or 'ex-tsmc-open/objects/'+ref+'.json' not in files:
                                raise ValueError('Missing or unsafe model dependency reference')
                        if any(set(p) != {'code','close','change','missing_reason'} for p in obj['prices']):
                            raise ValueError('Unexpected model price fields')
            CompactLedger(self.directory / 'ex-tsmc-open').snapshots()  # validates reconstruction
            if 'ex-tsmc-open/universe.json' not in files:
                raise ValueError('Model fixed-universe seed missing')
        return {'schema_version': SCHEMA_VERSION, 'observation_versions': count,
                'scalar_bytes': scalar, 'model_bytes': model, 'files': len(files),
                'capacity_warning': scalar > MAX_SCALAR_BYTES * .8 or model > MAX_MODEL_BYTES * .8}

    def hydrate(self, store):
        assert_safe_directory(store.directory)
        report = self.validate()
        with store.lock, store.connect() as db:
            if db.execute('SELECT count(*) FROM observations').fetchone()[0]:
                raise ValueError('Hydration requires an empty scratch Store')
            rows = list(self.rows())
            db.execute('CREATE TABLE IF NOT EXISTS cloud_observation_versions (source_id VARCHAR, series_id VARCHAR, observation_date VARCHAR, retrieved_at VARCHAR, parser_version VARCHAR, model_version VARCHAR)')
            if rows:
                db.executemany('INSERT INTO cloud_observation_versions VALUES (?,?,?,?,?,?)',
                    [[r[k] for k in ('source_id','series_id','observation_date','retrieved_at','parser_version','model_version')] for r in rows])
            if rows:
                db.executemany('INSERT INTO observations VALUES (?,?,?,?,?,?,?,?,?,?,?,?)',
                    [[r[k] if k != 'metadata' else canonical(r[k]).decode() for k in
                      ('source_id', 'series_id', 'label', 'unit', 'observation_date', 'value',
                       'retrieved_at', 'source_url', 'raw_sha256', 'release_at', 'quality', 'metadata')] for r in rows])
            state = self.state()
            for cp in state['checkpoints']:
                db.execute('INSERT INTO fetch_checkpoints VALUES (?,?,?,?,?,?,?,?,?,?,?)', [cp.get(k) for k in CP_KEYS])
            for sid, info in state['sources'].items():
                # Keep independent same-source success and latest attempt.
                if info.get('last_success'):
                    store.record_run(sid, info['last_success'], 'ok')
                if info.get('last_attempt') and (info['status'] != 'ok' or info['last_attempt'] != info.get('last_success')):
                    store.record_run(sid, info['last_attempt'], info['status'], error=info.get('error'))
            for key, value in state['settings'].items():
                db.execute('INSERT OR REPLACE INTO settings VALUES (?,?)', [key, canonical(value).decode()])
        from .engine import ensure_recovery_schema
        ensure_recovery_schema(store)
        with store.lock, store.connect() as db:
            for sid, info in state['sources'].items():
                recovery = info.get('recovery')
                if recovery:
                    db.execute('INSERT OR REPLACE INTO recovery_sources VALUES (?,?,?)',
                        [sid, info['last_attempt'], canonical(recovery).decode()])
        from .publication_calendar import ensure_schema
        ensure_schema(store)
        with store.lock, store.connect() as db:
            for item in state.get('calendars', []):
                payload = item['payload']
                # Restore presentation-only generated labels from date facts.
                payload = {**payload, 'events': [{**e, 'id': 'twse:' + e['date'],
                    'label': 'TWSE 休市' if e['status'] == 'closed' else 'TWSE 預定交易日',
                    'source_ids': [], 'scheduled_at': None, 'note': '年度規則；未含臨時停市'} for e in payload['events']]}
                db.execute('INSERT INTO publication_calendars VALUES (?,?,?,?,?)',
                    [item['calendar_key'], item['retrieved_at'], item['raw_sha256'], item['source_url'], canonical(payload).decode()])
        ledger = self.directory / 'ex-tsmc-open'
        if ledger.exists():
            target = store.directory / 'ex-tsmc-open'
            if target.exists():
                raise ValueError('Hydration target model ledger exists')
            shutil.copytree(ledger, target)
        return report

    def capture(self, store, as_of=None):
        """Merge versions without renaming source or changing original capture time.

        Non-allowlisted rows/settings are excluded. Callers supplying a large local
        Store must make their own authorized isolated copy before calling this API.
        """
        assert_safe_directory(store.directory)
        self.validate()
        oldfiles = self._files()
        reference = as_of or date.today()
        lower = cutoff_date(reference).isoformat()
        groups = defaultdict(list)
        for row in self.rows():
            if lower <= row['observation_date'] <= reference.isoformat():
                groups[(row['source_id'], row['series_id'], row['observation_date'])].append(row)
        allow = allowed_sources()
        persisted_versions = {}
        with store.lock, store.connect() as db:
            if db.execute("SELECT 1 FROM information_schema.tables WHERE table_name='cloud_observation_versions'").fetchone():
                persisted_versions = {tuple(r[:4]):r[4:] for r in db.execute('SELECT * FROM cloud_observation_versions').fetchall()}
        for row in store.observation_versions():
            if row['source_id'] in allow and lower <= row['observation_date'] <= reference.isoformat():
                version = persisted_versions.get(tuple(row[k] for k in ('source_id','series_id','observation_date','retrieved_at')))
                if version:
                    row = {**row, 'parser_version':version[0], 'model_version':version[1]}
                item = _row(row)
                groups[(item['source_id'], item['series_id'], item['observation_date'])].append(item)
        partitions = defaultdict(list)
        for key, versions in groups.items():
            seen, previous = {}, None
            for row in sorted(versions, key=lambda r: instant(r['retrieved_at'])):
                stamp = instant(row['retrieved_at'])
                signature = _signature(row)
                if stamp in seen:
                    if signature != seen[stamp]:
                        raise ValueError('Conflicting revision at the same timestamp')
                    continue
                seen[stamp] = signature
                if previous == signature:
                    continue
                previous = signature
                partitions[f'observations/{key[0]}/{key[1]}/{key[2][:4]}.jsonl'].append(row)
        files = {}
        for name, rows in partitions.items():
            files[name] = b''.join(canonical(r) + b'\n' for r in sorted(rows,
                key=lambda r: (r['observation_date'], instant(r['retrieved_at']))))
        state = self.state()
        state['window_start'] = lower
        with store.lock, store.connect() as db:
            cps = db.execute('SELECT ' + ','.join(CP_KEYS) + ' FROM fetch_checkpoints ORDER BY job_key').fetchall()
            state['checkpoints'] = [{k: (None if k == 'error' else value) for k, value in zip(CP_KEYS, cp)}
                                   for cp in cps if cp[1] in allow]
            # Retain a bounded set of current fetch identities, never raw contents.
            if len(state['checkpoints']) > 128:
                state['checkpoints'] = sorted(state['checkpoints'], key=lambda c: instant(c['fetched_at']), reverse=True)[:128]
            for sid in [*allow, 'calendar_twse']:
                latest = db.execute('SELECT attempted_at,status FROM ingestion_runs WHERE source_id=? ORDER BY CAST(attempted_at AS TIMESTAMPTZ) DESC,rowid DESC LIMIT 1', [sid]).fetchone()
                success_row = db.execute("SELECT attempted_at FROM ingestion_runs WHERE source_id=? AND status='ok' ORDER BY CAST(attempted_at AS TIMESTAMPTZ) DESC LIMIT 1", [sid]).fetchone()
                success = success_row[0] if success_row else None
                dates = db.execute('SELECT max(observation_date) FROM observations WHERE source_id=?', [sid]).fetchone()[0]
                if latest:
                    state['sources'][sid] = {'last_attempt': latest[0], 'status': latest[1],
                        'last_success': success, 'last_observation': dates,
                        'error': 'source_update_failed' if latest[1] == 'error' else None,
                        'collection_policy': 'seed_only_not_scheduled' if sid in SEED_ONLY_SOURCE_IDS else 'scheduled_reviewed_source'}
            if db.execute("SELECT 1 FROM information_schema.tables WHERE table_name='recovery_sources'").fetchone():
                for sid, payload in db.execute('SELECT source_id,payload_json FROM recovery_sources').fetchall():
                    if sid in state['sources'] and state['sources'][sid]['status'] != 'ok':
                        policy = json.loads(payload)
                        state['sources'][sid]['recovery'] = {k: policy.get(k) for k in
                            ('source_id','category','retryable','next_retry_at','attempts','parser_version')}
                        state['sources'][sid]['recovery']['error'] = 'source_update_failed'
            view = store.setting('ex_tsmc_open_view')
            if view is not None:
                state['settings']['ex_tsmc_open_view'] = view
            if db.execute("SELECT 1 FROM information_schema.tables WHERE table_name='publication_calendars'").fetchone():
                cals = db.execute("SELECT calendar_key,retrieved_at,raw_sha256,source_url,payload_json FROM publication_calendars WHERE calendar_key LIKE 'calendar:twse:%' QUALIFY row_number() OVER(PARTITION BY calendar_key ORDER BY CAST(retrieved_at AS TIMESTAMPTZ) DESC)=1").fetchall()
                state['calendars'] = sorted([_calendar(dict(zip(('calendar_key','retrieved_at','raw_sha256','source_url','payload'), [*r[:4], json.loads(r[4])])))
                                            for r in cals], key=lambda r: r['calendar_key'])
        files['state.json'] = canonical(state) + b'\n'
        ledger = store.directory / 'ex-tsmc-open'
        files.update((name, raw) for name, raw in oldfiles.items() if name.startswith('ex-tsmc-open/'))
        if ledger.exists():
            for path in ledger.rglob('*'):
                if path.is_symlink():
                    raise ValueError('Symlink in model dependency ledger')
                if path.is_file():
                    name = 'ex-tsmc-open/' + path.relative_to(ledger).as_posix()
                    if not _ledger_path(name):
                        raise ValueError('Unexpected model ledger file')
                    raw = path.read_bytes()
                    if name in files and name != 'ex-tsmc-open/verified-events.json' and files[name] != raw:
                        raise ValueError('Immutable model dependency collision')
                    files[name] = raw
        # Validate the complete candidate before changing this tree.
        import tempfile
        self.directory.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(prefix='.canonical-check-', dir=self.directory.parent) as temp:
            candidate = Path(temp)
            for name, raw in files.items():
                target = candidate / name
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(raw)
            report = CanonicalHistory(candidate).validate()
            changed = files != oldfiles
            observation_changed = {k:v for k,v in files.items() if k.startswith('observations/')} != {
                k:v for k,v in oldfiles.items() if k.startswith('observations/')}
            # Only validated individual files are installed; workflow commits/deploys
            # this tree only after successful validation of the whole candidate.
            self.directory.mkdir(parents=True, exist_ok=True)
            for name, raw in files.items():
                target = self.directory / name
                target.parent.mkdir(parents=True, exist_ok=True)
                if oldfiles.get(name) != raw:
                    temporary = target.with_suffix(target.suffix + '.pending')
                    temporary.write_bytes(raw)
                    temporary.replace(target)
            for name in set(oldfiles) - set(files):
                (self.directory / name).unlink()  # scalar windows only; model dependencies retained
        return {**report, 'changed': changed, 'observations_changed': observation_changed}


def validate_history(directory):
    return CanonicalHistory(directory).validate()


def hydrate_history(history_dir, scratch_data_dir):
    from .store import Store
    target = assert_safe_directory(scratch_data_dir)
    if target.exists() and any(target.iterdir()):
        raise ValueError('Scratch directory must be new or empty')
    store = Store(target)
    CanonicalHistory(history_dir).hydrate(store)
    return store

