"""Forward-only ordinary-share diagnostics and explicitly defined ex-2330 price chains.

No app/database import or implicit HTTP. Engine owns fetching; CompactLedger can
also be used by a database-free collector. Reported shares are estimated model
weights, never certified official index shares. Missing prices are never carried.
"""
from __future__ import annotations
from collections import Counter
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from functools import partial
import hashlib
import json
from pathlib import Path
import re

from .connectors import Source, SchemaError, TWSE_OPENAPI_LICENSE
from .contracts import Job, make_point

MODEL_VERSION = PARSER_VERSION = 'open-common-reported-cap-price-1'
URLS = {
    'basic': 'https://openapi.twse.com.tw/v1/opendata/t187ap03_L',
    'prices': 'https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL',
    'actions': 'https://openapi.twse.com.tw/v1/exchangeReport/TWT48U_ALL',
}
DATASETS = {'basic': '18419', 'prices': '11549', 'actions': '89748'}
SOURCE_IDS = {key: 'twse_open_common_' + key for key in URLS}
SOURCES = tuple(Source(SOURCE_IDS[key], 'TWSE 普通股模型：' + label, URLS[key],
    '開放資料集 ' + DATASETS[key] + '。只累積實際取得的最新快照，不回造歷史；股數出表日不代表生效日，除權息表是預告。',
    attribution='臺灣證券交易所 {year}〈' + label + '〉；EagleEye 自行篩選與計算',
    license_url=TWSE_OPENAPI_LICENSE) for key, label in
    [('basic', '上市公司基本資料'), ('prices', '個股日成交資訊'), ('actions', '上市股票除權除息預告表')])
DERIVED_SOURCE = Source('eagleeye_ex_tsmc_open', '自建上市普通股排除台積電模型', URLS['prices'],
    '申報股數市值價格鏈、同母體每日等權鏈及資料品質診斷；非官方 TAIEX。完整候選與基日固定有價樣本分列；缺價、未知事件及缺日中斷。',
    attribution='臺灣證券交易所 {year}開放資料；EagleEye 自建模型，非交易所編製指數',
    license_url=TWSE_OPENAPI_LICENSE, content_revisions_only=True)
PREFIX = 'twse_open_common_'
WEIGHT = PREFIX + 'tsmc_weight_reported_pct_v1'
COVERAGE = PREFIX + 'priced_count_pct_v1'
CAP = PREFIX + 'known_cap_twd_v1'
FULL_CAP = PREFIX + 'ex2330_cap_price_v1'
FULL_EQUAL = PREFIX + 'ex2330_equal_price_v1'
FIXED_CAP = PREFIX + 'fixed_priced_ex2330_cap_price_v1'
FIXED_EQUAL = PREFIX + 'fixed_priced_ex2330_equal_price_v1'
SERIES = {
    WEIGHT: ('台積電占有價普通股申報市值（子樣本估算）', 'percent'),
    COVERAGE: ('普通股候選檔數配對率（非市值覆蓋）', 'percent'),
    CAP: ('有價普通股子樣本申報市值（估算）', 'TWD'),
    FULL_CAP: ('完整普通股候選排除台積電市值價格鏈（自建）', 'rebased_100'),
    FULL_EQUAL: ('完整普通股候選排除台積電等權價格鏈（自建）', 'rebased_100'),
    FIXED_CAP: ('基日固定有價普通股排除台積電市值價格鏈（自建樣本）', 'rebased_100'),
    FIXED_EQUAL: ('基日固定有價普通股排除台積電等權價格鏈（自建樣本）', 'rebased_100'),
}
REASONS = {
    'missing_prices': '母體有缺價，未補零或沿用舊價',
    'missing_tsmc': '台積電缺少有效價格或股數',
    'shares_changed': '申報股數異動尚未核對生效事件',
    'universe_changed': '普通股母體異動尚未核對',
    'reference_changed': '兩日真實收盤與官方漲跌不一致，待查公司行動或參考價',
    'event_unconfirmed': '當日除權息事件尚未確認',
    'missing_session': '中間有未保存的可能交易日，價格鏈中斷',
    'awaiting_second_day': '等待第二個獨立且有效的交易日快照',
    'source_unavailable': '來源本輪未成功或三份快照未對齊',
    'no_snapshot': '尚未取得普通股模型快照',
}


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':')).encode('utf-8')


def digest(value):
    return hashlib.sha256(canonical(value)).hexdigest()


def number(value):
    if value is None or isinstance(value, bool):
        raise SchemaError('missing number')
    try:
        result = Decimal(str(value).replace(',', '').strip())
    except InvalidOperation as exc:
        raise SchemaError('invalid number') from exc
    if not result.is_finite():
        raise SchemaError('non-finite number')
    return result


def day(value):
    value = str(value)
    if re.fullmatch(r'\d{7}', value):
        value = f'{int(value[:3]) + 1911:04d}-{value[3:5]}-{value[5:]}'
    elif re.fullmatch(r'\d{8}', value):
        value = f'{value[:4]}-{value[4:6]}-{value[6:]}'
    return date.fromisoformat(value).isoformat()


def timestamp(value):
    parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if parsed.utcoffset() is None:
        raise SchemaError('capture time requires timezone')
    return parsed


def _rows(raw, required, allow_empty=False):
    try:
        rows = json.loads(raw.decode('utf-8-sig'))
    except (ValueError, UnicodeError) as exc:
        raise SchemaError('invalid official JSON') from exc
    if not isinstance(rows, list) or (not rows and not allow_empty) or any(not isinstance(r, dict) or not required <= set(r) for r in rows):
        raise SchemaError('missing rows or required fields')
    return rows


def parse_resource(raw, key, today=None):
    required = {
        'basic': {'公司代號', '公司簡稱', '出表日期', '已發行普通股數或TDR原股發行股數', '私募股數', '上市日期'},
        'prices': {'Code', 'Date', 'ClosingPrice', 'Change'},
        'actions': {'Code', 'Date', 'Exdividend', 'StockDividendRatio', 'SubscriptionRatio', 'SubscriptionPricePerShare',
                    'CashDividend', 'SharesOffered', 'SharesEmpOwner', 'SharesholderOwner'},
    }[key]
    rows = _rows(raw, required, allow_empty=key == 'actions')
    code_key = '公司代號' if key == 'basic' else 'Code'
    if key != 'actions' and len({r[code_key] for r in rows}) != len(rows):
        raise SchemaError('duplicate security code')
    dates = {day(r['出表日期'] if key == 'basic' else r['Date']) for r in rows}
    if key != 'actions':
        if len(dates) != 1 or max(dates) > (today or datetime.now(timezone(timedelta(hours=8))).date()).isoformat():
            raise SchemaError('mixed, absent or future content date')
    return rows


def validate_resource(raw, key):
    parse_resource(raw, key)
    return []  # Source captures, not invented scalar observations.


def jobs(start, end):
    return [Job('ex-tsmc-open:' + key, SOURCE_IDS[key], url, partial(validate_resource, key=key),
                parser_version=PARSER_VERSION, headers={'Accept': 'application/json'}, max_bytes=4_000_000,
                refresh_after_hours=2, empty_ok=True) for key, url in URLS.items()]


def collect(raw_by_key, retrieved_at, today=None):
    """Normalize only fields needed by the model; never retain company contact data."""
    parsed = {key: parse_resource(raw_by_key[key], key, today) for key in URLS}
    stamps = {key: (retrieved_at[key] if isinstance(retrieved_at, dict) else retrieved_at) for key in URLS}
    instants = [timestamp(v) for v in stamps.values()]
    if max(instants) - min(instants) > timedelta(hours=6):
        raise SchemaError('source captures are more than six hours apart')
    basic_day, price_day = day(parsed['basic'][0]['出表日期']), day(parsed['prices'][0]['Date'])
    if basic_day != price_day:
        raise SchemaError('basic report and price dates differ')
    for key in ('basic', 'prices'):
        if price_day > timestamp(stamps[key]).astimezone(timezone(timedelta(hours=8))).date().isoformat():
            raise SchemaError('content date is after its capture date')
    prices = {r['Code']: r for r in parsed['prices']}
    candidates, excluded = [], []
    for row in sorted(parsed['basic'], key=lambda r: r['公司代號']):
        code, name = row['公司代號'], row['公司簡稱'].strip().upper()
        reason = 'tdr' if name.endswith('-DR') or code.startswith('91') else None
        if not re.fullmatch(r'\d{4}', code):
            reason = reason or 'not_four_digit_ordinary'
        listed = day(row['上市日期'])
        if listed > price_day:
            reason = reason or 'not_yet_listed'
        if reason:
            excluded.append({'code': code, 'reason': reason})
            continue
        quote = prices.get(code, {})
        item = {'code': code, 'shares': row['已發行普通股數或TDR原股發行股數'],
                'private_shares': row['私募股數'], 'listing_date': listed,
                'close': quote.get('ClosingPrice'), 'change': quote.get('Change')}
        item['missing_reason'] = _invalid(item, quote_present=bool(quote))
        candidates.append(item)
    actions = []
    for row in parsed['actions']:
        event = {k: row[k] for k in sorted(row) if k != 'Name'}
        event['event_date'] = day(row['Date'])
        event['event_id'] = digest(event)
        event['confirmed'] = False
        actions.append(event)
    if len({r['event_id'] for r in actions}) != len(actions):
        raise SchemaError('duplicate corporate action')
    result = {'method_version': MODEL_VERSION, 'data_date': price_day, 'price_date': price_day,
              'shares_report_date': basic_day, 'shares_effective_date': None,
              'retrieved_at': max(stamps.values(), key=timestamp), 'known_at': max(stamps.values(), key=timestamp),
              'candidates': candidates, 'excluded': excluded, 'actions': actions,
              'provenance': {k: {'url': URLS[k], 'dataset': DATASETS[k], 'retrieved_at': stamps[k],
                  'sha256': hashlib.sha256(raw_by_key[k]).hexdigest(),
                  'content_date': price_day if k != 'actions' else None} for k in URLS},
              'original_raw_retained_by_ledger': False}
    result['snapshot_id'] = digest({k: v for k, v in result.items() if k not in {'retrieved_at', 'known_at', 'provenance'}} |
                                  {'source_hashes': {k: v['sha256'] for k, v in result['provenance'].items()}})
    return result


def _invalid(item, quote_present=True):
    if not quote_present:
        return 'quote_absent'
    try:
        q, private, price = (number(item[k]) for k in ('shares', 'private_shares', 'close'))
        if q <= 0 or q != q.to_integral_value() or price <= 0 or not 0 <= private <= q:
            return 'invalid_quote_or_shares'
    except (ValueError, KeyError):
        return 'invalid_quote_or_shares'
    return None


class CompactLedger:
    """Immutable content-addressed share/event objects and monthly daily snapshots.

    A same-day correction is appended as a revision. No raw original reconstruction
    is claimed. Creating this object and read methods do not create directories.
    """
    def __init__(self, directory):
        self.directory = Path(directory)

    def _write_once(self, path, value):
        path.parent.mkdir(parents=True, exist_ok=True)
        raw = canonical(value)
        if path.exists():
            if path.read_bytes() != raw:
                raise ValueError('immutable ledger collision')
        else:
            with path.open('xb') as stream:
                stream.write(raw)

    def append(self, snapshot):
        universe_path = self.directory / 'universe.json'
        if not universe_path.exists():
            self._write_once(universe_path, {'base_universe_date': snapshot['data_date'],
                'base_snapshot_id': snapshot['snapshot_id'],
                'all_codes': sorted(r['code'] for r in snapshot['candidates']),
                'fixed_codes': sorted(r['code'] for r in snapshot['candidates'] if not r['missing_reason']),
                'fixed_exclusions': [{'code': r['code'], 'reason': r['missing_reason']} for r in snapshot['candidates'] if r['missing_reason']]})
        shares = [{k: r[k] for k in ('code', 'shares', 'private_shares', 'listing_date')} for r in snapshot['candidates']]
        shares_hash, actions_hash = digest(shares), digest(snapshot['actions'])
        self._write_once(self.directory / 'objects' / (shares_hash + '.json'), shares)
        self._write_once(self.directory / 'objects' / (actions_hash + '.json'), snapshot['actions'])
        compact = {k: v for k, v in snapshot.items() if k not in {'candidates', 'actions'}}
        compact.update(shares_object=shares_hash, actions_object=actions_hash,
                       prices=[{k: r[k] for k in ('code', 'close', 'change', 'missing_reason')} for r in snapshot['candidates']])
        month = self.directory / 'snapshots' / snapshot['data_date'][:7]
        versions = [json.loads(p.read_text(encoding='utf-8')) for p in month.glob(snapshot['data_date'] + '-*.json')]
        latest = max(versions, key=lambda r: timestamp(r['known_at']), default=None)
        if latest and latest['snapshot_id'] == snapshot['snapshot_id']:
            return False  # Same latest response is not a revision; A to B to A is.
        target = month / (snapshot['data_date'] + '-' + snapshot['snapshot_id'] + '-' + digest(snapshot['known_at'])[:16] + '.json')
        self._write_once(target, compact)
        return True

    def snapshots(self):
        latest = {}
        for path in sorted((self.directory / 'snapshots').glob('*/*.json')):
            item = json.loads(path.read_text(encoding='utf-8'))
            old = latest.get(item['data_date'])
            if old is None or timestamp(item['known_at']) > timestamp(old['known_at']):
                latest[item['data_date']] = item
        output = []
        for item in sorted(latest.values(), key=lambda r: r['data_date']):
            shares = json.loads((self.directory / 'objects' / (item.pop('shares_object') + '.json')).read_text(encoding='utf-8'))
            item['actions'] = json.loads((self.directory / 'objects' / (item.pop('actions_object') + '.json')).read_text(encoding='utf-8'))
            prices = {r['code']: r for r in item.pop('prices')}
            item['candidates'] = [r | prices[r['code']] for r in shares]
            output.append(item)
        return output


def diagnose(snapshot):
    rows = snapshot['candidates']
    valid = [r for r in rows if not r['missing_reason']]
    capital = sum((number(r['close']) * number(r['shares']) for r in valid), Decimal(0))
    tsmc = next((number(r['close']) * number(r['shares']) for r in valid if r['code'] == '2330'), None)
    missing = [r['code'] for r in rows if r['missing_reason']]
    return {'data_date': snapshot['data_date'], 'candidate_count': len(rows), 'priced_count': len(valid),
            'missing_codes': missing, 'coverage_by_count': float(Decimal(len(valid)) / len(rows)) if rows else 0,
            'coverage_by_cap': None, 'known_cap_twd': str(capital),
            'tsmc_reported_weight_pct': str(tsmc / capital * 100) if tsmc and capital > tsmc else None,
            'excluded_reason_counts': dict(Counter(r['reason'] for r in snapshot['excluded'])),
            'universe_hash': digest(sorted(r['code'] for r in rows)),
            'shares_report_date': snapshot['shares_report_date'], 'shares_effective_date': None,
            'point_in_time_eligible': False, 'classification': 'estimated',
            'weight_scope': 'priced_candidate_subset_reported_shares',
            'source_hashes': {k: v['sha256'] for k, v in snapshot['provenance'].items()},
            'event_ids': [r['event_id'] for r in snapshot['actions'] if r['event_date'] == snapshot['data_date']],
            'pending_actions': [{'code': r['Code'], 'event_date': r['event_date'], 'event_id': r['event_id'],
                'exdividend': r['Exdividend'], 'subscription_ratio': r['SubscriptionRatio'],
                'subscription_price': r['SubscriptionPricePerShare'], 'confirmed': False}
                for r in snapshot['actions'] if r['event_date'] == snapshot['data_date'] and r['Code'] in {x['code'] for x in rows}],
            'quality_flags': ['reported_shares_effective_date_unknown'] + (['missing_prices'] if missing else []),
            'method_version': MODEL_VERSION}


def _gap(previous, current, holidays):
    cursor = date.fromisoformat(previous) + timedelta(days=1)
    stop = date.fromisoformat(current)
    if stop <= date.fromisoformat(previous):
        return True
    while cursor < stop:
        if cursor.weekday() < 5 and cursor.isoformat() not in holidays:
            return True
        cursor += timedelta(days=1)
    return False


def _automatic_cash_event(previous, current, code, p0, p1, q0, q1, change):
    # A dated cash-only announcement plus the independently observed reference
    # adjustment is evidence of this price adjustment, not of effective shares.
    events = {e['event_id']: e for e in previous['actions'] + current['actions']
              if e['Code'] == code and e['event_date'] == current['data_date']}
    if len(events) != 1 or q0 != q1:
        return None
    event = next(iter(events.values()))
    if event['Exdividend'] != '息':
        return None
    try:
        cash = number(event['CashDividend'])
        if cash <= 0 or any(str(event.get(k, '')).strip() not in {'', '0', '0.0', '0.00'} for k in
                           ('StockDividendRatio', 'SubscriptionRatio', 'SubscriptionPricePerShare',
                            'SharesOffered', 'SharesEmpOwner', 'SharesholderOwner')):
            return None
        if abs((p1 - p0 + cash) - number(change)) > Decimal('0.000001'):
            return None
    except ValueError:
        return None
    return {'type': 'cash_dividend', 'old_shares': str(q0), 'new_shares': str(q1),
            'cash_dividend': str(cash), 'event_id': event['event_id'],
            'automatic_verification': 'two_saved_closes_official_change_and_cash_only_announcement'}


def price_step(previous, current, codes, verified_events=(), holidays=()):
    """Strict transition; Change only cross-checks two real closes, never creates one.

    Verified events require external evidence hashes and explicit effective shares.
    Unknown event types, same-day announcements and unexplained share changes stop.
    """
    problems, returns, adjustment, evidence_ids = [], [], Decimal(0), []
    if _gap(previous['data_date'], current['data_date'], set(holidays)):
        return {'ok': False, 'reasons': [{'code': 'missing_session'}]}
    old = {r['code']: r for r in previous['candidates']}
    new = {r['code']: r for r in current['candidates']}
    announced = {r['Code'] for r in previous['actions'] + current['actions'] if previous['data_date'] < r['event_date'] <= current['data_date']}
    verified = {}
    for event in verified_events:
        if event.get('date') != current['data_date']:
            continue
        if (not event.get('confirmed') or not event.get('evidence_hashes') or
                any(not re.fullmatch('[a-f0-9]{64}', h) for h in event['evidence_hashes'])):
            raise SchemaError('verified event requires confirmation and SHA-256 evidence')
        timestamp(event['known_at'])
        if event['code'] in verified:
            raise SchemaError('multiple verified events for one stock require a combined reviewed event')
        verified[event['code']] = event
    old_total = new_total = Decimal(0)
    for code in codes:
        if code == '2330':
            continue
        if code not in old or code not in new:
            problems.append({'code': 'universe_changed', 'security': code}); continue
        a, b = old[code], new[code]
        if a['missing_reason'] or b['missing_reason']:
            problems.append({'code': 'missing_prices', 'security': code}); continue
        p0, p1, q0, q1 = map(number, (a['close'], b['close'], a['shares'], b['shares']))
        event = verified.get(code) or _automatic_cash_event(previous, current, code, p0, p1, q0, q1, b['change'])
        add = Decimal(0)
        if event:
            kind = event.get('type')
            if number(event['old_shares']) != q0 or number(event['new_shares']) != q1:
                problems.append({'code': 'shares_changed', 'security': code}); continue
            if kind == 'cash_dividend':
                if q0 != q1 or number(event['cash_dividend']) < 0:
                    raise SchemaError('invalid cash dividend event')
            elif kind in {'split', 'bonus', 'reverse_split'}:
                if number(event['ratio']) <= 0 or q1 != q0 * number(event['ratio']):
                    raise SchemaError('split/bonus effective shares do not match ratio')
            elif kind == 'cash_subscription':
                if q1 <= q0 or number(event['subscription_price']) <= 0:
                    raise SchemaError('invalid cash subscription')
                add = (q1 - q0) * number(event['subscription_price'])
            else:
                problems.append({'code': 'event_unconfirmed', 'security': code, 'event_type': kind}); continue
            evidence_ids.append(event.get('event_id') or digest(event))
        else:
            if code in announced:
                problems.append({'code': 'event_unconfirmed', 'security': code}); continue
            if q0 != q1:
                problems.append({'code': 'shares_changed', 'security': code}); continue
            try:
                change = number(b['change'])
            except ValueError:
                problems.append({'code': 'reference_changed', 'security': code}); continue
            if abs((p1 - p0) - change) > Decimal('0.000001'):
                problems.append({'code': 'reference_changed', 'security': code}); continue
        denominator = p0 * q0 + add
        if denominator <= 0:
            raise SchemaError('non-positive return denominator')
        gross = p1 * q1 / denominator
        if gross <= 0:
            raise SchemaError('non-positive return denominator')
        returns.append(gross - 1)
        old_total += p0 * q0
        new_total += p1 * q1
        adjustment += add
    if problems or not returns:
        return {'ok': False, 'reasons': problems or [{'code': 'missing_prices'}]}
    return {'ok': True, 'cap_return': new_total / (old_total + adjustment) - 1,
            'equal_return': sum(returns) / len(returns), 'adjustment_twd': str(adjustment),
            'event_ids': evidence_ids, 'reasons': []}


def build_model(snapshots, verified_events=(), holidays=(), universe_seed=None):
    """Both full-candidate and once-fixed priced-universe models, with broken segments."""
    snapshots = sorted(snapshots, key=lambda r: r['data_date'])
    if len({r['data_date'] for r in snapshots}) != len(snapshots):
        raise SchemaError('same-day revisions must be resolved before model building')
    if not snapshots:
        return {'method_version': MODEL_VERSION, 'status': 'no_snapshot', 'message': REASONS['no_snapshot'], 'points': []}
    if universe_seed:
        snapshots = [s for s in snapshots if s['data_date'] >= universe_seed['base_universe_date']]
        if not snapshots:
            raise SchemaError('no snapshots within the fixed universe lifetime')
    baseline = snapshots[0]
    fixed_codes = sorted(r['code'] for r in baseline['candidates'] if not r['missing_reason'])
    fixed_excluded = [{'code': r['code'], 'reason': r['missing_reason']} for r in baseline['candidates'] if r['missing_reason']]
    all_codes = sorted(r['code'] for r in baseline['candidates'])
    if universe_seed:
        fixed_codes, fixed_excluded = universe_seed['fixed_codes'], universe_seed['fixed_exclusions']
        all_codes = universe_seed['all_codes']
    diagnostics = [diagnose(s) for s in snapshots]
    points, models = [], {}
    for mode, ids in [('full', (FULL_CAP, FULL_EQUAL)), ('fixed_priced', (FIXED_CAP, FIXED_EQUAL))]:
        codes = all_codes if mode == 'full' else fixed_codes
        universe = {'universe_id': 'open_common_' + mode, 'universe_version': MODEL_VERSION,
                    'universe_hash': digest(codes), 'base_universe_date': universe_seed['base_universe_date'] if universe_seed else baseline['data_date'],
                    'constituent_count': len(codes), 'fixed_exclusions': fixed_excluded if mode == 'fixed_priced' else [],
                    'is_official_taiex': False, 'is_full_candidate_universe': mode == 'full'}
        pending = None
        levels = None
        last_valid = None
        segment = None
        state_reasons = []
        for snap in snapshots:
            lookup = {r['code']: r for r in snap['candidates']}
            missing = [c for c in codes if c not in lookup or lookup[c]['missing_reason']]
            reasons = [{'code': 'missing_prices', 'securities': missing}] if missing else []
            if '2330' not in codes:
                reasons.append({'code': 'missing_tsmc'})
            if mode == 'full' and sorted(lookup) != codes:
                reasons.append({'code': 'universe_changed'})
            if reasons:
                pending = levels = None
                state_reasons = reasons
                continue
            if pending is None:
                pending = snap
                levels = None
                state_reasons = [{'code': 'awaiting_second_day'}]
                continue
            step = price_step(pending, snap, codes, verified_events, holidays)
            if not step['ok']:
                pending, levels = snap, None
                state_reasons = step['reasons']
                continue
            if levels is None:
                levels = [Decimal(100), Decimal(100)]
                segment = pending['data_date'] + '-' + universe['universe_hash'][:12]
                for sid in ids:
                    points.append(make_point(SERIES, sid, pending['data_date'], 100, status='estimated', metadata={
                        **universe, 'method_version': MODEL_VERSION, 'segment_id': segment, 'base_date': pending['data_date'],
                        'point_in_time_eligible': False, 'quality_schema_version': 1, 'quality_flags': [],
                        'quality_issues': [{'code': 'reported_shares_effective_date_unknown', 'blocking': False}],
                        'source_hashes': diagnose(pending)['source_hashes'], 'is_segment_base': True}))
            for i, sid in enumerate(ids):
                levels[i] *= 1 + step['cap_return' if i == 0 else 'equal_return']
                points.append(make_point(SERIES, sid, snap['data_date'], levels[i], status='estimated', metadata={
                    **universe, 'method_version': MODEL_VERSION, 'segment_id': segment, 'base_date': segment[:10],
                    'point_in_time_eligible': False, 'quality_schema_version': 1, 'quality_flags': [],
                        'quality_issues': [{'code': 'reported_shares_effective_date_unknown', 'blocking': False}],
                    'source_hashes': diagnose(snap)['source_hashes'], 'previous_snapshot_id': pending['snapshot_id'],
                    'snapshot_id': snap['snapshot_id'], 'adjustment_twd': step['adjustment_twd'],
                    'event_ids': step['event_ids'], 'reference_check': 'two_independent_closes_against_official_change',
                    'rebalancing': 'daily', 'measure': 'price_return_cash_dividends_not_reinvested'}))
            last_valid = {'data_date': snap['data_date'], 'capital_price': float(levels[0]), 'equal_price': float(levels[1]), 'segment_id': segment}
            pending = snap
            state_reasons = []
        models[mode] = {**universe, 'status': 'ok' if not state_reasons else 'waiting',
                        'reasons': [{**r, 'message': REASONS.get(r['code'], r['code'])} for r in state_reasons],
                        'last_valid': last_valid}
    for diagnostic in diagnostics:
        for sid, value in [(COVERAGE, diagnostic['coverage_by_count'] * 100), (CAP, diagnostic['known_cap_twd']),
                           (WEIGHT, diagnostic['tsmc_reported_weight_pct'])]:
            if value is not None:
                points.append(make_point(SERIES, sid, diagnostic['data_date'], value, status='estimated', metadata={
                    **diagnostic, 'diagnostic_only': True, 'eligible_for_full_market_analysis': False}))
    return {'method_version': MODEL_VERSION, 'status': 'ok' if models['fixed_priced']['status'] == 'ok' else 'waiting',
            'data_date': snapshots[-1]['data_date'], 'diagnostics': diagnostics[-1], 'models': models,
            'points': points, 'limitations': ['申報股數不保證當日生效；不是官方TAIEX或投資報酬。',
                '固定樣本只在基日選一次有價普通股，後續缺價即中斷，不每日縮減。',
                'Change僅核對兩份獨立收盤；未發現異常不等於官方公司行動完整認證。',
                '中斷後新段重新設基期100；各段基點不能跨段接線或比較漲跌。']}


def _current_captures(store):
    raw, stamps = {}, {}
    for key in URLS:
        cp = store.checkpoint('ex-tsmc-open:' + key)
        if not cp or cp['status'] != 'ok' or not cp.get('raw_sha256') or cp.get('url') != URLS[key]:
            raise SchemaError('source_unavailable: ' + key)
        path = store.raw_dir / (cp['raw_sha256'] + '.json')
        raw[key] = path.read_bytes()
        if hashlib.sha256(raw[key]).hexdigest() != cp['raw_sha256']:
            raise SchemaError('source raw hash mismatch')
        stamps[key] = cp['fetched_at']
    return raw, stamps


def _inputs(store):
    ledger = CompactLedger(store.directory / 'ex-tsmc-open')
    verified_path = ledger.directory / 'verified-events.json'
    events = json.loads(verified_path.read_text(encoding='utf-8')) if verified_path.exists() else []
    from .market_holidays import build as holidays_view
    holidays = set()
    snapshots = ledger.snapshots()
    for year in {int(s['data_date'][:4]) for s in snapshots}:
        holidays.update(holidays_view(store, year=year)['markets'].get('tw', []))
    seed_path = ledger.directory / 'universe.json'
    seed = json.loads(seed_path.read_text(encoding='utf-8')) if seed_path.exists() else None
    return ledger, snapshots, events, holidays, seed


def derive(store):
    raw, stamps = _current_captures(store)
    snapshot = collect(raw, stamps)
    ledger = CompactLedger(store.directory / 'ex-tsmc-open')
    ledger.append(snapshot)
    _, snapshots, events, holidays, seed = _inputs(store)
    result = build_model(snapshots, events, holidays, seed)
    chains = {FULL_CAP, FULL_EQUAL, FIXED_CAP, FIXED_EQUAL}
    expected = {(p['series_id'], p['observation_date']) for p in result['points'] if p['series_id'] in chains}
    for series_id in chains:
        for old in store.observations(series_id):
            if old.get('source_id') != DERIVED_SOURCE.id or (series_id, old['observation_date']) in expected:
                continue
            metadata = {**old.get('metadata', {}), 'invalidated_by_source_revision': True,
                'quality_schema_version': 1, 'quality_flags': ['source_revision_invalidated'],
                'quality_issues': [{'code': 'source_revision_invalidated', 'blocking': True}],
                'invalidation_source_hashes': result['diagnostics']['source_hashes']}
            result['points'].append(make_point(SERIES, series_id, old['observation_date'], old['value'],
                                               status='estimated', metadata=metadata))
    result['model_state'] = 'calculated'
    result['series_persistence'] = 'engine_owned_not_verified_by_this_view'
    store.set_setting('ex_tsmc_open_view', {k: v for k, v in result.items() if k != 'points'})
    return [(DERIVED_SOURCE, result['points'])]


def public_view(store):
    """Read-only prepared API view; works from an exported DB without raw/ledger files."""
    result = store.setting('ex_tsmc_open_view', None)
    if result is None:
        return {'method_version': MODEL_VERSION, 'status': 'no_snapshot',
                'source_state': 'unavailable', 'message': REASONS['no_snapshot']}
    # settings returns a fresh object; never mutate database state from a GET.
    expected = result.get('diagnostics', {}).get('source_hashes', {})
    result['source_state'] = 'ok'
    for key in URLS:
        cp = store.checkpoint('ex-tsmc-open:' + key)
        if (not cp or cp['status'] != 'ok' or cp.get('url') != URLS[key] or
                cp.get('parser_version') != PARSER_VERSION):
            result['source_state'] = 'unavailable'
            result['source_error'] = 'source_unavailable: ' + key
            break
        if cp.get('raw_sha256') != expected.get(key):
            result['source_state'] = 'pending'
            result['source_error'] = 'new source capture is not yet an aligned model snapshot: ' + key
            break
    if result['source_state'] != 'ok':
        result['status'] = 'waiting'
        result['message'] = REASONS['source_unavailable']
    return result


def filter_current_segments(rows):
    """Keep each new chain's latest segment for analyses that cannot handle gaps.

    Original observations and earlier segments remain in Store. Diagnostic and
    unrelated series pass through. Unknown segment metadata on chain IDs is not
    eligible for a continuous-return analysis.
    """
    chains = {FULL_CAP, FULL_EQUAL, FIXED_CAP, FIXED_EQUAL}
    rows = [r for r in rows if not (r.get('metadata') or {}).get('invalidated_by_source_revision')]
    latest = {}
    for row in rows:
        if row['series_id'] not in chains:
            continue
        segment = (row.get('metadata') or {}).get('segment_id')
        if not segment:
            continue
        key = (row.get('source_id'), row['series_id'])
        candidate = (row['observation_date'], segment)
        if key not in latest or candidate > latest[key]:
            latest[key] = candidate
    return [row for row in rows if row['series_id'] not in chains or
            ((row.get('source_id'), row['series_id']) in latest and
             (row.get('metadata') or {}).get('segment_id') == latest[(row.get('source_id'), row['series_id'])][1])]
