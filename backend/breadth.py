"""Official Taiwan market breadth. No guessed stock universe or imputed sessions."""
from datetime import date, datetime, timedelta, timezone
from functools import partial
import json
import re

from .connectors import Source, SchemaError, ReportNotAvailable, number, observation_date, TWSE_LOCAL_ONLY_NOTE, twse_website_enabled
from .contracts import Job, make_point
from .quality import issue, with_quality, blocking_quality

MODEL_VERSION = 'breadth-1'
DAILY_CALENDAR = 'taiex'
TWSE_URL = 'https://www.twse.com.tw/exchangeReport/MI_INDEX'
TPEX_URL = 'https://www.tpex.org.tw/www/zh-tw/afterTrading/highlight'
SOURCES = (
    Source('twse_breadth', 'TWSE 股票漲跌家數（網站端點，僅供本機）', TWSE_URL,
           'MI_INDEX 大盤統計的股票欄，排除整體市場含權證等欄。上漲比率分母為漲+跌+平，不含未成交及無比價。'
           + TWSE_LOCAL_ONLY_NOTE, publish=False),
    Source('tpex_breadth', 'TPEx 股票市場現況與漲跌家數', TPEX_URL,
           '官方等價與鉅額成交股票及換股權利證書；與上市股票欄母體不同。上漲比率分母為漲+跌+平，不含未成交。'),
)
SERIES = {}
for market, label in [('twse', '上市'), ('tpex', '上櫃')]:
    for suffix, name in [('advances', '上漲家數'), ('declines', '下跌家數'), ('unchanged', '平盤家數'),
                         ('limit_up', '漲停家數'), ('limit_down', '跌停家數'), ('untraded', '未成交家數'),
                         ('net_advances', '上漲減下跌家數')]:
        SERIES[market + '_' + suffix] = (label + name, 'count')
    SERIES[market + '_advance_pct'] = (label + '可比成交股票上漲比率', 'percent')
SERIES['twse_no_comparison'] = ('上市無比價家數', 'count')
SERIES['tpex_listed_count'] = ('上櫃報表總家數', 'count')
TWSE_FIELDS = ['類型', '整體市場', '股票']
TWSE_ROWS = ['上漲(漲停)', '下跌(跌停)', '持平', '未成交', '無比價']
TPEX_FIELDS = ['上櫃家數', '總資本額(佰萬元)', '總市值(佰萬元)', '本日總成交值(佰萬元)',
    '本日總成交股數(張數)', '收市指數', '指數漲跌', '上漲家數', '漲停家數', '下跌家數',
    '跌停家數', '平盤家數', '未成交(含暫停交易)家數']
LIMITATIONS = [
    '家數反映漲跌參與度，不反映資金量、公司規模或個人投資組合報酬。上漲家數已包含漲停，下跌家數已包含跌停。',
    '上漲比率 = 上漲 ÷（上漲 + 下跌 + 平盤）×100。未成交、暫停與上市無比價均不在分母；不能解讀為全部上市公司上漲機率。',
    '上市取官方「股票」欄；上櫃含等價及鉅額股票、換股權利證書。兩市場分開呈現，並非完全相同的純普通股母體。',
    'TWSE 無比價可能含除權息、新上市、恢復交易或前日無收盤價；只保留官方分類，不自行把缺值當平盤。',
    '未取得完整歷史成分及還原價格，因此尚不提供個股報酬中位數、等權報酬或站上均線比例。',
    '日期是觀測交易日；官方報表未提供歷史發布時間。本地首次取得時間不能冒充當時公開時間。',
]


def _payload(raw, expected_date, status):
    try:
        data = json.loads(raw.decode('utf-8-sig'))
    except (UnicodeError, ValueError) as exc:
        raise SchemaError('Invalid breadth JSON') from exc
    if not isinstance(data, dict):
        raise SchemaError('Breadth response is not a report object')
    if data.get('stat') != status or observation_date(data.get('date')) != expected_date:
        raise ReportNotAvailable('Breadth report missing or returned a different session', expected_date)
    if not isinstance(data.get('tables'), list):
        raise SchemaError('Breadth tables missing')
    return data


def _count(value):
    n = number(value)
    if n < 0 or n != n.to_integral_value():
        raise SchemaError('Breadth counts must be nonnegative integers')
    return int(n)


def _limited_count(value):
    match = re.fullmatch(r'([0-9,]+)\(([0-9,]+)\)', str(value))
    if not match:
        raise SchemaError('Rise/fall total and limit-count format changed')
    total, limited = (_count(v) for v in match.groups())
    if limited > total:
        raise SchemaError('Limit-up/down count exceeds its rise/fall total')
    return total, limited


def _points(market, day, counts, notes, issues=None):
    denominator = counts['advances'] + counts['declines'] + counts['unchanged']
    if denominator <= 0:
        raise SchemaError('No comparable traded stocks; do not fabricate an advance ratio')
    if counts['limit_up'] > counts['advances'] or counts['limit_down'] > counts['declines']:
        raise SchemaError('Limit-count subset bounds failed')
    values = {**counts, 'advance_pct': counts['advances'] / denominator * 100,
              'net_advances': counts['advances'] - counts['declines']}
    output = []
    for suffix, value in values.items():
        sid = market + '_' + suffix
        metadata = with_quality({'frequency': 'daily', 'market': market,
            'calendar': market + '_official_sessions', 'release_date_known': False,
            'scope': 'twse_official_stock_column' if market == 'twse' else 'tpex_stocks_and_exchange_right_certificates',
            'denominator': denominator, 'denominator_formula': 'advances + declines + unchanged',
            'untraded_excluded': counts['untraded'], 'no_comparison_excluded': counts.get('no_comparison'),
            'limit_counts_already_included': True, 'source_notes': notes,
            'formula': 'advances / (advances + declines + unchanged) * 100' if suffix == 'advance_pct'
                       else 'advances - declines' if suffix == 'net_advances' else None},
            [{**i, 'scope': sid} for i in (issues or [])])
        output.append(make_point(SERIES, sid, day, value, release_at=None,
            status='derived' if suffix in {'advance_pct', 'net_advances'} else 'observed', metadata=metadata))
    return output


def parse_twse(raw: bytes, expected_date: str) -> list[dict]:
    data = _payload(raw, expected_date, 'OK')
    if data.get('type') != 'MS':
        raise SchemaError('TWSE breadth requires market-statistics selector MS')
    tables = [t for t in data['tables'] if isinstance(t, dict) and t.get('title') == '漲跌證券數合計']
    if len(tables) != 1 or tables[0].get('fields') != TWSE_FIELDS:
        raise SchemaError('TWSE breadth stock-column table changed')
    table = tables[0]
    rows = table.get('data', [])
    if len(rows) != 5 or any(not isinstance(r, list) or len(r) != 3 for r in rows):
        raise SchemaError('TWSE breadth rows changed')
    indexed = {r[0]: r for r in rows}
    if set(indexed) != set(TWSE_ROWS) or len(indexed) != len(rows):
        raise SchemaError('TWSE breadth categories missing or duplicated')
    counts = {}
    for label, suffix, limit_suffix in [('上漲(漲停)', 'advances', 'limit_up'), ('下跌(跌停)', 'declines', 'limit_down')]:
        stock, limit = _limited_count(indexed[label][2])
        overall, overall_limit = _limited_count(indexed[label][1])
        if stock > overall or limit > overall_limit:
            raise SchemaError('TWSE stock counts exceed all-securities counts')
        counts[suffix], counts[limit_suffix] = stock, limit
    for label, suffix in [('持平', 'unchanged'), ('未成交', 'untraded'), ('無比價', 'no_comparison')]:
        counts[suffix] = _count(indexed[label][2])
        if counts[suffix] > _count(indexed[label][1]):
            raise SchemaError('TWSE stock counts exceed all-securities counts')
    return _points('twse', expected_date, counts, table.get('notes', []))


def parse_tpex(raw: bytes, expected_date: str) -> list[dict]:
    data = _payload(raw, expected_date, 'ok')
    if len(data['tables']) != 1 or not isinstance(data['tables'][0], dict):
        raise SchemaError('Expected one TPEx highlight table')
    table = data['tables'][0]
    if observation_date(table.get('date')) != expected_date or table.get('fields') != TPEX_FIELDS:
        raise SchemaError('TPEx highlight date or units changed')
    if table.get('title') != '上櫃股票當日彙總資訊':
        raise SchemaError('TPEx highlight stock universe changed')
    notes = table.get('notes', [])
    note_text = ''.join(notes) if isinstance(notes, list) and all(isinstance(n, str) for n in notes) else ''
    if '股票、換股權利證書' not in note_text or '不含可轉債' not in note_text:
        raise SchemaError('TPEx breadth scope notes missing or changed')
    rows = table.get('data', [])
    if len(rows) != 1 or not isinstance(rows[0], list) or len(rows[0]) != len(TPEX_FIELDS):
        raise SchemaError('TPEx highlight row malformed')
    r = rows[0]
    counts = {name: _count(r[col]) for name, col in [('listed_count', 0), ('advances', 7), ('limit_up', 8),
        ('declines', 9), ('limit_down', 10), ('unchanged', 11), ('untraded', 12)]}
    expected = sum(counts[x] for x in ['advances', 'declines', 'unchanged', 'untraded'])
    residual = counts['listed_count'] - expected
    issues = [issue('breadth_total_mismatch', field='listed_count', observed=counts['listed_count'],
        expected=expected, residual=residual)] if residual else []
    return _points('tpex', expected_date, counts, notes, issues)


def jobs(start: date, end: date) -> list[Job]:
    if start > end:
        raise ValueError('History start must not exceed end')
    return []  # Dates must come from independently observed venue sessions.


def daily_jobs(trading_dates: list[str], market='twse') -> list[Job]:
    if market not in {'twse', 'tpex'}:
        raise ValueError('Unknown breadth venue')
    if market == 'twse' and not twse_website_enabled():
        return []  # The OpenAPI publishes no advance/decline counts.
    today = datetime.now(timezone(timedelta(hours=8))).date()
    output = []
    for day in sorted(set(trading_dates)):
        d = date.fromisoformat(day)
        url = (TWSE_URL + '?date=' + d.strftime('%Y%m%d') + '&type=MS&response=json' if market == 'twse'
               else TPEX_URL + '?date=' + d.strftime('%Y/%m/%d') + '&response=json')
        output.append(Job(f'{market}_breadth:{day}', market + '_breadth', url,
            partial(parse_twse if market == 'twse' else parse_tpex, expected_date=day),
            parser_version=MODEL_VERSION, refresh_after_hours=8760 if d < today - timedelta(days=30) else 24,
            max_bytes=500_000))
    return output


def tpex_daily_jobs(trading_dates: list[str]) -> list[Job]:
    return daily_jobs(trading_dates, 'tpex')


DIVERGENCE_RULE = {'return_pct': 0.5, 'advance_low_pct': 40.0, 'advance_high_pct': 60.0, 'max_gap_days': 7}
DIVERGENCE_LABELS = {
    'narrow_rise': '指數上漲，但多數股票下跌：漲勢集中在少數權值股。',
    'narrow_fall': '指數下跌，但多數股票上漲：跌勢集中在少數權值股。',
}


def divergence_days(index_rows, ratio_rows, ex_rows=None, rule=DIVERGENCE_RULE):
    """Sessions where the cap-weighted index and the equal-count breadth disagree.

    A local reading rule, not a calibrated signal: it marks days worth reading, with
    the index return computed only between adjacent official sessions.
    """
    index = sorted((p for p in index_rows if not blocking_quality(p)), key=lambda p: p['observation_date'])
    ex = {p['observation_date']: p for p in (ex_rows or [])}
    ex_dates = sorted(ex)
    position = {p['observation_date']: i for i, p in enumerate(index)}
    days, readable = [], 0
    for point in ratio_rows:
        day = point['observation_date']
        i = position.get(day)
        if i is None or i == 0 or blocking_quality(point):
            continue
        before = index[i - 1]
        if (date.fromisoformat(day) - date.fromisoformat(before['observation_date'])).days > rule['max_gap_days'] or before['value'] <= 0:
            continue
        readable += 1
        change = (index[i]['value'] / before['value'] - 1) * 100
        ratio = point['value']
        kind = ('narrow_rise' if change >= rule['return_pct'] and ratio <= rule['advance_low_pct'] else
                'narrow_fall' if change <= -rule['return_pct'] and ratio >= rule['advance_high_pct'] else None)
        if not kind:
            continue
        ex_change = None
        if day in ex:
            j = ex_dates.index(day)
            previous = ex.get(ex_dates[j - 1]) if j else None
            if previous and previous['observation_date'] == before['observation_date'] and previous['value'] > 0 and \
                    (ex[day].get('metadata') or {}).get('base_date') == (previous.get('metadata') or {}).get('base_date'):
                ex_change = (ex[day]['value'] / previous['value'] - 1) * 100
        days.append({'date': day, 'kind': kind, 'label': DIVERGENCE_LABELS[kind], 'index_return_pct': change,
                     'advance_pct': ratio, 'ex_tsmc_return_pct': ex_change, 'from_date': before['observation_date']})
    return {'days': days, 'count': len(days), 'readable_sessions': readable, 'rule': rule,
            'note': '本地閱讀規則，門檻未經校準：指數日報酬至少 ±0.5%，且上漲家數比例 ≤40% 或 ≥60%。'
                    '只標出值得查看的日子，不代表後續走勢；指數由市值加權，家數每檔計一次，兩者母體不同。'}


def overview(histories, sources=None, period=252, today=None):
    """Snapshot without filling missing dates or combining incompatible venue universes."""
    if period not in (20, 60, 120, 252):
        raise ValueError('Unsupported observation window')
    today = today or datetime.now(timezone(timedelta(hours=8))).date()
    today = date.fromisoformat(today) if isinstance(today, str) else today
    states = {s['id']: s for s in (sources or [])}
    venues = []
    for market, label in [('twse', '上市'), ('tpex', '上櫃')]:
        full = {sid: sorted([p for p in histories.get(sid, []) if p['observation_date'] <= today.isoformat()],
                           key=lambda p: p['observation_date']) for sid in SERIES if sid.startswith(market + '_')}
        ratios = full.get(market + '_advance_pct', [])
        latest_date = ratios[-1]['observation_date'] if ratios else None
        cutoff = None
        if latest_date and period == 252:
            anchor = date.fromisoformat(latest_date)
            try:
                cutoff = anchor.replace(year=anchor.year - 1).isoformat()
            except ValueError:
                cutoff = anchor.replace(year=anchor.year - 1, day=28).isoformat()
        series = {sid: [p for p in rows if p['observation_date'] >= cutoff] if cutoff else rows[-period:]
                  for sid, rows in full.items() if rows}
        ratio = series.get(market + '_advance_pct', [])
        latest = {sid.removeprefix(market + '_'): next((p for p in reversed(rows) if p['observation_date'] == latest_date), None)
                  for sid, rows in series.items()}
        latest_ratio = latest.get('advance_pct')
        age_days = (today - date.fromisoformat(latest_date)).days if latest_date else None
        source = states.get(market + '_breadth', {})
        stale = age_days is None or age_days > 7 or source.get('status') == 'error'
        index_id = 'taiex' if market == 'twse' else 'tpex_index'
        index_rows = [p for p in histories.get(index_id, []) if p['observation_date'] <= today.isoformat()]
        divergence = divergence_days(index_rows, ratio, histories.get('ex_tsmc_estimated') if market == 'twse' else None)
        venues.append({'market': market, 'label': label, 'as_of': latest_date, 'latest': latest, 'history': series,
            'divergence': divergence,
            'samples': len(ratio), 'blocked': blocking_quality(latest_ratio) if latest_ratio else False,
            'freshness': {'age_days': age_days, 'stale': stale, 'threshold_days': 7,
                          'source_status': source.get('status')}, 'stale': stale})
    return {'model_version': MODEL_VERSION, 'venues': venues, 'limitations': LIMITATIONS,
            'formula': '上漲 ÷（上漲 + 下跌 + 平盤）×100', 'period': period,
            'window_basis': 'latest_available_one_calendar_year' if period == 252 else 'observations'}
