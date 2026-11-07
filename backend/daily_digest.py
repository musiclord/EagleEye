"""Deterministic, evidence-linked daily notes. No network, AI, database, or notifications IO."""
from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import json
import math

from .history import TAIPEI, observation_availability, parse_instant
from .positions import calendar_dates, valid_change
from .quality import blocking_quality, observation_problem

MODEL_VERSION = 'daily-digest-2'
LIMITATIONS = [
    '摘要完全由本地數值規則與中文範本產生，不使用外部 AI；斷線時可閱讀已保存資料，但無法知道尚未下載的變化。',
    '每日摘要的日期是產生日期；每項另列觀測日期與比較期間。月資料不會被當成今日發生的變化。',
    '提醒表示值得查核，不代表危機機率、因果關係或買賣建議；沒有提醒也不代表沒有風險。',
    '交易日缺口以已取得的各市場報表日期檢查，不從星期推斷完整交易日曆；跨國收盤資料只作各自期間描述。',
]

# id, group, transform, frequency, age limit, accepted units, why, caveat
SPECS = [
    ('taiex','market','return','daily',7,('point',),'指數反映市值加權的整體價格變化。','價格指數不含股利再投資，也不代表多數股票的報酬。'),
    ('ex_tsmc_estimated','market','return','daily',7,('rebased_100',),'拆開台積電權重，有助分辨大盤與其他股票的表現。','週市值與股數估算，並非官方精確指數；尚未完整重建成分與公司行動。'),
    ('tpex_index','market','return','daily',7,('point',),'上櫃市場與大型股為主的上市市場分開觀察。','價格指數不含股利再投資，不能直接等同小型股投資組合。'),
    ('twse_advance_pct','breadth','level','daily',7,('percent','%'),'每檔計一次；上漲比例可檢查指數漲跌是否擴散到多數股票。','上市股票欄；分母為漲＋跌＋平，排除未成交與無比價，不能推算資金量。'),
    ('tpex_advance_pct','breadth','level','daily',7,('percent','%'),'上櫃股票的漲跌參與程度，可補充市值加權指數的視角。','上櫃股票及換股權利證書；母體與上市不同，兩市場不合併。'),
    ('foreign_net','positions','flow','daily',7,('TWD',),'正值為當日買超、負值為賣超；顯示這一類法人在上市市場的淨交易方向。','含 ETF 等有價證券；不是跨境匯出入，也不能識別同一個投資人。'),
    ('tpex_foreign_net','positions','flow','daily',7,('TWD',),'觀察外資在上櫃市場的當日淨交易方向。','與上市市場範圍不同；不能用買賣超推定全部資金匯出入。'),
    ('tx_foreign_oi_net','positions','difference','daily',7,('contract',),'多方減空方的淨未平倉變化，反映標準台指期部位調整。','未平倉是存量；可能含避險、套利、到期與轉倉，不能直接解讀為方向押注。'),
    ('margin_balance','positions','return','daily',7,('TWD',),'融資餘額變化顯示上市信用交易借款的增減。','不含信貸、股票質借或場外槓桿；不能推算融資維持率或被迫平倉。'),
    ('tpex_margin_balance','positions','return','daily',7,('TWD',),'上櫃融資借款與上市市場分開觀察。','償還、公司行動與報表修訂也會影響餘額，不等於股市總槓桿。'),
    ('usd_twd','global','return','daily',7,('TWD/USD',),'美元／台幣報價上升表示台幣相對美元走弱；下降表示走強。','這是銀行間收盤報價，不是即時或零售牌告；不能單憑匯率認定外資匯出。'),
    ('usd_jpy','global','return','daily',7,('JPY/USD',),'美元／日圓報價上升表示日圓相對美元走弱；下降表示走強。','東京收盤與台股、美國時點不同；不推定套利交易必然撤出。'),
    ('tw_policy_rate','global','policy','event',14,('%',),'重貼現率是台灣央行的政策工具，生效後持續有效。','不是每天重新報價；新鮮度依最近成功查核時間，不依政策生效日。'),
    ('tw_overnight_rate','global','bp','daily',7,('%',),'實際隔夜借款利率反映金融機構短期資金成本。','與政策利率不同，單日上升不等於信用危機。'),
    ('boj_overnight_call_pct','global','bp','daily',7,('%',),'日本實際隔夜利率反映日圓短期借款成本。','不是日本央行政策目標事件，也沒有扣除跨幣別避險成本。'),
    ('us_treasury_2y_pct','global','bp','daily',7,('%',),'兩年公債殖利率提供美元短端資金成本的觀察。','財政部平價殖利率不是持有債券的報酬率。'),
    ('us_treasury_10y_pct','global','bp','daily',7,('%',),'十年公債殖利率影響長期融資與折現條件。','升降原因可能包括通膨、成長與期限溢酬，不據此預測台股。'),
    ('tw_cpi_yoy','macro','monthly','monthly',65,('%',),'消費者物價年增率描述相較去年同月的平均價格變化。','不同家庭的消費組合不同；年增率下降不等於物價下跌。'),
    ('tw_real_regular_wage_yoy','macro','monthly','monthly',100,('%',),'實質經常性薪資年增率檢查平均薪資購買力是否高於去年同月。','本地以同月 CPI 平減；不是薪資中位數，也不代表每個家庭所得。'),
    ('exports_usd_yoy','macro','monthly','monthly',75,('%',),'出口年增率反映貨品外需的變化。','名目美元、未季調且含復出口；不是實質 GDP 成長率。'),
    ('retail_sales_twd_yoy','macro','monthly','monthly',75,('%',),'零售營業額年增率提供本地消費活動的一個觀察。','名目、未季調；沒有扣除物價，不等於實質民間消費。'),
]
TITLES = {
    'taiex':'台股加權指數','ex_tsmc_estimated':'排除台積電估算指數','tpex_index':'櫃買指數',
    'twse_advance_pct':'上市上漲家數比例','tpex_advance_pct':'上櫃上漲家數比例',
    'foreign_net':'上市外資現貨買賣超','tpex_foreign_net':'上櫃外資現貨買賣超',
    'tx_foreign_oi_net':'TX 外資淨未平倉','margin_balance':'上市融資餘額','tpex_margin_balance':'上櫃融資餘額',
    'usd_twd':'美元／新台幣','usd_jpy':'美元／日圓','tw_policy_rate':'台灣重貼現率',
    'tw_overnight_rate':'台灣隔夜拆款利率','boj_overnight_call_pct':'日本實際隔夜利率',
    'us_treasury_2y_pct':'美國 2 年公債殖利率','us_treasury_10y_pct':'美國 10 年公債殖利率',
    'tw_cpi_yoy':'台灣 CPI 年增率','tw_real_regular_wage_yoy':'實質經常性薪資年增率',
    'exports_usd_yoy':'出口年增率','retail_sales_twd_yoy':'零售營業額年增率',
}
MARKET_IDS = {'taiex','ex_tsmc_estimated','tpex_index'}
VENUE_IDS = MARKET_IDS | {'foreign_net','tpex_foreign_net','tx_foreign_oi_net','margin_balance','tpex_margin_balance'}


TOPIC_SPECS = [
    ('market_participation', '市場走勢與參與廣度', '指數的變化，有多少股票一起參與？', ('market','breadth'),
     '比對下一個已公布交易日的指數與上漲比例；若方向不同，先確認市值權重、股票母體與觀測日期。',
     '價格指數與家數比例衡量不同事情；不把多個同方向提醒當成多個獨立風險。'),
    ('positions_leverage', '法人部位與信用槓桿', '外資現貨、期貨與融資如何變動？', ('positions',),
     '核對現貨買賣超、TX 淨未平倉及融資的各自日期，再查期貨到期、轉倉與餘額修訂。',
     '現貨是流量，期貨與融資是存量；無法識別同一投資人，也不能據此認定匯出入或被迫平倉。'),
    ('external_real_economy', '外部條件與實體經濟', '匯率、利率與已發布的景氣資料呈現什麼？', ('global','macro'),
     '對照各國收盤日期與發布日曆；月資料等新月份或修訂後再比較，不把共同漲跌當成因果。',
     '各國收盤時間不同，月度資料有發布落差；匯率與利率共變不證明誰造成台股漲跌。'),
]
SERIES_TOPIC = {s[0]: topic[0] for s in SPECS for topic in TOPIC_SPECS if s[1] in topic[3]}
SERIES_TOPIC.update({'twse_short_balance':'positions_leverage', 'tpex_short_balance':'positions_leverage',
                     'us_yield_curve_10y_2y_bp':'external_real_economy'})


def topic_for_series(ids):
    """Only known series sharing a topic can be bundled; unknown rules stay separate."""
    topics={SERIES_TOPIC.get(sid) for sid in ids}
    return next(iter(topics)) if len(topics)==1 and None not in topics else None


def _finite(value):
    return isinstance(value,(int,float)) and not isinstance(value,bool) and math.isfinite(value)


def _safe(value):
    """Keep evidence serializable even when a malformed input is quarantined."""
    if isinstance(value,dict):return {str(k):_safe(v) for k,v in value.items()}
    if isinstance(value,(list,tuple)):return [_safe(v) for v in value]
    if isinstance(value,float) and not math.isfinite(value):return None
    return value


def _hash(value):
    return hashlib.sha256(json.dumps(_safe(value),ensure_ascii=False,sort_keys=True,separators=(',',':'),allow_nan=False).encode()).hexdigest()


def _semantic(point):
    if not point:return None
    metadata=point.get('metadata') or {}
    return {k:point.get(k) for k in ('series_id','observation_date','value','unit','source_id','status')} | {
        'quality':{k:metadata.get(k) for k in ('quality_schema_version','quality_issues','quality_flag','quality_flags','previous_reported_balance','base_date','method_version')}}


def _evidence(point):
    return _safe({k:deepcopy(point.get(k)) for k in ('series_id','label','observation_date','value','unit','source_id',
        'source_url','raw_sha256','retrieved_at','release_at','status','known_since','metadata')})


def _available(histories, at):
    """Latest locally available version per observation; unknown capture times fail closed."""
    result={}; rejected={}
    for sid,rows in histories.items():
        dated={}
        for row in rows:
            try:
                from datetime import date
                observed=date.fromisoformat(row['observation_date'])
                availability=observation_availability(row)
                known=parse_instant(availability['known_since'])
                if known>at or observed>at.astimezone(TAIPEI).date():continue
            except (ValueError,KeyError,TypeError):
                rejected[sid]='取得／發布時間或觀測日期不明，不能當成本次已知資料。'
                continue
            key=row['observation_date']; previous=dated.get(key)
            if previous is None or known>parse_instant(previous['known_since']):
                dated[key]={**deepcopy(row),**availability}
            elif known==parse_instant(previous['known_since']) and _semantic(row)!=_semantic(previous):
                rejected[sid]='同一觀測日期與可知時間有衝突值，需檢查來源版本。'
        result[sid]=[dated[d] for d in sorted(dated)]
    return result,rejected


def _format(value,unit,sign=False):
    if value is None:return '—'
    if unit=='TWD':value,unit=value/1e8,'億元'
    elif unit=='contract':unit='口'
    elif unit in ('percent','%'):unit='%'
    elif unit in ('point','rebased_100'):unit='點' if unit=='point' else '（基期 100）'
    return f'{value:+,.2f}' + f' {unit}' if sign else f'{value:,.2f} {unit}'


def _source_problem(point,sources,today,frequency,limit):
    source=sources.get(point.get('source_id'),{})
    if source.get('status')=='error':return '來源最近一次更新失敗；保留已保存的數值，暫停變動判讀。'
    if frequency=='event':
        try:age=(today-parse_instant(source.get('last_success_at')).astimezone(TAIPEI).date()).days
        except ValueError:return '尚無政策利率成功查核時間；不能確認目前仍為最新政策。'
    else:
        from datetime import date
        age=(today-date.fromisoformat(point['observation_date'])).days
    if age<0:return '查核時間晚於本次摘要截止時間，暫停判讀。'
    if age>limit:return f'最近資料超過 {limit} 日曆日監測門檻；可能是發布落後、休市或更新中斷，請查看來源與發布日曆。'
    return None


def _comparison(sid,rows,kind,frequency,histories):
    b=rows[-1]
    if kind in ('flow','level'):return None,None
    if len(rows)<2:return None,'只有一個可用端點，無法計算相鄰期間變化。'
    a=rows[-2]
    if blocking_quality(a):return None,'比較起點有阻擋分析的品質旗標；不跳過它改接更早日期。'
    if not _finite(a.get('value')) or a.get('unit')!=b.get('unit'):return None,'比較起點數值或單位不一致。'
    if kind=='monthly':
        month=lambda p:int(p['observation_date'][:4])*12+int(p['observation_date'][5:7])
        if month(b)-month(a)!=1:return None,'前後不是連續觀測月份；仍可閱讀最新同比，但不計算月間差。'
    elif kind!='policy':
        # Same session calendar as the indicator table: a known session between the
        # endpoints means the adjacent change would silently span a missing day.
        from .transmission import analysis_calendar
        calendar=calendar_dates(histories,sid) if sid in VENUE_IDS else analysis_calendar(histories,sid)
        if not valid_change(a,b,calendar):
            return None,'端點間超過 7 日、已有交易日缺口或前餘額修訂不連續；不計算這段變化。'
    if sid=='ex_tsmc_estimated' and (a.get('metadata') or {}).get('base_date')!=(b.get('metadata') or {}).get('base_date'):
        return None,'兩端估算指數基期不同，不能直接計算報酬。'
    if kind=='return':
        if a['value']<=0 or b['value']<=0:return None,'百分比變化需要兩個正值端點。'
        value=(b['value']/a['value']-1)*100;unit='%';formula='(本期 / 前期 − 1) × 100'
    elif kind in ('bp','policy'):
        value=(b['value']-a['value'])*100;unit='bp';formula='(本期百分比 − 前期百分比) × 100；1 bp = 0.01 百分點'
    else:
        value=b['value']-a['value'];unit='百分點' if kind=='monthly' else '口' if b['unit']=='contract' else b['unit'];formula='本期 − 前期'
    return {'from_date':a['observation_date'],'to_date':b['observation_date'],'value':value,'unit':unit,'formula':formula},None


def _update(previous,sid,state):
    old=(previous or {}).get('input_state',{}).get(sid)
    if not old:return 'initial'
    if old==state:return 'unchanged'
    if state.get('observation_date') and (not old.get('observation_date') or state['observation_date']>old['observation_date']):return 'new_observation'
    return 'revision'


def _build_item(spec,histories,sources,today,previous,rejected):
    sid,group,kind,frequency,limit,units,why,caveat=spec
    rows=histories.get(sid,[]);point=rows[-1] if rows else None
    state={'observation_date':point['observation_date'] if point else None,'inputs':[_semantic(p) for p in rows[-2:]]}
    item={'id':sid,'series_id':sid,'title':TITLES[sid],'group':group,'frequency':frequency,'status':'normal',
        'update_kind':_update(previous,sid,state),'summary':'','reason':why,'observation_date':point['observation_date'] if point else None,
        'comparison':None,'latest_value':point.get('value') if point and _finite(point.get('value')) else None,
        'unit':point.get('unit') if point else units[0],
        'evidence':[_evidence(p) for p in rows[-2:]],'limitations':[caveat],'quality_reason':None,'alerts':[]}
    problem=rejected.get(sid)
    if point is None:problem=problem or '尚未取得可用觀測，不以零值或示範值代替。'
    elif not _finite(point.get('value')):problem='最新觀測不是有效有限數值，暫停判讀。'
    elif point.get('unit') not in units:problem=f'最新單位 {point.get("unit")} 與定義不一致，暫停判讀。'
    elif blocking_quality(point):problem='最新觀測有阻擋分析的品質旗標；保留原值與證據，暫停判讀。'
    elif sid.endswith('advance_pct') and not 0<=point['value']<=100:problem='上漲比率超出 0–100% 範圍，暫停判讀。'
    if problem:
        item.update(status='unavailable',summary=problem,quality_reason=problem)
        return item,state
    stale=_source_problem(point,sources,today,frequency,limit)
    if stale:
        item.update(status='stale',summary=f'{point["observation_date"]} 的保存值為 {_format(point["value"],point["unit"])}；目前暫停判讀。',quality_reason=stale)
        return item,state
    comp,problem=_comparison(sid,rows,kind,frequency,histories)
    item['comparison']=comp
    stamp=point['observation_date'][:7] if frequency=='monthly' else point['observation_date']
    value=point['value']
    if kind=='flow':
        action='買超' if value>0 else '賣超' if value<0 else '買賣相抵'
        item['summary']=f'{stamp} 當日{action} {_format(abs(value),point["unit"])}。'
    elif kind=='level':item['summary']=f'{stamp} 可比且有成交的股票中，上漲比例 {_format(value,"%")}。'
    elif kind=='monthly':
        item['summary']=f'{stamp} 相較去年同月為 {_format(value,"%",True)}。'
        if comp:item['summary']+=f'較前月的年增率變動 {_format(comp["value"],"百分點",True)}。'
    elif kind=='policy':
        item['summary']=f'最近保存的政策利率為 {_format(value,"%")}，生效日 {stamp}；不是今日新報價。'
        if comp:item['summary']+=f'相較 {comp["from_date"]} 的政策水準變動 {_format(comp["value"],"bp",True)}。'
    else:
        item['summary']=f'{stamp} 為 {_format(value,point["unit"])}。'
        if comp:item['summary']+=f'較 {comp["from_date"]} 變動 {_format(comp["value"],comp["unit"],True)}。'
    if problem:
        item['quality_reason']=problem
        item['limitations'].append(problem)
        if kind not in ('monthly','policy'):item['status']='unavailable'
    return item,state


def _alert_input_valid(alert,histories,items,sources,today):
    from .positions import POSITION_CATALOG
    ids=alert.get('source_ids') or []
    if not ids or alert.get('status')!='triggered' or not _finite(alert.get('value')):return False
    for sid in ids:
        rows=histories.get(sid,[])
        if not rows:return False
        row=rows[-1]
        if row['observation_date']!=alert.get('observation_date') or not _finite(row.get('value')) or blocking_quality(row):return False
        if sid in items:
            if items[sid]['status'] in ('unavailable','stale'):return False
        else:
            expected=POSITION_CATALOG[sid][1] if sid in POSITION_CATALOG else 'bp' if sid=='us_yield_curve_10y_2y_bp' else None
            if observation_problem(row,expected):return False
            frequency=(row.get('metadata') or {}).get('frequency','daily')
            if _source_problem(row,sources,today,frequency,100 if frequency=='monthly' else 7):return False
            if sid in ('twse_short_balance','tpex_short_balance'):
                if len(rows)<2 or observation_problem(rows[-2],expected):return False
                if not valid_change(rows[-2],row,calendar_dates(histories,sid)):return False
        # Most existing rules describe adjacent changes; never admit a flagged
        # previous point simply because an upstream older rule omitted its check.
        if sid not in ('twse_advance_pct','tpex_advance_pct','foreign_net','tpex_foreign_net','us_yield_curve_10y_2y_bp') and len(rows)>1:
            if blocking_quality(rows[-2]):return False
    return True


def _unique_evidence(rows):
    # Preserve distinct provenance/quality versions; do not reduce to series/date alone.
    return list({_hash(row):row for row in rows}.values())


def _fact_kind(notice):
    """Only merge known equivalent transforms, never every rule on the same date."""
    ids=notice.get('source_ids',[])
    rule=notice.get('rule_id','')
    if len(ids)==1 and ids[0] in SERIES_TOPIC:
        sid=ids[0]
        known={sid+':move',sid+':unusual',sid+':digest_move',
               'positions:'+sid+':move','positions:'+sid+':unusual'}
        if rule in known:
            return 'daily_flow' if sid in ('foreign_net','tpex_foreign_net') else 'adjacent_change'
    return 'rule:'+rule


def _merge_signals(notices,lookup):
    grouped={}; data=[]
    for notice in notices:
        if notice['kind']!='market':
            data.append(notice);continue
        key=(tuple(sorted(notice['source_ids'])),notice['observation_date'],_fact_kind(notice))
        grouped.setdefault(key,[]).append(notice)
    output=[]
    for (ids,day,fact_kind),rules in sorted(grouped.items()):
        rules=list({r['id']:r for r in sorted(rules,key=lambda r:r['rule_id'])}.values())
        first=rules[0];series=lookup.get(ids[0]) if len(ids)==1 else None
        # Keep singleton fields for v1 API readers, while rules is the lossless record.
        merged={**first,'id':_hash({'model':MODEL_VERSION,'rules':sorted(r['id'] for r in rules)}),
                'source_ids':list(ids),'fact_kind':fact_kind,'rule_ids':[r['rule_id'] for r in rules],
                'rules':deepcopy(rules),'event_ids':sorted({r['event_id'] for r in rules if r.get('event_id')}),
                'evidence':_unique_evidence([e for r in rules for e in r['evidence']])}
        if len(rules)>1:
            title=series['title'] if series else first['title']
            merged['title']=title+'：同一觀測觸及多項規則'
            merged['body']=(series['summary'] if series else first['body'])+f' {len(rules)} 條規則檢查同一事實，合併為一個訊號。'
            merged['reason']='各規則的門檻、統計基期及原始證據分別保留；規則數量不代表獨立風險數量。'
        merged['summary']=series['summary'] if series else first['body']
        output.append(merged)
    return output+data


def _summary_items(topic_id,items):
    usable=[i for i in items if i['status'] in ('normal','watch')]
    lookup={i['id']:i for i in usable}
    selected=[]
    if topic_id=='market_participation':
        categories=[('taiex','ex_tsmc_estimated','tpex_index'),('twse_advance_pct','tpex_advance_pct')]
    elif topic_id=='positions_leverage':
        categories=[('foreign_net','tpex_foreign_net'),('tx_foreign_oi_net',),('margin_balance','tpex_margin_balance')]
    else:
        categories=[('usd_twd','usd_jpy'),('us_treasury_10y_pct','us_treasury_2y_pct','tw_overnight_rate','boj_overnight_call_pct','tw_policy_rate'),
                    ('tw_real_regular_wage_yoy','tw_cpi_yoy','exports_usd_yoy','retail_sales_twd_yoy')]
    for ids in categories:
        candidates=[lookup[sid] for sid in ids if sid in lookup]
        # A monthly revision/new observation deserves a fresh look. An unchanged
        # monthly alert is still retained below as evidence, not dressed as news.
        candidates.sort(key=lambda i:(not (i['frequency']=='monthly' and i['update_kind'] in ('new_observation','revision')),
                                      i['status']!='watch'))
        if candidates:selected.append(candidates[0])
    remainder=sorted([i for i in usable if i not in selected],key=lambda i:i['status']!='watch')
    return (selected+remainder)[:3]


def _build_topics(items,notifications):
    topics=[];topic_notices=[]
    for topic_id,title,question,groups,next_check,caveat in TOPIC_SPECS:
        members=[i for i in items if i['group'] in groups]
        signals=[n for n in notifications if n['kind']=='market' and topic_for_series(n['source_ids'])==topic_id]
        usable=[i for i in members if i['status'] in ('normal','watch')]
        unavailable=len(members)-len(usable)
        quality_count=sum(bool(i['quality_reason']) for i in members)
        status='watch' if signals else 'unavailable' if not usable else 'partial' if quality_count else 'normal'
        selected=_summary_items(topic_id,members)
        sentences=[]
        for item in selected:
            prefix='已保存月資料：' if item['frequency']=='monthly' else ''
            if item['frequency']=='monthly' and item['update_kind']=='new_observation':prefix='新觀測月份：'
            if item['frequency']=='monthly' and item['update_kind']=='revision':prefix='月資料修訂：'
            sentences.append(prefix+item['title']+'，'+item['summary'])
        summary=' '.join(sentences) if sentences else '目前沒有足夠且可比較的資料，暫時無法判讀此主題；不以零值或「平穩」替代缺值。'
        facts=[{'id':n['id'],'series_ids':n['source_ids'],'observation_date':n['observation_date'],
                'summary':n['summary'],'rule_ids':n['rule_ids'],'rules':n['rules'],'event_ids':n['event_ids'],
                'evidence':n['evidence'],'fact_kind':n['fact_kind']} for n in signals]
        limitations=[caveat]
        if topic_id=='market_participation':limitations.append('排除台積電是週市值與股數估算，不是官方精確指數；上市與上櫃家數比例的母體不同。')
        if topic_id=='external_real_economy':limitations.append('同比是與去年同月比較；前後月份同比之差是百分點，不是月增率。平均實質薪資也不等於個人所得。')
        if quality_count:limitations.append(f'此主題有 {quality_count} 項資料或比較條件待查核，其中 {unavailable} 項暫停整項判讀；詳見指標原值與品質原因。')
        if not signals and usable:limitations.append('可判讀的資料未觸及本系統觀察門檻；這不是風險為零的證明。')
        rules=[r for n in signals for r in n['rules']]
        topic={'id':topic_id,'title':title,'question':question,'status':status,'summary':summary,'facts':facts,
               'evidence':_unique_evidence([e for i in members for e in i['evidence']]+[e for n in signals for e in n['evidence']]),
               'limitations':limitations,'next_check':next_check,'item_ids':[i['id'] for i in members],
               'rule_ids':[r['rule_id'] for r in rules],'signal_count':len(signals),'rule_count':len(rules),
               'unavailable_count':unavailable,'quality_issue_count':quality_count}
        topics.append(topic)
        if signals:
            topic_notices.append({'id':_hash({'model':MODEL_VERSION,'topic':topic_id,'signals':sorted(n['id'] for n in signals)}),
                'topic_id':topic_id,'kind':'market','severity':'watch','title':title+'：觀測更新',
                'body':summary,'reason':caveat,'rule_ids':topic['rule_ids'],'rules':deepcopy(rules),
                'signal_ids':[n['id'] for n in signals],'event_ids':sorted({r['event_id'] for r in rules if r.get('event_id')}),
                'evidence':_unique_evidence([e for n in signals for e in n['evidence']]),
                'observation_dates':sorted({n['observation_date'] for n in signals if n['observation_date']})})
    return topics,topic_notices


def build(histories,sources,*,generated_at=None,previous=None,alerts=None,events=None):
    """Build one immutable candidate; caller owns persistence/dedup/notification delivery.

    At is an explicit offset instant. Inputs are defensively filtered by local
    availability. previous is a saved build result, never a query-time side effect.
    """
    at=parse_instant(generated_at or datetime.now(timezone.utc));today=at.astimezone(TAIPEI).date()
    available,rejected=_available(histories,at)
    source_map={s['id']:s for s in sources} if not isinstance(sources,dict) else sources
    items=[];input_state={}
    for spec in SPECS:
        item,state=_build_item(spec,available,source_map,today,previous,rejected)
        items.append(item);input_state[spec[0]]=state
    lookup={i['id']:i for i in items};notifications=[];usable_alerts=[]
    events=(events or {}).get('events',[]) if isinstance(events,dict) else events or []
    event_map={e['rule_id']:e for e in events if e.get('rule_id') and e.get('state')=='active'}
    for alert in sorted(alerts or [],key=lambda a:a.get('rule_id','')):
        if not _alert_input_valid(alert,available,lookup,source_map,today):continue
        ids=alert['source_ids'];rule=alert.get('rule_id',alert.get('id','unknown'))
        evidence=[_evidence(p) for sid in ids for p in available.get(sid,[])[-2:]]
        semantic={k:alert.get(k) for k in ('rule_id','title','observation_date','value','unit','threshold','method','explanation','baseline')}
        semantic['inputs']={sid:[_semantic(p) for p in available[sid][-2:]] for sid in ids}
        ident=_hash({'model':MODEL_VERSION,'kind':'market','rule':semantic})
        notice={'id':ident,'kind':'market','title':alert.get('title',rule),'body':f'{alert["observation_date"]}：{_format(alert.get("value"),alert.get("unit",""))}；門檻：{alert.get("threshold","未提供")}。',
            'reason':alert.get('explanation','請查核原始觀測。'),'severity':'watch','rule_id':rule,
            'source_ids':list(ids),'value':alert.get('value'),'unit':alert.get('unit'),'original_alert':deepcopy(alert),
            'observation_date':alert.get('observation_date'),'threshold':alert.get('threshold'),
            'method':alert.get('method'),'baseline':alert.get('baseline'),'evidence':evidence,
            'event_id':event_map.get(rule,{}).get('event_id')}
        notifications.append(notice);usable_alerts.append(semantic)
        for sid in ids:
            if sid in lookup:
                lookup[sid]['status']='watch';lookup[sid]['alerts'].append({'rule_id':rule,'title':notice['title'],'threshold':notice['threshold']})
    # A plain-language market-change threshold supplements the existing monetary/
    # positioning rules; fixed and disclosed, not a statistical risk score.
    for item in items:
        c=item['comparison']
        if item['id'] in MARKET_IDS and item['status'] not in ('unavailable','stale') and c and abs(c['value'])>=2:
            item['status']='watch';item['reason']='單期漲跌幅絕對值達 2% 的固定觀察門檻。'+item['reason']
            rule=item['id']+':digest_move'
            notifications.append({'id':_hash({'model':MODEL_VERSION,'rule':rule,'inputs':input_state[item['id']]}),
                'kind':'market','title':item['title']+'單期變動達觀察門檻','body':item['summary'],'reason':item['reason'],
                'severity':'watch','rule_id':rule,'source_ids':[item['id']],'value':c['value'],'unit':c['unit'],
                'observation_date':item['observation_date'],'threshold':'|單期報酬| ≥ 2%',
                'method':c['formula'],'evidence':item['evidence'],'event_id':None})
    quality=[]
    for item in items:
        if not item['quality_reason']:continue
        ident=_hash({'model':MODEL_VERSION,'kind':'data','sid':item['id'],'reason':item['quality_reason'],'inputs':input_state[item['id']]})
        issue={'id':ident,'series_id':item['id'],'title':item['title'],'reason':item['quality_reason'],'status':item['status'],'evidence':item['evidence']}
        quality.append(issue)
        notifications.append({'id':ident,'kind':'data','title':item['title']+'資料待查核','body':item['quality_reason'],
            'reason':'資料不足或不可比較時，不把它寫成市場平穩，也不補零產生訊號。','severity':'info',
            'rule_id':item['id']+':digest_data','observation_date':item['observation_date'],'evidence':item['evidence'],'event_id':None})
    watch=sum(i['status']=='watch' for i in items)
    usable=sum(i['status'] in ('normal','watch') for i in items)
    newly=sum(i['update_kind']=='new_observation' for i in items)
    revised=sum(i['update_kind']=='revision' for i in items)
    market_dates=[i['observation_date'] for i in items if i['id'] in MARKET_IDS and i['observation_date']]
    market_notices=sum(n['kind']=='market' for n in notifications)
    rule_notifications=notifications
    notifications=_merge_signals(rule_notifications,lookup)
    topics,topic_notifications=_build_topics(items,notifications)
    watch_topics=[t['title'] for t in topics if t['status']=='watch']
    lead=('本次重點為'+ '、'.join(watch_topics)+'；三個主題分別保留事實、依據與待查核事項。' if watch_topics else
          '三個主題整理目前可讀的事實；尚無已驗證的門檻觸發，缺資料仍明確標示。')
    if previous and not newly and not revised:lead='相較上一份摘要，沒有新的觀測期或數值修訂。'+lead
    elif previous:lead=f'相較上一份摘要，{newly} 項出現新觀測期、{revised} 項資料或品質修訂。'+lead
    else:lead='這是目前已保存資料的首次整理，不代表全部變化發生在今天。'+lead
    semantic_items=[{k:i[k] for k in ('id','status','comparison','quality_reason','latest_value','unit','observation_date')} for i in items]
    return _safe({'model_version':MODEL_VERSION,'engine':'local_rules','uses_ai':False,'digest_date':today.isoformat(),
        'generated_at':at.isoformat(),'title':today.isoformat()+' 每日觀測摘要','lead':lead,'items':items,
        'topics':topics,'notifications':notifications,'rule_notifications':rule_notifications,
        'topic_notifications':topic_notifications,'quality_issues':quality,'input_state':input_state,
        'content_hash':_hash({'model':MODEL_VERSION,'inputs':input_state,'items':semantic_items,'alerts':usable_alerts}),
        'coverage':{'observed_series':sum(bool(i['observation_date']) for i in items),'usable_series':usable,
            'watch_items':watch,'unavailable_series':len(items)-usable,'latest_market_date':max(market_dates) if market_dates else None,
            'new_observations':newly,'revisions':revised,'quality_issues':len(quality),'triggered_rules':market_notices,
            'market_signals':sum(n['kind']=='market' for n in notifications),'watch_topics':len(watch_topics)},'limitations':LIMITATIONS})