"""Transparent retrospective comparisons and local observation alerts, not trading signals."""
from datetime import date,datetime,timedelta,timezone
from bisect import bisect_left
from statistics import median
import math,hashlib,json

MODEL_VERSION="transmission-6"
from .quality import observation_problem
DEFAULT_SETTINGS={"z_threshold":3.5,"rate_move_bp":10.0,"fx_move_pct":1.0}
from .positions import POSITION_CATALOG,POSITION_COMPARISONS,POSITION_DEFAULTS,CALENDAR_INPUT_IDS
DEFAULT_SETTINGS.update(POSITION_DEFAULTS)
# unit/frequency are semantic definitions; values and provenance always come from stored observations.
CATALOG={
 "tw_policy_rate":("台灣重貼現率","%","event","台灣","央行政策工具；生效日後延續有效，不是每天重新報價。",14),
 "tw_overnight_rate":("台灣隔夜拆款利率","%","daily","台灣","金融機構短期資金的實際借用成本，與政策利率分開。",7),
 "boj_overnight_call_pct":("日本無擔保隔夜利率","%","daily","日本","日本隔夜拆款實際加權平均年率，不等於央行政策目標。",7),
 "usd_jpy":("美元／日圓","JPY/USD","daily","日本","東京17:00報價；上升表示日圓相對美元貶值。",7),
 "us_treasury_2y_pct":("美國2年公債殖利率","%","daily","美國","財政部殖利率曲線的2年期平價殖利率；不是債券報酬率。",7),
 "us_treasury_10y_pct":("美國10年公債殖利率","%","daily","美國","長期融資與折現率參考；利率上升的原因不只一種。",7),
 "us_yield_curve_10y_2y_bp":("美債10年−2年利差","bp","daily","美國","同日10年期減2年期；負數為曲線倒掛，不是衰退倒數計時。",7),
 "tw_cpi":("台灣消費者物價指數","index","monthly","物價與薪資","消費者物價總指數，2021年=100；未季節調整。",65),
 "tw_cpi_yoy":("台灣CPI年增率","%","monthly","物價與薪資","與去年同月比較的物價變化；不是每個家庭的個別通膨。",65),
 "tw_regular_wage":("名目經常性薪資","TWD","monthly","物價與薪資","工業及服務業平均經常性薪資；不等於薪資中位數或家庭收入。",100),
 "tw_regular_wage_yoy":("名目經常性薪資年增率","%","monthly","物價與薪資","同口徑薪資與去年同月比較，不混用總薪資與經常性薪資。",100),
 "tw_real_regular_wage":("實質經常性薪資","TWD","monthly","物價與薪資","以CPI調整購買力；是基年價格的平均薪資，不是實際領到的金額。",100),
 "tw_real_regular_wage_yoy":("實質經常性薪資年增率","%","monthly","物價與薪資","檢查平均經常性薪資成長是否跑贏物價。",100),
 "taiex":("台股加權指數","point","daily","台灣","價格指數，不含股利再投資。",7),
 "usd_twd":("美元／新台幣","TWD/USD","daily","台灣","上升表示台幣相對美元貶值。",7),
}
COMPARISONS=[
 {"id":"us10y_taiwan","title":"美國長債與台股","question":"美國長期資金成本變動後，台股是否同向變化？","x_id":"us_treasury_10y_pct","y_id":"taiex","x_label":"美債10年變化（bp）","y_label":"台股區間報酬（%）","x_transform":"bp_change","y_transform":"return_pct","frequency":"daily","alignment":"台股兩端收盤日，各取嚴格早於該日期的最近美國觀測，最多7日；不使用台股當日晚出的美國數字。","minimum_samples":20,"prior":True},
 {"id":"yen_taiwan","title":"日圓與台股","question":"日圓報價變動後，台股是否有共同變化？","x_id":"usd_jpy","y_id":"taiex","x_label":"USD/JPY變化（%）","y_label":"台股區間報酬（%）","x_transform":"return_pct","y_transform":"return_pct","frequency":"daily","alignment":"東京17:00晚於台股收盤，因此各端採嚴格早於台股日期的最近日圓觀測，最多7日。正的X表示日圓貶值。","minimum_samples":20,"prior":True},
 {"id":"us2y_yen","title":"美國短債與日圓","question":"短端美元利率變化與日圓報價如何共同變動？","x_id":"us_treasury_2y_pct","y_id":"usd_jpy","x_label":"美債2年變化（bp）","y_label":"USD/JPY變化（%）","x_transform":"bp_change","y_transform":"return_pct","frequency":"daily","alignment":"同標示日期且兩端皆有觀測才配對；美國收盤晚於東京，僅作事後共變描述，不能當成東京當時已知。","minimum_samples":20,"prior":False},
 {"id":"tw_jp_rates","title":"台日短期資金成本","question":"台灣與日本市場隔夜利率的變動是否一致？","x_id":"tw_overnight_rate","y_id":"boj_overnight_call_pct","x_label":"台灣隔夜變化（bp）","y_label":"日本隔夜變化（bp）","x_transform":"bp_change","y_transform":"bp_change","frequency":"daily","alignment":"同標示日期的實際隔夜市場利率變化；不是政策利差，也未扣跨幣別避險成本。","minimum_samples":20,"prior":False},
 {"id":"prices_wages","title":"物價與薪資購買力","question":"通膨上升時，實質經常性薪資成長如何變化？","x_id":"tw_cpi_yoy","y_id":"tw_real_regular_wage_yoy","x_label":"CPI年增率（%）","y_label":"實質薪資年增率（%）","x_transform":"level","y_transform":"level","frequency":"monthly","alignment":"只配同一觀測月份，最近36個月；不將月資料填成每日。實質薪資本身以CPI平減，存在機械關聯。","minimum_samples":24,"prior":False},
]
BREADTH_CATALOG={
 "twse_advance_pct":("上市上漲家數比例","%","daily","台灣廣度","上漲÷(上漲+下跌+平盤)；TWSE股票欄，不含未成交與無比價。",7),
 "tpex_advance_pct":("上櫃上漲家數比例","%","daily","台灣廣度","TPEx股票及換股權利證書；母體不同，不與上市直接相加。",7),
}
CATALOG.update(BREADTH_CATALOG)
for venue,market,label in (("twse","taiex","上市"),("tpex","tpex_index","上櫃")):
 COMPARISONS.append({"id":venue+"_breadth_return","title":label+"廣度與指數","question":"指數漲跌是否伴隨多數股票同向變化？",
   "x_id":venue+"_advance_pct","y_id":market,"x_label":label+"上漲家數比例（%）","y_label":label+"指數日報酬（%）",
   "x_transform":"level","y_transform":"return_pct","frequency":"daily","group":"positions","minimum_samples":20,
   "prior":False,"reject_flagged":True,"alignment":"同一交易日的上漲家數比例與指數收盤報酬；只描述事後共同變化。上漲比例不需要前一日廣度值；指數仍需相鄰可比收盤。"})
CATALOG.update(POSITION_CATALOG)
COMPARISONS.extend(POSITION_COMPARISONS)
LIMITATIONS=[
 "相關是共同變化，不代表因果；利率採bp變化，股價與匯率採百分比報酬，不用價格水準製造假相關。",
 "美國與東京收盤時點不同。前日期對齊只避免明顯時差前視；歷史資料目前回溯取得、發布版本未知，仍不是point-in-time回測。",
 "交易日缺口以同一市場其他已取得觀測核對；來源同時缺日仍可能無法發現，不宣稱完整官方交易日曆。",
 "逐日結果受假日、缺值與期間選擇影響；rolling採完整配對樣本。月同比有重疊期間與自相關，未作顯著性或預測檢定。",
 "提醒只標記值得檢查的觀測，未觸發不代表安全。門檻是研究設定，沒有校準成危機機率，也不提供買賣建議。"
]
def finite(x):return isinstance(x,(int,float)) and not isinstance(x,bool) and math.isfinite(x)
def pearson(xs,ys,minimum=3):
    if len(xs)!=len(ys) or len(xs)<minimum or not all(finite(x) for x in xs+ys):return None
    mx=sum(xs)/len(xs);my=sum(ys)/len(ys)
    xx=sum((x-mx)**2 for x in xs);yy=sum((y-my)**2 for y in ys)
    if xx<=1e-20 or yy<=1e-20:return None
    return max(-1,min(1,sum((x-mx)*(y-my) for x,y in zip(xs,ys))/math.sqrt(xx*yy)))
def anniversary(day):
    d=date.fromisoformat(day)
    try:return d.replace(year=d.year-1).isoformat()
    except ValueError:return d.replace(year=d.year-1,day=28).isoformat()
def month_number(day):return int(day[:4])*12+int(day[5:7])
def change(a,b,kind):
    if not finite(a) or not finite(b):return None
    if kind=="difference":return b-a
    if kind=="twd_100m":return b/1e8
    if kind=="bp_change":return (b-a)*100
    if kind=="return_pct":return (b/a-1)*100 if a>0 and b>0 else None
    return b
def gap(a,b):return (date.fromisoformat(b)-date.fromisoformat(a)).days
def point_problem(row,sid):
    return observation_problem(row,CATALOG[sid][1] if sid in CATALOG else None)

def analysis_calendar(histories,sid):
    """Known sessions from the same market only; never invent weekday observations."""
    if sid.startswith('us_'):
        members=('us_treasury_2y_pct','us_treasury_10y_pct','us_yield_curve_10y_2y_bp')
    elif sid in ('usd_jpy','boj_overnight_call_pct'):
        members=('usd_jpy','boj_overnight_call_pct')
    elif sid in ('usd_twd','tw_overnight_rate'):
        members=('usd_twd','tw_overnight_rate')
    elif sid in POSITION_CATALOG or sid in BREADTH_CATALOG or sid=='taiex':
        from .positions import calendar_dates
        return calendar_dates(histories,sid)
    else:
        members=(sid,)
    return sorted({p['observation_date'] for member in members for p in histories.get(member,[])})

def interval_problem(a,b,sid,calendar=None):
    from .positions import known_session_between,valid_change
    problem=point_problem(a,sid) or point_problem(b,sid)
    if problem:return problem
    if not 0<gap(a['observation_date'],b['observation_date'])<=7:return 'interval_over_7_days'
    if known_session_between(a['observation_date'],b['observation_date'],calendar):return 'known_trading_session_gap'
    if not valid_change(a,b,calendar):return 'quality_flagged_endpoint'
    return None

def _monthly_rows(rows):
    """Group by observation month; conflicting values in one month are ambiguous."""
    grouped={}
    for p in rows:
        grouped.setdefault(p["observation_date"][:7],[]).append(p)
    return {month:(max(items,key=lambda p:p["observation_date"])
                   if len({p["value"] for p in items})==1 else None)
            for month,items in grouped.items()}

def wage_decomposition(histories,months=36):
    """Nominal wage growth = real growth combined with inflation: (1+g_n)=(1+g_r)(1+pi).

    This is an identity, not evidence of a relationship. Each month shows the three
    official growth rates and the residual left by rounding or by different vintages.
    """
    nominal=_monthly_rows(histories.get("tw_regular_wage_yoy",[]))
    prices=_monthly_rows(histories.get("tw_cpi_yoy",[]))
    real=_monthly_rows(histories.get("tw_real_regular_wage_yoy",[]))
    rows=[]
    for month in sorted(set(nominal)&set(prices)&set(real))[-months:]:
        n,c,r=nominal[month],prices[month],real[month]
        if not n or not c or not r or any(point_problem(p,sid) for p,sid in ((n,"tw_regular_wage_yoy"),(c,"tw_cpi_yoy"),(r,"tw_real_regular_wage_yoy"))):continue
        gn,pi,gr=n["value"],c["value"],r["value"]
        implied=((1+gn/100)/(1+pi/100)-1)*100
        rows.append({"month":month,"nominal_pct":gn,"inflation_pct":pi,"real_pct":gr,"implied_real_pct":implied,
                     "residual_pp":gr-implied,"approx_real_pct":gn-pi})
    latest=rows[-1] if rows else None
    if latest:
        sign="增加" if latest["real_pct"]>0 else "減少" if latest["real_pct"]<0 else "持平"
        summary=(f"{latest['month']}：名目經常性薪資年增 {latest['nominal_pct']:.2f}%，消費者物價年增 {latest['inflation_pct']:.2f}%，"
                 f"扣掉物價後的實質薪資年增 {latest['real_pct']:.2f}%（購買力{sign}）。大致等於名目減物價：{latest['approx_real_pct']:.2f} 個百分點。")
    else:
        summary="三個官方年增率還沒有同月份的完整資料，暫不拆解。"
    return {"rows":rows,"summary":summary,
            "formula":"(1 + 名目薪資年增率) = (1 + 實質薪資年增率) × (1 + 物價年增率)；小幅變動時，實質 ≈ 名目 − 物價。",
            "note":"這是定義上的拆解，不是兩個指標互相影響的證據。殘差來自官方四捨五入或不同版本，超過 0.1 個百分點時請查看來源。"}

def _pairing(histories,pair):
    xrows=histories.get(pair["x_id"],[]);yrows=histories.get(pair["y_id"],[])
    out=[];candidates=[]
    if pair["frequency"]=="monthly":
        x=_monthly_rows(xrows);y=_monthly_rows(yrows)
        for month in sorted(y):
            yp=y[month];xp=x.get(month)
            # Use the target observation's date even when source monthly labels
            # use different days. Never pair different observation months.
            d=yp["observation_date"] if yp else max(p["observation_date"] for p in yrows if p["observation_date"].startswith(month))
            candidate={"date":d,"y_from_date":d,"reason":None}
            problem=next((point_problem(p,sid) for sid,rows in ((pair['x_id'],xrows),(pair['y_id'],yrows))
                          for p in rows if p['observation_date'].startswith(month) and point_problem(p,sid)),None)
            if problem:candidate['reason']=problem
            elif yp is None or month in x and xp is None:candidate["reason"]="ambiguous_month"
            elif xp is None:candidate["reason"]="missing_source_month"
            else:
                a=xp["observation_date"]
                out.append({"date":d,"x":xp["value"],"y":yp["value"],"x_date":a,"y_date":d,"x_from_date":a,"y_from_date":d})
            candidates.append(candidate)
        return out,candidates
    x={p["observation_date"]:p for p in xrows}
    y={p["observation_date"]:p for p in yrows}
    xd=sorted(x);yd=sorted(y);used=set()
    if pair.get("x_transform")=="level" and pair.get("y_transform")=="return_pct" and pair["x_id"] in BREADTH_CATALOG:
        from .positions import flagged,valid_change,calendar_dates
        calendar=calendar_dates(histories,pair["y_id"])
        for before,now in zip(yd,yd[1:]):
            candidate={"date":now,"y_from_date":before,"reason":None};candidates.append(candidate)
            if now not in x:candidate["reason"]="missing_same_date_source_endpoint";continue
            problem=point_problem(x[now],pair['x_id']) or interval_problem(y[before],y[now],pair['y_id'],calendar)
            if problem:candidate['reason']=problem;continue
            value=change(y[before]["value"],y[now]["value"],"return_pct")
            if value is None:candidate["reason"]="invalid_value";continue
            out.append({"date":now,"x":x[now]["value"],"y":value,"x_date":now,"x_from_date":now,"y_date":now,"y_from_date":before})
        return out,candidates
    if pair.get("group")=="positions" and pair["x_transform"]==pair["y_transform"]=="twd_100m":
        # Both are daily flows. A previous observation is not an input and its
        # absence must not remove a valid same-day cash/cash comparison.
        from .positions import flagged
        for now in yd:
            candidate={"date":now,"y_from_date":now,"reason":None};candidates.append(candidate)
            if now not in x:candidate["reason"]="missing_same_date_source_endpoint";continue
            problem=point_problem(x[now],pair['x_id']) or point_problem(y[now],pair['y_id'])
            if problem:candidate['reason']=problem;continue
            out.append({"date":now,"x":x[now]["value"]/1e8,"y":y[now]["value"]/1e8,
                        "x_date":now,"y_date":now,"x_from_date":now,"y_from_date":now})
        return out,candidates
    from .positions import known_session_between
    xcalendar=analysis_calendar(histories,pair['x_id']);ycalendar=analysis_calendar(histories,pair['y_id'])
    for before,now in zip(yd,yd[1:]):
        candidate={"date":now,"y_from_date":before,"reason":None}
        candidates.append(candidate)
        if gap(before,now)>7:
            candidate["reason"]="target_interval_over_7_days";continue
        if pair["prior"]:
            ai=bisect_left(xd,before)-1;bi=bisect_left(xd,now)-1
            if ai<0 or bi<0:
                candidate["reason"]="missing_prior_source_endpoint";continue
            a,b=xd[ai],xd[bi]
            if a==b:
                candidate["reason"]="no_new_source_observation";continue
            if gap(a,before)>7 or gap(b,now)>7:
                candidate["reason"]="source_anchor_over_7_days";continue
        else:
            # A cash flow only needs today's value; it is not a difference.
            source_flow=pair["x_transform"]=="twd_100m"
            if now not in x or (not source_flow and before not in x):
                candidate["reason"]="missing_same_date_source_endpoint";continue
            a,b=(now,now) if source_flow else (before,now)
        if gap(a,b)>7:
            candidate["reason"]="source_interval_over_7_days";continue
        # One transformed source interval is never reused across target sessions.
        if (a,b) in used:
            candidate["reason"]="reused_source_interval";continue
        source_flow=pair["x_transform"]=="twd_100m"
        if known_session_between(before,now,ycalendar) or (not source_flow and known_session_between(a,b,xcalendar)):
            candidate["reason"]="known_trading_session_gap";continue
        # A single-day flow cannot be paired with a multi-session OI change.
        if source_flow and known_session_between(before,now,xcalendar):
            candidate["reason"]="known_trading_session_gap";continue
        source_problem=point_problem(x[b],pair['x_id']) if source_flow else interval_problem(x[a],x[b],pair['x_id'],xcalendar)
        problem=source_problem or interval_problem(y[before],y[now],pair['y_id'],ycalendar)
        if problem:candidate['reason']=problem;continue
        xv=change(x[a]["value"],x[b]["value"],pair["x_transform"])
        yv=change(y[before]["value"],y[now]["value"],pair["y_transform"])
        if xv is None or yv is None:
            candidate["reason"]="invalid_transform";continue
        used.add((a,b))
        out.append({"date":now,"x":xv,"y":yv,"x_date":b,"y_date":now,"x_from_date":a,"y_from_date":before})
    return out,candidates

def paired_samples(histories,pair):
    return _pairing(histories,pair)[0]

def analyse(histories,pair,period=252,window=60):
    samples,candidates=_pairing(histories,pair)
    anchor=sorted(histories.get(pair["y_id"],[]),key=lambda p:p["observation_date"])
    end=anchor[-1]["observation_date"] if anchor else None
    if pair["frequency"]=="monthly":
        samples=[p for p in samples if not end or month_number(end)-month_number(p["date"])<36]
        candidates=[p for p in candidates if not end or 0<=month_number(end)-month_number(p["date"])<36]
        window=12
    elif end:
        if period==252:
            cutoff=anniversary(end);samples=[p for p in samples if p["y_from_date"]>=cutoff]
            candidates=[p for p in candidates if p["y_from_date"]>=cutoff]
        else:
            daily_flows=pair.get("group")=="positions" and pair["x_transform"]==pair["y_transform"]=="twd_100m"
            count=period if daily_flows else period+1
            dates=[p["observation_date"] for p in anchor][-count:]
            cutoff=dates[0] if dates else end;samples=[p for p in samples if p["y_from_date"]>=cutoff]
            candidates=[p for p in candidates if p["y_from_date"]>=cutoff]
    rolling=[]
    for i,p in enumerate(samples):
        sub=samples[max(0,i-window+1):i+1]
        # A monthly window must also contain consecutive months.
        consecutive=pair["frequency"]!="monthly" or all(month_number(b["date"])-month_number(a["date"])==1 for a,b in zip(sub,sub[1:]))
        r=pearson([s["x"] for s in sub],[s["y"] for s in sub],minimum=window) if consecutive else None
        rolling.append({"date":p["date"],"value":r,"n":len(sub)})
    r=pearson([s["x"] for s in samples],[s["y"] for s in samples],minimum=pair["minimum_samples"])
    magnitude=abs(r) if r is not None else None
    strength="無法判定" if magnitude is None else "較強" if magnitude>=.7 else "中等" if magnitude>=.4 else "較弱"
    direction="無法判定" if r is None else "同向" if r>0 else "反向" if r<0 else "無線性關聯"
    if len(samples)<pair["minimum_samples"]:summary=f"目前只有{len(samples)}組完整配對，至少需要{pair['minimum_samples']}組；暫不計算相關。"
    elif r is None:summary="其中一個序列幾乎沒有變化，相關係數無法可靠計算。"
    else:summary=f"這段期間呈{strength}{direction}線性關聯（r={r:.2f}）；切換期間或看滾動圖，檢查這個關係是否穩定。"
    warnings=[pair["alignment"],"分級門檻僅為閱讀輔助：|r| 0.4／0.7；不代表統計顯著性。"]
    if pair["frequency"]=="monthly":warnings.append("實質薪資含CPI作為分母，且年增率共享12個月資料；負相關可能來自公式與重疊期間，不能當作獨立因果證據。")
    excluded={}
    for candidate in candidates:
        if candidate["reason"]:excluded[candidate["reason"]]=excluded.get(candidate["reason"],0)+1
    warnings.append(f"候選{len(candidates)}組、完整配對{len(samples)}組、排除{len(candidates)-len(samples)}組。跨國假日、端點缺值與7日門檻都可能減少配對；不代表每個缺口都是資料故障。")
    mechanical=pair["id"]=="prices_wages"
    if mechanical and r is not None:
        summary=f"這組序列共享CPI公式，r={r:.2f}只描述{len(samples)}個月的共同變化；機械關聯與重疊同比期間使它不能作為獨立的因果證據。"
    return {"pair_id":pair["id"],"candidate_count":len(candidates),"excluded_count":len(candidates)-len(samples),"exclusion_reasons":excluded,"mechanical_relationship":mechanical,"sample_count":len(samples),"correlation":r,"strength":strength,"direction":direction,
            "summary":summary,"samples":samples,"rolling":rolling,"window":window,
            "decomposition":wage_decomposition(histories) if mechanical else None,
            "start":samples[0]["date"] if samples else None,"end":samples[-1]["date"] if samples else None,"warnings":warnings}
def indicator(sid,rows,source_states,today):
    label,unit,frequency,group,description,limit=CATALOG[sid]
    latest=rows[-1] if rows else None; delta=None
    if len(rows)>1 and not point_problem(rows[-1],sid) and not point_problem(rows[-2],sid):
        kind="bp_change" if sid not in BREADTH_CATALOG and unit=="%" and frequency in ("daily","event") else "return_pct" if unit in ("point","TWD/USD","JPY/USD") else "level"
        a,b=rows[-2],rows[-1]
        amount=change(a["value"],b["value"],kind) if kind!="level" else b["value"]-a["value"]
        delta={"value":amount,"unit":"bp" if kind=="bp_change" else "%" if kind=="return_pct" else "百分點" if unit=="%" else unit,"from_date":a["observation_date"]}
    if latest:
        source=source_states.get(latest["source_id"],{})
        if frequency=="event":
            stamp=source.get("last_success_at")
            try:
                checked=datetime.fromisoformat(stamp.replace("Z","+00:00")) if stamp else None
                checked=checked.astimezone(timezone(timedelta(hours=8))) if checked and checked.tzinfo else checked
                age=(today-checked.date()).days if checked else None
            except (ValueError,TypeError):age=None
            stale=age is None or age<0 or age>limit
            note="政策事件持續有效；新鮮度看最近成功查核日，並非生效日距今天多久。"
        else:
            age=(today-date.fromisoformat(latest["observation_date"])).days
            stale=age>limit
            note=f"依觀測期末超過{limit}日曆日提示；完整官方發布行事曆尚未接入。"
        if source_states.get(latest["source_id"],{}).get("status")=="error":
            stale=True
            note+=" 最近來源更新失敗，保留舊觀測並暫停提醒判讀。"
    else:age=None;stale=True;note="尚未取得可驗證資料。"
    return {"id":sid,"label":label,"unit":latest["unit"] if latest else unit,"frequency":frequency,"group":group,"description":description,
            "latest":latest,"change":delta,"coverage":{"start":rows[0]["observation_date"] if rows else None,"end":latest["observation_date"] if latest else None,"count":len(rows)},
            "freshness":{"stale":stale,"age_days":age,"limit_days":limit,"note":note}}
def transformed(rows,kind,sid=None,calendar=None):
    output=[]
    sid=sid or (rows[0].get('series_id') if rows else None)
    for a,b in zip(rows,rows[1:]):
        if interval_problem(a,b,sid,calendar):continue
        v=change(a["value"],b["value"],kind)
        if v is not None:output.append((b,v))
    return output
def robust_baseline(values,current,minimum=40):
    values=values[-60:]
    if len(values)<minimum or not finite(current) or not all(finite(v) for v in values):return None
    center=median(values);mad=median([abs(v-center) for v in values])
    return {"n":len(values),"median":center,"mad":mad,"z":.6745*(current-center)/mad if mad>1e-12 else None}
def alerts_for(histories,indicators,settings,acks):
    meta={p["id"]:p for p in indicators};out=[]
    def emit(rule,title,status,obs,value,unit,threshold,explanation,method,sids,baseline=None):
        fingerprint={"rule":rule,"date":obs,"value":value,"threshold":threshold,"z":round(baseline["z"],8) if baseline and baseline["z"] is not None else None,"version":MODEL_VERSION}
        ident=hashlib.sha256(json.dumps(fingerprint,sort_keys=True).encode()).hexdigest()[:24]
        out.append({"id":ident,"rule_id":rule,"title":title,"status":status,"severity":"watch" if status=="triggered" else "info",
            "observation_date":obs,"value":value,"unit":unit,"threshold":threshold,"baseline":baseline,
            "explanation":explanation,"method":method,"source_ids":sids,"acknowledged":bool(acks.get(ident))})
    for sid,kind,threshold in [
        ("us_treasury_10y_pct","bp_change",settings["rate_move_bp"]),
        ("us_treasury_2y_pct","bp_change",settings["rate_move_bp"]),
        ("boj_overnight_call_pct","bp_change",settings["rate_move_bp"]),
        ("tw_overnight_rate","bp_change",settings["rate_move_bp"]),
        ("usd_jpy","return_pct",settings["fx_move_pct"]),
        ("usd_twd","return_pct",settings["fx_move_pct"]),
    ]:
        series=histories.get(sid,[]);changes=transformed(series,kind,sid,analysis_calendar(histories,sid))
        last_point,value=changes[-1] if changes else (None,None)
        current=series[-1] if series else None
        baseline=None
        label=CATALOG[sid][0];unit="bp" if kind=="bp_change" else "%"
        # Missing most recent endpoints must never cause an old move to masquerade as today's signal.
        valid_latest=current and last_point and current["observation_date"]==last_point["observation_date"]
        baseline=robust_baseline([v for _,v in changes[:-1]],value) if valid_latest else None
        state="insufficient" if not valid_latest else "stale" if meta[sid]["freshness"]["stale"] else "triggered" if abs(value)>=threshold else "normal"
        emit(sid+":move",label+"單期變動",state,current["observation_date"] if current else None,value if valid_latest else None,unit,
             f"|變動| ≥ {threshold:g} {unit}","利率變動會改變借款與折現成本；匯率變動會改變未避險部位價值。這不保證股市漲跌。",
             "比較相鄰可用觀測；阻擋旗標、單位錯誤、已知營業日缺口或間隔超過7日均不計。休假日不補值；門檻是研究設定。",[sid],baseline)
        z=baseline["z"] if baseline else None
        state_z="insufficient" if not valid_latest else "stale" if meta[sid]["freshness"]["stale"] else "insufficient" if z is None else "triggered" if abs(z)>=settings["z_threshold"] else "normal"
        emit(sid+":unusual",label+"偏離近期常態",state_z,current["observation_date"] if current else None,z if valid_latest else None,"modified z",
             f"|modified z| ≥ {settings['z_threshold']:g}","與自身最近60次變動比較，提示需要檢查的觀測；不是崩盤機率，也不把異常值直接刪除。",
             "0.6745 × (本次變動−過去中位數) / 過去MAD；至少40筆、排除本次。MAD為零或樣本不足就不判定。",[sid],baseline)
    from .quality import blocking_quality
    for sid in BREADTH_CATALOG:
        row=(histories.get(sid) or [None])[-1]
        state="insufficient" if point_problem(row,sid) else "stale" if meta[sid]["freshness"]["stale"] else "triggered" if row["value"]<20 else "normal"
        emit(sid+":low",CATALOG[sid][0]+"偏低",state,row["observation_date"] if row else None,row["value"] if row and state!="insufficient" else None,"%",
            "上漲家數比例 < 20%","當天上漲股票相對少；顯示下跌的廣泛程度，不能判定明天漲跌或推論資金流出量。",
            "上漲÷(上漲+下跌+平盤)×100；固定研究門檻20%，未成交及無比價不列入分母。",[sid])
    wage=histories.get("tw_real_regular_wage_yoy",[])
    contiguous=(len(wage)>=2 and month_number(wage[-1]["observation_date"])-month_number(wage[-2]["observation_date"])==1
                and not any(point_problem(p,'tw_real_regular_wage_yoy') for p in wage[-2:]))
    state="insufficient" if not contiguous else "stale" if meta["tw_real_regular_wage_yoy"]["freshness"]["stale"] else "triggered" if all(p["value"]<0 for p in wage[-2:]) else "normal"
    emit("real_wage:negative","實質經常性薪資連兩月負成長",state,wage[-1]["observation_date"] if wage else None,wage[-1]["value"] if wage and state!="insufficient" else None,"%",
         "連續2個月同比 < 0%","平均經常性薪資的購買力比去年同月低；不能外推為每個人的所得都下降。",
         "需要兩個連續觀測月份；採同口徑官方或明示衍生的實質經常性薪資同比。",["tw_real_regular_wage_yoy"])
    spread=histories.get("us_yield_curve_10y_2y_bp",[])
    state="insufficient" if not spread or point_problem(spread[-1],"us_yield_curve_10y_2y_bp") else "stale" if meta["us_yield_curve_10y_2y_bp"]["freshness"]["stale"] else "triggered" if spread[-1]["value"]<0 else "normal"
    emit("yield_curve:inverted","美債殖利率曲線倒掛",state,spread[-1]["observation_date"] if spread else None,spread[-1]["value"] if spread and state!="insufficient" else None,"bp",
         "10年−2年 < 0 bp","短期殖利率高於長期可能反映政策與景氣預期，但不能單獨判定衰退時間。",
         "僅使用相同日期、相同Treasury平價曲線口徑的10年與2年殖利率。",["us_yield_curve_10y_2y_bp"])
    for sid in CATALOG:
        if sid not in ("taiex","usd_twd") and meta[sid]["freshness"]["stale"]:
            row=meta[sid]["latest"]
            emit(sid+":freshness",CATALOG[sid][0]+"資料待查核","stale",row["observation_date"] if row else None,meta[sid]["freshness"]["age_days"],"日",
                 f"門檻 {meta[sid]['freshness']['limit_days']} 日曆日","資料不足或日期落後，不能把沒有提醒當成市場平穩。",
                 meta[sid]["freshness"]["note"],[sid])
    from .positions import alerts as position_alerts
    out.extend(position_alerts(histories,meta,settings,acks))
    for alert in out:
        if alert.get("group") is None:
            alert["group"]="positions" if any(sid in POSITION_CATALOG or sid in BREADTH_CATALOG for sid in alert["source_ids"]) else "global"
    return out
def build(histories,sources,pair_id="us10y_taiwan",period=252,window=60,settings=None,acks=None,today=None):
    if period not in (20,60,120,252) or window not in (20,60,120):raise ValueError("Unsupported analysis window")
    pair=next((p for p in COMPARISONS if p["id"]==pair_id),None)
    if pair is None:raise ValueError("Unknown comparison")
    today=today or datetime.now(timezone(timedelta(hours=8))).date()
    # Sorted snapshots make results independent of upstream row ordering.
    data={sid:sorted([p for p in histories.get(sid,[]) if p["observation_date"]<=today.isoformat()],
                     key=lambda p:p["observation_date"]) for sid in dict.fromkeys((*CATALOG,*CALENDAR_INPUT_IDS))}
    scheduled_events={sid:sorted([p for p in histories.get(sid,[]) if p["observation_date"]>today.isoformat()],
                                key=lambda p:p["observation_date"])
                      for sid in CATALOG if CATALOG[sid][2]=="event"}
    indicators=[indicator(sid,data[sid],{s["id"]:s for s in sources},today) for sid in CATALOG]
    for item in indicators:
        sid=item['id'];rows=data[sid]
        current_problem=point_problem(rows[-1],sid) if rows else 'missing_observation'
        item['analysis_eligibility']={'usable':current_problem is None,'reason':current_problem}
        problem=current_problem
        if len(rows)>1:
            if CATALOG[sid][2]=='daily':problem=interval_problem(rows[-2],rows[-1],sid,analysis_calendar(data,sid))
            else:
                problem=point_problem(rows[-2],sid) or current_problem
                if CATALOG[sid][2]=='monthly' and month_number(rows[-1]['observation_date'])-month_number(rows[-2]['observation_date'])!=1:
                    problem=problem or 'missing_adjacent_month'
        if problem:
            item['change']=None
            item['change_unavailable_reason']='品質旗標、單位不符、缺少連續已知日期或餘額修訂不連續；保留原值供查核，不計算變動。'
            item['change_exclusion_code']=problem
    settings={**DEFAULT_SETTINGS,**(settings or {})}
    return {"as_of":datetime.now(timezone.utc).isoformat(),"model_version":MODEL_VERSION,"indicators":indicators,"histories":data,"scheduled_events":scheduled_events,
        "comparisons":[{k:v for k,v in p.items() if k!="prior"} for p in COMPARISONS],
        "analysis":analyse(data,pair,period,window),"alerts":alerts_for(data,indicators,settings,acks or {}),
        "alert_settings":settings,"limitations":LIMITATIONS}
