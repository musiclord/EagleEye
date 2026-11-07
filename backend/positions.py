"""Cash / futures / credit observations. No inference of a single investor or total leverage."""
from datetime import date,datetime,timedelta,timezone
import hashlib,json,math
from bisect import bisect_right

MODEL_VERSION="positions-2"
POSITION_DEFAULTS={"oi_move_contracts":5000.0,"margin_move_pct":2.0,"short_move_pct":10.0}
# Catalog tuple is shared with transmission.indicator; each value keeps its own unit.
POSITION_CATALOG={
 "tpex_index":("櫃買指數","point","daily","台灣部位","上櫃價格指數；不含股利，與上市市場分開觀察。",7),
 "foreign_net":("上市外資現貨買賣超","TWD","daily","台灣部位","當日買進減賣出，含ETF等有價證券；不是匯出入。",7),
 "tpex_foreign_net":("上櫃外資現貨買賣超","TWD","daily","台灣部位","上櫃有價證券外資及陸資淨交易，與上市範圍不同。",7),
 "margin_balance":("上市融資金額餘額","TWD","daily","台灣部位","未償還信用交易借款；不涵蓋股票質借、信貸與場外借款。",7),
 "tpex_margin_balance":("上櫃融資金額餘額","TWD","daily","台灣部位","上櫃信用交易借款餘額；金額不等於融資維持率。",7),
 "twse_short_balance":("上市融券餘額","trading_unit","daily","台灣部位","官方交易單位，不一律換算1000股；不是全部借券賣出。",7),
 "tpex_short_balance":("上櫃融券餘額","lot","daily","台灣部位","官方張數；券償、到期與公司行動也可能改變餘額。",7),
}
for owner,label in (("foreign","外資及陸資"),("trust","投信"),("dealer","自營商")):
 for leg,name in (("long","多方"),("short","空方"),("net","多減空淨")):
  POSITION_CATALOG[f"tx_{owner}_oi_{leg}"]=(f"TX {label}{name}未平倉","contract","daily","台灣部位",
    "標準臺股期貨TX、所有到期月份；是未平倉存量，不是當日交易量。類別含多個機構與避險。",7)

def comparison(id,title,question,x,y,xlabel,ylabel,xt,yt,prior=False):
 return {"id":id,"title":title,"question":question,"x_id":x,"y_id":y,"x_label":xlabel,"y_label":ylabel,
  "x_transform":xt,"y_transform":yt,"frequency":"daily","group":"positions","minimum_samples":20,"prior":prior,"reject_flagged":True,
  "alignment":("台股區間兩端各取嚴格早於當日的TX觀測；日盤13:45與盤後發布晚於現貨收盤，只能避免明顯時差前視。"
     if prior else "同標示日期完整端點配對，只描述事後共同變化；現貨、期貨收盤和發布時點不同，不作盤中預測。")
     +" 超過7日、品質旗標、缺少端點或已知交易日缺口均排除；休市日不補值。"}

POSITION_COMPARISONS=[
 comparison("cash_futures","外資現貨與期貨調整","外資現貨買賣超，和期貨淨未平倉的改變是否同向？",
  "foreign_net","tx_foreign_oi_net","上市外資買賣超（億元）","TX外資淨未平倉變化（口）","twd_100m","difference"),
 comparison("cash_markets","上市與上櫃外資","外資在上市與上櫃市場的當日交易是否同向？",
  "foreign_net","tpex_foreign_net","上市外資買賣超（億元）","上櫃外資買賣超（億元）","twd_100m","twd_100m"),
 comparison("markets_return","上市與上櫃漲跌","大型股為主的上市市場，與上櫃市場是否共同變化？",
  "taiex","tpex_index","台股加權報酬（%）","櫃買指數報酬（%）","return_pct","return_pct"),
 comparison("margin_market","上市融資與股價","信用借款餘額變動，是否和上市股價變動同向？",
  "margin_balance","taiex","上市融資餘額變動（%）","台股加權報酬（%）","return_pct","return_pct"),
 comparison("tpex_margin_market","上櫃融資與股價","上櫃市場的信用借款與股價如何共同變化？",
  "tpex_margin_balance","tpex_index","上櫃融資餘額變動（%）","櫃買指數報酬（%）","return_pct","return_pct"),
 comparison("futures_next_cash","較早期貨部位與台股","較早公布的外資期貨淨部位變化，與後續現貨報酬是否相關？",
  "tx_foreign_oi_net","taiex","較早TX外資淨未平倉變化（口）","台股加權區間報酬（%）","difference","return_pct",True),
]
POSITION_NOTES=[
 "現貨買賣超是流量，期貨未平倉與融資融券餘額是存量。比較變化時仍保留億元、口、交易單位／張，不把它們相加成總資金。",
 "TX只含標準臺股期貨，未加入小型、微型、選擇權及海外期貨；多、空都可能是避險或套利，三類法人不是單一交易人。",
 "融券與借券賣出不同；信用交易借款不能代表全體家庭入市資金、總槓桿或融資維持率。",
 "日盤、夜盤歸屬與發布時間各有差異。所有同日比較皆為事後描述，未取得歷史發布版本前不能作嚴格回測。",
 "到期結算、轉倉、除權息、現金償還與現券償還都會影響部位。異常只提示查核，不自動判定追繳或踩踏。",
 "缺口檢核使用已取得的上市、上櫃或期貨交易日期；若相關來源同時漏掉同一天，尚不能證明日曆完整。",
]
EXTRA_SERIES=["market_turnover","tpex_turnover","taiex","tpex_trust_net","tpex_dealer_prop_net","tpex_dealer_hedge_net",
 "trust_net","dealer_prop_net","dealer_hedge_net","twse_margin_units_balance","tpex_margin_units_balance",
 "twse_margin_balance_change","tpex_margin_balance_change","twse_short_balance_change","tpex_short_balance_change",
 "tx_market_oi_total","tx_foreign_oi_share_pct"]
for owner in ("foreign","trust","dealer"):EXTRA_SERIES.append(f"tx_{owner}_oi_net_notional")
def flagged(row):
 from .quality import blocking_quality
 return blocking_quality(row)
# Dates from independently reported observations are evidence of a session. A
# weekend/holiday absent from all reports is NOT inferred to be a missing row.
# TX follows the Taiwan equity trading calendar; venue-specific OTC reports stay
# separate. These are observed calendars, not a claim of complete official dates.
CALENDAR_SERIES = {
 "twse": ("taiex", "market_turnover", "foreign_net", "margin_balance", "twse_short_balance"),
 "tpex": ("tpex_index", "tpex_turnover", "tpex_foreign_net", "tpex_margin_balance", "tpex_short_balance"),
 "tx": ("taiex", "tx_market_oi_total") + tuple(f"tx_{owner}_oi_{leg}"
         for owner in ("foreign", "trust", "dealer") for leg in ("long", "short", "net")),
}
CALENDAR_INPUT_IDS = tuple(dict.fromkeys(sid for ids in CALENDAR_SERIES.values() for sid in ids))

def calendar_dates(histories,sid):
 venue="tx" if sid.startswith("tx_") else "tpex" if sid.startswith("tpex_") else "twse"
 return sorted({p["observation_date"] for member in CALENDAR_SERIES[venue]
                for p in histories.get(member,[])})

def known_session_between(start,end,calendar):
 """Use published session dates, never weekdays or a guessed holiday calendar."""
 if not calendar:return False
 i=bisect_right(calendar,start)
 return i<len(calendar) and calendar[i]<end

def valid_change(a,b,calendar=None):
 from .quality import observation_problem
 expected=POSITION_CATALOG.get(a.get('series_id'),('',None))[1]
 if a.get('series_id')=='taiex':expected='point'
 if observation_problem(a,expected) or observation_problem(b,expected):return False
 previous=(b.get("metadata") or {}).get("previous_reported_balance")
 if not all(isinstance(v,(int,float)) and not isinstance(v,bool) and math.isfinite(v) for v in (a["value"],b["value"])):return False
 continuity=previous is None or (isinstance(previous,(int,float)) and math.isfinite(previous)
                                and math.isclose(previous,a["value"],rel_tol=0,abs_tol=1e-8))
 first,last=a["observation_date"],b["observation_date"]
 return (continuity and 0<(date.fromisoformat(last)-date.fromisoformat(first)).days<=7
         and not flagged(a) and not flagged(b) and not known_session_between(first,last,calendar))
def alerts(histories,meta,settings,acks):
 from .transmission import change,robust_baseline,finite
 from .quality import observation_problem
 settings={"z_threshold":3.5,**POSITION_DEFAULTS,**(settings or {})}
 out=[]
 def emit(sid,kind,title,point,value,unit,threshold,method,explanation,baseline,state):
  identity={"model":MODEL_VERSION,"rule":sid+":"+kind,"date":point["observation_date"] if point else None,"value":value,
    "threshold":threshold,"z":round(baseline["z"],8) if baseline and baseline["z"] is not None else None}
  ident=hashlib.sha256(json.dumps(identity,sort_keys=True).encode()).hexdigest()[:24]
  out.append({"id":ident,"rule_id":"positions:"+sid+":"+kind,"title":title,"group":"positions",
    "status":state,"severity":"watch" if state=="triggered" else "info","observation_date":identity["date"],
    "value":value,"unit":unit,"threshold":threshold,"baseline":baseline,"method":method,"explanation":explanation,
    "source_ids":[sid],"acknowledged":bool(acks.get(ident))})
 specs=[("foreign_net","twd_100m",None,"億元"),("tpex_foreign_net","twd_100m",None,"億元"),
  ("tx_foreign_oi_net","difference",settings["oi_move_contracts"],"口"),
  ("margin_balance","return_pct",settings["margin_move_pct"],"%"),("tpex_margin_balance","return_pct",settings["margin_move_pct"],"%"),
  ("twse_short_balance","return_pct",settings["short_move_pct"],"%"),("tpex_short_balance","return_pct",settings["short_move_pct"],"%")]
 for sid,kind,threshold,unit in specs:
  rows=histories.get(sid,[]);current=rows[-1] if rows else None;values=[]
  calendar=calendar_dates(histories,sid)
  if kind=="twd_100m":
   values=[(p,p["value"]/1e8) for p in rows if not observation_problem(p,POSITION_CATALOG[sid][1])]
  else:
   for a,b in zip(rows,rows[1:]):
    if valid_change(a,b,calendar):
     value=change(a["value"],b["value"],kind)
     if value is not None:values.append((b,value))
  point,value=values[-1] if values else (None,None)
  valid=bool(current and point and current["observation_date"]==point["observation_date"])
  base=robust_baseline([v for _,v in values[:-1]],value) if valid else None
  state="insufficient" if not valid else "stale" if meta[sid]["freshness"]["stale"] else None
  name=POSITION_CATALOG[sid][0]
  explanation=("期貨淨部位變動可能來自避險、套利、到期結算與轉倉，不能直接當成外資現金匯出入。" if sid.startswith("tx_") else
   "信用餘額也會受現金／現券償還、公司行動與修訂影響，不能據此推定被迫平倉或融資維持率。" if "balance" in sid else
   "買賣超只反映該市場當日淨交易，不能識別個別機構，也不等於跨境匯出入。")
  if threshold is not None:
   emit(sid,"move",name+"單期變動",current,value if valid else None,unit,f"|變動| ≥ {threshold:g} {unit}",
    "相鄰已知交易日觀測變動；中間已有交易日但缺資料、超過7日或端點有品質旗標不判讀。到期與轉倉仍須另查。",explanation,base,
    state or ("triggered" if abs(value)>=threshold else "normal"))
  z=base["z"] if base else None
  emit(sid,"unusual",name+"偏離近期常態",current,z if valid else None,"modified z",f"|modified z| ≥ {settings['z_threshold']:g}",
   "與自身前60次有效觀測／變動比較，至少40次、排除本次；0.6745×(本次−中位數)/MAD，MAD為零不判讀。",
   explanation,base,state or ("insufficient" if z is None else "triggered" if abs(z)>=settings["z_threshold"] else "normal"))
 return out

def overview(histories,sources,period=252,today=None):
 from .transmission import indicator,anniversary
 if period not in (20,60,120,252):raise ValueError("Unsupported observation window")
 today=today or datetime.now(timezone(timedelta(hours=8))).date()
 ids=list(POSITION_CATALOG)+EXTRA_SERIES
 full={sid:sorted([p for p in histories.get(sid,[]) if p["observation_date"]<=today.isoformat()],key=lambda p:p["observation_date"]) for sid in ids}
 data={}
 for sid,rows in full.items():
  if period==252 and rows:
   cutoff=anniversary(rows[-1]["observation_date"]);data[sid]=[p for p in rows if p["observation_date"]>=cutoff]
  else:data[sid]=rows[-period-1:]
 states={s["id"]:s for s in sources}
 indicators=[indicator(sid,full[sid],states,today) for sid in POSITION_CATALOG]
 for item in indicators:
  rows=full[item["id"]]
  if len(rows)>1 and not valid_change(rows[-2],rows[-1],calendar_dates(full,item["id"])):
   item["change"]=None
   item["change_unavailable_reason"]="缺少連續已知交易日、品質旗標或餘額修訂不連續；不計算變動。"
 coverage=[]
 for i in indicators:
  sid=i["id"];cal="tpex_index" if sid.startswith("tpex_") else "tx_market_oi_total" if sid.startswith("tx_") else "taiex"
  reference={p["observation_date"] for p in data.get(cal,[])};observed={p["observation_date"] for p in data[sid]}
  coverage.append({"id":sid,"label":i["label"],"count":len(observed),"first":min(observed) if observed else None,"last":max(observed) if observed else None,
   "calendar":cal,"expected":len(reference) if reference else None,"missing_dates":sorted(reference-observed),
   "quality_flags":sum(flagged(p) for p in data[sid])})
 cash={p["observation_date"]:p for p in full.get("foreign_net",[])}
 future=full.get("tx_foreign_oi_net",[]);joint=None
 joint_calendar=sorted(set(calendar_dates(full,"tx_foreign_oi_net"))|set(calendar_dates(full,"foreign_net")))
 for a,b in reversed(list(zip(future,future[1:]))):
  d=b["observation_date"]
  if d not in cash or flagged(cash[d]) or not math.isfinite(cash[d]["value"]) or not valid_change(a,b,joint_calendar):continue
  flow=cash[d]["value"];delta=b["value"]-a["value"]
  joint={"date":d,"cash_flow":flow,"oi_net":b["value"],"oi_change":delta,"oi_from_date":a["observation_date"],
    "cash_direction":"買超" if flow>0 else "賣超" if flow<0 else "持平",
    "futures_direction":"淨多增加／淨空減少" if delta>0 else "淨多減少／淨空增加" if delta<0 else "淨部位不變",
    "stale":(today-date.fromisoformat(d)).days>7 or any(states.get(p.get("source_id"),{}).get("status")=="error" for p in (cash[d],b)),
    "note":"同日事後對照；不代表兩組數字來自同一批法人，更不能把億元與口數相加。"}
  break
 return {"as_of":datetime.now(timezone.utc).isoformat(),"model_version":MODEL_VERSION,"histories":data,"indicators":indicators,
   "coverage":coverage,"joint_observation":joint,"limitations":POSITION_NOTES}
