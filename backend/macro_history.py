"""Official monthly Taiwan goods trade and domestic turnover adapters.

All dates are reporting-period ends, never download/publication dates. These
are nominal, unadjusted amounts and a latest-vintage history, not a real-time
as-published backtest. The ingestion engine owns fetching and persistence.
"""
from __future__ import annotations

import calendar
import csv
import hashlib
import io
import re
from datetime import date
from decimal import Decimal
from functools import partial

from .connectors import Source, SchemaError, number
from .contracts import Job, make_point

EXPORT_URL = 'https://service.mof.gov.tw/public/data/statistic/trade/u2010ex.csv'
IMPORT_URL = 'https://service.mof.gov.tw/public/data/statistic/trade/u2010im.csv'
DEMAND_URL = 'https://service.moea.gov.tw/EE520/opendata/ea.csv'
METHOD_URL = 'https://web02.mof.gov.tw/njswww/webMain.aspx?funid=i7000&kind=10&sys=100&type=1'
SOURCES = (
    Source('mof_exports', '財政部：貨品出口與國家地區', EXPORT_URL,
           '月資料，含復出口；出口 FOB、已知最終目的地。原始千美元轉美元；名目、未季調，歷史可能修訂。逐期發布日未知。', 'monthly', 75),
    Source('mof_imports', '財政部：貨品進口與國家地區', IMPORT_URL,
           '月資料，含復進口；進口 CIF、原生產國。原始千美元轉美元；名目、未季調，歷史可能修訂。逐期發布日未知。', 'monthly', 75),
    Source('moea_demand', '經濟部：零售、餐飲及批發營業額', DEMAND_URL,
           '月資料，千元轉新台幣元；名目且未季調。零售與餐飲是消費相關指標，非實質民間消費或整體內需；批發含外銷。逐期發布日未知。', 'monthly', 75),
)
DERIVED_SOURCE = Source('macro_derived', '本地計算：貿易差額與同月年增率', METHOD_URL,
    '只用同月份的同範圍、同幣別資料；年增率精確對齊去年同月，不補值。最新修訂資料不代表當時可取得資訊。', 'monthly', 75)

SERIES = {
    'exports_usd': ('貨品出口總值（含復出口）', 'USD'),
    'imports_usd': ('貨品進口總值（含復進口）', 'USD'),
    'trade_balance_usd': ('貨品貿易差額（出口減進口）', 'USD'),
    'retail_sales_twd': ('零售業營業額（名目、未季調）', 'TWD'),
    'food_services_sales_twd': ('餐飲業營業額（名目、未季調）', 'TWD'),
    'wholesale_sales_twd': ('批發業營業額（名目、未季調）', 'TWD'),
}
for _base in ('exports_usd', 'imports_usd', 'retail_sales_twd',
              'food_services_sales_twd', 'wholesale_sales_twd'):
    SERIES[_base + '_yoy'] = (SERIES[_base][0] + '年增率', '%')

# Reviewed MOF column names, independent of column position and suffix spelling.
# A new or renamed geography requires review so an aggregate is never silently
# treated as a country/region. Geography IDs derive from names, not positions.
TRADE_COLUMNS = ('總計', '亞洲', '中國大陸', '香港', '日本', '韓國', '新加坡', '馬來西亞', '印尼', '越南', '泰國', '菲律賓', '印度', '孟加拉', '巴基斯坦', '澳門', '亞塞拜然', '柬埔寨', '斯里蘭卡', '緬甸', '不丹', '汶萊', '尼泊爾', '寮國', '北韓', '哈薩克', '烏茲別克', '喬治亞', '亞美尼亞', '土庫曼', '塔吉克', '吉爾吉斯', '東帝汶', '中東', '沙烏地阿拉伯', '科威特', '阿拉伯聯合大公國', '伊朗', '伊拉克', '土耳其', '卡達', '阿曼', '以色列', '巴林', '阿富汗', '塞普勒斯', '約旦', '黎巴嫩', '敘利亞', '葉門', '歐洲', '德國', '荷蘭', '英國', '義大利', '法國', '俄羅斯', '西班牙', '瑞士', '比利時', '芬蘭', '瑞典', '奧地利', '波蘭', '丹麥', '愛爾蘭', '匈牙利', '葡萄牙', '捷克', '挪威', '斯洛伐克', '烏克蘭', '希臘', '阿爾巴尼亞', '保加利亞', '教廷', '冰島', '盧森堡', '馬爾他', '摩納哥', '羅馬尼亞', '塞爾維亞', '蒙特內哥羅共和國', '北馬其頓', '波士尼亞與赫塞哥維納', '克羅埃西亞', '斯洛維尼亞', '白俄羅斯', '摩爾多瓦', '立陶宛', '愛沙尼亞', '拉脫維亞', '北美洲', '美國', '加拿大', '中美洲', '墨西哥', '巴拿馬', '哥斯大黎加', '巴貝多', '巴哈馬', '安圭拉', '安地卡及巴布達', '貝里斯', '多明尼克', '多明尼加', '薩爾瓦多', '瓜地馬拉', '格瑞那達', '海地', '宏都拉斯', '牙買加', '蒙瑟拉特', '尼加拉瓜', '玻多黎各', '聖露西亞', '聖文森', '千里達及托巴哥', '聖克里斯多福及尼維斯', '古巴', '南美洲', '巴西', '智利', '秘魯', '阿根廷', '哥倫比亞', '玻利維亞', '厄瓜多', '法屬圭亞那', '蓋亞那', '巴拉圭', '蘇利南', '烏拉圭', '委內瑞拉', '大洋洲', '澳大利亞', '紐西蘭', '薩摩亞(美屬)', '斐濟', '吉里巴斯', '馬爾地夫', '諾魯', '巴布亞新幾內亞', '紐克里多尼亞', '萬那杜', '所羅門島', '大溪地', '東加', '吐瓦魯', '薩摩亞', '非洲', '南非', '安哥拉', '赤道幾內亞', '剛果', '奈及利亞', '埃及', '賴比瑞亞', '阿爾及利亞', '波札那', '蒲隆地', '維德角', '喀麥隆', '中非共和國', '查德', '剛果民主共和國', '葛摩聯盟', '貝南', '衣索比亞', '厄利垂亞', '吉布地', '加彭', '甘比亞', '迦納', '幾內亞', '幾內亞比索', '象牙海岸', '肯亞', '賴索托', '利比亞', '馬達加斯加', '馬拉威', '馬利', '茅利塔尼亞伊斯蘭共和國', '摩洛哥', '模里西斯', '莫三比克', '納米比亞', '尼日', '盧安達', '辛巴威', '聖多美普林西比', '塞內加爾', '獅子山', '索馬利亞', '蘇丹', '史瓦帝尼', '坦尚尼亞', '多哥', '突尼西亞', '烏干達', '布吉那法索', '尚比亞')
AGGREGATES = frozenset(('總計', '亞洲', '中東', '歐洲', '北美洲', '中美洲', '南美洲', '大洋洲', '非洲'))
DEMAND_FIELDS = ('統計項目', '行業代碼', '行業別', '資料期(民國年)', '統計值(金額)', '計量單位')
INDUSTRIES = {
    '47_48': ('零售業', 'retail_sales_twd'),
    '56': ('餐飲業', 'food_services_sales_twd'),
    '45_46': ('批發業', 'wholesale_sales_twd'),
}


def partner_id(direction: str, name: str) -> str:
    if direction not in ('exports', 'imports') or name in AGGREGATES:
        raise SchemaError('Invalid trade direction or aggregate partner')
    return f'{direction}_partner_' + hashlib.sha256(name.encode('utf-8')).hexdigest()[:10]


def _month_end(year: int, month: int) -> date:
    try:
        return date(year, month, calendar.monthrange(year, month)[1])
    except (ValueError, OverflowError) as exc:
        raise SchemaError('Invalid calendar month') from exc


def _metadata(observed: date, **extra) -> dict:
    return {'frequency': 'monthly', 'period_start': observed.replace(day=1).isoformat(),
            'period_end': observed.isoformat(), 'price_basis': 'nominal',
            'seasonally_adjusted': False, 'release_date_known': False,
            'vintage': 'latest_available_revision', **extra}


def _csv(raw: bytes, encoding: str) -> list[list[str]]:
    try:
        rows = list(csv.reader(io.StringIO(raw.decode(encoding))))
    except (UnicodeError, csv.Error) as exc:
        raise SchemaError('Invalid source CSV encoding or syntax') from exc
    rows = [row for row in rows if row and any(x.strip() for x in row)]
    if len(rows) < 2:
        raise SchemaError('Empty monthly source')
    return rows


def _amount(value: str) -> Decimal:
    result = number(value)
    if result < 0:
        raise SchemaError('Negative reported turnover/trade amount')
    return result * 1000


def parse_trade(raw: bytes, direction: str, start: date, end: date) -> list[dict]:
    """Parse total and partner amounts from MOF's same-scope wide monthly CSV."""
    if direction not in ('exports', 'imports'):
        raise SchemaError('Unknown trade direction')
    rows = _csv(raw, 'cp950')
    expected_title = ('Exports to Country' if direction == 'exports' else 'Imports from Country')
    title = rows[0][0].replace(' ', '')
    if title != (expected_title + '(Unit:US$Thousand)').replace(' ', ''):
        raise SchemaError('MOF direction or USD-thousand unit changed')
    names = [x.strip().removesuffix('(千元)') for x in rows[0][1:]]
    if len(names) != len(set(names)) or set(names) != set(TRADE_COLUMNS):
        raise SchemaError('MOF geography columns changed; review new names or aggregates')
    width = len(rows[0])
    output, seen = [], set()
    current_year = None
    for row in rows[1:]:
        if len(row) != width:
            raise SchemaError('MOF row width changed')
        period = re.sub(r'\s+', '', row[0])
        year_match = re.match(r'^(\d{4})年', period)
        if year_match:
            current_year = int(year_match.group(1))
        # Annual and year-to-date summaries are not monthly observations.
        if re.fullmatch(r'\d{4}年(?:累計至\d{1,2}月)?', period):
            continue
        month_match = re.fullmatch(r'(?:\d{4}年)?(\d{1,2})月', period)
        if not month_match or current_year is None:
            raise SchemaError('Unrecognized or yearless MOF monthly period')
        observed = _month_end(current_year, int(month_match.group(1)))
        if not start <= observed <= end:
            continue
        if observed in seen:
            raise SchemaError('Duplicate MOF monthly observation')
        seen.add(observed)
        values = {name: _amount(value) for name, value in zip(names, row[1:])}
        total = values['總計']
        partners = {name: value for name, value in values.items() if name not in AGGREGATES}
        # Some minor territories are not separate columns: residual is allowed.
        # Roundoff across 194 partner cells is bounded at USD 1,000 per cell.
        if sum(partners.values()) > total + Decimal(1000 * len(partners)):
            raise SchemaError('MOF partner values exceed total beyond rounding tolerance')
        if any(value > total for value in partners.values()):
            raise SchemaError('MOF partner exceeds market total')
        metadata = _metadata(observed, scope='goods_including_reexports_reimports',
            trade_system='general', direction=direction, reported_unit='USD thousand',
            valuation='FOB' if direction == 'exports' else 'CIF',
            partner_basis='known_final_destination' if direction == 'exports' else 'country_of_origin')
        output.append(make_point(SERIES, direction + '_usd', observed.isoformat(), total,
                                 release_at=None, status='observed', metadata=metadata))
        for name, value in partners.items():
            sid = partner_id(direction, name)
            label = ('出口至' if direction == 'exports' else '進口自') + name
            output.append(make_point({sid: (label, 'USD')}, sid, observed.isoformat(), value,
                release_at=None, status='observed', metadata={**metadata, 'partner': name}))
    if not output:
        raise SchemaError('No monthly MOF observations within requested history')
    return output


def parse_demand(raw: bytes, start: date, end: date) -> list[dict]:
    rows = _csv(raw, 'utf-8-sig')
    if tuple(rows[0]) != DEMAND_FIELDS:
        raise SchemaError('MOEA turnover columns changed')
    output, seen, included = [], set(), set()
    for row in rows[1:]:
        if len(row) != len(DEMAND_FIELDS):
            raise SchemaError('MOEA turnover row width changed')
        item, code, name, period, raw_value, unit = row
        if code not in INDUSTRIES:
            continue
        expected_name, sid = INDUSTRIES[code]
        if item != '營業額' or name != expected_name or unit != '千元':
            raise SchemaError('MOEA selected industry, measure or unit changed')
        if not re.fullmatch(r'\d{5}', period):
            raise SchemaError('MOEA period is not ROC year/month')
        observed = _month_end(int(period[:3]) + 1911, int(period[3:]))
        if not start <= observed <= end:
            continue
        key = (sid, observed)
        if key in seen:
            raise SchemaError('Duplicate MOEA monthly industry value')
        seen.add(key)
        included.add(sid)
        output.append(make_point(SERIES, sid, observed.isoformat(), _amount(raw_value),
            release_at=None, status='observed', metadata=_metadata(observed,
            scope='industry_turnover', industry_code=code, industry=name,
            reported_unit='TWD thousand', indicator_type='nominal_turnover_proxy')))
    if included != {value[1] for value in INDUSTRIES.values()}:
        raise SchemaError('Missing selected MOEA industry in requested period')
    return sorted(output, key=lambda p: (p['observation_date'], p['series_id']))


def jobs(start: date, end: date) -> list[Job]:
    if start > end:
        raise ValueError('History start must not exceed end')
    # Preserve three years even for a one-year UI window to support same-month YoY.
    lower = min(start.replace(day=1), date(end.year - 3, end.month, 1))
    return [
        Job('macro:mof:exports', 'mof_exports', EXPORT_URL,
            partial(parse_trade, direction='exports', start=lower, end=end),
            parser_version='mof-monthly-2', refresh_after_hours=24, parse_scope=f'{lower}:{end}',
            refresh_at_local='16:30', requested_end=end),
        Job('macro:mof:imports', 'mof_imports', IMPORT_URL,
            partial(parse_trade, direction='imports', start=lower, end=end),
            parser_version='mof-monthly-2', refresh_after_hours=24, parse_scope=f'{lower}:{end}',
            refresh_at_local='16:30', requested_end=end),
        Job('macro:moea:turnover', 'moea_demand', DEMAND_URL,
            partial(parse_demand, start=lower, end=end),
            parser_version='moea-monthly-2', refresh_after_hours=24, parse_scope=f'{lower}:{end}',
            refresh_at_local='16:30', requested_end=end),
    ]


def _indexed(rows: list[dict], sid: str) -> dict[str, dict]:
    indexed = {}
    for row in rows:
        if row['series_id'] != sid or row['unit'] != SERIES[sid][1]:
            raise SchemaError('Derived input series or currency mismatch')
        day = date.fromisoformat(row['observation_date'])
        if day != _month_end(day.year, day.month):
            raise SchemaError('Derived monthly input must be a period end')
        if day.isoformat() in indexed:
            raise SchemaError('Ambiguous monthly input from multiple sources')
        number(row['value'])
        indexed[day.isoformat()] = row
    return indexed


def _inputs(*rows: dict) -> list[dict]:
    return [{key: row.get(key) for key in ('source_id', 'series_id', 'observation_date',
             'value', 'unit', 'raw_sha256', 'retrieved_at')} for row in rows]


def derive(store) -> list[tuple[Source, list[dict]]]:
    """Return reproducible calculations from latest same-period stored inputs."""
    bases = ('exports_usd', 'imports_usd', 'retail_sales_twd',
             'food_services_sales_twd', 'wholesale_sales_twd')
    indexed = {sid: _indexed(store.observations(sid), sid) for sid in bases}
    output = []
    for observed in sorted(indexed['exports_usd'].keys() & indexed['imports_usd'].keys()):
        exports, imports = indexed['exports_usd'][observed], indexed['imports_usd'][observed]
        value = number(exports['value']) - number(imports['value'])
        output.append(make_point(SERIES, 'trade_balance_usd', observed, value,
            status='derived', release_at=None,
            metadata=_metadata(date.fromisoformat(observed),
                scope='goods_including_reexports_reimports', trade_system='general',
                formula='exports_usd[t] - imports_usd[t]', inputs=_inputs(exports, imports),
                limitation='Exports FOB minus imports CIF; not current-account balance or financial flows')))
    for sid, dates in indexed.items():
        for observed, current in sorted(dates.items()):
            day = date.fromisoformat(observed)
            prior_date = _month_end(day.year - 1, day.month).isoformat()
            prior = dates.get(prior_date)
            if prior is None or number(prior['value']) <= 0:
                continue
            value = (number(current['value']) / number(prior['value']) - 1) * 100
            output.append(make_point(SERIES, sid + '_yoy', observed, value,
                status='derived', release_at=None,
                metadata=_metadata(day, formula='100 * (value[t] / value[same calendar month last year] - 1)',
                    inputs=_inputs(current, prior), indicator_type='nominal_yoy_percent')))
    return [(DERIVED_SOURCE, output)] if output else []
