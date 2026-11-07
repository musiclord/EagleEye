"""Official economic outlooks published by international institutions (IMF WEO, OECD EO).

Values are copied from the institutions' public SDMX APIs; nothing is interpolated,
carried forward or re-based. Each publication (vintage) is captured separately so
that the April and October WEO, or EO 118 and EO 119, can be compared side by side.

IMF: api.imf.org SDMX 2.1. Vintage = SDMX PUBLICATION_DATE (current flow) or the
archived ``WEO_<YYYY>_<MON>_VINTAGE`` dataflow id. Actual vs projection comes from
the per country/indicator attribute LATEST_ACTUAL_ANNUAL_DATA; years after it are
IMF staff estimates or projections. Aggregates without that attribute stay unknown.

OECD: sdmx.oecd.org SDMX 2.1. Vintage = dataflow name ("Economic Outlook 119").
The API leaves OBS_STATUS empty, so actual vs projection is reported as unknown
rather than inferred from the calendar. No publication date is exposed.

The DataMapper API (www.imf.org/external/datamapper) is not used: it carries no
actual/projection boundary and its country/period path filters are ignored.
"""
from __future__ import annotations

from datetime import date, datetime
from functools import partial
import hashlib
import json
import re
import xml.etree.ElementTree as ET

from .connectors import SchemaError, Source, number
from .contracts import Job, make_point

PARSER_VERSION = 'institution-outlook-1'
MODEL_VERSION = PARSER_VERSION

IMF_FLOWS_URL = 'https://api.imf.org/external/sdmx/2.1/dataflow/IMF.RES/all/latest'
IMF_DATA_BASE = 'https://api.imf.org/external/sdmx/2.1/data/IMF.RES,{flow}/{key}?startPeriod={start}&endPeriod={end}'
OECD_FLOWS_URL = 'https://sdmx.oecd.org/public/rest/dataflow/OECD.ECO.MAD/all/latest'
OECD_DATA_BASE = 'https://sdmx.oecd.org/public/rest/data/OECD.ECO.MAD,{flow},{version}/{key}?startPeriod={start}&endPeriod={end}'
STRUCTURE_ACCEPT = {'Accept': 'application/vnd.sdmx.structure+xml;version=2.1'}
IMF_DATA_ACCEPT = {'Accept': 'application/vnd.sdmx.structurespecificdata+xml;version=2.1'}
OECD_DATA_ACCEPT = {'Accept': 'application/vnd.sdmx.genericdata+xml;version=2.1'}
REFRESH_HOURS = 72
# Archived vintages kept alongside the current publication for revision comparisons.
IMF_ARCHIVES = 2
OECD_ARCHIVES = 1

NS = {
    'mes': 'http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message',
    'str': 'http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure',
    'com': 'http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common',
    'gen': 'http://www.sdmx.org/resources/sdmxml/schemas/v2_1/data/generic',
}
XSI_TYPE = '{http://www.w3.org/2001/XMLSchema-instance}type'
XML_LANG = '{http://www.w3.org/XML/1998/namespace}lang'
MONTHS = {'JAN': 1, 'FEB': 2, 'MAR': 3, 'APR': 4, 'MAY': 5, 'JUN': 6,
          'JUL': 7, 'AUG': 8, 'SEP': 9, 'OCT': 10, 'NOV': 11, 'DEC': 12}

INSTITUTIONS = {
    'imf_weo': {'label': '國際貨幣基金（IMF）世界經濟展望', 'short': 'IMF WEO', 'publication': 'World Economic Outlook',
                'portal_url': 'https://data.imf.org/en/datasets/IMF.RES:WEO', 'series_prefix': 'imf_weo_',
                'projection_basis': 'IMF SDMX 屬性 LATEST_ACTUAL_ANNUAL_DATA：該年以前（含）為實際值，之後為 IMF 人員估計或預測；未提供者標為未知。'},
    'oecd_eo': {'label': '經濟合作暨發展組織（OECD）經濟展望', 'short': 'OECD EO', 'publication': 'OECD Economic Outlook',
                'portal_url': 'https://www.oecd.org/en/publications/serials/oecd-economic-outlook_16097408.html', 'series_prefix': 'oecd_eo_',
                'projection_basis': 'OECD API 的 OBS_STATUS 未填，無法由官方資料區分實際與預測；一律標為未知，不以年份推測。'},
}
# key, label, unit, {institution: code}, definition notes
INDICATORS = (
    ('real_gdp_growth', '實質 GDP 成長率', '%', {'imf_weo': 'NGDP_RPCH', 'oecd_eo': 'GDPV_ANNPCT'},
     'IMF：GDP 定價年增率；OECD：GDP 實質量年增率。皆為年對年 %。'),
    ('inflation', '消費者物價年增率（年平均）', '%', {'imf_weo': 'PCPIPCH', 'oecd_eo': 'CPI_YTYPCT'},
     'IMF：CPI 年平均變動率；OECD：Headline inflation。OECD 歐元區只提供調和 CPI，本模組不以調和 CPI 代替。'),
    ('unemployment', '失業率', '% of labour force', {'imf_weo': 'LUR', 'oecd_eo': 'UNR'},
     '各國定義依官方統計；IMF 與 OECD 可能採不同調查口徑。'),
    ('current_account', '經常帳餘額占 GDP', '% of GDP', {'imf_weo': 'BCA_NGDPD', 'oecd_eo': 'CBGDPR'},
     '正值為順差。'),
    ('gov_gross_debt', '一般政府債務占 GDP', '% of GDP', {'imf_weo': 'GGXWDG_NGDP', 'oecd_eo': 'GGFLQ'},
     'IMF：GFS 一般政府總債務；OECD：一般政府金融負債總額（SNA）。口徑不同，跨機構數值不可直接比較。'),
    ('gov_net_lending', '一般政府淨借貸（財政收支）占 GDP', '% of GDP', {'imf_weo': 'GGXCNL_NGDP', 'oecd_eo': 'NLGQ'},
     '負值為財政赤字。'),
    ('short_term_rate', '短期利率', '% per annum', {'oecd_eo': 'IRS'},
     'OECD 短期市場利率假設／預測，不是央行政策利率；IMF WEO 無對應序列。'),
)
# key, label, {institution: code}, note
ECONOMIES = (
    ('TWN', '台灣', {'imf_weo': 'TWN', 'oecd_eo': 'TWN'}, 'IMF 名稱 Taiwan Province of China；OECD Economic Outlook 不涵蓋台灣。'),
    ('USA', '美國', {'imf_weo': 'USA', 'oecd_eo': 'USA'}, None),
    ('JPN', '日本', {'imf_weo': 'JPN', 'oecd_eo': 'JPN'}, None),
    ('CHN', '中國', {'imf_weo': 'CHN', 'oecd_eo': 'CHN'}, None),
    ('EA', '歐元區（IMF：全體成員）', {'imf_weo': 'G163'}, 'IMF 代碼 G163 Euro Area。'),
    ('EA17', '歐元區 17 個 OECD 會員國（OECD）', {'oecd_eo': 'EA17'}, '成員範圍與 IMF 歐元區不同，分列不合併。'),
    ('DEU', '德國', {'imf_weo': 'DEU', 'oecd_eo': 'DEU'}, None),
    ('KOR', '韓國', {'imf_weo': 'KOR', 'oecd_eo': 'KOR'}, None),
    ('WLD', '全球', {'imf_weo': 'G001', 'oecd_eo': 'W'}, 'IMF G001 World；OECD W World。兩者皆為機構自身加權的全球彙總。'),
)
# Identity checks: a changed definition is a SchemaError, never a silent remap.
IMF_SERIES_NAME = {
    'NGDP_RPCH': ('Constant prices', 'Percent change'),
    'PCPIPCH': ('Consumer price index', 'Period average', 'percent change'),
    'LUR': ('Unemployment rate',),
    'BCA_NGDPD': ('Current account balance', 'Percent of GDP'),
    'GGXWDG_NGDP': ('Gross debt', 'General government', 'Percent of GDP'),
    'GGXCNL_NGDP': ('Net lending', 'General government', 'Percent of GDP'),
}
OECD_UNITS = {'GDPV_ANNPCT': 'PA', 'CPI_YTYPCT': 'GR', 'UNR': 'PT_LF', 'CBGDPR': 'PT_B1GQ',
              'GGFLQ': 'PT_B1GQ', 'NLGQ': 'PT_B1GQ', 'IRS': 'PO'}

INDICATOR_BY_CODE = {(inst, code): spec for spec in INDICATORS for inst, code in spec[3].items()}
ECONOMY_BY_CODE = {(inst, code): spec for spec in ECONOMIES for inst, code in spec[2].items()}


def series_id(institution, indicator_code, economy_key):
    return f"{INSTITUTIONS[institution]['series_prefix']}{indicator_code.lower()}_{economy_key.lower()}"


SERIES = {}
for _inst, _meta in INSTITUTIONS.items():
    for _key, _label, _unit, _codes, _note in INDICATORS:
        if _inst not in _codes:
            continue
        for _ekey, _elabel, _ecodes, _enote in ECONOMIES:
            if _inst in _ecodes:
                SERIES[series_id(_inst, _codes[_inst], _ekey)] = (f"{_meta['short']} {_label}：{_elabel}", _unit)

SLOTS = {'imf_weo': ['current'] + [f'archive_{i}' for i in range(1, IMF_ARCHIVES + 1)],
         'oecd_eo': ['current'] + [f'archive_{i}' for i in range(1, OECD_ARCHIVES + 1)]}
_NOTE = ('國際機構官方展望；每個發布版本分開保存，同一年度可比較不同版本的修正。'
         '年度觀測日記為該年 12 月 31 日，僅代表期間，不是發布日。不補值、不內插。')
SOURCES = (
    Source('imf_weo_current', 'IMF 世界經濟展望（現行版）', INSTITUTIONS['imf_weo']['portal_url'],
           _NOTE + 'IMF SDMX 現行 WEO 資料流；發布日取自 PUBLICATION_DATE。', 'semiannual', 200),
    *(Source(f'imf_weo_archive_{i}', f'IMF 世界經濟展望（前第 {i} 版）', INSTITUTIONS['imf_weo']['portal_url'],
             _NOTE + 'IMF SDMX 封存版本資料流（WEO_<年>_<月>_VINTAGE）。', 'semiannual', 200)
      for i in range(1, IMF_ARCHIVES + 1)),
    Source('oecd_eo_current', 'OECD 經濟展望（現行版）', INSTITUTIONS['oecd_eo']['portal_url'],
           _NOTE + 'OECD SDMX DSD_EO@DF_EO；期別取自資料流名稱，API 不提供發布日與實際／預測標記。', 'semiannual', 200),
    *(Source(f'oecd_eo_archive_{i}', f'OECD 經濟展望（前第 {i} 期）', INSTITUTIONS['oecd_eo']['portal_url'],
             _NOTE + 'OECD SDMX 封存期別資料流（DSD_EO_<期>@DF_EO_<期>）。', 'semiannual', 200)
      for i in range(1, OECD_ARCHIVES + 1)),
)
SOURCE_IDS = tuple(s.id for s in SOURCES)


def year_window(today: date | None = None):
    """Recent actual years plus the projection horizon; computed, never stored as data."""
    year = (today or date.today()).year
    return year - 3, year + 5


def _xml(raw: bytes, what: str):
    try:
        return ET.fromstring(raw)
    except ET.ParseError as exc:
        raise SchemaError(f'{what} is not well-formed XML') from exc


def _english_name(element):
    names = [n.text or '' for n in element.findall('com:Name', NS) if n.get(XML_LANG) == 'en']
    if len(names) != 1 or not names[0].strip():
        raise SchemaError('Dataflow English name missing or ambiguous')
    return ' '.join(names[0].split())


def parse_dataflows(raw: bytes, agency: str) -> list[dict]:
    root = _xml(raw, 'Dataflow listing')
    if root.tag != f"{{{NS['mes']}}}Structure":
        raise SchemaError('Dataflow listing is not an SDMX structure message')
    flows = []
    for flow in root.iterfind('mes:Structures/str:Dataflows/str:Dataflow', NS):
        if flow.get('agencyID') != agency:
            raise SchemaError('Dataflow listing contains another agency')
        flows.append({'id': flow.get('id'), 'version': flow.get('version'), 'name': _english_name(flow)})
    if not flows or not all(f['id'] and f['version'] for f in flows):
        raise SchemaError('Dataflow listing is empty or malformed')
    return flows


def _sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


# ---------------------------------------------------------------- IMF WEO
def imf_flows(raw: bytes) -> list[dict]:
    """Current WEO plus the newest archived vintages; anything unexpected is a SchemaError."""
    flows = parse_dataflows(raw, 'IMF.RES')
    current = [f for f in flows if f['id'] == 'WEO']
    if len(current) != 1:
        raise SchemaError('IMF WEO dataflow missing or duplicated')
    archived = []
    for flow in flows:
        match = re.fullmatch(r'WEO_(\d{4})_([A-Z]{3})_VINTAGE', flow['id'])
        if not match:
            continue
        if match.group(2) not in MONTHS:
            raise SchemaError('Unrecognised WEO vintage month')
        if 'World Economic Outlook' not in flow['name']:
            raise SchemaError('Archived WEO dataflow name changed')
        archived.append({**flow, 'edition': (int(match.group(1)), MONTHS[match.group(2)])})
    if len({f['edition'] for f in archived}) != len(archived):
        raise SchemaError('Duplicate WEO vintage edition')
    archived.sort(key=lambda f: f['edition'], reverse=True)
    output = [{**current[0], 'edition': None, 'slot': 'current'}]
    output += [{**f, 'slot': f'archive_{i}'} for i, f in enumerate(archived[:IMF_ARCHIVES], 1)]
    return output


def _imf_key():
    countries = '+'.join(c['imf_weo'] for _, _, c, _ in ECONOMIES if 'imf_weo' in c)
    indicators = '+'.join(c['imf_weo'] for _, _, _, c, _ in INDICATORS if 'imf_weo' in c)
    return f'{countries}.{indicators}.A'


def imf_data_jobs(raw: bytes, today: date | None = None) -> list[Job]:
    flows = imf_flows(raw)
    start, end = year_window(today)
    listing = _sha(raw)
    output = []
    for flow in flows:
        url = IMF_DATA_BASE.format(flow=flow['id'], key=_imf_key(), start=start, end=end)
        scope = f"imf-weo:{flow['id']}:{flow['version']}:{start}-{end}"
        output.append(Job(key=f"{scope}:{flow['slot']}", source_id=f"imf_weo_{flow['slot']}", url=url,
                          parser=partial(parse_imf, flow=flow, years=(start, end), listing_sha256=listing, request_url=url),
                          parser_version=PARSER_VERSION, refresh_after_hours=REFRESH_HOURS, headers=dict(IMF_DATA_ACCEPT),
                          max_bytes=3_000_000, parse_scope=scope))
    return output


def check_imf_flows(raw: bytes) -> list[dict]:
    imf_flows(raw)
    return []


def _publication(value, what):
    try:
        stamp = datetime.fromisoformat(str(value).replace('Z', '+00:00'))
    except ValueError as exc:
        raise SchemaError(f'{what} is not an ISO timestamp') from exc
    if stamp.tzinfo is None:
        raise SchemaError(f'{what} has no timezone')
    return stamp


def _ref(root):
    refs = root.findall('mes:Header/mes:Structure/com:StructureUsage/Ref', NS)
    if len(refs) != 1:
        raise SchemaError('SDMX structure reference missing')
    return refs[0].attrib


def _year(value, years):
    if not re.fullmatch(r'\d{4}', value or ''):
        raise SchemaError('Annual TIME_PERIOD expected')
    year = int(value)
    if not years[0] <= year <= years[1]:
        raise SchemaError('Observation outside the requested years')
    return year


def _value(raw_value):
    """Absent and NaN mean "not published" and stay missing; other text is a format change."""
    if raw_value is None or raw_value.strip() in ('', 'NaN'):
        return None
    return float(number(raw_value))


def parse_imf(raw: bytes, *, flow: dict, years: tuple[int, int], listing_sha256: str, request_url: str) -> list[dict]:
    root = _xml(raw, 'IMF WEO data')
    if root.tag != f"{{{NS['mes']}}}StructureSpecificData":
        raise SchemaError('IMF response is not SDMX structure-specific data')
    ref = _ref(root)
    if (ref.get('agencyID'), ref.get('id'), ref.get('version')) != ('IMF.RES', flow['id'], flow['version']):
        raise SchemaError('IMF response belongs to another dataflow or version')
    datasets = root.findall('mes:DataSet', NS)
    if len(datasets) != 1:
        raise SchemaError('Exactly one IMF dataset expected')
    dataset = datasets[0]
    published = _publication(dataset.get('PUBLICATION_DATE'), 'PUBLICATION_DATE')
    updated = dataset.get('UPDATE_DATE')
    if updated:
        _publication(updated, 'UPDATE_DATE')
    if flow.get('edition'):
        y, m = flow['edition']
        if abs((published.year * 12 + published.month) - (y * 12 + m)) > 1:
            raise SchemaError('Archived WEO id disagrees with its PUBLICATION_DATE')
        vintage_year, vintage_month, basis = y, m, 'archived dataflow id'
    else:
        vintage_year, vintage_month, basis = published.year, published.month, 'PUBLICATION_DATE month'
    vintage = f'{vintage_year:04d}-{vintage_month:02d}'
    latest_actual, names, seen = {}, {}, set()
    for group in dataset.findall('Group'):
        kind = (group.get(XSI_TYPE) or '').rsplit(':', 1)[-1]
        if kind == 'GROUP_COUNTRY__INDICATOR':
            key = (group.get('COUNTRY'), group.get('INDICATOR'))
            if key in latest_actual:
                raise SchemaError('Duplicate IMF country/indicator group')
            value = group.get('LATEST_ACTUAL_ANNUAL_DATA')
            if value is not None and not re.fullmatch(r'\d{4}', value):
                raise SchemaError('Invalid LATEST_ACTUAL_ANNUAL_DATA')
            latest_actual[key] = int(value) if value else None
        elif kind == 'GROUP_INDICATOR':
            names[group.get('INDICATOR')] = group.get('SERIES_NAME') or ''
    output = []
    for series in dataset.findall('Series'):
        country, code = series.get('COUNTRY'), series.get('INDICATOR')
        if series.get('FREQUENCY') != 'A' or series.get('SCALE') not in (None, '0'):
            raise SchemaError('IMF series frequency or scale changed')
        indicator, economy = INDICATOR_BY_CODE.get(('imf_weo', code)), ECONOMY_BY_CODE.get(('imf_weo', country))
        if not indicator or not economy:
            raise SchemaError('IMF returned an unrequested country or indicator')
        if (country, code) in seen:
            raise SchemaError('Duplicate IMF series')
        seen.add((country, code))
        if not all(part in names.get(code, '') for part in IMF_SERIES_NAME[code]):
            raise SchemaError(f'IMF indicator definition changed: {code}')
        actual = latest_actual.get((country, code))
        sid = series_id('imf_weo', code, economy[0])
        years_seen = set()
        for obs in series.findall('Obs'):
            year = _year(obs.get('TIME_PERIOD'), years)
            if year in years_seen:
                raise SchemaError('Duplicate IMF observation year')
            years_seen.add(year)
            value = _value(obs.get('OBS_VALUE'))
            if value is None:
                continue
            projection = None if actual is None else year > actual
            output.append(make_point(SERIES, sid, f'{year}-12-31', value,
                status='observed' if projection is False else 'projection' if projection else 'unclassified',
                release_at=dataset.get('PUBLICATION_DATE'), metadata={
                    'parser_version': PARSER_VERSION, 'institution': 'imf_weo', 'institution_label': INSTITUTIONS['imf_weo']['label'],
                    'publication': flow['name'], 'vintage': vintage, 'vintage_label': f'WEO {vintage_year} 年 {vintage_month} 月版',
                    'vintage_basis': basis, 'publication_date': dataset.get('PUBLICATION_DATE'), 'update_date': updated,
                    'dataflow': f"IMF.RES:{flow['id']}({flow['version']})", 'slot': flow['slot'],
                    'year': year, 'requested_years': list(years), 'period_start': f'{year}-01-01', 'period_end': f'{year}-12-31', 'frequency': 'annual',
                    'is_projection': projection, 'latest_actual_year': actual,
                    'projection_basis': 'LATEST_ACTUAL_ANNUAL_DATA' if actual is not None else 'not_provided_by_source',
                    'indicator_key': indicator[0], 'indicator_code': code, 'source_series_name': names.get(code),
                    'economy_key': economy[0], 'country_code': country, 'country_update_date': series.get('COUNTRY_UPDATE_DATE'),
                    'source_url': request_url, 'dataflow_listing_sha256': listing_sha256}))
    if not seen:
        raise SchemaError('IMF response has no series')
    return output


# ---------------------------------------------------------------- OECD EO
def oecd_flows(raw: bytes) -> list[dict]:
    flows = parse_dataflows(raw, 'OECD.ECO.MAD')
    current = [f for f in flows if f['id'] == 'DSD_EO@DF_EO']
    if len(current) != 1:
        raise SchemaError('OECD Economic Outlook dataflow missing or duplicated')
    match = re.fullmatch(r'(?:OECD )?Economic Outlook (?:No\.? )?(\d+)', current[0]['name'])
    if not match:
        raise SchemaError('OECD Economic Outlook number not found in dataflow name')
    number_now = int(match.group(1))
    archived = []
    for flow in flows:
        match = re.fullmatch(r'DSD_EO_(\d+)@DF_EO_(\d+)', flow['id'])
        if match and match.group(1) == match.group(2) and int(match.group(1)) < number_now:
            if not re.search(rf'Economic Outlook (?:No\.? )?{match.group(1)}\b', flow['name']):
                raise SchemaError('Archived OECD EO name disagrees with its id')
            archived.append({**flow, 'edition': int(match.group(1))})
    archived.sort(key=lambda f: f['edition'], reverse=True)
    output = [{**current[0], 'edition': number_now, 'slot': 'current'}]
    output += [{**f, 'slot': f'archive_{i}'} for i, f in enumerate(archived[:OECD_ARCHIVES], 1)]
    return output


def _oecd_key():
    areas = '+'.join(c['oecd_eo'] for _, _, c, _ in ECONOMIES if 'oecd_eo' in c)
    measures = '+'.join(c['oecd_eo'] for _, _, _, c, _ in INDICATORS if 'oecd_eo' in c)
    return f'{areas}.{measures}.A'


def oecd_data_jobs(raw: bytes, today: date | None = None) -> list[Job]:
    flows = oecd_flows(raw)
    start, end = year_window(today)
    listing = _sha(raw)
    output = []
    for flow in flows:
        url = OECD_DATA_BASE.format(flow=flow['id'], version=flow['version'], key=_oecd_key(), start=start, end=end)
        scope = f"oecd-eo:{flow['id']}:{flow['version']}:{start}-{end}"
        output.append(Job(key=f"{scope}:{flow['slot']}", source_id=f"oecd_eo_{flow['slot']}", url=url,
                          parser=partial(parse_oecd, flow=flow, years=(start, end), listing_sha256=listing, request_url=url),
                          parser_version=PARSER_VERSION, refresh_after_hours=REFRESH_HOURS, headers=dict(OECD_DATA_ACCEPT),
                          max_bytes=3_000_000, parse_scope=scope))
    return output


def check_oecd_flows(raw: bytes) -> list[dict]:
    oecd_flows(raw)
    return []


def _values(element, path):
    return {v.get('id'): v.get('value') for v in element.findall(path, NS)}


OECD_STATUS = {'F': True, 'A': False, 'P': False}


def parse_oecd(raw: bytes, *, flow: dict, years: tuple[int, int], listing_sha256: str, request_url: str) -> list[dict]:
    root = _xml(raw, 'OECD EO data')
    if root.tag != f"{{{NS['mes']}}}GenericData":
        raise SchemaError('OECD response is not SDMX generic data')
    ref = _ref(root)
    if (ref.get('agencyID'), ref.get('id'), ref.get('version')) != ('OECD.ECO.MAD', flow['id'], flow['version']):
        raise SchemaError('OECD response belongs to another dataflow or version')
    datasets = root.findall('mes:DataSet', NS)
    if len(datasets) != 1:
        raise SchemaError('Exactly one OECD dataset expected')
    merged = {}
    for series in datasets[0].findall('gen:Series', NS):
        key = _values(series, 'gen:SeriesKey/gen:Value')
        attrs = _values(series, 'gen:Attributes/gen:Value')
        if set(key) != {'REF_AREA', 'MEASURE', 'FREQ'} or key['FREQ'] != 'A':
            raise SchemaError('OECD series key changed')
        area, code = key['REF_AREA'], key['MEASURE']
        if ('oecd_eo', code) not in INDICATOR_BY_CODE or ('oecd_eo', area) not in ECONOMY_BY_CODE:
            raise SchemaError('OECD returned an unrequested area or measure')
        if attrs.get('UNIT_MEASURE') != OECD_UNITS[code] or attrs.get('UNIT_MULT', '0') != '0':
            raise SchemaError(f'OECD unit changed: {code}')
        # The service splits one series into several blocks; identical repeats are harmless.
        target = merged.setdefault((area, code), {'attrs': attrs, 'obs': {}})
        for obs in series.findall('gen:Obs', NS):
            dims = obs.findall('gen:ObsDimension', NS)
            if len(dims) != 1 or dims[0].get('id') != 'TIME_PERIOD':
                raise SchemaError('OECD observation dimension changed')
            year = _year(dims[0].get('value'), years)
            values = obs.findall('gen:ObsValue', NS)
            value = _value(values[0].get('value')) if len(values) == 1 else None
            status = _values(obs, 'gen:Attributes/gen:Value').get('OBS_STATUS') or None
            previous = target['obs'].get(year)
            if previous is not None and previous != (value, status):
                raise SchemaError('Conflicting OECD observations for one year')
            target['obs'][year] = (value, status)
    output = []
    for (area, code), item in sorted(merged.items()):
        indicator, economy = INDICATOR_BY_CODE[('oecd_eo', code)], ECONOMY_BY_CODE[('oecd_eo', area)]
        sid = series_id('oecd_eo', code, economy[0])
        for year, (value, status) in sorted(item['obs'].items()):
            if value is None:
                continue
            projection = OECD_STATUS.get(status)
            output.append(make_point(SERIES, sid, f'{year}-12-31', value,
                status='observed' if projection is False else 'projection' if projection else 'unclassified',
                release_at=None, metadata={
                    'parser_version': PARSER_VERSION, 'institution': 'oecd_eo', 'institution_label': INSTITUTIONS['oecd_eo']['label'],
                    'publication': flow['name'], 'vintage': f"EO{flow['edition']}",
                    'vintage_label': f"OECD 經濟展望第 {flow['edition']} 期", 'vintage_basis': 'dataflow name',
                    'publication_date': None, 'update_date': None,
                    'dataflow': f"OECD.ECO.MAD:{flow['id']}({flow['version']})", 'slot': flow['slot'],
                    'year': year, 'requested_years': list(years), 'period_start': f'{year}-01-01', 'period_end': f'{year}-12-31', 'frequency': 'annual',
                    'is_projection': projection, 'obs_status': status, 'latest_actual_year': None,
                    'projection_basis': 'OBS_STATUS' if projection is not None else 'not_provided_by_source',
                    'indicator_key': indicator[0], 'indicator_code': code, 'unit_measure': item['attrs'].get('UNIT_MEASURE'),
                    'base_period': item['attrs'].get('BASE_PER'), 'economy_key': economy[0], 'country_code': area,
                    'source_url': request_url, 'dataflow_listing_sha256': listing_sha256}))
    if not merged:
        raise SchemaError('OECD response has no series')
    return output


def jobs(start, end) -> list[Job]:
    # Publications are semiannual; the sync range does not change what is requested.
    if start > end:
        raise ValueError('History start must not exceed end')
    return [
        Job(key='imf-weo:dataflows', source_id='imf_weo_current', url=IMF_FLOWS_URL, parser=check_imf_flows,
            parser_version=PARSER_VERSION, refresh_after_hours=REFRESH_HOURS, headers=dict(STRUCTURE_ACCEPT),
            max_bytes=2_000_000, followups=imf_data_jobs),
        Job(key='oecd-eo:dataflows', source_id='oecd_eo_current', url=OECD_FLOWS_URL, parser=check_oecd_flows,
            parser_version=PARSER_VERSION, refresh_after_hours=REFRESH_HOURS, headers=dict(STRUCTURE_ACCEPT),
            max_bytes=2_000_000, followups=oecd_data_jobs),
    ]


# ---------------------------------------------------------------- read model
def load_versions(store, as_of=None) -> list[dict]:
    """Every stored version of these sources; vintages are told apart by metadata, not by slot."""
    from .history import decode_observation, parse_instant
    marks = ','.join('?' for _ in SOURCE_IDS)
    with store.lock, store.connect() as db:
        cursor = db.execute(f'SELECT * FROM observations WHERE source_id IN ({marks})', list(SOURCE_IDS))
        keys = [x[0] for x in cursor.description]
        rows = [decode_observation(dict(zip(keys, row))) for row in cursor.fetchall()]
    if as_of is not None:
        cutoff = parse_instant(as_of)
        rows = [r for r in rows if r.get('known_since') and parse_instant(r['known_since']) <= cutoff]
    return rows


def _vintage_order(institution, meta):
    if institution == 'oecd_eo':
        return int(re.sub(r'\D', '', meta['vintage']) or 0), ''
    return meta['vintage'], meta.get('publication_date') or ''


def outlook_view(rows: list[dict], as_of=None) -> dict:
    from .history import parse_instant
    chosen, versions = {}, {}
    for row in rows:
        meta = row.get('metadata') or {}
        institution = meta.get('institution')
        if institution not in INSTITUTIONS or not meta.get('vintage') or meta.get('parser_version') != PARSER_VERSION:
            continue
        key = (institution, meta['vintage'], row['series_id'], row['observation_date'])
        versions[key] = versions.get(key, 0) + 1
        previous = chosen.get(key)
        if previous is None or parse_instant(row['retrieved_at']) > parse_instant(previous['retrieved_at']):
            chosen[key] = row
    vintages = {}
    for (institution, vintage, _, _), row in chosen.items():
        meta = row['metadata']
        entry = vintages.setdefault((institution, vintage), {
            'vintage': vintage, 'label': meta.get('vintage_label'), 'publication': meta.get('publication'),
            'publication_date': meta.get('publication_date'), 'vintage_basis': meta.get('vintage_basis'),
            'dataflow': meta.get('dataflow'), 'source_url': meta.get('source_url'), 'retrieved_at': row['retrieved_at'],
            'raw_sha256': set(), 'years': set(), 'order': _vintage_order(institution, meta)})
        entry['raw_sha256'].add(row['raw_sha256'])
        entry['years'].add(meta.get('year'))
        requested = meta.get('requested_years')
        if isinstance(requested, list) and len(requested) == 2 and all(isinstance(y, int) for y in requested):
            entry['years'].update(range(requested[0], requested[1] + 1))
        if parse_instant(row['retrieved_at']) > parse_instant(entry['retrieved_at']):
            entry.update(retrieved_at=row['retrieved_at'], source_url=meta.get('source_url'), dataflow=meta.get('dataflow'))
    institutions = []
    for institution, info in INSTITUTIONS.items():
        listed = sorted((v for (i, _), v in vintages.items() if i == institution), key=lambda v: v['order'], reverse=True)
        for i, item in enumerate(listed):
            item['is_latest'] = i == 0
        institutions.append({'id': institution, 'label': info['label'], 'publication': info['publication'],
                             'portal_url': info['portal_url'], 'projection_basis': info['projection_basis'],
                             'vintages': [{k: (sorted(v) if isinstance(v, set) else v) for k, v in item.items() if k not in ('order', 'years')}
                                          for item in listed]})
    years = sorted({y for v in vintages.values() for y in v['years'] if isinstance(y, int)})
    horizon, published = {}, set()
    for (institution, vintage, sid, observed) in chosen:
        horizon[(institution, vintage)] = max(horizon.get((institution, vintage), 0), int(observed[:4]))
        published.add((institution, vintage, sid))
    cells, missing = [], []
    for (institution, vintage), entry in sorted(vintages.items(), key=lambda p: (p[0][0], p[1]['order'])):
        for key, label, unit, codes, _ in INDICATORS:
            if institution not in codes:
                continue
            for ekey, elabel, ecodes, _ in ECONOMIES:
                if institution not in ecodes:
                    continue
                sid = series_id(institution, codes[institution], ekey)
                for year in sorted(y for y in entry['years'] if isinstance(y, int)):
                    row = chosen.get((institution, vintage, sid, f'{year}-12-31'))
                    meta = row['metadata'] if row else {}
                    cell = {'institution': institution, 'vintage': vintage, 'indicator': key, 'economy': ekey, 'year': year,
                            'series_id': sid, 'value': row['value'] if row else None, 'unit': unit,
                            'is_projection': meta.get('is_projection') if row else None,
                            'latest_actual_year': meta.get('latest_actual_year'), 'raw_sha256': row['raw_sha256'] if row else None,
                            'retrieved_at': row['retrieved_at'] if row else None, 'source_url': meta.get('source_url'),
                            'indicator_code': codes[institution], 'country_code': ecodes[institution],
                            'versions_in_vintage': versions.get((institution, vintage, sid, f'{year}-12-31'), 0),
                            'missing': row is None, 'missing_reason': None}
                    if row is None:
                        cell['missing_reason'] = ('series_not_in_response' if (institution, vintage, sid) not in published
                            else 'beyond_publication_horizon' if year > horizon[(institution, vintage)] else 'value_not_in_response')
                    cells.append(cell)
                    if row is None:
                        missing.append({k: cell[k] for k in ('institution', 'vintage', 'indicator', 'economy', 'year', 'indicator_code', 'country_code', 'missing_reason')})
    revisions = []
    by_key = {(c['institution'], c['vintage'], c['indicator'], c['economy'], c['year']): c for c in cells if not c['missing']}
    for item in institutions:
        ordered = list(reversed(item['vintages']))  # oldest first
        for older, newer in zip(ordered, ordered[1:]):
            for key, *_ in INDICATORS:
                for ekey, *_ in ECONOMIES:
                    for year in years:
                        a = by_key.get((item['id'], older['vintage'], key, ekey, year))
                        b = by_key.get((item['id'], newer['vintage'], key, ekey, year))
                        if a and b:
                            revisions.append({'institution': item['id'], 'indicator': key, 'economy': ekey, 'year': year,
                                              'from_vintage': older['vintage'], 'to_vintage': newer['vintage'],
                                              'from_value': a['value'], 'to_value': b['value'], 'change': round(b['value'] - a['value'], 10),
                                              'from_is_projection': a['is_projection'], 'to_is_projection': b['is_projection']})
    latest = {i['id']: i['vintages'][0]['vintage'] for i in institutions if i['vintages']}
    comparisons = []
    for key, label, unit, codes, note in INDICATORS:
        for ekey, *_ in ECONOMIES:
            for year in years:
                entries = [by_key[(inst, vintage, key, ekey, year)] for inst, vintage in latest.items()
                           if (inst, vintage, key, ekey, year) in by_key]
                if len(entries) > 1:
                    comparisons.append({'indicator': key, 'economy': ekey, 'year': year,
                                        'comparable': key != 'gov_gross_debt',
                                        'values': [{k: e[k] for k in ('institution', 'vintage', 'value', 'is_projection', 'raw_sha256')} for e in entries]})
    return {
        'as_of': as_of, 'parser_version': PARSER_VERSION,
        'institutions': institutions,
        'indicators': [{'key': k, 'label': l, 'unit': u, 'codes': c, 'definition': n,
                        'cross_institution_comparable': k != 'gov_gross_debt'} for k, l, u, c, n in INDICATORS],
        'economies': [{'key': k, 'label': l, 'codes': c, 'definition': n} for k, l, c, n in ECONOMIES],
        'years': years, 'cells': cells, 'missing': missing, 'revisions': revisions, 'comparisons': comparisons,
        'counts': {'cells': len(cells), 'values': len(cells) - len(missing), 'missing': len(missing),
                   'vintages': sum(len(i['vintages']) for i in institutions)},
        'caveats': [
            '數值為各機構公開資料庫原值，未補值、未內插；缺值保持缺值。missing_reason：series_not_in_response＝該版本未提供此經濟體／指標；'
            'beyond_publication_horizon＝超出該版本預測年限；value_not_in_response＝序列存在但該年無值。',
            'is_projection：IMF 依 LATEST_ACTUAL_ANNUAL_DATA 判定；之後年度含 IMF 人員估計與預測。OECD API 未提供標記，為 null。',
            '同一機構不同版本並列於 revisions；跨機構比較見 comparisons，債務口徑不同不可直接比較。',
            '年度觀測日記為 12 月 31 日，只代表期間；版本以發布日或官方期別識別。'],
    }
