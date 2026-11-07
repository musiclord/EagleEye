"""Federal Reserve Board release XML archives; no FRED retrieval or database side effects.

The engine owns HTTP, raw hashes and observation versions. This adapter selects five
explicit series from the complete H.4.1/H.10 archives and preserves their definitions.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from functools import partial
import io
import re
import zipfile
import xml.etree.ElementTree as ET

from .connectors import SchemaError, Source, number
from .contracts import Job, make_point

MODEL_VERSION = 'us-federalreserve-1'
HISTORY_START = date(2008, 1, 1)
LICENSE_URL = 'https://www.federalreserve.gov/disclaimer.htm'
H10_DEFINITION_URL = 'https://www.federalreserve.gov/releases/h10/h10_technical_qa.htm'
URLS = {rel: f'https://www.federalreserve.gov/releases/{rel}/data/FRB_{rel}_xml.zip'
        for rel in ('h41', 'h10')}
MAX_ZIP_BYTES = 16_000_000
MAX_XML_BYTES = 160_000_000
MAX_ARCHIVE_EXPANDED_BYTES = 180_000_000
MESSAGE_NS = 'http://www.SDMX.org/resources/SDMXML/schemas/v1_0/message'
COMMON_NS = 'http://www.federalreserve.gov/structure/compact/common'

SOURCES = (
    Source('frb_h41', '聯準會理事會 H.4.1 資產負債表與託管美債', URLS['h41'],
           '直接讀理事會官方 XML。總資產與託管美債為週三水位；TGA保留原有截至週三週平均口徑。'
           '每週四發布，資料日不等於發布日；未知發布時間維持空值。原始檔包含修訂後歷史，非歷史發布版本。',
           'weekly', 10, public_years=10,
           attribution='Board of Governors of the Federal Reserve System，H.4.1；由本站整理，非聯準會背書。',
           license_url=LICENSE_URL, content_revisions_only=True),
    Source('frb_h10', '聯準會理事會 H.10 廣義美元與人民幣匯率', URLS['h10'],
           '每週一發布前週日資料。美元指數為商品與服務貿易權重、2006年1月平均=100；'
           'XML沿用舊基期欄名，以官方技術問答定義為準。人民幣為紐約中午買入價，非CFETS中間價或CNH。',
           'daily', 12, public_years=10,
           attribution='Board of Governors of the Federal Reserve System，H.10；由本站整理，非聯準會背書。',
           license_url=LICENSE_URL, content_revisions_only=True),
)

SPECS = {
    'h41': {
        'RESPPA_N.WW': dict(series_id='fed_total_assets_usd_mn', label='聯準會總資產（週三水位）',
                           unit='百萬USD', measure='wednesday_level', positive=True,
                           attrs={'CATEGORY': 'ASSET', 'SUBCATEGORY': 'TA', 'COMPONENT': 'TA', 'SERIESTYPE': 'L'}),
        'RESH4FG_N.WW': dict(series_id='fed_custody_foreign_official_ust_usd_mn',
                            label='聯準會代外國官方與國際機構保管的可交易美國公債（週三水位）',
                            unit='百萬USD', measure='wednesday_level', positive=False,
                            attrs={'CATEGORY': 'MEMO', 'SUBCATEGORY': 'CH', 'COMPONENT': 'USTSN', 'SERIESTYPE': 'L'}),
        'RESPPLLDT_XAW_N.WW': dict(series_id='us_treasury_general_account_usd_mn',
                                  label='美國財政部一般帳戶 TGA（截至週三的週平均）',
                                  unit='百萬USD', measure='week_average_ending_wednesday', positive=False,
                                  attrs={'CATEGORY': 'LIABCAP', 'SUBCATEGORY': 'LIAB', 'COMPONENT': 'DEPUSTG', 'SERIESTYPE': 'A'}),
    },
    'h10': {
        'JRXWTFB_N.B': dict(series_id='usd_broad_index', label='名目廣義美元指數',
                           unit='index (2006-01=100)', measure='nominal_broad_trade_weighted', positive=True,
                           attrs={'FX': 'BRD', 'CURRENCY': 'NA', 'UNIT': 'Index:_1997_Jan_100'}),
        'RXI_N.B.CH': dict(series_id='usd_cny_noon_ny', label='人民幣兌美元（紐約中午買入匯率）',
                          unit='CNY/USD', measure='noon_buying_rate_new_york', positive=True,
                          attrs={'FX': 'CNY', 'CURRENCY': 'CNY', 'UNIT': 'Currency'}),
    },
}
SERIES = {s['series_id']: (s['label'], s['unit']) for specs in SPECS.values() for s in specs.values()}


def _xml_from_zip(raw: bytes, release: str) -> bytes:
    if len(raw) > MAX_ZIP_BYTES:
        raise SchemaError('FRB ZIP exceeds compressed size limit')
    try:
        with zipfile.ZipFile(io.BytesIO(raw)) as archive:
            entries = archive.infolist()
            if not 1 <= len(entries) <= 8 or len({e.filename for e in entries}) != len(entries):
                raise SchemaError('FRB ZIP member count or duplicate names changed')
            for item in entries:
                if '/' in item.filename or '\\' in item.filename or ':' in item.filename or item.filename in {'.', '..'}:
                    raise SchemaError('FRB ZIP contains unsafe member path')
                if item.flag_bits & 1 or item.compress_type not in {zipfile.ZIP_STORED, zipfile.ZIP_DEFLATED}:
                    raise SchemaError('FRB ZIP encryption or compression is unsupported')
            if sum(e.file_size for e in entries) > MAX_ARCHIVE_EXPANDED_BYTES:
                raise SchemaError('FRB ZIP exceeds expanded size limit')
            name = release.upper() + '_data.xml'
            if name not in archive.namelist():
                raise SchemaError('FRB data XML member is missing')
            item = archive.getinfo(name)
            if item.file_size > MAX_XML_BYTES:
                raise SchemaError('FRB XML exceeds size limit')
            with archive.open(item) as source:
                data = source.read(MAX_XML_BYTES + 1)
            if len(data) > MAX_XML_BYTES:
                raise SchemaError('FRB XML exceeds size limit')
    except (zipfile.BadZipFile, OSError, RuntimeError, NotImplementedError, EOFError) as exc:
        raise SchemaError('Invalid FRB ZIP archive') from exc
    # Only UTF-8 release files are supported. Reject DTD/entities before ElementTree sees them.
    if b'\x00' in data or re.search(br'<!\s*(?:DOCTYPE|ENTITY)\b', data, flags=re.IGNORECASE):
        raise SchemaError('FRB XML DTD, entity or unsupported encoding')
    try:
        data.decode('utf-8-sig')
    except UnicodeDecodeError as exc:
        raise SchemaError('FRB XML is not UTF-8') from exc
    return data


def _validate_series(attrs: dict, spec: dict, release: str):
    expected = dict(spec['attrs'])
    if release == 'h41':
        expected.update(FREQ='19', UNIT='Currency', UNIT_MULT='1000000', CURRENCY='USD', DISTRIBUTION='TOT')
    else:
        expected.update(FREQ='9', UNIT_MULT='1')
    if any(attrs.get(k) != v for k, v in expected.items()):
        raise SchemaError('FRB series unit, frequency or definition changed: ' + attrs['SERIES_NAME'])


def parse_release(raw: bytes, release: str, *, start: date = HISTORY_START,
                  end: date | None = None, today: date | None = None) -> list[dict]:
    """Read a bounded official ZIP; emit observed values only, with unchanged series semantics."""
    if release not in SPECS:
        raise SchemaError('Unknown FRB release')
    today = today or datetime.now(timezone(timedelta(hours=8))).date()
    end = end or today
    if start > end:
        raise ValueError('start must not follow end')
    xml = _xml_from_zip(raw, release)
    series_tag = '{http://www.federalreserve.gov/structure/compact/' + release.upper() + '_' + release.upper() + '}Series'
    obs_tag = '{' + COMMON_NS + '}Obs'
    specs, seen_series, output = SPECS[release], set(), []
    root_seen, header_seen, prepared = False, False, None
    try:
        for event, elem in ET.iterparse(io.BytesIO(xml), events=('start', 'end')):
            if not root_seen:
                if event != 'start' or elem.tag != '{' + MESSAGE_NS + '}MessageGroup':
                    raise SchemaError('FRB XML root or namespace changed')
                root_seen = True
            if event != 'end':
                continue
            if elem.tag == '{' + MESSAGE_NS + '}Header':
                if header_seen or elem.findtext('{' + MESSAGE_NS + '}ID') != release.upper():
                    raise SchemaError('FRB release header changed')
                sender = elem.find('{' + MESSAGE_NS + '}Sender')
                if sender is None or sender.attrib.get('id') != 'FRB':
                    raise SchemaError('FRB release sender changed')
                prepared = elem.findtext('{' + MESSAGE_NS + '}Prepared')
                header_seen = True
                elem.clear()
            elif elem.tag == series_tag:
                name = elem.attrib.get('SERIES_NAME')
                if name not in specs:
                    elem.clear()
                    continue
                if not header_seen or name in seen_series:
                    raise SchemaError('FRB duplicate series or missing header')
                seen_series.add(name)
                spec = specs[name]
                _validate_series(elem.attrib, spec, release)
                meta = dict(frequency='weekly' if release == 'h41' else 'daily',
                            source_series=name, original_source='Board of Governors of the Federal Reserve System, ' + ('H.4.1' if release == 'h41' else 'H.10'),
                            source_unit='Millions of U.S. Dollars' if release == 'h41' else spec['unit'],
                            source_xml_unit=elem.attrib['UNIT'], source_unit_multiplier=elem.attrib['UNIT_MULT'],
                            measure=spec['measure'], publication_time_known=False, historical_vintage_available=False,
                            publish=True, license='us_government_public', license_url=LICENSE_URL,
                            source_timezone='America/New_York', parser_version=MODEL_VERSION,
                            missing_rule='NA/ND and blank omitted; never zero-filled')
                if release == 'h41':
                    meta['observation_day'] = 'Wednesday' if spec['measure'] == 'wednesday_level' else 'Wednesday (week ending)'
                if name == 'RESPPLLDT_XAW_N.WW':
                    meta['comparability_note'] = 'Week average; total assets are a Wednesday level. Wednesday TGA is RESPPLLDT_N.WW and must use another series id.'
                elif name == 'JRXWTFB_N.B':
                    meta.update(unit_definition_url=H10_DEFINITION_URL, unit_definition_note='Official H.10 technical Q&A: January 2006 monthly mean=100; XML UNIT retains old 1997 label. Values are not rescaled.')
                elif name == 'RXI_N.B.CH':
                    meta.update(quote='CNY per 1 USD', rising_means='CNY depreciation', not_equivalent_to='CFETS central parity or offshore CNH')
                seen_dates = set()
                for obs in elem:
                    if obs.tag != obs_tag:
                        continue
                    try:
                        observed = date.fromisoformat(obs.attrib['TIME_PERIOD'])
                    except (KeyError, ValueError) as exc:
                        raise SchemaError('FRB invalid observation date') from exc
                    if observed in seen_dates:
                        raise SchemaError('FRB duplicate observation date')
                    seen_dates.add(observed)
                    if observed > today or (release == 'h41' and observed.weekday() != 2) or observed.weekday() > 4:
                        raise SchemaError('FRB future date or observation weekday changed')
                    status = obs.attrib.get('OBS_STATUS')
                    if status not in {'A', 'NA', 'ND'}:
                        raise SchemaError('FRB unknown observation status')
                    value_text = obs.attrib.get('OBS_VALUE', '').strip()
                    if status in {'NA', 'ND'}:
                        if value_text not in {'', '-999999', '-9999'}:
                            raise SchemaError('FRB missing observation has an unexpected value')
                        continue
                    if not value_text or not start <= observed <= end:
                        continue
                    value = number(value_text)
                    if value < 0 or (spec['positive'] and value == 0):
                        raise SchemaError('FRB observation outside valid range')
                    output.append(make_point(SERIES, spec['series_id'], observed.isoformat(), value, metadata=dict(meta)))
                elem.clear()
    except ET.ParseError as exc:
        raise SchemaError('Malformed FRB XML') from exc
    if seen_series != set(specs):
        raise SchemaError('FRB required series missing: ' + ', '.join(sorted(set(specs) - seen_series)))
    if not output:
        raise SchemaError('FRB release has no observations in requested range')
    return sorted(output, key=lambda p: (p['observation_date'], p['series_id']))


def jobs(start: date, end: date) -> list[Job]:
    """Two stable requests; decode the history once, with no date-by-date downloads.

    Like the previous FRED adapter, the archive covers 2008 onward even for an
    incremental refresh. Public export independently enforces Source.public_years.
    """
    if start > end:
        raise ValueError('start must not follow end')
    return [Job(key='frb:' + rel + ':xml', source_id='frb_' + rel, url=URLS[rel],
                parser=partial(parse_release, release=rel, start=HISTORY_START, end=end),
                parser_version=MODEL_VERSION, refresh_after_hours=12,
                headers={'User-Agent': 'EagleEye/1.0 (public official statistical releases)'},
                max_bytes=MAX_ZIP_BYTES, parse_scope=f'start={HISTORY_START.isoformat()}&end={end.isoformat()}',
                requested_end=end) for rel in ('h41', 'h10')]
