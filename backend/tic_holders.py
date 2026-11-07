"""US Treasury TIC: major foreign holders of Treasury securities (SLT Table 5).

Tab-separated text, billions of dollars, holdings at end of month, latest ~13 months.
One series per listed holder with a stable slug id. Parsers perform no network or DB writes.
"""
from __future__ import annotations

from calendar import monthrange
from datetime import date
import re
import unicodedata

from .connectors import SchemaError, Source, number
from .contracts import Job, make_point

TABLE5_URL = "https://ticdata.treasury.gov/resource-center/data-chart-center/tic/Documents/slt_table5.txt"
MODEL_VERSION = "tic-holders-1"
PREFIX = "tic_ust_holdings_"
SOURCES = (
    Source("tic_major_foreign_holders", "美國財政部 TIC 主要外國持有人（美國公債）", TABLE5_URL,
           "月底持有額，十億美元；只列最新月份的前 20 名國家／地區及合計，掉出名單的國家停止更新而非歸零。"
           "依託管機構所在地歸屬：比利時、盧森堡、開曼、英國等託管中心可能代表他國持有，不等於最終持有人國籍。"
           "每月約 18 日發布，含前期修訂；年度基準修訂可能改寫歷史。", "monthly", 85, public_years=10),
)
# Known names -> (slug, Chinese label). Unknown future names get a deterministic slug and keep the English name.
KNOWN = {
    "Japan": ("japan", "日本"), "United Kingdom": ("united_kingdom", "英國"),
    "China, Mainland": ("china_mainland", "中國大陸"), "Belgium": ("belgium", "比利時"),
    "Canada": ("canada", "加拿大"), "Cayman Islands": ("cayman_islands", "開曼群島"),
    "Luxembourg": ("luxembourg", "盧森堡"), "France": ("france", "法國"), "Ireland": ("ireland", "愛爾蘭"),
    "Taiwan": ("taiwan", "台灣"), "Switzerland": ("switzerland", "瑞士"), "Singapore": ("singapore", "新加坡"),
    "Hong Kong": ("hong_kong", "香港"), "India": ("india", "印度"), "Norway": ("norway", "挪威"),
    "Brazil": ("brazil", "巴西"), "Saudi Arabia": ("saudi_arabia", "沙烏地阿拉伯"),
    "Korea, South": ("korea_south", "南韓"), "United Arab Emirates": ("united_arab_emirates", "阿聯"),
    "Bermuda": ("bermuda", "百慕達"), "Germany": ("germany", "德國"), "Israel": ("israel", "以色列"),
    "Mexico": ("mexico", "墨西哥"), "Philippines": ("philippines", "菲律賓"), "Thailand": ("thailand", "泰國"),
    "Kuwait": ("kuwait", "科威特"), "Australia": ("australia", "澳洲"), "Netherlands": ("netherlands", "荷蘭"),
    "Sweden": ("sweden", "瑞典"), "Bahamas": ("bahamas", "巴哈馬"),
    "All Other": ("all_other", "其他國家合計"), "Grand Total": ("total", "全體外國合計"),
    "Of Which: Foreign Official": ("foreign_official", "其中外國官方"),
    "Of Which: Foreign Official Treasury Bills": ("foreign_official_bills", "其中外國官方國庫券"),
    "Of Which: Foreign Official T-Bonds & Notes": ("foreign_official_bonds_notes", "其中外國官方中長期公債"),
}
AGGREGATES = {"all_other", "total", "foreign_official", "foreign_official_bills", "foreign_official_bonds_notes"}
UNIT = "十億USD"
SERIES = {PREFIX + slug: (f"{label}持有美國公債", UNIT) for slug, label in KNOWN.values()}


def slug(name: str) -> str:
    s = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode().lower()
    s = re.sub(r"[^a-z0-9]+", "_", s).strip("_")
    if not s:
        raise SchemaError("Holder name cannot form a series id")
    return s


def holder(name: str) -> tuple[str, str]:
    if name in KNOWN:
        key, label = KNOWN[name]
        return PREFIX + key, f"{label}持有美國公債"
    return PREFIX + slug(name), f"{name} holdings of US Treasury securities"


def _month_end(text: str) -> date:
    m = re.fullmatch(r"(\d{4})-(\d{2})", text.strip())
    if not m:
        raise SchemaError("TIC month header changed")
    year, month = int(m.group(1)), int(m.group(2))
    if not 1 <= month <= 12:
        raise SchemaError("Invalid TIC month")
    return date(year, month, monthrange(year, month)[1])


def parse_table5(raw: bytes) -> list[dict]:
    try:
        text = raw.decode("utf-8-sig")
    except UnicodeDecodeError:
        text = raw.decode("latin-1")
    if text.lstrip().startswith("<"):
        raise SchemaError("Expected TIC text table, not HTML")
    lines = [line.rstrip("\r") for line in text.split("\n")]
    if not lines or not lines[0].startswith("Table 5: Major Foreign Holders of Treasury Securities"):
        raise SchemaError("TIC Table 5 title changed")
    if not any(line.strip().startswith("Billions of dollars") for line in lines[:6]):
        raise SchemaError("TIC unit changed")
    head = next((i for i, line in enumerate(lines) if line.split("\t")[0].strip() == "Country"), None)
    if head is None:
        raise SchemaError("TIC header row missing")
    months = [_month_end(c) for c in lines[head].split("\t")[1:] if c.strip()]
    if not months or len(set(months)) != len(months):
        raise SchemaError("TIC month columns missing or duplicated")
    latest = max(months)
    output, seen, names = [], set(), []
    for line in lines[head + 1:]:
        cells = line.split("\t")
        name = cells[0].strip()
        if not name:
            if any(c.strip() for c in cells[1:]):
                raise SchemaError("TIC data row without a holder name")
            break  # blank line ends the table; notes follow
        sid, label = holder(name)
        if sid in seen:
            raise SchemaError("Duplicate TIC holder or slug collision")
        seen.add(sid)
        names.append(name)
        values = [c.strip() for c in cells[1:1 + len(months)]]
        kind = "aggregate" if sid[len(PREFIX):] in AGGREGATES else "country_or_area"
        meta = {"frequency": "monthly", "observation_date_rule": "month_end_holdings",
                "source_unit": "billions of dollars", "holder_name": name, "holder_kind": kind,
                "attribution": "custodian-based; may not reflect the ultimate owner's country",
                "listed_in_latest_top_table": True, "table_latest_month": latest.isoformat(),
                "revision_note": "each release can revise earlier months; annual benchmark revisions occur",
                "publication_time_known": False}
        for month, cell in zip(months, values):
            if cell in ("", "-", "n.a.", "NA"):
                continue
            value = number(cell)
            if value < 0:
                raise SchemaError("Negative TIC holdings")
            output.append(make_point({sid: (label, UNIT)}, sid, month.isoformat(), value, metadata=meta))
    for required in ("Grand Total", "Of Which: Foreign Official"):
        if required not in names:
            raise SchemaError("TIC aggregate row missing: " + required)
    countries = [n for n in names if KNOWN.get(n, ("",))[0] not in AGGREGATES]
    if len(countries) < 15:
        raise SchemaError("TIC table lists fewer than 15 holders")
    return sorted(output, key=lambda p: (p["observation_date"], p["series_id"]))


def jobs(start: date, end: date) -> list[Job]:
    if start > end:
        raise ValueError("start must not follow end")
    return [Job(key="tic:slt-table5", source_id="tic_major_foreign_holders", url=TABLE5_URL,
                parser=parse_table5, parser_version=MODEL_VERSION, refresh_after_hours=24, max_bytes=1_000_000)]
