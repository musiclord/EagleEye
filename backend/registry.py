"""Adapter catalogue. Missing adapters are reported, never fabricated."""
import importlib
from . import connectors
MODULE_NAMES = ("market_history", "fx_foreign", "macro_history", "ex_tsmc", "global_rates", "taiwan_macro", "tpex_history", "margin_history", "taifex_history", "breadth", "taiex_weights", "institution_outlook", "us_fred", "china_csi", "japan_mof", "hkex_connect", "tic_holders", "us_federalreserve", "twse_openapi_indices", "twse_open_data_market", "fsc_monthly_reports", "ex_tsmc_open",
                # Last: its MI_MARGN daily job compares against the website credit summaries planned above.
                "twse_openapi")

def modules():
    output = []
    for name in MODULE_NAMES:
        try:
            output.append(importlib.import_module("." + name, __package__))
        except ModuleNotFoundError as exc:
            if exc.name != __package__ + "." + name:
                raise
    return output

def source_map():
    result = {s.id:s for s in connectors.SOURCES}
    for module in modules():
        result.update({s.id:s for s in getattr(module, "SOURCES", ())})
        derived=getattr(module,"DERIVED_SOURCE",None)
        if derived:result[derived.id]=derived
    return result

def local_only_sources():
    """Sources flagged publish=False: stored and served locally, never in a public snapshot."""
    return {source_id for source_id, source in source_map().items() if not getattr(source, "publish", True)}

def prefer_publishable(rows):
    """Drop a local-only row when a publishable source carries the same series and date.

    TWSE website and OpenAPI sources write the same series ids (taiex, tsmc_close, credit units).
    Models need one row per date; keeping the OpenAPI row makes the local view match the public one.
    Rows only a local-only source carries stay, so local history is never shortened."""
    hidden = local_only_sources()
    public = {(r["series_id"], r["observation_date"]) for r in rows if r["source_id"] not in hidden}
    eligible = [r for r in rows if r["source_id"] not in hidden or (r["series_id"], r["observation_date"]) not in public]
    # These two official resources have the same daily definition. CSV is the preferred delivery route.
    priority = {'twse_oa_market': 1, 'twse_od_market': 2}
    groups = {}
    for row in eligible:
        if row['source_id'] in priority and row['series_id'] in {'taiex', 'market_turnover'}:
            groups.setdefault((row['series_id'], row['observation_date']), []).append(row)
    chosen = {}
    for key, candidates in groups.items():
        if len(candidates) < 2:
            continue
        winner = max(candidates, key=lambda r: priority[r['source_id']])
        if len({(r['value'], r.get('unit')) for r in candidates}) > 1:
            from .quality import issue, with_quality
            meta = dict(winner.get('metadata') or {})
            problems = list(meta.get('quality_issues') or [])
            problems.append(issue('official_source_conflict', field='value_and_unit',
                observed=[{'source_id': r['source_id'], 'value': r['value'], 'unit': r.get('unit'),
                           'raw_sha256': r.get('raw_sha256')} for r in candidates],
                expected='same official daily value and unit', residual=None, scope=key[0]))
            meta = with_quality(meta, problems)
            meta['blocking_quality'] = True
            winner = {**winner, 'metadata': meta}
        chosen[key] = winner
    output, emitted = [], set()
    for row in eligible:
        key = (row['series_id'], row['observation_date'])
        if key in chosen and row['source_id'] in priority:
            if key not in emitted:
                output.append(chosen[key]); emitted.add(key)
        else:
            output.append(row)
    return output

def public_history_years():
    """Sources whose public snapshot is limited to the most recent N years (source_id -> N)."""
    return {source_id: source.public_years for source_id, source in source_map().items()
            if source.publish and getattr(source, "public_years", None)}

def series_map():
    result = dict(connectors.SERIES)
    for module in modules():
        result.update(getattr(module,"SERIES",{}))
    return result
