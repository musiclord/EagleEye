"""Observation-level publication rules; apply before models, including to old saved rows."""

def date_unconfirmed(row):
    meta = row.get('metadata') or {}
    return (meta.get('session_date_basis') == 'anchor_from_same_run_stock_day_all'
            or any(isinstance(i, dict) and i.get('code') == 'session_date_inferred'
                   for i in (meta.get('quality_issues') or [])))
