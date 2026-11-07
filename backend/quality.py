"""Per-observation quality: preserve evidence, separate warning context from exclusions."""

def issue(code, *, field, observed, expected, residual, blocking=True, scope='series', severity='warning', **extra):
    return {'code': code, 'severity': severity, 'scope': scope, 'field': field,
            'observed': observed, 'expected': expected, 'residual': residual,
            'blocking': bool(blocking), **extra}


def with_quality(metadata, issues):
    """Legacy flags describe only blocking issues on this observation."""
    codes = list(dict.fromkeys(i['code'] for i in issues if i.get('blocking') is True))
    return {**metadata, 'quality_schema_version': 1, 'quality_issues': issues,
            'quality_flags': codes, 'quality_flag': codes[0] if codes else None}


def blocking_quality(row):
    metadata = row.get('metadata') or {}
    issues = metadata.get('quality_issues')
    if isinstance(issues, list):
        # Explicit valid canonical issues override report-level legacy flags.
        if all(isinstance(i, dict) and isinstance(i.get('blocking'), bool) for i in issues):
            return any(i['blocking'] for i in issues)
        return True  # Malformed new-format quality must not silently enter analysis.
    return bool(metadata.get('quality_flag') or metadata.get('quality_flags'))


def observation_problem(row, expected_unit=None):
    """Analysis eligibility; descriptive/nonblocking report issues remain usable."""
    from math import isfinite
    if not row:
        return 'missing_observation'
    if blocking_quality(row):
        return 'quality_flagged_endpoint'
    value=row.get('value')
    if not isinstance(value,(int,float)) or isinstance(value,bool) or not isfinite(value):
        return 'invalid_value'
    # Existing official adapters use both spellings for percentage points.
    canonical=lambda unit: '%' if unit=='percent' else unit
    if expected_unit is not None and canonical(row.get('unit')) != canonical(expected_unit):
        return 'unit_mismatch'
    return None
