"""Validate the exact G3 equity universe independently of unrelated feed symbols."""
from __future__ import annotations


def cash_scope(marker, symbols, universe_hash):
    """Return an error and explicitly identified exclusions; never infer missing names.

    The producer checks completed real candles and history quality per stock.
    Its failure sample is usable only when it contains EVERY failed symbol.
    Truncated/unknown failure evidence remains blocked.
    """
    names = {str(s).strip().upper() for s in symbols}
    if not names or not universe_hash:
        return 'cash_5m_marker_fno_equity_universe_missing', {}
    if marker.get('fno_equity_universe_sha256') != universe_hash:
        return 'cash_5m_marker_fno_equity_universe_mismatch', {}
    expected = marker.get('fno_equity_expected')
    ready = marker.get('fno_equity_ready')
    failed = marker.get('fno_equity_failed')
    if (any(type(v) is not int for v in (expected, ready, failed))
            or expected != len(names) or ready <= 0 or failed < 0
            or ready + failed != expected):
        return 'cash_5m_marker_fno_equity_incomplete_coverage', {}
    if failed == 0:
        if marker.get('fno_equity_quality_complete') is not True:
            return 'cash_5m_marker_fno_equity_quality_incomplete', {}
        return '', {}
    failures = marker.get('fno_equity_failure_sample')
    if marker.get('fno_equity_quality_complete') is not False or not isinstance(failures, list):
        return 'cash_5m_marker_fno_equity_exclusions_unverified', {}
    exclusions = {}
    for item in failures:
        if not isinstance(item, str) or ':' not in item:
            return 'cash_5m_marker_fno_equity_exclusions_unverified', {}
        symbol, reason = item.split(':', 1)
        symbol = symbol.strip().upper()
        if symbol not in names or symbol in exclusions or not reason.strip():
            return 'cash_5m_marker_fno_equity_exclusions_unverified', {}
        exclusions[symbol] = reason
    if len(exclusions) != failed:
        return 'cash_5m_marker_fno_equity_exclusions_unverified', {}
    return '', exclusions
