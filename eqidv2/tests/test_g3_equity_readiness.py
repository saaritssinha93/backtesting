"""Offline regression for an unrelated missing ticker blocking all G3 stocks."""
from copy import deepcopy

import pytest

from g3_equity_readiness import cash_scope


def marker():
    return dict(complete=False, tickers_expected=4, tickers_written=3, tickers_failed=1,
                fno_equity_expected=3, fno_equity_ready=3, fno_equity_failed=0,
                fno_equity_quality_complete=True, fno_equity_universe_sha256='verified',
                fno_equity_failure_sample=[])


def test_missing_non_g3_symbol_does_not_block_g3():
    assert cash_scope(marker(), ['A', 'B', 'C'], 'verified') == ('', {})


def test_exact_known_failure_is_excluded():
    value = marker()
    value.update(fno_equity_ready=2, fno_equity_failed=1, fno_equity_quality_complete=False,
                 fno_equity_failure_sample=['B:last=missing:unsafe=target_missing'])
    assert cash_scope(value, ['A', 'B', 'C'], 'verified') == ('', {'B': 'last=missing:unsafe=target_missing'})


@pytest.mark.parametrize('updates', [
    {'fno_equity_universe_sha256': 'wrong'}, {'fno_equity_expected': 4},
    {'fno_equity_ready': 0}, {'fno_equity_quality_complete': False},
    {'fno_equity_ready': 2, 'fno_equity_failed': 1, 'fno_equity_quality_complete': False},
    {'fno_equity_ready': 2, 'fno_equity_failed': 1, 'fno_equity_quality_complete': False,
     'fno_equity_failure_sample': ['OUTSIDE:last=missing']},
    {'fno_equity_ready': 1, 'fno_equity_failed': 2, 'fno_equity_quality_complete': False,
     'fno_equity_failure_sample': ['B:last=missing', 'B:last=missing']},
])
def test_unknown_scope_and_incomplete_failure_evidence_stay_blocked(updates):
    value = {**marker(), **updates}
    assert cash_scope(value, ['A', 'B', 'C'], 'verified')[0]
