import pandas as pd
import pytest

import fno_v13_v10_g_3_contract as contract
from research.g3_0920_target_boundary_check import LONG_EXTRA, SHORT_EXTRA, override_targets
from research.g3_0920_target_sweep import TARGETS


def test_boundary_grid_extends_long_and_refines_short_without_duplicates():
    assert len(LONG_EXTRA) == 140 and LONG_EXTRA[0] == 3.05 and LONG_EXTRA[-1] == 10.
    assert len(SHORT_EXTRA) == 12 and SHORT_EXTRA[0] == .06 and SHORT_EXTRA[-1] == .19
    assert not set(LONG_EXTRA) & set(TARGETS)
    assert not set(SHORT_EXTRA) & set(TARGETS)
    assert min(SHORT_EXTRA) > contract.ROUND_TRIP_COST_BPS / 100


def test_extended_targets_do_not_mutate_production_or_original_orders():
    fingerprint = contract.strategy_fingerprint()
    original = pd.DataFrame(dict(side=["LONG", "SHORT"], native_target_pct=[1., 1.], native_stop_pct=[1.25, 1.25]))
    changed = override_targets(original, 5., .11)
    assert changed.native_target_pct.tolist() == [5., .11]
    assert changed.native_stop_pct.tolist() == [1.25, 1.25]
    assert original.native_target_pct.tolist() == [1., 1.]
    assert contract.strategy_fingerprint() == fingerprint


@pytest.mark.parametrize("target", [.05, 10.1, float('nan')])
def test_outside_research_boundary_rejected(target):
    with pytest.raises(ValueError):
        override_targets(pd.DataFrame(dict(side=["LONG"])), target, .1)
