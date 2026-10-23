"""Selection and regime checks for the isolated G8 research runner."""
from itertools import product

import numpy as np
import pandas as pd
import pytest

from research import g8_exit_engine as engine
from research import g8_nifty_vix_backtest as g8


def _features():
    rows = []
    for side, base, aligned, vix in product(
        ["LONG", "SHORT"], [False, True], [False, True],
        [(1., 1.), (-1., -1.), (.2, .2), (1., -1.)],
    ):
        rows.append(dict(side=side, nifty_base_allowed=base, nifty_both_aligned=aligned,
                         vix_session_pct=vix[0], vix_recent_pct=vix[1]))
    return pd.DataFrame(rows, index=np.arange(100, 100+len(rows)))


@pytest.mark.parametrize("mode", ["baseline", "stress_alignment", "long_caution", "pair_consistency"])
def test_gate_semantics_keep_nifty_veto_and_treat_mixed_vix_as_neutral(mode):
    frame = _features()
    result = g8.gate_decisions(frame, dict(mode=mode, threshold=1.))
    expected = []
    for row in frame.itertuples():
        rising = row.vix_session_pct >= 1 and row.vix_recent_pct >= 1
        falling = row.vix_session_pct <= -1 and row.vix_recent_pct <= -1
        allowed = row.nifty_base_allowed
        if mode == "stress_alignment" and rising:
            allowed = allowed and row.nifty_both_aligned
        if mode == "long_caution" and rising and row.side == "LONG":
            allowed = allowed and row.nifty_both_aligned
        if mode == "pair_consistency" and ((rising and row.side == "LONG") or (falling and row.side == "SHORT")):
            allowed = False
        expected.append(allowed)
    assert result.tolist() == expected
    assert result.index.equals(frame.index)


def test_exit_modes_align_to_sid_and_bound_scaling():
    frame = pd.DataFrame(dict(
        baseline_target_pct=[.6, 1.5, 2.],
        vix_session_pct=[1., 1., -.5], vix_recent_pct=[.5, .49, -.5],
        vix_previous_close_pct=[50., -50., 0.]), index=[22, 3, 18])
    stop, target = g8.exit_parameters(frame, dict(mode="fixed", stop=.9, target_cap=.75))
    assert stop.tolist() == [.9, .9, .9]
    assert target.tolist() == [.6, .75, .75]
    stop, target = g8.exit_parameters(frame, dict(mode="stress_wider", stop=1.2, target_cap=None))
    assert stop.tolist() == [1.5, 1.2, 1.2]
    assert target.tolist() == [.6, 1.5, 2.]
    stop, target = g8.exit_parameters(frame, dict(mode="vix_ratio", stop=1.2, target_cap=1.))
    assert stop.tolist() == pytest.approx([1.5, 1.02, 1.2])
    assert target.tolist() == pytest.approx([.75, .85, 1.])
    assert stop.index.tolist() == [22, 3, 18]
    frame.loc[3, "vix_previous_close_pct"] = np.nan
    with pytest.raises(ValueError, match="prior-session"):
        g8.exit_parameters(frame, dict(mode="vix_ratio", stop=1.2, target_cap=None))


def _record(policy=g8.BASELINE, **changes):
    return dict(dict(policy=policy, net_pnl_rs=10000., trades=100, win_rate_pct=80.,
                     negative_trade_sum_abs_rs=3000., daily_close_drawdown_rs=1000., losing_days=5),
                **changes)


def test_selection_is_deterministic_and_ignores_extra_future_results():
    records = [_record(), _record("zeta", net_pnl_rs=9500., win_rate_pct=85., losing_days=4),
               _record("alpha", net_pnl_rs=9500., win_rate_pct=85., losing_days=4),
               _record("overfit_high_win", net_pnl_rs=8999., win_rate_pct=99.)]
    assert g8.choose(records) == "alpha"
    changed = [dict(row, final_test_net_pnl_rs=(-10_000_000 if row["policy"] == "alpha" else 10_000_000),
                    final_test_win_rate_pct=(0 if row["policy"] == "alpha" else 100)) for row in reversed(records)]
    assert g8.choose(changed) == "alpha"


@pytest.mark.parametrize("changes", [
    {"trades": 64}, {"negative_trade_sum_abs_rs": 3001},
    {"daily_close_drawdown_rs": 1151}, {"losing_days": 6},
    {"net_pnl_rs": 8999}, {"win_rate_pct": 79},
])
def test_candidate_must_pass_every_development_guard(changes):
    challenger = _record("challenger", win_rate_pct=90.)
    challenger.update(changes)
    assert g8.choose([_record(), challenger]) == g8.BASELINE


def test_validation_can_reject_high_win_rate_with_damaged_profit():
    baseline = _record()
    assert g8.validation_pass(_record("candidate", net_pnl_rs=9000., trades=65,
                                     win_rate_pct=78., daily_close_drawdown_rs=1150.), baseline)
    assert not g8.validation_pass(_record("candidate", net_pnl_rs=8999., win_rate_pct=99.), baseline)
    assert not g8.validation_pass(_record("candidate", net_pnl_rs=-1., win_rate_pct=99.), baseline)


def test_beyond_cutoff_rejected_before_any_output_is_created(tmp_path):
    destination = tmp_path / "must_not_exist"
    with pytest.raises(ValueError, match="2026-10-22"):
        g8.run(through="2026-10-23", output=destination)
    assert not destination.exists()


def test_repeated_allocation_does_not_compound_risk_reduction():
    # apply_fixed_capital_model resets money amounts before each scale operation.
    # A half-size source must remain half-size across cost-sensitivity replays.
    raw = pd.DataFrame([dict(sid=9, day="2026-10-22", tradingsymbol="TEST", side="LONG",
        filled=True, g8_baseline_filled=True, g8_quantity_scale=.5, entry_price=100.,
        entry_path_index=0, entry_ts="2026-10-22 10:01:00+05:30", exit_ts="2026-10-22 10:05:00+05:30",
        confirmation_ts="2026-10-22 10:00:00+05:30", gross_return_pct=.6,
        net_return_pct=.55, cost_pct=.05, initial_stop_pct=2.4)])
    config = dict(capital_per_entry_rs=100000., leverage=5., portfolio_capital_rs=1000000.)
    first = engine.allocate_variant(raw, pd.Series({9:True}), config, "first")
    second = engine.allocate_variant(first, pd.Series({9:True}), config, "second", cost_bps=10.)
    assert first.loc[0, "exposure_per_entry_rupees"] == second.loc[0, "exposure_per_entry_rupees"] == 250000.
    assert first.loc[0, "portfolio_cost_rupees"] == 125.
    assert second.loc[0, "portfolio_cost_rupees"] == 250.
    assert second.loc[0, "portfolio_net_profit_rupees"] == pytest.approx(1250.)
