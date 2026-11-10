"""Audit integrity and frozen descriptive decision tests; no simulated claims."""
import pandas as pd
import pytest

from . import run as r
from . import selection as s
from .tests_selection import row


def test_fresh_output_cannot_write_into_previous_experiment_or_overwrite():
    with pytest.raises(ValueError, match="NEW child"):
        r.fresh_output(r.PACKAGE.parent / "cvd_kite_proxy_20261102" / "new_result")
    with pytest.raises(FileExistsError, match="overwrite"):
        r.fresh_output(r.PACKAGE / "run.py")


def test_audit_invariants_and_substitution_trace():
    result = s.select_all(pd.DataFrame([row("LEADER", 1, acceleration=2.), row("NEAR", 2, liquidity=950., acceleration=-2.)]))
    original, changed = result["base_control"][1], result["acceleration_rank"][1]
    assert r.verify_audit(original, changed)
    trace = r.decision_groups(original, changed, "acceleration_rank")
    assert trace[0]["selected_changed"] and trace[0]["substitutions"] == 1
    assert trace[0]["original_symbols"] == '["LEADER"]'
    assert trace[0]["selected_symbols"] == '["NEAR"]'
    bad = changed.copy()
    bad.loc[0, "traded_value"] += 1
    with pytest.raises(ValueError, match="source/filter"):
        r.verify_audit(original, bad)
    bad = changed.copy()
    bad["v9_selected"] = True
    with pytest.raises(ValueError, match="quota"):
        r.verify_audit(original, bad)


def test_identical_or_deteriorated_variant_cannot_pass_descriptive_screen():
    rows = [dict(variant=v, period="october", cost_bps=5, net_pnl_rs=1000., daily_close_drawdown_rs=100., trades=100)
            for v in s.VARIANTS]
    groups = pd.DataFrame([dict(variant=v, day="2026-10-23", selected_changed=False, substitutions=0) for v in s.VARIANTS])
    overall = pd.DataFrame(rows)
    assert not any(x["descriptive_screen_pass"] for x in r.qualification(overall, groups))
    groups.loc[groups.variant.ne("base_control"), ["selected_changed", "substitutions"]] = [True, 1]
    overall.loc[overall.variant.eq("acceleration_rank"), "net_pnl_rs"] += 1
    overall.loc[overall.variant.eq("acceleration_rvol_rank"), "net_pnl_rs"] -= 1
    decisions = {x["variant"]: x for x in r.qualification(overall, groups)}
    assert decisions["acceleration_rank"]["descriptive_screen_pass"]
    assert not decisions["acceleration_rvol_rank"]["descriptive_screen_pass"]
    overall.loc[overall.variant.eq("acceleration_rank"), "trades"] = 89
    assert not r.qualification(overall, groups)[0]["descriptive_screen_pass"]


def test_complete_attribution_includes_shared_capital_effects():
    # Synthetic arithmetic fixture only; never exported as backtest performance.
    common = dict(day="2026-10-23", setup_id="S", signal_ts="2026-10-23 09:50:00+05:30", filled=True)
    baseline = pd.DataFrame([dict(common, sid=1, tradingsymbol="A", portfolio_executed=True, portfolio_net_profit_rupees=500.),
                             dict(common, sid=2, tradingsymbol="B", portfolio_executed=True, portfolio_net_profit_rupees=300.)])
    candidate = pd.DataFrame([dict(common, sid=1, tradingsymbol="A", portfolio_executed=False, portfolio_net_profit_rupees=0.),
                              dict(common, sid=3, tradingsymbol="C", portfolio_executed=True, portfolio_net_profit_rupees=700.)])
    detail, proof = r.pnl_attribution(baseline, candidate, "acceleration_rank")
    assert proof["reconciled"] and proof["headline_pnl_delta_rs"] == -100.
    assert detail.pnl_delta_rs.sum() == -100.
    assert set(detail.effect) == {"ADDED_SELECTION", "REMOVED_SELECTION", "SHARED_PORTFOLIO_EFFECT"}
