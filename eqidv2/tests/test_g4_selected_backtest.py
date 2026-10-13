"""Selection, isolation and accounting checks for the selected ten G4 additions."""
import json
import sys

import pandas as pd
import pytest

import fno_v13_v10_g_3_contract as production
import fno_v13_v10_g_4_backtest as cli
from research import g4_selected_backtest as study


@pytest.mark.parametrize("mutation", ["extra", "missing", "duplicate", "target", "stop", "clock", "disabled", "authority"])
def test_rejects_changes_to_requested_subset_and_execution_policy(tmp_path, mutation):
    config = study.configuration()
    row = config["setups"][0]
    if mutation == "extra":
        config["setups"].append(dict(row, setup_id="1041_SHORT", signal_end="10:40", confirmation_end="10:41", side="SHORT"))
    elif mutation == "missing":
        config["setups"].pop()
    elif mutation == "duplicate":
        config["setups"][-1] = row.copy()
    elif mutation == "target":
        row["target_pct"] = 1.
    elif mutation == "stop":
        row["tightened_stop_pct"] = .5
    elif mutation == "clock":
        row["signal_end"] = "10:34"
    elif mutation == "disabled":
        row["enabled"] = False
    else:
        config["execution_authority"] = True
    path = tmp_path / "config.json"
    path.write_text(json.dumps(config), encoding="utf-8")
    with pytest.raises(ValueError):
        study.configuration(path)


def test_selected_context_excludes_other_sides_and_restores_archived_state_after_error():
    archived_before = study.contract.ACTIVE_SETUPS
    production_before = production.ACTIVE_SETUPS
    profile_before = study.contract.STRATEGY_VERSION
    with pytest.raises(RuntimeError, match="test interruption"):
        with study.selected_setups(study.configuration()):
            assert production.ACTIVE_SETUPS is production_before
            assert {s.setup_id for s in study.contract.ACTIVE_SETUPS} == set(study.SELECTED_TARGETS)
            for clock, side in [("10:40", "SHORT"), ("10:50", "SHORT"), ("10:55", "LONG"),
                                ("11:00", "LONG"), ("11:05", "SHORT"), ("11:15", "LONG"),
                                ("11:25", "LONG"), ("11:30", "SHORT")]:
                assert study.contract.setup_for(clock, side) is None
            raise RuntimeError("test interruption")
    assert study.contract.ACTIVE_SETUPS is archived_before
    assert study.contract.STRATEGY_VERSION == profile_before
    assert production.ACTIVE_SETUPS is production_before


def candidate(clock, side, sid, **changes):
    stamp = pd.Timestamp("2026-10-09 " + clock, tz="Asia/Kolkata")
    row = dict(sid=sid, tradingsymbol=f"STOCK{sid}", side=side,
        signal_timestamp=stamp, confirmation_timestamp=stamp + pd.Timedelta(minutes=1),
        v9_1m_feature_ts=stamp + pd.Timedelta(minutes=1), signal_close=100.,
        price_change_pct=.2 if side == "LONG" else -.2, oi_change_pct=.2,
        oi=100200., prev_oi=100000., volume_ratio=2.,
        ema9=102. if side == "LONG" else 98., ema20=101. if side == "LONG" else 99., ema50=100.,
        body_ratio=.8, wick_ratio=.1, traded_value=1000., v9_1m_volume_ratio=2., confirmed=True)
    return dict(row, **changes)


def test_selection_obeys_exact_clock_liquidity_and_allowed_side():
    rows = [candidate("10:40", "LONG", 1), candidate("10:40", "LONG", 2, traded_value=2000.),
            candidate("10:40", "SHORT", 3, traded_value=100000.)]
    delayed = candidate("10:40", "LONG", 4, traded_value=3000.)
    delayed["confirmation_timestamp"] += pd.Timedelta(minutes=1)
    delayed["v9_1m_feature_ts"] = delayed["confirmation_timestamp"]
    with study.selected_setups(study.configuration()):
        audit = study.contract.selection_audit(pd.DataFrame(rows + [delayed]))
    selected = audit.loc[audit.v9_selected.eq(True)]
    assert selected.sid.tolist() == [2]
    assert selected.native_target_pct.tolist() == [1.71]
    assert selected.native_stop_pct.tolist() == [1.25]
    assert 3 not in audit.sid.tolist()


def test_relaxed_confirmation_still_rejects_missing_next_minute_and_low_volume():
    row = candidate("10:55", "SHORT", 1, body_ratio=.1, wick_ratio=.8, v9_1m_volume_ratio=.8)
    with study.selected_setups(study.configuration()):
        spec = study.contract.setup_for("10:55", "SHORT")
        assert study.contract.passes_selected_filters(row, spec)
        assert not study.contract.passes_selected_filters(dict(row, v9_1m_volume_ratio=.79), spec)
        assert not study.contract.passes_selected_filters(dict(row, confirmation_timestamp=None), spec)


def test_metrics_exclude_capital_rejected_profit_and_include_empty_days():
    ledger = pd.DataFrame([
        dict(day="2026-10-08", filled=True, portfolio_executed=True,
             portfolio_net_profit_rupees=-100., portfolio_gross_profit_rupees=-90., portfolio_cost_rupees=10.),
        dict(day="2026-10-09", filled=True, portfolio_executed=True,
             portfolio_net_profit_rupees=300., portfolio_gross_profit_rupees=310., portfolio_cost_rupees=10.),
        dict(day="2026-10-09", filled=True, portfolio_executed=False,
             portfolio_net_profit_rupees=0., portfolio_gross_profit_rupees=0., portfolio_cost_rupees=0.),
    ])
    result = study.metrics(ledger, ["2026-10-07", "2026-10-08", "2026-10-09"])
    assert result["executed_trades"] == 2 and result["capital_rejected_fills"] == 1
    assert result["win_rate_pct"] == 50.
    assert result["net_pnl_rs"] == 200. and result["max_drawdown_rs"] == 100.
    assert result["profit_factor"] == 3. and result["flat_sessions"] == 1


def test_default_cli_uses_selected_profile_without_running_a_new_sweep(monkeypatch, tmp_path):
    called = []
    monkeypatch.setattr(cli, "BACKTEST_ROOT", tmp_path)
    monkeypatch.setattr(sys, "argv", ["g4"])
    def run_selected(config, output, *, cache):
        called.append((config, output, cache))
        return {key: 0 for key in ("sessions", "added_setups", "total_setups", "baseline_g4",
            "selected_additions", "combined_g4", "incremental_net_pnl_rs", "baseline_trades_changed")}
    def forbidden(*args, **kwargs):
        pytest.fail("The default command must not run an earlier research sweep")
    monkeypatch.setattr(cli, "run_selected", run_selected)
    monkeypatch.setattr(cli, "run_extended", forbidden)
    monkeypatch.setattr(cli, "run_original", forbidden)
    cli.main()
    assert len(called) == 1 and called[0][0] == study.DEFAULT_CONFIG
