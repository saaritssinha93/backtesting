"""Contract and causal-execution tests for the staged V13-v10-G-2 replay."""
import copy

import numpy as np
import pandas as pd
import pytest

import fno_v13_v10_g_2_backtest as g2


def source_config():
    return {
        "version": "V13-v10-G-NEW",
        "source_version": "V13-v10-F",
        "selection_change": {
            "price_multiplier": 0.65,
            "oi_multiplier": 1.0,
            "body_reduction": 0.0,
            "wick_increase": 0.0,
            "extra_setup_entries": 0,
            "expansion_side": "SHORT",
        },
        "core_first": True,
        "exit": {
            "version": "V13-v10-B",
            "default": {"stop_pct": 0.82, "target_pct": 1.23},
            "setups": {
                "0926_LONG": {"stop_pct": 0.60, "target_pct": 0.97},
                "0931_SHORT": {"stop_pct": 0.89, "target_pct": 2.00},
            },
            "partial_exits": False,
            "breakeven_stop": False,
        },
        "minimum_confirmation_1m_volume_ratio": 1.2,
        "entry_expiry_minutes": 10,
        "portfolio_capital_rupees": 1_000_000.0,
        "capital_per_entry_rupees": 100_000.0,
        "leverage_factor": 5.0,
        "max_positions": None,
        "cost_bps": 5.0,
        "partial_exits": False,
        "breakeven_stop": False,
        "source_oi_floor_pct": 0.05,
        "evidence": "SOURCE",
    }


def test_every_initial_stop_is_125_percent_and_targets_are_unchanged():
    source = source_config()
    result = g2.config(source)
    assert result["version"] == "V13-v10-G-2"
    assert result["exit"]["default"]["stop_pct"] == 1.25
    assert result["exit"]["default"]["target_pct"] == source["exit"]["default"]["target_pct"]
    for setup_id, pair in result["exit"]["setups"].items():
        assert pair["stop_pct"] == 1.25
        assert pair["target_pct"] == source["exit"]["setups"][setup_id]["target_pct"]
    assert source == source_config(), "the source G configuration must not be mutated"
    assert result["exit"]["scheduled_tightening"]["after_minutes"] == 120
    assert result["exit"]["scheduled_tightening"]["stop_pct"] == 1.0


@pytest.mark.parametrize("mutation", ["stop", "target", "selection", "partial", "timer", "tightened_stop"])
def test_fixed_g2_contract_rejects_drift(mutation):
    source = source_config()
    candidate = g2.config(source)
    if mutation == "stop":
        candidate["exit"]["setups"]["0926_LONG"]["stop_pct"] = 0.99
    elif mutation == "target":
        candidate["exit"]["setups"]["0926_LONG"]["target_pct"] = 1.00
    elif mutation == "selection":
        candidate["selection_change"]["price_multiplier"] = 0.75
    elif mutation == "partial":
        candidate["partial_exits"] = True
    elif mutation == "timer":
        candidate["exit"]["scheduled_tightening"]["after_minutes"] = 119
    else:
        candidate["exit"]["scheduled_tightening"]["stop_pct"] = 0.9
    with pytest.raises(ValueError, match="staged 1.25% to 1.00% stop experiment"):
        g2.checked_settings(candidate, source)


def test_exit_validator_rejects_setup_key_changes():
    source = source_config()["exit"]
    candidate = g2.transformed_exit(source)
    del candidate["setups"]["0931_SHORT"]
    with pytest.raises(ValueError, match="setup keys"):
        g2.validate_exit(candidate, source)


def test_configuration_has_no_execution_authority():
    result = g2.config(copy.deepcopy(source_config()))
    assert result["live_configuration_changed"] is False
    assert result["execution_authority"] is False
    assert result["evidence"] == g2.EVIDENCE


def path(rows, end_minutes):
    return {
        "timestamp_ns": np.array(end_minutes, dtype=np.int64) * g2.MINUTE_NS,
        **{key: np.array([r[i] for r in rows], dtype=float)
           for i, key in enumerate(("open", "high", "low", "close"))},
    }


@pytest.mark.parametrize("is_long", [True, False])
def test_tightening_activates_at_first_eligible_open_not_prior_close(is_long):
    rows = [(100, 100.1, 99.9, 100), (100, 100.1, 98.9, 99), (98.8, 99.3, 98.7, 99)]
    if not is_long:
        rows = [(200-o, 200-l, 200-h, 200-c) for o, h, l, c in rows]
    # Entry end=1, activation=121: end=121 bar still has original stop.
    p = path(rows, [1, 121, 122])
    result = g2.staged_exit(p, 0, 100, is_long, 2)
    assert result == (2, 98.8 if is_long else 101.2, "TIGHTENED_STOP", 1.0, "OPEN")


@pytest.mark.parametrize("is_long", [True, False])
def test_initial_stop_remains_active_and_same_bar_ties_are_stop_first(is_long):
    p = path([(100, 103, 97, 100)], [1])
    assert g2.staged_exit(p, 0, 100, is_long, 2) == (
        0, 98.75 if is_long else 101.25, "STOP", 1.25, "INTRABAR"
    )


def test_target_and_session_exit_are_unchanged():
    p = path([(100, 100.1, 99.9, 100), (100, 102.1, 99.9, 102)], [1, 2])
    assert g2.staged_exit(p, 0, 100, True, 2) == (1, 102, "TARGET", 1.25, "INTRABAR")
    p["high"][1] = 100.5
    p["close"][1] = 100.4
    assert g2.staged_exit(p, 0, 100, True, 2) == (1, 100.4, "TIME_EXIT_1515", 1.25, "CLOSE")


def test_timer_uses_entry_not_path_start():
    p = path([(100, 100.1, 99.9, 100), (100, 100.1, 99.9, 100),
              (100, 100.1, 98.9, 99)], [1, 100, 122])
    assert g2.staged_exit(p, 1, 100, True, 2)[2:] == ("TIME_EXIT_1515", 1.25, "CLOSE")


def test_legacy_fixed_configuration_cannot_be_run_as_staged():
    source = source_config()
    legacy = g2.config(source, legacy_fixed=True)
    assert legacy["exit"]["default"]["stop_pct"] == 1.0
    assert "scheduled_tightening" not in legacy["exit"]
    with pytest.raises(ValueError):
        g2.checked_settings(legacy, source)


@pytest.mark.parametrize("is_long", [True, False])
def test_staged_adapter_recomputes_flags_costs_timestamps_and_excursions(monkeypatch, is_long):
    rows = [(100, 100.1, 99.9, 100), (100, 100.1, 98.9, 99), (98.8, 104, 95, 100)]
    if not is_long:
        rows = [(200-o, 200-l, 200-h, 200-c) for o, h, l, c in rows]
    p = path(rows, [1, 121, 122])
    side = "LONG" if is_long else "SHORT"
    orders = pd.DataFrame([
        dict(sid=1, side=side, native_target_pct=2.0),
        dict(sid=2, side=side, native_target_pct=2.0),
    ])
    monkeypatch.setattr(g2.g.v9.v5, "_entry",
                        lambda row, *a, **kw: (0, 100.0, 100.0, False, 0.0) if row.sid == 1 else None)
    result = g2.simulate_staged(orders, {1: p, 2: p}, cost_bps=5, max_entry_delay_minutes=10)
    row = result.iloc[0]
    assert row.exit_reason == "TIGHTENED_STOP"
    assert row.exit_event == "OPEN"
    assert row.exit_gap_through
    assert row.exit_gap_bps == pytest.approx(abs(row.exit_price / (99 if is_long else 101) - 1) * 10000)
    assert row.stop_hit and not row.target_hit and not row.same_bar_ambiguous
    assert not row.first_target_hit and not row.runner_target_hit
    assert row.initial_stop_pct == 1.25 and row.active_stop_pct_at_exit == 1.0
    assert row.gross_return_pct == pytest.approx(-1.2)
    assert row.net_return_pct == pytest.approx(-1.25)
    assert row.holding_minutes == 120
    assert pd.Timestamp(row.exit_bar_end_ts) - row.exit_ts == pd.Timedelta(minutes=1)
    assert row.mfe_pct == pytest.approx(0.1)
    assert row.mae_pct == pytest.approx(-1.2), "must exclude post-exit extremes"
    assert not result.iloc[1].filled and result.iloc[1].exit_reason == "UNFILLED"
    assert "native_stop_pct" not in orders, "caller orders must remain unchanged"
