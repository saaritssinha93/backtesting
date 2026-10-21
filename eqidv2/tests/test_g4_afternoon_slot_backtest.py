"""Isolation, clock correctness and causal selection for G4 afternoon research."""
import copy
import json
import sys

import pandas as pd
import pytest

import fno_v13_replay_source as source
import fno_v13_v10_g_3_contract as production
import fno_v13_v10_g_4_backtest as cli
from research import g4_extended_slot_backtest as study
from research.g4_late_slot_source import expanded_source_clocks


REQUESTED_CLOCKS = tuple(pd.date_range("2000-01-01 12:35", "2000-01-01 13:30", freq="5min").strftime("%H:%M"))


def afternoon_config():
    return study.configuration(study.AFTERNOON_CONFIG)


def candidate(clock, side):
    stamp = pd.Timestamp("2026-10-09 " + clock, tz="Asia/Kolkata")
    return dict(tradingsymbol="STOCK", side=side, signal_timestamp=stamp,
        confirmation_timestamp=stamp + pd.Timedelta(minutes=1),
        v9_1m_feature_ts=stamp + pd.Timedelta(minutes=1), signal_close=100.,
        price_change_pct=.5 if side == "LONG" else -.5, oi_change_pct=.5,
        oi=100500., prev_oi=100000., volume_ratio=10.,
        ema9=102. if side == "LONG" else 98., ema20=101. if side == "LONG" else 99., ema50=100.,
        body_ratio=.9, wick_ratio=.05, traded_value=1000., v9_1m_volume_ratio=10., confirmed=True)


def test_afternoon_profile_uses_all_requested_clocks_and_selected_49_setup_baseline():
    config = afternoon_config()
    profile = study.PROFILES[config["variant_id"]]
    assert tuple(config["signal_ends"]) == REQUESTED_CLOCKS == study.AFTERNOON_CLOCKS
    assert config["sides"] == ["LONG", "SHORT"]
    assert profile["baseline_version"] == "V13-V10-G-4-SELECTED-MIDDAY-TWELVE-V5"
    assert profile["baseline_setups"] == 49
    assert profile["baseline_ledger"] == "combined_trades.csv"
    assert profile["sid_base"] == 7_000_000
    assert len({p["sid_base"] for p in study.PROFILES.values()}) == len(study.PROFILES)
    assert len({str(p["cache"]) for p in study.PROFILES.values()}) == len(study.PROFILES)
    assert config["scope"] == "BACKTEST_ONLY" and config["execution_authority"] is False
    assert (config["initial_stop_pct"], config["tightened_stop_pct"], config["tighten_after_minutes"],
            config["target_pct"], config["entry_expiry_minutes"], config["square_off"]) == (1.25, 1., 120, 1., 10, "15:15")


def test_afternoon_contract_preserves_current_g3_and_restores_archived_state_after_error():
    config = afternoon_config()
    archived_before = copy.deepcopy(study.contract.strategy_payload())
    production_before = copy.deepcopy(production.strategy_payload())
    choices = {f"{clock}_{side}": 0 for clock in REQUESTED_CLOCKS for side in config["sides"]}
    with pytest.raises(RuntimeError, match="restore"):
        with study.setup_context(config, choices):
            assert production.strategy_payload() == production_before
            assert len(study.contract.ACTIVE_SETUPS) == 24
            assert {s.signal_end for s in study.contract.ACTIVE_SETUPS} == set(REQUESTED_CLOCKS)
            assert study.contract.setup_for("09:50", "LONG") is None
            for spec in study.contract.ACTIVE_SETUPS:
                assert spec.setup_id == study.confirmation(spec.signal_end).replace(":", "") + "_" + spec.side
                assert (spec.target_pct, spec.stop_pct, spec.max_entries, spec.picker) == (1., 1.25, 1, "max_liquidity")
                assert spec.source_version == study.AFTERNOON_VERSION
            raise RuntimeError("restore")
    assert study.contract.strategy_payload() == archived_before
    assert production.strategy_payload() == production_before


@pytest.mark.parametrize("clock", REQUESTED_CLOCKS)
@pytest.mark.parametrize("side", ["LONG", "SHORT"])
def test_each_afternoon_setup_requires_exact_next_minute(clock, side):
    config = afternoon_config()
    with study.setup_context(config):
        spec = study.contract.setup_for(clock, side)
        row = candidate(clock, side)
        assert study.contract.passes_selected_filters(row, spec)
        row["confirmation_timestamp"] += pd.Timedelta(minutes=1)
        row["v9_1m_feature_ts"] = row["confirmation_timestamp"]
        assert not study.contract.passes_selected_filters(row, spec)


def test_source_clocks_cover_hour_boundary_and_restore_g3_mapping_even_on_failure():
    original = source.config.SIGNAL_TO_CONFIRMATION
    before = dict(original)
    with pytest.raises(RuntimeError):
        with expanded_source_clocks(REQUESTED_CLOCKS):
            assert source.config.SIGNAL_TO_CONFIRMATION["12:55"] == "12:56"
            assert source.config.SIGNAL_TO_CONFIRMATION["13:00"] == "13:01"
            assert source.config.SIGNAL_TO_CONFIRMATION["13:30"] == "13:31"
            raise RuntimeError("restore")
    assert source.config.SIGNAL_TO_CONFIRMATION is original
    assert source.config.SIGNAL_TO_CONFIRMATION == before


def test_new_profile_reuses_prior_strict_then_1m_then_5m_grids_without_filter_drift():
    new = afternoon_config()
    midday = study.configuration(study.MIDDAY_CONFIG)
    morning = study.configuration(study.DEFAULT_CONFIG)
    assert new["candidate_variants"] == midday["candidate_variants"] == morning["candidate_variants"]
    for side, trials in new["candidate_variants"].items():
        donor = trials[0]["thresholds"]
        assert {trial["stage"] for trial in trials} == set(study.STAGES)
        for trial in trials:
            level = trial["thresholds"]
            assert level["price_change_pct"] >= .1
            assert .05 <= level["oi_change_pct"] <= 1.
            assert level["five_minute_volume_ratio_min"] >= .8
            if trial["stage"] == "relax_1m":
                assert all(level[key] == donor[key] for key in (
                    "price_change_pct", "oi_change_pct", "five_minute_volume_ratio_min"))


@pytest.mark.parametrize("changes", [
    {"execution_authority": True}, {"scope": "LIVE"}, {"signal_ends": ["12:35"]},
    {"sides": ["LONG"]}, {"confirmation_offset_minutes": 5}, {"target_pct": .75},
    {"initial_stop_pct": 1.}, {"tightened_stop_pct": .75}, {"tighten_after_minutes": 60},
    {"stage_order": ["relax_5m", "strict", "relax_1m"]}, {"required_win_rate_pct": 69.},
    {"preserve_existing_g4_orders": False}, {"round_trip_cost_bps": 0.},
])
def test_new_profile_rejects_clock_authority_stop_and_protocol_drift(tmp_path, changes):
    config = afternoon_config()
    config.update(changes)
    path = tmp_path / "invalid.json"
    path.write_text(json.dumps(config), encoding="utf-8")
    with pytest.raises(ValueError):
        study.configuration(path)


def test_new_profile_rejects_relaxing_below_verified_source_envelope(tmp_path):
    config = afternoon_config()
    config["relax_5m"]["LONG"]["volume"].append(.79)
    path = tmp_path / "invalid.json"
    path.write_text(json.dumps(config), encoding="utf-8")
    with pytest.raises(ValueError, match="source envelope"):
        study.configuration(path)


def test_public_cli_routes_explicit_afternoon_profile_to_its_isolated_cache(monkeypatch, capsys):
    seen = []
    output = study.BACKTEST_ROOT / "tests" / "afternoon_cli_only"

    def fake_run(config, folder, *, cache):
        seen.append((config, folder, cache))
        return {key: [] for key in ("sessions", "new_setups", "tested_configurations",
            "training_eligible_setups", "chronological_check_pass_setups", "historical_fit_pass_setups")}

    monkeypatch.setattr(cli, "run_extended", fake_run)
    monkeypatch.setattr(sys, "argv", ["g4", "--config", str(study.AFTERNOON_CONFIG), "--output-dir", str(output)])
    cli.main()
    assert seen == [(study.AFTERNOON_CONFIG, output, study.AFTERNOON_CACHE)]
    assert str(output.resolve()) in capsys.readouterr().out
