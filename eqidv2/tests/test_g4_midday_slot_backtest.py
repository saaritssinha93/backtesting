"""Isolation and causal-selection checks for the 11:35-12:30 G4 study."""
import copy
import json
import sys

import pandas as pd
import pytest

import fno_v13_v10_g_3_contract as production
import fno_v13_v10_g_4_backtest as cli
from research import g4_extended_slot_backtest as study
from research.g4_late_slot_source import expanded_source_clocks


REQUESTED_CLOCKS = (
    "11:35", "11:40", "11:45", "11:50", "11:55", "12:00",
    "12:05", "12:10", "12:15", "12:20", "12:25", "12:30",
)


def midday_config():
    return study.configuration(study.MIDDAY_CONFIG)


def test_midday_profile_contains_exactly_requested_pairs_and_restores_state():
    config = midday_config()
    archived_before = study.contract.strategy_payload()
    production_before = production.strategy_payload()
    with pytest.raises(RuntimeError, match="interrupted research"):
        with study.setup_context(config):
            specs = study.contract.ACTIVE_SETUPS
            assert len(specs) == 24
            assert {(s.signal_end, s.side) for s in specs} == {
                (clock, side) for clock in REQUESTED_CLOCKS for side in ("LONG", "SHORT")
            }
            assert study.contract.STRATEGY_VERSION == study.MIDDAY_VERSION
            assert production.strategy_payload() == production_before
            for spec in specs:
                assert spec.source_version == study.MIDDAY_VERSION
                assert spec.confirmation_end == study.confirmation(spec.signal_end)
                assert spec.stop_pct == 1.25
                assert spec.target_pct == 1.0
            raise RuntimeError("interrupted research")
    assert study.contract.strategy_payload() == archived_before
    assert production.strategy_payload() == production_before


@pytest.mark.parametrize("clock,confirmation", [("11:55", "11:56"), ("12:00", "12:01"), ("12:30", "12:31")])
def test_source_and_selector_agree_on_exact_next_minute(clock, confirmation):
    import fno_v13_replay_source as source

    before = source.config.SIGNAL_TO_CONFIRMATION
    with expanded_source_clocks(REQUESTED_CLOCKS):
        assert source.config.SIGNAL_TO_CONFIRMATION[clock] == confirmation
    assert source.config.SIGNAL_TO_CONFIRMATION is before
    stamp = pd.Timestamp("2026-10-09 " + clock, tz="Asia/Kolkata")
    row = dict(tradingsymbol="TEST", side="LONG", signal_timestamp=stamp,
        confirmation_timestamp=stamp + pd.Timedelta(minutes=1),
        v9_1m_feature_ts=stamp + pd.Timedelta(minutes=1), signal_close=100.,
        price_change_pct=.2, oi_change_pct=.1, oi=100100., prev_oi=100000.,
        volume_ratio=2., ema9=102., ema20=101., ema50=100.,
        body_ratio=.8, wick_ratio=.1, traded_value=1000., v9_1m_volume_ratio=2., confirmed=True)
    with study.setup_context(midday_config()):
        spec = study.contract.setup_for(clock, "LONG")
        assert spec.confirmation_end == confirmation
        assert study.contract.passes_selected_filters(row, spec)
        for offset in (0, 2, 5):
            delayed = dict(row, confirmation_timestamp=stamp + pd.Timedelta(minutes=offset),
                           v9_1m_feature_ts=stamp + pd.Timedelta(minutes=offset))
            assert not study.contract.passes_selected_filters(delayed, spec)


@pytest.mark.parametrize("field,value", [
    ("signal_ends", list(REQUESTED_CLOCKS[:-1])),
    ("sides", ["LONG"]),
    ("target_pct", .75),
    ("initial_stop_pct", 1.),
    ("tighten_after_minutes", 60),
    ("confirmation_offset_minutes", 5),
    ("stage_order", ["relax_5m", "strict", "relax_1m"]),
    ("execution_authority", True),
])
def test_midday_profile_rejects_request_or_protocol_drift(tmp_path, field, value):
    config = json.loads(study.MIDDAY_CONFIG.read_text(encoding="utf-8"))
    config[field] = value
    path = tmp_path / "changed.json"
    path.write_text(json.dumps(config), encoding="utf-8")
    with pytest.raises(ValueError):
        study.configuration(path)


def test_midday_relaxes_features_in_order_without_using_later_profit():
    config = midday_config()
    rows = [
        dict(index=0, stage="strict", train_net_pnl_rs=100., train_fills=5,
             train_win_rate_pct=80., train_max_drawdown_rs=10.),
        dict(index=1, stage="relax_1m", train_net_pnl_rs=1000., train_fills=5,
             train_win_rate_pct=80., train_max_drawdown_rs=10.),
        dict(index=2, stage="relax_5m", train_net_pnl_rs=10000., train_fills=5,
             train_win_rate_pct=80., train_max_drawdown_rs=10.),
    ]
    picked, passed = study.choose(rows, config)
    assert passed and picked["index"] == 0
    later_changed = copy.deepcopy(rows)
    later_changed[0].update(holdout_win_rate_pct=0., all_net_pnl_rs=-1e9)
    later_changed[2].update(holdout_win_rate_pct=100., all_net_pnl_rs=1e9)
    assert study.choose(later_changed, config)[0]["index"] == 0
    later_changed[0]["train_win_rate_pct"] = 60.
    assert study.choose(later_changed, config)[0]["index"] == 1
    later_changed[1]["train_fills"] = 4
    assert study.choose(later_changed, config)[0]["index"] == 2


def test_midday_and_previous_profiles_keep_separate_clocks_and_source_cache():
    old = study.configuration()
    new = midday_config()
    assert old["variant_id"] == study.VERSION
    assert tuple(old["signal_ends"]) == study.CLOCKS
    assert new["variant_id"] == study.MIDDAY_VERSION
    assert tuple(new["signal_ends"]) == REQUESTED_CLOCKS
    assert not set(old["signal_ends"]) & set(new["signal_ends"])
    assert study.MIDDAY_CACHE != study.DEFAULT_CACHE
    assert (new["initial_stop_pct"], new["tightened_stop_pct"], new["tighten_after_minutes"]) == (1.25, 1., 120)


def test_explicit_midday_cli_dispatches_new_profile_and_separate_cache(monkeypatch, tmp_path):
    called = []
    monkeypatch.setattr(cli, "BACKTEST_ROOT", tmp_path)
    monkeypatch.setattr(sys, "argv", ["g4", "--config", str(study.MIDDAY_CONFIG)])

    def run_extended(config, output, *, cache):
        called.append((config, output, cache))
        return {key: 0 for key in (
            "sessions", "new_setups", "tested_configurations", "training_eligible_setups",
            "chronological_check_pass_setups", "historical_fit_pass_setups",
        )}

    def forbidden(*args, **kwargs):
        pytest.fail("An explicit midday request must dispatch the midday research profile")

    monkeypatch.setattr(cli, "run_extended", run_extended)
    monkeypatch.setattr(cli, "run_selected", forbidden)
    monkeypatch.setattr(cli, "run_original", forbidden)
    cli.main()
    assert len(called) == 1
    assert called[0][0] == study.MIDDAY_CONFIG
    assert called[0][2] == study.MIDDAY_CACHE
    assert called[0][1].resolve().is_relative_to(tmp_path.resolve())


def test_invalid_source_liquidity_is_excluded_before_outcomes_are_examined():
    source = pd.DataFrame([
        dict(sid=1, traded_value=-14291., filled=True, net_return_pct=1.),
        dict(sid=2, traded_value=float("nan"), filled=True, net_return_pct=1.),
        dict(sid=3, traded_value=float("inf"), filled=True, net_return_pct=1.),
        dict(sid=4, traded_value=100., filled=False, net_return_pct=None),
        dict(sid=5, traded_value=0., filled=True, net_return_pct=-1.),
    ])
    accepted, rejected = study.screen_source_liquidity(source)
    assert accepted.sid.tolist() == [4, 5]
    assert rejected.sid.tolist() == [1, 2, 3]
    assert set(rejected.source_rejection_reason) == {"NONFINITE_OR_NEGATIVE_TRADED_VALUE"}
    source["net_return_pct"] = [999., -999., 0., 100., -100.]
    assert study.screen_source_liquidity(source)[0].sid.tolist() == [4, 5]
