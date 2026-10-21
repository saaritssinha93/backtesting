"""Exact G4-to-G3 promotion and preservation of the previous production book."""
from dataclasses import asdict
import json
from pathlib import Path

import pandas as pd
import pytest

import fno_v13_v10_g_3_contract as contract
import fno_v13_v10_g_3_live_config as runtime
import fno_v13_v10_g_3_midday_setups as midday

ROOT = Path(__file__).resolve().parents[1]
SOURCE = json.loads((ROOT / "configs/strategies/v13_v10_g_4_backtest_selected_midday_v5.json").read_text())


def test_exact_twelve_promoted_and_every_previous_rule_is_preserved():
    previous = json.loads((ROOT / "configs/strategies/v13_v10_g_3_production_v5.json").read_text())
    wanted = {s["setup_id"] for s in SOURCE["setups"]}
    assert midday.SETUP_IDS == wanted and len(wanted) == 12
    for family in ("core_setups", "active_setups"):
        now = contract.strategy_payload()[family]
        old = [{**s, "source_version": previous["strategy_version"]} for s in now
               if s["confirmation_end"].replace(":", "") + "_" + s["side"] not in (wanted | contract.afternoon_setups.SETUP_IDS | contract.ADDED_SETUP_IDS)]
        assert old == previous[family]
        assert len(now) == 61
    for key, value in previous["minimum_confirmation_1m_volume_ratio_by_setup"].items():
        assert contract.MIN_CONFIRMATION_VOLUME_BY_SETUP[key] == value
    previous_fingerprint = "fcb7c981b518b8c9e427155cdfb06e3cc34c6ed16525e78514e6ecf3afab2aea"
    assert contract.supported_identity(previous["strategy_version"], previous_fingerprint, allow_legacy=True)
    assert not contract.supported_identity(previous["strategy_version"], previous_fingerprint)
    runtime.validate_strategy()
    assert runtime.PIPELINE_DEADLINE == "13:33"
    assert len(contract.SIGNAL_TO_CONFIRMATION) == 40
    assert (contract.ARMED, contract.EXECUTION_AUTHORITY) == (False, False)


@pytest.mark.parametrize("source", SOURCE["setups"], ids=lambda row: row["setup_id"])
def test_every_promoted_filter_target_quota_and_timing_matches_research(source):
    setup = contract.setup_for(source["signal_end"], source["side"])
    assert setup == contract._CORE_BY_ID[source["setup_id"]]
    assert setup.setup_id == source["setup_id"]
    assert setup.confirmation_end == source["confirmation_end"]
    fields = {"price_change_pct": "price_change_pct", "oi_change_pct": "oi_change_pct",
              "volume_ratio": "five_minute_volume_ratio_min", "body_ratio": "one_minute_body_ratio_min",
              "max_wick_ratio": "one_minute_wick_ratio_max"}
    for actual, expected in fields.items():
        assert getattr(setup, actual) == source["thresholds"][expected]
    assert contract.confirmation_volume_minimum(setup.side, setup_id=setup.setup_id) == source["thresholds"]["one_minute_volume_ratio_min"]
    assert (setup.target_pct, setup.stop_pct, setup.max_entries, setup.picker) == (source["target_pct"], 1.25, 1, "max_liquidity")
    assert (contract.TIGHTENED_STOP_PCT, contract.TIGHTEN_AFTER_MINUTES) == (1., 120)


@pytest.mark.parametrize("clock,side", [("11:45", "LONG"), ("11:50", "LONG"), ("11:50", "SHORT"),
    ("11:55", "LONG"), ("11:55", "SHORT"), ("12:00", "LONG"), ("12:00", "SHORT"),
    ("12:05", "SHORT"), ("12:10", "LONG"), ("12:20", "LONG"), ("12:20", "SHORT"), ("12:25", "LONG")])
def test_unrequested_midday_setups_are_absent(clock, side):
    assert contract.setup_for(clock, side) is None


@pytest.mark.parametrize("setup_id", sorted(midday.SETUP_IDS))
def test_exact_next_minute_highest_liquidity_and_missing_features(setup_id):
    setup = contract._ACTIVE_BY_ID[setup_id]
    stamp = pd.Timestamp("2026-10-14 " + setup.signal_end, tz=contract.IST)
    row = dict(tradingsymbol="VALID", side=setup.side, signal_timestamp=stamp,
        confirmation_timestamp=stamp + pd.Timedelta(minutes=1), v9_1m_feature_ts=stamp + pd.Timedelta(minutes=1),
        signal_close=100., price_change_pct=setup.price_change_pct * (1 if setup.side == "LONG" else -1),
        oi_change_pct=setup.oi_change_pct, oi=100100., prev_oi=100000., volume_ratio=setup.volume_ratio,
        ema9=102. if setup.side == "LONG" else 98., ema20=101. if setup.side == "LONG" else 99., ema50=100.,
        body_ratio=setup.body_ratio, wick_ratio=setup.max_wick_ratio, traded_value=1000.,
        v9_1m_volume_ratio=midday.CONFIRMATION_VOLUME[setup_id], confirmed=True)
    assert contract.passes_selected_filters(row, setup)
    best = dict(row, tradingsymbol="BEST", traded_value=2000.)
    delayed = dict(row, tradingsymbol="DELAYED", traded_value=3000.,
        confirmation_timestamp=stamp + pd.Timedelta(minutes=2), v9_1m_feature_ts=stamp + pd.Timedelta(minutes=2))
    assert [item["tradingsymbol"] for item in contract.rank_candidates([row, delayed, best], setup)] == ["BEST"]
    for missing in (None, float("nan"), float("inf")):
        assert not contract.passes_selected_filters(dict(row, v9_1m_volume_ratio=missing), setup)


def test_runtime_and_backtest_share_the_production_rules_with_explicit_subset():
    from research import g3_backtest_profile as profile
    import fno_v13_v10_g_3_daily_replay as replay
    assert profile.contract is replay.contract
    assert runtime.contract is contract is profile.production
    assert profile.CONFIG_PATH != runtime.CONFIG_PATH
    assert set(profile.contract._ACTIVE_BY_ID) <= set(contract._ACTIVE_BY_ID)
    for setup_id in contract.ADDED_SETUP_IDS:
        actual = asdict(profile.contract._ACTIVE_BY_ID[setup_id])
        expected = asdict(contract._ACTIVE_BY_ID[setup_id])
        actual.pop("source_version")
        expected.pop("source_version")
        assert actual == expected
    assert json.loads(runtime.CONFIG_PATH.read_text()) == contract.strategy_payload()
