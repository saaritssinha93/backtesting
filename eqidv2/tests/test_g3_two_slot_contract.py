"""Both requested slots share entry rules and 1.5% targets across consumers."""
from dataclasses import asdict, replace
import json
from pathlib import Path

import pandas as pd
import pytest

import fno_v13_v10_g_3_contract as production
import fno_v13_v10_g_3_live_config as runtime
from research import g3_target_cutoff_contract as backtest


def candidate(side, slot, **changes):
    stamp = pd.Timestamp(f"2026-10-16 {slot}", tz=production.IST)
    short = side == "SHORT"
    row = dict(tradingsymbol="VALID", side=side, signal_timestamp=stamp,
        confirmation_timestamp=stamp + pd.Timedelta(minutes=1),
        v9_1m_feature_ts=stamp + pd.Timedelta(minutes=1),
        price_change_pct=-.3 if short else .3, oi_change_pct=.1, oi=100100., prev_oi=100000.,
        volume_ratio=2., body_ratio=.6, wick_ratio=.2, traded_value=1e7,
        ema9=98. if short else 102., ema20=99. if short else 101., ema50=100.,
        v9_1m_volume_ratio=1.2 if short else .6, nifty_first_bar_return_pct=-.05,
        confirmed=True, trigger=99. if short else 101.,
        opening_0920_status="VALID", price_change_basis="09:15_CASH_OPEN_TO_09:20_CASH_CLOSE",
        oi_change_source="FIRST_MINUTE_0916_TO_COMPLETED_0920_FUTURES_4M")
    row.update(changes)
    return row


@pytest.mark.parametrize("slot,side,donor,minimum", [
    ("09:20", "SHORT", "0926_SHORT", 1.2), ("10:10", "LONG", "0951_LONG", .6)])
def test_new_slot_rules_match_donor_and_target_in_every_contract(slot, side, donor, minimum):
    spec = production.setup_for(slot, side)
    for rules in (production._CORE_BY_ID, production._ACTIVE_BY_ID):
        assert rules[spec.setup_id] == replace(rules[donor], signal_end=slot,
                                              confirmation_end=spec.confirmation_end, target_pct=1.5)
    for contract in (production, runtime, backtest):
        configured = contract.setup_for(slot, side)
        assert configured.target_pct == 1.5
        assert configured.stop_pct == 1.25
        assert contract.confirmation_volume_minimum(side, signal_end=slot) == minimum
        good = candidate(side, slot)
        assert contract.passes_selected_filters(good, configured)
        assert not contract.passes_selected_filters({**good, "v9_1m_volume_ratio": minimum - .00001}, configured)
        chosen = contract.select_orders(pd.DataFrame([good]))
        assert chosen.setup_id.tolist() == [spec.setup_id]
        assert chosen.native_target_pct.tolist() == [1.5]


def test_existing_61_rules_except_0926_volume_and_side_defaults_are_preserved():
    path = Path(__file__).resolve().parents[1] / "configs/strategies/v13_v10_g_3_production_v10.json"
    old = json.loads(path.read_text(encoding="utf-8"))
    current = production.strategy_payload()
    for key in ("core_setups", "active_setups"):
        rows = [{**row, "source_version": old["strategy_version"],
                 "volume_ratio": 3.0 if row["confirmation_end"] == "09:26" and row["side"] == "LONG"
                 else row["volume_ratio"]} for row in current[key]]
        assert rows == old[key]
    assert set(production._ACTIVE_BY_ID) - set(backtest._ACTIVE_BY_ID) == set(backtest.DISABLED_IDS)
    assert production.ADDED_SETUP_IDS <= set(backtest._ACTIVE_BY_ID)
    assert len(production.ACTIVE_SETUPS) == 61 and len(backtest.ACTIVE_SETUPS) == 51
    assert old["minimum_confirmation_1m_volume_ratio"] == current["minimum_confirmation_1m_volume_ratio"]
    assert production.supported_identity(old["strategy_version"], production.PREVIOUS_STRATEGY_FINGERPRINT, allow_legacy=True)
    assert not production.supported_identity(old["strategy_version"], production.PREVIOUS_STRATEGY_FINGERPRINT)


def test_0926_long_five_minute_volume_120_applies_to_backtest_paper_and_live_selection():
    good = candidate("LONG", "09:25", price_change_pct=.31, volume_ratio=1.2,
                     v9_1m_volume_ratio=1.1)
    for contract in (production, runtime, backtest):
        setup = contract.setup_for("09:25", "LONG")
        assert setup.volume_ratio == 1.2
        assert contract.passes_selected_filters(good, setup)
        assert not contract.passes_selected_filters({**good, "volume_ratio": 1.19999}, setup)
        assert contract.select_orders(pd.DataFrame([good])).setup_id.tolist() == ["0926_LONG"]


def test_pre_volume_change_backtest_identity_remains_readable():
    from research.archived_g3_v12 import contract as old
    assert old._ACTIVE_BY_ID["0926_LONG"].volume_ratio == 3.0
    assert backtest.contract_for_identity(old.STRATEGY_VERSION, old.strategy_fingerprint()) is old


@pytest.mark.parametrize("field,value", [("nifty_first_bar_return_pct", None),
    ("nifty_first_bar_return_pct", -.04999), ("opening_0920_status", "MISSING"),
    ("price_change_basis", "PREVIOUS_CLOSE"), ("oi_change_source", "PREVIOUS_SESSION")])
def test_new_opening_short_requires_same_day_opening_inputs_and_nifty(field, value):
    row = candidate("SHORT", "09:20", **{field: value})
    for contract in (runtime, backtest):
        assert not contract.passes_selected_filters(row, contract.setup_for("09:20", "SHORT"))


def test_reliance_1011_still_needs_body_and_wick_thresholds_after_enablement():
    raw = candidate("LONG", "10:10", price_change_pct=.58937442115,
        oi_change_pct=.07623741146, volume_ratio=2.16141035, v9_1m_volume_ratio=2.19519798,
        body_ratio=.1764705882, wick_ratio=.5294117647)
    setup = production.setup_for("10:10", "LONG")
    assert setup.body_ratio == .5 and setup.max_wick_ratio == .5
    assert not runtime.passes_selected_filters(raw, setup)
    assert not backtest.passes_selected_filters(raw, backtest.setup_for("10:10", "LONG"))


def test_pre_addition_backtest_identity_remains_readable():
    from research.archived_g3_v11 import contract as old
    assert len(old.ACTIVE_SETUPS) == 49
    assert backtest.contract_for_identity(old.STRATEGY_VERSION, old.strategy_fingerprint()) is old
    assert not backtest.supported_identity(old.STRATEGY_VERSION, old.strategy_fingerprint())
