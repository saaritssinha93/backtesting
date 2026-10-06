"""Dated G promotion: original priority, exact gates, and causal staged stops."""
from datetime import date

import numpy as np
import pandas as pd
import pytest

import fno_v13_v10_g_daily_replay as replay
import fno_v13_v10_g_selection as selection
import fno_v13_v10_g_staged_replay as staged


DAY = date(2026, 10, 6)


def raw_row(**changes):
    value = dict(day=DAY, tradingsymbol="EXAMPLE", signal_ts=f"{DAY}T09:25:00+05:30",
                 confirmation_ts=f"{DAY}T09:26:00+05:30", oi=100120., prev_oi=100000.,
                 oi_change_pct=1.2, price_change_pct=.30, volume_ratio=1.75,
                 signal_close=100., confirmation_open=100., confirmation_high=101.,
                 confirmation_low=99.9, confirmation_close=100.7, body_ratio=.54,
                 v9_1m_upper_wick_ratio=.30, v9_1m_lower_wick_ratio=.10, v9_1m_volume_ratio=1.2, traded_value=100.,
                 v9_1m_feature_ts=f"{DAY}T09:26:00+05:30", v9_exact_confirmation_present=True,
                 v9_5m_ema_bull=False, v9_5m_ema_bear=False, hhmm="0925", hhmm_int=925)
    value.update(changes)
    return value


def original(**changes):
    value = dict(day=DAY, sid=4, setup_id="0931_LONG", side="LONG",
                 tradingsymbol="ORIGINAL", hhmm_int=930)
    value.update(changes)
    return pd.DataFrame([value])


def test_activation_is_session_date_based_and_preserves_original_book():
    old = date(2026, 10, 5)
    before = original(day=old)
    historical, audit = selection.apply_relaxed_0925_long(before, pd.DataFrame([raw_row()]), session_date=old)
    pd.testing.assert_frame_equal(historical, before)
    assert audit.empty
    promoted, audit = selection.apply_relaxed_0925_long(original(), pd.DataFrame([raw_row()]), session_date=DAY)
    assert len(promoted) == 2 and audit.relaxed_0925_added.all()
    assert promoted.loc[promoted.sid.eq(4), "tradingsymbol"].tolist() == ["ORIGINAL"]


def test_original_priority_and_one_order_liquidity_quota():
    observed = pd.DataFrame([raw_row(tradingsymbol="ZZZ"), raw_row(tradingsymbol="AAA")])
    combined, _ = selection.apply_relaxed_0925_long(original(), observed, session_date=DAY)
    assert combined.loc[combined.relaxed_0925_added, "tradingsymbol"].tolist() == ["AAA"]
    combined, audit = selection.apply_relaxed_0925_long(
        original(setup_id="0926_LONG", hhmm_int=925), observed, session_date=DAY)
    assert combined.tradingsymbol.tolist() == ["ORIGINAL"]
    assert audit.original_slot_occupied.all() and not audit.relaxed_0925_added.any()


@pytest.mark.parametrize("changes", [
    dict(oi_change_pct=1.2001), dict(oi_change_pct=.0999), dict(volume_ratio=1.7499),
    dict(body_ratio=.5399), dict(price_change_pct=.2999), dict(v9_1m_volume_ratio=1.1999),
    dict(v9_1m_upper_wick_ratio=.6001), dict(confirmation_source_flagged=True),
    dict(v9_exact_confirmation_present=False), dict(traded_value=np.nan),
    dict(confirmation_high=100.5), dict(confirmation_low=100.1),
    dict(confirmation_open=0.), dict(confirmation_low=-1.), dict(signal_close=0.),
    dict(v9_1m_feature_ts=f"{DAY}T09:27:00+05:30"),
    dict(signal_ts=f"{DAY}T09:30:00+05:30", confirmation_ts=f"{DAY}T09:31:00+05:30"),
])
def test_relaxation_keeps_all_other_gates_and_clocks(changes):
    combined, _ = selection.apply_relaxed_0925_long(original(), pd.DataFrame([raw_row(**changes)]), session_date=DAY)
    assert combined.tradingsymbol.tolist() == ["ORIGINAL"]


def test_replay_can_add_raw_ema_rejection_without_identity_collision():
    settings = replay.config.load_frozen_config()
    base = replay.g.v9.V9Config()
    observed = pd.DataFrame([raw_row()])
    signals, audit, orders, relaxed = replay.select_day_orders(replay._empty_signals(), observed, base, settings, DAY)
    assert len(signals) == len(orders) == 1
    assert orders.iloc[0].configured_confirmation_end == "09:26"
    assert orders.iloc[0].side == "LONG" and orders.iloc[0].setup_id == "0926_LONG"
    assert orders.iloc[0].signal_id == replay.canonical_signal_id(
        replay.config.STRATEGY_VERSION, DAY, "0925", "09:26", "LONG", "EXAMPLE")
    assert audit.v9_selected.all() and relaxed.relaxed_0925_added.all()


def test_preexisting_strict_candidate_reuses_its_canonical_identity():
    observations = pd.DataFrame([raw_row(v9_5m_ema_bull=True, oi_change_pct=.2)])
    strict = replay._strict_signals(observations, -.1)
    strict["sid"] = 17
    strict["signal_id"] = replay.canonical_signal_id(
        replay.config.STRATEGY_VERSION, DAY, "0925", "09:26", "LONG", "EXAMPLE")
    signals, _, orders, _ = replay.select_day_orders(
        strict, observations, replay.g.v9.V9Config(), replay.config.load_frozen_config(), DAY)
    assert len(signals) == len(orders) == 1
    assert orders.sid.tolist() == [17]


def test_retained_original_order_snapshot_has_the_promoted_stop_schedule():
    observations = pd.DataFrame([raw_row(v9_5m_ema_bull=True, oi_change_pct=.2,
                                          body_ratio=.60, volume_ratio=3.)])
    strict = replay._strict_signals(observations, -.1)
    strict["sid"] = 17
    strict["signal_id"] = replay.canonical_signal_id(
        replay.config.STRATEGY_VERSION, DAY, "0925", "09:26", "LONG", "EXAMPLE")
    _, _, orders, raw_audit = replay.select_day_orders(
        strict, observations, replay.g.v9.V9Config(), replay.config.load_frozen_config(), DAY)
    assert orders.sid.tolist() == [17] and not raw_audit.relaxed_0925_added.any()
    assert orders.native_stop_pct.tolist() == [1.25]
    assert orders.tightened_stop_pct.tolist() == [1.]
    assert orders.tighten_after_minutes.tolist() == [120]


@pytest.mark.parametrize("is_long", [True, False])
def test_staged_replay_waits_full_120_minutes_and_prices_gap_at_open(is_long):
    rows = [(100, 100.1, 99.9, 100), (100, 100.1, 98.9, 99), (98.8, 99.3, 98.7, 99)]
    if not is_long:
        rows = [(200-o, 200-l, 200-h, 200-c) for o, h, l, c in rows]
    path = {"timestamp_ns": np.array([1, 121, 122]) * staged.MINUTE_NS,
            **{key: np.array([row[i] for row in rows]) for i, key in enumerate(("open", "high", "low", "close"))}}
    assert staged.staged_exit(path, 0, 100, is_long, 2) == (
        2, 98.8 if is_long else 101.2, "TIGHTENED_STOP", 1., "OPEN")


def test_staged_simulator_recomputes_exit_fields_and_preserves_native_entry(monkeypatch):
    path = {"timestamp_ns": np.array([1, 121, 122]) * staged.MINUTE_NS,
            "open": np.array([100, 100, 98.8]), "high": np.array([100.1, 100.1, 104]),
            "low": np.array([99.9, 98.9, 95]), "close": np.array([100, 99, 100])}
    monkeypatch.setattr(staged.native, "_entry", lambda *a, **kw: (0, 100., 100., False, 0.))
    orders = pd.DataFrame([dict(sid=1, side="LONG", native_target_pct=2.)])
    result = staged.simulate_staged(orders, {1: path}, cost_bps=5, max_entry_delay_minutes=10).iloc[0]
    assert result.entry_price == 100. and result.native_stop_pct == 1.25
    assert result.exit_reason == "TIGHTENED_STOP" and result.exit_gap_through
    assert result.holding_minutes == 120 and result.active_stop_pct_at_exit == 1.
    assert result.net_return_pct == pytest.approx(-1.25)
    assert result.mfe_pct == pytest.approx(.1) and result.mae_pct == pytest.approx(-1.2)
    assert result.stop_hit and not result.target_hit and not result.same_bar_ambiguous


@pytest.mark.parametrize("day,expected_stop,reason", [
    (date(2026, 10, 5), .60, "STOP"), (DAY, 1.25, "TIME_EXIT_1515")])
def test_daily_simulation_uses_dated_stops_and_unchanged_targets(day, expected_stop, reason):
    stamps = pd.date_range(f"{day} 09:27", f"{day} 15:15", freq="min", tz="Asia/Kolkata")
    path = {"timestamp_ns": stamps.asi8, "open": np.full(len(stamps), 100.),
            "high": np.full(len(stamps), 100.1), "low": np.full(len(stamps), 99.9),
            "close": np.full(len(stamps), 100.)}
    path["low"][1] = 99.3  # Historic .60% stop only; safely before the tightening timer.
    order = dict(sid=0, day=day, side="LONG", setup_id="0926_LONG", tradingsymbol="EXAMPLE",
                 trigger=100., confirmation_ts=pd.Timestamp(f"{day} 09:26", tz="Asia/Kolkata"))
    settings = {"exit": {"setups": {"0926_LONG": {"stop_pct": .60, "target_pct": .97}}}}
    ledger, metrics = replay.simulate_day(dict(day=day, orders=pd.DataFrame([order]), paths={0: path},
        settings=settings, v9_config=replay.g.v9.V9Config()))
    assert ledger.native_stop_pct.tolist() == [expected_stop]
    assert ledger.native_target_pct.tolist() == [.97]
    assert ledger.exit_reason.tolist() == [reason]
    assert metrics["trades"] == 1
