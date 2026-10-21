"""Executable event ordering and portfolio controls for G4 hybrid replay."""
from __future__ import annotations

import json

import numpy as np
import pandas as pd
import pytest

from research import g4_hybrid_monitor_backtest as runner


def path(rows, *, start="2026-10-16 10:01"):
    values = np.asarray(rows, float)
    stamps = pd.date_range(start, periods=len(rows), freq="min", tz="Asia/Kolkata")
    return {"timestamp_ns": stamps.asi8,
            **{key: values[:, index] for index, key in enumerate(("open", "high", "low", "close"))}}


def source_row(sid, minute_path):
    first = pd.Timestamp(int(minute_path["timestamp_ns"][0]), tz="UTC").tz_convert("Asia/Kolkata")
    return dict(sid=sid, day=str(first.date()), signal_id=f"synthetic-{sid}",
                tradingsymbol=f"SYNTHETIC{sid}", side="LONG", setup_id=f"{sid:04d}_LONG",
                confirmation_ts=first - pd.Timedelta(minutes=1),
                confirmation_open=100., confirmation_high=100.1,
                confirmation_low=99.9, confirmation_close=100., trigger=100.,
                native_target_pct=1., native_stop_pct=1.2, initial_stop_pct=1.2,
                picker="max_liquidity", traded_value=1000. - sid)


def native_book(paths):
    selected = pd.DataFrame([source_row(sid, minute_path) for sid, minute_path in paths.items()])
    return runner.sealed.replay(selected, paths, "current_g4")


def event(minute_path, *, signal_index=2, flag="VOLUME_CONFIRMED"):
    execution_index = signal_index + 1
    end = pd.Timestamp(int(minute_path["timestamp_ns"][execution_index]), tz="UTC").tz_convert("Asia/Kolkata")
    return dict(execution_path_index=execution_index, modeled_execution_at_ist=end.isoformat(),
                next_bar_open_at_ist=(end - pd.Timedelta(minutes=1)).isoformat(),
                decision_at_ist=pd.Timestamp(int(minute_path["timestamp_ns"][signal_index]), tz="UTC").tz_convert("Asia/Kolkata").isoformat(),
                next_bar_open_price=float(minute_path["open"][execution_index]), flags=json.dumps([flag]))


def checks(native, veto=()):
    return pd.DataFrame({"sid": native.sid, "entry_veto": native.sid.isin(veto)})


@pytest.fixture
def two_policies(monkeypatch):
    monkeypatch.setattr(runner.rules, "POLICIES", {
        "baseline": dict(entry_veto=False, exit_flags=[]),
        "volume_confirmed": dict(entry_veto=False, exit_flags=["VOLUME_CONFIRMED"]),
    })


def stub_observation(monkeypatch, signal_index=2):
    monkeypatch.setattr(runner.rules, "entry_assessment", lambda row, session: {"entry_veto": False})

    def states(row, session, minute_path):
        return [dict(sid=int(row["sid"]), path_index=signal_index,
                     decision_at_ist=pd.Timestamp(int(minute_path["timestamp_ns"][signal_index]), tz="UTC").tz_convert("Asia/Kolkata").isoformat(),
                     flags=["VOLUME_CONFIRMED"])]

    monkeypatch.setattr(runner.rules, "trade_observations", states)


def test_next_open_exit_precedes_native_later_intrabar_stop_and_excludes_future_extremes(monkeypatch, two_policies):
    minute_path = path([[100., 100.1, 99.9, 100.], [100., 100.1, 99.8, 99.9],
                        [99.9, 100., 99.4, 99.5], [99.4, 150., 98., 99.3]])
    paths = {1: minute_path}
    native = native_book(paths)
    assert native.iloc[0].exit_event == "INTRABAR"
    assert native.iloc[0].exit_reason == "STOP"
    stub_observation(monkeypatch)
    entries, _, events = runner._first_events(native, {1: pd.DataFrame()}, paths)
    books = runner.apply_policies(native, paths, entries, events)
    result = books["volume_confirmed"].iloc[0]
    assert result.monitor_exit
    assert result.exit_price == pytest.approx(99.4)
    assert result.exit_execution_ts == "2026-10-16T10:03:00+05:30"
    assert result.exit_ts == pd.Timestamp("2026-10-16 10:04", tz="Asia/Kolkata")
    assert result.gross_return_pct == pytest.approx(-.6)
    assert result.net_return_pct == pytest.approx(-.65)
    assert result.portfolio_net_profit_rupees == pytest.approx(-3250.)
    assert result.mfe_pct == pytest.approx(.1)  # Later 150 high cannot enter an open fill.
    assert result.mae_pct == pytest.approx(-.6)  # Later 98 low cannot enter an open fill.
    assert not result.stop_hit
    assert result.active_stop_pct_at_exit == pytest.approx(1.2)
    assert result.active_stop_price == pytest.approx(98.8)
    runner.sealed.parity(books["baseline"], native)


def test_native_gap_stop_at_same_open_has_priority(monkeypatch, two_policies):
    minute_path = path([[100., 100.1, 99.9, 100.], [100., 100.1, 99.8, 99.9],
                        [99.9, 100., 99.4, 99.5], [98., 100., 97., 99.3]])
    paths = {1: minute_path}
    native = native_book(paths)
    assert native.iloc[0].exit_event == "OPEN"
    assert native.iloc[0].exit_ts == pd.Timestamp("2026-10-16 10:03", tz="Asia/Kolkata")
    stub_observation(monkeypatch)
    # A cache cannot contain a completed observation at the native open exit.
    with pytest.raises(ValueError, match="Invalid cached observation timing"):
        runner._first_events(native, {1: pd.DataFrame()}, paths)
    books = runner.apply_policies(native, paths, checks(native), {1: {}})
    runner.sealed.parity(books["volume_confirmed"], native)
    assert not books["volume_confirmed"].iloc[0].monitor_exit


def target_path(*, start="2026-10-16 10:01"):
    first = pd.Timestamp(start, tz="Asia/Kolkata")
    count = int((pd.Timestamp("2026-10-16 10:10", tz="Asia/Kolkata") - first).total_seconds() / 60) + 1
    rows = [[100., 100.1, 99.9, 100.]] * count
    rows[-1] = [100., 101.1, 99.9, 100.]
    return path(rows, start=start)


def test_entry_veto_retains_selected_identities_and_has_no_fill_or_profit(monkeypatch):
    paths = {1: target_path(), 2: target_path()}
    native = native_book(paths)
    monkeypatch.setattr(runner.rules, "POLICIES", {
        "baseline": dict(entry_veto=False, exit_flags=[]),
        "entry_exhaustion": dict(entry_veto=True, exit_flags=[]),
    })
    books = runner.apply_policies(native, paths, checks(native, veto=[1]), {1: {}, 2: {}})
    result = books["entry_exhaustion"].set_index("sid")
    assert set(result.index) == {1, 2}
    assert len(result) == len(native)
    removed = result.loc[1]
    assert removed.entry_vetoed and not removed.filled and not removed.portfolio_executed
    assert removed.exit_reason == "ENTRY_VETO_EXHAUSTION"
    assert pd.isna(removed.entry_ts) and pd.isna(removed.exit_ts)
    assert removed.portfolio_net_profit_rupees == 0.
    assert removed.portfolio_cost_rupees == 0.
    assert removed.exposure_per_entry_rupees == 0.
    for flag in ("stop_hit", "target_hit", "first_target_hit", "runner_target_hit", "same_bar_ambiguous", "exit_gap_through", "entry_gap_through", "protection_applied"):
        assert not removed[flag]
    for field in ("holding_minutes", "mfe_pct", "mae_pct", "active_stop_pct_at_exit", "active_stop_price", "entry_overshoot_bps", "exit_gap_bps", "protection_armed_path_index", "protection_effective_path_index", "exit_event", "exit_execution_ts", "exit_bar_end_ts"):
        assert pd.isna(removed[field])
    assert result.loc[2, "portfolio_executed"]
    assert result.loc[2, "portfolio_net_profit_rupees"] == pytest.approx(4750.)
    assert list(result.signal_id) == list(native.set_index("sid").signal_id)
    runner.sealed.parity(books["baseline"], native)


@pytest.mark.parametrize("later_entry,admitted", [("10:03", False), ("10:04", True)])
def test_shorter_exit_reallocates_all_selected_fills_with_end_of_bar_reservation(two_policies, later_entry, admitted):
    paths = {sid: target_path() for sid in range(10)}
    paths[10] = target_path(start="2026-10-16 " + later_entry)
    native = native_book(paths)
    assert native.portfolio_executed.sum() == 10
    assert not native.set_index("sid").loc[10, "portfolio_executed"]
    exits = {sid: {} for sid in paths}
    exits[0] = {"VOLUME_CONFIRMED": event(paths[0])}
    books = runner.apply_policies(native, paths, checks(native), exits)
    monitored = books["volume_confirmed"].set_index("sid")
    assert bool(monitored.loc[10, "portfolio_executed"]) is admitted
    assert monitored.loc[0, "exit_execution_ts"] == "2026-10-16T10:03:00+05:30"
    assert monitored.loc[0, "exit_ts"] == pd.Timestamp("2026-10-16 10:04", tz="Asia/Kolkata")
    assert monitored.portfolio_executed.sum() == 10 + int(admitted)
    runner.sealed.parity(books["baseline"], native)


def test_baseline_accounting_corruption_is_rejected(monkeypatch, two_policies):
    paths = {1: target_path()}
    native = native_book(paths)
    actual_allocate = runner.previous.allocate

    def corrupt(raw, policy):
        ledger = actual_allocate(raw, policy)
        if policy == "baseline":
            ledger.loc[ledger.index[0], "portfolio_net_profit_rupees"] += 1.
        return ledger

    monkeypatch.setattr(runner.previous, "allocate", corrupt)
    with pytest.raises(ValueError, match="portfolio accounting differs"):
        runner.apply_policies(native, paths, checks(native), {1: {}})


def cached_observation(minute_path, *, decision_index=2, sid=1, flags=("VOLUME_CONFIRMED",)):
    return dict(sid=sid, path_index=decision_index,
                decision_at_ist=pd.Timestamp(int(minute_path["timestamp_ns"][decision_index]), tz="UTC").tz_convert("Asia/Kolkata").isoformat(),
                flags=json.dumps(list(flags)))


def test_cached_events_use_first_completed_signal_and_following_open():
    paths = {1: target_path()}
    native = native_book(paths)
    observations = pd.DataFrame([
        cached_observation(paths[1], decision_index=1, flags=()),
        cached_observation(paths[1], decision_index=2),
        cached_observation(paths[1], decision_index=3),
        cached_observation(paths[1], decision_index=4, flags=("PROFIT_STRUCTURE",)),
    ])
    events = runner.events_from_observations(native, paths, observations)[1]
    assert events["VOLUME_CONFIRMED"] == event(paths[1], signal_index=2)
    assert events["PROFIT_STRUCTURE"] == event(paths[1], signal_index=4, flag="PROFIT_STRUCTURE")
    assert events["VOLUME_CONFIRMED"]["execution_path_index"] == 3


@pytest.mark.parametrize("invalid", ["entry_candle", "incorrect_timestamp", "native_exit", "unfilled"])
def test_cached_observations_reject_timing_that_cannot_be_observed(invalid):
    paths = {1: target_path()}
    native = native_book(paths)
    item = cached_observation(paths[1])
    if invalid == "entry_candle":
        item = cached_observation(paths[1], decision_index=0)
    elif invalid == "incorrect_timestamp":
        item["decision_at_ist"] = (pd.Timestamp(item["decision_at_ist"]) + pd.Timedelta(seconds=1)).isoformat()
    elif invalid == "native_exit":
        item = cached_observation(paths[1], decision_index=len(paths[1]["timestamp_ns"]) - 1)
    else:
        native.loc[native.index[0], "filled"] = False
    with pytest.raises(ValueError, match="Invalid cached observation timing"):
        runner.events_from_observations(native, paths, pd.DataFrame([item]))


@pytest.mark.parametrize("baseline_net", [-6250., 0., 4750.])
def test_profit_retention_is_undefined_for_zero_or_negative_baseline(two_policies, baseline_net):
    paths = {1: target_path()}
    native = native_book(paths)
    # Adjust a synthetic book's return consistently, leaving allocation causal.
    native.loc[:, "gross_return_pct"] = baseline_net / 5000. + .05
    native.loc[:, "net_return_pct"] = baseline_net / 5000.
    native = runner.previous.allocate(native, "baseline")
    books = runner.apply_policies(native, paths, checks(native), {1: {}})
    metrics, _, _, chosen = runner.comparisons(books, ["2026-10-16"])
    if baseline_net > 0:
        assert metrics.profit_retained_pct.eq(100.).all()
    else:
        assert metrics.profit_retained_pct.isna().all()
    assert chosen is None
