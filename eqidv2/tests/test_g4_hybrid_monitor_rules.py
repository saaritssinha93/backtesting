"""Causal price/volume evidence checks for the fixed G4 hybrid hypotheses."""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from research import g4_hybrid_monitor_rules as rules


def mirror(frame):
    result = frame.copy()
    for field in ("open", "close", "ema20", "session_vwap"):
        result[field] = 200.0 - frame[field]
    result["high"] = 200.0 - frame.low
    result["low"] = 200.0 - frame.high
    result["roc3_atr"] = -frame.roc3_atr
    result["ema_slope3_atr"] = -frame.ema_slope3_atr
    result["upper_wick_ratio"] = frame.lower_wick_ratio
    result["lower_wick_ratio"] = frame.upper_wick_ratio
    return result


def market(closes=(100.0, 99.9, 99.75, 99.6), *, side="LONG", entry_extremes=False):
    """Five structure bars, one entry bar, complete observations, executable future."""
    prices = [100.0] * 6 + list(closes) + [100.0, 100.0]
    stamps = pd.date_range("2026-10-16 10:00", periods=len(prices), freq="min", tz="Asia/Kolkata")
    frame = pd.DataFrame({
        "ts": stamps, "open": prices, "close": prices,
        "high": np.asarray(prices) + .2, "low": np.asarray(prices) - .2,
        "atr14": .1, "ema20": 100.05, "session_vwap": 100.1,
        "roc3_atr": -1.5, "ema_slope3_atr": -.5, "relvol_time": 1.5,
        "upper_wick_ratio": .1, "lower_wick_ratio": .1,
    })
    if entry_extremes:
        frame.loc[5, ["high", "low", "close", "relvol_time"]] = [150., 50., 130., 1000.]
    if side == "SHORT":
        frame = mirror(frame)
    path_frame = frame.iloc[5:]
    path = {"timestamp_ns": path_frame.ts.astype("int64").to_numpy(),
            **{name: path_frame[name].to_numpy(float) for name in ("open", "high", "low", "close")}}
    row = dict(sid=7, day="2026-10-16", setup_id="1005_" + side,
               tradingsymbol="SYNTHETIC", side=side, filled=True,
               entry_price=100., entry_ts=stamps[5], entry_path_index=0,
               confirmation_ts=stamps[4], exit_ts=stamps[6 + len(closes)],
               native_target_pct=1.2)
    return row, frame, path


def observations(closes=(100.0, 99.9, 99.75, 99.6), *, side="LONG", **kwargs):
    row, session, path = market(closes, side=side, **kwargs)
    return rules.trade_observations(row, session, path)


@pytest.mark.parametrize("side", ["LONG", "SHORT"])
def test_entry_veto_uses_only_confirmation_and_earlier_features(side):
    row, session, _ = market(side=side)
    sign = 1 if side == "LONG" else -1
    confirmation = session.ts.eq(row["confirmation_ts"])
    session.loc[confirmation, ["ema20", "roc3_atr", "relvol_time"]] = [100. - sign * .4, sign * .5, .6]
    expected = rules.entry_assessment(row, session)
    assert expected["entry_veto"]
    assert expected["extension_atr"] == pytest.approx(4.)
    assert expected["confirmation_signed_roc3_atr"] == pytest.approx(.5)
    session.loc[session.ts.gt(row["confirmation_ts"]), ["close", "ema20", "roc3_atr", "relvol_time"]] = [1e6, -1e6, 1e6, 1e6]
    assert rules.entry_assessment(row, session) == expected


@pytest.mark.parametrize("side", ["LONG", "SHORT"])
@pytest.mark.parametrize("wick,volume,momentum,expected", [
    (.4, 2., 3., True),
    (.1, .6, .5, True),
    (.1, 2., .5, False),
    (.1, .6, 3., False),
    (.1, 2., 3., False),
])
def test_entry_requires_extension_and_rejection_or_joint_weak_participation(side, wick, volume, momentum, expected):
    row, session, _ = market(side=side)
    sign = 1 if side == "LONG" else -1
    selected = session.ts.eq(row["confirmation_ts"])
    wick_field = "upper_wick_ratio" if side == "LONG" else "lower_wick_ratio"
    session.loc[selected, ["ema20", "roc3_atr", "relvol_time", wick_field]] = [100. - sign * .4, sign * momentum, volume, wick]
    assert rules.entry_assessment(row, session)["entry_veto"] is expected
    session.loc[selected, "ema20"] = 100. - sign * .2
    assert not rules.entry_assessment(row, session)["entry_veto"]


def test_entry_missing_warmup_preserves_candidate_and_exact_confirmation_is_required():
    row, session, _ = market()
    session.loc[session.ts.eq(row["confirmation_ts"]), "relvol_time"] = np.nan
    result = rules.entry_assessment(row, session)
    assert result["entry_feature_status"] == "INSUFFICIENT_WARMUP"
    assert not result["entry_veto"]
    with pytest.raises(ValueError, match="Exact confirmation"):
        rules.entry_assessment(row, session.loc[session.ts.ne(row["confirmation_ts"])])


@pytest.mark.parametrize("side", ["LONG", "SHORT"])
def test_failure_is_symmetric_and_requires_four_full_post_entry_minutes(side):
    records = observations(side=side)
    assert len(records) == 1
    first = records[0]
    assert first["decision_at_ist"] == "2026-10-16T10:09:00+05:30"
    assert first["completed_post_entry_bars"] == 4
    assert first["path_index"] == 4
    assert first["state"] == "REVERSAL_RISK"
    assert first["signed_move_pct"] == pytest.approx(-.4)
    assert set(first["flags"]) == {"PRICE_STRUCTURE", "VOLUME_CONFIRMED", "CONTEXT_CONFIRMED", "VOLUME_CONTEXT_CONFIRMED"}
    assert observations((99.9, 99.75, 99.6), side=side) == []


@pytest.mark.parametrize("side", ["LONG", "SHORT"])
def test_entry_extremes_close_and_volume_are_excluded_from_post_entry_evidence(side):
    ordinary = observations(side=side)
    changed_entry = observations(side=side, entry_extremes=True)
    assert changed_entry == ordinary
    assert ordinary[0]["frozen_structure_boundary"] == pytest.approx(99.8 if side == "LONG" else 100.2)
    assert ordinary[0]["relative_volume_3m"] == pytest.approx(1.5)


@pytest.mark.parametrize("side", ["LONG", "SHORT"])
def test_structure_is_frozen_at_confirmation_and_excludes_older_and_later_extremes(side):
    row, session, path = market(side=side)
    older = session.iloc[[0]].copy()
    older["ts"] = older.ts - pd.Timedelta(minutes=1)
    older["high"], older["low"] = 180., 20.
    session = pd.concat([older, session], ignore_index=True)
    session.loc[session.ts.gt(row["confirmation_ts"]), ["high", "low"]] = [170., 30.]
    record = rules.trade_observations(row, session, path)[0]
    assert record["frozen_structure_boundary"] == pytest.approx(99.8 if side == "LONG" else 100.2)
    assert "PRICE_STRUCTURE" in record["flags"]


@pytest.mark.parametrize("component", ["previous_close", "current_buffer", "adverse_move", "ema", "momentum"])
def test_structure_failure_requires_every_piece_of_price_evidence(component):
    row, session, path = market()
    last, previous = session.index[9], session.index[8]
    if component == "previous_close":
        session.loc[previous, "close"] = 99.8  # Equal to boundary is not a prior break.
    elif component == "current_buffer":
        session.loc[:4, "atr14"] = 1.0  # Adverse move qualifies, but .20 is below .25 ATR.
    elif component == "adverse_move":
        session.loc[:4, "low"] = 100.0
        session.loc[[previous, last], "close"] = [99.9, 99.8]
    elif component == "ema":
        session.loc[last, "ema20"] = 99.5
    else:
        session.loc[last, "roc3_atr"] = -.9
    assert "PRICE_STRUCTURE" not in rules.trade_observations(row, session, path)[0]["flags"]


@pytest.mark.parametrize("volume,context,expected", [
    (1.5, True, {"PRICE_STRUCTURE", "VOLUME_CONFIRMED", "CONTEXT_CONFIRMED", "VOLUME_CONTEXT_CONFIRMED"}),
    (.7, True, {"PRICE_STRUCTURE", "CONTEXT_CONFIRMED"}),
    (1.5, False, {"PRICE_STRUCTURE", "VOLUME_CONFIRMED"}),
    (.7, False, {"PRICE_STRUCTURE"}),
    (np.nan, True, {"PRICE_STRUCTURE", "CONTEXT_CONFIRMED"}),
])
def test_volume_and_context_confirmation_have_separate_and_joint_gates(volume, context, expected):
    row, session, path = market()
    session.loc[session.ts.gt(row["entry_ts"]), "relvol_time"] = volume
    if not context:
        session.loc[session.ts.gt(row["entry_ts"]), ["session_vwap", "ema_slope3_atr"]] = [99., .5]
    assert set(rules.trade_observations(row, session, path)[0]["flags"]) == expected


def test_volume_confirmation_uses_last_three_completed_minutes():
    row, session, path = market()
    session.loc[6:9, "relvol_time"] = [100., .5, .5, 2.]
    result = rules.trade_observations(row, session, path)[0]
    assert result["relative_volume_3m"] == pytest.approx(1.)
    assert "PRICE_STRUCTURE" in result["flags"]
    assert "VOLUME_CONFIRMED" not in result["flags"]


@pytest.mark.parametrize("side", ["LONG", "SHORT"])
def test_profit_structure_can_fire_after_completed_close_gain_and_giveback(side):
    row, session, path = market((100.8, 100.65, 100.4, 100.3), side=side)
    sign = 1 if side == "LONG" else -1
    session.loc[session.ts.gt(row["entry_ts"]), "ema20"] = 100.7 if sign == 1 else 99.3
    result = rules.trade_observations(row, session, path)[0]
    assert result["best_completed_close_profit_pct"] == pytest.approx(.8)
    assert result["close_giveback_pct"] == pytest.approx(.5)
    assert result["signed_move_pct"] == pytest.approx(.3)
    assert "PROFIT_STRUCTURE" in result["flags"]
    assert "PRICE_STRUCTURE" not in result["flags"]


@pytest.mark.parametrize("missing_evidence", ["activation", "giveback", "current_profit", "persistent_ema", "momentum", "participation_and_context"])
def test_profit_protection_requires_each_piece_of_completed_evidence(missing_evidence):
    closes = [100.8, 100.65, 100.4, 100.3]
    if missing_evidence == "current_profit":
        closes[-1] = 100.05
    row, session, path = market(closes)
    session.loc[session.ts.gt(row["entry_ts"]), "ema20"] = 100.7
    if missing_evidence == "activation":
        row["native_target_pct"] = 2.0  # Half target now requires a full 1% close gain.
    elif missing_evidence == "giveback":
        session.loc[:4, "atr14"] = .4  # Requires .8 percentage points of giveback.
    elif missing_evidence == "persistent_ema":
        session.loc[8, "ema20"] = 100.2
    elif missing_evidence == "momentum":
        session.loc[9, "roc3_atr"] = -.9
    elif missing_evidence == "participation_and_context":
        session.loc[session.ts.gt(row["entry_ts"]), ["relvol_time", "session_vwap", "ema_slope3_atr"]] = [.7, 99., .5]
    assert "PROFIT_STRUCTURE" not in rules.trade_observations(row, session, path)[0]["flags"]


@pytest.mark.parametrize("side", ["LONG", "SHORT"])
def test_profit_activation_ignores_entry_and_later_intrabar_extremes(side):
    row, session, path = market((100.2, 100.15, 100.1, 100.1), side=side, entry_extremes=True)
    session.loc[session.ts.gt(row["entry_ts"]), ["high", "low"]] = [150., 50.]
    session.loc[session.ts.gt(row["entry_ts"]), "ema20"] = 100.7 if side == "LONG" else 99.3
    result = rules.trade_observations(row, session, path)[0]
    assert result["best_completed_close_profit_pct"] == pytest.approx(.2)
    assert "PROFIT_STRUCTURE" not in result["flags"]


def test_future_session_and_execution_candle_changes_do_not_change_prior_states():
    row, session, path = market((100., 99.9, 99.75, 99.6, 99.4, 99.2))
    decision = pd.Timestamp("2026-10-16 10:09", tz="Asia/Kolkata")
    prior = [x for x in rules.trade_observations(row, session, path) if pd.Timestamp(x["decision_at_ist"]) <= decision]
    future = session.ts.gt(decision)
    session.loc[future, ["close", "high", "ema20", "session_vwap", "roc3_atr", "relvol_time"]] = [1000., 1001., -1000., -1000., 1000., 1000.]
    path["high"][path["timestamp_ns"] > decision.value] = 1e9
    after = [x for x in rules.trade_observations(row, session, path) if pd.Timestamp(x["decision_at_ist"]) <= decision]
    assert after == prior


def test_native_exit_and_unfilled_orders_produce_no_later_observations():
    row, session, path = market()
    row["exit_ts"] = pd.Timestamp("2026-10-16 10:09", tz="Asia/Kolkata")
    assert rules.trade_observations(row, session, path) == []
    row["filled"] = False
    assert rules.trade_observations(row, session, path) == []


@pytest.mark.parametrize("remove_index", [2, 7])
def test_incomplete_structure_or_post_entry_minutes_are_rejected(remove_index):
    row, session, path = market()
    with pytest.raises(ValueError, match="Contiguous"):
        rules.trade_observations(row, session.drop(index=remove_index), path)


def test_structure_must_end_at_exact_confirmation_even_if_earlier_five_are_contiguous():
    row, session, path = market()
    older = session.iloc[[0]].copy()
    older["ts"] -= pd.Timedelta(minutes=1)
    missing_confirmation = pd.concat([older, session.loc[session.ts.ne(row["confirmation_ts"])]], ignore_index=True)
    with pytest.raises(ValueError, match="Contiguous"):
        rules.trade_observations(row, missing_confirmation, path)


def test_structure_cannot_cross_into_the_preceding_session():
    row, session, path = market()
    shift = pd.Timedelta(hours=10, minutes=3)
    session["ts"] -= shift
    for key in ("confirmation_ts", "entry_ts", "exit_ts"):
        row[key] -= shift
    path["timestamp_ns"] -= shift.value
    assert row["confirmation_ts"].strftime("%H:%M") == "00:01"
    with pytest.raises(ValueError, match="Contiguous"):
        rules.trade_observations(row, session, path)
