"""Predeclared causal G4 close-decision exit hypotheses; no order execution.

All 280 alternatives are generated deterministically before their results are
seen. A completed-bar signal requests the following observed open; the native
stop/target wins whenever it already executes at or before that open. Entries,
targets, native stops, portfolio allocation and policy selection belong to the
research runner. These rules do not identify reversals with certainty.
"""
from __future__ import annotations

from collections.abc import Mapping, Sequence
import itertools
import json

import numpy as np
import pandas as pd


VERSION = "G4-ITERATIVE-CLOSE-EXIT-HYPOTHESES-V1"
MINUTE_NS = 60_000_000_000
MINIMUM_POST_ENTRY_BARS = 4
MINIMUM_PROTECT_GROSS_PCT = .07  # Five-basis-point cost plus .02 percentage points.
TEMPLATE_RATIONALES = {
    "target_giveback": "After closing-price progress reaches a target-relative activation, exit on a declared fraction of peak closing profit surrendered, with an optional entry-ATR buffer and confirmation.",
    "net_breakeven": "After meaningful closing profit, request an exit when closing profit returns to estimated net breakeven or a small entry-ATR profit cushion, with adverse-price confirmation.",
    "rolling_structure": "After meaningful closing profit, request an exit on a breach of the preceding three or five fully post-entry candles' directional structure while the decision remains profitable.",
    "atr_close_trail": "After meaningful closing profit, request an exit when closing price gives back a fixed multiple of current ATR from its best completed post-entry close; this is a next-open market request, not an intrabar resting stop.",
    "delayed_failure": "After a fixed delay with little completed-close favorable excursion, request an exit only on persistent adverse closes, adverse EMA/momentum, and volume or VWAP context.",
    "mature_failure": "After thirty or sixty minutes with little target-relative completed-close progress, request an exit on three adverse closes and a declared adverse return, with adverse EMA and volume or VWAP context.",
    "late_session_trail": "From 14:00 onward, protect ninety percent of meaningful best completed-close profit, allowing a declared entry-ATR giveback buffer and optional context confirmation.",
}
GRID = {
    "target_giveback": dict(target_fraction=(.4, .6, .8), activation_floor_pct=(.3, .5), retained_peak_fraction=(.25, .5, .7), entry_atr_buffer=(0., .5, 1.), confirmation=("none", "adverse2", "context")),
    "net_breakeven": dict(target_fraction=(.4, .6, .8), activation_floor_pct=(.3,), profit_buffer_entry_atr=(0., .25, .5), confirmation=("adverse2", "context")),
    "rolling_structure": dict(lookback_bars=(3, 5), target_fraction=(.4, .6, .8), activation_floor_pct=(.3,), entry_atr_buffer=(0., .25), confirmation=("none", "volume", "context")),
    "atr_close_trail": dict(target_fraction=(.4, .6, .8), activation_floor_pct=(.3,), current_atr_multiple=(1., 1.5, 2.), confirmation=("none", "context")),
    "delayed_failure": dict(delay_minutes=(8, 15, 25), maximum_peak_profit_pct=(.15, .3), confirmation=("volume", "context")),
    "mature_failure": dict(delay_minutes=(30, 60), maximum_peak_target_fraction=(.2, .4), adverse_return_pct=(.4, .6), confirmation=("volume", "context")),
    "late_session_trail": dict(target_fraction=(.4, .6, .8), activation_floor_pct=(.3,), retained_peak_fraction=(.9,), entry_atr_buffer=(0., .5, 1.), confirmation=("none", "context")),
}


def policy_catalog() -> list[dict]:
    """Return the fixed baseline plus 280 alternatives, without reading results."""
    policies = [dict(id="baseline", family="baseline", kind="baseline", params={}, rationale="Preserve every native G4 entry and exit.")]
    for family, dimensions in GRID.items():
        names = tuple(dimensions)
        for number, values in enumerate(itertools.product(*dimensions.values()), 1):
            policies.append(dict(id=f"{family}_{number:03d}", family=family,
                kind="failure" if family in ("delayed_failure", "mature_failure") else "profit_protect",
                params=dict(zip(names, values)), rationale=TEMPLATE_RATIONALES[family]))
    return policies


ARRAY_KEYS = (
    "decision_ns", "execution_path_index", "next_open_ns", "reserve_end_ns", "next_open_price",
    "elapsed_minutes", "post_entry_bars", "session_minute", "signed_move_pct", "best_close_profit_pct", "giveback_pct",
    "entry_atr_pct", "current_atr_pct", "current_to_entry_atr", "native_target_pct", "target_progress",
    "signed_momentum3_atr", "signed_ema_distance_atr", "signed_ema_slope3_atr", "signed_vwap_distance_atr",
    "relvol_time", "relvol20", "relvol3", "adverse_closes2", "adverse_closes3", "under_ema2", "under_ema3",
    "rolling_low3_distance_atr", "rolling_low5_distance_atr", "signed_body_atr", "range_atr", "adverse_wick_ratio",
)


def _stamp(value) -> pd.Timestamp:
    stamp = pd.Timestamp(value)
    if stamp.tzinfo is None:
        raise ValueError("Native trade timestamps must be timezone-aware")
    return stamp.tz_convert("Asia/Kolkata")


def _empty_arrays() -> dict[str, np.ndarray]:
    return {key: np.empty(0, dtype=bool if key in ("adverse_closes2", "adverse_closes3", "under_ema2", "under_ema3") else np.int64 if key in ("decision_ns", "execution_path_index", "next_open_ns", "reserve_end_ns", "post_entry_bars", "session_minute") else float) for key in ARRAY_KEYS}


def _rolling_all(values: np.ndarray, count: int) -> np.ndarray:
    return pd.Series(values, dtype=float).rolling(count, min_periods=count).sum().eq(count).to_numpy()


def trade_arrays(row: Mapping, session: pd.DataFrame, path: Mapping) -> dict[str, np.ndarray]:
    """Export fixed causal per-decision arrays for rules or training-only analysis.

    Full post-entry candles only contribute to price peaks, close-change runs,
    rolling structure, three-bar momentum and three-bar volume. EMA/VWAP retain
    their trailing historical indicator context. Structure distance is signed
    close minus the preceding rolling low for longs, or the preceding rolling
    high minus close for shorts, divided by confirmation ATR. Missing feature
    values remain NaN and cannot satisfy a corresponding rule condition.
    """
    if str(row.get("filled", False)).lower() not in ("true", "1"):
        return _empty_arrays()
    side = row["side"]
    if side not in ("LONG", "SHORT"):
        raise ValueError("Unknown trade side")
    sign = 1. if side == "LONG" else -1.
    entry, target = float(row["entry_price"]), float(row["native_target_pct"])
    if not np.isfinite([entry, target]).all() or entry <= 0 or target <= 0:
        raise ValueError("Positive finite entry and target required")
    entry_end, native_exit, confirmation = (_stamp(row[key]) for key in ("entry_ts", "exit_ts", "confirmation_ts"))
    if not isinstance(session.ts.dtype, pd.DatetimeTZDtype) or session.ts.isna().any() or session.ts.duplicated().any() or not session.ts.is_monotonic_increasing:
        raise ValueError("Sorted unique timezone-aware feature timestamps required")
    initial = session.loc[session.ts.eq(confirmation)]
    if len(initial) != 1:
        raise ValueError("Exact confirmation features required")
    entry_atr = float(initial.atr14.iloc[0])
    if not np.isfinite(entry_atr) or entry_atr <= 0:
        return _empty_arrays()
    post = session.loc[(session.ts - pd.Timedelta(minutes=1) >= entry_end) & session.ts.lt(native_exit)].reset_index(drop=True)
    if post.empty:
        return _empty_arrays()
    stamps = post.ts.dt.as_unit("ns").astype("int64").to_numpy()
    if np.any(np.diff(stamps) != MINUTE_NS):
        raise ValueError("Post-entry features require contiguous completed minute candles")
    path_stamps = np.asarray(path["timestamp_ns"], dtype=np.int64)
    if path_stamps.ndim != 1 or not len(path_stamps) or np.any(np.diff(path_stamps) != MINUTE_NS):
        raise ValueError("Contiguous nanosecond execution path required")
    positions = np.searchsorted(path_stamps, stamps)
    if np.any(positions >= len(path_stamps)) or not np.array_equal(path_stamps[positions], stamps):
        raise ValueError("Feature and execution timestamps differ")
    if np.any(positions <= int(row["entry_path_index"])):
        raise ValueError("Entry candle must be excluded from decisions")
    keep = positions + 1 < len(path_stamps)
    next_positions = np.minimum(positions + 1, len(path_stamps) - 1)
    next_open_ns = path_stamps[next_positions] - MINUTE_NS
    keep &= (next_open_ns >= stamps) & (next_open_ns < native_exit.value)
    if not keep.any():
        return _empty_arrays()

    close = post.close.to_numpy(float)
    atr = post.atr14.to_numpy(float)
    positive_atr = np.where(np.isfinite(atr) & (atr > 0), atr, np.nan)
    signed_move = sign * (close / entry - 1) * 100
    peak = np.maximum.accumulate(np.r_[0., signed_move])[1:]
    adverse_change = np.r_[False, sign * np.diff(close) < 0]
    ema = post.ema20.to_numpy(float)
    below_ema = np.isfinite(ema) & (sign * (close - ema) < 0)
    volume = post.relvol_time.to_numpy(float)
    three_volume = pd.Series(volume).rolling(3, min_periods=3).mean().to_numpy()
    three_momentum = sign * (pd.Series(close) - pd.Series(close).shift(3)).to_numpy() / positive_atr
    arrays = dict(
        decision_ns=stamps, execution_path_index=next_positions, next_open_ns=next_open_ns,
        reserve_end_ns=path_stamps[next_positions], next_open_price=np.asarray(path["open"], dtype=float)[next_positions],
        elapsed_minutes=(stamps - entry_end.value) / MINUTE_NS, post_entry_bars=np.arange(1, len(post) + 1),
        session_minute=(post.ts.dt.hour * 60 + post.ts.dt.minute).to_numpy(),
        signed_move_pct=signed_move, best_close_profit_pct=peak, giveback_pct=peak - signed_move,
        entry_atr_pct=np.full(len(post), entry_atr / entry * 100), current_atr_pct=positive_atr / entry * 100,
        current_to_entry_atr=positive_atr / entry_atr, native_target_pct=np.full(len(post), target),
        target_progress=signed_move / target, signed_momentum3_atr=three_momentum,
        signed_ema_distance_atr=sign * (close - ema) / positive_atr,
        signed_ema_slope3_atr=sign * post.ema_slope3_atr.to_numpy(float),
        signed_vwap_distance_atr=sign * (close - post.session_vwap.to_numpy(float)) / positive_atr,
        relvol_time=volume, relvol20=post.relvol20.to_numpy(float), relvol3=three_volume,
        adverse_closes2=_rolling_all(adverse_change, 2), adverse_closes3=_rolling_all(adverse_change, 3),
        under_ema2=_rolling_all(below_ema, 2), under_ema3=_rolling_all(below_ema, 3),
        signed_body_atr=sign * (close - post.open.to_numpy(float)) / positive_atr,
        range_atr=(post.high.to_numpy(float) - post.low.to_numpy(float)) / positive_atr,
        adverse_wick_ratio=post["upper_wick_ratio" if sign == 1 else "lower_wick_ratio"].to_numpy(float),
    )
    for lookback in (3, 5):
        boundary = post["low" if sign == 1 else "high"].shift(1).rolling(lookback, min_periods=lookback)
        boundary = (boundary.min() if sign == 1 else boundary.max()).to_numpy()
        arrays[f"rolling_low{lookback}_distance_atr"] = sign * (close - boundary) / entry_atr
    return {key: np.asarray(arrays[key])[keep] for key in ARRAY_KEYS}


def _confirmation(arrays: Mapping[str, np.ndarray], name: str) -> np.ndarray:
    if name == "none":
        return np.ones(len(arrays["decision_ns"]), dtype=bool)
    if name == "adverse2":
        return arrays["adverse_closes2"]
    if name == "volume":
        return arrays["adverse_closes2"] & np.isfinite(arrays["relvol3"]) & (arrays["relvol3"] >= 1.25)
    if name == "context":
        return (arrays["under_ema2"] & (arrays["signed_momentum3_atr"] < 0)
            & ((arrays["signed_vwap_distance_atr"] < 0) | (arrays["signed_ema_slope3_atr"] < 0)))
    raise ValueError("Unknown confirmation: " + name)


def _decision_mask(arrays: Mapping[str, np.ndarray], policy: Mapping) -> np.ndarray:
    p, family = policy["params"], policy["family"]
    eligible = arrays["post_entry_bars"] >= MINIMUM_POST_ENTRY_BARS
    if family == "baseline":
        return np.zeros(len(arrays["decision_ns"]), dtype=bool)
    confirmed = _confirmation(arrays, p["confirmation"])
    move, peak = arrays["signed_move_pct"], arrays["best_close_profit_pct"]
    if family == "delayed_failure":
        return (eligible & confirmed & (arrays["elapsed_minutes"] >= p["delay_minutes"])
            & (peak <= p["maximum_peak_profit_pct"]) & arrays["adverse_closes2"]
            & arrays["under_ema2"] & (arrays["signed_momentum3_atr"] <= -.5)
            & (move <= -np.maximum(.2, .5 * arrays["entry_atr_pct"])))
    if family == "mature_failure":
        return (eligible & confirmed & (arrays["elapsed_minutes"] >= p["delay_minutes"])
            & (peak < p["maximum_peak_target_fraction"] * arrays["native_target_pct"])
            & arrays["adverse_closes3"] & arrays["under_ema2"]
            & (move <= -p["adverse_return_pct"]))
    activation = np.maximum(p["activation_floor_pct"], p["target_fraction"] * arrays["native_target_pct"])
    armed = peak >= activation
    if family in ("target_giveback", "late_session_trail"):
        threshold = peak * (1 - p["retained_peak_fraction"]) + p["entry_atr_buffer"] * arrays["entry_atr_pct"]
        condition = (arrays["giveback_pct"] >= threshold) & (move >= MINIMUM_PROTECT_GROSS_PCT)
        if family == "late_session_trail":
            condition &= arrays["session_minute"] >= 14 * 60
    elif family == "net_breakeven":
        threshold = .05 + p["profit_buffer_entry_atr"] * arrays["entry_atr_pct"]
        condition = move <= threshold
    elif family == "rolling_structure":
        condition = ((arrays[f'rolling_low{p["lookback_bars"]}_distance_atr'] < -p["entry_atr_buffer"])
                     & (move >= MINIMUM_PROTECT_GROSS_PCT))
    elif family == "atr_close_trail":
        condition = ((arrays["giveback_pct"] >= p["current_atr_multiple"] * arrays["current_atr_pct"])
                     & (move >= MINIMUM_PROTECT_GROSS_PCT))
    else:
        raise ValueError("Unknown policy family: " + family)
    return eligible & confirmed & armed & condition


def events_for_trade(row: Mapping, session: pd.DataFrame, path: Mapping,
                     policies: Sequence[Mapping] | None = None) -> dict[str, dict | None]:
    """Return each policy's first causal signal, leaving actual fills to runner."""
    catalog = policy_catalog() if policies is None else list(policies)
    if len({policy["id"] for policy in catalog}) != len(catalog):
        raise ValueError("Policy ids must be unique")
    arrays = trade_arrays(row, session, path)
    result = dict.fromkeys(policy["id"] for policy in catalog)
    if not len(arrays["decision_ns"]):
        return result
    for policy in catalog:
        indices = np.flatnonzero(_decision_mask(arrays, policy))
        if not len(indices):
            continue
        index = int(indices[0])
        def timestamp(key):
            return pd.Timestamp(int(arrays[key][index]), tz="UTC").tz_convert("Asia/Kolkata").isoformat()
        result[policy["id"]] = dict(
            execution_path_index=int(arrays["execution_path_index"][index]),
            modeled_execution_at_ist=timestamp("reserve_end_ns"),
            next_bar_open_at_ist=timestamp("next_open_ns"), decision_at_ist=timestamp("decision_ns"),
            next_bar_open_price=float(arrays["next_open_price"][index]),
            flags=json.dumps([policy["id"]]), family=policy["family"],
            signed_move_pct=float(arrays["signed_move_pct"][index]),
            best_completed_close_profit_pct=float(arrays["best_close_profit_pct"][index]),
        )
    return result
