"""Fixed, causal price/volume hypotheses for isolated G4 research.

These parameters are hypotheses, not fitted predictions. Entries are assessed
at confirmation; post-entry decisions use completed full subsequent minutes.
Execution and native stop/target precedence belong to the research runner.
"""
from __future__ import annotations

import math
from collections.abc import Mapping

import numpy as np
import pandas as pd

VERSION = "G4-HYBRID-PRICE-VOLUME-RULES-V1"
PARAMETERS = dict(
    entry_extension_atr=3.0, entry_adverse_wick=.35,
    entry_weak_relative_volume=.8, entry_weak_momentum_atr=1.0,
    structure_lookback_minutes=5, structure_buffer_entry_atr=.25,
    persistent_closes=2, minimum_post_entry_bars=4,
    reversal_adverse_entry_pct=.30, reversal_momentum_3m_atr=1.0,
    volume_confirmation=1.25, volume_window_minutes=3,
    profit_activation_floor_pct=.60, profit_activation_target_fraction=.50,
    profit_giveback_floor_pct=.30, profit_giveback_entry_atr=2.0,
    profit_minimum_current_gross_pct=.10,
)
POLICIES = {
    "baseline": dict(entry_veto=False, exit_flags=[]),
    "entry_exhaustion": dict(entry_veto=True, exit_flags=[]),
    "price_structure": dict(entry_veto=False, exit_flags=["PRICE_STRUCTURE"]),
    "volume_confirmed": dict(entry_veto=False, exit_flags=["VOLUME_CONFIRMED"]),
    "context_confirmed": dict(entry_veto=False, exit_flags=["CONTEXT_CONFIRMED"]),
    "volume_context_confirmed": dict(entry_veto=False, exit_flags=["VOLUME_CONTEXT_CONFIRMED"]),
    "profit_structure": dict(entry_veto=False, exit_flags=["PROFIT_STRUCTURE"]),
    "volume_and_profit": dict(entry_veto=False, exit_flags=["VOLUME_CONFIRMED", "PROFIT_STRUCTURE"]),
    "entry_and_volume_profit": dict(entry_veto=True, exit_flags=["VOLUME_CONFIRMED", "PROFIT_STRUCTURE"]),
}


def _finite(*values):
    return all(value is not None and np.isfinite(float(value)) for value in values)


def _side(row):
    side = row["side"]
    if side not in ("LONG", "SHORT"):
        raise ValueError("Unknown trade side")
    return 1 if side == "LONG" else -1


def entry_assessment(row: Mapping, session: pd.DataFrame) -> dict:
    """Assess a selected candidate without inspecting any later candle.

    A veto needs substantial extension plus a rejection wick, or extension
    plus both weak same-time participation and weak recent progress. Missing
    feature warmup preserves the candidate and is explicitly reported.
    """
    sign = _side(row)
    stamp = pd.Timestamp(row["confirmation_ts"])
    observed = session.loc[session.ts.le(stamp)]
    if observed.empty or observed.ts.iloc[-1] != stamp:
        raise ValueError("Exact confirmation minute required")
    last = observed.iloc[-1]
    atr = float(last.atr14)
    required = (atr, last.ema20, last.roc3_atr, last.relvol_time)
    result = dict(entry_veto=False, entry_feature_status="INSUFFICIENT_WARMUP",
                  confirmation_atr14=atr, extension_atr=math.nan,
                  confirmation_relvol_time=float(last.relvol_time),
                  confirmation_signed_roc3_atr=sign * float(last.roc3_atr),
                  adverse_wick_ratio=math.nan)
    if not _finite(*required) or atr <= 0:
        return result
    extension = sign * (float(last.close) - float(last.ema20)) / atr
    wick = float(last.upper_wick_ratio if sign == 1 else last.lower_wick_ratio)
    exhausted = (extension >= PARAMETERS["entry_extension_atr"] and
        (wick >= PARAMETERS["entry_adverse_wick"] or
         (last.relvol_time < PARAMETERS["entry_weak_relative_volume"] and
          sign * last.roc3_atr < PARAMETERS["entry_weak_momentum_atr"])))
    result.update(entry_veto=bool(exhausted), entry_feature_status="APPLICABLE",
                  extension_atr=float(extension), adverse_wick_ratio=wick)
    return result


def trade_observations(row: Mapping, session: pd.DataFrame, path: Mapping) -> list[dict]:
    """Return pre-native-exit states and flag hypotheses, without fill prices.

    The structure boundary is frozen from five completed minutes ending at
    confirmation. Entry-candle extrema/volume never enter post-entry windows.
    Indicator context (EMA/VWAP) can use all history already completed.
    """
    if not bool(row["filled"]):
        return []
    sign = _side(row)
    entry = float(row["entry_price"])
    entry_end = pd.Timestamp(row["entry_ts"])
    confirmation = pd.Timestamp(row["confirmation_ts"])
    initial = session.loc[session.ts.le(confirmation)].tail(PARAMETERS["structure_lookback_minutes"])
    if (len(initial) != PARAMETERS["structure_lookback_minutes"] or
            initial.ts.iloc[-1] != confirmation or
            not initial.ts.dt.normalize().eq(confirmation.normalize()).all() or
            not np.all(np.diff(initial.ts.astype("int64")) == 60_000_000_000)):
        raise ValueError("Contiguous pre-entry structure required")
    atr = float(initial.atr14.iloc[-1])
    if not _finite(atr) or atr <= 0:
        return []
    boundary = float(initial.low.min() if sign == 1 else initial.high.max())
    cutoff = pd.Timestamp(row["exit_ts"])
    index_by_ns = {int(value): index for index, value in enumerate(path["timestamp_ns"])}
    post = session.loc[(session.ts - pd.Timedelta(minutes=1) >= entry_end) & session.ts.lt(cutoff)].copy()
    if post.empty:
        return []
    if not np.all(np.diff(post.ts.astype("int64")) == 60_000_000_000):
        raise ValueError("Contiguous post-entry minutes required")
    sign_close = sign * post.close.to_numpy(float)
    best_close = -math.inf
    records = []
    for n, bar in enumerate(post.itertuples(index=False)):
        best_close = max(best_close, sign * float(bar.close))
        if n + 1 < PARAMETERS["minimum_post_entry_bars"]:
            continue
        index = index_by_ns[int(bar.ts.value)]
        if index <= int(row["entry_path_index"]):
            raise ValueError("Entry candle entered a post-entry observation")
        if index + 1 >= len(path["timestamp_ns"]):
            continue
        recent = post.iloc[n - 2:n + 1]
        volume_values = recent.relvol_time.to_numpy(float)
        volume_ratio = float(np.mean(volume_values)) if np.isfinite(volume_values).all() else math.nan
        close = float(bar.close)
        move = sign * (close / entry - 1) * 100
        peak = (best_close - sign * entry) / entry * 100
        giveback = peak - move
        signed_boundary = sign * (close - boundary)
        previous_boundary = sign * (float(post.close.iloc[n - 1]) - boundary)
        momentum = sign * float(bar.roc3_atr)
        under_ema = sign * (close - float(bar.ema20)) < 0
        against_vwap = sign * (close - float(bar.session_vwap)) < 0
        slope_against = sign * float(bar.ema_slope3_atr) < 0
        enough = _finite(momentum, bar.ema20, bar.session_vwap, bar.ema_slope3_atr)
        broken = (signed_boundary < -PARAMETERS["structure_buffer_entry_atr"] * atr and previous_boundary < 0)
        failure = bool(enough and broken and under_ema and
            move <= -PARAMETERS["reversal_adverse_entry_pct"] and
            momentum <= -PARAMETERS["reversal_momentum_3m_atr"])
        volume_support = _finite(volume_ratio) and volume_ratio >= PARAMETERS["volume_confirmation"]
        context_support = enough and against_vwap and slope_against
        activation = max(PARAMETERS["profit_activation_floor_pct"],
            PARAMETERS["profit_activation_target_fraction"] * float(row["native_target_pct"]))
        giveback_threshold = max(PARAMETERS["profit_giveback_floor_pct"],
            PARAMETERS["profit_giveback_entry_atr"] * atr / entry * 100)
        recent_under_ema = all(sign * (float(x.close) - float(x.ema20)) < 0 for x in recent.itertuples())
        protect = bool(enough and peak >= activation and giveback >= giveback_threshold and
            move >= PARAMETERS["profit_minimum_current_gross_pct"] and
            recent_under_ema and momentum <= -PARAMETERS["reversal_momentum_3m_atr"] and
            (volume_support or context_support))
        flags = []
        if failure:
            flags.append("PRICE_STRUCTURE")
            if volume_support:
                flags.append("VOLUME_CONFIRMED")
            if context_support:
                flags.append("CONTEXT_CONFIRMED")
            if volume_support and context_support:
                flags.append("VOLUME_CONTEXT_CONFIRMED")
        if protect:
            flags.append("PROFIT_STRUCTURE")
        state = "UNRESOLVED"
        if failure:
            state = "REVERSAL_RISK"
        elif enough and move > 0 and momentum > 0 and sign_close[n] >= max(sign_close[:n]):
            state = "CONTINUATION"
        elif enough and signed_boundary >= 0 and giveback > 0 and momentum > -1 and _finite(volume_ratio) and volume_ratio < 1:
            state = "PULLBACK_CANDIDATE"
        records.append(dict(sid=int(row["sid"]), day=str(row["day"])[:10],
            setup_id=row["setup_id"], tradingsymbol=row["tradingsymbol"], side=row["side"],
            decision_at_ist=bar.ts.isoformat(), path_index=index, state=state, flags=flags,
            completed_post_entry_bars=n + 1, signed_move_pct=move,
            best_completed_close_profit_pct=peak, close_giveback_pct=giveback,
            confirmation_atr14=atr, frozen_structure_boundary=boundary,
            structure_distance_atr=signed_boundary / atr, signed_momentum_3m_atr=momentum,
            relative_volume_3m=volume_ratio, against_vwap=bool(against_vwap),
            ema_slope_against=bool(slope_against), feature_applicable=bool(enough)))
    return records
