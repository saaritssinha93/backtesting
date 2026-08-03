"""Backward-compatible V11 research exit-policy resolver.

With no ``exit_policy`` this delegates byte-for-byte behavior to the shared
v17D resolver. Dynamic policies are used only by explicitly selected V11
research books.
"""

from __future__ import annotations

from typing import Any

import pandas as pd

import v17D_exit_resolver as _base


ResolutionResult = _base.ResolutionResult


def resolve(
    bars: pd.DataFrame | None,
    side: str,
    entry_price: float,
    entry_time_ist: pd.Timestamp | str,
    sl_pct: float,
    tgt_pct: float,
    exit_policy: dict[str, Any] | None = None,
) -> ResolutionResult | None:
    if not exit_policy:
        return _base.resolve(bars, side, entry_price, entry_time_ist, sl_pct, tgt_pct)
    if bars is None or bars.empty:
        return None
    side = side.upper()
    if side not in {"LONG", "SHORT"}:
        raise ValueError(f"side must be LONG or SHORT, got {side!r}")

    policy = dict(exit_policy)
    allowed = {
        "max_hold_minutes",
        "forced_exit_time",
        "stop_gap_mode",
        "breakeven_trigger_r",
        "trailing_trigger_r",
        "trailing_distance_r",
    }
    unknown = sorted(set(policy) - allowed)
    if unknown:
        raise ValueError(f"unsupported exit_policy keys: {unknown}")
    for key, value in policy.items():
        if key in {"forced_exit_time", "stop_gap_mode"}:
            continue
        if float(value) <= 0:
            raise ValueError(f"{key} must be positive")
    if ("trailing_trigger_r" in policy) != ("trailing_distance_r" in policy):
        raise ValueError("trailing_trigger_r and trailing_distance_r must be supplied together")

    et = pd.to_datetime(entry_time_ist)
    et = et.tz_localize("Asia/Kolkata") if et.tz is None else et.tz_convert("Asia/Kolkata")
    cutoff = et.normalize() + pd.Timedelta(
        hours=_base.EOD_CUTOFF_HOUR,
        minutes=_base.EOD_CUTOFF_MIN,
    )
    forced_exit = None
    if policy.get("forced_exit_time"):
        try:
            hh, mm = str(policy["forced_exit_time"]).split(":", 1)
            forced_exit = et.normalize() + pd.Timedelta(hours=int(hh), minutes=int(mm))
            cutoff = min(cutoff, forced_exit)
        except Exception as exc:
            raise ValueError(
                f"invalid forced_exit_time: {policy['forced_exit_time']!r}"
            ) from exc
    stop_gap_mode = str(policy.get("stop_gap_mode", "level")).strip().lower()
    if stop_gap_mode not in {"level", "worse_open"}:
        raise ValueError(f"unsupported stop_gap_mode: {stop_gap_mode!r}")
    sub = bars[(bars.index >= et) & (bars.index <= cutoff)]
    if sub.empty:
        return None

    if side == "LONG":
        initial_stop = entry_price * (1.0 - sl_pct / 100.0)
        target = entry_price * (1.0 + tgt_pct / 100.0)
    else:
        initial_stop = entry_price * (1.0 + sl_pct / 100.0)
        target = entry_price * (1.0 - tgt_pct / 100.0)
    initial_risk = abs(entry_price - initial_stop)
    active_stop = initial_stop
    stop_outcome = "SL"
    best_price = entry_price

    def result(outcome: str, price: float, i: int) -> ResolutionResult:
        pnl_pct = (price - entry_price) / entry_price * 100.0
        if side == "SHORT":
            pnl_pct = -pnl_pct
        return ResolutionResult(outcome, float(price), sub.index[i], float(pnl_pct), i + 1)

    for i, (_, bar) in enumerate(sub.iterrows()):
        high, low, close = float(bar["high"]), float(bar["low"]), float(bar["close"])
        op = float(bar.get("open", close))
        stop_hit = low <= active_stop if side == "LONG" else high >= active_stop
        target_hit = high >= target if side == "LONG" else low <= target
        stop_fill = active_stop
        if stop_gap_mode == "worse_open":
            stop_fill = min(active_stop, op) if side == "LONG" else max(active_stop, op)
        if stop_hit and target_hit:
            return result(stop_outcome, stop_fill, i)
        if stop_hit:
            return result(stop_outcome, stop_fill, i)
        if target_hit:
            return result("TARGET", target, i)

        max_hold = policy.get("max_hold_minutes")
        if max_hold is not None:
            held = (sub.index[i] - et).total_seconds() / 60.0
            if held >= float(max_hold):
                return result("TIME", close, i)
        if forced_exit is not None and sub.index[i] >= forced_exit:
            return result("TIME", close, i)

        # Arm dynamic stops for the following bar. The order of high and low
        # within the current OHLC bar is unknowable.
        if side == "LONG":
            best_price = max(best_price, high)
            favorable_r = (best_price - entry_price) / initial_risk
            if favorable_r >= float(policy.get("breakeven_trigger_r", "inf")):
                if entry_price > active_stop:
                    active_stop, stop_outcome = entry_price, "BREAKEVEN"
            if favorable_r >= float(policy.get("trailing_trigger_r", "inf")):
                trail = best_price - float(policy["trailing_distance_r"]) * initial_risk
                if trail > active_stop:
                    active_stop, stop_outcome = trail, "TRAIL"
        else:
            best_price = min(best_price, low)
            favorable_r = (entry_price - best_price) / initial_risk
            if favorable_r >= float(policy.get("breakeven_trigger_r", "inf")):
                if entry_price < active_stop:
                    active_stop, stop_outcome = entry_price, "BREAKEVEN"
            if favorable_r >= float(policy.get("trailing_trigger_r", "inf")):
                trail = best_price + float(policy["trailing_distance_r"]) * initial_risk
                if trail < active_stop:
                    active_stop, stop_outcome = trail, "TRAIL"

    return result("EOD", float(sub.iloc[-1]["close"]), len(sub) - 1)
