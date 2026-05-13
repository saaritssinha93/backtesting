"""v17D Step 3.4 — trailing stop post-1R.

Two-stage trailing logic:
    Stage 1: After 1R favorable -> stop moves to breakeven
    Stage 2: After 1.5R favorable -> stop trails at 0.5R behind highest
                                      favorable price (LONG) / lowest (SHORT)

Captures runners on trend days while protecting gains. Typically lifts
expectancy 10-15% on high-MFE setups (B_HUGE_*, A_MOD_BREAK_*); modest
or flat on tight-range setups (D_EMA20_BOUNCE).

Public API:
    resolve_with_trailing(bars, side, entry_price, entry_time,
                           sl_pct, tgt_pct,
                           breakeven_at_r=1.0,
                           trail_start_r=1.5,
                           trail_distance_r=0.5) -> ResolutionResult | None

Outcome label:
    TRAIL  - exited via trailing stop (between SL and TGT)
    SL     - never reached 1R; exited at original SL
    TARGET - reached TGT before any trail trigger
    EOD    - never triggered SL, TGT, or trail
"""
from __future__ import annotations

import pandas as pd

from eqidv2.v17D_exit_resolver import (
    ResolutionResult, EOD_CUTOFF_HOUR, EOD_CUTOFF_MIN,
)


def resolve_with_trailing(
    bars: pd.DataFrame | None,
    side: str,
    entry_price: float,
    entry_time_ist,
    sl_pct: float,
    tgt_pct: float,
    *,
    breakeven_at_r: float = 1.0,
    trail_start_r: float = 1.5,
    trail_distance_r: float = 0.5,
) -> ResolutionResult | None:
    """Trailing-stop resolver. SL_R unit = sl_pct.

    Args:
        breakeven_at_r: move stop to breakeven once price reaches this many R
        trail_start_r: start trailing stop once price reaches this many R
        trail_distance_r: trail stop this many R behind the high-water mark
    """
    if bars is None or bars.empty:
        return None

    side = side.upper()
    et = pd.to_datetime(entry_time_ist)
    if et.tz is None:
        et = et.tz_localize("Asia/Kolkata")
    else:
        et = et.tz_convert("Asia/Kolkata")
    eod_cutoff = (et.normalize()
                  + pd.Timedelta(hours=EOD_CUTOFF_HOUR, minutes=EOD_CUTOFF_MIN))
    sub = bars[(bars.index >= et) & (bars.index <= eod_cutoff)]
    if sub.empty:
        return None

    # In R-units expressed as % of entry: 1R = sl_pct, 1.5R = 1.5 * sl_pct, ...
    if side == "LONG":
        original_sl = entry_price * (1 - sl_pct / 100)
        tgt_price = entry_price * (1 + tgt_pct / 100)
    else:
        original_sl = entry_price * (1 + sl_pct / 100)
        tgt_price = entry_price * (1 - tgt_pct / 100)

    h = sub["high"].to_numpy()
    l = sub["low"].to_numpy()
    c = sub["close"].to_numpy()
    times = sub.index

    current_sl = original_sl
    high_water_pct = 0.0    # max favorable excursion in % of entry

    for i in range(len(sub)):
        # Update high-water excursion
        if side == "LONG":
            fav_now = (h[i] - entry_price) / entry_price * 100
        else:
            fav_now = (entry_price - l[i]) / entry_price * 100
        if fav_now > high_water_pct:
            high_water_pct = fav_now

        # Update trailing stop
        if high_water_pct >= breakeven_at_r * sl_pct:
            new_sl = entry_price   # breakeven
            current_sl = max(current_sl, new_sl) if side == "LONG" \
                         else min(current_sl, new_sl)
        if high_water_pct >= trail_start_r * sl_pct:
            # Trail at trail_distance_r below the high-water price
            if side == "LONG":
                hwm_price = entry_price * (1 + high_water_pct / 100)
                trail_sl = hwm_price * (1 - (trail_distance_r * sl_pct) / 100)
                current_sl = max(current_sl, trail_sl)
            else:
                hwm_price = entry_price * (1 - high_water_pct / 100)
                trail_sl = hwm_price * (1 + (trail_distance_r * sl_pct) / 100)
                current_sl = min(current_sl, trail_sl)

        # Check exits in priority: TGT > current_sl
        if side == "LONG":
            if h[i] >= tgt_price:
                return ResolutionResult(
                    outcome="TARGET", exit_price=tgt_price,
                    exit_time_ist=times[i],
                    pnl_pct_price=tgt_pct, bars_held=i + 1,
                )
            if l[i] <= current_sl:
                exit_price = current_sl
                pnl = (exit_price - entry_price) / entry_price * 100
                outcome = "TRAIL" if current_sl > original_sl else "SL"
                return ResolutionResult(
                    outcome=outcome, exit_price=exit_price,
                    exit_time_ist=times[i],
                    pnl_pct_price=pnl, bars_held=i + 1,
                )
        else:
            if l[i] <= tgt_price:
                return ResolutionResult(
                    outcome="TARGET", exit_price=tgt_price,
                    exit_time_ist=times[i],
                    pnl_pct_price=tgt_pct, bars_held=i + 1,
                )
            if h[i] >= current_sl:
                exit_price = current_sl
                pnl = (entry_price - exit_price) / entry_price * 100
                outcome = "TRAIL" if current_sl < original_sl else "SL"
                return ResolutionResult(
                    outcome=outcome, exit_price=exit_price,
                    exit_time_ist=times[i],
                    pnl_pct_price=pnl, bars_held=i + 1,
                )

    eod_close = float(c[-1])
    raw = (eod_close - entry_price) / entry_price * 100
    if side == "SHORT":
        raw = -raw
    return ResolutionResult(
        outcome="EOD", exit_price=eod_close,
        exit_time_ist=times[-1],
        pnl_pct_price=raw, bars_held=len(sub),
    )


if __name__ == "__main__":
    import numpy as np

    # LONG entry 500, SL 1%, TGT 2%. Price hits 1.2R then pulls back.
    # Expected: stop should move to breakeven (entry=500), trade exits at trail.
    idx = pd.DatetimeIndex(
        [pd.Timestamp("2026-05-08 09:30", tz="Asia/Kolkata")
         + pd.Timedelta(minutes=i) for i in range(60)]
    )
    n = 60
    highs = np.full(n, 500.5)
    lows = np.full(n, 499.5)
    closes = np.full(n, 500.0)
    # bar 5: hits 1.2R (= 1.2% above entry = 506.0)
    highs[5] = 506.0
    closes[5] = 506.0
    # bar 10: pulls back to entry, triggering breakeven stop
    lows[10] = 499.0
    closes[10] = 499.0
    bars = pd.DataFrame({"high": highs, "low": lows, "close": closes}, index=idx)
    res = resolve_with_trailing(
        bars, "LONG", 500.0, "2026-05-08 09:30",
        sl_pct=1.0, tgt_pct=2.0,
    )
    print(f"Trailing test: {res}")
    assert res.outcome == "TRAIL" and abs(res.exit_price - 500.0) < 0.01
    print("Trail-to-breakeven fired correctly.")
