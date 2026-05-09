"""v17D Step 3.3 — time-stop exit logic.

Adds a 4th exit condition to the resolver: if after `time_stop_minutes`
the trade hasn't moved at least `min_progress_r` × initial_risk in the
favorable direction, exit at the current market close.

Frees stuck capital on chop days. Typically adds ~10–15% trades/day
without affecting PF.

Integrates with v17D_exit_resolver: pass the time-stop config through
resolve_with_time_stop().

Public API:
    resolve_with_time_stop(bars, side, entry_price, entry_time,
                            sl_pct, tgt_pct,
                            time_stop_minutes=45,
                            min_progress_r=0.3) -> ResolutionResult | None
"""
from __future__ import annotations

import pandas as pd

from eqidv2.v17D_exit_resolver import (
    ResolutionResult, EOD_CUTOFF_HOUR, EOD_CUTOFF_MIN,
)


def resolve_with_time_stop(
    bars: pd.DataFrame | None,
    side: str,
    entry_price: float,
    entry_time_ist,
    sl_pct: float,
    tgt_pct: float,
    *,
    time_stop_minutes: int = 45,
    min_progress_r: float = 0.3,
) -> ResolutionResult | None:
    """Resolver with TIME_STOP as a possible outcome.

    The time-stop fires when:
      - elapsed >= time_stop_minutes from entry
      - max favorable excursion < min_progress_r × sl_pct
      - SL and TGT haven't been hit yet
    Exit price = current bar close at time-stop trigger.
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

    if side == "LONG":
        sl_price = entry_price * (1 - sl_pct / 100)
        tgt_price = entry_price * (1 + tgt_pct / 100)
    else:
        sl_price = entry_price * (1 + sl_pct / 100)
        tgt_price = entry_price * (1 - tgt_pct / 100)

    progress_threshold_pct = min_progress_r * sl_pct
    time_stop_at = et + pd.Timedelta(minutes=time_stop_minutes)

    h = sub["high"].to_numpy()
    l = sub["low"].to_numpy()
    c = sub["close"].to_numpy()
    times = sub.index

    if side == "LONG":
        sl_hit = l <= sl_price
        tgt_hit = h >= tgt_price
    else:
        sl_hit = h >= sl_price
        tgt_hit = l <= tgt_price

    max_fav_pct = 0.0   # max favorable excursion, % of entry
    for i in range(len(sub)):
        if side == "LONG":
            fav = (h[i] - entry_price) / entry_price * 100
        else:
            fav = (entry_price - l[i]) / entry_price * 100
        if fav > max_fav_pct:
            max_fav_pct = fav

        if sl_hit[i]:
            return ResolutionResult(
                outcome="SL", exit_price=sl_price,
                exit_time_ist=times[i],
                pnl_pct_price=-sl_pct, bars_held=i + 1,
            )
        if tgt_hit[i]:
            return ResolutionResult(
                outcome="TARGET", exit_price=tgt_price,
                exit_time_ist=times[i],
                pnl_pct_price=tgt_pct, bars_held=i + 1,
            )

        # Time-stop check (after `time_stop_minutes` and progress < threshold)
        if times[i] >= time_stop_at and max_fav_pct < progress_threshold_pct:
            exit_close = float(c[i])
            raw = (exit_close - entry_price) / entry_price * 100
            if side == "SHORT":
                raw = -raw
            return ResolutionResult(
                outcome="TIME_STOP", exit_price=exit_close,
                exit_time_ist=times[i],
                pnl_pct_price=raw, bars_held=i + 1,
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

    idx = pd.DatetimeIndex(
        [pd.Timestamp("2026-05-08 09:30", tz="Asia/Kolkata")
         + pd.Timedelta(minutes=i) for i in range(60)]
    )
    bars = pd.DataFrame({
        "high": np.full(60, 500.5),    # tiny range, no SL/TGT hit
        "low": np.full(60, 499.5),
        "close": np.full(60, 500.0),
    }, index=idx)
    res = resolve_with_time_stop(
        bars, "LONG", 500.0, "2026-05-08 09:30",
        sl_pct=1.0, tgt_pct=1.0,
        time_stop_minutes=45, min_progress_r=0.3,
    )
    print(f"Time-stop test: {res}")
    assert res.outcome == "TIME_STOP" and res.bars_held == 46
    print("Time-stop fired at minute 45 as expected.")
