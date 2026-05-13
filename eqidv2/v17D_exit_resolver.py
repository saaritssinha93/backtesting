"""v17D exit resolver — extracted from v17C noNF for reuse.

Walks 1-min OHLC bars from entry time to EOD cutoff (15:20 IST) and
returns the exit outcome:
    TARGET   - target price hit first
    SL       - stop price hit first
    EOD      - neither hit; closed at EOD bar close
    None     - bar data missing or empty

Used by:
    - v17D_signal_to_row (one-shot per Signal)
    - v17D_library_backtest (full backtest loop)
    - v17D_mae_mfe_analyzer (already does its own MAE/MFE walk)

The cost-aware variants are in v17D_cost_model; this module is purely
about price-path resolution.

Public API:
    resolve(bars, side, entry_price, entry_time_ist, sl_pct, tgt_pct)
        -> ResolutionResult | None
    EOD_CUTOFF_HOUR = 15, EOD_CUTOFF_MIN = 20
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

import pandas as pd

EOD_CUTOFF_HOUR = 15
EOD_CUTOFF_MIN = 20

Outcome = Literal["TARGET", "SL", "EOD"]


@dataclass(frozen=True)
class ResolutionResult:
    outcome: Outcome
    exit_price: float
    exit_time_ist: pd.Timestamp
    pnl_pct_price: float    # PRICE-ONLY return %, BEFORE leverage and BEFORE costs
    bars_held: int


def resolve(
    bars: pd.DataFrame | None,
    side: str,
    entry_price: float,
    entry_time_ist: pd.Timestamp | str,
    sl_pct: float,
    tgt_pct: float,
) -> ResolutionResult | None:
    """Walk 1-min bars from entry_time to EOD cutoff and return resolution.

    Args:
        bars: DataFrame with index=DatetimeIndex (tz-aware IST) and columns
              high, low, close. Pass full-day bars or a sliced subset; the
              function further slices to [entry_time, EOD cutoff].
        side: "LONG" or "SHORT"
        entry_price: per-share entry price
        entry_time_ist: entry timestamp (any pandas-parseable; coerced to IST)
        sl_pct: stop-loss percentage (positive number, e.g. 0.75 = 0.75%)
        tgt_pct: target percentage (positive number)

    Returns:
        ResolutionResult or None if no bars available in window.
    """
    if bars is None or bars.empty:
        return None

    side = side.upper()
    if side not in ("LONG", "SHORT"):
        raise ValueError(f"side must be LONG or SHORT, got {side!r}")

    # Normalize entry time to tz-aware IST
    et = pd.to_datetime(entry_time_ist)
    if et.tz is None:
        et = et.tz_localize("Asia/Kolkata")
    else:
        et = et.tz_convert("Asia/Kolkata")

    eod_cutoff = (et.normalize()
                  + pd.Timedelta(hours=EOD_CUTOFF_HOUR, minutes=EOD_CUTOFF_MIN))

    # Slice bars to [entry, EOD]
    sub = bars[(bars.index >= et) & (bars.index <= eod_cutoff)]
    if sub.empty:
        return None

    if side == "LONG":
        sl_price = entry_price * (1.0 - sl_pct / 100.0)
        tgt_price = entry_price * (1.0 + tgt_pct / 100.0)
    else:
        sl_price = entry_price * (1.0 + sl_pct / 100.0)
        tgt_price = entry_price * (1.0 - tgt_pct / 100.0)

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

    for i in range(len(sub)):
        if sl_hit[i] and tgt_hit[i]:
            # Both hit in same bar — pessimistic: assume SL first
            return ResolutionResult(
                outcome="SL",
                exit_price=sl_price,
                exit_time_ist=times[i],
                pnl_pct_price=-sl_pct if side == "LONG" else -sl_pct,
                bars_held=i + 1,
            )
        if sl_hit[i]:
            return ResolutionResult(
                outcome="SL",
                exit_price=sl_price,
                exit_time_ist=times[i],
                pnl_pct_price=-sl_pct,
                bars_held=i + 1,
            )
        if tgt_hit[i]:
            return ResolutionResult(
                outcome="TARGET",
                exit_price=tgt_price,
                exit_time_ist=times[i],
                pnl_pct_price=tgt_pct,
                bars_held=i + 1,
            )

    # Neither hit — close at EOD bar close
    eod_close = float(c[-1])
    raw = (eod_close - entry_price) / entry_price * 100.0
    if side == "SHORT":
        raw = -raw
    return ResolutionResult(
        outcome="EOD",
        exit_price=eod_close,
        exit_time_ist=times[-1],
        pnl_pct_price=raw,
        bars_held=len(sub),
    )


def load_1min(parquet_dir, ticker: str) -> pd.DataFrame | None:
    """Load a ticker's 1-min OHLC parquet, return DataFrame indexed by date."""
    from pathlib import Path
    p = Path(parquet_dir) / f"{ticker}_stocks_indicators_1min.parquet"
    if not p.exists():
        return None
    df = pd.read_parquet(p)
    df = df[["date", "high", "low", "close"]].copy()
    df["date"] = pd.to_datetime(df["date"]).dt.tz_convert("Asia/Kolkata")
    return df.set_index("date").sort_index()


if __name__ == "__main__":
    import numpy as np
    # Synthetic test: LONG, entry at 500, SL at -1%, TGT at +1%
    idx = pd.DatetimeIndex(
        [pd.Timestamp("2026-05-08 09:30", tz="Asia/Kolkata")
         + pd.Timedelta(minutes=i) for i in range(60)]
    )
    bars = pd.DataFrame({
        "high": np.full(60, 502.0),
        "low": np.full(60, 498.0),
        "close": np.full(60, 500.0),
    }, index=idx)
    bars.iloc[10, bars.columns.get_loc("high")] = 506.0  # tgt hit at bar 10
    res = resolve(bars, "LONG", 500.0, "2026-05-08 09:30", 1.0, 1.0)
    print(f"LONG TGT test: {res}")
    assert res.outcome == "TARGET" and res.bars_held == 11

    bars.iloc[10, bars.columns.get_loc("high")] = 502.0
    bars.iloc[15, bars.columns.get_loc("low")] = 494.0  # SL hit at bar 15
    res = resolve(bars, "LONG", 500.0, "2026-05-08 09:30", 1.0, 1.0)
    print(f"LONG SL test: {res}")
    assert res.outcome == "SL" and res.bars_held == 16

    bars.iloc[15, bars.columns.get_loc("low")] = 498.0
    bars.iloc[-1, bars.columns.get_loc("close")] = 502.5
    res = resolve(bars, "LONG", 500.0, "2026-05-08 09:30", 1.0, 1.0)
    print(f"LONG EOD test: {res}")
    assert res.outcome == "EOD"
    print("All resolver tests pass.")
