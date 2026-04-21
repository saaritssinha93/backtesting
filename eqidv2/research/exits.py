"""
V17 research framework — exit-logic re-resolution.

Reads 5-min bars (stocks_indicators_5min_eq_live2) for each trade and walks
forward from entry_time to simulate alternative SL / target / time-stop /
trailing-stop / breakeven-trigger policies.

Uses the SAME cost model as the production runner:
  - 0.05% slippage + 0.03% commission per side → 0.16% round-trip on notional
  - +3 bps extra slip on stop exits
  - Leverage 5x; position = 20,000, notional = 100,000
  - pnl_pct is returned on NOTIONAL (matches CSV column meaning)

Ambiguous-bar rule (conservative): if SL and TGT both hit within same bar → SL.
"""
from __future__ import annotations

from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from pathlib import Path as _P

from .core import BARS_5M_DIR, LEVERAGE, OUTPUT_ROOT, STOP_EXTRA_SLIP_BPS

BARS_1M_DIR = OUTPUT_ROOT / "stocks_indicators_1min_eq"

_BAR_CACHE: Dict[str, pd.DataFrame] = {}  # key = (ticker, granularity)


def _load_bars_for(ticker: str, prefer_1min: bool = True) -> pd.DataFrame:
    """Prefer 1-min bars (matches production). Fall back to 5-min if unavailable."""
    key = f"{ticker.upper()}|{'1m' if prefer_1min else '5m'}"
    if key in _BAR_CACHE:
        return _BAR_CACHE[key]

    candidates = []
    if prefer_1min:
        candidates.append(BARS_1M_DIR / f"{ticker.upper()}_stocks_indicators_1min.parquet")
    candidates.append(BARS_5M_DIR / f"{ticker.upper()}_stocks_indicators_5min.parquet")

    df = pd.DataFrame()
    for fpath in candidates:
        if not fpath.exists():
            continue
        try:
            df = pd.read_parquet(fpath)
            break
        except Exception:
            df = pd.DataFrame()
    if df.empty or "date" not in df.columns:
        _BAR_CACHE[key] = pd.DataFrame()
        return _BAR_CACHE[key]
    df = df.sort_values("date").reset_index(drop=True)
    df["date"] = pd.to_datetime(df["date"], utc=False, errors="coerce")
    if df["date"].dt.tz is None:
        df["date"] = df["date"].dt.tz_localize("Asia/Kolkata")
    else:
        df["date"] = df["date"].dt.tz_convert("Asia/Kolkata")
    for c in ("open", "high", "low", "close"):
        df[c] = pd.to_numeric(df.get(c), errors="coerce")
    _BAR_CACHE[key] = df[["date", "open", "high", "low", "close"]].copy()
    return _BAR_CACHE[key]


# Back-compat alias used by validator code
def _load_bars(ticker: str) -> pd.DataFrame:
    return _load_bars_for(ticker, prefer_1min=True)


def _slice_intraday(bars: pd.DataFrame, entry_time: pd.Timestamp, eod_cutoff: pd.Timestamp) -> pd.DataFrame:
    if bars.empty:
        return bars
    et = entry_time
    if bars["date"].dt.tz is not None and et.tz is None:
        et = et.tz_localize("Asia/Kolkata")
    ec = eod_cutoff
    if bars["date"].dt.tz is not None and ec.tz is None:
        ec = ec.tz_localize("Asia/Kolkata")
    day = et.normalize()
    mask = (bars["date"] > et) & (bars["date"] <= ec) & (bars["date"].dt.normalize() == day)
    return bars.loc[mask].reset_index(drop=True)


# ---------------------------------------------------------------------------
# Exit policy config
# ---------------------------------------------------------------------------
class ExitPolicy:
    """
    sl_pct   : stop-loss as fraction of entry price (e.g. 0.0075 = 0.75%)
    tgt_pct  : target
    max_bars : time stop after N 5-min bars (None = disabled). EOD used if reached.
    be_trigger_pct : if price moves at least this far in favour, move stop to entry (+/- small buffer).
                     None = disabled.
    be_buffer_pct  : distance from entry for new stop when BE triggered (default 0.0005).
    trail_pct      : trailing stop distance as fraction of current favorable extreme. None = disabled.
    trail_activate_pct : only start trailing after price moves at least this far in favour. Default 0.
    partial_frac   : fraction of position exited when partial_trigger_pct hit. 0 = disabled.
    partial_trigger_pct : where partial exit triggers.
    """

    __slots__ = (
        "sl_pct",
        "tgt_pct",
        "max_bars",
        "be_trigger_pct",
        "be_buffer_pct",
        "trail_pct",
        "trail_activate_pct",
        "partial_frac",
        "partial_trigger_pct",
    )

    def __init__(
        self,
        sl_pct: float = 0.0075,
        tgt_pct: float = 0.0100,
        max_bars: Optional[int] = None,
        be_trigger_pct: Optional[float] = None,
        be_buffer_pct: float = 0.0005,
        trail_pct: Optional[float] = None,
        trail_activate_pct: float = 0.0,
        partial_frac: float = 0.0,
        partial_trigger_pct: float = 0.0,
    ):
        self.sl_pct = float(sl_pct)
        self.tgt_pct = float(tgt_pct)
        self.max_bars = int(max_bars) if max_bars is not None else None
        self.be_trigger_pct = float(be_trigger_pct) if be_trigger_pct is not None else None
        self.be_buffer_pct = float(be_buffer_pct)
        self.trail_pct = float(trail_pct) if trail_pct is not None else None
        self.trail_activate_pct = float(trail_activate_pct)
        self.partial_frac = float(partial_frac)
        self.partial_trigger_pct = float(partial_trigger_pct)


# ---------------------------------------------------------------------------
# Walk-forward resolver
# ---------------------------------------------------------------------------
def _stop_slip(side: str, stop_price: float) -> float:
    slip = STOP_EXTRA_SLIP_BPS / 10000.0
    return stop_price * (1.0 + slip) if side == "SHORT" else stop_price * (1.0 - slip)


def _resolve_single_trade(trade: pd.Series, bars: pd.DataFrame, policy: ExitPolicy) -> Dict:
    """
    Returns dict with keys: exit_price, exit_time, outcome, pnl_pct_gross_notional
                            (net already applied with round-trip cost),
                            realized_bars, partial_price, partial_outcome.
    pnl_pct_gross_notional is leveraged 5x (matches CSV semantics).
    """
    side = trade["side"].upper()
    entry = float(trade["entry_price"])

    if side == "SHORT":
        sl_price = entry * (1.0 + policy.sl_pct)
        tgt_price = entry * (1.0 - policy.tgt_pct)
    else:
        sl_price = entry * (1.0 - policy.sl_pct)
        tgt_price = entry * (1.0 + policy.tgt_pct)

    # Dynamic state
    be_armed = False
    trail_armed = False
    favorable_extreme = entry  # for trailing
    partial_hit = False
    partial_price = np.nan

    exit_price = None
    exit_time = None
    outcome = None
    stop_penalty = False
    bars_walked = 0

    for i, bar in enumerate(bars.itertuples(index=False), start=1):
        bars_walked = i
        if policy.max_bars is not None and i > policy.max_bars:
            # time stop: exit at prior close (use this bar's open as realistic next-bar)
            exit_price = float(bar.open) if not np.isnan(bar.open) else float(bar.close)
            exit_time = bar.date
            outcome = "TIMESTOP"
            break

        bar_high = float(bar.high)
        bar_low = float(bar.low)
        if np.isnan(bar_high) or np.isnan(bar_low):
            continue

        # Update favorable extreme for trailing
        if side == "LONG":
            favorable_extreme = max(favorable_extreme, bar_high)
        else:
            favorable_extreme = min(favorable_extreme, bar_low)

        # Arm breakeven if triggered
        if policy.be_trigger_pct is not None and not be_armed:
            trig = entry * (1.0 + policy.be_trigger_pct) if side == "LONG" else entry * (1.0 - policy.be_trigger_pct)
            hit_trig = bar_high >= trig if side == "LONG" else bar_low <= trig
            if hit_trig:
                be_armed = True
                new_sl = entry * (1.0 + policy.be_buffer_pct) if side == "SHORT" else entry * (1.0 - policy.be_buffer_pct)
                # tighten only — never loosen
                if side == "SHORT":
                    sl_price = min(sl_price, new_sl)
                else:
                    sl_price = max(sl_price, new_sl)

        # Arm trailing if triggered
        if policy.trail_pct is not None and not trail_armed:
            trig_t = entry * (1.0 + policy.trail_activate_pct) if side == "LONG" else entry * (1.0 - policy.trail_activate_pct)
            hit_trig_t = bar_high >= trig_t if side == "LONG" else bar_low <= trig_t
            if hit_trig_t:
                trail_armed = True

        # Update trailing stop
        if policy.trail_pct is not None and trail_armed:
            if side == "LONG":
                new_sl = favorable_extreme * (1.0 - policy.trail_pct)
                sl_price = max(sl_price, new_sl)
            else:
                new_sl = favorable_extreme * (1.0 + policy.trail_pct)
                sl_price = min(sl_price, new_sl)

        # Partial exit
        if policy.partial_frac > 0 and not partial_hit:
            pt = entry * (1.0 + policy.partial_trigger_pct) if side == "LONG" else entry * (1.0 - policy.partial_trigger_pct)
            hit_partial = bar_high >= pt if side == "LONG" else bar_low <= pt
            if hit_partial:
                partial_hit = True
                partial_price = pt

        # Detect exit in this bar
        if side == "SHORT":
            stop_hit = bar_high >= sl_price
            target_hit = bar_low <= tgt_price
        else:
            stop_hit = bar_low <= sl_price
            target_hit = bar_high >= tgt_price

        if stop_hit and target_hit:
            # Conservative: assume SL first (same convention as production runner)
            exit_price = _stop_slip(side, sl_price)
            exit_time = bar.date
            outcome = "SL"
            stop_penalty = True
            break
        if stop_hit:
            exit_price = _stop_slip(side, sl_price)
            exit_time = bar.date
            outcome = "SL"
            stop_penalty = True
            break
        if target_hit:
            exit_price = tgt_price
            exit_time = bar.date
            outcome = "TARGET"
            break

    if exit_price is None:
        if len(bars) == 0:
            return {"exit_price": np.nan, "exit_time": pd.NaT, "outcome": "NO_BARS",
                    "pnl_pct_gross": 0.0, "pnl_pct": 0.0, "bars_walked": 0,
                    "partial_outcome": "NONE", "partial_price": np.nan}
        last = bars.iloc[-1]
        exit_price = float(last["close"])
        exit_time = last["date"]
        outcome = "EOD"

    # P&L — match CSV semantics: leveraged on notional (5x)
    if side == "LONG":
        gross_price_pct = 100.0 * (exit_price - entry) / entry
    else:
        gross_price_pct = 100.0 * (entry - exit_price) / entry

    gross_levered = gross_price_pct * LEVERAGE  # 5x leverage (matches CSV pnl_pct_gross)
    # Production cost is 0.08% one-way on notional × 2 = 0.16% round-trip on notional.
    # When reported on leveraged/position basis the cost becomes 0.80% (= 0.16 × 5).
    rt_cost_levered_pct = 0.80  # matches CSV column 'pnl_pct' = pnl_pct_gross - 0.80
    net_levered = gross_levered - rt_cost_levered_pct

    # If partial was taken, blend 50% @ partial and 50% @ final (if partial_frac=0.5)
    if policy.partial_frac > 0 and not np.isnan(partial_price):
        if side == "LONG":
            p_gross_price = 100.0 * (partial_price - entry) / entry
        else:
            p_gross_price = 100.0 * (entry - partial_price) / entry
        p_gross_lev = p_gross_price * LEVERAGE
        p_net_lev = p_gross_lev - rt_cost_levered_pct  # round-trip on that fraction too
        blend = policy.partial_frac
        gross_levered = blend * p_gross_lev + (1 - blend) * gross_levered
        net_levered = blend * p_net_lev + (1 - blend) * net_levered

    return {
        "exit_price": exit_price,
        "exit_time": exit_time,
        "outcome": outcome,
        "pnl_pct_gross": gross_levered,
        "pnl_pct": net_levered,
        "bars_walked": bars_walked,
        "partial_outcome": "PARTIAL" if partial_hit else "NONE",
        "partial_price": partial_price,
        "stop_penalty": stop_penalty,
    }


def resolve_exits(trades: pd.DataFrame, policy: ExitPolicy, eod_time_str: str = "15:20:00") -> pd.DataFrame:
    """
    Resolve alternative exit policy for each trade using 5-min bars.
    Returns a new DataFrame with updated: exit_price, exit_time_ist, outcome, pnl_pct, pnl_pct_gross, bars_walked.
    Any trades with no bar data are dropped.
    """
    if trades.empty:
        return trades.copy()
    out = trades.copy().reset_index(drop=True)
    out["_new_exit_price"] = np.nan
    out["_new_exit_time"] = pd.NaT
    out["_new_outcome"] = ""
    out["_new_pnl_pct"] = np.nan
    out["_new_pnl_pct_gross"] = np.nan
    out["_new_bars_walked"] = 0

    eod_hh, eod_mm, eod_ss = [int(x) for x in eod_time_str.split(":")]

    for idx in out.index:
        ticker = str(out.at[idx, "ticker"])
        entry_time = out.at[idx, "entry_time_ist"]
        if pd.isna(entry_time):
            continue
        day = pd.Timestamp(entry_time).normalize()
        eod_cutoff = day.replace(hour=eod_hh, minute=eod_mm, second=eod_ss)

        # Try 1-min first; fall back to 5-min if empty or no coverage on this day.
        intraday = pd.DataFrame()
        for prefer_1m in (True, False):
            bars = _load_bars_for(ticker, prefer_1min=prefer_1m)
            if bars.empty:
                continue
            if bars["date"].dt.tz is not None:
                if eod_cutoff.tz is None:
                    eod_cutoff = eod_cutoff.tz_localize("Asia/Kolkata")
                if entry_time.tz is None:
                    entry_time = entry_time.tz_localize("Asia/Kolkata")
            intraday = _slice_intraday(bars, entry_time, eod_cutoff)
            if not intraday.empty:
                break
        if intraday.empty:
            continue
        res = _resolve_single_trade(out.loc[idx], intraday, policy)
        out.at[idx, "_new_exit_price"] = res["exit_price"]
        out.at[idx, "_new_exit_time"] = res["exit_time"]
        out.at[idx, "_new_outcome"] = res["outcome"]
        out.at[idx, "_new_pnl_pct"] = res["pnl_pct"]
        out.at[idx, "_new_pnl_pct_gross"] = res["pnl_pct_gross"]
        out.at[idx, "_new_bars_walked"] = res["bars_walked"]

    resolved = out.dropna(subset=["_new_exit_price"]).copy()
    resolved["exit_price"] = resolved["_new_exit_price"]
    resolved["exit_time_ist"] = resolved["_new_exit_time"]
    resolved["outcome"] = resolved["_new_outcome"]
    resolved["pnl_pct"] = resolved["_new_pnl_pct"]
    resolved["pnl_pct_gross"] = resolved["_new_pnl_pct_gross"]
    resolved["bars_walked"] = resolved["_new_bars_walked"]
    drop_cols = [c for c in resolved.columns if c.startswith("_new_")]
    return resolved.drop(columns=drop_cols).reset_index(drop=True)
