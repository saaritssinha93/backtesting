"""
5-Min Fresh Momentum AVWAP Breakout — v1 backtester.

Spec     : docs/5min_momentum_avwap_algo_spec.md  (Section 23 v1)
Universe : filtered_stocks_MIS.selected_stocks
5-min    : C:\\TradingData\\eqidv2\\stocks_indicators_5min_eq_live2\\<TICKER>_stocks_indicators_5min.parquet
1-min    : C:\\TradingData\\eqidv2\\stocks_indicators_1min_eq\\<TICKER>_stocks_indicators_1min.parquet
            (1-min used only to resolve intrabar target/SL hits and entry trigger timing)
Output   : outputs_avwap_mom_v1_5min/ (trades.csv, daily.csv, summary.txt)

Position sizing:
    Capital per trade  : Rs 10,000 (user margin)
    Leverage           : 5x (intraday MIS)  ->  effective notional Rs 50,000
    qty = floor(effective_notional / entry_px)

Run examples:
    python avwap_5min_mom_v1_backtesting.py
    python avwap_5min_mom_v1_backtesting.py --limit 50
    python avwap_5min_mom_v1_backtesting.py --cost_bps 10 --workers 8
"""

from __future__ import annotations

import argparse
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass, asdict
from datetime import time as dtime
from pathlib import Path

import numpy as np
import pandas as pd

from filtered_stocks_MIS import selected_stocks


# -------------------- config --------------------
DATA_ROOT_5M = Path(r"C:\TradingData\eqidv2\stocks_indicators_5min_eq_live2")
DATA_ROOT_1M = Path(r"C:\TradingData\eqidv2\stocks_indicators_1min_eq")
OUT_ROOT = Path("outputs_avwap_mom_v1_5min")

# Capital / leverage
CAPITAL_PER_TRADE = 10_000.0      # Rs user margin
LEVERAGE = 5.0                    # intraday MIS leverage
EFFECTIVE_NOTIONAL = CAPITAL_PER_TRADE * LEVERAGE  # 50,000 per trade

# Signal window (IST)
SIGNAL_START = dtime(9, 25)
SIGNAL_END   = dtime(14, 30)
EOD_EXIT     = dtime(15, 15)

# Liquidity
MIN_PRICE      = 100.0
MIN_AVG_VOL_20 = 20_000

# Breakout candle
RET_5M_MIN     = 3.0
BODY_PCT_MIN   = 0.60
CLOSE_LOC_MIN  = 0.75
PREV_HIGH_LOOK = 10

# Freshness
PREV_TOTAL_LOOK = 5
PREV_TOTAL_MAX  = 1.0
BIG_GREEN_THR   = 1.0
BIG_GREEN_MAX   = 1

# Volume
VOL_AVG_LOOK  = 20
VOL_RATIO_MIN = 3.0
VOL_RATIO_MAX = 12.0

# Pre-breakout AVWAP
AVWAP_ANCHOR_LOOK  = 20
AVWAP_DIST_MAX_PCT = 4.5

# Target / stop
TARGET_PCT = 1.0
SL_PCT     = 0.75

# Date range filter (inclusive). None = no filter.
START_DATE: str | None = None    # "YYYY-MM-DD"
END_DATE:   str | None = None    # "YYYY-MM-DD"

DEFAULT_COST_BPS = 10.0
DEFAULT_WORKERS  = 16
MAX_WORKERS      = 16

# Per-side slippage in bps applied to entry/exit fill prices (long-only).
# Entry filled WORSE (higher), exit filled WORSE (lower). Affects realized P&L.
SLIP_BPS_PER_SIDE = 0.0


# -------------------- worker init (propagates CLI overrides to children) --------------------
def _init_worker(overrides: dict):
    g = globals()
    for k, v in overrides.items():
        g[k] = v


# -------------------- trade record --------------------
@dataclass
class Trade:
    ticker: str
    date: str
    signal_ts: pd.Timestamp
    entry_ts: pd.Timestamp
    entry_px: float
    qty: int
    notional_rs: float
    target_px: float
    sl_px: float
    exit_ts: pd.Timestamp
    exit_px: float
    exit_reason: str          # "target" | "sl" | "eod"
    bars_held_1m: int
    pnl_pct: float
    pnl_pct_net: float
    gross_pnl_rs: float
    cost_rs: float
    net_pnl_rs: float
    ret_on_capital_pct: float # net_pnl_rs / CAPITAL_PER_TRADE * 100
    ret_5m: float
    vol_ratio: float
    body_pct: float
    close_loc: float
    prev_5_total_ret: float
    prev_5_big_green: int
    avwap_dist_pct: float
    resolution: str           # "1min" if intrabar resolution used, else "5min"


# -------------------- math helpers --------------------
def avwap_from_anchor(highs: np.ndarray, lows: np.ndarray, closes: np.ndarray,
                      vols: np.ndarray, anchor: int, upto: int) -> float:
    tp = (highs[anchor:upto + 1] + lows[anchor:upto + 1] + closes[anchor:upto + 1]) / 3.0
    v = vols[anchor:upto + 1]
    sv = v.sum()
    if sv <= 0:
        return float("nan")
    return float((tp * v).sum() / sv)


def resolve_exit_1min(day1: pd.DataFrame, entry_trigger_ts: pd.Timestamp,
                      signal_high: float, target_px: float, sl_px: float
                      ) -> tuple[pd.Timestamp, float, str, int, pd.Timestamp]:
    """
    Walk 1-min bars from entry_trigger_ts (start of the next 5-min after signal).
    Returns (entry_ts, entry_px, exit_reason placeholder='', bars_held, exit_ts)
    -- exit handled by caller using same df slice.

    Here we just locate entry: first 1-min bar in [entry_trigger_ts, entry_trigger_ts+5min)
    whose high >= signal_high. entry_px = signal_high (stop-buy fill at trigger level).
    """
    raise NotImplementedError  # kept for clarity; logic inlined in process_day


def find_entry_and_exit_1m(day1: pd.DataFrame, entry_window_start: pd.Timestamp,
                           entry_window_end: pd.Timestamp,
                           signal_high: float, target_px: float, sl_px: float,
                           eod_cutoff: pd.Timestamp
                           ) -> dict | None:
    """
    Using 1-min data, find:
      1) first 1-min bar in [entry_window_start, entry_window_end) with high >= signal_high
         -> entry_ts and entry_px = signal_high
      2) walk forward; first 1-min bar where low<=sl_px OR high>=target_px; SL-first on conflict.
      3) if neither hits before eod_cutoff, exit at the close of bar nearest to eod_cutoff.
    Returns None if entry never triggers or 1-min data is empty.
    """
    if day1.empty:
        return None
    # entry search
    entry_mask = (day1["date"] >= entry_window_start) & (day1["date"] < entry_window_end) & (day1["high"] >= signal_high)
    entry_rows = day1[entry_mask]
    if entry_rows.empty:
        return None
    entry_row = entry_rows.iloc[0]
    entry_ts = entry_row["date"]
    entry_px = signal_high

    # exit search
    after = day1[day1["date"] > entry_ts].reset_index(drop=True)
    if after.empty:
        return None
    for idx, row in after.iterrows():
        if row["date"] >= eod_cutoff:
            return dict(entry_ts=entry_ts, entry_px=entry_px,
                        exit_ts=row["date"], exit_px=float(row["close"]),
                        exit_reason="eod", bars_held=int(idx))
        hit_sl = row["low"] <= sl_px
        hit_tg = row["high"] >= target_px
        if hit_sl:  # SL-first on same-bar conflict
            return dict(entry_ts=entry_ts, entry_px=entry_px,
                        exit_ts=row["date"], exit_px=float(sl_px),
                        exit_reason="sl", bars_held=int(idx))
        if hit_tg:
            return dict(entry_ts=entry_ts, entry_px=entry_px,
                        exit_ts=row["date"], exit_px=float(target_px),
                        exit_reason="target", bars_held=int(idx))
    last = after.iloc[-1]
    return dict(entry_ts=entry_ts, entry_px=entry_px,
                exit_ts=last["date"], exit_px=float(last["close"]),
                exit_reason="eod", bars_held=int(len(after) - 1))


# -------------------- core per-day signal logic --------------------
def process_day(day5: pd.DataFrame, day1: pd.DataFrame, ticker: str) -> list[Trade]:
    df = day5.reset_index(drop=True)
    n = len(df)
    if n < max(VOL_AVG_LOOK, AVWAP_ANCHOR_LOOK, PREV_HIGH_LOOK, PREV_TOTAL_LOOK) + 3:
        return []

    o = df["open"].to_numpy(dtype=np.float64)
    h = df["high"].to_numpy(dtype=np.float64)
    l = df["low"].to_numpy(dtype=np.float64)
    c = df["close"].to_numpy(dtype=np.float64)
    v = df["volume"].to_numpy(dtype=np.float64)
    ts = df["date"].to_numpy()

    ret_5m = np.concatenate([[np.nan], (c[1:] / c[:-1] - 1.0) * 100.0])
    rng = h - l
    body = np.abs(c - o)
    with np.errstate(divide="ignore", invalid="ignore"):
        body_pct = np.where(rng > 0, body / rng, 0.0)
        close_loc = np.where(rng > 0, (c - l) / rng, 0.0)

    s_ret = pd.Series(ret_5m)
    s_h = pd.Series(h)
    s_v = pd.Series(v)
    prev_high = s_h.shift(1).rolling(PREV_HIGH_LOOK).max().to_numpy()
    avg_vol_20 = s_v.shift(1).rolling(VOL_AVG_LOOK).mean().to_numpy()
    with np.errstate(divide="ignore", invalid="ignore"):
        vol_ratio = np.where(avg_vol_20 > 0, v / avg_vol_20, np.nan)
    prev5_total_ret = (pd.Series(c).shift(1) / pd.Series(c).shift(PREV_TOTAL_LOOK + 1) - 1.0).to_numpy() * 100.0
    prev5_big_green = (s_ret.shift(1).rolling(PREV_TOTAL_LOOK)
                       .apply(lambda x: int((x > BIG_GREEN_THR).sum()), raw=True)
                       .to_numpy())

    bars_time = pd.to_datetime(ts).time
    in_window = np.array([(SIGNAL_START <= t <= SIGNAL_END) for t in bars_time])

    for i in range(max(VOL_AVG_LOOK, AVWAP_ANCHOR_LOOK) + 1, n - 1):
        if not in_window[i]: continue
        if c[i] < MIN_PRICE: continue
        if not (avg_vol_20[i] >= MIN_AVG_VOL_20): continue
        if not (ret_5m[i] >= RET_5M_MIN): continue
        if not (c[i] > o[i]): continue
        if not (body_pct[i] >= BODY_PCT_MIN): continue
        if not (close_loc[i] >= CLOSE_LOC_MIN): continue
        if not (c[i] > prev_high[i]): continue
        if not (VOL_RATIO_MIN <= vol_ratio[i] <= VOL_RATIO_MAX): continue
        if not (prev5_total_ret[i] < PREV_TOTAL_MAX): continue
        if not (prev5_big_green[i] <= BIG_GREEN_MAX): continue

        # Pre-breakout AVWAP
        a_start = i - AVWAP_ANCHOR_LOOK
        a_end = i
        anchor = a_start + int(np.argmin(l[a_start:a_end]))
        pre_avwap = avwap_from_anchor(h, l, c, v, anchor, i)
        if not np.isfinite(pre_avwap) or pre_avwap <= 0: continue
        if not (c[i] > pre_avwap): continue
        avwap_dist_pct = (c[i] - pre_avwap) / pre_avwap * 100.0
        if not (avwap_dist_pct <= AVWAP_DIST_MAX_PCT): continue

        # Confirm next-bar high break at 5-min resolution (faithful to spec)
        nxt = i + 1
        signal_high = h[i]
        if h[nxt] < signal_high:
            continue

        # Breakout AVWAP at fill (signal-bar anchor) -- coarse 5-min check
        bo_avwap = avwap_from_anchor(h, l, c, v, i, nxt)
        if not (signal_high > bo_avwap):
            continue

        target_px = signal_high * (1.0 + TARGET_PCT / 100.0)
        sl_px     = signal_high * (1.0 - SL_PCT / 100.0)

        # 1-min resolution for entry timing + exit
        entry_window_start = pd.Timestamp(ts[nxt])
        # next 5-min bar covers exactly 5 minutes
        entry_window_end = entry_window_start + pd.Timedelta(minutes=5)
        eod_cutoff = pd.Timestamp(ts[i]).normalize().tz_localize(None)
        # rebuild tz-aware cutoff at 15:15 IST
        eod_cutoff = pd.Timestamp(ts[i]).tz_convert("Asia/Kolkata").replace(
            hour=EOD_EXIT.hour, minute=EOD_EXIT.minute, second=0, microsecond=0
        )

        resolution = "1min"
        out = find_entry_and_exit_1m(day1, entry_window_start, entry_window_end,
                                     signal_high, target_px, sl_px, eod_cutoff)
        if out is None:
            # fall back to 5-min resolution
            resolution = "5min"
            entry_ts = pd.Timestamp(ts[nxt])
            entry_px = signal_high
            exit_reason = "eod"; exit_ts = ts[-1]; exit_px = c[-1]; bars_held = (n - 1 - nxt) * 5
            for j in range(nxt, n):
                t_j = pd.Timestamp(ts[j]).time()
                if t_j >= EOD_EXIT:
                    exit_reason = "eod"; exit_ts = ts[j]; exit_px = c[j]; bars_held = (j - nxt) * 5; break
                hit_sl = l[j] <= sl_px; hit_tg = h[j] >= target_px
                if hit_sl: exit_reason = "sl"; exit_ts = ts[j]; exit_px = sl_px; bars_held = (j - nxt) * 5; break
                if hit_tg: exit_reason = "target"; exit_ts = ts[j]; exit_px = target_px; bars_held = (j - nxt) * 5; break
            out = dict(entry_ts=entry_ts, entry_px=entry_px,
                       exit_ts=pd.Timestamp(exit_ts), exit_px=float(exit_px),
                       exit_reason=exit_reason, bars_held=int(bars_held))

        # rupee math (apply per-side slippage to fills — entry worse/higher, exit worse/lower)
        slip = SLIP_BPS_PER_SIDE / 10_000.0
        entry_px_f = float(out["entry_px"]) * (1.0 + slip)
        exit_px_f  = float(out["exit_px"])  * (1.0 - slip)
        qty = int(EFFECTIVE_NOTIONAL // entry_px_f)
        if qty <= 0:
            continue
        notional_rs = qty * entry_px_f
        gross_pnl_rs = qty * (exit_px_f - entry_px_f)
        pnl_pct = (exit_px_f / entry_px_f - 1.0) * 100.0

        return [Trade(
            ticker=ticker,
            date=str(pd.Timestamp(ts[i]).date()),
            signal_ts=pd.Timestamp(ts[i]),
            entry_ts=pd.Timestamp(out["entry_ts"]),
            entry_px=entry_px_f,
            qty=qty,
            notional_rs=float(notional_rs),
            target_px=float(target_px),
            sl_px=float(sl_px),
            exit_ts=pd.Timestamp(out["exit_ts"]),
            exit_px=float(exit_px_f),
            exit_reason=str(out["exit_reason"]),
            bars_held_1m=int(out["bars_held"]),
            pnl_pct=float(pnl_pct),
            pnl_pct_net=float(pnl_pct),  # filled post-cost in summarize
            gross_pnl_rs=float(gross_pnl_rs),
            cost_rs=0.0,
            net_pnl_rs=float(gross_pnl_rs),
            ret_on_capital_pct=float(gross_pnl_rs / CAPITAL_PER_TRADE * 100.0),
            ret_5m=float(ret_5m[i]),
            vol_ratio=float(vol_ratio[i]),
            body_pct=float(body_pct[i]),
            close_loc=float(close_loc[i]),
            prev_5_total_ret=float(prev5_total_ret[i]),
            prev_5_big_green=int(prev5_big_green[i]),
            avwap_dist_pct=float(avwap_dist_pct),
            resolution=resolution,
        )]
    return []


# -------------------- per-ticker worker --------------------
def read_ohlcv_parquet(fp: Path) -> pd.DataFrame:
    cols = ["date", "open", "high", "low", "close", "volume"]
    try:
        df = pd.read_parquet(fp, columns=cols + ["date_only"])
    except Exception as e:
        if "date_only" not in str(e):
            raise
        df = pd.read_parquet(fp, columns=cols)

    df["date"] = pd.to_datetime(df["date"])
    if "date_only" not in df.columns:
        df["date_only"] = df["date"].dt.date
    else:
        df["date_only"] = pd.to_datetime(df["date_only"]).dt.date
    return df


def run_ticker(ticker: str) -> list[Trade]:
    fp5 = DATA_ROOT_5M / f"{ticker}_stocks_indicators_5min.parquet"
    fp1 = DATA_ROOT_1M / f"{ticker}_stocks_indicators_1min.parquet"
    if not fp5.exists():
        return []
    df5 = read_ohlcv_parquet(fp5)
    if df5.empty:
        return []
    df5 = df5.sort_values("date").reset_index(drop=True)
    if START_DATE is not None:
        df5 = df5[df5["date_only"] >= pd.to_datetime(START_DATE).date()]
    if END_DATE is not None:
        df5 = df5[df5["date_only"] <= pd.to_datetime(END_DATE).date()]
    if df5.empty:
        return []

    df1 = None
    if fp1.exists():
        try:
            df1 = read_ohlcv_parquet(fp1)
            df1 = df1.sort_values("date").reset_index(drop=True)
            if START_DATE is not None:
                df1 = df1[df1["date_only"] >= pd.to_datetime(START_DATE).date()]
            if END_DATE is not None:
                df1 = df1[df1["date_only"] <= pd.to_datetime(END_DATE).date()]
        except Exception:
            df1 = None

    by_day_1m = {}
    if df1 is not None and not df1.empty:
        by_day_1m = {k: g for k, g in df1.groupby("date_only", sort=False)}

    trades: list[Trade] = []
    for day, day_df5 in df5.groupby("date_only", sort=True):
        day_df1 = by_day_1m.get(day, pd.DataFrame(columns=df5.columns))
        trades.extend(process_day(day_df5, day_df1, ticker))
    return trades


def _worker(ticker: str) -> tuple[str, list[Trade], str | None]:
    try:
        return ticker, run_ticker(ticker), None
    except Exception as e:
        return ticker, [], f"{type(e).__name__}: {e}"


# -------------------- metrics --------------------
def summarize(trades: pd.DataFrame, cost_bps: float) -> dict:
    if trades.empty:
        return {"total_trades": 0}
    cost_pct = cost_bps / 100.0
    cost_frac = cost_bps / 10_000.0

    trades = trades.copy()
    trades["pnl_pct_net"] = trades["pnl_pct"] - cost_pct
    trades["cost_rs"] = trades["notional_rs"] * cost_frac
    trades["net_pnl_rs"] = trades["gross_pnl_rs"] - trades["cost_rs"]
    trades["ret_on_capital_pct"] = trades["net_pnl_rs"] / CAPITAL_PER_TRADE * 100.0

    wins = trades[trades["net_pnl_rs"] > 0]
    losses = trades[trades["net_pnl_rs"] <= 0]
    gross_win_rs = wins["net_pnl_rs"].sum()
    gross_loss_rs = -losses["net_pnl_rs"].sum()
    pf = (gross_win_rs / gross_loss_rs) if gross_loss_rs > 0 else float("inf")

    daily_rs = trades.groupby("date")["net_pnl_rs"].sum().sort_index()
    cum = daily_rs.cumsum()
    max_dd_rs = float((cum - cum.cummax()).min())
    peak = cum.cummax().replace(0, np.nan)
    max_dd_pct_of_peak = float(((cum - cum.cummax()) / peak).min() * 100.0) if peak.notna().any() else 0.0
    day_win = float((daily_rs > 0).mean() * 100.0)

    # Sharpe / Sortino (annualized via sqrt(252))
    d_mean = float(daily_rs.mean())
    d_std  = float(daily_rs.std(ddof=1)) if len(daily_rs) > 1 else 0.0
    sharpe = (d_mean / d_std * np.sqrt(252)) if d_std > 0 else float("nan")
    downside = daily_rs[daily_rs < 0]
    d_dn_std = float(downside.std(ddof=1)) if len(downside) > 1 else 0.0
    sortino = (d_mean / d_dn_std * np.sqrt(252)) if d_dn_std > 0 else float("nan")

    # Consecutive losing days
    sign = (daily_rs <= 0).astype(int)
    max_consec_losses = 0
    cur = 0
    for s in sign:
        cur = cur + 1 if s == 1 else 0
        if cur > max_consec_losses:
            max_consec_losses = cur

    # Time-to-target / SL in minutes (1 bar = 1 min for the 1-min resolved trades; 5 min for fallback)
    minutes_per_bar = trades["resolution"].map({"1min": 1, "5min": 5}).fillna(5)
    trade_minutes = trades["bars_held_1m"] * minutes_per_bar
    t_target = trade_minutes[trades["exit_reason"] == "target"]
    t_sl     = trade_minutes[trades["exit_reason"] == "sl"]

    avg_win = float(wins["net_pnl_rs"].mean()) if not wins.empty else 0.0
    avg_loss = float(losses["net_pnl_rs"].mean()) if not losses.empty else 0.0
    expectancy_rs = float(trades["net_pnl_rs"].mean())
    expectancy_R = expectancy_rs / abs(avg_loss) if avg_loss < 0 else float("nan")

    return {
        "total_trades": int(len(trades)),
        "trading_days": int(daily_rs.size),
        "avg_trades_per_day": float(len(trades) / max(daily_rs.size, 1)),
        "win_rate_pct": float((trades["net_pnl_rs"] > 0).mean() * 100.0),
        "target_hit_rate_pct": float((trades["exit_reason"] == "target").mean() * 100.0),
        "sl_hit_rate_pct": float((trades["exit_reason"] == "sl").mean() * 100.0),
        "eod_rate_pct": float((trades["exit_reason"] == "eod").mean() * 100.0),
        "avg_pnl_pct_net": float(trades["pnl_pct_net"].mean()),
        "median_pnl_pct_net": float(trades["pnl_pct_net"].median()),
        "profit_factor": pf,
        "sharpe_ratio_ann": sharpe,
        "sortino_ratio_ann": sortino,
        "avg_win_rs": avg_win,
        "avg_loss_rs": avg_loss,
        "expectancy_rs": expectancy_rs,
        "expectancy_R": expectancy_R,
        "gross_pnl_rs": float(trades["gross_pnl_rs"].sum()),
        "total_cost_rs": float(trades["cost_rs"].sum()),
        "net_pnl_rs": float(trades["net_pnl_rs"].sum()),
        "ret_on_capital_pct_total": float(trades["net_pnl_rs"].sum() / CAPITAL_PER_TRADE * 100.0),
        "max_dd_rs": max_dd_rs,
        "max_dd_pct_of_peak": max_dd_pct_of_peak,
        "max_consec_losing_days": int(max_consec_losses),
        "day_win_rate_pct": day_win,
        "best_day_rs": float(daily_rs.max()),
        "worst_day_rs": float(daily_rs.min()),
        "avg_bars_held_1m": float(trades["bars_held_1m"].mean()),
        "avg_time_to_target_min": float(t_target.mean()) if not t_target.empty else float("nan"),
        "avg_time_to_sl_min":     float(t_sl.mean())     if not t_sl.empty     else float("nan"),
        "pct_resolved_1min": float((trades["resolution"] == "1min").mean() * 100.0),
        "cost_bps": float(cost_bps),
    }, trades, daily_rs


# -------------------- pretty print --------------------
def _fmt_rs(x: float) -> str:
    sign = "-" if x < 0 else ""
    return f"{sign}Rs {abs(x):>12,.2f}"


def print_pretty(summary: dict, cost_bps: float) -> str:
    if summary.get("total_trades", 0) == 0:
        return "No trades."

    rows = [
        ("Universe / horizon", ""),
        ("  Total trades",            f"{summary['total_trades']:>14,}"),
        ("  Trading days",            f"{summary['trading_days']:>14,}"),
        ("  Avg trades/day",          f"{summary['avg_trades_per_day']:>14.2f}"),
        ("  Avg bars held (1-min)",   f"{summary['avg_bars_held_1m']:>14.2f}"),
        ("  Avg time to target",      f"{summary['avg_time_to_target_min']:>11.2f} min"),
        ("  Avg time to SL",          f"{summary['avg_time_to_sl_min']:>11.2f} min"),
        ("  Pct resolved at 1-min",   f"{summary['pct_resolved_1min']:>13.2f}%"),
        ("Hit-rate breakdown", ""),
        ("  Win rate",                f"{summary['win_rate_pct']:>13.2f}%"),
        ("  Target hits",             f"{summary['target_hit_rate_pct']:>13.2f}%"),
        ("  SL hits",                 f"{summary['sl_hit_rate_pct']:>13.2f}%"),
        ("  EOD exits",               f"{summary['eod_rate_pct']:>13.2f}%"),
        ("  Day win rate",            f"{summary['day_win_rate_pct']:>13.2f}%"),
        ("Per-trade economics (Rs 10,000 capital, 5x leverage = Rs 50,000 notional)", ""),
        ("  Avg win",                 _fmt_rs(summary['avg_win_rs'])),
        ("  Avg loss",                _fmt_rs(summary['avg_loss_rs'])),
        ("  Expectancy / trade",      _fmt_rs(summary['expectancy_rs'])),
        ("  Expectancy (R-multiple)", f"{summary['expectancy_R']:>14.3f}"),
        ("  Profit factor",           f"{summary['profit_factor']:>14.3f}"),
        ("  Avg net pnl %",           f"{summary['avg_pnl_pct_net']:>13.3f}%"),
        ("  Median net pnl %",        f"{summary['median_pnl_pct_net']:>13.3f}%"),
        ("Risk-adjusted return", ""),
        ("  Sharpe (annualized)",     f"{summary['sharpe_ratio_ann']:>14.3f}"),
        ("  Sortino (annualized)",    f"{summary['sortino_ratio_ann']:>14.3f}"),
        ("Aggregate P&L", ""),
        ("  Gross P&L",               _fmt_rs(summary['gross_pnl_rs'])),
        ("  Total cost (@%.1f bps)" % cost_bps, _fmt_rs(summary['total_cost_rs'])),
        ("  Net P&L",                 _fmt_rs(summary['net_pnl_rs'])),
        ("  Return on capital (sum)", f"{summary['ret_on_capital_pct_total']:>13.2f}%"),
        ("Risk", ""),
        ("  Max drawdown",            _fmt_rs(summary['max_dd_rs'])),
        ("  Max DD (% of peak)",      f"{summary['max_dd_pct_of_peak']:>13.2f}%"),
        ("  Max consec losing days",  f"{summary['max_consec_losing_days']:>14d}"),
        ("  Best day",                _fmt_rs(summary['best_day_rs'])),
        ("  Worst day",               _fmt_rs(summary['worst_day_rs'])),
    ]
    width_l = max(len(l) for l, _ in rows) + 2
    out = []
    out.append("=" * 72)
    out.append("  5-Min Fresh Momentum AVWAP Breakout - v1  |  Target +1%  SL -0.75%")
    out.append("  Capital Rs 10,000 per trade @ 5x leverage  |  cost = %.1f bps" % cost_bps)
    out.append("=" * 72)
    for label, val in rows:
        if val == "":
            out.append("")
            out.append(f"  {label}")
            out.append("  " + "-" * (width_l + 18))
        else:
            out.append(f"  {label:<{width_l}}{val}")
    out.append("=" * 72)
    return "\n".join(out)


# -------------------- main --------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=0, help="limit number of tickers (0 = all)")
    ap.add_argument("--cost_bps", type=float, default=DEFAULT_COST_BPS, help="round-trip cost in bps")
    ap.add_argument("--workers", type=int, default=DEFAULT_WORKERS,
                    help=f"parallel worker processes (default {DEFAULT_WORKERS}; capped at {MAX_WORKERS}; 1 = serial)")
    ap.add_argument("--out", type=str, default=str(OUT_ROOT))
    # Filter knobs for calibration sweeps
    ap.add_argument("--ret_min", type=float, default=None)
    ap.add_argument("--body_pct_min", type=float, default=None)
    ap.add_argument("--close_loc_min", type=float, default=None)
    ap.add_argument("--vol_min", type=float, default=None)
    ap.add_argument("--vol_max", type=float, default=None)
    ap.add_argument("--prev_total_max", type=float, default=None)
    ap.add_argument("--big_green_max", type=int, default=None)
    ap.add_argument("--avwap_dist_max", type=float, default=None)
    ap.add_argument("--target_pct", type=float, default=None)
    ap.add_argument("--sl_pct", type=float, default=None)
    ap.add_argument("--signal_start", type=str, default=None, help="HH:MM IST (default 09:25)")
    ap.add_argument("--signal_end",   type=str, default=None, help="HH:MM IST (default 14:30)")
    ap.add_argument("--start_date",   type=str, default=None, help="YYYY-MM-DD inclusive")
    ap.add_argument("--end_date",     type=str, default=None, help="YYYY-MM-DD inclusive")
    ap.add_argument("--slip_bps_per_side", type=float, default=None, help="per-side slippage bps")
    ap.add_argument("--min_price",         type=float, default=None, help="min close price filter")
    args = ap.parse_args()

    def _parse_hhmm(s):
        h, m = s.split(":")
        return dtime(int(h), int(m))

    overrides = {}
    for cli, const in [
        ("ret_min", "RET_5M_MIN"),
        ("body_pct_min", "BODY_PCT_MIN"),
        ("close_loc_min", "CLOSE_LOC_MIN"),
        ("vol_min", "VOL_RATIO_MIN"),
        ("vol_max", "VOL_RATIO_MAX"),
        ("prev_total_max", "PREV_TOTAL_MAX"),
        ("big_green_max", "BIG_GREEN_MAX"),
        ("avwap_dist_max", "AVWAP_DIST_MAX_PCT"),
        ("target_pct", "TARGET_PCT"),
        ("sl_pct", "SL_PCT"),
    ]:
        v = getattr(args, cli)
        if v is not None:
            overrides[const] = v
    if args.signal_start is not None:
        overrides["SIGNAL_START"] = _parse_hhmm(args.signal_start)
    if args.signal_end is not None:
        overrides["SIGNAL_END"]   = _parse_hhmm(args.signal_end)
    if args.start_date is not None:
        overrides["START_DATE"] = args.start_date
    if args.end_date is not None:
        overrides["END_DATE"]   = args.end_date
    if args.slip_bps_per_side is not None:
        overrides["SLIP_BPS_PER_SIDE"] = args.slip_bps_per_side
    if args.min_price is not None:
        overrides["MIN_PRICE"] = args.min_price
    # Apply to parent process (for serial / single-worker mode)
    if overrides:
        _init_worker(overrides)
        print(f"  filter overrides: {overrides}")

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    universe = sorted(selected_stocks)
    if args.limit:
        universe = universe[:args.limit]

    workers = max(1, min(args.workers, MAX_WORKERS))
    print(f"[avwap_5min_mom_v1] universe={len(universe)}  cost_bps={args.cost_bps}  workers={workers}")
    print(f"  capital/trade = Rs {CAPITAL_PER_TRADE:,.0f}  leverage = {LEVERAGE:.0f}x  notional = Rs {EFFECTIVE_NOTIONAL:,.0f}")

    t0 = time.time()
    all_trades: list[Trade] = []
    errors: list[tuple[str, str]] = []

    if workers == 1:
        for k, tk in enumerate(universe, 1):
            tk2, trs, err = _worker(tk)
            if err: errors.append((tk2, err))
            else:   all_trades.extend(trs)
            if k % 100 == 0 or k == len(universe):
                print(f"  [{k:4d}/{len(universe)}] trades={len(all_trades)}  elapsed={time.time()-t0:.1f}s")
    else:
        done = 0
        with ProcessPoolExecutor(max_workers=workers,
                                  initializer=_init_worker,
                                  initargs=(overrides,)) as ex:
            futures = {ex.submit(_worker, tk): tk for tk in universe}
            for fut in as_completed(futures):
                tk2, trs, err = fut.result()
                done += 1
                if err: errors.append((tk2, err))
                else:   all_trades.extend(trs)
                if done % 100 == 0 or done == len(universe):
                    print(f"  [{done:4d}/{len(universe)}] trades={len(all_trades)}  elapsed={time.time()-t0:.1f}s")

    if errors:
        print(f"\n{len(errors)} tickers errored. First 5: {errors[:5]}")

    trades_df = pd.DataFrame([asdict(t) for t in all_trades])
    trades_path = out_dir / "trades.csv"

    if trades_df.empty:
        trades_df.to_csv(trades_path, index=False)
        with open(out_dir / "summary.txt", "w") as f:
            f.write("No trades.\n")
        print("\nNo trades generated.")
        return

    summary, trades_df, daily_rs = summarize(trades_df, args.cost_bps)

    # Order trade columns nicely
    col_order = [
        "ticker", "date", "signal_ts", "entry_ts", "exit_ts", "exit_reason", "resolution",
        "entry_px", "target_px", "sl_px", "exit_px",
        "qty", "notional_rs",
        "gross_pnl_rs", "cost_rs", "net_pnl_rs",
        "pnl_pct", "pnl_pct_net", "ret_on_capital_pct",
        "bars_held_1m",
        "ret_5m", "vol_ratio", "body_pct", "close_loc",
        "prev_5_total_ret", "prev_5_big_green", "avwap_dist_pct",
    ]
    col_order = [c for c in col_order if c in trades_df.columns]
    trades_df = trades_df[col_order + [c for c in trades_df.columns if c not in col_order]]
    trades_df.to_csv(trades_path, index=False, float_format="%.4f")

    daily_path = out_dir / "daily.csv"
    daily_df = daily_rs.reset_index().rename(columns={"net_pnl_rs": "net_pnl_rs"})
    daily_df["cum_net_pnl_rs"] = daily_df["net_pnl_rs"].cumsum()
    daily_df.to_csv(daily_path, index=False, float_format="%.2f")

    pretty = print_pretty(summary, args.cost_bps)
    print()
    print(pretty)
    print(f"\nwrote {trades_path}   ({len(trades_df)} trades)")
    print(f"wrote {daily_path}")

    with open(out_dir / "summary.txt", "w", encoding="utf-8") as f:
        f.write(pretty + "\n\nRaw metrics:\n")
        for k, val in summary.items():
            f.write(f"  {k:28s} : {val}\n")
    print(f"wrote {out_dir/'summary.txt'}")


if __name__ == "__main__":
    main()
