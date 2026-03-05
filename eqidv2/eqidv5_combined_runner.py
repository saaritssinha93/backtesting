# -*- coding: utf-8 -*-
"""
eqidv5_combined_runner.py -- Combined LONG + SHORT runner for eqidv5 strategy
==============================================================================

Volume Profile + Anchored VWAP + Liquidity Sweep Backtester

Usage:
    python eqidv5_combined_runner.py
    python eqidv5_combined_runner.py --dir15m path/to/15min --workers 8

Features:
  - Parallel ticker scanning via ProcessPoolExecutor
  - LONG + SHORT sides run independently on same data
  - Notional P&L with intraday leverage
  - Cash-constrained portfolio simulation (optional)
  - 20 comprehensive analysis charts
  - Console output tee'd to outputs_eqidv5/run_log_<ts>.txt
  - CSV exports: trades_long, trades_short, trades_combined
  - Full backtest metrics (Sharpe, Sortino, Calmar, drawdown, profit factor)
"""

from __future__ import annotations

import heapq
import os
import sys
import warnings
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import asdict
from datetime import time as dtime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore", category=FutureWarning)

# ---------------------------------------------------------------------------
# Ensure this script's directory is importable
# ---------------------------------------------------------------------------
_this_dir = Path(__file__).resolve().parent
if str(_this_dir) not in sys.path:
    sys.path.insert(0, str(_this_dir))

from eqidv5_strategy_common import (
    IST,
    BacktestMetrics,
    Eqidv5Config,
    apply_topn_per_day,
    compute_backtest_metrics,
    default_long_config,
    default_short_config,
    list_tickers_15m,
    now_ist,
    print_metrics,
    read_15m_parquet,
    trades_to_df,
)
from eqidv5_long_strategy import (
    scan_all_days_for_ticker as scan_long,
)
from eqidv5_short_strategy import (
    scan_all_days_for_ticker as scan_short,
)


# ===========================================================================
# CONSOLE OUTPUT TEE
# ===========================================================================
class _Tee:
    """Write to multiple streams simultaneously (console + log file)."""

    def __init__(self, *streams):
        self.streams = [s for s in streams if s is not None]

    def write(self, data: str) -> None:
        for s in self.streams:
            try:
                s.write(data)
            except Exception:
                pass

    def flush(self) -> None:
        for s in self.streams:
            try:
                s.flush()
            except Exception:
                pass

    def isatty(self) -> bool:
        return False


# ===========================================================================
# RUNNER CONFIG (top-level orchestration)
# ===========================================================================

# Position sizes (capital / margin per trade in Rs.)
POSITION_SIZE_RS_SHORT = 50_000
POSITION_SIZE_RS_LONG  = 100_000

# Intraday leverage
INTRADAY_LEVERAGE_SHORT = 5.0
INTRADAY_LEVERAGE_LONG  = 5.0

# Portfolio start capital for cash-constrained sim
PORTFOLIO_START_CAPITAL_RS = 1_000_000

# Parallel workers (set 1 for serial debug)
MAX_WORKERS = 4

# Optional: restrict both scans to top-N tickers by data availability
# Set to None or 0 to scan all tickers
MAX_TICKERS_OVERRIDE = None

ENABLE_CASH_CONSTRAINED_PORTFOLIO_SIM = False


def _derive_5m_dir(dir15m: str) -> str:
    p15 = Path(dir15m)
    cands = []
    if "15min" in p15.name:
        cands.append(p15.with_name(p15.name.replace("15min", "5min")))
    cands.append(p15.with_name("stocks_indicators_5min_eq"))
    cands.append(Path("eqidv2/stocks_indicators_5min_eq"))
    cands.append(Path("stocks_indicators_5min_eq"))
    for c in cands:
        if c.exists():
            return str(c)
    return str(cands[0])


# ===========================================================================
# WORKER FUNCTIONS (top-level for pickling in ProcessPoolExecutor)
# ===========================================================================
def _scan_one_ticker_long(args: Tuple[str, str, Eqidv5Config]) -> List[dict]:
    ticker, path, cfg = args
    df = read_15m_parquet(path, cfg.parquet_engine)
    if df.empty:
        return []
    trades = scan_long(ticker, df, cfg)
    return [asdict(t) for t in trades]


def _scan_one_ticker_short(args: Tuple[str, str, Eqidv5Config]) -> List[dict]:
    ticker, path, cfg = args
    df = read_15m_parquet(path, cfg.parquet_engine)
    if df.empty:
        return []
    trades = scan_short(ticker, df, cfg)
    return [asdict(t) for t in trades]


# ===========================================================================
# PARALLEL SCAN RUNNER
# ===========================================================================
def _run_side_parallel(
    side: str,
    cfg: Eqidv5Config,
    max_workers: int = MAX_WORKERS,
    max_tickers: Optional[int] = None,
) -> pd.DataFrame:
    """
    Scan all tickers for one direction using ProcessPoolExecutor.
    Falls back to serial execution when max_workers <= 1.
    """
    tickers = list_tickers_15m(cfg.dir_15m, cfg.end_15m)
    if max_tickers and max_tickers > 0:
        tickers = tickers[:max_tickers]

    print(f"[{side}] Scanning {len(tickers)} tickers ...")

    worker_fn = _scan_one_ticker_long if side == "LONG" else _scan_one_ticker_short
    task_args = [
        (t, os.path.join(cfg.dir_15m, f"{t}{cfg.end_15m}"), cfg)
        for t in tickers
    ]

    all_dicts: List[dict] = []

    if max_workers <= 1:
        for k, args in enumerate(task_args, 1):
            try:
                result = worker_fn(args)
                all_dicts.extend(result)
            except Exception as exc:
                print(f"  [{side}] ERROR on {args[0]}: {exc}")
            if k % 50 == 0 or k == len(task_args):
                print(f"  [{side}] scanned {k}/{len(tickers)} | trades={len(all_dicts)}")
    else:
        done = 0
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(worker_fn, a): a[0] for a in task_args}
            for future in as_completed(futures):
                done += 1
                try:
                    result = future.result()
                    all_dicts.extend(result)
                except Exception as exc:
                    print(f"  [{side}] ERROR on {futures[future]}: {exc}")
                if done % 100 == 0 or done == len(tickers):
                    print(f"  [{side}] scanned {done}/{len(tickers)} | trades={len(all_dicts)}")

    if not all_dicts:
        print(f"[{side}] No trades found.")
        return pd.DataFrame()

    out = pd.DataFrame(all_dicts)
    out = apply_topn_per_day(out, cfg)

    for c in ["signal_time_ist", "entry_time_ist", "exit_time_ist"]:
        if c in out.columns:
            out[c] = pd.to_datetime(out[c], errors="coerce")

    sort_cols = [c for c in ["trade_date", "ticker", "entry_time_ist"] if c in out.columns]
    if sort_cols:
        out = out.sort_values(sort_cols).reset_index(drop=True)

    print(f"[{side}] Scan complete: {len(out)} trades across {out['trade_date'].nunique() if 'trade_date' in out.columns else 0} days.")
    return out


# ===========================================================================
# NOTIONAL P&L
# ===========================================================================
def _add_notional_pnl(df: pd.DataFrame) -> pd.DataFrame:
    """
    Augment trades DataFrame with leverage-adjusted P&L and rupee P&L.

    pnl_pct_price   = raw price return % (unlevered, as computed by strategy)
    pnl_pct         = capital ROI % = pnl_pct_price * leverage
    pnl_rs          = rupee P&L on notional = (pnl_pct_price/100) * notional
    """
    if df.empty:
        return df

    d = df.copy()

    if "pnl_pct_price" not in d.columns:
        d["pnl_pct_price"] = pd.to_numeric(d.get("pnl_pct", 0.0), errors="coerce").fillna(0.0)
    if "pnl_pct_gross_price" not in d.columns:
        d["pnl_pct_gross_price"] = pd.to_numeric(
            d.get("pnl_pct_gross", d["pnl_pct_price"]), errors="coerce"
        ).fillna(0.0)

    side_u = d["side"].astype(str).str.upper() if "side" in d.columns else pd.Series([""] * len(d))

    # Position size
    if "position_size_rs" not in d.columns:
        d["position_size_rs"] = np.nan
    d.loc[side_u.eq("SHORT") & d["position_size_rs"].isna(), "position_size_rs"] = float(POSITION_SIZE_RS_SHORT)
    d.loc[~side_u.eq("SHORT") & d["position_size_rs"].isna(), "position_size_rs"] = float(POSITION_SIZE_RS_LONG)
    d["position_size_rs"] = pd.to_numeric(d["position_size_rs"], errors="coerce").fillna(0.0)

    # Leverage
    if "leverage" not in d.columns:
        d["leverage"] = np.nan
    d.loc[side_u.eq("SHORT") & d["leverage"].isna(), "leverage"] = float(INTRADAY_LEVERAGE_SHORT)
    d.loc[~side_u.eq("SHORT") & d["leverage"].isna(), "leverage"] = float(INTRADAY_LEVERAGE_LONG)
    d["leverage"] = pd.to_numeric(d["leverage"], errors="coerce").fillna(1.0)

    # Notional exposure
    d["notional_exposure_rs"] = d["position_size_rs"] * d["leverage"]

    # Capital ROI (levered)
    d["pnl_pct"]       = d["pnl_pct_price"] * d["leverage"]
    d["pnl_pct_gross"] = d["pnl_pct_gross_price"] * d["leverage"]

    # Rupee P&L
    d["pnl_rs"]       = (d["pnl_pct_price"] / 100.0) * d["notional_exposure_rs"]
    d["pnl_rs_gross"] = (d["pnl_pct_gross_price"] / 100.0) * d["notional_exposure_rs"]

    return d


def _print_notional_summary(label: str, df: pd.DataFrame) -> None:
    if df.empty or "pnl_rs" not in df.columns:
        print(f"[NOTIONAL] {label:<8} | trades=0")
        return

    pnl_rs = float(pd.to_numeric(df["pnl_rs"], errors="coerce").fillna(0.0).sum())
    pnl_rs_gross = float(
        pd.to_numeric(df.get("pnl_rs_gross", 0.0), errors="coerce").fillna(0.0).sum()
    )
    notional = float(
        pd.to_numeric(df.get("notional_exposure_rs", 0.0), errors="coerce").fillna(0.0).sum()
    )
    trades = int(len(df))
    avg_trade_rs = pnl_rs / trades if trades else 0.0
    roi_notional_pct = (pnl_rs / notional * 100.0) if notional > 0 else 0.0

    print(
        f"[NOTIONAL] {label:<8} | trades={trades:<5} | pnl_net=Rs.{pnl_rs:,.0f} "
        f"| pnl_gross=Rs.{pnl_rs_gross:,.0f} | notional=Rs.{notional:,.0f} "
        f"| roi_on_notional={roi_notional_pct:.3f}% | avg/trade=Rs.{avg_trade_rs:,.0f}"
    )


# ===========================================================================
# CASH-CONSTRAINED PORTFOLIO SIMULATION
# ===========================================================================
def _simulate_cash_constrained(
    df: pd.DataFrame,
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    Walk through trades in chronological order.
    Only take a trade if capital is available; mark skipped ones.
    Uses itertuples for speed.
    """
    empty_stats = {
        "start_capital": PORTFOLIO_START_CAPITAL_RS,
        "taken": 0,
        "skipped": 0,
        "net_pnl_rs": 0.0,
        "final_equity": float(PORTFOLIO_START_CAPITAL_RS),
        "roi_pct": 0.0,
        "max_concurrent": 0,
        "min_cash": float(PORTFOLIO_START_CAPITAL_RS),
    }

    if df.empty:
        return df, empty_stats

    d = df.copy()
    for c in ["entry_time_ist", "exit_time_ist"]:
        if c in d.columns:
            d[c] = pd.to_datetime(d[c], errors="coerce")

    d = d.sort_values(
        ["entry_time_ist", "exit_time_ist", "ticker", "side"]
    ).reset_index(drop=True)

    cash = float(PORTFOLIO_START_CAPITAL_RS)
    open_heap: list = []      # (exit_time, size, pnl_rs)
    seen_ticker_day: set = set()

    taken_flags    = np.zeros(len(d), dtype=bool)
    cash_before    = np.zeros(len(d))
    cash_after     = np.zeros(len(d))
    pos_sizes      = np.zeros(len(d))
    pnl_rs_sim     = np.zeros(len(d))

    min_cash = cash
    max_concurrent = 0

    for row in d.itertuples(index=True):
        idx = row.Index
        entry_t = row.entry_time_ist
        exit_t  = row.exit_time_ist

        # Release matured positions
        while open_heap and open_heap[0][0] <= entry_t:
            _, size, pnl = heapq.heappop(open_heap)
            cash += size + pnl

        cash_before[idx] = cash

        tkey = (getattr(row, "trade_date", None), getattr(row, "ticker", None), getattr(row, "side", None))
        if tkey[0] is not None and tkey in seen_ticker_day:
            continue

        size = float(getattr(row, "position_size_rs", POSITION_SIZE_RS_LONG) or POSITION_SIZE_RS_LONG)
        pnl  = float(getattr(row, "pnl_rs", 0.0) or 0.0)

        if cash >= size:
            cash -= size
            if min_cash > cash:
                min_cash = cash
            heapq.heappush(open_heap, (exit_t, size, pnl))
            taken_flags[idx] = True
            pos_sizes[idx]   = size
            pnl_rs_sim[idx]  = pnl
            cash_after[idx]  = cash
            max_concurrent = max(max_concurrent, len(open_heap))
            if tkey[0] is not None:
                seen_ticker_day.add(tkey)

    # Release all remaining positions
    while open_heap:
        _, size, pnl = heapq.heappop(open_heap)
        cash += size + pnl

    d["sim_taken"]       = taken_flags
    d["sim_cash_before"] = cash_before
    d["sim_pnl_rs"]      = pnl_rs_sim
    d["sim_position_rs"] = pos_sizes

    taken_df  = d[d["sim_taken"]].copy()
    net_pnl   = float(taken_df["sim_pnl_rs"].sum()) if not taken_df.empty else 0.0
    final_eq  = float(PORTFOLIO_START_CAPITAL_RS) + net_pnl
    roi_pct   = net_pnl / float(PORTFOLIO_START_CAPITAL_RS) * 100.0

    stats = {
        "start_capital": PORTFOLIO_START_CAPITAL_RS,
        "taken": int(taken_flags.sum()),
        "skipped": int((~taken_flags).sum()),
        "net_pnl_rs": net_pnl,
        "final_equity": final_eq,
        "roi_pct": roi_pct,
        "max_concurrent": max_concurrent,
        "min_cash": min_cash,
    }
    return d, stats


# ===========================================================================
# CHART GENERATION
# ===========================================================================
def generate_eqidv5_charts(
    df_combined: pd.DataFrame,
    df_long: pd.DataFrame,
    df_short: pd.DataFrame,
    save_dir: Path,
    ts_label: str = "",
) -> List[str]:
    """
    Generate 20 comprehensive backtest analysis charts.
    Returns list of saved file paths.
    """
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        import matplotlib.dates as mdates
        from matplotlib.gridspec import GridSpec
    except ImportError:
        print("[WARN] matplotlib not installed -- skipping charts.")
        return []

    save_dir.mkdir(parents=True, exist_ok=True)
    saved: List[str] = []

    if df_combined.empty:
        print("[WARN] No combined trades data -- skipping charts.")
        return saved

    outcome_colors = {
        "TARGET": "#2ecc71",
        "SL": "#e74c3c",
        "BE": "#f39c12",
        "EOD": "#3498db",
    }

    def _prep(d: pd.DataFrame) -> pd.DataFrame:
        d = d.copy()
        d["pnl_pct"] = pd.to_numeric(d["pnl_pct"], errors="coerce").fillna(0.0)
        d["trade_date"] = pd.to_datetime(d["trade_date"], errors="coerce")
        for c in ["entry_time_ist", "exit_time_ist", "signal_time_ist"]:
            if c in d.columns:
                d[c] = pd.to_datetime(d[c], errors="coerce")
        d = d.sort_values("trade_date").reset_index(drop=True)
        return d

    d   = _prep(df_combined)
    d_l = _prep(df_long)  if not df_long.empty  else pd.DataFrame()
    d_s = _prep(df_short) if not df_short.empty else pd.DataFrame()

    # ------------------------------------------------------------------
    # 1. Equity Curve (combined + sides)
    # ------------------------------------------------------------------
    fig, ax = plt.subplots(figsize=(14, 6))
    cum_all = d["pnl_pct"].cumsum()
    ax.plot(range(len(cum_all)), cum_all.values, label="Combined", color="black", lw=2)
    if not d_l.empty:
        ax.plot(range(len(d_l)), d_l["pnl_pct"].cumsum().values, label="LONG", color="green", alpha=0.75)
    if not d_s.empty:
        ax.plot(range(len(d_s)), d_s["pnl_pct"].cumsum().values, label="SHORT", color="red", alpha=0.75)
    ax.axhline(y=0, color="gray", ls="--", alpha=0.5)
    ax.set_xlabel("Trade #"); ax.set_ylabel("Cumulative PnL (%)")
    ax.set_title("Equity Curve -- Cumulative Net PnL %")
    ax.legend(); ax.grid(True, alpha=0.3)
    p = save_dir / f"01_equity_curve_{ts_label}.png"
    fig.savefig(p, dpi=150, bbox_inches="tight"); plt.close(fig); saved.append(str(p))

    # ------------------------------------------------------------------
    # 2. Daily PnL Bar Chart
    # ------------------------------------------------------------------
    fig, ax = plt.subplots(figsize=(14, 5))
    daily = d.groupby("trade_date")["pnl_pct"].sum()
    cols = ["green" if v >= 0 else "red" for v in daily.values]
    ax.bar(range(len(daily)), daily.values, color=cols, alpha=0.75, width=0.8)
    ax.axhline(y=0, color="black", lw=0.8)
    ax.set_xlabel("Trading Day (index)"); ax.set_ylabel("Daily Net PnL (%)")
    ax.set_title("Daily PnL -- Sum of All Trades per Day")
    ax.grid(True, alpha=0.3, axis="y")
    p = save_dir / f"02_daily_pnl_{ts_label}.png"
    fig.savefig(p, dpi=150, bbox_inches="tight"); plt.close(fig); saved.append(str(p))

    # ------------------------------------------------------------------
    # 3. Outcome Distribution (3 pies: combined, long, short)
    # ------------------------------------------------------------------
    if "outcome" in d.columns:
        fig, axes = plt.subplots(1, 3, figsize=(16, 5))
        for ax, (title, sub) in zip(axes, [("Combined", d), ("LONG", d_l), ("SHORT", d_s)]):
            if isinstance(sub, pd.DataFrame) and not sub.empty and "outcome" in sub.columns:
                counts = sub["outcome"].value_counts()
                labels = [f"{k}\n({v}, {v/len(sub)*100:.1f}%)" for k, v in counts.items()]
                cols = [outcome_colors.get(k, "gray") for k in counts.index]
                ax.pie(counts.values, labels=labels, colors=cols, startangle=90)
                ax.set_title(f"{title} Outcomes (n={len(sub)})")
            else:
                ax.set_title(f"{title} (no data)")
        fig.suptitle("Trade Outcome Distribution", fontsize=14, fontweight="bold")
        fig.tight_layout()
        p = save_dir / f"03_outcome_distribution_{ts_label}.png"
        fig.savefig(p, dpi=150, bbox_inches="tight"); plt.close(fig); saved.append(str(p))

    # ------------------------------------------------------------------
    # 4. PnL Distribution Histogram
    # ------------------------------------------------------------------
    fig, ax = plt.subplots(figsize=(12, 5))
    pnl_vals = d["pnl_pct"].values
    ax.hist(pnl_vals, bins=50, color="steelblue", edgecolor="white", alpha=0.8)
    ax.axvline(x=0, color="red", ls="--", lw=1.5, label="Breakeven")
    ax.axvline(x=np.mean(pnl_vals), color="green", lw=1.5, label=f"Mean={np.mean(pnl_vals):.3f}%")
    ax.set_xlabel("PnL per Trade (%)"); ax.set_ylabel("Frequency")
    ax.set_title("PnL Distribution -- All Trades (net)")
    ax.legend(); ax.grid(True, alpha=0.3, axis="y")
    p = save_dir / f"04_pnl_distribution_{ts_label}.png"
    fig.savefig(p, dpi=150, bbox_inches="tight"); plt.close(fig); saved.append(str(p))

    # ------------------------------------------------------------------
    # 5. Trades per Day
    # ------------------------------------------------------------------
    fig, ax = plt.subplots(figsize=(10, 5))
    tpd = d.groupby("trade_date").size()
    ax.hist(tpd.values, bins=range(0, int(tpd.max()) + 2), color="teal",
            edgecolor="white", alpha=0.8, align="left")
    ax.axvline(x=tpd.mean(), color="red", ls="--", label=f"Avg={tpd.mean():.1f}/day")
    ax.set_xlabel("Trades per Day"); ax.set_ylabel("Number of Days")
    ax.set_title("Trade Frequency Distribution"); ax.legend()
    ax.grid(True, alpha=0.3, axis="y")
    p = save_dir / f"05_trades_per_day_{ts_label}.png"
    fig.savefig(p, dpi=150, bbox_inches="tight"); plt.close(fig); saved.append(str(p))

    # ------------------------------------------------------------------
    # 6. Drawdown Chart
    # ------------------------------------------------------------------
    fig, ax = plt.subplots(figsize=(14, 5))
    cum = d["pnl_pct"].cumsum().values
    run_max = np.maximum.accumulate(cum)
    dd = run_max - cum
    ax.fill_between(range(len(dd)), dd, color="red", alpha=0.3)
    ax.plot(range(len(dd)), dd, color="red", lw=0.8)
    ax.set_xlabel("Trade #"); ax.set_ylabel("Drawdown (%)")
    ax.set_title("Drawdown from Peak"); ax.grid(True, alpha=0.3)
    ax.invert_yaxis()
    p = save_dir / f"06_drawdown_{ts_label}.png"
    fig.savefig(p, dpi=150, bbox_inches="tight"); plt.close(fig); saved.append(str(p))

    # ------------------------------------------------------------------
    # 7. Win Rate by Hour of Day
    # ------------------------------------------------------------------
    if "entry_time_ist" in d.columns:
        fig, ax = plt.subplots(figsize=(10, 5))
        d["entry_hour"] = d["entry_time_ist"].dt.hour
        hourly = d.groupby("entry_hour").agg(
            count=("pnl_pct", "size"),
            wins=("pnl_pct", lambda x: (x > 0).sum()),
        )
        hourly["win_rate"] = hourly["wins"] / hourly["count"] * 100
        ax2 = ax.twinx()
        ax.bar(hourly.index, hourly["count"], color="steelblue", alpha=0.6, label="Count")
        ax2.plot(hourly.index, hourly["win_rate"], "go-", lw=2, label="Win Rate %")
        ax.set_xlabel("Hour (IST)"); ax.set_ylabel("Trade Count")
        ax2.set_ylabel("Win Rate (%)")
        ax.set_title("Trade Activity & Win Rate by Hour")
        ax.set_xticks(hourly.index)
        lines1, labs1 = ax.get_legend_handles_labels()
        lines2, labs2 = ax2.get_legend_handles_labels()
        ax.legend(lines1 + lines2, labs1 + labs2, loc="upper right")
        ax.grid(True, alpha=0.3, axis="y")
        p = save_dir / f"07_hourly_winrate_{ts_label}.png"
        fig.savefig(p, dpi=150, bbox_inches="tight"); plt.close(fig); saved.append(str(p))

    # ------------------------------------------------------------------
    # 8. Monthly Returns Bar Chart
    # ------------------------------------------------------------------
    fig, ax = plt.subplots(figsize=(13, 4))
    d["year_month"] = d["trade_date"].dt.to_period("M")
    monthly = d.groupby("year_month")["pnl_pct"].sum()
    cols = ["green" if v >= 0 else "red" for v in monthly.values]
    ax.bar(range(len(monthly)), monthly.values, color=cols, alpha=0.75)
    ax.set_xticks(range(len(monthly)))
    ax.set_xticklabels([str(p) for p in monthly.index], rotation=45, ha="right", fontsize=8)
    ax.set_ylabel("Monthly Net PnL (%)"); ax.set_title("Monthly Return Summary")
    ax.axhline(y=0, color="black", lw=0.8); ax.grid(True, alpha=0.3, axis="y")
    p = save_dir / f"08_monthly_returns_{ts_label}.png"
    fig.savefig(p, dpi=150, bbox_inches="tight"); plt.close(fig); saved.append(str(p))

    # ------------------------------------------------------------------
    # 9. Quality Score vs PnL Scatter
    # ------------------------------------------------------------------
    if "quality_score" in d.columns:
        fig, ax = plt.subplots(figsize=(10, 6))
        qs = pd.to_numeric(d["quality_score"], errors="coerce")
        pnl = d["pnl_pct"]
        ax.scatter(qs, pnl, alpha=0.4, s=20, c=pnl.apply(lambda x: "green" if x > 0 else "red"))
        ax.axhline(y=0, color="gray", ls="--", alpha=0.5)
        ax.set_xlabel("Quality Score (0-10)"); ax.set_ylabel("Net PnL %")
        ax.set_title("Quality Score vs Trade PnL")
        ax.grid(True, alpha=0.3)
        # Rolling mean per score level
        bins = np.arange(0, 11)
        means = [pnl[(qs >= b) & (qs < b + 1)].mean() for b in bins]
        ax.step(bins, means, color="blue", lw=2, where="mid", label="Avg PnL per score")
        ax.legend()
        p = save_dir / f"09_quality_vs_pnl_{ts_label}.png"
        fig.savefig(p, dpi=150, bbox_inches="tight"); plt.close(fig); saved.append(str(p))

    # ------------------------------------------------------------------
    # 10. Volume Ratio vs PnL
    # ------------------------------------------------------------------
    if "volume_ratio" in d.columns:
        fig, ax = plt.subplots(figsize=(10, 5))
        vr = pd.to_numeric(d["volume_ratio"], errors="coerce").clip(upper=6)
        pnl = d["pnl_pct"]
        ax.scatter(vr, pnl, alpha=0.35, s=15, c=pnl.apply(lambda x: "green" if x > 0 else "red"))
        ax.axhline(y=0, color="gray", ls="--", alpha=0.5)
        ax.set_xlabel("Volume Ratio (x SMA20, clipped at 6x)"); ax.set_ylabel("Net PnL %")
        ax.set_title("Volume Spike Ratio vs Trade PnL")
        ax.grid(True, alpha=0.3)
        p = save_dir / f"10_volume_ratio_vs_pnl_{ts_label}.png"
        fig.savefig(p, dpi=150, bbox_inches="tight"); plt.close(fig); saved.append(str(p))

    # ------------------------------------------------------------------
    # 11. Wick / ATR Ratio vs Outcome
    # ------------------------------------------------------------------
    if "wick_atr_ratio" in d.columns and "outcome" in d.columns:
        fig, ax = plt.subplots(figsize=(10, 5))
        for outcome, grp in d.groupby("outcome"):
            ax.hist(
                pd.to_numeric(grp["wick_atr_ratio"], errors="coerce").dropna().clip(upper=4),
                bins=30,
                alpha=0.5,
                label=outcome,
                color=outcome_colors.get(outcome, "gray"),
            )
        ax.set_xlabel("Sweep Wick Size (ATR units)"); ax.set_ylabel("Count")
        ax.set_title("Sweep Wick Size Distribution by Outcome")
        ax.legend(); ax.grid(True, alpha=0.3, axis="y")
        p = save_dir / f"11_wick_atr_by_outcome_{ts_label}.png"
        fig.savefig(p, dpi=150, bbox_inches="tight"); plt.close(fig); saved.append(str(p))

    # ------------------------------------------------------------------
    # 12. Near-POC vs Near-VAL vs Other Performance
    # ------------------------------------------------------------------
    if "prior_poc" in d.columns and "entry_price" in d.columns and "prior_val" in d.columns:
        fig, ax = plt.subplots(figsize=(9, 5))
        atr_proxy = pd.to_numeric(d.get("wick_atr_ratio", pd.Series(1.0, index=d.index)), errors="coerce").fillna(1.0) * 10
        poc_dist = (pd.to_numeric(d["entry_price"], errors="coerce") - pd.to_numeric(d["prior_poc"], errors="coerce")).abs()
        val_dist = (pd.to_numeric(d["entry_price"], errors="coerce") - pd.to_numeric(d["prior_val"], errors="coerce")).abs()
        near_poc = poc_dist < atr_proxy
        near_val = val_dist < atr_proxy

        categories = ["Near POC", "Near VAL", "Neither"]
        means = [
            d.loc[near_poc, "pnl_pct"].mean() if near_poc.any() else 0.0,
            d.loc[near_val & ~near_poc, "pnl_pct"].mean() if (near_val & ~near_poc).any() else 0.0,
            d.loc[~near_poc & ~near_val, "pnl_pct"].mean() if (~near_poc & ~near_val).any() else 0.0,
        ]
        counts = [near_poc.sum(), (near_val & ~near_poc).sum(), (~near_poc & ~near_val).sum()]
        bar_colors = ["green" if m > 0 else "red" for m in means]
        bars = ax.bar(categories, means, color=bar_colors, alpha=0.75)
        for bar, cnt in zip(bars, counts):
            ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.005,
                    f"n={cnt}", ha="center", fontsize=9)
        ax.axhline(y=0, color="black", lw=0.8)
        ax.set_ylabel("Avg Net PnL %"); ax.set_title("Avg PnL: Near-POC vs Near-VAL vs Neither")
        ax.grid(True, alpha=0.3, axis="y")
        p = save_dir / f"12_poc_val_performance_{ts_label}.png"
        fig.savefig(p, dpi=150, bbox_inches="tight"); plt.close(fig); saved.append(str(p))

    # ------------------------------------------------------------------
    # 13. R:R Distribution (realised reward vs planned risk)
    # ------------------------------------------------------------------
    if {"entry_price", "sl_price", "target_price"}.issubset(d.columns):
        fig, ax = plt.subplots(figsize=(10, 5))
        ep  = pd.to_numeric(d["entry_price"],  errors="coerce")
        sl  = pd.to_numeric(d["sl_price"],     errors="coerce")
        tgt = pd.to_numeric(d["target_price"], errors="coerce")
        side = d["side"].astype(str).str.upper() if "side" in d.columns else pd.Series(["LONG"] * len(d))
        risk   = np.where(side.eq("SHORT"), sl - ep, ep - sl)
        reward = np.where(side.eq("SHORT"), ep - tgt, tgt - ep)
        rr = np.where(risk > 0, reward / risk, np.nan)
        rr_series = pd.Series(rr).dropna().clip(0, 10)
        ax.hist(rr_series, bins=40, color="purple", edgecolor="white", alpha=0.8)
        ax.axvline(x=2.0, color="red", ls="--", lw=1.5, label="Min R:R = 2.0")
        ax.set_xlabel("Planned R:R Ratio"); ax.set_ylabel("Count")
        ax.set_title("Planned R:R Distribution (at Entry)")
        ax.legend(); ax.grid(True, alpha=0.3, axis="y")
        p = save_dir / f"13_rr_distribution_{ts_label}.png"
        fig.savefig(p, dpi=150, bbox_inches="tight"); plt.close(fig); saved.append(str(p))

    # ------------------------------------------------------------------
    # 14. Trade Duration Distribution
    # ------------------------------------------------------------------
    if {"entry_time_ist", "exit_time_ist"}.issubset(d.columns):
        fig, ax = plt.subplots(figsize=(10, 5))
        dur = (d["exit_time_ist"] - d["entry_time_ist"]).dt.total_seconds() / 60.0
        dur = dur.dropna().clip(0, 400)
        ax.hist(dur, bins=40, color="darkcyan", edgecolor="white", alpha=0.8)
        ax.axvline(x=dur.mean(), color="red", ls="--", label=f"Mean={dur.mean():.0f} min")
        ax.set_xlabel("Trade Duration (minutes)"); ax.set_ylabel("Count")
        ax.set_title("Trade Duration Distribution")
        ax.legend(); ax.grid(True, alpha=0.3, axis="y")
        p = save_dir / f"14_duration_distribution_{ts_label}.png"
        fig.savefig(p, dpi=150, bbox_inches="tight"); plt.close(fig); saved.append(str(p))

    # ------------------------------------------------------------------
    # 15. Weekly Returns Heatmap
    # ------------------------------------------------------------------
    if len(d) > 10:
        fig, ax = plt.subplots(figsize=(12, 4))
        d["week"] = d["trade_date"].dt.to_period("W")
        weekly = d.groupby("week")["pnl_pct"].sum()
        cols = ["green" if v >= 0 else "red" for v in weekly.values]
        ax.bar(range(len(weekly)), weekly.values, color=cols, alpha=0.75)
        ax.axhline(y=0, color="black", lw=0.8)
        ax.set_xticks(range(0, len(weekly), max(1, len(weekly) // 12)))
        ax.set_xticklabels(
            [str(weekly.index[i]) for i in range(0, len(weekly), max(1, len(weekly) // 12))],
            rotation=45, ha="right", fontsize=7,
        )
        ax.set_ylabel("Weekly Net PnL (%)"); ax.set_title("Weekly Returns")
        ax.grid(True, alpha=0.3, axis="y")
        p = save_dir / f"15_weekly_returns_{ts_label}.png"
        fig.savefig(p, dpi=150, bbox_inches="tight"); plt.close(fig); saved.append(str(p))

    # ------------------------------------------------------------------
    # 16. LONG vs SHORT Side-by-Side Metrics
    # ------------------------------------------------------------------
    if not d_l.empty or not d_s.empty:
        fig, axes = plt.subplots(1, 3, figsize=(15, 5))
        # Avg PnL %
        sides_data = []
        if not d_l.empty:
            sides_data.append(("LONG",  float(d_l["pnl_pct"].mean()), len(d_l),  "#27ae60"))
        if not d_s.empty:
            sides_data.append(("SHORT", float(d_s["pnl_pct"].mean()), len(d_s), "#e74c3c"))

        side_names  = [x[0] for x in sides_data]
        side_avgpnl = [x[1] for x in sides_data]
        side_counts = [x[2] for x in sides_data]
        side_colors = [x[3] for x in sides_data]

        axes[0].bar(side_names, side_avgpnl, color=side_colors, alpha=0.8)
        axes[0].axhline(y=0, color="black", lw=0.8)
        axes[0].set_title("Avg Net PnL %"); axes[0].set_ylabel("Avg PnL %")

        axes[1].bar(side_names, side_counts, color=side_colors, alpha=0.8)
        axes[1].set_title("Total Trade Count"); axes[1].set_ylabel("# Trades")

        # Hit rates
        if side_names:
            hit_rates = []
            for nm in side_names:
                sub = d_l if nm == "LONG" else d_s
                if "outcome" in sub.columns and len(sub):
                    hr = (sub["outcome"] == "TARGET").sum() / len(sub) * 100
                else:
                    hr = 0.0
                hit_rates.append(hr)
            axes[2].bar(side_names, hit_rates, color=side_colors, alpha=0.8)
            axes[2].set_title("Hit Rate %"); axes[2].set_ylabel("Hit Rate %")
            axes[2].set_ylim(0, 100)

        fig.suptitle("LONG vs SHORT Performance Comparison", fontsize=13, fontweight="bold")
        fig.tight_layout()
        p = save_dir / f"16_long_vs_short_{ts_label}.png"
        fig.savefig(p, dpi=150, bbox_inches="tight"); plt.close(fig); saved.append(str(p))

    # ------------------------------------------------------------------
    # 17. Entry Time Distribution (heatmap style)
    # ------------------------------------------------------------------
    if "entry_time_ist" in d.columns:
        fig, ax = plt.subplots(figsize=(12, 5))
        mins = d["entry_time_ist"].dt.hour * 60 + d["entry_time_ist"].dt.minute
        ax.hist(mins.dropna(), bins=range(555, 935, 15), color="steelblue",
                edgecolor="white", alpha=0.8)
        ax.set_xlabel("Entry Time (minutes from midnight IST)")
        ax.set_ylabel("# Trades")
        ax.set_title("Entry Time Distribution (15-min buckets)")

        tick_mins = list(range(555, 936, 60))
        ax.set_xticks(tick_mins)
        ax.set_xticklabels([f"{m//60:02d}:{m%60:02d}" for m in tick_mins], rotation=45)
        ax.grid(True, alpha=0.3, axis="y")
        p = save_dir / f"17_entry_time_distribution_{ts_label}.png"
        fig.savefig(p, dpi=150, bbox_inches="tight"); plt.close(fig); saved.append(str(p))

    # ------------------------------------------------------------------
    # 18. Top-20 Tickers by Trade Count + Avg PnL
    # ------------------------------------------------------------------
    if "ticker" in d.columns:
        fig, axes = plt.subplots(1, 2, figsize=(16, 6))
        ticker_stats = d.groupby("ticker").agg(
            count=("pnl_pct", "size"),
            avg_pnl=("pnl_pct", "mean"),
        ).sort_values("count", ascending=False).head(20)

        axes[0].barh(ticker_stats.index[::-1], ticker_stats["count"][::-1], color="steelblue", alpha=0.8)
        axes[0].set_xlabel("# Trades"); axes[0].set_title("Top 20 Tickers by Trade Count")

        colors_t = ["green" if v >= 0 else "red" for v in ticker_stats["avg_pnl"][::-1]]
        axes[1].barh(ticker_stats.index[::-1], ticker_stats["avg_pnl"][::-1], color=colors_t, alpha=0.8)
        axes[1].axvline(x=0, color="black", lw=0.8)
        axes[1].set_xlabel("Avg Net PnL %"); axes[1].set_title("Avg PnL % for Top 20 Tickers")
        fig.tight_layout()
        p = save_dir / f"18_top_tickers_{ts_label}.png"
        fig.savefig(p, dpi=150, bbox_inches="tight"); plt.close(fig); saved.append(str(p))

    # ------------------------------------------------------------------
    # 19. Consecutive Win/Loss Run-Length Chart
    # ------------------------------------------------------------------
    if len(d) > 5:
        fig, ax = plt.subplots(figsize=(14, 4))
        pnl_vals = d["pnl_pct"].values
        run_lens = []
        run_sign = []
        cur_sign = 1 if pnl_vals[0] >= 0 else -1
        cur_len = 1
        for v in pnl_vals[1:]:
            s = 1 if v >= 0 else -1
            if s == cur_sign:
                cur_len += 1
            else:
                run_lens.append(cur_len * cur_sign)
                run_sign.append(cur_sign)
                cur_sign = s
                cur_len = 1
        run_lens.append(cur_len * cur_sign)
        colors_r = ["green" if v > 0 else "red" for v in run_lens]
        ax.bar(range(len(run_lens)), run_lens, color=colors_r, alpha=0.75, width=0.8)
        ax.axhline(y=0, color="black", lw=0.8)
        ax.set_xlabel("Streak #"); ax.set_ylabel("Run Length (+ = wins, - = losses)")
        ax.set_title("Win/Loss Streak Lengths")
        ax.grid(True, alpha=0.3, axis="y")
        p = save_dir / f"19_streak_analysis_{ts_label}.png"
        fig.savefig(p, dpi=150, bbox_inches="tight"); plt.close(fig); saved.append(str(p))

    # ------------------------------------------------------------------
    # 20. PnL by Ticker (top + bottom 15 performers)
    # ------------------------------------------------------------------
    if "ticker" in d.columns and len(d["ticker"].unique()) > 5:
        fig, axes = plt.subplots(1, 2, figsize=(18, 7))
        ticker_sum = d.groupby("ticker")["pnl_pct"].sum().sort_values()

        top15 = ticker_sum.tail(15)
        bot15 = ticker_sum.head(15)

        axes[0].barh(bot15.index, bot15.values, color="red", alpha=0.75)
        axes[0].axvline(x=0, color="black", lw=0.8)
        axes[0].set_xlabel("Total Net PnL %"); axes[0].set_title("Bottom 15 Tickers (Total PnL)")
        axes[0].grid(True, alpha=0.3, axis="x")

        axes[1].barh(top15.index, top15.values, color="green", alpha=0.75)
        axes[1].axvline(x=0, color="black", lw=0.8)
        axes[1].set_xlabel("Total Net PnL %"); axes[1].set_title("Top 15 Tickers (Total PnL)")
        axes[1].grid(True, alpha=0.3, axis="x")

        fig.tight_layout()
        p = save_dir / f"20_ticker_pnl_ranking_{ts_label}.png"
        fig.savefig(p, dpi=150, bbox_inches="tight"); plt.close(fig); saved.append(str(p))

    print(f"[CHARTS] Saved {len(saved)} charts to {save_dir}")
    return saved


# ===========================================================================
# SORT HELPER
# ===========================================================================
def _sort_trades(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    d = df.copy()
    if "trade_date" in d.columns:
        d["trade_date"] = pd.to_datetime(d["trade_date"], errors="coerce")
    for c in ["signal_time_ist", "entry_time_ist", "exit_time_ist"]:
        if c in d.columns:
            d[c] = pd.to_datetime(d[c], errors="coerce")
    sort_cols = [c for c in ["trade_date", "entry_time_ist", "ticker", "side"] if c in d.columns]
    if sort_cols:
        d = d.sort_values(sort_cols).reset_index(drop=True)
    return d


# ===========================================================================
# MAIN
# ===========================================================================
def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(
        description="eqidv5 Combined Runner: VP + AVWAP + Liquidity Sweep Backtester"
    )
    parser.add_argument(
        "--dir15m", default="stocks_indicators_15min_eq",
        help="Directory containing 15-min parquet files (default: stocks_indicators_15min_eq)"
    )
    parser.add_argument(
        "--workers", type=int, default=MAX_WORKERS,
        help=f"Parallel workers (default: {MAX_WORKERS})"
    )
    parser.add_argument(
        "--max_tickers", type=int, default=0,
        help="Limit number of tickers (0 = all, useful for quick test)"
    )
    parser.add_argument(
        "--min_score", type=int, default=None,
        help="Minimum quality score override (default: use strategy default)"
    )
    parser.add_argument(
        "--min_rr", type=float, default=None,
        help="Minimum reward:risk ratio override (default: use strategy default)"
    )
    parser.add_argument(
        "--vol_ratio", type=float, default=None,
        help="Minimum volume spike ratio override (default: use strategy default)"
    )
    parser.add_argument("--adx_min", type=float, default=None, help="ADX minimum override")
    parser.add_argument("--swing_lookback", type=int, default=None, help="Swing lookback bars override")
    parser.add_argument("--no_require_5m", action="store_true", help="Disable mandatory 5-min confirmation")
    parser.add_argument("--no_require_adx", action="store_true", help="Disable mandatory ADX filter")
    parser.add_argument("--no_require_ema", action="store_true", help="Disable mandatory EMA alignment filter")
    parser.add_argument("--no_require_rsi", action="store_true", help="Disable mandatory RSI filter")
    parser.add_argument("--no_require_vp", action="store_true", help="Disable mandatory VP confluence filter")
    parser.add_argument("--avwap_slope", action="store_true", help="Enable AVWAP slope direction filter")
    parser.add_argument("--close_quality", action="store_true", help="Enable close quality filter (top/bottom X% of bar)")
    parser.add_argument("--wick_mult", type=float, default=None, help="Minimum sweep wick as multiple of ATR (default 0.35)")
    parser.add_argument("--close_quality_pct", type=float, default=None, help="Close quality min pct (default 0.55)")
    parser.add_argument("--time_stop", type=int, default=None, help="Time stop in minutes (default 90)")
    args = parser.parse_args()

    # Resolve output directory
    outputs_dir = _this_dir / "outputs_eqidv5"
    outputs_dir.mkdir(parents=True, exist_ok=True)

    ts = now_ist().strftime("%Y%m%d_%H%M%S")

    # Tee stdout + stderr to log file
    log_path = outputs_dir / f"run_log_{ts}.txt"
    _orig_stdout = sys.stdout
    _orig_stderr = sys.stderr
    try:
        log_file = open(log_path, "w", encoding="utf-8", errors="replace")
    except Exception:
        log_file = None

    sys.stdout = _Tee(_orig_stdout, log_file)
    sys.stderr = _Tee(_orig_stderr, log_file)

    try:
        print("=" * 70)
        print("  eqidv5 Combined Runner -- VP + AVWAP + Liquidity Sweep")
        print(f"  Run timestamp : {ts}")
        print(f"  Dir 15m       : {args.dir15m}")
        print(f"  Workers       : {args.workers}")
        print(f"  Min quality   : {args.min_score if args.min_score is not None else 'DEFAULT'}")
        print(f"  Min R:R       : {args.min_rr if args.min_rr is not None else 'DEFAULT'}")
        print(f"  Vol ratio     : {(str(args.vol_ratio) + 'x SMA20') if args.vol_ratio is not None else 'DEFAULT'}")
        print("=" * 70)

        # ---- Build configs ----
        dir5m = _derive_5m_dir(args.dir15m)
        print(f"  Dir 5m        : {dir5m}")
        long_overrides = {"dir_15m": args.dir15m, "dir_5m": dir5m}
        short_overrides = {"dir_15m": args.dir15m, "dir_5m": dir5m}
        if args.min_score is not None:
            long_overrides["min_quality_score"] = args.min_score
            short_overrides["min_quality_score"] = args.min_score
        if args.min_rr is not None:
            long_overrides["min_rr"] = args.min_rr
            short_overrides["min_rr"] = args.min_rr
        if args.vol_ratio is not None:
            long_overrides["sweep_vol_ratio"] = args.vol_ratio
            short_overrides["sweep_vol_ratio"] = args.vol_ratio
        if args.adx_min is not None:
            long_overrides["adx_min"] = args.adx_min
            short_overrides["adx_min"] = args.adx_min
        if args.swing_lookback is not None:
            long_overrides["swing_lookback"] = args.swing_lookback
            short_overrides["swing_lookback"] = args.swing_lookback
        if args.no_require_5m:
            long_overrides["require_5m_confirmation"] = False
            short_overrides["require_5m_confirmation"] = False
        if args.no_require_adx:
            long_overrides["require_adx_filter"] = False
            short_overrides["require_adx_filter"] = False
        if args.no_require_ema:
            long_overrides["require_ema_alignment"] = False
            short_overrides["require_ema_alignment"] = False
        if args.no_require_rsi:
            long_overrides["require_rsi_filter"] = False
            short_overrides["require_rsi_filter"] = False
        if args.no_require_vp:
            long_overrides["require_vp_confluence"] = False
            short_overrides["require_vp_confluence"] = False
        if args.avwap_slope:
            long_overrides["require_avwap_slope"] = True
            short_overrides["require_avwap_slope"] = True
        if args.close_quality:
            long_overrides["require_close_quality"] = True
            short_overrides["require_close_quality"] = True
        if args.wick_mult is not None:
            long_overrides["sweep_min_wick_atr_mult"] = args.wick_mult
            short_overrides["sweep_min_wick_atr_mult"] = args.wick_mult
        if args.close_quality_pct is not None:
            long_overrides["close_quality_min_pct"] = args.close_quality_pct
            short_overrides["close_quality_min_pct"] = args.close_quality_pct
        if args.time_stop is not None:
            long_overrides["scalp_time_stop_minutes"] = args.time_stop
            short_overrides["scalp_time_stop_minutes"] = args.time_stop

        long_cfg = default_long_config(**long_overrides)
        short_cfg = default_short_config(**short_overrides)

        print(
            "  Active LONG cfg: "
            f"quality>={long_cfg.min_quality_score}, rr>={long_cfg.min_rr}, vol>={long_cfg.sweep_vol_ratio}x, "
            f"adx>={long_cfg.adx_min}, wick_atr>={long_cfg.sweep_min_wick_atr_mult}, "
            f"vp_mult={long_cfg.near_level_atr_mult}, atr%>={long_cfg.min_atr_pct}, "
            f"entry={long_cfg.entry_start.strftime('%H:%M')}-{long_cfg.entry_end.strftime('%H:%M')}, "
            f"5m_confirm={'ON' if long_cfg.use_5m_confirmation else 'OFF'}"
            f"(req={'Y' if long_cfg.require_5m_confirmation else 'N'}, win={long_cfg.confirm_window_min}m, "
            f"score>={long_cfg.confirm_min_score}), "
            f"time_stop={long_cfg.scalp_time_stop_minutes}m, "
            f"topn={'OFF' if not long_cfg.enable_topn_per_day else long_cfg.topn_per_day}, "
            f"max_ticker_day={'UNLIMITED' if long_cfg.max_trades_per_ticker_per_day <= 0 else long_cfg.max_trades_per_ticker_per_day}"
        )
        print(
            "  Active SHORT cfg: "
            f"quality>={short_cfg.min_quality_score}, rr>={short_cfg.min_rr}, vol>={short_cfg.sweep_vol_ratio}x, "
            f"adx>={short_cfg.adx_min}, wick_atr>={short_cfg.sweep_min_wick_atr_mult}, "
            f"vp_mult={short_cfg.near_level_atr_mult}, atr%>={short_cfg.min_atr_pct}, "
            f"entry={short_cfg.entry_start.strftime('%H:%M')}-{short_cfg.entry_end.strftime('%H:%M')}, "
            f"5m_confirm={'ON' if short_cfg.use_5m_confirmation else 'OFF'}"
            f"(req={'Y' if short_cfg.require_5m_confirmation else 'N'}, win={short_cfg.confirm_window_min}m, "
            f"score>={short_cfg.confirm_min_score}), "
            f"time_stop={short_cfg.scalp_time_stop_minutes}m, "
            f"topn={'OFF' if not short_cfg.enable_topn_per_day else short_cfg.topn_per_day}, "
            f"max_ticker_day={'UNLIMITED' if short_cfg.max_trades_per_ticker_per_day <= 0 else short_cfg.max_trades_per_ticker_per_day}"
        )

        max_tickers = args.max_tickers if args.max_tickers > 0 else None

        # ---- LONG scan ----
        print("\n[LONG] Starting scan ...")
        df_long = _run_side_parallel(
            side="LONG",
            cfg=long_cfg,
            max_workers=args.workers,
            max_tickers=max_tickers,
        )

        # ---- SHORT scan ----
        print("\n[SHORT] Starting scan ...")
        df_short = _run_side_parallel(
            side="SHORT",
            cfg=short_cfg,
            max_workers=args.workers,
            max_tickers=max_tickers,
        )

        # ---- Combine ----
        parts = [p for p in [df_long, df_short] if not p.empty]
        if parts:
            df_combined = pd.concat(parts, ignore_index=True)
            df_combined = _sort_trades(df_combined)
        else:
            df_combined = pd.DataFrame()
            print("[WARN] No trades found on either side.")

        # ---- Notional P&L ----
        df_long     = _add_notional_pnl(df_long)     if not df_long.empty     else df_long
        df_short    = _add_notional_pnl(df_short)    if not df_short.empty    else df_short
        df_combined = _add_notional_pnl(df_combined) if not df_combined.empty else df_combined

        # ---- Metrics ----
        print("\n")
        m_long  = compute_backtest_metrics(df_long)
        m_short = compute_backtest_metrics(df_short)
        m_combined = compute_backtest_metrics(df_combined)

        print_metrics("LONG  Strategy", m_long)
        print_metrics("SHORT Strategy", m_short)
        print_metrics("COMBINED (L+S)", m_combined)

        print("\n[NOTIONAL] P&L summary (levered exposure):")
        _print_notional_summary("LONG", df_long)
        _print_notional_summary("SHORT", df_short)
        _print_notional_summary("COMBINED", df_combined)

        if not df_combined.empty and "pnl_rs" in df_combined.columns:
            total_pnl_rs = float(df_combined["pnl_rs"].sum())
            print(f"\n[PORTFOLIO] Total P&L (Rs., levered) : Rs.{total_pnl_rs:,.0f}")
            print(f"[PORTFOLIO] Total trades              : {len(df_combined)}")
            print(f"[PORTFOLIO] Unique tickers            : {df_combined['ticker'].nunique() if 'ticker' in df_combined.columns else 'N/A'}")

        # ---- Cash-constrained sim ----
        if ENABLE_CASH_CONSTRAINED_PORTFOLIO_SIM and not df_combined.empty:
            print("\n[SIM] Cash-constrained portfolio simulation ...")
            df_combined, sim_stats = _simulate_cash_constrained(df_combined)
            print(f"[SIM] Start capital  : Rs.{sim_stats['start_capital']:,.0f}")
            print(f"[SIM] Trades taken   : {sim_stats['taken']}")
            print(f"[SIM] Trades skipped : {sim_stats['skipped']}")
            print(f"[SIM] Net PnL (Rs.)  : Rs.{sim_stats['net_pnl_rs']:,.0f}")
            print(f"[SIM] Final equity   : Rs.{sim_stats['final_equity']:,.0f}")
            print(f"[SIM] ROI %          : {sim_stats['roi_pct']:.2f}%")
            print(f"[SIM] Max concurrent : {sim_stats['max_concurrent']} positions")

        # ---- CSV exports ----
        csv_dir = outputs_dir / f"trades_{ts}"
        csv_dir.mkdir(parents=True, exist_ok=True)

        if not df_long.empty:
            f = csv_dir / "trades_long.csv"
            df_long.to_csv(f, index=False)
            print(f"\n[EXPORT] LONG trades  -> {f}")

        if not df_short.empty:
            f = csv_dir / "trades_short.csv"
            df_short.to_csv(f, index=False)
            print(f"[EXPORT] SHORT trades -> {f}")

        if not df_combined.empty:
            f = csv_dir / "trades_combined.csv"
            df_combined.to_csv(f, index=False)
            print(f"[EXPORT] Combined     -> {f}")

        # ---- Charts ----
        print("\n[CHARTS] Generating analysis charts ...")
        charts_dir = outputs_dir / f"charts_{ts}"
        saved_charts = generate_eqidv5_charts(
            df_combined=df_combined,
            df_long=df_long,
            df_short=df_short,
            save_dir=charts_dir,
            ts_label=ts,
        )
        if saved_charts:
            print(f"[CHARTS] {len(saved_charts)} charts -> {charts_dir}")

        print(f"\n[DONE] Log -> {log_path}")
        print("=" * 70)

    finally:
        sys.stdout = _orig_stdout
        sys.stderr = _orig_stderr
        if log_file:
            try:
                log_file.close()
            except Exception:
                pass


if __name__ == "__main__":
    main()
