# -*- coding: utf-8 -*-
from __future__ import annotations

import io
import sys
import time
from datetime import date, datetime, time as dtime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

if hasattr(sys.stdout, "buffer"):
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

HERE = Path(__file__).resolve().parent
if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))

import avwap_combined_runner_v19a_5min as runner

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------
OUTPUT_CANDIDATES = [
    Path(r"C:\TradingData\eqidv2\outputs_v19a_5min"),
    Path(r"C:\TradingData\eqidv2\outputs_v18c6_5min"),
]
DIR_1MIN = runner._resolve_1min_dir()
DIR_5MIN = runner._resolve_15m_dir()
EOD_TIME = runner.V15_EOD_EXIT_TIME
STOP_SLIP = float(getattr(runner, "STOP_EXIT_EXTRA_SLIPPAGE_BPS", 3.0)) / 10_000.0

# Deep but still practical grid.
SL_GRID = [x / 10_000.0 for x in range(45, 121, 5)]   # 0.45% -> 1.20%
TGT_GRID = [x / 10_000.0 for x in range(50, 201, 5)]  # 0.50% -> 2.00%

MIN_TRADES_FOR_MAIN_RECO = 25

W_TOTAL = 0.30
W_PF = 0.25
W_SHARPE = 0.20
W_DWR = 0.10
W_DD = 0.15

# ---------------------------------------------------------------------------
# HELPERS
# ---------------------------------------------------------------------------
def _find_latest_v19a_csv() -> Path:
    candidates: List[Path] = []
    for out_dir in OUTPUT_CANDIDATES:
        if not out_dir.is_dir():
            continue
        candidates.extend(sorted(out_dir.glob("*trades*v19a*.csv")))
    if not candidates:
        raise FileNotFoundError("No v19a trades CSV found in expected output directories.")
    return sorted(candidates)[-1]


def _to_naive_ist(series: pd.Series) -> pd.Series:
    s = pd.to_datetime(series, errors="coerce")
    if hasattr(s.dt, "tz") and s.dt.tz is not None:
        s = s.dt.tz_convert("Asia/Kolkata").dt.tz_localize(None)
    return s


def _compute_metrics(
    pnl_arr: np.ndarray,
    outcomes: List[str],
    trade_dates: List[date],
    notional_arr: Optional[np.ndarray] = None,
    price_pnl_arr: Optional[np.ndarray] = None,
) -> dict:
    n = len(pnl_arr)
    if n == 0:
        return {}

    wins = int((pnl_arr > 0).sum())
    losses = int((pnl_arr < 0).sum())
    flats = int((pnl_arr == 0).sum())

    gross_wins = float(pnl_arr[pnl_arr > 0].sum())
    gross_losses = float(abs(pnl_arr[pnl_arr < 0].sum()))
    pf = gross_wins / gross_losses if gross_losses > 0 else 999.0

    total_pct = float(pnl_arr.sum())
    avg_pct = float(pnl_arr.mean())
    wr = wins / n * 100.0

    day_pnl: Dict[date, float] = {}
    for i, td in enumerate(trade_dates):
        day_pnl[td] = day_pnl.get(td, 0.0) + float(pnl_arr[i])
    day_values = np.array(list(day_pnl.values()), dtype=np.float64)
    dwr = float((day_values > 0).sum() / len(day_values) * 100.0) if len(day_values) else 0.0
    sharpe = float(day_values.mean() / day_values.std() * np.sqrt(252)) if len(day_values) and day_values.std() > 1e-9 else 0.0

    cum = np.cumsum(pnl_arr)
    max_dd = float((cum - np.maximum.accumulate(cum)).min()) if len(cum) else 0.0

    if price_pnl_arr is not None and notional_arr is not None:
        total_rs = float(np.dot(notional_arr, price_pnl_arr / 100.0))
    else:
        total_rs = float("nan")

    return {
        "n": n,
        "wins": wins,
        "losses": losses,
        "flats": flats,
        "wr": wr,
        "pf": pf,
        "total_pct": total_pct,
        "avg_pct": avg_pct,
        "total_rs": total_rs,
        "sharpe": sharpe,
        "dwr": dwr,
        "max_dd": max_dd,
        "tgt_rate": outcomes.count("TARGET") / n * 100.0,
        "sl_rate": outcomes.count("SL") / n * 100.0,
        "eod_rate": outcomes.count("EOD") / n * 100.0,
        "days": len(day_pnl),
    }


def _add_balanced_score(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    total_max = max(df["total_pct"].quantile(0.95), 1e-9)
    pf_max = max(df["pf"].replace([np.inf], np.nan).quantile(0.95), 1e-9)
    sharpe_max = max(df["sharpe"].quantile(0.95), 1e-9)
    dwr_max = max(df["dwr"].max(), 1e-9)
    dd_worst = max(abs(df["max_dd"].min()), 1e-9)

    def _score(row: pd.Series) -> float:
        total_n = min(max(row["total_pct"], 0.0), total_max) / total_max
        pf_n = min(row["pf"], pf_max) / pf_max
        sharpe_n = min(max(row["sharpe"], 0.0), sharpe_max) / sharpe_max
        dwr_n = row["dwr"] / dwr_max
        dd_n = 1.0 - abs(row["max_dd"]) / dd_worst
        return (
            W_TOTAL * total_n
            + W_PF * pf_n
            + W_SHARPE * sharpe_n
            + W_DWR * dwr_n
            + W_DD * dd_n
        )

    df["balanced_score"] = df.apply(_score, axis=1)
    return df


def _pick_best(df: pd.DataFrame, sort_col: str, ascending: bool = False) -> pd.Series:
    return df.sort_values([sort_col, "total_pct", "pf", "wr"], ascending=[ascending, False, False, False]).iloc[0]


# ---------------------------------------------------------------------------
# LOAD CSV
# ---------------------------------------------------------------------------
print("=" * 88)
print("  V19A SETUP PARAMETER SWEEP  —  SL / TARGET / EOD ONLY")
print("=" * 88)

csv_path = _find_latest_v19a_csv()
print(f"\n[LOAD] {csv_path}")

raw = pd.read_csv(csv_path)
raw["trade_date"] = pd.to_datetime(raw["trade_date"], errors="coerce")
raw["entry_time_ist"] = _to_naive_ist(raw["entry_time_ist"])
raw["trade_date_d"] = raw["trade_date"].dt.date

slip_col = (
    pd.to_numeric(raw["slippage_pct"], errors="coerce").fillna(0.0005)
    if "slippage_pct" in raw.columns
    else pd.Series(0.0005, index=raw.index)
)
comm_col = (
    pd.to_numeric(raw["commission_pct"], errors="coerce").fillna(0.0003)
    if "commission_pct" in raw.columns
    else pd.Series(0.0003, index=raw.index)
)
raw["cost_rt"] = (slip_col + comm_col) * 2.0 * 100.0

if "notional_exposure_rs" in raw.columns:
    raw["notional_rs"] = pd.to_numeric(raw["notional_exposure_rs"], errors="coerce").fillna(100000.0)
else:
    raw["notional_rs"] = 100000.0

raw["side"] = raw["side"].astype(str).str.upper()
raw["setup"] = raw["setup"].astype(str)

print(f"[LOAD] Trades={len(raw)} | SHORT={(raw['side'] == 'SHORT').sum()} | LONG={(raw['side'] == 'LONG').sum()}")

side_setup_counts = raw.groupby(["side", "setup"]).size().sort_values(ascending=False)
print("[LOAD] Setup counts:")
for (side, setup), cnt in side_setup_counts.items():
    print(f"  - {side:<5} {setup:<32} {cnt:>4}")

# ---------------------------------------------------------------------------
# LOAD 1-MIN DATA
# ---------------------------------------------------------------------------
tickers = sorted(raw["ticker"].astype(str).unique())
print(f"\n[1MIN] Loading {len(tickers)} tickers from {DIR_1MIN} ...")
t0 = time.perf_counter()

bar_cache: Dict[str, pd.DataFrame] = {}
bar_cache_5m: Dict[str, pd.DataFrame] = {}
for tk in tickers:
    for fname in [
        f"{tk}.parquet",
        f"{tk}_1min.parquet",
        f"{tk}_stocks_indicators_1min.parquet",
    ]:
        fpath = DIR_1MIN / fname
        if not fpath.exists():
            continue
        try:
            df_b = pd.read_parquet(fpath, engine="pyarrow")
        except Exception:
            continue
        if df_b.empty:
            break

        df_b.columns = [c.lower() for c in df_b.columns]
        dt_col = None
        for cand in ("datetime", "date", "timestamp"):
            if cand in df_b.columns:
                dt_col = cand
                break
        if dt_col is None and isinstance(df_b.index, pd.DatetimeIndex):
            df_b = df_b.reset_index()
            dt_col = df_b.columns[0]
        if dt_col is None:
            break
        if dt_col != "datetime":
            df_b = df_b.rename(columns={dt_col: "datetime"})

        df_b["datetime"] = pd.to_datetime(df_b["datetime"], errors="coerce")
        if df_b["datetime"].dt.tz is not None:
            df_b["datetime"] = df_b["datetime"].dt.tz_convert("Asia/Kolkata").dt.tz_localize(None)

        needed = ["datetime"] + [c for c in ["high", "low", "close"] if c in df_b.columns]
        dropna_cols = ["datetime", "high", "low"] + (["close"] if "close" in df_b.columns else [])
        df_b = (
            df_b[needed]
            .dropna(subset=dropna_cols)
            .sort_values("datetime")
            .reset_index(drop=True)
        )

        bar_cache[tk] = df_b
        break

    if tk not in bar_cache:
        bar_cache[tk] = pd.DataFrame()

loaded_n = sum(1 for df_b in bar_cache.values() if not df_b.empty)
print(f"[1MIN] Loaded {loaded_n}/{len(tickers)} tickers in {time.perf_counter() - t0:.1f}s")


def _load_5m_bars(ticker: str) -> pd.DataFrame:
    if ticker in bar_cache_5m:
        return bar_cache_5m[ticker]

    fpath = DIR_5MIN / f"{ticker}_stocks_indicators_5min.parquet"
    if not fpath.exists():
        bar_cache_5m[ticker] = pd.DataFrame()
        return bar_cache_5m[ticker]

    try:
        df_b = runner.read_15m_parquet(str(fpath), engine="pyarrow")
    except Exception:
        bar_cache_5m[ticker] = pd.DataFrame()
        return bar_cache_5m[ticker]

    if df_b is None or df_b.empty:
        bar_cache_5m[ticker] = pd.DataFrame()
        return bar_cache_5m[ticker]

    cols = {c.lower(): c for c in df_b.columns}
    time_col = "date" if "date" in cols else ("datetime" if "datetime" in cols else None)
    if time_col is None:
        if isinstance(df_b.index, pd.DatetimeIndex):
            df_b = df_b.reset_index().rename(columns={df_b.reset_index().columns[0]: "date"})
            time_col = "date"
        else:
            bar_cache_5m[ticker] = pd.DataFrame()
            return bar_cache_5m[ticker]

    if time_col != "date":
        df_b = df_b.rename(columns={time_col: "date"})

    if "date" not in df_b.columns:
        bar_cache_5m[ticker] = pd.DataFrame()
        return bar_cache_5m[ticker]

    df_b["date"] = pd.to_datetime(df_b["date"], errors="coerce")
    if df_b["date"].dt.tz is not None:
        df_b["date"] = df_b["date"].dt.tz_convert("Asia/Kolkata").dt.tz_localize(None)

    rename_map = {}
    if "high" not in df_b.columns and "High" in df_b.columns:
        rename_map["High"] = "high"
    if "low" not in df_b.columns and "Low" in df_b.columns:
        rename_map["Low"] = "low"
    if "close" not in df_b.columns and "Close" in df_b.columns:
        rename_map["Close"] = "close"
    if rename_map:
        df_b = df_b.rename(columns=rename_map)

    keep = ["date"] + [c for c in ["high", "low", "close"] if c in df_b.columns]
    df_b = (
        df_b[keep]
        .dropna(subset=["date", "high", "low"] + (["close"] if "close" in df_b.columns else []))
        .sort_values("date")
        .reset_index(drop=True)
    )
    bar_cache_5m[ticker] = df_b
    return df_b


# ---------------------------------------------------------------------------
# BUILD TRADE BAR ARRAYS
# ---------------------------------------------------------------------------
def build_trade_bars(trades_df: pd.DataFrame, label: str) -> List[dict]:
    items: List[dict] = []
    no_bars = 0
    bars_1m_used = 0
    bars_5m_used = 0
    for row in trades_df.itertuples(index=False):
        ticker = str(row.ticker)
        entry_time = row.entry_time_ist
        trade_date = row.trade_date_d
        entry_price = float(row.entry_price)
        cost_pct = float(row.cost_rt)
        notional = float(row.notional_rs)
        leverage = float(getattr(row, "leverage", 5.0))
        side = str(row.side).upper()
        setup = str(row.setup)
        bars = bar_cache.get(ticker, pd.DataFrame())

        eod_dt = pd.Timestamp(datetime.combine(trade_date, EOD_TIME))
        tb = pd.DataFrame()
        source = ""

        if not bars.empty and "high" in bars.columns:
            mask = (
                (bars["datetime"] > entry_time)
                & (bars["datetime"].dt.date == trade_date)
                & (bars["datetime"] <= eod_dt)
            )
            tb = bars.loc[mask].sort_values("datetime")
            if not tb.empty:
                source = "1m"

        if tb.empty:
            bars5 = _load_5m_bars(ticker)
            if not bars5.empty:
                mask5 = (
                    (bars5["date"] > entry_time)
                    & (bars5["date"].dt.date == trade_date)
                    & (bars5["date"] <= eod_dt)
                )
                tb = bars5.loc[mask5].sort_values("date")
                if tb.empty:
                    same_day = bars5.loc[
                        (bars5["date"].dt.date == trade_date)
                        & (bars5["date"] <= eod_dt)
                    ].sort_values("date")
                    tb = same_day.tail(1)
                if not tb.empty:
                    source = "5m"

        if not tb.empty and "close" in tb.columns:
            eod_close = float(tb["close"].iloc[-1])
        else:
            eod_close = entry_price

        if source == "1m":
            bars_1m_used += 1
        elif source == "5m":
            bars_5m_used += 1
        else:
            no_bars += 1

        items.append(
            {
                "ticker": ticker,
                "side": side,
                "setup": setup,
                "trade_date": trade_date,
                "entry_price": entry_price,
                "cost_pct": cost_pct,
                "notional": notional,
                "leverage": leverage,
                "highs": tb["high"].values.astype(np.float64) if not tb.empty else np.empty(0, dtype=np.float64),
                "lows": tb["lows"].values.astype(np.float64) if "lows" in tb.columns else (tb["low"].values.astype(np.float64) if not tb.empty else np.empty(0, dtype=np.float64)),
                "eod_close": eod_close,
            }
        )

    total_bars = sum(len(x["highs"]) for x in items)
    print(
        f"[PREP] {label}: {len(items)} trades | "
        f"1m_used={bars_1m_used} | 5m_fallback={bars_5m_used} | no_bars={no_bars} | "
        f"{total_bars:,} total bar observations"
    )
    return items


# keep compatibility if pandas kept only 'low'
for tk, df_b in list(bar_cache.items()):
    if not df_b.empty and "low" in df_b.columns and "lows" not in df_b.columns:
        df_b["lows"] = df_b["low"]
        bar_cache[tk] = df_b

trade_bars = build_trade_bars(raw, "ALL")


# ---------------------------------------------------------------------------
# RESOLVER: SL / TARGET / EOD ONLY, NO BE/TRAIL
# ---------------------------------------------------------------------------
def resolve_single(trade: dict, sl_pct: float, tgt_pct: float) -> Tuple[str, float, float]:
    entry_price = trade["entry_price"]
    highs = trade["highs"]
    lows = trade["lows"]
    cost = trade["cost_pct"]
    leverage = float(trade.get("leverage", 5.0))
    side = trade["side"]

    if side == "SHORT":
        stop_px = entry_price * (1.0 + sl_pct)
        target_px = entry_price * (1.0 - tgt_pct)
    else:
        stop_px = entry_price * (1.0 - sl_pct)
        target_px = entry_price * (1.0 + tgt_pct)

    outcome = "EOD"
    exit_px = trade["eod_close"]

    for h, l in zip(highs, lows):
        if side == "SHORT":
            stop_hit = h >= stop_px
            target_hit = l <= target_px
        else:
            stop_hit = l <= stop_px
            target_hit = h >= target_px

        # Same pessimistic rule as the quick re-resolve band:
        # if both happen in one bar, assume the stop wins.
        if stop_hit:
            stressed = stop_px * (1.0 + STOP_SLIP) if side == "SHORT" else stop_px * (1.0 - STOP_SLIP)
            outcome = "SL"
            exit_px = stressed
            break
        if target_hit:
            outcome = "TARGET"
            exit_px = target_px
            break

    if side == "SHORT":
        raw_pct = (entry_price - exit_px) / entry_price * 100.0
    else:
        raw_pct = (exit_px - entry_price) / entry_price * 100.0

    net_price_pct = raw_pct - cost
    net_roi_pct = net_price_pct * leverage
    return outcome, net_price_pct, net_roi_pct


def evaluate_group(
    trades: List[dict],
    group_name: str,
    side: str,
) -> pd.DataFrame:
    dates = [t["trade_date"] for t in trades]
    notional = np.array([t["notional"] for t in trades], dtype=np.float64)
    rows: List[dict] = []
    t_start = time.perf_counter()
    combo_count = len(SL_GRID) * len(TGT_GRID)
    print(f"\n[SWEEP] {group_name}: {len(trades)} trades | {combo_count} combos")

    done = 0
    for sl_pct in SL_GRID:
        for tgt_pct in TGT_GRID:
            outcomes: List[str] = []
            pnl_price_pcts: List[float] = []
            pnl_roi_pcts: List[float] = []
            for trade in trades:
                oc, price_pct, roi_pct = resolve_single(trade, sl_pct, tgt_pct)
                outcomes.append(oc)
                pnl_price_pcts.append(price_pct)
                pnl_roi_pcts.append(roi_pct)

            pnl_arr = np.array(pnl_roi_pcts, dtype=np.float64)
            pnl_price_arr = np.array(pnl_price_pcts, dtype=np.float64)
            metrics = _compute_metrics(pnl_arr, outcomes, dates, notional, price_pnl_arr=pnl_price_arr)
            metrics["group_name"] = group_name
            metrics["side"] = side
            metrics["sl_pct"] = sl_pct
            metrics["tgt_pct"] = tgt_pct
            metrics["rr"] = tgt_pct / sl_pct if sl_pct > 0 else np.nan
            rows.append(metrics)

            done += 1
            if done % 120 == 0:
                elapsed = time.perf_counter() - t_start
                eta = elapsed / done * (combo_count - done)
                print(f"  ... {done}/{combo_count} done ({elapsed:.1f}s, ETA {eta:.0f}s)")

    df = pd.DataFrame(rows)
    df = _add_balanced_score(df)
    print(f"[SWEEP] {group_name}: done in {time.perf_counter() - t_start:.1f}s")
    return df


# ---------------------------------------------------------------------------
# RUN GROUP SWEEPS
# ---------------------------------------------------------------------------
group_defs: List[Tuple[str, pd.DataFrame, str]] = []

for side in ["SHORT", "LONG"]:
    side_df = raw[raw["side"] == side].copy()
    group_defs.append((f"{side}__ALL", side_df, side))
    for setup in sorted(side_df["setup"].unique()):
        setup_df = side_df[side_df["setup"] == setup].copy()
        group_defs.append((f"{side}__{setup}", setup_df, side))

all_grid_frames: List[pd.DataFrame] = []
summary_rows: List[dict] = []

print("\n" + "=" * 88)
print("  RUNNING PER-GROUP SWEEPS")
print("=" * 88)

for group_name, group_df, side in group_defs:
    trades = [t for t in trade_bars if t["side"] == side and (group_name.endswith("__ALL") or t["setup"] == group_name.split("__", 1)[1])]
    if not trades:
        continue

    setup_name = "ALL" if group_name.endswith("__ALL") else group_name.split("__", 1)[1]
    baseline_profile = None
    if setup_name != "ALL":
        baseline_profile = runner._v17c_setup_exit_profile(side, setup_name)

    grid_df = evaluate_group(trades, group_name, side)
    grid_df["setup"] = setup_name
    all_grid_frames.append(grid_df)

    objective_rows = {
        "max_win_rate": _pick_best(grid_df, "wr", ascending=False),
        "max_sum_pnl": _pick_best(grid_df, "total_pct", ascending=False),
        "max_profit_factor": _pick_best(grid_df, "pf", ascending=False),
        "max_sharpe": _pick_best(grid_df, "sharpe", ascending=False),
        "max_day_win_rate": _pick_best(grid_df, "dwr", ascending=False),
        "balanced": _pick_best(grid_df, "balanced_score", ascending=False),
    }

    if baseline_profile is not None:
        baseline_match = grid_df[
            (np.isclose(grid_df["sl_pct"], float(baseline_profile["stop_pct"])))
            & (np.isclose(grid_df["tgt_pct"], float(baseline_profile["target_pct"])))
        ]
        if not baseline_match.empty:
            objective_rows["baseline_v19a"] = baseline_match.iloc[0]

    for objective_name, row in objective_rows.items():
        summary_rows.append(
            {
                "group_name": group_name,
                "side": side,
                "setup": setup_name,
                "trades": int(row["n"]),
                "objective": objective_name,
                "sl_pct": float(row["sl_pct"]),
                "tgt_pct": float(row["tgt_pct"]),
                "rr": float(row["rr"]),
                "wr": float(row["wr"]),
                "total_pct": float(row["total_pct"]),
                "pf": float(row["pf"]),
                "sharpe": float(row["sharpe"]),
                "dwr": float(row["dwr"]),
                "max_dd": float(row["max_dd"]),
                "tgt_rate": float(row["tgt_rate"]),
                "sl_rate": float(row["sl_rate"]),
                "eod_rate": float(row["eod_rate"]),
                "total_rs": float(row["total_rs"]) if not np.isnan(row["total_rs"]) else np.nan,
                "balanced_score": float(row["balanced_score"]),
            }
        )


all_grid = pd.concat(all_grid_frames, ignore_index=True)
summary_df = pd.DataFrame(summary_rows)


# ---------------------------------------------------------------------------
# PRINT SUMMARY
# ---------------------------------------------------------------------------
def fmt_pct(x: float) -> str:
    return f"{x * 100:.2f}%"


print("\n" + "=" * 112)
print("  BEST PARAMETERS BY GROUP")
print("=" * 112)

for group_name, group_block in summary_df.groupby("group_name", sort=False):
    group_block = group_block.copy()
    side = group_block["side"].iloc[0]
    setup = group_block["setup"].iloc[0]
    trades = int(group_block["trades"].iloc[0])
    print(f"\n[{group_name}] side={side} | setup={setup} | trades={trades}")
    if trades < MIN_TRADES_FOR_MAIN_RECO:
        print("  note: very small sample, treat recommendations as low-confidence.")

    for objective in [
        "baseline_v19a",
        "max_win_rate",
        "max_sum_pnl",
        "max_profit_factor",
        "max_sharpe",
        "max_day_win_rate",
        "balanced",
    ]:
        row_df = group_block[group_block["objective"] == objective]
        if row_df.empty:
            continue
        row = row_df.iloc[0]
        print(
            f"  {objective:<17} "
            f"SL={fmt_pct(row['sl_pct'])} "
            f"TGT={fmt_pct(row['tgt_pct'])} "
            f"RR={row['rr']:.2f}x | "
            f"WR={row['wr']:.2f}% "
            f"SumPnL={row['total_pct']:.2f}% "
            f"PF={row['pf']:.3f} "
            f"Sharpe={row['sharpe']:.2f} "
            f"DWR={row['dwr']:.2f}% "
            f"MaxDD={row['max_dd']:.2f}%"
        )


# ---------------------------------------------------------------------------
# SAVE OUTPUTS
# ---------------------------------------------------------------------------
out_dir = HERE / "sweep_results"
out_dir.mkdir(parents=True, exist_ok=True)
ts = datetime.now().strftime("%Y%m%d_%H%M%S")

grid_path = out_dir / f"v19a_setup_sweep_grid_{ts}.csv"
summary_path = out_dir / f"v19a_setup_sweep_summary_{ts}.csv"

all_grid.to_csv(grid_path, index=False)
summary_df.to_csv(summary_path, index=False)

print("\n[SAVE]")
print(f"  grid   -> {grid_path}")
print(f"  summary-> {summary_path}")
print("\n[DONE] V19A setup sweep complete.")
