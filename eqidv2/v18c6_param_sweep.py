# -*- coding: utf-8 -*-
from __future__ import annotations
import sys, io
if hasattr(sys.stdout, "buffer"):
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
"""
v18c6_param_sweep.py
====================
Fast in-memory parameter sweep — finds optimal SL/TGT for v18c6.

How it works
------------
1. Loads the v18c6 trades CSV (entries only).
2. Pre-loads 1-min bar data for ALL tickers into RAM once (~30 s).
3. Pre-extracts per-trade bar arrays (highs[], lows[], eod_close).
4. Sweeps SHORT side: 10 SL × 15 TGT = up to 150 combos (R:R >= 0.50 filter).
5. Sweeps LONG  side: 10 SL × 15 TGT = up to 150 combos.
6. Cross-validates top-12 SHORT × top-12 LONG = 144 combined evals.
7. Prints:
   - Per-side ranked table (by composite score, PF, Sharpe, DayWin)
   - Per-side heatmap of PF over SL × TGT grid
   - Combined top-20 table
   - Final recommendations (canonical vs best-found vs balanced alternative)

Run:  python v18c6_param_sweep.py
"""
import sys
import time
from datetime import datetime, date, time as dtime
from pathlib import Path
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

_HERE = Path(__file__).resolve().parent
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))

# ===========================================================================
# CONFIG
# ===========================================================================
CSV_DIR   = Path(r"C:\TradingData\eqidv2\outputs_v18c6_5min")
DIR_1MIN  = Path(r"C:\TradingData\eqidv2\stocks_indicators_1min_eq")
EOD_TIME  = dtime(15, 20, 0)
STOP_SLIP = 3.0 / 10_000  # 3 bps stress on SL fills

# Grid (as fractions, not percent)
SL_GRID  = [0.0050, 0.0055, 0.0060, 0.0065, 0.0070,
            0.0075, 0.0080, 0.0085, 0.0090, 0.0100]
TGT_GRID = [0.0050, 0.0060, 0.0065, 0.0070, 0.0075,
            0.0080, 0.0085, 0.0090, 0.0095, 0.0100,
            0.0110, 0.0120, 0.0130, 0.0140, 0.0150]

MIN_RR    = 0.50   # minimum TGT/SL ratio — filters out hopeless combos
TOP_CROSS = 12     # top-N per side used for cross-product combined eval

# Composite score weights (must sum to 1.0)
W_PF    = 0.30
W_SHARP = 0.30
W_DWR   = 0.15
W_DD    = 0.15
W_RS    = 0.10

# ===========================================================================
# HELPERS
# ===========================================================================

def _to_naive_ist(series: pd.Series) -> pd.Series:
    s = pd.to_datetime(series, errors="coerce")
    if hasattr(s.dt, "tz") and s.dt.tz is not None:
        s = s.dt.tz_convert("Asia/Kolkata").dt.tz_localize(None)
    return s


def _compute_metrics(pnl_arr: np.ndarray, outcomes: List[str], trade_dates: List[date],
                     notional_arr: Optional[np.ndarray] = None) -> dict:
    n = len(pnl_arr)
    if n == 0:
        return {}
    wins   = int((pnl_arr > 0).sum())
    losses = int((pnl_arr < 0).sum())
    gw = float(pnl_arr[pnl_arr > 0].sum())
    gl = float(abs(pnl_arr[pnl_arr < 0].sum()))
    pf = gw / gl if gl > 0 else 999.0
    total_pct = float(pnl_arr.sum())
    wr = wins / n * 100

    # Day-level
    day_pnl: Dict[date, float] = {}
    for i, td in enumerate(trade_dates):
        day_pnl[td] = day_pnl.get(td, 0.0) + float(pnl_arr[i])
    dv = np.array(list(day_pnl.values()))
    dwr = float((dv > 0).sum() / len(dv) * 100) if len(dv) else 0.0

    sharpe = float(dv.mean() / dv.std() * np.sqrt(252)) if dv.std() > 1e-9 else 0.0

    cum = np.cumsum(pnl_arr)
    max_dd = float((cum - np.maximum.accumulate(cum)).min())

    avg_win  = float(pnl_arr[pnl_arr > 0].mean()) if wins   else 0.0
    avg_loss = float(pnl_arr[pnl_arr < 0].mean()) if losses else 0.0

    tgt_rate = outcomes.count("TARGET") / n * 100
    sl_rate  = outcomes.count("SL")     / n * 100
    eod_rate = outcomes.count("EOD")    / n * 100

    # Notional Rs
    total_rs = float(
        np.dot(notional_arr, pnl_arr / 100.0)
    ) if notional_arr is not None else float("nan")

    return dict(
        n=n, wins=wins, losses=losses, wr=wr, pf=pf,
        total_pct=total_pct, total_rs=total_rs,
        sharpe=sharpe, dwr=dwr, max_dd=max_dd,
        avg_win=avg_win, avg_loss=avg_loss,
        tgt_rate=tgt_rate, sl_rate=sl_rate, eod_rate=eod_rate,
        days=len(day_pnl),
    )


def _composite_score(row: pd.Series, pf_max: float, sharpe_max: float,
                     dwr_max: float, dd_worst: float, rs_max: float) -> float:
    """Percentile-normalised weighted composite. Higher = better."""
    pf_n     = min(row["pf"],      pf_max)      / max(pf_max,      1e-9)
    sh_n     = min(row["sharpe"],  sharpe_max)  / max(sharpe_max,  1e-9)
    dwr_n    = row["dwr"]                        / max(dwr_max,     1e-9)
    dd_n     = 1.0 - abs(row["max_dd"]) / max(abs(dd_worst), 1e-9)
    rs_n     = 0.0
    if not np.isnan(row.get("total_rs", float("nan"))):
        rs_n = row["total_rs"] / max(rs_max, 1e-9)
    return (W_PF * pf_n + W_SHARP * sh_n + W_DWR * dwr_n
            + W_DD * dd_n + W_RS * rs_n)


# ===========================================================================
# STEP 1 — LOAD CSV
# ===========================================================================
print("=" * 70)
print("  V18C6 PARAMETER SWEEP — SL / TARGET OPTIMISATION")
print("=" * 70)

csv_path = sorted(CSV_DIR.glob("*trades*.csv"))[-1]
print(f"\n[LOAD] {csv_path.name}")

raw = pd.read_csv(csv_path)
raw["trade_date"]    = pd.to_datetime(raw["trade_date"],    errors="coerce")
raw["entry_time_ist"] = _to_naive_ist(raw["entry_time_ist"])
raw["trade_date_d"]  = raw["trade_date"].dt.date

# Cost per trade (round-trip %, using CSV columns or defaults)
slip_col = (pd.to_numeric(raw["slippage_pct"],   errors="coerce").fillna(0.0005)
            if "slippage_pct"   in raw.columns
            else pd.Series(0.0005, index=raw.index))
comm_col = (pd.to_numeric(raw["commission_pct"], errors="coerce").fillna(0.0003)
            if "commission_pct" in raw.columns
            else pd.Series(0.0003, index=raw.index))
raw["cost_rt"] = (slip_col + comm_col) * 2.0 * 100.0  # round-trip, in %

# Notional exposure (Rs) for Rs P&L estimation
if "notional_exposure_rs" in raw.columns:
    raw["notional_rs"] = pd.to_numeric(raw["notional_exposure_rs"], errors="coerce").fillna(200.0)
else:
    raw["notional_rs"] = 200.0  # fallback guess

print(f"[LOAD] Columns detected: slippage_pct={'yes' if 'slippage_pct' in raw.columns else 'no (default 0.05%)'} | "
      f"commission_pct={'yes' if 'commission_pct' in raw.columns else 'no (default 0.03%)'} | "
      f"notional_exposure_rs={'yes' if 'notional_exposure_rs' in raw.columns else 'no (default 200)'}")

short_raw = raw[raw["side"].str.upper().eq("SHORT")].copy().reset_index(drop=True)
long_raw  = raw[raw["side"].str.upper().eq("LONG") ].copy().reset_index(drop=True)
print(f"[LOAD] {len(raw)} trades  SHORT={len(short_raw)}  LONG={len(long_raw)}")

# ===========================================================================
# STEP 2 — LOAD 1-MIN BAR DATA
# ===========================================================================
tickers = sorted(raw["ticker"].unique())
print(f"\n[1MIN] Loading {len(tickers)} ticker files from {DIR_1MIN} ...")
t0 = time.perf_counter()

bar_cache: Dict[str, pd.DataFrame] = {}

for tk in tickers:
    for fname in [f"{tk}.parquet", f"{tk}_1min.parquet",
                  f"{tk}_stocks_indicators_1min.parquet"]:
        fpath = DIR_1MIN / fname
        if not fpath.exists():
            continue
        try:
            df_b = pd.read_parquet(fpath, engine="pyarrow")
        except Exception:
            continue
        if df_b.empty:
            break

        # Normalise column names to lowercase first, then find datetime col
        df_b.columns = [c.lower() for c in df_b.columns]

        dt_col = None
        for cand in ("datetime", "date", "timestamp"):
            if cand in df_b.columns:
                dt_col = cand; break
        if dt_col is None and isinstance(df_b.index, pd.DatetimeIndex):
            df_b = df_b.reset_index()
            dt_col = df_b.columns[0]
        if dt_col is None:
            break
        if dt_col != "datetime":
            df_b = df_b.rename(columns={dt_col: "datetime"})

        df_b["datetime"] = pd.to_datetime(df_b["datetime"], errors="coerce")
        if df_b["datetime"].dt.tz is not None:
            df_b["datetime"] = (df_b["datetime"]
                                .dt.tz_convert("Asia/Kolkata")
                                .dt.tz_localize(None))

        needed = ["datetime"] + [c for c in ["high", "low", "close"] if c in df_b.columns]
        dropna_cols = ["datetime", "high", "low"] + (["close"] if "close" in df_b.columns else [])
        df_b = (df_b[needed]
                .dropna(subset=dropna_cols)
                .sort_values("datetime")
                .reset_index(drop=True))

        bar_cache[tk] = df_b
        break
    if tk not in bar_cache:
        bar_cache[tk] = pd.DataFrame()

loaded_n = sum(1 for v in bar_cache.values() if not v.empty)
print(f"[1MIN] Loaded {loaded_n}/{len(tickers)} tickers in {time.perf_counter()-t0:.1f}s")

# ===========================================================================
# STEP 3 — PRE-EXTRACT BAR ARRAYS PER TRADE
# ===========================================================================
print("\n[PREP] Extracting bar arrays per trade ...")

def build_trade_bars(trades_df: pd.DataFrame, side_label: str) -> List[dict]:
    result = []
    no_bars = 0
    for row in trades_df.itertuples(index=False):
        ticker      = str(row.ticker)
        entry_time  = row.entry_time_ist  # tz-naive IST
        trade_date  = row.trade_date_d
        entry_price = float(row.entry_price)
        cost_pct    = float(row.cost_rt)
        notional    = float(row.notional_rs)

        bars = bar_cache.get(ticker, pd.DataFrame())

        if bars.empty or "high" not in bars.columns:
            result.append(dict(entry_price=entry_price, cost_pct=cost_pct,
                               notional=notional, trade_date=trade_date,
                               highs=np.empty(0), lows=np.empty(0),
                               eod_close=entry_price,
                               ticker=ticker,
                               setup=str(getattr(row, "setup", ""))))
            no_bars += 1
            continue

        eod_dt = pd.Timestamp(datetime.combine(trade_date, EOD_TIME))
        mask = (
            (bars["datetime"] > entry_time) &
            (bars["datetime"].dt.date == trade_date) &
            (bars["datetime"] <= eod_dt)
        )
        tb = bars.loc[mask]

        if not tb.empty and "close" in tb.columns:
            _c = tb["close"].dropna()
            eod_close = float(_c.iloc[-1]) if not _c.empty else entry_price
        else:
            eod_close = entry_price

        result.append(dict(
            entry_price=entry_price, cost_pct=cost_pct,
            notional=notional, trade_date=trade_date,
            highs=tb["high"].values.astype(np.float64),
            lows= tb["low"].values.astype(np.float64),
            eod_close=eod_close,
            ticker=ticker,
            setup=str(getattr(row, "setup", "")),
        ))

    bar_count = sum(len(t["highs"]) for t in result)
    print(f"[PREP]   {side_label}: {len(result)} trades | "
          f"{len(result)-no_bars} with 1-min bars | "
          f"{bar_count:,} total bar observations")
    return result

short_bars = build_trade_bars(short_raw, "SHORT")
long_bars  = build_trade_bars(long_raw,  "LONG")

# Sanity check: verify a sample trade resolves correctly
_s0 = short_bars[0]
_ep, _ec, _cost = _s0["entry_price"], _s0["eod_close"], _s0["cost_pct"]
print(f"[SANITY] SHORT trade 0: ep={_ep:.2f}  eod_close={_ec:.2f}  cost={_cost:.4f}%  n_bars={len(_s0['highs'])}")
_raw_eod = (_ep - _ec) / _ep * 100.0
print(f"[SANITY]   EOD P&L (net): {_raw_eod - _cost:.4f}%  (should be non-NaN, roughly -3% to +3%)")

short_dates = [t["trade_date"] for t in short_bars]
long_dates  = [t["trade_date"] for t in long_bars]
short_notional = np.array([t["notional"] for t in short_bars])
long_notional  = np.array([t["notional"] for t in long_bars])

# ===========================================================================
# STEP 4 — FAST EXIT RESOLVER
# ===========================================================================

def resolve_single(trade: dict, sl_pct: float, tgt_pct: float, side: str) -> tuple:
    """Return (outcome, net_pnl_pct) for one trade."""
    ep     = trade["entry_price"]
    highs  = trade["highs"]
    lows   = trade["lows"]
    cost   = trade["cost_pct"]

    if side == "SHORT":
        stop_px = ep * (1.0 + sl_pct)
        tgt_px  = ep * (1.0 - tgt_pct)
    else:
        stop_px = ep * (1.0 - sl_pct)
        tgt_px  = ep * (1.0 + tgt_pct)

    outcome  = "EOD"
    exit_px  = trade["eod_close"]

    for i in range(len(highs)):
        h, l = highs[i], lows[i]
        if side == "SHORT":
            sl_hit  = h >= stop_px
            tgt_hit = l <= tgt_px
        else:
            sl_hit  = l <= stop_px
            tgt_hit = h >= tgt_px

        if sl_hit:
            # Pessimistic: stop wins ambiguous bars
            stressed = stop_px * (1.0 + STOP_SLIP) if side == "SHORT" else stop_px * (1.0 - STOP_SLIP)
            outcome  = "SL"
            exit_px  = stressed
            break
        if tgt_hit:
            outcome = "TARGET"
            exit_px = tgt_px
            break

    if side == "SHORT":
        raw = (ep - exit_px) / ep * 100.0
    else:
        raw = (exit_px - ep) / ep * 100.0

    return outcome, raw - cost


def evaluate_params(trades: List[dict], sl_pct: float, tgt_pct: float,
                    side: str, dates: List[date],
                    notional: np.ndarray) -> Optional[dict]:
    outcomes  = []
    pnl_pcts  = []
    for trade in trades:
        oc, pct = resolve_single(trade, sl_pct, tgt_pct, side)
        outcomes.append(oc)
        pnl_pcts.append(pct)
    pnl_arr = np.array(pnl_pcts)
    m = _compute_metrics(pnl_arr, outcomes, dates, notional)
    m["sl_pct"]  = sl_pct
    m["tgt_pct"] = tgt_pct
    m["rr"]      = tgt_pct / sl_pct
    return m


# ===========================================================================
# STEP 5 — GRID SWEEP PER SIDE
# ===========================================================================

def run_side_sweep(trades, sl_grid, tgt_grid, side, dates, notional, label) -> pd.DataFrame:
    combos = [(sl, tgt) for sl in sl_grid for tgt in tgt_grid
              if tgt / sl >= MIN_RR]
    print(f"\n[SWEEP] {label}: {len(combos)} combos "
          f"({len(sl_grid)}SL × {len(tgt_grid)}TGT, R:R>={MIN_RR})")

    t_start = time.perf_counter()
    rows = []
    for i, (sl, tgt) in enumerate(combos, 1):
        m = evaluate_params(trades, sl, tgt, side, dates, notional)
        if m:
            rows.append(m)
        if i % 30 == 0:
            elapsed = time.perf_counter() - t_start
            eta = elapsed / i * (len(combos) - i)
            print(f"  ... {i}/{len(combos)} done ({elapsed:.1f}s, ETA {eta:.0f}s)")

    df = pd.DataFrame(rows)
    elapsed = time.perf_counter() - t_start
    print(f"[SWEEP] {label}: done in {elapsed:.1f}s")
    return df


print("\n" + "=" * 70)
print("  PHASE 1 — INDEPENDENT SIDE SWEEPS")
print("=" * 70)

short_sweep = run_side_sweep(short_bars, SL_GRID, TGT_GRID,
                              "SHORT", short_dates, short_notional, "SHORT")
long_sweep  = run_side_sweep(long_bars,  SL_GRID, TGT_GRID,
                              "LONG",  long_dates,  long_notional,  "LONG")


# ===========================================================================
# STEP 6 — SCORING & REPORTING
# ===========================================================================

def add_composite(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    pf_max     = df["pf"].replace([np.inf], np.nan).quantile(0.95)
    sharpe_max = df["sharpe"].quantile(0.95)
    dwr_max    = df["dwr"].max()
    dd_worst   = df["max_dd"].min()   # most negative
    rs_max     = df["total_rs"].replace([np.nan], 0).quantile(0.95) if "total_rs" in df else 1.0

    df["composite"] = df.apply(
        _composite_score, axis=1,
        pf_max=pf_max, sharpe_max=sharpe_max,
        dwr_max=dwr_max, dd_worst=dd_worst, rs_max=rs_max,
    )
    return df

short_sweep = add_composite(short_sweep)
long_sweep  = add_composite(long_sweep)


def print_top_table(df: pd.DataFrame, label: str, sort_col: str = "composite",
                    n: int = 15) -> None:
    top = df.sort_values(sort_col, ascending=False).head(n).copy()
    top["pf_str"]   = top["pf"].apply(lambda x: f"{x:.3f}" if x < 100 else "inf")
    top["sl_%"]     = (top["sl_pct"]  * 100).map("{:.2f}%".format)
    top["tgt_%"]    = (top["tgt_pct"] * 100).map("{:.2f}%".format)
    top["rr"]       = top["rr"].map("{:.2f}x".format)
    top["sharpe_s"] = top["sharpe"].map("{:.2f}".format)
    top["dwr_s"]    = top["dwr"].map("{:.1f}%".format)
    top["dd_s"]     = top["max_dd"].map("{:.1f}%".format)
    top["tgt_r"]    = top["tgt_rate"].map("{:.0f}%".format)
    top["sl_r"]     = top["sl_rate"].map("{:.0f}%".format)
    top["eod_r"]    = top["eod_rate"].map("{:.0f}%".format)
    top["rs_s"]     = top["total_rs"].apply(
        lambda x: f"Rs.{x:+,.0f}" if not np.isnan(x) else "n/a")
    top["comp"]     = top["composite"].map("{:.4f}".format)

    hdr = (f"{'SL':>6} {'TGT':>7} {'R:R':>5} | {'PF':>6} {'Sharpe':>7} "
           f"{'DWR':>6} {'MaxDD':>7} | {'TGT%':>5} {'SL%':>5} {'EOD%':>5} "
           f"| {'Rs PnL':>13} {'Score':>7}")
    print(f"\n{'─'*110}")
    print(f"  {label}  —  TOP {n} by [{sort_col}]")
    print(f"{'─'*110}")
    print(f"  {hdr}")
    print(f"  {'─'*106}")
    for _, r in top.iterrows():
        print(f"  {r['sl_%']:>6} {r['tgt_%']:>7} {r['rr']:>5} | "
              f"{r['pf_str']:>6} {r['sharpe_s']:>7} "
              f"{r['dwr_s']:>6} {r['dd_s']:>7} | "
              f"{r['tgt_r']:>5} {r['sl_r']:>5} {r['eod_r']:>5} "
              f"| {r['rs_s']:>13} {r['comp']:>7}")


def print_heatmap(df: pd.DataFrame, metric: str, label: str) -> None:
    pivot = df.pivot_table(index="sl_pct", columns="tgt_pct", values=metric, aggfunc="mean")
    pivot.index   = [f"{v*100:.2f}%" for v in pivot.index]
    pivot.columns = [f"{v*100:.2f}%" for v in pivot.columns]
    pivot_f = pivot.applymap(lambda x: f"{x:.2f}" if pd.notna(x) and x < 100 else ("inf" if x >= 100 else "—"))
    print(f"\n{'─'*110}")
    print(f"  HEATMAP [{metric}]  —  {label}   (rows=SL, cols=TGT)")
    print(f"{'─'*110}")
    _hdr_label = "SL \\ TGT"
    header = f"  {_hdr_label:>9}" + "".join(f"{c:>8}" for c in pivot_f.columns)
    print(header)
    for sl_label, row_s in pivot_f.iterrows():
        print(f"  {sl_label:>9}" + "".join(f"{v:>8}" for v in row_s.values))


# ---- Print SHORT results ----
print("\n" + "=" * 110)
print("  SHORT SIDE RESULTS")
print("=" * 110)
print_top_table(short_sweep, "SHORT", "composite",  15)
print_top_table(short_sweep, "SHORT", "pf",         10)
print_top_table(short_sweep, "SHORT", "sharpe",     10)
print_top_table(short_sweep, "SHORT", "dwr",        10)
print_heatmap(short_sweep, "pf",     "SHORT PF")
print_heatmap(short_sweep, "sharpe", "SHORT Sharpe")

# ---- Print LONG results ----
print("\n" + "=" * 110)
print("  LONG SIDE RESULTS")
print("=" * 110)
print_top_table(long_sweep, "LONG", "composite", 15)
print_top_table(long_sweep, "LONG", "pf",        10)
print_top_table(long_sweep, "LONG", "sharpe",    10)
print_top_table(long_sweep, "LONG", "dwr",       10)
print_heatmap(long_sweep, "pf",     "LONG PF")
print_heatmap(long_sweep, "sharpe", "LONG Sharpe")


# ===========================================================================
# STEP 7 — CROSS-VALIDATE TOP-SHORT × TOP-LONG COMBINED
# ===========================================================================
print("\n" + "=" * 70)
print("  PHASE 2 — COMBINED CROSS-VALIDATION")
print("=" * 70)

top_short_params = (short_sweep.sort_values("composite", ascending=False)
                    .head(TOP_CROSS)[["sl_pct", "tgt_pct"]].values.tolist())
top_long_params  = (long_sweep.sort_values("composite", ascending=False)
                    .head(TOP_CROSS)[["sl_pct", "tgt_pct"]].values.tolist())

print(f"[CROSS] {len(top_short_params)} short × {len(top_long_params)} long = "
      f"{len(top_short_params)*len(top_long_params)} combined evals ...")

cross_rows = []
t_cross = time.perf_counter()
total_cross = len(top_short_params) * len(top_long_params)
done = 0

for ssl, stgt in top_short_params:
    s_row = short_sweep[(short_sweep.sl_pct == ssl) & (short_sweep.tgt_pct == stgt)].iloc[0]
    for lsl, ltgt in top_long_params:
        l_row = long_sweep[(long_sweep.sl_pct == lsl) & (long_sweep.tgt_pct == ltgt)].iloc[0]

        # Combined metrics: merge the two outcome lists
        # Re-evaluate both sides for this combo
        s_ocs, s_pnl = [], []
        for trade in short_bars:
            oc, pct = resolve_single(trade, ssl, stgt, "SHORT")
            s_ocs.append(oc); s_pnl.append(pct)
        l_ocs, l_pnl = [], []
        for trade in long_bars:
            oc, pct = resolve_single(trade, lsl, ltgt, "LONG")
            l_ocs.append(oc); l_pnl.append(pct)

        all_pnl     = np.array(s_pnl + l_pnl)
        all_ocs     = s_ocs + l_ocs
        all_dates   = short_dates + long_dates
        all_notional = np.concatenate([short_notional, long_notional])

        m = _compute_metrics(all_pnl, all_ocs, all_dates, all_notional)
        m.update({
            "short_sl":  ssl, "short_tgt": stgt, "short_rr": stgt/ssl,
            "long_sl":   lsl, "long_tgt":  ltgt, "long_rr":  ltgt/lsl,
            "short_pf":  s_row["pf"],  "long_pf": l_row["pf"],
            "short_dwr": s_row["dwr"], "long_dwr": l_row["dwr"],
            "short_sharpe": s_row["sharpe"], "long_sharpe": l_row["sharpe"],
        })
        cross_rows.append(m)
        done += 1

cross_df = pd.DataFrame(cross_rows)
print(f"[CROSS] Done in {time.perf_counter()-t_cross:.1f}s")

# Add composite to combined results
cross_df = add_composite(cross_df)

# Print combined top-20
top_cross = cross_df.sort_values("composite", ascending=False).head(20).copy()

print(f"\n{'─'*130}")
print(f"  COMBINED — TOP 20 by composite score")
print(f"{'─'*130}")
hdr2 = (f"  {'SHORT':>12} {'LONG':>12} | {'Comb PF':>8} {'Sharpe':>7} "
        f"{'DWR':>6} {'MaxDD':>7} {'Rs PnL':>13} | "
        f"{'sPF':>6} {'lPF':>6} | {'Score':>7}")
print(hdr2)
print(f"  {'─'*126}")

for _, r in top_cross.iterrows():
    s_str  = f"SL{r['short_sl']*100:.2f}%/T{r['short_tgt']*100:.2f}%"
    l_str  = f"SL{r['long_sl']*100:.2f}%/T{r['long_tgt']*100:.2f}%"
    pf_str = f"{r['pf']:.3f}" if r["pf"] < 100 else "inf"
    rs_str = f"Rs.{r['total_rs']:+,.0f}" if not np.isnan(r["total_rs"]) else "n/a"
    print(f"  {s_str:>12} {l_str:>12} | {pf_str:>8} {r['sharpe']:>7.2f} "
          f"{r['dwr']:>6.1f}% {r['max_dd']:>7.1f}% {rs_str:>13} | "
          f"{r['short_pf']:>6.3f} {r['long_pf']:>6.3f} | "
          f"{r['composite']:>7.4f}")


# ===========================================================================
# STEP 8 — FINAL RECOMMENDATIONS
# ===========================================================================
print(f"\n{'=' * 90}")
print("  RECOMMENDATIONS")
print(f"{'=' * 90}")

# Canonical v18c6 params
CAN_SHORT_SL  = 0.0075; CAN_SHORT_TGT = 0.0095
CAN_LONG_SL   = 0.0075; CAN_LONG_TGT  = 0.0100

# Find canonical in the combined grid (closest match)
can_s_row = short_sweep[(short_sweep.sl_pct == CAN_SHORT_SL) &
                         (short_sweep.tgt_pct == CAN_SHORT_TGT)]
can_l_row = long_sweep[(long_sweep.sl_pct == CAN_LONG_SL) &
                        (long_sweep.tgt_pct == CAN_LONG_TGT)]

if not can_s_row.empty and not can_l_row.empty:
    cs = can_s_row.iloc[0]
    cl = can_l_row.iloc[0]

    # Find canonical in cross_df
    can_cross = cross_df[
        (cross_df.short_sl  == CAN_SHORT_SL)  & (cross_df.short_tgt == CAN_SHORT_TGT) &
        (cross_df.long_sl   == CAN_LONG_SL)   & (cross_df.long_tgt  == CAN_LONG_TGT)
    ]
    can_rs = can_cross.iloc[0]["total_rs"] if not can_cross.empty else float("nan")

    print(f"\n  CANONICAL v18c6  (SHORT SL={CAN_SHORT_SL*100:.2f}% TGT={CAN_SHORT_TGT*100:.2f}%  |"
          f"  LONG SL={CAN_LONG_SL*100:.2f}% TGT={CAN_LONG_TGT*100:.2f}%)")
    print(f"    SHORT  PF={cs['pf']:.3f}  Sharpe={cs['sharpe']:.2f}  DWR={cs['dwr']:.1f}%  "
          f"MaxDD={cs['max_dd']:.1f}%  TGT-rate={cs['tgt_rate']:.0f}%  Score={cs['composite']:.4f}")
    print(f"    LONG   PF={cl['pf']:.3f}  Sharpe={cl['sharpe']:.2f}  DWR={cl['dwr']:.1f}%  "
          f"MaxDD={cl['max_dd']:.1f}%  TGT-rate={cl['tgt_rate']:.0f}%  Score={cl['composite']:.4f}")
    if not np.isnan(can_rs):
        print(f"    Combined Rs: Rs.{can_rs:+,.0f}")

best_overall = top_cross.iloc[0]
print(f"\n  BEST OVERALL  (by composite score)")
print(f"    SHORT  SL={best_overall['short_sl']*100:.2f}%  TGT={best_overall['short_tgt']*100:.2f}%  "
      f"R:R={best_overall['short_rr']:.2f}x  PF={best_overall['short_pf']:.3f}")
print(f"    LONG   SL={best_overall['long_sl']*100:.2f}%  TGT={best_overall['long_tgt']*100:.2f}%  "
      f"R:R={best_overall['long_rr']:.2f}x  PF={best_overall['long_pf']:.3f}")
print(f"    Combined  PF={best_overall['pf']:.3f}  Sharpe={best_overall['sharpe']:.2f}  "
      f"DWR={best_overall['dwr']:.1f}%  MaxDD={best_overall['max_dd']:.1f}%  "
      f"Score={best_overall['composite']:.4f}")
rs_b = best_overall["total_rs"]
if not np.isnan(rs_b):
    print(f"    Combined Rs: Rs.{rs_b:+,.0f}")

# Best by each single metric
for metric, desc in [("pf", "Max Profit Factor"), ("sharpe", "Max Sharpe"),
                     ("dwr", "Max Day-Win Rate")]:
    b = cross_df.sort_values(metric, ascending=False).iloc[0]
    print(f"\n  BEST {desc.upper()}")
    print(f"    SHORT SL={b['short_sl']*100:.2f}% TGT={b['short_tgt']*100:.2f}%  |  "
          f"LONG SL={b['long_sl']*100:.2f}% TGT={b['long_tgt']*100:.2f}%")
    pf_s = f"{b['pf']:.3f}" if b["pf"] < 100 else "inf"
    print(f"    Combined  PF={pf_s}  Sharpe={b['sharpe']:.2f}  "
          f"DWR={b['dwr']:.1f}%  MaxDD={b['max_dd']:.1f}%")
    if not np.isnan(b["total_rs"]):
        print(f"    Combined Rs: Rs.{b['total_rs']:+,.0f}")

# Recommendation: best composite with PF >= canonical PF
can_pf_threshold = can_cross.iloc[0]["pf"] if not can_cross.empty else 1.70
improved = cross_df[cross_df["pf"] >= can_pf_threshold].sort_values("composite", ascending=False)
if not improved.empty:
    rec = improved.iloc[0]
    print(f"\n  BALANCED RECOMMENDATION  (PF ≥ {can_pf_threshold:.3f}, best composite)")
    print(f"    SHORT SL={rec['short_sl']*100:.2f}%  TGT={rec['short_tgt']*100:.2f}%  "
          f"R:R={rec['short_rr']:.2f}x  PF={rec['short_pf']:.3f}  "
          f"Sharpe={rec['short_sharpe']:.2f}  DWR={rec['short_dwr']:.1f}%")
    print(f"    LONG  SL={rec['long_sl']*100:.2f}%  TGT={rec['long_tgt']*100:.2f}%  "
          f"R:R={rec['long_rr']:.2f}x  PF={rec['long_pf']:.3f}  "
          f"Sharpe={rec['long_sharpe']:.2f}  DWR={rec['long_dwr']:.1f}%")
    print(f"    Combined  PF={rec['pf']:.3f}  Sharpe={rec['sharpe']:.2f}  "
          f"DWR={rec['dwr']:.1f}%  MaxDD={rec['max_dd']:.1f}%  "
          f"Score={rec['composite']:.4f}")
    if not np.isnan(rec["total_rs"]):
        print(f"    Combined Rs: Rs.{rec['total_rs']:+,.0f}")

# Save sweep results to CSV for further analysis
out_dir = Path(r"C:\TradingData\eqidv2\sweep_results")
out_dir.mkdir(parents=True, exist_ok=True)
ts = datetime.now().strftime("%Y%m%d_%H%M%S")
short_sweep.to_csv(out_dir / f"v18c6_sweep_short_{ts}.csv", index=False)
long_sweep.to_csv(out_dir  / f"v18c6_sweep_long_{ts}.csv",  index=False)
cross_df.to_csv(out_dir    / f"v18c6_sweep_combined_{ts}.csv", index=False)
print(f"\n[SAVE] Sweep CSVs -> {out_dir}")

print(f"\n{'=' * 90}")
print("  SWEEP COMPLETE")
print(f"{'=' * 90}")
