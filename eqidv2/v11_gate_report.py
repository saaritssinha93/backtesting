"""
v11_gate_report.py
==================

Adapter that turns a finished v11 backtest (``trades.csv`` / ``v11_ID_trades.csv``)
into a **net-of-cost, walk-forward promotion decision** per setup, using the
canonical ``walkforward_gate.run_gate`` plus a **day-clustered (block) bootstrap**.

Why this exists
---------------
The v11 historical modes resolve trades **price-only, no backtest cost**
(``v6_cost_rs = 0.0``) — the headline PnL is GROSS. At ~Rs 100k notional the
statutory NSE intraday cost is ~Rs 83 round-trip, so a 1,330-trade book carries
~Rs 110k of invisible drag. This script:

  1. Maps the v11 trade rows into the gate's 6-column input contract.
  2. Applies the real ``nse_intraday_costs`` model to every trade (gross -> net).
  3. Runs ``walkforward_gate.run_gate`` (purged WF folds, BH-FDR, overfit flag) —
     the ONLY thing allowed to author a promotion.
  4. Adds a **day-clustered bootstrap** p-value: intraday trades within a day
     share the same market move (Jun-4 proves it), so trade-level resampling
     understates variance. We resample *days*, not trades.
  5. Writes a per-setup decision report + a per-trade costed book.

This does NOT edit the gate or the backtester. It consumes outcomes and reports
which setups deserve forward trust. Eyeball "exclude last week's losers and
re-score" is explicitly what this replaces.

Usage
-----
  python v11_gate_report.py
  python v11_gate_report.py --trades C:\\TradingData\\eqidv2\\outputs_ID_v11_5min\\trades.csv
  python v11_gate_report.py --train_days 40 --test_days 15   # smaller WF window
"""

from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd

from nse_intraday_costs import CostConfig, intraday_equity_costs
import walkforward_gate as wfg


DEFAULT_TRADES = Path(r"C:\TradingData\eqidv2\outputs_ID_v11_5min\trades.csv")

# trades.csv (internal v11 schema) -> gate contract.
COLUMN_MAP = {
    "trade_date": "date",
    "setup": "setup",
    "side": "side",
    "entry_price_v6": "entry_price",
    "v6_exit_price": "exit_price",
    "quantity": "qty",
}
# v11_ID_trades.csv (canonical schema) fallback mapping.
CANONICAL_MAP = {
    "date": "date",
    "setup_name": "setup",
    "side": "side",
    "entry_price": "entry_price",
    "exit_price": "exit_price",
    "quantity": "qty",
}
# Numeric features carried through for optional TRAIN-only threshold fitting.
FEATURE_COLS = [
    "quality_score", "ranker_score", "rs_pct", "market_ret_pct", "market_abs_ret_pct",
    "vol_ratio", "atr_pct", "body_pct", "close_loc", "vwap_dist_atr",
    "v7_signal_notional_rs", "signal_minute", "upper_wick_pct", "lower_wick_pct",
    "wick_skew_pct", "signal_range_pct",
]


def _load_and_map(trades_path: Path) -> pd.DataFrame:
    df = pd.read_csv(trades_path)
    if "entry_price_v6" in df.columns and "v6_exit_price" in df.columns:
        cmap = COLUMN_MAP
    elif "setup_name" in df.columns and "exit_price" in df.columns:
        cmap = CANONICAL_MAP
    else:
        raise SystemExit(
            f"[v11_gate] {trades_path} is neither a v11 trades.csv nor a v11_ID_trades.csv "
            f"(missing entry/exit columns). Columns: {list(df.columns)[:12]}..."
        )

    out = pd.DataFrame()
    for src, dst in cmap.items():
        if src not in df.columns:
            raise SystemExit(f"[v11_gate] required column {src!r} missing from {trades_path}")
        out[dst] = df[src]
    for feat in FEATURE_COLS:
        if feat in df.columns:
            out[feat] = pd.to_numeric(df[feat], errors="coerce")

    out["date"] = pd.to_datetime(out["date"], errors="coerce").dt.normalize()
    out["setup"] = out["setup"].astype(str).str.strip()
    out["side"] = out["side"].astype(str).str.upper().str.strip()
    for col in ("entry_price", "exit_price", "qty"):
        out[col] = pd.to_numeric(out[col], errors="coerce")

    before = len(out)
    out = out.dropna(subset=["date", "setup", "side", "entry_price", "exit_price", "qty"])
    out = out[(out["entry_price"] > 0) & (out["exit_price"] > 0) & (out["qty"] > 0)]
    out = out[out["side"].isin(["LONG", "SHORT"])].reset_index(drop=True)
    dropped = before - len(out)
    if dropped:
        print(f"[v11_gate] dropped {dropped} rows with missing/invalid required fields")
    return out


def _cost_book(df: pd.DataFrame, cfg: CostConfig) -> pd.DataFrame:
    """Per-trade gross/cost/net using the statutory NSE intraday model."""
    gross = np.where(
        df["side"].to_numpy() == "LONG",
        (df["exit_price"] - df["entry_price"]) * df["qty"],
        (df["entry_price"] - df["exit_price"]) * df["qty"],
    )
    net = wfg.net_pnl_vectorized(
        df["entry_price"].to_numpy(), df["exit_price"].to_numpy(),
        df["qty"].to_numpy(), df["side"].to_numpy(), cfg,
    )
    out = df.copy()
    out["gross_pnl_rs"] = gross
    out["net_pnl_rs"] = net
    out["cost_rs"] = gross - net
    return out


def _block_bootstrap_p(daily_net: np.ndarray, n_boot: int, seed: int) -> float:
    """One-sided p-value that mean daily net > 0, resampling DAYS (block bootstrap).

    Resampling trading days rather than trades respects intraday correlation:
    all trades on a single market-move day rise/fall together, so trade-level
    resampling understates variance and makes p-values optimistic.
    """
    d = np.asarray(daily_net, float)
    if len(d) < 3:
        return float("nan")
    rng = np.random.default_rng(seed)
    idx = rng.integers(0, len(d), size=(n_boot, len(d)))
    boot_means = d[idx].mean(axis=1)
    return float((boot_means <= 0).mean())


def _per_setup_economics(costed: pd.DataFrame, n_boot: int, seed: int) -> pd.DataFrame:
    rows = []
    for setup, g in costed.groupby("setup"):
        daily = g.groupby("date")["net_pnl_rs"].sum()
        rows.append({
            "setup": setup,
            "side": g["side"].mode().iloc[0] if not g["side"].empty else "",
            "n_trades": int(len(g)),
            "n_days": int(daily.shape[0]),
            "gross_total_rs": round(float(g["gross_pnl_rs"].sum()), 2),
            "cost_total_rs": round(float(g["cost_rs"].sum()), 2),
            "net_total_rs": round(float(g["net_pnl_rs"].sum()), 2),
            "net_per_trade_rs": round(float(g["net_pnl_rs"].mean()), 2),
            "net_win_rate": round(float((g["net_pnl_rs"] > 0).mean()), 4),
            "net_pf": round(wfg._profit_factor(g["net_pnl_rs"].to_numpy()), 3),
            "day_block_boot_p": round(_block_bootstrap_p(daily.to_numpy(), n_boot, seed), 4),
        })
    return pd.DataFrame(rows).sort_values("net_total_rs", ascending=False).reset_index(drop=True)


def main() -> int:
    ap = argparse.ArgumentParser(description="v11 trades -> net-of-cost walk-forward gate report")
    ap.add_argument("--trades", type=str, default=str(DEFAULT_TRADES))
    ap.add_argument("--out_dir", type=str, default="")
    ap.add_argument("--train_days", type=int, default=60)
    ap.add_argument("--test_days", type=int, default=20)
    ap.add_argument("--embargo_days", type=int, default=1)
    ap.add_argument("--min_oos_trades", type=int, default=40)
    ap.add_argument("--min_net_pf", type=float, default=1.20)
    ap.add_argument("--n_bootstrap", type=int, default=10000)
    ap.add_argument("--seed", type=int, default=7)
    args = ap.parse_args()

    trades_path = Path(args.trades)
    out_dir = Path(args.out_dir) if args.out_dir else trades_path.parent
    out_dir.mkdir(parents=True, exist_ok=True)
    cfg = CostConfig()

    df = _load_and_map(trades_path)
    if df.empty:
        raise SystemExit(f"[v11_gate] no usable trades in {trades_path}")
    costed = _cost_book(df, cfg)

    n = len(costed)
    days = costed["date"].nunique()
    gross = float(costed["gross_pnl_rs"].sum())
    cost = float(costed["cost_rs"].sum())
    net = float(costed["net_pnl_rs"].sum())

    print("=" * 92)
    print("v11 NET-OF-COST WALK-FORWARD GATE REPORT")
    print(f"  trades file : {trades_path}")
    print(f"  cost model  : nse_intraday_costs CostConfig (rates_as_of={cfg.rates_as_of})")
    print("=" * 92)
    print(f"  Trades / days        : {n:,} / {days}")
    print(f"  GROSS PnL            : Rs {gross:,.2f}")
    print(f"  Statutory cost       : Rs {cost:,.2f}   (avg Rs {cost / n:,.2f}/trade)")
    print(f"  NET PnL              : Rs {net:,.2f}")
    print(f"  Gross profit factor  : {wfg._profit_factor(costed['gross_pnl_rs'].to_numpy()):.3f}")
    print(f"  Net profit factor    : {wfg._profit_factor(costed['net_pnl_rs'].to_numpy()):.3f}")
    print("=" * 92)

    # ---- Per-setup net economics + day-clustered bootstrap ----
    econ = _per_setup_economics(costed, args.n_bootstrap, args.seed)

    # ---- The gate (the only thing allowed to promote) ----
    wf = wfg.WalkForwardConfig(
        train_days=args.train_days, test_days=args.test_days, embargo_days=args.embargo_days,
        min_oos_trades=args.min_oos_trades, min_net_pf=args.min_net_pf,
        n_bootstrap=args.n_bootstrap, random_seed=args.seed,
    )
    gate = wfg.run_gate(df, wf=wf, cost_cfg=cfg, fit_fn=wfg.no_fit, apply_fn=wfg.apply_identity)
    gate_cols = [
        "setup", "n_oos", "win_rate", "net_pf_oos", "net_expectancy_oos",
        "total_net_oos", "n_folds", "fold_consistency", "p_value",
        "fdr_significant", "overfit_flag", "decision",
    ]
    gate_slim = gate[[c for c in gate_cols if c in gate.columns]]

    # ---- Merge full-sample economics with the WF gate verdict ----
    merged = econ.merge(
        gate_slim.rename(columns={"win_rate": "oos_win_rate", "p_value": "oos_trade_boot_p"}),
        on="setup", how="left",
    ).sort_values("net_total_rs", ascending=False).reset_index(drop=True)

    econ_path = out_dir / "v11_gate_setup_economics.csv"
    gate_path = out_dir / "v11_gate_decisions.csv"
    book_path = out_dir / "v11_gate_costed_book.csv"
    merged_path = out_dir / "v11_gate_report.csv"
    econ.to_csv(econ_path, index=False)
    gate.to_csv(gate_path, index=False)
    costed.to_csv(book_path, index=False)
    merged.to_csv(merged_path, index=False)

    print("\nPER-SETUP NET ECONOMICS (full sample) + day-clustered bootstrap + WF gate verdict")
    print("-" * 92)
    show = merged[[
        "setup", "side", "n_trades", "n_days", "gross_total_rs", "net_total_rs",
        "net_per_trade_rs", "net_pf", "day_block_boot_p", "n_oos", "decision",
    ]].copy()
    with pd.option_context("display.max_rows", None, "display.width", 200):
        print(show.to_string(index=False))

    promoted = gate[gate["decision"] == "PROMOTE"]["setup"].tolist()
    probation = gate[gate["decision"] == "PROBATION"]["setup"].tolist()
    rejected = gate[gate["decision"] == "REJECT"]["setup"].tolist()
    print("-" * 92)
    print(f"  PROMOTE  ({len(promoted)}): {', '.join(promoted) or '(none)'}")
    print(f"  PROBATION({len(probation)}): {', '.join(probation) or '(none)'}")
    print(f"  REJECT   ({len(rejected)}): {', '.join(rejected) or '(none)'}")
    print("-" * 92)
    print(f"  wrote {merged_path}")
    print(f"  wrote {econ_path}")
    print(f"  wrote {gate_path}")
    print(f"  wrote {book_path}")
    print("=" * 92)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
