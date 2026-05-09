"""v17D Step 0.4 — MAE/MFE per-setup analyzer.

For every closed trade in the input CSV, walks the 1-min OHLC parquet from
entry_time_ist to exit_time_ist and computes:

    MAE = Maximum Adverse Excursion  (worst point against the trade, % of entry)
    MFE = Maximum Favorable Excursion (best point with the trade, % of entry)

These distributions reveal:
  - "Where do winners actually go?"  -> better TGT placement (e.g. P80 of MFE)
  - "Where do losers initially dip?"  -> better SL placement (e.g. P80 of MAE
                                          for losers, so we don't get
                                          stopped out on noise)

Output: per-(side, setup) summary table with recommended SL/TGT picks that:
  - keep R:R >= 1.0
  - keep SL >= 0.75% (your invariant)
  - keep TGT >= 0.80%

Usage:
    python -m eqidv2.v17D_mae_mfe_analyzer \
        --trades outputs_v17C_noNF_live_5min/avwap_longshort_trades_*_candE3.csv \
        --parquet-dir C:/TradingData/eqidv2/stocks_indicators_1min_eq \
        --output v17D_mae_mfe.csv
"""
from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd


def _load_1min(ticker: str, parquet_dir: Path):
    p = parquet_dir / f"{ticker}_stocks_indicators_1min.parquet"
    if not p.exists():
        return None
    df = pd.read_parquet(p)
    df = df[["date", "high", "low", "close"]].copy()
    df["date"] = pd.to_datetime(df["date"]).dt.tz_convert("Asia/Kolkata")
    return df.set_index("date").sort_index()


def _mae_mfe_for_trade(bars, side, entry_time, exit_time, entry_price):
    """Walk 1-min bars from entry to exit and return MAE%, MFE%."""
    if bars is None or bars.empty or entry_price <= 0:
        return float("nan"), float("nan")
    et = pd.to_datetime(entry_time, utc=True).tz_convert("Asia/Kolkata")
    xt = pd.to_datetime(exit_time, utc=True).tz_convert("Asia/Kolkata")
    sub = bars[(bars.index >= et) & (bars.index <= xt)]
    if sub.empty:
        return float("nan"), float("nan")
    h = sub["high"].to_numpy()
    l = sub["low"].to_numpy()
    if side == "LONG":
        mae_price = float(l.min())
        mfe_price = float(h.max())
        mae_pct = (mae_price - entry_price) / entry_price * 100  # negative
        mfe_pct = (mfe_price - entry_price) / entry_price * 100  # positive
    else:
        mae_price = float(h.max())
        mfe_price = float(l.min())
        mae_pct = -(mae_price - entry_price) / entry_price * 100
        mfe_pct = -(mfe_price - entry_price) / entry_price * 100
    return mae_pct, mfe_pct


def analyze(trades: pd.DataFrame, parquet_dir: Path) -> pd.DataFrame:
    df = trades.copy()
    cache: dict = {}
    mae_col, mfe_col = [], []
    for _, t in df.iterrows():
        ticker = t["ticker"]
        if ticker not in cache:
            cache[ticker] = _load_1min(ticker, parquet_dir)
        mae, mfe = _mae_mfe_for_trade(
            cache[ticker], t["side"],
            t["entry_time_ist"], t["exit_time_ist"],
            float(t["entry_price"]),
        )
        mae_col.append(mae)
        mfe_col.append(mfe)
    df["mae_pct"] = mae_col
    df["mfe_pct"] = mfe_col
    return df


def per_setup_summary(df: pd.DataFrame) -> pd.DataFrame:
    """Recommend new SL/TGT per (side, setup) based on MAE/MFE percentiles.

    Heuristic:
      SL_new  = max(0.75, P80(|MAE| of LOSERS only))    -> wide enough to ride out noise
      TGT_new = max(0.80, SL_new, P50(MFE of WINNERS))  -> closer to typical winner peak
                                                           (more hits, smaller per-hit)
    Constraint: TGT_new >= SL_new (R:R >= 1.0).
    """
    rows = []
    for (side, setup), g in df.groupby(["side", "setup"]):
        g = g.copy()
        winners = g[g["outcome"] == "TARGET"]
        losers = g[g["outcome"] == "SL"]
        mae_winners = winners["mae_pct"].dropna()
        mae_losers = losers["mae_pct"].dropna()
        mfe_winners = winners["mfe_pct"].dropna()
        mfe_losers = losers["mfe_pct"].dropna()

        def pctl(s, q):
            if len(s) == 0:
                return float("nan")
            return float(np.nanpercentile(s, q))

        sl_floor_p80_losers = abs(pctl(mae_losers, 20))   # P20 of negatives = 80th pct of magnitude
        tgt_p50_winners = pctl(mfe_winners, 50)
        tgt_p80_winners = pctl(mfe_winners, 80)

        sl_new = max(0.75, sl_floor_p80_losers if not np.isnan(sl_floor_p80_losers) else 0.75)
        tgt_new = max(0.80, sl_new, tgt_p50_winners if not np.isnan(tgt_p50_winners) else 0.80)

        rows.append({
            "side": side, "setup": setup, "n": len(g),
            "n_winners": len(winners), "n_losers": len(losers),
            "p20_mae_winners": pctl(mae_winners, 20),
            "p20_mae_losers": pctl(mae_losers, 20),
            "p50_mfe_winners": tgt_p50_winners,
            "p80_mfe_winners": tgt_p80_winners,
            "current_sl_pct": float(g.get("candE3_sl_pct", pd.Series([0.75])).iloc[0]) if "candE3_sl_pct" in g else 0.75,
            "current_tgt_pct": float(g.get("candE3_tgt_pct", pd.Series([0.80])).iloc[0]) if "candE3_tgt_pct" in g else 0.80,
            "recommended_sl_pct": round(sl_new, 3),
            "recommended_tgt_pct": round(tgt_new, 3),
            "recommended_rr": round(tgt_new / sl_new, 3) if sl_new > 0 else float("nan"),
        })
    return pd.DataFrame(rows).sort_values(["side", "setup"]).reset_index(drop=True)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--trades", required=True)
    p.add_argument("--parquet-dir", required=True)
    p.add_argument("--output", required=True,
                   help="per-trade MAE/MFE-enriched CSV; "
                   "summary written next to it as _summary.csv")
    args = p.parse_args()

    trades = pd.read_csv(args.trades)
    parquet_dir = Path(args.parquet_dir)
    if not parquet_dir.is_dir():
        raise SystemExit(f"parquet dir not found: {parquet_dir}")

    enriched = analyze(trades, parquet_dir)
    enriched.to_csv(args.output, index=False)

    summary = per_setup_summary(enriched)
    summary_path = Path(args.output).with_name(
        Path(args.output).stem + "_summary.csv"
    )
    summary.to_csv(summary_path, index=False)

    print(f"\nv17D MAE/MFE analyzer — {len(enriched)} trades")
    print("=" * 80)
    for _, r in summary.iterrows():
        print(
            f"  {r['side']:5s} {r['setup']:<32s}  n={r['n']:<4d}  "
            f"current SL/TGT={r['current_sl_pct']:.2f}/{r['current_tgt_pct']:.2f}  "
            f"recommended={r['recommended_sl_pct']:.2f}/{r['recommended_tgt_pct']:.2f}  "
            f"R:R={r['recommended_rr']:.2f}"
        )
    print(f"\nWrote per-trade: {args.output}")
    print(f"Wrote per-setup summary: {summary_path}")


if __name__ == "__main__":
    main()
