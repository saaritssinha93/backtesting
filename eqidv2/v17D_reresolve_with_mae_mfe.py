"""v17D Phase 3.1 — re-resolve trades with MAE/MFE-recommended SL/TGT.

Reads a candE3 trades CSV plus the per-setup MAE/MFE summary (with
recommended_sl_pct / recommended_tgt_pct columns) and walks the 1-min
parquets to re-resolve each trade's outcome under the new stops.

Then writes a new trades CSV with re-resolved outcome / pnl_pct_price /
pnl_pct columns. Pass it to v17D_slippage_stress_test next to see if PF
lifts above the 1.30 RED gate.

Usage:
    python -m eqidv2.v17D_reresolve_with_mae_mfe \
        --trades   outputs_v17C_noNF_live_5min/avwap_..._candE3.csv \
        --mae-mfe  outputs_v17D_phase0/mae_mfe_v17C_candE4_summary.csv \
        --parquet-dir C:/TradingData/eqidv2/stocks_indicators_1min_eq \
        --output   outputs_v17D_phase0/trades_reresolved_mae_mfe.csv
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from eqidv2 import v17D_exit_resolver as er


def reresolve(
    trades: pd.DataFrame, picks: pd.DataFrame, parquet_dir: Path,
    leverage: float = 5.0,
) -> pd.DataFrame:
    pick_lookup = {
        (str(r["side"]).upper(), str(r["setup"])): (
            float(r["recommended_sl_pct"]),
            float(r["recommended_tgt_pct"]),
        )
        for _, r in picks.iterrows()
    }

    out = trades.copy()
    cache: dict = {}
    new_outcome, new_exit_price, new_exit_time, new_pnl_price, new_bars = (
        [], [], [], [], []
    )
    new_sl_pct, new_tgt_pct = [], []
    n_missing_pick, n_missing_bars, n_re = 0, 0, 0

    for _, t in out.iterrows():
        side = str(t["side"]).upper()
        setup = str(t["setup"])
        ticker = str(t["ticker"])
        key = (side, setup)
        if key not in pick_lookup:
            n_missing_pick += 1
            new_outcome.append(t.get("outcome"))
            new_exit_price.append(t.get("exit_price"))
            new_exit_time.append(t.get("exit_time_ist"))
            new_pnl_price.append(t.get("pnl_pct_price"))
            new_bars.append(t.get("bars_held"))
            new_sl_pct.append(t.get("candE3_sl_pct"))
            new_tgt_pct.append(t.get("candE3_tgt_pct"))
            continue

        sl_new, tgt_new = pick_lookup[key]
        if ticker not in cache:
            cache[ticker] = er.load_1min(parquet_dir, ticker)
        bars = cache[ticker]
        if bars is None or bars.empty:
            n_missing_bars += 1
            new_outcome.append(t.get("outcome"))
            new_exit_price.append(t.get("exit_price"))
            new_exit_time.append(t.get("exit_time_ist"))
            new_pnl_price.append(t.get("pnl_pct_price"))
            new_bars.append(t.get("bars_held"))
            new_sl_pct.append(sl_new)
            new_tgt_pct.append(tgt_new)
            continue

        res = er.resolve(
            bars, side, float(t["entry_price"]),
            t["entry_time_ist"], sl_new, tgt_new,
        )
        if res is None:
            n_missing_bars += 1
            new_outcome.append(t.get("outcome"))
            new_exit_price.append(t.get("exit_price"))
            new_exit_time.append(t.get("exit_time_ist"))
            new_pnl_price.append(t.get("pnl_pct_price"))
            new_bars.append(t.get("bars_held"))
            new_sl_pct.append(sl_new)
            new_tgt_pct.append(tgt_new)
            continue

        n_re += 1
        new_outcome.append(res.outcome)
        new_exit_price.append(res.exit_price)
        new_exit_time.append(res.exit_time_ist)
        new_pnl_price.append(res.pnl_pct_price)
        new_bars.append(res.bars_held)
        new_sl_pct.append(sl_new)
        new_tgt_pct.append(tgt_new)

    out["outcome"] = new_outcome
    out["exit_price"] = new_exit_price
    out["exit_time_ist"] = new_exit_time
    out["pnl_pct_price"] = new_pnl_price
    out["bars_held"] = new_bars
    out["candE3_sl_pct"] = new_sl_pct
    out["candE3_tgt_pct"] = new_tgt_pct
    out["pnl_pct_gross_price"] = out["pnl_pct_price"]
    out["pnl_pct"] = pd.to_numeric(out["pnl_pct_price"], errors="coerce") * leverage
    out["pnl_rs"] = (
        pd.to_numeric(out["pnl_pct"], errors="coerce") / 100.0
        * pd.to_numeric(out.get("position_size_rs", 20000.0), errors="coerce")
    )
    if "candE_size_mult" in out.columns:
        out["pnl_pct_eff"] = (
            pd.to_numeric(out["pnl_pct"], errors="coerce")
            * pd.to_numeric(out["candE_size_mult"], errors="coerce").fillna(1.0)
        )
        out["pnl_rs_eff"] = (
            out["pnl_pct_eff"] / 100.0
            * pd.to_numeric(out.get("position_size_rs", 20000.0), errors="coerce")
        )

    print(
        f"  re-resolved {n_re} | missing pick {n_missing_pick} | missing bars {n_missing_bars}"
    )
    return out


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--trades", required=True)
    p.add_argument("--mae-mfe", required=True)
    p.add_argument("--parquet-dir", required=True)
    p.add_argument("--output", required=True)
    p.add_argument("--leverage", type=float, default=5.0)
    args = p.parse_args()

    trades = pd.read_csv(args.trades)
    picks = pd.read_csv(args.mae_mfe)
    pq = Path(args.parquet_dir)
    if not pq.is_dir():
        raise SystemExit(f"parquet dir not found: {pq}")

    print(f"v17D re-resolve with MAE/MFE picks — {len(trades)} input trades")
    out = reresolve(trades, picks, pq, leverage=args.leverage)
    out.to_csv(args.output, index=False)

    win = (out["outcome"] == "TARGET").mean() * 100
    sl = (out["outcome"] == "SL").mean() * 100
    eod = (out["outcome"] == "EOD").mean() * 100
    print(f"  outcome mix: TARGET {win:.1f}% | SL {sl:.1f}% | EOD {eod:.1f}%")
    print(f"Wrote: {args.output}")


if __name__ == "__main__":
    main()
