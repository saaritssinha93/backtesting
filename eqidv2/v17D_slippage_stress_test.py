"""v17D Step 0.1 — slippage stress test.

Re-evaluates an existing v17C trades CSV under multiple cost assumptions
to answer the binding question: is PF=2.16 real, or a slippage-arbitrage
illusion?

Usage:
    python -m eqidv2.v17D_slippage_stress_test \
        --trades outputs_v17C_noNF_live_5min/avwap_longshort_trades_*_candE3.csv \
        --output v17D_stress.csv

Cost model:
    By default uses v17D_cost_model (realistic Indian intraday costs).
    The CSV's existing `pnl_pct_price` is the LEVERED-OUT price-only return;
    we add cost columns for each scenario and recompute PF.

Cost scenarios:
    BASELINE   : v17C's assumed 0.16% / 0.19%             (sanity floor)
    REALISTIC  : v17D cost model with adv_bucket="mid"    (most likely live)
    PESSIMISTIC: v17D cost model with adv_bucket="long_tail" (worst-case)
    USER       : --cost-multiplier x BASELINE              (manual sweep)

Output: one summary row per scenario:
    scenario, pf, win_pct, day_win_pct, sum_pnl_pct, sum_pnl_rs, n_trades
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from eqidv2 import v17D_cost_model as cm


def _pf(s: pd.Series) -> float:
    s = pd.to_numeric(s, errors="coerce").dropna()
    w = float(s[s > 0].sum())
    l = float(-s[s < 0].sum())
    if l <= 0:
        return float("inf") if w > 0 else 0.0
    return w / l


def _scenario_costs_pct(scenario: str, exit_kind: str, mult: float = 1.0):
    """Per-trade costs (% of price) for a given scenario."""
    if scenario == "BASELINE":
        return (0.16 if exit_kind == "TARGET" else 0.19) * mult
    if scenario == "REALISTIC":
        return cm.costs_pct_for_v17C("mid", exit_kind)
    if scenario == "PESSIMISTIC":
        return cm.costs_pct_for_v17C("long_tail", exit_kind)
    if scenario == "OPTIMISTIC":
        return cm.costs_pct_for_v17C("top100", exit_kind)
    raise ValueError(f"unknown scenario {scenario!r}")


def stress_test(trades: pd.DataFrame, leverage: float = 5.0) -> pd.DataFrame:
    """Re-resolve PnL across all cost scenarios. Trades CSV must contain:
      - outcome (TARGET/SL/EOD)
      - pnl_pct_price (price-only return %, before leverage)
      - trade_date (or signal_time_ist parseable)
      - pnl_rs (optional, for sumRs)
    """
    df = trades.copy()
    if "pnl_pct_price" not in df.columns:
        # fall back: derive from pnl_pct / leverage
        if "pnl_pct" in df.columns:
            df["pnl_pct_price"] = pd.to_numeric(df["pnl_pct"], errors="coerce") / leverage
        else:
            raise ValueError("trades CSV missing pnl_pct_price and pnl_pct")
    if "trade_date" not in df.columns and "signal_time_ist" in df.columns:
        df["trade_date"] = pd.to_datetime(df["signal_time_ist"]).dt.date
    df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date

    rows = []
    scenarios = [
        ("BASELINE", 1.0),
        ("OPTIMISTIC", 1.0),
        ("REALISTIC", 1.0),
        ("PESSIMISTIC", 1.0),
    ]
    for label, mult in scenarios:
        sl_cost = _scenario_costs_pct(label, "SL", mult)
        tgt_cost = _scenario_costs_pct(label, "TARGET", mult)
        eod_cost = tgt_cost
        cost_per_row = df["outcome"].map(
            {"TARGET": tgt_cost, "SL": sl_cost, "EOD": eod_cost}
        ).fillna(tgt_cost)
        # gross price PnL minus cost; then re-leverage
        gross_price = pd.to_numeric(df["pnl_pct_price"], errors="coerce")
        net_price = gross_price - cost_per_row
        net_lev = net_price * leverage

        n = len(df)
        win_pct = (df["outcome"] == "TARGET").mean() * 100 if n else 0.0
        daily = pd.DataFrame({"date": df["trade_date"], "p": net_lev}) \
                  .groupby("date")["p"].sum()
        day_win_pct = (daily > 0).mean() * 100 if len(daily) else 0.0
        sum_pnl_pct = float(net_lev.sum())
        sum_rs = (
            float((net_lev / 100.0
                   * pd.to_numeric(df.get("position_size_rs", 20000.0), errors="coerce")
                  ).sum())
        )
        rows.append({
            "scenario": label,
            "tgt_cost_pct": tgt_cost,
            "sl_cost_pct": sl_cost,
            "n_trades": n,
            "pf": _pf(net_lev),
            "win_pct": win_pct,
            "day_win_pct": day_win_pct,
            "sum_pnl_pct": sum_pnl_pct,
            "sum_pnl_rs": sum_rs,
        })

    # Optional --cost-multiplier sweep
    return pd.DataFrame(rows)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--trades", required=True, help="path to v17C trades CSV")
    p.add_argument("--output", required=True, help="path to write stress summary CSV")
    p.add_argument("--leverage", type=float, default=5.0)
    args = p.parse_args()

    df = pd.read_csv(args.trades)
    summary = stress_test(df, leverage=args.leverage)
    summary.to_csv(args.output, index=False)

    print(f"\nv17D slippage stress test — {len(df)} trades from {args.trades}")
    print("=" * 78)
    for _, row in summary.iterrows():
        print(
            f"  {row['scenario']:12s}  "
            f"costs T/S={row['tgt_cost_pct']:.3f}/{row['sl_cost_pct']:.3f}%  "
            f"PF={row['pf']:.3f}  win={row['win_pct']:.1f}%  "
            f"day_win={row['day_win_pct']:.1f}%  "
            f"sumPnL={row['sum_pnl_pct']:>+8.1f}%  "
            f"Rs={row['sum_pnl_rs']:>+11,.0f}"
        )
    print()
    real_pf = float(summary[summary["scenario"] == "REALISTIC"]["pf"].iloc[0])
    print(f"Verdict: REALISTIC PF = {real_pf:.3f}")
    if real_pf < 1.30:
        print("  RED — strategy edge is largely a costs illusion. STOP.")
    elif real_pf < 1.50:
        print("  YELLOW — proceed with reduced expectations / pilot at 0.3x size.")
    else:
        print("  GREEN — proceed to Phase 1 build.")


if __name__ == "__main__":
    main()
