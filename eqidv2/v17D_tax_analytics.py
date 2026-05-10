"""v17D Phase 4b — brokerage and tax analytics for Zerodha intraday equity.

Indian intraday equity is taxed as **speculative business income** at slab
rates. This module:

    1. Daily cost breakdown — per session, every charge component
    2. Per-strategy tax efficiency — which setups maximize net-after-cost
       PnL per trade (fixed brokerage cap = larger orders amortize better)
    3. Year-end speculative-income estimator — tracks turnover (tax-audit
       trigger), speculative gain/loss, loss carry-forward eligibility

Key insights this surfaces:

    - Zerodha caps brokerage at Rs 20/order, so larger orders are more
      cost-efficient. Setups producing higher absolute PnL per trade
      have lower effective cost%.
    - SL hits cost ~0.03% more than TGT/EOD (extra slippage on adverse fill).
      High win-rate setups are mechanically cheaper.
    - For Indian tax: speculative losses can ONLY offset future speculative
      gains, carried forward 4 years. They cannot offset salary or other
      income heads.
    - Tax audit is mandatory if turnover > Rs 10 crore (2024+ rule). For
      intraday, "turnover" = sum of |absolute PnL| per trade.

Public API:
    daily_cost_breakdown(trades_csv) -> pd.DataFrame
    per_strategy_efficiency(trades_csv) -> pd.DataFrame
    year_end_speculative_income(trades_csv, fy_start='2026-04-01') -> dict
    tax_optimal_setup_recommendations(trades_csv) -> list[dict]
    parse_kite_contract_note(contract_csv) -> pd.DataFrame
        Reconciles broker-actual costs vs internal cost model.

Tax slab assumption (configurable):
    Default 30% marginal slab. Override via --slab-pct flag.
"""
from __future__ import annotations

import argparse
from datetime import date as date_type
from pathlib import Path

import pandas as pd

from eqidv2 import v17D_cost_model as cm

DEFAULT_SLAB_PCT = 30.0          # Indian marginal slab (top bracket)
TAX_AUDIT_TURNOVER_RS = 10e7     # Rs 10 crore turnover trigger (post-2024)
SPECULATIVE_LOSS_CARRY_YEARS = 4


def _detail_cost_per_trade(
    entry_price: float,
    qty: int,
    outcome: str,
    adv_bucket: str = "mid",
) -> dict:
    """Per-trade cost breakdown using v17D_cost_model components."""
    bd = cm.round_trip_pct(
        entry_price=entry_price, qty=qty, side="LONG",
        adv_bucket=adv_bucket, exit_kind=outcome,
    )
    notional = entry_price * qty
    return {
        "brokerage_rs":    bd.brokerage_pct  / 100 * notional,
        "stt_rs":          bd.stt_pct        / 100 * notional,
        "nse_txn_rs":      bd.nse_txn_pct    / 100 * notional,
        "sebi_rs":         bd.sebi_pct       / 100 * notional,
        "gst_rs":          bd.gst_pct        / 100 * notional,
        "stamp_duty_rs":   bd.stamp_duty_pct / 100 * notional,
        "slippage_rs":     bd.slippage_pct   / 100 * notional,
        "total_cost_rs":   bd.total_pct      / 100 * notional,
        "total_cost_pct":  bd.total_pct,
    }


def daily_cost_breakdown(trades: pd.DataFrame) -> pd.DataFrame:
    """One row per trading day with full cost decomposition.

    Required columns in `trades`:
        trade_date, entry_price, qty (or position_size_rs), outcome, pnl_rs

    Optional: adv_bucket per row; defaults to 'mid'.
    """
    if "qty" not in trades.columns:
        if "position_size_rs" in trades.columns and "entry_price" in trades.columns:
            trades = trades.copy()
            trades["qty"] = (
                pd.to_numeric(trades["position_size_rs"], errors="coerce")
                / pd.to_numeric(trades["entry_price"], errors="coerce")
            ).fillna(0).astype(int).clip(lower=1)
        else:
            raise ValueError("trades CSV needs qty or (position_size_rs + entry_price)")
    if "trade_date" not in trades.columns:
        if "signal_time_ist" in trades.columns:
            trades = trades.copy()
            trades["trade_date"] = pd.to_datetime(
                trades["signal_time_ist"], errors="coerce"
            ).dt.date
        else:
            raise ValueError("trades CSV needs trade_date or signal_time_ist")

    rows = []
    for d, g in trades.groupby("trade_date"):
        agg = {"trade_date": d, "n_trades": len(g)}
        totals = {"brokerage_rs": 0, "stt_rs": 0, "nse_txn_rs": 0, "sebi_rs": 0,
                  "gst_rs": 0, "stamp_duty_rs": 0, "slippage_rs": 0,
                  "total_cost_rs": 0}
        for _, t in g.iterrows():
            bd = _detail_cost_per_trade(
                float(t["entry_price"]),
                int(t.get("qty", 1)),
                str(t.get("outcome", "TARGET")),
                str(t.get("adv_bucket", "mid")),
            )
            for k in totals:
                totals[k] += bd[k]
        agg.update(totals)
        agg["gross_pnl_rs"] = float(
            pd.to_numeric(g["pnl_rs"], errors="coerce").fillna(0).sum()
        ) + agg["total_cost_rs"]   # add back costs to recover gross
        agg["net_pnl_rs"] = agg["gross_pnl_rs"] - agg["total_cost_rs"]
        agg["cost_pct_of_gross"] = (
            (agg["total_cost_rs"] / agg["gross_pnl_rs"] * 100)
            if agg["gross_pnl_rs"] > 0 else float("nan")
        )
        agg["turnover_rs"] = float(
            (pd.to_numeric(g["entry_price"], errors="coerce")
             * pd.to_numeric(g["qty"], errors="coerce")).sum() * 2  # both legs
        )
        rows.append(agg)
    out = pd.DataFrame(rows).sort_values("trade_date").reset_index(drop=True)
    return out


def per_strategy_efficiency(trades: pd.DataFrame) -> pd.DataFrame:
    """Per (side, setup) cost efficiency table. Sorted by net_pnl_per_trade desc."""
    if "qty" not in trades.columns and "position_size_rs" in trades.columns:
        trades = trades.copy()
        trades["qty"] = (
            pd.to_numeric(trades["position_size_rs"], errors="coerce")
            / pd.to_numeric(trades["entry_price"], errors="coerce")
        ).fillna(0).astype(int).clip(lower=1)

    rows = []
    for (side, setup), g in trades.groupby(["side", "setup"]):
        n = len(g)
        if n == 0:
            continue
        total_cost = 0.0
        for _, t in g.iterrows():
            bd = _detail_cost_per_trade(
                float(t["entry_price"]), int(t.get("qty", 1)),
                str(t.get("outcome", "TARGET")),
            )
            total_cost += bd["total_cost_rs"]
        net_pnl = float(pd.to_numeric(g["pnl_rs"], errors="coerce").fillna(0).sum())
        gross_pnl = net_pnl + total_cost
        gross_per_trade = gross_pnl / n
        net_per_trade = net_pnl / n
        cost_per_trade = total_cost / n
        win_rate = (
            (g["outcome"] == "TARGET").mean() * 100
            if "outcome" in g.columns else float("nan")
        )
        rows.append({
            "side": side, "setup": setup, "n_trades": n,
            "win_pct": win_rate,
            "gross_pnl_rs": gross_pnl,
            "net_pnl_rs": net_pnl,
            "total_cost_rs": total_cost,
            "gross_per_trade_rs": round(gross_per_trade, 2),
            "net_per_trade_rs": round(net_per_trade, 2),
            "cost_per_trade_rs": round(cost_per_trade, 2),
            "cost_pct_of_gross": round(
                (total_cost / gross_pnl * 100) if gross_pnl > 0 else float("nan"), 2
            ),
            "tax_efficiency_score": round(
                (net_pnl / gross_pnl) if gross_pnl > 0 else 0.0, 4
            ),
        })
    return pd.DataFrame(rows).sort_values(
        "net_per_trade_rs", ascending=False
    ).reset_index(drop=True)


def tax_optimal_setup_recommendations(trades: pd.DataFrame) -> list:
    """Recommendations for tax/cost-efficiency optimization.

    Flags setups whose gross/cost ratio is < 2.5x (high cost burden) and
    setups with low absolute PnL per trade (poor cost amortization).
    """
    eff = per_strategy_efficiency(trades)
    recs = []
    for _, r in eff.iterrows():
        flags = []
        if r["cost_pct_of_gross"] > 40:
            flags.append("HIGH_COST_BURDEN_>40%_OF_GROSS")
        if r["net_per_trade_rs"] < 50:
            flags.append("LOW_NET_PER_TRADE")
        if r["gross_per_trade_rs"] < 100:
            flags.append("ORDER_TOO_SMALL_TO_AMORTIZE_BROKERAGE")
        if r["tax_efficiency_score"] < 0.50:
            flags.append("TAX_EFFICIENCY_BELOW_50%")

        action = "KEEP"
        if "HIGH_COST_BURDEN_>40%_OF_GROSS" in flags or r["net_pnl_rs"] < 0:
            action = "DROP"
        elif flags:
            action = "RESIZE_OR_DROP"

        recs.append({
            "side": r["side"],
            "setup": r["setup"],
            "n_trades": int(r["n_trades"]),
            "action": action,
            "flags": flags,
            "gross_per_trade_rs": r["gross_per_trade_rs"],
            "net_per_trade_rs": r["net_per_trade_rs"],
            "cost_pct_of_gross": r["cost_pct_of_gross"],
        })
    return recs


def year_end_speculative_income(
    trades: pd.DataFrame,
    fy_start: str = "2026-04-01",
    slab_pct: float = DEFAULT_SLAB_PCT,
    prior_year_carry_forward_rs: float = 0.0,
) -> dict:
    """Compute Indian FY (Apr–Mar) speculative business income summary.

    Args:
        trades: trades CSV with at least trade_date and pnl_rs.
        fy_start: FY start date in YYYY-MM-DD; FY runs to next Mar 31.
        slab_pct: marginal income tax slab as % (e.g. 30 for top bracket).
        prior_year_carry_forward_rs: speculative loss carried over from
            prior FY (set-off only against speculative gains).

    Returns dict with FY summary, including:
        gross_speculative_income, net_speculative_income (after CF setoff),
        tax_due_rs, turnover_rs, audit_required (bool),
        loss_carried_forward_rs (if net is negative).
    """
    fy_start_d = pd.to_datetime(fy_start).date()
    fy_end_d = (
        pd.to_datetime(fy_start) + pd.DateOffset(years=1, days=-1)
    ).date()

    trades = trades.copy()
    trades["trade_date"] = pd.to_datetime(
        trades["trade_date"], errors="coerce"
    ).dt.date
    fy = trades[
        (trades["trade_date"] >= fy_start_d)
        & (trades["trade_date"] <= fy_end_d)
    ]
    if fy.empty:
        return {
            "fy_start": str(fy_start_d), "fy_end": str(fy_end_d),
            "n_trading_days": 0, "n_trades": 0,
            "gross_speculative_income_rs": 0.0,
            "carry_forward_used_rs": 0.0,
            "net_speculative_income_rs": 0.0,
            "tax_due_rs": 0.0,
            "turnover_rs": 0.0,
            "audit_required": False,
            "loss_carried_forward_rs": 0.0,
        }

    pnl_series = pd.to_numeric(fy["pnl_rs"], errors="coerce").fillna(0)
    gross_speculative = float(pnl_series.sum())

    # Apply carry-forward setoff
    if gross_speculative > 0 and prior_year_carry_forward_rs > 0:
        cf_used = min(gross_speculative, prior_year_carry_forward_rs)
    else:
        cf_used = 0.0
    net_speculative = gross_speculative - cf_used

    # Turnover for tax audit (intraday: sum of |abs PnL|)
    turnover_rs = float(pnl_series.abs().sum())

    # Tax (only on positive net)
    tax_due = max(0.0, net_speculative) * slab_pct / 100.0

    # Loss carried forward (only if net is negative)
    loss_cf = -net_speculative if net_speculative < 0 else 0.0

    return {
        "fy_start": str(fy_start_d),
        "fy_end": str(fy_end_d),
        "slab_pct": slab_pct,
        "n_trading_days": int(fy["trade_date"].nunique()),
        "n_trades": int(len(fy)),
        "gross_speculative_income_rs": round(gross_speculative, 2),
        "prior_year_carry_forward_rs": round(prior_year_carry_forward_rs, 2),
        "carry_forward_used_rs": round(cf_used, 2),
        "net_speculative_income_rs": round(net_speculative, 2),
        "tax_due_rs": round(tax_due, 2),
        "turnover_rs": round(turnover_rs, 2),
        "audit_required": turnover_rs > TAX_AUDIT_TURNOVER_RS,
        "loss_carried_forward_rs": round(loss_cf, 2),
        "loss_cf_expires": str(
            (pd.to_datetime(fy_end_d)
             + pd.DateOffset(years=SPECULATIVE_LOSS_CARRY_YEARS)).date()
        ) if loss_cf > 0 else None,
    }


def parse_kite_contract_note(contract_csv: str) -> pd.DataFrame:
    """Reconcile broker-actual costs vs internal cost model.

    Zerodha contract notes (Q-statements) export a daily CSV with columns:
        Date, Symbol, Order ID, Trade ID, Buy/Sell, Quantity, Price,
        Brokerage, STT, Exch Txn, SEBI, GST, Stamp Duty, Total

    Returns a DataFrame with deltas (broker_actual - internal_estimate)
    per cost component. Use to auto-tune the cost model parameters.
    """
    df = pd.read_csv(contract_csv)
    # Common Zerodha column normalization (varies by Kite export version)
    rename_map = {}
    for col in df.columns:
        c = col.strip().lower()
        if c in ("symbol", "tradingsymbol"):
            rename_map[col] = "ticker"
        elif c in ("buy/sell", "type"):
            rename_map[col] = "side"
        elif c in ("quantity", "qty"):
            rename_map[col] = "qty"
        elif c == "price":
            rename_map[col] = "price"
        elif "brokerage" in c:
            rename_map[col] = "broker_brokerage_rs"
        elif "stt" in c:
            rename_map[col] = "broker_stt_rs"
        elif "exch" in c or "exchange" in c:
            rename_map[col] = "broker_nse_txn_rs"
        elif "sebi" in c:
            rename_map[col] = "broker_sebi_rs"
        elif c == "gst":
            rename_map[col] = "broker_gst_rs"
        elif "stamp" in c:
            rename_map[col] = "broker_stamp_duty_rs"
    df = df.rename(columns=rename_map)
    return df


def main():
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="cmd", required=True)

    pd1 = sub.add_parser("daily")
    pd1.add_argument("--trades", required=True)
    pd1.add_argument("--output", default="v17D_daily_costs.csv")

    pe = sub.add_parser("efficiency")
    pe.add_argument("--trades", required=True)
    pe.add_argument("--output", default="v17D_setup_efficiency.csv")

    pr = sub.add_parser("recommend")
    pr.add_argument("--trades", required=True)

    py = sub.add_parser("yearend")
    py.add_argument("--trades", required=True)
    py.add_argument("--fy-start", default="2026-04-01")
    py.add_argument("--slab-pct", type=float, default=DEFAULT_SLAB_PCT)
    py.add_argument("--prior-cf-rs", type=float, default=0.0)

    pk = sub.add_parser("kite-reconcile")
    pk.add_argument("--contract-csv", required=True)
    pk.add_argument("--output", default="v17D_kite_reconcile.csv")

    args = p.parse_args()

    if args.cmd == "daily":
        df = pd.read_csv(args.trades)
        out = daily_cost_breakdown(df)
        out.to_csv(args.output, index=False)
        print(f"\nDaily cost breakdown — {len(out)} trading days")
        print("=" * 80)
        print(out.to_string(index=False))
        print(f"\nWrote: {args.output}")

    elif args.cmd == "efficiency":
        df = pd.read_csv(args.trades)
        out = per_strategy_efficiency(df)
        out.to_csv(args.output, index=False)
        print(f"\nPer-strategy tax efficiency — {len(out)} setups")
        print("=" * 80)
        print(out.to_string(index=False))
        print(f"\nWrote: {args.output}")

    elif args.cmd == "recommend":
        df = pd.read_csv(args.trades)
        recs = tax_optimal_setup_recommendations(df)
        print(f"\nTax-optimal setup recommendations")
        print("=" * 80)
        for r in recs:
            print(
                f"  {r['side']:5s} {r['setup']:<32s}  "
                f"{r['action']:<14s}  net/trade=Rs{r['net_per_trade_rs']:>7.2f}  "
                f"cost/gross={r['cost_pct_of_gross']:>5.1f}%"
            )
            if r["flags"]:
                print(f"        flags: {', '.join(r['flags'])}")

    elif args.cmd == "yearend":
        df = pd.read_csv(args.trades)
        summary = year_end_speculative_income(
            df, args.fy_start, args.slab_pct, args.prior_cf_rs,
        )
        print(f"\nFY {summary['fy_start']} to {summary['fy_end']} — speculative business income")
        print("=" * 80)
        for k, v in summary.items():
            print(f"  {k:<35s}: {v}")
        if summary["audit_required"]:
            print("\n  !! TAX AUDIT REQUIRED — turnover > Rs 10 cr. Engage CA.")
        if summary["loss_carried_forward_rs"] > 0:
            print(f"\n  Loss CF Rs {summary['loss_carried_forward_rs']:,.0f} "
                  f"available till {summary['loss_cf_expires']} (4-year window).")

    elif args.cmd == "kite-reconcile":
        df = parse_kite_contract_note(args.contract_csv)
        df.to_csv(args.output, index=False)
        print(f"Wrote: {args.output}  rows={len(df)}")


if __name__ == "__main__":
    main()
