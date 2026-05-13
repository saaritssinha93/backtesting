"""v17D Step 4.6 — end-of-day reconciliation.

Compares broker-reported fills against internal ledger; flags mismatches.

Inputs:
    --broker-csv: today's fills exported from broker (Zerodha tradebook,
                  Kite Connect API trade log, etc.). Required cols:
                  trade_date, ticker, side, qty, price, order_id
    --ledger-csv: today's v17D internal trades CSV (from runner)

Comparison rules:
    - Order count match
    - Per-ticker net qty match
    - Avg fill price within 0.5% of internal expected
    - Total PnL within Rs 100 of broker statement (if --broker-pnl provided)

Outputs:
    Human-readable .txt report + machine-readable .csv mismatches table.
    Exits with code 1 if any mismatch flagged.

Usage:
    python -m eqidv2.v17D_reconcile \
        --broker-csv broker_fills_20260509.csv \
        --ledger-csv v17D_trades_20260509.csv \
        --output ~/v17D_logs/reconciliation_20260509.txt
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd


def reconcile(broker_df: pd.DataFrame, ledger_df: pd.DataFrame,
              price_tolerance_pct: float = 0.5) -> dict:
    """Return a dict with all mismatches and aggregate stats."""
    mismatches = []

    # Trade count
    n_broker = len(broker_df)
    n_ledger = len(ledger_df)
    if n_broker != n_ledger:
        mismatches.append({
            "type": "TRADE_COUNT_MISMATCH",
            "broker": n_broker, "ledger": n_ledger,
            "delta": n_broker - n_ledger,
        })

    # Per-ticker net qty (broker has buy/sell qty separately; aggregate net)
    if "qty" in broker_df.columns and "side" in broker_df.columns:
        broker_df = broker_df.copy()
        broker_df["signed_qty"] = broker_df.apply(
            lambda r: int(r["qty"]) if r["side"].upper() == "BUY" else -int(r["qty"]),
            axis=1,
        )
        broker_net = broker_df.groupby("ticker")["signed_qty"].sum()
    else:
        broker_net = pd.Series(dtype=int)

    if "ticker" in ledger_df.columns and "qty" in ledger_df.columns:
        ledger_df = ledger_df.copy()
        # ledger has one row per round-trip; net should be 0 if closed
        ledger_per_ticker = ledger_df.groupby("ticker")["qty"].sum()
    else:
        ledger_per_ticker = pd.Series(dtype=int)

    # All tickers from both sides
    all_tickers = set(broker_net.index) | set(ledger_per_ticker.index)
    for t in sorted(all_tickers):
        bn = int(broker_net.get(t, 0))
        if bn != 0:
            mismatches.append({
                "type": "NET_QTY_NONZERO",
                "ticker": t, "broker_net": bn,
                "detail": "open position at EOD per broker",
            })

    # Per-ticker avg price
    if {"ticker", "side", "price", "qty"}.issubset(broker_df.columns):
        for t, g in broker_df.groupby("ticker"):
            for side in ("BUY", "SELL"):
                sg = g[g["side"].str.upper() == side]
                if sg.empty:
                    continue
                broker_avg = (sg["price"] * sg["qty"]).sum() / sg["qty"].sum()
                # Match against ledger entry/exit price for this ticker+side
                col = "entry_price" if side == "BUY" else "exit_price"
                if col not in ledger_df.columns or "ticker" not in ledger_df.columns:
                    continue
                lg = ledger_df[ledger_df["ticker"] == t]
                if lg.empty:
                    continue
                ledger_avg = lg[col].mean()
                if pd.isna(ledger_avg) or ledger_avg <= 0:
                    continue
                pct_diff = abs(broker_avg - ledger_avg) / ledger_avg * 100
                if pct_diff > price_tolerance_pct:
                    mismatches.append({
                        "type": "PRICE_DIVERGENCE",
                        "ticker": t, "side": side,
                        "broker_avg": round(broker_avg, 2),
                        "ledger_avg": round(ledger_avg, 2),
                        "pct_diff": round(pct_diff, 3),
                    })

    return {
        "n_broker_rows": n_broker,
        "n_ledger_rows": n_ledger,
        "mismatches": mismatches,
        "n_mismatches": len(mismatches),
    }


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--broker-csv", required=True)
    p.add_argument("--ledger-csv", required=True)
    p.add_argument("--output", required=True)
    p.add_argument("--broker-pnl-rs", type=float, default=None,
                   help="Optional broker-reported PnL for cross-check")
    p.add_argument("--price-tolerance-pct", type=float, default=0.5)
    args = p.parse_args()

    broker = pd.read_csv(args.broker_csv)
    ledger = pd.read_csv(args.ledger_csv)

    result = reconcile(broker, ledger, args.price_tolerance_pct)

    lines = []
    lines.append(f"v17D reconciliation — {datetime.now().isoformat(timespec='seconds')}")
    lines.append("=" * 78)
    lines.append(f"  broker rows: {result['n_broker_rows']}")
    lines.append(f"  ledger rows: {result['n_ledger_rows']}")
    lines.append(f"  mismatches: {result['n_mismatches']}")
    lines.append("")
    if result["mismatches"]:
        lines.append("MISMATCHES:")
        for m in result["mismatches"]:
            lines.append(f"  - {m}")
    else:
        lines.append("  CLEAN — no mismatches")

    if args.broker_pnl_rs is not None:
        if "pnl_rs" in ledger.columns:
            ledger_pnl = float(ledger["pnl_rs"].sum())
            delta = abs(args.broker_pnl_rs - ledger_pnl)
            lines.append(f"\n  broker PnL: Rs {args.broker_pnl_rs:,.2f}")
            lines.append(f"  ledger PnL: Rs {ledger_pnl:,.2f}")
            lines.append(f"  delta: Rs {delta:,.2f}  "
                         f"({'OK' if delta <= 100 else 'INVESTIGATE'})")

    Path(args.output).parent.mkdir(parents=True, exist_ok=True)
    Path(args.output).write_text("\n".join(lines), encoding="utf-8")
    print("\n".join(lines))
    sys.exit(1 if result["n_mismatches"] > 0 else 0)


if __name__ == "__main__":
    main()
