"""Read-only audit of selected fixed-stop V13-v10-G-2 variants."""
from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pandas as pd


SWEEP_PATH = Path(__file__).with_name("v13_g2_sl_sweep.py")
spec = importlib.util.spec_from_file_location("sl_sweep", SWEEP_PATH)
sweep = importlib.util.module_from_spec(spec)
assert spec.loader is not None
spec.loader.exec_module(sweep)


def build_ledger(stop_pct: float, published, segments):
    trade_frames = []
    for _, orders, paths in segments:
        trades, _, _ = sweep.ext._simulate(
            orders, paths, published["base"], stop_pct=stop_pct
        )
        trade_frames.append(trades)
    all_trades = pd.concat(trade_frames, ignore_index=True, sort=False)
    ledger, portfolio = sweep.g2.g.v9.v6.apply_portfolio_constraints(
        all_trades, published["base"].portfolio_config()
    )
    return ledger, portfolio


def daywise(ledger: pd.DataFrame, days):
    executed = ledger.loc[ledger["portfolio_executed"].eq(True)].copy()
    executed["_day"] = pd.to_datetime(executed["day"]).dt.strftime("%Y-%m-%d")
    executed["_pnl"] = pd.to_numeric(
        executed["portfolio_net_profit_rupees"], errors="coerce"
    ).fillna(0.0)
    executed["_gross"] = pd.to_numeric(
        executed["portfolio_gross_profit_rupees"], errors="coerce"
    ).fillna(0.0)
    executed["_cost"] = pd.to_numeric(
        executed["portfolio_cost_rupees"], errors="coerce"
    ).fillna(0.0)
    records = []
    cumulative = 0.0
    peak = 0.0
    for day in map(str, days):
        frame = executed.loc[executed["_day"].eq(day)]
        net = float(frame["_pnl"].sum())
        cumulative += net
        peak = max(peak, cumulative)
        records.append(
            {
                "day": day,
                "trades": int(len(frame)),
                "wins": int(frame["_pnl"].gt(1e-9).sum()),
                "losses": int(frame["_pnl"].lt(-1e-9).sum()),
                "gross": float(frame["_gross"].sum()),
                "cost": float(frame["_cost"].sum()),
                "net": net,
                "cumulative": cumulative,
                "drawdown": peak - cumulative,
            }
        )
    return records


def main():
    published, segments, days = sweep.prepared_segments()
    payload = {"days": [str(day) for day in days], "variants": {}}
    for stop in (1.0, 1.25, 2.75):
        ledger, portfolio = build_ledger(stop, published, segments)
        daily = daywise(ledger, days)
        metric = sweep.metrics(ledger, days, stop)
        payload["variants"][str(stop)] = {
            "metric": metric,
            "portfolio": portfolio,
            "daily": daily,
        }

    base = payload["variants"]["1.0"]["daily"]
    diffs = {}
    for other in ("1.25", "2.75"):
        comparable = payload["variants"][other]["daily"]
        diffs[other] = [
            {
                "day": left["day"],
                "net_1.0": left["net"],
                f"net_{other}": right["net"],
                f"delta_{other}_vs_1.0": right["net"] - left["net"],
                "wins_1.0": left["wins"],
                f"wins_{other}": right["wins"],
                "losses_1.0": left["losses"],
                f"losses_{other}": right["losses"],
            }
            for left, right in zip(base, comparable)
            if abs(right["net"] - left["net"]) > 1e-9
            or right["wins"] != left["wins"]
            or right["losses"] != left["losses"]
        ]
    payload["different_days_vs_1.0"] = diffs
    print(json.dumps(payload, indent=2, allow_nan=False))


if __name__ == "__main__":
    main()
