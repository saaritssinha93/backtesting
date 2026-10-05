"""Focused risk diagnostics for selected rows of the fixed-stop sweep."""
from __future__ import annotations

import importlib.util
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
spec = importlib.util.spec_from_file_location("sl_sweep", Path(__file__).with_name("v13_g2_sl_sweep.py"))
module = importlib.util.module_from_spec(spec)
assert spec.loader is not None
spec.loader.exec_module(module)

published, segments, days = module.prepared_segments()
for stop in (0.75, 1.0, 1.15, 1.25, 1.5, 2.0, 2.45, 2.75):
    frames = []
    for _, orders, paths in segments:
        trades, _, _ = module.ext._simulate(orders, paths, published["base"], stop_pct=stop)
        frames.append(trades)
    trades = pd.concat(frames, ignore_index=True, sort=False)
    ledger, summary = module.g2.g.v9.v6.apply_portfolio_constraints(
        trades, published["base"].portfolio_config()
    )
    executed = ledger.loc[ledger.portfolio_executed.eq(True)].copy()
    pnl = pd.to_numeric(executed.portfolio_net_profit_rupees)
    pnl_9bps = (
        pd.to_numeric(executed.gross_return_pct) / 100.0
        * pd.to_numeric(executed.exposure_per_entry_rupees)
        - 9.0 / 10000.0 * pd.to_numeric(executed.exposure_per_entry_rupees)
    )
    gains_9bps = pnl_9bps.loc[pnl_9bps.gt(1e-9)].sum()
    losses_9bps = -pnl_9bps.loc[pnl_9bps.lt(-1e-9)].sum()
    worst = executed.loc[pnl.idxmin()]
    daily = executed.assign(_day=pd.to_datetime(executed.day).dt.strftime("%Y-%m-%d")).groupby("_day").portfolio_net_profit_rupees.sum()
    print({
        "stop_pct": stop,
        "peak_positions": summary["peak_concurrent_positions"],
        "peak_open_initial_risk_rupees": summary["peak_open_initial_risk_rupees"],
        "worst_trade_rupees": float(pnl.min()),
        "worst_trade_day": str(worst.day),
        "worst_trade_symbol": str(worst.tradingsymbol),
        "worst_trade_reason": str(worst.exit_reason),
        "worst_trade_gross_return_pct": float(worst.gross_return_pct),
        "most_adverse_mae_pct": float(pd.to_numeric(executed.mae_pct).min()),
        "worst_day_rupees": float(daily.min()),
        "worst_day": str(daily.idxmin()),
        "9bps_wins": int(pnl_9bps.gt(1e-9).sum()),
        "9bps_losses": int(pnl_9bps.lt(-1e-9).sum()),
        "9bps_win_rate_pct": float(pnl_9bps.gt(1e-9).mean() * 100.0),
        "9bps_profit_factor": float(gains_9bps / losses_9bps),
        "9bps_net_profit_rupees": float(pnl_9bps.sum()),
    })
