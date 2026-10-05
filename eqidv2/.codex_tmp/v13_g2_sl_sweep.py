"""Research-only fixed-stop sweep over sealed V13-v10-G selections through 2026-09-30."""
from __future__ import annotations

import json
from pathlib import Path
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import fno_v13_v10_g_2_backtest as g2
import fno_v13_v10_g_2_extend_results as ext
import fno_v13_v10_g_daily_replay as daily_replay


OUT = Path(__file__).with_name("v13_g2_sl_sweep_results.csv")
MONTHLY_OUT = Path(__file__).with_name("v13_g2_sl_sweep_monthly.csv")
SUMMARY_OUT = Path(__file__).with_name("v13_g2_sl_sweep_summary.json")


def prepared_segments():
    # Validate the already-published 1% output and its immutable base inputs first.
    published = ext._load_base(ext.DEFAULT_BASE_G2)
    sealed = g2.load_bundle(g2.DEFAULT_SOURCE_BUNDLE, g2.DEFAULT_G_CONFIG)
    if sealed["source"].resolve() != published["source"].resolve():
        raise RuntimeError("Base source mismatch")

    base_orders = ext._apply_retained_g_exits(sealed["orders"], published["source_g"])
    _, base_g_ledger, _ = ext._simulate(
        base_orders, sealed["paths"], published["base"], stop_pct=None
    )
    base_official = pd.read_csv(published["source"] / "g_backtest/portfolio_trades.csv")
    ext._assert_daily_parity(base_g_ledger, base_official, sealed["days"][-1])
    segments = [("BASE_TO_2026-09-23", base_orders, sealed["paths"])]

    for day in ext.COMPLETE_EXTENSION_DAYS:
        run, result = ext._latest_successful_run(ext.DEFAULT_DAILY_ROOT, day)
        source_manifest = ext._read_json(run / "source_manifest.json")
        expected_hash = g2.sha256(g2.DEFAULT_G_CONFIG)
        if source_manifest.get("frozen_config_sha256") != expected_hash:
            raise RuntimeError(f"Daily configuration mismatch for {day}")
        if source_manifest.get("source_fingerprint") != result.get("source_fingerprint"):
            raise RuntimeError(f"Daily fingerprint mismatch for {day}")

        snapshot_manifest_path, _ = ext._snapshot_for_day(
            ext.DEFAULT_DAILY_ROOT, day, run, result
        )
        snapshot_manifest = ext._read_json(snapshot_manifest_path)
        if snapshot_manifest.get("complete") is not True:
            raise RuntimeError(f"Incomplete daily input snapshot for {day}")
        daily_replay._verify_input_snapshot(snapshot_manifest_path.parent, snapshot_manifest)

        orders = ext._apply_retained_g_exits(
            pd.read_csv(run / "selected_orders.csv"), published["source_g"]
        )
        paths = ext._selected_paths(orders, day, snapshot_manifest_path.parent)
        _, g_ledger, _ = ext._simulate(orders, paths, published["base"], stop_pct=None)
        ext._assert_daily_parity(g_ledger, pd.read_csv(run / "portfolio_trades.csv"), day)
        segments.append((day.isoformat(), orders, paths))

    days = [*sealed["days"], *ext.COMPLETE_EXTENSION_DAYS]
    return published, segments, days


def metrics(ledger: pd.DataFrame, days, stop_pct: float):
    base = g2.g.r.metric(ledger, days)
    executed = ledger.loc[ledger["portfolio_executed"].eq(True)].copy()
    pnl = pd.to_numeric(executed["portfolio_net_profit_rupees"], errors="coerce").fillna(0.0)
    gross = pd.to_numeric(executed["portfolio_gross_profit_rupees"], errors="coerce").fillna(0.0)
    costs = pd.to_numeric(executed["portfolio_cost_rupees"], errors="coerce").fillna(0.0)
    day_pnl = (
        executed.assign(_day=pd.to_datetime(executed["day"]).dt.strftime("%Y-%m-%d"))
        .groupby("_day")["portfolio_net_profit_rupees"]
        .sum()
        .reindex([str(day) for day in days], fill_value=0.0)
    )
    active_days = day_pnl.loc[day_pnl.abs().gt(1e-9)]
    wins = pnl.loc[pnl.gt(1e-9)]
    losses = pnl.loc[pnl.lt(-1e-9)]
    exits = executed["exit_reason"].value_counts()
    return {
        "stop_pct": round(stop_pct, 4),
        **base,
        "gross_profit_rupees": float(gross.sum()),
        "cost_rupees": float(costs.sum()),
        "average_trade_rupees": float(pnl.mean()),
        "average_win_rupees": float(wins.mean()) if len(wins) else 0.0,
        "average_loss_rupees": float(losses.mean()) if len(losses) else 0.0,
        "payoff_ratio": float(wins.mean() / -losses.mean()) if len(losses) else None,
        "target_exits": int(exits.get("TARGET", 0)),
        "stop_exits": int(exits.get("STOP", 0)),
        "time_exits": int(exits.get("TIME_EXIT_1515", 0)),
        "positive_sessions": int(day_pnl.gt(1e-9).sum()),
        "negative_sessions": int(day_pnl.lt(-1e-9).sum()),
        "zero_sessions": int(day_pnl.abs().le(1e-9).sum()),
        "positive_session_pct_all": float(day_pnl.gt(1e-9).mean() * 100.0),
        "positive_session_pct_active": float(active_days.gt(1e-9).mean() * 100.0),
    }


def main():
    published, segments, days = prepared_segments()
    rows = []
    month_rows = []
    # A fine grid diagnoses local stability and also tests whether the apparent
    # win-rate optimum simply sits at the widest boundary.
    stops = np.round(np.arange(0.40, 3.0001, 0.05), 2)
    for stop in stops:
        trade_frames = []
        for _, orders, paths in segments:
            trades, _, _ = ext._simulate(
                orders, paths, published["base"], stop_pct=float(stop)
            )
            trade_frames.append(trades)
        all_trades = pd.concat(trade_frames, ignore_index=True, sort=False)
        ledger, _ = g2.g.v9.v6.apply_portfolio_constraints(
            all_trades, published["base"].portfolio_config()
        )
        rows.append(metrics(ledger, days, float(stop)))

        executed = ledger.loc[ledger["portfolio_executed"].eq(True)].copy()
        executed["month"] = pd.to_datetime(executed["day"]).dt.to_period("M").astype(str)
        for month, frame in executed.groupby("month", sort=True):
            pnl = pd.to_numeric(frame["portfolio_net_profit_rupees"], errors="coerce").fillna(0.0)
            gains = pnl.loc[pnl.gt(1e-9)].sum()
            losses = -pnl.loc[pnl.lt(-1e-9)].sum()
            month_rows.append({
                "stop_pct": round(float(stop), 4),
                "month": month,
                "trades": int(len(frame)),
                "wins": int(pnl.gt(1e-9).sum()),
                "losses": int(pnl.lt(-1e-9).sum()),
                "win_rate_pct": float(pnl.gt(1e-9).mean() * 100.0),
                "profit_factor": float(gains / losses) if losses > 1e-9 else None,
                "net_profit_rupees": float(pnl.sum()),
            })

    frame = pd.DataFrame(rows)
    frame.to_csv(OUT, index=False)
    pd.DataFrame(month_rows).to_csv(MONTHLY_OUT, index=False)
    summary = {
        "window": [str(days[0]), str(days[-1])],
        "sessions": len(days),
        "grid": {"minimum": 0.4, "maximum": 3.0, "step": 0.05},
        "best_win_rate": frame.loc[frame["win_rate_pct"].idxmax()].to_dict(),
        "best_net_profit": frame.loc[frame["net_profit_rupees"].idxmax()].to_dict(),
        "best_profit_factor": frame.loc[frame["profit_factor"].idxmax()].to_dict(),
        "lowest_drawdown": frame.loc[frame["daily_close_drawdown_rupees"].idxmin()].to_dict(),
        "outputs": [str(OUT.resolve()), str(MONTHLY_OUT.resolve())],
    }
    SUMMARY_OUT.write_text(json.dumps(summary, indent=2, allow_nan=False) + "\n", encoding="utf-8")
    print(frame.to_json(orient="records"))
    print(json.dumps(summary, indent=2, allow_nan=False))


if __name__ == "__main__":
    main()
