"""Replay retained V13-V10-G with Rs3 lakh allocated capital per trade."""
from __future__ import annotations

import argparse
import hashlib
import json
from dataclasses import replace
from pathlib import Path

import numpy as np
import pandas as pd

import fno_v13_v10_g_backtest as g
from fno_v13_v10_d_research import detailed_daily

SOURCE = g.DEFAULT_OUTPUT / "final" / "V13_V10_G"
MANIFEST = g.DEFAULT_OUTPUT / "research_manifest.json"
DEFAULT_OUTPUT = g.DEFAULT_OUTPUT.parent / "run_20260914_capital_3l_portfolio_15l"


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def replay(*, allocation: float, portfolio: float, leverage: float) -> tuple[pd.DataFrame, pd.DataFrame, dict, list[str]]:
    if not np.isfinite(allocation) or allocation <= 0:
        raise ValueError("Allocation per trade must be finite and positive")
    if not np.isfinite(portfolio) or portfolio < allocation:
        raise ValueError("Portfolio capital must fund at least one trade")
    if not np.isfinite(leverage) or leverage <= 0:
        raise ValueError("Leverage must be finite and positive")
    manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
    artifacts = {key.replace("\\", "/"): value for key, value in manifest["artifacts"].items()}
    artifact_key = "final/V13_V10_G/selected_trades.csv"
    source_path = SOURCE / "selected_trades.csv"
    if sha256(source_path) != artifacts[artifact_key]:
        raise RuntimeError("Frozen V13-V10-G selected-trade ledger drift")
    source = pd.read_csv(source_path, float_precision="round_trip")
    day_source = g.DEFAULT_OUTPUT / "daily_detailed.csv"
    day_key = "daily_detailed.csv"
    if sha256(day_source) != artifacts[day_key]:
        raise RuntimeError("Frozen V13-V10-G session calendar drift")
    days = pd.read_csv(day_source).day.astype(str).tolist()
    if len(days) != 31 or len(source) != 73 or int(source.filled.sum()) != 66:
        raise RuntimeError("Unexpected retained G source shape")
    trades = g.v9.v5.apply_fixed_capital_model(source.copy(), allocation, leverage)
    cfg = replace(g.v9.V9Config(), capital_per_entry_rupees=allocation,
                  portfolio_capital_rupees=portfolio, leverage_factor=leverage,
                  max_positions=None, cost_bps=5.)
    ledger, summary = g.v9.v6.apply_portfolio_constraints(trades, cfg.portfolio_config())
    return trades, ledger, summary, days


def run(output: Path, *, allocation: float, portfolio: float, leverage: float) -> None:
    output.mkdir(parents=True, exist_ok=True)
    trades, ledger, summary, days = replay(allocation=allocation, portfolio=portfolio, leverage=leverage)
    original = pd.read_csv(SOURCE / "portfolio_trades.csv", float_precision="round_trip")
    if not ledger[["sid", "setup_id"]].equals(original[["sid", "setup_id"]]):
        raise RuntimeError("Trade identity changed during capital replay")
    if not ledger.filled.equals(original.filled):
        raise RuntimeError("Trigger/fill results changed during capital replay")

    scale = allocation / 100_000.
    original_exec = original.portfolio_executed.astype(bool)
    if not ledger.portfolio_executed.equals(original_exec):
        raise RuntimeError("Rs15 lakh capacity changed the retained execution set")
    np.testing.assert_allclose(
        ledger.portfolio_net_profit_rupees.to_numpy(float),
        original.portfolio_net_profit_rupees.to_numpy(float) * scale,
        atol=1e-6, rtol=0,
    )
    g.r.save(output / "result", trades, ledger, summary)
    daily = detailed_daily(ledger, days)
    daily.to_csv(output / "daily_detailed.csv", index=False)

    groups = {
        "ALL_31_SESSIONS": days,
        "JULY": [day for day in days if day.startswith("2026-07")],
        "AUGUST": [day for day in days if day.startswith("2026-08")],
        "SEPTEMBER_1_11": [day for day in days if day.startswith("2026-09")],
    }
    period_rows = []
    for name, period_days in groups.items():
        period_rows.append(dict(period=name, **g.r.metric(ledger, period_days)))
    periods = pd.DataFrame(period_rows)
    periods.to_csv(output / "period_summary.csv", index=False)

    executed = ledger.loc[ledger.portfolio_executed].copy()
    executed.to_csv(output / "executed_trades_detailed.csv", index=False)
    setup = executed.groupby(["setup_id", "side"], dropna=False).agg(
        trades=("sid", "size"),
        wins=("portfolio_net_profit_rupees", lambda value: int((value > 1e-9).sum())),
        losses=("portfolio_net_profit_rupees", lambda value: int((value < -1e-9).sum())),
        gross_profit_rupees=("portfolio_net_profit_rupees", lambda value: float(value[value > 0].sum())),
        gross_loss_rupees=("portfolio_net_profit_rupees", lambda value: float(-value[value < 0].sum())),
        net_profit_rupees=("portfolio_net_profit_rupees", "sum"),
    ).reset_index()
    setup["win_rate_pct"] = setup.wins / setup.trades * 100
    setup["profit_factor"] = setup.gross_profit_rupees.div(setup.gross_loss_rupees.replace(0, np.nan))
    setup.to_csv(output / "setup_summary.csv", index=False)
    exits = executed.groupby("exit_reason", dropna=False).agg(
        trades=("sid", "size"), net_profit_rupees=("portfolio_net_profit_rupees", "sum")
    ).reset_index()
    exits.to_csv(output / "exit_reason_summary.csv", index=False)

    headline = periods.loc[periods.period.eq("ALL_31_SESSIONS")].iloc[0]
    config = {
        "strategy": "V13-v10-G retained",
        "capital_per_trade_rupees": allocation,
        "leverage_factor": leverage,
        "gross_exposure_per_trade_rupees": allocation * leverage,
        "portfolio_capital_rupees": portfolio,
        "capacity_in_simultaneous_allocations": int(portfolio // allocation),
        "cost_bps_round_trip": 5.,
        "selection_entry_exit_rules": "Unchanged retained V13-v10-G",
        "partial_exits": False,
        "breakeven_stop": False,
        "sessions": len(days),
        "source_selected_orders": len(original),
        "source_fills": int(original.filled.sum()),
        "execution_set_matches_original_g": True,
        "rupee_results_scale_vs_rs1l": scale,
        "evidence": "Previously reviewed reused history; no untouched out-of-sample test",
    }
    (output / "configuration.json").write_text(json.dumps(config, indent=2), encoding="utf-8")

    daily_view = daily[["day", "selected", "triggered", "executed", "wins", "losses", "win_rate_pct",
                        "profit_factor", "net_profit_rupees", "cumulative_net_profit_rupees",
                        "drawdown_rupees", "targets", "stops", "time_exits"]]
    report = "\n\n".join([
        "# V13-v10-G — Rs3 lakh capital per trade",
        "This is a sizing-only replay of the retained G ledger. Selection, entry, stop, target, ranking, 10-minute trigger expiry, 15:15 square-off, full exits and 5 bps modeled round-trip cost are unchanged.",
        f"Assumptions: Rs{allocation:,.0f} allocated capital per trade, {leverage:g}x modeled leverage (Rs{allocation*leverage:,.0f} gross exposure per filled trade), and Rs{portfolio:,.0f} portfolio capital. The book can reserve {int(portfolio//allocation)} concurrent entries.",
        "## Headline and monthly results",
        periods[["period", "selected_orders", "trades", "wins", "losses", "win_rate_pct", "profit_factor",
                 "net_profit_rupees", "daily_close_drawdown_rupees", "median_trade_rupees"]].to_markdown(index=False, floatfmt=".2f"),
        "## Daywise detailed results",
        daily_view.to_markdown(index=False, floatfmt=".2f"),
        "## Setup-wise results",
        setup.to_markdown(index=False, floatfmt=".2f"),
        "## Exit-reason results",
        exits.to_markdown(index=False, floatfmt=".2f"),
        f"The replay executes {int(headline.trades)} trades from {int(headline.selected_orders)} selected orders. Peak concurrent positions are {summary['peak_concurrent_positions']}; peak reserved capital is Rs{summary['peak_reserved_capital_rupees']:,.0f}. No retained G trade is rejected by the Rs15 lakh capital limit.",
        "Because the execution set is unchanged and position sizing is exactly 3x, win rate and profit factor are unchanged while every rupee P&L and realized daily-close drawdown is exactly 3x the Rs1 lakh-allocation result.",
        "This is historical modeled execution on reused development history, not an out-of-sample result. Slippage beyond the 5 bps model, liquidity limits at Rs15 lakh gross exposure per trade, taxes and actual futures lot sizing can materially reduce realized performance. Drawdown is based on daily realized closes, not intraday mark-to-market.",
    ]) + "\n"
    (output / "V13_V10_G_3L_CAPITAL_DETAILED_RESULTS.md").write_text(report, encoding="utf-8")
    artifacts = {str(path.relative_to(output)): sha256(path) for path in sorted(output.rglob("*")) if path.is_file()}
    manifest = {"complete": True, "source_manifest": str(MANIFEST), "source_selected_trades_sha256": sha256(SOURCE / "selected_trades.csv"),
                "configuration": config, "artifacts": artifacts}
    (output / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    print(periods.to_string(index=False))
    print(json.dumps({**config, "headline": headline.to_dict(), "portfolio_summary": summary}, indent=2, default=str))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--allocation-per-trade", type=float, default=300_000.)
    parser.add_argument("--portfolio-capital", type=float, default=1_500_000.)
    parser.add_argument("--leverage", type=float, default=5.)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()
    run(args.output_dir, allocation=args.allocation_per_trade,
        portfolio=args.portfolio_capital, leverage=args.leverage)


if __name__ == "__main__":
    main()
