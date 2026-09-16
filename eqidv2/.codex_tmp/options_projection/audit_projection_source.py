"""Independent, read-only audit of the fixed options projection source.

Only audit artifacts next to this file are written. The source research files
and the requested HTML dashboard are never modified.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path("C:/TradingData/eqidv2/fno_oi/strategy_research/v13_corrected_v10_g")
SOURCE = ROOT / "options_3lots_5min_20260914_sl12p5_target25"
OUTPUT = Path(__file__).resolve().parent
HTML = ROOT / "V13_V10_G_INTERACTIVE_BACKTEST.html"


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def fees(premium, side):
    brokerage = 20.0
    exchange, sebi, ipft = premium * .0003553, premium * .000001, premium * .000000001
    stamp = premium * .00003 if side == "BUY" else 0.
    stt = premium * .0015 if side == "SELL" else 0.
    return brokerage + exchange + sebi + ipft + stamp + stt + .18 * (brokerage + exchange + sebi + ipft)


def run():
    checks = []
    source_hashes = {}

    def check(name, okay):
        checks.append({"name": name, "passed": bool(okay)})

    def verify(path, expected):
        actual = sha(path)
        source_hashes[str(path)] = actual
        check("sha256:" + str(path), actual == expected)

    manifest = json.loads((SOURCE / "manifest.json").read_text())
    source_hashes[str(SOURCE / "manifest.json")] = sha(SOURCE / "manifest.json")
    for name, expected in manifest["artifacts"].items():
        verify(SOURCE / name, expected)
    configuration = manifest["configuration"]
    original = Path(configuration["source_run"])
    verify(original / "manifest.json", configuration["source_manifest_sha256"])
    original_manifest = json.loads((original / "manifest.json").read_text())
    original_artifacts = {k.replace("\\", "/"): v for k, v in original_manifest["artifacts"].items()}
    verify(original / "frozen_input/inputs.json", original_artifacts["frozen_input/inputs.json"])
    frozen = original / "frozen_input"
    inputs = json.loads((frozen / "inputs.json").read_text())
    for name, expected in inputs["artifacts"].items():
        verify(frozen / name, expected)
    for name, expected in manifest["code_sha256"].items():
        verify(Path(name), expected)

    trades = pd.read_csv(SOURCE / "options_trades.csv", float_precision="round_trip")
    daily = pd.read_csv(SOURCE / "options_daily.csv", float_precision="round_trip")
    cash_events = pd.read_csv(SOURCE / "premium_cash_events.csv", float_precision="round_trip")
    summary = json.loads((SOURCE / "summary.json").read_text())
    closed = trades.loc[trades.portfolio_status.eq("ADMITTED") & trades.status.eq("CLOSED")].copy()
    window = daily.loc[daily.day.between("2026-08-26", "2026-09-11")].copy()
    prewindow = daily.loc[daily.day.lt("2026-08-26")]
    check("configured_lots_3", configuration["lots"] == 3)
    check("configured_stop_12_5_target_25", configuration["stop_pct"] == 12.5 and configuration["target_pct"] == 25)
    check("73_original_attempts", len(trades) == 73)
    check("20_closed_trades", len(closed) == 20)
    check("13_available_sample_sessions", len(window) == 13)
    check("18_prewindow_sessions_excluded", len(prewindow) == 18)
    check("zero_unresolved_admissions", not (trades.portfolio_status.eq("ADMITTED") & ~trades.status.eq("CLOSED")).any())
    check("historical_day_list_matches_frozen", daily.day.tolist() == inputs["days"])
    check("all_executions_in_window", closed.day.between("2026-08-26", "2026-09-11").all())
    check("fixed_three_lot_quantities", (closed.quantity == 3 * closed.lot_size).all())
    check("no_equity_leverage_in_premium", np.allclose(closed.entry_premium_outlay, closed.quantity * closed.entry_price, rtol=0, atol=1e-7))
    check("net_total", np.isclose(closed.net_pnl.sum(), 114943.08321153207, rtol=0, atol=1e-7))
    check("13_wins_7_losses", closed.net_pnl.gt(0).sum() == 13 and closed.net_pnl.lt(0).sum() == 7)
    check("daily_net_reconciles", np.allclose(closed.groupby("day").net_pnl.sum().reindex(window.day, fill_value=0), window.net_pnl, rtol=0, atol=1e-7))
    check("cash_reconciles_every_event", np.allclose(configuration["capital"] + cash_events.cash_change.cumsum(), cash_events.free_cash, rtol=0, atol=1e-7))
    check("free_cash_never_negative", cash_events.free_cash.ge(-1e-8).all())
    check("ending_cash_matches_summary", np.isclose(cash_events.free_cash.iloc[-1], summary["ending_free_cash"], rtol=0, atol=1e-7))
    check("projection_starts_after_backtest", np.isclose(summary["ending_free_cash"], configuration["capital"] + closed.net_pnl.sum(), rtol=0, atol=1e-7))

    reserve, peak = 0., 0.
    cost_by_trade = (closed.entry_price * closed.quantity + closed.entry_costs).set_axis(closed.trade_id).to_dict()
    for row in cash_events.itertuples():
        reserve += cost_by_trade[row.trade_id] * (1 if row.event == "BUY" else -1)
        peak = max(peak, reserve)
    check("premium_reserve_clears_at_end", abs(reserve) < 1e-7)
    check("premium_peak_matches_summary", np.isclose(peak, summary["peak_reserved_premium_and_fees"], rtol=0, atol=1e-7))

    for row in closed.itertuples():
        entry, exit, observed = [pd.Timestamp(x) for x in (row.entry_ts, row.exit_ts, row.exit_observed_ts)]
        event = cash_events.loc[cash_events.trade_id.eq(row.trade_id)]
        buy = event.loc[event.event.eq("BUY")].iloc[0]
        sell = event.loc[event.event.eq("SELL")].iloc[0]
        check(row.trade_id + ":net", np.isclose(row.gross_pnl - row.entry_costs - row.exit_costs, row.net_pnl, rtol=0, atol=1e-7))
        check(row.trade_id + ":gross", np.isclose((row.exit_price - row.entry_price) * row.quantity, row.gross_pnl, rtol=0, atol=1e-7))
        check(row.trade_id + ":entry_fee", np.isclose(fees(row.entry_price * row.quantity, "BUY"), row.entry_costs, rtol=0, atol=1e-7))
        check(row.trade_id + ":exit_fee", np.isclose(fees(row.exit_price * row.quantity, "SELL"), row.exit_costs, rtol=0, atol=1e-7))
        check(row.trade_id + ":cash_debit", np.isclose(buy.cash_change, -cost_by_trade[row.trade_id], rtol=0, atol=1e-7))
        check(row.trade_id + ":cash_credit", np.isclose(sell.cash_change, row.exit_price * row.quantity - row.exit_costs, rtol=0, atol=1e-7))
        check(row.trade_id + ":release_at_observed_exit", pd.Timestamp(sell.timestamp) == observed and observed >= exit >= entry)

    calendar = []
    for row in daily.itertuples():
        attempts = trades.loc[trades.day.eq(row.day)]
        executed = closed.loc[closed.day.eq(row.day)]
        partial = attempts.reason.eq("PREVIOUS_BAR_MISSING").sum()
        if row.day < "2026-08-26":
            classification = "EXCLUDED_BEFORE_OPTION_WINDOW"
        elif len(executed):
            classification = "EXECUTED_WITH_PARTIAL_PATH_EXCLUSIONS" if partial else "EXECUTED"
        elif not len(attempts):
            classification = "ZERO_NO_SELECTED_ORDERS"
        elif attempts.signal_status.eq("UNDERLYING_UNFILLED").all():
            classification = "ZERO_NO_UNDERLYING_TRIGGER"
        else:
            classification = "UNKNOWN_COVERAGE_NOT_VERIFIED_ZERO"
        calendar.append(dict(day=row.day, included=row.day >= "2026-08-26", classification=classification,
                             selected_orders=len(attempts), executed=len(executed),
                             missing_previous_bar=int(partial), modeled_net=float(executed.net_pnl.sum())))
    calendar = pd.DataFrame(calendar)
    eligible = calendar.loc[calendar.included]
    check("three_verified_zero_sessions", eligible.classification.str.startswith("ZERO_").sum() == 3)
    check("zero_sessions_exact", eligible.loc[eligible.classification.str.startswith("ZERO_"), "day"].tolist() == ["2026-09-03", "2026-09-04", "2026-09-08"])
    check("no_unknown_full_session_in_window", not eligible.classification.eq("UNKNOWN_COVERAGE_NOT_VERIFIED_ZERO").any())
    check("three_missing_path_attempts_retained_as_exclusions", eligible.missing_previous_bar.sum() == 3)
    check("40_triggered_prewindow_metadata_failures", len(trades.loc[trades.day.lt("2026-08-26") & trades.signal_status.eq("READY")]) == 40)

    positive = float(closed.gross_pnl.clip(lower=0).sum())
    negative = float(closed.gross_pnl.clip(upper=0).sum())
    scenarios = []
    for retained in [1., .75, .5, .2]:
        # Analytic fixed-cash, source-cost expectation; not a path forecast.
        net = positive * retained + negative - float(closed.total_costs.sum())
        scenarios.append(dict(positive_gross_retained=retained, sample_net_source_fees=net,
                              annual_net_252_over_13_source_fees=net * 252 / 13))
    result = dict(passed=all(c["passed"] for c in checks), checks_count=len(checks), checks=checks,
                  source_hashes=source_hashes, html_read_only_snapshot_sha256=sha(HTML),
                  initial_capital=configuration["capital"], projection_start_equity=summary["ending_free_cash"],
                  executed_trades=len(closed), sample_sessions=len(window), executed_days=closed.day.nunique(),
                  trades_per_available_session=len(closed)/len(window), model_252_session_mean_trades=len(closed)/len(window)*252,
                  positive_gross=positive, negative_gross=negative, sample_costs=float(closed.total_costs.sum()),
                  peak_premium_and_fees=peak, analytic_source_fee_expectations=scenarios,
                  notes=["Scenarios retain a fraction of positive gross P&L, not a win-rate percentage.",
                         "The 13-session sample includes three verified no-entry sessions; 10 is active-day count only.",
                         "Pre-window missing options history is excluded, never converted into zero returns.",
                         "Three PREVIOUS_BAR_MISSING exclusions remain unobserved opportunities, not evidence of no trade.",
                         "Retain fixed three lots. Full premium plus buy fees is required; release sale cash at exit_observed_ts.",
                         "Five-session circular resampling across 13 observations is hypothetical; percentile bands exclude unseen regimes.",
                         "Extra slippage must be labeled incremental because adverse market-fill slippage is already in source prices.",
                         "Do not scale brokerage linearly with quantity if offering optional monthly lot sizing."])
    OUTPUT.mkdir(parents=True, exist_ok=True)
    calendar.to_csv(OUTPUT / "source_calendar_audit.csv", index=False)
    (OUTPUT / "projection_source_audit.json").write_text(json.dumps(result, indent=2, allow_nan=False) + "\n", encoding="utf-8")
    print(json.dumps({k: result[k] for k in ["passed", "checks_count", "projection_start_equity", "sample_sessions", "trades_per_available_session", "model_252_session_mean_trades", "positive_gross", "negative_gross", "sample_costs", "analytic_source_fee_expectations"]}, indent=2))
    if not result["passed"]:
        raise AssertionError([c for c in checks if not c["passed"]])


if __name__ == "__main__":
    run()
