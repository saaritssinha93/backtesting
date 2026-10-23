"""Read verified G8 outputs and write a separate, explicitly retrospective review.

Does not change the fitted policy, its experiment manifest, or any strategy.
"""
from __future__ import annotations

import argparse
import html
import json
from pathlib import Path

import numpy as np
import pandas as pd

from research.g8_nifty_vix_backtest import verify, sha, dump, BASELINE


def review(run_dir):
    root = Path(run_dir).resolve()
    verify(root)
    summary = json.loads((root / "summary.json").read_text(encoding="utf-8"))
    trials = pd.read_csv(root / "parameter_trials.csv")
    overall = trials.loc[trials.period.eq("all")].set_index("policy", drop=False)
    raw_high_hit = overall.reset_index(drop=True).sort_values(
        ["win_rate_pct", "net_pnl_rs"], ascending=False).iloc[0].policy
    best_adaptive = overall.loc[overall.exit.str.contains("stress_wider|vix_ratio")].sort_values(
        "net_pnl_rs", ascending=False).iloc[0].policy
    labels = {
        "Copied G7 V2 baseline": BASELINE,
        "Chronologically selected G8": summary["selected_policy"],
        "Best historical profit (exploratory)": summary["exploratory_best_net"],
        "Highest historical win rate (unconstrained)": raw_high_hit,
        "High win rate with >=60% profit retention": summary["exploratory_high_win_rate"],
        "Best historical VIX-adjusted exits": best_adaptive,
    }
    comparison = pd.DataFrame([dict(label=label, **overall.loc[policy].to_dict())
        for label, policy in labels.items()])
    baseline = overall.loc[BASELINE]
    comparison["net_change_vs_baseline_rs"] = comparison.net_pnl_rs - baseline.net_pnl_rs
    comparison["net_profit_retained_pct"] = comparison.net_pnl_rs / baseline.net_pnl_rs * 100.
    out = root / "review"
    out.mkdir(exist_ok=True)
    comparison.to_csv(out / "decision_comparison.csv", index=False)
    daily = pd.read_csv(root / "daywise_comparison.csv")
    changes = daily.loc[daily.selected_net_change_rs.abs().gt(1e-6),
        ["day", "G7_V2_baseline_net_pnl_rs", "G8_selected_net_pnl_rs", "selected_net_change_rs"]]
    changes.to_csv(out / "changed_days.csv", index=False)
    b = pd.read_csv(root / "G7_V2_baseline/portfolio_trades.csv").set_index("sid")
    s = pd.read_csv(root / "G8_selected/portfolio_trades.csv").set_index("sid")
    affected = b.loc[b.portfolio_executed & ~s.portfolio_executed,
        ["day", "setup_id", "tradingsymbol", "side", "portfolio_net_profit_rupees"]]
    affected.to_csv(out / "baseline_trades_avoided.csv")
    full_comparison = pd.read_csv(root / "overall_comparison.csv")
    test = full_comparison.loc[full_comparison.period.eq("final_test")].set_index("label")
    final_delta = float(test.loc["G8_selected", "net_pnl_rs"] - test.loc["G7_V2_baseline", "net_pnl_rs"])
    wf = full_comparison.loc[full_comparison.period.eq("walk_forward_last28")].set_index("label")
    wf_delta = float(wf.loc["past_only_walk_forward", "net_pnl_rs"] - wf.loc["matched_walk_forward_baseline", "net_pnl_rs"])
    validation_days = summary["splits"]["validation"]
    validation_changed = int(((b.portfolio_executed != s.portfolio_executed) & b.day.isin(validation_days)).sum())
    # This interval describes daily variability only, not selection uncertainty.
    daily_delta = daily.selected_net_change_rs.to_numpy()
    rng = np.random.default_rng(20261022)
    starts = rng.integers(0, len(daily_delta), size=(10000, int(np.ceil(len(daily_delta) / 5))))
    indices = (starts[:, :, None] + np.arange(5)) % len(daily_delta)
    totals = daily_delta[indices.reshape(10000, -1)[:, :len(daily_delta)]].sum(axis=1)
    interval = np.percentile(totals, [2.5, 97.5]).tolist()
    conclusion = dict(
        recommendation="Keep this as research; do not replace the baseline based on this sample.",
        research_policy_unchanged=summary["selected_policy"],
        final_internal_test_net_change_rs=final_delta,
        past_only_walk_forward_net_change_rs=wf_delta,
        validation_execution_changes=validation_changed,
        selected_losing_days=int(overall.loc[summary["selected_policy"], "losing_days"]),
        baseline_losing_days=int(baseline.losing_days),
        highest_unconstrained_win_rate_policy=raw_high_hit,
        high_win_rate_report_qualification="The main report's exploratory_high_win_rate retains >=60% baseline net profit and >=65% trades; it is NOT the unconstrained maximum.",
        daily_block_bootstrap_95pct_total_delta_interval_rs=interval,
        bootstrap_caveat="Descriptive 5-session circular blocks, 10,000 resamples; NOT corrected for parameter selection or inherited historical fitting.",
        no_independent_holdout=True, execution_authority=False,
        original_run_manifest_sha256=sha(root / "manifest.json"))
    dump(out / "conclusion.json", conclusion)
    cols = ["label", "trades", "win_rate_pct", "net_pnl_rs", "net_change_vs_baseline_rs", "losing_days", "daily_close_drawdown_rs"]
    table = comparison[cols].to_html(index=False, float_format=lambda v: f"{v:,.2f}")
    changed_table = changes.to_html(index=False, float_format=lambda v: f"{v:,.2f}")
    slots = pd.read_csv(root / "slot_settings.csv")
    slot_table = slots[["slot", "side", "baseline_target_pct", "reference_stop_pct", "reference_target_pct", "timed_stop", "development_selected_orders"]].to_html(index=False)
    content = f"""<!doctype html><html><head><meta charset="utf-8"><title>G8 decision review — 22 October 2026</title>
    <style>body{{font:16px system-ui;max-width:1500px;margin:32px auto;padding:0 24px;color:#203347}}h1,h2{{color:#183f68}}
    table{{border-collapse:collapse;font-size:13px}}th,td{{padding:8px;border:1px solid #d8e1eb;text-align:right}}th{{background:#eaf1f8}}
    .scroll{{overflow:auto}}.note{{background:#fff1ce;padding:20px}}a{{color:#1754a0}}</style></head><body>
    <h1>G8 results: modest improvement, not a reliable loss-day filter</h1>
    <p>Copied G7 <b>V2</b>, not V3. Tested 230 combinations over 56 eligible sessions, 29 July–22 October 2026.
    All figures below include modeled 5bps round-trip cost. INR 10 lakh portfolio; 5x modeled position exposure.</p>
    <div class="note"><b>Recommendation: keep the new rule experimental.</b> The chronologically chosen filter adds only
    INR {overall.loc[summary['selected_policy'],'net_pnl_rs']-baseline.net_pnl_rs:,.2f} overall, leaves 7 losing days,
    and worsens daily-close drawdown. The final 11-session period loses INR {-final_delta:,.2f} relative to baseline;
    the matched past-only walk-forward loses INR{-wf_delta:,.2f}. This is not convincing evidence to replace the baseline.
    No strategy here produces definite profits.</div>
    <h2>Overall comparison</h2><div class="scroll">{table}</div>
    <p>“Best historical” rows are selected after seeing the full sample and are exploratory. The unconstrained highest-hit-rate
    variant uses a 0.35% target and 1.20% stop. Its better hit rate comes with much lower profit and more losing days.
    The separately reported high-win-rate alternative has a 60% baseline-profit-retention and 65% trade-retention constraint.</p>
    <h2>Exact selected entry rule, for every valid long/short slot</h2>
    <ol><li>Preserve every G7 V2 entry criterion, slot-side availability, ranking and quota.</li>
    <li>Reject when both completed Nifty-futures slot and session directions oppose the stock trade.</li>
    <li>If both VIX session change and completed recent 15-minute change are at least +1%, allow only if
    <b>both Nifty directions agree</b> with the trade: both up for a long, both down for a short.</li>
    <li>When that rising-VIX condition is absent, retain the original Nifty gate. Before 15 minutes of history are
    available, use the VIX session change and flag the fallback.</li></ol>
    <p>The rule avoided {len(affected)} baseline trades: {int(affected.portfolio_net_profit_rupees.lt(0).sum())} losers
    and {int(affected.portfolio_net_profit_rupees.gt(0).sum())} winners. It cannot identify losing trades perfectly.</p>
    <h2>SL and target decision</h2><p>The chronologically selected candidate retained the original exits:
    <b>1.20% initial SL; tighten to 1.00% after 120 minutes; existing per-slot targets 0.60–2.00%; 15:15 square-off.</b>
    G7 V3 partial exits were not used. VIX-adjusted exits were tested but did not establish a better overall result.
    These are retained research parameters, not a claim that they are universally optimal.</p>
    <p>Stress-wider alternatives used 1.50% SL with 80% baseline quantity when VIX session change was at least +1%
    and recent change at least +0.5%. Ratio alternatives scaled exits by current VIX/prior close, bounded 0.85–1.25.
    All decisions were fixed at entry; no active stop was widened retrospectively.</p>
    <h2>Every slot's selected stop and target</h2><div class="scroll">{slot_table}</div>
    <p>Samples are too small to justify independent optimization of 51 slots. Shared rules were tested;
    no slot was deleted simply because its observed trades lost.</p>
    <h2>Days changed by the selected filter</h2><div class="scroll">{changed_table}</div>
    <h2>What is verified, and what is not</h2><p>All 571 selected orders, original execution paths and baseline allocations reproduce
    the archived G7 V2 evidence. VIX candles are completed before confirmation. The 230-case grid was declared before outcome selection;
    development/validation/final periods were separated, and no final-period outcome selected parameters.
    Additional 7-session walk-forward blocks use only earlier training and validation days.</p>
    <p><b>Validation changed {validation_changed} trades:</b> passing the validation guard is not evidence of incremental benefit
    when the overlay did nothing during that period. This is a material reason not to claim a validated edge.</p>
    <p>However, the inherited strategy was already fitted after viewing this history. These are internal robustness checks,
    not a fresh holdout. The descriptive 5-day-block bootstrap interval for the selected total P&amp;L change is
    INR {interval[0]:,.0f} to INR {interval[1]:,.0f}; it includes zero and does not correct selection bias.</p>
    <p>One-minute stop-first/gap assumptions, fractional model quantities, transaction-cost assumptions, missing 1 October inputs,
    opening-OI coverage gaps and daily-close-only drawdown remain limitations. No live/paper settings were changed.</p>
    <p>VIX measures expected volatility, not a guaranteed stock direction or exact stop distance:
    <a href="https://www.nseindia.com/static/products-services/indices-indiavix-index">NSE India VIX explanation</a>.</p>
    <h2>Detailed evidence</h2><p><a href="../report.html">Full experiment report</a> |
    <a href="../daywise_comparison.csv">Day-wise CSV</a> | <a href="../slot_settings.csv">51-slot settings CSV</a> |
    <a href="../slot_side_comparison.csv">Slot-side results</a> | <a href="../parameter_trials.csv">All 230 tests</a> |
    <a href="baseline_trades_avoided.csv">Trades avoided</a> | <a href="../cost_sensitivity.csv">Cost sensitivity</a></p>
    </body></html>"""
    (out / "review.html").write_text(content, encoding="utf-8")
    dump(out / "manifest.json", dict(parent_run_manifest_sha256=sha(root / "manifest.json"),
        review_code_sha256=sha(__file__), files={p.name:sha(p) for p in out.iterdir() if p.is_file() and p.name != "manifest.json"}))
    print(json.dumps(conclusion, indent=2))
    print(out / "review.html")
    return out


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("run_dir")
    review(parser.parse_args().run_dir)
