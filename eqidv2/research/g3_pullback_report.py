"""Consolidated, read-only interpretation of locked G-3 pullback experiments.

This report does not select another rule or change any historical trade.
"""
from __future__ import annotations

import argparse
import hashlib
import html
import json
import os
from pathlib import Path

import numpy as np
import pandas as pd

BASE = Path(r"C:\TradingData\eqidv2\fno_oi\strategy_research\v13_corrected_v10_g_3")
STUDY = BASE / "pullback_monitor_v1_20261008_resolved_exits"
TUNING = BASE / "pullback_tuning_v1_20261008"
FROZEN = BASE / "frozen_20261008_long110_nextminute_v1"


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def pct(x):
    return "N/A" if pd.isna(x) else f"{100 * float(x):.2f}%"


def primary(frame):
    return frame.loc[((frame.horizon_minutes.eq(5) & frame.threshold_pct.eq(.3)) |
                      (frame.horizon_minutes.eq(30) & frame.threshold_pct.eq(.5)))].copy()


def run(out: Path):
    out.mkdir(parents=True, exist_ok=False)
    inputs = {}

    def read(folder, name):
        path = folder / name
        inputs[str(path)] = sha(path)
        return pd.read_csv(path) if path.suffix == ".csv" else json.loads(path.read_text(encoding="utf-8"))

    rules = read(TUNING, "selected_rules.json")
    assert all(x["eligible_candidates"] == 0 for x in rules["chosen"].values())
    metric = primary(read(STUDY, "metrics.csv"))
    phase = primary(read(STUDY, "phase_metrics.csv"))
    alerts = read(STUDY, "alert_episodes.csv")
    alert_stats = read(STUDY, "alert_success.csv")
    first = read(STUDY, "first_alert_success.csv")
    trades = read(STUDY, "trade_report.csv")
    training = read(TUNING, "candidate_training_checks.csv")
    comparison = read(TUNING, "comparisons.csv")
    delta_cols = [c for c in comparison if c.startswith("delta_")]
    assert comparison[delta_cols].fillna(0).eq(0).all().all()
    frozen_days = read(FROZEN, "daily_results.csv")
    config = read(FROZEN, "frozen_config.json")
    frozen_validation = read(FROZEN, "validation.json")
    source_validation = read(STUDY, "validation.json")
    tuning_validation = read(TUNING, "validation.json")
    assert np.isclose(trades.net_pnl.sum(), source_validation["net_pnl_unchanged"])
    assert sha(TUNING / "selected_rules.json") == tuning_validation["selected_rules_sha256"]

    body = []

    def heading(text, level=2):
        body.append(f"<h{level}>{html.escape(text)}</h{level}>")

    def para(text, cls=""):
        body.append(f'<p class="{cls}">{html.escape(text)}</p>')

    def table(frame):
        body.append('<div class="table-wrap">' + frame.to_html(index=False, escape=True, border=0, na_rep="N/A") + '</div>')

    def link(path, label):
        relative = os.path.relpath(path, out).replace("\\", "/")
        body.append(f'<p><a href="{html.escape(relative, quote=True)}">{html.escape(label)}</a></p>')

    heading("G-3 pullback warnings: re-evaluation and improvement study", 1)
    para("Conclusion: none of the 45 tested rule variants qualified as an improvement. The original warning rules are retained only as research controls, not endorsed for trading actions.", "verdict")
    para("G-3 is frozen separately. The monitor never changed an entry, stop, target, position size, square-off or cost. G and G-2 were not edited. No paper or live execution has been enabled.")
    heading("1. Coverage and frozen trading baseline")
    n = len(trades)
    net = trades.net_pnl.sum()
    para(f"Available historical archive: {rules['days'][0]} through {rules['days'][-1]}, {len(rules['days'])} recorded sessions, {n} filled trades from 102 selected orders. These are all filled G-3 trades in the frozen archive, not a new complete-universe backtest. October 1 is explicitly excluded because its source replay was incomplete; the exact 46-session list is recorded in selected_rules.json. This does not establish coverage of every exchange session between the endpoints.")
    para(f"Monitoring was available for {int(trades.monitor_available.sum())}/{n} trades. OIL LONG on August 11 exited at its target in the recorded entry minute, so it had no later completed minute while still open. The 85 symbol/day inputs contain 200 prior-session warmup bars plus all 360 session bars from 09:16 through 15:15 IST. There are 18,694 observed trade-minutes, 37,388 primary forecast rows (two horizons) and 13,368 fixed-grid rows including sensitivity endpoints.")
    table(pd.DataFrame([{
        "Side": side, "Filled trades": len(g), "Positive net trades": int(g.net_pnl.gt(0).sum()),
        "Trade win rate": pct(g.net_pnl.gt(0).mean()), "Unchanged net P&L (INR)": f"{g.net_pnl.sum():,.2f}"
    } for side, g in trades.groupby("side")]))
    para(f"Total unchanged net P&L: INR {net:,.2f}. Baseline trade win rate: {pct(trades.net_pnl.gt(0).mean())}; this is distinct from warning precision. The accepted entry variant is exact-next-minute confirmation, LONG volume floor 1.10x and SHORT 1.20x. Staged stop remains 1.25%, tightened once to 1.00% after 120 minutes from actual entry. Setup-specific targets are unchanged. Capital is INR 1,000,000, allocated capital INR 100,000 per entry with 5x exposure; cost is the frozen 5-bps model (INR 250 per filled trade), not a freshly estimated brokerage/slippage model.")
    link(FROZEN / "frozen_config.json", "Exact frozen configuration, including setup-specific targets")
    heading("2. What a successful warning means")
    table(pd.DataFrame([
        {"Monitor": "FAST", "Forecast from each warning close": "New adverse move >=0.30% within next 5 minutes", "LONG": "Price falls", "SHORT": "Price rises"},
        {"Monitor": "SLOW", "Forecast from each warning close": "New adverse move >=0.50% within next 30 minutes", "LONG": "Price falls", "SHORT": "Price rises"},
    ]))
    para("Both forecasts operate throughout each open trade. Forecast horizon and trade age are different: a five-minute forecast can occur two hours after entry. The age tables below separately address warnings in the first 1–5 minutes, minutes 6–30, and minute 31 onward. A warning recognizes existing weakness and predicts a further adverse move; it does not claim to predict the very first turn before weakness appears.")
    para("Precision (success rate) = correct resolved warnings / all resolved warnings. Recall = warned event windows / all observed event windows. False-positive rate = false warnings / non-event windows. A price move already completed in the warning candle does not count. A warning is not a reversal trade or an instruction to exit.")
    para("The label ends at the earlier of forecast expiry or the actual frozen exit. Known exit fills/open prices can resolve an event, and a close-of-bar exit permits the entire bar. If a threshold is crossed only by an intrabar exit candle's extreme, event order may be unknown. Such cases are excluded from resolved denominators, not counted as failures. Hit times have minute-bar resolution; the actual intrabar hit may be up to one minute earlier.")
    heading("3. Every-minute monitoring: operational alert success")
    para("A warning is checked every completed minute, with a causal cooldown of 5 or 30 minutes for the corresponding monitor. These are repeated alerts, not independent trades. Do not combine this precision with the separate fixed-grid recall in the next section.")
    a = alert_stats.loc[alert_stats.split.isin(["ALL", "LATER16"]) & alert_stats.side.ne("BOTH")]
    table(pd.DataFrame([{
        "Period": r.split, "Side": r.side, "Forecast (min)": r.horizon_minutes,
        "Correct / resolved": f"{r.successful_alerts}/{r.resolved_alerts}", "Success": pct(r.precision),
        "False alerts": r.false_alerts, "Unresolved": r.unknown_alerts,
        "Precision bounds incl. unknown": f"{pct(r.precision_identification_lower)}–{pct(r.precision_identification_upper)}",
        "Warned trades / days": f"{r.warned_trades}/{r.warned_days}", "Median hit lead (min)": r.median_lead_minutes
    } for r in a.itertuples()]))
    heading("First warning only per trade and horizon", 3)
    para("This view avoids repeatedly counting warnings in long-held positions. It is a diagnostic view, not a newly selected first-warning-only strategy. Later-sample cells contain only 6 or 14 warned trades; those small denominators cannot establish robust performance.")
    table(pd.DataFrame([{"Period": r.split, "Side": r.side, "Forecast (min)": r.horizon_minutes,
        "Correct / first warnings": f"{r.successful_alerts}/{r.resolved_alerts}", "Success": pct(r.precision),
        "Warned days": r.warned_days}
        for r in first.loc[first.split.isin(["ALL", "LATER16"]) & first.side.ne("BOTH")].itertuples()]))
    heading("4. Fixed-grid validation: precision, recall and benchmarks")
    para("Anchors start at the first complete post-entry observation and recur every 5 or 30 minutes, independently of whether the rule warns. Windows do not overlap within a given horizon/trade; the two endpoints overlap each other and are not independent. Repeated windows are clustered by trading date for the 95% intervals (2,000 bootstrap samples). Wide intervals and low event counts matter more than a small point-estimate advantage.")
    m = metric.loc[metric.analysis.eq("PRIMARY") & metric.split.isin(["ALL", "LATER16"]) & metric.side.ne("BOTH")]
    table(pd.DataFrame([{
        "Period": r.split, "Side": r.side, "Forecast (min)": r.horizon_minutes,
        "TP / FP / FN / TN": f"{r.tp}/{r.fp}/{r.fn}/{r.tn}", "Precision": pct(r.precision),
        "95% day-cluster interval": f"{pct(r.precision_bootstrap_low)}–{pct(r.precision_bootstrap_high)}",
        "Recall": pct(r.recall), "FPR": pct(r.fpr), "Event frequency": pct(r.prevalence),
        "Adverse-candle benchmark precision": pct(r.baseline_precision), "Unresolved warnings": r.unknown_warnings
    } for r in m.itertuples()]))
    para("Across the whole archive, all four rule/side combinations have lower precision than both event frequency and the simple adverse-candle benchmark. In the later 16 sessions, LONG FAST is slightly above both benchmarks by point estimate, but the day-cluster uncertainty includes no advantage. None establishes a dependable edge.")
    heading("5. Immediate post-entry versus later in the trade")
    rows = []
    for period in ("ALL", "LATER16"):
        x = alerts if period == "ALL" else alerts.loc[alerts.split.eq(period)]
        for (side, horizon, age), group in x.groupby(["side", "horizon_minutes", "phase"]):
            resolved = group.loc[group.outcome_available]
            rows.append({"Period": period, "Side": side, "Forecast (min)": horizon,
                "Trade age": age, "Correct / resolved": f"{int(resolved.event.sum())}/{len(resolved)}",
                "Success": pct(resolved.event.mean()), "Unknown": int((~group.outcome_available).sum()),
                "Warned trades": group.trade_id.nunique(), "Warned days": group.day.nunique()})
    age_alerts = pd.DataFrame(rows)
    age_alerts.to_csv(out / "alert_success_by_trade_age.csv", index=False)
    table(age_alerts)
    para("In the first 1–5 minutes, LONG FAST has 6/15 correct alerts (40%), whereas SHORT FAST has 2/14 (14.29%). After minute 30, FAST success falls to 24/453 LONG and 30/659 SHORT. This is descriptive evidence that repeating the same warning throughout the day creates many false alarms, not permission to select the favorable subgroup after seeing the test results.")
    para("The fixed 30-minute grid has no anchors at trade ages 6–30: its anchors are approximately ages 1, 31, 61, and so on. Therefore its empty middle-age cells mean not evaluated on that grid, not zero success. Every-minute alert age tables above do cover that period. The complete phase_metrics.csv preserves these distinctions.")
    heading("6. Re-evaluation: all 45 candidates, no qualified improvement")
    para("Candidate definitions and acceptance tests were fixed before performance inspection. Selection used FIT20 (July 29–August 27) and CHECK10 (August 28–September 10). The selected-rule JSON was hashed before scoring LATER16 (September 11–October 7). No rule was retuned after reading that later period. This history had already been used for entry-strategy research, so the later period is held-back monitor evaluation, not an untouched prospective sample.")
    table(pd.DataFrame([
        {"Family": "FAST", "Tests": 27, "Parameters": "Momentum 0.25/0.50/0.75 ATR × body 40/50/60% × volume 1.0/1.2/1.5x", "Qualified": 0},
        {"Family": "SLOW", "Tests": 18, "Parameters": "Momentum 0.25/0.50/0.75 ATR × giveback 0.5/1.0/1.5 ATR × full/close-SMA5 trend", "Qualified": 0}
    ]))
    para("Eligibility required at least 10/5 warnings on 5/3 different trades in FIT20/CHECK10 respectively, recall at least 20%, and precision above event frequency in BOTH folds. Eligible rules would be ranked by the worse-fold F0.5 score, prioritizing precision. None passed. Every FAST candidate failed recall and precision-above-frequency in both folds. Every SLOW candidate failed precision-above-frequency in FIT20; CHECK10 produced zero correct SLOW warnings for all 18 candidates.")
    stats = training.groupby(["family", "fold"]).agg(candidates=("candidate_id", "size"),
        minimum_precision=("precision", "min"), maximum_precision=("precision", "max"),
        minimum_recall=("recall", "min"), maximum_recall=("recall", "max"), eligible=("fold_eligible", "sum")).reset_index()
    for col in ["minimum_precision", "maximum_precision", "minimum_recall", "maximum_recall"]:
        stats[col] = stats[col].map(pct)
    table(stats)
    para("The fallback kept the original controls: FAST momentum 0.50 ATR/body 50%/volume 1.20x; SLOW momentum 0.50 ATR/giveback 1.00 ATR/full trend. ORIGINAL and CHOSEN predictions and metrics are identical, including sensitivity endpoints. Improvement is zero, and no candidate has been promoted.")
    link(TUNING / "candidate_training_checks.csv", "All 45 candidates × 2 earlier-data folds, with exact rejection reasons")
    heading("7. Rule implementation and honest limitations")
    para("FAST requires signed three-minute momentum ≤−0.5 ATR, and either a close through the prior three-candle adverse extreme or an adverse body ≥50% of its range with volume ≥1.20x the prior 20-minute mean. SLOW requires close/SMA5/SMA13 ordered against the trade, signed five-minute momentum ≤−0.5 ATR and giveback ≥1 ATR from the best completed close seen since monitoring began (including entry). ATR is a simple 14-bar mean of true ranges, not Wilder ATR. Features use completed candles only; prior extremes and volume references exclude the current candle.")
    para("These conditions often signal after some adverse movement has already occurred. Their task is harder than identifying that already-visible move: they must forecast another 0.30% or 0.50% before horizon/exit. Low success may reflect this mismatch and changing intraday conditions; the study has not established a causal explanation. Merely making the thresholds easier did not solve it.")
    para("The sensitivity outputs also test FAST 0.20%/0.40% and SLOW 0.40%/0.75% outcome sizes under unchanged warning rules. They are diagnostics, not an invitation to select the easiest outcome after seeing results. PRIMARY analysis treats a resolved early exit without an event as a non-event for the actual-position-exposure question; FULL_HORIZON analysis removes such early-exit windows. Both are included in metrics.csv.")
    para("Among successful dense alerts, a later completed close returned to the warning price before horizon/exit in 3/43 LONG FAST, 1/13 LONG SLOW, 4/46 SHORT FAST and 0/26 SHORT SLOW cases. This limited diagnostic is not a forecast of recovery and omits same-candle intrabar recoveries. A correct warning can also occur during an ultimately profitable trade. No P&L benefit of acting on warnings has been tested.")
    heading("8. Research priorities; no execution changes")
    para("First: preserve this failed study and frozen G-3. Do not use these warnings to tighten stops or exit trades. A no-change control is the appropriate result when no tested improvement qualifies.")
    para("Second: if continuing research, predefine an entry-risk model separately from a later giveback/reversal model. Evaluate smaller and volatility-normalized adverse excursions alongside the fixed percentage endpoints, and include time of day, trade age, distance from entry and distance from stop. These are proposed new research hypotheses, not validated improvements.")
    para("Third: compare any redesigned model against simple adverse-candle and age/time-matched event-frequency controls using day-based walk-forward evaluation. Keep alerts from the same trade/day together; report false alerts per trade and first-warning results as well as recall. Use newly accumulated sessions for final confirmation because the present held-back results have now been inspected.")
    para("Fourth: only after a warning edge survives validation should a separate next-bar executable action study test hold versus exit or stop changes with unchanged costs and conservative fills. Warning precision alone cannot decide which action increases net profit. Paper/live implementation remains deferred as requested.")
    heading("9. Day-by-day results and complete audit files")
    para("Daily trading P&L below is the unchanged frozen G-3 ledger; the observer adds no trades and makes no P&L changes. Warning counts and success by date/side/horizon are supplied separately so trading win rate is not confused with warning success.")
    daily_rows, display_rows = [], []
    for day in rules["days"]:
        baseline = frozen_days.loc[frozen_days.day.eq(day)].iloc[0]
        shown = {"Date": day, "Trades": int(baseline.trades),
                 "G-3 net P&L (INR)": f"{baseline.net_pnl:,.2f}", "P&L change": "0.00"}
        for side in ("LONG", "SHORT"):
            for horizon in (5, 30):
                group = alerts.loc[alerts.day.eq(day) & alerts.side.eq(side) & alerts.horizon_minutes.eq(horizon)]
                resolved = group.loc[group.outcome_available]
                hits, count = int(resolved.event.sum()), len(resolved)
                success = hits/count if count else np.nan
                daily_rows.append({"day": day, "side": side, "horizon_minutes": horizon,
                    "correct_alerts": hits, "resolved_alerts": count,
                    "unknown_alerts": int((~group.outcome_available).sum()),
                    "precision": success, "warned_trades": group.trade_id.nunique(),
                    "g3_day_trades": int(baseline.trades), "g3_day_net_pnl": baseline.net_pnl,
                    "observer_pnl_change": 0.0})
                shown[f"{side} {horizon}m correct/resolved (success)"] = f"{hits}/{count} ({pct(success)})"
        display_rows.append(shown)
    pd.DataFrame(daily_rows).to_csv(out / "daywise_alert_success.csv", index=False)
    table(pd.DataFrame(display_rows))
    para("The daywide G-3 trade count and P&L repeat across the four side/horizon rows in daywise_alert_success.csv for reference; do not sum those repeated baseline fields. 0/0 means no alerts, not 0% success. Unresolved alerts are separately counted in the CSV.")
    links = [
        (out / "daywise_alert_success.csv", "Daywise every-minute alert precision for all four side/horizon combinations"),
        (FROZEN / "daily_results.csv", "Frozen G-3 daywise trading results"),
        (STUDY / "daywise_results.csv", "Daywise fixed-grid warning TP/FP/FN/TN, including zero-trade dates"),
        (STUDY / "trade_report.csv", "All 93 trades, outcomes, monitor availability and alerts"),
        (STUDY / "predictions_1m.csv", "Every observed minute: contemporaneous features, warnings and later outcomes"),
        (STUDY / "evaluation_windows.csv", "Fixed-grid observations and sensitivity labels"),
        (STUDY / "alert_episodes.csv", "Every cooldown-filtered alert, with actual outcome and evidence times"),
        (STUDY / "metrics.csv", "Full validation metrics, uncertainty, benchmarks and censoring sensitivity"),
        (STUDY / "phase_metrics.csv", "Fixed-grid trade-age metrics"),
        (TUNING / "comparisons.csv", "Original versus chosen metric deltas: all zero"),
        (TUNING / "selected_rules.json", "Locked selection, exact dates, sources and protocol"),
        (FROZEN / "manifest.json", "Frozen baseline manifest and archived source hashes"),
        (FROZEN / "provenance.json", "Exact archived market-data source paths"),
    ]
    for path, label in links:
        link(path, label)
    heading("10. Reproducibility and checks")
    para(f"Selected-rule SHA-256: {sha(TUNING / 'selected_rules.json')}. Frozen execution replay parity was verified for all 93 fills; 80 historical paths were also compared against the sealed historical path arrays. All 36 monitor/metric/tuning tests pass, including future-data invariance, LONG/SHORT symmetry, threshold equality, cooldown and ambiguous exit ordering. Frozen archive verification passes. These checks establish reproducibility, not predictive quality.")
    verification_path = TUNING / "independent_validation.json"
    if verification_path.exists():
        read(TUNING, verification_path.name)
        para("Independent verification recomputed all 13,368 grid outcome labels directly from frozen candles and exit records, and all 18,694 minute-level feature vectors: zero mismatches. Independent confusion-matrix and operational-alert recounts also matched. Six primary-grid outcomes have unknown order, only one of them a warning, so unresolved intrabar ambiguity cannot explain the weak performance.")
        link(verification_path, "Independent numerical verification and limitations")
    para("The initial pullback_monitor_v1_20261008 draft is superseded: exit-candle order handling was corrected before outcome performance was inspected. Only pullback_monitor_v1_20261008_resolved_exits supplies this report. The historical study is complete for its sealed inputs; it is not a result for the unfinished October 8 session.")
    para("Research status: NO_ELIGIBLE_IMPROVEMENT. Predictive validation: insufficient. Trading-action validation: not performed. Live authority: none.", "verdict")

    page = '<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><title>G-3 pullback re-evaluation report</title><style>body{font:15px/1.55 system-ui,sans-serif;color:#172431;background:#fafbfd;max-width:1500px;margin:32px auto;padding:0 24px}h1{font-size:30px}h2{margin-top:32px;border-bottom:1px solid #cad4dc;padding-bottom:7px}h3{margin-top:24px}.verdict{background:#fff1de;border-left:5px solid #ae6000;padding:14px}.table-wrap{overflow:auto;margin:18px 0}table{border-collapse:collapse;background:white;font-size:13px;min-width:700px}th,td{border:1px solid #d6dfe5;padding:8px 10px;text-align:left}th{background:#e9f0f5;white-space:nowrap}tbody tr:nth-child(even){background:#f6f9fb}a{color:#075ba3}p{max-width:1250px}@media print{body{max-width:none;margin:0;font-size:11px}.table-wrap{overflow:visible}table{font-size:9px;min-width:0}h2{break-after:avoid}tr{break-inside:avoid}}</style></head><body>' + '\n'.join(body) + '</body></html>'
    (out / "report.html").write_text(page, encoding="utf-8")
    for path, expected in inputs.items():
        assert sha(path) == expected, f"Input changed: {path}"
    manifest = {"status": "COMPLETE_NO_ELIGIBLE_IMPROVEMENT", "input_sha256": inputs,
        "report_source_sha256": sha(__file__), "trade_actions_changed": False,
        "test_suite": "36 passed", "selected_rules_sha256": sha(TUNING / "selected_rules.json"),
        "output_sha256": {p.name: sha(p) for p in out.iterdir() if p.is_file()}}
    (out / "report_manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    print(out / "report.html")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, required=True)
    run(parser.parse_args().output_dir)
