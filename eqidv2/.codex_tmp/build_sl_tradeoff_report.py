"""Build a research-only, auditable HTML report from completed SL experiments."""
from __future__ import annotations

import hashlib
import html
import json
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "outputs/v13_sl_tradeoff_20261004"


def read(name):
    return json.loads((OUT / name).read_text(encoding="utf-8"))


def money(value):
    return f"₹{value:,.0f}"


def table(headers, rows):
    escape = lambda value: html.escape(str(value))
    return ('<div class="table-wrap"><table><thead><tr>'
            + "".join(f"<th>{escape(x)}</th>" for x in headers)
            + "</tr></thead><tbody>"
            + "".join("<tr>" + "".join(f"<td>{escape(x)}</td>" for x in row)
                      + "</tr>" for row in rows)
            + "</tbody></table></div>")


def main():
    fixed = read("static_analysis.json")
    timed = read("timing_analysis.json")
    recovery = read("pullback_analysis.json")
    costs = read("cost_stress_analysis.json")
    by_stop = {r["stop_pct"]: r for r in fixed["records"]}
    by_name = {r["rule"]["name"]: r for r in timed["results"]}
    baseline = by_name["STATIC_1.00"]
    staged = by_name["TIGHTEN_1.25_TO_1.00_AFTER_120M"]
    assert fixed["iterations"] == 94 and timed["rule_count"] == 48
    assert timed["synthetic_checks_passed"] == recovery["synthetic_timer_checks_passed"] == 6
    assert all(r["trades"] == 85 for r in timed["results"])
    assert all(r["full"]["trades"] == 85 for r in fixed["records"])
    for stop in (1.0, 1.25, 2.75):
        assert abs(by_stop[stop]["full"]["net_rupees"]
                   - by_name[f"STATIC_{stop:.2f}"]["net_profit_rupees"]) < 0.01
    assert abs(sum(t["delta_rupees"] for t in staged["changed_trades"])
               - (staged["net_profit_rupees"] - baseline["net_profit_rupees"])) < 0.01
    assert all(r["wins"] < by_stop[r["stop_pct"]]["full"]["wins"]
               and r["net_rupees"] < by_stop[r["stop_pct"]]["full"]["net_rupees"]
               for r in recovery["simple_causal_time_rules"])

    # Paired circular blocks retain short clusters of daily outcomes. This does
    # not undo repeated parameter selection or make these days out-of-sample.
    dates = sorted(baseline["daily_net"])
    delta = np.array([staged["daily_net"][d] - baseline["daily_net"][d] for d in dates])
    rng = np.random.default_rng(20261004)
    starts = rng.integers(0, len(dates), size=(30000, 9))
    indices = ((starts[:, :, None] + np.arange(5)) % len(dates)).reshape(30000, -1)[:, :len(dates)]
    samples = delta[indices].sum(axis=1)
    block_interval = np.quantile(samples, [.025, .975]).tolist()
    diagnostic = {
        "status": "EXPLORATORY_REUSED_HISTORY_NOT_UNTOUCHED_VALIDATION",
        "method": "30000 paired circular 5-session block resamples; 43 sessions; seed20261004",
        "staged_minus_1pct_net_rupees": float(delta.sum()),
        "net_delta_95pct_percentile_interval": block_interval,
        "caution": "Unadjusted for parameter search. Not a probability of future superiority.",
    }
    (OUT / "staged_block_bootstrap.json").write_text(json.dumps(diagnostic, indent=2), encoding="utf-8")

    sections = []
    sections.append('''<h1>V13-v10-G-2: stop-loss trade-off research</h1>
    <p class="sub">Research run: 4 October 2026 · Completed history: 29 July–30 September 2026 · 43 sessions · 85 executed trades</p>
    <div class="callout"><h2>Decision</h2>
    <p><strong>Keep 1.00% as the present fixed-stop default.</strong> For the next forward test, the leading staged candidate is <strong>1.25% initially, tightened to 1.00% after 120 minutes since entry</strong>. Both stop percentages are measured from the original entry price. This is a tested trade-off, not a demonstrated future optimum.</p>
    <p>The staged rule preserves two additional historical winners and improves net profit, but allows larger losses. No live settings or frozen strategy configuration were changed.</p></div>
    <p>October 1 is excluded: the final 11:20 signal / 11:21 confirmation scan slot is incomplete. These are the same repeatedly reviewed historical selections; targets also used this history. September splits and bootstraps are stability diagnostics, not an untouched test.</p>
    <h2>Finalist comparison</h2>
    <p>Same ₹100,000 capital per trade, 5× exposure (₹500,000), ₹1,000,000 portfolio, and 5 bps round-trip cost (₹250/trade). Loss columns are positive magnitudes; profits are after modeled costs.</p>''')
    finalists = [("Fixed 1.00%", baseline), ("Fixed 1.25%", by_name["STATIC_1.25"]),
                 ("1.25% → 1.00% after 120 min", staged), ("Fixed 2.75%", by_name["STATIC_2.75"])]
    sections.append(table(["Rule", "Wins / trades", "Win %", "Net", "Profit factor", "Average loss", "Worst observed loss", "Daily-close DD", "Minute-close DD", "Positive active days"],
                         [[label, f'{r["wins"]}/85', f'{r["win_rate_pct"]:.2f}%', money(r["net_profit_rupees"]),
                           f'{r["profit_factor"]:.3f}', money(r["avg_loss_magnitude_rupees"]), money(r["worst_loss_rupees"]),
                           money(r["daily_close_drawdown_rupees"]), money(r["minute_close_drawdown_rupees"]),
                           f'{r["positive_sessions"]}/34'] for label, r in finalists]))
    sections.append('''<p>There are 9 no-trade sessions. Positive-day rates for fixed 1% and the staged rule are 67.65% and 73.53% of active days, or 53.49% and 58.14% of all 43 sessions. These are different from trade win rates.</p>
    <p>Daily-close drawdown omits intraday open-position losses. Minute-close drawdown includes them but still misses intra-minute extremes; it is not tick-level maximum drawdown. The 2.75% stop was never triggered in these 85 trades, so its higher win rate does not establish good reversal protection.</p>
    <h2>What the staged rule buys—and costs</h2>
    <p>Versus fixed 1%: net increases ₹9,258 (+4.20%), win rate increases 2.35 percentage points, and 2 losing trades become winners. Three trades improve in total; eight worsen. Average losing trade rises from ₹4,200 to ₹4,385 (+4.40%), worst observed loss from ₹5,279 to ₹6,500 (+23.13%), and daily-close drawdown from ₹11,671 to ₹14,142 (+21.17%). Minute-close drawdown is essentially unchanged (₹21,938 vs ₹21,805).</p>
    <p>105, 115, 120 and 125 minutes have identical realized wins and profit. This supports a rounded two-hour candidate rather than claiming a special optimal minute. With the same 120-minute tightening rule, initial stops of 1.30% produce the same 59 wins but lower profit; 1.20% saves only one extra winner. No setup-specific percentages are recommended from this small sample.</p>
    <h2>Exact candidate rules for a new-data test</h2>
    <ol><li>At entry, use a resting 1.25% hard stop. Long: entry × 0.9875; short: entry × 1.0125.</li>
    <li>After 120 minutes from entry, tighten to 1.00% from the <strong>original entry</strong>: long entry × 0.99; short entry × 1.01. This is not a trailing stop and never widens.</li>
    <li>If price is already beyond the tightened level, exit at the available executable price. Do not assume a fill back at the theoretical stop.</li>
    <li>Keep the original targets, entry selection, and 15:15 session exit. A two-hour point tightens protection; it does not force an exit merely because a position is still losing.</li>
    <li>The hard stop remains active throughout. Do not wait for a five-minute close after it is breached.</li></ol>
    <p>The simulation has one-minute end labels and uses the entry bar end as a conservative time origin; actual fill time is only known within that minute. A live implementation should use the real fill timestamp.</p>
    <h2>Position size is part of the stop decision</h2>
    <p>At the present ₹500,000 exposure, a 1% stop is approximately ₹5,250 including modeled costs; a 1.25% stop is ₹6,500. To retain the same ₹5,250 nominal initial risk, reduce exposure to ₹403,846 (capital ₹80,769 at 5×), or 80.77% of current size. Under the same linear-cost assumptions, the staged net then becomes ₹185,702, below fixed 1% at current size (₹220,659). Its win rate does not change.</p>
    <p>That exposes the trade-off: extra price room is not free. Nominal stop budgets are not guaranteed loss caps; gaps, slippage and liquidity can worsen execution.</p>
    <h2>Pullback recovery evidence</h2>
    <p>Of 19 trades crossing 1% adverse in the wide-stop reference, only 3 eventually finish positive and 16 negative. A 1.25% stop retains 59 of 60 reference winners, versus 57 at 1%. The reference uses a 3% hard stop, which never triggers in this sample; it is not a recommendation to trade without a stop.</p>''')
    sections.append(table(["Trade", "Date / side", "Maximum adverse move", "First 1% breach", "Breakeven close", "Minutes breach → breakeven", "Eventual net"],
        [[r["symbol"], f'{r["day"]} / {r["side"]}', f'{r["maximum_adverse_pct"]:.3f}%',
          next(c for c in r["crossings"] if c["adverse_threshold_pct"] == 1)["first_cross_ts"][11:16],
          next(c for c in r["crossings"] if c["adverse_threshold_pct"] == 1)["breakeven_recovery_ts"][11:16],
          next(c for c in r["crossings"] if c["adverse_threshold_pct"] == 1)["minutes_cross_to_breakeven"],
          money(r["eventual_net_rupees"])] for r in recovery["one_25_pct_rescued_winners"]]))
    sections.append('''<p>These recoveries took 171 and 250 minutes after the first 1% breach. They were back inside the 1% boundary when needed for the two-hour tightening rule, although still below breakeven. A rule requiring recovery to breakeven within 30–90 minutes cuts them too early. All 20 tested continuous-underwater timers and all 16 age-plus-adverse-loss timers reduce both wins and net versus their matching fixed-stop baseline.</p>
    <h2>Stability and uncertainty</h2>''')
    ci = staged["paired_day_bootstrap_net_delta_95pct"]
    sections.append(f'<p>The paired-day 95% resampling interval for staged minus fixed-1% net is {money(ci[0])} to {money(ci[1])}. The paired five-session-block interval is {money(block_interval[0])} to {money(block_interval[1])}. Both include zero. These intervals are not corrected for searching many parameters and cannot establish future superiority.</p>')
    sections.append(table(["Rule", "Jul–Aug wins / 53", "Jul–Aug net", "September wins / 32", "September net"],
        [[label, r["slices"]["jul_aug"]["wins"], money(r["slices"]["jul_aug"]["net"]),
          r["slices"]["september"]["wins"], money(r["slices"]["september"]["net"])] for label, r in finalists]))
    sections.append('''<p>Fine-grid fixed stops: 0.98% has the lowest daily-close drawdown (₹11,571), versus ₹11,671 at 1%. Fixed 1.23% produces the same 59 winners as 1.25% with ₹225,144 net. These exact thresholds were selected after seeing the sample, so they are not reliable reasons to prefer hundredths of a percent. The broader neighborhood and a rounded setting matter more.</p>
    <p>Recommendation remains conditional: use 1% if keeping current loss size is the priority; freeze the two-hour staged candidate for forward testing if accepting wider price room is the priority. Do not keep retuning it on this same history or call it validated.</p>
    <h2>Execution and audit notes</h2>
    <ul><li>94 fixed stops: 0.60%–1.50% at 0.01 percentage-point steps, plus 1.75%, 2.00%, 2.75%. Also tested 29 scheduled tightening rules, 16 age-plus-loss rules and 20 continuous-underwater rules.</li>
    <li>Three static control replays reproduce native engine price, exit index, reason and totals. All 94 fixed and 48 timing-suite variants retain the same 85 executions. No portfolio rejection occurs.</li>
    <li>Targets and order selection are held fixed; entry expires after 10 minutes. Costs are 5 bps of exposure, not capital. Existing portfolio constraints are retained.</li>
    <li>Time-exit signals use completed closes and execute at the following open, with resting stop-gap / target-gap priority. Six synthetic checks pass in each timing implementation.</li>
    <li>Stop and target in the same candle use conservative stop-first precedence. Later-bar adverse gaps fill at the worse opening price. One-minute OHLC cannot establish all intrabar paths; entry-bar extrema may predate the fill.</li>
    <li>Maximum adverse/favorable excursions from full candles may include pre-entry or post-exit extrema. The two named recovered winners have the same adverse maxima when entry and exit bars are excluded.</li>
    <li>Static control timestamps retain published candle-end labels. Dynamic open executions carry the actual open time. Neither intrabar stop nor target touch time is known more precisely than its candle.</li>
    <li>Inputs are the sealed base bundle plus official complete-day snapshots checked by the existing helper. October 1 is not silently treated as a no-trade day.</li></ul>
    <details><summary>All 94 fixed-stop results</summary>''')
    sections.append(table(["SL %", "Wins", "Win %", "Net", "PF", "Avg loss", "Worst loss", "Daily DD", "Positive / 34", "Net at equal ₹5,250 risk"],
        [[f'{r["stop_pct"]:.2f}', r["full"]["wins"], f'{r["full"]["win_rate_pct"]:.2f}', money(r["full"]["net_rupees"]),
          f'{r["full"]["profit_factor"]:.3f}', money(abs(r["full"]["average_loss_rupees"])), money(abs(r["full"]["worst_loss_rupees"])),
          money(r["full"]["daily_close_drawdown_rupees"]), r["full"]["positive_days"], money(r["equal_nominal_stop_risk"]["full"]["net_rupees"])] for r in fixed["records"]]))
    sections.append('</details><details><summary>All 45 scheduled / age-plus-loss timing rules</summary>')
    sections.append(table(["Rule", "Wins", "Net", "Average loss", "Worst loss", "Daily DD", "Minute-close DD"],
        [[r["rule"]["name"], r["wins"], money(r["net_profit_rupees"]), money(r["avg_loss_magnitude_rupees"]), money(r["worst_loss_rupees"]),
          money(r["daily_close_drawdown_rupees"]), money(r["minute_close_drawdown_rupees"])] for r in timed["results"] if r["rule"]["family"] != "STATIC"]))
    sections.append('</details><details><summary>All 20 continuous-underwater timers</summary>')
    sections.append(table(["Hard stop %", "Underwater minutes", "Wins", "Win %", "Net", "Average loss", "Worst loss", "Daily DD"],
        [[r["stop_pct"], r["max_underwater_minutes"], r["wins"], f'{r["win_rate_pct"]:.2f}', money(r["net_rupees"]),
          money(abs(r["average_loss_rupees"])), money(abs(r["worst_loss_rupees"])), money(r["max_daily_drawdown_rupees"])] for r in recovery["simple_causal_time_rules"]]))
    sections.append('</details><details><summary>Cost stress: 5, 10 and 15 bps</summary><p>Same selected fills; no modeled market impact or liquidity-driven missed executions. Additional 5 / 10 bps subtract ₹21,250 / ₹42,500 over 85 trades.</p>')
    sections.append(table(["SL %", "Total cost bps", "Wins", "Net", "PF", "Wins turned into losses vs 5 bps"],
        [[r["stop_pct"], r["total_cost_bps"], r["full"]["wins"], money(r["full"]["net_rupees"]), f'{r["full"]["profit_factor"]:.3f}',
          r["winning_trades_flipped_to_loss"]] for r in costs["records"]]))
    sections.append('</details><details><summary>43-session daywise finalist comparison</summary>')
    sections.append(table(["Date"] + [label for label, _ in finalists],
        [[d] + [money(r["daily_net"][d]) for _, r in finalists] for d in dates]))
    sections.append('</details><details><summary>Every changed trade: staged vs fixed 1%</summary>')
    sections.append(table(["Date", "Symbol", "Setup", "Side", "Fixed 1% net", "Staged net", "Difference", "Staged exit"],
        [[r["day"], r["tradingsymbol"], r["setup_id"], r["side"], money(r["portfolio_net_profit_rupees_1pct"]),
          money(r["portfolio_net_profit_rupees"]), money(r["delta_rupees"]), r["exit_reason"]] for r in staged["changed_trades"]]))
    sections.append('''</details><h2>Sources and reproducibility</h2>
    <p>General cautions: <a href="https://www.davidhbailey.com/dhbpapers/backtest-prob.pdf">Bailey et al., The Probability of Backtest Overfitting</a> explains parameter-selection risk. <a href="https://www.investor.gov/introduction-investing/general-resources/news-alerts/alerts-bulletins/investor-bulletins-15">Investor.gov order-types bulletin</a> explains why a stop price does not guarantee its execution price. Local numerical results come from the linked research outputs, not those sources.</p>
    <p>Regenerate from the project root using Python 3.12: run .codex_tmp/sl_tradeoff_static.py, .codex_tmp/sl_tradeoff_static.py --cost-stress-only, .codex_tmp/sl_tradeoff_pullbacks.py, .codex_tmp/sl_tradeoff_timing.py, then .codex_tmp/build_sl_tradeoff_report.py. These scripts only write research outputs.</p>''')
    files = ["static_analysis.json", "timing_analysis.json", "pullback_analysis.json", "cost_stress_analysis.json", "staged_block_bootstrap.json"]
    sections.append('<ul>' + ''.join(f'<li><a href="{f}">{f}</a></li>' for f in files) + '</ul>')
    manifest = {f: hashlib.sha256((OUT / f).read_bytes()).hexdigest() for f in files}
    for f in ("sl_tradeoff_static.py", "sl_tradeoff_pullbacks.py", "sl_tradeoff_timing.py", "build_sl_tradeoff_report.py"):
        manifest[f".codex_tmp/{f}"] = hashlib.sha256((ROOT / ".codex_tmp" / f).read_bytes()).hexdigest()
    (OUT / "research_manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    sections.append('<p>Output and script SHA-256 fingerprints: <a href="research_manifest.json">research_manifest.json</a>.</p>')
    style = '''body{font:16px/1.6 system-ui,sans-serif;color:#172a3a;background:#f5f7fa;margin:0}main{max-width:1250px;margin:auto;padding:36px;background:white}h1{font-size:30px;line-height:1.2}h2{font-size:21px;margin-top:30px}.sub{color:#526273}.callout{background:#eef5ff;border-left:5px solid #2860a5;padding:8px 22px}.callout h2{margin-top:12px}.table-wrap{overflow-x:auto;margin:18px 0}table{border-collapse:collapse;width:100%;font-size:13px;line-height:1.45}th,td{padding:9px 11px;text-align:right;border-bottom:1px solid #d9e1ea;white-space:nowrap}th{background:#e9eff7}th:first-child,td:first-child{text-align:left}tr:nth-child(even){background:#f7f9fc}details{border:1px solid #d9e1ea;border-radius:5px;padding:12px;margin:16px 0}summary{cursor:pointer;font-weight:650}a{color:#245992}li{margin:8px 0}@media print{main{padding:0}details{break-inside:avoid}.table-wrap{overflow:visible}table{font-size:9px}th,td{padding:4px;white-space:normal}}'''
    document = '<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>V13-v10-G-2 SL trade-off research</title><style>' + style + '</style></head><body><main>' + '\n'.join(sections) + '</main></body></html>'
    destination = OUT / "SL_TRADEOFF_REPORT.html"
    destination.write_text(document, encoding="utf-8")
    print(json.dumps({"report": str(destination), "size_bytes": destination.stat().st_size,
                      "checks": "PASS", "block_bootstrap": diagnostic}, indent=2))


if __name__ == "__main__":
    main()
