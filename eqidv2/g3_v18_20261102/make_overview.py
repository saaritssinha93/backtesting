"""Create the local reading guide for this completed research delivery."""
import html
import json
from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parent
RESULT = ROOT / "results_20261102_complete"
SNAPSHOT = ROOT / "current_20261102_latest"


def main():
    summary = json.loads((RESULT / "summary.json").read_text())
    snapshot = json.loads((SNAPSHOT / "summary.json").read_text())
    verified = json.loads((ROOT / "verification_20261102.json").read_text())
    if not verified["passed"]:
        raise ValueError("Unverified result")
    comparison = pd.read_csv(RESULT / "overall_comparison.csv")
    comparison = comparison.loc[comparison.cost_bps.eq(5) & comparison.period.eq("all"),
        ["variant", "trades", "win_rate_pct", "net_pnl_rs", "daily_close_drawdown_rs"]]
    comparison.columns = ["Variant", "Trades", "Win rate %", "Net profit ₹", "Daily-close drawdown ₹"]
    table = comparison.to_html(index=False, border=0, float_format=lambda n: f"{n:,.2f}")
    body = f'''<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>G3 V18 — Results and reading guide</title><style>
body{{background:#0c1420;color:#e8edf5;font:16px/1.6 system-ui;margin:0}}main{{max-width:1150px;margin:auto;padding:38px 28px}}h1{{font-size:36px;line-height:1.2}}h2{{margin-top:30px}}a{{color:#70c9ff}}.tag{{color:#6edac3;font-size:12px;letter-spacing:2px}}.cards{{display:flex;gap:16px;flex-wrap:wrap}}.card{{flex:1;min-width:280px;background:#17273c;padding:22px;border-radius:10px}}.notice{{border-left:4px solid #e9bd69;padding:16px 20px;background:#17273c}}table{{border-collapse:collapse;white-space:nowrap;width:100%;font-size:14px}}td,th{{padding:10px 12px;border-bottom:1px solid #2a3a50;text-align:left}}th{{background:#1b2e46}}.scroll{{overflow:auto}}.muted{{color:#a2b2c8}}li{{margin:8px 0}}</style></head><body><main>
<div class="tag">SEPARATE NEW VERSION · EXISTING G3 RETAINED</div><h1>G3 V18 research results</h1>
<p>Advanced market/stock participation indicators and reduced-rule experiments, tested against the original G3 V17. No live strategy or current-result pointer was changed.</p>
<div class="cards"><div class="card"><h2>Historical backtest</h2><p>{summary['first_date']} through {summary['last_date']} · 62 replayed sessions, including one restricted session.</p><a href="results_20261102_complete/report.html">Open the full comparison →</a><p><a href="results_20261102_complete/daily_results.csv">Day-by-day CSV</a> · <a href="results_20261102_complete/overall_comparison.csv">All metrics CSV</a></p></div>
<div class="card"><h2>Today’s observed movers</h2><p>November 2 · {html.escape(snapshot['common_bar_end_ist'])}<br>{snapshot['snapshot_stocks']}/{snapshot['universe_stocks']} stocks at the same completed clock.</p><a href="current_20261102_latest/report.html">Open today’s indicator snapshot →</a><p>Intraday indicator observations only. Today’s execution P&amp;L is unavailable because a complete sealed one-minute replay source is missing.</p></div></div>
<h2>What the comparison showed</h2><p class="notice">None of the six changed variants passed every predeclared October improvement check. Keep V17 as the reference. V18 is a reproducible research version, not a proven replacement.</p>
<div class="scroll">{table}</div><p class="muted">5 bps modeled round-trip costs; ₹10 lakh portfolio capacity, ₹1 lakh per trade with 5× exposure. These inherited, retrospectively selected historical results are not an independent holdout or a forecast.</p>
<ul><li>Added and tested: market breadth, relative strength versus true Nifty spot, session VWAP/slope, and volume-weighted candle pressure. Same-time relative volume and efficiency are exported as diagnostics.</li><li>VWAP/pressure cut October daily-close drawdown from ₹25,645 to ₹20,500, but reduced October net profit from ₹1.66 lakh to ₹1.47 lakh.</li><li>Removing EMA stacking and candle-shape thresholds substantially lowered win rate and profit. The reduced-rule branches remain experimental.</li></ul>
<h2>Coverage and preservation</h2><ul><li>October 28 uses 215/218 stocks: COROMANDEL, KARURVYSYA and SAIL are excluded for missing or invalid exact OI. Original blocked-source evidence is preserved. The other 61 sessions retain their existing source eligibility.</li><li>August 24, August 25 and October 1 lack a validated complete source. Exchange holidays are distinguished in the coverage table. Missing dates are not silently counted as zero-P&amp;L sessions.</li><li>August 20 has insufficient continuous Nifty context after the opening bars; market-dependent variants fail closed. This data-availability effect is part of the results, not evidence of predictive skill.</li><li>All writes for this task stayed inside this new folder. Existing G3 inputs are unchanged. The audit observed two unrelated G7 files changing elsewhere and left them alone.</li><li>51 tests passed. Independent validation checked trade chronology, selected-order identities, all daily/monthly P&amp;L totals, cost sensitivity and 30 result-artifact hashes. October 29’s control also exactly matches the saved G3 production replay.</li></ul>
<p><a href="verification_20261102.json">Verification receipt</a> · <a href="parity_oct29_smoke.json">October 29 parity</a> · <a href="README.md">Run instructions</a> · <a href="results_20261102_complete/coverage.csv">Coverage audit</a> · <a href="results_20261102_complete/preservation_check.json">Preservation audit</a></p>
<p class="muted">Automatic tool review rejected removing the new temporary whole-history snapshot copies with “blocked by policy,” providing no further reason. Those copies remain; no pre-existing files were deleted.</p>
</main></body></html>'''
    (ROOT / "START_HERE.html").write_text(body, encoding="utf-8")
    print(ROOT / "START_HERE.html")


if __name__ == "__main__":
    main()
