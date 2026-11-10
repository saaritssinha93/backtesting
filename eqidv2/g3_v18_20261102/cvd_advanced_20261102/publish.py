"""Create the advanced-CVD delivery page and stock-level exports."""
from __future__ import annotations

import argparse
import html
import json
import os
from pathlib import Path

import pandas as pd

from g3_v18_20261102.run import chart, dump, guard_writes, sha

PACKAGE = Path(__file__).resolve().parent
LABELS = {"base_control": "Original G3", "acceleration_rank": "CVD acceleration ranking",
          "acceleration_rvol_rank": "Acceleration + relative volume ranking"}


def link(path, folder):
    return html.escape(Path(os.path.relpath(Path(path).resolve(), folder.resolve())).as_posix(), quote=True)


def table(frame):
    return '<div class="scroll">' + frame.to_html(index=False, border=0, na_rep="Unavailable",
        float_format=lambda value: f"{value:,.2f}") + '</div>'


def run(feature_path: Path, data: Path, results: Path, output: Path):
    if output.resolve() == PACKAGE or not output.resolve().is_relative_to(PACKAGE):
        raise ValueError("Output must be a new child of the advanced experiment")
    output.mkdir(parents=True, exist_ok=False)
    guard_writes(PACKAGE)
    features = pd.read_parquet(feature_path)
    membership = pd.read_parquet(data / "universe_membership.parquet")
    source = json.loads((data / "fetch_manifest.json").read_text(encoding="utf-8"))
    backtest = json.loads((results / "summary.json").read_text(encoding="utf-8"))
    membership["session_date"] = membership.session_date.astype(str).str[:10]
    features["session_date"] = features.bar_end.dt.strftime("%Y-%m-%d")
    fields = ["bar_end", "close", "adv_session_cvd", "adv_recent15_pressure", "adv_prior15_pressure",
        "adv_acceleration", "adv_acceleration_normalized", "adv_rvol20", "adv_activity_multiplier",
        "adv_ready", "adv_rvol_ready", "adv_reason", "adv_rvol_reason"]
    latest = features.sort_values(["symbol", "bar_end"]).groupby(["session_date", "symbol"], sort=False).tail(1)
    daily_stocks = membership.merge(latest[["session_date", "symbol", *fields]], on=["session_date", "symbol"],
        how="left", validate="one_to_one")
    for field in ("adv_ready", "adv_rvol_ready"):
        daily_stocks[field] = daily_stocks[field].eq(True)
    for field in ("adv_reason", "adv_rvol_reason"):
        daily_stocks[field] = daily_stocks[field].fillna("NO_RETURNED_CANDLES")
    daily_stocks.to_csv(output / "all_stock_daily_features.csv", index=False)
    session = str(membership.session_date.max())
    snapshot = daily_stocks.loc[daily_stocks.session_date.eq(session)].copy()
    last_bar = snapshot.bar_end.max()
    snapshot["at_latest_returned_clock"] = snapshot.bar_end.eq(last_bar)
    snapshot = snapshot.sort_values(["adv_ready", "adv_acceleration", "symbol"], ascending=[False, False, True])
    snapshot.to_csv(output / "latest_stock_features.csv", index=False)
    current = snapshot[["symbol", "bar_end", "close", "adv_recent15_pressure", "adv_prior15_pressure",
        "adv_acceleration", "adv_rvol20", "adv_ready", "adv_rvol_ready"]].copy()
    for field in ("adv_recent15_pressure", "adv_prior15_pressure", "adv_acceleration"):
        current[field] *= 100
    current.columns = ["Stock", "Last bar (IST)", "Close", "Recent pressure %", "Previous pressure %",
        "Pressure change (percentage points)", "Relative volume (times)", "Acceleration ready", "RVOL ready"]
    current_html = current.to_html(index=False, border=0, table_id="stocks", na_rep="Unavailable",
                                  float_format=lambda value: f"{value:,.2f}")
    metrics = pd.read_csv(results / "overall_comparison.csv")

    def performance(period):
        selected = metrics.loc[metrics.period.eq(period) & metrics.cost_bps.eq(5),
            ["variant", "trades", "win_rate_pct", "net_pnl_rs", "daily_close_drawdown_rs"]].copy()
        selected["variant"] = selected.variant.replace(LABELS)
        selected.columns = ["Strategy", "Trades", "Win rate (%)", "Net P&L (INR)", "Daily realized drawdown (INR)"]
        return table(selected)

    daily = pd.read_csv(results / "daily_results.csv")
    daily = daily.loc[daily.cost_bps.eq(5)].copy()
    baseline_daily = daily.loc[daily.variant.eq("base_control"), ["day", "net_pnl_rs"]].rename(columns={"net_pnl_rs": "baseline_pnl"})
    daily = daily.merge(baseline_daily, on="day", how="left", validate="many_to_one")
    daily["net_pnl_rs"] = daily.net_pnl_rs - daily.baseline_pnl
    daily["variant"] = daily.variant.replace(LABELS)
    unchanged = all(int(backtest["changed_groups_by_variant"].get(v, 0)) == 0 for v in LABELS if v != "base_control")
    conclusion = ("Both advanced variants selected exactly the same stocks as original G3. Profit, trade count and drawdown were identical; the fixed ranking adjustment showed no practical benefit in this test."
                  if unchanged else backtest["conclusion"])
    plot = ('<p class="notice">Both variants stayed at a cumulative P&amp;L difference of INR 0 throughout the replay.</p>'
            if daily.net_pnl_rs.abs().le(1e-8).all() else chart(daily))
    effects = []
    for variant in ("acceleration_rank", "acceleration_rvol_rank"):
        audit = pd.read_csv(results / (variant + "_candidate_scores.csv"), low_memory=False)
        adjusted = audit.loc[audit.adv_rank_group_applied.eq(True) & audit.v9_filter_pass.eq(True)]
        effects.append(dict(variant=variant, eligible_adjusted_candidates=len(adjusted),
            nonzero_adjustments=int(adjusted.adv_rank_adjustment.abs().gt(1e-12).sum()),
            maximum_absolute_adjustment=float(adjusted.adv_rank_adjustment.abs().max()),
            changed_candidate_rank_positions=int(adjusted.adv_rank_position.ne(adjusted.adv_rank_original_position).sum()),
            changed_selected_stock_groups=int(backtest["changed_groups_by_variant"].get(variant, 0))))
    pd.DataFrame(effects).to_csv(output / "ranking_effects.csv", index=False)
    status = dict(current_session=session, historical_symbols=int(features.symbol.nunique()),
        minute_candles=source["minute_rows"], five_minute_feature_rows=len(features),
        current_universe=len(snapshot), current_acceleration_ready=int(snapshot.adv_ready.sum()),
        current_rvol_ready=int(snapshot.adv_rvol_ready.sum()), latest_returned_bar=str(last_bar),
        current_at_latest_clock=int(snapshot.at_latest_returned_clock.sum()),
        requested_cutoff_ist=source["completed_minute_cutoff_ist"], backtest_sessions=backtest["sessions"],
        backtest_last_date=backtest["last_date"], execution_authority=False,
        feature_sha256=sha(feature_path), source_manifest_sha256=sha(data / "fetch_manifest.json"),
        backtest_summary_sha256=sha(results / "summary.json"))
    dump(output / "summary.json", status)
    page = '''<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>G3 Advanced Estimated CVD Test</title><style>
body{margin:0;background:#101827;color:#edf2f8;font:15px/1.6 system-ui,sans-serif}main{max-width:1280px;margin:auto;padding:32px 26px}h1{font-size:34px;line-height:1.2}h2{margin-top:32px}a{color:#8bcfff}.muted{color:#acbdd1}.notice{background:#233148;border-left:4px solid #e3bc71;padding:16px 20px}.cards{display:flex;flex-wrap:wrap;gap:14px;margin:22px 0}.card{background:#1c2a41;padding:16px 24px;border-radius:8px}.card strong{display:block;font-size:26px}.scroll{overflow:auto;max-height:600px}table{border-collapse:collapse;width:100%;white-space:nowrap;font-size:13px}th,td{padding:10px;border-bottom:1px solid #34455d;text-align:right}th:first-child,td:first-child{text-align:left}th{background:#26374f;position:sticky;top:0}p{max-width:1080px}svg{width:100%;max-height:350px;background:#17253b;border-radius:8px}input{background:#233148;color:#fff;border:1px solid #7687a0;border-radius:6px;padding:9px 12px}.legend{font-size:13px}code{color:#a8e5d7}@media(max-width:700px){main{padding:24px 14px}h1{font-size:28px}}
</style></head><body><main><p class="muted">G3 V18 · ADVANCED ESTIMATED CVD · BACKTEST RESEARCH</p><h1>Does CVD acceleration improve stock selection?</h1>
<p class="notice">@@CONCLUSION@@</p><div class="cards"><div class="card">Sealed replay sessions<strong>@@SESSIONS@@</strong></div><div class="card">Historical symbols<strong>@@SYMBOLS@@</strong></div><div class="card">One-minute candles<strong>@@MINUTES@@</strong></div><div class="card">Latest returned stock bar<strong>@@CLOCK@@</strong></div></div>
<h2>Full historical comparison</h2><p>@@FIRST@@ through @@LAST@@. The table uses 5 bps modeled round-trip friction. Detailed outputs include 9 and 15 bps, daily and monthly results, and every changed stock selection.</p>@@ALL@@
<h2>Cumulative P&amp;L difference from original G3</h2><p>Above zero means the ranking variant earned more than the original control over the same dates; below zero means less. The original control is the zero line.</p>@@CHART@@<h2>October comparison</h2>@@OCTOBER@@<p><a href="@@BACKTEST@@">Open detailed results, coverage, ranking changes and verification →</a></p>
<h2>What was tested</h2><p>Each completed minute contributes <code>volume × (2 × close − high − low) ÷ (high − low)</code>, with zero for a flat range. Recent pressure divides the latest 15 minutes' estimated delta by their volume. Previous pressure uses the preceding, nonoverlapping 15 minutes. Acceleration is the difference between these two pressures.</p>
<p>The adjustment is <code>0.10 × trade direction × acceleration ÷ 2</code>, added to the existing setup's ranking metric divided by its maximum across eligible candidates. The second variant multiplies that adjustment by <code>clip(relative volume − 1, 0, 1)</code>. Relative volume compares the latest 15 minutes with the same clock across exactly 20 prior observed stock sessions.</p>
<p>Original filters, entry slots, core and extra quotas, confirmation, exits and capital stay the same. Before 09:45, during the 20-session volume warmup, or when any eligible competitor lacks required data, that variant uses the original ranking for the entire competition. An indicator reading changes ranking only after the stock already qualifies for G3.</p>
<p class="notice">These are candle-based estimates of volume pressure. They do not identify trade aggressors. All 62 original sessions were already inspected in earlier research; no portion is claimed as an unseen holdout. Any apparent improvement needs a later prospective test. Drawdown here is daily realized P&amp;L.</p>
<h2>Latest stock observations · @@DAY@@</h2><p>@@READY@@ of @@CURRENT@@ stocks have usable acceleration, and @@RVOLREADY@@ have the required relative-volume history. The requested cutoff was @@REQUESTED@@; the latest returned bar is @@LASTBAR@@. Missing closing minutes are listed in source coverage. Today's observations are separate from the sealed historical execution replay.</p>
<p>Positive acceleration means pressure became more positive; the recent pressure can still be negative. These observations alone are not G3 entry signals.</p><p><label for="search">Find a stock: </label><input id="search" placeholder="Stock symbol" autocomplete="off"></p><div class="scroll">@@STOCKS@@</div>
<h2>Saved data and evidence</h2><p><a href="ranking_effects.csv">Actual ranking changes (CSV)</a> · <a href="latest_stock_features.csv">All current stock features (CSV)</a> · <a href="all_stock_daily_features.csv">Historical stock/session features (CSV)</a> · <a href="@@FEATURES@@">Every five-minute feature (Parquet)</a> · <a href="@@COVERAGE@@">Minute source coverage</a> · <a href="@@RECONCILE@@">Reconciliation with earlier five-minute candles</a> · <a href="@@README@@">Method and reproduction commands</a></p>
<p class="muted">Kite historical candles: <a href="https://kite.trade/docs/connect/v3/historical/">official data schema</a>. All new work is contained in this separate advanced-CVD folder.</p>
</main><script>document.getElementById('search').addEventListener('input',e=>{const q=e.target.value.trim().toUpperCase();document.querySelectorAll('#stocks tbody tr').forEach(r=>r.hidden=!r.cells[0].textContent.toUpperCase().includes(q));});</script></body></html>'''
    replacements = dict(CONCLUSION=html.escape(conclusion), SESSIONS=str(backtest["sessions"]),
        SYMBOLS=str(status["historical_symbols"]), MINUTES=f"{source['minute_rows']:,}",
        CLOCK=last_bar.strftime("%H:%M IST") if pd.notna(last_bar) else "Unavailable", FIRST=backtest["first_date"], LAST=backtest["last_date"],
        ALL=performance("all"), OCTOBER=performance("october"), CHART=plot,
        BACKTEST=link(results / "report.html", output), DAY=session, READY=str(status["current_acceleration_ready"]),
        CURRENT=str(len(snapshot)), RVOLREADY=str(status["current_rvol_ready"]), REQUESTED=html.escape(status["requested_cutoff_ist"]),
        LASTBAR=html.escape(str(last_bar)), STOCKS=current_html, FEATURES=link(feature_path, output),
        COVERAGE=link(data / "coverage.csv", output), RECONCILE=link(data / "reconciliation_5m.csv", output),
        README=link(PACKAGE / "README.md", output))
    for name, value in replacements.items():
        page = page.replace("@@" + name + "@@", value)
    (output / "report.html").write_text(page, encoding="utf-8")
    dump(output / "manifest.json", dict(files={p.name: sha(p) for p in output.iterdir() if p.is_file()}))
    print(json.dumps(status, indent=2), flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--features", type=Path, required=True)
    parser.add_argument("--data", type=Path, required=True)
    parser.add_argument("--results", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    run(args.features, args.data, args.results, args.output)
