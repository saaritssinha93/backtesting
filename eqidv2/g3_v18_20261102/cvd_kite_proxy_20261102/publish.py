"""Publish a full-universe descriptive CVD-proxy report in a new folder."""
from __future__ import annotations

import argparse
import html
import json
from pathlib import Path

import pandas as pd

from g3_v18_20261102.run import dump, guard_writes, sha

PACKAGE = Path(__file__).resolve().parent


def relative(path, output):
    import os
    return Path(os.path.relpath(Path(path).resolve(), output.resolve())).as_posix()


def build(features_path: Path, membership_path: Path, backtest: Path, output: Path, session_date: str | None = None):
    if not output.resolve().is_relative_to(PACKAGE):
        raise ValueError("Output must stay in this new experiment")
    output.mkdir(parents=True, exist_ok=False)
    guard_writes(PACKAGE)
    bars = pd.read_parquet(features_path)
    members = pd.read_parquet(membership_path)
    if members.duplicated(["session_date", "symbol"]).any():
        raise ValueError("Duplicate universe membership")
    bars["session_date"] = bars.bar_end.dt.strftime("%Y-%m-%d")
    members["session_date"] = members.session_date.astype(str).str[:10]
    bars = bars.sort_values(["symbol", "bar_end"])
    keys = ["session_date", "symbol"]
    last = bars.groupby(keys, sort=False).tail(1).copy()
    counts = bars.groupby(keys).agg(observed_bars=("bar_end", "size"),
        ready_bars=("cvd_proxy_ready", "sum"), first_bar=("bar_end", "min"),
        session_first_open=("open", "first"))
    last = last.merge(counts.reset_index(), on=keys, validate="one_to_one")
    last["price_from_open_pct"] = 100 * (last.close / last.session_first_open - 1)
    expected_open = last.first_bar.dt.normalize() + pd.Timedelta(hours=9, minutes=20)
    last.loc[~last.first_bar.eq(expected_open), "price_from_open_pct"] = float("nan")
    last["estimated_session_pressure_pct"] = 100 * last.cvd_proxy_session_ratio
    last["estimated_recent15_pressure_pct"] = 100 * last.cvd_proxy_recent15_ratio
    fields = ["bar_end", "close", "price_from_open_pct", "cvd_proxy_session", "cvd_proxy_recent15",
        "estimated_session_pressure_pct", "estimated_recent15_pressure_pct", "observed_bars",
        "ready_bars", "first_bar", "cvd_proxy_ready", "cvd_proxy_session_ready", "cvd_proxy_reason"]
    daily = members.merge(last[keys + fields], on=keys, how="left", validate="one_to_one")
    daily["cvd_proxy_reason"] = daily.cvd_proxy_reason.fillna("NO_OBSERVED_CANDLES")
    daily["cvd_proxy_ready"] = daily.cvd_proxy_ready.eq(True)
    daily["cvd_proxy_session_ready"] = daily.cvd_proxy_session_ready.eq(True)
    daily.to_csv(output / "all_stock_daily_summary.csv", index=False)
    coverage = daily.groupby("session_date").agg(universe_stocks=("symbol", "size"),
        stocks_with_bars=("bar_end", "count"), stocks_with_session_cvd=("cvd_proxy_session_ready", "sum"),
        stocks_ready_at_own_latest_bar=("cvd_proxy_ready", "sum"), earliest_latest_bar=("bar_end", "min"),
        latest_observed_bar=("bar_end", "max")).reset_index()
    coverage.to_csv(output / "historical_coverage.csv", index=False)
    today = session_date or str(members.session_date.max())
    if today not in set(members.session_date):
        raise ValueError("Requested snapshot session has no universe membership")
    current = daily.loc[daily.session_date.eq(today)].copy()
    today_bars = bars.loc[bars.session_date.eq(today) & bars.symbol.isin(current.symbol)]
    latest_bar = today_bars.bar_end.max()
    current["at_latest_observed_clock"] = current.bar_end.eq(latest_bar)
    current.loc[current.cvd_proxy_ready & ~current.at_latest_observed_clock, "cvd_proxy_reason"] = "READY_AT_EARLIER_CLOCK"
    current = current.sort_values(["cvd_proxy_ready", "estimated_session_pressure_pct", "symbol"],
                                  ascending=[False, False, True])
    current.to_csv(output / "latest_stock_snapshot.csv", index=False)
    summary = dict(scope="DESCRIPTIVE_ESTIMATED_CVD_AND_HISTORICAL_BACKTEST", execution_authority=False,
        actual_aggressor_delta=False, first_bar=str(bars.bar_end.min()), last_bar=str(bars.bar_end.max()),
        fetched_symbols=int(bars.symbol.nunique()), computed_bars=len(bars), observed_sessions=int(bars.session_date.nunique()),
        requested_membership_sessions=int(members.session_date.nunique()), current_session=today,
        current_universe=len(current), current_stocks_with_bars=int(current.bar_end.notna().sum()),
        current_stocks_ready=int(current.cvd_proxy_ready.sum()), latest_observed_clock=str(latest_bar),
        current_stocks_at_latest_clock=int(current.at_latest_observed_clock.sum()),
        features=dict(path=str(features_path.resolve()), sha256=sha(features_path)),
        membership=dict(path=str(membership_path.resolve()), sha256=sha(membership_path)),
        backtest_report=relative(backtest / "report.html", output))
    dump(output / "summary.json", summary)

    display = current[["symbol", "bar_end", "close", "price_from_open_pct", "cvd_proxy_session",
        "cvd_proxy_recent15", "estimated_session_pressure_pct", "estimated_recent15_pressure_pct", "at_latest_observed_clock", "cvd_proxy_reason"]].copy()
    display.columns = ["Stock", "Last bar (IST)", "Close", "Price from open %", "Estimated session CVD",
        "Estimated last 15m delta", "Session pressure %", "15m pressure %", "At latest clock", "Status"]
    table = display.to_html(index=False, border=0, table_id="stocks", na_rep="Unavailable", float_format=lambda v: f"{v:,.2f}")
    chart_data = {}
    for symbol, part in today_bars.groupby("symbol"):
        if not part.cvd_proxy_session.notna().any():
            continue
        chart_data[str(symbol)] = [[t.strftime("%H:%M"), None if pd.isna(v) else round(float(v), 2), round(float(p), 4)]
            for t, v, p in zip(part.bar_end, part.cvd_proxy_session, part.close)]
    payload = json.dumps(chart_data, separators=(",", ":"), allow_nan=False).replace("<", "\\u003c")
    options = "".join(f'<option>{html.escape(s)}</option>' for s in sorted(chart_data))
    backtest_summary = pd.read_csv(backtest / "overall_comparison.csv") if (backtest / "overall_comparison.csv").exists() else pd.DataFrame()
    backtest_metadata = json.loads((backtest / "summary.json").read_text(encoding="utf-8"))
    fetch_metadata = json.loads((membership_path.parent / "fetch_manifest.json").read_text(encoding="utf-8"))
    source_coverage = pd.read_csv(membership_path.parent / "coverage.csv")
    replay_coverage = source_coverage.loc[source_coverage.session_date.isin(backtest_metadata["days"])]
    complete_replay = int(replay_coverage.strategy_window_complete.astype(str).str.lower().eq("true").sum())
    source_note = (f"{complete_replay:,} of {len(replay_coverage):,} member stock-days across "
        f"{backtest_metadata['sessions']} sealed replay sessions have complete five-minute data through G3's 13:30 final entry signal. "
        "Historical series with missing closing bars are listed in source coverage; their displayed session totals stop at the last observed bar. ")
    missing_dates = fetch_metadata.get("membership_dates_without_any_provider_bars", [])
    if missing_dates:
        source_note += "Dated membership files with no returned candles: " + ", ".join(missing_dates) + ". "
    if fetch_metadata.get("aliases"):
        source_note += "Historical symbol aliases are resolved by verified equity instrument tokens; dated membership determines which label belongs in G3."
    if len(backtest_summary):
        mask = backtest_summary.cost_bps.eq(5) & backtest_summary.period.eq("all")
        columns = [x for x in ("variant", "trades", "win_rate_pct", "net_pnl_rs", "daily_close_drawdown_rs") if x in backtest_summary]
        comparison = backtest_summary.loc[mask, columns].copy()
        comparison["variant"] = comparison.variant.replace({"base_control": "Original G3", "covered_control": "G3 with the same CVD warmup",
            "cvd_session": "Session CVD filter", "cvd_recent15": "Last 15 minutes filter", "cvd_both": "Both CVD filters", "body_both": "Body/range sensitivity"})
        comparison = comparison.rename(columns={"variant": "Strategy", "trades": "Trades", "win_rate_pct": "Win rate (%)",
            "net_pnl_rs": "Net P&L (INR)", "daily_close_drawdown_rs": "Daily realized drawdown (INR)"})
        performance = comparison.to_html(index=False, border=0, float_format=lambda v: f"{v:,.2f}")
    else:
        performance = '<p>Open the backtest report for the completed comparisons.</p>'
    backtest_link = html.escape(relative(backtest / "report.html", output), quote=True)
    page = '''<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>G3 V18 · Estimated CVD from Kite</title><style>
body{margin:0;background:#0c1422;color:#e9edf5;font:15px/1.6 system-ui,sans-serif}main{max-width:1380px;margin:auto;padding:32px 28px}h1{font-size:34px;line-height:1.2}h2{margin-top:32px}a{color:#8ecbff}p{max-width:1000px}.muted{color:#a9b9d1}.notice{background:#202c40;border-left:4px solid #f0c579;padding:16px 20px}.cards{display:flex;flex-wrap:wrap;gap:14px;margin:24px 0}.card{background:#18263d;padding:15px 22px;border-radius:8px}.card strong{display:block;font-size:26px}.scroll{overflow:auto;max-height:650px}table{border-collapse:collapse;width:100%;white-space:nowrap;font-size:13px}th,td{padding:10px;border-bottom:1px solid #344158;text-align:right}th:first-child,td:first-child{text-align:left}th{background:#1c2d46;position:sticky;top:0}input,select{background:#1c2d46;color:#f4f6fa;border:1px solid #60728e;border-radius:6px;padding:9px;margin:8px 8px 12px 0}code{color:#a3decf}.charts{display:grid;grid-template-columns:1fr 1fr;gap:16px}svg{width:100%;background:#132139;border-radius:8px}button{cursor:pointer} @media(max-width:800px){.charts{grid-template-columns:1fr}main{padding:22px 14px}}
</style></head><body><main>
<p class="muted">G3 V18 · ISOLATED CANDLE-VOLUME RESEARCH</p><h1>Estimated CVD across the G3 stock universe</h1>
<p class="notice">This estimates volume pressure from five-minute OHLCV. The buy/sell allocations are mathematical estimates. Kite historical candles do not identify each trade's buyer or seller aggressor.</p>
<div class="cards"><div class="card">Historical symbols<strong>@@SYMBOLS@@</strong></div><div class="card">Completed five-minute bars<strong>@@BARS@@</strong></div><div class="card">Current universe<strong>@@CURRENT@@</strong></div><div class="card">Latest observed clock<strong>@@CLOCK@@</strong></div></div>
<h2>Calculation</h2><p><code>Estimated delta = volume × (2 × close − high − low) / (high − low)</code>. A flat candle contributes zero. Session CVD adds these estimates from the 09:15 open; the last 15 minutes adds the latest three completed candles. Dividing by volume in the same window gives a pressure percentage between −100% and +100%. A close near the high gives a positive estimate; a close near the low gives a negative estimate.</p>
<p>Only completed bars are used. Kite opening timestamps are shifted five minutes to match G3's completed signal clock. Session sums reset each day. Missing or invalid bars prevent a usable session estimate; no volume is invented. This calculation is similar to a session accumulation/distribution measure.</p>
<h2>Historical G3 comparison</h2><p class="notice">@@CONCLUSION@@</p><p>The replay keeps G3's original entry ranking, one-minute execution, capital and exits. Filters act before ranking so other eligible stocks can replace rejected candidates. The coverage control uses the same available CVD history as every tested filter. The table uses 5 bps modeled round-trip friction; the detailed report also tests 9 and 15 bps.</p><div class="scroll">@@PERFORMANCE@@</div><p><a href="@@BACKTEST@@">Open the full backtest, daily results and source comparisons →</a></p>
<p>These are retrospective research results. G3's inherited slot choices were selected using history through October 27; earlier results are not independent validation. The three later sealed sessions provide only a small descriptive check. Historical universe and opening-OI limitations remain. Today's CVD snapshot is separate from the completed-session backtest.</p>
<h2>Current session · @@DAY@@</h2><p>@@READY@@ of @@CURRENT@@ stocks have a usable estimate at their own latest bar; @@ATCLOCK@@ are at the latest observed clock. Each row shows its actual clock. Readiness describes data availability, not an instruction to trade.</p>
<label for="stock">Inspect stock:</label><select id="stock">@@OPTIONS@@</select><div class="charts"><div><p class="muted">Estimated session CVD · share-volume units</p><svg id="cvd" viewBox="0 0 650 260" role="img" aria-label="Estimated session CVD"></svg></div><div><p class="muted">Stock closing price · INR</p><svg id="price" viewBox="0 0 650 260" role="img" aria-label="Stock closing price"></svg></div></div>
<label for="search">Filter stocks:</label><input id="search" placeholder="Type a stock symbol" autocomplete="off"><div class="scroll">@@TABLE@@</div>
<h2>Saved outputs</h2><p><a href="latest_stock_snapshot.csv">Current all-stock snapshot (CSV)</a> · <a href="all_stock_daily_summary.csv">Every stock and historical session (CSV)</a> · <a href="historical_coverage.csv">Historical universe coverage (CSV)</a> · <a href="@@SOURCECOVERAGE@@">Missing source bars by stock and date (CSV)</a> · <a href="@@FEATURES@@">All computed five-minute candles (Parquet)</a> · <a href="summary.json">Source hashes and summary</a></p>
<p>@@SOURCENOTE@@</p>
<p class="muted">Data: <a href="https://kite.trade/docs/connect/v3/historical/">Kite historical candles</a>. Fetching uses cached completed requests, bounded retries and a pace below the <a href="https://kite.trade/docs/connect/v3/exceptions/">published historical API limit</a>. Existing G3 files are preserved. This folder has no live execution authority.</p>
</main><script>const series=@@DATA@@;function chart(id,rows,k){const svg=document.getElementById(id),valid=rows.filter(r=>r[k]!==null&&Number.isFinite(r[k]));if(!valid.length){svg.innerHTML='<text x="30" y="50" fill="#ddd">Unavailable</text>';return;}let lo=Math.min(...valid.map(r=>r[k])),hi=Math.max(...valid.map(r=>r[k]));if(id==='cvd'){lo=Math.min(lo,0);hi=Math.max(hi,0);}if(hi===lo){hi+=1;lo-=1;}const x=i=>62+i*565/Math.max(rows.length-1,1),y=v=>218-(v-lo)*186/(hi-lo);let d='',pen=false;rows.forEach((r,i)=>{if(r[k]===null||!Number.isFinite(r[k])){pen=false;return;}d+=(pen?'L':'M')+x(i).toFixed(1)+' '+y(r[k]).toFixed(1)+' ';pen=true;});let zero=(lo<=0&&hi>=0)?'<line x1="62" x2="627" y1="'+y(0)+'" y2="'+y(0)+'" stroke="#60728e" stroke-dasharray="4 4"/>':'';svg.innerHTML=zero+'<path d="'+d+'" fill="none" stroke="'+(id==='cvd'?'#6ce0c4':'#8cb9ff')+'" stroke-width="2"/><text x="8" y="25" fill="#b8c9de">'+hi.toLocaleString(undefined,{maximumFractionDigits:0})+'</text><text x="8" y="223" fill="#b8c9de">'+lo.toLocaleString(undefined,{maximumFractionDigits:0})+'</text><text x="62" y="248" fill="#b8c9de">'+rows[0][0]+'</text><text x="580" y="248" fill="#b8c9de">'+rows[rows.length-1][0]+'</text>';};function update(){const rows=series[document.getElementById('stock').value]||[];chart('cvd',rows,1);chart('price',rows,2);}document.getElementById('stock').addEventListener('change',update);document.getElementById('search').addEventListener('input',e=>{const q=e.target.value.trim().toUpperCase();document.querySelectorAll('#stocks tbody tr').forEach(r=>r.hidden=!r.cells[0].textContent.toUpperCase().includes(q));});update();</script></body></html>'''
    replacements = {"SYMBOLS": f"{summary['fetched_symbols']:,}", "BARS": f"{len(bars):,}",
        "CURRENT": str(len(current)), "CLOCK": latest_bar.strftime("%H:%M IST") if pd.notna(latest_bar) else "Unavailable",
        "PERFORMANCE": performance, "BACKTEST": backtest_link, "DAY": today,
        "READY": str(summary["current_stocks_ready"]), "ATCLOCK": str(summary["current_stocks_at_latest_clock"]),
        "OPTIONS": options, "TABLE": table, "FEATURES": html.escape(relative(features_path, output), quote=True),
        "SOURCECOVERAGE": html.escape(relative(membership_path.parent / "coverage.csv", output), quote=True),
        "SOURCENOTE": html.escape(source_note), "CONCLUSION": html.escape(backtest_metadata["conclusion"]), "DATA": payload}
    for key, value in replacements.items():
        page = page.replace("@@" + key + "@@", value)
    (output / "report.html").write_text(page, encoding="utf-8")
    dump(output / "manifest.json", {"files": {p.name: sha(p) for p in output.iterdir() if p.is_file()}})
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--features", type=Path, required=True)
    parser.add_argument("--membership", type=Path, required=True)
    parser.add_argument("--backtest", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--session-date", help="Explicit session shown in the snapshot; otherwise latest universe date")
    args = parser.parse_args()
    build(args.features, args.membership, args.backtest, args.output, args.session_date)
