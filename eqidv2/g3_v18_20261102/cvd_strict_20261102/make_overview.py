"""Publish a compact, read-only view of the frozen strict CVD replay."""
from __future__ import annotations

import csv
import hashlib
import html
import json
import math
from pathlib import Path

ROOT = Path(__file__).resolve().parent
RESULTS = ROOT / "results_20261102"
LABELS = {
    "base_control": "Original G3",
    "cvd_ready_control": "CVD-ready control",
    "rvol_ready_control": "RVOL-ready control",
    "aligned_gate": "Aligned CVD",
    "strict_gate": "Strict CVD",
    "strict_rvol_gate": "Strict CVD + volume",
}
COLORS = ["#64748b", "#0e7490", "#7c3aed", "#ca8a04", "#047857", "#dc2626"]


def esc(value):
    return html.escape(str(value))


def money(value, signed=False):
    value = float(value)
    sign = "+" if value > 0 and signed else "-" if value < 0 else ""
    return f"{sign}₹{abs(value):,.0f}"


def percent(value):
    return f"{float(value):.1f}%" if value is not None and math.isfinite(float(value)) else "—"


def records(name):
    with (RESULTS / name).open(encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def table(headers, rows):
    return '<div class="scroll"><table><thead><tr>' + ''.join(f'<th>{esc(h)}</th>' for h in headers) + '</tr></thead><tbody>' + ''.join('<tr>' + ''.join(f'<td>{esc(c)}</td>' for c in row) + '</tr>' for row in rows) + '</tbody></table></div>'


def chart(daily):
    series = {}
    for variant in LABELS:
        rows = sorted((r for r in daily if r["variant"] == variant and float(r["cost_bps"]) == 5), key=lambda r: r["day"])
        total, points = 0., [0.]
        for row in rows:
            total += float(row["net_pnl_rs"])
            points.append(total)
        series[variant] = points
    values = [n for points in series.values() for n in points]
    low, high = min(0., min(values)), max(1., max(values))
    padding = .08 * (high - low)
    low, high = low-padding, high+padding
    x = lambda i, count: 82 + i * 930 / max(1, count-1)
    y = lambda v: 282 - (v-low) * 248/(high-low)
    parts = ['<svg viewBox="0 0 1060 330" role="img" aria-label="Cumulative daily realized net profit for six backtest variants"><rect x="0" y="0" width="1060" height="330" fill="white"/>']
    for i in range(5):
        value = low + (high-low)*i/4
        parts.append(f'<line x1="82" y1="{y(value):.1f}" x2="1012" y2="{y(value):.1f}" stroke="#e2e8f0"/><text x="72" y="{y(value)+4:.1f}" text-anchor="end" fill="#64748b" font-size="12">₹{value/100000:.1f}L</text>')
    for (variant, points), color in zip(series.items(), COLORS):
        path = ' '.join(f'{x(i,len(points)):.1f},{y(value):.1f}' for i,value in enumerate(points))
        parts.append(f'<polyline points="{path}" fill="none" stroke="{color}" stroke-width="2.5"><title>{esc(LABELS[variant])}: {money(points[-1])}</title></polyline>')
    parts.append('<text x="82" y="315" fill="#64748b" font-size="13">29 Jul 2026</text><text x="1012" y="315" text-anchor="end" fill="#64748b" font-size="13">30 Oct 2026 · 62 sealed sessions</text></svg>')
    parts.append('<div class="legend">' + ''.join(f'<span><i style="background:{color}"></i>{esc(LABELS[variant])}</span>' for variant,color in zip(LABELS,COLORS)) + '</div>')
    return ''.join(parts)


def main():
    manifest = json.loads((RESULTS / "manifest.json").read_text(encoding="utf-8"))
    for name, digest in manifest["files"].items():
        if hashlib.sha256((RESULTS / name).read_bytes()).hexdigest() != digest:
            raise ValueError(f"Result artifact changed: {name}")
    summary = json.loads((RESULTS / "summary.json").read_text(encoding="utf-8"))
    metrics = {(r["variant"], r["period"], int(r["cost_bps"])): r for r in summary["overall_metrics"]}
    analysis = {(r["variant"], r["reference_variant"], r["period"]):r for r in records("trade_change_analysis.csv")}
    matched = {"aligned_gate":"cvd_ready_control", "strict_gate":"cvd_ready_control", "strict_rvol_gate":"rvol_ready_control"}
    def performance(period):
        rows = []
        for variant, label in LABELS.items():
            r = metrics[variant, period, 5]
            rows.append([label, int(r["trades"]), percent(r["win_rate_pct"]), money(r["net_pnl_rs"]), money(r["daily_close_drawdown_rs"])])
        return table(["Variant", "Executed trades", "Win rate", "Net profit", "Daily realized drawdown"],rows)
    comparisons, removals = [], []
    for variant, reference in matched.items():
        for period, period_label in [("all","All 62 sessions"),("october","October")]:
            own, ref = metrics[variant,period,5], metrics[reference,period,5]
            fraction = f'{100*own["trades"]/ref["trades"]:.1f}%' if ref["trades"] else "N/A"
            comparisons.append([LABELS[variant], period_label, LABELS[reference], fraction, money(own["net_pnl_rs"]-ref["net_pnl_rs"],True), money(own["daily_close_drawdown_rs"]-ref["daily_close_drawdown_rs"],True)])
        change = analysis[variant,reference,"all"]
        removals.append([LABELS[variant], int(change["removed_winning_trades"]), int(change["removed_losing_trades"]), int(change["added_executed"]), money(change["removed_net_pnl_rs"]), money(change["added_net_pnl_rs"])])
    strict, ready = metrics["strict_gate","all",5], metrics["cvd_ready_control","all",5]
    changed = analysis["strict_gate","cvd_ready_control","all"]
    screen = summary["descriptive_screen_passing_variants"]
    conclusion = ("Passed the frozen October screen: " + ', '.join(LABELS[v] for v in screen) + ". This is a candidate for prospective evaluation, not validation on unseen data.") if screen else "The gates changed trade selection, but none passed the frozen October improvement screen against its matched control. Keep the original G3 settings."
    screening = table(["Gate", "Matched control", "October screen"],[[LABELS[r["variant"]],LABELS[r["reference_variant"]],"Passed" if r["descriptive_screen_pass"] else "Did not pass"] for r in summary["comparisons"]])
    cards = f'<div class="cards"><article><small>Strict CVD · executed trades</small><strong>{strict["trades"]} <span>vs {ready["trades"]}</span></strong><p>Compared with the CVD-ready control</p></article><article><small>Strict CVD · net profit change</small><strong>{money(strict["net_pnl_rs"]-ready["net_pnl_rs"],True)}</strong><p>All 62 sessions, after modeled costs</p></article><article><small>Strict CVD · reference trades removed</small><strong>{int(changed["removed_executed_trades"])}</strong><p>{int(changed["removed_winning_trades"])} winners · {int(changed["removed_losing_trades"])} losers</p></article></div>'
    body = f'''<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>G3 V18 · Strict CVD results</title><style>
*{{box-sizing:border-box}}body{{margin:0;background:#f2f5f8;color:#142b3f;font:16px/1.55 system-ui,sans-serif}}main{{max-width:1180px;margin:auto;padding:44px 24px 60px}}h1{{font-size:38px;line-height:1.18;margin:10px 0 18px}}h2{{font-size:23px;margin:0 0 14px}}p{{margin:10px 0}}.eyebrow{{font-size:12px;font-weight:700;letter-spacing:.13em;color:#53677a;text-transform:uppercase}}.lead{{font-size:19px;max-width:980px}}.status{{border-left:4px solid #b47a15;background:#fff4d6;padding:16px 20px;margin:24px 0}}.cards{{display:grid;grid-template-columns:repeat(3,1fr);gap:16px;margin:24px 0}}article,section{{background:white;border:1px solid #dce4ec;border-radius:12px;padding:24px}}article small{{display:block;color:#52677b}}article strong{{display:block;font-size:30px;margin:10px 0}}article strong span{{font-size:17px;color:#64748b}}article p,.muted{{font-size:14px;color:#52677b}}section{{margin-top:20px}}.scroll{{overflow:auto}}table{{border-collapse:collapse;width:100%;font-size:14px}}th,td{{text-align:right;padding:12px 14px;border-bottom:1px solid #e3eaf0;white-space:nowrap}}th{{color:#53677a;background:#f8fafc;font-size:12px}}th:first-child,td:first-child{{text-align:left}}svg{{width:100%;height:auto}}.legend{{display:flex;gap:10px 22px;flex-wrap:wrap;font-size:13px}}.legend i{{display:inline-block;width:13px;height:3px;vertical-align:middle;margin-right:6px}}a{{color:#06658c}}.links{{display:flex;gap:12px 24px;flex-wrap:wrap;margin-top:16px}}.rules{{display:grid;grid-template-columns:repeat(3,1fr);gap:20px}}.rules b{{display:block;margin-bottom:6px}}@media(max-width:760px){{main{{padding:24px 14px}}h1{{font-size:30px}}.cards,.rules{{grid-template-columns:1fr}}section,article{{padding:18px}}}}
</style></head><body><main><div class="eyebrow">G3 V18 · isolated research experiment · 2 November 2026</div><h1>Stricter CVD. Measured trade impact.</h1><p class="lead">CVD now rejects candidates before G3 ranking, so eligible replacements can enter. The thresholds were fixed before this run.</p><div class="status">{esc(conclusion)}</div>{cards}
<section><h2>Three hard entry gates</h2><div class="rules"><div><b>Aligned CVD</b>Recent 15-minute pressure and acceleration must both support the trade direction.</div><div><b>Strict CVD</b>Directional pressure ≥10%, and pressure acceleration ≥5 percentage points.</div><div><b>Strict CVD + volume</b>The strict gate plus same-clock relative volume ≥1.20×.</div></div><p class="muted">Both long and short trades use their own direction. Missing features reject the candidate. The first CVD-eligible signal is 09:45 IST; relative volume also needs 20 prior sessions.</p></section>
<section><h2>All 62 sessions · 29 July–30 October</h2>{performance('all')}<p class="muted">5 bps total modeled round-trip friction. Net profit uses the existing G3 fixed-capital portfolio rules. Drawdown uses daily realized P&amp;L, not intraday open-position values.</p>{chart(records('daily_results.csv'))}</section>
<section><h2>Effect after matching data availability</h2><p>Aligned and strict gates are compared with the CVD-ready control. The volume gate is compared with the RVOL-ready control. This separates indicator thresholds from losses of opening trades and historical warmup.</p>{table(['Gate','Period','Compared with','Trade retention','Net profit change','Drawdown change'],comparisons)}<p class="muted">Negative net profit change is worse. Negative drawdown change is better. Trade retention is executed trade count relative to the named control; it includes replacement trades.</p></section>
<section><h2>October results</h2>{performance('october')}{screening}<p class="muted">The screen requires at least max(20, 50% of matched-control trades), no lower net profit, no greater drawdown, a selection change, and a strict profit or drawdown improvement.</p></section>
<section><h2>What the gates removed and added</h2>{table(['Gate','Removed winners','Removed losers','Added executed trades','Removed net profit','Added net profit'],removals)}<p class="muted">All-session changes against each gate’s matched control, at 5 bps. Removed net profit is the sum of reference trades excluded by the new selection. Shared trades can also change because of portfolio constraints; the detailed attribution reconciles the entire P&amp;L difference.</p></section>
<section><h2>Evidence and limits</h2><p>The input is verified Kite one-minute OHLCV, aggregated into completed five-minute decision bars. Estimated delta weights volume by the close’s location in each minute’s range. It does not observe buyer-initiated or seller-initiated trade volume.</p><p>All 62 sessions were already viewed in earlier research. There is no unseen holdout, and stricter selection alone is not proof of predictive value. Historical universe membership and opening open-interest inputs retain the original retrospective limitations. The RVOL history requirement first becomes available on August 26 in this capture.</p><p>November 2 has no sealed G3 replay source; the latest current-universe feature capture is 15:15 IST that day. October 28 retains the inherited restricted 215-stock cohort.</p><p>The original G3 filters, ranking of survivors, core and extra quotas, confirmation, execution, exits and capital model are retained. Existing files and previous results are preserved.</p><div class="links"><a href="../results_20261102/report.html">Detailed results and 5/9/15 bps sensitivity</a><a href="../results_20261102/overall_comparison.csv">All performance results</a><a href="../results_20261102/trade_change_analysis.csv">Trade changes</a><a href="../results_20261102/pnl_attribution.csv">Trade-level attribution</a><a href="../results_20261102/gate_reasons.csv">Gate rejection reasons</a><a href="../methodology.json">Frozen methodology</a><a href="../results_20261102/verification.json">Replay verification</a><a href="../final_verification.json">Preservation and publication checks</a><a href="../README.md">Reproduction instructions</a></div></section><p class="muted">Backtest research only. No live trading configuration was changed.</p></main></body></html>'''
    target = ROOT / "dashboard_20261102"
    target.mkdir(exist_ok=True)
    (target / "report.html").write_text(body, encoding="utf-8")
    (ROOT / "START_HERE.html").write_text('<!doctype html><html lang="en"><meta charset="utf-8"><meta http-equiv="refresh" content="0; url=dashboard_20261102/report.html"><title>G3 strict CVD results</title><p><a href="dashboard_20261102/report.html">Open strict CVD test results</a></p></html>',encoding="utf-8")
    print(json.dumps({"published":str(target / "report.html"),"verified_result_artifacts":len(manifest["files"])}))


if __name__ == "__main__":
    main()
