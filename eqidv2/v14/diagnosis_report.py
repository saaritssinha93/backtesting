"""Readable, evidence-linked report for the completed V14 transfer diagnosis."""
from __future__ import annotations

import argparse
import html
import json
import re
from pathlib import Path

import pandas as pd

from .research import sha256, write_json


def render(output: Path, diagnostics: Path):
    output, diagnostics = Path(output), Path(diagnostics)
    summary = json.loads((output / "summary.json").read_text())
    coverage = json.loads((output / "evaluation_coverage.json").read_text())
    metrics = pd.read_csv(output / "variant_comparison.csv")
    ablation = pd.read_csv(diagnostics / "fno_oi_ablation/comparison.csv")
    capacities = [name for name in coverage["own_days"] if name.startswith("capacity")]
    if not all(coverage["own_days"][name] == coverage["own_days"][capacities[0]] for name in capacities):
        raise ValueError("Capacity variants no longer share identical dates")
    labels = {"capacity_guard": "Liquidity limits only",
              "capacity_anti_chase": "Liquidity + price-extension limits",
              "capacity_anti_chase_rvol1p5": "Both + RVOL 1.5",
              "capacity_anti_chase_rvol1p5_cmf0": "Both + RVOL 1.5 + directional CMF"}
    base = metrics.loc[metrics.scope.eq("own_coverage_only") & metrics.additional_bps.eq(0)
                       & metrics.variant.isin(capacities)].copy()
    table = base.pivot(index="variant", columns="split", values="net_pnl")
    table = base.loc[base.split.eq("ALL"), ["variant", "trades", "max_drawdown"]].set_index("variant").join(table)
    table = table.reindex(capacities).rename(index=labels).rename(columns={"trades":"Trades", "max_drawdown":"Drawdown", "ALL":"Net total"})
    table = table[["Trades", "TRAIN", "VALIDATION", "TEST", "Net total", "Drawdown"]]
    table.index.name = "Configuration"
    selected = summary["selection"]["name"]
    stress = metrics.loc[metrics.scope.eq("common_all_dates") & metrics.variant.eq(selected) & metrics.split.eq("ALL"),
                         ["additional_bps", "sessions", "trades", "net_pnl"]]
    stress = stress.rename(columns={"additional_bps":"Extra round-trip bps", "sessions":"Sessions", "trades":"Trades", "net_pnl":"Net P&L"})
    comparison = pd.read_csv(diagnostics / "g3_v14_profit_comparison.csv")
    comparison["comparison"] = comparison.comparison.str.strip()
    reference = comparison.set_index("comparison")
    g3 = reference.loc["G3_MATCHED_V14_DATES"]
    v14 = reference.loc["V14_NO_OI_CONTROL_MATCHED"]
    oi_off = ablation.set_index("variant").loc["OI_OFF"]
    # These independent diagnostics are linked in full as well as summarized.
    ablation_html = ablation.to_html(index=False, float_format=lambda x: f"{x:,.2f}")
    font = "font:16px/1.55 system-ui,-apple-system,sans-serif"
    document = f"""<!doctype html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1"><title>Why G3 and V14 differ</title>
<style>body{{{font};background:#101721;color:#e6edf7;margin:0}}main{{max-width:1180px;margin:auto;padding:32px}}
h1{{font-size:32px;line-height:1.2}}h2{{font-size:23px;margin-top:32px}}a{{color:#93c5fd}}.notice{{border-left:5px solid #fbbf24;background:#293042;padding:16px}}
table{{border-collapse:collapse;width:100%;font-size:14px}}th,td{{padding:9px;border:1px solid #435065;text-align:right}}th:first-child,td:first-child{{text-align:left}}
.table{{overflow-x:auto}}.small{{color:#b7c4d9;font-size:14px}}code{{background:#293042;padding:2px 5px}}details{{margin:20px 0}}li{{margin-bottom:8px}}</style></head>
<body><main><h1>Why profitable G3 did not transfer to V14</h1>
<p class="notice"><strong>The OI filter and the stock universe are substantial strategy changes.</strong>
The calculation audit found no mismatch in sampled indicators or archived execution. Entry-quality filters improve the historical results, but no tested V14 candidate has passed the profitability screen.</p>
<h2>First, compare the same original 18 dates</h2>
<div class="table"><table><thead><tr><th>Strategy</th><th>Executed trades</th><th>Net P&amp;L</th><th>Win rate</th></tr></thead><tbody>
<tr><td>Original G3, F&amp;O universe, OI on</td><td>{g3.executed:.0f}</td><td>INR {g3.net_pnl:+,.0f}</td><td>{g3.win_rate_pct:.1f}%</td></tr>
<tr><td>Same F&amp;O universe, only OI removed</td><td>{oi_off.executed:.0f}</td><td>INR {oi_off.net_pnl:+,.0f}</td><td>{oi_off.win_rate_pct:.1f}%</td></tr>
<tr><td>V14 non-F&amp;O universe, no-OI control</td><td>{v14.executed:.0f}</td><td>INR {v14.net_pnl:+,.0f}</td><td>{v14.win_rate_pct:.1f}%</td></tr>
</tbody></table></div>
<p>G3's full archive made INR232,886 across 46 dates. That total should not be compared directly with the original V14's 18-date result.
The same-universe OI test used broad candidates before OI filtering, exactly reproduced G3 with OI enabled, and had complete execution paths for all94 unique selections.</p>
<ul><li><strong>OI supplied useful selectivity in this sample.</strong> It tracks outstanding contracts; traded volume measures transactions and is not an equivalent signal. <a href="https://www.cmegroup.com/education/lessons/open-interest">CME explanation</a>.</li>
<li><strong>More trades amplified weak entries.</strong> V14 traded5.4 times as often. Extra costs account for INR35,000 of the INR170,105 matched-date gap; poorer gross outcomes account for the remaining INR135,105.</li>
<li><strong>The unchanged stop often followed an already extended move.</strong> In the original V14 control,61 trades with a five-minute move above1.25% lost INR103,877. Those are descriptive loss groups; the experiment below reselects stocks after applying causal gates.</li>
<li><strong>Thin trading affects capacity and data coverage.</strong> For unresolved original selections, the planned INR500,000 order was a median30.9% of signal five-minute turnover, versus0.408% for frozen F&amp;O G3 selections. A turnover limit is an execution-realism screen, not proof of alpha.</li>
<li><strong>The original G3 archive reused this history.</strong> Its tuned rules and targets do not constitute fresh validation when transferred to a different universe.</li></ul>
<h2>What was changed and tested</h2>
<p>Six fixed configurations filter candidates before the original stock ranking. The indicators,14 setups, confirmation, INR500,000 notional, native stops, targets and portfolio logic are preserved.</p>
<ul><li>Capacity: planned order at most5% of signal five-minute turnover and10% of confirmation-minute turnover.</li>
<li>Price extension: absolute five-minute move at most1.25%; signal range/close and distance from EMA9 at most2.5%.</li>
<li>Participation: optional same-clock interval RVOL at least1.5, then optional side-aligned CMF10 at least0.</li></ul>
<h2>The broader comparison: identical 48 dates</h2>
<p>All four configurations below have exactly the same48 evaluated dates, verified from their date lists. All figures are INR after the inherited5bps cost. Historical TRAIN/VALIDATION/TEST labels retain the original calendar partitions; every period has already been examined.</p>
<div class="table">{table.to_html(float_format=lambda x: f'{x:,.2f}')}</div>
<p>Price-extension limits substantially reduce the loss. Adding both RVOL and CMF gives the smallest48-date loss, but it still loses overall and on validation. This all-period observation does not change the frozen historical selector.</p>
<h2>Why a positive subset is insufficient</h2>
<p>The six-arm common comparison has18 dates, only16 of which overlap the original18. Its historical selector chose <code>{html.escape(selected)}</code>: TRAIN +INR66,852, VALIDATION -INR15,453, historical TEST -INR38,134. Total +INR13,265. It fails validation and cost sensitivity:</p>
<div class="table">{stress.to_html(index=False, float_format=lambda x: f'{x:,.2f}')}</div>
<p class="notice"><strong>No profitable default was promoted.</strong> The original selected profile and baseline remain unchanged. The fixed research candidate and all alternatives are retained for review. A future candidate needs frozen rules and genuinely unseen sessions; extending threshold searches over these same dates would add overfitting risk.</p>
<h2>Data and verification</h2>
<p>The original16-arm study lost33 of51 dates because any unresolved selected trade excluded that whole date across all arms. The liquidity-filtered configurations now have48 usable dates. Missing candles were not invented; sparse selected paths remain unresolved. The provider had already been queried for the original missing timestamps.</p>
<p>The feature audit compared3.23 million five-minute values across12 deterministically selected G3 symbols, plus141,696 confirmation values and2,516 archived feature values, with no discrepancies at the stated numerical tolerance. All93 archived G3 executions were separately covered by baseline tests.</p>
<p><a href="report.html">Full six-configuration report</a> · <a href="variant_comparison.csv">All metrics and cost stresses</a> · <a href="evaluation_coverage.json">Exact evaluated dates</a> · <a href="research_candidate.json">Frozen research candidate</a> · <a href="control_reconciliation.json">Original control reconciliation</a></p>
<details><summary>Independent same-F&amp;O-universe OI ablation</summary><div class="table">{ablation_html}</div></details>
<p class="small">Research only. Historical MIS membership is approximated where unavailable; uniform5x exposure and flat costs are modeled assumptions. These diagnostics do not establish actual executable returns. Generated from immutable completed studies; no orders are sent.</p>
</main></body></html>"""
    # Normalize numeric prose spacing without touching identifiers or URLs.
    document = re.sub(r"\b(INR|all|All|traded|above|median|versus|most|and|least|same|inherited|smallest|has|only|original|lost|of|have|compared|across|plus|uniform)(?=\d)", r"\1 ", document)
    document = document.replace("control,61", "control, 61").replace("indicators,14", "indicators, 14").replace("5bps", "5 bps")
    target = output / "diagnosis.html"
    target.write_text(document, encoding="utf-8")
    source = Path(summary["source_study"])
    write_json(output / "diagnosis_provenance.json", dict(
        generated_after_study=True, strategy_or_selection_changed=False,
        capacity_date_sets_identical=True, capacity_evaluated_dates=coverage["own_days"][capacities[0]],
        source_artifact_sha256={name:sha256(source / name) for name in (
            "evaluation_coverage.json", "selected_no_oi_control.csv", "trades_no_oi_control.csv", "summary.json")},
        report_source_sha256=sha256(Path(__file__)), report_sha256=sha256(target),
        ablation_summary_sha256=sha256(diagnostics / "fno_oi_ablation/summary.json")))
    return target


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--diagnostics", type=Path, required=True)
    args = parser.parse_args()
    print(render(args.output, args.diagnostics))


if __name__ == "__main__":
    main()
