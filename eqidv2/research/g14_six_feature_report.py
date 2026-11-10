"""Render the completed, audited V14 six-field run without changing results."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import pandas as pd


DEFAULT = Path("C:/TradingData/eqidv2/backtesting_result_v13_v10_g_14/runs/2026-11-03/six_feature_full_pool_v2_20261103")
VARIANTS = ("V14_V1_BASELINE", "A_direction_only", "B_with_gap", "C_three_paths_OR", "D_strict_OR")


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def table(frame: pd.DataFrame, columns: list[str]) -> str:
    return frame[columns].to_markdown(index=False, floatfmt=",.2f")


def fmt(frame: pd.DataFrame) -> pd.DataFrame:
    work = frame.copy()
    work["win rate"] = work.win_rate_pct.map(lambda x: f"{x:.1f}%")
    work["net ₹"] = work.net_pnl_rs.map(lambda x: f"{x:,.0f}")
    work["worst day ₹"] = work.worst_day_rs.map(lambda x: f"{x:,.0f}")
    work["drawdown ₹"] = work.max_daily_close_drawdown_rs.map(lambda x: f"{x:,.0f}")
    return work


def run(folder: Path) -> None:
    folder = folder.resolve()
    summary = json.loads((folder / "summary.json").read_text(encoding="utf-8"))
    overall = pd.read_csv(folder / "overall_by_period_and_cost.csv")
    daily = pd.read_csv(folder / "daywise_by_cost.csv")
    changes = pd.read_csv(folder / "trade_changes_vs_v14_v1.csv")
    if summary["selected_on_development_only"] is not None:
        raise ValueError("This report text must be revised if a rule qualified")
    if set(overall.variant) != set(VARIANTS) or set(daily.cost_bps) != {5., 9., 15.}:
        raise ValueError("V14 six-field result inventory changed")
    for cost in (5., 9., 15.):
        one = daily.loc[daily.cost_bps.eq(cost)].copy()
        if len(one) != 63 * len(VARIANTS):
            raise ValueError(f"Incomplete daywise inventory at {cost} bps")
        wide = one.pivot(index="day", columns="variant", values=["trades", "wins", "net_pnl_rs"])
        wide.columns = [f"{variant}_{field}" for field, variant in wide.columns]
        wide = wide.reset_index()
        for variant in VARIANTS[1:]:
            wide[f"{variant}_minus_v14_v1_rs"] = (
                wide[f"{variant}_net_pnl_rs"] - wide["V14_V1_BASELINE_net_pnl_rs"])
        wide.to_csv(folder / f"daywise_{int(cost)}bps_comparison.csv", index=False)

    all5 = fmt(overall.loc[overall.period.eq("all") & overall.cost_bps.eq(5)].copy())
    all9 = fmt(overall.loc[overall.period.eq("all") & overall.cost_bps.eq(9)].copy())
    later9 = fmt(overall.loc[overall.period.eq("assessment") & overall.cost_bps.eq(9)].copy())
    for frame in (all5, all9, later9):
        frame["name"] = frame.variant.map({
            "V14_V1_BASELINE": "V14 v1 baseline", "A_direction_only": "A: directional",
            "B_with_gap": "B: gap aligned", "C_three_paths_OR": "C: three path OR",
            "D_strict_OR": "D: strict OR"})
    columns = ["name", "trades", "wins", "win rate", "net ₹", "losing_days", "worst day ₹", "drawdown ₹"]
    md = ["# V14: six-field morning-entry backtest", "",
          "**Decision: retain V14 v1.** None of the four frozen six-field variants met the 70% development win-rate and downside screen. All four lost money during the later assessment. The V14 v1 backtest remains available unchanged for rollback. No live or paper configuration was changed.", "",
          "## What was tested", "",
          "The entry decision used only opening gap %, completed-signal move from the official previous-day cash close (PDC %), completed five-minute price move %, current same-contract futures OI, OI change over the last completed bar, and same-day OI change from 09:20. The 09:20 OI change is the observed 09:16→09:20 **four-minute** proxy, and OI day change is zero at 09:20. Absolute OI was a validity check, because its level is not comparable between stocks.", "",
          "The 12 cells were 09:20, 09:25, 09:30, 09:35, 09:40 and 09:45, each LONG and SHORT. EMA, relative volume, body, wick, VWAP, Nifty direction, RSI and one-minute directional admission gates were removed. The next-minute candle defines the time-valid entry trigger, not another alpha vote. Original V14 targets, 1.2% initial stop, 1.0% stop after 120 minutes, +0.1% protection after a completed +0.6% close, ten-minute entry expiry, stop-first ties, fixed capital and two-realized-loss admission lock were retained.", "",
          "Four rule variants were frozen in `research/g14_six_feature_rule_protocol.json` before examining their outcomes. A required a directional PDC move ≥0.20%, five-minute move ≥0.10%, and nonnegative OI changes. B added an aligned opening gap. C accepted three complete price paths (continuation, flat gap or reversal). D tightened the C price/OI thresholds. The same thresholds applied to every slot and side; a six-field score ranked eligible stocks within each preserved V14 slot quota.", "",
          "## Whole history at 5 bps round-trip modeled cost", "",
          table(all5, columns), "",
          "## Whole history at 9 bps", "",
          table(all9, columns), "",
          "## Later assessment at 9 bps", "",
          "July 29–September 30 (43 sessions) was the development block; October 5–November 2 (20 available sessions) was the chronological assessment. These dates had seen earlier G7/V14 research, so the assessment is **not** a virgin future holdout.", "",
          table(later9, columns), "",
          "At 9 bps, the most selective variant (D) still executed 485 trades at 59.8% wins over all 63 days and lost ₹148,860. The gap-aligned B variant lost ₹138,949 with 59.9% wins. The unchanged V14 v1 executed 73 trades at 76.7% wins and made ₹131,435. In the later 20 sessions, B made -₹140,623 on 161 trades (54.0% wins), whereas V14 v1 made +₹26,526 on 22 trades (72.7% wins). This is a material failure of the six-field-only replacement.", "",
          "## Coverage and limits", "",
          "The full prefilter candidate grid contained 79,536 stock×slot records from 63 source sessions; 70,915 passed the six-feature signal integrity checks. Official NSE prior-day closing prices were available for 13,255 of 13,256 stock-days. At 09:20 only 6,516 of 13,256 candidate rows passed all six-field checks, largely because the four-minute OI comparison was absent. Missing inputs were excluded, never imputed. The 09:40 and 09:45 signal clocks on October 28, 29, 30 and November 2 were reconstructed from hash-verified archived minute sources before selection. See `research/g14_six_feature_source_audit.md` for hashes and exact reasons.", "",
          "This comparison changes the admission gates and the resulting ranked book together, so it does not isolate one indicator's causal effect. More trades raised the number of both winners and losers; at 5 bps the A variant added 300 winners and 206 losers relative to V14 v1 executions while omitting 49 of the V14 v1's 56 winners. The result is sensitive to the observed source universe, modeled fills and costs. Twenty-four unique selected 09:20 candidates during October 13–23 have an archived completed OI bar but no separately recorded OI receipt timestamp; their intraday receipt timing cannot be independently proved from this source. A 70% historical win rate cannot be guaranteed, and high win rate alone is insufficient if losses or costs dominate.", "",
          "## Files", "",
          "- `overall_by_period_and_cost.csv`: complete 5/9/15 bps development, assessment and full-period metrics.",
          "- `daywise_5bps_comparison.csv`, `daywise_9bps_comparison.csv`, `daywise_15bps_comparison.csv`: 63-day side-by-side counts, wins, P&L and difference from V14 v1.",
          "- `slot_side_results.csv`: executed results by slot and LONG/SHORT.",
          "- `executed_trades.csv`, `all_variant_orders.csv`: execution and rejection details.",
          "- `candidate_rule_decisions.csv` and `candidate_rule_decision_counts.csv`: every candidate's four rule decisions and grouped exclusion reasons.",
          "- `candidate_coverage_by_day.csv`, `development_rule_screen.csv`, `trade_changes_vs_v14_v1.csv`: coverage and decision audit.",
          "- `summary.json`, `artifact_manifest.json`: source and output hashes. The archived V14 v1 output is `../morning12_config_complete_v1/`.", "",
          "The prior-close source is NSE's [CM UDiFF Bhavcopy](https://www.nseindia.com/all-reports)."]
    (folder / "REPORT.md").write_text("\n".join(md) + "\n", encoding="utf-8")
    manifest_path = folder / "artifact_manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["files"] = {p.name: sha(p) for p in folder.iterdir()
                         if p.is_file() and p.name != "artifact_manifest.json"}
    manifest["report_code_sha256"] = sha(Path(__file__))
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("folder", nargs="?", type=Path, default=DEFAULT)
    arguments = parser.parse_args()
    run(arguments.folder)
    print((arguments.folder / "REPORT.md").resolve())
