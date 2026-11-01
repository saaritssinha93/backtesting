"""Package the frozen missing-midday audit without changing its selections."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pandas as pd


OUT = Path(__file__).resolve().parent
SETUPS = ("1051_LONG", "1111_SHORT", "1146_SHORT")
STRATEGIES = ("G3", "G7_V4_LOCK2")
PERIODS = ("development", "validation", "assessment", "final_sessions", "later", "october", "all")


def sha(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def finite(value):
    value = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    return None if pd.isna(value) else float(value)


setup = pd.read_csv(OUT / "variant_setup_metrics.csv")
portfolio = pd.read_csv(OUT / "variant_portfolio_metrics.csv")
dev = pd.read_csv(OUT / "development_selection.csv")
selected = pd.read_csv(OUT / "selected_variant_results.csv")
daywise = pd.read_csv(OUT / "selected_variant_daywise.csv")
trade_details = pd.read_csv(OUT / "variant_trade_details.csv", low_memory=False)
parity = json.loads((OUT / "baseline_reproduction.json").read_text(encoding="utf-8"))

choices: dict[tuple[str, str], str | None] = {}
for setup_id in SETUPS:
    for strategy in STRATEGIES:
        chosen = dev.loc[
            dev.setup_id.eq(setup_id) & dev.strategy.eq(strategy) & dev.selected_from_development
        ]
        choices[(setup_id, strategy)] = None if chosen.empty else str(chosen.variant.iloc[0])

decision_rows = []
for setup_id in SETUPS:
    for strategy in STRATEGIES:
        choice = choices[(setup_id, strategy)]
        variant = choice or "baseline"
        rows = setup.loc[
            setup.setup_id.eq(setup_id) & setup.strategy.eq(strategy) & setup.variant.eq(variant)
            & setup.cost_bps.eq(5.0)
        ].set_index("period")
        base = setup.loc[
            setup.setup_id.eq(setup_id) & setup.strategy.eq(strategy) & setup.variant.eq("baseline")
            & setup.cost_bps.eq(5.0)
        ].set_index("period")
        development = rows.loc["development"]
        later = rows.loc["later"]
        overall = rows.loc["all"]
        october = rows.loc["october"]
        later_target = bool(later.trades > 0 and later.win_rate_pct > 75.0)
        overall_target = bool(overall.trades > 0 and overall.win_rate_pct > 75.0)
        wilson_target = bool(overall.trades > 0 and overall.wilson_95_lower_pct > 75.0)
        if choice is None:
            reason = "No non-baseline rule passed every frozen development gate."
        else:
            reason = (
                f"Development-only diagnostic passed, but later win rate was {later.win_rate_pct:.2f}% "
                f"({int(later.wins)}/{int(later.trades)}) and all-history win rate was "
                f"{overall.win_rate_pct:.2f}% ({int(overall.wins)}/{int(overall.trades)}); "
                "the >75% target did not persist outside development."
            )
        decision_rows.append({
            "setup_id": setup_id,
            "strategy": strategy,
            "development_selected_variant": choice,
            "development_gate_passed": choice is not None,
            "development_trades": int(development.trades),
            "development_wins": int(development.wins),
            "development_losses": int(development.losses),
            "development_win_rate_pct": finite(development.win_rate_pct),
            "development_wilson_95_lower_pct": finite(development.wilson_95_lower_pct),
            "development_net_pnl_rs": finite(development.net_pnl_rs),
            "development_net_change_vs_baseline_rs": finite(development.net_change_vs_baseline_rs),
            "development_loss_amount_reduction_rs": finite(development.loss_amount_reduction_vs_baseline_rs),
            "later_trades": int(later.trades),
            "later_wins": int(later.wins),
            "later_losses": int(later.losses),
            "later_win_rate_pct": finite(later.win_rate_pct),
            "later_wilson_95_lower_pct": finite(later.wilson_95_lower_pct),
            "later_net_pnl_rs": finite(later.net_pnl_rs),
            "later_net_change_vs_baseline_rs": finite(later.net_change_vs_baseline_rs),
            "later_loss_amount_reduction_rs": finite(later.loss_amount_reduction_vs_baseline_rs),
            "october_trades": int(october.trades),
            "october_wins": int(october.wins),
            "october_losses": int(october.losses),
            "october_win_rate_pct": finite(october.win_rate_pct),
            "october_net_pnl_rs": finite(october.net_pnl_rs),
            "all_trades": int(overall.trades),
            "all_wins": int(overall.wins),
            "all_losses": int(overall.losses),
            "all_win_rate_pct": finite(overall.win_rate_pct),
            "all_wilson_95_lower_pct": finite(overall.wilson_95_lower_pct),
            "all_wilson_familywise_110_lower_pct": finite(overall.wilson_familywise_110_lower_pct),
            "all_wilson_familywise_220_lower_pct": finite(overall.wilson_familywise_220_lower_pct),
            "all_net_pnl_rs": finite(overall.net_pnl_rs),
            "all_net_change_vs_baseline_rs": finite(overall.net_change_vs_baseline_rs),
            "all_loss_amount_reduction_rs": finite(overall.loss_amount_reduction_vs_baseline_rs),
            "later_point_win_rate_gt_75": later_target,
            "all_point_win_rate_gt_75": overall_target,
            "all_wilson_lower_gt_75": wilson_target,
            "promotion_variant": "baseline",
            "promotion_decision": "RETAIN_ACCEPTED_BASELINE",
            "reason": reason,
        })

decisions = pd.DataFrame(decision_rows)
decisions.to_csv(OUT / "promotion_decision.csv", index=False)

# The development-locked variants remain visible as diagnostics for every
# chronological period.  Final recommendation rows always use the baseline.
selected.loc[
    selected.cost_bps.eq(5.0) & selected.period.isin(PERIODS)
].to_csv(OUT / "development_locked_chronology.csv", index=False)

final_rows = []
for setup_id in SETUPS:
    for strategy in STRATEGIES:
        slot = setup.loc[
            setup.setup_id.eq(setup_id) & setup.strategy.eq(strategy)
            & setup.variant.eq("baseline") & setup.period.isin(PERIODS)
        ].copy()
        port = portfolio.loc[
            portfolio.setup_id.eq(setup_id) & portfolio.strategy.eq(strategy)
            & portfolio.variant.eq("baseline") & portfolio.period.isin(PERIODS)
        ].copy()
        merged = slot.merge(port, on=["strategy", "setup_id", "variant", "cost_bps", "period"],
                            suffixes=("_setup", "_portfolio"), validate="one_to_one")
        merged.insert(0, "promotion_variant", "baseline")
        final_rows.append(merged)
pd.concat(final_rows, ignore_index=True, sort=False).to_csv(
    OUT / "final_recommended_chronology_and_cost_stress.csv", index=False
)

calendar = sorted(daywise.day.astype(str).str[:10].unique())
baseline_details = trade_details.loc[trade_details.audit_variant.eq("baseline")].copy()
baseline_details["day"] = baseline_details.day.astype(str).str[:10]
executed = baseline_details.portfolio_executed.astype(str).str.lower().eq("true")
pnl = pd.to_numeric(baseline_details.portfolio_net_profit_rupees, errors="coerce").fillna(0.0)
baseline_details["_executed"] = executed
baseline_details["_win"] = executed & pnl.gt(1e-8)
baseline_details["_loss"] = executed & pnl.lt(-1e-8)
baseline_details["_pnl"] = pnl.where(executed, 0.0)
daywise_rows = []
for setup_id in SETUPS:
    for strategy in STRATEGIES:
        part = baseline_details.loc[
            baseline_details.audit_setup_id.eq(setup_id)
            & baseline_details.audit_strategy.eq(strategy)
        ]
        grouped = part.groupby("day")[["_executed", "_win", "_loss", "_pnl"]].sum()
        for day in calendar:
            row = grouped.loc[day] if day in grouped.index else pd.Series(
                {"_executed": 0, "_win": 0, "_loss": 0, "_pnl": 0.0}
            )
            trades = int(row._executed)
            wins = int(row._win)
            losses = int(row._loss)
            daywise_rows.append({
                "setup_id": setup_id,
                "strategy": strategy,
                "promotion_variant": "baseline",
                "day": day,
                "trades": trades,
                "wins": wins,
                "losses": losses,
                "flats": trades - wins - losses,
                "win_rate_pct": 100.0 * wins / trades if trades else None,
                "net_pnl_rs": float(row._pnl),
                "change_vs_accepted_baseline_rs": 0.0,
            })
pd.DataFrame(daywise_rows).to_csv(OUT / "final_recommended_daywise_slot.csv", index=False)

methodology = {
    "protocol_frozen_before_outcomes": True,
    "variants_per_slot_including_baseline": 12,
    "nonbaseline_variants_per_slot": 11,
    "addendum_slots": 5,
    "strategies": 2,
    "addendum_planned_nonbaseline_comparisons": 110,
    "original_plus_addendum_nonbaseline_comparisons": 220,
    "development_only_selection": True,
    "development_gate": {
        "minimum_executed_trades": 8,
        "point_win_rate_strictly_above_pct": 75.0,
        "setup_net_pnl_not_below_baseline": True,
        "losing_trade_amount_strictly_below_baseline": True,
        "full_portfolio_net_pnl_not_below_baseline": True,
        "tie_break": "highest setup net PnL, then fewer losses, then frozen variant order",
    },
    "later_periods_never_selected_or_replaced_a_rule": True,
    "unadjusted_wilson_95_is_descriptive_after_selection": True,
    "bonferroni_context_bounds_reported_for_110_and_220_comparisons": True,
    "independent_holdout": False,
    "combined_replay_performed": False,
    "combined_replay_reason": (
        "The two G3 development-selected diagnostics both failed the >75% target later and overall; "
        "therefore neither was promoted and there were no promoted rules to combine."
    ),
    "combined_claim_permitted": False,
    "sample_size_warning": (
        "All point estimates have small denominators and every relevant Wilson lower bound is below 75%; "
        "the same 59 sessions have also been repeatedly inspected."
    ),
}
(OUT / "methodology_audit.json").write_text(
    json.dumps(methodology, indent=2, sort_keys=True), encoding="utf-8"
)

summary = {
    "scope": "BACKTEST_ONLY",
    "execution_authority": False,
    "active_configs_modified": False,
    "protocol_id": "G3_G7_V4_LOCK_MULTI_MISSING_SLOTS_PREQUOTA_V1",
    "baseline_reproduction_passed": bool(parity and all(row["passed"] for row in parity)),
    "baseline_checks": parity,
    "fresh_execution_and_full_portfolio_allocation": True,
    "g7_nifty_recomputed": True,
    "g7_low_efficiency_recomputed_for_replacements": True,
    "g7_profit_protection_replayed": True,
    "g7_two_realized_loss_lock_replayed": True,
    "g7_unprovable_replacements": int(len(pd.read_csv(OUT / "unprovable_replacements.csv"))),
    "honest_qualifiers_above_75": 0,
    "final_promotion": "RETAIN_ACCEPTED_BASELINE_FOR_ALL_SIX_SLOT_STRATEGY_PAIRS",
    "results": decisions.where(pd.notna(decisions), None).to_dict("records"),
    "methodology": methodology,
}
(OUT / "summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")

report = [
    "# Missing-midday replacement-aware audit",
    "",
    "The frozen addendum was applied before rank and quota, including replacement candidates. Every baseline replay matched the accepted 09:26-adjusted books exactly. G7 used fresh Nifty permission, causal low-efficiency features for replacements, the existing 11:46 SHORT 4.0 ATR guard, +0.6/+0.1 protection, and the causal two-realized-loss lock.",
    "",
    "## Decision",
    "",
    "No tested rule honestly sustains a win rate above 75%. Keep the accepted baseline for all three slots in both strategies. No main configuration was changed.",
    "",
    "The frozen development gate required at least 8 executed trades, a point win rate strictly above 75%, setup PnL at least baseline, losing-trade amount strictly below baseline, and full-portfolio PnL at least baseline. Ties used setup PnL, then fewer losses, then frozen rule order.",
    "",
    "Two G3 rules passed the frozen development gate and remain diagnostic only:",
    "",
]
for setup_id, variant in (("1051_LONG", "move_magnitude_le_0p75"),
                          ("1146_SHORT", "extension_le_3p5")):
    row = decisions.loc[decisions.setup_id.eq(setup_id) & decisions.strategy.eq("G3")].iloc[0]
    report.append(
        f"- {setup_id} `{variant}`: development {row.development_wins}/{row.development_trades} "
        f"({row.development_win_rate_pct:.2f}%, Wilson lower {row.development_wilson_95_lower_pct:.2f}%), "
        f"later {row.later_wins}/{row.later_trades} ({row.later_win_rate_pct:.2f}%), "
        f"all {row.all_wins}/{row.all_trades} ({row.all_win_rate_pct:.2f}%)."
    )
report += [
    "",
    "For 10:51 LONG the rule improved development PnL by Rs 2,900 and reduced losing amount by Rs 6,500, but later results were unchanged at 4/6 and October was 3/5. For 11:46 SHORT the development PnL gain was Rs 7,970.60, but validation lost Rs 6,840.25 versus baseline and later fell to 5/8. The 11:11 SHORT grid produced no development qualifier in either strategy. No G7 variant passed all development gates.",
    "",
    "The closest rejected cases show why. G7 10:51 move<=0.75 reached 7/9, but lost Rs 3,598.50 versus baseline in both the slot and full portfolio and did not reduce loss amount. At 11:11, G3 had only 6 development trades and no loss reduction, while G7 had only 4 trades and exactly 75%, which is not strictly above the gate. G7 11:46 rules all stayed below 8 trades; extension<=2.5 was 4/4 but was rejected for sample size. G3 11:46 body<=0.90 also passed development, and the frozen tie-break chose extension<=3.5 because its development setup PnL was higher.",
    "",
    "The two diagnostics were not combined because both failed the >75% target outside development. Summing their isolated gains would ignore shared capital, same-symbol exclusion, and the G7 loss lock.",
    "",
    "All 12 variants, replacement decisions, 5/9/15 bps results, October/daywise rows, Wilson bounds, and failure reasons remain in the CSV artifacts. The 95% Wilson lower bounds and the 110/220-comparison context bounds are all below 75%, and this 59-session history is not an independent holdout.",
    "",
]
(OUT / "report.md").write_text("\n".join(report), encoding="utf-8")

provenance_path = OUT / "source_provenance.json"
provenance = json.loads(provenance_path.read_text(encoding="utf-8"))
provenance["postprocess_file"] = str(Path(__file__).resolve())
provenance["postprocess_sha256"] = sha(Path(__file__))
provenance["final_promotion"] = "accepted_baseline"
provenance_path.write_text(json.dumps(provenance, indent=2, sort_keys=True), encoding="utf-8")

generated = [path for path in OUT.iterdir() if path.is_file() and path.name != "artifact_manifest.json"]
manifest = {
    "scope": "BACKTEST_ONLY",
    "execution_authority": False,
    "active_configs_modified": False,
    "complete": True,
    "files": {path.name: sha(path) for path in sorted(generated)},
}
(OUT / "artifact_manifest.json").write_text(
    json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8"
)

print(decisions[["setup_id", "strategy", "development_selected_variant", "later_win_rate_pct",
                 "all_win_rate_pct", "promotion_variant", "promotion_decision"]].to_string(index=False))
