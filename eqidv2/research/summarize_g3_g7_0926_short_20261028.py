from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pandas as pd
from scipy.stats import beta, binom


ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "outputs" / "g3_g7_0926_short_iterative_20261028"
PRE = OUT / "prequota_reselection"
STRATEGIES = ("G3", "G7_V4_LOCK2")
VARIANTS = ("baseline", "wick_ge_0p20")


def dump_json(path: Path, value: object) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def exact_binomial(wins: int, trades: int, null_rate: float = 0.75) -> dict[str, float]:
    lower = 0.0 if wins == 0 else float(beta.ppf(0.025, wins, trades - wins + 1))
    upper = 1.0 if wins == trades else float(beta.ppf(0.975, wins + 1, trades - wins))
    return {
        "clopper_pearson_95_lower_pct": 100.0 * lower,
        "clopper_pearson_95_upper_pct": 100.0 * upper,
        "one_sided_p_value_vs_75_pct": float(binom.sf(wins - 1, trades, null_rate)),
    }


def metric_record(frame: pd.DataFrame, strategy: str, variant: str, scope: str) -> dict:
    row = frame.loc[
        frame.strategy.eq(strategy)
        & frame.variant.eq(variant)
        & frame.scope.eq(scope)
        & frame.period.eq("all")
        & frame.cost_bps_round_trip.eq(5.0)
    ].iloc[0]
    fields = (
        "trades",
        "wins",
        "losses",
        "flats",
        "win_rate_pct",
        "net_pnl_rs",
        "net_delta_vs_baseline_same_cost_rs",
    )
    return {field: float(row[field]) for field in fields}


def make_daywise(details: pd.DataFrame, calendar: list[str]) -> pd.DataFrame:
    rows: list[dict] = []
    details = details.loc[
        details.audit_strategy.isin(STRATEGIES) & details.audit_variant.isin(VARIANTS)
    ].copy()
    details["day"] = details.day.astype(str).str[:10]
    for strategy in STRATEGIES:
        for day in calendar:
            row: dict[str, object] = {
                "strategy": strategy,
                "day": day,
                "development_period": day <= "2026-09-18",
                "post_development_period": day >= "2026-09-21",
                "october": day.startswith("2026-10"),
            }
            for variant in VARIANTS:
                subset = details.loc[
                    details.audit_strategy.eq(strategy)
                    & details.audit_variant.eq(variant)
                    & details.day.eq(day)
                ]
                executed = subset.portfolio_executed.fillna(False).astype(bool)
                pnl = pd.to_numeric(subset.portfolio_net_profit_rupees, errors="coerce").fillna(0.0)
                prefix = "baseline" if variant == "baseline" else "wick_ge_0p20"
                row[f"{prefix}_selected_opportunities"] = int(len(subset))
                row[f"{prefix}_executed_trades"] = int(executed.sum())
                row[f"{prefix}_wins"] = int((executed & pnl.gt(0)).sum())
                row[f"{prefix}_losses"] = int((executed & pnl.lt(0)).sum())
                row[f"{prefix}_net_pnl_rs"] = float(pnl.loc[executed].sum())
                row[f"{prefix}_selected_symbols"] = "|".join(subset.tradingsymbol.astype(str))
                row[f"{prefix}_executed_symbols"] = "|".join(
                    subset.loc[executed, "tradingsymbol"].astype(str)
                )
            row["candidate_net_delta_rs"] = (
                float(row["wick_ge_0p20_net_pnl_rs"]) - float(row["baseline_net_pnl_rs"])
            )
            rows.append(row)
    return pd.DataFrame(rows)


def main() -> None:
    details_path = PRE / "variant_0926_trade_details.csv"
    setup_path = PRE / "variant_setup_metrics.csv"
    portfolio_path = PRE / "variant_portfolio_metrics.csv"
    cost_path = PRE / "cost_stress_metrics.csv"
    provenance_path = PRE / "cost_stress_provenance.json"
    parity_path = PRE / "baseline_reproduction.json"

    details = pd.read_csv(details_path, low_memory=False)
    setup = pd.read_csv(setup_path)
    portfolio = pd.read_csv(portfolio_path)
    costs = pd.read_csv(cost_path)
    cost_provenance = json.loads(provenance_path.read_text(encoding="utf-8"))
    parity = json.loads(parity_path.read_text(encoding="utf-8"))

    daywise = make_daywise(details, cost_provenance["calendars"]["all"])
    daywise_path = PRE / "candidate_daywise_comparison.csv"
    daywise.to_csv(daywise_path, index=False)

    setup_all = setup.loc[setup.period.eq("all")].copy()
    candidate_results: dict[str, dict] = {}
    for strategy in STRATEGIES:
        baseline = setup_all.loc[
            setup_all.strategy.eq(strategy) & setup_all.variant.eq("baseline")
        ].iloc[0]
        candidate = setup_all.loc[
            setup_all.strategy.eq(strategy) & setup_all.variant.eq("wick_ge_0p20")
        ].iloc[0]
        wins, trades = int(candidate.wins), int(candidate.trades)
        post = setup.loc[
            setup.strategy.eq(strategy)
            & setup.variant.eq("wick_ge_0p20")
            & setup.period.eq("post_development")
        ].iloc[0]
        strategy_details = details.loc[details.audit_strategy.eq(strategy)].copy()

        def execution_sets(variant: str) -> tuple[set[tuple[str, str]], set[tuple[str, str]]]:
            variant_rows = strategy_details.loc[
                strategy_details.audit_variant.eq(variant)
                & strategy_details.portfolio_executed.fillna(False).astype(bool)
            ].copy()
            keys = set(zip(
                variant_rows.day.astype(str).str[:10],
                variant_rows.tradingsymbol.astype(str),
            ))
            winner_rows = variant_rows.loc[
                pd.to_numeric(variant_rows.portfolio_net_profit_rupees, errors="coerce").gt(0)
            ]
            winner_keys = set(zip(
                winner_rows.day.astype(str).str[:10],
                winner_rows.tradingsymbol.astype(str),
            ))
            return keys, winner_keys

        baseline_execution_keys, baseline_winner_keys = execution_sets("baseline")
        candidate_execution_keys, candidate_winner_keys = execution_sets("wick_ge_0p20")
        candidate_results[strategy] = {
            "baseline_5bps": {
                "selected_opportunities": int(baseline.selected_opportunities),
                "executed_trades": int(baseline.trades),
                "wins": int(baseline.wins),
                "losses": int(baseline.losses),
                "win_rate_pct": float(baseline.win_rate_pct),
                "net_pnl_rs": float(baseline.net_pnl_rs),
            },
            "candidate_5bps": {
                "selected_opportunities": int(candidate.selected_opportunities),
                "executed_trades": trades,
                "wins": wins,
                "losses": int(candidate.losses),
                "win_rate_pct": float(candidate.win_rate_pct),
                "net_pnl_rs": float(candidate.net_pnl_rs),
                "executed_trade_count_ratio_vs_baseline_pct": float(
                    candidate.executed_trade_count_ratio_vs_baseline_pct
                ),
                "winner_count_ratio_vs_baseline_pct": float(
                    candidate.winner_count_ratio_vs_baseline_pct
                ),
                "same_baseline_execution_identities": len(
                    baseline_execution_keys & candidate_execution_keys
                ),
                "same_baseline_execution_identity_pct": (
                    100.0 * len(baseline_execution_keys & candidate_execution_keys)
                    / len(baseline_execution_keys)
                ),
                "same_baseline_winner_identities": len(
                    baseline_winner_keys & candidate_winner_keys
                ),
                "same_baseline_winner_identity_pct": (
                    100.0 * len(baseline_winner_keys & candidate_winner_keys)
                    / len(baseline_winner_keys)
                ),
                "replacement_winner_identities": [
                    {"day": day, "tradingsymbol": symbol}
                    for day, symbol in sorted(candidate_winner_keys - baseline_execution_keys)
                ],
                **exact_binomial(wins, trades),
            },
            "post_development_candidate_5bps": {
                "selected_opportunities": int(post.selected_opportunities),
                "executed_trades": int(post.trades),
                "wins": int(post.wins),
                "losses": int(post.losses),
                "net_pnl_rs": float(post.net_pnl_rs),
            },
            "full_portfolio_baseline_5bps": metric_record(costs, strategy, "baseline", "full_portfolio"),
            "full_portfolio_candidate_5bps": metric_record(
                costs, strategy, "wick_ge_0p20", "full_portfolio"
            ),
        }

    stress: dict[str, dict[str, dict]] = {}
    for strategy in STRATEGIES:
        stress[strategy] = {}
        for cost_bps in (5.0, 9.0, 15.0):
            record: dict[str, dict] = {}
            for scope in ("0926_SHORT", "full_portfolio"):
                row = costs.loc[
                    costs.strategy.eq(strategy)
                    & costs.variant.eq("wick_ge_0p20")
                    & costs.cost_bps_round_trip.eq(cost_bps)
                    & costs.scope.eq(scope)
                    & costs.period.eq("all")
                ].iloc[0]
                record[scope] = {
                    "trades": int(row.trades),
                    "wins": int(row.wins),
                    "losses": int(row.losses),
                    "win_rate_pct": float(row.win_rate_pct),
                    "net_pnl_rs": float(row.net_pnl_rs),
                    "net_delta_vs_baseline_same_cost_rs": float(
                        row.net_delta_vs_baseline_same_cost_rs
                    ),
                }
            stress[strategy][f"{cost_bps:g}_bps"] = record

    decision = {
        "study": "09:26 SHORT G-3 and G7 V4 two-loss-lock replacement-aware audit",
        "as_of_session": "2026-10-27",
        "status": "RESEARCH_SPECIFICATION_NOT_IMPLEMENTED_OR_ADOPTED",
        "scope": "BACKTEST_ONLY",
        "execution_authority": False,
        "active_configs_modified": False,
        "numerical_target_above_75_pct_at_5bps": True,
        "reliable_true_win_rate_above_75_pct_established": False,
        "candidate_rule": {
            "setup_id": "0926_SHORT",
            "side": "SHORT",
            "parameter": "minimum_directional_wick_ratio",
            "value": 0.20,
            "feature": "confirmation lower wick / confirmation candle range",
            "timing": "Apply after existing minimum gates and before max-volume ranking and two-entry quota",
            "intent": (
                "Require the confirmation candle to close at least 20% of its range above its low; "
                "the later break of that low must then reconfirm downside rather than chase a close at the extreme."
            ),
        },
        "threshold_origin": (
            "The 0.20 threshold had already appeared in selected-ledger exploratory diagnostics. "
            "The replacement-aware grid was frozen before its own outcomes, but the threshold is not an untouched hypothesis."
        ),
        "candidate_pool": {
            "base_gate_candidates": 213,
            "candidate_days": 25,
            "calendar_sessions": 59,
            "variant_applied_before_ranking_and_quota": True,
            "replacement_candidates_allowed": True,
        },
        "baseline_reproduction": parity,
        "results": candidate_results,
        "cost_stress": stress,
        "rejection_reasons_for_active_adoption": [
            "The candidate leaves 11 executions in each strategy, 61.1% as many as baseline; replacements mean this is not identity retention.",
            "The exact 95% lower confidence bounds are below 75% for both strategies.",
            "After the development period, G-3 retains one execution and it loses; G7 retains one and it wins.",
            "At 15 bps G7 falls to 7 wins from 11 executions because small protected exits become net losses and alter the two-loss lock chronology.",
            "The same history has been inspected repeatedly, and the 0.20 threshold was motivated by a prior post-hoc diagnostic.",
        ],
        "next_gate": (
            "First implement this exact research specification in a shadow-only path and prove parity. "
            "Then freeze its rule and source hashes and collect untouched forward sessions. "
            "Do not promote it until both books retain adequate trade count, remain profitable under stress, "
            "and show a lower confidence bound compatible with the required rate."
        ),
        "artifacts": {
            "candidate_daywise_comparison.csv": sha256(daywise_path),
            "variant_setup_metrics.csv": sha256(setup_path),
            "variant_portfolio_metrics.csv": sha256(portfolio_path),
            "cost_stress_metrics.csv": sha256(cost_path),
            "baseline_reproduction.json": sha256(parity_path),
        },
    }
    decision_path = PRE / "candidate_decision.json"
    dump_json(decision_path, decision)

    research_config = {
        "config_name": "0926_SHORT_MIN_DIRECTIONAL_WICK_0P20_FORWARD_RESEARCH",
        "status": "RESEARCH_SPECIFICATION_NOT_IMPLEMENTED_OR_ADOPTED",
        "scope": "BACKTEST_ONLY",
        "execution_authority": False,
        "active_configs_modified": False,
        "inherits_current_strategy_settings": {
            "G3": "current pinned G-3 full-history configuration",
            "G7_V4_LOCK2": "current pinned G7 V4 profit protection and two-loss-lock configuration",
        },
        "research_overlay": {
            "setup_id": "0926_SHORT",
            "side": "SHORT",
            "minimum_directional_wick_ratio": 0.20,
            "apply_before_ranking_and_quota": True,
            "ranking": "MAX_VOLUME",
            "max_entries": 2,
        },
        "implementation_note": (
            "The live configuration schema currently exposes a maximum wick gate. "
            "A separate minimum directional wick gate must be implemented and validated before any future adoption."
        ),
        "decision_artifact": str(decision_path.relative_to(ROOT)),
    }
    research_config_path = PRE / "research_candidate_config.json"
    dump_json(research_config_path, research_config)

    prequota_files = sorted(
        path for path in PRE.iterdir()
        if path.is_file() and path.name != "artifact_manifest.json"
    )
    prequota_manifest_path = PRE / "artifact_manifest.json"
    dump_json(prequota_manifest_path, {
        "scope": "BACKTEST_ONLY",
        "execution_authority": False,
        "active_configs_modified": False,
        "files": {path.name: sha256(path) for path in prequota_files},
    })

    top_manifest_path = OUT / "artifact_manifest.json"
    output_files = sorted(
        path for path in OUT.rglob("*")
        if path.is_file()
        and path != top_manifest_path
        and "__pycache__" not in path.parts
    )
    code_files = [
        ROOT / "research" / "g3_g7_0926_short_experiments_20261028.py",
        Path(__file__).resolve(),
    ]
    dump_json(top_manifest_path, {
        "scope": "BACKTEST_ONLY",
        "execution_authority": False,
        "active_configs_modified": False,
        "files": {
            str(path.relative_to(OUT)).replace("\\", "/"): sha256(path)
            for path in output_files
        },
        "code": {
            str(path.relative_to(ROOT)).replace("\\", "/"): sha256(path)
            for path in code_files
        },
    })

    print(daywise_path)
    print(decision_path)


if __name__ == "__main__":
    main()
