"""Exploratory post-selection integration of V10 filters and gap guards.

This bounded integration is intentionally *not* a predeclared strategy stage.
It was authorized only after isolated tests identified the 09:35 LONG maximum
five-minute move of 0.50% as the sole viable selection challenger.  The runner
composes that overlay with the already-isolated adverse-gap guards while
retaining the locked Stage-7 09:40 LONG >= 0.40% gate and the neutral full
chronological state machine.

Frozen/live files are never edited.  Outputs are research-only,
promotion-ineligible, and explicitly carry their post-selection status.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import shutil
from dataclasses import asdict, dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd

import fno_oi_common as common
import fno_v10_experiment_backtest as experiment
import fno_v10_followup_challenger_research as filters
import fno_v10_gap_guard_research as gaps
import fno_v8_windowed_1m_entry_backtest as engine


SCHEMA_VERSION = "fno_v10_stage7_postselection_0935max050_gap_combo_v1"
RESEARCH_DESIGN = "EXPLORATORY_POST_SELECTION_COMBINATION"
OUTPUT_ROOT = (
    common.FNO_ROOT
    / "strategy_research"
    / "v10_stage7_postselection_0935max050_gap_combo_v1"
)


@dataclass(frozen=True)
class Profile:
    profile_id: str
    selection_variant: str
    gap_variant: str
    required_composed_profile: bool


PROFILES: tuple[Profile, ...] = (
    Profile("STAGE7", "STAGE7_CONTROL", "CONTROL", False),
    Profile("STAGE7_GAP2", "STAGE7_CONTROL", "MAX_2_BPS", False),
    Profile("MAX050", "0935_LONG_MOVE_MAX_050", "CONTROL", True),
    Profile("MAX050_GAP0", "0935_LONG_MOVE_MAX_050", "MAX_0_BPS", True),
    Profile("MAX050_GAP2", "0935_LONG_MOVE_MAX_050", "MAX_2_BPS", True),
    Profile(
        "MAX050_REJECT_ALL",
        "0935_LONG_MOVE_MAX_050",
        "REJECT_ALL_GAP_FILLS",
        True,
    ),
)
PROFILE_BY_ID = {profile.profile_id: profile for profile in PROFILES}
GAP_BY_NAME = {spec.variant: spec for spec in gaps.GAP_GUARDS}

COMPARATORS = ("STAGE7", "MAX050", "STAGE7_GAP2")
COMPOSED_TARGETS = ("MAX050_GAP0", "MAX050_GAP2", "MAX050_REJECT_ALL")


def validate_design() -> None:
    filters.validate_registry()
    if len(PROFILE_BY_ID) != len(PROFILES):
        raise AssertionError("Integration profile IDs must be unique")
    if set(PROFILE_BY_ID) != {
        "STAGE7",
        "STAGE7_GAP2",
        "MAX050",
        "MAX050_GAP0",
        "MAX050_GAP2",
        "MAX050_REJECT_ALL",
    }:
        raise AssertionError("Bounded integration profile set changed")
    for profile in PROFILES:
        if profile.selection_variant not in filters.SPEC_BY_NAME:
            raise AssertionError(f"Unknown selection profile: {profile}")
        if profile.gap_variant not in GAP_BY_NAME:
            raise AssertionError(f"Unknown gap profile: {profile}")
    selected = filters.SPEC_BY_NAME["0935_LONG_MOVE_MAX_050"]
    if selected.move_0935_long_max != 0.50:
        raise AssertionError("Post-selection integration must remain at max 0.50%")


def _hash_json(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(
            gaps._json_ready(value),
            allow_nan=False,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()


def _safe(value: str) -> str:
    return value.lower().replace("+", "plus").replace(" ", "_")


def _selection_spec(profile: Profile) -> filters.ChallengerSpec:
    return filters.SPEC_BY_NAME[profile.selection_variant]


def _gap_spec(profile: Profile) -> gaps.GapGuardSpec:
    return GAP_BY_NAME[profile.gap_variant]


def replay_profile(
    bundle: gaps.DatasetBundle,
    profile: Profile,
    *,
    cost_bps: float,
    slippage_bps: float,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Compose selection-before-rerank with a causal gap guard and replay."""

    selected, decisions = filters.selection_overlay(
        bundle.candidates, _selection_spec(profile)
    )
    policy = gaps._entry_policy(cost_bps, slippage_bps)
    with gaps.installed_gap_guard(_gap_spec(profile)):
        audit = experiment._NEUTRAL_RUN_BACKTEST(
            selected,
            bundle.minute_paths,
            variant=profile.profile_id,
            policy=policy,
            target_exposure_per_entry_rs=50_000.0,
        )
    audit = audit.copy()
    audit["integration_profile"] = profile.profile_id
    audit["selection_variant"] = profile.selection_variant
    audit["gap_variant"] = profile.gap_variant
    audit["research_design"] = RESEARCH_DESIGN
    audit["research_cost_bps"] = float(cost_bps)
    audit["research_slippage_bps"] = float(slippage_bps)
    audit["predeclared"] = False
    audit["research_only"] = True
    audit["promotion_eligible"] = False
    return audit, decisions


def metric_for_period(
    audit: pd.DataFrame,
    days: Sequence[date],
    *,
    dataset: str,
    period: str,
    scenario: str,
    profile: Profile,
    cost_bps: float,
    slippage_bps: float,
) -> tuple[dict[str, Any], pd.DataFrame]:
    subset = audit.loc[audit["session_date"].isin(days)].copy()
    row, daily = gaps.metric_row(
        subset,
        days,
        dataset=dataset,
        period=period,
        scenario=scenario,
        spec=_gap_spec(profile),
        cost_bps=cost_bps,
        slippage_bps=slippage_bps,
    )
    row.update(
        {
            "profile_id": profile.profile_id,
            "selection_variant": profile.selection_variant,
            "gap_variant": profile.gap_variant,
            "research_design": RESEARCH_DESIGN,
            "predeclared": False,
        }
    )
    daily["profile_id"] = profile.profile_id
    daily["selection_variant"] = profile.selection_variant
    daily["gap_variant"] = profile.gap_variant
    return row, daily


PARITY_COLUMNS = gaps.PARITY_COLUMNS


def exact_audit_parity(
    observed: pd.DataFrame, reference_path: Path
) -> dict[str, Any]:
    reference_path = reference_path.resolve()
    reference = (
        pd.read_parquet(reference_path)
        if reference_path.suffix.lower() == ".parquet"
        else pd.read_csv(reference_path)
    )
    missing = sorted(
        set(PARITY_COLUMNS) - set(observed.columns)
        | (set(PARITY_COLUMNS) - set(reference.columns))
    )
    if missing:
        raise AssertionError(f"Parity fields unavailable: {missing}")
    left = observed[list(PARITY_COLUMNS)].sort_values("candidate_id").reset_index(drop=True)
    right = reference[list(PARITY_COLUMNS)].sort_values("candidate_id").reset_index(drop=True)
    if left["candidate_id"].astype(str).tolist() != right["candidate_id"].astype(str).tolist():
        raise AssertionError("Parity candidate IDs differ")
    numeric = {
        "entry_price",
        "stop_price",
        "target_price",
        "exit_price",
        "gross_return_pct",
        "net_return_pct",
        "quantity",
        "gross_pnl_rs",
        "estimated_cost_rs",
        "net_pnl_rs",
    }
    mismatches: dict[str, int] = {}
    for column in PARITY_COLUMNS:
        if column == "candidate_id":
            continue
        if column in numeric:
            left_value = pd.to_numeric(left[column], errors="coerce").to_numpy(float)
            right_value = pd.to_numeric(right[column], errors="coerce").to_numpy(float)
            same = np.isclose(
                left_value,
                right_value,
                rtol=0.0,
                atol=1e-9,
                equal_nan=True,
            )
        else:
            left_value = left[column].fillna("<NA>").astype(str).replace({"": "<NA>"})
            right_value = right[column].fillna("<NA>").astype(str).replace({"": "<NA>"})
            same = left_value.to_numpy() == right_value.to_numpy()
        count = int((~same).sum())
        if count:
            mismatches[column] = count
    if mismatches:
        raise AssertionError(f"Exact integration parity failed: {mismatches}")
    return {
        "passed": True,
        "candidate_rows": int(len(left)),
        "columns_compared": list(PARITY_COLUMNS),
        "reference_path": str(reference_path),
        "reference_sha256": gaps._sha256_file(reference_path),
    }


def _prior_filter_references() -> dict[tuple[str, str], Path]:
    latest = filters.ROOT / "latest_comparison.json"
    payload = json.loads(latest.read_text(encoding="utf-8"))
    manifest = Path(payload["comparison_manifest"])
    index = pd.read_csv(manifest.parent / "run_provenance_index.csv")
    result: dict[tuple[str, str], Path] = {}
    for dataset, label in (
        ("historical_59_sessions", "HISTORICAL"),
        ("today_2026_08_27", "TODAY"),
    ):
        selected = index.loc[
            index["dataset"].eq(dataset)
            & index["variant"].eq("0935_LONG_MOVE_MAX_050")
        ]
        if len(selected) != 1:
            raise AssertionError("Prior max-0.50 reference is not unique")
        provenance = Path(selected.iloc[0]["provenance_path"])
        result[(label, "MAX050")] = provenance.parent / "candidate_order_audit.csv"
    return result


def _prior_gap2_references() -> dict[tuple[str, str], Path]:
    latest_path = gaps.DEFAULT_OUTPUT_ROOT / "latest.json"
    latest = json.loads(latest_path.read_text(encoding="utf-8"))
    run_dir = Path(latest["run_dir"])
    return {
        (dataset, "STAGE7_GAP2"): run_dir
        / "variants"
        / dataset.lower()
        / "reference_15_0"
        / "max_2_bps"
        / "candidate_order_audit.parquet"
        for dataset in ("HISTORICAL", "TODAY")
    }


def comparison_rows(metrics: pd.DataFrame) -> pd.DataFrame:
    keys = ["dataset", "period", "scenario"]
    fields = (
        "fills",
        "wins",
        "losses",
        "win_rate_pct",
        "profit_factor",
        "net_return_points",
        "net_pnl_rs",
        "max_daily_drawdown_points",
        "remaining_gap_fills",
        "guard_rejections",
    )
    rows: list[dict[str, Any]] = []
    indexed = metrics.set_index(keys + ["profile_id"])
    for key in metrics[keys].drop_duplicates().itertuples(index=False, name=None):
        dataset, period, scenario = key
        for target in COMPOSED_TARGETS:
            target_row = indexed.loc[(*key, target)]
            for comparator in COMPARATORS:
                base = indexed.loc[(*key, comparator)]
                row: dict[str, Any] = {
                    "dataset": dataset,
                    "period": period,
                    "scenario": scenario,
                    "target_profile": target,
                    "comparator_profile": comparator,
                }
                for field in fields:
                    target_value = float(target_row[field])
                    base_value = float(base[field])
                    row[f"target_{field}"] = target_value
                    row[f"comparator_{field}"] = base_value
                    row[f"delta_{field}"] = target_value - base_value
                row["higher_net"] = row["delta_net_return_points"] > 1e-12
                row["higher_profit_factor"] = row["delta_profit_factor"] > 1e-12
                row["drawdown_not_worse"] = (
                    row["delta_max_daily_drawdown_points"] <= 1e-12
                )
                row["period_dominates"] = bool(
                    row["higher_net"]
                    and row["higher_profit_factor"]
                    and row["drawdown_not_worse"]
                )
                rows.append(row)
    return pd.DataFrame(rows)


def train_test_decisions(comparisons: pd.DataFrame) -> pd.DataFrame:
    historical = comparisons.loc[
        comparisons["dataset"].eq("HISTORICAL")
        & comparisons["period"].isin(["TRAIN", "TEST"])
    ].copy()
    rows: list[dict[str, Any]] = []
    for (scenario, target, comparator), frame in historical.groupby(
        ["scenario", "target_profile", "comparator_profile"], sort=False
    ):
        mapped = frame.set_index("period")
        if set(mapped.index) != {"TRAIN", "TEST"}:
            raise AssertionError("Decision table requires TRAIN and TEST")
        rows.append(
            {
                "scenario": scenario,
                "target_profile": target,
                "comparator_profile": comparator,
                "higher_net_both_train_test": bool(mapped["higher_net"].all()),
                "higher_pf_both_train_test": bool(
                    mapped["higher_profit_factor"].all()
                ),
                "drawdown_not_worse_both_train_test": bool(
                    mapped["drawdown_not_worse"].all()
                ),
                "dominates_both_train_test": bool(mapped["period_dominates"].all()),
                "train_delta_net_points": float(
                    mapped.loc["TRAIN", "delta_net_return_points"]
                ),
                "test_delta_net_points": float(
                    mapped.loc["TEST", "delta_net_return_points"]
                ),
                "train_delta_pf": float(mapped.loc["TRAIN", "delta_profit_factor"]),
                "test_delta_pf": float(mapped.loc["TEST", "delta_profit_factor"]),
                "train_delta_drawdown": float(
                    mapped.loc["TRAIN", "delta_max_daily_drawdown_points"]
                ),
                "test_delta_drawdown": float(
                    mapped.loc["TEST", "delta_max_daily_drawdown_points"]
                ),
            }
        )
    return pd.DataFrame(rows)


def _markdown_table(frame: pd.DataFrame, columns: Sequence[str]) -> list[str]:
    lines = [
        "| " + " | ".join(columns) + " |",
        "| " + " | ".join("---" for _ in columns) + " |",
    ]
    for row in frame[list(columns)].to_dict("records"):
        values = []
        for column in columns:
            value = row[column]
            if isinstance(value, (float, np.floating)):
                values.append("n/a" if not math.isfinite(float(value)) else f"{float(value):.4f}")
            else:
                values.append(str(value))
        lines.append("| " + " | ".join(values) + " |")
    return lines


def build_report(
    output_dir: Path,
    metrics: pd.DataFrame,
    decisions: pd.DataFrame,
    parity: Mapping[str, Any],
) -> str:
    reference = metrics.loc[
        metrics["scenario"].eq("REFERENCE_15_0")
        & (
            (metrics["dataset"].eq("HISTORICAL") & metrics["period"].eq("FULL"))
            | (metrics["dataset"].eq("TODAY") & metrics["period"].eq("TODAY"))
        )
    ].copy()
    lines = [
        "# V10 Stage-7 post-selection max-0.50 + gap integration",
        "",
        "Exploratory post-selection combination; not predeclared and not promotion-eligible.",
        "Frozen/live files were not modified.",
        "",
        "## Reference economics",
        "",
    ]
    lines.extend(
        _markdown_table(
            reference,
            [
                "dataset",
                "profile_id",
                "fills",
                "wins",
                "losses",
                "win_rate_pct",
                "profit_factor",
                "net_return_points",
                "net_pnl_rs",
                "max_daily_drawdown_points",
            ],
        )
    )
    lines.extend(["", "## Train/test dominance checks", ""])
    lines.extend(
        _markdown_table(
            decisions.loc[decisions["scenario"].eq("REFERENCE_15_0")],
            [
                "target_profile",
                "comparator_profile",
                "higher_net_both_train_test",
                "higher_pf_both_train_test",
                "drawdown_not_worse_both_train_test",
                "dominates_both_train_test",
                "train_delta_net_points",
                "test_delta_net_points",
            ],
        )
    )
    lines.extend(
        [
            "",
            "## Integrity",
            "",
            f"- Exact parity checks passed: `{all(item['passed'] for item in parity.values())}`.",
            "- Selection filter runs before rerank and state-machine replay.",
            "- Gap rejection is evaluated causally at the completed one-minute bar open.",
            "- Every cost scenario is replayed; economics are not post-hoc repriced.",
            "- `predeclared=false`; `research_only=true`; `promotion_eligible=false`.",
            "",
            f"Package: `{output_dir}`",
            "",
        ]
    )
    return "\n".join(lines)


def run(args: argparse.Namespace) -> Path:
    validate_design()
    historical = gaps.load_dataset(
        "HISTORICAL",
        args.historical_cache_manifest,
        args.historical_reference_audit,
    )
    today = gaps.load_dataset(
        "TODAY", args.today_cache_manifest, args.today_reference_audit
    )
    if len(historical.sessions) != 59 or historical.sessions[0] != gaps.HISTORICAL_FROM_DAY or historical.sessions[-1] != gaps.HISTORICAL_THROUGH_DAY:
        raise AssertionError("Historical 59-session contract changed")
    if today.sessions != (gaps.TODAY_DAY,):
        raise AssertionError("Immutable-today contract changed")

    experiment.configure_engine("0940_LONG_MOVE_040")
    stamp = datetime.now(gaps.IST).strftime("%Y%m%dT%H%M%S%f%z")
    output_dir = args.output_root.resolve() / "runs" / f"combo_{stamp}"
    output_dir.mkdir(parents=True, exist_ok=False)
    source_dir = output_dir / "source"
    source_dir.mkdir()
    sources = [
        Path(__file__).resolve(),
        Path(filters.__file__).resolve(),
        Path(gaps.__file__).resolve(),
        Path(experiment.__file__).resolve(),
        Path(engine.__file__).resolve(),
    ]
    for source in sources:
        shutil.copy2(source, source_dir / source.name)

    prior_filter = _prior_filter_references()
    prior_gap2 = _prior_gap2_references()
    summary_rows: list[dict[str, Any]] = []
    daily_parts: list[pd.DataFrame] = []
    parity: dict[str, Any] = {}
    reference_audits: dict[tuple[str, str], pd.DataFrame] = {}
    written_selection: set[tuple[str, str]] = set()

    for bundle in (historical, today):
        for scenario, cost_bps, slippage_bps in gaps.COST_SCENARIOS:
            for profile in PROFILES:
                print(
                    f"[V10-COMBO] dataset={bundle.name} scenario={scenario} "
                    f"profile={profile.profile_id}",
                    flush=True,
                )
                audit, selection = replay_profile(
                    bundle,
                    profile,
                    cost_bps=cost_bps,
                    slippage_bps=slippage_bps,
                )
                full_period = "FULL" if bundle.name == "HISTORICAL" else "TODAY"
                full_row, full_daily = metric_for_period(
                    audit,
                    bundle.sessions,
                    dataset=bundle.name,
                    period=full_period,
                    scenario=scenario,
                    profile=profile,
                    cost_bps=cost_bps,
                    slippage_bps=slippage_bps,
                )
                summary_rows.append(full_row)
                daily_parts.append(full_daily)
                if bundle.name == "HISTORICAL":
                    for period, days in (
                        ("TRAIN", tuple(day for day in bundle.sessions if day < gaps.SPLIT_DAY)),
                        ("TEST", tuple(day for day in bundle.sessions if day >= gaps.SPLIT_DAY)),
                    ):
                        row, daily = metric_for_period(
                            audit,
                            days,
                            dataset=bundle.name,
                            period=period,
                            scenario=scenario,
                            profile=profile,
                            cost_bps=cost_bps,
                            slippage_bps=slippage_bps,
                        )
                        summary_rows.append(row)
                        daily_parts.append(daily)

                run_dir = (
                    output_dir
                    / "variants"
                    / bundle.name.lower()
                    / _safe(scenario)
                    / _safe(profile.profile_id)
                )
                gaps._write_run_artifacts(run_dir, audit, full_daily, full_row)
                selection_key = (bundle.name, profile.selection_variant)
                if selection_key not in written_selection:
                    selection_dir = output_dir / "selection_decisions" / bundle.name.lower()
                    selection_dir.mkdir(parents=True, exist_ok=True)
                    selection.to_csv(
                        selection_dir / f"{_safe(profile.selection_variant)}.csv",
                        index=False,
                    )
                    written_selection.add(selection_key)

                if scenario == "REFERENCE_15_0":
                    reference_audits[(bundle.name, profile.profile_id)] = audit
                    if profile.profile_id == "STAGE7":
                        parity[f"{bundle.name}_STAGE7_FROZEN"] = exact_audit_parity(
                            audit, bundle.reference_audit_path
                        )
                    elif profile.profile_id == "MAX050":
                        parity[f"{bundle.name}_MAX050_PRIOR"] = exact_audit_parity(
                            audit, prior_filter[(bundle.name, "MAX050")]
                        )
                    elif profile.profile_id == "STAGE7_GAP2":
                        parity[f"{bundle.name}_GAP2_PRIOR"] = exact_audit_parity(
                            audit, prior_gap2[(bundle.name, "STAGE7_GAP2")]
                        )

    metrics = pd.DataFrame(summary_rows)
    daily = pd.concat(daily_parts, ignore_index=True)
    comparisons = comparison_rows(metrics)
    decisions = train_test_decisions(comparisons)
    metrics.to_csv(output_dir / "all_period_metrics.csv", index=False)
    daily.to_csv(output_dir / "daywise.csv", index=False)
    comparisons.to_csv(output_dir / "comparisons_vs_required_bases.csv", index=False)
    decisions.to_csv(output_dir / "train_test_decisions.csv", index=False)
    pd.concat(
        [
            audit.loc[gaps._closed_mask(audit)].assign(profile_id=profile)
            for (dataset, profile), audit in reference_audits.items()
            if dataset == "TODAY"
        ],
        ignore_index=True,
    ).to_csv(output_dir / "today_trades_reference_cost.csv", index=False)
    gaps._write_json(output_dir / "parity.json", parity)
    (output_dir / "report.md").write_text(
        build_report(output_dir, metrics, decisions, parity), encoding="utf-8"
    )

    provenance_path = output_dir / "provenance.json"
    provenance = {
        "schema_version": SCHEMA_VERSION,
        "created_at_ist": datetime.now(gaps.IST),
        "research_design": RESEARCH_DESIGN,
        "design_timing": "AUTHORIZED_AFTER_ISOLATED_RESULTS_WERE_OBSERVED",
        "predeclared": False,
        "profiles": [asdict(profile) for profile in PROFILES],
        "profile_registry_sha256": _hash_json([asdict(profile) for profile in PROFILES]),
        "selection_contract": {
            "base": "LOCKED_STAGE7_0940_LONG_MOVE_MIN_040",
            "composed_overlay": "0935_LONG_MOVE_MAX_050",
            "application_order": "SELECTION_FILTER_THEN_RERANK_THEN_FULL_STATE_REPLAY",
        },
        "gap_contract": {
            "source_module": str(Path(gaps.__file__).resolve()),
            "source_sha256": gaps._sha256_file(Path(gaps.__file__).resolve()),
            "evaluation": "COMPLETED_1M_BAR_OPEN_THROUGH_PENDING_STOP",
            "capacity_release": "AFTER_REJECTION_BAR;NEXT_MINUTE_EARLIEST_BACKFILL",
        },
        "cost_scenarios": [
            {"scenario": name, "cost_bps": cost, "slippage_bps": slippage}
            for name, cost, slippage in gaps.COST_SCENARIOS
        ],
        "split_day": gaps.SPLIT_DAY,
        "parity": parity,
        "source_archives": [
            {
                "path": str((source_dir / source.name).resolve()),
                "sha256": gaps._sha256_file(source_dir / source.name),
            }
            for source in sources
        ],
        "source_inputs": {
            bundle.name: {
                "cache_manifest": str(bundle.manifest_path),
                "cache_manifest_sha256": gaps._sha256_file(bundle.manifest_path),
                "cache_input_fingerprint": bundle.manifest.get("input_fingerprint"),
                "reference_audit": str(bundle.reference_audit_path),
                "reference_audit_sha256": gaps._sha256_file(bundle.reference_audit_path),
                "sessions": [day.isoformat() for day in bundle.sessions],
            }
            for bundle in (historical, today)
        },
        "limitations": [
            "POST_SELECTION_COMBINATION_MULTIPLE_TESTING",
            "NOT_PREDECLARED",
            "STATIC_LATER_DATED_UNIVERSE",
            "STATIC_AUGUST_FUTURES_OI_NOT_ROLLING_POINT_IN_TIME",
            "UPSTREAM_SOURCE_SLOT_COVERAGE_INCOMPLETE",
            "LAST_REAL_BAR_SENSITIVITY_NOT_HEADLINE",
            "REQUIRES_UNTOUCHED_PROSPECTIVE_SHADOW_VALIDATION",
        ],
        "research_only": True,
        "promotion_eligible": False,
        "live_or_paper_authority": False,
    }
    gaps._write_json(provenance_path, provenance)
    inventory_path = output_dir / "artifact_inventory.json"
    gaps._write_json(
        inventory_path,
        {
            "schema_version": SCHEMA_VERSION,
            "artifacts": gaps._inventory_files(
                output_dir, exclude={inventory_path, provenance_path}
            ),
        },
    )
    provenance["artifact_inventory"] = {
        "path": str(inventory_path.resolve()),
        "sha256": gaps._sha256_file(inventory_path),
    }
    gaps._write_json(provenance_path, provenance)
    latest = args.output_root.resolve() / "latest.json"
    gaps._write_json(
        latest,
        {
            "schema_version": SCHEMA_VERSION,
            "run_dir": str(output_dir),
            "provenance_sha256": gaps._sha256_file(provenance_path),
            "research_design": RESEARCH_DESIGN,
            "predeclared": False,
            "research_only": True,
            "promotion_eligible": False,
        },
    )
    print(f"[V10-COMBO] complete: {output_dir}", flush=True)
    return output_dir


def parser() -> argparse.ArgumentParser:
    value = argparse.ArgumentParser(description=__doc__)
    value.add_argument(
        "--historical-cache-manifest",
        type=Path,
        default=gaps.DEFAULT_HISTORICAL_CACHE_MANIFEST,
    )
    value.add_argument(
        "--historical-reference-audit",
        type=Path,
        default=gaps.DEFAULT_HISTORICAL_REFERENCE_AUDIT,
    )
    value.add_argument(
        "--today-cache-manifest",
        type=Path,
        default=gaps.DEFAULT_TODAY_CACHE_MANIFEST,
    )
    value.add_argument(
        "--today-reference-audit",
        type=Path,
        default=gaps.DEFAULT_TODAY_REFERENCE_AUDIT,
    )
    value.add_argument("--output-root", type=Path, default=OUTPUT_ROOT)
    return value


def main(argv: Sequence[str] | None = None) -> int:
    run(parser().parse_args(argv))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
