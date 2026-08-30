"""Research-only rerun of V10 challengers on immutable repaired snapshots.

This launcher composes existing, tested research seams.  It does not edit the
frozen V8/V10 launchers, the locked Stage-7 profile, or any live strategy file.
It rebuilds source-bound caches, creates an independent fresh Stage-7 anchor,
runs the isolated challenger family, runs the exploratory max-0.50 + gap
family at all declared costs, and measures drift from the original packages.

The historical contract is the static 26AUG universe for 2026-05-27 through
2026-08-19 (59 exchange sessions).  The repaired-today contract is the dated
2026-08-27 26SEP universe.  A previously supplied 26AUG today snapshot is
recorded as rejected and is never consumed.
"""

from __future__ import annotations

import argparse
import json
import math
import shutil
from dataclasses import asdict
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

import numpy as np
import pandas as pd

import fno_oi_common as common
import fno_v10_experiment_backtest as experiment
import fno_v10_experiment_config as experiment_config
import fno_v10_followup_challenger_research as filters
import fno_v10_gap_guard_research as gaps
import fno_v10_stage7_postselection_combo_research as combo
import fno_v8_windowed_1m_entry_backtest as engine


SCHEMA_VERSION = "fno_v10_repaired_snapshot_rerun_v1"
OUTPUT_ROOT = (
    common.FNO_ROOT
    / "strategy_research"
    / "v10_repaired_snapshot_reruns_20260827_v1"
)
HISTORICAL_SNAPSHOT = Path(
    r"C:\TradingData\eqidv2\fno_oi\strategy_research"
    r"\fno_historical_repair_20260827\historical_snapshots"
    r"\snapshot_20260827T223607687461+0530_41hgqjpi\manifest.json"
)
TODAY_SEP_SNAPSHOT = Path(
    r"C:\TradingData\eqidv2\fno_oi\strategy_research"
    r"\fno_historical_repair_20260827\today_sep_snapshots"
    r"\snapshot_20260827T223955196682+0530_pcsrdrjp\manifest.json"
)
REJECTED_TODAY_AUG_SNAPSHOT = Path(
    r"C:\TradingData\eqidv2\fno_oi\strategy_research"
    r"\fno_historical_repair_20260827\today_snapshots"
    r"\snapshot_20260827T223632439512+0530_k5u8680g\manifest.json"
)
OLD_FILTER_COMPARISON = Path(
    r"C:\TradingData\eqidv2\fno_oi\strategy_research"
    r"\v10_stage7_followup_challengers_v1\comparisons"
    r"\comparison_20260827T221323661673+0530"
)
OLD_COMBO_RUN = Path(
    r"C:\TradingData\eqidv2\fno_oi\strategy_research"
    r"\v10_stage7_postselection_0935max050_gap_combo_v1\runs"
    r"\combo_20260827T222606225473+0530"
)
HIST_FROM = date(2026, 5, 27)
HIST_THROUGH = date(2026, 8, 19)
SPLIT_DAY = date(2026, 8, 6)
TODAY_DAY = date(2026, 8, 27)


def _snapshot_payload(path: Path) -> dict[str, Any]:
    payload = json.loads(path.resolve().read_text(encoding="utf-8"))
    if payload.get("schema_version") != "fno_backtest_source_snapshot_v1":
        raise AssertionError(f"unexpected snapshot schema: {path}")
    if not bool(payload.get("complete")):
        raise AssertionError(f"snapshot is incomplete: {path}")
    return payload


def observed_snapshot_contract(path: Path) -> dict[str, Any]:
    payload = _snapshot_payload(path)
    universe = dict(payload.get("universe", {}))
    captures = list(payload.get("captures", []))
    roles: dict[str, int] = {}
    for item in captures:
        role = str(item.get("role", ""))
        roles[role] = roles.get(role, 0) + 1
    return {
        "manifest_path": str(path.resolve()),
        "manifest_sha256": gaps._sha256_file(path.resolve()),
        "snapshot_fingerprint": str(payload.get("snapshot_fingerprint", "")),
        "complete": bool(payload.get("complete")),
        "physical_copy": bool(payload.get("physical_copy")),
        "capture_count": len(captures),
        "capture_roles": roles,
        "universe": universe,
    }


def validate_source_contracts(
    historical: Path, today: Path, rejected: Path
) -> dict[str, Any]:
    hist = observed_snapshot_contract(historical)
    current = observed_snapshot_contract(today)
    wrong = observed_snapshot_contract(rejected)
    hu = hist["universe"]
    tu = current["universe"]
    wu = wrong["universe"]
    if hu.get("contract_month_filter") != "26AUG" or int(
        hu.get("mapped_stock_futures", -1)
    ) != 208:
        raise AssertionError("historical repaired snapshot is not the declared 208/26AUG contract")
    if tu.get("contract_month_filter") != "26SEP" or int(
        tu.get("mapped_stock_futures", -1)
    ) != 210:
        raise AssertionError("today repaired snapshot is not the declared 210/26SEP contract")
    if tu.get("master_date") != "2026-08-27" or current["capture_count"] != 420:
        raise AssertionError("today repaired snapshot date/capture cardinality changed")
    if wu.get("contract_month_filter") != "26AUG":
        raise AssertionError("rejected snapshot no longer demonstrates the AUG mismatch")
    return {
        "historical_accepted": hist,
        "today_sep_accepted": current,
        "today_aug_rejected": {
            **wrong,
            "accepted": False,
            "rejection_code": "WRONG_CONTRACT_MONTH_FOR_REPAIRED_TODAY",
            "expected_contract_month_filter": "26SEP",
            "observed_contract_month_filter": str(wu.get("contract_month_filter", "")),
        },
    }


def bind_engine_universe(snapshot_manifest: Path) -> dict[str, Any]:
    """Process-local binding needed for the dated SEP source validator."""

    payload = _snapshot_payload(snapshot_manifest)
    universe = dict(payload["universe"])
    engine.BACKTEST_UNIVERSE_DATE = date.fromisoformat(str(universe["master_date"]))
    engine.BACKTEST_UNIVERSE_PATH = Path(str(universe["path"]))
    engine.BACKTEST_CONTRACT_MONTH_FILTER = str(universe["contract_month_filter"])
    keys = (
        "file_sha256",
        "universe_sha256",
        "mapped_universe_sha256",
        "mapped_symbol_set_sha256",
    )
    engine.BACKTEST_UNIVERSE_HASHES = {key: str(universe[key]) for key in keys}
    return universe


def build_cache(
    *, snapshot: Path, cache_dir: Path, from_day: date, through_day: date, rebuild: bool
) -> Path:
    experiment.configure_engine("0940_LONG_MOVE_040")
    bind_engine_universe(snapshot)
    engine.CACHE_DIR = cache_dir.resolve()
    engine.CACHE_MANIFEST_PATH = engine.CACHE_DIR / "manifest.json"
    engine.CANDIDATE_CACHE_PATH = engine.CACHE_DIR / "five_minute_candidates.parquet"
    engine.PATH_CACHE_PATH = engine.CACHE_DIR / "same_session_minute_paths.parquet"
    _, _, _, _, manifest_path = experiment._NEUTRAL_LOAD_OR_BUILD_CACHE(
        source_snapshot_path=snapshot,
        from_day=from_day,
        through_day=through_day,
        rebuild=rebuild,
    )
    return manifest_path.resolve()


def _load_cache(manifest_path: Path) -> tuple[dict[str, Any], pd.DataFrame, pd.DataFrame, tuple[date, ...]]:
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    if not bool(payload.get("complete")):
        raise AssertionError(f"cache is incomplete: {manifest_path}")
    candidate_path = Path(str(payload["artifacts"]["candidates"]["path"])).resolve()
    path_path = Path(str(payload["artifacts"]["paths"]["path"])).resolve()
    for name, path in (("candidates", candidate_path), ("paths", path_path)):
        expected = str(payload["artifacts"][name]["sha256"])
        if gaps._sha256_file(path) != expected:
            raise AssertionError(f"cache artifact hash changed: {path}")
    candidates = pd.read_parquet(candidate_path)
    minute_paths = pd.read_parquet(path_path)
    sessions = tuple(engine._parse_day(item) for item in payload["session_dates"])
    return payload, candidates, minute_paths, sessions


def _reference_policy(cost_bps: float = 15.0, slippage_bps: float = 0.0) -> engine.EntryPolicy:
    return experiment._entry_policy_for_variant(
        "0940_LONG_MOVE_040",
        cost_bps=cost_bps,
        slippage_bps=slippage_bps,
        square_off="15:30",
        eod_policy="LAST_REAL_BAR_SENSITIVITY",
    )


def create_fresh_stage7_reference(
    *, label: str, manifest_path: Path, output_dir: Path
) -> tuple[Path, pd.DataFrame, pd.DataFrame, tuple[date, ...]]:
    _, candidates, minute_paths, sessions = _load_cache(manifest_path)
    experiment.configure_engine("0940_LONG_MOVE_040")
    engine._confirmation_check = experiment._NEUTRAL_CONFIRMATION_CHECK
    spec = experiment_config.get_spec("0940_LONG_MOVE_040")
    selected, decisions = experiment.apply_selection_overlay(candidates, spec)
    audit = experiment._NEUTRAL_RUN_BACKTEST(
        selected,
        minute_paths,
        variant="FRESH_STAGE7_REFERENCE",
        policy=_reference_policy(),
        target_exposure_per_entry_rs=50_000.0,
    )
    run_dir = output_dir / "fresh_stage7_references" / label.lower()
    run_dir.mkdir(parents=True, exist_ok=False)
    audit_path = run_dir / "candidate_order_audit.csv"
    audit.to_csv(audit_path, index=False)
    decisions.to_csv(run_dir / "selection_decisions.csv", index=False)
    metric = filters._metric_row(
        audit, label=label, variant="FRESH_STAGE7_REFERENCE", sessions=sessions
    )
    gaps._write_json(run_dir / "summary.json", metric)
    return audit_path, audit, minute_paths, sessions


def exact_parity(observed: pd.DataFrame, reference: pd.DataFrame) -> dict[str, Any]:
    columns = combo.PARITY_COLUMNS
    left = observed[list(columns)].sort_values("candidate_id").reset_index(drop=True)
    right = reference[list(columns)].sort_values("candidate_id").reset_index(drop=True)
    if left["candidate_id"].astype(str).tolist() != right["candidate_id"].astype(str).tolist():
        raise AssertionError("fresh control parity candidate IDs differ")
    mismatches: dict[str, int] = {}
    numeric = {
        "entry_price", "stop_price", "target_price", "exit_price",
        "gross_return_pct", "net_return_pct", "quantity", "gross_pnl_rs",
        "estimated_cost_rs", "net_pnl_rs",
    }
    for column in columns:
        if column == "candidate_id":
            continue
        if column in numeric:
            a = pd.to_numeric(left[column], errors="coerce").to_numpy(float)
            b = pd.to_numeric(right[column], errors="coerce").to_numpy(float)
            same = np.isclose(a, b, rtol=0.0, atol=1e-9, equal_nan=True)
        else:
            a = left[column].fillna("<NA>").astype(str).replace({"": "<NA>"}).to_numpy()
            b = right[column].fillna("<NA>").astype(str).replace({"": "<NA>"}).to_numpy()
            same = a == b
        count = int((~same).sum())
        if count:
            mismatches[column] = count
    if mismatches:
        raise AssertionError(f"fresh control parity failed: {mismatches}")
    return {"passed": True, "candidate_rows": len(left), "columns": list(columns)}


def _period_metric_rows(
    audit: pd.DataFrame,
    *,
    dataset: str,
    variant: str,
    sessions: Sequence[date],
) -> list[dict[str, Any]]:
    periods: list[tuple[str, tuple[date, ...]]]
    if dataset == "historical_59_sessions":
        periods = [
            ("FULL", tuple(sessions)),
            ("TRAIN", tuple(day for day in sessions if day < SPLIT_DAY)),
            ("TEST", tuple(day for day in sessions if day >= SPLIT_DAY)),
        ]
    else:
        periods = [("TODAY", tuple(sessions))]
    rows: list[dict[str, Any]] = []
    for period, days in periods:
        subset = audit.loc[audit["session_date"].isin(days)].copy()
        row = filters._metric_row(
            subset, label=dataset, variant=variant, sessions=days
        )
        row["period"] = period
        rows.append(row)
    return rows


def run_v8_controls(
    datasets: Mapping[str, tuple[Path, pd.DataFrame, Sequence[date]]],
    output_dir: Path,
) -> tuple[pd.DataFrame, dict[tuple[str, str], pd.DataFrame]]:
    experiment.configure_engine("0940_LONG_MOVE_040")
    engine._confirmation_check = experiment._NEUTRAL_CONFIRMATION_CHECK
    rows: list[dict[str, Any]] = []
    audits: dict[tuple[str, str], pd.DataFrame] = {}
    gap_control = gaps.GapGuardSpec("CONTROL", None)
    for dataset, (manifest_path, minute_paths, sessions_value) in datasets.items():
        _, candidates, _, loaded_sessions = _load_cache(manifest_path)
        sessions = tuple(sessions_value)
        if tuple(loaded_sessions) != sessions:
            raise AssertionError("V8 control session binding changed")
        for scenario, cost_bps, slippage_bps in gaps.COST_SCENARIOS:
            print(f"[REPAIRED-V8] dataset={dataset} scenario={scenario}", flush=True)
            audit = experiment._NEUTRAL_RUN_BACKTEST(
                candidates,
                minute_paths,
                variant="V8_COMBINED_CONTROL",
                policy=_reference_policy(cost_bps, slippage_bps),
                target_exposure_per_entry_rs=50_000.0,
            )
            audits[(dataset, scenario)] = audit
            periods = (
                [("FULL", sessions),
                 ("TRAIN", tuple(day for day in sessions if day < SPLIT_DAY)),
                 ("TEST", tuple(day for day in sessions if day >= SPLIT_DAY))]
                if dataset == "HISTORICAL"
                else [("TODAY", sessions)]
            )
            for period, days in periods:
                row, daily = gaps.metric_row(
                    audit.loc[audit["session_date"].isin(days)].copy(),
                    days,
                    dataset=dataset,
                    period=period,
                    scenario=scenario,
                    spec=gap_control,
                    cost_bps=cost_bps,
                    slippage_bps=slippage_bps,
                )
                row.update({
                    "profile_id": "V8_COMBINED_CONTROL",
                    "selection_variant": "V10B_RAW_0940_LONG_MOVE_MIN_020",
                    "gap_variant": "CONTROL",
                    "research_design": "REPAIRED_SOURCE_COMPARATOR",
                    "predeclared": False,
                })
                rows.append(row)
                if period in {"FULL", "TODAY"}:
                    run_dir = output_dir / "v8_combined_control" / dataset.lower() / scenario.lower()
                    gaps._write_run_artifacts(run_dir, audit, daily, row)
    return pd.DataFrame(rows), audits


def run_individual_suite(
    *,
    historical_manifest: Path,
    historical_snapshot: Path,
    historical_reference: pd.DataFrame,
    today_manifest: Path,
    today_snapshot: Path,
    today_reference: pd.DataFrame,
    output_dir: Path,
    stamp: str,
) -> tuple[pd.DataFrame, dict[str, Path], dict[str, Any]]:
    suite_root = output_dir / "individual_filter_suite"
    filters.ROOT = suite_root
    references = {
        "historical_59_sessions": historical_reference,
        "today_2026_08_27": today_reference,
    }
    parity: dict[str, Any] = {}

    def anchor(label: str, audit: pd.DataFrame) -> None:
        parity[label] = exact_parity(audit, references[label])

    filters._assert_control_anchor = anchor
    contracts = (
        filters.DatasetContract(
            "historical_59_sessions", historical_manifest, historical_snapshot, "2026-08-06"
        ),
        filters.DatasetContract(
            "today_2026_08_27", today_manifest, today_snapshot, None
        ),
    )
    summary_parts: list[pd.DataFrame] = []
    period_parts: list[pd.DataFrame] = []
    pairwise: list[dict[str, Any]] = []
    paths: dict[str, Path] = {}
    detailed_rows: list[dict[str, Any]] = []
    for contract in contracts:
        summary, period, deltas, provenance = filters.run_dataset(contract, run_stamp=stamp)
        summary_parts.append(summary)
        period_parts.append(period)
        pairwise.extend(deltas)
        for variant, provenance_path in provenance.items():
            paths[f"{contract.label}|{variant}"] = provenance_path
            audit = pd.read_csv(provenance_path.parent / "candidate_order_audit.csv")
            audit["session_date"] = pd.to_datetime(audit["session_date"]).dt.date
            cache_payload = json.loads(
                contract.cache_manifest.read_text(encoding="utf-8")
            )
            sessions = tuple(
                engine._parse_day(value)
                for value in cache_payload["session_dates"]
            )
            detailed_rows.extend(
                _period_metric_rows(
                    audit, dataset=contract.label, variant=variant, sessions=sessions
                )
            )
    comparison = suite_root / "comparison"
    comparison.mkdir(parents=True, exist_ok=False)
    pd.concat(summary_parts, ignore_index=True).to_csv(comparison / "all_results_summary.csv", index=False)
    pd.concat(period_parts, ignore_index=True).to_csv(comparison / "legacy_period_summary.csv", index=False)
    pd.DataFrame(detailed_rows).to_csv(comparison / "all_period_metrics.csv", index=False)
    pd.DataFrame(pairwise).to_csv(comparison / "stage7_pairwise_deltas.csv", index=False)
    pd.DataFrame(
        [
            {"dataset_variant": key, "provenance_path": str(value), "provenance_sha256": gaps._sha256_file(value)}
            for key, value in paths.items()
        ]
    ).to_csv(comparison / "run_provenance_index.csv", index=False)
    gaps._write_json(comparison / "fresh_stage7_parity.json", parity)
    return pd.DataFrame(detailed_rows), paths, parity


def _old_individual_period_metrics() -> pd.DataFrame:
    index = pd.read_csv(OLD_FILTER_COMPARISON / "run_provenance_index.csv")
    rows: list[dict[str, Any]] = []
    for item in index.to_dict("records"):
        provenance = Path(str(item["provenance_path"]))
        audit = pd.read_csv(provenance.parent / "candidate_order_audit.csv")
        audit["session_date"] = pd.to_datetime(audit["session_date"]).dt.date
        payload = json.loads(provenance.read_text(encoding="utf-8"))
        window = dict(payload["window"])
        if str(item["dataset"]) == "historical_59_sessions":
            sessions = tuple(engine.expected_regular_session_dates(window["from_day"], window["through_day"]))
        else:
            sessions = (TODAY_DAY,)
        rows.extend(
            _period_metric_rows(
                audit,
                dataset=str(item["dataset"]),
                variant=str(item["variant"]),
                sessions=sessions,
            )
        )
    return pd.DataFrame(rows)


DRIFT_FIELDS = (
    "candidates", "confirmed", "fills", "closed_fills", "wins", "losses",
    "win_rate_pct", "profit_factor", "net_return_points", "net_pnl_rs",
    "max_daily_drawdown_points", "positive_days", "negative_days", "flat_days",
    "data_incomplete_candidates",
)


def metric_drift(
    old: pd.DataFrame,
    repaired: pd.DataFrame,
    *,
    keys: Sequence[str],
    fields: Iterable[str],
) -> pd.DataFrame:
    fields = tuple(field for field in fields if field in old.columns and field in repaired.columns)
    left = old[list(keys) + list(fields)].copy()
    right = repaired[list(keys) + list(fields)].copy()
    merged = left.merge(right, on=list(keys), how="outer", suffixes=("_old", "_repaired"), indicator=True)
    for field in fields:
        merged[f"delta_{field}"] = (
            pd.to_numeric(merged[f"{field}_repaired"], errors="coerce")
            - pd.to_numeric(merged[f"{field}_old"], errors="coerce")
        )
    return merged


def candidate_identity_drift(
    old_paths: Mapping[tuple[str, str], Path],
    new_paths: Mapping[str, Path],
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for (dataset, variant), old_path in old_paths.items():
        key = f"{dataset}|{variant}"
        if key not in new_paths:
            continue
        old = pd.read_csv(old_path)[["candidate_id", "status", "filled"]]
        new = pd.read_csv(new_paths[key].parent / "candidate_order_audit.csv")[["candidate_id", "status", "filled"]]
        old_ids = set(old["candidate_id"].astype(str))
        new_ids = set(new["candidate_id"].astype(str))
        joined = old.merge(new, on="candidate_id", suffixes=("_old", "_repaired"))
        rows.append({
            "dataset": dataset,
            "variant": variant,
            "old_candidates": len(old_ids),
            "repaired_candidates": len(new_ids),
            "retained_candidate_ids": len(old_ids & new_ids),
            "removed_candidate_ids": len(old_ids - new_ids),
            "added_candidate_ids": len(new_ids - old_ids),
            "retained_status_changes": int(
                (
                    joined["status_old"].astype(str)
                    != joined["status_repaired"].astype(str)
                ).sum()
            ),
            "retained_fill_changes": int(
                (
                    joined["filled_old"].astype(str)
                    != joined["filled_repaired"].astype(str)
                ).sum()
            ),
        })
    return pd.DataFrame(rows)


def _old_individual_audit_paths() -> dict[tuple[str, str], Path]:
    index = pd.read_csv(OLD_FILTER_COMPARISON / "run_provenance_index.csv")
    return {
        (str(row["dataset"]), str(row["variant"])): Path(
            str(row["provenance_path"])
        ).parent
        / "candidate_order_audit.csv"
        for row in index.to_dict("records")
    }


def _write_summary_report(
    output_dir: Path,
    individual: pd.DataFrame,
    combo_metrics: pd.DataFrame,
    v8_metrics: pd.DataFrame,
    parity: Mapping[str, Any],
) -> None:
    individual_head = individual.loc[individual["period"].isin(["FULL", "TODAY"])].copy()
    combo_head = combo_metrics.loc[
        combo_metrics["scenario"].eq("REFERENCE_15_0")
        & combo_metrics["period"].isin(["FULL", "TODAY"])
    ].copy()
    v8_head = v8_metrics.loc[
        v8_metrics["scenario"].eq("REFERENCE_15_0")
        & v8_metrics["period"].isin(["FULL", "TODAY"])
    ].copy()
    lines = [
        "# V10 repaired immutable-snapshot rerun",
        "",
        "Research-only; frozen/live files were not modified.",
        "",
        "## Integrity",
        "",
        f"- Fresh Stage-7 exact parity passed: `{all(bool(value.get('passed')) for value in parity.values())}`.",
        "- Historical: static 208-stock 26AUG universe, 59 sessions, May 27-Aug 19.",
        "- Today: repaired dated 210-stock 26SEP universe for Aug 27.",
        "- The supplied 26AUG today snapshot was rejected before cache construction.",
        "- Economics: 15 bps / 0 slip plus 20+2 and 25+5 stress for V8 and combo.",
        "",
        "## Reference metric files",
        "",
        "- `individual_filter_suite/comparison/all_period_metrics.csv`",
        "- `combo_suite/.../all_period_metrics.csv`",
        "- `v8_combined_control_metrics.csv`",
        "- `repaired_primary_comparison.csv`",
        "- `drift/individual_old_vs_repaired.csv`",
        "- `drift/combo_old_vs_repaired.csv`",
        "",
        (
            f"Individual headline rows: {len(individual_head)}; "
            f"combo headline rows: {len(combo_head)}; "
            f"V8 headline rows: {len(v8_head)}."
        ),
        "",
        f"Package: `{output_dir}`",
        "",
    ]
    (output_dir / "report.md").write_text("\n".join(lines), encoding="utf-8")


def run(args: argparse.Namespace) -> Path:
    source_contracts = validate_source_contracts(
        args.historical_snapshot, args.today_snapshot, args.rejected_today_snapshot
    )
    stamp = datetime.now(gaps.IST).strftime("%Y%m%dT%H%M%S%f%z")
    output_dir = args.output_root.resolve() / "runs" / f"repaired_{stamp}"
    output_dir.mkdir(parents=True, exist_ok=False)

    historical_cache = (
        build_cache(
            snapshot=args.historical_snapshot,
            cache_dir=args.output_root / "caches" / "historical_59_sessions",
            from_day=HIST_FROM,
            through_day=HIST_THROUGH,
            rebuild=args.rebuild_caches,
        )
        if args.historical_cache_manifest is None
        else args.historical_cache_manifest.resolve()
    )
    today_cache = (
        build_cache(
            snapshot=args.today_snapshot,
            cache_dir=args.output_root / "caches" / "today_2026_08_27_sep",
            from_day=TODAY_DAY,
            through_day=TODAY_DAY,
            rebuild=args.rebuild_caches,
        )
        if args.today_cache_manifest is None
        else args.today_cache_manifest.resolve()
    )
    hist_cache_payload, _, _, hist_sessions = _load_cache(historical_cache)
    today_cache_payload, _, _, today_sessions = _load_cache(today_cache)
    if len(hist_sessions) != 59 or hist_sessions[0] != HIST_FROM or hist_sessions[-1] != HIST_THROUGH:
        raise AssertionError("repaired historical cache is not the required 59-session window")
    if today_sessions != (TODAY_DAY,):
        raise AssertionError("repaired today cache is not exactly 2026-08-27")
    if (
        hist_cache_payload["input_contract"]["snapshot_fingerprint"]
        != source_contracts["historical_accepted"]["snapshot_fingerprint"]
    ):
        raise AssertionError("historical cache is not bound to repaired snapshot")
    if (
        today_cache_payload["input_contract"]["snapshot_fingerprint"]
        != source_contracts["today_sep_accepted"]["snapshot_fingerprint"]
    ):
        raise AssertionError("today cache is not bound to corrected SEP snapshot")

    hist_ref_path, hist_ref, hist_minute, hist_sessions = create_fresh_stage7_reference(
        label="HISTORICAL", manifest_path=historical_cache, output_dir=output_dir
    )
    today_ref_path, today_ref, today_minute, today_sessions = create_fresh_stage7_reference(
        label="TODAY", manifest_path=today_cache, output_dir=output_dir
    )

    individual_metrics, individual_paths, individual_parity = run_individual_suite(
        historical_manifest=historical_cache,
        historical_snapshot=args.historical_snapshot,
        historical_reference=hist_ref,
        today_manifest=today_cache,
        today_snapshot=args.today_snapshot,
        today_reference=today_ref,
        output_dir=output_dir,
        stamp=stamp,
    )

    datasets = {
        "HISTORICAL": (historical_cache, hist_minute, hist_sessions),
        "TODAY": (today_cache, today_minute, today_sessions),
    }
    v8_metrics, _ = run_v8_controls(datasets, output_dir)
    v8_metrics.to_csv(output_dir / "v8_combined_control_metrics.csv", index=False)

    max050_refs = {
        ("HISTORICAL", "MAX050"): individual_paths[
            "historical_59_sessions|0935_LONG_MOVE_MAX_050"
        ].parent / "candidate_order_audit.csv",
        ("TODAY", "MAX050"): individual_paths[
            "today_2026_08_27|0935_LONG_MOVE_MAX_050"
        ].parent / "candidate_order_audit.csv",
    }
    gap2_refs: dict[tuple[str, str], Path] = {}
    experiment.configure_engine("0940_LONG_MOVE_040")
    engine._confirmation_check = experiment._NEUTRAL_CONFIRMATION_CHECK
    for dataset, manifest, reference_path in (
        ("HISTORICAL", historical_cache, hist_ref_path),
        ("TODAY", today_cache, today_ref_path),
    ):
        bundle = gaps.load_dataset(dataset, manifest, reference_path)
        selected, _ = filters.selection_overlay(
            bundle.candidates, filters.SPEC_BY_NAME["STAGE7_CONTROL"]
        )
        with gaps.installed_gap_guard(combo.GAP_BY_NAME["MAX_2_BPS"]):
            audit = experiment._NEUTRAL_RUN_BACKTEST(
                selected,
                bundle.minute_paths,
                variant="FRESH_STAGE7_GAP2_REFERENCE",
                policy=_reference_policy(),
                target_exposure_per_entry_rs=50_000.0,
            )
        gap_dir = output_dir / "fresh_gap2_references" / dataset.lower()
        gap_dir.mkdir(parents=True, exist_ok=False)
        gap_path = gap_dir / "candidate_order_audit.parquet"
        audit.to_parquet(gap_path, index=False)
        gap2_refs[(dataset, "STAGE7_GAP2")] = gap_path

    combo._prior_filter_references = lambda: max050_refs
    combo._prior_gap2_references = lambda: gap2_refs
    combo_root = output_dir / "combo_suite"
    combo_dir = combo.run(
        argparse.Namespace(
            historical_cache_manifest=historical_cache,
            historical_reference_audit=hist_ref_path,
            today_cache_manifest=today_cache,
            today_reference_audit=today_ref_path,
            output_root=combo_root,
        )
    )
    combo_metrics = pd.read_csv(combo_dir / "all_period_metrics.csv")
    primary_comparison = pd.concat(
        [
            v8_metrics,
            combo_metrics,
        ],
        ignore_index=True,
        sort=False,
    ).sort_values(
        ["dataset", "scenario", "period", "profile_id"], kind="stable"
    )
    primary_comparison.to_csv(
        output_dir / "repaired_primary_comparison.csv", index=False
    )

    drift_dir = output_dir / "drift"
    drift_dir.mkdir()
    old_individual = _old_individual_period_metrics()
    individual_drift = metric_drift(
        old_individual,
        individual_metrics,
        keys=("dataset", "variant", "period"),
        fields=DRIFT_FIELDS,
    )
    individual_drift.to_csv(drift_dir / "individual_old_vs_repaired.csv", index=False)
    candidate_identity_drift(
        _old_individual_audit_paths(), individual_paths
    ).to_csv(drift_dir / "individual_candidate_identity_drift.csv", index=False)
    old_combo = pd.read_csv(OLD_COMBO_RUN / "all_period_metrics.csv")
    combo_drift = metric_drift(
        old_combo,
        combo_metrics,
        keys=("dataset", "period", "scenario", "profile_id"),
        fields=(
            "candidates", "fills", "wins", "losses", "win_rate_pct",
            "profit_factor", "net_return_points", "net_pnl_rs",
            "max_daily_drawdown_points", "positive_days", "negative_days",
            "flat_days", "remaining_gap_fills", "guard_rejections",
            "data_incomplete_candidates",
        ),
    )
    combo_drift.to_csv(drift_dir / "combo_old_vs_repaired.csv", index=False)
    gaps._write_json(
        drift_dir / "v8_old_drift_status.json",
        {
            "variant": "V8_COMBINED_CONTROL",
            "status": "NO_COUNTERPART_IN_THE_TWO_REQUESTED_FROZEN_PACKAGES",
            "repaired_metrics_available": True,
            "note": "V8 is an added repaired-source comparator, not an original filter/combo package member.",
        },
    )

    feasibility = filters.market_sector_feasibility(
        args.historical_snapshot, args.today_snapshot
    )
    gaps._write_json(output_dir / "market_sector_feasibility.json", feasibility)
    gaps._write_json(output_dir / "source_contracts.json", source_contracts)
    gaps._write_json(output_dir / "fresh_control_parity.json", individual_parity)
    _write_summary_report(output_dir, individual_metrics, combo_metrics, v8_metrics, individual_parity)

    source_dir = output_dir / "source"
    source_dir.mkdir()
    source_files = (
        Path(__file__).resolve(), Path(filters.__file__).resolve(),
        Path(gaps.__file__).resolve(), Path(combo.__file__).resolve(),
        Path(experiment.__file__).resolve(), Path(engine.__file__).resolve(),
    )
    for source in source_files:
        shutil.copy2(source, source_dir / source.name)

    provenance_path = output_dir / "provenance.json"
    provenance = {
        "schema_version": SCHEMA_VERSION,
        "complete": True,
        "created_at_ist": datetime.now(gaps.IST),
        "source_contracts": source_contracts,
        "cache_manifests": {
            "historical": gaps._json_ready(filters.artifact_record(historical_cache)),
            "today_sep": gaps._json_ready(filters.artifact_record(today_cache)),
        },
        "fresh_stage7_references": {
            "historical": filters.artifact_record(hist_ref_path),
            "today": filters.artifact_record(today_ref_path),
        },
        "fresh_control_parity": individual_parity,
        "individual_variants": [spec.payload() for spec in filters.SPECS],
        "combo_profiles": [asdict(profile) for profile in combo.PROFILES],
        "cost_scenarios": [
            {"scenario": name, "cost_bps": cost, "slippage_bps": slip}
            for name, cost, slip in gaps.COST_SCENARIOS
        ],
        "v8_comparator": {
            "profile_id": "V8_COMBINED_CONTROL",
            "five_minute_contract": "UNMODIFIED_V10B_RAW_CACHE;09:40_LONG_MIN_0.20",
            "full_chronological_state_machine": True,
        },
        "split_day": SPLIT_DAY,
        "old_packages": {
            "individual": str(OLD_FILTER_COMPARISON.resolve()),
            "combo": str(OLD_COMBO_RUN.resolve()),
        },
        "outputs": {
            "individual_metrics": str(
                (
                    output_dir
                    / "individual_filter_suite"
                    / "comparison"
                    / "all_period_metrics.csv"
                ).resolve()
            ),
            "combo_metrics": str((combo_dir / "all_period_metrics.csv").resolve()),
            "v8_metrics": str((output_dir / "v8_combined_control_metrics.csv").resolve()),
            "primary_comparison": str(
                (output_dir / "repaired_primary_comparison.csv").resolve()
            ),
            "individual_drift": str((drift_dir / "individual_old_vs_repaired.csv").resolve()),
            "combo_drift": str((drift_dir / "combo_old_vs_repaired.csv").resolve()),
        },
        "limitations": [
            "HISTORICAL_STATIC_26AUG_UNIVERSE_NOT_POINT_IN_TIME_ROLLING",
            "TODAY_EQUITY_LATEST_REAL_END_LABEL_15_15_NOT_15_30",
            "LAST_REAL_BAR_SENSITIVITY_NOT_HEADLINE",
            "EXPLORATORY_COMBO_POST_SELECTION_AND_NOT_PREDECLARED",
            "MULTIPLE_VARIANTS_REQUIRE_UNTOUCHED_PROSPECTIVE_VALIDATION",
            "NO_CAUSALLY_BOUND_MARKET_OR_SECTOR_INDEX_SERIES",
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
    gaps._write_json(
        args.output_root.resolve() / "latest.json",
        {
            "schema_version": SCHEMA_VERSION,
            "run_dir": str(output_dir),
            "provenance_sha256": gaps._sha256_file(provenance_path),
            "research_only": True,
            "promotion_eligible": False,
        },
    )
    print(f"[REPAIRED-V10] complete: {output_dir}", flush=True)
    return output_dir


def parser() -> argparse.ArgumentParser:
    value = argparse.ArgumentParser(description=__doc__)
    value.add_argument("--output-root", type=Path, default=OUTPUT_ROOT)
    value.add_argument("--historical-snapshot", type=Path, default=HISTORICAL_SNAPSHOT)
    value.add_argument("--today-snapshot", type=Path, default=TODAY_SEP_SNAPSHOT)
    value.add_argument("--rejected-today-snapshot", type=Path, default=REJECTED_TODAY_AUG_SNAPSHOT)
    value.add_argument("--historical-cache-manifest", type=Path)
    value.add_argument("--today-cache-manifest", type=Path)
    value.add_argument("--rebuild-caches", action="store_true")
    return value


def main(argv: Sequence[str] | None = None) -> int:
    run(parser().parse_args(argv))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
