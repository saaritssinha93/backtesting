#!/usr/bin/env python3
"""Replay the six requested V6/V8/V10 strategies for one sealed session.

This is a research-only adapter around the existing causal engines.  It does
not edit a live, paper, frozen, or scheduled strategy.  V10 and strict-V6 use
separate source-bound candidate caches because their setup books differ.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import shutil
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd

TOOLS_ROOT = Path(__file__).resolve().parent
WORKSPACE_ROOT = TOOLS_ROOT.parent
if str(WORKSPACE_ROOT) not in sys.path:
    sys.path.insert(0, str(WORKSPACE_ROOT))

import fno_oi_common as common
import fno_v10_experiment_backtest as experiment
import fno_v10_followup_challenger_research as filters
import fno_v10_gap_guard_research as gaps
import fno_v10_repaired_snapshot_rerun as repaired_v10
import fno_v8_windowed_1m_entry_backtest as engine
from tools import fno_v6_isolated_challenger_replay as v6


SCHEMA_VERSION = "fno_today_six_strategy_replay_v1"
IST = timezone(timedelta(hours=5, minutes=30))
COST_BPS = 15.0
SLIPPAGE_BPS = 0.0
SQUARE_OFF = "15:30"
EOD_POLICY = "LAST_REAL_BAR_SENSITIVITY"
EXPECTED_STRATEGIES = (
    "V6_CONTROL",
    "V6_A1_A2_0935_LONG_MAX_050",
    "V8_COMBINED",
    "V10_STAGE7",
    "V10_STAGE7_0935_LONG_MAX_050",
    "V10_STAGE7_0935_LONG_MAX_050_GAP2",
)
EXPECTED_CAPTURE_ROLES = {
    "NSE_EQUITY_1M": 210,
    "NFO_FUTURES_5M": 210,
}


def sha256_file(path: Path | str) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def canonical_sha256(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(
            json_ready(value),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        ).encode("utf-8")
    ).hexdigest()


def json_ready(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): json_ready(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_ready(item) for item in value]
    if isinstance(value, Path):
        return str(value.resolve())
    if isinstance(value, (date, datetime, pd.Timestamp)):
        return value.isoformat()
    if isinstance(value, np.integer):
        return int(value)
    if isinstance(value, np.floating):
        value = float(value)
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if value is pd.NA:
        return None
    return value


def write_json(path: Path, value: Any) -> None:
    common.atomic_write_json(path, json_ready(value))


def artifact(path: Path) -> dict[str, Any]:
    resolved = path.resolve()
    return {
        "path": str(resolved),
        "bytes": int(resolved.stat().st_size),
        "sha256": sha256_file(resolved),
    }


def validate_snapshot(path: Path, session: date) -> dict[str, Any]:
    resolved = path.resolve()
    payload = json.loads(resolved.read_text(encoding="utf-8"))
    if payload.get("schema_version") != "fno_backtest_source_snapshot_v1":
        raise ValueError(f"Unexpected snapshot schema: {resolved}")
    if payload.get("complete") is not True or payload.get("physical_copy") is not True:
        raise ValueError(f"Snapshot is not a complete physical copy: {resolved}")
    universe = dict(payload.get("universe", {}))
    if str(universe.get("master_date", "")) != session.isoformat():
        raise ValueError("Snapshot master date does not match requested session")
    if str(universe.get("contract_month_filter", "")).upper() != "26SEP":
        raise ValueError("Today snapshot must use 26SEP")
    if int(universe.get("mapped_stock_futures", -1)) != 210:
        raise ValueError("Today snapshot must contain 210 mapped stock futures")
    captures = list(payload.get("captures", []))
    if len(captures) != 420:
        raise ValueError(f"Expected 420 physical captures, observed {len(captures)}")
    roles: dict[str, int] = {}
    for item in captures:
        role = str(item.get("role", ""))
        roles[role] = roles.get(role, 0) + 1
    if roles != EXPECTED_CAPTURE_ROLES:
        raise ValueError(f"Unexpected snapshot roles: {roles}")
    return {
        "manifest": artifact(resolved),
        "snapshot_fingerprint": str(payload.get("snapshot_fingerprint", "")),
        "universe": universe,
        "capture_roles": roles,
    }


def _bool_series(values: pd.Series) -> pd.Series:
    def convert(value: Any) -> bool:
        if isinstance(value, (bool, np.bool_)):
            return bool(value)
        if value is None or value is pd.NA:
            return False
        if isinstance(value, float) and math.isnan(value):
            return False
        text = str(value).strip().lower()
        if text in {"true", "1", "yes", "y"}:
            return True
        if text in {"false", "0", "no", "n", "", "nan", "none", "<na>"}:
            return False
        raise ValueError(f"Unsupported Boolean value: {value!r}")

    return values.map(convert).astype(bool)


def metric_row(
    audit: pd.DataFrame,
    *,
    strategy: str,
    session: date,
    source_complete: bool,
    incomplete_symbol_sessions: int,
) -> dict[str, Any]:
    if audit.empty:
        filled = pd.Series(False, index=audit.index, dtype=bool)
    else:
        filled = _bool_series(audit["filled"])
    returns = pd.to_numeric(audit.get("net_return_pct"), errors="coerce")
    pnl = pd.to_numeric(audit.get("net_pnl_rs"), errors="coerce")
    closed = filled & np.isfinite(returns) & np.isfinite(pnl)
    closed_returns = returns.loc[closed]
    closed_pnl = pnl.loc[closed]
    profits = float(closed_returns.loc[closed_returns.gt(0)].sum())
    losses = float(-closed_returns.loc[closed_returns.lt(0)].sum())
    return {
        "session_date": session.isoformat(),
        "strategy": strategy,
        "candidates": int(len(audit)),
        "fills": int(closed.sum()),
        "wins": int(closed_returns.gt(0).sum()),
        "losses": int(closed_returns.lt(0).sum()),
        "flat_trades": int(closed_returns.eq(0).sum()),
        "win_rate_pct": (
            float(closed_returns.gt(0).mean() * 100.0)
            if len(closed_returns)
            else None
        ),
        "profit_factor": (
            profits / losses
            if losses > 0
            else math.inf
            if profits > 0
            else None
        ),
        "net_return_points": float(closed_returns.sum()),
        "net_pnl_rs": float(closed_pnl.sum()),
        "positive_session": bool(float(closed_returns.sum()) > 0),
        "source_complete": bool(source_complete),
        "source_incomplete_symbol_sessions": int(incomplete_symbol_sessions),
        "headline_valid": False,
        "last_real_bar_sensitivity": True,
        "research_only": True,
        "promotion_eligible": False,
    }


def write_strategy(
    root: Path,
    strategy: str,
    audit: pd.DataFrame,
    decisions: pd.DataFrame,
    metric: Mapping[str, Any],
) -> dict[str, Any]:
    run_dir = root / "strategies" / strategy.lower()
    run_dir.mkdir(parents=True, exist_ok=False)
    audit_path = run_dir / "candidate_order_audit.csv"
    decisions_path = run_dir / "selection_decisions.csv"
    closed_path = run_dir / "closed_trades.csv"
    summary_path = run_dir / "summary.json"
    common.atomic_write_csv(audit, audit_path)
    common.atomic_write_csv(decisions, decisions_path)
    closed = audit.loc[
        _bool_series(audit["filled"])
        & np.isfinite(pd.to_numeric(audit["net_return_pct"], errors="coerce"))
        & np.isfinite(pd.to_numeric(audit["net_pnl_rs"], errors="coerce"))
    ].copy()
    common.atomic_write_csv(closed, closed_path)
    write_json(summary_path, metric)
    return {
        "directory": str(run_dir.resolve()),
        "audit": artifact(audit_path),
        "decisions": artifact(decisions_path),
        "closed_trades": artifact(closed_path),
        "summary": artifact(summary_path),
    }


def replay_v10(
    snapshot: Path, session: date, output: Path
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    cache_manifest = repaired_v10.build_cache(
        snapshot=snapshot,
        cache_dir=output / "v10_cache",
        from_day=session,
        through_day=session,
        rebuild=True,
    )
    cache, candidates, minute_paths, sessions = repaired_v10._load_cache(cache_manifest)
    if sessions != (session,):
        raise AssertionError(f"V10 cache sessions changed: {sessions}")
    experiment.configure_engine("0940_LONG_MOVE_040")
    repaired_v10.bind_engine_universe(snapshot)
    engine._confirmation_check = experiment._NEUTRAL_CONFIRMATION_CHECK
    policy = repaired_v10._reference_policy(COST_BPS, SLIPPAGE_BPS)

    coverage_complete = bool(cache.get("headline_source_complete", False))
    incomplete = int(cache.get("source_incomplete_symbol_sessions", 0))
    results: list[tuple[str, pd.DataFrame, pd.DataFrame]] = []

    raw_decisions = candidates[[
        column
        for column in (
            "candidate_id", "session_date", "signal_time", "setup_id", "side",
            "symbol", "price_change_pct", "frozen_rank"
        )
        if column in candidates.columns
    ]].copy()
    raw_decisions["selection_passed"] = True
    raw_decisions["selection_reason"] = "RAW_V8_CONTROL"
    v8_audit = experiment._NEUTRAL_RUN_BACKTEST(
        candidates,
        minute_paths,
        variant="V8_COMBINED",
        policy=policy,
        target_exposure_per_entry_rs=50_000.0,
    )
    results.append(("V8_COMBINED", v8_audit, raw_decisions))

    stage7_selected, stage7_decisions = filters.selection_overlay(
        candidates, filters.SPEC_BY_NAME["STAGE7_CONTROL"]
    )
    stage7_audit = experiment._NEUTRAL_RUN_BACKTEST(
        stage7_selected,
        minute_paths,
        variant="V10_STAGE7",
        policy=policy,
        target_exposure_per_entry_rs=50_000.0,
    )
    results.append(("V10_STAGE7", stage7_audit, stage7_decisions))

    max050_selected, max050_decisions = filters.selection_overlay(
        candidates, filters.SPEC_BY_NAME["0935_LONG_MOVE_MAX_050"]
    )
    max050_audit = experiment._NEUTRAL_RUN_BACKTEST(
        max050_selected,
        minute_paths,
        variant="V10_STAGE7_0935_LONG_MAX_050",
        policy=policy,
        target_exposure_per_entry_rs=50_000.0,
    )
    results.append(("V10_STAGE7_0935_LONG_MAX_050", max050_audit, max050_decisions))

    with gaps.installed_gap_guard(gaps.GAP_GUARDS[2]):
        gap2_audit = experiment._NEUTRAL_RUN_BACKTEST(
            max050_selected,
            minute_paths,
            variant="V10_STAGE7_0935_LONG_MAX_050_GAP2",
            policy=policy,
            target_exposure_per_entry_rs=50_000.0,
        )
    results.append(
        ("V10_STAGE7_0935_LONG_MAX_050_GAP2", gap2_audit, max050_decisions)
    )

    metrics: list[dict[str, Any]] = []
    outputs: dict[str, Any] = {}
    for strategy, audit, decisions in results:
        audit = audit.copy()
        audit["requested_strategy"] = strategy
        metric = metric_row(
            audit,
            strategy=strategy,
            session=session,
            source_complete=coverage_complete,
            incomplete_symbol_sessions=incomplete,
        )
        metrics.append(metric)
        outputs[strategy] = write_strategy(output, strategy, audit, decisions, metric)
    return metrics, {
        "cache_manifest": artifact(cache_manifest),
        "cache_input_fingerprint": str(cache.get("input_fingerprint", "")),
        "candidate_count": int(len(candidates)),
        "minute_path_rows": int(len(minute_paths)),
        "headline_source_complete": coverage_complete,
        "source_incomplete_symbol_sessions": incomplete,
        "strategies": outputs,
    }


def replay_v6(
    snapshot: Path, session: date, output: Path
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    v6.validate_registry()
    v6.strict.configure_engine()
    candidates, minute_paths, coverage, cache, cache_manifest = v6._build_snapshot_inputs(
        snapshot,
        output / "v6_cache",
        from_day=session.isoformat(),
        through_day=session.isoformat(),
        run_label=f"TODAY_{session.isoformat()}_SEP_DIAGNOSTIC",
        expected_master_date=session.isoformat(),
        expected_contract_month_filter="26SEP",
        expected_mapped_stock_futures=210,
    )
    policy = v6.engine.entry_policy_for_variant(
        "VS",
        cost_bps=COST_BPS,
        slippage_bps=SLIPPAGE_BPS,
        square_off=SQUARE_OFF,
        eod_policy=EOD_POLICY,
    )
    selected_specs = (
        v6.CHALLENGER_BY_NAME["CONTROL"],
        v6.CHALLENGER_BY_NAME["A1_A2_0935_LONG_MAX_050"],
    )
    metrics: list[dict[str, Any]] = []
    outputs: dict[str, Any] = {}
    completeness = v6._coverage_summary(coverage, cache)
    for spec in selected_specs:
        replay = v6._replay_one(
            dataset="TODAY",
            spec=spec,
            candidates=candidates,
            minute_paths=minute_paths,
            coverage=coverage,
            manifest=cache,
            policy=policy,
            output_dir=output / "v6_replays",
            split_day=None,
        )
        strategy = (
            "V6_CONTROL"
            if spec.variant == "CONTROL"
            else "V6_A1_A2_0935_LONG_MAX_050"
        )
        audit = replay["audit"].copy()
        audit["requested_strategy"] = strategy
        metric = metric_row(
            audit,
            strategy=strategy,
            session=session,
            source_complete=bool(completeness["headline_source_complete"]),
            incomplete_symbol_sessions=int(
                completeness["source_incomplete_symbol_sessions"]
            ),
        )
        metrics.append(metric)
        outputs[strategy] = {
            "directory": str(Path(replay["directory"]).resolve()),
            "audit": artifact(Path(replay["directory"]) / "candidate_order_audit.csv"),
            "decisions": artifact(Path(replay["directory"]) / "selection_decisions.csv"),
            "summary": artifact(Path(replay["directory"]) / "summary.json"),
        }
    return metrics, {
        "cache_manifest": artifact(cache_manifest),
        "cache_input_fingerprint": str(cache.get("input_fingerprint", "")),
        "candidate_count": int(len(candidates)),
        "minute_path_rows": int(len(minute_paths)),
        "headline_source_complete": bool(completeness["headline_source_complete"]),
        "source_incomplete_symbol_sessions": int(
            completeness["source_incomplete_symbol_sessions"]
        ),
        "strategies": outputs,
    }


def load_existing_v10(
    output: Path, session: date
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    manifests = sorted((output / "v10_cache").glob("*/manifest.json"))
    if len(manifests) != 1:
        raise ValueError(
            f"Expected one completed V10 cache manifest, observed {len(manifests)}"
        )
    cache_manifest = manifests[0]
    cache, candidates, minute_paths, sessions = repaired_v10._load_cache(cache_manifest)
    if sessions != (session,):
        raise AssertionError(f"Resumed V10 cache sessions changed: {sessions}")
    coverage_complete = bool(cache.get("headline_source_complete", False))
    incomplete = int(cache.get("source_incomplete_symbol_sessions", 0))
    strategy_names = EXPECTED_STRATEGIES[2:]
    metrics: list[dict[str, Any]] = []
    outputs: dict[str, Any] = {}
    for strategy in strategy_names:
        run_dir = output / "strategies" / strategy.lower()
        audit_path = run_dir / "candidate_order_audit.csv"
        decisions_path = run_dir / "selection_decisions.csv"
        closed_path = run_dir / "closed_trades.csv"
        summary_path = run_dir / "summary.json"
        for required in (audit_path, decisions_path, closed_path, summary_path):
            if not required.is_file():
                raise FileNotFoundError(f"Incomplete resumed V10 strategy: {required}")
        audit = pd.read_csv(audit_path)
        metric = metric_row(
            audit,
            strategy=strategy,
            session=session,
            source_complete=coverage_complete,
            incomplete_symbol_sessions=incomplete,
        )
        recorded = json.loads(summary_path.read_text(encoding="utf-8"))
        if canonical_sha256(recorded) != canonical_sha256(metric):
            raise AssertionError(f"Resumed V10 metric changed: {strategy}")
        metrics.append(metric)
        outputs[strategy] = {
            "directory": str(run_dir.resolve()),
            "audit": artifact(audit_path),
            "decisions": artifact(decisions_path),
            "closed_trades": artifact(closed_path),
            "summary": artifact(summary_path),
        }
    return metrics, {
        "cache_manifest": artifact(cache_manifest),
        "cache_input_fingerprint": str(cache.get("input_fingerprint", "")),
        "candidate_count": int(len(candidates)),
        "minute_path_rows": int(len(minute_paths)),
        "headline_source_complete": coverage_complete,
        "source_incomplete_symbol_sessions": incomplete,
        "strategies": outputs,
        "resumed_after_v6_adapter_failure": True,
    }


def inventory(root: Path, *, exclude: set[Path]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for path in sorted(item for item in root.rglob("*") if item.is_file()):
        if path.resolve() in {item.resolve() for item in exclude}:
            continue
        rows.append(
            {
                "relative_path": path.relative_to(root).as_posix(),
                "bytes": int(path.stat().st_size),
                "sha256": sha256_file(path),
            }
        )
    return rows


def finalize_output(
    *,
    snapshot_contract: Mapping[str, Any],
    session: date,
    output: Path,
    output_root: Path,
    v10_metrics: Sequence[Mapping[str, Any]],
    v10_provenance: Mapping[str, Any],
    v6_metrics: Sequence[Mapping[str, Any]],
    v6_provenance: Mapping[str, Any],
) -> Path:
    metrics = pd.DataFrame([*v6_metrics, *v10_metrics])
    observed = tuple(metrics["strategy"].astype(str))
    if set(observed) != set(EXPECTED_STRATEGIES) or len(observed) != len(EXPECTED_STRATEGIES):
        raise AssertionError(f"Requested strategy registry changed: {observed}")
    metrics = metrics.set_index("strategy").loc[list(EXPECTED_STRATEGIES)].reset_index()
    comparison_path = output / "comparison.csv"
    common.atomic_write_csv(metrics, comparison_path)

    manifest_path = output / "manifest.json"
    inventory_path = output / "inventory.json"
    write_json(
        inventory_path,
        {
            "schema_version": SCHEMA_VERSION,
            "artifacts": inventory(output, exclude={manifest_path, inventory_path}),
        },
    )
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "complete": True,
        "created_at_ist": datetime.now(IST),
        "session_date": session,
        "snapshot": snapshot_contract,
        "economics": {
            "cost_bps": COST_BPS,
            "slippage_bps": SLIPPAGE_BPS,
            "square_off": SQUARE_OFF,
            "eod_policy": EOD_POLICY,
        },
        "requested_strategies": list(EXPECTED_STRATEGIES),
        "comparison": artifact(comparison_path),
        "v10": v10_provenance,
        "v6": v6_provenance,
        "inventory": artifact(inventory_path),
        "limitations": [
            "LATEST_REAL_EQUITY_BAR_END_LABEL_15_15",
            "LAST_REAL_BAR_SENSITIVITY_NOT_HEADLINE",
            "STATIC_CURRENT_26SEP_UNIVERSE_RESEARCH_ONLY",
            "SINGLE_SESSION_DIAGNOSTIC",
        ],
        "research_only": True,
        "promotion_eligible": False,
        "live_or_paper_authority": False,
    }
    write_json(manifest_path, manifest)
    write_json(
        output_root.resolve() / "latest.json",
        {
            "schema_version": SCHEMA_VERSION,
            "run_dir": output,
            "manifest_sha256": sha256_file(manifest_path),
            "session_date": session,
        },
    )
    return output


def run(snapshot: Path, session: date, output_root: Path) -> Path:
    snapshot_contract = validate_snapshot(snapshot, session)
    stamp = datetime.now(IST).strftime("%Y%m%dT%H%M%S%f%z")
    output = output_root.resolve() / f"today_{session.isoformat()}_{stamp}"
    output.mkdir(parents=True, exist_ok=False)
    source_dir = output / "source"
    source_dir.mkdir()
    shutil.copy2(Path(__file__), source_dir / Path(__file__).name)

    v10_metrics, v10_provenance = replay_v10(snapshot.resolve(), session, output)
    v6_metrics, v6_provenance = replay_v6(snapshot.resolve(), session, output)
    return finalize_output(
        snapshot_contract=snapshot_contract,
        session=session,
        output=output,
        output_root=output_root,
        v10_metrics=v10_metrics,
        v10_provenance=v10_provenance,
        v6_metrics=v6_metrics,
        v6_provenance=v6_provenance,
    )


def resume_failed_run(snapshot: Path, session: date, output: Path) -> Path:
    output = output.resolve()
    if not output.is_dir():
        raise FileNotFoundError(output)
    for forbidden in (output / "manifest.json", output / "comparison.csv"):
        if forbidden.exists():
            raise ValueError(f"Refusing to resume an already finalized run: {forbidden}")
    snapshot_contract = validate_snapshot(snapshot, session)
    source_dir = output / "source"
    source_dir.mkdir(exist_ok=True)
    shutil.copy2(Path(__file__), source_dir / Path(__file__).name)
    v10_metrics, v10_provenance = load_existing_v10(output, session)
    v6_metrics, v6_provenance = replay_v6(snapshot.resolve(), session, output)
    return finalize_output(
        snapshot_contract=snapshot_contract,
        session=session,
        output=output,
        output_root=output.parent,
        v10_metrics=v10_metrics,
        v10_provenance=v10_provenance,
        v6_metrics=v6_metrics,
        v6_provenance=v6_provenance,
    )


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot", type=Path, required=True)
    parser.add_argument("--session-date", type=date.fromisoformat, required=True)
    parser.add_argument(
        "--output-root",
        type=Path,
        default=(
            common.FNO_ROOT
            / "strategy_research"
            / "today_six_strategy_replays_v1"
        ),
    )
    parser.add_argument(
        "--resume-output",
        type=Path,
        help="Resume a pre-finalization run after validating its completed V10 artifacts.",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    if args.resume_output is not None:
        output = resume_failed_run(args.snapshot, args.session_date, args.resume_output)
    else:
        output = run(args.snapshot, args.session_date, args.output_root)
    print(output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
