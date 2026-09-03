#!/usr/bin/env python3
"""Run and publish the daily FnO V6/V8/V10/V11/V12 comparison.

Every strategy is replayed from the same immutable, dated FnO source snapshot
and the same cash-equity one-minute execution paths.  This is the scheduled
FnO dashboard entry point; it intentionally has no dependency on the equity
AVWAP ``backtesting_result_v11_daily.py`` pipeline.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import re
import shutil
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd


TOOLS_DIR = Path(__file__).resolve().parent
BASE_DIR = TOOLS_DIR.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import fno_oi_backtest_provenance as source_provenance
import fno_oi_common as common
import fno_v10_backtest_config as locked_config
import fno_v10_experiment_backtest as experiment
import fno_v10_followup_challenger_research as filters
import fno_v10_gap_guard_research as gaps
import fno_v10_repaired_snapshot_rerun as repaired_v10
import fno_v11_backtest as v11_backtest
import fno_v11_execution_runtime as v11_execution
import fno_v11_gap_runtime as v11_gap
import fno_v12_backtest as v12_backtest
import fno_v12_execution_runtime as v12_execution
import fno_v12_selection_runtime as v12_selection
import fno_v8_windowed_1m_entry_backtest as engine
from tools import fno_today_six_strategy_replay as replay


SCHEMA_VERSION = "fno_daily_strategy_comparison_v1"
PUBLICATION_SCHEMA_VERSION = "fno_daily_dashboard_publication_v1"
EXPECTED_STRATEGIES = (
    "V6_CONTROL",
    "V8_COMBINED",
    "V10_STAGE7_0935_LONG_MAX_050_GAP2",
    "V11_STAGE10_FROZEN",
    "V12_SELECTED",
)
FRIENDLY_NAMES = {
    "V6_CONTROL": "V6 Control",
    "V8_COMBINED": "V8 Combined",
    "V10_STAGE7_0935_LONG_MAX_050_GAP2": "V10 .50 + Gap2",
    "V11_STAGE10_FROZEN": "V11 Stage 10 Frozen",
    "V12_SELECTED": "V12 Selected",
}
COST_BPS = 15.0
SLIPPAGE_BPS = 0.0
TARGET_EXPOSURE_RS = 50_000.0
DEFAULT_RESEARCH_ROOT = common.FNO_ROOT / "strategy_research" / "daily_fno_comparison_v1"
DEFAULT_RUN_ROOT = DEFAULT_RESEARCH_ROOT / "runs"
DEFAULT_SNAPSHOT_ROOT = DEFAULT_RESEARCH_ROOT / "source_snapshots"
DEFAULT_DASHBOARD_ROOT = Path(
    os.environ.get("EQIDV2_RUNTIME_ROOT", r"C:\TradingData\eqidv2")
) / "backtesting_result_v11"


class DailyComparisonError(RuntimeError):
    pass


def _json_ready(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_ready(item) for item in value]
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


def _atomic_write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp.{os.getpid()}")
    try:
        temporary.write_text(text, encoding="utf-8", newline="\n")
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _atomic_write_json(path: Path, payload: Mapping[str, Any]) -> None:
    _atomic_write_text(path, json.dumps(_json_ready(payload), indent=2, sort_keys=True) + "\n")


def _universe_path(session: date) -> Path:
    return common.UNIVERSE_DIR / f"near_month_{session.isoformat()}.parquet"


def _contract_month_for_universe(path: Path) -> str:
    if not path.is_file():
        raise FileNotFoundError(f"Dated FnO universe is missing: {path}")
    frame = pd.read_parquet(path, columns=["expiry", "is_index_future"])
    stocks = frame.loc[~frame["is_index_future"].fillna(False).astype(bool)]
    expiries = pd.to_datetime(stocks["expiry"], errors="coerce").dropna().dt.normalize().unique()
    if len(expiries) != 1:
        raise DailyComparisonError(
            f"Dated FnO universe must contain one stock-future expiry; observed {len(expiries)}"
        )
    expiry = pd.Timestamp(expiries[0])
    return expiry.strftime("%y%b").upper()


def create_daily_snapshot(session: date, snapshot_root: Path) -> Path:
    universe_path = _universe_path(session)
    contract_month = _contract_month_for_universe(universe_path)
    mapped, universe_record = source_provenance.load_backtest_universe(
        universe_path=universe_path,
        universe_date=session,
        contract_month_contains=contract_month,
        require_persisted_mapping=True,
    )
    snapshot = source_provenance.create_source_snapshot(
        mapped,
        universe_record,
        universe_path=universe_path,
        snapshot_root=snapshot_root / session.isoformat(),
        require_complete_sources=True,
    )
    return Path(str(snapshot["manifest_path"])).resolve()


def _configure_v10(snapshot: Path) -> engine.EntryPolicy:
    experiment.configure_engine(locked_config.ACTIVE_VARIANT)
    repaired_v10.bind_engine_universe(snapshot)
    engine._confirmation_check = experiment._NEUTRAL_CONFIRMATION_CHECK
    return repaired_v10._reference_policy(COST_BPS, SLIPPAGE_BPS)


def _run_v11_v12(
    *,
    output: Path,
    snapshot: Path,
    session: date,
    candidates: pd.DataFrame,
    minute_paths: pd.DataFrame,
    source_complete: bool,
    incomplete_count: int,
) -> list[dict[str, Any]]:
    outputs: list[dict[str, Any]] = []

    policy = _configure_v10(snapshot)
    selected, decisions = filters.selection_overlay(
        candidates, filters.SPEC_BY_NAME[v11_backtest.SELECTION_VARIANT]
    )
    with v11_execution.installed_runtime_hooks(
        v11_backtest.FIXED_RUNTIME_SPEC, allow_composite=True
    ):
        with v11_gap.installed_gap_guard(v11_backtest._gap_spec()):
            audit = experiment._NEUTRAL_RUN_BACKTEST(
                selected,
                minute_paths,
                variant=v11_backtest.PROFILE_ID,
                policy=policy,
                target_exposure_per_entry_rs=TARGET_EXPOSURE_RS,
            )
    metric = replay.metric_row(
        audit,
        strategy="V11_STAGE10_FROZEN",
        session=session,
        source_complete=source_complete,
        incomplete_symbol_sessions=incomplete_count,
    )
    metric.update(
        profile_id=v11_backtest.PROFILE_ID,
        profile_sha256=v11_backtest.LOCKED_PROFILE_SHA256,
    )
    replay.write_strategy(output, "V11_STAGE10_FROZEN", audit, decisions, metric)
    outputs.append(metric)

    policy = _configure_v10(snapshot)
    base_setups = tuple(engine.ACTIVE_SETUPS)
    prepared = v12_selection.prepare_variant_selection(
        candidates, base_setups, v12_backtest.FIXED_CONFIG
    )
    try:
        engine.ACTIVE_SETUPS = tuple(prepared.setups)
        runtime = v12_backtest._runtime_spec(prepared)
        with v11_execution.installed_runtime_hooks(
            v11_backtest.FIXED_RUNTIME_SPEC, allow_composite=True
        ):
            with v12_execution.installed_runtime_hooks(runtime):
                with v11_gap.installed_gap_guard(v12_backtest._gap_spec()):
                    audit = experiment._NEUTRAL_RUN_BACKTEST(
                        prepared.candidates,
                        minute_paths,
                        variant=v12_backtest.PROFILE_ID,
                        policy=policy,
                        target_exposure_per_entry_rs=TARGET_EXPOSURE_RS,
                    )
    finally:
        engine.ACTIVE_SETUPS = base_setups
    metric = replay.metric_row(
        audit,
        strategy="V12_SELECTED",
        session=session,
        source_complete=source_complete,
        incomplete_symbol_sessions=incomplete_count,
    )
    metric.update(
        all_input_candidates=int(len(candidates)),
        selected_candidates=int(len(prepared.candidates)),
        profile_id=v12_backtest.PROFILE_ID,
        profile_sha256=v12_backtest.EXPECTED_PROFILE_SHA256,
    )
    replay.write_strategy(output, "V12_SELECTED", audit, prepared.decisions, metric)
    outputs.append(metric)
    return outputs


def _trade_contracts(output: Path) -> dict[str, list[dict[str, Any]]]:
    contracts: dict[str, list[dict[str, Any]]] = {}
    fields = (
        "candidate_id", "symbol", "side", "confirmation_time", "entry_time",
        "entry_price", "stop_price", "target_price", "exit_time", "exit_price",
        "exit_reason", "net_return_pct", "net_pnl_rs",
    )
    for strategy in EXPECTED_STRATEGIES:
        audit_path = output / "strategies" / strategy.lower() / "candidate_order_audit.csv"
        audit = pd.read_csv(audit_path, low_memory=False)
        closed = replay._bool_series(audit["filled"])
        available = [field for field in fields if field in audit.columns]
        contracts[strategy] = _json_ready(audit.loc[closed, available].to_dict("records"))
    return contracts


def run_comparison(
    *, session: date, snapshot: Path, output_root: Path
) -> Path:
    snapshot = snapshot.resolve()
    snapshot_contract = replay.validate_snapshot(snapshot, session)
    stamp = common.now_ist().strftime("%Y%m%dT%H%M%S%f%z")
    output = output_root.resolve() / f"fno_{session.isoformat()}_{stamp}"
    output.mkdir(parents=True, exist_ok=False)
    source_dir = output / "source"
    source_dir.mkdir()
    shutil.copy2(Path(__file__), source_dir / Path(__file__).name)

    v10_metrics, v10_provenance = replay.replay_v10(snapshot, session, output)
    v6_metrics, v6_provenance = replay.replay_v6(snapshot, session, output)

    # The reusable V6 adapter keeps its research artifacts under v6_replays;
    # publish the selected control into the common strategy layout consumed by
    # the dashboard and trade-contract validator.
    v6_control_metric = next(
        dict(row) for row in v6_metrics if row["strategy"] == "V6_CONTROL"
    )
    v6_control_artifacts = v6_provenance["strategies"]["V6_CONTROL"]
    v6_control_audit = pd.read_csv(
        v6_control_artifacts["audit"]["path"], low_memory=False
    )
    v6_control_decisions = pd.read_csv(
        v6_control_artifacts["decisions"]["path"], low_memory=False
    )
    replay.write_strategy(
        output,
        "V6_CONTROL",
        v6_control_audit,
        v6_control_decisions,
        v6_control_metric,
    )

    cache_manifest = Path(str(v10_provenance["cache_manifest"]["path"]))
    cache, candidates, minute_paths, sessions = repaired_v10._load_cache(cache_manifest)
    if sessions != (session,):
        raise DailyComparisonError(f"FnO cache session mismatch: {sessions}")
    source_complete = bool(cache.get("headline_source_complete", False))
    incomplete_count = int(cache.get("source_incomplete_symbol_sessions", 0))
    new_metrics = _run_v11_v12(
        output=output,
        snapshot=snapshot,
        session=session,
        candidates=candidates,
        minute_paths=minute_paths,
        source_complete=source_complete,
        incomplete_count=incomplete_count,
    )

    all_metrics = [*v6_metrics, *v10_metrics, *new_metrics]
    by_strategy = {str(row["strategy"]): dict(row) for row in all_metrics}
    missing = [strategy for strategy in EXPECTED_STRATEGIES if strategy not in by_strategy]
    if missing:
        raise DailyComparisonError(f"FnO strategies did not run: {missing}")
    comparison_rows = [by_strategy[strategy] for strategy in EXPECTED_STRATEGIES]
    for row in comparison_rows:
        row.update(
            cost_bps=COST_BPS,
            slippage_bps=SLIPPAGE_BPS,
            target_exposure_per_entry_rs=TARGET_EXPOSURE_RS,
        )
    comparison = pd.DataFrame(comparison_rows)
    common.atomic_write_csv(comparison, output / "comparison.csv")
    contracts = _trade_contracts(output)
    common.atomic_write_json(output / "trade_contracts.json", contracts)

    manifest = {
        "schema_version": SCHEMA_VERSION,
        "complete": True,
        "created_at_ist": common.now_ist(),
        "session_date": session,
        "snapshot": snapshot_contract,
        "snapshot_manifest": replay.artifact(snapshot),
        "requested_strategies": EXPECTED_STRATEGIES,
        "economics": {
            "cost_bps": COST_BPS,
            "slippage_bps": SLIPPAGE_BPS,
            "target_exposure_per_entry_rs": TARGET_EXPOSURE_RS,
            "square_off": "15:30",
            "eod_policy": "LAST_REAL_BAR_SENSITIVITY",
        },
        "source_complete": bool(all(bool(row["source_complete"]) for row in comparison_rows)),
        "source_incomplete_symbol_sessions": int(
            max(int(row["source_incomplete_symbol_sessions"]) for row in comparison_rows)
        ),
        "comparison": replay.artifact(output / "comparison.csv"),
        "trade_contracts": replay.artifact(output / "trade_contracts.json"),
        "v6": v6_provenance,
        "v10_v8": v10_provenance,
        "research_only": True,
        "promotion_eligible": False,
        "live_or_paper_authority": False,
    }
    common.atomic_write_json(output / "manifest.json", _json_ready(manifest))
    common.atomic_write_json(
        output_root.resolve() / "latest.json",
        {
            "schema_version": SCHEMA_VERSION,
            "session_date": session,
            "run_dir": output,
            "manifest_sha256": replay.sha256_file(output / "manifest.json"),
        },
    )
    return output


def _number(value: Any, default: float = 0.0) -> float:
    try:
        result = float(value)
        return result if math.isfinite(result) else default
    except (TypeError, ValueError):
        return default


def _pf(value: Any) -> str:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return "NA"
    return "inf" if math.isinf(result) else f"{result:.3f}"


def render_report(run_dir: Path) -> tuple[str, dict[str, Any]]:
    manifest = json.loads((run_dir / "manifest.json").read_text(encoding="utf-8"))
    comparison = pd.read_csv(run_dir / "comparison.csv").replace({np.nan: None})
    rows = comparison.to_dict("records")
    observed = tuple(str(row["strategy"]) for row in rows)
    if observed != EXPECTED_STRATEGIES:
        raise DailyComparisonError(f"Published strategy order changed: {observed}")
    contracts = json.loads((run_dir / "trade_contracts.json").read_text(encoding="utf-8"))
    if set(contracts) != set(EXPECTED_STRATEGIES):
        raise DailyComparisonError("Trade-contract strategy set is incomplete")

    ranked = sorted(rows, key=lambda row: _number(row.get("net_pnl_rs")), reverse=True)
    complete = bool(manifest.get("source_complete"))
    status = "COMPLETED — SOURCE COMPLETE" if complete else "COMPLETED — SOURCE COVERAGE WARNING"
    day = str(manifest["session_date"])
    lines = [
        f"# Backtesting result v6/v8/v10/v11/v12 — FnO — {day}",
        "",
        f"**Session status:** {status}",
        "",
        "All five FnO strategies were replayed on one immutable dated source snapshot, with identical economics.",
        "",
        "## FnO strategy comparison",
        "",
        "| Rank | Strategy | Candidates | Fills | W/L | WR | PF | Net return | Net P&L |",
        "|---:|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for rank, row in enumerate(ranked, start=1):
        fills = int(_number(row.get("fills")))
        wins = int(_number(row.get("wins")))
        losses = int(_number(row.get("losses")))
        candidates = int(_number(row.get("candidates")))
        if row["strategy"] == "V12_SELECTED" and row.get("all_input_candidates") is not None:
            candidates_text = f"{candidates}/{int(_number(row['all_input_candidates']))}"
        else:
            candidates_text = str(candidates)
        wr = (100.0 * wins / fills) if fills else 0.0
        lines.append(
            f"| {rank} | {FRIENDLY_NAMES[str(row['strategy'])]} | {candidates_text} | "
            f"{fills} | {wins}/{losses} | {wr:.2f}% | {_pf(row.get('profit_factor'))} | "
            f"{_number(row.get('net_return_points')):+.6f} | ₹{_number(row.get('net_pnl_rs')):+,.2f} |"
        )

    lines.extend([
        "",
        "## Filled trades",
        "",
        "| Strategy | Symbol | Side | Entry | Exit | Result |",
        "|---|---|---|---|---|---:|",
    ])
    trade_count = 0
    for strategy in EXPECTED_STRATEGIES:
        for trade in contracts[strategy]:
            trade_count += 1
            entry = str(trade.get("entry_time", ""))[11:16]
            exit_time = str(trade.get("exit_time", ""))[11:16]
            lines.append(
                f"| {FRIENDLY_NAMES[strategy]} | {trade.get('symbol', '')} | {trade.get('side', '')} | "
                f"{entry} @ {_number(trade.get('entry_price')):,.2f} | "
                f"{exit_time} {trade.get('exit_reason', '')} | ₹{_number(trade.get('net_pnl_rs')):+,.2f} |"
            )
    if not trade_count:
        lines.append("| — | — | — | — | — | No fills |")

    winner = ranked[0]
    lines.extend([
        "",
        "## Daily conclusion",
        "",
        f"- Highest FnO net P&L: **{FRIENDLY_NAMES[str(winner['strategy'])]} — ₹{_number(winner.get('net_pnl_rs')):+,.2f}**.",
        f"- Source-complete: `{str(complete).lower()}`; incomplete symbol-sessions: `{manifest.get('source_incomplete_symbol_sessions', 0)}`.",
        f"- Cost: `{COST_BPS:g} bps`; slippage: `{SLIPPAGE_BPS:g} bps`; target exposure: `₹{TARGET_EXPOSURE_RS:,.0f}` per entry.",
        "- These are FnO strategy selections replayed with the cash-equity one-minute execution proxy; they are not actual futures fills.",
        "",
        "## Artifacts",
        "",
        f"- Run: `{run_dir}`",
        f"- Comparison: `{run_dir / 'comparison.csv'}`",
        f"- Manifest: `{run_dir / 'manifest.json'}`",
        "",
    ])
    return "\n".join(lines), {"manifest": manifest, "strategies": rows}


def publish(run_dir: Path, dashboard_root: Path) -> dict[str, Path]:
    report, validated = render_report(run_dir)
    day = str(validated["manifest"]["session_date"])
    reports_dir = dashboard_root / "reports"
    latest_dir = dashboard_root / "latest"
    dated_report = reports_dir / f"backtesting_result_v6_v8_v10_v11_v12_{day}.md"
    latest_report = latest_dir / "latest_backtesting_result_v11.md"
    combined_json = latest_dir / "latest_backtesting_result_v6_v8_v10_v11_v12.json"
    payload = {
        "schema_version": PUBLICATION_SCHEMA_VERSION,
        "published_at_ist": common.now_ist(),
        "session_date": day,
        "run_root": run_dir,
        "source_complete": validated["manifest"].get("source_complete"),
        "strategies": validated["strategies"],
        "report": dated_report,
    }
    _atomic_write_text(dated_report, report)
    _atomic_write_text(latest_report, report)
    _atomic_write_json(combined_json, payload)
    return {
        "dated_report": dated_report,
        "latest_report": latest_report,
        "combined_json": combined_json,
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", type=date.fromisoformat, required=True)
    parser.add_argument("--snapshot", type=Path, help="Reuse a completed dated source snapshot.")
    parser.add_argument("--snapshot-root", type=Path, default=DEFAULT_SNAPSHOT_ROOT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_RUN_ROOT)
    parser.add_argument("--dashboard-root", type=Path, default=DEFAULT_DASHBOARD_ROOT)
    parser.add_argument("--publish-only", type=Path, metavar="RUN_DIR")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.publish_only is not None:
            run_dir = args.publish_only.resolve()
        else:
            snapshot = (
                args.snapshot.resolve()
                if args.snapshot is not None
                else create_daily_snapshot(args.date, args.snapshot_root)
            )
            print(f"FNO_SOURCE_SNAPSHOT={snapshot}", flush=True)
            run_dir = run_comparison(
                session=args.date,
                snapshot=snapshot,
                output_root=args.output_root,
            )
        paths = publish(run_dir, args.dashboard_root)
        report = paths["latest_report"].read_text(encoding="utf-8")
        print(report, flush=True)
        print(f"FNO_DASHBOARD_REPORT={paths['latest_report']}", flush=True)
        return 0
    except Exception as exc:
        print(f"FNO_DAILY_COMPARISON_FAILED: {type(exc).__name__}: {exc}", file=sys.stderr, flush=True)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
