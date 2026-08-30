"""Compose validated AUG and retrospective SEP child runs into ten sessions."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import subprocess
import sys
from collections import Counter
from pathlib import Path
from typing import Any, Mapping

import numpy as np
import pandas as pd

import fno_oi_common as common
import fno_oi_backtest_provenance as provenance


EXPECTED_SESSIONS = [
    "2026-08-12",
    "2026-08-13",
    "2026-08-14",
    "2026-08-17",
    "2026-08-18",
    "2026-08-19",
    "2026-08-20",
    "2026-08-21",
    "2026-08-24",
    "2026-08-25",
]
ROOT = (
    common.FNO_ROOT
    / "strategy_research"
    / "v8_v9_last_10_backtests"
    / "rolling_diagnostic"
    / "composite_results"
)
LATEST_REPORT = common.LATEST_DIR / "latest_fno_v8_v9_last_10_rolling_diagnostic.md"
WORKSPACE_ROOT = Path(__file__).resolve().parent
SETUP_BOOK_SHA256 = "ee97e86d3689767df95d1bbe8cb71215cf8a0cb40934f4e08d4c0805d90ee675"
AUG_UNIVERSE = {
    "contract_month_filter": "26AUG",
    "master_date": "2026-08-11",
    "file_sha256": "24170f39c7cf99021553396e40e0d88a435f857364b2423dcfbe9312539dbf09",
    "mapped_universe_sha256": "2cc160189f87bff4eb987a15a4684d95619ee9c810db3cd37276b114ad5824bf",
    "mapped_symbol_set_sha256": "d42f87a9c5fc8ab1710b09b6c4c9832c9d19ecc440ef92b84cad6981499a05a3",
    "mapped_stock_futures": 208,
}
SEP_UNIVERSE = {
    "contract_month_filter": "26SEP",
    "master_date": "2026-08-24",
    "file_sha256": "7444b185bd85f42df68f791228edb5444e9b0e6cfa959722c73c8a0f684e5902",
    "mapped_universe_sha256": "4357d6482c04abd692091d18174ebb269d7d5778a71a74db194ef821a269d7c8",
    "mapped_symbol_set_sha256": "308934dbbb8f1f3c400028def1ea0d617dbc38e9f62b50f96df7d381f93c163a",
    "mapped_stock_futures": 207,
}
VALIDATOR_LAUNCHERS = {
    ("V8_COMBINED", "AUG"): "fno_v8_combined_best_per_leg_backtest.py",
    ("V8_COMBINED", "SEP_ROLLOVER"): "fno_v8_combined_rollover_diagnostic.py",
    ("V9_HONEST", "AUG"): "fno_v9_honest_v8_backtest.py",
    ("V9_HONEST", "SEP_ROLLOVER"): "fno_v9_honest_rollover_diagnostic.py",
}
LINEAGE_FILES = (
    "build_fno_rollover_diagnostic_universe.py",
    "fno_rollover_diagnostic_config.py",
    "fno_v8_combined_rollover_diagnostic.py",
    "fno_v9_honest_rollover_diagnostic.py",
    "compose_fno_v8_v9_last_10_diagnostic.py",
)


def _load_provenance(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict) or "outputs" not in payload:
        raise ValueError(f"Invalid child provenance: {path}")
    return payload


def _validate_child(strategy: str, block: str, path: Path) -> str:
    launcher = WORKSPACE_ROOT / VALIDATOR_LAUNCHERS[(strategy, block)]
    completed = subprocess.run(
        [
            sys.executable,
            str(launcher),
            "validate",
            "--provenance",
            str(path.resolve()),
        ],
        cwd=WORKSPACE_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    output = [line.strip() for line in completed.stdout.splitlines() if line.strip()]
    if not output or len(output[-1]) != 64:
        raise RuntimeError(f"{strategy} {block} validator returned no fingerprint")
    return output[-1]


def _artifact_path(payload: Mapping[str, Any], name: str) -> Path:
    record = dict(dict(payload["outputs"])[name])
    path = Path(str(record["path"]))
    observed = provenance.sha256_file(path)
    if observed != str(record["sha256"]):
        raise AssertionError(f"Child artifact hash mismatch for {name}: {path}")
    return path


def _filled_mask(frame: pd.DataFrame) -> pd.Series:
    values = frame["filled"]
    if pd.api.types.is_bool_dtype(values):
        return values.fillna(False).astype(bool)
    return values.astype(str).str.strip().str.lower().isin({"true", "1"})


def _metrics(audit: pd.DataFrame, daily: pd.DataFrame) -> dict[str, Any]:
    filled = _filled_mask(audit)
    returns = pd.to_numeric(audit["net_return_pct"], errors="coerce")
    pnl = pd.to_numeric(audit["net_pnl_rs"], errors="coerce")
    closed = filled & np.isfinite(returns) & np.isfinite(pnl)
    closed_returns = returns.loc[closed]
    profits = float(closed_returns.loc[closed_returns.gt(0)].sum())
    losses = float(-closed_returns.loc[closed_returns.lt(0)].sum())
    daily_returns = pd.to_numeric(daily["net_return_pct"], errors="raise")
    cumulative = np.concatenate(([0.0], daily_returns.cumsum().to_numpy(float)))
    drawdown = cumulative - np.maximum.accumulate(cumulative)
    status_counts = Counter(audit["status"].astype(str))
    return {
        "sessions": int(len(daily)),
        "candidates": int(len(audit)),
        "fills": int(filled.sum()),
        "closed_fills": int(closed.sum()),
        "wins": int(closed_returns.gt(0).sum()),
        "losses": int(closed_returns.lt(0).sum()),
        "flat_trades": int(closed_returns.eq(0).sum()),
        "profit_factor": (
            profits / losses if losses > 0 else math.inf if profits > 0 else None
        ),
        "net_return_percentage_points": float(closed_returns.sum()),
        "net_pnl_rs": float(pnl.loc[closed].sum()),
        "max_daily_drawdown_percentage_points": max(
            0.0, float(-drawdown.min()) if drawdown.size else 0.0
        ),
        "positive_days": int(daily_returns.gt(0).sum()),
        "negative_days": int(daily_returns.lt(0).sum()),
        "flat_days": int(daily_returns.eq(0).sum()),
        "data_incomplete_candidates": int(
            audit["status"].astype(str).eq("DATA_INCOMPLETE").sum()
        ),
        "status_counts": dict(sorted(status_counts.items())),
    }


def _assert_close(observed: Any, expected: Any, label: str) -> None:
    if not math.isclose(
        float(observed), float(expected), rel_tol=1e-10, abs_tol=1e-9
    ):
        raise AssertionError(f"{label} mismatch: observed={observed}, expected={expected}")


def _reconcile_child(
    strategy: str,
    block: str,
    audit: pd.DataFrame,
    daily: pd.DataFrame,
    results: Mapping[str, Any],
) -> dict[str, Any]:
    audit = audit.copy()
    audit["session_date"] = audit["session_date"].astype(str)
    filled = _filled_mask(audit)
    returns = pd.to_numeric(audit["net_return_pct"], errors="coerce")
    pnl = pd.to_numeric(audit["net_pnl_rs"], errors="coerce")
    closed = filled & np.isfinite(returns) & np.isfinite(pnl)
    unresolved = int((filled & ~closed).sum())
    incomplete = int(audit["status"].astype(str).eq("DATA_INCOMPLETE").sum())
    if unresolved or incomplete:
        raise AssertionError(
            f"{strategy} {block} is not terminally complete: "
            f"unresolved_fills={unresolved}, data_incomplete={incomplete}"
        )
    derived = (
        pd.DataFrame({"session_date": daily["session_date"].astype(str)})
        .merge(
            audit.groupby("session_date", as_index=False).agg(
                candidates=("candidate_id", "size")
            ),
            on="session_date",
            how="left",
        )
        .merge(
            audit.loc[closed]
            .assign(
                net_return_pct=returns.loc[closed],
                net_pnl_rs=pnl.loc[closed],
            )
            .groupby("session_date", as_index=False)
            .agg(
                fills=("candidate_id", "size"),
                net_return_pct=("net_return_pct", "sum"),
                net_pnl_rs=("net_pnl_rs", "sum"),
            ),
            on="session_date",
            how="left",
        )
        .fillna(
            {
                "candidates": 0,
                "fills": 0,
                "net_return_pct": 0.0,
                "net_pnl_rs": 0.0,
            }
        )
    )
    for column in ("candidates", "fills"):
        observed = pd.to_numeric(daily[column], errors="raise").astype(int).tolist()
        expected = pd.to_numeric(derived[column], errors="raise").astype(int).tolist()
        if observed != expected:
            raise AssertionError(f"{strategy} {block} daily {column} mismatch")
    for column in ("net_return_pct", "net_pnl_rs"):
        observed = pd.to_numeric(daily[column], errors="raise").to_numpy(float)
        expected = pd.to_numeric(derived[column], errors="raise").to_numpy(float)
        if not np.allclose(observed, expected, rtol=1e-10, atol=1e-9):
            raise AssertionError(f"{strategy} {block} daily {column} mismatch")

    metrics = _metrics(audit, daily)
    integer_fields = (
        "sessions",
        "candidates",
        "fills",
        "closed_fills",
        "data_incomplete_candidates",
    )
    for field in integer_fields:
        if int(metrics[field]) != int(results[field]):
            raise AssertionError(f"{strategy} {block} result {field} mismatch")
    if int(results.get("unresolved_filled_trades", -1)) != unresolved:
        raise AssertionError(f"{strategy} {block} unresolved-fill summary mismatch")
    if metrics["status_counts"] != dict(results["status_counts"]):
        raise AssertionError(f"{strategy} {block} status counts mismatch")
    diagnostic = dict(results["diagnostic_closed_trade_metrics"])
    for field in (
        "profit_factor",
        "net_return_percentage_points",
        "net_pnl_rs",
        "max_daily_drawdown_percentage_points",
    ):
        _assert_close(metrics[field], diagnostic[field], f"{strategy} {block} {field}")
    return metrics


def _assert_universe(record: Mapping[str, Any], expected: Mapping[str, Any]) -> None:
    for key, value in expected.items():
        if record.get(key) != value:
            raise AssertionError(
                f"Child universe {key} mismatch: observed={record.get(key)}, expected={value}"
            )


def _compose_strategy(
    strategy: str,
    aug_path: Path,
    sep_path: Path,
) -> tuple[dict[str, Any], pd.DataFrame, pd.DataFrame]:
    children: list[dict[str, Any]] = []
    audits: list[pd.DataFrame] = []
    daily_frames: list[pd.DataFrame] = []
    for block, path, expected_dates, contract in (
        ("AUG", aug_path, EXPECTED_SESSIONS[:8], "26AUG"),
        ("SEP_ROLLOVER", sep_path, EXPECTED_SESSIONS[8:], "26SEP"),
    ):
        validation_fingerprint = _validate_child(strategy, block, path)
        payload = _load_provenance(path)
        parameters = dict(payload["parameters"])
        entry_policy = dict(parameters["entry_policy"])
        expected_variant = "VC" if strategy == "V8_COMBINED" else "VH"
        if parameters.get("variant") != expected_variant:
            raise AssertionError(f"{strategy} {block} variant is not {expected_variant}")
        if parameters.get("from_day") != expected_dates[0] or parameters.get(
            "through_day"
        ) != expected_dates[-1]:
            raise AssertionError(f"{strategy} {block} window identity changed")
        if float(entry_policy["cost_bps"]) != 15.0:
            raise AssertionError(f"{strategy} {block} cost is not 15 bps")
        if float(entry_policy["slippage_bps"]) != 0.0:
            raise AssertionError(f"{strategy} {block} slippage is not 0 bps")
        if entry_policy["eod_policy"] != "LAST_REAL_BAR_SENSITIVITY":
            raise AssertionError(f"{strategy} {block} EOD policy changed")
        if entry_policy["square_off"] != "15:30":
            raise AssertionError(f"{strategy} {block} square-off changed")
        daily = pd.read_csv(_artifact_path(payload, "daily"))
        daily["session_date"] = daily["session_date"].astype(str)
        if daily["session_date"].tolist() != expected_dates:
            raise AssertionError(
                f"{strategy} {block} sessions differ: {daily['session_date'].tolist()}"
            )
        audit = pd.read_csv(_artifact_path(payload, "candidate_order_audit"))
        if audit["candidate_id"].duplicated().any():
            raise AssertionError(f"{strategy} {block} has duplicate candidate IDs")
        daily["strategy"] = strategy
        daily["contract_block"] = contract
        daily_frames.append(daily)
        audits.append(audit)
        strategy_payload = dict(payload["strategy_payload"])
        if strategy_payload.get("setup_book_sha256") != SETUP_BOOK_SHA256:
            raise AssertionError(f"{strategy} {block} setup book identity changed")
        has_v9_lineage = "v9_honest_launcher" in strategy_payload
        if has_v9_lineage != (strategy == "V9_HONEST"):
            raise AssertionError(f"{strategy} {block} V9 lineage identity changed")
        expected_universe = AUG_UNIVERSE if block == "AUG" else SEP_UNIVERSE
        _assert_universe(dict(payload["universe"]), expected_universe)
        data_contract = dict(strategy_payload["data_contract"])
        expected_oi_fragment = "26AUG" if block == "AUG" else "26SEP"
        if expected_oi_fragment not in str(data_contract.get("oi_instrument", "")):
            raise AssertionError(f"{strategy} {block} OI instrument identity changed")
        if block == "SEP_ROLLOVER":
            configuration_source = str(strategy_payload.get("configuration_source", ""))
            if "RETROSPECTIVELY_RECONSTRUCTED_ROLLOVER_DIAGNOSTIC" not in configuration_source:
                raise AssertionError(f"{strategy} {block} reconstruction label is absent")
        child_metrics = _reconcile_child(
            strategy, block, audit, daily, dict(payload["results"])
        )
        children.append(
            {
                "block": block,
                "contract_month": contract,
                "provenance_path": str(path.resolve()),
                "provenance_sha256": provenance.sha256_file(path),
                "validator_fingerprint": validation_fingerprint,
                "validator_launcher": VALIDATOR_LAUNCHERS[(strategy, block)],
                "strategy_version": payload["strategy_version"],
                "backtest_input_fingerprint": payload["backtest_input_fingerprint"],
                "source_snapshot": payload["source_snapshot"],
                "universe": payload["universe"],
                "results": payload["results"],
                "reconciled_metrics": child_metrics,
            }
        )
    audit = pd.concat(audits, ignore_index=True, sort=False)
    if audit["candidate_id"].duplicated().any():
        raise AssertionError(f"{strategy} child blocks overlap candidate IDs")
    daily = pd.concat(daily_frames, ignore_index=True, sort=False)
    if daily["session_date"].tolist() != EXPECTED_SESSIONS:
        raise AssertionError(f"{strategy} composite calendar differs")
    metrics = _metrics(audit, daily)
    child_candidate_count = sum(int(child["results"]["candidates"]) for child in children)
    if metrics["candidates"] != child_candidate_count:
        raise AssertionError(f"{strategy} child candidate count does not add up")
    summary = {
        "strategy": strategy,
        "classification": "RETROSPECTIVELY_RECONSTRUCTED_ROLLOVER_DIAGNOSTIC",
        "research_only": True,
        "promotion_eligible": False,
        "headline_valid": False,
        "metrics": metrics,
        "children": children,
    }
    return summary, daily, audit


def _fmt(value: Any, digits: int = 3) -> str:
    if value is None:
        return "n/a"
    if isinstance(value, float) and math.isinf(value):
        return "inf"
    return f"{float(value):.{digits}f}"


ECONOMIC_AUDIT_COLUMNS = (
    "candidate_id",
    "status",
    "reason",
    "filled",
    "entry_time",
    "entry_price",
    "exit_time",
    "exit_price",
    "exit_reason",
    "gross_return_pct",
    "net_return_pct",
    "quantity",
    "gross_pnl_rs",
    "estimated_cost_rs",
    "net_pnl_rs",
    "portfolio_decision",
    "portfolio_reject_reason",
)


def _assert_v8_v9_economics_identical(
    v8_audit: pd.DataFrame, v9_audit: pd.DataFrame
) -> None:
    missing = set(ECONOMIC_AUDIT_COLUMNS) - set(v8_audit.columns) | (
        set(ECONOMIC_AUDIT_COLUMNS) - set(v9_audit.columns)
    )
    if missing:
        raise AssertionError(f"Economic audit comparison columns missing: {sorted(missing)}")
    left = v8_audit.loc[:, ECONOMIC_AUDIT_COLUMNS].sort_values(
        "candidate_id", kind="stable"
    ).reset_index(drop=True)
    right = v9_audit.loc[:, ECONOMIC_AUDIT_COLUMNS].sort_values(
        "candidate_id", kind="stable"
    ).reset_index(drop=True)
    try:
        pd.testing.assert_frame_equal(
            left,
            right,
            check_dtype=False,
            check_exact=False,
            rtol=1e-12,
            atol=1e-12,
        )
    except AssertionError as exc:
        raise AssertionError("V8 and V9 constrained economic audits diverged") from exc


def _archive_lineage(run_dir: Path) -> dict[str, dict[str, Any]]:
    archive_root = run_dir / "lineage"
    archive_root.mkdir(parents=True, exist_ok=False)
    sources: list[Path] = [WORKSPACE_ROOT / name for name in LINEAGE_FILES]
    sources.extend(
        [
            common.FNO_ROOT
            / "strategy_research"
            / "v8_v9_last_10_backtests"
            / "retrospective_rollover_universe"
            / "reconstruction_manifest.json",
            common.LATEST_DIR / "latest_fno_oi_backfill_5min.md",
            common.FNO_ROOT
            / "historical_repair"
            / "equity_1m_backfill.json",
        ]
    )
    archived: dict[str, dict[str, Any]] = {}
    for source in sources:
        if not source.exists():
            raise FileNotFoundError(f"Composite lineage evidence is missing: {source}")
        target = archive_root / source.name
        digest = provenance.sha256_file(source)
        provenance.publish_immutable_copy(source, target, expected_sha256=digest)
        archived[source.name] = {
            "source_path": str(source.resolve()),
            "archive_path": str(target.resolve()),
            "sha256": digest,
            "size": int(target.stat().st_size),
        }
    return dict(sorted(archived.items()))


def _report(payload: Mapping[str, Any], daily: pd.DataFrame) -> str:
    lines = [
        "# FnO V8/V9 Last 10 Sessions - Rolling Diagnostic",
        "",
        f"Generated: `{payload['generated_at_ist']}`",
        "",
        "This is a research-only rollover diagnostic, not a canonical static-26AUG headline backtest. Genuine 26AUG five-minute OI is unavailable for Aug 24-25; those sessions use genuine 26SEP Kite history from a retrospectively reconstructed, hash-pinned universe. DALBHARAT is excluded after rollover because no SEP contract exists.",
        "",
        "Economics: 15 bps cost, 0 bps slippage, 15:30 requested square-off using `LAST_REAL_BAR_SENSITIVITY` (the last broker cash candle is 15:15). Return figures are summed trade return percentage points, not compounded account returns.",
        "",
        "| Strategy | Sessions | Candidates | Fills/closed | Incomplete | Wins | Losses | Net return pts | Net P&L (Rs) | PF | Max daily DD pts |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for key in ("V8_COMBINED", "V9_HONEST"):
        metric = dict(payload["strategies"][key]["metrics"])
        lines.append(
            f"| {key} | {metric['sessions']} | {metric['candidates']} | "
            f"{metric['fills']}/{metric['closed_fills']} | {metric['data_incomplete_candidates']} | "
            f"{metric['wins']} | {metric['losses']} | {_fmt(metric['net_return_percentage_points'])} | "
            f"{_fmt(metric['net_pnl_rs'], 2)} | {_fmt(metric['profit_factor'])} | "
            f"{_fmt(metric['max_daily_drawdown_percentage_points'])} |"
        )
    lines.extend(
        [
            "",
            f"V8/V9 constrained economics identical: `{str(payload['v8_v9_economics_identical']).lower()}`.",
            "",
            "## Daily results",
            "",
            "| Date | Contract | V8 return pts | V8 P&L | V8 fills | V9 return pts | V9 P&L | V9 fills |",
            "|---|---|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for session in EXPECTED_SESSIONS:
        rows = daily.loc[daily["session_date"].eq(session)].set_index("strategy")
        v8 = rows.loc["V8_COMBINED"]
        v9 = rows.loc["V9_HONEST"]
        lines.append(
            f"| {session} | {v8['contract_block']} | {_fmt(v8['net_return_pct'])} | "
            f"{_fmt(v8['net_pnl_rs'], 2)} | {int(v8['fills'])} | "
            f"{_fmt(v9['net_return_pct'])} | {_fmt(v9['net_pnl_rs'], 2)} | {int(v9['fills'])} |"
        )
    lines.extend(
        [
            "",
            "## Validity",
            "",
            "- Canonical static-26AUG last-ten result available: `false`.",
            "- Diagnostic headline valid: `false` (last-real-bar policy and retrospective rollover universe).",
            "- Promotion eligible: `false`.",
            "- Contract schedule: AUG through Aug 21; SEP on Aug 24-25.",
            "- Child run provenance and source snapshots are SHA-256-bound in `summary.json`.",
            "",
            f"Composite fingerprint: `{payload['composite_fingerprint']}`",
            "",
        ]
    )
    return "\n".join(lines)


def compose(args: argparse.Namespace) -> dict[str, Any]:
    v8, v8_daily, v8_audit = _compose_strategy(
        "V8_COMBINED", args.v8_aug, args.v8_sep
    )
    v9, v9_daily, v9_audit = _compose_strategy(
        "V9_HONEST", args.v9_aug, args.v9_sep
    )
    daily = pd.concat([v8_daily, v9_daily], ignore_index=True, sort=False)
    compare_columns = ["session_date", "candidates", "net_return_pct", "net_pnl_rs", "fills"]
    v8_compare = v8_daily[compare_columns].reset_index(drop=True)
    v9_compare = v9_daily[compare_columns].reset_index(drop=True)
    if not v8_compare.equals(v9_compare):
        raise AssertionError("V8 and V9 daily constrained economics diverged")
    _assert_v8_v9_economics_identical(v8_audit, v9_audit)
    economics_identical = True
    generated = common.now_ist()
    stamp = generated.strftime("%Y%m%dT%H%M%S%f%z")
    run_dir = ROOT / f"composite_{stamp}"
    run_dir.mkdir(parents=True, exist_ok=False)
    lineage = _archive_lineage(run_dir)
    daily_path = run_dir / "daily.csv"
    common.atomic_write_csv(daily, daily_path)
    daily_record = {
        "path": str(daily_path.resolve()),
        "sha256": provenance.sha256_file(daily_path),
        "size": int(daily_path.stat().st_size),
    }
    payload: dict[str, Any] = {
        "schema_version": "fno_v8_v9_rolling_diagnostic_composite_v1",
        "generated_at_ist": generated.isoformat(timespec="microseconds"),
        "classification": "RETROSPECTIVELY_RECONSTRUCTED_ROLLOVER_DIAGNOSTIC",
        "research_only": True,
        "promotion_eligible": False,
        "headline_valid": False,
        "canonical_static_26aug_last_ten_available": False,
        "sessions": EXPECTED_SESSIONS,
        "contract_schedule": [
            {"from": "2026-08-12", "through": "2026-08-21", "contract": "26AUG"},
            {"from": "2026-08-24", "through": "2026-08-25", "contract": "26SEP"},
        ],
        "economics": {
            "cost_bps": 15.0,
            "slippage_bps": 0.0,
            "requested_square_off": "15:30",
            "eod_policy": "LAST_REAL_BAR_SENSITIVITY",
            "observed_cash_last_bar": "15:15",
        },
        "strategies": {"V8_COMBINED": v8, "V9_HONEST": v9},
        "v8_v9_economics_identical": economics_identical,
        "lineage_artifacts": lineage,
        "daily_artifact": daily_record,
        "promotion_blockers": [
            "EXPIRED_26AUG_TOKENS_REJECTED_BY_KITE_FOR_AUG24_25",
            "RETROSPECTIVE_SEP_ROLLOVER_UNIVERSE_NOT_POINT_IN_TIME",
            "LAST_REAL_BAR_SENSITIVITY_NOT_HEADLINE",
            "DALBHARAT_EXCLUDED_AFTER_FNO_EXIT",
        ],
        "composer_source_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
    }
    payload["composite_fingerprint"] = common.canonical_json_sha256(payload)
    summary_path = run_dir / "summary.json"
    report_path = run_dir / "report.md"
    common.atomic_write_json(summary_path, payload)
    report = _report(payload, daily)
    common.atomic_write_text(report_path, report)
    artifact_manifest: dict[str, Any] = {
        "schema_version": "fno_v8_v9_rolling_diagnostic_artifact_manifest_v1",
        "composite_fingerprint": payload["composite_fingerprint"],
        "artifacts": {
            "summary": {
                "path": str(summary_path.resolve()),
                "sha256": provenance.sha256_file(summary_path),
                "size": int(summary_path.stat().st_size),
            },
            "daily": daily_record,
            "report": {
                "path": str(report_path.resolve()),
                "sha256": provenance.sha256_file(report_path),
                "size": int(report_path.stat().st_size),
            },
        },
    }
    artifact_manifest["manifest_fingerprint"] = common.canonical_json_sha256(
        artifact_manifest
    )
    common.atomic_write_json(run_dir / "manifest.json", artifact_manifest)
    common.atomic_write_text(LATEST_REPORT, report)
    print(str(summary_path), flush=True)
    print(str(report_path), flush=True)
    return payload


def validate_composite(path: Path) -> str:
    run_dir = path.resolve() if path.is_dir() else path.resolve().parent
    manifest_path = run_dir / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    fingerprint = str(manifest.pop("manifest_fingerprint", ""))
    if common.canonical_json_sha256(manifest) != fingerprint:
        raise AssertionError("Composite artifact-manifest fingerprint is invalid")
    for name, record in dict(manifest["artifacts"]).items():
        artifact = Path(str(record["path"])).resolve()
        if artifact.parent != run_dir:
            raise ValueError(f"Composite {name} artifact escaped the run directory")
        if provenance.sha256_file(artifact) != str(record["sha256"]):
            raise AssertionError(f"Composite {name} artifact hash changed")
        if int(artifact.stat().st_size) != int(record["size"]):
            raise AssertionError(f"Composite {name} artifact size changed")

    summary_path = Path(str(manifest["artifacts"]["summary"]["path"]))
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    composite_fingerprint = str(summary.pop("composite_fingerprint", ""))
    if common.canonical_json_sha256(summary) != composite_fingerprint:
        raise AssertionError("Composite summary fingerprint is invalid")
    if composite_fingerprint != str(manifest["composite_fingerprint"]):
        raise AssertionError("Composite summary and artifact manifest are unbound")
    daily_manifest = dict(manifest["artifacts"]["daily"])
    if dict(summary["daily_artifact"]) != daily_manifest:
        raise AssertionError("Composite daily artifact record is inconsistent")
    for name, record in dict(summary["lineage_artifacts"]).items():
        archive = Path(str(record["archive_path"])).resolve()
        if archive.parent != run_dir / "lineage" or archive.name != name:
            raise ValueError(f"Composite lineage path escaped the archive: {name}")
        if provenance.sha256_file(archive) != str(record["sha256"]):
            raise AssertionError(f"Composite lineage artifact changed: {name}")
    for strategy, strategy_record in dict(summary["strategies"]).items():
        for child in strategy_record["children"]:
            child_path = Path(str(child["provenance_path"]))
            if provenance.sha256_file(child_path) != str(child["provenance_sha256"]):
                raise AssertionError(f"Composite child provenance changed: {strategy}")
            observed = _validate_child(strategy, str(child["block"]), child_path)
            if observed != str(child["validator_fingerprint"]):
                raise AssertionError(f"Composite child validator changed: {strategy}")
    return fingerprint


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--v8-aug", type=Path)
    parser.add_argument("--v8-sep", type=Path)
    parser.add_argument("--v9-aug", type=Path)
    parser.add_argument("--v9-sep", type=Path)
    parser.add_argument("--validate-composite", type=Path)
    args = parser.parse_args()
    if args.validate_composite is not None:
        if any((args.v8_aug, args.v8_sep, args.v9_aug, args.v9_sep)):
            parser.error("--validate-composite cannot be combined with child inputs")
        print(validate_composite(args.validate_composite), flush=True)
        return 0
    if not all((args.v8_aug, args.v8_sep, args.v9_aug, args.v9_sep)):
        parser.error("all four child provenance paths are required")
    compose(args)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
