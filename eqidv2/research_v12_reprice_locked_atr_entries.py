"""Reprice immutable locked ATR raw entries with the current V12 mechanics.

This is a research-only repair utility.  It intentionally does *not* rebuild
hourly memberships, five-minute features, signals, candidates, or V12 entry
timestamps.  Its only input is an existing
``entry_engine_raw_entries.csv`` produced by the locked ATR-impulse runner.

The utility verifies the source artifact (including its sibling integrity
manifest when present), restores the entry-engine placeholder fields if the
source was already guard-annotated, and then reapplies the current
``research_v12_hourly_two_bar_long_backtest`` execution guards, V12's
one-ticker-per-day selector, and the current primary structural exit policy.

Discovery remains frozen at 2026-06-05..2026-08-04.  No result from this tool
can approve or modify a production setup.
"""

from __future__ import annotations

import argparse
import json
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator, Sequence

import numpy as np
import pandas as pd

import research_v12_hourly_atr_impulse_long_backtest as locked
import research_v12_hourly_two_bar_long_backtest as base
import research_v12_prefilter_train_test_optimizer as optimizer


SETUP = locked.SETUP
PRODUCTION_APPROVED = False
PREWARM_WORKERS = 8

REQUIRED_COLUMNS = frozenset(
    {
        "_optimizer_row_id",
        "ticker",
        "setup",
        "side",
        "trade_date",
        "slot_ist",
        "selection_rank",
        "context_score",
        "signal_time_ist",
        "bar_time_ist",
        "v7_signal_entry_time_ist",
        "v7_signal_entry_price",
        "v7_signal_stop_price",
        "v7_signal_target_price",
        "v7_signal_sl_pct",
        "v7_signal_target_pct",
        "v7_signal_notional_rs",
        "quantity",
        "score",
        "signal_low",
        "signal_high",
        "signal_close",
        "signal_volume",
        "signal_atr",
        "range_atr",
        "previous_return_5m_close_pct",
        "return_5m_close_pct",
        "impulse_atr_ratio",
        "vwap_dist_atr",
        "traded_value_rs",
        "preregistered_signal",
    }
)

# A current guard-annotated source preserves the actual entry-engine values in
# these audit columns.  Restoring them makes rerunning this utility idempotent
# and prevents entry slippage from being applied twice.
PLACEHOLDER_RECOVERY = {
    "entry_engine_raw_entry_price": "v7_signal_entry_price",
    "entry_engine_placeholder_stop_price": "v7_signal_stop_price",
    "entry_engine_placeholder_target_price": "v7_signal_target_price",
    "entry_engine_placeholder_sl_pct": "v7_signal_sl_pct",
    "entry_engine_placeholder_target_pct": "v7_signal_target_pct",
    "entry_engine_placeholder_quantity": "quantity",
    "entry_engine_placeholder_notional_rs": "v7_signal_notional_rs",
}

OUTPUT_FILES = {
    "raw": "repriced_raw_entries.csv",
    "selected": "repriced_selected_entries.csv",
    "trades": "repriced_trades.csv",
    "summary": "repricing_summary.json",
    "manifest": "integrity_manifest.json",
}


def _date(value: object) -> pd.Timestamp:
    return pd.Timestamp(value).tz_localize(None).normalize()


def _parse_ist(values: pd.Series, label: str) -> pd.Series:
    parsed = pd.to_datetime(values, errors="coerce", utc=True)
    if parsed.isna().any():
        rows = values.index[parsed.isna()].tolist()[:10]
        raise ValueError(f"invalid {label} timestamps at rows {rows}")
    return parsed.dt.tz_convert(base.IST)


def _as_bool(values: pd.Series) -> pd.Series:
    if pd.api.types.is_bool_dtype(values):
        return values.fillna(False).astype(bool)
    normalised = values.astype("string").str.strip().str.lower()
    invalid = ~normalised.isin(["true", "false", "1", "0"])
    if invalid.any():
        raise ValueError(
            "invalid boolean values: "
            + ", ".join(sorted(normalised.loc[invalid].dropna().unique())[:10])
        )
    return normalised.isin(["true", "1"])


def _manifest_artifact_hash(manifest: dict[str, Any], filename: str) -> str | None:
    for artifact in manifest.get("artifacts", []):
        if str(artifact.get("file", "")) == filename:
            value = str(artifact.get("sha256", "")).strip().lower()
            return value or None
    return None


def source_provenance(raw_path: Path) -> dict[str, Any]:
    """Verify and describe the immutable source artifact and its siblings."""

    source_hash = base._sha256(raw_path)
    parent = raw_path.parent
    manifest_path = parent / "integrity_manifest.json"
    summary_path = parent / "summary.json"
    manifest: dict[str, Any] | None = None
    declared_hash: str | None = None
    verification = "sibling_manifest_not_present"

    if manifest_path.is_file():
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        declared_hash = _manifest_artifact_hash(manifest, raw_path.name)
        if declared_hash is None:
            raise ValueError(
                f"source manifest does not list {raw_path.name}: {manifest_path}"
            )
        if declared_hash != source_hash:
            raise ValueError(
                "source raw-entry hash differs from sibling integrity manifest: "
                f"declared={declared_hash} actual={source_hash}"
            )
        verification = "verified_against_sibling_integrity_manifest"

        if summary_path.is_file():
            declared_summary = _manifest_artifact_hash(manifest, summary_path.name)
            if declared_summary is not None:
                actual_summary = base._sha256(summary_path)
                if declared_summary != actual_summary:
                    raise ValueError(
                        "source summary hash differs from sibling integrity manifest"
                    )

    result: dict[str, Any] = {
        "path": str(raw_path.resolve()),
        "bytes": raw_path.stat().st_size,
        "sha256": source_hash,
        "verification": verification,
        "declared_sha256": declared_hash,
        "sibling_integrity_manifest": (
            str(manifest_path.resolve()) if manifest_path.is_file() else None
        ),
        "sibling_integrity_manifest_sha256": (
            base._sha256(manifest_path) if manifest_path.is_file() else None
        ),
        "sibling_summary": str(summary_path.resolve()) if summary_path.is_file() else None,
        "sibling_summary_sha256": (
            base._sha256(summary_path) if summary_path.is_file() else None
        ),
        "source_runner_inputs": manifest.get("inputs", {}) if manifest else {},
    }
    return result


def validate_locked_raw_entries(frame: pd.DataFrame) -> None:
    """Reject inputs that cannot be proven to be locked ATR raw candidates."""

    missing = sorted(REQUIRED_COLUMNS - set(frame.columns))
    if missing:
        raise ValueError("raw-entry artifact missing required columns: " + ", ".join(missing))
    if frame.empty:
        raise ValueError("raw-entry artifact is empty")

    setups = set(frame["setup"].astype(str).str.strip())
    if setups != {SETUP}:
        raise ValueError(f"unexpected setup values {sorted(setups)}; required {SETUP}")
    sides = set(frame["side"].astype(str).str.upper().str.strip())
    if sides != {"LONG"}:
        raise ValueError(f"unexpected side values {sorted(sides)}; required LONG")
    if frame["_optimizer_row_id"].duplicated().any():
        raise ValueError("duplicate _optimizer_row_id in raw-entry artifact")

    signal_time = _parse_ist(frame["signal_time_ist"], "signal_time_ist")
    bar_time = _parse_ist(frame["bar_time_ist"], "bar_time_ist")
    entry_time = _parse_ist(
        frame["v7_signal_entry_time_ist"], "v7_signal_entry_time_ist"
    )
    if not signal_time.eq(bar_time).all():
        raise ValueError("bar_time_ist must equal the immutable signal_time_ist")
    delay = (entry_time - signal_time).dt.total_seconds() / 60.0
    if not (
        delay.gt(0.0)
        & delay.le(float(base.v12.V7_ENTRY_SEARCH_MAX_DELAY_MIN))
    ).all():
        raise ValueError("entry timestamps violate the locked causal V12 delay window")
    declared_day = frame["trade_date"].astype(str).str.slice(0, 10)
    if not declared_day.eq(signal_time.dt.strftime("%Y-%m-%d")).all():
        raise ValueError("trade_date differs from signal_time_ist calendar date")

    numeric_columns = [
        "v7_signal_entry_price",
        "signal_low",
        "signal_high",
        "signal_close",
        "signal_volume",
        "signal_atr",
        "range_atr",
        "previous_return_5m_close_pct",
        "return_5m_close_pct",
        "impulse_atr_ratio",
        "vwap_dist_atr",
        "traded_value_rs",
    ]
    numeric = frame[numeric_columns].apply(pd.to_numeric, errors="coerce")
    if numeric.isna().any().any() or not np.isfinite(numeric.to_numpy()).all():
        bad = numeric.columns[numeric.isna().any()].tolist()
        raise ValueError(f"non-finite locked raw-entry values in columns: {bad}")

    checks = {
        "preregistered_signal": _as_bool(frame["preregistered_signal"]),
        "previous_return": numeric["previous_return_5m_close_pct"].between(
            locked.RETURN_MIN_PCT, locked.RETURN_MAX_PCT, inclusive="both"
        ),
        "current_return": numeric["return_5m_close_pct"].between(
            locked.RETURN_MIN_PCT, locked.RETURN_MAX_PCT, inclusive="both"
        ),
        "impulse_atr_ratio": numeric["impulse_atr_ratio"].ge(
            locked.MIN_IMPULSE_ATR_RATIO
        ),
        "vwap_distance": numeric["vwap_dist_atr"].ge(locked.MIN_VWAP_DISTANCE_ATR),
        "traded_value": numeric["traded_value_rs"].ge(locked.MIN_TRADED_VALUE_RS),
        "signal_price": numeric["signal_close"].ge(locked.MIN_SIGNAL_PRICE_RS),
        "positive_atr": numeric["signal_atr"].gt(0.0),
        "range_atr": numeric["range_atr"].le(locked.MAX_SIGNAL_RANGE_ATR),
        "positive_entry": numeric["v7_signal_entry_price"].gt(0.0),
    }
    failed = {name: int((~passed).sum()) for name, passed in checks.items() if not passed.all()}
    if failed:
        raise ValueError(f"raw entries violate the locked ATR signal/entry contract: {failed}")


def filter_requested_window(
    frame: pd.DataFrame, start_date: str, end_date: str
) -> pd.DataFrame:
    signal_time = _parse_ist(frame["signal_time_ist"], "signal_time_ist")
    days = signal_time.dt.tz_localize(None).dt.normalize()
    keep = days.between(_date(start_date), _date(end_date), inclusive="both")
    out = frame.loc[keep].copy().reset_index(drop=True)
    if out.empty:
        raise ValueError("raw-entry artifact has no rows in the requested date window")
    return out


def restore_entry_engine_fields(frame: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    """Return an in-memory raw view suitable for the current guard function."""

    work = frame.copy()
    recovered: list[str] = []
    for audit_column, public_column in PLACEHOLDER_RECOVERY.items():
        if audit_column in work.columns:
            restored = pd.to_numeric(work[audit_column], errors="coerce")
            if restored.isna().any():
                raise ValueError(f"invalid preserved entry-engine field: {audit_column}")
            work[public_column] = restored
            recovered.append(public_column)
    return work, recovered


def _source_sessions(
    raw_path: Path,
    frame: pd.DataFrame,
    start_date: str,
    end_date: str,
) -> tuple[list[str], str]:
    summary_path = raw_path.parent / "summary.json"
    if summary_path.is_file():
        payload = json.loads(summary_path.read_text(encoding="utf-8"))
        rows = payload.get("membership_audit", {}).get("session_rows", [])
        sessions = sorted(
            {
                str(row.get("trade_date", ""))[:10]
                for row in rows
                if isinstance(row, dict)
                and start_date <= str(row.get("trade_date", ""))[:10] <= end_date
            }
        )
        if sessions:
            return sessions, "verified_source_summary_membership_sessions"
    sessions = sorted(set(frame["trade_date"].astype(str).str.slice(0, 10)))
    return sessions, "raw_candidate_sessions_only_fallback"


@contextmanager
def installed_one_minute_runtime(
    one_minute_dir: Path,
    start_date: str,
    end_date: str,
    tickers: pd.Series,
) -> Iterator[dict[str, int]]:
    """Install, prewarm, and later restore the process-local V12 1m adapters."""

    sentinel = object()
    attribute_names = [
        "_load_1m_with_open",
        "_optimizer_load_1m_day",
        "_entry_bars_for_signal",
        "_V11_EXACT_LIVE_PARITY",
        "_V11_COST_MODEL",
        "_V11_SLIPPAGE_BPS",
    ]
    saved = {name: getattr(base.v12, name, sentinel) for name in attribute_names}
    previous_dir = base.v12.v6.DATA_1M_DIR
    previous_setup = base.SETUP
    base.v12.v6.DATA_1M_DIR = one_minute_dir
    base.SETUP = SETUP
    try:
        loader = optimizer.install_windowed_1m_loader(
            base.v12, start_date=start_date, end_date=end_date
        )
        prewarm = optimizer.prewarm_windowed_1m_loader(
            loader, tickers, workers=PREWARM_WORKERS
        )
        if int(prewarm.get("missing", 0)) or int(prewarm.get("failed", 0)):
            raise RuntimeError(f"incomplete one-minute prewarm: {prewarm}")
        optimizer.install_day_1m_adapter(base.v12, loader)
        base.v12._V11_EXACT_LIVE_PARITY = False
        base.v12._V11_COST_MODEL = "statutory"
        base.v12._V11_SLIPPAGE_BPS = 0.0
        yield prewarm
    finally:
        base.SETUP = previous_setup
        base.v12.v6.DATA_1M_DIR = previous_dir
        for name, value in saved.items():
            if value is sentinel:
                if hasattr(base.v12, name):
                    delattr(base.v12, name)
            else:
                setattr(base.v12, name, value)


def _validate_args(args: argparse.Namespace) -> None:
    if _date(args.start_date) > _date(args.end_date):
        raise ValueError("--start-date must not be after --end-date")
    if not args.raw_entries.is_file():
        raise FileNotFoundError(f"raw entries not found: {args.raw_entries}")
    if not args.one_minute_dir.is_dir():
        raise FileNotFoundError(f"one-minute directory not found: {args.one_minute_dir}")
    if args.out.resolve() == args.raw_entries.parent.resolve():
        raise ValueError("--out must differ from the immutable source artifact directory")


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Research-only repricing of immutable locked ATR V12 raw entries"
    )
    parser.add_argument("--raw-entries", type=Path, required=True)
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--one-minute-dir", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    return parser.parse_args(argv)


def _guard_change_count(source: pd.DataFrame, repriced: pd.DataFrame) -> int | None:
    if "execution_guard_pass" not in source.columns:
        return None
    old = _as_bool(source["execution_guard_pass"])
    new = _as_bool(repriced["execution_guard_pass"])
    return int(old.ne(new).sum())


def run(args: argparse.Namespace) -> dict[str, Any]:
    _validate_args(args)
    started = time.time()
    source = source_provenance(args.raw_entries)
    source_hash_before = source["sha256"]
    all_rows = pd.read_csv(args.raw_entries, low_memory=False)
    validate_locked_raw_entries(all_rows)
    window_rows = filter_requested_window(all_rows, args.start_date, args.end_date)
    raw_for_guards, recovered_fields = restore_entry_engine_fields(window_rows)
    sessions, session_source = _source_sessions(
        args.raw_entries, window_rows, args.start_date, args.end_date
    )

    with installed_one_minute_runtime(
        args.one_minute_dir,
        args.start_date,
        args.end_date,
        raw_for_guards["ticker"],
    ) as prewarm:
        repriced = base.add_execution_guards(raw_for_guards)
        executable_mask = _as_bool(repriced["execution_guard_pass"])
        executable = repriced.loc[executable_mask].copy()
        selected = (
            base.v12._select_v7_entry_engine_signals(executable)
            if not executable.empty
            else pd.DataFrame(columns=repriced.columns)
        )
        trades = (
            base.resolve_policy(selected, base.PRIMARY_POLICY, SETUP)
            if not selected.empty
            else pd.DataFrame()
        )

    if len(trades) != len(selected):
        raise RuntimeError(
            "one-minute exit resolution was incomplete: "
            f"selected={len(selected)} resolved={len(trades)}"
        )

    repriced["research_window"] = locked.label_trade_windows(repriced["trade_date"])
    if not selected.empty:
        selected["research_window"] = locked.label_trade_windows(selected["trade_date"])
    if not trades.empty:
        trades["research_window"] = locked.label_trade_windows(trades["trade_date"])

    window_results, _ = locked.summarize_windows(trades, sessions)
    pre_sessions = locked._window_sessions(sessions, "backward_pre_discovery")
    pre_trades = (
        trades.loc[trades["research_window"].eq("backward_pre_discovery")].copy()
        if not trades.empty
        else trades.copy()
    )
    validation = locked.evaluate_backward_validation_gates(pre_trades, pre_sessions)

    source_guard_pass = (
        int(_as_bool(window_rows["execution_guard_pass"]).sum())
        if "execution_guard_pass" in window_rows.columns
        else None
    )
    source_hash_after = base._sha256(args.raw_entries)
    if source_hash_after != source_hash_before:
        raise RuntimeError("immutable raw-entry source changed during repricing")

    summary: dict[str, Any] = {
        "setup": SETUP,
        "research_only": True,
        "production_approved": False,
        "promotion_action": "NONE_RESEARCH_ONLY",
        "operation": "execution_guard_and_exit_repricing_only",
        "signal_regeneration_performed": False,
        "entry_engine_regeneration_performed": False,
        "source_raw_candidates_immutable": True,
        "source_artifact": {
            **source,
            "sha256_after_run": source_hash_after,
            "all_rows": int(len(all_rows)),
            "requested_window_rows": int(len(window_rows)),
        },
        "requested_window": {
            "start": args.start_date,
            "end": args.end_date,
            "sessions": len(sessions),
            "session_source": session_source,
        },
        "discovery_window": {
            "start": locked.DISCOVERY_START,
            "end": locked.DISCOVERY_END,
            "locked": True,
            "eligible_for_backward_validation": False,
        },
        "current_execution": {
            "execution_guard_source": str(Path(base.__file__).resolve()),
            "execution_guard_source_sha256": base._sha256(Path(base.__file__)),
            "locked_runner_source": str(Path(locked.__file__).resolve()),
            "locked_runner_source_sha256": base._sha256(Path(locked.__file__)),
            "restored_entry_engine_fields": recovered_fields,
            "source_guard_pass": source_guard_pass,
            "repriced_guard_pass": int(executable_mask.sum()),
            "guard_decisions_changed": _guard_change_count(window_rows, repriced),
            "selected_one_ticker_per_day": int(len(selected)),
            "resolved_trades": int(len(trades)),
            "prewarm": prewarm,
            "selector": "V12 _select_v7_entry_engine_signals",
            "exit_policy": base.PRIMARY_POLICY.name,
            "target_r": base.PRIMARY_POLICY.target_r,
            "conditional_time_stop": base.PRIMARY_POLICY.conditional_time_stop,
            "two_bar_low_trail": base.PRIMARY_POLICY.two_bar_low_trail,
        },
        "window_results": window_results,
        "backward_validation_gates": validation,
        "runtime_seconds": time.time() - started,
        "limitations": [
            "the raw signal and entry candidates are reused, not regenerated",
            "the discovery thresholds remain in-sample for 2026-06-05..2026-08-04",
            "repricing corrects execution mechanics but cannot remove universe survivorship bias",
            "historical quoted spreads remain unavailable",
            "production approval is hard-coded false regardless of profit factor",
        ],
    }

    args.out.mkdir(parents=True, exist_ok=True)
    repriced.to_csv(args.out / OUTPUT_FILES["raw"], index=False)
    selected.to_csv(args.out / OUTPUT_FILES["selected"], index=False)
    trades.to_csv(args.out / OUTPUT_FILES["trades"], index=False)
    summary_path = args.out / OUTPUT_FILES["summary"]
    summary_path.write_text(
        json.dumps(base._json_value(summary), indent=2), encoding="utf-8"
    )

    artifact_paths = [
        args.out / OUTPUT_FILES["raw"],
        args.out / OUTPUT_FILES["selected"],
        args.out / OUTPUT_FILES["trades"],
        summary_path,
    ]
    manifest = {
        "setup": SETUP,
        "research_only": True,
        "production_approved": False,
        "promotion_action": "NONE_RESEARCH_ONLY",
        "source_artifact": source,
        "one_minute_dir": str(args.one_minute_dir.resolve()),
        "repricer_source": str(Path(__file__).resolve()),
        "repricer_source_sha256": base._sha256(Path(__file__)),
        "current_execution_source": str(Path(base.__file__).resolve()),
        "current_execution_source_sha256": base._sha256(Path(base.__file__)),
        "locked_runner_source": str(Path(locked.__file__).resolve()),
        "locked_runner_source_sha256": base._sha256(Path(locked.__file__)),
        "artifacts": [
            {
                "file": path.name,
                "bytes": path.stat().st_size,
                "sha256": base._sha256(path),
            }
            for path in artifact_paths
        ],
    }
    (args.out / OUTPUT_FILES["manifest"]).write_text(
        json.dumps(base._json_value(manifest), indent=2), encoding="utf-8"
    )
    return summary


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    summary = run(args)
    print(json.dumps(base._json_value(summary), indent=2), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
