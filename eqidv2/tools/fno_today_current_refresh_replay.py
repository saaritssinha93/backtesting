#!/usr/bin/env python3
"""Refresh a frozen intraday FnO replay to one common completed 1-minute cutoff.

This research-only utility preserves the 2026-08-31 frozen candidate frames and
five-minute features.  It replaces only the same-session cash-equity execution
paths for the symbols used by the five requested strategies, then reruns their
unchanged causal entry/exit engines at the reference economics.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any, Mapping

import numpy as np
import pandas as pd

TOOLS_ROOT = Path(__file__).resolve().parent
WORKSPACE_ROOT = TOOLS_ROOT.parent
if str(WORKSPACE_ROOT) not in sys.path:
    sys.path.insert(0, str(WORKSPACE_ROOT))

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
from tools import fno_today_six_strategy_replay as today
from tools import fno_v6_isolated_challenger_replay as v6


SCHEMA_VERSION = "fno_today_current_refresh_replay_v1"
SESSION = date(2026, 8, 31)
SYMBOLS = ("MCX", "KAYNES", "HINDPETRO", "PRESTIGE", "KFINTECH")
SOURCE_ROOT = Path(r"C:\TradingData\eqidv2\stocks_indicators_1min_eq")
BASE_RUN = Path(
    r"C:\TradingData\eqidv2\fno_oi\strategy_research\today_six_strategy_replays_v1"
    r"\today_2026-08-31_20260831T111230099112+0530"
)
SNAPSHOT = Path(
    r"C:\TradingData\eqidv2\fno_oi\strategy_research\fno_today_20260831"
    r"\snapshot_current_20260831T1106"
    r"\snapshot_20260831T111106593703+0530_913qyxge\manifest.json"
)
V10_CACHE = BASE_RUN / "v10_cache" / "f0d6e3bf13dbeb84"
V6_CACHE = BASE_RUN / "v6_cache" / "c822a5dfab09f13b"
PRIOR_1144 = Path(
    r"C:\TradingData\eqidv2\fno_oi\strategy_research"
    r"\today_v10_v11_refreshed_paths_1144_v1"
    r"\today_2026-08-31_20260831T114651116297+0530"
)
PRIOR_V12_1144 = Path(
    r"C:\TradingData\eqidv2\fno_oi\strategy_research"
    r"\today_v12_selected_refreshed_paths_1144_v1"
    r"\today_2026-08-31_cutoff_1144_20260831T114718683047+0530"
)
DEFAULT_OUTPUT_ROOT = Path(
    r"C:\TradingData\eqidv2\fno_oi\strategy_research"
    r"\today_five_strategy_current_refresh_v1"
)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _json_ready(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_ready(item) for item in value]
    if isinstance(value, Path):
        return str(value.resolve())
    if isinstance(value, (datetime, date, pd.Timestamp)):
        return value.isoformat()
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        value = float(value)
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if value is pd.NA:
        return None
    return value


def _to_ist(values: pd.Series) -> pd.Series:
    normalized: list[pd.Timestamp] = []
    for value in values:
        try:
            stamp = pd.Timestamp(value)
            if stamp.tzinfo is None:
                stamp = stamp.tz_localize(common.IST)
            else:
                stamp = stamp.tz_convert(common.IST)
            normalized.append(stamp)
        except (TypeError, ValueError):
            normalized.append(pd.NaT)
    return pd.Series(pd.DatetimeIndex(normalized), index=values.index, name=values.name)


def _source_frames() -> tuple[dict[str, pd.DataFrame], pd.Timestamp, dict[str, Any]]:
    frames: dict[str, pd.DataFrame] = {}
    maxima: dict[str, pd.Timestamp] = {}
    evidence: dict[str, Any] = {}
    for symbol in SYMBOLS:
        path = SOURCE_ROOT / f"{symbol}_stocks_indicators_1min.parquet"
        frame = pd.read_parquet(path)
        frame = frame.copy()
        frame["date"] = _to_ist(frame["date"])
        frame = frame.loc[frame["date"].dt.date.eq(SESSION)].copy()
        frame = frame.drop_duplicates("date", keep="last").sort_values("date")
        if frame.empty:
            raise RuntimeError(f"No current-session 1-minute source for {symbol}")
        frames[symbol] = frame
        maxima[symbol] = frame["date"].max()
        evidence[symbol] = {
            "path": path,
            "sha256": _sha256(path),
            "rows_today": len(frame),
            "observed_max_ist": maxima[symbol],
        }
    cutoff = min(maxima.values())
    expected = pd.date_range(
        pd.Timestamp(SESSION, tz=common.IST) + pd.Timedelta(hours=9, minutes=16),
        cutoff,
        freq="1min",
    )
    for symbol, frame in frames.items():
        used = frame.loc[frame["date"].le(cutoff)]
        if not pd.DatetimeIndex(used["date"]).equals(expected):
            missing = expected.difference(pd.DatetimeIndex(used["date"]))
            raise AssertionError(f"{symbol} is not continuous through cutoff; missing={list(missing[:5])}")
        evidence[symbol]["used_rows"] = len(used)
        evidence[symbol]["used_max_ist"] = used["date"].max()
    return frames, cutoff, evidence


def _refresh_paths(
    candidates: pd.DataFrame,
    cached_paths: pd.DataFrame,
    frames: Mapping[str, pd.DataFrame],
    cutoff: pd.Timestamp,
    symbols: set[str],
) -> pd.DataFrame:
    candidate_ids = set(
        candidates.loc[candidates["symbol"].astype(str).isin(symbols), "candidate_id"].astype(str)
    )
    retained = cached_paths.loc[
        ~cached_paths["candidate_id"].astype(str).isin(candidate_ids)
    ].copy()
    columns = list(cached_paths.columns)
    parts = [retained]
    for row in candidates.loc[candidates["candidate_id"].astype(str).isin(candidate_ids)].itertuples(index=False):
        signal = pd.Timestamp(row.signal_time)
        if signal.tzinfo is None:
            signal = signal.tz_localize(common.IST)
        else:
            signal = signal.tz_convert(common.IST)
        source = frames[str(row.symbol)]
        selected = source.loc[source["date"].gt(signal) & source["date"].le(cutoff)].copy()
        if selected.empty or selected["date"].max() != cutoff:
            raise AssertionError(f"Refreshed path does not reach cutoff: {row.candidate_id}")
        path = pd.DataFrame(
            {
                "candidate_id": str(row.candidate_id),
                "session_date": SESSION,
                "signal_time": signal,
                "setup_id": str(row.setup_id),
                "side": str(row.side),
                "symbol": str(row.symbol),
                "bar_ts": selected["date"].to_numpy(),
                "minute_index": np.arange(1, len(selected) + 1, dtype=int),
                "open": pd.to_numeric(selected["open"], errors="coerce").to_numpy(),
                "high": pd.to_numeric(selected["high"], errors="coerce").to_numpy(),
                "low": pd.to_numeric(selected["low"], errors="coerce").to_numpy(),
                "close": pd.to_numeric(selected["close"], errors="coerce").to_numpy(),
                "volume": pd.to_numeric(selected["volume"], errors="coerce").fillna(0).to_numpy(),
                "gap_filled": False,
                "opening_snapshot": False,
                "provisional_stale": False,
                "legacy_lineage_flags_absent": True,
                "path_policy_version": cached_paths["path_policy_version"].iloc[0],
            }
        )
        parts.append(path[columns])
    combined = pd.concat(parts, ignore_index=True)
    combined["bar_ts"] = _to_ist(combined["bar_ts"])
    combined = combined.sort_values(["candidate_id", "bar_ts"], kind="stable").reset_index(drop=True)
    if combined.duplicated(["candidate_id", "bar_ts"]).any():
        duplicates = combined.loc[
            combined.duplicated(["candidate_id", "bar_ts"], keep=False),
            ["candidate_id", "bar_ts", "minute_index"],
        ]
        raise AssertionError(
            "Duplicate candidate/minute paths after refresh: "
            + duplicates.head(12).to_json(orient="records", date_format="iso")
        )
    return combined


def _metric(audit: pd.DataFrame, strategy: str) -> dict[str, Any]:
    row = today.metric_row(
        audit,
        strategy=strategy,
        session=SESSION,
        source_complete=False,
        incomplete_symbol_sessions=210,
    )
    row["explicit_uniform_cutoff_ist"] = CUTOFF
    row["cost_bps"] = 15.0
    row["slippage_bps"] = 0.0
    row["target_exposure_per_entry_rs"] = 50_000.0
    return row


def _write_strategy(root: Path, strategy: str, audit: pd.DataFrame, decisions: pd.DataFrame) -> dict[str, Any]:
    directory = root / "strategies" / strategy.lower()
    directory.mkdir(parents=True, exist_ok=False)
    metric = _metric(audit, strategy)
    filled = today._bool_series(audit["filled"])
    paths = {
        "audit": directory / "candidate_order_audit.csv",
        "decisions": directory / "selection_decisions.csv",
        "closed": directory / "closed_trades.csv",
        "summary": directory / "summary.json",
    }
    common.atomic_write_csv(audit, paths["audit"])
    common.atomic_write_csv(decisions, paths["decisions"])
    common.atomic_write_csv(audit.loc[filled].copy(), paths["closed"])
    common.atomic_write_json(paths["summary"], _json_ready(metric))
    return {"metric": metric, "paths": {key: str(path.resolve()) for key, path in paths.items()}}


def _configure_v10() -> Any:
    experiment.configure_engine(locked_config.ACTIVE_VARIANT)
    repaired_v10.bind_engine_universe(SNAPSHOT)
    engine._confirmation_check = experiment._NEUTRAL_CONFIRMATION_CHECK
    return repaired_v10._reference_policy(15.0, 0.0)


def _run_v8_v12(root: Path, candidates: pd.DataFrame, paths: pd.DataFrame) -> list[dict[str, Any]]:
    outputs: list[dict[str, Any]] = []

    policy = _configure_v10()
    decisions = candidates[[
        column for column in (
            "candidate_id", "session_date", "signal_time", "setup_id", "side",
            "symbol", "price_change_pct", "frozen_rank"
        ) if column in candidates.columns
    ]].copy()
    decisions["selection_passed"] = True
    decisions["selection_reason"] = "RAW_V8_CONTROL"
    audit = experiment._NEUTRAL_RUN_BACKTEST(
        candidates, paths, variant="V8_COMBINED", policy=policy,
        target_exposure_per_entry_rs=50_000.0,
    )
    outputs.append(_write_strategy(root, "V8_COMBINED", audit, decisions))

    policy = _configure_v10()
    selected, decisions = filters.selection_overlay(
        candidates, filters.SPEC_BY_NAME["0935_LONG_MOVE_MAX_050"]
    )
    with gaps.installed_gap_guard(gaps.GAP_GUARDS[2]):
        audit = experiment._NEUTRAL_RUN_BACKTEST(
            selected, paths, variant="V10_STAGE7_0935_LONG_MAX_050_GAP2",
            policy=policy, target_exposure_per_entry_rs=50_000.0,
        )
    outputs.append(_write_strategy(root, "V10_STAGE7_0935_LONG_MAX_050_GAP2", audit, decisions))

    policy = _configure_v10()
    selected, decisions = filters.selection_overlay(
        candidates, filters.SPEC_BY_NAME[v11_backtest.SELECTION_VARIANT]
    )
    with v11_execution.installed_runtime_hooks(
        v11_backtest.FIXED_RUNTIME_SPEC, allow_composite=True
    ):
        with v11_gap.installed_gap_guard(v11_backtest._gap_spec()):
            audit = experiment._NEUTRAL_RUN_BACKTEST(
                selected, paths, variant=v11_backtest.PROFILE_ID, policy=policy,
                target_exposure_per_entry_rs=50_000.0,
            )
    outputs.append(_write_strategy(root, "V11_STAGE10_FROZEN", audit, decisions))

    policy = _configure_v10()
    base_setups = tuple(engine.ACTIVE_SETUPS)
    prepared = v12_selection.prepare_variant_selection(
        candidates, base_setups, v12_backtest.FIXED_CONFIG
    )
    engine.ACTIVE_SETUPS = tuple(prepared.setups)
    runtime = v12_backtest._runtime_spec(prepared)
    with v11_execution.installed_runtime_hooks(
        v11_backtest.FIXED_RUNTIME_SPEC, allow_composite=True
    ):
        with v12_execution.installed_runtime_hooks(runtime):
            with v11_gap.installed_gap_guard(v12_backtest._gap_spec()):
                audit = experiment._NEUTRAL_RUN_BACKTEST(
                    prepared.candidates, paths, variant=v12_backtest.PROFILE_ID,
                    policy=policy, target_exposure_per_entry_rs=50_000.0,
                )
    result = _write_strategy(root, "V12_SELECTED", audit, prepared.decisions)
    result["metric"].update(
        {
            "all_input_candidates": len(candidates),
            "selected_candidates": len(prepared.candidates),
            "profile_id": v12_backtest.PROFILE_ID,
            "profile_sha256": v12_backtest.EXPECTED_PROFILE_SHA256,
        }
    )
    common.atomic_write_json(
        Path(result["paths"]["summary"]), _json_ready(result["metric"])
    )
    outputs.append(result)
    return outputs


def _run_v6(root: Path, candidates: pd.DataFrame, paths: pd.DataFrame) -> dict[str, Any]:
    v6.strict.configure_engine()
    selected, decisions = v6.apply_selection_overlay(
        candidates, v6.CHALLENGER_BY_NAME["CONTROL"]
    )
    policy = engine.entry_policy_for_variant(
        "VS", cost_bps=15.0, slippage_bps=0.0,
        square_off="15:30", eod_policy="LAST_REAL_BAR_SENSITIVITY",
    )
    audit = engine.run_v8_backtest(selected, paths, variant="VS", policy=policy)
    return _write_strategy(root, "V6_CONTROL", audit, decisions)


def _trade_contract(audit: pd.DataFrame) -> pd.DataFrame:
    fields = [
        "candidate_id", "symbol", "side", "confirmation_time", "entry_time",
        "entry_price", "stop_price", "target_price", "exit_time", "exit_price",
        "exit_reason", "net_return_pct", "net_pnl_rs",
    ]
    return audit.loc[today._bool_series(audit["filled"]), fields].copy()


def run(output_root: Path) -> Path:
    global CUTOFF
    frames, CUTOFF, source_evidence = _source_frames()
    stamp = datetime.now(common.IST).strftime("%Y%m%dT%H%M%S%f%z")
    root = output_root.resolve() / f"today_{SESSION.isoformat()}_cutoff_{CUTOFF.strftime('%H%M')}_{stamp}"
    root.mkdir(parents=True, exist_ok=False)

    v6_candidates = pd.read_parquet(V6_CACHE / "five_minute_candidates.parquet")
    v6_cached_paths = pd.read_parquet(V6_CACHE / "same_session_minute_paths.parquet")
    v6_paths = _refresh_paths(v6_candidates, v6_cached_paths, frames, CUTOFF, {"KAYNES", "PRESTIGE"})
    common.atomic_write_parquet(v6_paths, root / "v6_refreshed_paths.parquet")
    results = [_run_v6(root, v6_candidates, v6_paths)]

    v10_candidates = pd.read_parquet(V10_CACHE / "five_minute_candidates.parquet")
    v10_cached_paths = pd.read_parquet(V10_CACHE / "same_session_minute_paths.parquet")
    v10_paths = _refresh_paths(v10_candidates, v10_cached_paths, frames, CUTOFF, set(SYMBOLS))
    common.atomic_write_parquet(v10_paths, root / "v8_v12_refreshed_paths.parquet")
    results.extend(_run_v8_v12(root, v10_candidates, v10_paths))

    comparison = pd.DataFrame([item["metric"] for item in results])
    common.atomic_write_csv(comparison, root / "comparison.csv")
    contracts: dict[str, Any] = {}
    for item in results:
        audit = pd.read_csv(item["paths"]["audit"])
        contracts[str(item["metric"]["strategy"])] = _trade_contract(audit).to_dict("records")
    common.atomic_write_json(root / "trade_contracts.json", _json_ready(contracts))

    prior_checks: dict[str, Any] = {}
    prior_map = {
        "V6_CONTROL": BASE_RUN / "refreshed_paths_1144" / "v6_control" / "candidate_order_audit.csv",
        "V8_COMBINED": BASE_RUN / "refreshed_paths_1144" / "v8_combined" / "candidate_order_audit.csv",
        "V10_STAGE7_0935_LONG_MAX_050_GAP2": PRIOR_1144 / "strategies" / "v10_stage7_0935_long_max_050_gap2" / "candidate_order_audit.csv",
        "V11_STAGE10_FROZEN": PRIOR_1144 / "strategies" / "v11_stage10_frozen" / "candidate_order_audit.csv",
        "V12_SELECTED": PRIOR_V12_1144 / "candidate_order_audit.csv",
    }
    immutable = ["candidate_id", "confirmation_time", "entry_time", "entry_price", "stop_price", "target_price"]
    for item in results:
        strategy = str(item["metric"]["strategy"])
        current = _trade_contract(pd.read_csv(item["paths"]["audit"]))
        prior = _trade_contract(pd.read_csv(prior_map[strategy]))
        left = current.set_index("candidate_id")
        right = prior.set_index("candidate_id")
        common_ids = sorted(set(left.index) & set(right.index))
        unchanged = set(left.index) == set(right.index)
        for field in immutable[1:]:
            a = left.loc[common_ids, field].astype(str)
            b = right.loc[common_ids, field].astype(str)
            unchanged = unchanged and a.equals(b)
        prior_checks[strategy] = {
            "same_fill_identity": set(left.index) == set(right.index),
            "confirmation_entry_stop_target_unchanged": bool(unchanged),
        }

    manifest = {
        "schema_version": SCHEMA_VERSION,
        "complete": True,
        "created_at_ist": datetime.now(common.IST),
        "session_date": SESSION,
        "explicit_uniform_cutoff_ist": CUTOFF,
        "method": "FROZEN_CANDIDATES_REFRESHED_CASH_1M_PATHS_UNIFORM_CUTOFF",
        "source_evidence": source_evidence,
        "source_complete": False,
        "headline_valid": False,
        "economics": {
            "cost_bps": 15.0,
            "slippage_bps": 0.0,
            "target_exposure_per_entry_rs": 50_000.0,
            "square_off": "15:30",
            "eod_policy": "LAST_REAL_BAR_SENSITIVITY",
        },
        "checks_vs_1144": prior_checks,
        "artifacts": {
            "comparison": root / "comparison.csv",
            "trade_contracts": root / "trade_contracts.json",
            "v8_v12_paths": root / "v8_v12_refreshed_paths.parquet",
            "v6_paths": root / "v6_refreshed_paths.parquet",
        },
        "limitations": [
            "INTRADAY_PARTIAL_SESSION",
            "LAST_REAL_BAR_SENSITIVITY_NOT_FINAL_EOD",
            "CASH_EQUITY_EXECUTION_PROXY_NOT_ACTUAL_FUTURES_EXECUTION",
            "STATIC_CURRENT_26SEP_UNIVERSE_RESEARCH_ONLY",
        ],
        "research_only": True,
        "promotion_eligible": False,
        "live_or_paper_authority": False,
    }
    common.atomic_write_json(root / "manifest.json", _json_ready(manifest))
    print(root, flush=True)
    print(comparison.to_string(index=False), flush=True)
    return root


CUTOFF = pd.Timestamp(SESSION, tz=common.IST)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args()
    run(args.output_root)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
