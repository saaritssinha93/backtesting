"""Isolated V12 backtest adapter for an hourly experimental pre-filter.

This file does not modify V11 or V12. It temporarily wraps V12's candidate boundary
inside this process, applies the causal hourly ticker membership, and then lets
the unchanged V11 pipeline choose side, setup, entry, exit, costs, and P&L.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Callable
from zoneinfo import ZoneInfo

import pandas as pd


IST = "Asia/Kolkata"
DEFAULT_CANDIDATES = Path(
    r"C:\TradingData\eqidv2_experiments\prefilter_hourly_replay\hourly_candidates_20260803.csv"
)
DEFAULT_OUTPUT = Path(r"C:\TradingData\eqidv2_experiments\v12_hourly_prefilter_loose_20260803")
DEFAULT_5M_DIR = Path(r"C:\TradingData\eqidv2\stocks_indicators_5min_eq_live2")


@dataclass(frozen=True)
class HourlyPool:
    slot_ist: pd.Timestamp
    candidates: pd.DataFrame
    ticker_metadata: dict[str, dict[str, Any]]

    @property
    def tickers(self) -> frozenset[str]:
        return frozenset(self.ticker_metadata)


def _ist(value: object) -> pd.Timestamp:
    timestamp = pd.Timestamp(value)
    if pd.isna(timestamp):
        return pd.NaT
    if timestamp.tzinfo is None:
        return timestamp.tz_localize(IST)
    return timestamp.tz_convert(IST)


def _sha256(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def load_hourly_pools(
    path: str | Path,
    date_text: str,
    *,
    end_date_text: str | None = None,
    expected_budget: int = 300,
    effective_budget: int | None = None,
) -> list[HourlyPool]:
    source = Path(path)
    frame = pd.read_csv(source)
    required = {"slot_ist", "ticker", "selection_rank", "selection_bucket", "primary_side"}
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ValueError(f"prefilter candidates are missing columns: {missing}")
    frame = frame.copy()
    frame["ticker"] = frame["ticker"].astype(str).str.upper().str.strip()
    frame["_slot"] = frame["slot_ist"].map(_ist)
    if frame["_slot"].isna().any() or frame["ticker"].eq("").any():
        raise ValueError("prefilter candidates contain invalid slot/ticker values")
    requested_start = pd.Timestamp(date_text).date()
    requested_end = pd.Timestamp(end_date_text or date_text).date()
    if requested_end < requested_start:
        raise ValueError("prefilter end date precedes start date")
    frame_dates = frame["_slot"].map(lambda value: value.date())
    if end_date_text is None:
        if set(frame_dates) != {requested_start}:
            raise ValueError(f"prefilter candidates are not exclusively for {date_text}")
    else:
        frame = frame.loc[frame_dates.between(requested_start, requested_end)].copy()
        if frame.empty:
            raise ValueError(
                f"prefilter candidates contain no rows from {date_text} through {end_date_text}"
            )

    expected_slots = [
        pd.Timestamp(f"{day_text} {hour:02d}:20", tz=IST)
        for day_text in sorted(set(frame["_slot"].map(lambda value: value.strftime("%Y-%m-%d"))))
        for hour in range(9, 16)
    ]
    actual_slots = sorted(frame["_slot"].unique())
    if actual_slots != expected_slots:
        raise ValueError(
            "hourly prefilter schedule mismatch: "
            f"expected={[value.isoformat() for value in expected_slots]} "
            f"actual={[pd.Timestamp(value).isoformat() for value in actual_slots]}"
        )

    pools: list[HourlyPool] = []
    for slot in expected_slots:
        selected = frame.loc[frame["_slot"].eq(slot)].copy()
        if len(selected) != expected_budget or selected["ticker"].nunique() != expected_budget:
            raise ValueError(
                f"invalid prefilter budget at {slot.isoformat()}: "
                f"rows={len(selected)} unique={selected['ticker'].nunique()} expected={expected_budget}"
            )
        ranks = pd.to_numeric(selected["selection_rank"], errors="coerce")
        if set(ranks.dropna().astype(int)) != set(range(1, expected_budget + 1)):
            raise ValueError(f"selection ranks are not complete at {slot.isoformat()}")
        active_budget = int(effective_budget or expected_budget)
        if active_budget <= 0 or active_budget > expected_budget:
            raise ValueError(
                f"effective budget must be between 1 and {expected_budget}: {active_budget}"
            )
        selected = selected.loc[ranks.le(active_budget)].copy()
        if len(selected) != active_budget or selected["ticker"].nunique() != active_budget:
            raise ValueError(f"effective budget selection failed at {slot.isoformat()}")
        selected = selected.sort_values("selection_rank", kind="mergesort").reset_index(drop=True)
        metadata_columns = [
            column
            for column in (
                "selection_rank",
                "selection_bucket",
                "primary_side",
                "primary_family",
                "overall_score",
                "long_score",
                "short_score",
                "activity_score",
            )
            if column in selected.columns
        ]
        metadata = {
            str(row["ticker"]): {column: row[column] for column in metadata_columns}
            for _, row in selected.iterrows()
        }
        pools.append(HourlyPool(slot_ist=slot, candidates=selected, ticker_metadata=metadata))
    return pools


def _candidate_time_column(frame: pd.DataFrame) -> str:
    for column in ("scan_slot_ist", "signal_time_ist", "signal_ts", "bar_time_ist", "slot_ist"):
        if column in frame.columns:
            return column
    raise ValueError("V11 candidates contain no recognised signal/slot timestamp")


def filter_candidates_for_hourly_pools(
    frame: pd.DataFrame | None,
    pools: list[HourlyPool],
) -> tuple[pd.DataFrame, dict[str, Any]]:
    if frame is None or frame.empty:
        empty = pd.DataFrame() if frame is None else frame.copy()
        return empty, {"input_rows": 0, "kept_rows": 0, "rejected_rows": 0, "by_pool": {}}
    if "ticker" not in frame.columns:
        raise ValueError("V11 candidates contain no ticker column")
    work = frame.copy()
    time_column = _candidate_time_column(work)
    work["_prefilter_ts"] = work[time_column].map(_ist)
    work["_prefilter_ticker"] = work["ticker"].astype(str).str.upper().str.strip()
    pool_slots = [pool.slot_ist for pool in pools]
    activation_slots = [slot + pd.Timedelta(minutes=5) for slot in pool_slots]
    keep: list[bool] = []
    matched_slots: list[str | None] = []
    metadata_rows: list[dict[str, Any]] = []
    by_pool: dict[str, dict[str, int]] = {
        slot.isoformat(): {"input_rows": 0, "kept_rows": 0}
        for slot in pool_slots
    }
    for timestamp, ticker in zip(work["_prefilter_ts"], work["_prefilter_ticker"]):
        pool_index = -1
        if pd.notna(timestamp):
            for index, activation in enumerate(activation_slots):
                if timestamp >= activation:
                    pool_index = index
                else:
                    break
        if pool_index < 0 or timestamp.date() != pool_slots[pool_index].date():
            keep.append(False)
            matched_slots.append(None)
            metadata_rows.append({})
            continue
        pool = pools[pool_index]
        slot_key = pool.slot_ist.isoformat()
        by_pool[slot_key]["input_rows"] += 1
        accepted = ticker in pool.tickers
        keep.append(accepted)
        matched_slots.append(slot_key)
        metadata_rows.append(pool.ticker_metadata.get(ticker, {}))
        if accepted:
            by_pool[slot_key]["kept_rows"] += 1

    mask = pd.Series(keep, index=work.index, dtype=bool)
    kept = work.loc[mask].copy()
    kept["prefilter_slot_ist"] = [value for value, accepted in zip(matched_slots, keep) if accepted]
    kept_metadata = [value for value, accepted in zip(metadata_rows, keep) if accepted]
    metadata_mapping = {
        "selection_rank": "prefilter_selection_rank",
        "selection_bucket": "prefilter_selection_bucket",
        "primary_side": "prefilter_suggested_side",
        "primary_family": "prefilter_primary_family",
        "overall_score": "prefilter_overall_score",
        "long_score": "prefilter_long_score",
        "short_score": "prefilter_short_score",
        "activity_score": "prefilter_activity_score",
    }
    for source, target in metadata_mapping.items():
        kept[target] = [values.get(source) for values in kept_metadata]
    kept = kept.drop(columns=["_prefilter_ts", "_prefilter_ticker"], errors="ignore")
    return kept.reset_index(drop=True), {
        "input_rows": int(len(work)),
        "kept_rows": int(len(kept)),
        "rejected_rows": int(len(work) - len(kept)),
        "by_pool": by_pool,
        "time_column": time_column,
    }


def ticker_union_by_date(pools: list[HourlyPool]) -> dict[str, frozenset[str]]:
    """Exact scanner universe reduction for a causal hourly membership replay.

    A ticker absent from every pool on a date can never survive the downstream
    membership filter, so omitting it before detector computation changes no
    accepted candidate while substantially reducing replay time.
    """
    grouped: dict[str, set[str]] = {}
    for pool in pools:
        day = pool.slot_ist.strftime("%Y-%m-%d")
        grouped.setdefault(day, set()).update(
            str(ticker).upper().strip() for ticker in pool.tickers if str(ticker).strip()
        )
    return {day: frozenset(tickers) for day, tickers in grouped.items()}


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Isolated hourly-prefilter V12 research backtest")
    parser.add_argument("--date", default="2026-08-03")
    parser.add_argument("--start-date")
    parser.add_argument("--end-date")
    parser.add_argument("--prefilter-candidates", type=Path, default=DEFAULT_CANDIDATES)
    parser.add_argument("--budget", type=int, default=300)
    parser.add_argument("--effective-budget", type=int)
    parser.add_argument("--out", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--candidate-5m-dir", type=Path, default=DEFAULT_5M_DIR)
    parser.add_argument("--setup-conf-module", default="final_setup_conf_v12")
    parser.add_argument(
        "--cached-raw-snapshot-dir",
        type=Path,
        help="optional complete full-universe raw V11/V12 slot snapshots with unchanged detectors",
    )
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument(
        "--candidate-pool-only",
        action="store_true",
        help="write the corrected hourly-prefilter candidate pool without resolving trades",
    )
    parser.add_argument("--start-time", default="09:20")
    parser.add_argument("--end-time", default="15:00")
    parser.add_argument("--ab-gate-min-quality", type=float, default=200.0)
    parser.add_argument("--ab-gate-max-per-side", type=int, default=2)
    parser.add_argument("--ab-gate-max-per-slot", type=int, default=4)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    range_start = args.start_date or args.date
    range_end = args.end_date or args.start_date or args.date
    multi_day = range_start != range_end or args.start_date is not None or args.end_date is not None
    if pd.Timestamp(range_end) < pd.Timestamp(range_start):
        raise SystemExit("end date precedes start date")
    if args.cached_raw_snapshot_dir and range_start != range_end:
        raise SystemExit("cached raw snapshot mode currently supports one date only")
    if args.out.exists() and any(args.out.iterdir()):
        raise SystemExit(f"refusing non-empty output directory: {args.out}")
    pools = load_hourly_pools(
        args.prefilter_candidates,
        range_start,
        end_date_text=(range_end if multi_day else None),
        expected_budget=args.budget,
        effective_budget=args.effective_budget,
    )
    pool_dates = sorted({pool.slot_ist.strftime("%Y-%m-%d") for pool in pools})

    os.environ["EQIDV2_V12_FINAL_SETUP_CONF_MODULE"] = str(args.setup_conf_module)
    import avwap_5min_ID_v12_backtesting as v11

    original_scan: Callable[..., Any] = v11._scan_historical_full_day_candidates
    original_merge: Callable[..., Any] = v11._merge_external_conf_candidates
    original_pipeline: Callable[..., Any] = v11._apply_v7_live_strategy
    original_universe_loader: Callable[..., Any] = v11.candidate_scan.v2._load_universe
    eligible_tickers_by_day = ticker_union_by_date(pools)
    audit_rows: list[dict[str, Any]] = []

    def filtered_scan(**kwargs: Any) -> tuple[pd.DataFrame, pd.DataFrame]:
        if args.cached_raw_snapshot_dir:
            raw = v11._load_live_candidate_snapshots(args.cached_raw_snapshot_dir, range_start)
            # Archived raw JSON files were written during a later research run,
            # so their file publication time is not the historical decision
            # time. Direct historical scanning uses the completed signal close.
            if not raw.empty and "signal_time_ist" in raw.columns:
                raw = raw.copy()
                raw["decision_ready_at_ist"] = raw["signal_time_ist"]
                raw["decision_ready_source"] = "cached_historical_signal_close"
            slots = v11._slot_range_for_day(range_start, args.start_time, args.end_time)
            slot_summary = pd.DataFrame(
                {
                    "slot_ist": [v11._fmt_ist(slot) for slot in slots],
                    "raw_candidate_count": 0,
                    "long_candidates": 0,
                    "short_candidates": 0,
                    "elapsed_sec": 0.0,
                    "snapshot_path": str(args.cached_raw_snapshot_dir),
                }
            )
            audit_rows.append(
                {
                    "stage": "candidate_source",
                    "source": "cached_complete_full_universe_raw_slot_snapshots",
                    "input_rows": int(len(raw)),
                    "path": str(args.cached_raw_snapshot_dir.resolve()),
                }
            )
        else:
            scan_day = str(kwargs.get("day", ""))[:10]
            eligible = eligible_tickers_by_day.get(scan_day, frozenset())
            full_universe = original_universe_loader()
            narrowed_universe = [
                ticker for ticker in full_universe
                if str(ticker).upper().strip() in eligible
            ]
            audit_rows.append({
                "stage": "scanner_universe",
                "day": scan_day,
                "full_universe_tickers": int(len(full_universe)),
                "prefilter_daily_union_tickers": int(len(eligible)),
                "scanner_tickers": int(len(narrowed_universe)),
                "exact_reduction": True,
            })
            v11.candidate_scan.v2._load_universe = lambda: narrowed_universe
            try:
                raw, slot_summary = original_scan(**kwargs)
            finally:
                v11.candidate_scan.v2._load_universe = original_universe_loader
        filtered, stats = filter_candidates_for_hourly_pools(raw, pools)
        audit_rows.append({"stage": "scanner_raw", **stats})
        if not slot_summary.empty:
            slot_summary = slot_summary.copy()
            filtered_times = (
                filtered["scan_slot_ist"].map(_ist)
                if not filtered.empty and "scan_slot_ist" in filtered.columns
                else pd.Series(dtype="datetime64[ns]")
            )
            for index, row in slot_summary.iterrows():
                slot = _ist(row["slot_ist"])
                group = filtered.loc[filtered_times.eq(slot)].copy() if not filtered.empty else pd.DataFrame()
                slot_summary.at[index, "raw_candidate_count"] = len(group)
                slot_summary.at[index, "long_candidates"] = int(
                    0 if group.empty else group["side"].astype(str).str.upper().eq("LONG").sum()
                )
                slot_summary.at[index, "short_candidates"] = int(
                    0 if group.empty else group["side"].astype(str).str.upper().eq("SHORT").sum()
                )
                slot_summary.at[index, "prefilter_active_slot_ist"] = (
                    max(
                        (
                            pool.slot_ist
                            for pool in pools
                            if pool.slot_ist + pd.Timedelta(minutes=5) <= slot
                            and pool.slot_ist.date() == slot.date()
                        ),
                        default=pd.NaT,
                    ).isoformat()
                    if any(
                        pool.slot_ist + pd.Timedelta(minutes=5) <= slot
                        and pool.slot_ist.date() == slot.date()
                        for pool in pools
                    )
                    else ""
                )
        return filtered, slot_summary

    def filtered_merge(raw: pd.DataFrame, day: str) -> pd.DataFrame:
        merged = original_merge(raw, day)
        filtered, stats = filter_candidates_for_hourly_pools(merged, pools)
        audit_rows.append({"stage": "after_external_conf_merge", **stats})
        return filtered

    def filtered_pipeline(raw: pd.DataFrame, date_hint: str = "", **kwargs: Any) -> dict:
        filtered, stats = filter_candidates_for_hourly_pools(raw, pools)
        audit_rows.append({"stage": "pipeline_defensive_gate", **stats})
        return original_pipeline(filtered, date_hint, **kwargs)

    v11._scan_historical_full_day_candidates = filtered_scan
    v11._merge_external_conf_candidates = filtered_merge
    v11._apply_v7_live_strategy = filtered_pipeline
    # The prefilter manifest already provides the exact, validated trading-date
    # schedule.  Avoid rescanning every ticker parquet merely to rediscover the
    # same dates (an O(universe) startup cost before each chunk).
    def direct_dates_with_roots(
        primary: Path,
        fallback: Path | None = None,
        start_date: str = "",
        end_date: str = "",
    ) -> tuple[list[str], dict[str, Path]]:
        selected_dates = [
            day for day in pool_dates
            if (not start_date or day >= str(start_date)[:10])
            and (not end_date or day <= str(end_date)[:10])
        ]
        return selected_dates, {day: Path(args.candidate_5m_dir) for day in selected_dates}

    v11._available_historical_dates_with_roots = direct_dates_with_roots

    v11_args = [
        str(Path(v11.__file__).resolve()),
        "--mode", "historical_all_available",
        "--out", str(args.out),
        "--start_date", range_start,
        "--end_date", range_end,
        "--start_time", args.start_time,
        "--end_time", args.end_time,
        "--workers", str(args.workers),
        "--candidate_5m_dir", str(args.candidate_5m_dir),
        "--fallback_candidate_5m_dir", str(args.candidate_5m_dir),
        "--selected_strategy_profile", "final_setup_conf",
        "--ab_gate_profile", "quality_top_slot",
        "--ab_gate_min_quality", str(args.ab_gate_min_quality),
        "--ab_gate_max_per_side", str(args.ab_gate_max_per_side),
        "--ab_gate_max_per_slot", str(args.ab_gate_max_per_slot),
        "--cost_model", "statutory",
        "--slippage_bps", "0",
        "--entry_fill_model", "ltp_on_signal_1m_open",
        "--parity-debug",
    ]
    if args.candidate_pool_only:
        v11_args.append("--candidate_pool_only")
    previous_argv = sys.argv
    sys.argv = v11_args
    try:
        result = int(v11.main())
    finally:
        sys.argv = previous_argv

    args.out.mkdir(parents=True, exist_ok=True)
    audit_path = args.out / "prefilter_integration_audit.json"
    manifest = {
        "schema_version": "eqidv2_experimental_v12_hourly_prefilter_v1",
        "mode": "RESEARCH_ONLY",
        "production_consumption_allowed": False,
        "created_at_ist": datetime.now(ZoneInfo(IST)).isoformat(),
        "research_start_date": range_start,
        "research_end_date": range_end,
        "research_trading_dates": pool_dates,
        "research_trading_day_count": len(pool_dates),
        "main_v11_modified": False,
        "main_v12_modified_during_run": False,
        "v12_source_path": str(Path(v11.__file__).resolve()),
        "v12_source_sha256": _sha256(v11.__file__),
        "adapter_path": str(Path(__file__).resolve()),
        "adapter_sha256": _sha256(__file__),
        "prefilter_candidates_path": str(args.prefilter_candidates.resolve()),
        "prefilter_candidates_sha256": _sha256(args.prefilter_candidates),
        "prefilter_source_budget": args.budget,
        "prefilter_effective_budget": args.effective_budget or args.budget,
        "candidate_5m_dir": str(args.candidate_5m_dir.resolve()),
        "strategy_profile": "final_setup_conf",
        "setup_conf_module": str(args.setup_conf_module),
        "ab_gate_min_quality": args.ab_gate_min_quality,
        "ab_gate_max_per_side": args.ab_gate_max_per_side,
        "ab_gate_max_per_slot": args.ab_gate_max_per_slot,
        "cached_raw_snapshot_dir": (
            str(args.cached_raw_snapshot_dir.resolve()) if args.cached_raw_snapshot_dir else None
        ),
        "start_time": args.start_time,
        "end_time": args.end_time,
        "direction_policy": "ticker_membership_only_v11_retains_final_long_short_decision",
        "scanner_universe_policy": "exact_daily_union_of_hourly_prefilter_memberships",
        "candidate_pool_only": bool(args.candidate_pool_only),
        "active_intervals": [
            {
                "prefilter_slot_ist": pool.slot_ist.isoformat(),
                "active_from_ist": (pool.slot_ist + pd.Timedelta(minutes=5)).isoformat(),
                "active_through_ist": (
                    pools[index + 1].slot_ist.isoformat()
                    if index + 1 < len(pools)
                    and pools[index + 1].slot_ist.date() == pool.slot_ist.date()
                    else None
                ),
                "candidate_count": len(pool.tickers),
                "used_by_backtest": (
                    pool.slot_ist + pd.Timedelta(minutes=5)
                ).strftime("%H:%M") <= args.end_time,
            }
            for index, pool in enumerate(pools)
        ],
        "filter_audit": audit_rows,
    }
    audit_path.write_text(json.dumps(manifest, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")
    inputs_path = args.out / "inputs.txt"
    with inputs_path.open("a", encoding="utf-8") as handle:
        handle.write(
            f"experimental_hourly_prefilter_candidates={args.prefilter_candidates.resolve()}\n"
            f"experimental_hourly_prefilter_sha256={_sha256(args.prefilter_candidates)}\n"
            f"experimental_hourly_prefilter_budget={args.budget}\n"
            f"experimental_hourly_prefilter_start_date={range_start}\n"
            f"experimental_hourly_prefilter_end_date={range_end}\n"
            "experimental_hourly_prefilter_direction_policy=ticker_membership_only\n"
            "experimental_hourly_prefilter_production_consumption_allowed=false\n"
        )
    print(f"[v12 hourly prefilter] audit={audit_path}", flush=True)
    return result


if __name__ == "__main__":
    raise SystemExit(main())
