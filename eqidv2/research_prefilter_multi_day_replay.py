"""Causal multi-day hourly replay for the standalone experimental pre-filter.

This research utility does not import or modify V7, V11, or V12.  It creates
09:20/10:20/.../15:20 candidate lists using only bars at or before each slot.
Archived final-marker authority is audited independently because marker history
may cover only part of a requested historical price window.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import time
from collections import Counter
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd
import pyarrow.parquet as pq

from experimental_prefilter.config import PrefilterConfig
from experimental_prefilter.engine import (
    annotate_budget_grid,
    build_features,
    rank_universe,
    select_candidates,
)
from experimental_prefilter.io import (
    BAR_COLUMNS,
    file_sha256,
    final_marker_path,
    load_slot_marker,
    load_universe_manifest,
    write_research_text,
)
from experimental_prefilter.manifest import candidate_sha256, config_sha256


IST_NAME = "Asia/Kolkata"
IST = ZoneInfo(IST_NAME)
DEFAULT_DATA_DIR = Path(r"C:\TradingData\eqidv2\stocks_indicators_5min_eq_live2")
DEFAULT_UNIVERSE_MANIFEST = Path(
    r"C:\TradingData\eqidv2\runtime_status\feed_universe_kiteticker_5m.json"
)
DEFAULT_MARKER_DIR = Path(r"C:\TradingData\eqidv2\slot_ready_5m")
DEFAULT_OUTPUT_DIR = Path(
    r"C:\TradingData\eqidv2_experiments\prefilter_six_month_replay_20260204_20260803_k300"
)
OUTPUT_COLUMNS = (
    "slot_ist",
    "ticker",
    "selection_rank",
    "selection_bucket",
    "primary_side",
    "primary_family",
    "selection_reason",
    "overall_score",
    "long_score",
    "short_score",
    "activity_score",
    "date",
    "staleness_seconds",
)


def _ist_timestamp(value: object) -> pd.Timestamp:
    stamp = pd.Timestamp(value)
    if stamp.tzinfo is None:
        return stamp.tz_localize(IST_NAME)
    return stamp.tz_convert(IST_NAME)


def combined_selection_sha256(candidates: pd.DataFrame) -> str:
    ordered = candidates.copy()
    ordered["_slot"] = pd.to_datetime(ordered["slot_ist"], errors="raise")
    ordered = ordered.sort_values(["_slot", "selection_rank", "ticker"], kind="mergesort")
    payload = "\n".join(
        f"{row.slot_ist}|{row.ticker}|{int(row.selection_rank)}|{row.primary_side}|{row.primary_family}"
        for row in ordered.itertuples(index=False)
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _read_symbol_range(
    data_dir: Path,
    ticker: str,
    start_ist: pd.Timestamp,
    end_ist: pd.Timestamp,
) -> tuple[str, pd.DataFrame | None, str | None]:
    path = data_dir / f"{ticker}_stocks_indicators_5min.parquet"
    if not path.exists():
        return ticker, None, "missing_file"
    try:
        parquet = pq.ParquetFile(path)
        available = set(parquet.schema.names)
        columns = [column for column in BAR_COLUMNS if column in available]
        time_column = "date" if "date" in columns else "datetime" if "datetime" in columns else None
        required = {"open", "high", "low", "close", "volume"}
        if time_column is None or not required.issubset(columns):
            return ticker, None, "missing_required_columns"
        table = pq.read_table(
            path,
            columns=columns,
            filters=[
                (time_column, ">=", start_ist.to_pydatetime()),
                (time_column, "<=", end_ist.to_pydatetime()),
            ],
            use_threads=False,
        )
        frame = table.to_pandas()
        if frame.empty:
            return ticker, None, "no_rows_in_range"
        if time_column != "date":
            frame = frame.rename(columns={time_column: "date"})
        frame["date"] = pd.to_datetime(frame["date"])
        if frame["date"].dt.tz is None:
            frame["date"] = frame["date"].dt.tz_localize(IST_NAME)
        else:
            frame["date"] = frame["date"].dt.tz_convert(IST_NAME)
        frame = frame.loc[frame["date"].between(start_ist, end_ist, inclusive="both")].copy()
        frame["ticker"] = ticker
        return ticker, frame, None
    except Exception as exc:  # pragma: no cover - environment-specific parquet failures
        return ticker, None, f"{type(exc).__name__}:{exc}"


def load_range_once(
    data_dir: Path,
    symbols: tuple[str, ...],
    start_ist: pd.Timestamp,
    end_ist: pd.Timestamp,
    max_workers: int,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    started = time.perf_counter()
    frames: list[pd.DataFrame] = []
    errors: list[str] = []
    workers = max(1, min(int(max_workers), len(symbols)))
    with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="prefilter-six-month-read") as pool:
        futures = {
            pool.submit(_read_symbol_range, data_dir, ticker, start_ist, end_ist): ticker
            for ticker in symbols
        }
        completed = 0
        for future in as_completed(futures):
            ticker, frame, error = future.result()
            completed += 1
            if frame is not None:
                frames.append(frame)
            elif len(errors) < 200:
                errors.append(f"{ticker}:{error}")
            if completed % 200 == 0 or completed == len(symbols):
                print(
                    f"[prefilter multi-day load] symbols={completed}/{len(symbols)} "
                    f"loaded={len(frames)} errors={len(errors)} elapsed={time.perf_counter()-started:.1f}s",
                    flush=True,
                )
    if not frames:
        raise RuntimeError("no historical bars were loaded")
    bars = pd.concat(frames, ignore_index=True, copy=False)
    bars = bars.sort_values("date", kind="mergesort").set_index("date", drop=False)
    stats = {
        "requested_symbols": len(symbols),
        "loaded_symbols": len(frames),
        "errors": errors,
        "rows": int(len(bars)),
        "elapsed_seconds": time.perf_counter() - started,
        "range_start_ist": start_ist.isoformat(),
        "range_end_ist": end_ist.isoformat(),
    }
    return bars, stats


def discover_complete_dates(
    data_dir: Path,
    symbols: tuple[str, ...],
    start_date: str,
    end_date: str,
    completion_time: str = "15:20",
) -> list[str]:
    reference = "RELIANCE" if "RELIANCE" in symbols else symbols[0]
    path = data_dir / f"{reference}_stocks_indicators_5min.parquet"
    parquet = pq.ParquetFile(path)
    time_column = "date" if "date" in parquet.schema.names else "datetime"
    frame = parquet.read(columns=[time_column], use_threads=False).to_pandas()
    stamps = pd.to_datetime(frame[time_column])
    if stamps.dt.tz is None:
        stamps = stamps.dt.tz_localize(IST_NAME)
    else:
        stamps = stamps.dt.tz_convert(IST_NAME)
    start = pd.Timestamp(start_date, tz=IST_NAME)
    end = pd.Timestamp(f"{end_date} 23:59:59", tz=IST_NAME)
    stamps = stamps.loc[stamps.between(start, end, inclusive="both")]
    completed = stamps.loc[stamps.dt.strftime("%H:%M").eq(completion_time)]
    return sorted(set(completed.dt.strftime("%Y-%m-%d")))


def marker_status(marker_dir: Path, slot: pd.Timestamp, universe: Any) -> tuple[str, str]:
    path = final_marker_path(marker_dir, slot.isoformat())
    if not path.exists():
        return "MISSING", ""
    try:
        marker = load_slot_marker(path)
        if _ist_timestamp(marker.slot_ist) != slot:
            return "MISMATCH", marker.sha256
        if marker.tickers_expected != len(universe.symbols):
            return "MISMATCH", marker.sha256
        if marker.universe_sha256 != universe.universe_sha256:
            return "MISMATCH", marker.sha256
        return "VALID", marker.sha256
    except Exception:
        return "INVALID", ""


def _daily_frame(bars: pd.DataFrame, date_text: str) -> pd.DataFrame:
    start = pd.Timestamp(date_text, tz=IST_NAME)
    end = start + pd.Timedelta(days=1) - pd.Timedelta(nanoseconds=1)
    try:
        return bars.loc[start:end].copy()
    except KeyError:
        return bars.iloc[0:0].copy()


def evaluate_day(
    date_text: str,
    history: pd.DataFrame,
    day: pd.DataFrame,
    cfg: PrefilterConfig,
    universe: Any,
    marker_dir: Path,
    daily_dir: Path,
) -> tuple[str, pd.DataFrame, list[dict[str, Any]]]:
    daily_candidates: list[pd.DataFrame] = []
    audit_rows: list[dict[str, Any]] = []
    for hour in range(9, 16):
        slot_started = time.perf_counter()
        slot = pd.Timestamp(f"{date_text} {hour:02d}:20", tz=IST_NAME)
        current = day.loc[day["date"].le(slot)].copy()
        causal = pd.concat([history, current], ignore_index=True, copy=False)
        features, feature_stats = build_features(causal, slot, cfg)
        ranked = annotate_budget_grid(rank_universe(features), cfg)
        selected = select_candidates(ranked, cfg.budget, cfg)
        expected_count = min(cfg.budget, feature_stats.eligible_count)
        if len(selected) != expected_count:
            raise RuntimeError(
                f"selection count mismatch date={date_text} slot={slot.isoformat()} "
                f"selected={len(selected)} expected={expected_count} "
                f"eligible={feature_stats.eligible_count} budget={cfg.budget}"
            )
        if selected["ticker"].duplicated().any():
            raise RuntimeError(f"duplicate ticker selection at {slot.isoformat()}")
        if selected["selection_rank"].astype(int).tolist() != list(range(1, expected_count + 1)):
            raise RuntimeError(f"non-contiguous selection ranks at {slot.isoformat()}")

        selected = selected.copy()
        selected["slot_ist"] = slot.isoformat()
        selected = selected.loc[:, [c for c in OUTPUT_COLUMNS if c in selected.columns]]
        daily_candidates.append(selected)

        status, marker_hash = marker_status(marker_dir, slot, universe)
        exact_count = int(day.loc[day["date"].eq(slot), "ticker"].nunique())
        side_counts = Counter(selected["primary_side"].astype(str))
        bucket_counts = Counter(selected["selection_bucket"].astype(str))
        audit_rows.append(
            {
                "date": date_text,
                "slot_ist": slot.isoformat(),
                "universe_count": len(universe.symbols),
                "exact_bar_symbol_count": exact_count,
                "feature_universe_count": feature_stats.universe_count,
                "eligible_count": feature_stats.eligible_count,
                "selected_count": len(selected),
                "long_count": side_counts.get("LONG", 0),
                "short_count": side_counts.get("SHORT", 0),
                "activity_bucket_count": sum(
                    count for bucket, count in bucket_counts.items() if bucket.endswith(":ACTIVITY")
                ),
                "marker_status": status,
                "marker_sha256": marker_hash,
                "candidate_sha256": candidate_sha256(selected),
                "elapsed_seconds": time.perf_counter() - slot_started,
            }
        )

    combined_day = pd.concat(daily_candidates, ignore_index=True)
    daily_path = daily_dir / f"hourly_candidates_{date_text.replace('-', '')}.csv"
    write_research_text(daily_path, combined_day.to_csv(index=False))
    return date_text, combined_day, audit_rows


def load_validated_daily_resume(
    path: Path,
    date_text: str,
    budget: int,
    day: pd.DataFrame,
    universe: Any,
    marker_dir: Path,
) -> tuple[pd.DataFrame, list[dict[str, Any]]]:
    frame = pd.read_csv(path)
    required = {"slot_ist", "ticker", "selection_rank", "primary_side", "primary_family"}
    missing = sorted(required - set(frame.columns))
    if missing:
        raise RuntimeError(f"resume file is missing columns path={path} missing={missing}")
    frame["_slot"] = pd.to_datetime(frame["slot_ist"], errors="coerce")
    expected_slots = [
        pd.Timestamp(f"{date_text} {hour:02d}:20", tz=IST_NAME) for hour in range(9, 16)
    ]
    if frame["_slot"].isna().any() or sorted(frame["_slot"].unique()) != expected_slots:
        raise RuntimeError(f"resume file has an invalid hourly schedule: {path}")
    audit_rows: list[dict[str, Any]] = []
    for slot, group in frame.groupby("_slot", sort=True):
        if len(group) > budget or group.empty or group["ticker"].duplicated().any():
            raise RuntimeError(f"resume file slot uniqueness mismatch path={path} slot={slot}")
        ranks = pd.to_numeric(group["selection_rank"], errors="raise").astype(int).sort_values().tolist()
        if ranks != list(range(1, len(group) + 1)):
            raise RuntimeError(f"resume file ranks are not contiguous path={path} slot={slot}")
        selected = group.drop(columns="_slot").sort_values("selection_rank", kind="mergesort")
        status, marker_hash = marker_status(marker_dir, slot, universe)
        side_counts = Counter(selected["primary_side"].astype(str))
        bucket_counts = Counter(selected["selection_bucket"].astype(str))
        audit_rows.append(
            {
                "date": date_text,
                "slot_ist": slot.isoformat(),
                "universe_count": len(universe.symbols),
                "exact_bar_symbol_count": int(day.loc[day["date"].eq(slot), "ticker"].nunique()),
                "feature_universe_count": None,
                "eligible_count": None,
                "selected_count": len(selected),
                "long_count": side_counts.get("LONG", 0),
                "short_count": side_counts.get("SHORT", 0),
                "activity_bucket_count": sum(
                    count for bucket, count in bucket_counts.items() if bucket.endswith(":ACTIVITY")
                ),
                "marker_status": status,
                "marker_sha256": marker_hash,
                "candidate_sha256": candidate_sha256(selected),
                "elapsed_seconds": 0.0,
                "audit_source": "RESUMED_VALIDATED_DAILY_FILE",
            }
        )
    return frame.drop(columns="_slot"), audit_rows


def run(args: argparse.Namespace) -> int:
    started = time.perf_counter()
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    daily_dir = output_dir / "daily"
    daily_dir.mkdir(parents=True, exist_ok=True)

    universe = load_universe_manifest(args.universe_manifest)
    cfg = PrefilterConfig().with_overrides(budget=args.budget)
    dates = discover_complete_dates(
        args.data_dir, universe.symbols, args.start_date, args.end_date
    )
    if not dates:
        raise RuntimeError("no complete trading dates were found")
    if args.limit_days:
        dates = dates[: int(args.limit_days)]

    warmup_start = pd.Timestamp(dates[0], tz=IST_NAME) - pd.Timedelta(days=10)
    range_end = pd.Timestamp(f"{dates[-1]} 15:20", tz=IST_NAME)
    bars, load_stats = load_range_once(
        Path(args.data_dir),
        universe.symbols,
        warmup_start,
        range_end,
        args.max_workers,
    )
    if load_stats["loaded_symbols"] != len(universe.symbols):
        raise RuntimeError(
            f"incomplete symbol-file load: loaded={load_stats['loaded_symbols']} "
            f"expected={len(universe.symbols)} errors={load_stats['errors'][:10]}"
        )

    emit_dates = set(dates)
    all_loaded_dates = sorted(set(bars.index.strftime("%Y-%m-%d")))
    process_dates = [date for date in all_loaded_dates if date <= dates[-1]]
    history = bars.iloc[0:0].reset_index(drop=True)
    jobs: list[tuple[str, pd.DataFrame, pd.DataFrame]] = []
    all_candidates: list[pd.DataFrame] = []
    slot_rows: list[dict[str, Any]] = []
    resumed_days = 0

    for date_text in process_dates:
        day = _daily_frame(bars, date_text).reset_index(drop=True)
        if day.empty:
            continue
        if date_text in emit_dates:
            daily_path = daily_dir / f"hourly_candidates_{date_text.replace('-', '')}.csv"
            if args.resume_daily and daily_path.exists():
                resumed, resumed_audit = load_validated_daily_resume(
                    daily_path,
                    date_text,
                    cfg.budget,
                    day,
                    universe,
                    Path(args.marker_dir),
                )
                all_candidates.append(resumed)
                slot_rows.extend(resumed_audit)
                resumed_days += 1
            else:
                jobs.append((date_text, history.copy(), day.copy()))

        history = pd.concat([history, day], ignore_index=True, copy=False)
        history = (
            history.sort_values(["ticker", "date"], kind="mergesort")
            .groupby("ticker", sort=False, group_keys=False)
            .tail(cfg.lookback_bars)
            .reset_index(drop=True)
        )

    workers = max(1, min(int(args.day_workers), len(jobs)))
    executor_class = ProcessPoolExecutor if args.day_executor == "process" else ThreadPoolExecutor
    with executor_class(max_workers=workers) as pool:
        futures = {
            pool.submit(
                evaluate_day,
                date_text,
                history_snapshot,
                day,
                cfg,
                universe,
                Path(args.marker_dir),
                daily_dir,
            ): date_text
            for date_text, history_snapshot, day in jobs
        }
        completed = 0
        for future in as_completed(futures):
            date_text, combined_day, audit_rows = future.result()
            completed += 1
            all_candidates.append(combined_day)
            slot_rows.extend(audit_rows)
            print(
                f"[prefilter multi-day new {completed:03d}/{len(jobs):03d} "
                f"resumed={resumed_days:03d}] date={date_text} "
                f"rows={len(combined_day)} eligible_min="
                f"{min(row['eligible_count'] for row in audit_rows)} elapsed={time.perf_counter()-started:.1f}s",
                flush=True,
            )

    combined = pd.concat(all_candidates, ignore_index=True)
    combined["_slot_sort"] = pd.to_datetime(combined["slot_ist"])
    combined = (
        combined.sort_values(["_slot_sort", "selection_rank", "ticker"], kind="mergesort")
        .drop(columns="_slot_sort")
        .reset_index(drop=True)
    )
    combined_path = output_dir / (
        f"hourly_candidates_{dates[0].replace('-', '')}_{dates[-1].replace('-', '')}_k{cfg.budget}.csv"
    )
    write_research_text(combined_path, combined.to_csv(index=False))
    slot_frame = pd.DataFrame(slot_rows).sort_values("slot_ist", kind="mergesort").reset_index(drop=True)
    slot_path = output_dir / "slot_audit.csv"
    write_research_text(slot_path, slot_frame.to_csv(index=False))

    marker_counts = {str(k): int(v) for k, v in slot_frame["marker_status"].value_counts().items()}
    eligible_recorded = pd.to_numeric(slot_frame["eligible_count"], errors="coerce").dropna()
    summary = {
        "schema_version": "eqidv2_experimental_prefilter_multi_day_replay_v1",
        "mode": "SHADOW_RESEARCH_ONLY",
        "production_consumption_allowed": False,
        "created_at_ist": datetime.now(IST).isoformat(),
        "requested_start_date": args.start_date,
        "requested_end_date": args.end_date,
        "actual_start_date": dates[0],
        "actual_end_date": dates[-1],
        "trading_days": len(dates),
        "slots_per_day": 7,
        "slots_completed": len(slot_frame),
        "budget": cfg.budget,
        "combined_candidate_rows": len(combined),
        "combined_candidates_path": str(combined_path.resolve()),
        "combined_candidates_file_sha256": file_sha256(combined_path),
        "combined_selection_sha256": combined_selection_sha256(combined),
        "config": cfg.to_dict(),
        "config_sha256": config_sha256(cfg),
        "source_data_dir": str(Path(args.data_dir).resolve()),
        "universe_manifest_path": str(Path(args.universe_manifest).resolve()),
        "universe_count": len(universe.symbols),
        "universe_sha256": universe.universe_sha256,
        "price_rows_loaded": load_stats["rows"],
        "bar_load": load_stats,
        "marker_status_counts": marker_counts,
        "exact_bar_symbol_count": {
            "min": int(slot_frame["exact_bar_symbol_count"].min()),
            "median": float(slot_frame["exact_bar_symbol_count"].median()),
            "max": int(slot_frame["exact_bar_symbol_count"].max()),
        },
        "eligible_count": (
            {
                "recorded_slots": len(eligible_recorded),
                "min": int(eligible_recorded.min()),
                "median": float(eligible_recorded.median()),
                "max": int(eligible_recorded.max()),
            }
            if not eligible_recorded.empty
            else {"recorded_slots": 0}
        ),
        "generation": {
            "resumed_validated_daily_files": resumed_days,
            "newly_computed_days": len(jobs),
            "day_executor": args.day_executor,
            "day_workers": workers,
        },
        "causality": {
            "future_price_rows_allowed": False,
            "slot_timestamp_convention": "end_labeled_completed_5m",
            "selection_activation_contract": "slot selection applies from next 5m candle",
        },
        "historical_universe_limitations": {
            "point_in_time_universe_available": False,
            "static_current_manifest_used": True,
            "survivorship_bias_risk": True,
            "marker_authority_is_reported_per_slot": True,
        },
        "total_seconds": time.perf_counter() - started,
    }
    summary_path = output_dir / "summary.json"
    write_research_text(summary_path, json.dumps(summary, indent=2, sort_keys=True, default=str) + "\n")
    print(json.dumps(summary, indent=2, sort_keys=True, default=str), flush=True)
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Causal multi-day experimental prefilter replay")
    parser.add_argument("--start-date", default="2026-02-04")
    parser.add_argument("--end-date", default="2026-08-03")
    parser.add_argument("--data-dir", type=Path, default=DEFAULT_DATA_DIR)
    parser.add_argument("--universe-manifest", type=Path, default=DEFAULT_UNIVERSE_MANIFEST)
    parser.add_argument("--marker-dir", type=Path, default=DEFAULT_MARKER_DIR)
    parser.add_argument("--budget", type=int, default=300)
    parser.add_argument("--max-workers", type=int, default=8)
    parser.add_argument("--day-workers", type=int, default=8)
    parser.add_argument("--day-executor", choices=("process", "thread"), default="process")
    parser.add_argument(
        "--resume-daily",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="reuse daily files only after strict schedule/count/rank/uniqueness validation",
    )
    parser.add_argument("--limit-days", type=int, help="explicit leading-date smoke-test limit")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    return parser


if __name__ == "__main__":
    raise SystemExit(run(build_parser().parse_args()))
