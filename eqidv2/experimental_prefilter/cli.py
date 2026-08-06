from __future__ import annotations

import argparse
import json
import sys
import time
from dataclasses import replace
from datetime import datetime
from pathlib import Path
from typing import Any, Sequence
from zoneinfo import ZoneInfo

import pandas as pd

from .config import DEFAULT_BUDGET_GRID, PrefilterConfig
from .engine import annotate_budget_grid, build_features, rank_universe, select_candidates
from .evaluation import evaluate_budget_grid
from .io import (
    file_sha256,
    final_marker_path,
    load_bar_directory,
    load_bar_directory_through_slot,
    load_slot_marker,
    load_universe_manifest,
    validate_bar_snapshot,
    validate_slot_contract,
    write_research_text,
    write_shadow_outputs,
)
from .latency import profile_archives
from .manifest import build_shadow_manifest


DEFAULT_DATA_DIR = Path(r"C:\TradingData\eqidv2\stocks_indicators_5min_eq_live")
DEFAULT_UNIVERSE_MANIFEST = Path(r"C:\TradingData\eqidv2\runtime_status\feed_universe_5m.json")
DEFAULT_FINAL_MARKER_DIR = Path(r"C:\TradingData\eqidv2\slot_ready_5m")
DEFAULT_SCANNER_MARKER_DIR = Path(r"C:\TradingData\eqidv2\signal_discovery_v7_5mins_ID\json")
DEFAULT_REPLAY_UNIVERSE_MANIFEST = Path(r"C:\TradingData\eqidv2\runtime_status\feed_universe_kiteticker_5m.json")
DEFAULT_REPLAY_OUTPUT_DIR = Path(r"C:\TradingData\eqidv2_experiments\prefilter_hourly_replay")


def _budgets(value: str) -> tuple[int, ...]:
    budgets = sorted({int(item.strip()) for item in value.split(",") if item.strip()})
    if not budgets or any(item <= 0 for item in budgets):
        raise argparse.ArgumentTypeError("budgets must be comma-separated positive integers")
    return tuple(budgets)


def _config(args: argparse.Namespace) -> PrefilterConfig:
    config = PrefilterConfig.from_json(args.config) if getattr(args, "config", None) else PrefilterConfig()
    budget = getattr(args, "budget", None)
    budget_grid = getattr(args, "budgets", None)
    return config.with_overrides(budget=budget, budget_grid=budget_grid)


def _json_print(payload: Any) -> None:
    print(json.dumps(payload, indent=2, sort_keys=True, default=str))


def run_shadow_rank(args: argparse.Namespace) -> int:
    total_started = time.perf_counter()
    cfg = _config(args)
    universe_path = Path(args.universe_manifest)
    universe_before_sha = file_sha256(universe_path)
    universe = load_universe_manifest(universe_path)
    marker_path = Path(args.slot_marker) if args.slot_marker else final_marker_path(args.marker_dir, universe.slot_ist)
    marker_before = load_slot_marker(marker_path)
    validate_slot_contract(marker_before, universe)

    symbols = list(universe.symbols)
    if args.limit_symbols:
        symbols = symbols[: int(args.limit_symbols)]
    load_started = time.perf_counter()
    bars, load_stats = load_bar_directory(
        args.data_dir,
        symbols,
        lookback_bars=cfg.lookback_bars,
        max_workers=args.max_workers,
    )
    load_seconds = time.perf_counter() - load_started
    validate_bar_snapshot(bars, symbols, universe.slot_ist)

    # Re-read both control files after all Parquets.  Any change means the
    # snapshot crossed a publication boundary and must be discarded.
    marker_after = load_slot_marker(marker_path)
    universe_after = load_universe_manifest(universe_path)
    if marker_after.sha256 != marker_before.sha256:
        raise RuntimeError("final slot marker changed while bars were read")
    if file_sha256(universe_path) != universe_before_sha or universe_after != universe:
        raise RuntimeError("universe manifest changed while bars were read")
    validate_slot_contract(marker_after, universe_after)

    feature_started = time.perf_counter()
    features, feature_stats = build_features(bars, universe.slot_ist, cfg)
    feature_build_seconds = time.perf_counter() - feature_started
    ranking_started = time.perf_counter()
    ranked = rank_universe(features)
    ranking_seconds = time.perf_counter() - ranking_started
    annotation_started = time.perf_counter()
    ranked = annotate_budget_grid(ranked, cfg)
    annotation_seconds = time.perf_counter() - annotation_started
    selection_started = time.perf_counter()
    selected = select_candidates(ranked, cfg.budget, cfg)
    selection_seconds = time.perf_counter() - selection_started
    total_seconds = time.perf_counter() - total_started
    timing = {
        "bar_load": load_seconds,
        "feature_build": feature_build_seconds,
        "universe_rank": ranking_seconds,
        "budget_annotation": annotation_seconds,
        "final_selection": selection_seconds,
        "total": total_seconds,
    }
    manifest = build_shadow_manifest(
        slot_marker=marker_after,
        universe_manifest=universe_after,
        config=cfg,
        candidates=selected,
        full_ranking=ranked,
        load_stats=load_stats,
        timing=timing,
    )
    manifest["statistics"]["feature_build"] = {
        "input_rows": feature_stats.input_rows,
        "causal_rows": feature_stats.causal_rows,
        "universe_count": feature_stats.universe_count,
        "eligible_count": feature_stats.eligible_count,
        "rejected_count": feature_stats.rejected_count,
    }
    outputs = None
    if args.output_dir:
        outputs = write_shadow_outputs(args.output_dir, manifest, ranked)
    _json_print(
        {
            "mode": "SHADOW_RESEARCH_ONLY",
            "slot_ist": universe.slot_ist,
            "state": manifest["state"],
            "requested_symbols": len(symbols),
            "eligible_count": feature_stats.eligible_count,
            "selected_count": len(selected),
            "budget": cfg.budget,
            "timing_seconds": timing,
            "output_written": outputs is not None,
            "outputs": outputs,
            "top_candidates": manifest["candidates"][: min(10, len(manifest["candidates"]))],
        }
    )
    return 0


def _hourly_slots(date_text: str) -> list[pd.Timestamp]:
    day = pd.Timestamp(date_text)
    if pd.isna(day):
        raise ValueError("date is invalid")
    return [
        pd.Timestamp(f"{day.strftime('%Y-%m-%d')} {hour:02d}:20", tz="Asia/Kolkata")
        for hour in range(9, 16)
    ]


def run_hourly_replay(args: argparse.Namespace) -> int:
    """Replay frozen hourly candidate lists without importing V7 or V11."""

    total_started = time.perf_counter()
    cfg = _config(args)
    slots = _hourly_slots(args.date)
    universe = load_universe_manifest(args.universe_manifest)
    markers = [load_slot_marker(final_marker_path(args.marker_dir, slot.isoformat())) for slot in slots]
    for marker, slot in zip(markers, slots):
        if pd.Timestamp(marker.slot_ist) != slot:
            raise ValueError(f"marker slot mismatch: expected={slot.isoformat()} actual={marker.slot_ist}")
        if marker.tickers_expected != len(universe.symbols):
            raise ValueError(
                f"historical universe count mismatch at {slot.isoformat()}: "
                f"marker={marker.tickers_expected} manifest={len(universe.symbols)}"
            )
        if marker.universe_sha256 != universe.universe_sha256:
            raise ValueError(f"historical universe hash mismatch at {slot.isoformat()}")

    extra_session_bars = int((slots[-1] - slots[0]).total_seconds() // 300)
    history_bars = cfg.lookback_bars + extra_session_bars
    bars, load_stats = load_bar_directory_through_slot(
        args.data_dir,
        universe.symbols,
        slots[-1],
        history_bars=history_bars,
        max_workers=args.max_workers,
    )
    if load_stats.loaded_symbols != len(universe.symbols) or load_stats.missing_files or load_stats.read_errors:
        raise ValueError(
            "historical bar load is incomplete: "
            f"requested={load_stats.requested_symbols} loaded={load_stats.loaded_symbols} "
            f"missing={load_stats.missing_files} errors={load_stats.read_errors}"
        )

    all_candidates: list[pd.DataFrame] = []
    slot_summaries: list[dict[str, Any]] = []
    output_dir = Path(args.output_dir)
    for marker, slot in zip(markers, slots):
        slot_started = time.perf_counter()
        causal_bars = bars.loc[pd.to_datetime(bars["date"]).le(slot)].copy()
        validate_bar_snapshot(causal_bars, universe.symbols, slot.isoformat())
        feature_started = time.perf_counter()
        features, feature_stats = build_features(causal_bars, slot, cfg)
        feature_seconds = time.perf_counter() - feature_started
        rank_started = time.perf_counter()
        ranked = annotate_budget_grid(rank_universe(features), cfg)
        ranking_seconds = time.perf_counter() - rank_started
        selection_started = time.perf_counter()
        selected = select_candidates(ranked, cfg.budget, cfg)
        selection_seconds = time.perf_counter() - selection_started
        if len(selected) != min(cfg.budget, feature_stats.eligible_count):
            raise ValueError(
                f"selection count mismatch at {slot.isoformat()}: "
                f"selected={len(selected)} eligible={feature_stats.eligible_count} budget={cfg.budget}"
            )
        timing = {
            "shared_bar_load": load_stats.elapsed_seconds,
            "feature_build": feature_seconds,
            "universe_rank_and_budget_annotation": ranking_seconds,
            "final_selection": selection_seconds,
            "slot_total": time.perf_counter() - slot_started,
        }
        replay_universe = replace(universe, slot_ist=slot.isoformat())
        manifest = build_shadow_manifest(
            slot_marker=marker,
            universe_manifest=replay_universe,
            config=cfg,
            candidates=selected,
            full_ranking=ranked,
            load_stats=load_stats,
            timing=timing,
        )
        manifest["replay"] = {
            "type": "CAUSAL_HOURLY_5MIN_REPLAY",
            "research_date": args.date,
            "future_rows_allowed": False,
            "strategy_consumption_allowed": False,
        }
        manifest["statistics"]["feature_build"] = {
            "input_rows": feature_stats.input_rows,
            "causal_rows": feature_stats.causal_rows,
            "universe_count": feature_stats.universe_count,
            "eligible_count": feature_stats.eligible_count,
            "rejected_count": feature_stats.rejected_count,
        }
        outputs = write_shadow_outputs(output_dir, manifest, ranked)
        selected_copy = selected.copy()
        selected_copy["slot_ist"] = slot.isoformat()
        all_candidates.append(selected_copy)
        bucket_counts = {
            str(key): int(value)
            for key, value in selected["selection_bucket"].value_counts().sort_index().items()
        }
        side_counts = {
            str(key): int(value)
            for key, value in selected["primary_side"].value_counts().sort_index().items()
        }
        slot_summaries.append(
            {
                "slot_ist": slot.isoformat(),
                "feed_published_at_ist": marker.published_at_ist,
                "eligible_count": feature_stats.eligible_count,
                "selected_count": len(selected),
                "selection_bucket_counts": bucket_counts,
                "primary_side_counts": side_counts,
                "outputs": outputs,
                "timing_seconds": timing,
            }
        )

    combined = pd.concat(all_candidates, ignore_index=True)
    combined_columns = [
        column
        for column in (
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
        if column in combined.columns
    ]
    date_key = pd.Timestamp(args.date).strftime("%Y%m%d")
    combined_path = output_dir / f"hourly_candidates_{date_key}.csv"
    write_research_text(combined_path, combined.loc[:, combined_columns].to_csv(index=False))
    summary = {
        "schema_version": "eqidv2_experimental_prefilter_hourly_replay_v1",
        "mode": "SHADOW_RESEARCH_ONLY",
        "production_consumption_allowed": False,
        "created_at_ist": datetime.now(ZoneInfo("Asia/Kolkata")).isoformat(),
        "research_date": args.date,
        "source_timeframe": "5min",
        "schedule": [slot.isoformat() for slot in slots],
        "budget": cfg.budget,
        "universe_count": len(universe.symbols),
        "universe_sha256": universe.universe_sha256,
        "combined_candidates_path": str(combined_path.resolve()),
        "combined_candidate_rows": len(combined),
        "total_seconds": time.perf_counter() - total_started,
        "slots": slot_summaries,
    }
    summary_path = output_dir / f"hourly_replay_{date_key}_summary.json"
    write_research_text(summary_path, json.dumps(summary, indent=2, sort_keys=True, default=str) + "\n")
    _json_print(
        {
            "mode": summary["mode"],
            "research_date": args.date,
            "slots_completed": len(slot_summaries),
            "universe_count": len(universe.symbols),
            "budget": cfg.budget,
            "combined_candidate_rows": len(combined),
            "summary_path": str(summary_path.resolve()),
            "combined_candidates_path": str(combined_path.resolve()),
            "total_seconds": summary["total_seconds"],
        }
    )
    return 0


def run_latency_profile(args: argparse.Namespace) -> int:
    rows, summary = profile_archives(
        args.feed_marker_dir,
        args.scanner_marker_dir,
        date_prefix=args.date,
    )
    payload = {
        "schema_version": "eqidv2_experimental_prefilter_latency_v1",
        "date": args.date,
        "summary": summary,
        "slots": [row.__dict__ for row in rows],
    }
    if args.output:
        write_research_text(args.output, json.dumps(payload, indent=2, sort_keys=True) + "\n")
    _json_print(payload)
    return 0


def run_evaluate(args: argparse.Namespace) -> int:
    ranking = pd.read_csv(args.ranking)
    oracle = pd.read_csv(args.oracle)
    result = evaluate_budget_grid(
        ranking,
        oracle,
        args.budgets,
        universe_count=args.universe_count,
        pnl_column=args.pnl_column,
    )
    if args.output:
        write_research_text(args.output, result.to_csv(index=False))
    print(result.to_string(index=False))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="experimental-prefilter",
        description="Standalone V7/V11-independent shadow pre-filter research tools.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    rank = subparsers.add_parser("shadow-rank", help="rank one final, completed feed slot")
    rank.add_argument("--data-dir", type=Path, default=DEFAULT_DATA_DIR)
    rank.add_argument("--universe-manifest", type=Path, default=DEFAULT_UNIVERSE_MANIFEST)
    rank.add_argument("--marker-dir", type=Path, default=DEFAULT_FINAL_MARKER_DIR)
    rank.add_argument("--slot-marker", type=Path)
    rank.add_argument("--config", type=Path)
    rank.add_argument("--budget", type=int)
    rank.add_argument("--budgets", type=_budgets)
    rank.add_argument("--max-workers", type=int, default=8)
    rank.add_argument("--limit-symbols", type=int, help="explicit smoke-test subset; never for validation")
    rank.add_argument("--output-dir", type=Path, help="must contain 'experiment' or 'research'; omitted is dry-run")
    rank.set_defaults(func=run_shadow_rank)

    replay = subparsers.add_parser(
        "hourly-replay",
        help="causally replay 09:20 through 15:20 hourly candidate lists",
    )
    replay.add_argument("--date", required=True, help="historical date in YYYY-MM-DD format")
    replay.add_argument("--data-dir", type=Path, default=DEFAULT_DATA_DIR)
    replay.add_argument(
        "--universe-manifest",
        type=Path,
        default=DEFAULT_REPLAY_UNIVERSE_MANIFEST,
        help="manifest whose count/hash must match every archived final marker",
    )
    replay.add_argument("--marker-dir", type=Path, default=DEFAULT_FINAL_MARKER_DIR)
    replay.add_argument("--config", type=Path)
    replay.add_argument("--budget", type=int, default=300)
    replay.add_argument("--budgets", type=_budgets)
    replay.add_argument("--max-workers", type=int, default=8)
    replay.add_argument("--output-dir", type=Path, default=DEFAULT_REPLAY_OUTPUT_DIR)
    replay.set_defaults(func=run_hourly_replay)

    latency = subparsers.add_parser("profile-latency", help="profile archived feed/scanner markers")
    latency.add_argument("--feed-marker-dir", type=Path, default=DEFAULT_FINAL_MARKER_DIR)
    latency.add_argument("--scanner-marker-dir", type=Path, default=DEFAULT_SCANNER_MARKER_DIR)
    latency.add_argument("--date", help="YYYY-MM-DD filter")
    latency.add_argument("--output", type=Path, help="optional experimental/research JSON path")
    latency.set_defaults(func=run_latency_profile)

    evaluate = subparsers.add_parser("evaluate", help="compare ranking CSV with a full-universe oracle CSV")
    evaluate.add_argument("--ranking", type=Path, required=True)
    evaluate.add_argument("--oracle", type=Path, required=True)
    evaluate.add_argument("--budgets", type=_budgets, default=DEFAULT_BUDGET_GRID)
    evaluate.add_argument("--universe-count", type=int)
    evaluate.add_argument("--pnl-column", default="net_pnl_rs")
    evaluate.add_argument("--output", type=Path, help="optional experimental/research CSV path")
    evaluate.set_defaults(func=run_evaluate)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return int(args.func(args))
    except Exception as exc:
        print(f"ERROR: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 2
