"""Merge disjoint V12 hourly-prefilter chunk backtests at full precision."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd


IST = "Asia/Kolkata"
MERGED_PIPELINE_FILES = (
    "historical_all_available_raw_candidates.csv",
    "historical_all_available_slot_summary.csv",
    "historical_all_available_ranked_raw_candidates.csv",
    "historical_all_available_v8_gated_candidates.csv",
    "historical_all_available_gated_candidates.csv",
    "historical_all_available_research_rejected_candidates.csv",
    "historical_all_available_pre_dedupe_live_candidates.csv",
    "historical_all_available_live_like_candidates.csv",
    "historical_all_available_entry_engine_raw_entries.csv",
    "historical_all_available_entry_engine_rejects.csv",
    "historical_all_available_entry_engine_signals.csv",
    "historical_all_available_selected_strategy_signals.csv",
    "historical_all_available_selected_strategy_rejects.csv",
    "historical_all_available_live_pipeline_slot_audit.csv",
    "historical_all_available_pipeline_stats_by_day.csv",
)


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def expected_dates(candidate_path: Path, start_date: str, end_date: str) -> list[str]:
    frame = pd.read_csv(candidate_path, usecols=["slot_ist"])
    days = pd.to_datetime(frame["slot_ist"], errors="raise").dt.strftime("%Y-%m-%d")
    return sorted({day for day in days if start_date <= day <= end_date})


def _filter_expected_dates(frame: pd.DataFrame, expected: set[str]) -> pd.DataFrame:
    for column in ("v11_source_day", "trade_date", "date", "v11_pipeline_day"):
        if column in frame.columns:
            values = frame[column].astype(str).str.slice(0, 10)
            if values.isin(expected).any():
                return frame.loc[values.isin(expected)].copy()
    return frame


def _merge_csvs(chunks: list[Path], filename: str, expected: set[str]) -> pd.DataFrame:
    frames = []
    for chunk in chunks:
        path = chunk / filename
        if path.exists() and path.stat().st_size:
            frames.append(pd.read_csv(path, low_memory=False))
    if not frames:
        return pd.DataFrame()
    return _filter_expected_dates(pd.concat(frames, ignore_index=True, sort=False), expected)


def run(args: argparse.Namespace) -> int:
    output = Path(args.output)
    if output.exists() and any(output.iterdir()):
        raise RuntimeError(f"refusing non-empty combined output directory: {output}")
    output.mkdir(parents=True, exist_ok=True)
    chunks = [Path(value) for value in args.chunk]
    candidates = Path(args.prefilter_candidates)
    expected = expected_dates(candidates, args.start_date, args.end_date)
    expected_set = set(expected)
    if not expected:
        raise RuntimeError("prefilter file contains no dates in requested range")

    scanned_dates: list[str] = []
    chunk_audits = []
    for chunk in chunks:
        dates_path = chunk / "historical_all_available_dates.csv"
        summary_path = chunk / "summary.txt"
        integration_path = chunk / "prefilter_integration_audit.json"
        for required in (dates_path, summary_path, integration_path, chunk / "trades.csv"):
            if not required.exists():
                raise RuntimeError(f"chunk is incomplete; missing {required}")
        scanned_dates.extend(pd.read_csv(dates_path)["date"].astype(str).tolist())
        chunk_audits.append(
            {
                "path": str(chunk.resolve()),
                "dates_sha256": file_sha256(dates_path),
                "summary_sha256": file_sha256(summary_path),
                "integration_audit_sha256": file_sha256(integration_path),
                "trades_sha256": file_sha256(chunk / "trades.csv"),
            }
        )

    missing = sorted(expected_set - set(scanned_dates))
    extra = sorted(set(scanned_dates) - expected_set)
    if missing:
        raise RuntimeError(f"chunk date coverage is missing prefilter dates: {missing}")

    trades = _merge_csvs(chunks, "trades.csv", expected_set)
    if not trades.empty:
        trades["_entry_sort"] = pd.to_datetime(trades["entry_time_v6"], errors="coerce")
        trades = trades.sort_values(["_entry_sort", "ticker", "setup"], kind="mergesort")
        trades = trades.drop(columns="_entry_sort").reset_index(drop=True)
        if "candidate_id" in trades and trades["candidate_id"].duplicated().any():
            raise RuntimeError("duplicate candidate_id across chunks")

    for filename in MERGED_PIPELINE_FILES:
        merged = _merge_csvs(chunks, filename, expected_set)
        merged.to_csv(output / filename, index=False)
    pd.DataFrame({"date": expected}).to_csv(
        output / "historical_all_available_dates.csv", index=False
    )
    accepted = pd.read_csv(output / "historical_all_available_pipeline_stats_by_day.csv")

    os.environ["EQIDV2_V12_FINAL_SETUP_CONF_MODULE"] = str(args.setup_conf_module)
    import avwap_5min_ID_v12_backtesting as v12

    v12._write_outputs(
        output,
        trades,
        accepted,
        str(output / "historical_all_available_raw_candidates.csv"),
        accepted_filename="historical_all_available_rules_used.csv",
        strategy_description=(
            "current v7 live scanner + v7 1-minute entry engine + PAPER_TRUE-style historical fill + "
            "V12 relaxed-retained setup profile + hourly K300 experimental prefilter + setup-specific "
            "1-minute exits"
        ),
    )

    calendar = pd.DataFrame({"date": expected})
    if trades.empty:
        pnl_by_day = pd.Series(dtype=float)
        count_by_day = pd.Series(dtype=int)
    else:
        pnl_by_day = trades.groupby(trades["trade_date"].astype(str))["v6_net_pnl_rs"].sum()
        count_by_day = trades.groupby(trades["trade_date"].astype(str)).size()
    calendar["trades"] = calendar["date"].map(count_by_day).fillna(0).astype(int)
    calendar["net_pnl_rs"] = calendar["date"].map(pnl_by_day).fillna(0.0)
    calendar["cum_pnl_rs"] = calendar["net_pnl_rs"].cumsum()
    calendar["drawdown_rs"] = calendar["cum_pnl_rs"] - calendar["cum_pnl_rs"].cummax().clip(lower=0.0)
    calendar.to_csv(output / "calendar_daily.csv", index=False)

    audit = {
        "schema_version": "eqidv2_v12_chunk_merge_audit_v1",
        "mode": "RESEARCH_ONLY",
        "production_consumption_allowed": False,
        "created_at_ist": datetime.now(ZoneInfo(IST)).isoformat(),
        "start_date": args.start_date,
        "end_date": args.end_date,
        "expected_prefilter_trading_dates": expected,
        "expected_prefilter_trading_day_count": len(expected),
        "scanned_dates": sorted(set(scanned_dates)),
        "extra_scanned_dates_excluded": extra,
        "missing_prefilter_dates": missing,
        "combined_trades": len(trades),
        "combined_trade_dates": int(trades["trade_date"].astype(str).nunique()) if not trades.empty else 0,
        "prefilter_candidates_path": str(candidates.resolve()),
        "prefilter_candidates_sha256": file_sha256(candidates),
        "setup_conf_module": args.setup_conf_module,
        "v12_source_path": str(Path(v12.__file__).resolve()),
        "v12_source_sha256": file_sha256(Path(v12.__file__)),
        "chunks": chunk_audits,
    }
    (output / "combined_audit.json").write_text(
        json.dumps(audit, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    (output / "inputs.txt").write_text(
        "mode=merged_disjoint_v12_hourly_prefilter_chunks\n"
        f"start_date={args.start_date}\nend_date={args.end_date}\n"
        f"prefilter_candidates={candidates.resolve()}\n"
        f"prefilter_candidates_sha256={file_sha256(candidates)}\n"
        f"setup_conf_module={args.setup_conf_module}\n"
        "prefilter_direction_policy=ticker_membership_only\n"
        "production_consumption_allowed=false\n",
        encoding="utf-8",
    )
    print(f"[v12 merge] output={output} trades={len(trades)} dates={len(expected)} extra={extra}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Merge disjoint V12 prefilter backtest chunks")
    parser.add_argument("--chunk", action="append", required=True)
    parser.add_argument("--prefilter-candidates", required=True)
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--setup-conf-module", default="final_setup_conf_v12")
    parser.add_argument("--output", required=True)
    return parser


if __name__ == "__main__":
    raise SystemExit(run(build_parser().parse_args()))
