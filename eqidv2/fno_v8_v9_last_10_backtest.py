"""Repair and replay the latest ten completed FnO sessions on V8 and V9.

This one-shot operational pipeline is intentionally pinned to the user's
request-time window (2026-08-12 through 2026-08-25).  August 26 was still an
open session when the request was made, and the research launchers themselves
are pinned to the static 26AUG futures universe.

The pipeline:

1. fetches the full requested cash-equity 1-minute window (not merely the
   incremental warm-up tail),
2. backfills the retained 26AUG futures contracts through expiry,
3. audits the exact V8/V9 source grids,
4. creates one physical source snapshot shared by both launchers,
5. runs V8-Combined and V9-Honest with identical costs, and
6. validates both provenance manifests.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import traceback
from datetime import time
from pathlib import Path
from typing import Any, Sequence

import numpy as np
import pandas as pd

import fno_oi_common as common


BASE_DIR = Path(__file__).resolve().parent
FROZEN_UNIVERSE = common.UNIVERSE_DIR / "near_month_2026-08-11.parquet"
EQUITY_1M_ROOT = Path(r"C:\TradingData\eqidv2\stocks_indicators_1min_eq")
PIPELINE_ROOT = (
    common.FNO_ROOT / "strategy_research" / "v8_v9_last_10_backtests"
)
DEFAULT_FROM_DAY = "2026-08-12"
DEFAULT_THROUGH_DAY = "2026-08-25"
MARKET_CLOSE = time(15, 30)


def _log(message: str) -> None:
    print(f"[V8V9-10D] {message}", flush=True)


def _run_command(
    stage: str,
    arguments: Sequence[str | Path],
    *,
    allow_failure: bool = False,
) -> tuple[int, list[str]]:
    command = [str(value) for value in arguments]
    _log(f"START {stage}: {' '.join(command)}")
    environment = os.environ.copy()
    environment.setdefault("EQIDV2_RUNTIME_ROOT", r"C:\TradingData\eqidv2")
    environment["PYTHONUNBUFFERED"] = "1"
    environment["PYTHONIOENCODING"] = "utf-8"
    process = subprocess.Popen(
        command,
        cwd=BASE_DIR,
        env=environment,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
        bufsize=1,
    )
    output: list[str] = []
    assert process.stdout is not None
    for raw_line in process.stdout:
        line = raw_line.rstrip("\r\n")
        output.append(line)
        print(f"[{stage}] {line}", flush=True)
    exit_code = int(process.wait())
    _log(f"END {stage}: exit={exit_code}")
    if exit_code and not allow_failure:
        raise RuntimeError(f"{stage} failed with exit code {exit_code}")
    return exit_code, output


def _mapped_universe() -> pd.DataFrame:
    universe = pd.read_parquet(FROZEN_UNIVERSE)
    required = {
        "equity_symbol",
        "futures_tradingsymbol",
        "equity_instrument_token",
        "futures_instrument_token",
    }
    missing = sorted(required - set(universe.columns))
    if missing:
        raise RuntimeError(f"Frozen universe is missing columns: {missing}")
    mapped = universe.dropna(
        subset=[
            "equity_symbol",
            "futures_tradingsymbol",
            "equity_instrument_token",
            "futures_instrument_token",
        ]
    ).copy()
    mapped["equity_symbol"] = (
        mapped["equity_symbol"].astype(str).str.strip().str.upper()
    )
    mapped["futures_tradingsymbol"] = (
        mapped["futures_tradingsymbol"].astype(str).str.strip().str.upper()
    )
    mapped = mapped.drop_duplicates("equity_symbol", keep="last").reset_index(
        drop=True
    )
    if len(mapped) != 208:
        raise RuntimeError(f"Expected 208 frozen mapped stocks, observed {len(mapped)}")
    return mapped


def _valid_ohlcv(frame: pd.DataFrame) -> pd.Series:
    numeric = frame[["open", "high", "low", "close", "volume"]].apply(
        pd.to_numeric, errors="coerce"
    )
    prices = numeric[["open", "high", "low", "close"]]
    return pd.Series(
        np.isfinite(prices.to_numpy(dtype=float)).all(axis=1)
        & prices.gt(0).all(axis=1).to_numpy(bool)
        & numeric["high"].ge(numeric[["open", "close"]].max(axis=1)).to_numpy(bool)
        & numeric["low"].le(numeric[["open", "close"]].min(axis=1)).to_numpy(bool)
        & numeric["high"].ge(numeric["low"]).to_numpy(bool)
        & np.isfinite(numeric["volume"].to_numpy(dtype=float))
        & numeric["volume"].ge(0).to_numpy(bool),
        index=frame.index,
        dtype=bool,
    )


def _source_readiness(from_day: str, through_day: str) -> dict[str, Any]:
    mapped = _mapped_universe()
    sessions = [stamp.date() for stamp in pd.bdate_range(from_day, through_day)]
    range_start = pd.Timestamp(f"{from_day} 09:00", tz=common.IST)
    range_end = pd.Timestamp(f"{through_day} 15:31", tz=common.IST)
    equity_complete: dict[str, int] = {day.isoformat(): 0 for day in sessions}
    futures_complete: dict[str, int] = {day.isoformat(): 0 for day in sessions}
    combined_complete: dict[str, int] = {day.isoformat(): 0 for day in sessions}
    incomplete_samples: dict[str, list[str]] = {
        day.isoformat(): [] for day in sessions
    }

    for row in mapped.itertuples(index=False):
        equity_symbol = str(row.equity_symbol)
        futures_symbol = str(row.futures_tradingsymbol)
        equity_path = EQUITY_1M_ROOT / (
            f"{equity_symbol}_stocks_indicators_1min.parquet"
        )
        futures_path = common.raw_contract_path(futures_symbol)

        equity_valid_stamps: set[pd.Timestamp] = set()
        if equity_path.exists():
            try:
                equity = pd.read_parquet(
                    equity_path,
                    columns=["date", "open", "high", "low", "close", "volume"],
                    filters=[
                        ("date", ">=", range_start),
                        ("date", "<=", range_end),
                    ],
                )
                equity["_ts"] = common._to_ist(equity["date"])
                equity_valid_stamps = set(equity.loc[_valid_ohlcv(equity), "_ts"])
            except Exception as exc:
                _log(f"WARN unreadable equity source {equity_symbol}: {exc}")

        futures_valid_stamps: set[pd.Timestamp] = set()
        if futures_path.exists():
            try:
                futures = pd.read_parquet(
                    futures_path,
                    columns=["timestamp", "oi", "quality_state"],
                    filters=[
                        ("timestamp", ">=", range_start),
                        ("timestamp", "<=", range_end),
                    ],
                )
                futures["_ts"] = common._to_ist(futures["timestamp"])
                oi = pd.to_numeric(futures["oi"], errors="coerce")
                valid = (
                    futures["quality_state"]
                    .astype(str)
                    .str.strip()
                    .str.upper()
                    .eq("VALID")
                    & oi.gt(0)
                    & np.isfinite(oi.to_numpy(dtype=float))
                )
                futures_valid_stamps = set(futures.loc[valid, "_ts"])
            except Exception as exc:
                _log(f"WARN unreadable futures source {futures_symbol}: {exc}")

        for session_day in sessions:
            day_key = session_day.isoformat()
            expected_equity = set(
                pd.date_range(
                    pd.Timestamp(f"{day_key} 09:16", tz=common.IST),
                    pd.Timestamp(f"{day_key} 15:30", tz=common.IST),
                    freq="1min",
                )
            )
            expected_futures = {
                pd.Timestamp(f"{day_key} {clock}", tz=common.IST)
                for clock in ("09:20", "09:25", "09:30", "09:35", "09:40", "09:45")
            }
            equity_ok = expected_equity.issubset(equity_valid_stamps)
            futures_ok = expected_futures.issubset(futures_valid_stamps)
            if equity_ok:
                equity_complete[day_key] += 1
            if futures_ok:
                futures_complete[day_key] += 1
            if equity_ok and futures_ok:
                combined_complete[day_key] += 1
            elif len(incomplete_samples[day_key]) < 12:
                incomplete_samples[day_key].append(
                    f"{equity_symbol}:equity={equity_ok},futures={futures_ok}"
                )

    return {
        "schema_version": "fno_v8_v9_last_10_source_readiness_v1",
        "generated_at_ist": common.now_ist().isoformat(),
        "from_day": from_day,
        "through_day": through_day,
        "session_dates": [day.isoformat() for day in sessions],
        "mapped_symbol_count": int(len(mapped)),
        "equity_complete_by_session": equity_complete,
        "futures_complete_by_session": futures_complete,
        "combined_complete_by_session": combined_complete,
        "source_complete": all(
            count == len(mapped) for count in combined_complete.values()
        ),
        "incomplete_samples": incomplete_samples,
    }


def _extract_path(lines: Sequence[str], pattern: str, label: str) -> Path:
    matcher = re.compile(pattern)
    for line in reversed(lines):
        match = matcher.search(line.strip())
        if match:
            path = Path(match.group(1).strip()).resolve()
            if path.exists():
                return path
            raise RuntimeError(f"{label} path was printed but does not exist: {path}")
    raise RuntimeError(f"Could not extract {label} path from command output")


def _provenance_summary(run_dir: Path) -> dict[str, Any]:
    provenance_path = run_dir / "provenance.json"
    payload = json.loads(provenance_path.read_text(encoding="utf-8"))
    return {
        "run_dir": str(run_dir),
        "report_path": str(run_dir / "report.md"),
        "provenance_path": str(provenance_path),
        "backtest_input_fingerprint": payload.get("backtest_input_fingerprint"),
        "parameters": payload.get("parameters"),
        "results": payload.get("results"),
    }


def run_pipeline(args: argparse.Namespace) -> dict[str, Any]:
    now = common.now_ist()
    if (
        common.is_trading_day(now.date(), common.load_holidays())
        and now.time() <= MARKET_CLOSE
        and not args.allow_market_hours
    ):
        raise RuntimeError(
            "Historical repair is blocked until after 15:30 IST so it cannot "
            "compete with the live FnO feed"
        )
    if args.from_day != DEFAULT_FROM_DAY or args.through_day != DEFAULT_THROUGH_DAY:
        _log(
            "WARN overriding the request-time ten-session window; confirm the "
            "static 26AUG universe is still appropriate"
        )

    job_id = f"run_{common.now_ist().strftime('%Y%m%dT%H%M%S%f%z')}"
    job_dir = PIPELINE_ROOT / job_id
    job_dir.mkdir(parents=True, exist_ok=False)
    result: dict[str, Any] = {
        "schema_version": "fno_v8_v9_last_10_pipeline_v1",
        "job_id": job_id,
        "job_dir": str(job_dir),
        "started_at_ist": common.now_ist().isoformat(),
        "status": "RUNNING",
        "from_day": args.from_day,
        "through_day": args.through_day,
        "economics": {
            "cost_bps": args.cost_bps,
            "slippage_bps": args.slippage_bps,
            "square_off": "15:30",
            "eod_policy": "EXACT_SQUARE_OFF",
        },
    }

    try:
        before = _source_readiness(args.from_day, args.through_day)
        common.atomic_write_json(job_dir / "source_readiness_before.json", before)
        result["source_readiness_before"] = before

        if not all(
            value == before["mapped_symbol_count"]
            for value in before["futures_complete_by_session"].values()
        ):
            futures_arguments: list[str | Path] = [
                sys.executable,
                "-u",
                BASE_DIR / "fno_oi_backfill_5min.py",
                "--contract-months",
                "2026-08",
                "--from-date",
                args.from_day,
                "--to-date",
                args.through_day,
                "--max-apps",
                str(args.max_apps),
            ]
            if args.allow_market_hours:
                futures_arguments.append("--allow-market-hours")
            _run_command(
                "AUG-FUTURES-5M",
                futures_arguments,
            )
        else:
            _log("AUG futures early OI grids are already complete; skipping fetch")

        if not all(
            value == before["mapped_symbol_count"]
            for value in before["equity_complete_by_session"].values()
        ):
            cash_arguments = [
                sys.executable,
                "-u",
                BASE_DIR / "fno_equity_1m_backfill.py",
                "--all-mapped",
                "--universe-path",
                FROZEN_UNIVERSE,
                "--start-date",
                args.from_day,
                "--end-date",
                args.through_day,
                "--force-window",
                "--backup-root",
                common.FNO_ROOT
                / "historical_repair"
                / "equity_1m_original"
                / job_id,
            ]
            cash_exit, _ = _run_command(
                "EQUITY-1M", cash_arguments, allow_failure=True
            )
            if cash_exit:
                _log("Cash backfill had failures; retrying the complete window once")
                _run_command("EQUITY-1M-RETRY", cash_arguments)
        else:
            _log("Cash one-minute grids are already complete; skipping fetch")

        after = _source_readiness(args.from_day, args.through_day)
        common.atomic_write_json(job_dir / "source_readiness_after.json", after)
        result["source_readiness_after"] = after
        empty_sessions = [
            day
            for day, count in after["combined_complete_by_session"].items()
            if int(count) == 0
        ]
        if empty_sessions:
            raise RuntimeError(
                "No exact-ready frozen-universe symbols remain for sessions: "
                + ", ".join(empty_sessions)
            )

        _, snapshot_lines = _run_command(
            "SNAPSHOT",
            [
                sys.executable,
                "-u",
                BASE_DIR / "fno_v8_combined_best_per_leg_backtest.py",
                "snapshot",
            ],
        )
        snapshot_manifest = _extract_path(
            snapshot_lines, r"^(.+manifest\.json)$", "source snapshot"
        )
        result["source_snapshot_manifest"] = str(snapshot_manifest)

        common_run_arguments = [
            "run",
            "--source-snapshot",
            snapshot_manifest,
            "--from-day",
            args.from_day,
            "--through-day",
            args.through_day,
            "--cost-bps",
            str(args.cost_bps),
            "--slippage-bps",
            str(args.slippage_bps),
            "--square-off",
            "15:30",
            "--eod-policy",
            "EXACT_SQUARE_OFF",
            "--rebuild-cache",
        ]

        _, v8_lines = _run_command(
            "V8-RUN",
            [
                sys.executable,
                "-u",
                BASE_DIR / "fno_v8_combined_best_per_leg_backtest.py",
                *common_run_arguments,
            ],
        )
        v8_run_dir = _extract_path(
            v8_lines, r"^\[V8\]\[RUN\]\s+(.+)$", "V8 run directory"
        )
        _run_command(
            "V8-VALIDATE",
            [
                sys.executable,
                "-u",
                BASE_DIR / "fno_v8_combined_best_per_leg_backtest.py",
                "validate",
                "--provenance",
                v8_run_dir / "provenance.json",
            ],
        )
        result["v8"] = _provenance_summary(v8_run_dir)

        _, v9_lines = _run_command(
            "V9-RUN",
            [
                sys.executable,
                "-u",
                BASE_DIR / "fno_v9_honest_v8_backtest.py",
                *common_run_arguments,
            ],
        )
        v9_run_dir = _extract_path(
            v9_lines, r"^\[V8\]\[RUN\]\s+(.+)$", "V9 run directory"
        )
        _run_command(
            "V9-VALIDATE",
            [
                sys.executable,
                "-u",
                BASE_DIR / "fno_v9_honest_v8_backtest.py",
                "validate",
                "--provenance",
                v9_run_dir / "provenance.json",
            ],
        )
        result["v9"] = _provenance_summary(v9_run_dir)
        result["status"] = "SUCCESS"
        return result
    except Exception as exc:
        result["status"] = "FAILED"
        result["error"] = f"{type(exc).__name__}: {exc}"
        result["traceback"] = traceback.format_exc()
        raise
    finally:
        result["finished_at_ist"] = common.now_ist().isoformat()
        common.atomic_write_json(job_dir / "result.json", result)
        PIPELINE_ROOT.mkdir(parents=True, exist_ok=True)
        common.atomic_write_json(PIPELINE_ROOT / "latest.json", result)
        _log(f"RESULT {job_dir / 'result.json'} status={result['status']}")


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--from-day", default=DEFAULT_FROM_DAY)
    parser.add_argument("--through-day", default=DEFAULT_THROUGH_DAY)
    parser.add_argument("--cost-bps", type=float, default=15.0)
    parser.add_argument("--slippage-bps", type=float, default=0.0)
    parser.add_argument("--max-apps", type=int, default=8)
    parser.add_argument("--allow-market-hours", action="store_true")
    parser.add_argument(
        "--audit-only",
        action="store_true",
        help="Print the current exact-grid readiness without fetching or running.",
    )
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    if args.audit_only:
        print(
            json.dumps(
                _source_readiness(args.from_day, args.through_day),
                indent=2,
                sort_keys=True,
            ),
            flush=True,
        )
        return 0
    result = run_pipeline(args)
    print(json.dumps(result, indent=2, sort_keys=True, default=str), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
