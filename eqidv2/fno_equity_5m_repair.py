"""Repair mapped FnO cash-equity 5-minute stores from exact 1-minute bars.

This is a one-time maintenance tool. It rebuilds only equities in the current
FnO universe, keeps the full and slim store schemas separate, archives every
replaced parquet, and refuses partial 5-minute buckets.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

import fno_oi_common as common
import fno_oi_hybrid_data as hybrid
import trading_data_continous_run_historical_alltf_v3_parquet_stocksonly_5minonly as full_5m
import trading_data_continous_run_historical_alltf_v3_parquet_stocksonly_live_minimal as slim_5m


DEFAULT_BACKUP_ROOT = common.FNO_ROOT / "historical_repair" / "equity_5m_original"
DEFAULT_REPORT_PATH = common.FNO_ROOT / "historical_repair" / "equity_5m_repair.json"
DEFAULT_PRODUCTION_LIVE_ROOT = Path(
    os.getenv(
        "EQIDV2_FNO_V5_PRODUCTION_EQUITY_5M_DIR",
        r"C:\TradingData\eqidv2\stocks_indicators_5min_eq_live",
    )
)
BASE_COLUMNS = ("date", "open", "high", "low", "close", "volume")
QUALITY_COLUMNS = (
    "gap_filled",
    "source_1m_count",
    "provisional_stale",
    "opening_snapshot",
)
STRATEGY_SIGNAL_ENDS = ("09:25", "09:30", "09:35", "09:40", "09:45")


@dataclass(frozen=True)
class RepairTarget:
    root: Path
    profile: str


def _to_ist(values: pd.Series) -> pd.Series:
    parsed = pd.to_datetime(values, errors="coerce")
    if parsed.dt.tz is None:
        return parsed.dt.tz_localize(common.IST)
    return parsed.dt.tz_convert(common.IST)


def _mapped_symbols() -> list[str]:
    path = common.UNIVERSE_DIR / "latest_near_month.parquet"
    universe = pd.read_parquet(path)
    if "equity_symbol" not in universe.columns:
        raise RuntimeError(f"equity_symbol is missing from {path}")
    symbols = sorted(
        {
            str(value).strip().upper()
            for value in universe["equity_symbol"].dropna()
            if str(value).strip()
        }
    )
    if not symbols:
        raise RuntimeError(f"No mapped cash equities found in {path}")
    return symbols


def _universe_sha256(symbols: list[str]) -> str:
    return hashlib.sha256("\n".join(sorted(symbols)).encode("utf-8")).hexdigest()


def _path(root: Path, symbol: str, interval: str) -> Path:
    return root / f"{symbol}_stocks_indicators_{interval}.parquet"


def _archive(path: Path, backup_root: Path, run_id: str) -> str:
    if not path.exists():
        return ""
    backup = backup_root / run_id / path.parent.name / path.name
    backup.parent.mkdir(parents=True, exist_ok=True)
    if not backup.exists():
        try:
            os.link(path, backup)
        except OSError:
            shutil.copy2(path, backup)
    return str(backup)


def _opening_mask(frame: pd.DataFrame) -> pd.Series:
    ts = _to_ist(frame["date"])
    stored = pd.Series(False, index=frame.index)
    if "opening_snapshot" in frame.columns:
        stored = (
            pd.to_numeric(frame["opening_snapshot"], errors="coerce").fillna(0).ne(0)
            | frame["opening_snapshot"]
            .astype(str)
            .str.strip()
            .str.lower()
            .isin({"true", "yes", "on"})
        )
    return stored | ((ts.dt.hour == 9) & (ts.dt.minute == 15))


def audit_frame(frame: pd.DataFrame) -> dict[str, Any]:
    if frame.empty:
        return {
            "rows": 0,
            "sessions": 0,
            "adjacent_real_ohlcv_copies": 0,
            "untrusted_adjacent_real_ohlcv_copies": 0,
            "bad_real_source_count": 0,
            "strategy_slot_rows": {slot: 0 for slot in STRATEGY_SIGNAL_ENDS},
        }

    work = frame.copy()
    work["date"] = _to_ist(work["date"])
    work = work.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
    opening = _opening_mask(work)
    previous = work.shift(1)
    previous_opening = opening.shift(1, fill_value=False)
    adjacent = work["date"].dt.date.eq(previous["date"].dt.date)
    adjacent &= work["date"].sub(previous["date"]).eq(pd.Timedelta(minutes=5))
    adjacent &= ~opening & ~previous_opening
    for column in ("open", "high", "low", "close", "volume"):
        adjacent &= pd.to_numeric(work[column], errors="coerce").eq(
            pd.to_numeric(previous[column], errors="coerce")
        )

    real = ~opening
    if "source_1m_count" in work.columns:
        source_count = pd.to_numeric(work["source_1m_count"], errors="coerce")
        bad_source = real & source_count.ne(5)
        previous_source_count = source_count.shift(1)
        untrusted_adjacent = adjacent & ~(
            source_count.eq(5) & previous_source_count.eq(5)
        )
    else:
        bad_source = pd.Series(False, index=work.index)
        untrusted_adjacent = adjacent
    hhmm = work["date"].dt.strftime("%H:%M")
    return {
        "rows": int(len(work)),
        "sessions": int(work["date"].dt.date.nunique()),
        "first": work["date"].min().isoformat(),
        "last": work["date"].max().isoformat(),
        "opening_snapshots": int(opening.sum()),
        "adjacent_real_ohlcv_copies": int(adjacent.sum()),
        "untrusted_adjacent_real_ohlcv_copies": int(untrusted_adjacent.sum()),
        "bad_real_source_count": int(bad_source.sum()),
        "strategy_slot_rows": {
            slot: int((real & hhmm.eq(slot)).sum()) for slot in STRATEGY_SIGNAL_ENDS
        },
    }


def _read_existing_audit(path: Path) -> dict[str, Any]:
    if not path.exists():
        return audit_frame(pd.DataFrame())
    frame = pd.read_parquet(path)
    required = list(BASE_COLUMNS)
    optional = [
        column
        for column in QUALITY_COLUMNS
        if column in frame.columns
    ]
    return audit_frame(frame[required + optional])


def _target_cutoff(
    target_path: Path,
    source_first: pd.Timestamp,
    source_last: pd.Timestamp,
    *,
    profile: str,
    live_retention_days: int,
) -> pd.Timestamp:
    source_floor = source_first.normalize() + pd.Timedelta(hours=9, minutes=15)
    if profile == "slim":
        retention_floor = source_last.normalize() - pd.Timedelta(days=live_retention_days)
        retention_floor += pd.Timedelta(hours=9, minutes=15)
        return max(source_floor, retention_floor)
    return source_floor


def _append_opening_snapshots(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    out["date"] = _to_ist(out["date"])
    out["opening_snapshot"] = False
    ts = out["date"]
    first_real = out.loc[(ts.dt.hour == 9) & (ts.dt.minute == 20)].copy()
    if first_real.empty:
        return out.sort_values("date").reset_index(drop=True)
    first_real["date"] = first_real["date"] - pd.Timedelta(minutes=5)
    first_real["opening_snapshot"] = True
    first_real["source_1m_count"] = 0
    first_real["gap_filled"] = 0
    first_real["provisional_stale"] = 0
    combined = pd.concat([out, first_real], ignore_index=True, sort=False)
    if "AVWAP" in combined.columns:
        combined.loc[combined["opening_snapshot"].eq(True), "AVWAP"] = np.nan
    return (
        combined.drop_duplicates("date", keep="last")
        .sort_values("date", kind="stable")
        .reset_index(drop=True)
    )


def build_profile(real_bars: pd.DataFrame, profile: str) -> pd.DataFrame:
    columns = list(BASE_COLUMNS) + [
        "gap_filled",
        "source_1m_count",
        "provisional_stale",
        "opening_snapshot",
    ]
    work = real_bars[columns].copy()
    work["date"] = _to_ist(work["date"])
    if profile == "full":
        featured = full_5m._compute_features_5m(work)
        stoch_k, stoch_d = slim_5m.calculate_stochastic_slow(
            featured, 14, 3, 3, "sma"
        )
        featured["Stoch_%K"] = stoch_k
        featured["Stoch_%D"] = stoch_d
        featured = full_5m._downcast_numeric_columns(featured)
    elif profile == "slim":
        featured = slim_5m._compute_common_features(work, "5min")
        featured = slim_5m._downcast_numeric_columns(featured)
    else:
        raise ValueError(f"Unknown profile: {profile}")
    return _append_opening_snapshots(featured)


def _load_real_bars(symbol: str, one_minute_root: Path) -> pd.DataFrame:
    minute = hybrid.load_equity_one_minute(symbol, one_minute_root)
    if minute.empty:
        raise RuntimeError(f"No 1-minute cash-equity data for {symbol}")
    real = hybrid.aggregate_equity_one_minute_to_five_minute(minute)
    if real.empty:
        raise RuntimeError(f"No exact five-row 1-minute buckets for {symbol}")
    real = real.drop(columns=["ts"], errors="ignore")
    real["date"] = _to_ist(real["date"])
    return real


def _repair_symbol(
    symbol: str,
    *,
    one_minute_root: Path,
    targets: list[RepairTarget],
    backup_root: Path,
    run_id: str,
    live_retention_days: int,
) -> dict[str, Any]:
    real = _load_real_bars(symbol, one_minute_root)
    source_first = real["date"].min()
    source_last = real["date"].max()
    profiles = {target.profile: build_profile(real, target.profile) for target in targets}
    outputs: list[dict[str, Any]] = []

    for target in targets:
        target_path = _path(target.root, symbol, "5min")
        before = _read_existing_audit(target_path)
        cutoff = _target_cutoff(
            target_path,
            source_first,
            source_last,
            profile=target.profile,
            live_retention_days=live_retention_days,
        )
        rebuilt = profiles[target.profile]
        rebuilt = rebuilt.loc[_to_ist(rebuilt["date"]).ge(cutoff)].reset_index(drop=True)
        if rebuilt.empty:
            raise RuntimeError(f"{symbol} produced no {target.profile} rows after {cutoff}")
        after = audit_frame(rebuilt)
        if (
            after["bad_real_source_count"]
            or after["untrusted_adjacent_real_ohlcv_copies"]
        ):
            raise RuntimeError(f"{symbol} failed causal {target.profile} audit: {after}")
        backup = _archive(target_path, backup_root, run_id)
        common.atomic_write_parquet(rebuilt, target_path)
        persisted = _read_existing_audit(target_path)
        if persisted != after:
            raise RuntimeError(f"{symbol} persisted {target.profile} audit changed")
        outputs.append(
            {
                "profile": target.profile,
                "path": str(target_path),
                "cutoff": cutoff.isoformat(),
                "backup": backup,
                "before": before,
                "after": after,
            }
        )
    return {
        "symbol": symbol,
        "source_rows": int(len(real)),
        "source_first": source_first.isoformat(),
        "source_last": source_last.isoformat(),
        "outputs": outputs,
    }


def run(
    symbols: list[str],
    *,
    one_minute_root: Path,
    backtest_root: Path,
    live_root: Path,
    backup_root: Path,
    workers: int,
    live_retention_days: int,
    report_path: Path,
) -> dict[str, Any]:
    mapped = _mapped_symbols()
    selected = sorted({value.strip().upper() for value in symbols if value.strip()})
    if selected:
        unknown = sorted(set(selected) - set(mapped))
        if unknown:
            raise RuntimeError(f"Symbols are not in the current FnO map: {', '.join(unknown)}")
    else:
        selected = mapped

    if backtest_root.resolve() == live_root.resolve():
        raise RuntimeError(
            "Backtest full-profile and production slim-profile roots must be distinct: "
            f"{backtest_root.resolve()}"
        )

    run_id = common.now_ist().strftime("%Y%m%d_%H%M%S")
    targets = [RepairTarget(backtest_root, "full"), RepairTarget(live_root, "slim")]
    outcomes: list[dict[str, Any]] = []
    failures: list[dict[str, str]] = []
    with ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
        futures = {
            pool.submit(
                _repair_symbol,
                symbol,
                one_minute_root=one_minute_root,
                targets=targets,
                backup_root=backup_root,
                run_id=run_id,
                live_retention_days=live_retention_days,
            ): symbol
            for symbol in selected
        }
        for future in as_completed(futures):
            symbol = futures[future]
            try:
                outcomes.append(future.result())
                print(f"[OK] {symbol}", flush=True)
            except Exception as exc:
                failures.append({"symbol": symbol, "error": str(exc)})
                print(f"[FAILED] {symbol}: {exc}", flush=True)

    payload = {
        "generated_at_ist": common.now_ist().isoformat(),
        "run_id": run_id,
        "data_contract": hybrid.DATA_CONTRACT_VERSION,
        "mapped_symbol_count": len(mapped),
        "selected_symbol_count": len(selected),
        "mapped_equity_sha256": _universe_sha256(mapped),
        "completed": len(outcomes),
        "failed": len(failures),
        "failures": sorted(failures, key=lambda row: row["symbol"]),
        "outcomes": sorted(outcomes, key=lambda row: row["symbol"]),
    }
    common.atomic_write_json(report_path, payload)
    return payload


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--symbols", default="", help="Comma-separated subset; blank means all mapped equities.")
    parser.add_argument("--one-minute-root", type=Path, default=hybrid.DEFAULT_BACKTEST_EQUITY_1M_DIR)
    parser.add_argument("--backtest-root", type=Path, default=hybrid.DEFAULT_BACKTEST_EQUITY_5M_DIR)
    parser.add_argument("--live-root", type=Path, default=DEFAULT_PRODUCTION_LIVE_ROOT)
    parser.add_argument("--backup-root", type=Path, default=DEFAULT_BACKUP_ROOT)
    parser.add_argument("--report-path", type=Path, default=DEFAULT_REPORT_PATH)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--live-retention-days", type=int, default=10)
    args = parser.parse_args(argv)
    payload = run(
        args.symbols.split(","),
        one_minute_root=args.one_minute_root,
        backtest_root=args.backtest_root,
        live_root=args.live_root,
        backup_root=args.backup_root,
        workers=args.workers,
        live_retention_days=max(1, args.live_retention_days),
        report_path=args.report_path,
    )
    print(
        json.dumps(
            {
                "completed": payload["completed"],
                "failed": payload["failed"],
                "report": str(args.report_path),
            },
            indent=2,
        ),
        flush=True,
    )
    return 1 if payload["failed"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
