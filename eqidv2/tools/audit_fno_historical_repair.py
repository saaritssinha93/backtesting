"""Audit recoverable FnO equity/futures history repairs without changing data."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import fno_oi_common as common


EQUITY_COLUMNS = ("date", "open", "high", "low", "close", "volume")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _ist(series: pd.Series) -> pd.Series:
    values = pd.to_datetime(series, errors="coerce")
    if values.dt.tz is None:
        return values.dt.tz_localize(common.IST)
    return values.dt.tz_convert(common.IST)


def _sessions(start: date, end: date) -> list[date]:
    holidays = common.load_holidays()
    days: list[date] = []
    cursor = start
    while cursor <= end:
        if common.is_trading_day(cursor, holidays):
            days.append(cursor)
        cursor += timedelta(days=1)
    return days


def _equity_audit(
    current_root: Path,
    backup_root: Path,
    today_backup_root: Path | None,
    start: date,
    end: date,
) -> tuple[dict[str, Any], pd.DataFrame]:
    session_days = _sessions(start, end)
    observed_session_days: set[date] = set()
    records: list[dict[str, Any]] = []
    for current_path in sorted(current_root.glob("*_stocks_indicators_1min.parquet")):
        symbol = current_path.name.removesuffix("_stocks_indicators_1min.parquet")
        backup_path = backup_root / current_path.name
        if not backup_path.exists():
            continue
        current = pd.read_parquet(current_path, columns=list(EQUITY_COLUMNS))
        before = pd.read_parquet(backup_path, columns=list(EQUITY_COLUMNS))
        current["date"] = _ist(current["date"])
        before["date"] = _ist(before["date"])
        lower = pd.Timestamp(start, tz=common.IST)
        upper = pd.Timestamp(end + timedelta(days=1), tz=common.IST)
        current = current.loc[current["date"].between(lower, upper, inclusive="left")]
        before = before.loc[before["date"].between(lower, upper, inclusive="left")]
        observed_session_days.update(current["date"].dt.date.unique())
        current = current.drop_duplicates("date", keep="last").set_index("date").sort_index()
        before = before.drop_duplicates("date", keep="last").set_index("date").sort_index()
        added = current.index.difference(before.index)
        removed = before.index.difference(current.index)
        overlap = current.index.intersection(before.index)
        changed = 0
        changed_by_column = {column: 0 for column in EQUITY_COLUMNS[1:]}
        if len(overlap):
            left = current.loc[overlap, list(EQUITY_COLUMNS[1:])].apply(
                pd.to_numeric, errors="coerce"
            )
            right = before.loc[overlap, list(EQUITY_COLUMNS[1:])].apply(
                pd.to_numeric, errors="coerce"
            )
            unequal = ~np.isclose(
                left.to_numpy(dtype=float),
                right.to_numpy(dtype=float),
                rtol=1e-10,
                atol=1e-10,
                equal_nan=True,
            )
            changed = int(np.any(unequal, axis=1).sum())
            changed_by_column = {
                column: int(unequal[:, position].sum())
                for position, column in enumerate(EQUITY_COLUMNS[1:])
            }

        counts = current.groupby(current.index.date).size()
        expected_counts = [int(counts.get(day, 0)) for day in session_days]
        record: dict[str, Any] = {
            "symbol": symbol,
            "rows_before_window": int(len(before)),
            "rows_after_window": int(len(current)),
            "timestamps_added": int(len(added)),
            "timestamps_removed": int(len(removed)),
            "overlap_ohlcv_changed": changed,
            **{
                f"overlap_{column}_changed": count
                for column, count in changed_by_column.items()
            },
            "complete_sessions_to_1515": int(sum(value >= 360 for value in expected_counts)),
            "complete_sessions_to_1530": int(sum(value >= 375 for value in expected_counts)),
            "session_rows_min": int(min(expected_counts, default=0)),
            "session_rows_max": int(max(expected_counts, default=0)),
            "aug27_rows_after": int(counts.get(date(2026, 8, 27), 0)),
            "aug27_last_after": str(current.index.max()) if not current.empty else "",
        }
        if today_backup_root is not None:
            today_backup = today_backup_root / current_path.name
            if today_backup.exists():
                old_today = pd.read_parquet(today_backup, columns=["date"])
                old_dates = _ist(old_today["date"])
                record["aug27_rows_before_targeted_repair"] = int(
                    (old_dates.dt.date == date(2026, 8, 27)).sum()
                )
            else:
                record["aug27_rows_before_targeted_repair"] = 0
        records.append(record)

    detail = pd.DataFrame(records).sort_values("symbol", kind="stable")
    observed_exchange_sessions = sorted(
        day for day in observed_session_days if start <= day <= end
    )
    expected_symbol_sessions = len(detail) * len(observed_exchange_sessions)
    summary = {
        "symbols_compared": int(len(detail)),
        "calendar_weekdays_not_in_holiday_file": len(session_days),
        "observed_exchange_sessions": len(observed_exchange_sessions),
        "observed_exchange_session_dates": [day.isoformat() for day in observed_exchange_sessions],
        "expected_symbol_sessions": expected_symbol_sessions,
        "timestamps_added": int(detail["timestamps_added"].sum()),
        "timestamps_removed": int(detail["timestamps_removed"].sum()),
        "overlap_ohlcv_changed": int(detail["overlap_ohlcv_changed"].sum()),
        "symbols_with_overlap_corrections": int(
            detail["overlap_ohlcv_changed"].gt(0).sum()
        ),
        "symbols_with_overlap_corrections_list": detail.loc[
            detail["overlap_ohlcv_changed"].gt(0), "symbol"
        ].tolist(),
        "overlap_changed_by_column": {
            column: int(detail[f"overlap_{column}_changed"].sum())
            for column in EQUITY_COLUMNS[1:]
        },
        "complete_symbol_sessions_to_1515": int(detail["complete_sessions_to_1515"].sum()),
        "complete_symbol_sessions_to_1530": int(detail["complete_sessions_to_1530"].sum()),
        "aug27_rows_after": int(detail["aug27_rows_after"].sum()),
        "aug27_symbols_with_360_rows": int(detail["aug27_rows_after"].eq(360).sum()),
        "aug27_latest_timestamp": sorted(set(detail["aug27_last_after"]))[-1] if len(detail) else "",
    }
    if "aug27_rows_before_targeted_repair" in detail:
        summary["aug27_rows_before_targeted_repair"] = int(
            detail["aug27_rows_before_targeted_repair"].sum()
        )
        summary["aug27_symbols_missing_before_targeted_repair"] = int(
            detail["aug27_rows_before_targeted_repair"].eq(0).sum()
        )
    return summary, detail


def _futures_audit(current_root: Path, backup_root: Path) -> tuple[dict[str, Any], pd.DataFrame]:
    records: list[dict[str, Any]] = []
    for backup in sorted(backup_root.glob("*_5minute.parquet")):
        if "26AUGFUT" not in backup.name and "26SEPFUT" not in backup.name:
            continue
        current = current_root / backup.name
        record: dict[str, Any] = {
            "file": backup.name,
            "contract_month": "2026-08" if "26AUGFUT" in backup.name else "2026-09",
            "exists_after": current.exists(),
            "bytes_before": int(backup.stat().st_size),
            "bytes_after": int(current.stat().st_size) if current.exists() else 0,
            "sha256_equal": bool(current.exists() and _sha256(backup) == _sha256(current)),
            "rows_before": 0,
            "rows_after": 0,
            "timestamps_added": 0,
            "timestamps_removed": 0,
        }
        if current.exists():
            old = pd.read_parquet(backup, columns=["timestamp"])
            new = pd.read_parquet(current, columns=["timestamp"])
            old_ts = pd.Index(_ist(old["timestamp"]).dropna().unique())
            new_ts = pd.Index(_ist(new["timestamp"]).dropna().unique())
            record.update(
                {
                    "rows_before": int(len(old)),
                    "rows_after": int(len(new)),
                    "timestamps_added": int(len(new_ts.difference(old_ts))),
                    "timestamps_removed": int(len(old_ts.difference(new_ts))),
                    "first_after": str(new_ts.min()) if len(new_ts) else "",
                    "last_after": str(new_ts.max()) if len(new_ts) else "",
                }
            )
        records.append(record)
    detail = pd.DataFrame(records)
    summary = {
        "contracts_compared": int(len(detail)),
        "aug_contracts": int(detail["contract_month"].eq("2026-08").sum()),
        "sep_contracts": int(detail["contract_month"].eq("2026-09").sum()),
        "timestamps_added": int(detail["timestamps_added"].sum()),
        "timestamps_removed": int(detail["timestamps_removed"].sum()),
        "byte_identical_files": int(detail["sha256_equal"].sum()),
        "missing_after": int((~detail["exists_after"]).sum()),
    }
    return summary, detail


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--from-day", default="2026-05-27")
    parser.add_argument("--through-day", default="2026-08-27")
    parser.add_argument(
        "--equity-root",
        type=Path,
        default=Path(r"C:\TradingData\eqidv2\stocks_indicators_1min_eq"),
    )
    parser.add_argument("--equity-backup-root", type=Path, required=True)
    parser.add_argument("--today-backup-root", type=Path)
    parser.add_argument(
        "--futures-root", type=Path, default=common.RAW_CONTRACT_DIR
    )
    parser.add_argument("--futures-backup-root", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    equity, equity_detail = _equity_audit(
        args.equity_root,
        args.equity_backup_root,
        args.today_backup_root,
        pd.Timestamp(args.from_day).date(),
        pd.Timestamp(args.through_day).date(),
    )
    futures, futures_detail = _futures_audit(args.futures_root, args.futures_backup_root)
    equity_detail.to_csv(args.output_dir / "equity_per_symbol.csv", index=False)
    futures_detail.to_csv(args.output_dir / "futures_per_contract.csv", index=False)
    payload = {
        "scope": {"from_day": args.from_day, "through_day": args.through_day},
        "equity": equity,
        "futures": futures,
        "source_limitations": [
            "BROKER_EQUITY_1M_LATEST_REAL_END_LABEL_IS_15:15_NOT_15:30",
            "EXPIRED_2026_08_FUTURES_TOKENS_REJECTED_BY_LIVE_HISTORICAL_ENDPOINT",
        ],
    }
    (args.output_dir / "repair_audit.json").write_text(
        json.dumps(payload, indent=2), encoding="utf-8"
    )
    print(json.dumps(payload, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
