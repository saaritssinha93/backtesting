"""Backfill missing NSE-equity 1-minute histories required by FnO V5.

The current FnO universe is authoritative for cash symbols and instrument
tokens. LTM additionally inherits the pre-rename LTIM cash history before the
new symbol's first row. All writes are atomic and existing files are archived.
"""

from __future__ import annotations

import argparse
import json
import logging
import shutil
from datetime import datetime, time
from pathlib import Path
from typing import Any

import pandas as pd

import fno_oi_common as common
import trading_data_continous_run_historical_alltf_v3_parquet_stocksonly_1min as stock_1m


DEFAULT_OUTPUT_DIR = Path(r"C:\TradingData\eqidv2\stocks_indicators_1min_eq")
DEFAULT_BACKUP_ROOT = common.FNO_ROOT / "historical_repair" / "equity_1m_original"
PREDECESSOR_SYMBOLS = {"LTM": "LTIM"}
BASE_COLUMNS = ("date", "open", "high", "low", "close", "volume")


def _mapped_tokens() -> dict[str, int]:
    path = common.UNIVERSE_DIR / "latest_near_month.parquet"
    universe = pd.read_parquet(path)
    required = {"equity_symbol", "equity_instrument_token"}
    if not required.issubset(universe.columns):
        raise RuntimeError(f"Mapped equity columns are missing from {path}")
    rows = universe[list(required)].dropna().copy()
    rows["equity_symbol"] = rows["equity_symbol"].astype(str).str.strip().str.upper()
    rows["equity_instrument_token"] = pd.to_numeric(
        rows["equity_instrument_token"], errors="coerce"
    )
    return {
        str(row.equity_symbol): int(row.equity_instrument_token)
        for row in rows.itertuples(index=False)
        if str(row.equity_symbol) and float(row.equity_instrument_token) > 0
    }


def _symbol_path(root: Path, symbol: str) -> Path:
    return root / f"{symbol}_stocks_indicators_1min.parquet"


def _archive_existing(path: Path, backup_root: Path) -> Path | None:
    if not path.exists():
        return None
    backup_root.mkdir(parents=True, exist_ok=True)
    backup = backup_root / path.name
    if not backup.exists():
        shutil.copy2(path, backup)
    return backup


def _recompute(frame: pd.DataFrame) -> pd.DataFrame:
    work = frame[list(BASE_COLUMNS)].copy()
    work["date"] = pd.to_datetime(work["date"], errors="coerce")
    if work["date"].dt.tz is None:
        work["date"] = work["date"].dt.tz_localize(stock_1m.IST_TZ)
    else:
        work["date"] = work["date"].dt.tz_convert(stock_1m.IST_TZ)
    for column in BASE_COLUMNS[1:]:
        work[column] = pd.to_numeric(work[column], errors="coerce")
    work = (
        work.dropna(subset=list(BASE_COLUMNS))
        .drop_duplicates("date", keep="last")
        .sort_values("date", kind="stable")
        .reset_index(drop=True)
    )
    work = stock_1m._compute_common_features(work, "1min")
    return stock_1m._downcast_numeric_columns(work)


def _merge_predecessor(root: Path, symbol: str) -> int:
    predecessor = PREDECESSOR_SYMBOLS.get(symbol)
    if not predecessor:
        return 0
    current_path = _symbol_path(root, symbol)
    predecessor_path = _symbol_path(root, predecessor)
    if not current_path.exists() or not predecessor_path.exists():
        return 0
    current = pd.read_parquet(current_path, columns=list(BASE_COLUMNS))
    historical = pd.read_parquet(predecessor_path, columns=list(BASE_COLUMNS))
    current_dates = pd.to_datetime(current["date"], errors="coerce")
    first_current = current_dates.dropna().min()
    if pd.isna(first_current):
        raise RuntimeError(f"{symbol} has no valid fetched timestamps")
    historical_dates = pd.to_datetime(historical["date"], errors="coerce")
    historical = historical.loc[historical_dates.lt(first_current)].copy()
    if historical.empty:
        return 0
    combined = _recompute(pd.concat([historical, current], ignore_index=True, sort=False))
    common.atomic_write_parquet(combined, current_path)
    return int(len(historical))


def run(
    symbols: list[str],
    *,
    start_date: str,
    end_date: str,
    output_dir: Path,
    backup_root: Path,
) -> list[dict[str, Any]]:
    tokens = _mapped_tokens()
    requested = sorted({value.strip().upper() for value in symbols if value.strip()})
    missing_tokens = [symbol for symbol in requested if symbol not in tokens]
    if missing_tokens:
        raise RuntimeError(f"Missing current NSE tokens for: {', '.join(missing_tokens)}")

    logger = logging.getLogger("fno_equity_1m_backfill")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
    primary = stock_1m.setup_kite_session()
    holidays = stock_1m._read_holidays(stock_1m.HOLIDAYS_FILE_DEFAULT)
    start = stock_1m.IST_TZ.localize(datetime.combine(pd.Timestamp(start_date).date(), time(0, 0)))
    end = stock_1m.IST_TZ.localize(datetime.combine(pd.Timestamp(end_date).date(), time(15, 30)))
    output_dir.mkdir(parents=True, exist_ok=True)
    stock_1m.DIRS["1min"]["out"] = str(output_dir)

    outcomes: list[dict[str, Any]] = []
    for symbol in requested:
        path = _symbol_path(output_dir, symbol)
        backup = _archive_existing(path, backup_root)
        report = stock_1m.process_ticker(
            "1min",
            symbol,
            tokens[symbol],
            primary,
            start,
            end,
            logger,
            holidays,
            False,
            "end",
            str(common.FNO_ROOT / "historical_repair" / "equity_1m_reports"),
            False,
            5,
        )
        if report.status == "failed" or not path.exists():
            raise RuntimeError(f"{symbol} 1-minute backfill failed: {report}")
        predecessor_rows = _merge_predecessor(output_dir, symbol)
        dates = pd.to_datetime(pd.read_parquet(path, columns=["date"])["date"], errors="coerce")
        outcomes.append(
            {
                "symbol": symbol,
                "token": tokens[symbol],
                "status": report.status,
                "rows": int(len(dates)),
                "first": str(dates.min()),
                "last": str(dates.max()),
                "predecessor_rows": predecessor_rows,
                "backup": str(backup) if backup else "",
            }
        )
    return outcomes


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--symbols", default="IDEA,LTM")
    parser.add_argument("--start-date", default="2025-06-01")
    parser.add_argument("--end-date", default=common.now_ist().date().isoformat())
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--backup-root", type=Path, default=DEFAULT_BACKUP_ROOT)
    args = parser.parse_args(argv)
    outcomes = run(
        args.symbols.split(","),
        start_date=args.start_date,
        end_date=args.end_date,
        output_dir=args.output_dir,
        backup_root=args.backup_root,
    )
    report_path = common.FNO_ROOT / "historical_repair" / "equity_1m_backfill.json"
    common.atomic_write_json(report_path, {"generated_at_ist": common.now_ist().isoformat(), "outcomes": outcomes})
    print(json.dumps(outcomes, indent=2), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
