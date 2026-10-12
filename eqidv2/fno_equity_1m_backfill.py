"""Backfill missing NSE-equity 1-minute histories required by FnO research.

The selected persisted FnO universe is authoritative for cash symbols and
instrument tokens. LTM additionally inherits the pre-rename LTIM cash history
before the new symbol's first row. Existing files are archived before repair.
"""

from __future__ import annotations

import argparse
import json
import logging
import math
import shutil
from datetime import datetime, time, timedelta
from pathlib import Path
from typing import Any

import pandas as pd

import fno_oi_common as common
import trading_data_continous_run_historical_alltf_v3_parquet_stocksonly_1min as stock_1m
from nse_market_calendar import market_closed_reason


DEFAULT_OUTPUT_DIR = Path(r"C:\TradingData\eqidv2\stocks_indicators_1min_eq")
DEFAULT_BACKUP_ROOT = common.FNO_ROOT / "historical_repair" / "equity_1m_original"
PREDECESSOR_SYMBOLS = {"LTM": "LTIM"}
BASE_COLUMNS = ("date", "open", "high", "low", "close", "volume")
QUALITY_FLAGS = ("gap_filled", "is_gap_filled", "is_synthetic", "synthetic", "forward_filled", "synthetic_repair", "repaired_synthetic",
                 "opening_snapshot", "provisional_stale", "is_filled", "interpolated", "is_interpolated")


def _mapped_tokens(universe_path: Path | None = None) -> dict[str, int]:
    path = (
        Path(universe_path)
        if universe_path is not None
        else common.UNIVERSE_DIR / "latest_near_month.parquet"
    )
    if not path.exists():
        raise FileNotFoundError(f"FnO universe is missing: {path}")
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
        common._atomic_replace_bytes(backup, lambda staged: shutil.copy2(path, staged))
    return backup


def _flagged(value: Any) -> bool:
    if pd.isna(value):
        return False
    return str(value).strip().lower() not in {"", "false", "0", "0.0", "no", "none"}


def _normalize(frame: pd.DataFrame, *, validate_prices: bool = True) -> pd.DataFrame:
    """Keep all metadata; malformed rows are rejected, never silently discarded."""
    if frame.empty:
        return frame.copy() if set(BASE_COLUMNS).issubset(frame) else pd.DataFrame(columns=BASE_COLUMNS)
    if not set(BASE_COLUMNS).issubset(frame):
        raise ValueError("Minute rows are missing required OHLCV columns")
    work = frame.copy()
    work["date"] = pd.to_datetime(work["date"], errors="raise")
    if work["date"].dt.tz is None:
        work["date"] = work["date"].dt.tz_localize(stock_1m.IST_TZ)
    else:
        work["date"] = work["date"].dt.tz_convert(stock_1m.IST_TZ)
    if work["date"].isna().any() or work["date"].duplicated().any() or not work["date"].eq(work["date"].dt.floor("min")).all():
        raise ValueError("Minute timestamps must be unique, valid minute boundaries")
    for column in BASE_COLUMNS[1:]:
        if work[column].map(lambda value: isinstance(value, bool)).any():
            raise ValueError("Boolean OHLCV values are invalid")
        work[column] = pd.to_numeric(work[column], errors="coerce")
    if validate_prices:
        finite = work[list(BASE_COLUMNS[1:])].apply(lambda values: values.map(lambda value: pd.notna(value) and math.isfinite(float(value)))).all(axis=1)
        valid = (finite & work[["open", "high", "low", "close"]].gt(0).all(axis=1)
                 & work["volume"].ge(0) & work["volume"].mod(1).eq(0)
                 & work["low"].le(work[["open", "close"]].min(axis=1))
                 & work["high"].ge(work[["open", "close"]].max(axis=1)))
        if not valid.all():
            raise ValueError("Minute OHLCV values must be finite and internally valid")
    for column in QUALITY_FLAGS:
        if column in work:
            work[column] = pd.array([pd.NA if pd.isna(value) else _flagged(value) for value in work[column]], dtype="boolean")
    return work.sort_values("date", kind="stable").reset_index(drop=True)


def _genuine(frame: pd.DataFrame) -> pd.Series:
    valid = pd.Series(True, index=frame.index)
    for column in QUALITY_FLAGS:
        if column in frame:
            valid &= ~frame[column].map(_flagged)
    if "quality_state" in frame:
        state = frame["quality_state"].fillna("").astype(str).str.strip().str.upper()
        valid &= state.isin({"", "VALID", "RAW", "REAL", "GENUINE", "OK"})
    if "source" in frame:
        valid &= ~frame["source"].fillna("").astype(str).str.upper().str.contains("SYNTHETIC|GAP.?FILL|FORWARD.?FILL|CARRY.?FORWARD|INTERPOLAT|PROXY", regex=True)
    return valid


def _merge_fetched(existing: pd.DataFrame, fetched: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    existing = _normalize(existing, validate_prices=False)
    fetched = _normalize(fetched)
    if fetched.empty:
        return existing, 0
    accepted = fetched.loc[_genuine(fetched)].copy()
    if accepted.empty:
        return existing, 0
    # Only exact rows returned genuinely by the broker may retire old flags.
    for column in QUALITY_FLAGS:
        if column in existing or column in accepted:
            accepted[column] = False
    accepted["gap_filled"] = False
    accepted["is_synthetic"] = False
    accepted["quality_state"] = "VALID"
    accepted["source"] = "KITE_HISTORICAL_1M"
    accepted["fetched_at"] = common.now_ist().isoformat()
    retained = existing.loc[~existing["date"].isin(accepted["date"])]
    parts = [frame for frame in (retained, accepted) if not frame.empty]
    return pd.concat(parts, ignore_index=True, sort=False), len(accepted)


def _recompute(frame: pd.DataFrame) -> pd.DataFrame:
    work = _normalize(frame)
    if work.empty:
        return work
    work = stock_1m._compute_common_features(work, "1min")
    return _normalize(stock_1m._downcast_numeric_columns(work))


def _write_validated(frame: pd.DataFrame, path: Path) -> None:
    """Validate serialized staging bytes before replacing the original file."""
    frame = _normalize(frame)
    def write(staged: Path) -> None:
        frame.to_parquet(staged, index=False)
        restored = _normalize(pd.read_parquet(staged))
        if len(restored) != len(frame) or list(restored.columns) != list(frame.columns):
            raise ValueError("Staged minute history failed validation")
    common._atomic_replace_bytes(path, write)


def _with_predecessor(root: Path, symbol: str, current: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    predecessor = PREDECESSOR_SYMBOLS.get(symbol)
    predecessor_path = _symbol_path(root, predecessor) if predecessor else None
    if current.empty or predecessor_path is None or not predecessor_path.exists():
        return current, 0
    current = _normalize(current, validate_prices=False)
    historical = _normalize(pd.read_parquet(predecessor_path), validate_prices=False)
    historical = historical.loc[historical["date"].lt(current["date"].min())].copy()
    if historical.empty:
        return current, 0
    return pd.concat([historical, current], ignore_index=True, sort=False), len(historical)


def _merge_predecessor(root: Path, symbol: str, backup_root: Path | None = None) -> int:
    predecessor = PREDECESSOR_SYMBOLS.get(symbol)
    if not predecessor:
        return 0
    current_path = _symbol_path(root, symbol)
    predecessor_path = _symbol_path(root, predecessor)
    if not current_path.exists() or not predecessor_path.exists():
        return 0
    combined, count = _with_predecessor(root, symbol, pd.read_parquet(current_path))
    if count:
        _archive_existing(current_path, backup_root if backup_root is not None else DEFAULT_BACKUP_ROOT)
        _write_validated(_recompute(combined), current_path)
    return count


def _coverage(frame: pd.DataFrame, start: datetime, end: datetime, holidays: set) -> dict[str, Any]:
    observed = pd.DatetimeIndex(frame.loc[_genuine(frame), "date"]) if not frame.empty else pd.DatetimeIndex([], tz=stock_1m.IST_TZ)
    missing, expected, genuine = [], 0, 0
    day = start.date()
    while day <= end.date():
        if day not in holidays and not market_closed_reason(day):
            bounds = pd.date_range(f"{day} 09:16", f"{day} 15:30", freq="min", tz=stock_1m.IST_TZ)
            bounds = bounds[(bounds >= start) & (bounds <= end)]
            absent = bounds.difference(observed)
            expected += len(bounds)
            genuine += len(bounds) - len(absent)
            missing.extend(stamp.isoformat() for stamp in absent[:max(0, 20 - len(missing))])
        day += timedelta(days=1)
    return {"expected_minutes": expected, "genuine_minutes": genuine,
            "missing_or_synthetic_minutes": expected - genuine, "missing_sample": missing}


def run(
    symbols: list[str],
    *,
    start_date: str,
    end_date: str,
    output_dir: Path,
    backup_root: Path,
    universe_path: Path | None = None,
    force_window: bool = False,
) -> list[dict[str, Any]]:
    tokens = _mapped_tokens(universe_path)
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
    if start > end:
        raise ValueError("Backfill start must not follow its end")
    end = min(end, stock_1m.last_completed_intraday_end(common.now_ist(), 1, holidays))
    if start > end:
        raise ValueError("The requested window has no completed market minutes")
    output_dir.mkdir(parents=True, exist_ok=True)

    outcomes: list[dict[str, Any]] = []
    for symbol in requested:
        path = _symbol_path(output_dir, symbol)
        backup = _archive_existing(path, backup_root)
        existing = pd.read_parquet(path) if path.exists() else pd.DataFrame(columns=BASE_COLUMNS)
        # Reuse the core's warm-up boundary, but never its metadata-dropping writer.
        fetch_start = start if force_window else stock_1m._incremental_start_from_existing(
            "1min", str(path), start, holidays=holidays, intraday_ts="end")
        fetched = pd.DataFrame(columns=BASE_COLUMNS)
        if fetch_start < end:
            try:
                try:
                    client = stock_1m.get_kite_client_rr()
                except Exception:
                    client = primary
                fetched = stock_1m.fetch_historical_1min_df(
                    client,
                    tokens[symbol],
                    fetch_start,
                    end,
                    logger,
                    "end",
                )
            except Exception as exc:
                raise RuntimeError(f"{symbol} 1-minute fetch failed ({type(exc).__name__})") from None
        fetched = _normalize(fetched if fetched is not None else pd.DataFrame(columns=BASE_COLUMNS))
        if not fetched.empty:
            stamps = fetched["date"]
            minute = stamps.dt.hour * 60 + stamps.dt.minute
            if not (stamps.ge(fetch_start) & stamps.le(end) & minute.between(556, 930)).all():
                raise ValueError(f"{symbol} fetched minute lies outside its completed request window")
        combined, accepted_rows = _merge_fetched(existing, fetched)
        combined, predecessor_rows = _with_predecessor(output_dir, symbol, combined)
        combined = _recompute(combined)
        if not combined.empty and (accepted_rows or predecessor_rows):
            _write_validated(combined, path)
        coverage = _coverage(combined, start, end, holidays)
        status = "partial" if coverage["missing_or_synthetic_minutes"] else "updated" if accepted_rows or predecessor_rows else "noop"
        if status == "updated" and existing.empty:
            status = "created"
        dates = combined["date"]
        outcomes.append(
            {
                "symbol": symbol,
                "token": tokens[symbol],
                "status": status,
                "force_window": bool(force_window),
                "fetch_start": str(fetch_start),
                "fetch_end": str(end),
                "fetched_rows": int(len(fetched)),
                "accepted_fetched_rows": int(accepted_rows),
                "rejected_quality_rows": int(len(fetched) - accepted_rows),
                "retained_non_genuine_rows": int((~_genuine(combined)).sum()),
                **coverage,
                "rows": int(len(dates)),
                "first": str(dates.min()) if len(dates) else None,
                "last": str(dates.max()) if len(dates) else None,
                "predecessor_rows": predecessor_rows,
                "backup": str(backup) if backup else "",
            }
        )
    return outcomes


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    symbols = parser.add_mutually_exclusive_group()
    symbols.add_argument("--symbols", default="IDEA,LTM")
    symbols.add_argument(
        "--all-mapped",
        action="store_true",
        help="Backfill every mapped cash symbol in the selected FnO universe.",
    )
    parser.add_argument("--start-date", default="2025-06-01")
    parser.add_argument("--end-date", default=common.now_ist().date().isoformat())
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--backup-root", type=Path, default=DEFAULT_BACKUP_ROOT)
    parser.add_argument(
        "--universe-path",
        type=Path,
        default=common.UNIVERSE_DIR / "latest_near_month.parquet",
        help="Persisted mapped FnO universe whose cash symbols/tokens are authoritative.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate the universe and print the selected symbols without calling Kite.",
    )
    parser.add_argument(
        "--force-window",
        action="store_true",
        help=(
            "Fetch the complete requested window and merge it explicitly instead of "
            "using the incremental warm-up boundary. Use this to repair older partial "
            "session tails inside an already-populated file."
        ),
    )
    args = parser.parse_args(argv)
    mapped = _mapped_tokens(args.universe_path)
    selected = sorted(mapped) if args.all_mapped else args.symbols.split(",")
    if args.dry_run:
        requested = sorted({value.strip().upper() for value in selected if value.strip()})
        print(
            json.dumps(
                {
                    "universe_path": str(args.universe_path.resolve()),
                    "mapped_symbol_count": len(mapped),
                    "selected_symbol_count": len(requested),
                    "symbols": requested,
                },
                indent=2,
            ),
            flush=True,
        )
        return 0
    outcomes = run(
        selected,
        start_date=args.start_date,
        end_date=args.end_date,
        output_dir=args.output_dir,
        backup_root=args.backup_root,
        universe_path=args.universe_path,
        force_window=args.force_window,
    )
    report_path = common.FNO_ROOT / "historical_repair" / "equity_1m_backfill.json"
    common.atomic_write_json(
        report_path,
        {
            "generated_at_ist": common.now_ist().isoformat(),
            "universe_path": str(args.universe_path.resolve()),
            "outcomes": outcomes,
        },
    )
    print(json.dumps(outcomes, indent=2), flush=True)
    return 2 if any(row["status"] == "partial" for row in outcomes) else 0


if __name__ == "__main__":
    raise SystemExit(main())
