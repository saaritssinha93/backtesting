r"""
Exact completed-candle verification for stored cash-market data.

Scans the 5-min and 1-min parquet directories for the target date
and validates the completed IST timestamp grid, OHLCV and source quality.

Writes:
  C:\TradingData\eqidv2\backtesting_result_v11\latest\data_verify_{date}.json
  C:\TradingData\eqidv2\backtesting_result_v11\latest\data_verify_latest.json

Exit codes:
  0 — PASS (all due candles valid), or explicitly NOT_DUE / NOT_A_SESSION
  1 — WARN (market-context anomaly)
  2 — FAIL (any missing, duplicate, off-grid, invalid or flagged due candle)
"""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path
from typing import Any, Optional, Set

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

from eqidv2_runtime_paths import DATA_5M_DIR, DATA_1MIN_DIR, runtime_dir
from nse_market_calendar import market_closed_reason

VERIFY_DIR = runtime_dir("backtesting_result_v11", "latest")
VERIFY_DIR.mkdir(parents=True, exist_ok=True)

# Keep the legacy caller's 76 identifier: only the 75 completed 5m candles
# 09:20..15:30 are required. The optional 09:15 opening snapshot is not a candle.
EXPECTED_5M_BARS = 76
EXPECTED_1MIN_BARS = 375  # completed minute ends 09:16..15:30
OHLCV_COLUMNS = ("open", "high", "low", "close", "volume")
QUALITY_FLAGS = (
    "gap_filled", "is_gap_filled", "opening_snapshot", "provisional_stale",
    "synthetic", "is_synthetic", "synthetic_repair", "repaired_synthetic",
    "forward_filled", "is_filled", "interpolated", "is_interpolated",
)
QUALITY_TEXT_COLUMNS = ("quality_state", "source")

# Market/regime context files (NIFTY proxy/index). A true zero-volume index or a
# wrong price scale (~24,000 instead of ETF ~268) leaking in here corrupts the
# VWAP regime, so these get extra sanity checks.
MARKET_FILE_PREFIXES = ("NIFTY", "NIFTYBEES")
MARKET_SCALE_MAX_CLOSE = 5000.0  # ETF proxy ~ hundreds; true NIFTY 50 index ~ tens of thousands
# Aliases that are INTENTIONALLY a true zero-volume index (research only, not a
# regime source) — exempt from the zero-volume/scale anomaly check.
INTENTIONAL_INDEX_TICKERS = {"NIFTY50_INDEX"}
_STORE_SUFFIXES = ("_stocks_indicators_5min", "_stocks_indicators_1min")

def _read_parquet_timestamps(fpath: Path) -> pd.Series:
    schema_names = set(pq.ParquetFile(fpath).schema_arrow.names)
    timestamp_col = next(
        (name for name in ("date", "datetime", "timestamp") if name in schema_names),
        None,
    )
    if timestamp_col is None:
        raise ValueError(
            "No supported timestamp column found; expected one of "
            f"date/datetime/timestamp, found={sorted(schema_names)}"
        )

    df = pd.read_parquet(fpath, columns=[timestamp_col])
    if df.empty:
        return pd.Series([], dtype="datetime64[ns]")
    return pd.to_datetime(df[timestamp_col], errors="coerce")


def _now_ist(now: Any = None) -> pd.Timestamp:
    current = pd.Timestamp.now(tz="Asia/Kolkata") if now is None else pd.Timestamp(now)
    return current.tz_localize("Asia/Kolkata") if current.tzinfo is None else current.tz_convert("Asia/Kolkata")


def _completed_grid(date_str: str, *, five_minute: bool, now: Any = None) -> tuple[pd.DatetimeIndex, str]:
    """Only closed end-stamped candles are due, including special regular sessions."""
    target = pd.Timestamp(date_str).date()
    empty = pd.DatetimeIndex([], tz="Asia/Kolkata")
    if market_closed_reason(target):
        return empty, "NOT_A_SESSION"
    current = _now_ist(now)
    if target > current.date():
        return empty, "NOT_DUE"
    start = pd.Timestamp(f"{target.isoformat()} {'09:20' if five_minute else '09:16'}", tz="Asia/Kolkata")
    close = pd.Timestamp(f"{target.isoformat()} 15:30", tz="Asia/Kolkata")
    cutoff = min(current, close)
    if cutoff < start:
        return empty, "NOT_DUE"
    grid = pd.date_range(start, cutoff, freq="5min" if five_minute else "1min")
    return grid, "COMPLETE" if cutoff >= close else "IN_PROGRESS"


def _read_quality_frame(fpath: Path) -> tuple[pd.DataFrame, pd.Series]:
    names = set(pq.ParquetFile(fpath).schema_arrow.names)
    stamp = next((name for name in ("date", "datetime", "timestamp") if name in names), None)
    if stamp is None:
        raise ValueError("No supported timestamp column; expected date/datetime/timestamp")
    wanted = [stamp] + [name for name in (*OHLCV_COLUMNS, *QUALITY_FLAGS, *QUALITY_TEXT_COLUMNS, "source_1m_count") if name in names]
    frame = pd.read_parquet(fpath, columns=wanted)
    timestamps = pd.to_datetime(frame[stamp], errors="coerce")
    timestamps = timestamps.dt.tz_localize("Asia/Kolkata") if timestamps.dt.tz is None else timestamps.dt.tz_convert("Asia/Kolkata")
    return frame, timestamps


def _quality_flagged(values: pd.Series) -> pd.Series:
    """Explicit nonzero/true flags and malformed nonempty flags cannot certify real bars."""
    numeric = pd.to_numeric(values, errors="coerce")
    text = values.astype(str).str.strip().str.lower()
    clean_text = text.isin({"", "false", "no", "off"})
    return values.notna() & ~clean_text & (numeric.isna() | numeric.ne(0))


def _invalid_quality_metadata(frame: pd.DataFrame) -> dict[str, pd.Series]:
    """Legacy absent/null/blank text metadata is unspecified, not explicit approval.

    All rows still need valid OHLCV and an exact timestamp. Supplied quality
    states must be VALID; declared proxy/synthetic sources cannot certify real
    candles. A genuine broker backfill/repair source remains eligible.
    """
    invalid = {}
    if "quality_state" in frame:
        state = frame.quality_state.fillna("").astype(str).str.strip().str.upper()
        invalid["invalid_quality_state"] = ~state.isin({"", "VALID"})
    if "source" in frame:
        source = frame.source.fillna("").astype(str).str.strip().str.lower()
        invalid["invalid_source"] = source.str.contains(
            r"synthetic|gap[\s_-]*fill|forward[\s_-]*fill|interpolat|proxy|carry[\s_-]*forward",
            regex=True, na=False,
        )
    return invalid


def _invalid_candles(frame: pd.DataFrame, *, five_minute: bool) -> tuple[pd.Series, dict[str, int]]:
    values = frame.reindex(columns=OHLCV_COLUMNS).apply(pd.to_numeric, errors="coerce")
    finite = pd.Series(np.isfinite(values.to_numpy(dtype=float)).all(axis=1), index=frame.index)
    valid = (finite & values[list(OHLCV_COLUMNS[:4])].gt(0).all(axis=1)
             & values.volume.ge(0) & values.high.ge(values[["open", "close"]].max(axis=1))
             & values.low.le(values[["open", "close"]].min(axis=1)) & values.high.ge(values.low))
    reasons = {"invalid_ohlcv": int((~valid).sum())}
    invalid = ~valid
    for flag in QUALITY_FLAGS:
        if flag in frame:
            flagged = _quality_flagged(frame[flag])
            reasons["flagged_" + flag] = int(flagged.sum())
            invalid |= flagged
    for reason, rejected in _invalid_quality_metadata(frame).items():
        reasons[reason] = int(rejected.sum())
        invalid |= rejected
    if five_minute and "source_1m_count" in frame:
        # A provided exact-minute provenance field must prove all five inputs;
        # missing metadata in a plain legacy schema remains optional.
        incomplete = pd.to_numeric(frame.source_1m_count, errors="coerce").ne(5)
        reasons["incomplete_minute_source"] = int(incomplete.sum())
        invalid |= incomplete
    return invalid, {key: count for key, count in reasons.items() if count}


def _closing_bar_requirement(date_str: str, now: Any = None) -> str:
    """Only require a completed regular session, using the shared NSE calendar."""
    target = pd.Timestamp(date_str).date()
    current = _now_ist(now)
    if market_closed_reason(target):
        return "NOT_A_SESSION"
    if target > current.date() or (target == current.date() and current.strftime("%H:%M") < "15:30"):
        return "NOT_DUE"
    return "REQUIRED"


def _closing_bar_status(fpath: Path, date_str: str, *, five_minute: bool) -> str:
    """A timestamp placeholder cannot certify an actual 15:30 cash close."""
    frame, timestamps = _read_quality_frame(fpath)
    return _closing_status_from_frame(frame, timestamps, date_str, five_minute=five_minute)


def _closing_status_from_frame(frame: pd.DataFrame, timestamps: pd.Series, date_str: str, *, five_minute: bool) -> str:
    exact = frame.loc[timestamps.eq(pd.Timestamp(f"{date_str} 15:30", tz="Asia/Kolkata"))]
    if exact.empty:
        return "MISSING"
    if len(exact) != 1:
        return "DUPLICATE"
    row = exact.iloc[0]
    for flag in QUALITY_FLAGS:
        if flag in exact and _quality_flagged(exact[flag]).any():
            return "FLAGGED_" + flag.upper()
    for reason, rejected in _invalid_quality_metadata(exact).items():
        if rejected.any():
            return reason.upper()
    close = pd.to_numeric(row.get("close"), errors="coerce")
    if pd.isna(close) or not np.isfinite(close) or close <= 0:
        return "INVALID_PRICE"
    count = pd.to_numeric(row.get("source_1m_count"), errors="coerce")
    if five_minute and "source_1m_count" in exact and (pd.isna(count) or count != 5):
        return "INCOMPLETE_MINUTE_SOURCE"
    invalid, _ = _invalid_candles(exact, five_minute=five_minute)
    if invalid.any():
        return "INVALID_OHLCV"
    return "VERIFIED"


def _ticker_from_path(fpath: Path) -> str:
    """Ticker = file stem minus the known store suffix.

    `stem.split("_")[0]` was wrong: it collapsed NIFTY_50 -> NIFTY and
    NIFTYBEES_PROXY -> NIFTYBEES, colliding distinct series in the stats dict.
    """
    name = fpath.stem
    for suf in _STORE_SUFFIXES:
        if name.endswith(suf):
            name = name[: -len(suf)]
            break
    return name.upper()


def _market_file_anomaly(fpath: Path) -> str | None:
    """Flag zero-volume (index leaked in) or wrong price scale on a market file."""
    try:
        df = pd.read_parquet(fpath, columns=["close", "volume"])
    except Exception:
        return None
    if df.empty:
        return None
    close = pd.to_numeric(df["close"], errors="coerce").dropna()
    vol = pd.to_numeric(df["volume"], errors="coerce").fillna(0.0)
    issues = []
    if float(vol.sum()) <= 0:
        issues.append("zero_volume(true-index?)")
    if not close.empty and float(close.iloc[-1]) > MARKET_SCALE_MAX_CLOSE:
        issues.append(f"scale_anomaly(close={float(close.iloc[-1]):.0f}; true-index leaked?)")
    return "; ".join(issues) or None


def _scan_parquet_dir(
    data_dir: Path,
    date_str: str,
    expected_bars: int,
    required_tickers: Optional[Set[str]] = None,
    *,
    require_closing_bar: Optional[bool] = None,
    now: Any = None,
) -> dict[str, Any]:
    """Verify every due regular-session candle; aggregate tolerances never waive gaps."""
    if expected_bars not in (75, EXPECTED_5M_BARS, EXPECTED_1MIN_BARS):
        raise ValueError("expected_bars must identify the 75/76-bar 5m or 375-bar 1m store")
    five_minute = expected_bars in (75, EXPECTED_5M_BARS)
    current = _now_ist(now)
    grid, coverage_state = _completed_grid(date_str, five_minute=five_minute, now=current)
    closing_requirement = _closing_bar_requirement(date_str, current) if require_closing_bar is None else "REQUIRED" if require_closing_bar else "NOT_DUE"
    closing_required = closing_requirement == "REQUIRED"
    required = {str(ticker).strip().upper() for ticker in (required_tickers or set()) if str(ticker).strip()}
    base = {
        "dir": str(data_dir), "date": date_str, "expected_bars": len(grid),
        "full_session_expected_bars": 75 if five_minute else 375,
        "coverage_state": coverage_state,
        "coverage_policy": "Every completed IST candle; no missing, duplicate, off-grid, invalid OHLCV or flagged source rows",
        "expected_first_timestamp": grid[0].isoformat() if len(grid) else None,
        "expected_last_timestamp": grid[-1].isoformat() if len(grid) else None,
        "opening_snapshot_policy": "Optional 09:15 snapshot excluded from completed candles" if five_minute else None,
        "total_tickers": 0, "ok": 0, "warn": 0, "fail": 0, "dup_files": 0,
        "missing_pct": 0.0, "worst_tickers": {}, "required_tickers": len(required) if required else None,
        "closing_bar_requirement": closing_requirement, "closing_bar_verified": 0, "closing_bar_failed": 0,
    }
    if not len(grid):
        # No full-session claim and no dated-universe/file requirement before
        # a session's first completed candle or on a closed calendar date.
        return {**base, "overall": coverage_state, "exit_code": 0}
    if not data_dir.exists():
        return {**base, "error": f"dir not found: {data_dir}", "overall": "FAIL", "exit_code": 2}
    parquet_files = list(data_dir.glob("*.parquet"))
    if not parquet_files and not required:
        return {**base, "error": f"no parquet files in {data_dir}", "overall": "FAIL", "exit_code": 2}
    if required:
        parquet_files = [path for path in parquet_files if _ticker_from_path(path) in required]

    target_date = pd.Timestamp(date_str).date()
    closing_verified = 0
    closing_failed = 0
    ticker_stats: dict[str, dict[str, Any]] = {}
    ok, warn, fail, dup_files = 0, 0, 0, 0

    if required:
        present = {_ticker_from_path(path) for path in parquet_files}
        for ticker in sorted(required - present):
            ticker_stats[ticker] = {
                "bars": 0,
                "unique_bars": 0,
                "duplicates": 0,
                "status": "FAIL",
                "missing_candles": len(grid),
                "missing_timestamps": [stamp.isoformat() for stamp in grid],
                "stale_min": None,
                "error": "required ticker parquet missing",
                "closing_bar": "MISSING_FILE" if closing_required else closing_requirement,
            }
            fail += 1
            closing_failed += int(closing_required)

    for fpath in parquet_files:
        ticker = _ticker_from_path(fpath)
        try:
            if ticker in ticker_stats:
                raise ValueError(f"Multiple parquet files resolve to ticker {ticker}")
            frame, dt_col = _read_quality_frame(fpath)
            # Future rows do not enter the current audit. The currently forming
            # bucket is outside the due timestamp grid and cannot fill a gap.
            day_mask = dt_col.dt.date.eq(target_date) & dt_col.le(current)
            day_ts = dt_col[day_mask]
            bar_count = int(day_ts.shape[0])
            duplicates = int(day_ts.duplicated().sum())
            opening = day_ts.eq(pd.Timestamp(f"{date_str} 09:15", tz="Asia/Kolkata")) if five_minute else pd.Series(False, index=day_ts.index)
            due_mask = day_ts.isin(grid)
            due_frame = frame.loc[day_ts.index[due_mask]]
            due_ts = day_ts[due_mask]
            invalid, reasons = _invalid_candles(due_frame, five_minute=five_minute)
            observed = pd.DatetimeIndex(due_ts.unique())
            missing = grid.difference(observed)
            valid = pd.DatetimeIndex(due_ts.loc[~invalid].unique())
            unique_bars = len(observed)
            off_grid = day_ts.loc[~due_mask & ~opening]
            invalid_timestamps = int(dt_col.isna().sum())
            stale_sec = time.time() - fpath.stat().st_mtime
            stale_min = round(stale_sec / 60, 1)
            pct = len(valid) / len(grid)
            closing_status = _closing_status_from_frame(frame, dt_col, date_str, five_minute=five_minute) if closing_required else closing_requirement
            if closing_required:
                closing_verified += int(closing_status == "VERIFIED")
                closing_failed += int(closing_status != "VERIFIED")
            if duplicates > 0:
                status = "DUP"
                fail += 1
                dup_files += 1
            elif len(missing) or invalid.any() or len(off_grid) or invalid_timestamps or (closing_required and closing_status != "VERIFIED"):
                status = "FAIL"
                fail += 1
            else:
                status = "OK"
                ok += 1
            ticker_stats[ticker] = {
                "bars": bar_count,
                "unique_bars": unique_bars,
                "valid_candles": len(valid),
                "opening_snapshots_ignored": int(opening.sum()),
                "duplicates": duplicates,
                "pct_of_expected": round(100.0 * pct, 1),
                "missing_candles": len(missing),
                "missing_timestamps": [stamp.isoformat() for stamp in missing],
                "invalid_candles": int(invalid.sum()),
                "invalid_timestamps": invalid_timestamps,
                "invalid_reasons": reasons,
                "invalid_candle_timestamps": [stamp.isoformat() for stamp in due_ts.loc[invalid]],
                "off_grid_candles": len(off_grid),
                "off_grid_timestamps": [stamp.isoformat() for stamp in off_grid],
                "future_rows_ignored": int((dt_col.dt.date.eq(target_date) & dt_col.gt(current)).sum()),
                "status": status,
                "stale_min": stale_min,
                "closing_bar": closing_status,
            }
            if any(ticker.startswith(p) for p in MARKET_FILE_PREFIXES) and ticker not in INTENTIONAL_INDEX_TICKERS:
                anomaly = _market_file_anomaly(fpath)
                if anomaly:
                    ticker_stats[ticker]["market_anomaly"] = anomaly
                    if status == "OK":
                        ticker_stats[ticker]["status"] = "WARN"
                        ok -= 1
                        warn += 1
        except Exception as e:
            ticker_stats[ticker] = {"bars": -1, "status": "ERROR", "error": str(e), "stale_min": None}
            fail += 1
            closing_failed += int(closing_required)

    total = ok + warn + fail
    missing_pct = (warn + fail) / max(total, 1)
    overall, exit_code = ("FAIL", 2) if fail else ("WARN", 1) if warn else ("PASS", 0)

    worst = sorted(
        [(t, s) for t, s in ticker_stats.items() if s["status"] != "OK"],
        key=lambda x: x[1].get("bars", -1),
    )[:20]

    return {
        **base,
        "total_tickers": total,
        "ok": ok,
        "warn": warn,
        "fail": fail,
        "dup_files": dup_files,
        "missing_pct": round(100.0 * missing_pct, 1),
        "overall": overall,
        "exit_code": exit_code,
        "worst_tickers": {t: s for t, s in worst},
        "required_tickers": len(required) if required else None,
        "closing_bar_requirement": closing_requirement,
        "closing_bar_verified": closing_verified,
        "closing_bar_failed": closing_failed,
    }


def _fno_equity_universe(date_str: str) -> tuple[Set[str], Path]:
    universe_path = runtime_dir("fno_oi", "universe", f"near_month_{date_str}.parquet")
    if not universe_path.exists():
        raise FileNotFoundError(f"dated FnO universe not found: {universe_path}")
    frame = pd.read_parquet(universe_path)
    if "equity_symbol" not in frame.columns:
        raise ValueError(f"FnO universe lacks equity_symbol: {universe_path}")
    if "is_index_future" in frame.columns:
        frame = frame.loc[~frame["is_index_future"].fillna(False).astype(bool)]
    symbols = {
        str(value).strip().upper()
        for value in frame["equity_symbol"].dropna().tolist()
        if str(value).strip()
    }
    if not symbols:
        raise ValueError(f"FnO universe has no mapped stock equities: {universe_path}")
    return symbols, universe_path


def run_verify(date_str: str, scope: str = "all", *, now: Any = None) -> int:
    print(f"\n{'='*70}")
    print(f" Data Completeness Verify — {date_str}")
    print(f"{'='*70}")

    scope_key = str(scope or "all").strip().lower()
    current = _now_ist(now)
    required_tickers: Optional[Set[str]] = None
    universe_path: Optional[Path] = None
    due_minutes, _ = _completed_grid(date_str, five_minute=False, now=current)
    if scope_key == "fno" and len(due_minutes):
        required_tickers, universe_path = _fno_equity_universe(date_str)
        print(f" Scope: dated FnO stock-equity universe ({len(required_tickers)} symbols)")
        print(f" Universe: {universe_path}")
    result_5m = _scan_parquet_dir(
        DATA_5M_DIR,
        date_str,
        EXPECTED_5M_BARS,
        required_tickers=required_tickers,
        now=current,
    )
    result_1m = _scan_parquet_dir(
        DATA_1MIN_DIR,
        date_str,
        EXPECTED_1MIN_BARS,
        required_tickers=required_tickers,
        now=current,
    )

    overall_exit = max(result_5m.get("exit_code", 2), result_1m.get("exit_code", 2))
    overall_status = {0: "PASS", 1: "WARN", 2: "FAIL"}.get(overall_exit, "FAIL")
    if not len(due_minutes):
        overall_status = result_1m["coverage_state"]

    payload = {
        "date": date_str,
        "scope": scope_key,
        "required_ticker_count": len(required_tickers or set()),
        "universe_path": str(universe_path) if universe_path else "",
        "overall_status": overall_status,
        "overall_exit_code": overall_exit,
        "5min": result_5m,
        "1min": result_1m,
        "run_at_ist": current.strftime("%Y-%m-%d %H:%M:%S %Z"),
    }

    out_path = VERIFY_DIR / f"data_verify_{date_str}.json"
    latest_path = VERIFY_DIR / "data_verify_latest.json"
    out_path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    latest_path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")

    def _print_section(label: str, r: dict[str, Any]) -> None:
        status = r.get("overall", r.get("error", "ERROR"))
        total = r.get("total_tickers", 0)
        ok = r.get("ok", 0)
        warn_ct = r.get("warn", 0)
        fail_ct = r.get("fail", 0)
        missing = r.get("missing_pct", 100.0)
        print(f"\n  [{label}] {r.get('dir', '')}")
        print(f"    status       : {status}")
        print(f"    tickers      : {total} total | {ok} OK | {warn_ct} WARN | {fail_ct} FAIL")
        print(f"    non_OK_pct   : {missing:.1f}%  (every required ticker must pass)")
        print(f"    candle_grid  : {r.get('expected_bars', 0)} completed candles | {r.get('coverage_state', 'UNKNOWN')}")
        print(f"    closing_bar  : {r.get('closing_bar_requirement', 'UNKNOWN')} | verified={r.get('closing_bar_verified', 0)} | missing/invalid={r.get('closing_bar_failed', 0)}")
        if "error" in r:
            print(f"    ERROR        : {r['error']}")
        worst = r.get("worst_tickers", {})
        if worst:
            print(f"    worst tickers (non-OK):")
            for ticker, st in list(worst.items())[:10]:
                bars = st.get("bars", "?")
                pct = st.get("pct_of_expected", "?")
                stale = st.get("stale_min", "?")
                print(f"      {ticker:<15} bars={bars}  pct={pct}%  stale={stale}min  status={st.get('status','?')}  close={st.get('closing_bar', 'UNKNOWN')}")

    _print_section("5MIN", result_5m)
    _print_section("1MIN", result_1m)

    print(f"\n  OVERALL: {overall_status} (exit_code={overall_exit})")
    print(f"  Written: {out_path}")

    if overall_exit == 2:
        print(
            "\n  ACTION: Required real candles are missing or invalid. Review the listed "
            "timestamps and source-quality findings before targeted recovery."
        )
    elif overall_exit == 1:
        print(
            "\n  WARNING: Market-context data anomalies require review; this is not a clean pass."
        )
    else:
        if overall_status in {"NOT_DUE", "NOT_A_SESSION"}:
            print("\n  No regular-session candles are due; no full-session certification was made.")
        elif result_1m["coverage_state"] == "IN_PROGRESS":
            print("\n  All currently completed candles passed; the session is still in progress.")
        else:
            print("\n  Cash candle coverage and quality passed. Strategy/OI readiness requires its own audit.")

    print()
    return overall_exit


def _default_day() -> str:
    return pd.Timestamp.now(tz="Asia/Kolkata").strftime("%Y-%m-%d")


def main() -> int:
    ap = argparse.ArgumentParser(description="Verify exact completed cash candles and source quality.")
    ap.add_argument("--date", default=_default_day(), help="Trading date YYYY-MM-DD")
    ap.add_argument(
        "--scope",
        choices=("all", "fno"),
        default="all",
        help="Verify every archived ticker or only the dated mapped FnO stock-equity universe",
    )
    ap.add_argument("--no-fail", action="store_true", help="Explicit legacy override: always exit 0; report still records failures")
    args = ap.parse_args()
    code = run_verify(str(args.date), scope=str(args.scope))
    if args.no_fail:
        print(f"  EXPLICIT --no-fail override: report exit_code={code}; process exit_code=0")
        return 0
    return code


if __name__ == "__main__":
    raise SystemExit(main())
