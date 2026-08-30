r"""
Data completeness check for V7 backtesting.

Scans the 5-min and 1-min parquet directories for the target date
and reports how many tickers have sufficient bars.

Writes:
  C:\TradingData\eqidv2\backtesting_result_v11\latest\data_verify_{date}.json
  C:\TradingData\eqidv2\backtesting_result_v11\latest\data_verify_latest.json

Exit codes:
  0 — PASS  (>= 80% of expected bars for >= 90% of tickers)
  1 — WARN  (some tickers short, majority OK)
  2 — FAIL  (too many tickers missing or critically short)
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

VERIFY_DIR = runtime_dir("backtesting_result_v11", "latest")
VERIFY_DIR.mkdir(parents=True, exist_ok=True)

# Expected intraday bars. Live 5m hybrid convention = 76 unique bars:
#   09:15 (opening snapshot) + 09:20,09:25,...,15:30 (75 completed end-stamped
#   5m candles). The old value of 75 dropped the 15:30 close bar.
EXPECTED_5M_BARS = 76
EXPECTED_1MIN_BARS = 375  # 1-min: 9:15–15:29 → 375 slots
MIN_BAR_PCT = 0.80       # a ticker is "OK" if it has >= 80% of expected bars

# Market/regime context files (NIFTY proxy/index). A true zero-volume index or a
# wrong price scale (~24,000 instead of ETF ~268) leaking in here corrupts the
# VWAP regime, so these get extra sanity checks.
MARKET_FILE_PREFIXES = ("NIFTY", "NIFTYBEES")
MARKET_SCALE_MAX_CLOSE = 5000.0  # ETF proxy ~ hundreds; true NIFTY 50 index ~ tens of thousands
# Aliases that are INTENTIONALLY a true zero-volume index (research only, not a
# regime source) — exempt from the zero-volume/scale anomaly check.
INTENTIONAL_INDEX_TICKERS = {"NIFTY50_INDEX"}
_STORE_SUFFIXES = ("_stocks_indicators_5min", "_stocks_indicators_1min")

WARN_MISSING_TICKERS_PCT = 0.15   # <15% missing tickers → PASS
FAIL_MISSING_TICKERS_PCT = 0.35   # >=35% missing tickers → FAIL


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
) -> dict[str, Any]:
    """Scan a parquet directory and return bar counts for target date."""
    if not data_dir.exists():
        return {"error": f"dir not found: {data_dir}", "tickers": {}, "total": 0, "ok": 0, "warn": 0, "fail": 0, "dup_files": 0}

    parquet_files = list(data_dir.glob("*.parquet"))
    if not parquet_files:
        return {"error": f"no parquet files in {data_dir}", "tickers": {}, "total": 0, "ok": 0, "warn": 0, "fail": 0, "dup_files": 0}

    required = {str(ticker).strip().upper() for ticker in (required_tickers or set()) if str(ticker).strip()}
    if required:
        files_by_ticker = {_ticker_from_path(path): path for path in parquet_files}
        parquet_files = [files_by_ticker[ticker] for ticker in sorted(required) if ticker in files_by_ticker]

    target_date = pd.Timestamp(date_str).date()
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
                "stale_min": None,
                "error": "required ticker parquet missing",
            }
            fail += 1

    for fpath in parquet_files:
        ticker = _ticker_from_path(fpath)
        try:
            dt_col = _read_parquet_timestamps(fpath)
            if dt_col.empty:
                ticker_stats[ticker] = {"bars": 0, "unique_bars": 0, "duplicates": 0, "status": "FAIL", "stale_min": None}
                fail += 1
                continue
            day_mask = dt_col.dt.date == target_date
            day_ts = dt_col[day_mask]
            bar_count = int(day_ts.shape[0])
            unique_bars = int(day_ts.nunique())
            duplicates = bar_count - unique_bars
            stale_sec = time.time() - fpath.stat().st_mtime
            stale_min = round(stale_sec / 60, 1)
            pct = unique_bars / expected_bars  # unique timestamps, not raw rows
            if duplicates > 0:
                # Duplicate timestamps for the day -> data integrity problem
                # (moving_files full-row append / proxy+index scale-mix). Fail it.
                status = "DUP"
                fail += 1
                dup_files += 1
            elif pct >= MIN_BAR_PCT:
                status = "OK"
                ok += 1
            elif pct >= 0.5:
                status = "WARN"
                warn += 1
            else:
                status = "FAIL"
                fail += 1
            ticker_stats[ticker] = {
                "bars": bar_count,
                "unique_bars": unique_bars,
                "duplicates": duplicates,
                "pct_of_expected": round(100.0 * pct, 1),
                "status": status,
                "stale_min": stale_min,
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

    total = ok + warn + fail
    missing_pct = (warn + fail) / max(total, 1)
    if missing_pct < WARN_MISSING_TICKERS_PCT:
        overall = "PASS"
        exit_code = 0
    elif missing_pct < FAIL_MISSING_TICKERS_PCT:
        overall = "WARN"
        exit_code = 1
    else:
        overall = "FAIL"
        exit_code = 2

    # Any duplicate-timestamp file is a data-integrity problem; never report PASS.
    if dup_files > 0 and exit_code == 0:
        overall = "WARN"
        exit_code = 1

    worst = sorted(
        [(t, s) for t, s in ticker_stats.items() if s["status"] != "OK"],
        key=lambda x: x[1].get("bars", -1),
    )[:20]

    return {
        "dir": str(data_dir),
        "date": date_str,
        "expected_bars": expected_bars,
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


def run_verify(date_str: str, scope: str = "all") -> int:
    print(f"\n{'='*70}")
    print(f" Data Completeness Verify — {date_str}")
    print(f"{'='*70}")

    scope_key = str(scope or "all").strip().lower()
    required_tickers: Optional[Set[str]] = None
    universe_path: Optional[Path] = None
    if scope_key == "fno":
        required_tickers, universe_path = _fno_equity_universe(date_str)
        print(f" Scope: dated FnO stock-equity universe ({len(required_tickers)} symbols)")
        print(f" Universe: {universe_path}")
    result_5m = _scan_parquet_dir(
        DATA_5M_DIR,
        date_str,
        EXPECTED_5M_BARS,
        required_tickers=required_tickers,
    )
    result_1m = _scan_parquet_dir(
        DATA_1MIN_DIR,
        date_str,
        EXPECTED_1MIN_BARS,
        required_tickers=required_tickers,
    )

    overall_exit = max(result_5m.get("exit_code", 2), result_1m.get("exit_code", 2))
    overall_status = {0: "PASS", 1: "WARN", 2: "FAIL"}.get(overall_exit, "FAIL")

    payload = {
        "date": date_str,
        "scope": scope_key,
        "required_ticker_count": len(required_tickers or set()),
        "universe_path": str(universe_path) if universe_path else "",
        "overall_status": overall_status,
        "overall_exit_code": overall_exit,
        "5min": result_5m,
        "1min": result_1m,
        "run_at_ist": pd.Timestamp.now(tz="Asia/Kolkata").strftime("%Y-%m-%d %H:%M:%S %Z"),
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
        print(f"    missing_pct  : {missing:.1f}%  (WARN threshold={WARN_MISSING_TICKERS_PCT*100:.0f}%, FAIL={FAIL_MISSING_TICKERS_PCT*100:.0f}%)")
        if "error" in r:
            print(f"    ERROR        : {r['error']}")
        worst = r.get("worst_tickers", {})
        if worst:
            print(f"    worst tickers (non-OK):")
            for ticker, st in list(worst.items())[:10]:
                bars = st.get("bars", "?")
                pct = st.get("pct_of_expected", "?")
                stale = st.get("stale_min", "?")
                print(f"      {ticker:<15} bars={bars}  pct={pct}%  stale={stale}min  status={st.get('status','?')}")

    _print_section("5MIN", result_5m)
    _print_section("1MIN", result_1m)

    print(f"\n  OVERALL: {overall_status} (exit_code={overall_exit})")
    print(f"  Written: {out_path}")

    if overall_exit == 2:
        print(
            "\n  ACTION: Too many tickers missing bars. "
            "Run trading_data_continous_run_historical_alltf_v3_parquet_stocksonly_1min.py "
            "before backtesting, or check NSE data feed."
        )
    elif overall_exit == 1:
        print(
            "\n  WARNING: Some tickers have incomplete data. "
            "Backtesting may proceed but results for affected tickers may be unreliable. "
            "Worst tickers listed above."
        )
    else:
        print("\n  Data completeness OK — safe to run backtesting.")

    print()
    return overall_exit


def _default_day() -> str:
    return pd.Timestamp.now(tz="Asia/Kolkata").strftime("%Y-%m-%d")


def main() -> int:
    ap = argparse.ArgumentParser(description="Verify data completeness for V7 backtesting.")
    ap.add_argument("--date", default=_default_day(), help="Trading date YYYY-MM-DD")
    ap.add_argument(
        "--scope",
        choices=("all", "fno"),
        default="all",
        help="Verify every archived ticker or only the dated mapped FnO stock-equity universe",
    )
    ap.add_argument("--no-fail", action="store_true", help="Always exit 0 (WARN mode only)")
    args = ap.parse_args()
    code = run_verify(str(args.date), scope=str(args.scope))
    if args.no_fail:
        return 0
    return code


if __name__ == "__main__":
    raise SystemExit(main())
