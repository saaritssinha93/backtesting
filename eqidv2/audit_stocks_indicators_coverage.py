#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Audit stocks_indicators_* data coverage across all timeframes.

Outputs:
1) timeframe_summary.csv         -> one row per timeframe
2) ticker_tf_coverage.csv        -> one row per (ticker, timeframe), with start/end/rows
3) missing_by_timeframe.csv      -> one row per missing ticker-timeframe
4) ticker_tf_matrix.csv          -> one row per ticker, columns for each timeframe (has/start/end/rows)
5) summary.json                  -> compact run metadata

Default ticker universe source (in order):
- filtered_stocks_MIS.py (stocks_tokens or selected_stocks)
- stocks_tickers.txt
- union of tickers present across all timeframe folders
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional

import pandas as pd
import pytz

IST = pytz.timezone("Asia/Kolkata")

TF_ORDER = ["1min", "5min", "15min", "1hr", "3hr", "daily", "weekly"]
DIR_PREFIX = "stocks_indicators_"
DIR_SUFFIX = "_eq"
FILE_RE = re.compile(r"^(?P<ticker>.+?)_stocks_indicators_(?P<tf>[A-Za-z0-9]+)\.(?P<ext>parquet|csv)$")


@dataclass
class FileStat:
    ticker: str
    timeframe: str
    file_path: str
    rows: Optional[int]
    start_ts: Optional[pd.Timestamp]
    end_ts: Optional[pd.Timestamp]
    error: Optional[str]


def _tf_sort_key(tf: str) -> tuple[int, str]:
    try:
        return (TF_ORDER.index(tf), tf)
    except ValueError:
        return (10_000, tf)


def _normalize_tickers(obj) -> list[str]:
    if obj is None:
        return []
    if isinstance(obj, dict):
        arr = list(obj.keys())
    elif isinstance(obj, (set, list, tuple)):
        arr = list(obj)
    elif isinstance(obj, str):
        arr = re.split(r"[\s,;]+", obj.strip())
    else:
        try:
            arr = list(obj)
        except Exception:
            arr = [obj]

    out: set[str] = set()
    for x in arr:
        s = str(x).strip().upper()
        if not s:
            continue
        s = s.replace("NSE:", "").replace("BSE:", "")
        out.add(s)
    return sorted(out)


def load_universe(base_dir: Path) -> tuple[list[str], str]:
    script_dir = base_dir.resolve()
    if str(script_dir) not in sys.path:
        sys.path.insert(0, str(script_dir))

    try:
        import importlib

        mod = importlib.import_module("filtered_stocks_MIS")
        if hasattr(mod, "stocks_tokens") and isinstance(getattr(mod, "stocks_tokens"), dict):
            t = _normalize_tickers(getattr(mod, "stocks_tokens"))
            if t:
                return t, "filtered_stocks_MIS.stocks_tokens"
        if hasattr(mod, "selected_stocks"):
            t = _normalize_tickers(getattr(mod, "selected_stocks"))
            if t:
                return t, "filtered_stocks_MIS.selected_stocks"
    except Exception:
        pass

    txt = script_dir / "stocks_tickers.txt"
    if txt.exists():
        lines = txt.read_text(encoding="utf-8", errors="ignore").splitlines()
        t = _normalize_tickers(lines)
        if t:
            return t, "stocks_tickers.txt"

    return [], "union_fallback"


def discover_tf_dirs(base_dir: Path) -> dict[str, Path]:
    out: dict[str, Path] = {}
    for d in base_dir.iterdir():
        if not d.is_dir():
            continue
        name = d.name
        if not (name.startswith(DIR_PREFIX) and name.endswith(DIR_SUFFIX)):
            continue
        tf = name[len(DIR_PREFIX) : -len(DIR_SUFFIX)]
        if tf:
            out[tf] = d
    return dict(sorted(out.items(), key=lambda kv: _tf_sort_key(kv[0])))


def _to_ist_timestamp(value) -> Optional[pd.Timestamp]:
    ts = pd.to_datetime(value, errors="coerce")
    if pd.isna(ts):
        return None
    if ts.tzinfo is None:
        return ts.tz_localize(IST)
    return ts.tz_convert(IST)


def _read_csv_first_last(path: Path) -> tuple[Optional[pd.Timestamp], Optional[pd.Timestamp], Optional[int], Optional[str]]:
    try:
        df = pd.read_csv(path, usecols=["date"])
        if df.empty:
            return None, None, 0, None
        s = pd.to_datetime(df["date"], errors="coerce").dropna()
        if s.empty:
            return None, None, int(len(df)), "date column parse failed"
        first = _to_ist_timestamp(s.iloc[0])
        last = _to_ist_timestamp(s.iloc[-1])
        return first, last, int(len(df)), None
    except Exception as e:
        return None, None, None, str(e)


def _read_parquet_first_last(path: Path) -> tuple[Optional[pd.Timestamp], Optional[pd.Timestamp], Optional[int], Optional[str]]:
    try:
        import pyarrow.parquet as pq

        pf = pq.ParquetFile(path)
        md = pf.metadata
        if md is None or md.num_rows <= 0:
            return None, None, 0, None

        rg_first = 0
        rg_last = md.num_row_groups - 1

        t_first = pf.read_row_group(rg_first, columns=["date"])
        t_last = pf.read_row_group(rg_last, columns=["date"])

        if t_first.num_rows <= 0 or t_last.num_rows <= 0:
            return None, None, int(md.num_rows), "empty row group for date column"

        first_val = t_first.column(0)[0].as_py()
        last_col = t_last.column(0)
        last_val = last_col[last_col.length() - 1].as_py()

        first = _to_ist_timestamp(first_val)
        last = _to_ist_timestamp(last_val)
        return first, last, int(md.num_rows), None
    except Exception:
        try:
            df = pd.read_parquet(path, columns=["date"])
            if df.empty:
                return None, None, 0, None
            s = pd.to_datetime(df["date"], errors="coerce").dropna()
            if s.empty:
                return None, None, int(len(df)), "date column parse failed"
            first = _to_ist_timestamp(s.iloc[0])
            last = _to_ist_timestamp(s.iloc[-1])
            return first, last, int(len(df)), None
        except Exception as e2:
            return None, None, None, str(e2)


def read_first_last(path: Path) -> tuple[Optional[pd.Timestamp], Optional[pd.Timestamp], Optional[int], Optional[str]]:
    if path.suffix.lower() == ".parquet":
        return _read_parquet_first_last(path)
    return _read_csv_first_last(path)


def collect_tf_files(tf: str, tf_dir: Path) -> dict[str, Path]:
    """
    Returns ticker -> file_path, preferring parquet over csv when both exist.
    """
    out: dict[str, Path] = {}
    for p in tf_dir.iterdir():
        if not p.is_file():
            continue
        m = FILE_RE.match(p.name)
        if not m:
            continue
        ticker = m.group("ticker").upper()
        file_tf = m.group("tf")
        ext = m.group("ext").lower()
        if file_tf != tf:
            continue

        prev = out.get(ticker)
        if prev is None:
            out[ticker] = p
            continue

        if prev.suffix.lower() != ".parquet" and ext == "parquet":
            out[ticker] = p
    return out


def fmt_ts(ts: Optional[pd.Timestamp]) -> Optional[str]:
    if ts is None:
        return None
    return ts.strftime("%Y-%m-%d %H:%M:%S%z")


def build_matrix(coverage_df: pd.DataFrame, tfs: list[str], universe: list[str]) -> pd.DataFrame:
    idx = pd.Index(sorted(universe), name="ticker")
    out = pd.DataFrame(index=idx).reset_index()

    for tf in tfs:
        sub = coverage_df[coverage_df["timeframe"] == tf].copy()
        sub = sub.drop_duplicates(subset=["ticker"], keep="last")
        mapper_has = dict(zip(sub["ticker"], sub["has_data"]))
        mapper_start = dict(zip(sub["ticker"], sub["start_ts"]))
        mapper_end = dict(zip(sub["ticker"], sub["end_ts"]))
        mapper_rows = dict(zip(sub["ticker"], sub["rows"]))

        out[f"{tf}_has"] = out["ticker"].map(mapper_has).fillna(False)
        out[f"{tf}_start"] = out["ticker"].map(mapper_start)
        out[f"{tf}_end"] = out["ticker"].map(mapper_end)
        out[f"{tf}_rows"] = out["ticker"].map(mapper_rows)

    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--base-dir",
        default=str(Path(__file__).resolve().parent),
        help="Base directory containing stocks_indicators_*_eq folders",
    )
    ap.add_argument(
        "--out-dir",
        default="reports/stocks_indicators_audit",
        help="Output directory for audit CSV/JSON reports",
    )
    ap.add_argument(
        "--max-workers",
        type=int,
        default=16,
        help="Parallel workers for reading first/last timestamps",
    )
    ap.add_argument(
        "--missing-preview",
        type=int,
        default=25,
        help="How many missing tickers to print per timeframe in console",
    )
    args = ap.parse_args()

    base_dir = Path(args.base_dir).resolve()
    out_dir = (base_dir / args.out_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    tf_dirs = discover_tf_dirs(base_dir)
    if not tf_dirs:
        print(f"[ERROR] No timeframe folders found under: {base_dir}")
        return 1

    expected_tickers, universe_source = load_universe(base_dir)

    tf_files: dict[str, dict[str, Path]] = {}
    all_present: set[str] = set()
    for tf, d in tf_dirs.items():
        mp = collect_tf_files(tf, d)
        tf_files[tf] = mp
        all_present.update(mp.keys())

    if expected_tickers:
        universe = sorted(set(expected_tickers))
    else:
        universe = sorted(all_present)
        universe_source = "union_of_present_tickers"

    jobs = []
    for tf, mp in tf_files.items():
        for ticker, path in mp.items():
            jobs.append((tf, ticker, path))

    stats: list[FileStat] = []
    max_workers = max(1, int(args.max_workers))
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futs = {
            ex.submit(read_first_last, path): (tf, ticker, path)
            for (tf, ticker, path) in jobs
        }
        for fut in as_completed(futs):
            tf, ticker, path = futs[fut]
            try:
                start_ts, end_ts, rows, err = fut.result()
            except Exception as e:
                start_ts, end_ts, rows, err = None, None, None, str(e)
            stats.append(
                FileStat(
                    ticker=ticker,
                    timeframe=tf,
                    file_path=str(path),
                    rows=rows,
                    start_ts=start_ts,
                    end_ts=end_ts,
                    error=err,
                )
            )

    stats_df = pd.DataFrame(
        [
            {
                "ticker": s.ticker,
                "timeframe": s.timeframe,
                "file_path": s.file_path,
                "rows": s.rows,
                "start_ts": fmt_ts(s.start_ts),
                "end_ts": fmt_ts(s.end_ts),
                "error": s.error,
                "has_data": bool(s.start_ts is not None and s.end_ts is not None),
            }
            for s in stats
        ]
    )

    if stats_df.empty:
        stats_df = pd.DataFrame(
            columns=["ticker", "timeframe", "file_path", "rows", "start_ts", "end_ts", "error", "has_data"]
        )

    # Ensure missing rows are explicitly represented for full matrix coverage.
    full_rows = []
    for tf in tf_dirs.keys():
        present = set(tf_files.get(tf, {}).keys())
        for t in universe:
            if t not in present:
                full_rows.append(
                    {
                        "ticker": t,
                        "timeframe": tf,
                        "file_path": None,
                        "rows": None,
                        "start_ts": None,
                        "end_ts": None,
                        "error": "missing_file",
                        "has_data": False,
                    }
                )

    if full_rows:
        stats_df = pd.concat([stats_df, pd.DataFrame(full_rows)], ignore_index=True)

    stats_df = stats_df.sort_values(["timeframe", "ticker"], ascending=[True, True]).reset_index(drop=True)

    # Missing list per timeframe.
    missing_rows = stats_df[(stats_df["has_data"] == False) & (stats_df["error"] == "missing_file")][
        ["timeframe", "ticker"]
    ].copy()
    missing_rows = missing_rows.sort_values(["timeframe", "ticker"]).reset_index(drop=True)

    # Timeframe summary.
    summary_rows = []
    for tf in sorted(tf_dirs.keys(), key=_tf_sort_key):
        sub = stats_df[stats_df["timeframe"] == tf]
        present_sub = sub[sub["has_data"] == True]
        missing_count = int((sub["has_data"] == False).sum())

        tf_start = None
        tf_end = None
        if not present_sub.empty:
            start_col = pd.to_datetime(present_sub["start_ts"], errors="coerce")
            end_col = pd.to_datetime(present_sub["end_ts"], errors="coerce")
            smin = start_col.dropna().min()
            emax = end_col.dropna().max()
            tf_start = smin.strftime("%Y-%m-%d %H:%M:%S%z") if pd.notna(smin) else None
            tf_end = emax.strftime("%Y-%m-%d %H:%M:%S%z") if pd.notna(emax) else None

        summary_rows.append(
            {
                "timeframe": tf,
                "universe_tickers": len(universe),
                "present_tickers": int(present_sub["ticker"].nunique()),
                "missing_tickers": missing_count,
                "earliest_start_ts": tf_start,
                "latest_end_ts": tf_end,
                "files_dir": str(tf_dirs[tf]),
            }
        )

    summary_df = pd.DataFrame(summary_rows).sort_values("timeframe", key=lambda s: s.map(lambda x: _tf_sort_key(x)))

    matrix_df = build_matrix(stats_df, list(tf_dirs.keys()), universe)

    # Save reports.
    coverage_csv = out_dir / "ticker_tf_coverage.csv"
    missing_csv = out_dir / "missing_by_timeframe.csv"
    tf_summary_csv = out_dir / "timeframe_summary.csv"
    matrix_csv = out_dir / "ticker_tf_matrix.csv"
    summary_json = out_dir / "summary.json"

    stats_df.to_csv(coverage_csv, index=False)
    missing_rows.to_csv(missing_csv, index=False)
    summary_df.to_csv(tf_summary_csv, index=False)
    matrix_df.to_csv(matrix_csv, index=False)

    payload = {
        "generated_at_ist": datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S%z"),
        "base_dir": str(base_dir),
        "out_dir": str(out_dir),
        "timeframes_found": list(tf_dirs.keys()),
        "universe_source": universe_source,
        "universe_count": len(universe),
        "coverage_rows": int(len(stats_df)),
        "missing_rows": int(len(missing_rows)),
        "reports": {
            "timeframe_summary_csv": str(tf_summary_csv),
            "ticker_tf_coverage_csv": str(coverage_csv),
            "missing_by_timeframe_csv": str(missing_csv),
            "ticker_tf_matrix_csv": str(matrix_csv),
        },
    }
    summary_json.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    # Console summary.
    print("=" * 96)
    print(f"Stocks Indicators Coverage Audit | base_dir={base_dir}")
    print(f"Universe source: {universe_source} | tickers={len(universe)} | tfs={len(tf_dirs)}")
    print("-" * 96)
    for _, r in summary_df.iterrows():
        tf = r["timeframe"]
        print(
            f"{tf:>7} | present={int(r['present_tickers'])}/{int(r['universe_tickers'])} | "
            f"missing={int(r['missing_tickers'])} | start={r['earliest_start_ts']} | end={r['latest_end_ts']}"
        )
        miss = missing_rows[missing_rows["timeframe"] == tf]["ticker"].tolist()
        if miss:
            preview = ", ".join(miss[: max(0, int(args.missing_preview))])
            print(f"        missing_sample: {preview}")
    print("-" * 96)
    print(f"Saved: {tf_summary_csv}")
    print(f"Saved: {coverage_csv}")
    print(f"Saved: {missing_csv}")
    print(f"Saved: {matrix_csv}")
    print(f"Saved: {summary_json}")
    print("=" * 96)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

