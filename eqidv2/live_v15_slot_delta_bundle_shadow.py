from __future__ import annotations

import argparse
import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import pandas as pd

import eqidv2_live_combined_analyser_csv_v15 as base_v15
from live_v15_slot_snapshot_v15_new import (
    DEFAULT_SNAPSHOT_MAX_WORKERS,
    DEFAULT_TAIL_ROWS,
    END_15M,
    load_combined_shard_map,
    normalize_slot_ist,
    slot_bundle_dir,
    slot_bundle_parquet_path,
    slot_key,
)
from eqidv2_runtime_paths import DATA_15M_DIR, RUNTIME_ROOT


DEFAULT_BUNDLE_READ_TAIL_ROWS = max(8, min(int(DEFAULT_TAIL_ROWS), 32))


def _read_one_ticker_slot_row(
    ticker: str,
    source_dir: Path,
    slot_ist: pd.Timestamp,
    tail_rows: int,
) -> Tuple[str, pd.DataFrame, Dict[str, Any]]:
    started = time.perf_counter()
    ticker_u = str(ticker).strip().upper()
    path = source_dir / f"{ticker_u}{END_15M}"
    meta: Dict[str, Any] = {
        "ticker": ticker_u,
        "source_path": str(path),
        "rows": 0,
        "elapsed_sec": 0.0,
        "error": "",
    }
    if not path.exists():
        meta["error"] = "missing_file"
        meta["elapsed_sec"] = round(time.perf_counter() - started, 6)
        return ticker_u, pd.DataFrame(), meta

    try:
        df = base_v15.read_parquet_tail(str(path), n=int(tail_rows))
        if df is None or df.empty:
            meta["elapsed_sec"] = round(time.perf_counter() - started, 6)
            return ticker_u, pd.DataFrame(), meta
        df = base_v15.normalize_dates(df)
        if df.empty or "date" not in df.columns:
            meta["elapsed_sec"] = round(time.perf_counter() - started, 6)
            return ticker_u, pd.DataFrame(), meta
        df = df.sort_values("date")
        slot_df = df.loc[df["date"] == slot_ist].copy()
        if slot_df.empty:
            meta["elapsed_sec"] = round(time.perf_counter() - started, 6)
            return ticker_u, pd.DataFrame(), meta
        slot_df["ticker"] = ticker_u
        meta["rows"] = int(len(slot_df))
        meta["elapsed_sec"] = round(time.perf_counter() - started, 6)
        return ticker_u, slot_df.reset_index(drop=True), meta
    except Exception as exc:
        meta["error"] = repr(exc)
        meta["elapsed_sec"] = round(time.perf_counter() - started, 6)
        return ticker_u, pd.DataFrame(), meta


def build_slot_delta_bundle(
    slot_ts: Any,
    *,
    runtime_root: Optional[Path | str] = None,
    source_dir: Optional[Path | str] = None,
    shard_count: int = 10,
    max_workers: int = DEFAULT_SNAPSHOT_MAX_WORKERS,
    tail_rows: int = DEFAULT_BUNDLE_READ_TAIL_ROWS,
) -> Dict[str, Any]:
    slot_ist = normalize_slot_ist(slot_ts)
    source_root = Path(source_dir) if source_dir is not None else Path(DATA_15M_DIR)
    runtime_root = Path(runtime_root) if runtime_root is not None else Path(RUNTIME_ROOT)
    tickers = sorted({
        str(ticker).strip().upper()
        for tickers_in_shard in load_combined_shard_map(shard_count=shard_count).values()
        for ticker in tickers_in_shard
    })

    started = time.perf_counter()
    workers = max(1, min(int(max_workers), len(tickers)))
    slot_frames = []
    errored = []
    per_ticker = []

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(
                _read_one_ticker_slot_row,
                ticker,
                source_root,
                slot_ist,
                int(tail_rows),
            ): ticker
            for ticker in tickers
        }
        for future in as_completed(futures):
            ticker_name, df_slot, meta = future.result()
            per_ticker.append(meta)
            if str(meta.get("error", "")):
                errored.append(ticker_name)
            if df_slot is not None and not df_slot.empty:
                slot_frames.append(df_slot)

    slot_df = pd.concat(slot_frames, ignore_index=True) if slot_frames else pd.DataFrame()
    if not slot_df.empty and "ticker" in slot_df.columns and "date" in slot_df.columns:
        slot_df = (
            slot_df.sort_values(["ticker", "date"])
            .drop_duplicates(subset=["ticker", "date"], keep="last")
            .reset_index(drop=True)
        )

    out_dir = slot_bundle_dir(slot_ist, runtime_root)
    bundle_path = slot_bundle_parquet_path(slot_ist, runtime_root)
    if not slot_df.empty:
        slot_df.to_parquet(bundle_path, index=False)

    meta = {
        "slot_ist": slot_ist.strftime("%Y-%m-%d %H:%M:%S%z"),
        "slot_key": slot_key(slot_ist),
        "runtime_root": str(runtime_root),
        "source_root": str(source_root),
        "tail_rows": int(tail_rows),
        "ticker_count": int(len(tickers)),
        "bundle_rows": int(len(slot_df)),
        "errored_tickers": sorted(errored),
        "elapsed_sec": round(time.perf_counter() - started, 3),
        "bundle_path": str(bundle_path),
    }
    (out_dir / "meta.json").write_text(json.dumps(meta, indent=2, sort_keys=True), encoding="utf-8")
    return meta


def _parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Build backend-only per-slot shadow delta bundles for v15_new parity tests.")
    ap.add_argument("--slot", required=True)
    ap.add_argument("--runtime-root", default=str(RUNTIME_ROOT))
    ap.add_argument("--source-dir", default=str(DATA_15M_DIR))
    ap.add_argument("--shard-count", type=int, default=10)
    ap.add_argument("--max-workers", type=int, default=DEFAULT_SNAPSHOT_MAX_WORKERS)
    ap.add_argument("--tail-rows", type=int, default=DEFAULT_BUNDLE_READ_TAIL_ROWS)
    return ap.parse_args()


def main() -> None:
    args = _parse_args()
    meta = build_slot_delta_bundle(
        args.slot,
        runtime_root=args.runtime_root,
        source_dir=args.source_dir,
        shard_count=int(args.shard_count),
        max_workers=int(args.max_workers),
        tail_rows=int(args.tail_rows),
    )
    print(json.dumps(meta, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
