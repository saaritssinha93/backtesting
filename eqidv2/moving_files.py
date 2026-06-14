# -*- coding: utf-8 -*-
"""
Created on Tue Apr 14 11:21:01 2026

@author: Saarit
"""

from pathlib import Path
import hashlib
import shutil
import pandas as pd


# =========================
# CONFIG
# =========================
SOURCE_DIR = Path(r"C:\TradingData\eqidv2\stocks_indicators_5min_eq_live")
DEST_DIR = Path(r"C:\TradingData\eqidv2\stocks_indicators_5min_eq_live2")

# If you know the exact unique key columns, put them here.
# Example:
# KEY_COLUMNS = ["date"]
# or
# KEY_COLUMNS = ["date", "ticker"]
#
# If left as None, the script will compare full rows based on common columns.
KEY_COLUMNS = None

# Use pyarrow engine for parquet
PARQUET_ENGINE = "pyarrow"


def row_hash_from_values(values) -> str:
    """
    Create a stable hash for a row/key tuple.
    """
    joined = "||".join("" if pd.isna(v) else str(v) for v in values)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()


def build_row_hashes(df: pd.DataFrame, columns: list[str]) -> pd.Series:
    """
    Build a hash per row using selected columns.
    """
    return df[columns].apply(lambda row: row_hash_from_values(row.values), axis=1)


# Canonical intraday timestamp column for the 5m/1m parquet stores.
TIMESTAMP_COL = "date"


def _sync_sidecar(src_file: Path, dst_file: Path) -> None:
    """Carry the .source.json provenance sidecar with the parquet, if present.

    moving_files used to copy only *.parquet, so _eq_live2 could receive flipped
    data without the metadata that says whether it is the NIFTYBEES proxy or the
    true NIFTY 50 index. Sync the sidecar so the rebuild guard / provenance work
    downstream too.
    """
    src_meta = src_file.with_name(src_file.name + ".source.json")
    if not src_meta.exists():
        return
    dst_meta = dst_file.with_name(dst_file.name + ".source.json")
    try:
        shutil.copy2(src_meta, dst_meta)
    except Exception as exc:  # pragma: no cover - best-effort provenance sync
        print(f"  WARN: sidecar copy failed for {src_meta.name}: {exc}")


def _resolve_dedupe_key(df_src: pd.DataFrame, df_dst: pd.DataFrame, src_name: str) -> list[str]:
    if KEY_COLUMNS is not None:
        key = list(KEY_COLUMNS)
    elif TIMESTAMP_COL in df_src.columns and TIMESTAMP_COL in df_dst.columns:
        key = [TIMESTAMP_COL]
    else:
        # Fallback: common columns. Last resort only — should not happen for the
        # 5m/1m stores which always carry a "date" column.
        key = [c for c in df_src.columns if c in df_dst.columns]
    missing_in_src = [c for c in key if c not in df_src.columns]
    missing_in_dst = [c for c in key if c not in df_dst.columns]
    if not key or missing_in_src or missing_in_dst:
        raise ValueError(
            f"Cannot resolve a valid de-dup key for {src_name}. "
            f"key={key} missing_in_src={missing_in_src} missing_in_dst={missing_in_dst}"
        )
    return key


def sync_one_file(src_file: Path, dst_file: Path) -> None:
    """
    UPSERT one parquet file from source (live _eq_live) to destination
    (backtest _eq_live2) by timestamp:
    - copy full file if destination does not exist
    - otherwise UNION both frames and keep the freshest (source) row per
      timestamp key. This is an upsert-by-date, NOT a blind full-row-hash
      append, so it can never:
        * produce duplicate timestamps, or
        * stack a true-index row beside a proxy row for the same bar
          (the proxy ~268 -> index ~24,000 scale-mix hazard).
    Running this once also collapses any pre-existing duplicate timestamps in
    the destination. The .source.json provenance sidecar is always synced too.
    """
    print(f"\nProcessing: {src_file.name}")
    _sync_sidecar(src_file, dst_file)

    # Case 1: destination file doesn't exist -> copy full source file
    if not dst_file.exists():
        df_src = pd.read_parquet(src_file, engine=PARQUET_ENGINE)
        df_src.to_parquet(dst_file, index=False, engine=PARQUET_ENGINE)
        print(f"  Destination file missing. Copied full file with {len(df_src)} rows.")
        return

    df_src = pd.read_parquet(src_file, engine=PARQUET_ENGINE)
    df_dst = pd.read_parquet(dst_file, engine=PARQUET_ENGINE)

    if df_src.empty:
        print("  Source file is empty. Nothing to do.")
        return
    if df_dst.empty:
        df_src.to_parquet(dst_file, index=False, engine=PARQUET_ENGINE)
        print(f"  Destination file was empty. Wrote {len(df_src)} rows.")
        return

    key_cols = _resolve_dedupe_key(df_src, df_dst, src_file.name)

    # Union dst + src, then keep the LAST (source/live = freshest) row per key.
    final_columns = list(dict.fromkeys(list(df_dst.columns) + list(df_src.columns)))
    df_dst_aligned = df_dst.reindex(columns=final_columns)
    df_src_aligned = df_src.reindex(columns=final_columns)

    combined = pd.concat([df_dst_aligned, df_src_aligned], ignore_index=True)
    before = len(combined)
    df_final = combined.drop_duplicates(subset=key_cols, keep="last")
    if TIMESTAMP_COL in df_final.columns:
        df_final = df_final.sort_values(TIMESTAMP_COL)
    df_final = df_final.reset_index(drop=True)

    df_final.to_parquet(dst_file, index=False, engine=PARQUET_ENGINE)

    print(
        f"  Upserted on {key_cols}. dst={len(df_dst)} src={len(df_src)} "
        f"-> {len(df_final)} rows (removed {before - len(df_final)} duplicate/overwritten)."
    )


def main() -> None:
    if not SOURCE_DIR.exists():
        raise FileNotFoundError(f"Source folder not found: {SOURCE_DIR}")

    DEST_DIR.mkdir(parents=True, exist_ok=True)

    src_files = sorted(SOURCE_DIR.glob("*.parquet"))

    if not src_files:
        print("No parquet files found in source folder.")
        return

    print(f"Found {len(src_files)} parquet files in source folder.")
    print(f"Source:      {SOURCE_DIR}")
    print(f"Destination: {DEST_DIR}")

    for src_file in src_files:
        dst_file = DEST_DIR / src_file.name
        try:
            sync_one_file(src_file, dst_file)
        except Exception as e:
            print(f"  ERROR in {src_file.name}: {e}")

    print("\nDone.")


if __name__ == "__main__":
    main()