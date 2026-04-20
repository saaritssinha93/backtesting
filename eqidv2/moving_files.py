# -*- coding: utf-8 -*-
"""
Created on Tue Apr 14 11:21:01 2026

@author: Saarit
"""

from pathlib import Path
import hashlib
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


def sync_one_file(src_file: Path, dst_file: Path) -> None:
    """
    Sync one parquet file from source to destination:
    - copy full file if destination file does not exist
    - otherwise append only missing rows
    """
    print(f"\nProcessing: {src_file.name}")

    # Case 1: destination file doesn't exist -> copy full source file
    if not dst_file.exists():
        df_src = pd.read_parquet(src_file, engine=PARQUET_ENGINE)
        df_src.to_parquet(dst_file, index=False, engine=PARQUET_ENGINE)
        print(f"  Destination file missing. Copied full file with {len(df_src)} rows.")
        return

    # Case 2: destination exists -> append only missing rows
    df_src = pd.read_parquet(src_file, engine=PARQUET_ENGINE)
    df_dst = pd.read_parquet(dst_file, engine=PARQUET_ENGINE)

    if df_src.empty:
        print("  Source file is empty. Nothing to do.")
        return

    if df_dst.empty:
        df_src.to_parquet(dst_file, index=False, engine=PARQUET_ENGINE)
        print(f"  Destination file was empty. Wrote {len(df_src)} rows.")
        return

    # Align columns safely
    src_cols = list(df_src.columns)
    dst_cols = list(df_dst.columns)

    if KEY_COLUMNS is not None:
        missing_in_src = [c for c in KEY_COLUMNS if c not in df_src.columns]
        missing_in_dst = [c for c in KEY_COLUMNS if c not in df_dst.columns]

        if missing_in_src or missing_in_dst:
            raise ValueError(
                f"Key columns missing in file {src_file.name}. "
                f"Missing in source: {missing_in_src}, missing in destination: {missing_in_dst}"
            )

        compare_cols = KEY_COLUMNS
    else:
        # Compare based on common columns only
        compare_cols = [c for c in src_cols if c in dst_cols]
        if not compare_cols:
            raise ValueError(
                f"No common columns between source and destination for file: {src_file.name}"
            )

    # Build hashes for detecting existing rows
    src_hashes = build_row_hashes(df_src, compare_cols)
    dst_hashes = set(build_row_hashes(df_dst, compare_cols).tolist())

    missing_mask = ~src_hashes.isin(dst_hashes)
    df_missing = df_src.loc[missing_mask].copy()

    if df_missing.empty:
        print("  No missing rows found. Nothing appended.")
        return

    # Make sure missing rows fit destination column order if schemas differ slightly
    for col in df_dst.columns:
        if col not in df_missing.columns:
            df_missing[col] = pd.NA

    # Keep source extra columns too, if destination doesn't have them
    final_columns = list(dict.fromkeys(list(df_dst.columns) + list(df_missing.columns)))
    df_dst_aligned = df_dst.reindex(columns=final_columns)
    df_missing_aligned = df_missing.reindex(columns=final_columns)

    df_final = pd.concat([df_dst_aligned, df_missing_aligned], ignore_index=True)

    # Write back only by appending missing rows to existing data logically.
    # Existing destination rows stay unchanged.
    df_final.to_parquet(dst_file, index=False, engine=PARQUET_ENGINE)

    print(f"  Appended {len(df_missing)} missing rows.")
    print(f"  Final row count: {len(df_final)}")


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