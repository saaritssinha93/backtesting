"""Regenerate final_setup_conf scanner-source CSVs with the current code/data.

This is the repair path when validate_conf_tier_c_parity.py reports stale source
rows: rebuild the Tier123 and new-setups source files, point v11 at the fresh
paths via env vars, rerun v11 final-conf, then rerun parity.
"""
from __future__ import annotations

import argparse
import time
from pathlib import Path

import pandas as pd

import new_setups_scan_v11 as ns11
import research_v11_tier123_new_setups as r123

TIER123_CONF_SETUPS = {
    "E_ORB_RETEST_HOLD_LONG",
    "V_RECLAIM_PULLBACK_LONG",
    "P_PDH_BREAK_RETEST_LONG",
}
NEWSETUPS_CONF_SETUPS = {"L_RS_LEADER_VWAP_HOLD"}


def _universe(limit: int = 0) -> list[str]:
    out = [
        str(x).upper()
        for x in r123._load_probe_universe()
        if not str(x).upper().startswith("NIFTY")
        and not str(x).upper().endswith("BEES")
        and (r123.DATA_ROOT / f"{str(x).upper()}_stocks_indicators_5min.parquet").exists()
    ]
    return out[:limit] if limit and limit > 0 else out


def _rebuild_tier123(out_dir: Path, *, limit: int, workers: int) -> Path:
    old_out_dir = r123.OUT_DIR
    old_workers = r123.SCAN_WORKERS
    old_end_index = r123.SCAN_END_INDEX
    try:
        r123.OUT_DIR = out_dir
        r123.SCAN_WORKERS = max(1, int(workers))
        r123.SCAN_END_INDEX = int(limit) if limit and limit > 0 else None
        out_dir.mkdir(parents=True, exist_ok=True)
        print(f"[tier123] scanning current sources -> {out_dir}", flush=True)
        candidates = r123._scan_all()
        candidates.to_csv(out_dir / "tier123_raw_candidates.csv", index=False)
        candidates = candidates[candidates["setup"].astype(str).isin(TIER123_CONF_SETUPS)].copy()
        candidates.to_csv(out_dir / "tier123_conf_raw_candidates.csv", index=False)
        print(f"[tier123] conf raw rows={len(candidates):,}", flush=True)

        frames = []
        rows = []
        r123.v11.v6.SETUP_EXIT_RULES.update(r123.PROBE_EXIT_RULES)
        for setup, group in candidates.groupby("setup", sort=True):
            print(f"[tier123] resolve {setup} raw={len(group):,}", flush=True)
            trades, _, _ = r123._resolve_candidates(group.copy(), f"conf_tier123_current_{setup}")
            trades = r123._prepare_trades(trades)
            if not trades.empty:
                frames.append(trades)
            rows.extend(r123._flatten_stats("conf_current", setup, trades))
        out = pd.concat(frames, ignore_index=True, sort=False) if frames else pd.DataFrame()
        source_csv = out_dir / "tier123_standalone_trades.csv"
        out.to_csv(source_csv, index=False)
        pd.DataFrame(rows).to_csv(out_dir / "tier123_conf_standalone_by_setup_split.csv", index=False)
        print(f"[tier123] wrote {len(out):,} rows -> {source_csv}", flush=True)
        return source_csv
    finally:
        r123.OUT_DIR = old_out_dir
        r123.SCAN_WORKERS = old_workers
        r123.SCAN_END_INDEX = old_end_index


def _rebuild_newsetups(out_dir: Path, *, limit: int) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    market_ctx = r123._market_context()
    universe = _universe(limit)
    rows: list[dict] = []
    t0 = time.time()
    print(f"[newsetups] scanning current sources universe={len(universe):,} -> {out_dir}", flush=True)
    for i, ticker in enumerate(universe, 1):
        try:
            rows.extend(ns11._scan_ticker(ticker, market_ctx))
        except Exception as exc:
            print(f"  [newsetups skip {i}/{len(universe)}] {ticker}: {exc!r}", flush=True)
        if i % 25 == 0 or i == len(universe):
            print(f"  [newsetups {i}/{len(universe)}] raw_rows={len(rows):,} elapsed={time.time() - t0:.0f}s", flush=True)
    out = pd.DataFrame(rows)
    if not out.empty:
        out = out[out["setup"].astype(str).isin(NEWSETUPS_CONF_SETUPS)].copy()
    source_csv = out_dir / "new_setups_standalone_trades.csv"
    out.to_csv(source_csv, index=False)
    print(f"[newsetups] wrote {len(out):,} rows -> {source_csv}", flush=True)
    return source_csv


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--out-root",
        default=r"C:\TradingData\eqidv2\outputs_ID_v11_conf_tier_c_current",
        help="versioned root where fresh source CSVs are written",
    )
    ap.add_argument("--limit", type=int, default=0, help="scan only first N tickers for a quick smoke test")
    ap.add_argument("--workers", type=int, default=4, help="Tier123 process workers")
    ap.add_argument("--skip-tier123", action="store_true")
    ap.add_argument("--skip-newsetups", action="store_true")
    args = ap.parse_args()

    root = Path(args.out_root)
    tier_csv = None
    new_csv = None
    if not args.skip_tier123:
        tier_csv = _rebuild_tier123(root / "tier123", limit=args.limit, workers=args.workers)
    if not args.skip_newsetups:
        new_csv = _rebuild_newsetups(root / "new_setups", limit=args.limit)

    print("\n[env for v11/live validation]")
    if tier_csv is not None:
        print(f"set EQIDV2_FINAL_CONF_TIER123_SOURCE_CSV={tier_csv}")
    if new_csv is not None:
        print(f"set EQIDV2_FINAL_CONF_NEW_SETUPS_SOURCE_CSV={new_csv}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
