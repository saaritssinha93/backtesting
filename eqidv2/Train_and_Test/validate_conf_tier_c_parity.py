"""Validate conf-mode Tier-C live detectors against CURRENT research sources.

The scanner-source CSVs are historical artifacts. If the 5-minute data or feature
preparation changes, an old CSV row may no longer be emitted by the current
research detector. This validator separates those cases:

1. source-current check: sampled CSV row is still emitted by the current research
   scanner at that ticker/setup/slot.
2. live parity check: for source-current rows, the causal live detector emits the
   same setup at the same slot.
3. causality check: the live detector never future-stamps an emitted candidate.
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from typing import Dict, Iterable, Set, Tuple

import pandas as pd

# This script lives in Train_and_Test/; shared core modules stay in the repo root.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import avwap_5min_ID_v7_candidate_scan as candidate_scan
import eqidv2_conf_tier_c_live_scan as tc
import new_setups_scan_v11 as ns11
import research_v11_tier123_new_setups as r123

DEFAULT_DATA_ROOT = r"C:\TradingData\eqidv2\stocks_indicators_5min_eq_live2"
DEFAULT_TIER123_CSV = os.getenv(
    "EQIDV2_FINAL_CONF_TIER123_SOURCE_CSV",
    r"C:\TradingData\eqidv2\outputs_ID_v11_conf_tier_c_current\tier123\tier123_standalone_trades.csv",
)
DEFAULT_NEWSETUPS_CSV = os.getenv(
    "EQIDV2_FINAL_CONF_NEW_SETUPS_SOURCE_CSV",
    r"C:\TradingData\eqidv2\outputs_ID_v11_conf_tier_c_current\new_setups\new_setups_standalone_trades.csv",
)

TIER123_SETUPS = {
    "E_ORB_RETEST_HOLD_LONG",
    "V_RECLAIM_PULLBACK_LONG",
    "P_PDH_BREAK_RETEST_LONG",
}
NEWSETUP_SETUPS = {"L_RS_LEADER_VWAP_HOLD"}


def _normal_slot(value: object) -> pd.Timestamp:
    ts = pd.Timestamp(value)
    if ts.tz is None:
        ts = ts.tz_localize("Asia/Kolkata")
    else:
        ts = ts.tz_convert("Asia/Kolkata")
    return ts.floor("min")


def _sample(csv: str, setup: str, n: int) -> pd.DataFrame:
    d = pd.read_csv(csv, low_memory=False)
    d = d[d["setup"].astype(str) == setup].copy()
    if d.empty:
        return d
    d["signal_time_ist"] = pd.to_datetime(d["signal_time_ist"], errors="coerce")
    d = d.dropna(subset=["signal_time_ist"])
    return d.sample(min(n, len(d)), random_state=7)


def _row_key(row: dict) -> Tuple[str, pd.Timestamp]:
    return str(row.get("setup", "")), _normal_slot(row.get("signal_time_ist"))


def _row_key_set(rows: Iterable[dict]) -> Set[Tuple[str, pd.Timestamp]]:
    keys: Set[Tuple[str, pd.Timestamp]] = set()
    for row in rows:
        try:
            keys.add(_row_key(row))
        except Exception:
            continue
    return keys


def _tier123_reference_cache(mkt: dict) -> Dict[str, Set[Tuple[str, pd.Timestamp]]]:
    cache: Dict[str, Set[Tuple[str, pd.Timestamp]]] = {}

    def get(ticker: str) -> Set[Tuple[str, pd.Timestamp]]:
        ticker = str(ticker).upper()
        if ticker not in cache:
            cache[ticker] = _row_key_set(r123._scan_ticker(ticker, mkt))
        return cache[ticker]

    return {"get": get}  # simple mutable closure container


def _newsetups_reference_cache(mkt: dict) -> Dict[str, Set[Tuple[str, pd.Timestamp]]]:
    cache: Dict[str, Set[Tuple[str, pd.Timestamp]]] = {}

    def get(ticker: str) -> Set[Tuple[str, pd.Timestamp]]:
        ticker = str(ticker).upper()
        if ticker not in cache:
            cache[ticker] = _row_key_set(ns11._scan_ticker(ticker, mkt))
        return cache[ticker]

    return {"get": get}


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--tier123-csv", default=DEFAULT_TIER123_CSV)
    ap.add_argument("--newsetups-csv", default=DEFAULT_NEWSETUPS_CSV)
    ap.add_argument("--data-root", default=os.getenv("EQIDV2_TIER_C_DATA_ROOT", DEFAULT_DATA_ROOT))
    ap.add_argument("--sample-per-setup", type=int, default=60)
    ap.add_argument("--min-current-recall-pct", type=float, default=95.0)
    args = ap.parse_args(argv)

    candidate_scan.LIVE_5M_DIR = Path(args.data_root)
    r123.DATA_ROOT = Path(args.data_root)

    print(f"data_root={args.data_root}")
    print(f"tier123_csv={args.tier123_csv}")
    print(f"newsetups_csv={args.newsetups_csv}")
    print("building research market context (one-time, may take a minute)...", flush=True)
    mkt = r123._market_context()

    tier_ref = _tier123_reference_cache(mkt)
    new_ref = _newsetups_reference_cache(mkt)
    targets = {
        "E_ORB_RETEST_HOLD_LONG": args.tier123_csv,
        "V_RECLAIM_PULLBACK_LONG": args.tier123_csv,
        "P_PDH_BREAK_RETEST_LONG": args.tier123_csv,
        "L_RS_LEADER_VWAP_HOLD": args.newsetups_csv,
    }

    overall_ok = True
    causal_violations = 0
    checked_total = 0
    for setup, csv in targets.items():
        sample = _sample(csv, setup, args.sample_per_setup)
        if sample.empty:
            print(f"  {setup:26} : NO source rows found")
            overall_ok = False
            continue

        source_current = 0
        stale_source = 0
        live_hit = 0
        live_miss = 0
        source_ref = tier_ref["get"] if setup in TIER123_SETUPS else new_ref["get"]
        for _, r in sample.iterrows():
            tk = str(r["ticker"]).upper()
            slot = _normal_slot(r["signal_time_ist"])
            key = (setup, slot)

            try:
                rows = tc._scan_conf_tier_c_ticker_slot(tk, slot, mkt)
            except Exception:
                rows = []
            got = {str(x.get("setup")) for x in rows}
            if setup in got:
                # Fast path: a live hit at the exact slot is enough to prove the
                # row is reproducible by the current causal detector. Only misses
                # need the heavier full-ticker research rescan to separate stale
                # source rows from live-port bugs.
                source_current += 1
                live_hit += 1
                checked_total += 1
            elif key in source_ref(tk):
                source_current += 1
                live_miss += 1
                checked_total += 1
            else:
                stale_source += 1
                continue
            for x in rows:
                cand_ts = _normal_slot(x.get("signal_time_ist"))
                if cand_ts != slot:
                    causal_violations += 1

        recall = (live_hit / source_current * 100.0) if source_current else 0.0
        flag = "OK" if stale_source == 0 and recall >= args.min_current_recall_pct else "REVIEW"
        print(
            f"  {setup:26} : sampled={len(sample):3d} source_current={source_current:3d} "
            f"stale_source={stale_source:3d} live_hit={live_hit:3d} live_miss={live_miss:3d} "
            f"current_recall={recall:5.1f}% [{flag}]"
        )
        overall_ok &= stale_source == 0
        overall_ok &= source_current > 0 and recall >= args.min_current_recall_pct

    print(f"\ncausality: {causal_violations} future-stamped candidates out of {checked_total} current-source scans (expect 0)")
    overall_ok &= causal_violations == 0
    print("RESULT:", "PASS" if overall_ok else "REVIEW")
    return 0 if overall_ok else 1


if __name__ == "__main__":
    sys.exit(main())
