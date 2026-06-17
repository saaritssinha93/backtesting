"""build_unified_pool.py — ONE candidate pool for v11 train/test, faithful to option (i).

Produces the file `setup_train_test.py` actually reads
(`historical_all_available_pre_dedupe_live_candidates.csv`), combining, per setup,
the candidate basis that matches how it runs in production (option i = match v11):

  - NATIVE + non-conf setups  -> LIVE-GATED candidates (post v8/research)
        source: outputs_ID_v11_traintest_pool/historical_all_available_days/*/pre_dedupe_live_candidates.csv
  - READMIT setups (RAW_PRE_GATE_POOL / TIER123 / NEW_SETUPS, incl. the v8-dropped
    L_DOUBLE/L_PRESSURE and the 4 Tier-C) -> RAW candidates (bypass v8/research)
        source: outputs_ID_v11_cleanpool/chunk_*/historical_all_available_raw_candidates.csv
                + outputs_ID_v11_conf_tier_c_current/{tier123,new_setups}/*_standalone_trades.csv

So each setup is tuned on the same gating it sees live, and accepted values flow via
final_setup_conf.py to BOTH the v11 backtest and v7 live.

Readmit membership comes from eqidv2_final_conf_live_bootstrap.readmit_setups() (the
conf's provenance), so it stays in sync with the conf.

Output: outputs_ID_v11_unified_pool/historical_all_available_pre_dedupe_live_candidates.csv
        + _manifest.json
Usage:  py -3.12 build_unified_pool.py [--train-end 2026-04-30 --test-start 2026-05-01]
"""
from __future__ import annotations

import argparse
import glob
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

# Train_and_Test/ is a subfolder; the conf modules live in the parent (repo root),
# and the sibling train_test_window module lives here.
_HERE = Path(__file__).resolve().parent
for _p in (str(_HERE.parent), str(_HERE)):
    if _p not in sys.path:
        sys.path.insert(0, _p)
import eqidv2_final_conf_live_bootstrap as boot
from train_test_window import compute_windows

ROOT = Path(r"C:\TradingData\eqidv2")
LIVE_GATED_POOL = ROOT / "outputs_ID_v11_traintest_pool"   # pre_dedupe_live_candidates (post v8/research)
RAW_POOL = ROOT / "outputs_ID_v11_cleanpool"               # raw_candidates (pre-gate)
TIER123_CSV = ROOT / "outputs_ID_v11_conf_tier_c_current" / "tier123" / "tier123_standalone_trades.csv"
NEWSETUPS_CSV = ROOT / "outputs_ID_v11_conf_tier_c_current" / "new_setups" / "new_setups_standalone_trades.csv"
OUT_POOL = ROOT / "outputs_ID_v11_unified_pool"
OUT_FILE = OUT_POOL / "historical_all_available_pre_dedupe_live_candidates.csv"
KEY = ["ticker", "side", "setup", "signal_time_ist"]


def _read_live_gated(pool: Path) -> pd.DataFrame:
    files = glob.glob(str(pool / "historical_all_available_pre_dedupe_live_candidates.csv"))
    files += glob.glob(str(pool / "**" / "pre_dedupe_live_candidates.csv"), recursive=True)
    files = sorted(set(files))
    if not files:
        raise SystemExit(f"[unified] no live-gated pool under {pool}")
    df = pd.concat([pd.read_csv(f, low_memory=False) for f in files], ignore_index=True, sort=False)
    print(f"[unified] live-gated base: {len(df):,} rows / {df['setup'].nunique()} setups from {len(files)} files")
    return df


def _read_raw(pools, keep_setups: set[str] | None = None) -> pd.DataFrame:
    """Read raw_candidates from one or more pool dirs (chunked OR flat layout).
    keep_setups=None keeps every setup; otherwise filters to that set."""
    files: list[str] = []
    for p in pools:
        p = Path(p)
        files += [str(f) for f in p.glob("chunk_*/historical_all_available_raw_candidates.csv")]
        files += [str(f) for f in p.glob("historical_all_available_raw_candidates.csv")]  # flat (e.g. recent gen)
    files = sorted(set(files))
    if not files:
        print(f"[unified] WARN no raw_candidates under {pools}"); return pd.DataFrame()
    df = pd.concat([pd.read_csv(f, low_memory=False) for f in files], ignore_index=True, sort=False)
    if keep_setups is not None:
        df = df[df["setup"].astype(str).isin(keep_setups)].copy()
    print(f"[unified] raw: {len(df):,} rows / {df['setup'].nunique()} setups from {len(files)} file(s)")
    return df


def _read_tier_c(csv: Path, label: str) -> pd.DataFrame:
    if not csv.exists():
        print(f"[unified] {label}: MISSING {csv}"); return pd.DataFrame()
    d = pd.read_csv(csv, low_memory=False)
    print(f"[unified] {label}: {len(d):,} rows / {d['setup'].nunique()} setups")
    return d


def main() -> int:
    _w = compute_windows()   # dynamic default: TEST=last 2 weeks, TRAIN=3mo before
    ap = argparse.ArgumentParser()
    ap.add_argument("--train-end", default=_w["train"][1], help=f"default (dynamic) {_w['train'][1]}")
    ap.add_argument("--test-start", default=_w["test"][0], help=f"default (dynamic) {_w['test'][0]}")
    ap.add_argument("--native-basis", choices=["raw", "live_gated"], default="raw",
                    help="raw = ALL setups from the raw pool(s) (use when the live-gated pool is stale; the conf "
                         "gate is the binding filter and live adds v8/research on top = conservative subset). "
                         "live_gated = option-(i) split (natives post v8/research, readmit raw).")
    ap.add_argument("--extra-raw", default=str(ROOT / "outputs_ID_v11_unified_recent_raw"),
                    help="comma-list of EXTRA raw-candidate dirs to fold in (e.g. freshly generated recent days)")
    ap.add_argument("--tier123-csv", default=str(TIER123_CSV), help="tier123 candidate CSV source")
    ap.add_argument("--newsetups-csv", default=str(NEWSETUPS_CSV), help="new-setups candidate CSV source")
    args = ap.parse_args()
    print(f"[unified] window (manifest): train_end={args.train_end} test_start={args.test_start} "
          f"| basis={args.native_basis} | {_w['policy']} | today={_w['today']}", flush=True)

    readmit = set(boot.readmit_setups())                 # 10 non-native conf setups
    raw_pools = [RAW_POOL] + [Path(p.strip()) for p in str(args.extra_raw).split(",")
                              if p.strip() and Path(p.strip()).exists()]
    tier = pd.concat([_read_tier_c(Path(args.tier123_csv), "tier123"), _read_tier_c(Path(args.newsetups_csv), "new_setups")],
                     ignore_index=True, sort=False)
    tier_setups = set(tier["setup"].astype(str)) if not tier.empty else set()

    if args.native_basis == "raw":
        # everything from the raw pool(s) EXCEPT the setups the tier-c CSVs supply
        rawall = _read_raw(raw_pools, keep_setups=None)
        rawall = rawall[~rawall["setup"].astype(str).isin(tier_setups)].copy()
        rawall["_basis"] = "raw"
        frames = [rawall]
    else:  # option (i): live-gated natives + raw readmit
        base = _read_live_gated(LIVE_GATED_POOL)
        base = base[~base["setup"].astype(str).isin(readmit)].copy(); base["_basis"] = "live_gated"
        raw = _read_raw(raw_pools, keep_setups=readmit)
        if not raw.empty: raw["_basis"] = "raw_readmit"
        frames = [base] + ([raw] if not raw.empty else [])

    if not tier.empty:
        tier = tier.copy(); tier["_basis"] = "tier_c"
        frames.append(tier)

    # union schema (tuner uses ticker/side/setup/signal_time_ist + signal_o/h/l/c + quality/ranker + features)
    cols = list(dict.fromkeys([c for f in frames if not f.empty for c in f.columns]))
    pool = pd.concat([f.reindex(columns=cols) for f in frames if not f.empty], ignore_index=True, sort=False)

    _sig = pd.to_datetime(pool["signal_time_ist"], errors="coerce", utc=True)
    pool["_sig"] = _sig.dt.tz_convert("Asia/Kolkata").dt.tz_localize(None)
    pool = pool.dropna(subset=["_sig"])
    before = len(pool)
    pool = pool.drop_duplicates(subset=KEY, keep="first").reset_index(drop=True)

    OUT_POOL.mkdir(parents=True, exist_ok=True)
    pool.drop(columns=["_sig"]).to_csv(OUT_FILE, index=False)

    train_n = int((pool["_sig"] <= pd.Timestamp(args.train_end)).sum())
    test_n = int((pool["_sig"] >= pd.Timestamp(args.test_start)).sum())
    manifest = {
        "built_utc": datetime.now(timezone.utc).isoformat(),
        "out_file": str(OUT_FILE),
        "rows_total": int(len(pool)), "rows_pre_dedupe": int(before),
        "date_min": str(pool["_sig"].min()), "date_max": str(pool["_sig"].max()),
        "split": {"train_end": args.train_end, "test_start": args.test_start,
                  "train_rows": train_n, "test_rows": test_n},
        "basis_policy": "native/non-conf = live-gated (post v8/research); readmit (10) = raw (option i)",
        "readmit_setups": sorted(readmit),
        "rows_by_basis": pool["_basis"].value_counts().to_dict(),
        "rows_by_setup": pool["setup"].astype(str).value_counts().to_dict(),
        "n_setups": int(pool["setup"].nunique()),
    }
    (OUT_POOL / "_manifest.json").write_text(json.dumps(manifest, indent=2, default=str), encoding="utf-8")
    print(f"[unified] wrote {len(pool):,} rows ({pool['setup'].nunique()} setups) -> {OUT_FILE}")
    print(f"[unified] basis: {manifest['rows_by_basis']} | train={train_n:,} test={test_n:,}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
