r"""research_scan_catalog.py — one-pass research scan (2026-03-01..2026-07-01) that
builds the raw pool for the two catalog-only detectors:

  B_HUGE_FAILED_BOUNCE (SHORT)  and  B_HUGE_GREEN_PULLBACK_HOLD_THEN_BREAK (LONG)

RESEARCH-ONLY. No live trades, no order placement, no final_setup_conf.py edits.
Writes ONLY under Train_and_Test/setup_pf_1_4_full_loop/B_HUGE_FAILED_BOUNCE/pools/.
(The B_HUGE_GREEN_PULLBACK_HOLD_THEN_BREAK campaign reads the same scan output
read-only — one shared scan, two per-setup pools.)

Why a custom driver (full story in each setup's POOL_RECREATION_REPORT.md):
  1. Production raw scans NEVER emit these two labels. candidate_scan filters
     candidates to ALLOWED_SETUPS = v6.SETUP_EXIT_RULES (neither target has a v6
     exit entry -> dropped), and the per-(ticker,candle) collapse
     (_dedupe_candidate_frame) additionally keeps only ONE label per candle
     (quality_score desc, then alphabetical setup name).
  2. This driver reruns the UNMODIFIED production detector stack
     (candidate_scan.v2._scan_day + the early-slot scan, same worker overrides as
     the v11 historical scan) and applies the production filters with ONE research
     change: the v8-exit allowlist is widened to ALLOWED_SETUPS + the two targets —
     i.e. the candidate universe exactly as it would look IF these two setups were
     promoted with exit rules.
  3. The production same-candle collapse is then applied UNCHANGED, so the pool
     stays live-faithful to collapse behaviour (a target label only survives a
     candle when no higher-quality / alphabetically-earlier allowlisted label
     fires on it). Pre-collapse per-label rows are ALSO saved for diagnostics and
     as a documented fallback basis if the collapsed pool is too thin.

Outputs:
  pools/_research_scan_catalog_20260301_20260701/historical_all_available_raw_candidates.csv
      as-promoted-collapsed rows, ALL setups (targets included) — pool basis
  pools/_research_scan_catalog_20260301_20260701/precollapse_target_candidates.csv
      pre-collapse per-label rows for the two targets — diagnostics / fallback
  pools/_research_scan_catalog_20260301_20260701/_manifest.json

Run from repo root (post-market hours only; 8 workers max per repo convention):
  py -3.12 Train_and_Test\setup_pf_1_4_full_loop\B_HUGE_FAILED_BOUNCE\scripts\research_scan_catalog.py
"""
from __future__ import annotations

import json
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

_HERE = Path(__file__).resolve().parent            # scripts/
WORK = _HERE.parent                                # B_HUGE_FAILED_BOUNCE/
TT_DIR = WORK.parent.parent                        # Train_and_Test/
REPO = TT_DIR.parent
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

TARGETS = ("B_HUGE_FAILED_BOUNCE", "B_HUGE_GREEN_PULLBACK_HOLD_THEN_BREAK")
DATA_ROOT_5M = Path(r"C:\TradingData\eqidv2\stocks_indicators_5min_eq_live2")
DAY_LO, DAY_HI = "2026-03-01", "2026-07-01"        # 07-02 excluded: 1-min EOD sync incomplete
START_MIN, END_MIN = 9 * 60 + 15, 15 * 60          # 09:15..15:00 slot window (production)
OUT_DIR = WORK / "pools" / "_research_scan_catalog_20260301_20260701"
WORKERS = 8

_G: dict = {"ready": False}


def _boot():
    """Lazy per-process init (Windows spawn-safe)."""
    if _G.get("ready"):
        return
    import avwap_5min_ID_v11_backtesting as v11            # noqa: F401 (heavy)
    import avwap_5min_ID_v7_candidate_scan as candidate_scan
    v11._set_candidate_5m_dir(DATA_ROOT_5M)
    candidate_scan.v2._init_worker({
        "ENABLE_NOISY_ADVANCED_SHORTS": True,
        "ENABLE_NATIVE_V2_MINED_FILTER": False,
    })
    try:
        ctx = candidate_scan.v2._load_market_context()
    except Exception:
        ctx = {}
    _G.update({
        "ready": True,
        "v11": v11,
        "cs": candidate_scan,
        "ctx": ctx,
        "allowed_plus": set(candidate_scan.ALLOWED_SETUPS) | set(TARGETS),
        "ab_probation": set(v11.AB_PROBATION_SETUPS),
    })


def scan_ticker(ticker: str) -> tuple[str, list[dict], list[dict], str]:
    """Returns (ticker, prod_records, precollapse_target_records, error)."""
    _boot()
    cs = _G["cs"]
    ctx = _G["ctx"]
    allowed_plus = _G["allowed_plus"]
    ab_prob = _G["ab_probation"]
    try:
        df = cs._load_live_5m(ticker)
        if df is None or df.empty:
            return ticker, [], [], ""
        prep = cs.v2._prepare_5m(df)
        if prep is None or prep.empty:
            return ticker, [], [], ""
    except Exception as e:
        return ticker, [], [], f"load/prepare: {type(e).__name__}: {e}"

    d = pd.to_datetime(prep["date"], errors="coerce")
    d = d.dt.tz_localize("Asia/Kolkata") if getattr(d.dt, "tz", None) is None else d.dt.tz_convert("Asia/Kolkata")
    prep = prep.assign(_date_ist=d).dropna(subset=["_date_ist"])
    prep["date_only"] = prep["_date_ist"].dt.date

    lo, hi = pd.Timestamp(DAY_LO).date(), pd.Timestamp(DAY_HI).date()
    days = sorted({x for x in prep["date_only"].unique() if lo <= x <= hi})

    prod_rows: list[dict] = []
    pre_rows: list[dict] = []
    for day in days:
        try:
            day_df = prep[prep["date_only"] == day].copy().reset_index(drop=True)
            if day_df.empty:
                continue
            day_df["date"] = day_df["_date_ist"]
            day_df = (day_df.dropna(subset=["date"]).sort_values("date")
                      .drop_duplicates(subset=["date"], keep="last").reset_index(drop=True))
            minutes = day_df["date"].dt.hour * 60 + day_df["date"].dt.minute
            slot_df = day_df.loc[(minutes >= START_MIN) & (minutes <= END_MIN)]
            if slot_df.empty:
                continue
            slot_times = sorted({pd.Timestamp(ts).floor("min") for ts in slot_df["date"]})
            slot_set = set(slot_times)
            signal_map = {pd.Timestamp(r["date"]).floor("min"): r.to_dict()
                          for _, r in slot_df.iterrows()}

            scan_df = cs._append_synthetic_successor(day_df, slot_times[-1])
            candidates: list = []
            try:
                candidates.extend(cs.v2._scan_day(scan_df, str(ticker).upper(), ctx) or [])
            except Exception:
                pass
            if cs.EARLY_MODE_ENABLE:
                for slot in slot_times:
                    try:
                        candidates.extend(cs._scan_early_slot_candidates(scan_df, str(ticker).upper(), slot, ctx) or [])
                    except Exception:
                        pass

            by_slot: dict[pd.Timestamp, list[tuple]] = {}
            for c in candidates:
                try:
                    c_ts = pd.Timestamp(c.signal_ts)
                    c_ts = (c_ts.tz_localize("Asia/Kolkata") if c_ts.tzinfo is None
                            else c_ts.tz_convert("Asia/Kolkata")).floor("min")
                except Exception:
                    continue
                if c_ts not in slot_set:
                    continue
                setup = str(c.setup)
                # production exclusion rule, ab-gate-enabled variant (matches the
                # basis of the harvested cleanpool/fresh scans used by the pooled
                # B-setup campaigns)
                if setup in cs.EXCLUDED_SETUPS and setup not in ab_prob:
                    continue
                # research change: production allowlist WIDENED by the two targets
                if setup not in allowed_plus:
                    continue
                srow = signal_map.get(c_ts)
                if srow is None:
                    continue
                by_slot.setdefault(c_ts, []).append((c, srow))

            for slot, rows in sorted(by_slot.items()):
                pre = cs.candidates_to_dataframe(rows, slot, dedupe=False)
                if pre is None or pre.empty:
                    continue
                prod = cs._dedupe_candidate_frame(pre)   # production collapse, UNCHANGED
                if prod is not None and not prod.empty:
                    prod_rows.extend(prod.to_dict("records"))
                tgt = pre[pre["setup"].isin(TARGETS)]
                if not tgt.empty:
                    pre_rows.extend(tgt.to_dict("records"))
        except Exception as e:
            return ticker, prod_rows, pre_rows, f"day {day}: {type(e).__name__}: {e}"
    return ticker, prod_rows, pre_rows, ""


def main() -> int:
    t0 = time.time()
    _boot()
    cs = _G["cs"]
    universe = cs.v2._load_universe()
    tickers = sorted({str(t).strip().upper() for t in universe if str(t).strip()})
    print(f"[scan] {len(tickers)} tickers | window {DAY_LO}..{DAY_HI} | workers={WORKERS}")
    print(f"[scan] allowlist = production ALLOWED_SETUPS ({len(cs.ALLOWED_SETUPS)}) + targets {list(TARGETS)}")

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    prod_all: list[dict] = []
    pre_all: list[dict] = []
    errs: list[str] = []
    done = 0
    with ProcessPoolExecutor(max_workers=WORKERS) as ex:
        futs = {ex.submit(scan_ticker, tk): tk for tk in tickers}
        for fut in as_completed(futs):
            tk = futs[fut]
            try:
                _, prod, pre, err = fut.result()
            except Exception as e:
                prod, pre, err = [], [], f"worker crash: {type(e).__name__}: {e}"
            prod_all.extend(prod)
            pre_all.extend(pre)
            if err:
                errs.append(f"{tk}: {err}")
            done += 1
            if done % 100 == 0 or done == len(tickers):
                print(f"[scan] {done}/{len(tickers)} tickers | prod_rows={len(prod_all):,} "
                      f"pre_target_rows={len(pre_all):,} | {time.time()-t0:.0f}s", flush=True)

    prod_df = pd.DataFrame(prod_all)
    pre_df = pd.DataFrame(pre_all)
    if not prod_df.empty:
        prod_df = prod_df.sort_values(["signal_time_ist", "ticker", "setup"]).reset_index(drop=True)
    if not pre_df.empty:
        pre_df = pre_df.sort_values(["signal_time_ist", "ticker", "setup"]).reset_index(drop=True)
    prod_csv = OUT_DIR / "historical_all_available_raw_candidates.csv"
    pre_csv = OUT_DIR / "precollapse_target_candidates.csv"
    prod_df.to_csv(prod_csv, index=False)
    pre_df.to_csv(pre_csv, index=False)

    tgt_counts = (prod_df[prod_df["setup"].isin(TARGETS)]["setup"].value_counts().to_dict()
                  if not prod_df.empty else {})
    pre_counts = (pre_df["setup"].value_counts().to_dict() if not pre_df.empty else {})
    manifest = {
        "built_utc": datetime.now(timezone.utc).isoformat(),
        "targets": list(TARGETS),
        "window": [DAY_LO, DAY_HI],
        "data_root_5m": str(DATA_ROOT_5M),
        "detector": "candidate_scan.v2._scan_day + early-slot scan, UNMODIFIED (worker overrides: "
                    "ENABLE_NOISY_ADVANCED_SHORTS=True, ENABLE_NATIVE_V2_MINED_FILTER=False)",
        "research_change": "v8-exit allowlist widened to production ALLOWED_SETUPS + targets "
                           "(as-promoted universe); production same-candle collapse applied UNCHANGED",
        "excluded_rule": "production EXCLUDED_SETUPS dropped unless in AB_PROBATION_SETUPS (ab-gate basis)",
        "n_tickers": len(tickers),
        "prod_rows_total": int(len(prod_df)),
        "pre_target_rows_total": int(len(pre_df)),
        "target_rows_collapsed": tgt_counts,
        "target_rows_precollapse": pre_counts,
        "errors": errs[:50],
        "n_errors": len(errs),
        "elapsed_sec": round(time.time() - t0, 1),
    }
    (OUT_DIR / "_manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    print(f"[scan] DONE in {time.time()-t0:.0f}s -> {prod_csv}")
    print(f"[scan] collapsed target rows: {tgt_counts}")
    print(f"[scan] pre-collapse target rows: {pre_counts}")
    if errs:
        print(f"[scan] {len(errs)} ticker errors (first 5): {errs[:5]}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
