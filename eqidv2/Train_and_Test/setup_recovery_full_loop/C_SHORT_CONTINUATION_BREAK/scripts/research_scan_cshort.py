r"""research_scan_cshort.py — one-pass research scan (2026-03-01..2026-07-02) building the
raw pool for the collapse-shadowed catalog detector C_SHORT_CONTINUATION_BREAK (SHORT).

RESEARCH-ONLY. No live trades, no final_setup_conf.py edits.
Writes ONLY under Train_and_Test/setup_recovery_full_loop/C_SHORT_CONTINUATION_BREAK/pools/.

Why a custom driver (adapted from the B-family research_scan_catalog.py):
  1. Production raw scans NEVER emit this label: it has no v6 exit entry (allowlist drop),
     and the per-(ticker,candle) collapse (_dedupe_candidate_frame: quality_score desc,
     then alphabetical setup) ALWAYS awards its candles to another label — the 2026-03..07
     research_catalog scan produced ZERO collapsed rows for it.
  2. This driver reruns the UNMODIFIED production detector stack and applies production
     filters with TWO research changes: (a) allowlist widened by the target (as-promoted),
     (b) the target bypasses EXCLUDED_SETUPS.
  3. Both the production-collapsed rows AND the PRE-collapse per-label target rows are
     saved. Given finding (1), the campaign pool basis is the PRE-collapse file — this is
     the honest "if this setup were scanned standalone" universe, and the final report MUST
     state that live trading it requires a collapse-priority change.

Run from repo root, POST-MARKET only (8 workers max per repo convention):
  py -3.12 Train_and_Test\setup_recovery_full_loop\C_SHORT_CONTINUATION_BREAK\scripts\research_scan_cshort.py
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
WORK = _HERE.parent                                # C_SHORT_CONTINUATION_BREAK/
TT_DIR = WORK.parent.parent                        # Train_and_Test/
REPO = TT_DIR.parent
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

TARGETS = ("C_SHORT_CONTINUATION_BREAK",)
DATA_ROOT_5M = Path(r"C:\TradingData\eqidv2\stocks_indicators_5min_eq_live2")
DAY_LO, DAY_HI = "2026-03-01", "2026-07-02"
START_MIN, END_MIN = 9 * 60 + 15, 15 * 60          # 09:15..15:00 slot window (production)
OUT_DIR = WORK / "pools" / "_research_scan_20260301_20260702"
WORKERS = 6

_G: dict = {"ready": False}


def _boot():
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
        "target_was_excluded": bool(set(TARGETS) & set(candidate_scan.EXCLUDED_SETUPS)),
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
                # research change (b): the target bypasses the production exclusion
                if setup in cs.EXCLUDED_SETUPS and setup not in ab_prob and setup not in TARGETS:
                    continue
                # research change (a): production allowlist WIDENED by the target
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
    print(f"[scan] target_was_in_EXCLUDED_SETUPS={_G['target_was_excluded']}")

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
        "detector": "candidate_scan.v2._scan_day + early-slot scan, UNMODIFIED",
        "research_changes": "allowlist widened by target; target bypasses EXCLUDED_SETUPS; "
                            "production same-candle collapse applied UNCHANGED for the collapsed file; "
                            "pre-collapse per-label target rows saved separately (campaign basis)",
        "target_was_in_EXCLUDED_SETUPS": _G["target_was_excluded"],
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
