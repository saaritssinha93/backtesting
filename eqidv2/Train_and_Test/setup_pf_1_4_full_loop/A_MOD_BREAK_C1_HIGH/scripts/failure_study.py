r"""failure_study.py — Stage-2 failure/segment study for A_MOD_BREAK_C1_HIGH (research-only).

Evaluates the RAW detector book on TRAIN (production exit 0.70/1.00, 15bps) and reports
per-segment PF/win/net across the main candidate features, so Stage-3 sweep ranges are
grounded in evidence instead of guesses. Writes failure_segments.csv + prints a digest.
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

HERE = Path(__file__).resolve()
WORK = HERE.parents[1]
TT_DIR = HERE.parents[3]
REPO = TT_DIR.parent
ENGINE_DIR = TT_DIR / "setup_pf_1_4_approval_loop" / "_engine"
for p in (REPO, TT_DIR, ENGINE_DIR):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))

import setup_train_test as tt  # noqa: E402
import pf_band_fitval_loop as eng  # noqa: E402

SETUP = "A_MOD_BREAK_C1_HIGH"
TRAIN_START = pd.Timestamp("2026-03-01")
TRAIN_END = pd.Timestamp("2026-05-30")

FEATS = ["rs_pct", "atr_pct", "vol_ratio", "body_pct", "close_loc", "vwap_dist_atr",
         "quality_score", "ranker_score", "signal_range_pct", "upper_wick_pct",
         "wick_skew_pct", "signal_minute", "sig5_adx_calc", "pre5_mom_r", "pre3_range_r",
         "pre1_adx", "sig5_vol_ratio20", "pre_entry_momentum_score"]


def pf(x: pd.Series) -> float:
    gp = float(x[x > 0].sum()); gl = float(-x[x < 0].sum())
    return round(gp / gl, 3) if gl > 0 else (np.inf if gp > 0 else np.nan)


def main() -> int:
    pool_dir = sys.argv[1] if len(sys.argv) > 1 else str(WORK / "pools" / "pool_full")
    tt.POOL_DIRS = [str(Path(pool_dir).resolve())]
    tt.SLIPPAGE_BPS = 15.0
    tt.MAX_POSITIONS = 20
    tt.DAILY_LOSS_RS = 0.0
    tt._entry.cache_clear(); tt._resolve_full.cache_clear(); tt._premom.cache_clear()

    pool = tt.load_pool()
    pool = pool[pool["setup"].astype(str).str.upper().eq(SETUP)].copy()
    train_raw = pool[(pool["_day"] >= TRAIN_START) & (pool["_day"] <= TRAIN_END)].reset_index(drop=True)
    train = tt.attach_entries(train_raw)

    cfg = {"sl": 0.70, "tgt": 1.00, "mask_terms": [], "premom_terms": [], "guard": None,
           "status": "OK", "max_positions": 20, "daily_loss_rs": 0.0}
    m = eng.full_metrics(SETUP, cfg, train)
    detail = m.get("detail")
    if detail is None or (hasattr(detail, "empty") and detail.empty):
        # fall back: resolve book manually through eval_family
        fam = tt.eval_family({SETUP: cfg}, train)
        detail = fam.get("book")
    book = pd.DataFrame(detail)
    print(f"[study] detail cols: {list(book.columns)}")
    pnl_col = next(c for c in ("net_pnl_rs", "pnl_rs", "net_pnl", "pnl") if c in book.columns)
    book["_pnl"] = pd.to_numeric(book[pnl_col], errors="coerce").fillna(0.0)

    # merge candidate features onto the trade book
    missing = [f for f in FEATS if f not in book.columns]
    if missing:
        if "candidate_id" in book.columns and "candidate_id" in train.columns:
            feats = train.drop_duplicates("candidate_id")[["candidate_id"] + [f for f in missing if f in train.columns]]
            book = book.merge(feats, on="candidate_id", how="left")
        else:
            tcol = next((c for c in ("signal_time_ist", "signal_time", "entry_time") if c in book.columns), None)
            scol = next((c for c in ("ticker", "symbol") if c in book.columns), None)
            if tcol and scol and "signal_time_ist" in train.columns:
                feats = train.copy()
                feats["_k"] = feats["ticker"].astype(str).str.upper() + "|" + feats["signal_time_ist"].astype(str)
                feats = feats.drop_duplicates("_k")[["_k"] + [f for f in missing if f in train.columns]]
                book["_k"] = book[scol].astype(str).str.upper() + "|" + book[tcol].astype(str)
                book = book.merge(feats, on="_k", how="left")
    book.to_csv(WORK / "train_raw_book_with_features.csv", index=False)
    print(f"[study] TRAIN book trades={len(book)} net=Rs{book['_pnl'].sum():,.0f} PF={pf(book['_pnl'])}")

    rows = []
    for f in FEATS:
        if f not in book.columns:
            continue
        x = pd.to_numeric(book[f], errors="coerce")
        if x.notna().sum() < 100:
            continue
        try:
            q = pd.qcut(x, 5, duplicates="drop")
        except Exception:
            continue
        g = book.groupby(q, observed=True)["_pnl"]
        for interval, s in g:
            rows.append({"feature": f, "bucket": str(interval), "n": len(s),
                         "pf": pf(s), "net": round(float(s.sum())),
                         "win%": round(100.0 * (s > 0).mean(), 1)})
    seg = pd.DataFrame(rows)
    out_csv = WORK / "failure_segments.csv"
    seg.to_csv(out_csv, index=False)
    if seg.empty:
        print("[study] WARN: no feature segments computed (features missing from book+pool merge)")
        return 0

    # digest: best & worst bucket per feature
    print("\n=== per-feature quintile extremes (RAW TRAIN book) ===")
    for f in seg["feature"].unique():
        s = seg[seg["feature"] == f].sort_values("pf")
        lo, hi = s.iloc[0], s.iloc[-1]
        print(f"{f:26s} worst {lo['bucket']:>24s} pf={lo['pf']:<6} | best {hi['bucket']:>24s} pf={hi['pf']:<6} n={hi['n']}")

    # exit outcome mix + time-of-day
    if "outcome" in book.columns:
        print("\n=== outcome mix ===")
        print(book.groupby(book["outcome"].astype(str))["_pnl"].agg(n="size", net="sum").to_string())
    if "signal_minute" in book.columns:
        sm = pd.to_numeric(book["signal_minute"], errors="coerce")
        hh = (sm // 60).astype("Int64")
        print("\n=== by hour ===")
        print(book.groupby(hh)["_pnl"].agg(n="size", net="sum", pf=pf).to_string())

    # worst symbols / days
    sym_col = "ticker" if "ticker" in book.columns else "symbol"
    print("\n=== worst 10 symbols ===")
    print(book.groupby(book[sym_col].astype(str))["_pnl"].agg(n="size", net="sum").sort_values("net").head(10).to_string())
    day_col = "_day" if "_day" in book.columns else None
    if day_col:
        print("\n=== worst 5 days ===")
        print(book.groupby(book[day_col].astype(str).str[:10])["_pnl"].agg(n="size", net="sum").sort_values("net").head(5).to_string())
        print("\n=== best 5 days ===")
        print(book.groupby(book[day_col].astype(str).str[:10])["_pnl"].agg(n="size", net="sum").sort_values("net").tail(5).to_string())
    print(f"\n[study] wrote {out_csv}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
