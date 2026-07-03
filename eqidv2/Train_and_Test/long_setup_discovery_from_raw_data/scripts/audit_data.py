r"""audit_data.py — Stage 1 raw-data + pool audit for LONG-setup discovery (research-only).

Locates the raw 5-minute and 1-minute parquet stores (via the repo's own constants in
avwap_5min_ID_v11_backtesting / v6) and the unified candidate pool (derived from the raw data),
then prints + writes DATA_AUDIT.md: paths, symbol counts, date ranges, columns (indicator vs
non-indicator), the LONG-candidate universe in the pool, session coverage, and obvious quality issues.
No live trades.

Run:
  py -3.12 Train_and_Test/long_setup_discovery_from_raw_data/scripts/audit_data.py
"""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

_P = Path(__file__).resolve()
TT_DIR = next(par for par in _P.parents if par.name == "Train_and_Test")
REPO_ROOT = TT_DIR.parent
for _d in (str(REPO_ROOT), str(TT_DIR)):
    if _d not in sys.path:
        sys.path.insert(0, _d)

import setup_train_test as tt           # noqa: E402  (imports v11 + v6)
v11 = tt.v11

POOL = Path(r"C:/TradingData/eqidv2/outputs_ID_v11_unified_pool/historical_all_available_pre_dedupe_live_candidates.csv")
INDICATOR_HINTS = ("rsi", "adx", "macd", "ema", "sma", "vwap", "atr", "supertrend", "boll", "bb_",
                   "keltner", "mfi", "obv", "cci", "stoch", "williams", "roc", "vol_ratio", "vol_sma",
                   "pressure", "ranker", "quality", "slope", "mom", "range_r", "adx_calc", "rsi_dir")


def _dir_of(*cands):
    for c in cands:
        try:
            p = Path(c)
            if p.exists():
                return p
        except Exception:
            pass
    return None


def _scan_parquet_dir(d: Path, suffix: str, label: str, lines: list):
    lines.append(f"### {label}")
    if d is None or not d.exists():
        lines.append(f"- **MISSING**: {d}"); lines.append(""); return None
    files = sorted(d.glob(f"*{suffix}"))
    lines.append(f"- path: `{d}`")
    lines.append(f"- files (symbols): **{len(files)}**")
    if not files:
        lines.append("- ⚠️ no parquet files found"); lines.append(""); return None
    # sample a few files for columns + date coverage
    sample = files[0]
    try:
        df = pd.read_parquet(sample)
        dcol = "date" if "date" in df.columns else df.columns[0]
        dts = pd.to_datetime(df[dcol], errors="coerce").dropna()
        lines.append(f"- sample file: `{sample.name}` rows={len(df)} cols={len(df.columns)}")
        lines.append(f"- sample date range: {dts.min()} .. {dts.max()}")
        cols = list(df.columns)
        ind = [c for c in cols if any(h in c.lower() for h in INDICATOR_HINTS)]
        non = [c for c in cols if c not in ind]
        lines.append(f"- indicator-like columns ({len(ind)}): {', '.join(ind)}")
        lines.append(f"- other columns ({len(non)}): {', '.join(non)}")
    except Exception as e:
        lines.append(f"- ⚠️ could not read sample: {e}")
        df = None
    lines.append("")
    return files


def main() -> int:
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
    except Exception:
        pass
    out = TT_DIR / "long_setup_discovery_from_raw_data"
    L = ["# DATA_AUDIT — raw 5-min / 1-min stores + candidate pool", "",
         "Stage-1 audit for LONG-setup discovery. Paths resolved from the repo's own loader constants "
         "(`avwap_5min_ID_v11_backtesting.py` / v6). Read-only.", ""]

    # --- resolve dirs from repo constants ---
    dir_5m = _dir_of(getattr(v11, "HISTORICAL_5M_DIR", None), r"C:/TradingData/eqidv2/stocks_indicators_5min_eq_live2")
    dir_1m = _dir_of(getattr(v11.v6, "DATA_1M_DIR", None), r"C:/TradingData/eqidv2/stocks_indicators_1min_eq")
    dir_1m_raw = _dir_of(getattr(v11, "V7_LIVE_RAW_1M_DIR", None), r"C:/TradingData/eqidv2/stocks_raw_1min_entry_v5_id_live")
    print(f"[audit] 5m={dir_5m}\n[audit] 1m={dir_1m}\n[audit] 1m_raw={dir_1m_raw}\n[audit] pool={POOL}")

    L.append("## Raw data stores")
    files_5m = _scan_parquet_dir(dir_5m, "_stocks_indicators_5min.parquet", "5-MINUTE indicators (entry/setup discovery)", L)
    files_1m = _scan_parquet_dir(dir_1m, "_stocks_indicators_1min.parquet", "1-MINUTE indicators (exit simulation)", L)
    files_1mraw = _scan_parquet_dir(dir_1m_raw, "_stocks_raw_1min.parquet", "1-MINUTE raw (live entry parity)", L)

    # --- pool audit ---
    L.append("## Candidate pool (derived from raw 5-min via the scanner)")
    if not POOL.exists():
        L.append(f"- **MISSING POOL**: {POOL}")
    else:
        sz = POOL.stat().st_size / 1e6
        L.append(f"- path: `{POOL}`  ({sz:.0f} MB)")
        tt.POOL_DIRS = [POOL.parent]; tt.POOL_DIR = POOL.parent
        pool = tt.load_pool()
        days = sorted(pd.Series(pool["_day"].dropna().unique()))
        L.append(f"- total candidates: **{len(pool):,}** · sessions: **{len(days)}** "
                 f"({pd.Timestamp(days[0]).date()} .. {pd.Timestamp(days[-1]).date()})")
        L.append(f"- distinct symbols: **{pool['ticker'].nunique()}**")
        side = pool["side"].astype(str).str.upper()
        L.append(f"- side split: LONG={int((side=='LONG').sum()):,}  SHORT={int((side=='SHORT').sum()):,}")
        # LONG universe by source detector (these become structural 'families')
        lp = pool[side == "LONG"]
        by_setup = lp["setup"].value_counts()
        L.append("")
        L.append(f"### LONG candidate universe = **{len(lp):,}** rows, by source detector (structural families):")
        L.append("| source setup | LONG candidates |")
        L.append("|---|---:|")
        for s, n in by_setup.items():
            L.append(f"| {s} | {n:,} |")
        # columns in pool
        cols = list(pool.columns)
        ind = [c for c in cols if any(h in c.lower() for h in INDICATOR_HINTS)]
        non = [c for c in cols if c not in ind]
        L += ["", f"### Pool columns ({len(cols)})",
              f"- indicator/feature columns ({len(ind)}): {', '.join(sorted(ind))}",
              f"- non-indicator columns ({len(non)}): {', '.join(sorted(non))}"]
        # premom (computed at entry, not raw columns) availability note
        L += ["", "### Pre-momentum features (computed at entry from 1-min bars, not raw pool columns)",
              f"- {', '.join(tt.PREMOM_FEATURES)} — available via the entry pipeline (`_pre_entry_momentum_features_v11`)."]
        # quality issues
        L += ["", "### Data quality checks"]
        miss_sig = int(pool["signal_time_ist"].isna().sum()) if "signal_time_ist" in pool.columns else "n/a"
        L.append(f"- rows with bad/missing signal_time_ist: {miss_sig}")
        for c in ("vwap_dist_atr", "vol_ratio", "atr_pct", "close_loc"):
            if c in pool.columns:
                x = pd.to_numeric(pool[c], errors="coerce")
                L.append(f"- `{c}`: non-null {int(x.notna().sum()):,}/{len(pool):,}, "
                         f"min {x.min():.4g}, med {x.median():.4g}, max {x.max():.4g}")
        # 5m VWAP caveat (known repo issue)
        L.append("- ⚠️ known repo caveat: the 5-min parquet `VWAP` column is stale/anchored; the backtester "
                 "recomputes session VWAP (see v11 `_selected_strategy_features` / VWAP_parquet). Discovery uses the "
                 "corrected `vwap_dist_atr` already in the pool.")

    (out / "DATA_AUDIT.md").write_text("\n".join(L), encoding="utf-8")
    print(f"[audit] wrote {out/'DATA_AUDIT.md'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
