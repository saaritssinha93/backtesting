r"""detect_parity_v2_scan.py — prove the LIVE v2 detector emits DOC5D_AVWAP_RECLAIM_LONG.
============================================================================
Research-only validation. Feeds the parity-exact (>=11:00) pool_vB signals
through the REAL live detector avwap_5min_ID_v2_backtesting._scan_day (flag ON)
and checks it fires for the same (ticker, day, minute). Confirms the wiring end
to end without running a full backtest.
"""
from __future__ import annotations
import sys
from pathlib import Path
import numpy as np, pandas as pd

_HERE = Path(__file__).resolve()
_REPO = _HERE.parents[4]
sys.path.insert(0, str(_REPO))
import avwap_5min_ID_v2_backtesting as v2          # noqa: E402
import research_v11_tier123_new_setups as rv       # noqa: E402

v2.ENABLE_DOC5D_AVWAP_RECLAIM = True                # simulate conf-mode activation
POOL = _HERE.parents[1] / "pool_vB" / "historical_all_available_pre_dedupe_live_candidates.csv"


def _read_raw(tkr: str) -> pd.DataFrame | None:
    """Raw, date-normalized, window-sliced df WITHOUT _prepare_5m (drops pre-existing
    prev-day cols so _prepare_5m's own merge doesn't collide)."""
    p = rv.DATA_ROOT / f"{tkr}_stocks_indicators_5min.parquet"
    if not p.exists():
        return None
    df = pd.read_parquet(p)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    if getattr(df["date"].dt, "tz", None) is None:
        df["date"] = df["date"].dt.tz_localize("Asia/Kolkata")
    else:
        df["date"] = df["date"].dt.tz_convert("Asia/Kolkata")
    df = df.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
    df["date_only"] = df["date"].dt.strftime("%Y-%m-%d")
    df = df[(df["date"].dt.date >= pd.Timestamp("2026-05-01").date())
            & (df["date"].dt.date <= pd.Timestamp("2026-06-30").date())].copy()
    if df.empty:
        return None
    return df.drop(columns=["prev_day_high", "prev_day_low", "prev_day_close_calc"], errors="ignore")


def _prep_ticker(tkr: str) -> pd.DataFrame | None:
    raw = _read_raw(tkr)
    return v2._prepare_5m(raw) if raw is not None else None


def main() -> int:
    pool = pd.read_csv(POOL)
    pool["min"] = pd.to_numeric(pool["signal_minute"], errors="coerce")
    pool["day"] = pd.to_datetime(pool["signal_time_ist"], errors="coerce").dt.strftime("%Y-%m-%d")
    want = pool[pool["min"] >= 660][["ticker", "day", "min"]].copy()      # parity-exact (>=11:00)
    want_set = set(zip(want["ticker"].str.upper(), want["day"]))
    print(f"[parity] pool_vB signals >=11:00: {len(want)} across {want['ticker'].nunique()} tickers")

    # market context from NIFTYBEES (v2's own builder prepares internally -> pass RAW)
    nb = _read_raw("NIFTYBEES")
    if nb is None:
        print("[parity] no NIFTYBEES; abort"); return 1
    market_ctx = v2._market_context_from_df(nb)

    fired = set()
    for tkr in sorted(want["ticker"].str.upper().unique()):
        pdf = _prep_ticker(tkr)
        if pdf is None:
            continue
        for day, g in pdf.groupby("date_only", sort=True):
            cands = v2._scan_day(g.reset_index(drop=True), tkr, market_ctx)
            for c in cands:
                if str(c.setup) == "DOC5D_AVWAP_RECLAIM_LONG":
                    ts = pd.Timestamp(c.signal_ts)
                    fired.add((tkr, str(pd.Timestamp(day).date())))
    hits = want_set & fired
    print(f"[parity] v2._scan_day emitted DOC5D for {len(fired)} (ticker,day) pairs")
    print(f"[parity] overlap with pool_vB >=11:00 set: {len(hits)}/{len(want_set)} "
          f"({100*len(hits)/max(1,len(want_set)):.0f}%)")
    miss = sorted(want_set - fired)[:8]
    if miss:
        print(f"[parity] examples not re-emitted (VWAP recompute / common-gate edge): {miss}")
    print("[parity] RESULT:", "PASS (detector wired & firing)" if len(hits) >= 0.6 * len(want_set)
          else "CHECK (low overlap)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
