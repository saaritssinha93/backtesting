r"""test_conf_last2mo.py — evaluate the reinvented candidate conf over the last 2 months
as ONE continuous book (no train/test split), at 5 bps and 15 bps.
Research-only. NO conf edits, NO live trades.

Conf under test:
  DOC5C_GAP_RETEST_HOLD_LONG (LONG)
  exit SL 1.0 / Tgt 1.0
  mask: retest_depth_atr>=0.5 AND vwap_dist_atr<=2.5
"""
from __future__ import annotations
import sys
from pathlib import Path
import numpy as np
import pandas as pd

_HERE = Path(__file__).resolve()
OUTDIR = _HERE.parent.parent
POOL = OUTDIR / "reinvent_pool"
TT_DIR = None
for p in _HERE.parents:
    if (p / "setup_train_test.py").exists():
        TT_DIR = p
        break
REPO = TT_DIR.parent
ENGINE_DIR = OUTDIR.parent / "_engine"
for _p in (str(REPO), str(TT_DIR), str(ENGINE_DIR)):
    if _p not in sys.path:
        sys.path.insert(0, _p)
import setup_train_test as tt          # noqa: E402
import pf_band_fitval_loop as eng      # noqa: E402

SETUP = "DOC5C_GAP_RETEST_HOLD_LONG"
WIN_START = pd.Timestamp("2026-05-01")
WIN_END = pd.Timestamp("2026-06-30")
CFG = {"sl": 1.0, "tgt": 1.0,
       "mask_terms": [("retest_depth_atr", ">=", 0.5), ("vwap_dist_atr", "<=", 2.5)],
       "premom_terms": [], "guard": None, "status": "OK",
       "max_positions": 20, "daily_loss_rs": 0.0}


def run_at(bps, df):
    tt.SLIPPAGE_BPS = float(bps)
    tt._entry.cache_clear(); tt._resolve_full.cache_clear(); tt._premom.cache_clear()
    sub = tt.attach_entries(df)
    tt.MAX_POSITIONS = 20; tt.DAILY_LOSS_RS = 0.0
    m = eng.full_metrics(SETUP, CFG, sub)
    return m


def show(m, label):
    print(f"\n===== {label} =====")
    print(f" trades         : {m['n']}")
    print(f" net PF         : {m['net_pf']}")
    print(f" net PnL        : Rs {m['net_pnl']:,.0f}")
    print(f" win rate       : {m['win_rate']}%  ({m['wins']}W / {m['losses']}L)")
    print(f" avg win/loss   : Rs {m['avg_win']:,.0f} / Rs {m['avg_loss']:,.0f}")
    print(f" gross P / L    : Rs {m['gross_profit']:,.0f} / Rs {m['gross_loss']:,.0f}")
    print(f" max drawdown   : Rs {m['max_dd']:,.0f}")
    print(f" exits SL/TGT/EOD: {m['sl_cnt']} / {m['tgt_cnt']} / {m['eod_cnt']}   target-fill {m['target_rate']}%")
    print(f" days / symbols : {m['n_days']} / {m['n_syms']}   trades/day {m['trades_per_day']}")
    print(f" day-block p    : {m['day_block_p']}")
    print(f" dominance t/d/s: {m['trade_dom_gross']} / {m['day_dom']} / {m['sym_dom']}")
    print(f" top day / sym  : {m['top_day']}  |  {m['top_sym']}")


def main():
    try:
        sys.stdout.reconfigure(line_buffering=True)
    except Exception:
        pass
    tt.POOL_DIRS = [POOL]; tt.POOL_DIR = POOL
    pool = tt.load_pool()
    pool = pool[pool["setup"] == SETUP].copy()
    win = pool[(pool["_day"] >= WIN_START) & (pool["_day"] <= WIN_END)].copy()
    sess = sorted(pd.Series(win["_day"].dropna().unique()))
    print(f"CONF UNDER TEST: {SETUP} LONG | exit SL {CFG['sl']}/Tgt {CFG['tgt']} | "
          f"mask retest_depth_atr>=0.5 & vwap_dist_atr<=2.5")
    print(f"WINDOW (last 2 months): {WIN_START.date()} .. {WIN_END.date()}  "
          f"({len(sess)} sessions, {sess[0].date()}..{sess[-1].date()})")
    print(f"raw pre-gate candidates in window: {len(win)}")

    for bps in (5.0, 15.0):
        m = run_at(bps, win)
        show(m, f"WHOLE WINDOW @ {bps:.0f} bps/leg")
        det = m["detail"]
        if not det.empty:
            det = det.copy()
            det["month"] = pd.to_datetime(det["trade_date"]).dt.strftime("%Y-%m")
            print(f"\n -- monthly (@ {bps:.0f} bps) --")
            for mo, g in det.groupby("month"):
                net = g["net_pnl_rs"].to_numpy()
                pf = tt._pf(net)
                print(f"   {mo}: n={len(g):>3} PF={pf:6.3f} net=Rs{net.sum():>8,.0f} "
                      f"win%={round((net>0).mean()*100,1):>5} "
                      f"tgt={int((g['outcome'].astype(str).str.upper()=='TARGET').sum())}")
            print(f"\n -- weekly (@ {bps:.0f} bps) --")
            det["week"] = pd.to_datetime(det["trade_date"]).dt.strftime("%G-W%V")
            for wk, g in det.groupby("week"):
                net = g["net_pnl_rs"].to_numpy()
                print(f"   {wk}: n={len(g):>3} PF={tt._pf(net):6.3f} net=Rs{net.sum():>8,.0f}")
            if bps == 5.0:
                print(f"\n -- all {len(det)} trades @5bps (date, ticker, outcome, bars, net) --")
                for _, r in det.sort_values('trade_date').iterrows():
                    print(f"   {r['trade_date']} {r['ticker']:<12} {str(r['outcome']):<7} "
                          f"bars={int(r['bars_held']):>3} net=Rs{r['net_pnl_rs']:>8,.0f}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
