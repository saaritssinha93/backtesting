r"""eval_vA_s23_last2mo.py — full last-2-months backtest of the vA-s23 near-miss config.
============================================================================
Research-only. Evaluates the reinvented DOC5D vA detector + the vA-s23 config
(premom sig5_adx_calc<=20.8676, SL1.0/T2.5, min_slot 11:00, top_n3) over the
WHOLE available window in pool_vA (2026-05-01..2026-06-30 = last ~2 months),
i.e. NOT split into TRAIN/TEST. @5 bps/leg. Prints overall metrics plus a full
day-by-day and symbol breakdown. NOTE: this window INCLUDES the TRAIN fit period,
so it is a full-period backtest, not an OOS result.
"""
from __future__ import annotations
import sys
from pathlib import Path
import numpy as np
import pandas as pd

_HERE = Path(__file__).resolve()
_SETUP_DIR = _HERE.parent.parent; _LOOP_DIR = _SETUP_DIR.parent; _TT_DIR = _LOOP_DIR.parent
_REPO = _TT_DIR.parent
for _p in (str(_REPO), str(_TT_DIR), str(_LOOP_DIR / "_engine")):
    if _p not in sys.path:
        sys.path.insert(0, _p)
import setup_train_test as tt          # noqa: E402
import pf_band_fitval_loop as pfb      # noqa: E402

SETUP = "DOC5D_AVWAP_RECLAIM_LONG"
POOL = _SETUP_DIR / "pool_vA"
CFG = {"sl": 1.0, "tgt": 2.5, "mask_terms": [],
       "premom_terms": [("sig5_adx_calc", "<=", 20.8676)],
       "guard": {"min_slot": "11:00", "top_n": 3},
       "status": "OK", "max_positions": 20, "daily_loss_rs": 0.0}
BPS = 5.0


def main() -> int:
    tt.POOL_DIRS = [POOL]; tt.POOL_DIR = POOL
    pool = tt.load_pool(); pool = pool[pool["setup"] == SETUP].copy()
    sess = sorted(pd.Series(pool["_day"].dropna().unique()))
    print(f"[eval] vA-s23 over last-2-months  sessions={len(sess)} "
          f"({pd.Timestamp(sess[0]).date()}..{pd.Timestamp(sess[-1]).date()})  @ {BPS}bps")
    print(f"[eval] cfg: premom sig5_adx_calc<=20.8676  SL1.0/T2.5  min_slot 11:00  top_n3\n")

    pfb._set_slippage(BPS); tt.MAX_POSITIONS = 20; tt.DAILY_LOSS_RS = 0.0
    sub = tt.attach_entries(pool.copy())
    m = pfb.full_metrics(SETUP, CFG, sub)
    d = m["detail"]
    print("==== OVERALL (2026-05-01..2026-06-30) ====")
    print(f"  trades={m['n']}  PF={m['net_pf']}  net=Rs{m['net_pnl']:,.0f}  win%={m['win_rate']}  "
          f"wins/losses={m['wins']}/{m['losses']}")
    print(f"  avgW=Rs{m['avg_win']:,.0f}  avgL=Rs{m['avg_loss']:,.0f}  maxDD=Rs{m['max_dd']:,.0f}")
    print(f"  SL/TGT/EOD={m['sl_cnt']}/{m['tgt_cnt']}/{m['eod_cnt']}  target-fill={m['target_rate']}%  "
          f"trades/day={m['trades_per_day']}  days={m['n_days']}  symbols={m['n_syms']}")
    print(f"  top-trade share={m['trade_dom_gross']}  top-day share={m['day_dom']}  "
          f"top-sym share={m['sym_dom']}  day-block p={m['day_block_p']}")
    print(f"  top day={m['top_day']}   top symbol={m['top_sym']}")

    if not d.empty:
        d = d.copy()
        dayg = d.groupby("trade_date").agg(n=("net_pnl_rs", "size"),
                                           net=("net_pnl_rs", "sum"),
                                           wins=("net_pnl_rs", lambda s: int((s > 0).sum())))
        gp = d.loc[d.net_pnl_rs > 0, "net_pnl_rs"]; gl = d.loc[d.net_pnl_rs < 0, "net_pnl_rs"]
        print("\n==== DAY-BY-DAY ====")
        print(f"  {'date':12s} {'n':>3s} {'wins':>4s} {'netRs':>9s}")
        for dt, r in dayg.iterrows():
            print(f"  {str(dt):12s} {int(r['n']):3d} {int(r['wins']):4d} {r['net']:9,.0f}")
        pos_days = int((dayg["net"] > 0).sum()); tot_days = len(dayg)
        print(f"  positive days: {pos_days}/{tot_days} ({100*pos_days/tot_days:.0f}%)")

        print("\n==== TOP / BOTTOM SYMBOLS (net Rs) ====")
        symg = d.groupby("ticker")["net_pnl_rs"].sum().sort_values()
        print("  worst:", [f"{k}:{v:,.0f}" for k, v in symg.head(5).items()])
        print("  best :", [f"{k}:{v:,.0f}" for k, v in symg.tail(5).items()])

        # split-context reminder
        tr = d[d["trade_date"].astype(str).between("2026-05-18", "2026-06-19")]
        te = d[d["trade_date"].astype(str) >= "2026-06-20"]
        pre = d[d["trade_date"].astype(str) < "2026-05-18"]
        def _pf(x):
            g = x.loc[x.net_pnl_rs > 0, "net_pnl_rs"].sum(); l = -x.loc[x.net_pnl_rs < 0, "net_pnl_rs"].sum()
            return (g / l) if l > 0 else float("inf")
        print("\n==== sub-window context (same cfg) ====")
        for lbl, x in (("pre-TRAIN 05-01..05-17", pre), ("TRAIN 05-18..06-19", tr), ("TEST 06-20..06-30", te)):
            if len(x):
                print(f"  {lbl:24s} n={len(x):3d} PF={_pf(x):.2f} net=Rs{x.net_pnl_rs.sum():,.0f} "
                      f"win%={100*(x.net_pnl_rs>0).mean():.0f}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
