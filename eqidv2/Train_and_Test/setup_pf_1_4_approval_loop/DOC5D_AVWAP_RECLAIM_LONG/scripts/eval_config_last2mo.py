r"""eval_config_last2mo.py — full last-2-months backtest of any near-miss config.
============================================================================
Research-only. Evaluates a given mask/premom/exit/guard config over the WHOLE
window available in a pool dir (~last 2 months, NOT split into TRAIN/TEST),
@5 bps/leg, with a full day-by-day + symbol breakdown and the TRAIN/TEST/pre
sub-window context. NOTE: the window INCLUDES the fit period → a full-period
backtest, not OOS.

  py -3.12 .../scripts/eval_config_last2mo.py --pool .../pool_vB --cfg '{...}'
"""
from __future__ import annotations
import argparse, json, sys
from pathlib import Path
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


def _pf(x):
    g = x.loc[x.net_pnl_rs > 0, "net_pnl_rs"].sum(); l = -x.loc[x.net_pnl_rs < 0, "net_pnl_rs"].sum()
    return (g / l) if l > 0 else float("inf")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--pool", required=True)
    ap.add_argument("--cfg", required=True, help="JSON: {sl,tgt,mask_terms,premom_terms,guard,max_positions,daily_loss_rs}")
    ap.add_argument("--bps", type=float, default=5.0)
    ap.add_argument("--label", default="config")
    args = ap.parse_args()

    raw = json.loads(args.cfg)
    cfg = {"sl": float(raw["sl"]), "tgt": float(raw["tgt"]),
           "mask_terms": [tuple(t) for t in raw.get("mask_terms", [])],
           "premom_terms": [tuple(t) for t in raw.get("premom_terms", [])],
           "guard": raw.get("guard") or None, "status": "OK",
           "max_positions": int(raw.get("max_positions", 20)),
           "daily_loss_rs": float(raw.get("daily_loss_rs", 0.0))}

    tt.POOL_DIRS = [Path(args.pool)]; tt.POOL_DIR = Path(args.pool)
    pool = tt.load_pool(); pool = pool[pool["setup"] == SETUP].copy()
    sess = sorted(pd.Series(pool["_day"].dropna().unique()))
    print(f"[eval] {args.label} over last-2-months  sessions={len(sess)} "
          f"({pd.Timestamp(sess[0]).date()}..{pd.Timestamp(sess[-1]).date()})  @ {args.bps}bps")
    print(f"[eval] cfg: {raw}\n")

    pfb._set_slippage(args.bps); tt.MAX_POSITIONS = cfg["max_positions"]; tt.DAILY_LOSS_RS = cfg["daily_loss_rs"]
    sub = tt.attach_entries(pool.copy())
    m = pfb.full_metrics(SETUP, cfg, sub)
    d = m["detail"]
    print("==== OVERALL (full window) ====")
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
        dayg = d.groupby("trade_date").agg(n=("net_pnl_rs", "size"), net=("net_pnl_rs", "sum"),
                                           wins=("net_pnl_rs", lambda s: int((s > 0).sum())))
        print("\n==== DAY-BY-DAY ====")
        print(f"  {'date':12s} {'n':>3s} {'wins':>4s} {'netRs':>9s}")
        for dt, r in dayg.iterrows():
            print(f"  {str(dt):12s} {int(r['n']):3d} {int(r['wins']):4d} {r['net']:9,.0f}")
        pos = int((dayg['net'] > 0).sum()); tot = len(dayg)
        print(f"  positive days: {pos}/{tot} ({100*pos/tot:.0f}%)")
        symg = d.groupby("ticker")["net_pnl_rs"].sum().sort_values()
        print("\n==== SYMBOLS ====")
        print("  worst:", [f"{k}:{v:,.0f}" for k, v in symg.head(5).items()])
        print("  best :", [f"{k}:{v:,.0f}" for k, v in symg.tail(5).items()])
        pre = d[d["trade_date"].astype(str) < "2026-05-18"]
        tr = d[d["trade_date"].astype(str).between("2026-05-18", "2026-06-19")]
        te = d[d["trade_date"].astype(str) >= "2026-06-20"]
        print("\n==== sub-window context (same cfg) ====")
        for lbl, x in (("pre-TRAIN 05-01..05-17", pre), ("TRAIN 05-18..06-19", tr), ("TEST 06-20..06-30", te)):
            if len(x):
                print(f"  {lbl:24s} n={len(x):3d} PF={_pf(x):.2f} net=Rs{x.net_pnl_rs.sum():,.0f} "
                      f"win%={100*(x.net_pnl_rs>0).mean():.0f}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
