"""verify_A_PULLBACK_last_month.py — re-run the LIVE conf for
A_PULLBACK_C2_THEN_BREAK_C2_LOW over the last ~1 month (till today) and report
trades / win% / net PF / net PnL / DD / day-block p.

The gate is read STRAIGHT FROM the root final_setup_conf.py (mask_terms +
pre_momentum_terms + exit + entry_guards) — so this verifies the actual config of
record, not a hand-copied gate. Pipeline is setup_train_test's exact
entry/exit/cost path (net of v6 cost), the same one the tuner & loop runner use.

Run from repo root:
    py -3.12 Train_and_Test\setup_looping_results\verify_A_PULLBACK_last_month.py \
        [--start 2026-05-30] [--end 2026-06-29] [--pool_dir <dir>]
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

HERE = Path(__file__).resolve().parent
TRAIN_DIR = HERE.parent
REPO_ROOT = TRAIN_DIR.parent
for p in (str(REPO_ROOT), str(TRAIN_DIR)):
    if p not in sys.path:
        sys.path.insert(0, p)

import setup_train_test as tt          # noqa: E402
import final_setup_conf as fc          # noqa: E402

SETUP = "A_PULLBACK_C2_THEN_BREAK_C2_LOW"
DEFAULT_POOL = r"C:\TradingData\eqidv2\outputs_ID_v11_unified_pool"


def _conf_config() -> dict:
    """Build the eval_family config for SETUP straight from final_setup_conf.py."""
    cfg = fc.FINAL_SETUP_CONF[SETUP]
    ex = cfg.get("exit", {})
    return {
        "sl": float(ex["sl_pct"]), "tgt": float(ex["tgt_pct"]),
        "mask_terms": [tuple(t) for t in cfg.get("mask_terms", [])],
        "premom_terms": [tuple(t) for t in cfg.get("pre_momentum_terms", [])],
        "guard": (cfg.get("entry_guards") or None),
        "status": "OK",
    }


def _max_drawdown(net: np.ndarray) -> float:
    net = np.asarray(net, dtype=float)
    net = net[np.isfinite(net)]
    if not len(net):
        return 0.0
    curve = np.cumsum(net)
    peak = np.maximum.accumulate(np.r_[0.0, curve])[:-1]
    dd = curve - peak
    return float(dd.min()) if len(dd) else 0.0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="2026-05-30", help="requested window start (last ~1 month)")
    ap.add_argument("--end", default="2026-06-29", help="requested window end (today)")
    ap.add_argument("--pool_dir", default=DEFAULT_POOL)
    args = ap.parse_args()
    try:
        sys.stdout.reconfigure(line_buffering=True)
    except Exception:
        pass

    tt.POOL_DIRS = [Path(args.pool_dir)]
    tt.POOL_DIR = Path(args.pool_dir)

    cfg = _conf_config()
    print(f"[verify] setup={SETUP}  side={fc.FINAL_SETUP_CONF[SETUP].get('side')}")
    print(f"[verify] conf gate (from root final_setup_conf.py):")
    print(f"          exit          SL/Tgt = {cfg['sl']:.2f}/{cfg['tgt']:.2f}")
    print(f"          mask_terms    = {[list(t) for t in cfg['mask_terms']]}")
    print(f"          premom_terms  = {[list(t) for t in cfg['premom_terms']]}")
    print(f"          entry_guards  = {cfg['guard'] or {}}")
    print(f"[verify] requested window: {args.start}..{args.end}  | pool={args.pool_dir}\n")

    pool = tt.load_pool()
    pool = pool[pool["setup"].eq(SETUP)].copy()
    day = pool["_day"]
    win = pool[(day >= pd.Timestamp(args.start)) & (day <= pd.Timestamp(args.end))].copy()
    if win.empty:
        print("[verify] no candidates in window — pool may not cover these dates.")
        return 1
    avail = (win["_day"].min().strftime("%Y-%m-%d"), win["_day"].max().strftime("%Y-%m-%d"))
    print(f"[verify] candidate rows in window: {len(win)}  (available {avail[0]}..{avail[1]})")

    win = tt.attach_entries(win)
    print(f"[verify] rows with resolved 1-min entry: {len(win)}\n")

    m = tt.eval_family({SETUP: cfg}, win)
    book = m["book"].copy()
    detail = tt.book_detail(book, {SETUP: (cfg["sl"], cfg["tgt"])}) if int(m["trades"]) else pd.DataFrame()
    net = detail["net_pnl_rs"].to_numpy(dtype=float) if not detail.empty else np.array([])
    pos, neg = net[net > 0], net[net < 0]

    trades = int(m["trades"])
    win_pct = round(float((net > 0).mean()) * 100, 2) if len(net) else 0.0
    gross_p = round(float(pos.sum()), 2) if len(pos) else 0.0
    gross_l = round(float(-neg.sum()), 2) if len(neg) else 0.0
    outcomes = detail["outcome"].astype(str).value_counts().to_dict() if not detail.empty else {}
    daywise = (detail.groupby("trade_date")["net_pnl_rs"].agg(["size", "sum"]).reset_index()
               if not detail.empty else pd.DataFrame())

    print("=" * 64)
    print(f"  RESULT — {SETUP}  last ~1 month ({avail[0]}..{avail[1]})")
    print("=" * 64)
    print(f"  trades        : {trades}")
    print(f"  win %         : {win_pct}")
    print(f"  gross profit  : {gross_p:>12,.2f}")
    print(f"  gross loss    : {gross_l:>12,.2f}")
    print(f"  net PnL (Rs)  : {float(m['net_pnl']):>12,.2f}")
    print(f"  net PF        : {float(m['net_pf']):.4f}")
    print(f"  max drawdown  : {_max_drawdown(net):>12,.2f}")
    print(f"  day-block p   : {float(m['day_block_p']):.4f}")
    print(f"  outcomes      : {outcomes}")
    print("=" * 64)
    if not daywise.empty:
        print("\n  day-wise net PnL:")
        for r in daywise.itertuples():
            print(f"    {r.trade_date}  n={int(r.size):>2}  net={r.sum:>11,.2f}")

    # Write Train_and_Test artifacts (re-verification record).
    stamp = pd.Timestamp(args.end).strftime("%Y-%m-%d")
    summary = {
        "setup": SETUP, "verified_on": pd.Timestamp.today().strftime("%Y-%m-%d"),
        "requested_window": [args.start, args.end],
        "available_candidate_window": list(avail),
        "conf_mask_terms": [list(t) for t in cfg["mask_terms"]],
        "conf_premom_terms": [list(t) for t in cfg["premom_terms"]],
        "exit_sl_pct": cfg["sl"], "exit_tgt_pct": cfg["tgt"],
        "trades": trades, "win_pct": win_pct,
        "gross_profit_rs": gross_p, "gross_loss_rs": gross_l,
        "net_pnl_rs": round(float(m["net_pnl"]), 2), "net_pf": round(float(m["net_pf"]), 4),
        "max_drawdown_rs": round(_max_drawdown(net), 2),
        "day_block_p": round(float(m["day_block_p"]), 4),
        "outcome_split": outcomes,
    }
    out_json = HERE / f"{SETUP}_verify_last_month_to_{stamp}.json"
    out_json.write_text(json.dumps(summary, indent=2, default=str), encoding="utf-8")
    if not detail.empty:
        detail.to_csv(HERE / f"{SETUP}_verify_last_month_to_{stamp}_trades.csv", index=False)
    print(f"\n[verify] wrote {out_json.name}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
