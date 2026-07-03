"""verify_setup_last_month.py — re-run ANY live conf setup over the last ~1 month
(till today) and report trades / win% / net PF / net PnL / DD / day-block p.

The gate is read STRAIGHT FROM the root final_setup_conf.py (mask_terms +
pre_momentum_terms + exit + entry_guards) — verifying the actual config of record.
Pipeline is setup_train_test's exact entry/exit/cost path (net of v6 cost).

IMPORTANT — top_n parity: the LIVE conf-mask path (eqidv2_final_conf_live_bootstrap
+ v11 _final_setup_conf_mask) honours ONLY entry_guards.min_slot, NOT top_n. So for
any setup whose conf carries a top_n guard, this script reports BOTH:
  * LIVE-FAITHFUL : min_slot only (what v11/live actually fire)  <-- headline
  * SCREEN        : full guard incl. top_n (what the tuner reported)

Run from repo root:
    py -3.12 Train_and_Test\setup_looping_results\verify_setup_last_month.py --setup D_EMA20_REJECTION
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

DEFAULT_POOL = r"C:\TradingData\eqidv2\outputs_ID_v11_unified_pool"


def _conf_config(setup: str, guard_override) -> dict:
    cfg = fc.FINAL_SETUP_CONF[setup]
    ex = cfg.get("exit", {})
    return {
        "sl": float(ex["sl_pct"]), "tgt": float(ex["tgt_pct"]),
        "mask_terms": [tuple(t) for t in cfg.get("mask_terms", [])],
        "premom_terms": [tuple(t) for t in cfg.get("pre_momentum_terms", [])],
        "guard": guard_override,
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


def _evaluate(setup: str, cfg: dict, frame: pd.DataFrame) -> dict:
    m = tt.eval_family({setup: cfg}, frame)
    book = m["book"].copy()
    detail = tt.book_detail(book, {setup: (cfg["sl"], cfg["tgt"])}) if int(m["trades"]) else pd.DataFrame()
    net = detail["net_pnl_rs"].to_numpy(dtype=float) if not detail.empty else np.array([])
    pos, neg = net[net > 0], net[net < 0]
    return {
        "trades": int(m["trades"]),
        "win_pct": round(float((net > 0).mean()) * 100, 2) if len(net) else 0.0,
        "gross_profit_rs": round(float(pos.sum()), 2) if len(pos) else 0.0,
        "gross_loss_rs": round(float(-neg.sum()), 2) if len(neg) else 0.0,
        "net_pnl_rs": round(float(m["net_pnl"]), 2),
        "net_pf": round(float(m["net_pf"]), 4),
        "max_drawdown_rs": round(_max_drawdown(net), 2),
        "day_block_p": round(float(m["day_block_p"]), 4),
        "outcome_split": detail["outcome"].astype(str).value_counts().to_dict() if not detail.empty else {},
        "daywise": (detail.groupby("trade_date")["net_pnl_rs"].agg(["size", "sum"]).reset_index()
                    if not detail.empty else pd.DataFrame()),
    }


def _print_block(title: str, r: dict) -> None:
    print("=" * 64)
    print(f"  {title}")
    print("=" * 64)
    print(f"  trades        : {r['trades']}")
    print(f"  win %         : {r['win_pct']}")
    print(f"  gross profit  : {r['gross_profit_rs']:>12,.2f}")
    print(f"  gross loss    : {r['gross_loss_rs']:>12,.2f}")
    print(f"  net PnL (Rs)  : {r['net_pnl_rs']:>12,.2f}")
    print(f"  net PF        : {r['net_pf']:.4f}")
    print(f"  max drawdown  : {r['max_drawdown_rs']:>12,.2f}")
    print(f"  day-block p   : {r['day_block_p']:.4f}")
    print(f"  outcomes      : {r['outcome_split']}")
    if not r["daywise"].empty:
        print("  day-wise net PnL:")
        for d in r["daywise"].itertuples():
            print(f"    {d.trade_date}  n={int(d.size):>2}  net={d.sum:>11,.2f}")
    print()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--setup", required=True)
    ap.add_argument("--start", default="2026-05-30")
    ap.add_argument("--end", default="2026-06-29")
    ap.add_argument("--pool_dir", default=DEFAULT_POOL)
    args = ap.parse_args()
    try:
        sys.stdout.reconfigure(line_buffering=True)
    except Exception:
        pass

    setup = args.setup.strip().upper()
    if setup not in fc.FINAL_SETUP_CONF:
        print(f"[verify] {setup} not in FINAL_SETUP_CONF")
        return 1

    tt.POOL_DIRS = [Path(args.pool_dir)]
    tt.POOL_DIR = Path(args.pool_dir)

    raw_guard = fc.FINAL_SETUP_CONF[setup].get("entry_guards") or {}
    has_top_n = bool(raw_guard.get("top_n"))
    # LIVE-FAITHFUL guard = min_slot/max_slot only (conf-mask path ignores top_n).
    live_guard = {k: v for k, v in raw_guard.items() if k in ("min_slot", "max_slot")} or None
    screen_guard = (raw_guard or None)

    cfg_live = _conf_config(setup, live_guard)
    print(f"[verify] setup={setup}  side={fc.FINAL_SETUP_CONF[setup].get('side')}")
    print(f"[verify] conf gate (from root final_setup_conf.py):")
    print(f"          exit          SL/Tgt = {cfg_live['sl']:.2f}/{cfg_live['tgt']:.2f}")
    print(f"          mask_terms    = {[list(t) for t in cfg_live['mask_terms']]}")
    print(f"          premom_terms  = {[list(t) for t in cfg_live['premom_terms']]}")
    print(f"          entry_guards  = {raw_guard}  (top_n enforced by conf-mask path: NO)")
    print(f"[verify] requested window: {args.start}..{args.end}  | pool={args.pool_dir}\n")

    pool = tt.load_pool()
    pool = pool[pool["setup"].eq(setup)].copy()
    day = pool["_day"]
    win = pool[(day >= pd.Timestamp(args.start)) & (day <= pd.Timestamp(args.end))].copy()
    if win.empty:
        print("[verify] no candidates in window — pool may not cover these dates.")
        return 1
    avail = (win["_day"].min().strftime("%Y-%m-%d"), win["_day"].max().strftime("%Y-%m-%d"))
    print(f"[verify] candidate rows in window: {len(win)}  (available {avail[0]}..{avail[1]})")
    win = tt.attach_entries(win)
    print(f"[verify] rows with resolved 1-min entry: {len(win)}\n")

    res_live = _evaluate(setup, cfg_live, win)
    label = f"LIVE-FAITHFUL (min_slot only){'  <-- what v11/live fire' if has_top_n else ''}"
    _print_block(f"{setup}  last ~1mo ({avail[0]}..{avail[1]})  |  {label}", res_live)

    res_screen = None
    if has_top_n:
        cfg_screen = _conf_config(setup, screen_guard)
        res_screen = _evaluate(setup, cfg_screen, win)
        _print_block(f"{setup}  |  SCREEN (full guard incl. top_n={raw_guard.get('top_n')})  <-- tuner-reported", res_screen)

    stamp = pd.Timestamp(args.end).strftime("%Y-%m-%d")

    def _strip(r):
        return {k: v for k, v in r.items() if k != "daywise"} | {
            "daywise": r["daywise"].to_dict("records") if not r["daywise"].empty else []}

    summary = {
        "setup": setup, "verified_on": pd.Timestamp.today().strftime("%Y-%m-%d"),
        "requested_window": [args.start, args.end], "available_candidate_window": list(avail),
        "conf_mask_terms": [list(t) for t in cfg_live["mask_terms"]],
        "conf_premom_terms": [list(t) for t in cfg_live["premom_terms"]],
        "exit_sl_pct": cfg_live["sl"], "exit_tgt_pct": cfg_live["tgt"],
        "entry_guards_raw": raw_guard, "top_n_enforced_live": False,
        "live_faithful": _strip(res_live),
        "screen": _strip(res_screen) if res_screen else None,
    }
    out_json = HERE / f"{setup}_verify_last_month_to_{stamp}.json"
    out_json.write_text(json.dumps(summary, indent=2, default=str), encoding="utf-8")
    print(f"[verify] wrote {out_json.name}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
