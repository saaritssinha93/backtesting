r"""eval_window.py — replay a FIXED config over an arbitrary window (research-only, no tuning).

Evaluates the B_AVWAP_RECLAIM_REVERSAL best-candidate config (from the pf_band_search loop) AND the
card baseline over a date window (default: trailing 3 months), net of cost at BOTH 15 bps/leg (realistic)
and 5 bps/leg (paper). Reuses setup_train_test + pf_band_search.full_metrics so numbers match the loop.
Prints full metrics + monthly breakdown + day/symbol concentration; writes LAST_3MO_RESULT.md.
No live trades; nothing written to final_setup_conf.py.

Run:
  py -3.12 Train_and_Test/setup_pf_1_4_approval_loop/B_AVWAP_RECLAIM_REVERSAL/scripts/eval_window.py \
      --start 2026-03-24 --end 2026-06-24
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

_P = Path(__file__).resolve()
TT_DIR = next(par for par in _P.parents if par.name == "Train_and_Test")
REPO_ROOT = TT_DIR.parent
for _d in (str(REPO_ROOT), str(TT_DIR), str(_P.parent)):
    if _d not in sys.path:
        sys.path.insert(0, _d)

import setup_train_test as tt          # noqa: E402
import pf_band_search as pb            # noqa: E402

SETUP = "B_AVWAP_RECLAIM_REVERSAL"
POOL = r"C:/TradingData/eqidv2/setup_pools_2026_06_29/B_AVWAP_RECLAIM_REVERSAL"

# Best candidate from the pf_band_search loop (FIT/VAL band score 1.28; gate FAIL — overfit/2-day TEST).
BEST = {"sl": 0.9, "tgt": 3.0,
        "mask_terms": [["vwap_dist_atr", "<=", 1.0], ["vol_ratio", ">=", 3.537825], ["atr_pct", "<=", 0.003921]],
        "premom_terms": [["pre1_adx", ">=", 30.675856], ["pre5_mom_r", ">=", 0.317166]],
        "guard": {"max_slot": "14:00"}, "max_positions": 20, "daily_loss_rs": 0.0}
# Card baseline (§2): near-VWAP mask, exit 0.70/1.50.
CARD = {"sl": 0.70, "tgt": 1.50, "mask_terms": [["vwap_dist_atr", "<=", 1.0]],
        "premom_terms": [], "guard": None, "max_positions": 20, "daily_loss_rs": 0.0}


def _monthly(det: pd.DataFrame) -> str:
    if det.empty:
        return "(none)"
    d = det.copy(); d["m"] = pd.to_datetime(d["trade_date"]).dt.to_period("M").astype(str)
    out = []
    for m, g in d.groupby("m"):
        net = g["net_pnl_rs"].to_numpy()
        out.append(f"{m}: n{len(g)} PF{pb._pf(net):.2f} Rs{net.sum():,.0f}")
    return " | ".join(out)


def _fmt(m: dict) -> str:
    if not m or m["trades"] == 0:
        return "n=0 (no trades)"
    return (f"n={m['trades']} PF={m['net_pf']} net=Rs{m['net_pnl']:,.0f} win={m['win_rate_pct']}% "
            f"t/s/e={m['tgt_cnt']}/{m['sl_cnt']}/{m['eod_cnt']} avgW/L={m['avg_win']:,.0f}/{m['avg_loss']:,.0f} "
            f"maxDD=Rs{m['max_drawdown']:,.0f} tpd={m['trades_per_day']} ndays={m['n_days']} "
            f"domTr/Day/Sym={m['trade_dom_gross']}/{m['day_dom']}/{m['sym_dom']}")


def _cfg_from_json(j):
    return {"sl": float(j["exit"]["sl_pct"]), "tgt": float(j["exit"]["tgt_pct"]),
            "mask_terms": [list(t) for t in j.get("mask_terms", [])],
            "premom_terms": [list(t) for t in j.get("pre_momentum_terms", [])],
            "guard": (j.get("entry_guards") or None),
            "max_positions": int(j.get("max_positions", 20)), "daily_loss_rs": float(j.get("daily_loss_rs", 0.0))}


def main() -> int:
    import json
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="2026-03-24")
    ap.add_argument("--end", default="2026-06-30")
    ap.add_argument("--config", default="", help="JSON config to evaluate as the candidate (default: built-in BEST)")
    ap.add_argument("--label", default="BEST_CANDIDATE", help="label for the candidate config")
    ap.add_argument("--out", default="", help="output .md filename stem (default LAST_3MO_RESULT)")
    args = ap.parse_args()
    cand_cfg = _cfg_from_json(json.loads(Path(args.config).read_text(encoding="utf-8"))) if args.config else BEST
    cand_label = args.label
    configs = ((cand_label, cand_cfg), ("CARD_BASELINE", CARD))
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
    except Exception:
        pass

    tt.POOL_DIRS = [Path(POOL)]; tt.POOL_DIR = Path(POOL)
    start, end = pd.Timestamp(args.start), pd.Timestamp(args.end)
    outdir = TT_DIR / "setup_pf_1_4_approval_loop" / SETUP

    md = [f"# LAST-3-MONTHS RESULT — {SETUP} (LONG)", "",
          f"Replay of fixed configs over **{start.date()}..{end.date()}** (no tuning), net of cost. "
          "Best-candidate config = pf_band_search loop winner (which FAILED the approval gate — overfit/2-day TEST); "
          "shown here purely as a longer-window read. Nothing written to final_setup_conf.py.", ""]

    results = {}
    for bps in (15.0, 5.0):
        pb._set_slip(bps)
        pool = tt.load_pool(); pool = pool[pool["setup"] == SETUP].copy()
        win = pool[(pool["_day"] >= start) & (pool["_day"] <= end)].copy()
        days = sorted(pd.Series(win["_day"].dt.date.unique()))
        w = tt.attach_entries(win)
        print(f"\n===== @ {bps:.0f} bps/leg | window {start.date()}..{end.date()} | "
              f"{len(days)} sessions ({days[0]}..{days[-1]}) | entry rows {len(w)} =====")
        for name, cfg in configs:
            m = pb.full_metrics(SETUP, cfg, w)
            print(f"  {name:16} {_fmt(m)}")
            print(f"                   monthly: {_monthly(m['detail'])}")
            results[(bps, name)] = m

    md += [f"- window sessions: {len(days)}  ({days[0]}..{days[-1]})", ""]
    for name, cfg in configs:
        bm = "; ".join(f"{a}{o}{b}" for a, o, b in cfg["mask_terms"]) or "(none)"
        bp = "; ".join(f"{a}{o}{b}" for a, o, b in cfg["premom_terms"]) or "(none)"
        md += [f"## {name}",
               f"- exit SL {cfg['sl']} / Tgt {cfg['tgt']} · mask [{bm}] · premom [{bp}] · guard {cfg['guard'] or '{}'}", "",
               "| cost | n | PF | net Rs | win% | tgt/sl/eod | maxDD | tpd | dayDom | symDom |",
               "|---|---:|---:|---:|---:|---|---:|---:|---:|---:|"]
        for bps in (15.0, 5.0):
            m = results[(bps, name)]
            md.append(f"| {bps:.0f}bps | {m['trades']} | {m['net_pf']} | {m['net_pnl']:,.0f} | {m['win_rate_pct']} | "
                      f"{m['tgt_cnt']}/{m['sl_cnt']}/{m['eod_cnt']} | {m['max_drawdown']:,.0f} | {m['trades_per_day']} | "
                      f"{m['day_dom']} | {m['sym_dom']} |")
        md += ["", f"- monthly @15bps: {_monthly(results[(15.0, name)]['detail'])}", ""]
    stem = args.out or "LAST_3MO_RESULT"
    (outdir / f"{stem}.md").write_text("\n".join(md), encoding="utf-8")
    print(f"\n[eval-window] wrote {outdir/(stem + '.md')}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
