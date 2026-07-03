r"""eval_best_config_window.py — replay a setup's saved best_config.json over an
arbitrary date window (default: trailing 1 month to today) using the EXACT
setup_train_test pipeline (guards -> premom -> family dedupe -> mask -> portfolio
overlay -> resolve, net of cost). No tuning, no fitting — pure out-of-loop replay.

Reuses tt.eval_family / tt.book_detail so the numbers match the optuna_fitval_loop
report basis. Evaluates at BOTH the realistic 15 bps/leg and paper 5 bps/leg.

Writes (under --out, default Train_and_Test/setup_looping_results/):
  <SETUP>_last_1_month_to_<END>_summary.csv   (one row per slippage variant)
  <SETUP>_last_1_month_to_<END>_trades.csv    (per-trade detail @ 15 bps)

Run:
  py -3.12 Train_and_Test\eval_best_config_window.py --setup G_HIGHER_HIGH_BREAK \
      --pool C:\TradingData\eqidv2\setup_pools_2026_06_29\G_HIGHER_HIGH_BREAK \
      [--start 2026-05-30] [--end 2026-06-29] [--results Train_and_Test\results]
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

_HERE = Path(__file__).resolve().parent
for _p in (str(_HERE.parent), str(_HERE)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import setup_train_test as tt  # noqa: E402


def _terms(raw):
    return [tuple(t) for t in (raw or [])]


def _set_slip(bps: float):
    tt.SLIPPAGE_BPS = float(bps)
    tt._entry.cache_clear(); tt._resolve_full.cache_clear(); tt._premom.cache_clear()


def _metrics(setup: str, cfg: dict, df: pd.DataFrame) -> tuple[dict, pd.DataFrame]:
    tt.MAX_POSITIONS = cfg.get("max_positions", 20)
    tt.DAILY_LOSS_RS = cfg.get("daily_loss_rs", 0.0)
    fam = tt.eval_family({setup: cfg}, df)
    exits = {setup: (cfg["sl"], cfg["tgt"])}
    det = tt.book_detail(fam["book"], exits) if fam["trades"] else pd.DataFrame()
    m = {"trades": int(fam["trades"]), "net_pf": round(float(fam["net_pf"]), 4),
         "net_pnl_rs": round(float(fam["net_pnl"]), 2),
         "day_block_p": (None if not np.isfinite(fam["day_block_p"]) else round(float(fam["day_block_p"]), 4))}
    if det.empty:
        m.update({"win_rate_pct": 0.0, "gross_profit_rs": 0.0, "gross_loss_rs": 0.0,
                  "avg_win_rs": 0.0, "avg_loss_rs": 0.0, "max_drawdown_rs": 0.0,
                  "n_days": 0, "n_syms": 0, "trades_per_day": 0.0,
                  "trade_dom_gross": None, "day_dom": None, "sym_dom": None,
                  "outcome_split": {}, "daywise": [], "symbolwise": []})
        return m, det
    net = det["net_pnl_rs"].to_numpy()
    wins, losses = net[net > 0], net[net <= 0]
    tot = float(net.sum()); gp = float(wins.sum())
    det_sorted = det.sort_values("entry_time")
    cum = det_sorted["net_pnl_rs"].cumsum().to_numpy()
    dd = float((cum - np.maximum.accumulate(cum)).min()) if len(cum) else 0.0
    day_net = det.groupby("trade_date")["net_pnl_rs"].sum()
    sym_net = det.groupby("ticker")["net_pnl_rs"].sum()
    m.update({
        "win_rate_pct": round(float((net > 0).mean()) * 100, 2),
        "gross_profit_rs": round(gp, 2), "gross_loss_rs": round(float(losses.sum()), 2),
        "avg_win_rs": round(float(wins.mean()), 2) if len(wins) else 0.0,
        "avg_loss_rs": round(float(losses.mean()), 2) if len(losses) else 0.0,
        "max_drawdown_rs": round(dd, 2),
        "n_days": int(det["trade_date"].nunique()), "n_syms": int(det["ticker"].nunique()),
        "trades_per_day": round(m["trades"] / max(1, det["trade_date"].nunique()), 2),
        "trade_dom_gross": round(float(net.max()) / gp, 3) if gp > 0 else None,
        "day_dom": round(float(day_net.max()) / tot, 3) if tot > 0 else None,
        "sym_dom": round(float(sym_net.max()) / tot, 3) if tot > 0 else None,
        "outcome_split": {k: int(v) for k, v in det["outcome"].astype(str).value_counts().items()},
        "daywise": [{"trade_date": str(i), "size": int(g.size), "sum": round(float(g.sum()), 2)}
                    for i, g in det.groupby("trade_date")["net_pnl_rs"]],
        "symbolwise": [{"ticker": i, "size": int(g.size), "sum": round(float(g.sum()), 2)}
                       for i, g in sorted(det.groupby("ticker")["net_pnl_rs"], key=lambda kv: kv[1].sum())[:10]],
    })
    return m, det


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--setup", required=True)
    ap.add_argument("--pool", required=True)
    ap.add_argument("--start", default="", help="window start YYYY-MM-DD (default: end - 30d)")
    ap.add_argument("--end", default="2026-06-29", help="window end YYYY-MM-DD (default 2026-06-29)")
    ap.add_argument("--results", default="Train_and_Test/results", help="dir holding <setup>/best_config.json")
    ap.add_argument("--out", default="Train_and_Test/setup_looping_results")
    args = ap.parse_args()
    try:
        sys.stdout.reconfigure(line_buffering=True)
    except Exception:
        pass

    setup = args.setup.strip().upper()
    end = pd.Timestamp(args.end)
    start = pd.Timestamp(args.start) if args.start else (end - pd.Timedelta(days=30))

    bc_path = Path(args.results) / setup / "best_config.json"
    bc = json.loads(bc_path.read_text(encoding="utf-8"))["best_config"]
    cfg = {"sl": float(bc["exit"]["sl_pct"]), "tgt": float(bc["exit"]["tgt_pct"]),
           "mask_terms": _terms(bc.get("mask_terms")), "premom_terms": _terms(bc.get("pre_momentum_terms")),
           "guard": (bc.get("entry_guards") or None), "status": "OK",
           "max_positions": bc.get("max_positions", 20), "daily_loss_rs": bc.get("daily_loss_rs", 0.0)}
    bm = "; ".join(f"{a}{o}{b}" for a, o, b in cfg["mask_terms"]) or "(none)"
    bp = "; ".join(f"{a}{o}{b}" for a, o, b in cfg["premom_terms"]) or "(none)"
    print(f"[replay] setup={setup}  window {start.date()}..{end.date()}")
    print(f"[replay] best cfg: SL/Tgt={cfg['sl']}/{cfg['tgt']} mask=[{bm}] premom=[{bp}] "
          f"guard={cfg['guard'] or '-'} maxpos={cfg['max_positions']} dloss={cfg['daily_loss_rs']}")

    tt.POOL_DIRS = [Path(args.pool)]; tt.POOL_DIR = Path(args.pool)
    pool = tt.load_pool()
    pool = pool[pool["setup"] == setup].copy()
    win = pool[(pool["_day"] >= start) & (pool["_day"] <= end)].copy()
    if win.empty:
        print(f"[replay] NO pool rows in window for {setup}"); return 0
    pool_min, pool_max = win["tt_sig_ts"].min(), win["tt_sig_ts"].max()
    days = sorted(pd.Series(win["_day"].dt.date.unique()))
    print(f"[replay] window pool rows={len(win)}  sessions={len(days)}  "
          f"signal range {pool_min} .. {pool_max}")

    rows = []
    det15 = pd.DataFrame()
    for bps in (15.0, 5.0):
        _set_slip(bps)
        w = tt.attach_entries(win.copy())
        m, det = _metrics(setup, cfg, w)
        if bps == 15.0:
            det15 = det
        print(f"\n  @ {bps:>4.1f} bps/leg: trades={m['trades']} PF={m['net_pf']} net=Rs{m['net_pnl_rs']:,.0f} "
              f"win={m['win_rate_pct']}% tpd={m['trades_per_day']} dd=Rs{m['max_drawdown_rs']:,.0f}")
        print(f"      outcome={m['outcome_split']}  dayDom={m['day_dom']} symDom={m['sym_dom']} dbp={m['day_block_p']}")
        rows.append({
            "variant": f"best_loop_config_{bps:.0f}bps", "sl": cfg["sl"], "tgt": cfg["tgt"],
            "mask": bm, "premom": bp, "guard": json.dumps(cfg["guard"]) if cfg["guard"] else "-",
            "requested_start": str(start.date()), "requested_end": str(end.date()),
            "pool_min_signal": str(pool_min), "pool_max_signal": str(pool_max),
            "available_trading_days": ",".join(str(d) for d in days),
            "entry_rows": int(len(w)), **{k: v for k, v in m.items()
                                          if k in ("trades", "win_rate_pct", "gross_profit_rs", "gross_loss_rs",
                                                   "net_pnl_rs", "net_pf", "avg_win_rs", "avg_loss_rs",
                                                   "max_drawdown_rs", "day_block_p", "n_days", "n_syms",
                                                   "trades_per_day", "trade_dom_gross", "day_dom", "sym_dom")},
            "outcome_split": json.dumps(m["outcome_split"]),
            "daywise": json.dumps(m["daywise"]), "symbolwise": json.dumps(m["symbolwise"]),
        })

    outdir = Path(args.out); outdir.mkdir(parents=True, exist_ok=True)
    stem = f"{setup}_last_1_month_to_{end.date()}"
    pd.DataFrame(rows).to_csv(outdir / f"{stem}_summary.csv", index=False)
    if not det15.empty:
        det15.sort_values("entry_time").to_csv(outdir / f"{stem}_trades.csv", index=False)
    print(f"\n[replay] wrote {outdir / (stem + '_summary.csv')}")
    print(f"[replay] wrote {outdir / (stem + '_trades.csv')}  ({len(det15)} trades @15bps)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
