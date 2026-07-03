r"""setup_loop_runner.py — fast single-load iteration harness for ONE setup.

Loads a (small, per-setup) pool ONCE, resolves 1-min entries ONCE (cached), then
evaluates a BATCH of candidate configs in the same process so 20-50 iterations cost
one load. Reuses setup_train_test's EXACT pipeline (guards -> premom -> family dedupe ->
mask -> portfolio overlay -> resolve, net of cost) so the numbers match the tuner and
the v11 conf backtest basis (for readmit setups raw IS the live basis -> faithful).

For every config it reports, for TRAIN and TEST:
  n, net_pf, net_pnl, win%, tgt/sl/eod%, day_block_p, n_days, n_syms,
  top-1-day net share, top-1-trade net share, top-1-symbol net share  (concentration),
  avg_win, avg_loss, and monthly net.

Config JSON = list of {name, sl, tgt, mask_terms, premom_terms, guard}. mask_terms /
premom_terms are lists of [feature, op, value]; guard is {min_slot/max_slot/top_n} or null.

Run:
  py -3.12 Train_and_Test\setup_loop_runner.py --setup B_HUGE_RED_FAILED_BOUNCE \
       --pool <per-setup pool dir> --configs <configs.json> \
       --train_start .. --train_end .. --test_start .. --test_end .. \
       [--detail baseline]   # dump per-trade detail + loss analysis for the named config
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


def _to_terms(raw):
    return [tuple(t) for t in (raw or [])]


def _concentration(detail: pd.DataFrame) -> dict:
    """Day / trade / symbol concentration of NET pnl. Shares are vs total net when
    positive (a high share = the edge rests on one lucky day/trade/symbol)."""
    if detail.empty:
        return {}
    net = detail["net_pnl_rs"].to_numpy()
    tot = float(net.sum())
    day_net = detail.groupby("trade_date")["net_pnl_rs"].sum()
    sym_net = detail.groupby("ticker")["net_pnl_rs"].sum()
    def _share(x):
        return (float(x) / tot) if tot > 0 else float("nan")
    return {
        "n_days": int(detail["trade_date"].nunique()),
        "n_syms": int(detail["ticker"].nunique()),
        "top1_day_net": round(float(day_net.max()), 0),
        "top1_day_share": round(_share(day_net.max()), 3),
        "top1_trade_net": round(float(net.max()), 0),
        "top1_trade_share": round(_share(net.max()), 3),
        "top1_sym_net": round(float(sym_net.max()), 0),
        "top1_sym_share": round(_share(sym_net.max()), 3),
        "worst_day_net": round(float(day_net.min()), 0),
    }


def _avgwl(detail: pd.DataFrame) -> tuple[float, float]:
    if detail.empty:
        return 0.0, 0.0
    net = detail["net_pnl_rs"].to_numpy()
    wins, losses = net[net > 0], net[net <= 0]
    return (round(float(wins.mean()), 0) if len(wins) else 0.0,
            round(float(losses.mean()), 0) if len(losses) else 0.0)


def _monthly(detail: pd.DataFrame) -> str:
    if detail.empty:
        return "(none)"
    m = detail.copy()
    m["mon"] = pd.to_datetime(m["trade_date"]).dt.to_period("M").astype(str)
    by = m.groupby("mon")["net_pnl_rs"].agg(["size", "sum"])
    return ", ".join(f"{i}:n{int(r['size'])}/Rs{r['sum']:,.0f}" for i, r in by.iterrows())


def evaluate(setup: str, cfg: dict, tr: pd.DataFrame, te: pd.DataFrame) -> dict:
    config = {setup: {
        "sl": float(cfg["sl"]), "tgt": float(cfg["tgt"]),
        "mask_terms": _to_terms(cfg.get("mask_terms")),
        "premom_terms": _to_terms(cfg.get("premom_terms")),
        "guard": cfg.get("guard") or None, "status": "OK",
    }}
    exits = {setup: (config[setup]["sl"], config[setup]["tgt"])}
    out = {"name": cfg.get("name", "cfg")}
    for lbl, df in (("train", tr), ("test", te)):
        fam = tt.eval_family(config, df)
        detail = tt.book_detail(fam["book"], exits) if fam["trades"] else pd.DataFrame()
        aw, al = _avgwl(detail)
        rec = {
            "n": int(fam["trades"]), "net_pf": round(float(fam["net_pf"]), 3),
            "net_pnl": round(float(fam["net_pnl"]), 0),
            "day_block_p": (None if not np.isfinite(fam["day_block_p"]) else round(float(fam["day_block_p"]), 4)),
            "avg_win": aw, "avg_loss": al,
        }
        if not detail.empty:
            oc = detail["outcome"].astype(str)
            net = detail["net_pnl_rs"].to_numpy()
            rec["win_pct"] = round(float((net > 0).mean()) * 100, 1)
            rec["tgt_pct"] = round(float((oc == "TARGET").mean()) * 100, 1)
            rec["sl_pct"] = round(float((oc == "SL").mean()) * 100, 1)
            rec["eod_pct"] = round(float((oc == "EOD").mean()) * 100, 1)
            rec.update(_concentration(detail))
            rec["monthly"] = _monthly(detail)
        out[lbl] = rec
        out[f"{lbl}_detail"] = detail
    return out


def _fmt(rec: dict) -> str:
    if not rec or rec.get("n", 0) == 0:
        return f"n={rec.get('n',0):>3}  (no trades)"
    return (f"n={rec['n']:>3} PF={rec['net_pf']:>5.2f} net=Rs{rec['net_pnl']:>9,.0f} "
            f"win={rec.get('win_pct',0):>4.1f}% t/s/e={rec.get('tgt_pct',0):>4.1f}/{rec.get('sl_pct',0):>4.1f}/{rec.get('eod_pct',0):>4.1f} "
            f"dbp={rec.get('day_block_p')} days={rec.get('n_days','?')} syms={rec.get('n_syms','?')} "
            f"top1day={rec.get('top1_day_share')} top1tr={rec.get('top1_trade_share')} top1sym={rec.get('top1_sym_share')} "
            f"aw/al={rec.get('avg_win',0):,.0f}/{rec.get('avg_loss',0):,.0f}")


def _loss_analysis(detail: pd.DataFrame, label: str) -> None:
    if detail.empty:
        print(f"  [{label}] no trades"); return
    d = detail.copy()
    d["hour"] = pd.to_datetime(d["entry_time"]).dt.strftime("%H")
    losers = d[d["net_pnl_rs"] <= 0]
    print(f"  [{label}] {len(d)} trades, {len(losers)} losers (net Rs {losers['net_pnl_rs'].sum():,.0f})")
    print(f"    outcome split: " + ", ".join(f"{k}:{v}" for k, v in d["outcome"].value_counts().items()))
    print(f"    losers by hour: " + ", ".join(f"{h}:{n}" for h, n in losers["hour"].value_counts().sort_index().items()))
    by_sym = d.groupby("ticker")["net_pnl_rs"].agg(["size", "sum"]).sort_values("sum")
    print("    worst symbols: " + ", ".join(f"{i}(n{int(r['size'])}/Rs{r['sum']:,.0f})" for i, r in by_sym.head(6).iterrows()))
    print("    best symbols : " + ", ".join(f"{i}(n{int(r['size'])}/Rs{r['sum']:,.0f})" for i, r in by_sym.tail(4).iterrows()))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--setup", required=True)
    ap.add_argument("--pool", required=True, help="per-setup pool dir (from split_pool_by_setup.py)")
    ap.add_argument("--configs", required=True, help="JSON list of candidate configs")
    ap.add_argument("--train_start", required=True)
    ap.add_argument("--train_end", required=True)
    ap.add_argument("--test_start", required=True)
    ap.add_argument("--test_end", required=True)
    ap.add_argument("--slippage_bps", type=float, default=tt.SLIPPAGE_BPS)
    ap.add_argument("--detail", default="", help="config name to dump per-trade detail + loss analysis for")
    ap.add_argument("--detail_out", default="", help="optional CSV path prefix for the --detail config's trades")
    args = ap.parse_args()
    try:
        sys.stdout.reconfigure(line_buffering=True)
    except Exception:
        pass

    tt.SLIPPAGE_BPS = float(args.slippage_bps)
    tt.POOL_DIRS = [Path(args.pool)]
    tt.POOL_DIR = Path(args.pool)
    tt.TRAIN = (args.train_start, args.train_end)
    tt.TEST = (args.test_start, args.test_end)

    setup = args.setup.strip().upper()
    configs = json.loads(Path(args.configs).read_text(encoding="utf-8"))
    print(f"[loop] setup={setup} pool={args.pool}")
    print(f"[loop] TRAIN {tt.TRAIN[0]}..{tt.TRAIN[1]}  TEST {tt.TEST[0]}..{tt.TEST[1]}  slippage={tt.SLIPPAGE_BPS}bps/leg")

    pool = tt.load_pool()
    pool = pool[pool["setup"] == setup].copy()
    tr, te = tt.split_train_test(pool)
    print(f"[loop] pool rows: train={len(tr)} test={len(te)} (raw, pre-entry)")
    tr = tt.attach_entries(tr)
    te = tt.attach_entries(te)
    print(f"[loop] with 1m entry: train={len(tr)} test={len(te)}\n")

    results = []
    for cfg in configs:
        r = evaluate(setup, cfg, tr, te)
        results.append(r)
        mt = " AND ".join(f"{f}{op}{v}" for f, op, v in _to_terms(cfg.get("mask_terms"))) or "-"
        pm = " AND ".join(f"{f}{op}{v}" for f, op, v in _to_terms(cfg.get("premom_terms"))) or "-"
        print(f"### {r['name']}  SL/Tgt={cfg['sl']}/{cfg['tgt']}  guard={cfg.get('guard') or '-'}")
        print(f"    mask=[{mt}]  premom=[{pm}]")
        print(f"    TRAIN {_fmt(r['train'])}")
        print(f"    TEST  {_fmt(r['test'])}")
        if r['train'].get('n'):
            print(f"    TRAIN monthly: {r['train'].get('monthly','')}")
        if r['test'].get('n'):
            print(f"    TEST  monthly: {r['test'].get('monthly','')}")
        print()

    if args.detail:
        target = next((x for x in results if x["name"] == args.detail), None)
        if target:
            print("=" * 80)
            print(f"LOSS ANALYSIS for config '{args.detail}'")
            _loss_analysis(target["train_detail"], "TRAIN")
            _loss_analysis(target["test_detail"], "TEST")
            if args.detail_out:
                for lbl in ("train", "test"):
                    det = target[f"{lbl}_detail"]
                    if not det.empty:
                        p = Path(args.detail_out + f"_{lbl}.csv")
                        det.to_csv(p, index=False)
                        print(f"  wrote {p}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
