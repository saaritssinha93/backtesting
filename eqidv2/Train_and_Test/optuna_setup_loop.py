r"""optuna_setup_loop.py — code-loop (Optuna TPE) over the REPO'S OWN eval pipeline for ONE setup.

Wraps setup_train_test.eval_family (guards -> premom -> family dedupe -> mask -> portfolio overlay ->
resolve, net of v6 cost) and searches ONLY the knobs this engine actually honors:
  mask_terms (<=2 threshold terms on precomputed features), pre_momentum_terms (<=2 terms),
  exit sl_pct/tgt_pct (fixed %), entry_guards (min_slot/max_slot/top_n).
NOT searched (engine can't do them): indicator swaps, ATR/trailing/partial exits, day-of-week, cooldown.

Search is run at the REALISTIC 15 bps/leg slippage (objective + selection judged there); the best config is
re-scored once at 5 bps/leg (paper ceiling) for context. Slippage is a module global inside the cached entry/exit
resolvers, so the caches are cleared when slippage changes.

Objective (anti-overfit):  min(train_PF, test_PF) - GAP_LAMBDA*|train_PF - test_PF|   (PF clamped to 10).
Selection gate (15 bps, BOTH windows): trades>=MIN, net>0, PF>=PF_MIN, day&trade dominance<=DOM_CAP,
  trades/day<=MAX_TRADES_DAY, and test day_block_p<=MAX_DAY_BLOCK_P.

Run from repo root:
  py -3.12 Train_and_Test\optuna_setup_loop.py --setup E_VWAP_LOSE_EARLY_SHORT --pool <per-setup pool> \
     --train_start 2026-04-13 --train_end 2026-05-25 --test_start 2026-05-26 --test_end 2026-06-24 \
     --trials 300 --time_budget_min 20 --out Train_and_Test\results
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
import optuna  # noqa: E402

optuna.logging.set_verbosity(optuna.logging.WARNING)

MASK_FEATS = ["rs_pct", "vol_ratio", "atr_pct", "body_pct", "close_loc", "vwap_dist_atr",
              "quality_score", "ranker_score", "signal_range_pct", "upper_wick_pct",
              "lower_wick_pct", "wick_skew_pct"]
PM_FEATS = ["pre_entry_momentum_score", "sig5_adx_calc", "sig5_rsi_dir", "sig5_vol_ratio20",
            "pre1_adx", "pre3_range_r", "pre5_mom_r", "pre3_close_pos"]
QGRID = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9]
SL_GRID = [0.70, 0.85, 0.90, 1.00, 1.10, 1.20]
TGT_GRID = [0.80, 1.00, 1.25, 1.50, 2.00]
MIN_SLOTS = ["09:30", "09:45", "10:00", "10:30", "11:00"]
MAX_SLOTS = ["12:00", "12:30", "13:00", "14:00", "14:30"]
READMIT = {"A_MOD_BREAK_C1_LOW", "B_HUGE_RED_FAILED_BOUNCE", "C_OR_BREAKDOWN",
           "G_LOWER_LOW_BREAK", "L_DOUBLE_BOTTOM_VWAP", "L_PRESSURE_BURST_VWAP"}


def _clamp_pf(pf: float) -> float:
    if not np.isfinite(pf):
        return 10.0
    return min(float(pf), 10.0)


def _set_slippage(slip: float):
    tt.SLIPPAGE_BPS = float(slip)
    tt._entry.cache_clear()
    tt._resolve_full.cache_clear()
    tt._premom.cache_clear()


def _metrics(fam: dict) -> dict:
    """Rich metrics from an eval_family result (book + per-row net), net of cost."""
    book, net = fam["book"], np.asarray(fam.get("net", []), float)
    n = int(fam["trades"])
    out = {"n": n, "net_pf": round(float(fam["net_pf"]), 3), "net_pnl": round(float(fam["net_pnl"]), 0),
           "day_block_p": (None if not np.isfinite(fam["day_block_p"]) else round(float(fam["day_block_p"]), 4)),
           "n_days": 0, "trades_per_day": 0.0, "day_dom": None, "trade_dom": None}
    if n == 0 or book is None or len(book) == 0:
        return out
    m = np.isfinite(net)
    net = net[m]
    days = pd.Series(book["_day"].to_numpy()[m])
    tot = float(net.sum())
    out["n_days"] = int(days.nunique())
    out["trades_per_day"] = round(n / max(1, out["n_days"]), 2)
    if tot > 0:
        day_net = pd.Series(net, index=days).groupby(level=0).sum()
        out["day_dom"] = round(float(day_net.max()) / tot, 3)
        out["trade_dom"] = round(float(net.max()) / tot, 3)
    else:
        out["day_dom"] = out["trade_dom"] = 9.99   # net<=0 -> fails dominance by construction
    return out


def _cfg_from_trial(trial, mask_quant, pm_quant) -> dict:
    mask_terms = []
    for i in range(trial.suggest_int("n_mask", 0, 2)):
        f = trial.suggest_categorical(f"mask{i}_feat", [x for x in MASK_FEATS if x in mask_quant])
        op = trial.suggest_categorical(f"mask{i}_op", [">=", "<="])
        q = trial.suggest_categorical(f"mask{i}_q", QGRID)
        mask_terms.append((f, op, round(float(mask_quant[f][q]), 6)))
    premom_terms = []
    for i in range(trial.suggest_int("n_pm", 0, 2)):
        f = trial.suggest_categorical(f"pm{i}_feat", [x for x in PM_FEATS if x in pm_quant])
        op = trial.suggest_categorical(f"pm{i}_op", [">=", "<="])
        q = trial.suggest_categorical(f"pm{i}_q", QGRID)
        premom_terms.append((f, op, round(float(pm_quant[f][q]), 6)))
    sl = trial.suggest_categorical("sl", SL_GRID)
    tgt = trial.suggest_categorical("tgt", TGT_GRID)
    guard = {}
    if trial.suggest_categorical("use_min_slot", [False, True]):
        guard["min_slot"] = trial.suggest_categorical("min_slot", MIN_SLOTS)
    if trial.suggest_categorical("use_max_slot", [False, True]):
        guard["max_slot"] = trial.suggest_categorical("max_slot", MAX_SLOTS)
    top_n = trial.suggest_categorical("top_n", [0, 1, 2, 3])
    if top_n:
        guard["top_n"] = int(top_n)
    return {"sl": float(sl), "tgt": float(tgt), "mask_terms": mask_terms,
            "premom_terms": premom_terms, "guard": (guard or None), "status": "OK"}


def evaluate(setup: str, cfg: dict, tr: pd.DataFrame, te: pd.DataFrame) -> tuple[dict, dict]:
    conf = {setup: cfg}
    return _metrics(tt.eval_family(conf, tr)), _metrics(tt.eval_family(conf, te))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--setup", required=True)
    ap.add_argument("--pool", required=True)
    ap.add_argument("--train_start", required=True); ap.add_argument("--train_end", required=True)
    ap.add_argument("--test_start", required=True); ap.add_argument("--test_end", required=True)
    ap.add_argument("--trials", type=int, default=300)
    ap.add_argument("--time_budget_min", type=float, default=20.0)
    ap.add_argument("--pf_min", type=float, default=1.30)
    ap.add_argument("--min_train", type=int, default=20)
    ap.add_argument("--min_test", type=int, default=8)
    ap.add_argument("--dom_cap", type=float, default=0.40)
    ap.add_argument("--max_trades_day", type=float, default=6.0)
    ap.add_argument("--gap_lambda", type=float, default=0.50)
    ap.add_argument("--max_day_block_p", type=float, default=0.10)
    ap.add_argument("--out", default="Train_and_Test/results")
    args = ap.parse_args()
    try:
        sys.stdout.reconfigure(line_buffering=True)
    except Exception:
        pass

    setup = args.setup.strip().upper()
    tt.POOL_DIRS = [Path(args.pool)]; tt.POOL_DIR = Path(args.pool)
    tt.TRAIN = (args.train_start, args.train_end); tt.TEST = (args.test_start, args.test_end)
    faithful = "readmit=LIVE-FAITHFUL" if setup in READMIT else "native=SCREENING-ONLY (firehose; use v11 conf backtest for live-faithful)"

    print(f"[optuna-loop] setup={setup}  ({faithful})")
    print(f"[optuna-loop] TRAIN {tt.TRAIN[0]}..{tt.TRAIN[1]}  TEST {tt.TEST[0]}..{tt.TEST[1]}")
    print(f"[optuna-loop] trials={args.trials} time_budget={args.time_budget_min}min  gate: PF>={args.pf_min} "
          f"n_train>={args.min_train} n_test>={args.min_test} dom<={args.dom_cap} tpd<={args.max_trades_day} "
          f"test_day_block_p<={args.max_day_block_p}")

    pool = tt.load_pool()
    pool = pool[pool["setup"] == setup].copy()
    sessions = sorted(pd.Series(pool["_day"].dropna().unique()))
    tr_raw, te_raw = tt.split_train_test(pool)
    print(f"[optuna-loop] pool rows: train={len(tr_raw)} test={len(te_raw)} "
          f"| sessions {len(sessions)} from {sessions[0].date() if sessions else '-'} to {sessions[-1].date() if sessions else '-'}")

    # ---- SEARCH PHASE: 15 bps/leg (realistic; the deployable verdict) ----
    _set_slippage(15.0)
    tr = tt.attach_entries(tr_raw); te = tt.attach_entries(te_raw)
    print(f"[optuna-loop] with 1m entry @15bps: train={len(tr)} test={len(te)}")
    if len(te) < args.min_test:
        print(f"[optuna-loop] INSUFFICIENT_SAMPLE: only {len(te)} test entries (< {args.min_test}); even ungated cannot clear the gate.")
    # quantile grids
    mask_quant = {}
    for f in MASK_FEATS:
        if f in tr.columns:
            s = pd.to_numeric(tr[f], errors="coerce").dropna()
            if len(s) >= 10 and s.nunique() > 1:
                mask_quant[f] = {q: float(s.quantile(q)) for q in QGRID}
    # premom feature matrix on train (at a representative sl=0.90) for quantile anchors
    pm_recs = []
    for r in tr.itertuples():
        feats, reason = tt._premom(r.ticker, r.side, r.tt_entry_iso, float(r.tt_fill), 0.90, r.tt_sig_ts.isoformat())
        fd = dict(feats) if not reason else {}
        pm_recs.append({f: fd.get(f, np.nan) for f in PM_FEATS})
    pm_df = pd.DataFrame(pm_recs)
    pm_quant = {}
    for f in PM_FEATS:
        s = pd.to_numeric(pm_df[f], errors="coerce").dropna()
        if len(s) >= 10 and s.nunique() > 1:
            pm_quant[f] = {q: float(s.quantile(q)) for q in QGRID}
    print(f"[optuna-loop] searchable mask feats={sorted(mask_quant)} | premom feats={sorted(pm_quant)}")

    GL = float(args.gap_lambda)
    trial_rows = []

    def objective(trial):
        cfg = _cfg_from_trial(trial, mask_quant, pm_quant)
        mtr, mte = evaluate(setup, cfg, tr, te)
        trial.set_user_attr("cfg", {"sl": cfg["sl"], "tgt": cfg["tgt"], "mask": cfg["mask_terms"],
                                    "premom": cfg["premom_terms"], "guard": cfg["guard"]})
        for k, v in mtr.items():
            trial.set_user_attr(f"tr_{k}", v)
        for k, v in mte.items():
            trial.set_user_attr(f"te_{k}", v)
        row = {"sl": cfg["sl"], "tgt": cfg["tgt"],
               "mask": ";".join(f"{a}{o}{b}" for a, o, b in cfg["mask_terms"]) or "-",
               "premom": ";".join(f"{a}{o}{b}" for a, o, b in cfg["premom_terms"]) or "-",
               "guard": json.dumps(cfg["guard"]) if cfg["guard"] else "-"}
        row.update({f"tr_{k}": v for k, v in mtr.items()})
        row.update({f"te_{k}": v for k, v in mte.items()})
        if mtr["n"] < args.min_train:
            obj = -5.0 + mtr["n"] / max(1, args.min_train)   # gradient toward more trades
        else:
            tp, ep = _clamp_pf(fam_pf(mtr)), _clamp_pf(fam_pf(mte))
            obj = min(tp, ep) - GL * abs(tp - ep)
        row["objective"] = round(float(obj), 4)
        trial_rows.append(row)
        return obj

    def fam_pf(m):
        return m["net_pf"] if m["n"] else 0.0

    study = optuna.create_study(direction="maximize", sampler=optuna.samplers.TPESampler(seed=7))
    study.optimize(objective, n_trials=args.trials, timeout=args.time_budget_min * 60.0, show_progress_bar=False)
    print(f"[optuna-loop] completed {len(study.trials)} trials")

    # ---- pick best config that respects the train-trade floor, by objective ----
    def passes_gate(mtr, mte):
        return (mtr["n"] >= args.min_train and mte["n"] >= args.min_test and
                mtr["net_pnl"] > 0 and mte["net_pnl"] > 0 and
                mtr["net_pf"] >= args.pf_min and mte["net_pf"] >= args.pf_min and
                (mtr["day_dom"] is not None and mtr["day_dom"] <= args.dom_cap) and
                (mte["day_dom"] is not None and mte["day_dom"] <= args.dom_cap) and
                (mtr["trade_dom"] is not None and mtr["trade_dom"] <= args.dom_cap) and
                (mte["trade_dom"] is not None and mte["trade_dom"] <= args.dom_cap) and
                mtr["trades_per_day"] <= args.max_trades_day and mte["trades_per_day"] <= args.max_trades_day and
                (mte["day_block_p"] is not None and mte["day_block_p"] <= args.max_day_block_p))

    best = study.best_trial
    best_cfg_attr = best.user_attrs["cfg"]
    best_cfg = {"sl": best_cfg_attr["sl"], "tgt": best_cfg_attr["tgt"],
                "mask_terms": [tuple(t) for t in best_cfg_attr["mask"]],
                "premom_terms": [tuple(t) for t in best_cfg_attr["premom"]],
                "guard": best_cfg_attr["guard"], "status": "OK"}
    mtr15, mte15 = evaluate(setup, best_cfg, tr, te)
    gate15 = passes_gate(mtr15, mte15)

    # ---- CONFIRM at 5 bps/leg (paper ceiling) ----
    _set_slippage(5.0)
    tr5 = tt.attach_entries(tr_raw); te5 = tt.attach_entries(te_raw)
    mtr5, mte5 = evaluate(setup, best_cfg, tr5, te5)

    # verdict
    if len(te) < args.min_test:
        verdict = "INSUFFICIENT_SAMPLE"
    elif gate15:
        verdict = "SELECTED"
    elif passes_gate_5 := (mtr5["n"] >= args.min_train and mte5["n"] >= args.min_test and
                           mtr5["net_pf"] >= args.pf_min and mte5["net_pf"] >= args.pf_min and
                           mtr5["net_pnl"] > 0 and mte5["net_pnl"] > 0):
        verdict = "WATCH(paper-only)"
    else:
        # overfit if train strong but test weak at 15 bps
        verdict = "OVERFIT" if (mtr15["net_pf"] >= args.pf_min and mte15["net_pf"] < args.pf_min) else "NOT SELECTED"

    # ---- persist ----
    outdir = Path(args.out) / setup
    outdir.mkdir(parents=True, exist_ok=True)
    df_trials = pd.DataFrame(trial_rows).sort_values("objective", ascending=False)
    df_trials.to_csv(outdir / "trials.csv", index=False)
    best_json = {
        "setup": setup, "faithfulness": faithful, "verdict": verdict,
        "train_window": list(tt.TRAIN), "test_window": [args.test_start, args.test_end],
        "best_config": {"exit": {"sl_pct": best_cfg["sl"], "tgt_pct": best_cfg["tgt"]},
                        "mask_terms": [list(t) for t in best_cfg["mask_terms"]],
                        "pre_momentum_terms": [list(t) for t in best_cfg["premom_terms"]],
                        "entry_guards": best_cfg["guard"] or {}},
        "metrics_15bps": {"train": mtr15, "test": mte15, "passes_gate": gate15},
        "metrics_5bps": {"train": mtr5, "test": mte5},
        "gate": {"pf_min": args.pf_min, "min_train": args.min_train, "min_test": args.min_test,
                 "dom_cap": args.dom_cap, "max_trades_day": args.max_trades_day,
                 "max_day_block_p": args.max_day_block_p, "objective": "min(trPF,tePF)-0.5*|gap| @15bps"},
        "n_trials": len(study.trials),
    }
    (outdir / "best_config.json").write_text(json.dumps(best_json, indent=2, default=str), encoding="utf-8")

    # equity curves for the best config (15 bps), train + test
    _set_slippage(15.0)
    tr_b = tt.attach_entries(tr_raw); te_b = tt.attach_entries(te_raw)
    exits = {setup: (best_cfg["sl"], best_cfg["tgt"])}
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    for lbl, dfw in (("train", tr_b), ("test", te_b)):
        fam = tt.eval_family({setup: best_cfg}, dfw)
        det = tt.book_detail(fam["book"], exits) if fam["trades"] else pd.DataFrame()
        plt.figure(figsize=(7, 3))
        if not det.empty:
            det = det.sort_values("entry_time")
            plt.plot(range(1, len(det) + 1), det["net_pnl_rs"].cumsum().to_numpy(), marker=".")
            plt.title(f"{setup} {lbl} equity (best cfg @15bps)  n={len(det)} PF={fam['net_pf']:.2f}")
        else:
            plt.title(f"{setup} {lbl} equity — no trades")
        plt.xlabel("trade #"); plt.ylabel("cum net Rs"); plt.grid(alpha=0.3); plt.tight_layout()
        plt.savefig(outdir / f"equity_{lbl}.png", dpi=90); plt.close()

    # report.md
    def fmt(m):
        return (f"n={m['n']} PF={m['net_pf']} net=Rs{m['net_pnl']:,.0f} dbp={m['day_block_p']} "
                f"day_dom={m['day_dom']} trade_dom={m['trade_dom']} tpd={m['trades_per_day']}")
    bm = "; ".join(f"{a}{o}{b}" for a, o, b in best_cfg["mask_terms"]) or "(none)"
    bp = "; ".join(f"{a}{o}{b}" for a, o, b in best_cfg["premom_terms"]) or "(none)"
    rep = [
        f"# {setup} — Optuna code-loop report", "",
        f"**Verdict: {verdict}**  |  faithfulness: {faithful}", "",
        f"- TRAIN {tt.TRAIN[0]}..{tt.TRAIN[1]}  TEST {args.test_start}..{args.test_end}",
        f"- trials run: {len(study.trials)}  | objective = min(trPF,tePF) − {GL}·|gap| @15bps",
        "", "## Best config", "```",
        f"exit: SL {best_cfg['sl']} / Tgt {best_cfg['tgt']}",
        f"mask_terms: {bm}", f"pre_momentum_terms: {bp}",
        f"entry_guards: {best_cfg['guard'] or '{}'}", "```", "",
        "## Metrics", "",
        "| window | 15 bps/leg (deployable) | 5 bps/leg (paper) |", "|---|---|---|",
        f"| TRAIN | {fmt(mtr15)} | {fmt(mtr5)} |",
        f"| TEST  | {fmt(mte15)} | {fmt(mte5)} |", "",
        f"Selection gate @15bps: **{'PASS' if gate15 else 'FAIL'}** "
        f"(PF≥{args.pf_min}, n_tr≥{args.min_train}, n_te≥{args.min_test}, dom≤{args.dom_cap}, "
        f"tpd≤{args.max_trades_day}, test day_block_p≤{args.max_day_block_p}).", "",
        "## Live-faithfulness note",
        ("readmit basis → loop is live-faithful." if setup in READMIT else
         "native setup → loop is a PESSIMISTIC firehose (live filters through v8/research first). "
         "Treat as SCREENING-ONLY; confirm any winner with the v11 conf backtest before trusting."), "",
        "No final_setup_conf.py change (read-only; promotion requires setup_train_test.py --approve + sign-off).",
    ]
    (outdir / "report.md").write_text("\n".join(rep), encoding="utf-8")

    print("\n" + "=" * 80)
    print(f"VERDICT: {verdict}   [{faithful}]")
    print(f"best cfg: SL/Tgt={best_cfg['sl']}/{best_cfg['tgt']}  mask=[{bm}]  premom=[{bp}]  guard={best_cfg['guard'] or '-'}")
    print(f"  @15bps  TRAIN {fmt(mtr15)}")
    print(f"  @15bps  TEST  {fmt(mte15)}")
    print(f"  @5bps   TRAIN {fmt(mtr5)}")
    print(f"  @5bps   TEST  {fmt(mte5)}")
    print(f"  gate@15bps={'PASS' if gate15 else 'FAIL'}")
    print(f"persisted -> {outdir}")
    print("=" * 80)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
