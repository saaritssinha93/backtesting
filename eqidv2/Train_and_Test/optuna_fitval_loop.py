r"""optuna_fitval_loop.py — FIT/VAL-inner-optimize, TRAIN/TEST-confirm code-loop for ONE setup.

Protocol (session-based; windows inferred from the setup pool's sorted distinct sessions):
    TEST  = last 5 completed sessions
    TRAIN = the 10 sessions immediately before TEST
    FIT   = first 5 sessions of TRAIN
    VAL   = last  5 sessions of TRAIN
Optimize ONLY on FIT and VAL:  score = min(FIT_PF, VAL_PF) − GAP_LAMBDA·|FIT_PF − VAL_PF|.
After the loop: score the best config on full TRAIN (=FIT+VAL) and on TEST exactly once (no further tuning).

Search space = ONLY repo-supported knobs (mask_terms / pre_momentum_terms / sl,tgt / entry_guards
min_slot,max_slot,top_n / max_positions / daily_loss_rs / regime_align,regime_band). Reuses
setup_train_test.eval_family (entry+exit+cost+dedupe+premom+mask+overlay). Net of v6 cost.

Optuna TPE if available; else a deterministic seeded random-search fallback (logged).

Run from repo root:
  py -3.12 Train_and_Test\optuna_fitval_loop.py --setup B_AVWAP_RECLAIM_REVERSAL --pool <per-setup pool> \
     --trials 300 --time_budget_min 20 --seed 7 --out Train_and_Test\results
"""
from __future__ import annotations

import argparse
import json
import random
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd

_HERE = Path(__file__).resolve().parent
for _p in (str(_HERE.parent), str(_HERE)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import setup_train_test as tt  # noqa: E402

try:
    import optuna
    optuna.logging.set_verbosity(optuna.logging.WARNING)
    _HAVE_OPTUNA = True
except Exception:
    _HAVE_OPTUNA = False

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
MAXPOS_GRID = [10, 20]
DLOSS_GRID = [0.0, 4000.0]
READMIT = {"A_MOD_BREAK_C1_LOW", "B_HUGE_RED_FAILED_BOUNCE", "C_OR_BREAKDOWN",
           "G_LOWER_LOW_BREAK", "L_DOUBLE_BOTTOM_VWAP", "L_PRESSURE_BURST_VWAP"}


def _clamp_pf(pf):
    return 10.0 if not np.isfinite(pf) else min(float(pf), 10.0)


def _set_slippage(s):
    tt.SLIPPAGE_BPS = float(s)
    tt._entry.cache_clear(); tt._resolve_full.cache_clear(); tt._premom.cache_clear()


def _eval(setup, cfg, df):
    """Return (trades, net_pf, n_days) for a config on a window — fast (no per-trade detail)."""
    fam = tt.eval_family({setup: cfg}, df)
    book = fam.get("book")
    n_days = int(pd.Series(book["_day"]).nunique()) if (book is not None and len(book)) else 0
    return int(fam["trades"]), float(fam["net_pf"]), n_days


def _gate_metrics(setup, cfg, df):
    """Full metrics incl. gross-profit / day / symbol dominance via per-trade detail."""
    fam = tt.eval_family({setup: cfg}, df)
    exits = {setup: (cfg["sl"], cfg["tgt"])}
    det = tt.book_detail(fam["book"], exits) if fam["trades"] else pd.DataFrame()
    out = {"n": int(fam["trades"]), "net_pf": round(float(fam["net_pf"]), 3),
           "net_pnl": round(float(fam["net_pnl"]), 0),
           "day_block_p": (None if not np.isfinite(fam["day_block_p"]) else round(float(fam["day_block_p"]), 4)),
           "n_days": 0, "n_syms": 0, "trades_per_day": 0.0,
           "trade_dom_gross": None, "day_dom": None, "sym_dom": None, "detail": det}
    if det.empty:
        return out
    net = det["net_pnl_rs"].to_numpy()
    tot = float(net.sum())
    gross_profit = float(net[net > 0].sum())
    out["n_days"] = int(det["trade_date"].nunique())
    out["n_syms"] = int(det["ticker"].nunique())
    out["trades_per_day"] = round(out["n"] / max(1, out["n_days"]), 2)
    out["trade_dom_gross"] = round(float(net.max()) / gross_profit, 3) if gross_profit > 0 else 9.99
    if tot > 0:
        out["day_dom"] = round(float(det.groupby("trade_date")["net_pnl_rs"].sum().max()) / tot, 3)
        out["sym_dom"] = round(float(det.groupby("ticker")["net_pnl_rs"].sum().max()) / tot, 3)
    else:
        out["day_dom"] = out["sym_dom"] = 9.99
    return out


def _suggest(trial, mask_quant, pm_quant, max_mask=2, max_pm=2):
    """Build a config dict from a (optuna trial | dict of choices)."""
    def cat(name, choices):
        return trial.suggest_categorical(name, choices)
    def integ(name, lo, hi):
        return trial.suggest_int(name, lo, hi)
    mask_terms = []
    for i in range(integ("n_mask", 0, max_mask)):
        f = cat(f"mask{i}_feat", [x for x in MASK_FEATS if x in mask_quant])
        op = cat(f"mask{i}_op", [">=", "<="])
        q = cat(f"mask{i}_q", QGRID)
        mask_terms.append((f, op, round(float(mask_quant[f][q]), 6)))
    premom_terms = []
    for i in range(integ("n_pm", 0, max_pm)):
        f = cat(f"pm{i}_feat", [x for x in PM_FEATS if x in pm_quant])
        op = cat(f"pm{i}_op", [">=", "<="])
        q = cat(f"pm{i}_q", QGRID)
        premom_terms.append((f, op, round(float(pm_quant[f][q]), 6)))
    guard = {}
    if cat("use_min_slot", [False, True]):
        guard["min_slot"] = cat("min_slot", MIN_SLOTS)
    if cat("use_max_slot", [False, True]):
        guard["max_slot"] = cat("max_slot", MAX_SLOTS)
    tn = cat("top_n", [0, 1, 2, 3])
    if tn:
        guard["top_n"] = int(tn)
    return {"sl": float(cat("sl", SL_GRID)), "tgt": float(cat("tgt", TGT_GRID)),
            "mask_terms": mask_terms, "premom_terms": premom_terms, "guard": (guard or None),
            "status": "OK",
            "max_positions": int(cat("max_positions", MAXPOS_GRID)),
            "daily_loss_rs": float(cat("daily_loss_rs", DLOSS_GRID))}


class _RandTrial:
    """Minimal optuna-trial-like shim for the seeded random fallback."""
    def __init__(self, rng): self.rng = rng; self.params = {}
    def suggest_categorical(self, name, choices):
        v = choices[self.rng.randrange(len(choices))]; self.params[name] = v; return v
    def suggest_int(self, name, lo, hi):
        v = self.rng.randint(lo, hi); self.params[name] = v; return v
    def set_user_attr(self, *a, **k): pass


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--setup", required=True)
    ap.add_argument("--pool", required=True)
    ap.add_argument("--trials", type=int, default=300)
    ap.add_argument("--time_budget_min", type=float, default=20.0)
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--pf_min", type=float, default=1.30)
    ap.add_argument("--net_min", type=float, default=0.0)
    ap.add_argument("--min_trades_train", type=int, default=15)
    ap.add_argument("--min_trades_test", type=int, default=5)
    ap.add_argument("--dom_cap", type=float, default=0.40)
    ap.add_argument("--max_trades_day", type=float, default=6.0)
    ap.add_argument("--gap_lambda", type=float, default=0.50)
    ap.add_argument("--slippage_bps", type=float, default=15.0)
    # ---- STRICTER selection controls ----
    ap.add_argument("--max_mask_terms", type=int, default=2, help="max AND-combined mask terms (raise for tighter gating)")
    ap.add_argument("--max_pm_terms", type=int, default=2, help="max AND-combined pre-momentum terms")
    ap.add_argument("--search_tpd_cap", type=float, default=1e9,
                    help="reject trials whose FIT or VAL trades/day exceed this (forces a SELECTIVE, low-frequency config)")
    ap.add_argument("--max_trades_fitval", type=int, default=10**9,
                    help="reject trials whose FIT or VAL trade COUNT exceeds this (hard selectivity cap)")
    ap.add_argument("--out", default="Train_and_Test/results")
    args = ap.parse_args()
    try:
        sys.stdout.reconfigure(line_buffering=True)
    except Exception:
        pass

    setup = args.setup.strip().upper()
    min_fv = max(args.min_trades_test, args.min_trades_train // 2)   # per-window (FIT/VAL) floor
    faithful = "readmit=LIVE-FAITHFUL" if setup in READMIT else "native=SCREENING-ONLY (firehose; use v11 conf backtest for live-faithful)"
    engine = "Optuna TPE" if _HAVE_OPTUNA else "Optuna unavailable; using seeded random search fallback."
    print(f"[fitval-loop] setup={setup}  ({faithful})")
    print(f"[fitval-loop] optimizer: {engine}")

    tt.POOL_DIRS = [Path(args.pool)]; tt.POOL_DIR = Path(args.pool)
    pool = tt.load_pool()
    pool = pool[pool["setup"] == setup].copy()
    sessions = sorted(pd.Series(pool["_day"].dropna().unique()))
    if len(sessions) < 15:
        print(f"[fitval-loop] INSUFFICIENT sessions: {len(sessions)} < 15 -> cannot form FIT/VAL/TRAIN/TEST")
        return 0
    TEST_s = sessions[-5:]; TRAIN_s = sessions[-15:-5]; FIT_s = TRAIN_s[:5]; VAL_s = TRAIN_s[5:]

    def _rng(ss):
        return f"{pd.Timestamp(ss[0]).date()}..{pd.Timestamp(ss[-1]).date()}"
    print(f"[fitval-loop] FIT   {_rng(FIT_s)}  ({len(FIT_s)} sessions)")
    print(f"[fitval-loop] VAL   {_rng(VAL_s)}  ({len(VAL_s)} sessions)")
    print(f"[fitval-loop] TRAIN {_rng(TRAIN_s)}  ({len(TRAIN_s)} sessions)")
    print(f"[fitval-loop] TEST  {_rng(TEST_s)}  ({len(TEST_s)} sessions)")
    print(f"[fitval-loop] gate: PF>={args.pf_min} net>{args.net_min} train_n>={args.min_trades_train} "
          f"test_n>={args.min_trades_test} dom<={args.dom_cap} tpd<={args.max_trades_day} | FIT/VAL trade floor={min_fv}")

    span = set(map(pd.Timestamp, FIT_s + VAL_s + TEST_s))
    sub = pool[pool["_day"].isin(span)].copy()
    _set_slippage(args.slippage_bps)
    sub = tt.attach_entries(sub)

    def _slice(ss):
        return sub[sub["_day"].isin(set(map(pd.Timestamp, ss)))].copy()
    FIT, VAL, TRAIN, TEST = _slice(FIT_s), _slice(VAL_s), _slice(FIT_s + VAL_s), _slice(TEST_s)
    print(f"[fitval-loop] entries @ {args.slippage_bps}bps: FIT={len(FIT)} VAL={len(VAL)} TRAIN={len(TRAIN)} TEST={len(TEST)}")

    # quantile grids from TRAIN (FIT+VAL) only — never TEST
    mask_quant = {}
    for f in MASK_FEATS:
        if f in TRAIN.columns:
            s = pd.to_numeric(TRAIN[f], errors="coerce").dropna()
            if len(s) >= 8 and s.nunique() > 1:
                mask_quant[f] = {q: float(s.quantile(q)) for q in QGRID}
    pm_recs = []
    for r in TRAIN.itertuples():
        feats, reason = tt._premom(r.ticker, r.side, r.tt_entry_iso, float(r.tt_fill), 0.90, r.tt_sig_ts.isoformat())
        fd = dict(feats) if not reason else {}
        pm_recs.append({f: fd.get(f, np.nan) for f in PM_FEATS})
    pm_df = pd.DataFrame(pm_recs)
    pm_quant = {}
    for f in PM_FEATS:
        s = pd.to_numeric(pm_df[f], errors="coerce").dropna()
        if len(s) >= 8 and s.nunique() > 1:
            pm_quant[f] = {q: float(s.quantile(q)) for q in QGRID}
    print(f"[fitval-loop] searchable mask={sorted(mask_quant)} | premom={sorted(pm_quant)}")

    GL = float(args.gap_lambda)
    trial_rows = []
    t0 = time.time()

    def score_cfg(cfg):
        tt.MAX_POSITIONS = cfg.get("max_positions", 20)
        tt.DAILY_LOSS_RS = cfg.get("daily_loss_rs", 0.0)
        nf, pf_f, df_ = _eval(setup, cfg, FIT)
        nv, pf_v, dv_ = _eval(setup, cfg, VAL)
        tpd_f, tpd_v = nf / max(1, df_), nv / max(1, dv_)
        if nf < min_fv or nv < min_fv:                       # too FEW -> nudge toward more trades
            return -5.0 + (min(nf, nv) / max(1, min_fv)), nf, pf_f, nv, pf_v
        if (tpd_f > args.search_tpd_cap or tpd_v > args.search_tpd_cap
                or nf > args.max_trades_fitval or nv > args.max_trades_fitval):
            return -2.0, nf, pf_f, nv, pf_v                  # over-fires -> not selective enough, reject
        cf, cv = _clamp_pf(pf_f), _clamp_pf(pf_v)
        return (min(cf, cv) - GL * abs(cf - cv)), nf, pf_f, nv, pf_v

    def record(cfg, sc, nf, pf_f, nv, pf_v):
        trial_rows.append({"sl": cfg["sl"], "tgt": cfg["tgt"],
                           "mask": ";".join(f"{a}{o}{b}" for a, o, b in cfg["mask_terms"]) or "-",
                           "premom": ";".join(f"{a}{o}{b}" for a, o, b in cfg["premom_terms"]) or "-",
                           "guard": json.dumps(cfg["guard"]) if cfg["guard"] else "-",
                           "max_positions": cfg.get("max_positions"), "daily_loss_rs": cfg.get("daily_loss_rs"),
                           "fit_n": nf, "fit_pf": round(pf_f, 3), "val_n": nv, "val_pf": round(pf_v, 3),
                           "score": round(float(sc), 4)})

    best = {"score": -1e9, "cfg": None}

    if _HAVE_OPTUNA:
        def objective(trial):
            cfg = _suggest(trial, mask_quant, pm_quant, args.max_mask_terms, args.max_pm_terms)
            sc, nf, pf_f, nv, pf_v = score_cfg(cfg)
            record(cfg, sc, nf, pf_f, nv, pf_v)
            if sc > best["score"]:
                best["score"], best["cfg"] = sc, cfg
            return sc
        study = optuna.create_study(direction="maximize", sampler=optuna.samplers.TPESampler(seed=args.seed))
        study.optimize(objective, n_trials=args.trials, timeout=args.time_budget_min * 60.0)
        n_done = len(study.trials)
    else:
        rng = random.Random(args.seed)
        n_done = 0
        for _ in range(args.trials):
            if time.time() - t0 > args.time_budget_min * 60.0:
                break
            cfg = _suggest(_RandTrial(rng), mask_quant, pm_quant, args.max_mask_terms, args.max_pm_terms)
            sc, nf, pf_f, nv, pf_v = score_cfg(cfg)
            record(cfg, sc, nf, pf_f, nv, pf_v)
            if sc > best["score"]:
                best["score"], best["cfg"] = sc, cfg
            n_done += 1
    print(f"[fitval-loop] completed {n_done} trials | best FIT/VAL score={best['score']:.4f}")

    bc = best["cfg"]
    tt.MAX_POSITIONS = bc.get("max_positions", 20); tt.DAILY_LOSS_RS = bc.get("daily_loss_rs", 0.0)
    # confirm on full TRAIN and TEST (once), at search slippage + paper 5bps
    mTR = _gate_metrics(setup, bc, TRAIN); mTE = _gate_metrics(setup, bc, TEST)
    _set_slippage(5.0); TRAIN5 = _slice(FIT_s + VAL_s); TEST5 = _slice(TEST_s)
    mTR5 = _gate_metrics(setup, bc, TRAIN5); mTE5 = _gate_metrics(setup, bc, TEST5)
    _set_slippage(args.slippage_bps)

    def dom_ok(m):
        return (m["trade_dom_gross"] is not None and m["trade_dom_gross"] <= args.dom_cap and
                m["day_dom"] is not None and m["day_dom"] <= args.dom_cap and
                m["sym_dom"] is not None and m["sym_dom"] <= args.dom_cap)
    gate = (mTR["n"] >= args.min_trades_train and mTE["n"] >= args.min_trades_test and
            mTR["net_pnl"] > args.net_min and mTE["net_pnl"] > args.net_min and
            mTR["net_pf"] >= args.pf_min and mTE["net_pf"] >= args.pf_min and
            dom_ok(mTR) and dom_ok(mTE) and
            mTR["trades_per_day"] <= args.max_trades_day and mTE["trades_per_day"] <= args.max_trades_day)
    if mTE["n"] < args.min_trades_test or mTR["n"] < args.min_trades_train:
        verdict = "INSUFFICIENT_SAMPLE"
    elif gate:
        verdict = "SELECTED"
    elif mTR["net_pf"] >= args.pf_min and mTE["net_pf"] < args.pf_min:
        verdict = "OVERFIT"
    else:
        verdict = "NOT SELECTED"

    outdir = Path(args.out) / setup
    outdir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(trial_rows).sort_values("score", ascending=False).to_csv(outdir / "trials.csv", index=False)
    bj = {"setup": setup, "faithfulness": faithful, "optimizer": engine, "verdict": verdict,
          "windows": {"FIT": _rng(FIT_s), "VAL": _rng(VAL_s), "TRAIN": _rng(TRAIN_s), "TEST": _rng(TEST_s)},
          "best_config": {"exit": {"sl_pct": bc["sl"], "tgt_pct": bc["tgt"]},
                          "mask_terms": [list(t) for t in bc["mask_terms"]],
                          "pre_momentum_terms": [list(t) for t in bc["premom_terms"]],
                          "entry_guards": bc["guard"] or {}, "max_positions": bc.get("max_positions"),
                          "daily_loss_rs": bc.get("daily_loss_rs")},
          "metrics": {"train_15bps": {k: v for k, v in mTR.items() if k != "detail"},
                      "test_15bps": {k: v for k, v in mTE.items() if k != "detail"},
                      "train_5bps": {k: v for k, v in mTR5.items() if k != "detail"},
                      "test_5bps": {k: v for k, v in mTE5.items() if k != "detail"}},
          "n_trials": n_done}
    (outdir / "best_config.json").write_text(json.dumps(bj, indent=2, default=str), encoding="utf-8")

    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    for lbl, m in (("train", mTR), ("test", mTE)):
        det = m["detail"]
        plt.figure(figsize=(7, 3))
        if not det.empty:
            det = det.sort_values("entry_time")
            plt.plot(range(1, len(det) + 1), det["net_pnl_rs"].cumsum().to_numpy(), marker=".")
            plt.title(f"{setup} {lbl} equity (best cfg @{args.slippage_bps:.0f}bps) n={m['n']} PF={m['net_pf']}")
        else:
            plt.title(f"{setup} {lbl} — no trades")
        plt.xlabel("trade #"); plt.ylabel("cum net Rs"); plt.grid(alpha=0.3); plt.tight_layout()
        plt.savefig(outdir / f"equity_{lbl}.png", dpi=90); plt.close()

    def fmt(m):
        return (f"n={m['n']} PF={m['net_pf']} net=Rs{m['net_pnl']:,.0f} tradeDomGross={m['trade_dom_gross']} "
                f"dayDom={m['day_dom']} symDom={m['sym_dom']} tpd={m['trades_per_day']} dbp={m['day_block_p']}")
    bm = "; ".join(f"{a}{o}{b}" for a, o, b in bc["mask_terms"]) or "(none)"
    bp = "; ".join(f"{a}{o}{b}" for a, o, b in bc["premom_terms"]) or "(none)"
    cmd = (f"py -3.12 Train_and_Test\\optuna_fitval_loop.py --setup {setup} --pool {args.pool} "
           f"--trials {args.trials} --time_budget_min {args.time_budget_min} --seed {args.seed} --out {args.out}")
    rep = [f"# {setup} — FIT/VAL→TRAIN/TEST code-loop report", "",
           f"**Verdict: {verdict}**  |  faithfulness: {faithful}  |  optimizer: {engine}", "",
           f"- FIT {_rng(FIT_s)} | VAL {_rng(VAL_s)} | TRAIN {_rng(TRAIN_s)} | TEST {_rng(TEST_s)}",
           f"- trials: {n_done}  | objective on FIT/VAL only = min(PF) − {GL}·|gap|; TEST scored once", "",
           "## Best config (only knobs the engine honors)", "```",
           f"exit: SL {bc['sl']} / Tgt {bc['tgt']}", f"mask_terms: {bm}", f"pre_momentum_terms: {bp}",
           f"entry_guards: {bc['guard'] or '{}'}  (NB: conf-mask honors only min_slot; top_n/max_slot NOT enforced live)",
           f"max_positions: {bc.get('max_positions')}  daily_loss_rs: {bc.get('daily_loss_rs')}", "```", "",
           "## Metrics (best config)", "",
           "| window | 15 bps/leg | 5 bps/leg |", "|---|---|---|",
           f"| TRAIN | {fmt(mTR)} | {fmt(mTR5)} |", f"| TEST  | {fmt(mTE)} | {fmt(mTE5)} |", "",
           f"Selection gate (TRAIN & TEST @15bps): **{'PASS' if gate else 'FAIL'}** "
           f"(PF≥{args.pf_min}, train_n≥{args.min_trades_train}, test_n≥{args.min_trades_test}, "
           f"gross-profit/day/symbol dominance≤{args.dom_cap}, trades/day≤{args.max_trades_day}).", "",
           "## Live-crosscheck", ("readmit basis → loop is live-faithful." if setup in READMIT else
            "native setup → SCREENING-ONLY firehose; the v11 conf backtest is the live-faithful arbiter. "
            "Doc §5 note for B_AVWAP: the v11 live overlay uses the INVERTED vwap_dist_atr≥0.60 mask (conf uses ≤1.0) "
            "— any winner must not rely on that inverted gate."), "",
           f"## Command\n```\n{cmd}\n```", "",
           "## Files changed\n- none to production. Artifacts under this folder. No final_setup_conf.py edit; no live trades.", "",
           f"## Why {verdict}\n- " + ("cleared all gate rules on TRAIN and TEST." if verdict == "SELECTED" else
            "best FIT/VAL config does not clear the gate on TRAIN+TEST at realistic cost (see metrics; "
            "dominance/PF/sample the binding constraints)."),
           ]
    (outdir / "report.md").write_text("\n".join(rep), encoding="utf-8")

    print("\n" + "=" * 84)
    print(f"VERDICT: {verdict}   [{faithful}]  optimizer: {engine}")
    print(f"best cfg: SL/Tgt={bc['sl']}/{bc['tgt']} mask=[{bm}] premom=[{bp}] guard={bc['guard'] or '-'} "
          f"maxpos={bc.get('max_positions')} dloss={bc.get('daily_loss_rs')}")
    print(f"  @15bps TRAIN {fmt(mTR)}")
    print(f"  @15bps TEST  {fmt(mTE)}")
    print(f"  @5bps  TRAIN {fmt(mTR5)}")
    print(f"  @5bps  TEST  {fmt(mTE5)}")
    print(f"  gate@15bps={'PASS' if gate else 'FAIL'}  -> persisted {outdir}")
    print("=" * 84)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
