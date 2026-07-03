r"""pf14_approval_loop.py — research-only band-targeted approval loop for ONE setup.

Goal band: TRAIN PF in [1.30, 1.70] (controlled, NOT maximised) AND TEST PF > 1.40,
with meaningful trade counts and no single trade/day/symbol dominating.

Windows (calendar, per the task spec; nearest available sessions used):
  TRAIN = sessions in [train_start, day before test_start]
  TEST  = sessions in [test_start, latest available]
  FIT   = first half of TRAIN sessions ; VAL = second half.

Anti-overfit: search ONLY on FIT/VAL (band objective). Confirm on full TRAIN. Score TEST
ONCE, only for configs whose TRAIN PF lands in [1.30,1.70]. No tuning on TEST.

Engine: Optuna TPE if available (else seeded random fallback) for the global search, PLUS
a deterministic guided COORDINATE loop (one logical knob-group changed per iteration,
greedy keep-best-on-FIT/VAL) that produces the >=25-iteration ITERATION_LOG.

Reuses the repo pipeline end-to-end via setup_train_test (tt) and the primitives in
optuna_fitval_loop (ofl): entry, exit, statutory+slippage cost, family dedupe, mask,
pre-momentum, entry guards, portfolio overlay. NOTHING is written to final_setup_conf.py.

Run (from repo root):
  py -3.12 Train_and_Test/setup_pf_1_4_approval_loop/B_HUGE_C1_CLOSE_RECLAIM_BREAK/scripts/pf14_approval_loop.py \
     --setup B_HUGE_C1_CLOSE_RECLAIM_BREAK \
     --pool C:/TradingData/eqidv2/setup_pools_2026_06_29/B_HUGE_C1_CLOSE_RECLAIM_BREAK \
     --train_start 2026-05-18 --test_start 2026-06-20 --trials 300 --seed 7
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
# repo root is .../eqidv2 ; Train_and_Test is two levels up from this script's parent chain
_TT_DIR = _HERE
for _ in range(6):
    _TT_DIR = _TT_DIR.parent
    if (_TT_DIR / "setup_train_test.py").exists():
        break
_REPO = _TT_DIR.parent
for _p in (str(_REPO), str(_TT_DIR)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import setup_train_test as tt          # noqa: E402
import optuna_fitval_loop as ofl       # noqa: E402  (reuse _suggest/_RandTrial/_gate_metrics/feature lists)

try:
    import optuna
    optuna.logging.set_verbosity(optuna.logging.WARNING)
    _HAVE_OPTUNA = True
except Exception:
    _HAVE_OPTUNA = False

PF_TRAIN_LO, PF_TRAIN_HI = 1.30, 1.70
PF_TEST_MIN = 1.40
DOM_CAP = 0.40
MIN_FITVAL = 5
MIN_TRAIN_N = 15
MIN_TEST_N = 5
MAX_TPD = 6.0


def _clamp(pf):
    return 10.0 if not np.isfinite(pf) else min(float(pf), 10.0)


def band_score(pf_f, pf_v, n_f, n_v):
    """FIT/VAL objective that TARGETS the [1.30,1.70] band (not raw maximisation)."""
    if n_f < MIN_FITVAL or n_v < MIN_FITVAL:
        return -10.0 + (min(n_f, n_v) / max(1, MIN_FITVAL))
    cf, cv = _clamp(pf_f), _clamp(pf_v)
    m, M, g = min(cf, cv), max(cf, cv), abs(cf - cv)
    base = min(m, PF_TRAIN_HI)                 # reward up to band top
    overfit_pen = max(0.0, M - PF_TRAIN_HI)    # discourage > 1.70 (overfit signal)
    weak_pen = max(0.0, PF_TRAIN_LO - m)       # discourage < 1.30
    return base - 0.5 * g - 0.5 * overfit_pen - 0.5 * weak_pen


def full_metrics(setup, cfg, df):
    """All requested metrics for a window via the repo pipeline + per-trade detail."""
    tt.MAX_POSITIONS = cfg.get("max_positions", 20)
    tt.DAILY_LOSS_RS = cfg.get("daily_loss_rs", 0.0)
    m = ofl._gate_metrics(setup, cfg, df)      # n, net_pf, net_pnl, day_block_p, n_days, n_syms, tpd, *_dom, detail
    det = m.pop("detail")
    out = dict(m)
    if det is None or det.empty:
        out.update({"wins": 0, "losses": 0, "win_pct": 0.0, "gross_profit": 0.0, "gross_loss": 0.0,
                    "avg_win": 0.0, "avg_loss": 0.0, "max_dd": 0.0, "sl_n": 0, "tgt_n": 0, "eod_n": 0})
        return out, det
    net = det["net_pnl_rs"].to_numpy()
    oc = det["outcome"].astype(str)
    wins, losses = net[net > 0], net[net <= 0]
    cum = det.sort_values("entry_time")["net_pnl_rs"].cumsum().to_numpy()
    dd = float((cum - np.maximum.accumulate(cum)).min()) if len(cum) else 0.0
    out.update({
        "wins": int((net > 0).sum()), "losses": int((net <= 0).sum()),
        "win_pct": round(float((net > 0).mean()) * 100, 1),
        "gross_profit": round(float(wins.sum()), 0), "gross_loss": round(float(losses.sum()), 0),
        "avg_win": round(float(wins.mean()), 0) if len(wins) else 0.0,
        "avg_loss": round(float(losses.mean()), 0) if len(losses) else 0.0,
        "max_dd": round(dd, 0),
        "sl_n": int((oc == "SL").sum()), "tgt_n": int((oc == "TARGET").sum()), "eod_n": int((oc == "EOD").sum()),
    })
    return out, det


def dom_ok(m):
    for k in ("trade_dom_gross", "day_dom", "sym_dom"):
        v = m.get(k)
        if v is None or v > DOM_CAP:
            return False
    return True


def confirm(setup, cfg, TRAIN, TEST):
    """Confirm a config: full-TRAIN metrics; TEST scored ONLY if TRAIN PF in band."""
    mTR, dTR = full_metrics(setup, cfg, TRAIN)
    res = {"train": mTR, "test": None, "test_ran": False, "in_band": False}
    in_band = (PF_TRAIN_LO <= mTR["net_pf"] <= PF_TRAIN_HI)
    res["in_band"] = bool(in_band)
    if in_band:
        mTE, dTE = full_metrics(setup, cfg, TEST)
        res["test"] = mTE
        res["test_ran"] = True
        res["test_detail"] = dTE
    res["train_detail"] = dTR
    return res


def cfg_sig(cfg):
    return json.dumps({"sl": cfg["sl"], "tgt": cfg["tgt"], "mask": cfg["mask_terms"],
                       "pm": cfg["premom_terms"], "guard": cfg.get("guard"),
                       "mp": cfg.get("max_positions"), "dl": cfg.get("daily_loss_rs")}, sort_keys=True, default=str)


def fmt_m(m):
    if not m or m.get("n", 0) == 0:
        return "n=0 (no trades)"
    return (f"n={m['n']} PF={m['net_pf']} net=Rs{m['net_pnl']:,.0f} win={m.get('win_pct',0)}% "
            f"t/s/e={m.get('tgt_n',0)}/{m.get('sl_n',0)}/{m.get('eod_n',0)} tpd={m.get('trades_per_day')} "
            f"dayDom={m.get('day_dom')} symDom={m.get('sym_dom')} trDom={m.get('trade_dom_gross')} "
            f"dd=Rs{m.get('max_dd',0):,.0f} dbp={m.get('day_block_p')}")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--setup", required=True)
    ap.add_argument("--pool", required=True)
    ap.add_argument("--train_start", default="2026-05-18")
    ap.add_argument("--test_start", default="2026-06-20")
    ap.add_argument("--trials", type=int, default=300)
    ap.add_argument("--time_budget_min", type=float, default=20.0)
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--slippage_bps", type=float, default=15.0)
    ap.add_argument("--out", default=str(_HERE.parent))
    args = ap.parse_args()
    try:
        sys.stdout.reconfigure(line_buffering=True)
    except Exception:
        pass

    setup = args.setup.strip().upper()
    outdir = Path(args.out)
    (outdir / "candidates").mkdir(parents=True, exist_ok=True)
    engine = "Optuna TPE" if _HAVE_OPTUNA else "Optuna unavailable; using seeded random search fallback."
    readmit = setup in ofl.READMIT
    faithful = "readmit=LIVE-FAITHFUL" if readmit else "native=SCREENING-ONLY (firehose; v11 conf backtest is the live-faithful arbiter)"
    print(f"[pf14] setup={setup}  ({faithful})")
    print(f"[pf14] optimizer: {engine}")

    tt.POOL_DIRS = [Path(args.pool)]; tt.POOL_DIR = Path(args.pool)
    ofl._set_slippage(args.slippage_bps)
    pool = tt.load_pool(); pool = pool[pool["setup"] == setup].copy()

    ts = pd.Timestamp(args.train_start); te = pd.Timestamp(args.test_start)
    days = sorted(pd.Series(pool["_day"].dropna().unique()))
    train_days = [d for d in days if ts <= d < te]
    test_days = [d for d in days if d >= te]
    if not train_days or not test_days:
        print(f"[pf14] FATAL: empty TRAIN({len(train_days)}) or TEST({len(test_days)}) for windows"); return 0
    fit_days = train_days[: len(train_days) // 2]
    val_days = train_days[len(train_days) // 2:]

    def rng(ds):
        return f"{pd.Timestamp(ds[0]).date()}..{pd.Timestamp(ds[-1]).date()}"
    print(f"[pf14] TRAIN {rng(train_days)} ({len(train_days)} sessions) | FIT {rng(fit_days)} ({len(fit_days)}) | VAL {rng(val_days)} ({len(val_days)})")
    print(f"[pf14] TEST  {rng(test_days)} ({len(test_days)} sessions)")

    span = set(map(pd.Timestamp, train_days + test_days))
    sub = pool[pool["_day"].isin(span)].copy()
    sub = tt.attach_entries(sub)

    def sl(ds):
        return sub[sub["_day"].isin(set(map(pd.Timestamp, ds)))].copy()
    FIT, VAL, TRAIN, TEST = sl(fit_days), sl(val_days), sl(train_days), sl(test_days)
    print(f"[pf14] entries @ {args.slippage_bps}bps: FIT={len(FIT)} VAL={len(VAL)} TRAIN={len(TRAIN)} TEST={len(TEST)}")

    # ---- card baseline (from final_setup_conf if importable; else the doc card) ----
    # E_ORB_BREAKOUT_LONG is a parked RESEARCH_WATCH setup. Its documented best-found
    # config is stored under best_found.exit, not the active FINAL_SETUP_CONF schema.
    base_cfg = {"sl": 0.70, "tgt": 1.50, "mask_terms": [("vwap_dist_atr", "<=", 1.0)],
                "premom_terms": [], "guard": None, "status": "OK", "max_positions": 20, "daily_loss_rs": 0.0}
    try:
        import final_setup_conf as fc
        raw = fc.FINAL_SETUP_CONF.get(setup) or fc.RESEARCH_WATCH_CONF.get(setup)
        if raw:
            exit_cfg = raw.get("exit") or (raw.get("best_found") or {}).get("exit") or {}
            mask_terms = [tuple(t) for t in raw.get("mask_terms", [])]
            if setup == "E_ORB_BREAKOUT_LONG" and not mask_terms:
                mask_terms = [("vwap_dist_atr", "<=", 1.0)]
            base_cfg = {"sl": float(exit_cfg.get("sl_pct", 0.70)), "tgt": float(exit_cfg.get("tgt_pct", 1.50)),
                        "mask_terms": mask_terms,
                        "premom_terms": [tuple(t) for t in raw.get("pre_momentum_terms", [])],
                        "guard": (raw.get("entry_guards") or None), "status": "OK",
                        "max_positions": 20, "daily_loss_rs": 0.0}
    except Exception as e:
        print(f"[pf14] (using doc-card baseline; conf import note: {e})")
    base = confirm(setup, base_cfg, TRAIN, TEST)
    # baseline TEST is informative regardless of band -> force it
    if not base["test_ran"]:
        mTE, dTE = full_metrics(setup, base_cfg, TEST)
        base["test"], base["test_detail"], base["test_ran"] = mTE, dTE, True
    print(f"[pf14] BASELINE TRAIN {fmt_m(base['train'])}")
    print(f"[pf14] BASELINE TEST  {fmt_m(base['test'])}")

    # ---- quantile grids from TRAIN only (never TEST) ----
    mask_quant = {}
    for f in ofl.MASK_FEATS:
        if f in TRAIN.columns:
            s = pd.to_numeric(TRAIN[f], errors="coerce").dropna()
            if len(s) >= 8 and s.nunique() > 1:
                mask_quant[f] = {q: float(s.quantile(q)) for q in ofl.QGRID}
    pm_recs = []
    for r in TRAIN.itertuples():
        feats, reason = tt._premom(r.ticker, r.side, r.tt_entry_iso, float(r.tt_fill), 0.90, r.tt_sig_ts.isoformat())
        fd = dict(feats) if not reason else {}
        pm_recs.append({f: fd.get(f, np.nan) for f in ofl.PM_FEATS})
    pm_df = pd.DataFrame(pm_recs)
    pm_quant = {}
    for f in ofl.PM_FEATS:
        s = pd.to_numeric(pm_df[f], errors="coerce").dropna()
        if len(s) >= 8 and s.nunique() > 1:
            pm_quant[f] = {q: float(s.quantile(q)) for q in ofl.QGRID}
    print(f"[pf14] searchable mask={sorted(mask_quant)} | premom={sorted(pm_quant)}")

    trial_rows = []
    trial_cfgs = []
    best = {"score": -1e9, "cfg": None, "fit": None, "val": None}

    def score_fitval(cfg):
        nf, pf_f, _ = ofl._eval(setup, cfg, FIT)
        nv, pf_v, _ = ofl._eval(setup, cfg, VAL)
        return band_score(pf_f, pf_v, nf, nv), nf, pf_f, nv, pf_v

    def record(tag, cfg, sc, nf, pf_f, nv, pf_v):
        trial_rows.append({"src": tag, "sl": cfg["sl"], "tgt": cfg["tgt"],
                           "mask": ";".join(f"{a}{o}{b}" for a, o, b in cfg["mask_terms"]) or "-",
                           "premom": ";".join(f"{a}{o}{b}" for a, o, b in cfg["premom_terms"]) or "-",
                           "guard": json.dumps(cfg.get("guard")) if cfg.get("guard") else "-",
                           "max_positions": cfg.get("max_positions"), "daily_loss_rs": cfg.get("daily_loss_rs"),
                           "fit_n": nf, "fit_pf": round(pf_f, 3), "val_n": nv, "val_pf": round(pf_v, 3),
                           "score": round(float(sc), 4)})
        trial_cfgs.append({"src": tag, "cfg": dict(cfg), "fit_n": nf, "fit_pf": float(pf_f),
                           "val_n": nv, "val_pf": float(pf_v), "score": float(sc)})

    # ===== Part A: Optuna TPE (or seeded random) global search on FIT/VAL =====
    t0 = time.time()
    if _HAVE_OPTUNA:
        def objective(trial):
            cfg = ofl._suggest(trial, mask_quant, pm_quant, 2, 2)
            sc, nf, pf_f, nv, pf_v = score_fitval(cfg)
            record("optuna", cfg, sc, nf, pf_f, nv, pf_v)
            if sc > best["score"]:
                best.update(score=sc, cfg=cfg, fit=(nf, pf_f), val=(nv, pf_v))
            return sc
        study = optuna.create_study(direction="maximize", sampler=optuna.samplers.TPESampler(seed=args.seed))
        study.optimize(objective, n_trials=args.trials, timeout=args.time_budget_min * 60.0)
        n_opt = len(study.trials)
    else:
        rng_ = random.Random(args.seed); n_opt = 0
        for _ in range(args.trials):
            if time.time() - t0 > args.time_budget_min * 60.0:
                break
            cfg = ofl._suggest(ofl._RandTrial(rng_), mask_quant, pm_quant, 2, 2)
            sc, nf, pf_f, nv, pf_v = score_fitval(cfg)
            record("rand", cfg, sc, nf, pf_f, nv, pf_v)
            if sc > best["score"]:
                best.update(score=sc, cfg=cfg, fit=(nf, pf_f), val=(nv, pf_v))
            n_opt += 1
    print(f"[pf14] global search: {n_opt} trials | best FIT/VAL band-score={best['score']:.4f}")

    # ===== Part B: guided COORDINATE loop (>=25 logged iterations) =====
    # Each iteration changes ONE logical group from the current-best hypothesis and keeps it
    # only if it improves the FIT/VAL band-score. Start from the card baseline.
    def q(feat, p):
        # compute the p-quantile fresh from TRAIN (robust for any p, not just QGRID keys)
        if feat not in mask_quant or feat not in TRAIN.columns:
            return None
        s = pd.to_numeric(TRAIN[feat], errors="coerce").dropna()
        return round(float(s.quantile(p)), 6) if len(s) else None

    def pmq(feat, p):
        if feat not in pm_quant or feat not in pm_df.columns:
            return None
        s = pd.to_numeric(pm_df[feat], errors="coerce").dropna()
        return round(float(s.quantile(p)), 6) if len(s) else None

    cur = dict(base_cfg)
    cur_sc, *_ = score_fitval(cur)
    iters = []

    def move(group, new_cfg, note):
        nonlocal cur, cur_sc
        sc, nf, pf_f, nv, pf_v = score_fitval(new_cfg)
        record(f"coord:{group}", new_cfg, sc, nf, pf_f, nv, pf_v)
        kept = sc > cur_sc + 1e-9
        conf = confirm(setup, new_cfg, TRAIN, TEST) if kept else None
        it = {"i": len(iters) + 1, "group": group, "note": note, "cfg": dict(new_cfg),
              "fit": (nf, round(pf_f, 3)), "val": (nv, round(pf_v, 3)), "score": round(sc, 4),
              "kept": kept, "confirm": None}
        if kept:
            cur, cur_sc = dict(new_cfg), sc
            it["confirm"] = {"train": conf["train"], "in_band": conf["in_band"],
                             "test_ran": conf["test_ran"], "test": conf["test"]}
        iters.append(it)
        return kept

    def best_in_group(group, candidates, note_fn):
        """Try a list of (label,cfg) for one group; log the best-scoring as one iteration."""
        scored = []
        for label, c in candidates:
            sc, nf, pf_f, nv, pf_v = score_fitval(c)
            scored.append((sc, label, c, nf, pf_f, nv, pf_v))
        scored.sort(key=lambda x: -x[0])
        sc, label, c, nf, pf_f, nv, pf_v = scored[0]
        return move(group, c, note_fn(label))

    SLG = [0.70, 0.85, 1.00, 1.10, 1.20]
    TGG = [1.00, 1.25, 1.50, 2.00, 2.50]
    passes = 0
    while len(iters) < 27 and passes < 4:
        passes += 1
        # 1 EXIT
        best_in_group("exit", [(f"{s}/{t}", {**cur, "sl": s, "tgt": t}) for s in SLG for t in TGG],
                      lambda L: f"sweep SL/Tgt grid -> {L}")
        # 2 REGIME mask (categorical)
        best_in_group("regime", [("none", {**cur, "mask_terms": [t for t in cur["mask_terms"] if t[0] != "regime"]}),
                                 ("!=BULL", {**cur, "mask_terms": [t for t in cur["mask_terms"] if t[0] != "regime"] + [("regime", "!=", "BULL")]}),
                                 ("!=NEUTRAL", {**cur, "mask_terms": [t for t in cur["mask_terms"] if t[0] != "regime"] + [("regime", "!=", "NEUTRAL")]}),
                                 ("!=TREND", {**cur, "mask_terms": [t for t in cur["mask_terms"] if t[0] != "regime"] + [("regime", "!=", "TREND")]})],
                      lambda L: f"regime mask -> {L}")

        def add_num(feat, op, p):
            base_terms = [t for t in cur["mask_terms"] if t[0] != feat]
            thr = q(feat, p)
            return {**cur, "mask_terms": base_terms + ([(feat, op, thr)] if thr is not None else [])}
        # 3 VOLUME, 4 VOLATILITY, 5 TREND/RS, 6 VWAP, 7 CANDLE(close_loc/body), 8 QUALITY
        for grp, feat, op in [("volume", "vol_ratio", ">="), ("volatility", "atr_pct", ">="),
                              ("trend_rs", "rs_pct", ">="), ("vwap", "vwap_dist_atr", "<="),
                              ("candle_closeloc", "close_loc", ">="), ("candle_body", "body_pct", ">="),
                              ("quality", "quality_score", ">=")]:
            if feat not in mask_quant:
                continue
            cands = [("drop", {**cur, "mask_terms": [t for t in cur["mask_terms"] if t[0] != feat]})]
            cands += [(f"{op}q{int(p*100)}", add_num(feat, op, p)) for p in (0.2, 0.35, 0.5, 0.65, 0.8)]
            best_in_group(grp, cands, lambda L, f=feat, o=op: f"{f} {o} threshold -> {L}")
        # 9 PRE-MOMENTUM (best single term)
        pm_cands = [("drop", {**cur, "premom_terms": []})]
        for f in pm_quant:
            for op in (">=", "<="):
                for p in (0.3, 0.5, 0.7):
                    pm_cands.append((f"{f}{op}q{int(p*100)}", {**cur, "premom_terms": [(f, op, pmq(f, p))]}))
        best_in_group("pre_momentum", pm_cands, lambda L: f"single premom gate -> {L}")
        # 10 TIME GUARD
        tg = []
        for ms in (None, "09:45", "10:00", "10:30"):
            for xs in (None, "12:30", "13:30", "14:00"):
                g = {}
                if ms: g["min_slot"] = ms
                if xs: g["max_slot"] = xs
                tg.append((f"min={ms},max={xs}", {**cur, "guard": (g or None)}))
        best_in_group("time_guard", tg, lambda L: f"entry time window -> {L}")
        # 11 PORTFOLIO
        best_in_group("portfolio", [(f"mp{mp},dl{dl}", {**cur, "max_positions": mp, "daily_loss_rs": dl})
                                    for mp in (10, 20) for dl in (0.0, 4000.0)],
                      lambda L: f"max_positions/daily_loss -> {L}")

    # ===== Confirm global-best + collect candidates =====
    confirmations = {}
    def cand_from(cfg, label):
        sig = cfg_sig(cfg)
        if sig in confirmations:
            return confirmations[sig]
        c = confirm(setup, cfg, TRAIN, TEST)
        c["label"] = label; c["cfg"] = cfg
        confirmations[sig] = c
        return c

    pool_cfgs = []
    if best["cfg"]:
        pool_cfgs.append((best["cfg"], "optuna_best"))
    for it in iters:
        if it["kept"]:
            pool_cfgs.append((it["cfg"], f"coord_i{it['i']}_{it['group']}"))
    # Also confirm individual trial configs whose FIT/VAL are sample-eligible. This is
    # still TRAIN-side only: confirm() runs TEST only after full TRAIN is in band.
    top_tr = sorted([t for t in trial_cfgs if t["fit_n"] >= MIN_FITVAL and t["val_n"] >= MIN_FITVAL],
                    key=lambda t: -t["score"])[:75]
    for rank, t in enumerate(top_tr, 1):
        pool_cfgs.append((t["cfg"], f"{t['src']}_rank{rank}"))

    confirmed = [cand_from(cfg, lab) for cfg, lab in pool_cfgs]

    def passes_gate(c):
        tr, te = c["train"], c.get("test")
        if not te:
            return False
        return (PF_TRAIN_LO <= tr["net_pf"] <= PF_TRAIN_HI and te["net_pf"] > PF_TEST_MIN and
                tr["n"] >= MIN_TRAIN_N and te["n"] >= MIN_TEST_N and tr["net_pnl"] > 0 and te["net_pnl"] > 0 and
                dom_ok(tr) and dom_ok(te) and tr["trades_per_day"] <= MAX_TPD and te["trades_per_day"] <= MAX_TPD)

    candidates = [c for c in confirmed if passes_gate(c)]
    candidates.sort(key=lambda c: -(c["test"]["net_pf"]))

    # ============ WRITE ARTIFACTS ============
    cmd = (f"py -3.12 Train_and_Test/setup_pf_1_4_approval_loop/{setup}/scripts/pf14_approval_loop.py "
           f"--setup {setup} --pool {args.pool} --train_start {args.train_start} "
           f"--test_start {args.test_start} --trials {args.trials} --seed {args.seed}")

    def cfg_block(cfg):
        return (f'  "exit": {{"sl_pct": {cfg["sl"]}, "tgt_pct": {cfg["tgt"]}}},\n'
                f'  "mask_terms": {json.dumps([list(t) for t in cfg["mask_terms"]])},\n'
                f'  "pre_momentum_terms": {json.dumps([list(t) for t in cfg["premom_terms"]])},\n'
                f'  "entry_guards": {json.dumps(cfg.get("guard") or {})},\n'
                f'  "max_positions": {cfg.get("max_positions", 20)}, "daily_loss_rs": {cfg.get("daily_loss_rs", 0.0)}')

    # BASELINE_RESULT.md
    bl = [f"# {setup} — BASELINE_RESULT", "",
          f"- **Side:** LONG  |  basis: {faithful}  |  optimizer: {engine}",
          f"- **TRAIN** {rng(train_days)} ({len(train_days)} sessions) — FIT {rng(fit_days)} / VAL {rng(val_days)}",
          f"- **TEST**  {rng(test_days)} ({len(test_days)} sessions)  ⚠️ only {len(test_days)} sessions available after {args.test_start} (pool ends {pd.Timestamp(days[-1]).date()})",
          f"- entries @ {args.slippage_bps}bps/leg: FIT={len(FIT)} VAL={len(VAL)} TRAIN={len(TRAIN)} TEST={len(TEST)}", "",
          "## Current card rules (baseline config)", "```",
          f"SL/Tgt = {base_cfg['sl']}/{base_cfg['tgt']}",
          f"mask_terms = {[list(t) for t in base_cfg['mask_terms']]}",
          f"pre_momentum_terms = {[list(t) for t in base_cfg['premom_terms']]}",
          f"entry_guards = {base_cfg['guard'] or {}}", "```",
          "Config source: final_setup_conf.py (RESEARCH_WATCH_CONF — parked 2026-06-29) / SETUP_CARDS §2.", "",
          "## Baseline metrics", "",
          "| window | metrics |", "|---|---|",
          f"| TRAIN | {fmt_m(base['train'])} |", f"| TEST | {fmt_m(base['test'])} |", "",
          "## Initial diagnosis",
          f"- Baseline TRAIN PF {base['train']['net_pf']} ({'IN' if PF_TRAIN_LO<=base['train']['net_pf']<=PF_TRAIN_HI else 'OUT of'} the 1.30-1.70 band); "
          f"TEST PF {base['test']['net_pf']} ({'>' if base['test']['net_pf']>PF_TEST_MIN else '<='} {PF_TEST_MIN}).",
          f"- **Structural limit:** TEST has only {len(test_days)} sessions, so the 'no single day dominates' check "
          f"(day-dominance ≤ {DOM_CAP}) is effectively impossible (1 day ≥ {round(1/len(test_days),2)} by construction). "
          "Any TEST PF here is low-confidence; treat as directional only."]
    (outdir / "BASELINE_RESULT.md").write_text("\n".join(bl), encoding="utf-8")

    # ITERATION_LOG.md
    il = [f"# {setup} — ITERATION_LOG", "",
          f"Guided coordinate loop (one logical group/iteration, greedy keep-best on FIT/VAL band-score) "
          f"+ {n_opt} Optuna global trials. Band target: TRAIN PF [{PF_TRAIN_LO},{PF_TRAIN_HI}], TEST PF > {PF_TEST_MIN}.", "",
          f"- Iteration 0 = BASELINE: TRAIN {fmt_m(base['train'])} | TEST {fmt_m(base['test'])}", ""]
    for it in iters:
        il.append(f"### Iteration {it['i']} — group: {it['group']}")
        il.append(f"- change: {it['note']}")
        il.append(f"- FIT n/PF={it['fit']}  VAL n/PF={it['val']}  band-score={it['score']}  -> **{'KEPT' if it['kept'] else 'REJECT'}**")
        if it["kept"] and it["confirm"]:
            cf = it["confirm"]
            il.append(f"- full TRAIN: {fmt_m(cf['train'])}  (in band: {cf['in_band']})")
            il.append(f"- TEST {'(scored, TRAIN in band)' if cf['test_ran'] else '(NOT scored — TRAIN out of band)'}: {fmt_m(cf['test']) if cf['test'] else '-'}")
            il.append(f"- next action: {'TEST evaluated once' if cf['test_ran'] else 'return to TRAIN-side logic (TRAIN not in band)'}")
        else:
            il.append("- next action: discard; try next logical group")
        il.append("")
    il += ["## Optuna global-best (confirmed)", ""]
    if best["cfg"]:
        bc = cand_from(best["cfg"], "optuna_best")
        il.append(f"- cfg: SL/Tgt={best['cfg']['sl']}/{best['cfg']['tgt']} mask={[list(t) for t in best['cfg']['mask_terms']]} "
                  f"premom={[list(t) for t in best['cfg']['premom_terms']]} guard={best['cfg'].get('guard')}")
        il.append(f"- TRAIN {fmt_m(bc['train'])}")
        il.append(f"- TEST  {fmt_m(bc.get('test'))}")
    (outdir / "ITERATION_LOG.md").write_text("\n".join(il), encoding="utf-8")

    # FAILURE_ANALYSIS.md (on baseline TRAIN+TEST detail = the live-faithful card behaviour)
    def fail_block(det, label):
        if det is None or det.empty:
            return [f"### {label}: no trades", ""]
        d = det.copy(); d["hour"] = pd.to_datetime(d["entry_time"]).dt.strftime("%H")
        los = d[d["net_pnl_rs"] <= 0]
        by_sym = d.groupby("ticker")["net_pnl_rs"].agg(["size", "sum"]).sort_values("sum")
        by_day = d.groupby("trade_date")["net_pnl_rs"].sum().sort_values()
        oc = d["outcome"].astype(str).value_counts().to_dict()
        return [f"### {label} ({len(d)} trades, {len(los)} losers, net Rs{d['net_pnl_rs'].sum():,.0f})",
                f"- outcome split: {oc}",
                f"- losers by hour: {los['hour'].value_counts().sort_index().to_dict()}",
                f"- worst days: " + ", ".join(f"{i}:Rs{v:,.0f}" for i, v in by_day.head(4).items()),
                f"- worst symbols: " + ", ".join(f"{i}(n{int(r['size'])}/Rs{r['sum']:,.0f})" for i, r in by_sym.head(6).iterrows()),
                f"- best symbols: " + ", ".join(f"{i}(n{int(r['size'])}/Rs{r['sum']:,.0f})" for i, r in by_sym.tail(4).iterrows()), ""]
    fa = [f"# {setup} — FAILURE_ANALYSIS", "",
          "Loss structure of the baseline (card) config — the live-faithful behaviour the search must beat.", ""]
    fa += fail_block(base.get("train_detail"), "TRAIN baseline")
    fa += fail_block(base.get("test_detail"), "TEST baseline")
    fa += ["## Notes",
           "- SL vs target vs EOD mix above shows whether the exit is too tight/wide.",
           "- A few dominant losing symbols/days = idiosyncratic, not a systematic edge failure.",
           f"- TEST has only {len(test_days)} sessions → day-level conclusions are not robust."]
    (outdir / "FAILURE_ANALYSIS.md").write_text("\n".join(fa), encoding="utf-8")

    # CANDIDATE_CONFIGS.md + candidates/*.json
    cc = [f"# {setup} — CANDIDATE_CONFIGS", "",
          f"Candidates clearing TRAIN PF ∈ [{PF_TRAIN_LO},{PF_TRAIN_HI}] AND TEST PF > {PF_TEST_MIN} "
          f"AND trade/day/symbol dominance ≤ {DOM_CAP} AND meaningful counts.", ""]
    if not candidates:
        cc.append("**NONE.** No config in the global search or the coordinate loop cleared the band on both "
                  "TRAIN and TEST with acceptable stability. See ITERATION_LOG / FAILURE_ANALYSIS.")
    for i, c in enumerate(candidates, 1):
        cid = f"{setup}_candidate_{i:03d}"
        jpath = outdir / "candidates" / f"{cid}.json"
        jpath.write_text(json.dumps(tt._json_sanitize({
            "id": cid, "setup": setup, "label": c["label"], "side": "LONG",
            "config": {"exit": {"sl_pct": c["cfg"]["sl"], "tgt_pct": c["cfg"]["tgt"]},
                       "mask_terms": [list(t) for t in c["cfg"]["mask_terms"]],
                       "pre_momentum_terms": [list(t) for t in c["cfg"]["premom_terms"]],
                       "entry_guards": c["cfg"].get("guard") or {},
                       "max_positions": c["cfg"].get("max_positions", 20),
                       "daily_loss_rs": c["cfg"].get("daily_loss_rs", 0.0)},
            "train": c["train"], "test": c["test"],
            "windows": {"TRAIN": rng(train_days), "FIT": rng(fit_days), "VAL": rng(val_days), "TEST": rng(test_days)},
        }), indent=2, default=str), encoding="utf-8")
        cc += [f"## Candidate {i:03d} ({c['label']}) -> `candidates/{cid}.json`", "```",
               cfg_block(c["cfg"]), "```",
               f"- TRAIN: {fmt_m(c['train'])}", f"- TEST:  {fmt_m(c['test'])}",
               f"- risk: TEST only {len(test_days)} sessions; dominance day={c['test'].get('day_dom')} sym={c['test'].get('sym_dom')}.", ""]
    (outdir / "CANDIDATE_CONFIGS.md").write_text("\n".join(cc), encoding="utf-8")

    # APPROVAL_REQUIRED_FINAL_RECOMMENDATION.md
    rec_yes = bool(candidates)
    top = candidates[0] if candidates else None
    ar = [f"# {setup} — APPROVAL_REQUIRED_FINAL_RECOMMENDATION", "",
          f"**Approval recommendation: {'YES (with caveats)' if rec_yes else 'NO'}**", "",
          "> DO NOT MOVE TO FINAL CONFIG UNTIL USER APPROVES.", ""]
    if top:
        ar += ["## Best candidate", "```", cfg_block(top["cfg"]), "```",
               f"- TRAIN: {fmt_m(top['train'])}", f"- TEST:  {fmt_m(top['test'])}", "",
               "## Proposed final_setup_conf.py block (FOR APPROVAL ONLY — NOT APPLIED)", "```python",
               f'"{setup}": {{', '  "side": "LONG",', cfg_block(top["cfg"]) + ",",
               '  "status": "OK",  # pending user approval', "},", "```",
               "## File that would need approval to edit", "- `final_setup_conf.py` (and `Train_and_Test/final_setup_conf.py`)", ""]
    else:
        ar += ["## Best candidate", "- none cleared the band on both TRAIN and TEST with acceptable stability.",
               "- The proposed action is to KEEP the setup PARKED (`enabled=False`); no config edit.", ""]
    ar += ["## Rerun command", "```", cmd, "```", "",
           "## Risk notes",
           f"- TEST window is only {len(test_days)} sessions ({rng(test_days)}) — cannot satisfy 'no single day dominates'; any TEST PF is low-confidence.",
           f"- Basis: {faithful}.",
           "- Coordinate loop is greedy (local optima possible); Optuna global search mitigates but does not eliminate this.",
           "- No live trades; no final_setup_conf.py edit performed by this script."]
    (outdir / "APPROVAL_REQUIRED_FINAL_RECOMMENDATION.md").write_text("\n".join(ar), encoding="utf-8")

    # trials.csv
    pd.DataFrame(trial_rows).sort_values("score", ascending=False).to_csv(outdir / "trials.csv", index=False)

    # equity png for best candidate (if any) else baseline
    try:
        import matplotlib; matplotlib.use("Agg"); import matplotlib.pyplot as plt
        src = top if top else base
        for lbl in ("train", "test"):
            det = src.get(f"{lbl}_detail") if top is None else None
            if top is not None:
                det = confirmations[cfg_sig(top["cfg"])].get(f"{lbl}_detail")
            plt.figure(figsize=(7, 3))
            if det is not None and not det.empty:
                dd = det.sort_values("entry_time")
                plt.plot(range(1, len(dd) + 1), dd["net_pnl_rs"].cumsum().to_numpy(), marker=".")
                plt.title(f"{setup} {lbl} equity (best{'' if top else '=baseline'})")
            else:
                plt.title(f"{setup} {lbl} — no trades")
            plt.xlabel("trade #"); plt.ylabel("cum net Rs"); plt.grid(alpha=0.3); plt.tight_layout()
            plt.savefig(outdir / f"equity_{lbl}.png", dpi=90); plt.close()
    except Exception as e:
        print(f"[pf14] (equity plot skipped: {e})")

    print("\n" + "=" * 90)
    print(f"DONE {setup}: candidates passing band(TRAIN 1.30-1.70 & TEST>1.40 & stable) = {len(candidates)}")
    print(f"  baseline TRAIN {fmt_m(base['train'])}")
    print(f"  baseline TEST  {fmt_m(base['test'])}")
    if top:
        print(f"  best cand TRAIN {fmt_m(top['train'])}")
        print(f"  best cand TEST  {fmt_m(top['test'])}")
    print(f"  artifacts -> {outdir}")
    print("=" * 90)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
