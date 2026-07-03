r"""campaign_amccb.py — staged optimization campaign for A_MOD_CLOSE_CONTINUATION_BREAK.

RESEARCH ONLY: no live trades, no order placement, never writes final_setup_conf.py.

Stages (all in ONE process so the lru caches for 1-min bars / entries / resolves /
premom stay warm across the whole campaign):

  stage2  : failure study of the baseline book on TRAIN  -> failure_study.json
  stage3  : one-knob-at-a-time sweeps on FIT/VAL         -> sweeps.json
  stage4  : combination search (Optuna TPE, band objective on FIT/VAL,
            + greedy stacking of stable Stage-3 winners)  -> trials.csv, combos.json
  stage5  : full-TRAIN confirmation of top distinct configs; TEST scored ONCE per
            qualifying config (budget-capped)              -> confirmations.json
  stage6  : rescue loop if nothing passes                  -> rescue.json

Anti-overfit protocol:
  * search/quantiles use TRAIN only (never TEST);
  * optimize on FIT (first 60% of TRAIN sessions) + VAL (last 40%), band objective
    reward(min(FIT_PF,VAL_PF)) - lambda*|FIT_PF-VAL_PF| tenting at PF 1.70;
  * TEST is scored once per confirmed candidate, budget-capped (default 10);
  * domination caps: top trade <=35% gross profit, top day <=40% net, top symbol <=40% net;
  * neighborhood + term-dropout robustness via pf_band_fitval_loop.robustness_report.

Usage:
  py -3.12 campaign_amccb.py --stages 2,3,4,5,6 [--trials 500] [--seed 7]
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

HERE = Path(__file__).resolve()
WORK = HERE.parents[1]
TT_DIR = HERE.parents[3]          # Train_and_Test
REPO = TT_DIR.parent
ENGINE_DIR = TT_DIR / "setup_pf_1_4_approval_loop" / "_engine"
for p in (REPO, TT_DIR, ENGINE_DIR):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))

import setup_train_test as tt  # noqa: E402
import pf_band_fitval_loop as eng  # noqa: E402

try:
    import optuna
    optuna.logging.set_verbosity(optuna.logging.WARNING)
    HAVE_OPTUNA = True
except Exception:
    HAVE_OPTUNA = False

SETUP = "A_MOD_CLOSE_CONTINUATION_BREAK"
SIDE = "LONG"
TRAIN_START = pd.Timestamp("2026-03-01")
TRAIN_END = pd.Timestamp("2026-05-30")
TEST_START = pd.Timestamp("2026-06-01")
TEST_END = pd.Timestamp("2026-07-01")   # 07-02 excluded: 1-min data truncated ~09:30, exits unresolvable
FIT_FRAC = 0.60

BASE_SL, BASE_TGT = 0.70, 1.50           # production v6 exit rule for this setup

PF_LO, PF_HI = 1.30, 1.80                # user's TRAIN band (accept)
TENT_HI = 1.70                           # objective tents here (conservative, inside band)
TEST_PF_MIN = 1.40                       # user's TEST gate
GAP_LAMBDA = 0.80
FV_FLOOR = 6                             # min FIT and VAL trades for a scored config
DOM_TRADE, DOM_DAY, DOM_SYM = 0.35, 0.40, 0.40

SL_GRID = [0.40, 0.50, 0.60, 0.70, 0.85, 1.00, 1.20]
TGT_GRID = [0.60, 0.80, 1.00, 1.25, 1.50, 2.00, 2.50]
MIN_SLOTS = ["09:30", "09:45", "10:00", "10:30", "11:00"]
MAX_SLOTS = ["11:00", "12:00", "13:00", "14:00", "14:30"]
TOPN_GRID = [1, 2, 3]
QGRID = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9]

# mask features actually populated for this setup's raw rows (verified >99% non-null),
# plus the derived columns load_pool adds. market_ret/notional/signal_minute are
# EXCLUDED from the mask search (repo doctrine: dominant overfit vectors; time-of-day
# is expressed through min_slot/max_slot guards instead).
MASK_FEATS = ["rs_pct", "vol_ratio", "atr_pct", "body_pct", "close_loc",
              "vwap_dist_atr", "quality_score", "signal_range_pct",
              "upper_wick_pct", "lower_wick_pct", "wick_skew_pct"]
PM_FEATS = ["pre_entry_momentum_score", "sig5_adx_calc", "sig5_rsi_dir",
            "sig5_vol_ratio20", "pre1_adx", "pre3_range_r", "pre5_mom_r", "pre3_close_pos"]
PM_SL_FOR_QUANT = 0.70   # premom features depend on the stop; quantile grid fixed at baseline SL

STATE: dict = {}          # windows + dataframes, filled by prepare()
ITER_LOG: list[dict] = [] # every scored hypothesis
TEST_EVALS = 0            # global TEST budget counter


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------
def _clamp_pf(pf):
    return 10.0 if not np.isfinite(pf) else min(float(pf), 10.0)


def band_reward(pf):
    if pf <= TENT_HI:
        return pf
    return TENT_HI - 1.5 * (pf - TENT_HI)


def mk_cfg(sl=BASE_SL, tgt=BASE_TGT, mask=None, premom=None, guard=None,
           max_positions=20, daily_loss_rs=0.0):
    return {"sl": float(sl), "tgt": float(tgt),
            "mask_terms": [tuple(t) for t in (mask or [])],
            "premom_terms": [tuple(t) for t in (premom or [])],
            "guard": (dict(guard) if guard else None), "status": "OK",
            "max_positions": int(max_positions), "daily_loss_rs": float(daily_loss_rs)}


def cfg_key(cfg):
    return json.dumps({"sl": cfg["sl"], "tgt": cfg["tgt"],
                       "mask": sorted(map(list, cfg["mask_terms"])),
                       "pm": sorted(map(list, cfg["premom_terms"])),
                       "guard": cfg["guard"], "mp": cfg["max_positions"],
                       "dl": cfg["daily_loss_rs"]}, sort_keys=True)


def cfg_str(cfg):
    m = ";".join(f"{a}{o}{b}" for a, o, b in cfg["mask_terms"]) or "-"
    p = ";".join(f"{a}{o}{b}" for a, o, b in cfg["premom_terms"]) or "-"
    g = json.dumps(cfg["guard"]) if cfg["guard"] else "-"
    return f"SL{cfg['sl']}/T{cfg['tgt']} mask[{m}] pm[{p}] g{g}"


def fast_eval(cfg, df):
    tt.MAX_POSITIONS = cfg.get("max_positions", 20)
    tt.DAILY_LOSS_RS = cfg.get("daily_loss_rs", 0.0)
    fam = tt.eval_family({SETUP: cfg}, df)
    return int(fam["trades"]), float(fam["net_pf"]), float(fam["net_pnl"])


def full_eval(cfg, df):
    tt.MAX_POSITIONS = cfg.get("max_positions", 20)
    tt.DAILY_LOSS_RS = cfg.get("daily_loss_rs", 0.0)
    return eng.full_metrics(SETUP, cfg, df)


def score_cfg(cfg):
    """Band objective on FIT/VAL (same shape as pf_band_fitval_loop.score_cfg)."""
    nf, pf_f, _ = fast_eval(cfg, STATE["FIT"])
    nv, pf_v, _ = fast_eval(cfg, STATE["VAL"])
    if nf < FV_FLOOR or nv < FV_FLOOR:
        return -5.0 + min(nf, nv) / max(1, FV_FLOOR), nf, pf_f, nv, pf_v
    cf, cv = _clamp_pf(pf_f), _clamp_pf(pf_v)
    sc = band_reward(min(cf, cv)) - GAP_LAMBDA * abs(cf - cv)
    if min(cf, cv) >= PF_LO:
        sc += 0.003 * min(min(nf, nv), 40)
    return sc, nf, pf_f, nv, pf_v


def log_iter(group, change, old, new, reason, fitm, valm, trainm=None, testm=None,
             decision="", fail_class="", next_action=""):
    ITER_LOG.append({
        "iter": len(ITER_LOG) + 1, "group": group, "change": change,
        "old": str(old), "new": str(new), "reason": reason,
        "fit": fitm, "val": valm, "train": trainm, "test": testm,
        "decision": decision, "failure_class": fail_class, "next_action": next_action,
    })


def fv_pack(n, pf, pnl):
    return {"n": int(n), "pf": round(float(pf), 3), "net": round(float(pnl), 0)}


def dom_ok(m):
    return (m["trade_dom_gross"] is not None and m["trade_dom_gross"] <= DOM_TRADE
            and m["day_dom"] is not None and m["day_dom"] <= DOM_DAY
            and m["sym_dom"] is not None and m["sym_dom"] <= DOM_SYM)


def save_json(name, obj):
    (WORK / name).write_text(json.dumps(obj, indent=2, default=str), encoding="utf-8")
    print(f"[campaign] wrote {WORK / name}", flush=True)


# ---------------------------------------------------------------------------
# data prep
# ---------------------------------------------------------------------------
def prepare(pool_dir: Path):
    tt.POOL_DIRS = [str(pool_dir)]
    tt.SLIPPAGE_BPS = 15.0
    tt.MAX_POSITIONS = 20
    tt.DAILY_LOSS_RS = 0.0
    tt._entry.cache_clear(); tt._resolve_full.cache_clear(); tt._premom.cache_clear()

    pool = tt.load_pool()
    pool = pool[pool["setup"].astype(str).str.upper().eq(SETUP)].copy()
    train_raw = pool[(pool["_day"] >= TRAIN_START) & (pool["_day"] <= TRAIN_END)].reset_index(drop=True)
    test_raw = pool[(pool["_day"] >= TEST_START) & (pool["_day"] <= TEST_END)].reset_index(drop=True)
    t0 = time.time()
    train = tt.attach_entries(train_raw)
    test = tt.attach_entries(test_raw)
    print(f"[campaign] attach_entries: train {len(train_raw)}->{len(train)} "
          f"test {len(test_raw)}->{len(test)} in {time.time()-t0:.0f}s", flush=True)

    tr_sessions = sorted(train["_day"].dt.strftime("%Y-%m-%d").unique())
    te_sessions = sorted(test["_day"].dt.strftime("%Y-%m-%d").unique())
    n_fit = max(1, int(round(FIT_FRAC * len(tr_sessions))))
    fit_s, val_s = tr_sessions[:n_fit], tr_sessions[n_fit:]
    fit = train[train["_day"].dt.strftime("%Y-%m-%d").isin(fit_s)].reset_index(drop=True)
    val = train[train["_day"].dt.strftime("%Y-%m-%d").isin(val_s)].reset_index(drop=True)

    # TRAIN-only quantile grids
    mask_quant = {}
    for f in MASK_FEATS:
        if f in train.columns:
            s = pd.to_numeric(train[f], errors="coerce").dropna()
            if len(s) >= 8 and s.nunique() > 1:
                mask_quant[f] = {q: float(s.quantile(q)) for q in QGRID}

    STATE.update({"pool": pool, "TRAIN": train, "TEST": test, "FIT": fit, "VAL": val,
                  "fit_sessions": fit_s, "val_sessions": val_s,
                  "train_sessions": tr_sessions, "test_sessions": te_sessions,
                  "mask_quant": mask_quant})
    print(f"[campaign] FIT {fit_s[0]}..{fit_s[-1]} ({len(fit_s)}) n={len(fit)} | "
          f"VAL {val_s[0]}..{val_s[-1]} ({len(val_s)}) n={len(val)} | "
          f"TEST {te_sessions[0]}..{te_sessions[-1]} ({len(te_sessions)}) n={len(test)}", flush=True)


def pm_quantiles(sample_n=1500, seed=7):
    """Premom feature quantile grid from a TRAIN sample (features at PM_SL_FOR_QUANT)."""
    train = STATE["TRAIN"]
    src = train.sample(n=min(sample_n, len(train)), random_state=seed).sort_index()
    recs = []
    for j, r in enumerate(src.itertuples(), 1):
        feats, reason = tt._premom(r.ticker, r.side, r.tt_entry_iso, float(r.tt_fill),
                                   PM_SL_FOR_QUANT, r.tt_sig_ts.isoformat())
        fd = dict(feats) if not reason else {}
        recs.append({f: fd.get(f, np.nan) for f in PM_FEATS})
        if j % 500 == 0:
            print(f"[campaign] premom quantiles {j}/{len(src)}", flush=True)
    pm_df = pd.DataFrame(recs)
    out = {}
    for f in PM_FEATS:
        s = pd.to_numeric(pm_df[f], errors="coerce").dropna()
        if len(s) >= 8 and s.nunique() > 1:
            out[f] = {q: float(s.quantile(q)) for q in QGRID}
    STATE["pm_quant"] = out
    print(f"[campaign] premom quantile feats: {sorted(out)}", flush=True)


# ---------------------------------------------------------------------------
# stage 2 — failure study (baseline book on TRAIN)
# ---------------------------------------------------------------------------
def stage2():
    cfg = mk_cfg()
    m = full_eval(cfg, STATE["TRAIN"])
    det = m["detail"]
    out = {"baseline_cfg": {k: v for k, v in cfg.items() if k != "status"},
           "train_metrics": {k: v for k, v in m.items() if k != "detail"}}
    if det.empty:
        out["note"] = "no baseline book"
        save_json("failure_study.json", out)
        return
    det = det.copy()
    det["hour"] = pd.to_datetime(det["signal_time"]).dt.hour + pd.to_datetime(det["signal_time"]).dt.minute / 60.0
    det["hh"] = pd.cut(det["hour"], bins=[9.0, 10.0, 11.0, 12.0, 13.0, 14.0, 15.5],
                       labels=["09-10", "10-11", "11-12", "12-13", "13-14", "14+"])
    # join signal features back for loser classification
    key = ["ticker", "side"]
    feat_cols = ["rs_pct", "vol_ratio", "atr_pct", "body_pct", "close_loc", "vwap_dist_atr",
                 "quality_score", "signal_range_pct", "upper_wick_pct", "regime"]
    src = STATE["TRAIN"]
    fmap = src.set_index(["ticker", "tt_sig_ts"])
    feats = []
    for r in det.itertuples():
        try:
            row = fmap.loc[(r.ticker, pd.Timestamp(r.signal_time))]
            if isinstance(row, pd.DataFrame):
                row = row.iloc[0]
            feats.append({c: row.get(c) for c in feat_cols})
        except Exception:
            feats.append({c: None for c in feat_cols})
    det = pd.concat([det.reset_index(drop=True), pd.DataFrame(feats)], axis=1)

    losers = det[det["net_pnl_rs"] < 0]
    winners = det[det["net_pnl_rs"] > 0]

    def seg(dframe, col):
        g = dframe.groupby(col, observed=True)["net_pnl_rs"]
        return {str(k): {"n": int(len(v)), "net": round(float(v.sum()), 0),
                         "pf": round(eng._clamp_pf(tt._pf(v.to_numpy())), 3)}
                for k, v in g}

    out.update({
        "by_hour": seg(det, "hh"),
        "by_outcome": det["outcome"].astype(str).value_counts().to_dict(),
        "by_regime": seg(det, "regime"),
        "loser_medians": {c: (round(float(pd.to_numeric(losers[c], errors="coerce").median()), 4)
                              if c != "regime" else None) for c in feat_cols},
        "winner_medians": {c: (round(float(pd.to_numeric(winners[c], errors="coerce").median()), 4)
                               if c != "regime" else None) for c in feat_cols},
        "worst_days": {str(k): v for k, v in det.groupby("trade_date")["net_pnl_rs"].sum()
                       .nsmallest(8).round(0).items()},
        "best_days": {str(k): v for k, v in det.groupby("trade_date")["net_pnl_rs"].sum()
                      .nlargest(5).round(0).items()},
        "worst_symbols": {str(k): v for k, v in det.groupby("ticker")["net_pnl_rs"].sum()
                          .nsmallest(8).round(0).items()},
        "worst_trades": det.nsmallest(10, "net_pnl_rs")[
            ["trade_date", "ticker", "outcome", "bars_held", "net_pnl_rs"]].to_dict("records"),
        "avg_bars_held_by_outcome": det.groupby("outcome")["bars_held"].mean().round(1).to_dict(),
        "n_losers": int(len(losers)), "n_winners": int(len(winners)),
    })
    det.to_csv(WORK / "baseline_train_detail.csv", index=False)
    save_json("failure_study.json", out)


# ---------------------------------------------------------------------------
# stage 3 — one-knob sweeps on FIT/VAL
# ---------------------------------------------------------------------------
def sweep_one(group, label, cfg, old, new, reason):
    sc, nf, pf_f, nv, pf_v = score_cfg(cfg)
    rec = {"group": group, "label": label, "score": round(float(sc), 4),
           "fit_n": nf, "fit_pf": round(pf_f, 3), "val_n": nv, "val_pf": round(pf_v, 3)}
    both_pos = pf_f >= 1.0 and pf_v >= 1.0 and nf >= FV_FLOOR and nv >= FV_FLOOR
    decision = "keep-for-combos" if (both_pos and min(pf_f, pf_v) >= 1.05) else "reject"
    log_iter(group, label, old, new, reason,
             fv_pack(nf, pf_f, 0), fv_pack(nv, pf_v, 0),
             decision=decision,
             fail_class=("" if decision.startswith("keep") else "weak FIT/VAL PF or too few trades"),
             next_action="stage4 combos" if decision.startswith("keep") else "drop")
    rec["decision"] = decision
    return rec


def stage3():
    res = {"exits": [], "mask": [], "premom": [], "guards": [], "regime": [], "overlay": []}

    # exits (SL x TGT grid)
    for sl in SL_GRID:
        for tgt in TGT_GRID:
            res["exits"].append(sweep_one(
                "exit", f"SL{sl}/T{tgt}", mk_cfg(sl=sl, tgt=tgt),
                f"SL{BASE_SL}/T{BASE_TGT}", f"SL{sl}/T{tgt}",
                "exit grid sweep"))
        print(f"[campaign] exit sweep SL={sl} done", flush=True)

    # single mask terms at baseline exit
    for f, qs in STATE["mask_quant"].items():
        for q, thr in qs.items():
            for op in (">=", "<="):
                res["mask"].append(sweep_one(
                    "mask", f"{f}{op}{round(thr,5)}(q{q})",
                    mk_cfg(mask=[(f, op, round(thr, 6))]),
                    "no-mask", f"{f}{op}{round(thr,5)}", "single indicator/price-action mask"))
        print(f"[campaign] mask sweep {f} done", flush=True)

    # regime categorical
    for lab, term in [("regime!=BEAR", ("regime", "!=", "BEAR")),
                      ("regime==BULL", ("regime", "==", "BULL")),
                      ("regime!=BULL", ("regime", "!=", "BULL"))]:
        res["regime"].append(sweep_one("regime", lab, mk_cfg(mask=[term]),
                                       "no-regime", lab, "categorical regime mask"))

    # premom single terms at baseline exit
    for f, qs in STATE.get("pm_quant", {}).items():
        for q, thr in qs.items():
            for op in (">=", "<="):
                res["premom"].append(sweep_one(
                    "premom", f"{f}{op}{round(thr,5)}(q{q})",
                    mk_cfg(premom=[(f, op, round(thr, 6))]),
                    "no-premom", f"{f}{op}{round(thr,5)}", "single pre-momentum gate"))
        print(f"[campaign] premom sweep {f} done", flush=True)

    # guards
    for ms in MIN_SLOTS:
        res["guards"].append(sweep_one("guard", f"min_slot={ms}", mk_cfg(guard={"min_slot": ms}),
                                       "none", ms, "avoid early traps"))
    for ms in MAX_SLOTS:
        res["guards"].append(sweep_one("guard", f"max_slot={ms}", mk_cfg(guard={"max_slot": ms}),
                                       "none", ms, "avoid late-day churn"))
    for tn in TOPN_GRID:
        res["guards"].append(sweep_one("guard", f"top_n={tn}", mk_cfg(guard={"top_n": tn}),
                                       "none", tn, "cap per-slot signal count"))
    for mp in (10, 20):
        res["guards"].append(sweep_one("guard", f"max_positions={mp}", mk_cfg(max_positions=mp),
                                       20, mp, "portfolio cap"))
    for dl in (0.0, 4000.0):
        res["guards"].append(sweep_one("guard", f"daily_loss_rs={dl}", mk_cfg(daily_loss_rs=dl),
                                       0.0, dl, "daily kill-switch"))

    save_json("sweeps.json", res)
    return res


# ---------------------------------------------------------------------------
# stage 4 — combination search
# ---------------------------------------------------------------------------
def _suggest(trial, max_mask, max_pm):
    mq, pq = STATE["mask_quant"], STATE.get("pm_quant", {})
    def cat(n, ch): return trial.suggest_categorical(n, ch)
    mask_terms = []
    for i in range(trial.suggest_int("n_mask", 0, max_mask)):
        f = cat(f"mask{i}_feat", sorted(mq))
        op = cat(f"mask{i}_op", [">=", "<="]); q = cat(f"mask{i}_q", QGRID)
        mask_terms.append((f, op, round(float(mq[f][q]), 6)))
    pm_terms = []
    for i in range(trial.suggest_int("n_pm", 0, max_pm)):
        f = cat(f"pm{i}_feat", sorted(pq))
        op = cat(f"pm{i}_op", [">=", "<="]); q = cat(f"pm{i}_q", QGRID)
        pm_terms.append((f, op, round(float(pq[f][q]), 6)))
    guard = {}
    if cat("use_min_slot", [False, True]):
        guard["min_slot"] = cat("min_slot", MIN_SLOTS)
    if cat("use_max_slot", [False, True]):
        guard["max_slot"] = cat("max_slot", MAX_SLOTS)
    tn = cat("top_n", [0, 1, 2, 3])
    if tn:
        guard["top_n"] = int(tn)
    return mk_cfg(sl=cat("sl", SL_GRID), tgt=cat("tgt", TGT_GRID),
                  mask=mask_terms, premom=pm_terms, guard=(guard or None),
                  max_positions=cat("max_positions", [10, 20]),
                  daily_loss_rs=cat("daily_loss_rs", [0.0, 4000.0]))


class _RandTrial:
    def __init__(self, rng): self.rng = rng
    def suggest_categorical(self, n, ch): return ch[self.rng.randrange(len(ch))]
    def suggest_int(self, n, lo, hi): return self.rng.randint(lo, hi)


def stage4(sweeps, trials=500, seed=7, time_budget_min=45.0, max_mask=2, max_pm=1):
    trial_rows = []
    best_pool: dict[str, dict] = {}   # cfg_key -> {"score","cfg",...}

    def consider(cfg, tag):
        sc, nf, pf_f, nv, pf_v = score_cfg(cfg)
        row = {"tag": tag, "sl": cfg["sl"], "tgt": cfg["tgt"],
               "mask": ";".join(f"{a}{o}{b}" for a, o, b in cfg["mask_terms"]) or "-",
               "premom": ";".join(f"{a}{o}{b}" for a, o, b in cfg["premom_terms"]) or "-",
               "guard": json.dumps(cfg["guard"]) if cfg["guard"] else "-",
               "max_positions": cfg["max_positions"], "daily_loss_rs": cfg["daily_loss_rs"],
               "fit_n": nf, "fit_pf": round(pf_f, 3), "val_n": nv, "val_pf": round(pf_v, 3),
               "score": round(float(sc), 4)}
        trial_rows.append(row)
        k = cfg_key(cfg)
        if k not in best_pool or sc > best_pool[k]["score"]:
            best_pool[k] = {"score": float(sc), "cfg": cfg, "fit_pf": pf_f, "val_pf": pf_v,
                            "fit_n": nf, "val_n": nv, "tag": tag}
        return sc, nf, pf_f, nv, pf_v

    # 4a. greedy stacking of stable Stage-3 winners: take each group's top keeps,
    # add one group at a time onto the best exit.
    def top_keeps(group, k):
        rows = [r for r in sweeps[group] if r["decision"].startswith("keep")]
        rows.sort(key=lambda r: -r["score"])
        return rows[:k]

    exit_rows = sorted(sweeps["exits"], key=lambda r: -r["score"])[:5]
    combo_iters = 0
    for er in exit_rows:
        sl, tgt = er["label"].replace("SL", "").split("/T")
        base = mk_cfg(sl=float(sl), tgt=float(tgt))
        base_sc, *_ = consider(base, "greedy_base")
        cur, cur_sc = base, base_sc
        for group, field in (("mask", "mask"), ("premom", "premom"), ("guards", "guard"),
                             ("regime", "mask")):
            improved = None
            for cand in top_keeps(group, 6):
                cfg2 = json.loads(json.dumps({k: v for k, v in cur.items()}))
                cfg2 = mk_cfg(sl=cur["sl"], tgt=cur["tgt"], mask=cur["mask_terms"],
                              premom=cur["premom_terms"], guard=cur["guard"],
                              max_positions=cur["max_positions"], daily_loss_rs=cur["daily_loss_rs"])
                lab = cand["label"]
                if group in ("mask", "regime"):
                    if len(cfg2["mask_terms"]) >= 2:
                        continue
                    if group == "regime":
                        term = {"regime!=BEAR": ("regime", "!=", "BEAR"),
                                "regime==BULL": ("regime", "==", "BULL"),
                                "regime!=BULL": ("regime", "!=", "BULL")}[lab]
                    else:
                        f = lab.split(">=")[0].split("<=")[0]
                        op = ">=" if ">=" in lab else "<="
                        thr = float(lab.split(op)[1].split("(")[0])
                        if any(t[0] == f for t in cfg2["mask_terms"]):
                            continue
                        term = (f, op, thr)
                    cfg2["mask_terms"] = list(cfg2["mask_terms"]) + [term]
                elif group == "premom":
                    if cfg2["premom_terms"]:
                        continue
                    f = lab.split(">=")[0].split("<=")[0]
                    op = ">=" if ">=" in lab else "<="
                    thr = float(lab.split(op)[1].split("(")[0])
                    cfg2["premom_terms"] = [(f, op, thr)]
                else:  # guards
                    g = dict(cfg2["guard"] or {})
                    if lab.startswith("min_slot"):
                        g["min_slot"] = lab.split("=")[1]
                    elif lab.startswith("max_slot"):
                        g["max_slot"] = lab.split("=")[1]
                    elif lab.startswith("top_n"):
                        g["top_n"] = int(lab.split("=")[1])
                    elif lab.startswith("max_positions"):
                        cfg2["max_positions"] = int(lab.split("=")[1])
                    elif lab.startswith("daily_loss"):
                        cfg2["daily_loss_rs"] = float(lab.split("=")[1])
                    cfg2["guard"] = g or None
                sc2, nf2, pf_f2, nv2, pf_v2 = consider(cfg2, f"greedy+{group}")
                combo_iters += 1
                log_iter("combo-greedy", f"add {lab} to [{cfg_str(cur)}]",
                         cfg_str(cur), cfg_str(cfg2), f"stack stable {group} winner",
                         fv_pack(nf2, pf_f2, 0), fv_pack(nv2, pf_v2, 0),
                         decision=("keep" if sc2 > cur_sc + 1e-6 else "reject"),
                         fail_class=("" if sc2 > cur_sc + 1e-6 else "no FIT/VAL band-score gain"),
                         next_action=("carry forward" if sc2 > cur_sc + 1e-6 else "try next"))
                if sc2 > cur_sc + 1e-6 and (improved is None or sc2 > improved[0]):
                    improved = (sc2, cfg2)
            if improved:
                cur_sc, cur = improved[0], improved[1]
    print(f"[campaign] greedy stacking: {combo_iters} combo iterations", flush=True)

    # 4b. Optuna TPE (or seeded random fallback)
    t0 = time.time()
    if HAVE_OPTUNA:
        def objective(trial):
            cfg = _suggest(trial, max_mask, max_pm)
            sc, *_ = consider(cfg, "optuna")
            return sc
        study = optuna.create_study(direction="maximize",
                                    sampler=optuna.samplers.TPESampler(seed=seed))
        study.optimize(objective, n_trials=trials, timeout=time_budget_min * 60.0)
        n_done = len(study.trials)
        engine = "Optuna TPE"
    else:
        print("Optuna unavailable; using seeded random search fallback.", flush=True)
        rng = random.Random(seed); n_done = 0
        for _ in range(trials):
            if time.time() - t0 > time_budget_min * 60.0:
                break
            consider(_suggest(_RandTrial(rng), max_mask, max_pm), "random")
            n_done += 1
        engine = "seeded random search"
    print(f"[campaign] stage4 search: {n_done} trials via {engine}", flush=True)

    tdf = pd.DataFrame(trial_rows).sort_values("score", ascending=False)
    tdf.to_csv(WORK / "trials.csv", index=False)
    ranked = sorted(best_pool.values(), key=lambda x: -x["score"])
    save_json("combos.json", [{**{k: v for k, v in r.items() if k != "cfg"},
                               "cfg": {k: v for k, v in r["cfg"].items() if k != "status"}}
                              for r in ranked[:40]])
    return ranked


# ---------------------------------------------------------------------------
# stage 5 — TRAIN confirmation + budget-capped TEST
# ---------------------------------------------------------------------------
def stage5(ranked, test_budget=10, max_confirm=16, robustness=True, prior=None):
    global TEST_EVALS
    results = list(prior or [])
    seen_masks = set()
    confirmed = 0
    for r in ranked:
        if confirmed >= max_confirm or TEST_EVALS >= test_budget:
            break
        cfg = r["cfg"]
        # diversity: skip configs whose mask+premom+guard signature we already confirmed
        sig = json.dumps([sorted(map(list, cfg["mask_terms"])),
                          sorted(map(list, cfg["premom_terms"])), cfg["guard"]], sort_keys=True)
        if sig in seen_masks:
            continue
        seen_masks.add(sig)
        confirmed += 1
        mTR = full_eval(cfg, STATE["TRAIN"])
        trainm = {k: v for k, v in mTR.items() if k != "detail"}
        in_band = PF_LO <= mTR["net_pf"] <= PF_HI and mTR["n"] >= 20 and mTR["net_pnl"] > 0
        rec = {"tag": r.get("tag"), "score": r["score"],
               "cfg": {k: v for k, v in cfg.items() if k != "status"},
               "fit_pf": r["fit_pf"], "val_pf": r["val_pf"],
               "train": trainm, "in_band": bool(in_band), "test": None, "verdict": None}
        if not in_band:
            rec["verdict"] = ("REJECT: TRAIN PF outside [1.30,1.80]"
                              if not (PF_LO <= mTR["net_pf"] <= PF_HI)
                              else "REJECT: TRAIN too thin/negative")
            log_iter("confirm", cfg_str(cfg), "-", "-", "full-TRAIN confirmation",
                     fv_pack(r["fit_n"], r["fit_pf"], 0), fv_pack(r["val_n"], r["val_pf"], 0),
                     trainm=trainm, decision="reject", fail_class=rec["verdict"],
                     next_action="next candidate")
            results.append(rec)
            continue
        # TRAIN OK -> score TEST once
        TEST_EVALS += 1
        mTE = full_eval(cfg, STATE["TEST"])
        testm = {k: v for k, v in mTE.items() if k != "detail"}
        rec["test"] = testm
        checks = {
            "test_pf_gt_1.40": mTE["net_pf"] > TEST_PF_MIN,
            "test_net_pos": mTE["net_pnl"] > 0,
            "test_n_ge_5": mTE["n"] >= 5,
            "train_dom_ok": dom_ok(mTR),
            "test_dom_ok": dom_ok(mTE),
            "fit_val_no_collapse": min(r["fit_pf"], r["val_pf"]) >= 1.05,
        }
        rec["checks"] = checks
        passed = all(checks.values())
        if passed and robustness:
            rob = eng.robustness_report(SETUP, cfg, STATE["TRAIN"],
                                        STATE["mask_quant"], STATE.get("pm_quant", {}),
                                        argparse.Namespace(min_trades_train=20,
                                                           neighborhood_pf_min=1.10,
                                                           dropout_pf_min=1.00))
            rec["robustness"] = {k: v for k, v in rob.items()
                                 if k in ("neighbor_pass", "dropout_pass", "passed", "base")}
            rec["robustness_detail"] = rob
            passed = passed and rob["passed"]
        rec["verdict"] = "CANDIDATE" if passed else \
            "REJECT: " + ";".join(k for k, v in checks.items() if not v) + \
            ("" if rec.get("robustness", {}).get("passed", True) else ";robustness_failed")
        log_iter("confirm+test", cfg_str(cfg), "-", "-", "full-TRAIN in band -> single TEST scoring",
                 fv_pack(r["fit_n"], r["fit_pf"], 0), fv_pack(r["val_n"], r["val_pf"], 0),
                 trainm=trainm, testm=testm,
                 decision=("KEEP-CANDIDATE" if passed else "reject"),
                 fail_class=("" if passed else rec["verdict"]),
                 next_action=("write candidate json" if passed else "next candidate"))
        results.append(rec)
    save_json("confirmations.json", results)
    return results


# ---------------------------------------------------------------------------
# stage 6 — rescue loop
# ---------------------------------------------------------------------------
def stage6(sweeps, seed=7, extra_trials=250):
    """If stage5 produced nothing: simplify + retune around near-misses."""
    print("[campaign] stage6 rescue loop", flush=True)
    # 6a: single-term configs from strongest sweep knobs across a small exit fan
    rescue_ranked = []
    keeps = []
    for grp in ("mask", "premom", "guards", "regime"):
        rows = [r for r in sweeps[grp] if r["decision"].startswith("keep")]
        rows.sort(key=lambda r: -r["score"])
        keeps.extend((grp, r) for r in rows[:8])
    exits_top = sorted(sweeps["exits"], key=lambda r: -r["score"])[:6]
    pool = {}
    def consider(cfg, tag):
        sc, nf, pf_f, nv, pf_v = score_cfg(cfg)
        k = cfg_key(cfg)
        if k not in pool or sc > pool[k]["score"]:
            pool[k] = {"score": float(sc), "cfg": cfg, "fit_pf": pf_f, "val_pf": pf_v,
                       "fit_n": nf, "val_n": nv, "tag": tag}
        log_iter("rescue", cfg_str(cfg), "-", "-", tag,
                 fv_pack(nf, pf_f, 0), fv_pack(nv, pf_v, 0),
                 decision=("shortlist" if sc > 0 else "reject"),
                 fail_class=("" if sc > 0 else "below FV floor / negative band score"),
                 next_action="confirm if shortlisted")
    for er in exits_top:
        sl, tgt = er["label"].replace("SL", "").split("/T")
        for grp, r in keeps:
            lab = r["label"]
            if grp in ("mask", "regime"):
                if grp == "regime":
                    term = {"regime!=BEAR": ("regime", "!=", "BEAR"),
                            "regime==BULL": ("regime", "==", "BULL"),
                            "regime!=BULL": ("regime", "!=", "BULL")}[lab]
                else:
                    f = lab.split(">=")[0].split("<=")[0]
                    op = ">=" if ">=" in lab else "<="
                    term = (f, op, float(lab.split(op)[1].split("(")[0]))
                consider(mk_cfg(sl=float(sl), tgt=float(tgt), mask=[term]), f"rescue single-mask {lab}")
            elif grp == "premom":
                f = lab.split(">=")[0].split("<=")[0]
                op = ">=" if ">=" in lab else "<="
                thr = float(lab.split(op)[1].split("(")[0])
                consider(mk_cfg(sl=float(sl), tgt=float(tgt), premom=[(f, op, thr)]),
                         f"rescue single-premom {lab}")
            else:
                g = {}
                if lab.startswith("min_slot"):
                    g["min_slot"] = lab.split("=")[1]
                elif lab.startswith("max_slot"):
                    g["max_slot"] = lab.split("=")[1]
                elif lab.startswith("top_n"):
                    g["top_n"] = int(lab.split("=")[1])
                else:
                    continue
                consider(mk_cfg(sl=float(sl), tgt=float(tgt), guard=g), f"rescue guard-only {lab}")
    # 6b: a second, tighter Optuna run restricted to 1 mask + 1 premom max
    if HAVE_OPTUNA:
        def objective(trial):
            cfg = _suggest(trial, 1, 1)
            sc, nf, pf_f, nv, pf_v = score_cfg(cfg)
            k = cfg_key(cfg)
            if k not in pool or sc > pool[k]["score"]:
                pool[k] = {"score": float(sc), "cfg": cfg, "fit_pf": pf_f, "val_pf": pf_v,
                           "fit_n": nf, "val_n": nv, "tag": "rescue_optuna"}
            return sc
        study = optuna.create_study(direction="maximize",
                                    sampler=optuna.samplers.TPESampler(seed=seed + 101))
        study.optimize(objective, n_trials=extra_trials, timeout=20 * 60.0)
    ranked = sorted(pool.values(), key=lambda x: -x["score"])
    save_json("rescue.json", [{**{k: v for k, v in r.items() if k != "cfg"},
                               "cfg": {k: v for k, v in r["cfg"].items() if k != "status"}}
                              for r in ranked[:30]])
    return ranked


# ---------------------------------------------------------------------------
def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--pool", default=str(WORK / "pools" / "pool_full"))
    ap.add_argument("--stages", default="2,3,4,5")
    ap.add_argument("--trials", type=int, default=500)
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--time_budget_min", type=float, default=45.0)
    ap.add_argument("--test_budget", type=int, default=10)
    args = ap.parse_args()
    try:
        sys.stdout.reconfigure(line_buffering=True)
    except Exception:
        pass

    stages = {s.strip() for s in args.stages.split(",")}
    print(f"[campaign] optimizer: {'Optuna TPE' if HAVE_OPTUNA else 'Optuna unavailable; using seeded random search fallback.'}",
          flush=True)
    prepare(Path(args.pool))
    pm_quantiles(seed=args.seed)

    sweeps = None
    ranked = None
    if "2" in stages:
        stage2()
    if "3" in stages:
        sweeps = stage3()
        save_json("iteration_records.json", ITER_LOG)
    if sweeps is None and (WORK / "sweeps.json").exists():
        sweeps = json.loads((WORK / "sweeps.json").read_text())
    if "4" in stages:
        ranked = stage4(sweeps, trials=args.trials, seed=args.seed,
                        time_budget_min=args.time_budget_min)
        save_json("iteration_records.json", ITER_LOG)
    results = []
    if "5" in stages and ranked:
        results = stage5(ranked, test_budget=args.test_budget)
        save_json("iteration_records.json", ITER_LOG)
    n_cand = sum(1 for r in results if r["verdict"] == "CANDIDATE")
    if "6" in stages and n_cand == 0:
        rranked = stage6(sweeps, seed=args.seed)
        results = stage5(rranked, test_budget=args.test_budget, prior=results)

    # persist the raw iteration log (markdown rendering happens in a separate reporter)
    save_json("iteration_records.json", ITER_LOG)
    n_cand = sum(1 for r in results if r.get("verdict") == "CANDIDATE")
    print(f"[campaign] DONE. iterations={len(ITER_LOG)} test_evals={TEST_EVALS} candidates={n_cand}",
          flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
