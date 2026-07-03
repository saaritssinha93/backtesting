r"""run_full_loop.py — B_AVWAP_RECLAIM_REVERSAL (LONG) full PF-band optimization loop.
================================================================================
RESEARCH-ONLY. No live trades, no order placement, no final_setup_conf.py edits.
All artifacts stay under Train_and_Test/setup_pf_1_4_full_loop/B_AVWAP_RECLAIM_REVERSAL/.

Campaign windows (user-mandated):
  TRAIN = completed sessions 2026-03-01 .. 2026-05-30
  TEST  = completed sessions 2026-06-01 .. 2026-07-02 (2026-07-02 drops out —
          its EOD 1-min sync had not run at pool build; requested vs actual is
          reported by the loop)
  FIT   = first 60% of TRAIN sessions ; VAL = remaining 40%

Target: TRAIN PF in [1.30, 1.80], TEST PF > 1.40 (prefer >= 1.50), positive net
PnL both windows, meaningful trades, no trade/day/symbol domination
(trade<=35% of gross profit, day<=40% / symbol<=40% of net), day-block p<=0.10,
threshold-neighborhood + term-dropout stability on TRAIN.

Staged protocol (anti-overfit) identical to the A_MOD full-loop campaigns:
  Stage 1 baseline / Stage 2 failure study / Stage 3 one-knob sweeps (FIT+VAL) /
  Stage 4 Optuna-TPE combination search on FIT/VAL (never sees TEST) /
  Stage 5 finalists -> full-TRAIN confirm -> TEST scored ONCE per in-band finalist /
  Stage 6 rescue loop (premom-off re-search, term simplification, time windows).

Reuses the repo pipeline via setup_train_test.py (entry = next 1-min open +
slippage, exits resolved on 1-min OHLC to 15:20 IST, statutory NSE costs,
family dedupe, portfolio overlay) and the shared approval-loop engine helpers.

Run from repo root:
  py -3.12 Train_and_Test\setup_pf_1_4_full_loop\B_AVWAP_RECLAIM_REVERSAL\scripts\run_full_loop.py ^
     --trials 500 --time_budget_min 60 --seed 7
"""
from __future__ import annotations

import argparse
import json
import random
import sys
import time
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd

_HERE = Path(__file__).resolve().parent            # scripts/
WORK = _HERE.parent                                # A_MOD_BREAK_C1_LOW/
TT_DIR = WORK.parent.parent                        # Train_and_Test/
REPO = TT_DIR.parent
ENGINE_DIR = TT_DIR / "setup_pf_1_4_approval_loop" / "_engine"
for _p in (str(REPO), str(TT_DIR), str(ENGINE_DIR)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import setup_train_test as tt                      # noqa: E402
import pf_band_fitval_loop as eng                  # noqa: E402  (shared engine helpers)

try:
    import optuna
    optuna.logging.set_verbosity(optuna.logging.WARNING)
    _HAVE_OPTUNA = True
except Exception:
    _HAVE_OPTUNA = False

SETUP = "B_AVWAP_RECLAIM_REVERSAL"
SIDE = "LONG"

# ---- user-mandated band + gates -------------------------------------------------
PF_LO, PF_HI = 1.30, 1.80
eng.PF_LO, eng.PF_HI = PF_LO, PF_HI                # retune the engine band tent to 1.80
TEST_PF_MIN = 1.40
TRADE_DOM_CAP = 0.35        # top trade share of GROSS profit
DAY_DOM_CAP = 0.40          # top day share of NET
SYM_DOM_CAP = 0.40          # top symbol share of NET
DAY_BLOCK_P_MAX = 0.10
MIN_TRAIN_TRADES = 20
MIN_TEST_TRADES = 5
MAX_TRADES_DAY = 6.0
MIN_TRAIN_TARGET_RATE = 12.0

# search the full engine mask-feature list; quantile construction prunes anything
# empty/constant for this setup's raw rows (TRAIN-only quantiles, never TEST)
MASK_FEATS = list(eng.MASK_FEATS)
REGIME_TERMS = [("regime", "!=", "BEAR"), ("regime", "==", "NEUTRAL"),
                ("regime", "!=", "TREND"), ("regime", "!=", "BULL")]
PM_FEATS = list(eng.PM_FEATS)
QGRID = list(eng.QGRID)
QUANT_KEYS = sorted(set(QGRID) | {0.2, 0.35, 0.5, 0.65, 0.8})  # sweep grid needs 0.35/0.65
SL_GRID = [0.50, 0.70, 0.85, 1.00, 1.10, 1.20, 1.50]
TGT_GRID = [0.60, 0.80, 1.00, 1.25, 1.50, 2.00, 2.50]
MIN_SLOTS = ["09:30", "09:45", "10:00", "10:30", "11:00", "12:00"]
MAX_SLOTS = ["11:30", "12:00", "12:30", "13:00", "14:00", "14:30"]

TODAY = date.today().isoformat()


# ---------------------------------------------------------------------------------
def evalm(cfg, df, full=False):
    tt.MAX_POSITIONS = cfg.get("max_positions", 20)
    tt.DAILY_LOSS_RS = cfg.get("daily_loss_rs", 0.0)
    return (eng.full_metrics if full else eng.fast_metrics)(SETUP, cfg, df)


def eval_fast(cfg, df):
    tt.MAX_POSITIONS = cfg.get("max_positions", 20)
    tt.DAILY_LOSS_RS = cfg.get("daily_loss_rs", 0.0)
    return eng._eval_fast(SETUP, cfg, df)


def band_score(pf_f, pf_v, nf, nv, fv_floor, gap_lambda):
    if nf < fv_floor or nv < fv_floor:
        return -5.0 + min(nf, nv) / max(1, fv_floor)
    cf, cv = eng._clamp_pf(pf_f), eng._clamp_pf(pf_v)
    sc = eng.band_reward(min(cf, cv)) - gap_lambda * abs(cf - cv)
    if min(cf, cv) >= PF_LO:
        sc += 0.003 * min(min(nf, nv), 40)
    return sc


def cfg_key(cfg):
    return json.dumps({"sl": cfg["sl"], "tgt": cfg["tgt"],
                       "m": sorted(map(list, cfg["mask_terms"])),
                       "p": sorted(map(list, cfg["premom_terms"])),
                       "g": cfg["guard"] or {}, "mp": cfg.get("max_positions", 20),
                       "dl": cfg.get("daily_loss_rs", 0.0)}, default=str)


def terms_str(terms):
    return "; ".join(f"{a}{o}{b}" for a, o, b in terms) or "(none)"


def dom_ok(m):
    return (m["trade_dom_gross"] is not None and m["trade_dom_gross"] <= TRADE_DOM_CAP
            and m["day_dom"] is not None and m["day_dom"] <= DAY_DOM_CAP
            and m["sym_dom"] is not None and m["sym_dom"] <= SYM_DOM_CAP)


def classify_failure(mTR, mTE=None):
    """Heuristic failure tags for the iteration log / failure analysis."""
    tags = []
    if mTR["n"] < MIN_TRAIN_TRADES:
        tags.append("thin_train_sample")
    if mTR["net_pf"] < 1.0:
        tags.append("no_train_edge")
    elif mTR["net_pf"] < PF_LO:
        tags.append("weak_train_edge")
    if mTR["net_pf"] > PF_HI:
        tags.append("train_overfit_suspect(PF>1.80)")
    tot = mTR["sl_cnt"] + mTR["tgt_cnt"] + mTR["eod_cnt"]
    if tot:
        if mTR["eod_cnt"] / tot > 0.40:
            tags.append("too_many_time_exits")
        if mTR["tgt_cnt"] / tot < 0.12:
            tags.append("target_too_ambitious")
        if mTR["sl_cnt"] / tot > 0.55:
            tags.append("sl_too_tight_or_bad_entries")
    if mTR["trades_per_day"] > MAX_TRADES_DAY:
        tags.append("overtrading")
    if not dom_ok(mTR):
        tags.append("train_concentration")
    if mTE is not None:
        if mTE["n"] < MIN_TEST_TRADES:
            tags.append("thin_test_sample")
        elif mTE["net_pf"] < TEST_PF_MIN:
            tags.append("test_collapse" if mTR["net_pf"] >= PF_LO else "no_edge_anywhere")
        if mTE["n"] >= MIN_TEST_TRADES and not dom_ok(mTE):
            tags.append("test_concentration")
        if mTE["day_block_p"] is not None and mTE["day_block_p"] > DAY_BLOCK_P_MAX:
            tags.append("test_day_block_insignificant")
    return tags or ["-"]


def acceptance(mTR, mTE, robust):
    """User acceptance rules. Returns (passed, hard_reasons, warnings)."""
    hard, warn = [], []
    if not (PF_LO <= mTR["net_pf"] <= PF_HI):
        hard.append(f"TRAIN PF {mTR['net_pf']} outside [{PF_LO},{PF_HI}]")
    if mTR["n"] < MIN_TRAIN_TRADES:
        hard.append(f"TRAIN n {mTR['n']} < {MIN_TRAIN_TRADES}")
    if mTR["net_pnl"] <= 0:
        hard.append("TRAIN net PnL not positive")
    if mTR["target_rate"] < MIN_TRAIN_TARGET_RATE:
        hard.append(f"TRAIN target-fill {mTR['target_rate']}% < {MIN_TRAIN_TARGET_RATE}%")
    if mTR["trades_per_day"] > MAX_TRADES_DAY:
        hard.append("TRAIN trades/day above cap")
    if not dom_ok(mTR):
        hard.append("TRAIN domination (trade>35% gross or day/sym>40% net)")
    if mTE["n"] < MIN_TEST_TRADES:
        hard.append(f"TEST n {mTE['n']} < {MIN_TEST_TRADES}")
    else:
        if mTE["net_pf"] <= TEST_PF_MIN:
            hard.append(f"TEST PF {mTE['net_pf']} <= {TEST_PF_MIN}")
        if mTE["net_pnl"] <= 0:
            hard.append("TEST net PnL not positive")
        if not dom_ok(mTE):
            hard.append("TEST domination (trade>35% gross or day/sym>40% net)")
        if mTE["day_block_p"] is None or not np.isfinite(float(mTE["day_block_p"])):
            warn.append("TEST day-block p unavailable")
        elif mTE["day_block_p"] > DAY_BLOCK_P_MAX:
            hard.append(f"TEST day-block p {mTE['day_block_p']} > {DAY_BLOCK_P_MAX}")
        if mTE["trades_per_day"] > MAX_TRADES_DAY:
            hard.append("TEST trades/day above cap")
        if mTE["n"] < 20:
            warn.append(f"TEST n {mTE['n']} < 20 (thin, sample-limited)")
    if robust is not None:
        if not robust["neighbor_pass"]:
            hard.append("threshold-neighborhood robustness failed")
        if not robust["dropout_pass"]:
            hard.append("term-dropout robustness failed")
    if mTR["net_pf"] > 1.70:
        warn.append("TRAIN PF in upper band (1.70-1.80) — watch for overfit")
    return (len(hard) == 0), hard, warn


def mline(m):
    return (f"n={m['n']} PF={m['net_pf']} net=Rs{m['net_pnl']:,.0f} win%={m['win_rate']} "
            f"avgW=Rs{m['avg_win']:,.0f} avgL=Rs{m['avg_loss']:,.0f} maxDD=Rs{m['max_dd']:,.0f} "
            f"SL/TGT/EOD={m['sl_cnt']}/{m['tgt_cnt']}/{m['eod_cnt']} tgt%={m['target_rate']} "
            f"tpd={m['trades_per_day']} tradeDom={m['trade_dom_gross']} dayDom={m['day_dom']} "
            f"symDom={m['sym_dom']} dbp={m['day_block_p']}")


def mtable(m):
    return (f"| trades | {m['n']} |\n| net PF | {m['net_pf']} |\n| net PnL | Rs{m['net_pnl']:,.0f} |\n"
            f"| win rate | {m['win_rate']}% |\n| wins / losses | {m['wins']} / {m['losses']} |\n"
            f"| avg win / avg loss | Rs{m['avg_win']:,.0f} / Rs{m['avg_loss']:,.0f} |\n"
            f"| avgW/avgL ratio | {round(abs(m['avg_win'] / m['avg_loss']), 2) if m['avg_loss'] else 'n/a'} |\n"
            f"| gross profit / loss | Rs{m['gross_profit']:,.0f} / Rs{m['gross_loss']:,.0f} |\n"
            f"| max drawdown | Rs{m['max_dd']:,.0f} |\n"
            f"| SL / TGT / EOD exits | {m['sl_cnt']} / {m['tgt_cnt']} / {m['eod_cnt']} |\n"
            f"| target-fill rate | {m['target_rate']}% |\n"
            f"| trades/day | {m['trades_per_day']} |\n| days / symbols | {m['n_days']} / {m['n_syms']} |\n"
            f"| top-trade gross share | {m['trade_dom_gross']} |\n| top-day net share | {m['day_dom']} |\n"
            f"| top-symbol net share | {m['sym_dom']} |\n| day-block p | {m['day_block_p']} |\n"
            f"| top day | {m['top_day']} |\n| top symbol | {m['top_sym']} |")


# ---------------------------------------------------------------------------------
def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--pool", default=str(WORK / "pools" / SETUP))
    ap.add_argument("--train_start", default="2026-03-01")
    ap.add_argument("--train_end", default="2026-05-30")
    ap.add_argument("--test_start", default="2026-06-01")
    ap.add_argument("--test_end", default="2026-07-02")
    ap.add_argument("--fit_frac", type=float, default=0.60)
    ap.add_argument("--trials", type=int, default=500)
    ap.add_argument("--time_budget_min", type=float, default=60.0)
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--gap_lambda", type=float, default=0.80)
    ap.add_argument("--fv_floor", type=int, default=10)
    ap.add_argument("--max_mask_terms", type=int, default=2)
    ap.add_argument("--max_pm_terms", type=int, default=2)
    ap.add_argument("--n_finalists", type=int, default=6)
    ap.add_argument("--max_test_evals", type=int, default=8)
    ap.add_argument("--search_slippage_bps", type=float, default=15.0)
    ap.add_argument("--pm_quantile_sample", type=int, default=1500)
    ap.add_argument("--min_trades_train", type=int, default=MIN_TRAIN_TRADES)
    ap.add_argument("--neighborhood_pf_min", type=float, default=1.15)
    ap.add_argument("--dropout_pf_min", type=float, default=1.00)
    ap.add_argument("--skip_sweeps", action="store_true")
    args = ap.parse_args()
    try:
        sys.stdout.reconfigure(line_buffering=True)
    except Exception:
        pass
    rng = random.Random(args.seed)

    print(f"[full-loop] {SETUP} ({SIDE}) — band [{PF_LO},{PF_HI}], TEST>{TEST_PF_MIN}")
    engine_name = "Optuna TPE" if _HAVE_OPTUNA else "seeded random search"
    if not _HAVE_OPTUNA:
        print("Optuna unavailable; using seeded random search fallback.")
    print(f"[full-loop] optimizer: {engine_name}")

    # ---------------- Stage 0: pool load + window split --------------------------
    tt.POOL_DIRS = [Path(args.pool)]
    tt.POOL_DIR = Path(args.pool)
    pool = tt.load_pool()
    pool = pool[pool["setup"] == SETUP].copy()
    sessions = sorted(pd.Series(pool["_day"].dropna().unique()))

    def _in(s, lo, hi):
        return pd.Timestamp(lo) <= s <= pd.Timestamp(hi)
    TRAIN_s = [s for s in sessions if _in(s, args.train_start, args.train_end)]
    TEST_s = [s for s in sessions if _in(s, args.test_start, args.test_end)]
    n_fit = max(1, int(round(len(TRAIN_s) * args.fit_frac)))
    FIT_s, VAL_s = TRAIN_s[:n_fit], TRAIN_s[n_fit:]

    def _rng_lbl(ss):
        return f"{pd.Timestamp(ss[0]).date()}..{pd.Timestamp(ss[-1]).date()}" if ss else "(empty)"
    print(f"[full-loop] requested TRAIN {args.train_start}..{args.train_end} -> actual {_rng_lbl(TRAIN_s)} ({len(TRAIN_s)} sessions)")
    print(f"[full-loop] requested TEST  {args.test_start}..{args.test_end} -> actual {_rng_lbl(TEST_s)} ({len(TEST_s)} sessions)")
    print(f"[full-loop] FIT {_rng_lbl(FIT_s)} ({len(FIT_s)}) | VAL {_rng_lbl(VAL_s)} ({len(VAL_s)})")

    span = set(map(pd.Timestamp, TRAIN_s + TEST_s))
    sub = pool[pool["_day"].isin(span)].copy()
    eng._set_slippage(args.search_slippage_bps)
    t0 = time.time()
    print(f"[full-loop] attaching 1-min entries for {len(sub)} raw candidates ...")
    sub = tt.attach_entries(sub)
    print(f"[full-loop] entries attached: {len(sub)} rows in {time.time()-t0:.0f}s")

    def _slice(ss):
        return sub[sub["_day"].isin(set(map(pd.Timestamp, ss)))].copy()
    FIT, VAL, TRAIN, TEST = _slice(FIT_s), _slice(VAL_s), _slice(TRAIN_s), _slice(TEST_s)
    print(f"[full-loop] entry-attached rows: FIT={len(FIT)} VAL={len(VAL)} TRAIN={len(TRAIN)} TEST={len(TEST)}")

    windows = {"FIT": _rng_lbl(FIT_s), "VAL": _rng_lbl(VAL_s),
               "TRAIN": _rng_lbl(TRAIN_s), "TEST": _rng_lbl(TEST_s),
               "FIT_sessions": [str(pd.Timestamp(s).date()) for s in FIT_s],
               "VAL_sessions": [str(pd.Timestamp(s).date()) for s in VAL_s],
               "TEST_sessions": [str(pd.Timestamp(s).date()) for s in TEST_s],
               "n_train_sessions": len(TRAIN_s), "n_test_sessions": len(TEST_s)}

    # quantile grids from TRAIN only (never TEST)
    mask_quant = {}
    for f in MASK_FEATS:
        if f in TRAIN.columns:
            s = pd.to_numeric(TRAIN[f], errors="coerce").dropna()
            if len(s) >= 8 and s.nunique() > 1:
                mask_quant[f] = {q: float(s.quantile(q)) for q in QUANT_KEYS}
    pm_source = TRAIN
    if args.pm_quantile_sample and len(TRAIN) > args.pm_quantile_sample:
        pm_source = TRAIN.sample(n=int(args.pm_quantile_sample), random_state=args.seed).sort_index()
    print(f"[full-loop] computing premom quantiles on {len(pm_source)} TRAIN rows ...")
    pm_recs = []
    for j, r in enumerate(pm_source.itertuples(), 1):
        feats, reason = tt._premom(r.ticker, r.side, r.tt_entry_iso, float(r.tt_fill), 0.90, r.tt_sig_ts.isoformat())
        fd = dict(feats) if not reason else {}
        pm_recs.append({f: fd.get(f, np.nan) for f in PM_FEATS})
        if j % 500 == 0:
            print(f"[full-loop] premom quantiles {j}/{len(pm_source)}", flush=True)
    pm_df_q = pd.DataFrame(pm_recs)
    pm_quant = {}
    for f in PM_FEATS:
        s = pd.to_numeric(pm_df_q[f], errors="coerce").dropna()
        if len(s) >= 8 and s.nunique() > 1:
            pm_quant[f] = {q: float(s.quantile(q)) for q in QUANT_KEYS}
    print(f"[full-loop] searchable mask={sorted(mask_quant)} premom={sorted(pm_quant)}")

    # ---------------- Stage 1: baseline ------------------------------------------
    base_block, base_src = eng.get_baseline_block(SETUP)
    if base_block is not None:
        base_cfg = eng.conf_to_cfg(base_block)
    else:
        base_cfg = {"sl": 0.70, "tgt": 1.25, "mask_terms": [], "premom_terms": [],
                    "guard": None, "status": "OK", "max_positions": 20,
                    "daily_loss_rs": 0.0}
        base_src = "default exits 0.70/1.25 (no conf entry — raw detection baseline)"
    print(f"[stage1] baseline source: {base_src}")
    mB = {}
    for lbl, df in (("FIT", FIT), ("VAL", VAL), ("TRAIN", TRAIN), ("TEST", TEST)):
        mB[lbl] = evalm(base_cfg, df, full=True)
        print(f"[stage1] baseline {lbl}: {mline(mB[lbl])}")
    for lbl in ("TRAIN", "TEST"):
        det = mB[lbl]["detail"]
        if det is not None and not det.empty:
            det.to_csv(WORK / f"baseline_trades_{lbl.lower()}.csv", index=False)

    iter_rows = []      # unified iteration log
    it_no = [0]

    def log_iter(stage, group, change, old, new, cfg, mF=None, mV=None, mTR=None, mTE=None,
                 keep="", why="", nxt=""):
        it_no[0] += 1
        iter_rows.append({
            "iter": it_no[0], "stage": stage, "group": group, "change": change,
            "old": old, "new": new,
            "sl": cfg["sl"], "tgt": cfg["tgt"], "mask": terms_str(cfg["mask_terms"]),
            "premom": terms_str(cfg["premom_terms"]),
            "guard": json.dumps(cfg["guard"]) if cfg["guard"] else "-",
            "fit_n": (mF or {}).get("n"), "fit_pf": (mF or {}).get("net_pf"),
            "val_n": (mV or {}).get("n"), "val_pf": (mV or {}).get("net_pf"),
            "train_n": (mTR or {}).get("n"), "train_pf": (mTR or {}).get("net_pf"),
            "train_net": (mTR or {}).get("net_pnl"),
            "test_n": (mTE or {}).get("n"), "test_pf": (mTE or {}).get("net_pf"),
            "test_net": (mTE or {}).get("net_pnl"),
            "sl_cnt": (mTR or {}).get("sl_cnt"), "tgt_cnt": (mTR or {}).get("tgt_cnt"),
            "eod_cnt": (mTR or {}).get("eod_cnt"),
            "keep": keep, "why": why, "next": nxt,
        })

    log_iter("1-baseline", "baseline", "current conf config", "-", "-", base_cfg,
             mB["FIT"], mB["VAL"], mB["TRAIN"], mB["TEST"],
             keep="baseline", why=",".join(classify_failure(mB["TRAIN"], mB["TEST"])),
             nxt="failure study + sweeps")

    # ---------------- Stage 3: one-knob sweeps from baseline ---------------------
    sweep_rows = []
    base_fit = evalm(base_cfg, FIT)
    base_val = evalm(base_cfg, VAL)
    base_sc = band_score(base_fit["net_pf"], base_val["net_pf"], base_fit["n"], base_val["n"],
                         args.fv_floor, args.gap_lambda)

    def sweep(group, knob, old, new, cfg):
        mF, mV = evalm(cfg, FIT), evalm(cfg, VAL)
        sc = band_score(mF["net_pf"], mV["net_pf"], mF["n"], mV["n"], args.fv_floor, args.gap_lambda)
        verdict = "improve" if sc > base_sc + 1e-6 else ("flat" if abs(sc - base_sc) <= 1e-6 else "worse")
        sweep_rows.append({"group": group, "knob": knob, "old": old, "new": new,
                           "fit_n": mF["n"], "fit_pf": mF["net_pf"], "val_n": mV["n"],
                           "val_pf": mV["net_pf"], "score": round(sc, 4),
                           "vs_baseline": verdict})
        log_iter("3-sweep", group, knob, old, new, cfg, mF, mV,
                 keep=verdict, why=f"band score {sc:.3f} vs baseline {base_sc:.3f}",
                 nxt="feed stable knobs into stage-4 combos")
        return sc

    if not args.skip_sweeps:
        print(f"[stage3] baseline FIT/VAL band score = {base_sc:.4f}; sweeping ...")
        # exits
        for slv in SL_GRID:
            if slv != base_cfg["sl"]:
                c = eng._copy_cfg(base_cfg); c["sl"] = slv
                sweep("exit", "sl_pct", base_cfg["sl"], slv, c)
        for tg in TGT_GRID:
            if tg != base_cfg["tgt"]:
                c = eng._copy_cfg(base_cfg); c["tgt"] = tg
                sweep("exit", "tgt_pct", base_cfg["tgt"], tg, c)
        # existing mask term threshold (vol_ratio) across quantiles + drop
        for i, (f, op, thr) in enumerate(list(base_cfg["mask_terms"])):
            if f in mask_quant:
                for q in (0.2, 0.35, 0.5, 0.65, 0.8):
                    nv = round(mask_quant[f][q], 6)
                    c = eng._copy_cfg(base_cfg)
                    mt = list(c["mask_terms"]); mt[i] = (f, op, nv); c["mask_terms"] = mt
                    sweep("filter", f"mask {f}{op}", thr, f"{nv} (q{q})", c)
            c = eng._copy_cfg(base_cfg)
            mt = list(c["mask_terms"]); mt.pop(i); c["mask_terms"] = mt
            sweep("filter", f"drop mask {f}{op}{thr}", f"{f}{op}{thr}", "dropped", c)
        # additional single mask terms (relaxed/medium/strict)
        for f in sorted(mask_quant):
            for q in (0.2, 0.5, 0.8):
                for op in (">=", "<="):
                    nv = round(mask_quant[f][q], 6)
                    c = eng._copy_cfg(base_cfg)
                    c["mask_terms"] = list(c["mask_terms"]) + [(f, op, nv)]
                    sweep("indicator/price-action", f"+mask {f}{op}", "-", f"{nv} (q{q})", c)
        # regime categorical
        for term in REGIME_TERMS:
            c = eng._copy_cfg(base_cfg)
            c["mask_terms"] = list(c["mask_terms"]) + [term]
            sweep("regime", f"+mask {term[0]}{term[1]}{term[2]}", "-", term[2], c)
        # existing premom thresholds across quantiles + drop each
        for i, (f, op, thr) in enumerate(list(base_cfg["premom_terms"])):
            if f in pm_quant:
                for q in (0.2, 0.35, 0.5, 0.65, 0.8):
                    nv = round(pm_quant[f][q], 6)
                    c = eng._copy_cfg(base_cfg)
                    pt = list(c["premom_terms"]); pt[i] = (f, op, nv); c["premom_terms"] = pt
                    sweep("pre-momentum", f"premom {f}{op}", thr, f"{nv} (q{q})", c)
            c = eng._copy_cfg(base_cfg)
            pt = list(c["premom_terms"]); pt.pop(i); c["premom_terms"] = pt
            sweep("pre-momentum", f"drop premom {f}{op}{thr}", f"{f}{op}{thr}", "dropped", c)
        # new premom terms
        for f in sorted(pm_quant):
            for q in (0.2, 0.5, 0.8):
                for op in (">=", "<="):
                    nv = round(pm_quant[f][q], 6)
                    c = eng._copy_cfg(base_cfg)
                    c["premom_terms"] = list(c["premom_terms"]) + [(f, op, nv)]
                    sweep("pre-momentum", f"+premom {f}{op}", "-", f"{nv} (q{q})", c)
        # guards
        for ms in MIN_SLOTS:
            c = eng._copy_cfg(base_cfg); g = dict(c["guard"] or {}); g["min_slot"] = ms; c["guard"] = g
            sweep("guard", "min_slot", (base_cfg["guard"] or {}).get("min_slot", "-"), ms, c)
        for mx in MAX_SLOTS:
            c = eng._copy_cfg(base_cfg); g = dict(c["guard"] or {}); g["max_slot"] = mx; c["guard"] = g
            sweep("guard", "max_slot", (base_cfg["guard"] or {}).get("max_slot", "-"), mx, c)
        for tn in (1, 2, 3):
            c = eng._copy_cfg(base_cfg); g = dict(c["guard"] or {}); g["top_n"] = tn; c["guard"] = g
            sweep("guard", "top_n", "-", tn, c)
        for dl in (2000.0, 4000.0):
            c = eng._copy_cfg(base_cfg); c["daily_loss_rs"] = dl
            sweep("guard", "daily_loss_rs", base_cfg.get("daily_loss_rs", 0.0), dl, c)
        for mp in (5, 10):
            c = eng._copy_cfg(base_cfg); c["max_positions"] = mp
            sweep("guard", "max_positions", base_cfg.get("max_positions", 20), mp, c)
        pd.DataFrame(sweep_rows).to_csv(WORK / "sweeps.csv", index=False)
        print(f"[stage3] {len(sweep_rows)} single-knob sweeps done")

    # ---------------- Stage 4: combination search on FIT/VAL ---------------------
    trial_rows = []
    best = {"score": -1e9, "cfg": None}
    seen = {}

    def _suggest_cfg(trial):
        mask_terms = []
        n_mask = trial.suggest_int("n_mask", 0, args.max_mask_terms)
        for i in range(n_mask):
            f = trial.suggest_categorical(f"mask{i}_feat", sorted(mask_quant))
            op = trial.suggest_categorical(f"mask{i}_op", [">=", "<="])
            q = trial.suggest_categorical(f"mask{i}_q", QGRID)
            mask_terms.append((f, op, round(float(mask_quant[f][q]), 6)))
        rg = trial.suggest_categorical("regime_term", ["none", "!=BEAR", "==NEUTRAL", "!=TREND", "!=BULL"])
        if rg != "none":
            op, val = rg[:2], rg[2:]
            mask_terms.append(("regime", op, val))
        premom_terms = []
        n_pm = trial.suggest_int("n_pm", 0, args.max_pm_terms)
        for i in range(n_pm):
            f = trial.suggest_categorical(f"pm{i}_feat", sorted(pm_quant))
            op = trial.suggest_categorical(f"pm{i}_op", [">=", "<="])
            q = trial.suggest_categorical(f"pm{i}_q", QGRID)
            premom_terms.append((f, op, round(float(pm_quant[f][q]), 6)))
        guard = {}
        if trial.suggest_categorical("use_min_slot", [False, True]):
            guard["min_slot"] = trial.suggest_categorical("min_slot", MIN_SLOTS)
        if trial.suggest_categorical("use_max_slot", [False, True]):
            guard["max_slot"] = trial.suggest_categorical("max_slot", MAX_SLOTS)
        tn = trial.suggest_categorical("top_n", [0, 1, 2, 3])
        if tn:
            guard["top_n"] = int(tn)
        return {"sl": float(trial.suggest_categorical("sl", SL_GRID)),
                "tgt": float(trial.suggest_categorical("tgt", TGT_GRID)),
                "mask_terms": mask_terms, "premom_terms": premom_terms,
                "guard": (guard or None), "status": "OK",
                "max_positions": int(trial.suggest_categorical("max_positions", [10, 20])),
                "daily_loss_rs": float(trial.suggest_categorical("daily_loss_rs", [0.0, 4000.0]))}

    t_search = time.time()

    def _score_and_record(cfg):
        k = cfg_key(cfg)
        if k in seen:
            return seen[k]
        nf, pf_f, _ = eval_fast(cfg, FIT)
        nv, pf_v, _ = eval_fast(cfg, VAL)
        sc = band_score(pf_f, pf_v, nf, nv, args.fv_floor, args.gap_lambda)
        trial_rows.append({"sl": cfg["sl"], "tgt": cfg["tgt"],
                           "mask": terms_str(cfg["mask_terms"]), "premom": terms_str(cfg["premom_terms"]),
                           "guard": json.dumps(cfg["guard"]) if cfg["guard"] else "-",
                           "max_positions": cfg.get("max_positions"), "daily_loss_rs": cfg.get("daily_loss_rs"),
                           "fit_n": nf, "fit_pf": round(pf_f, 3), "val_n": nv, "val_pf": round(pf_v, 3),
                           "score": round(float(sc), 4), "cfg_json": k})
        if sc > best["score"]:
            best["score"], best["cfg"] = sc, eng._copy_cfg(cfg)
        seen[k] = sc
        return sc

    print(f"[stage4] combination search: {args.trials} trials / {args.time_budget_min} min budget")
    if _HAVE_OPTUNA:
        def objective(trial):
            return _score_and_record(_suggest_cfg(trial))
        study = optuna.create_study(direction="maximize",
                                    sampler=optuna.samplers.TPESampler(seed=args.seed))
        study.optimize(objective, n_trials=args.trials, timeout=args.time_budget_min * 60.0)
        n_done = len(study.trials)
    else:
        n_done = 0
        for _ in range(args.trials):
            if time.time() - t_search > args.time_budget_min * 60.0:
                break
            _score_and_record(eng._suggest(eng._RandTrial(rng), mask_quant, pm_quant,
                                           args.max_mask_terms, args.max_pm_terms))
            n_done += 1
    tdf = pd.DataFrame(trial_rows).sort_values("score", ascending=False).reset_index(drop=True)
    tdf.to_csv(WORK / "trials.csv", index=False)
    print(f"[stage4] {n_done} trials ({len(tdf)} unique configs) in {time.time()-t_search:.0f}s; "
          f"best FIT/VAL score={best['score']:.4f}")

    # ---------------- Stage 5: finalists -> TRAIN confirm -> TEST once -----------
    finalists = []
    fam_seen = set()
    test_evals = 0
    for _, r in tdf.iterrows():
        if len(finalists) >= args.n_finalists:
            break
        cfg = json.loads(r["cfg_json"])
        cfg = {"sl": cfg["sl"], "tgt": cfg["tgt"],
               "mask_terms": [tuple(t) for t in cfg["m"]],
               "premom_terms": [tuple(t) for t in cfg["p"]],
               "guard": (cfg["g"] or None), "status": "OK",
               "max_positions": cfg.get("mp", 20), "daily_loss_rs": cfg.get("dl", 0.0)}
        # coerce numeric thresholds back to float (json round-trip)
        cfg["mask_terms"] = [(f, op, (thr if isinstance(thr, str) else float(thr)))
                             for f, op, thr in cfg["mask_terms"]]
        cfg["premom_terms"] = [(f, op, float(thr)) for f, op, thr in cfg["premom_terms"]]
        if float(r["score"]) < 0:
            break
        # diversity: skip near-duplicates of an already-picked finalist (same feature/op
        # family and same exits, just one quantile step apart)
        fam = (cfg["sl"], cfg["tgt"],
               frozenset((f, op) for f, op, _ in cfg["mask_terms"]),
               frozenset((f, op) for f, op, _ in cfg["premom_terms"]),
               frozenset((cfg["guard"] or {}).keys()))
        if fam in fam_seen:
            continue
        fam_seen.add(fam)
        finalists.append({"cfg": cfg, "fitval_score": float(r["score"]),
                          "fit_pf": float(r["fit_pf"]), "val_pf": float(r["val_pf"]),
                          "fit_n": int(r["fit_n"]), "val_n": int(r["val_n"])})

    results = []
    ns = argparse.Namespace(min_trades_train=args.min_trades_train,
                            neighborhood_pf_min=args.neighborhood_pf_min,
                            dropout_pf_min=args.dropout_pf_min)
    for fi, cand in enumerate(finalists, 1):
        cfg = cand["cfg"]
        mTR = evalm(cfg, TRAIN, full=True)
        in_band = PF_LO <= mTR["net_pf"] <= PF_HI and mTR["n"] >= MIN_TRAIN_TRADES
        rec = {"id": fi, "cfg": cfg, **{f"fitval_{k}": v for k, v in cand.items() if k != "cfg"},
               "train": {k: v for k, v in mTR.items() if k != "detail"}}
        if in_band and test_evals < args.max_test_evals:
            robust = eng.robustness_report(SETUP, cfg, TRAIN, mask_quant, pm_quant, ns)
            mTE = evalm(cfg, TEST, full=True)
            test_evals += 1
            passed, hard, warn = acceptance(mTR, mTE, robust)
            rec.update({"test": {k: v for k, v in mTE.items() if k != "detail"},
                        "robust": {k: robust[k] for k in ("neighbor_pass", "dropout_pass", "passed")},
                        "passed": passed, "hard_reasons": hard, "warnings": warn})
            log_iter("5-finalist", "combination", f"finalist #{fi}", "-", "-", cfg,
                     None, None, mTR, mTE,
                     keep=("PASS" if passed else "reject"),
                     why="; ".join(hard + warn) or "all gates passed",
                     nxt=("candidate saved" if passed else "rescue loop"))
            det = mTE["detail"]
            if det is not None and not det.empty:
                det.to_csv(WORK / f"finalist_{fi:02d}_trades_test.csv", index=False)
            mTR["detail"].to_csv(WORK / f"finalist_{fi:02d}_trades_train.csv", index=False)
        else:
            rec.update({"passed": False,
                        "hard_reasons": [f"TRAIN not in band or too thin (PF {mTR['net_pf']}, n {mTR['n']})"
                                         if not in_band else "TEST budget exhausted"],
                        "warnings": []})
            log_iter("5-finalist", "combination", f"finalist #{fi}", "-", "-", cfg,
                     None, None, mTR, None, keep="reject",
                     why=rec["hard_reasons"][0], nxt="rescue loop")
        results.append(rec)
        print(f"[stage5] finalist {fi}: TRAIN {mline(mTR)}"
              + (f"\n[stage5] finalist {fi}: TEST  {mline({k: v for k, v in rec['test'].items()})}"
                 if "test" in rec else ""))

    # ---------------- Stage 6: rescue loop (only if nothing passed) --------------
    passing = [r for r in results if r.get("passed")]
    rescue_notes = []
    if not passing:
        print("[stage6] no finalist passed — rescue loop")
        # R1: premom-off re-search (mask+exits+guards only)
        best_r = {"score": -1e9, "cfg": None}
        r_trials = []
        def _score_r(cfg):
            k = cfg_key(cfg)
            if k in seen:
                return seen[k]
            nf, pf_f, _ = eval_fast(cfg, FIT)
            nv, pf_v, _ = eval_fast(cfg, VAL)
            sc = band_score(pf_f, pf_v, nf, nv, args.fv_floor, args.gap_lambda)
            seen[k] = sc
            r_trials.append((sc, cfg, nf, pf_f, nv, pf_v))
            if sc > best_r["score"]:
                best_r["score"], best_r["cfg"] = sc, eng._copy_cfg(cfg)
            return sc
        if _HAVE_OPTUNA:
            def obj_r(trial):
                cfg = _suggest_cfg(trial)
                cfg["premom_terms"] = []
                return _score_r(cfg)
            st2 = optuna.create_study(direction="maximize",
                                      sampler=optuna.samplers.TPESampler(seed=args.seed + 1))
            st2.optimize(obj_r, n_trials=max(120, args.trials // 3), timeout=args.time_budget_min * 20.0)
        for sc, cfg, nf, pf_f, nv, pf_v in sorted(r_trials, key=lambda x: -x[0])[:40]:
            log_iter("6-rescue-R1", "premom-off search", "premom removed", "-", "-", cfg,
                     {"n": nf, "net_pf": round(pf_f, 3)}, {"n": nv, "net_pf": round(pf_v, 3)},
                     keep="-", why=f"score {sc:.3f}", nxt="confirm best on TRAIN")
        rescue_cands = []
        if best_r["cfg"] is not None:
            rescue_cands.append(("R1-premom-off", best_r["cfg"]))
        # R2: simplification of the best stage-4 config (drop each term)
        if best["cfg"] is not None:
            bc = best["cfg"]
            for i in range(len(bc["mask_terms"])):
                c = eng._copy_cfg(bc); mt = list(c["mask_terms"]); mt.pop(i); c["mask_terms"] = mt
                rescue_cands.append((f"R2-drop-mask-{i}", c))
            for i in range(len(bc["premom_terms"])):
                c = eng._copy_cfg(bc); pt = list(c["premom_terms"]); pt.pop(i); c["premom_terms"] = pt
                rescue_cands.append((f"R2-drop-premom-{i}", c))
        # R3: time-window restriction on the best stage-4 config
        if best["cfg"] is not None:
            for g in ({"max_slot": "12:00"}, {"min_slot": "10:00"}, {"min_slot": "10:00", "max_slot": "14:00"}):
                c = eng._copy_cfg(best["cfg"]); gg = dict(c["guard"] or {}); gg.update(g); c["guard"] = gg
                rescue_cands.append((f"R3-window-{json.dumps(g)}", c))
        # confirm rescues: FIT/VAL floor -> TRAIN band -> TEST once (budget-capped)
        scored = []
        for tag, cfg in rescue_cands:
            nf, pf_f, _ = eval_fast(cfg, FIT)
            nv, pf_v, _ = eval_fast(cfg, VAL)
            sc = band_score(pf_f, pf_v, nf, nv, args.fv_floor, args.gap_lambda)
            scored.append((sc, tag, cfg, nf, pf_f, nv, pf_v))
        for sc, tag, cfg, nf, pf_f, nv, pf_v in sorted(scored, key=lambda x: -x[0]):
            if test_evals >= args.max_test_evals:
                rescue_notes.append(f"{tag}: skipped (TEST budget exhausted)")
                break
            mTR = evalm(cfg, TRAIN, full=True)
            in_band = PF_LO <= mTR["net_pf"] <= PF_HI and mTR["n"] >= MIN_TRAIN_TRADES
            if not in_band:
                log_iter("6-rescue", tag, "rescue variant", "-", "-", cfg,
                         {"n": nf, "net_pf": round(pf_f, 3)}, {"n": nv, "net_pf": round(pf_v, 3)}, mTR, None,
                         keep="reject", why=f"TRAIN out of band (PF {mTR['net_pf']}, n {mTR['n']})",
                         nxt="next rescue")
                continue
            robust = eng.robustness_report(SETUP, cfg, TRAIN, mask_quant, pm_quant, ns)
            mTE = evalm(cfg, TEST, full=True)
            test_evals += 1
            passed, hard, warn = acceptance(mTR, mTE, robust)
            log_iter("6-rescue", tag, "rescue variant", "-", "-", cfg,
                     {"n": nf, "net_pf": round(pf_f, 3)}, {"n": nv, "net_pf": round(pf_v, 3)}, mTR, mTE,
                     keep=("PASS" if passed else "reject"), why="; ".join(hard + warn) or "all gates passed",
                     nxt=("candidate saved" if passed else "next rescue"))
            rec = {"id": 100 + len(rescue_notes), "tag": tag, "cfg": cfg,
                   "train": {k: v for k, v in mTR.items() if k != "detail"},
                   "test": {k: v for k, v in mTE.items() if k != "detail"},
                   "robust": {k: robust[k] for k in ("neighbor_pass", "dropout_pass", "passed")},
                   "passed": passed, "hard_reasons": hard, "warnings": warn}
            results.append(rec)
            if passed:
                passing.append(rec)
                break

    # ---------------- persist everything ------------------------------------------
    pd.DataFrame(iter_rows).to_csv(WORK / "iteration_log.csv", index=False)
    summary = {
        "generated": TODAY, "setup": SETUP, "side": SIDE, "optimizer": engine_name,
        "windows": windows, "band": [PF_LO, PF_HI], "test_pf_min": TEST_PF_MIN,
        "n_sweeps": len(sweep_rows), "n_trials": int(n_done), "n_unique_configs": int(len(tdf)),
        "n_test_evals": test_evals,
        "baseline_src": base_src,
        "baseline_cfg": eng.cfg_to_conf_block(SETUP, SIDE, base_cfg),
        "baseline_metrics": {lbl: {k: v for k, v in m.items() if k != "detail"} for lbl, m in mB.items()},
        "results": [{k: (v if k != "cfg" else eng.cfg_to_conf_block(SETUP, SIDE, v)) for k, v in r.items()}
                    for r in results],
        "n_passing": len(passing),
        "rescue_notes": rescue_notes,
    }
    (WORK / "run_summary.json").write_text(json.dumps(tt._json_sanitize(summary), indent=2, default=str),
                                           encoding="utf-8")
    print(f"[full-loop] DONE — {len(passing)} passing candidate(s); artifacts in {WORK}")
    print(f"[full-loop] iterations logged: {len(iter_rows)} (sweeps {len(sweep_rows)}, trials {n_done})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
