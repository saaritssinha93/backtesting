r"""run_recovery_loop.py — FROM-SCRATCH RECOVERY loop for A_MOD_BREAK_C1_LOW (SHORT).
======================================================================================
RESEARCH-ONLY. No live trades, no final_setup_conf.py edits. All artifacts stay here.

Logical variants evaluated (same core intent — impulse continuation short through the
prior bar's low, below session VWAP — but REDESIGNED detection/entry/exits):

  BASELINE        original scanner pool + production conf config (anchor)
  RX2_ALL         re-detected CORE events, incidental scanner gates removed
  RX2_FRESHLOW    CORE + bar makes a NEW session low (fresh-low continuation)
  RX2_CONFIRM2    CORE + previous bar also a red prior-low break (persistence)
  RX2_DEEP        CORE + closes >= 0.35 ATR below the broken level (deep flow)
  RX2_FIRST_MORN  first CORE event of the symbol-day, morning only (<= 12:00)
  RX2_MKT         CORE + NIFTY50 below its 5-min EMA20 (market alignment)
  RETEST          break -> pullback to the broken level within 4 bars -> red
                  rejection close back below it (later, better-priced entry)

Protocol per variant (anti-overfit, TEST never optimized):
  A: ungated exit-grid scan on FIT/VAL (grid includes TRAIN-only MFE/MAE-derived pairs)
  B: single-feature scan (relaxed/medium/strict = q0.2/0.5/0.8 both directions)
  C: Optuna TPE combination search on FIT/VAL band objective (<=2 mask + regime +
     guards + <=1 premom + exits)
  D: cross-variant finalists -> full TRAIN confirm -> TEST scored ONCE if TRAIN PF in
     [1.30, 1.80] (shared TEST budget)
  E: rescue (simplify / window-restrict best config) if nothing passes

Run:  py -3.12 Train_and_Test\setup_from_scratch_recovery_loop\A_MOD_BREAK_C1_LOW\scripts\run_recovery_loop.py ^
        --trials_per_variant 150 --minutes_per_variant 8 --seed 21
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd

_HERE = Path(__file__).resolve().parent
WORK = _HERE.parent
TT_DIR = WORK.parent.parent
REPO = TT_DIR.parent
ENGINE_DIR = TT_DIR / "setup_pf_1_4_approval_loop" / "_engine"
for _p in (str(REPO), str(TT_DIR), str(ENGINE_DIR)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import setup_train_test as tt                      # noqa: E402
import pf_band_fitval_loop as eng                  # noqa: E402

try:
    import optuna
    optuna.logging.set_verbosity(optuna.logging.WARNING)
    _HAVE_OPTUNA = True
except Exception:
    _HAVE_OPTUNA = False

SETUP0 = "A_MOD_BREAK_C1_LOW"
PF_LO, PF_HI = 1.30, 1.80
eng.PF_LO, eng.PF_HI = PF_LO, PF_HI
TEST_PF_MIN = 1.40
TRADE_DOM_CAP, DAY_DOM_CAP, SYM_DOM_CAP = 0.35, 0.40, 0.40
DAY_BLOCK_P_MAX = 0.10
MIN_TRAIN_TRADES = 20
MIN_TEST_TRADES = 5
MAX_TRADES_DAY = 6.0
MIN_TRAIN_TARGET_RATE = 12.0
TODAY = date.today().isoformat()

TRAIN_W = ("2026-03-01", "2026-05-30")
TEST_W = ("2026-06-01", "2026-07-02")
FIT_FRAC = 0.60

# search dictionary for redesigned pools (all causal; market_ret/signal_minute excluded)
RX_FEATS = ["vol_ratio", "atr_pct", "body_pct", "close_loc", "vwap_dist_atr", "break_depth_atr",
            "quality_score", "rsi", "rsi_slope3", "adx5", "adx_slope3", "ema20_dist_atr",
            "ema_stack_atr", "macd_hist_atr", "bb_pos", "bb_width_atr", "stoch_k", "mfi14",
            "obv_slope6", "vol_z", "sess_vwap_dist_atr", "below_vwap_streak6", "day_pos",
            "day_low_dist_atr", "bars_since_day_low", "gap_pct", "day_ret_pct", "ret6_atr",
            "red_streak", "body_sum6_atr", "range6_atr", "range_expansion",
            "upper_wick_pct", "lower_wick_pct", "wick_skew_pct", "signal_range_pct"]
BASE_FEATS = ["rs_pct", "vol_ratio", "atr_pct", "body_pct", "close_loc", "vwap_dist_atr",
              "quality_score", "signal_range_pct", "upper_wick_pct", "lower_wick_pct",
              "wick_skew_pct"]
PM_FEATS = list(eng.PM_FEATS)
QGRID = list(eng.QGRID)
SL_GRID = [0.70, 1.00, 1.20, 1.50, 2.00]
TGT_GRID = [0.60, 0.80, 1.00, 1.50, 2.00, 2.50]
MIN_SLOTS = ["09:45", "10:00", "10:30", "11:00", "12:00"]
MAX_SLOTS = ["11:30", "12:00", "13:00", "14:00", "14:30"]


def evalm(setup, cfg, df, full=False):
    tt.MAX_POSITIONS = cfg.get("max_positions", 20)
    tt.DAILY_LOSS_RS = cfg.get("daily_loss_rs", 0.0)
    return (eng.full_metrics if full else eng.fast_metrics)(setup, cfg, df)


def eval_fast(setup, cfg, df):
    tt.MAX_POSITIONS = cfg.get("max_positions", 20)
    tt.DAILY_LOSS_RS = cfg.get("daily_loss_rs", 0.0)
    return eng._eval_fast(setup, cfg, df)


def band_score(pf_f, pf_v, nf, nv, fv_floor=10, gap_lambda=0.80):
    if nf < fv_floor or nv < fv_floor:
        return -5.0 + min(nf, nv) / max(1, fv_floor)
    cf, cv = eng._clamp_pf(pf_f), eng._clamp_pf(pf_v)
    sc = eng.band_reward(min(cf, cv)) - gap_lambda * abs(cf - cv)
    if min(cf, cv) >= PF_LO:
        sc += 0.003 * min(min(nf, nv), 40)
    return sc


def terms_str(terms):
    return "; ".join(f"{a}{o}{b}" for a, o, b in terms) or "(none)"


def dom_ok(m):
    return (m["trade_dom_gross"] is not None and m["trade_dom_gross"] <= TRADE_DOM_CAP
            and m["day_dom"] is not None and m["day_dom"] <= DAY_DOM_CAP
            and m["sym_dom"] is not None and m["sym_dom"] <= SYM_DOM_CAP)


def acceptance(mTR, mTE, robust):
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
        hard.append("TRAIN domination")
    if mTE["n"] < MIN_TEST_TRADES:
        hard.append(f"TEST n {mTE['n']} < {MIN_TEST_TRADES}")
    else:
        if mTE["net_pf"] <= TEST_PF_MIN:
            hard.append(f"TEST PF {mTE['net_pf']} <= {TEST_PF_MIN}")
        if mTE["net_pnl"] <= 0:
            hard.append("TEST net PnL not positive")
        if not dom_ok(mTE):
            hard.append("TEST domination")
        if mTE["day_block_p"] is None or not np.isfinite(float(mTE["day_block_p"])):
            warn.append("TEST day-block p unavailable")
        elif mTE["day_block_p"] > DAY_BLOCK_P_MAX:
            hard.append(f"TEST day-block p {mTE['day_block_p']} > {DAY_BLOCK_P_MAX}")
        if mTE["trades_per_day"] > MAX_TRADES_DAY:
            hard.append("TEST trades/day above cap")
        if mTE["n"] < 20:
            warn.append(f"TEST n {mTE['n']} < 20 (thin)")
    if robust is not None:
        if not robust["neighbor_pass"]:
            hard.append("neighborhood robustness failed")
        if not robust["dropout_pass"]:
            hard.append("term-dropout robustness failed")
    if mTR["net_pf"] > 1.70:
        warn.append("TRAIN PF in upper band (1.70-1.80)")
    return (len(hard) == 0), hard, warn


def mline(m):
    return (f"n={m['n']} PF={m['net_pf']} net=Rs{m['net_pnl']:,.0f} win%={m['win_rate']} "
            f"tgt%={m['target_rate']} tpd={m['trades_per_day']} tradeDom={m['trade_dom_gross']} "
            f"dayDom={m['day_dom']} symDom={m['sym_dom']} dbp={m['day_block_p']}")


def load_tt_pool(pool_dir, setup):
    tt.POOL_DIRS = [Path(pool_dir)]
    tt.POOL_DIR = Path(pool_dir)
    p = tt.load_pool()
    return p[p["setup"] == setup].copy()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--trials_per_variant", type=int, default=150)
    ap.add_argument("--minutes_per_variant", type=float, default=8.0)
    ap.add_argument("--seed", type=int, default=21)
    ap.add_argument("--fv_floor", type=int, default=10)
    ap.add_argument("--gap_lambda", type=float, default=0.80)
    ap.add_argument("--max_test_evals", type=int, default=10)
    ap.add_argument("--n_finalists", type=int, default=10)
    ap.add_argument("--min_trades_train", type=int, default=MIN_TRAIN_TRADES)
    ap.add_argument("--neighborhood_pf_min", type=float, default=1.15)
    ap.add_argument("--dropout_pf_min", type=float, default=1.00)
    args = ap.parse_args()
    try:
        sys.stdout.reconfigure(line_buffering=True)
    except Exception:
        pass

    engine_name = "Optuna TPE" if _HAVE_OPTUNA else "seeded random search"
    if not _HAVE_OPTUNA:
        print("Optuna unavailable; using seeded random search fallback.")
    print(f"[rec] recovery loop — optimizer {engine_name}")

    # MFE/MAE-derived exit pairs (TRAIN-only study output)
    mfe_pairs = []
    wl_path = WORK / "winner_loser_stats.json"
    if wl_path.exists():
        wl = json.loads(wl_path.read_text(encoding="utf-8"))
        tgts = [round(x, 2) for x in wl["suggested_exits"]["tgt_candidates_pct"]]
        sls = [round(x, 2) for x in wl["suggested_exits"]["sl_candidates_pct"]]
        mfe_pairs = [(s, t) for s in sls for t in tgts if 0.3 <= t <= 3.0 and 0.4 <= s <= 3.0]
        print(f"[rec] MFE/MAE exit pairs: {mfe_pairs}")

    eng._set_slippage(15.0)

    # ---- load pools ---------------------------------------------------------------
    pools = {}
    pools["BASELINE"] = (SETUP0, load_tt_pool(WORK / "pools" / SETUP0, SETUP0))
    rx = load_tt_pool(WORK / "pools" / "redesigned" / "AMOD_RX2", "AMOD_RX2")
    rt = load_tt_pool(WORK / "pools" / "redesigned" / "AMOD_RETEST", "AMOD_RETEST")
    print(f"[rec] pools: baseline {len(pools['BASELINE'][1])}, RX2 {len(rx)}, RETEST {len(rt)}")

    # tractability caps (documented in reports): the raw redesigned universe is ~146k
    # events (the scanner's incidental gates cut ~83%). We keep the DEEPEST break per
    # ticker-day (mirrors the family dedupe that keeps one ticker-day trade anyway),
    # then a SEEDED RANDOM sample per window — the same unbiased-sampling precedent as
    # the original mined pool (amod_mine_gen.py, random_state=7).
    def cap_pool(df, n_train=20000, n_test=8000):
        df = df.copy()
        df["_qs"] = pd.to_numeric(df["quality_score"], errors="coerce").fillna(0.0)
        df = (df.sort_values("_qs", ascending=False)
              .drop_duplicates(subset=["ticker", "_day"], keep="first"))
        tr = df[(df["_day"] >= pd.Timestamp(TRAIN_W[0])) & (df["_day"] <= pd.Timestamp(TRAIN_W[1]))]
        te = df[(df["_day"] >= pd.Timestamp(TEST_W[0])) & (df["_day"] <= pd.Timestamp(TEST_W[1]))]
        if len(tr) > n_train:
            tr = tr.sample(n_train, random_state=7)
        if len(te) > n_test:
            te = te.sample(n_test, random_state=7)
        return pd.concat([tr, te], ignore_index=True).drop(columns=["_qs"])

    rx = cap_pool(rx)
    rt = cap_pool(rt, n_train=14000, n_test=6000)
    print(f"[rec] capped: RX2 {len(rx)} (1/ticker-day deepest + seeded sample), RETEST {len(rt)}")

    # window split (sessions from the union of pools)
    all_days = sorted(set(pd.concat([pools["BASELINE"][1]["_day"], rx["_day"], rt["_day"]]).dropna().unique()))
    TRAIN_s = [s for s in all_days if pd.Timestamp(TRAIN_W[0]) <= s <= pd.Timestamp(TRAIN_W[1])]
    TEST_s = [s for s in all_days if pd.Timestamp(TEST_W[0]) <= s <= pd.Timestamp(TEST_W[1])]
    n_fit = max(1, int(round(len(TRAIN_s) * FIT_FRAC)))
    FIT_s, VAL_s = TRAIN_s[:n_fit], TRAIN_s[n_fit:]
    print(f"[rec] TRAIN {len(TRAIN_s)} sessions (FIT {len(FIT_s)}/VAL {len(VAL_s)}) | TEST {len(TEST_s)}")

    def attach(df):
        span = set(map(pd.Timestamp, TRAIN_s + TEST_s))
        sub = df[df["_day"].isin(span)].copy()
        return tt.attach_entries(sub)

    t0 = time.time()
    frames = {}
    frames["BASELINE"] = attach(pools["BASELINE"][1])
    rx_at = attach(rx)
    rt_at = attach(rt)
    print(f"[rec] entries attached in {time.time()-t0:.0f}s "
          f"(baseline {len(frames['BASELINE'])}, RX2 {len(rx_at)}, RETEST {len(rt_at)})")

    def flag_slice(df, col):
        return df[pd.to_numeric(df[col], errors="coerce").fillna(0) >= 1].copy()

    variants = {
        "RX2_ALL": (rx_at, None),
        "RX2_FRESHLOW": (flag_slice(rx_at, "flag_fresh_low"), None),
        "RX2_CONFIRM2": (flag_slice(rx_at, "flag_confirm2"), None),
        "RX2_DEEP": (flag_slice(rx_at, "flag_deep"), None),
        "RX2_FIRST_MORN": (flag_slice(rx_at, "flag_first"), {"max_slot": "12:00"}),
        "RX2_MKT": (flag_slice(rx_at, "nifty_below_ema20"), None),
        "RETEST": (rt_at, None),
    }

    def _slices(df):
        def _s(ss):
            return df[df["_day"].isin(set(map(pd.Timestamp, ss)))].copy()
        return {"FIT": _s(FIT_s), "VAL": _s(VAL_s), "TRAIN": _s(TRAIN_s), "TEST": _s(TEST_s)}

    V = {}
    V["BASELINE"] = ("A_MOD_BREAK_C1_LOW", _slices(frames["BASELINE"]), None, BASE_FEATS)
    for name, (sub, fixed_guard) in variants.items():
        setup = sub["setup"].iloc[0] if len(sub) else "AMOD_RX2"
        V[name] = (setup, _slices(sub), fixed_guard, RX_FEATS)
        print(f"[rec] variant {name}: FIT={len(V[name][1]['FIT'])} VAL={len(V[name][1]['VAL'])} "
              f"TRAIN={len(V[name][1]['TRAIN'])} TEST={len(V[name][1]['TEST'])}")

    # premom quantiles once, from RX2 TRAIN sample (shared across variants)
    pm_src = V["RX2_ALL"][1]["TRAIN"]
    if len(pm_src) > 1200:
        pm_src = pm_src.sample(1200, random_state=args.seed).sort_index()
    pm_recs = []
    for j, r in enumerate(pm_src.itertuples(), 1):
        feats, reason = tt._premom(r.ticker, r.side, r.tt_entry_iso, float(r.tt_fill), 1.0, r.tt_sig_ts.isoformat())
        fd = dict(feats) if not reason else {}
        pm_recs.append({f: fd.get(f, np.nan) for f in PM_FEATS})
        if j % 400 == 0:
            print(f"[rec] premom quantiles {j}/{len(pm_src)}", flush=True)
    pm_df = pd.DataFrame(pm_recs)
    pm_quant = {}
    for f in PM_FEATS:
        s = pd.to_numeric(pm_df[f], errors="coerce").dropna()
        if len(s) >= 8 and s.nunique() > 1:
            pm_quant[f] = {q: float(s.quantile(q)) for q in QGRID}

    iter_rows, trial_rows = [], []
    it_no = [0]

    def log_iter(stage, variant, group, change, cfg, mF=None, mV=None, mTR=None, mTE=None, keep="", why=""):
        it_no[0] += 1
        iter_rows.append({"iter": it_no[0], "stage": stage, "variant": variant, "group": group,
                          "change": change, "sl": cfg.get("sl"), "tgt": cfg.get("tgt"),
                          "mask": terms_str(cfg.get("mask_terms", [])),
                          "premom": terms_str(cfg.get("premom_terms", [])),
                          "guard": json.dumps(cfg.get("guard")) if cfg.get("guard") else "-",
                          "fit_n": (mF or {}).get("n"), "fit_pf": (mF or {}).get("net_pf"),
                          "val_n": (mV or {}).get("n"), "val_pf": (mV or {}).get("net_pf"),
                          "train_n": (mTR or {}).get("n"), "train_pf": (mTR or {}).get("net_pf"),
                          "train_net": (mTR or {}).get("net_pnl"),
                          "test_n": (mTE or {}).get("n"), "test_pf": (mTE or {}).get("net_pf"),
                          "test_net": (mTE or {}).get("net_pnl"), "keep": keep, "why": why})

    # ---- Stage 2: baseline anchor ----------------------------------------------------
    base_block, base_src = eng.get_baseline_block(SETUP0)
    base_cfg = eng.conf_to_cfg(base_block)
    setup, sl_b, gd, feats_b = V["BASELINE"]
    mB = {k: evalm(setup, base_cfg, sl_b[k], full=(k in ("TRAIN", "TEST"))) for k in ("FIT", "VAL", "TRAIN", "TEST")}
    for k in ("TRAIN", "TEST"):
        det = mB[k].get("detail")
        if det is not None and not det.empty:
            det.to_csv(WORK / f"baseline_trades_{k.lower()}.csv", index=False)
    print(f"[rec] BASELINE TRAIN {mline(mB['TRAIN'])}")
    print(f"[rec] BASELINE TEST  {mline(mB['TEST'])}")
    log_iter("2-baseline", "BASELINE", "conf config", "-", base_cfg, mB["FIT"], mB["VAL"], mB["TRAIN"], mB["TEST"],
             keep="anchor", why="production config on recreated pool")

    best_global = {"score": -1e9, "cfg": None, "variant": None}
    seen = {}

    def score_record(variant, setup, cfg, sl):
        k = variant + "|" + json.dumps({"sl": cfg["sl"], "tgt": cfg["tgt"],
                                        "m": sorted(map(list, cfg["mask_terms"])),
                                        "p": sorted(map(list, cfg["premom_terms"])),
                                        "g": cfg["guard"] or {}}, default=str)
        if k in seen:
            return seen[k]
        try:
            nf, pf_f, _ = eval_fast(setup, cfg, sl["FIT"])
            nv, pf_v, _ = eval_fast(setup, cfg, sl["VAL"])
        except Exception:
            seen[k] = -9.0
            return -9.0
        sc = band_score(pf_f, pf_v, nf, nv, args.fv_floor, args.gap_lambda)
        trial_rows.append({"variant": variant, "sl": cfg["sl"], "tgt": cfg["tgt"],
                           "mask": terms_str(cfg["mask_terms"]), "premom": terms_str(cfg["premom_terms"]),
                           "guard": json.dumps(cfg["guard"]) if cfg["guard"] else "-",
                           "fit_n": nf, "fit_pf": round(pf_f, 3), "val_n": nv, "val_pf": round(pf_v, 3),
                           "score": round(float(sc), 4), "cfg_json": k})
        if sc > best_global["score"]:
            best_global.update(score=sc, cfg=eng._copy_cfg(cfg), variant=variant)
        seen[k] = sc
        return sc

    exit_pairs = sorted(set([(s, t) for s in SL_GRID for t in TGT_GRID][:0]
                            + [(1.0, 0.8), (1.0, 1.0), (1.2, 1.0), (1.5, 1.5), (1.5, 2.5),
                               (0.7, 0.8), (2.0, 2.5), (1.0, 1.5), (1.2, 2.0)]
                            + mfe_pairs))

    # ---- per-variant stages A/B/C ------------------------------------------------------
    variant_best_exit = {}
    for vname, (setup, sl, fixed_guard, feat_list) in V.items():
        if vname == "BASELINE":
            continue
        if len(sl["FIT"]) < 50 or len(sl["VAL"]) < 30:
            print(f"[rec] {vname}: too thin (FIT {len(sl['FIT'])}) — skipped")
            continue
        # quantile grid per variant (TRAIN only)
        mq = {}
        for f in feat_list:
            if f in sl["TRAIN"].columns:
                s = pd.to_numeric(sl["TRAIN"][f], errors="coerce").dropna()
                if len(s) >= 8 and s.nunique() > 1:
                    mq[f] = {q: float(s.quantile(q)) for q in QGRID}

        # A: exit-pair scan (ungated)
        best_pair, best_pair_sc = None, -1e9
        for slp, tgp in exit_pairs:
            cfg = {"sl": slp, "tgt": tgp, "mask_terms": [], "premom_terms": [],
                   "guard": (dict(fixed_guard) if fixed_guard else None), "status": "OK",
                   "max_positions": 20, "daily_loss_rs": 0.0}
            sc = score_record(vname, setup, cfg, sl)
            if sc > best_pair_sc:
                best_pair_sc, best_pair = sc, (slp, tgp)
            r = trial_rows[-1] if trial_rows else {}
            log_iter("A-exits", vname, "exit", f"{slp}/{tgp}", cfg,
                     {"n": r.get("fit_n"), "net_pf": r.get("fit_pf")},
                     {"n": r.get("val_n"), "net_pf": r.get("val_pf")},
                     keep="-", why=f"score {sc:.3f}")
        variant_best_exit[vname] = best_pair
        print(f"[rec] {vname}: best ungated exit {best_pair} (score {best_pair_sc:.3f})")

        # B: single-feature scan at best exit
        for f in sorted(mq):
            for q in (0.2, 0.5, 0.8):
                thr = round(mq[f][q], 6)
                for op in (">=", "<="):
                    cfg = {"sl": best_pair[0], "tgt": best_pair[1],
                           "mask_terms": [(f, op, thr)], "premom_terms": [],
                           "guard": (dict(fixed_guard) if fixed_guard else None), "status": "OK",
                           "max_positions": 20, "daily_loss_rs": 0.0}
                    score_record(vname, setup, cfg, sl)

        # C: TPE combination search
        def _suggest(trial):
            mask_terms = []
            for i in range(trial.suggest_int("n_mask", 0, 2)):
                f = trial.suggest_categorical(f"m{i}_f", sorted(mq))
                op = trial.suggest_categorical(f"m{i}_o", [">=", "<="])
                q = trial.suggest_categorical(f"m{i}_q", QGRID)
                mask_terms.append((f, op, round(float(mq[f][q]), 6)))
            rg = trial.suggest_categorical("regime", ["none", "==BEAR", "!=BULL"])
            if rg != "none":
                mask_terms.append(("regime", rg[:2], rg[2:]))
            premom_terms = []
            if trial.suggest_int("n_pm", 0, 1):
                f = trial.suggest_categorical("pm_f", sorted(pm_quant))
                op = trial.suggest_categorical("pm_o", [">=", "<="])
                q = trial.suggest_categorical("pm_q", QGRID)
                premom_terms.append((f, op, round(float(pm_quant[f][q]), 6)))
            guard = dict(fixed_guard) if fixed_guard else {}
            if trial.suggest_categorical("use_min_slot", [False, True]):
                guard["min_slot"] = trial.suggest_categorical("min_slot", MIN_SLOTS)
            if "max_slot" not in guard and trial.suggest_categorical("use_max_slot", [False, True]):
                guard["max_slot"] = trial.suggest_categorical("max_slot", MAX_SLOTS)
            tn = trial.suggest_categorical("top_n", [0, 1, 2, 3])
            if tn:
                guard["top_n"] = int(tn)
            return {"sl": float(trial.suggest_categorical("sl", SL_GRID)),
                    "tgt": float(trial.suggest_categorical("tgt", TGT_GRID)),
                    "mask_terms": mask_terms, "premom_terms": premom_terms,
                    "guard": (guard or None), "status": "OK",
                    "max_positions": int(trial.suggest_categorical("maxpos", [10, 20])),
                    "daily_loss_rs": 0.0}
        if _HAVE_OPTUNA:
            st = optuna.create_study(direction="maximize",
                                     sampler=optuna.samplers.TPESampler(seed=args.seed))
            st.optimize(lambda tr: score_record(vname, setup, _suggest(tr), sl),
                        n_trials=args.trials_per_variant,
                        timeout=args.minutes_per_variant * 60.0)
        print(f"[rec] {vname}: stage C done ({len(trial_rows)} cumulative trials)")

    tdf = pd.DataFrame(trial_rows).sort_values("score", ascending=False).reset_index(drop=True)
    tdf.to_csv(WORK / "trials_recovery.csv", index=False)
    print(f"[rec] total unique configs scored: {len(tdf)}")

    # ---- Stage D: cross-variant finalists -> TRAIN -> TEST once ------------------------
    finalists, fam_seen, test_evals = [], set(), 0
    for _, r in tdf.iterrows():
        if len(finalists) >= args.n_finalists or float(r["score"]) < 0:
            break
        vname = r["variant"]
        k = r["cfg_json"]
        raw = json.loads(k.split("|", 1)[1])
        cfg = {"sl": raw["sl"], "tgt": raw["tgt"],
               "mask_terms": [(f, op, (thr if isinstance(thr, str) else float(thr)))
                              for f, op, thr in (tuple(t) for t in raw["m"])],
               "premom_terms": [(f, op, float(thr)) for f, op, thr in (tuple(t) for t in raw["p"])],
               "guard": (raw["g"] or None), "status": "OK", "max_positions": 20, "daily_loss_rs": 0.0}
        fam = (vname, cfg["sl"], cfg["tgt"],
               frozenset((f, op) for f, op, _ in cfg["mask_terms"]),
               frozenset((f, op) for f, op, _ in cfg["premom_terms"]),
               frozenset((cfg["guard"] or {}).keys()))
        if fam in fam_seen:
            continue
        fam_seen.add(fam)
        finalists.append({"variant": vname, "cfg": cfg, "score": float(r["score"]),
                          "fit_pf": float(r["fit_pf"]), "val_pf": float(r["val_pf"])})

    results = []
    ns = argparse.Namespace(min_trades_train=args.min_trades_train,
                            neighborhood_pf_min=args.neighborhood_pf_min,
                            dropout_pf_min=args.dropout_pf_min)
    for fi, cand in enumerate(finalists, 1):
        vname, cfg = cand["variant"], cand["cfg"]
        setup, sl, fixed_guard, feat_list = V[vname]
        try:
            mTR = evalm(setup, cfg, sl["TRAIN"], full=True)
        except Exception as e:
            results.append({"id": fi, "variant": vname, "cfg": cfg, "passed": False,
                            "hard_reasons": [f"eval error {type(e).__name__}"], "warnings": []})
            continue
        in_band = PF_LO <= mTR["net_pf"] <= PF_HI and mTR["n"] >= MIN_TRAIN_TRADES
        rec = {"id": fi, "variant": vname, "cfg": cfg, "fitval_score": cand["score"],
               "train": {k2: v for k2, v in mTR.items() if k2 != "detail"}}
        if in_band and test_evals < args.max_test_evals:
            mq = {}
            for f in feat_list:
                if f in sl["TRAIN"].columns:
                    s = pd.to_numeric(sl["TRAIN"][f], errors="coerce").dropna()
                    if len(s) >= 8 and s.nunique() > 1:
                        mq[f] = {q: float(s.quantile(q)) for q in QGRID}
            robust = eng.robustness_report(setup, cfg, sl["TRAIN"], mq, pm_quant, ns)
            mTE = evalm(setup, cfg, sl["TEST"], full=True)
            test_evals += 1
            passed, hard, warn = acceptance(mTR, mTE, robust)
            rec.update({"test": {k2: v for k2, v in mTE.items() if k2 != "detail"},
                        "robust": {k2: robust[k2] for k2 in ("neighbor_pass", "dropout_pass", "passed")},
                        "passed": passed, "hard_reasons": hard, "warnings": warn})
            log_iter("D-finalist", vname, "combination", f"finalist #{fi}", cfg, None, None, mTR, mTE,
                     keep=("PASS" if passed else "reject"), why="; ".join(hard + warn) or "all gates passed")
            mTR["detail"].to_csv(WORK / f"rec_finalist_{fi:02d}_{vname}_train.csv", index=False)
            if not mTE["detail"].empty:
                mTE["detail"].to_csv(WORK / f"rec_finalist_{fi:02d}_{vname}_test.csv", index=False)
            print(f"[rec] finalist {fi} ({vname}): TRAIN {mline(mTR)}")
            print(f"[rec] finalist {fi} ({vname}): TEST  {mline(mTE)}")
        else:
            rec.update({"passed": False,
                        "hard_reasons": [f"TRAIN not in band or thin (PF {mTR['net_pf']}, n {mTR['n']})"
                                         if not in_band else "TEST budget exhausted"], "warnings": []})
            log_iter("D-finalist", vname, "combination", f"finalist #{fi}", cfg, None, None, mTR, None,
                     keep="reject", why=rec["hard_reasons"][0])
            print(f"[rec] finalist {fi} ({vname}): TRAIN {mline(mTR)} -> {rec['hard_reasons'][0]}")
        results.append(rec)

    # ---- Stage E: rescue ---------------------------------------------------------------
    passing = [r for r in results if r.get("passed")]
    if not passing and best_global["cfg"] is not None:
        print("[rec] rescue loop")
        vname = best_global["variant"]
        setup, sl, fixed_guard, feat_list = V[vname]
        bc = best_global["cfg"]
        rescue = []
        for i in range(len(bc["mask_terms"])):
            c = eng._copy_cfg(bc); mt = list(c["mask_terms"]); mt.pop(i); c["mask_terms"] = mt
            rescue.append((f"R-drop-mask-{i}", c))
        if bc["premom_terms"]:
            c = eng._copy_cfg(bc); c["premom_terms"] = []
            rescue.append(("R-premom-off", c))
        for g in ({"max_slot": "12:00"}, {"min_slot": "10:00"}):
            c = eng._copy_cfg(bc); gg = dict(c["guard"] or {}); gg.update(g); c["guard"] = gg
            rescue.append((f"R-window-{json.dumps(g)}", c))
        scored = []
        for tag, cfg in rescue:
            try:
                nf, pf_f, _ = eval_fast(setup, cfg, sl["FIT"])
                nv, pf_v, _ = eval_fast(setup, cfg, sl["VAL"])
            except Exception:
                continue
            scored.append((band_score(pf_f, pf_v, nf, nv, args.fv_floor, args.gap_lambda), tag, cfg))
        for sc, tag, cfg in sorted(scored, key=lambda x: -x[0]):
            if test_evals >= args.max_test_evals:
                break
            mTR = evalm(setup, cfg, sl["TRAIN"], full=True)
            in_band = PF_LO <= mTR["net_pf"] <= PF_HI and mTR["n"] >= MIN_TRAIN_TRADES
            if not in_band:
                log_iter("E-rescue", vname, tag, "rescue", cfg, None, None, mTR, None,
                         keep="reject", why=f"TRAIN out of band (PF {mTR['net_pf']}, n {mTR['n']})")
                continue
            mq = {}
            for f in feat_list:
                if f in sl["TRAIN"].columns:
                    s = pd.to_numeric(sl["TRAIN"][f], errors="coerce").dropna()
                    if len(s) >= 8 and s.nunique() > 1:
                        mq[f] = {q: float(s.quantile(q)) for q in QGRID}
            robust = eng.robustness_report(setup, cfg, sl["TRAIN"], mq, pm_quant, ns)
            mTE = evalm(setup, cfg, sl["TEST"], full=True)
            test_evals += 1
            passed, hard, warn = acceptance(mTR, mTE, robust)
            log_iter("E-rescue", vname, tag, "rescue", cfg, None, None, mTR, mTE,
                     keep=("PASS" if passed else "reject"), why="; ".join(hard + warn) or "all gates passed")
            rec = {"id": 300 + len(results), "variant": vname, "tag": tag, "cfg": cfg,
                   "train": {k2: v for k2, v in mTR.items() if k2 != "detail"},
                   "test": {k2: v for k2, v in mTE.items() if k2 != "detail"},
                   "robust": {k2: robust[k2] for k2 in ("neighbor_pass", "dropout_pass", "passed")},
                   "passed": passed, "hard_reasons": hard, "warnings": warn}
            results.append(rec)
            if passed:
                passing.append(rec)
                break

    # ---- persist ------------------------------------------------------------------------
    pd.DataFrame(iter_rows).to_csv(WORK / "iteration_log_recovery.csv", index=False)
    summary = {
        "generated": TODAY, "campaign": "from_scratch_recovery", "setup": SETUP0,
        "optimizer": engine_name,
        "windows": {"TRAIN": f"{pd.Timestamp(TRAIN_s[0]).date()}..{pd.Timestamp(TRAIN_s[-1]).date()}",
                    "TEST": f"{pd.Timestamp(TEST_s[0]).date()}..{pd.Timestamp(TEST_s[-1]).date()}",
                    "n_train_sessions": len(TRAIN_s), "n_test_sessions": len(TEST_s),
                    "FIT_n": len(FIT_s), "VAL_n": len(VAL_s)},
        "variant_rows": {k: {w: len(v[1][w]) for w in ("FIT", "VAL", "TRAIN", "TEST")} for k, v in V.items()},
        "variant_best_exit": {k: list(v) for k, v in variant_best_exit.items() if v},
        "baseline_src": base_src,
        "baseline_metrics": {k: {k2: v for k2, v in m.items() if k2 != "detail"} for k, m in mB.items()},
        "n_trials": int(len(tdf)), "n_test_evals": test_evals,
        "best_global": {"variant": best_global["variant"], "score": best_global["score"],
                        "cfg": (eng.cfg_to_conf_block(SETUP0, "SHORT", best_global["cfg"])
                                if best_global["cfg"] else None)},
        "results": [{k2: (v if k2 != "cfg" else eng.cfg_to_conf_block(SETUP0, "SHORT", v))
                     for k2, v in r.items()} for r in results],
        "n_passing": len(passing),
    }
    (WORK / "run_summary_recovery.json").write_text(
        json.dumps(tt._json_sanitize(summary), indent=2, default=str), encoding="utf-8")
    print(f"[rec] DONE — {len(passing)} passing candidate(s); {len(iter_rows)} logged iterations; "
          f"{len(tdf)} scored configs")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
