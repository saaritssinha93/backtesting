r"""exit_regime_walkforward_diagnostics.py - the three honest follow-ups.

Answers, with numbers off the cached raw-discovery tables, the three things the
TRAIN-PF optimizer could NOT tell us about FAST_MOMENTUM_LONG
(rule LONG_VOLUME_EXPANSION_BREAKOUT_vol2_h5):

  PART A - EXIT ASYMMETRY / BREAK-EVEN SENSITIVITY
      For the base population AND the train-PF "optimized" pocket, sweep all 19
      cached exit brackets and report, per exit, the empirical avg-win / avg-loss,
      the implied break-even win-rate (the bar the setup must clear just to not
      lose), the actual win-rate, the margin, and PF - on TRAIN and on TEST.
      This shows whether a better exit can lift the *whole* base population OOS,
      which is the only honest way an exit fix counts.

  PART B - WALK-FORWARD (the honest judge)
      Anchored expanding folds across all 40 sessions. Per fold we (i) score the
      raw base setup unconditionally and (ii) re-run a constrained filter search
      on that fold's TRAIN and score the winner on its unseen TEST. A real edge
      holds across folds; a single-window artifact does not.

  PART C - REGIME CONDITIONING
      Build a CAUSAL NIFTY50 market-regime feature (trend-from-open, above-EMA20,
      EMA20>EMA50, 30m momentum) joined at each signal bar, then test whether
      conditioning the base setup on "market up" generalizes - both as a single
      split and, crucially, across the walk-forward folds. (walk_forward.py bans
      market_ret as an overfit vector; so regime only earns credit if it holds
      fold-after-fold, never on one split.)

Everything runs off the cached parquet (candidates / rule_candidates / exits) plus
the NIFTY50 index 5m store. No 1-minute re-resolution, no live-feed pressure.

Run from repo root:
  py -3.12 Train_and_Test/long_setup_discovery_from_raw_data/scripts/exit_regime_walkforward_diagnostics.py
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))

import optimize_fast_momentum_long_train_pf as opt  # noqa: E402  reuse primitives

OUT_DIR = opt.OUT_DIR
RESULTS_DIR = opt.RESULTS_DIR
RULE_ID = opt.RULE_ID
NIFTY_PATH = Path(r"C:/TradingData/eqidv2/stocks_indicators_5min_eq_live2/NIFTY50_INDEX_stocks_indicators_5min.parquet")

# Walk-forward shape (index-based over the 40 sorted sessions).
MIN_TRAIN_SESS = 15
TEST_SESS = 5
STEP_SESS = 5
# Per-fold filter search (kept deliberately shallow - deep search = overfit).
WF_DEPTH = 3
WF_BEAM = 120
WF_TRAIN_PF_MIN = 1.30
WF_TRAIN_PF_MAX = 2.00
WF_MIN_TRAIN_TRADES = 25
WF_MIN_TEST_TRADES = 6


# ----------------------------------------------------------------------------- helpers
def pnl_for_exit(df: pd.DataFrame, exits: pd.DataFrame, exit_id: str) -> np.ndarray:
    """Costed net_pnl per df row for one exit bracket (NaN where unresolved)."""
    return (
        exits[exits["exit_id"].eq(exit_id)]
        .drop_duplicates("candidate_id")
        .set_index("candidate_id")["net_pnl"]
        .reindex(df["candidate_id"])
        .to_numpy(dtype=float)
    )


def stat_pack(pnl: np.ndarray, mask: np.ndarray) -> dict:
    """Empirical edge stats incl. the break-even win-rate the setup must clear."""
    v = pnl[mask & np.isfinite(pnl)]
    n = int(len(v))
    if n == 0:
        return dict(n=0, wr=0.0, avg_win=0.0, avg_loss=0.0, be_wr=0.0, margin=0.0, pf=0.0, net=0.0, expectancy=0.0)
    pos, neg = v[v > 0], v[v < 0]
    gp, gl = float(pos.sum()), float(-neg.sum())
    avg_w = float(pos.mean()) if len(pos) else 0.0
    avg_l = float(-neg.mean()) if len(neg) else 0.0          # positive magnitude
    wr = float((v > 0).mean())
    be_wr = avg_l / (avg_w + avg_l) if (avg_w + avg_l) > 0 else 1.0
    return dict(
        n=n, wr=round(wr, 4), avg_win=round(avg_w, 1), avg_loss=round(avg_l, 1),
        be_wr=round(be_wr, 4), margin=round(wr - be_wr, 4),
        pf=round(gp / gl, 4) if gl > 0 else (999.0 if gp > 0 else 0.0),
        net=round(gp - gl, 1), expectancy=round(float(v.mean()), 1),
    )


def mask_from_predicates(df: pd.DataFrame, predicates: list[str], top_n) -> np.ndarray:
    bank = dict(opt.predicate_bank(df))
    m = np.ones(len(df), dtype=bool)
    for p in predicates:
        if p in bank:
            m &= bank[p]
    return opt.topn_mask(df, m, top_n)


def session_index(df: pd.DataFrame) -> list[pd.Timestamp]:
    return sorted(pd.to_datetime(df["session"]).dt.normalize().unique())


def make_folds(sessions: list[pd.Timestamp]) -> list[dict]:
    folds = []
    i = MIN_TRAIN_SESS
    while i + TEST_SESS <= len(sessions):
        folds.append({
            "train": set(sessions[:i]),
            "test": set(sessions[i:i + TEST_SESS]),
            "train_lbl": f"{sessions[0].date()}..{sessions[i-1].date()}",
            "test_lbl": f"{sessions[i].date()}..{sessions[i+TEST_SESS-1].date()}",
        })
        i += STEP_SESS
    return folds


def fold_filter_search(df: pd.DataFrame, pnl: np.ndarray, train_mask: np.ndarray,
                       extra_preds: list[tuple[str, np.ndarray]] | None = None) -> dict | None:
    """Shallow beam search for the best TRAIN filter pocket within the PF band.

    Returns the pocket predicates + train stats, or None if nothing clears the
    band with enough trades. `extra_preds` lets PART C inject regime predicates
    into the very same search universe."""
    n = len(df)
    preds = opt.predicate_bank(df) + list(extra_preds or [])
    states: list[tuple[np.ndarray, list[str]]] = [(np.ones(n, dtype=bool), [])]
    seen = {""}
    best = None
    for _ in range(WF_DEPTH):
        scored = []
        for mask, names in states:
            used = set(names)
            for name, pmask in preds:
                if name in used:
                    continue
                nxt = mask & pmask
                if int((nxt & train_mask).sum()) < WF_MIN_TRAIN_TRADES:
                    continue
                key = "|".join(sorted(names + [name]))
                if key in seen:
                    continue
                seen.add(key)
                for top_n in (None, 2, 3, 5):
                    fm = opt.topn_mask(df, nxt, top_n)
                    tr = opt.quick_metrics(fm, pnl, train_mask)
                    if tr["trades"] < WF_MIN_TRAIN_TRADES or tr["net_pf"] < WF_TRAIN_PF_MIN:
                        continue
                    # prefer pockets inside the band, trade-rich, then higher PF
                    in_band = WF_TRAIN_PF_MIN <= tr["net_pf"] <= WF_TRAIN_PF_MAX
                    sc = (1 if in_band else 0) * 1e6 + min(tr["trades"], 200) * 100 + tr["net_pf"]
                    cand = {"predicates": names + [name], "top_n": top_n,
                            "train_pf": tr["net_pf"], "train_n": tr["trades"], "_mask": nxt}
                    if best is None or sc > best["_sc"]:
                        best = {**cand, "_sc": sc}
                scored.append((tr["net_pf"], nxt, names + [name]))
        scored.sort(key=lambda x: x[0], reverse=True)
        states = [(m, nm) for _, m, nm in scored[:WF_BEAM]]
        if not states:
            break
    if best:
        best.pop("_mask", None)
        best.pop("_sc", None)
    return best


def verdict(frac_pos: float, med_test: float, n_eval: int, min_folds: int = 3) -> str:
    if n_eval < min_folds:
        return "INSUFFICIENT_DATA"
    if frac_pos >= 0.60 and med_test >= 1.30:
        return "ROBUST"
    if frac_pos >= 0.40 or med_test >= 1.10:
        return "FRAGILE"
    return "DEAD"


# ----------------------------------------------------------------------------- regime
def load_regime() -> pd.DataFrame:
    """Causal NIFTY50 regime indexed by 5m signal-bar timestamp."""
    n = pd.read_parquet(NIFTY_PATH)
    n["date"] = opt.pd.to_datetime(n["date"], errors="coerce")
    try:
        if n["date"].dt.tz is not None:
            n["date"] = n["date"].dt.tz_convert("Asia/Kolkata").dt.tz_localize(None)
    except Exception:
        pass
    n = n.dropna(subset=["date"]).sort_values("date").drop_duplicates("date")
    n["sess"] = n["date"].dt.normalize()
    for c in ("close", "open", "EMA_20", "EMA_50"):
        n[c] = pd.to_numeric(n.get(c), errors="coerce")
    g = n.groupby("sess", group_keys=False)
    sess_open = g["open"].transform("first")
    n["mkt_ret_open_pct"] = (n["close"] / sess_open - 1.0) * 100.0      # day trend so far
    n["mkt_above_ema20"] = (n["close"] > n["EMA_20"]).astype(float)
    n["mkt_ema_stack"] = (n["EMA_20"] > n["EMA_50"]).astype(float)
    n["mkt_mom_30m_pct"] = (n["close"] / g["close"].shift(6) - 1.0) * 100.0
    keep = ["date", "mkt_ret_open_pct", "mkt_above_ema20", "mkt_ema_stack", "mkt_mom_30m_pct"]
    return n[keep].rename(columns={"date": "_sig"})


def attach_regime(df: pd.DataFrame, regime: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["_sig"] = pd.to_datetime(df["date"]).dt.tz_localize(None) if pd.to_datetime(df["date"]).dt.tz is not None \
        else pd.to_datetime(df["date"])
    out = df.merge(regime, on="_sig", how="left")
    return out


def regime_predicates(df: pd.DataFrame) -> list[tuple[str, np.ndarray]]:
    """Causal market-regime predicates for the search universe / direct gates."""
    n = len(df)
    out: list[tuple[str, np.ndarray]] = []

    def add(name, mask):
        a = np.asarray(mask, dtype=bool)
        if 10 <= int(a.sum()) < n:
            out.append((name, a))

    add("mkt_above_ema20", df["mkt_above_ema20"].fillna(0).gt(0.5))
    add("mkt_ema_stack", df["mkt_ema_stack"].fillna(0).gt(0.5))
    add("mkt_trend_up", (df["mkt_above_ema20"].fillna(0).gt(0.5)) & (df["mkt_ema_stack"].fillna(0).gt(0.5)))
    for t in (-0.3, 0.0, 0.2, 0.5):
        add(f"mkt_ret_open>={t:g}", df["mkt_ret_open_pct"].ge(t))
    for t in (-0.2, 0.0, 0.15):
        add(f"mkt_mom_30m>={t:g}", df["mkt_mom_30m_pct"].ge(t))
    return out


# ----------------------------------------------------------------------------- parts
def part_a(df: pd.DataFrame, exits: pd.DataFrame, splits: dict, pocket: dict) -> tuple[pd.DataFrame, str]:
    exit_ids = sorted(exits["exit_id"].unique())
    base_mask = np.ones(len(df), dtype=bool)
    pock_mask = mask_from_predicates(df, pocket["predicates"], pocket["top_n"])
    rows = []
    for exit_id in exit_ids:
        pnl = pnl_for_exit(df, exits, exit_id)
        for pop, m in (("BASE", base_mask), ("POCKET", pock_mask)):
            for split in ("train", "test"):
                sm = df["session"].isin(splits[split]).to_numpy()
                s = stat_pack(pnl, m & sm)
                rows.append({"exit_id": exit_id, "population": pop, "split": split.upper(), **s})
    tab = pd.DataFrame(rows)

    # Which exit best lifts the BASE population OOS?
    base_test = tab[(tab.population == "BASE") & (tab.split == "TEST")].sort_values("pf", ascending=False)
    md = ["## PART A - Exit asymmetry / break-even sensitivity", "",
          "`be_wr` = win-rate the bracket needs just to break even (= avgLoss/(avgWin+avgLoss), "
          "costs baked in). `margin` = actual WR - be_wr. A real edge shows a POSITIVE margin on the "
          "**base population** (no overfit filter), OOS.", "",
          "### Base population (all vol2_h5 breakouts), TEST window, every bracket:", "",
          "| exit | n | WR | avg_win | avg_loss | be_WR | margin | PF | net |",
          "|---|--:|--:|--:|--:|--:|--:|--:|--:|"]
    for _, r in base_test.iterrows():
        md.append(f"| {r.exit_id} | {r.n} | {r.wr:.0%} | {r.avg_win:.0f} | {r.avg_loss:.0f} | "
                  f"{r.be_wr:.0%} | {r.margin:+.1%} | {r.pf:.2f} | {r.net:,.0f} |")
    best = base_test.iloc[0]
    md += ["", f"**Best OOS bracket for the base population: `{best.exit_id}`** "
           f"(PF {best.pf:.2f}, margin {best.margin:+.1%}). "
           "Read the margin column: if every bracket is negative, no exit redesign rescues this setup - "
           "the entry has no edge to harvest. If the best bracket flips positive, the exit was the bottleneck.", ""]
    return tab, "\n".join(md)


def part_b(df, exits, sessions, exit_id) -> tuple[dict, str]:
    pnl = pnl_for_exit(df, exits, exit_id)
    folds = make_folds(sessions)
    base_recs, srch_recs = [], []
    for k, f in enumerate(folds):
        tr_m = df["session"].isin(f["train"]).to_numpy()
        te_m = df["session"].isin(f["test"]).to_numpy()
        base_tr = opt.quick_metrics(np.ones(len(df), bool), pnl, tr_m)
        base_te = opt.quick_metrics(np.ones(len(df), bool), pnl, te_m)
        base_recs.append({"fold": k, "test_window": f["test_lbl"],
                          "train_pf": round(base_tr["net_pf"], 2), "train_n": base_tr["trades"],
                          "test_pf": round(base_te["net_pf"], 2), "test_n": base_te["trades"],
                          "test_net": round(base_te["net_pnl"], 0)})
        pk = fold_filter_search(df, pnl, tr_m)
        if pk is None:
            srch_recs.append({"fold": k, "test_window": f["test_lbl"], "train_pf": None,
                              "test_pf": None, "test_n": 0, "status": "no_pocket_in_band"})
            continue
        fm = mask_from_predicates(df, pk["predicates"], pk["top_n"])
        te = opt.quick_metrics(fm, pnl, te_m)
        srch_recs.append({"fold": k, "test_window": f["test_lbl"],
                          "train_pf": round(pk["train_pf"], 2), "train_n": pk["train_n"],
                          "test_pf": round(te["net_pf"], 2), "test_n": te["trades"],
                          "test_net": round(te["net_pnl"], 0),
                          "pocket": ", ".join(pk["predicates"]) + f" . top_n={pk['top_n']}"})

    def agg(recs):
        ev = [r for r in recs if r.get("test_pf") is not None and r.get("test_n", 0) >= WF_MIN_TEST_TRADES]
        pos = [r for r in ev if r["test_pf"] >= 1.3]
        med = float(np.median([r["test_pf"] for r in ev])) if ev else 0.0
        frac = (len(pos) / len(ev)) if ev else 0.0
        return {"folds_eval": len(ev), "folds_pos": len(pos), "frac_pos": round(frac, 2),
                "median_test_pf": round(med, 2), "verdict": verdict(frac, med, len(ev))}

    res = {"exit_id": exit_id, "n_folds": len(folds),
           "base_unconditional": {"detail": base_recs, **agg(base_recs)},
           "filter_search": {"detail": srch_recs, **agg(srch_recs)}}

    md = ["## PART B - Walk-forward (the honest judge)", "",
          f"Anchored expanding folds (min train {MIN_TRAIN_SESS} sessions, test {TEST_SESS}, step {STEP_SESS}); "
          f"exit `{exit_id}`. A setup is ROBUST only if >=60% of folds test PF>=1.3 AND median test PF>=1.3.", "",
          "### B1 - base setup, unconditional (no filter) per fold:", "",
          "| fold | test window | train PF | test PF | test n | test net |",
          "|--:|---|--:|--:|--:|--:|"]
    for r in base_recs:
        md.append(f"| {r['fold']} | {r['test_window']} | {r['train_pf']} | {r['test_pf']} | {r['test_n']} | {r['test_net']:,.0f} |")
    a = res["base_unconditional"]
    md += ["", f"-> folds positive {a['folds_pos']}/{a['folds_eval']} (frac {a['frac_pos']}), "
           f"median test PF {a['median_test_pf']} -> **{a['verdict']}**", "",
           "### B2 - re-tuned filter pocket per fold (search TRAIN, score unseen TEST):", "",
           "| fold | test window | train PF | train n | test PF | test n | pocket |",
           "|--:|---|--:|--:|--:|--:|---|"]
    for r in srch_recs:
        if r.get("test_pf") is None:
            md.append(f"| {r['fold']} | {r['test_window']} | - | - | - | - | {r.get('status','')} |")
        else:
            md.append(f"| {r['fold']} | {r['test_window']} | {r['train_pf']} | {r['train_n']} | "
                      f"{r['test_pf']} | {r['test_n']} | {r['pocket']} |")
    b = res["filter_search"]
    md += ["", f"-> folds positive {b['folds_pos']}/{b['folds_eval']} (frac {b['frac_pos']}), "
           f"median test PF {b['median_test_pf']} -> **{b['verdict']}**", ""]
    return res, "\n".join(md)


def part_c(df, exits, sessions, exit_id, regime) -> tuple[dict, str]:
    dfr = attach_regime(df, regime)
    cov = float(dfr["mkt_above_ema20"].notna().mean())
    pnl = pnl_for_exit(dfr, exits, exit_id)
    base = np.ones(len(dfr), dtype=bool)

    # C1 - direct regime gates on the base setup, single TRAIN/TEST split.
    gates = {
        "mkt_above_ema20": dfr["mkt_above_ema20"].fillna(0).gt(0.5).to_numpy(),
        "mkt_trend_up (ema20>50 & above)": ((dfr["mkt_above_ema20"].fillna(0).gt(0.5)) &
                                            (dfr["mkt_ema_stack"].fillna(0).gt(0.5))).to_numpy(),
        "mkt_ret_open>=0": dfr["mkt_ret_open_pct"].ge(0).to_numpy(),
        "mkt_mom_30m>=0": dfr["mkt_mom_30m_pct"].ge(0).to_numpy(),
    }
    tr_m = dfr["session"].isin(set(sessions[:opt_len_train(sessions)])).to_numpy()
    te_m = dfr["session"].isin(set(sessions[opt_len_train(sessions):])).to_numpy()
    c1 = []
    none_tr, none_te = stat_pack(pnl, base & tr_m), stat_pack(pnl, base & te_m)
    c1.append({"gate": "(none / base)", "train_pf": none_tr["pf"], "train_n": none_tr["n"],
               "test_pf": none_te["pf"], "test_n": none_te["n"]})
    for name, gm in gates.items():
        s_tr, s_te = stat_pack(pnl, base & gm & tr_m), stat_pack(pnl, base & gm & te_m)
        c1.append({"gate": name, "train_pf": s_tr["pf"], "train_n": s_tr["n"],
                   "test_pf": s_te["pf"], "test_n": s_te["n"]})

    # C2 - walk-forward WITH regime in the search universe (does it earn its keep across folds?)
    folds = make_folds(sessions)
    recs = []
    reg_preds_all = regime_predicates(dfr)
    for k, f in enumerate(folds):
        trm = dfr["session"].isin(f["train"]).to_numpy()
        tem = dfr["session"].isin(f["test"]).to_numpy()
        pk = fold_filter_search(dfr, pnl, trm, extra_preds=reg_preds_all)
        if pk is None:
            recs.append({"fold": k, "test_window": f["test_lbl"], "test_pf": None, "test_n": 0,
                         "status": "no_pocket", "used_regime": False})
            continue
        fm = mask_from_predicates_with_regime(dfr, pk["predicates"], pk["top_n"], reg_preds_all)
        te = opt.quick_metrics(fm, pnl, tem)
        used_reg = any(p.startswith("mkt_") for p in pk["predicates"])
        recs.append({"fold": k, "test_window": f["test_lbl"], "train_pf": round(pk["train_pf"], 2),
                     "train_n": pk["train_n"], "test_pf": round(te["net_pf"], 2), "test_n": te["trades"],
                     "used_regime": used_reg, "pocket": ", ".join(pk["predicates"]) + f" . top_n={pk['top_n']}"})

    ev = [r for r in recs if r.get("test_pf") is not None and r.get("test_n", 0) >= WF_MIN_TEST_TRADES]
    pos = [r for r in ev if r["test_pf"] >= 1.3]
    med = float(np.median([r["test_pf"] for r in ev])) if ev else 0.0
    frac = (len(pos) / len(ev)) if ev else 0.0
    n_used = sum(1 for r in ev if r.get("used_regime"))

    md = ["## PART C - Regime conditioning (causal NIFTY50)", "",
          f"Causal market features joined at the signal bar (coverage {cov:.0%} of rows). "
          "Note: walk_forward.py bans contemporaneous market_ret as a known overfit vector, so regime "
          "is credited ONLY if it holds across folds - never on a single split.", "",
          "### C1 - direct market gate on the base setup, single split:", "",
          "| market gate | train PF | train n | test PF | test n |",
          "|---|--:|--:|--:|--:|"]
    for r in c1:
        md.append(f"| {r['gate']} | {r['train_pf']:.2f} | {r['train_n']} | {r['test_pf']:.2f} | {r['test_n']} |")
    md += ["", "### C2 - walk-forward with regime IN the search universe:", "",
           "| fold | test window | train PF | test PF | test n | used regime? | pocket |",
           "|--:|---|--:|--:|--:|:--:|---|"]
    for r in recs:
        if r.get("test_pf") is None:
            md.append(f"| {r['fold']} | {r['test_window']} | - | - | - | - | {r.get('status','')} |")
        else:
            md.append(f"| {r['fold']} | {r['test_window']} | {r.get('train_pf')} | {r['test_pf']} | "
                      f"{r['test_n']} | {'yes' if r['used_regime'] else 'no'} | {r['pocket']} |")
    v = verdict(frac, med, len(ev))
    md += ["", f"-> with regime available: folds positive {len(pos)}/{len(ev)} (frac {round(frac,2)}), "
           f"median test PF {round(med,2)}, regime chosen in {n_used}/{len(ev)} evaluated folds -> **{v}**", ""]
    res = {"coverage": cov, "c1_single_split": c1, "c2_walkforward": recs,
           "frac_pos": round(frac, 2), "median_test_pf": round(med, 2), "verdict": v}
    return res, "\n".join(md)


def mask_from_predicates_with_regime(df, predicates, top_n, reg_preds):
    bank = dict(opt.predicate_bank(df))
    bank.update(dict(reg_preds))
    m = np.ones(len(df), dtype=bool)
    for p in predicates:
        if p in bank:
            m &= bank[p]
    return opt.topn_mask(df, m, top_n)


def opt_len_train(sessions: list[pd.Timestamp]) -> int:
    """Train length matching the original 30/10 split used by the optimizer."""
    return len(sessions) - 10 if len(sessions) > 10 else len(sessions) // 2


# ----------------------------------------------------------------------------- main
def main() -> int:
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
    except Exception:
        pass
    summary = opt.load_summary()
    cand, rules, exits = opt.load_caches(summary)
    df = opt.make_base(cand, rules)
    splits = opt.split_sets(summary)
    sessions = session_index(df)
    print(f"[diag] base rows={len(df):,} sessions={len(sessions)} "
          f"({sessions[0].date()}..{sessions[-1].date()})", flush=True)

    cfg = json.loads((opt.CANDIDATES_DIR / "FAST_MOMENTUM_LONG_TRAIN_PF_OPTIMIZED_config.json").read_text("utf-8"))
    pocket = {"predicates": cfg["optimized_parameters"]["predicates"],
              "top_n": cfg["optimized_parameters"]["top_n_per_slot"]}
    pocket_exit = cfg["optimized_parameters"]["exit_id"]

    # PART A
    print("[diag] PART A exit sweep ...", flush=True)
    tab, md_a = part_a(df, exits, splits, pocket)
    tab.to_csv(RESULTS_DIR / "diag_exit_breakeven_sweep.csv", index=False)
    # choose the exit that maximises BASE-population PF over ALL sessions (most stable estimator)
    all_mask = np.ones(len(df), dtype=bool)
    exit_scores = {e: stat_pack(pnl_for_exit(df, exits, e), all_mask)["pf"] for e in exits["exit_id"].unique()}
    best_exit = max(exit_scores, key=exit_scores.get)
    print(f"[diag] best base-population exit (all sessions) = {best_exit} (PF {exit_scores[best_exit]:.3f})", flush=True)

    # PART B  (use the best base exit so we judge the entry, not a lucky bracket)
    print("[diag] PART B walk-forward ...", flush=True)
    res_b, md_b = part_b(df, exits, sessions, best_exit)
    (RESULTS_DIR / "diag_walkforward.json").write_text(json.dumps(res_b, indent=2, default=str), "utf-8")

    # PART C
    print("[diag] PART C regime conditioning ...", flush=True)
    regime = load_regime()
    res_c, md_c = part_c(df, exits, sessions, best_exit, regime)
    (RESULTS_DIR / "diag_regime.json").write_text(json.dumps(res_c, indent=2, default=str), "utf-8")

    # consolidated report
    head = [
        "# Exit / Walk-forward / Regime Diagnostics - FAST_MOMENTUM_LONG",
        "",
        f"Rule: `{RULE_ID}`  |  base signals: {len(df):,} over {len(sessions)} sessions  "
        f"({sessions[0].date()}..{sessions[-1].date()})",
        f"Pocket exit (from optimizer): `{pocket_exit}`  |  exit used for B/C "
        f"(best base-population bracket, all sessions): `{best_exit}`",
        "",
        "These three parts test the three follow-ups to the TRAIN-PF optimizer: can a better EXIT "
        "rescue the edge (A), does any edge survive WALK-FORWARD (B), and does market REGIME "
        "conditioning generalize (C).",
        "",
    ]
    report = "\n".join(head) + "\n" + md_a + "\n" + md_b + "\n" + md_c + "\n" + _verdict_block(res_b, res_c)
    (OUT_DIR / "DIAGNOSTICS_EXIT_REGIME_WALKFORWARD.md").write_text(report, "utf-8")
    print(f"[done] wrote {OUT_DIR / 'DIAGNOSTICS_EXIT_REGIME_WALKFORWARD.md'}")
    print(f"[done] B base verdict={res_b['base_unconditional']['verdict']} "
          f"search verdict={res_b['filter_search']['verdict']} | C verdict={res_c['verdict']}")
    return 0


def _verdict_block(res_b: dict, res_c: dict) -> str:
    a = res_b["base_unconditional"]; b = res_b["filter_search"]
    return "\n".join([
        "## Bottom line", "",
        f"- **Base setup, walk-forward:** {a['verdict']} "
        f"(median test PF {a['median_test_pf']}, {a['folds_pos']}/{a['folds_eval']} folds positive).",
        f"- **Re-tuned filter pocket, walk-forward:** {b['verdict']} "
        f"(median test PF {b['median_test_pf']}, {b['folds_pos']}/{b['folds_eval']} folds positive).",
        f"- **Regime conditioning, walk-forward:** {res_c['verdict']} "
        f"(median test PF {res_c['median_test_pf']}, frac positive {res_c['frac_pos']}).",
        "",
        "If all three are FRAGILE/DEAD, the conclusion is structural: this entry has no regime-robust "
        "long edge at this frequency in this window, and neither exit redesign, per-fold filtering, nor "
        "market-regime gating changes that. That is the honest answer to \"why TEST isn't improving.\"",
        "",
    ])


if __name__ == "__main__":
    raise SystemExit(main())
