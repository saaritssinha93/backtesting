r"""recovery_engine.py — shared engine for the from-scratch recovery loop.

RESEARCH-ONLY. Reused by every setup's scripts/run_recovery.py wrapper. Never
touches final_setup_conf.py; no live execution.

What it adds over the pf-band loops (rounds 1-3):
  * execution-layer degrees of freedom — retest/limit entries (fill_retest_limit)
    and break-even / trailing / time-stop exits (resolver2, validated identical
    to the production resolver with extras off);
  * redesign VARIANTS: each variant = a named entry/filter/exit family with its
    own structured grid (defined per setup in the wrapper, informed by the
    execution diagnostics + winner/loser study);
  * the same anti-overfit discipline: grids scored on FIT/VAL band objective
    (tent at PF 1.80, gap penalty), full-TRAIN confirmation with domination
    caps, robustness-lite (threshold-shift + term-dropout), ONE TEST evaluation
    per variant family, all iterations logged.

Trade mechanics (mirrors setup_train_test exactly where shared):
  entry market : next-1-min-open fill already attached by tt.attach_entries
                 (15 bps/leg adverse slippage), qty = notional/raw_open.
  entry retest : resting limit at signal_close -/+ alpha*ATR (pullback against
                 the signal direction), cancel if untouched within K minutes of
                 the market-entry minute; fill = limit with the same 15 bps
                 adverse slippage; qty = notional/limit.
  exits        : resolver2 (SL/TARGET/BE/TRAIL/TIME/EOD), exit-leg slippage via
                 tt._exit_with_slippage, statutory NSE costs via tt._trade_net.
  dedupe       : first signal per (ticker, day) after filters (live-faithful).
  top_n        : per (day, slot) by vwap_dist_atr desc (mirrors apply_guards).
  max_positions: sequential concurrency cap on open intervals.
"""
from __future__ import annotations

import json
import sys
import time
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd

_HERE = Path(__file__).resolve().parent
RECOVERY = _HERE.parent
TT_DIR = RECOVERY.parent
REPO = TT_DIR.parent
for _p in (str(REPO), str(TT_DIR), str(_HERE)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import setup_train_test as tt                    # noqa: E402
import avwap_5min_ID_v11_backtesting as v11      # noqa: E402
import resolver2 as r2                           # noqa: E402

PF_LO, PF_HI = 1.30, 1.80
TEST_PF_MIN = 1.40
TRAIN_W = ("2026-03-01", "2026-05-30")
TEST_W = ("2026-06-01", "2026-07-02")
FIT_FRAC = 0.60
SLIP_BPS = 15.0
NOTIONAL = float(v11.V7_SIGNAL_NOTIONAL_RS)

_RESOLVE_CACHE: dict = {}
_FILL_CACHE: dict = {}


# ---------------------------------------------------------------------------
def load_windows(setup: str):
    work = RECOVERY / setup
    tt.POOL_DIRS = [work / "pools" / (setup + "_enriched")]
    tt.POOL_DIR = tt.POOL_DIRS[0]
    pool = tt.load_pool()
    pool = pool[pool["setup"] == setup].copy()
    tt.SLIPPAGE_BPS = SLIP_BPS
    tt._entry.cache_clear(); tt._resolve_full.cache_clear(); tt._premom.cache_clear()
    pool = tt.attach_entries(pool)
    sessions = sorted(pd.Series(pool["_day"].dropna().unique()))
    TRAIN_s = [s for s in sessions if pd.Timestamp(TRAIN_W[0]) <= s <= pd.Timestamp(TRAIN_W[1])]
    TEST_s = [s for s in sessions if pd.Timestamp(TEST_W[0]) <= s <= pd.Timestamp(TEST_W[1])]
    n_fit = max(1, int(round(len(TRAIN_s) * FIT_FRAC)))
    FIT_s, VAL_s = TRAIN_s[:n_fit], TRAIN_s[n_fit:]

    def _slice(ss):
        return pool[pool["_day"].isin(set(map(pd.Timestamp, ss)))].copy()
    wins = {"FIT": _slice(FIT_s), "VAL": _slice(VAL_s),
            "TRAIN": _slice(TRAIN_s), "TEST": _slice(TEST_s)}
    meta = {"FIT_s": FIT_s, "VAL_s": VAL_s, "TRAIN_s": TRAIN_s, "TEST_s": TEST_s}
    return wins, meta


def apply_filters(df: pd.DataFrame, mask_terms, min_slot=None, max_slot=None, top_n=None):
    out = tt.apply_mask_terms(df, [tuple(t) for t in (mask_terms or [])])
    if out.empty:
        return out
    if min_slot:
        hh, mm = str(min_slot).split(":")
        out = out[out["signal_minute"] >= int(hh) * 60 + int(mm)]
    if max_slot:
        hh, mm = str(max_slot).split(":")
        out = out[out["signal_minute"] <= int(hh) * 60 + int(mm)]
    if top_n and not out.empty:
        out = (out.sort_values("vwap_dist_atr", ascending=False)
               .groupby(["_day", "_slot"], sort=False).head(int(top_n)))
    if out.empty:
        return out
    # premom gate is applied by caller (needs sl); dedupe LAST (after all filters)
    return out


def apply_premom(df: pd.DataFrame, terms, sl: float):
    if df.empty or not terms:
        return df
    return tt.apply_premom_terms(df, [tuple(t) for t in terms], sl)


def dedupe_first(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    return (df.sort_values("tt_sig_ts")
            .drop_duplicates(subset=["ticker", "_day"], keep="first"))


def _resolve_one(ticker, side, entry_iso, fill, sl, tgt, be, trail, tstop):
    key = (ticker, entry_iso, round(fill, 4), sl, tgt, be, trail, tstop)
    if key in _RESOLVE_CACHE:
        return _RESOLVE_CACHE[key]
    bars = v11._load_1m_with_open(ticker)
    res = r2.resolve2(bars, side, fill, pd.Timestamp(entry_iso), sl, tgt,
                      be_at_pct=be, trail_pct=trail, time_stop_min=tstop)
    out = None
    if res is not None:
        exit_px = tt._exit_with_slippage(side, float(res.exit_price))
        out = (res.outcome, float(exit_px), res.exit_time_ist.isoformat())
    _RESOLVE_CACHE[key] = out
    return out


def _retest_fill(ticker, side, sig_close, atr_pct, entry_iso, alpha, within_min):
    key = (ticker, entry_iso, round(sig_close, 4), round(alpha, 3), within_min)
    if key in _FILL_CACHE:
        return _FILL_CACHE[key]
    out = None
    if np.isfinite(sig_close) and sig_close > 0 and np.isfinite(atr_pct) and atr_pct > 0:
        limit = sig_close * (1 - alpha * atr_pct) if side == "LONG" else sig_close * (1 + alpha * atr_pct)
        bars = v11._load_1m_with_open(ticker)
        f = r2.fill_retest_limit(bars, side, limit, pd.Timestamp(entry_iso), within_min)
        if f is not None:
            ts, limit_px = f
            slip = SLIP_BPS / 1e4
            fill = round(limit_px * (1 + slip), 2) if side == "LONG" else round(limit_px * (1 - slip), 2)
            qty = max(1, int(NOTIONAL / limit_px))
            out = (ts.isoformat(), fill, qty)
    _FILL_CACHE[key] = out
    return out


def run_book(df: pd.DataFrame, exec_cfg: dict) -> pd.DataFrame:
    """Execute a filtered/deduped book under an execution config. Returns
    per-trade detail with net_pnl_rs (statutory + slippage both legs)."""
    sl, tgt = float(exec_cfg["sl"]), float(exec_cfg["tgt"])
    be = exec_cfg.get("be"); trail = exec_cfg.get("trail"); tstop = exec_cfg.get("tstop")
    entry = exec_cfg.get("entry", "market")
    rows = []
    df = df.assign(day_str=df["_day"].astype(str).str.slice(0, 10))
    for r in df.itertuples():
        if entry == "market":
            e_iso, fill, qty = r.tt_entry_iso, float(r.tt_fill), int(r.tt_qty)
        else:
            alpha, within = float(entry[1]), int(entry[2])
            f = _retest_fill(r.ticker, r.side, float(getattr(r, "signal_close", np.nan)),
                             float(getattr(r, "atr_pct", np.nan)), r.tt_entry_iso, alpha, within)
            if f is None:
                continue
            e_iso, fill, qty = f
        res = _resolve_one(r.ticker, r.side, e_iso, fill, sl, tgt, be, trail, tstop)
        if res is None:
            continue
        outcome, exit_px, exit_iso = res
        net = tt._trade_net(r.side, fill, qty, "SL" if outcome in ("SL",) else outcome, exit_px)
        rows.append({"ticker": r.ticker, "side": r.side, "trade_date": r.day_str,
                     "signal_time": str(r.tt_sig_ts), "entry_time": e_iso, "exit_time": exit_iso,
                     "fill": fill, "qty": qty, "outcome": outcome, "exit_px": exit_px,
                     "net_pnl_rs": float(net)})
    det = pd.DataFrame(rows)
    if det.empty:
        return det
    mp = int(exec_cfg.get("max_positions", 10) or 10)
    det = det.sort_values("entry_time").reset_index(drop=True)
    open_exits: list = []
    keep = []
    for r in det.itertuples():
        et = pd.Timestamp(r.entry_time)
        open_exits = [x for x in open_exits if x > et]
        if len(open_exits) >= mp:
            keep.append(False)
            continue
        open_exits.append(pd.Timestamp(r.exit_time))
        keep.append(True)
    return det[pd.Series(keep, index=det.index)].reset_index(drop=True)


def metrics(det: pd.DataFrame) -> dict:
    m = {"n": 0, "net_pf": 0.0, "net_pnl": 0.0, "win_rate": 0.0, "avg_win": 0.0, "avg_loss": 0.0,
         "max_dd": 0.0, "sl_cnt": 0, "tgt_cnt": 0, "be_cnt": 0, "trail_cnt": 0, "time_cnt": 0,
         "eod_cnt": 0, "target_rate": 0.0, "trades_per_day": 0.0, "n_days": 0, "n_syms": 0,
         "trade_dom_gross": None, "day_dom": None, "sym_dom": None, "day_block_p": None,
         "top_day": None, "top_sym": None}
    if det is None or det.empty:
        return m
    net = det["net_pnl_rs"].to_numpy()
    gp = float(net[net > 0].sum()); gl = float(-net[net < 0].sum())
    eq = net.cumsum(); dd = eq - np.maximum.accumulate(eq)
    oc = det["outcome"].astype(str)
    day_sum = det.groupby("trade_date")["net_pnl_rs"].sum()
    sym_sum = det.groupby("ticker")["net_pnl_rs"].sum()
    tot = float(net.sum())
    rng = np.random.default_rng(7)
    idx = rng.integers(0, len(day_sum), size=(10000, len(day_sum)))
    dbp = float((day_sum.to_numpy()[idx].mean(axis=1) <= 0).mean()) if len(day_sum) >= 3 else float("nan")
    m.update({
        "n": int(len(det)), "net_pf": round(gp / gl, 3) if gl > 0 else (99.0 if gp > 0 else 0.0),
        "net_pnl": round(tot, 0), "win_rate": round(float((net > 0).mean()) * 100, 1),
        "avg_win": round(float(net[net > 0].mean()), 0) if (net > 0).any() else 0.0,
        "avg_loss": round(float(net[net < 0].mean()), 0) if (net < 0).any() else 0.0,
        "max_dd": round(float(dd.min()), 0) if len(dd) else 0.0,
        "sl_cnt": int((oc == "SL").sum()), "tgt_cnt": int((oc == "TARGET").sum()),
        "be_cnt": int((oc == "BE").sum()), "trail_cnt": int((oc == "TRAIL").sum()),
        "time_cnt": int((oc == "TIME").sum()), "eod_cnt": int((oc == "EOD").sum()),
        "target_rate": round(float((oc == "TARGET").mean()) * 100, 1),
        "n_days": int(det["trade_date"].nunique()), "n_syms": int(det["ticker"].nunique()),
        "trade_dom_gross": round(float(net.max()) / gp, 3) if gp > 0 else 9.99,
        "day_dom": round(float(day_sum.max()) / tot, 3) if tot > 0 else 9.99,
        "sym_dom": round(float(sym_sum.max()) / tot, 3) if tot > 0 else 9.99,
        "day_block_p": round(dbp, 4) if np.isfinite(dbp) else None,
        "top_day": f"{day_sum.idxmax()}: Rs{day_sum.max():,.0f}",
        "top_sym": f"{sym_sum.idxmax()}: Rs{sym_sum.max():,.0f}",
    })
    m["trades_per_day"] = round(m["n"] / max(1, m["n_days"]), 2)
    return m


def band_score(mF: dict, mV: dict, floor: int = 10) -> float:
    if mF["n"] < floor or mV["n"] < floor:
        return -5.0 + min(mF["n"], mV["n"]) / floor
    def clamp(pf):
        return min(float(pf), 10.0)
    lo = min(clamp(mF["net_pf"]), clamp(mV["net_pf"]))
    tent = lo if lo <= PF_HI else PF_HI - 1.5 * (lo - PF_HI)
    sc = tent - 0.80 * abs(clamp(mF["net_pf"]) - clamp(mV["net_pf"]))
    if lo >= PF_LO:
        sc += 0.003 * min(min(mF["n"], mV["n"]), 40)
    return sc


def dom_ok(m: dict) -> bool:
    return (m["trade_dom_gross"] is not None and m["trade_dom_gross"] <= 0.35
            and m["day_dom"] is not None and m["day_dom"] <= 0.40
            and m["sym_dom"] is not None and m["sym_dom"] <= 0.40)


def train_ok(m: dict) -> tuple[bool, list]:
    bad = []
    if not (PF_LO <= m["net_pf"] <= PF_HI):
        bad.append(f"TRAIN PF {m['net_pf']} outside band")
    if m["n"] < 25:
        bad.append(f"TRAIN n {m['n']} < 25")
    if m["net_pnl"] <= 0:
        bad.append("TRAIN net <= 0")
    if m["trades_per_day"] > 6.0:
        bad.append("TRAIN tpd > 6")
    if not dom_ok(m):
        bad.append("TRAIN domination")
    return (not bad), bad


def test_ok(m: dict) -> tuple[bool, list]:
    bad = []
    if m["n"] < 5:
        bad.append(f"TEST n {m['n']} < 5")
        return False, bad
    if m["net_pf"] <= TEST_PF_MIN:
        bad.append(f"TEST PF {m['net_pf']} <= {TEST_PF_MIN}")
    if m["net_pnl"] <= 0:
        bad.append("TEST net <= 0")
    if not dom_ok(m):
        bad.append("TEST domination")
    if m["day_block_p"] is None or m["day_block_p"] > 0.10:
        bad.append(f"TEST day-block p {m['day_block_p']} > 0.10")
    return (not bad), bad


# ---------------------------------------------------------------------------
def eval_variant_cfg(wins: dict, spec: dict, window: str) -> dict:
    df = apply_filters(wins[window], spec.get("mask_terms"),
                       spec.get("min_slot"), spec.get("max_slot"), spec.get("top_n"))
    df = apply_premom(df, spec.get("premom_terms"), float(spec["sl"]))
    df = dedupe_first(df)
    det = run_book(df, spec)
    return metrics(det), det


def robustness_lite(wins: dict, spec: dict) -> dict:
    """Threshold-shift (+/-10% relative on numeric mask/premom thresholds) and
    single-term dropout on full TRAIN. Pass floors mirror the pf-band engine."""
    checks = []
    def m_of(s):
        m, _ = eval_variant_cfg(wins, s, "TRAIN")
        return m
    for key in ("mask_terms", "premom_terms"):
        terms = list(spec.get(key) or [])
        for i, (f, op, thr) in enumerate(terms):
            if isinstance(thr, str):
                continue
            for mult in (0.9, 1.1):
                s2 = dict(spec); t2 = list(terms); t2[i] = (f, op, float(thr) * mult); s2[key] = t2
                m = m_of(s2)
                checks.append({"check": "neighbor", "term": f"{f}{op}{thr}", "mult": mult,
                               "n": m["n"], "pf": m["net_pf"],
                               "pass": bool(m["n"] >= 20 and m["net_pf"] >= 1.15)})
        for i in range(len(terms)):
            s2 = dict(spec); t2 = list(terms); t2.pop(i); s2[key] = t2
            m = m_of(s2)
            checks.append({"check": "dropout", "term": str(terms[i]), "mult": None,
                           "n": m["n"], "pf": m["net_pf"],
                           "pass": bool(m["n"] >= 20 and m["net_pf"] >= 1.00)})
    npass = all(c["pass"] for c in checks if c["check"] == "neighbor") if checks else True
    dpass = all(c["pass"] for c in checks if c["check"] == "dropout") if checks else True
    return {"neighbor_pass": npass, "dropout_pass": dpass, "checks": checks}


def run_recovery(setup: str, side: str, variants: dict, out_dir: Path,
                 max_test_evals: int = 6) -> dict:
    """variants: {family_name: [spec, spec, ...]} — each spec is a full config
    (entry/mask/premom/slots/top_n/sl/tgt/be/trail/tstop/max_positions)."""
    t0 = time.time()
    wins, meta = load_windows(setup)
    print(f"[recover] {setup} rows FIT={len(wins['FIT'])} VAL={len(wins['VAL'])} "
          f"TRAIN={len(wins['TRAIN'])} TEST={len(wins['TEST'])}")
    log_rows = []
    results = []
    it = 0
    test_evals = 0
    for fam, specs in variants.items():
        print(f"[recover] family {fam}: {len(specs)} configs")
        scored = []
        for spec in specs:
            it += 1
            mF, _ = eval_variant_cfg(wins, spec, "FIT")
            mV, _ = eval_variant_cfg(wins, spec, "VAL")
            sc = band_score(mF, mV)
            scored.append((sc, spec, mF, mV))
            log_rows.append({"iter": it, "family": fam, "spec": json.dumps(tt._json_sanitize(spec), default=str),
                             "fit_n": mF["n"], "fit_pf": mF["net_pf"], "val_n": mV["n"],
                             "val_pf": mV["net_pf"], "score": round(sc, 4), "stage": "fitval",
                             "keep": "", "why": ""})
        scored.sort(key=lambda x: -x[0])
        # TRAIN confirm the best few distinct configs of the family
        confirmed = None
        for sc, spec, mF, mV in scored[:6]:
            if min(mF["net_pf"], mV["net_pf"]) < PF_LO:
                break
            it += 1
            mTR, detTR = eval_variant_cfg(wins, spec, "TRAIN")
            ok, bad = train_ok(mTR)
            log_rows.append({"iter": it, "family": fam, "spec": json.dumps(tt._json_sanitize(spec), default=str),
                             "fit_n": mF["n"], "fit_pf": mF["net_pf"], "val_n": mV["n"],
                             "val_pf": mV["net_pf"], "score": round(sc, 4), "stage": "train_confirm",
                             "train_n": mTR["n"], "train_pf": mTR["net_pf"], "train_net": mTR["net_pnl"],
                             "keep": "OK" if ok else "reject", "why": "; ".join(bad)})
            print(f"[recover] {fam} TRAIN confirm: PF {mTR['net_pf']} n {mTR['n']} "
                  f"dayDom {mTR['day_dom']} -> {'OK' if ok else '; '.join(bad)}")
            if ok:
                confirmed = (sc, spec, mF, mV, mTR, detTR)
                break
        rec = {"family": fam, "passed": False}
        if confirmed and test_evals < max_test_evals:
            sc, spec, mF, mV, mTR, detTR = confirmed
            rob = robustness_lite(wins, spec)
            mTE, detTE = eval_variant_cfg(wins, spec, "TEST")
            test_evals += 1
            okT, badT = test_ok(mTE)
            hard = badT + ([] if rob["neighbor_pass"] else ["neighborhood robustness failed"]) \
                        + ([] if rob["dropout_pass"] else ["dropout robustness failed"])
            passed = not hard
            it += 1
            log_rows.append({"iter": it, "family": fam, "spec": json.dumps(tt._json_sanitize(spec), default=str),
                             "stage": "test_once", "train_n": mTR["n"], "train_pf": mTR["net_pf"],
                             "test_n": mTE["n"], "test_pf": mTE["net_pf"], "test_net": mTE["net_pnl"],
                             "keep": "PASS" if passed else "reject", "why": "; ".join(hard)})
            print(f"[recover] {fam} TEST: PF {mTE['net_pf']} n {mTE['n']} net {mTE['net_pnl']} "
                  f"-> {'PASS' if passed else '; '.join(hard)}")
            rec.update({"passed": passed, "hard": hard, "spec": tt._json_sanitize(spec),
                        "fitval": {"fit": mF, "val": mV, "score": sc},
                        "train": mTR, "test": mTE,
                        "robust": {k: rob[k] for k in ("neighbor_pass", "dropout_pass")}})
            detTR.to_csv(out_dir / f"trades_train_{fam}.csv", index=False)
            if detTE is not None and not detTE.empty:
                detTE.to_csv(out_dir / f"trades_test_{fam}.csv", index=False)
        elif confirmed:
            rec.update({"passed": False, "hard": ["TEST budget exhausted"],
                        "spec": tt._json_sanitize(confirmed[1]), "train": confirmed[4]})
        else:
            best = scored[0] if scored else None
            rec.update({"hard": ["no config cleared FIT/VAL+TRAIN gates"],
                        "best_fitval": {"score": round(best[0], 4) if best else None,
                                        "spec": tt._json_sanitize(best[1]) if best else None,
                                        "fit": best[2] if best else None,
                                        "val": best[3] if best else None}})
        results.append(rec)
    log = pd.DataFrame(log_rows)
    log.to_csv(out_dir / "iteration_log.csv", index=False)
    summary = {"setup": setup, "side": side, "generated": date.today().isoformat(),
               "windows": {k: f"{pd.Timestamp(v[0]).date()}..{pd.Timestamp(v[-1]).date()} ({len(v)})"
                           for k, v in meta.items() if v},
               "n_iterations": int(it), "n_test_evals": test_evals,
               "results": results,
               "elapsed_sec": round(time.time() - t0, 1)}
    (out_dir / "recovery_summary.json").write_text(
        json.dumps(tt._json_sanitize(summary), indent=2, default=str), encoding="utf-8")
    print(f"[recover] {setup} done: {it} iterations, {test_evals} TEST evals, "
          f"{sum(1 for r in results if r.get('passed'))} passing, {time.time()-t0:.0f}s")
    return summary


def winner_loser_study(setup: str, side: str, base_exit: tuple, out_dir: Path,
                       feats: list[str]) -> None:
    """Broad detection book at baseline exits: winners vs losers across features,
    time windows, symbols; writes WINNER_LOSER_STUDY.md.

    Runs on the FIT window ONLY so that any threshold learned from it can be
    validated on VAL without contamination."""
    wins, _ = load_windows(setup)
    df = dedupe_first(wins["FIT"])
    det = run_book(df, {"sl": base_exit[0], "tgt": base_exit[1], "entry": "market",
                        "max_positions": 20})
    if det.empty:
        (out_dir / "WINNER_LOSER_STUDY.md").write_text("no resolvable book", encoding="utf-8")
        return
    j = det.merge(df[["ticker"] + [c for c in feats if c in df.columns]
                  + ["signal_minute"]].assign(signal_time=df["tt_sig_ts"].astype(str)),
                  on=["ticker", "signal_time"], how="left")
    j["win"] = j["net_pnl_rs"] > 0
    lines = [f"# {setup} ({side}) — WINNER_LOSER_STUDY", "",
             f"_Generated {date.today().isoformat()}. Broad detection book (family dedupe, "
             f"exit {base_exit[0]}/{base_exit[1]}, statutory @15bps), FIT window ONLY "
             f"(thresholds learned here are validated on untouched VAL); "
             f"n={len(j)}, win rate {j['win'].mean()*100:.1f}%._", "",
             "## Feature separation (winner mean vs loser mean, top |t|-like gaps)", "",
             "| feature | win mean | loss mean | gap/std |", "|---|---|---|---|"]
    gaps = []
    for f in feats:
        if f not in j.columns:
            continue
        x = pd.to_numeric(j[f], errors="coerce")
        if x.notna().mean() < 0.5 or x.nunique() < 5:
            continue
        w, l = x[j["win"]], x[~j["win"]]
        sd = x.std()
        if not (np.isfinite(sd) and sd > 0):
            continue
        gaps.append((abs(w.mean() - l.mean()) / sd, f, w.mean(), l.mean(), sd))
    for g, f, wm, lm, sd in sorted(gaps, reverse=True)[:20]:
        lines.append(f"| {f} | {wm:.3f} | {lm:.3f} | {g:.3f} |")
    hh = pd.to_datetime(j["signal_time"]).dt.hour
    lines += ["", "## Net by signal hour", "", "| hour | n | net Rs | win% |", "|---|---|---|---|"]
    for h, g in j.groupby(hh):
        lines.append(f"| {h}:00 | {len(g)} | {g['net_pnl_rs'].sum():,.0f} | {g['win'].mean()*100:.0f} |")
    days = j.groupby("trade_date")["net_pnl_rs"].sum().sort_values()
    syms = j.groupby("ticker")["net_pnl_rs"].sum().sort_values()
    lines += ["", "## Worst days"] + [f"- {d}: Rs{v:,.0f}" for d, v in days.head(5).items()]
    lines += ["", "## Best days"] + [f"- {d}: Rs{v:,.0f}" for d, v in days.tail(3).items()]
    lines += ["", "## Worst symbols"] + [f"- {s}: Rs{v:,.0f}" for s, v in syms.head(5).items()]
    lines += ["", "## Exit mix", "", f"- {det['outcome'].value_counts().to_dict()}"]
    (out_dir / "WINNER_LOSER_STUDY.md").write_text("\n".join(lines), encoding="utf-8")
    j.to_csv(out_dir / "winner_loser_book.csv", index=False)
