#!/usr/bin/env python3
"""
Full-Pipeline Entry Research v2 - governed setup-independent LONG/SHORT miner.

READ-ONLY research. Does not modify any live scanner/filter/setup/executor/conf.

What it does (vs v1):
  * DISCOVERS on the long-history unified pool (Nov-2025..Jun-2026, ~258k rows),
    not on ~12 recent truth sessions.
  * SHORTS come from real `side==SHORT` rows (no force-inversion of longs).
  * Outcomes use a LIVE-FAITHFUL intraday/EOD (15:20) resolver, net of the house
    v6 cost - not a multi-day no-EOD walk.
  * Only Rs>=50cr ADV names, and only canonical features that also exist in the
    live truth tables (so a discovery can be monitored forward).
  * Governance before a pattern is called a survivor: TRAIN / VALIDATE / locked
    HOLDOUT split with a 1-day embargo, an embargoed rolling walk-forward, a
    day-block bootstrap p-value, BH-FDR across every tested config, a top-1-day
    P&L-share cap, and (for shorts) market-return neutralization (alpha>0).

Survivors are emitted as DISCOVERED candidates for the V7 Shadow Candidate
Monitor (v2 tag). Nothing here is promoted to live; exit geometry is a single
per-side bracket (per-mask exit optimization is a deferred Phase-2 item).
"""

from __future__ import annotations

import argparse
import datetime as dt
import itertools
import json
import math
import time
from pathlib import Path
from typing import Any, Iterable
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

from eqidv2_runtime_paths import RUNTIME_STATUS_DIR, runtime_dir
import v7_fpe_v2_common as cm
import walkforward_gate as wfg


IST = ZoneInfo("Asia/Kolkata")
SESSION_SLUG = "v7_full_pipeline_entry_research_v2"
SESSION_ROOT = runtime_dir(SESSION_SLUG)
LATEST_DIR = SESSION_ROOT / "latest"
REPORT_DIR = SESSION_ROOT / "reports"
HEARTBEAT_DIR = SESSION_ROOT / "heartbeat"

# ---- default gates (honest, deliberately strict) ---------------------------
DEF = dict(
    sl_pct=0.85, tgt_pct=1.10, cost_bps=cm.DEFAULT_COST_BPS,
    adv_min_cr=50.0, qbins=6, holdout_days=20, val_frac=0.20, embargo_days=1,
    min_train_trades=40, min_val_trades=20, min_val_days=3,
    train_pf_min=1.30, val_pf_min=1.20, top1_share_max=0.40,
    day_block_p_max=0.10, fdr_alpha=0.10, wf_fold_frac_min=0.55,
    wf_train_days=60, wf_test_days=20, wf_step_days=20, wf_min_test_trades=8,
    pair_top_n=12, monitor_sessions=25, n_boot=5000,
)

for _p in (SESSION_ROOT, LATEST_DIR, REPORT_DIR, HEARTBEAT_DIR, RUNTIME_STATUS_DIR):
    _p.mkdir(parents=True, exist_ok=True)


# ---- io / status -----------------------------------------------------------
def _now() -> dt.datetime:
    return dt.datetime.now(IST)


def _sanitize(v: Any) -> Any:
    if isinstance(v, dict):
        return {str(k): _sanitize(x) for k, x in v.items()}
    if isinstance(v, (list, tuple)):
        return [_sanitize(x) for x in v]
    if isinstance(v, (np.integer,)):
        return int(v)
    if isinstance(v, (np.floating, float)):
        f = float(v)
        return f if math.isfinite(f) else None
    if isinstance(v, (pd.Timestamp, dt.datetime, dt.date)):
        return str(v)
    return v


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(text, encoding="utf-8", errors="replace")
    tmp.replace(path)


def _write_json(path: Path, payload: dict) -> None:
    _write_text(path, json.dumps(_sanitize(payload), indent=2, sort_keys=True))


def _status(state: str, **extra: Any) -> None:
    payload = {"status": state, "state": state, "ts_ist": _now().isoformat(), "session": SESSION_SLUG, **extra}
    text = "\n".join(f"{k}={v}" for k, v in payload.items()) + "\n"
    _write_text(RUNTIME_STATUS_DIR / f"{SESSION_SLUG}.status", text)
    _write_text(RUNTIME_STATUS_DIR / f"{SESSION_SLUG}.heartbeat", text)
    _write_json(HEARTBEAT_DIR / f"{SESSION_SLUG}.status.json", payload)


# ---- data ------------------------------------------------------------------
def load_pool(cfg: dict, since: str | None, limit: int | None) -> pd.DataFrame:
    pool_cols = ["ticker", "side", "setup", "signal_time_ist"] + list(cm.canon_columns("pool").values()) + [cm.MARKET_RET["pool"]]
    usecols = lambda c: c in set(pool_cols)
    df = pd.read_csv(cm.POOL_CSV, usecols=usecols, low_memory=False)
    df = cm.rename_to_canon(df, "pool")
    df["ticker"] = df["ticker"].astype(str).str.upper().str.strip()
    df["side"] = df["side"].astype(str).str.upper().str.strip()
    df = df[df["side"].isin(["LONG", "SHORT"])].copy()
    df["_sig_ns"] = df["signal_time_ist"].map(cm.to_ns)
    df = df[df["_sig_ns"] > 0].copy()
    df["_day"] = df["_sig_ns"].map(cm.ist_day_str)
    if since:
        df = df[df["_day"] >= since].copy()
    _, liquid = cm.load_adv_map(cfg["adv_min_cr"])
    if liquid:
        df = df[df["ticker"].isin(liquid)].copy()
    df = df.sort_values("_sig_ns").reset_index(drop=True)
    if limit:
        df = df.head(limit).copy()
    return df


def resolve_pool(df: pd.DataFrame, cfg: dict, store: cm.MinuteStore) -> pd.DataFrame:
    sl, tgt, cost = cfg["sl_pct"], cfg["tgt_pct"], cfg["cost_bps"]
    out_idx, outcome, net, gross, cost_rs, bars = [], [], [], [], [], []
    for ticker, grp in df.groupby("ticker", sort=False):
        arr = store.load(ticker)
        if arr is None:
            continue
        for idx, sig_ns, side in zip(grp.index, grp["_sig_ns"], grp["side"]):
            r = cm.resolve_one(arr, int(sig_ns), side, sl, tgt, cost)
            if r is None:
                continue
            out_idx.append(idx); outcome.append(r["outcome"]); net.append(r["pnl_rs"])
            gross.append(r["gross_pnl_rs"]); cost_rs.append(r["cost_rs"]); bars.append(r["bars_held"])
    res = df.loc[out_idx].copy()
    res["outcome"] = outcome
    res["pnl_rs"] = net
    res["gross_pnl_rs"] = gross
    res["cost_rs"] = cost_rs
    res["bars_held"] = bars
    return res.reset_index(drop=True)


def split_days(days: list[str], cfg: dict) -> tuple[set, set, set]:
    days = sorted(days)
    hd = cfg["holdout_days"]; emb = cfg["embargo_days"]; vf = cfg["val_frac"]
    holdout = days[-hd:] if hd < len(days) else days[len(days) // 2:]
    pre = days[:-hd] if hd < len(days) else days[:len(days) // 2]
    if emb and len(pre) > emb:
        pre = pre[:-emb]
    n_val = max(5, int(len(pre) * vf))
    val = pre[-n_val:]
    train = pre[:-n_val]
    if emb and len(train) > emb:
        train = train[:-emb]
    return set(train), set(val), set(holdout)


# ---- mining ----------------------------------------------------------------
def _cond_terms(items: list[tuple[str, float, float]]) -> str:
    return " AND ".join(f"{f} ({l:.6g}, {r:.6g}]" for f, l, r in items)


def _mask(df: pd.DataFrame, items: list[tuple[str, float, float]]) -> pd.Series:
    m = pd.Series(True, index=df.index)
    for feat, left, right in items:
        v = pd.to_numeric(df[feat], errors="coerce")
        m &= v.gt(left) & v.le(right)
    return m.fillna(False)


def _day_block_p(part: pd.DataFrame, rng: np.random.Generator, n_boot: int) -> float:
    if part.empty:
        return 1.0
    daily = part.assign(_n=pd.to_numeric(part["pnl_rs"], errors="coerce").fillna(0.0)).groupby("_day")["_n"].sum()
    arr = daily.to_numpy(dtype=float)
    try:
        return float(wfg._bootstrap_p_gt_zero(arr, n_boot, rng))
    except Exception:
        return 1.0


def _market_alpha(part: pd.DataFrame) -> float:
    if part.empty or "market_ret_pct" not in part.columns:
        return float("nan")
    g = part.assign(
        _n=pd.to_numeric(part["pnl_rs"], errors="coerce").fillna(0.0),
        _m=pd.to_numeric(part["market_ret_pct"], errors="coerce"),
    ).dropna(subset=["_m"]).groupby("_day").agg(_n=("_n", "sum"), _m=("_m", "mean"))
    if len(g) < 4 or g["_m"].nunique() < 2:
        return float("nan")
    slope, intercept = np.polyfit(g["_m"].to_numpy(float), g["_n"].to_numpy(float), 1)
    return float(intercept)


def _make_folds(days: list[str], cfg: dict) -> list[tuple[set, set]]:
    days = sorted(days)
    tr, te, step, emb = cfg["wf_train_days"], cfg["wf_test_days"], cfg["wf_step_days"], cfg["embargo_days"]
    folds: list[tuple[set, set]] = []
    i = tr
    while i + te <= len(days):
        train = days[max(0, i - tr):max(0, i - emb)]
        test = days[i:i + te]
        if train and test:
            folds.append((set(train), set(test)))
        i += step
    return folds


def _wf_frac(side_df: pd.DataFrame, items, folds: list[tuple[set, set]], cfg: dict) -> float:
    if not folds:
        return 0.0
    ok = 0; used = 0
    for train_days, test_days in folds:
        te = side_df[side_df["_day"].isin(test_days)]
        te = te[_mask(te, items)]
        if len(te) < cfg["wf_min_test_trades"]:
            continue
        used += 1
        if cm.profit_factor(te["pnl_rs"]) >= cfg["val_pf_min"]:
            ok += 1
    return ok / used if used else 0.0


def mine_side(side: str, res: pd.DataFrame, train_d, val_d, hold_d, cfg, rng) -> tuple[list[dict], dict[str, float]]:
    sdf = res[res["side"] == side].copy()
    train = sdf[sdf["_day"].isin(train_d)]
    if train.empty:
        return [], {}
    folds = _make_folds(sorted(sdf["_day"].unique()), cfg)

    # 1) single-feature quantile bands (fit bins on TRAIN)
    singles: list[dict] = []
    for feat in cm.MINEABLE_FEATURES:
        if feat not in train.columns:
            continue
        vals = pd.to_numeric(train[feat], errors="coerce")
        valid = train[vals.notna()]
        if len(valid) < cfg["min_train_trades"] or vals.nunique() < 4:
            continue
        try:
            bins = pd.qcut(pd.to_numeric(valid[feat], errors="coerce"), q=cfg["qbins"], duplicates="drop")
        except ValueError:
            continue
        for interval in bins.cat.categories:
            items = [(feat, float(interval.left), float(interval.right))]
            tm = cm.block_metrics(train[_mask(train, items)])
            if tm["trades"] < cfg["min_train_trades"]:
                continue
            singles.append({"items": items, "train": tm})

    # 2) pair combos from the strongest, net-positive single bands
    pos = sorted([s for s in singles if s["train"]["net_pnl_rs"] > 0],
                 key=lambda s: s["train"]["profit_factor"], reverse=True)[: cfg["pair_top_n"]]
    candidates = list(singles)
    for a, b in itertools.combinations(pos, 2):
        if a["items"][0][0] == b["items"][0][0]:
            continue
        items = a["items"] + b["items"]
        tm = cm.block_metrics(train[_mask(train, items)])
        if tm["trades"] < cfg["min_train_trades"]:
            continue
        candidates.append({"items": items, "train": tm})

    # 3) score every candidate on validate + holdout + p-value (+ dedupe by text)
    seen: set[str] = set()
    scored: list[dict] = []
    pvals: dict[str, float] = {}
    for cand in candidates:
        text = _cond_terms(cand["items"])
        if text in seen:
            continue
        seen.add(text)
        tr_part = train[_mask(train, cand["items"])]
        vpart = sdf[sdf["_day"].isin(val_d)]
        vpart = vpart[_mask(vpart, cand["items"])]
        hpart = sdf[sdf["_day"].isin(hold_d)]
        hpart = hpart[_mask(hpart, cand["items"])]
        p = _day_block_p(tr_part, rng, cfg["n_boot"])
        rec = {
            "side": side, "items": cand["items"], "condition": text,
            "kind": "single" if len(cand["items"]) == 1 else "pair",
            "train": cand["train"], "val": cm.block_metrics(vpart), "holdout": cm.block_metrics(hpart),
            "day_block_p": p, "market_alpha": _market_alpha(pd.concat([tr_part, vpart])),
            "wf_fold_frac": _wf_frac(sdf, cand["items"], folds, cfg),
        }
        scored.append(rec)
        pvals[text] = p
    return scored, pvals


def survivor_ok(rec: dict, fdr_sig: dict[str, bool], cfg: dict) -> bool:
    tr, va, ho = rec["train"], rec["val"], rec["holdout"]
    if tr["trades"] < cfg["min_train_trades"] or tr["profit_factor"] < cfg["train_pf_min"] or tr["net_pnl_rs"] <= 0:
        return False
    if va["trades"] < cfg["min_val_trades"] or va["days"] < cfg["min_val_days"]:
        return False
    if va["profit_factor"] < cfg["val_pf_min"] or va["net_pnl_rs"] <= 0:
        return False
    if ho["net_pnl_rs"] <= 0:
        return False
    if tr["top1_day_share"] > cfg["top1_share_max"]:
        return False
    if rec["day_block_p"] > cfg["day_block_p_max"]:
        return False
    if not fdr_sig.get(rec["condition"], False):
        return False
    if rec["wf_fold_frac"] < cfg["wf_fold_frac_min"]:
        return False
    if rec["side"] == "SHORT":
        a = rec["market_alpha"]
        if not (isinstance(a, float) and math.isfinite(a) and a > 0):
            return False
    return True


# ---- recent-window monitoring sim table (for the shadow monitor) -----------
def build_recent_sim(cfg: dict, store: cm.MinuteStore) -> pd.DataFrame:
    files = sorted(cm.TRUTH_DIR.glob("truth_table_*.csv"))[-cfg["monitor_sessions"]:]
    parts: list[pd.DataFrame] = []
    _, liquid = cm.load_adv_map(cfg["adv_min_cr"])
    for path in files:
        try:
            t = pd.read_csv(path, low_memory=False)
        except Exception:
            continue
        if t.empty or "ticker" not in t.columns or "side" not in t.columns:
            continue
        t = cm.rename_to_canon(t, "truth")
        t["ticker"] = t["ticker"].astype(str).str.upper().str.strip()
        t["side"] = t["side"].astype(str).str.upper().str.strip()
        t = t[t["side"].isin(["LONG", "SHORT"])].copy()
        if liquid:
            t = t[t["ticker"].isin(liquid)].copy()
        if "signal_time_ist" not in t.columns:
            continue
        t["_sig_ns"] = t["signal_time_ist"].map(cm.to_ns)
        t = t[t["_sig_ns"] > 0]
        for _, row in t.iterrows():
            arr = store.load(row["ticker"])
            r = cm.resolve_one(arr, int(row["_sig_ns"]), row["side"], cfg["sl_pct"], cfg["tgt_pct"], cfg["cost_bps"])
            if r is None:
                continue
            rec = {
                "truth_date": cm.ist_day_str(int(row["_sig_ns"])),
                "direction": row["side"],
                "pipeline_level": "RAW_SCANNER",
                "ticker": row["ticker"],
                "setup": row.get("setup", ""),
                "outcome": r["outcome"],
                "pnl_rs": r["pnl_rs"],
                "gross_pnl_rs": r["gross_pnl_rs"],
                "cost_rs": r["cost_rs"],
            }
            for canon in cm.MINEABLE_FEATURES:
                rec[canon] = cm.sf(row.get(canon))
            parts.append(rec)
    return pd.DataFrame(parts)


# ---- report ----------------------------------------------------------------
def _fmt(v, d=2):
    try:
        f = float(v)
    except Exception:
        return str(v)
    if math.isinf(f):
        return "inf"
    return "" if math.isnan(f) else f"{f:,.{d}f}"


def _pat_table(rows: list[dict], title: str) -> list[str]:
    if not rows:
        return [f"_{title}: none._"]
    out = ["| side | kind | condition | tr_pf | tr_n | val_pf | val_n | hold_pf | hold_n | p | wf | mkt_alpha |",
           "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |"]
    for r in rows[:40]:
        out.append("| " + " | ".join([
            r["side"], r["kind"], r["condition"].replace("|", "/"),
            _fmt(r["train"]["profit_factor"]), str(r["train"]["trades"]),
            _fmt(r["val"]["profit_factor"]), str(r["val"]["trades"]),
            _fmt(r["holdout"]["profit_factor"]), str(r["holdout"]["trades"]),
            _fmt(r["day_block_p"], 3), _fmt(r["wf_fold_frac"], 2), _fmt(r["market_alpha"], 1),
        ]) + " |")
    return out


def run(cfg: dict, since: str | None, limit: int | None) -> dict:
    t0 = time.perf_counter()
    day = _now().date().isoformat()
    _status("RUNNING", phase="LOAD_POOL", day=day)
    store = cm.MinuteStore()
    pool = load_pool(cfg, since, limit)
    _status("RUNNING", phase="RESOLVE", day=day, pool_rows=len(pool))
    res = resolve_pool(pool, cfg, store)
    if res.empty:
        _status("DONE", phase="NO_DATA", day=day)
        _write_text(LATEST_DIR / f"latest_{SESSION_SLUG}.md", f"# Full-Pipeline Entry Research v2 - {day}\n\nNo resolvable pool rows.\n")
        return {"status": "NO_DATA", "day": day}

    days = sorted(res["_day"].unique())
    train_d, val_d, hold_d = split_days(days, cfg)
    rng = np.random.default_rng(20260708)

    _status("RUNNING", phase="MINE", day=day, resolved=len(res))
    all_scored: list[dict] = []
    all_p: dict[str, float] = {}
    for side in ("LONG", "SHORT"):
        scored, pvals = mine_side(side, res, train_d, val_d, hold_d, cfg, rng)
        for k, v in pvals.items():
            all_p[f"{side}::{k}"] = v
        all_scored.extend(scored)

    fdr_raw = wfg._benjamini_hochberg(all_p, cfg["fdr_alpha"]) if all_p else {}
    fdr_sig = {k.split("::", 1)[1]: v for k, v in fdr_raw.items()}

    survivors = [r for r in all_scored if survivor_ok(r, fdr_sig, cfg)]
    survivors.sort(key=lambda r: (r["holdout"]["net_pnl_rs"], r["val"]["net_pnl_rs"]), reverse=True)
    rejects = [r for r in all_scored if r not in survivors and r["train"]["net_pnl_rs"] > 0]
    rejects.sort(key=lambda r: r["train"]["net_pnl_rs"], reverse=True)

    # ---- emit pattern candidates (shadow-compatible schema) ----
    def _pat_row(r: dict, status: str) -> dict:
        return {
            "direction": r["side"], "kind": r["kind"], "condition": r["condition"],
            "honesty_status": status, "research_version": "v2",
            "train_trades": r["train"]["trades"], "train_days": r["train"]["days"],
            "train_pnl_rs": r["train"]["net_pnl_rs"], "train_profit_factor": r["train"]["profit_factor"],
            "val_trades": r["val"]["trades"], "val_days": r["val"]["days"],
            "val_pnl_rs": r["val"]["net_pnl_rs"], "val_profit_factor": r["val"]["profit_factor"],
            "holdout_trades": r["holdout"]["trades"], "holdout_pnl_rs": r["holdout"]["net_pnl_rs"],
            "holdout_profit_factor": r["holdout"]["profit_factor"],
            "day_block_p": r["day_block_p"], "fdr_significant": bool(fdr_sig.get(r["condition"], False)),
            "top1_day_share_train": r["train"]["top1_day_share"], "wf_fold_frac": r["wf_fold_frac"],
            "market_alpha": r["market_alpha"], "exit_sl_pct": cfg["sl_pct"], "exit_tgt_pct": cfg["tgt_pct"],
            "discovered_date": day,
        }

    pat_df = pd.DataFrame([_pat_row(r, "PASS_VALIDATION_RESEARCH_ONLY") for r in survivors])
    rej_df = pd.DataFrame([_pat_row(r, "REJECT") for r in rejects])

    # ---- direction summary on resolved pool ----
    dir_rows = []
    for side in ("LONG", "SHORT"):
        for name, dset in (("train", train_d), ("validate", val_d), ("holdout", hold_d)):
            m = cm.block_metrics(res[(res["side"] == side) & (res["_day"].isin(dset))])
            dir_rows.append({"side": side, "window": name, **m})
    dir_df = pd.DataFrame(dir_rows)

    _status("RUNNING", phase="MONITOR_SIM", day=day)
    sim_df = build_recent_sim(cfg, store)

    # ---- write outputs ----
    paths = {
        "pattern_candidates_csv": LATEST_DIR / "latest_pattern_candidates.csv",
        "rejected_patterns_csv": LATEST_DIR / "latest_rejected_patterns.csv",
        "direction_summary_csv": LATEST_DIR / "latest_direction_summary.csv",
        "simulated_candidates_csv": LATEST_DIR / "latest_simulated_candidates.csv",
    }
    (pat_df if not pat_df.empty else pd.DataFrame(columns=["direction", "condition"])).to_csv(paths["pattern_candidates_csv"], index=False)
    (rej_df if not rej_df.empty else pd.DataFrame(columns=["direction", "condition"])).to_csv(paths["rejected_patterns_csv"], index=False)
    dir_df.replace([np.inf, -np.inf], np.nan).to_csv(paths["direction_summary_csv"], index=False)
    (sim_df if not sim_df.empty else pd.DataFrame(columns=["truth_date", "direction"])).to_csv(paths["simulated_candidates_csv"], index=False)

    elapsed = time.perf_counter() - t0
    md = _build_report(day, cfg, since, res, days, train_d, val_d, hold_d, dir_df, survivors, rejects, sim_df, len(all_scored), store, elapsed)
    latest_md = LATEST_DIR / f"latest_{SESSION_SLUG}.md"
    _write_text(latest_md, md)
    _write_text(REPORT_DIR / f"{SESSION_SLUG}_{day}.md", md)

    payload = {
        "status": "DONE", "day": day, "cfg": cfg, "since": since,
        "pool_rows": int(len(pool)), "resolved_rows": int(len(res)),
        "train_days": len(train_d), "val_days": len(val_d), "holdout_days": len(hold_d),
        "tested_configs": int(len(all_scored)), "survivors": int(len(survivors)),
        "latest_report": str(latest_md),
        "latest_outputs": {k: str(v) for k, v in paths.items()},
        "elapsed_sec": elapsed,
        "missing_bar_tickers": sorted(store.missing)[:40],
    }
    _write_json(LATEST_DIR / f"latest_{SESSION_SLUG}.json", payload)
    _write_json(REPORT_DIR / f"{SESSION_SLUG}_{day}.json", payload)
    _status("DONE", phase="COMPLETE", day=day, resolved=len(res), survivors=len(survivors), tested=len(all_scored), elapsed_sec=round(elapsed, 1))
    print(f"[{SESSION_SLUG}] DONE day={day} resolved={len(res)} tested={len(all_scored)} survivors={len(survivors)} report={latest_md}", flush=True)
    return payload


def _build_report(day, cfg, since, res, days, train_d, val_d, hold_d, dir_df, survivors, rejects, sim_df, tested, store, elapsed) -> str:
    L = [f"# Full-Pipeline Entry Research v2 - {day}", "",
         "Read-only governed setup miner. No live scanner/filter/setup/executor/conf is modified.", "",
         "## Parameters", ]
    L += [
        f"- Discovery pool: `{cm.POOL_CSV}` (rows used: `{len(res)}`, since=`{since or 'all'}`)",
        f"- ADV filter: Rs>=`{cfg['adv_min_cr']:.0f}`cr (from `configs/universe.csv`)",
        f"- Sides: real `side` column (no force-inversion); shorts require market-alpha>0",
        f"- Exit: intraday/EOD 15:20 resolver, per-side bracket `+{cfg['tgt_pct']:.2f}%/-{cfg['sl_pct']:.2f}%` "
        f"(per-mask exit optimization deferred), net of v6 flat `{cfg['cost_bps']:.0f}`bps (+3 on stops), notional Rs `{cm.NOTIONAL_RS:,.0f}`",
        f"- Split: train=`{len(train_d)}`d / validate=`{len(val_d)}`d / **locked holdout=`{len(hold_d)}`d**, embargo=`{cfg['embargo_days']}`d "
        f"(span `{min(days)}`..`{max(days)}`)",
        f"- Gates: trainPF>=`{cfg['train_pf_min']}`, valPF>=`{cfg['val_pf_min']}`+pnl>0, holdout pnl>0, "
        f"day-block p<=`{cfg['day_block_p_max']}`, BH-FDR alpha=`{cfg['fdr_alpha']}`, top-1-day share<=`{cfg['top1_share_max']}`, "
        f"walk-forward fold-frac>=`{cfg['wf_fold_frac_min']}`",
        f"- Tested configs: `{tested}` | Missing 1-min tickers: `{len(store.missing)}` | Runtime: `{elapsed:.1f}s`",
        "",
        "## Resolved pool by side x window (net of cost)",
    ]
    if not dir_df.empty:
        L += ["| side | window | trades | days | win_rate | net_pnl_rs | profit_factor |",
              "| --- | --- | --- | --- | --- | --- | --- |"]
        for _, r in dir_df.iterrows():
            L.append("| " + " | ".join([r["side"], r["window"], str(int(r["trades"])), str(int(r["days"])),
                                        _fmt(r["win_rate"] * 100, 1) + "%", _fmt(r["net_pnl_rs"]), _fmt(r["profit_factor"])]) + " |")
    L += ["", f"## Survivors (DISCOVERED, research-only) - {len(survivors)}"]
    L += _pat_table(survivors, "Survivors")
    L += ["", f"## Rejected (train-positive but failed governance) - top {min(len(rejects), 40)}"]
    L += _pat_table(rejects, "Rejected")
    L += ["", "## Forward-monitoring sim table",
          f"- Recent truth sessions resolved for the shadow monitor: `{0 if sim_df is None or sim_df.empty else len(sim_df)}` rows "
          f"(last `{cfg['monitor_sessions']}` sessions).", ""]
    L += ["## Honest notes",
          "- Exit is a single per-side bracket; per-mask exit optimization, PBO/CSCV, and dual-file conf emission are deferred (see plan).",
          "- Survivors are DISCOVERED only. Promotion needs shadow burn-in, a live-parity check, and your explicit approval.",
          f"- Data wall: dense history is ~7-8 months and the locked holdout is ~{len(hold_d)} days; zero survivors is an honest, expected outcome.",
          ""]
    return "\n".join(L)


def main(argv: Iterable[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--since", default="", help="Only pool rows on/after this YYYY-MM-DD (speed/testing)")
    ap.add_argument("--limit", type=int, default=0, help="Cap pool rows (testing only)")
    ap.add_argument("--sl-pct", type=float, default=DEF["sl_pct"])
    ap.add_argument("--tgt-pct", type=float, default=DEF["tgt_pct"])
    ap.add_argument("--cost-bps", type=float, default=DEF["cost_bps"])
    ap.add_argument("--adv-min-cr", type=float, default=DEF["adv_min_cr"])
    args = ap.parse_args(list(argv) if argv is not None else None)
    cfg = dict(DEF)
    cfg.update(sl_pct=args.sl_pct, tgt_pct=args.tgt_pct, cost_bps=args.cost_bps, adv_min_cr=args.adv_min_cr)
    try:
        run(cfg, args.since or None, args.limit or None)
        return 0
    except Exception as exc:
        _status("ERROR", phase="FAILED", error=f"{type(exc).__name__}: {exc}")
        raise


if __name__ == "__main__":
    raise SystemExit(main())
