"""Research-only PF 1.4 full loop for A_PULLBACK_C2_THEN_BREAK_C2_LOW.

This script stays inside Train_and_Test/setup_pf_1_4_full_loop/<SETUP>/ and
writes only local research artifacts. It reuses setup_train_test.py plus the
shared pf_band engine for entry, 1-minute exit resolution, costs, dedupe, mask,
pre-momentum, guards, and portfolio overlay.
"""
from __future__ import annotations

import argparse
import json
import math
import random
import sys
import time
from datetime import date
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

HERE = Path(__file__).resolve()
WORK = HERE.parents[1]
TT_DIR = HERE.parents[3]
REPO = TT_DIR.parent
ENGINE_DIR = TT_DIR / "setup_pf_1_4_approval_loop" / "_engine"
for p in (REPO, TT_DIR, ENGINE_DIR):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))

import final_setup_conf as fsc  # noqa: E402
import setup_train_test as tt  # noqa: E402
import pf_band_fitval_loop as eng  # noqa: E402

try:
    import optuna  # type: ignore

    optuna.logging.set_verbosity(optuna.logging.WARNING)
    HAVE_OPTUNA = True
except Exception:
    HAVE_OPTUNA = False

SETUP = "A_PULLBACK_C2_THEN_BREAK_C2_LOW"
SIDE = "SHORT"
FNAME = "historical_all_available_pre_dedupe_live_candidates.csv"
REQUESTED_TRAIN = (pd.Timestamp("2026-03-01"), pd.Timestamp("2026-05-30"))
REQUESTED_TEST = (pd.Timestamp("2026-06-01"), pd.Timestamp("2026-07-02"))
FIT_FRAC = 0.60
PF_LO = 1.30
PF_HI = 1.80
TEST_PF_MIN = 1.40

SOURCE_FILES = [
    r"C:\TradingData\eqidv2\outputs_ID_v11_unified_pool\historical_all_available_pre_dedupe_live_candidates.csv",
    r"C:\TradingData\eqidv2\outputs_ID_v11_conf_fresh_20260629\historical_all_available_raw_candidates.csv",
    r"C:\TradingData\eqidv2\outputs_ID_v11_unified_recent_raw_0622\historical_all_available_raw_candidates.csv",
    r"C:\TradingData\eqidv2\outputs_ID_v11_unified_recent_raw_0624\historical_all_available_raw_candidates.csv",
    r"C:\TradingData\eqidv2\backtesting_result_v11\2026-07-01\historical_full_day_raw_candidates.csv",
    r"C:\TradingData\eqidv2\backtesting_result_v11\2026-07-01\live_parity_raw_candidates.csv",
    r"C:\TradingData\eqidv2\backtesting_result_v11\2026-07-01\historical_full_day_pre_dedupe_live_candidates.csv",
    r"C:\TradingData\eqidv2\backtesting_result_v11\2026-07-01\live_parity_pre_dedupe_live_candidates.csv",
]

MASK_FEATS = list(dict.fromkeys(eng.MASK_FEATS + [
    "rsi", "RSI", "adx", "ADX", "macd_hist", "MACD_Hist", "macd_hist_delta",
    "ema20_slope", "ema20_slope_3bar", "EMA_20", "EMA20", "EMA_50", "EMA50",
    "market_ret_pct", "market_abs_ret_pct", "signal_minute", "notional",
]))
PM_FEATS = eng.PM_FEATS
QGRID = [0.05, 0.10, 0.20, 0.30, 0.40, 0.50, 0.60, 0.70, 0.80, 0.90, 0.95, 0.975, 0.99]
SL_GRID = [0.50, 0.70, 0.85, 0.90, 1.00, 1.10, 1.20, 1.50]
TGT_GRID = [0.60, 0.80, 1.00, 1.25, 1.50, 2.00, 2.50]
MIN_SLOTS = ["09:30", "09:45", "10:00", "10:30", "11:00"]
MAX_SLOTS = ["11:30", "12:00", "12:30", "13:00", "14:00", "14:30"]
MAXPOS_GRID = [10, 20]
DLOSS_GRID = [0.0, 4000.0]
PM_CACHE_SL = 1.20


class RandTrial:
    def __init__(self, rng: random.Random) -> None:
        self.rng = rng
        self.params: dict[str, Any] = {}

    def suggest_categorical(self, name: str, choices: list[Any]) -> Any:
        value = choices[self.rng.randrange(len(choices))]
        self.params[name] = value
        return value

    def suggest_int(self, name: str, lo: int, hi: int) -> int:
        value = self.rng.randint(lo, hi)
        self.params[name] = value
        return value

    def set_user_attr(self, *args: Any, **kwargs: Any) -> None:
        return None


def as_json(obj: Any) -> str:
    return json.dumps(obj, indent=2, default=str)


def clean_float(value: Any) -> Any:
    if isinstance(value, (float, np.floating)):
        if not math.isfinite(float(value)):
            return None
        return round(float(value), 6)
    if isinstance(value, dict):
        return {k: clean_float(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [clean_float(v) for v in value]
    return value


def conf_to_cfg(block: dict[str, Any]) -> dict[str, Any]:
    ex = block.get("exit", {}) or {}
    guard = dict(block.get("entry_guards", {}) or {})
    return {
        "sl": float(ex.get("sl_pct", 1.20)),
        "tgt": float(ex.get("tgt_pct", 1.50)),
        "mask_terms": [tuple(t) for t in (block.get("mask_terms", []) or [])],
        "premom_terms": [tuple(t) for t in (block.get("pre_momentum_terms", []) or [])],
        "guard": guard or None,
        "status": "OK",
        "max_positions": int(block.get("max_positions") or 20),
        "daily_loss_rs": float(block.get("daily_loss_rs") or 0.0),
    }


def cfg_to_block(cfg: dict[str, Any]) -> dict[str, Any]:
    return {
        "side": SIDE,
        "exit": {"sl_pct": float(cfg["sl"]), "tgt_pct": float(cfg["tgt"])},
        "mask_terms": [list(t) for t in cfg.get("mask_terms", []) or []],
        "pre_momentum_terms": [list(t) for t in cfg.get("premom_terms", []) or []],
        "entry_guards": dict(cfg.get("guard") or {}),
        "max_positions": int(cfg.get("max_positions") or 20),
        "daily_loss_rs": float(cfg.get("daily_loss_rs") or 0.0),
    }


def terms_text(terms: list[tuple[str, str, Any]] | tuple[tuple[str, str, Any], ...] | None) -> str:
    if not terms:
        return "(none)"
    return "; ".join(f"{a}{o}{b}" for a, o, b in terms)


def _source_day_summary(df: pd.DataFrame) -> dict[str, Any]:
    if df.empty:
        return {"rows": 0, "sessions": 0, "first": None, "last": None}
    ts = pd.to_datetime(df["signal_time_ist"], errors="coerce", utc=True)
    ts = ts.dt.tz_convert("Asia/Kolkata").dt.tz_localize(None)
    sessions = sorted(ts.dt.normalize().dropna().unique())
    return {
        "rows": int(len(df)),
        "sessions": int(len(sessions)),
        "first": str(pd.Timestamp(sessions[0]).date()) if sessions else None,
        "last": str(pd.Timestamp(sessions[-1]).date()) if sessions else None,
    }


def recreate_pool() -> dict[str, Any]:
    pools_dir = WORK / "pools"
    pools_dir.mkdir(parents=True, exist_ok=True)
    out_file = pools_dir / FNAME

    frames: list[pd.DataFrame] = []
    src_reports = []
    for src in SOURCE_FILES:
        path = Path(src)
        if not path.exists():
            src_reports.append({"path": src, "exists": False, "setup_rows": 0})
            continue
        try:
            df = pd.read_csv(path, low_memory=False)
        except Exception as exc:
            src_reports.append({"path": src, "exists": True, "error": repr(exc), "setup_rows": 0})
            continue
        if "setup" not in df.columns or "signal_time_ist" not in df.columns:
            src_reports.append({"path": src, "exists": True, "rows": int(len(df)), "setup_rows": 0, "usable": False})
            continue
        d = df[df["setup"].astype(str).str.strip().eq(SETUP)].copy()
        d["_source_path"] = src
        src_reports.append({
            "path": src,
            "exists": True,
            "rows": int(len(df)),
            "setup_rows": int(len(d)),
            "usable": True,
            **_source_day_summary(d),
        })
        if len(d):
            frames.append(d)

    if not frames:
        raise SystemExit(f"No source rows found for {SETUP}")

    cols = list(dict.fromkeys(c for f in frames for c in f.columns))
    pool = pd.concat([f.reindex(columns=cols) for f in frames], ignore_index=True, sort=False)
    ts = pd.to_datetime(pool["signal_time_ist"], errors="coerce", utc=True)
    pool["_sig_local"] = ts.dt.tz_convert("Asia/Kolkata").dt.tz_localize(None)
    pool = pool.dropna(subset=["_sig_local"]).copy()
    pool = pool[(pool["_sig_local"] >= REQUESTED_TRAIN[0]) & (pool["_sig_local"] <= REQUESTED_TEST[1] + pd.Timedelta(days=1))]
    key = ["ticker", "side", "setup", "signal_time_ist"]
    for k in key:
        if k not in pool.columns:
            pool[k] = ""
    before = len(pool)
    pool = pool.sort_values(["_sig_local", "_source_path"]).drop_duplicates(subset=key, keep="first")
    pool = pool.reset_index(drop=True)
    pool.drop(columns=["_sig_local"], errors="ignore").to_csv(out_file, index=False)

    ts2 = pd.to_datetime(pool["signal_time_ist"], errors="coerce", utc=True)
    ts2 = ts2.dt.tz_convert("Asia/Kolkata").dt.tz_localize(None)
    all_sessions = sorted(ts2.dt.normalize().dropna().unique())
    tr_sessions = [s for s in all_sessions if REQUESTED_TRAIN[0] <= pd.Timestamp(s) <= REQUESTED_TRAIN[1]]
    te_sessions = [s for s in all_sessions if REQUESTED_TEST[0] <= pd.Timestamp(s) <= REQUESTED_TEST[1]]

    requested_train_dates = pd.date_range(REQUESTED_TRAIN[0], REQUESTED_TRAIN[1], freq="B")
    requested_test_dates = pd.date_range(REQUESTED_TEST[0], REQUESTED_TEST[1], freq="B")
    actual_train_set = {pd.Timestamp(s).date().isoformat() for s in tr_sessions}
    actual_test_set = {pd.Timestamp(s).date().isoformat() for s in te_sessions}
    missing_train = [d.date().isoformat() for d in requested_train_dates if d.date().isoformat() not in actual_train_set]
    missing_test = [d.date().isoformat() for d in requested_test_dates if d.date().isoformat() not in actual_test_set]

    report = {
        "setup": SETUP,
        "pool_path": str(out_file),
        "source_reports": src_reports,
        "rows_pre_dedupe": int(before),
        "rows": int(len(pool)),
        "columns": list(pool.columns),
        "available_first_session": str(pd.Timestamp(all_sessions[0]).date()) if all_sessions else None,
        "available_last_session": str(pd.Timestamp(all_sessions[-1]).date()) if all_sessions else None,
        "requested_train": [str(REQUESTED_TRAIN[0].date()), str(REQUESTED_TRAIN[1].date())],
        "requested_test": [str(REQUESTED_TEST[0].date()), str(REQUESTED_TEST[1].date())],
        "actual_train_sessions": [str(pd.Timestamp(s).date()) for s in tr_sessions],
        "actual_test_sessions": [str(pd.Timestamp(s).date()) for s in te_sessions],
        "missing_train_business_dates": missing_train,
        "missing_test_business_dates": missing_test,
    }
    (pools_dir / "pool_manifest.json").write_text(as_json(report), encoding="utf-8")
    return report


def setup_tt(pool_dir: Path, slippage_bps: float) -> None:
    tt.POOL_DIRS = [pool_dir]
    tt.POOL_DIR = pool_dir
    tt.SLIPPAGE_BPS = float(slippage_bps)
    tt.MAX_POSITIONS = 20
    tt.DAILY_LOSS_RS = 0.0
    tt._entry.cache_clear()
    tt._resolve_full.cache_clear()
    tt._premom.cache_clear()


def premom_cache_path() -> Path:
    return WORK / "pools" / f"premom_cache_sl_{str(PM_CACHE_SL).replace('.', 'p')}.csv"


def _row_key(df: pd.DataFrame) -> pd.Series:
    return (
        df["ticker"].astype(str)
        + "|"
        + df["side"].astype(str)
        + "|"
        + df["tt_entry_iso"].astype(str)
        + "|"
        + df["tt_fill"].round(4).astype(str)
        + "|"
        + df["tt_sig_ts"].astype(str)
    )


def attach_cached_premom(df: pd.DataFrame) -> pd.DataFrame:
    """Attach pre-momentum features once, using the baseline 1.20 stop.

    setup_train_test.apply_premom_terms calls the same tt._premom primitive per
    evaluation. This cache keeps the order faithful while avoiding repeated
    1-minute feature reads across hundreds of iterations.
    """
    out = df.copy()
    if out.empty:
        for feat in PM_FEATS:
            out[feat] = np.nan
        out["premom_reason"] = ""
        return out

    out["_pm_key"] = _row_key(out)
    cache_file = premom_cache_path()
    cache = pd.DataFrame()
    if cache_file.exists():
        try:
            cache = pd.read_csv(cache_file, low_memory=False)
        except Exception:
            cache = pd.DataFrame()
    if cache.empty or "_pm_key" not in cache.columns:
        cache = pd.DataFrame(columns=["_pm_key", "premom_reason", *PM_FEATS])

    have = set(cache["_pm_key"].astype(str))
    need = out[~out["_pm_key"].astype(str).isin(have)].copy()
    if len(need):
        print(f"[premom-cache] computing {len(need)} rows at SL={PM_CACHE_SL} (existing cache {len(cache)})", flush=True)
        recs = []
        for i, (_, rr) in enumerate(need.iterrows(), 1):
            try:
                feats, reason = tt._premom(
                    str(rr["ticker"]),
                    str(rr["side"]),
                    str(rr["tt_entry_iso"]),
                    float(rr["tt_fill"]),
                    PM_CACHE_SL,
                    rr["tt_sig_ts"].isoformat(),
                )
                fd = dict(feats) if not reason else {}
            except Exception as exc:
                reason = repr(exc)
                fd = {}
            rec = {"_pm_key": rr["_pm_key"], "premom_reason": reason or ""}
            for feat in PM_FEATS:
                rec[feat] = fd.get(feat, np.nan)
            recs.append(rec)
            if i % 500 == 0:
                print(f"[premom-cache] {i}/{len(need)}", flush=True)
        add = pd.DataFrame(recs)
        cache = pd.concat([cache, add], ignore_index=True).drop_duplicates("_pm_key", keep="last")
        cache_file.parent.mkdir(parents=True, exist_ok=True)
        cache.to_csv(cache_file, index=False)
        print(f"[premom-cache] wrote {cache_file} rows={len(cache)}", flush=True)

    merged = out.merge(cache, on="_pm_key", how="left", suffixes=("", "_pm"))
    for feat in PM_FEATS:
        if f"{feat}_pm" in merged.columns:
            merged[feat] = pd.to_numeric(merged[f"{feat}_pm"], errors="coerce")
            merged = merged.drop(columns=[f"{feat}_pm"])
        elif feat not in merged.columns:
            merged[feat] = np.nan
    if "premom_reason_pm" in merged.columns:
        merged["premom_reason"] = merged["premom_reason_pm"].fillna("")
        merged = merged.drop(columns=["premom_reason_pm"])
    return merged.drop(columns=["_pm_key"], errors="ignore")


def split_frames(pool_dir: Path) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    setup_tt(pool_dir, 15.0)
    pool = tt.load_pool()
    pool = pool[pool["setup"].astype(str).str.strip().eq(SETUP)].reset_index(drop=True)
    train_raw = pool[(pool["_day"] >= REQUESTED_TRAIN[0]) & (pool["_day"] <= REQUESTED_TRAIN[1])].reset_index(drop=True)
    test_raw = pool[(pool["_day"] >= REQUESTED_TEST[0]) & (pool["_day"] <= REQUESTED_TEST[1])].reset_index(drop=True)
    train = tt.attach_entries(train_raw)
    test = tt.attach_entries(test_raw)
    both = pd.concat([train.assign(_split_name="TRAIN"), test.assign(_split_name="TEST")], ignore_index=True, sort=False)
    both = attach_cached_premom(both)
    train = both[both["_split_name"].eq("TRAIN")].drop(columns=["_split_name"]).reset_index(drop=True)
    test = both[both["_split_name"].eq("TEST")].drop(columns=["_split_name"]).reset_index(drop=True)

    train_sessions = sorted(train_raw["_day"].dt.strftime("%Y-%m-%d").unique())
    test_sessions = sorted(test_raw["_day"].dt.strftime("%Y-%m-%d").unique())
    n_fit = max(1, int(round(FIT_FRAC * len(train_sessions)))) if train_sessions else 0
    fit_sessions = train_sessions[:n_fit]
    val_sessions = train_sessions[n_fit:]
    fit = train[train["_day"].dt.strftime("%Y-%m-%d").isin(fit_sessions)].reset_index(drop=True)
    val = train[train["_day"].dt.strftime("%Y-%m-%d").isin(val_sessions)].reset_index(drop=True)
    split = {
        "train_sessions": train_sessions,
        "test_sessions": test_sessions,
        "fit_sessions": fit_sessions,
        "val_sessions": val_sessions,
        "rows": {
            "pool": int(len(pool)),
            "train_raw": int(len(train_raw)),
            "test_raw": int(len(test_raw)),
            "train_entries": int(len(train)),
            "test_entries": int(len(test)),
            "fit_entries": int(len(fit)),
            "val_entries": int(len(val)),
        },
    }
    return fit, val, train, test, split


def apply_premom_cached(df: pd.DataFrame, terms: list[tuple[str, str, Any]] | tuple[tuple[str, str, Any], ...] | None) -> pd.DataFrame:
    if df.empty or not terms:
        return df
    keep = pd.Series(True, index=df.index)
    for feat, op, thr in terms:
        x = pd.to_numeric(df[feat], errors="coerce") if feat in df.columns else pd.Series(np.nan, index=df.index)
        if op == ">=":
            keep &= x >= float(thr)
        elif op == "<=":
            keep &= x <= float(thr)
        elif op == ">":
            keep &= x > float(thr)
        elif op == "<":
            keep &= x < float(thr)
        elif op == "!=":
            keep &= x != float(thr)
        else:
            keep &= x == float(thr)
    return df[keep.fillna(False)]


def local_eval_family(cfg: dict[str, Any], df: pd.DataFrame, detail: bool) -> dict[str, Any]:
    rows = df[df["setup"].astype(str).eq(SETUP)].copy()
    rows = tt.apply_guards(rows, cfg.get("guard"))
    rows = apply_premom_cached(rows, cfg.get("premom_terms"))
    deduped = tt.dedupe_family(rows)
    book = tt.apply_mask_terms(deduped, cfg.get("mask_terms", []))
    if book.empty:
        return empty_metrics(book, detail)
    exits = {SETUP: (float(cfg["sl"]), float(cfg["tgt"]))}
    book = tt._apply_regime_align(book)
    book = tt._apply_portfolio_overlay(book, exits)
    if book.empty:
        return empty_metrics(book, detail)
    net = tt.resolve_book(book, exits)
    return metrics_from_book(book, net, exits, detail)


def empty_metrics(book: pd.DataFrame, detail: bool) -> dict[str, Any]:
    return {
        "n": 0, "net_pf": 0.0, "net_pnl": 0.0, "day_block_p": None,
        "wins": 0, "losses": 0, "win_rate": 0.0, "gross_profit": 0.0, "gross_loss": 0.0,
        "avg_win": 0.0, "avg_loss": 0.0, "max_dd": 0.0, "n_days": 0, "n_syms": 0,
        "trades_per_day": 0.0, "sl_cnt": 0, "tgt_cnt": 0, "eod_cnt": 0, "other_cnt": 0,
        "target_rate": 0.0, "trade_dom_gross": 9.99, "day_dom": 9.99, "sym_dom": 9.99,
        "top_day": None, "top_sym": None, "detail": pd.DataFrame() if detail else pd.DataFrame(),
    }


def metrics_from_book(book: pd.DataFrame, net: np.ndarray, exits: dict[str, tuple], detail: bool) -> dict[str, Any]:
    finite = np.isfinite(net)
    book = book.loc[finite].reset_index(drop=True)
    netf = np.asarray(net[finite], dtype=float)
    det = tt.book_detail(book, exits) if (detail and len(book)) else pd.DataFrame()
    if detail and not det.empty:
        netf = det["net_pnl_rs"].to_numpy(dtype=float)
    gp = float(netf[netf > 0].sum()) if len(netf) else 0.0
    gl = float(-netf[netf < 0].sum()) if len(netf) else 0.0
    wins = netf[netf > 0]
    losses = netf[netf < 0]
    eq = netf.cumsum() if len(netf) else np.array([])
    dd = eq - np.maximum.accumulate(eq) if len(eq) else np.array([])
    total = float(netf.sum()) if len(netf) else 0.0
    day_dom = sym_dom = 9.99
    top_day = top_sym = None
    if len(book) and len(netf):
        day_sum = pd.Series(netf, index=book["_day"].to_numpy()).groupby(level=0).sum()
        sym_sum = pd.Series(netf, index=book["ticker"].to_numpy()).groupby(level=0).sum()
        if total > 0:
            day_dom = round(float(day_sum.max()) / total, 3)
            sym_dom = round(float(sym_sum.max()) / total, 3)
        top_day = f"{pd.Timestamp(day_sum.idxmax()).date()}: Rs{day_sum.max():,.0f}" if len(day_sum) else None
        top_sym = f"{sym_sum.idxmax()}: Rs{sym_sum.max():,.0f}" if len(sym_sum) else None
    if detail and not det.empty:
        oc = det["outcome"].astype(str).str.upper()
        sl_cnt = int((oc == "SL").sum())
        tgt_cnt = int((oc == "TARGET").sum())
        eod_cnt = int((oc == "EOD").sum())
        other_cnt = int((~oc.isin(["SL", "TARGET", "EOD"])).sum())
    else:
        sl_cnt = tgt_cnt = eod_cnt = other_cnt = 0
    day_p = tt._day_block_p(book, netf) if len(book) else float("nan")
    return {
        "n": int(len(netf)),
        "net_pf": round(float(tt._pf(netf)), 3) if len(netf) else 0.0,
        "net_pnl": round(total, 0),
        "day_block_p": None if not np.isfinite(day_p) else round(float(day_p), 4),
        "wins": int((netf > 0).sum()),
        "losses": int((netf < 0).sum()),
        "win_rate": round(float((netf > 0).mean()) * 100, 1) if len(netf) else 0.0,
        "gross_profit": round(gp, 0),
        "gross_loss": round(gl, 0),
        "avg_win": round(float(wins.mean()), 0) if len(wins) else 0.0,
        "avg_loss": round(float(losses.mean()), 0) if len(losses) else 0.0,
        "max_dd": round(float(dd.min()), 0) if len(dd) else 0.0,
        "n_days": int(book["_day"].nunique()) if len(book) else 0,
        "n_syms": int(book["ticker"].nunique()) if len(book) else 0,
        "trades_per_day": round(len(netf) / max(1, int(book["_day"].nunique()) if len(book) else 1), 2),
        "sl_cnt": sl_cnt,
        "tgt_cnt": tgt_cnt,
        "eod_cnt": eod_cnt,
        "other_cnt": other_cnt,
        "target_rate": round(float(tgt_cnt / len(netf)) * 100, 1) if len(netf) and detail else 0.0,
        "trade_dom_gross": round(float(netf.max()) / gp, 3) if gp > 0 and len(netf) else 9.99,
        "day_dom": day_dom,
        "sym_dom": sym_dom,
        "top_day": top_day,
        "top_sym": top_sym,
        "detail": det,
    }


def eval_cfg(cfg: dict[str, Any], df: pd.DataFrame, detail: bool = True) -> dict[str, Any]:
    tt.MAX_POSITIONS = int(cfg.get("max_positions") or 20)
    tt.DAILY_LOSS_RS = float(cfg.get("daily_loss_rs") or 0.0)
    return local_eval_family(cfg, df, detail)


def strip_detail(m: dict[str, Any]) -> dict[str, Any]:
    return {k: clean_float(v) for k, v in m.items() if k != "detail"}


def metric_line(m: dict[str, Any]) -> str:
    return (
        f"n={m.get('n', 0)} PF={m.get('net_pf')} net=Rs{float(m.get('net_pnl') or 0):,.0f} "
        f"win%={m.get('win_rate')} avgW=Rs{float(m.get('avg_win') or 0):,.0f} "
        f"avgL=Rs{float(m.get('avg_loss') or 0):,.0f} SL/TGT/EOD="
        f"{m.get('sl_cnt')}/{m.get('tgt_cnt')}/{m.get('eod_cnt')} "
        f"tpd={m.get('trades_per_day')} domT/D/S="
        f"{m.get('trade_dom_gross')}/{m.get('day_dom')}/{m.get('sym_dom')}"
    )


def mtable(m: dict[str, Any]) -> str:
    rows = [
        ("trades", m.get("n")),
        ("wins", m.get("wins")),
        ("losses", m.get("losses")),
        ("win rate", f"{m.get('win_rate')}%"),
        ("gross profit", f"Rs{float(m.get('gross_profit') or 0):,.0f}"),
        ("gross loss", f"Rs{float(m.get('gross_loss') or 0):,.0f}"),
        ("net PnL", f"Rs{float(m.get('net_pnl') or 0):,.0f}"),
        ("Profit Factor", m.get("net_pf")),
        ("average win", f"Rs{float(m.get('avg_win') or 0):,.0f}"),
        ("average loss", f"Rs{float(m.get('avg_loss') or 0):,.0f}"),
        ("avg win / avg loss ratio", round(abs(float(m.get("avg_win") or 0) / float(m.get("avg_loss") or -1)), 3) if float(m.get("avg_loss") or 0) else None),
        ("max drawdown", f"Rs{float(m.get('max_dd') or 0):,.0f}"),
        ("SL / target / EOD exits", f"{m.get('sl_cnt')} / {m.get('tgt_cnt')} / {m.get('eod_cnt')}"),
        ("trades per day", m.get("trades_per_day")),
        ("top trade gross-profit share", m.get("trade_dom_gross")),
        ("top day net share", m.get("day_dom")),
        ("top symbol net share", m.get("sym_dom")),
        ("day-block p", m.get("day_block_p")),
        ("top day", m.get("top_day")),
        ("top symbol", m.get("top_sym")),
    ]
    return "\n".join(f"| {a} | {b} |" for a, b in rows)


def band_reward(pf: float) -> float:
    if not math.isfinite(float(pf)):
        return -2.0
    pf = float(pf)
    if pf <= PF_HI:
        return pf
    return PF_HI - 1.6 * (pf - PF_HI)


def quantiles_for(df: pd.DataFrame) -> dict[str, dict[float, float]]:
    out: dict[str, dict[float, float]] = {}
    for feat in MASK_FEATS:
        if feat not in df.columns:
            continue
        s = pd.to_numeric(df[feat], errors="coerce").dropna()
        if len(s) >= 20 and s.nunique() > 1:
            out[feat] = {q: float(s.quantile(q)) for q in QGRID}
    return out


def premom_quantiles(df: pd.DataFrame, sample_n: int, seed: int) -> dict[str, dict[float, float]]:
    if df.empty:
        return {}
    pm = df.sample(n=min(sample_n, len(df)), random_state=seed) if len(df) > sample_n else df
    out: dict[str, dict[float, float]] = {}
    for feat in PM_FEATS:
        if feat not in pm.columns:
            continue
        s = pd.to_numeric(pm[feat], errors="coerce").dropna()
        if len(s) >= 20 and s.nunique() > 1:
            out[feat] = {q: float(s.quantile(q)) for q in QGRID}
    return out


def suggest_cfg(trial: Any, mask_quant: dict[str, dict[float, float]], pm_quant: dict[str, dict[float, float]], max_mask: int, max_pm: int) -> dict[str, Any]:
    def cat(name: str, choices: list[Any]) -> Any:
        return trial.suggest_categorical(name, choices)

    def integ(name: str, lo: int, hi: int) -> int:
        return trial.suggest_int(name, lo, hi)

    mask_terms = []
    avail_mask = [x for x in MASK_FEATS if x in mask_quant]
    for i in range(integ("n_mask", 0, max_mask)):
        if not avail_mask:
            break
        feat = cat(f"mask{i}_feat", avail_mask)
        op = cat(f"mask{i}_op", [">=", "<="])
        q = cat(f"mask{i}_q", QGRID)
        mask_terms.append((feat, op, round(float(mask_quant[feat][q]), 6)))

    premom_terms = []
    avail_pm = [x for x in PM_FEATS if x in pm_quant]
    for i in range(integ("n_pm", 0, max_pm)):
        if not avail_pm:
            break
        feat = cat(f"pm{i}_feat", avail_pm)
        op = cat(f"pm{i}_op", [">=", "<="])
        q = cat(f"pm{i}_q", QGRID)
        premom_terms.append((feat, op, round(float(pm_quant[feat][q]), 6)))

    guard: dict[str, Any] = {}
    if cat("use_min_slot", [False, True]):
        guard["min_slot"] = cat("min_slot", MIN_SLOTS)
    if cat("use_max_slot", [False, True]):
        guard["max_slot"] = cat("max_slot", MAX_SLOTS)
    top_n = cat("top_n", [0, 1, 2, 3])
    if top_n:
        guard["top_n"] = int(top_n)

    return {
        "sl": float(cat("sl", SL_GRID)),
        "tgt": float(cat("tgt", TGT_GRID)),
        "mask_terms": mask_terms,
        "premom_terms": premom_terms,
        "guard": guard or None,
        "status": "OK",
        "max_positions": int(cat("max_positions", MAXPOS_GRID)),
        "daily_loss_rs": float(cat("daily_loss_rs", DLOSS_GRID)),
    }


def concentration_ok(m: dict[str, Any]) -> tuple[bool, list[str]]:
    reasons: list[str] = []
    if (m.get("trade_dom_gross") or 9.99) > 0.35:
        reasons.append(f"top trade gross share {m.get('trade_dom_gross')} > 0.35")
    if (m.get("day_dom") or 9.99) > 0.40:
        reasons.append(f"top day net share {m.get('day_dom')} > 0.40")
    if (m.get("sym_dom") or 9.99) > 0.40:
        reasons.append(f"top symbol net share {m.get('sym_dom')} > 0.40")
    return (not reasons), reasons


def pass_candidate(train_m: dict[str, Any], test_m: dict[str, Any]) -> tuple[bool, list[str]]:
    reasons: list[str] = []
    if not (PF_LO <= float(train_m.get("net_pf") or 0) <= PF_HI):
        reasons.append(f"TRAIN PF {train_m.get('net_pf')} outside {PF_LO}-{PF_HI}")
    if float(test_m.get("net_pf") or 0) <= TEST_PF_MIN:
        reasons.append(f"TEST PF {test_m.get('net_pf')} <= {TEST_PF_MIN}")
    if int(train_m.get("n") or 0) < 20:
        reasons.append(f"TRAIN trades {train_m.get('n')} < 20")
    if int(test_m.get("n") or 0) < 5:
        reasons.append(f"TEST trades {test_m.get('n')} < 5")
    if float(train_m.get("net_pnl") or 0) <= 0:
        reasons.append("TRAIN net PnL <= 0")
    if float(test_m.get("net_pnl") or 0) <= 0:
        reasons.append("TEST net PnL <= 0")
    for label, m in (("TRAIN", train_m), ("TEST", test_m)):
        ok, dom_reasons = concentration_ok(m)
        if not ok:
            reasons.extend([f"{label}: {r}" for r in dom_reasons])
    return (not reasons), reasons


def trial_score(fit_m: dict[str, Any], val_m: dict[str, Any], min_fv: int, gap_lambda: float) -> float:
    nf, nv = int(fit_m.get("n") or 0), int(val_m.get("n") or 0)
    if nf < min_fv or nv < max(4, min_fv // 2):
        return -4.0 + min(nf, nv) / max(1, min_fv)
    pf_f = float(fit_m.get("net_pf") or 0.0)
    pf_v = float(val_m.get("net_pf") or 0.0)
    score = min(band_reward(pf_f), band_reward(pf_v)) - gap_lambda * abs(pf_f - pf_v)
    if float(fit_m.get("net_pnl") or 0) <= 0:
        score -= 0.75
    if float(val_m.get("net_pnl") or 0) <= 0:
        score -= 1.25
    return float(score)


def run_search(args: argparse.Namespace, fit: pd.DataFrame, val: pd.DataFrame, train: pd.DataFrame, test: pd.DataFrame) -> dict[str, Any]:
    mask_quant = quantiles_for(fit)
    pm_quant = premom_quantiles(fit, args.pm_quantile_sample, args.seed)
    engine = "Optuna TPE" if HAVE_OPTUNA else "Optuna unavailable; using seeded random search fallback."
    print(f"[search] optimizer={engine}")
    print(f"[search] mask features={sorted(mask_quant)}")
    print(f"[search] premom features={sorted(pm_quant)}")

    rows: list[dict[str, Any]] = []
    tested: list[dict[str, Any]] = []
    passing: list[dict[str, Any]] = []
    best = {"score": -1e9, "cfg": None}
    min_fv = int(args.min_fv_trades)
    t0 = time.time()

    def evaluate_trial(cfg: dict[str, Any], idx: int) -> float:
        fit_m = eval_cfg(cfg, fit, detail=False)
        val_m = eval_cfg(cfg, val, detail=False)
        score = trial_score(fit_m, val_m, min_fv, args.gap_lambda)
        train_m: dict[str, Any] | None = None
        test_m: dict[str, Any] | None = None
        keep = "REJECT"
        failure = ""
        tested_test = False
        if (
            int(fit_m.get("n") or 0) >= min_fv
            and int(val_m.get("n") or 0) >= max(4, min_fv // 2)
            and float(fit_m.get("net_pf") or 0) >= 1.05
            and float(val_m.get("net_pf") or 0) >= 0.90
            and float(fit_m.get("net_pnl") or 0) > 0
        ):
            train_m = eval_cfg(cfg, train, detail=True)
            if PF_LO <= float(train_m.get("net_pf") or 0) <= PF_HI and float(train_m.get("net_pnl") or 0) > 0 and int(train_m.get("n") or 0) >= 20:
                test_m = eval_cfg(cfg, test, detail=True)
                tested_test = True
                passed, reasons = pass_candidate(train_m, test_m)
                if passed:
                    if cfg.get("premom_terms") and abs(float(cfg["sl"]) - PM_CACHE_SL) > 1e-9:
                        passed = False
                        reasons.append(f"pre-momentum cache uses SL {PM_CACHE_SL}; candidate SL {cfg['sl']} is not approval-safe")
                if passed:
                    keep = "PASS_APPROVAL_REQUIRED"
                    passing.append({
                        "candidate_id": f"{SETUP}_candidate_{len(passing) + 1:03d}",
                        "cfg": clean_float(cfg_to_block(cfg)),
                        "train": strip_detail(train_m),
                        "test": strip_detail(test_m),
                    })
                else:
                    keep = "REJECT_TEST_OR_STABILITY"
                    failure = "; ".join(reasons)
            else:
                keep = "REJECT_FULL_TRAIN"
                failure = f"full TRAIN PF/n/net = {train_m.get('net_pf')}/{train_m.get('n')}/{train_m.get('net_pnl')}"
        else:
            failure = "FIT/VAL gate failed"

        row = {
            "iteration": idx,
            "parameter_group": classify_cfg(cfg),
            "changed_rule_parameter": summarize_cfg(cfg),
            "old_value": "baseline current config",
            "new_value": summarize_cfg(cfg),
            "reason": "FIT/VAL band search over repo-supported knobs",
            "command": rerun_command(args),
            "sl": cfg["sl"],
            "tgt": cfg["tgt"],
            "mask_terms": terms_text(cfg.get("mask_terms")),
            "premom_terms": terms_text(cfg.get("premom_terms")),
            "guard": json.dumps(cfg.get("guard") or {}),
            "max_positions": cfg.get("max_positions"),
            "daily_loss_rs": cfg.get("daily_loss_rs"),
            "fit_n": fit_m.get("n"),
            "fit_pf": fit_m.get("net_pf"),
            "fit_net": fit_m.get("net_pnl"),
            "val_n": val_m.get("n"),
            "val_pf": val_m.get("net_pf"),
            "val_net": val_m.get("net_pnl"),
            "train_n": train_m.get("n") if train_m else None,
            "train_pf": train_m.get("net_pf") if train_m else None,
            "train_net": train_m.get("net_pnl") if train_m else None,
            "test_n": test_m.get("n") if test_m else None,
            "test_pf": test_m.get("net_pf") if test_m else None,
            "test_net": test_m.get("net_pnl") if test_m else None,
            "sl_count": train_m.get("sl_cnt") if train_m else 0,
            "target_count": train_m.get("tgt_cnt") if train_m else 0,
            "time_exit_count": train_m.get("eod_cnt") if train_m else 0,
            "score": round(score, 6),
            "keep_reject": keep,
            "failure_classification": failure,
            "next_action": "combine if stable" if keep == "PASS_APPROVAL_REQUIRED" else "continue train-side search",
            "tested_test": tested_test,
        }
        rows.append(row)
        tested.append({"cfg": clean_float(cfg), "row": clean_float(row)})
        if score > best["score"]:
            best["score"] = score
            best["cfg"] = cfg
        if idx % 25 == 0:
            print(f"[search] {idx} trials done best_score={best['score']:.4f} pass={len(passing)} elapsed={time.time() - t0:.0f}s", flush=True)
        return score

    if HAVE_OPTUNA:
        def objective(trial: Any) -> float:
            idx = len(rows) + 1
            cfg = suggest_cfg(trial, mask_quant, pm_quant, args.max_mask_terms, args.max_pm_terms)
            return evaluate_trial(cfg, idx)

        study = optuna.create_study(direction="maximize", sampler=optuna.samplers.TPESampler(seed=args.seed))
        study.optimize(objective, n_trials=args.trials, timeout=args.time_budget_min * 60.0)
    else:
        rng = random.Random(args.seed)
        for idx in range(1, args.trials + 1):
            if time.time() - t0 > args.time_budget_min * 60.0:
                break
            cfg = suggest_cfg(RandTrial(rng), mask_quant, pm_quant, args.max_mask_terms, args.max_pm_terms)
            evaluate_trial(cfg, idx)

    trials = pd.DataFrame(rows)
    trials.to_csv(WORK / "trials.csv", index=False)
    trials.to_csv(WORK / "ITERATION_LOG_TABLE.csv", index=False)
    (WORK / "tested_configs.json").write_text(as_json(clean_float(tested)), encoding="utf-8")
    return {
        "engine": engine,
        "mask_quant": clean_float(mask_quant),
        "pm_quant": clean_float(pm_quant),
        "n_trials": len(rows),
        "trials": rows,
        "passing": passing,
        "best_fitval": {"score": clean_float(best["score"]), "cfg": clean_float(best["cfg"] or {})},
    }


def classify_cfg(cfg: dict[str, Any]) -> str:
    groups = []
    if cfg["sl"] != 1.20 or cfg["tgt"] != 1.50:
        groups.append("exit")
    if cfg.get("mask_terms"):
        feats = {t[0].lower() for t in cfg["mask_terms"]}
        if feats & {"rsi", "adx", "macd_hist", "macd_hist_delta", "vwap_dist_atr", "atr_pct", "vol_ratio"}:
            groups.append("indicator/filter")
        if feats & {"body_pct", "close_loc", "signal_range_pct", "upper_wick_pct", "lower_wick_pct", "wick_skew_pct"}:
            groups.append("price_action")
        if feats & {"signal_minute", "market_ret_pct", "market_abs_ret_pct", "quality_score", "ranker_score"}:
            groups.append("filter")
    if cfg.get("premom_terms"):
        groups.append("pre_momentum")
    if cfg.get("guard"):
        groups.append("guard")
    return "+".join(dict.fromkeys(groups)) or "raw"


def summarize_cfg(cfg: dict[str, Any]) -> str:
    return (
        f"SL/Tgt={cfg['sl']}/{cfg['tgt']} mask=[{terms_text(cfg.get('mask_terms'))}] "
        f"premom=[{terms_text(cfg.get('premom_terms'))}] guard={cfg.get('guard') or {}} "
        f"maxpos={cfg.get('max_positions')} dloss={cfg.get('daily_loss_rs')}"
    )


def rerun_command(args: argparse.Namespace) -> str:
    return (
        f"python Train_and_Test\\setup_pf_1_4_full_loop\\{SETUP}\\scripts\\a_pullback_c2_low_full_loop.py "
        f"--trials {args.trials} --time_budget_min {args.time_budget_min} --seed {args.seed} "
        f"--max_mask_terms {args.max_mask_terms} --max_pm_terms {args.max_pm_terms}"
    )


def write_pool_report(pool_report: dict[str, Any], split: dict[str, Any]) -> None:
    lines = [
        f"# {SETUP} - POOL_RECREATION_REPORT",
        "",
        f"Generated {date.today().isoformat()}. Research-only; no live execution.",
        "",
        "## Raw Data Sources Used",
        "",
    ]
    for s in pool_report["source_reports"]:
        lines.append(f"- `{s.get('path')}`: exists={s.get('exists')} setup_rows={s.get('setup_rows')} first={s.get('first')} last={s.get('last')} sessions={s.get('sessions')}")
    lines += [
        "",
        "## Recreated Pool",
        "",
        f"- Path: `{pool_report['pool_path']}`",
        f"- Rows pre-dedupe: {pool_report['rows_pre_dedupe']}",
        f"- Rows after dedupe: {pool_report['rows']}",
        f"- Columns: {len(pool_report['columns'])}",
        f"- Available first/last setup-candidate session: {pool_report['available_first_session']} / {pool_report['available_last_session']}",
        "",
        "## Requested vs Actual",
        "",
        f"- Requested TRAIN: {pool_report['requested_train'][0]} to {pool_report['requested_train'][1]}",
        f"- Actual TRAIN setup-candidate sessions: {len(pool_report['actual_train_sessions'])} ({rng_text(pool_report['actual_train_sessions'])})",
        f"- Requested TEST: {pool_report['requested_test'][0]} to {pool_report['requested_test'][1]}",
        f"- Actual TEST setup-candidate sessions: {len(pool_report['actual_test_sessions'])} ({rng_text(pool_report['actual_test_sessions'])})",
        f"- Missing TRAIN business dates from setup-candidate pool: {', '.join(pool_report['missing_train_business_dates']) or '(none)'}",
        f"- Missing TEST business dates from setup-candidate pool: {', '.join(pool_report['missing_test_business_dates']) or '(none)'}",
        "",
        "## 5-Minute and 1-Minute Coverage",
        "",
        "- 5-minute signal pool was recreated from v11 unified/raw/backtest candidate files listed above.",
        "- 1-minute exit realism is provided by `setup_train_test.py` via `avwap_5min_ID_v11_backtesting._load_1m_with_open` and `v5_exit_resolver`.",
        f"- Entry rows after 1-minute entry attach: TRAIN {split['rows']['train_entries']} / TEST {split['rows']['test_entries']}.",
        "",
        "## Data Quality Issues",
        "",
        "- No `A_PULLBACK_C2_THEN_BREAK_C2_LOW` rows were found in the July 1 backtesting raw/pre-dedupe files inspected.",
        "- Therefore the nearest available TEST setup-candidate session in the recreated pool is reported above.",
    ]
    (WORK / "POOL_RECREATION_REPORT.md").write_text("\n".join(lines), encoding="utf-8")


def rng_text(sessions: list[str]) -> str:
    if not sessions:
        return "none"
    return f"{sessions[0]}..{sessions[-1]}"


def write_baseline_report(block: dict[str, Any], baseline: dict[str, Any], split: dict[str, Any]) -> None:
    cfg = baseline["cfg"]
    lines = [
        f"# {SETUP} ({SIDE}) - BASELINE_RESULT",
        "",
        f"Generated {date.today().isoformat()}.",
        "",
        "## Current Rules",
        "",
        "- Logic: after a 2-bar up-pullback in a non-bull regime, price loses VWAP and breaks the prior bar low on volume.",
        "- Entry trigger: 5-minute signal, next 1-minute open entry.",
        "- Detection: close<open, close_loc<=0.40, close<VWAP, close<prev_bar_low, prev_close>prev2_close, vol_ratio>=1.4, regime!=BULL.",
        "- Current filters: " + terms_text(cfg["mask_terms"]),
        "- Current pre-momentum: " + terms_text(cfg["premom_terms"]),
        f"- Current guards: {cfg.get('guard') or {}}",
        f"- Current SL/target: {cfg['sl']} / {cfg['tgt']}",
        "- Exit logic: 1-minute SL / target / EOD resolve to 15:20 IST, net of statutory NSE intraday costs plus slippage.",
        "- Config source: root `final_setup_conf.py`, active `FINAL_SETUP_CONF` block.",
        "",
        "## Sessions",
        "",
        f"- FIT: {rng_text(split['fit_sessions'])} ({len(split['fit_sessions'])} setup-candidate sessions)",
        f"- VAL: {rng_text(split['val_sessions'])} ({len(split['val_sessions'])} setup-candidate sessions)",
        f"- TRAIN: {rng_text(split['train_sessions'])} ({len(split['train_sessions'])} setup-candidate sessions)",
        f"- TEST: {rng_text(split['test_sessions'])} ({len(split['test_sessions'])} setup-candidate sessions)",
        "",
    ]
    for label in ["FIT", "VAL", "TRAIN", "TEST"]:
        lines += [f"## Baseline {label} Metrics", "", "| metric | value |", "|---|---|", mtable(baseline["metrics"][label]), ""]
    lines += [
        "## Initial Diagnosis",
        "",
        f"- Baseline TRAIN PF {baseline['metrics']['TRAIN']['net_pf']} with {baseline['metrics']['TRAIN']['n']} trades.",
        f"- Baseline TEST PF {baseline['metrics']['TEST']['net_pf']} with {baseline['metrics']['TEST']['n']} trades.",
        "- Optimization continues only from FIT/VAL evidence; TEST is used for final validation of train-side candidates.",
    ]
    (WORK / "BASELINE_RESULT.md").write_text("\n".join(lines), encoding="utf-8")


def categorize_columns(cols: list[str]) -> dict[str, list[str]]:
    buckets = {
        "price_ohlc": ["open", "high", "low", "close", "fill", "price"],
        "volume": ["vol", "volume", "notional", "traded"],
        "vwap_avwap": ["vwap", "avwap"],
        "ema_sma": ["ema", "sma"],
        "rsi_adx_macd": ["rsi", "adx", "macd"],
        "atr_volatility": ["atr", "range", "volatility"],
        "candle_structure": ["body", "wick", "close_loc", "signal_range"],
        "pre_momentum": ["pre", "sig5"],
        "setup_reason": ["setup", "reason", "quality", "ranker"],
        "time_session": ["time", "date", "day", "slot", "minute"],
        "symbol": ["ticker", "symbol"],
    }
    out: dict[str, list[str]] = {}
    low = {c: c.lower() for c in cols}
    used: set[str] = set()
    for bucket, keys in buckets.items():
        vals = [c for c, lc in low.items() if any(k in lc for k in keys)]
        out[bucket] = sorted(vals)
        used.update(vals)
    out["other"] = sorted([c for c in cols if c not in used])
    return out


def write_inventory(pool_report: dict[str, Any], block: dict[str, Any], search: dict[str, Any] | None = None) -> None:
    cats = categorize_columns(pool_report["columns"])
    cfg = conf_to_cfg(block)
    lines = [
        f"# {SETUP} - PARAMETER_INVENTORY",
        "",
        "## 1. Current Setup Rules",
        "",
        f"- Setup name: {SETUP}",
        f"- Side: {SIDE}",
        "- Current entry trigger: next 1-minute open after 5-minute signal.",
        "- Indicator rules: VWAP loss, vol_ratio>=1.4, regime!=BULL, sig5_adx_calc gate.",
        "- Non-indicator rules: red candle, close_loc<=0.40, close below previous bar low, prior two-bar pullback.",
        "- Current pre-momentum rules: " + terms_text(cfg["premom_terms"]),
        "- Current filters: " + terms_text(cfg["mask_terms"]),
        f"- Current guards: {cfg.get('guard') or {}}",
        f"- Current SL/target: {cfg['sl']} / {cfg['tgt']}",
        "- Current exit logic: SL/target/EOD via 1-minute resolver.",
        "- Current time windows: default live 09:30-14:30 unless guard overrides.",
        "- Current portfolio limits: max_positions default 20; daily_loss_rs default 0 unless candidate overrides.",
        "- Current config source: root `final_setup_conf.py`.",
        "",
        "## 2. Available Columns/Features In Recreated Pool",
        "",
    ]
    for bucket, vals in cats.items():
        lines += [f"### {bucket}", "", ", ".join(f"`{v}`" for v in vals) or "(none)", ""]
    lines += [
        "## 3. Supported Optimization Knobs",
        "",
        "- mask_terms",
        "- pre_momentum_terms",
        "- min_slot / max_slot / top_n entry guards",
        "- max_positions",
        "- daily_loss_rs",
        "- fixed SL and fixed target",
        "- EOD exit through repo resolver",
        "- portfolio overlay via `setup_train_test.eval_family`",
        "",
        "## 4. Candidate Parameter Ranges",
        "",
        f"- SL grid: {SL_GRID}",
        f"- Target grid: {TGT_GRID}",
        f"- Min slot grid: {MIN_SLOTS}",
        f"- Max slot grid: {MAX_SLOTS}",
        f"- Top-N grid: [0, 1, 2, 3]",
        f"- max_positions grid: {MAXPOS_GRID}",
        f"- daily_loss_rs grid: {DLOSS_GRID}",
        "- Mask thresholds: FIT-only quantiles q=0.1..0.9 for available indicator, candle, volume, VWAP, quality, market, and time columns.",
        "- Pre-momentum thresholds: FIT-only sampled 1-minute pre-entry quantiles q=0.1..0.9.",
        "- Ranges are realistic because they use observed FIT distributions and repo-supported fields only; TEST columns are never used for threshold construction.",
    ]
    if search:
        lines += [
            "",
            "## Searchable FIT Quantile Features",
            "",
            f"- Mask: {', '.join(sorted(search['mask_quant']))}",
            f"- Pre-momentum: {', '.join(sorted(search['pm_quant']))}",
        ]
    (WORK / "PARAMETER_INVENTORY.md").write_text("\n".join(lines), encoding="utf-8")


def write_sweep_summary(search: dict[str, Any]) -> None:
    rows = pd.DataFrame(search["trials"])
    lines = [
        f"# {SETUP} - PARAMETER_SWEEP_SUMMARY",
        "",
        f"- Optimizer: {search['engine']}",
        f"- Iterations: {search['n_trials']}",
        f"- Search rule: FIT/VAL only; full TRAIN confirmation only for reasonable FIT/VAL; TEST only for full TRAIN PF {PF_LO}-{PF_HI}.",
        "",
        "## Value Families Tested",
        "",
        f"- Indicator/filter values: FIT quantiles for {', '.join(sorted(search['mask_quant']))}.",
        f"- Pre-momentum values: FIT quantiles for {', '.join(sorted(search['pm_quant']))}.",
        f"- Exit values: SL {SL_GRID}; target {TGT_GRID}.",
        f"- Guard values: min slots {MIN_SLOTS}; max slots {MAX_SLOTS}; top_n 1/2/3.",
        f"- Portfolio values: max_positions {MAXPOS_GRID}; daily_loss_rs {DLOSS_GRID}.",
        "",
    ]
    if not rows.empty:
        top = rows.sort_values("score", ascending=False).head(20)
        lines += ["## Top 20 FIT/VAL Trials", "", "| iter | group | config | FIT n/PF | VAL n/PF | TRAIN n/PF | TEST n/PF | keep/reject |", "|---|---|---|---|---|---|---|---|"]
        for _, r in top.iterrows():
            lines.append(
                f"| {r['iteration']} | {r['parameter_group']} | {r['new_value']} | "
                f"{r['fit_n']}/{r['fit_pf']} | {r['val_n']}/{r['val_pf']} | "
                f"{r['train_n']}/{r['train_pf']} | {r['test_n']}/{r['test_pf']} | {r['keep_reject']} |"
            )
        lines += ["", "## Stable/Rejected Ranges", ""]
        for group, g in rows.groupby("parameter_group"):
            g2 = g.sort_values("score", ascending=False).head(3)
            lines.append(f"- {group}: best FIT/VAL examples: " + " | ".join(f"iter {int(x.iteration)} score {x.score} {x.keep_reject}" for x in g2.itertuples()))
        overfit = rows[(rows["train_pf"].fillna(0) > PF_HI) | ((rows["fit_pf"].fillna(0) > 2.5) & (rows["val_pf"].fillna(0) < 1.0))]
        lines += ["", f"- Overfit-risk rows flagged: {len(overfit)} (TRAIN PF>{PF_HI} or FIT high/VAL weak)."]
    (WORK / "PARAMETER_SWEEP_SUMMARY.md").write_text("\n".join(lines), encoding="utf-8")


def write_iteration_log(search: dict[str, Any]) -> None:
    rows = search["trials"]
    lines = [
        f"# {SETUP} - ITERATION_LOG",
        "",
        f"Optimizer: {search['engine']}. Each row below is one logical config iteration.",
        "",
        "| iteration | changed rule/parameter | group | old value | new value | FIT | VAL | TRAIN | TEST | keep/reject | failure |",
        "|---|---|---|---|---|---|---|---|---|---|---|",
    ]
    for r in rows:
        lines.append(
            f"| {r['iteration']} | {r['changed_rule_parameter']} | {r['parameter_group']} | {r['old_value']} | "
            f"{r['new_value']} | {r['fit_n']}/{r['fit_pf']}/Rs{r['fit_net']} | "
            f"{r['val_n']}/{r['val_pf']}/Rs{r['val_net']} | {r['train_n']}/{r['train_pf']}/Rs{r['train_net']} | "
            f"{r['test_n']}/{r['test_pf']}/Rs{r['test_net']} | {r['keep_reject']} | {r['failure_classification']} |"
        )
    (WORK / "ITERATION_LOG.md").write_text("\n".join(lines), encoding="utf-8")


def write_failure_analysis(baseline: dict[str, Any], search: dict[str, Any], chosen: dict[str, Any] | None) -> None:
    train_m = chosen["train"] if chosen else strip_detail(baseline["metrics"]["TRAIN"])
    test_m = chosen["test"] if chosen else strip_detail(baseline["metrics"]["TEST"])
    rows = pd.DataFrame(search["trials"])
    reject_counts = rows["keep_reject"].value_counts().to_dict() if not rows.empty else {}
    lines = [
        f"# {SETUP} - FAILURE_ANALYSIS",
        "",
        "## Losing Trade And Weakness Classification",
        "",
        f"- Iteration outcomes: {reject_counts}",
        "- Common failure classes are FIT/VAL gate failed, full TRAIN PF outside the controlled band, TEST PF below 1.40, thin TEST sample, or domination failure.",
        "",
        "## Baseline Exit Behavior",
        "",
        f"- TRAIN: {metric_line(baseline['metrics']['TRAIN'])}",
        f"- TEST: {metric_line(baseline['metrics']['TEST'])}",
        "",
        "## Selected/Best Observed Behavior",
        "",
        f"- TRAIN: {metric_line(train_m)}",
        f"- TEST: {metric_line(test_m)}",
        "",
        "## Notes",
        "",
        "- Fake-breakdown and weak-momentum risk was proxied with close_loc/body/wick/VWAP/volume/ADX/pre-momentum sweeps.",
        "- Bad time-window risk was proxied with min_slot/max_slot/top_n sweeps.",
        "- SL/target behavior was swept over tight, balanced, runner, and wide-stop combinations.",
        "- TEST validation was deliberately restricted to candidates that first landed in the full TRAIN PF band.",
    ]
    if not rows.empty:
        worst = rows.sort_values("score").head(10)
        lines += ["", "## Worst FIT/VAL Rows", "", "| iter | cfg | FIT PF | VAL PF | failure |", "|---|---|---|---|---|"]
        for _, r in worst.iterrows():
            lines.append(f"| {r['iteration']} | {r['new_value']} | {r['fit_pf']} | {r['val_pf']} | {r['failure_classification']} |")
    (WORK / "FAILURE_ANALYSIS.md").write_text("\n".join(lines), encoding="utf-8")


def write_candidate_files(search: dict[str, Any], baseline: dict[str, Any]) -> dict[str, Any] | None:
    cand_dir = WORK / "candidates"
    cand_dir.mkdir(exist_ok=True)
    passing = search["passing"]
    lines = [
        f"# {SETUP} - CANDIDATE_CONFIGS",
        "",
    ]
    chosen = None
    if not passing:
        lines += [
            "No candidate passed all acceptance checks:",
            f"- TRAIN PF must be {PF_LO}-{PF_HI}.",
            f"- TEST PF must be > {TEST_PF_MIN}.",
            "- TRAIN and TEST net PnL must be positive.",
            "- Domination checks must pass.",
        ]
        (cand_dir / "NO_CANDIDATES.md").write_text("\n".join(lines), encoding="utf-8")
    else:
        passing = sorted(passing, key=lambda c: (c["test"]["net_pf"], c["test"]["net_pnl"], c["train"]["n"]), reverse=True)
        chosen = passing[0]
        for i, c in enumerate(passing, 1):
            cid = f"{SETUP}_candidate_{i:03d}"
            c["candidate_id"] = cid
            path = cand_dir / f"{cid}.json"
            path.write_text(as_json(clean_float(c)), encoding="utf-8")
            lines += [
                f"## {cid}",
                "",
                "```json",
                as_json(clean_float(c["cfg"])),
                "```",
                "",
                f"- TRAIN: {metric_line(c['train'])}",
                f"- TEST: {metric_line(c['test'])}",
                f"- Domination: train top trade/day/symbol {c['train'].get('trade_dom_gross')}/{c['train'].get('day_dom')}/{c['train'].get('sym_dom')}; test {c['test'].get('trade_dom_gross')}/{c['test'].get('day_dom')}/{c['test'].get('sym_dom')}.",
                "",
            ]
    (WORK / "CANDIDATE_CONFIGS.md").write_text("\n".join(lines), encoding="utf-8")
    return chosen


def write_final_recommendation(chosen: dict[str, Any] | None, baseline: dict[str, Any], split: dict[str, Any], args: argparse.Namespace) -> None:
    rec = "YES" if chosen else "NO"
    block = chosen["cfg"] if chosen else cfg_to_block(baseline["cfg"])
    final_path = REPO / "final_setup_conf.py"
    candidate_heading = "Best Candidate" if chosen else "Baseline Reference (No Passing Candidate)"
    lines = [
        f"# {SETUP} - APPROVAL_REQUIRED_FINAL_RECOMMENDATION",
        "",
        f"Approval recommendation: {rec}",
        "",
        f"## {candidate_heading}",
        "",
        "```json",
        as_json(clean_float(block)),
        "```",
        "",
        "## Metrics",
        "",
    ]
    if chosen:
        lines += [
            f"- TRAIN: {metric_line(chosen['train'])}",
            f"- TEST: {metric_line(chosen['test'])}",
            f"- Candidate config path: `{WORK / 'candidates' / (chosen['candidate_id'] + '.json')}`",
        ]
    else:
        lines += [
            "- No passing candidate. Baseline retained for reference only.",
            f"- Baseline TRAIN: {metric_line(baseline['metrics']['TRAIN'])}",
            f"- Baseline TEST: {metric_line(baseline['metrics']['TEST'])}",
        ]
    lines += [
        "",
        "## Final File That Would Need Approval Before Edit",
        "",
        f"- `{final_path}`",
        "",
        "## Proposed Patch",
        "",
        "- Do not apply automatically. If approved, replace only the setup block with the JSON-equivalent block above.",
        "",
        "DO NOT MOVE TO FINAL CONFIG UNTIL USER APPROVES",
        "",
        "## Rerun Commands",
        "",
        "```powershell",
        rerun_command(args),
        "```",
        "",
        "## Risk Notes",
        "",
        f"- TRAIN sessions: {rng_text(split['train_sessions'])} ({len(split['train_sessions'])}).",
        f"- TEST sessions: {rng_text(split['test_sessions'])} ({len(split['test_sessions'])}).",
        "- July 1 inspected backtesting files contained zero setup rows; nearest available setup-candidate TEST session is in the reports.",
        "- No live trades, order placement, or final config edits were performed.",
    ]
    (WORK / "APPROVAL_REQUIRED_FINAL_RECOMMENDATION.md").write_text("\n".join(lines), encoding="utf-8")


def run_baseline(block: dict[str, Any], fit: pd.DataFrame, val: pd.DataFrame, train: pd.DataFrame, test: pd.DataFrame) -> dict[str, Any]:
    cfg = conf_to_cfg(block)
    return {
        "cfg": cfg,
        "metrics": {
            "FIT": eval_cfg(cfg, fit, detail=True),
            "VAL": eval_cfg(cfg, val, detail=True),
            "TRAIN": eval_cfg(cfg, train, detail=True),
            "TEST": eval_cfg(cfg, test, detail=True),
        },
    }


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser()
    ap.add_argument("--trials", type=int, default=160)
    ap.add_argument("--time_budget_min", type=float, default=45.0)
    ap.add_argument("--seed", type=int, default=17)
    ap.add_argument("--max_mask_terms", type=int, default=2)
    ap.add_argument("--max_pm_terms", type=int, default=2)
    ap.add_argument("--min_fv_trades", type=int, default=10)
    ap.add_argument("--gap_lambda", type=float, default=0.70)
    ap.add_argument("--pm_quantile_sample", type=int, default=1500)
    ap.add_argument("--skip_search", action="store_true")
    return ap.parse_args()


def main() -> int:
    args = parse_args()
    WORK.mkdir(parents=True, exist_ok=True)
    (WORK / "scripts").mkdir(exist_ok=True)
    (WORK / "candidates").mkdir(exist_ok=True)
    print(f"[full-loop] setup={SETUP}")
    print(f"[full-loop] work={WORK}")

    pool_report = recreate_pool()
    pool_dir = WORK / "pools"
    fit, val, train, test, split = split_frames(pool_dir)
    print(f"[full-loop] FIT rows={len(fit)} VAL rows={len(val)} TRAIN rows={len(train)} TEST rows={len(test)}")

    block = fsc.FINAL_SETUP_CONF.get(SETUP) or fsc.RESEARCH_WATCH_CONF.get(SETUP)
    if not block:
        raise SystemExit(f"{SETUP} missing from final_setup_conf.py")
    baseline = run_baseline(block, fit, val, train, test)
    print(f"[baseline] TRAIN {metric_line(baseline['metrics']['TRAIN'])}")
    print(f"[baseline] TEST  {metric_line(baseline['metrics']['TEST'])}")

    write_pool_report(pool_report, split)
    write_baseline_report(block, baseline, split)

    if args.skip_search:
        search = {"engine": "skipped", "mask_quant": {}, "pm_quant": {}, "n_trials": 0, "trials": [], "passing": [], "best_fitval": {}}
    else:
        search = run_search(args, fit, val, train, test)

    write_inventory(pool_report, block, search)
    write_sweep_summary(search)
    write_iteration_log(search)
    chosen = write_candidate_files(search, baseline)
    write_failure_analysis(baseline, search, chosen)
    write_final_recommendation(chosen, baseline, split, args)

    summary = {
        "setup": SETUP,
        "pool_recreation_succeeded": True,
        "pool_path": str(WORK / "pools" / FNAME),
        "requested_train": [str(REQUESTED_TRAIN[0].date()), str(REQUESTED_TRAIN[1].date())],
        "actual_train_sessions": split["train_sessions"],
        "requested_test": [str(REQUESTED_TEST[0].date()), str(REQUESTED_TEST[1].date())],
        "actual_test_sessions": split["test_sessions"],
        "baseline": {k: strip_detail(v) for k, v in baseline["metrics"].items()},
        "search": {k: v for k, v in search.items() if k != "trials"},
        "best_candidate": chosen,
        "approval_recommendation": "YES" if chosen else "NO",
        "rerun_command": rerun_command(args),
    }
    (WORK / "run_summary.json").write_text(as_json(clean_float(summary)), encoding="utf-8")
    print(f"[full-loop] wrote artifacts under {WORK}")
    print(f"[full-loop] approval_recommendation={summary['approval_recommendation']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
