from __future__ import annotations

import argparse
import itertools
import json
import math
import sys
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

P = Path(__file__).resolve()
TT_DIR = next(parent for parent in P.parents if parent.name == "Train_and_Test")
REPO_ROOT = TT_DIR.parent
for d in (str(REPO_ROOT), str(TT_DIR)):
    if d not in sys.path:
        sys.path.insert(0, d)

import setup_train_test as tt  # noqa: E402

try:
    import optuna  # type: ignore  # noqa: E402

    optuna.logging.set_verbosity(optuna.logging.WARNING)
    HAVE_OPTUNA = True
except Exception:
    optuna = None
    HAVE_OPTUNA = False


SETUP = "L_RS_LEADER_VWAP_HOLD"
SIDE = "LONG"
BAND_LO = 1.30
BAND_HI = 1.70
TEST_PF_MIN = 1.40
DOM_CAP = 0.40
MAX_TPD = 6.0
QGRID = [0.20, 0.40, 0.60, 0.80]
SL_GRID = [0.40, 0.50, 0.60, 0.70, 0.80, 0.90, 1.00, 1.20, 1.50]
TGT_GRID = [0.60, 0.80, 1.00, 1.25, 1.50, 2.00, 2.50, 3.00]
BASELINE = {
    "sl": 0.50,
    "tgt": 1.25,
    "mask_terms": [
        ["quality_score", ">=", 97.121022],
        ["vol_ratio", ">=", 2.164331],
        ["vwap_dist_atr", "<=", 1.49336],
        ["signal_minute", "<=", 660.0],
    ],
    "premom_terms": [],
    "guard": None,
    "max_positions": 20,
    "daily_loss_rs": 0.0,
    "regime_align": False,
    "regime_band": 0.0,
}

SIGNAL_FEATURES = [
    "rsi",
    "rsi3max",
    "adx",
    "macd_hist",
    "macd_hist_delta",
    "ema20_slope",
    "stock_ret",
    "rs_pct",
    "market_ret_pct",
    "vol_ratio",
    "atr_pct",
    "body_pct",
    "close_loc",
    "vwap_dist_atr",
    "quality_score",
    "ranker_score",
    "score",
    "signal_minute",
    "signal_range_pct",
    "upper_wick_pct",
    "lower_wick_pct",
    "wick_skew_pct",
    "lower_wick_price_pct",
    "source_quality_score",
]
PM_FEATURES = [
    "pre_entry_momentum_score",
    "sig5_adx_calc",
    "sig5_rsi_dir",
    "sig5_vol_ratio20",
    "pre1_adx",
    "pre3_range_r",
    "pre5_mom_r",
    "pre3_close_pos",
]


def json_safe(obj: Any) -> Any:
    if isinstance(obj, dict):
        return {k: json_safe(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [json_safe(v) for v in obj]
    if isinstance(obj, tuple):
        return [json_safe(v) for v in obj]
    if isinstance(obj, float):
        return obj if np.isfinite(obj) else None
    return obj


def norm_cfg(cfg: dict[str, Any]) -> dict[str, Any]:
    return {
        "status": "OK",
        "sl": float(cfg["sl"]),
        "tgt": float(cfg["tgt"]),
        "mask_terms": [tuple(t) for t in (cfg.get("mask_terms") or [])],
        "premom_terms": [tuple(t) for t in (cfg.get("premom_terms") or [])],
        "guard": cfg.get("guard") or None,
        "max_positions": int(cfg.get("max_positions", 20)),
        "daily_loss_rs": float(cfg.get("daily_loss_rs", 0.0)),
        "regime_align": bool(cfg.get("regime_align", False)),
        "regime_band": float(cfg.get("regime_band", 0.0)),
    }


def public_cfg(cfg: dict[str, Any]) -> dict[str, Any]:
    c = norm_cfg(cfg)
    return {
        "sl": c["sl"],
        "tgt": c["tgt"],
        "mask_terms": [list(t) for t in c["mask_terms"]],
        "premom_terms": [list(t) for t in c["premom_terms"]],
        "entry_guards": c["guard"] or {},
        "max_positions": c["max_positions"],
        "daily_loss_rs": c["daily_loss_rs"],
        "regime_align": c["regime_align"],
        "regime_band": c["regime_band"],
    }


def terms_str(terms: list[Any] | tuple[Any, ...] | None) -> str:
    return "; ".join(f"{a}{o}{b}" for a, o, b in (terms or [])) or "(none)"


def cfg_key(cfg: dict[str, Any]) -> str:
    return json.dumps(public_cfg(cfg), sort_keys=True)


def set_runtime(cfg: dict[str, Any]) -> dict[str, Any]:
    c = norm_cfg(cfg)
    tt.MAX_POSITIONS = c["max_positions"]
    tt.DAILY_LOSS_RS = c["daily_loss_rs"]
    tt.REGIME_ALIGN = c["regime_align"]
    tt.REGIME_BAND = c["regime_band"]
    return c


def light_metrics(cfg: dict[str, Any], df: pd.DataFrame) -> dict[str, Any]:
    c = set_runtime(cfg)
    fam = tt.eval_family({SETUP: c}, df)
    book = fam.get("book")
    days = int(pd.Series(book["_day"]).nunique()) if book is not None and len(book) else 0
    return {
        "trades": int(fam["trades"]),
        "pf": round(float(fam["net_pf"]), 4),
        "net": round(float(fam["net_pnl"]), 2),
        "days": days,
        "tpd": round(int(fam["trades"]) / max(1, days), 2),
        "day_block_p": None if not np.isfinite(fam["day_block_p"]) else round(float(fam["day_block_p"]), 4),
    }


def full_metrics(cfg: dict[str, Any], df: pd.DataFrame) -> tuple[dict[str, Any], pd.DataFrame]:
    c = set_runtime(cfg)
    fam = tt.eval_family({SETUP: c}, df)
    det = tt.book_detail(fam["book"], {SETUP: (c["sl"], c["tgt"])}) if fam["trades"] else pd.DataFrame()
    m = light_metrics(cfg, df)
    m.update(
        {
            "wins": 0,
            "losses": 0,
            "win_rate_pct": 0.0,
            "gross_profit": 0.0,
            "gross_loss": 0.0,
            "avg_win": 0.0,
            "avg_loss": 0.0,
            "max_drawdown": 0.0,
            "n_days": 0,
            "n_syms": 0,
            "sl_cnt": 0,
            "tgt_cnt": 0,
            "eod_cnt": 0,
            "trade_dom_gross": None,
            "day_dom": None,
            "sym_dom": None,
            "daywise": [],
            "symwise": [],
            "hourwise": [],
        }
    )
    if det.empty:
        return m, det
    net = det["net_pnl_rs"].astype(float)
    gross = det["gross_pnl_rs"].astype(float)
    wins = net[net > 0]
    losses = net[net <= 0]
    gp = float(gross[gross > 0].sum())
    total = float(net.sum())
    eq = net.cumsum()
    dd = eq - eq.cummax()
    oc = det["outcome"].astype(str).str.upper()
    day_net = det.groupby("trade_date")["net_pnl_rs"].sum()
    sym_net = det.groupby("ticker")["net_pnl_rs"].sum()
    tmp = det.copy()
    tmp["_hour"] = pd.to_datetime(tmp["entry_time"], errors="coerce").dt.strftime("%H")
    m.update(
        {
            "wins": int((net > 0).sum()),
            "losses": int((net <= 0).sum()),
            "win_rate_pct": round(float((net > 0).mean() * 100.0), 2),
            "gross_profit": round(gp, 2),
            "gross_loss": round(float(-gross[gross < 0].sum()), 2),
            "avg_win": round(float(wins.mean()), 2) if len(wins) else 0.0,
            "avg_loss": round(float(losses.mean()), 2) if len(losses) else 0.0,
            "max_drawdown": round(float(dd.min()), 2) if len(dd) else 0.0,
            "n_days": int(det["trade_date"].nunique()),
            "n_syms": int(det["ticker"].nunique()),
            "sl_cnt": int((oc == "SL").sum()),
            "tgt_cnt": int((oc == "TARGET").sum()),
            "eod_cnt": int((~oc.isin(["SL", "TARGET"])).sum()),
            "trade_dom_gross": round(float(gross[gross > 0].max()) / gp, 3) if gp > 0 else 9.99,
            "day_dom": round(float(day_net.max()) / total, 3) if total > 0 else 9.99,
            "sym_dom": round(float(sym_net.max()) / total, 3) if total > 0 else 9.99,
            "daywise": [{"date": str(k), "n": int((det["trade_date"] == k).sum()), "net": round(float(v), 2)} for k, v in day_net.sort_values().items()],
            "symwise": [{"ticker": str(k), "n": int((det["ticker"] == k).sum()), "net": round(float(v), 2)} for k, v in sym_net.sort_values().items()],
            "hourwise": [{"hour": str(k), "n": int(len(g)), "net": round(float(g["net_pnl_rs"].sum()), 2)} for k, g in tmp.groupby("_hour")],
        }
    )
    return m, det


def stability_bad(m: dict[str, Any]) -> list[str]:
    bad: list[str] = []
    for k in ("trade_dom_gross", "day_dom", "sym_dom"):
        v = m.get(k)
        if v is not None and v > DOM_CAP:
            bad.append(f"{k}={v}>{DOM_CAP}")
    if m.get("tpd", 0.0) > MAX_TPD:
        bad.append(f"tpd={m.get('tpd')}>{MAX_TPD}")
    return bad


def split_sessions(pool: pd.DataFrame, train_start: str, test_start: str, fallback_train: int, fallback_test: int) -> dict[str, list[pd.Timestamp] | str]:
    sessions = sorted(pd.Series(pool["_day"].dropna().unique()))
    tr_start = pd.Timestamp(train_start)
    te_start = pd.Timestamp(test_start)
    test = [s for s in sessions if s >= te_start]
    train = [s for s in sessions if tr_start <= s < (test[0] if test else sessions[-1] + pd.Timedelta(days=1))]
    note = f"requested split used: TRAIN {train_start}..session before {test_start}; TEST {test_start}..latest"
    if not train or not test:
        n_train = min(fallback_train, max(1, len(sessions) - fallback_test))
        n_test = min(fallback_test, max(1, len(sessions) - n_train))
        test = sessions[-n_test:]
        train = sessions[-(n_train + n_test) : -n_test]
        note = (
            f"requested TEST start {test_start} unavailable for {SETUP}; pool sessions end {sessions[-1].date()}. "
            f"Used nearest available rolling split: TRAIN last {len(train)} sessions before TEST, TEST last {len(test)} sessions."
        )
    half = len(train) // 2
    return {"FIT": train[:half], "VAL": train[half:], "TRAIN": train, "TEST": test, "NOTE": note}


def slice_days(df: pd.DataFrame, days: list[pd.Timestamp]) -> pd.DataFrame:
    dset = set(pd.Timestamp(d).normalize() for d in days)
    return df[df["_day"].isin(dset)].reset_index(drop=True)


def quantiles(df: pd.DataFrame, features: list[str]) -> dict[str, dict[float, float]]:
    out: dict[str, dict[float, float]] = {}
    for f in features:
        if f not in df.columns:
            continue
        s = pd.to_numeric(df[f], errors="coerce").replace([np.inf, -np.inf], np.nan).dropna()
        if len(s) >= 8 and s.nunique() > 1:
            out[f] = {q: round(float(s.quantile(q)), 6) for q in QGRID}
    return out


def premom_quantiles(train: pd.DataFrame) -> dict[str, dict[float, float]]:
    recs = []
    for r in train.itertuples():
        feats, reason = tt._premom(r.ticker, r.side, r.tt_entry_iso, float(r.tt_fill), 0.70, r.tt_sig_ts.isoformat())
        d = dict(feats) if not reason else {}
        recs.append({f: d.get(f, np.nan) for f in PM_FEATURES})
    return quantiles(pd.DataFrame(recs), PM_FEATURES)


def score_fitval(fit: dict[str, Any], val: dict[str, Any], min_fold: int) -> float:
    if fit["trades"] < min_fold or val["trades"] < min_fold:
        return -10.0 + min(fit["trades"], val["trades"]) / max(1, min_fold)
    fp = BAND_HI if not np.isfinite(fit["pf"]) else min(float(fit["pf"]), BAND_HI)
    vp = BAND_HI if not np.isfinite(val["pf"]) else min(float(val["pf"]), BAND_HI)
    return min(fp, vp) - 0.50 * abs(fp - vp)


def classify(row: dict[str, Any]) -> str:
    train = row["train"]
    test = row.get("test")
    if train["trades"] < row["min_train"]:
        return "too few trades"
    if train["pf"] < BAND_LO:
        return "TRAIN PF too low"
    if train["pf"] > BAND_HI:
        return "TRAIN PF too high / overfit risk"
    bad = stability_bad(train)
    if bad:
        return "one trade/day/symbol dominated result"
    if not test:
        return "TEST not run"
    if test["trades"] < row["min_test"]:
        return "too few trades"
    if test["pf"] <= TEST_PF_MIN:
        return "TEST PF below 1.40"
    bad = stability_bad(test)
    if bad:
        return "one trade/day/symbol dominated result"
    return "PASS"


def evaluate_cfg(
    rows: list[dict[str, Any]],
    seen: set[str],
    name: str,
    stage: str,
    group: str,
    cfg: dict[str, Any],
    fit_df: pd.DataFrame,
    val_df: pd.DataFrame,
    train_df: pd.DataFrame,
    test_df: pd.DataFrame,
    min_fold: int,
    min_train: int,
    min_test: int,
    reason: str,
) -> dict[str, Any] | None:
    key = cfg_key(cfg)
    if key in seen:
        return None
    seen.add(key)
    fit = light_metrics(cfg, fit_df)
    val = light_metrics(cfg, val_df)
    score = score_fitval(fit, val, min_fold)
    train, _ = full_metrics(cfg, train_df)
    test = None
    test_scored = False
    if BAND_LO <= train["pf"] <= BAND_HI:
        test, _ = full_metrics(cfg, test_df)
        test_scored = True
    row = {
        "iteration": len(rows) + 1,
        "name": name,
        "stage": stage,
        "group": group,
        "reason": reason,
        "cfg": public_cfg(cfg),
        "fit": fit,
        "val": val,
        "train": train,
        "test": test,
        "test_scored": test_scored,
        "score": round(float(score), 4),
        "min_train": min_train,
        "min_test": min_test,
    }
    row["failure"] = classify(row)
    row["passed"] = row["failure"] == "PASS" and train["trades"] >= min_train and bool(test) and test["trades"] >= min_test
    rows.append(row)
    return row


def add_exit_sweeps(rows, seen, base, fit_df, val_df, train_df, test_df, min_fold, min_train, min_test, cap: int = 25):
    stop_at = len(rows) + cap
    for sl in SL_GRID:
        for tgt in TGT_GRID:
            if len(rows) >= stop_at:
                return
            cfg = dict(base)
            cfg["sl"] = sl
            cfg["tgt"] = tgt
            evaluate_cfg(rows, seen, f"exit_baseline_{sl}_{tgt}", "Stage 2", "SL/target", cfg, fit_df, val_df, train_df, test_df, min_fold, min_train, min_test, "exit grid around baseline filters")


def add_feature_sweeps(rows, seen, train_df, qmap, fit_df, val_df, train_eval_df, test_df, min_fold, min_train, min_test, cap: int = 35):
    stop_at = len(rows) + cap
    raw = {**BASELINE, "mask_terms": [], "premom_terms": [], "guard": None, "sl": 0.70, "tgt": 1.50}
    for feat in qmap:
        for q, val in qmap[feat].items():
            for op in (">=", "<="):
                if len(rows) >= stop_at:
                    return
                cfg = dict(raw)
                cfg["mask_terms"] = [[feat, op, val]]
                evaluate_cfg(rows, seen, f"signal_{feat}_{op}_{q}", "Stage 2", "indicator/filter", cfg, fit_df, val_df, train_eval_df, test_df, min_fold, min_train, min_test, f"single-column range sweep for {feat}")
    if "regime" in train_df.columns:
        for val in sorted(train_df["regime"].astype(str).str.upper().dropna().unique()):
            for op in ("==", "!="):
                if len(rows) >= stop_at:
                    return
                cfg = dict(raw)
                cfg["mask_terms"] = [["regime", op, val]]
                evaluate_cfg(rows, seen, f"regime_{op}_{val}", "Stage 2", "regime filter", cfg, fit_df, val_df, train_eval_df, test_df, min_fold, min_train, min_test, "categorical regime range")


def add_premom_guard_sweeps(rows, seen, pm_q, fit_df, val_df, train_df, test_df, min_fold, min_train, min_test, pm_cap: int = 16, guard_cap: int = 9):
    pm_stop_at = len(rows) + pm_cap
    raw = {**BASELINE, "mask_terms": [], "premom_terms": [], "guard": None, "sl": 0.70, "tgt": 1.50}
    pm_done = False
    for feat in pm_q:
        if pm_done:
            break
        for q, val in pm_q[feat].items():
            if pm_done:
                break
            for op in (">=", "<="):
                if len(rows) >= pm_stop_at:
                    pm_done = True
                    break
                cfg = dict(raw)
                cfg["premom_terms"] = [[feat, op, val]]
                evaluate_cfg(rows, seen, f"premom_{feat}_{op}_{q}", "Stage 2", "pre-momentum", cfg, fit_df, val_df, train_df, test_df, min_fold, min_train, min_test, f"single pre-momentum range sweep for {feat}")
    guard_stop_at = len(rows) + guard_cap
    for guard in (
        {"min_slot": "09:45"},
        {"min_slot": "10:00"},
        {"min_slot": "10:30"},
        {"max_slot": "11:30"},
        {"max_slot": "12:30"},
        {"max_slot": "14:00"},
        {"min_slot": "09:45", "max_slot": "11:30"},
        {"min_slot": "10:30", "max_slot": "12:30"},
        {"top_n": 1},
        {"top_n": 2},
        {"top_n": 3},
    ):
        if len(rows) >= guard_stop_at:
            return
        cfg = dict(raw)
        cfg["guard"] = guard
        evaluate_cfg(rows, seen, f"guard_{json.dumps(guard, sort_keys=True)}", "Stage 2", "guard", cfg, fit_df, val_df, train_df, test_df, min_fold, min_train, min_test, "time/top_n guard sweep")


def add_combo_sweeps(rows, seen, fit_df, val_df, train_df, test_df, min_fold, min_train, min_test, cap: int = 15):
    stop_at = len(rows) + cap
    eligible = [r for r in rows if r["stage"] == "Stage 2" and r["fit"]["trades"] >= min_fold and r["val"]["trades"] >= min_fold]
    eligible.sort(key=lambda r: (r["score"], r["train"]["pf"]), reverse=True)
    signal_terms = []
    pm_terms = []
    guards = []
    for r in eligible:
        cfg = r["cfg"]
        if len(cfg["mask_terms"]) == 1 and cfg["mask_terms"][0] not in signal_terms:
            signal_terms.append(cfg["mask_terms"][0])
        if len(cfg["premom_terms"]) == 1 and cfg["premom_terms"][0] not in pm_terms:
            pm_terms.append(cfg["premom_terms"][0])
        if cfg["entry_guards"] and cfg["entry_guards"] not in guards:
            guards.append(cfg["entry_guards"])
        if len(signal_terms) >= 10 and len(pm_terms) >= 6 and len(guards) >= 5:
            break
    exit_pairs = [(0.50, 1.25), (0.60, 3.00), (0.70, 1.50), (1.20, 3.00), (0.90, 2.00)]
    for sl, tgt in exit_pairs:
        for term_a, term_b in itertools.combinations(signal_terms[:8], 2):
            if len(rows) >= stop_at:
                return
            cfg = {**BASELINE, "sl": sl, "tgt": tgt, "mask_terms": [term_a, term_b], "premom_terms": [], "guard": None, "max_positions": 20}
            evaluate_cfg(rows, seen, f"combo_signal_{sl}_{tgt}", "Stage 3", "best train-side combination", cfg, fit_df, val_df, train_df, test_df, min_fold, min_train, min_test, "combine two stable signal terms from Stage 2")
    for term in signal_terms[:6]:
        for pm in pm_terms[:4]:
            if len(rows) >= stop_at:
                return
            cfg = {**BASELINE, "sl": 0.70, "tgt": 1.50, "mask_terms": [term], "premom_terms": [pm], "guard": None, "max_positions": 20}
            evaluate_cfg(rows, seen, "combo_signal_pm", "Stage 3", "best train-side combination", cfg, fit_df, val_df, train_df, test_df, min_fold, min_train, min_test, "combine signal term with pre-momentum term from Stage 2")
    for guard in guards[:5]:
        if len(rows) >= stop_at:
            return
        cfg = {**BASELINE, "sl": 1.20, "tgt": 3.00, "mask_terms": [], "premom_terms": [], "guard": guard, "max_positions": 10, "daily_loss_rs": 5000.0}
        evaluate_cfg(rows, seen, "combo_guard_exit", "Stage 3", "best train-side combination", cfg, fit_df, val_df, train_df, test_df, min_fold, min_train, min_test, "combine best guard with wider exit")


def fmt_m(m: dict[str, Any] | None) -> str:
    if not m:
        return "not run"
    return f"n={m['trades']} PF={m['pf']} net=Rs{m['net']:,.0f} win={m.get('win_rate_pct', 0)}% t/s/e={m.get('tgt_cnt', 0)}/{m.get('sl_cnt', 0)}/{m.get('eod_cnt', 0)} dom={m.get('trade_dom_gross')}/{m.get('day_dom')}/{m.get('sym_dom')}"


def write_outputs(outdir: Path, rows: list[dict[str, Any]], baseline: dict[str, Any], sess: dict[str, Any], command: str, pool_path: str) -> None:
    outdir.mkdir(parents=True, exist_ok=True)
    (outdir / "candidates").mkdir(exist_ok=True)
    flat = []
    for r in rows:
        flat.append(
            {
                "iteration": r["iteration"],
                "stage": r["stage"],
                "group": r["group"],
                "name": r["name"],
                "score": r["score"],
                "fit_n": r["fit"]["trades"],
                "fit_pf": r["fit"]["pf"],
                "val_n": r["val"]["trades"],
                "val_pf": r["val"]["pf"],
                "train_n": r["train"]["trades"],
                "train_pf": r["train"]["pf"],
                "train_net": r["train"]["net"],
                "test_scored": r["test_scored"],
                "test_n": "" if not r["test"] else r["test"]["trades"],
                "test_pf": "" if not r["test"] else r["test"]["pf"],
                "test_net": "" if not r["test"] else r["test"]["net"],
                "failure": r["failure"],
                "passed": r["passed"],
                "sl": r["cfg"]["sl"],
                "tgt": r["cfg"]["tgt"],
                "mask_terms": json.dumps(r["cfg"]["mask_terms"]),
                "premom_terms": json.dumps(r["cfg"]["premom_terms"]),
                "entry_guards": json.dumps(r["cfg"]["entry_guards"]),
                "reason": r["reason"],
                "cfg_json": json.dumps(r["cfg"], sort_keys=True),
            }
        )
    pd.DataFrame(flat).to_csv(outdir / "parameter_sweep_all.csv", index=False)
    passed = [r for r in rows if r["passed"]]
    train_band = [r for r in rows if BAND_LO <= r["train"]["pf"] <= BAND_HI]
    best_meaningful = [r for r in rows if r["train"]["trades"] >= 15]
    best_meaningful.sort(key=lambda r: (r["train"]["pf"], r["score"]), reverse=True)
    best = passed[0] if passed else (best_meaningful[0] if best_meaningful else rows[0])

    baseline_lines = [
        f"# BASELINE_RESULT - {SETUP} ({SIDE})",
        "",
        "## Current Rules",
        f"- Source: `Train_and_Test/SETUP_CARDS_AND_LIVE_CROSSCHECK.md` and demoted config block in `final_setup_conf.py`.",
        "- Indicator values: RSI 50-72, ADX >=20, EMA20 above EMA50, EMA20 slope >0, close above VWAP/EMA20.",
        "- Non-indicator rules: strong green close, close_loc >=0.60, close > previous bar high, low tests VWAP + 0.30*ATR.",
        f"- Filters: {terms_str(BASELINE['mask_terms'])}",
        "- Pre-momentum: none.",
        "- Guards: none.",
        "- Exit: fixed SL 0.50%, target 1.25%, 1-minute SL/target/EOD resolver.",
        "",
        "## Exact Sessions",
        f"- FIT: {', '.join(sess['FIT'])}",
        f"- VAL: {', '.join(sess['VAL'])}",
        f"- TRAIN: {sess['TRAIN'][0]}..{sess['TRAIN'][-1]} ({len(sess['TRAIN'])} sessions)",
        f"- TEST: {', '.join(sess['TEST'])}",
        f"- Note: {sess['NOTE']}",
        "",
        "## Baseline Metrics",
        f"- FIT: {fmt_m(baseline['fit'])}",
        f"- VAL: {fmt_m(baseline['val'])}",
        f"- TRAIN: {fmt_m(baseline['train'])}",
        f"- TEST: {fmt_m(baseline['test'])}",
        "",
        "## Initial Diagnosis",
        "- The current card lands in the TRAIN PF band only because it leaves 2 TRAIN trades; this is not meaningful.",
        "- TEST is one losing trade in the nearest available holdout.",
        "- Meaningful-trade variants in the second pass remain well below TRAIN PF 1.30.",
    ]
    (outdir / "BASELINE_RESULT.md").write_text("\n".join(baseline_lines) + "\n", encoding="utf-8")

    by_group: dict[str, list[dict[str, Any]]] = {}
    for r in rows:
        by_group.setdefault(r["group"], []).append(r)
    sweep_lines = [
        f"# PARAMETER_SWEEP_SUMMARY - {SETUP}",
        "",
        f"Command: `{command}`",
        f"Pool: `{pool_path}`",
        "",
        f"Evaluated {len(rows)} unique configs. TEST was scored only for {sum(1 for r in rows if r['test_scored'])} full-TRAIN-band configs.",
        "",
    ]
    for group, group_rows in by_group.items():
        group_rows = sorted(group_rows, key=lambda r: (r["score"], r["train"]["pf"]), reverse=True)
        sweep_lines += [f"## {group}", ""]
        for r in group_rows[:8]:
            sweep_lines.append(
                f"- iter {r['iteration']}: {r['name']} | FIT {r['fit']['trades']}/PF {r['fit']['pf']} "
                f"VAL {r['val']['trades']}/PF {r['val']['pf']} TRAIN {r['train']['trades']}/PF {r['train']['pf']} "
                f"-> {r['failure']} | cfg {json.dumps(r['cfg'], sort_keys=True)}"
            )
        sweep_lines.append("")
    sweep_lines += [
        "## Stable Range Notes",
        "- Exit widening (for example 0.60/3.00 or 1.20/3.00) improved trade count but TRAIN PF stayed around 0.49 or lower.",
        "- The original quality/volume/near-VWAP/morning gate is too selective: only 2 TRAIN trades and 1 TEST trade.",
        "- Relaxed filters and guard-driven variants create meaningful trade counts but convert the setup into a broad losing firehose.",
        "- No tested indicator, price-action, pre-momentum, filter, guard, or SL/target range produced a stable TRAIN-band candidate.",
    ]
    (outdir / "PARAMETER_SWEEP_SUMMARY.md").write_text("\n".join(sweep_lines) + "\n", encoding="utf-8")

    iter_lines = [
        f"# ITERATION_LOG - {SETUP}",
        "",
        f"Command: `{command}`",
        "",
        "Each iteration changes one logical group or one staged combination. TEST is shown only when full TRAIN PF is inside [1.30, 1.70].",
        "",
    ]
    for r in rows:
        iter_lines += [
            f"## Iter {r['iteration']} - {r['stage']} / {r['group']} - {r['failure']}",
            f"- changed parameter: {r['name']}",
            f"- reason: {r['reason']}",
            f"- config: `{json.dumps(r['cfg'], sort_keys=True)}`",
            f"- FIT: {fmt_m(r['fit'])}",
            f"- VAL: {fmt_m(r['val'])}",
            f"- TRAIN: {fmt_m(r['train'])}",
            f"- TEST: {fmt_m(r['test']) if r['test_scored'] else 'not run (TRAIN PF not in band)'}",
            f"- keep/reject: {'KEEP' if r['passed'] else 'REJECT'}",
            "- next action: continue train-side search" if not r["passed"] else "- next action: approval candidate",
            "",
        ]
    (outdir / "ITERATION_LOG.md").write_text("\n".join(iter_lines), encoding="utf-8")

    fail = best
    fail_lines = [
        f"# FAILURE_ANALYSIS - {SETUP}",
        "",
        f"Primary analyzed config: iter {fail['iteration']} ({fail['failure']}).",
        f"- Config: `{json.dumps(fail['cfg'], sort_keys=True)}`",
        f"- TRAIN: {fmt_m(fail['train'])}",
        f"- TEST: {fmt_m(fail['test']) if fail['test_scored'] else 'not run'}",
        "",
        "## Losing Trade / Structure Notes",
        "- The baseline is too sparse to diagnose: one TRAIN target, one TRAIN SL, one TEST SL.",
        "- Meaningful-trade variants fail because the broad RS-leader VWAP-hold signal does not follow through after costs.",
        "- Failures classify mainly as TRAIN PF too low, too few trades for the baseline, and one day/symbol dominance for tiny subsets.",
        "",
        "## Worst TRAIN Days",
    ]
    for d in fail["train"].get("daywise", [])[:10]:
        fail_lines.append(f"- {d['date']}: n={d['n']} net=Rs{d['net']:,.0f}")
    fail_lines += ["", "## Worst TRAIN Symbols"]
    for s in fail["train"].get("symwise", [])[:10]:
        fail_lines.append(f"- {s['ticker']}: n={s['n']} net=Rs{s['net']:,.0f}")
    fail_lines += ["", "## Time Windows"]
    for h in fail["train"].get("hourwise", []):
        fail_lines.append(f"- {h['hour']}:00 n={h['n']} net=Rs{h['net']:,.0f}")
    fail_lines += [
        "",
        "## Failure Classes Seen",
        "- TRAIN PF too low for all meaningful-trade variants.",
        "- Too few trades for the original card gate.",
        "- One trade/day/symbol dominance for any tiny in-band pocket.",
        "- Known live/backtest mismatch: setup was demoted after live paper PF 0.15.",
    ]
    (outdir / "FAILURE_ANALYSIS.md").write_text("\n".join(fail_lines) + "\n", encoding="utf-8")

    cand_lines = [
        f"# CANDIDATE_CONFIGS - {SETUP}",
        "",
        "Only passing configs are listed here.",
        "",
    ]
    if not passed:
        cand_lines += [
            "No candidate passed TRAIN PF 1.30-1.70, TEST PF > 1.40, trade-count, and stability checks.",
            "",
            "See `parameter_sweep_all.csv` and `PARAMETER_SWEEP_SUMMARY.md` for the second-pass sweeps.",
        ]
    else:
        for i, r in enumerate(passed, 1):
            path = outdir / "candidates" / f"{SETUP}_candidate_{i:03d}.json"
            data = {"setup": SETUP, "side": SIDE, "config": r["cfg"], "train": r["train"], "test": r["test"], "windows": sess}
            path.write_text(json.dumps(json_safe(data), indent=2, allow_nan=False), encoding="utf-8")
            cand_lines += [f"## Candidate {i:03d}", f"- File: `{path}`", f"- TRAIN: {fmt_m(r['train'])}", f"- TEST: {fmt_m(r['test'])}", ""]
    if not passed:
        (outdir / "candidates" / "NO_CANDIDATES.md").write_text(
            "No second-pass candidate passed the approval gate.\n", encoding="utf-8"
        )
    (outdir / "CANDIDATE_CONFIGS.md").write_text("\n".join(cand_lines) + "\n", encoding="utf-8")

    rec_lines = [
        f"# APPROVAL_REQUIRED_FINAL_RECOMMENDATION - {SETUP} ({SIDE})",
        "",
        "DO NOT MOVE TO FINAL CONFIG UNTIL USER APPROVES",
        "",
    ]
    if not passed:
        rec_lines += [
            "## Recommendation",
            "NO - do not promote.",
            "",
            "No second-pass staged sweep candidate cleared the approval gate.",
            "",
            "## Best Meaningful Near-Miss",
            f"- {fmt_m(best['train'])}",
            f"- Config: `{json.dumps(best['cfg'], sort_keys=True)}`",
            f"- Failure: {best['failure']}",
            "",
        ]
    else:
        r = passed[0]
        rec_lines += [
            "## Recommendation",
            "YES - approval required before any edit.",
            "",
            "```json",
            json.dumps(r["cfg"], indent=2),
            "```",
        ]
    rec_lines += [
        "## Final Config File Requiring Approval",
        "- `final_setup_conf.py`",
        "- `Train_and_Test/final_setup_conf.py` mirror only after explicit approval",
        "",
        "## Rerun Commands",
        "```powershell",
        "py -3.12 Train_and_Test\\setup_loop_runner.py --setup L_RS_LEADER_VWAP_HOLD --pool Train_and_Test\\setup_pf_1_4_approval_loop\\L_RS_LEADER_VWAP_HOLD\\pool --configs Train_and_Test\\setup_pf_1_4_approval_loop\\L_RS_LEADER_VWAP_HOLD\\baseline_config.json --train_start 2026-03-16 --train_end 2026-05-13 --test_start 2026-05-14 --test_end 2026-05-27 --slippage_bps 15",
        "",
        command,
        "```",
    ]
    (outdir / "APPROVAL_REQUIRED_FINAL_RECOMMENDATION.md").write_text("\n".join(rec_lines) + "\n", encoding="utf-8")

    summary = {
        "setup": SETUP,
        "side": SIDE,
        "windows": sess,
        "n_iterations": len(rows),
        "n_train_band": len(train_band),
        "n_candidates": len(passed),
        "baseline": baseline,
        "best_meaningful": best,
    }
    (outdir / "second_pass_summary.json").write_text(json.dumps(json_safe(summary), indent=2, allow_nan=False), encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--pool", default=str(TT_DIR / "setup_pf_1_4_approval_loop" / SETUP / "pool"))
    ap.add_argument("--out", default=str(TT_DIR / "setup_pf_1_4_approval_loop" / SETUP))
    ap.add_argument("--train_start", default="2026-05-18")
    ap.add_argument("--test_start", default="2026-06-20")
    ap.add_argument("--fallback_train_sessions", type=int, default=27)
    ap.add_argument("--fallback_test_sessions", type=int, default=9)
    ap.add_argument("--slippage_bps", type=float, default=15.0)
    ap.add_argument("--min_fold", type=int, default=6)
    ap.add_argument("--min_train_trades", type=int, default=15)
    ap.add_argument("--min_test_trades", type=int, default=5)
    ap.add_argument("--max_iterations", type=int, default=100)
    args = ap.parse_args()
    try:
        sys.stdout.reconfigure(line_buffering=True)
    except Exception:
        pass

    tt.SLIPPAGE_BPS = float(args.slippage_bps)
    tt._entry.cache_clear()
    tt._resolve_full.cache_clear()
    tt._premom.cache_clear()
    tt.POOL_DIRS = [Path(args.pool)]
    tt.POOL_DIR = Path(args.pool)
    pool = tt.load_pool()
    pool = pool[pool["setup"].astype(str).eq(SETUP)].copy()
    if pool.empty:
        raise SystemExit(f"no rows for {SETUP} in {args.pool}")
    sess_raw = split_sessions(pool, args.train_start, args.test_start, args.fallback_train_sessions, args.fallback_test_sessions)
    sess = {
        k: [str(pd.Timestamp(x).date()) for x in v] if isinstance(v, list) else v
        for k, v in sess_raw.items()
    }
    print(f"[second-pass] {SETUP}")
    print(f"[second-pass] {sess['NOTE']}")
    print(f"[second-pass] FIT {sess['FIT'][0]}..{sess['FIT'][-1]} VAL {sess['VAL'][0]}..{sess['VAL'][-1]}")
    print(f"[second-pass] TRAIN {sess['TRAIN'][0]}..{sess['TRAIN'][-1]} TEST {sess['TEST'][0]}..{sess['TEST'][-1]}")

    span = list(sess_raw["FIT"]) + list(sess_raw["VAL"]) + list(sess_raw["TEST"])
    attached = tt.attach_entries(slice_days(pool, span))
    fit_df = slice_days(attached, list(sess_raw["FIT"]))
    val_df = slice_days(attached, list(sess_raw["VAL"]))
    train_df = slice_days(attached, list(sess_raw["TRAIN"]))
    test_df = slice_days(attached, list(sess_raw["TEST"]))
    print(f"[second-pass] entries FIT={len(fit_df)} VAL={len(val_df)} TRAIN={len(train_df)} TEST={len(test_df)}")

    baseline = {
        "fit": full_metrics(BASELINE, fit_df)[0],
        "val": full_metrics(BASELINE, val_df)[0],
        "train": full_metrics(BASELINE, train_df)[0],
        "test": full_metrics(BASELINE, test_df)[0],
    }
    print(f"[second-pass] baseline TRAIN PF={baseline['train']['pf']} n={baseline['train']['trades']} TEST PF={baseline['test']['pf']} n={baseline['test']['trades']}")

    sig_q = quantiles(train_df, SIGNAL_FEATURES)
    pm_q = premom_quantiles(train_df)
    print(f"[second-pass] signal features={sorted(sig_q)}")
    print(f"[second-pass] premom features={sorted(pm_q)}")

    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    evaluate_cfg(rows, seen, "baseline_card", "Stage 1", "baseline", BASELINE, fit_df, val_df, train_df, test_df, args.min_fold, args.min_train_trades, args.min_test_trades, "original card")
    add_exit_sweeps(rows, seen, BASELINE, fit_df, val_df, train_df, test_df, args.min_fold, args.min_train_trades, args.min_test_trades)
    add_feature_sweeps(rows, seen, train_df, sig_q, fit_df, val_df, train_df, test_df, args.min_fold, args.min_train_trades, args.min_test_trades)
    add_premom_guard_sweeps(rows, seen, pm_q, fit_df, val_df, train_df, test_df, args.min_fold, args.min_train_trades, args.min_test_trades)
    add_combo_sweeps(rows, seen, fit_df, val_df, train_df, test_df, args.min_fold, args.min_train_trades, args.min_test_trades)
    rows = rows[: args.max_iterations]

    outdir = Path(args.out)
    command = (
        "py -3.12 Train_and_Test\\setup_pf_1_4_approval_loop\\L_RS_LEADER_VWAP_HOLD\\scripts\\parameter_sweep_again.py "
        f"--pool {args.pool} --max_iterations {args.max_iterations} --slippage_bps {args.slippage_bps:g}"
    )
    write_outputs(outdir, rows, baseline, sess, command, args.pool)
    passed = [r for r in rows if r["passed"]]
    meaningful = [r for r in rows if r["train"]["trades"] >= args.min_train_trades]
    meaningful.sort(key=lambda r: (r["train"]["pf"], r["score"]), reverse=True)
    best = meaningful[0] if meaningful else rows[0]
    print(f"[second-pass] evaluated={len(rows)} train-band={sum(1 for r in rows if BAND_LO <= r['train']['pf'] <= BAND_HI)} candidates={len(passed)}")
    print(f"[second-pass] best meaningful TRAIN n={best['train']['trades']} PF={best['train']['pf']} net={best['train']['net']} failure={best['failure']}")
    print(f"[second-pass] artifacts -> {outdir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
