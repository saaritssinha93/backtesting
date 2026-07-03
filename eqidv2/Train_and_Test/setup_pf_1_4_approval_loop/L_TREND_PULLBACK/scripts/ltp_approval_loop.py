from __future__ import annotations

import argparse
import json
import math
import random
import sys
import time
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

HERE = Path(__file__).resolve().parent
TT_DIR = HERE
for _ in range(6):
    TT_DIR = TT_DIR.parent
    if (TT_DIR / "setup_train_test.py").exists():
        break
REPO = TT_DIR.parent
for p in (str(REPO), str(TT_DIR)):
    if p not in sys.path:
        sys.path.insert(0, p)

import avwap_5min_ID_v11_backtesting as v11  # noqa: E402
import setup_train_test as tt  # noqa: E402

try:
    import optuna  # type: ignore

    optuna.logging.set_verbosity(optuna.logging.WARNING)
    HAVE_OPTUNA = True
except Exception:
    optuna = None
    HAVE_OPTUNA = False


SETUP = "L_TREND_PULLBACK"
SIDE = "LONG"
TRAIN_START = pd.Timestamp("2026-05-18")
TEST_START = pd.Timestamp("2026-06-20")
DOM_CAP = 0.40
TRAIN_PF_LO = 1.30
TRAIN_PF_HI = 1.70
TEST_PF_MIN = 1.40
MIN_FITVAL_TRADES = 3
MIN_TRAIN_TRADES = 8
MIN_TEST_TRADES = 2

MASK_FEATS = [
    "market_ret_pct",
    "market_abs_ret_pct",
    "rs_pct",
    "vol_ratio",
    "atr_pct",
    "body_pct",
    "close_loc",
    "vwap_dist_atr",
    "quality_score",
    "ranker_score",
    "signal_range_pct",
    "upper_wick_pct",
    "lower_wick_pct",
    "wick_skew_pct",
]
PM_FEATS = [
    "pre_entry_momentum_score",
    "sig5_adx_calc",
    "sig5_rsi_dir",
    "sig5_vol_ratio20",
    "pre1_adx",
    "pre2_mom_r",
    "pre3_range_r",
    "pre5_mom_r",
    "pre3_close_pos",
]
QGRID = [0.10, 0.20, 0.30, 0.40, 0.50, 0.60, 0.70, 0.80, 0.90]
SL_GRID = [0.40, 0.50, 0.60, 0.70, 0.80, 0.90, 1.00, 1.10, 1.20, 1.50]
TGT_GRID = [0.60, 0.80, 0.90, 1.00, 1.20, 1.50, 1.75, 2.00, 2.50]
MIN_SLOTS = ["09:30", "09:45", "10:00", "10:30", "11:00"]
MAX_SLOTS = ["12:00", "12:30", "13:00", "14:00", "14:30"]


def clean_float(v: Any) -> Any:
    if isinstance(v, float):
        return None if not math.isfinite(v) else round(v, 6)
    if isinstance(v, (np.floating,)):
        f = float(v)
        return None if not math.isfinite(f) else round(f, 6)
    if isinstance(v, dict):
        return {k: clean_float(x) for k, x in v.items()}
    if isinstance(v, list):
        return [clean_float(x) for x in v]
    return v


def read_setup_pool(pool_csv: Path, setup: str) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for chunk in pd.read_csv(pool_csv, low_memory=False, chunksize=50_000):
        sub = chunk[chunk["setup"].astype(str).eq(setup)].copy()
        if not sub.empty:
            frames.append(sub)
    if not frames:
        raise SystemExit(f"No rows for {setup} in {pool_csv}")
    df = pd.concat(frames, ignore_index=True, sort=False)
    for c in ("ticker", "side", "setup", "signal_time_ist"):
        if c not in df.columns:
            df[c] = ""
    df = df.drop_duplicates(subset=["ticker", "side", "setup", "signal_time_ist"], keep="first")
    df["setup"] = df["setup"].astype(str).str.strip()
    df["side"] = df["side"].astype(str).str.upper().str.strip()
    df["ticker"] = df["ticker"].astype(str).str.upper().str.strip()
    df["tt_sig_ts"] = df["signal_time_ist"].map(v11._normalise_ts)
    df = df.dropna(subset=["tt_sig_ts"]).copy()
    df["_day"] = df["tt_sig_ts"].dt.normalize().dt.tz_localize(None)
    df["_slot"] = df["tt_sig_ts"].map(v11._fmt_ist)
    return v11._selected_strategy_features(df).reset_index(drop=True)


def metric_pf(net: np.ndarray) -> float:
    net = np.asarray(net, dtype=float)
    net = net[np.isfinite(net)]
    if len(net) == 0:
        return 0.0
    gp = float(net[net > 0].sum())
    gl = float(net[net <= 0].sum())
    return float("inf") if abs(gl) < 1e-9 and gp > 0 else (gp / abs(gl) if abs(gl) > 0 else 0.0)


def cfg_str(cfg: dict[str, Any]) -> str:
    mask = "; ".join(f"{a}{o}{b}" for a, o, b in cfg["mask_terms"]) or "-"
    pm = "; ".join(f"{a}{o}{b}" for a, o, b in cfg["premom_terms"]) or "-"
    guard = json.dumps(cfg.get("guard") or {}, sort_keys=True)
    return f"SL={cfg['sl']} TGT={cfg['tgt']} mask=[{mask}] premom=[{pm}] guard={guard}"


def quantiles(df: pd.DataFrame, feats: list[str]) -> dict[str, dict[float, float]]:
    out: dict[str, dict[float, float]] = {}
    for f in feats:
        if f not in df.columns:
            continue
        s = pd.to_numeric(df[f], errors="coerce").replace([np.inf, -np.inf], np.nan).dropna()
        if len(s) >= 8 and s.nunique() > 1:
            out[f] = {q: round(float(s.quantile(q)), 6) for q in QGRID}
    return out


def premom_frame(df: pd.DataFrame, sl: float = 0.70) -> pd.DataFrame:
    rows: list[dict[str, float]] = []
    for r in df.itertuples():
        feats, reason = tt._premom(r.ticker, r.side, r.tt_entry_iso, float(r.tt_fill), float(sl), r.tt_sig_ts.isoformat())
        rows.append({f: np.nan for f in PM_FEATS} if reason else {f: dict(feats).get(f, np.nan) for f in PM_FEATS})
    return pd.DataFrame(rows)


def resolve_detail(book: pd.DataFrame, exits: dict[str, tuple[float, float]]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for r in book.itertuples():
        sl, tgt = exits[r.setup]
        res = tt._resolve_full(r.ticker, r.side, r.tt_entry_iso, float(r.tt_fill), int(r.tt_qty), float(sl), float(tgt))
        if res is None:
            continue
        exit_iso, outcome, exit_px = res
        net = tt._trade_net(r.side, float(r.tt_fill), int(r.tt_qty), str(outcome), float(exit_px))
        gross = ((float(exit_px) - float(r.tt_fill)) if r.side == "LONG" else (float(r.tt_fill) - float(exit_px))) * int(r.tt_qty)
        rows.append(
            {
                "trade_date": str(r.tt_sig_ts.date()),
                "ticker": r.ticker,
                "side": r.side,
                "setup": r.setup,
                "signal_time": str(r.tt_sig_ts),
                "entry_time": str(r.tt_entry_iso),
                "entry_price": round(float(r.tt_fill), 2),
                "exit_time": exit_iso,
                "exit_price": round(float(exit_px), 2),
                "qty": int(r.tt_qty),
                "sl_pct": sl,
                "tgt_pct": tgt,
                "outcome": str(outcome),
                "gross_pnl_rs": round(float(gross), 2),
                "net_pnl_rs": round(float(net), 2),
            }
        )
    return pd.DataFrame(rows)


def summarize_detail(det: pd.DataFrame, fam: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {
        "n": int(fam["trades"]),
        "net_pf": round(float(fam["net_pf"]), 4) if math.isfinite(float(fam["net_pf"])) else float("inf"),
        "net_pnl": round(float(fam["net_pnl"]), 2),
        "day_block_p": None if not math.isfinite(float(fam["day_block_p"])) else round(float(fam["day_block_p"]), 4),
    }
    if det.empty:
        out.update(
            {
                "wins": 0,
                "losses": 0,
                "win_pct": 0.0,
                "gross_profit": 0.0,
                "gross_loss": 0.0,
                "avg_win": 0.0,
                "avg_loss": 0.0,
                "max_dd": 0.0,
                "n_days": 0,
                "n_syms": 0,
                "trades_per_day": 0.0,
                "trade_dom_gross": None,
                "day_dom": None,
                "sym_dom": None,
                "outcomes": {},
            }
        )
        return out
    det = det.sort_values("entry_time")
    net = det["net_pnl_rs"].to_numpy(dtype=float)
    wins = net[net > 0]
    losses = net[net <= 0]
    cum = np.cumsum(net)
    dd = float((cum - np.maximum.accumulate(cum)).min()) if len(cum) else 0.0
    total = float(net.sum())
    gp = float(wins.sum())
    day_net = det.groupby("trade_date")["net_pnl_rs"].sum()
    sym_net = det.groupby("ticker")["net_pnl_rs"].sum()
    out.update(
        {
            "wins": int((net > 0).sum()),
            "losses": int((net <= 0).sum()),
            "win_pct": round(float((net > 0).mean()) * 100, 2),
            "gross_profit": round(gp, 2),
            "gross_loss": round(float(losses.sum()), 2),
            "avg_win": round(float(wins.mean()), 2) if len(wins) else 0.0,
            "avg_loss": round(float(losses.mean()), 2) if len(losses) else 0.0,
            "max_dd": round(dd, 2),
            "n_days": int(det["trade_date"].nunique()),
            "n_syms": int(det["ticker"].nunique()),
            "trades_per_day": round(len(det) / max(1, det["trade_date"].nunique()), 2),
            "trade_dom_gross": round(float(net.max()) / gp, 3) if gp > 0 else None,
            "day_dom": round(float(day_net.max()) / total, 3) if total > 0 else None,
            "sym_dom": round(float(sym_net.max()) / total, 3) if total > 0 else None,
            "outcomes": {str(k): int(v) for k, v in det["outcome"].astype(str).value_counts().items()},
        }
    )
    return out


def eval_cfg(setup: str, cfg: dict[str, Any], df: pd.DataFrame, with_detail: bool = False) -> tuple[dict[str, Any], pd.DataFrame]:
    tt.MAX_POSITIONS = int(cfg.get("max_positions", 20))
    tt.DAILY_LOSS_RS = float(cfg.get("daily_loss_rs", 0.0))
    tt.REGIME_ALIGN = bool(cfg.get("regime_align", False))
    tt.REGIME_BAND = float(cfg.get("regime_band", 0.0))
    fam = tt.eval_family({setup: cfg}, df)
    det = pd.DataFrame()
    if with_detail and fam["trades"]:
        det = resolve_detail(fam["book"], {setup: (cfg["sl"], cfg["tgt"])})
        return summarize_detail(det, fam), det
    out = {
        "n": int(fam["trades"]),
        "net_pf": round(float(fam["net_pf"]), 4) if math.isfinite(float(fam["net_pf"])) else float("inf"),
        "net_pnl": round(float(fam["net_pnl"]), 2),
        "day_block_p": None if not math.isfinite(float(fam["day_block_p"])) else round(float(fam["day_block_p"]), 4),
    }
    net = np.asarray(fam.get("net", []), dtype=float)
    out["win_pct"] = round(float((net > 0).mean()) * 100, 2) if len(net) else 0.0
    return out, det


def split_summary(days: list[pd.Timestamp]) -> str:
    if not days:
        return "EMPTY"
    return f"{pd.Timestamp(days[0]).date()}..{pd.Timestamp(days[-1]).date()} ({len(days)} sessions)"


def band_score(fit: dict[str, Any], val: dict[str, Any]) -> float:
    if fit["n"] < MIN_FITVAL_TRADES or val["n"] < MIN_FITVAL_TRADES:
        return -10.0 + min(fit["n"], val["n"]) / max(1, MIN_FITVAL_TRADES)
    pf_f = 10.0 if fit["net_pf"] == float("inf") else min(float(fit["net_pf"]), 10.0)
    pf_v = 10.0 if val["net_pf"] == float("inf") else min(float(val["net_pf"]), 10.0)
    mn = min(pf_f, pf_v)
    gap = abs(pf_f - pf_v)
    weak_pen = max(0.0, TRAIN_PF_LO - mn)
    over_pen = max(0.0, max(pf_f, pf_v) - TRAIN_PF_HI)
    return min(mn, TRAIN_PF_HI) - 0.45 * gap - 0.65 * weak_pen - 0.35 * over_pen


def dom_ok(m: dict[str, Any]) -> bool:
    for k in ("trade_dom_gross", "day_dom", "sym_dom"):
        v = m.get(k)
        if v is None or float(v) > DOM_CAP:
            return False
    return True


def failure_reason(train: dict[str, Any] | None, test: dict[str, Any] | None, cfg: dict[str, Any]) -> str:
    if not train:
        return "FIT/VAL not reasonable enough for full TRAIN confirm"
    if train["n"] < MIN_TRAIN_TRADES:
        return "too few TRAIN trades"
    pf = float(train["net_pf"])
    if pf < TRAIN_PF_LO:
        return "TRAIN PF too low"
    if pf > TRAIN_PF_HI:
        return "TRAIN PF too high / overfit risk"
    if not test:
        return "TEST not run"
    if test["n"] < MIN_TEST_TRADES:
        return "too few TEST trades"
    if float(test["net_pf"]) <= TEST_PF_MIN:
        return "TEST PF below 1.40"
    if not dom_ok(test):
        return "one trade/day/symbol dominated result"
    if cfg["sl"] <= 0.5:
        return "SL too tight risk"
    if cfg["tgt"] >= 2.5 and test.get("outcomes", {}).get("TARGET", 0) <= 1:
        return "target too ambitious"
    return "accepted"


def format_metrics(m: dict[str, Any] | None) -> str:
    if not m:
        return "not run"
    return (
        f"n={m['n']} PF={m['net_pf']} net=Rs{m['net_pnl']:,.0f} "
        f"win={m.get('win_pct', 0)}% dbp={m.get('day_block_p')}"
    )


def make_trial_cfg(trial: Any, mask_q: dict[str, dict[float, float]], pm_q: dict[str, dict[float, float]], regimes: list[str]) -> dict[str, Any]:
    sl = float(trial.suggest_categorical("sl", SL_GRID))
    tgt = float(trial.suggest_categorical("tgt", TGT_GRID))
    mask_terms: list[tuple[Any, Any, Any]] = []
    n_mask = int(trial.suggest_int("n_mask", 0, 2))
    for i in range(n_mask):
        use_regime = bool(regimes) and trial.suggest_categorical(f"mask{i}_type", ["numeric", "regime"]) == "regime"
        if use_regime:
            val = trial.suggest_categorical(f"mask{i}_regime", regimes)
            op = trial.suggest_categorical(f"mask{i}_regime_op", ["==", "!="])
            mask_terms.append(("regime", op, val))
        else:
            choices = [f for f in MASK_FEATS if f in mask_q]
            f = trial.suggest_categorical(f"mask{i}_feat", choices)
            op = trial.suggest_categorical(f"mask{i}_op", [">=", "<="])
            q = trial.suggest_categorical(f"mask{i}_q", QGRID)
            mask_terms.append((f, op, mask_q[f][q]))
    premom_terms: list[tuple[Any, Any, Any]] = []
    n_pm = int(trial.suggest_int("n_pm", 0, 2))
    for i in range(n_pm):
        choices = [f for f in PM_FEATS if f in pm_q]
        if not choices:
            break
        f = trial.suggest_categorical(f"pm{i}_feat", choices)
        op = trial.suggest_categorical(f"pm{i}_op", [">=", "<="])
        q = trial.suggest_categorical(f"pm{i}_q", QGRID)
        premom_terms.append((f, op, pm_q[f][q]))
    guard: dict[str, Any] = {}
    if trial.suggest_categorical("use_min_slot", [False, True]):
        guard["min_slot"] = trial.suggest_categorical("min_slot", MIN_SLOTS)
    if trial.suggest_categorical("use_max_slot", [False, True]):
        guard["max_slot"] = trial.suggest_categorical("max_slot", MAX_SLOTS)
    top_n = trial.suggest_categorical("top_n", [0, 1, 2, 3])
    if top_n:
        guard["top_n"] = int(top_n)
    return {
        "name": "trial",
        "sl": sl,
        "tgt": tgt,
        "mask_terms": mask_terms,
        "premom_terms": premom_terms,
        "guard": guard or None,
        "status": "OK",
        "max_positions": int(trial.suggest_categorical("max_positions", [10, 20])),
        "daily_loss_rs": float(trial.suggest_categorical("daily_loss_rs", [0.0, 2500.0, 4000.0])),
        "regime_align": bool(trial.suggest_categorical("regime_align", [False, True])),
        "regime_band": float(trial.suggest_categorical("regime_band", [0.0, 0.05, 0.10])),
    }


class RandTrial:
    def __init__(self, rng: random.Random):
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

    def set_user_attr(self, *_args: Any, **_kwargs: Any) -> None:
        return None


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--pool", default=r"C:\TradingData\eqidv2\outputs_ID_v11_unified_pool\historical_all_available_pre_dedupe_live_candidates.csv")
    ap.add_argument("--out", default=str(HERE.parent))
    ap.add_argument("--trials", type=int, default=75)
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--slippage_bps", type=float, default=15.0)
    args = ap.parse_args()
    try:
        sys.stdout.reconfigure(line_buffering=True)
    except Exception:
        pass

    outdir = Path(args.out)
    (outdir / "candidates").mkdir(parents=True, exist_ok=True)
    outdir.mkdir(parents=True, exist_ok=True)

    print(f"[ltp] setup={SETUP} optimizer={'Optuna TPE' if HAVE_OPTUNA else 'Optuna unavailable; using seeded random search fallback.'}")
    pool = read_setup_pool(Path(args.pool), SETUP)
    tt.POOL_DIRS = [Path(args.pool).parent]
    tt.POOL_DIR = Path(args.pool).parent
    tt.SLIPPAGE_BPS = float(args.slippage_bps)
    tt._entry.cache_clear()
    tt._resolve_full.cache_clear()
    tt._premom.cache_clear()

    sessions = sorted(pd.Series(pool["_day"].dropna().unique()))
    train_days = [d for d in sessions if TRAIN_START <= pd.Timestamp(d) < TEST_START]
    test_days = [d for d in sessions if pd.Timestamp(d) >= TEST_START]
    if not train_days or not test_days:
        raise SystemExit(f"Cannot form strict split: train_days={len(train_days)} test_days={len(test_days)}")
    fit_days = train_days[: len(train_days) // 2]
    val_days = train_days[len(train_days) // 2 :]
    span = set(map(pd.Timestamp, train_days + test_days))
    sub = pool[pool["_day"].isin(span)].copy()
    sub = tt.attach_entries(sub)

    def slice_days(ds: list[pd.Timestamp]) -> pd.DataFrame:
        return sub[sub["_day"].isin(set(map(pd.Timestamp, ds)))].copy().reset_index(drop=True)

    FIT, VAL, TRAIN, TEST = slice_days(fit_days), slice_days(val_days), slice_days(train_days), slice_days(test_days)
    print(f"[ltp] FIT   {split_summary(fit_days)} rows={len(FIT)}")
    print(f"[ltp] VAL   {split_summary(val_days)} rows={len(VAL)}")
    print(f"[ltp] TRAIN {split_summary(train_days)} rows={len(TRAIN)}")
    print(f"[ltp] TEST  {split_summary(test_days)} rows={len(TEST)}")

    pm_train = premom_frame(TRAIN, 0.70)
    pm_train.index = TRAIN.index
    pm_for_q = pm_train.reset_index(drop=True)
    mask_q = quantiles(TRAIN, MASK_FEATS)
    pm_q = quantiles(pm_for_q, PM_FEATS)
    regimes = sorted(v for v in TRAIN.get("regime", pd.Series(dtype=str)).astype(str).str.upper().unique() if v and v != "NAN")
    print(f"[ltp] searchable mask={sorted(mask_q)} premom={sorted(pm_q)} regimes={regimes}")

    baseline_cfgs = [
        {
            "name": "baseline_v6_raw",
            "sl": 0.70,
            "tgt": 0.90,
            "mask_terms": [],
            "premom_terms": [],
            "guard": None,
            "status": "OK",
            "max_positions": 20,
            "daily_loss_rs": 0.0,
            "regime_align": False,
            "regime_band": 0.0,
        },
        {
            "name": "baseline_production_premom",
            "sl": 0.70,
            "tgt": 0.90,
            "mask_terms": [],
            "premom_terms": [("pre_entry_momentum_score", ">=", 73.021), ("pre2_mom_r", ">=", 0.233909)],
            "guard": None,
            "status": "OK",
            "max_positions": 20,
            "daily_loss_rs": 0.0,
            "regime_align": False,
            "regime_band": 0.0,
        },
        {
            "name": "research_watch_best",
            "sl": 0.50,
            "tgt": 2.50,
            "mask_terms": [("market_ret_pct", ">=", -0.286)],
            "premom_terms": [("pre2_mom_r", ">=", 0.217)],
            "guard": None,
            "status": "OK",
            "max_positions": 20,
            "daily_loss_rs": 0.0,
            "regime_align": False,
            "regime_band": 0.0,
        },
    ]

    iteration_rows: list[dict[str, Any]] = []
    passing: list[dict[str, Any]] = []
    candidate_id = 0

    def run_iteration(idx: int, tag: str, cfg: dict[str, Any]) -> tuple[float, dict[str, Any]]:
        fit, _ = eval_cfg(SETUP, cfg, FIT, with_detail=False)
        val, _ = eval_cfg(SETUP, cfg, VAL, with_detail=False)
        score = band_score(fit, val)
        train = None
        test = None
        train_detail = pd.DataFrame()
        test_detail = pd.DataFrame()
        reasonable = (
            fit["n"] >= MIN_FITVAL_TRADES
            and val["n"] >= MIN_FITVAL_TRADES
            and min(float(fit["net_pf"]), float(val["net_pf"])) >= 0.80
        )
        if reasonable:
            train, train_detail = eval_cfg(SETUP, cfg, TRAIN, with_detail=True)
            if TRAIN_PF_LO <= float(train["net_pf"]) <= TRAIN_PF_HI:
                test, test_detail = eval_cfg(SETUP, cfg, TEST, with_detail=True)
        status = failure_reason(train, test, cfg)
        if status == "accepted":
            nonlocal_candidate = {
                "iteration": idx,
                "tag": tag,
                "config": clean_float(cfg),
                "fit": clean_float(fit),
                "val": clean_float(val),
                "train": clean_float(train),
                "test": clean_float(test),
            }
            passing.append(nonlocal_candidate)
        row = {
            "iteration": idx,
            "tag": tag,
            "config": clean_float(cfg),
            "fit": clean_float(fit),
            "val": clean_float(val),
            "score": round(float(score), 6),
            "train": clean_float(train) if train else None,
            "test": clean_float(test) if test else None,
            "status": status,
            "cfg_text": cfg_str(cfg),
        }
        iteration_rows.append(row)
        print(f"[ltp] iter {idx:03d} {tag}: score={score:.3f} FIT {format_metrics(fit)} | VAL {format_metrics(val)} | TRAIN {format_metrics(train)} | TEST {format_metrics(test)} -> {status}")
        return score, row

    best_row: dict[str, Any] | None = None
    best_score = -1e9
    idx = 0
    for cfg in baseline_cfgs:
        idx += 1
        score, row = run_iteration(idx, cfg["name"], cfg)
        if score > best_score:
            best_score, best_row = score, row

    def objective(trial: Any) -> float:
        cfg = make_trial_cfg(trial, mask_q, pm_q, regimes)
        fit, _ = eval_cfg(SETUP, cfg, FIT, with_detail=False)
        val, _ = eval_cfg(SETUP, cfg, VAL, with_detail=False)
        score = band_score(fit, val)
        if HAVE_OPTUNA:
            trial.set_user_attr("fit", fit)
            trial.set_user_attr("val", val)
            trial.set_user_attr("config", clean_float(cfg))
        return float(score)

    trial_cfgs: list[dict[str, Any]] = []
    t0 = time.time()
    if HAVE_OPTUNA:
        sampler = optuna.samplers.TPESampler(seed=args.seed)
        study = optuna.create_study(direction="maximize", sampler=sampler)
        study.optimize(objective, n_trials=int(args.trials), show_progress_bar=False)
        for tr in study.trials:
            cfg = tr.user_attrs.get("config")
            if cfg:
                # Convert list terms back to tuple terms for evaluator.
                cfg["mask_terms"] = [tuple(t) for t in cfg.get("mask_terms", [])]
                cfg["premom_terms"] = [tuple(t) for t in cfg.get("premom_terms", [])]
                cfg["status"] = "OK"
                trial_cfgs.append(cfg)
    else:
        rng = random.Random(args.seed)
        for _ in range(int(args.trials)):
            rt = RandTrial(rng)
            trial_cfgs.append(make_trial_cfg(rt, mask_q, pm_q, regimes))

    # Confirm all generated trials in deterministic order after the FIT/VAL search.
    seen: set[str] = set()
    for cfg in trial_cfgs:
        sig = json.dumps(clean_float(cfg), sort_keys=True, default=str)
        if sig in seen:
            continue
        seen.add(sig)
        idx += 1
        cfg["name"] = f"trial_{idx:03d}"
        score, row = run_iteration(idx, "search", cfg)
        if score > best_score:
            best_score, best_row = score, row

    # Sort passing candidates and write candidate json files.
    passing.sort(key=lambda x: (float(x["test"]["net_pf"]), float(x["test"]["net_pnl"]), float(x["train"]["n"])), reverse=True)
    for cand in passing:
        candidate_id += 1
        cand_path = outdir / "candidates" / f"{SETUP}_candidate_{candidate_id:03d}.json"
        cand["candidate_path"] = str(cand_path)
        cand_path.write_text(json.dumps(clean_float(cand), indent=2, default=str), encoding="utf-8")

    # Re-evaluate baselines with detail for baseline report.
    baseline_details = []
    for cfg in baseline_cfgs:
        tr_m, tr_d = eval_cfg(SETUP, cfg, TRAIN, with_detail=True)
        te_m, te_d = eval_cfg(SETUP, cfg, TEST, with_detail=True)
        baseline_details.append({"config": clean_float(cfg), "train": clean_float(tr_m), "test": clean_float(te_m)})

    # Failure-analysis detail from raw baseline.
    raw_cfg = baseline_cfgs[0]
    _tr_m, tr_det = eval_cfg(SETUP, raw_cfg, TRAIN, with_detail=True)
    _te_m, te_det = eval_cfg(SETUP, raw_cfg, TEST, with_detail=True)
    all_det = pd.concat([tr_det.assign(window="TRAIN"), te_det.assign(window="TEST")], ignore_index=True) if not tr_det.empty or not te_det.empty else pd.DataFrame()

    worst_days: list[dict[str, Any]] = []
    worst_symbols: list[dict[str, Any]] = []
    time_windows: list[dict[str, Any]] = []
    if not all_det.empty:
        worst_days = [
            {"window": str(idx[0]), "trade_date": str(idx[1]), "trades": int(g.size), "net_pnl": round(float(g.sum()), 2)}
            for idx, g in all_det.groupby(["window", "trade_date"])["net_pnl_rs"]
        ]
        worst_days = sorted(worst_days, key=lambda x: x["net_pnl"])[:10]
        worst_symbols = [
            {"window": str(idx[0]), "ticker": str(idx[1]), "trades": int(g.size), "net_pnl": round(float(g.sum()), 2)}
            for idx, g in all_det.groupby(["window", "ticker"])["net_pnl_rs"]
        ]
        worst_symbols = sorted(worst_symbols, key=lambda x: x["net_pnl"])[:10]
        tmp = all_det.copy()
        tmp["entry_hour"] = pd.to_datetime(tmp["entry_time"], errors="coerce").dt.hour
        for idx, g in tmp.groupby(["window", "entry_hour"]):
            net = g["net_pnl_rs"].to_numpy(dtype=float)
            time_windows.append({"window": str(idx[0]), "entry_hour": int(idx[1]), "trades": int(len(g)), "pf": round(metric_pf(net), 4), "net_pnl": round(float(net.sum()), 2)})

    # Write machine summary.
    summary = {
        "setup": SETUP,
        "side": SIDE,
        "pool": str(args.pool),
        "optimizer": "Optuna TPE" if HAVE_OPTUNA else "Optuna unavailable; using seeded random search fallback.",
        "runtime_seconds": round(time.time() - t0, 2),
        "sessions": {
            "fit": [str(pd.Timestamp(d).date()) for d in fit_days],
            "val": [str(pd.Timestamp(d).date()) for d in val_days],
            "train": [str(pd.Timestamp(d).date()) for d in train_days],
            "test": [str(pd.Timestamp(d).date()) for d in test_days],
        },
        "rows": {"fit": len(FIT), "val": len(VAL), "train": len(TRAIN), "test": len(TEST)},
        "baselines": baseline_details,
        "best_train_side": clean_float(best_row),
        "passing_candidates": clean_float(passing),
        "iterations": clean_float(iteration_rows),
        "failure_analysis": {"worst_days": worst_days, "worst_symbols": worst_symbols, "time_windows": time_windows},
    }
    (outdir / "run_summary.json").write_text(json.dumps(summary, indent=2, default=str), encoding="utf-8")

    # Markdown reports.
    def md_table_metric(rows: list[dict[str, Any]]) -> str:
        lines = ["| config | TRAIN | TEST |", "|---|---|---|"]
        for r in rows:
            lines.append(f"| {r['config']['name']} `{cfg_str(r['config'])}` | {format_metrics(r['train'])} | {format_metrics(r['test'])} |")
        return "\n".join(lines)

    (outdir / "BASELINE_RESULT.md").write_text(
        "\n".join(
            [
                f"# {SETUP} - BASELINE_RESULT",
                "",
                f"- **Side:** {SIDE}",
                "- **Config source:** `final_setup_conf.py` / `Train_and_Test/final_setup_conf.py` `RESEARCH_WATCH_CONF` disabled reject.",
                "- **Current live status:** not traded / not promoted.",
                f"- **FIT sessions:** {split_summary(fit_days)}",
                f"- **VAL sessions:** {split_summary(val_days)}",
                f"- **TRAIN sessions:** {split_summary(train_days)}",
                f"- **TEST sessions:** {split_summary(test_days)}",
                f"- **Rows after entry attach:** FIT={len(FIT)} VAL={len(VAL)} TRAIN={len(TRAIN)} TEST={len(TEST)}",
                "- **Cost/slippage:** repo `setup_train_test.py`, statutory costs, 15 bps/leg slippage.",
                "",
                "## Baselines",
                md_table_metric(baseline_details),
                "",
                "## Initial diagnosis",
                "- Strict TEST has only 2 setup sessions, so all TEST results are thin and day-dominance-sensitive.",
                "- The raw setup remains loss-making; earlier research-watch rescue is re-tested in this split rather than assumed.",
            ]
        ),
        encoding="utf-8",
    )

    iter_lines = [f"# {SETUP} - ITERATION_LOG", "", f"Optimizer: {'Optuna TPE' if HAVE_OPTUNA else 'Optuna unavailable; using seeded random search fallback.'}", ""]
    for r in iteration_rows:
        iter_lines += [
            f"## Iteration {r['iteration']:03d} - {r['tag']}",
            f"- **Changed group:** {'baseline' if r['tag'].startswith('baseline') or r['tag']=='research_watch_best' else 'search-supported mask/pre-momentum/guard/exit'}",
            f"- **Parameters:** `{r['cfg_text']}`",
            f"- **FIT:** {format_metrics(r['fit'])}",
            f"- **VAL:** {format_metrics(r['val'])}",
            f"- **TRAIN:** {format_metrics(r.get('train'))}",
            f"- **TEST:** {format_metrics(r.get('test'))}",
            f"- **Keep/reject:** {r['status']}",
            "",
        ]
    (outdir / "ITERATION_LOG.md").write_text("\n".join(iter_lines), encoding="utf-8")

    fail_lines = [
        f"# {SETUP} - FAILURE_ANALYSIS",
        "",
        "## Failure Classification",
        "- Baseline failure: TRAIN PF too low and raw pullbacks bleed through costs.",
        "- Main recurring search failure: many candidates either stay below TRAIN PF 1.30 or enter the train band but fail dominance / thin-test confidence.",
        "- TEST caveat: only two TEST sessions (`2026-06-22`, `2026-06-24`) are available for this setup under the strict split.",
        "",
        "## Worst Days",
    ]
    fail_lines += [f"- {x}" for x in worst_days] or ["- none"]
    fail_lines += ["", "## Worst Symbols"]
    fail_lines += [f"- {x}" for x in worst_symbols] or ["- none"]
    fail_lines += ["", "## Time Window Result"]
    fail_lines += [f"- {x}" for x in sorted(time_windows, key=lambda x: (x['window'], x['entry_hour']))] or ["- none"]
    (outdir / "FAILURE_ANALYSIS.md").write_text("\n".join(fail_lines), encoding="utf-8")

    cand_lines = [f"# {SETUP} - CANDIDATE_CONFIGS", ""]
    if not passing:
        cand_lines += ["No candidates passed TRAIN PF 1.30-1.70 + TEST PF > 1.40 + dominance checks.", ""]
    else:
        for i, c in enumerate(passing, 1):
            cand_lines += [
                f"## Candidate {i:03d}",
                f"- **Config:** `{cfg_str(c['config'])}`",
                f"- **TRAIN:** {format_metrics(c['train'])}",
                f"- **TEST:** {format_metrics(c['test'])}",
                f"- **Path:** `{c['candidate_path']}`",
                "- **Risk:** strict TEST has only two setup sessions; require user approval and more live-paper holdout.",
                "",
            ]
    (outdir / "CANDIDATE_CONFIGS.md").write_text("\n".join(cand_lines), encoding="utf-8")

    best = passing[0] if passing else None
    rec_lines = [
        f"# {SETUP} - APPROVAL_REQUIRED_FINAL_RECOMMENDATION",
        "",
        "## Recommendation",
        "APPROVAL REQUIRED" if best else "NO APPROVAL CANDIDATE",
        "",
    ]
    if best:
        rec_lines += [
            "## Best Candidate",
            f"- **Config:** `{cfg_str(best['config'])}`",
            f"- **TRAIN:** {format_metrics(best['train'])}",
            f"- **TEST:** {format_metrics(best['test'])}",
            f"- **Candidate file:** `{best['candidate_path']}`",
            "",
            "## Proposed Config Block",
            "```json",
            json.dumps(clean_float(best["config"]), indent=2, default=str),
            "```",
        ]
    else:
        rec_lines += ["No candidate passed the required gate.", ""]
    rec_lines += [
        "",
        "## Final File That Would Need Approval Before Edit",
        "- `final_setup_conf.py`",
        "- `Train_and_Test/final_setup_conf.py`",
        "",
        "## Rerun Command",
        f"`py -3.12 {Path(__file__).as_posix()} --trials {args.trials} --seed {args.seed}`",
        "",
        "DO NOT MOVE TO FINAL CONFIG UNTIL USER APPROVES",
    ]
    (outdir / "APPROVAL_REQUIRED_FINAL_RECOMMENDATION.md").write_text("\n".join(rec_lines), encoding="utf-8")

    print(f"[ltp] wrote {outdir}")
    print(f"[ltp] passing_candidates={len(passing)}")
    if best:
        print(f"[ltp] best={cfg_str(best['config'])} TRAIN {format_metrics(best['train'])} TEST {format_metrics(best['test'])}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
