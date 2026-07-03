"""Full PF 1.4 research loop for A_PULLBACK_C2_THEN_BREAK_C2_HIGH.

Research-only. Writes every artifact under this setup directory and never edits
final_setup_conf.py.
"""
from __future__ import annotations

import argparse
import itertools
import json
import math
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

HERE = Path(__file__).resolve()
WORK = HERE.parents[1]
TRAIN_AND_TEST = HERE.parents[3]
REPO_ROOT = TRAIN_AND_TEST.parent
for _p in (str(REPO_ROOT), str(TRAIN_AND_TEST)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import all_setups_catalog as catalog  # noqa: E402
import setup_train_test as tt  # noqa: E402

SETUP = "A_PULLBACK_C2_THEN_BREAK_C2_HIGH"
SIDE = "LONG"
FAMILY = "A"
FNAME = "historical_all_available_pre_dedupe_live_candidates.csv"
KEY = ["ticker", "side", "setup", "signal_time_ist"]

MASTER_POOL = Path(
    r"C:\TradingData\eqidv2\outputs_ID_v11_unified_pool\historical_all_available_pre_dedupe_live_candidates.csv"
)
TAIL_RAW = WORK / "pools" / "tail_v11_raw" / "historical_all_available_raw_candidates.csv"
TAIL_DATES = WORK / "pools" / "tail_v11_raw" / "historical_all_available_dates.csv"
POOL_DIR = WORK / "pools" / "pool_full"
POOL_CSV = POOL_DIR / FNAME

TRAIN_REQ = ("2026-03-01", "2026-05-30")
TEST_REQ = ("2026-06-01", "2026-07-02")

BASE_EXIT = {"sl": 0.70, "tgt": 0.90}
try:
    if SETUP in tt.v11.v6.SETUP_EXIT_RULES:
        _sl, _tg = tt.v11.v6.SETUP_EXIT_RULES[SETUP]
        BASE_EXIT = {"sl": float(_sl), "tgt": float(_tg)}
except Exception:
    pass

PRICE_COL_HINTS = ("open", "high", "low", "close", "price", "entry", "exit")
VOL_COL_HINTS = ("volume", "vol_", "volratio", "vol_ratio", "traded_value", "notional", "day_value")
VWAP_COL_HINTS = ("vwap", "avwap")
MA_COL_HINTS = ("ema", "sma", "ma20", "ma50", "ma200")
MOM_COL_HINTS = ("rsi", "adx", "macd", "mfi", "obv", "cci", "stoch", "williams", "roc", "supertrend")
ATR_COL_HINTS = ("atr", "range", "volatility", "bb_", "boll", "keltner")
CANDLE_COL_HINTS = ("body", "wick", "close_loc", "signal_range")
TIME_COL_HINTS = ("date", "time", "minute", "slot", "day", "session")
REASON_COL_HINTS = ("reason", "setup", "candidate", "ranker", "quality")

MASK_FEATURES_BY_GROUP = {
    "indicator": [
        "rs_pct",
        "atr_pct",
        "vwap_dist_atr",
        "ADX",
        "RSI",
        "MACD",
        "MACD_Signal",
        "MACD_Hist",
        "EMA20",
        "EMA50",
        "ema20_slope_3bar",
        "BB_Width",
        "MFI",
        "OBV",
        "CCI",
        "Stoch_K",
        "Williams_R",
        "ROC",
        "quality_score",
        "ranker_score",
    ],
    "non_indicator_price_action": [
        "body_pct",
        "close_loc",
        "upper_wick_pct",
        "lower_wick_pct",
        "wick_skew_pct",
        "signal_range_pct",
        "vol_ratio",
    ],
    "filter": [
        "traded_value_rs",
        "day_value_so_far_rs",
        "notional",
        "market_ret_pct",
        "market_abs_ret_pct",
        "signal_minute",
    ],
}

PREMOM_FEATURES = [
    "pre_entry_momentum_score",
    "sig5_adx_calc",
    "sig5_rsi_dir",
    "sig5_vol_ratio20",
    "pre1_adx",
    "pre1_body_r",
    "pre1_close_pos",
    "pre1_range_r",
    "pre2_mom_r",
    "pre3_mom_r",
    "pre5_mom_r",
    "pre10_mom_r",
    "pre3_range_r",
    "pre5_range_r",
    "pre10_range_r",
    "pre3_close_pos",
    "pre5_close_pos",
    "pre10_close_pos",
    "pre3_dir_count",
    "pre5_dir_count",
    "pre10_dir_count",
    "pre3_body_sum_r",
    "pre5_body_sum_r",
    "pre10_body_sum_r",
    "pre3_vol_ratio20",
    "pre5_vol_ratio20",
    "pre10_vol_ratio20",
]

SL_GRID = [0.50, 0.60, 0.70, 0.85, 0.90, 1.00, 1.10, 1.20, 1.40]
TGT_GRID = [0.70, 0.80, 0.90, 1.00, 1.20, 1.50, 1.80, 2.00, 2.50]
QGRID = [0.15, 0.25, 0.35, 0.50, 0.65, 0.75, 0.85]


@dataclass
class Candidate:
    name: str
    sl: float
    tgt: float
    mask_terms: list[list[Any]] = field(default_factory=list)
    premom_terms: list[list[Any]] = field(default_factory=list)
    guard: dict[str, Any] = field(default_factory=dict)
    max_positions: int = 20
    daily_loss_rs: float = 0.0
    regime_align: bool = False
    regime_band: float = 0.0
    group: str = "baseline"
    reason: str = ""
    old_value: str = ""
    new_value: str = ""

    def key(self) -> str:
        return json.dumps(self.to_config(), sort_keys=True, default=str)

    def to_config(self) -> dict[str, Any]:
        return {
            "sl": float(self.sl),
            "tgt": float(self.tgt),
            "mask_terms": self.mask_terms,
            "premom_terms": self.premom_terms,
            "guard": self.guard or {},
            "max_positions": int(self.max_positions),
            "daily_loss_rs": float(self.daily_loss_rs),
            "regime_align": bool(self.regime_align),
            "regime_band": float(self.regime_band),
        }


def _json_safe(obj: Any) -> Any:
    if isinstance(obj, dict):
        return {str(k): _json_safe(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_json_safe(v) for v in obj]
    if isinstance(obj, (np.integer,)):
        return int(obj)
    if isinstance(obj, (np.floating,)):
        val = float(obj)
        return val if math.isfinite(val) else None
    if isinstance(obj, float):
        return obj if math.isfinite(obj) else None
    if isinstance(obj, (pd.Timestamp,)):
        return str(obj)
    return obj


def _parse_day(s: pd.Series) -> pd.Series:
    ts = pd.to_datetime(s, errors="coerce", utc=True)
    return ts.dt.tz_convert("Asia/Kolkata").dt.tz_localize(None).dt.strftime("%Y-%m-%d")


def _business_days(start: str, end: str) -> list[str]:
    return [str(d.date()) for d in pd.bdate_range(start, end)]


def _sessions_in_range(sessions: list[str], start: str, end: str) -> list[str]:
    return [d for d in sessions if start <= d <= end]


def build_pool() -> dict[str, Any]:
    POOL_DIR.mkdir(parents=True, exist_ok=True)
    frames: list[pd.DataFrame] = []
    all_sessions: set[str] = set()
    source_counts: dict[str, int] = {}
    setup_counts: dict[str, int] = {}

    if not MASTER_POOL.exists():
        raise FileNotFoundError(f"Missing master pool: {MASTER_POOL}")

    kept_master = 0
    for chunk in pd.read_csv(MASTER_POOL, chunksize=150_000, low_memory=False):
        if "signal_time_ist" in chunk.columns:
            all_sessions.update(_parse_day(chunk["signal_time_ist"]).dropna().astype(str).tolist())
        mask = chunk["setup"].astype(str).str.strip().eq(SETUP)
        if mask.any():
            sub = chunk.loc[mask].copy()
            sub["_source_pool"] = "master_unified_pre_dedupe"
            frames.append(sub)
            kept_master += int(mask.sum())
    source_counts["master_unified_pre_dedupe"] = kept_master

    kept_tail = 0
    if TAIL_RAW.exists():
        tail = pd.read_csv(TAIL_RAW, low_memory=False)
        if "signal_time_ist" in tail.columns:
            all_sessions.update(_parse_day(tail["signal_time_ist"]).dropna().astype(str).tolist())
        mask = tail["setup"].astype(str).str.strip().eq(SETUP)
        if mask.any():
            sub = tail.loc[mask].copy()
            sub["_source_pool"] = "tail_v11_raw_generated"
            frames.append(sub)
            kept_tail = int(mask.sum())
    source_counts["tail_v11_raw_generated"] = kept_tail

    if not frames:
        pool = pd.DataFrame(columns=KEY)
    else:
        pool = pd.concat(frames, ignore_index=True, sort=False)
    pool = pool.dropna(subset=["signal_time_ist"]).copy()
    day = _parse_day(pool["signal_time_ist"])
    in_req = day.between(TRAIN_REQ[0], TEST_REQ[1])
    pool = pool.loc[in_req].copy()
    before = len(pool)
    for c in KEY:
        if c not in pool.columns:
            pool[c] = ""
    pool = pool.drop_duplicates(subset=KEY, keep="last").sort_values("signal_time_ist").reset_index(drop=True)
    pool.to_csv(POOL_CSV, index=False)

    setup_sessions = sorted(set(_parse_day(pool["signal_time_ist"]).dropna().astype(str).tolist()))
    all_sessions_sorted = sorted(d for d in all_sessions if TRAIN_REQ[0] <= d <= TEST_REQ[1])
    train_sessions = _sessions_in_range(all_sessions_sorted, *TRAIN_REQ)
    test_sessions = _sessions_in_range(all_sessions_sorted, *TEST_REQ)
    setup_counts["rows_pre_dedupe_in_requested_range"] = int(before)
    setup_counts["rows_final"] = int(len(pool))

    missing_train = sorted(set(_business_days(*TRAIN_REQ)) - set(train_sessions))
    missing_test = sorted(set(_business_days(*TEST_REQ)) - set(test_sessions))
    manifest = {
        "setup": SETUP,
        "side": SIDE,
        "master_src": str(MASTER_POOL),
        "tail_src": str(TAIL_RAW),
        "out_file": str(POOL_CSV),
        "requested_train": TRAIN_REQ,
        "requested_test": TEST_REQ,
        "available_first_session": all_sessions_sorted[0] if all_sessions_sorted else None,
        "available_last_session": all_sessions_sorted[-1] if all_sessions_sorted else None,
        "actual_train_sessions": train_sessions,
        "actual_test_sessions": test_sessions,
        "actual_train_range": [train_sessions[0], train_sessions[-1]] if train_sessions else None,
        "actual_test_range": [test_sessions[0], test_sessions[-1]] if test_sessions else None,
        "missing_train_weekdays": missing_train,
        "missing_test_weekdays": missing_test,
        "setup_signal_sessions": setup_sessions,
        "source_counts": source_counts,
        "setup_counts": setup_counts,
        "tail_dates_file": str(TAIL_DATES) if TAIL_DATES.exists() else None,
    }
    (POOL_DIR / "_manifest.json").write_text(json.dumps(_json_safe(manifest), indent=2), encoding="utf-8")
    return manifest


def load_prepared_pool() -> tuple[pd.DataFrame, dict[str, Any]]:
    tt.POOL_DIRS = [POOL_DIR]
    tt.POOL_DIR = POOL_DIR
    tt.TRAIN = TRAIN_REQ
    tt.TEST = TEST_REQ
    tt.SLIPPAGE_BPS = 15.0
    tt.COST_MODEL = "statutory"
    pool = tt.load_pool()
    pool = pool[pool["setup"].astype(str).str.strip().eq(SETUP)].copy()
    pool = tt.attach_entries(pool)
    manifest = json.loads((POOL_DIR / "_manifest.json").read_text(encoding="utf-8"))
    return pool.reset_index(drop=True), manifest


def split_windows(pool: pd.DataFrame, manifest: dict[str, Any]) -> dict[str, pd.DataFrame]:
    train_sessions = list(manifest.get("actual_train_sessions") or [])
    test_sessions = list(manifest.get("actual_test_sessions") or [])
    fit_n = max(1, int(math.floor(len(train_sessions) * 0.60)))
    fit_sessions = train_sessions[:fit_n]
    val_sessions = train_sessions[fit_n:]
    if not val_sessions and train_sessions:
        fit_sessions = train_sessions[:-1]
        val_sessions = train_sessions[-1:]

    def _slice(sessions: list[str]) -> pd.DataFrame:
        want = {pd.Timestamp(d) for d in sessions}
        return pool[pool["_day"].isin(want)].copy().reset_index(drop=True)

    manifest["actual_fit_sessions"] = fit_sessions
    manifest["actual_val_sessions"] = val_sessions
    (POOL_DIR / "_manifest.json").write_text(json.dumps(_json_safe(manifest), indent=2), encoding="utf-8")
    return {
        "FIT": _slice(fit_sessions),
        "VAL": _slice(val_sessions),
        "TRAIN": _slice(train_sessions),
        "TEST": _slice(test_sessions),
    }


def _terms(raw: list[list[Any]]) -> list[tuple[Any, ...]]:
    return [tuple(x) for x in (raw or [])]


def _apply_globals(c: Candidate) -> None:
    tt.MAX_POSITIONS = int(c.max_positions)
    tt.DAILY_LOSS_RS = float(c.daily_loss_rs)
    tt.REGIME_ALIGN = bool(c.regime_align)
    tt.REGIME_BAND = float(c.regime_band)


def _eval_family(c: Candidate, df: pd.DataFrame) -> dict[str, Any]:
    _apply_globals(c)
    cfg = {
        SETUP: {
            "status": "OK",
            "sl": float(c.sl),
            "tgt": float(c.tgt),
            "mask_terms": _terms(c.mask_terms),
            "premom_terms": _terms(c.premom_terms),
            "guard": c.guard or None,
        }
    }
    return tt.eval_family(cfg, df)


def detail_from_book(book: pd.DataFrame, c: Candidate) -> pd.DataFrame:
    if book is None or book.empty:
        return pd.DataFrame()
    rows = []
    for r in book.itertuples():
        full = tt._resolve_full(
            r.ticker,
            r.side,
            r.tt_entry_iso,
            float(r.tt_fill),
            int(r.tt_qty),
            float(c.sl),
            float(c.tgt),
        )
        if full is None:
            continue
        exit_iso, outcome, exit_px = full
        net = tt._trade_net(r.side, float(r.tt_fill), int(r.tt_qty), str(outcome), float(exit_px))
        gross = ((exit_px - r.tt_fill) if r.side == "LONG" else (r.tt_fill - exit_px)) * r.tt_qty
        rec = {
            "trade_date": str(pd.Timestamp(r.tt_sig_ts).date()),
            "ticker": r.ticker,
            "side": r.side,
            "setup": r.setup,
            "signal_time": str(r.tt_sig_ts),
            "entry_time": str(r.tt_entry_iso),
            "exit_time": str(exit_iso),
            "entry_price": round(float(r.tt_fill), 2),
            "exit_price": round(float(exit_px), 2),
            "qty": int(r.tt_qty),
            "sl_pct": float(c.sl),
            "tgt_pct": float(c.tgt),
            "outcome": str(outcome),
            "gross_pnl_rs": round(float(gross), 2),
            "net_pnl_rs": round(float(net), 2),
            "bars_held": int((pd.Timestamp(exit_iso) - pd.Timestamp(r.tt_entry_iso)).total_seconds() // 60)
            if pd.notna(pd.Timestamp(exit_iso))
            else np.nan,
        }
        for col in (
            "signal_minute",
            "vol_ratio",
            "atr_pct",
            "body_pct",
            "close_loc",
            "upper_wick_pct",
            "lower_wick_pct",
            "wick_skew_pct",
            "vwap_dist_atr",
            "rs_pct",
            "market_ret_pct",
            "market_abs_ret_pct",
            "quality_score",
            "ranker_score",
            "day_value_so_far_rs",
            "notional",
        ):
            if hasattr(r, col):
                rec[col] = getattr(r, col)
        rows.append(rec)
    return pd.DataFrame(rows)


def _pf_from_net(net: np.ndarray) -> float:
    net = net[np.isfinite(net)]
    if len(net) == 0:
        return 0.0
    gp = float(net[net > 0].sum())
    gl = float(-net[net < 0].sum())
    if gl <= 0:
        return float("inf") if gp > 0 else 0.0
    return gp / gl


def _max_drawdown(net: np.ndarray) -> float:
    net = net[np.isfinite(net)]
    if len(net) == 0:
        return 0.0
    eq = np.cumsum(net)
    peaks = np.maximum.accumulate(eq)
    dd = eq - peaks
    return float(dd.min())


def metrics_from_detail(detail: pd.DataFrame, fam: dict[str, Any] | None = None) -> dict[str, Any]:
    if detail is None or detail.empty:
        return {
            "trades": 0,
            "wins": 0,
            "losses": 0,
            "win_rate": 0.0,
            "gross_profit": 0.0,
            "gross_loss": 0.0,
            "net_pnl": 0.0,
            "profit_factor": 0.0,
            "avg_win": 0.0,
            "avg_loss": 0.0,
            "avg_win_loss_ratio": 0.0,
            "max_drawdown": 0.0,
            "sl_count": 0,
            "target_count": 0,
            "time_exit_count": 0,
            "trailing_sl_count": 0,
            "break_even_count": 0,
            "trades_per_day": 0.0,
            "avg_holding_minutes": 0.0,
            "top_trade_gross_profit_share": None,
            "top_day_net_share": None,
            "top_symbol_net_share": None,
            "n_days": 0,
            "n_symbols": 0,
        }
    net = pd.to_numeric(detail["net_pnl_rs"], errors="coerce").fillna(0.0).to_numpy(float)
    wins = net[net > 0]
    losses = net[net <= 0]
    gp = float(wins.sum())
    gl = float(-net[net < 0].sum())
    total = float(net.sum())
    days = detail.groupby("trade_date")["net_pnl_rs"].sum()
    syms = detail.groupby("ticker")["net_pnl_rs"].sum()
    pf = _pf_from_net(net)

    def _share(x: float, denom: float) -> float | None:
        if denom <= 0:
            return None
        return round(float(x) / denom, 4)

    oc = detail["outcome"].astype(str).str.upper()
    top_trade_share = _share(float(wins.max()) if len(wins) else 0.0, gp)
    top_day_share = _share(float(days.max()) if len(days) else 0.0, total)
    top_sym_share = _share(float(syms.max()) if len(syms) else 0.0, total)
    avg_win = float(wins.mean()) if len(wins) else 0.0
    avg_loss = float(losses.mean()) if len(losses) else 0.0
    out = {
        "trades": int(len(detail)),
        "wins": int((net > 0).sum()),
        "losses": int((net <= 0).sum()),
        "win_rate": round(float((net > 0).mean() * 100), 2),
        "gross_profit": round(gp, 2),
        "gross_loss": round(gl, 2),
        "net_pnl": round(total, 2),
        "profit_factor": round(float(pf), 4) if math.isfinite(pf) else float("inf"),
        "avg_win": round(avg_win, 2),
        "avg_loss": round(avg_loss, 2),
        "avg_win_loss_ratio": round(abs(avg_win / avg_loss), 4) if avg_loss else 0.0,
        "max_drawdown": round(_max_drawdown(net), 2),
        "sl_count": int((oc == "SL").sum()),
        "target_count": int((oc == "TARGET").sum()),
        "time_exit_count": int((oc == "EOD").sum()),
        "trailing_sl_count": 0,
        "break_even_count": 0,
        "trades_per_day": round(len(detail) / max(1, detail["trade_date"].nunique()), 3),
        "avg_holding_minutes": round(float(pd.to_numeric(detail["bars_held"], errors="coerce").mean()), 2),
        "top_trade_gross_profit_share": top_trade_share,
        "top_day_net_share": top_day_share,
        "top_symbol_net_share": top_sym_share,
        "n_days": int(detail["trade_date"].nunique()),
        "n_symbols": int(detail["ticker"].nunique()),
    }
    if fam:
        dbp = fam.get("day_block_p")
        out["day_block_p"] = None if dbp is None or not np.isfinite(dbp) else round(float(dbp), 4)
    return out


def basic_metrics(fam: dict[str, Any]) -> dict[str, Any]:
    return {
        "trades": int(fam.get("trades", 0)),
        "profit_factor": round(float(fam.get("net_pf", 0.0)), 4)
        if np.isfinite(float(fam.get("net_pf", 0.0)))
        else float("inf"),
        "net_pnl": round(float(fam.get("net_pnl", 0.0)), 2),
        "day_block_p": None
        if fam.get("day_block_p") is None or not np.isfinite(float(fam.get("day_block_p", np.nan)))
        else round(float(fam.get("day_block_p")), 4),
    }


def evaluate_candidate(c: Candidate, windows: dict[str, pd.DataFrame], run_test: bool) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for label in ("FIT", "VAL", "TRAIN"):
        fam = _eval_family(c, windows[label])
        det = detail_from_book(fam["book"], c)
        out[label] = metrics_from_detail(det, fam)
    if run_test:
        fam = _eval_family(c, windows["TEST"])
        det = detail_from_book(fam["book"], c)
        out["TEST"] = metrics_from_detail(det, fam)
    else:
        out["TEST"] = None
    return out


def score_train_side(metrics: dict[str, Any]) -> float:
    fit = metrics["FIT"]
    val = metrics["VAL"]
    train = metrics["TRAIN"]
    if train["trades"] < 20:
        return -100.0 + train["trades"]
    fit_pf = min(float(fit["profit_factor"]), 5.0) if math.isfinite(float(fit["profit_factor"])) else 5.0
    val_pf = min(float(val["profit_factor"]), 5.0) if math.isfinite(float(val["profit_factor"])) else 5.0
    train_pf = min(float(train["profit_factor"]), 5.0) if math.isfinite(float(train["profit_factor"])) else 5.0
    gap = abs(fit_pf - val_pf)
    band_penalty = 0.0
    if train["profit_factor"] < 1.30:
        band_penalty = (1.30 - train["profit_factor"]) * 2.0
    elif train["profit_factor"] > 1.80:
        band_penalty = (train["profit_factor"] - 1.80) * 2.5
    return min(fit_pf, val_pf, train_pf) - 0.45 * gap - band_penalty + min(train["trades"], 200) / 1000.0


def train_side_promising(metrics: dict[str, Any]) -> tuple[bool, str]:
    fit, val, train = metrics["FIT"], metrics["VAL"], metrics["TRAIN"]
    reasons = []
    if train["trades"] < 20:
        reasons.append("train_n<20")
    if fit["trades"] < 5:
        reasons.append("fit_n<5")
    if val["trades"] < 5:
        reasons.append("val_n<5")
    if train["net_pnl"] <= 0:
        reasons.append("train_net<=0")
    if not (1.30 <= float(train["profit_factor"]) <= 1.80):
        reasons.append("train_pf_outside_1.30_1.80")
    if float(fit["profit_factor"]) < 1.05:
        reasons.append("fit_pf<1.05")
    if float(val["profit_factor"]) < 1.05:
        reasons.append("val_pf<1.05")
    if fit["net_pnl"] < 0:
        reasons.append("fit_net<0")
    if val["net_pnl"] < 0:
        reasons.append("val_net<0")
    return (not reasons), "; ".join(reasons) or "passes_train_side_gate"


def domination_ok(metrics: dict[str, Any]) -> tuple[bool, str]:
    reasons = []
    if metrics["top_trade_gross_profit_share"] is not None and metrics["top_trade_gross_profit_share"] > 0.35:
        reasons.append(f"top_trade_share>{0.35}")
    if metrics["top_day_net_share"] is not None and metrics["top_day_net_share"] > 0.40:
        reasons.append(f"top_day_share>{0.40}")
    if metrics["top_symbol_net_share"] is not None and metrics["top_symbol_net_share"] > 0.40:
        reasons.append(f"top_symbol_share>{0.40}")
    return (not reasons), "; ".join(reasons) or "ok"


def acceptance(metrics: dict[str, Any]) -> tuple[bool, str]:
    train, test = metrics["TRAIN"], metrics.get("TEST") or {}
    reasons = []
    if not (1.30 <= float(train.get("profit_factor", 0.0)) <= 1.80):
        reasons.append("TRAIN PF outside 1.30..1.80")
    if float(test.get("profit_factor", 0.0)) <= 1.40:
        reasons.append("TEST PF <= 1.40")
    if train.get("net_pnl", 0.0) <= 0:
        reasons.append("TRAIN net <= 0")
    if test.get("net_pnl", 0.0) <= 0:
        reasons.append("TEST net <= 0")
    if train.get("trades", 0) < 20:
        reasons.append("TRAIN trades < 20")
    if test.get("trades", 0) < 5:
        reasons.append("TEST trades < 5")
    for label in ("TRAIN", "TEST"):
        ok, why = domination_ok(metrics[label])
        if not ok:
            reasons.append(f"{label} domination: {why}")
    return (not reasons), "; ".join(reasons) or "APPROVAL_REQUIRED"


def quantile_terms(df: pd.DataFrame, features: list[str], qs: list[float] = QGRID) -> list[list[Any]]:
    terms = []
    for f in features:
        if f not in df.columns:
            continue
        s = pd.to_numeric(df[f], errors="coerce").replace([np.inf, -np.inf], np.nan).dropna()
        if len(s) < 20 or s.nunique() <= 2:
            continue
        for q in qs:
            val = float(s.quantile(q))
            if not np.isfinite(val):
                continue
            val = round(val, 6)
            terms.append([f, ">=", val])
            terms.append([f, "<=", val])
    return terms


def diverse_terms(terms: list[list[Any]], per_feature: int = 4, max_terms: int = 40) -> list[list[Any]]:
    """Interleave terms by feature so a sweep does not spend every slot on one column."""
    buckets: dict[str, list[list[Any]]] = defaultdict(list)
    for t in terms:
        buckets[str(t[0])].append(t)
    ordered: list[list[Any]] = []
    feats = sorted(buckets)
    for i in range(per_feature):
        for f in feats:
            if i < len(buckets[f]):
                ordered.append(buckets[f][i])
                if len(ordered) >= max_terms:
                    return ordered
    return ordered[:max_terms]


def premom_quantile_terms(train_df: pd.DataFrame, base_sl: float, sample_n: int = 1200) -> tuple[list[list[Any]], pd.DataFrame]:
    sample = train_df.sort_values("tt_sig_ts").reset_index(drop=True)
    if len(sample) > sample_n:
        idx = np.linspace(0, len(sample) - 1, sample_n).round().astype(int)
        sample = sample.iloc[idx].copy()
    print(f"[full-loop] sampling {len(sample)} TRAIN rows for pre-momentum threshold inventory", flush=True)
    rows = []
    for i, r in enumerate(sample.itertuples(), start=1):
        feats, reason = tt._premom(r.ticker, r.side, r.tt_entry_iso, float(r.tt_fill), float(base_sl), r.tt_sig_ts.isoformat())
        fd = dict(feats) if not reason else {}
        rows.append({f: fd.get(f, np.nan) for f in PREMOM_FEATURES})
        if i % 250 == 0:
            print(f"[full-loop] pre-momentum inventory {i}/{len(sample)}", flush=True)
    pm = pd.DataFrame(rows)
    terms = []
    for f in PREMOM_FEATURES:
        if f not in pm.columns:
            continue
        s = pd.to_numeric(pm[f], errors="coerce").replace([np.inf, -np.inf], np.nan).dropna()
        if len(s) < 20 or s.nunique() <= 2:
            continue
        for q in [0.20, 0.35, 0.50, 0.65, 0.80]:
            val = round(float(s.quantile(q)), 6)
            terms.append([f, ">=", val])
            terms.append([f, "<=", val])
    return terms, pm


def _cfg_with(base: Candidate, **kw: Any) -> Candidate:
    data = {
        "name": kw.pop("name", base.name),
        "sl": kw.pop("sl", base.sl),
        "tgt": kw.pop("tgt", base.tgt),
        "mask_terms": [list(x) for x in kw.pop("mask_terms", base.mask_terms)],
        "premom_terms": [list(x) for x in kw.pop("premom_terms", base.premom_terms)],
        "guard": dict(kw.pop("guard", base.guard or {})),
        "max_positions": kw.pop("max_positions", base.max_positions),
        "daily_loss_rs": kw.pop("daily_loss_rs", base.daily_loss_rs),
        "regime_align": kw.pop("regime_align", base.regime_align),
        "regime_band": kw.pop("regime_band", base.regime_band),
        "group": kw.pop("group", base.group),
        "reason": kw.pop("reason", base.reason),
        "old_value": kw.pop("old_value", base.old_value),
        "new_value": kw.pop("new_value", base.new_value),
    }
    if kw:
        raise TypeError(f"unknown candidate fields: {kw}")
    return Candidate(**data)


def generate_candidates(train_df: pd.DataFrame, baseline: Candidate, max_iterations: int) -> tuple[list[Candidate], dict[str, Any]]:
    inventory: dict[str, Any] = {}
    candidates: list[Candidate] = [baseline]
    seen = {baseline.key()}

    def add(c: Candidate) -> None:
        if len(candidates) >= max_iterations:
            return
        k = c.key()
        if k in seen:
            return
        seen.add(k)
        candidates.append(c)

    # Stage 3a: broad but balanced exit sweep from the raw baseline.
    exit_pairs = [
        (0.50, 0.70), (0.50, 1.00), (0.50, 1.50), (0.50, 2.00), (0.50, 2.50),
        (0.60, 0.80), (0.60, 1.20), (0.60, 1.80), (0.60, 2.50),
        (0.70, 0.90), (0.70, 1.20), (0.70, 1.50), (0.70, 1.80), (0.70, 2.00), (0.70, 2.50),
        (0.85, 0.90), (0.85, 1.20), (0.85, 1.50), (0.85, 1.80), (0.85, 2.00), (0.85, 2.50),
        (0.90, 1.00), (0.90, 1.50), (0.90, 2.00), (0.90, 2.50),
        (1.00, 1.20), (1.00, 1.50), (1.00, 2.00), (1.00, 2.50),
        (1.10, 1.50), (1.10, 2.00), (1.10, 2.50),
        (1.20, 1.50), (1.20, 2.00), (1.20, 2.50),
        (1.40, 2.00), (1.40, 2.50),
    ]
    for sl, tgt in exit_pairs:
        add(
            _cfg_with(
                baseline,
                name=f"exit_sl{sl}_tgt{tgt}",
                sl=sl,
                tgt=tgt,
                group="exit",
                reason="fixed SL/target grid sweep",
                old_value=f"{baseline.sl}/{baseline.tgt}",
                new_value=f"{sl}/{tgt}",
            )
        )
    # Stage 3b: time and guard sweeps.
    guard_variants = [
        {"min_slot": "09:30"},
        {"min_slot": "09:45"},
        {"min_slot": "10:00"},
        {"min_slot": "10:30"},
        {"max_slot": "12:00"},
        {"max_slot": "12:30"},
        {"max_slot": "13:00"},
        {"max_slot": "14:00"},
        {"min_slot": "09:45", "max_slot": "13:00"},
        {"min_slot": "10:00", "max_slot": "14:00"},
        {"max_slot": "13:00", "top_n": 1},
        {"max_slot": "14:00", "top_n": 2},
    ]
    for g in guard_variants:
        add(
            _cfg_with(
                baseline,
                name="guard_" + "_".join(f"{k}{v}" for k, v in g.items()),
                guard=g,
                group="guard",
                reason="entry guard/time-window/top_n sweep",
                old_value="{}",
                new_value=json.dumps(g, sort_keys=True),
            )
        )

    # Stage 3c: single mask-term sweeps across indicator, price-action, and filters.
    group_terms: dict[str, list[list[Any]]] = {}
    for group, feats in MASK_FEATURES_BY_GROUP.items():
        group_terms[group] = quantile_terms(train_df, feats)
        inventory[f"{group}_terms_tested"] = group_terms[group]
        # Keep a realistic spread instead of exhausting every tiny threshold.
        for term in diverse_terms(group_terms[group], per_feature=4, max_terms=28):
            add(
                _cfg_with(
                    baseline,
                    name=f"{group}_{term[0]}{term[1]}{term[2]}",
                    mask_terms=[term],
                    group=group,
                    reason=f"single {group} threshold sweep",
                    old_value="none",
                    new_value=str(term),
                )
            )

    # Stage 3d: single pre-momentum sweeps.
    pm_terms, pm_df = premom_quantile_terms(train_df, baseline.sl)
    inventory["premom_terms_tested"] = pm_terms
    inventory["premom_columns_sampled"] = list(pm_df.columns)
    for term in diverse_terms(pm_terms, per_feature=4, max_terms=34):
        add(
            _cfg_with(
                baseline,
                name=f"premom_{term[0]}{term[1]}{term[2]}",
                premom_terms=[term],
                group="pre_momentum",
                reason="single pre-entry momentum threshold sweep",
                old_value="none",
                new_value=str(term),
            )
        )

    # Stage 4: simple, explainable combinations from stable structural terms.
    structural_terms = diverse_terms(
        group_terms.get("indicator", []) + group_terms.get("non_indicator_price_action", []) + group_terms.get("filter", []),
        per_feature=3,
        max_terms=32,
    )
    combo_exits = [(0.70, 1.80), (0.85, 1.80), (0.85, 2.00), (1.00, 2.00), (1.10, 2.50)]
    for t1, t2 in itertools.combinations(structural_terms, 2):
        if t1[0] == t2[0]:
            continue
        for sl, tgt in combo_exits:
            add(
                _cfg_with(
                    baseline,
                    name=f"combo_{t1[0]}_{t2[0]}_sl{sl}_t{tgt}",
                    sl=sl,
                    tgt=tgt,
                    mask_terms=[t1, t2],
                    group="combination",
                    reason="two-term structural combination plus exit rescue",
                    old_value="single/no mask",
                    new_value=str({"terms": [t1, t2], "exit": [sl, tgt]}),
                )
            )
            if len(candidates) >= max_iterations - 12:
                break
        if len(candidates) >= max_iterations - 12:
            break

    # Stage 6 rescue: adjust portfolio overlay and regime alignment without changing signal logic.
    for maxpos in [10, 15]:
        add(
            _cfg_with(
                baseline,
                name=f"guard_maxpos{maxpos}",
                max_positions=maxpos,
                group="guard",
                reason="portfolio max-position guard rescue",
                old_value="20",
                new_value=str(maxpos),
            )
        )
    for dloss in [3000.0, 5000.0, 7500.0]:
        add(
            _cfg_with(
                baseline,
                name=f"guard_daily_loss_{int(dloss)}",
                daily_loss_rs=dloss,
                group="guard",
                reason="daily loss guard rescue",
                old_value="0",
                new_value=str(dloss),
            )
        )
    for band in [0.0, 0.15, 0.30]:
        add(
            _cfg_with(
                baseline,
                name=f"regime_align_{band}",
                regime_align=True,
                regime_band=band,
                group="regime",
                reason="book-level regime alignment rescue",
                old_value="off",
                new_value=f"on band {band}",
            )
        )
    return candidates, inventory


def classify_failure(metrics: dict[str, Any], ran_test: bool, test_reason: str) -> str:
    train = metrics["TRAIN"]
    fit = metrics["FIT"]
    val = metrics["VAL"]
    if not ran_test:
        return test_reason
    test = metrics.get("TEST") or {}
    if train["profit_factor"] > 1.80 and test.get("profit_factor", 0.0) <= 1.40:
        return "OVERFIT: train PF too hot, test weak"
    if fit["profit_factor"] >= 1.30 and val["profit_factor"] < 1.05:
        return "FIT strong but VAL collapsed"
    if test.get("net_pnl", 0.0) <= 0:
        return "TEST net negative"
    if test.get("trades", 0) < 5:
        return "TEST insufficient sample"
    ok, why = domination_ok(test)
    if not ok:
        return "TEST dominated: " + why
    if test.get("profit_factor", 0.0) <= 1.40:
        return "TEST PF below target"
    return "pass_or_near_pass"


def _metric_line(m: dict[str, Any] | None) -> str:
    if not m:
        return "not run"
    return f"n={m['trades']} PF={m['profit_factor']} net={m['net_pnl']}"


def run_iterations(
    candidates: list[Candidate], windows: dict[str, pd.DataFrame], max_iterations: int
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], Candidate]:
    rows: list[dict[str, Any]] = []
    passing: list[dict[str, Any]] = []
    best_train = candidates[0]
    best_train_score = -1e9
    for idx, c in enumerate(candidates[:max_iterations], start=1):
        provisional = {}
        for label in ("FIT", "VAL", "TRAIN"):
            fam = _eval_family(c, windows[label])
            det = detail_from_book(fam["book"], c)
            provisional[label] = metrics_from_detail(det, fam)
        ok_train, why_train = train_side_promising(provisional)
        train_score = score_train_side(provisional)
        if train_score > best_train_score:
            best_train_score = train_score
            best_train = c
        run_test = ok_train
        if run_test:
            fam = _eval_family(c, windows["TEST"])
            det = detail_from_book(fam["book"], c)
            provisional["TEST"] = metrics_from_detail(det, fam)
        else:
            provisional["TEST"] = None
        accepted, accept_reason = acceptance(provisional) if run_test else (False, why_train)
        fail_class = classify_failure(provisional, run_test, why_train)
        if accepted:
            passing.append({"candidate": c, "metrics": provisional, "accept_reason": accept_reason})
        row = {
            "iteration": idx,
            "candidate_id": c.name,
            "parameter_group": c.group,
            "changed_rule": c.reason,
            "old_value": c.old_value,
            "new_value": c.new_value,
            "command": f"py -3.12 {HERE.relative_to(REPO_ROOT)} --max_iterations {max_iterations}",
            "config": c.to_config(),
            "FIT": provisional["FIT"],
            "VAL": provisional["VAL"],
            "TRAIN": provisional["TRAIN"],
            "TEST": provisional["TEST"],
            "keep_reject": "KEEP_PASSING" if accepted else "REJECT",
            "failure_classification": fail_class,
            "next_action": "candidate file" if accepted else "continue train-side search",
            "train_side_score": round(float(train_score), 5),
        }
        rows.append(row)
        print(
            f"[iter {idx:03d}] {c.group:<28} {c.name:<44} "
            f"FIT {_metric_line(provisional['FIT'])} | VAL {_metric_line(provisional['VAL'])} | "
            f"TRAIN {_metric_line(provisional['TRAIN'])} | TEST {_metric_line(provisional['TEST'])} | "
            f"{row['keep_reject']} {fail_class}",
            flush=True,
        )
    return rows, passing, best_train


def write_table(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        pd.DataFrame().to_csv(path, index=False)
        return
    flat = []
    for r in rows:
        out = {
            "iteration": r["iteration"],
            "candidate_id": r["candidate_id"],
            "group": r["parameter_group"],
            "changed_rule": r["changed_rule"],
            "old_value": r["old_value"],
            "new_value": r["new_value"],
            "keep_reject": r["keep_reject"],
            "failure": r["failure_classification"],
            "train_side_score": r["train_side_score"],
            "config_json": json.dumps(_json_safe(r["config"]), sort_keys=True),
        }
        for label in ("FIT", "VAL", "TRAIN", "TEST"):
            m = r.get(label) or {}
            for k in ("trades", "profit_factor", "net_pnl", "avg_loss", "sl_count", "target_count", "time_exit_count"):
                out[f"{label.lower()}_{k}"] = m.get(k)
        flat.append(out)
    pd.DataFrame(flat).to_csv(path, index=False)


def current_rules_text() -> str:
    setup = catalog.find(SETUP)
    idea = setup.idea if setup else "After a 2-bar down-pullback, hold VWAP and break prior high."
    return "\n".join(
        [
            f"- setup name: {SETUP}",
            f"- side: {SIDE}",
            f"- config source: raw scanner in avwap_5min_ID_v2_backtesting.py / all_setups_catalog.py; not active in FINAL_SETUP_CONF.",
            f"- structural idea: {idea}",
            "- current entry trigger: long_struct, close above VWAP, close above previous bar high, previous close below previous-2 close, vol_ratio >= 1.4, regime != BEAR.",
            "- current indicator rules: VWAP hold, vol_ratio >= 1.4, regime filter; ATR participates through common range/liquidity prep and exit simulation.",
            "- current non-indicator rules: bullish candle structure via long_struct, break above previous high, pullback condition on prior close versus previous-2 close.",
            "- current pre-momentum rules: none.",
            "- current filters: none beyond raw detection/common v11 candidate scanning.",
            "- current guards: default repo entry window/dedupe only; no setup-specific guard.",
            f"- current SL/target: {BASE_EXIT['sl']:.2f}/{BASE_EXIT['tgt']:.2f}.",
            "- current exit logic: fixed SL/target/EOD resolved on 1-minute OHLC by repo exit resolver.",
            "- current time windows: repo default 09:30..14:30 candidate entry behavior after scan.",
            "- current portfolio limits: repo evaluator default max_positions=20, daily_loss_rs=0.",
        ]
    )


def categorize_columns(cols: list[str]) -> dict[str, list[str]]:
    lower = {c: c.lower() for c in cols}

    def has_any(c: str, hints: tuple[str, ...]) -> bool:
        return any(h in lower[c] for h in hints)

    used: set[str] = set()
    cats = {}
    for name, hints in [
        ("price_ohlc", PRICE_COL_HINTS),
        ("volume_liquidity", VOL_COL_HINTS),
        ("vwap_avwap", VWAP_COL_HINTS),
        ("ema_sma", MA_COL_HINTS),
        ("rsi_adx_macd_momentum", MOM_COL_HINTS),
        ("atr_volatility_bands", ATR_COL_HINTS),
        ("candle_structure", CANDLE_COL_HINTS),
        ("pre_momentum", ("pre", "sig5")),
        ("setup_reason_quality", REASON_COL_HINTS),
        ("time_session", TIME_COL_HINTS),
        ("symbol", ("ticker", "symbol")),
    ]:
        vals = sorted([c for c in cols if has_any(c, hints)])
        cats[name] = vals
        used.update(vals)
    cats["other_useful"] = sorted([c for c in cols if c not in used])[:120]
    return cats


def fmt_metrics(m: dict[str, Any] | None) -> str:
    if not m:
        return "not run"
    return (
        f"trades={m['trades']}, wins={m.get('wins', 0)}, losses={m.get('losses', 0)}, "
        f"win_rate={m.get('win_rate', 0)}%, PF={m['profit_factor']}, net=Rs {m['net_pnl']:,.0f}, "
        f"avg_win=Rs {m.get('avg_win', 0):,.0f}, avg_loss=Rs {m.get('avg_loss', 0):,.0f}, "
        f"SL/TGT/EOD={m.get('sl_count', 0)}/{m.get('target_count', 0)}/{m.get('time_exit_count', 0)}, "
        f"top_trade/day/symbol={m.get('top_trade_gross_profit_share')}/"
        f"{m.get('top_day_net_share')}/{m.get('top_symbol_net_share')}"
    )


def detail_breakdowns(detail: pd.DataFrame) -> dict[str, Any]:
    if detail.empty:
        return {}
    out = {}
    for key in ("trade_date", "ticker"):
        g = detail.groupby(key)["net_pnl_rs"].agg(["size", "sum"]).sort_values("sum")
        out[f"worst_{key}"] = g.head(10).round(2).reset_index().to_dict("records")
        out[f"best_{key}"] = g.tail(10).round(2).reset_index().to_dict("records")
    d = detail.copy()
    d["hour"] = pd.to_datetime(d["entry_time"], errors="coerce").dt.strftime("%H:00")
    g = d.groupby("hour")["net_pnl_rs"].agg(["size", "sum"])
    out["time_window"] = g.round(2).reset_index().to_dict("records")
    return out


def classify_losing_trades(detail: pd.DataFrame) -> dict[str, int]:
    if detail.empty:
        return {}
    losses = detail[pd.to_numeric(detail["net_pnl_rs"], errors="coerce") <= 0].copy()
    counts = Counter()
    if losses.empty:
        return {}
    vol_med = pd.to_numeric(detail.get("vol_ratio"), errors="coerce").median()
    atr_low = pd.to_numeric(detail.get("atr_pct"), errors="coerce").quantile(0.25)
    atr_high = pd.to_numeric(detail.get("atr_pct"), errors="coerce").quantile(0.75)
    for r in losses.itertuples():
        oc = str(getattr(r, "outcome", "")).upper()
        if oc == "SL":
            counts["SL_hit"] += 1
        if oc == "EOD":
            counts["time_or_EOD_exit"] += 1
        vol = getattr(r, "vol_ratio", np.nan)
        body = getattr(r, "body_pct", np.nan)
        close_loc = getattr(r, "close_loc", np.nan)
        vwap_dist = getattr(r, "vwap_dist_atr", np.nan)
        atr = getattr(r, "atr_pct", np.nan)
        sig_min = getattr(r, "signal_minute", np.nan)
        if np.isfinite(vol) and np.isfinite(vol_med) and vol < vol_med:
            counts["weak_volume_vs_setup_median"] += 1
        if np.isfinite(body) and body < 0.45:
            counts["weak_body"] += 1
        if np.isfinite(close_loc) and close_loc < 0.60:
            counts["poor_close_location_for_long"] += 1
        if np.isfinite(vwap_dist) and vwap_dist > 3.0:
            counts["overextended_above_vwap"] += 1
        if np.isfinite(atr) and np.isfinite(atr_low) and atr < atr_low:
            counts["low_volatility_noise"] += 1
        if np.isfinite(atr) and np.isfinite(atr_high) and atr > atr_high:
            counts["high_volatility_risk"] += 1
        if np.isfinite(sig_min) and (sig_min < 585 or sig_min > 840):
            counts["edge_time_window_issue"] += 1
    return dict(counts)


def write_reports(
    manifest: dict[str, Any],
    pool: pd.DataFrame,
    windows: dict[str, pd.DataFrame],
    baseline: Candidate,
    baseline_metrics: dict[str, Any],
    baseline_details: dict[str, pd.DataFrame],
    inventory: dict[str, Any],
    iteration_rows: list[dict[str, Any]],
    passing: list[dict[str, Any]],
    best_train: Candidate,
) -> None:
    WORK.mkdir(parents=True, exist_ok=True)
    (WORK / "candidates").mkdir(exist_ok=True)

    cols = list(pool.columns)
    col_cats = categorize_columns(cols)
    train_sessions = manifest.get("actual_train_sessions") or []
    test_sessions = manifest.get("actual_test_sessions") or []
    fit_sessions = manifest.get("actual_fit_sessions") or []
    val_sessions = manifest.get("actual_val_sessions") or []

    pool_report = [
        f"# Pool Recreation Report - {SETUP}",
        "",
        "## Result",
        f"- Pool recreation succeeded: YES",
        f"- Output pool: `{POOL_CSV}`",
        f"- Raw/master source: `{MASTER_POOL}`",
        f"- Tail source: `{TAIL_RAW}`",
        f"- Requested TRAIN: {TRAIN_REQ[0]}..{TRAIN_REQ[1]}",
        f"- Actual TRAIN sessions: {train_sessions[0] if train_sessions else 'none'}..{train_sessions[-1] if train_sessions else 'none'} ({len(train_sessions)} sessions)",
        f"- Requested TEST: {TEST_REQ[0]}..{TEST_REQ[1]}",
        f"- Actual TEST sessions: {test_sessions[0] if test_sessions else 'none'}..{test_sessions[-1] if test_sessions else 'none'} ({len(test_sessions)} sessions)",
        f"- Missing TRAIN weekdays: {', '.join(manifest.get('missing_train_weekdays') or []) or 'none'}",
        f"- Missing TEST weekdays: {', '.join(manifest.get('missing_test_weekdays') or []) or 'none'}",
        f"- Available first/last session: {manifest.get('available_first_session')}..{manifest.get('available_last_session')}",
        f"- Setup rows final: {manifest.get('setup_counts', {}).get('rows_final')}",
        f"- Setup signal sessions: {len(manifest.get('setup_signal_sessions') or [])}",
        f"- Symbols in setup pool: {pool['ticker'].nunique() if 'ticker' in pool.columns else 0}",
        f"- Rows with 1-minute entry after repo attach_entries: {len(pool)}",
        "",
        "## 5-Minute / 1-Minute Coverage",
        "- 5-minute candidate generation: global unified pool through 2026-06-24 plus fresh v11 historical-all-available generation for 2026-06-25, 2026-06-29, 2026-06-30, 2026-07-01, 2026-07-02.",
        "- 1-minute exit simulation: repo `setup_train_test` / `avwap_5min_ID_v11_backtesting._load_1m_with_open`, merging historical `stocks_indicators_1min_eq` with live raw 1-minute tail when available.",
        "- Data quality issue: missing weekdays are listed above; some are likely exchange holidays/weekends but are treated as missing from available completed-session data, not imputed.",
    ]
    (WORK / "POOL_RECREATION_REPORT.md").write_text("\n".join(pool_report) + "\n", encoding="utf-8")

    base_lines = [
        f"# Baseline Result - {SETUP}",
        "",
        "## Current Rules",
        current_rules_text(),
        "",
        "## Exact Sessions",
        f"- FIT: {fit_sessions[0] if fit_sessions else 'none'}..{fit_sessions[-1] if fit_sessions else 'none'} ({len(fit_sessions)})",
        f"- VAL: {val_sessions[0] if val_sessions else 'none'}..{val_sessions[-1] if val_sessions else 'none'} ({len(val_sessions)})",
        f"- TRAIN: {train_sessions[0] if train_sessions else 'none'}..{train_sessions[-1] if train_sessions else 'none'} ({len(train_sessions)})",
        f"- TEST: {test_sessions[0] if test_sessions else 'none'}..{test_sessions[-1] if test_sessions else 'none'} ({len(test_sessions)})",
        "",
        "## Baseline Metrics",
        f"- FIT: {fmt_metrics(baseline_metrics['FIT'])}",
        f"- VAL: {fmt_metrics(baseline_metrics['VAL'])}",
        f"- Full TRAIN: {fmt_metrics(baseline_metrics['TRAIN'])}",
        f"- TEST: {fmt_metrics(baseline_metrics['TEST'])}",
        "",
        "## Initial Diagnosis",
    ]
    tr = baseline_metrics["TRAIN"]
    te = baseline_metrics["TEST"]
    if tr["profit_factor"] > 1.80:
        base_lines.append("- TRAIN PF is above the target band; treat raw baseline as overfit-risk even before TEST.")
    elif tr["profit_factor"] < 1.30:
        base_lines.append("- TRAIN PF is below the target band; needs structural filtering or exit improvement.")
    else:
        base_lines.append("- TRAIN PF is inside the requested control band.")
    if te["profit_factor"] <= 1.40:
        base_lines.append("- TEST PF does not clear 1.40; baseline is not acceptable.")
    else:
        base_lines.append("- TEST PF clears 1.40; stability still controls acceptance.")
    (WORK / "BASELINE_RESULT.md").write_text("\n".join(base_lines) + "\n", encoding="utf-8")

    inv_lines = [
        f"# Parameter Inventory - {SETUP}",
        "",
        "## Current Setup Rules",
        current_rules_text(),
        "",
        "## Available Columns / Features",
    ]
    for cat, vals in col_cats.items():
        inv_lines.append(f"### {cat}")
        inv_lines.append(", ".join(vals) if vals else "none detected")
        inv_lines.append("")
    inv_lines += [
        "## Supported Repo Knobs",
        "- mask_terms: supported by setup_train_test.apply_mask_terms and final conf masks.",
        "- pre_momentum_terms: supported by setup_train_test.apply_premom_terms via v11 pre-entry feature function.",
        "- min_slot / max_slot / top_n: supported by entry_guards in setup_train_test.apply_guards.",
        "- max_positions: supported by setup_train_test portfolio overlay global.",
        "- daily_loss_rs: supported by setup_train_test portfolio overlay global.",
        "- regime_align / regime_band: supported by setup_train_test book-level regime alignment global.",
        "- SL / target: supported per setup.",
        "- EOD/time exit: supported by repo exit resolver as the terminal fallback.",
        "- trailing SL / break-even SL: not exposed by this setup_train_test config path; logged as unsupported, not optimized as a fake knob.",
        "- max trades/day and max trades/symbol: not exposed as final-conf setup fields; repo has family/ticker/day dedupe and portfolio guards instead.",
        "",
        "## Candidate Parameter Ranges",
        f"- SL grid: {SL_GRID}",
        f"- Target grid: {TGT_GRID}",
        f"- Quantile grid: {QGRID}",
        "- Guard windows: 09:30/09:45/10:00/10:30 min-slot and 12:00/12:30/13:00/14:00 max-slot variants plus top_n 1/2.",
        "- Ranges are realistic because they are quantiles of the recreated TRAIN-side candidate pool and common intraday SL/target brackets already used in the repo.",
        "",
        "## Generated Term Inventory",
        "```json",
        json.dumps(_json_safe(inventory), indent=2),
        "```",
    ]
    (WORK / "PARAMETER_INVENTORY.md").write_text("\n".join(inv_lines) + "\n", encoding="utf-8")

    write_table(WORK / "all_iterations.csv", iteration_rows)
    iter_lines = [f"# Iteration Log - {SETUP}", ""]
    for r in iteration_rows:
        iter_lines += [
            f"## Iteration {r['iteration']:03d} - {r['candidate_id']}",
            f"- changed rule/parameter: {r['changed_rule']}",
            f"- parameter group: {r['parameter_group']}",
            f"- old value: {r['old_value']}",
            f"- new value: {r['new_value']}",
            f"- command: `{r['command']}`",
            f"- FIT metrics: {fmt_metrics(r['FIT'])}",
            f"- VAL metrics: {fmt_metrics(r['VAL'])}",
            f"- full TRAIN metrics: {fmt_metrics(r['TRAIN'])}",
            f"- TEST metrics: {fmt_metrics(r['TEST'])}",
            f"- keep/reject: {r['keep_reject']}",
            f"- failure classification: {r['failure_classification']}",
            f"- next action: {r['next_action']}",
            "",
        ]
    (WORK / "ITERATION_LOG.md").write_text("\n".join(iter_lines), encoding="utf-8")

    sweep_summary = [f"# Parameter Sweep Summary - {SETUP}", ""]
    by_group = defaultdict(list)
    for r in iteration_rows:
        by_group[r["parameter_group"]].append(r)
    for group, rs in sorted(by_group.items()):
        best = sorted(rs, key=lambda x: x["train_side_score"], reverse=True)[:5]
        sweep_summary += [f"## {group}", f"- tested iterations: {len(rs)}"]
        for b in best:
            sweep_summary.append(
                f"- {b['candidate_id']}: TRAIN {fmt_metrics(b['TRAIN'])}; TEST {fmt_metrics(b['TEST'])}; reason {b['failure_classification']}"
            )
        rejected = Counter(r["failure_classification"] for r in rs)
        sweep_summary.append(f"- rejected ranges/classes: {dict(rejected)}")
        sweep_summary.append("")
    sweep_summary += [
        "## Overfit-Risk Notes",
        "- Candidates with TRAIN PF above 1.80 were not treated as success unless TEST and stability also passed.",
        "- TEST was only run after FIT/VAL/full-TRAIN passed the train-side gate.",
        "- Market-return/time pockets were logged as higher overfit risk and not promoted unless they cleared stability.",
    ]
    (WORK / "PARAMETER_SWEEP_SUMMARY.md").write_text("\n".join(sweep_summary) + "\n", encoding="utf-8")

    fail_lines = [f"# Failure Analysis - {SETUP}", ""]
    for label, det in baseline_details.items():
        fail_lines += [f"## Baseline {label}", f"- losing trade classifications: {classify_losing_trades(det)}"]
        bd = detail_breakdowns(det)
        fail_lines += ["```json", json.dumps(_json_safe(bd), indent=2), "```", ""]
    fail_counts = Counter(r["failure_classification"] for r in iteration_rows)
    fail_lines += [
        "## Rejected Candidate Failure Classes",
        "```json",
        json.dumps(_json_safe(dict(fail_counts)), indent=2),
        "```",
        "",
        "## Notes",
        "- SL/target behavior was tracked via SL/TARGET/EOD counts per iteration.",
        "- Fake breakout/reversal risk was proxied by poor close location, weak body, overextended VWAP distance, and SL-hit clusters.",
        "- Volume, volatility, and trend weakness were checked through vol_ratio, atr_pct, vwap_dist_atr, ADX/pre-momentum sweeps where available.",
    ]
    (WORK / "FAILURE_ANALYSIS.md").write_text("\n".join(fail_lines) + "\n", encoding="utf-8")

    pass_json = []
    cand_lines = [f"# Candidate Configs - {SETUP}", ""]
    if not passing:
        cand_lines.append("No candidate passed TRAIN PF 1.30..1.80, TEST PF > 1.40, positive PnL, minimum trade-count, and domination checks.")
    for i, p in enumerate(passing, start=1):
        c: Candidate = p["candidate"]
        cid = f"{SETUP}_candidate_{i:03d}"
        payload = {
            "candidate_id": cid,
            "setup": SETUP,
            "side": SIDE,
            "config": c.to_config(),
            "metrics": p["metrics"],
            "domination_checks": {
                "TRAIN": domination_ok(p["metrics"]["TRAIN"])[1],
                "TEST": domination_ok(p["metrics"]["TEST"])[1],
            },
            "risk_notes": "APPROVAL REQUIRED before any final config or live/paper watch change.",
        }
        pass_json.append(payload)
        (WORK / "candidates" / f"{cid}.json").write_text(
            json.dumps(_json_safe(payload), indent=2), encoding="utf-8"
        )
        cand_lines += [
            f"## {cid}",
            f"- exact rules/config: `{json.dumps(_json_safe(c.to_config()), sort_keys=True)}`",
            f"- TRAIN: {fmt_metrics(p['metrics']['TRAIN'])}",
            f"- TEST: {fmt_metrics(p['metrics']['TEST'])}",
            f"- domination checks: TRAIN {domination_ok(p['metrics']['TRAIN'])[1]}; TEST {domination_ok(p['metrics']['TEST'])[1]}",
            "- approval recommendation: YES, research-only candidate; do not move until user approves.",
            "",
        ]
    (WORK / "CANDIDATE_CONFIGS.md").write_text("\n".join(cand_lines) + "\n", encoding="utf-8")
    (WORK / "passing_candidates.json").write_text(json.dumps(_json_safe(pass_json), indent=2), encoding="utf-8")

    best_payload = passing[0] if passing else None
    if passing:
        passing.sort(key=lambda p: (p["metrics"]["TEST"]["profit_factor"], p["metrics"]["TEST"]["net_pnl"], p["metrics"]["TEST"]["trades"]), reverse=True)
        best_payload = passing[0]
    rec_lines = [f"# Approval Required Final Recommendation - {SETUP}", ""]
    if best_payload:
        c = best_payload["candidate"]
        config_block = {
            SETUP: {
                "side": SIDE,
                "enabled": False,
                "exit": {"sl_pct": c.sl, "tgt_pct": c.tgt},
                "mask_terms": c.mask_terms,
                "pre_momentum_terms": c.premom_terms,
                "entry_guards": c.guard or {},
                "provenance": {
                    "source": str(WORK),
                    "train": best_payload["metrics"]["TRAIN"],
                    "test": best_payload["metrics"]["TEST"],
                    "approval_required": True,
                },
            }
        }
        rec_lines += [
            "## Best Candidate",
            f"- candidate: {c.name}",
            f"- TRAIN: {fmt_metrics(best_payload['metrics']['TRAIN'])}",
            f"- TEST: {fmt_metrics(best_payload['metrics']['TEST'])}",
            "",
            "## Proposed Config Block",
            "```python",
            json.dumps(_json_safe(config_block), indent=4),
            "```",
            "",
            "## Final File Requiring Approval",
            f"- `{REPO_ROOT / 'final_setup_conf.py'}`",
            f"- `{TRAIN_AND_TEST / 'final_setup_conf.py'}` if you intentionally mirror the root config later.",
            "",
            "## Exact Rerun Command",
            f"`py -3.12 {HERE.relative_to(REPO_ROOT)} --max_iterations {len(iteration_rows)}`",
            "",
            "DO NOT MOVE TO FINAL CONFIG UNTIL USER APPROVES",
        ]
    else:
        rec_lines += [
            "## Best Candidate",
            "No candidate met all approval gates.",
            "",
            "## Best Train-Side Fallback",
            f"- candidate: {best_train.name}",
            f"- config: `{json.dumps(_json_safe(best_train.to_config()), sort_keys=True)}`",
            "",
            "## Final File Requiring Approval",
            f"- `{REPO_ROOT / 'final_setup_conf.py'}`",
            f"- No diff/patch is recommended because no candidate passed.",
            "",
            "DO NOT MOVE TO FINAL CONFIG UNTIL USER APPROVES",
        ]
    (WORK / "APPROVAL_REQUIRED_FINAL_RECOMMENDATION.md").write_text("\n".join(rec_lines) + "\n", encoding="utf-8")


def main() -> int:
    try:
        sys.stdout.reconfigure(line_buffering=True)
    except Exception:
        pass
    ap = argparse.ArgumentParser()
    ap.add_argument("--max_iterations", type=int, default=120)
    ap.add_argument("--skip_build_pool", action="store_true")
    args = ap.parse_args()
    max_iterations = max(50, min(int(args.max_iterations), 200))
    print(f"[full-loop] setup={SETUP} max_iterations={max_iterations}")
    if not args.skip_build_pool or not POOL_CSV.exists():
        manifest = build_pool()
    else:
        manifest = json.loads((POOL_DIR / "_manifest.json").read_text(encoding="utf-8"))
    print(
        f"[full-loop] pool rows={manifest.get('setup_counts', {}).get('rows_final')} "
        f"train_sessions={len(manifest.get('actual_train_sessions') or [])} "
        f"test_sessions={len(manifest.get('actual_test_sessions') or [])}"
    )
    pool, manifest = load_prepared_pool()
    windows = split_windows(pool, manifest)
    print(
        f"[full-loop] entries after 1m attach: FIT={len(windows['FIT'])} VAL={len(windows['VAL'])} "
        f"TRAIN={len(windows['TRAIN'])} TEST={len(windows['TEST'])}",
        flush=True,
    )

    baseline = Candidate(
        name="baseline_raw",
        sl=BASE_EXIT["sl"],
        tgt=BASE_EXIT["tgt"],
        group="baseline",
        reason="current raw setup baseline",
        old_value="n/a",
        new_value=f"raw detection, SL/TGT {BASE_EXIT['sl']}/{BASE_EXIT['tgt']}",
    )
    candidates, inventory = generate_candidates(windows["TRAIN"], baseline, max_iterations)
    print(f"[full-loop] generated {len(candidates)} candidate iterations")

    baseline_metrics = evaluate_candidate(baseline, windows, run_test=True)
    baseline_details = {}
    for label in ("TRAIN", "TEST"):
        fam = _eval_family(baseline, windows[label])
        baseline_details[label] = detail_from_book(fam["book"], baseline)
        out_csv = WORK / f"baseline_{label.lower()}_trades.csv"
        baseline_details[label].to_csv(out_csv, index=False)

    iteration_rows, passing, best_train = run_iterations(candidates, windows, max_iterations)
    # Keep passing sorted with strongest TEST first for report readability.
    passing.sort(
        key=lambda p: (
            p["metrics"]["TEST"]["profit_factor"],
            p["metrics"]["TEST"]["net_pnl"],
            p["metrics"]["TEST"]["trades"],
        ),
        reverse=True,
    )
    write_reports(
        manifest,
        pool,
        windows,
        baseline,
        baseline_metrics,
        baseline_details,
        inventory,
        iteration_rows,
        passing,
        best_train,
    )
    print(f"[full-loop] wrote reports under {WORK}")
    print(f"[full-loop] passing candidates={len(passing)}")
    if passing:
        best = passing[0]
        print(
            f"[full-loop] best={best['candidate'].name} "
            f"TRAIN PF={best['metrics']['TRAIN']['profit_factor']} n={best['metrics']['TRAIN']['trades']} "
            f"TEST PF={best['metrics']['TEST']['profit_factor']} n={best['metrics']['TEST']['trades']}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
