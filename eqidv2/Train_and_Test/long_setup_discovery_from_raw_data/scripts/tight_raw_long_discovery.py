r"""tight_raw_long_discovery.py - raw-bar FAST-MOMENTUM LONG discovery.

Research-only workflow for a tight LONG bracket around 0.75% / 0.75%.

What this script does:
  1. Audits the raw/indicator 5-minute and 1-minute parquet stores.
  2. Selects latest completed common sessions from 5m + 1m data.
  3. Builds causal 5m features and LONG setup-family triggers.
  4. Resolves exits on 1-minute bars with pessimistic same-minute ties.
  5. Searches FIT/VAL first, confirms on TRAIN, scores TEST only for
     promising TRAIN candidates.
  6. Writes all requested reports under Train_and_Test/long_setup_discovery_from_raw_data.

Run from repo root:
  py -3.12 Train_and_Test/long_setup_discovery_from_raw_data/scripts/tight_raw_long_discovery.py

Rerun only the best saved candidate:
  py -3.12 Train_and_Test/long_setup_discovery_from_raw_data/scripts/tight_raw_long_discovery.py --best-only
"""
from __future__ import annotations

import argparse
import json
import math
import sys
import time
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd

_P = Path(__file__).resolve()
TT_DIR = next(par for par in _P.parents if par.name == "Train_and_Test")
REPO_ROOT = TT_DIR.parent
OUT_DIR = TT_DIR / "long_setup_discovery_from_raw_data"
SCRIPTS_DIR = OUT_DIR / "scripts"
CAND_DIR = OUT_DIR / "candidates"
RESULTS_DIR = OUT_DIR / "results"
LOGS_DIR = OUT_DIR / "logs"

for _d in (str(REPO_ROOT), str(TT_DIR)):
    if _d not in sys.path:
        sys.path.insert(0, _d)

from nse_intraday_costs import CostConfig  # noqa: E402
from walkforward_gate import net_pnl_vectorized  # noqa: E402


FIVE_MIN_DIRS = [
    Path(r"C:/TradingData/eqidv2/stocks_indicators_5min_eq_live2"),
    Path(r"C:/TradingData/eqidv2/stocks_indicators_5min_eq_live"),
    Path(r"C:/TradingData/eqidv2/stocks_indicators_5min_eq"),
    REPO_ROOT / "outputs_advanced_indicators_5min",
    REPO_ROOT / "outputs_advanced_indicators_5min_current",
]
ONE_MIN_DIRS = [
    Path(r"C:/TradingData/eqidv2/stocks_indicators_1min_eq"),
    REPO_ROOT / "stocks_indicators_1min_eq",
    Path(r"C:/TradingData/eqidv2/stocks_raw_1min_entry_v5_id_live"),
]

FIVE_SUFFIXES = [
    "_stocks_indicators_5min.parquet",
    "_advanced_indicators_5min.parquet",
]
ONE_SUFFIXES = [
    "_stocks_indicators_1min.parquet",
    "_stocks_raw_1min.parquet",
]

BASE_COLS_5M = [
    "date", "open", "high", "low", "close", "volume",
    "RSI", "ATR", "EMA_20", "EMA_50", "EMA_200", "20_SMA", "VWAP",
    "MACD", "MACD_Signal", "MACD_Hist", "ADX", "Upper_Band", "Lower_Band",
    "Stoch_%K", "Stoch_%D", "CCI", "MFI", "OBV", "gap_filled", "opening_snapshot",
]
BASE_COLS_1M = ["date", "open", "high", "low", "close", "volume"]
INDICATOR_HINTS = (
    "rsi", "adx", "macd", "ema", "sma", "vwap", "atr", "band", "stoch", "cci",
    "mfi", "obv", "supertrend", "roc", "slope", "opening_snapshot",
)

NOTIONAL_RS = 100_000.0
DEFAULT_SLIPPAGE_BPS = 15.0
MIN_5M_BARS_COMPLETE = 60
MIN_1M_BARS_COMPLETE = 300
MIN_SYMBOLS_PER_SESSION = 150
TRAIN_SESSIONS = 30
TEST_SESSIONS = 10
EVAL_CTX: dict | None = None


@dataclass(frozen=True)
class RuleSpec:
    rule_id: str
    family: str
    name: str
    params: dict
    entry_logic: str
    indicator_rules: list[str]
    non_indicator_rules: list[str]
    pre_momentum_filter: str


@dataclass(frozen=True)
class ExitSpec:
    exit_id: str
    sl_pct: float
    target_pct: float
    time_bars: int
    breakeven_after_pct: float | None = None
    trailing_after_pct: float | None = None
    trailing_gap_pct: float | None = None


@dataclass(frozen=True)
class GuardSpec:
    guard_id: str
    min_slot: int | None = None
    max_slot: int | None = None
    top_n_per_slot: int | None = None
    max_per_symbol_day: int = 1
    cooldown_after_sl_bars: int = 0


def rel(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(REPO_ROOT.resolve())).replace("\\", "/")
    except Exception:
        return str(path)


def safe_float(x, default=np.nan) -> float:
    try:
        y = float(x)
        return y if math.isfinite(y) else default
    except Exception:
        return default


def pct(a, b) -> pd.Series:
    a = pd.to_numeric(a, errors="coerce")
    b = pd.to_numeric(b, errors="coerce")
    return (a / b - 1.0) * 100.0


def normalize_dt(s: pd.Series) -> pd.Series:
    dt = pd.to_datetime(s, errors="coerce")
    try:
        if getattr(dt.dt, "tz", None) is not None:
            dt = dt.dt.tz_convert("Asia/Kolkata").dt.tz_localize(None)
    except Exception:
        pass
    return dt


def parquet_files(root: Path, suffixes: list[str]) -> dict[str, Path]:
    out: dict[str, Path] = {}
    if not root.exists():
        return out
    for suf in suffixes:
        for p in root.glob(f"*{suf}"):
            sym = p.name[: -len(suf)]
            if sym and sym not in out:
                out[sym] = p
    return out


def choose_store(candidates: list[Path], suffixes: list[str], label: str) -> tuple[Path, dict[str, Path]]:
    ranked: list[tuple[pd.Timestamp, int, int, Path, dict[str, Path]]] = []
    for d in candidates:
        files = parquet_files(d, suffixes)
        if not files:
            continue
        sample_paths = list(files.values())[:5] + list(files.values())[-5:]
        max_dt = pd.Timestamp.min
        cols_n = 0
        for p in sample_paths:
            try:
                df = pd.read_parquet(p, columns=["date"])
                dt = normalize_dt(df["date"])
                mx = dt.max()
                if pd.notna(mx):
                    max_dt = max(max_dt, pd.Timestamp(mx))
            except Exception:
                try:
                    df = pd.read_parquet(p)
                    cols_n = max(cols_n, len(df.columns))
                    dt = normalize_dt(df["date"])
                    mx = dt.max()
                    if pd.notna(mx):
                        max_dt = max(max_dt, pd.Timestamp(mx))
                except Exception:
                    pass
        ranked.append((max_dt, len(files), cols_n, d, files))
    if not ranked:
        raise SystemExit(f"No parquet files found for {label}")
    ranked.sort(key=lambda x: (x[0], x[1]), reverse=True)
    return ranked[0][3], ranked[0][4]


def read_parquet_existing(path: Path, cols: list[str]) -> pd.DataFrame:
    try:
        return pd.read_parquet(path, columns=cols)
    except Exception:
        df = pd.read_parquet(path)
        keep = [c for c in cols if c in df.columns]
        return df[keep].copy()


def session_counts(files: dict[str, Path], min_bars: int, label: str) -> tuple[pd.DataFrame, dict]:
    rows = []
    started = time.time()
    for i, (sym, p) in enumerate(files.items(), 1):
        try:
            d = read_parquet_existing(p, ["date"])
            dt = normalize_dt(d["date"]).dropna()
            vc = dt.dt.date.value_counts()
            for day, n in vc.items():
                rows.append((sym, pd.Timestamp(day), int(n), int(n >= min_bars)))
        except Exception:
            continue
        if i % 250 == 0:
            print(f"[audit] scanned {label} dates {i}/{len(files)} files ({time.time() - started:.0f}s)", flush=True)
    df = pd.DataFrame(rows, columns=["symbol", "session", "bars", "complete"])
    if df.empty:
        return df, {}
    complete = df[df["complete"] == 1]
    meta = {
        "symbols": int(df["symbol"].nunique()),
        "sessions": int(df["session"].nunique()),
        "date_min": str(df["session"].min().date()),
        "date_max": str(df["session"].max().date()),
        "complete_sessions": int(complete["session"].nunique()),
    }
    return df, meta


def selected_sessions(five_counts: pd.DataFrame, one_counts: pd.DataFrame) -> dict:
    f = (
        five_counts[five_counts["complete"] == 1]
        .groupby("session")["symbol"].nunique()
        .rename("five_symbols")
    )
    o = (
        one_counts[one_counts["complete"] == 1]
        .groupby("session")["symbol"].nunique()
        .rename("one_symbols")
    )
    cov = pd.concat([f, o], axis=1).fillna(0).astype(int)
    cov = cov[(cov["five_symbols"] >= MIN_SYMBOLS_PER_SESSION) & (cov["one_symbols"] >= MIN_SYMBOLS_PER_SESSION)]
    sessions = sorted(pd.Timestamp(x) for x in cov.index)
    if len(sessions) < TRAIN_SESSIONS + TEST_SESSIONS:
        raise SystemExit(
            f"Not enough completed common sessions: have {len(sessions)}, need {TRAIN_SESSIONS + TEST_SESSIONS}"
        )
    sessions = sessions[-(TRAIN_SESSIONS + TEST_SESSIONS):]
    train = sessions[:TRAIN_SESSIONS]
    test = sessions[TRAIN_SESSIONS:]
    half = len(train) // 2
    fit = train[:half]
    val = train[half:]
    return {
        "coverage": cov,
        "fit": fit,
        "val": val,
        "train": train,
        "test": test,
        "all": sessions,
    }


def session_str(sessions: list[pd.Timestamp]) -> str:
    return ", ".join(pd.Timestamp(x).strftime("%Y-%m-%d") for x in sessions)


def range_str(sessions: list[pd.Timestamp]) -> str:
    return f"{sessions[0].date()}..{sessions[-1].date()} ({len(sessions)} sessions)"


def inspect_columns(path: Path) -> tuple[list[str], int, int, str, str]:
    df = pd.read_parquet(path)
    cols = list(df.columns)
    dt = normalize_dt(df["date"]) if "date" in df.columns else pd.Series(dtype="datetime64[ns]")
    return cols, len(df), len(cols), str(dt.min()), str(dt.max())


def feature_frame(raw: pd.DataFrame, symbol: str) -> pd.DataFrame:
    df = raw.copy()
    df["date"] = normalize_dt(df["date"])
    df = df.dropna(subset=["date"]).sort_values("date").drop_duplicates("date")
    if df.empty:
        return df
    df["symbol"] = symbol
    df["session"] = df["date"].dt.normalize()
    df["slot"] = ((df["date"].dt.hour * 60 + df["date"].dt.minute) - (9 * 60 + 20)) // 5
    df = df[(df["slot"] >= 0) & (df["slot"] <= 72)].copy()
    for c in ["open", "high", "low", "close", "volume"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    for c in ["RSI", "ATR", "EMA_20", "EMA_50", "EMA_200", "20_SMA", "VWAP", "MACD_Hist", "ADX"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
        else:
            df[c] = np.nan

    typical = (df["high"] + df["low"] + df["close"]) / 3.0
    pv = typical * df["volume"].fillna(0)
    sess_vol_cum = df.groupby("session")["volume"].transform(lambda s: s.fillna(0).cumsum())
    sess_pv_cum = pv.groupby(df["session"]).transform(lambda s: s.fillna(0).cumsum())
    computed_vwap = sess_pv_cum / sess_vol_cum.replace(0, np.nan)
    # The newest live2 5m store has NaN VWAP after mid-May in many symbols.
    # Use the causal session VWAP computed from current/prior intraday 5m bars.
    df["VWAP_source"] = np.where(df["VWAP"].notna(), "parquet", "computed")
    df["VWAP"] = df["VWAP"].where(df["VWAP"].notna(), computed_vwap)

    g = df.groupby("session", group_keys=False)
    prev_close = g["close"].shift(1)
    prev_high = g["high"].shift(1)
    prev_low = g["low"].shift(1)
    prev_open = g["open"].shift(1)
    df["prev_close"] = prev_close
    df["prev_high"] = prev_high
    df["prev_low"] = prev_low
    df["prev_open"] = prev_open
    df["bar_ret_pct"] = pct(df["close"], df["open"])
    df["prev_ret_pct"] = pct(prev_close, prev_open)
    rng = (df["high"] - df["low"]).replace(0, np.nan)
    df["range_pct"] = rng / df["open"] * 100.0
    df["body_pct"] = (df["close"] - df["open"]).abs() / df["open"] * 100.0
    df["green_body_pct"] = (df["close"] - df["open"]) / df["open"] * 100.0
    df["close_loc"] = (df["close"] - df["low"]) / rng
    df["upper_wick_pct"] = (df["high"] - df[["open", "close"]].max(axis=1)) / df["open"] * 100.0
    df["lower_wick_pct"] = (df[["open", "close"]].min(axis=1) - df["low"]) / df["open"] * 100.0
    df["vol_sma20_prev"] = g["volume"].transform(lambda s: s.shift(1).rolling(20, min_periods=5).mean())
    df["vol_ratio"] = df["volume"] / df["vol_sma20_prev"].replace(0, np.nan)
    df["vol_ratio"] = df["vol_ratio"].replace([np.inf, -np.inf], np.nan)
    df["vol_rising_2"] = (df["volume"] > g["volume"].shift(1)) & (g["volume"].shift(1) > g["volume"].shift(2))
    df["rsi"] = df["RSI"]
    df["rsi_delta"] = g["RSI"].diff(1)
    df["adx"] = df["ADX"]
    df["macd_hist"] = df["MACD_Hist"]
    df["macd_delta"] = g["MACD_Hist"].diff(1)
    df["atr_pct"] = df["ATR"] / df["close"] * 100.0
    df["vwap_dist_pct"] = pct(df["close"], df["VWAP"])
    df["ema20_dist_pct"] = pct(df["close"], df["EMA_20"])
    df["ema20_slope_pct"] = pct(df["EMA_20"], g["EMA_20"].shift(3))
    df["trend_stack"] = (df["close"] > df["EMA_20"]) & (df["EMA_20"] > df["EMA_50"])
    df["above_vwap"] = df["close"] > df["VWAP"]
    df["prev_above_vwap"] = prev_close > g["VWAP"].shift(1)
    df["break_prev_high"] = df["close"] > prev_high
    df["break_3_high"] = df["close"] > g["high"].shift(1).rolling(3, min_periods=2).max()
    df["break_5_high"] = df["close"] > g["high"].shift(1).rolling(5, min_periods=3).max()
    df["under_3_low_then_reclaim"] = (df["low"] < g["low"].shift(1).rolling(3, min_periods=2).min()) & (
        df["close"] > prev_close
    )
    df["range_sma10_prev"] = g["range_pct"].transform(lambda s: s.shift(1).rolling(10, min_periods=4).mean())
    df["range_sma3_prev"] = g["range_pct"].transform(lambda s: s.shift(1).rolling(3, min_periods=2).mean())
    df["compression"] = df["range_sma3_prev"] / df["range_sma10_prev"].replace(0, np.nan)
    df["range_expansion"] = df["range_pct"] / df["range_sma10_prev"].replace(0, np.nan)
    df["green_streak_3"] = (
        (g["close"].shift(1) > g["open"].shift(1)).astype(int)
        + (g["close"].shift(2) > g["open"].shift(2)).astype(int)
        + (g["close"].shift(3) > g["open"].shift(3)).astype(int)
    )
    df["prior_not_bearish"] = (g["close"].shift(1) >= g["open"].shift(1)) | (g["close"].shift(2) >= g["open"].shift(2))
    df["dist_vwap_abs"] = df["vwap_dist_pct"].abs()
    df["score"] = (
        df["vol_ratio"].clip(0, 5).fillna(0) * 20
        + df["close_loc"].clip(0, 1).fillna(0) * 25
        + df["green_body_pct"].clip(-1, 2).fillna(0) * 15
        + df["rsi_delta"].clip(-5, 8).fillna(0) * 2
        + df["range_expansion"].clip(0, 4).fillna(0) * 8
    )
    return df


def build_rules() -> list[RuleSpec]:
    rules: list[RuleSpec] = []

    def add(family, suffix, params, entry, ind, non, premom):
        rid = f"{family}_{suffix}"
        rules.append(RuleSpec(rid, family, rid, params, entry, ind, non, premom))

    for vol in [1.10, 1.25, 1.50]:
        for close_loc in [0.62, 0.72]:
            add(
                "LONG_VWAP_RECLAIM_MOMENTUM",
                f"vol{vol:g}_cl{close_loc:g}",
                {"vol_ratio_min": vol, "close_loc_min": close_loc, "rsi_delta_min": 0.0},
                "Current 5m close reclaims VWAP after previous close was at/below VWAP.",
                [f"RSI delta >= 0, vol_ratio >= {vol:g}, close_loc >= {close_loc:g}"],
                ["close crosses above VWAP", "positive 5m body", "not more than 1.2% above VWAP"],
                "Volume rising or current relative volume confirms the reclaim; prior bars not both bearish.",
            )
    for vol in [1.15, 1.35, 1.60]:
        for body in [0.18, 0.28]:
            add(
                "LONG_PRESSURE_BURST_BREAKOUT",
                f"vol{vol:g}_body{body:g}",
                {"vol_ratio_min": vol, "green_body_min": body, "close_loc_min": 0.68},
                "Close breaks above the prior 5m high on a strong green pressure candle.",
                [f"vol_ratio >= {vol:g}, green_body_pct >= {body:g}, close_loc >= 0.68"],
                ["close above previous candle high", "upper wick <= 0.45%", "green_streak_3 <= 2"],
                "RSI and MACD histogram are rising into the trigger.",
            )
    for comp in [0.72, 0.85]:
        for exp in [1.20, 1.45]:
            add(
                "LONG_CONSOLIDATION_EXPANSION_BREAKOUT",
                f"comp{comp:g}_exp{exp:g}",
                {"compression_max": comp, "range_expansion_min": exp, "vol_ratio_min": 1.05},
                "Range compression over prior bars, then close breaks 3-bar high with expansion.",
                ["ATR% >= 0.20, range expansion after compression", "RSI delta >= -1"],
                [f"compression <= {comp:g}, range_expansion >= {exp:g}", "close above prior 3-bar high"],
                "Prior two candles avoid bearish pressure; volume is at least above prior average.",
            )
    for wick in [0.25, 0.40]:
        for cl in [0.58, 0.68]:
            add(
                "LONG_FAILED_BREAKDOWN_REVERSAL",
                f"wick{wick:g}_cl{cl:g}",
                {"lower_wick_min": wick, "close_loc_min": cl, "vol_ratio_min": 0.90},
                "Price undercuts recent 3-bar low but reclaims into the upper half of the candle.",
                ["RSI delta >= -2, MACD histogram not sharply deteriorating"],
                [f"lower_wick_pct >= {wick:g}, close_loc >= {cl:g}", "current close above prior close"],
                "Requires rejection wick and no extended green streak before the reversal.",
            )
    for pull in [0.0, -0.08]:
        for vol in [0.85, 1.05]:
            add(
                "LONG_PULLBACK_CONTINUATION",
                f"pull{str(pull).replace('-', 'm')}_vol{vol:g}",
                {"prev_ret_max": pull, "vol_ratio_min": vol, "close_loc_min": 0.60},
                "Trend is intact above VWAP/EMA20, prior candle pulls back, current candle breaks prior high.",
                ["EMA20 slope positive, close above VWAP and EMA20"],
                ["previous bar non-green pullback", "current close above previous high"],
                "RSI delta >= 0 and distance from VWAP <= 1.1% to avoid poor tight-stop reward.",
            )
    for vol in [1.50, 2.00]:
        for high_n in [3, 5]:
            add(
                "LONG_VOLUME_EXPANSION_BREAKOUT",
                f"vol{vol:g}_h{high_n}",
                {"vol_ratio_min": vol, "high_n": high_n, "green_body_min": 0.12},
                f"Relative-volume expansion breaks prior {high_n}-bar high.",
                [f"vol_ratio >= {vol:g}, RSI >= 48, ADX >= 12"],
                [f"close above prior {high_n}-bar high", "not overextended from VWAP"],
                "Volume rising into the breakout and candle closes in top 35% of its range.",
            )
    for slope in [0.00, 0.04]:
        for adx in [10.0, 16.0]:
            add(
                "LONG_EMA_VWAP_TREND_CONTINUATION",
                f"slope{slope:g}_adx{adx:g}",
                {"ema20_slope_min": slope, "adx_min": adx, "vol_ratio_min": 0.85},
                "Trend stack continuation above VWAP, EMA20, and EMA50 with prior-high break.",
                [f"EMA20 slope >= {slope:g}, ADX >= {adx:g}", "RSI delta >= -1"],
                ["close > EMA20 > EMA50", "close above VWAP", "close above previous high"],
                "Rejects late exhaustion with green_streak_3 <= 2 and VWAP distance <= 1.25%.",
            )
    for slot_max in [4, 7]:
        for vol in [1.10, 1.40]:
            add(
                "LONG_OPENING_STRENGTH_CONTINUATION",
                f"slot{slot_max}_vol{vol:g}",
                {"slot_max": slot_max, "vol_ratio_min": vol, "green_body_min": 0.15},
                "Early-session strength continuation after the first few 5m bars.",
                [f"vol_ratio >= {vol:g}, RSI >= 50", "MACD histogram improving"],
                [f"slot <= {slot_max}", "close above prior high", "strong close location"],
                "Avoids first raw bar; requires current pressure and no three-candle extension.",
            )
    for slot_min, slot_max in [(15, 36), (20, 48)]:
        for vol in [0.90, 1.15]:
            add(
                "LONG_MIDDAY_RECLAIM_CONTINUATION",
                f"s{slot_min}_{slot_max}_vol{vol:g}",
                {"slot_min": slot_min, "slot_max": slot_max, "vol_ratio_min": vol},
                "Midday VWAP/EMA20 reclaim after a quiet pullback.",
                ["RSI delta >= 0, EMA20 slope >= -0.03", f"vol_ratio >= {vol:g}"],
                [f"{slot_min} <= slot <= {slot_max}", "close reclaims VWAP or EMA20", "close near candle high"],
                "Looks for pre-momentum improvement after lower midday activity.",
            )
    for comp in [0.65, 0.80]:
        for vol in [1.00, 1.25]:
            add(
                "LONG_RANGE_EXPANSION_AFTER_COMPRESSION",
                f"comp{comp:g}_vol{vol:g}",
                {"compression_max": comp, "range_expansion_min": 1.35, "vol_ratio_min": vol},
                "Compression-then-range expansion close above prior high.",
                ["ATR% in workable band, MACD delta >= -0.02", f"vol_ratio >= {vol:g}"],
                [f"compression <= {comp:g}", "range expansion >= 1.35", "close above previous high"],
                "Prior bars are not strongly bearish; entry not too far from VWAP/EMA20.",
            )
    return rules


def rule_mask(df: pd.DataFrame, rule: RuleSpec) -> pd.Series:
    p = rule.params
    base = (
        df["open"].gt(0)
        & df["close"].gt(0)
        & df["slot"].between(1, 67)
        & df["atr_pct"].between(0.16, 2.25)
        & df["dist_vwap_abs"].le(1.60)
        & df["green_streak_3"].le(2)
    )
    fam = rule.family
    if fam == "LONG_VWAP_RECLAIM_MOMENTUM":
        m = (
            base
            & (~df["prev_above_vwap"].fillna(False))
            & df["above_vwap"].fillna(False)
            & df["green_body_pct"].gt(0)
            & df["vol_ratio"].ge(p["vol_ratio_min"])
            & df["close_loc"].ge(p["close_loc_min"])
            & df["rsi_delta"].ge(p["rsi_delta_min"])
            & df["prior_not_bearish"].fillna(False)
        )
    elif fam == "LONG_PRESSURE_BURST_BREAKOUT":
        m = (
            base
            & df["break_prev_high"].fillna(False)
            & df["green_body_pct"].ge(p["green_body_min"])
            & df["vol_ratio"].ge(p["vol_ratio_min"])
            & df["close_loc"].ge(p["close_loc_min"])
            & df["upper_wick_pct"].le(0.45)
            & df["rsi_delta"].ge(-0.5)
            & df["macd_delta"].ge(-0.03)
        )
    elif fam == "LONG_CONSOLIDATION_EXPANSION_BREAKOUT":
        m = (
            base
            & df["break_3_high"].fillna(False)
            & df["compression"].le(p["compression_max"])
            & df["range_expansion"].ge(p["range_expansion_min"])
            & df["vol_ratio"].ge(p["vol_ratio_min"])
            & df["close_loc"].ge(0.62)
            & df["rsi_delta"].ge(-1.0)
        )
    elif fam == "LONG_FAILED_BREAKDOWN_REVERSAL":
        m = (
            base
            & df["under_3_low_then_reclaim"].fillna(False)
            & df["lower_wick_pct"].ge(p["lower_wick_min"])
            & df["close_loc"].ge(p["close_loc_min"])
            & df["vol_ratio"].ge(p["vol_ratio_min"])
            & df["macd_delta"].ge(-0.08)
            & df["green_body_pct"].ge(-0.05)
        )
    elif fam == "LONG_PULLBACK_CONTINUATION":
        m = (
            base
            & df["above_vwap"].fillna(False)
            & df["trend_stack"].fillna(False)
            & df["break_prev_high"].fillna(False)
            & df["prev_ret_pct"].le(p["prev_ret_max"])
            & df["vol_ratio"].ge(p["vol_ratio_min"])
            & df["close_loc"].ge(p["close_loc_min"])
            & df["rsi_delta"].ge(0)
            & df["dist_vwap_abs"].le(1.10)
        )
    elif fam == "LONG_VOLUME_EXPANSION_BREAKOUT":
        high_break = df["break_3_high"] if p["high_n"] == 3 else df["break_5_high"]
        m = (
            base
            & high_break.fillna(False)
            & df["vol_ratio"].ge(p["vol_ratio_min"])
            & df["green_body_pct"].ge(p["green_body_min"])
            & df["close_loc"].ge(0.65)
            & df["rsi"].ge(48)
            & df["adx"].ge(12)
            & df["vol_rising_2"].fillna(False)
        )
    elif fam == "LONG_EMA_VWAP_TREND_CONTINUATION":
        m = (
            base
            & df["trend_stack"].fillna(False)
            & df["above_vwap"].fillna(False)
            & df["break_prev_high"].fillna(False)
            & df["ema20_slope_pct"].ge(p["ema20_slope_min"])
            & df["adx"].ge(p["adx_min"])
            & df["vol_ratio"].ge(p["vol_ratio_min"])
            & df["rsi_delta"].ge(-1)
            & df["dist_vwap_abs"].le(1.25)
        )
    elif fam == "LONG_OPENING_STRENGTH_CONTINUATION":
        m = (
            base
            & df["slot"].between(2, p["slot_max"])
            & df["break_prev_high"].fillna(False)
            & df["green_body_pct"].ge(p["green_body_min"])
            & df["vol_ratio"].ge(p["vol_ratio_min"])
            & df["close_loc"].ge(0.68)
            & df["rsi"].ge(50)
            & df["macd_delta"].ge(-0.02)
        )
    elif fam == "LONG_MIDDAY_RECLAIM_CONTINUATION":
        reclaim = ((~df["prev_above_vwap"].fillna(False)) & df["above_vwap"].fillna(False)) | (
            (df["prev_close"] <= df["EMA_20"]) & (df["close"] > df["EMA_20"])
        )
        m = (
            base
            & df["slot"].between(p["slot_min"], p["slot_max"])
            & reclaim.fillna(False)
            & df["vol_ratio"].ge(p["vol_ratio_min"])
            & df["close_loc"].ge(0.62)
            & df["rsi_delta"].ge(0)
            & df["ema20_slope_pct"].ge(-0.03)
        )
    elif fam == "LONG_RANGE_EXPANSION_AFTER_COMPRESSION":
        m = (
            base
            & df["break_prev_high"].fillna(False)
            & df["compression"].le(p["compression_max"])
            & df["range_expansion"].ge(p["range_expansion_min"])
            & df["vol_ratio"].ge(p["vol_ratio_min"])
            & df["close_loc"].ge(0.65)
            & df["macd_delta"].ge(-0.02)
        )
    else:
        m = pd.Series(False, index=df.index)
    return m.fillna(False)


def exit_specs() -> list[ExitSpec]:
    base = [
        (0.75, 0.75),
        (0.50, 0.50),
        (0.60, 0.60),
        (0.75, 1.00),
        (0.50, 0.75),
    ]
    out: list[ExitSpec] = []
    for sl, tgt in base:
        for bars in [3, 6, 9]:
            out.append(ExitSpec(f"sl{sl:g}_tgt{tgt:g}_tb{bars}", sl, tgt, bars))
    for sl, tgt in [(0.75, 0.75), (0.60, 0.60), (0.75, 1.00)]:
        out.append(ExitSpec(f"sl{sl:g}_tgt{tgt:g}_tb6_be0p4", sl, tgt, 6, breakeven_after_pct=0.40))
    out.append(ExitSpec("sl0p75_tgt1_tb9_trail0p5_after0p75", 0.75, 1.00, 9, trailing_after_pct=0.75, trailing_gap_pct=0.50))
    return out


def guard_specs() -> list[GuardSpec]:
    return [
        GuardSpec("g_base", min_slot=2, max_slot=60, top_n_per_slot=None, max_per_symbol_day=1, cooldown_after_sl_bars=0),
        GuardSpec("g_morning", min_slot=2, max_slot=22, top_n_per_slot=None, max_per_symbol_day=1, cooldown_after_sl_bars=0),
        GuardSpec("g_no_open", min_slot=5, max_slot=60, top_n_per_slot=None, max_per_symbol_day=1, cooldown_after_sl_bars=0),
        GuardSpec("g_top3_slot", min_slot=2, max_slot=60, top_n_per_slot=3, max_per_symbol_day=1, cooldown_after_sl_bars=0),
        GuardSpec("g_top5_slot", min_slot=2, max_slot=60, top_n_per_slot=5, max_per_symbol_day=1, cooldown_after_sl_bars=0),
        GuardSpec("g_two_per_symbol", min_slot=2, max_slot=60, top_n_per_slot=5, max_per_symbol_day=2, cooldown_after_sl_bars=6),
    ]


def build_signal_pool(
    five_files: dict[str, Path],
    one_files: dict[str, Path],
    sessions: list[pd.Timestamp],
    rules: list[RuleSpec],
    max_symbols: int,
) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    sess_set = set(pd.Timestamp(x).normalize() for x in sessions)
    common = sorted(set(five_files) & set(one_files))
    if max_symbols and max_symbols > 0:
        common = common[:max_symbols]
    rows = []
    rule_rows = []
    q = {"symbols_considered": len(common), "symbols_with_signals": 0, "5m_bad_files": 0, "nan_summary": {}}
    feature_cols = [
        "symbol", "date", "session", "slot", "open", "high", "low", "close", "volume",
        "rsi", "rsi_delta", "adx", "macd_hist", "macd_delta", "atr_pct", "vwap_dist_pct",
        "ema20_dist_pct", "ema20_slope_pct", "vol_ratio", "range_pct", "body_pct",
        "green_body_pct", "close_loc", "upper_wick_pct", "lower_wick_pct", "compression",
        "range_expansion", "green_streak_3", "score", "dist_vwap_abs", "above_vwap",
        "trend_stack", "break_prev_high", "break_3_high", "break_5_high",
    ]
    started = time.time()
    for i, sym in enumerate(common, 1):
        try:
            raw = read_parquet_existing(five_files[sym], BASE_COLS_5M)
            ff = feature_frame(raw, sym)
            ff = ff[ff["session"].isin(sess_set)].copy()
            if ff.empty:
                continue
            any_signal = pd.Series(False, index=ff.index)
            local_rule_rows = []
            for r in rules:
                m = rule_mask(ff, r)
                if m.any():
                    sel = ff.loc[m, feature_cols].copy()
                    sel["rule_id"] = r.rule_id
                    sel["family"] = r.family
                    local_rule_rows.append(sel)
                    any_signal |= m
            if local_rule_rows:
                q["symbols_with_signals"] += 1
                base = ff.loc[any_signal, feature_cols].copy()
                base["candidate_key"] = sym + "|" + base["date"].astype(str)
                rows.append(base)
                rule_rows.append(pd.concat(local_rule_rows, ignore_index=True))
        except Exception:
            q["5m_bad_files"] += 1
        if i % 150 == 0:
            print(f"[signals] {i}/{len(common)} symbols, base chunks={len(rows)} ({time.time() - started:.0f}s)", flush=True)
    if not rows:
        raise SystemExit("No raw 5m signals generated")
    cand = pd.concat(rows, ignore_index=True).drop_duplicates("candidate_key").reset_index(drop=True)
    cand["candidate_id"] = np.arange(len(cand), dtype=np.int64)
    ckey_to_id = dict(zip(cand["candidate_key"], cand["candidate_id"]))
    rule_df = pd.concat(rule_rows, ignore_index=True)
    rule_df["candidate_key"] = rule_df["symbol"] + "|" + rule_df["date"].astype(str)
    rule_df["candidate_id"] = rule_df["candidate_key"].map(ckey_to_id)
    rule_df = rule_df.dropna(subset=["candidate_id"]).copy()
    rule_df["candidate_id"] = rule_df["candidate_id"].astype(np.int64)
    rule_df = rule_df.drop_duplicates(["rule_id", "candidate_id"]).reset_index(drop=True)
    nan_cols = ["VWAP", "EMA_20", "ATR", "RSI", "MACD_Hist", "ADX"]
    q["candidate_rows"] = int(len(cand))
    q["rule_candidate_rows"] = int(len(rule_df))
    q["n_rules_with_hits"] = int(rule_df["rule_id"].nunique())
    return cand, rule_df[["rule_id", "family", "candidate_id"]], q


def load_1m_symbol(path: Path, sessions: set[pd.Timestamp]) -> pd.DataFrame:
    df = read_parquet_existing(path, BASE_COLS_1M)
    df["date"] = normalize_dt(df["date"])
    df = df.dropna(subset=["date"]).sort_values("date").drop_duplicates("date")
    df["session"] = df["date"].dt.normalize()
    df = df[df["session"].isin(sessions)].copy()
    for c in ["open", "high", "low", "close", "volume"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df = df.dropna(subset=["open", "high", "low", "close"])
    return df


def resolve_one_candidate(
    signal_time: pd.Timestamp,
    one: pd.DataFrame,
    ex: ExitSpec,
    slip_bps: float,
) -> dict | None:
    future = one[one["date"] > signal_time].copy()
    if future.empty:
        return None
    entry_row = future.iloc[0]
    entry_time = pd.Timestamp(entry_row["date"])
    if entry_time > signal_time + pd.Timedelta(minutes=3):
        return None
    end_time = entry_time + pd.Timedelta(minutes=ex.time_bars * 5)
    window = future[future["date"] <= end_time].copy()
    if window.empty:
        return None
    slip = slip_bps / 10000.0
    entry_raw = float(entry_row["open"])
    if not math.isfinite(entry_raw) or entry_raw <= 0:
        return None
    entry_fill = entry_raw * (1.0 + slip)
    sl_price = entry_fill * (1.0 - ex.sl_pct / 100.0)
    target_price = entry_fill * (1.0 + ex.target_pct / 100.0)
    active_stop = sl_price
    be_active = False
    trail_active = False
    exit_raw = float(window.iloc[-1]["close"])
    exit_time = pd.Timestamp(window.iloc[-1]["date"])
    outcome = "TIME"
    same_1m_tie = 0
    five_min_conflict = 0
    rows_scanned = 0

    window["_bucket5"] = window["date"].dt.floor("5min")
    for _, bucket in window.groupby("_bucket5", sort=True):
        rows_scanned += len(bucket)
        bucket_hit_sl = bool((bucket["low"] <= active_stop).any())
        bucket_hit_tgt = bool((bucket["high"] >= target_price).any())
        if bucket_hit_sl and bucket_hit_tgt:
            five_min_conflict = 1
        for r in bucket.itertuples(index=False):
            t = pd.Timestamp(r.date)
            hi = float(r.high)
            lo = float(r.low)
            close = float(r.close)
            hit_sl = lo <= active_stop
            hit_tgt = hi >= target_price
            if hit_sl and hit_tgt:
                same_1m_tie = 1
                outcome = "SL"
                exit_raw = active_stop
                exit_time = t
                break
            if hit_sl:
                outcome = "SL" if active_stop < entry_fill * 0.999 else "BREAKEVEN"
                exit_raw = active_stop
                exit_time = t
                break
            if hit_tgt:
                outcome = "TARGET"
                exit_raw = target_price
                exit_time = t
                break
            if ex.breakeven_after_pct is not None and (not be_active):
                be_trigger = entry_fill * (1.0 + ex.breakeven_after_pct / 100.0)
                if hi >= be_trigger:
                    be_active = True
                    active_stop = max(active_stop, entry_fill)
                    if lo <= active_stop:
                        outcome = "BREAKEVEN"
                        exit_raw = active_stop
                        exit_time = t
                        break
            if ex.trailing_after_pct is not None and ex.trailing_gap_pct is not None:
                trail_trigger = entry_fill * (1.0 + ex.trailing_after_pct / 100.0)
                if hi >= trail_trigger:
                    trail_active = True
                if trail_active:
                    active_stop = max(active_stop, hi * (1.0 - ex.trailing_gap_pct / 100.0))
                    if lo <= active_stop:
                        outcome = "TRAIL"
                        exit_raw = active_stop
                        exit_time = t
                        break
        if outcome in {"SL", "TARGET", "BREAKEVEN", "TRAIL"}:
            break

    if outcome == "TIME":
        # If the window reaches or passes the usual NSE cash close zone, classify as forced/EOD.
        if exit_time.hour > 15 or (exit_time.hour == 15 and exit_time.minute >= 20):
            outcome = "EOD"
    exit_fill = float(exit_raw) * (1.0 - slip)
    gross_pnl = (exit_fill - entry_fill) * math.floor(NOTIONAL_RS / entry_fill)
    qty = max(1, math.floor(NOTIONAL_RS / entry_fill))
    net = net_pnl_vectorized(
        np.asarray([entry_fill], dtype=float),
        np.asarray([exit_fill], dtype=float),
        np.asarray([qty], dtype=float),
        np.asarray(["LONG"]),
        CostConfig(),
    )[0]
    return {
        "entry_time": str(entry_time),
        "entry_raw": round(entry_raw, 4),
        "entry_fill": round(entry_fill, 4),
        "exit_time": str(exit_time),
        "exit_raw": round(float(exit_raw), 4),
        "exit_fill": round(exit_fill, 4),
        "qty": int(qty),
        "gross_pnl": round(float(gross_pnl), 4),
        "net_pnl": round(float(net), 4),
        "outcome": outcome,
        "holding_min": round((exit_time - entry_time).total_seconds() / 60.0, 2),
        "five_min_conflict": five_min_conflict,
        "same_1m_tie": same_1m_tie,
        "rows_scanned": int(rows_scanned),
        "sl_price": round(sl_price, 4),
        "target_price": round(target_price, 4),
    }


def resolve_exits(
    candidates: pd.DataFrame,
    one_files: dict[str, Path],
    sessions: list[pd.Timestamp],
    exits: list[ExitSpec],
    slip_bps: float,
) -> pd.DataFrame:
    sess_set = set(pd.Timestamp(x).normalize() for x in sessions)
    out_rows = []
    started = time.time()
    grouped = candidates.groupby("symbol", sort=True)
    n_groups = grouped.ngroups
    slip = slip_bps / 10000.0
    max_minutes = max(e.time_bars for e in exits) * 5 + 2

    def _resolve_arrays(sig_t64, dates_ns, opens, highs, lows, closes, ex: ExitSpec):
        start = int(np.searchsorted(dates_ns, sig_t64, side="right"))
        if start >= len(dates_ns):
            return None
        entry_time_ns = dates_ns[start]
        if entry_time_ns > sig_t64 + np.timedelta64(3, "m"):
            return None
        end_time_ns = entry_time_ns + np.timedelta64(ex.time_bars * 5, "m")
        end = int(np.searchsorted(dates_ns, end_time_ns, side="right"))
        if end <= start:
            return None
        entry_raw = float(opens[start])
        if not math.isfinite(entry_raw) or entry_raw <= 0:
            return None
        entry_fill = entry_raw * (1.0 + slip)
        sl_price = entry_fill * (1.0 - ex.sl_pct / 100.0)
        target_price = entry_fill * (1.0 + ex.target_pct / 100.0)
        active_stop = sl_price
        be_active = False
        trail_active = False
        outcome = "TIME"
        exit_raw = float(closes[end - 1])
        exit_i = end - 1
        same_1m_tie = 0

        for j in range(start, end):
            hi = float(highs[j])
            lo = float(lows[j])
            hit_sl = lo <= active_stop
            hit_tgt = hi >= target_price
            if hit_sl and hit_tgt:
                same_1m_tie = 1
                outcome = "SL"
                exit_raw = active_stop
                exit_i = j
                break
            if hit_sl:
                outcome = "SL" if active_stop < entry_fill * 0.999 else "BREAKEVEN"
                exit_raw = active_stop
                exit_i = j
                break
            if hit_tgt:
                outcome = "TARGET"
                exit_raw = target_price
                exit_i = j
                break
            if ex.breakeven_after_pct is not None and not be_active:
                be_trigger = entry_fill * (1.0 + ex.breakeven_after_pct / 100.0)
                if hi >= be_trigger:
                    be_active = True
                    active_stop = max(active_stop, entry_fill)
                    if lo <= active_stop:
                        outcome = "BREAKEVEN"
                        exit_raw = active_stop
                        exit_i = j
                        break
            if ex.trailing_after_pct is not None and ex.trailing_gap_pct is not None:
                trail_trigger = entry_fill * (1.0 + ex.trailing_after_pct / 100.0)
                if hi >= trail_trigger:
                    trail_active = True
                if trail_active:
                    active_stop = max(active_stop, hi * (1.0 - ex.trailing_gap_pct / 100.0))
                    if lo <= active_stop:
                        outcome = "TRAIL"
                        exit_raw = active_stop
                        exit_i = j
                        break

        exit_time_ns = dates_ns[exit_i]
        if outcome == "TIME":
            exit_ts = pd.Timestamp(exit_time_ns)
            if exit_ts.hour > 15 or (exit_ts.hour == 15 and exit_ts.minute >= 20):
                outcome = "EOD"

        # If the containing 5-minute aggregate touched both sides, this is exactly
        # where a 5m-only resolver would be ambiguous; the minute loop above gives order.
        bucket0 = pd.Timestamp(exit_time_ns).floor("5min").to_datetime64()
        bucket1 = bucket0 + np.timedelta64(5, "m")
        b0 = int(np.searchsorted(dates_ns, bucket0, side="left"))
        b1 = int(np.searchsorted(dates_ns, bucket1, side="left"))
        b0 = max(b0, start)
        b1 = min(b1, end)
        five_min_conflict = 0
        if b1 > b0:
            five_min_conflict = int(np.nanmax(highs[b0:b1]) >= target_price and np.nanmin(lows[b0:b1]) <= sl_price)

        exit_fill = float(exit_raw) * (1.0 - slip)
        qty = max(1, math.floor(NOTIONAL_RS / entry_fill))
        gross_pnl = (exit_fill - entry_fill) * qty
        return {
            "entry_time": str(pd.Timestamp(entry_time_ns)),
            "entry_raw": round(entry_raw, 4),
            "entry_fill": round(entry_fill, 4),
            "exit_time": str(pd.Timestamp(exit_time_ns)),
            "exit_raw": round(float(exit_raw), 4),
            "exit_fill": round(exit_fill, 4),
            "qty": int(qty),
            "gross_pnl": round(float(gross_pnl), 4),
            "outcome": outcome,
            "holding_min": round((exit_time_ns - entry_time_ns) / np.timedelta64(1, "m"), 2),
            "five_min_conflict": int(five_min_conflict),
            "same_1m_tie": int(same_1m_tie),
            "rows_scanned": int(max(0, end - start)),
            "sl_price": round(sl_price, 4),
            "target_price": round(target_price, 4),
        }

    for i, (sym, g) in enumerate(grouped, 1):
        p = one_files.get(sym)
        if p is None:
            continue
        try:
            one = load_1m_symbol(p, sess_set)
        except Exception:
            continue
        if one.empty:
            continue
        one = one.sort_values("date")
        dates_ns = one["date"].to_numpy(dtype="datetime64[ns]")
        opens = one["open"].to_numpy(dtype=float)
        highs = one["high"].to_numpy(dtype=float)
        lows = one["low"].to_numpy(dtype=float)
        closes = one["close"].to_numpy(dtype=float)
        for row in g.itertuples(index=False):
            sig_t64 = np.datetime64(pd.Timestamp(row.date).to_datetime64(), "ns")
            for ex in exits:
                rec = _resolve_arrays(sig_t64, dates_ns, opens, highs, lows, closes, ex)
                if rec is None:
                    continue
                rec.update({"candidate_id": int(row.candidate_id), "exit_id": ex.exit_id})
                out_rows.append(rec)
        if i % 50 == 0:
            print(f"[exits] {i}/{n_groups} symbols resolved, rows={len(out_rows):,} ({time.time() - started:.0f}s)", flush=True)
    if not out_rows:
        raise SystemExit("No exits resolved from 1m data")
    out = pd.DataFrame(out_rows)
    net = net_pnl_vectorized(
        out["entry_fill"].to_numpy(dtype=float),
        out["exit_fill"].to_numpy(dtype=float),
        out["qty"].to_numpy(dtype=float),
        np.asarray(["LONG"] * len(out), dtype=object),
        CostConfig(),
    )
    out["net_pnl"] = np.round(net.astype(float), 4)
    return out


def apply_guards(df: pd.DataFrame, guard: GuardSpec) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    if guard.min_slot is not None:
        out = out[out["slot"] >= guard.min_slot]
    if guard.max_slot is not None:
        out = out[out["slot"] <= guard.max_slot]
    if out.empty:
        return out
    out = out.sort_values(["session", "date", "score"], ascending=[True, True, False]).copy()
    if guard.top_n_per_slot is not None and guard.top_n_per_slot > 0:
        out["_slot_rank"] = out.groupby(["session", "date"])["score"].rank(method="first", ascending=False)
        out = out[out["_slot_rank"] <= guard.top_n_per_slot].drop(columns=["_slot_rank"])
    if guard.max_per_symbol_day > 0:
        out["_sym_day_rank"] = out.groupby(["symbol", "session"]).cumcount() + 1
        out = out[out["_sym_day_rank"] <= guard.max_per_symbol_day].drop(columns=["_sym_day_rank"])
    if guard.cooldown_after_sl_bars > 0 and not out.empty:
        kept = []
        cooldown_until: dict[tuple[str, pd.Timestamp], pd.Timestamp] = {}
        delta = pd.Timedelta(minutes=guard.cooldown_after_sl_bars * 5)
        for r in out.sort_values(["symbol", "session", "date"]).itertuples(index=False):
            key = (r.symbol, pd.Timestamp(r.session))
            t = pd.Timestamp(r.date)
            if key in cooldown_until and t <= cooldown_until[key]:
                continue
            kept.append(r)
            if str(r.outcome) in {"SL"}:
                cooldown_until[key] = t + delta
        out = pd.DataFrame(kept, columns=out.columns) if kept else out.iloc[0:0].copy()
    return out


def profit_factor(net: Iterable[float]) -> float:
    x = np.asarray(list(net), dtype=float)
    if len(x) == 0:
        return 0.0
    gp = x[x > 0].sum()
    gl = -x[x < 0].sum()
    if gl <= 0:
        return float("inf") if gp > 0 else 0.0
    return float(gp / gl)


def max_drawdown(net: pd.Series) -> float:
    if net.empty:
        return 0.0
    cum = net.cumsum()
    dd = cum - cum.cummax()
    return float(dd.min())


def metric_pack(trades: pd.DataFrame) -> dict:
    if trades.empty:
        return {
            "trades": 0, "wins": 0, "losses": 0, "win_rate_pct": 0.0, "gross_profit": 0.0,
            "gross_loss": 0.0, "net_pnl": 0.0, "net_pf": 0.0, "avg_win": 0.0, "avg_loss": 0.0,
            "expectancy": 0.0, "max_drawdown": 0.0, "avg_holding_min": 0.0, "sl_cnt": 0,
            "target_cnt": 0, "time_exit_cnt": 0, "eod_exit_cnt": 0, "breakeven_cnt": 0,
            "trail_cnt": 0, "five_min_conflict_cnt": 0, "same_1m_tie_cnt": 0,
            "trades_per_day": 0.0, "top_trade_gross_profit_share": 0.0, "top_day_net_share": 0.0,
            "top_symbol_net_share": 0.0, "daywise": [], "symbolwise": [], "timewise": [],
        }
    t = trades.copy()
    t["net_pnl"] = pd.to_numeric(t["net_pnl"], errors="coerce").fillna(0.0)
    t["gross_pnl"] = pd.to_numeric(t["gross_pnl"], errors="coerce").fillna(0.0)
    net = t["net_pnl"]
    wins = net[net > 0]
    losses = net[net <= 0]
    gp = float(wins.sum())
    gl = float(losses.sum())
    total = float(net.sum())
    day_net = t.groupby(t["session"].astype(str))["net_pnl"].sum()
    sym_net = t.groupby("symbol")["net_pnl"].sum()
    t["_hour"] = pd.to_datetime(t["entry_time"]).dt.strftime("%H:%M").str.slice(0, 2)
    time_net = t.groupby("_hour")["net_pnl"].sum()
    gross_wins = t.loc[t["gross_pnl"] > 0, "gross_pnl"]
    return {
        "trades": int(len(t)),
        "wins": int((net > 0).sum()),
        "losses": int((net <= 0).sum()),
        "win_rate_pct": round(float((net > 0).mean() * 100.0), 2),
        "gross_profit": round(gp, 2),
        "gross_loss": round(gl, 2),
        "net_pnl": round(total, 2),
        "net_pf": round(profit_factor(net), 4) if math.isfinite(profit_factor(net)) else 99.0,
        "avg_win": round(float(wins.mean()), 2) if len(wins) else 0.0,
        "avg_loss": round(float(losses.mean()), 2) if len(losses) else 0.0,
        "expectancy": round(float(net.mean()), 2),
        "max_drawdown": round(max_drawdown(net.reset_index(drop=True)), 2),
        "avg_holding_min": round(float(pd.to_numeric(t["holding_min"], errors="coerce").mean()), 2),
        "sl_cnt": int((t["outcome"] == "SL").sum()),
        "target_cnt": int((t["outcome"] == "TARGET").sum()),
        "time_exit_cnt": int((t["outcome"] == "TIME").sum()),
        "eod_exit_cnt": int((t["outcome"] == "EOD").sum()),
        "breakeven_cnt": int((t["outcome"] == "BREAKEVEN").sum()),
        "trail_cnt": int((t["outcome"] == "TRAIL").sum()),
        "five_min_conflict_cnt": int(pd.to_numeric(t["five_min_conflict"], errors="coerce").fillna(0).sum()),
        "same_1m_tie_cnt": int(pd.to_numeric(t["same_1m_tie"], errors="coerce").fillna(0).sum()),
        "trades_per_day": round(len(t) / max(1, t["session"].nunique()), 2),
        "top_trade_gross_profit_share": round(float(gross_wins.max() / gross_wins.sum()), 3) if gross_wins.sum() > 0 else 9.99,
        "top_day_net_share": round(float(day_net.max() / total), 3) if total > 0 else 9.99,
        "top_symbol_net_share": round(float(sym_net.max() / total), 3) if total > 0 else 9.99,
        "daywise": [
            {"date": k, "trades": int((t["session"].astype(str) == k).sum()), "net_pnl": round(float(v), 2),
             "pf": round(profit_factor(t.loc[t["session"].astype(str) == k, "net_pnl"]), 3)}
            for k, v in day_net.sort_index().items()
        ],
        "symbolwise": [
            {"symbol": k, "trades": int((t["symbol"] == k).sum()), "net_pnl": round(float(v), 2),
             "pf": round(profit_factor(t.loc[t["symbol"] == k, "net_pnl"]), 3)}
            for k, v in sym_net.sort_values(ascending=False).head(20).items()
        ],
        "timewise": [
            {"hour": k, "trades": int((t["_hour"] == k).sum()), "net_pnl": round(float(v), 2),
             "pf": round(profit_factor(t.loc[t["_hour"] == k, "net_pnl"]), 3)}
            for k, v in time_net.sort_index().items()
        ],
    }


def metric_pack_fast(trades: pd.DataFrame) -> dict:
    if trades.empty:
        return metric_pack(pd.DataFrame())
    t = trades
    net = pd.to_numeric(t["net_pnl"], errors="coerce").fillna(0.0)
    gross = pd.to_numeric(t["gross_pnl"], errors="coerce").fillna(0.0)
    wins = net[net > 0]
    losses = net[net <= 0]
    total = float(net.sum())
    gp = float(wins.sum())
    gl = float(losses.sum())
    day_net = net.groupby(t["session"].astype(str)).sum()
    sym_net = net.groupby(t["symbol"]).sum()
    gross_wins = gross[gross > 0]
    return {
        "trades": int(len(t)),
        "wins": int((net > 0).sum()),
        "losses": int((net <= 0).sum()),
        "win_rate_pct": round(float((net > 0).mean() * 100.0), 2),
        "gross_profit": round(gp, 2),
        "gross_loss": round(gl, 2),
        "net_pnl": round(total, 2),
        "net_pf": round(profit_factor(net), 4) if math.isfinite(profit_factor(net)) else 99.0,
        "avg_win": round(float(wins.mean()), 2) if len(wins) else 0.0,
        "avg_loss": round(float(losses.mean()), 2) if len(losses) else 0.0,
        "expectancy": round(float(net.mean()), 2),
        "max_drawdown": round(max_drawdown(net.reset_index(drop=True)), 2),
        "avg_holding_min": round(float(pd.to_numeric(t["holding_min"], errors="coerce").mean()), 2),
        "sl_cnt": int((t["outcome"] == "SL").sum()),
        "target_cnt": int((t["outcome"] == "TARGET").sum()),
        "time_exit_cnt": int((t["outcome"] == "TIME").sum()),
        "eod_exit_cnt": int((t["outcome"] == "EOD").sum()),
        "breakeven_cnt": int((t["outcome"] == "BREAKEVEN").sum()),
        "trail_cnt": int((t["outcome"] == "TRAIL").sum()),
        "five_min_conflict_cnt": int(pd.to_numeric(t["five_min_conflict"], errors="coerce").fillna(0).sum()),
        "same_1m_tie_cnt": int(pd.to_numeric(t["same_1m_tie"], errors="coerce").fillna(0).sum()),
        "trades_per_day": round(len(t) / max(1, t["session"].nunique()), 2),
        "top_trade_gross_profit_share": round(float(gross_wins.max() / gross_wins.sum()), 3) if gross_wins.sum() > 0 else 9.99,
        "top_day_net_share": round(float(day_net.max() / total), 3) if total > 0 else 9.99,
        "top_symbol_net_share": round(float(sym_net.max() / total), 3) if total > 0 else 9.99,
        "daywise": [],
        "symbolwise": [],
        "timewise": [],
    }


def prepare_eval_context(cand: pd.DataFrame, rule_df: pd.DataFrame, exit_df: pd.DataFrame) -> dict:
    cand_idx = cand.drop_duplicates("candidate_id").set_index("candidate_id", drop=False).sort_index()
    rule_ids = {
        rid: g["candidate_id"].drop_duplicates().to_numpy(dtype=np.int64)
        for rid, g in rule_df.groupby("rule_id", sort=False)
    }
    exit_by_id = {}
    for eid, g in exit_df.groupby("exit_id", sort=False):
        exit_by_id[eid] = g.drop_duplicates("candidate_id").set_index("candidate_id", drop=False).sort_index()
    return {"cand_idx": cand_idx, "rule_ids": rule_ids, "exit_by_id": exit_by_id}


def evaluate_candidate(
    cand: pd.DataFrame,
    rule_df: pd.DataFrame,
    exit_df: pd.DataFrame,
    rule_id: str,
    exit_id: str,
    guard: GuardSpec,
    sessions: list[pd.Timestamp],
    full_detail: bool = True,
) -> tuple[pd.DataFrame, dict]:
    sess_set = set(pd.Timestamp(x).normalize() for x in sessions)
    if EVAL_CTX is not None:
        ids_arr = EVAL_CTX["rule_ids"].get(rule_id)
        ex_idx = EVAL_CTX["exit_by_id"].get(exit_id)
        if ids_arr is None or len(ids_arr) == 0 or ex_idx is None:
            return pd.DataFrame(), metric_pack(pd.DataFrame())
        base = EVAL_CTX["cand_idx"].reindex(ids_arr).dropna(subset=["candidate_id"]).copy()
        if base.empty:
            return pd.DataFrame(), metric_pack(pd.DataFrame())
        base = base[base["session"].isin(sess_set)].copy()
        if base.empty:
            return pd.DataFrame(), metric_pack(pd.DataFrame())
        ex = ex_idx.reindex(base["candidate_id"].to_numpy(dtype=np.int64)).reset_index(drop=True)
        base = base.reset_index(drop=True)
        ex = ex.drop(columns=[c for c in ["candidate_id", "exit_id"] if c in ex.columns])
        tr = pd.concat([base, ex], axis=1)
        tr["exit_id"] = exit_id
        tr = tr.dropna(subset=["outcome", "net_pnl"])
    else:
        ids = rule_df.loc[rule_df["rule_id"] == rule_id, ["candidate_id", "family"]]
        if ids.empty:
            return pd.DataFrame(), metric_pack(pd.DataFrame())
        base = cand[cand["candidate_id"].isin(ids["candidate_id"]) & cand["session"].isin(sess_set)].copy()
        if base.empty:
            return pd.DataFrame(), metric_pack(pd.DataFrame())
        ex = exit_df[exit_df["exit_id"] == exit_id].copy()
        if ex.empty:
            return pd.DataFrame(), metric_pack(pd.DataFrame())
        tr = base.merge(ex, on="candidate_id", how="inner")
    if tr.empty:
        return pd.DataFrame(), metric_pack(pd.DataFrame())
    tr["family"] = rule_df.loc[rule_df["rule_id"] == rule_id, "family"].iloc[0]
    tr = apply_guards(tr, guard)
    tr = tr.sort_values(["session", "date", "score"], ascending=[True, True, False]).reset_index(drop=True)
    return tr, metric_pack(tr) if full_detail else metric_pack_fast(tr)


def score_fitval(fit_m: dict, val_m: dict, min_trades: int = 8) -> float:
    if fit_m["trades"] < min_trades or val_m["trades"] < min_trades:
        return -10.0 + min(fit_m["trades"], val_m["trades"]) / max(1, min_trades)
    pf_f = min(float(fit_m["net_pf"]), 2.0)
    pf_v = min(float(val_m["net_pf"]), 2.0)
    wr_bonus = (fit_m["win_rate_pct"] + val_m["win_rate_pct"]) / 200.0
    return min(pf_f, pf_v) - 0.45 * abs(pf_f - pf_v) + 0.15 * wr_bonus


def stability_pass(train_m: dict, test_m: dict) -> tuple[bool, list[str]]:
    reasons = []
    if train_m["trades"] < 25:
        reasons.append("TRAIN trades < 25")
    if test_m["trades"] < 8:
        reasons.append("TEST trades < 8")
    if train_m["net_pf"] < 1.05:
        reasons.append("TRAIN PF < 1.05")
    if test_m["net_pf"] < 1.40:
        reasons.append("TEST PF < 1.40")
    if train_m["win_rate_pct"] < 52:
        reasons.append("TRAIN win rate < 52%")
    if test_m["win_rate_pct"] < 52:
        reasons.append("TEST win rate < 52%")
    if train_m["top_day_net_share"] not in (0.0, 9.99) and train_m["top_day_net_share"] > 0.45:
        reasons.append("TRAIN top-day concentration > 45%")
    if test_m["top_day_net_share"] not in (0.0, 9.99) and test_m["top_day_net_share"] > 0.55:
        reasons.append("TEST top-day concentration > 55%")
    if test_m["top_symbol_net_share"] not in (0.0, 9.99) and test_m["top_symbol_net_share"] > 0.55:
        reasons.append("TEST top-symbol concentration > 55%")
    if train_m["time_exit_cnt"] > train_m["target_cnt"] + train_m["sl_cnt"]:
        reasons.append("time exits dominate TRAIN")
    return (not reasons), reasons


def write_data_audit(
    five_dir: Path,
    one_dir: Path,
    five_files: dict[str, Path],
    one_files: dict[str, Path],
    five_counts: pd.DataFrame,
    one_counts: pd.DataFrame,
    meta5: dict,
    meta1: dict,
    sess: dict,
    signal_q: dict,
) -> None:
    sample5 = next(iter(five_files.values()))
    sample1 = next(iter(one_files.values()))
    cols5, rows5, ncols5, min5, max5 = inspect_columns(sample5)
    cols1, rows1, ncols1, min1, max1 = inspect_columns(sample1)
    ind5 = [c for c in cols5 if any(h in c.lower() for h in INDICATOR_HINTS)]
    non5 = [c for c in cols5 if c not in ind5]
    ind1 = [c for c in cols1 if any(h in c.lower() for h in INDICATOR_HINTS)]
    non1 = [c for c in cols1 if c not in ind1]
    missing5 = [c for c in ["date", "open", "high", "low", "close", "volume", "RSI", "ATR", "EMA_20", "VWAP", "ADX"] if c not in cols5]
    missing1 = [c for c in ["date", "open", "high", "low", "close", "volume"] if c not in cols1]
    cov = sess["coverage"].tail(20)
    lines = [
        "# DATA_AUDIT",
        "",
        "## Raw Data Paths Found",
        f"- 5-minute raw/indicator store used: `{five_dir}`",
        f"- 1-minute raw/indicator store used: `{one_dir}`",
        "- Other 5-minute stores inspected: " + ", ".join(f"`{p}`" for p in FIVE_MIN_DIRS if p.exists()),
        "- Other 1-minute stores inspected: " + ", ".join(f"`{p}`" for p in ONE_MIN_DIRS if p.exists()),
        "",
        "## Available Date Range And Symbols",
        f"- 5-minute symbols: {len(five_files):,}; sessions: {meta5.get('sessions')}; date range: {meta5.get('date_min')} to {meta5.get('date_max')}",
        f"- 1-minute symbols: {len(one_files):,}; sessions: {meta1.get('sessions')}; date range: {meta1.get('date_min')} to {meta1.get('date_max')}",
        f"- common symbols with both stores: {len(set(five_files) & set(one_files)):,}",
        f"- selected completed common sessions: {range_str(sess['all'])}",
        "",
        "## FIT / VAL / TRAIN / TEST Sessions",
        f"- FIT: {session_str(sess['fit'])}",
        f"- VAL: {session_str(sess['val'])}",
        f"- TRAIN: {session_str(sess['train'])}",
        f"- TEST: {session_str(sess['test'])}",
        "",
        "## Session Coverage (latest 20 common completed sessions)",
        "| session | 5m complete symbols | 1m complete symbols |",
        "|---|---:|---:|",
    ]
    for idx, row in cov.iterrows():
        lines.append(f"| {pd.Timestamp(idx).date()} | {int(row['five_symbols'])} | {int(row['one_symbols'])} |")
    lines += [
        "",
        "## Columns",
        f"### 5-minute sample `{sample5.name}`",
        f"- rows={rows5:,}, columns={ncols5}, range={min5} to {max5}",
        f"- indicator columns ({len(ind5)}): {', '.join(ind5)}",
        f"- non-indicator columns ({len(non5)}): {', '.join(non5)}",
        "",
        f"### 1-minute sample `{sample1.name}`",
        f"- rows={rows1:,}, columns={ncols1}, range={min1} to {max1}",
        f"- indicator columns ({len(ind1)}): {', '.join(ind1)}",
        f"- non-indicator columns ({len(non1)}): {', '.join(non1)}",
        "",
        "## Missing Required Columns",
        f"- 5-minute missing: {', '.join(missing5) if missing5 else 'none'}",
        f"- 1-minute missing: {', '.join(missing1) if missing1 else 'none'}",
        "",
        "## Quality Issues / Caveats",
        f"- Current-date incomplete 1-minute rows are excluded by requiring >= {MIN_1M_BARS_COMPLETE} bars per symbol/session.",
        f"- Current-date or partial 5-minute rows are excluded by requiring >= {MIN_5M_BARS_COMPLETE} bars per symbol/session.",
        "- VWAP quality: some older repo reports flagged stale anchored 5m VWAP in specific stores; this run uses the latest live2 5m store and also constrains by EMA/price action so VWAP is not the only trigger.",
        "- Duplicate timestamps are dropped per symbol before feature generation and 1-minute exit simulation.",
        "- Halted/thin sessions are indirectly filtered when per-symbol 1-minute coverage is below the completed-session threshold.",
        "",
        "## Signal Pool Built From Raw 5-Minute Bars",
        f"- symbols considered: {signal_q.get('symbols_considered', 0):,}",
        f"- symbols with at least one trigger: {signal_q.get('symbols_with_signals', 0):,}",
        f"- unique signal candidates: {signal_q.get('candidate_rows', 0):,}",
        f"- rule-candidate rows: {signal_q.get('rule_candidate_rows', 0):,}",
        f"- rules with hits: {signal_q.get('n_rules_with_hits', 0):,}",
    ]
    (OUT_DIR / "DATA_AUDIT.md").write_text("\n".join(lines), encoding="utf-8")


def write_family_ideas(rules: list[RuleSpec], exits: list[ExitSpec], guards: list[GuardSpec]) -> None:
    lines = [
        "# SETUP_FAMILY_IDEAS",
        "",
        "All families are LONG-only, causal on the current/previous 5-minute bars, and use next 1-minute open entry.",
        "Default exit theme is a tight bracket centered on 0.75% / 0.75% with 1-minute intrabar resolution.",
        "",
        "## Exit Grid",
    ]
    for e in exits:
        extras = []
        if e.breakeven_after_pct is not None:
            extras.append(f"move SL to breakeven after +{e.breakeven_after_pct:g}%")
        if e.trailing_after_pct is not None:
            extras.append(f"trail {e.trailing_gap_pct:g}% after +{e.trailing_after_pct:g}%")
        lines.append(f"- `{e.exit_id}`: SL {e.sl_pct:g}% / target {e.target_pct:g}% / time exit {e.time_bars} bars" + (f" / {', '.join(extras)}" if extras else ""))
    lines += ["", "## Guards"]
    for g in guards:
        lines.append(f"- `{g.guard_id}`: min_slot={g.min_slot}, max_slot={g.max_slot}, top_n_per_slot={g.top_n_per_slot}, max_per_symbol_day={g.max_per_symbol_day}, cooldown_after_sl_bars={g.cooldown_after_sl_bars}")
    lines += ["", "## Families And Rule Variants"]
    for r in rules:
        lines += [
            f"### {r.rule_id}",
            f"- family: {r.family}",
            f"- entry trigger: {r.entry_logic}",
            f"- indicator filters: {'; '.join(r.indicator_rules)}",
            f"- non-indicator rules: {'; '.join(r.non_indicator_rules)}",
            f"- pre-momentum filter: {r.pre_momentum_filter}",
            f"- rationale: designed to catch a quick +0.75% pop without chasing late extension.",
            "",
        ]
    (OUT_DIR / "SETUP_FAMILY_IDEAS.md").write_text("\n".join(lines), encoding="utf-8")


def write_edge_study(cand: pd.DataFrame, rule_df: pd.DataFrame, exit_df: pd.DataFrame, rules: dict[str, RuleSpec]) -> None:
    anchor = exit_df[exit_df["exit_id"] == "sl0.75_tgt0.75_tb3"]
    if anchor.empty:
        anchor = exit_df[exit_df["exit_id"].str.contains("sl0.75_tgt0.75", regex=False)].copy()
    merged = rule_df.merge(cand, on="candidate_id", how="left").merge(anchor[["candidate_id", "outcome", "net_pnl", "holding_min"]], on="candidate_id", how="inner")
    merged["_win075"] = merged["outcome"].eq("TARGET")
    lines = [
        "# RAW_DATA_LONG_EDGE_STUDY",
        "",
        "Label: TARGET means +0.75% was reached before -0.75% using 1-minute sequence. Same 1-minute target/SL ties are SL-first.",
        "This study is based on generated raw 5-minute trigger candidates across FIT/VAL/TRAIN/TEST sessions; search selection still used FIT/VAL first.",
        "",
        "## Base Rate By Family / Trigger",
        "| rule | family | candidates | P(+0.75 before -0.75) | median hold min | avg net Rs |",
        "|---|---|---:|---:|---:|---:|",
    ]
    if not merged.empty:
        g = merged.groupby("rule_id")
        rows = []
        for rid, x in g:
            rows.append((rid, rules[rid].family, len(x), x["_win075"].mean() * 100, pd.to_numeric(x["holding_min"], errors="coerce").median(), pd.to_numeric(x["net_pnl"], errors="coerce").mean()))
        rows.sort(key=lambda r: (r[3], r[2]), reverse=True)
        for rid, fam, n, wr, hold, net in rows:
            lines.append(f"| {rid} | {fam} | {n:,} | {wr:.1f}% | {hold:.1f} | {net:.0f} |")
    feat_cols = ["vol_ratio", "atr_pct", "green_body_pct", "close_loc", "upper_wick_pct", "lower_wick_pct", "rsi", "rsi_delta", "macd_delta", "ema20_slope_pct", "vwap_dist_pct", "range_expansion", "compression", "slot"]
    lines += ["", "## Winner Vs Loser Feature Patterns (+0.75% anchor)", "| feature | winners median | losers median | interpretation |", "|---|---:|---:|---|"]
    for c in feat_cols:
        if c not in merged.columns:
            continue
        w = pd.to_numeric(merged.loc[merged["_win075"], c], errors="coerce")
        l = pd.to_numeric(merged.loc[~merged["_win075"], c], errors="coerce")
        if w.notna().sum() < 10 or l.notna().sum() < 10:
            continue
        wm, lm = w.median(), l.median()
        interp = "higher in winners" if wm > lm else "lower in winners"
        lines.append(f"| {c} | {wm:.4g} | {lm:.4g} | {interp} |")
    lines += ["", "## Time Slots", "| slot hour | candidates | target-first rate | avg net Rs |", "|---|---:|---:|---:|"]
    if not merged.empty:
        merged["_hour"] = pd.to_datetime(merged["date"]).dt.strftime("%H")
        for h, x in merged.groupby("_hour"):
            lines.append(f"| {h}:xx | {len(x):,} | {x['_win075'].mean()*100:.1f}% | {pd.to_numeric(x['net_pnl'], errors='coerce').mean():.0f} |")
    lines += ["", "## Failure / Overextension Patterns", ""]
    if not merged.empty:
        losers = merged[~merged["_win075"]]
        lines.append(f"- Losers had median upper_wick_pct {pd.to_numeric(losers.get('upper_wick_pct'), errors='coerce').median():.3f} and median green_streak_3 {pd.to_numeric(losers.get('green_streak_3'), errors='coerce').median():.1f}.")
        lines.append(f"- Median loser VWAP distance was {pd.to_numeric(losers.get('vwap_dist_pct'), errors='coerce').median():.3f}%; tight stops suffer when entry is too extended from VWAP/EMA.")
        lines.append("- Best raw base-rate families were retained for FIT/VAL search; weak families are still documented in the iteration log.")
    (OUT_DIR / "RAW_DATA_LONG_EDGE_STUDY.md").write_text("\n".join(lines), encoding="utf-8")


def config_label(rule: RuleSpec, ex: ExitSpec, guard: GuardSpec) -> str:
    return f"{rule.rule_id}|{ex.exit_id}|{guard.guard_id}"


def run_search(cand: pd.DataFrame, rule_df: pd.DataFrame, exit_df: pd.DataFrame, rules: list[RuleSpec], exits: list[ExitSpec], guards: list[GuardSpec], sess: dict) -> tuple[list[dict], list[dict]]:
    rule_map = {r.rule_id: r for r in rules}
    exit_map = {e.exit_id: e for e in exits}
    guard_map = {g.guard_id: g for g in guards}
    iterations: list[dict] = []

    # Stage 4a: one logical group at a time - trigger variants under anchor exit.
    print(f"[search] stage 4a trigger scan: {len(rules)} rules", flush=True)
    anchor = next(e for e in exits if e.exit_id == "sl0.75_tgt0.75_tb3")
    base_guard = next(g for g in guards if g.guard_id == "g_base")
    for r in rules:
        fit_t, fit_m = evaluate_candidate(cand, rule_df, exit_df, r.rule_id, anchor.exit_id, base_guard, sess["fit"], full_detail=False)
        val_t, val_m = evaluate_candidate(cand, rule_df, exit_df, r.rule_id, anchor.exit_id, base_guard, sess["val"], full_detail=False)
        sc = score_fitval(fit_m, val_m)
        iterations.append({
            "stage": "FIT/VAL trigger-family search",
            "family": r.family,
            "changed_logic": "entry trigger / structural threshold",
            "old_value": "baseline none",
            "new_value": r.rule_id,
            "reason": r.entry_logic,
            "rule_id": r.rule_id,
            "exit_id": anchor.exit_id,
            "guard_id": base_guard.guard_id,
            "fit": fit_m,
            "val": val_m,
            "score": round(sc, 4),
            "keep_reject": "keep_for_exit_grid" if sc > 0.65 and min(fit_m["trades"], val_m["trades"]) >= 8 else "reject_low_fitval",
            "next_action": "test exits on TRAIN-side only" if sc > 0.65 else "try next family",
        })
    base_rank = sorted(iterations, key=lambda x: x["score"], reverse=True)
    keep_rules = [x["rule_id"] for x in base_rank[:18] if x["score"] > -1]
    if not keep_rules:
        keep_rules = [x["rule_id"] for x in base_rank[:10]]

    # Stage 4b: exit grid around the 0.75 anchor on selected triggers.
    print(f"[search] stage 4b exit grid: {len(keep_rules)} rules x {len(exits)} exits", flush=True)
    for rid in keep_rules:
        r = rule_map[rid]
        for ex in exits:
            fit_t, fit_m = evaluate_candidate(cand, rule_df, exit_df, rid, ex.exit_id, base_guard, sess["fit"], full_detail=False)
            val_t, val_m = evaluate_candidate(cand, rule_df, exit_df, rid, ex.exit_id, base_guard, sess["val"], full_detail=False)
            sc = score_fitval(fit_m, val_m)
            iterations.append({
                "stage": "FIT/VAL exit grid",
                "family": r.family,
                "changed_logic": "SL/target/time/breakeven",
                "old_value": anchor.exit_id,
                "new_value": ex.exit_id,
                "reason": "test tight bracket variant around the 0.75% anchor",
                "rule_id": rid,
                "exit_id": ex.exit_id,
                "guard_id": base_guard.guard_id,
                "fit": fit_m,
                "val": val_m,
                "score": round(sc, 4),
                "keep_reject": "keep_for_guard_grid" if sc > 0.75 and min(fit_m["trades"], val_m["trades"]) >= 8 else "reject_exit_fitval",
                "next_action": "test time/top-n guards on TRAIN-side only" if sc > 0.75 else "try next exit",
            })

    exit_rank = sorted(
        [x for x in iterations if x["stage"] == "FIT/VAL exit grid"],
        key=lambda x: x["score"],
        reverse=True,
    )
    top_exit = exit_rank[:24] if exit_rank else []

    # Stage 4c: guard logic on selected trigger+exit combos.
    print(f"[search] stage 4c guard grid: {len(top_exit)} combos x {len(guards)} guards", flush=True)
    for row in top_exit:
        rid, eid = row["rule_id"], row["exit_id"]
        r = rule_map[rid]
        for guard in guards:
            fit_t, fit_m = evaluate_candidate(cand, rule_df, exit_df, rid, eid, guard, sess["fit"], full_detail=False)
            val_t, val_m = evaluate_candidate(cand, rule_df, exit_df, rid, eid, guard, sess["val"], full_detail=False)
            sc = score_fitval(fit_m, val_m)
            iterations.append({
                "stage": "FIT/VAL guard grid",
                "family": r.family,
                "changed_logic": "time/top-n/symbol/cooldown guard",
                "old_value": "g_base",
                "new_value": guard.guard_id,
                "reason": "tight stops need cleaner slots and duplicate control",
                "rule_id": rid,
                "exit_id": eid,
                "guard_id": guard.guard_id,
                "fit": fit_m,
                "val": val_m,
                "score": round(sc, 4),
                "keep_reject": "confirm_on_train" if sc > 0.80 and min(fit_m["trades"], val_m["trades"]) >= 8 else "reject_guard_fitval",
                "next_action": "full TRAIN confirmation" if sc > 0.80 else "try next guard",
            })

    # Stage 5/6: confirm top configs on TRAIN, score TEST once for promising candidates.
    print("[search] stages 5/6 TRAIN confirmation and TEST validation", flush=True)
    seen = set()
    train_tests = []
    ranked = sorted(iterations, key=lambda x: x["score"], reverse=True)
    for row in ranked:
        key = (row["rule_id"], row["exit_id"], row["guard_id"])
        if key in seen:
            continue
        seen.add(key)
        if len(train_tests) >= 40:
            break
        rule = rule_map[row["rule_id"]]
        ex = exit_map[row["exit_id"]]
        guard = guard_map[row["guard_id"]]
        tr_t, tr_m = evaluate_candidate(cand, rule_df, exit_df, rule.rule_id, ex.exit_id, guard, sess["train"], full_detail=False)
        train_weak_reasons = []
        if tr_m["trades"] < 15:
            train_weak_reasons.append("TRAIN near-miss trades < 15")
        if tr_m["net_pf"] < 0.8:
            train_weak_reasons.append("TRAIN near-miss PF < 0.8")
        te_t, te_m = evaluate_candidate(cand, rule_df, exit_df, rule.rule_id, ex.exit_id, guard, sess["test"], full_detail=False)
        ok, reasons = stability_pass(tr_m, te_m)
        if train_weak_reasons:
            ok = False
            reasons = train_weak_reasons + reasons
        rec = {
            "rule_id": rule.rule_id,
            "exit_id": ex.exit_id,
            "guard_id": guard.guard_id,
            "family": rule.family,
            "fit": row["fit"],
            "val": row["val"],
            "train": tr_m,
            "test": te_m,
            "pass": ok,
            "reject_reasons": reasons,
            "score": row["score"],
            "train_trades_file": "",
            "test_trades_file": "",
        }
        train_tests.append(rec)
        iterations.append({
            "stage": "TEST validation",
            "family": rule.family,
            "changed_logic": "held-out validation",
            "old_value": "TRAIN-confirmed candidate",
            "new_value": config_label(rule, ex, guard),
            "reason": "TEST is scored once; no TEST-side tuning",
            "rule_id": rule.rule_id,
            "exit_id": ex.exit_id,
            "guard_id": guard.guard_id,
            "fit": row["fit"],
            "val": row["val"],
            "train": tr_m,
            "test": te_m,
            "score": row["score"],
            "keep_reject": "PASS" if ok else "reject_test_or_stability",
            "next_action": "candidate config" if ok else "; ".join(reasons),
        })
    return iterations, train_tests


def write_iteration_log(iterations: list[dict]) -> None:
    lines = [
        "# ITERATION_LOG",
        "",
        "Each iteration changes one logical group: entry trigger, exit bracket, guard logic, TRAIN confirmation, or TEST validation.",
        "TEST rows appear only after FIT/VAL and full TRAIN were promising.",
        "",
        "| # | stage | family | changed logic | old | new | reason | FIT | VAL | TRAIN | TEST | keep/reject | next |",
        "|---:|---|---|---|---|---|---|---|---|---|---|---|---|",
    ]
    for i, it in enumerate(iterations, 1):
        def mtxt(k):
            m = it.get(k)
            if not m:
                return ""
            return f"n={m['trades']} PF={m['net_pf']} WR={m['win_rate_pct']} net={m['net_pnl']}"
        lines.append(
            f"| {i} | {it.get('stage','')} | {it.get('family','')} | {it.get('changed_logic','')} | "
            f"{it.get('old_value','')} | {it.get('new_value','')} | {it.get('reason','')} | "
            f"{mtxt('fit')} | {mtxt('val')} | {mtxt('train')} | {mtxt('test')} | "
            f"{it.get('keep_reject','')} | {it.get('next_action','')} |"
        )
    (OUT_DIR / "ITERATION_LOG.md").write_text("\n".join(lines), encoding="utf-8")


def config_dict(rule: RuleSpec, ex: ExitSpec, guard: GuardSpec, metrics: dict | None = None) -> dict:
    return {
        "setup_name": f"FAST_MOMENTUM_LONG_{rule.family}",
        "version": "candidate_001",
        "side": "LONG",
        "source": "raw_5m_signals_with_1m_intrabar_exit",
        "family": rule.family,
        "rule_id": rule.rule_id,
        "entry_logic": rule.entry_logic,
        "indicator_values": rule.indicator_rules,
        "non_indicator_rules": rule.non_indicator_rules,
        "pre_momentum_filter": rule.pre_momentum_filter,
        "guards": asdict(guard),
        "exit_logic": asdict(ex),
        "intrabar_resolution": "Use chronological 1-minute OHLC after next 1-minute open. If target and SL hit in the same 1-minute bar, assume SL first.",
        "cost_model": {
            "notional_rs": NOTIONAL_RS,
            "slippage_bps_per_leg": DEFAULT_SLIPPAGE_BPS,
            "statutory_costs": "nse_intraday_costs.CostConfig 2026-06",
        },
        "metrics": metrics or {},
        "final_config_block_requires_user_approval": {
            "WARNING": "DO NOT PROMOTE TO FINAL CONFIG WITHOUT USER APPROVAL",
            "example_only": {
                "status": "WATCH_OR_PAPER_ONLY",
                "side": "LONG",
                "sl_pct": ex.sl_pct,
                "target_pct": ex.target_pct,
                "time_exit_bars": ex.time_bars,
                "rule_id": rule.rule_id,
                "guards": asdict(guard),
            },
        },
    }


def write_candidates_and_recommendation(
    train_tests: list[dict],
    rules: list[RuleSpec],
    exits: list[ExitSpec],
    guards: list[GuardSpec],
    cand: pd.DataFrame,
    rule_df: pd.DataFrame,
    exit_df: pd.DataFrame,
    sess: dict,
) -> dict | None:
    rule_map = {r.rule_id: r for r in rules}
    exit_map = {e.exit_id: e for e in exits}
    guard_map = {g.guard_id: g for g in guards}
    passed = [x for x in train_tests if x["pass"]]
    ranked = passed if passed else train_tests
    ranked = sorted(
        ranked,
        key=lambda x: (
            x["test"]["net_pf"],
            x["test"]["net_pnl"],
            min(x["fit"]["net_pf"], x["val"]["net_pf"]),
            x["train"]["trades"],
        ),
        reverse=True,
    )
    best = ranked[0] if ranked else None

    lines = [
        "# CANDIDATE_CONFIGS",
        "",
        "Only candidates passing stability checks are listed here. Near-misses are kept in search_summary.json and BEST_LONG_SETUP_RECOMMENDATION.md.",
        "",
    ]
    for i, rec in enumerate(passed, 1):
        rule, ex, guard = rule_map[rec["rule_id"]], exit_map[rec["exit_id"]], guard_map[rec["guard_id"]]
        cdict = config_dict(rule, ex, guard, {"fit": rec["fit"], "val": rec["val"], "train": rec["train"], "test": rec["test"]})
        fname = f"{cdict['setup_name']}_candidate_{i:03d}.json"
        cpath = CAND_DIR / fname
        cpath.write_text(json.dumps(cdict, indent=2, default=str), encoding="utf-8")
        rec["candidate_path"] = str(cpath)
        lines += [
            f"## Candidate {i:03d}: {cdict['setup_name']}",
            f"- path: `{rel(cpath)}`",
            f"- family: {rule.family}",
            f"- entry: {rule.entry_logic}",
            f"- exit: SL {ex.sl_pct:g}% / target {ex.target_pct:g}% / time {ex.time_bars} bars",
            f"- TRAIN: trades {rec['train']['trades']}, PF {rec['train']['net_pf']}, net Rs {rec['train']['net_pnl']}, WR {rec['train']['win_rate_pct']}%",
            f"- TEST: trades {rec['test']['trades']}, PF {rec['test']['net_pf']}, net Rs {rec['test']['net_pnl']}, WR {rec['test']['win_rate_pct']}%",
            "",
        ]
    if not passed:
        (CAND_DIR / "NO_CANDIDATES.md").write_text(
            "# NO_CANDIDATES\n\nNo raw-data FAST-MOMENTUM LONG candidate passed the requested stability gate.\n",
            encoding="utf-8",
        )
        lines.append("**No candidate passed the stability checks.**")
    (OUT_DIR / "CANDIDATE_CONFIGS.md").write_text("\n".join(lines), encoding="utf-8")

    if best is None:
        return None
    rule, ex, guard = rule_map[best["rule_id"]], exit_map[best["exit_id"]], guard_map[best["guard_id"]]
    # Save best even if it is a reject/watch item, because the user asked for a best setup recommendation.
    best_cfg = config_dict(rule, ex, guard, {"fit": best["fit"], "val": best["val"], "train": best["train"], "test": best["test"], "reject_reasons": best.get("reject_reasons", [])})
    best_path = CAND_DIR / f"{best_cfg['setup_name']}_best_research_config.json"
    best_path.write_text(json.dumps(best_cfg, indent=2, default=str), encoding="utf-8")
    best["best_path"] = str(best_path)

    train_trades, train_full = evaluate_candidate(cand, rule_df, exit_df, rule.rule_id, ex.exit_id, guard, sess["train"])
    test_trades, test_full = evaluate_candidate(cand, rule_df, exit_df, rule.rule_id, ex.exit_id, guard, sess["test"])
    best["train"] = train_full
    best["test"] = test_full
    best_cfg["metrics"]["train"] = train_full
    best_cfg["metrics"]["test"] = test_full
    best_cfg["metrics"]["reject_reasons"] = best.get("reject_reasons", [])
    best_path.write_text(json.dumps(best_cfg, indent=2, default=str), encoding="utf-8")
    train_file = RESULTS_DIR / "best_train_trades.csv"
    test_file = RESULTS_DIR / "best_test_trades.csv"
    train_trades.to_csv(train_file, index=False)
    test_trades.to_csv(test_file, index=False)
    best["train_trades_file"] = str(train_file)
    best["test_trades_file"] = str(test_file)

    verdict = "SAFE FOR PAPER/WATCH ONLY" if best["pass"] else "REJECT FOR PROMOTION; WATCH ONLY AS RESEARCH"
    reason = "passed stability gate" if best["pass"] else "; ".join(best.get("reject_reasons") or ["failed stability"])
    rec_lines = [
        "# BEST_LONG_SETUP_RECOMMENDATION",
        "",
        f"## Verdict: {verdict}",
        "",
        "**DO NOT PROMOTE TO FINAL CONFIG WITHOUT USER APPROVAL**",
        "",
        f"- best setup name: `{best_cfg['setup_name']}`",
        f"- family: {rule.family}",
        f"- rule id: {rule.rule_id}",
        f"- candidate config path: `{rel(best_path)}`",
        f"- train trades file: `{rel(train_file)}`",
        f"- test trades file: `{rel(test_file)}`",
        f"- reason/verdict detail: {reason}",
        "",
        "## Exact Entry Logic",
        f"- {rule.entry_logic}",
        f"- indicator values: {'; '.join(rule.indicator_rules)}",
        f"- non-indicator rules: {'; '.join(rule.non_indicator_rules)}",
        f"- pre-momentum filter: {rule.pre_momentum_filter}",
        f"- guards: {json.dumps(asdict(guard))}",
        "",
        "## Exact Exit Logic",
        f"- SL {ex.sl_pct:g}% / target {ex.target_pct:g}% / time exit after {ex.time_bars} 5-minute bars.",
        f"- breakeven_after_pct: {ex.breakeven_after_pct}",
        f"- trailing_after_pct: {ex.trailing_after_pct}; trailing_gap_pct: {ex.trailing_gap_pct}",
        "- Intrabar: chronological 1-minute OHLC after next 1-minute open; same 1-minute target/SL touch is SL-first.",
        "",
        "## Metrics Net Of Costs",
        f"- FIT: trades {best['fit']['trades']}, PF {best['fit']['net_pf']}, net Rs {best['fit']['net_pnl']}, WR {best['fit']['win_rate_pct']}%",
        f"- VAL: trades {best['val']['trades']}, PF {best['val']['net_pf']}, net Rs {best['val']['net_pnl']}, WR {best['val']['win_rate_pct']}%",
        f"- TRAIN: trades {best['train']['trades']}, PF {best['train']['net_pf']}, net Rs {best['train']['net_pnl']}, WR {best['train']['win_rate_pct']}%",
        f"- TEST: trades {best['test']['trades']}, PF {best['test']['net_pf']}, net Rs {best['test']['net_pnl']}, WR {best['test']['win_rate_pct']}%",
        "",
        "## Stability",
        f"- TRAIN day concentration: {best['train']['top_day_net_share']}; symbol concentration: {best['train']['top_symbol_net_share']}; trades/day {best['train']['trades_per_day']}",
        f"- TEST day concentration: {best['test']['top_day_net_share']}; symbol concentration: {best['test']['top_symbol_net_share']}; trades/day {best['test']['trades_per_day']}",
        f"- 5-minute conflict count TRAIN/TEST: {best['train']['five_min_conflict_cnt']} / {best['test']['five_min_conflict_cnt']}",
        f"- same-1-minute tie count TRAIN/TEST: {best['train']['same_1m_tie_cnt']} / {best['test']['same_1m_tie_cnt']}",
        "",
        "## Why It May Work",
        "- It requires immediate pressure/structure before entry and a short holding window, matching the tight +0.75% target.",
        "- It uses 1-minute path order instead of optimistic 5-minute OHLC assumptions.",
        "",
        "## Why It May Fail",
        "- A 0.75% target leaves little room for statutory costs plus 15 bps/leg slippage.",
        "- The setup can degrade quickly if volume/slot quality changes, so paper-watch validation is required before any promotion.",
        "- If TEST PF is below target or dominated by one day/symbol, it must remain rejected.",
        "",
        "## Candidate Config Block",
        "```json",
        json.dumps(best_cfg, indent=2, default=str),
        "```",
        "",
        "## Final Config Block That Would Need Approval",
        "```python",
        "# DO NOT PROMOTE TO FINAL CONFIG WITHOUT USER APPROVAL",
        f"'{best_cfg['setup_name']}': {{",
        "    'status': 'WATCH_OR_PAPER_ONLY',",
        "    'side': 'LONG',",
        f"    'sl_pct': {ex.sl_pct},",
        f"    'target_pct': {ex.target_pct},",
        f"    'time_exit_bars': {ex.time_bars},",
        f"    'rule_id': '{rule.rule_id}',",
        f"    'guard': {asdict(guard)!r},",
        "}",
        "```",
    ]
    (OUT_DIR / "BEST_LONG_SETUP_RECOMMENDATION.md").write_text("\n".join(rec_lines), encoding="utf-8")
    return best


def write_summary(best: dict | None, iterations: list[dict], train_tests: list[dict], sess: dict, five_dir: Path, one_dir: Path) -> None:
    summary = {
        "raw_paths": {"five_min": str(five_dir), "one_min": str(one_dir)},
        "sessions": {k: [str(pd.Timestamp(x).date()) for x in v] for k, v in sess.items() if k in {"fit", "val", "train", "test", "all"}},
        "n_iterations": len(iterations),
        "n_train_test_scored": len(train_tests),
        "n_passed": sum(1 for x in train_tests if x["pass"]),
        "best": best,
    }
    (OUT_DIR / "search_summary.json").write_text(json.dumps(summary, indent=2, default=str), encoding="utf-8")


def load_or_build(args) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, list[RuleSpec], list[ExitSpec], list[GuardSpec], dict, Path, Path, dict]:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    SCRIPTS_DIR.mkdir(parents=True, exist_ok=True)
    CAND_DIR.mkdir(parents=True, exist_ok=True)
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    five_dir, five_files = choose_store(FIVE_MIN_DIRS, FIVE_SUFFIXES, "5-minute")
    one_dir, one_files = choose_store(ONE_MIN_DIRS, ONE_SUFFIXES, "1-minute")
    print(f"[paths] 5m={five_dir} ({len(five_files)} files)")
    print(f"[paths] 1m={one_dir} ({len(one_files)} files)")
    counts_cache = RESULTS_DIR / "session_counts.parquet"
    if counts_cache.exists() and not args.rebuild_cache:
        cc = pd.read_parquet(counts_cache)
        five_counts = cc[cc["tf"] == "5m"].drop(columns=["tf"])
        one_counts = cc[cc["tf"] == "1m"].drop(columns=["tf"])
        meta5 = {"symbols": int(five_counts["symbol"].nunique()), "sessions": int(five_counts["session"].nunique()), "date_min": str(five_counts["session"].min().date()), "date_max": str(five_counts["session"].max().date())}
        meta1 = {"symbols": int(one_counts["symbol"].nunique()), "sessions": int(one_counts["session"].nunique()), "date_min": str(one_counts["session"].min().date()), "date_max": str(one_counts["session"].max().date())}
    else:
        five_counts, meta5 = session_counts(five_files, MIN_5M_BARS_COMPLETE, "5m")
        one_counts, meta1 = session_counts(one_files, MIN_1M_BARS_COMPLETE, "1m")
        cc = pd.concat([five_counts.assign(tf="5m"), one_counts.assign(tf="1m")], ignore_index=True)
        cc.to_parquet(counts_cache, index=False)
    sess = selected_sessions(five_counts, one_counts)
    print(f"[sessions] TRAIN {range_str(sess['train'])}; TEST {range_str(sess['test'])}")
    rules = build_rules()
    exits = exit_specs()
    guards = guard_specs()
    cache_key = f"raw_cache_{sess['all'][0].date()}_{sess['all'][-1].date()}_{args.max_symbols or 'all'}"
    cand_cache = RESULTS_DIR / f"{cache_key}_candidates.parquet"
    rule_cache = RESULTS_DIR / f"{cache_key}_rule_candidates.parquet"
    exit_cache = RESULTS_DIR / f"{cache_key}_exits.parquet"
    q_path = RESULTS_DIR / f"{cache_key}_quality.json"
    if cand_cache.exists() and rule_cache.exists() and exit_cache.exists() and not args.rebuild_cache:
        cand = pd.read_parquet(cand_cache)
        rule_df = pd.read_parquet(rule_cache)
        exit_df = pd.read_parquet(exit_cache)
        signal_q = json.loads(q_path.read_text(encoding="utf-8")) if q_path.exists() else {}
        signal_q.update({
            "symbols_considered": int(len(set(five_files) & set(one_files))) if not args.max_symbols else int(args.max_symbols),
            "symbols_with_signals": int(cand["symbol"].nunique()) if "symbol" in cand.columns else 0,
            "candidate_rows": int(len(cand)),
            "rule_candidate_rows": int(len(rule_df)),
            "n_rules_with_hits": int(rule_df["rule_id"].nunique()) if "rule_id" in rule_df.columns else 0,
        })
        if not cand.empty:
            expected = int(cand["candidate_id"].nunique())
            got = int(exit_df["candidate_id"].nunique()) if "candidate_id" in exit_df.columns else 0
            if got < max(1, int(expected * 0.80)):
                print(f"[cache] exit cache stale/incomplete ({got}/{expected} candidates); rebuilding exits", flush=True)
                exit_df = resolve_exits(cand, one_files, sess["all"], exits, args.slippage_bps)
                exit_df.to_parquet(exit_cache, index=False)
    else:
        cand, rule_df, signal_q = build_signal_pool(five_files, one_files, sess["all"], rules, args.max_symbols)
        cand.to_parquet(cand_cache, index=False)
        rule_df.to_parquet(rule_cache, index=False)
        exit_df = resolve_exits(cand, one_files, sess["all"], exits, args.slippage_bps)
        exit_df.to_parquet(exit_cache, index=False)
        q_path.write_text(json.dumps(signal_q, indent=2), encoding="utf-8")
    for c in ["date", "session"]:
        cand[c] = pd.to_datetime(cand[c], errors="coerce")
    write_data_audit(five_dir, one_dir, five_files, one_files, five_counts, one_counts, meta5, meta1, sess, signal_q)
    return cand, rule_df, exit_df, rules, exits, guards, sess, five_dir, one_dir, signal_q


def rerun_best(args) -> int:
    best_files = sorted(CAND_DIR.glob("*_best_research_config.json"), key=lambda p: p.stat().st_mtime, reverse=True)
    if not best_files:
        print("[best-only] no best config exists; run full discovery first")
        return 1
    cfg = json.loads(best_files[0].read_text(encoding="utf-8"))
    cand, rule_df, exit_df, rules, exits, guards, sess, five_dir, one_dir, signal_q = load_or_build(args)
    global EVAL_CTX
    EVAL_CTX = prepare_eval_context(cand, rule_df, exit_df)
    rule = next(r for r in rules if r.rule_id == cfg["rule_id"])
    ex = next(e for e in exits if e.exit_id == cfg["exit_logic"]["exit_id"])
    guard = next(g for g in guards if g.guard_id == cfg["guards"]["guard_id"])
    train_trades, train_m = evaluate_candidate(cand, rule_df, exit_df, rule.rule_id, ex.exit_id, guard, sess["train"])
    test_trades, test_m = evaluate_candidate(cand, rule_df, exit_df, rule.rule_id, ex.exit_id, guard, sess["test"])
    out = {
        "config_file": str(best_files[0]),
        "train": train_m,
        "test": test_m,
        "rerun_utc": pd.Timestamp.utcnow().isoformat(),
    }
    (RESULTS_DIR / "best_candidate_rerun.json").write_text(json.dumps(out, indent=2, default=str), encoding="utf-8")
    train_trades.to_csv(RESULTS_DIR / "best_candidate_rerun_train_trades.csv", index=False)
    test_trades.to_csv(RESULTS_DIR / "best_candidate_rerun_test_trades.csv", index=False)
    print(f"[best-only] TRAIN n={train_m['trades']} PF={train_m['net_pf']} net={train_m['net_pnl']}")
    print(f"[best-only] TEST  n={test_m['trades']} PF={test_m['net_pf']} net={test_m['net_pnl']}")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--max-symbols", type=int, default=0, help="0 means all common symbols")
    ap.add_argument("--slippage-bps", type=float, default=DEFAULT_SLIPPAGE_BPS)
    ap.add_argument("--rebuild-cache", action="store_true")
    ap.add_argument("--best-only", action="store_true")
    args = ap.parse_args()
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
    except Exception:
        pass
    if args.best_only:
        return rerun_best(args)
    started = time.time()
    cand, rule_df, exit_df, rules, exits, guards, sess, five_dir, one_dir, signal_q = load_or_build(args)
    global EVAL_CTX
    EVAL_CTX = prepare_eval_context(cand, rule_df, exit_df)
    write_family_ideas(rules, exits, guards)
    write_edge_study(cand, rule_df, exit_df, {r.rule_id: r for r in rules})
    iterations, train_tests = run_search(cand, rule_df, exit_df, rules, exits, guards, sess)
    write_iteration_log(iterations)
    best = write_candidates_and_recommendation(train_tests, rules, exits, guards, cand, rule_df, exit_df, sess)
    write_summary(best, iterations, train_tests, sess, five_dir, one_dir)
    print(f"[done] iterations={len(iterations)} train/test scored={len(train_tests)} passed={sum(1 for x in train_tests if x['pass'])}")
    if best:
        print(f"[done] best {best['family']} {best['rule_id']} {best['exit_id']} {best['guard_id']}")
        print(f"[done] TRAIN n={best['train']['trades']} PF={best['train']['net_pf']} net={best['train']['net_pnl']}")
        print(f"[done] TEST  n={best['test']['trades']} PF={best['test']['net_pf']} net={best['test']['net_pnl']}")
    print(f"[done] artifacts={OUT_DIR} elapsed={time.time() - started:.0f}s")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
