"""
V17 research framework — core utilities.
Modifies nothing in the existing pipeline. Reads from saved CSVs + 5-min parquets.
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
# Paths — known-good locations from pipeline memory
# ---------------------------------------------------------------------------
OUTPUT_ROOT = Path(r"C:\TradingData\eqidv2")
BARS_5M_DIR = OUTPUT_ROOT / "stocks_indicators_5min_eq_live2"

V17_TRADE_CSVS: Dict[str, Path] = {
    "v17b": OUTPUT_ROOT / "outputs_v17b_5min" / "avwap_longshort_trades_v16_5min_ALL_DAYS_20260418_202805.csv",
    "v17c": OUTPUT_ROOT / "outputs_v17c_5min" / "avwap_longshort_trades_v16_5min_ALL_DAYS_20260418_210236.csv",
    "v17d": OUTPUT_ROOT / "outputs_v17d_5min" / "avwap_longshort_trades_v16_5min_ALL_DAYS_20260418_225430.csv",
    "v17e": OUTPUT_ROOT / "outputs_v17e_5min" / "avwap_longshort_trades_v16_5min_ALL_DAYS_20260419_144355.csv",
    "v17f": OUTPUT_ROOT / "outputs_v17f_5min" / "avwap_longshort_trades_v16_5min_ALL_DAYS_20260419_125208.csv",
}

# Baseline cost model (matches v16/v17 runner constants):
#   slippage 0.05% + commission 0.03% per side → 0.16% round-trip per unit notional
#   Reported pnl_pct is leveraged (5x), so notional round-trip cost = 0.16% ≈ 0.80% on position
BASELINE_RT_COST_PCT = 0.80  # applied to "position-level" pnl. pnl_pct col is already net.
LEVERAGE = 5.0
STOP_EXTRA_SLIP_BPS = 3.0

# ---------------------------------------------------------------------------
# Trade loading
# ---------------------------------------------------------------------------
def load_trades(csv_path: Path) -> pd.DataFrame:
    df = pd.read_csv(csv_path)
    for col in ("signal_time_ist", "entry_time_ist", "exit_time_ist", "trade_date"):
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce", utc=False)
    df["side"] = df["side"].astype(str).str.upper()
    df["outcome"] = df["outcome"].astype(str).str.upper()
    df["entry_hour"] = df["entry_time_ist"].dt.hour
    df["entry_minute"] = df["entry_time_ist"].dt.hour * 60 + df["entry_time_ist"].dt.minute
    df["trade_day"] = df["entry_time_ist"].dt.normalize()
    return df


def load_all_v17() -> Dict[str, pd.DataFrame]:
    return {name: load_trades(p) for name, p in V17_TRADE_CSVS.items() if p.exists()}


# ---------------------------------------------------------------------------
# Metrics
# ---------------------------------------------------------------------------
@dataclass
class Metrics:
    n_trades: int
    n_days: int
    trades_per_day: float
    n_long: int
    n_short: int
    win_rate: float
    sl_rate: float
    eod_rate: float
    avg_pnl_pct: float
    sum_pnl_pct: float
    profit_factor: float
    max_drawdown_pct: float
    sharpe_ann: float
    sortino_ann: float
    calmar: float
    expectancy_pct: float
    # per-leg
    long_pf: float = 0.0
    long_sum_pnl: float = 0.0
    long_win_rate: float = 0.0
    long_n: int = 0
    short_pf: float = 0.0
    short_sum_pnl: float = 0.0
    short_win_rate: float = 0.0
    short_n: int = 0
    long_maxdd: float = 0.0
    short_maxdd: float = 0.0

    def as_row(self) -> Dict:
        return self.__dict__


def _pf(pnls: pd.Series) -> float:
    gains = pnls[pnls > 0].sum()
    losses = -pnls[pnls < 0].sum()
    if losses <= 0:
        return float("inf") if gains > 0 else 0.0
    return float(gains / losses)


def _maxdd(pnls: pd.Series) -> float:
    """Max drawdown in pnl_pct units (additive equity curve)."""
    if len(pnls) == 0:
        return 0.0
    equity = pnls.cumsum()
    peak = equity.cummax()
    dd = peak - equity
    return float(dd.max())


def _sharpe_sortino(daily_pnl: pd.Series) -> Tuple[float, float]:
    if len(daily_pnl) < 2:
        return 0.0, 0.0
    mu = daily_pnl.mean()
    sd = daily_pnl.std(ddof=1)
    if sd <= 0:
        sharpe = 0.0
    else:
        sharpe = (mu / sd) * math.sqrt(252)
    downside = daily_pnl[daily_pnl < 0]
    if len(downside) < 2:
        sortino = float("inf") if mu > 0 else 0.0
    else:
        dsd = downside.std(ddof=1)
        sortino = (mu / dsd) * math.sqrt(252) if dsd > 0 else 0.0
    return float(sharpe), float(sortino)


def compute_metrics(trades: pd.DataFrame, pnl_col: str = "pnl_pct") -> Metrics:
    if trades.empty:
        return Metrics(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
    pnls = trades[pnl_col].astype(float)
    n = len(trades)
    days = trades["trade_day"].nunique()
    outcome = trades["outcome"].astype(str).str.upper()
    n_target = int((outcome == "TARGET").sum())
    n_sl = int((outcome == "SL").sum())
    n_eod = int((outcome == "EOD").sum())

    daily = trades.groupby("trade_day")[pnl_col].sum()
    sharpe, sortino = _sharpe_sortino(daily)
    # Use CSV-native row order for DD (matches production runner's canonical equity curve).
    mdd = _maxdd(trades[pnl_col])

    longs = trades[trades["side"] == "LONG"]
    shorts = trades[trades["side"] == "SHORT"]

    expectancy = float(pnls.mean()) if n else 0.0
    calmar = (pnls.sum() / mdd) if mdd > 0 else 0.0

    return Metrics(
        n_trades=n,
        n_days=int(days),
        trades_per_day=float(n / days) if days else 0.0,
        n_long=int(len(longs)),
        n_short=int(len(shorts)),
        win_rate=100.0 * n_target / n if n else 0.0,
        sl_rate=100.0 * n_sl / n if n else 0.0,
        eod_rate=100.0 * n_eod / n if n else 0.0,
        avg_pnl_pct=float(pnls.mean()),
        sum_pnl_pct=float(pnls.sum()),
        profit_factor=_pf(pnls),
        max_drawdown_pct=mdd,
        sharpe_ann=sharpe,
        sortino_ann=sortino,
        calmar=float(calmar),
        expectancy_pct=expectancy,
        long_pf=_pf(longs[pnl_col]) if len(longs) else 0.0,
        long_sum_pnl=float(longs[pnl_col].sum()) if len(longs) else 0.0,
        long_win_rate=100.0 * (longs["outcome"] == "TARGET").sum() / max(1, len(longs)),
        long_n=int(len(longs)),
        short_pf=_pf(shorts[pnl_col]) if len(shorts) else 0.0,
        short_sum_pnl=float(shorts[pnl_col].sum()) if len(shorts) else 0.0,
        short_win_rate=100.0 * (shorts["outcome"] == "TARGET").sum() / max(1, len(shorts)),
        short_n=int(len(shorts)),
        long_maxdd=_maxdd(longs[pnl_col]) if len(longs) else 0.0,
        short_maxdd=_maxdd(shorts[pnl_col]) if len(shorts) else 0.0,
    )


# ---------------------------------------------------------------------------
# Ranking
# ---------------------------------------------------------------------------
def rank_variants(df: pd.DataFrame, weights: Optional[Dict[str, float]] = None) -> pd.DataFrame:
    """
    Balanced rank across: profit_factor (+), sum_pnl_pct (+), n_trades (+),
    max_drawdown_pct (-), win_rate (+), short_n (+).
    Z-score each, weight, sum.
    """
    if weights is None:
        weights = {
            "profit_factor": 1.5,
            "sum_pnl_pct": 1.5,
            "n_trades": 0.6,
            "max_drawdown_pct": -1.3,
            "win_rate": 0.8,
            "short_n": 0.6,
            "short_pf": 0.8,
            "trades_per_day": 0.5,
        }
    d = df.copy()
    score = pd.Series(0.0, index=d.index)
    for col, w in weights.items():
        if col not in d.columns:
            continue
        vals = pd.to_numeric(d[col], errors="coerce").replace([np.inf, -np.inf], np.nan)
        mu = vals.mean(skipna=True)
        sd = vals.std(ddof=0, skipna=True)
        if not sd or math.isnan(sd) or sd == 0:
            continue
        z = (vals - mu) / sd
        score = score.add(w * z.fillna(0), fill_value=0)
    d["balance_score"] = score
    return d.sort_values("balance_score", ascending=False).reset_index(drop=True)
