"""
V17 research framework — filter variants.
Each filter takes a trade DataFrame and returns a filtered view.
Used for drop-one / add-one / session / quality / regime experiments.
"""
from __future__ import annotations

from typing import Callable, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

FilterFn = Callable[[pd.DataFrame], pd.DataFrame]


# ---------------------------------------------------------------------------
# Individual filter primitives (pure subset selection, no re-simulation)
# ---------------------------------------------------------------------------
def keep_side(df: pd.DataFrame, side: str) -> pd.DataFrame:
    return df[df["side"] == side.upper()].copy()


def drop_entry_window(df: pd.DataFrame, start_min: int, end_min: int, side: Optional[str] = None) -> pd.DataFrame:
    mask = (df["entry_minute"] >= start_min) & (df["entry_minute"] < end_min)
    if side:
        mask &= df["side"] == side.upper()
    return df[~mask].copy()


def keep_entry_window(df: pd.DataFrame, start_min: int, end_min: int) -> pd.DataFrame:
    mask = (df["entry_minute"] >= start_min) & (df["entry_minute"] < end_min)
    return df[mask].copy()


def drop_by_col_range(
    df: pd.DataFrame, col: str, lo: float, hi: float, side: Optional[str] = None
) -> pd.DataFrame:
    if col not in df.columns:
        return df
    vals = pd.to_numeric(df[col], errors="coerce")
    mask = (vals >= lo) & (vals < hi)
    if side:
        mask &= df["side"] == side.upper()
    return df[~mask].copy()


def keep_by_col_min(df: pd.DataFrame, col: str, thresh: float, side: Optional[str] = None) -> pd.DataFrame:
    if col not in df.columns:
        return df
    vals = pd.to_numeric(df[col], errors="coerce")
    mask = vals >= thresh
    if side:
        mask |= df["side"] != side.upper()  # only apply to specified side
    return df[mask].copy()


def drop_by_col_min(df: pd.DataFrame, col: str, thresh: float, side: Optional[str] = None) -> pd.DataFrame:
    if col not in df.columns:
        return df
    vals = pd.to_numeric(df[col], errors="coerce")
    mask = vals >= thresh
    if side:
        mask &= df["side"] == side.upper()
    return df[~mask].copy()


def drop_setup(df: pd.DataFrame, setup_name: str, side: Optional[str] = None) -> pd.DataFrame:
    mask = df["setup"].astype(str).str.upper() == setup_name.upper()
    if side:
        mask &= df["side"] == side.upper()
    return df[~mask].copy()


def keep_mode(df: pd.DataFrame, modes: List[str], side: Optional[str] = None) -> pd.DataFrame:
    modes_u = [m.upper() for m in modes]
    mask = df["nifty_context_mode"].astype(str).str.upper().isin(modes_u)
    if side:
        mask |= df["side"] != side.upper()
    return df[mask].copy()


def topn_per_day(df: pd.DataFrame, n: int, rank_col: str = "quality_score") -> pd.DataFrame:
    if rank_col not in df.columns:
        return df
    ranked = df.copy()
    ranked["_rk"] = ranked.groupby("trade_day")[rank_col].rank(method="first", ascending=False)
    return ranked[ranked["_rk"] <= n].drop(columns=["_rk"])


def vix_regime(df: pd.DataFrame, vix_max: float, side: Optional[str] = None) -> pd.DataFrame:
    if "india_vix" not in df.columns:
        return df
    v = pd.to_numeric(df["india_vix"], errors="coerce")
    mask = v <= vix_max
    if side:
        mask |= df["side"] != side.upper()
    return df[mask].copy()


# ---------------------------------------------------------------------------
# Composed filter recipes (named variants)
# ---------------------------------------------------------------------------
def apply_filters(df: pd.DataFrame, ops: List[Tuple[str, Dict]]) -> pd.DataFrame:
    """ops: list of (name, kwargs) — name must be a function below."""
    d = df
    for name, kwargs in ops:
        fn = FILTER_REGISTRY.get(name)
        if fn is None:
            raise ValueError(f"unknown filter {name}")
        d = fn(d, **kwargs)
    return d


FILTER_REGISTRY: Dict[str, FilterFn] = {
    "keep_side": keep_side,
    "drop_entry_window": drop_entry_window,
    "keep_entry_window": keep_entry_window,
    "drop_by_col_range": drop_by_col_range,
    "keep_by_col_min": keep_by_col_min,
    "drop_by_col_min": drop_by_col_min,
    "drop_setup": drop_setup,
    "keep_mode": keep_mode,
    "topn_per_day": topn_per_day,
    "vix_regime": vix_regime,
}
