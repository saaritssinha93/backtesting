# -*- coding: utf-8 -*-
"""
V17r 5-min combined runner -- v17q plus research filters.

This file keeps v17q untouched and adds one extra post-scan filter layer aimed
at the full v17q realism path.  The default profile uses only signal/clock
features that are available before or at the configured entry decision.

Profiles:
  causal  (default): safer first candidate, avoids entry_bar_vol_ratio.
  volume            : stronger backtest candidate, uses entry_bar_vol_ratio;
                      keep it as research unless entry timing is made explicit.

Expected on latest v17q full CSV snapshot:
  causal : ~3541 trades, PF ~1.64, hit ~69.3%, sum pnl ~3086%
  volume : ~3499 trades, PF ~1.71, hit ~70.2%, sum pnl ~3283%

Outputs go to outputs_ID_v2_5min/.
"""
from __future__ import annotations

import os
from typing import Iterable, Tuple

import numpy as np
import pandas as pd

from . import v17q_5min as _v17q  # cascade -- pulls v17q/v17p/.../v16
from . import v16_5min as _base


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in ("1", "true", "yes", "on")


def _env_choice(name: str, default: str, choices: tuple[str, ...]) -> str:
    raw = os.environ.get(name)
    if raw is None:
        return default
    val = str(raw).strip().lower()
    return val if val in choices else default


V17R_FILTERS_ENABLED = _env_bool("EQIDV17R_FILTERS_ENABLED", True)
V17R_PROFILE = _env_choice("EQIDV17R_PROFILE", "causal", ("causal", "volume"))


# ---------------------------------------------------------------------------
# Output dir routing.
# ---------------------------------------------------------------------------
_orig_runtime_dir_v17r = _base.runtime_dir


def _v17r_runtime_dir(*parts):
    new_parts = []
    for part in parts:
        text = str(part)
        if text.startswith("outputs_"):
            text = "outputs_ID_v2_5min"
        else:
            for old in (
                "v17r_5min", "v17q_5min", "v17p_5min", "v17o_5min", "v17n_5min",
                "v17m_5min", "v17l_5min", "v17k_5min", "v17j_5min", "v17i_5min",
                "v17h_5min", "v17g_5min", "v17f_5min", "v17d_5min", "v17c_5min",
                "v17b_5min", "v16_5min",
            ):
                text = text.replace(old, "outputs_ID_v2_5min")
        new_parts.append(text)
    return _orig_runtime_dir_v17r(*tuple(new_parts))


_base.runtime_dir = _v17r_runtime_dir


def _num(df: pd.DataFrame, col: str) -> pd.Series:
    if col not in df.columns:
        return pd.Series(np.nan, index=df.index, dtype="float64")
    return pd.to_numeric(df[col], errors="coerce")


def _drop_rule(
    df: pd.DataFrame,
    setup: str,
    col: str,
    op: str,
    threshold: float,
) -> tuple[pd.Series, str]:
    if "setup" not in df.columns:
        print(f"[V17R_WARN] rule '{setup} {col} {op} {threshold:g}' skipped -- df has no 'setup' column")
        return pd.Series(False, index=df.index), f"{setup}: skipped (no setup col)"
    if col not in df.columns:
        print(f"[V17R_WARN] rule '{setup} {col} {op} {threshold:g}' skipped -- column '{col}' missing from df")
        return pd.Series(False, index=df.index), f"{setup}: skipped (missing {col})"

    setup_norm = df["setup"].astype(str).str.upper().str.strip()
    in_setup = setup_norm.eq(setup)
    n_setup = int(in_setup.sum())
    if n_setup == 0:
        print(f"[V17R_WARN] rule '{setup} {col} {op} {threshold:g}' matched ZERO rows -- setup name may have been renamed")
        return pd.Series(False, index=df.index), f"{setup}: 0 rows matched"

    vals = _num(df, col)
    n_numeric = int((in_setup & vals.notna()).sum())
    if n_numeric == 0:
        print(f"[V17R_WARN] rule '{setup} {col} {op} {threshold:g}' -- {n_setup} setup rows but column '{col}' is all-NaN for them")
        return pd.Series(False, index=df.index), f"{setup}: {col} all-NaN"

    if op == "<":
        mask = in_setup & (vals < threshold)
    elif op == ">":
        mask = in_setup & (vals > threshold)
    else:
        raise ValueError(f"Unsupported rule op: {op!r}")
    return mask.fillna(False), f"{setup}: {col} {op} {threshold:g}"


def _apply_rules(df: pd.DataFrame, side_label: str, rules: Iterable[tuple[str, str, str, float]]) -> pd.DataFrame:
    if df is None or df.empty or not V17R_FILTERS_ENABLED:
        return df

    work = df.copy()
    drop = pd.Series(False, index=work.index)
    details: list[str] = []

    for setup, col, op, threshold in rules:
        mask, label = _drop_rule(work, setup, col, op, threshold)
        n = int(mask.sum())
        if n:
            details.append(f"-{n} {label}")
        drop = drop | mask

    before = len(work)
    work = work.loc[~drop].copy()
    joined = ", ".join(details) if details else "no drops"
    print(f"[V17R_{V17R_PROFILE.upper()}] {side_label} research filters: {before}->{len(work)} ({joined})")
    return work


# Rules mined on latest full v17q output with post-Feb validation.
CAUSAL_LONG_RULES = (
    ("G_HIGHER_HIGH_BREAK", "quality_score", "<", 2.257),
    ("C_OR_BREAKOUT", "quality_score", "<", 1.485),
    ("A_MOD_BREAK_C1_HIGH", "avwap_dist_atr_signal", "<", 1.539),
)

CAUSAL_SHORT_RULES = (
    ("G_LOWER_LOW_BREAK", "ema20_gap_atr_signal", ">", 2.944),
)

VOLUME_LONG_RULES = (
    ("G_HIGHER_HIGH_BREAK", "entry_bar_vol_ratio", ">", 2.951),
    ("G_HIGHER_HIGH_BREAK", "bars_from_open", ">", 21.0),
    ("C_OR_BREAKOUT", "quality_score", "<", 1.485),
    ("A_MOD_BREAK_C1_HIGH", "avwap_dist_atr_signal", "<", 1.539),
)

VOLUME_SHORT_RULES = (
    ("G_LOWER_LOW_BREAK", "stochk_signal", "<", 38.5),
)


if V17R_FILTERS_ENABLED:
    _v17q_post_scan_chain = _base._apply_v16_post_scan_filters

    def _v17r_apply_post_scan_filters(
        short_df: pd.DataFrame,
        long_df: pd.DataFrame,
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        short_df, long_df = _v17q_post_scan_chain(short_df, long_df)

        if V17R_PROFILE == "volume":
            long_df = _apply_rules(long_df, "LONG", VOLUME_LONG_RULES)
            short_df = _apply_rules(short_df, "SHORT", VOLUME_SHORT_RULES)
        else:
            long_df = _apply_rules(long_df, "LONG", CAUSAL_LONG_RULES)
            short_df = _apply_rules(short_df, "SHORT", CAUSAL_SHORT_RULES)

        return short_df, long_df

    _base._apply_v16_post_scan_filters = _v17r_apply_post_scan_filters


def _enabled_toggles() -> list[str]:
    flags = []
    if V17R_FILTERS_ENABLED:
        flags.append(f"FILTERS={V17R_PROFILE}")
    else:
        flags.append("FILTERS=off")
    return flags


if __name__ == "__main__":
    print("=" * 78)
    print("V17r 5-min runner -- v17q plus research filters")
    print("  Output dir: outputs_ID_v2_5min")
    print(f"  Active V17R toggles: {', '.join(_enabled_toggles())}")
    print("  Inherits v17q / v17p / v17o / ... / v16 behavior")
    print("=" * 78)
    _base.main()
