# -*- coding: utf-8 -*-
"""
V17e 5-min combined runner - beats v17b / v17c / v17d by surgically removing
SHORT bleeder pockets from the v17d expansion.

Design
------
Inherit v17d's entire setup (long core = v17c, short upstream relaxation,
v16 + v17d post-scan cleanup). On top of that, v17e adds a second cleanup
pass derived from v17d's trade distribution:

  v17d SHORT edge map (294 trades, PF 1.65):
    - SHORT_ONLY|A_MOD          : 128 trades PF 1.25  <- weak mode
    - RSI 20-25                 :  60 trades PF 0.97  <- bleeder
    - AVWAP dist >= 2.0         :  17 trades PF 0.89  <- exhausted
    - ADX 45-50                 :  34 trades PF 1.09  <- weak chop
    - RS  -0.25..0              :   4 trades PF 0.23  <- wrong context

v17e removes those and keeps the winning pockets:
    - BOTH mode (PF 2.06)
    - RS -1.5..-1 (PF 2.78)
    - ATR% 0.4-0.5 (PF 2.95) and 0.6-0.7 (PF 2.76)
    - ADX 31-35 (PF 2.45)
    - AVWAP 0.25-0.75 (PF 2.26-3.33)

All knobs are env-var tunable. LONG logic is identical to v17c.

Outputs go to outputs_v17e_5min/.
"""
from __future__ import annotations

import os
from typing import Tuple

import numpy as np
import pandas as pd

import avwap_combined_runner_v17d_5min as _v17d
import avwap_combined_runner_v17c_5min as _v17c
import avwap_combined_runner_v17b_5min as _v17b
import avwap_combined_runner_v16_5min as _base


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or str(raw).strip() == "":
        return float(default)
    try:
        return float(raw)
    except (TypeError, ValueError):
        return float(default)


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return bool(default)
    text = str(raw).strip().lower()
    if text in {"1", "true", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "no", "n", "off"}:
        return False
    return bool(default)


# ---------------------------------------------------------------------------
# v17e-specific SHORT bleeder filters (applied AFTER v17d's cleanup)
# ---------------------------------------------------------------------------
V17E_DROP_RSI_OVERSOLD_ENABLED = _env_bool("EQIDV17E_DROP_RSI_OVERSOLD", True)
V17E_RSI_OVERSOLD_LO = _env_float("EQIDV17E_RSI_OVERSOLD_LO", 20.0)
V17E_RSI_OVERSOLD_HI = _env_float("EQIDV17E_RSI_OVERSOLD_HI", 25.0)

V17E_TIGHTEN_AVWAP_CAP_ENABLED = _env_bool("EQIDV17E_TIGHTEN_AVWAP_CAP", True)
V17E_AVWAP_HARD_CAP = _env_float("EQIDV17E_AVWAP_HARD_CAP", 2.00)

V17E_DROP_ADX_WEAK_HIGH_ENABLED = _env_bool("EQIDV17E_DROP_ADX_WEAK_HIGH", True)
V17E_ADX_WEAK_LO = _env_float("EQIDV17E_ADX_WEAK_LO", 45.0)
V17E_ADX_WEAK_HI = _env_float("EQIDV17E_ADX_WEAK_HI", 50.0)

V17E_SHORTONLY_STRICT_RS_ENABLED = _env_bool(
    "EQIDV17E_SHORTONLY_STRICT_RS", True
)
V17E_SHORTONLY_RS_MAX = _env_float("EQIDV17E_SHORTONLY_RS_MAX", -0.75)

V17E_DROP_NEUTRAL_RS_ENABLED = _env_bool("EQIDV17E_DROP_NEUTRAL_RS", True)
V17E_NEUTRAL_RS_LO = _env_float("EQIDV17E_NEUTRAL_RS_LO", -0.25)
V17E_NEUTRAL_RS_HI = _env_float("EQIDV17E_NEUTRAL_RS_HI", 0.00)

V17E_TIGHTEN_EXTREME_RS_ENABLED = _env_bool(
    "EQIDV17E_TIGHTEN_EXTREME_RS", True
)
V17E_EXTREME_RS_CAP = _env_float("EQIDV17E_EXTREME_RS_CAP", -1.80)


# ---------------------------------------------------------------------------
# PATCH 1: route outputs to outputs_v17e_5min
# ---------------------------------------------------------------------------
_orig_runtime_dir = _v17c._orig_runtime_dir


def _v17e_runtime_dir(*parts):
    new_parts = []
    for part in parts:
        text = str(part)
        for old in (
            "v17e_5min",
            "v17d_5min",
            "v17c_5min",
            "v17b_5min",
            "v16_5min",
        ):
            text = text.replace(old, "v17e_5min")
        new_parts.append(text)
    return _orig_runtime_dir(*tuple(new_parts))


_base.runtime_dir = _v17e_runtime_dir


# ---------------------------------------------------------------------------
# PATCH 2: inherit v17d upstream short expansion exactly; tighten extreme RS
# ---------------------------------------------------------------------------
# v17d already installed: FINAL_SHORT_SIGNAL_WINDOWS, V15_SHORT_ENTRY_CUTOFF,
# NIFTY_RS_BOTH_MODE_THRESHOLD_SHORT_PCT, V15_SHORT_SIGNAL_AVWAP_DIST_ATR_MAX,
# _base.scan_short_prepared -> _v17d_scan_short_prepared.
#
# v17e only rewraps the extreme-RS cap (pass-through on everything else).
if V17E_TIGHTEN_EXTREME_RS_ENABLED:
    _v17d.V17D_EXTREME_RS_CAP = float(V17E_EXTREME_RS_CAP)


# ---------------------------------------------------------------------------
# PATCH 3: second cleanup pass — bleeder pockets only
# ---------------------------------------------------------------------------
_v17d_apply_post_scan_filters = _v17d._v17d_apply_post_scan_filters
_v17d_get_filter_reason = _v17d._v17d_get_filter_reason


def _v17e_apply_post_scan_filters(
    short_df: pd.DataFrame,
    long_df: pd.DataFrame,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    short_df, long_df = _v17d_apply_post_scan_filters(short_df, long_df)

    short_before_v17e = len(short_df)
    rsi_oversold_removed = 0
    avwap_cap_removed = 0
    adx_weak_high_removed = 0
    shortonly_rs_removed = 0
    neutral_rs_removed = 0

    if not short_df.empty:
        work = short_df.copy()

        mode = (
            work["nifty_context_mode"].astype(str).str.upper().str.strip()
            if "nifty_context_mode" in work.columns
            else pd.Series("", index=work.index)
        )
        rs = (
            pd.to_numeric(work["nifty_rel_strength_pct"], errors="coerce")
            if "nifty_rel_strength_pct" in work.columns
            else pd.Series(np.nan, index=work.index)
        )
        adx = (
            pd.to_numeric(work["adx_signal"], errors="coerce")
            if "adx_signal" in work.columns
            else pd.Series(np.nan, index=work.index)
        )
        avwap = (
            pd.to_numeric(work["avwap_dist_atr_signal"], errors="coerce")
            if "avwap_dist_atr_signal" in work.columns
            else pd.Series(np.nan, index=work.index)
        )
        rsi = None
        for col in ("rsi_signal", "rsi", "rsi_entry"):
            if col in work.columns:
                rsi = pd.to_numeric(work[col], errors="coerce")
                break
        if rsi is None:
            rsi = pd.Series(np.nan, index=work.index)

        def _reduce(mask: pd.Series) -> int:
            nonlocal work, mode, rs, adx, avwap, rsi
            mask = mask.fillna(False)
            removed = int(mask.sum())
            if removed == 0:
                return 0
            work = work.loc[~mask].copy()
            mode = mode.loc[work.index]
            rs = rs.loc[work.index]
            adx = adx.loc[work.index]
            avwap = avwap.loc[work.index]
            rsi = rsi.loc[work.index]
            return removed

        if V17E_DROP_RSI_OVERSOLD_ENABLED:
            mask = rsi.ge(V17E_RSI_OVERSOLD_LO) & rsi.lt(V17E_RSI_OVERSOLD_HI)
            rsi_oversold_removed = _reduce(mask)

        if V17E_TIGHTEN_AVWAP_CAP_ENABLED:
            mask = avwap.ge(V17E_AVWAP_HARD_CAP)
            avwap_cap_removed = _reduce(mask)

        if V17E_DROP_ADX_WEAK_HIGH_ENABLED:
            mask = adx.ge(V17E_ADX_WEAK_LO) & adx.lt(V17E_ADX_WEAK_HI)
            adx_weak_high_removed = _reduce(mask)

        if V17E_SHORTONLY_STRICT_RS_ENABLED:
            mask = mode.eq("SHORT_ONLY") & rs.gt(V17E_SHORTONLY_RS_MAX)
            shortonly_rs_removed = _reduce(mask)

        if V17E_DROP_NEUTRAL_RS_ENABLED:
            mask = rs.ge(V17E_NEUTRAL_RS_LO) & rs.lt(V17E_NEUTRAL_RS_HI)
            neutral_rs_removed = _reduce(mask)

        short_df = work

    print(
        "[V17E_FILTER] SHORT: {before}->{after} "
        "(-{ro} rsi[{rlo:.0f},{rhi:.0f}) | -{av} avwap>={acap:.2f} | "
        "-{ax} adx[{alo:.0f},{ahi:.0f}) | -{so} SHORT_ONLY rs>{srs:.2f}% | "
        "-{nr} rs[{nlo:.2f},{nhi:.2f}))".format(
            before=short_before_v17e,
            after=len(short_df),
            ro=rsi_oversold_removed,
            rlo=V17E_RSI_OVERSOLD_LO,
            rhi=V17E_RSI_OVERSOLD_HI,
            av=avwap_cap_removed,
            acap=V17E_AVWAP_HARD_CAP,
            ax=adx_weak_high_removed,
            alo=V17E_ADX_WEAK_LO,
            ahi=V17E_ADX_WEAK_HI,
            so=shortonly_rs_removed,
            srs=V17E_SHORTONLY_RS_MAX,
            nr=neutral_rs_removed,
            nlo=V17E_NEUTRAL_RS_LO,
            nhi=V17E_NEUTRAL_RS_HI,
        )
    )
    return short_df, long_df


def _v17e_get_filter_reason(row: dict, side: str):
    reason = _v17d_get_filter_reason(row, side)
    if reason is not None:
        return reason

    side_u = str(side).upper().strip()
    if side_u != "SHORT":
        return None

    try:
        rsi = float(row.get("rsi_signal", row.get("rsi", row.get("rsi_entry", float("nan")))))
    except (TypeError, ValueError):
        rsi = float("nan")
    try:
        avwap = float(row.get("avwap_dist_atr_signal", float("nan")))
    except (TypeError, ValueError):
        avwap = float("nan")
    try:
        adx = float(row.get("adx_signal", float("nan")))
    except (TypeError, ValueError):
        adx = float("nan")
    try:
        rs = float(row.get("nifty_rel_strength_pct", float("nan")))
    except (TypeError, ValueError):
        rs = float("nan")
    mode = str(row.get("nifty_context_mode", "")).upper().strip()

    if (
        V17E_DROP_RSI_OVERSOLD_ENABLED
        and np.isfinite(rsi)
        and V17E_RSI_OVERSOLD_LO <= rsi < V17E_RSI_OVERSOLD_HI
    ):
        return (
            f"v17e short cleanup: rsi={rsi:.1f} in "
            f"[{V17E_RSI_OVERSOLD_LO:.0f},{V17E_RSI_OVERSOLD_HI:.0f})"
        )

    if (
        V17E_TIGHTEN_AVWAP_CAP_ENABLED
        and np.isfinite(avwap)
        and avwap >= V17E_AVWAP_HARD_CAP
    ):
        return f"v17e short cleanup: avwap_dist_atr={avwap:.2f} >= {V17E_AVWAP_HARD_CAP:.2f}"

    if (
        V17E_DROP_ADX_WEAK_HIGH_ENABLED
        and np.isfinite(adx)
        and V17E_ADX_WEAK_LO <= adx < V17E_ADX_WEAK_HI
    ):
        return (
            f"v17e short cleanup: adx={adx:.1f} in "
            f"[{V17E_ADX_WEAK_LO:.0f},{V17E_ADX_WEAK_HI:.0f})"
        )

    if (
        V17E_SHORTONLY_STRICT_RS_ENABLED
        and mode == "SHORT_ONLY"
        and np.isfinite(rs)
        and rs > V17E_SHORTONLY_RS_MAX
    ):
        return (
            f"v17e short cleanup: SHORT_ONLY requires rs<={V17E_SHORTONLY_RS_MAX:.2f}% "
            f"(got {rs:.2f}%)"
        )

    if (
        V17E_DROP_NEUTRAL_RS_ENABLED
        and np.isfinite(rs)
        and V17E_NEUTRAL_RS_LO <= rs < V17E_NEUTRAL_RS_HI
    ):
        return (
            f"v17e short cleanup: rs={rs:.2f}% in "
            f"[{V17E_NEUTRAL_RS_LO:.2f},{V17E_NEUTRAL_RS_HI:.2f})"
        )

    return None


_base._apply_v16_post_scan_filters = _v17e_apply_post_scan_filters
_base.get_v16_filter_reason = _v17e_get_filter_reason


if __name__ == "__main__":
    print("=" * 76)
    print("V17e 5-min runner: v17d base + surgical SHORT bleeder removal")
    print("  LONG: unchanged from v17c")
    print("  SHORT upstream: inherited from v17d (cutoff=14:00, BOTH RS<=-0.60%)")
    print(
        "  SHORT v17e-only filters:"
    )
    print(
        f"    drop_rsi[{V17E_RSI_OVERSOLD_LO:.0f},{V17E_RSI_OVERSOLD_HI:.0f})="
        f"{V17E_DROP_RSI_OVERSOLD_ENABLED} | "
        f"hard_avwap_cap={V17E_AVWAP_HARD_CAP:.2f} ({V17E_TIGHTEN_AVWAP_CAP_ENABLED}) | "
        f"drop_adx[{V17E_ADX_WEAK_LO:.0f},{V17E_ADX_WEAK_HI:.0f})="
        f"{V17E_DROP_ADX_WEAK_HIGH_ENABLED}"
    )
    print(
        f"    SHORT_ONLY rs<={V17E_SHORTONLY_RS_MAX:.2f}%={V17E_SHORTONLY_STRICT_RS_ENABLED} | "
        f"drop_rs[{V17E_NEUTRAL_RS_LO:.2f},{V17E_NEUTRAL_RS_HI:.2f})={V17E_DROP_NEUTRAL_RS_ENABLED} | "
        f"extreme_rs_cap={V17E_EXTREME_RS_CAP:.2f}%"
    )
    print("  Output dir: outputs_v17e_5min")
    print("=" * 76)
    _base.main()
