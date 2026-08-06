"""Runtime activation of the configured setup book into the LIVE v7 pipeline.

Single source of truth = the module selected by EQIDV2_FINAL_SETUP_CONF_MODULE.
When EQIDV2_USE_FINAL_SETUP_CONF is truthy, the live scanner and 1-minute entry
engine call into this module to push the conf's setups / masks / pre-momentum
gates / exit LEVELS into the *existing* v7 globals, while every filter stays
exactly where v7 has always applied it:

    scanner  : candidate_scan.ALLOWED_SETUPS (detection whitelist)
               apply_v8_live_gate           -> native setups pass normally
               apply_research_live_filters  -> native setups pass normally
               readmit provenance setups    -> bypass v8+research like v11
               apply_conf_gate              -> conf mask + entry_guards final gate
    engine   : PRE_ENTRY_MOMENTUM_SETUP_GATES / _SHADOW_SETUPS (momentum gate)
               v6.SETUP_EXIT_RULES          -> per-setup SL/target LEVELS

This mirrors avwap_5min_ID_v11_backtesting.py::_activate_final_setup_conf so the
live entries match the v11 backtest's entries (same setups, masks, momentum gates,
exit levels) for the natively-detected setups and the explicit readmit groups.

EXIT MECHANISM IS UNTOUCHED. The executor still places real market SL/target
orders; only the per-setup SL/target *levels* are sourced from the conf (the
entry engine writes stop_price/target_price into the signal CSV from
v6.SETUP_EXIT_RULES, and the executor reads those prices).

Default OFF: with the flag unset, every function here is a no-op / not referenced,
so live behaviour is identical to before. This module performs NO module-level
side effects and never connects to a broker; it only reads final_setup_conf and
returns/installs plain Python values.

Tier-C setups (L_RS_LEADER_VWAP_HOLD, P_PDH_BREAK_RETEST_LONG,
E_ORB_RETEST_HOLD_LONG, V_RECLAIM_PULLBACK_LONG) are configured here too, but they
are emitted by the conf-mode Tier-C live scanner and then readmitted before the
final conf gate, matching v11's external scan-source path.
"""
from __future__ import annotations

import os
from typing import Any, Dict, Tuple

import numpy as np
import pandas as pd

import eqidv2_setup_conf_loader as setup_conf_loader

FLAG_ENV = "EQIDV2_USE_FINAL_SETUP_CONF"
_TRUE = {"1", "true", "yes", "on", "enable", "enabled"}

GATE_VERSION = "final_setup_conf_live_2026_07_26"

# Candidate-row timestamp columns, in priority order (mirrors v11
# _selected_strategy_features signal-minute derivation).
_SIG_TS_COLS = ("signal_time_ist", "signal_datetime", "signal_ts", "bar_time_ist", "scan_slot_ist")


def is_enabled() -> bool:
    """True when the master flag is set. Everything is a no-op otherwise."""
    return str(os.getenv(FLAG_ENV, "0")).strip().lower() in _TRUE


def _u(value: Any) -> str:
    return str(value).upper().strip()


# --------------------------------------------------------------------------- #
# Pure values pulled from the conf (so each live module mutates its OWN globals;
# avoids __main__ vs named-module import pitfalls when a file is run as a script).
# --------------------------------------------------------------------------- #
def conf() -> Dict[str, dict]:
    return setup_conf_loader.load_setup_conf_module().FINAL_SETUP_CONF


def conf_source() -> str:
    """Configured module/path used by both live V7 and V11."""
    return setup_conf_loader.configured_target()


def conf_keys() -> frozenset:
    return frozenset(conf())


def conf_keys_upper() -> frozenset:
    return frozenset(_u(k) for k in conf())


def exit_rules_from_conf() -> Dict[str, Tuple[float, float]]:
    """{setup: (sl_pct, tgt_pct)} for every setup that declares an exit."""
    out: Dict[str, Tuple[float, float]] = {}
    for name, cfg in conf().items():
        ex = cfg.get("exit", {})
        if "sl_pct" in ex and "tgt_pct" in ex:
            out[name] = (float(ex["sl_pct"]), float(ex["tgt_pct"]))
    return out


def entry_policy_for_setup(setup: str) -> Dict[str, Any]:
    cfg = conf().get(str(setup), {})
    policy = cfg.get("entry_policy", {})
    return dict(policy) if isinstance(policy, dict) else {}


def exit_policy_for_setup(setup: str) -> Dict[str, Any]:
    cfg = conf().get(str(setup), {})
    policy = cfg.get("exit_policy", {})
    return dict(policy) if isinstance(policy, dict) else {}


# Setups validated OFF the production v8/research gates (raw pre-gate pool, tier123
# probe, new-setups scan). These are READMITTED past v8 + research in conf mode,
# mirroring avwap_5min_ID_v11_backtesting._FINAL_CONF_READMIT_SETUPS. Native setups
# (no such provenance) go THROUGH v8 + research, exactly as v11 does.
_READMIT_EVALS = frozenset({"RAW_PRE_GATE_POOL", "TIER123_OVERLAY_PROBE", "NEW_SETUPS_SCAN"})


def readmit_setups() -> frozenset:
    """Conf setups that bypass v8 + research (validated off those gates)."""
    return frozenset(
        name for name, cfg in conf().items()
        if str(cfg.get("provenance", {}).get("evaluated_on", "")).upper() in _READMIT_EVALS
    )


def pre_momentum_gates_from_conf() -> Dict[str, Tuple[Tuple[str, str, float], ...]]:
    """{setup: ((feature, op, threshold), ...)} for setups with a momentum gate."""
    return {
        name: tuple((t[0], t[1], float(t[2])) for t in cfg.get("pre_momentum_terms", []))
        for name, cfg in conf().items()
        if cfg.get("pre_momentum_terms")
    }


# --------------------------------------------------------------------------- #
# Scanner-side gate: the conf mask_terms + entry_guards, applied in the same
# pipeline position as apply_research_live_filters. Faithful port of
# avwap_5min_ID_v11_backtesting.py::_final_setup_conf_mask (+ the bits of
# _selected_strategy_features it relies on).
# --------------------------------------------------------------------------- #
def _num(frame: pd.DataFrame, col: str) -> pd.Series:
    if col in frame.columns:
        return pd.to_numeric(frame[col], errors="coerce")
    return pd.Series(np.nan, index=frame.index, dtype="float64")


def _normalise_ts(value: Any) -> pd.Timestamp:
    ts = pd.to_datetime(value, errors="coerce")
    if pd.isna(ts):
        return pd.NaT
    if ts.tz is None:
        return ts.tz_localize("Asia/Kolkata")
    return ts.tz_convert("Asia/Kolkata")


def _setup_feature_column(setup: str, configured_column: str) -> str:
    """Keep AVWAP setup gates on the anchored distance feature."""
    name = str(setup).strip().upper()
    column = str(configured_column).strip()
    if "AVWAP" in name and column == "vwap_dist_atr":
        return "avwap_dist_atr"
    return column


def _with_features(signals: pd.DataFrame) -> pd.DataFrame:
    work = signals.copy()
    for col in (
        "vol_ratio", "vwap_dist_atr", "avwap_dist_atr", "v7_signal_notional_rs", "market_ret_pct",
        "quality_score", "ranker_score", "body_pct", "rs_pct",
        "signal_open", "signal_high", "signal_low", "signal_close",
    ):
        if col in work.columns:
            work[col] = pd.to_numeric(work[col], errors="coerce")

    # Always derive this field from the authoritative timestamp, exactly like
    # V11. Trusting a pre-existing signal_minute lets stale/source-local values
    # make the two masks disagree.
    signal_days = []
    minutes = []
    for _, row in work.iterrows():
        ts = pd.NaT
        for col in _SIG_TS_COLS:
            if col in row.index and pd.notna(row.get(col)):
                ts = _normalise_ts(row.get(col))
                if pd.notna(ts):
                    break
        signal_days.append(ts.strftime("%Y-%m-%d") if pd.notna(ts) else "")
        minutes.append(float(ts.hour * 60 + ts.minute) if pd.notna(ts) else np.nan)
    work["signal_day"] = signal_days
    work["signal_minute"] = minutes

    # Derived mask features must be rebuilt from the signal candle exactly as
    # V11 does. Candidate snapshots do not consistently carry these columns.
    open_px = _num(work, "signal_open")
    high_px = _num(work, "signal_high")
    low_px = _num(work, "signal_low")
    close_px = _num(work, "signal_close")
    close_safe = close_px.replace(0, np.nan)
    body_top = pd.concat([open_px, close_px], axis=1).max(axis=1)
    body_bottom = pd.concat([open_px, close_px], axis=1).min(axis=1)
    work["upper_wick_pct"] = (high_px - body_top) / close_safe * 100.0
    work["lower_wick_pct"] = (body_bottom - low_px) / close_safe * 100.0
    work["wick_skew_pct"] = work["upper_wick_pct"] - work["lower_wick_pct"]
    work["signal_range_pct"] = (high_px - low_px) / close_safe * 100.0
    work["market_abs_ret_pct"] = _num(work, "market_ret_pct").abs()
    return work


def conf_mask(signals: pd.DataFrame) -> pd.Series:
    """Boolean keep-mask: keep ONLY conf setups, each filtered by its own
    mask_terms + entry guards. Mirrors v11 _final_setup_conf_mask."""
    if signals is None or len(signals) == 0:
        return pd.Series(False, index=getattr(signals, "index", pd.RangeIndex(0)))
    work = _with_features(signals)
    setup = work.get("setup", pd.Series("", index=work.index)).astype(str)
    regime = work.get("regime", pd.Series("", index=work.index)).astype(str).str.upper()
    sig_min = _num(work, "signal_minute")
    mask = pd.Series(False, index=work.index)
    for name, cfg in conf().items():
        m = setup.eq(name)
        for term in cfg.get("mask_terms", []):
            f, op, v = term[0], term[1], term[2]
            f = _setup_feature_column(name, f)
            if isinstance(v, str):
                col = regime if f == "regime" else work.get(f, pd.Series("", index=work.index)).astype(str).str.upper()
                vv = v.upper()
                m = m & (col.ne(vv) if op == "!=" else col.eq(vv))
            else:
                col = _num(work, f)
                if op == ">=":
                    m = m & (col >= v)
                elif op == "<=":
                    m = m & (col <= v)
                elif op == "!=":
                    m = m & (col != v)
                else:
                    m = m & (col == v)
        guard = cfg.get("entry_guards", {})

        def _slot_minute(value: object) -> int:
            hh, mm = str(value).split(":")
            return int(hh) * 60 + int(mm)

        if guard.get("min_slot"):
            m = m & (sig_min >= _slot_minute(guard["min_slot"]))
        if guard.get("max_slot"):
            m = m & (sig_min <= _slot_minute(guard["max_slot"]))
        for start, end in guard.get("exclude_windows", []):
            start_min = _slot_minute(start)
            end_min = _slot_minute(end)
            m = m & ~sig_min.between(start_min, end_min, inclusive="both")
        top_n = int(guard.get("top_n") or 0)
        if top_n > 0 and bool(m.any()):
            ranked = work.loc[m].copy()
            rank_column = _setup_feature_column(name, "vwap_dist_atr")
            ranked["_conf_top_n_distance"] = _num(ranked, rank_column)
            ranked["_conf_top_n_order"] = np.arange(len(ranked))
            ranked = ranked.sort_values(
                ["signal_day", "signal_minute", "_conf_top_n_distance", "_conf_top_n_order"],
                ascending=[True, True, False, True],
                kind="mergesort",
            )
            kept_index = ranked.groupby(
                ["signal_day", "signal_minute"], sort=False, dropna=False
            ).head(top_n).index
            m = m & work.index.isin(kept_index)
        mask = mask | m.fillna(False)
    return mask.fillna(False)


def apply_conf_gate(df: pd.DataFrame, day: str | None = None) -> Tuple[pd.DataFrame, pd.DataFrame, Dict[str, Any]]:
    """Drop-in replacement for apply_research_live_filters when the conf is active.
    Returns (accepted, rejected, stats) in the same shape the scanner expects."""
    stats: Dict[str, Any] = {
        "final_setup_conf_gate": True,
        "final_setup_conf_gate_version": GATE_VERSION,
        "research_live_filter_version": GATE_VERSION,
        "final_setup_conf_input_rows": int(0 if df is None else len(df)),
    }
    if df is None or len(df) == 0:
        empty = df.copy() if isinstance(df, pd.DataFrame) else pd.DataFrame()
        stats.update({"final_setup_conf_accepted": 0, "final_setup_conf_rejected": 0,
                      "research_live_filter_rejected": 0})
        return empty, empty.copy(), stats

    mask = conf_mask(df)
    accepted = df.loc[mask].copy()
    rejected = df.loc[~mask].copy()
    for frame, status, reason in (
        (accepted, "PASSED", ""),
        (rejected, "REJECTED", "final_setup_conf_mask_reject"),
    ):
        if not frame.empty:
            frame["research_live_filter_status"] = status
            frame["research_live_filter_reason"] = reason
            frame["research_live_filter_version"] = GATE_VERSION
    stats.update({
        "final_setup_conf_accepted": int(len(accepted)),
        "final_setup_conf_rejected": int(len(rejected)),
        "research_live_filter_rejected": int(len(rejected)),
    })
    return accepted, rejected, stats


def summary() -> Dict[str, Any]:
    """Human-readable snapshot of what activation would install (for logs/tests)."""
    c = conf()
    longs = sorted(k for k, v in c.items() if _u(v.get("side")) == "LONG")
    shorts = sorted(k for k, v in c.items() if _u(v.get("side")) == "SHORT")
    return {
        "flag_env": FLAG_ENV,
        "enabled": is_enabled(),
        "gate_version": GATE_VERSION,
        "config_source": conf_source(),
        "n_setups": len(c),
        "longs": longs,
        "shorts": shorts,
        "exit_rules": exit_rules_from_conf(),
        "entry_policies": {
            k: entry_policy_for_setup(k) for k in c if entry_policy_for_setup(k)
        },
        "exit_policies": {
            k: exit_policy_for_setup(k) for k in c if exit_policy_for_setup(k)
        },
        "pre_momentum_gates": {k: v for k, v in pre_momentum_gates_from_conf().items()},
        "masked_setups": sorted(k for k, v in c.items() if v.get("mask_terms")),
        "guarded_setups": sorted(k for k, v in c.items() if v.get("entry_guards")),
    }
