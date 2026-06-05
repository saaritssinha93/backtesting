"""
Shadow-only V7 pre-momentum filter analyst.

This session writes advisory profile/suggestion files only. It does not change
scanner rules, entry-engine gates, paper-trade values, live-trade values, or any
runtime inputs used by trading sessions.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import math
import os
import sys
import time
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from eqidv2_runtime_paths import LIVE_SIGNALS_DIR, RUNTIME_STATUS_DIR, runtime_dir


SESSION_NAME = "v7 pre momentum filter analyst"
SESSION_SLUG = "v7_pre_momentum_filter_analyst"
SESSION_ROOT = runtime_dir(SESSION_SLUG)
SESSION_LATEST_DIR = SESSION_ROOT / "latest"
SESSION_REPORTS_DIR = SESSION_ROOT / "reports"
SESSION_HEARTBEAT_DIR = SESSION_ROOT / "heartbeat"
SUGGESTION_DIR = SESSION_ROOT / "suggestions"

DYNAMIC_ROOT = runtime_dir("dynamic_pre_momentum")
DYNAMIC_LATEST_DIR = DYNAMIC_ROOT / "latest"

SIGNAL_DISCOVERY_CSV_DIR = runtime_dir("signal_discovery_v7_5mins_ID", "csv")
ENTRY_AUDIT_DIR = runtime_dir("entry_engine_1min_v5_ID", "audit")

for _path in (
    SESSION_ROOT,
    SESSION_LATEST_DIR,
    SESSION_REPORTS_DIR,
    SESSION_HEARTBEAT_DIR,
    SUGGESTION_DIR,
    DYNAMIC_ROOT,
    DYNAMIC_LATEST_DIR,
    RUNTIME_STATUS_DIR,
):
    _path.mkdir(parents=True, exist_ok=True)


STATIC_SETUP_GUIDANCE: dict[str, dict[str, str]] = {
    "E_VWAP_LOSE_EARLY_SHORT": {
        "verdict": "adequate_strong",
        "base_action": "KEEP_STATIC_ACTIVE",
        "note": "Strong pre-momentum proof; tighten/disable only when market regime is hostile to shorts or data health is poor.",
    },
    "L_TREND_PULLBACK": {
        "verdict": "adequate_tight",
        "base_action": "KEEP_STATIC_ACTIVE",
        "note": "Very strong but tight; dynamic overlay should mostly switch it off in unsupported regimes, not loosen it.",
    },
    "C_OR_BREAKOUT": {
        "verdict": "adequate_not_standalone",
        "base_action": "KEEP_STATIC_WITH_SHADOW_TIGHTENING",
        "note": "Entry screen is usable; watch flood/chop/VWAP extension and pair with C_OR time stop/saturation controls.",
    },
    "T_TREND_DAY_EMA_STAIR_SHORT": {
        "verdict": "useful_needs_regime",
        "base_action": "SHADOW_REGIME_TIGHTEN",
        "note": "Good damage control but still leaves SL; make stricter in neutral/chop and more permissive only on bearish trend days.",
    },
    "C_OR_BREAKDOWN": {
        "verdict": "not_production_adequate",
        "base_action": "KEEP_SHADOW_BLOCKED",
        "note": "Current shadow/blocked behavior is safe; do not reopen until clean out-of-sample recovery appears.",
    },
    "D_AVWAP_LOSE_REVERSAL": {
        "verdict": "not_adequate",
        "base_action": "KEEP_PROBATION_SHADOW",
        "note": "No working filter yet; mine a real pre-momentum rule before activation.",
    },
    "B_AVWAP_RECLAIM_REVERSAL": {
        "verdict": "probation_small_sample",
        "base_action": "KEEP_PROBATION_SHADOW",
        "note": "Risk improves but sample/top-winner retention is thin.",
    },
}

WATCH_SETUPS = tuple(STATIC_SETUP_GUIDANCE.keys())


def _now_ist() -> pd.Timestamp:
    return pd.Timestamp.now(tz="Asia/Kolkata")


def _normalise_ts(value: Any) -> pd.Timestamp:
    ts = pd.to_datetime(value, errors="coerce")
    if pd.isna(ts):
        return pd.NaT
    if ts.tzinfo is None:
        return ts.tz_localize("Asia/Kolkata")
    return ts.tz_convert("Asia/Kolkata")


def _fmt_ts(value: Any) -> str:
    ts = _normalise_ts(value)
    if pd.isna(ts):
        return ""
    offset = ts.strftime("%z")
    return f"{ts.strftime('%Y-%m-%d %H:%M:%S')}{offset[:3]}:{offset[3:]}"


def _safe_float(value: Any, default: float = np.nan) -> float:
    try:
        out = float(value)
    except Exception:
        return default
    return out if math.isfinite(out) else default


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists() or path.stat().st_size <= 2:
        return pd.DataFrame()
    try:
        return pd.read_csv(path, low_memory=False)
    except Exception:
        return pd.DataFrame()


def _read_many(paths: list[Path]) -> pd.DataFrame:
    parts = [_read_csv(path) for path in paths]
    parts = [part for part in parts if not part.empty]
    if not parts:
        return pd.DataFrame()
    return pd.concat(parts, ignore_index=True, sort=False)


def _write_text_atomic(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f"{path.name}.tmp.{os.getpid()}")
    tmp.write_text(text, encoding="utf-8")
    tmp.replace(path)


def _write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    _write_text_atomic(path, json.dumps(_json_sanitize(payload), indent=2, sort_keys=True, default=str, allow_nan=False))


def _json_sanitize(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _json_sanitize(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_json_sanitize(v) for v in value]
    if isinstance(value, tuple):
        return [_json_sanitize(v) for v in value]
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating, float)):
        x = float(value)
        return x if math.isfinite(x) else None
    if pd.isna(value) and not isinstance(value, (str, bytes)):
        return None
    return value


def _kv_text(payload: dict[str, Any]) -> str:
    rows: list[str] = []
    for key, value in payload.items():
        if isinstance(value, (dict, list, tuple)):
            value = json.dumps(value, sort_keys=True, default=str)
        rows.append(f"{key}={value}")
    return "\n".join(rows) + "\n"


def _write_status(state: str, **extra: Any) -> None:
    payload: dict[str, Any] = {
        "session": SESSION_NAME,
        "session_slug": SESSION_SLUG,
        "state": state,
        "pid": os.getpid(),
        "mode": "shadow_only",
        "applies_live_changes": False,
        "applies_paper_changes": False,
        "updated_at_ist": _fmt_ts(_now_ist()),
    }
    payload.update(extra)
    text = _kv_text(payload)
    _write_json_atomic(SESSION_HEARTBEAT_DIR / f"{SESSION_SLUG}.status.json", payload)
    _write_json_atomic(SESSION_HEARTBEAT_DIR / f"{SESSION_SLUG}.heartbeat.json", payload)
    _write_text_atomic(RUNTIME_STATUS_DIR / f"{SESSION_SLUG}.status", text)
    _write_text_atomic(RUNTIME_STATUS_DIR / f"{SESSION_SLUG}.heartbeat", text)


def _day_paths(day: str, prefix: str) -> list[Path]:
    compact = day.replace("-", "")
    return sorted(ENTRY_AUDIT_DIR.glob(f"{prefix}_{compact}_*.csv"))


def _today_entry_rows(day: str, prefix: str) -> pd.DataFrame:
    return _read_many(_day_paths(day, prefix))


def _load_raw_candidates(day: str) -> pd.DataFrame:
    return _read_csv(SIGNAL_DISCOVERY_CSV_DIR / f"raw_candidate_tickers_{day}.csv")


def _load_gated_candidates(day: str) -> pd.DataFrame:
    return _read_csv(SIGNAL_DISCOVERY_CSV_DIR / f"candidate_tickers_{day}.csv")


def _load_paper(day: str) -> pd.DataFrame:
    return _read_csv(LIVE_SIGNALS_DIR / f"paper_trades_{day}_id_5min_v7.csv")


def _num(df: pd.DataFrame, col: str, default: float = np.nan) -> pd.Series:
    if df.empty:
        return pd.Series(dtype=float)
    if col not in df.columns:
        return pd.Series(default, index=df.index, dtype=float)
    return pd.to_numeric(df[col], errors="coerce")


def _text(df: pd.DataFrame, col: str) -> pd.Series:
    if df.empty:
        return pd.Series(dtype=str)
    if col not in df.columns:
        return pd.Series("", index=df.index, dtype=str)
    return df[col].fillna("").astype(str)


def _latest_validation_per_setup() -> pd.DataFrame:
    patterns = [
        "v7_backend_validation*_per_setup.csv",
        "v7_*pre_momentum*_per_setup.csv",
    ]
    paths: list[Path] = []
    for pattern in patterns:
        paths.extend(LIVE_SIGNALS_DIR.glob(pattern))
    paths = sorted(set(paths), key=lambda p: p.stat().st_mtime if p.exists() else 0, reverse=True)
    for path in paths:
        df = _read_csv(path)
        if not df.empty and "setup" in df.columns:
            return df
    return pd.DataFrame()


def _classify_market(raw: pd.DataFrame, accepted: pd.DataFrame) -> dict[str, Any]:
    source = accepted if not accepted.empty else raw
    if source.empty:
        return {
            "regime": "UNKNOWN",
            "reason": "no current scanner/entry rows",
            "market_ret_pct": np.nan,
            "median_vol_ratio": np.nan,
            "median_atr_pct": np.nan,
            "long_rows": 0,
            "short_rows": 0,
            "c_or_rows": 0,
            "flood_state": "UNKNOWN",
        }
    market_ret = _safe_float(_num(source, "market_ret_pct").dropna().tail(20).mean(), np.nan)
    vol = _safe_float(_num(source, "vol_ratio").dropna().median(), np.nan)
    atr = _safe_float(_num(source, "atr_pct").dropna().median(), np.nan)
    side = _text(source, "side").str.upper()
    long_rows = int(side.eq("LONG").sum())
    short_rows = int(side.eq("SHORT").sum())
    setup = _text(source, "setup").str.upper()
    c_or_rows = int(setup.eq("C_OR_BREAKOUT").sum())
    if c_or_rows >= 50:
        flood = "C_OR_FLOOD"
    elif c_or_rows >= 25:
        flood = "C_OR_ELEVATED"
    else:
        flood = "NORMAL"
    if np.isfinite(vol) and vol >= 2.5:
        vol_state = "HIGH_VOL"
    elif np.isfinite(vol) and vol <= 1.1:
        vol_state = "LOW_VOL"
    else:
        vol_state = "NORMAL_VOL"
    if np.isfinite(market_ret) and market_ret >= 0.20:
        direction = "TREND_UP"
    elif np.isfinite(market_ret) and market_ret <= -0.20:
        direction = "TREND_DOWN"
    else:
        direction = "CHOP_NEUTRAL"
    if "CHOP" in direction and vol_state == "HIGH_VOL":
        regime = "CHOP_HIGH_VOL"
    elif flood != "NORMAL":
        regime = f"{direction}_{flood}"
    else:
        regime = f"{direction}_{vol_state}"
    return {
        "regime": regime,
        "reason": f"market_ret={market_ret:.3f}, median_vol={vol:.3f}, c_or_rows={c_or_rows}",
        "market_ret_pct": market_ret,
        "median_vol_ratio": vol,
        "median_atr_pct": atr,
        "long_rows": long_rows,
        "short_rows": short_rows,
        "c_or_rows": c_or_rows,
        "flood_state": flood,
    }


def _setup_stats(day: str, raw: pd.DataFrame, gated: pd.DataFrame, accepted: pd.DataFrame, rejected: pd.DataFrame, paper: pd.DataFrame) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for stage, frame in (
        ("raw_scanner", raw),
        ("gated_scanner", gated),
        ("entry_accepted", accepted),
        ("pre_momentum_rejected", rejected),
    ):
        if frame.empty:
            continue
        f = frame.copy()
        f["_stage"] = stage
        frames.append(f)
    if not frames:
        return pd.DataFrame()
    all_rows = pd.concat(frames, ignore_index=True, sort=False)
    all_rows["setup"] = _text(all_rows, "setup").str.upper().replace("", "UNKNOWN_SETUP")
    rows: list[dict[str, Any]] = []
    paper_setup = pd.DataFrame()
    if not paper.empty and "setup" in paper.columns:
        paper_setup = paper.copy()
        paper_setup["setup"] = _text(paper_setup, "setup").str.upper().replace("", "UNKNOWN_SETUP")
        paper_setup["pnl_rs"] = _num(paper_setup, "pnl_rs").fillna(0.0)
    for setup, group in all_rows.groupby("setup", dropna=False):
        acc = group.loc[group["_stage"].eq("entry_accepted")]
        rej = group.loc[group["_stage"].eq("pre_momentum_rejected")]
        raw_count = int(group["_stage"].eq("raw_scanner").sum())
        gated_count = int(group["_stage"].eq("gated_scanner").sum())
        accepted_count = int(len(acc))
        rejected_count = int(len(rej))
        denom = accepted_count + rejected_count
        setup_paper = paper_setup.loc[paper_setup["setup"].eq(setup)] if not paper_setup.empty else pd.DataFrame()
        pnl = float(setup_paper["pnl_rs"].sum()) if not setup_paper.empty else 0.0
        outcomes = _text(setup_paper, "outcome").str.upper() if not setup_paper.empty else pd.Series(dtype=str)
        rows.append(
            {
                "date": day,
                "setup": setup,
                "raw_scanner_rows": raw_count,
                "gated_scanner_rows": gated_count,
                "entry_accepted_rows": accepted_count,
                "pre_momentum_rejected_rows": rejected_count,
                "pre_momentum_pass_rate_pct": 100.0 * accepted_count / max(1, denom),
                "accepted_median_pre_score": _safe_float(_num(acc, "pre_entry_momentum_score").median(), np.nan),
                "rejected_median_pre_score": _safe_float(_num(rej, "pre_entry_momentum_score").median(), np.nan),
                "accepted_median_pre2_mom_r": _safe_float(_num(acc, "pre2_mom_r").median(), np.nan),
                "accepted_median_sig5_adx_calc": _safe_float(_num(acc, "sig5_adx_calc").median(), np.nan),
                "accepted_median_sig5_vol_ratio20": _safe_float(_num(acc, "sig5_vol_ratio20").median(), np.nan),
                "accepted_median_vwap_dist_atr": _safe_float(_num(acc, "vwap_dist_atr").median(), np.nan),
                "current_rule": str(_text(acc, "pre_momentum_gate_rule").replace("", np.nan).dropna().tail(1).iloc[0]) if not acc.empty and _text(acc, "pre_momentum_gate_rule").replace("", np.nan).dropna().shape[0] else "",
                "paper_trades": int(len(setup_paper)),
                "paper_pnl_rs": pnl,
                "paper_sl": int(outcomes.eq("SL").sum()) if not outcomes.empty else 0,
                "paper_target": int(outcomes.str.contains("TARGET", regex=False).sum()) if not outcomes.empty else 0,
                "paper_eod": int(outcomes.str.contains("EOD", regex=False).sum()) if not outcomes.empty else 0,
            }
        )
    out = pd.DataFrame(rows)
    if out.empty:
        return out
    return out.sort_values(["entry_accepted_rows", "raw_scanner_rows", "setup"], ascending=[False, False, True]).reset_index(drop=True)


def _validation_lookup(validation: pd.DataFrame) -> dict[str, dict[str, Any]]:
    if validation.empty or "setup" not in validation.columns:
        return {}
    out: dict[str, dict[str, Any]] = {}
    for _, row in validation.iterrows():
        setup = str(row.get("setup", "")).upper().strip()
        if not setup:
            continue
        out[setup] = dict(row)
    return out


def _suggestions(stats: pd.DataFrame, validation: pd.DataFrame, regime: dict[str, Any]) -> pd.DataFrame:
    validation_by_setup = _validation_lookup(validation)
    setups = set(WATCH_SETUPS)
    if not stats.empty:
        setups.update(str(x).upper() for x in stats["setup"].dropna().astype(str).tolist())
    rows: list[dict[str, Any]] = []
    flood_state = str(regime.get("flood_state", ""))
    market_regime = str(regime.get("regime", "UNKNOWN"))
    for setup in sorted(setups):
        srow = stats.loc[stats["setup"].eq(setup)].iloc[0].to_dict() if not stats.empty and bool(stats["setup"].eq(setup).any()) else {}
        guidance = STATIC_SETUP_GUIDANCE.get(setup, {})
        validation_row = validation_by_setup.get(setup, {})
        action = guidance.get("base_action", "OBSERVE_SHADOW")
        level = "INFO"
        proposed = "Keep static pre-momentum behavior; continue collecting shadow evidence."
        evidence_parts = []
        if validation_row:
            delta = _safe_float(validation_row.get("pnl_delta_rs"), _safe_float(validation_row.get("delta_pnl"), np.nan))
            sl_reduction = _safe_float(validation_row.get("sl_reduction"), np.nan)
            target_ret = _safe_float(validation_row.get("target_retention"), np.nan)
            if np.isfinite(delta):
                evidence_parts.append(f"validation_delta_rs={delta:.2f}")
            if np.isfinite(sl_reduction):
                evidence_parts.append(f"sl_reduction={sl_reduction:.0f}")
            if np.isfinite(target_ret):
                evidence_parts.append(f"target_retention={target_ret:.0%}")
        accepted = int(_safe_float(srow.get("entry_accepted_rows"), 0))
        rejected = int(_safe_float(srow.get("pre_momentum_rejected_rows"), 0))
        paper_sl = int(_safe_float(srow.get("paper_sl"), 0))
        paper_pnl = _safe_float(srow.get("paper_pnl_rs"), 0.0)
        evidence_parts.append(f"today_accept={accepted}")
        evidence_parts.append(f"today_reject={rejected}")
        if setup == "C_OR_BREAKOUT":
            if flood_state in {"C_OR_FLOOD", "C_OR_ELEVATED"}:
                level = "SHADOW_TIGHTEN"
                action = "SHADOW_TIGHTEN_IN_FLOOD"
                proposed = "Shadow-test stricter C_OR pre-momentum in flood/chop: require stronger ADX and non-negative pre2 momentum; do not apply yet."
            else:
                level = "WATCH"
                proposed = "Keep current C_OR gate; monitor flood, VWAP extension, and 30m time-stop outcomes."
        elif setup == "T_TREND_DAY_EMA_STAIR_SHORT":
            level = "SHADOW_TIGHTEN"
            proposed = "Shadow-test regime gating: allow only in bearish/weak market regimes; tighten in neutral/chop."
        elif setup == "D_AVWAP_LOSE_REVERSAL":
            level = "PROBATION"
            proposed = "Keep probation/shadow; mine a real pre-momentum rule before any activation."
        elif setup == "C_OR_BREAKDOWN":
            level = "PROBATION"
            proposed = "Keep shadow/blocked; reopen only if shadow outcome proves target retention and PF."
        elif setup == "B_AVWAP_RECLAIM_REVERSAL":
            level = "PROBATION"
            proposed = "Keep probation/shadow until sample grows and top-winner retention improves."
        elif setup in {"E_VWAP_LOSE_EARLY_SHORT", "L_TREND_PULLBACK"}:
            level = "KEEP"
            proposed = "Keep static gate active; dynamic overlay may only tighten/disable in hostile regimes."
        if paper_sl > 0 or paper_pnl < 0:
            level = "SHADOW_TIGHTEN" if level in {"INFO", "WATCH", "KEEP"} else level
            evidence_parts.append(f"today_paper_pnl_rs={paper_pnl:.2f}")
            evidence_parts.append(f"today_sl={paper_sl}")
        rule_value = srow.get("current_rule") or validation_row.get("rule", "")
        if pd.isna(rule_value) or str(rule_value).strip().lower() == "nan":
            rule_value = ""
        rows.append(
            {
                "session": SESSION_NAME,
                "mode": "SHADOW_ONLY",
                "applied_to_entry_engine": False,
                "applied_to_paper_executor": False,
                "market_regime": market_regime,
                "setup": setup,
                "level": level,
                "shadow_action": action,
                "current_rule": rule_value,
                "proposed_shadow_change": proposed,
                "evidence": "; ".join(evidence_parts),
                "note": guidance.get("note", "No static adequacy verdict yet; observe only."),
            }
        )
    return pd.DataFrame(rows)


def _profile_payload(day: str, run_time_ist: str, regime: dict[str, Any], suggestions: pd.DataFrame) -> dict[str, Any]:
    setup_profiles = []
    for _, row in suggestions.iterrows():
        setup_profiles.append(
            {
                "setup": row.get("setup", ""),
                "level": row.get("level", ""),
                "shadow_action": row.get("shadow_action", ""),
                "current_rule": row.get("current_rule", ""),
                "proposed_shadow_change": row.get("proposed_shadow_change", ""),
                "evidence": row.get("evidence", ""),
                "applied": False,
            }
        )
    return {
        "session": SESSION_NAME,
        "session_slug": SESSION_SLUG,
        "mode": "shadow",
        "day": day,
        "generated_at_ist": run_time_ist or _fmt_ts(_now_ist()),
        "expires_at_ist": _fmt_ts(_now_ist() + pd.Timedelta(minutes=20)),
        "applied_to_entry_engine": False,
        "applied_to_paper_executor": False,
        "market_regime": regime,
        "setup_profiles": setup_profiles,
        "safety_note": "Advisory only. No live/paper values or rules are changed by this file.",
    }


def _fmt_num(value: Any, digits: int = 2) -> str:
    x = _safe_float(value)
    if not np.isfinite(x):
        return "NA"
    return f"{x:,.{digits}f}"


def _report(day: str, run_time_ist: str, regime: dict[str, Any], stats: pd.DataFrame, suggestions: pd.DataFrame) -> str:
    lines = [
        f"# v7 pre momentum filter analyst - {day}",
        "",
        "Mode: SHADOW_ONLY. This session suggests pre-momentum changes but does not apply them to scanner, entry engine, paper trading, or live trading.",
        "",
        "## Market Regime",
        "",
        f"- Regime: {regime.get('regime', 'UNKNOWN')}",
        f"- Reason: {regime.get('reason', '')}",
        f"- C_OR rows: {regime.get('c_or_rows', 0)}",
        f"- Long/short rows: {regime.get('long_rows', 0)} / {regime.get('short_rows', 0)}",
        f"- Generated at: {run_time_ist or _fmt_ts(_now_ist())}",
        "",
        "## Shadow Suggestions",
        "",
    ]
    if suggestions.empty:
        lines.append("No suggestions yet. Waiting for scanner/entry audit rows.")
    else:
        lines.append("| setup | level | shadow action | current rule | proposed shadow change | evidence |")
        lines.append("|---|---|---|---|---|---|")
        order = {"SHADOW_TIGHTEN": 0, "PROBATION": 1, "WATCH": 2, "KEEP": 3, "INFO": 4}
        view = suggestions.copy()
        view["_order"] = view["level"].map(order).fillna(9)
        for _, row in view.sort_values(["_order", "setup"]).head(40).iterrows():
            current_rule = str(row.get("current_rule", "")).replace("|", "/")
            proposed = str(row.get("proposed_shadow_change", "")).replace("|", "/")
            evidence = str(row.get("evidence", "")).replace("|", "/")
            lines.append(
                f"| {row.get('setup', '')} | {row.get('level', '')} | {row.get('shadow_action', '')} | "
                f"{current_rule[:120]} | {proposed[:180]} | {evidence[:180]} |"
            )
    lines.extend(["", "## Today Setup Stats", ""])
    if stats.empty:
        lines.append("No today setup stats yet.")
    else:
        lines.append("| setup | raw | gated | accepted | pre-rejected | pass % | med pre score | med ADX | paper trades | paper pnl | SL |")
        lines.append("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|")
        for _, row in stats.head(35).iterrows():
            lines.append(
                f"| {row.get('setup', '')} | {int(row.get('raw_scanner_rows', 0))} | {int(row.get('gated_scanner_rows', 0))} | "
                f"{int(row.get('entry_accepted_rows', 0))} | {int(row.get('pre_momentum_rejected_rows', 0))} | "
                f"{_fmt_num(row.get('pre_momentum_pass_rate_pct'), 1)}% | {_fmt_num(row.get('accepted_median_pre_score'), 2)} | "
                f"{_fmt_num(row.get('accepted_median_sig5_adx_calc'), 2)} | {int(row.get('paper_trades', 0))} | "
                f"Rs {_fmt_num(row.get('paper_pnl_rs'), 2)} | {int(row.get('paper_sl', 0))} |"
            )
    lines.extend(
        [
            "",
            "## Guardrail",
            "",
            "- This analyst writes only to `v7_pre_momentum_filter_analyst` and `dynamic_pre_momentum` advisory folders.",
            "- Entry engine integration remains off; no env var is set here and no trading files are modified.",
            "- Promote only after replay plus multi-session shadow proof.",
            "",
        ]
    )
    return "\n".join(lines)


def run_analysis(day: str, *, run_time_ist: str = "") -> dict[str, Any]:
    _write_status("RUNNING", phase="BUILD_SHADOW_ANALYSIS", day=day, run_time_ist=run_time_ist)
    raw = _load_raw_candidates(day)
    gated = _load_gated_candidates(day)
    accepted = _today_entry_rows(day, "entry_rows")
    rejected = _today_entry_rows(day, "entry_rejected_pre_momentum")
    paper = _load_paper(day)
    validation = _latest_validation_per_setup()

    regime = _classify_market(raw, accepted)
    stats = _setup_stats(day, raw, gated, accepted, rejected, paper)
    suggestions = _suggestions(stats, validation, regime)
    profile = _profile_payload(day, run_time_ist, regime, suggestions)
    report_text = _report(day, run_time_ist, regime, stats, suggestions)

    dated = day.replace("-", "")
    report_path = SESSION_REPORTS_DIR / f"v7_pre_momentum_filter_analyst_{day}.md"
    json_path = SESSION_REPORTS_DIR / f"v7_pre_momentum_filter_analyst_{day}.json"
    csv_path = SUGGESTION_DIR / f"v7_pre_momentum_filter_suggestions_{day}.csv"
    stats_path = SUGGESTION_DIR / f"v7_pre_momentum_filter_setup_stats_{day}.csv"
    profile_path = DYNAMIC_LATEST_DIR / "latest_dynamic_pre_momentum_profile.json"

    _write_text_atomic(report_path, report_text)
    _write_json_atomic(json_path, profile)
    suggestions.to_csv(csv_path, index=False)
    stats.to_csv(stats_path, index=False)
    _write_text_atomic(SESSION_LATEST_DIR / "latest_v7_pre_momentum_filter_analyst.md", report_text)
    _write_json_atomic(SESSION_LATEST_DIR / "latest_v7_pre_momentum_filter_analyst.json", profile)
    _write_text_atomic(SESSION_LATEST_DIR / "latest_v7_pre_momentum_filter_suggestions.csv", suggestions.to_csv(index=False))
    _write_text_atomic(SESSION_LATEST_DIR / "latest_v7_pre_momentum_filter_setup_stats.csv", stats.to_csv(index=False))
    _write_json_atomic(profile_path, profile)

    payload = {
        "day": day,
        "run_time_ist": run_time_ist or _fmt_ts(_now_ist()),
        "report": str(report_path),
        "latest_report": str(SESSION_LATEST_DIR / "latest_v7_pre_momentum_filter_analyst.md"),
        "profile": str(profile_path),
        "suggestions_csv": str(csv_path),
        "stats_csv": str(stats_path),
        "suggestion_count": int(len(suggestions)),
        "shadow_tighten_count": int((suggestions.get("level", pd.Series(dtype=str)) == "SHADOW_TIGHTEN").sum()) if not suggestions.empty else 0,
        "probation_count": int((suggestions.get("level", pd.Series(dtype=str)) == "PROBATION").sum()) if not suggestions.empty else 0,
        "market_regime": regime.get("regime", "UNKNOWN"),
        "mode": "shadow_only",
        "applies_live_changes": False,
        "applies_paper_changes": False,
    }
    _write_json_atomic(SESSION_LATEST_DIR / "latest_v7_pre_momentum_filter_analyst_summary.json", payload)
    _write_status("RUNNING", phase="SHADOW_ANALYSIS_DONE", **payload)
    print(
        f"[v7_pre_momentum_filter_analyst] wrote {SESSION_LATEST_DIR / 'latest_v7_pre_momentum_filter_analyst.md'} "
        f"suggestions={payload['suggestion_count']} regime={payload['market_regime']}",
        flush=True,
    )
    return payload


def _parse_today_time(day: dt.date, value: str) -> pd.Timestamp:
    parsed = dt.datetime.strptime(str(value), "%H:%M:%S").time()
    return pd.Timestamp(dt.datetime.combine(day, parsed), tz="Asia/Kolkata")


def _next_run_time(now: pd.Timestamp, start: pd.Timestamp, end: pd.Timestamp, interval_min: int) -> pd.Timestamp | None:
    if now < start:
        return start
    end_grace = pd.Timedelta(seconds=60)
    if now > end + end_grace:
        return None
    if now > end:
        return end
    interval = pd.Timedelta(minutes=max(1, int(interval_min)))
    elapsed = now - start
    steps = int(np.floor(elapsed / interval))
    due_slot = start + steps * interval
    if pd.Timedelta(0) <= now - due_slot <= pd.Timedelta(seconds=60):
        return due_slot
    candidate = due_slot + interval
    if candidate > end:
        return end if now <= end else None
    return candidate


def _is_advisory_due(run_ts: pd.Timestamp, start: pd.Timestamp, suggestion_interval_min: int) -> bool:
    interval = max(1, int(suggestion_interval_min))
    elapsed_min = int(round((run_ts - start).total_seconds() / 60.0))
    return elapsed_min >= 0 and elapsed_min % interval == 0


def _next_advisory_time(now: pd.Timestamp, start: pd.Timestamp, end: pd.Timestamp, suggestion_interval_min: int) -> pd.Timestamp | None:
    if now < start:
        return start
    interval = pd.Timedelta(minutes=max(1, int(suggestion_interval_min)))
    elapsed = now - start
    steps = int(np.ceil(max(0.0, elapsed.total_seconds()) / interval.total_seconds()))
    candidate = start + steps * interval
    if candidate < now:
        candidate += interval
    return candidate if candidate <= end else None


def run_loop(*, start_time: str, end_time: str, interval_min: int, suggestion_interval_min: int, run_now: bool) -> int:
    today = _now_ist().date()
    start = _parse_today_time(today, start_time)
    end = _parse_today_time(today, end_time)
    last_run_key = ""
    _write_status(
        "RUNNING",
        phase="LOOP_START",
        start_time=start_time,
        end_time=end_time,
        interval_min=int(interval_min),
        suggestion_interval_min=int(suggestion_interval_min),
        run_now=bool(run_now),
    )
    if run_now and start <= _now_ist() <= end:
        now = _now_ist()
        run_analysis(now.strftime("%Y-%m-%d"), run_time_ist=_fmt_ts(now))

    while True:
        now = _now_ist()
        if now.date() != today:
            _write_status("STOPPED", phase="DATE_ROLLED", reason="date_changed")
            return 0
        next_run = _next_run_time(now, start, end, interval_min)
        if next_run is None:
            _write_status("STOPPED", phase="DONE", reason="after_end_time", end_time=end_time)
            return 0
        wait_sec = max(0.0, (next_run - now).total_seconds())
        run_key = _fmt_ts(next_run)
        if wait_sec > 0:
            next_adv = _next_advisory_time(now, start, end, suggestion_interval_min)
            _write_status(
                "RUNNING",
                phase="WAIT",
                next_run_ist=run_key,
                next_advisory_ist=_fmt_ts(next_adv) if next_adv is not None else "",
                wait_sec=round(wait_sec, 1),
                start_time=start_time,
                end_time=end_time,
                interval_min=int(interval_min),
                suggestion_interval_min=int(suggestion_interval_min),
            )
            time.sleep(min(wait_sec, 60.0))
            continue
        if run_key == last_run_key:
            time.sleep(1.0)
            continue
        last_run_key = run_key
        try:
            if _is_advisory_due(next_run, start, suggestion_interval_min):
                run_analysis(next_run.strftime("%Y-%m-%d"), run_time_ist=run_key)
            else:
                next_adv = _next_advisory_time(next_run + pd.Timedelta(seconds=1), start, end, suggestion_interval_min)
                _write_status(
                    "RUNNING",
                    phase="HEARTBEAT_ONLY_WAITING_15M_ADVISORY",
                    day=next_run.strftime("%Y-%m-%d"),
                    run_time_ist=run_key,
                    next_advisory_ist=_fmt_ts(next_adv) if next_adv is not None else "",
                    latest_report=str(SESSION_LATEST_DIR / "latest_v7_pre_momentum_filter_analyst.md"),
                    mode="shadow_only",
                )
                print(f"[v7_pre_momentum_filter_analyst] heartbeat {run_key}; advisory not due yet", flush=True)
        except Exception as exc:
            _write_status("ERROR", phase="ANALYST_FAILED", run_time_ist=run_key, error=f"{type(exc).__name__}: {exc}")
            print(f"[v7_pre_momentum_filter_analyst] ERROR {type(exc).__name__}: {exc}", flush=True)
        time.sleep(1.0)


def _default_day() -> str:
    return _now_ist().strftime("%Y-%m-%d")


def main() -> int:
    ap = argparse.ArgumentParser(description="Run shadow-only V7 pre-momentum filter analyst")
    ap.add_argument("--date", default=_default_day(), help="Trading date YYYY-MM-DD; default today IST")
    ap.add_argument("--loop", action="store_true", help="Run loop during market session")
    ap.add_argument("--run-now", action="store_true", help="Run one advisory snapshot immediately if inside loop time")
    ap.add_argument("--start-time", default="09:17:00", help="Loop start HH:MM:SS IST")
    ap.add_argument("--end-time", default="15:37:00", help="Loop end HH:MM:SS IST")
    ap.add_argument("--interval-min", type=int, default=5, help="Heartbeat loop interval minutes")
    ap.add_argument("--suggestion-interval-min", type=int, default=15, help="Advisory suggestion interval minutes")
    args = ap.parse_args()
    if args.loop:
        return run_loop(
            start_time=str(args.start_time),
            end_time=str(args.end_time),
            interval_min=int(args.interval_min),
            suggestion_interval_min=int(args.suggestion_interval_min),
            run_now=bool(args.run_now),
        )
    run_analysis(str(args.date), run_time_ist=_fmt_ts(_now_ist()))
    _write_status("STOPPED", phase="ONE_SHOT_DONE", day=str(args.date))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
