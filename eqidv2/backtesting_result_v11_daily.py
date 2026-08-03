"""
Daily V7 parity report: run v11 backtester for a trading day and compare
with V7 live paper trades to measure and diagnose divergence.

Run strategy (tried in order):
  1. live_parity mode   — replays actual live scanner JSON candidates
  2. historical_full_day — reconstructs signals from parquet data
Reports which mode produced results and what implications that has.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from eqidv2_runtime_paths import LIVE_SIGNALS_DIR, runtime_dir
from eqidv2_runtime_manifest import freeze_runtime_manifest

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass


BASE_DIR = Path(__file__).resolve().parent
PYTHON_EXE = Path(sys.executable)
OUT_ROOT = runtime_dir("backtesting_result_v11")
LATEST_DIR = OUT_ROOT / "latest"
REPORTS_DIR = OUT_ROOT / "reports"
V11_SCRIPT = BASE_DIR / "avwap_5min_ID_v11_backtesting.py"
LIVE_CANDIDATE_JSON_DIR = runtime_dir("signal_discovery_v7_5mins_ID", "json")
SCANNER_AUDIT_DIR = runtime_dir("signal_discovery_v7_5mins_ID", "audit")
ENTRY_ENGINE_AUDIT_DIR = runtime_dir("entry_engine_1min_v5_ID", "audit")

BACKTEST_PNL_COL = "v6_net_pnl_rs"
PNL_COL_CANDIDATES = (
    BACKTEST_PNL_COL,
    "pnl_rs",
    "net_pnl_rs",
    "v7_net_pnl_rs",
    "pnl",
    "net_pnl",
    "gross_pnl",
)
SLOT_MINUTES = 5  # 5-min bar size; signals are aligned to this grid
_MISSING_TEXT = {"", "NAN", "NONE", "NULL", "NA", "NAT"}
_TRUE_ENV = {"1", "true", "yes", "on", "enable", "enabled"}

for _p in (OUT_ROOT, LATEST_DIR, REPORTS_DIR):
    _p.mkdir(parents=True, exist_ok=True)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists() or path.stat().st_size <= 2:
        return pd.DataFrame()
    try:
        return pd.read_csv(path, low_memory=False)
    except Exception:
        return pd.DataFrame()


def _load_funnel_day(audit_dir: Path, prefix: str, day: str) -> pd.DataFrame:
    day_key = str(day).replace("-", "")
    frames: list[pd.DataFrame] = []
    for path in sorted(audit_dir.glob(f"{prefix}_{day_key}_*.csv")):
        frame = _read_csv(path)
        if not frame.empty:
            frame = frame.copy()
            frame["funnel_source_path"] = str(path)
            frames.append(frame)
    return pd.concat(frames, ignore_index=True, sort=False) if frames else pd.DataFrame()


def _funnel_summary(frame: pd.DataFrame) -> pd.DataFrame:
    if frame is None or frame.empty:
        return pd.DataFrame()
    work = frame.copy()
    for col in ("stage", "outcome", "reason", "candidate_id"):
        if col not in work.columns:
            work[col] = ""
        work[col] = work[col].fillna("").astype(str)
    return (
        work.groupby(["stage", "outcome", "reason"], dropna=False)
        .agg(
            records=("candidate_id", "size"),
            candidates=("candidate_id", "nunique"),
        )
        .reset_index()
        .sort_values(["stage", "outcome", "candidates"], ascending=[True, True, False])
        .reset_index(drop=True)
    )


def _entry_latency_summary(day: str) -> dict[str, Any]:
    path = ENTRY_ENGINE_AUDIT_DIR / f"entry_engine_audit_{day}.jsonl"
    if not path.exists():
        return {"audit_path": str(path), "slots": 0}
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        try:
            item = json.loads(line)
        except Exception:
            continue
        if str(item.get("slot_ist", ""))[:10] == str(day):
            rows.append(item)
    out: dict[str, Any] = {"audit_path": str(path), "slots": int(len(rows))}
    for key in (
        "bar_close_to_candidate_ready_sec",
        "candidate_ready_to_entry_engine_ready_sec",
        "candidate_ready_to_signal_write_sec",
        "intended_entry_to_signal_write_sec",
    ):
        values = pd.to_numeric(
            pd.Series([row.get(key) for row in rows], dtype="object"),
            errors="coerce",
        ).dropna()
        out[f"{key}_median"] = float(values.median()) if len(values) else None
        out[f"{key}_p95"] = float(values.quantile(0.95)) if len(values) else None
        out[f"{key}_max"] = float(values.max()) if len(values) else None
    out["marker_missing_slots"] = int(
        sum(not bool(row.get("slot_complete_marker_ready")) for row in rows)
    )
    out["writer_rejected_rows"] = int(
        sum(int(row.get("signal_writer_rejected_rows", 0) or 0) for row in rows)
    )
    return out


def _safe_float(value: Any, default: float = np.nan) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def _series_from_any(df: pd.DataFrame, names: tuple[str, ...], default: Any = "") -> pd.Series:
    for name in names:
        if name in df.columns:
            return df[name]
    return pd.Series(default, index=df.index)


def _text_from_any(df: pd.DataFrame, names: tuple[str, ...]) -> pd.Series:
    return _series_from_any(df, names, "").fillna("").astype(str).str.strip()


def _pnl_series(df: pd.DataFrame, preferred: str | None = None) -> pd.Series:
    names = ((preferred,) if preferred else tuple()) + tuple(c for c in PNL_COL_CANDIDATES if c != preferred)
    return pd.to_numeric(_series_from_any(df, names, np.nan), errors="coerce").fillna(0.0)


def _row_pnl(row: pd.Series) -> float:
    for col in PNL_COL_CANDIDATES:
        if col in row.index:
            val = _safe_float(row.get(col), np.nan)
            if np.isfinite(val):
                return val
    return float("nan")


def _valid_text(value: Any) -> bool:
    return str(value).strip().upper() not in _MISSING_TEXT


def _drop_invalid_key_rows(df: pd.DataFrame, source: str) -> pd.DataFrame:
    if df.empty:
        return df
    required = ("_ticker", "_side", "_setup")
    mask = pd.Series(True, index=df.index)
    for col in required:
        mask &= df[col].map(_valid_text) if col in df.columns else False
    mask &= df.get("_ts_ist", pd.Series(pd.NaT, index=df.index)).notna()
    dropped = int((~mask).sum())
    if dropped:
        print(f"[backtesting_result_v11] dropped {dropped} {source} rows with invalid match keys", flush=True)
    return df.loc[mask].copy()


def _profit_factor(pnl: pd.Series) -> float:
    values = pd.to_numeric(pnl, errors="coerce").fillna(0.0)
    gp = float(values[values > 0].sum())
    gl = float(-values[values < 0].sum())
    if gl <= 0:
        return np.inf if gp > 0 else np.nan
    return gp / gl


def _fmt_num(value: Any, digits: int = 2) -> str:
    val = _safe_float(value)
    if np.isinf(val):
        return "inf"
    if not np.isfinite(val):
        return "NA"
    return f"{val:,.{digits}f}"


def _fmt_pf(value: Any) -> str:
    return _fmt_num(value, 3)


def _paper_path(day: str) -> Path:
    return LIVE_SIGNALS_DIR / f"paper_trades_{day}_id_5min_v7.csv"


def _default_selected_strategy_profile() -> str:
    profile = str(os.getenv("EQIDV2_V11_SELECTED_STRATEGY_PROFILE", "")).strip()
    if profile:
        return profile
    if str(os.getenv("EQIDV2_USE_FINAL_SETUP_CONF", "")).strip().lower() in _TRUE_ENV:
        return "final_setup_conf"
    return "none"


# ---------------------------------------------------------------------------
# Timestamp normalisation & slot-based key
# ---------------------------------------------------------------------------

def _to_ist(value: Any) -> pd.Timestamp:
    ts = pd.to_datetime(value, errors="coerce")
    if pd.isna(ts):
        return pd.NaT
    if ts.tzinfo is None:
        return ts.tz_localize("Asia/Kolkata")
    return ts.tz_convert("Asia/Kolkata")


def _slot_key(ticker: str, side: str, setup: str, ts: pd.Timestamp) -> str:
    """Slot-rounded match key: robust to ±1 minute timestamp drift."""
    if pd.isna(ts):
        return f"{ticker}|{side}|{setup}|"
    slot_min = (ts.hour * 60 + ts.minute) // SLOT_MINUTES * SLOT_MINUTES
    slot_str = f"{ts.date()}T{slot_min // 60:02d}:{slot_min % 60:02d}"
    return f"{ticker}|{side}|{setup}|{slot_str}"


def _exact_key(ticker: str, side: str, setup: str, ts: pd.Timestamp) -> str:
    if pd.isna(ts):
        return f"{ticker}|{side}|{setup}|"
    return f"{ticker}|{side}|{setup}|{ts.strftime('%Y-%m-%d %H:%M:%S%z')}"


def _enrich_keys(df: pd.DataFrame, *, source: str) -> pd.DataFrame:
    """Add _ts_ist, _slot_key, _exact_key columns to a trade dataframe."""
    if df.empty:
        return df
    out = df.copy()
    out["_ticker"] = _text_from_any(out, ("ticker", "symbol", "stock", "tradingsymbol")).str.upper()
    out["_side"] = _text_from_any(out, ("side", "trade_side", "direction", "position_side")).str.upper()
    out["_setup"] = _text_from_any(out, ("setup", "setup_name", "setup_id", "strategy", "rule_name"))
    out["_pnl_rs"] = _pnl_series(out)

    if source == "v11":
        ts_raw = _series_from_any(out, ("signal_time_v8", "signal_time_ist", "entry_time", "entry_time_ist", "signal_ts"), "")
    else:
        ts_raw = _series_from_any(out, ("signal_entry_datetime_ist", "signal_datetime", "entry_time", "entry_time_ist", "signal_time_ist"), "")
    out["_ts_ist"] = ts_raw.map(_to_ist)

    out["_slot_key"] = [
        _slot_key(str(r["_ticker"]), str(r["_side"]), str(r["_setup"]), r["_ts_ist"])
        for _, r in out.iterrows()
    ]
    out["_exact_key"] = [
        _exact_key(str(r["_ticker"]), str(r["_side"]), str(r["_setup"]), r["_ts_ist"])
        for _, r in out.iterrows()
    ]
    return _drop_invalid_key_rows(out, source)


# ---------------------------------------------------------------------------
# Core matching
# ---------------------------------------------------------------------------

def _match_sets(v11: pd.DataFrame, paper: pd.DataFrame) -> dict[str, Any]:
    """
    Match v11 and paper trades using slot-rounded key (5-min window).
    Returns sets of matched/unmatched indices and overlap stats.
    """
    v11_slot = set(v11.get("_slot_key", pd.Series(dtype=str)).dropna().astype(str))
    paper_slot = set(paper.get("_slot_key", pd.Series(dtype=str)).dropna().astype(str))
    v11_exact = set(v11.get("_exact_key", pd.Series(dtype=str)).dropna().astype(str))
    paper_exact = set(paper.get("_exact_key", pd.Series(dtype=str)).dropna().astype(str))

    matched_exact = v11_exact & paper_exact
    matched_slot = v11_slot & paper_slot
    only_v11_slot = v11_slot - paper_slot
    only_paper_slot = paper_slot - v11_slot

    v11_match_idx = v11.index[v11["_slot_key"].isin(matched_slot)].tolist() if not v11.empty else []
    v11_only_idx = v11.index[v11["_slot_key"].isin(only_v11_slot)].tolist() if not v11.empty else []
    paper_match_idx = paper.index[paper["_slot_key"].isin(matched_slot)].tolist() if not paper.empty else []
    paper_only_idx = paper.index[paper["_slot_key"].isin(only_paper_slot)].tolist() if not paper.empty else []

    total_paper = max(len(paper), 1)
    parity_score = round(100.0 * len(paper_match_idx) / total_paper, 1)

    return {
        "matched_exact": len(matched_exact),
        "matched_slot": len(matched_slot),
        "only_v11_slot": len(only_v11_slot),
        "only_paper_slot": len(only_paper_slot),
        "parity_score_pct": parity_score,
        "v11_match_idx": v11_match_idx,
        "v11_only_idx": v11_only_idx,
        "paper_match_idx": paper_match_idx,
        "paper_only_idx": paper_only_idx,
    }


def _entry_price_gap(v11: pd.DataFrame, paper: pd.DataFrame, match: dict[str, Any]) -> pd.DataFrame:
    """For matched slot-key trades, show entry price and PnL gaps."""
    v11_m = v11.loc[match["v11_match_idx"]].copy() if match["v11_match_idx"] else pd.DataFrame()
    paper_m = paper.loc[match["paper_match_idx"]].copy() if match["paper_match_idx"] else pd.DataFrame()
    if v11_m.empty or paper_m.empty:
        return pd.DataFrame()
    v11_m = v11_m.set_index("_slot_key", drop=False)
    paper_m = paper_m.set_index("_slot_key", drop=False)
    rows: list[dict[str, Any]] = []
    for key in sorted(set(v11_m.index) & set(paper_m.index)):
        vr = v11_m.loc[key]
        pr = paper_m.loc[key]
        if isinstance(vr, pd.DataFrame):
            vr = vr.iloc[0]
        if isinstance(pr, pd.DataFrame):
            pr = pr.iloc[0]
        v11_entry = _safe_float(vr.get("entry_price_v6", vr.get("entry_price", np.nan)))
        paper_entry = _safe_float(pr.get("entry_price", np.nan))
        v11_pnl = _row_pnl(vr)
        paper_pnl = _safe_float(pr.get("pnl_rs", np.nan))
        v11_qty = _safe_float(vr.get("quantity", np.nan))
        paper_qty = _safe_float(pr.get("quantity", np.nan))
        v11_notional = _safe_float(vr.get("notional_exposure_rs", np.nan))
        paper_notional = (
            paper_entry * paper_qty
            if np.isfinite(paper_entry) and np.isfinite(paper_qty)
            else np.nan
        )
        v11_cost = _safe_float(vr.get("total_cost_rs", vr.get("v6_cost_rs", np.nan)))
        paper_cost = _safe_float(pr.get("total_cost_rs", pr.get("total_cost", np.nan)))
        entry_gap_pct = (
            100.0 * (v11_entry - paper_entry) / paper_entry
            if np.isfinite(v11_entry) and np.isfinite(paper_entry) and paper_entry > 0
            else np.nan
        )
        rows.append(
            {
                "slot_key": key,
                "ticker": vr.get("_ticker", ""),
                "side": vr.get("_side", ""),
                "setup": vr.get("_setup", ""),
                "v11_signal_time": vr.get("_ts_ist", ""),
                "paper_signal_time": pr.get("_ts_ist", ""),
                "v11_entry": round(v11_entry, 2) if np.isfinite(v11_entry) else np.nan,
                "paper_entry": round(paper_entry, 2) if np.isfinite(paper_entry) else np.nan,
                "entry_gap_pct": round(entry_gap_pct, 3) if np.isfinite(entry_gap_pct) else np.nan,
                "v11_quantity": int(v11_qty) if np.isfinite(v11_qty) else np.nan,
                "paper_quantity": int(paper_qty) if np.isfinite(paper_qty) else np.nan,
                "v11_notional_rs": round(v11_notional, 2) if np.isfinite(v11_notional) else np.nan,
                "paper_notional_rs": round(paper_notional, 2) if np.isfinite(paper_notional) else np.nan,
                "v11_cost_rs": round(v11_cost, 2) if np.isfinite(v11_cost) else np.nan,
                "paper_cost_rs": round(paper_cost, 2) if np.isfinite(paper_cost) else np.nan,
                "v11_pnl_rs": round(v11_pnl, 2) if np.isfinite(v11_pnl) else np.nan,
                "paper_pnl_rs": round(paper_pnl, 2) if np.isfinite(paper_pnl) else np.nan,
                "pnl_gap_v11_minus_paper": round(
                    v11_pnl - paper_pnl, 2
                ) if np.isfinite(v11_pnl) and np.isfinite(paper_pnl) else np.nan,
            }
        )
    return pd.DataFrame(rows)


def _per_setup_parity(v11: pd.DataFrame, paper: pd.DataFrame) -> pd.DataFrame:
    """Per-setup match rate: shows which setups have parity and which diverge."""
    if v11.empty and paper.empty:
        return pd.DataFrame()
    v11_slot = set(v11.get("_slot_key", pd.Series(dtype=str)).dropna().astype(str)) if not v11.empty else set()
    paper_slot = set(paper.get("_slot_key", pd.Series(dtype=str)).dropna().astype(str)) if not paper.empty else set()
    all_setups: set[str] = set()
    if not v11.empty and "_setup" in v11.columns:
        all_setups.update(v11["_setup"].fillna("").astype(str).unique())
    if not paper.empty and "_setup" in paper.columns:
        all_setups.update(paper["_setup"].fillna("").astype(str).unique())

    rows: list[dict[str, Any]] = []
    for setup in sorted(all_setups):
        v11_s = v11.loc[v11["_setup"].eq(setup)].copy() if not v11.empty and "_setup" in v11.columns else pd.DataFrame()
        paper_s = paper.loc[paper["_setup"].eq(setup)].copy() if not paper.empty and "_setup" in paper.columns else pd.DataFrame()
        v11_keys_s = set(v11_s["_slot_key"].astype(str)) if not v11_s.empty else set()
        paper_keys_s = set(paper_s["_slot_key"].astype(str)) if not paper_s.empty else set()
        matched_s = len(v11_keys_s & paper_keys_s)
        v11_n = len(v11_s)
        paper_n = len(paper_s)
        paper_pnl = _pnl_series(paper_s, "pnl_rs") if not paper_s.empty else pd.Series(dtype=float)
        v11_pnl = _pnl_series(v11_s, BACKTEST_PNL_COL) if not v11_s.empty else pd.Series(dtype=float)
        rows.append(
            {
                "setup": setup,
                "v11_trades": v11_n,
                "paper_trades": paper_n,
                "matched": matched_s,
                "v11_only": v11_n - matched_s,
                "paper_only": paper_n - matched_s,
                "parity_%": f"{100.0 * matched_s / max(paper_n, 1):.0f}",
                "paper_pnl_rs": _fmt_num(float(paper_pnl.sum()), 2),
                "v11_pnl_rs": _fmt_num(float(v11_pnl.sum()), 2),
                "paper_pf": _fmt_pf(_profit_factor(paper_pnl)),
                "v11_pf": _fmt_pf(_profit_factor(v11_pnl)),
            }
        )
    return pd.DataFrame(rows).sort_values("paper_trades", ascending=False).reset_index(drop=True)


def _root_cause_paper_only(paper_only: pd.DataFrame, v11: pd.DataFrame, day: str) -> pd.DataFrame:
    """
    For each paper-only trade (live traded but missing from v11), diagnose WHY.

    Checks in order:
    1. anti_chase_skip  — paper outcome starts with ENTRY_SKIPPED (shouldn't be here, but check)
    2. parity_mode_gap  — if v11 ran in historical_full_day, the signal may not have appeared in
                          the parquet-reconstructed pipeline
    3. timing_drift     — a v11 signal exists for same (ticker, side, setup) on same day but
                          different slot (within 30 min)
    4. v11_gate_reject  — v11 rejected the candidate at gate/overlay/entry stage
    5. no_v11_signal    — no v11 signal at all for this (ticker, side, setup) on this day
    """
    if paper_only.empty:
        return pd.DataFrame()
    rows: list[dict[str, Any]] = []
    v11_day = v11.loc[
        v11.get("_ts_ist", pd.Series(dtype=object)).map(
            lambda t: str(getattr(t, "date", lambda: None)()) if pd.notna(t) else ""
        ).eq(day)
    ].copy() if not v11.empty else pd.DataFrame()

    for _, row in paper_only.iterrows():
        ticker = str(row.get("_ticker", row.get("ticker", ""))).upper()
        side = str(row.get("_side", row.get("side", ""))).upper()
        setup = str(row.get("_setup", row.get("setup", "")))
        ts = row.get("_ts_ist", pd.NaT)
        outcome = str(row.get("outcome", "")).upper()

        if outcome.startswith("ENTRY_SKIPPED"):
            cause = "paper_skipped_not_a_real_trade"
            note = f"outcome={outcome} — paper executor skipped; not a true trade"
        elif v11_day.empty:
            cause = "no_v11_output"
            note = "v11 produced 0 trades — check if backtest data exists for this date"
        else:
            same_combo = v11_day.loc[
                v11_day["_ticker"].eq(ticker)
                & v11_day["_side"].eq(side)
                & v11_day["_setup"].eq(setup)
            ] if not v11_day.empty else pd.DataFrame()

            if same_combo.empty:
                cause = "no_v11_signal_for_setup"
                note = f"v11 has no {setup} {side} signal for {ticker} on {day}. Check: gate/overlay filter, pre-momentum block, or setup not in SELECTED_STRATEGY"
            else:
                # Check timing drift
                if pd.notna(ts):
                    deltas = [
                        abs((t - ts).total_seconds())
                        for t in same_combo.get("_ts_ist", pd.Series(dtype=object))
                        if pd.notna(t)
                    ]
                    min_delta = min(deltas) if deltas else np.inf
                    if min_delta <= 300:
                        cause = "slot_timing_drift_within_5min"
                        note = f"nearest v11 signal is {min_delta:.0f}s away — same slot, key mismatch due to timestamp offset"
                    elif min_delta <= 1800:
                        cause = "timing_drift_within_30min"
                        note = f"nearest v11 signal is {min_delta/60:.1f}min away — different slot; possible race or bar boundary difference"
                    else:
                        cause = "different_slot"
                        note = f"v11 has {setup} {side} {ticker} but at different time; paper trade may be from a later re-entry attempt"
                else:
                    cause = "timestamp_missing"
                    note = "paper trade has no parseable timestamp"

        rows.append(
            {
                "ticker": ticker,
                "side": side,
                "setup": setup,
                "paper_signal_time": str(ts) if pd.notna(ts) else "",
                "paper_pnl_rs": _fmt_num(row.get("pnl_rs", np.nan), 2),
                "paper_outcome": outcome,
                "root_cause": cause,
                "diagnostic_note": note,
            }
        )
    return pd.DataFrame(rows)


def _root_cause_v11_only(v11_only: pd.DataFrame, paper: pd.DataFrame, day: str) -> pd.DataFrame:
    """
    For each v11-only trade (backtested but not in paper), diagnose WHY.

    Possible causes:
    1. paper_anti_chase_skip  — paper has a skipped row for same signal
    2. paper_entry_skipped    — paper executor skipped on capacity/duplicate/PF gate
    3. timing_drift           — paper has signal at same (ticker, side, setup) at different time
    4. no_paper_signal        — paper has no record at all (feed race, late detection, or live scanner dropped it)
    5. v11_phantom            — v11 generated a signal that the live scanner didn't (using parquet vs live data differences)
    """
    if v11_only.empty:
        return pd.DataFrame()
    paper_day = paper.copy() if not paper.empty else pd.DataFrame()
    rows: list[dict[str, Any]] = []

    for _, row in v11_only.iterrows():
        ticker = str(row.get("_ticker", row.get("ticker", ""))).upper()
        side = str(row.get("_side", row.get("side", ""))).upper()
        setup = str(row.get("_setup", row.get("setup", "")))
        ts = row.get("_ts_ist", pd.NaT)
        v11_pnl = _fmt_num(_row_pnl(row), 2)

        if paper_day.empty:
            cause = "no_paper_file"
            note = f"No paper_trades_{day}_id_5min_v7.csv found; live executor may not have run"
        else:
            same_combo = paper_day.loc[
                paper_day.get("_ticker", pd.Series(dtype=str)).astype(str).eq(ticker)
                & paper_day.get("_side", pd.Series(dtype=str)).astype(str).eq(side)
                & paper_day.get("_setup", pd.Series(dtype=str)).astype(str).eq(setup)
            ] if not paper_day.empty else pd.DataFrame()

            if same_combo.empty:
                cause = "no_paper_signal"
                note = (
                    f"Paper has no {setup} {side} record for {ticker} on {day}. "
                    "Possible: live scanner missed it (data lag, feed race), or "
                    "live V8 gate/research filter blocked with different feature values "
                    "(parquet vs Zerodha real-time divergence)"
                )
            else:
                skipped = same_combo.loc[
                    same_combo.get("outcome", pd.Series(dtype=str)).fillna("").astype(str).str.upper().str.startswith("ENTRY_SKIPPED")
                ]
                if not skipped.empty:
                    cause = "paper_entry_skipped"
                    skip_reason = str(skipped.iloc[0].get("outcome", ""))
                    note = f"Paper has ENTRY_SKIPPED for this signal: {skip_reason}"
                else:
                    if pd.notna(ts):
                        deltas = [
                            abs((t - ts).total_seconds())
                            for t in same_combo.get("_ts_ist", pd.Series(dtype=object))
                            if pd.notna(t)
                        ]
                        min_delta = min(deltas) if deltas else np.inf
                        if min_delta <= 300:
                            cause = "slot_timing_drift_within_5min"
                            note = f"Paper signal is {min_delta:.0f}s away — timestamp format difference"
                        elif min_delta <= 1800:
                            cause = "timing_drift_within_30min"
                            note = f"Paper {setup} {side} {ticker} at different slot ({min_delta/60:.1f}min gap)"
                        else:
                            cause = "different_slot"
                            note = f"Paper has {setup} {side} {ticker} but at a very different time"
                    else:
                        cause = "timestamp_missing"
                        note = "v11 trade has no parseable timestamp"

        rows.append(
            {
                "ticker": ticker,
                "side": side,
                "setup": setup,
                "v11_signal_time": str(ts) if pd.notna(ts) else "",
                "v11_pnl_rs": v11_pnl,
                "root_cause": cause,
                "diagnostic_note": note,
            }
        )
    return pd.DataFrame(rows)


def _parity_verdict(score: float, matched: int, total_paper: int) -> str:
    if total_paper == 0:
        return "NO_PAPER_TRADES"
    if score >= 90:
        return "EXCELLENT"
    if score >= 75:
        return "GOOD"
    if score >= 50:
        return "FAIR_INVESTIGATE"
    if score >= 25:
        return "POOR_ACTION_NEEDED"
    return "CRITICAL_MISMATCH"


# ---------------------------------------------------------------------------
# V11 runner
# ---------------------------------------------------------------------------

def _run_v11(day: str, mode: str, out_dir: Path, selected_strategy_profile: str) -> tuple[int, str]:
    cmd = [
        str(PYTHON_EXE),
        "-u",
        str(V11_SCRIPT),
        "--out",
        str(out_dir),
        "--cost_model",
        "statutory",
        "--slippage_bps",
        "0",
    ]
    if selected_strategy_profile:
        cmd += ["--selected_strategy_profile", str(selected_strategy_profile)]
    if mode == "live_parity":
        cmd += ["--mode", "live_parity", "--live_date", day,
                "--live_candidate_json_dir", str(LIVE_CANDIDATE_JSON_DIR)]
    else:
        cmd += ["--mode", "historical_full_day", "--historical_date", day]
    result = subprocess.run(cmd, cwd=str(BASE_DIR), capture_output=True, text=True, timeout=600)
    return int(result.returncode), (result.stdout or "") + (result.stderr or "")


def _live_json_exists_for_day(day: str) -> bool:
    if not LIVE_CANDIDATE_JSON_DIR.exists():
        return False
    day_compact = day.replace("-", "")
    return any(
        LIVE_CANDIDATE_JSON_DIR.glob(f"*{day_compact}*.json")
    ) or any(LIVE_CANDIDATE_JSON_DIR.glob(f"*{day}*.json"))


# ---------------------------------------------------------------------------
# Summary helper
# ---------------------------------------------------------------------------

def _summary(df: pd.DataFrame, pnl_col: str) -> dict[str, Any]:
    if df.empty:
        return {
            "trades": 0, "wins": 0, "losses": 0, "gross": 0.0,
            "cost": 0.0, "net": 0.0, "pf": np.nan, "win_rate_pct": 0.0,
        }
    pnl = _pnl_series(df, pnl_col)
    gross = pd.to_numeric(
        _series_from_any(df, ("gross_pnl_rs", "v6_gross_pnl_rs", "gross_pnl"), np.nan),
        errors="coerce",
    )
    gross = gross.where(gross.notna(), pnl)
    cost = pd.to_numeric(
        _series_from_any(df, ("total_cost_rs", "v6_cost_rs", "total_cost"), 0.0),
        errors="coerce",
    ).fillna(0.0)
    wins = int((pnl > 0).sum())
    losses = int((pnl < 0).sum())
    return {
        "trades": int(len(df)),
        "wins": wins,
        "losses": losses,
        "gross": float(gross.sum()),
        "cost": float(cost.sum()),
        "net": float(pnl.sum()),
        "pf": _profit_factor(pnl),
        "win_rate_pct": round(100.0 * wins / max(len(df), 1), 1),
    }


# ---------------------------------------------------------------------------
# Build report
# ---------------------------------------------------------------------------

def build_report(
    day: str,
    *,
    run_v11: bool = True,
    selected_strategy_profile: str = "",
    force_historical: bool = False,
) -> int:
    out_dir = OUT_ROOT / day
    out_dir.mkdir(parents=True, exist_ok=True)
    selected_strategy_profile = str(selected_strategy_profile or _default_selected_strategy_profile())

    # --- Run v11 backtester ---
    mode_used = "skipped"
    log_text = ""
    rc = 0

    if run_v11:
        # Try live_parity first (uses actual live signals), fall back to historical
        if _live_json_exists_for_day(day) and not force_historical:
            mode_used = "live_parity"
            rc, log_text = _run_v11(day, "live_parity", out_dir, selected_strategy_profile)
            if rc != 0:
                (out_dir / "v11_live_parity_run.log").write_text(log_text, encoding="utf-8")
                # fall back
                mode_used = "historical_full_day_fallback"
                rc2, log2 = _run_v11(day, "historical_full_day", out_dir, selected_strategy_profile)
                log_text += f"\n\n--- FALLBACK historical_full_day (rc={rc2}) ---\n{log2}"
                rc = rc2
        else:
            mode_used = "historical_full_day"
            rc, log_text = _run_v11(day, "historical_full_day", out_dir, selected_strategy_profile)
        (out_dir / "v11_run.log").write_text(log_text, encoding="utf-8")

    # --- Load outputs ---
    v11 = _read_csv(out_dir / "v11_ID_trades.csv")
    if v11.empty:
        v11 = _read_csv(out_dir / "trades.csv")
    paper = _read_csv(_paper_path(day))

    # Filter paper to executed trades only (exclude ENTRY_SKIPPED)
    paper_executed = paper.copy()
    if not paper.empty and "outcome" in paper.columns:
        paper_executed = paper.loc[
            ~paper["outcome"].fillna("").astype(str).str.upper().str.startswith("ENTRY_SKIPPED")
        ].copy()

    # --- Enrich keys ---
    v11 = _enrich_keys(v11, source="v11") if not v11.empty else v11
    paper = _enrich_keys(paper, source="paper") if not paper.empty else paper
    paper_executed = _enrich_keys(paper_executed, source="paper") if not paper_executed.empty else paper_executed

    v11_summary = _summary(v11, BACKTEST_PNL_COL)
    paper_summary = _summary(paper_executed, "pnl_rs")
    paper_all_summary = _summary(paper, "pnl_rs")
    paper_skipped = (
        paper.loc[
            paper.get("outcome", pd.Series("", index=paper.index))
            .fillna("")
            .astype(str)
            .str.upper()
            .str.startswith("ENTRY_SKIPPED")
        ].copy()
        if not paper.empty
        else pd.DataFrame()
    )
    paper_skip_summary = (
        paper_skipped.get("outcome", pd.Series(dtype=str))
        .fillna("ENTRY_SKIPPED_UNKNOWN")
        .astype(str)
        .value_counts()
        .rename_axis("outcome")
        .reset_index(name="rows")
        if not paper_skipped.empty
        else pd.DataFrame()
    )
    scanner_funnel = _load_funnel_day(
        SCANNER_AUDIT_DIR, "candidate_decision_funnel", day
    )
    entry_funnel = _load_funnel_day(
        ENTRY_ENGINE_AUDIT_DIR, "entry_decision_funnel", day
    )
    scanner_funnel_summary = _funnel_summary(scanner_funnel)
    entry_funnel_summary = _funnel_summary(entry_funnel)
    latency_summary = _entry_latency_summary(day)

    # --- Match analysis ---
    match = _match_sets(v11, paper_executed)

    v11_only = v11.loc[match["v11_only_idx"]].copy() if match["v11_only_idx"] else pd.DataFrame()
    paper_only = paper_executed.loc[match["paper_only_idx"]].copy() if match["paper_only_idx"] else pd.DataFrame()
    v11_matched = v11.loc[match["v11_match_idx"]].copy() if match["v11_match_idx"] else pd.DataFrame()
    paper_matched = paper_executed.loc[match["paper_match_idx"]].copy() if match["paper_match_idx"] else pd.DataFrame()

    # --- Deep analysis ---
    entry_gaps = _entry_price_gap(v11, paper_executed, match)
    setup_parity = _per_setup_parity(v11, paper_executed)
    paper_only_causes = _root_cause_paper_only(paper_only, v11, day)
    v11_only_causes = _root_cause_v11_only(v11_only, paper_executed, day)

    parity_score = match["parity_score_pct"]
    verdict = _parity_verdict(parity_score, match["matched_slot"], len(paper_executed))

    # --- Matched PnL correlation ---
    avg_entry_gap_pct = np.nan
    avg_pnl_gap_rs = np.nan
    if not entry_gaps.empty:
        avg_entry_gap_pct = pd.to_numeric(entry_gaps.get("entry_gap_pct"), errors="coerce").abs().mean()
        avg_pnl_gap_rs = pd.to_numeric(entry_gaps.get("pnl_gap_v11_minus_paper"), errors="coerce").mean()

    # --- Root cause summary ---
    cause_summary: dict[str, int] = {}
    for df in (paper_only_causes, v11_only_causes):
        if not df.empty and "root_cause" in df.columns:
            for cause, cnt in df["root_cause"].value_counts().items():
                cause_summary[str(cause)] = cause_summary.get(str(cause), 0) + int(cnt)

    # --- Report ---
    parity_bar = "█" * int(parity_score / 5) + "░" * (20 - int(parity_score / 5))
    mode_note = {
        "live_parity": "Used actual live scanner JSON candidates (best parity mode)",
        "historical_full_day": "Reconstructed signals from parquet (live JSON not found; minor feature drift expected)",
        "historical_full_day_fallback": "live_parity failed; fell back to parquet reconstruction",
        "skipped": "v11 backtest not run (--no-run-v11 flag)",
    }.get(mode_used, mode_used)

    lines = [
        f"# Backtesting Result v11 — {day}",
        "",
        f"**Parity Score: {parity_score:.1f}% [{parity_bar}] → {verdict}**",
        "",
        "V11 economics use V7 risk-based quantity and PAPER_TRUE statutory costs.",
        "",
        "## Run Status",
        "",
        f"| field | value |",
        f"|---|---|",
        f"| mode | `{mode_used}` |",
        f"| selected strategy profile | `{selected_strategy_profile}` |",
        f"| mode note | {mode_note} |",
        f"| v11 exit code | {rc} |",
        f"| v11 output dir | `{out_dir}` |",
        f"| paper TRUE file | `{_paper_path(day)}` |",
        f"| paper TRUE exists | {_paper_path(day).exists()} |",
        "",
        "## Result Comparison",
        "",
        "| source | trades | wins | losses | win_rate | gross P&L | costs | net P&L | PF |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|",
        f"| v11 backtest ({mode_used}) | {v11_summary['trades']} | {v11_summary['wins']} | {v11_summary['losses']} | {v11_summary['win_rate_pct']:.1f}% | Rs {_fmt_num(v11_summary['gross'])} | Rs {_fmt_num(v11_summary['cost'])} | Rs {_fmt_num(v11_summary['net'])} | {_fmt_pf(v11_summary['pf'])} |",
        f"| V7 paper TRUE (executed only) | {paper_summary['trades']} | {paper_summary['wins']} | {paper_summary['losses']} | {paper_summary['win_rate_pct']:.1f}% | Rs {_fmt_num(paper_summary['gross'])} | Rs {_fmt_num(paper_summary['cost'])} | Rs {_fmt_num(paper_summary['net'])} | {_fmt_pf(paper_summary['pf'])} |",
        f"| V7 paper TRUE (all incl skips) | {paper_all_summary['trades']} | — | — | — | — | — | — | — |",
        "",
        "## Parity Analysis",
        "",
        f"| metric | value |",
        f"|---|---|",
        f"| parity score (matched/paper_executed) | **{parity_score:.1f}%** |",
        f"| verdict | **{verdict}** |",
        f"| matched (slot-key, 5-min window) | {match['matched_slot']} |",
        f"| exact timestamp matches | {match['matched_exact']} |",
        f"| v11-only (backtest has, paper doesn't) | {match['only_v11_slot']} |",
        f"| paper-only (live has, backtest doesn't) | {match['only_paper_slot']} |",
        f"| avg entry price gap (matched) | {_fmt_num(avg_entry_gap_pct, 3)}% |",
        f"| avg P&L gap v11 minus paper (matched) | Rs {_fmt_num(avg_pnl_gap_rs, 2)} |",
        "",
    ]

    lines.extend(
        [
            "## Operational Parity Scope",
            "",
            "Strategy results above are kept separate from live operational "
            "gates. Capacity, freshness, writer, or executor skips therefore "
            "cannot be mistaken for strategy-gate differences.",
            "",
            "| metric | value |",
            "|---|---:|",
            f"| scanner funnel records | {len(scanner_funnel)} |",
            f"| entry-engine funnel records | {len(entry_funnel)} |",
            f"| executor ENTRY_SKIPPED rows | {len(paper_skipped)} |",
            f"| slot-complete marker missing slots | {latency_summary.get('marker_missing_slots', 0)} |",
            f"| signal-writer rejected rows | {latency_summary.get('writer_rejected_rows', 0)} |",
            "",
            "Latency is split at the authoritative scanner marker:",
            "",
            "| latency | median sec | p95 sec | max sec |",
            "|---|---:|---:|---:|",
        ]
    )
    for key, label in (
        ("bar_close_to_candidate_ready_sec", "5m bar close → candidate ready"),
        ("candidate_ready_to_entry_engine_ready_sec", "candidate ready → entry decision"),
        ("candidate_ready_to_signal_write_sec", "candidate ready → signal written"),
        ("intended_entry_to_signal_write_sec", "intended T+1 entry → signal written"),
    ):
        lines.append(
            f"| {label} | "
            f"{_fmt_num(latency_summary.get(f'{key}_median'), 2)} | "
            f"{_fmt_num(latency_summary.get(f'{key}_p95'), 2)} | "
            f"{_fmt_num(latency_summary.get(f'{key}_max'), 2)} |"
        )
    lines.append("")
    if not paper_skip_summary.empty:
        lines.extend(["Executor skips:", "", "| outcome | rows |", "|---|---:|"])
        for _, row in paper_skip_summary.iterrows():
            lines.append(f"| {row['outcome']} | {int(row['rows'])} |")
        lines.append("")
    if not entry_funnel_summary.empty:
        lines.extend(
            [
                "Entry-engine rejection funnel:",
                "",
                "| stage | outcome | candidates | reason |",
                "|---|---|---:|---|",
            ]
        )
        rejected_summary = entry_funnel_summary.loc[
            entry_funnel_summary["outcome"].astype(str).str.upper().eq("REJECT")
        ]
        for _, row in rejected_summary.head(30).iterrows():
            reason_text = str(row["reason"]).replace("|", "\\|")[:120]
            lines.append(
                f"| {row['stage']} | {row['outcome']} | "
                f"{int(row['candidates'])} | {reason_text} |"
            )
        if rejected_summary.empty:
            lines.append("| — | — | 0 | no entry-engine rejections |")
        lines.append("")

    # Per-setup parity
    lines.extend(["## Per-Setup Parity", ""])
    lines.append(
        "`parity_%` = matched/paper_executed per setup. "
        "<60% means significant divergence for that setup — investigate entry gate or feature drift."
    )
    lines.append("")
    if not setup_parity.empty:
        scols = list(setup_parity.columns)
        lines.append("| " + " | ".join(scols) + " |")
        lines.append("| " + " | ".join("---" for _ in scols) + " |")
        for _, row in setup_parity.iterrows():
            lines.append("| " + " | ".join(str(row.get(c, "")).replace("|", "\\|") for c in scols) + " |")
    else:
        lines.append("No setup data.")
    lines.append("")

    # Entry price & PnL gap for matched trades
    lines.extend(["## Matched Trade Entry/PnL Comparison", ""])
    if not entry_gaps.empty:
        lines.append(
            "`entry_gap_pct` = (v11_entry − paper_entry) / paper_entry × 100. "
            "Near 0 = good fill model parity. Large gap = v11 uses different entry assumption than live."
        )
        lines.append("")
        ecols = list(entry_gaps.columns)
        lines.append("| " + " | ".join(ecols) + " |")
        lines.append("| " + " | ".join("---" for _ in ecols) + " |")
        for _, row in entry_gaps.head(30).iterrows():
            lines.append("| " + " | ".join(str(row.get(c, "")).replace("|", "\\|") for c in ecols) + " |")
    else:
        lines.append("No matched trades to compare.")
    lines.append("")

    # Root cause summary
    lines.extend(["## Root Cause Summary", ""])
    if cause_summary:
        lines.append("| root cause | count | interpretation |")
        lines.append("|---|---:|---|")
        cause_interpret = {
            "no_v11_signal_for_setup": "v11 gate/overlay/entry engine blocked this setup — check SELECTED_STRATEGY, V8 gate, research filters",
            "no_paper_signal": "Live scanner didn't fire — parquet vs Zerodha feature divergence, or feed race condition",
            "paper_entry_skipped": "Paper executor ran ENTRY_SKIPPED — anti-chase, capacity, PF gate, or duplicate guard",
            "slot_timing_drift_within_5min": "Same signal, different timestamp format — key matching artifact (not a real mismatch)",
            "timing_drift_within_30min": "Same setup/ticker but different bar slot — possible re-entry or bar boundary difference",
            "different_slot": "Same setup/ticker at very different time — check if live had re-entry after initial skip",
            "no_paper_file": "Paper trades file missing — executor may not have run this day",
            "no_v11_output": "v11 produced 0 trades — check if parquet data exists for this date",
            "paper_skipped_not_a_real_trade": "Paper row is ENTRY_SKIPPED — correctly excluded from parity denominator",
            "timestamp_missing": "Timestamp could not be parsed — check CSV column names",
        }
        for cause, cnt in sorted(cause_summary.items(), key=lambda x: -x[1]):
            interp = cause_interpret.get(cause, "—")
            lines.append(f"| {cause} | {cnt} | {interp} |")
    else:
        lines.append("No discrepancies found.")
    lines.append("")

    # Paper-only detailed
    lines.extend(["## Paper-Only Trades (live traded, v11 missed)", ""])
    if not paper_only_causes.empty:
        lines.append(
            "These are the most important rows: live strategy made real (paper) trades that v11 didn't replicate. "
            "If `root_cause` is `no_v11_signal_for_setup`, the v11 gate/filter is the problem."
        )
        lines.append("")
        pcols = list(paper_only_causes.columns)
        lines.append("| " + " | ".join(pcols) + " |")
        lines.append("| " + " | ".join("---" for _ in pcols) + " |")
        for _, row in paper_only_causes.iterrows():
            lines.append("| " + " | ".join(str(row.get(c, "")).replace("|", "\\|")[:100] for c in pcols) + " |")
    else:
        lines.append("None — all paper trades found a v11 match.")
    lines.append("")

    # V11-only detailed
    lines.extend(["## V11-Only Trades (backtested, live missed)", ""])
    if not v11_only_causes.empty:
        lines.append(
            "v11 found these signals but live didn't execute them. "
            "If `root_cause` is `no_paper_signal`, the live scanner had different features than parquet reconstruction."
        )
        lines.append("")
        vcols = list(v11_only_causes.columns)
        lines.append("| " + " | ".join(vcols) + " |")
        lines.append("| " + " | ".join("---" for _ in vcols) + " |")
        for _, row in v11_only_causes.head(30).iterrows():
            lines.append("| " + " | ".join(str(row.get(c, "")).replace("|", "\\|")[:100] for c in vcols) + " |")
    else:
        lines.append("None — all v11 trades matched a paper trade.")
    lines.append("")

    # Improvement actions
    lines.extend(["## Parity Improvement Actions", ""])
    actions = []
    if parity_score < 90:
        actions.append(
            f"Parity is {parity_score:.1f}% (target ≥90%). "
            "Start with `paper-only` root causes — those represent live trades v11 fails to replicate."
        )
    if mode_used in ("historical_full_day", "historical_full_day_fallback"):
        actions.append(
            "Mode is `historical_full_day` (parquet reconstruction). "
            "For exact parity: ensure live JSON candidates exist at "
            f"`{LIVE_CANDIDATE_JSON_DIR}` for {day}. "
            "Switch to `live_parity` mode once live JSON files are available."
        )
    no_v11_count = cause_summary.get("no_v11_signal_for_setup", 0)
    if no_v11_count > 0:
        actions.append(
            f"{no_v11_count} paper-only trades have `no_v11_signal_for_setup`. "
            "Run `--parity-debug` mode to see which pipeline stage (gate, overlay, entry, pre-momentum) is blocking each."
        )
    no_paper_count = cause_summary.get("no_paper_signal", 0)
    if no_paper_count > 0:
        actions.append(
            f"{no_paper_count} v11-only trades have `no_paper_signal`. "
            "Most likely: parquet VWAP/ATR differs from live Zerodha features. "
            "Run `v7_causality_audit.py` for these tickers to check feature drift."
        )
    drift_5m = cause_summary.get("slot_timing_drift_within_5min", 0)
    if drift_5m > 0:
        actions.append(
            f"{drift_5m} rows appear as mismatches due to timestamp format differences (<5min drift). "
            "These are NOT real discrepancies — slot-key matching already handles them but exact-key counts will be lower."
        )
    entry_skipped = cause_summary.get("paper_entry_skipped", 0)
    if entry_skipped > 0:
        actions.append(
            f"{entry_skipped} paper ENTRY_SKIPPED rows (anti-chase, duplicate, capacity). "
            "V11 does not model these; they are excluded from parity denominator but visible in 'all' paper count."
        )
    if not actions:
        actions.append("Parity ≥90% — system is healthy. Monitor over multiple days to confirm stability.")
    for action in actions:
        lines.append(f"- {action}")
    lines.append("")

    if rc != 0:
        lines.extend(["## v11 Run Log Tail", ""])
        tail = "\n".join(log_text.splitlines()[-50:])
        lines.extend(["```text", tail, "```", ""])

    report = "\n".join(lines) + "\n"

    # --- Write outputs ---
    report_path = REPORTS_DIR / f"backtesting_result_v11_{day}.md"
    report_path.write_text(report, encoding="utf-8")
    (LATEST_DIR / "latest_backtesting_result_v11.md").write_text(report, encoding="utf-8")

    json_payload = {
        "day": day,
        "mode_used": mode_used,
        "selected_strategy_profile": selected_strategy_profile,
        "exit_code": rc,
        "parity_score_pct": parity_score,
        "verdict": verdict,
        "v11_summary": {k: (v if np.isfinite(v) else None) if isinstance(v, float) else v for k, v in v11_summary.items()},
        "paper_summary": {k: (v if np.isfinite(v) else None) if isinstance(v, float) else v for k, v in paper_summary.items()},
        "matched_slot": match["matched_slot"],
        "matched_exact": match["matched_exact"],
        "only_v11_slot": match["only_v11_slot"],
        "only_paper_slot": match["only_paper_slot"],
        "avg_entry_gap_pct": None if not np.isfinite(avg_entry_gap_pct) else float(avg_entry_gap_pct),
        "avg_pnl_gap_rs": None if not np.isfinite(avg_pnl_gap_rs) else float(avg_pnl_gap_rs),
        "root_cause_summary": cause_summary,
        "operational_parity": {
            "scanner_funnel_records": int(len(scanner_funnel)),
            "entry_funnel_records": int(len(entry_funnel)),
            "executor_skipped_rows": int(len(paper_skipped)),
            "entry_latency": latency_summary,
        },
        "report": str(report_path),
        "out_dir": str(out_dir),
        "backtesting_script": str(V11_SCRIPT),
    }
    (LATEST_DIR / "latest_backtesting_result_v11.json").write_text(
        json.dumps(json_payload, indent=2, sort_keys=True, default=str),
        encoding="utf-8",
    )

    v11_only.to_csv(out_dir / "v11_only_vs_paper.csv", index=False)
    paper_only.to_csv(out_dir / "paper_only_vs_v11.csv", index=False)
    v11_matched.to_csv(out_dir / "matched_v11.csv", index=False)
    paper_matched.to_csv(out_dir / "matched_paper.csv", index=False)
    entry_gaps.to_csv(out_dir / "matched_entry_pnl_gaps.csv", index=False)
    setup_parity.to_csv(out_dir / "per_setup_parity.csv", index=False)
    paper_only_causes.to_csv(out_dir / "paper_only_root_causes.csv", index=False)
    v11_only_causes.to_csv(out_dir / "v11_only_root_causes.csv", index=False)
    scanner_funnel.to_csv(out_dir / "scanner_candidate_decision_funnel.csv", index=False)
    entry_funnel.to_csv(out_dir / "entry_engine_decision_funnel.csv", index=False)
    scanner_funnel_summary.to_csv(out_dir / "scanner_funnel_summary.csv", index=False)
    entry_funnel_summary.to_csv(out_dir / "entry_funnel_summary.csv", index=False)
    paper_skip_summary.to_csv(out_dir / "executor_skip_summary.csv", index=False)

    print(report)
    return rc


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def _default_day() -> str:
    return pd.Timestamp.now(tz="Asia/Kolkata").strftime("%Y-%m-%d")


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Daily V7 parity: run v11 backtester and compare with live paper trades."
    )
    ap.add_argument("--date", default=_default_day(), help="Trading date YYYY-MM-DD (default: today IST)")
    ap.add_argument("--no-run-v11", action="store_true", help="Skip running v11; only compare existing outputs")
    ap.add_argument("--force-historical", action="store_true", help="Force historical_full_day mode even if live JSON exists")
    ap.add_argument(
        "--selected-strategy-profile",
        default=_default_selected_strategy_profile(),
        help="Profile passed to avwap_5min_ID_v11_backtesting.py; defaults from EQIDV2_V11_SELECTED_STRATEGY_PROFILE, or final_setup_conf when EQIDV2_USE_FINAL_SETUP_CONF=1",
    )
    args = ap.parse_args()
    manifest_path, _ = freeze_runtime_manifest(
        "backtesting_result_v11_daily",
        runtime_root=OUT_ROOT.parent,
        source_files=(Path(__file__), V11_SCRIPT),
        resolved_config={
            "date": str(args.date),
            "run_v11": not bool(args.no_run_v11),
            "selected_strategy_profile": str(args.selected_strategy_profile),
            "force_historical": bool(args.force_historical),
        },
    )
    print(f"[backtesting_result_v11] frozen_runtime_manifest={manifest_path}", flush=True)
    return build_report(
        str(args.date),
        run_v11=not args.no_run_v11,
        selected_strategy_profile=str(args.selected_strategy_profile),
        force_historical=bool(args.force_historical),
    )


if __name__ == "__main__":
    raise SystemExit(main())
