#!/usr/bin/env python3
"""
Daily V11 lab shadow monitor.

Runs a lab candidate config beside the current V11 config in live-parity mode
and compares both against actual V7 paper results. This is research-only:
it does not modify live scanner, entry engine, executor, or config files.
"""

from __future__ import annotations

import argparse
import datetime as dt
import importlib
import json
import math
import os
import subprocess
import sys
from pathlib import Path
from typing import Any, Iterable
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

from eqidv2_runtime_paths import LIVE_SIGNALS_DIR, RUNTIME_STATUS_DIR, runtime_dir


IST = ZoneInfo("Asia/Kolkata")
SESSION_SLUG = "v11_lab_shadow_monitor"
SESSION_ROOT = runtime_dir("v11_lab_shadow")
RUNS_DIR = SESSION_ROOT / "runs"
DAILY_DIR = SESSION_ROOT / "daily"
LATEST_DIR = SESSION_ROOT / "latest"
REPORTS_DIR = SESSION_ROOT / "reports"
HEARTBEAT_DIR = SESSION_ROOT / "heartbeat"
BASE_DIR = Path(__file__).resolve().parent

DEFAULT_BASELINE_MODULE = os.getenv("EQIDV2_V11_LAB_SHADOW_BASELINE_MODULE", "final_setup_conf_v11_working")
DEFAULT_CANDIDATE_MODULE = os.getenv("EQIDV2_V11_LAB_SHADOW_CANDIDATE_MODULE", "final_setup_conf_v11_conf_d")
DEFAULT_JSON_DIR = Path(os.getenv(
    "EQIDV2_V11_LIVE_CANDIDATE_JSON_DIR",
    str(runtime_dir("signal_discovery_v7_5mins_ID") / "json"),
))
DAILY_LEDGER = DAILY_DIR / "daily_v11_lab_shadow_results.csv"


LIVE_PARITY_ENV = {
    "PYTHONUNBUFFERED": "1",
    "PYTHONIOENCODING": "utf-8",
    "EQIDV2_RUNTIME_ROOT": str(runtime_dir()),
    "EQIDV2_USE_FINAL_SETUP_CONF": "1",
    "EQIDV2_V11_SELECTED_STRATEGY_PROFILE": "final_setup_conf",
    "EQIDV2_SIGNAL_DISCOVERY_V7_V8_GATE": "1",
    "EQIDV2_SIGNAL_DISCOVERY_V7_V8_ACCEPTED_RULES": str(runtime_dir("outputs_ID_v8_5min_research_restore") / "accepted_rules.csv"),
    "EQIDV2_SIGNAL_DISCOVERY_V7_V11_TIER123": "1",
    "EQIDV2_SIGNAL_DISCOVERY_V7_ENTRY_WINDOW_START": "09:30",
    "EQIDV2_SIGNAL_DISCOVERY_V7_ENTRY_WINDOW_END": "14:30",
    "EQIDV2_SIGNAL_DISCOVERY_V7_ENTRY_LAG_MIN": "1",
    "EQIDV2_SIGNAL_DISCOVERY_V7_SELECTION_MODE": "v8_setup_compatible",
    "EQIDV2_SIGNAL_DISCOVERY_V7_RESEARCH_FILTERS": "1",
    "EQIDV2_SIGNAL_DISCOVERY_V7_RESEARCH_FILTER_MODE": "active",
    "EQIDV2_SIGNAL_DISCOVERY_V7_LONG_ANTI_CHASE_CLOSE_LOC_GT": "0.97",
    "EQIDV2_SIGNAL_DISCOVERY_V7_LONG_ANTI_CHASE_VWAP_DIST_ATR_GT": "3.50",
    "EQIDV2_SIGNAL_DISCOVERY_V7_ANTI_CHASE_LONG_CLOSE_LOC_MIN": "0.97",
    "EQIDV2_SIGNAL_DISCOVERY_V7_ANTI_CHASE_LONG_VWAP_DIST_ATR_MIN": "3.50",
    "EQIDV2_SIGNAL_DISCOVERY_V7_B_AVWAP_RECLAIM_RANKER_MIN": "0.65",
    "EQIDV2_SIGNAL_DISCOVERY_V7_L_TREND_PULLBACK_PROBATION_BLOCK": "1",
    "EQIDV2_SIGNAL_DISCOVERY_V7_SHORT_FOCUS": "0",
    "EQIDV2_SIGNAL_DISCOVERY_V7_SHORT_FOCUS_ALLOWED_SIDES": "SHORT,LONG",
    "EQIDV2_SIGNAL_DISCOVERY_V7_SHORT_FOCUS_EXEMPT_SETUPS": "A_MOD_BREAK_C1_HIGH,C_OR_BREAKOUT",
    "EQIDV2_SIGNAL_DISCOVERY_V7_UNCOVERED_FALLBACK": "1",
    "EQIDV2_SIGNAL_DISCOVERY_V7_UNCOVERED_FALLBACK_START_TIME": "11:05",
    "EQIDV2_SIGNAL_DISCOVERY_V7_UNCOVERED_FALLBACK_END_TIME": "13:55",
    "EQIDV2_SIGNAL_DISCOVERY_V7_UNCOVERED_FALLBACK_MIN_RANKER": "0.65",
    "EQIDV2_SIGNAL_DISCOVERY_V7_UNCOVERED_FALLBACK_MIN_QUALITY": "125",
    "EQIDV2_SIGNAL_DISCOVERY_V7_UNCOVERED_FALLBACK_MAX_PER_SLOT": "1",
    "EQIDV2_SIGNAL_DISCOVERY_V7_UNCOVERED_FALLBACK_ALLOWED_SIDES": "SHORT,LONG",
    "EQIDV2_SIGNAL_DISCOVERY_V7_UNCOVERED_FALLBACK_ALLOWED_SETUPS": (
        "A_MOD_BREAK_C1_LOW,C_OR_BREAKDOWN,A_PULLBACK_C2_THEN_BREAK_C2_LOW,"
        "B_HUGE_RED_FAILED_BOUNCE,D_AVWAP_LOSE_REVERSAL,G_LOWER_LOW_BREAK,C_OR_BREAKOUT"
    ),
    "EQIDV2_SIGNAL_DISCOVERY_V7_EARLY_MODE": "1",
    "EQIDV2_SIGNAL_DISCOVERY_V7_EARLY_MIN_5M_TRADED_VALUE_RS": "1000000",
    "EQIDV2_SIGNAL_DISCOVERY_V7_EARLY_MAX_VWAP_DIST_ATR": "2.80",
    "EQIDV2_SIGNAL_DISCOVERY_V7_EARLY_TIGHT_FILTERS": "1",
    "EQIDV2_SIGNAL_DISCOVERY_V7_EARLY_BLOCKED_SETUPS": (
        "E_RS_FIRST_HOUR_BREAK_LONG,E_RS_FIRST_HOUR_BREAK_SHORT,E_VWAP_RECLAIM_EARLY_LONG,"
        "E_FAILED_OR_BREAKOUT_TRAP_SHORT,E_ORB_RETEST_HOLD_SHORT,E_ORB_RETEST_HOLD_LONG,"
        "E_FAILED_OR_BREAKDOWN_TRAP_LONG,E_GAP_HOLD_CONTINUATION_LONG,E_GAP_HOLD_CONTINUATION_SHORT,"
        "E_OPENING_DRIVE_CONTINUATION_LONG,E_OPENING_DRIVE_CONTINUATION_SHORT"
    ),
    "EQIDV2_SIGNAL_DISCOVERY_V7_EARLY_ORB_LONG_MAX_VOL_RATIO": "2.00",
    "EQIDV2_SIGNAL_DISCOVERY_V7_EARLY_ORB_LONG_MIN_RS_PCT": "4.00",
    "EQIDV2_SIGNAL_DISCOVERY_V7_EARLY_ORB_LONG_MAX_VWAP_DIST_ATR": "1.80",
    "EQIDV2_SIGNAL_DISCOVERY_V7_EARLY_GAP_LONG_MIN_RS_PCT": "3.00",
    "EQIDV2_SIGNAL_DISCOVERY_V7_EARLY_GAP_LONG_MIN_QUALITY": "160.00",
    "EQIDV2_SIGNAL_DISCOVERY_V7_EARLY_ORB_SHORT_MIN_RS_PCT": "-1.50",
    "EQIDV2_SIGNAL_DISCOVERY_V7_EARLY_ORB_SHORT_MAX_ATR_PCT": "0.0065",
    "EQIDV2_SIGNAL_DISCOVERY_V7_EARLY_ORB_SHORT_MIN_BODY_PCT": "0.82",
    "EQIDV2_SIGNAL_DISCOVERY_V7_EARLY_VWAP_SHORT_MIN_RS_PCT": "-1.20",
    "EQIDV2_SIGNAL_DISCOVERY_V7_EARLY_VWAP_SHORT_MIN_CLOSE_LOC": "0.08",
    "EQIDV2_SIGNAL_DISCOVERY_V7_EARLY_VWAP_SHORT_MAX_ATR_PCT": "0.008",
    "EQIDV2_SIGNAL_DISCOVERY_V7_EARLY_LIVE_GATE_MIN_SCORE": "95",
    "EQIDV2_SIGNAL_DISCOVERY_V7_EARLY_LIVE_GATE_MAX_PER_SIDE": "4",
    "EQIDV2_SIGNAL_DISCOVERY_V7_EARLY_LIVE_GATE_MAX_PER_SLOT": "8",
}


for _path in (SESSION_ROOT, RUNS_DIR, DAILY_DIR, LATEST_DIR, REPORTS_DIR, HEARTBEAT_DIR, RUNTIME_STATUS_DIR):
    _path.mkdir(parents=True, exist_ok=True)


def _now_ist() -> dt.datetime:
    return dt.datetime.now(IST)


def _default_day() -> str:
    day = _now_ist().date()
    while day.weekday() >= 5:
        day -= dt.timedelta(days=1)
    return day.isoformat()


def _write_text_atomic(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(text, encoding="utf-8", errors="replace")
    tmp.replace(path)


def _json_sanitize(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _json_sanitize(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_sanitize(v) for v in value]
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        f = float(value)
        return f if math.isfinite(f) else None
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, (pd.Timestamp, dt.datetime, dt.date)):
        return str(value)
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    return value


def _write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    _write_text_atomic(path, json.dumps(_json_sanitize(payload), indent=2, sort_keys=True))


def _write_status(state: str, **extra: Any) -> None:
    now = _now_ist().isoformat()
    payload = {"status": state, "state": state, "ts_ist": now, "session": SESSION_SLUG, **extra}
    text = "\n".join(f"{k}={v}" for k, v in payload.items()) + "\n"
    _write_text_atomic(RUNTIME_STATUS_DIR / f"{SESSION_SLUG}.status", text)
    _write_text_atomic(RUNTIME_STATUS_DIR / f"{SESSION_SLUG}.heartbeat", text)
    _write_json_atomic(HEARTBEAT_DIR / f"{SESSION_SLUG}.status.json", payload)


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists() or path.stat().st_size <= 2:
        return pd.DataFrame()
    try:
        return pd.read_csv(path, low_memory=False)
    except pd.errors.EmptyDataError:
        return pd.DataFrame()


def _profit_factor(gross_profit: float, gross_loss: float) -> float:
    if gross_loss > 0:
        return gross_profit / gross_loss
    return float("inf") if gross_profit > 0 else 0.0


def _summarize_trades(trades: pd.DataFrame, pnl_col: str = "pnl") -> dict[str, Any]:
    if trades.empty or pnl_col not in trades.columns:
        return {
            "trades": 0, "wins": 0, "losses": 0, "win_rate_pct": 0.0,
            "net_pnl_rs": 0.0, "gross_profit_rs": 0.0, "gross_loss_rs": 0.0,
            "profit_factor": 0.0, "target_hits": 0, "sl_hits": 0, "eod_exits": 0,
        }
    pnl = pd.to_numeric(trades[pnl_col], errors="coerce").fillna(0.0)
    gross_profit = float(pnl[pnl > 0].sum())
    gross_loss = float(-pnl[pnl < 0].sum())
    outcome = trades.get("exit_reason", trades.get("outcome", pd.Series("", index=trades.index))).astype(str).str.upper()
    return {
        "trades": int(len(trades)),
        "wins": int((pnl > 0).sum()),
        "losses": int((pnl < 0).sum()),
        "win_rate_pct": round(float((pnl > 0).mean() * 100.0), 2) if len(pnl) else 0.0,
        "net_pnl_rs": round(float(pnl.sum()), 2),
        "gross_profit_rs": round(gross_profit, 2),
        "gross_loss_rs": round(gross_loss, 2),
        "profit_factor": round(_profit_factor(gross_profit, gross_loss), 3),
        "target_hits": int(outcome.eq("TARGET").sum()),
        "sl_hits": int(outcome.isin(["SL", "STOP", "STOP_LOSS"]).sum()),
        "eod_exits": int(outcome.eq("EOD").sum()),
    }


def _setup_summary(trades: pd.DataFrame, pnl_col: str = "pnl") -> pd.DataFrame:
    if trades.empty or "setup_name" not in trades.columns:
        return pd.DataFrame()
    rows: list[dict[str, Any]] = []
    side_col = "side" if "side" in trades.columns else None
    for keys, group in trades.groupby(([side_col] if side_col else []) + ["setup_name"], dropna=False):
        if not isinstance(keys, tuple):
            keys = ("", keys) if not side_col else (keys,)
        if side_col:
            side, setup = keys
        else:
            side, setup = "", keys[-1]
        metrics = _summarize_trades(group, pnl_col)
        rows.append({"side": side, "setup": setup, **metrics})
    return pd.DataFrame(rows).sort_values("net_pnl_rs", ascending=False).reset_index(drop=True)


def _trade_key(df: pd.DataFrame) -> pd.Series:
    if df.empty:
        return pd.Series([], dtype=str)
    setup_col = "setup_name" if "setup_name" in df.columns else "setup"
    return (
        df.get("date", "").astype(str) + "|"
        + df.get("symbol", df.get("ticker", "")).astype(str) + "|"
        + df.get("side", "").astype(str) + "|"
        + df.get(setup_col, "").astype(str) + "|"
        + df.get("entry_time", df.get("signal_entry_datetime_ist", "")).astype(str)
    )


def _module_summary(module_name: str) -> dict[str, Any]:
    try:
        mod = importlib.import_module(module_name)
        final_conf = getattr(mod, "FINAL_SETUP_CONF", {})
        watch_conf = getattr(mod, "RESEARCH_WATCH_CONF", {})
        return {
            "module": module_name,
            "active_count": len(final_conf),
            "active_setups": sorted(map(str, final_conf.keys())),
            "watch_count": len(watch_conf),
        }
    except Exception as exc:
        return {"module": module_name, "import_error": f"{type(exc).__name__}: {exc}"}


def _run_verify(day: str) -> tuple[int, str]:
    script = BASE_DIR / "data_for_backtesting_verify.py"
    if not script.exists():
        return 0, "verify script missing; skipped"
    proc = subprocess.run(
        [sys.executable, "-u", str(script), "--date", day],
        cwd=str(BASE_DIR),
        text=True,
        capture_output=True,
        timeout=600,
    )
    return int(proc.returncode), (proc.stdout or "") + (proc.stderr or "")


def _run_v11(day: str, module_name: str, label: str, json_dir: Path) -> tuple[pd.DataFrame, dict[str, Any], Path]:
    out_dir = RUNS_DIR / day / label
    out_dir.mkdir(parents=True, exist_ok=True)
    env = os.environ.copy()
    env.update(LIVE_PARITY_ENV)
    # The neutral setup-book variable is the authoritative V7/V11 parity
    # contract.  Keep the legacy V11 variable for compatibility, but never
    # launch a shadow run with only the legacy selector: the frozen runtime
    # manifest deliberately fails closed in that case.
    env["EQIDV2_FINAL_SETUP_CONF_MODULE"] = module_name
    env["EQIDV2_EXPECT_FINAL_SETUP_CONF_MODULE"] = module_name
    env["EQIDV2_V11_FINAL_SETUP_CONF_MODULE"] = module_name
    cmd = [
        sys.executable, "-u", str(BASE_DIR / "avwap_5min_ID_v11_backtesting.py"),
        "--out", str(out_dir),
        "--selected_strategy_profile", "final_setup_conf",
        "--mode", "live_parity",
        "--live_date", day,
        "--live_candidate_json_dir", str(json_dir),
    ]
    proc = subprocess.run(cmd, cwd=str(BASE_DIR), text=True, capture_output=True, env=env, timeout=1800)
    _write_text_atomic(
        out_dir / "shadow_monitor_run.log",
        f"cmd={' '.join(cmd)}\nmodule={module_name}\nreturncode={proc.returncode}\n\nSTDOUT\n{proc.stdout}\n\nSTDERR\n{proc.stderr}\n",
    )
    if proc.returncode != 0:
        raise RuntimeError(f"{label} backtest failed exit={proc.returncode}; see {out_dir / 'shadow_monitor_run.log'}")
    trades = _read_csv(out_dir / "v11_ID_trades.csv")
    if not trades.empty:
        trades["date"] = trades.get("date", day).astype(str)
        trades["pnl"] = pd.to_numeric(trades.get("pnl", 0.0), errors="coerce").fillna(0.0)
    return trades, _summarize_trades(trades, "pnl"), out_dir


def _load_live_paper(day: str) -> tuple[pd.DataFrame, dict[str, Any], Path | None, str]:
    exact = LIVE_SIGNALS_DIR / f"paper_trades_{day}_id_5min_v7.csv"
    matches = sorted(LIVE_SIGNALS_DIR.glob(f"paper_trades_{day}*_id_5min_v7.csv"))
    path = exact if exact.exists() else (matches[-1] if matches else None)
    if path is None:
        return pd.DataFrame(), _summarize_trades(pd.DataFrame(), "net_pnl_rs"), None, "MISSING"
    trades = _read_csv(path)
    if trades.empty:
        return trades, _summarize_trades(trades, "net_pnl_rs"), path, "EMPTY"
    pnl_col = "net_pnl_rs" if "net_pnl_rs" in trades.columns else ("net_pnl" if "net_pnl" in trades.columns else "pnl_rs" if "pnl_rs" in trades.columns else "pnl")
    if "setup_name" not in trades.columns and "setup" in trades.columns:
        trades["setup_name"] = trades["setup"]
    if "symbol" not in trades.columns and "ticker" in trades.columns:
        trades["symbol"] = trades["ticker"]
    if "exit_reason" not in trades.columns and "outcome" in trades.columns:
        trades["exit_reason"] = trades["outcome"]
    if "entry_time" not in trades.columns and "signal_entry_datetime_ist" in trades.columns:
        trades["entry_time"] = trades["signal_entry_datetime_ist"]
    trades[pnl_col] = pd.to_numeric(trades[pnl_col], errors="coerce").fillna(0.0)
    metrics = _summarize_trades(trades, pnl_col)
    return trades, metrics, path, "FOUND"


def _merge_ledger(row: dict[str, Any]) -> pd.DataFrame:
    old = _read_csv(DAILY_LEDGER)
    new = pd.DataFrame([row])
    merged = pd.concat([old, new], ignore_index=True, sort=False) if not old.empty else new
    merged = merged.drop_duplicates(["date", "baseline_module", "candidate_module"], keep="last")
    merged = merged.sort_values("date").reset_index(drop=True)
    merged.to_csv(DAILY_LEDGER, index=False)
    return merged


def _rolling(ledger: pd.DataFrame, baseline_module: str, candidate_module: str) -> dict[str, Any]:
    if ledger.empty:
        return {"tracked_days": 0, "recommendation": "COLLECT_MORE_DATA"}
    part = ledger[
        ledger["baseline_module"].astype(str).eq(baseline_module)
        & ledger["candidate_module"].astype(str).eq(candidate_module)
        & ledger["status"].astype(str).eq("DONE")
    ].copy()
    if part.empty:
        return {"tracked_days": 0, "recommendation": "COLLECT_MORE_DATA"}
    part = part.sort_values("date")

    def block(df: pd.DataFrame, prefix: str) -> dict[str, Any]:
        c_net = float(pd.to_numeric(df["candidate_net_pnl_rs"], errors="coerce").fillna(0).sum())
        b_net = float(pd.to_numeric(df["baseline_net_pnl_rs"], errors="coerce").fillna(0).sum())
        c_gp = float(pd.to_numeric(df["candidate_gross_profit_rs"], errors="coerce").fillna(0).sum())
        c_gl = float(pd.to_numeric(df["candidate_gross_loss_rs"], errors="coerce").fillna(0).sum())
        b_gp = float(pd.to_numeric(df["baseline_gross_profit_rs"], errors="coerce").fillna(0).sum())
        b_gl = float(pd.to_numeric(df["baseline_gross_loss_rs"], errors="coerce").fillna(0).sum())
        best = float(pd.to_numeric(df["candidate_net_pnl_rs"], errors="coerce").fillna(0).max()) if not df.empty else 0.0
        pos_sum = float(pd.to_numeric(df.loc[pd.to_numeric(df["candidate_net_pnl_rs"], errors="coerce").fillna(0) > 0, "candidate_net_pnl_rs"], errors="coerce").fillna(0).sum()) if not df.empty else 0.0
        return {
            f"{prefix}_days": int(len(df)),
            f"{prefix}_candidate_trades": int(pd.to_numeric(df["candidate_trades"], errors="coerce").fillna(0).sum()),
            f"{prefix}_baseline_trades": int(pd.to_numeric(df["baseline_trades"], errors="coerce").fillna(0).sum()),
            f"{prefix}_candidate_net_pnl_rs": round(c_net, 2),
            f"{prefix}_baseline_net_pnl_rs": round(b_net, 2),
            f"{prefix}_delta_rs": round(c_net - b_net, 2),
            f"{prefix}_candidate_pf": round(_profit_factor(c_gp, c_gl), 3),
            f"{prefix}_baseline_pf": round(_profit_factor(b_gp, b_gl), 3),
            f"{prefix}_candidate_positive_days": int((pd.to_numeric(df["candidate_net_pnl_rs"], errors="coerce").fillna(0) > 0).sum()),
            f"{prefix}_candidate_negative_days": int((pd.to_numeric(df["candidate_net_pnl_rs"], errors="coerce").fillna(0) < 0).sum()),
            f"{prefix}_baseline_negative_days": int((pd.to_numeric(df["baseline_net_pnl_rs"], errors="coerce").fillna(0) < 0).sum()),
            f"{prefix}_candidate_best_day_share": round(best / pos_sum, 4) if best > 0 and pos_sum > 0 else 0.0,
            f"{prefix}_candidate_without_best_day_rs": round(c_net - best, 2),
        }

    total = block(part, "total")
    last5 = block(part.tail(5), "last5")
    last10 = block(part.tail(10), "last10")
    tracked = int(total["total_days"])
    recommendation = "COLLECT_MORE_DATA"
    if tracked >= 5:
        if (
            total["total_candidate_net_pnl_rs"] > total["total_baseline_net_pnl_rs"]
            and total["total_candidate_net_pnl_rs"] > 0
            and total["total_candidate_pf"] >= 1.25
            and last5["last5_candidate_net_pnl_rs"] > 0
            and last5["last5_candidate_pf"] >= 1.15
            and total["total_candidate_without_best_day_rs"] > 0
            and total["total_candidate_best_day_share"] <= 0.45
        ):
            recommendation = "READY_FOR_REVIEW_SHADOW_ONLY"
        elif total["total_candidate_net_pnl_rs"] < 0 or last5["last5_candidate_net_pnl_rs"] < 0:
            recommendation = "REJECT_OR_REWORK_REVIEW"
        else:
            recommendation = "CONTINUE_SHADOW"
    return {"tracked_days": tracked, "recommendation": recommendation, **total, **last5, **last10}


def _md_table(df: pd.DataFrame, columns: list[str], max_rows: int = 30) -> list[str]:
    if df.empty:
        return ["_No rows._"]
    rows = df.head(max_rows)
    out = ["| " + " | ".join(columns) + " |", "| " + " | ".join("---" for _ in columns) + " |"]
    for _, row in rows.iterrows():
        vals: list[str] = []
        for col in columns:
            val = row.get(col, "")
            if isinstance(val, (float, np.floating)):
                vals.append("inf" if math.isinf(float(val)) else f"{float(val):,.2f}")
            else:
                vals.append(str(val).replace("|", "/"))
        out.append("| " + " | ".join(vals) + " |")
    return out


def _build_report(
    day: str,
    baseline_module: str,
    candidate_module: str,
    baseline: dict[str, Any],
    candidate: dict[str, Any],
    live_paper: dict[str, Any],
    live_paper_status: str,
    live_paper_path: Path | None,
    baseline_trades: pd.DataFrame,
    candidate_trades: pd.DataFrame,
    added: pd.DataFrame,
    removed: pd.DataFrame,
    ledger: pd.DataFrame,
    rolling: dict[str, Any],
    out_dirs: dict[str, Path],
) -> str:
    overview = pd.DataFrame([
        {"book": "current_v11", "module_or_source": baseline_module, **baseline},
        {"book": "candidate_shadow", "module_or_source": candidate_module, **candidate},
        {"book": "v7_paper_actual", "module_or_source": str(live_paper_path or live_paper_status), **live_paper},
    ])
    delta = float(candidate.get("net_pnl_rs", 0.0)) - float(baseline.get("net_pnl_rs", 0.0))
    lines = [
        f"# V11 Lab Shadow Monitor - {day}",
        "",
        "Shadow-only EOD monitor. It runs V11 live-parity comparisons and reads V7 paper results; it does not modify live trading code or production config.",
        "",
        "## Books",
        f"- Current V11 baseline: `{baseline_module}`",
        f"- Shadow candidate: `{candidate_module}`",
        f"- V7 paper actual: `{live_paper_path or live_paper_status}`",
        "- P&L note: V11 live-parity runs use the backtester price-path model and v7 signal quantity; V7 paper actual uses the paper trade file's net P&L when available.",
        "",
        "## Today Summary",
    ]
    lines.extend(_md_table(
        overview,
        ["book", "module_or_source", "trades", "wins", "losses", "win_rate_pct", "net_pnl_rs", "gross_profit_rs", "gross_loss_rs", "profit_factor", "target_hits", "sl_hits", "eod_exits"],
        5,
    ))
    lines.extend([
        "",
        f"Candidate vs current V11 delta today: `Rs {delta:,.2f}`.",
        "",
        "## Current V11 By Setup",
    ])
    lines.extend(_md_table(_setup_summary(baseline_trades), ["side", "setup", "trades", "net_pnl_rs", "profit_factor", "win_rate_pct"], 30))
    lines.extend(["", "## Candidate Shadow By Setup"])
    lines.extend(_md_table(_setup_summary(candidate_trades), ["side", "setup", "trades", "net_pnl_rs", "profit_factor", "win_rate_pct"], 30))
    lines.extend(["", "## Candidate Added Trades vs Current V11"])
    lines.extend(_md_table(added, ["date", "symbol", "side", "setup_name", "entry_time", "exit_reason", "pnl"], 25))
    lines.extend(["", "## Candidate Removed Trades vs Current V11"])
    lines.extend(_md_table(removed, ["date", "symbol", "side", "setup_name", "entry_time", "exit_reason", "pnl"], 25))
    roll_df = pd.DataFrame([rolling])
    lines.extend(["", "## Rolling Shadow Gate"])
    lines.extend(_md_table(
        roll_df,
        [
            "tracked_days", "recommendation", "total_candidate_trades", "total_candidate_net_pnl_rs",
            "total_baseline_net_pnl_rs", "total_delta_rs", "total_candidate_pf", "last5_candidate_net_pnl_rs",
            "last5_delta_rs", "last5_candidate_pf", "total_candidate_best_day_share",
            "total_candidate_without_best_day_rs",
        ],
        1,
    ))
    lines.extend(["", "## Recent Ledger"])
    recent = ledger.tail(10).sort_values("date", ascending=False)
    lines.extend(_md_table(
        recent,
        [
            "date", "candidate_module", "candidate_trades", "candidate_net_pnl_rs",
            "baseline_trades", "baseline_net_pnl_rs", "candidate_vs_baseline_delta_rs",
            "live_paper_trades", "live_paper_net_pnl_rs", "status",
        ],
        10,
    ))
    lines.extend([
        "",
        "## Guardrail",
        "- No auto-promotion is performed by this monitor.",
        "- Minimum review horizon: 5-10 completed live sessions.",
        "- Promotion requires explicit approval plus live-parity, execution, cost, and drawdown review.",
        "",
        "## Output Dirs",
        f"- Current V11 run: `{out_dirs.get('baseline', '')}`",
        f"- Candidate shadow run: `{out_dirs.get('candidate', '')}`",
        f"- Daily ledger: `{DAILY_LEDGER}`",
        "",
    ])
    return "\n".join(lines)


def run(
    day: str,
    baseline_module: str,
    candidate_module: str,
    json_dir: Path,
    skip_data_verify: bool = False,
) -> dict[str, Any]:
    _write_status("RUNNING", phase="START", day=day, candidate_module=candidate_module)
    if not skip_data_verify:
        rc, verify_text = _run_verify(day)
        _write_text_atomic(RUNS_DIR / day / "data_verify.log", verify_text)
        if rc != 0:
            _write_status("DATA_VERIFY_FAILED", phase="DATA_VERIFY", day=day, verify_exit=rc)
            raise RuntimeError(f"data verify failed for {day} exit={rc}; see {RUNS_DIR / day / 'data_verify.log'}")

    baseline_trades, baseline_metrics, baseline_out = _run_v11(day, baseline_module, "current_v11", json_dir)
    candidate_trades, candidate_metrics, candidate_out = _run_v11(day, candidate_module, "candidate_shadow", json_dir)
    live_trades, live_metrics, live_path, live_status = _load_live_paper(day)

    b_keys = set(_trade_key(baseline_trades))
    c_keys = set(_trade_key(candidate_trades))
    base = baseline_trades.copy()
    cand = candidate_trades.copy()
    base["trade_key"] = _trade_key(base)
    cand["trade_key"] = _trade_key(cand)
    added = cand[cand["trade_key"].isin(c_keys - b_keys)].copy() if not cand.empty else pd.DataFrame()
    removed = base[base["trade_key"].isin(b_keys - c_keys)].copy() if not base.empty else pd.DataFrame()

    now = _now_ist().isoformat()
    row = {
        "date": day,
        "run_ts_ist": now,
        "status": "DONE",
        "baseline_module": baseline_module,
        "candidate_module": candidate_module,
        "baseline_active_setups": _module_summary(baseline_module).get("active_count"),
        "candidate_active_setups": _module_summary(candidate_module).get("active_count"),
        "baseline_trades": baseline_metrics["trades"],
        "candidate_trades": candidate_metrics["trades"],
        "live_paper_trades": live_metrics["trades"],
        "baseline_net_pnl_rs": baseline_metrics["net_pnl_rs"],
        "candidate_net_pnl_rs": candidate_metrics["net_pnl_rs"],
        "live_paper_net_pnl_rs": live_metrics["net_pnl_rs"],
        "candidate_vs_baseline_delta_rs": round(candidate_metrics["net_pnl_rs"] - baseline_metrics["net_pnl_rs"], 2),
        "candidate_vs_live_paper_delta_rs": round(candidate_metrics["net_pnl_rs"] - live_metrics["net_pnl_rs"], 2),
        "baseline_gross_profit_rs": baseline_metrics["gross_profit_rs"],
        "baseline_gross_loss_rs": baseline_metrics["gross_loss_rs"],
        "candidate_gross_profit_rs": candidate_metrics["gross_profit_rs"],
        "candidate_gross_loss_rs": candidate_metrics["gross_loss_rs"],
        "baseline_profit_factor": baseline_metrics["profit_factor"],
        "candidate_profit_factor": candidate_metrics["profit_factor"],
        "baseline_win_rate_pct": baseline_metrics["win_rate_pct"],
        "candidate_win_rate_pct": candidate_metrics["win_rate_pct"],
        "live_paper_status": live_status,
        "live_paper_path": str(live_path or ""),
        "candidate_added_trades": int(len(added)),
        "candidate_removed_trades": int(len(removed)),
    }
    ledger = _merge_ledger(row)
    rolling = _rolling(ledger, baseline_module, candidate_module)

    latest_baseline_csv = LATEST_DIR / "latest_current_v11_trades.csv"
    latest_candidate_csv = LATEST_DIR / "latest_candidate_shadow_trades.csv"
    latest_added_csv = LATEST_DIR / "latest_candidate_added_trades.csv"
    latest_removed_csv = LATEST_DIR / "latest_candidate_removed_trades.csv"
    latest_md = LATEST_DIR / "latest_v11_lab_shadow_monitor.md"
    latest_json = LATEST_DIR / "latest_v11_lab_shadow_monitor.json"
    dated_md = REPORTS_DIR / f"v11_lab_shadow_monitor_{day}.md"
    dated_json = REPORTS_DIR / f"v11_lab_shadow_monitor_{day}.json"

    baseline_trades.to_csv(latest_baseline_csv, index=False)
    candidate_trades.to_csv(latest_candidate_csv, index=False)
    added.to_csv(latest_added_csv, index=False)
    removed.to_csv(latest_removed_csv, index=False)

    md = _build_report(
        day, baseline_module, candidate_module, baseline_metrics, candidate_metrics,
        live_metrics, live_status, live_path, baseline_trades, candidate_trades,
        added, removed, ledger, rolling, {"baseline": baseline_out, "candidate": candidate_out},
    )
    _write_text_atomic(latest_md, md)
    _write_text_atomic(dated_md, md)
    payload = {
        "status": "DONE",
        "day": day,
        "baseline_module": baseline_module,
        "candidate_module": candidate_module,
        "baseline": baseline_metrics,
        "candidate": candidate_metrics,
        "live_paper": live_metrics,
        "live_paper_status": live_status,
        "live_paper_path": str(live_path or ""),
        "rolling": rolling,
        "latest_report": str(latest_md),
        "dated_report": str(dated_md),
        "daily_ledger": str(DAILY_LEDGER),
        "module_summary": {
            "baseline": _module_summary(baseline_module),
            "candidate": _module_summary(candidate_module),
        },
    }
    _write_json_atomic(latest_json, payload)
    _write_json_atomic(dated_json, payload)
    _write_status(
        "DONE",
        phase="COMPLETE",
        day=day,
        candidate_module=candidate_module,
        report=str(latest_md),
        candidate_trades=candidate_metrics["trades"],
        candidate_net_pnl_rs=candidate_metrics["net_pnl_rs"],
        recommendation=rolling.get("recommendation", ""),
    )
    print(
        f"[{SESSION_SLUG}] DONE day={day} candidate_net={candidate_metrics['net_pnl_rs']:.2f} "
        f"baseline_net={baseline_metrics['net_pnl_rs']:.2f} report={latest_md}",
        flush=True,
    )
    return payload


def main(argv: Iterable[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--date", default="", help="Trading date YYYY-MM-DD. Defaults to today, or previous weekday on weekends.")
    ap.add_argument("--baseline-module", default=DEFAULT_BASELINE_MODULE)
    ap.add_argument("--candidate-module", default=DEFAULT_CANDIDATE_MODULE)
    ap.add_argument("--live-candidate-json-dir", default=str(DEFAULT_JSON_DIR))
    ap.add_argument("--skip-data-verify", action="store_true")
    args = ap.parse_args(list(argv) if argv is not None else None)
    day = str(args.date or _default_day())
    try:
        run(
            day=day,
            baseline_module=str(args.baseline_module),
            candidate_module=str(args.candidate_module),
            json_dir=Path(args.live_candidate_json_dir),
            skip_data_verify=bool(args.skip_data_verify),
        )
        return 0
    except Exception as exc:
        _write_status("ERROR", phase="FAILED", day=day, error=f"{type(exc).__name__}: {exc}")
        raise


if __name__ == "__main__":
    raise SystemExit(main())
