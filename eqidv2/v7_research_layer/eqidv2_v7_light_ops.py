"""
Lightweight intraday operations monitor for V7 ID 5-min live research.

This module reads only today's live JSON/CSV audit files. It intentionally avoids
multi-window replays and parquet scans so it can run every 15 minutes during the
market session.
"""

from __future__ import annotations

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

from eqidv2_runtime_paths import DATA_1MIN_DIR, LIVE_SIGNALS_DIR, RUNTIME_STATUS_DIR, runtime_dir
from nse_intraday_costs import CostConfig, intraday_equity_costs


RESEARCH_ROOT = runtime_dir("live_research_v7_research_layer")
OPS_DIR = RESEARCH_ROOT / "ops_audit"
LATEST_DIR = RESEARCH_ROOT / "latest"
HEARTBEAT_DIR = RESEARCH_ROOT / "heartbeat"

SIGNAL_ROOT = runtime_dir("signal_discovery_v7_5mins_ID")
SIGNAL_JSON_DIR = SIGNAL_ROOT / "json"
SIGNAL_LATEST_DIR = SIGNAL_ROOT / "latest"
ENTRY_ROOT = runtime_dir("entry_engine_1min_v5_ID")
ENTRY_AUDIT_DIR = ENTRY_ROOT / "audit"
ENTRY_LATEST_DIR = ENTRY_ROOT / "latest"
SLOT_READY_DIR = runtime_dir("slot_ready_5m")

LIVE_5MIN_STATUS_JSON = REPO_ROOT / "logs" / "eqidv2_eod_scheduler_for_5mins_data_live_minimal.status.json"
LIVE_5MIN_SUPERVISOR_STATUS = REPO_ROOT / "logs" / "eqidv2_eod_scheduler_for_5mins_data_live_minimal.supervisor.status"

TTP_WAIT_MIN = 8.0
TTP_MIN_PROGRESS_R = 0.25
FRESHNESS_WEAK_SCORE = 45.0
FRESHNESS_STRONG_SCORE = 65.0
ANTI_CHASE_AUDIT_MAX_ROWS = 25
PATH_LAB_MAX_ROWS = 35
CONCENTRATION_WARN_PCT = 70.0
SLOT_FLOOD_MIN_CANDIDATES = 8
SLOT_DOMINANT_SETUP_WARN_PCT = 80.0
PORTFOLIO_YELLOW_NET_RS = -75_000.0
PORTFOLIO_RED_NET_RS = -150_000.0
NSE_ID_COST_CONFIG = CostConfig()
# Scanner publish deadline used for slot slack computation
OPS_ENTRY_WINDOW_END_HHMM = __import__("os").environ.get(
    "EQIDV2_OPS_ENTRY_WINDOW_END", "14:30"
)

for _p in (RESEARCH_ROOT, OPS_DIR, LATEST_DIR, HEARTBEAT_DIR):
    _p.mkdir(parents=True, exist_ok=True)


def _write_text_retry(path: Path, text: str, *, retries: int = 6, delay_sec: float = 0.25) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    last_exc: Exception | None = None
    for attempt in range(retries):
        tmp = path.with_name(f"{path.name}.tmp.{os.getpid()}.{attempt}")
        try:
            tmp.write_text(text, encoding="utf-8")
            os.replace(tmp, path)
            return
        except PermissionError as exc:
            last_exc = exc
            try:
                if tmp.exists():
                    tmp.unlink()
            except Exception:
                pass
            if attempt < retries - 1:
                time.sleep(delay_sec * (attempt + 1))
        except Exception:
            try:
                if tmp.exists():
                    tmp.unlink()
            except Exception:
                pass
            raise
    if last_exc is not None:
        raise last_exc


def _write_csv_retry(path: Path, frame: pd.DataFrame, *, retries: int = 6, delay_sec: float = 0.25) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    last_exc: Exception | None = None
    for attempt in range(retries):
        tmp = path.with_name(f"{path.name}.tmp.{os.getpid()}.{attempt}")
        try:
            frame.to_csv(tmp, index=False)
            os.replace(tmp, path)
            return
        except PermissionError as exc:
            last_exc = exc
            try:
                if tmp.exists():
                    tmp.unlink()
            except Exception:
                pass
            if attempt < retries - 1:
                time.sleep(delay_sec * (attempt + 1))
        except Exception:
            try:
                if tmp.exists():
                    tmp.unlink()
            except Exception:
                pass
            raise
    if last_exc is not None:
        raise last_exc


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


def _fmt_num(value: Any, digits: int = 2, suffix: str = "") -> str:
    try:
        x = float(value)
    except Exception:
        return ""
    if not math.isfinite(x):
        return ""
    return f"{x:.{digits}f}{suffix}"


def _safe_float(value: Any, default: float = np.nan) -> float:
    try:
        out = float(value)
    except Exception:
        return default
    return out if np.isfinite(out) else default


def _num_series(df: pd.DataFrame, col: str, default: float = np.nan) -> pd.Series:
    if df.empty:
        return pd.Series(dtype=float)
    if col not in df.columns:
        return pd.Series(default, index=df.index, dtype=float)
    return pd.to_numeric(df[col], errors="coerce")


def _side_dir_series(df: pd.DataFrame) -> pd.Series:
    if df.empty:
        return pd.Series(dtype=float)
    return (
        df.get("side", pd.Series("", index=df.index))
        .astype(str)
        .str.upper()
        .map(lambda side: 1.0 if side == "LONG" else (-1.0 if side == "SHORT" else np.nan))
    )


def _apply_v7_nse_id_costs(df: pd.DataFrame) -> pd.DataFrame:
    """Append NSE intraday cost columns to closed V7 ID paper trades."""
    if df.empty:
        return df
    out = df.copy()
    cost_cols = [
        "v7_nse_id_gross_pnl_rs",
        "v7_nse_id_brokerage_rs",
        "v7_nse_id_stt_rs",
        "v7_nse_id_exch_txn_rs",
        "v7_nse_id_sebi_rs",
        "v7_nse_id_ipft_rs",
        "v7_nse_id_stamp_rs",
        "v7_nse_id_gst_rs",
        "v7_nse_id_total_cost_rs",
        "v7_nse_id_net_pnl_rs",
        "v7_nse_id_cost_bps_of_turnover",
        "v7_nse_id_cost_pct_of_entry",
        "v7_nse_id_cost_error",
    ]
    for col in cost_cols:
        if col not in out.columns:
            out[col] = "" if col == "v7_nse_id_cost_error" else np.nan

    outcome = out.get("outcome", pd.Series("", index=out.index)).fillna("").astype(str).str.upper()
    entry = pd.to_numeric(out.get("entry_price", pd.Series(dtype=float)), errors="coerce")
    exit_ = pd.to_numeric(out.get("exit_price", pd.Series(dtype=float)), errors="coerce")
    qty = pd.to_numeric(out.get("quantity", pd.Series(dtype=float)), errors="coerce")
    side = out.get("side", pd.Series("", index=out.index)).fillna("").astype(str).str.upper().str.strip()
    eligible = (
        ~outcome.str.startswith("ENTRY_SKIPPED")
        & entry.gt(0)
        & exit_.gt(0)
        & qty.gt(0)
        & side.isin(["LONG", "SHORT"])
    )
    for idx in out.index[eligible]:
        try:
            b = intraday_equity_costs(
                float(entry.loc[idx]),
                float(exit_.loc[idx]),
                float(qty.loc[idx]),
                str(side.loc[idx]),
                NSE_ID_COST_CONFIG,
            )
        except Exception as exc:
            out.at[idx, "v7_nse_id_cost_error"] = f"{type(exc).__name__}: {exc}"
            continue
        out.at[idx, "v7_nse_id_gross_pnl_rs"] = b.gross_pnl
        out.at[idx, "v7_nse_id_brokerage_rs"] = b.brokerage
        out.at[idx, "v7_nse_id_stt_rs"] = b.stt
        out.at[idx, "v7_nse_id_exch_txn_rs"] = b.exch_txn
        out.at[idx, "v7_nse_id_sebi_rs"] = b.sebi
        out.at[idx, "v7_nse_id_ipft_rs"] = b.ipft
        out.at[idx, "v7_nse_id_stamp_rs"] = b.stamp
        out.at[idx, "v7_nse_id_gst_rs"] = b.gst
        out.at[idx, "v7_nse_id_total_cost_rs"] = b.total_cost
        out.at[idx, "v7_nse_id_net_pnl_rs"] = b.net_pnl
        out.at[idx, "v7_nse_id_cost_bps_of_turnover"] = b.cost_bps_of_turnover
        out.at[idx, "v7_nse_id_cost_pct_of_entry"] = b.cost_pct_of_entry
    return out


def _clip01(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").clip(lower=0.0, upper=1.0)


def _parse_count_text(text: Any) -> dict[str, int]:
    out: dict[str, int] = {}
    for part in str(text or "").split(","):
        if ":" not in part:
            continue
        key, value = part.split(":", 1)
        key = key.strip()
        try:
            out[key] = int(float(value.strip()))
        except Exception:
            continue
    return out


def _profit_factor_from_series(values: pd.Series) -> float:
    pnl = pd.to_numeric(values, errors="coerce").fillna(0.0)
    gross_profit = float(pnl[pnl > 0].sum())
    gross_loss = float(-pnl[pnl < 0].sum())
    if gross_loss <= 0:
        return np.inf if gross_profit > 0 else np.nan
    return gross_profit / gross_loss


def _read_json_file(path: Path) -> dict[str, Any]:
    if not path.exists() or path.stat().st_size <= 2:
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8", errors="replace"))
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


def _read_kv_file(path: Path) -> dict[str, str]:
    if not path.exists() or path.stat().st_size <= 2:
        return {}
    out: dict[str, str] = {}
    try:
        for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
            if "=" in line:
                k, v = line.split("=", 1)
                out[k.strip()] = v.strip()
    except Exception:
        return {}
    return out


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists() or path.stat().st_size <= 2:
        return pd.DataFrame()
    try:
        return pd.read_csv(path, low_memory=False)
    except Exception:
        return pd.DataFrame()


def _read_many(paths: list[Path]) -> pd.DataFrame:
    parts = [_read_csv(p) for p in paths]
    parts = [p for p in parts if not p.empty]
    if not parts:
        return pd.DataFrame()
    return pd.concat(parts, ignore_index=True, sort=False)


def _slot_key(value: Any) -> str:
    ts = _normalise_ts(value)
    if pd.isna(ts):
        return ""
    return ts.strftime("%Y%m%d_%H%M")


def _candidate_key(ticker: Any, side: Any, setup: Any, signal_time: Any) -> str:
    return "|".join(
        [
            str(ticker or "").upper().strip(),
            str(side or "").upper().strip(),
            str(setup or "").upper().strip(),
            _fmt_ts(signal_time),
        ]
    )


def _add_key(df: pd.DataFrame, time_col: str, key_col: str = "_flow_key") -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    for col in ("ticker", "side", "setup"):
        if col not in out.columns:
            out[col] = ""
    if time_col not in out.columns:
        out[time_col] = ""
    out[key_col] = [
        _candidate_key(row.get("ticker"), row.get("side"), row.get("setup"), row.get(time_col))
        for _, row in out.iterrows()
    ]
    return out


def _file_mtime_ist(path: Path) -> pd.Timestamp:
    try:
        return pd.Timestamp.fromtimestamp(path.stat().st_mtime, tz="Asia/Kolkata")
    except Exception:
        return pd.NaT


def _latest_candidates_payload(name: str) -> dict[str, Any]:
    return _read_json_file(SIGNAL_LATEST_DIR / name)


def _payload_candidates(payload: dict[str, Any]) -> list[dict[str, Any]]:
    rows = payload.get("candidates", [])
    return rows if isinstance(rows, list) else []


def _setup_counts_text(payload: dict[str, Any], limit: int = 6) -> str:
    counts = payload.get("setup_counts", {})
    if not isinstance(counts, dict) or not counts:
        return ""
    items = sorted(counts.items(), key=lambda kv: int(_safe_float(kv[1], 0)), reverse=True)[:limit]
    return ", ".join(f"{k}:{int(_safe_float(v, 0))}" for k, v in items)


def _latest_signal_summary() -> dict[str, Any]:
    final = _latest_candidates_payload("latest_candidate_tickers.json")
    raw = _latest_candidates_payload("latest_raw_candidate_tickers.json")
    v11_rej = _latest_candidates_payload("latest_v11_overlay_rejected_candidate_tickers.json")
    research_rej = _latest_candidates_payload("latest_research_filter_rejected_candidate_tickers.json")
    slot_ist = final.get("slot_ist") or raw.get("slot_ist") or v11_rej.get("slot_ist")
    slot_ts = _normalise_ts(slot_ist)
    created_ts = _normalise_ts(final.get("created_at_ist"))
    publish_delay = (created_ts - slot_ts).total_seconds() if not pd.isna(slot_ts) and not pd.isna(created_ts) else np.nan
    return {
        "slot_ist": _fmt_ts(slot_ist),
        "created_at_ist": _fmt_ts(final.get("created_at_ist")),
        "publish_delay_sec": publish_delay,
        "raw_candidates": int(_safe_float(raw.get("total_candidates", len(_payload_candidates(raw))), 0)),
        "raw_setup_counts": _setup_counts_text(raw),
        "research_rejected": int(_safe_float(final.get("research_live_filter_rejected", research_rej.get("total_candidates", len(_payload_candidates(research_rej)))), 0)),
        "v11_input": int(_safe_float(final.get("v11_live_overlay_input_candidates", raw.get("total_candidates", 0)), 0)),
        "v11_selected": int(_safe_float(final.get("v11_live_overlay_selected", final.get("total_candidates", len(_payload_candidates(final)))), 0)),
        "v11_rejected": int(_safe_float(final.get("v11_live_overlay_rejected_total", v11_rej.get("total_candidates", len(_payload_candidates(v11_rej)))), 0)),
        "final_candidates": int(_safe_float(final.get("total_candidates", len(_payload_candidates(final))), 0)),
        "final_setup_counts": _setup_counts_text(final),
        "final_tickers": ", ".join(str(r.get("ticker", "")).upper() for r in _payload_candidates(final)[:20]),
        "tier123_enabled": bool(final.get("v11_tier123_live_enabled", False)),
        "tier123_workers": int(_safe_float(final.get("v11_tier123_live_scan_workers", 0), 0)),
        "tier123_scan_elapsed_sec": _safe_float(final.get("v11_tier123_live_scan_elapsed_sec"), np.nan),
        "tier123_tickers_scanned": int(_safe_float(final.get("v11_tier123_live_tickers_scanned", 0), 0)),
        "ab_gate_candidates": int(_safe_float(final.get("ab_gate_candidates", 0), 0)),
        "ab_gate_quality_rejected": int(_safe_float(final.get("ab_gate_quality_rejected", 0), 0)),
    }


def _latest_entry_summary() -> dict[str, Any]:
    payload = _read_json_file(ENTRY_LATEST_DIR / "latest_summary.json")
    slot_ist = payload.get("slot_ist") or payload.get("candidate_snapshot_slot_ist")
    slot_key = _slot_key(slot_ist)
    pre_rej = _read_csv(ENTRY_AUDIT_DIR / f"entry_rejected_pre_momentum_{slot_key}.csv") if slot_key else pd.DataFrame()
    v11_rej = _read_csv(ENTRY_AUDIT_DIR / f"entry_rejected_v11_overlay_{slot_key}.csv") if slot_key else pd.DataFrame()
    rows = _read_csv(ENTRY_AUDIT_DIR / f"entry_rows_{slot_key}.csv") if slot_key else pd.DataFrame()
    raw_rows = _read_csv(ENTRY_AUDIT_DIR / f"entry_rows_raw_candidates_{slot_key}.csv") if slot_key else pd.DataFrame()
    slot_ts = _normalise_ts(slot_ist)
    entry_file = ENTRY_AUDIT_DIR / f"entry_rows_{slot_key}.csv"
    entry_mtime = _file_mtime_ist(entry_file) if entry_file.exists() else pd.NaT
    write_delay = (entry_mtime - slot_ts).total_seconds() if not pd.isna(slot_ts) and not pd.isna(entry_mtime) else np.nan
    return {
        "slot_ist": _fmt_ts(slot_ist),
        "slot_key": slot_key,
        "candidate_count": int(_safe_float(payload.get("candidate_count", 0), 0)),
        "candidate_wait_sec": _safe_float(payload.get("candidate_wait_sec"), np.nan),
        "candidate_wait_used": bool(payload.get("candidate_wait_used", False)),
        "raw_fetch_elapsed_sec": _safe_float(payload.get("raw_fetch_elapsed_sec"), np.nan),
        "raw_fetch_mode": str(payload.get("raw_fetch_mode", "")),
        "raw_fetch_active_apps": int(_safe_float(payload.get("raw_fetch_active_apps", payload.get("raw_fetch_worker_apps", 0)), 0)),
        "raw_fetch_failures": int(_safe_float(payload.get("raw_fetch_failures", 0), 0)),
        "engine_elapsed_sec": _safe_float(payload.get("elapsed_sec"), np.nan),
        "tickers_requested": int(_safe_float(payload.get("tickers_requested", 0), 0)),
        "tickers_fetched": int(_safe_float(payload.get("tickers_fetched", 0), 0)),
        "raw_entry_rows": int(_safe_float(payload.get("raw_entry_rows", len(raw_rows)), 0)),
        "v11_entry_rejected_rows": int(_safe_float(payload.get("v11_entry_rejected_rows", len(v11_rej)), 0)),
        "pre_momentum_input_rows": int(_safe_float(payload.get("pre_momentum_input_rows", len(raw_rows)), 0)),
        "pre_momentum_output_rows": int(_safe_float(payload.get("pre_momentum_output_rows", len(rows)), 0)),
        "pre_momentum_rejected_rows": int(_safe_float(payload.get("pre_momentum_rejected_rows", len(pre_rej)), 0)),
        "selected_entry_rows": int(_safe_float(payload.get("selected_entry_rows", len(rows)), 0)),
        "entry_rows": int(_safe_float(payload.get("entry_rows", len(rows)), 0)),
        "intraday_duplicate_rows": int(_safe_float(payload.get("intraday_duplicate_rows", 0), 0)),
        "write_delay_sec": write_delay,
        "entry_tickers": ", ".join(rows.get("ticker", pd.Series(dtype=str)).astype(str).str.upper().head(20).tolist()) if not rows.empty else "",
    }


def _slot_flow_rows(day: str, limit: int = 8) -> pd.DataFrame:
    ymd = day.replace("-", "")
    candidate_paths = sorted(SIGNAL_JSON_DIR.glob(f"candidate_tickers_{ymd}_*.json"))[-limit:]
    rows: list[dict[str, Any]] = []
    for path in candidate_paths:
        payload = _read_json_file(path)
        slot_ist = payload.get("slot_ist", "")
        key = _slot_key(slot_ist)
        raw_payload = _read_json_file(SIGNAL_JSON_DIR / f"raw_candidate_tickers_{key}.json")
        entry_rows = _read_csv(ENTRY_AUDIT_DIR / f"entry_rows_{key}.csv")
        raw_entry = _read_csv(ENTRY_AUDIT_DIR / f"entry_rows_raw_candidates_{key}.csv")
        pre_rej = _read_csv(ENTRY_AUDIT_DIR / f"entry_rejected_pre_momentum_{key}.csv")
        v11_rej = _read_csv(ENTRY_AUDIT_DIR / f"entry_rejected_v11_overlay_{key}.csv")
        slot_ready = _read_json_file(SLOT_READY_DIR / f"slot_{key}.json")
        slot_ts = _normalise_ts(slot_ist)
        scan_created = _normalise_ts(payload.get("created_at_ist"))
        entry_mtime = _file_mtime_ist(ENTRY_AUDIT_DIR / f"entry_rows_{key}.csv")
        scan_publish_delay_sec = (scan_created - slot_ts).total_seconds() if not pd.isna(scan_created) and not pd.isna(slot_ts) else np.nan
        entry_write_delay_sec = (entry_mtime - slot_ts).total_seconds() if not pd.isna(entry_mtime) and not pd.isna(slot_ts) else np.nan
        tier123_sec = _safe_float(payload.get("v11_tier123_live_scan_elapsed_sec"), np.nan)
        rows.append(
            {
                "slot": slot_ist,
                "fetch_sec": _safe_float(slot_ready.get("duration_ms"), np.nan) / 1000.0 if slot_ready else np.nan,
                "scan_publish_delay_sec": scan_publish_delay_sec,
                "entry_write_delay_sec": entry_write_delay_sec,
                "scanner_overhead_sec": max(0.0, scan_publish_delay_sec - tier123_sec) if np.isfinite(scan_publish_delay_sec) and np.isfinite(tier123_sec) else np.nan,
                "entry_after_scan_sec": max(0.0, entry_write_delay_sec - scan_publish_delay_sec) if np.isfinite(entry_write_delay_sec) and np.isfinite(scan_publish_delay_sec) else np.nan,
                "raw_scanner_candidates": int(_safe_float(raw_payload.get("total_candidates", len(_payload_candidates(raw_payload))), 0)),
                "v11_input": int(_safe_float(payload.get("v11_live_overlay_input_candidates", 0), 0)),
                "v11_selected": int(_safe_float(payload.get("v11_live_overlay_selected", payload.get("total_candidates", 0)), 0)),
                "v11_rejected": int(_safe_float(payload.get("v11_live_overlay_rejected_total", 0), 0)),
                "entry_raw": int(len(raw_entry)),
                "entry_v11_rejected": int(len(v11_rej)),
                "pre_mom_rejected": int(len(pre_rej)),
                "entry_rows": int(len(entry_rows)),
                "tier123_sec": tier123_sec,
                # candidates that passed V11 gate but received no entry row
                "no_entry_row_count": max(
                    0,
                    int(_safe_float(payload.get("v11_live_overlay_selected", payload.get("total_candidates", 0)), 0))
                    - int(len(entry_rows)),
                ),
                # flag when total pipeline delay (fetch + scan) exceeds 60 s
                "pipeline_late": (
                    (scan_publish_delay_sec + (_safe_float(slot_ready.get("duration_ms"), np.nan) / 1000.0 if slot_ready else np.nan)) > 60.0
                    if np.isfinite(scan_publish_delay_sec)
                    else False
                ),
            }
        )
    return pd.DataFrame(rows)


def _today_entry_rows(day: str, prefix: str) -> pd.DataFrame:
    ymd = day.replace("-", "")
    paths = sorted(ENTRY_AUDIT_DIR.glob(f"{prefix}_{ymd}_*.csv"))
    return _read_many(paths)


def _payload_frame(name: str, stage: str, status: str) -> pd.DataFrame:
    payload = _latest_candidates_payload(name)
    rows = _payload_candidates(payload)
    df = pd.DataFrame(rows if isinstance(rows, list) else [])
    if df.empty:
        return pd.DataFrame()
    df = df.copy()
    df["flow_stage"] = stage
    df["flow_status"] = status
    df["flow_slot_ist"] = _fmt_ts(payload.get("slot_ist"))
    df["flow_created_at_ist"] = _fmt_ts(payload.get("created_at_ist"))
    return df


def _entry_stage_frame(slot_key: str, filename_prefix: str, stage: str, status: str) -> pd.DataFrame:
    if not slot_key:
        return pd.DataFrame()
    df = _read_csv(ENTRY_AUDIT_DIR / f"{filename_prefix}_{slot_key}.csv")
    if df.empty:
        return pd.DataFrame()
    df = df.copy()
    df["flow_stage"] = stage
    df["flow_status"] = status
    df["flow_slot_ist"] = _fmt_ts(df.get("signal_time_ist", pd.Series(dtype=str)).iloc[0]) if "signal_time_ist" in df.columns and len(df) else ""
    df["flow_created_at_ist"] = _fmt_ts(_file_mtime_ist(ENTRY_AUDIT_DIR / f"{filename_prefix}_{slot_key}.csv"))
    return df


def _first_nonblank(row: pd.Series, columns: list[str]) -> str:
    for col in columns:
        if col in row.index:
            value = row.get(col)
            if pd.notna(value) and str(value).strip():
                return str(value).strip()
    return ""


def _latest_flow_detail(day: str, entry: dict[str, Any]) -> pd.DataFrame:
    slot_key = str(entry.get("slot_key", ""))
    parts = [
        _payload_frame("latest_raw_candidate_tickers.json", "5M_SCANNER_RAW", "POTENTIAL"),
        _payload_frame("latest_research_filter_rejected_candidate_tickers.json", "5M_RESEARCH_FILTER", "FAILED"),
        _payload_frame("latest_v11_overlay_rejected_candidate_tickers.json", "5M_V11_OVERLAY", "FAILED"),
        _payload_frame("latest_candidate_tickers.json", "5M_SCANNER_FINAL", "PASSED"),
        _entry_stage_frame(slot_key, "entry_rows_raw_candidates", "1M_ENTRY_RAW", "POTENTIAL"),
        _entry_stage_frame(slot_key, "entry_rejected_v11_overlay", "1M_ENTRY_V11", "FAILED"),
        _entry_stage_frame(slot_key, "entry_rejected_pre_momentum", "1M_PRE_MOMENTUM", "FAILED"),
        _entry_stage_frame(slot_key, "entry_rows", "1M_ENTRY_FINAL", "PASSED"),
    ]
    parts = [p for p in parts if not p.empty]
    if not parts:
        return pd.DataFrame()

    merged = pd.concat(parts, ignore_index=True, sort=False)
    rows: list[dict[str, Any]] = []
    for _, row in merged.iterrows():
        rows.append(
            {
                "day": day,
                "stage": row.get("flow_stage", ""),
                "status": row.get("flow_status", ""),
                "slot_ist": row.get("flow_slot_ist", "") or _fmt_ts(row.get("scan_slot_ist") or row.get("bar_time_ist") or row.get("signal_time_ist")),
                "created_at_ist": row.get("flow_created_at_ist", "") or _fmt_ts(row.get("created_at_ist")),
                "ticker": str(row.get("ticker", "")).upper(),
                "side": row.get("side", ""),
                "setup": row.get("setup", ""),
                "signal_time_ist": _fmt_ts(row.get("signal_time_ist") or row.get("bar_time_ist") or row.get("scan_slot_ist")),
                "entry_time_ist": _fmt_ts(row.get("entry_time_ist") or row.get("v7_signal_entry_time_ist")),
                "entry_price": _safe_float(row.get("entry_price", row.get("v7_signal_entry_price")), np.nan),
                "sl_price": _safe_float(row.get("sl_price", row.get("v7_signal_stop_price")), np.nan),
                "target_price": _safe_float(row.get("target_price", row.get("v7_signal_target_price")), np.nan),
                "ranker_score": _safe_float(row.get("ranker_score", row.get("score")), np.nan),
                "quality_score": _safe_float(row.get("quality_score", row.get("score")), np.nan),
                "rs_pct": _safe_float(row.get("rs_pct"), np.nan),
                "vol_ratio": _safe_float(row.get("vol_ratio"), np.nan),
                "close_loc": _safe_float(row.get("close_loc"), np.nan),
                "vwap_dist_atr": _safe_float(row.get("vwap_dist_atr"), np.nan),
                "pre2_mom_r": _safe_float(row.get("pre2_mom_r"), np.nan),
                "pre_entry_momentum_score": _safe_float(row.get("pre_entry_momentum_score"), np.nan),
                "sig5_adx_calc": _safe_float(row.get("sig5_adx_calc"), np.nan),
                "reason": _first_nonblank(
                    row,
                    [
                        "pre_momentum_gate_reason",
                        "v11_selected_strategy_reject_reason",
                        "reject_reason",
                        "reason",
                        "v11_live_overlay_status",
                        "status",
                    ],
                ),
                "candidate_id": row.get("candidate_id", ""),
                "selection_mode": row.get("selection_mode", ""),
            }
        )
    out = pd.DataFrame(rows)
    if out.empty:
        return out
    stage_order = {
        "5M_SCANNER_RAW": 10,
        "5M_RESEARCH_FILTER": 20,
        "5M_V11_OVERLAY": 30,
        "5M_SCANNER_FINAL": 40,
        "1M_ENTRY_RAW": 50,
        "1M_ENTRY_V11": 60,
        "1M_PRE_MOMENTUM": 70,
        "1M_ENTRY_FINAL": 80,
    }
    out["_stage_order"] = out["stage"].map(stage_order).fillna(999)
    out = out.sort_values(["_stage_order", "ticker", "setup"]).drop(columns=["_stage_order"])
    return out


def _feature_gap_rows(scope: str, frame: pd.DataFrame, required: list[str]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for col in required:
        if frame.empty:
            missing_mask = pd.Series(dtype=bool)
        elif col not in frame.columns:
            missing_mask = pd.Series(True, index=frame.index)
        else:
            missing_mask = pd.to_numeric(frame[col], errors="coerce").isna()
        count = int(missing_mask.sum()) if len(missing_mask) else 0
        if count <= 0:
            continue
        sample = frame.loc[missing_mask].head(12) if not frame.empty else pd.DataFrame()
        tickers = ""
        setups = ""
        reasons = ""
        if not sample.empty:
            if "ticker" in sample.columns:
                tickers = ", ".join(sample["ticker"].fillna("").astype(str).str.upper().drop_duplicates().head(12).tolist())
            if "setup" in sample.columns:
                setups = ", ".join(sample["setup"].fillna("").astype(str).drop_duplicates().head(6).tolist())
            reason_col = "pre_momentum_gate_reason" if "pre_momentum_gate_reason" in sample.columns else "reject_reason"
            if reason_col in sample.columns:
                reasons = " | ".join(sample[reason_col].fillna("").astype(str).replace("", "blank").drop_duplicates().head(4).tolist())
        rows.append(
            {
                "scope": scope,
                "feature": col,
                "missing_rows": count,
                "total_rows": int(len(frame)),
                "sample_tickers": tickers,
                "sample_setups": setups,
                "sample_reasons": reasons,
            }
        )
    return rows


def _nan_reason_feature_counts(rejected: pd.DataFrame) -> dict[str, int]:
    if rejected.empty or "pre_momentum_gate_reason" not in rejected.columns:
        return {}
    reasons = rejected["pre_momentum_gate_reason"].fillna("").astype(str)
    extracted = reasons.str.extractall(r"([A-Za-z0-9_]+)=nan")
    if extracted.empty:
        return {}
    return {str(k): int(v) for k, v in extracted[0].value_counts().head(12).items()}


def _pre_momentum_health(day: str) -> tuple[dict[str, Any], pd.DataFrame, pd.DataFrame]:
    rows = _today_entry_rows(day, "entry_rows")
    rejected = _today_entry_rows(day, "entry_rejected_pre_momentum")
    latest = _latest_entry_summary()
    latest_key = str(latest.get("slot_key", ""))
    latest_rows = _read_csv(ENTRY_AUDIT_DIR / f"entry_rows_{latest_key}.csv") if latest_key else pd.DataFrame()
    latest_rejected = _read_csv(ENTRY_AUDIT_DIR / f"entry_rejected_pre_momentum_{latest_key}.csv") if latest_key else pd.DataFrame()
    required = ["pre_bars", "sig5_adx_calc", "sig5_vol_ratio20", "pre2_mom_r", "pre_entry_momentum_score"]
    day_missing: dict[str, int] = {}
    latest_missing: dict[str, int] = {}
    for col in required:
        if rows.empty or col not in rows.columns:
            day_missing[col] = int(len(rows))
        else:
            day_missing[col] = int(pd.to_numeric(rows[col], errors="coerce").isna().sum())
        if latest_rows.empty or col not in latest_rows.columns:
            latest_missing[col] = int(len(latest_rows))
        else:
            latest_missing[col] = int(pd.to_numeric(latest_rows[col], errors="coerce").isna().sum())
    pass_count = int(len(rows))
    reject_count = int(len(rejected))
    nan_rejects = 0
    if not rejected.empty and "pre_momentum_gate_reason" in rejected.columns:
        nan_rejects = int(rejected["pre_momentum_gate_reason"].fillna("").astype(str).str.contains("=nan", regex=False).sum())
    latest_nan_rejects = 0
    if not latest_rejected.empty and "pre_momentum_gate_reason" in latest_rejected.columns:
        latest_nan_rejects = int(latest_rejected["pre_momentum_gate_reason"].fillna("").astype(str).str.contains("=nan", regex=False).sum())
    reasons = pd.DataFrame()
    if not rejected.empty and "pre_momentum_gate_reason" in rejected.columns:
        reasons = (
            rejected["pre_momentum_gate_reason"]
            .fillna("")
            .astype(str)
            .replace("", "blank_reason")
            .value_counts()
            .head(12)
            .rename_axis("reason")
            .reset_index(name="count")
        )
    gap_rows = []
    gap_rows.extend(_feature_gap_rows("latest_accepted", latest_rows, required))
    gap_rows.extend(_feature_gap_rows("latest_rejected", latest_rejected, required))
    gap_rows.extend(_feature_gap_rows("day_accepted", rows, required))
    gap_rows.extend(_feature_gap_rows("day_rejected", rejected, required))
    gap_df = pd.DataFrame(gap_rows)
    min_pre_bars = _safe_float(pd.to_numeric(rows.get("pre_bars", pd.Series(dtype=float)), errors="coerce").min(), np.nan) if not rows.empty else np.nan
    latest_min_pre_bars = _safe_float(pd.to_numeric(latest_rows.get("pre_bars", pd.Series(dtype=float)), errors="coerce").min(), np.nan) if not latest_rows.empty else np.nan
    return (
        {
            "accepted_rows": pass_count,
            "rejected_rows": reject_count,
            "pass_rate_pct": 100.0 * pass_count / max(1, pass_count + reject_count),
            "nan_rejects": nan_rejects,
            "latest_slot": latest_key,
            "latest_accepted_rows": int(len(latest_rows)),
            "latest_rejected_rows": int(len(latest_rejected)),
            "latest_nan_rejects": latest_nan_rejects,
            "missing_required_feature_counts": day_missing,
            "latest_missing_required_feature_counts": latest_missing,
            "nan_reason_feature_counts": _nan_reason_feature_counts(rejected),
            "feature_gap_rows": int(len(gap_df)),
            "latest_feature_gap_rows": int(len(gap_df.loc[gap_df.get("scope", pd.Series(dtype=str)).astype(str).str.startswith("latest")])) if not gap_df.empty else 0,
            "min_pre_bars": min_pre_bars,
            "latest_min_pre_bars": latest_min_pre_bars,
            "data_sufficiency_ok": bool(len(latest_rows) > 0 and latest_nan_rejects == 0 and all(v == 0 for v in latest_missing.values())),
        },
        reasons,
        gap_df,
    )


def _load_live_signals(day: str) -> pd.DataFrame:
    long_df = _read_csv(LIVE_SIGNALS_DIR / f"signals_{day}_id_5min_v7_long.csv")
    short_df = _read_csv(LIVE_SIGNALS_DIR / f"signals_{day}_id_5min_v7_short.csv")
    live = _read_many([LIVE_SIGNALS_DIR / f"signals_{day}_id_5min_v7_long.csv", LIVE_SIGNALS_DIR / f"signals_{day}_id_5min_v7_short.csv"])
    if live.empty:
        return live
    if "signal_datetime" not in live.columns:
        live["signal_datetime"] = live.get("signal_entry_datetime_ist", live.get("signal_bar_time_ist", ""))
    return _add_key(live, "signal_datetime")


def _add_open_trade_shadow_fields(open_df: pd.DataFrame) -> pd.DataFrame:
    if open_df.empty:
        return open_df
    out = open_df.copy()
    side_dir = _side_dir_series(out)
    entry = _num_series(out, "entry_price")
    stop = _num_series(out, "stop_price")
    target = _num_series(out, "target_price")
    last = _num_series(out, "last_ltp")
    qty = _num_series(out, "quantity", 0.0).fillna(0.0)
    risk_per_share = (entry - stop).abs()
    target_r = (target - entry).abs() / risk_per_share.replace(0, np.nan)
    out["risk_per_share"] = risk_per_share
    out["paper_risk_rs"] = risk_per_share * qty
    out["open_unrealized_pnl_rs"] = side_dir * (last - entry) * qty
    out["open_unrealized_pnl_pct"] = side_dir * (last - entry) / entry.replace(0, np.nan) * 100.0
    out["target_r"] = target_r
    out["ttp_wait_min"] = TTP_WAIT_MIN
    out["ttp_min_progress_r"] = TTP_MIN_PROGRESS_R
    out["ttp_shadow_trigger"] = (
        (_num_series(out, "age_min").fillna(0.0) >= TTP_WAIT_MIN)
        & (_num_series(out, "progress_r").fillna(-999.0) < TTP_MIN_PROGRESS_R)
    )
    out["ttp_shadow_action"] = "HOLD"
    out.loc[out["ttp_shadow_trigger"] & (_num_series(out, "progress_r").fillna(0.0) < 0.0), "ttp_shadow_action"] = "EXIT_OR_TIGHTEN_SHADOW"
    out.loc[out["ttp_shadow_trigger"] & (_num_series(out, "progress_r").fillna(0.0) >= 0.0), "ttp_shadow_action"] = "TIGHTEN_TO_BREAKEVEN_SHADOW"
    out["ttp_shadow_reason"] = ""
    triggered = out["ttp_shadow_trigger"].fillna(False)
    out.loc[triggered, "ttp_shadow_reason"] = (
        "age>="
        + str(TTP_WAIT_MIN)
        + "m and progress_r<"
        + str(TTP_MIN_PROGRESS_R)
    )
    return out


def _add_momentum_freshness_fields(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    side_dir = _side_dir_series(out).fillna(1.0)
    pre2 = side_dir * _num_series(out, "pre2_mom_r").fillna(0.0)
    pre1_body = side_dir * _num_series(out, "pre1_body_r").fillna(0.0)
    pre3_close = _num_series(out, "pre3_close_pos").fillna(0.5)
    sig5_close = _num_series(out, "sig5_close_pos").fillna(_num_series(out, "close_loc").fillna(0.5))
    longish = out.get("side", pd.Series("", index=out.index)).astype(str).str.upper().ne("SHORT")
    pre3_close_side = pre3_close.where(longish, 1.0 - pre3_close)
    sig5_close_side = sig5_close.where(longish, 1.0 - sig5_close)
    vol = _num_series(out, "sig5_vol_ratio20").fillna(_num_series(out, "vol_ratio").fillna(1.0)).clip(lower=0.0)

    pre2_score = _clip01((pre2 + 0.25) / 0.75)
    pre1_body_score = _clip01((pre1_body + 0.15) / 0.55)
    pre3_close_score = _clip01(pre3_close_side)
    sig5_close_score = _clip01(sig5_close_side)
    vol_score = _clip01(np.log1p(vol) / np.log1p(5.0))

    out["freshness_pre2_score"] = pre2_score
    out["freshness_pre1_body_score"] = pre1_body_score
    out["freshness_pre3_close_score"] = pre3_close_score
    out["freshness_sig5_close_score"] = sig5_close_score
    out["freshness_vol_score"] = vol_score
    out["freshness_score"] = (
        100.0
        * (
            0.30 * pre2_score
            + 0.20 * pre1_body_score
            + 0.20 * sig5_close_score
            + 0.15 * pre3_close_score
            + 0.15 * vol_score
        )
    )
    out["freshness_bucket"] = "MID"
    out.loc[out["freshness_score"] < FRESHNESS_WEAK_SCORE, "freshness_bucket"] = "WEAK"
    out.loc[out["freshness_score"] >= FRESHNESS_STRONG_SCORE, "freshness_bucket"] = "STRONG"
    out["freshness_shadow_action"] = "KEEP_SHADOW"
    out.loc[out["freshness_bucket"].eq("WEAK"), "freshness_shadow_action"] = "DELAY_OR_RECONFIRM_SHADOW"
    return out


def _read_1min_bars(ticker: str, cache: dict[str, pd.DataFrame]) -> pd.DataFrame:
    symbol = str(ticker or "").upper().strip()
    if not symbol:
        return pd.DataFrame()
    if symbol in cache:
        return cache[symbol]
    path = DATA_1MIN_DIR / f"{symbol}_stocks_indicators_1min.parquet"
    if not path.exists():
        cache[symbol] = pd.DataFrame()
        return cache[symbol]
    try:
        df = pd.read_parquet(path, columns=["date", "high", "low", "close"])
    except Exception:
        cache[symbol] = pd.DataFrame()
        return cache[symbol]
    if df.empty:
        cache[symbol] = pd.DataFrame()
        return cache[symbol]
    df = df.copy()
    df["date"] = df["date"].map(_normalise_ts)
    for col in ("high", "low", "close"):
        df[col] = pd.to_numeric(df[col], errors="coerce")
    cache[symbol] = df.dropna(subset=["date"]).sort_values("date")
    return cache[symbol]


def _resolve_shadow_trade(row: pd.Series, bars: pd.DataFrame) -> dict[str, Any]:
    entry_ts = _normalise_ts(row.get("entry_time") or row.get("signal_datetime"))
    side = str(row.get("side", "")).upper().strip()
    entry = _safe_float(row.get("entry_price"))
    stop = _safe_float(row.get("stop_price"))
    target = _safe_float(row.get("target_price"))
    qty = _safe_float(row.get("quantity"), 0.0)
    if pd.isna(entry_ts) or bars.empty or side not in {"LONG", "SHORT"} or entry <= 0 or stop <= 0 or target <= 0:
        return {"anti_chase_shadow_outcome": "NO_DATA", "anti_chase_shadow_pnl_rs": 0.0}
    start_ts = entry_ts.ceil("min")
    future = bars.loc[bars["date"] >= start_ts].copy()
    if future.empty:
        return {"anti_chase_shadow_outcome": "NO_FUTURE_BARS", "anti_chase_shadow_pnl_rs": 0.0}
    risk = abs(entry - stop)
    if risk <= 0:
        return {"anti_chase_shadow_outcome": "NO_RISK", "anti_chase_shadow_pnl_rs": 0.0}

    side_dir = 1.0 if side == "LONG" else -1.0
    best_r = np.nan
    worst_r = np.nan
    exit_ts = pd.NaT
    exit_price = np.nan
    outcome = "OPEN"
    if side == "LONG":
        best_r = _safe_float(((future["high"] - entry) / risk).max(), np.nan)
        worst_r = _safe_float(((future["low"] - entry) / risk).min(), np.nan)
    else:
        best_r = _safe_float(((entry - future["low"]) / risk).max(), np.nan)
        worst_r = _safe_float(((entry - future["high"]) / risk).min(), np.nan)

    for _, bar in future.iterrows():
        high = _safe_float(bar.get("high"))
        low = _safe_float(bar.get("low"))
        hit_target = high >= target if side == "LONG" else low <= target
        hit_sl = low <= stop if side == "LONG" else high >= stop
        if hit_target and hit_sl:
            outcome = "AMBIGUOUS_BOTH"
            exit_price = stop
            exit_ts = bar.get("date")
            break
        if hit_sl:
            outcome = "SL"
            exit_price = stop
            exit_ts = bar.get("date")
            break
        if hit_target:
            outcome = "TARGET"
            exit_price = target
            exit_ts = bar.get("date")
            break

    last_close = _safe_float(future.iloc[-1].get("close"), np.nan)
    if outcome == "OPEN":
        exit_price = last_close
        exit_ts = future.iloc[-1].get("date")
    pnl_rs = side_dir * (exit_price - entry) * qty if np.isfinite(exit_price) else 0.0
    minutes_to_shadow_exit = (
        (_normalise_ts(exit_ts) - entry_ts).total_seconds() / 60.0
        if not pd.isna(_normalise_ts(exit_ts))
        else np.nan
    )
    return {
        "anti_chase_shadow_outcome": outcome,
        "anti_chase_shadow_exit_time": _fmt_ts(exit_ts),
        "anti_chase_shadow_exit_price": exit_price,
        "anti_chase_shadow_pnl_rs": pnl_rs,
        "anti_chase_shadow_minutes": minutes_to_shadow_exit,
        "anti_chase_shadow_best_r": best_r,
        "anti_chase_shadow_worst_r": worst_r,
        "anti_chase_shadow_start": _fmt_ts(start_ts),
    }


def _anti_chase_shadow_audit(paper: pd.DataFrame, cache: dict[str, pd.DataFrame] | None = None) -> pd.DataFrame:
    if paper.empty:
        return pd.DataFrame()
    outcome = paper.get("outcome", pd.Series(dtype=str)).fillna("").astype(str)
    skipped = paper.loc[outcome.str.contains("ANTI_CHASE", regex=False)].copy()
    if skipped.empty:
        return pd.DataFrame()
    skipped = skipped.tail(ANTI_CHASE_AUDIT_MAX_ROWS).copy()
    if cache is None:
        cache = {}
    rows: list[dict[str, Any]] = []
    for _, row in skipped.iterrows():
        bars = _read_1min_bars(str(row.get("ticker", "")), cache)
        resolved = _resolve_shadow_trade(row, bars)
        base = {
            "trade_id": row.get("trade_id", ""),
            "signal_id": row.get("signal_id", ""),
            "ticker": row.get("ticker", ""),
            "side": row.get("side", ""),
            "setup": row.get("setup", ""),
            "entry_time": row.get("entry_time", ""),
            "entry_price": row.get("entry_price", ""),
            "stop_price": row.get("stop_price", ""),
            "target_price": row.get("target_price", ""),
            "quantity": row.get("quantity", ""),
            "skip_outcome": row.get("outcome", ""),
            "freshness_score": row.get("freshness_score", np.nan),
            "freshness_bucket": row.get("freshness_bucket", ""),
        }
        rows.append({**base, **resolved})
    return pd.DataFrame(rows)


def _setup_concentration_shadow(open_df: pd.DataFrame) -> tuple[dict[str, Any], pd.DataFrame]:
    if open_df.empty:
        return (
            {
                "open_dominant_setup": "",
                "open_dominant_setup_count": 0,
                "open_dominant_setup_pct": 0.0,
                "open_dominant_setup_unrealized_rs": 0.0,
                "open_concentration_state": "NO_OPEN_TRADES",
            },
            pd.DataFrame(),
        )
    work = open_df.copy()
    work["setup"] = work.get("setup", pd.Series("", index=work.index)).fillna("").astype(str).replace("", "UNKNOWN")
    work["open_unrealized_pnl_rs"] = pd.to_numeric(work.get("open_unrealized_pnl_rs", 0.0), errors="coerce").fillna(0.0)
    work["progress_r"] = pd.to_numeric(work.get("progress_r", np.nan), errors="coerce")
    work["freshness_score"] = pd.to_numeric(work.get("freshness_score", np.nan), errors="coerce")
    grouped = (
        work.groupby("setup", dropna=False)
        .agg(
            open_trades=("setup", "count"),
            unrealized_pnl_rs=("open_unrealized_pnl_rs", "sum"),
            avg_progress_r=("progress_r", "mean"),
            weak_open=("freshness_bucket", lambda s: int(s.astype(str).eq("WEAK").sum())),
            ttp_shadow=("ttp_shadow_trigger", lambda s: int(pd.Series(s).fillna(False).sum())),
        )
        .reset_index()
        .sort_values(["open_trades", "unrealized_pnl_rs"], ascending=[False, True])
    )
    total = int(len(work))
    grouped["open_pct"] = grouped["open_trades"] / max(1, total) * 100.0
    top = grouped.iloc[0]
    state = "CONCENTRATED_SHADOW" if float(top.get("open_pct", 0.0)) >= CONCENTRATION_WARN_PCT else "DIVERSIFIED_SHADOW"
    return (
        {
            "open_dominant_setup": str(top.get("setup", "")),
            "open_dominant_setup_count": int(top.get("open_trades", 0)),
            "open_dominant_setup_pct": float(top.get("open_pct", 0.0)),
            "open_dominant_setup_unrealized_rs": float(top.get("unrealized_pnl_rs", 0.0)),
            "open_concentration_state": state,
            "open_setup_count": int(len(grouped)),
        },
        grouped,
    )


def _merge_open_trade_signal_context(
    open_df: pd.DataFrame,
    live: pd.DataFrame,
) -> pd.DataFrame:
    """Add live-signal context without losing fields already in open state."""
    if (
        open_df.empty
        or live.empty
        or "signal_id" not in open_df.columns
        or "signal_id" not in live.columns
    ):
        return open_df
    context_cols = [
        col
        for col in ("signal_id", "signal_datetime", "setup", "_flow_key")
        if col in live.columns
    ]
    merged = open_df.merge(
        live[context_cols].drop_duplicates("signal_id"),
        on="signal_id",
        how="left",
        suffixes=("", "_live"),
    )
    for col in context_cols:
        if col == "signal_id":
            continue
        live_col = f"{col}_live"
        if live_col not in merged.columns:
            continue
        if col not in merged.columns:
            merged = merged.rename(columns={live_col: col})
            continue
        current = merged[col]
        missing = current.isna() | current.astype(str).str.strip().eq("")
        merged.loc[missing, col] = merged.loc[missing, live_col]
        merged = merged.drop(columns=[live_col])
    return merged


def _slot_pressure_shadow(signal: dict[str, Any], entry: dict[str, Any]) -> dict[str, Any]:
    counts = _parse_count_text(signal.get("final_setup_counts", ""))
    total = int(_safe_float(signal.get("final_candidates", 0), 0))
    dominant_setup = ""
    dominant_count = 0
    if counts:
        dominant_setup, dominant_count = max(counts.items(), key=lambda kv: kv[1])
    dominant_pct = (dominant_count / max(1, total) * 100.0) if total else 0.0
    duplicates = int(_safe_float(entry.get("intraday_duplicate_rows", 0), 0))
    raw_entry = int(_safe_float(entry.get("raw_entry_rows", 0), 0))
    duplicate_pct = duplicates / max(1, raw_entry) * 100.0
    state = "NORMAL_SHADOW"
    if total >= SLOT_FLOOD_MIN_CANDIDATES and dominant_pct >= SLOT_DOMINANT_SETUP_WARN_PCT:
        state = "SETUP_FLOOD_SHADOW"
    if duplicate_pct >= 50.0:
        state = "DUPLICATE_PRESSURE_SHADOW" if state == "NORMAL_SHADOW" else f"{state}+DUPLICATE_PRESSURE"
    return {
        "slot_pressure_state": state,
        "slot_final_candidates": total,
        "slot_dominant_setup": dominant_setup,
        "slot_dominant_setup_count": int(dominant_count),
        "slot_dominant_setup_pct": float(dominant_pct),
        "slot_duplicate_rows": duplicates,
        "slot_duplicate_pct": float(duplicate_pct),
    }


def _portfolio_shadow_state(paper: pd.DataFrame, open_df: pd.DataFrame) -> dict[str, Any]:
    if paper.empty:
        traded = pd.DataFrame()
    else:
        outcome = paper.get("outcome", pd.Series(dtype=str)).fillna("").astype(str)
        traded = paper.loc[~outcome.str.startswith("ENTRY_SKIPPED")].copy()
    pnl_source = "pnl_rs"
    if not traded.empty and "v7_nse_id_net_pnl_rs" in traded.columns:
        net_series = pd.to_numeric(traded["v7_nse_id_net_pnl_rs"], errors="coerce")
        if net_series.notna().any():
            pnl_source = "v7_nse_id_net_pnl_rs"
            closed_pnl = net_series.fillna(pd.to_numeric(traded.get("pnl_rs", pd.Series(dtype=float)), errors="coerce")).fillna(0.0)
        else:
            closed_pnl = pd.to_numeric(traded.get("pnl_rs", pd.Series(dtype=float)), errors="coerce").fillna(0.0)
    else:
        closed_pnl = pd.to_numeric(traded.get("pnl_rs", pd.Series(dtype=float)), errors="coerce").fillna(0.0)
    open_pnl = pd.to_numeric(open_df.get("open_unrealized_pnl_rs", pd.Series(dtype=float)), errors="coerce").fillna(0.0) if not open_df.empty else pd.Series(dtype=float)
    combined = float(closed_pnl.sum() + open_pnl.sum())
    targets = int((traded.get("outcome", pd.Series(dtype=str)) == "TARGET").sum()) if not traded.empty else 0
    sl = int((traded.get("outcome", pd.Series(dtype=str)) == "SL").sum()) if not traded.empty else 0
    state = "OK_SHADOW"
    if combined <= PORTFOLIO_RED_NET_RS and sl > targets:
        state = "RED_PAUSE_SHADOW"
    elif combined <= PORTFOLIO_YELLOW_NET_RS or sl > targets * 1.5:
        state = "YELLOW_THROTTLE_SHADOW"
    return {
        "closed_pnl_rs": float(closed_pnl.sum()),
        "closed_pnl_source": pnl_source,
        "open_unrealized_pnl_rs": float(open_pnl.sum()),
        "combined_paper_pnl_rs": combined,
        "closed_profit_factor": _profit_factor_from_series(closed_pnl) if not closed_pnl.empty else np.nan,
        "closed_targets": targets,
        "closed_sl": sl,
        "portfolio_shadow_state": state,
        "portfolio_shadow_rule": f"yellow<={PORTFOLIO_YELLOW_NET_RS:.0f}, red<={PORTFOLIO_RED_NET_RS:.0f} with SL pressure",
    }


def _trade_path_metrics(row: pd.Series, bars: pd.DataFrame, source: str, end_time: Any = "") -> dict[str, Any]:
    entry_ts = _normalise_ts(row.get("entry_time") or row.get("signal_datetime"))
    side = str(row.get("side", "")).upper().strip()
    entry = _safe_float(row.get("entry_price"))
    stop = _safe_float(row.get("stop_price"))
    target = _safe_float(row.get("target_price"))
    qty = _safe_float(row.get("quantity"), 0.0)
    if pd.isna(entry_ts) or bars.empty or side not in {"LONG", "SHORT"} or entry <= 0 or stop <= 0 or target <= 0:
        return {"path_source": source, "path_status": "NO_DATA"}
    start_ts = entry_ts.ceil("min")
    end_ts = _normalise_ts(end_time) if str(end_time or "").strip() else pd.NaT
    future = bars.loc[bars["date"] >= start_ts].copy()
    if not pd.isna(end_ts):
        future = future.loc[future["date"] <= end_ts]
    if future.empty:
        return {"path_source": source, "path_status": "NO_FUTURE_BARS"}

    side_dir = 1.0 if side == "LONG" else -1.0
    risk = abs(entry - stop)
    if risk <= 0:
        return {"path_source": source, "path_status": "NO_RISK"}
    if side == "LONG":
        fav_r = (future["high"] - entry) / risk
        adv_r = (future["low"] - entry) / risk
    else:
        fav_r = (entry - future["low"]) / risk
        adv_r = (entry - future["high"]) / risk
    best_r = _safe_float(fav_r.max(), np.nan)
    worst_r = _safe_float(adv_r.min(), np.nan)

    def _first_time(mask: pd.Series) -> float:
        hits = future.loc[mask.fillna(False), "date"]
        if hits.empty:
            return np.nan
        return (_normalise_ts(hits.iloc[0]) - entry_ts).total_seconds() / 60.0

    time_025 = _first_time(fav_r >= 0.25)
    time_050 = _first_time(fav_r >= 0.50)
    time_neg050 = _first_time(adv_r <= -0.50)
    if source == "OPEN_PAPER":
        final_price = _safe_float(row.get("last_ltp"), _safe_float(future.iloc[-1].get("close"), np.nan))
        final_time = row.get("last_ltp_time") or future.iloc[-1].get("date")
        outcome = "OPEN"
    else:
        final_price = _safe_float(row.get("exit_price"), _safe_float(future.iloc[-1].get("close"), np.nan))
        final_time = row.get("exit_time") or future.iloc[-1].get("date")
        outcome = str(row.get("outcome", ""))
    final_r = side_dir * (final_price - entry) / risk if np.isfinite(final_price) else np.nan
    giveback_r = best_r - final_r if np.isfinite(best_r) and np.isfinite(final_r) else np.nan
    hold_min = (_normalise_ts(final_time) - entry_ts).total_seconds() / 60.0 if not pd.isna(_normalise_ts(final_time)) else np.nan
    action = "HOLD_SHADOW"
    if np.isfinite(best_r) and best_r >= 0.50 and np.isfinite(final_r) and final_r <= 0.10:
        action = "TRAIL_AFTER_0_5R_SHADOW"
    elif (not np.isfinite(time_025)) and np.isfinite(hold_min) and hold_min >= TTP_WAIT_MIN:
        action = "TIME_TO_PROGRESS_SHADOW"
    elif np.isfinite(time_neg050) and time_neg050 <= 8.0:
        action = "EARLY_ADVERSE_MOVE_SHADOW"
    pnl_rs = side_dir * (final_price - entry) * qty if np.isfinite(final_price) else np.nan
    return {
        "path_source": source,
        "path_status": "OK",
        "ticker": row.get("ticker", ""),
        "side": side,
        "setup": row.get("setup", ""),
        "trade_id": row.get("trade_id", ""),
        "signal_id": row.get("signal_id", ""),
        "outcome": outcome,
        "entry_time": row.get("entry_time", ""),
        "final_time": _fmt_ts(final_time),
        "hold_min": hold_min,
        "entry_price": entry,
        "final_price": final_price,
        "pnl_rs_path": pnl_rs,
        "final_r": final_r,
        "best_r": best_r,
        "worst_r": worst_r,
        "giveback_r": giveback_r,
        "time_to_025r_min": time_025,
        "time_to_050r_min": time_050,
        "time_to_neg050r_min": time_neg050,
        "path_shadow_action": action,
        "freshness_score": row.get("freshness_score", np.nan),
        "freshness_bucket": row.get("freshness_bucket", ""),
    }


def _path_quality_lab(paper: pd.DataFrame, open_df: pd.DataFrame, cache: dict[str, pd.DataFrame] | None = None) -> tuple[dict[str, Any], pd.DataFrame]:
    rows: list[dict[str, Any]] = []
    if cache is None:
        cache = {}
    if not paper.empty:
        outcome = paper.get("outcome", pd.Series(dtype=str)).fillna("").astype(str)
        traded = paper.loc[~outcome.str.startswith("ENTRY_SKIPPED")].copy()
        if not traded.empty:
            traded = traded.tail(max(20, PATH_LAB_MAX_ROWS // 2)).copy()
            for _, row in traded.iterrows():
                bars = _read_1min_bars(str(row.get("ticker", "")), cache)
                rows.append(_trade_path_metrics(row, bars, "ACTUAL_PAPER", row.get("exit_time", "")))
    if not open_df.empty:
        open_sample = open_df.copy()
        ttp_priority = (
            pd.to_numeric(open_sample["ttp_shadow_trigger"], errors="coerce").fillna(0.0)
            if "ttp_shadow_trigger" in open_sample.columns
            else pd.Series(0.0, index=open_sample.index)
        )
        open_sample["_path_priority"] = (
            ttp_priority * 1000.0
            - pd.to_numeric(open_sample.get("progress_r", 0.0), errors="coerce").fillna(0.0)
        )
        open_sample = open_sample.sort_values("_path_priority", ascending=False).head(max(20, PATH_LAB_MAX_ROWS - len(rows)))
        for _, row in open_sample.iterrows():
            bars = _read_1min_bars(str(row.get("ticker", "")), cache)
            rows.append(_trade_path_metrics(row, bars, "OPEN_PAPER", row.get("last_ltp_time", "")))
    lab = pd.DataFrame(rows)
    if lab.empty:
        return {"path_lab_rows": 0}, lab
    ok = lab.loc[lab.get("path_status", pd.Series(dtype=str)).eq("OK")].copy()
    if ok.empty:
        return {"path_lab_rows": int(len(lab)), "path_lab_ok_rows": 0}, lab
    action = ok.get("path_shadow_action", pd.Series(dtype=str)).astype(str)
    summary = {
        "path_lab_rows": int(len(lab)),
        "path_lab_ok_rows": int(len(ok)),
        "path_avg_best_r": _safe_float(pd.to_numeric(ok.get("best_r", pd.Series(dtype=float)), errors="coerce").mean(), np.nan),
        "path_avg_worst_r": _safe_float(pd.to_numeric(ok.get("worst_r", pd.Series(dtype=float)), errors="coerce").mean(), np.nan),
        "path_avg_giveback_r": _safe_float(pd.to_numeric(ok.get("giveback_r", pd.Series(dtype=float)), errors="coerce").mean(), np.nan),
        "path_time_to_progress_count": int(action.eq("TIME_TO_PROGRESS_SHADOW").sum()),
        "path_trail_after_05r_count": int(action.eq("TRAIL_AFTER_0_5R_SHADOW").sum()),
        "path_early_adverse_count": int(action.eq("EARLY_ADVERSE_MOVE_SHADOW").sum()),
        "path_reached_025r_count": int(pd.to_numeric(ok.get("time_to_025r_min", pd.Series(dtype=float)), errors="coerce").notna().sum()),
        "path_reached_050r_count": int(pd.to_numeric(ok.get("time_to_050r_min", pd.Series(dtype=float)), errors="coerce").notna().sum()),
    }
    return summary, lab


def _paper_trade_analysis(day: str, entry_rows: pd.DataFrame, cache: dict[str, pd.DataFrame] | None = None) -> tuple[dict[str, Any], pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    paper = _read_csv(LIVE_SIGNALS_DIR / f"paper_trades_{day}_id_5min_v7.csv")
    live = _load_live_signals(day)
    if not paper.empty:
        paper = _add_key(paper, "signal_datetime")
        paper["entry_ts"] = paper.get("entry_time", "").map(_normalise_ts)
        paper["exit_ts"] = paper.get("exit_time", "").map(_normalise_ts)
        paper["hold_min"] = (paper["exit_ts"] - paper["entry_ts"]).dt.total_seconds() / 60.0
    if not entry_rows.empty:
        entry_rows = _add_key(entry_rows, "signal_time_ist")
        keep_cols = [
            "_flow_key",
            "pre2_mom_r",
            "pre1_body_r",
            "pre1_mom_r",
            "pre3_close_pos",
            "pre3_mom_r",
            "pre5_mom_r",
            "pre10_mom_r",
            "pre_entry_momentum_score",
            "sig5_close_pos",
            "sig5_adx_calc",
            "sig5_vol_ratio20",
            "close_loc",
            "vwap_dist_atr",
            "ranker_score",
            "quality_score",
        ]
        keep_cols = [c for c in keep_cols if c in entry_rows.columns]
        if not paper.empty and keep_cols:
            paper = paper.merge(entry_rows[keep_cols].drop_duplicates("_flow_key"), on="_flow_key", how="left", suffixes=("", "_entry"))
            paper = _add_momentum_freshness_fields(paper)
    if not paper.empty:
        paper = _apply_v7_nse_id_costs(paper)

    skipped_mask = paper.get("outcome", pd.Series(dtype=str)).fillna("").astype(str).str.startswith("ENTRY_SKIPPED") if not paper.empty else pd.Series(dtype=bool)
    traded = paper.loc[~skipped_mask].copy() if not paper.empty else pd.DataFrame()
    closed_hits = traded.loc[traded.get("outcome", pd.Series(dtype=str)).isin(["TARGET", "SL"])].copy() if not traded.empty else pd.DataFrame()
    quick_target = closed_hits.loc[(closed_hits["outcome"] == "TARGET") & (pd.to_numeric(closed_hits.get("hold_min"), errors="coerce") <= 10)].copy() if not closed_hits.empty else pd.DataFrame()
    quick_sl = closed_hits.loc[(closed_hits["outcome"] == "SL") & (pd.to_numeric(closed_hits.get("hold_min"), errors="coerce") <= 15)].copy() if not closed_hits.empty else pd.DataFrame()

    open_payload = _read_json_file(LIVE_SIGNALS_DIR / f"open_trades_state_{day}_id_5min_v7.json")
    open_rows = open_payload.get("open_trades", [])
    open_df = pd.DataFrame(open_rows if isinstance(open_rows, list) else [])
    if not open_df.empty:
        open_df = _merge_open_trade_signal_context(open_df, live)
        open_df["entry_ts"] = open_df.get("entry_time", "").map(_normalise_ts)
        open_df["age_min"] = (_now_ist() - open_df["entry_ts"]).dt.total_seconds() / 60.0
        for col in ("entry_price", "stop_price", "target_price", "last_ltp"):
            open_df[col] = pd.to_numeric(open_df.get(col), errors="coerce")
        side_dir = open_df.get("side", pd.Series("", index=open_df.index)).astype(str).str.upper().map(lambda x: 1.0 if x == "LONG" else -1.0)
        risk = (open_df["entry_price"] - open_df["stop_price"]).abs()
        open_df["progress_r"] = side_dir * (open_df["last_ltp"] - open_df["entry_price"]) / risk.replace(0, np.nan)
        open_df.loc[open_df["last_ltp"].fillna(0) <= 0, "progress_r"] = np.nan
        open_df["slow_mover"] = (open_df["age_min"] >= 10) & (open_df["progress_r"].fillna(0) < 0.25)
        if not entry_rows.empty and "_flow_key" in open_df.columns:
            keep_cols = [
                "_flow_key",
                "pre2_mom_r",
                "pre1_body_r",
                "pre1_mom_r",
                "pre3_close_pos",
                "pre3_mom_r",
                "pre5_mom_r",
                "pre_entry_momentum_score",
                "sig5_close_pos",
                "sig5_adx_calc",
                "sig5_vol_ratio20",
                "close_loc",
                "vwap_dist_atr",
                "ranker_score",
            ]
            keep_cols = [c for c in keep_cols if c in entry_rows.columns]
            if keep_cols:
                open_df = open_df.merge(entry_rows[keep_cols].drop_duplicates("_flow_key"), on="_flow_key", how="left", suffixes=("", "_entry"))
                open_df = _add_momentum_freshness_fields(open_df)
        open_df = _add_open_trade_shadow_fields(open_df)

    skip_reasons = paper.get("outcome", pd.Series(dtype=str)).fillna("").astype(str)
    anti_chase_skips = int(skip_reasons.str.contains("ANTI_CHASE", regex=False).sum()) if not paper.empty else 0
    anti_chase_audit = _anti_chase_shadow_audit(paper, cache=cache)
    anti_shadow_pnl = pd.to_numeric(anti_chase_audit.get("anti_chase_shadow_pnl_rs", pd.Series(dtype=float)), errors="coerce").fillna(0.0) if not anti_chase_audit.empty else pd.Series(dtype=float)
    anti_shadow_outcome = anti_chase_audit.get("anti_chase_shadow_outcome", pd.Series(dtype=str)).fillna("").astype(str) if not anti_chase_audit.empty else pd.Series(dtype=str)
    freshness = _add_momentum_freshness_fields(traded) if not traded.empty else pd.DataFrame()
    weak_freshness = freshness.loc[freshness.get("freshness_bucket", pd.Series(dtype=str)).eq("WEAK")].copy() if not freshness.empty else pd.DataFrame()
    strong_freshness = freshness.loc[freshness.get("freshness_bucket", pd.Series(dtype=str)).eq("STRONG")].copy() if not freshness.empty else pd.DataFrame()
    # --- NSE ID cost model ---
    _trade_count = int(len(traded))
    _paper_gross_pnl = _safe_float(
        pd.to_numeric(traded.get("pnl_rs", pd.Series(dtype=float)), errors="coerce").sum()
        if not traded.empty else 0.0,
        0.0,
    )
    _nse_costed = (
        traded.loc[pd.to_numeric(traded.get("v7_nse_id_total_cost_rs", pd.Series(dtype=float)), errors="coerce").notna()].copy()
        if not traded.empty else pd.DataFrame()
    )
    _nse_costed_count = int(len(_nse_costed))
    _nse_gross_pnl = _safe_float(
        pd.to_numeric(_nse_costed.get("v7_nse_id_gross_pnl_rs", pd.Series(dtype=float)), errors="coerce").sum()
        if not _nse_costed.empty else _paper_gross_pnl,
        0.0,
    )
    _nse_total_cost = _safe_float(
        pd.to_numeric(_nse_costed.get("v7_nse_id_total_cost_rs", pd.Series(dtype=float)), errors="coerce").sum()
        if not _nse_costed.empty else 0.0,
        0.0,
    )
    _nse_net_pnl = _safe_float(
        pd.to_numeric(_nse_costed.get("v7_nse_id_net_pnl_rs", pd.Series(dtype=float)), errors="coerce").sum()
        if not _nse_costed.empty else _paper_gross_pnl,
        0.0,
    )
    _nse_avg_cost_bps = _safe_float(
        pd.to_numeric(_nse_costed.get("v7_nse_id_cost_bps_of_turnover", pd.Series(dtype=float)), errors="coerce").mean()
        if not _nse_costed.empty else np.nan,
        np.nan,
    )
    _nse_avg_cost_pct_entry = _safe_float(
        pd.to_numeric(_nse_costed.get("v7_nse_id_cost_pct_of_entry", pd.Series(dtype=float)), errors="coerce").mean()
        if not _nse_costed.empty else np.nan,
        np.nan,
    )
    _nse_component_sums = {
        "v7_nse_id_brokerage_rs": _safe_float(pd.to_numeric(_nse_costed.get("v7_nse_id_brokerage_rs", pd.Series(dtype=float)), errors="coerce").sum(), 0.0) if not _nse_costed.empty else 0.0,
        "v7_nse_id_stt_rs": _safe_float(pd.to_numeric(_nse_costed.get("v7_nse_id_stt_rs", pd.Series(dtype=float)), errors="coerce").sum(), 0.0) if not _nse_costed.empty else 0.0,
        "v7_nse_id_exch_txn_rs": _safe_float(pd.to_numeric(_nse_costed.get("v7_nse_id_exch_txn_rs", pd.Series(dtype=float)), errors="coerce").sum(), 0.0) if not _nse_costed.empty else 0.0,
        "v7_nse_id_sebi_rs": _safe_float(pd.to_numeric(_nse_costed.get("v7_nse_id_sebi_rs", pd.Series(dtype=float)), errors="coerce").sum(), 0.0) if not _nse_costed.empty else 0.0,
        "v7_nse_id_ipft_rs": _safe_float(pd.to_numeric(_nse_costed.get("v7_nse_id_ipft_rs", pd.Series(dtype=float)), errors="coerce").sum(), 0.0) if not _nse_costed.empty else 0.0,
        "v7_nse_id_stamp_rs": _safe_float(pd.to_numeric(_nse_costed.get("v7_nse_id_stamp_rs", pd.Series(dtype=float)), errors="coerce").sum(), 0.0) if not _nse_costed.empty else 0.0,
        "v7_nse_id_gst_rs": _safe_float(pd.to_numeric(_nse_costed.get("v7_nse_id_gst_rs", pd.Series(dtype=float)), errors="coerce").sum(), 0.0) if not _nse_costed.empty else 0.0,
    }
    summary = {
        "paper_rows": int(len(paper)),
        "paper_traded_rows": int(len(traded)),
        "paper_skipped_rows": int(skipped_mask.sum()) if len(skipped_mask) else 0,
        "anti_chase_skips": anti_chase_skips,
        "targets": int((traded.get("outcome", pd.Series(dtype=str)) == "TARGET").sum()) if not traded.empty else 0,
        "sl": int((traded.get("outcome", pd.Series(dtype=str)) == "SL").sum()) if not traded.empty else 0,
        "quick_targets_10m": int(len(quick_target)),
        "quick_sl_15m": int(len(quick_sl)),
        "avg_hold_target_min": _safe_float(pd.to_numeric(traded.loc[traded.get("outcome", pd.Series(dtype=str)) == "TARGET", "hold_min"], errors="coerce").mean(), np.nan) if not traded.empty and "hold_min" in traded.columns else np.nan,
        "avg_hold_sl_min": _safe_float(pd.to_numeric(traded.loc[traded.get("outcome", pd.Series(dtype=str)) == "SL", "hold_min"], errors="coerce").mean(), np.nan) if not traded.empty and "hold_min" in traded.columns else np.nan,
        "open_trades": int(len(open_df)),
        "slow_open_trades": int(open_df.get("slow_mover", pd.Series(dtype=bool)).sum()) if not open_df.empty else 0,
        "open_avg_progress_r": _safe_float(pd.to_numeric(open_df.get("progress_r", pd.Series(dtype=float)), errors="coerce").mean(), np.nan) if not open_df.empty else np.nan,
        "open_unrealized_pnl_rs": _safe_float(pd.to_numeric(open_df.get("open_unrealized_pnl_rs", pd.Series(dtype=float)), errors="coerce").sum(), 0.0) if not open_df.empty else 0.0,
        "ttp_shadow_trigger_count": int(open_df.get("ttp_shadow_trigger", pd.Series(dtype=bool)).sum()) if not open_df.empty else 0,
        "ttp_shadow_exit_or_tighten_count": int((open_df.get("ttp_shadow_action", pd.Series(dtype=str)) == "EXIT_OR_TIGHTEN_SHADOW").sum()) if not open_df.empty else 0,
        "ttp_shadow_breakeven_count": int((open_df.get("ttp_shadow_action", pd.Series(dtype=str)) == "TIGHTEN_TO_BREAKEVEN_SHADOW").sum()) if not open_df.empty else 0,
        "freshness_weak_trades": int(len(weak_freshness)),
        "freshness_strong_trades": int(len(strong_freshness)),
        "freshness_weak_sl": int((weak_freshness.get("outcome", pd.Series(dtype=str)) == "SL").sum()) if not weak_freshness.empty else 0,
        "freshness_weak_target": int((weak_freshness.get("outcome", pd.Series(dtype=str)) == "TARGET").sum()) if not weak_freshness.empty else 0,
        "freshness_strong_sl": int((strong_freshness.get("outcome", pd.Series(dtype=str)) == "SL").sum()) if not strong_freshness.empty else 0,
        "freshness_strong_target": int((strong_freshness.get("outcome", pd.Series(dtype=str)) == "TARGET").sum()) if not strong_freshness.empty else 0,
        "freshness_avg_score": _safe_float(pd.to_numeric(freshness.get("freshness_score", pd.Series(dtype=float)), errors="coerce").mean(), np.nan) if not freshness.empty else np.nan,
        "freshness_weak_open": int(open_df.get("freshness_bucket", pd.Series(dtype=str)).eq("WEAK").sum()) if not open_df.empty else 0,
        "anti_chase_audited": int(len(anti_chase_audit)),
        "anti_chase_shadow_target": int(anti_shadow_outcome.eq("TARGET").sum()) if len(anti_shadow_outcome) else 0,
        "anti_chase_shadow_sl": int(anti_shadow_outcome.isin(["SL", "AMBIGUOUS_BOTH"]).sum()) if len(anti_shadow_outcome) else 0,
        "anti_chase_shadow_open": int(anti_shadow_outcome.eq("OPEN").sum()) if len(anti_shadow_outcome) else 0,
        "anti_chase_shadow_net_rs": _safe_float(anti_shadow_pnl.sum(), 0.0) if len(anti_shadow_pnl) else 0.0,
        # cost model fields
        "cost_model": "v7_nse_id_cost",
        "cost_rates_as_of": NSE_ID_COST_CONFIG.rates_as_of,
        "gross_pnl_rs": _nse_gross_pnl,
        "paper_gross_pnl_rs": _paper_gross_pnl,
        "v7_nse_id_costed_trades": _nse_costed_count,
        "v7_nse_id_total_cost_rs": _nse_total_cost,
        "v7_nse_id_net_pnl_rs": _nse_net_pnl,
        "v7_nse_id_avg_cost_bps_of_turnover": _nse_avg_cost_bps,
        "v7_nse_id_avg_cost_pct_of_entry": _nse_avg_cost_pct_entry,
        **_nse_component_sums,
        # legacy dashboard keys now point at NSE model outputs
        "est_brokerage_rs": _nse_component_sums["v7_nse_id_brokerage_rs"],
        "est_slippage_rs": 0.0,
        "est_net_pnl_rs": _nse_net_pnl,
    }
    return summary, paper, open_df, anti_chase_audit


def _latest_fetch_context(day: str) -> dict[str, Any]:
    status = _read_json_file(LIVE_5MIN_STATUS_JSON)
    supervisor = _read_kv_file(LIVE_5MIN_SUPERVISOR_STATUS)
    ymd = day.replace("-", "")
    markers = sorted(SLOT_READY_DIR.glob(f"slot_{ymd}_*.json"))
    latest_marker = _read_json_file(markers[-1]) if markers else {}
    source = status or latest_marker
    slot_ist = source.get("slot_ist") or latest_marker.get("slot_ist", "")
    duration = _safe_float(source.get("total_elapsed_sec"), np.nan)
    if not np.isfinite(duration):
        duration = _safe_float(source.get("duration_ms", latest_marker.get("duration_ms")), np.nan)
        if np.isfinite(duration) and duration > 1000:
            duration /= 1000.0
    return {
        "slot_ist": _fmt_ts(slot_ist),
        "updated_at_ist": _fmt_ts(source.get("updated_at_ist") or latest_marker.get("published_at_ist")),
        "duration_sec": duration,
        "sla_state": str(source.get("sla_state", "")),
        "overall_state": str(source.get("overall_state", "")),
        "tickers_expected": int(_safe_float(source.get("tickers_expected", latest_marker.get("tickers_expected", 0)), 0)),
        "tickers_written": int(_safe_float(source.get("tickers_written", latest_marker.get("tickers_written", 0)), 0)),
        "verification_failed_count": int(_safe_float(source.get("verification_failed_count", latest_marker.get("verification_failed_count", 0)), 0)),
        "max_partition_elapsed_sec": _safe_float(source.get("max_partition_elapsed_sec"), np.nan),
        "min_partition_elapsed_sec": _safe_float(source.get("min_partition_elapsed_sec"), np.nan),
        "avg_partition_elapsed_sec": _safe_float(source.get("avg_partition_elapsed_sec"), np.nan),
        "worker_budget": int(_safe_float(source.get("total_worker_budget", 0), 0)),
        "effective_per_app": int(_safe_float(source.get("effective_per_app", 0), 0)),
        "supervisor_status": str(supervisor.get("status", "")),
    }


def _latency_audit(fetch: dict[str, Any], signal: dict[str, Any], entry: dict[str, Any], slot_rows: pd.DataFrame) -> dict[str, Any]:
    publish = _safe_float(signal.get("publish_delay_sec"), np.nan)
    tier123 = _safe_float(signal.get("tier123_scan_elapsed_sec"), np.nan)
    entry_write = _safe_float(entry.get("write_delay_sec"), np.nan)
    fetch_sec = _safe_float(fetch.get("duration_sec"), np.nan)
    raw_entry_fetch = _safe_float(entry.get("raw_fetch_elapsed_sec"), np.nan)
    scanner_overhead = max(0.0, publish - tier123) if np.isfinite(publish) and np.isfinite(tier123) else np.nan
    entry_after_scan = max(0.0, entry_write - publish) if np.isfinite(entry_write) and np.isfinite(publish) else np.nan

    components = {
        "5min_fetch_sec": fetch_sec,
        "tier123_scan_sec": tier123,
        "scanner_overhead_sec": scanner_overhead,
        "entry_after_scan_sec": entry_after_scan,
        "entry_raw_fetch_sec": raw_entry_fetch,
    }
    finite_components = {k: v for k, v in components.items() if np.isfinite(_safe_float(v))}
    bottleneck = max(finite_components.items(), key=lambda kv: kv[1])[0] if finite_components else ""

    recent = slot_rows.tail(8).copy() if not slot_rows.empty else pd.DataFrame()
    return {
        "bottleneck": bottleneck,
        **components,
        "recent_avg_fetch_sec": _safe_float(pd.to_numeric(recent.get("fetch_sec", pd.Series(dtype=float)), errors="coerce").mean(), np.nan) if not recent.empty else np.nan,
        "recent_avg_publish_delay_sec": _safe_float(pd.to_numeric(recent.get("scan_publish_delay_sec", pd.Series(dtype=float)), errors="coerce").mean(), np.nan) if not recent.empty else np.nan,
        "recent_avg_scanner_overhead_sec": _safe_float(pd.to_numeric(recent.get("scanner_overhead_sec", pd.Series(dtype=float)), errors="coerce").mean(), np.nan) if not recent.empty else np.nan,
        "recent_avg_entry_after_scan_sec": _safe_float(pd.to_numeric(recent.get("entry_after_scan_sec", pd.Series(dtype=float)), errors="coerce").mean(), np.nan) if not recent.empty else np.nan,
        "note": "scanner_publish_path" if _safe_float(scanner_overhead, 0.0) > 20.0 else ("data_fetch_path" if _safe_float(fetch_sec, 0.0) > 30.0 else "entry_path_ok"),
    }


def _recommendations(
    fetch: dict[str, Any],
    signal: dict[str, Any],
    entry: dict[str, Any],
    pre: dict[str, Any],
    paper: dict[str, Any],
    latency: dict[str, Any],
    concentration: dict[str, Any],
    slot_pressure: dict[str, Any],
    portfolio: dict[str, Any],
    path_lab: dict[str, Any],
) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    if _safe_float(fetch.get("duration_sec"), np.nan) > 30:
        rows.append(
            {
                "area": "5min fetch",
                "severity": "WATCH",
                "finding": f"5min fetch is over soft SLA at {_fmt_num(fetch.get('duration_sec'), 1)}s.",
                "suggestion": "Keep current workers for now; if this persists, quarantine consistently slow symbols or split the slowest app partition, not a blanket CPU increase.",
            }
        )
    if _safe_float(signal.get("publish_delay_sec"), np.nan) > 45:
        rows.append(
            {
                "area": "5min scanner",
                "severity": "WARN",
                "finding": f"candidate publish delay is {_fmt_num(signal.get('publish_delay_sec'), 1)}s.",
                "suggestion": "Scanner is the latency source; keep Tier123 parallel and watch candidate JSON creation time before changing entry engine.",
            }
        )
    if _safe_float(latency.get("scanner_overhead_sec"), np.nan) > 20:
        rows.append(
            {
                "area": "scanner publish overhead",
                "severity": "WARN",
                "finding": f"Scanner overhead outside Tier123 is {_fmt_num(latency.get('scanner_overhead_sec'), 1)}s.",
                "suggestion": "Instrument candidate JSON assembly/write and v11 overlay merge timing before increasing entry-engine workers.",
            }
        )
    if _safe_float(latency.get("entry_raw_fetch_sec"), np.nan) < 2 and _safe_float(latency.get("entry_after_scan_sec"), np.nan) < 15:
        rows.append(
            {
                "area": "entry latency",
                "severity": "OK",
                "finding": f"Entry raw fetch is {_fmt_num(latency.get('entry_raw_fetch_sec'), 3)}s and entry-after-scan lag is {_fmt_num(latency.get('entry_after_scan_sec'), 1)}s.",
                "suggestion": "Do not spend the next optimization pass on entry raw fetch; it is not the bottleneck in this snapshot.",
            }
        )
    if int(pre.get("latest_nan_rejects", 0)) > 0:
        rows.append(
            {
                "area": "pre momentum",
                "severity": "FAIL",
                "finding": f"{pre.get('latest_nan_rejects')} latest-slot pre-momentum rejects had NaN feature reasons.",
                "suggestion": "Treat as data-source bug first, not strict filter evidence.",
            }
        )
    if int(pre.get("latest_feature_gap_rows", 0)) > 0:
        rows.append(
            {
                "area": "pre momentum data gaps",
                "severity": "WARN",
                "finding": f"{pre.get('latest_feature_gap_rows')} latest feature-gap rows exist on required pre-momentum inputs.",
                "suggestion": "Inspect the feature-gap table by ticker before changing the pre-momentum thresholds.",
            }
        )
    if int(paper.get("anti_chase_skips", 0)) > 0:
        rows.append(
            {
                "area": "paper executor",
                "severity": "INFO",
                "finding": f"{paper.get('anti_chase_skips')} paper rows skipped by anti-chase research gate.",
                "suggestion": "This is still a second active gate after scanner/entry. Keep it only as research accounting, or disable it if you want one gate path.",
            }
        )
    if int(paper.get("anti_chase_audited", 0)) > 0:
        severity = "WARN" if int(paper.get("anti_chase_shadow_target", 0)) > int(paper.get("anti_chase_shadow_sl", 0)) else "INFO"
        rows.append(
            {
                "area": "anti-chase shadow audit",
                "severity": severity,
                "finding": (
                    f"Skipped audit: target {paper.get('anti_chase_shadow_target', 0)}, "
                    f"SL {paper.get('anti_chase_shadow_sl', 0)}, open {paper.get('anti_chase_shadow_open', 0)}, "
                    f"net Rs {_fmt_num(paper.get('anti_chase_shadow_net_rs'), 0)}."
                ),
                "suggestion": "If skipped targets keep exceeding skipped SLs, soften anti-chase into delayed-entry/sizing instead of hard skip.",
            }
        )
    if int(paper.get("slow_open_trades", 0)) > 0:
        rows.append(
            {
                "area": "slow movers",
                "severity": "WATCH",
                "finding": f"{paper.get('slow_open_trades')}/{paper.get('open_trades')} open trades are older than 10m and below +0.25R progress.",
                "suggestion": "Research a time-to-progress gate: after entry, require +0.20R to +0.30R within 5-8 minutes, otherwise tighten to breakeven/partial exit. Do not add it as a pre-entry block yet.",
            }
        )
    if int(paper.get("ttp_shadow_trigger_count", 0)) > 0:
        rows.append(
            {
                "area": "time-to-progress shadow",
                "severity": "WATCH",
                "finding": f"{paper.get('ttp_shadow_trigger_count')} open trades trigger the {TTP_WAIT_MIN:.0f}m/<+{TTP_MIN_PROGRESS_R:.2f}R shadow rule.",
                "suggestion": "Log the shadow action only today; evaluate target retention before making any executor change.",
            }
        )
    if int(paper.get("quick_sl_15m", 0)) > int(paper.get("quick_targets_10m", 0)) and int(paper.get("quick_sl_15m", 0)) >= 2:
        rows.append(
            {
                "area": "momentum quality",
                "severity": "WARN",
                "finding": "Quick SL count is exceeding quick target count.",
                "suggestion": "The pre-entry filter may be allowing direction without acceleration. Shadow-test an acceleration score using pre1_body_r, pre2_mom_r, pre3_close_pos, and first-minute follow-through.",
            }
        )
    if int(paper.get("freshness_weak_trades", 0)) > 0:
        rows.append(
            {
                "area": "freshness score",
                "severity": "WATCH",
                "finding": f"{paper.get('freshness_weak_trades')} traded rows are WEAK by the shadow freshness score.",
                "suggestion": "Compare weak-score SL/target mix against STRONG rows before using freshness as a delay/reconfirm rule.",
            }
        )
    if str(concentration.get("open_concentration_state", "")) == "CONCENTRATED_SHADOW":
        rows.append(
            {
                "area": "setup concentration",
                "severity": "WARN",
                "finding": (
                    f"{concentration.get('open_dominant_setup_count', 0)} open trades "
                    f"({ _fmt_num(concentration.get('open_dominant_setup_pct'), 1)}%) are "
                    f"{concentration.get('open_dominant_setup', '')}."
                ),
                "suggestion": "Shadow-test per-setup exposure caps or staggered entries when one setup dominates the open book.",
            }
        )
    if "FLOOD" in str(slot_pressure.get("slot_pressure_state", "")) or "DUPLICATE" in str(slot_pressure.get("slot_pressure_state", "")):
        rows.append(
            {
                "area": "slot pressure",
                "severity": "WATCH",
                "finding": (
                    f"Latest slot state {slot_pressure.get('slot_pressure_state', '')}: "
                    f"{slot_pressure.get('slot_final_candidates', 0)} candidates, "
                    f"{_fmt_num(slot_pressure.get('slot_dominant_setup_pct'), 1)}% "
                    f"{slot_pressure.get('slot_dominant_setup', '')}, duplicate { _fmt_num(slot_pressure.get('slot_duplicate_pct'), 1)}%."
                ),
                "suggestion": "Shadow-test top-N/staggered entry when a slot is mostly one setup and many candidates are duplicates.",
            }
        )
    if str(portfolio.get("portfolio_shadow_state", "")) in {"YELLOW_THROTTLE_SHADOW", "RED_PAUSE_SHADOW"}:
        rows.append(
            {
                "area": "portfolio shadow brake",
                "severity": "WARN" if str(portfolio.get("portfolio_shadow_state", "")) == "YELLOW_THROTTLE_SHADOW" else "FAIL",
                "finding": (
                    f"{portfolio.get('portfolio_shadow_state', '')}: closed Rs {_fmt_num(portfolio.get('closed_pnl_rs'), 0)}, "
                    f"open Rs {_fmt_num(portfolio.get('open_unrealized_pnl_rs'), 0)}, combined Rs {_fmt_num(portfolio.get('combined_paper_pnl_rs'), 0)}."
                ),
                "suggestion": "Shadow-test session throttling after combined paper PnL breaches the day-risk line; do not enforce live without multi-day proof.",
            }
        )
    if int(path_lab.get("path_trail_after_05r_count", 0)) > 0 or int(path_lab.get("path_time_to_progress_count", 0)) > 0:
        rows.append(
            {
                "area": "path quality lab",
                "severity": "WATCH",
                "finding": (
                    f"path rows {path_lab.get('path_lab_ok_rows', 0)}: trail-after-0.5R "
                    f"{path_lab.get('path_trail_after_05r_count', 0)}, no-progress {path_lab.get('path_time_to_progress_count', 0)}, "
                    f"avg giveback {_fmt_num(path_lab.get('path_avg_giveback_r'), 2)}R."
                ),
                "suggestion": "Shadow-test a two-stage exit: require early progress, then trail only after +0.5R is seen.",
            }
        )
    if not rows:
        rows.append(
            {
                "area": "overall",
                "severity": "OK",
                "finding": "No immediate operations blocker from the lightweight scan.",
                "suggestion": "Continue collecting slot-level evidence before changing live gates.",
            }
        )
    return rows


def _render_markdown(
    day: str,
    fetch: dict[str, Any],
    signal: dict[str, Any],
    entry: dict[str, Any],
    latency: dict[str, Any],
    slot_pressure: dict[str, Any],
    pre: dict[str, Any],
    pre_reasons: pd.DataFrame,
    pre_gap_rows: pd.DataFrame,
    flow_rows: pd.DataFrame,
    paper: dict[str, Any],
    concentration: dict[str, Any],
    concentration_rows: pd.DataFrame,
    portfolio: dict[str, Any],
    path_summary: dict[str, Any],
    path_lab: pd.DataFrame,
    paper_rows: pd.DataFrame,
    open_df: pd.DataFrame,
    anti_chase_audit: pd.DataFrame,
    slot_rows: pd.DataFrame,
    recs: list[dict[str, str]],
) -> str:
    lines: list[str] = []
    lines.append(f"# V7 Live Light Ops Research - {day}")
    lines.append("")
    lines.append(f"Generated: {_fmt_ts(_now_ist())}")
    lines.append("")
    lines.append("This is lightweight intraday research. It reads existing status/audit files only; no replay or parquet sweep.")
    lines.append("")

    lines.append("## Snapshot")
    lines.append("")
    lines.append("| area | slot | key metrics | status |")
    lines.append("|---|---|---|---|")
    lines.append(
        f"| 5min live fetch | {fetch.get('slot_ist', '')} | "
        f"{fetch.get('tickers_written', 0)}/{fetch.get('tickers_expected', 0)} tickers, "
        f"{_fmt_num(fetch.get('duration_sec'), 1)}s, max app {_fmt_num(fetch.get('max_partition_elapsed_sec'), 1)}s | "
        f"{fetch.get('overall_state', '')}/{fetch.get('sla_state', '')} |"
    )
    lines.append(
        f"| 5min scanner | {signal.get('slot_ist', '')} | raw {signal.get('raw_candidates', 0)}, "
        f"v11 in {signal.get('v11_input', 0)}, selected {signal.get('v11_selected', 0)}, rejected {signal.get('v11_rejected', 0)}, "
        f"publish {_fmt_num(signal.get('publish_delay_sec'), 1)}s | tier123 {_fmt_num(signal.get('tier123_scan_elapsed_sec'), 1)}s/{signal.get('tier123_workers', 0)}w |"
    )
    lines.append(
        f"| 1min entry engine | {entry.get('slot_ist', '')} | requested {entry.get('tickers_requested', 0)}, "
        f"fetched {entry.get('tickers_fetched', 0)}, raw fetch {_fmt_num(entry.get('raw_fetch_elapsed_sec'), 3)}s, "
        f"entries {entry.get('entry_rows', 0)}, duplicates {entry.get('intraday_duplicate_rows', 0)} | "
        f"{entry.get('raw_fetch_mode', '')}/{entry.get('raw_fetch_active_apps', 0)} apps |"
    )
    lines.append(
        f"| pre momentum | {entry.get('slot_ist', '')} | input {entry.get('pre_momentum_input_rows', 0)}, "
        f"pass {entry.get('pre_momentum_output_rows', 0)}, reject {entry.get('pre_momentum_rejected_rows', 0)}, "
        f"latest NaN rejects {pre.get('latest_nan_rejects', 0)}, day pass rate {_fmt_num(pre.get('pass_rate_pct'), 1)}% | data ok {pre.get('data_sufficiency_ok')} |"
    )
    lines.append(
        f"| paper trades | {day} | traded {paper.get('paper_traded_rows', 0)}, target {paper.get('targets', 0)}, "
        f"SL {paper.get('sl', 0)}, open {paper.get('open_trades', 0)}, slow open {paper.get('slow_open_trades', 0)}, "
        f"anti-chase skips {paper.get('anti_chase_skips', 0)} | avg open progress {_fmt_num(paper.get('open_avg_progress_r'), 2)}R, open PnL Rs {_fmt_num(paper.get('open_unrealized_pnl_rs'), 0)} |"
    )
    lines.append("")

    lines.append("## Shadow Research Summary")
    lines.append("")
    lines.append("| step | current finding | shadow action |")
    lines.append("|---|---|---|")
    lines.append(
        f"| 1 time-to-progress | {paper.get('ttp_shadow_trigger_count', 0)} open trades older than {TTP_WAIT_MIN:.0f}m are below +{TTP_MIN_PROGRESS_R:.2f}R | log tighten/exit candidates only |"
    )
    lines.append(
        f"| 2 freshness score | weak traded {paper.get('freshness_weak_trades', 0)} vs strong traded {paper.get('freshness_strong_trades', 0)}; avg score {_fmt_num(paper.get('freshness_avg_score'), 1)} | compare weak/strong target retention |"
    )
    lines.append(
        f"| 3 feature gaps | latest gap rows {pre.get('latest_feature_gap_rows', 0)}, day NaN rejects {pre.get('nan_rejects', 0)} | fix data before tightening thresholds |"
    )
    lines.append(
        f"| 4 latency | bottleneck {latency.get('bottleneck', '')}; scanner overhead {_fmt_num(latency.get('scanner_overhead_sec'), 1)}s | instrument scanner publish path |"
    )
    lines.append(
        f"| 5 anti-chase audit | audited {paper.get('anti_chase_audited', 0)} skipped: target {paper.get('anti_chase_shadow_target', 0)}, SL {paper.get('anti_chase_shadow_sl', 0)}, open {paper.get('anti_chase_shadow_open', 0)} | keep/soften based on skipped target retention |"
    )
    lines.append("")

    lines.append("## Deep Improvement Radar")
    lines.append("")
    lines.append("| module | state | evidence | shadow suggestion |")
    lines.append("|---|---|---|---|")
    lines.append(
        f"| setup concentration | {concentration.get('open_concentration_state', '')} | "
        f"{concentration.get('open_dominant_setup', '')} is {concentration.get('open_dominant_setup_count', 0)}/{paper.get('open_trades', 0)} open "
        f"({_fmt_num(concentration.get('open_dominant_setup_pct'), 1)}%), unrealized Rs {_fmt_num(concentration.get('open_dominant_setup_unrealized_rs'), 0)} | "
        "shadow per-setup exposure cap/stagger |"
    )
    lines.append(
        f"| slot pressure | {slot_pressure.get('slot_pressure_state', '')} | "
        f"{slot_pressure.get('slot_final_candidates', 0)} candidates, {slot_pressure.get('slot_dominant_setup', '')} "
        f"{_fmt_num(slot_pressure.get('slot_dominant_setup_pct'), 1)}%, duplicates {_fmt_num(slot_pressure.get('slot_duplicate_pct'), 1)}% | "
        "shadow top-N/stagger when one setup floods a slot |"
    )
    lines.append(
        f"| portfolio brake | {portfolio.get('portfolio_shadow_state', '')} | "
        f"closed Rs {_fmt_num(portfolio.get('closed_pnl_rs'), 0)}, open Rs {_fmt_num(portfolio.get('open_unrealized_pnl_rs'), 0)}, "
        f"combined Rs {_fmt_num(portfolio.get('combined_paper_pnl_rs'), 0)}, PF {_fmt_num(portfolio.get('closed_profit_factor'), 2)} | "
        "shadow throttle/pause only after multi-day proof |"
    )
    lines.append(
        f"| path/MFE lab | rows {path_summary.get('path_lab_ok_rows', 0)} | "
        f"avg best {_fmt_num(path_summary.get('path_avg_best_r'), 2)}R, avg worst {_fmt_num(path_summary.get('path_avg_worst_r'), 2)}R, "
        f"giveback {_fmt_num(path_summary.get('path_avg_giveback_r'), 2)}R | "
        "shadow trail after +0.5R and no-progress exit |"
    )
    lines.append("")

    lines.append("## Candidate Flow By Recent Slot")
    lines.append("")
    if slot_rows.empty:
        lines.append("No recent slot rows found.")
    else:
        lines.append("| slot | fetch s | scan pub s | tier123 s | overhead s | entry after s | raw | v11 in | v11 sel | no_entry | pre rej | entries | late? |")
        lines.append("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|")
        for _, row in slot_rows.tail(8).iterrows():
            no_entry = int(row.get("no_entry_row_count", 0))
            late_flag = "⚠ LATE" if row.get("pipeline_late", False) else ""
            lines.append(
                f"| {row.get('slot', '')} | {_fmt_num(row.get('fetch_sec'), 1)} | {_fmt_num(row.get('scan_publish_delay_sec'), 1)} | "
                f"{_fmt_num(row.get('tier123_sec'), 1)} | {_fmt_num(row.get('scanner_overhead_sec'), 1)} | "
                f"{_fmt_num(row.get('entry_after_scan_sec'), 1)} | {int(row.get('raw_scanner_candidates', 0))} | "
                f"{int(row.get('v11_input', 0))} | {int(row.get('v11_selected', 0))} | "
                f"**{no_entry}** | {int(row.get('pre_mom_rejected', 0))} | {int(row.get('entry_rows', 0))} | {late_flag} |"
            )
        # alert if any slot had gated candidates with no entry rows
        _no_entry_total = int(slot_rows.get("no_entry_row_count", pd.Series(0, dtype=int)).sum())
        if _no_entry_total > 0:
            lines.append("")
            lines.append(f"> **{_no_entry_total} gated candidates had no entry row today** — check entry engine for C_OR_BREAKOUT profile rule or time-window cutoff.")
    lines.append("")

    lines.append("## Scanner And Entry Candidates")
    lines.append("")
    lines.append(f"- Latest scanner setup counts: {signal.get('final_setup_counts', '') or 'none'}")
    lines.append(f"- Latest scanner selected tickers: {signal.get('final_tickers', '') or 'none'}")
    lines.append(f"- Latest 1min entry tickers: {entry.get('entry_tickers', '') or 'none'}")
    lines.append(f"- Raw scanner setup counts before gates: {signal.get('raw_setup_counts', '') or 'none'}")
    lines.append("")

    lines.append("## Latest Ticker-Level Flow")
    lines.append("")
    if flow_rows.empty:
        lines.append("No latest scanner/entry detail rows yet. This is normal before the first live 5-minute slot is processed.")
    else:
        flow_counts = (
            flow_rows.groupby(["stage", "status"], dropna=False)
            .size()
            .reset_index(name="rows")
            .sort_values(["stage", "status"])
        )
        lines.append("| stage | status | rows |")
        lines.append("|---|---|---:|")
        for _, row in flow_counts.iterrows():
            lines.append(f"| {row.get('stage', '')} | {row.get('status', '')} | {int(row.get('rows', 0))} |")
        lines.append("")
        lines.append("| stage | status | ticker | side | setup | ranker | pre2 R | pre score | adx5 | reason |")
        lines.append("|---|---|---|---|---|---:|---:|---:|---:|---|")
        view = flow_rows.copy()
        if len(view) > 45:
            failed = view.loc[view["status"].astype(str).eq("FAILED")].head(25)
            passed = view.loc[view["status"].astype(str).ne("FAILED")].head(20)
            view = pd.concat([failed, passed], ignore_index=True, sort=False)
        for _, row in view.iterrows():
            reason = str(row.get("reason", "")).replace("|", "/")
            if len(reason) > 95:
                reason = reason[:92] + "..."
            lines.append(
                f"| {row.get('stage', '')} | {row.get('status', '')} | {row.get('ticker', '')} | "
                f"{row.get('side', '')} | {row.get('setup', '')} | {_fmt_num(row.get('ranker_score'), 1)} | "
                f"{_fmt_num(row.get('pre2_mom_r'), 2)} | {_fmt_num(row.get('pre_entry_momentum_score'), 1)} | "
                f"{_fmt_num(row.get('sig5_adx_calc'), 1)} | {reason} |"
            )
    lines.append("")

    lines.append("## Pre-Momentum Filter Health")
    lines.append("")
    missing = pre.get("latest_missing_required_feature_counts", {})
    lines.append(f"- Required features checked: `{', '.join(missing.keys())}`")
    lines.append(f"- Latest-slot missing required feature counts on accepted rows: {missing}")
    lines.append(f"- Latest-slot minimum pre-entry 1m bars seen on accepted rows: {_fmt_num(pre.get('latest_min_pre_bars'), 0)}")
    lines.append(f"- Day-so-far NaN rejects: {pre.get('nan_rejects', 0)}; latest-slot NaN rejects: {pre.get('latest_nan_rejects', 0)}")
    nan_features = pre.get("nan_reason_feature_counts", {})
    if nan_features:
        lines.append(f"- NaN reject features by count: {nan_features}")
    if not pre_reasons.empty:
        lines.append("")
        lines.append("| reject reason | count |")
        lines.append("|---|---:|")
        for _, row in pre_reasons.iterrows():
            lines.append(f"| {row.get('reason', '')} | {int(row.get('count', 0))} |")
    if not pre_gap_rows.empty:
        lines.append("")
        lines.append("### Feature Gap Samples")
        lines.append("")
        lines.append("| scope | feature | missing | total | sample tickers | sample setups |")
        lines.append("|---|---|---:|---:|---|---|")
        for _, row in pre_gap_rows.head(20).iterrows():
            lines.append(
                f"| {row.get('scope', '')} | {row.get('feature', '')} | {int(row.get('missing_rows', 0))} | "
                f"{int(row.get('total_rows', 0))} | {row.get('sample_tickers', '')} | {row.get('sample_setups', '')} |"
            )
        # setup-level coverage summary: count gap rows per setup
        _gap_by_setup: dict[str, int] = {}
        for _, row in pre_gap_rows.iterrows():
            for setup_str in str(row.get("sample_setups", "")).split(","):
                s = setup_str.strip()
                if s:
                    _gap_by_setup[s] = _gap_by_setup.get(s, 0) + int(row.get("missing_rows", 0))
        if _gap_by_setup:
            lines.append("")
            lines.append("### Missing Pre-Momentum Feature Coverage By Setup")
            lines.append("")
            lines.append("| setup | missing feature-row count | action |")
            lines.append("|---|---:|---|")
            for _s, _cnt in sorted(_gap_by_setup.items(), key=lambda kv: -kv[1]):
                _action = "investigate 1-min bar count at signal time" if _cnt > 0 else "ok"
                lines.append(f"| {_s} | {_cnt} | {_action} |")
    lines.append("")

    lines.append("## Paper SL/Target And Slow-Mover Check")
    lines.append("")

    # --- cost section ---
    lines.append("### P&L Cost Summary")
    lines.append("")
    lines.append("| metric | value |")
    lines.append("|---|---:|")
    lines.append(f"| cost model | V7 NSE ID cost ({paper.get('cost_rates_as_of', '')}) |")
    lines.append(f"| costed trades | {paper.get('v7_nse_id_costed_trades', 0)} / {paper.get('paper_traded_rows', 0)} |")
    lines.append(f"| gross paper P&L | Rs {_fmt_num(paper.get('gross_pnl_rs'), 0)} |")
    lines.append(f"| NSE statutory + brokerage cost | Rs {_fmt_num(paper.get('v7_nse_id_total_cost_rs'), 0)} |")
    lines.append(f"| brokerage component | Rs {_fmt_num(paper.get('v7_nse_id_brokerage_rs'), 0)} |")
    lines.append(f"| STT / stamp / GST | Rs {_fmt_num(paper.get('v7_nse_id_stt_rs'), 0)} / Rs {_fmt_num(paper.get('v7_nse_id_stamp_rs'), 0)} / Rs {_fmt_num(paper.get('v7_nse_id_gst_rs'), 0)} |")
    lines.append(f"| avg cost | {_fmt_num(paper.get('v7_nse_id_avg_cost_bps_of_turnover'), 2)} bps turnover / {_fmt_num(paper.get('v7_nse_id_avg_cost_pct_of_entry'), 3)}% entry notional |")
    lines.append(f"| **V7 NSE ID net P&L** | **Rs {_fmt_num(paper.get('v7_nse_id_net_pnl_rs'), 0)}** |")
    lines.append("")

    if not paper_rows.empty and "setup" in paper_rows.columns:
        lines.append("### Per-Setup Net PnL Attribution")
        lines.append("")
        lines.append("Exact per-setup breakdown for strategy improvement decisions. "
                     "`win_rate` = TARGET/(TARGET+SL) for closed trades. "
                     "`net_pf` = net wins / |net losses| (net-of-cost).")
        lines.append("")
        _attr_rows: list[dict[str, Any]] = []
        for _setup, _grp in paper_rows.groupby("setup", dropna=False):
            _out = _grp.get("outcome", pd.Series(dtype=str)).fillna("").astype(str).str.upper()
            _closed = _grp.loc[_out.isin(["TARGET", "SL"])].copy()
            _tgt = int((_out == "TARGET").sum())
            _sl_n = int((_out == "SL").sum())
            _net_col = _grp.get("v7_nse_id_net_pnl_rs", pd.Series(dtype=float))
            if pd.to_numeric(_net_col, errors="coerce").notna().sum() == 0:
                _net_col = _grp.get("pnl_rs", pd.Series(dtype=float))
            _net_vals = pd.to_numeric(_net_col, errors="coerce").fillna(0.0)
            _net_wins = float(_net_vals[_net_vals > 0].sum())
            _net_losses = float(-_net_vals[_net_vals < 0].sum())
            _net_pf = (_net_wins / _net_losses) if _net_losses > 0 else (float("inf") if _net_wins > 0 else 0.0)
            _win_rate = _tgt / max(_tgt + _sl_n, 1) if (_tgt + _sl_n) > 0 else float("nan")
            _cost_total = float(
                pd.to_numeric(_grp.get("v7_nse_id_total_cost_rs", pd.Series(dtype=float)), errors="coerce").fillna(0.0).sum()
            )
            _attr_rows.append(
                {
                    "setup": _setup,
                    "trades": int(len(_grp)),
                    "T": _tgt,
                    "SL": _sl_n,
                    "open": int(len(_grp)) - _tgt - _sl_n,
                    "win_rate%": f"{100.0 * _win_rate:.1f}" if not math.isnan(_win_rate) else "N/A",
                    "net_pnl_rs": f"{float(_net_vals.sum()):.0f}",
                    "cost_rs": f"{_cost_total:.0f}",
                    "net_pf": f"{_net_pf:.3f}" if np.isfinite(_net_pf) else "inf",
                }
            )
        _attr_df = pd.DataFrame(_attr_rows).sort_values("trades", ascending=False)
        _acols = list(_attr_df.columns)
        lines.append("| " + " | ".join(_acols) + " |")
        lines.append("| " + " | ".join("---" for _ in _acols) + " |")
        for _, _row in _attr_df.iterrows():
            lines.append("| " + " | ".join(str(_row.get(c, "")).replace("|", "\\|") for c in _acols) + " |")
        lines.append("")

    lines.append(
        f"- Quick target <=10m: {paper.get('quick_targets_10m', 0)}; quick SL <=15m: {paper.get('quick_sl_15m', 0)}."
    )
    lines.append(
        f"- Avg target hold: {_fmt_num(paper.get('avg_hold_target_min'), 1)}m; avg SL hold: {_fmt_num(paper.get('avg_hold_sl_min'), 1)}m."
    )
    lines.append(
        f"- Time-to-progress shadow: {paper.get('ttp_shadow_exit_or_tighten_count', 0)} exit/tighten, "
        f"{paper.get('ttp_shadow_breakeven_count', 0)} breakeven-tighten; open unrealized PnL Rs {_fmt_num(paper.get('open_unrealized_pnl_rs'), 0)}."
    )
    lines.append(
        f"- Freshness shadow: WEAK target/SL {paper.get('freshness_weak_target', 0)}/{paper.get('freshness_weak_sl', 0)}, "
        f"STRONG target/SL {paper.get('freshness_strong_target', 0)}/{paper.get('freshness_strong_sl', 0)}."
    )
    if not paper_rows.empty:
        closed_or_recent = paper_rows.copy()
        if "entry_ts" in closed_or_recent.columns:
            closed_or_recent = closed_or_recent.sort_values("entry_ts")
        closed_or_recent = closed_or_recent.tail(20)
        # join path quality from path_lab on signal_id where available
        _path_view = pd.DataFrame()
        if not path_lab.empty and "signal_id" in path_lab.columns:
            _path_cols = [c for c in ["signal_id", "best_r", "worst_r", "giveback_r", "time_to_025r_min", "time_to_050r_min", "path_shadow_action"] if c in path_lab.columns]
            _path_view = path_lab[_path_cols].drop_duplicates("signal_id")
        if not _path_view.empty and "signal_id" in closed_or_recent.columns:
            closed_or_recent = closed_or_recent.merge(_path_view, on="signal_id", how="left")
        has_path = (
            not _path_view.empty
            and any(c in closed_or_recent.columns for c in ["best_r", "worst_r", "giveback_r"])
        )
        lines.append("")
        lines.append("### Recent Paper Outcomes")
        lines.append("")
        if has_path:
            lines.append("| ticker | side | setup | entry | outcome | hold m | gross | cost | net | target | sl | best R | worst R | giveback R | t025 m | t050 m | path action |")
            lines.append("|---|---|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|")
        else:
            lines.append("| ticker | side | setup | entry | outcome | hold m | gross | cost | net | target | sl |")
            lines.append("|---|---|---|---|---|---:|---:|---:|---:|---:|---:|")
        for _, row in closed_or_recent.iterrows():
            _gross = row.get("v7_nse_id_gross_pnl_rs")
            if pd.isna(_gross):
                _gross = row.get("pnl_rs")
            _net = row.get("v7_nse_id_net_pnl_rs")
            if pd.isna(_net):
                _net = row.get("pnl_rs")
            base = (
                f"| {row.get('ticker', '')} | {row.get('side', '')} | {row.get('setup', '')} | "
                f"{_fmt_ts(row.get('entry_time'))} | {row.get('outcome', '')} | {_fmt_num(row.get('hold_min'), 1)} | "
                f"{_fmt_num(_gross, 0)} | {_fmt_num(row.get('v7_nse_id_total_cost_rs'), 0)} | {_fmt_num(_net, 0)} | "
                f"{_fmt_num(row.get('target_price'), 2)} | {_fmt_num(row.get('stop_price'), 2)} |"
            )
            if has_path:
                base += (
                    f" {_fmt_num(row.get('best_r'), 2)} | {_fmt_num(row.get('worst_r'), 2)} | "
                    f"{_fmt_num(row.get('giveback_r'), 2)} | {_fmt_num(row.get('time_to_025r_min'), 1)} | "
                    f"{_fmt_num(row.get('time_to_050r_min'), 1)} | {row.get('path_shadow_action', '')} |"
                )
            lines.append(base)
    if not open_df.empty:
        view_cols = [
            "ticker",
            "side",
            "setup",
            "age_min",
            "progress_r",
            "pre2_mom_r",
            "freshness_score",
            "freshness_bucket",
            "pre_entry_momentum_score",
            "sig5_adx_calc",
            "ttp_shadow_action",
            "slow_mover",
        ]
        view_cols = [c for c in view_cols if c in open_df.columns]
        slow_view = open_df.sort_values(["ttp_shadow_trigger", "slow_mover", "age_min"], ascending=[False, False, False]).head(15)
        lines.append("")
        lines.append("| ticker | side | setup | age m | progress R | fresh | bucket | pre score | action |")
        lines.append("|---|---|---|---:|---:|---:|---|---:|---|")
        for _, row in slow_view.iterrows():
            lines.append(
                f"| {row.get('ticker', '')} | {row.get('side', '')} | {row.get('setup', '')} | "
                f"{_fmt_num(row.get('age_min'), 1)} | {_fmt_num(row.get('progress_r'), 2)} | "
                f"{_fmt_num(row.get('freshness_score'), 1)} | {row.get('freshness_bucket', '')} | "
                f"{_fmt_num(row.get('pre_entry_momentum_score'), 1)} | {row.get('ttp_shadow_action', '')} |"
            )
    lines.append("")

    lines.append("## Setup Concentration Shadow")
    lines.append("")
    if concentration_rows.empty:
        lines.append("No open setup concentration rows.")
    else:
        lines.append("| setup | open | pct | unrealized | avg progress R | weak | ttp shadow |")
        lines.append("|---|---:|---:|---:|---:|---:|---:|")
        for _, row in concentration_rows.head(12).iterrows():
            lines.append(
                f"| {row.get('setup', '')} | {int(row.get('open_trades', 0))} | {_fmt_num(row.get('open_pct'), 1)}% | "
                f"Rs {_fmt_num(row.get('unrealized_pnl_rs'), 0)} | {_fmt_num(row.get('avg_progress_r'), 2)} | "
                f"{int(row.get('weak_open', 0))} | {int(row.get('ttp_shadow', 0))} |"
            )
    lines.append("")

    lines.append("## Path Quality Lab")
    lines.append("")
    if path_lab.empty:
        lines.append("No path-quality rows.")
    else:
        lines.append(
            f"- Rows analysed: {path_summary.get('path_lab_ok_rows', 0)} ok / {path_summary.get('path_lab_rows', 0)} total."
        )
        lines.append(
            f"- Shadow actions: trail-after-0.5R {path_summary.get('path_trail_after_05r_count', 0)}, "
            f"no-progress {path_summary.get('path_time_to_progress_count', 0)}, early adverse {path_summary.get('path_early_adverse_count', 0)}."
        )
        lines.append("")
        lines.append("| src | ticker | side | setup | outcome | final R | best R | worst R | giveback R | t025 m | action |")
        lines.append("|---|---|---|---|---|---:|---:|---:|---:|---:|---|")
        view = path_lab.loc[path_lab.get("path_status", pd.Series(dtype=str)).eq("OK")].copy()
        if not view.empty:
            view["_sort_giveback"] = pd.to_numeric(view.get("giveback_r", 0.0), errors="coerce").fillna(0.0)
            view = view.sort_values(["path_shadow_action", "_sort_giveback"], ascending=[True, False]).head(18)
        for _, row in view.iterrows():
            lines.append(
                f"| {row.get('path_source', '')} | {row.get('ticker', '')} | {row.get('side', '')} | {row.get('setup', '')} | "
                f"{row.get('outcome', '')} | {_fmt_num(row.get('final_r'), 2)} | {_fmt_num(row.get('best_r'), 2)} | "
                f"{_fmt_num(row.get('worst_r'), 2)} | {_fmt_num(row.get('giveback_r'), 2)} | "
                f"{_fmt_num(row.get('time_to_025r_min'), 1)} | {row.get('path_shadow_action', '')} |"
            )
    lines.append("")

    lines.append("## Anti-Chase Shadow Audit")
    lines.append("")
    if anti_chase_audit.empty:
        lines.append("No anti-chase skipped rows available for shadow resolution.")
    else:
        lines.append(
            f"- Audited {paper.get('anti_chase_audited', 0)} skipped rows against 1-minute data from `{DATA_1MIN_DIR}`."
        )
        lines.append(
            f"- Shadow outcome: target {paper.get('anti_chase_shadow_target', 0)}, SL {paper.get('anti_chase_shadow_sl', 0)}, "
            f"open {paper.get('anti_chase_shadow_open', 0)}, net Rs {_fmt_num(paper.get('anti_chase_shadow_net_rs'), 0)}."
        )
        lines.append("")
        lines.append("| ticker | side | setup | skipped | shadow | min | pnl | best R | worst R | fresh |")
        lines.append("|---|---|---|---|---|---:|---:|---:|---:|---:|")
        for _, row in anti_chase_audit.head(15).iterrows():
            lines.append(
                f"| {row.get('ticker', '')} | {row.get('side', '')} | {row.get('setup', '')} | "
                f"{row.get('skip_outcome', '')} | {row.get('anti_chase_shadow_outcome', '')} | "
                f"{_fmt_num(row.get('anti_chase_shadow_minutes'), 1)} | {_fmt_num(row.get('anti_chase_shadow_pnl_rs'), 0)} | "
                f"{_fmt_num(row.get('anti_chase_shadow_best_r'), 2)} | {_fmt_num(row.get('anti_chase_shadow_worst_r'), 2)} | "
                f"{_fmt_num(row.get('freshness_score'), 1)} |"
            )
    lines.append("")

    lines.append("## Latency Breakdown")
    lines.append("")
    lines.append("| component | latest | recent avg |")
    lines.append("|---|---:|---:|")
    lines.append(f"| 5min fetch | {_fmt_num(latency.get('5min_fetch_sec'), 1)}s | {_fmt_num(latency.get('recent_avg_fetch_sec'), 1)}s |")
    lines.append(f"| Tier123 scan | {_fmt_num(latency.get('tier123_scan_sec'), 1)}s |  |")
    lines.append(f"| scanner overhead outside Tier123 | {_fmt_num(latency.get('scanner_overhead_sec'), 1)}s | {_fmt_num(latency.get('recent_avg_scanner_overhead_sec'), 1)}s |")
    lines.append(f"| entry after scanner publish | {_fmt_num(latency.get('entry_after_scan_sec'), 1)}s | {_fmt_num(latency.get('recent_avg_entry_after_scan_sec'), 1)}s |")
    lines.append(f"| entry raw fetch | {_fmt_num(latency.get('entry_raw_fetch_sec'), 3)}s |  |")
    lines.append(f"| bottleneck | {latency.get('bottleneck', '')} | {latency.get('note', '')} |")
    # scanner deadline-slack: how much time is left between scan publish and entry window close
    _pub_delay = _safe_float(signal.get("publish_delay_sec"), np.nan)
    _entry_delay = _safe_float(entry.get("write_delay_sec"), np.nan)
    if np.isfinite(_pub_delay):
        _total_pipeline_sec = _pub_delay + (_entry_delay if np.isfinite(_entry_delay) else 0.0)
        _warn_55 = "⚠ scan > 55s" if _pub_delay > 55 else ""
        _warn_70 = " ⛔ scan > 70s" if _pub_delay > 70 else ""
        lines.append(f"| total pipeline (scan+entry) | {_fmt_num(_total_pipeline_sec, 1)}s | {_warn_55}{_warn_70} |")
    lines.append("")

    lines.append("## Recommendations")
    lines.append("")
    lines.append("| area | severity | finding | suggestion |")
    lines.append("|---|---|---|---|")
    for row in recs:
        lines.append(f"| {row.get('area', '')} | {row.get('severity', '')} | {row.get('finding', '')} | {row.get('suggestion', '')} |")
    lines.append("")

    lines.append("## Out-Of-Box Ideas To Research, Not Deploy Yet")
    lines.append("")
    lines.append("- Time-to-progress gate: keep entry, but if price does not reach +0.20R to +0.30R within 5-8 minutes, tighten stop or exit partial. This targets slow movers without blocking the best instant winners.")
    lines.append("- Freshness score: rank same-slot candidates by pre2_mom_r + pre1_body_r + sig5_close_pos + first-minute spread/volume sanity, then cap marginal names when many candidates arrive together.")
    lines.append("- Two-speed entry: strong momentum names enter immediately; weak-but-valid names wait for one extra 1m confirmation instead of being rejected outright.")
    lines.append("- No-new-filter rule: first shadow-log these ideas and compare against actual winners; only promote if target retention stays high and SL reduction is real.")
    lines.append("")
    return "\n".join(lines) + "\n"


def run_light_ops(day: str) -> tuple[Path, Path]:
    fetch = _latest_fetch_context(day)
    signal = _latest_signal_summary()
    entry = _latest_entry_summary()
    slot_rows = _slot_flow_rows(day)
    entry_rows = _today_entry_rows(day, "entry_rows")
    latency = _latency_audit(fetch, signal, entry, slot_rows)
    slot_pressure = _slot_pressure_shadow(signal, entry)
    pre_summary, pre_reasons, pre_gap_rows = _pre_momentum_health(day)
    flow_rows = _latest_flow_detail(day, entry)
    parquet_cache: dict[str, pd.DataFrame] = {}
    paper_summary, paper_rows, open_df, anti_chase_audit = _paper_trade_analysis(day, entry_rows, cache=parquet_cache)
    concentration_summary, concentration_rows = _setup_concentration_shadow(open_df)
    portfolio_summary = _portfolio_shadow_state(paper_rows, open_df)
    path_summary, path_lab = _path_quality_lab(paper_rows, open_df, cache=parquet_cache)
    deep_summary = {
        "concentration": concentration_summary,
        "slot_pressure": slot_pressure,
        "portfolio": portfolio_summary,
        "path_quality": path_summary,
    }
    recs = _recommendations(
        fetch,
        signal,
        entry,
        pre_summary,
        paper_summary,
        latency,
        concentration_summary,
        slot_pressure,
        portfolio_summary,
        path_summary,
    )

    report_text = _render_markdown(
        day,
        fetch,
        signal,
        entry,
        latency,
        slot_pressure,
        pre_summary,
        pre_reasons,
        pre_gap_rows,
        flow_rows,
        paper_summary,
        concentration_summary,
        concentration_rows,
        portfolio_summary,
        path_summary,
        path_lab,
        paper_rows,
        open_df,
        anti_chase_audit,
        slot_rows,
        recs,
    )
    report_path = OPS_DIR / f"live_ops_snapshot_{day}.md"
    json_path = OPS_DIR / f"live_ops_snapshot_{day}.json"
    slot_csv_path = OPS_DIR / f"live_ops_slot_flow_{day}.csv"
    open_csv_path = OPS_DIR / f"live_ops_open_trade_progress_{day}.csv"
    pre_gap_csv_path = OPS_DIR / f"live_ops_pre_momentum_feature_gaps_{day}.csv"
    flow_detail_csv_path = OPS_DIR / f"live_ops_latest_flow_detail_{day}.csv"
    anti_chase_csv_path = OPS_DIR / f"live_ops_anti_chase_shadow_audit_{day}.csv"
    concentration_csv_path = OPS_DIR / f"live_ops_setup_concentration_shadow_{day}.csv"
    path_lab_csv_path = OPS_DIR / f"live_ops_path_quality_lab_{day}.csv"

    payload = {
        "day": day,
        "generated_at_ist": _fmt_ts(_now_ist()),
        "mode": "light_ops",
        "report": str(report_path),
        "slot_flow_csv": str(slot_csv_path),
        "open_trade_progress_csv": str(open_csv_path),
        "pre_momentum_feature_gaps_csv": str(pre_gap_csv_path),
        "latest_flow_detail_csv": str(flow_detail_csv_path),
        "anti_chase_shadow_audit_csv": str(anti_chase_csv_path),
        "setup_concentration_shadow_csv": str(concentration_csv_path),
        "path_quality_lab_csv": str(path_lab_csv_path),
        "fetch": fetch,
        "signal": signal,
        "entry": entry,
        "latency": latency,
        "deep": deep_summary,
        "latest_flow_detail_rows": int(len(flow_rows)),
        "pre_momentum": pre_summary,
        "paper": paper_summary,
        "recommendations": recs,
    }
    payload_text = json.dumps(payload, indent=2, sort_keys=True, default=str)
    _write_text_retry(report_path, report_text)
    _write_text_retry(json_path, payload_text)
    _write_csv_retry(slot_csv_path, slot_rows)
    _write_csv_retry(open_csv_path, open_df)
    _write_csv_retry(pre_gap_csv_path, pre_gap_rows)
    _write_csv_retry(flow_detail_csv_path, flow_rows)
    _write_csv_retry(anti_chase_csv_path, anti_chase_audit)
    _write_csv_retry(concentration_csv_path, concentration_rows)
    _write_csv_retry(path_lab_csv_path, path_lab)
    _write_text_retry(LATEST_DIR / "latest_live_ops_snapshot.md", report_text)
    _write_text_retry(LATEST_DIR / "latest_live_ops_snapshot.json", payload_text)
    _write_csv_retry(LATEST_DIR / "latest_live_ops_slot_flow.csv", slot_rows)
    _write_csv_retry(LATEST_DIR / "latest_live_ops_open_trade_progress.csv", open_df)
    _write_csv_retry(LATEST_DIR / "latest_live_ops_pre_momentum_feature_gaps.csv", pre_gap_rows)
    _write_csv_retry(LATEST_DIR / "latest_live_ops_latest_flow_detail.csv", flow_rows)
    _write_csv_retry(LATEST_DIR / "latest_live_ops_anti_chase_shadow_audit.csv", anti_chase_audit)
    _write_csv_retry(LATEST_DIR / "latest_live_ops_setup_concentration_shadow.csv", concentration_rows)
    _write_csv_retry(LATEST_DIR / "latest_live_ops_path_quality_lab.csv", path_lab)
    return report_path, json_path
