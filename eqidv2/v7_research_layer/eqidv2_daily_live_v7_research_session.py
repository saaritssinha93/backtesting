"""
Separate Daily Live V7 Research session.

This runner reuses the lightweight V7 ops collector, but keeps its own process
identity, logs, heartbeat/status files, and latest report copies.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import shutil
import sys
import time
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from eqidv2_runtime_paths import RUNTIME_STATUS_DIR, runtime_dir
from v7_research_layer.eqidv2_v7_light_ops import LATEST_DIR as LIGHT_OPS_LATEST_DIR
from v7_research_layer.eqidv2_v7_light_ops import run_light_ops


SESSION_NAME = "Daily Live V7 Research"
SESSION_SLUG = "daily_live_v7_research_session"
SESSION_ROOT = runtime_dir(SESSION_SLUG)
SESSION_LATEST_DIR = SESSION_ROOT / "latest"
SESSION_HEARTBEAT_DIR = SESSION_ROOT / "heartbeat"
SESSION_REPORTS_DIR = SESSION_ROOT / "reports"

for _path in (SESSION_ROOT, SESSION_LATEST_DIR, SESSION_HEARTBEAT_DIR, SESSION_REPORTS_DIR, RUNTIME_STATUS_DIR):
    _path.mkdir(parents=True, exist_ok=True)


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


def _fmt_seconds(value: Any) -> str:
    try:
        x = float(value)
    except Exception:
        return ""
    if not np.isfinite(x):
        return ""
    if abs(x) >= 10:
        return f"{x:.1f}s"
    return f"{x:.3f}s"


def _write_text_atomic(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f"{path.name}.tmp.{os.getpid()}")
    tmp.write_text(text, encoding="utf-8")
    tmp.replace(path)


def _write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    _write_text_atomic(path, json.dumps(payload, indent=2, sort_keys=True, default=str))


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
        "updated_at_ist": _fmt_ts(_now_ist()),
        "mode": "light_ops",
    }
    payload.update(extra)
    text = _kv_text(payload)
    _write_json_atomic(SESSION_HEARTBEAT_DIR / f"{SESSION_SLUG}.status.json", payload)
    _write_json_atomic(SESSION_HEARTBEAT_DIR / f"{SESSION_SLUG}.heartbeat.json", payload)
    _write_text_atomic(RUNTIME_STATUS_DIR / f"{SESSION_SLUG}.status", text)
    _write_text_atomic(RUNTIME_STATUS_DIR / f"{SESSION_SLUG}.heartbeat", text)


def _load_latest_summary() -> dict[str, Any]:
    path = LIGHT_OPS_LATEST_DIR / "latest_live_ops_snapshot.json"
    if not path.exists() or path.stat().st_size <= 2:
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8", errors="replace"))
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


def _copy_latest_outputs(summary: dict[str, Any], report_path: Path, json_path: Path) -> None:
    day = str(summary.get("day") or _now_ist().strftime("%Y-%m-%d"))
    dated_report = SESSION_REPORTS_DIR / f"daily_live_v7_research_{day}.md"
    dated_json = SESSION_REPORTS_DIR / f"daily_live_v7_research_{day}.json"
    latest_report = SESSION_LATEST_DIR / "latest_daily_live_v7_research.md"
    latest_json = SESSION_LATEST_DIR / "latest_daily_live_v7_research.json"

    if report_path.exists():
        shutil.copyfile(report_path, dated_report)
        shutil.copyfile(report_path, latest_report)
    if json_path.exists():
        shutil.copyfile(json_path, dated_json)
        shutil.copyfile(json_path, latest_json)

    for source_name, target_name in (
        ("latest_live_ops_slot_flow.csv", "latest_daily_live_v7_slot_flow.csv"),
        ("latest_live_ops_open_trade_progress.csv", "latest_daily_live_v7_open_trade_progress.csv"),
        ("latest_live_ops_latest_flow_detail.csv", "latest_daily_live_v7_flow_detail.csv"),
        ("latest_live_ops_pre_momentum_feature_gaps.csv", "latest_daily_live_v7_pre_momentum_feature_gaps.csv"),
        ("latest_live_ops_anti_chase_shadow_audit.csv", "latest_daily_live_v7_anti_chase_shadow_audit.csv"),
        ("latest_live_ops_setup_concentration_shadow.csv", "latest_daily_live_v7_setup_concentration_shadow.csv"),
        ("latest_live_ops_path_quality_lab.csv", "latest_daily_live_v7_path_quality_lab.csv"),
    ):
        source = LIGHT_OPS_LATEST_DIR / source_name
        target = SESSION_LATEST_DIR / target_name
        if source.exists():
            shutil.copyfile(source, target)


def _console_lines(summary: dict[str, Any]) -> list[str]:
    fetch = summary.get("fetch", {}) if isinstance(summary.get("fetch"), dict) else {}
    signal = summary.get("signal", {}) if isinstance(summary.get("signal"), dict) else {}
    entry = summary.get("entry", {}) if isinstance(summary.get("entry"), dict) else {}
    pre = summary.get("pre_momentum", {}) if isinstance(summary.get("pre_momentum"), dict) else {}
    paper = summary.get("paper", {}) if isinstance(summary.get("paper"), dict) else {}
    latency = summary.get("latency", {}) if isinstance(summary.get("latency"), dict) else {}
    deep = summary.get("deep", {}) if isinstance(summary.get("deep"), dict) else {}
    concentration = deep.get("concentration", {}) if isinstance(deep.get("concentration"), dict) else {}
    portfolio = deep.get("portfolio", {}) if isinstance(deep.get("portfolio"), dict) else {}
    slot_pressure = deep.get("slot_pressure", {}) if isinstance(deep.get("slot_pressure"), dict) else {}
    path_quality = deep.get("path_quality", {}) if isinstance(deep.get("path_quality"), dict) else {}
    recs = summary.get("recommendations", [])
    if not isinstance(recs, list):
        recs = []

    lines = [
        (
            "[daily_live_v7_research ops] "
            f"5m_fetch={_fmt_seconds(fetch.get('duration_sec'))} "
            f"state={fetch.get('overall_state', '')}/{fetch.get('sla_state', '')} "
            f"written={fetch.get('tickers_written', 0)}/{fetch.get('tickers_expected', 0)}"
        ),
        (
            "[daily_live_v7_research ops] "
            f"scanner_slot={signal.get('slot_ist', '')} "
            f"publish_delay={_fmt_seconds(signal.get('publish_delay_sec'))} "
            f"raw={signal.get('raw_candidates', 0)} "
            f"v11_in={signal.get('v11_input', 0)} "
            f"selected={signal.get('v11_selected', 0)} "
            f"tier123={_fmt_seconds(signal.get('tier123_scan_elapsed_sec'))}/{signal.get('tier123_workers', 0)}w"
        ),
        (
            "[daily_live_v7_research ops] "
            f"entry_slot={entry.get('slot_ist', '')} "
            f"raw_fetch={_fmt_seconds(entry.get('raw_fetch_elapsed_sec'))} "
            f"apps={entry.get('raw_fetch_active_apps', 0)} "
            f"entries={entry.get('entry_rows', 0)} "
            f"pre_pass={entry.get('pre_momentum_output_rows', 0)}/{entry.get('pre_momentum_input_rows', 0)} "
            f"nan_rejects={pre.get('latest_nan_rejects', 0)}"
        ),
        (
            "[daily_live_v7_research ops] "
            f"paper_traded={paper.get('paper_traded_rows', 0)} "
            f"target={paper.get('targets', 0)} "
            f"sl={paper.get('sl', 0)} "
            f"open={paper.get('open_trades', 0)} "
            f"slow_open={paper.get('slow_open_trades', 0)} "
            f"ttp_shadow={paper.get('ttp_shadow_trigger_count', 0)} "
            f"fresh_weak={paper.get('freshness_weak_trades', 0)} "
            f"quick_target_10m={paper.get('quick_targets_10m', 0)} "
            f"quick_sl_15m={paper.get('quick_sl_15m', 0)} "
            f"nse_net={paper.get('v7_nse_id_net_pnl_rs', paper.get('est_net_pnl_rs', 0)):.0f}"
        ),
        (
            "[daily_live_v7_research ops] "
            f"latency_bottleneck={latency.get('bottleneck', '')} "
            f"scanner_overhead={_fmt_seconds(latency.get('scanner_overhead_sec'))} "
            f"entry_after_scan={_fmt_seconds(latency.get('entry_after_scan_sec'))} "
            f"anti_chase_audit={paper.get('anti_chase_audited', 0)} "
            f"anti_shadow_t/sl/o={paper.get('anti_chase_shadow_target', 0)}/{paper.get('anti_chase_shadow_sl', 0)}/{paper.get('anti_chase_shadow_open', 0)}"
        ),
        (
            "[daily_live_v7_research deep] "
            f"concentration={concentration.get('open_concentration_state', '')}:"
            f"{concentration.get('open_dominant_setup', '')}@{concentration.get('open_dominant_setup_pct', 0):.1f}% "
            f"slot_pressure={slot_pressure.get('slot_pressure_state', '')} "
            f"portfolio={portfolio.get('portfolio_shadow_state', '')} "
            f"combined_pnl={portfolio.get('combined_paper_pnl_rs', 0):.0f} "
            f"path_trail={path_quality.get('path_trail_after_05r_count', 0)} "
            f"path_no_progress={path_quality.get('path_time_to_progress_count', 0)}"
        ),
    ]
    for rec in recs[:8]:
        if isinstance(rec, dict):
            lines.append(
                "[daily_live_v7_research suggestion] "
                f"{rec.get('severity', '')} {rec.get('area', '')}: "
                f"{rec.get('finding', '')} -> {rec.get('suggestion', '')}"
            )
    return lines


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


def run_once(day: str, *, run_time_ist: str = "") -> dict[str, Any]:
    _write_status("RUNNING", phase="BUILD_LIGHT_OPS", day=day, run_time_ist=run_time_ist)
    report_path, json_path = run_light_ops(day)
    summary = _load_latest_summary()
    if not summary:
        summary = {"day": day, "report": str(report_path)}
    _copy_latest_outputs(summary, report_path, json_path)
    _write_status(
        "RUNNING",
        phase="LIGHT_OPS_DONE",
        day=day,
        run_time_ist=run_time_ist,
        report=str(SESSION_LATEST_DIR / "latest_daily_live_v7_research.md"),
        json=str(SESSION_LATEST_DIR / "latest_daily_live_v7_research.json"),
        source_report=str(report_path),
        latest_signal_slot=summary.get("signal", {}).get("slot_ist", "") if isinstance(summary.get("signal"), dict) else "",
        latest_entry_slot=summary.get("entry", {}).get("slot_ist", "") if isinstance(summary.get("entry"), dict) else "",
        paper_open_trades=summary.get("paper", {}).get("open_trades", 0) if isinstance(summary.get("paper"), dict) else 0,
        paper_slow_open_trades=summary.get("paper", {}).get("slow_open_trades", 0) if isinstance(summary.get("paper"), dict) else 0,
        ttp_shadow_triggers=summary.get("paper", {}).get("ttp_shadow_trigger_count", 0) if isinstance(summary.get("paper"), dict) else 0,
        anti_chase_audited=summary.get("paper", {}).get("anti_chase_audited", 0) if isinstance(summary.get("paper"), dict) else 0,
        concentration_state=(
            summary.get("deep", {}).get("concentration", {}).get("open_concentration_state", "")
            if isinstance(summary.get("deep"), dict) and isinstance(summary.get("deep", {}).get("concentration"), dict)
            else ""
        ),
        portfolio_shadow_state=(
            summary.get("deep", {}).get("portfolio", {}).get("portfolio_shadow_state", "")
            if isinstance(summary.get("deep"), dict) and isinstance(summary.get("deep", {}).get("portfolio"), dict)
            else ""
        ),
        pre_momentum_latest_nan_rejects=summary.get("pre_momentum", {}).get("latest_nan_rejects", 0) if isinstance(summary.get("pre_momentum"), dict) else 0,
    )
    print(f"[daily_live_v7_research] {run_time_ist or _fmt_ts(_now_ist())} wrote {SESSION_LATEST_DIR / 'latest_daily_live_v7_research.md'}", flush=True)
    for line in _console_lines(summary):
        print(line, flush=True)
    return summary


def run_loop(*, start_time: str, end_time: str, interval_min: int, run_now: bool) -> int:
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
        run_now=bool(run_now),
    )

    if run_now:
        now = _now_ist()
        if start <= now <= end:
            next_due = _next_run_time(_now_ist(), start, end, interval_min)
            due_in_sec = (next_due - now).total_seconds() if next_due is not None else np.inf
            if due_in_sec <= 120:
                print(
                    f"[daily_live_v7_research] startup is close to scheduled slot {_fmt_ts(next_due)}; waiting for aligned run",
                    flush=True,
                )
            else:
                run_key = _fmt_ts(now)
                day = now.strftime("%Y-%m-%d")
                try:
                    run_once(day, run_time_ist=run_key)
                except Exception as exc:
                    _write_status("ERROR", phase="RUN_NOW_FAILED", run_time_ist=run_key, error=f"{type(exc).__name__}: {exc}")
                    print(f"[daily_live_v7_research] ERROR {type(exc).__name__}: {exc}", flush=True)

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
        if wait_sec <= 0 and run_key == last_run_key:
            interval = pd.Timedelta(minutes=max(1, int(interval_min)))
            next_after = next_run + interval
            sleep_sec = max(1.0, min(60.0, (next_after - now).total_seconds()))
            _write_status(
                "RUNNING",
                phase="WAIT",
                next_run_ist=_fmt_ts(next_after),
                wait_sec=round(max(0.0, (next_after - now).total_seconds()), 1),
                start_time=start_time,
                end_time=end_time,
                interval_min=int(interval_min),
            )
            time.sleep(sleep_sec)
            continue
        _write_status(
            "RUNNING",
            phase="WAIT",
            next_run_ist=_fmt_ts(next_run),
            wait_sec=round(wait_sec, 1),
            start_time=start_time,
            end_time=end_time,
            interval_min=int(interval_min),
        )
        if wait_sec > 0:
            time.sleep(min(wait_sec, 60.0))
            continue

        last_run_key = run_key
        try:
            run_once(next_run.strftime("%Y-%m-%d"), run_time_ist=run_key)
        except Exception as exc:
            _write_status("ERROR", phase="LIGHT_OPS_FAILED", run_time_ist=run_key, error=f"{type(exc).__name__}: {exc}")
            print(f"[daily_live_v7_research] ERROR {type(exc).__name__}: {exc}", flush=True)
        time.sleep(1.0)


def _default_day() -> str:
    return _now_ist().strftime("%Y-%m-%d")


def main() -> int:
    ap = argparse.ArgumentParser(description="Run separate Daily Live V7 Research light-ops session")
    ap.add_argument("--date", default=_default_day(), help="Trading date YYYY-MM-DD; default today IST")
    ap.add_argument("--loop", action="store_true", help="Run every interval during the live session")
    ap.add_argument("--run-now", action="store_true", help="Run one snapshot immediately when inside market loop time")
    ap.add_argument("--start-time", default="09:17:30", help="Loop start time HH:MM:SS IST")
    ap.add_argument("--end-time", default="16:00:00", help="Loop end time HH:MM:SS IST")
    ap.add_argument("--interval-min", type=int, default=15, help="Loop interval in minutes")
    args = ap.parse_args()

    if args.loop:
        return run_loop(
            start_time=str(args.start_time),
            end_time=str(args.end_time),
            interval_min=int(args.interval_min),
            run_now=bool(args.run_now),
        )

    run_once(str(args.date), run_time_ist=_fmt_ts(_now_ist()))
    _write_status("STOPPED", phase="ONE_SHOT_DONE", day=str(args.date))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
