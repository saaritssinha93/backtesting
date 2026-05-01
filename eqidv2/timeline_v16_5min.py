"""V16 5min signal-lifecycle timeline reconstructor.

Joins these per-day artifacts on signal_id (or synthetic key for PRE_POOL drops):
  - pool_lifecycle_<date>_v16_5min.jsonl  (events: WRITTEN, PF_VERIFIED, DE_PASSED, DROPPED)
  - pending_signals_<date>_v16_5min.csv   (final pool state per signal)
  - detected_signals_<date>_v16_5min.csv  (DE-passed signals with lag stats)
  - signals_<date>_v16_5min_long.csv      (dispatched LONG signals)
  - signals_<date>_v16_5min_short.csv     (dispatched SHORT signals)
  - live_trades_<date>_v16_5min.csv       (executor outcomes, real broker)
  - paper_trades_<date>_v16_5min.csv      (executor outcomes, paper)
  - late_skipped_<date>_v16_5min.csv      (signals dropped by executor lag gate)

Emits to <out_dir>:
  timeline_wide_<date>.csv   one row per signal — every stage timestamp side-by-side
  timeline_events_<date>.csv one row per event sorted by ts — chronological scrub
  summary_<date>.txt         counts at each funnel stage + per-stage latency stats

Usage:
  python timeline_v16_5min.py --date 2026-04-27
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Optional

import pandas as pd

LIVE_DIR = Path("C:/TradingData/eqidv2/live_signals")
SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_REPORTS_DIR = SCRIPT_DIR / "reports"


def _read_jsonl(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    rows = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return pd.DataFrame(rows)


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except pd.errors.EmptyDataError:
        return pd.DataFrame()


def _to_ts(s) -> Optional[pd.Timestamp]:
    if s is None or (isinstance(s, float) and pd.isna(s)):
        return None
    if isinstance(s, str) and not s.strip():
        return None
    try:
        ts = pd.to_datetime(s, errors="coerce", utc=False)
        if pd.isna(ts):
            return None
        if ts.tz is None:
            ts = ts.tz_localize("Asia/Kolkata")
        else:
            ts = ts.tz_convert("Asia/Kolkata")
        return ts
    except Exception:
        return None


def _fmt_ts(ts) -> str:
    if ts is None or (isinstance(ts, float) and pd.isna(ts)):
        return ""
    if pd.isna(ts):
        return ""
    return pd.Timestamp(ts).strftime("%H:%M:%S")


def _lag_seconds(later, earlier) -> Optional[float]:
    if later is None or earlier is None:
        return None
    if pd.isna(later) or pd.isna(earlier):
        return None
    return float((pd.Timestamp(later) - pd.Timestamp(earlier)).total_seconds())


def build_wide(
    pool_lifecycle: pd.DataFrame,
    pending: pd.DataFrame,
    detected: pd.DataFrame,
    dispatched: pd.DataFrame,
    live_trades: pd.DataFrame,
    paper_trades: pd.DataFrame,
    late_skipped: pd.DataFrame,
) -> pd.DataFrame:
    rows = []

    # 1) Pool-bound signals (have a signal_id)
    by_sid: dict[str, dict] = {}

    if not pending.empty:
        for _, r in pending.iterrows():
            sid = str(r.get("signal_id", "") or "")
            if not sid:
                continue
            by_sid[sid] = {
                "signal_id": sid,
                "ticker": r.get("ticker", ""),
                "side": r.get("side", ""),
                "setup": r.get("setup", ""),
                "signal_bar_close": _to_ts(r.get("signal_datetime", "")),
                "entry_slot": _to_ts(r.get("signal_entry_datetime_ist", "")),
                "pool_written_ts": _to_ts(r.get("added_at", "")),
                "pool_status": r.get("status", ""),
                "filter_reason": r.get("filter_reason", ""),
                "quality_score": r.get("quality_score", ""),
            }

    # 2) Pool lifecycle augments per-signal stamps (PF_VERIFIED, DE_PASSED, DROPPED with sid)
    if not pool_lifecycle.empty:
        for _, ev in pool_lifecycle.iterrows():
            sid = str(ev.get("signal_id", "") or "")
            if not sid:
                continue  # pre-pool DROPs handled later
            event = ev.get("event", "")
            ts = _to_ts(ev.get("ts", ""))
            entry = by_sid.setdefault(sid, {"signal_id": sid, "ticker": ev.get("ticker", ""),
                                            "side": ev.get("side", ""), "setup": ev.get("setup", "")})
            if event == "PF_VERIFIED":
                entry.setdefault("pf_verified_ts", ts)
            elif event == "DE_PASSED":
                entry.setdefault("de_passed_ts", ts)
            elif event == "WRITTEN":
                entry.setdefault("pool_written_ts", ts)
            elif event == "DROPPED":
                entry.setdefault("dropped_event_ts", ts)
                entry.setdefault("dropped_event_reason", ev.get("reason", ""))

    # 3) Detected signals (DE_PASSED with explicit lag stats)
    if not detected.empty:
        for _, r in detected.iterrows():
            sid = str(r.get("signal_id", "") or "")
            if not sid:
                continue
            entry = by_sid.setdefault(sid, {"signal_id": sid, "ticker": r.get("ticker", ""),
                                            "side": r.get("side", ""), "setup": r.get("setup", "")})
            entry.setdefault("de_passed_ts", _to_ts(r.get("detected_time", "")))
            entry.setdefault("de_lag_signal_bar_sec", r.get("lag_from_signal_bar_sec", ""))
            entry.setdefault("de_lag_entry_slot_sec", r.get("lag_from_entry_slot_sec", ""))

    # 4) Dispatched signals (signals_*.csv combined)
    if not dispatched.empty:
        for _, r in dispatched.iterrows():
            sid = str(r.get("signal_id", "") or "")
            if not sid:
                continue
            entry = by_sid.setdefault(sid, {"signal_id": sid, "ticker": r.get("ticker", ""),
                                            "side": r.get("side", ""), "setup": r.get("setup", "")})
            entry.setdefault("dispatched_ts", _to_ts(r.get("logtime_ist", r.get("received_time", ""))))
            entry.setdefault("stage2_detected_ts", _to_ts(r.get("stage2_detected_at_ist", "")))
            entry.setdefault("entry_price_signal", r.get("entry_price", ""))
            entry.setdefault("stop_price_signal", r.get("stop_price", ""))
            entry.setdefault("target_price_signal", r.get("target_price", ""))

    # 5) Trade execution outcomes (live + paper, tagged)
    def _merge_trades(trades_df: pd.DataFrame, mode: str) -> None:
        if trades_df.empty:
            return
        for _, r in trades_df.iterrows():
            sid = str(r.get("signal_id", "") or "")
            if not sid:
                continue
            entry = by_sid.setdefault(sid, {"signal_id": sid, "ticker": r.get("ticker", ""),
                                            "side": r.get("side", ""), "setup": r.get("setup", "")})
            prefix = "" if mode == "live" else "paper_"
            if mode == "live":
                entry["entry_filled_ts"] = _to_ts(r.get("entry_time", ""))
                entry["exit_ts"] = _to_ts(r.get("exit_time", ""))
                entry["outcome"] = r.get("outcome", "")
                entry["pnl_rs"] = r.get("pnl_rs", "")
                entry["pnl_pct"] = r.get("pnl_pct", "")
                entry["filled_price"] = r.get("filled_price", "")
                entry["exit_price"] = r.get("exit_price", "")
                entry["entry_order_id"] = r.get("entry_order_id", "")
                entry["target_order_id"] = r.get("target_order_id", "")
                entry["sl_order_id"] = r.get("sl_order_id", "")
            else:
                entry["paper_entry_filled_ts"] = _to_ts(r.get("entry_time", ""))
                entry["paper_exit_ts"] = _to_ts(r.get("exit_time", ""))
                entry["paper_outcome"] = r.get("outcome", "")
                entry["paper_pnl_rs"] = r.get("pnl_rs", "")
                entry["paper_pnl_pct"] = r.get("pnl_pct", "")

    _merge_trades(live_trades, "live")
    _merge_trades(paper_trades, "paper")

    # 6) Late-skipped overlay (executor lag-gate rejection)
    if not late_skipped.empty:
        for _, r in late_skipped.iterrows():
            sid = str(r.get("signal_id", "") or "")
            if not sid:
                continue
            entry = by_sid.setdefault(sid, {"signal_id": sid, "ticker": r.get("ticker", ""),
                                            "side": r.get("side", ""), "setup": r.get("setup", "")})
            entry["late_skipped_ts"] = _to_ts(r.get("skipped_at_ist", ""))
            entry["late_skip_lag_sec"] = r.get("lag_sec", "")
            entry["late_skip_threshold_sec"] = r.get("threshold_sec", "")

    # 7) Pre-pool DROPs (no signal_id)
    if not pool_lifecycle.empty:
        prep = pool_lifecycle[pool_lifecycle["event"] == "DROPPED"].copy() if "event" in pool_lifecycle.columns else pd.DataFrame()
        if not prep.empty and "signal_id" in prep.columns:
            prep = prep[prep["signal_id"].isna() | (prep["signal_id"].astype(str).str.strip() == "")]
        for _, ev in prep.iterrows() if not prep.empty else []:
            synth_sid = f"PREPOOL::{ev.get('ticker', '')}::{ev.get('side', '')}::{ev.get('signal_time_ist', '')}::{ev.get('setup', '')}"
            rows.append({
                "signal_id": "",  # no real id
                "synth_id": synth_sid,
                "ticker": ev.get("ticker", ""),
                "side": ev.get("side", ""),
                "setup": ev.get("setup", ""),
                "signal_bar_close": _to_ts(ev.get("signal_time_ist", "")),
                "entry_slot": "",
                "pool_written_ts": "",
                "pf_verified_ts": "",
                "de_passed_ts": "",
                "dispatched_ts": "",
                "entry_filled_ts": "",
                "exit_ts": "",
                "outcome": "PREPOOL_DROP",
                "filter_reason": ev.get("reason", ""),
                "dropped_event_ts": _to_ts(ev.get("ts", "")),
                "dropped_event_reason": ev.get("reason", ""),
                "pnl_rs": "",
                "pnl_pct": "",
            })

    # 8) Pool-bound signals → final rows + per-stage lag computation + dropped_at classification
    for sid, e in by_sid.items():
        ts_bar = e.get("signal_bar_close")
        ts_pool = e.get("pool_written_ts")
        ts_pf = e.get("pf_verified_ts")
        ts_de = e.get("de_passed_ts")
        ts_disp = e.get("dispatched_ts")
        ts_fill = e.get("entry_filled_ts")
        ts_exit = e.get("exit_ts")
        ts_late = e.get("late_skipped_ts")

        # Determine where the funnel terminated
        if ts_exit:
            dropped_at = ""
        elif ts_fill:
            dropped_at = "AFTER_ENTRY_NO_EXIT"
        elif ts_late:
            dropped_at = "EXECUTOR_LATE_GATE"
        elif ts_disp:
            dropped_at = "EXECUTOR_NO_FILL"
        elif ts_de:
            dropped_at = "POST_DE_NO_DISPATCH"
        elif ts_pf:
            dropped_at = "POST_PF_NO_DE"
        elif ts_pool:
            dropped_at = "POST_POOL_NO_PF"
        else:
            dropped_at = "UNKNOWN"

        e["dropped_at"] = dropped_at
        e["lag_bar_to_pool_sec"]    = _lag_seconds(ts_pool, ts_bar)
        e["lag_pool_to_pf_sec"]     = _lag_seconds(ts_pf,   ts_pool)
        e["lag_pf_to_de_sec"]       = _lag_seconds(ts_de,   ts_pf)
        e["lag_de_to_dispatch_sec"] = _lag_seconds(ts_disp, ts_de)
        e["lag_dispatch_to_fill_sec"] = _lag_seconds(ts_fill, ts_disp)
        e["lag_fill_to_exit_sec"]     = _lag_seconds(ts_exit, ts_fill)
        rows.append(e)

    df = pd.DataFrame(rows)

    # Stable column order
    preferred = [
        "signal_id", "synth_id", "ticker", "side", "setup",
        "signal_bar_close", "entry_slot",
        "pool_written_ts", "pf_verified_ts", "de_passed_ts",
        "dispatched_ts", "stage2_detected_ts",
        "entry_filled_ts", "exit_ts", "outcome",
        "filled_price", "exit_price", "pnl_rs", "pnl_pct",
        "paper_entry_filled_ts", "paper_exit_ts", "paper_outcome", "paper_pnl_rs", "paper_pnl_pct",
        "late_skipped_ts", "late_skip_lag_sec", "late_skip_threshold_sec",
        "dropped_at", "filter_reason", "dropped_event_ts", "dropped_event_reason",
        "lag_bar_to_pool_sec", "lag_pool_to_pf_sec", "lag_pf_to_de_sec",
        "lag_de_to_dispatch_sec", "lag_dispatch_to_fill_sec", "lag_fill_to_exit_sec",
        "de_lag_signal_bar_sec", "de_lag_entry_slot_sec",
        "quality_score", "pool_status",
        "entry_price_signal", "stop_price_signal", "target_price_signal",
        "entry_order_id", "target_order_id", "sl_order_id",
    ]
    for col in preferred:
        if col not in df.columns:
            df[col] = ""
    other = [c for c in df.columns if c not in preferred]
    df = df[preferred + other]

    # Stringify timestamps for CSV stability
    ts_cols = [c for c in df.columns if c.endswith("_ts") or c in ("signal_bar_close", "entry_slot", "dropped_event_ts")]
    for c in ts_cols:
        df[c] = df[c].apply(lambda v: pd.Timestamp(v).strftime("%Y-%m-%d %H:%M:%S%z") if (v not in ("", None) and not pd.isna(v)) else "")

    # Sort by ticker + signal_bar_close + signal_id
    df = df.sort_values(["signal_bar_close", "ticker", "signal_id"], na_position="last").reset_index(drop=True)
    return df


def build_events(
    pool_lifecycle: pd.DataFrame,
    wide: pd.DataFrame,
) -> pd.DataFrame:
    events = []

    # Pool lifecycle events first (ground truth for pool stage timestamps)
    if not pool_lifecycle.empty:
        for _, ev in pool_lifecycle.iterrows():
            event = ev.get("event", "")
            if event == "NF_OPEN_NEUTRAL":
                continue  # noisy boot heartbeat
            ts = _to_ts(ev.get("ts", ""))
            events.append({
                "ts": ts,
                "stage": event,
                "ticker": ev.get("ticker", "") or "",
                "side": ev.get("side", "") or "",
                "setup": ev.get("setup", "") or "",
                "signal_id": str(ev.get("signal_id", "") or ""),
                "detail": ev.get("reason", "") or ev.get("marker", "") or ev.get("source_slot", "") or "",
            })

    # Per-signal stage timestamps from wide table
    if not wide.empty:
        for _, r in wide.iterrows():
            sid = r.get("signal_id", "") or ""
            ticker = r.get("ticker", "") or ""
            side = r.get("side", "") or ""
            setup = r.get("setup", "") or ""

            for col, stage, detail_col in [
                ("dispatched_ts",          "DISPATCHED",     ""),
                ("entry_filled_ts",        "ENTRY_FILLED",   "filled_price"),
                ("exit_ts",                "EXIT",           "outcome"),
                ("late_skipped_ts",        "LATE_SKIPPED",   "late_skip_lag_sec"),
                ("paper_entry_filled_ts",  "PAPER_ENTRY",    ""),
                ("paper_exit_ts",          "PAPER_EXIT",     "paper_outcome"),
            ]:
                v = r.get(col, "")
                if not v:
                    continue
                detail = ""
                if detail_col and r.get(detail_col, "") not in ("", None):
                    detail = f"{detail_col}={r.get(detail_col)}"
                events.append({
                    "ts": pd.to_datetime(v, errors="coerce") if isinstance(v, str) else v,
                    "stage": stage,
                    "ticker": ticker,
                    "side": side,
                    "setup": setup,
                    "signal_id": sid,
                    "detail": detail,
                })

    df = pd.DataFrame(events)
    if df.empty:
        return df

    df = df.dropna(subset=["ts"]).copy()
    df["ts"] = df["ts"].apply(lambda v: pd.Timestamp(v).tz_convert("Asia/Kolkata") if pd.Timestamp(v).tz is not None else pd.Timestamp(v).tz_localize("Asia/Kolkata"))
    df = df.sort_values("ts").reset_index(drop=True)
    df["ts"] = df["ts"].apply(lambda v: v.strftime("%Y-%m-%d %H:%M:%S%z"))
    return df[["ts", "stage", "ticker", "side", "setup", "signal_id", "detail"]]


def write_summary(wide: pd.DataFrame, events: pd.DataFrame, out_path: Path) -> str:
    lines: list[str] = []

    def w(line: str = "") -> None:
        lines.append(line)

    w("V16 5MIN TIMELINE SUMMARY")
    w("=" * 60)
    w()

    if wide.empty:
        w("(no rows in wide table)")
    else:
        is_real = wide["signal_id"].astype(str).str.len() > 0
        real = wide[is_real].copy()
        prepool = wide[~is_real].copy()

        w(f"Pool-bound signals (have signal_id): {len(real)}")
        w(f"Pre-pool DROPs (no signal_id):       {len(prepool)}")
        w()

        # Funnel by stage
        def _has(col): return real[col].astype(str).str.len() > 0
        stages = [
            ("WRITTEN to pool",    _has("pool_written_ts")),
            ("PF_VERIFIED",        _has("pf_verified_ts")),
            ("DE_PASSED",          _has("de_passed_ts")),
            ("DISPATCHED",         _has("dispatched_ts")),
            ("ENTRY_FILLED (live)", _has("entry_filled_ts")),
            ("EXIT (live)",        _has("exit_ts")),
            ("LATE_SKIPPED",       _has("late_skipped_ts")),
            ("PAPER_ENTRY_FILLED", _has("paper_entry_filled_ts")),
            ("PAPER_EXIT",         _has("paper_exit_ts")),
        ]
        w("Funnel:")
        for name, mask in stages:
            w(f"  {name:<22} {int(mask.sum())}")
        w()

        # Where did each signal terminate
        w("Termination by dropped_at:")
        if "dropped_at" in real.columns:
            for term, n in real["dropped_at"].fillna("").value_counts().items():
                w(f"  {term or '(reached EXIT)':<22} {n}")
        w()

        # Outcomes for filled live trades
        if "outcome" in real.columns:
            outcomes = real[real["outcome"].astype(str).str.len() > 0]["outcome"].value_counts()
            if len(outcomes):
                w("Live outcomes:")
                for o, n in outcomes.items():
                    w(f"  {o:<22} {n}")
                w()

        # Latency stats (per-stage)
        lag_cols = [c for c in real.columns if c.startswith("lag_") and c.endswith("_sec")]
        if lag_cols:
            w("Per-stage latency (seconds, when both endpoints present):")
            stats = []
            for c in lag_cols:
                s = pd.to_numeric(real[c], errors="coerce").dropna()
                if not len(s):
                    continue
                stats.append((c, len(s), s.median(), s.mean(), s.quantile(0.95), s.max()))
            if stats:
                w(f"  {'stage':<32}{'n':>5}{'p50':>8}{'mean':>9}{'p95':>9}{'max':>9}")
                for c, n, p50, mean, p95, mx in stats:
                    w(f"  {c:<32}{n:>5}{p50:>8.1f}{mean:>9.1f}{p95:>9.1f}{mx:>9.1f}")
            w()

        # Top drop reasons
        if "filter_reason" in real.columns:
            reasons = real[real["filter_reason"].astype(str).str.len() > 0]["filter_reason"].value_counts().head(10)
            if len(reasons):
                w("Top pool filter_reason (real signals):")
                for r, n in reasons.items():
                    w(f"  {n:>3} | {r}")
                w()
        if not prepool.empty and "filter_reason" in prepool.columns:
            preasons = prepool["filter_reason"].fillna("").value_counts().head(10)
            if len(preasons):
                w("Top pre-pool DROP reasons:")
                for r, n in preasons.items():
                    w(f"  {n:>3} | {r}")
                w()

    if not events.empty:
        w(f"Total events: {len(events)}")
        w(f"Time range:   {events['ts'].iloc[0]} -> {events['ts'].iloc[-1]}")
        w("Events by stage:")
        for s, n in events["stage"].value_counts().items():
            w(f"  {s:<22} {n}")

    body = "\n".join(lines)
    out_path.write_text(body, encoding="utf-8")
    return body


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--date", required=True, help="IST date (YYYY-MM-DD), e.g. 2026-04-27")
    ap.add_argument("--live-dir", default=str(LIVE_DIR), help=f"live_signals dir (default: {LIVE_DIR})")
    ap.add_argument("--out", default="", help="output dir (default: reports/timeline_v16_5min_<date>)")
    args = ap.parse_args()

    date = args.date
    live_dir = Path(args.live_dir)
    out_dir = Path(args.out) if args.out else (DEFAULT_REPORTS_DIR / f"timeline_v16_5min_{date}")
    out_dir.mkdir(parents=True, exist_ok=True)

    print(f"[INFO] live_dir = {live_dir}")
    print(f"[INFO] out_dir  = {out_dir}")

    pool_lifecycle = _read_jsonl(live_dir / f"pool_lifecycle_{date}_v16_5min.jsonl")
    pending        = _read_csv (live_dir / f"pending_signals_{date}_v16_5min.csv")
    detected       = _read_csv (live_dir / f"detected_signals_{date}_v16_5min.csv")
    sig_long       = _read_csv (live_dir / f"signals_{date}_v16_5min_long.csv")
    sig_short      = _read_csv (live_dir / f"signals_{date}_v16_5min_short.csv")
    live_trades    = _read_csv (live_dir / f"live_trades_{date}_v16_5min.csv")
    paper_trades   = _read_csv (live_dir / f"paper_trades_{date}_v16_5min.csv")
    late_skipped   = _read_csv (live_dir / f"late_skipped_{date}_v16_5min.csv")

    dispatched = pd.concat([sig_long, sig_short], ignore_index=True) if (not sig_long.empty or not sig_short.empty) else pd.DataFrame()

    print(f"[INFO] sources: lifecycle={len(pool_lifecycle)} pending={len(pending)} detected={len(detected)} "
          f"dispatched={len(dispatched)} live_trades={len(live_trades)} paper_trades={len(paper_trades)} "
          f"late_skipped={len(late_skipped)}")

    wide = build_wide(pool_lifecycle, pending, detected, dispatched, live_trades, paper_trades, late_skipped)
    events = build_events(pool_lifecycle, wide)

    wide_path = out_dir / f"timeline_wide_{date}.csv"
    events_path = out_dir / f"timeline_events_{date}.csv"
    summary_path = out_dir / f"summary_{date}.txt"

    wide.to_csv(wide_path, index=False, encoding="utf-8")
    events.to_csv(events_path, index=False, encoding="utf-8")
    body = write_summary(wide, events, summary_path)

    print(f"[INFO] wrote {wide_path} ({len(wide)} rows)")
    print(f"[INFO] wrote {events_path} ({len(events)} rows)")
    print(f"[INFO] wrote {summary_path}")
    print()
    print(body)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
