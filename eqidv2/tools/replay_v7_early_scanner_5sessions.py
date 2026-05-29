from __future__ import annotations

import argparse
import json
import math
import os
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Iterable, Optional

import numpy as np
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import avwap_5min_ID_v6_backtesting as v6
import avwap_5min_ID_v7_candidate_scan as scanner
import eqidv2_signal_discovery_v7_5min_id_persistent as discovery
import v17D_exit_resolver as exit_resolver


IST_TZ = "Asia/Kolkata"
TRUTH_DIR = Path(r"C:\TradingData\eqidv2\live_research_v7_research_layer\truth_table")
DATA_1MIN_DIR = Path(r"C:\TradingData\eqidv2\stocks_indicators_1min_eq")
OUT_ROOT = Path(r"C:\TradingData\eqidv2\scanner_replay_v7_early_5sessions")
DEFAULT_SESSIONS = 5
DEFAULT_START = "09:30"
DEFAULT_END = "11:00"

_WORKER_MARKET_CTX: Optional[dict[str, dict[str, Any]]] = None
_WORKER_DAY: Optional[str] = None
_WORKER_SLOTS: list[pd.Timestamp] = []


def _ensure_ist_ts(value: Any) -> pd.Timestamp:
    ts = pd.Timestamp(value)
    if ts.tz is None:
        return ts.tz_localize(IST_TZ)
    return ts.tz_convert(IST_TZ)


def _fmt_ist(value: Any) -> str:
    ts = _ensure_ist_ts(value)
    offset = ts.strftime("%z")
    return f"{ts.strftime('%Y-%m-%d %H:%M:%S')}{offset[:3]}:{offset[3:]}"


def _safe_float(value: Any, default: float = np.nan) -> float:
    try:
        out = float(value)
    except Exception:
        return default
    return out if np.isfinite(out) else default


def _discover_sessions(n: int) -> list[str]:
    if not TRUTH_DIR.exists():
        return []
    days: list[str] = []
    for path in sorted(TRUTH_DIR.glob("truth_table_*.csv")):
        day = path.stem.replace("truth_table_", "")[:10]
        if path.stat().st_size > 2:
            days.append(day)
    return days[-max(1, int(n)) :]


def _parse_sessions(text: str | None, n: int) -> list[str]:
    if text:
        days = [x.strip()[:10] for x in text.split(",") if x.strip()]
        return sorted(dict.fromkeys(days))
    return _discover_sessions(n)


def _session_slots(day: str, start: str, end: str) -> list[pd.Timestamp]:
    start_ts = _ensure_ist_ts(f"{day} {start}:00")
    end_ts = _ensure_ist_ts(f"{day} {end}:00")
    return list(pd.date_range(start_ts, end_ts, freq="5min"))


def _load_universe(limit: Optional[int] = None) -> list[str]:
    tickers = discovery._load_universe()
    tickers = sorted({str(t).strip().upper() for t in tickers if str(t).strip()})
    if limit and limit > 0:
        return tickers[: int(limit)]
    return tickers


def _worker_init(day: str, slot_isos: list[str]) -> None:
    global _WORKER_DAY, _WORKER_SLOTS, _WORKER_MARKET_CTX
    _WORKER_DAY = str(day)[:10]
    _WORKER_SLOTS = [_ensure_ist_ts(x).floor("min") for x in slot_isos]
    scanner.v2.DATA_ROOT_5M = scanner.LIVE_5M_DIR
    scanner.v2._init_worker(
        {
            "ENABLE_NOISY_ADVANCED_SHORTS": True,
            "ENABLE_NATIVE_V2_MINED_FILTER": False,
        }
    )
    try:
        _WORKER_MARKET_CTX = scanner.v2._load_market_context()
    except Exception:
        _WORKER_MARKET_CTX = {}


def _normalise_5m_dates(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["date"] = pd.to_datetime(out["date"], errors="coerce")
    out = out.dropna(subset=["date"])
    if out.empty:
        return out
    if getattr(out["date"].dt, "tz", None) is None:
        out["date"] = out["date"].dt.tz_localize(IST_TZ)
    else:
        out["date"] = out["date"].dt.tz_convert(IST_TZ)
    out["date_only"] = out["date"].dt.date
    return out.sort_values("date").drop_duplicates(subset=["date"], keep="last").reset_index(drop=True)


def _scan_ticker_day_early(ticker: str) -> list[dict[str, Any]]:
    global _WORKER_DAY, _WORKER_SLOTS, _WORKER_MARKET_CTX
    if not _WORKER_DAY or not _WORKER_SLOTS:
        return []
    ticker = str(ticker).upper().strip()
    df = scanner._load_live_5m(ticker)
    if df is None or df.empty:
        return []
    try:
        df = _normalise_5m_dates(df)
        if df.empty:
            return []
        max_slot = max(_WORKER_SLOTS)
        # Keep prior sessions for Prev_Day_Close, but never scan after the slot window.
        df = df.loc[df["date"] <= max_slot].copy()
        if df.empty:
            return []
        prepared = scanner.v2._prepare_5m(df)
    except Exception:
        return []

    day_date = pd.Timestamp(_WORKER_DAY).date()
    if "date_only" not in prepared.columns:
        prepared["date_only"] = pd.to_datetime(prepared["date"], errors="coerce").dt.date
    day_df = prepared.loc[prepared["date_only"] == day_date].copy().reset_index(drop=True)
    if day_df.empty:
        return []
    day_df["date"] = pd.to_datetime(day_df["date"], errors="coerce")
    if getattr(day_df["date"].dt, "tz", None) is None:
        day_df["date"] = day_df["date"].dt.tz_localize(IST_TZ)
    else:
        day_df["date"] = day_df["date"].dt.tz_convert(IST_TZ)

    rows: list[dict[str, Any]] = []
    for slot in _WORKER_SLOTS:
        signal_rows = day_df.loc[day_df["date"].dt.floor("min").eq(slot)]
        if signal_rows.empty:
            continue
        scan_df = day_df.loc[day_df["date"] <= slot].copy().reset_index(drop=True)
        if scan_df.empty:
            continue
        scan_df = scanner._append_synthetic_successor(scan_df, slot)
        try:
            found = scanner._scan_early_slot_candidates(scan_df, ticker, slot, _WORKER_MARKET_CTX or {})
        except Exception:
            found = []
        if not found:
            continue
        signal_row = signal_rows.iloc[-1].to_dict()
        frame = scanner.candidates_to_dataframe([(c, signal_row) for c in found], slot)
        if not frame.empty:
            rows.extend(frame.to_dict("records"))
    return rows


def _rank_gate_and_dedupe(raw: pd.DataFrame, sessions: Iterable[str]) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    raw_frames: list[pd.DataFrame] = []
    gated_frames: list[pd.DataFrame] = []
    live_like_rows: list[dict[str, Any]] = []
    audit_rows: list[dict[str, Any]] = []

    raw = raw.copy() if raw is not None else pd.DataFrame()
    if raw.empty:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    raw["signal_time_sort"] = pd.to_datetime(raw["signal_time_ist"], errors="coerce")
    raw = raw.sort_values(["signal_time_sort", "quality_score", "ticker"], ascending=[True, False, True]).reset_index(drop=True)

    for day in sessions:
        day_raw = raw.loc[raw["signal_time_ist"].astype(str).str.startswith(day)].copy()
        day_ids: set[str] = set()
        day_tickers: set[str] = set()
        if day_raw.empty:
            continue
        for slot_text, slot_df in day_raw.groupby("scan_slot_ist", sort=True):
            ranked = discovery.add_live_ranker_scores(slot_df.drop(columns=["signal_time_sort"], errors="ignore"), day)
            gated, gate_stats = discovery.apply_v8_live_gate(ranked)
            gated, research_rejected, research_stats = discovery.apply_research_live_filters(gated, day)
            if not ranked.empty:
                raw_frames.append(ranked)
            if not gated.empty:
                gated_frames.append(gated)

            written = 0
            duplicates = 0
            if not gated.empty:
                sort_cols = [c for c in ["quality_score", "ticker", "setup"] if c in gated.columns]
                gated_iter = gated.sort_values(sort_cols, ascending=[False, True, True][: len(sort_cols)])
                for _, row in gated_iter.iterrows():
                    cid = str(row.get("candidate_id", ""))
                    ticker = str(row.get("ticker", "")).upper().strip()
                    if not cid or not ticker or cid in day_ids or ticker in day_tickers:
                        duplicates += 1
                        continue
                    item = row.to_dict()
                    item["live_like_daily_status"] = "WRITTEN"
                    live_like_rows.append(item)
                    day_ids.add(cid)
                    day_tickers.add(ticker)
                    written += 1

            audit_rows.append(
                {
                    "day": day,
                    "scan_slot_ist": slot_text,
                    "raw_candidates": int(len(slot_df)),
                    "gated_candidates": int(len(gated)),
                    "live_like_written": int(written),
                    "live_like_duplicates": int(duplicates),
                    "research_rejected": int(len(research_rejected)),
                    **gate_stats,
                    **research_stats,
                }
            )

    raw_ranked = pd.concat(raw_frames, ignore_index=True, sort=False) if raw_frames else pd.DataFrame()
    gated_pre_dedupe = pd.concat(gated_frames, ignore_index=True, sort=False) if gated_frames else pd.DataFrame()
    live_like = pd.DataFrame(live_like_rows)
    audit = pd.DataFrame(audit_rows)
    return raw_ranked, gated_pre_dedupe, live_like, audit


def _load_1min(ticker: str, cache: dict[str, pd.DataFrame]) -> Optional[pd.DataFrame]:
    ticker = str(ticker).upper().strip()
    if ticker in cache:
        return cache[ticker]
    path = DATA_1MIN_DIR / f"{ticker}_stocks_indicators_1min.parquet"
    if not path.exists():
        cache[ticker] = pd.DataFrame()
        return None
    try:
        df = pd.read_parquet(path, columns=["date", "open", "high", "low", "close", "volume"])
    except Exception:
        try:
            df = pd.read_parquet(path)
        except Exception:
            cache[ticker] = pd.DataFrame()
            return None
    if df.empty or "date" not in df.columns:
        cache[ticker] = pd.DataFrame()
        return None
    keep = [c for c in ["date", "open", "high", "low", "close", "volume"] if c in df.columns]
    df = df[keep].copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"])
    if df.empty:
        cache[ticker] = pd.DataFrame()
        return None
    if getattr(df["date"].dt, "tz", None) is None:
        df["date"] = df["date"].dt.tz_localize(IST_TZ)
    else:
        df["date"] = df["date"].dt.tz_convert(IST_TZ)
    for col in ("open", "high", "low", "close", "volume"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.sort_values("date").drop_duplicates(subset=["date"], keep="last").reset_index(drop=True)
    cache[ticker] = df
    return df


def _entry_bar(df: pd.DataFrame, signal_time: Any) -> Optional[pd.Series]:
    if df is None or df.empty:
        return None
    sig = _ensure_ist_ts(signal_time)
    sub = df.loc[(df["date"] >= sig) & (df["date"] <= sig + pd.Timedelta(minutes=5))].sort_values("date")
    if sub.empty:
        return None
    return sub.iloc[0]


def _net_pnl_rs(price_pnl_pct: float, outcome: str, cost_bps: float) -> tuple[float, float, float]:
    extra_bps = v6.STOP_EXTRA_BPS if str(outcome).upper() == "SL" else 0.0
    gross = float(price_pnl_pct) / 100.0 * v6.EFFECTIVE_NOTIONAL
    cost = v6.EFFECTIVE_NOTIONAL * ((float(cost_bps) + extra_bps) / 10_000.0)
    return gross - cost, gross, cost


def _resolve_entries_and_exits(candidates: pd.DataFrame, cost_bps: float) -> tuple[pd.DataFrame, pd.DataFrame]:
    if candidates is None or candidates.empty:
        return pd.DataFrame(), pd.DataFrame()

    cache: dict[str, pd.DataFrame] = {}
    trades: list[dict[str, Any]] = []
    rejects: list[dict[str, Any]] = []
    for _, cand in candidates.iterrows():
        ticker = str(cand.get("ticker", "")).upper().strip()
        setup = str(cand.get("setup", ""))
        side = str(cand.get("side", "")).upper().strip()
        signal_time = cand.get("signal_time_ist")
        reject_base = {
            "ticker": ticker,
            "side": side,
            "setup": setup,
            "signal_time_ist": signal_time,
            "candidate_id": cand.get("candidate_id", ""),
        }
        if setup not in v6.SETUP_EXIT_RULES:
            rejects.append({**reject_base, "reject_reason": "missing_v6_exit_rule"})
            continue
        bars = _load_1min(ticker, cache)
        if bars is None or bars.empty:
            rejects.append({**reject_base, "reject_reason": "missing_1min_file"})
            continue
        entry = _entry_bar(bars, signal_time)
        if entry is None:
            rejects.append({**reject_base, "reject_reason": "missing_1min_entry_bar"})
            continue
        entry_price = _safe_float(entry.get("open"))
        if not np.isfinite(entry_price) or entry_price <= 0:
            rejects.append({**reject_base, "reject_reason": "bad_entry_price"})
            continue
        sl_pct, target_pct = v6.SETUP_EXIT_RULES[setup]
        indexed = bars.set_index("date")[["high", "low", "close"]].sort_index()
        res = exit_resolver.resolve(
            bars=indexed,
            side=side,
            entry_price=float(entry_price),
            entry_time_ist=entry.get("date"),
            sl_pct=float(sl_pct),
            tgt_pct=float(target_pct),
        )
        if res is None:
            rejects.append({**reject_base, "reject_reason": "missing_1min_exit_window"})
            continue
        net, gross, cost = _net_pnl_rs(res.pnl_pct_price, res.outcome, cost_bps)
        row = cand.to_dict()
        entry_time = _ensure_ist_ts(entry.get("date"))
        exit_time = _ensure_ist_ts(res.exit_time_ist)
        row.update(
            {
                "trade_date": entry_time.strftime("%Y-%m-%d"),
                "entry_time_ist": _fmt_ist(entry_time),
                "entry_price": float(entry_price),
                "sl_pct": float(sl_pct),
                "target_pct": float(target_pct),
                "sl_price": float(entry_price * (1.0 - sl_pct / 100.0)) if side == "LONG" else float(entry_price * (1.0 + sl_pct / 100.0)),
                "target_price": float(entry_price * (1.0 + target_pct / 100.0)) if side == "LONG" else float(entry_price * (1.0 - target_pct / 100.0)),
                "exit_outcome": res.outcome,
                "exit_time_ist": _fmt_ist(exit_time),
                "exit_price": float(res.exit_price),
                "bars_held": int(res.bars_held),
                "price_pnl_pct": float(res.pnl_pct_price),
                "gross_pnl_rs": float(gross),
                "cost_rs": float(cost),
                "net_pnl_rs": float(net),
                "capital_per_trade_rs": float(v6.CAPITAL_PER_TRADE),
                "leverage": float(v6.LEVERAGE),
                "notional_exposure_rs": float(v6.EFFECTIVE_NOTIONAL),
                "cost_bps": float(cost_bps),
                "hold_minutes": float((exit_time - entry_time) / pd.Timedelta(minutes=1)),
            }
        )
        trades.append(row)
    return pd.DataFrame(trades), pd.DataFrame(rejects)


def _profit_factor(values: pd.Series) -> float:
    pnl = pd.to_numeric(values, errors="coerce").fillna(0.0)
    gains = float(pnl[pnl > 0].sum())
    losses = float(-pnl[pnl < 0].sum())
    if losses <= 0:
        return math.inf if gains > 0 else 0.0
    return gains / losses


def _summarise(trades: pd.DataFrame, by: list[str]) -> pd.DataFrame:
    if trades is None or trades.empty:
        return pd.DataFrame()
    rows: list[dict[str, Any]] = []
    for keys, group in trades.groupby(by, dropna=False, sort=True):
        if not isinstance(keys, tuple):
            keys = (keys,)
        pnl = pd.to_numeric(group["net_pnl_rs"], errors="coerce").fillna(0.0)
        out = {col: val for col, val in zip(by, keys)}
        out.update(
            {
                "trades": int(len(group)),
                "win_rate_pct": float((pnl > 0).mean() * 100.0),
                "target_rate_pct": float((group["exit_outcome"].astype(str) == "TARGET").mean() * 100.0),
                "sl_rate_pct": float((group["exit_outcome"].astype(str) == "SL").mean() * 100.0),
                "eod_rate_pct": float((group["exit_outcome"].astype(str) == "EOD").mean() * 100.0),
                "profit_factor": float(_profit_factor(pnl)),
                "net_pnl_rs": float(pnl.sum()),
                "avg_net_pnl_rs": float(pnl.mean()),
                "best_trade_rs": float(pnl.max()),
                "worst_trade_rs": float(pnl.min()),
                "avg_hold_minutes": float(pd.to_numeric(group["hold_minutes"], errors="coerce").mean()),
            }
        )
        rows.append(out)
    return pd.DataFrame(rows).sort_values(["net_pnl_rs", "trades"], ascending=[False, False]).reset_index(drop=True)


def _overall_metrics(trades: pd.DataFrame, raw: pd.DataFrame, gated: pd.DataFrame, live_like: pd.DataFrame, rejects: pd.DataFrame) -> dict[str, Any]:
    pnl = pd.to_numeric(trades.get("net_pnl_rs", pd.Series(dtype=float)), errors="coerce").fillna(0.0)
    side = trades.get("side", pd.Series(dtype=str)).astype(str).str.upper() if not trades.empty else pd.Series(dtype=str)
    return {
        "raw_candidates": int(len(raw)),
        "gated_pre_daily_dedupe": int(len(gated)),
        "live_like_candidates": int(len(live_like)),
        "resolved_trades": int(len(trades)),
        "entry_exit_rejects": int(len(rejects)),
        "win_rate_pct": float((pnl > 0).mean() * 100.0) if len(pnl) else 0.0,
        "profit_factor": float(_profit_factor(pnl)) if len(pnl) else 0.0,
        "net_pnl_rs": float(pnl.sum()) if len(pnl) else 0.0,
        "avg_net_pnl_rs": float(pnl.mean()) if len(pnl) else 0.0,
        "long_trades": int((side == "LONG").sum()) if len(side) else 0,
        "short_trades": int((side == "SHORT").sum()) if len(side) else 0,
        "target_rate_pct": float((trades.get("exit_outcome", pd.Series(dtype=str)).astype(str) == "TARGET").mean() * 100.0) if not trades.empty else 0.0,
        "sl_rate_pct": float((trades.get("exit_outcome", pd.Series(dtype=str)).astype(str) == "SL").mean() * 100.0) if not trades.empty else 0.0,
        "eod_rate_pct": float((trades.get("exit_outcome", pd.Series(dtype=str)).astype(str) == "EOD").mean() * 100.0) if not trades.empty else 0.0,
    }


def _write_report(
    out_dir: Path,
    sessions: list[str],
    start: str,
    end: str,
    metrics: dict[str, Any],
    daily: pd.DataFrame,
    setup: pd.DataFrame,
    side: pd.DataFrame,
    files: dict[str, Path],
) -> Path:
    report_path = out_dir / f"v7_early_scanner_replay_report_{sessions[0]}_to_{sessions[-1]}.md"

    def md_table(df: pd.DataFrame, cols: list[str], n: int = 20) -> str:
        if df is None or df.empty:
            return "_No rows._"
        view = df.loc[:, [c for c in cols if c in df.columns]].head(n).copy()
        for col in view.columns:
            if pd.api.types.is_float_dtype(view[col]):
                view[col] = view[col].map(lambda x: f"{x:.2f}" if np.isfinite(x) else "inf")
        headers = [str(c) for c in view.columns]
        body = [[str(x) for x in row] for row in view.astype(str).to_numpy().tolist()]
        widths = [
            max(len(headers[i]), *(len(row[i]) for row in body)) if body else len(headers[i])
            for i in range(len(headers))
        ]

        def fmt_row(values: list[str]) -> str:
            return "| " + " | ".join(values[i].ljust(widths[i]) for i in range(len(values))) + " |"

        lines = [
            fmt_row(headers),
            "| " + " | ".join("-" * widths[i] for i in range(len(headers))) + " |",
        ]
        lines.extend(fmt_row(row) for row in body)
        return "\n".join(lines)

    lines = [
        "# V7 Early Scanner Replay",
        "",
        f"Sessions: {', '.join(sessions)}",
        f"Scan window: {start} to {end} IST, 5-minute slots",
        "Scope: early-mode scanner candidates only, then live V8 early gate, live-like one ticker per day dedupe, 1-minute entry/exit resolution.",
        "",
        "## Overall",
        "",
        f"- Raw candidates: {metrics['raw_candidates']}",
        f"- Gated before daily dedupe: {metrics['gated_pre_daily_dedupe']}",
        f"- Live-like candidates: {metrics['live_like_candidates']}",
        f"- Resolved trades: {metrics['resolved_trades']}",
        f"- Entry/exit rejects: {metrics['entry_exit_rejects']}",
        f"- Net PnL: Rs {metrics['net_pnl_rs']:.2f}",
        f"- Win rate: {metrics['win_rate_pct']:.2f}%",
        f"- Profit factor: {metrics['profit_factor']:.2f}" if np.isfinite(metrics["profit_factor"]) else "- Profit factor: inf",
        f"- Target / SL / EOD: {metrics['target_rate_pct']:.2f}% / {metrics['sl_rate_pct']:.2f}% / {metrics['eod_rate_pct']:.2f}%",
        "",
        "## Daily",
        "",
        md_table(daily, ["trade_date", "trades", "win_rate_pct", "profit_factor", "net_pnl_rs", "avg_net_pnl_rs", "target_rate_pct", "sl_rate_pct", "eod_rate_pct"]),
        "",
        "## By Side",
        "",
        md_table(side, ["side", "trades", "win_rate_pct", "profit_factor", "net_pnl_rs", "avg_net_pnl_rs", "target_rate_pct", "sl_rate_pct", "eod_rate_pct"]),
        "",
        "## By Setup",
        "",
        md_table(setup, ["side", "setup", "trades", "win_rate_pct", "profit_factor", "net_pnl_rs", "avg_net_pnl_rs", "target_rate_pct", "sl_rate_pct", "eod_rate_pct"], n=50),
        "",
        "## Files",
        "",
    ]
    for label, path in files.items():
        lines.append(f"- {label}: `{path}`")
    report_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return report_path


def _save_outputs(
    out_dir: Path,
    sessions: list[str],
    raw: pd.DataFrame,
    raw_ranked: pd.DataFrame,
    gated: pd.DataFrame,
    live_like: pd.DataFrame,
    audit: pd.DataFrame,
    trades: pd.DataFrame,
    rejects: pd.DataFrame,
    args: argparse.Namespace,
    elapsed_sec: float,
) -> dict[str, Path]:
    out_dir.mkdir(parents=True, exist_ok=True)
    span = f"{sessions[0]}_to_{sessions[-1]}"
    files = {
        "raw_candidates": out_dir / f"v7_early_scanner_raw_candidates_{span}.csv",
        "raw_ranked_candidates": out_dir / f"v7_early_scanner_raw_ranked_candidates_{span}.csv",
        "gated_pre_daily_dedupe": out_dir / f"v7_early_scanner_gated_pre_daily_dedupe_{span}.csv",
        "live_like_candidates": out_dir / f"v7_early_scanner_live_like_candidates_{span}.csv",
        "slot_audit": out_dir / f"v7_early_scanner_slot_audit_{span}.csv",
        "trades_1min_exit": out_dir / f"v7_early_scanner_trades_1min_exit_{span}.csv",
        "entry_exit_rejects": out_dir / f"v7_early_scanner_entry_exit_rejects_{span}.csv",
        "summary_daily": out_dir / f"v7_early_scanner_summary_daily_{span}.csv",
        "summary_setup": out_dir / f"v7_early_scanner_summary_setup_{span}.csv",
        "summary_side": out_dir / f"v7_early_scanner_summary_side_{span}.csv",
        "summary_slot": out_dir / f"v7_early_scanner_summary_slot_{span}.csv",
        "metadata": out_dir / f"v7_early_scanner_replay_metadata_{span}.json",
    }

    raw.to_csv(files["raw_candidates"], index=False)
    raw_ranked.to_csv(files["raw_ranked_candidates"], index=False)
    gated.to_csv(files["gated_pre_daily_dedupe"], index=False)
    live_like.to_csv(files["live_like_candidates"], index=False)
    audit.to_csv(files["slot_audit"], index=False)
    trades.to_csv(files["trades_1min_exit"], index=False)
    rejects.to_csv(files["entry_exit_rejects"], index=False)

    daily = _summarise(trades, ["trade_date"])
    setup = _summarise(trades, ["side", "setup"])
    side = _summarise(trades, ["side"])
    slot = _summarise(trades.assign(slot_time=trades.get("scan_slot_ist", "")), ["scan_slot_ist"]) if not trades.empty else pd.DataFrame()
    daily.to_csv(files["summary_daily"], index=False)
    setup.to_csv(files["summary_setup"], index=False)
    side.to_csv(files["summary_side"], index=False)
    slot.to_csv(files["summary_slot"], index=False)

    metrics = _overall_metrics(trades, raw, gated, live_like, rejects)
    metadata = {
        "sessions": sessions,
        "start": args.start,
        "end": args.end,
        "workers": int(args.workers),
        "limit": args.limit,
        "elapsed_sec": float(elapsed_sec),
        "data_1min_dir": str(DATA_1MIN_DIR),
        "live_5min_dir": str(scanner.LIVE_5M_DIR),
        "metrics": metrics,
        "env": {
            "early_mode": os.getenv("EQIDV2_SIGNAL_DISCOVERY_V7_EARLY_MODE", "default"),
            "early_live_gate_min_score": discovery.EARLY_LIVE_GATE_MIN_SCORE,
            "early_live_gate_max_per_side": discovery.EARLY_LIVE_GATE_MAX_PER_SIDE,
            "early_live_gate_max_per_slot": discovery.EARLY_LIVE_GATE_MAX_PER_SLOT,
        },
    }
    files["metadata"].write_text(json.dumps(metadata, indent=2, sort_keys=True, default=str), encoding="utf-8")
    report = _write_report(out_dir, sessions, args.start, args.end, metrics, daily, setup, side, files)
    files["report"] = report
    return files


def run(args: argparse.Namespace) -> tuple[dict[str, Path], dict[str, Any]]:
    started = time.perf_counter()
    sessions = _parse_sessions(args.sessions, args.num_sessions)
    if not sessions:
        raise SystemExit("No valid sessions found. Pass --sessions YYYY-MM-DD,...")
    tickers = _load_universe(args.limit)
    if not tickers:
        raise SystemExit("No universe tickers found.")

    print(f"[replay] sessions={sessions}", flush=True)
    print(f"[replay] tickers={len(tickers)} workers={args.workers} window={args.start}-{args.end}", flush=True)

    all_rows: list[dict[str, Any]] = []
    for day in sessions:
        day_started = time.perf_counter()
        slots = _session_slots(day, args.start, args.end)
        slot_isos = [x.isoformat() for x in slots]
        day_rows: list[dict[str, Any]] = []
        with ProcessPoolExecutor(max_workers=max(1, int(args.workers)), initializer=_worker_init, initargs=(day, slot_isos)) as ex:
            future_map = {ex.submit(_scan_ticker_day_early, ticker): ticker for ticker in tickers}
            done = 0
            for fut in as_completed(future_map):
                done += 1
                try:
                    rows = fut.result()
                except Exception:
                    rows = []
                if rows:
                    day_rows.extend(rows)
                if done % 200 == 0 or done == len(future_map):
                    print(f"[replay] {day} tickers_done={done}/{len(future_map)} raw_rows={len(day_rows)}", flush=True)
        print(f"[replay] {day} raw_rows={len(day_rows)} elapsed_sec={time.perf_counter() - day_started:.1f}", flush=True)
        all_rows.extend(day_rows)

    raw = pd.DataFrame(all_rows)
    if not raw.empty:
        raw = scanner._dedupe_candidate_frame(raw)
        raw["scan_date"] = pd.to_datetime(raw["signal_time_ist"], errors="coerce").dt.strftime("%Y-%m-%d")
    print(f"[replay] raw_candidates={len(raw)}", flush=True)
    raw_ranked, gated, live_like, audit = _rank_gate_and_dedupe(raw, sessions)
    print(
        f"[replay] gated_pre_dedupe={len(gated)} live_like={len(live_like)} audit_slots={len(audit)}",
        flush=True,
    )
    trades, rejects = _resolve_entries_and_exits(live_like, args.cost_bps)
    print(f"[replay] resolved_trades={len(trades)} rejects={len(rejects)}", flush=True)

    elapsed = time.perf_counter() - started
    out_dir = Path(args.output_root)
    files = _save_outputs(out_dir, sessions, raw, raw_ranked, gated, live_like, audit, trades, rejects, args, elapsed)
    metrics = _overall_metrics(trades, raw, gated, live_like, rejects)
    metrics["elapsed_sec"] = elapsed
    return files, metrics


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Replay v7 early scanner over recent sessions and resolve exits with stored 1-minute data.")
    parser.add_argument("--sessions", default="", help="Comma-separated YYYY-MM-DD list. Defaults to last non-empty truth-table sessions.")
    parser.add_argument("--num-sessions", type=int, default=DEFAULT_SESSIONS)
    parser.add_argument("--start", default=DEFAULT_START)
    parser.add_argument("--end", default=DEFAULT_END)
    parser.add_argument("--workers", type=int, default=int(os.getenv("EQIDV2_SIGNAL_DISCOVERY_V7_SCAN_WORKERS", "8")))
    parser.add_argument("--limit", type=int, default=0, help="Optional ticker limit for smoke runs.")
    parser.add_argument("--cost-bps", type=float, default=v6.DEFAULT_COST_BPS)
    parser.add_argument("--output-root", default=str(OUT_ROOT))
    return parser


def main(argv: Optional[list[str]] = None) -> int:
    args = build_arg_parser().parse_args(argv)
    files, metrics = run(args)
    print("[replay] done", flush=True)
    print(json.dumps({"metrics": metrics, "files": {k: str(v) for k, v in files.items()}}, indent=2, default=str), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
