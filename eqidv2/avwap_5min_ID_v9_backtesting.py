"""
AVWAP ID 5-min v8 backtester.

v8 keeps the v7 signal/setup selection flow intact, but removes the forced
next-5-minute entry. A setup is still confirmed on the completed 5-minute
signal candle; entry is then recalculated from the first available 1-minute
bar at/after the signal time, using that 1-minute bar's open.

Default output:
  C:\\TradingData\\eqidv2\\outputs_ID_v8_5min
"""

from __future__ import annotations

import argparse
import json
import time
from functools import lru_cache
from pathlib import Path

import numpy as np
import pandas as pd

import eqidv2_signal_discovery_v7_5min_id_persistent as live_discovery
import avwap_5min_ID_v7_candidate_scan as candidate_scan
import avwap_5min_ID_v5_backtesting as v5
import avwap_5min_ID_v6_backtesting as v6
import avwap_5min_ID_v7_backtesting as v7
import v17D_exit_resolver as er


OUT_ROOT = Path(r"C:\TradingData\eqidv2\outputs_ID_v8_5min")
LIVE_CANDIDATE_JSON_DIR = Path(r"C:\TradingData\eqidv2\signal_discovery_v7_5mins_ID\json")
PNL_COL = v7.PNL_COL
EXCLUDED_SETUPS = v7.EXCLUDED_SETUPS


@lru_cache(maxsize=None)
def _load_1m_with_open(ticker: str) -> pd.DataFrame | None:
    path = v6.DATA_1M_DIR / f"{str(ticker).upper()}_stocks_indicators_1min.parquet"
    if not path.exists():
        return None
    df = pd.read_parquet(path, columns=["date", "open", "high", "low", "close"])
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    if getattr(df["date"].dt, "tz", None) is None:
        df["date"] = df["date"].dt.tz_localize("UTC").dt.tz_convert("Asia/Kolkata")
    else:
        df["date"] = df["date"].dt.tz_convert("Asia/Kolkata")
    df = df.dropna(subset=["date"]).sort_values("date")
    return df.set_index("date")


def _normalise_ts(value) -> pd.Timestamp:
    ts = pd.to_datetime(value, errors="coerce")
    if pd.isna(ts):
        return pd.NaT
    if ts.tz is None:
        return ts.tz_localize("Asia/Kolkata")
    return ts.tz_convert("Asia/Kolkata")


def _signal_time(row: pd.Series) -> pd.Timestamp:
    for col in ("signal_time_ist", "signal_ts", "bar_time_ist"):
        if col in row.index and pd.notna(row.get(col)):
            ts = _normalise_ts(row.get(col))
            if pd.notna(ts):
                return ts
    entry = row.get("entry_time_v6", row.get("entry_time_ist", row.get("entry_ts", pd.NaT)))
    ts = _normalise_ts(entry)
    if pd.notna(ts):
        return ts - pd.Timedelta(minutes=5)
    return pd.NaT


def _first_1m_entry(
    bars: pd.DataFrame,
    signal_ts: pd.Timestamp,
    *,
    max_delay_minutes: int = 5,
) -> tuple[pd.Timestamp, float] | None:
    sig = _normalise_ts(signal_ts)
    if pd.isna(sig):
        return None
    latest_allowed = sig + pd.Timedelta(minutes=max_delay_minutes)
    sub = bars[(bars.index >= sig) & (bars.index <= latest_allowed)]
    if sub.empty:
        return None
    entry_ts = pd.Timestamp(sub.index[0])
    entry_px = float(sub.iloc[0]["open"])
    if not np.isfinite(entry_px) or entry_px <= 0:
        return None
    return entry_ts, entry_px


def _resolve_trade_1m_entry(row: pd.Series, cost_bps: float) -> dict | None:
    setup = str(row["setup"])
    if setup not in v6.SETUP_EXIT_RULES:
        raise ValueError(f"missing v8 exit rule for setup: {setup}")
    sl_pct, target_pct = v6.SETUP_EXIT_RULES[setup]
    if sl_pct < v6.MIN_SL_PCT:
        raise ValueError(f"SL for {setup} is below {v6.MIN_SL_PCT:.2f}%: {sl_pct}")

    bars = _load_1m_with_open(str(row["ticker"]))
    if bars is None or bars.empty:
        return None

    sig_ts = _signal_time(row)
    entry = _first_1m_entry(bars, sig_ts)
    if entry is None:
        return None
    entry_ts, entry_px = entry

    res = er.resolve(
        bars=bars,
        side=str(row["side"]),
        entry_price=entry_px,
        entry_time_ist=entry_ts,
        sl_pct=sl_pct,
        tgt_pct=target_pct,
    )
    if res is None:
        return None

    net, gross, cost = v6._net_pnl_rs(res.pnl_pct_price, res.outcome, cost_bps)
    old_entry_ts = row.get("entry_time_v6", row.get("entry_time_ist", row.get("entry_ts", pd.NaT)))
    old_entry_px = row.get("entry_price_v6", row.get("entry_price", row.get("entry_px", np.nan)))
    rec = row.to_dict()
    rec.update({
        "signal_time_v8": sig_ts,
        "old_entry_time_v7": old_entry_ts,
        "old_entry_price_v7": old_entry_px,
        "entry_time_v6": entry_ts,
        "entry_price_v6": float(entry_px),
        "trade_date": str(entry_ts.date()),
        "v8_entry_model": "FIRST_1MIN_OPEN_AT_OR_AFTER_SIGNAL",
        "v8_entry_delay_minutes": float((entry_ts - sig_ts).total_seconds() / 60.0) if pd.notna(sig_ts) else np.nan,
        "v6_sl_pct": sl_pct,
        "v6_target_pct": target_pct,
        "v6_outcome": res.outcome,
        "v6_exit_price": float(res.exit_price),
        "v6_exit_time_ist": res.exit_time_ist,
        "v6_bars_held": int(res.bars_held),
        "v6_pnl_pct_price": float(res.pnl_pct_price),
        "v6_gross_pnl_rs": float(gross),
        "v6_cost_rs": float(cost),
        "v6_net_pnl_rs": float(net),
        "capital_per_trade_rs": v6.CAPITAL_PER_TRADE,
        "leverage": v6.LEVERAGE,
        "notional_exposure_rs": v6.EFFECTIVE_NOTIONAL,
    })
    return rec


def _resolve_trades(trades: pd.DataFrame, cost_bps: float, label: str) -> pd.DataFrame:
    normalised = v6._normalise_trades(trades)
    normalised = normalised.loc[~normalised["setup"].astype(str).isin(EXCLUDED_SETUPS)].copy()
    missing = sorted(set(normalised["setup"].astype(str)) - set(v6.SETUP_EXIT_RULES))
    if missing:
        print(f"[v8] skipping {label} setups without setup-specific exits: {missing}")
        normalised = normalised.loc[normalised["setup"].astype(str).isin(v6.SETUP_EXIT_RULES)].copy()

    print(f"[v8] resolving {len(normalised):,} {label} trades with 1-min entry + 1-min exits")
    rows = []
    misses = 0
    t0 = time.time()
    for i, (_, row) in enumerate(normalised.iterrows(), 1):
        rec = _resolve_trade_1m_entry(row, cost_bps=cost_bps)
        if rec is None:
            misses += 1
        else:
            rows.append(rec)
        if i % 500 == 0 or i == len(normalised):
            print(f"  [v8 {label} {i:5d}/{len(normalised)}] resolved={len(rows)} misses={misses} elapsed={time.time()-t0:.1f}s")

    if not rows:
        raise SystemExit(f"[v8] no {label} trades resolved")
    out = pd.DataFrame(rows)
    out["v7_resolution_source"] = label
    out["v8_resolution_source"] = label
    return out


def _load_live_candidate_snapshots(json_dir: Path, day: str = "") -> pd.DataFrame:
    day_key = "".join(str(day or "").split("-"))
    patterns = []
    if day_key:
        patterns.extend([f"raw_candidate_tickers_{day_key}_*.json", f"candidate_tickers_{day_key}_*.json"])
    else:
        patterns.extend(["raw_candidate_tickers_*.json", "candidate_tickers_*.json"])

    rows: list[dict] = []
    seen_files: set[Path] = set()
    for pattern in patterns:
        for path in sorted(json_dir.glob(pattern)):
            if path in seen_files:
                continue
            seen_files.add(path)
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
            except Exception:
                continue
            # For new runs, raw_candidate_tickers_* is the pre-gate source.
            # For older runs, candidate_tickers_* may still be raw.
            for row in payload.get("candidates", []) or []:
                if isinstance(row, dict):
                    rows.append(row)
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    if day:
        sig = pd.to_datetime(df.get("signal_time_ist"), errors="coerce")
        df = df.loc[sig.dt.strftime("%Y-%m-%d").eq(str(day))].copy()
    return df.reset_index(drop=True)


def _parse_hhmm(text: str) -> int:
    try:
        hh_text, mm_text = str(text).strip().split(":", 1)
        hh = int(hh_text)
        mm = int(mm_text)
    except (TypeError, ValueError) as exc:
        raise SystemExit(f"[v8 historical_full_day] invalid time {text!r}; expected HH:MM") from exc
    if not (0 <= hh <= 23 and 0 <= mm <= 59):
        raise SystemExit(f"[v8 historical_full_day] invalid time {text!r}; expected HH:MM")
    return hh * 60 + mm


def _slot_range_for_day(day: str, start_time: str, end_time: str) -> list[pd.Timestamp]:
    if not str(day or "").strip():
        raise SystemExit("[v8 historical_full_day] --historical_date is required")
    day_date = pd.to_datetime(day, errors="coerce")
    if pd.isna(day_date):
        raise SystemExit(f"[v8 historical_full_day] invalid --historical_date={day!r}")
    start_min = _parse_hhmm(start_time)
    end_min = _parse_hhmm(end_time)
    if end_min < start_min:
        raise SystemExit("[v8 historical_full_day] --end_time must be >= --start_time")

    base = pd.Timestamp(
        year=int(day_date.year),
        month=int(day_date.month),
        day=int(day_date.day),
        tz="Asia/Kolkata",
    )
    return [base + pd.Timedelta(minutes=minute) for minute in range(start_min, end_min + 1, 5)]


def _fmt_ist(ts: pd.Timestamp) -> str:
    t = _normalise_ts(ts)
    if pd.isna(t):
        return ""
    offset = t.strftime("%z")
    return f"{t.strftime('%Y-%m-%d %H:%M:%S')}{offset[:3]}:{offset[3:]}"


def _write_candidate_snapshot(
    df: pd.DataFrame,
    slot: pd.Timestamp,
    path: Path,
    *,
    output_label: str,
    payload_extra: dict | None = None,
) -> None:
    rows = [] if df is None or df.empty else df.to_dict("records")
    side_counts = {"LONG": 0, "SHORT": 0}
    setup_counts: dict[str, int] = {}
    for row in rows:
        side = str(row.get("side", "")).upper()
        setup = str(row.get("setup", ""))
        side_counts[side] = side_counts.get(side, 0) + 1
        setup_counts[setup] = setup_counts.get(setup, 0) + 1

    payload = {
        "session": "V8 historical full-day candidate regeneration",
        "output_label": output_label,
        "slot_ist": _fmt_ist(slot),
        "created_at_ist": _fmt_ist(pd.Timestamp.now(tz="Asia/Kolkata")),
        "total_candidates": int(len(rows)),
        "long_candidates": int(side_counts.get("LONG", 0)),
        "short_candidates": int(side_counts.get("SHORT", 0)),
        "setup_counts": setup_counts,
        "candidates": rows,
        **(payload_extra or {}),
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str), encoding="utf-8")


def _scan_historical_full_day_candidates(
    *,
    day: str,
    start_time: str,
    end_time: str,
    workers: int,
    snapshot_dir: Path,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    slots = _slot_range_for_day(day, start_time, end_time)
    try:
        universe = candidate_scan.v2._load_universe()
    except Exception as exc:
        raise SystemExit(f"[v8 historical_full_day] universe load failed: {type(exc).__name__}: {exc}") from exc

    tickers = sorted({str(t).strip().upper() for t in universe if str(t).strip()})
    if not tickers:
        raise SystemExit("[v8 historical_full_day] universe is empty")

    worker_count = max(1, int(workers))
    market_ctx = candidate_scan.build_market_context_once() if worker_count <= 1 else None
    frames: list[pd.DataFrame] = []
    slot_rows: list[dict] = []

    print(
        f"[v8 historical_full_day] scanning {len(slots)} slots "
        f"from {start_time} to {end_time} for {day} across {len(tickers)} tickers",
        flush=True,
    )
    t0 = time.time()
    day_key = "".join(str(day).split("-"))
    for i, slot in enumerate(slots, 1):
        slot_t0 = time.time()
        df = candidate_scan.scan_slot_candidates(
            slot,
            tickers,
            market_ctx,
            max_workers=worker_count,
        )
        if df is None:
            df = pd.DataFrame()
        raw_path = snapshot_dir / f"raw_candidate_tickers_{day_key}_{slot.strftime('%H%M')}.json"
        _write_candidate_snapshot(
            df,
            slot,
            raw_path,
            output_label="raw_pre_gate",
            payload_extra={"candidate_source": "candidate_scan.scan_slot_candidates"},
        )
        if not df.empty:
            frames.append(df)
        long_count = int(0 if df.empty else (df["side"].astype(str).str.upper() == "LONG").sum())
        short_count = int(0 if df.empty else (df["side"].astype(str).str.upper() == "SHORT").sum())
        slot_rows.append({
            "slot_ist": _fmt_ist(slot),
            "raw_candidate_count": int(len(df)),
            "long_candidates": long_count,
            "short_candidates": short_count,
            "elapsed_sec": round(time.time() - slot_t0, 3),
            "snapshot_path": str(raw_path),
        })
        print(
            f"  [v8 historical_full_day {i:02d}/{len(slots):02d}] "
            f"{slot.strftime('%H:%M')} raw={len(df)} long={long_count} short={short_count} "
            f"elapsed={time.time() - slot_t0:.1f}s total={time.time() - t0:.1f}s",
            flush=True,
        )

    if not frames:
        return pd.DataFrame(), pd.DataFrame(slot_rows)
    raw = pd.concat(frames, ignore_index=True)
    return raw.reset_index(drop=True), pd.DataFrame(slot_rows)


def _normalise_live_candidates(candidates: pd.DataFrame) -> pd.DataFrame:
    if candidates is None or candidates.empty:
        return pd.DataFrame()
    out = candidates.copy()
    required = {"ticker", "side", "setup", "signal_time_ist"}
    if not required.issubset(out.columns):
        missing = sorted(required - set(out.columns))
        raise SystemExit(f"[v8 live_parity] missing candidate columns: {missing}")
    out["ticker"] = out["ticker"].astype(str).str.upper().str.strip()
    out["side"] = out["side"].astype(str).str.upper().str.strip()
    out["setup"] = out["setup"].astype(str).str.strip()
    out["signal_time_ist"] = pd.to_datetime(out["signal_time_ist"], errors="coerce")
    if getattr(out["signal_time_ist"].dt, "tz", None) is None:
        out["signal_time_ist"] = out["signal_time_ist"].dt.tz_localize("Asia/Kolkata")
    else:
        out["signal_time_ist"] = out["signal_time_ist"].dt.tz_convert("Asia/Kolkata")
    out = out.dropna(subset=["ticker", "side", "setup", "signal_time_ist"]).copy()
    out = out.loc[~out["setup"].isin(EXCLUDED_SETUPS)].copy()
    out = out.loc[out["setup"].isin(v6.SETUP_EXIT_RULES)].copy()
    out["signal_time_ist"] = out["signal_time_ist"].map(lambda x: x.strftime("%Y-%m-%d %H:%M:%S%z"))
    out["signal_time_ist"] = out["signal_time_ist"].str.replace(r"(\+\d{2})(\d{2})$", r"\1:\2", regex=True)
    out["quality_score"] = pd.to_numeric(out.get("quality_score", 0.0), errors="coerce").fillna(0.0)
    if "candidate_id" not in out.columns:
        out["candidate_id"] = (
            out["ticker"].astype(str)
            + "|"
            + out["side"].astype(str)
            + "|"
            + out["setup"].astype(str)
            + "|"
            + out["signal_time_ist"].astype(str)
        )
    out = (
        out.sort_values(["signal_time_ist", "quality_score", "ticker", "setup"], ascending=[True, False, True, True])
        .drop_duplicates(subset=["candidate_id"], keep="first")
        .drop_duplicates(subset=["signal_time_ist", "ticker"], keep="first")
        .reset_index(drop=True)
    )
    return out


def _resolve_live_parity_candidates(candidates: pd.DataFrame, cost_bps: float, label: str) -> pd.DataFrame:
    normalised = _normalise_live_candidates(candidates)
    print(f"[v8 {label}] resolving {len(normalised):,} gated live candidates")
    rows = []
    misses = 0
    t0 = time.time()
    for i, (_, row) in enumerate(normalised.iterrows(), 1):
        rec = _resolve_trade_1m_entry(row, cost_bps=cost_bps)
        if rec is None:
            misses += 1
        else:
            rows.append(rec)
        if i % 250 == 0 or i == len(normalised):
            print(f"  [v8 {label} {i:5d}/{len(normalised)}] resolved={len(rows)} misses={misses} elapsed={time.time()-t0:.1f}s")
    if not rows:
        detail = _format_1m_miss_diagnostics(normalised)
        raise SystemExit(f"[v8 {label}] no trades resolved; {detail}")
    out = pd.DataFrame(rows)
    out["v7_resolution_source"] = label
    out["v8_resolution_source"] = label
    out["trade_date"] = out["trade_date"].astype(str).str[:10]
    out["_entry_sort"] = pd.to_datetime(out["entry_time_v6"], errors="coerce")
    out["_score_sort"] = pd.to_numeric(out.get("quality_score", 0.0), errors="coerce").fillna(0.0)
    out = (
        out.sort_values(["trade_date", "_entry_sort", "_score_sort", "ticker"], ascending=[True, True, False, True])
        .drop_duplicates(subset=["trade_date", "ticker"], keep="first")
        .drop(columns=["_entry_sort", "_score_sort"], errors="ignore")
        .reset_index(drop=True)
    )
    return out


def _format_1m_miss_diagnostics(candidates: pd.DataFrame) -> str:
    if candidates is None or candidates.empty:
        return "no normalised candidates remained after v8 filtering"

    signal_times = pd.to_datetime(candidates.get("signal_time_ist"), errors="coerce")
    if getattr(signal_times.dt, "tz", None) is None:
        signal_times = signal_times.dt.tz_localize("Asia/Kolkata")
    else:
        signal_times = signal_times.dt.tz_convert("Asia/Kolkata")
    valid_signals = signal_times.dropna()
    min_signal = valid_signals.min() if not valid_signals.empty else pd.NaT
    max_signal = valid_signals.max() if not valid_signals.empty else pd.NaT

    missing_files = 0
    stale_before_signal = 0
    latest_seen = pd.NaT
    examples: list[str] = []
    for _, row in candidates.iterrows():
        ticker = str(row.get("ticker", "")).upper().strip()
        sig = _normalise_ts(row.get("signal_time_ist"))
        bars = _load_1m_with_open(ticker)
        if bars is None or bars.empty:
            missing_files += 1
            if len(examples) < 3:
                examples.append(f"{ticker}: no 1m file")
            continue
        latest = pd.Timestamp(bars.index.max())
        latest_seen = latest if pd.isna(latest_seen) else max(latest_seen, latest)
        if pd.notna(sig) and latest < sig:
            stale_before_signal += 1
            if len(examples) < 3:
                examples.append(f"{ticker}: latest 1m {_fmt_ist(latest)} before signal {_fmt_ist(sig)}")

    parts = [
        f"candidates={len(candidates)}",
        f"missing_1m_files={missing_files}",
        f"stale_before_signal={stale_before_signal}",
    ]
    if pd.notna(min_signal) and pd.notna(max_signal):
        parts.append(f"signal_window={_fmt_ist(min_signal)}..{_fmt_ist(max_signal)}")
    if pd.notna(latest_seen):
        parts.append(f"latest_1m_seen={_fmt_ist(latest_seen)}")
    parts.append(f"data_1m_dir={v6.DATA_1M_DIR}")
    if examples:
        parts.append("examples=[" + "; ".join(examples) + "]")
    return ", ".join(parts)


def _run_live_parity(args: argparse.Namespace) -> int:
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    source_dir = Path(args.live_candidate_json_dir)
    raw_candidates = _load_live_candidate_snapshots(source_dir, str(args.live_date or ""))
    if raw_candidates.empty:
        raise SystemExit(f"[v8 live_parity] no live candidate snapshots found in {source_dir} for date={args.live_date!r}")

    gated_candidates, gate_stats = live_discovery.apply_v8_live_gate(raw_candidates)
    raw_candidates.to_csv(out_dir / "live_parity_raw_candidates.csv", index=False)
    gated_candidates.to_csv(out_dir / "live_parity_gated_candidates.csv", index=False)
    pd.DataFrame([gate_stats]).to_csv(out_dir / "live_parity_gate_stats.csv", index=False)

    accepted = pd.DataFrame([{
        "stage": "live_parity",
        "source": str(source_dir),
        "date": str(args.live_date or ""),
        **gate_stats,
    }])
    if gated_candidates.empty:
        _write_empty_outputs(
            out_dir,
            accepted,
            str(source_dir),
            accepted_filename="live_parity_rules_used.csv",
            reason="[v8 live_parity] no candidates passed the v8 live gate",
        )
        (out_dir / "inputs.txt").write_text(
            f"mode=live_parity\nlive_candidate_json_dir={source_dir}\nlive_date={args.live_date}\n"
            "entry_model=FIRST_1MIN_OPEN_AT_OR_AFTER_SIGNAL\n"
            "candidate_source=signal_discovery_v7_5mins_ID snapshots\n"
            "gate=v8_live_safe_accepted_rules_only\n"
            "dedupe=one ticker per trade_date, matching live entry-engine intraday ticker guard\n",
            encoding="utf-8",
        )
        print(f"[v8 live_parity] wrote empty no-trade outputs to {out_dir}")
        return 0

    final = _resolve_live_parity_candidates(gated_candidates, float(args.cost_bps), "live_parity")
    _write_outputs(out_dir, final, accepted, str(source_dir), accepted_filename="live_parity_rules_used.csv")
    (out_dir / "inputs.txt").write_text(
        f"mode=live_parity\nlive_candidate_json_dir={source_dir}\nlive_date={args.live_date}\n"
        "entry_model=FIRST_1MIN_OPEN_AT_OR_AFTER_SIGNAL\n"
        "candidate_source=signal_discovery_v7_5mins_ID snapshots\n"
        "gate=v8_live_safe_accepted_rules_only\n"
        "dedupe=one ticker per trade_date, matching live entry-engine intraday ticker guard\n",
        encoding="utf-8",
    )
    print(f"[v8 live_parity] wrote {out_dir}")
    return 0


def _run_historical_full_day(args: argparse.Namespace) -> int:
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    historical_date = str(args.historical_date or args.live_date or "").strip()
    snapshot_dir = out_dir / "generated_candidate_snapshots"

    raw_candidates, slot_summary = _scan_historical_full_day_candidates(
        day=historical_date,
        start_time=str(args.start_time),
        end_time=str(args.end_time),
        workers=int(args.workers),
        snapshot_dir=snapshot_dir,
    )
    slot_summary.to_csv(out_dir / "historical_full_day_slot_summary.csv", index=False)
    raw_candidates.to_csv(out_dir / "historical_full_day_raw_candidates.csv", index=False)
    if raw_candidates.empty:
        raise SystemExit(f"[v8 historical_full_day] no generated candidates for {historical_date}")

    gated_candidates, gate_stats = live_discovery.apply_v8_live_gate(raw_candidates)
    gated_candidates.to_csv(out_dir / "historical_full_day_gated_candidates.csv", index=False)
    pd.DataFrame([gate_stats]).to_csv(out_dir / "historical_full_day_gate_stats.csv", index=False)

    if not gated_candidates.empty and "signal_time_ist" in gated_candidates.columns:
        slot_keys = pd.to_datetime(gated_candidates["signal_time_ist"], errors="coerce").dt.strftime("%H%M")
        for slot_key, group in gated_candidates.groupby(slot_keys):
            if not str(slot_key) or str(slot_key) == "NaT":
                continue
            slot = _normalise_ts(group["signal_time_ist"].iloc[0])
            _write_candidate_snapshot(
                group,
                slot,
                snapshot_dir / f"gated_candidate_tickers_{''.join(historical_date.split('-'))}_{slot_key}.json",
                output_label="gated_for_entry_engine",
                payload_extra={"candidate_source": "historical_full_day_raw_candidates", **gate_stats},
            )

    accepted = pd.DataFrame([{
        "stage": "historical_full_day",
        "source": "candidate_scan.scan_slot_candidates",
        "date": historical_date,
        "start_time": str(args.start_time),
        "end_time": str(args.end_time),
        "slots_scanned": int(len(slot_summary)),
        "raw_candidates": int(len(raw_candidates)),
        "gated_candidates": int(len(gated_candidates)),
        **gate_stats,
    }])
    if gated_candidates.empty:
        _write_empty_outputs(
            out_dir,
            accepted,
            str(out_dir / "historical_full_day_raw_candidates.csv"),
            accepted_filename="historical_full_day_rules_used.csv",
            reason="[v8 historical_full_day] no regenerated candidates passed the v8 live gate",
        )
        (out_dir / "inputs.txt").write_text(
            f"mode=historical_full_day\nhistorical_date={historical_date}\n"
            f"start_time={args.start_time}\nend_time={args.end_time}\nworkers={args.workers}\n"
            f"candidate_snapshot_dir={snapshot_dir}\n"
            "entry_model=FIRST_1MIN_OPEN_AT_OR_AFTER_SIGNAL\n"
            "candidate_source=candidate_scan.scan_slot_candidates regenerated from historical 5-minute bars\n"
            "gate=v8_live_safe_accepted_rules_only\n"
            "dedupe=one ticker per trade_date, matching live entry-engine intraday ticker guard\n",
            encoding="utf-8",
        )
        print(f"[v8 historical_full_day] wrote empty no-trade outputs to {out_dir}")
        return 0

    final = _resolve_live_parity_candidates(gated_candidates, float(args.cost_bps), "historical_full_day")
    _write_outputs(
        out_dir,
        final,
        accepted,
        str(out_dir / "historical_full_day_raw_candidates.csv"),
        accepted_filename="historical_full_day_rules_used.csv",
    )
    (out_dir / "inputs.txt").write_text(
        f"mode=historical_full_day\nhistorical_date={historical_date}\n"
        f"start_time={args.start_time}\nend_time={args.end_time}\nworkers={args.workers}\n"
        f"candidate_snapshot_dir={snapshot_dir}\n"
        "entry_model=FIRST_1MIN_OPEN_AT_OR_AFTER_SIGNAL\n"
        "candidate_source=candidate_scan.scan_slot_candidates regenerated from historical 5-minute bars\n"
        "gate=v8_live_safe_accepted_rules_only\n"
        "dedupe=one ticker per trade_date, matching live entry-engine intraday ticker guard\n",
        encoding="utf-8",
    )
    print(f"[v8 historical_full_day] wrote {out_dir}")
    return 0


def _write_empty_outputs(
    out_dir: Path,
    accepted: pd.DataFrame,
    source: str,
    *,
    accepted_filename: str,
    reason: str,
) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    trade_cols = [
        "ticker", "side", "setup", "signal_time_v8", "old_entry_time_v7",
        "entry_time_v6", "old_entry_price_v7", "entry_price_v6",
        "trade_date", "v8_entry_model", "v8_entry_delay_minutes",
        "v6_sl_pct", "v6_target_pct", "v6_outcome", "v6_exit_price",
        "v6_exit_time_ist", "v6_bars_held", "v6_pnl_pct_price",
        "v6_gross_pnl_rs", "v6_cost_rs", "v6_net_pnl_rs",
        "capital_per_trade_rs", "leverage", "notional_exposure_rs",
        "v7_resolution_source", "v8_resolution_source",
    ]
    pd.DataFrame(columns=trade_cols).to_csv(out_dir / "trades.csv", index=False)
    pd.DataFrame(columns=["trade_date", "net_pnl_rs", "cum_pnl_rs", "drawdown_rs"]).to_csv(
        out_dir / "daily.csv",
        index=False,
    )
    pd.DataFrame(
        columns=[
            "side", "setup", "trades", "win_rate_pct", "target_rate_pct",
            "sl_rate_pct", "eod_rate_pct", "pnl_rs", "sl_pct", "target_pct",
        ]
    ).to_csv(out_dir / "by_setup.csv", index=False)
    accepted.to_csv(out_dir / accepted_filename, index=False)
    pd.DataFrame(
        [{"setup": k, "sl_pct": v[0], "target_pct": v[1]} for k, v in sorted(v6.SETUP_EXIT_RULES.items())]
    ).to_csv(out_dir / "setup_exit_rules.csv", index=False)
    pd.DataFrame(
        columns=[
            "ticker", "side", "setup", "signal_time_v8", "old_entry_time_v7",
            "entry_time_v6", "old_entry_price_v7", "entry_price_v6",
            "v8_entry_delay_minutes", "v6_outcome", "v6_net_pnl_rs",
        ]
    ).to_csv(out_dir / "entry_timing_audit.csv", index=False)

    text = "\n".join([
        "=" * 100,
        "AVWAP ID 5-min v8 backtest",
        "v7 signal flow + first 1-minute open entry + setup-specific 1-minute exits",
        f"Input: {source}",
        "=" * 100,
        "Trades              : 0",
        "Trading days        : 0",
        "Avg trades/day      : 0.00",
        "Win rate            : 0.00%",
        "Target / SL / EOD   : 0.00% / 0.00% / 0.00%",
        "Profit factor       : NA",
        "Net PnL             : Rs 0.00",
        "Day win rate        : 0.00%",
        "Max drawdown        : Rs 0.00",
        "LONG trades/PnL     : 0 / Rs 0.00",
        "SHORT trades/PnL    : 0 / Rs 0.00",
        "",
        f"No-trade reason     : {reason}",
        "=" * 100,
    ])
    print(text)
    (out_dir / "summary.txt").write_text(text + "\n", encoding="utf-8")


def _write_outputs(
    out_dir: Path,
    trades: pd.DataFrame,
    accepted: pd.DataFrame,
    source: str,
    accepted_filename: str = "accepted_rules.csv",
) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    trades.to_csv(out_dir / "trades.csv", index=False)
    summary, daily, by_setup = v6._metrics(trades)
    daily.rename(columns={"_pnl": "net_pnl_rs"}).to_csv(out_dir / "daily.csv", index=False)
    by_setup.to_csv(out_dir / "by_setup.csv", index=False)
    accepted.to_csv(out_dir / accepted_filename, index=False)
    pd.DataFrame(
        [{"setup": k, "sl_pct": v[0], "target_pct": v[1]} for k, v in sorted(v6.SETUP_EXIT_RULES.items())]
    ).to_csv(out_dir / "setup_exit_rules.csv", index=False)

    entry_audit = trades[[
        c for c in [
            "ticker", "side", "setup", "signal_time_v8", "old_entry_time_v7",
            "entry_time_v6", "old_entry_price_v7", "entry_price_v6",
            "v8_entry_delay_minutes", "v6_outcome", "v6_net_pnl_rs",
        ] if c in trades.columns
    ]].copy()
    entry_audit.to_csv(out_dir / "entry_timing_audit.csv", index=False)

    text = v6._summary_text(summary, by_setup, Path(source))
    text = text.replace("AVWAP ID 5-min v6 backtest", "AVWAP ID 5-min v8 backtest")
    text = text.replace(
        "v5 trades re-resolved on 1-minute bars with setup-specific SL/target exits",
        "v7 signal flow + first 1-minute open entry + setup-specific 1-minute exits",
    )
    print(text)
    (out_dir / "summary.txt").write_text(text + "\n", encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser(description="AVWAP ID 5-min v8 with first-1min-open entries")
    ap.add_argument("--mode", choices=["live_parity", "historical_full_day", "research"], default="research")
    ap.add_argument("--out", type=str, default=str(OUT_ROOT))
    ap.add_argument("--live_candidate_json_dir", type=str, default=str(LIVE_CANDIDATE_JSON_DIR))
    ap.add_argument("--live_date", type=str, default="")
    ap.add_argument("--historical_date", type=str, default="")
    ap.add_argument("--start_time", type=str, default="09:15")
    ap.add_argument("--end_time", type=str, default="15:00")
    ap.add_argument("--workers", type=int, default=v5.v2.DEFAULT_WORKERS)
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--cost_bps", type=float, default=v6.DEFAULT_COST_BPS)
    ap.add_argument("--max_trades_per_day", type=int, default=999_999)
    ap.add_argument("--max_same_side_per_day", type=int, default=999_999)
    ap.add_argument("--refresh_baseline", action="store_true")
    ap.add_argument("--use_v5_cache", action="store_true", help="reuse the v8/v5_stage or v7/v5_stage CSVs instead of rescanning raw data")
    ap.add_argument("--v7_cache_dir", type=str, default=str(v7.OUT_ROOT / "v5_stage"), help="fallback v7 stage cache to reuse")
    args = ap.parse_args()

    if args.mode == "live_parity":
        return _run_live_parity(args)
    if args.mode == "historical_full_day":
        return _run_historical_full_day(args)

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    stage_dir = out_dir / "v5_stage"
    cache_stage = Path(args.v7_cache_dir)

    if args.use_v5_cache and not stage_dir.exists() and cache_stage.exists():
        stage_dir = cache_stage

    if not args.use_v5_cache:
        v5_args = argparse.Namespace(
            baseline_csv="",
            expansion_csv=str(v5.DEFAULT_EXPANSION_CACHE),
            out=str(stage_dir),
            refresh_baseline=bool(args.refresh_baseline),
            use_expansion_cache=False,
            v17_timeout_sec=0,
            limit=int(args.limit),
            workers=int(args.workers),
            cost_bps=float(args.cost_bps),
            max_trades_per_day=int(args.max_trades_per_day),
            max_same_side_per_day=int(args.max_same_side_per_day),
            min_pocket_pf=1.30,
            min_total_pf=1.62,
            min_trades=3,
            max_steps=30,
        )
        v5._run_v5(v5_args)

    v5_trades = stage_dir / "trades.csv"
    raw_trades = stage_dir / "native_all_setups_raw" / "trades.csv"
    if not v5_trades.exists() or not raw_trades.exists():
        raise SystemExit(f"[v8] missing v5 stage files under {stage_dir}; rerun without --use_v5_cache")

    base = _resolve_trades(pd.read_csv(v5_trades, low_memory=False), float(args.cost_bps), "base")
    raw = pd.read_csv(raw_trades, low_memory=False)
    raw_norm = v6._normalise_trades(raw)
    raw_norm = raw_norm.loc[~raw_norm["setup"].astype(str).isin(EXCLUDED_SETUPS)].copy()
    raw_norm = raw_norm.loc[~v7._trade_key(raw_norm).isin(set(v7._trade_key(base)))].copy()

    pf2_setups = [str(setup) for setup, group in base.groupby("setup") if v7._pf(group[PNL_COL]) > 2.0]
    raw_norm = raw_norm[raw_norm["setup"].isin(pf2_setups)].copy()
    extra = _resolve_trades(raw_norm, float(args.cost_bps), "extra_pf2_pool")
    extra.to_csv(out_dir / "extra_pf2_setup_candidates_resolved.csv", index=False)

    final, accepted = v7._apply_v7_expansion(base, extra)
    _write_outputs(out_dir, final, accepted, str(v5_trades))
    (out_dir / "inputs.txt").write_text(
        f"v5_trades={v5_trades}\nraw_trades={raw_trades}\nfull_scan={not args.use_v5_cache}\n"
        "entry_model=FIRST_1MIN_OPEN_AT_OR_AFTER_SIGNAL\n",
        encoding="utf-8",
    )
    print(f"[v8] wrote {out_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
