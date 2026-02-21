# -*- coding: utf-8 -*-
"""
EQIDV4: ORB + VWAP + RVOL live scanner.

Modes:
- --run-once: evaluate latest completed 5m bar for today's date.
- --replay-date YYYY-MM-DD: scan historical date (full-day signal replay).
"""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd

from eqidv4_orb_strategy_core import (
    IST,
    StrategyConfig,
    attach_index_flags,
    build_turnover_cache,
    check_entry_signal,
    infer_index_regime_flags,
    list_tickers,
    load_symbol_data,
    opening_range,
    prepare_day_merged_5m_15m,
    select_universe_for_date,
)


def _signal_id(ticker: str, side: str, ts: pd.Timestamp, setup: str) -> str:
    raw = f"EQIDV4_ORB|{ticker.upper()}|{side.upper()}|{str(ts)}|{setup}"
    return hashlib.md5(raw.encode("utf-8")).hexdigest()[:16]


def _state_path(root: Path) -> Path:
    p = root / "logs" / "eqidv4_orb_live_state.json"
    p.parent.mkdir(parents=True, exist_ok=True)
    return p


def _load_state(path: Path) -> Dict[str, object]:
    if not path.exists():
        return {"seen_ids": []}
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {"seen_ids": []}
    if not isinstance(obj, dict):
        return {"seen_ids": []}
    obj.setdefault("seen_ids", [])
    return obj


def _save_state(path: Path, state: Dict[str, object]) -> None:
    seen = list(dict.fromkeys([str(x) for x in state.get("seen_ids", [])]))
    state["seen_ids"] = seen[-20000:]
    path.write_text(json.dumps(state, indent=2), encoding="utf-8")


def _latest_completed_5m_ts(now_ist: pd.Timestamp) -> pd.Timestamp:
    # Subtract a few seconds before flooring to avoid boundary race.
    return (now_ist - pd.Timedelta(seconds=5)).floor("5min")


def _scan_symbol_day(
    day: pd.DataFrame,
    orb: Dict[str, object],
    cfg: StrategyConfig,
    ticker: str,
    latest_only_ts: Optional[pd.Timestamp] = None,
) -> List[Dict[str, object]]:
    if day.empty:
        return []
    rows: List[Dict[str, object]] = []
    used = 0
    for i in range(1, len(day)):
        row = day.iloc[i]
        prev = day.iloc[i - 1]
        ts = pd.Timestamp(row["date"])
        if latest_only_ts is not None and ts != latest_only_ts:
            continue
        if ts <= pd.Timestamp(orb["or_end_ts"]):
            continue
        if row["clock"] > cfg.no_new_entries_after:
            continue
        if used >= int(cfg.max_trades_per_symbol_per_day):
            break

        for side in ("LONG", "SHORT"):
            ok, reason, buffer_val = check_entry_signal(row, prev, orb, side, cfg)
            if not ok:
                continue
            stop_dist = float(cfg.stop_atr5_mult) * float(row["atr5"])
            if stop_dist <= 0:
                continue
            entry = float(row["close"])
            stop = entry - stop_dist if side == "LONG" else entry + stop_dist
            tp1 = entry + (cfg.partial_at_r * stop_dist if side == "LONG" else -cfg.partial_at_r * stop_dist)
            sig = {
                "date": str(pd.to_datetime(row["session_date"]).date()),
                "ticker": ticker.upper(),
                "side": side,
                "signal_entry_datetime_ist": str(ts),
                "signal_bar_time_ist": str(ts),
                "setup": "ORB_VWAP_RVOL",
                "entry_price": entry,
                "stop_price": stop,
                "tp1_price": tp1,
                "quality_score": float(row["rvol"]),
                "rvol": float(row["rvol"]),
                "atr5": float(row["atr5"]),
                "atr15": float(row["atr15"]),
                "or_high": float(orb["or_high"]),
                "or_low": float(orb["or_low"]),
                "or_range": float(orb["or_range"]),
                "or_buffer": float(buffer_val or 0.0),
                "signal_id": _signal_id(ticker, side, ts, "ORB_VWAP_RVOL"),
            }
            rows.append(sig)
            used += 1
            break
    return rows


def scan_signals(
    root: Path,
    cfg: StrategyConfig,
    trade_date: str,
    replay_full_day: bool,
    ticker_limit: Optional[int],
    refresh_universe_cache: bool,
    verbose: bool,
) -> pd.DataFrame:
    d = pd.to_datetime(trade_date).date()
    tickers = list_tickers(root)
    if ticker_limit and ticker_limit > 0:
        tickers = tickers[: int(ticker_limit)]
    turnover = build_turnover_cache(root, tickers=tickers, refresh=refresh_universe_cache)
    if turnover.empty:
        return pd.DataFrame()

    universe = select_universe_for_date(turnover, d, cfg)
    if ticker_limit and ticker_limit > 0:
        universe = universe[: int(ticker_limit)]
    if verbose:
        print(f"[SCAN] date={d} universe={len(universe)}", flush=True)
    if not universe:
        return pd.DataFrame()

    idx_flags = infer_index_regime_flags(root, d, cfg)
    cache: Dict[str, Optional[tuple]] = {}
    all_rows: List[Dict[str, object]] = []

    latest_ts: Optional[pd.Timestamp] = None
    if not replay_full_day:
        latest_ts = _latest_completed_5m_ts(pd.Timestamp.now(tz=IST))
        if verbose:
            print(f"[SCAN] latest_completed_5m={latest_ts}", flush=True)

    for i, t in enumerate(universe, start=1):
        data = cache.get(t)
        if data is None and t not in cache:
            data = load_symbol_data(root, t, cfg)
            cache[t] = data
        if data is None:
            continue
        df5, df15 = data
        day = prepare_day_merged_5m_15m(df5, df15, d)
        if day.empty:
            continue
        day = attach_index_flags(day, idx_flags)
        orb = opening_range(day, cfg.orb_minutes)
        if orb is None:
            continue

        rows = _scan_symbol_day(
            day=day.reset_index(drop=True),
            orb=orb,
            cfg=cfg,
            ticker=t,
            latest_only_ts=None if replay_full_day else latest_ts,
        )
        all_rows.extend(rows)
        if verbose and i % 100 == 0:
            print(f"[SCAN] processed {i}/{len(universe)} symbols | signals={len(all_rows)}", flush=True)

    if not all_rows:
        return pd.DataFrame()
    out = pd.DataFrame(all_rows).sort_values(["signal_bar_time_ist", "side", "ticker"]).reset_index(drop=True)
    return out


def _write_live_csv(root: Path, df: pd.DataFrame) -> Path:
    live_dir = root / "live_signals"
    live_dir.mkdir(parents=True, exist_ok=True)
    d = str(df.iloc[0]["date"])
    out = live_dir / f"signals_{d}.csv"
    if out.exists():
        prev = pd.read_csv(out)
        merged = pd.concat([prev, df], ignore_index=True)
        merged = merged.drop_duplicates(subset=["signal_id"], keep="last")
        merged.to_csv(out, index=False)
    else:
        df.to_csv(out, index=False)
    return out


def main() -> None:
    ap = argparse.ArgumentParser(description="EQIDV4 ORB+VWAP+RVOL live scanner")
    ap.add_argument("--run-once", action="store_true", help="Scan latest completed 5m bar for today.")
    ap.add_argument("--replay-date", default=None, help="YYYY-MM-DD; if provided, scan this date.")
    ap.add_argument("--replay-full-day", action="store_true", help="With --replay-date, scan the full day.")
    ap.add_argument("--orb-minutes", type=int, default=15, choices=[15, 30])
    ap.add_argument("--rvol-min", type=float, default=1.5)
    ap.add_argument("--stop-atr5-mult", type=float, default=0.8)
    ap.add_argument("--trail-method", default="vwap15", choices=["vwap15", "ema20_5m"])
    ap.add_argument("--require-two-close-confirm", action="store_true", default=False)
    ap.add_argument("--universe-top-n", type=int, default=500)
    ap.add_argument("--ticker-limit", type=int, default=0, help="Optional cap for faster runs.")
    ap.add_argument("--disable-index-regime", action="store_true")
    ap.add_argument("--refresh-universe-cache", action="store_true")
    ap.add_argument("--replay-out-csv", default=None)
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    root = Path(__file__).resolve().parent
    cfg = StrategyConfig()
    cfg.orb_minutes = int(args.orb_minutes)
    cfg.rvol_min = float(args.rvol_min)
    cfg.stop_atr5_mult = float(args.stop_atr5_mult)
    cfg.trail_method = str(args.trail_method)
    cfg.require_two_close_confirm = bool(args.require_two_close_confirm)
    cfg.universe_top_n = int(args.universe_top_n)
    cfg.use_index_regime_filter = not bool(args.disable_index_regime)

    ticker_limit = int(args.ticker_limit) if int(args.ticker_limit) > 0 else None

    replay_date = args.replay_date
    if replay_date:
        trade_date = replay_date
        replay_full = bool(args.replay_full_day) or True
    else:
        trade_date = str(pd.Timestamp.now(tz=IST).date())
        replay_full = False

    if not args.run_once and not replay_date:
        raise SystemExit("Use --run-once for live scan, or --replay-date for historical scan.")

    df = scan_signals(
        root=root,
        cfg=cfg,
        trade_date=trade_date,
        replay_full_day=replay_full,
        ticker_limit=ticker_limit,
        refresh_universe_cache=bool(args.refresh_universe_cache),
        verbose=bool(args.verbose),
    )

    if df.empty:
        print(f"[SCAN] No signals for {trade_date}.")
        return

    if replay_date:
        out_dir = root / "out_eqidv4_orb_live_signals_5m"
        out_dir.mkdir(parents=True, exist_ok=True)
        out = Path(args.replay_out_csv) if args.replay_out_csv else out_dir / f"replay_signals_{trade_date}.csv"
        df.to_csv(out, index=False)
        print(f"[REPLAY] Wrote {len(df)} signals -> {out}")
        return

    # Live run_once mode: de-duplicate by state.
    st_path = _state_path(root)
    state = _load_state(st_path)
    seen = set(str(x) for x in state.get("seen_ids", []))
    fresh = df[~df["signal_id"].astype(str).isin(seen)].copy()
    if fresh.empty:
        print("[LIVE] No new unique signals.")
        return
    out = _write_live_csv(root, fresh)
    state["seen_ids"] = list(seen.union(set(fresh["signal_id"].astype(str).tolist())))
    state["last_run_ist"] = datetime.now(tz=IST).isoformat()
    _save_state(st_path, state)
    print(f"[LIVE] Wrote {len(fresh)} new signals -> {out}")


if __name__ == "__main__":
    main()

