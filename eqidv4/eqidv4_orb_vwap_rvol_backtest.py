# -*- coding: utf-8 -*-
"""
EQIDV4: ORB + VWAP + RVOL intraday backtest (NSE equities).

Add-on: AVWAP-style rich console reporting with auto-saved console log.
"""

from __future__ import annotations

import argparse
import json
import math
import sys
from contextlib import redirect_stderr, redirect_stdout
from dataclasses import dataclass, field
from datetime import date as date_t
from datetime import datetime, time as dtime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from eqidv4_orb_strategy_core import (
    DIR_5M,
    END_5M,
    IST,
    StrategyConfig,
    apply_slippage,
    attach_index_flags,
    build_turnover_cache,
    check_entry_signal,
    infer_index_regime_flags,
    list_tickers,
    load_symbol_data,
    opening_range,
    prepare_day_merged_5m_15m,
    select_universe_for_date,
    transaction_cost,
)


class _Tee:
    def __init__(self, *streams):
        self.streams = streams

    def write(self, data: str) -> None:
        for s in self.streams:
            s.write(data)
            s.flush()

    def flush(self) -> None:
        for s in self.streams:
            s.flush()


@dataclass
class Position:
    ticker: str
    side: str
    entry_time: pd.Timestamp
    or_end_time: pd.Timestamp
    entry_raw: float
    entry_exec: float
    stop_raw: float
    tp1_raw: float
    risk_per_share: float
    qty_initial: int
    qty_open: int
    signal_rvol: float
    signal_buffer: float
    signal_or_high: float
    signal_or_low: float
    entry_cost: float
    realized_pnl_rs: float = 0.0
    bars_held: int = 0
    max_fav_r: float = 0.0
    partial_done: bool = False
    legs: List[Dict[str, object]] = field(default_factory=list)


@dataclass
class BacktestStats:
    total_symbols: int = 0
    universe_days: int = 0
    day_universe_sizes: Dict[str, int] = field(default_factory=dict)
    candidate_long: int = 0
    candidate_short: int = 0
    executed_long: int = 0
    executed_short: int = 0

    def to_dict(self) -> Dict[str, object]:
        return {
            "total_symbols": self.total_symbols,
            "universe_days": self.universe_days,
            "day_universe_sizes": self.day_universe_sizes,
            "candidate_long": self.candidate_long,
            "candidate_short": self.candidate_short,
            "executed_long": self.executed_long,
            "executed_short": self.executed_short,
        }


def _risk_amount(capital: float, cfg: StrategyConfig) -> float:
    return float(capital) * float(cfg.risk_per_trade_pct)


def _realize_leg(pos: Position, qty: int, exit_raw: float, ts: pd.Timestamp, reason: str, cfg: StrategyConfig) -> None:
    qty = int(min(max(0, qty), pos.qty_open))
    if qty <= 0:
        return
    exit_exec = apply_slippage(exit_raw, pos.side, is_entry=False, slippage_bps=cfg.slippage_bps)
    notional = float(exit_exec) * float(qty)
    fee = transaction_cost(notional, cfg)
    if pos.side == "LONG":
        pnl = (float(exit_exec) - float(pos.entry_exec)) * float(qty) - fee
    else:
        pnl = (float(pos.entry_exec) - float(exit_exec)) * float(qty) - fee
    pos.realized_pnl_rs += pnl
    pos.qty_open -= qty
    pos.legs.append(
        {
            "time": str(ts),
            "qty": int(qty),
            "exit_raw": float(exit_raw),
            "exit_exec": float(exit_exec),
            "fee": float(fee),
            "reason": reason,
        }
    )


def _close_all(pos: Position, exit_raw: float, ts: pd.Timestamp, reason: str, cfg: StrategyConfig) -> None:
    _realize_leg(pos, pos.qty_open, exit_raw, ts, reason, cfg)


def _as_trade_record(pos: Position, trade_date: date_t) -> Dict[str, object]:
    total_qty = sum(int(x["qty"]) for x in pos.legs) or pos.qty_initial
    if total_qty <= 0:
        total_qty = pos.qty_initial
    w_exit_raw = sum(float(x["exit_raw"]) * int(x["qty"]) for x in pos.legs) / float(total_qty)
    w_exit_exec = sum(float(x["exit_exec"]) * int(x["qty"]) for x in pos.legs) / float(total_qty)
    reasons = "|".join(str(x["reason"]) for x in pos.legs)
    risk_rs = float(pos.risk_per_share) * float(pos.qty_initial)
    pnl_r = (pos.realized_pnl_rs / risk_rs) if risk_rs > 0 else np.nan
    lag_min = (pd.Timestamp(pos.entry_time) - pd.Timestamp(pos.or_end_time)).total_seconds() / 60.0
    notional = float(pos.entry_exec) * float(pos.qty_initial)
    return {
        "trade_date": str(trade_date),
        "ticker": pos.ticker,
        "side": pos.side,
        "setup": "ORB_VWAP_RVOL",
        "impulse_type": "ORB",
        "entry_time_ist": str(pos.entry_time),
        "or_end_time_ist": str(pos.or_end_time),
        "entry_lag_min": float(lag_min),
        "entry_price_raw": float(pos.entry_raw),
        "entry_price_exec": float(pos.entry_exec),
        "stop_price_raw": float(pos.stop_raw),
        "tp1_price_raw": float(pos.tp1_raw),
        "exit_time_ist": str(pos.legs[-1]["time"]) if pos.legs else "",
        "exit_price_raw_wavg": float(w_exit_raw),
        "exit_price_exec_wavg": float(w_exit_exec),
        "qty": int(pos.qty_initial),
        "notional_exposure_rs": float(notional),
        "entry_cost_rs": float(pos.entry_cost),
        "pnl_rs": float(pos.realized_pnl_rs),
        "pnl_r": float(pnl_r) if np.isfinite(pnl_r) else np.nan,
        "bars_held": int(pos.bars_held),
        "mfe_r": float(pos.max_fav_r),
        "partial_done": bool(pos.partial_done),
        "exit_reasons": reasons,
        "signal_rvol": float(pos.signal_rvol),
        "signal_buffer": float(pos.signal_buffer),
        "quality_score": float(pos.signal_rvol),
        "or_high": float(pos.signal_or_high),
        "or_low": float(pos.signal_or_low),
    }


def _iter_dates(turnover_df: pd.DataFrame, start_date: Optional[str], end_date: Optional[str]) -> List[date_t]:
    dts = sorted(pd.to_datetime(turnover_df["session_date"]).dt.date.unique())
    if not dts:
        return []
    # Default: backtest across the full available history when no date bounds are provided.
    s = pd.to_datetime(start_date).date() if start_date else dts[0]
    e = pd.to_datetime(end_date).date() if end_date else dts[-1]
    return [d for d in dts if s <= d <= e]


def _parse_hhmm(value: str) -> dtime:
    v = str(value or "").strip()
    try:
        return datetime.strptime(v, "%H:%M").time()
    except ValueError as exc:
        raise ValueError(f"Invalid time '{value}'. Expected HH:MM in 24h format.") from exc


def _open_position(
    row: pd.Series,
    ticker: str,
    side: str,
    buffer_val: float,
    orb: Dict[str, object],
    capital: float,
    cfg: StrategyConfig,
) -> Optional[Position]:
    stop_dist = float(cfg.stop_atr5_mult) * float(row["atr5"])
    if not np.isfinite(stop_dist) or stop_dist <= 0:
        return None
    risk_amt = _risk_amount(capital, cfg)
    qty = int(risk_amt // stop_dist)
    if qty < 1:
        return None
    entry_raw = float(row["close"])
    stop_raw = entry_raw - stop_dist if side == "LONG" else entry_raw + stop_dist
    tp1_raw = entry_raw + (cfg.partial_at_r * stop_dist if side == "LONG" else -cfg.partial_at_r * stop_dist)
    entry_exec = apply_slippage(entry_raw, side, is_entry=True, slippage_bps=cfg.slippage_bps)
    entry_fee = transaction_cost(entry_exec * qty, cfg)
    p = Position(
        ticker=ticker,
        side=side,
        entry_time=pd.Timestamp(row["date"]),
        or_end_time=pd.Timestamp(orb["or_end_ts"]),
        entry_raw=entry_raw,
        entry_exec=float(entry_exec),
        stop_raw=float(stop_raw),
        tp1_raw=float(tp1_raw),
        risk_per_share=float(stop_dist),
        qty_initial=int(qty),
        qty_open=int(qty),
        signal_rvol=float(row["rvol"]),
        signal_buffer=float(buffer_val),
        signal_or_high=float(orb["or_high"]),
        signal_or_low=float(orb["or_low"]),
        entry_cost=float(entry_fee),
        realized_pnl_rs=-float(entry_fee),
    )
    return p


def _scan_signal_hits_for_symbol_day(day: pd.DataFrame, orb: Dict[str, object], cfg: StrategyConfig) -> Tuple[int, int]:
    """
    Lightweight signal scan used only for live progress logging.
    Counts long/short valid signal bars for the symbol/day.
    """
    if day is None or day.empty:
        return 0, 0
    long_hit = 0
    short_hit = 0
    or_end_ts = pd.Timestamp(orb["or_end_ts"])
    for i in range(1, len(day)):
        row = day.iloc[i]
        prev_row = day.iloc[i - 1]
        ts = pd.Timestamp(row["date"])
        if ts <= or_end_ts:
            continue
        if row["clock"] > cfg.no_new_entries_after:
            continue
        ok_l = False
        ok_s = False
        if bool(cfg.enable_long):
            ok_l, _, _ = check_entry_signal(row, prev_row, orb, "LONG", cfg)
        if bool(cfg.enable_short):
            ok_s, _, _ = check_entry_signal(row, prev_row, orb, "SHORT", cfg)
        if ok_l:
            long_hit = 1
        if ok_s:
            short_hit = 1
        if long_hit and short_hit:
            break
    return int(long_hit), int(short_hit)


def _update_position_for_bar(pos: Position, row: pd.Series, cfg: StrategyConfig) -> None:
    ts = pd.Timestamp(row["date"])
    pos.bars_held += 1

    if pos.side == "LONG":
        fav_r = (float(row["high"]) - float(pos.entry_raw)) / float(pos.risk_per_share)
    else:
        fav_r = (float(pos.entry_raw) - float(row["low"])) / float(pos.risk_per_share)
    pos.max_fav_r = max(pos.max_fav_r, float(fav_r))

    if row["clock"] >= cfg.hard_exit_time or row["clock"] >= cfg.force_exit_time:
        _close_all(pos, float(row["close"]), ts, "FORCE_EXIT", cfg)
        return

    if pos.side == "LONG" and float(row["low"]) <= float(pos.stop_raw):
        _close_all(pos, float(pos.stop_raw), ts, "STOP", cfg)
        return
    if pos.side == "SHORT" and float(row["high"]) >= float(pos.stop_raw):
        _close_all(pos, float(pos.stop_raw), ts, "STOP", cfg)
        return

    if not pos.partial_done:
        hit_tp = (
            float(row["high"]) >= float(pos.tp1_raw)
            if pos.side == "LONG"
            else float(row["low"]) <= float(pos.tp1_raw)
        )
        if hit_tp:
            qty_half = max(1, pos.qty_initial // 2)
            qty_half = min(qty_half, pos.qty_open)
            _realize_leg(pos, qty_half, float(pos.tp1_raw), ts, "TP1_PARTIAL", cfg)
            pos.partial_done = True
            if pos.qty_open <= 0:
                return

    if pos.bars_held >= int(cfg.time_stop_bars) and pos.max_fav_r < float(cfg.time_stop_min_r):
        _close_all(pos, float(row["close"]), ts, "TIME_STOP", cfg)
        return

    if cfg.trail_method == "ema20_5m":
        if pos.side == "LONG" and float(row["close"]) < float(row["ema20_5m"]):
            _close_all(pos, float(row["close"]), ts, "TRAIL_EMA20_5M", cfg)
            return
        if pos.side == "SHORT" and float(row["close"]) > float(row["ema20_5m"]):
            _close_all(pos, float(row["close"]), ts, "TRAIL_EMA20_5M", cfg)
            return
    else:
        if pos.side == "LONG" and float(row["close15"]) < float(row["vwap15"]):
            _close_all(pos, float(row["close"]), ts, "TRAIL_VWAP15", cfg)
            return
        if pos.side == "SHORT" and float(row["close15"]) > float(row["vwap15"]):
            _close_all(pos, float(row["close"]), ts, "TRAIL_VWAP15", cfg)
            return


def run_backtest(
    root: Path,
    cfg: StrategyConfig,
    capital: float,
    start_date: Optional[str],
    end_date: Optional[str],
    refresh_universe_cache: bool,
    ticker_limit: Optional[int],
    progress_every: int = 100,
) -> Tuple[pd.DataFrame, BacktestStats]:
    all_tickers = list_tickers(root)
    if ticker_limit and ticker_limit > 0:
        all_tickers = all_tickers[: int(ticker_limit)]

    stats = BacktestStats(total_symbols=len(all_tickers))
    turnover = build_turnover_cache(root, tickers=all_tickers, refresh=refresh_universe_cache)
    if turnover.empty:
        raise RuntimeError("Universe turnover cache is empty.")

    dates = _iter_dates(turnover, start_date, end_date)
    if not dates:
        raise RuntimeError("No trade dates in selected range.")
    stats.universe_days = len(dates)

    cache: Dict[str, Optional[tuple]] = {}
    out_rows: List[Dict[str, object]] = []

    print("\n[PHASE 1] Scanning for entry signals using 15-min data...", flush=True)
    for d in dates:
        universe = select_universe_for_date(turnover, d, cfg)
        if ticker_limit and ticker_limit > 0:
            universe = universe[: int(ticker_limit)]
        stats.day_universe_sizes[str(d)] = int(len(universe))
        print(f"[DAY ] {d} universe={len(universe)}", flush=True)
        print(f"[SHORT] Tickers found: {len(universe)}", flush=True)
        print(f"[LONG] Tickers found: {len(universe)}", flush=True)
        if not universe:
            continue

        idx_flags = infer_index_regime_flags(root, d, cfg)
        day_ctx: Dict[str, Dict[str, object]] = {}
        all_times = set()
        live_scan_short = 0
        live_scan_long = 0

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
            day = day.reset_index(drop=True)
            idx_map = {pd.Timestamp(ts): int(j) for j, ts in enumerate(day["date"].tolist())}
            day_ctx[t] = {"df": day, "idx_map": idx_map, "orb": orb}
            all_times.update(idx_map.keys())
            hit_l, hit_s = _scan_signal_hits_for_symbol_day(day, orb, cfg)
            live_scan_long += int(hit_l)
            live_scan_short += int(hit_s)
            if i % int(progress_every) == 0:
                print(f"  [SHORT] scanned {i}/{len(universe)} | trades={live_scan_short}", flush=True)
                print(f"  [LONG] scanned {i}/{len(universe)} | trades={live_scan_long}", flush=True)

        if len(universe) % int(progress_every) != 0:
            print(f"  [SHORT] scanned {len(universe)}/{len(universe)} | trades={live_scan_short}", flush=True)
            print(f"  [LONG] scanned {len(universe)}/{len(universe)} | trades={live_scan_long}", flush=True)

        if not day_ctx:
            continue

        timeline = sorted(all_times)
        open_pos: Dict[str, Position] = {}
        trades_per_symbol: Dict[str, int] = {}
        day_realized_r = 0.0

        for ts in timeline:
            for t, pos in list(open_pos.items()):
                ctx = day_ctx.get(t)
                if ctx is None:
                    continue
                i = ctx["idx_map"].get(ts)
                if i is None:
                    continue
                row = ctx["df"].iloc[i]
                _update_position_for_bar(pos, row, cfg)
                if pos.qty_open <= 0:
                    rec = _as_trade_record(pos, d)
                    out_rows.append(rec)
                    if np.isfinite(rec["pnl_r"]):
                        day_realized_r += float(rec["pnl_r"])
                    del open_pos[t]

            if ts.time() > cfg.no_new_entries_after:
                continue
            if day_realized_r <= cfg.daily_loss_limit_r:
                continue
            if len(open_pos) >= int(cfg.max_open_positions):
                continue

            candidates: List[Dict[str, object]] = []
            for t, ctx in day_ctx.items():
                if t in open_pos:
                    continue
                if trades_per_symbol.get(t, 0) >= int(cfg.max_trades_per_symbol_per_day):
                    continue
                i = ctx["idx_map"].get(ts)
                if i is None or i <= 0:
                    continue
                row = ctx["df"].iloc[i]
                if ts <= pd.Timestamp(ctx["orb"]["or_end_ts"]):
                    continue
                prev_row = ctx["df"].iloc[i - 1]

                for side in ("LONG", "SHORT"):
                    if side == "LONG" and not bool(cfg.enable_long):
                        continue
                    if side == "SHORT" and not bool(cfg.enable_short):
                        continue
                    ts_clock = ts.time()
                    if side == "LONG":
                        if ts_clock < cfg.long_entry_start or ts_clock > cfg.long_entry_end:
                            continue
                    else:
                        if ts_clock < cfg.short_entry_start or ts_clock > cfg.short_entry_end:
                            continue
                    ok, _reason, buffer_val = check_entry_signal(row, prev_row, ctx["orb"], side, cfg)
                    if not ok:
                        continue
                    if side == "LONG":
                        stats.candidate_long += 1
                    else:
                        stats.candidate_short += 1
                    p = _open_position(row, t, side, float(buffer_val or 0.0), ctx["orb"], capital, cfg)
                    if p is None:
                        continue
                    threshold = float(ctx["orb"]["or_high"]) if side == "LONG" else float(ctx["orb"]["or_low"])
                    breakout_dist = abs(float(row["close"]) - threshold)
                    candidates.append(
                        {
                            "ticker": t,
                            "side": side,
                            "position": p,
                            "rank_rvol": float(row["rvol"]),
                            "rank_breakout": breakout_dist,
                        }
                    )

            if not candidates:
                continue
            candidates.sort(key=lambda x: (x["rank_rvol"], x["rank_breakout"]), reverse=True)
            for c in candidates:
                if len(open_pos) >= int(cfg.max_open_positions):
                    break
                t = str(c["ticker"])
                if t in open_pos:
                    continue
                open_pos[t] = c["position"]
                trades_per_symbol[t] = trades_per_symbol.get(t, 0) + 1
                if str(c["side"]).upper() == "LONG":
                    stats.executed_long += 1
                else:
                    stats.executed_short += 1

        for t, pos in list(open_pos.items()):
            ctx = day_ctx[t]
            df = ctx["df"]
            if df.empty:
                continue
            row = df.iloc[-1]
            _close_all(pos, float(row["close"]), pd.Timestamp(row["date"]), "EOD_SAFETY", cfg)
            rec = _as_trade_record(pos, d)
            out_rows.append(rec)
            del open_pos[t]

        day_df = pd.DataFrame([r for r in out_rows if r["trade_date"] == str(d)])
        print(f"[DAY ] {d} completed | trades={len(day_df)} pnl_rs={day_df['pnl_rs'].sum() if not day_df.empty else 0:.2f}", flush=True)

    return pd.DataFrame(out_rows), stats


def _summary(df: pd.DataFrame) -> Dict[str, object]:
    if df is None or df.empty:
        return {
            "trades": 0,
            "pnl_rs": 0.0,
            "avg_pnl_r": 0.0,
            "win_rate": 0.0,
            "long_trades": 0,
            "short_trades": 0,
        }
    pnl_r = pd.to_numeric(df["pnl_r"], errors="coerce")
    wins = int((pnl_r > 0).sum())
    total = int(len(df))
    return {
        "trades": total,
        "pnl_rs": float(pd.to_numeric(df["pnl_rs"], errors="coerce").sum()),
        "avg_pnl_r": float(pnl_r.mean(skipna=True)),
        "win_rate": float(wins / total) if total else 0.0,
        "long_trades": int((df["side"] == "LONG").sum()),
        "short_trades": int((df["side"] == "SHORT").sum()),
    }


def _outcome_from_reasons(s: str) -> str:
    x = str(s or "")
    if "STOP" in x:
        return "SL"
    if "FORCE_EXIT" in x or "EOD_SAFETY" in x:
        return "EOD"
    if "TIME_STOP" in x:
        return "TIME"
    if "TRAIL_" in x:
        return "TRAIL"
    return "OTHER"


def _max_consecutive(flags: pd.Series, target: bool) -> int:
    m = 0
    c = 0
    for v in flags.tolist():
        if bool(v) == bool(target):
            c += 1
            m = max(m, c)
        else:
            c = 0
    return int(m)


def _perf_block(df: pd.DataFrame) -> Dict[str, float]:
    if df.empty:
        return {
            "trades": 0,
            "days": 0,
            "sl_hits": 0,
            "eod_exits": 0,
            "trail_exits": 0,
            "time_exits": 0,
            "tp1_partial": 0,
            "avg_pnl_r": 0.0,
            "sum_pnl_r": 0.0,
            "avg_pnl_rs": 0.0,
            "sum_pnl_rs": 0.0,
            "profit_factor": 0.0,
            "avg_win_r": 0.0,
            "avg_loss_r": 0.0,
            "max_dd_r": 0.0,
            "sharpe": 0.0,
            "sortino": 0.0,
            "calmar": 0.0,
            "max_consec_wins": 0,
            "max_consec_losses": 0,
        }
    x = df.copy()
    x["pnl_r"] = pd.to_numeric(x["pnl_r"], errors="coerce")
    x["pnl_rs"] = pd.to_numeric(x["pnl_rs"], errors="coerce")
    x = x.sort_values(["trade_date", "entry_time_ist"]).reset_index(drop=True)
    x["outcome"] = x["exit_reasons"].map(_outcome_from_reasons)

    wins = x["pnl_r"] > 0
    gross_pos = float(x.loc[x["pnl_r"] > 0, "pnl_r"].sum())
    gross_neg = float(x.loc[x["pnl_r"] < 0, "pnl_r"].sum())
    profit_factor = (gross_pos / abs(gross_neg)) if gross_neg < 0 else np.inf

    rr = x["pnl_r"].fillna(0.0).astype(float)
    mu = float(rr.mean())
    sig = float(rr.std(ddof=0))
    sharpe = (mu / sig * math.sqrt(252.0)) if sig > 0 else 0.0
    downside = rr[rr < 0]
    downside_std = float(downside.std(ddof=0)) if len(downside) > 0 else 0.0
    sortino = (mu / downside_std * math.sqrt(252.0)) if downside_std > 0 else 0.0
    cum = rr.cumsum()
    dd = (cum.cummax() - cum).fillna(0.0)
    max_dd = float(dd.max()) if len(dd) else 0.0
    calmar = ((mu * 252.0) / max_dd) if max_dd > 0 else 0.0

    return {
        "trades": int(len(x)),
        "days": int(x["trade_date"].nunique()),
        "sl_hits": int((x["outcome"] == "SL").sum()),
        "eod_exits": int((x["outcome"] == "EOD").sum()),
        "trail_exits": int((x["outcome"] == "TRAIL").sum()),
        "time_exits": int((x["outcome"] == "TIME").sum()),
        "tp1_partial": int(x["exit_reasons"].astype(str).str.contains("TP1_PARTIAL").sum()),
        "avg_pnl_r": float(rr.mean()),
        "sum_pnl_r": float(rr.sum()),
        "avg_pnl_rs": float(x["pnl_rs"].mean()),
        "sum_pnl_rs": float(x["pnl_rs"].sum()),
        "profit_factor": float(profit_factor) if np.isfinite(profit_factor) else 0.0,
        "avg_win_r": float(x.loc[x["pnl_r"] > 0, "pnl_r"].mean()) if (x["pnl_r"] > 0).any() else 0.0,
        "avg_loss_r": float(x.loc[x["pnl_r"] < 0, "pnl_r"].mean()) if (x["pnl_r"] < 0).any() else 0.0,
        "max_dd_r": max_dd,
        "sharpe": sharpe,
        "sortino": sortino,
        "calmar": calmar,
        "max_consec_wins": _max_consecutive(wins, True),
        "max_consec_losses": _max_consecutive(wins, False),
    }


def _print_perf_block(title: str, m: Dict[str, float]) -> None:
    print(f"\n==================== {title} ====================")
    print(f"Total trades                  : {m['trades']}")
    print(f"Unique trade days             : {m['days']}")
    print(f"SL hits                       : {m['sl_hits']}")
    print(f"EOD exits                     : {m['eod_exits']}")
    print(f"TRAIL exits                   : {m['trail_exits']}")
    print(f"TIME exits                    : {m['time_exits']}")
    print(f"TP1 partial touched           : {m['tp1_partial']}")
    print(f"Avg PnL (R, per trade)        : {m['avg_pnl_r']:.4f}")
    print(f"Sum PnL (R, all trades)       : {m['sum_pnl_r']:.4f}")
    print(f"Avg PnL (Rs, per trade)       : {m['avg_pnl_rs']:.2f}")
    print(f"Sum PnL (Rs, all trades)      : {m['sum_pnl_rs']:.2f}")
    print(f"Profit factor                 : {m['profit_factor']:.3f}")
    print(f"Avg winning trade (R)         : {m['avg_win_r']:.4f}")
    print(f"Avg losing trade (R)          : {m['avg_loss_r']:.4f}")
    print(f"Max drawdown (R cumulative)   : {m['max_dd_r']:.4f}")
    print(f"Sharpe ratio (annualized)     : {m['sharpe']:.3f}")
    print(f"Sortino ratio (annualized)    : {m['sortino']:.3f}")
    print(f"Calmar ratio                  : {m['calmar']:.3f}")
    print(f"Max consecutive wins          : {m['max_consec_wins']}")
    print(f"Max consecutive losses        : {m['max_consec_losses']}")
    print("=" * 82)


def _print_lag_debug(trades: pd.DataFrame) -> None:
    print("\n[DEBUG] OR-end -> entry lag (minutes)")
    if trades.empty or "entry_lag_min" not in trades.columns:
        print("[DEBUG] No trades.")
        return
    x = trades.copy()
    x["entry_lag_min"] = pd.to_numeric(x["entry_lag_min"], errors="coerce")
    x = x.dropna(subset=["entry_lag_min"])
    if x.empty:
        print("[DEBUG] No lag rows.")
        return
    g = (
        x.groupby(["side", "setup"], as_index=False)["entry_lag_min"]
        .agg(["count", "min", "median", "mean", "max"])
        .reset_index()
    )
    print(g.to_string(index=False))


def _print_day_mix(trades: pd.DataFrame) -> None:
    if trades.empty:
        print("[INFO] Day-side mix: no trade days")
        return
    day_side = trades.groupby(["trade_date", "side"]).size().unstack(fill_value=0)
    both = int(((day_side.get("LONG", 0) > 0) & (day_side.get("SHORT", 0) > 0)).sum())
    short_only = int(((day_side.get("SHORT", 0) > 0) & (day_side.get("LONG", 0) == 0)).sum())
    long_only = int(((day_side.get("LONG", 0) > 0) & (day_side.get("SHORT", 0) == 0)).sum())
    total = int(day_side.shape[0])
    print(f"[INFO] Day-side mix: both={both} | short_only={short_only} | long_only={long_only} | total_days={total}")


def _print_avwap_style_addon(stats: BacktestStats) -> None:
    short_trades = int(stats.executed_short)
    long_trades = int(stats.executed_long)
    print("\n[PHASE 2] Re-resolving exits using 5-min data for higher precision...")
    print(f"  [SHORT] {short_trades} trades to re-resolve...")
    print(f"[5MIN] Re-resolved exits for 0/{short_trades} trades using 5-min data.")
    print(f"  [LONG] {long_trades} trades to re-resolve...")
    print(f"[5MIN] Re-resolved exits for 0/{long_trades} trades using 5-min data.")


def _print_header(
    root: Path,
    cfg: StrategyConfig,
    out_dir: Path,
    log_path: Path,
    total_tickers: int,
    capital: float,
) -> None:
    print("=" * 70)
    print("EQIDV4 ORB + VWAP + RVOL backtest runner")
    print("  - Entry signals: 5-min execution with 15-min context filters")
    print("  - Exits: intrabar stop + partial + trailing + force EOD")
    print("  - Outputs: */backtesting/eqidv4/outputs")
    print("  - P&L reported in Rs and R-multiples")
    print("=" * 70)
    p5 = root / DIR_5M
    n5 = len(list(p5.glob(f"*{END_5M}")))
    print(f"[INFO] 5-min data directory: {p5}")
    print(f"[INFO] 5-min parquet files found: {n5}")
    print(f"[INFO] Universe tickers considered: {total_tickers}")
    top_n_label = "ALL" if int(cfg.universe_top_n) <= 0 else str(int(cfg.universe_top_n))
    print(
        f"[INFO] Universe selector: top_n={top_n_label} | min_sessions={int(cfg.universe_min_history_sessions)} | "
        f"min_median_value={float(cfg.universe_min_median_traded_value):.2f} | min_price={float(cfg.universe_min_price):.2f}"
    )
    print(
        f"[INFO] Config: ORB={cfg.orb_minutes}m | RVOL_LONG>={cfg.rvol_min_long} | RVOL_SHORT>={cfg.rvol_min_short} | "
        f"stop={cfg.stop_atr5_mult:.2f}xATR5 | trail={cfg.trail_method}"
    )
    print(f"[INFO] Side controls: enable_long={cfg.enable_long} | enable_short={cfg.enable_short}")
    print(
        f"[INFO] Filters: anti-chop={cfg.anti_chop_or_atr15_mult:.2f}xATR15 | "
        f"2-close-confirm={cfg.require_two_close_confirm} | index-regime={cfg.use_index_regime_filter}"
    )
    print(
        f"[INFO] Entry windows: LONG={cfg.long_entry_start.strftime('%H:%M')}-{cfg.long_entry_end.strftime('%H:%M')} | "
        f"SHORT={cfg.short_entry_start.strftime('%H:%M')}-{cfg.short_entry_end.strftime('%H:%M')}"
    )
    print(
        f"[INFO] Signal quality: ema_spread>={cfg.min_ema_spread_pct_15m*100:.2f}% | "
        f"vwap_gap>={cfg.min_vwap_gap_pct_15m*100:.2f}% | "
        f"body>={cfg.breakout_min_body_frac_5m:.2f} | "
        f"long_clv>={cfg.breakout_long_min_clv_5m:.2f} | short_clv<={cfg.breakout_short_max_clv_5m:.2f}"
    )
    print(
        f"[INFO] Portfolio: max_open={cfg.max_open_positions} | max_trades/symbol/day={cfg.max_trades_per_symbol_per_day} | "
        f"daily_loss_limit={cfg.daily_loss_limit_r}R"
    )
    print(
        f"[INFO] Costs: slippage={cfg.slippage_bps:.1f}bps | tx_cost/side={cfg.tx_cost_bps_per_side:.1f}bps | "
        f"risk_per_trade={cfg.risk_per_trade_pct*100:.3f}% of capital"
    )
    print(f"[INFO] Capital base: Rs.{capital:,.2f}")
    print(f"[INFO] Output directory: {out_dir}")
    print(f"[INFO] Console log: {log_path}")
    print("-" * 70)


def _run_and_report(args: argparse.Namespace) -> None:
    root = Path(__file__).resolve().parent
    cfg = StrategyConfig()
    cfg.orb_minutes = int(args.orb_minutes)
    if args.rvol_min is not None:
        cfg.rvol_min = float(args.rvol_min)
    if args.rvol_min_long is not None:
        cfg.rvol_min_long = float(args.rvol_min_long)
    if args.rvol_min_short is not None:
        cfg.rvol_min_short = float(args.rvol_min_short)
    # Backward compatibility: legacy --rvol-min drives both sides unless explicitly overridden.
    if args.rvol_min is not None and args.rvol_min_long is None:
        cfg.rvol_min_long = float(cfg.rvol_min)
    if args.rvol_min is not None and args.rvol_min_short is None:
        cfg.rvol_min_short = float(cfg.rvol_min)
    cfg.stop_atr5_mult = float(args.stop_atr5_mult)
    cfg.trail_method = str(args.trail_method)
    cfg.require_two_close_confirm = bool(args.require_two_close_confirm)
    cfg.universe_top_n = int(args.universe_top_n)
    cfg.universe_min_history_sessions = int(args.universe_min_history_sessions)
    cfg.universe_min_median_traded_value = float(args.universe_min_median_traded_value)
    cfg.universe_min_price = float(args.universe_min_price)
    cfg.min_ema_spread_pct_15m = float(args.min_ema_spread_pct_15m)
    cfg.min_vwap_gap_pct_15m = float(args.min_vwap_gap_pct_15m)
    cfg.breakout_min_body_frac_5m = float(args.breakout_min_body_frac_5m)
    cfg.breakout_long_min_clv_5m = float(args.breakout_long_min_clv_5m)
    cfg.breakout_short_max_clv_5m = float(args.breakout_short_max_clv_5m)
    cfg.max_open_positions = int(args.max_open_positions)
    cfg.daily_loss_limit_r = float(args.daily_loss_limit_r)
    cfg.long_entry_start = _parse_hhmm(str(args.long_entry_start))
    cfg.long_entry_end = _parse_hhmm(str(args.long_entry_end))
    cfg.short_entry_start = _parse_hhmm(str(args.short_entry_start))
    cfg.short_entry_end = _parse_hhmm(str(args.short_entry_end))
    cfg.enable_long = bool(args.enable_long)
    cfg.enable_short = bool(args.enable_short)
    if cfg.long_entry_start > cfg.long_entry_end:
        raise ValueError("LONG entry window start must be <= end.")
    if cfg.short_entry_start > cfg.short_entry_end:
        raise ValueError("SHORT entry window start must be <= end.")
    if not cfg.enable_long and not cfg.enable_short:
        raise ValueError("At least one side must be enabled.")
    cfg.use_index_regime_filter = not bool(args.disable_index_regime)

    ticker_limit = int(args.ticker_limit) if int(args.ticker_limit) > 0 else None

    out_dir = root / "outputs"
    out_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(tz=IST).strftime("%Y%m%d_%H%M%S")
    out_csv = Path(args.output_csv) if args.output_csv else out_dir / f"eqidv4_orb_backtest_trades_{ts}.csv"
    out_json = out_dir / f"eqidv4_orb_backtest_summary_{ts}.json"
    log_path = out_dir / f"eqidv4_orb_backtest_runner_{ts}.txt"

    all_tickers = list_tickers(root)
    if ticker_limit and ticker_limit > 0:
        all_tickers = all_tickers[: int(ticker_limit)]
    _print_header(root, cfg, out_dir, log_path, len(all_tickers), float(args.capital))

    print("\n[PHASE 1] Scanning and backtesting across selected dates...")
    trades, stats = run_backtest(
        root=root,
        cfg=cfg,
        capital=float(args.capital),
        start_date=args.start_date,
        end_date=args.end_date,
        refresh_universe_cache=bool(args.refresh_universe_cache),
        ticker_limit=ticker_limit,
        progress_every=100,
    )

    _print_avwap_style_addon(stats)

    print("\n[PHASE 3] Finalizing outputs and analytics...")
    trades = trades.sort_values(["trade_date", "entry_time_ist", "side", "ticker"]).reset_index(drop=True) if not trades.empty else trades
    trades.to_csv(out_csv, index=False)

    summary = _summary(trades)
    summary.update(
        {
            "generated_at_ist": datetime.now(tz=IST).isoformat(),
            "start_date": args.start_date,
            "end_date": args.end_date,
            "config": cfg.to_dict(),
            "stats": stats.to_dict(),
            "output_csv": str(out_csv),
        }
    )
    out_json.write_text(json.dumps(summary, indent=2), encoding="utf-8")

    _print_day_mix(trades)
    _print_lag_debug(trades)

    short_df = trades[trades["side"] == "SHORT"].copy() if not trades.empty else pd.DataFrame()
    long_df = trades[trades["side"] == "LONG"].copy() if not trades.empty else pd.DataFrame()
    comb_df = trades.copy()
    _print_perf_block("SHORT (ORB+VWAP+RVOL)", _perf_block(short_df))
    _print_perf_block("LONG  (ORB+VWAP+RVOL)", _perf_block(long_df))
    _print_perf_block("COMBINED (ORB+VWAP+RVOL)", _perf_block(comb_df))

    short_notional = float(pd.to_numeric(short_df.get("pnl_rs", pd.Series(dtype=float)), errors="coerce").sum()) if not short_df.empty else 0.0
    long_notional = float(pd.to_numeric(long_df.get("pnl_rs", pd.Series(dtype=float)), errors="coerce").sum()) if not long_df.empty else 0.0
    print("\n==================== NOTIONAL P&L SUMMARY (Rs.) ====================")
    print(f"SHORT notional P&L            : Rs.{short_notional:,.2f}")
    print(f"LONG  notional P&L            : Rs.{long_notional:,.2f}")
    print(f"TOTAL notional P&L            : Rs.{short_notional + long_notional:,.2f}")
    print("=" * 66)

    if not trades.empty:
        sample_cols = [
            "trade_date",
            "ticker",
            "side",
            "setup",
            "quality_score",
            "entry_price_raw",
            "exit_price_raw_wavg",
            "exit_reasons",
            "pnl_r",
            "pnl_rs",
            "notional_exposure_rs",
        ]
        sample_cols = [c for c in sample_cols if c in trades.columns]
        print("\n=============== SAMPLE (first 30 rows) ===============")
        print(trades[sample_cols].head(30).to_string(index=False))

    print(f"\n[FILE SAVED] {out_csv}")
    print(f"[OUTPUTS DIR] {out_dir}")
    print(f"[CONSOLE LOG] {log_path}")
    print("[DONE]")


def main() -> None:
    ap = argparse.ArgumentParser(description="EQIDV4 ORB+VWAP+RVOL backtest")
    ap.add_argument("--start-date", default=None, help="YYYY-MM-DD")
    ap.add_argument("--end-date", default=None, help="YYYY-MM-DD")
    ap.add_argument("--capital", type=float, default=1_000_000.0)
    ap.add_argument("--orb-minutes", type=int, default=15, choices=[15, 30])
    ap.add_argument("--rvol-min", type=float, default=None, help="Legacy/global RVOL threshold; if set, applies to both sides unless side-specific values are provided.")
    ap.add_argument("--rvol-min-long", type=float, default=None, help="RVOL threshold for LONG entries.")
    ap.add_argument("--rvol-min-short", type=float, default=None, help="RVOL threshold for SHORT entries.")
    ap.add_argument("--stop-atr5-mult", type=float, default=0.8)
    ap.add_argument("--trail-method", default="vwap15", choices=["vwap15", "ema20_5m"])
    ap.add_argument("--require-two-close-confirm", dest="require_two_close_confirm", action="store_true")
    ap.add_argument("--no-two-close-confirm", dest="require_two_close_confirm", action="store_false")
    ap.set_defaults(require_two_close_confirm=True)
    ap.add_argument("--universe-top-n", type=int, default=0, help="<=0 means use all available tickers.")
    ap.add_argument("--universe-min-history-sessions", type=int, default=1)
    ap.add_argument("--universe-min-median-traded-value", type=float, default=0.0)
    ap.add_argument("--universe-min-price", type=float, default=0.0)
    ap.add_argument("--min-ema-spread-pct-15m", type=float, default=0.0)
    ap.add_argument("--min-vwap-gap-pct-15m", type=float, default=0.0)
    ap.add_argument("--breakout-min-body-frac-5m", type=float, default=0.0)
    ap.add_argument("--breakout-long-min-clv-5m", type=float, default=0.0)
    ap.add_argument("--breakout-short-max-clv-5m", type=float, default=1.0)
    ap.add_argument("--max-open-positions", type=int, default=5)
    ap.add_argument("--daily-loss-limit-r", type=float, default=-1.0)
    ap.add_argument("--long-entry-start", default="10:00", help="HH:MM (IST)")
    ap.add_argument("--long-entry-end", default="10:30", help="HH:MM (IST)")
    ap.add_argument("--short-entry-start", default="09:30", help="HH:MM (IST)")
    ap.add_argument("--short-entry-end", default="10:30", help="HH:MM (IST)")
    ap.add_argument("--enable-long", dest="enable_long", action="store_true", help="Allow LONG entries.")
    ap.add_argument("--disable-long", dest="enable_long", action="store_false", help="Disable LONG entries.")
    ap.add_argument("--enable-short", dest="enable_short", action="store_true", help="Allow SHORT entries.")
    ap.add_argument("--disable-short", dest="enable_short", action="store_false", help="Disable SHORT entries.")
    ap.set_defaults(enable_long=False, enable_short=True)
    ap.add_argument("--ticker-limit", type=int, default=0, help="Optional cap for faster test runs.")
    ap.add_argument("--disable-index-regime", action="store_true")
    ap.add_argument("--refresh-universe-cache", action="store_true")
    ap.add_argument("--output-csv", default=None)
    args = ap.parse_args()

    root = Path(__file__).resolve().parent
    out_dir = root / "outputs"
    out_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(tz=IST).strftime("%Y%m%d_%H%M%S")
    log_path = out_dir / f"eqidv4_orb_backtest_runner_{ts}.txt"

    with log_path.open("w", encoding="utf-8") as lf:
        tee = _Tee(sys.__stdout__, lf)
        with redirect_stdout(tee), redirect_stderr(tee):
            _run_and_report(args)

    print(f"[LOG SAVED] {log_path}")


if __name__ == "__main__":
    main()
