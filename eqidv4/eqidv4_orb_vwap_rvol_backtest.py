# -*- coding: utf-8 -*-
"""
EQIDV4: ORB + VWAP + RVOL intraday backtest (NSE equities).
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass, field
from datetime import date as date_t
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

from eqidv4_orb_strategy_core import (
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


@dataclass
class Position:
    ticker: str
    side: str
    entry_time: pd.Timestamp
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
    return {
        "trade_date": str(trade_date),
        "ticker": pos.ticker,
        "side": pos.side,
        "entry_time_ist": str(pos.entry_time),
        "entry_price_raw": float(pos.entry_raw),
        "entry_price_exec": float(pos.entry_exec),
        "stop_price_raw": float(pos.stop_raw),
        "tp1_price_raw": float(pos.tp1_raw),
        "exit_time_ist": str(pos.legs[-1]["time"]) if pos.legs else "",
        "exit_price_raw_wavg": float(w_exit_raw),
        "exit_price_exec_wavg": float(w_exit_exec),
        "qty": int(pos.qty_initial),
        "entry_cost_rs": float(pos.entry_cost),
        "pnl_rs": float(pos.realized_pnl_rs),
        "pnl_r": float(pnl_r) if np.isfinite(pnl_r) else np.nan,
        "bars_held": int(pos.bars_held),
        "mfe_r": float(pos.max_fav_r),
        "partial_done": bool(pos.partial_done),
        "exit_reasons": reasons,
        "signal_rvol": float(pos.signal_rvol),
        "signal_buffer": float(pos.signal_buffer),
        "or_high": float(pos.signal_or_high),
        "or_low": float(pos.signal_or_low),
    }


def _iter_dates(turnover_df: pd.DataFrame, start_date: Optional[str], end_date: Optional[str]) -> List[date_t]:
    dts = sorted(pd.to_datetime(turnover_df["session_date"]).dt.date.unique())
    if not dts:
        return []
    s = pd.to_datetime(start_date).date() if start_date else dts[-5]
    e = pd.to_datetime(end_date).date() if end_date else dts[-1]
    return [d for d in dts if s <= d <= e]


def _open_position(row: pd.Series, ticker: str, side: str, buffer_val: float, orb: Dict[str, object], capital: float, cfg: StrategyConfig) -> Optional[Position]:
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


def _update_position_for_bar(pos: Position, row: pd.Series, cfg: StrategyConfig) -> None:
    ts = pd.Timestamp(row["date"])
    pos.bars_held += 1

    # Track max favorable move in R using bar extremes.
    if pos.side == "LONG":
        fav_r = (float(row["high"]) - float(pos.entry_raw)) / float(pos.risk_per_share)
    else:
        fav_r = (float(pos.entry_raw) - float(row["low"])) / float(pos.risk_per_share)
    pos.max_fav_r = max(pos.max_fav_r, float(fav_r))

    # Hard/force exits first near close.
    if row["clock"] >= cfg.hard_exit_time or row["clock"] >= cfg.force_exit_time:
        _close_all(pos, float(row["close"]), ts, "FORCE_EXIT", cfg)
        return

    # Conservative intrabar ordering: stop before any target.
    if pos.side == "LONG" and float(row["low"]) <= float(pos.stop_raw):
        _close_all(pos, float(pos.stop_raw), ts, "STOP", cfg)
        return
    if pos.side == "SHORT" and float(row["high"]) >= float(pos.stop_raw):
        _close_all(pos, float(pos.stop_raw), ts, "STOP", cfg)
        return

    # Partial at +1R.
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

    # Time stop.
    if pos.bars_held >= int(cfg.time_stop_bars) and pos.max_fav_r < float(cfg.time_stop_min_r):
        _close_all(pos, float(row["close"]), ts, "TIME_STOP", cfg)
        return

    # Trailing.
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
    verbose: bool,
) -> pd.DataFrame:
    all_tickers = list_tickers(root)
    if ticker_limit and ticker_limit > 0:
        all_tickers = all_tickers[: int(ticker_limit)]
    turnover = build_turnover_cache(root, tickers=all_tickers, refresh=refresh_universe_cache)
    if turnover.empty:
        raise RuntimeError("Universe turnover cache is empty.")

    dates = _iter_dates(turnover, start_date, end_date)
    if not dates:
        raise RuntimeError("No trade dates in selected range.")

    cache: Dict[str, Optional[tuple]] = {}
    out_rows: List[Dict[str, object]] = []

    for d in dates:
        universe = select_universe_for_date(turnover, d, cfg)
        if ticker_limit and ticker_limit > 0:
            universe = universe[: int(ticker_limit)]
        if verbose:
            print(f"\n[DAY] {d} universe={len(universe)}", flush=True)
        if not universe:
            continue

        idx_flags = infer_index_regime_flags(root, d, cfg)
        day_ctx: Dict[str, Dict[str, object]] = {}
        all_times = set()

        for t in universe:
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
            idx_map = {pd.Timestamp(ts): int(i) for i, ts in enumerate(day["date"].tolist())}
            day_ctx[t] = {"df": day, "idx_map": idx_map, "orb": orb}
            all_times.update(idx_map.keys())

        if not day_ctx:
            continue

        timeline = sorted(all_times)
        open_pos: Dict[str, Position] = {}
        trades_per_symbol: Dict[str, int] = {}
        day_realized_r = 0.0

        for ts in timeline:
            # 1) Manage exits for currently open positions.
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

            # 2) Gate new entries after time cutoff / daily loss / max positions.
            if ts.time() > cfg.no_new_entries_after:
                continue
            if day_realized_r <= cfg.daily_loss_limit_r:
                continue
            free_slots = int(cfg.max_open_positions) - len(open_pos)
            if free_slots <= 0:
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
                    ok, reason, buffer_val = check_entry_signal(row, prev_row, ctx["orb"], side, cfg)
                    if not ok:
                        continue
                    p = _open_position(row, t, side, float(buffer_val or 0.0), ctx["orb"], capital, cfg)
                    if p is None:
                        continue
                    threshold = float(ctx["orb"]["or_high"]) if side == "LONG" else float(ctx["orb"]["or_low"])
                    breakout_dist = abs(float(row["close"]) - threshold)
                    candidates.append(
                        {
                            "ticker": t,
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
                open_pos[t] = c["position"]  # entry already cost-adjusted
                trades_per_symbol[t] = trades_per_symbol.get(t, 0) + 1

        # 3) End-of-day safety close for any remaining positions.
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

        if verbose:
            day_df = pd.DataFrame([r for r in out_rows if r["trade_date"] == str(d)])
            print(
                f"[DAY] {d} trades={len(day_df)} pnl_rs={day_df['pnl_rs'].sum() if not day_df.empty else 0:.2f}",
                flush=True,
            )

    return pd.DataFrame(out_rows)


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


def main() -> None:
    ap = argparse.ArgumentParser(description="EQIDV4 ORB+VWAP+RVOL backtest")
    ap.add_argument("--start-date", default=None, help="YYYY-MM-DD")
    ap.add_argument("--end-date", default=None, help="YYYY-MM-DD")
    ap.add_argument("--capital", type=float, default=1_000_000.0)
    ap.add_argument("--orb-minutes", type=int, default=15, choices=[15, 30])
    ap.add_argument("--rvol-min", type=float, default=1.5)
    ap.add_argument("--stop-atr5-mult", type=float, default=0.8)
    ap.add_argument("--trail-method", default="vwap15", choices=["vwap15", "ema20_5m"])
    ap.add_argument("--require-two-close-confirm", action="store_true", default=False)
    ap.add_argument("--universe-top-n", type=int, default=500)
    ap.add_argument("--ticker-limit", type=int, default=0, help="Optional cap for faster test runs.")
    ap.add_argument("--disable-index-regime", action="store_true")
    ap.add_argument("--refresh-universe-cache", action="store_true")
    ap.add_argument("--output-csv", default=None)
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

    trades = run_backtest(
        root=root,
        cfg=cfg,
        capital=float(args.capital),
        start_date=args.start_date,
        end_date=args.end_date,
        refresh_universe_cache=bool(args.refresh_universe_cache),
        ticker_limit=ticker_limit,
        verbose=bool(args.verbose),
    )

    out_dir = root / "outputs"
    out_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(tz=IST).strftime("%Y%m%d_%H%M%S")
    out_csv = Path(args.output_csv) if args.output_csv else out_dir / f"eqidv4_orb_backtest_trades_{ts}.csv"
    trades.to_csv(out_csv, index=False)

    summary = _summary(trades)
    summary.update(
        {
            "generated_at_ist": datetime.now(tz=IST).isoformat(),
            "start_date": args.start_date,
            "end_date": args.end_date,
            "config": cfg.to_dict(),
            "output_csv": str(out_csv),
        }
    )
    out_json = out_dir / f"eqidv4_orb_backtest_summary_{ts}.json"
    out_json.write_text(json.dumps(summary, indent=2), encoding="utf-8")

    print(f"[DONE] trades={summary['trades']} pnl_rs={summary['pnl_rs']:.2f} win_rate={summary['win_rate']:.2%}")
    print(f"[OUT ] {out_csv}")
    print(f"[OUT ] {out_json}")


if __name__ == "__main__":
    main()

