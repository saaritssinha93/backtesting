"""Research backtest for the four Hilega Milega setups.

Signals are calculated on completed 5/15/60-minute bars and executed at the
next available five-minute open.  The 09:15 hybrid-store opening snapshot is
always excluded from indicators and signals because it is not a completed bar.
This runner is intentionally separate from the production v11 setup book.
"""

from __future__ import annotations

import argparse
import math
import time
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

import hilega_milega_setups as hm


DEFAULT_DATA_ROOT = Path(r"C:\TradingData\eqidv2\stocks_indicators_5min_eq_live2")
DEFAULT_OUT_ROOT = Path(r"C:\TradingData\eqidv2\outputs_hilega_milega_research")
IST_TZ = "Asia/Kolkata"
OPENING_SNAPSHOT_HHMM = (9, 15)

SETUPS = (
    (hm.LONG_RSI50_REVERSAL, "LONG", hm.SETUP_FLAG_COLUMNS[hm.LONG_RSI50_REVERSAL]),
    (hm.SHORT_RSI50_REVERSAL, "SHORT", hm.SETUP_FLAG_COLUMNS[hm.SHORT_RSI50_REVERSAL]),
    (hm.LONG_BB20_PULLBACK, "LONG", hm.SETUP_FLAG_COLUMNS[hm.LONG_BB20_PULLBACK]),
    (hm.SHORT_BB20_PULLBACK, "SHORT", hm.SETUP_FLAG_COLUMNS[hm.SHORT_BB20_PULLBACK]),
)


def _normalise_ts(series: pd.Series) -> pd.Series:
    ts = pd.to_datetime(series, errors="coerce")
    if getattr(ts.dt, "tz", None) is None:
        return ts.dt.tz_localize("UTC").dt.tz_convert(IST_TZ)
    return ts.dt.tz_convert(IST_TZ)


def _minute_of_day(value: pd.Timestamp) -> int:
    return int(value.hour) * 60 + int(value.minute)


def _parse_hhmm(value: str) -> int:
    parsed = pd.Timestamp(f"2000-01-01 {value}")
    return int(parsed.hour) * 60 + int(parsed.minute)


def _drop_opening_snapshots(frame: pd.DataFrame) -> pd.DataFrame:
    timestamps = pd.to_datetime(frame["date"], errors="coerce")
    opening = (
        timestamps.dt.hour.eq(OPENING_SNAPSHOT_HHMM[0])
        & timestamps.dt.minute.eq(OPENING_SNAPSHOT_HHMM[1])
    )
    return frame.loc[~opening.fillna(False)].copy()


def _add_atr(frame: pd.DataFrame, period: int = 14) -> pd.DataFrame:
    out = frame.copy()
    high = pd.to_numeric(out["high"], errors="coerce")
    low = pd.to_numeric(out["low"], errors="coerce")
    close = pd.to_numeric(out["close"], errors="coerce")
    previous_close = close.shift(1)
    true_range = pd.concat(
        [high - low, (high - previous_close).abs(), (low - previous_close).abs()],
        axis=1,
    ).max(axis=1)
    out["HM_ATR_14"] = true_range.ewm(
        alpha=1.0 / float(period),
        adjust=False,
        min_periods=period,
    ).mean()
    return out


def _add_adx(frame: pd.DataFrame, period: int = 14) -> pd.DataFrame:
    out = frame.copy()
    high = pd.to_numeric(out["high"], errors="coerce")
    low = pd.to_numeric(out["low"], errors="coerce")
    close = pd.to_numeric(out["close"], errors="coerce")
    up_move = high.diff()
    down_move = -low.diff()
    plus_dm = pd.Series(
        np.where((up_move > down_move) & (up_move > 0.0), up_move, 0.0),
        index=out.index,
        dtype=float,
    )
    minus_dm = pd.Series(
        np.where((down_move > up_move) & (down_move > 0.0), down_move, 0.0),
        index=out.index,
        dtype=float,
    )
    previous_close = close.shift(1)
    true_range = pd.concat(
        [high - low, (high - previous_close).abs(), (low - previous_close).abs()],
        axis=1,
    ).max(axis=1)
    alpha = 1.0 / float(period)
    smoothed_tr = true_range.ewm(alpha=alpha, adjust=False, min_periods=period).mean()
    smoothed_plus = plus_dm.ewm(alpha=alpha, adjust=False, min_periods=period).mean()
    smoothed_minus = minus_dm.ewm(alpha=alpha, adjust=False, min_periods=period).mean()
    plus_di = 100.0 * smoothed_plus / smoothed_tr.replace(0.0, np.nan)
    minus_di = 100.0 * smoothed_minus / smoothed_tr.replace(0.0, np.nan)
    dx = 100.0 * (plus_di - minus_di).abs() / (plus_di + minus_di).replace(0.0, np.nan)
    out["HM_ADX_14"] = dx.ewm(alpha=alpha, adjust=False, min_periods=period).mean()
    return out


def _aggregate_signal_bars(frame: pd.DataFrame, minutes: int) -> pd.DataFrame:
    if minutes == 5:
        return frame.copy()
    if minutes not in (15, 60):
        raise ValueError("signal timeframe must be 5, 15, or 60 minutes")

    expected_rows = minutes // 5
    pieces: list[pd.DataFrame] = []
    working = frame.copy()
    working["trade_day"] = working["date"].dt.date
    for _, day in working.groupby("trade_day", sort=True):
        indexed = day.set_index("date").sort_index()
        aggregate = indexed.resample(
            f"{minutes}min",
            origin="start_day",
            offset="15min",
            closed="right",
            label="right",
        ).agg(
            open=("open", "first"),
            high=("high", "max"),
            low=("low", "min"),
            close=("close", "last"),
            source_rows=("close", "count"),
        )
        aggregate = aggregate.loc[aggregate["source_rows"].eq(expected_rows)]
        if not aggregate.empty:
            pieces.append(aggregate.reset_index())
    if not pieces:
        return pd.DataFrame(columns=["date", "open", "high", "low", "close"])
    return pd.concat(pieces, ignore_index=True).sort_values("date").reset_index(drop=True)


def _pnl_pct(side: str, entry: float, exit_price: float) -> float:
    pct = (exit_price - entry) / entry * 100.0
    return pct if side == "LONG" else -pct


def _equity_intraday_cost(
    side: str,
    entry_price: float,
    exit_price: float,
    notional: float,
) -> float:
    """Approximate current Zerodha/NSE equity-intraday round-trip charges."""
    entry_turnover = float(notional)
    exit_turnover = float(notional) * float(exit_price) / float(entry_price)
    if side == "LONG":
        buy_turnover, sell_turnover = entry_turnover, exit_turnover
    else:
        buy_turnover, sell_turnover = exit_turnover, entry_turnover
    total_turnover = buy_turnover + sell_turnover
    brokerage = min(20.0, 0.0003 * buy_turnover) + min(20.0, 0.0003 * sell_turnover)
    exchange = 0.0000307 * total_turnover
    sebi = 0.000001 * total_turnover
    stt = 0.00025 * sell_turnover
    stamp = 0.00003 * buy_turnover
    gst = 0.18 * (brokerage + exchange + sebi)
    return float(brokerage + exchange + sebi + stt + stamp + gst)


def _resolve_exit_levels(
    day_df: pd.DataFrame,
    entry_index: int,
    side: str,
    stop_price: float,
    target_price: float,
    max_bars: int,
) -> tuple[str, float, pd.Timestamp, int]:
    final_index = len(day_df) - 1
    if max_bars > 0:
        final_index = min(final_index, entry_index + max_bars - 1)

    for bars_held, row_index in enumerate(range(entry_index, final_index + 1), 1):
        row = day_df.iloc[row_index]
        high = float(row["high"])
        low = float(row["low"])
        if side == "LONG":
            stop_hit = low <= stop_price
            target_hit = high >= target_price
        else:
            stop_hit = high >= stop_price
            target_hit = low <= target_price

        # Five-minute OHLC cannot reveal intrabar order.  A same-bar collision
        # is therefore resolved conservatively as a stop.
        if stop_hit:
            outcome = "SL_BOTH_HIT_CONSERVATIVE" if target_hit else "SL"
            return outcome, float(stop_price), row["date"], bars_held
        if target_hit:
            return "TARGET", float(target_price), row["date"], bars_held

    last = day_df.iloc[final_index]
    outcome = "TIME" if max_bars > 0 and final_index < len(day_df) - 1 else "EOD"
    return outcome, float(last["close"]), last["date"], max(1, final_index - entry_index + 1)


def _build_exit_levels(
    signal: pd.Series,
    side: str,
    entry_price: float,
    options: dict[str, Any],
) -> tuple[float, float, float] | None:
    model = str(options["exit_model"])
    risk_reward = float(options["risk_reward"])
    if model == "fixed":
        stop_distance = entry_price * float(options["sl_pct"]) / 100.0
        target_distance = entry_price * float(options["target_pct"]) / 100.0
    elif model == "signal_rr":
        signal_stop = float(signal["low"] if side == "LONG" else signal["high"])
        stop_distance = entry_price - signal_stop if side == "LONG" else signal_stop - entry_price
        target_distance = stop_distance * risk_reward
    elif model == "atr_rr":
        atr = float(signal.get("HM_ATR_14", np.nan))
        stop_distance = atr * float(options["atr_multiplier"])
        target_distance = stop_distance * risk_reward
    elif model == "signal_bb":
        signal_stop = float(signal["low"] if side == "LONG" else signal["high"])
        stop_distance = entry_price - signal_stop if side == "LONG" else signal_stop - entry_price
        bb_target = float(
            signal[hm.BB_UPPER_COLUMN] if side == "LONG" else signal[hm.BB_LOWER_COLUMN]
        )
        target_distance = bb_target - entry_price if side == "LONG" else entry_price - bb_target
    else:
        raise ValueError(f"unknown exit model: {model}")

    if not (np.isfinite(stop_distance) and np.isfinite(target_distance)):
        return None
    if stop_distance <= 0.0 or target_distance <= 0.0:
        return None
    risk_pct = stop_distance / entry_price * 100.0
    if risk_pct < float(options["min_risk_pct"]) or risk_pct > float(options["max_risk_pct"]):
        return None
    if side == "LONG":
        return entry_price - stop_distance, entry_price + target_distance, risk_pct
    return entry_price + stop_distance, entry_price - target_distance, risk_pct


def _read_execution_bars(
    path: Path,
    start_date: pd.Timestamp,
    end_date: pd.Timestamp,
    warmup_days: int,
) -> pd.DataFrame:
    frame = pd.read_parquet(path, columns=["date", "open", "high", "low", "close"])
    if frame.empty:
        return frame
    frame["date"] = _normalise_ts(frame["date"])
    frame = (
        frame.dropna(subset=["date", "open", "high", "low", "close"])
        .sort_values("date")
        .drop_duplicates("date", keep="last")
        .reset_index(drop=True)
    )
    frame = _drop_opening_snapshots(frame)
    warmup_start = (start_date - pd.Timedelta(days=max(0, int(warmup_days)))).date()
    dates = frame["date"].dt.date
    frame = frame.loc[(dates >= warmup_start) & (dates <= end_date.date())].reset_index(drop=True)
    return _add_adx(frame)


def _select_data_files(data_root: Path, universe_path: str = "") -> list[Path]:
    files = sorted(data_root.glob("*_stocks_indicators_5min.parquet"))
    if not universe_path:
        return files
    path = Path(universe_path)
    if path.suffix.lower() == ".parquet":
        universe = pd.read_parquet(path)
    else:
        universe = pd.read_csv(path)
    symbol_column = next(
        (column for column in ("underlying", "ticker", "symbol", "tradingsymbol") if column in universe.columns),
        None,
    )
    if symbol_column is None:
        raise ValueError("universe file needs an underlying, ticker, symbol, or tradingsymbol column")
    if "is_index_future" in universe.columns:
        universe = universe.loc[~universe["is_index_future"].fillna(False).astype(bool)]
    allowed = set(universe[symbol_column].astype(str).str.strip().str.upper())
    return [
        path
        for path in files
        if path.name.replace("_stocks_indicators_5min.parquet", "").upper() in allowed
    ]


def _process_file(payload: tuple[str, dict[str, Any]]) -> tuple[list[dict], list[dict]]:
    path_text, options = payload
    path = Path(path_text)
    ticker = path.name.replace("_stocks_indicators_5min.parquet", "").upper()
    start_date = pd.Timestamp(options["start_date"])
    end_date = pd.Timestamp(options["end_date"])
    notional = float(options["capital"]) * float(options["leverage"])
    allowed_setups = set(options["setups"])
    allowed_sides = set(options["sides"])
    trades: list[dict] = []
    skipped: list[dict] = []

    try:
        execution = _read_execution_bars(
            path,
            start_date,
            end_date,
            int(options["warmup_days"]),
        )
        if execution.empty:
            return trades, skipped
        signals = _aggregate_signal_bars(execution, int(options["signal_timeframe"]))
        if signals.empty:
            return trades, skipped
        signals = hm.add_hilega_milega_features(_add_atr(signals))
        signals["trade_day"] = signals["date"].dt.date
        execution["trade_day"] = execution["date"].dt.date
        start_day = start_date.date()
        end_day = end_date.date()

        for day, day_df in execution.groupby("trade_day", sort=True):
            if day < start_day or day > end_day:
                continue
            day_df = day_df.sort_values("date").reset_index(drop=True)
            day_signals = signals.loc[signals["trade_day"].eq(day)].sort_values("date")
            if day_signals.empty or len(day_df) < 2:
                continue
            execution_times = day_df["date"].astype("int64").to_numpy()
            blocked_until: pd.Timestamp | None = None
            traded_today = False

            for _, signal in day_signals.iterrows():
                signal_time = signal["date"]
                signal_minute = _minute_of_day(signal_time)
                if signal_minute < int(options["start_minute"]) or signal_minute > int(options["end_minute"]):
                    continue
                if str(options["position_policy"]) == "one_per_day" and traded_today:
                    break
                if (
                    str(options["position_policy"]) == "one_active"
                    and blocked_until is not None
                    and signal_time < blocked_until
                ):
                    continue

                entry_index = int(np.searchsorted(execution_times, signal_time.value, side="right"))
                if entry_index >= len(day_df):
                    continue
                entry = day_df.iloc[entry_index]
                completed_execution_bar = day_df.iloc[entry_index - 1]
                entry_price = float(entry["open"])
                if not np.isfinite(entry_price) or entry_price <= 0:
                    continue

                for setup, side, flag_column in SETUPS:
                    if setup not in allowed_setups or side not in allowed_sides:
                        continue
                    if not bool(signal.get(flag_column, False)):
                        continue
                    execution_adx = float(completed_execution_bar.get("HM_ADX_14", np.nan))
                    if not np.isfinite(execution_adx) or execution_adx < float(options["min_adx"]):
                        continue
                    if float(signal.get("HM_LINE_DISTANCE", 0.0)) < float(options["min_line_distance"]):
                        continue
                    rsi = float(signal.get(hm.RSI_COLUMN, np.nan))
                    if side == "LONG" and rsi < float(options["long_min_rsi"]):
                        continue
                    if side == "SHORT" and rsi > float(options["short_max_rsi"]):
                        continue
                    levels = _build_exit_levels(signal, side, entry_price, options)
                    if levels is None:
                        continue
                    stop_price, target_price, risk_pct = levels
                    outcome, exit_price, exit_time, bars_held = _resolve_exit_levels(
                        day_df,
                        entry_index,
                        side,
                        stop_price,
                        target_price,
                        int(options["max_bars"]),
                    )
                    pnl_pct = _pnl_pct(side, entry_price, exit_price)
                    gross_pnl = pnl_pct / 100.0 * notional
                    costs = _equity_intraday_cost(side, entry_price, exit_price, notional)
                    turnover = notional + notional * exit_price / entry_price
                    slippage = turnover * float(options["slippage_bps_per_side"]) / 10_000.0
                    trades.append(
                        {
                            "ticker": ticker,
                            "side": side,
                            "setup": setup,
                            "signal_timeframe_min": int(options["signal_timeframe"]),
                            "signal_time_ist": signal_time,
                            "entry_time_ist": entry["date"],
                            "entry_price": entry_price,
                            "stop_price": stop_price,
                            "target_price": target_price,
                            "initial_risk_pct": risk_pct,
                            "exit_time_ist": exit_time,
                            "exit_price": exit_price,
                            "outcome": outcome,
                            "bars_held_5m": int(bars_held),
                            "trade_date": str(day),
                            "pnl_pct_price": float(pnl_pct),
                            "gross_pnl_rs": float(gross_pnl),
                            "estimated_cost_rs": float(costs),
                            "estimated_slippage_rs": float(slippage),
                            "net_pnl_rs": float(gross_pnl - costs - slippage),
                            "capital_per_trade_rs": float(options["capital"]),
                            "leverage": float(options["leverage"]),
                            "notional_exposure_rs": float(notional),
                            "exit_model": str(options["exit_model"]),
                            "risk_reward": float(options["risk_reward"]),
                            "hm_rsi_9": rsi,
                            "hm_rsi_ema_3": float(signal.get(hm.EMA_COLUMN, np.nan)),
                            "hm_rsi_wma_21": float(signal.get(hm.WMA_COLUMN, np.nan)),
                            "hm_line_distance": float(signal.get("HM_LINE_DISTANCE", np.nan)),
                            "execution_adx_14": execution_adx,
                        }
                    )
                    traded_today = True
                    blocked_until = exit_time
                    if str(options["position_policy"]) != "independent":
                        break
    except Exception as exc:
        skipped.append({"ticker": ticker, "setup": "", "signal_time_ist": "", "reason": repr(exc)})
    return trades, skipped


def _profit_factor(pnl: pd.Series) -> float:
    values = pd.to_numeric(pnl, errors="coerce").fillna(0.0)
    gains = float(values[values > 0].sum())
    losses = float(-values[values < 0].sum())
    if losses <= 0:
        return math.inf if gains > 0 else 0.0
    return gains / losses


def _aggregate_results(trades: pd.DataFrame, group_columns: list[str]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    grouped = trades.groupby(group_columns, sort=True, dropna=False)
    for keys, group in grouped:
        if not isinstance(keys, tuple):
            keys = (keys,)
        row = dict(zip(group_columns, keys))
        row.update(
            {
                "trades": int(len(group)),
                "win_rate_pct": float((group["net_pnl_rs"] > 0).mean() * 100.0),
                "target_rate_pct": float(group["outcome"].astype(str).str.startswith("TARGET").mean() * 100.0),
                "sl_rate_pct": float(group["outcome"].astype(str).str.startswith("SL").mean() * 100.0),
                "time_rate_pct": float(group["outcome"].astype(str).eq("TIME").mean() * 100.0),
                "eod_rate_pct": float(group["outcome"].astype(str).eq("EOD").mean() * 100.0),
                "gross_pnl_rs": float(group["gross_pnl_rs"].sum()),
                "net_pnl_rs": float(group["net_pnl_rs"].sum()),
                "gross_profit_factor": _profit_factor(group["gross_pnl_rs"]),
                "net_profit_factor": _profit_factor(group["net_pnl_rs"]),
                "avg_bars_held_5m": float(group["bars_held_5m"].mean()),
            }
        )
        rows.append(row)
    return pd.DataFrame(rows)


def _write_outputs(
    trades: pd.DataFrame,
    skipped: pd.DataFrame,
    out_dir: Path,
    args: argparse.Namespace,
    files_scanned: int,
) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    trades.to_csv(out_dir / "trades.csv", index=False)
    skipped.to_csv(out_dir / "skipped.csv", index=False)

    if trades.empty:
        pd.DataFrame([{"start_date": args.start_date, "end_date": args.end_date, "trades": 0}]).to_csv(
            out_dir / "summary.csv", index=False
        )
        pd.DataFrame().to_csv(out_dir / "by_setup.csv", index=False)
        pd.DataFrame().to_csv(out_dir / "daily.csv", index=False)
        return

    daily = _aggregate_results(trades, ["trade_date"])
    daily["cum_net_pnl_rs"] = daily["net_pnl_rs"].cumsum()
    daily["drawdown_rs"] = daily["cum_net_pnl_rs"] - daily["cum_net_pnl_rs"].cummax()
    by_setup = _aggregate_results(trades, ["side", "setup"])
    summary = pd.DataFrame(
        [
            {
                "start_date": args.start_date,
                "end_date": args.end_date,
                "files_scanned": files_scanned,
                "trades": int(len(trades)),
                "trading_days": int(trades["trade_date"].nunique()),
                "signal_timeframe_min": int(args.signal_timeframe),
                "capital_per_trade_rs": float(args.capital),
                "leverage": float(args.leverage),
                "notional_exposure_rs": float(args.capital * args.leverage),
                "exit_model": args.exit_model,
                "sl_pct": float(args.sl_pct),
                "target_pct": float(args.target_pct),
                "risk_reward": float(args.risk_reward),
                "atr_multiplier": float(args.atr_multiplier),
                "max_bars": int(args.max_bars),
                "position_policy": args.position_policy,
                "min_adx": float(args.min_adx),
                "gross_pnl_rs": float(trades["gross_pnl_rs"].sum()),
                "net_pnl_rs": float(trades["net_pnl_rs"].sum()),
                "estimated_cost_rs": float(trades["estimated_cost_rs"].sum()),
                "estimated_slippage_rs": float(trades["estimated_slippage_rs"].sum()),
                "gross_profit_factor": _profit_factor(trades["gross_pnl_rs"]),
                "net_profit_factor": _profit_factor(trades["net_pnl_rs"]),
                "net_win_rate_pct": float((trades["net_pnl_rs"] > 0).mean() * 100.0),
                "max_daily_drawdown_rs": float(daily["drawdown_rs"].min()),
                "opening_snapshot_policy": "EXCLUDED_FROM_INDICATORS_AND_SIGNALS",
                "entry_model": "next_5m_open_after_completed_signal_bar",
            }
        ]
    )
    summary.to_csv(out_dir / "summary.csv", index=False)
    by_setup.to_csv(out_dir / "by_setup.csv", index=False)
    daily.to_csv(out_dir / "daily.csv", index=False)


def _options_from_args(args: argparse.Namespace) -> dict[str, Any]:
    setups = tuple(value.strip() for value in args.setups.split(",") if value.strip())
    unknown = sorted(set(setups) - set(hm.ALL_SETUPS))
    if unknown:
        raise ValueError(f"unknown setups: {unknown}")
    sides = tuple(value.strip().upper() for value in args.sides.split(",") if value.strip())
    if not set(sides).issubset({"LONG", "SHORT"}):
        raise ValueError("sides must contain LONG and/or SHORT")
    return {
        "start_date": args.start_date,
        "end_date": args.end_date,
        "capital": args.capital,
        "leverage": args.leverage,
        "signal_timeframe": args.signal_timeframe,
        "exit_model": args.exit_model,
        "sl_pct": args.sl_pct,
        "target_pct": args.target_pct,
        "risk_reward": args.risk_reward,
        "atr_multiplier": args.atr_multiplier,
        "min_risk_pct": args.min_risk_pct,
        "max_risk_pct": args.max_risk_pct,
        "max_bars": args.max_bars,
        "slippage_bps_per_side": args.slippage_bps_per_side,
        "position_policy": args.position_policy,
        "setups": setups,
        "sides": sides,
        "start_minute": _parse_hhmm(args.start_time),
        "end_minute": _parse_hhmm(args.end_time),
        "min_line_distance": args.min_line_distance,
        "long_min_rsi": args.long_min_rsi,
        "short_max_rsi": args.short_max_rsi,
        "warmup_days": args.warmup_days,
        "min_adx": args.min_adx,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Hilega Milega completed-bar research backtest")
    parser.add_argument("--data_root", type=str, default=str(DEFAULT_DATA_ROOT))
    parser.add_argument("--universe", type=str, default="")
    parser.add_argument("--out", type=str, default=str(DEFAULT_OUT_ROOT))
    parser.add_argument("--start_date", type=str, required=True)
    parser.add_argument("--end_date", type=str, required=True)
    parser.add_argument("--capital", type=float, default=10_000.0)
    parser.add_argument("--leverage", type=float, default=5.0)
    parser.add_argument("--signal_timeframe", type=int, choices=(5, 15, 60), default=5)
    parser.add_argument("--exit_model", choices=("fixed", "signal_rr", "atr_rr", "signal_bb"), default="fixed")
    parser.add_argument("--sl_pct", type=float, default=1.0)
    parser.add_argument("--target_pct", type=float, default=1.0)
    parser.add_argument("--risk_reward", type=float, default=1.5)
    parser.add_argument("--atr_multiplier", type=float, default=1.0)
    parser.add_argument("--min_risk_pct", type=float, default=0.15)
    parser.add_argument("--max_risk_pct", type=float, default=2.0)
    parser.add_argument("--max_bars", type=int, default=0, help="0 means hold until SL, target, or EOD")
    parser.add_argument("--slippage_bps_per_side", type=float, default=5.0)
    parser.add_argument("--position_policy", choices=("independent", "one_active", "one_per_day"), default="independent")
    parser.add_argument("--setups", type=str, default=",".join(hm.ALL_SETUPS))
    parser.add_argument("--sides", type=str, default="LONG,SHORT")
    parser.add_argument("--start_time", type=str, default="09:20")
    parser.add_argument("--end_time", type=str, default="14:30")
    parser.add_argument("--min_line_distance", type=float, default=0.0)
    parser.add_argument("--long_min_rsi", type=float, default=0.0)
    parser.add_argument("--short_max_rsi", type=float, default=100.0)
    parser.add_argument("--warmup_days", type=int, default=45)
    parser.add_argument("--min_adx", type=float, default=0.0)
    parser.add_argument("--workers", type=int, default=8)
    args = parser.parse_args()

    if pd.Timestamp(args.start_date) > pd.Timestamp(args.end_date):
        parser.error("start_date must be on or before end_date")
    if args.min_risk_pct <= 0 or args.max_risk_pct < args.min_risk_pct:
        parser.error("risk bounds must be positive and ordered")
    options = _options_from_args(args)
    data_root = Path(args.data_root)
    out_dir = Path(args.out)
    files = _select_data_files(data_root, args.universe)
    payloads = [(str(path), options) for path in files]
    all_trades: list[dict] = []
    all_skipped: list[dict] = []
    started = time.time()
    workers = max(1, int(args.workers))
    executor: ProcessPoolExecutor | None = None
    iterator = map(_process_file, payloads)
    if workers > 1:
        executor = ProcessPoolExecutor(max_workers=workers)
        iterator = executor.map(_process_file, payloads, chunksize=12)
    try:
        for index, (trades, skipped) in enumerate(iterator, 1):
            all_trades.extend(trades)
            all_skipped.extend(skipped)
            if index % 100 == 0 or index == len(payloads):
                print(
                    f"[hm_research] processed={index}/{len(payloads)} trades={len(all_trades)} "
                    f"skipped={len(all_skipped)} elapsed={time.time() - started:.1f}s",
                    flush=True,
                )
    finally:
        if executor is not None:
            executor.shutdown(wait=True)

    trades_frame = pd.DataFrame(all_trades)
    if not trades_frame.empty:
        trades_frame = trades_frame.sort_values(
            ["trade_date", "entry_time_ist", "ticker", "setup"]
        ).reset_index(drop=True)
    skipped_frame = pd.DataFrame(all_skipped)
    _write_outputs(trades_frame, skipped_frame, out_dir, args, len(files))
    print(f"[hm_research] wrote {out_dir}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
