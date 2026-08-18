"""Batched, cost-aware research sweep for Hilega Milega setups.

The matrix is deliberately small and declared in code.  It is intended to
identify broad, stable regions worth validating, not to manufacture a peak PF
from hundreds of nearly identical parameter combinations.
"""

from __future__ import annotations

import argparse
import time
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

import hilega_milega_setups as hm
import run_hilega_milega_5m_research_backtest as engine


def _variant(
    timeframe: int,
    exit_model: str,
    *,
    context_timeframe: int = 0,
    sl_pct: float = 1.0,
    target_pct: float = 1.0,
    risk_reward: float = 1.5,
    atr_multiplier: float = 1.0,
    max_bars: int = 0,
) -> dict[str, Any]:
    if exit_model == "fixed":
        exit_label = f"fixed_s{sl_pct:g}_t{target_pct:g}"
    elif exit_model == "atr_rr":
        exit_label = f"atr{atr_multiplier:g}_rr{risk_reward:g}"
    elif exit_model == "signal_rr":
        exit_label = f"signal_rr{risk_reward:g}"
    else:
        exit_label = "signal_bb"
    hold_label = "eod" if max_bars <= 0 else f"b{max_bars}"
    context_label = "" if context_timeframe <= 0 else f"_ctx{context_timeframe}"
    return {
        "variant": f"tf{timeframe}{context_label}_{exit_label}_{hold_label}",
        "signal_timeframe": timeframe,
        "context_timeframe": context_timeframe,
        "exit_model": exit_model,
        "sl_pct": sl_pct,
        "target_pct": target_pct,
        "risk_reward": risk_reward,
        "atr_multiplier": atr_multiplier,
        "max_bars": max_bars,
    }


def build_variants(round_name: str = "full") -> list[dict[str, Any]]:
    variants: list[dict[str, Any]] = []
    if round_name == "fine_reward":
        for timeframe in (15, 60):
            for risk_reward in (1.05, 1.1, 1.15, 1.2, 1.25, 1.3, 1.35, 1.4, 1.5):
                variants.append(
                    _variant(timeframe, "signal_rr", risk_reward=risk_reward)
                )
        return variants
    if round_name == "reward":
        for timeframe in (15, 60):
            for risk_reward in (1.0, 1.25, 1.5, 1.75, 2.0, 2.5, 3.0):
                variants.append(
                    _variant(timeframe, "signal_rr", risk_reward=risk_reward)
                )
                variants.append(
                    _variant(
                        timeframe,
                        "atr_rr",
                        atr_multiplier=1.0,
                        risk_reward=risk_reward,
                    )
                )
            for target_pct in (1.5, 2.0, 2.5, 3.0):
                variants.append(
                    _variant(timeframe, "fixed", sl_pct=1.0, target_pct=target_pct)
                )
        return variants
    if round_name == "context":
        for timeframe, context_timeframe in ((5, 15), (5, 60), (15, 60)):
            variants.extend(
                [
                    _variant(timeframe, "fixed", context_timeframe=context_timeframe, sl_pct=1.0, target_pct=1.0),
                    _variant(timeframe, "fixed", context_timeframe=context_timeframe, sl_pct=0.75, target_pct=1.125),
                    _variant(timeframe, "atr_rr", context_timeframe=context_timeframe, atr_multiplier=1.0, risk_reward=1.5),
                    _variant(timeframe, "signal_rr", context_timeframe=context_timeframe, risk_reward=1.5),
                ]
            )
        return variants
    for timeframe in (5, 15, 60):
        if round_name == "quick":
            variants.extend(
                [
                    _variant(timeframe, "fixed", sl_pct=1.0, target_pct=1.0),
                    _variant(timeframe, "fixed", sl_pct=0.75, target_pct=1.125),
                    _variant(timeframe, "atr_rr", atr_multiplier=1.0, risk_reward=1.5),
                    _variant(timeframe, "signal_rr", risk_reward=1.5),
                ]
            )
            continue
        variants.extend(
            [
                _variant(timeframe, "fixed", sl_pct=0.5, target_pct=0.5),
                _variant(timeframe, "fixed", sl_pct=0.5, target_pct=0.75),
                _variant(timeframe, "fixed", sl_pct=0.75, target_pct=0.75),
                _variant(timeframe, "fixed", sl_pct=0.75, target_pct=1.125),
                _variant(timeframe, "fixed", sl_pct=1.0, target_pct=1.0),
                _variant(timeframe, "fixed", sl_pct=1.0, target_pct=1.5),
                _variant(timeframe, "fixed", sl_pct=1.0, target_pct=2.0),
                _variant(timeframe, "atr_rr", atr_multiplier=0.75, risk_reward=1.5),
                _variant(timeframe, "atr_rr", atr_multiplier=1.0, risk_reward=1.5),
                _variant(timeframe, "signal_rr", risk_reward=1.5),
                _variant(timeframe, "signal_bb"),
                _variant(timeframe, "fixed", sl_pct=0.75, target_pct=1.125, max_bars=12),
                _variant(timeframe, "atr_rr", atr_multiplier=1.0, risk_reward=1.5, max_bars=12),
            ]
        )
    return variants


def _empty_stat() -> dict[str, float]:
    return {
        "trades": 0.0,
        "gross_pnl_rs": 0.0,
        "net_pnl_rs": 0.0,
        "gross_gain_rs": 0.0,
        "gross_loss_rs": 0.0,
        "net_gain_rs": 0.0,
        "net_loss_rs": 0.0,
        "net_wins": 0.0,
        "targets": 0.0,
        "stops": 0.0,
        "times": 0.0,
        "eods": 0.0,
    }


def _add_stat(target: dict[str, float], gross: float, net: float, outcome: str) -> None:
    target["trades"] += 1.0
    target["gross_pnl_rs"] += gross
    target["net_pnl_rs"] += net
    target["gross_gain_rs"] += max(0.0, gross)
    target["gross_loss_rs"] += max(0.0, -gross)
    target["net_gain_rs"] += max(0.0, net)
    target["net_loss_rs"] += max(0.0, -net)
    target["net_wins"] += float(net > 0.0)
    target["targets"] += float(outcome.startswith("TARGET"))
    target["stops"] += float(outcome.startswith("SL"))
    target["times"] += float(outcome == "TIME")
    target["eods"] += float(outcome == "EOD")


def _resolve_exit_numpy(
    highs: np.ndarray,
    lows: np.ndarray,
    closes: np.ndarray,
    entry_index: int,
    side: str,
    stop_price: float,
    target_price: float,
    max_bars: int,
) -> tuple[str, float]:
    final_index = len(closes) - 1
    if max_bars > 0:
        final_index = min(final_index, entry_index + max_bars - 1)
    high_path = highs[entry_index : final_index + 1]
    low_path = lows[entry_index : final_index + 1]
    if side == "LONG":
        stop_hits = np.flatnonzero(low_path <= stop_price)
        target_hits = np.flatnonzero(high_path >= target_price)
    else:
        stop_hits = np.flatnonzero(high_path >= stop_price)
        target_hits = np.flatnonzero(low_path <= target_price)
    first_stop = int(stop_hits[0]) if len(stop_hits) else None
    first_target = int(target_hits[0]) if len(target_hits) else None
    if first_stop is not None and (first_target is None or first_stop <= first_target):
        outcome = "SL_BOTH_HIT_CONSERVATIVE" if first_target == first_stop else "SL"
        return outcome, float(stop_price)
    if first_target is not None:
        return "TARGET", float(target_price)
    outcome = "TIME" if max_bars > 0 and final_index < len(closes) - 1 else "EOD"
    return outcome, float(closes[final_index])


def _process_file(
    payload: tuple[str, dict[str, Any], list[dict[str, Any]]]
) -> tuple[dict[tuple[str, str, str, str], dict[str, float]], list[dict[str, str]]]:
    path_text, options, variants = payload
    path = Path(path_text)
    ticker = path.name.replace("_stocks_indicators_5min.parquet", "").upper()
    start_date = pd.Timestamp(options["start_date"])
    end_date = pd.Timestamp(options["end_date"])
    start_day = start_date.date()
    end_day = end_date.date()
    notional = float(options["capital"]) * float(options["leverage"])
    allowed_setups = set(options["setups"])
    allowed_sides = set(options["sides"])
    stats: dict[tuple[str, str, str, str], dict[str, float]] = defaultdict(_empty_stat)
    skipped: list[dict[str, str]] = []

    try:
        execution = engine._read_execution_bars(
            path,
            start_date,
            end_date,
            int(options["warmup_days"]),
        )
        if execution.empty:
            return dict(stats), skipped
        execution["trade_day"] = execution["date"].dt.date
        variants_by_timeframe: dict[int, list[dict[str, Any]]] = defaultdict(list)
        for variant in variants:
            variants_by_timeframe[int(variant["signal_timeframe"])].append(variant)

        required_timeframes = set(variants_by_timeframe)
        required_timeframes.update(
            int(variant["context_timeframe"])
            for variant in variants
            if int(variant["context_timeframe"]) > 0
        )
        features_by_timeframe: dict[int, pd.DataFrame] = {}
        context_days: dict[tuple[int, object], pd.DataFrame] = {}
        for timeframe in sorted(required_timeframes):
            bars = engine._aggregate_signal_bars(execution, timeframe)
            if bars.empty:
                continue
            bars = hm.add_hilega_milega_features(engine._add_atr(bars))
            bars["trade_day"] = bars["date"].dt.date
            features_by_timeframe[timeframe] = bars
            for day, context_day in bars.groupby("trade_day", sort=False):
                context_days[(timeframe, day)] = context_day.sort_values("date").reset_index(drop=True)

        for timeframe, timeframe_variants in variants_by_timeframe.items():
            signal_bars = features_by_timeframe.get(timeframe)
            if signal_bars is None or signal_bars.empty:
                continue

            for day, day_df in execution.groupby("trade_day", sort=True):
                if day < start_day or day > end_day:
                    continue
                day_df = day_df.sort_values("date").reset_index(drop=True)
                day_signals = signal_bars.loc[signal_bars["trade_day"].eq(day)].sort_values("date")
                if day_signals.empty:
                    continue
                execution_times = day_df["date"].astype("int64").to_numpy()
                highs = pd.to_numeric(day_df["high"], errors="coerce").to_numpy(dtype=float)
                lows = pd.to_numeric(day_df["low"], errors="coerce").to_numpy(dtype=float)
                closes = pd.to_numeric(day_df["close"], errors="coerce").to_numpy(dtype=float)
                for _, signal in day_signals.iterrows():
                    signal_minute = engine._minute_of_day(signal["date"])
                    if signal_minute < int(options["start_minute"]) or signal_minute > int(options["end_minute"]):
                        continue
                    entry_index = int(
                        np.searchsorted(execution_times, signal["date"].value, side="right")
                    )
                    if entry_index >= len(day_df):
                        continue
                    entry_price = float(day_df.iloc[entry_index]["open"])
                    if not np.isfinite(entry_price) or entry_price <= 0.0:
                        continue
                    execution_adx = float(day_df.iloc[entry_index - 1].get("HM_ADX_14", np.nan))
                    if not np.isfinite(execution_adx) or execution_adx < float(options["min_adx"]):
                        continue

                    for setup, side, flag_column in engine.SETUPS:
                        if setup not in allowed_setups or side not in allowed_sides:
                            continue
                        if not bool(signal.get(flag_column, False)):
                            continue
                        if float(signal.get("HM_LINE_DISTANCE", 0.0)) < float(options["min_line_distance"]):
                            continue
                        rsi = float(signal.get(hm.RSI_COLUMN, np.nan))
                        if side == "LONG" and rsi < float(options["long_min_rsi"]):
                            continue
                        if side == "SHORT" and rsi > float(options["short_max_rsi"]):
                            continue
                        for variant in timeframe_variants:
                            context_timeframe = int(variant["context_timeframe"])
                            if context_timeframe > 0:
                                context_day = context_days.get((context_timeframe, day))
                                if context_day is None or context_day.empty:
                                    continue
                                available = context_day.loc[context_day["date"].le(signal["date"])]
                                if available.empty:
                                    continue
                                context = available.iloc[-1]
                                if side == "LONG":
                                    context_ok = bool(context.get("HM_BULLISH_ALIGNMENT", False)) and float(
                                        context.get(hm.RSI_COLUMN, np.nan)
                                    ) > 50.0
                                else:
                                    context_ok = bool(context.get("HM_BEARISH_ALIGNMENT", False)) and float(
                                        context.get(hm.RSI_COLUMN, np.nan)
                                    ) < 50.0
                                if not context_ok:
                                    continue
                            level_options = {
                                **variant,
                                "min_risk_pct": options["min_risk_pct"],
                                "max_risk_pct": options["max_risk_pct"],
                            }
                            levels = engine._build_exit_levels(signal, side, entry_price, level_options)
                            if levels is None:
                                continue
                            stop_price, target_price, _ = levels
                            outcome, exit_price = _resolve_exit_numpy(
                                highs,
                                lows,
                                closes,
                                entry_index,
                                side,
                                stop_price,
                                target_price,
                                int(variant["max_bars"]),
                            )
                            gross = engine._pnl_pct(side, entry_price, exit_price) / 100.0 * notional
                            costs = engine._equity_intraday_cost(side, entry_price, exit_price, notional)
                            turnover = notional + notional * exit_price / entry_price
                            slippage = turnover * float(options["slippage_bps_per_side"]) / 10_000.0
                            net = gross - costs - slippage
                            key = (str(variant["variant"]), side, setup, str(day))
                            _add_stat(stats[key], gross, net, outcome)
    except Exception as exc:
        skipped.append({"ticker": ticker, "reason": repr(exc)})
    return dict(stats), skipped


def _merge_stat(target: dict[str, float], source: dict[str, float]) -> None:
    for field, value in source.items():
        target[field] += float(value)


def _pf(gain: float, loss: float) -> float:
    if loss <= 0.0:
        return float("inf") if gain > 0.0 else 0.0
    return float(gain / loss)


def _rows_from_stats(
    stats: dict[tuple[str, str, str, str], dict[str, float]],
    variants: list[dict[str, Any]],
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    variant_lookup = {str(item["variant"]): item for item in variants}
    daily_rows: list[dict[str, Any]] = []
    side_totals: dict[tuple[str, str], dict[str, float]] = defaultdict(_empty_stat)
    setup_totals: dict[tuple[str, str, str], dict[str, float]] = defaultdict(_empty_stat)
    daily_side: dict[tuple[str, str, str], dict[str, float]] = defaultdict(_empty_stat)

    for (variant, side, setup, day), stat in stats.items():
        _merge_stat(side_totals[(variant, side)], stat)
        _merge_stat(setup_totals[(variant, side, setup)], stat)
        _merge_stat(daily_side[(variant, side, day)], stat)

    for (variant, side, day), stat in daily_side.items():
        daily_rows.append(
            {
                "variant": variant,
                "side": side,
                "trade_date": day,
                "trades": int(stat["trades"]),
                "net_pnl_rs": stat["net_pnl_rs"],
                "net_profit_factor": _pf(stat["net_gain_rs"], stat["net_loss_rs"]),
            }
        )
    daily = pd.DataFrame(daily_rows)

    def make_rows(source: dict[tuple, dict[str, float]], key_names: list[str]) -> pd.DataFrame:
        rows: list[dict[str, Any]] = []
        for keys, stat in source.items():
            row = dict(zip(key_names, keys))
            variant = str(keys[0])
            config = variant_lookup[variant]
            row.update(config)
            trades = max(1.0, stat["trades"])
            row.update(
                {
                    "trades": int(stat["trades"]),
                    "gross_pnl_rs": stat["gross_pnl_rs"],
                    "net_pnl_rs": stat["net_pnl_rs"],
                    "gross_profit_factor": _pf(stat["gross_gain_rs"], stat["gross_loss_rs"]),
                    "net_profit_factor": _pf(stat["net_gain_rs"], stat["net_loss_rs"]),
                    "net_win_rate_pct": stat["net_wins"] / trades * 100.0,
                    "target_rate_pct": stat["targets"] / trades * 100.0,
                    "sl_rate_pct": stat["stops"] / trades * 100.0,
                    "time_rate_pct": stat["times"] / trades * 100.0,
                    "eod_rate_pct": stat["eods"] / trades * 100.0,
                }
            )
            if len(key_names) == 2 and not daily.empty:
                selection = daily.loc[(daily["variant"] == variant) & (daily["side"] == keys[1])]
                row["positive_days"] = int((selection["net_pnl_rs"] > 0.0).sum())
                row["tested_days"] = int(len(selection))
                row["min_daily_net_profit_factor"] = float(selection["net_profit_factor"].min())
            rows.append(row)
        return pd.DataFrame(rows)

    side = make_rows(side_totals, ["variant", "side"])
    setup = make_rows(setup_totals, ["variant", "side", "setup"])
    if not side.empty:
        side = side.sort_values(["side", "net_profit_factor", "trades"], ascending=[True, False, False])
    if not setup.empty:
        setup = setup.sort_values(["side", "net_profit_factor", "trades"], ascending=[True, False, False])
    if not daily.empty:
        daily = daily.sort_values(["variant", "side", "trade_date"])
    return side, setup, daily


def main() -> int:
    parser = argparse.ArgumentParser(description="Batched Hilega Milega research sweep")
    parser.add_argument("--data_root", default=str(engine.DEFAULT_DATA_ROOT))
    parser.add_argument("--universe", default="")
    parser.add_argument("--out", required=True)
    parser.add_argument("--start_date", required=True)
    parser.add_argument("--end_date", required=True)
    parser.add_argument("--capital", type=float, default=10_000.0)
    parser.add_argument("--leverage", type=float, default=5.0)
    parser.add_argument("--slippage_bps_per_side", type=float, default=5.0)
    parser.add_argument("--start_time", default="09:20")
    parser.add_argument("--end_time", default="14:30")
    parser.add_argument("--min_risk_pct", type=float, default=0.15)
    parser.add_argument("--max_risk_pct", type=float, default=2.0)
    parser.add_argument("--warmup_days", type=int, default=45)
    parser.add_argument("--min_adx", type=float, default=0.0)
    parser.add_argument("--min_line_distance", type=float, default=0.0)
    parser.add_argument("--long_min_rsi", type=float, default=0.0)
    parser.add_argument("--short_max_rsi", type=float, default=100.0)
    parser.add_argument("--setups", default=",".join(hm.ALL_SETUPS))
    parser.add_argument("--sides", default="LONG,SHORT")
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument(
        "--round",
        choices=("quick", "context", "reward", "fine_reward", "full"),
        default="quick",
    )
    parser.add_argument("--timeframes", default="5,15,60")
    args = parser.parse_args()

    selected_timeframes = {
        int(value.strip()) for value in args.timeframes.split(",") if value.strip()
    }
    if not selected_timeframes or not selected_timeframes.issubset({5, 15, 60}):
        parser.error("timeframes must contain 5, 15, and/or 60")
    variants = [
        variant
        for variant in build_variants(args.round)
        if int(variant["signal_timeframe"]) in selected_timeframes
    ]
    setups = tuple(value.strip() for value in args.setups.split(",") if value.strip())
    if not setups or not set(setups).issubset(set(hm.ALL_SETUPS)):
        parser.error("setups contains an unknown Hilega Milega setup")
    sides = tuple(value.strip().upper() for value in args.sides.split(",") if value.strip())
    if not sides or not set(sides).issubset({"LONG", "SHORT"}):
        parser.error("sides must contain LONG and/or SHORT")
    options = {
        "start_date": args.start_date,
        "end_date": args.end_date,
        "capital": args.capital,
        "leverage": args.leverage,
        "slippage_bps_per_side": args.slippage_bps_per_side,
        "start_minute": engine._parse_hhmm(args.start_time),
        "end_minute": engine._parse_hhmm(args.end_time),
        "min_risk_pct": args.min_risk_pct,
        "max_risk_pct": args.max_risk_pct,
        "warmup_days": args.warmup_days,
        "min_adx": args.min_adx,
        "min_line_distance": args.min_line_distance,
        "long_min_rsi": args.long_min_rsi,
        "short_max_rsi": args.short_max_rsi,
        "setups": setups,
        "sides": sides,
    }
    files = engine._select_data_files(Path(args.data_root), args.universe)
    payloads = [(str(path), options, variants) for path in files]
    combined: dict[tuple[str, str, str, str], dict[str, float]] = defaultdict(_empty_stat)
    all_skipped: list[dict[str, str]] = []
    started = time.time()
    workers = max(1, int(args.workers))
    executor: ProcessPoolExecutor | None = None
    iterator = map(_process_file, payloads)
    if workers > 1:
        executor = ProcessPoolExecutor(max_workers=workers)
        iterator = executor.map(_process_file, payloads, chunksize=8)
    try:
        for index, (stats, skipped) in enumerate(iterator, 1):
            for key, stat in stats.items():
                _merge_stat(combined[key], stat)
            all_skipped.extend(skipped)
            if index % 100 == 0 or index == len(payloads):
                print(
                    f"[hm_sweep] processed={index}/{len(payloads)} skipped={len(all_skipped)} "
                    f"elapsed={time.time() - started:.1f}s",
                    flush=True,
                )
    finally:
        if executor is not None:
            executor.shutdown(wait=True)

    output = Path(args.out)
    output.mkdir(parents=True, exist_ok=True)
    side, setup, daily = _rows_from_stats(dict(combined), variants)
    side.to_csv(output / "side_summary.csv", index=False)
    setup.to_csv(output / "setup_summary.csv", index=False)
    daily.to_csv(output / "daily.csv", index=False)
    pd.DataFrame(variants).to_csv(output / "variants.csv", index=False)
    pd.DataFrame(all_skipped).to_csv(output / "skipped.csv", index=False)
    print(f"[hm_sweep] wrote {output}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
