from __future__ import annotations

import argparse
import math
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd


IST = "Asia/Kolkata"
DEFAULT_SOURCE = Path(
    r"C:\TradingData\eqidv2\outputs_ID_v10_full_historical_v7_live_logic\trades.csv"
)
DEFAULT_ONE_MIN_DIR = Path(r"C:\TradingData\eqidv2\stocks_indicators_1min_eq")
DEFAULT_OUT_DIR = Path(r"C:\TradingData\eqidv2\v7_entry_timing_comparison")
DEFAULT_NOTIONAL_RS = 100_000.0

VARIANTS = {
    "ideal_immediate_t1": {
        "offset_min": 1,
        "description": "Ideal next 1-minute open; may be unavailable to the current scanner.",
    },
    "fast_executable_t2": {
        "offset_min": 2,
        "description": "Conservative next open after roughly 60 seconds of detection time.",
    },
    "true_5min_delay_t6": {
        "offset_min": 6,
        "description": "Wait for the next complete 5-minute candle, then enter next 1-minute open.",
    },
}


def _normalise_ts(value) -> pd.Timestamp:
    ts = pd.to_datetime(value, errors="coerce")
    if pd.isna(ts):
        return pd.NaT
    if ts.tz is None:
        return ts.tz_localize(IST)
    return ts.tz_convert(IST)


def _parse_float_list(value: str) -> list[float]:
    values = [float(item.strip()) for item in str(value).split(",") if item.strip()]
    if not values:
        raise argparse.ArgumentTypeError("at least one numeric value is required")
    return values


def _profit_factor(pnl: Iterable[float]) -> float:
    values = pd.to_numeric(pd.Series(list(pnl)), errors="coerce").fillna(0.0)
    gains = float(values[values > 0].sum())
    losses = float(-values[values < 0].sum())
    if losses <= 0:
        return math.inf if gains > 0 else 0.0
    return gains / losses


def _max_drawdown(daily_pnl: pd.Series) -> float:
    if daily_pnl.empty:
        return 0.0
    equity = pd.to_numeric(daily_pnl, errors="coerce").fillna(0.0).cumsum()
    drawdown = equity - equity.cummax()
    return float(drawdown.min())


def _daily_sharpe(daily_pnl: pd.Series) -> float:
    values = pd.to_numeric(daily_pnl, errors="coerce").fillna(0.0)
    if len(values) < 2 or float(values.std(ddof=1)) <= 0:
        return 0.0
    return float(values.mean() / values.std(ddof=1) * math.sqrt(252.0))


def _load_source(path: Path, start: str, end: str, max_trades: int) -> pd.DataFrame:
    required = [
        "candidate_id",
        "signal_time_ist",
        "ticker",
        "side",
        "setup",
        "quality_score",
        "v6_sl_pct",
        "v6_target_pct",
    ]
    header = pd.read_csv(path, nrows=0).columns
    missing = [column for column in required if column not in header]
    if missing:
        raise ValueError(f"source is missing required columns: {missing}")

    source = pd.read_csv(path, usecols=required)
    source["signal_time_ist"] = source["signal_time_ist"].map(_normalise_ts)
    source["ticker"] = source["ticker"].astype(str).str.upper().str.strip()
    source["side"] = source["side"].astype(str).str.upper().str.strip()
    source["setup"] = source["setup"].astype(str).str.strip()
    source["v6_sl_pct"] = pd.to_numeric(source["v6_sl_pct"], errors="coerce")
    source["v6_target_pct"] = pd.to_numeric(source["v6_target_pct"], errors="coerce")
    source["quality_score"] = pd.to_numeric(source["quality_score"], errors="coerce")
    source = source.dropna(
        subset=["signal_time_ist", "v6_sl_pct", "v6_target_pct"]
    )
    source = source[
        source["side"].isin(["LONG", "SHORT"])
        & source["ticker"].ne("")
        & source["setup"].ne("")
        & source["v6_sl_pct"].gt(0)
        & source["v6_target_pct"].gt(0)
    ]
    if start:
        source = source[source["signal_time_ist"] >= _normalise_ts(start)]
    if end:
        end_ts = _normalise_ts(end)
        if end_ts.hour == 0 and end_ts.minute == 0 and end_ts.second == 0:
            end_ts = end_ts + pd.Timedelta(days=1) - pd.Timedelta(microseconds=1)
        source = source[source["signal_time_ist"] <= end_ts]
    source = source.drop_duplicates(
        subset=["signal_time_ist", "ticker", "side", "setup"], keep="first"
    ).sort_values(["ticker", "signal_time_ist", "setup"])
    if max_trades > 0:
        source = source.head(max_trades)
    return source.reset_index(drop=True)


def _load_one_min(path: Path) -> pd.DataFrame | None:
    if not path.exists():
        return None
    try:
        bars = pd.read_parquet(
            path,
            columns=["date", "open", "high", "low", "close"],
        )
    except Exception:
        return None
    if bars.empty:
        return None
    bars["date"] = pd.to_datetime(bars["date"], errors="coerce")
    if getattr(bars["date"].dt, "tz", None) is None:
        bars["date"] = bars["date"].dt.tz_localize("UTC").dt.tz_convert(IST)
    else:
        bars["date"] = bars["date"].dt.tz_convert(IST)
    for column in ("open", "high", "low", "close"):
        bars[column] = pd.to_numeric(bars[column], errors="coerce")
    return (
        bars.dropna(subset=["date", "open", "high", "low", "close"])
        .drop_duplicates(subset=["date"], keep="last")
        .sort_values("date")
        .set_index("date")
    )


def _resolve(
    bars: pd.DataFrame,
    *,
    side: str,
    signal_ts: pd.Timestamp,
    offset_min: int,
    sl_pct: float,
    target_pct: float,
    slippage_bps: float,
    cost_bps: float,
    stop_extra_bps: float,
    notional_rs: float,
) -> dict | None:
    entry_ts = signal_ts + pd.Timedelta(minutes=offset_min)
    if entry_ts not in bars.index:
        return None
    entry_open = float(bars.at[entry_ts, "open"])
    if not np.isfinite(entry_open) or entry_open <= 0:
        return None

    slip = float(slippage_bps) / 10_000.0
    entry_price = entry_open * (1.0 + slip if side == "LONG" else 1.0 - slip)
    if entry_price <= 0:
        return None
    quantity = max(1, int(float(notional_rs) / entry_price))
    actual_notional = entry_price * quantity

    if side == "LONG":
        stop_price = entry_price * (1.0 - sl_pct / 100.0)
        target_price = entry_price * (1.0 + target_pct / 100.0)
    else:
        stop_price = entry_price * (1.0 + sl_pct / 100.0)
        target_price = entry_price * (1.0 - target_pct / 100.0)

    eod = entry_ts.normalize() + pd.Timedelta(hours=15, minutes=20)
    walk = bars[(bars.index >= entry_ts) & (bars.index <= eod)]
    if walk.empty:
        return None

    outcome = "EOD"
    exit_ts = pd.Timestamp(walk.index[-1])
    exit_price = float(walk.iloc[-1]["close"])
    bars_held = len(walk)

    for index, (bar_ts, bar) in enumerate(walk.iterrows(), start=1):
        if side == "LONG":
            hit_stop = float(bar["low"]) <= stop_price
            hit_target = float(bar["high"]) >= target_price
        else:
            hit_stop = float(bar["high"]) >= stop_price
            hit_target = float(bar["low"]) <= target_price
        if hit_stop:
            outcome = "SL"
            exit_ts = pd.Timestamp(bar_ts)
            exit_price = stop_price
            bars_held = index
            break
        if hit_target:
            outcome = "TARGET"
            exit_ts = pd.Timestamp(bar_ts)
            exit_price = target_price
            bars_held = index
            break

    if side == "LONG":
        gross_pnl = (exit_price - entry_price) * quantity
    else:
        gross_pnl = (entry_price - exit_price) * quantity
    charged_bps = float(cost_bps) + (
        float(stop_extra_bps) if outcome == "SL" else 0.0
    )
    cost_rs = actual_notional * charged_bps / 10_000.0
    net_pnl = gross_pnl - cost_rs
    return {
        "entry_time_ist": entry_ts,
        "raw_entry_open": entry_open,
        "entry_price": entry_price,
        "quantity": quantity,
        "notional_rs": actual_notional,
        "stop_price": stop_price,
        "target_price": target_price,
        "outcome": outcome,
        "exit_time_ist": exit_ts,
        "exit_price": exit_price,
        "bars_held": bars_held,
        "gross_pnl_rs": gross_pnl,
        "cost_rs": cost_rs,
        "net_pnl_rs": net_pnl,
    }


def _run_replay(
    source: pd.DataFrame,
    one_min_dir: Path,
    slippage_values: list[float],
    cost_bps: float,
    stop_extra_bps: float,
    notional_rs: float,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    resolved: list[dict] = []
    integrity: list[dict] = []
    total_tickers = int(source["ticker"].nunique())

    for ticker_number, (ticker, signals) in enumerate(
        source.groupby("ticker", sort=True), start=1
    ):
        path = one_min_dir / f"{ticker}_stocks_indicators_1min.parquet"
        bars = _load_one_min(path)
        missing_file = bars is None
        attempted = 0
        completed = 0
        missing_entry_bar = 0
        if bars is not None:
            min_signal = signals["signal_time_ist"].min()
            max_signal = signals["signal_time_ist"].max() + pd.Timedelta(days=1)
            bars = bars[(bars.index >= min_signal.normalize()) & (bars.index <= max_signal)]

        for _, signal in signals.iterrows():
            for variant, spec in VARIANTS.items():
                for slippage_bps in slippage_values:
                    attempted += 1
                    if bars is None:
                        continue
                    result = _resolve(
                        bars,
                        side=str(signal["side"]),
                        signal_ts=signal["signal_time_ist"],
                        offset_min=int(spec["offset_min"]),
                        sl_pct=float(signal["v6_sl_pct"]),
                        target_pct=float(signal["v6_target_pct"]),
                        slippage_bps=float(slippage_bps),
                        cost_bps=float(cost_bps),
                        stop_extra_bps=float(stop_extra_bps),
                        notional_rs=float(notional_rs),
                    )
                    if result is None:
                        missing_entry_bar += 1
                        continue
                    completed += 1
                    resolved.append(
                        {
                            **signal.to_dict(),
                            "trade_date": str(signal["signal_time_ist"].date()),
                            "variant": variant,
                            "entry_offset_min": int(spec["offset_min"]),
                            "slippage_bps": float(slippage_bps),
                            **result,
                        }
                    )
        integrity.append(
            {
                "ticker": ticker,
                "signals": int(len(signals)),
                "missing_1min_file": bool(missing_file),
                "attempted_resolutions": attempted,
                "completed_resolutions": completed,
                "missing_entry_bar_resolutions": missing_entry_bar,
            }
        )
        if ticker_number % 100 == 0 or ticker_number == total_tickers:
            print(
                f"[timing] tickers {ticker_number:,}/{total_tickers:,} "
                f"resolved rows={len(resolved):,}"
            )
    return pd.DataFrame(resolved), pd.DataFrame(integrity)


def _complete_case_rows(resolved: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    required_variants = len(VARIANTS)
    counts = (
        resolved.groupby(["candidate_id", "slippage_bps"])["variant"]
        .nunique()
        .rename("variant_count")
        .reset_index()
    )
    complete_keys = counts[counts["variant_count"] == required_variants][
        ["candidate_id", "slippage_bps"]
    ]
    complete = resolved.merge(
        complete_keys,
        on=["candidate_id", "slippage_bps"],
        how="inner",
    )
    coverage = (
        counts.groupby("slippage_bps")
        .agg(
            candidate_scenarios=("candidate_id", "count"),
            complete_candidate_scenarios=(
                "variant_count",
                lambda values: int((values == required_variants).sum()),
            ),
        )
        .reset_index()
    )
    coverage["coverage_pct"] = (
        coverage["complete_candidate_scenarios"]
        / coverage["candidate_scenarios"].clip(lower=1)
        * 100.0
    )
    return complete, coverage


def _summary(complete: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict] = []
    for (slippage_bps, variant), group in complete.groupby(
        ["slippage_bps", "variant"], sort=True
    ):
        daily = group.groupby("trade_date")["net_pnl_rs"].sum().sort_index()
        pnl = pd.to_numeric(group["net_pnl_rs"], errors="coerce").fillna(0.0)
        rows.append(
            {
                "slippage_bps": slippage_bps,
                "variant": variant,
                "entry_offset_min": int(group["entry_offset_min"].iloc[0]),
                "trades": int(len(group)),
                "days": int(group["trade_date"].nunique()),
                "win_rate_pct": float((pnl > 0).mean() * 100.0),
                "profit_factor": _profit_factor(pnl),
                "net_pnl_rs": float(pnl.sum()),
                "avg_trade_rs": float(pnl.mean()),
                "median_trade_rs": float(pnl.median()),
                "max_drawdown_rs": _max_drawdown(daily),
                "daily_sharpe": _daily_sharpe(daily),
                "day_win_rate_pct": float((daily > 0).mean() * 100.0),
                "target_rate_pct": float(
                    group["outcome"].astype(str).eq("TARGET").mean() * 100.0
                ),
                "sl_rate_pct": float(
                    group["outcome"].astype(str).eq("SL").mean() * 100.0
                ),
                "eod_rate_pct": float(
                    group["outcome"].astype(str).eq("EOD").mean() * 100.0
                ),
            }
        )
    return pd.DataFrame(rows).sort_values(["slippage_bps", "entry_offset_min"])


def _gross_summary(complete: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict] = []
    for (slippage_bps, variant), group in complete.groupby(
        ["slippage_bps", "variant"], sort=True
    ):
        daily = group.groupby("trade_date")["gross_pnl_rs"].sum().sort_index()
        pnl = pd.to_numeric(group["gross_pnl_rs"], errors="coerce").fillna(0.0)
        rows.append(
            {
                "slippage_bps": slippage_bps,
                "variant": variant,
                "entry_offset_min": int(group["entry_offset_min"].iloc[0]),
                "trades": int(len(group)),
                "gross_pnl_rs": float(pnl.sum()),
                "avg_trade_rs": float(pnl.mean()),
                "win_rate_pct": float((pnl > 0).mean() * 100.0),
                "profit_factor": _profit_factor(pnl),
                "max_drawdown_rs": _max_drawdown(daily),
            }
        )
    return pd.DataFrame(rows).sort_values(["slippage_bps", "entry_offset_min"])


def _group_summary(complete: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    rows: list[dict] = []
    keys = ["slippage_bps", "variant", *columns]
    for group_key, group in complete.groupby(keys, sort=True, dropna=False):
        key_values = group_key if isinstance(group_key, tuple) else (group_key,)
        record = dict(zip(keys, key_values))
        pnl = pd.to_numeric(group["net_pnl_rs"], errors="coerce").fillna(0.0)
        record.update(
            {
                "trades": int(len(group)),
                "win_rate_pct": float((pnl > 0).mean() * 100.0),
                "profit_factor": _profit_factor(pnl),
                "net_pnl_rs": float(pnl.sum()),
                "avg_trade_rs": float(pnl.mean()),
            }
        )
        rows.append(record)
    return pd.DataFrame(rows)


def _bootstrap_daily_delta(
    day_delta: pd.Series,
    rng: np.random.Generator,
    samples: int,
) -> tuple[float, float]:
    values = pd.to_numeric(day_delta, errors="coerce").fillna(0.0).to_numpy()
    if len(values) == 0:
        return 0.0, 0.0
    if samples <= 0:
        return float("nan"), float("nan")
    draws = rng.choice(values, size=(samples, len(values)), replace=True).mean(axis=1)
    low, high = np.quantile(draws, [0.025, 0.975])
    return float(low), float(high)


def _paired_comparisons(
    complete: pd.DataFrame,
    bootstrap_samples: int,
    seed: int,
) -> pd.DataFrame:
    comparisons = [
        ("ideal_immediate_t1", "true_5min_delay_t6"),
        ("fast_executable_t2", "true_5min_delay_t6"),
        ("ideal_immediate_t1", "fast_executable_t2"),
    ]
    rows: list[dict] = []
    rng = np.random.default_rng(seed)
    for slippage_bps, group in complete.groupby("slippage_bps", sort=True):
        pivot = group.pivot_table(
            index=["candidate_id", "trade_date"],
            columns="variant",
            values="net_pnl_rs",
            aggfunc="first",
        )
        for left, right in comparisons:
            pair = pivot[[left, right]].dropna()
            delta = pair[left] - pair[right]
            daily_delta = delta.groupby(level="trade_date").sum()
            ci_low, ci_high = _bootstrap_daily_delta(
                daily_delta, rng, bootstrap_samples
            )
            rows.append(
                {
                    "slippage_bps": slippage_bps,
                    "variant_a": left,
                    "variant_b": right,
                    "delta_definition": "variant_a_minus_variant_b",
                    "paired_trades": int(len(pair)),
                    "days": int(len(daily_delta)),
                    "net_pnl_delta_rs": float(delta.sum()),
                    "avg_trade_delta_rs": float(delta.mean()),
                    "median_trade_delta_rs": float(delta.median()),
                    "variant_a_better_pct": float((delta > 0).mean() * 100.0),
                    "variant_b_better_pct": float((delta < 0).mean() * 100.0),
                    "equal_pct": float((delta == 0).mean() * 100.0),
                    "mean_daily_delta_rs": float(daily_delta.mean()),
                    "bootstrap_95ci_mean_daily_low_rs": ci_low,
                    "bootstrap_95ci_mean_daily_high_rs": ci_high,
                }
            )
    return pd.DataFrame(rows)


def _paired_setup_comparison(
    complete: pd.DataFrame,
    *,
    slippage_bps: float,
    variant_a: str,
    variant_b: str,
    bootstrap_samples: int,
    seed: int,
) -> pd.DataFrame:
    subset = complete[complete["slippage_bps"] == float(slippage_bps)]
    pivot = subset.pivot_table(
        index=["candidate_id", "trade_date", "side", "setup"],
        columns="variant",
        values="net_pnl_rs",
        aggfunc="first",
    ).reset_index()
    rng = np.random.default_rng(seed)
    rows: list[dict] = []
    for (side, setup), group in pivot.groupby(["side", "setup"], sort=True):
        pair = group[[variant_a, variant_b]].dropna()
        delta = pair[variant_a] - pair[variant_b]
        daily_delta = (
            pd.DataFrame(
                {
                    "trade_date": group.loc[pair.index, "trade_date"],
                    "delta": delta,
                }
            )
            .groupby("trade_date")["delta"]
            .sum()
        )
        ci_low, ci_high = _bootstrap_daily_delta(
            daily_delta, rng, bootstrap_samples
        )
        rows.append(
            {
                "slippage_bps": float(slippage_bps),
                "side": side,
                "setup": setup,
                "variant_a": variant_a,
                "variant_b": variant_b,
                "paired_trades": int(len(pair)),
                "days": int(len(daily_delta)),
                "net_pnl_delta_rs": float(delta.sum()),
                "avg_trade_delta_rs": float(delta.mean()),
                "variant_a_better_pct": float((delta > 0).mean() * 100.0),
                "mean_daily_delta_rs": float(daily_delta.mean()),
                "bootstrap_95ci_mean_daily_low_rs": ci_low,
                "bootstrap_95ci_mean_daily_high_rs": ci_high,
            }
        )
    return pd.DataFrame(rows).sort_values("net_pnl_delta_rs", ascending=False)


def _markdown_table(frame: pd.DataFrame) -> str:
    if frame.empty:
        return "_No rows._"
    display = frame.copy()
    headers = [str(column) for column in display.columns]
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join(["---"] * len(headers)) + " |",
    ]
    for row in display.itertuples(index=False, name=None):
        values = [str(value).replace("|", r"\|") for value in row]
        lines.append("| " + " | ".join(values) + " |")
    return "\n".join(lines)


def _write_report(
    out_dir: Path,
    source: pd.DataFrame,
    complete: pd.DataFrame,
    coverage: pd.DataFrame,
    summary: pd.DataFrame,
    gross_summary: pd.DataFrame,
    comparisons: pd.DataFrame,
    paired_by_setup: pd.DataFrame,
    cost_bps: float,
    stop_extra_bps: float,
) -> None:
    lines = [
        "# V7 Entry Timing Comparison",
        "",
        "Timing-only paired replay using one fixed post-filter V7 signal population.",
        "",
        "## Contract",
        "",
        "- Stored 5-minute candles are end-labelled.",
        "- `T+1`: ideal immediate next 1-minute open.",
        "- `T+2`: conservative executable next open after approximately 60 seconds of detection.",
        "- `T+6`: wait for the next complete 5-minute candle, then enter.",
        f"- Base transaction cost: {cost_bps:.2f} bps.",
        f"- Additional stop cost: {stop_extra_bps:.2f} bps.",
        "- SL, target, quantity, and exits are recalculated from each variant's fill.",
        "- Same-bar SL and target collisions are resolved pessimistically as SL.",
        "",
        "## Population",
        "",
        f"- Source signals: {len(source):,}",
        f"- Date range: {source['signal_time_ist'].min()} to {source['signal_time_ist'].max()}",
        f"- Complete matched replay rows: {len(complete):,}",
        "",
        "## Coverage",
        "",
        _markdown_table(coverage.round(4)),
        "",
        "## Summary",
        "",
        _markdown_table(summary.round(4)),
        "",
        "## Before Transaction Costs",
        "",
        _markdown_table(gross_summary.round(4)),
        "",
        "## Paired Comparisons",
        "",
        _markdown_table(comparisons.round(4)),
        "",
        "## Fast Executable Versus Five-Minute Delay By Setup",
        "",
        _markdown_table(paired_by_setup.round(4)),
        "",
        "A positive delta means `variant_a` outperformed `variant_b`. A timing",
        "choice is statistically supported only when the bootstrap confidence",
        "interval excludes zero and the advantage survives all slippage cases.",
    ]
    (out_dir / "report.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def _self_check() -> None:
    index = pd.date_range(
        pd.Timestamp("2026-06-05 10:01", tz=IST),
        periods=20,
        freq="min",
    )
    bars = pd.DataFrame(
        {
            "open": 100.0,
            "high": 100.1,
            "low": 99.9,
            "close": 100.0,
        },
        index=index,
    )
    bars.loc[index[3], "high"] = 102.0
    signal = pd.Timestamp("2026-06-05 10:00", tz=IST)
    result = _resolve(
        bars,
        side="LONG",
        signal_ts=signal,
        offset_min=1,
        sl_pct=1.0,
        target_pct=1.0,
        slippage_bps=0.0,
        cost_bps=0.0,
        stop_extra_bps=0.0,
        notional_rs=100_000.0,
    )
    assert result is not None
    assert result["entry_time_ist"] == signal + pd.Timedelta(minutes=1)
    assert result["outcome"] == "TARGET"

    collision = bars.copy()
    collision.loc[index[0], "high"] = 102.0
    collision.loc[index[0], "low"] = 98.0
    result = _resolve(
        collision,
        side="LONG",
        signal_ts=signal,
        offset_min=1,
        sl_pct=1.0,
        target_pct=1.0,
        slippage_bps=0.0,
        cost_bps=0.0,
        stop_extra_bps=0.0,
        notional_rs=100_000.0,
    )
    assert result is not None and result["outcome"] == "SL"
    print("[timing] self-check passed")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Compare V7 entry timing variants on matched historical signals."
    )
    parser.add_argument("--source", type=Path, default=DEFAULT_SOURCE)
    parser.add_argument("--one-min-dir", type=Path, default=DEFAULT_ONE_MIN_DIR)
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    parser.add_argument("--start", default="")
    parser.add_argument("--end", default="")
    parser.add_argument("--max-trades", type=int, default=0)
    parser.add_argument("--notional-rs", type=float, default=DEFAULT_NOTIONAL_RS)
    parser.add_argument("--cost-bps", type=float, default=16.0)
    parser.add_argument("--stop-extra-bps", type=float, default=5.0)
    parser.add_argument("--slippage-bps", type=_parse_float_list, default=[0.0, 5.0, 10.0])
    parser.add_argument("--bootstrap-samples", type=int, default=5_000)
    parser.add_argument("--seed", type=int, default=20260607)
    parser.add_argument("--self-check", action="store_true")
    args = parser.parse_args()

    _self_check()
    if args.self_check:
        return 0

    source = _load_source(args.source, args.start, args.end, args.max_trades)
    if source.empty:
        raise SystemExit("no source signals remained after validation")
    args.out_dir.mkdir(parents=True, exist_ok=True)
    print(
        f"[timing] source signals={len(source):,} "
        f"tickers={source['ticker'].nunique():,} "
        f"range={source['signal_time_ist'].min()} to {source['signal_time_ist'].max()}"
    )

    resolved, integrity = _run_replay(
        source,
        args.one_min_dir,
        args.slippage_bps,
        args.cost_bps,
        args.stop_extra_bps,
        args.notional_rs,
    )
    if resolved.empty:
        raise SystemExit("no entry timing variants could be resolved")

    complete, coverage = _complete_case_rows(resolved)
    if complete.empty:
        raise SystemExit("no complete matched candidate set remained")
    complete["month"] = pd.to_datetime(
        complete["signal_time_ist"], errors="coerce"
    ).dt.strftime("%Y-%m")

    summary = _summary(complete)
    gross_summary = _gross_summary(complete)
    comparisons = _paired_comparisons(
        complete,
        bootstrap_samples=args.bootstrap_samples,
        seed=args.seed,
    )
    paired_by_setup = _paired_setup_comparison(
        complete,
        slippage_bps=5.0,
        variant_a="fast_executable_t2",
        variant_b="true_5min_delay_t6",
        bootstrap_samples=args.bootstrap_samples,
        seed=args.seed,
    )
    by_setup = _group_summary(complete, ["side", "setup"])
    by_side = _group_summary(complete, ["side"])
    by_month = _group_summary(complete, ["month"])

    resolved.to_csv(args.out_dir / "resolved_all.csv", index=False)
    complete.to_csv(args.out_dir / "resolved_complete_matched.csv", index=False)
    integrity.to_csv(args.out_dir / "integrity_by_ticker.csv", index=False)
    coverage.to_csv(args.out_dir / "coverage.csv", index=False)
    summary.to_csv(args.out_dir / "summary.csv", index=False)
    gross_summary.to_csv(
        args.out_dir / "summary_before_transaction_costs.csv", index=False
    )
    comparisons.to_csv(args.out_dir / "paired_comparisons.csv", index=False)
    paired_by_setup.to_csv(
        args.out_dir / "paired_by_setup_fast_vs_delay_5bps.csv", index=False
    )
    by_setup.to_csv(args.out_dir / "by_setup.csv", index=False)
    by_side.to_csv(args.out_dir / "by_side.csv", index=False)
    by_month.to_csv(args.out_dir / "by_month.csv", index=False)
    _write_report(
        args.out_dir,
        source,
        complete,
        coverage,
        summary,
        gross_summary,
        comparisons,
        paired_by_setup,
        args.cost_bps,
        args.stop_extra_bps,
    )
    print(f"[timing] wrote analysis to {args.out_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
