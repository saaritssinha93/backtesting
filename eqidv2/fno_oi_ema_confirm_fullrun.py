"""Full 52-day run of the two fitted 5m EMA/OI + 1m confirmation setups.

Takes the LONG and SHORT configurations selected by fno_oi_ema_confirm_sweep.py
and reports them in full: per-trade detail, per-day P&L, equity curve, drawdown,
streaks, monthly breakdown and exit mix.

It also reports a train/test split it did not fit on. The configurations were
chosen as the best of ~41k combinations over this same window, so the in-sample
numbers are optimistic by construction; the split is the only figure here that
carries any out-of-sample information.

Returns are percent per trade, equal-weighted, net of a round-trip cost. There
is no position sizing, compounding or capital constraint -- summing percentages
assumes a fixed notional per trade and unlimited concurrent capacity.
"""

from __future__ import annotations

import argparse
import sys
import time
from dataclasses import dataclass, asdict
from datetime import date
from typing import Any

import numpy as np
import pandas as pd

import fno_oi_common as common
import fno_oi_ema_confirm_sweep as sw


SESSION = "fno_oi_ema_confirm_fullrun"
RESULT_DIR = common.FNO_ROOT / "strategy_research"
REPORT_PATH = common.LATEST_DIR / "latest_fno_oi_ema_confirm_fullrun.md"


@dataclass(frozen=True)
class Config:
    side: str
    price_change_pct: float
    oi_change_pct: float
    volume_ratio: float
    body_ratio: float
    max_wick_ratio: float
    stop_pct: float
    target_pct: float


LONG_CONFIG = Config("LONG", 0.50, 0.50, 2.0, 0.60, 0.30, 0.50, 2.00)
SHORT_CONFIG = Config("SHORT", 0.50, 0.25, 3.0, 0.50, 0.30, 0.50, 1.50)


def select(signals: pd.DataFrame, cfg: Config) -> pd.DataFrame:
    price = signals["price_change_pct"]
    mask = (
        signals["side"].eq(cfg.side)
        & signals["oi_change_pct"].ge(cfg.oi_change_pct)
        & signals["volume_ratio"].ge(cfg.volume_ratio)
        & signals["body_ratio"].ge(cfg.body_ratio)
        & signals["wick_ratio"].le(cfg.max_wick_ratio)
        & (price.ge(cfg.price_change_pct) if cfg.side == "LONG" else price.le(-cfg.price_change_pct))
    )
    return signals.loc[mask].reset_index(drop=True)


def simulate_detailed(
    signals: pd.DataFrame,
    paths: dict[int, dict[str, np.ndarray]],
    cfg: Config,
    *,
    cost_bps: float,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    cost = cost_bps / 10000.0
    long_side = cfg.side == "LONG"
    for _, sig in signals.iterrows():
        path = paths.get(int(sig["sid"]))
        if path is None:
            continue
        high, low, close = path["high"], path["low"], path["close"]
        if high.size == 0:
            continue
        trigger = float(sig["trigger"])
        touched = np.flatnonzero(high >= trigger) if long_side else np.flatnonzero(low <= trigger)
        if touched.size == 0:
            rows.append({**sig.to_dict(), "filled": False})
            continue
        e = int(touched[0])
        if long_side:
            stop = trigger * (1 - cfg.stop_pct / 100.0)
            target = trigger * (1 + cfg.target_pct / 100.0)
            hit_stop = np.flatnonzero(low[e:] <= stop)
            hit_target = np.flatnonzero(high[e:] >= target)
        else:
            stop = trigger * (1 + cfg.stop_pct / 100.0)
            target = trigger * (1 - cfg.target_pct / 100.0)
            hit_stop = np.flatnonzero(high[e:] >= stop)
            hit_target = np.flatnonzero(low[e:] <= target)
        big = np.iinfo(np.int32).max
        s_i = int(hit_stop[0]) if hit_stop.size else big
        t_i = int(hit_target[0]) if hit_target.size else big
        if s_i == big and t_i == big:
            exit_price, reason, held = float(close[-1]), "SQUARE_OFF", int(close.size - e)
        elif s_i <= t_i:
            exit_price, reason, held = stop, "STOP", s_i + 1
        else:
            exit_price, reason, held = target, "TARGET", t_i + 1
        gross = (exit_price / trigger - 1.0) if long_side else (1.0 - exit_price / trigger)
        rows.append(
            {
                **sig.to_dict(),
                "filled": True,
                "entry": trigger,
                "stop": stop,
                "target": target,
                "exit": exit_price,
                "exit_reason": reason,
                "minutes_held": held,
                "gross_ret_pct": gross * 100.0,
                "net_ret_pct": (gross - cost) * 100.0,
            }
        )
    return pd.DataFrame(rows)


def stats(trades: pd.DataFrame, label: str) -> dict[str, Any]:
    filled = trades.loc[trades["filled"].fillna(False)]
    if filled.empty:
        return {"label": label, "trades": 0}
    net = filled["net_ret_pct"]
    profit = net[net > 0].sum()
    loss = -net[net < 0].sum()
    by_day = filled.groupby("day")["net_ret_pct"].sum().sort_index()
    equity = by_day.cumsum()
    dd = equity - equity.cummax()

    streak = best_streak = 0
    for value in by_day:
        streak = streak - 1 if value <= 0 else 0
        best_streak = min(best_streak, streak)
    return {
        "label": label,
        "signals": int(len(trades)),
        "trades": int(len(filled)),
        "fill_rate": round(float(len(filled) / len(trades)), 3),
        "win_rate": round(float((net > 0).mean()), 4),
        "pf": round(float(profit / loss), 3) if loss > 0 else None,
        "net_sum": round(float(net.sum()), 3),
        "net_mean": round(float(net.mean()), 4),
        "gross_mean": round(float(filled["gross_ret_pct"].mean()), 4),
        "best": round(float(net.max()), 3),
        "worst": round(float(net.min()), 3),
        "n_days": int(by_day.size),
        "days_profitable": int((by_day > 0).sum()),
        "day_win_rate": round(float((by_day > 0).mean()), 3),
        "best_day": round(float(by_day.max()), 3),
        "worst_day": round(float(by_day.min()), 3),
        "max_drawdown": round(float(dd.min()), 3),
        "longest_losing_days": int(-best_streak),
        "avg_minutes_held": round(float(filled["minutes_held"].mean()), 1),
        "trades_per_day": round(float(len(filled) / by_day.size), 2),
        "exit_mix": filled["exit_reason"].value_counts().to_dict(),
    }


def render(all_trades: pd.DataFrame, summaries: list[dict[str, Any]], meta: dict[str, Any]) -> str:
    filled = all_trades.loc[all_trades["filled"].fillna(False)]
    by_day = filled.groupby(["day", "side"])["net_ret_pct"].sum().unstack(fill_value=0.0)
    for col in ("LONG", "SHORT"):
        if col not in by_day.columns:
            by_day[col] = 0.0
    by_day["TOTAL"] = by_day["LONG"] + by_day["SHORT"]
    by_day["CUM"] = by_day["TOTAL"].cumsum()
    counts = filled.groupby(["day", "side"]).size().unstack(fill_value=0)
    for col in ("LONG", "SHORT"):
        if col not in counts.columns:
            counts[col] = 0

    lines = [
        "# 5m EMA/OI + 1m Confirmation -- Full 52-Day Run",
        "",
        f"- Generated: {common.now_ist().isoformat(timespec='seconds')}",
        f"- Window: {meta['first_day']} -> {meta['last_day']} ({meta['n_days']} sessions)",
        f"- Universe: {meta['contracts']} near-month (26AUG) futures",
        f"- Cost: {meta['cost_bps']} bps round trip | square-off 15:30",
        "",
        "> Both configurations were selected as the best of ~41,000 combinations "
        "fitted on this same window. In-sample figures are optimistic by "
        "construction. The train/test section is the only out-of-sample read.",
        "",
        "## Configurations",
        "",
        "| | LONG | SHORT |",
        "| --- | --- | --- |",
        f"| 5m price change | >= +{LONG_CONFIG.price_change_pct}% | <= -{SHORT_CONFIG.price_change_pct}% |",
        f"| 5m OI change | >= +{LONG_CONFIG.oi_change_pct}% | >= +{SHORT_CONFIG.oi_change_pct}% |",
        f"| 5m volume ratio | >= {LONG_CONFIG.volume_ratio} | >= {SHORT_CONFIG.volume_ratio} |",
        f"| 1m body/range | >= {LONG_CONFIG.body_ratio} | >= {SHORT_CONFIG.body_ratio} |",
        f"| 1m wick/range | <= {LONG_CONFIG.max_wick_ratio} | <= {SHORT_CONFIG.max_wick_ratio} |",
        f"| stop | {LONG_CONFIG.stop_pct}% | {SHORT_CONFIG.stop_pct}% |",
        f"| target | {LONG_CONFIG.target_pct}% | {SHORT_CONFIG.target_pct}% |",
        "",
        "## Headline",
        "",
        "| Metric | LONG | SHORT | COMBINED |",
        "| --- | ---: | ---: | ---: |",
    ]
    keys = [
        ("signals", "Signals"), ("trades", "Trades filled"), ("fill_rate", "Fill rate"),
        ("trades_per_day", "Trades/day"), ("win_rate", "Win rate"), ("pf", "Profit factor"),
        ("net_sum", "Net sum %"), ("net_mean", "Net mean %/trade"),
        ("gross_mean", "Gross mean %/trade"), ("best", "Best trade %"), ("worst", "Worst trade %"),
        ("n_days", "Days traded"), ("days_profitable", "Days profitable"),
        ("day_win_rate", "Day-win rate"), ("best_day", "Best day %"), ("worst_day", "Worst day %"),
        ("max_drawdown", "Max drawdown %"), ("longest_losing_days", "Longest losing streak (days)"),
        ("avg_minutes_held", "Avg minutes held"),
    ]
    lookup = {s["label"]: s for s in summaries}
    for key, title in keys:
        row = [str(lookup.get(lbl, {}).get(key, "-")) for lbl in ("LONG", "SHORT", "COMBINED")]
        lines.append(f"| {title} | {row[0]} | {row[1]} | {row[2]} |")

    lines += ["", "## Exit mix", "", "| Side | " + " | ".join(["TARGET", "STOP", "SQUARE_OFF"]) + " |",
              "| --- | ---: | ---: | ---: |"]
    for lbl in ("LONG", "SHORT", "COMBINED"):
        mix = lookup.get(lbl, {}).get("exit_mix", {})
        lines.append(f"| {lbl} | {mix.get('TARGET', 0)} | {mix.get('STOP', 0)} | {mix.get('SQUARE_OFF', 0)} |")

    monthly = filled.copy()
    monthly["month"] = pd.to_datetime(monthly["day"]).dt.strftime("%Y-%m")
    m = monthly.groupby("month").agg(
        trades=("net_ret_pct", "size"), net=("net_ret_pct", "sum"),
        win=("net_ret_pct", lambda s: (s > 0).mean()),
    )
    lines += ["", "## Monthly", "", "| Month | Trades | Net % | Win |", "| --- | ---: | ---: | ---: |"]
    for month, r in m.iterrows():
        lines.append(f"| {month} | {int(r['trades'])} | {r['net']:+.2f} | {r['win']:.0%} |")

    lines += [
        "", "## Per-day P&L (all sessions)", "",
        "| Day | L trades | S trades | LONG % | SHORT % | TOTAL % | Cumulative % |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for day in by_day.index:
        lines.append(
            f"| {day} | {int(counts.loc[day, 'LONG']) if day in counts.index else 0} | "
            f"{int(counts.loc[day, 'SHORT']) if day in counts.index else 0} | "
            f"{by_day.loc[day, 'LONG']:+.2f} | {by_day.loc[day, 'SHORT']:+.2f} | "
            f"**{by_day.loc[day, 'TOTAL']:+.2f}** | {by_day.loc[day, 'CUM']:+.2f} |"
        )

    split = meta["split_day"]
    lines += ["", "## Train / test split", "",
              f"Split at {split}: fitted parameters were chosen using the whole window, so "
              "even the test half is not clean -- treat it as the optimistic bound.", "",
              "| Window | Trades | Win | PF | Net % | Day-win |",
              "| --- | ---: | ---: | ---: | ---: | ---: |"]
    for name, sub in (("train", filled.loc[filled["day"] < split]),
                      ("test", filled.loc[filled["day"] >= split])):
        if sub.empty:
            lines.append(f"| {name} | 0 | - | - | - | - |")
            continue
        net = sub["net_ret_pct"]
        p, l = net[net > 0].sum(), -net[net < 0].sum()
        d = sub.groupby("day")["net_ret_pct"].sum()
        lines.append(
            f"| {name} | {len(sub)} | {(net > 0).mean():.0%} | "
            f"{p / l:.3f} | {net.sum():+.2f} | {(d > 0).mean():.0%} |"
        )

    top = filled.nlargest(5, "net_ret_pct")
    bot = filled.nsmallest(5, "net_ret_pct")
    for title, frame in (("Best 5 trades", top), ("Worst 5 trades", bot)):
        lines += ["", f"## {title}", "",
                  "| Day | Time | Contract | Side | Entry | Exit | Reason | Net % |",
                  "| --- | --- | --- | --- | ---: | ---: | --- | ---: |"]
        for _, r in frame.iterrows():
            lines.append(
                f"| {r['day']} | {r['hhmm']} | {r['tradingsymbol']} | {r['side']} | "
                f"{r['entry']:.2f} | {r['exit']:.2f} | {r['exit_reason']} | {r['net_ret_pct']:+.2f} |"
            )
    lines.append("")
    return "\n".join(lines)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--cost-bps", type=float, default=5.0)
    p.add_argument("--square-off", default="1530")
    p.add_argument("--max-forward-bars", type=int, default=400)
    p.add_argument("--split-day", default="", help="Train/test boundary (default: 70%% through).")
    p.add_argument("--from-date", default="", help="Only sessions on/after this date.")
    p.add_argument("--to-date", default="", help="Only sessions on/before this date.")
    p.add_argument("--tag", default="", help="Suffix for the output CSV, e.g. a forward-test label.")
    for side in ("long", "short"):
        p.add_argument(f"--{side}-price", type=float, default=None)
        p.add_argument(f"--{side}-oi", type=float, default=None)
        p.add_argument(f"--{side}-vol", type=float, default=None)
        p.add_argument(f"--{side}-body", type=float, default=None)
        p.add_argument(f"--{side}-wick", type=float, default=None)
        p.add_argument(f"--{side}-stop", type=float, default=None)
        p.add_argument(f"--{side}-target", type=float, default=None)
    return p.parse_args(argv)


def _override(cfg: Config, args: argparse.Namespace, prefix: str) -> Config:
    """CLI values replace the built-in config, so a fixed spec can be pinned."""
    field_map = {
        "price": "price_change_pct", "oi": "oi_change_pct", "vol": "volume_ratio",
        "body": "body_ratio", "wick": "max_wick_ratio",
        "stop": "stop_pct", "target": "target_pct",
    }
    values = asdict(cfg)
    for short, full in field_map.items():
        supplied = getattr(args, f"{prefix}_{short}", None)
        if supplied is not None:
            values[full] = supplied
    return Config(**values)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    started = time.monotonic()
    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    common.publish_status(SESSION, "RUNNING")

    print("[BUILD] signal superset...", flush=True)
    signals, paths = sw.build_signal_table(
        None, square_off=args.square_off, max_forward_bars=args.max_forward_bars
    )
    print(f"[BUILD] {len(signals):,} candidates", flush=True)

    long_cfg = _override(LONG_CONFIG, args, "long")
    short_cfg = _override(SHORT_CONFIG, args, "short")
    if args.from_date:
        floor = pd.Timestamp(args.from_date).date()
        signals = signals.loc[signals["day"] >= floor].reset_index(drop=True)
    if args.to_date:
        ceil = pd.Timestamp(args.to_date).date()
        signals = signals.loc[signals["day"] <= ceil].reset_index(drop=True)
    if signals.empty:
        print("[DONE] no signals in the requested window", flush=True)
        return 0

    frames = []
    summaries = []
    for cfg in (long_cfg, short_cfg):
        chosen = select(signals, cfg)
        trades = simulate_detailed(chosen, paths, cfg, cost_bps=args.cost_bps)
        trades["side"] = cfg.side
        frames.append(trades)
        summaries.append(stats(trades, cfg.side))
        print(f"[RUN] {cfg.side}: {len(chosen)} signals -> "
              f"{int(trades['filled'].fillna(False).sum())} trades", flush=True)

    all_trades = pd.concat(frames, ignore_index=True)
    summaries.append(stats(all_trades, "COMBINED"))

    days = sorted(all_trades["day"].unique())
    split_day = (
        pd.Timestamp(args.split_day).date() if args.split_day else days[int(len(days) * 0.70)]
    )
    meta = {
        "first_day": days[0], "last_day": days[-1], "n_days": len(days),
        "contracts": int(all_trades["tradingsymbol"].nunique()),
        "cost_bps": args.cost_bps, "split_day": split_day,
    }
    report = render(all_trades, summaries, meta)
    common.atomic_write_text(REPORT_PATH, report)
    suffix = f"_{args.tag}" if args.tag else ""
    common.atomic_write_csv(all_trades, RESULT_DIR / f"ema_confirm_fullrun_trades{suffix}.csv")
    print(report, flush=True)
    common.publish_status(SESSION, "SUCCESS",
                          trades=int(all_trades["filled"].fillna(False).sum()),
                          duration_sec=round(time.monotonic() - started, 1))
    print(f"[REPORT] {REPORT_PATH}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
