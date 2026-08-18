"""Intraday long/short research harness over the FnO 5-minute OI rank history.

Setups are declarative filters on the ranked feature panel. Each signal enters
at the **next bar's open** (never the signal bar's close -- that price is not
tradeable once the bar has printed) and exits at the first of: target, stop,
max holding bars, or session end.

Design notes that exist because of how this data behaves:

* **Liquidity is not optional.** 25% of 5-minute contract bars trade zero value
  and the median bar is ~Rs 12 lakh. An unfiltered backtest fills at prices that
  do not exist. ``min_traded_value`` defaults to Rs 1 crore per bar.
* **Costs dominate.** The unconditional edge by OI classification is 1-7 bps
  while a round trip costs ~5 bps. Everything is reported net; ``--cost-bps``
  is round-trip and applied to every trade.
* **Stops resolve pessimistically.** If a bar's high and low would trigger both
  target and stop, the stop is taken. Intrabar sequence is unknowable at 5-min
  resolution, so the harness assumes the worse fill.
* **Day concentration is reported, not hidden.** This repo has a documented
  history of setups whose entire edge was one crash day. Every result carries
  the top-2-day share of gross profit and a per-day win rate.

52 trading days is a small sample. Treat everything here as a hypothesis with
an effect size, not a validated edge.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Any, Callable

import numpy as np
import pandas as pd

import fno_oi_common as common


SESSION = "fno_oi_intraday_strategies"

RANK_HISTORY_DIR = common.FNO_ROOT / "rank_history"
RESEARCH_DIR = common.FNO_ROOT / "strategy_research"
REPORT_PATH = common.LATEST_DIR / "latest_fno_oi_intraday_strategies.md"

DEFAULT_COST_BPS = 5.0
DEFAULT_MIN_TRADED_VALUE = 1e7  # Rs 1 crore in the signal bar

LOAD_COLUMNS = [
    "timestamp",
    "session_date",
    "tradingsymbol",
    "underlying",
    "contract_month",
    "is_front_month",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "oi",
    "traded_value_5m",
    "volume_ratio",
    "oi_change_pct_5m",
    "oi_change_pct_15m",
    "price_change_pct_5m",
    "price_change_pct_day",
    "classification",
    "eligible_for_rank",
    "oi_rank_5m",
    "oi_zscore_20",
    "activity_score",
    "day_volume",
]


@dataclass(frozen=True)
class Setup:
    name: str
    side: str  # "LONG" or "SHORT"
    rationale: str
    predicate: Callable[[pd.DataFrame], pd.Series]
    target_pct: float = 0.40
    stop_pct: float = 0.30
    max_bars: int = 6
    entry_from: str = "0930"
    entry_to: str = "1500"


def _hhmm(frame: pd.DataFrame) -> pd.Series:
    return frame["ist"].dt.strftime("%H%M")


# ---------------------------------------------------------------------------
# Setup definitions.
#
# Direction follows what the data showed, not the textbook. Over this sample
# fresh positioning is faded: LONG_BUILDUP precedes negative returns
# (monotonically stronger with OI size) and LONG_UNWINDING precedes positive
# ones. Each setup below is one hypothesis, deliberately few, to keep the
# multiple-comparisons burden low.
# ---------------------------------------------------------------------------

SETUPS: tuple[Setup, ...] = (
    Setup(
        name="S1_FADE_LONG_BUILDUP",
        side="SHORT",
        rationale=(
            "Large fresh long buildup in a liquid contract mean-reverts. Edge is "
            "monotonic in OI size (>1% was -4.7bps/30min unconditionally)."
        ),
        predicate=lambda d: (
            d["classification"].eq("LONG_BUILDUP")
            & d["oi_change_pct_5m"].ge(1.0)
        ),
        target_pct=0.40,
        stop_pct=0.30,
        max_bars=6,
    ),
    Setup(
        name="S2_FADE_LONG_BUILDUP_PM",
        side="SHORT",
        rationale=(
            "Same fade restricted to the afternoon, where the unconditional "
            "effect was strongest (14:xx = -8.5bps/30min)."
        ),
        predicate=lambda d: (
            d["classification"].eq("LONG_BUILDUP")
            & d["oi_change_pct_5m"].ge(0.75)
            & _hhmm(d).between("1300", "1500")
        ),
        target_pct=0.40,
        stop_pct=0.30,
        max_bars=6,
    ),
    Setup(
        name="L1_LONG_UNWINDING",
        side="LONG",
        rationale=(
            "Longs exiting into a liquid tape preceded the largest positive "
            "drift in the sample (+6.9bps/30min, +13.7 when OI fell >1%)."
        ),
        predicate=lambda d: (
            d["classification"].eq("LONG_UNWINDING")
            & d["oi_change_pct_5m"].le(-0.5)
        ),
        target_pct=0.40,
        stop_pct=0.30,
        max_bars=6,
    ),
    Setup(
        name="L2_FADE_SHORT_BUILDUP",
        side="LONG",
        rationale=(
            "Fresh shorts in a liquid contract drifted up (+1.9bps/30min, "
            "+3.1 in the smaller-OI bucket). Weakest of the four by effect size."
        ),
        predicate=lambda d: (
            d["classification"].eq("SHORT_BUILDUP")
            & d["oi_change_pct_5m"].between(0.25, 0.75)
        ),
        target_pct=0.35,
        stop_pct=0.30,
        max_bars=6,
    ),
    Setup(
        name="S3_FADE_LONG_BUILDUP_VOLSPIKE",
        side="SHORT",
        rationale=(
            "Long buildup arriving on a volume spike: participation confirms the "
            "positioning is real rather than a thin print."
        ),
        predicate=lambda d: (
            d["classification"].eq("LONG_BUILDUP")
            & d["oi_change_pct_5m"].ge(0.75)
            & d["volume_ratio"].ge(2.0)
        ),
        target_pct=0.40,
        stop_pct=0.30,
        max_bars=6,
    ),
)


def load_panel(
    *,
    contract_month: str,
    from_date: str,
    to_date: str,
    front_only: bool,
) -> pd.DataFrame:
    days = sorted(p for p in RANK_HISTORY_DIR.iterdir() if p.is_dir())
    frames = []
    for day_dir in days:
        path = day_dir / f"rankings_{day_dir.name}.parquet"
        if not path.exists():
            continue
        frames.append(pd.read_parquet(path, columns=LOAD_COLUMNS))
    if not frames:
        raise FileNotFoundError(f"No rank history under {RANK_HISTORY_DIR}")
    panel = pd.concat(frames, ignore_index=True)

    if contract_month:
        panel = panel.loc[panel["contract_month"].eq(contract_month)]
    if front_only:
        panel = panel.loc[panel["is_front_month"].fillna(False)]
    panel["timestamp"] = pd.to_datetime(panel["timestamp"], utc=True)
    panel["ist"] = panel["timestamp"].dt.tz_convert(common.IST)
    panel["session_date"] = pd.to_datetime(panel["session_date"]).dt.date
    if from_date:
        panel = panel.loc[panel["session_date"] >= pd.Timestamp(from_date).date()]
    if to_date:
        panel = panel.loc[panel["session_date"] <= pd.Timestamp(to_date).date()]
    return panel.sort_values(["tradingsymbol", "timestamp"]).reset_index(drop=True)


def simulate(
    panel: pd.DataFrame,
    setup: Setup,
    *,
    cost_bps: float,
    min_traded_value: float,
) -> pd.DataFrame:
    """Bracketed intraday simulation; entry at the bar after the signal."""

    work = panel.copy()
    work["hhmm"] = _hhmm(work)
    eligible = (
        work["eligible_for_rank"].fillna(False).astype(bool)
        & work["traded_value_5m"].ge(min_traded_value)
        & work["hhmm"].between(setup.entry_from, setup.entry_to)
    )
    signal = eligible & setup.predicate(work).fillna(False)
    if not signal.any():
        return pd.DataFrame()

    trades: list[dict[str, Any]] = []
    long_side = setup.side.upper() == "LONG"
    cost_frac = float(cost_bps) / 10000.0

    for symbol, chunk in work.groupby("tradingsymbol", sort=False):
        mask = signal.loc[chunk.index]
        if not mask.any():
            continue
        chunk = chunk.reset_index(drop=True)
        mask = mask.to_numpy()
        o = chunk["open"].to_numpy(float)
        h = chunk["high"].to_numpy(float)
        low = chunk["low"].to_numpy(float)
        c = chunk["close"].to_numpy(float)
        sess = chunk["session_date"].to_numpy()
        stamps = chunk["ist"].to_numpy()
        n = len(chunk)

        for i in np.flatnonzero(mask):
            entry_idx = i + 1
            if entry_idx >= n or sess[entry_idx] != sess[i]:
                continue  # signal on the last bar of the session
            entry = o[entry_idx]
            if not np.isfinite(entry) or entry <= 0:
                continue

            if long_side:
                target = entry * (1 + setup.target_pct / 100.0)
                stop = entry * (1 - setup.stop_pct / 100.0)
            else:
                target = entry * (1 - setup.target_pct / 100.0)
                stop = entry * (1 + setup.stop_pct / 100.0)

            exit_price = np.nan
            exit_idx = entry_idx
            reason = "TIME"
            last = min(entry_idx + setup.max_bars - 1, n - 1)
            for j in range(entry_idx, last + 1):
                if sess[j] != sess[i]:
                    exit_idx = j - 1
                    exit_price = c[exit_idx]
                    reason = "SESSION_END"
                    break
                hit_stop = (low[j] <= stop) if long_side else (h[j] >= stop)
                hit_target = (h[j] >= target) if long_side else (low[j] <= target)
                # Pessimistic: if both are touched in one bar, take the stop.
                if hit_stop:
                    exit_idx, exit_price, reason = j, stop, "STOP"
                    break
                if hit_target:
                    exit_idx, exit_price, reason = j, target, "TARGET"
                    break
                exit_idx, exit_price = j, c[j]
            if not np.isfinite(exit_price):
                continue

            gross = (exit_price / entry - 1.0) if long_side else (1.0 - exit_price / entry)
            net = gross - cost_frac
            trades.append(
                {
                    "setup": setup.name,
                    "side": setup.side,
                    "tradingsymbol": symbol,
                    "underlying": chunk["underlying"].iloc[i],
                    "session_date": sess[i],
                    "signal_ts": stamps[i],
                    "entry_ts": stamps[entry_idx],
                    "exit_ts": stamps[exit_idx],
                    "entry": entry,
                    "exit": exit_price,
                    "bars_held": int(exit_idx - entry_idx + 1),
                    "exit_reason": reason,
                    "gross_ret_pct": gross * 100.0,
                    "net_ret_pct": net * 100.0,
                    "traded_value_5m": chunk["traded_value_5m"].iloc[i],
                    "oi_change_pct_5m": chunk["oi_change_pct_5m"].iloc[i],
                    "is_front_month": bool(chunk["is_front_month"].iloc[i]),
                }
            )
    return pd.DataFrame(trades)


def summarize(trades: pd.DataFrame, label: str) -> dict[str, Any]:
    if trades.empty:
        return {"label": label, "trades": 0}
    net = trades["net_ret_pct"]
    wins = net.gt(0)
    gross_profit = net[net > 0].sum()
    gross_loss = -net[net < 0].sum()
    by_day = trades.groupby("session_date")["net_ret_pct"].sum().sort_index()
    equity = by_day.cumsum()
    drawdown = (equity - equity.cummax()).min()
    day_profit = by_day[by_day > 0].sum()
    top2 = by_day.nlargest(2).sum()
    return {
        "label": label,
        "trades": int(len(trades)),
        "days": int(by_day.size),
        "trades_per_day": round(len(trades) / max(1, by_day.size), 2),
        "win_rate": round(float(wins.mean()), 4),
        "net_sum_pct": round(float(net.sum()), 3),
        "net_mean_bps": round(float(net.mean() * 100), 2),
        "gross_mean_bps": round(float(trades["gross_ret_pct"].mean() * 100), 2),
        "profit_factor": round(float(gross_profit / gross_loss), 3) if gross_loss > 0 else None,
        "day_win_rate": round(float((by_day > 0).mean()), 4),
        "max_dd_pct": round(float(drawdown), 3),
        "top2_day_share": round(float(top2 / day_profit), 3) if day_profit > 0 else None,
        "avg_bars_held": round(float(trades["bars_held"].mean()), 2),
        "exit_mix": trades["exit_reason"].value_counts().to_dict(),
    }


def split_train_test(trades: pd.DataFrame, split_date: date) -> tuple[pd.DataFrame, pd.DataFrame]:
    if trades.empty:
        return trades, trades
    train = trades.loc[trades["session_date"] < split_date]
    test = trades.loc[trades["session_date"] >= split_date]
    return train, test


def _verdict(train: dict[str, Any], test: dict[str, Any]) -> str:
    """Deliberately strict: this repo's failure mode is promoting noise."""

    if test.get("trades", 0) < 30 or train.get("trades", 0) < 30:
        return "REJECT (sample too small)"
    tr_pf, te_pf = train.get("profit_factor"), test.get("profit_factor")
    if tr_pf is None or te_pf is None:
        return "REJECT (no losses/profits to compare)"
    if tr_pf < 1.10 or te_pf < 1.10:
        return "REJECT (net PF below 1.10 in a window)"
    top2 = test.get("top2_day_share")
    if top2 is not None and top2 > 0.60:
        return "REJECT (day-concentrated: top-2 days carry the edge)"
    if test.get("day_win_rate", 0) < 0.45:
        return "REJECT (day-win rate too low)"
    return "CANDIDATE"


def render_report(results: list[dict[str, Any]], meta: dict[str, Any]) -> str:
    lines = [
        "# FnO Intraday OI Strategies -- Research Result",
        "",
        f"- Generated: {common.now_ist().isoformat(timespec='seconds')}",
        f"- Panel: {meta['contract_month'] or 'all months'}"
        f"{' (front-month only)' if meta['front_only'] else ''}",
        f"- Window: {meta['first_day']} -> {meta['last_day']} ({meta['days']} trading days)",
        f"- Train/test split: {meta['split_date']}",
        f"- Round-trip cost: {meta['cost_bps']} bps",
        f"- Min traded value per signal bar: Rs {meta['min_traded_value']:,.0f}",
        f"- Rows evaluated: {meta['rows']:,}",
        "",
        "All returns are **net of costs**. Entry is the bar after the signal.",
        "When a bar touches both target and stop, the stop is taken.",
        "",
        "## Verdicts",
        "",
        "| Setup | Side | Verdict | Trades | Net PF (train / test) | Test net bps/trade | Test day-win | Top-2-day share |",
        "| --- | --- | --- | ---: | --- | ---: | ---: | ---: |",
    ]
    for res in results:
        tr, te = res["train"], res["test"]
        pf = f"{tr.get('profit_factor')} / {te.get('profit_factor')}"
        lines.append(
            f"| {res['name']} | {res['side']} | {res['verdict']} | "
            f"{res['all'].get('trades', 0):,} | {pf} | "
            f"{te.get('net_mean_bps', float('nan'))} | "
            f"{te.get('day_win_rate', float('nan'))} | "
            f"{te.get('top2_day_share', float('nan'))} |"
        )

    lines += ["", "## Detail", ""]
    for res in results:
        lines += [
            f"### {res['name']} ({res['side']}) -- {res['verdict']}",
            "",
            f"*{res['rationale']}*",
            "",
            f"Bracket: target {res['target_pct']}% / stop {res['stop_pct']}% / "
            f"max {res['max_bars']} bars.",
            "",
            "| Window | Trades | /day | Win | Net sum % | Net bps | Gross bps | PF | Day-win | MaxDD % | Top-2-day |",
            "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
        ]
        for window in ("all", "train", "test"):
            s = res[window]
            if not s.get("trades"):
                lines.append(f"| {window} | 0 | - | - | - | - | - | - | - | - | - |")
                continue
            lines.append(
                f"| {window} | {s['trades']:,} | {s['trades_per_day']} | "
                f"{s['win_rate']:.1%} | {s['net_sum_pct']} | {s['net_mean_bps']} | "
                f"{s['gross_mean_bps']} | {s['profit_factor']} | "
                f"{s['day_win_rate']:.1%} | {s['max_dd_pct']} | {s['top2_day_share']} |"
            )
        exits = res["all"].get("exit_mix", {})
        if exits:
            lines += ["", f"Exit mix: {exits}", ""]
        lines.append("")

    lines += [
        "## How to read this",
        "",
        f"- The sample is {meta['days']} trading days. That is small. A CANDIDATE "
        "verdict means 'survived the checks', not 'validated'.",
        "- Gross vs net bps shows how much of each edge the cost assumption eats. "
        "Where they straddle zero, the setup is a cost artefact either way.",
        "- Top-2-day share above 0.60 means most of the profit came from two "
        "sessions, which this repo has repeatedly found to be an artefact.",
        "",
    ]
    if meta["front_only"]:
        lines.append(
            "- Front-month only: results reflect the live-tradeable contract."
        )
    else:
        lines.append(
            "- **Not front-month restricted.** For most of this window the ranked "
            "contract was a next/far month, which is thinner than what live "
            "trades. Re-run with `--front-only` to see the tradeable subset."
        )
    return "\n".join(lines) + "\n"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--contract-month", default="2026-08", help="e.g. 2026-08. Empty = all.")
    parser.add_argument("--front-only", action="store_true", help="Restrict to front-month rows.")
    parser.add_argument("--from-date", default="")
    parser.add_argument("--to-date", default="")
    parser.add_argument("--split-date", default="", help="Train < date <= test. Default: 70%% through.")
    parser.add_argument("--cost-bps", type=float, default=DEFAULT_COST_BPS)
    parser.add_argument("--min-traded-value", type=float, default=DEFAULT_MIN_TRADED_VALUE)
    parser.add_argument("--setups", default="", help="Comma-separated subset of setup names.")
    parser.add_argument(
        "--hold-bars", type=int, default=0,
        help="Override every setup's max holding bars (sensitivity testing).",
    )
    parser.add_argument(
        "--target-pct", type=float, default=0.0,
        help="Override every setup's target %% (sensitivity testing).",
    )
    parser.add_argument(
        "--stop-pct", type=float, default=0.0,
        help="Override every setup's stop %% (sensitivity testing).",
    )
    parser.add_argument("--save-trades", action="store_true", help="Write per-trade CSVs.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    started = time.monotonic()
    RESEARCH_DIR.mkdir(parents=True, exist_ok=True)

    panel = load_panel(
        contract_month=args.contract_month,
        from_date=args.from_date,
        to_date=args.to_date,
        front_only=args.front_only,
    )
    if panel.empty:
        print("[PLAN] Panel is empty.", flush=True)
        return 0

    days = sorted(panel["session_date"].unique())
    if args.split_date:
        split_date = pd.Timestamp(args.split_date).date()
    else:
        split_date = days[int(len(days) * 0.70)]
    print(
        f"[PANEL] {len(panel):,} rows | {len(days)} days "
        f"({days[0]} -> {days[-1]}) | split {split_date}",
        flush=True,
    )

    wanted = {s.strip() for s in args.setups.split(",") if s.strip()}
    setups = [s for s in SETUPS if not wanted or s.name in wanted]
    overrides: dict[str, Any] = {}
    if args.hold_bars > 0:
        overrides["max_bars"] = args.hold_bars
    if args.target_pct > 0:
        overrides["target_pct"] = args.target_pct
    if args.stop_pct > 0:
        overrides["stop_pct"] = args.stop_pct
    if overrides:
        from dataclasses import replace as _replace

        setups = [_replace(s, **overrides) for s in setups]
        print(f"[OVERRIDE] {overrides}", flush=True)

    common.publish_status(SESSION, "RUNNING", setups=len(setups), rows=int(len(panel)))
    results: list[dict[str, Any]] = []
    for setup in setups:
        print(f"[SIM] {setup.name} ({setup.side})...", flush=True)
        trades = simulate(
            panel,
            setup,
            cost_bps=args.cost_bps,
            min_traded_value=args.min_traded_value,
        )
        train, test = split_train_test(trades, split_date)
        summary_all = summarize(trades, "all")
        summary_train = summarize(train, "train")
        summary_test = summarize(test, "test")
        verdict = _verdict(summary_train, summary_test)
        results.append(
            {
                "name": setup.name,
                "side": setup.side,
                "rationale": setup.rationale,
                "target_pct": setup.target_pct,
                "stop_pct": setup.stop_pct,
                "max_bars": setup.max_bars,
                "all": summary_all,
                "train": summary_train,
                "test": summary_test,
                "verdict": verdict,
            }
        )
        print(
            f"       trades={summary_all.get('trades', 0):,} "
            f"net_bps={summary_all.get('net_mean_bps')} "
            f"PF_train={summary_train.get('profit_factor')} "
            f"PF_test={summary_test.get('profit_factor')} -> {verdict}",
            flush=True,
        )
        if args.save_trades and not trades.empty:
            common.atomic_write_csv(trades, RESEARCH_DIR / f"trades_{setup.name}.csv")

    meta = {
        "contract_month": args.contract_month,
        "front_only": args.front_only,
        "first_day": days[0],
        "last_day": days[-1],
        "days": len(days),
        "split_date": split_date,
        "cost_bps": args.cost_bps,
        "min_traded_value": args.min_traded_value,
        "rows": int(len(panel)),
    }
    report = render_report(results, meta)
    common.atomic_write_text(REPORT_PATH, report)
    common.atomic_write_text(
        RESEARCH_DIR / "latest_results.json",
        json.dumps({"meta": {k: str(v) for k, v in meta.items()}, "results": results},
                   indent=2, default=str),
    )

    candidates = [r["name"] for r in results if r["verdict"] == "CANDIDATE"]
    duration = time.monotonic() - started
    common.publish_status(
        SESSION, "SUCCESS", setups=len(results), candidates=len(candidates),
        duration_sec=round(duration, 2),
    )
    print(f"\n[DONE] {len(results)} setups in {duration:.1f}s | candidates: {candidates or 'none'}", flush=True)
    print(f"[REPORT] {REPORT_PATH}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
