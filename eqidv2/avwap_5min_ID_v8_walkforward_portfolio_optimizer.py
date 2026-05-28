"""
Portfolio walk-forward optimizer for AVWAP ID 5-min v8.

This is the portfolio version of avwap_5min_ID_v8_walkforward_optimizer.py:
for each rolling window it trains on past data only, selects up to N independent
rules, then tests the locked portfolio on the next unseen window.

Default output:
  C:\\TradingData\\eqidv2\\outputs_ID_v8_walkforward_portfolio
"""

from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd

import avwap_5min_ID_v8_walkforward_optimizer as wf


DEFAULT_OUT = Path(r"C:\TradingData\eqidv2\outputs_ID_v8_walkforward_portfolio")


def _score_rules(rules: pd.DataFrame, max_train_avg_trades_day: float) -> pd.DataFrame:
    out = rules.copy()
    out["score"] = (
        out["train_pf"].clip(upper=5.0) * np.log1p(out["train_trades"])
        + out["train_day_win_pct"] / 100.0
        + out["train_pnl"] / 100000.0
        - out["train_avg_trades_day"].clip(lower=0.0) * 0.20
    )
    out = out[out["train_avg_trades_day"] <= float(max_train_avg_trades_day)].copy()
    return out.sort_values(["score", "train_pf", "train_pnl", "train_trades"], ascending=[False, False, False, False])


def _portfolio_trades(df: pd.DataFrame, rules: list[dict], pnl_col: str) -> pd.DataFrame:
    frames = []
    for index, rule in enumerate(rules, 1):
        subset = wf._apply_rule(df, rule, pnl_col=pnl_col)
        if subset.empty:
            continue
        subset = subset.copy()
        subset["portfolio_rule_rank"] = index
        subset["portfolio_rule"] = rule["rule"]
        frames.append(subset)
    if not frames:
        return pd.DataFrame()
    out = pd.concat(frames, ignore_index=True)
    out["_rank"] = pd.to_numeric(out["portfolio_rule_rank"], errors="coerce").fillna(999)
    sort_cols = ["trade_date", "ticker", "_rank", "setup"]
    out = out.sort_values(sort_cols).drop_duplicates(subset=["trade_date", "ticker"], keep="first")
    return out.drop(columns=["_rank"], errors="ignore").reset_index(drop=True)


def _select_portfolio(
    train: pd.DataFrame,
    rules: pd.DataFrame,
    *,
    max_rules: int,
    min_rule_pf: float,
    min_rule_trades: int,
    min_portfolio_pf: float,
    max_train_avg_trades_day: float,
) -> list[dict]:
    eligible = rules[
        (rules["train_trades"] >= int(min_rule_trades))
        & (rules["train_pf"] >= float(min_rule_pf))
        & (rules["train_pnl"] > 0)
    ].copy()
    if eligible.empty:
        return []
    eligible = _score_rules(eligible, max_train_avg_trades_day)

    selected: list[dict] = []
    selected_keys: set[tuple[str, str]] = set()
    current = pd.DataFrame()
    current_metrics = wf._metrics(current, wf.PNL_COL)

    for _, row in eligible.iterrows():
        rule = row.to_dict()
        key = (str(rule["side"]), str(rule["setup"]))
        if key in selected_keys:
            continue
        trial_rules = [*selected, rule]
        trial = _portfolio_trades(train, trial_rules, wf.PNL_COL)
        trial_metrics = wf._metrics(trial, wf.PNL_COL)
        if not selected:
            if trial_metrics["pf"] < min_portfolio_pf or trial_metrics["pnl"] <= 0:
                continue
        else:
            if trial_metrics["pf"] < min_portfolio_pf:
                continue
            # Require the added rule to improve either PnL or sample size without
            # damaging PF too much. This keeps the portfolio honest but not frozen.
            improves_pnl = trial_metrics["pnl"] > current_metrics["pnl"]
            improves_count = trial_metrics["trades"] >= current_metrics["trades"] * 1.10
            if not (improves_pnl or improves_count):
                continue

        selected.append(rule)
        selected_keys.add(key)
        current = trial
        current_metrics = trial_metrics
        if len(selected) >= int(max_rules):
            break

    return selected


def _write_rule_rows(window: int, rules: list[dict]) -> list[dict]:
    rows = []
    for rank, rule in enumerate(rules, 1):
        row = {"window": window, "rank": rank}
        row.update(rule)
        rows.append(row)
    return rows


def run(args: argparse.Namespace) -> int:
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    pool = wf._load_historical_pool(Path(args.source_dir))
    pool.to_csv(out_dir / "historical_pool.csv", index=False)

    dates = sorted(pool["trade_date"].dropna().unique().tolist())
    windows = wf._window_dates(dates, int(args.train_days), int(args.test_days), int(args.step_days))
    if not windows:
        raise SystemExit("Not enough dates for requested walk-forward windows")

    summaries = []
    selected_rule_rows = []
    train_frames = []
    test_frames = []

    for window, (train_dates, test_dates) in enumerate(windows, 1):
        train = pool[pool["trade_date"].isin(train_dates)].copy()
        test = pool[pool["trade_date"].isin(test_dates)].copy()
        rules = wf._candidate_rules(train, min_train_trades=int(args.min_rule_trades))
        selected = _select_portfolio(
            train,
            rules,
            max_rules=int(args.max_rules),
            min_rule_pf=float(args.min_rule_pf),
            min_rule_trades=int(args.min_rule_trades),
            min_portfolio_pf=float(args.min_portfolio_pf),
            max_train_avg_trades_day=float(args.max_rule_avg_trades_day),
        )
        if not selected:
            summaries.append({
                "window": window,
                "train_start": train_dates[0],
                "train_end": train_dates[-1],
                "test_start": test_dates[0],
                "test_end": test_dates[-1],
                "status": "NO_RULE",
            })
            continue

        selected_rule_rows.extend(_write_rule_rows(window, selected))
        train_selected = _portfolio_trades(train, selected, wf.PNL_COL)
        test_selected = _portfolio_trades(test, selected, wf.PNL_COL)
        if not train_selected.empty:
            train_selected = train_selected.copy()
            train_selected["walk_window"] = window
            train_frames.append(train_selected)
        if not test_selected.empty:
            test_selected = test_selected.copy()
            test_selected["walk_window"] = window
            test_frames.append(test_selected)

        train_metrics = wf._metrics(train_selected, wf.PNL_COL)
        test_metrics = wf._metrics(test_selected, wf.PNL_COL)
        summaries.append({
            "window": window,
            "train_start": train_dates[0],
            "train_end": train_dates[-1],
            "test_start": test_dates[0],
            "test_end": test_dates[-1],
            "status": "OK",
            "rules": len(selected),
            "rule_set": " | ".join(f"{r['side']} {r['setup']}: {r['rule']}" for r in selected),
            **{f"train_{k}": v for k, v in train_metrics.items()},
            **{f"test_{k}": v for k, v in test_metrics.items()},
        })
        print(
            f"[{window:02d}/{len(windows)}] {test_dates[0]}->{test_dates[-1]} "
            f"rules={len(selected)} test_n={test_metrics['trades']} "
            f"test_pf={test_metrics['pf']:.3f} pnl={test_metrics['pnl']:.0f}"
        )

    summary = pd.DataFrame(summaries)
    selected_rules = pd.DataFrame(selected_rule_rows)
    train_trades = pd.concat(train_frames, ignore_index=True) if train_frames else pd.DataFrame()
    test_trades = pd.concat(test_frames, ignore_index=True) if test_frames else pd.DataFrame()

    summary.to_csv(out_dir / "walkforward_portfolio_windows.csv", index=False)
    selected_rules.to_csv(out_dir / "selected_portfolio_rules.csv", index=False)
    train_trades.to_csv(out_dir / "walkforward_portfolio_train_trades.csv", index=False)
    test_trades.to_csv(out_dir / "walkforward_portfolio_test_trades.csv", index=False)

    overall = wf._metrics(test_trades, wf.PNL_COL)
    by_setup = (
        test_trades.groupby(["side", "setup"], dropna=False)
        .apply(lambda g: pd.Series(wf._metrics(g, wf.PNL_COL)), include_groups=False)
        .reset_index()
        .sort_values("pnl", ascending=False)
        if not test_trades.empty
        else pd.DataFrame()
    )
    by_setup.to_csv(out_dir / "walkforward_portfolio_by_setup.csv", index=False)

    today_metrics = wf._metrics(pd.DataFrame(), wf.TODAY_PNL_COL)
    today_trades = pd.DataFrame()
    latest_rules = []
    if not selected_rules.empty:
        latest_window = int(selected_rules["window"].max())
        latest_rules = selected_rules[selected_rules["window"].eq(latest_window)].sort_values("rank").to_dict("records")

    valid_combos = set(
        tuple(x)
        for x in pool[["side", "setup", "v6_sl_pct", "v6_target_pct"]]
        .drop_duplicates()
        .itertuples(index=False, name=None)
    )
    today = wf._normalise_today_grid(Path(args.today_grid), valid_combos)
    if latest_rules and not today.empty:
        today_trades = _portfolio_trades(today, latest_rules, wf.TODAY_PNL_COL)
        today_trades.to_csv(out_dir / "today_holdout_trades_latest_portfolio.csv", index=False)
        today_metrics = wf._metrics(today_trades, wf.TODAY_PNL_COL)

    lines = [
        "AVWAP ID 5-min v8 portfolio walk-forward optimizer",
        "=" * 80,
        f"source_dir={args.source_dir}",
        f"windows={len(windows)} train_days={args.train_days} test_days={args.test_days} step_days={args.step_days}",
        f"max_rules={args.max_rules} min_rule_pf={args.min_rule_pf} min_portfolio_pf={args.min_portfolio_pf}",
        "",
        "Walk-forward unseen test result:",
        f"Trades            : {overall['trades']:,}",
        f"Days              : {overall['days']:,}",
        f"Avg trades/day    : {overall['avg_trades_day']:.2f}",
        f"PF                : {overall['pf']:.3f}",
        f"PnL               : Rs {overall['pnl']:,.2f}",
        f"Win %             : {overall['win_pct']:.2f}%",
        f"Day win %         : {overall['day_win_pct']:.2f}%",
        f"Max day loss      : Rs {overall['max_day_loss']:,.2f}",
        "",
        "Today holdout using latest selected portfolio:",
        f"Trades            : {today_metrics['trades']:,}",
        f"PF                : {today_metrics['pf']:.3f}",
        f"PnL               : Rs {today_metrics['pnl']:,.2f}",
        f"Win %             : {today_metrics['win_pct']:.2f}%",
    ]
    if latest_rules:
        lines.append("Latest rules:")
        for rule in latest_rules:
            lines.append(
                f"  {int(rule['rank'])}. {rule['side']} {rule['setup']} "
                f"SL={float(rule['sl_pct']):.2f}% TGT={float(rule['target_pct']):.2f}% :: {rule['rule']}"
            )

    text = "\n".join(lines)
    print(text)
    (out_dir / "summary.txt").write_text(text + "\n", encoding="utf-8")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description="Portfolio walk-forward optimizer for AVWAP ID 5-min v8")
    ap.add_argument("--source_dir", default=str(wf.DEFAULT_SOURCE_DIR))
    ap.add_argument("--today_grid", default=str(wf.DEFAULT_TODAY_GRID))
    ap.add_argument("--out", default=str(DEFAULT_OUT))
    ap.add_argument("--train_days", type=int, default=60)
    ap.add_argument("--test_days", type=int, default=10)
    ap.add_argument("--step_days", type=int, default=10)
    ap.add_argument("--max_rules", type=int, default=3)
    ap.add_argument("--min_rule_trades", type=int, default=40)
    ap.add_argument("--min_rule_pf", type=float, default=1.4)
    ap.add_argument("--min_portfolio_pf", type=float, default=1.8)
    ap.add_argument("--max_rule_avg_trades_day", type=float, default=3.0)
    args = ap.parse_args()
    return run(args)


if __name__ == "__main__":
    raise SystemExit(main())
