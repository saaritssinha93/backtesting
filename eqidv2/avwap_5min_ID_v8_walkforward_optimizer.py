"""
Honest walk-forward optimizer for AVWAP ID 5-min v8.

This script trains simple signal-time filters on past v8-resolved trades and
tests the selected rule on the next unseen window. It does not use today's PnL
to choose a rule.

Default output:
  C:\\TradingData\\eqidv2\\outputs_ID_v8_walkforward
"""

from __future__ import annotations

import argparse
import math
from pathlib import Path

import numpy as np
import pandas as pd


DEFAULT_SOURCE_DIR = Path(r"C:\TradingData\eqidv2\outputs_ID_v8_5min_research_restore")
DEFAULT_TODAY_GRID = Path(r"C:\TradingData\eqidv2\outputs_ID_v8_today_iteration_2026-05-21\all_candidate_exit_grid.csv")
DEFAULT_OUT = Path(r"C:\TradingData\eqidv2\outputs_ID_v8_walkforward")

PNL_COL = "v6_net_pnl_rs"
TODAY_PNL_COL = "pnl_rs"

# Signal-time fields only. Do not add exit-known fields such as bars_held.
FEATURES = [
    "quality_score",
    "atr_pct",
    "body_pct",
    "close_loc",
    "market_ret_pct",
    "rs_pct",
    "vol_ratio",
    "vwap_dist_atr",
    "day_value_so_far_rs",
    "_signal_hour",
]


def _pf(pnl: pd.Series) -> float:
    pnl = pd.to_numeric(pnl, errors="coerce").fillna(0.0)
    gross_profit = float(pnl[pnl > 0].sum())
    gross_loss = float(-pnl[pnl < 0].sum())
    if gross_loss <= 0:
        return 999.0 if gross_profit > 0 else 0.0
    return gross_profit / gross_loss


def _metrics(df: pd.DataFrame, pnl_col: str) -> dict:
    if df.empty:
        return {
            "trades": 0,
            "days": 0,
            "avg_trades_day": 0.0,
            "pf": 0.0,
            "pnl": 0.0,
            "win_pct": 0.0,
            "day_win_pct": 0.0,
            "max_day_loss": 0.0,
        }
    pnl = pd.to_numeric(df[pnl_col], errors="coerce").fillna(0.0)
    daily = df.assign(_pnl=pnl).groupby("trade_date", dropna=False)["_pnl"].sum()
    return {
        "trades": int(len(df)),
        "days": int(df["trade_date"].nunique()),
        "avg_trades_day": float(len(df) / max(df["trade_date"].nunique(), 1)),
        "pf": float(_pf(pnl)),
        "pnl": float(pnl.sum()),
        "win_pct": float((pnl > 0).mean() * 100.0),
        "day_win_pct": float((daily > 0).mean() * 100.0),
        "max_day_loss": float(daily.min()) if not daily.empty else 0.0,
    }


def _read_csv_if_exists(path: Path, usecols: list[str]) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    header = pd.read_csv(path, nrows=1, low_memory=False)
    cols = [c for c in usecols if c in header.columns]
    if not cols:
        return pd.DataFrame()
    return pd.read_csv(path, usecols=cols, low_memory=False)


def _load_historical_pool(source_dir: Path) -> pd.DataFrame:
    usecols = [
        "trade_date",
        "date",
        "ticker",
        "side",
        "setup",
        "v6_sl_pct",
        "v6_target_pct",
        PNL_COL,
        *FEATURES,
    ]
    frames = [
        _read_csv_if_exists(source_dir / "trades.csv", usecols),
        _read_csv_if_exists(source_dir / "extra_pf2_setup_candidates_resolved.csv", usecols),
    ]
    frames = [df for df in frames if not df.empty]
    if not frames:
        raise SystemExit(f"No v8 historical pool found under {source_dir}")

    out = pd.concat(frames, ignore_index=True)
    if "trade_date" not in out.columns and "date" in out.columns:
        out["trade_date"] = out["date"]
    out["trade_date"] = pd.to_datetime(out["trade_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    out["ticker"] = out["ticker"].astype(str).str.upper().str.strip()
    out["side"] = out["side"].astype(str).str.upper().str.strip()
    out["setup"] = out["setup"].astype(str).str.strip()
    for col in ["v6_sl_pct", "v6_target_pct", PNL_COL, *FEATURES]:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")
    out = out.dropna(subset=["trade_date", "ticker", "side", "setup", "v6_sl_pct", "v6_target_pct", PNL_COL])
    out = out.sort_values(["trade_date", "ticker", "side", "setup"])
    out = out.drop_duplicates(
        subset=["trade_date", "ticker", "side", "setup", "v6_sl_pct", "v6_target_pct"],
        keep="first",
    )
    return out.reset_index(drop=True)


def _normalise_today_grid(path: Path, valid_combos: set[tuple]) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    usecols = [
        "trade_date",
        "ticker",
        "side",
        "setup",
        "sl_pct",
        "target_pct",
        TODAY_PNL_COL,
        "signal_time_ist",
        "entry_time_v8",
        "outcome",
        *FEATURES,
    ]
    df = _read_csv_if_exists(path, usecols)
    if df.empty:
        return df
    if "trade_date" not in df.columns:
        df["trade_date"] = "2026-05-21"
    df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    df["ticker"] = df["ticker"].astype(str).str.upper().str.strip()
    df["side"] = df["side"].astype(str).str.upper().str.strip()
    df["setup"] = df["setup"].astype(str).str.strip()
    for col in ["sl_pct", "target_pct", TODAY_PNL_COL, *FEATURES]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.dropna(subset=["trade_date", "ticker", "side", "setup", "sl_pct", "target_pct", TODAY_PNL_COL])
    df = df[
        df.apply(lambda r: (r["side"], r["setup"], float(r["sl_pct"]), float(r["target_pct"])) in valid_combos, axis=1)
    ].copy()
    return df.reset_index(drop=True)


def _apply_rule(df: pd.DataFrame, rule: dict, *, pnl_col: str) -> pd.DataFrame:
    sl_col = "v6_sl_pct" if "v6_sl_pct" in df.columns else "sl_pct"
    tgt_col = "v6_target_pct" if "v6_target_pct" in df.columns else "target_pct"
    out = df[
        df["side"].eq(rule["side"])
        & df["setup"].eq(rule["setup"])
        & np.isclose(df[sl_col].astype(float), float(rule["sl_pct"]))
        & np.isclose(df[tgt_col].astype(float), float(rule["target_pct"]))
    ].copy()
    for i in (1, 2):
        field = rule.get(f"field{i}", "")
        if not isinstance(field, str) or not field:
            continue
        if field not in out.columns:
            return out.iloc[0:0].copy()
        threshold = float(rule[f"threshold{i}"])
        op = str(rule[f"op{i}"])
        out = out[out[field] >= threshold] if op == ">=" else out[out[field] <= threshold]
    if not out.empty:
        sort_cols = ["trade_date", "ticker", "setup"]
        out = out.sort_values(sort_cols).drop_duplicates(subset=["trade_date", "ticker"], keep="first")
    return out


def _candidate_rules(train: pd.DataFrame, *, min_train_trades: int) -> pd.DataFrame:
    quantiles = [0.10, 0.20, 0.30, 0.40, 0.50, 0.60, 0.70, 0.80, 0.90]
    rules: list[dict] = []

    for (side, setup, sl_pct, target_pct), group in train.groupby(["side", "setup", "v6_sl_pct", "v6_target_pct"]):
        if len(group) < min_train_trades:
            continue

        base = {
            "side": side,
            "setup": setup,
            "sl_pct": float(sl_pct),
            "target_pct": float(target_pct),
        }

        met = _metrics(group, PNL_COL)
        rules.append({**base, "field1": "", "op1": "", "threshold1": np.nan, "field2": "", "op2": "", "threshold2": np.nan, "rule": "ALL", **{f"train_{k}": v for k, v in met.items()}})

        singles: list[dict] = []
        for field in FEATURES:
            if field not in group.columns:
                continue
            values = group[field].dropna()
            if len(values) < min_train_trades:
                continue
            thresholds = sorted({float(values.quantile(q)) for q in quantiles if pd.notna(values.quantile(q))})
            for threshold in thresholds:
                for op in (">=", "<="):
                    subset = group[group[field] >= threshold] if op == ">=" else group[group[field] <= threshold]
                    if len(subset) < min_train_trades:
                        continue
                    met = _metrics(subset, PNL_COL)
                    rec = {
                        **base,
                        "field1": field,
                        "op1": op,
                        "threshold1": threshold,
                        "field2": "",
                        "op2": "",
                        "threshold2": np.nan,
                        "rule": f"{field} {op} {threshold:.6g}",
                        **{f"train_{k}": v for k, v in met.items()},
                    }
                    rules.append(rec)
                    if met["pnl"] > 0 and met["pf"] >= 1.25:
                        singles.append(rec)

        singles = sorted(singles, key=lambda r: (r["train_pf"], r["train_pnl"]), reverse=True)[:40]
        for single in singles:
            first = _apply_rule(group, single, pnl_col=PNL_COL)
            for field in FEATURES:
                if field == single["field1"] or field not in first.columns:
                    continue
                values = first[field].dropna()
                if len(values) < min_train_trades:
                    continue
                thresholds = sorted({float(values.quantile(q)) for q in (0.25, 0.50, 0.75) if pd.notna(values.quantile(q))})
                for threshold in thresholds:
                    for op in (">=", "<="):
                        subset = first[first[field] >= threshold] if op == ">=" else first[first[field] <= threshold]
                        if len(subset) < min_train_trades:
                            continue
                        met = _metrics(subset, PNL_COL)
                        rules.append({
                            **base,
                            "field1": single["field1"],
                            "op1": single["op1"],
                            "threshold1": single["threshold1"],
                            "field2": field,
                            "op2": op,
                            "threshold2": threshold,
                            "rule": f"{single['rule']} AND {field} {op} {threshold:.6g}",
                            **{f"train_{k}": v for k, v in met.items()},
                        })

    if not rules:
        return pd.DataFrame()
    out = pd.DataFrame(rules).drop_duplicates(subset=["side", "setup", "sl_pct", "target_pct", "rule"])
    return out.reset_index(drop=True)


def _pick_rule(rules: pd.DataFrame, *, min_pf: float, min_trades: int, max_avg_trades_day: float) -> pd.Series | None:
    if rules.empty:
        return None
    eligible = rules[
        (rules["train_trades"] >= min_trades)
        & (rules["train_pf"] >= min_pf)
        & (rules["train_pnl"] > 0)
        & (rules["train_avg_trades_day"] <= max_avg_trades_day)
    ].copy()
    if eligible.empty:
        eligible = rules[
            (rules["train_trades"] >= min_trades)
            & (rules["train_pf"] >= max(1.25, min_pf - 0.25))
            & (rules["train_pnl"] > 0)
            & (rules["train_avg_trades_day"] <= max_avg_trades_day)
        ].copy()
    if eligible.empty:
        return None
    eligible["score"] = (
        eligible["train_pf"].clip(upper=5.0) * np.log1p(eligible["train_trades"])
        + eligible["train_day_win_pct"] / 100.0
        + eligible["train_pnl"] / 100000.0
        - eligible["train_avg_trades_day"].clip(lower=0.0) * 0.25
    )
    eligible = eligible.sort_values(["score", "train_pf", "train_pnl", "train_trades"], ascending=[False, False, False, False])
    return eligible.iloc[0]


def _window_dates(dates: list[str], train_days: int, test_days: int, step_days: int) -> list[tuple[list[str], list[str]]]:
    windows = []
    start = 0
    while start + train_days + test_days <= len(dates):
        train_slice = dates[start : start + train_days]
        test_slice = dates[start + train_days : start + train_days + test_days]
        windows.append((train_slice, test_slice))
        start += step_days
    return windows


def run(args: argparse.Namespace) -> int:
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    pool = _load_historical_pool(Path(args.source_dir))
    pool.to_csv(out_dir / "historical_pool.csv", index=False)

    dates = sorted(pool["trade_date"].dropna().unique().tolist())
    windows = _window_dates(dates, int(args.train_days), int(args.test_days), int(args.step_days))
    if not windows:
        raise SystemExit("Not enough dates for requested walk-forward windows")

    summaries = []
    selected_rules = []
    test_rows = []

    for idx, (train_dates, test_dates) in enumerate(windows, 1):
        train = pool[pool["trade_date"].isin(train_dates)].copy()
        test = pool[pool["trade_date"].isin(test_dates)].copy()
        rules = _candidate_rules(train, min_train_trades=int(args.min_train_trades))
        pick = _pick_rule(
            rules,
            min_pf=float(args.min_train_pf),
            min_trades=int(args.min_train_trades),
            max_avg_trades_day=float(args.max_train_avg_trades_day),
        )
        if pick is None:
            summaries.append({
                "window": idx,
                "train_start": train_dates[0],
                "train_end": train_dates[-1],
                "test_start": test_dates[0],
                "test_end": test_dates[-1],
                "status": "NO_RULE",
            })
            continue

        rule = pick.to_dict()
        selected_rules.append({"window": idx, **rule})
        train_selected = _apply_rule(train, rule, pnl_col=PNL_COL)
        test_selected = _apply_rule(test, rule, pnl_col=PNL_COL)
        if not test_selected.empty:
            test_selected = test_selected.copy()
            test_selected["walk_window"] = idx
            test_selected["walk_rule"] = rule["rule"]
            test_rows.append(test_selected)

        train_met = _metrics(train_selected, PNL_COL)
        test_met = _metrics(test_selected, PNL_COL)
        summaries.append({
            "window": idx,
            "train_start": train_dates[0],
            "train_end": train_dates[-1],
            "test_start": test_dates[0],
            "test_end": test_dates[-1],
            "status": "OK",
            "side": rule["side"],
            "setup": rule["setup"],
            "sl_pct": rule["sl_pct"],
            "target_pct": rule["target_pct"],
            "rule": rule["rule"],
            **{f"train_{k}": v for k, v in train_met.items()},
            **{f"test_{k}": v for k, v in test_met.items()},
        })
        print(
            f"[{idx:02d}/{len(windows)}] {test_dates[0]}->{test_dates[-1]} "
            f"{rule['side']} {rule['setup']} test_n={test_met['trades']} "
            f"test_pf={test_met['pf']:.3f} pnl={test_met['pnl']:.0f}"
        )

    summary = pd.DataFrame(summaries)
    rules_df = pd.DataFrame(selected_rules)
    tests = pd.concat(test_rows, ignore_index=True) if test_rows else pd.DataFrame()

    summary.to_csv(out_dir / "walkforward_windows.csv", index=False)
    rules_df.to_csv(out_dir / "selected_rules.csv", index=False)
    tests.to_csv(out_dir / "walkforward_test_trades.csv", index=False)

    overall = _metrics(tests, PNL_COL) if not tests.empty else _metrics(pd.DataFrame(), PNL_COL)
    setup = (
        tests.groupby(["side", "setup"], dropna=False)
        .apply(lambda g: pd.Series(_metrics(g, PNL_COL)), include_groups=False)
        .reset_index()
        .sort_values("pnl", ascending=False)
        if not tests.empty
        else pd.DataFrame()
    )
    setup.to_csv(out_dir / "walkforward_by_setup.csv", index=False)

    valid_combos = set(
        tuple(x)
        for x in pool[["side", "setup", "v6_sl_pct", "v6_target_pct"]]
        .drop_duplicates()
        .itertuples(index=False, name=None)
    )
    today = _normalise_today_grid(Path(args.today_grid), valid_combos)
    today_result = pd.DataFrame()
    today_pick = rules_df.tail(1)
    today_met = _metrics(pd.DataFrame(), TODAY_PNL_COL)
    if not today.empty and not today_pick.empty:
        rule = today_pick.iloc[0].to_dict()
        today_result = _apply_rule(today, rule, pnl_col=TODAY_PNL_COL)
        today_result.to_csv(out_dir / "today_holdout_trades_latest_rule.csv", index=False)
        today_met = _metrics(today_result, TODAY_PNL_COL)

    lines = [
        "AVWAP ID 5-min v8 walk-forward optimizer",
        "=" * 80,
        f"source_dir={args.source_dir}",
        f"windows={len(windows)} train_days={args.train_days} test_days={args.test_days} step_days={args.step_days}",
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
        "Today holdout using latest selected rule:",
        f"Trades            : {today_met['trades']:,}",
        f"PF                : {today_met['pf']:.3f}",
        f"PnL               : Rs {today_met['pnl']:,.2f}",
        f"Win %             : {today_met['win_pct']:.2f}%",
    ]
    if not today_pick.empty:
        r = today_pick.iloc[0]
        lines.extend([
            f"Latest rule       : {r['side']} {r['setup']} SL={float(r['sl_pct']):.2f}% TGT={float(r['target_pct']):.2f}%",
            f"Filter            : {r['rule']}",
        ])
    text = "\n".join(lines)
    print(text)
    (out_dir / "summary.txt").write_text(text + "\n", encoding="utf-8")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description="Walk-forward optimizer for AVWAP ID 5-min v8")
    ap.add_argument("--source_dir", default=str(DEFAULT_SOURCE_DIR))
    ap.add_argument("--today_grid", default=str(DEFAULT_TODAY_GRID))
    ap.add_argument("--out", default=str(DEFAULT_OUT))
    ap.add_argument("--train_days", type=int, default=60)
    ap.add_argument("--test_days", type=int, default=10)
    ap.add_argument("--step_days", type=int, default=10)
    ap.add_argument("--min_train_trades", type=int, default=30)
    ap.add_argument("--min_train_pf", type=float, default=1.5)
    ap.add_argument("--max_train_avg_trades_day", type=float, default=1.5)
    args = ap.parse_args()
    return run(args)


if __name__ == "__main__":
    raise SystemExit(main())
