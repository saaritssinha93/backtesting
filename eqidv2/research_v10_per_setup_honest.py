from __future__ import annotations

import itertools
import math
import re
from functools import lru_cache
from pathlib import Path

import numpy as np
import pandas as pd

import avwap_5min_ID_v10_backtesting as v10
import v17D_exit_resolver as er


OUT_DIR = Path(r"C:\TradingData\eqidv2\outputs_ID_v10_full_historical_v7_live_logic")
TRADES_CSV = OUT_DIR / "trades.csv"

TRAIN_END = pd.Timestamp("2026-01-31")
VALID_START = pd.Timestamp("2026-02-01")
VALID_END = pd.Timestamp("2026-03-31")
TEST_START = pd.Timestamp("2026-04-01")
TEST_END = pd.Timestamp("2026-05-29")

NUMERIC_FEATURES = [
    "ranker_score",
    "quality_score",
    "score",
    "atr_pct",
    "body_pct",
    "close_loc",
    "market_ret_pct",
    "rs_pct",
    "vol_ratio",
    "vwap_dist_atr",
    "signal_volume",
    "signal_close",
    "v7_signal_notional_rs",
    "v7_signal_sl_pct",
    "v7_signal_target_pct",
    "signal_minute",
    "signal_hour",
]

CATEGORICAL_FEATURES = ["regime", "candidate_family", "selection_mode"]

# Coarse exit grid only. Fine grids are too easy to overfit.
SL_GRID = [0.70, 0.80, 0.90, 1.00, 1.20, 1.50]
TGT_GRID = [0.50, 0.60, 0.75, 1.00, 1.20, 1.50, 2.00]


def _pf(pnl: pd.Series | np.ndarray) -> float:
    s = pd.Series(pnl, dtype="float64").fillna(0.0)
    gains = float(s[s > 0].sum())
    losses = float(-s[s < 0].sum())
    if losses <= 0:
        return math.inf if gains > 0 else math.nan
    return gains / losses


def _metrics(frame: pd.DataFrame) -> dict:
    if frame.empty:
        return {
            "trades": 0,
            "days": 0,
            "pnl": 0.0,
            "pf": math.nan,
            "win_pct": math.nan,
            "avg_trade": math.nan,
            "target_pct": math.nan,
            "sl_pct": math.nan,
            "eod_pct": math.nan,
        }
    pnl = pd.to_numeric(frame["pnl"], errors="coerce").fillna(0.0)
    return {
        "trades": int(len(frame)),
        "days": int(frame["date"].dt.date.nunique()),
        "pnl": float(pnl.sum()),
        "pf": float(_pf(pnl)),
        "win_pct": float((pnl > 0).mean() * 100.0),
        "avg_trade": float(pnl.mean()),
        "target_pct": float((frame["outcome"].astype(str) == "TARGET").mean() * 100.0),
        "sl_pct": float((frame["outcome"].astype(str) == "SL").mean() * 100.0),
        "eod_pct": float((frame["outcome"].astype(str) == "EOD").mean() * 100.0),
    }


def _split_masks(df: pd.DataFrame) -> dict[str, pd.Series]:
    return {
        "train": df["date"] <= TRAIN_END,
        "valid": (df["date"] >= VALID_START) & (df["date"] <= VALID_END),
        "test": (df["date"] >= TEST_START) & (df["date"] <= TEST_END),
        "full": pd.Series(True, index=df.index),
    }


def _metric_row(label: str, split: str, frame: pd.DataFrame) -> dict:
    out = {"label": label, "split": split}
    out.update(_metrics(frame))
    return out


def _load_trades() -> pd.DataFrame:
    df = pd.read_csv(TRADES_CSV)
    df["date"] = pd.to_datetime(df["trade_date"])
    df["pnl"] = pd.to_numeric(df["v6_net_pnl_rs"], errors="coerce").fillna(0.0)
    df["outcome"] = df["v6_outcome"].astype(str)
    sig = pd.to_datetime(df.get("signal_time_ist"), errors="coerce")
    df["signal_minute"] = sig.dt.hour * 60 + sig.dt.minute
    df["signal_hour"] = sig.dt.hour
    for col in NUMERIC_FEATURES:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    df["entry_ts"] = pd.to_datetime(df["entry_time_v6"], errors="coerce")
    df["entry_px"] = pd.to_numeric(df["entry_price_v6"], errors="coerce")
    df["qty"] = pd.to_numeric(df["quantity"], errors="coerce")
    return df


def _condition_mask(df: pd.DataFrame, setup_mask: pd.Series, condition: str) -> pd.Series:
    condition = condition.strip()
    if "==" in condition:
        col, value = condition.split("==", 1)
        return setup_mask & (df[col.strip()].astype(str) == value.strip())
    match = re.match(r"([^<>!=]+)(>=|<=)(.*)", condition)
    if not match:
        raise ValueError(f"Cannot parse condition: {condition}")
    col = match.group(1).strip()
    op = match.group(2)
    value = float(match.group(3).strip())
    if op == ">=":
        return setup_mask & (df[col] >= value)
    return setup_mask & (df[col] <= value)


def _rule_mask(df: pd.DataFrame, setup: str, rule: str | float | None) -> pd.Series:
    setup_mask = df["setup"].astype(str) == str(setup)
    if rule is None or (isinstance(rule, float) and math.isnan(rule)) or str(rule).strip() in {"", "nan"}:
        return pd.Series(False, index=df.index)
    mask = setup_mask.copy()
    for part in str(rule).split(" AND "):
        part = part.strip()
        if part == "ALL":
            continue
        mask &= _condition_mask(df, setup_mask, part)
    return mask


def _single_rule_pool(df: pd.DataFrame, setup: str) -> list[tuple[str, pd.Series]]:
    setup_mask = df["setup"].astype(str) == setup
    train_mask = _split_masks(df)["train"]
    train_setup = df[setup_mask & train_mask]
    rules: list[tuple[str, pd.Series]] = [("ALL", setup_mask)]

    for col in NUMERIC_FEATURES:
        if col not in df.columns:
            continue
        values = train_setup[col].dropna()
        if values.nunique() < 8:
            continue
        for q in [0.10, 0.15, 0.20, 0.25, 0.30, 0.35, 0.40, 0.50, 0.60, 0.65, 0.70, 0.75, 0.80, 0.85, 0.90]:
            threshold = float(values.quantile(q))
            rules.append((f"{col}>={threshold:.8g}", setup_mask & (df[col] >= threshold)))
            rules.append((f"{col}<={threshold:.8g}", setup_mask & (df[col] <= threshold)))

    for col in CATEGORICAL_FEATURES:
        if col not in df.columns:
            continue
        counts = train_setup[col].astype(str).value_counts()
        for value, count in counts.items():
            if count >= max(10, int(len(train_setup) * 0.04)):
                rules.append((f"{col}=={value}", setup_mask & (df[col].astype(str) == value)))
    return rules


def _select_entry_rule(df: pd.DataFrame, setup: str) -> tuple[str | None, pd.Series | None, dict | None]:
    splits = _split_masks(df)
    setup_mask = df["setup"].astype(str) == setup
    base_train_n = int((setup_mask & splits["train"]).sum())
    base_valid_n = int((setup_mask & splits["valid"]).sum())
    if base_train_n < 20 or base_valid_n < 5:
        return None, None, None

    min_train = max(25, int(base_train_n * 0.05)) if base_train_n >= 200 else max(15, int(base_train_n * 0.15))
    min_valid = max(12, int(base_valid_n * 0.05)) if base_valid_n >= 200 else max(8, int(base_valid_n * 0.15))
    singles = _single_rule_pool(df, setup)

    viable: list[tuple[str, pd.Series, dict, dict]] = []
    for rule, mask in singles:
        mt = _metrics(df[mask & splits["train"]])
        mv = _metrics(df[mask & splits["valid"]])
        if mt["trades"] >= min_train and mv["trades"] >= min_valid:
            viable.append((rule, mask, mt, mv))

    def sort_key(item: tuple[str, pd.Series, dict, dict]) -> tuple[float, float, int, int]:
        _, _, mt, mv = item
        min_pf = min(float(mt["pf"]), float(mv["pf"])) if np.isfinite(mt["pf"]) and np.isfinite(mv["pf"]) else -1.0
        return (min_pf, float(mt["pnl"] + mv["pnl"]), int(mv["trades"]), int(mt["trades"]))

    pool = sorted(viable, key=sort_key, reverse=True)[:70]
    candidates: list[tuple[str, pd.Series]] = [(rule, mask) for rule, mask, _, _ in viable]
    for (r1, m1, _, _), (r2, m2, _, _) in itertools.combinations(pool, 2):
        if r1 == "ALL" or r2 == "ALL":
            continue
        candidates.append((f"{r1} AND {r2}", m1 & m2))

    best: tuple[str, pd.Series, dict] | None = None
    seen: set[str] = set()
    for rule, mask in candidates:
        if rule in seen:
            continue
        seen.add(rule)
        mt = _metrics(df[mask & splits["train"]])
        mv = _metrics(df[mask & splits["valid"]])
        if mt["trades"] < min_train or mv["trades"] < min_valid:
            continue
        # Honest entry threshold: both train and validation must clear a useful bar.
        if mt["pf"] < 1.15 or mv["pf"] < 1.15:
            continue
        train_pf = _finite_pf_for_score(mt["pf"])
        valid_pf = _finite_pf_for_score(mv["pf"])
        min_pf = min(train_pf, valid_pf)
        dev_pnl = float(mt["pnl"] + mv["pnl"])
        # This research pass is quality-first: prefer robust PF over larger trade count.
        score = (
            min_pf * 10000.0
            + (train_pf + valid_pf) * 100.0
            + max(0.0, dev_pnl) / 1000.0
            + min(math.log1p(mt["trades"]), math.log1p(mv["trades"])) * 10.0
            - math.log1p(mt["trades"] + mv["trades"])
        )
        payload = {"score": score, "train": mt, "valid": mv}
        if best is None or score > best[2]["score"]:
            best = (rule, mask, payload)
    if best is None:
        return None, None, None
    return best


@lru_cache(maxsize=None)
def _bars(ticker: str) -> pd.DataFrame | None:
    return v10._load_1m_with_open(str(ticker).upper())


def _resolve_grid_for_frame(frame: pd.DataFrame, sl: float, tgt: float) -> pd.DataFrame:
    rows = []
    for _, row in frame.iterrows():
        bars = _bars(str(row["ticker"]))
        if bars is None or bars.empty:
            continue
        entry_ts = row["entry_ts"]
        entry_px = float(row["entry_px"])
        qty = int(row["qty"])
        if pd.isna(entry_ts) or not np.isfinite(entry_px) or entry_px <= 0 or qty <= 0:
            continue
        res = er.resolve(
            bars=bars,
            side=str(row["side"]),
            entry_price=entry_px,
            entry_time_ist=entry_ts,
            sl_pct=sl,
            tgt_pct=tgt,
        )
        if res is None:
            continue
        if str(row["side"]).upper() == "SHORT":
            pnl = (entry_px - float(res.exit_price)) * qty
        else:
            pnl = (float(res.exit_price) - entry_px) * qty
        rows.append(
            {
                "ticker": row["ticker"],
                "side": row["side"],
                "setup": row["setup"],
                "date": row["date"],
                "pnl": float(pnl),
                "outcome": res.outcome,
            }
        )
    return pd.DataFrame(rows)


def _exit_grid_search(frame: pd.DataFrame, setup: str) -> tuple[tuple[float, float] | None, pd.DataFrame]:
    records = []
    for sl, tgt in itertools.product(SL_GRID, TGT_GRID):
        resolved = _resolve_grid_for_frame(frame, sl, tgt)
        if resolved.empty:
            continue
        rsplits = _split_masks(resolved)
        mt = _metrics(resolved[rsplits["train"]])
        mv = _metrics(resolved[rsplits["valid"]])
        ms = _metrics(resolved[rsplits["test"]])
        mf = _metrics(resolved)
        records.append(
            {
                "setup": setup,
                "sl": sl,
                "target": tgt,
                "train_n": mt["trades"],
                "train_pf": mt["pf"],
                "train_pnl": mt["pnl"],
                "valid_n": mv["trades"],
                "valid_pf": mv["pf"],
                "valid_pnl": mv["pnl"],
                "test_n": ms["trades"],
                "test_pf": ms["pf"],
                "test_pnl": ms["pnl"],
                "full_n": mf["trades"],
                "full_pf": mf["pf"],
                "full_pnl": mf["pnl"],
            }
        )
    grid = pd.DataFrame(records)
    if grid.empty:
        return None, grid
    train_min = max(20, int(grid["train_n"].max() * 0.50))
    valid_min = max(5, int(grid["valid_n"].max() * 0.50))
    eligible = grid[
        (grid["train_n"] >= train_min)
        & (grid["valid_n"] >= valid_min)
        & (grid["train_pf"] >= 1.05)
        & (grid["valid_pf"] >= 1.05)
    ].copy()
    if eligible.empty:
        return None, grid
    eligible["select_score"] = (
        eligible[["train_pf", "valid_pf"]].min(axis=1)
        * np.log1p(eligible["train_n"])
        * np.log1p(eligible["valid_n"])
        / (1.0 + (eligible["sl"] - 0.7).abs() + (eligible["target"] - 1.0).abs() * 0.25)
    )
    best = eligible.sort_values(["select_score", "valid_pf", "train_pf"], ascending=False).iloc[0]
    return (float(best["sl"]), float(best["target"])), grid


def _finite_pf_for_score(value: float) -> float:
    if not np.isfinite(value):
        return 5.0 if value == math.inf else -1.0
    return float(value)


def _strategy_stats(frame: pd.DataFrame) -> dict[str, dict]:
    splits = _split_masks(frame)
    return {split: _metrics(frame[mask]) for split, mask in splits.items()}


def _strategy_score(stats: dict[str, dict], min_train: int, min_valid: int) -> float:
    train = stats["train"]
    valid = stats["valid"]
    if train["trades"] < min_train or valid["trades"] < min_valid:
        return -math.inf
    train_pf = _finite_pf_for_score(train["pf"])
    valid_pf = _finite_pf_for_score(valid["pf"])
    if train_pf < 1.05 or valid_pf < 1.05:
        return -math.inf
    pnl_bonus = max(0.0, float(train["pnl"] + valid["pnl"])) / 100000.0
    min_pf = min(train_pf, valid_pf)
    return (
        min_pf * 10000.0
        + (train_pf + valid_pf) * 100.0
        + pnl_bonus
        + min(math.log1p(train["trades"]), math.log1p(valid["trades"])) * 10.0
        - math.log1p(stats["full"]["trades"])
    )


def _strict_oos_pass(stats: dict[str, dict]) -> bool:
    train = stats["train"]
    valid = stats["valid"]
    test = stats["test"]
    full = stats["full"]
    return (
        _finite_pf_for_score(train["pf"]) >= 1.15
        and _finite_pf_for_score(valid["pf"]) >= 1.15
        and _finite_pf_for_score(full["pf"]) >= 1.50
        and test["trades"] >= 5
        and _finite_pf_for_score(test["pf"]) >= 1.20
    )


def _empty_like_metrics_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=["ticker", "side", "setup", "date", "pnl", "outcome"])


def _concat_frames(frames: list[pd.DataFrame]) -> pd.DataFrame:
    non_empty = [frame for frame in frames if frame is not None and not frame.empty]
    if not non_empty:
        return _empty_like_metrics_frame()
    return pd.concat(non_empty, ignore_index=True, sort=False)


def main() -> int:
    trades = _load_trades()
    splits = _split_masks(trades)
    setup_rows = []
    option_rows = []
    entry_only_frames: list[pd.DataFrame] = []
    dev_selected_frames: list[pd.DataFrame] = []
    strict_pass_frames: list[pd.DataFrame] = []
    all_exit_grids = []

    for setup in sorted(trades["setup"].dropna().astype(str).unique()):
        print(f"[setup] {setup}", flush=True)
        setup_mask = trades["setup"].astype(str) == setup
        base = trades[setup_mask].copy()
        base_stats = _strategy_stats(base)
        base_train_n = base_stats["train"]["trades"]
        base_valid_n = base_stats["valid"]["trades"]
        min_train = max(25, int(base_train_n * 0.05)) if base_train_n >= 200 else max(15, int(base_train_n * 0.15))
        min_valid = max(12, int(base_valid_n * 0.05)) if base_valid_n >= 200 else max(8, int(base_valid_n * 0.15))

        entry_rule, entry_mask, entry_payload = _select_entry_rule(trades, setup)
        if entry_mask is None:
            entry_frame = _empty_like_metrics_frame()
        else:
            entry_frame = trades[entry_mask].copy()
            entry_only_frames.append(entry_frame.assign(research_variant="entry_filter_current_exit"))

        exit_choice_base, exit_grid_base = _exit_grid_search(base, setup)
        if not exit_grid_base.empty:
            exit_grid_base["scope"] = "setup_all_signals"
            all_exit_grids.append(exit_grid_base)
        all_exit_frame = _empty_like_metrics_frame()
        if exit_choice_base is not None:
            all_exit_frame = _resolve_grid_for_frame(base, exit_choice_base[0], exit_choice_base[1])

        exit_choice_entry = None
        exit_grid_entry = pd.DataFrame()
        entry_exit_frame = _empty_like_metrics_frame()
        if not entry_frame.empty:
            exit_choice_entry, exit_grid_entry = _exit_grid_search(entry_frame, setup)
            if not exit_grid_entry.empty:
                exit_grid_entry["scope"] = "entry_filtered"
                all_exit_grids.append(exit_grid_entry)
            if exit_choice_entry is not None:
                entry_exit_frame = _resolve_grid_for_frame(entry_frame, exit_choice_entry[0], exit_choice_entry[1])

        options = [
            {
                "variant": "current_all_signals",
                "frame": base,
                "entry_rule": "ALL",
                "exit_rule": "current",
            }
        ]
        if not entry_frame.empty:
            options.append(
                {
                    "variant": "entry_filter_current_exit",
                    "frame": entry_frame,
                    "entry_rule": entry_rule or "",
                    "exit_rule": "current",
                }
            )
        if not all_exit_frame.empty:
            options.append(
                {
                    "variant": "all_signals_exit_retune",
                    "frame": all_exit_frame,
                    "entry_rule": "ALL",
                    "exit_rule": f"SL={exit_choice_base[0]:.2f},TGT={exit_choice_base[1]:.2f}",
                }
            )
        if not entry_exit_frame.empty:
            options.append(
                {
                    "variant": "entry_filter_exit_retune",
                    "frame": entry_exit_frame,
                    "entry_rule": entry_rule or "",
                    "exit_rule": f"SL={exit_choice_entry[0]:.2f},TGT={exit_choice_entry[1]:.2f}",
                }
            )

        selected = None
        for option in options:
            stats = _strategy_stats(option["frame"])
            score = _strategy_score(stats, min_train, min_valid)
            option["stats"] = stats
            option["score"] = score
            for split, metric in stats.items():
                row = {
                    "setup": setup,
                    "side": base["side"].mode().iat[0] if not base.empty else "",
                    "variant": option["variant"],
                    "entry_rule": option["entry_rule"],
                    "exit_rule": option["exit_rule"],
                    "split": split,
                    "dev_score": score,
                }
                row.update(metric)
                option_rows.append(row)
            if np.isfinite(score) and (selected is None or score > selected["score"]):
                selected = option

        if selected is None:
            final_frame = _empty_like_metrics_frame()
            final_stats = _strategy_stats(final_frame)
            final_variant = "disabled_no_train_valid_edge"
            final_entry = ""
            final_exit = ""
            final_score = -math.inf
        else:
            final_frame = selected["frame"].copy()
            final_variant = str(selected["variant"])
            final_entry = str(selected["entry_rule"])
            final_exit = str(selected["exit_rule"])
            final_score = float(selected["score"])
            final_stats = selected["stats"]
            dev_selected_frames.append(final_frame.assign(research_variant=final_variant, selected_setup=setup))

        final_ok = _strict_oos_pass(final_stats)
        if final_ok:
            strict_pass_frames.append(final_frame.assign(research_variant=final_variant, selected_setup=setup))

        setup_rows.append(
            {
                "setup": setup,
                "side": base["side"].mode().iat[0] if not base.empty else "",
                "base_train_n": base_stats["train"]["trades"],
                "base_train_pf": base_stats["train"]["pf"],
                "base_valid_n": base_stats["valid"]["trades"],
                "base_valid_pf": base_stats["valid"]["pf"],
                "base_test_n": base_stats["test"]["trades"],
                "base_test_pf": base_stats["test"]["pf"],
                "base_full_n": base_stats["full"]["trades"],
                "base_full_pf": base_stats["full"]["pf"],
                "base_full_pnl": base_stats["full"]["pnl"],
                "entry_rule": entry_rule or "",
                "entry_train_pf": entry_payload["train"]["pf"] if entry_payload else math.nan,
                "entry_valid_pf": entry_payload["valid"]["pf"] if entry_payload else math.nan,
                "entry_full_n": int(len(entry_frame)),
                "entry_full_pf": _metrics(entry_frame)["pf"] if not entry_frame.empty else math.nan,
                "exit_choice_all_signals": "" if exit_choice_base is None else f"SL={exit_choice_base[0]:.2f},TGT={exit_choice_base[1]:.2f}",
                "exit_choice_entry_filtered": "" if exit_choice_entry is None else f"SL={exit_choice_entry[0]:.2f},TGT={exit_choice_entry[1]:.2f}",
                "selected_variant": final_variant,
                "selected_entry_rule": final_entry,
                "final_exit": final_exit,
                "final_dev_score": final_score,
                "final_train_n": final_stats["train"]["trades"],
                "final_train_pf": final_stats["train"]["pf"],
                "final_train_pnl": final_stats["train"]["pnl"],
                "final_valid_n": final_stats["valid"]["trades"],
                "final_valid_pf": final_stats["valid"]["pf"],
                "final_valid_pnl": final_stats["valid"]["pnl"],
                "final_test_n": final_stats["test"]["trades"],
                "final_test_pf": final_stats["test"]["pf"],
                "final_test_pnl": final_stats["test"]["pnl"],
                "final_full_n": final_stats["full"]["trades"],
                "final_full_pf": final_stats["full"]["pf"],
                "final_full_pnl": final_stats["full"]["pnl"],
                "decision": "PASS_STRICT" if final_ok else "FAIL_OR_DISABLE",
            }
        )

    setup_summary = pd.DataFrame(setup_rows)
    setup_summary.to_csv(OUT_DIR / "per_setup_honest_logic_summary.csv", index=False)
    pd.DataFrame(option_rows).to_csv(OUT_DIR / "per_setup_option_results.csv", index=False)
    if all_exit_grids:
        pd.concat(all_exit_grids, ignore_index=True).to_csv(OUT_DIR / "per_setup_exit_grid_results.csv", index=False)

    scenario_frames = {
        "baseline_all": trades.copy(),
        "dev_entry_filters_current_exit": _concat_frames(entry_only_frames),
        "dev_selected_best_by_setup": _concat_frames(dev_selected_frames),
        "strict_oos_pass_only": _concat_frames(strict_pass_frames),
    }

    scenario_rows = []
    for label, frame in scenario_frames.items():
        for split, split_mask in _split_masks(frame).items():
            scenario_rows.append(_metric_row(label, split, frame[split_mask]))
    pd.DataFrame(scenario_rows).to_csv(OUT_DIR / "per_setup_honest_portfolio_scenarios.csv", index=False)

    print("[done] wrote per_setup_honest_logic_summary.csv", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
