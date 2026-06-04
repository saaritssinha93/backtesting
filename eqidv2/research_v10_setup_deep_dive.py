from __future__ import annotations

import itertools
import math
from pathlib import Path

import numpy as np
import pandas as pd

import research_v10_per_setup_honest as h


OUT_DIR = h.OUT_DIR

BASE_NUMERIC_FEATURES = [
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

DERIVED_NUMERIC_FEATURES = [
    "signal_range_pct",
    "signal_body_ret_pct",
    "signal_abs_body_ret_pct",
    "upper_wick_pct",
    "lower_wick_pct",
    "market_abs_ret_pct",
    "rs_abs_pct",
    "rs_minus_market_pct",
    "vwap_dist_abs_atr",
    "sl_to_target_ratio",
]

CATEGORICAL_FEATURES = [
    "regime",
    "candidate_family",
    "selection_mode",
    "candle_color",
    "time_bucket",
]

QUANTILES = [0.05, 0.10, 0.15, 0.20, 0.25, 0.30, 0.35, 0.40, 0.45, 0.50,
             0.55, 0.60, 0.65, 0.70, 0.75, 0.80, 0.85, 0.90, 0.95]
MAX_SINGLE_POOL = 100
MAX_DOUBLE_POOL = 80
MAX_EXIT_RULES_PER_SETUP = 4


def _safe_div(a: pd.Series, b: pd.Series) -> pd.Series:
    a = pd.to_numeric(a, errors="coerce")
    b = pd.to_numeric(b, errors="coerce")
    return a / b.replace(0, np.nan)


def _add_derived_features(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for col in ["signal_open", "signal_high", "signal_low", "signal_close"]:
        out[col] = pd.to_numeric(out[col], errors="coerce")
    open_px = out["signal_open"]
    high = out["signal_high"]
    low = out["signal_low"]
    close = out["signal_close"]
    body_top = pd.concat([open_px, close], axis=1).max(axis=1)
    body_bottom = pd.concat([open_px, close], axis=1).min(axis=1)
    out["signal_range_pct"] = _safe_div(high - low, close) * 100.0
    out["signal_body_ret_pct"] = _safe_div(close - open_px, open_px) * 100.0
    out["signal_abs_body_ret_pct"] = out["signal_body_ret_pct"].abs()
    out["upper_wick_pct"] = _safe_div(high - body_top, close) * 100.0
    out["lower_wick_pct"] = _safe_div(body_bottom - low, close) * 100.0
    out["market_abs_ret_pct"] = pd.to_numeric(out["market_ret_pct"], errors="coerce").abs()
    out["rs_abs_pct"] = pd.to_numeric(out["rs_pct"], errors="coerce").abs()
    out["rs_minus_market_pct"] = pd.to_numeric(out["rs_pct"], errors="coerce") - pd.to_numeric(out["market_ret_pct"], errors="coerce")
    out["vwap_dist_abs_atr"] = pd.to_numeric(out["vwap_dist_atr"], errors="coerce").abs()
    out["sl_to_target_ratio"] = _safe_div(out["v7_signal_sl_pct"], out["v7_signal_target_pct"])
    out["candle_color"] = np.where(close > open_px, "GREEN", np.where(close < open_px, "RED", "DOJI"))
    mins = pd.to_numeric(out["signal_minute"], errors="coerce")
    out["time_bucket"] = np.select(
        [mins <= 630, mins <= 720, mins <= 810],
        ["OPEN_0915_1030", "MID_1031_1200", "AFTERNOON_1201_1330"],
        default="LATE_1331_1500",
    )
    return out


def _pf_value(value: float) -> float:
    if pd.isna(value):
        return -1.0
    if value == math.inf:
        return 8.0
    if value == -math.inf:
        return -1.0
    return float(value)


def _stats_by_split(frame: pd.DataFrame) -> dict[str, dict]:
    masks = h._split_masks(frame)
    return {name: h._metrics(frame[mask]) for name, mask in masks.items()}


def _dev_score(stats: dict[str, dict], *, min_train: int, min_valid: int) -> float:
    train = stats["train"]
    valid = stats["valid"]
    if train["trades"] < min_train or valid["trades"] < min_valid:
        return -math.inf
    train_pf = _pf_value(train["pf"])
    valid_pf = _pf_value(valid["pf"])
    if train_pf < 1.05 or valid_pf < 1.05:
        return -math.inf
    min_pf = min(train_pf, valid_pf)
    dev_pnl = max(0.0, float(train["pnl"] + valid["pnl"]))
    size = min(math.log1p(train["trades"]), math.log1p(valid["trades"]))
    return min_pf * 10000.0 + (train_pf + valid_pf) * 100.0 + dev_pnl / 1000.0 + size * 15.0


def _strict_pass(stats: dict[str, dict]) -> bool:
    return (
        _pf_value(stats["train"]["pf"]) >= 1.15
        and _pf_value(stats["valid"]["pf"]) >= 1.15
        and _pf_value(stats["full"]["pf"]) >= 1.50
        and stats["test"]["trades"] >= 5
        and _pf_value(stats["test"]["pf"]) >= 1.20
    )


def _split_row(setup: str, variant: str, rule: str, exit_rule: str, stats: dict[str, dict], score: float) -> list[dict]:
    rows = []
    for split, metric in stats.items():
        row = {
            "setup": setup,
            "variant": variant,
            "rule": rule,
            "exit_rule": exit_rule,
            "split": split,
            "dev_score": score,
        }
        row.update(metric)
        rows.append(row)
    return rows


def _min_counts(base_stats: dict[str, dict]) -> tuple[int, int]:
    train_n = int(base_stats["train"]["trades"])
    valid_n = int(base_stats["valid"]["trades"])
    min_train = max(25, int(train_n * 0.05)) if train_n >= 200 else max(15, int(train_n * 0.15))
    min_valid = max(12, int(valid_n * 0.05)) if valid_n >= 200 else max(8, int(valid_n * 0.15))
    return min_train, min_valid


def _condition_pool(df: pd.DataFrame, setup: str) -> list[tuple[str, pd.Series]]:
    setup_mask = df["setup"].astype(str).eq(setup)
    train_mask = h._split_masks(df)["train"]
    train = df[setup_mask & train_mask]
    out: list[tuple[str, pd.Series]] = []
    for col in BASE_NUMERIC_FEATURES + DERIVED_NUMERIC_FEATURES:
        if col not in df.columns:
            continue
        values = pd.to_numeric(train[col], errors="coerce").dropna()
        if values.nunique() < 8:
            continue
        for q in QUANTILES:
            threshold = float(values.quantile(q))
            if not np.isfinite(threshold):
                continue
            series = pd.to_numeric(df[col], errors="coerce")
            out.append((f"{col}>={threshold:.8g}", setup_mask & (series >= threshold)))
            out.append((f"{col}<={threshold:.8g}", setup_mask & (series <= threshold)))
    for col in CATEGORICAL_FEATURES:
        if col not in df.columns:
            continue
        counts = train[col].astype(str).value_counts()
        for value, count in counts.items():
            if count >= max(8, int(len(train) * 0.04)):
                out.append((f"{col}=={value}", setup_mask & df[col].astype(str).eq(value)))
    return out


def _evaluate_rule(df: pd.DataFrame, setup: str, variant: str, rule: str, mask: pd.Series, min_train: int, min_valid: int) -> dict:
    frame = df[mask].copy()
    stats = _stats_by_split(frame)
    score = _dev_score(stats, min_train=min_train, min_valid=min_valid)
    return {
        "setup": setup,
        "variant": variant,
        "rule": rule,
        "mask": mask,
        "frame": frame,
        "stats": stats,
        "dev_score": score,
        "strict_pass_current_exit": _strict_pass(stats),
    }


def _ranked_rules(df: pd.DataFrame, setup: str, min_train: int, min_valid: int) -> list[dict]:
    setup_mask = df["setup"].astype(str).eq(setup)
    base = _evaluate_rule(df, setup, "baseline_all", "ALL", setup_mask, min_train, min_valid)
    candidates = [base]

    singles = []
    for rule, mask in _condition_pool(df, setup):
        item = _evaluate_rule(df, setup, "single", rule, mask, min_train, min_valid)
        singles.append(item)
        candidates.append(item)

    def build_key(item: dict) -> tuple[float, float, float, int]:
        stats = item["stats"]
        min_pf = min(_pf_value(stats["train"]["pf"]), _pf_value(stats["valid"]["pf"]))
        dev_pnl = float(stats["train"]["pnl"] + stats["valid"]["pnl"])
        return (min_pf, dev_pnl, item["dev_score"], stats["valid"]["trades"])

    singles_pool = sorted(
        [x for x in singles if x["stats"]["train"]["trades"] >= min_train and x["stats"]["valid"]["trades"] >= min_valid],
        key=build_key,
        reverse=True,
    )[:MAX_SINGLE_POOL]

    seen = {x["rule"] for x in candidates}
    doubles = []
    for a, b in itertools.combinations(singles_pool[:70], 2):
        if a["rule"] == b["rule"]:
            continue
        for op, variant, mask in [
            (" AND ", "double_and", a["mask"] & b["mask"]),
            (" OR ", "double_or", a["mask"] | b["mask"]),
        ]:
            rule = f"({a['rule']}){op}({b['rule']})"
            if rule in seen:
                continue
            seen.add(rule)
            item = _evaluate_rule(df, setup, variant, rule, mask, min_train, min_valid)
            if item["stats"]["train"]["trades"] >= min_train and item["stats"]["valid"]["trades"] >= min_valid:
                doubles.append(item)
                candidates.append(item)

    double_pool = sorted(doubles, key=build_key, reverse=True)[:MAX_DOUBLE_POOL]
    for pair in double_pool[:35]:
        for single in singles_pool[:35]:
            if single["rule"] in pair["rule"]:
                continue
            rule = f"({pair['rule']}) AND ({single['rule']})"
            if rule in seen:
                continue
            seen.add(rule)
            mask = pair["mask"] & single["mask"]
            item = _evaluate_rule(df, setup, "triple_and", rule, mask, min_train, min_valid)
            if item["stats"]["train"]["trades"] >= min_train and item["stats"]["valid"]["trades"] >= min_valid:
                candidates.append(item)

    return sorted(candidates, key=lambda x: x["dev_score"], reverse=True)


def _exit_tune_candidates(setup: str, ranked: list[dict], min_train: int, min_valid: int) -> list[dict]:
    chosen = []
    for item in ranked:
        if item["dev_score"] == -math.inf:
            continue
        if item["stats"]["train"]["pf"] < 1.15 or item["stats"]["valid"]["pf"] < 1.15:
            continue
        if len(item["frame"]) == 0:
            continue
        chosen.append(item)
        if len(chosen) >= MAX_EXIT_RULES_PER_SETUP:
            break

    exit_items = []
    for item in chosen:
        print(f"  [exit-grid] {setup}: {item['variant']} {item['rule'][:110]}", flush=True)
        choice, grid = h._exit_grid_search(item["frame"], setup)
        if not grid.empty:
            grid = grid.copy()
            grid["rule"] = item["rule"]
            grid["variant"] = item["variant"]
            exit_items.append({"grid": grid})
        if choice is None:
            continue
        resolved = h._resolve_grid_for_frame(item["frame"], choice[0], choice[1])
        stats = _stats_by_split(resolved)
        score = _dev_score(stats, min_train=min_train, min_valid=min_valid)
        exit_items.append(
            {
                "setup": setup,
                "variant": f"{item['variant']}_exit_retune",
                "rule": item["rule"],
                "exit_rule": f"SL={choice[0]:.2f},TGT={choice[1]:.2f}",
                "frame": resolved,
                "stats": stats,
                "dev_score": score,
                "strict_pass_current_exit": _strict_pass(stats),
            }
        )
    return exit_items


def main() -> int:
    df = _add_derived_features(h._load_trades())
    setup_rows = []
    option_rows = []
    exit_grid_frames = []
    selected_frames = []
    strict_frames = []

    for setup in sorted(df["setup"].dropna().astype(str).unique()):
        print(f"[deep-setup] {setup}", flush=True)
        setup_mask = df["setup"].astype(str).eq(setup)
        base_frame = df[setup_mask].copy()
        base_stats = _stats_by_split(base_frame)
        min_train, min_valid = _min_counts(base_stats)
        ranked = _ranked_rules(df, setup, min_train, min_valid)
        top_ranked = [x for x in ranked if x["dev_score"] != -math.inf][:25]

        for item in top_ranked:
            option_rows.extend(_split_row(setup, item["variant"], item["rule"], "current", item["stats"], item["dev_score"]))

        exit_items = _exit_tune_candidates(setup, ranked, min_train, min_valid)
        usable_exit_items = []
        for item in exit_items:
            if "grid" in item:
                exit_grid_frames.append(item["grid"])
            else:
                usable_exit_items.append(item)
                option_rows.extend(_split_row(setup, item["variant"], item["rule"], item["exit_rule"], item["stats"], item["dev_score"]))

        selectable = top_ranked + usable_exit_items
        selectable = [x for x in selectable if x["dev_score"] != -math.inf]
        selected = max(selectable, key=lambda x: x["dev_score"], default=None)

        if selected is not None:
            selected_frames.append(selected["frame"].assign(deep_selected_setup=setup, deep_variant=selected["variant"]))
            if _strict_pass(selected["stats"]):
                strict_frames.append(selected["frame"].assign(deep_selected_setup=setup, deep_variant=selected["variant"]))
            final_stats = selected["stats"]
            final_variant = selected["variant"]
            final_rule = selected["rule"]
            final_exit = selected.get("exit_rule", "current")
            final_score = selected["dev_score"]
        else:
            final_stats = _stats_by_split(base_frame.iloc[0:0].copy())
            final_variant = "disabled_no_dev_edge"
            final_rule = ""
            final_exit = ""
            final_score = -math.inf

        setup_rows.append(
            {
                "setup": setup,
                "side": base_frame["side"].mode().iat[0] if not base_frame.empty else "",
                "base_train_n": base_stats["train"]["trades"],
                "base_train_pf": base_stats["train"]["pf"],
                "base_valid_n": base_stats["valid"]["trades"],
                "base_valid_pf": base_stats["valid"]["pf"],
                "base_test_n": base_stats["test"]["trades"],
                "base_test_pf": base_stats["test"]["pf"],
                "base_full_n": base_stats["full"]["trades"],
                "base_full_pf": base_stats["full"]["pf"],
                "base_full_pnl": base_stats["full"]["pnl"],
                "min_train_required": min_train,
                "min_valid_required": min_valid,
                "selected_variant": final_variant,
                "selected_rule": final_rule,
                "selected_exit": final_exit,
                "selected_dev_score": final_score,
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
                "decision": "PASS_STRICT" if _strict_pass(final_stats) else "FAIL_OR_DISABLE",
            }
        )

    pd.DataFrame(setup_rows).to_csv(OUT_DIR / "deep_setup_final_by_setup.csv", index=False)
    pd.DataFrame(option_rows).to_csv(OUT_DIR / "deep_setup_option_results.csv", index=False)
    if exit_grid_frames:
        pd.concat(exit_grid_frames, ignore_index=True, sort=False).to_csv(OUT_DIR / "deep_setup_exit_grid_results.csv", index=False)

    def concat_or_empty(frames: list[pd.DataFrame]) -> pd.DataFrame:
        return pd.concat(frames, ignore_index=True, sort=False) if frames else pd.DataFrame(columns=df.columns)

    scenario_rows = []
    for label, frame in {
        "baseline_all": df.copy(),
        "deep_dev_selected_by_setup": concat_or_empty(selected_frames),
        "deep_strict_oos_pass_only": concat_or_empty(strict_frames),
    }.items():
        for split, metric in _stats_by_split(frame).items():
            row = {"label": label, "split": split}
            row.update(metric)
            scenario_rows.append(row)
    pd.DataFrame(scenario_rows).to_csv(OUT_DIR / "deep_setup_portfolio_scenarios.csv", index=False)

    print("[done] wrote deep_setup_final_by_setup.csv", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
