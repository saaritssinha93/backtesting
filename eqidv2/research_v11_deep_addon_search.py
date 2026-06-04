from __future__ import annotations

import itertools
import json
import math
from pathlib import Path

import numpy as np
import pandas as pd

import avwap_5min_ID_v11_backtesting as v11


OUT_DIR = Path(r"C:\TradingData\eqidv2\outputs_ID_v11_all_setups_with_ab_probe_from_v10_raw_q250")
TRADES_CSV = OUT_DIR / "trades.csv"
PROFILE = "production_core_ab_max_pnl_low_valid"

TRAIN_END = pd.Timestamp("2026-01-31")
VALID_START = pd.Timestamp("2026-02-01")
VALID_END = pd.Timestamp("2026-03-31")
HOLDOUT_START = pd.Timestamp("2026-04-01")
HOLDOUT_END = pd.Timestamp("2026-05-29")

QUANTILES = [
    0.02, 0.05, 0.10, 0.15, 0.20, 0.25, 0.30, 0.35, 0.40, 0.45,
    0.50, 0.55, 0.60, 0.65, 0.70, 0.75, 0.80, 0.85, 0.90, 0.95, 0.98,
]
RANGE_QUANTILE_PAIRS = [(0.10, 0.90), (0.15, 0.85), (0.20, 0.80), (0.25, 0.75), (0.30, 0.70)]

BASE_NUMERIC_FEATURES = [
    "ranker_score",
    "quality_score",
    "score",
    "atr_pct",
    "body_pct",
    "close_loc",
    "market_ret_pct",
    "market_abs_ret_pct",
    "rs_pct",
    "rs_abs_pct",
    "rs_minus_market_pct",
    "vol_ratio",
    "vwap_dist_atr",
    "vwap_dist_abs_atr",
    "signal_volume",
    "signal_close",
    "v7_signal_notional_rs",
    "signal_minute",
    "signal_hour",
    "signal_range_pct",
    "signal_body_ret_pct",
    "signal_abs_body_ret_pct",
    "upper_wick_pct",
    "lower_wick_pct",
    "day_value_so_far_rs",
    "log_day_value_so_far_rs",
    "side_aligned_rs_pct",
    "side_aligned_market_ret_pct",
    "side_aligned_vwap_dist_atr",
    "side_aligned_body_ret_pct",
    "side_aligned_close_loc",
    "directional_wick_against_pct",
    "directional_wick_with_pct",
]

CATEGORICAL_FEATURES = [
    "setup",
    "side",
    "regime",
    "candidate_family",
    "selection_mode",
    "candle_color",
    "time_bucket",
    "reason",
]


def _safe_div(a: pd.Series, b: pd.Series) -> pd.Series:
    a = pd.to_numeric(a, errors="coerce")
    b = pd.to_numeric(b, errors="coerce")
    return a / b.replace(0, np.nan)


def _pf(values: pd.Series | np.ndarray) -> float:
    s = pd.Series(values, dtype="float64").fillna(0.0)
    gains = float(s[s > 0].sum())
    losses = float(-s[s < 0].sum())
    if losses <= 0:
        return math.inf if gains > 0 else math.nan
    return gains / losses


def _pf_score(value: float) -> float:
    if pd.isna(value):
        return -1.0
    if value == math.inf:
        return 10.0
    if value == -math.inf:
        return -1.0
    return float(value)


def _metrics(frame: pd.DataFrame) -> dict:
    if frame.empty:
        return {
            "trades": 0,
            "days": 0,
            "pnl": 0.0,
            "pf": math.nan,
            "win_pct": math.nan,
            "avg_trade": math.nan,
        }
    pnl = pd.to_numeric(frame["pnl"], errors="coerce").fillna(0.0)
    return {
        "trades": int(len(frame)),
        "days": int(frame["date"].dt.date.nunique()),
        "pnl": float(pnl.sum()),
        "pf": float(_pf(pnl)),
        "win_pct": float((pnl > 0).mean() * 100.0),
        "avg_trade": float(pnl.mean()),
    }


def _split_masks(df: pd.DataFrame) -> dict[str, pd.Series]:
    return {
        "train": df["date"] <= TRAIN_END,
        "valid": (df["date"] >= VALID_START) & (df["date"] <= VALID_END),
        "holdout": (df["date"] >= HOLDOUT_START) & (df["date"] <= HOLDOUT_END),
        "train_valid": df["date"] <= VALID_END,
        "full": pd.Series(True, index=df.index),
    }


def _stats_by_split(frame: pd.DataFrame) -> dict[str, dict]:
    masks = _split_masks(frame)
    return {name: _metrics(frame[mask]) for name, mask in masks.items()}


def _parse_diagnostic(value: object, key: str) -> object:
    if pd.isna(value):
        return np.nan
    try:
        payload = json.loads(str(value))
    except Exception:
        return np.nan
    return payload.get(key, np.nan)


def _load_leftover_trades() -> pd.DataFrame:
    df = pd.read_csv(TRADES_CSV)
    accepted, _, _ = v11._apply_selected_strategy_profile(df, PROFILE)
    selected_ids = set(accepted.get("signal_id", pd.Series(dtype=str)).astype(str))
    df = df.loc[~df.get("signal_id", pd.Series("", index=df.index)).astype(str).isin(selected_ids)].copy()

    df["date"] = pd.to_datetime(df["trade_date"])
    df["pnl"] = pd.to_numeric(df["v6_net_pnl_rs"], errors="coerce").fillna(0.0)
    sig = pd.to_datetime(df.get("signal_time_ist"), errors="coerce")
    df["signal_minute"] = sig.dt.hour * 60 + sig.dt.minute
    df["signal_hour"] = sig.dt.hour

    for col in ["signal_open", "signal_high", "signal_low", "signal_close"]:
        df[col] = pd.to_numeric(df.get(col), errors="coerce")
    open_px = df["signal_open"]
    high = df["signal_high"]
    low = df["signal_low"]
    close = df["signal_close"]
    close_safe = close.replace(0, np.nan)
    body_top = pd.concat([open_px, close], axis=1).max(axis=1)
    body_bottom = pd.concat([open_px, close], axis=1).min(axis=1)
    df["signal_range_pct"] = _safe_div(high - low, close_safe) * 100.0
    df["signal_body_ret_pct"] = _safe_div(close - open_px, open_px) * 100.0
    df["signal_abs_body_ret_pct"] = df["signal_body_ret_pct"].abs()
    df["upper_wick_pct"] = _safe_div(high - body_top, close_safe) * 100.0
    df["lower_wick_pct"] = _safe_div(body_bottom - low, close_safe) * 100.0
    df["market_abs_ret_pct"] = pd.to_numeric(df.get("market_ret_pct"), errors="coerce").abs()
    df["rs_abs_pct"] = pd.to_numeric(df.get("rs_pct"), errors="coerce").abs()
    df["rs_minus_market_pct"] = (
        pd.to_numeric(df.get("rs_pct"), errors="coerce")
        - pd.to_numeric(df.get("market_ret_pct"), errors="coerce")
    )
    df["vwap_dist_abs_atr"] = pd.to_numeric(df.get("vwap_dist_atr"), errors="coerce").abs()
    df["candle_color"] = np.where(close > open_px, "GREEN", np.where(close < open_px, "RED", "DOJI"))

    mins = pd.to_numeric(df["signal_minute"], errors="coerce")
    df["time_bucket"] = np.select(
        [mins <= 600, mins <= 690, mins <= 780, mins <= 840],
        ["OPEN_0915_1000", "MORNING_1001_1130", "MIDDAY_1131_1300", "AFTERNOON_1301_1400"],
        default="LATE_1401_1500",
    )

    if "diagnostics_json" in df.columns:
        df["reason"] = df["diagnostics_json"].map(lambda x: _parse_diagnostic(x, "reason"))
        df["day_value_so_far_rs"] = pd.to_numeric(
            df["diagnostics_json"].map(lambda x: _parse_diagnostic(x, "day_value_so_far_rs")),
            errors="coerce",
        )
    else:
        df["reason"] = ""
        df["day_value_so_far_rs"] = np.nan
    df["log_day_value_so_far_rs"] = np.log10(pd.to_numeric(df["day_value_so_far_rs"], errors="coerce").clip(lower=1.0))

    side = df.get("side", pd.Series("", index=df.index)).astype(str).str.upper()
    side_sign = np.where(side.eq("SHORT"), -1.0, 1.0)
    df["side_aligned_rs_pct"] = pd.to_numeric(df.get("rs_pct"), errors="coerce") * side_sign
    df["side_aligned_market_ret_pct"] = pd.to_numeric(df.get("market_ret_pct"), errors="coerce") * side_sign
    df["side_aligned_vwap_dist_atr"] = pd.to_numeric(df.get("vwap_dist_atr"), errors="coerce") * side_sign
    df["side_aligned_body_ret_pct"] = pd.to_numeric(df["signal_body_ret_pct"], errors="coerce") * side_sign
    close_loc = pd.to_numeric(df.get("close_loc"), errors="coerce")
    df["side_aligned_close_loc"] = np.where(side.eq("SHORT"), 1.0 - close_loc, close_loc)
    upper = pd.to_numeric(df["upper_wick_pct"], errors="coerce")
    lower = pd.to_numeric(df["lower_wick_pct"], errors="coerce")
    df["directional_wick_against_pct"] = np.where(side.eq("SHORT"), lower, upper)
    df["directional_wick_with_pct"] = np.where(side.eq("SHORT"), upper, lower)

    for col in BASE_NUMERIC_FEATURES:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    for col in CATEGORICAL_FEATURES:
        if col in df.columns:
            df[col] = df[col].fillna("").astype(str)
    return df


def _base_groups(df: pd.DataFrame) -> list[tuple[str, pd.Series]]:
    groups: list[tuple[str, pd.Series]] = [("ALL_LEFTOVER", pd.Series(True, index=df.index))]
    for col in ["side", "regime", "time_bucket", "candidate_family"]:
        if col not in df.columns:
            continue
        for value, count in df[col].astype(str).value_counts().items():
            if count >= 80:
                groups.append((f"{col}={value}", df[col].astype(str).eq(value)))
    for setup, count in df["setup"].astype(str).value_counts().items():
        if count >= 20:
            groups.append((f"setup={setup}", df["setup"].astype(str).eq(setup)))
    if "reason" in df.columns:
        for reason, count in df["reason"].astype(str).value_counts().items():
            if count >= 60 and reason:
                groups.append((f"reason={reason}", df["reason"].astype(str).eq(reason)))
    return groups


def _condition_pool(df: pd.DataFrame, group_mask: pd.Series) -> list[tuple[str, pd.Series]]:
    train_mask = _split_masks(df)["train"] & group_mask
    train = df[train_mask]
    out: list[tuple[str, pd.Series]] = [("ALL", group_mask)]

    for col in BASE_NUMERIC_FEATURES:
        if col not in df.columns:
            continue
        values = pd.to_numeric(train[col], errors="coerce").dropna()
        if len(values) < 12 or values.nunique() < 8:
            continue
        series = pd.to_numeric(df[col], errors="coerce")
        thresholds = []
        for q in QUANTILES:
            threshold = float(values.quantile(q))
            if np.isfinite(threshold):
                thresholds.append((q, threshold))
        seen = set()
        for _, threshold in thresholds:
            key = round(threshold, 10)
            if key in seen:
                continue
            seen.add(key)
            out.append((f"{col}>={threshold:.8g}", group_mask & (series >= threshold)))
            out.append((f"{col}<={threshold:.8g}", group_mask & (series <= threshold)))
        quantile_map = {q: threshold for q, threshold in thresholds}
        for low_q, high_q in RANGE_QUANTILE_PAIRS:
            low = quantile_map.get(low_q)
            high = quantile_map.get(high_q)
            if low is None or high is None or not np.isfinite(low) or not np.isfinite(high) or low >= high:
                continue
            out.append((f"{low:.8g}<={col}<={high:.8g}", group_mask & (series >= low) & (series <= high)))

    for col in CATEGORICAL_FEATURES:
        if col not in df.columns:
            continue
        values = train[col].astype(str).value_counts()
        for value, count in values.items():
            min_count = max(6, int(len(train) * 0.03))
            if count >= min_count and value:
                out.append((f"{col}=={value}", group_mask & df[col].astype(str).eq(value)))
    return out


def _evaluate(df: pd.DataFrame, group: str, variant: str, rule: str, mask: pd.Series) -> dict:
    frame = df[mask].copy()
    stats = _stats_by_split(frame)
    row = {"group": group, "variant": variant, "rule": rule, "mask": mask}
    for split, values in stats.items():
        for key, value in values.items():
            row[f"{split}_{key}"] = value

    train_pf = _pf_score(row["train_pf"])
    valid_pf = _pf_score(row["valid_pf"])
    holdout_pf = _pf_score(row["holdout_pf"])
    train_valid_pf = _pf_score(row["train_valid_pf"])
    full_pf = _pf_score(row["full_pf"])

    # Selection-quality uses train + validation only. Holdout is deliberately not used here.
    row["selected_by_train_valid"] = bool(
        row["train_trades"] >= 20
        and row["valid_trades"] >= 6
        and train_pf >= 1.5
        and valid_pf >= 1.10
        and train_valid_pf >= 1.45
        and row["valid_pnl"] > 0
        and row["train_valid_pnl"] > 0
    )
    row["selected_looser_count"] = bool(
        row["train_trades"] >= 35
        and row["valid_trades"] >= 5
        and train_pf >= 1.5
        and valid_pf >= 1.0
        and row["valid_pnl"] > 0
        and row["train_valid_pnl"] > 0
    )
    # Honest pass is checked after selection; this is the out-of-sample survival label.
    row["holdout_survived"] = bool(
        row["selected_by_train_valid"]
        and row["holdout_trades"] >= 5
        and holdout_pf >= 1.0
        and row["holdout_pnl"] > 0
        and full_pf >= 1.35
    )
    row["strong_holdout_survived"] = bool(
        row["selected_by_train_valid"]
        and row["holdout_trades"] >= 8
        and holdout_pf >= 1.20
        and row["holdout_pnl"] > 0
        and full_pf >= 1.50
    )
    row["score_train_valid"] = (
        min(train_pf, 3.0) * 400000
        + min(valid_pf, 3.0) * 500000
        + math.log1p(max(row["train_valid_trades"], 0)) * 70000
        + row["train_valid_pnl"]
    )
    row["score_count"] = (
        min(train_pf, 2.2) * 250000
        + min(valid_pf, 2.2) * 300000
        + row["train_valid_trades"] * 600
        + row["train_valid_pnl"]
    )
    return row


def _strip_mask(row: dict) -> dict:
    return {k: v for k, v in row.items() if k != "mask"}


def _search_group(df: pd.DataFrame, group: str, group_mask: pd.Series) -> list[dict]:
    pool = _condition_pool(df, group_mask)
    singles = [_evaluate(df, group, "single", rule, mask) for rule, mask in pool]

    def promising_single(item: dict) -> bool:
        return (
            item["train_trades"] >= 8
            and _pf_score(item["train_pf"]) >= 1.0
            and item["train_pnl"] > -5000
        )

    def key_train(item: dict) -> tuple[float, int, float]:
        return (_pf_score(item["train_pf"]), int(item["train_trades"]), float(item["train_pnl"]))

    promising = sorted([x for x in singles if promising_single(x)], key=key_train, reverse=True)[:90]
    candidates = list(singles)
    seen = {x["rule"] for x in candidates}

    for a, b in itertools.combinations(promising[:70], 2):
        if a["rule"] == b["rule"]:
            continue
        mask = a["mask"] & b["mask"]
        rule = f"({a['rule']}) AND ({b['rule']})"
        if rule in seen:
            continue
        seen.add(rule)
        candidates.append(_evaluate(df, group, "double_and", rule, mask))

    double_pool = sorted(
        [
            x for x in candidates
            if x["variant"] == "double_and"
            and x["train_trades"] >= 10
            and _pf_score(x["train_pf"]) >= 1.35
        ],
        key=lambda x: (x["train_valid_pnl"], _pf_score(x["train_valid_pf"]), x["train_valid_trades"]),
        reverse=True,
    )[:60]
    for pair in double_pool[:40]:
        for single in promising[:45]:
            if single["rule"] in pair["rule"]:
                continue
            mask = pair["mask"] & single["mask"]
            rule = f"({pair['rule']}) AND ({single['rule']})"
            if rule in seen:
                continue
            seen.add(rule)
            candidates.append(_evaluate(df, group, "triple_and", rule, mask))
    return candidates


def _dedupe_rules(candidates: pd.DataFrame) -> pd.DataFrame:
    if candidates.empty:
        return candidates
    sort_cols = ["selected_by_train_valid", "holdout_survived", "train_valid_pnl", "full_pnl", "train_valid_trades"]
    out = candidates.sort_values(sort_cols, ascending=[False, False, False, False, False])
    return out.drop_duplicates(subset=["group", "rule"], keep="first").reset_index(drop=True)


def _portfolio_metrics(df: pd.DataFrame, label: str, masks: list[pd.Series]) -> list[dict]:
    if masks:
        selected_mask = masks[0].copy()
        for mask in masks[1:]:
            selected_mask |= mask
    else:
        selected_mask = pd.Series(False, index=df.index)
    frame = df[selected_mask].drop_duplicates(subset=["signal_id"]).copy()
    rows = []
    for split, stats in _stats_by_split(frame).items():
        row = {"label": label, "split": split}
        row.update(stats)
        rows.append(row)
    return rows


def _greedy_portfolio(df: pd.DataFrame, rules: list[dict]) -> tuple[list[dict], list[dict]]:
    chosen: list[dict] = []
    chosen_masks: list[pd.Series] = []
    used_ids: set[str] = set()

    def frame_from_masks(masks: list[pd.Series]) -> pd.DataFrame:
        if not masks:
            return df.iloc[0:0].copy()
        mask = masks[0].copy()
        for item in masks[1:]:
            mask |= item
        return df[mask].drop_duplicates(subset=["signal_id"]).copy()

    for rule in sorted(rules, key=lambda x: (x["train_valid_pnl"], x["train_valid_trades"]), reverse=True):
        frame = df[rule["mask"]]
        new_ids = set(frame["signal_id"].astype(str)) - used_ids
        if len(new_ids) < 8:
            continue
        test_masks = chosen_masks + [rule["mask"]]
        tv = frame_from_masks(test_masks)
        masks = _split_masks(tv)
        train = _metrics(tv[masks["train"]])
        valid = _metrics(tv[masks["valid"]])
        train_valid = _metrics(tv[masks["train_valid"]])
        if (
            train["trades"] >= 20
            and valid["trades"] >= 6
            and _pf_score(train["pf"]) >= 1.5
            and _pf_score(valid["pf"]) >= 1.05
            and _pf_score(train_valid["pf"]) >= 1.45
            and valid["pnl"] > 0
        ):
            chosen.append(rule)
            chosen_masks.append(rule["mask"])
            used_ids.update(new_ids)
    return chosen, _portfolio_metrics(df, "deep_addon_greedy_train_valid_selected", chosen_masks)


def main() -> None:
    df = _load_leftover_trades()
    print(f"[load] leftover trades after {PROFILE}: {len(df):,}")
    by_setup = df.groupby("setup")["pnl"].agg(["count", "sum"]).sort_values("count", ascending=False)
    print(by_setup.head(20).to_string())

    all_rows: list[dict] = []
    all_with_masks: list[dict] = []
    groups = _base_groups(df)
    print(f"[groups] {len(groups)} groups")
    for i, (group, mask) in enumerate(groups, 1):
        group_count = int(mask.sum())
        train_count = int((_split_masks(df)["train"] & mask).sum())
        if train_count < 12:
            continue
        print(f"[search {i:02d}/{len(groups):02d}] {group} rows={group_count:,} train={train_count:,}")
        rows = _search_group(df, group, mask)
        for row in rows:
            if (
                row["selected_by_train_valid"]
                or row["selected_looser_count"]
                or row["holdout_survived"]
                or row["strong_holdout_survived"]
                or (row["train_trades"] >= 20 and _pf_score(row["train_pf"]) >= 1.5)
            ):
                all_rows.append(_strip_mask(row))
                all_with_masks.append(row)

    candidates = _dedupe_rules(pd.DataFrame(all_rows))
    candidates.to_csv(OUT_DIR / "v11_deep_addon_candidate_rules.csv", index=False)
    if candidates.empty:
        print("[done] no candidates found")
        return

    selected = candidates[candidates["selected_by_train_valid"]].copy()
    selected.to_csv(OUT_DIR / "v11_deep_addon_train_valid_selected.csv", index=False)
    survived = candidates[candidates["holdout_survived"]].copy()
    survived.to_csv(OUT_DIR / "v11_deep_addon_holdout_survivors.csv", index=False)
    strong = candidates[candidates["strong_holdout_survived"]].copy()
    strong.to_csv(OUT_DIR / "v11_deep_addon_strong_holdout_survivors.csv", index=False)

    best_by_group = (
        selected.sort_values(["holdout_survived", "train_valid_pnl", "full_pnl"], ascending=[False, False, False])
        .drop_duplicates(subset=["group"], keep="first")
        .reset_index(drop=True)
    )
    best_by_group.to_csv(OUT_DIR / "v11_deep_addon_best_by_group.csv", index=False)

    selected_keys = set(zip(selected["group"], selected["rule"]))
    selected_rules = [r for r in all_with_masks if (r["group"], r["rule"]) in selected_keys]
    chosen, portfolio = _greedy_portfolio(df, selected_rules)
    pd.DataFrame([_strip_mask(x) for x in chosen]).to_csv(OUT_DIR / "v11_deep_addon_greedy_selected_rules.csv", index=False)
    pd.DataFrame(portfolio).to_csv(OUT_DIR / "v11_deep_addon_portfolio_scenarios.csv", index=False)

    print("[summary] selected_by_train_valid:", len(selected))
    print("[summary] holdout_survived:", len(survived))
    print("[summary] strong_holdout_survived:", len(strong))
    print("[best holdout survivors]")
    show_cols = [
        "group", "variant", "rule",
        "train_trades", "train_pf", "train_pnl",
        "valid_trades", "valid_pf", "valid_pnl",
        "holdout_trades", "holdout_pf", "holdout_pnl",
        "full_trades", "full_pf", "full_pnl",
    ]
    print(survived.sort_values(["full_pnl", "full_trades"], ascending=False).head(20)[show_cols].to_string(index=False))
    print("[portfolio]")
    print(pd.DataFrame(portfolio).to_string(index=False))


if __name__ == "__main__":
    main()
