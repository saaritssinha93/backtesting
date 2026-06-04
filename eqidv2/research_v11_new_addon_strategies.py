from __future__ import annotations

import itertools
import math
from pathlib import Path

import numpy as np
import pandas as pd

import avwap_5min_ID_v11_backtesting as v11
import research_v11_leftout_setup_iterations as base


OUT_DIR = Path(r"C:\TradingData\eqidv2\outputs_ID_v11_all_setups_with_ab_probe_from_v10_raw_q250")
TRADES_CSV = OUT_DIR / "trades.csv"
PROFILE = "production_core_ab_max_pnl_low_valid"

TRAIN_END = pd.Timestamp("2026-01-31")
VALID_START = pd.Timestamp("2026-02-01")
VALID_END = pd.Timestamp("2026-03-31")
HOLDOUT_START = pd.Timestamp("2026-04-01")
HOLDOUT_END = pd.Timestamp("2026-05-29")

LEFTOVER_SETUPS = [
    "A_MOD_BREAK_C1_HIGH",
    "A_PULLBACK_C2_THEN_BREAK_C2_HIGH",
    "B_HUGE_RED_FAILED_BOUNCE",
    "D_EMA20_REJECTION",
    "G_HIGHER_HIGH_BREAK",
]

NUMERIC_FEATURES = [
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
    "signal_range_pct",
    "signal_body_ret_pct",
    "signal_abs_body_ret_pct",
    "upper_wick_pct",
    "lower_wick_pct",
]

CATEGORICAL_FEATURES = ["regime", "candidate_family", "selection_mode", "candle_color", "time_bucket"]
QUANTILES = [0.05, 0.10, 0.15, 0.20, 0.25, 0.30, 0.35, 0.40, 0.45, 0.50,
             0.55, 0.60, 0.65, 0.70, 0.75, 0.80, 0.85, 0.90, 0.95]


def _safe_div(a: pd.Series, b: pd.Series) -> pd.Series:
    a = pd.to_numeric(a, errors="coerce")
    b = pd.to_numeric(b, errors="coerce")
    return a / b.replace(0, np.nan)


def _pf(pnl: pd.Series | np.ndarray) -> float:
    s = pd.Series(pnl, dtype="float64").fillna(0.0)
    gains = float(s[s > 0].sum())
    losses = float(-s[s < 0].sum())
    if losses <= 0:
        return math.inf if gains > 0 else math.nan
    return gains / losses


def _pf_score(value: float) -> float:
    if pd.isna(value):
        return -1.0
    if value == math.inf:
        return 8.0
    if value == -math.inf:
        return -1.0
    return float(value)


def _metrics(frame: pd.DataFrame) -> dict:
    if frame.empty:
        return {"trades": 0, "days": 0, "pnl": 0.0, "pf": math.nan, "win_pct": math.nan, "avg_trade": math.nan}
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
        "full": pd.Series(True, index=df.index),
    }


def _stats_by_split(frame: pd.DataFrame) -> dict[str, dict]:
    masks = _split_masks(frame)
    return {name: _metrics(frame[mask]) for name, mask in masks.items()}


def _load_trades() -> pd.DataFrame:
    df = pd.read_csv(TRADES_CSV)
    accepted, _, _ = v11._apply_selected_strategy_profile(df, PROFILE)
    selected_ids = set(accepted.get("signal_id", pd.Series(dtype=str)).astype(str))
    df = df.loc[~df.get("signal_id", pd.Series("", index=df.index)).astype(str).isin(selected_ids)].copy()
    df = df.loc[df["setup"].astype(str).isin(LEFTOVER_SETUPS)].copy()
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
    body_top = pd.concat([open_px, close], axis=1).max(axis=1)
    body_bottom = pd.concat([open_px, close], axis=1).min(axis=1)
    df["signal_range_pct"] = _safe_div(high - low, close) * 100.0
    df["signal_body_ret_pct"] = _safe_div(close - open_px, open_px) * 100.0
    df["signal_abs_body_ret_pct"] = df["signal_body_ret_pct"].abs()
    df["upper_wick_pct"] = _safe_div(high - body_top, close) * 100.0
    df["lower_wick_pct"] = _safe_div(body_bottom - low, close) * 100.0
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
        [mins <= 630, mins <= 720, mins <= 810],
        ["OPEN_0915_1030", "MID_1031_1200", "AFTERNOON_1201_1330"],
        default="LATE_1331_1500",
    )
    for col in NUMERIC_FEATURES:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def _condition_pool(df: pd.DataFrame, setup: str) -> list[tuple[str, pd.Series]]:
    setup_mask = df["setup"].astype(str).eq(setup)
    train = df[setup_mask & _split_masks(df)["train"]]
    out: list[tuple[str, pd.Series]] = [("ALL", setup_mask)]
    for col in NUMERIC_FEATURES:
        if col not in df.columns:
            continue
        values = pd.to_numeric(train[col], errors="coerce").dropna()
        if values.nunique() < 8:
            continue
        series = pd.to_numeric(df[col], errors="coerce")
        for q in QUANTILES:
            threshold = float(values.quantile(q))
            if not np.isfinite(threshold):
                continue
            out.append((f"{col}>={threshold:.8g}", setup_mask & (series >= threshold)))
            out.append((f"{col}<={threshold:.8g}", setup_mask & (series <= threshold)))
    for col in CATEGORICAL_FEATURES:
        if col not in df.columns:
            continue
        counts = train[col].astype(str).value_counts()
        for value, count in counts.items():
            if count >= max(4, int(len(train) * 0.04)):
                out.append((f"{col}=={value}", setup_mask & df[col].astype(str).eq(value)))
    return out


def _evaluate(df: pd.DataFrame, setup: str, variant: str, rule: str, mask: pd.Series) -> dict:
    frame = df[mask].copy()
    stats = _stats_by_split(frame)
    row = {"setup": setup, "variant": variant, "rule": rule, "mask": mask}
    for split, m in stats.items():
        for key, value in m.items():
            row[f"{split}_{key}"] = value
    row["strict_candidate"] = bool(
        row["train_trades"] >= 10
        and row["valid_trades"] >= 3
        and row["holdout_trades"] >= 3
        and _pf_score(row["train_pf"]) >= 1.5
        and _pf_score(row["valid_pf"]) >= 1.0
        and _pf_score(row["holdout_pf"]) >= 1.0
        and row["valid_pnl"] > 0
        and row["holdout_pnl"] > 0
        and _pf_score(row["full_pf"]) >= 1.3
    )
    row["quality_candidate"] = bool(
        row["train_trades"] >= 10
        and row["valid_trades"] >= 2
        and row["holdout_trades"] >= 2
        and _pf_score(row["train_pf"]) >= 1.5
        and row["valid_pnl"] > 0
        and row["holdout_pnl"] > 0
        and _pf_score(row["full_pf"]) >= 1.5
    )
    row["train_only_candidate"] = bool(row["train_trades"] >= 15 and _pf_score(row["train_pf"]) >= 1.5)
    return row


def _search_setup(df: pd.DataFrame, setup: str) -> list[dict]:
    singles = [_evaluate(df, setup, "single", rule, mask) for rule, mask in _condition_pool(df, setup)]

    def single_key(item: dict) -> tuple[float, int, float]:
        return (_pf_score(item["train_pf"]), int(item["train_trades"]), float(item["train_pnl"]))

    pool = sorted(
        [x for x in singles if x["train_trades"] >= 5 and _pf_score(x["train_pf"]) >= 1.0],
        key=single_key,
        reverse=True,
    )[:50]
    candidates = list(singles)
    seen = {x["rule"] for x in candidates}
    for a, b in itertools.combinations(pool[:35], 2):
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
            candidates.append(_evaluate(df, setup, variant, rule, mask))
    return candidates


def _strip_mask(row: dict) -> dict:
    return {k: v for k, v in row.items() if k != "mask"}


def _scenario_rows(df: pd.DataFrame, label: str, mask: pd.Series) -> list[dict]:
    rows = []
    for split, m in _stats_by_split(df[mask]).items():
        row = {"label": label, "split": split}
        row.update(m)
        rows.append(row)
    return rows


def main() -> None:
    df = _load_trades()
    print("[load] leftover resolved trades by setup")
    print(df["setup"].value_counts().to_string())

    all_rows: list[dict] = []
    for setup in LEFTOVER_SETUPS:
        print(f"[search] {setup}", flush=True)
        all_rows.extend(_strip_mask(x) for x in _search_setup(df, setup))

    candidates = pd.DataFrame(all_rows)
    candidates["score"] = (
        candidates["strict_candidate"].astype(int) * 1_000_000
        + candidates["quality_candidate"].astype(int) * 500_000
        + candidates["holdout_pf"].map(_pf_score) * 10_000
        + candidates["valid_pf"].map(_pf_score) * 2_500
        + candidates["full_trades"] * 100
        + candidates["full_pnl"] / 100.0
    )
    candidates = candidates.sort_values(
        ["strict_candidate", "quality_candidate", "full_trades", "holdout_trades", "full_pf", "train_pf"],
        ascending=False,
    )
    candidates.to_csv(OUT_DIR / "v11_new_addon_strategy_candidates.csv", index=False)

    best_rows = []
    for setup, group in candidates.groupby("setup", sort=True):
        pool = group.loc[group["quality_candidate"].astype(bool)]
        if pool.empty:
            pool = group.loc[group["strict_candidate"].astype(bool)]
        if pool.empty:
            pool = group.loc[group["train_only_candidate"].astype(bool)]
        if not pool.empty:
            best_rows.append(pool.sort_values(["quality_candidate", "strict_candidate", "full_trades", "full_pf"], ascending=False).iloc[0])
    best = pd.DataFrame(best_rows)
    if not best.empty:
        best.to_csv(OUT_DIR / "v11_new_addon_strategy_best_by_setup.csv", index=False)

    scenario_rows = []
    selected_masks = {}
    for _, row in best.iterrows():
        # Recompute only the selected mask for portfolio scenarios.
        setup = str(row["setup"])
        rule = str(row["rule"])
        for candidate in _search_setup(df, setup):
            if str(candidate["rule"]) == rule:
                selected_masks[setup] = candidate["mask"]
                break

    strict_mask = pd.Series(False, index=df.index)
    quality_mask = pd.Series(False, index=df.index)
    train_only_mask = pd.Series(False, index=df.index)
    for setup, mask in selected_masks.items():
        item = best.loc[best["setup"].astype(str).eq(setup)].iloc[0]
        if bool(item.get("strict_candidate", False)):
            strict_mask |= mask
        if bool(item.get("quality_candidate", False)):
            quality_mask |= mask
        if bool(item.get("train_only_candidate", False)):
            train_only_mask |= mask
    scenario_rows += _scenario_rows(df, "new_addons_strict_only", strict_mask)
    scenario_rows += _scenario_rows(df, "new_addons_quality_only", quality_mask)
    scenario_rows += _scenario_rows(df, "new_addons_train_pf15_best_by_setup", train_only_mask)
    pd.DataFrame(scenario_rows).to_csv(OUT_DIR / "v11_new_addon_strategy_scenarios.csv", index=False)

    print("[best]")
    if best.empty:
        print("no candidates")
    else:
        cols = [
            "setup", "variant", "rule",
            "train_trades", "train_pf", "train_pnl",
            "valid_trades", "valid_pf", "valid_pnl",
            "holdout_trades", "holdout_pf", "holdout_pnl",
            "full_trades", "full_pf", "full_pnl",
            "quality_candidate", "strict_candidate",
        ]
        print(best[cols].to_string(index=False, max_colwidth=120))
    print("[done]", flush=True)


if __name__ == "__main__":
    main()
