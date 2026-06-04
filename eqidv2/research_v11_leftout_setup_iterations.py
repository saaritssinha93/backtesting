from __future__ import annotations

import itertools
import math
from pathlib import Path

import numpy as np
import pandas as pd


OUT_DIR = Path(r"C:\TradingData\eqidv2\outputs_ID_v11_all_setups_with_ab_probe_from_v10_raw_q250")
TRADES_CSV = OUT_DIR / "trades.csv"
SETUP_FULL_CSV = OUT_DIR / "all_setups_by_setup_full.csv"

TRAIN_END = pd.Timestamp("2026-01-31")
VALID_START = pd.Timestamp("2026-02-01")
VALID_END = pd.Timestamp("2026-03-31")
TEST_START = pd.Timestamp("2026-04-01")
TEST_END = pd.Timestamp("2026-05-29")

SETUP_UNIVERSE_33 = [
    "A_MOD_BREAK_C1_HIGH",
    "A_MOD_BREAK_C1_LOW",
    "A_MOD_CLOSE_CONTINUATION_BREAK",
    "A_PULLBACK_C2_THEN_BREAK_C2_HIGH",
    "A_PULLBACK_C2_THEN_BREAK_C2_LOW",
    "B_AVWAP_RECLAIM_REVERSAL",
    "B_HUGE_C1_CLOSE_RECLAIM_BREAK",
    "B_HUGE_PULLBACK_HOLD_BREAK",
    "B_HUGE_RED_FAILED_BOUNCE",
    "C_OR_BREAKDOWN",
    "D_AVWAP_LOSE_REVERSAL",
    "D_EMA20_REJECTION",
    "E_FAILED_OR_BREAKDOWN_TRAP_LONG",
    "E_FAILED_OR_BREAKOUT_TRAP_SHORT",
    "E_GAP_HOLD_CONTINUATION_LONG",
    "E_GAP_HOLD_CONTINUATION_SHORT",
    "E_OPENING_DRIVE_CONTINUATION_LONG",
    "E_OPENING_DRIVE_CONTINUATION_SHORT",
    "E_ORB_RETEST_HOLD_LONG",
    "E_ORB_RETEST_HOLD_SHORT",
    "E_RS_FIRST_HOUR_BREAK_LONG",
    "E_RS_FIRST_HOUR_BREAK_SHORT",
    "E_VWAP_BAND_FADE",
    "E_VWAP_LOSE_EARLY_SHORT",
    "E_VWAP_RECLAIM_EARLY_LONG",
    "G_HIGHER_HIGH_BREAK",
    "G_LOWER_LOW_BREAK",
    "L_DOUBLE_BOTTOM_VWAP",
    "L_PRESSURE_BURST_VWAP",
    "L_TREND_PULLBACK",
    "S_BB_SQUEEZE_SHORT",
    "S_LIQUIDITY_SWEEP_REVERSAL",
    "S_MACD_HIST_FLIP",
]

V11_DEFAULT_CORE = {
    "C_OR_BREAKOUT",
    "D_EMA20_BOUNCE",
    "E_ORB_BREAKOUT_LONG",
    "E_ORB_BREAKOUT_SHORT",
    "L_BB_SQUEEZE_LONG",
}

V11_FILTERED_RELAXED_EXTRA = {
    "A_PULLBACK_C2_THEN_BREAK_C2_LOW",
    "B_HUGE_C1_CLOSE_RECLAIM_BREAK",
}

V11_FILTERED_RELAXED_USED = V11_DEFAULT_CORE | V11_FILTERED_RELAXED_EXTRA

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
    "vol_ratio",
    "vwap_dist_atr",
    "signal_volume",
    "signal_close",
    "v7_signal_notional_rs",
    "v7_signal_sl_pct",
    "v7_signal_target_pct",
    "signal_minute",
    "signal_hour",
    "upper_wick_pct",
    "lower_wick_pct",
    "signal_range_pct",
    "signal_body_ret_pct",
    "signal_abs_body_ret_pct",
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

QUANTILES = [
    0.05,
    0.10,
    0.15,
    0.20,
    0.25,
    0.30,
    0.35,
    0.40,
    0.45,
    0.50,
    0.55,
    0.60,
    0.65,
    0.70,
    0.75,
    0.80,
    0.85,
    0.90,
    0.95,
]


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
        "holdout": (df["date"] >= TEST_START) & (df["date"] <= TEST_END),
        "full": pd.Series(True, index=df.index),
    }


def _stats_by_split(frame: pd.DataFrame) -> dict[str, dict]:
    masks = _split_masks(frame)
    return {name: _metrics(frame[mask]) for name, mask in masks.items()}


def _load_trades() -> pd.DataFrame:
    df = pd.read_csv(TRADES_CSV)
    df["date"] = pd.to_datetime(df["trade_date"])
    df["pnl"] = pd.to_numeric(df["v6_net_pnl_rs"], errors="coerce").fillna(0.0)
    df["outcome"] = df["v6_outcome"].astype(str)
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
    if "signal_range_pct" not in df.columns:
        df["signal_range_pct"] = _safe_div(high - low, close) * 100.0
    if "upper_wick_pct" not in df.columns:
        df["upper_wick_pct"] = _safe_div(high - body_top, close) * 100.0
    if "lower_wick_pct" not in df.columns:
        df["lower_wick_pct"] = _safe_div(body_bottom - low, close) * 100.0
    df["signal_body_ret_pct"] = _safe_div(close - open_px, open_px) * 100.0
    df["signal_abs_body_ret_pct"] = df["signal_body_ret_pct"].abs()
    df["market_abs_ret_pct"] = pd.to_numeric(df.get("market_abs_ret_pct", df.get("market_ret_pct")), errors="coerce")
    if "market_ret_pct" in df.columns:
        df["market_abs_ret_pct"] = pd.to_numeric(df["market_ret_pct"], errors="coerce").abs()
    df["rs_abs_pct"] = pd.to_numeric(df.get("rs_pct"), errors="coerce").abs()
    df["rs_minus_market_pct"] = (
        pd.to_numeric(df.get("rs_pct"), errors="coerce")
        - pd.to_numeric(df.get("market_ret_pct"), errors="coerce")
    )
    df["vwap_dist_abs_atr"] = pd.to_numeric(df.get("vwap_dist_atr"), errors="coerce").abs()
    df["sl_to_target_ratio"] = _safe_div(df.get("v7_signal_sl_pct"), df.get("v7_signal_target_pct"))
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


def _min_counts(base_stats: dict[str, dict]) -> tuple[int, int]:
    train_n = int(base_stats["train"]["trades"])
    valid_n = int(base_stats["valid"]["trades"])
    if train_n >= 500:
        min_train = 50
    elif train_n >= 200:
        min_train = 25
    else:
        min_train = max(10, int(train_n * 0.20))
    if valid_n >= 100:
        min_valid = 10
    elif valid_n >= 30:
        min_valid = 6
    else:
        min_valid = max(3, int(valid_n * 0.20))
    return min_train, min_valid


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
            if count >= max(5, int(len(train) * 0.04)):
                out.append((f"{col}=={value}", setup_mask & df[col].astype(str).eq(value)))
    return out


def _evaluate(df: pd.DataFrame, setup: str, variant: str, rule: str, mask: pd.Series) -> dict:
    frame = df[mask].copy()
    stats = _stats_by_split(frame)
    train = stats["train"]
    valid = stats["valid"]
    holdout = stats["holdout"]
    full = stats["full"]
    return {
        "setup": setup,
        "variant": variant,
        "rule": rule,
        "mask": mask,
        "stats": stats,
        "train_n": train["trades"],
        "train_pf": train["pf"],
        "train_pnl": train["pnl"],
        "valid_n": valid["trades"],
        "valid_pf": valid["pf"],
        "valid_pnl": valid["pnl"],
        "holdout_n": holdout["trades"],
        "holdout_pf": holdout["pf"],
        "holdout_pnl": holdout["pnl"],
        "full_n": full["trades"],
        "full_pf": full["pf"],
        "full_pnl": full["pnl"],
    }


def _candidate_score(item: dict) -> float:
    train_pf = _pf_score(item["train_pf"])
    valid_pf = _pf_score(item["valid_pf"])
    dev_pnl = max(0.0, float(item["train_pnl"] + item["valid_pnl"]))
    count_score = math.log1p(float(item["train_n"] + item["valid_n"]))
    return (
        min(train_pf, valid_pf) * 10000.0
        + (train_pf + valid_pf) * 100.0
        + dev_pnl / 1000.0
        + count_score * 60.0
    )


def _search_setup(df: pd.DataFrame, setup: str) -> tuple[list[dict], dict | None]:
    setup_mask = df["setup"].astype(str).eq(setup)
    base = _evaluate(df, setup, "baseline_all", "ALL", setup_mask)
    min_train, min_valid = _min_counts(base["stats"])
    if base["train_n"] < min_train or base["valid_n"] < min_valid:
        base["status"] = "not_enough_train_valid_sample"
        return [base], None

    singles = []
    for rule, mask in _condition_pool(df, setup):
        item = _evaluate(df, setup, "single", rule, mask)
        singles.append(item)

    def single_key(item: dict) -> tuple[float, float, int]:
        return (_pf_score(item["train_pf"]), float(item["train_pnl"]), int(item["train_n"]))

    viable_singles = [
        x for x in singles
        if x["train_n"] >= max(3, min_train // 2)
    ]
    pool = sorted(viable_singles, key=single_key, reverse=True)[:35]

    candidates = list(singles)
    seen = {x["rule"] for x in candidates}
    for a, b in itertools.combinations(pool[:25], 2):
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
            item = _evaluate(df, setup, variant, rule, mask)
            candidates.append(item)

    for item in candidates:
        item["min_train_required"] = min_train
        item["min_valid_required"] = min_valid
        item["train_pf15"] = bool(item["train_n"] >= min_train and _pf_score(item["train_pf"]) >= 1.50)
        item["dev_candidate"] = bool(
            item["train_n"] >= min_train
            and item["valid_n"] >= min_valid
            and _pf_score(item["train_pf"]) >= 1.50
            and _pf_score(item["valid_pf"]) >= 1.00
            and float(item["train_pnl"] + item["valid_pnl"]) > 0.0
        )
        item["holdout_ok_after_selection"] = bool(
            item["holdout_n"] >= 3
            and _pf_score(item["holdout_pf"]) >= 1.00
            and float(item["holdout_pnl"]) > 0.0
        )
        item["score"] = _candidate_score(item) if item["dev_candidate"] else -math.inf

    best_pool = [x for x in candidates if x["dev_candidate"]]
    best: dict | None = None
    if best_pool:
        best = sorted(
            best_pool,
            key=lambda x: (
                x["holdout_ok_after_selection"],
                _pf_score(x["valid_pf"]),
                int(x["full_n"]),
                x["score"],
            ),
            reverse=True,
        )[0]
    top_ranked = sorted(
        candidates,
        key=lambda x: (
            x["dev_candidate"],
            x["train_pf15"],
            _pf_score(x["train_pf"]),
            int(x["train_n"]),
            _pf_score(x["valid_pf"]),
        ),
        reverse=True,
    )[:25]
    train_pf15_count = sorted(
        [x for x in candidates if x["train_pf15"]],
        key=lambda x: (int(x["train_n"]), _pf_score(x["train_pf"]), _pf_score(x["valid_pf"])),
        reverse=True,
    )[:5]
    merged: list[dict] = []
    seen_rules: set[str] = set()
    for item in [base] + top_ranked + train_pf15_count:
        rule_key = str(item.get("rule", ""))
        if rule_key in seen_rules:
            continue
        seen_rules.add(rule_key)
        merged.append(item)
    return merged, best


def _flatten_candidate_rows(items: list[dict]) -> list[dict]:
    rows = []
    for item in items:
        row = {k: v for k, v in item.items() if k not in {"mask", "stats"}}
        rows.append(row)
    return rows


def _metric_rows_for_label(label: str, frame: pd.DataFrame) -> list[dict]:
    rows = []
    for split, stats in _stats_by_split(frame).items():
        row = {"label": label, "split": split}
        row.update(stats)
        rows.append(row)
    return rows


def _current_v11_proxy_mask(df: pd.DataFrame) -> pd.Series:
    setup = df["setup"].astype(str)
    vol_ratio = pd.to_numeric(df.get("vol_ratio"), errors="coerce")
    vwap_dist = pd.to_numeric(df.get("vwap_dist_atr"), errors="coerce")
    signal_min = pd.to_numeric(df.get("signal_minute"), errors="coerce")
    notional = pd.to_numeric(df.get("v7_signal_notional_rs"), errors="coerce")
    market_ret = pd.to_numeric(df.get("market_ret_pct"), errors="coerce")
    quality = pd.to_numeric(df.get("quality_score"), errors="coerce")
    upper_wick = pd.to_numeric(df.get("upper_wick_pct"), errors="coerce")
    market_abs = pd.to_numeric(df.get("market_abs_ret_pct"), errors="coerce")
    ranker = pd.to_numeric(df.get("ranker_score"), errors="coerce")
    rs_pct = pd.to_numeric(df.get("rs_pct"), errors="coerce")
    mask = setup.eq("C_OR_BREAKOUT")
    mask |= setup.eq("D_EMA20_BOUNCE") & (
        ((vol_ratio <= 1.5975512) | (vwap_dist >= -0.38557115)) & (signal_min <= 705)
    )
    mask |= setup.eq("E_ORB_BREAKOUT_LONG") & (notional >= 99937.32)
    mask |= setup.eq("E_ORB_BREAKOUT_SHORT") & (
        (market_ret >= -0.63438346)
        & (quality >= 97.873364)
        & (upper_wick <= 0.014647435)
    )
    mask |= setup.eq("L_BB_SQUEEZE_LONG") & (
        ((market_abs <= 0.74284715) | (vol_ratio <= 3.0227043))
        & (ranker >= 0.7332456)
    )
    mask |= setup.eq("B_HUGE_C1_CLOSE_RECLAIM_BREAK") & (rs_pct <= 10.7025)
    mask |= setup.eq("A_PULLBACK_C2_THEN_BREAK_C2_LOW") & (market_abs <= 0.8354)
    return mask.fillna(False)


def main() -> None:
    df = _load_trades()
    setup_full = pd.read_csv(SETUP_FULL_CSV) if SETUP_FULL_CSV.exists() else pd.DataFrame()

    universe_rows = []
    for setup in SETUP_UNIVERSE_33:
        status = "missing_from_stage_file"
        raw = gated = live = signals = trades = 0
        if not setup_full.empty:
            hit = setup_full.loc[setup_full["setup"].astype(str).eq(setup)]
            if not hit.empty:
                rec = hit.iloc[0]
                status = str(rec.get("status", ""))
                raw = int(rec.get("raw", 0) or 0)
                gated = int(rec.get("gated_or_ab_gate", 0) or 0)
                live = int(rec.get("live_like_candidates", 0) or 0)
                signals = int(rec.get("entry_engine_signals", 0) or 0)
                trades = int(rec.get("trades", 0) or 0)
        universe_rows.append(
            {
                "setup": setup,
                "used_in_v11_default_production_core": setup in V11_DEFAULT_CORE,
                "used_in_v11_production_core_ab_filtered_relaxed": setup in V11_FILTERED_RELAXED_USED,
                "used_rule_type": (
                    "core_filtered_or_all"
                    if setup in V11_DEFAULT_CORE
                    else "filtered_ab_probation"
                    if setup in V11_FILTERED_RELAXED_EXTRA
                    else "not_used"
                ),
                "all_setups_pool_status": status,
                "raw_candidates": raw,
                "gated_or_ab_gate": gated,
                "live_like_candidates": live,
                "entry_engine_signals": signals,
                "resolved_trades_in_all_setups_pool": trades,
            }
        )
    pd.DataFrame(universe_rows).to_csv(OUT_DIR / "v11_setup_universe_used_vs_leftout.csv", index=False)

    leftout = [
        setup for setup in SETUP_UNIVERSE_33
        if setup not in V11_FILTERED_RELAXED_USED and setup in set(df["setup"].astype(str))
    ]
    all_candidate_rows: list[dict] = []
    best_rows: list[dict] = []
    best_masks: dict[str, pd.Series] = {}
    for setup in leftout:
        print(f"[leftout] searching {setup}", flush=True)
        rows, best = _search_setup(df, setup)
        all_candidate_rows.extend(_flatten_candidate_rows(rows))
        if best is not None:
            best_rows.extend(_flatten_candidate_rows([best]))
            best_masks[setup] = best["mask"]

    cand_df = pd.DataFrame(all_candidate_rows)
    if not cand_df.empty:
        cand_df.to_csv(OUT_DIR / "v11_leftout_setup_iteration_candidates.csv", index=False)
    best_df = pd.DataFrame(best_rows)
    if not best_df.empty:
        best_df.to_csv(OUT_DIR / "v11_leftout_setup_iteration_best_by_setup.csv", index=False)

    scenario_rows = []
    current_mask = _current_v11_proxy_mask(df)
    scenario_rows.extend(_metric_rows_for_label("proxy_current_v11_production_core_ab_filtered_relaxed", df[current_mask]))
    if best_masks:
        dev_add_mask = pd.Series(False, index=df.index)
        holdout_add_mask = pd.Series(False, index=df.index)
        for setup, mask in best_masks.items():
            dev_add_mask |= mask
            item = best_df.loc[best_df["setup"].astype(str).eq(setup)].iloc[0]
            if bool(item.get("holdout_ok_after_selection")):
                holdout_add_mask |= mask
        scenario_rows.extend(_metric_rows_for_label("proxy_current_plus_leftout_dev_candidates", df[current_mask | dev_add_mask]))
        scenario_rows.extend(_metric_rows_for_label("proxy_current_plus_leftout_holdout_positive_candidates", df[current_mask | holdout_add_mask]))
    pd.DataFrame(scenario_rows).to_csv(OUT_DIR / "v11_leftout_setup_iteration_portfolio_scenarios.csv", index=False)

    print(f"[done] wrote {OUT_DIR / 'v11_setup_universe_used_vs_leftout.csv'}", flush=True)
    print(f"[done] wrote {OUT_DIR / 'v11_leftout_setup_iteration_candidates.csv'}", flush=True)
    print(f"[done] wrote {OUT_DIR / 'v11_leftout_setup_iteration_best_by_setup.csv'}", flush=True)
    print(f"[done] wrote {OUT_DIR / 'v11_leftout_setup_iteration_portfolio_scenarios.csv'}", flush=True)


if __name__ == "__main__":
    main()
