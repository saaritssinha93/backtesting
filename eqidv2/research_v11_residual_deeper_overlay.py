from __future__ import annotations

import itertools
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

QUANTILES = [0.10, 0.15, 0.20, 0.25, 0.30, 0.35, 0.40, 0.45, 0.50,
             0.55, 0.60, 0.65, 0.70, 0.75, 0.80, 0.85, 0.90]

NUMERIC_FEATURES = [
    "ranker_score",
    "quality_score",
    "score",
    "atr_pct",
    "body_pct",
    "close_loc",
    "market_ret_pct",
    "market_abs_ret_pct",
    "market_with_side",
    "rs_pct",
    "rs_abs_pct",
    "rs_with_side",
    "rs_minus_market_pct",
    "vol_ratio",
    "vwap_dist_atr",
    "vwap_dist_abs_atr",
    "vwap_with_side",
    "signal_volume",
    "signal_close",
    "v7_signal_notional_rs",
    "notional_gap_rs",
    "signal_minute",
    "signal_range_pct",
    "signal_body_ret_pct",
    "signal_abs_body_ret_pct",
    "upper_wick_pct",
    "lower_wick_pct",
    "wick_skew_pct",
    "body_to_range",
    "close_in_range",
]

CATEGORICAL_FEATURES = [
    "side",
    "regime",
    "candidate_family",
    "selection_mode",
    "candle_color",
    "time_bucket_45",
]


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
    return float(value)


def _safe_div(a: pd.Series, b: pd.Series) -> pd.Series:
    a = pd.to_numeric(a, errors="coerce")
    b = pd.to_numeric(b, errors="coerce")
    return a / b.replace(0, np.nan)


def _add_features(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["date"] = pd.to_datetime(out["trade_date"])
    out["pnl"] = pd.to_numeric(out["v6_net_pnl_rs"], errors="coerce").fillna(0.0)

    sig = pd.to_datetime(out.get("signal_time_ist"), errors="coerce")
    out["signal_minute"] = sig.dt.hour * 60 + sig.dt.minute
    out["month"] = out["date"].dt.to_period("M").astype(str)

    for col in [
        "signal_open",
        "signal_high",
        "signal_low",
        "signal_close",
        "market_ret_pct",
        "rs_pct",
        "vol_ratio",
        "atr_pct",
        "body_pct",
        "close_loc",
        "vwap_dist_atr",
        "quality_score",
        "ranker_score",
        "score",
        "v7_signal_notional_rs",
        "signal_range_pct",
        "upper_wick_pct",
        "lower_wick_pct",
        "signal_volume",
    ]:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")

    open_px = out["signal_open"]
    high = out["signal_high"]
    low = out["signal_low"]
    close = out["signal_close"]
    body_top = pd.concat([open_px, close], axis=1).max(axis=1)
    body_bottom = pd.concat([open_px, close], axis=1).min(axis=1)
    candle_range = (high - low).replace(0, np.nan)

    out["signal_body_ret_pct"] = _safe_div(close - open_px, open_px) * 100.0
    out["signal_abs_body_ret_pct"] = out["signal_body_ret_pct"].abs()
    out["_calc_upper_wick_pct"] = _safe_div(high - body_top, close) * 100.0
    out["_calc_lower_wick_pct"] = _safe_div(body_bottom - low, close) * 100.0
    if "upper_wick_pct" not in out.columns:
        out["upper_wick_pct"] = out["_calc_upper_wick_pct"]
    if "lower_wick_pct" not in out.columns:
        out["lower_wick_pct"] = out["_calc_lower_wick_pct"]
    out["wick_skew_pct"] = out["upper_wick_pct"] - out["lower_wick_pct"]
    out["body_to_range"] = _safe_div(body_top - body_bottom, candle_range)
    out["close_in_range"] = _safe_div(close - low, candle_range)

    out["market_abs_ret_pct"] = pd.to_numeric(out.get("market_ret_pct"), errors="coerce").abs()
    out["rs_abs_pct"] = pd.to_numeric(out.get("rs_pct"), errors="coerce").abs()
    out["rs_minus_market_pct"] = (
        pd.to_numeric(out.get("rs_pct"), errors="coerce")
        - pd.to_numeric(out.get("market_ret_pct"), errors="coerce")
    )
    out["vwap_dist_abs_atr"] = pd.to_numeric(out.get("vwap_dist_atr"), errors="coerce").abs()
    out["notional_gap_rs"] = 100000.0 - pd.to_numeric(out.get("v7_signal_notional_rs"), errors="coerce")

    side = out["side"].astype(str).str.upper()
    out["market_with_side"] = np.where(side.eq("LONG"), out["market_ret_pct"], -out["market_ret_pct"])
    out["rs_with_side"] = np.where(side.eq("LONG"), out["rs_pct"], -out["rs_pct"])
    out["vwap_with_side"] = np.where(side.eq("LONG"), out["vwap_dist_atr"], -out["vwap_dist_atr"])
    out["candle_color"] = np.where(close > open_px, "GREEN", np.where(close < open_px, "RED", "DOJI"))

    mins = pd.to_numeric(out["signal_minute"], errors="coerce")
    out["time_bucket_45"] = np.select(
        [mins <= 600, mins <= 645, mins <= 690, mins <= 735, mins <= 780, mins <= 825, mins <= 870],
        ["0915_1000", "1001_1045", "1046_1130", "1131_1215", "1216_1300", "1301_1345", "1346_1430"],
        default="1431_1500",
    )
    return out


def _split_masks(df: pd.DataFrame) -> dict[str, pd.Series]:
    return {
        "train": df["date"] <= TRAIN_END,
        "valid": (df["date"] >= VALID_START) & (df["date"] <= VALID_END),
        "holdout": (df["date"] >= HOLDOUT_START) & (df["date"] <= HOLDOUT_END),
        "full": pd.Series(True, index=df.index),
    }


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


def _evaluate(df: pd.DataFrame, label: str, rule: str, mask: pd.Series) -> dict:
    frame = df.loc[mask].copy()
    row = {"label": label, "rule": rule, "mask": mask}
    for split, split_mask in _split_masks(frame).items():
        stats = _metrics(frame.loc[split_mask])
        for key, value in stats.items():
            row[f"{split}_{key}"] = value

    month_pnls = [float(g["pnl"].sum()) for _, g in frame.groupby("month")]
    row["months"] = int(len(month_pnls))
    row["positive_months"] = int(sum(x > 0 for x in month_pnls))
    row["worst_month_pnl"] = float(min(month_pnls) if month_pnls else 0.0)
    row["dev_pass"] = bool(
        row["train_trades"] >= 15
        and row["valid_trades"] >= 5
        and _pf_score(row["train_pf"]) >= 1.50
        and _pf_score(row["valid_pf"]) >= 1.00
        and row["train_pnl"] > 0
        and row["valid_pnl"] > 0
    )
    row["quality_watch"] = bool(
        row["dev_pass"]
        and row["holdout_trades"] >= 3
        and row["holdout_pnl"] > 0
        and _pf_score(row["full_pf"]) >= 1.50
    )
    row["strong_watch"] = bool(
        row["quality_watch"]
        and _pf_score(row["holdout_pf"]) >= 1.20
        and row["full_trades"] >= 25
    )
    row["score"] = (
        int(row["dev_pass"]) * 1_000_000
        + int(row["strong_watch"]) * 500_000
        + min(_pf_score(row["valid_pf"]), 3.0) * 50_000
        + row["full_trades"] * 300
        + row["valid_trades"] * 500
        + max(float(row["train_pnl"] + row["valid_pnl"]), 0.0) / 10.0
    )
    return row


def _condition_pool(df: pd.DataFrame, base_mask: pd.Series) -> list[tuple[str, pd.Series]]:
    train_mask = base_mask & _split_masks(df)["train"]
    out: list[tuple[str, pd.Series]] = []
    for col in NUMERIC_FEATURES:
        if col not in df.columns:
            continue
        values = pd.to_numeric(df.loc[train_mask, col], errors="coerce").dropna()
        if values.nunique() < 10:
            continue
        series = pd.to_numeric(df[col], errors="coerce")
        for quantile in QUANTILES:
            threshold = float(values.quantile(quantile))
            if not np.isfinite(threshold):
                continue
            out.append((f"{col}>={threshold:.8g}", base_mask & (series >= threshold)))
            out.append((f"{col}<={threshold:.8g}", base_mask & (series <= threshold)))

    for col in CATEGORICAL_FEATURES:
        if col not in df.columns:
            continue
        counts = df.loc[train_mask, col].astype(str).value_counts()
        for value, count in counts.items():
            if count >= max(8, int(train_mask.sum() * 0.04)):
                out.append((f"{col}=={value}", base_mask & df[col].astype(str).eq(str(value))))
    return out


def _search_setup(df: pd.DataFrame, label: str, base_mask: pd.Series) -> list[dict]:
    singles = [_evaluate(df, label, rule, mask) for rule, mask in _condition_pool(df, base_mask)]
    pool = sorted(
        [x for x in singles if x["train_trades"] >= 8 and _pf_score(x["train_pf"]) >= 1.0],
        key=lambda x: (_pf_score(x["train_pf"]), int(x["train_trades"]), float(x["train_pnl"])),
        reverse=True,
    )[:45]
    candidates = [_evaluate(df, label, "ALL", base_mask)] + singles
    seen = {x["rule"] for x in candidates}
    for left, right in itertools.combinations(pool[:32], 2):
        for op, mask in [
            (" AND ", left["mask"] & right["mask"]),
            (" OR ", left["mask"] | right["mask"]),
        ]:
            if int((mask & _split_masks(df)["train"]).sum()) < 8:
                continue
            rule = f"({left['rule']}){op}({right['rule']})"
            if rule in seen:
                continue
            seen.add(rule)
            candidates.append(_evaluate(df, label, rule, mask))
    return candidates


def _strip_mask(row: dict) -> dict:
    return {key: value for key, value in row.items() if key != "mask"}


def _scenario_rows(label: str, frame: pd.DataFrame) -> list[dict]:
    rows = []
    for split, split_mask in _split_masks(frame).items():
        row = {"label": label, "split": split}
        row.update(_metrics(frame.loc[split_mask]))
        rows.append(row)
    return rows


def main() -> None:
    df = _add_features(pd.read_csv(TRADES_CSV))
    accepted, _, _ = v11._apply_selected_strategy_profile(df, PROFILE)
    accepted_ids = set(accepted.get("signal_id", pd.Series(dtype=str)).astype(str))
    current_mask = df.get("signal_id", pd.Series("", index=df.index)).astype(str).isin(accepted_ids)
    residual = df.loc[~current_mask].copy().reset_index(drop=True)

    all_rows: list[dict] = []
    best_rows: list[dict] = []
    for setup, count in residual["setup"].astype(str).value_counts().items():
        if count < 10:
            continue
        label = f"setup:{setup}"
        base_mask = residual["setup"].astype(str).eq(setup)
        candidates = _search_setup(residual, label, base_mask)
        all_rows.extend(_strip_mask(x) for x in candidates)
        dev = [x for x in candidates if x["dev_pass"]]
        if dev:
            # Selected without using holdout in the sort key.
            best = sorted(
                dev,
                key=lambda x: (
                    _pf_score(x["valid_pf"]),
                    int(x["valid_trades"]),
                    int(x["full_trades"]),
                    _pf_score(x["train_pf"]),
                    float(x["train_pnl"] + x["valid_pnl"]),
                ),
                reverse=True,
            )[0]
            best_rows.append(_strip_mask(best))

    candidates_df = pd.DataFrame(all_rows)
    if not candidates_df.empty:
        candidates_df = candidates_df.sort_values(
            ["dev_pass", "strong_watch", "quality_watch", "valid_pf", "full_trades", "train_pf"],
            ascending=False,
        )
        candidates_df.to_csv(OUT_DIR / "v11_residual_deeper_overlay_candidates.csv", index=False)

    best_df = pd.DataFrame(best_rows)
    if not best_df.empty:
        best_df = best_df.sort_values(["strong_watch", "quality_watch", "valid_pf", "full_trades"], ascending=False)
        best_df.to_csv(OUT_DIR / "v11_residual_deeper_overlay_best_by_setup.csv", index=False)

    setup = df["setup"].astype(str)
    residual_full = ~current_mask
    d_body = (
        residual_full
        & setup.eq("D_EMA20_REJECTION")
        & df["time_bucket_45"].astype(str).eq("1301_1345")
        & (pd.to_numeric(df["body_pct"], errors="coerce") >= 0.92592279)
    )
    d_wick = (
        residual_full
        & setup.eq("D_EMA20_REJECTION")
        & df["time_bucket_45"].astype(str).eq("1301_1345")
        & (pd.to_numeric(df["wick_skew_pct"], errors="coerce") <= -0.064893645)
    )
    sbb_morning = (
        residual_full
        & setup.eq("S_BB_SQUEEZE_SHORT")
        & (pd.to_numeric(df["signal_minute"], errors="coerce") <= 704.5)
    )
    sbb_rs_vwap = (
        residual_full
        & setup.eq("S_BB_SQUEEZE_SHORT")
        & (pd.to_numeric(df["rs_pct"], errors="coerce") <= -1.2449309)
        & (pd.to_numeric(df["vwap_dist_atr"], errors="coerce") >= 27.115924)
    )

    scenarios = {
        "current_profile": current_mask,
        "addon_D_late_body_only": d_body,
        "addon_D_late_wick_only": d_wick,
        "addon_SBB_morning_only": sbb_morning,
        "addon_SBB_rs_vwap_only": sbb_rs_vwap,
        "addon_D_body_D_wick_only": d_body | d_wick,
        "addon_D_body_D_wick_SBB_morning_only": d_body | d_wick | sbb_morning,
        "current_plus_D_body_D_wick": current_mask | d_body | d_wick,
        "current_plus_D_body_D_wick_SBB_morning": current_mask | d_body | d_wick | sbb_morning,
        "current_plus_D_body_D_wick_SBB_rs_vwap": current_mask | d_body | d_wick | sbb_rs_vwap,
    }
    scenario_rows: list[dict] = []
    for label, mask in scenarios.items():
        scenario_rows.extend(_scenario_rows(label, df.loc[mask].copy()))
    pd.DataFrame(scenario_rows).to_csv(OUT_DIR / "v11_residual_deeper_overlay_scenarios.csv", index=False)

    overlay_mask = d_body | d_wick | sbb_morning
    by_setup_rows: list[dict] = []
    for setup_name, group in df.loc[overlay_mask].groupby("setup"):
        for split, split_mask in _split_masks(group).items():
            row = {"setup": setup_name, "split": split}
            row.update(_metrics(group.loc[split_mask]))
            by_setup_rows.append(row)
    pd.DataFrame(by_setup_rows).to_csv(OUT_DIR / "v11_residual_deeper_overlay_by_setup.csv", index=False)

    print(f"[done] residual={len(residual):,} current_profile={int(current_mask.sum()):,}")
    print(f"[done] wrote {OUT_DIR / 'v11_residual_deeper_overlay_candidates.csv'}")
    print(f"[done] wrote {OUT_DIR / 'v11_residual_deeper_overlay_best_by_setup.csv'}")
    print(f"[done] wrote {OUT_DIR / 'v11_residual_deeper_overlay_scenarios.csv'}")
    print(f"[done] wrote {OUT_DIR / 'v11_residual_deeper_overlay_by_setup.csv'}")


if __name__ == "__main__":
    main()
