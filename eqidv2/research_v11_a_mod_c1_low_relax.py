from __future__ import annotations

import itertools
import math
from pathlib import Path

import numpy as np
import pandas as pd

import avwap_5min_ID_v11_backtesting as v11


OUT_DIR = Path(r"C:\TradingData\eqidv2\outputs_ID_v11_all_setups_with_ab_probe_from_v10_raw_q250")
RAW_CANDIDATES_CSV = OUT_DIR / "all_setups_raw_candidates_from_v10_raw.csv"
SETUP = "A_MOD_BREAK_C1_LOW"

TRAIN_END = pd.Timestamp("2026-01-31")
VALID_START = pd.Timestamp("2026-02-01")
VALID_END = pd.Timestamp("2026-03-31")
HOLDOUT_START = pd.Timestamp("2026-04-01")
HOLDOUT_END = pd.Timestamp("2026-05-29")

GATE_CONFIGS = [
    (200, 1),
    (200, 5),
    (225, 1),
    (225, 5),
    (250, 1),
    (275, 1),
]
QUANTILES = [0.05, 0.10, 0.15, 0.20, 0.25, 0.30, 0.35, 0.40, 0.45, 0.50,
             0.55, 0.60, 0.65, 0.70, 0.75, 0.80, 0.85, 0.90, 0.95]

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
        "full": pd.Series(True, index=df.index),
    }


def _stats_by_split(frame: pd.DataFrame) -> dict[str, dict]:
    masks = _split_masks(frame)
    return {name: _metrics(frame[mask]) for name, mask in masks.items()}


def _load_setup_raw() -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for chunk in pd.read_csv(RAW_CANDIDATES_CSV, chunksize=250_000):
        hit = chunk.loc[chunk["setup"].astype(str).eq(SETUP)].copy()
        if not hit.empty:
            frames.append(hit)
    if not frames:
        raise SystemExit(f"no raw candidates found for {SETUP}")
    raw = pd.concat(frames, ignore_index=True)
    print(f"[load] {SETUP} raw candidates={len(raw):,}", flush=True)
    return raw


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
    out["signal_hour"] = sig.dt.hour
    for col in ["signal_open", "signal_high", "signal_low", "signal_close"]:
        out[col] = pd.to_numeric(out.get(col), errors="coerce")
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
    out["market_abs_ret_pct"] = pd.to_numeric(out.get("market_ret_pct"), errors="coerce").abs()
    out["rs_abs_pct"] = pd.to_numeric(out.get("rs_pct"), errors="coerce").abs()
    out["rs_minus_market_pct"] = (
        pd.to_numeric(out.get("rs_pct"), errors="coerce")
        - pd.to_numeric(out.get("market_ret_pct"), errors="coerce")
    )
    out["vwap_dist_abs_atr"] = pd.to_numeric(out.get("vwap_dist_atr"), errors="coerce").abs()
    out["candle_color"] = np.where(close > open_px, "GREEN", np.where(close < open_px, "RED", "DOJI"))
    mins = pd.to_numeric(out["signal_minute"], errors="coerce")
    out["time_bucket"] = np.select(
        [mins <= 630, mins <= 720, mins <= 810],
        ["OPEN_0915_1030", "MID_1031_1200", "AFTERNOON_1201_1330"],
        default="LATE_1331_1500",
    )
    for col in NUMERIC_FEATURES:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")
    return out


def _manual_ab_gate(raw: pd.DataFrame, min_quality: float, slot_cap: int) -> pd.DataFrame:
    work = v11._candidate_sort_frame(raw)
    work = v11._ensure_candidate_id(work)
    quality_source = work["quality_score"] if "quality_score" in work.columns else pd.Series(0.0, index=work.index)
    ranker_source = work["ranker_score"] if "ranker_score" in work.columns else pd.Series(0.0, index=work.index)
    quality = pd.to_numeric(quality_source, errors="coerce").fillna(0.0)
    work = work.loc[quality >= float(min_quality)].copy()
    if work.empty:
        return work
    quality_source = work["quality_score"] if "quality_score" in work.columns else pd.Series(0.0, index=work.index)
    work["_ab_quality_num"] = pd.to_numeric(quality_source, errors="coerce").fillna(0.0)
    work["_ab_ranker_num"] = pd.to_numeric(ranker_source.reindex(work.index), errors="coerce").fillna(0.0)
    rows = []
    for _, slot_df in work.groupby(["_v11_day", "_v11_slot_key"], sort=True, dropna=False):
        selected = (
            slot_df.sort_values(
                ["side", "_ab_quality_num", "_ab_ranker_num", "ticker", "setup"],
                ascending=[True, False, False, True, True],
            )
            .head(int(slot_cap))
        )
        rows.append(selected)
    if not rows:
        return work.iloc[0:0].copy()
    out = pd.concat(rows, ignore_index=True)
    out = out.sort_values(["_v11_day", "_v11_slot_ts", "_ab_quality_num", "_ab_ranker_num"], ascending=[True, True, False, False])
    out["_v11_live_order"] = np.arange(len(out))
    out = out.drop(columns=["_ab_quality_num", "_ab_ranker_num"], errors="ignore")
    out["v8_live_gate_status"] = "AB_RESEARCH_RELAXED_PASSED"
    out["ab_gate_rule"] = f"manual_ab_research|min_quality>={min_quality}|slot_cap={slot_cap}"
    live_like, _ = v11._live_like_daily_dedupe(out)
    return live_like


def _resolve_config(raw: pd.DataFrame, min_quality: float, slot_cap: int) -> pd.DataFrame:
    candidates = _manual_ab_gate(raw, min_quality, slot_cap)
    if candidates.empty:
        return pd.DataFrame()
    signals, raw_entries, rejects = v11._build_v7_entry_engine_signals(candidates)
    if signals.empty:
        return pd.DataFrame()
    trades = v11._resolve_v7_entry_engine_signals(
        signals,
        label=f"a_mod_c1_low_q{min_quality}_cap{slot_cap}",
        entry_fill_model="ltp_on_signal_1m_open",
        selected_strategy_profile="none",
    )
    trades = _add_features(trades)
    trades["research_min_quality"] = float(min_quality)
    trades["research_slot_cap"] = int(slot_cap)
    trades["research_live_like_candidates"] = int(len(candidates))
    trades["research_entry_engine_signals"] = int(len(signals))
    return trades


def _condition_pool(df: pd.DataFrame) -> list[tuple[str, pd.Series]]:
    setup_mask = pd.Series(True, index=df.index)
    train = df[_split_masks(df)["train"]]
    out: list[tuple[str, pd.Series]] = [("ALL", setup_mask)]
    for col in NUMERIC_FEATURES:
        if col not in df.columns:
            continue
        values = pd.to_numeric(train[col], errors="coerce").dropna()
        if values.nunique() < 6:
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
            if count >= max(2, int(len(train) * 0.05)):
                out.append((f"{col}=={value}", setup_mask & df[col].astype(str).eq(value)))
    return out


def _evaluate_rule(df: pd.DataFrame, rule: str, mask: pd.Series) -> dict:
    frame = df[mask].copy()
    stats = _stats_by_split(frame)
    row = {
        "rule": rule,
        "full_n": stats["full"]["trades"],
        "full_pf": stats["full"]["pf"],
        "full_pnl": stats["full"]["pnl"],
        "train_n": stats["train"]["trades"],
        "train_pf": stats["train"]["pf"],
        "train_pnl": stats["train"]["pnl"],
        "valid_n": stats["valid"]["trades"],
        "valid_pf": stats["valid"]["pf"],
        "valid_pnl": stats["valid"]["pnl"],
        "holdout_n": stats["holdout"]["trades"],
        "holdout_pf": stats["holdout"]["pf"],
        "holdout_pnl": stats["holdout"]["pnl"],
        "mask": mask,
    }
    row["strict_ok"] = bool(
        row["train_n"] >= 3
        and row["holdout_n"] >= 2
        and _pf_score(row["train_pf"]) >= 1.5
        and _pf_score(row["holdout_pf"]) >= 1.5
        and _pf_score(row["full_pf"]) >= 1.5
        and row["full_pnl"] > 0
    )
    row["relaxed_ok"] = bool(
        row["train_n"] >= 5
        and row["holdout_n"] >= 2
        and _pf_score(row["train_pf"]) >= 1.5
        and row["holdout_pnl"] > 0
        and _pf_score(row["full_pf"]) >= 1.5
    )
    return row


def _search_rules(df: pd.DataFrame) -> list[dict]:
    singles = []
    for rule, mask in _condition_pool(df):
        singles.append(_evaluate_rule(df, rule, mask))
    pool = sorted(
        [x for x in singles if x["train_n"] >= 2 and _pf_score(x["train_pf"]) >= 1.0],
        key=lambda x: (_pf_score(x["train_pf"]), x["train_n"]),
        reverse=True,
    )[:35]
    candidates = list(singles)
    seen = {x["rule"] for x in candidates}
    for a, b in itertools.combinations(pool[:25], 2):
        if a["rule"] == b["rule"]:
            continue
        for op, mask in [(" AND ", a["mask"] & b["mask"]), (" OR ", a["mask"] | b["mask"])]:
            rule = f"({a['rule']}){op}({b['rule']})"
            if rule in seen:
                continue
            seen.add(rule)
            candidates.append(_evaluate_rule(df, rule, mask))
    return candidates


def _strip_mask(row: dict) -> dict:
    return {k: v for k, v in row.items() if k != "mask"}


def main() -> None:
    raw = _load_setup_raw()
    config_rows = []
    candidate_rows = []
    all_trades = []
    for min_quality, slot_cap in GATE_CONFIGS:
        print(f"[config] min_quality={min_quality} slot_cap={slot_cap}", flush=True)
        trades = _resolve_config(raw, min_quality, slot_cap)
        if trades.empty:
            continue
        all_trades.append(trades)
        stats = _stats_by_split(trades)
        config_row = {
            "setup": SETUP,
            "min_quality": min_quality,
            "slot_cap": slot_cap,
            "live_like_candidates": int(trades["research_live_like_candidates"].iloc[0]),
            "entry_engine_signals": int(trades["research_entry_engine_signals"].iloc[0]),
        }
        for split, metric in stats.items():
            for key, value in metric.items():
                config_row[f"{split}_{key}"] = value
        config_rows.append(config_row)
        for item in _search_rules(trades):
            row = _strip_mask(item)
            row.update({
                "setup": SETUP,
                "min_quality": min_quality,
                "slot_cap": slot_cap,
                "live_like_candidates": int(trades["research_live_like_candidates"].iloc[0]),
                "entry_engine_signals": int(trades["research_entry_engine_signals"].iloc[0]),
            })
            candidate_rows.append(row)

    pd.DataFrame(config_rows).to_csv(OUT_DIR / "v11_a_mod_c1_low_gate_configs.csv", index=False)
    candidates = pd.DataFrame(candidate_rows)
    if not candidates.empty:
        candidates["score"] = (
            candidates["strict_ok"].astype(int) * 1_000_000
            + candidates["relaxed_ok"].astype(int) * 100_000
            + candidates["full_n"] * 100
            + candidates["full_pf"].map(_pf_score) * 10
            + candidates["holdout_n"] * 5
        )
        candidates = candidates.sort_values(
            ["strict_ok", "relaxed_ok", "full_n", "holdout_n", "full_pf", "train_pf"],
            ascending=False,
        )
        candidates.to_csv(OUT_DIR / "v11_a_mod_c1_low_rule_candidates.csv", index=False)
        best_strict = candidates.loc[candidates["strict_ok"].astype(bool)].head(20)
        best_relaxed = candidates.loc[candidates["relaxed_ok"].astype(bool)].head(20)
        best_train = candidates.loc[candidates["train_pf"].map(_pf_score).ge(1.5)].head(20)
        best_strict.to_csv(OUT_DIR / "v11_a_mod_c1_low_best_strict.csv", index=False)
        best_relaxed.to_csv(OUT_DIR / "v11_a_mod_c1_low_best_relaxed.csv", index=False)
        best_train.to_csv(OUT_DIR / "v11_a_mod_c1_low_best_train_pf15.csv", index=False)
        print("[best strict]")
        print(best_strict[["min_quality","slot_cap","rule","full_n","full_pf","full_pnl","train_n","train_pf","valid_n","valid_pf","holdout_n","holdout_pf","holdout_pnl"]].to_string(index=False, max_colwidth=120))
        print("[best relaxed]")
        print(best_relaxed[["min_quality","slot_cap","rule","full_n","full_pf","full_pnl","train_n","train_pf","valid_n","valid_pf","holdout_n","holdout_pf","holdout_pnl"]].to_string(index=False, max_colwidth=120))
        print("[best train pf>=1.5]")
        print(best_train[["min_quality","slot_cap","rule","full_n","full_pf","full_pnl","train_n","train_pf","valid_n","valid_pf","holdout_n","holdout_pf","holdout_pnl"]].to_string(index=False, max_colwidth=120))
    if all_trades:
        trades_all = pd.concat(all_trades, ignore_index=True)
        trades_all.to_csv(OUT_DIR / "v11_a_mod_c1_low_all_resolved_config_trades.csv", index=False)
    print("[done]", flush=True)


if __name__ == "__main__":
    main()
