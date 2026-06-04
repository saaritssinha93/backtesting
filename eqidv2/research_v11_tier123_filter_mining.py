from __future__ import annotations

import itertools
import math
from pathlib import Path

import numpy as np
import pandas as pd

import avwap_5min_ID_v11_backtesting as v11
import research_v11_tier123_new_setups as tier123


OUT_DIR = Path(r"C:\TradingData\eqidv2\outputs_ID_v11_tier123_new_setup_probe")
TRADES_CSV = OUT_DIR / "tier123_standalone_trades.csv"
BASE_TRADES_CSV = Path(r"C:\TradingData\eqidv2\outputs_ID_v11_all_setups_with_ab_probe_from_v10_raw_q250\trades.csv")
BASE_PROFILE = "production_core_ab_max_pnl_low_valid_residual_overlay"

MIN_TRAIN_TRADES = 20
MIN_VALID_TRADES = 5
MAX_SINGLE_CONDITIONS_FOR_COMBOS = 45

NUMERIC_FEATURES = [
    "quality_score",
    "rs_pct",
    "market_ret_pct",
    "vol_ratio",
    "atr_pct",
    "body_pct",
    "close_loc",
    "vwap_dist_atr",
    "abs_vwap_dist_atr",
    "signal_volume",
]

CATEGORICAL_FEATURES = [
    "regime",
    "reason",
    "time_bucket_30",
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


def _bucket(minute: int, width: int) -> str:
    start = (minute // width) * width
    end = start + width - 1
    return f"{start // 60:02d}{start % 60:02d}_{end // 60:02d}{end % 60:02d}"


def _prepare(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["date"] = pd.to_datetime(out["trade_date"], errors="coerce")
    out["pnl"] = pd.to_numeric(out["v6_net_pnl_rs"], errors="coerce").fillna(0.0)
    ts = pd.to_datetime(out["signal_time_ist"], errors="coerce")
    out["signal_minute"] = ts.dt.hour * 60 + ts.dt.minute
    out["time_bucket_30"] = out["signal_minute"].fillna(-1).astype(int).map(lambda x: _bucket(x, 30) if x >= 0 else "")
    out["time_bucket_45"] = out["signal_minute"].fillna(-1).astype(int).map(lambda x: _bucket(x, 45) if x >= 0 else "")
    out["abs_vwap_dist_atr"] = pd.to_numeric(out.get("vwap_dist_atr"), errors="coerce").abs()
    for col in NUMERIC_FEATURES:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")
    for col in CATEGORICAL_FEATURES:
        if col in out.columns:
            out[col] = out[col].astype(str).fillna("")
    return out


def _split_masks(df: pd.DataFrame) -> dict[str, pd.Series]:
    return {
        "train": df["date"] <= tier123.TRAIN_END,
        "valid": (df["date"] >= tier123.VALID_START) & (df["date"] <= tier123.VALID_END),
        "holdout": (df["date"] >= tier123.HOLDOUT_START) & (df["date"] <= tier123.HOLDOUT_END),
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


def _stats_by_split(frame: pd.DataFrame) -> dict[str, dict]:
    return {split: _metrics(frame.loc[mask]) for split, mask in _split_masks(frame).items()}


def _make_conditions(df: pd.DataFrame, train_mask: pd.Series) -> list[dict]:
    conditions: list[dict] = []
    for feature in NUMERIC_FEATURES:
        if feature not in df.columns:
            continue
        train_values = pd.to_numeric(df.loc[train_mask, feature], errors="coerce").replace([np.inf, -np.inf], np.nan).dropna()
        if train_values.nunique() < 4:
            continue
        for q in [0.10, 0.20, 0.30, 0.40, 0.50, 0.60, 0.70, 0.80, 0.90]:
            threshold = float(train_values.quantile(q))
            if not np.isfinite(threshold):
                continue
            for op in [">=", "<="]:
                series = pd.to_numeric(df[feature], errors="coerce")
                mask = series >= threshold if op == ">=" else series <= threshold
                conditions.append(
                    {
                        "text": f"{feature} {op} {threshold:.6g}",
                        "feature": feature,
                        "mask": mask.fillna(False),
                    }
                )

    for feature in CATEGORICAL_FEATURES:
        if feature not in df.columns:
            continue
        counts = df.loc[train_mask, feature].astype(str).value_counts()
        for value, count in counts.items():
            if count < MIN_TRAIN_TRADES or value == "":
                continue
            conditions.append(
                {
                    "text": f"{feature} == {value}",
                    "feature": feature,
                    "mask": df[feature].astype(str).eq(str(value)),
                }
            )
    return conditions


def _decision(stats: dict[str, dict]) -> str:
    train = stats["train"]
    valid = stats["valid"]
    holdout = stats["holdout"]
    full = stats["full"]
    train_pf_pass = train["trades"] >= MIN_TRAIN_TRADES and _pf_score(train["pf"]) > 1.5
    valid_ok = valid["trades"] >= MIN_VALID_TRADES and valid["pnl"] > 0 and _pf_score(valid["pf"]) >= 1.0
    holdout_ok = holdout["trades"] >= 5 and holdout["pnl"] > 0 and _pf_score(holdout["pf"]) >= 1.0
    if train_pf_pass and valid_ok and holdout_ok and _pf_score(full["pf"]) >= 1.5:
        return "promising_probation"
    if train_pf_pass and valid_ok:
        return "train_valid_pass_holdout_or_full_weak"
    if train_pf_pass:
        return "train_only_pass_reject_for_now"
    return "reject_train_pf_or_count"


def _row(rule_id: str, setup: str, conditions: list[dict], frame: pd.DataFrame) -> dict:
    stats = _stats_by_split(frame)
    row = {
        "rule_id": rule_id,
        "setup": setup,
        "tier": tier123.SETUP_TIERS.get(setup, ""),
        "n_conditions": len(conditions),
        "conditions": " AND ".join(c["text"] for c in conditions) if conditions else "ALL",
    }
    for split, metric in stats.items():
        for key, value in metric.items():
            row[f"{split}_{key}"] = value
    row["decision"] = _decision(stats)
    row["score"] = (
        1000.0 * (row["decision"] == "promising_probation")
        + 400.0 * (row["decision"] == "train_valid_pass_holdout_or_full_weak")
        + 100.0 * (row["decision"] == "train_only_pass_reject_for_now")
        + min(_pf_score(row["train_pf"]), 5.0) * 10.0
        + min(_pf_score(row["valid_pf"]), 5.0) * 6.0
        + min(_pf_score(row["holdout_pf"]), 5.0) * 4.0
        + min(row["full_trades"], 300) / 30.0
    )
    return row


def _mine_setup(setup: str, df: pd.DataFrame) -> tuple[list[dict], dict[str, pd.Series]]:
    train_mask = _split_masks(df)["train"]
    conditions = _make_conditions(df, train_mask)
    rows: list[dict] = []
    masks_by_rule: dict[str, pd.Series] = {}

    evaluated: set[tuple[str, ...]] = set()

    def evaluate(combo: tuple[int, ...]) -> None:
        key = tuple(sorted(conditions[i]["text"] for i in combo))
        if key in evaluated:
            return
        evaluated.add(key)
        mask = pd.Series(True, index=df.index)
        combo_conditions = [conditions[i] for i in combo]
        for cond in combo_conditions:
            mask &= cond["mask"]
        frame = df.loc[mask].copy()
        if len(frame) == 0:
            return
        stats = _stats_by_split(frame)
        if stats["train"]["trades"] < 15:
            return
        if _pf_score(stats["train"]["pf"]) < 1.2:
            return
        rule_id = f"{setup}__r{len(rows) + 1:04d}"
        row = _row(rule_id, setup, combo_conditions, frame)
        rows.append(row)
        masks_by_rule[rule_id] = mask

    for i in range(len(conditions)):
        evaluate((i,))

    single_df = pd.DataFrame(rows)
    if not single_df.empty:
        top_texts = set(
            single_df.sort_values(["score", "train_trades"], ascending=[False, False])
            .head(MAX_SINGLE_CONDITIONS_FOR_COMBOS)["conditions"]
            .astype(str)
        )
        top_indices = [i for i, cond in enumerate(conditions) if cond["text"] in top_texts]
    else:
        top_indices = list(range(min(len(conditions), MAX_SINGLE_CONDITIONS_FOR_COMBOS)))

    for combo in itertools.combinations(top_indices, 2):
        evaluate(combo)

    for combo in itertools.combinations(top_indices[:24], 3):
        evaluate(combo)

    return rows, masks_by_rule


def _scenario_rows(label: str, frame: pd.DataFrame) -> list[dict]:
    rows = []
    for split, stats in _stats_by_split(frame).items():
        row = {"label": label, "split": split}
        row.update(stats)
        rows.append(row)
    return rows


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    if not TRADES_CSV.exists():
        raise SystemExit(f"missing {TRADES_CSV}")

    trades = _prepare(pd.read_csv(TRADES_CSV))
    all_rows: list[dict] = []
    masks_by_rule: dict[str, tuple[str, pd.Series]] = {}

    for setup, group in trades.groupby("setup", sort=True):
        print(f"[mine] {setup} trades={len(group):,}", flush=True)
        group = group.copy()
        rows, masks = _mine_setup(setup, group)
        all_rows.extend(rows)
        for rule_id, mask in masks.items():
            masks_by_rule[rule_id] = (setup, mask)

    candidates = pd.DataFrame(all_rows)
    if candidates.empty:
        candidates.to_csv(OUT_DIR / "tier123_filter_mining_candidates.csv", index=False)
        raise SystemExit("[mine] no filter candidates generated")

    candidates = candidates.sort_values(["score", "train_trades"], ascending=[False, False]).reset_index(drop=True)
    candidates.to_csv(OUT_DIR / "tier123_filter_mining_candidates.csv", index=False)

    best_by_setup = (
        candidates.sort_values(["setup", "score", "train_trades"], ascending=[True, False, False])
        .groupby("setup", sort=True)
        .head(10)
        .reset_index(drop=True)
    )
    best_by_setup.to_csv(OUT_DIR / "tier123_filter_mining_best_by_setup.csv", index=False)

    promotion = candidates.loc[candidates["decision"].eq("promising_probation")].copy()
    watchlist = candidates.loc[candidates["decision"].eq("train_valid_pass_holdout_or_full_weak")].copy()
    selected_rules = (
        promotion.sort_values(["score", "full_pnl"], ascending=[False, False])
        .groupby("setup", sort=False)
        .head(1)
        .head(10)
        .reset_index(drop=True)
    )
    selected_rules.to_csv(OUT_DIR / "tier123_filter_mining_selected_promising.csv", index=False)
    watchlist.sort_values(["score", "full_pnl"], ascending=[False, False]).head(20).to_csv(
        OUT_DIR / "tier123_filter_mining_watchlist.csv",
        index=False,
    )

    scenario_rows = []
    if BASE_TRADES_CSV.exists():
        base_pool = pd.read_csv(BASE_TRADES_CSV)
        base_selected, _, _ = v11._apply_selected_strategy_profile(base_pool, BASE_PROFILE)
        base_selected = _prepare(base_selected)
        scenario_rows.extend(_scenario_rows("base_current_residual_overlay_profile", base_selected))

        current_keys = set(base_selected["ticker"].astype(str) + "|" + base_selected["trade_date"].astype(str))
        selected_frames = []
        used_new_keys: set[str] = set()
        for _, rule in selected_rules.iterrows():
            setup, mask = masks_by_rule[str(rule["rule_id"])]
            frame = trades.loc[trades["setup"].eq(setup)].loc[mask].copy()
            keys = frame["ticker"].astype(str) + "|" + frame["trade_date"].astype(str)
            frame = frame.loc[~keys.isin(current_keys | used_new_keys)].copy()
            if frame.empty:
                continue
            used_new_keys.update(frame["ticker"].astype(str) + "|" + frame["trade_date"].astype(str))
            selected_frames.append(frame)

        selected_new = pd.concat(selected_frames, ignore_index=True, sort=False) if selected_frames else trades.iloc[0:0].copy()
        scenario_rows.extend(_scenario_rows("tier123_filtered_promising_non_overlap", selected_new))
        scenario_rows.extend(
            _scenario_rows("current_plus_tier123_filtered_promising", pd.concat([base_selected, selected_new], ignore_index=True))
        )

    pd.DataFrame(scenario_rows).to_csv(OUT_DIR / "tier123_filter_mining_scenarios.csv", index=False)
    print(f"[done] candidates={len(candidates):,} promising={len(promotion):,} watchlist={len(watchlist):,}", flush=True)


if __name__ == "__main__":
    main()
