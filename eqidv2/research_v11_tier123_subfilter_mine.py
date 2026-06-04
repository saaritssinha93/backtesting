from __future__ import annotations

import math
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd


OUT_DIR = Path(r"C:\TradingData\eqidv2\outputs_ID_v11_tier123_new_setup_probe")
TRADES_CSV = OUT_DIR / "tier123_standalone_trades.csv"
BASE_TRADES_CSV = Path(r"C:\TradingData\eqidv2\outputs_ID_v11_all_setups_with_ab_probe_from_v10_raw_q250\trades.csv")

TRAIN_END = pd.Timestamp("2026-01-31")
VALID_START = pd.Timestamp("2026-02-01")
VALID_END = pd.Timestamp("2026-03-31")
HOLDOUT_START = pd.Timestamp("2026-04-01")
HOLDOUT_END = pd.Timestamp("2026-05-29")

MIN_TRAIN_TRADES = 25
MIN_VALID_TRADES = 5
MIN_HOLDOUT_TRADES = 5
MAX_FILTERS_PER_SETUP = 40

NUMERIC_FEATURES = [
    "signal_minute",
    "quality_score",
    "rs_pct",
    "market_ret_pct",
    "vol_ratio",
    "atr_pct",
    "body_pct",
    "close_loc",
    "vwap_dist_atr",
    "signal_volume",
]


@dataclass(frozen=True)
class FilterCondition:
    label: str
    feature: str
    op: str
    value: object


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


def _split_masks(df: pd.DataFrame) -> dict[str, pd.Series]:
    return {
        "train": df["date"] <= TRAIN_END,
        "valid": (df["date"] >= VALID_START) & (df["date"] <= VALID_END),
        "holdout": (df["date"] >= HOLDOUT_START) & (df["date"] <= HOLDOUT_END),
        "full": pd.Series(True, index=df.index),
    }


def _metrics(frame: pd.DataFrame) -> dict[str, float | int]:
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


def _stats(frame: pd.DataFrame) -> dict[str, dict[str, float | int]]:
    if frame.empty:
        return {split: _metrics(frame) for split in ["train", "valid", "holdout", "full"]}
    return {split: _metrics(frame.loc[mask]) for split, mask in _split_masks(frame).items()}


def _flatten_stats(row: dict, frame: pd.DataFrame) -> dict:
    for split, metric in _stats(frame).items():
        for key, value in metric.items():
            row[f"{split}_{key}"] = value
    return row


def _decision(row: dict) -> str:
    train_ok = row["train_trades"] >= MIN_TRAIN_TRADES and _pf_score(row["train_pf"]) > 1.5
    valid_ok = row["valid_trades"] >= MIN_VALID_TRADES and row["valid_pnl"] > 0 and _pf_score(row["valid_pf"]) >= 1.0
    holdout_ok = row["holdout_trades"] >= MIN_HOLDOUT_TRADES and row["holdout_pnl"] > 0 and _pf_score(row["holdout_pf"]) >= 1.0
    full_ok = row["full_trades"] >= (MIN_TRAIN_TRADES + MIN_VALID_TRADES + MIN_HOLDOUT_TRADES) and row["full_pnl"] > 0 and _pf_score(row["full_pf"]) >= 1.3
    if train_ok and valid_ok and holdout_ok and full_ok:
        return "promising_probation"
    if train_ok and valid_ok:
        return "train_valid_pass_holdout_or_full_weak"
    if train_ok:
        return "train_only_overfit_risk"
    return "reject_train_pf_or_count"


def _load_trades() -> pd.DataFrame:
    df = pd.read_csv(TRADES_CSV)
    df["date"] = pd.to_datetime(df["trade_date"], errors="coerce")
    df["pnl"] = pd.to_numeric(df["v6_net_pnl_rs"], errors="coerce").fillna(0.0)
    signal_ts = pd.to_datetime(df["signal_time_ist"], errors="coerce")
    df["signal_minute"] = signal_ts.dt.hour * 60 + signal_ts.dt.minute
    for col in NUMERIC_FEATURES:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df.dropna(subset=["date"]).copy()


def _mask_for_condition(df: pd.DataFrame, cond: FilterCondition) -> pd.Series:
    if cond.op == ">=":
        return pd.to_numeric(df[cond.feature], errors="coerce") >= float(cond.value)
    if cond.op == "<=":
        return pd.to_numeric(df[cond.feature], errors="coerce") <= float(cond.value)
    if cond.op == "==":
        return df[cond.feature].astype(str).eq(str(cond.value))
    if cond.op == "!=":
        return ~df[cond.feature].astype(str).eq(str(cond.value))
    raise ValueError(f"unknown op {cond.op!r}")


def _condition_pool(setup_df: pd.DataFrame) -> list[FilterCondition]:
    train = setup_df.loc[setup_df["date"] <= TRAIN_END].copy()
    conditions: list[FilterCondition] = []
    for feature in NUMERIC_FEATURES:
        if feature not in train.columns:
            continue
        s = pd.to_numeric(train[feature], errors="coerce").dropna()
        if s.nunique() < 8:
            continue
        for q in [0.20, 0.35, 0.50, 0.65, 0.80]:
            value = float(s.quantile(q))
            if not np.isfinite(value):
                continue
            rounded = round(value, 6)
            conditions.append(FilterCondition(f"{feature}>={rounded}", feature, ">=", rounded))
            conditions.append(FilterCondition(f"{feature}<={rounded}", feature, "<=", rounded))
    if "regime" in train.columns:
        regimes = sorted(str(x) for x in train["regime"].dropna().unique())
        for regime in regimes:
            conditions.append(FilterCondition(f"regime=={regime}", "regime", "==", regime))
            if regime in {"BULL", "BEAR"}:
                conditions.append(FilterCondition(f"regime!={regime}", "regime", "!=", regime))
    return conditions


def _candidate_row(setup: str, conditions: tuple[FilterCondition, ...], frame: pd.DataFrame) -> dict:
    row = {
        "setup": setup,
        "condition_count": len(conditions),
        "filter": " AND ".join(cond.label for cond in conditions),
    }
    _flatten_stats(row, frame)
    row["decision"] = _decision(row)
    row["score"] = (
        _pf_score(row["train_pf"]) * 0.35
        + _pf_score(row["valid_pf"]) * 0.30
        + _pf_score(row["holdout_pf"]) * 0.25
        + min(float(row["full_trades"]), 250.0) / 250.0 * 0.10
    )
    return row


def _mine_setup(setup: str, setup_df: pd.DataFrame) -> list[dict]:
    conditions = _condition_pool(setup_df)
    masks: dict[FilterCondition, pd.Series] = {}
    one_pass: list[tuple[FilterCondition, pd.Series, dict]] = []
    rows: list[dict] = []
    train_mask = setup_df["date"] <= TRAIN_END

    for cond in conditions:
        mask = _mask_for_condition(setup_df, cond).fillna(False)
        if int((mask & train_mask).sum()) < MIN_TRAIN_TRADES:
            continue
        frame = setup_df.loc[mask].copy()
        row = _candidate_row(setup, (cond,), frame)
        masks[cond] = mask
        if _pf_score(row["train_pf"]) >= 1.0:
            one_pass.append((cond, mask, row))
        if _pf_score(row["train_pf"]) > 1.5:
            rows.append(row)

    one_pass = sorted(one_pass, key=lambda item: _pf_score(item[2]["train_pf"]), reverse=True)[:MAX_FILTERS_PER_SETUP]
    for i, (left, left_mask, _) in enumerate(one_pass):
        for right, right_mask, _ in one_pass[i + 1 :]:
            if left.feature == right.feature and left.op != right.op:
                continue
            mask = left_mask & right_mask
            if int((mask & train_mask).sum()) < MIN_TRAIN_TRADES:
                continue
            frame = setup_df.loc[mask].copy()
            row = _candidate_row(setup, (left, right), frame)
            if _pf_score(row["train_pf"]) > 1.5:
                rows.append(row)

    if not rows:
        return []
    out = pd.DataFrame(rows).drop_duplicates(subset=["setup", "filter"])
    out = out.sort_values(
        ["decision", "score", "train_pf", "full_trades"],
        ascending=[True, False, False, False],
    )
    return out.head(80).to_dict("records")


def _scenario_rows(label: str, frame: pd.DataFrame) -> list[dict]:
    rows = []
    for split, stats in _stats(frame).items():
        row = {"label": label, "split": split}
        row.update(stats)
        rows.append(row)
    return rows


def _apply_filter(df: pd.DataFrame, filter_text: str) -> pd.Series:
    mask = pd.Series(True, index=df.index)
    for part in filter_text.split(" AND "):
        if ">=" in part:
            feature, value = part.split(">=", 1)
            mask &= pd.to_numeric(df[feature], errors="coerce") >= float(value)
        elif "<=" in part:
            feature, value = part.split("<=", 1)
            mask &= pd.to_numeric(df[feature], errors="coerce") <= float(value)
        elif "==" in part:
            feature, value = part.split("==", 1)
            mask &= df[feature].astype(str).eq(value)
        elif "!=" in part:
            feature, value = part.split("!=", 1)
            mask &= ~df[feature].astype(str).eq(value)
        else:
            raise ValueError(f"cannot parse filter part {part!r}")
    return mask.fillna(False)


def main() -> None:
    trades = _load_trades()
    candidate_rows: list[dict] = []
    for setup, setup_df in trades.groupby("setup", sort=True):
        rows = _mine_setup(str(setup), setup_df.copy())
        candidate_rows.extend(rows)
        print(f"[mine] {setup} rows={len(rows)}", flush=True)

    candidates = pd.DataFrame(candidate_rows)
    if candidates.empty:
        candidates.to_csv(OUT_DIR / "tier123_subfilter_candidates.csv", index=False)
        pd.DataFrame().to_csv(OUT_DIR / "tier123_subfilter_best_by_setup.csv", index=False)
        pd.DataFrame().to_csv(OUT_DIR / "tier123_subfilter_scenarios.csv", index=False)
        print("[mine] no train-pf>1.5 subfilters found", flush=True)
        return

    candidates = candidates.sort_values(["decision", "score", "valid_pf", "holdout_pf", "train_pf"], ascending=[True, False, False, False, False])
    candidates.to_csv(OUT_DIR / "tier123_subfilter_candidates.csv", index=False)
    best = candidates.sort_values(["setup", "decision", "score"], ascending=[True, True, False]).groupby("setup", as_index=False).head(3)
    best.to_csv(OUT_DIR / "tier123_subfilter_best_by_setup.csv", index=False)

    scenario_rows: list[dict] = []
    promising = candidates.loc[candidates["decision"].eq("promising_probation")].copy()
    train_valid = candidates.loc[candidates["decision"].eq("train_valid_pass_holdout_or_full_weak")].copy()
    for label, pool in [("subfilter_promising_probation", promising), ("subfilter_train_valid_only", train_valid)]:
        frames = []
        for _, row in pool.iterrows():
            setup_df = trades.loc[trades["setup"].astype(str).eq(str(row["setup"]))].copy()
            frames.append(setup_df.loc[_apply_filter(setup_df, str(row["filter"]))].copy())
        merged = pd.concat(frames, ignore_index=True) if frames else trades.iloc[0:0].copy()
        if not merged.empty:
            merged = merged.sort_values(["date", "setup", "quality_score"], ascending=[True, True, False]).drop_duplicates(subset=["candidate_id"], keep="first")
        scenario_rows.extend(_scenario_rows(label, merged))
    pd.DataFrame(scenario_rows).to_csv(OUT_DIR / "tier123_subfilter_scenarios.csv", index=False)

    print(f"[mine] wrote {OUT_DIR / 'tier123_subfilter_candidates.csv'} rows={len(candidates):,}", flush=True)
    cols = [
        "setup",
        "filter",
        "train_trades",
        "train_pf",
        "valid_trades",
        "valid_pf",
        "holdout_trades",
        "holdout_pf",
        "full_trades",
        "full_pf",
        "decision",
    ]
    print(candidates[cols].head(40).to_string(index=False, max_colwidth=120), flush=True)


if __name__ == "__main__":
    main()
