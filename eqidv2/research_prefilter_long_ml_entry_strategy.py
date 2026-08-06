"""Research-only ML/pattern discovery for one causal LONG entry strategy.

The input is the audited six-month opportunity cache produced by
``research_prefilter_long_5m_gt5pct.py``.  This module never changes a live
configuration.  It uses only information known at a completed 5-minute signal
bar, enters at the next 5-minute bar's open, permits one entry per ticker/day,
and applies a causal daily capacity cap.

The +5% session-maximum label is an oracle research endpoint, not an exit rule.
Stop loss, target, costs, slippage, and P&L are intentionally deferred.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import platform
import pprint
import sys
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

import joblib
import numpy as np
import pandas as pd
import pyarrow
import sklearn
from scipy.special import expit, logit
from sklearn.cluster import KMeans
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    average_precision_score,
    brier_score_loss,
    log_loss,
    roc_auc_score,
    silhouette_score,
)
from sklearn.preprocessing import StandardScaler
from sklearn.tree import DecisionTreeClassifier, DecisionTreeRegressor, _tree


IST = "Asia/Kolkata"
TARGET_RETURN_PCT = 5.0
DAILY_CAP = 15
RANDOM_SEED = 20260805
SOURCE_SCHEMA = "prefilter_long_gt5_causal_v2"
MODEL_SCHEMA = "prefilter_long_ml_entry_v1"
START_DATE = "2026-02-05"
END_DATE = "2026-08-04"
FRESH_HOLDOUT_START = "2026-08-06"

DEFAULT_CACHE = Path(
    r"C:\TradingData\eqidv2_experiments"
    r"\prefilter_long_5m_gt5pct_20260205_20260804"
    r"\causal_entry_opportunities_v2.parquet"
)
DEFAULT_OUT = Path(
    r"C:\TradingData\eqidv2_experiments"
    r"\prefilter_long_ml_entry_strategy_20260205_20260804"
)
DEFAULT_1M_DIR = Path(r"C:\TradingData\eqidv2\stocks_indicators_1min_eq")
DEFAULT_5M_DIR = Path(r"C:\TradingData\eqidv2\stocks_indicators_5min_eq_live2")

# Strict pre-entry allowlist.  In particular, no entry price, future maximum,
# path completeness, 1m/5m provenance, EOD value, or target field is allowed.
BASE_FEATURES = (
    "selection_rank",
    "overall_score",
    "long_score",
    "short_score",
    "activity_score",
    "signal_minute",
    "RSI",
    "ADX",
    "atr_pct",
    "vwap_dist_atr",
    "ema20_dist_atr",
    "ema50_dist_atr",
    "ema200_dist_atr",
    "ret_5m_pct",
    "ret_15m_pct",
    "ret_30m_pct",
    "ret_60m_pct",
    "session_return_so_far_pct",
    "range_pct",
    "body_pct",
    "upper_wick_pct",
    "lower_wick_pct",
    "close_position_in_bar",
    "volume_ratio20",
    "distance_from_running_session_high_atr",
    "distance_from_session_high_pct",
    "rebound_from_session_low_pct",
    "ema_long_stack",
)

ENGINEERED_FEATURES = (
    "minutes_to_close",
    "score_margin",
    "body_to_range",
    "wick_asymmetry",
    "breakout_prior_high",
    "above_vwap",
    "close_near_high",
    "rank_normalized",
    "log_traded_value",
    "minute_sin",
    "minute_cos",
    "family_MOMENTUM",
    "family_EXPANSION",
    "family_REVERSAL",
    "bucket_LONG",
    "bucket_ACTIVITY",
    "bucket_SHORT",
)

MODEL_FEATURES = (*BASE_FEATURES, *ENGINEERED_FEATURES)

FUTURE_PREFIXES = (
    "forward_",
    "daily_max_",
    "eod_",
    "hit_5pct",
    "one_minute_",
    "five_minute_",
    "cross_tf_",
)
FUTURE_EXACT = {
    "entry_price",
    "entry_gap_filled",
    "entry_price_source_bar_end_ist",
    "entry_execution_time_ist",
    "first_eligible_1m_bar_end_ist",
    "max_window_complete",
    "max_forward_return_pct",
    "max_forward_return_pct_5m",
    "max_time_resolution",
    "daily_max_time_source",
    "future_extreme_review_flag",
}

# This compact candidate was preannounced after an initial TRAIN-tail HGB
# surrogate exploration and before historical-confirmation evaluation.  The
# formal replay below deliberately retains it instead of silently replacing it
# with a later candidate.  Its initial exploratory estimator was not persisted,
# so the provenance record explicitly distinguishes recorded thresholds from a
# bit-for-bit replayable model artifact.
FROZEN_RULE = (
    {"feature": "atr_pct", "op": ">=", "value": 1.05},
    {"feature": "range_pct", "op": ">=", "value": 1.25},
    {"feature": "vwap_dist_atr", "op": ">=", "value": 0.05},
    {"feature": "signal_minute", "op": "<=", "value": 855.0},
)

RAW_INITIAL_SURROGATE_LEAF = (
    {"feature": "atr_pct", "op": ">=", "value": 1.0472424030303955},
    {"feature": "range_pct", "op": ">=", "value": 1.2497979998588562},
    {"feature": "vwap_dist_atr", "op": ">=", "value": 0.04591992683708668},
    {"feature": "signal_minute", "op": "<=", "value": 857.5},
)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def sha256_json(value: Any) -> str:
    payload = json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def jsonable(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [jsonable(item) for item in value]
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        value = float(value)
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if value is pd.NA:
        return None
    return value


def write_json(path: Path, value: Any) -> None:
    path.write_text(
        json.dumps(jsonable(value), indent=2, sort_keys=True, default=str) + "\n",
        encoding="utf-8",
    )


def canonical_rule_text(conditions: Sequence[Mapping[str, Any]]) -> str:
    return " AND ".join(
        f"{item['feature']} {item['op']} {float(item['value']):.8g}"
        for item in conditions
    )


def add_engineered_features(frame: pd.DataFrame) -> pd.DataFrame:
    work = frame.copy()
    numeric = lambda name: pd.to_numeric(work[name], errors="coerce")
    work["minutes_to_close"] = 930.0 - numeric("signal_minute")
    work["score_margin"] = numeric("long_score") - numeric("short_score")
    work["body_to_range"] = numeric("body_pct") / numeric("range_pct").replace(0.0, np.nan)
    work["wick_asymmetry"] = numeric("lower_wick_pct") - numeric("upper_wick_pct")
    work["breakout_prior_high"] = numeric("distance_from_session_high_pct").ge(0.0).astype(float)
    work["above_vwap"] = numeric("vwap_dist_atr").ge(0.0).astype(float)
    work["close_near_high"] = numeric("close_position_in_bar").ge(0.75).astype(float)
    work["rank_normalized"] = (numeric("selection_rank") - 200.0) / 100.0
    work["log_traded_value"] = np.log1p(numeric("traded_value_rs").clip(lower=0.0))
    minute_phase = (numeric("signal_minute") - 570.0) / 360.0 * 2.0 * math.pi
    work["minute_sin"] = np.sin(minute_phase)
    work["minute_cos"] = np.cos(minute_phase)
    family = work["primary_family"].astype(str).str.upper()
    bucket = work["selection_bucket"].astype(str).str.upper()
    for label in ("MOMENTUM", "EXPANSION", "REVERSAL"):
        work[f"family_{label}"] = family.eq(label).astype(float)
    for label in ("LONG", "ACTIVITY", "SHORT"):
        work[f"bucket_{label}"] = bucket.str.endswith(f":{label}").astype(float)
    return work


def load_learning_table(cache_path: Path) -> tuple[pd.DataFrame, dict[str, Any]]:
    raw = pd.read_parquet(cache_path)
    total = len(raw)
    incomplete = int((~raw["max_window_complete"].fillna(False)).sum())
    work = raw.loc[
        raw["max_window_complete"].fillna(False)
        & pd.to_numeric(raw["max_forward_return_pct"], errors="coerce").notna()
    ].copy()
    work["trade_date"] = work["trade_date"].astype(str)
    work = add_engineered_features(work)
    dates = sorted(work["trade_date"].unique())
    if len(dates) != 120:
        raise RuntimeError(f"expected 120 sessions, found {len(dates)}")
    train_dates = dates[:72]
    validation_dates = dates[72:96]
    confirmation_dates = dates[96:]
    mapping = {day: "TRAIN" for day in train_dates}
    mapping.update({day: "VALIDATION" for day in validation_dates})
    mapping.update({day: "HISTORICAL_CONFIRMATION" for day in confirmation_dates})
    work["split"] = work["trade_date"].map(mapping)
    work = work.sort_values(
        ["trade_date", "ticker", "entry_execution_time_ist", "membership_slot_ist"],
        kind="mergesort",
    ).reset_index(drop=True)
    return work, {
        "source_rows": total,
        "complete_rows": len(work),
        "incomplete_rows_excluded": incomplete,
        "positives": int(work["hit_5pct"].sum()),
        "ticker_days": int(work["ticker_day_id"].nunique()),
        "sessions": len(dates),
        "train": [train_dates[0], train_dates[-1], len(train_dates)],
        "validation": [validation_dates[0], validation_dates[-1], len(validation_dates)],
        "historical_confirmation": [
            confirmation_dates[0],
            confirmation_dates[-1],
            len(confirmation_dates),
        ],
    }


def feature_matrix(frame: pd.DataFrame) -> pd.DataFrame:
    missing = [name for name in MODEL_FEATURES if name not in frame]
    if missing:
        raise RuntimeError(f"missing model features: {missing}")
    return frame.loc[:, MODEL_FEATURES].replace([np.inf, -np.inf], np.nan).astype("float32")


def ticker_day_weights(frame: pd.DataFrame) -> np.ndarray:
    counts = frame.groupby("ticker_day_id", sort=False).size()
    weights = frame["ticker_day_id"].map(1.0 / counts).to_numpy(float)
    return weights / max(weights.mean(), 1e-12)


def apply_rule(frame: pd.DataFrame, conditions: Sequence[Mapping[str, Any]]) -> np.ndarray:
    mask = np.ones(len(frame), dtype=bool)
    for item in conditions:
        values = pd.to_numeric(frame[str(item["feature"])], errors="coerce").to_numpy(float)
        finite = np.isfinite(values)
        threshold = float(item["value"])
        if item["op"] in {">", ">="}:
            mask &= finite & (values >= threshold)
        elif item["op"] in {"<", "<="}:
            mask &= finite & (values <= threshold)
        else:
            raise ValueError(f"unsupported operator: {item['op']}")
    return mask


def causal_entries(
    frame: pd.DataFrame,
    mask: np.ndarray,
    *,
    daily_cap: int | None = DAILY_CAP,
    tie_score_column: str | None = None,
) -> pd.DataFrame:
    selected = frame.loc[mask].copy()
    if selected.empty:
        return selected
    selected = selected.sort_values(
        ["trade_date", "ticker", "entry_execution_time_ist", "membership_slot_ist"],
        kind="mergesort",
    ).drop_duplicates(["trade_date", "ticker"], keep="first")
    ordering = ["trade_date", "entry_execution_time_ist"]
    ascending = [True, True]
    if tie_score_column and tie_score_column in selected:
        ordering.append(tie_score_column)
        ascending.append(False)
    ordering.extend(["selection_rank", "ticker"])
    ascending.extend([True, True])
    selected = selected.sort_values(ordering, ascending=ascending, kind="mergesort")
    if daily_cap is not None:
        selected = selected.loc[selected.groupby("trade_date").cumcount().lt(int(daily_cap))]
    return selected.reset_index(drop=True)


def wilson_interval(hits: int, total: int, z: float = 1.96) -> tuple[float, float]:
    if total <= 0:
        return 0.0, 0.0
    p = hits / total
    denominator = 1.0 + z * z / total
    centre = p + z * z / (2.0 * total)
    spread = z * math.sqrt((p * (1.0 - p) + z * z / (4.0 * total)) / total)
    return max(0.0, (centre - spread) / denominator), min(
        1.0, (centre + spread) / denominator
    )


def block_bootstrap_hit_rate(entries: pd.DataFrame, repetitions: int = 2000) -> tuple[float, float]:
    if entries.empty:
        return 0.0, 0.0
    daily = entries.groupby("trade_date")["hit_5pct"].agg(["size", "sum"])
    values = daily[["size", "sum"]].to_numpy(float)
    rng = np.random.default_rng(RANDOM_SEED)
    rates = np.empty(repetitions, dtype=float)
    for index in range(repetitions):
        sample = values[rng.integers(0, len(values), len(values))].sum(axis=0)
        rates[index] = sample[1] / max(sample[0], 1.0)
    return float(np.quantile(rates, 0.025)), float(np.quantile(rates, 0.975))


def strategy_metrics(
    frame: pd.DataFrame,
    mask: np.ndarray,
    *,
    daily_cap: int | None = DAILY_CAP,
    baseline_rate: float | None = None,
    tie_score_column: str | None = None,
) -> tuple[dict[str, Any], pd.DataFrame]:
    entries = causal_entries(
        frame,
        mask,
        daily_cap=daily_cap,
        tie_score_column=tie_score_column,
    )
    total = len(entries)
    hits = int(entries["hit_5pct"].sum()) if total else 0
    rate = hits / total if total else 0.0
    daily = entries.groupby("trade_date").size() if total else pd.Series(dtype=float)
    low, high = wilson_interval(hits, total)
    boot_low, boot_high = block_bootstrap_hit_rate(entries)
    achievable = int(frame.groupby(["trade_date", "ticker"])["hit_5pct"].max().sum())
    metrics = {
        "entries": total,
        "hits_5pct": hits,
        "hit_rate": rate,
        "lift_vs_baseline": (
            rate / max(float(baseline_rate), 1e-12) if baseline_rate is not None else None
        ),
        "wilson_lower_95": low,
        "wilson_upper_95": high,
        "day_bootstrap_lower_95": boot_low,
        "day_bootstrap_upper_95": boot_high,
        "capture_of_achievable": hits / achievable if achievable else 0.0,
        "active_days": int(entries["trade_date"].nunique()) if total else 0,
        "median_entries_per_active_day": float(daily.median()) if len(daily) else 0.0,
        "mean_entries_per_all_session": total / max(frame["trade_date"].nunique(), 1),
        "p90_entries_per_active_day": float(daily.quantile(0.90)) if len(daily) else 0.0,
        "max_entries_per_day": int(daily.max()) if len(daily) else 0,
        "median_mfe_pct": float(entries["max_forward_return_pct"].median()) if total else 0.0,
        "median_eod_return_pct": float(entries["eod_return_pct"].median()) if total else 0.0,
    }
    return metrics, entries


@dataclass
class FittedModel:
    name: str
    imputer: SimpleImputer
    estimator: Any
    calibrator: LogisticRegression
    scaler: StandardScaler | None = None

    def raw_probability(self, matrix: pd.DataFrame) -> np.ndarray:
        values = self.imputer.transform(matrix)
        if self.scaler is not None:
            values = self.scaler.transform(values)
        return self.estimator.predict_proba(values)[:, 1]

    def probability(self, matrix: pd.DataFrame) -> np.ndarray:
        raw = np.clip(self.raw_probability(matrix), 1e-7, 1.0 - 1e-7)
        return self.calibrator.predict_proba(logit(raw).reshape(-1, 1))[:, 1]


def fit_sigmoid(raw_probability: np.ndarray, labels: np.ndarray, weights: np.ndarray) -> LogisticRegression:
    raw = np.clip(raw_probability, 1e-7, 1.0 - 1e-7)
    calibrator = LogisticRegression(C=1e6, max_iter=300, random_state=RANDOM_SEED)
    calibrator.fit(logit(raw).reshape(-1, 1), labels, sample_weight=weights)
    return calibrator


def fit_model(
    name: str,
    train_x: pd.DataFrame,
    train_y: np.ndarray,
    train_w: np.ndarray,
    calibration_x: pd.DataFrame,
    calibration_y: np.ndarray,
    calibration_w: np.ndarray,
) -> FittedModel:
    imputer = SimpleImputer(strategy="median", add_indicator=False)
    train_values = imputer.fit_transform(train_x)
    calibration_values = imputer.transform(calibration_x)
    scaler: StandardScaler | None = None
    if name == "LOGISTIC_L2":
        scaler = StandardScaler()
        train_values = scaler.fit_transform(train_values)
        calibration_values = scaler.transform(calibration_values)
        estimator = LogisticRegression(
            C=0.20,
            max_iter=400,
            random_state=RANDOM_SEED,
        )
    elif name == "SHALLOW_TREE":
        estimator = DecisionTreeClassifier(
            max_depth=4,
            min_samples_leaf=1200,
            random_state=RANDOM_SEED,
        )
    elif name == "HIST_GRADIENT_BOOSTING":
        estimator = HistGradientBoostingClassifier(
            max_iter=180,
            learning_rate=0.06,
            max_leaf_nodes=15,
            min_samples_leaf=300,
            l2_regularization=3.0,
            random_state=RANDOM_SEED,
        )
    else:
        raise ValueError(name)
    estimator.fit(train_values, train_y, sample_weight=train_w)
    raw_calibration = estimator.predict_proba(calibration_values)[:, 1]
    calibrator = fit_sigmoid(raw_calibration, calibration_y, calibration_w)
    return FittedModel(name, imputer, estimator, calibrator, scaler)


def expected_calibration_error(labels: np.ndarray, probability: np.ndarray, weights: np.ndarray) -> float:
    order = np.argsort(probability)
    bins = np.array_split(order, 10)
    total_weight = max(weights.sum(), 1e-12)
    error = 0.0
    for positions in bins:
        if not len(positions):
            continue
        weight = weights[positions]
        observed = np.average(labels[positions], weights=weight)
        predicted = np.average(probability[positions], weights=weight)
        error += weight.sum() / total_weight * abs(observed - predicted)
    return float(error)


def row_model_metrics(labels: np.ndarray, probability: np.ndarray, weights: np.ndarray) -> dict[str, Any]:
    return {
        "rows": len(labels),
        "positives": int(labels.sum()),
        "prevalence_unweighted": float(labels.mean()),
        "pr_auc_weighted": float(average_precision_score(labels, probability, sample_weight=weights)),
        "roc_auc_weighted": float(roc_auc_score(labels, probability, sample_weight=weights)),
        "brier_weighted": float(brier_score_loss(labels, probability, sample_weight=weights)),
        "log_loss_weighted": float(log_loss(labels, probability, sample_weight=weights)),
        "ece_10bin_weighted": expected_calibration_error(labels, probability, weights),
        "mean_prediction_weighted": float(np.average(probability, weights=weights)),
        "observed_rate_weighted": float(np.average(labels, weights=weights)),
    }


def walk_forward_models(frame: pd.DataFrame, matrix: pd.DataFrame) -> pd.DataFrame:
    dates = sorted(frame.loc[frame["split"].eq("TRAIN"), "trade_date"].unique())
    folds = (
        ("WF1", dates[:30], dates[30:40], dates[40:50]),
        ("WF2", dates[:40], dates[40:50], dates[50:60]),
        ("WF3", dates[:50], dates[50:60], dates[60:72]),
    )
    labels = frame["hit_5pct"].astype(int).to_numpy()
    weights = ticker_day_weights(frame)
    rows: list[dict[str, Any]] = []
    for fold, train_dates, calibration_dates, test_dates in folds:
        train_mask = frame["trade_date"].isin(train_dates).to_numpy()
        calibration_mask = frame["trade_date"].isin(calibration_dates).to_numpy()
        test_mask = frame["trade_date"].isin(test_dates).to_numpy()
        for name in ("LOGISTIC_L2", "SHALLOW_TREE", "HIST_GRADIENT_BOOSTING"):
            fitted = fit_model(
                name,
                matrix.loc[train_mask],
                labels[train_mask],
                weights[train_mask],
                matrix.loc[calibration_mask],
                labels[calibration_mask],
                weights[calibration_mask],
            )
            probability = fitted.probability(matrix.loc[test_mask])
            rows.append(
                {
                    "fold": fold,
                    "model": name,
                    "model_start": train_dates[0],
                    "model_end": train_dates[-1],
                    "calibration_start": calibration_dates[0],
                    "calibration_end": calibration_dates[-1],
                    "test_start": test_dates[0],
                    "test_end": test_dates[-1],
                    **row_model_metrics(labels[test_mask], probability, weights[test_mask]),
                }
            )
    return pd.DataFrame(rows)


def permutation_importance_rows(
    fitted: FittedModel,
    matrix: pd.DataFrame,
    labels: np.ndarray,
    weights: np.ndarray,
    *,
    maximum_rows: int = 30000,
) -> pd.DataFrame:
    rng = np.random.default_rng(RANDOM_SEED)
    positions = np.arange(len(matrix))
    if len(positions) > maximum_rows:
        positions = rng.choice(positions, maximum_rows, replace=False)
    subset = matrix.iloc[positions].copy()
    y = labels[positions]
    w = weights[positions]
    baseline_probability = fitted.probability(subset)
    baseline_ap = average_precision_score(y, baseline_probability, sample_weight=w)
    rows: list[dict[str, Any]] = []
    for feature in MODEL_FEATURES:
        shuffled = subset.copy()
        values = shuffled[feature].to_numpy(copy=True)
        shuffled[feature] = values[rng.permutation(len(values))]
        probability = fitted.probability(shuffled)
        rows.append(
            {
                "model": fitted.name,
                "feature": feature,
                "weighted_pr_auc_baseline": baseline_ap,
                "weighted_pr_auc_permuted": average_precision_score(y, probability, sample_weight=w),
                "pr_auc_decrease": baseline_ap
                - average_precision_score(y, probability, sample_weight=w),
            }
        )
    return pd.DataFrame(rows).sort_values("pr_auc_decrease", ascending=False)


def extract_leaf_paths(tree: DecisionTreeRegressor) -> list[tuple[list[dict[str, Any]], float, int]]:
    output: list[tuple[list[dict[str, Any]], float, int]] = []

    def recurse(node: int, path: list[dict[str, Any]]) -> None:
        feature_index = tree.tree_.feature[node]
        if feature_index != _tree.TREE_UNDEFINED:
            feature = MODEL_FEATURES[feature_index]
            threshold = float(tree.tree_.threshold[node])
            recurse(
                tree.tree_.children_left[node],
                [*path, {"feature": feature, "op": "<=", "value": threshold}],
            )
            recurse(
                tree.tree_.children_right[node],
                [*path, {"feature": feature, "op": ">=", "value": threshold}],
            )
        else:
            output.append(
                (
                    path,
                    float(np.ravel(tree.tree_.value[node])[0]),
                    int(tree.tree_.n_node_samples[node]),
                )
            )

    recurse(0, [])
    return output


def collapse_rule(path: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    bounds: dict[str, dict[str, float]] = {}
    for item in path:
        feature = str(item["feature"])
        bounds.setdefault(feature, {})
        if item["op"] in {">", ">="}:
            bounds[feature]["lower"] = max(
                bounds[feature].get("lower", -float("inf")), float(item["value"])
            )
        else:
            bounds[feature]["upper"] = min(
                bounds[feature].get("upper", float("inf")), float(item["value"])
            )
    output: list[dict[str, Any]] = []
    for feature in sorted(bounds):
        if "lower" in bounds[feature]:
            output.append({"feature": feature, "op": ">=", "value": bounds[feature]["lower"]})
        if "upper" in bounds[feature]:
            output.append({"feature": feature, "op": "<=", "value": bounds[feature]["upper"]})
    return output


def conservative_round_rule(conditions: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    granularity = {
        "atr_pct": 0.05,
        "range_pct": 0.05,
        "vwap_dist_atr": 0.05,
        "signal_minute": 5.0,
    }
    output: list[dict[str, Any]] = []
    for item in conditions:
        feature = str(item["feature"])
        step = granularity.get(feature)
        value = float(item["value"])
        if step is not None:
            if item["op"] == ">=":
                value = math.ceil((value - 1e-12) / step) * step
            else:
                value = math.floor((value + 1e-12) / step) * step
        output.append({"feature": feature, "op": str(item["op"]), "value": value})
    return output


def surrogate_rule_candidates(
    frame: pd.DataFrame,
    matrix: pd.DataFrame,
    fitted_hgb: FittedModel,
) -> tuple[pd.DataFrame, list[list[dict[str, Any]]]]:
    train_dates = sorted(frame.loc[frame["split"].eq("TRAIN"), "trade_date"].unique())
    tail_mask = frame["trade_date"].isin(train_dates[60:72]).to_numpy()
    tail = frame.loc[tail_mask]
    tail_matrix = matrix.loc[tail_mask]
    tail_score = fitted_hgb.probability(tail_matrix)
    imputed = fitted_hgb.imputer.transform(tail_matrix)
    weights = ticker_day_weights(frame)[tail_mask]
    candidates: dict[str, list[dict[str, Any]]] = {}
    audit: list[dict[str, Any]] = []
    for depth in (3, 4, 5):
        for minimum_leaf in (250, 500, 1000):
            surrogate = DecisionTreeRegressor(
                max_depth=depth,
                min_samples_leaf=minimum_leaf,
                random_state=RANDOM_SEED,
            ).fit(imputed, tail_score, sample_weight=weights)
            for path, predicted_score, leaf_rows in extract_leaf_paths(surrogate):
                collapsed = collapse_rule(path)
                indicator_features = {
                    item["feature"] for item in collapsed if item["feature"] != "signal_minute"
                }
                if len(indicator_features) > 4:
                    continue
                rounded = conservative_round_rule(collapsed)
                key = canonical_rule_text(rounded)
                candidates[key] = rounded
                metrics, _ = strategy_metrics(
                    tail,
                    apply_rule(tail, rounded),
                    daily_cap=DAILY_CAP,
                )
                audit.append(
                    {
                        "source": "HGB_SURROGATE_LEAF",
                        "tree_depth": depth,
                        "minimum_leaf_rows": minimum_leaf,
                        "leaf_rows": leaf_rows,
                        "leaf_predicted_score": predicted_score,
                        "rule": key,
                        **{f"train_tail_{name}": value for name, value in metrics.items()},
                    }
                )
    # Always retain the deterministic pre-frozen rounded leaf and broad ATR comparator.
    candidates[canonical_rule_text(FROZEN_RULE)] = [dict(item) for item in FROZEN_RULE]
    atr_comparator = [{"feature": "atr_pct", "op": ">=", "value": 0.70}]
    candidates[canonical_rule_text(atr_comparator)] = atr_comparator
    audit_frame = pd.DataFrame(audit)
    if not audit_frame.empty:
        audit_frame = audit_frame.sort_values(
            ["train_tail_hit_rate", "train_tail_hits_5pct", "train_tail_entries"],
            ascending=[False, False, False],
        )
    return audit_frame, list(candidates.values())


def select_rule(
    frame: pd.DataFrame,
    candidates: Sequence[Sequence[Mapping[str, Any]]],
) -> tuple[list[dict[str, Any]], pd.DataFrame]:
    train = frame.loc[frame["split"].eq("TRAIN")]
    train_dates = sorted(train["trade_date"].unique())
    tail = train.loc[train["trade_date"].isin(train_dates[60:72])]
    validation = frame.loc[frame["split"].eq("VALIDATION")]
    base_train, _ = strategy_metrics(train, np.ones(len(train), bool), daily_cap=None)
    base_tail, _ = strategy_metrics(tail, np.ones(len(tail), bool), daily_cap=None)
    base_validation, _ = strategy_metrics(validation, np.ones(len(validation), bool), daily_cap=None)
    tail_rows: list[tuple[float, list[dict[str, Any]], dict[str, Any]]] = []
    for candidate in candidates:
        rule = [dict(item) for item in candidate]
        metrics, _ = strategy_metrics(
            tail,
            apply_rule(tail, rule),
            baseline_rate=float(base_tail["hit_rate"]),
        )
        if metrics["entries"] < 50 or metrics["hits_5pct"] < 5:
            continue
        frequency_penalty = abs(float(metrics["median_entries_per_active_day"]) - 5.0) * 0.002
        score = float(metrics["day_bootstrap_lower_95"]) - frequency_penalty
        tail_rows.append((score, rule, metrics))
    tail_rows.sort(key=lambda item: item[0], reverse=True)
    shortlist = tail_rows[:20]
    frozen_text = canonical_rule_text(FROZEN_RULE)
    frozen_tail = next(
        (item for item in tail_rows if canonical_rule_text(item[1]) == frozen_text),
        None,
    )
    if frozen_tail is not None and not any(
        canonical_rule_text(item[1]) == frozen_text for item in shortlist
    ):
        shortlist.append(frozen_tail)
    audit_rows: list[dict[str, Any]] = []
    eligible: list[tuple[float, list[dict[str, Any]]]] = []
    for tail_rank, (_, rule, tail_metrics) in enumerate(shortlist, 1):
        train_metrics, _ = strategy_metrics(
            train,
            apply_rule(train, rule),
            baseline_rate=float(base_train["hit_rate"]),
        )
        validation_metrics, _ = strategy_metrics(
            validation,
            apply_rule(validation, rule),
            baseline_rate=float(base_validation["hit_rate"]),
        )
        passes = bool(
            train_metrics["entries"] >= 150
            and train_metrics["hits_5pct"] >= 20
            and validation_metrics["entries"] >= 50
            and validation_metrics["hits_5pct"] >= 5
            and validation_metrics["lift_vs_baseline"] >= 2.0
            and validation_metrics["hit_rate"] >= 0.10
            and validation_metrics["median_entries_per_active_day"] >= 3.0
            and validation_metrics["median_entries_per_active_day"] <= 8.0
            and validation_metrics["max_entries_per_day"] <= DAILY_CAP
            and validation_metrics["wilson_lower_95"] > base_validation["hit_rate"]
        )
        selection_score = (
            float(validation_metrics["day_bootstrap_lower_95"]) * 1000.0
            + float(validation_metrics["hit_rate"]) * 20.0
            + math.sqrt(float(validation_metrics["entries"])) * 0.01
        )
        if passes:
            eligible.append((selection_score, rule))
        audit_rows.append(
            {
                "train_tail_rank": tail_rank,
                "rule": canonical_rule_text(rule),
                "condition_count": len(rule),
                "passes_selection_gates": passes,
                "selection_score": selection_score,
                **{f"train_tail_{key}": value for key, value in tail_metrics.items()},
                **{f"train_{key}": value for key, value in train_metrics.items()},
                **{f"validation_{key}": value for key, value in validation_metrics.items()},
            }
        )
    eligible.sort(key=lambda item: item[0], reverse=True)
    frozen = [dict(item) for item in FROZEN_RULE]
    # The exact candidate was announced/frozen before historical confirmation.
    # If it unexpectedly fails the declared selection gates, return no rule rather
    # than silently substituting a post-hoc alternative.
    frozen_row = next(
        (row for row in audit_rows if row["rule"] == canonical_rule_text(frozen)),
        None,
    )
    if frozen_row is None or not frozen_row["passes_selection_gates"]:
        return [], pd.DataFrame(audit_rows)
    return frozen, pd.DataFrame(audit_rows).sort_values(
        ["passes_selection_gates", "selection_score"], ascending=[False, False]
    )


def weighted_quantile(values: np.ndarray, weights: np.ndarray, quantiles: Sequence[float]) -> np.ndarray:
    finite = np.isfinite(values) & np.isfinite(weights) & (weights > 0)
    if not finite.any():
        return np.full(len(quantiles), np.nan)
    x = values[finite]
    w = weights[finite]
    order = np.argsort(x)
    x = x[order]
    w = w[order]
    cumulative = np.cumsum(w) - 0.5 * w
    cumulative /= w.sum()
    return np.interp(np.asarray(quantiles), cumulative, x)


def winner_nonwinner_comparison(frame: pd.DataFrame) -> pd.DataFrame:
    train = frame.loc[frame["split"].eq("TRAIN")].copy()
    weights = ticker_day_weights(train)
    labels = train["hit_5pct"].to_numpy(bool)
    rows: list[dict[str, Any]] = []
    for feature in MODEL_FEATURES:
        values = pd.to_numeric(train[feature], errors="coerce").to_numpy(float)
        winner_q = weighted_quantile(values[labels], weights[labels], (0.10, 0.25, 0.50, 0.75, 0.90))
        loser_q = weighted_quantile(values[~labels], weights[~labels], (0.10, 0.25, 0.50, 0.75, 0.90))
        winner_mean = np.average(values[labels & np.isfinite(values)], weights=weights[labels & np.isfinite(values)]) if np.any(labels & np.isfinite(values)) else np.nan
        loser_mean = np.average(values[(~labels) & np.isfinite(values)], weights=weights[(~labels) & np.isfinite(values)]) if np.any((~labels) & np.isfinite(values)) else np.nan
        pooled = np.nanstd(values)
        rows.append(
            {
                "feature": feature,
                "winner_weighted_mean": winner_mean,
                "nonwinner_weighted_mean": loser_mean,
                "standardized_mean_difference": (winner_mean - loser_mean) / pooled if pooled > 0 else 0.0,
                **{f"winner_p{int(q*100):02d}": value for q, value in zip((.10,.25,.50,.75,.90), winner_q)},
                **{f"nonwinner_p{int(q*100):02d}": value for q, value in zip((.10,.25,.50,.75,.90), loser_q)},
            }
        )
    return pd.DataFrame(rows).sort_values(
        "standardized_mean_difference", key=lambda series: series.abs(), ascending=False
    )


def cluster_winners(frame: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    cluster_features = [
        "atr_pct",
        "range_pct",
        "signal_minute",
        "vwap_dist_atr",
        "RSI",
        "ADX",
        "session_return_so_far_pct",
        "rebound_from_session_low_pct",
        "distance_from_session_high_pct",
        "ema200_dist_atr",
        "volume_ratio20",
        "score_margin",
    ]
    train = frame.loc[frame["split"].eq("TRAIN")].sort_values(
        ["trade_date", "ticker", "entry_execution_time_ist"], kind="mergesort"
    )
    winners = train.loc[train["hit_5pct"]].drop_duplicates("ticker_day_id", keep="first").copy()
    nonwinners = train.loc[
        ~train["ticker_day_id"].isin(train.loc[train["hit_5pct"], "ticker_day_id"].unique())
    ].drop_duplicates("ticker_day_id", keep="first").copy()
    imputer = SimpleImputer(strategy="median")
    scaler = StandardScaler()
    winner_values = scaler.fit_transform(imputer.fit_transform(winners[cluster_features]))
    nonwinner_values = scaler.transform(imputer.transform(nonwinners[cluster_features]))
    silhouette_rows: list[dict[str, Any]] = []
    for clusters in range(2, 7):
        trial = KMeans(n_clusters=clusters, n_init=20, random_state=RANDOM_SEED).fit(winner_values)
        silhouette_rows.append(
            {
                "clusters": clusters,
                "silhouette": silhouette_score(winner_values, trial.labels_),
                "inertia": trial.inertia_,
            }
        )
    silhouette_frame = pd.DataFrame(silhouette_rows)
    chosen_k = int(silhouette_frame.sort_values(["silhouette", "clusters"], ascending=[False, True]).iloc[0]["clusters"])
    model = KMeans(n_clusters=chosen_k, n_init=50, random_state=RANDOM_SEED).fit(winner_values)
    winners["cluster"] = model.labels_
    nonwinners["nearest_winner_cluster"] = model.predict(nonwinner_values)
    range_rows: list[dict[str, Any]] = []
    for cluster in range(chosen_k):
        w = winners.loc[winners["cluster"].eq(cluster)]
        n = nonwinners.loc[nonwinners["nearest_winner_cluster"].eq(cluster)]
        for feature in cluster_features:
            wv = pd.to_numeric(w[feature], errors="coerce")
            nv = pd.to_numeric(n[feature], errors="coerce")
            range_rows.append(
                {
                    "cluster": cluster,
                    "winner_ticker_days": len(w),
                    "nearest_nonwinner_ticker_days": len(n),
                    "feature": feature,
                    "winner_p10": wv.quantile(.10),
                    "winner_p25": wv.quantile(.25),
                    "winner_median": wv.median(),
                    "winner_p75": wv.quantile(.75),
                    "winner_p90": wv.quantile(.90),
                    "nearest_nonwinner_median": nv.median(),
                }
            )
    assignment_columns = [
        "trade_date", "ticker", "ticker_day_id", "entry_execution_time_ist",
        "hit_5pct", "cluster", *cluster_features,
    ]
    return winners[assignment_columns], pd.DataFrame(range_rows), silhouette_frame


def source_and_time_stability(entries: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    if entries.empty:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    work = entries.copy()
    work["month"] = work["trade_date"].str[:7]
    work["entry_hour"] = pd.to_datetime(work["entry_execution_time_ist"]).dt.strftime("%H:00")
    def summarize(keys: list[str]) -> pd.DataFrame:
        return work.groupby(keys, dropna=False)["hit_5pct"].agg(entries="size", hits_5pct="sum", hit_rate="mean").reset_index()
    return summarize(["split", "month"]), summarize(["split", "entry_hour"]), summarize(["split", "daily_max_time_source"])


def enrich_selected_ticker(
    ticker: str,
    entries: pd.DataFrame,
    one_minute_dir: str,
    five_minute_dir: str,
) -> pd.DataFrame:
    from research_prefilter_long_5m_gt5pct import (
        SESSION_FIRST_5M_END,
        SESSION_LAST_END,
        filter_end_stamped_session,
        read_one_minute,
        read_parquet_window,
    )

    one = read_one_minute(Path(one_minute_dir) / f"{ticker}_stocks_indicators_1min.parquet")
    five = read_parquet_window(
        Path(five_minute_dir) / f"{ticker}_stocks_indicators_5min.parquet",
        ("date", "open", "high", "low", "close", "gap_filled"),
    )
    if one is not None and not one.empty:
        one = filter_end_stamped_session(one, first_label="09:16")
    if five is not None and not five.empty:
        for column in ("open", "high", "low", "close", "gap_filled"):
            five[column] = pd.to_numeric(five[column], errors="coerce")
        five = filter_end_stamped_session(five, first_label=SESSION_FIRST_5M_END)
        five = five.loc[five["gap_filled"].fillna(0.0).lt(0.5)].copy()
        five["trade_date"] = five["date"].dt.strftime("%Y-%m-%d")
    output: list[dict[str, Any]] = []
    for _, entry in entries.iterrows():
        row = entry.to_dict()
        day = str(entry["trade_date"])
        execution = pd.Timestamp(entry["entry_execution_time_ist"])
        eod = pd.Timestamp(f"{day} {SESSION_LAST_END}", tz=IST)
        source = str(entry["daily_max_time_source"])
        if source == "1min" and one is not None:
            path = one.loc[one["trade_date"].eq(day) & one["date"].gt(execution) & one["date"].le(eod)].copy()
            interval_minutes = 1
        elif five is not None:
            path = five.loc[five["trade_date"].eq(day) & five["date"].ge(entry["entry_price_source_bar_end_ist"]) & five["date"].le(eod)].copy()
            interval_minutes = 5
        else:
            path = pd.DataFrame()
            interval_minutes = 0
        price = float(entry["entry_price"])
        if path.empty:
            row["extended_outcome_complete"] = False
            output.append(row)
            continue
        path = path.sort_values("date", kind="mergesort")
        low_index = path["low"].astype(float).idxmin()
        row.update(
            {
                "extended_outcome_complete": True,
                "mae_pct": (float(path.loc[low_index, "low"]) / price - 1.0) * 100.0,
                "mae_bar_end_ist": path.loc[low_index, "date"],
                "mae_interval_start_ist": path.loc[low_index, "date"] - pd.Timedelta(minutes=interval_minutes),
                "minutes_to_daily_max": (
                    pd.Timestamp(entry["daily_max_interval_start_ist"]) - execution
                ).total_seconds() / 60.0,
            }
        )
        for target in (2, 3, 4, 5):
            reached = path.loc[path["high"].astype(float).ge(price * (1.0 + target / 100.0) - 1e-9)]
            bar_end = reached.iloc[0]["date"] if not reached.empty else pd.NaT
            row[f"first_hit_{target}pct_bar_end_ist"] = bar_end
            row[f"first_hit_{target}pct_interval_start_ist"] = (
                bar_end - pd.Timedelta(minutes=interval_minutes) if pd.notna(bar_end) else pd.NaT
            )
            row[f"minutes_to_{target}pct"] = (
                (bar_end - pd.Timedelta(minutes=interval_minutes) - execution).total_seconds() / 60.0
                if pd.notna(bar_end) else np.nan
            )
        for horizon in (30, 60, 90, 120):
            target_end = execution + pd.Timedelta(minutes=horizon)
            exact = path.loc[path["date"].eq(target_end)]
            row[f"return_{horizon}m_pct"] = (
                (float(exact.iloc[0]["close"]) / price - 1.0) * 100.0 if not exact.empty else np.nan
            )
        output.append(row)
    return pd.DataFrame(output)


def enrich_selected_entries(
    entries: pd.DataFrame,
    one_minute_dir: Path,
    five_minute_dir: Path,
    workers: int,
) -> pd.DataFrame:
    if entries.empty:
        return entries.copy()
    frames: list[pd.DataFrame] = []
    with ProcessPoolExecutor(max_workers=max(1, workers)) as executor:
        futures = {
            executor.submit(
                enrich_selected_ticker,
                str(ticker),
                group.copy(),
                str(one_minute_dir),
                str(five_minute_dir),
            ): str(ticker)
            for ticker, group in entries.groupby("ticker", sort=False)
        }
        for future in as_completed(futures):
            frames.append(future.result())
    return pd.concat(frames, ignore_index=True, sort=False).sort_values(
        ["trade_date", "entry_execution_time_ist", "ticker"], kind="mergesort"
    ).reset_index(drop=True)


def write_config(path: Path, conditions: Sequence[Mapping[str, Any]], summary: Mapping[str, Any]) -> None:
    conditions_repr = pprint.pformat([dict(item) for item in conditions], width=100, sort_dicts=True)
    metrics_repr = pprint.pformat(summary, width=120, sort_dicts=True)
    content = f'''"""Frozen research-only causal LONG entry candidate.

This file is not production approved.  The +5% label is a research endpoint;
SL, target, transaction costs, slippage, and P&L have not been validated.
"""

import math

PRODUCTION_APPROVED = False
RESEARCH_ONLY = True
SIDE = "LONG"
PREFILTER_RANK_MIN = 200
PREFILTER_RANK_MAX = 300
POOL_ACTIVATION_DELAY_MINUTES = 5
SIGNAL_TIMEFRAME = "5min"
ENTRY_POLICY = "NEXT_5MIN_BAR_OPEN_AT_SIGNAL_BOUNDARY"
ONE_ENTRY_PER_TICKER_DAY = True
DAILY_ENTRY_CAP = {DAILY_CAP}
CAP_TIE_BREAK = ("entry_time", "selection_rank_ascending", "ticker_lexical")
TARGET_LABEL_PCT = {TARGET_RETURN_PCT}
TARGET_LABEL_USE = "RESEARCH_ORACLE_ONLY"
SL_TARGET_DEFERRED = True
HOLDOUT_CONSUMED = True
FRESH_HOLDOUT_START = "{FRESH_HOLDOUT_START}"
FRESH_HOLDOUT_MIN_SESSIONS = 20
FRESH_HOLDOUT_MIN_ACTIVE_DAYS = 15
FRESH_HOLDOUT_MIN_ENTRIES = 60
FRESH_HOLDOUT_MIN_HITS_5PCT = 8
FRESH_HOLDOUT_MIN_HIT_RATE = 0.10
FRESH_HOLDOUT_MIN_LIFT = 2.0
FRESH_HOLDOUT_MIN_DAY_BOOTSTRAP_LIFT_LOWER = 1.0
FRESH_HOLDOUT_MEDIAN_ENTRIES_PER_ACTIVE_DAY = (3.0, 8.0)
FRESH_HOLDOUT_STATUS = "PENDING"
CONDITIONS = {conditions_repr}
HISTORICAL_METRICS = {metrics_repr}


def matches(features):
    for condition in CONDITIONS:
        value = features.get(condition["feature"])
        if value is None:
            return False
        try:
            value = float(value)
        except (TypeError, ValueError):
            return False
        if not math.isfinite(value):
            return False
        if condition["op"] == ">=" and value < float(condition["value"]):
            return False
        if condition["op"] == "<=" and value > float(condition["value"]):
            return False
    return True
'''
    path.write_text(content, encoding="utf-8")


def leakage_audit(frame: pd.DataFrame) -> dict[str, Any]:
    leaky_features = [
        feature
        for feature in MODEL_FEATURES
        if feature in FUTURE_EXACT or any(feature.startswith(prefix) for prefix in FUTURE_PREFIXES)
    ]
    duplicates = int(
        frame.duplicated(["ticker", "membership_slot_ist", "signal_time_ist"]).sum()
    )
    return {
        "passed": not leaky_features and duplicates == 0,
        "strict_feature_allowlist": list(MODEL_FEATURES),
        "leaky_features_found": leaky_features,
        "duplicate_opportunity_keys": duplicates,
        "infinite_feature_values": int(
            np.isinf(feature_matrix(frame).to_numpy(float)).sum()
        ),
        "label_definition": "post-entry same-session high >= entry_price * 1.05",
        "incomplete_paths_excluded": True,
        "ticker_day_equal_weighting": True,
        "random_split_used": False,
        "future_fields_used_as_predictors": False,
    }


def capacity_table(frame: pd.DataFrame, conditions: Sequence[Mapping[str, Any]]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for split in ("TRAIN", "VALIDATION", "HISTORICAL_CONFIRMATION"):
        part = frame.loc[frame["split"].eq(split)]
        baseline, _ = strategy_metrics(part, np.ones(len(part), bool), daily_cap=None)
        for cap in (3, 5, 10, 15):
            metrics, _ = strategy_metrics(
                part,
                apply_rule(part, conditions),
                daily_cap=cap,
                baseline_rate=float(baseline["hit_rate"]),
            )
            rows.append({"split": split, "daily_cap": cap, **metrics})
    return pd.DataFrame(rows)


def build_report(
    path: Path,
    summary: Mapping[str, Any],
    conditions: Sequence[Mapping[str, Any]],
    model_metrics: pd.DataFrame,
) -> None:
    historical = summary["strategy_metrics"]
    lines = [
        "# Six-Month Causal LONG Entry Strategy Study",
        "",
        f"Generated from {summary['data_contract']['complete_rows']:,} complete causal opportunities across 120 sessions.",
        "",
        "## Frozen entry rule",
        "",
        f"`{canonical_rule_text(conditions)}`",
        "",
        "- LONG prefilter members only, inclusive ranks 200–300.",
        "- Hourly pool becomes active at snapshot +5 minutes and remains valid only in its activation window.",
        "- Rule is evaluated on a completed 5-minute bar; entry is the following 5-minute bar open.",
        "- Earliest match per ticker/day; causal first-arrival cap of 15 entries/day.",
        "- The +5% session maximum is only the discovery label. SL, target and P&L remain deferred.",
        "",
        "## Historical decision-policy evidence",
        "",
        "| Segment | Entries | Hits | Hit rate | Baseline | Lift | Median/day | Max/day | Median EOD |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for split in ("TRAIN", "VALIDATION", "HISTORICAL_CONFIRMATION"):
        item = historical[split]
        display_split = "SELECTION_VALIDATION" if split == "VALIDATION" else split
        lines.append(
            f"| {display_split} | {item['entries']:,} | {item['hits_5pct']:,} | "
            f"{item['hit_rate']:.2%} | {item['baseline_hit_rate']:.2%} | "
            f"{item['lift_vs_baseline']:.2f}x | {item['median_entries_per_active_day']:.1f} | "
            f"{item['max_entries_per_day']} | {item['median_eod_return_pct']:.2f}% |"
        )
    lines.extend(
        [
            "",
            "## Interpretation",
            "",
            "The coherent pattern is an early/mid-session volatility-expansion bar: high ATR, a wide current candle, and a close just above causal session VWAP. ATR and bar range are correlated, so this should be treated as one volatility regime plus VWAP confirmation—not four independent discoveries.",
            "",
            "This research predicts the chance of a later +5% intraday excursion. It does not yet demonstrate a profitable trade because the exit, stop, costs and slippage were deliberately not chosen.",
            "",
            "The pure 5-minute target-label sensitivity is nearly identical to the primary 1-minute/fallback label (only two selected decisions differ across all six months). That does not eliminate outcome-source concentration: historical confirmation is 15/55 for 1-minute-source entries but 0/20 for 5-minute-fallback entries; selection-validation fallback is 1/16. August is also 0/7 versus July 15/68. These small cohorts are stability warnings, not filters to tune away.",
            "",
            "The chosen operating objective is sparse high conviction: median 3-8 entries per active day, capped at 15. It does not meet a 10-15 entries/day objective. Recall is deliberately low (about 8-9% of achievable +5% ticker-days).",
            "",
            "## Model evidence",
            "",
            "Three predeclared families were used: regularized logistic regression, a depth-4 tree, and calibrated shallow histogram gradient boosting. ML was used for discovery/ranking; the research candidate is the compact rule above. Twenty TRAIN-tail-shortlisted rules were checked on SELECTION_VALIDATION, so that segment is model-selection evidence, not an untouched holdout.",
            "",
        ]
    )
    walk_forward_only = model_metrics.loc[
        model_metrics["fold"].astype(str).str.startswith("WF")
    ]
    grouped = walk_forward_only.groupby("model")["pr_auc_weighted"].agg(["mean", "min", "max"])
    lines.extend(["| Model | Mean walk-forward PR-AUC | Min | Max |", "|---|---:|---:|---:|"])
    for model, row in grouped.iterrows():
        lines.append(f"| {model} | {row['mean']:.4f} | {row['min']:.4f} | {row['max']:.4f} |")
    lines.extend(
        [
            "",
            "## Promotion status",
            "",
            f"`PRODUCTION_APPROVED=False`. Jul–Aug is already-consumed historical development evidence, not a genuinely fresh holdout. Collect at least 20 full sessions starting {FRESH_HOLDOUT_START} and apply every predeclared fresh gate in summary.json before judging entry stability; then separately validate a causal exit, costs, slippage, PF and drawdown.",
            "",
        ]
    )
    path.write_text("\n".join(lines), encoding="utf-8")


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--cache", default=str(DEFAULT_CACHE))
    parser.add_argument("--out", default=str(DEFAULT_OUT))
    parser.add_argument("--one-minute-dir", default=str(DEFAULT_1M_DIR))
    parser.add_argument("--five-minute-dir", default=str(DEFAULT_5M_DIR))
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--skip-walk-forward", action="store_true")
    args = parser.parse_args(argv)
    cache = Path(args.cache).resolve()
    out = Path(args.out).resolve()
    out.mkdir(parents=True, exist_ok=True)

    frame, data_contract = load_learning_table(cache)
    matrix = feature_matrix(frame)
    labels = frame["hit_5pct"].astype(int).to_numpy()
    weights = ticker_day_weights(frame)
    leakage = leakage_audit(frame)
    if not leakage["passed"]:
        raise RuntimeError(f"leakage audit failed: {leakage}")
    write_json(out / "leakage_and_data_audit.json", leakage)

    train_dates = sorted(frame.loc[frame["split"].eq("TRAIN"), "trade_date"].unique())
    fit_mask = frame["trade_date"].isin(train_dates[:60]).to_numpy()
    calibration_mask = frame["trade_date"].isin(train_dates[60:72]).to_numpy()
    validation_mask = frame["split"].eq("VALIDATION").to_numpy()

    fitted_models: dict[str, FittedModel] = {}
    model_rows: list[dict[str, Any]] = []
    for name in ("LOGISTIC_L2", "SHALLOW_TREE", "HIST_GRADIENT_BOOSTING"):
        fitted = fit_model(
            name,
            matrix.loc[fit_mask], labels[fit_mask], weights[fit_mask],
            matrix.loc[calibration_mask], labels[calibration_mask], weights[calibration_mask],
        )
        fitted_models[name] = fitted
        for split_name, mask in (
            ("TRAIN_CALIBRATION", calibration_mask),
            ("SELECTION_VALIDATION", validation_mask),
        ):
            probability = fitted.probability(matrix.loc[mask])
            model_rows.append(
                {"fold": "FINAL_DISCOVERY", "model": name, "evaluation_segment": split_name,
                 **row_model_metrics(labels[mask], probability, weights[mask])}
            )
    final_model_metrics = pd.DataFrame(model_rows)
    if args.skip_walk_forward:
        previous_metrics = out / "walk_forward_model_metrics.csv"
        if previous_metrics.exists():
            prior = pd.read_csv(previous_metrics)
            walk_forward = prior.loc[~prior["fold"].eq("FINAL_DISCOVERY")].copy()
        else:
            walk_forward = pd.DataFrame()
    else:
        walk_forward = walk_forward_models(frame, matrix)
    all_model_metrics = pd.concat([walk_forward, final_model_metrics], ignore_index=True, sort=False)
    all_model_metrics.to_csv(out / "walk_forward_model_metrics.csv", index=False)

    hgb = fitted_models["HIST_GRADIENT_BOOSTING"]
    importance = permutation_importance_rows(
        hgb,
        matrix.loc[calibration_mask].reset_index(drop=True),
        labels[calibration_mask],
        weights[calibration_mask],
    )
    importance.to_csv(out / "model_feature_importance.csv", index=False)

    surrogate_audit, candidates = surrogate_rule_candidates(frame, matrix, hgb)
    surrogate_audit.to_csv(out / "surrogate_leaf_audit.csv", index=False)
    selected_rule, rule_search = select_rule(frame, candidates)
    rule_search.to_csv(out / "candidate_rule_search.csv", index=False)
    if not selected_rule:
        selected_rule = [dict(item) for item in FROZEN_RULE]
        selection_status = "FROZEN_RULE_FAILED_DECLARED_SELECTION_GATES"
    else:
        selection_status = "FROZEN_BEFORE_HISTORICAL_CONFIRMATION"
    candidate_provenance = {
        "origin": "PREANNOUNCED_INITIAL_TRAIN_TAIL_HGB_SURROGATE_CANDIDATE",
        "provenance_strength": "THRESHOLDS_RECORDED_EXACTLY;_INITIAL_ESTIMATOR_NOT_PERSISTED",
        "discovery_fit_sessions": [train_dates[0], train_dates[59], 60],
        "discovery_calibration_sessions": [train_dates[60], train_dates[71], 12],
        "initial_hgb_parameters": {
            "max_iter": 180,
            "learning_rate": 0.06,
            "max_leaf_nodes": 15,
            "min_samples_leaf": 300,
            "l2_regularization": 3.0,
            "random_state": 42,
        },
        "initial_calibration": "ISOTONIC_ON_FINAL_12_TRAIN_SESSIONS",
        "initial_surrogate": {
            "type": "DecisionTreeRegressor",
            "max_depth": 5,
            "min_samples_leaf": 250,
            "random_state": 42,
            "leaf_predicted_probability": 0.14297893293982458,
        },
        "raw_collapsed_leaf": [dict(item) for item in RAW_INITIAL_SURROGATE_LEAF],
        "rounding_policy": {
            "lower_bounds": "ceil to configured granularity",
            "upper_time_bound": "floor to completed 5-minute grid",
            "granularity": {"atr_pct": 0.05, "range_pct": 0.05, "vwap_dist_atr": 0.05, "signal_minute": 5.0},
        },
        "rounded_rule": selected_rule,
        "selection_validation_checks": {
            "raw_pre_cap": {"entries": 122, "hits": 23, "hit_rate": 23 / 122, "median_per_day": 5.0, "max_per_day": 16},
            "rounded_pre_cap": {"entries": 121, "hits": 23, "hit_rate": 23 / 121, "median_per_day": 5.0, "max_per_day": 16},
            "rounded_causal_cap15": {"entries": 120, "hits": 23, "hit_rate": 23 / 120, "median_per_day": 5.0, "max_per_day": 15},
        },
        "formal_replay_note": "The packaged sigmoid-calibrated model suite is separately reproducible; it is not claimed to be the exact initial exploratory estimator.",
    }
    candidate_provenance["provenance_sha256"] = sha256_json(candidate_provenance)
    write_json(out / "candidate_provenance.json", candidate_provenance)
    freeze_record = {
        "model_schema": MODEL_SCHEMA,
        "frozen_at_ist": pd.Timestamp.now(tz=IST).isoformat(),
        "generator_script": str(Path(__file__).resolve()),
        "generator_script_sha256": sha256_file(Path(__file__).resolve()),
        "source_cache": str(cache),
        "source_cache_sha256": sha256_file(cache),
        "conditions": selected_rule,
        "rule": canonical_rule_text(selected_rule),
        "daily_cap": DAILY_CAP,
        "selection_status": selection_status,
        "candidate_provenance_sha256": candidate_provenance["provenance_sha256"],
        "frozen_before_historical_confirmation_evaluation": True,
        "fresh_holdout_start": FRESH_HOLDOUT_START,
    }
    freeze_record["freeze_sha256"] = sha256_json(freeze_record)
    write_json(out / "candidate_freeze.json", freeze_record)

    # Historical confirmation is evaluated only after the freeze record above.
    strategy_by_split: dict[str, dict[str, Any]] = {}
    selected_frames: list[pd.DataFrame] = []
    for split in ("TRAIN", "VALIDATION", "HISTORICAL_CONFIRMATION"):
        part = frame.loc[frame["split"].eq(split)].copy()
        baseline_metrics, _ = strategy_metrics(part, np.ones(len(part), bool), daily_cap=None)
        metrics, entries = strategy_metrics(
            part,
            apply_rule(part, selected_rule),
            daily_cap=DAILY_CAP,
            baseline_rate=float(baseline_metrics["hit_rate"]),
        )
        metrics["baseline_entries"] = baseline_metrics["entries"]
        metrics["baseline_hits_5pct"] = baseline_metrics["hits_5pct"]
        metrics["baseline_hit_rate"] = baseline_metrics["hit_rate"]
        strategy_by_split[split] = metrics
        entries["split"] = split
        selected_frames.append(entries)
    selected = pd.concat(selected_frames, ignore_index=True, sort=False)

    # Score every complete causal opportunity; scores are discovery diagnostics,
    # not required by the final rule.
    for name, fitted in fitted_models.items():
        frame[f"score_{name.lower()}"] = fitted.probability(matrix)
    frame["frozen_rule_match"] = apply_rule(frame, selected_rule)
    selected_keys = set(
        selected["trade_date"].astype(str) + "|" + selected["ticker"].astype(str)
        + "|" + selected["entry_execution_time_ist"].astype(str)
    )
    frame["selected_by_cap15"] = (
        frame["trade_date"].astype(str) + "|" + frame["ticker"].astype(str)
        + "|" + frame["entry_execution_time_ist"].astype(str)
    ).isin(selected_keys)
    score_columns = list(dict.fromkeys([
        "trade_date", "ticker", "split", "membership_slot_ist", "signal_time_ist",
        "entry_execution_time_ist", "selection_rank", *MODEL_FEATURES,
        "score_logistic_l2", "score_shallow_tree", "score_hist_gradient_boosting",
        "hit_5pct", "max_forward_return_pct", "eod_return_pct",
        "daily_max_price", "daily_max_time_ist", "daily_max_time_source",
        "frozen_rule_match", "selected_by_cap15",
    ]))
    frame.loc[:, score_columns].to_parquet(out / "scored_opportunities.parquet", index=False)
    frame.loc[frame["frozen_rule_match"], score_columns].to_csv(
        out / "scored_rule_matches.csv", index=False
    )

    enriched = enrich_selected_entries(
        selected,
        Path(args.one_minute_dir).resolve(),
        Path(args.five_minute_dir).resolve(),
        args.workers,
    )
    enriched.to_csv(out / "selected_entries_with_extended_outcomes.csv", index=False)
    enriched.loc[enriched["hit_5pct"]].to_csv(out / "selected_gt5pct_movers.csv", index=False)
    compact_columns = [
        "trade_date", "ticker", "split", "membership_slot_ist", "selection_rank",
        "signal_time_ist", "entry_execution_time_ist", "entry_price",
        "atr_pct", "range_pct", "vwap_dist_atr", "hit_5pct",
        "max_forward_return_pct", "daily_max_price", "daily_max_time_ist",
        "daily_max_interval_start_ist", "daily_max_interval_end_ist",
        "daily_max_time_source", "first_hit_5pct_interval_start_ist",
        "first_hit_5pct_bar_end_ist", "mae_pct", "eod_return_pct",
    ]
    enriched.loc[:, compact_columns].to_csv(
        out / "selected_entry_and_peak_times_compact.csv", index=False
    )
    enriched.loc[enriched["hit_5pct"], compact_columns].to_csv(
        out / "selected_gt5pct_entry_and_peak_times_compact.csv", index=False
    )

    comparison = winner_nonwinner_comparison(frame)
    comparison.to_csv(out / "winner_nonwinner_feature_comparison.csv", index=False)
    cluster_assignments, cluster_ranges, silhouette = cluster_winners(frame)
    cluster_assignments.to_csv(out / "train_winner_cluster_assignments.csv", index=False)
    cluster_ranges.to_csv(out / "train_winner_cluster_feature_ranges.csv", index=False)
    silhouette.to_csv(out / "train_winner_cluster_selection.csv", index=False)

    capacity = capacity_table(frame, selected_rule)
    capacity.to_csv(out / "capacity_3_5_10_15_results.csv", index=False)
    monthly, hourly, source = source_and_time_stability(enriched)
    monthly.to_csv(out / "selected_monthly_stability.csv", index=False)
    hourly.to_csv(out / "selected_hourly_stability.csv", index=False)
    source.to_csv(out / "selected_label_source_sensitivity.csv", index=False)

    cross_tf: dict[str, Any] = {}
    five_minute_label: dict[str, Any] = {}
    five_minute_rows: list[dict[str, Any]] = []
    for split in ("TRAIN", "VALIDATION", "HISTORICAL_CONFIRMATION"):
        part = frame.loc[frame["split"].eq(split)]
        clean = part.loc[
            part["cross_tf_target_agreement"].isna()
            | part["cross_tf_target_agreement"].fillna(False)
        ].copy()
        base, _ = strategy_metrics(clean, np.ones(len(clean), bool), daily_cap=None)
        metric, _ = strategy_metrics(
            clean, apply_rule(clean, selected_rule), daily_cap=DAILY_CAP,
            baseline_rate=float(base["hit_rate"]),
        )
        metric["excluded_disagreement_rows"] = int(part["cross_tf_target_agreement"].eq(False).sum())
        cross_tf[split] = metric
        five_part = part.copy()
        five_part["hit_5pct"] = five_part["hit_5pct_5m"].fillna(False).astype(bool)
        five_base, _ = strategy_metrics(
            five_part, np.ones(len(five_part), bool), daily_cap=None
        )
        five_metric, _ = strategy_metrics(
            five_part,
            apply_rule(five_part, selected_rule),
            daily_cap=DAILY_CAP,
            baseline_rate=float(five_base["hit_rate"]),
        )
        five_metric["baseline_hit_rate"] = five_base["hit_rate"]
        five_minute_label[split] = five_metric
        five_minute_rows.append({"split": split, **five_metric})
    pd.DataFrame(five_minute_rows).to_csv(
        out / "five_minute_label_sensitivity.csv", index=False
    )

    confirmation = frame.loc[frame["split"].eq("HISTORICAL_CONFIRMATION")]
    quarantine_days = [day for day in sorted(confirmation["trade_date"].unique()) if day >= "2026-08-03"]
    confirmation_clean = confirmation.loc[~confirmation["trade_date"].isin(quarantine_days)]
    clean_base, _ = strategy_metrics(confirmation_clean, np.ones(len(confirmation_clean), bool), daily_cap=None)
    clean_metric, _ = strategy_metrics(
        confirmation_clean,
        apply_rule(confirmation_clean, selected_rule),
        daily_cap=DAILY_CAP,
        baseline_rate=float(clean_base["hit_rate"]),
    )

    runtime = {
        "python_executable": sys.executable,
        "python": platform.python_version(),
        "numpy": np.__version__,
        "pandas": pd.__version__,
        "pyarrow": pyarrow.__version__,
        "scikit_learn": sklearn.__version__,
        "joblib": joblib.__version__,
    }
    sparse_frequency_pass = all(
        3.0 <= float(item["median_entries_per_active_day"]) <= 8.0
        for item in strategy_by_split.values()
    )
    confirmation_fallback_rows = source.loc[
        source["split"].eq("HISTORICAL_CONFIRMATION")
        & source["daily_max_time_source"].eq("5min_fallback")
    ]
    confirmation_fallback_precision_pass = bool(
        len(confirmation_fallback_rows)
        and int(confirmation_fallback_rows.iloc[0]["entries"]) >= 20
        and float(confirmation_fallback_rows.iloc[0]["hit_rate"]) >= 0.10
    )
    source_stability_pass = confirmation_fallback_precision_pass
    gates = {
        "train_entries_at_least_150": strategy_by_split["TRAIN"]["entries"] >= 150,
        "train_hits_at_least_20": strategy_by_split["TRAIN"]["hits_5pct"] >= 20,
        "selection_validation_entries_at_least_50": strategy_by_split["VALIDATION"]["entries"] >= 50,
        "selection_validation_hits_at_least_5": strategy_by_split["VALIDATION"]["hits_5pct"] >= 5,
        "selection_validation_lift_at_least_2": strategy_by_split["VALIDATION"]["lift_vs_baseline"] >= 2.0,
        "historical_confirmation_lift_at_least_2": strategy_by_split["HISTORICAL_CONFIRMATION"]["lift_vs_baseline"] >= 2.0,
        "selection_validation_precision_preferred_10pct": strategy_by_split["VALIDATION"]["hit_rate"] >= 0.10,
        "historical_confirmation_precision_preferred_10pct": strategy_by_split["HISTORICAL_CONFIRMATION"]["hit_rate"] >= 0.10,
        "sparse_frequency_median_3_to_8_each_segment": sparse_frequency_pass,
        "daily_cap_compliant": all(item["max_entries_per_day"] <= DAILY_CAP for item in strategy_by_split.values()),
        "historical_confirmation_5m_fallback_precision_at_least_10pct": confirmation_fallback_precision_pass,
        "source_cohort_stability": source_stability_pass,
        "fresh_20_session_holdout": False,
        "causal_exit_cost_slippage_pnl": False,
    }
    fresh_gate_contract = {
        "start_on_next_full_session": FRESH_HOLDOUT_START,
        "august_5_excluded_because_freeze_was_intraday": True,
        "minimum_full_sessions": 20,
        "minimum_active_days": 15,
        "minimum_entries": 60,
        "minimum_hits_5pct": 8,
        "minimum_hit_rate": 0.10,
        "minimum_lift_vs_same_period_all_long": 2.0,
        "day_block_bootstrap_lift_lower_95_must_exceed": 1.0,
        "wilson_hit_rate_lower_95_must_exceed_same_period_baseline": True,
        "median_entries_per_active_day_range": [3.0, 8.0],
        "maximum_entries_per_day": DAILY_CAP,
        "pure_5m_label_minimum_lift": 2.0,
        "source_cohort_gate": "for any outcome-source cohort with >=20 entries, hit rate must exceed its same-period cohort baseline",
        "no_threshold_model_or_cap_changes_before_evaluation": True,
    }
    core_gate_names = {
        "train_entries_at_least_150",
        "train_hits_at_least_20",
        "selection_validation_entries_at_least_50",
        "selection_validation_hits_at_least_5",
        "selection_validation_lift_at_least_2",
        "historical_confirmation_lift_at_least_2",
        "selection_validation_precision_preferred_10pct",
        "historical_confirmation_precision_preferred_10pct",
        "sparse_frequency_median_3_to_8_each_segment",
        "daily_cap_compliant",
    }
    summary = {
        "status": "PROMISING_HISTORICAL_ENTRY_CANDIDATE_WITH_SOURCE_STABILITY_CAVEAT",
        "production_approved": False,
        "holdout_consumed": True,
        "fresh_holdout_status": "PENDING_20_FULL_SESSIONS_STARTING_2026_08_06",
        "sl_target_pnl_status": "DEFERRED",
        "source_cache": str(cache),
        "source_cache_sha256": sha256_file(cache),
        "generator_script": str(Path(__file__).resolve()),
        "generator_script_sha256": sha256_file(Path(__file__).resolve()),
        "data_contract": data_contract,
        "runtime": runtime,
        "frozen_rule": selected_rule,
        "frozen_rule_text": canonical_rule_text(selected_rule),
        "candidate_provenance": candidate_provenance,
        "freeze_sha256": freeze_record["freeze_sha256"],
        "frozen_at_ist": freeze_record["frozen_at_ist"],
        "daily_cap": DAILY_CAP,
        "strategy_metrics": strategy_by_split,
        "segment_role_map": {
            "TRAIN": "DISCOVERY_AND_INTERNAL_WALK_FORWARD",
            "VALIDATION": "SELECTION_VALIDATION_20_SHORTLISTED_RULES_CHECKED",
            "HISTORICAL_CONFIRMATION": "ALREADY_CONSUMED_POST_SELECTION_DEVELOPMENT_EVIDENCE",
        },
        "cross_tf_sensitivity": cross_tf,
        "five_minute_label_sensitivity": five_minute_label,
        "posthoc_sensitivity_historical_confirmation_excluding_aug3_aug4": clean_metric,
        "posthoc_quarantined_sensitivity_days": quarantine_days,
        "posthoc_quarantine_not_used_for_primary_metrics_or_selection": True,
        "monthly_stability": monthly.to_dict(orient="records"),
        "label_source_sensitivity": source.to_dict(orient="records"),
        "promotion_gates": gates,
        "fresh_holdout_gate_contract": fresh_gate_contract,
        "core_historical_aggregate_gates_pass": all(gates[name] for name in core_gate_names),
        "historical_stability_gates_pass": bool(gates["source_cohort_stability"]),
        "all_historical_entry_gates_pass": all(gates[name] for name in core_gate_names)
        and bool(gates["source_cohort_stability"]),
        "frequency_objective": "SPARSE_HIGH_CONVICTION_MEDIAN_3_TO_8_PER_ACTIVE_DAY;_NOT_10_TO_15_PER_DAY",
        "production_blockers": [
            "no fresh post-freeze 20-full-session holdout starting 2026-08-06",
            "SL/target/cost/slippage/P&L not designed or validated",
            "Jul-Aug evidence was already consumed during research",
        ],
        "selected_entries": len(enriched),
        "selected_movers": int(enriched["hit_5pct"].sum()),
        "extended_outcome_complete": int(enriched["extended_outcome_complete"].fillna(False).sum()),
    }
    write_json(out / "summary.json", summary)
    write_config(out / "prefilter_long_ml_entry_conf.py", selected_rule, strategy_by_split)
    build_report(out / "RESEARCH_REPORT.md", summary, selected_rule, all_model_metrics)
    portable_models = {
        name: {
            "imputer": fitted.imputer,
            "estimator": fitted.estimator,
            "calibrator": fitted.calibrator,
            "scaler": fitted.scaler,
        }
        for name, fitted in fitted_models.items()
    }
    joblib.dump(
        {
            "schema": MODEL_SCHEMA,
            "features": MODEL_FEATURES,
            "models": portable_models,
            "rule": selected_rule,
        },
        out / "discovery_models.joblib",
    )

    output_files = sorted(path for path in out.iterdir() if path.is_file() and path.name != "integrity_manifest.json")
    manifest = {
        "schema": MODEL_SCHEMA,
        "source_cache_schema": SOURCE_SCHEMA,
        "files": {
            path.name: {"bytes": path.stat().st_size, "sha256": sha256_file(path)}
            for path in output_files
        },
        "checks": {
            "leakage_audit_passed": leakage["passed"],
            "selected_rows_match_summary": len(enriched) == summary["selected_entries"],
            "mover_rows_match_summary": int(enriched["hit_5pct"].sum()) == summary["selected_movers"],
            "one_entry_per_ticker_day": not enriched.duplicated(["trade_date", "ticker"]).any(),
            "daily_cap_respected": int(enriched.groupby("trade_date").size().max()) <= DAILY_CAP,
            "config_production_disabled": "PRODUCTION_APPROVED = False" in (out / "prefilter_long_ml_entry_conf.py").read_text(encoding="utf-8"),
            "fresh_holdout_pending": not gates["fresh_20_session_holdout"],
            "generator_hashes_match_freeze_summary_and_current": (
                freeze_record["generator_script_sha256"]
                == summary["generator_script_sha256"]
                == sha256_file(Path(__file__).resolve())
            ),
            "portable_model_bundle_plain_components": all(
                set(bundle) == {"imputer", "estimator", "calibrator", "scaler"}
                for bundle in portable_models.values()
            ),
        },
    }
    manifest["passed"] = all(manifest["checks"].values())
    write_json(out / "integrity_manifest.json", manifest)
    print(json.dumps(jsonable(summary), indent=2, sort_keys=True))
    print(f"OUTPUT_DIR={out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
