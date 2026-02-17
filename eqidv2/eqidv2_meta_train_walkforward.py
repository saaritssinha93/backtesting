# -*- coding: utf-8 -*-
"""
eqidv2_meta_train_walkforward.py  (Strategy v1)
=================================================

Train and export a meta-label model that predicts p_win for candidate trades.
Designed to plug directly into ml_meta_filter.MetaLabelFilter.

Strategy v1 changes:
- Supports v1 expanded feature set (30 features) OR legacy 5-feature fallback
- Tries LightGBM first, falls back to LogisticRegression
- Profit-based threshold optimization on OOF predictions
- Walk-forward with purging + embargo

Outputs (default)
-----------------
- models/meta_model.pkl        (pickled model with predict_proba)
- models/meta_features.json    (list of feature names)
- outputs/meta_train_report.json
- outputs/meta_oof_predictions.csv

Notes
-----
- Purging: remove training samples whose event end time (t1) overlaps test start.
- Embargo: safety buffer before test_start.
"""
from __future__ import annotations

import argparse
import json
import pickle
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import Dict, List, Tuple, Optional

import numpy as np
import pandas as pd

from sklearn.linear_model import LogisticRegression
from sklearn.calibration import CalibratedClassifierCV
from sklearn.metrics import roc_auc_score, log_loss, brier_score_loss

from ml_meta_filter import ALL_FEATURES


IST_TZ = "Asia/Kolkata"


def _to_dt(x) -> pd.Timestamp:
    ts = pd.to_datetime(x, errors="coerce")
    if pd.isna(ts):
        return pd.NaT
    if ts.tzinfo is None:
        ts = ts.tz_localize(IST_TZ)
    else:
        ts = ts.tz_convert(IST_TZ)
    return ts


def _build_X(df: pd.DataFrame, feature_cols: List[str]) -> pd.DataFrame:
    """
    Build feature matrix from dataset columns.
    Supports both v1 (30-feature) and legacy (5-feature) modes.
    """
    return df[feature_cols].apply(pd.to_numeric, errors="coerce").fillna(0.0)


def _detect_feature_mode(df: pd.DataFrame) -> Tuple[str, List[str]]:
    """
    Auto-detect whether dataset has v1 expanded features or legacy features.
    Returns (mode, feature_cols).
    """
    v1_cols = [c for c in ALL_FEATURES if c in df.columns]
    if len(v1_cols) >= 15:
        return "v1", v1_cols

    # Legacy mode: derive features from raw columns
    legacy_required = {"quality_score", "atr_pct", "rsi", "adx", "side"}
    if legacy_required.issubset(set(df.columns)):
        # Build derived features inline
        df["_quality_score"] = pd.to_numeric(df["quality_score"], errors="coerce").fillna(0.0)
        df["_atr_pct"] = pd.to_numeric(df["atr_pct"], errors="coerce").fillna(0.0)
        df["_rsi_centered"] = (pd.to_numeric(df["rsi"], errors="coerce").fillna(50.0) - 50.0) / 50.0
        df["_adx_norm"] = pd.to_numeric(df["adx"], errors="coerce").fillna(20.0) / 50.0
        df["_side"] = np.where(df["side"].astype(str).str.upper().eq("LONG"), 1.0, -1.0)
        return "legacy", ["_quality_score", "_atr_pct", "_rsi_centered", "_adx_norm", "_side"]

    raise ValueError(f"Dataset has neither v1 features nor legacy features. Columns: {list(df.columns)}")


def _build_y(df: pd.DataFrame, label_col: str = "label") -> np.ndarray:
    return pd.to_numeric(df[label_col], errors="coerce").fillna(0).astype(int).to_numpy()


@dataclass
class FoldResult:
    fold: int
    train_start: str
    train_end: str
    test_start: str
    test_end: str
    rows_train: int
    rows_test: int
    auc: float
    logloss: float
    brier: float
    pos_rate_test: float
    model_type: str = "logistic"


def _walkforward_splits_by_days(
    df: pd.DataFrame,
    train_days: int,
    test_days: int,
) -> List[Tuple[pd.Timestamp, pd.Timestamp, pd.Timestamp, pd.Timestamp]]:
    """
    Returns list of (train_start_dt, train_end_dt, test_start_dt, test_end_dt).
    """
    days = pd.to_datetime(df["_entry_time"].dt.date).dropna().unique()
    days = pd.to_datetime(sorted(days))
    splits = []
    for i in range(0, len(days) - (train_days + test_days) + 1, test_days):
        train_start_day = days[i]
        train_end_day = days[i + train_days - 1]
        test_start_day = days[i + train_days]
        test_end_day = days[i + train_days + test_days - 1]
        train_start_dt = pd.Timestamp(train_start_day).tz_localize(IST_TZ)
        train_end_dt = (pd.Timestamp(train_end_day) + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)).tz_localize(IST_TZ)
        test_start_dt = pd.Timestamp(test_start_day).tz_localize(IST_TZ)
        test_end_dt = (pd.Timestamp(test_end_day) + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)).tz_localize(IST_TZ)
        splits.append((train_start_dt, train_end_dt, test_start_dt, test_end_dt))
    return splits


def _purge_train(
    train_df: pd.DataFrame,
    test_start_dt: pd.Timestamp,
    embargo_minutes: int,
) -> pd.DataFrame:
    """Purge samples whose event end time overlaps test start."""
    emb = pd.Timedelta(minutes=max(0, int(embargo_minutes)))
    cutoff = test_start_dt - emb
    return train_df[train_df["_t1"] < cutoff].copy()


def _try_lightgbm(X_train, y_train) -> Optional[object]:
    """Try to fit LightGBM. Returns model or None if unavailable."""
    try:
        import lightgbm as lgb
        clf = lgb.LGBMClassifier(
            n_estimators=300,
            max_depth=5,
            learning_rate=0.05,
            num_leaves=31,
            min_child_samples=20,
            class_weight="balanced",
            subsample=0.8,
            colsample_bytree=0.8,
            reg_alpha=0.1,
            reg_lambda=0.1,
            verbose=-1,
            n_jobs=-1,
        )
        clf.fit(X_train, y_train)
        return clf
    except ImportError:
        return None
    except Exception:
        return None


def _fit_model(
    X_train: pd.DataFrame,
    y_train: np.ndarray,
    X_cal: Optional[pd.DataFrame],
    y_cal: Optional[np.ndarray],
    calibration: str,
    use_lgb: bool = True,
) -> Tuple[object, str]:
    """Fit model. Try LightGBM first if use_lgb=True, fallback to Logistic."""
    model = None
    model_type = "logistic"

    if use_lgb:
        model = _try_lightgbm(X_train, y_train)
        if model is not None:
            model_type = "lightgbm"

    if model is None:
        model = LogisticRegression(max_iter=2000, class_weight="balanced")
        model.fit(X_train, y_train)
        model_type = "logistic"

    if calibration.lower() == "none" or X_cal is None or y_cal is None or len(X_cal) < 50:
        return model, model_type

    method = "isotonic" if calibration.lower() == "isotonic" else "sigmoid"
    cal = CalibratedClassifierCV(model, method=method, cv="prefit")
    cal.fit(X_cal, y_cal)
    return cal, model_type


def _optimize_threshold(y_true: np.ndarray, p_win: np.ndarray, r_net: Optional[np.ndarray] = None) -> Tuple[float, Dict]:
    """
    Find optimal p_win threshold by maximizing expected profit (R_net-weighted)
    or accuracy if R_net is not available.
    """
    best_thr = 0.55
    best_metric = -1e9
    results = {}

    thresholds = np.arange(0.50, 0.80, 0.01)
    for thr in thresholds:
        taken = p_win >= thr
        if taken.sum() < 10:
            continue

        if r_net is not None:
            # Profit-based: mean R_net of taken trades
            metric = float(r_net[taken].mean())
        else:
            # Accuracy-based: label=1 rate among taken trades
            metric = float(y_true[taken].mean())

        results[f"{thr:.2f}"] = {
            "metric": round(metric, 4),
            "trades_taken": int(taken.sum()),
            "take_rate": round(float(taken.mean()), 4),
        }

        if metric > best_metric:
            best_metric = metric
            best_thr = float(thr)

    return round(best_thr, 2), results


def train_walkforward(
    dataset_csv: Path,
    out_model_path: Path,
    out_features_path: Path,
    out_report_json: Path,
    out_oof_csv: Path,
    train_days: int,
    test_days: int,
    embargo_minutes: int,
    calibration: str,
    label_col: str = "label",
    use_lgb: bool = True,
) -> dict:
    df = pd.read_csv(dataset_csv)
    if df.empty:
        raise ValueError("Dataset is empty")

    # Detect feature mode
    mode, feature_cols = _detect_feature_mode(df)
    print(f"[INFO] Feature mode: {mode}, features: {len(feature_cols)}")

    # Require time columns
    for req_col in ["entry_time", label_col]:
        if req_col not in df.columns:
            raise ValueError(f"Dataset missing required column: {req_col}")

    # t1 may be missing in v1 datasets; use entry_time + horizon as fallback
    if "t1" not in df.columns:
        df["t1"] = df["entry_time"]

    df["_entry_time"] = df["entry_time"].apply(_to_dt)
    df["_t1"] = df["t1"].apply(_to_dt)
    df = df.dropna(subset=["_entry_time"]).sort_values("_entry_time").reset_index(drop=True)
    # Fill missing _t1 with _entry_time + 60 min (conservative)
    df["_t1"] = df["_t1"].fillna(df["_entry_time"] + pd.Timedelta(minutes=60))

    splits = _walkforward_splits_by_days(df, train_days=train_days, test_days=test_days)
    if not splits:
        raise ValueError("Not enough days in dataset for the requested train/test windows")

    # OOF storage
    oof = df[["ticker", "side", "entry_time", label_col]].copy()
    oof["p_win"] = np.nan
    if "r_net" in df.columns:
        oof["r_net"] = pd.to_numeric(df["r_net"], errors="coerce").fillna(0.0)

    fold_results: List[FoldResult] = []
    last_model_type = "logistic"

    for k, (tr_start, tr_end, te_start, te_end) in enumerate(splits, start=1):
        train_df = df[(df["_entry_time"] >= tr_start) & (df["_entry_time"] <= tr_end)].copy()
        test_df = df[(df["_entry_time"] >= te_start) & (df["_entry_time"] <= te_end)].copy()
        if train_df.empty or test_df.empty:
            continue

        train_df = _purge_train(train_df, te_start, embargo_minutes=embargo_minutes)
        if len(train_df) < 100 or len(test_df) < 20:
            continue

        # calibration split: last 20% of train by time
        train_df = train_df.sort_values("_entry_time")
        split_idx = int(len(train_df) * 0.8)
        train_main = train_df.iloc[:split_idx].copy()
        train_cal = train_df.iloc[split_idx:].copy()

        X_train = _build_X(train_main, feature_cols)
        y_train = _build_y(train_main, label_col=label_col)
        X_cal = _build_X(train_cal, feature_cols) if not train_cal.empty else None
        y_cal = _build_y(train_cal, label_col=label_col) if not train_cal.empty else None

        model, model_type = _fit_model(X_train, y_train, X_cal, y_cal, calibration=calibration, use_lgb=use_lgb)
        last_model_type = model_type

        X_test = _build_X(test_df, feature_cols)
        y_test = _build_y(test_df, label_col=label_col)
        p = model.predict_proba(X_test)[:, 1]
        p = np.clip(p, 0.01, 0.99)

        auc = float(roc_auc_score(y_test, p)) if len(np.unique(y_test)) > 1 else float("nan")
        ll = float(log_loss(y_test, p, labels=[0, 1]))
        brier = float(brier_score_loss(y_test, p))

        fold_results.append(FoldResult(
            fold=k,
            train_start=str(tr_start),
            train_end=str(tr_end),
            test_start=str(te_start),
            test_end=str(te_end),
            rows_train=int(len(train_df)),
            rows_test=int(len(test_df)),
            auc=auc,
            logloss=ll,
            brier=brier,
            pos_rate_test=float(np.mean(y_test)),
            model_type=model_type,
        ))

        test_idx = test_df.index
        oof.loc[test_idx, "p_win"] = p

    # Fit final model on all data
    last_test_start = splits[-1][2]
    final_train = _purge_train(df.copy(), last_test_start, embargo_minutes=embargo_minutes)
    final_train = final_train.sort_values("_entry_time")
    split_idx = int(len(final_train) * 0.8)
    train_main = final_train.iloc[:split_idx].copy()
    train_cal = final_train.iloc[split_idx:].copy()

    final_model, final_model_type = _fit_model(
        _build_X(train_main, feature_cols),
        _build_y(train_main, label_col=label_col),
        _build_X(train_cal, feature_cols) if not train_cal.empty else None,
        _build_y(train_cal, label_col=label_col) if not train_cal.empty else None,
        calibration=calibration,
        use_lgb=use_lgb,
    )

    # Export model + features
    out_model_path.parent.mkdir(parents=True, exist_ok=True)
    out_features_path.parent.mkdir(parents=True, exist_ok=True)
    out_model_path.write_bytes(pickle.dumps(final_model))

    # Export feature names (clean up legacy prefix)
    export_feats = [c.lstrip("_") if c.startswith("_") else c for c in feature_cols]
    if mode == "legacy":
        export_feats = ["quality_score", "atr_pct", "rsi_centered", "adx_norm", "side"]
    out_features_path.write_text(json.dumps(export_feats, indent=2), encoding="utf-8")

    # Profit-based threshold optimization on OOF
    oof_valid = oof.dropna(subset=["p_win"])
    r_net_arr = oof_valid["r_net"].to_numpy() if "r_net" in oof_valid.columns else None
    y_oof = _build_y(oof_valid, label_col=label_col) if label_col in oof_valid.columns else None

    opt_threshold = 0.62
    threshold_details = {}
    if y_oof is not None and len(oof_valid) >= 50:
        opt_threshold, threshold_details = _optimize_threshold(
            y_oof, oof_valid["p_win"].to_numpy(), r_net_arr
        )

    # Reports
    out_report_json.parent.mkdir(parents=True, exist_ok=True)
    out_oof_csv.parent.mkdir(parents=True, exist_ok=True)

    fold_dicts = [fr.__dict__ for fr in fold_results]
    report = {
        "dataset_csv": str(dataset_csv),
        "rows": int(len(df)),
        "pos_rate": float(df[label_col].mean()),
        "feature_mode": mode,
        "feature_count": len(feature_cols),
        "model_type": final_model_type,
        "splits": int(len(fold_results)),
        "train_days": int(train_days),
        "test_days": int(test_days),
        "embargo_minutes": int(embargo_minutes),
        "calibration": str(calibration),
        "optimal_threshold": opt_threshold,
        "threshold_analysis": threshold_details,
        "folds": fold_dicts,
        "export": {
            "model_path": str(out_model_path),
            "features_path": str(out_features_path),
            "features": export_feats,
        },
        "oof_coverage": float(oof["p_win"].notna().mean()),
    }
    out_report_json.write_text(json.dumps(report, indent=2), encoding="utf-8")
    oof.to_csv(out_oof_csv, index=False)

    return report


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dataset-csv", default="eqidv2/datasets/meta_dataset.csv")
    ap.add_argument("--out-model", default="models/meta_model.pkl")
    ap.add_argument("--out-features", default="models/meta_features.json")
    ap.add_argument("--out-report", default="outputs/meta_train_report.json")
    ap.add_argument("--out-oof", default="outputs/meta_oof_predictions.csv")
    ap.add_argument("--train-days", type=int, default=60)
    ap.add_argument("--test-days", type=int, default=10)
    ap.add_argument("--embargo-minutes", type=int, default=30)
    ap.add_argument("--calibration", type=str, default="isotonic", choices=["none", "sigmoid", "isotonic"])
    ap.add_argument("--no-lgb", action="store_true", help="Disable LightGBM, use LogisticRegression only")
    args = ap.parse_args()

    report = train_walkforward(
        dataset_csv=Path(args.dataset_csv),
        out_model_path=Path(args.out_model),
        out_features_path=Path(args.out_features),
        out_report_json=Path(args.out_report),
        out_oof_csv=Path(args.out_oof),
        train_days=int(args.train_days),
        test_days=int(args.test_days),
        embargo_minutes=int(args.embargo_minutes),
        calibration=str(args.calibration),
        use_lgb=not args.no_lgb,
    )
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
