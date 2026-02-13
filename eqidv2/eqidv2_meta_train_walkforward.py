# -*- coding: utf-8 -*-
"""
eqidv2_meta_train_walkforward.py
================================

Train and export a fast meta-label model that predicts p_win for candidate trades.
Designed to plug directly into your existing ml_meta_filter.MetaLabelFilter.

What it outputs (default)
-------------------------
- models/meta_model.pkl        (pickled sklearn model with predict_proba)
- models/meta_features.json    (list of feature names, matching MetaLabelFilter)
- outputs/meta_train_report.json
- outputs/meta_oof_predictions.csv

Key idea
--------
We do time-ordered walk-forward evaluation and then fit a final calibrated model.

This script expects a dataset CSV from eqidv2_meta_label_triple_barrier.py with:
  quality_score, atr_pct, rsi, adx, side, label, entry_time, t1

Notes
-----
- Purging: we remove training samples whose event end time (t1) overlaps the test start.
- Calibration: optional isotonic/sigmoid on the tail of the training window.
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


def _build_X(df: pd.DataFrame) -> pd.DataFrame:
    """
    Feature engineering MUST match ml_meta_filter.MetaLabelFilter.build_feature_row.

    MetaLabelFilter expects:
      - quality_score
      - atr_pct
      - rsi_centered
      - adx_norm
      - side
    """
    return pd.DataFrame({
        "quality_score": pd.to_numeric(df["quality_score"], errors="coerce").fillna(0.0),
        "atr_pct": pd.to_numeric(df["atr_pct"], errors="coerce").fillna(0.0),
        "rsi_centered": (pd.to_numeric(df["rsi"], errors="coerce").fillna(50.0) - 50.0) / 50.0,
        "adx_norm": pd.to_numeric(df["adx"], errors="coerce").fillna(20.0) / 50.0,
        "side": np.where(df["side"].astype(str).str.upper().eq("LONG"), 1.0, -1.0),
    })


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


def _walkforward_splits_by_days(
    df: pd.DataFrame,
    train_days: int,
    test_days: int,
) -> List[Tuple[pd.Timestamp, pd.Timestamp, pd.Timestamp, pd.Timestamp]]:
    """
    Returns list of (train_start_dt, train_end_dt, test_start_dt, test_end_dt).
    Splits are based on unique entry dates (IST).
    """
    days = pd.to_datetime(df["_entry_time"].dt.date).dropna().unique()
    days = pd.to_datetime(sorted(days))
    splits = []
    for i in range(0, len(days) - (train_days + test_days) + 1, test_days):
        train_start_day = days[i]
        train_end_day = days[i + train_days - 1]
        test_start_day = days[i + train_days]
        test_end_day = days[i + train_days + test_days - 1]
        # convert to full-day datetimes
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
    """
    Purge samples whose event end time overlaps test start.
    Embargo adds a safety buffer before test_start.
    """
    emb = pd.Timedelta(minutes=max(0, int(embargo_minutes)))
    cutoff = test_start_dt - emb
    # Drop train samples where t1 >= cutoff (overlap into embargo region / test region)
    return train_df[train_df["_t1"] < cutoff].copy()


def _fit_model(
    X_train: pd.DataFrame,
    y_train: np.ndarray,
    X_cal: Optional[pd.DataFrame],
    y_cal: Optional[np.ndarray],
    calibration: str,
) -> object:
    base = LogisticRegression(max_iter=2000, class_weight="balanced")
    base.fit(X_train, y_train)

    if calibration.lower() == "none" or X_cal is None or y_cal is None or len(X_cal) < 50:
        return base

    method = "isotonic" if calibration.lower() == "isotonic" else "sigmoid"
    cal = CalibratedClassifierCV(base, method=method, cv="prefit")
    cal.fit(X_cal, y_cal)
    return cal


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
) -> dict:
    df = pd.read_csv(dataset_csv)
    if df.empty:
        raise ValueError("Dataset is empty")

    req = {"quality_score", "atr_pct", "rsi", "adx", "side", label_col, "entry_time", "t1"}
    miss = req - set(df.columns)
    if miss:
        raise ValueError(f"Dataset missing required columns: {sorted(miss)}")

    df["_entry_time"] = df["entry_time"].apply(_to_dt)
    df["_t1"] = df["t1"].apply(_to_dt)
    df = df.dropna(subset=["_entry_time", "_t1"]).sort_values("_entry_time").reset_index(drop=True)

    splits = _walkforward_splits_by_days(df, train_days=train_days, test_days=test_days)
    if not splits:
        raise ValueError("Not enough days in dataset for the requested train/test windows")

    # OOF storage
    oof = df[["ticker", "side", "entry_time", "label"]].copy()
    oof["p_win"] = np.nan

    fold_results: List[FoldResult] = []

    for k, (tr_start, tr_end, te_start, te_end) in enumerate(splits, start=1):
        train_df = df[(df["_entry_time"] >= tr_start) & (df["_entry_time"] <= tr_end)].copy()
        test_df = df[(df["_entry_time"] >= te_start) & (df["_entry_time"] <= te_end)].copy()
        if train_df.empty or test_df.empty:
            continue

        train_df = _purge_train(train_df, te_start, embargo_minutes=embargo_minutes)
        if len(train_df) < 200 or len(test_df) < 50:
            continue

        # calibration split: last 20% of train by time
        train_df = train_df.sort_values("_entry_time")
        split_idx = int(len(train_df) * 0.8)
        train_main = train_df.iloc[:split_idx].copy()
        train_cal = train_df.iloc[split_idx:].copy()

        X_train = _build_X(train_main)
        y_train = _build_y(train_main, label_col=label_col)
        X_cal = _build_X(train_cal) if not train_cal.empty else None
        y_cal = _build_y(train_cal, label_col=label_col) if not train_cal.empty else None

        model = _fit_model(X_train, y_train, X_cal, y_cal, calibration=calibration)

        X_test = _build_X(test_df)
        y_test = _build_y(test_df, label_col=label_col)
        p = model.predict_proba(X_test)[:, 1]
        p = np.clip(p, 0.01, 0.99)

        # metrics
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
        ))

        # write OOF preds for the test slice
        test_idx = test_df.index
        oof.loc[test_idx, "p_win"] = p

    # Fit final model on all data (purged relative to the last test window start)
    last_test_start = splits[-1][2]
    final_train = _purge_train(df.copy(), last_test_start, embargo_minutes=embargo_minutes)
    final_train = final_train.sort_values("_entry_time")
    split_idx = int(len(final_train) * 0.8)
    train_main = final_train.iloc[:split_idx].copy()
    train_cal = final_train.iloc[split_idx:].copy()

    final_model = _fit_model(
        _build_X(train_main),
        _build_y(train_main, label_col=label_col),
        _build_X(train_cal) if not train_cal.empty else None,
        _build_y(train_cal, label_col=label_col) if not train_cal.empty else None,
        calibration=calibration,
    )

    # export model + features
    out_model_path.parent.mkdir(parents=True, exist_ok=True)
    out_features_path.parent.mkdir(parents=True, exist_ok=True)
    out_model_path.write_bytes(pickle.dumps(final_model))
    feats = ["quality_score", "atr_pct", "rsi_centered", "adx_norm", "side"]
    out_features_path.write_text(json.dumps(feats, indent=2), encoding="utf-8")

    # reports
    out_report_json.parent.mkdir(parents=True, exist_ok=True)
    out_oof_csv.parent.mkdir(parents=True, exist_ok=True)

    fold_dicts = [fr.__dict__ for fr in fold_results]
    report = {
        "dataset_csv": str(dataset_csv),
        "rows": int(len(df)),
        "pos_rate": float(df[label_col].mean()),
        "splits": int(len(fold_results)),
        "train_days": int(train_days),
        "test_days": int(test_days),
        "embargo_minutes": int(embargo_minutes),
        "calibration": str(calibration),
        "folds": fold_dicts,
        "export": {
            "model_path": str(out_model_path),
            "features_path": str(out_features_path),
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
    )
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
