# -*- coding: utf-8 -*-
"""ML meta-label filter/sizer for eqidv2 live + backtest usage."""
from __future__ import annotations

import json
import math
import pickle
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import numpy as np
import pandas as pd


@dataclass
class MetaFilterConfig:
    model_path: str = "eqidv2/models/meta_model.pkl"
    feature_path: str = "eqidv2/models/meta_features.json"
    pwin_threshold: float = 0.60
    min_mult: float = 0.8
    max_mult: float = 1.6


class MetaLabelFilter:
    """Fast inference wrapper with safe fallback heuristic when model is unavailable."""

    def __init__(self, cfg: Optional[MetaFilterConfig] = None):
        self.cfg = cfg or MetaFilterConfig()
        self.model = None
        self.features: Optional[List[str]] = None
        self._load_model_if_present()

    def _load_model_if_present(self) -> None:
        mp = Path(self.cfg.model_path)
        fp = Path(self.cfg.feature_path)

        if mp.exists() and fp.exists():
            try:
                self.model = pickle.loads(mp.read_bytes())
                self.features = json.loads(fp.read_text(encoding="utf-8"))
            except Exception:
                # If model load fails, fallback safely
                self.model = None
                self.features = None
        else:
            self.model = None
            self.features = None

    @staticmethod
    def _as_float(x: Any, default: float = 0.0) -> float:
        try:
            if x is None:
                return float(default)
            if isinstance(x, str) and x.strip() == "":
                return float(default)
            return float(x)
        except Exception:
            return float(default)

    def build_feature_row(self, signal: Dict[str, Any]) -> Dict[str, float]:
        """
        Keep feature engineering EXACTLY consistent between training and inference.

        Base set used by your pipeline:
          - quality_score
          - atr_pct
          - rsi_centered
          - adx_norm
          - side
        """
        quality_score = self._as_float(signal.get("quality_score", 0.0), 0.0)
        atr_pct = self._as_float(signal.get("atr_pct", signal.get("atr_pct_signal", 0.0)), 0.0)
        rsi = self._as_float(signal.get("rsi", signal.get("rsi_signal", 50.0)), 50.0)
        adx = self._as_float(signal.get("adx", signal.get("adx_signal", 20.0)), 20.0)
        side = str(signal.get("side", "LONG")).upper().strip()

        return {
            "quality_score": float(quality_score),
            "atr_pct": float(atr_pct),
            "rsi_centered": float((rsi - 50.0) / 50.0),
            "adx_norm": float(adx / 50.0),
            "side": 1.0 if side == "LONG" else -1.0,
        }

    def predict_pwin(self, signal: Dict[str, Any]) -> float:
        """
        Returns p_win in [0.01, 0.99].
        Uses trained model if present; otherwise uses a conservative heuristic.
        """
        # Use model if available
        if self.model is not None and self.features:
            try:
                row = self.build_feature_row(signal)
                X = pd.DataFrame([{f: row.get(f, 0.0) for f in self.features}])
                p = float(self.model.predict_proba(X)[:, 1][0])
                return float(max(0.01, min(0.99, p)))
            except Exception:
                pass  # fallback below

        # Heuristic fallback (stable, conservative)
        row = self.build_feature_row(signal)
        q = float(row["quality_score"])
        atr_pct = float(row["atr_pct"])
        rsi_c = float(row["rsi_centered"])
        adx_n = float(row["adx_norm"])

        # Simple score -> sigmoid probability
        # Higher quality, moderate ATR, stronger trend = better
        score = 0.0
        score += 1.8 * q
        score += 0.8 * adx_n
        score += -0.6 * max(0.0, atr_pct - 2.5) / 2.5  # penalize very high ATR%
        score += -0.5 * abs(rsi_c)                     # penalize extreme RSI

        p = 1.0 / (1.0 + math.exp(-score))
        return float(max(0.01, min(0.99, p)))

    def confidence_multiplier(self, p_win: float) -> float:
        """
        Hard gate: below threshold => 0.0 (skip trade)
        Above threshold => scale between [min_mult, max_mult]
        """
        p = float(p_win)
        thr = float(self.cfg.pwin_threshold)
        if p < thr:
            return 0.0
        # normalize to 0..1 above threshold
        x = (p - thr) / max(1e-9, (1.0 - thr))
        x = max(0.0, min(1.0, x))
        return float(self.cfg.min_mult + x * (self.cfg.max_mult - self.cfg.min_mult))


def train_simple_baseline(
    dataset_csv: str,
    out_model_path: str = "eqidv2/models/meta_model.pkl",
    out_features_path: str = "eqidv2/models/meta_features.json",
    label_col: str = "label",
) -> Dict[str, Any]:
    """
    Optional helper: trains a simple LogisticRegression on the standard features and exports it.
    This is NOT walk-forward; for proper WF training use eqidv2_meta_train_walkforward.py.
    """
    from sklearn.linear_model import LogisticRegression

    df = pd.read_csv(dataset_csv)
    req = {"quality_score", "atr_pct", "rsi", "adx", "side", label_col}
    miss = req - set(df.columns)
    if miss:
        raise ValueError(f"Dataset missing required columns: {sorted(miss)}")

    x = pd.DataFrame({
        "quality_score": pd.to_numeric(df["quality_score"], errors="coerce").fillna(0.0),
        "atr_pct": pd.to_numeric(df["atr_pct"], errors="coerce").fillna(0.0),
        "rsi_centered": (pd.to_numeric(df["rsi"], errors="coerce").fillna(50.0) - 50.0) / 50.0,
        "adx_norm": pd.to_numeric(df["adx"], errors="coerce").fillna(20.0) / 50.0,
        "side": np.where(df["side"].astype(str).str.upper().eq("LONG"), 1.0, -1.0),
    })
    y = pd.to_numeric(df[label_col], errors="coerce").fillna(0).astype(int)

    clf = LogisticRegression(max_iter=1000, class_weight="balanced")
    clf.fit(x, y)

    Path(out_model_path).parent.mkdir(parents=True, exist_ok=True)
    Path(out_model_path).write_bytes(pickle.dumps(clf))
    Path(out_features_path).write_text(json.dumps(list(x.columns), indent=2), encoding="utf-8")

    return {
        "rows": int(len(df)),
        "pos_rate": float(y.mean()) if len(y) else 0.0,
        "features": list(x.columns),
        "out_model_path": out_model_path,
        "out_features_path": out_features_path,
    }
