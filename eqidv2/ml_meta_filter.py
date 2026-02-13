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
        self.features: List[str] = []
        self._load()

    def _load(self) -> None:
        m = Path(self.cfg.model_path)
        f = Path(self.cfg.feature_path)
        if m.exists() and f.exists():
            try:
                self.model = pickle.loads(m.read_bytes())
                self.features = json.loads(f.read_text(encoding="utf-8"))
            except Exception:
                self.model = None
                self.features = []

    @staticmethod
    def _sigmoid(x: float) -> float:
        x = max(-20.0, min(20.0, x))
        return 1.0 / (1.0 + math.exp(-x))

    def build_feature_row(self, signal: Dict[str, Any]) -> Dict[str, float]:
        atr_pct = float(signal.get("atr_pct", 0.0) or 0.0)
        rsi = float(signal.get("rsi", 50.0) or 50.0)
        adx = float(signal.get("adx", 20.0) or 20.0)
        q = float(signal.get("quality_score", 0.0) or 0.0)
        side = 1.0 if str(signal.get("side", "")).upper() == "LONG" else -1.0
        return {
            "quality_score": q,
            "atr_pct": atr_pct,
            "rsi_centered": (rsi - 50.0) / 50.0,
            "adx_norm": adx / 50.0,
            "side": side,
        }

    def predict_pwin(self, signal: Dict[str, Any]) -> float:
        feats = self.build_feature_row(signal)
        if self.model is not None and self.features:
            try:
                row = pd.DataFrame([{k: feats.get(k, 0.0) for k in self.features}])
                p = self.model.predict_proba(row)[0][1]
                return float(max(0.01, min(0.99, p)))
            except Exception:
                pass

        z = (
            0.55 * feats["quality_score"]
            + 1.8 * feats["atr_pct"]
            + 0.6 * feats["adx_norm"]
            - 0.25 * abs(feats["rsi_centered"])
        )
        p = self._sigmoid(z - 1.0)
        return float(max(0.01, min(0.99, p)))

    def confidence_multiplier(self, pwin: float) -> float:
        th = self.cfg.pwin_threshold
        if pwin <= th:
            return 0.0
        span = max(1e-9, 0.95 - th)
        x = max(0.0, min(1.0, (pwin - th) / span))
        return float(self.cfg.min_mult + x * (self.cfg.max_mult - self.cfg.min_mult))


def train_logreg_meta_model(
    dataset_csv: str,
    out_model_path: str,
    out_features_path: str,
    label_col: str = "label",
) -> Dict[str, Any]:
    """Optional helper for quick meta-model training on candidate-trade datasets."""
    from sklearn.linear_model import LogisticRegression

    df = pd.read_csv(dataset_csv)
    cols = [
        "quality_score", "atr_pct", "rsi", "adx", "side",
    ]
    miss = [c for c in cols + [label_col] if c not in df.columns]
    if miss:
        raise ValueError(f"Missing columns in dataset: {miss}")

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
