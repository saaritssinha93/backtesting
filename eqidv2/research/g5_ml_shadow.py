"""Score later G5 orders from a frozen numeric model; never submit orders."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd

from research.g5_ml_backtest import build_entry_features, score_saved_model


def score(model, orders, context_frame):
    contexts = {int(sid): group.drop(columns="sid").reset_index(drop=True)
                for sid, group in context_frame.groupby("sid", sort=False)}
    features = build_entry_features(orders, contexts)
    if features.day.le(model["training_end"]).any():
        raise ValueError("Shadow scoring requires sessions strictly after the model's training cutoff")
    prediction = score_saved_model(features, model)
    result = features[["sid", "day", "decision_ts", "feature_ts", "feature_valid", "feature_reason"]].copy()
    result["prediction_net_r"] = prediction
    result["suggested_size_multiplier"] = np.where(prediction.le(0), .5, 1.)
    result["suggested_skip"] = prediction.le(0)
    result["model_training_end"] = model["training_end"]
    result["suggested_exit_policy"] = model["next_block_exit_policy"]
    result["execution_authority"] = False
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--model", type=Path, required=True)
    parser.add_argument("--orders", type=Path, required=True)
    parser.add_argument("--contexts", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        raise ValueError("Require a fresh shadow output file")
    model = json.loads(args.model.read_text(encoding="utf-8"))
    result = score(model, pd.read_csv(args.orders), pd.read_parquet(args.contexts))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    result.to_csv(args.output, index=False)
    print(result.to_string(index=False))


if __name__ == "__main__":
    main()
