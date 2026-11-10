"""Predeclared retrained feature-group diagnostics; no portfolio selections.

Refits the primary model families on identical matured prior-day labels after
dropping one feature group. Saved full-model predictions are the paired
reference. This module does not change any frozen strategy/model definition.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd

from g3_v18_20261102.run import dump, guard_writes, sha
from . import model as primary

PACKAGE = Path(__file__).resolve().parent
PROTOCOL = PACKAGE / "group_ablation_protocol.json"


def _deltas(dropped, full):
    return {
        "delta_log_loss_drop_minus_full": dropped["log_loss"] - full["log_loss"],
        "delta_brier_drop_minus_full": dropped["brier"] - full["brier"],
        "delta_auc_drop_minus_full": dropped["roc_auc"] - full["roc_auc"],
    }


def run(results: Path):
    results = results.resolve()
    if not results.is_relative_to(PACKAGE):
        raise ValueError("Lab results only")
    output = results / "models" / "group_ablation"
    if output.exists():
        raise FileExistsError(output)
    declaration = PROTOCOL.read_bytes()
    protocol = json.loads(declaration)
    config = json.loads((results / "config.json").read_text())
    cfg = config["protocol"]["model_validation"]
    if json.loads(primary.PROTOCOL.read_bytes()) != config["protocol"]:
        raise ValueError("Primary protocol changed since frozen rule runs")
    if sha(Path(primary.__file__)) != config["code_hashes"].get("model.py"):
        raise ValueError("Primary model code differs from frozen rule runs")
    if protocol["models"] != list(primary.MODELS) or set(protocol["drop_groups"]) != set(primary.FEATURE_GROUPS):
        raise ValueError("Declared model/group inventory differs")
    model_dir = results / "models"
    if not (model_dir / "complete.json").is_file():
        raise ValueError("Primary model allocation must complete before diagnostics")
    paths = {
        "config": results / "config.json",
        "model_pool": results / "model_pool.parquet",
        "primary_predictions": model_dir / "predictions.parquet",
        "primary_folds": model_dir / "model_folds.csv",
        "primary_diagnostics": model_dir / "model_diagnostics.json",
    }
    source_hashes = {name: sha(path) for name, path in paths.items()}
    diagnostics = json.loads(paths["primary_diagnostics"].read_text())
    if diagnostics["model_validation"] != cfg:
        raise ValueError("Primary fitted hyperparameters differ from frozen configuration")
    work = pd.read_parquet(paths["model_pool"]).reset_index(drop=True)
    work["day"] = work.day.astype(str).str[:10]
    predictions = pd.read_parquet(paths["primary_predictions"])
    folds = pd.read_csv(paths["primary_folds"])
    if work.sid.isna().any() or work.sid.duplicated().any() or predictions.sid.duplicated().any():
        raise ValueError("Nonunique candidate identities")
    if set(work.sid) != set(predictions.sid):
        raise ValueError("Full-model prediction pool differs")
    saved = predictions.set_index("sid").reindex(work.sid)
    saved.index = work.index
    if not saved.day.astype(str).str[:10].eq(work.day).all():
        raise ValueError("Full-model prediction day differs")
    calendar = config["days"]
    if calendar != sorted(set(calendar)) or not set(work.day).issubset(calendar):
        raise ValueError("Invalid frozen calendar")
    block_count = len(range(cfg["initial_train_days"], len(calendar), cfg["test_block_days"]))
    expected_blocks = list(range(block_count))
    if folds.block_id.tolist() != expected_blocks:
        raise ValueError("Primary folds differ from frozen block count")

    signal = pd.to_datetime(work.signal_ts, utc=True, errors="coerce")
    entry = pd.to_datetime(work.entry_ts, utc=True, errors="coerce")
    exit_time = pd.to_datetime(work.exit_ts, utc=True, errors="coerce")
    if signal.isna().any() or not signal.dt.tz_convert("Asia/Kolkata").dt.strftime("%Y-%m-%d").eq(work.day).all():
        raise ValueError("Invalid signal/day identity")
    filled = primary._filled(work)
    net = primary._num(work, "net_return_pct")
    valid = filled & np.isfinite(net) & entry.notna() & exit_time.notna() & entry.ge(signal) & exit_time.ge(entry)
    labels = net.gt(0).astype(int)
    matrix = primary.feature_matrix(work)

    guard_writes(PACKAGE)
    output.mkdir(parents=True)
    (output / "protocol.json").write_bytes(declaration)
    record = dict(scope=protocol["scope"], execution_authority=False,
        protocol_sha256=hashlib.sha256(declaration).hexdigest(),
        source_hashes=source_hashes, diagnostic_code_sha256=sha(Path(__file__)),
        primary_code_sha256=sha(Path(primary.__file__)), diagnostic_feature_subsets=8,
        portfolio_strategy_arms_added=0, attempted_refits=0, successful_refits=0,
        skipped_nonready_folds=int(folds.status.ne("READY").sum()),
        declared_before_primary_model_results=protocol["declared_before_primary_model_results"])
    dump(output / "run_started.json", record)
    rows, calibration, scores, fits = [], [], [], []
    for fold in folds.itertuples():
        if fold.status != "READY":
            continue
        test_days = [d for d in calendar if fold.test_first_day <= d <= fold.test_last_day]
        train_days = [d for d in calendar if d <= fold.train_last_day]
        test = work.day.isin(test_days)
        first_test = signal.loc[test].min()
        train = work.day.isin(train_days) & valid & exit_time.lt(first_test)
        labeled = test & valid
        y_train = labels.loc[train]
        y_test = labels.loc[labeled]
        counts = y_train.value_counts()
        checks = (
            int(train.sum()) == int(fold.train_filled_labels),
            int(counts.get(0, 0)) == int(fold.train_negative_labels),
            int(counts.get(1, 0)) == int(fold.train_positive_labels),
            int(labeled.sum()) == int(fold.test_filled_labels),
            int(test.sum()) == int(fold.test_candidates),
            len(train_days) == int(fold.train_sessions),
            len(test_days) == int(fold.test_sessions),
            bool(saved.loc[test, "model_ready"].eq(True).all()),
        )
        if not all(checks) or not exit_time.loc[train].max() < first_test:
            raise ValueError(f"Training/evaluation cohort differs in fold {fold.block_id}")
        base_rate = float(y_train.mean())
        if not np.isclose(base_rate, fold.training_base_rate, atol=1e-12):
            raise ValueError("Training base rate differs")
        for model_name in protocol["models"]:
            full_scores = saved.loc[labeled, model_name].to_numpy(float)
            if not np.isfinite(saved.loc[test, model_name]).all():
                raise ValueError("Missing full-model prediction in READY fold")
            full = primary._metrics(y_test, full_scores, base_rate)
            for group in protocol["drop_groups"]:
                kept = [name for name in primary.FEATURES if name not in primary.FEATURE_GROUPS[group]]
                record["attempted_refits"] += 1
                fit = dict(model=model_name, dropped_group=group, block_id=int(fold.block_id),
                    train_first_day=fold.train_first_day, train_last_day=fold.train_last_day,
                    test_first_day=fold.test_first_day, test_last_day=fold.test_last_day,
                    train_labels=len(y_train), test_candidates=int(test.sum()), test_filled_labels=len(y_test),
                    kept_features=kept, dropped_features=list(primary.FEATURE_GROUPS[group]), status="ATTEMPTED")
                fits.append(fit)
                # Fixed primary pipeline; only the declared input group changes.
                dump(output / "fit_progress.json", dict(**record, fits=fits))
                try:
                    fitted = primary._pipeline(model_name, cfg).fit(matrix.loc[train, kept], y_train)
                except Exception as exc:
                    fit.update(status="FAILED", exception_type=type(exc).__name__, error=str(exc))
                    dump(output / "fit_progress.json", dict(**record, fits=fits))
                    raise
                prediction = pd.Series(fitted.predict_proba(matrix.loc[test, kept])[:, 1], index=matrix.index[test])
                dropped_scores = prediction.loc[labeled[labeled].index].to_numpy(float)
                dropped = primary._metrics(y_test, dropped_scores, base_rate,
                    model=model_name, dropped_group=group, block_id=int(fold.block_id),
                    test_first_day=fold.test_first_day, test_last_day=fold.test_last_day)
                rows.append(dict(**dropped, full_log_loss=full["log_loss"], full_brier=full["brier"],
                    full_roc_auc=full["roc_auc"], **_deltas(dropped, full)))
                for arm, values in (("full", full_scores), ("drop", dropped_scores)):
                    calibration.extend(primary._calibration(y_test, values, model=model_name,
                        dropped_group=group, block_id=int(fold.block_id), feature_arm=arm))
                score_rows = work.loc[test, ["sid", "day"]].copy()
                score_rows["model"] = model_name
                score_rows["dropped_group"] = group
                score_rows["block_id"] = int(fold.block_id)
                score_rows["valid_filled_label"] = valid.loc[test].to_numpy()
                score_rows["label"] = labels.loc[test].where(valid.loc[test]).to_numpy()
                score_rows["full_score"] = saved.loc[test, model_name].to_numpy()
                score_rows["drop_score"] = prediction.to_numpy()
                score_rows["training_base_rate"] = base_rate
                scores.append(score_rows)
                record["successful_refits"] += 1
                fit["status"] = "SUCCESS"
                print(f"Diagnostic fold {fold.block_id} {model_name} drop={group}", flush=True)
        # Preserve attempt counts if a later diagnostic fails.
        dump(output / "fit_progress.json", dict(**record, fits=fits))

    all_scores = pd.concat(scores, ignore_index=True) if scores else pd.DataFrame(columns=[
        "sid", "day", "model", "dropped_group", "block_id", "valid_filled_label", "label",
        "full_score", "drop_score", "training_base_rate"])
    for (model_name, group), subset in all_scores.loc[all_scores.valid_filled_label.eq(True)].groupby(["model", "dropped_group"]):
        full = primary._metrics(subset.label.astype(int), subset.full_score, subset.training_base_rate)
        dropped = primary._metrics(subset.label.astype(int), subset.drop_score, subset.training_base_rate,
            model=model_name, dropped_group=group, block_id="ALL_READY",
            test_first_day=subset.day.min(), test_last_day=subset.day.max())
        rows.append(dict(**dropped, full_log_loss=full["log_loss"], full_brier=full["brier"],
            full_roc_auc=full["roc_auc"], **_deltas(dropped, full)))
        for arm in ("full", "drop"):
            calibration.extend(primary._calibration(subset.label.astype(int), subset[arm+"_score"],
                model=model_name, dropped_group=group, block_id="ALL_READY", feature_arm=arm))
    table = pd.DataFrame(rows)
    table.to_csv(output / "group_ablation_metrics.csv", index=False)
    pd.DataFrame(calibration).to_csv(output / "group_ablation_calibration.csv", index=False)
    all_scores.to_parquet(output / "predictions.parquet", index=False)
    dump(output / "fit_inventory.json", dict(**record, fits=fits))
    stability = []
    if len(table):
        fold_rows = table.loc[table.block_id.ne("ALL_READY")]
        for (model_name, group), part in fold_rows.groupby(["model", "dropped_group"]):
            stability.append(dict(model=model_name, dropped_group=group, ready_folds=len(part),
                drop_improved_log_loss_folds=int(part.delta_log_loss_drop_minus_full.lt(0).sum()),
                drop_worsened_log_loss_folds=int(part.delta_log_loss_drop_minus_full.gt(0).sum()),
                minimum_delta_log_loss=float(part.delta_log_loss_drop_minus_full.min()),
                maximum_delta_log_loss=float(part.delta_log_loss_drop_minus_full.max()),
                mean_delta_log_loss=float(part.delta_log_loss_drop_minus_full.mean())))
    pd.DataFrame(stability).to_csv(output / "fold_stability.csv", index=False)
    for name, path in paths.items():
        if sha(path) != source_hashes[name]:
            raise ValueError(f"Primary source changed during diagnostic: {name}")
    record.update(ready_folds=int(folds.status.eq("READY").sum()),
        expected_refits=int(folds.status.eq("READY").sum()) * protocol["diagnostic_feature_subsets"],
        assessment="PREDICTION_CONDITIONAL_ON_FILL_NOT_PORTFOLIO_PNL",
        smaller_log_loss_or_brier_is_better=True, larger_auc_is_better=True,
        independent_strategy_holdout=False, primary_outputs_unchanged=True)
    if record["successful_refits"] != record["expected_refits"]:
        raise ValueError("Diagnostic fit inventory incomplete")
    dump(output / "complete.json", record)
    dump(output / "manifest.json", dict(files={p.name: sha(p) for p in output.iterdir() if p.is_file() and p.name != "manifest.json"}))
    return output


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--results", type=Path, required=True)
    args = parser.parse_args()
    print(run(args.results))
