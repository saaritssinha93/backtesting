"""Predeclared historical tuning of the G-3 observational pullback monitor.

The entry book and all trade actions remain frozen. Candidate selection uses
FIT20 and CHECK10 only; chosen rules are written and hashed before HELD16 is
scored. HELD16 is a held-back monitor evaluation, not untouched prospective
evidence: this history was already used for entry-strategy research.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import itertools
import json
import math
from pathlib import Path
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from research.g3_pullback_metrics import summarize


PROTOCOL = Path(__file__).with_name("g3_pullback_tuning_protocol_v1.json")
PRIMARY = {5: 0.30, 30: 0.50}
ORIGINAL = {
    "FAST": dict(family="FAST", horizon_minutes=5, threshold_pct=.30,
                 momentum=.5, body=.5, volume=1.2),
    "SLOW": dict(family="SLOW", horizon_minutes=30, threshold_pct=.50,
                 momentum=.5, giveback=1., trend="full"),
}


def sha(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def candidates() -> list[dict]:
    """Exactly 27 FAST and 18 SLOW rules, fixed before examining outcomes."""
    records = []
    for momentum, body, volume in itertools.product((.25, .5, .75), (.4, .5, .6), (1., 1.2, 1.5)):
        records.append(dict(family="FAST", horizon_minutes=5, threshold_pct=.3,
            momentum=momentum, body=body, volume=volume,
            candidate_id=f"FAST_m{momentum:.2f}_b{body:.2f}_v{volume:.2f}",
            distance_from_original=(abs((.25,.5,.75).index(momentum)-1) +
                                    abs((.4,.5,.6).index(body)-1) +
                                    abs((1.,1.2,1.5).index(volume)-1))))
    for momentum, giveback, trend in itertools.product((.25, .5, .75), (.5, 1., 1.5), ("full", "close_sma5")):
        records.append(dict(family="SLOW", horizon_minutes=30, threshold_pct=.5,
            momentum=momentum, giveback=giveback, trend=trend,
            candidate_id=f"SLOW_m{momentum:.2f}_g{giveback:.2f}_{trend}",
            distance_from_original=(abs((.25,.5,.75).index(momentum)-1) +
                                    abs((.5,1.,1.5).index(giveback)-1) +
                                    int(trend != "full"))))
    return records


def _boolean(series: pd.Series) -> pd.Series:
    if series.dtype == bool:
        return series.copy()
    if series.isna().any():
        raise ValueError(f"Null Boolean in {series.name}")
    normalized = series.astype(str).str.lower().map({"true": True, "false": False, "1": True, "0": False})
    if normalized.isna().any():
        raise ValueError(f"Invalid Boolean in {series.name}")
    return normalized.astype(bool)


def warning_rule(frame: pd.DataFrame, rule: dict) -> pd.Series:
    """Apply only contemporaneously available features, symmetrically by side."""
    if frame.empty:
        return pd.Series(False, index=frame.index, dtype=bool)
    if not frame.side.isin(["LONG", "SHORT"]).all():
        raise ValueError("Unknown side")
    if rule["family"] == "FAST":
        momentum = pd.to_numeric(frame.momentum3_atr, errors="raise")
        body = pd.to_numeric(frame.adverse_body_fraction, errors="raise")
        volume = pd.to_numeric(frame.volume_ratio20, errors="raise")
        return (momentum.le(-rule["momentum"]) &
                (_boolean(frame.micro_break) | (body.ge(rule["body"]) &
                 np.isfinite(volume) & volume.ge(rule["volume"])))).fillna(False)
    if rule["family"] != "SLOW":
        raise ValueError("Unknown rule family")
    momentum = pd.to_numeric(frame.momentum5_atr, errors="raise")
    giveback = pd.to_numeric(frame.giveback_atr, errors="raise")
    if rule["trend"] == "full":
        trend = _boolean(frame.trend_damage)
    elif rule["trend"] == "close_sma5":
        sign = frame.side.map({"LONG": 1, "SHORT": -1})
        trend = (sign * (pd.to_numeric(frame.decision_close, errors="raise") -
                         pd.to_numeric(frame.sma5, errors="raise"))).lt(0)
    else:
        raise ValueError("Unknown trend gate")
    return (momentum.le(-rule["momentum"]) & giveback.ge(rule["giveback"]) & trend).fillna(False)


def _ratio(numerator, denominator):
    return float(numerator / denominator) if denominator else math.nan


def training_score(frame: pd.DataFrame, warning: pd.Series) -> dict:
    available = _boolean(frame.outcome_available) if "outcome_available" in frame else pd.Series(True, index=frame.index)
    unknown_observations, unknown_warnings = int((~available).sum()), int((warning & ~available).sum())
    source_observations = len(frame)
    frame, warning = frame.loc[available], warning.loc[available]
    event = _boolean(frame.event)
    tp, fp = int((warning & event).sum()), int((warning & ~event).sum())
    fn, tn = int((~warning & event).sum()), int((~warning & ~event).sum())
    precision, recall = _ratio(tp, tp + fp), _ratio(tp, tp + fn)
    return dict(source_observations=source_observations, observations=len(frame), warnings=tp+fp, events=tp+fn,
        unknown_observations=unknown_observations, unknown_warnings=unknown_warnings,
        precision_identification_lower=_ratio(tp, tp+fp+unknown_warnings),
        precision_identification_upper=_ratio(tp+unknown_warnings, tp+fp+unknown_warnings),
        warned_trades=len(frame.loc[warning, ["day", "trade_id"]].drop_duplicates()),
        warned_days=int(frame.loc[warning, "day"].nunique()),
        tp=tp, fp=fp, fn=fn, tn=tn, precision=precision, recall=recall,
        prevalence=_ratio(tp+fn, len(frame)),
        f05=_ratio(1.25*tp, 1.25*tp + .25*fn + fp))


def select_rules(grid: pd.DataFrame, days: list[str]) -> tuple[dict, pd.DataFrame]:
    """Select using only first30 outcomes; no held-day feature evaluation."""
    if len(days) != 46 or len(set(days)) != 46 or days != sorted(days):
        raise ValueError("Expected 46 unique chronologically ordered study sessions")
    train = grid.loc[grid.day.isin(days[:30])].copy()
    folds = {"FIT20": days[:20], "CHECK10": days[20:30]}
    records, eligible_by_family = [], {"FAST": [], "SLOW": []}
    all_candidates = candidates()
    for rule in all_candidates:
        parameter_rows = train.loc[train.horizon_minutes.eq(rule["horizon_minutes"]) &
                                   train.threshold_pct.eq(rule["threshold_pct"])]
        scores = []
        for fold, fold_days in folds.items():
            part = parameter_rows.loc[parameter_rows.day.isin(fold_days)]
            score = training_score(part, warning_rule(part, rule))
            warnings_min, trades_min = (10, 5) if fold == "FIT20" else (5, 3)
            failures = []
            if score["warnings"] < warnings_min:
                failures.append("INSUFFICIENT_WARNINGS")
            if score["warned_trades"] < trades_min:
                failures.append("INSUFFICIENT_WARNED_TRADES")
            if not score["recall"] >= .20:
                failures.append("RECALL_BELOW_0.20_OR_UNDEFINED")
            if not score["precision"] > score["prevalence"]:
                failures.append("PRECISION_NOT_ABOVE_PREVALENCE")
            score.update(fold=fold, fold_eligible=not failures,
                         ineligibility_reasons=";".join(failures))
            records.append(dict(rule, **score))
            scores.append(score)
        if all(score["fold_eligible"] for score in scores):
            eligible_by_family[rule["family"]].append((min(score["f05"] for score in scores), rule))
    selected = {}
    for family, eligible in eligible_by_family.items():
        original = next(rule for rule in all_candidates if rule["family"] == family and rule["distance_from_original"] == 0)
        if eligible:
            objective, chosen = sorted(eligible, key=lambda item: (-item[0], item[1]["distance_from_original"], item[1]["candidate_id"]))[0]
            status = "ORIGINAL_RULE_SELECTED_ELIGIBLE" if chosen["distance_from_original"] == 0 else "CANDIDATE_SELECTED_FOR_HELD_EVALUATION"
        else:
            objective, chosen, status = None, original, "NO_ELIGIBLE_IMPROVEMENT_KEEP_ORIGINAL"
        selected[family] = dict(rule=chosen, selection_status=status,
                                minimum_training_f05=objective, eligible_candidates=len(eligible))
    checks = pd.DataFrame(records)
    chosen_ids = {entry["rule"]["candidate_id"] for entry in selected.values()}
    checks["selected"] = checks.candidate_id.isin(chosen_ids)
    return selected, checks


def apply_rules(frame: pd.DataFrame, rules: dict) -> pd.DataFrame:
    result = frame.copy()
    if "warning" in result:
        result["original_warning"] = _boolean(result.warning)
    result["warning"] = False
    for rule in rules.values():
        mask = result.horizon_minutes.eq(rule["horizon_minutes"])
        result.loc[mask, "warning"] = warning_rule(result.loc[mask], rule)
    if not result.horizon_minutes.isin(PRIMARY).all():
        raise ValueError("Unexpected forecast horizon")
    return result


def _read(path: Path) -> pd.DataFrame:
    data = pd.read_csv(path, dtype={"day": str, "trade_id": str})
    for column in ("warning", "event", "full_horizon_eligible", "censored", "baseline_warning",
                   "micro_break", "trend_damage", "outcome_available"):
        if column in data:
            data[column] = _boolean(data[column])
    return data


def episodes(dense: pd.DataFrame) -> pd.DataFrame:
    dense = dense.copy()
    dense["decision_ts"] = pd.to_datetime(dense.decision_ts, utc=True)
    picked = []
    for (_, horizon), group in dense.groupby(["trade_id", "horizon_minutes"], sort=False):
        next_allowed = None
        for row in group.sort_values("decision_ts").itertuples(index=False):
            if row.warning and (next_allowed is None or row.decision_ts >= next_allowed):
                picked.append(row._asdict())
                next_allowed = row.decision_ts + pd.Timedelta(minutes=int(horizon))
    return pd.DataFrame(picked, columns=dense.columns)


def episode_summary(alerts: pd.DataFrame, first_only=False) -> pd.DataFrame:
    data = alerts.sort_values("decision_ts")
    if first_only:
        data = data.drop_duplicates(["trade_id", "horizon_minutes"])
    rows = []
    for split, side, horizon in itertools.product(("ALL", "EARLIER30", "LATER16"), ("BOTH", "LONG", "SHORT"), PRIMARY):
        part = data.loc[data.horizon_minutes.eq(horizon)]
        if split != "ALL":
            part = part.loc[part.split.eq(split)]
        if side != "BOTH":
            part = part.loc[part.side.eq(side)]
        available = _boolean(part.outcome_available) if "outcome_available" in part else pd.Series(True, index=part.index)
        resolved = part.loc[available]
        hits = int(resolved.event.sum())
        unknown = int((~available).sum())
        rows.append(dict(split=split, side=side, horizon_minutes=horizon,
            threshold_pct=PRIMARY[horizon], alerts=len(part), successful_alerts=hits,
            resolved_alerts=len(resolved), unknown_alerts=unknown,
            false_alerts=len(resolved)-hits, precision=_ratio(hits, len(resolved)),
            precision_identification_lower=_ratio(hits, len(part)),
            precision_identification_upper=_ratio(hits+unknown, len(part)),
            warned_trades=part.trade_id.nunique(), warned_days=part.day.nunique(),
            censored=int(resolved.censored.sum()), median_lead_minutes=resolved.loc[resolved.event, "lead_minutes"].median()))
    return pd.DataFrame(rows)


def freeze_selection(out: Path, payload: dict) -> str:
    """Write immutable run-local rule choice before any held evaluation."""
    path = out / "selected_rules.json"
    with path.open("x", encoding="utf-8") as stream:
        json.dump(payload, stream, indent=2, sort_keys=True, allow_nan=False)
        stream.write("\n")
    digest = sha(path)
    with (out / "selected_rules.sha256").open("x", encoding="ascii") as stream:
        stream.write(digest + "  selected_rules.json\n")
    return digest


def _comparisons(metric: pd.DataFrame) -> pd.DataFrame:
    keys = ["analysis", "split", "side", "horizon_minutes", "threshold_pct", "phase"]
    original = metric.loc[metric.variant.eq("ORIGINAL")].drop(columns="variant")
    chosen = metric.loc[metric.variant.eq("CHOSEN")].drop(columns="variant")
    joined = original.merge(chosen, on=keys, suffixes=("_original", "_chosen"), validate="one_to_one")
    for name in ("observations", "tp", "fp", "fn", "tn", "precision", "recall", "fpr",
                 "specificity", "accuracy", "warning_rate", "lift", "f05", "unknown_observations",
                 "unknown_warnings", "precision_identification_lower", "precision_identification_upper"):
        joined[f"delta_{name}"] = joined[f"{name}_chosen"] - joined[f"{name}_original"]
    return joined


def run(study_dir: Path, output_dir: Path, bootstrap_reps=2000):
    if output_dir.exists():
        raise FileExistsError("Tuning output must be a new directory")
    protocol = json.loads(PROTOCOL.read_text(encoding="utf-8"))
    metadata = json.loads((study_dir / "provenance.json").read_text(encoding="utf-8"))
    days = sorted(str(day) for day in metadata["days"])
    grid_path, dense_path = study_dir / "evaluation_windows.csv", study_dir / "predictions_1m.csv"
    grid_hash, dense_hash = sha(grid_path), sha(dense_path)
    grid = _read(grid_path)
    if set(grid.day).difference(days):
        raise ValueError("Grid dates not in study calendar")
    selected, training_checks = select_rules(grid, days)
    output_dir.mkdir(parents=True, exist_ok=False)
    payload = dict(tuning_protocol=protocol, protocol_sha256=sha(PROTOCOL),
        tuning_source_sha256=sha(Path(__file__)), metrics_source_sha256=sha(Path(__file__).with_name("g3_pullback_metrics.py")),
        study_dir=str(study_dir.resolve()), input_sha256={"evaluation_windows.csv":grid_hash, "predictions_1m.csv":dense_hash},
        source_provenance_sha256=sha(study_dir / "provenance.json"),
        days=days, fit20=days[:20], check10=days[20:30], held16=days[30:],
        chosen=selected, original_rules=ORIGINAL,
        candidate_count=len(candidates()), tie_distance="Manhattan distance in ordered grid levels; changed trend gate counts one",
        frozen_at_utc=datetime.now(timezone.utc).isoformat(),
        held_outcomes_used_for_selection=False, trade_actions_changed=False,
        interpretation="HELD_BACK_HISTORICAL_MONITOR_EVALUATION_NOT_UNTOUCHED_PROSPECTIVE")
    chosen_hash = freeze_selection(output_dir, payload)
    print(f"Rule choice frozen before held evaluation: {chosen_hash}", flush=True)
    training_checks.to_csv(output_dir / "candidate_training_checks.csv", index=False)
    (output_dir / "protocol.json").write_text(json.dumps(protocol, indent=2), encoding="utf-8")
    chosen_rules = {family: entry["rule"] for family, entry in selected.items()}
    original_grid, tuned_grid = apply_rules(grid, ORIGINAL), apply_rules(grid, chosen_rules)
    if not original_grid.warning.equals(_boolean(grid.warning)):
        raise ValueError("Original rule does not reproduce recorded grid warnings")
    metrics = []
    for name, frame in (("ORIGINAL", original_grid), ("CHOSEN", tuned_grid)):
        result = summarize(frame, days, bootstrap_reps=bootstrap_reps, seed=20261008)
        result["variant"] = name
        denominator = 1.25 * result.tp + .25 * result.fn + result.fp
        result["f05"] = (1.25 * result.tp / denominator.where(denominator.ne(0)))
        metrics.append(result)
    metric = pd.concat(metrics, ignore_index=True)
    metric.to_csv(output_dir / "tuned_metrics.csv", index=False)
    _comparisons(metric).to_csv(output_dir / "comparisons.csv", index=False)
    tuned_grid.to_csv(output_dir / "tuned_evaluation_windows.csv", index=False)
    # Dense outcomes are first loaded after choice is frozen; they never tune rules.
    dense = _read(dense_path)
    if not all(dense.loc[dense.horizon_minutes.eq(h), "threshold_pct"].eq(t).all() for h, t in PRIMARY.items()):
        raise ValueError("Dense alert input must use primary thresholds only")
    dense_frames, episode_frames, alert_frames, first_frames = [], [], [], []
    for name, rules in (("ORIGINAL", ORIGINAL), ("CHOSEN", chosen_rules)):
        predictions = apply_rules(dense, rules)
        if name == "ORIGINAL" and not predictions.warning.equals(_boolean(dense.warning)):
            raise ValueError("Original rule does not reproduce dense warnings")
        alerts = episodes(predictions)
        predictions["variant"], alerts["variant"] = name, name
        dense_frames.append(predictions)
        episode_frames.append(alerts)
        alert_frames.append(episode_summary(alerts).assign(variant=name))
        first_frames.append(episode_summary(alerts, True).assign(variant=name))
    pd.concat(dense_frames, ignore_index=True).to_csv(output_dir / "tuned_predictions_1m.csv", index=False)
    pd.concat(episode_frames, ignore_index=True).to_csv(output_dir / "tuned_alert_episodes.csv", index=False)
    pd.concat(alert_frames, ignore_index=True).to_csv(output_dir / "tuned_alert_success.csv", index=False)
    pd.concat(first_frames, ignore_index=True).to_csv(output_dir / "tuned_first_alert_success.csv", index=False)
    if sha(grid_path) != grid_hash or sha(dense_path) != dense_hash:
        raise RuntimeError("Input artifacts changed during analysis")
    if sha(output_dir / "selected_rules.json") != chosen_hash:
        raise RuntimeError("Frozen rule choice changed during evaluation")
    validation = dict(status="PASS", selected_rules_sha256=chosen_hash, candidates=45,
        original_grid_warning_parity=True, original_dense_warning_parity=True,
        selection_uses_fit20_check10_only=True, rules_frozen_before_held_scoring=True,
        same_rule_both_sides=True, no_dense_grid_metric_mixing=True, trade_actions_changed=False,
        bootstrap_reps=bootstrap_reps, prospective_validation=False)
    (output_dir / "validation.json").write_text(json.dumps(validation, indent=2), encoding="utf-8")
    hashes = {path.name: sha(path) for path in output_dir.iterdir() if path.is_file()}
    (output_dir / "artifact_hashes.json").write_text(json.dumps(hashes, indent=2, sort_keys=True), encoding="utf-8")
    print(f"Completed historical tuning and held evaluation: {output_dir}", flush=True)
    return selected, metric


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--study-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--bootstrap-reps", type=int, default=2000)
    args = parser.parse_args()
    run(args.study_dir, args.output_dir, args.bootstrap_reps)
