"""Independently reconcile immutable G5 result files and model predictions."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd

from research import g5_ml_backtest as g5


def verify(directory):
    directory = Path(directory).resolve()
    manifest = json.loads((directory / "manifest.json").read_text(encoding="utf-8"))
    for name, digest in manifest["files"].items():
        if g5.base.sha256(directory / name) != digest:
            raise ValueError("Artifact hash mismatch: " + name)
    for name, digest in manifest["code_sha256"].items():
        if g5.base.sha256(Path(name)) != digest:
            raise ValueError("Current implementation differs from saved implementation: " + name)
    summary = json.loads((directory / "summary.json").read_text(encoding="utf-8"))
    config = json.loads((directory / "config.json").read_text(encoding="utf-8"))
    source = Path(summary["source_dataset"])
    if g5.base.sha256(source / "manifest.json") != summary["source_manifest_sha256"]:
        raise ValueError("Source manifest changed")
    original, _, _, _ = g5.dataset.load(source)
    comparison = pd.read_csv(directory / "comparison.csv")
    daily = pd.read_csv(directory / "daily_comparison.csv")
    features = pd.read_csv(directory / "features.csv").set_index("sid", drop=False)
    predictions = pd.read_csv(directory / "predictions.csv").set_index("sid", drop=False)
    if set(features.sid) != set(original.sid) or set(predictions.sid) != set(original.sid):
        raise ValueError("Incomplete feature or prediction SID coverage")
    original = original.set_index("sid", drop=False)
    if not pd.to_datetime(features.decision_ts, utc=True).equals(
            pd.to_datetime(original.reindex(features.index).confirmation_ts, utc=True).rename("decision_ts")):
        raise ValueError("Prediction timestamps differ from source confirmation")
    model_count = 0
    for fold in sorted(predictions.loc[predictions.fold.ge(0), "fold"].unique()):
        model = json.loads((directory / "models" / f"fold_{fold}.json").read_text(encoding="utf-8"))
        subset = predictions.loc[predictions.fold.eq(fold)]
        if not subset.day.gt(model["training_end"]).all():
            raise ValueError("Model trained on its evaluation period")
        reconstructed = g5.score_saved_model(features.loc[subset.index], model)
        np.testing.assert_allclose(reconstructed, subset.prediction_net_r, atol=1e-10,
                                   rtol=0, equal_nan=True)
        model_count += 1
    ledgers = 0
    for (stress, variant), rows in comparison.groupby(["stress", "variant"]):
        folder = directory / stress / variant
        book = g5.base.read_frozen_csv(folder / "portfolio_trades.csv")
        filled = book.filled.eq(True)
        expected_cost = np.where(filled, book.exposure_per_entry_rupees * book.cost_pct / 100, 0.)
        expected_pnl = np.where(filled, book.exposure_per_entry_rupees * book.gross_return_pct / 100 - expected_cost, 0.)
        np.testing.assert_allclose(book.cost_rupees, expected_cost, atol=1e-7, rtol=0)
        np.testing.assert_allclose(book.net_profit_rupees, expected_pnl, atol=1e-6, rtol=0)
        np.testing.assert_allclose(book.portfolio_net_profit_rupees,
            np.where(book.portfolio_executed, expected_pnl, 0.), atol=1e-6, rtol=0)
        if stress == "base_5bps" and variant == "control":
            g5.sealed.parity(book, original.reset_index(drop=True))
        for row in rows.itertuples():
            if row.period == "all_including_warmup":
                days = summary["days"]
            elif row.period == "evaluation":
                days = summary["evaluation_days"]
            else:
                days = ["2026-10-15" if row.period == "october_15" else "2026-10-16"]
            metrics = g5.bounded.stats(book, days)
            for key, value in metrics.items():
                if not np.isclose(getattr(row, key), value, atol=1e-6, rtol=0):
                    raise ValueError(f"Saved comparison mismatch: {stress}/{variant}/{row.period}/{key}")
        day_rows = daily.loc[daily.stress.eq(stress) & daily.variant.eq(variant)]
        net_by_day = book.groupby("day").portfolio_net_profit_rupees.sum().reindex(day_rows.day, fill_value=0.)
        np.testing.assert_allclose(net_by_day, day_rows.net_pnl_rs, atol=1e-6, rtol=0)
        curve = pd.read_csv(folder / "minute_equity.csv")
        daily_close = curve.groupby("day", sort=True).equity_pnl_rs.last()
        np.testing.assert_allclose(daily_close, book.groupby("day").portfolio_net_profit_rupees.sum().reindex(
            daily_close.index, fill_value=0.).cumsum(), atol=1e-6, rtol=0)
        if not config["ml_enabled"] and variant in (
                "ml_size", "ml_skip", "adaptive_exit", "combined_size", "combined_skip"):
            control = g5.base.read_frozen_csv(directory / stress / "control" / "portfolio_trades.csv")
            g5.sealed.parity(book, control)
        ledgers += 1
    return dict(result_directory=str(directory), manifest_sha256=g5.base.sha256(directory / "manifest.json"),
                verified_artifact_hashes=len(manifest["files"]), verified_code_hashes=len(manifest["code_sha256"]),
                verified_portfolio_ledgers=ledgers, verified_models=model_count,
                prediction_timestamp_and_score_parity=True, baseline_parity=True,
                accounting_and_daily_curve_parity=True, ml_disabled=not config["ml_enabled"],
                audit_status="PASS", verification_code_sha256=g5.base.sha256(Path(__file__)))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.output is not None and args.output.exists():
        raise ValueError("Require a fresh audit output")
    result = verify(args.directory)
    if args.output:
        g5.write_json(args.output, result)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
