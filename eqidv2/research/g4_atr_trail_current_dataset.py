"""Append the newly completed G4 session to the sealed ATR research dataset.

The original 52-session cache remains untouched. The new day is recovered from
the exact input snapshots recorded by the current G4 run, and the 533 earlier
orders must reproduce the prior cache's entries, exits and allocation.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd

from research import g4_iterative_dataset as original
from research import g4_hybrid_monitor_backtest as hybrid
from research import g4_trade_monitor_backtest as paths_builder
from research import g4_g3_mirror_backtest as base
from research import g4_failed_breakout_research as sealed


DEFAULT_CURRENT = (base.G4_ROOT / "runs/2026-10-17/approved_1216_g4_report_v1")
DEFAULT_OUTPUT = base.G4_ROOT / "research/20261017_g4_53_session_sealed_for_atr"
PARITY_COLUMNS = ("sid", "day", "setup_id", "tradingsymbol", "side", "filled",
    "portfolio_executed", "entry_ts", "entry_price", "native_target_pct",
    "exit_ts", "exit_price", "exit_reason", "gross_return_pct",
    "net_return_pct", "portfolio_net_profit_rupees")


def prepare(output: Path = DEFAULT_OUTPUT, current: Path = DEFAULT_CURRENT,
            prior: Path = original.base.G4_ROOT / "research/20261016_iterative_sealed_dataset_v1") -> Path:
    output, current, prior = (Path(value).resolve() for value in (output, current, prior))
    if output.exists() or output.parent != (base.G4_ROOT / "research").resolve():
        raise ValueError("Fresh direct-child G4 research output required")
    current_summary = sealed.package(current)
    if current_summary["variant_id"] != "V13-V10-G-4-BOUNDED-EXIT-51-SETUPS-V13":
        raise ValueError("Unexpected current G4 variant")
    old, old_contexts, old_paths, old_summary = original.load(prior)
    frame = base.read_frozen_csv(current / "g4_bounded_exit/portfolio_trades.csv")
    if frame.sid.duplicated().any() or set(old.sid) - set(frame.sid):
        raise ValueError("New G4 run is missing or duplicating original orders")
    old_sorted = old.sort_values("sid").set_index("sid")
    current_old = frame.loc[frame.sid.isin(old.sid)].sort_values("sid").set_index("sid")
    if len(current_old) != len(old):
        raise ValueError("Previous cohort changed size")
    for field in PARITY_COLUMNS[1:]:
        before, after = old_sorted[field], current_old[field]
        if field in {"entry_price", "native_target_pct", "exit_price", "gross_return_pct",
                     "net_return_pct", "portfolio_net_profit_rupees"}:
            if not np.allclose(pd.to_numeric(before), pd.to_numeric(after), atol=1e-6, rtol=0,
                               equal_nan=True):
                raise ValueError("Previous cohort numeric parity changed: " + field)
        elif not before.fillna("<missing>").astype(str).equals(after.fillna("<missing>").astype(str)):
            raise ValueError("Previous cohort parity changed: " + field)
    added = frame.loc[~frame.sid.isin(old.sid)].copy()
    if not len(added) or set(added.day.astype(str).str[:10]) != {"2026-10-16"}:
        raise ValueError("Only the completed October 16 session may be added")
    if current_summary["days"] != old_summary["days"] + ["2026-10-16"]:
        raise ValueError("Current G4 session lineage differs")
    output.mkdir(parents=True)
    new_dir = output / "new_day_evidence"
    new_dir.mkdir()
    new_paths, price_resolution = paths_builder.historical_paths(added, current_summary, new_dir)
    new_contexts, context_evidence = hybrid._feature_contexts(
        added, new_paths, dict(price_sources=price_resolution), current_summary)
    for row in added.to_dict("records"):
        if not bool(row["filled"]):
            continue
        result = sealed.simulate_exit(new_paths[int(row["sid"])], int(row["entry_path_index"]),
            float(row["entry_price"]), row["side"] == "LONG", 1.2,
            float(row["native_target_pct"]), tighten_after_minutes=120,
            tightened_stop_pct=1.)
        if (not np.isclose(result["exit_price"], row["exit_price"], atol=1e-8, rtol=0)
                or pd.Timestamp(result["exit_ts"]) != pd.Timestamp(row["exit_ts"])):
            raise ValueError("New day exit replay differs from G4 source")
    combined_contexts = {**old_contexts, **new_contexts}
    combined_paths = {**old_paths, **new_paths}
    if len(combined_contexts) != len(frame) or len(combined_paths) != len(frame):
        raise ValueError("Combined sealed contexts or paths incomplete")
    pd.concat([context.assign(sid=sid) for sid, context in combined_contexts.items()],
              ignore_index=True).to_parquet(output / "contexts.parquet", index=False)
    frame.to_csv(output / "native.csv", index=False)
    np.savez_compressed(output / "historical_paths.npz", **{
        f"{sid}_{key}": value for sid, path in combined_paths.items() for key, value in path.items()})
    source_code = [Path(__file__), Path(original.__file__), Path(hybrid.__file__),
        Path(paths_builder.__file__), Path(sealed.__file__)]
    code = {str(path.resolve()): base.sha256(path) for path in source_code}
    summary = dict(variant_id="G4-ITERATIVE-SEALED-DATASET-V1", execution_authority=False,
        extension_variant_id="G4-ATR-53-SESSION-SEALED-EXTENSION-V1",
        days=current_summary["days"], original_source=str(current),
        original_manifest_sha256=base.sha256(current / "manifest.json"),
        prior_dataset=str(prior), prior_manifest_sha256=base.sha256(prior / "manifest.json"),
        previous_orders_reproduced=len(old), appended_day="2026-10-16",
        appended_orders=len(added), appended_executed_trades=int(added.portfolio_executed.sum()),
        native_control_exactly_reproduced=True,
        price_sources=context_evidence, code=code,
        orders=len(frame), executed_trades=int(frame.portfolio_executed.sum()))
    (output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    manifest = {path.relative_to(output).as_posix(): base.sha256(path)
                for path in output.rglob("*") if path.is_file()}
    (output / "manifest.json").write_text(json.dumps({"files": manifest}, indent=2) + "\n", encoding="utf-8")
    # Exercise the consumer's manifest and code verification before handoff.
    checked, contexts, paths, checked_summary = original.load(output)
    if len(checked) != len(frame) or set(contexts) != set(paths) or checked_summary["days"] != current_summary["days"]:
        raise ValueError("Extended sealed dataset failed consumer validation")
    print(f"Sealed current G4: {len(frame)} orders, {int(frame.portfolio_executed.sum())} executions", flush=True)
    print(output, flush=True)
    return output


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--current-run", type=Path, default=DEFAULT_CURRENT)
    args = parser.parse_args()
    prepare(args.output_dir, args.current_run)
