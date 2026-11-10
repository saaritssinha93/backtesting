"""Recover exports from completed daily replays, without simulating any trades.

``concat_for_parquet`` is also suitable for the partition merger.  It preserves
numeric columns, converting only columns independently evidenced as logical
(a Boolean dtype or actual boolean values in at least one input frame).
"""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from datetime import datetime, timezone
from numbers import Real

import numpy as np
import pandas as pd
from pandas.api.types import is_bool_dtype

from g3_v18_20261102.run import dump, guard_writes, sha
from .run import choose, control_parity

PACKAGE = Path(__file__).resolve().parent
COMPUTATIONAL = {"run.py", "selection.py", "features.py", "prepare.py", "model.py", "__init__.py"}
REQUIRED = ("daily.csv", "trades.parquet", "candidate_outcomes.parquet", "gates.parquet", "selection_decisions.parquet")
OUT_FIELDS = ["sid", "filled", "entry_ts", "exit_ts", "entry_price", "exit_price", "gross_return_pct", "net_return_pct", "exit_reason"]
STRESS = [v + "__trigger1bp" for v in ("control", "or_market", "group_score", "or_group_score")]


def boolean_columns(frames):
    """Infer logical semantics from original frames, before concat loses dtype."""
    found = set()
    for frame in frames:
        for column in frame:
            values = frame[column]
            if is_bool_dtype(values.dtype):
                found.add(column)
            elif values.dtype == object and any(isinstance(v, (bool, np.bool_)) for v in values.dropna()):
                found.add(column)
    return found


def _logical(value, column):
    if pd.isna(value):
        return pd.NA
    if isinstance(value, (bool, np.bool_)):
        return bool(value)
    if isinstance(value, Real) and np.isfinite(value) and value in (0, 1):
        return bool(value)
    if isinstance(value, str) and value.strip().lower() in ("true", "false"):
        return value.strip().lower() == "true"
    raise ValueError(f"Non-logical value in independently identified boolean column {column}: {value!r}")


def normalize_for_parquet(frame, logical_columns=None):
    """Return a copy and conversion audit; never stringify numeric values.

    Explicit logical_columns should come from boolean_columns(original_frames).
    A numeric-only 0/1 column is never automatically guessed to be boolean.
    """
    out = frame.copy()
    columns = boolean_columns([frame]) | set(logical_columns or ())
    audit = {}
    for column in sorted(columns.intersection(frame.columns)):
        before = str(frame[column].dtype)
        out[column] = pd.array([_logical(v, column) for v in frame[column]], dtype="boolean")
        audit[column] = {"original_dtype": before, "export_dtype": "boolean", "missing": int(out[column].isna().sum()),
                         "true": int(out[column].sum()), "false": int(out[column].eq(False).sum())}
    return out, audit


def concat_for_parquet(frames):
    """Concatenate without Arrow's ambiguous bool/float object inference."""
    frames = list(frames)
    if not frames:
        raise ValueError("No frames supplied")
    return normalize_for_parquet(pd.concat(frames, ignore_index=True, sort=False), boolean_columns(frames))


def _atomic_parquet(frame, path):
    temporary = path.with_name(path.name + ".recovery.tmp")
    frame.to_parquet(temporary, index=False)
    check = pd.read_parquet(temporary)
    pd.testing.assert_frame_equal(check, frame, check_dtype=False)
    os.replace(temporary, path)


def _atomic_csv(frame, path):
    temporary = path.with_name(path.name + ".recovery.tmp")
    frame.to_csv(temporary, index=False)
    os.replace(temporary, path)


def _same_csv(left, right, keys):
    left = left.sort_values(keys).reset_index(drop=True)
    right = right.sort_values(keys).reset_index(drop=True)
    if set(left.columns) != set(right.columns):
        raise ValueError("Existing aggregate has different columns")
    pd.testing.assert_frame_equal(left[right.columns], right, check_dtype=False, check_exact=False, rtol=1e-10, atol=1e-7)


def finalize(partition: Path, inputs: Path):
    partition, inputs = partition.resolve(), inputs.resolve()
    if not partition.is_relative_to(PACKAGE) or not partition.is_dir():
        raise ValueError("Existing lab partition required")
    config_path = partition / "config.json"
    config = json.loads(config_path.read_text())
    config_hash = sha(config_path)
    manifest = json.loads((inputs / "manifest.json").read_text())
    if sha(inputs / "manifest.json") != config["input_manifest_sha256"]:
        raise ValueError("Frozen input manifest changed")
    if sha(inputs / "protocol.json") != manifest["protocol_sha256"]:
        raise ValueError("Frozen protocol changed")
    if json.loads((inputs / "protocol.json").read_text()) != config["protocol"]:
        raise ValueError("Partition protocol differs")
    for name in sorted(COMPUTATIONAL):
        if sha(PACKAGE / name) != config["code_hashes"].get(name):
            raise ValueError(f"Frozen computation changed: {name}")
    days = config["days"]
    if len(days) != len(set(days)) or any(d not in manifest["days"] for d in days):
        raise ValueError("Invalid partition calendar")
    records = {r["day"]: r for r in manifest["sessions"]}
    protocol = config["protocol"]
    expected_rows = {(v, float(c)) for v in protocol["rules"] for c in protocol["costs_bps"]}
    expected_rows |= {(v, 5.) for v in STRESS}
    # All days must be complete before starting any export or recovery marker.
    for day in days:
        for name in REQUIRED:
            if not (partition / day / name).is_file():
                raise ValueError(f"Incomplete day: {day}/{name}")
    books, pools, daily_frames, failures, parities, provenance = [], [], [], [], [], []
    for day in days:
        folder = partition / day
        record = records[day]
        for name, key in (("raw.parquet", "raw_sha256"), ("minutes.parquet", "minutes_sha256")):
            if sha(inputs / day / name) != record[key]:
                raise ValueError(f"Frozen day input changed: {day}/{name}")
        hashes = {name: sha(folder / name) for name in REQUIRED}
        for name in ("all51_gates.parquet", "slot_expansion.json"):
            if (folder / name).exists():
                hashes[name] = sha(folder / name)
        daily = pd.read_csv(folder / "daily.csv")
        if not daily.day.astype(str).eq(day).all() or daily.duplicated(["variant", "cost_bps"]).any():
            raise ValueError(f"Invalid daily identifiers: {day}")
        if set(zip(daily.variant, daily.cost_bps.astype(float))) != expected_rows:
            raise ValueError(f"Incomplete variant/cost grid: {day}")
        book = pd.read_parquet(folder / "trades.parquet")
        if not book.day.astype(str).str[:10].eq(day).all() or book.duplicated(["variant", "sid"]).any():
            raise ValueError(f"Invalid trade identities: {day}")
        if not set(book.variant).issubset(set(protocol["rules"]) | set(STRESS)):
            raise ValueError(f"Unknown trade variant: {day}")
        for row in daily.loc[daily.cost_bps.eq(5)].itertuples():
            arm = book.loc[book.variant.eq(row.variant)]
            done = arm.loc[arm.portfolio_executed.eq(True)]
            if len(arm) != row.selected_orders or len(done) != row.trades:
                raise ValueError(f"Trade count mismatch: {day}/{row.variant}")
            if not np.isclose(done.portfolio_net_profit_rupees.sum(), row.net_pnl_rs, rtol=1e-10, atol=1e-6):
                raise ValueError(f"PnL mismatch: {day}/{row.variant}")
        prepared = pd.read_parquet(folder / "gates.parquet")
        master = pd.read_parquet(folder / "candidate_outcomes.parquet")
        decisions = pd.read_parquet(folder / "selection_decisions.parquet")
        if master.sid.duplicated().any() or prepared.sid.duplicated().any() or decisions.duplicated(["variant", "sid"]).any():
            raise ValueError(f"Duplicate candidate identities: {day}")
        if not decisions.day.astype(str).str[:10].eq(day).all():
            raise ValueError(f"Candidate decision date mismatch: {day}")
        _, audit = choose(prepared, "or_market")
        prior = decisions.loc[decisions.variant.eq("or_market")].set_index("sid")
        again = audit.set_index("sid")
        if set(prior.index) != set(again.index):
            raise ValueError(f"OR candidate pool changed: {day}")
        for col in ("v9_selected", "v9_filter_pass"):
            if not prior[col].astype("boolean").sort_index().equals(again[col].astype("boolean").sort_index()):
                raise ValueError(f"OR decisions changed: {day}/{col}")
        pool = audit.loc[audit.v9_filter_pass].copy()
        pool["variant"] = "or_market"  # run.py added this before capturing audit.
        if not set(pool.sid).issubset(set(master.sid)):
            raise ValueError(f"Missing independently simulated OR outcomes: {day}")
        pool = pool.drop(columns=[c for c in OUT_FIELDS if c in pool and c != "sid"])
        pool = pool.merge(master[OUT_FIELDS], on="sid", validate="one_to_one")
        for variant, group in decisions.groupby("variant", sort=False):
            for gate, count in group.loc[~group.v9_filter_pass.astype("boolean")].first_failed_gate.value_counts().items():
                failures.append(dict(day=day, variant=variant, first_failed_gate=gate, candidates=int(count)))
        raw = pd.read_parquet(inputs / day / "raw.parquet")
        orders, _ = choose(prepared, "control")
        parities.append(control_parity(raw, orders, book.loc[book.variant.eq("control")]))
        books.append(book)
        pools.append(pool)
        daily_frames.append(daily)
        provenance.append(dict(day=day, artifact_sha256=hashes, raw_sha256=record["raw_sha256"],
                               minutes_sha256=record["minutes_sha256"], trades_rows=len(book), model_pool_rows=len(pool)))
        print(f"{partition.name} {day}: saved artifacts verified; selection/ledger parity PASS", flush=True)
    daily = pd.concat(daily_frames, ignore_index=True)
    gates = pd.DataFrame(failures, columns=["day", "variant", "first_failed_gate", "candidates"])
    for name, frame, keys in (("daily_results.csv", daily, ["day", "variant", "cost_bps"]),
                              ("gate_failures.csv", gates, ["day", "variant", "first_failed_gate"])):
        path = partition / name
        if path.is_file():
            _same_csv(pd.read_csv(path), frame, keys)
        else:
            _atomic_csv(frame, path)
    trades, trade_types = concat_for_parquet(books)
    model_pool, pool_types = concat_for_parquet(pools)
    if trades.duplicated(["variant", "sid"]).any() or model_pool.sid.duplicated().any():
        raise ValueError("Cross-day duplicate identifiers")
    _atomic_parquet(trades, partition / "trades.parquet")
    _atomic_parquet(model_pool, partition / "model_pool.parquet")
    dump(partition / "control_parity.json", parities)
    for rec in provenance:
        for name, digest in rec["artifact_sha256"].items():
            if sha(partition / rec["day"] / name) != digest:
                raise ValueError("Daily artifact changed during recovery")
    if sha(config_path) != config_hash:
        raise ValueError("Partition configuration changed during recovery")
    report = dict(kind="EXPORT_ONLY_RECOVERY", recovered_utc=datetime.now(timezone.utc).isoformat(),
                  native_simulation_rerun=False, strategy_edited=False, daily_artifacts_unchanged=True,
                  config_sha256=config_hash, recovery_script_sha256=sha(Path(__file__)),
                  input_manifest_sha256=config["input_manifest_sha256"], days=days,
                  trades_rows=len(trades), model_pool_rows=len(model_pool), native_parity_days=len(parities),
                  normalization={"trades": trade_types, "model_pool": pool_types}, daily=provenance,
                  output_sha256={name: sha(partition / name) for name in
                                 ("daily_results.csv", "gate_failures.csv", "trades.parquet", "model_pool.parquet", "control_parity.json")})
    dump(partition / "export_recovery.json", report)
    dump(partition / "rules_complete.json", dict(days=days, variants=protocol["rules"], native_parity_days=len(parities),
                                                 execution_authority=False, export_recovery_sha256=sha(partition / "export_recovery.json")))
    print(f"{partition.name}: recovered {len(days)} days, {len(trades)} trade rows, {len(model_pool)} model candidates", flush=True)
    return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--partitions", "--partition", type=Path, nargs="+", required=True)
    parser.add_argument("--inputs", type=Path, default=PACKAGE / "inputs")
    args = parser.parse_args()
    guard_writes(PACKAGE)
    for partition in args.partitions:
        finalize(partition, args.inputs)
