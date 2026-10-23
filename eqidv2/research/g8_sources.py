"""Pinned G7 V2 inputs for isolated G8 research through 22 October 2026.

No G7 publication pointer is written. Original full-file hashes resolve to
byte-identical sealed price snapshots, including the older rolling files.
Both native executions and portfolio allocation are reproduced before the
combined, uniquely keyed G8 input cache is published.
"""
from __future__ import annotations

from datetime import date
import json
from pathlib import Path

import numpy as np
import pandas as pd

from research import g7_g6_replica_backtest as g7
from research import g7_daily_backtest as daily
from research.g4_failed_breakout_research import SealedPrices

SCHEMA = "g8_g7_v2_sources_v1"
THROUGH = "2026-10-22"
G8_ROOT = g7.G7_ROOT.parent / "backtesting_result_v13_v10_g_8"
CACHE = G8_ROOT / "inputs" / "baseline_v2"
HISTORY = g7.G7_ROOT / "runs/2026-10-22/20261022T160000000000_g7_v2_restored_targets_slot_filters"
DAILY = g7.G7_ROOT / "daily/runs/2026-10-22/20261022T213736388117_6f019403_g7_nifty_futures"
PINNED = {
    str(HISTORY): "e921d2812ab33239ed12c72e1e6ab1a7981f5c234ce43e7ebc00f48ed61b5d88",
    str(DAILY): "15191df5123a45bcae83c6bd82cf3df2032156b96252b6dbdfe5ab239a6a2d53",
}
PATH_FIELDS = ("timestamp_ns", "open", "high", "low", "close")


def _json(path, value):
    Path(path).write_text(json.dumps(value, indent=2, default=str, allow_nan=False) + "\n", encoding="utf-8")


def _package(directory):
    directory = Path(directory).resolve()
    if g7.mirror.sha256(directory / "manifest.json") != PINNED[str(directory)]:
        raise ValueError("Pinned G7 V2 manifest changed: " + str(directory))
    manifest = g7.mirror.read(directory / "manifest.json")
    for relative, digest in manifest["files"].items():
        path = (directory / relative).resolve()
        if not path.is_relative_to(directory) or g7.mirror.sha256(path) != digest:
            raise ValueError("Pinned G7 V2 member changed: " + relative)
    summary = g7.mirror.read(directory / "summary.json")
    config = g7.mirror.read(directory / "config.json")
    if (summary["variant_id"] != g7.V2_VERSION or summary["active_setups"] != 51
            or summary["execution_authority"] is not False
            or summary["selected_policy"] != g7.NIFTY_POLICY
            or config != g7.configuration(g7.V2_CONFIG)):
        raise ValueError("G8 requires the unchanged G7 V2 configuration and Nifty policy")
    return summary, config


def _legacy_accounting(directory, summary):
    """Check V2's own metric schema, which predates V3 partial-target counts."""
    comparison = g7.read_frozen_csv(directory / "comparison.csv")
    for (policy, period), recorded in comparison.groupby(["policy", "period"]):
        if len(recorded) != 1:
            raise ValueError("Duplicate G7 V2 comparison row")
        days = summary["days"] if period == "all" else summary["days"][:-1] if period == "prior" else summary["days"][-1:]
        book = g7.read_frozen_csv(directory / policy / "portfolio_trades.csv")
        for field, value in g7.bounded.stats(book, days).items():
            if not np.isclose(value, recorded.iloc[0][field], atol=1e-6, rtol=0):
                raise ValueError("G7 V2 accounting differs: " + field)
    g7.nifty_filter.verify_saved(directory, summary)


def _same_native(actual, expected):
    """Compare every selected order, including fills rejected by allocation."""
    a, b = (frame.set_index("sid").sort_index() for frame in (actual, expected))
    if not a.index.equals(b.index):
        raise ValueError("Native replay order inventory differs")
    for field in ("filled", "exit_reason"):
        if not a[field].fillna("").equals(b[field].fillna("")):
            raise ValueError("Native replay differs: " + field)
    for field in ("entry_ts", "exit_ts"):
        if not pd.to_datetime(a[field], utc=True).equals(pd.to_datetime(b[field], utc=True)):
            raise ValueError("Native replay timestamp differs: " + field)
    for field in ("entry_path_index", "exit_path_index", "entry_price", "exit_price", "gross_return_pct", "net_return_pct", "cost_pct"):
        if not np.allclose(pd.to_numeric(a[field], errors="coerce"), pd.to_numeric(b[field], errors="coerce"),
                           rtol=0, atol=1e-8, equal_nan=True):
            raise ValueError("Native replay value differs: " + field)


def _history_paths(orders, summary):
    source = Path(summary["current_g3_source_run"])
    if g7.mirror.sha256(source / "manifest.json") != summary["current_g3_manifest_sha256"]:
        raise ValueError("Pinned historical G3 source changed")
    source_summary = g7.mirror.read(source / "summary.json")
    prices = SealedPrices(source_summary, summary["price_sources"])
    paths = {}
    for n, row in enumerate(orders.sort_values(["tradingsymbol", "day", "confirmation_ts"]).to_dict("records"), 1):
        frame, _ = prices.frame(pd.Timestamp(row["day"]).date(), row["tradingsymbol"])
        confirmation = frame.loc[frame.ts.eq(pd.Timestamp(row["confirmation_ts"]))]
        if len(confirmation) != 1 or any(not np.isclose(float(confirmation.iloc[0][field]),
                float(row["confirmation_" + field]), atol=.001, rtol=0) for field in ("open", "high", "low", "close")):
            raise ValueError("Sealed confirmation OHLC differs from original selected order")
        paths.update(prices.paths(pd.DataFrame([row])))
        if n % 100 == 0:
            print(f"G8 exact original price paths: {n}/{len(orders)}", flush=True)
    return paths, dict(price_sources=prices.resolution,
        source_g3_manifest_sha256=summary["current_g3_manifest_sha256"],
        source_coverage=source_summary["coverage"],
        inherited_targets_selected_after_inspection=source_summary["historical_targets_selected_after_inspection"],
        inherited_independent_holdout_qualification=source_summary["independent_holdout_qualification"])


def _check_replay(orders, paths, days, config, original, baseline, audit):
    native = g7.bounded.history._simulate(orders, paths, [date.fromisoformat(d) for d in days], g7.Identity(config))
    unfiltered = g7._bounded(native, paths, config)
    _same_native(unfiltered, original)
    g7.bounded.history._same_allocation(unfiltered, original)
    filtered, _ = g7.nifty_filter.apply_filter(unfiltered, audit, config)
    g7.bounded.history._same_allocation(filtered, baseline)


def _pointer_hashes():
    return {str(p): g7.mirror.sha256(p) for p in
            (g7.G7_ROOT / "latest/bounded_exit.json", g7.G7_ROOT / "latest/daily_backtest.json") if p.is_file()}


def build_cache(cache=CACHE):
    cache = Path(cache).resolve()
    if cache.exists() or not cache.is_relative_to(G8_ROOT.resolve()):
        raise ValueError("G8 input cache requires a new directory inside its own result root")
    pointers = _pointer_hashes()
    parts = {name: [] for name in ("orders", "unfiltered", "baseline", "audit")}
    all_paths, days, evidence = {}, [], []
    next_sid = 1
    for directory in (HISTORY, DAILY):
        summary, config = _package(directory)
        orders = g7.read_frozen_csv(directory / "selected_orders.csv")
        unfiltered = g7.read_frozen_csv(directory / "g7_unfiltered/portfolio_trades.csv")
        baseline = g7.read_frozen_csv(directory / g7.NIFTY_POLICY / "portfolio_trades.csv")
        audit = g7.read_frozen_csv(directory / "nifty_direction_audit.csv")
        if directory == HISTORY:
            _legacy_accounting(directory, summary)
            paths, extra = _history_paths(orders, summary)
        else:
            daily.verify(directory)
            raw = pd.read_parquet(directory / "minute_paths.parquet")
            paths = {int(sid): {f: group[f].to_numpy() for f in PATH_FIELDS} for sid, group in raw.groupby("sid", sort=False)}
            extra = dict(source_coverage=summary["source_coverage"],
                         price_sources=g7.mirror.read(directory / "price_sources.json"))
        _check_replay(orders, paths, summary["days"], config, unfiltered, baseline, audit)
        keys = set(orders.sid.astype(int))
        if any(set(frame.sid.astype(int)) != keys for frame in (unfiltered, baseline, audit)) or set(paths) != keys:
            raise ValueError("Selected orders, native books, Nifty audit, and minute paths differ")
        remap = {int(sid): next_sid + i for i, sid in enumerate(orders.sid)}
        next_sid += len(remap)
        for name, frame in (("orders", orders), ("unfiltered", unfiltered), ("baseline", baseline), ("audit", audit)):
            frame = frame.copy()
            frame["g8_source_sid"] = frame.sid.astype(int)
            frame["g8_source_package"] = directory.name
            frame["sid"] = frame.g8_source_sid.map(remap).astype(int)
            frame["day"] = frame.day.astype(str).str[:10]
            parts[name].append(frame)
        all_paths.update({remap[sid]: path for sid, path in paths.items()})
        days.extend(summary["days"])
        evidence.append(dict(source_run=str(directory), manifest_sha256=PINNED[str(directory)],
            days=summary["days"], all_native_rows_reproduced=True, allocation_reproduced=True, **extra))
    if len(set(days)) != len(days) or max(days) != THROUGH:
        raise ValueError("G8 baseline session calendar is duplicated or incomplete")
    combined = {name: pd.concat(values, ignore_index=True, sort=False) for name, values in parts.items()}
    days = sorted(days)
    for frame in combined.values():
        if frame.sid.duplicated().any() or not set(frame.day).issubset(days):
            raise ValueError("G8 combined source identity is ambiguous")
    stats = g7.bounded.stats(combined["baseline"], days)
    if (stats["trades"], stats["wins"], stats["losses"]) != (428, 325, 103) or not np.isclose(
            stats["net_pnl_rs"], 1069336.820819679, atol=1e-6, rtol=0):
        raise ValueError("Pinned combined G7 V2 baseline changed")
    if _pointer_hashes() != pointers:
        raise ValueError("A G7 result pointer changed during source verification")
    provenance = dict(schema_version=SCHEMA, through=THROUGH, days=days, sessions=len(days),
        source_variant=g7.V2_VERSION, scope="BACKTEST_ONLY", execution_authority=False,
        exact_original_price_bytes=True, native_parity_all_orders=True, baseline_stats=stats,
        sources=evidence, g7_pointer_sha256_before_and_after=pointers,
        inherited_validation_warning="Inherited setup targets were selected after historical inspection. A G8 chronological split is not a fully unseen validation of the inherited strategy.")
    cache.mkdir(parents=True)
    for name, frame in combined.items():
        frame.to_csv(cache / (name + ".csv"), index=False)
    np.savez_compressed(cache / "minute_paths.npz", **{f"{sid}_{field}": value
        for sid, path in all_paths.items() for field, value in path.items()})
    _json(cache / "config.json", config)
    _json(cache / "provenance.json", provenance)
    _json(cache / "manifest.json", dict(schema_version=SCHEMA, through=THROUGH,
        files={p.name: g7.mirror.sha256(p) for p in cache.iterdir() if p.is_file()}))
    print("G8 BASELINE VERIFIED", cache, stats, flush=True)
    return cache


def load_sources(cache=CACHE, *, through=THROUGH):
    """Return config, unfiltered, baseline, orders, audit, paths, days, provenance."""
    if str(through) != THROUGH:
        raise ValueError("This pinned source revision ends at 2026-10-22")
    cache = Path(cache).resolve()
    if not cache.exists():
        build_cache(cache)
    manifest = g7.mirror.read(cache / "manifest.json")
    if manifest.get("schema_version") != SCHEMA or manifest.get("through") != THROUGH:
        raise ValueError("Unexpected G8 source cache identity")
    for name, digest in manifest["files"].items():
        path = (cache / name).resolve()
        if not path.is_relative_to(cache) or g7.mirror.sha256(path) != digest:
            raise ValueError("Frozen G8 source cache changed: " + name)
    frames = {name: g7.read_frozen_csv(cache / (name + ".csv")) for name in ("orders", "unfiltered", "baseline", "audit")}
    with np.load(cache / "minute_paths.npz", allow_pickle=False) as arrays:
        paths = {int(sid): {field: arrays[f"{sid}_{field}"].copy() for field in PATH_FIELDS}
                 for sid in frames["orders"].sid}
    provenance = g7.mirror.read(cache / "provenance.json")
    return dict(config=g7.mirror.read(cache / "config.json"), **frames, paths=paths,
        days=provenance["days"], provenance=provenance, cache=cache,
        manifest_sha256=g7.mirror.sha256(cache / "manifest.json"))


if __name__ == "__main__":
    result = load_sources()
    print(json.dumps(result["provenance"]["baseline_stats"], indent=2))
