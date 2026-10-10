"""Build a new, isolated frozen-G research source from a completed V13-v9 dataset.

This does not change the G configuration, production policy, or older source runs.
Publication is refused if the historical overlap no longer reproduces the seal.
"""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime
import hashlib
import json
import math
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import numpy as np
import pandas as pd

import fno_v13_v10_g_backtest as g


def _sha(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _write_json(path: Path, value: dict) -> None:
    path.write_text(json.dumps(value, indent=2, default=str) + "\n", encoding="utf-8")


def _check_overlap(old: pd.DataFrame, current: pd.DataFrame) -> None:
    cutoff = str(old.day.astype(str).max())
    current = current.loc[current.day.astype(str).le(cutoff)].copy()
    old = old.copy()
    old["day"] = old.day.astype(str)
    current["day"] = current.day.astype(str)
    keys = ["day", "hhmm_int", "side", "setup_id", "tradingsymbol"]
    if old[keys].to_records(index=False).tolist() != current[keys].to_records(index=False).tolist():
        raise ValueError("Frozen G selections changed in the historical overlap")
    for key in ("filled", "portfolio_executed", "exit_reason"):
        if old[key].astype(str).str.lower().tolist() != current[key].astype(str).str.lower().tolist():
            raise ValueError(f"Frozen G execution changed in the historical overlap: {key}")
    for key in ("entry_ts", "exit_ts"):
        if not pd.to_datetime(old[key], utc=True).equals(pd.to_datetime(current[key], utc=True)):
            raise ValueError(f"Frozen G execution time changed in the historical overlap: {key}")
    for key in ("entry_price", "exit_price", "portfolio_net_profit_rupees", "portfolio_cost_rupees"):
        if not np.allclose(pd.to_numeric(old[key]), pd.to_numeric(current[key]),
                           rtol=1e-10, atol=1e-7, equal_nan=True):
            raise ValueError(f"Frozen G result changed in the historical overlap: {key}")


def _coverage(features: pd.DataFrame, days: list[str]) -> dict:
    result = {}
    for day in days:
        rows = features.loc[features.day.astype(str).eq(day)]
        symbol_counts = rows.groupby("tradingsymbol").size()
        oi_rows = rows.loc[rows.oi.notna() & rows.oi.gt(0)]
        oi_counts = oi_rows.groupby("futures_tradingsymbol").size()
        result[day] = {
            "equity_1m": {
                "symbols_present": int(rows.loc[rows.source_1m_count.gt(0), "tradingsymbol"].nunique()),
                "coverage_basis": "at least one observed source 1m bar in a dataset 5m row",
            },
            "equity_5m": {
                "symbols_present": int(symbol_counts.size),
                "bar_count_distribution": {str(k): int(v) for k, v in sorted(Counter(symbol_counts).items())},
                "total_bars": int(len(rows)),
                "coverage_basis": "observed dataset 5m features, not an imputed full-day claim",
            },
            "futures_oi_5m": {
                "contracts_present": int(oi_counts.size),
                "nonnull_oi_bars": int(len(oi_rows)),
                "partial_contracts": {str(k): int(v) for k, v in oi_counts.items() if v < 72},
                "coverage_basis": "positive OI joined to observed equity 5m feature rows",
            },
        }
    return {"generated_at": datetime.now().astimezone().isoformat(),
            "scope": "derived from frozen causal dataset features; not raw-exchange full-session coverage",
            "dates": result}


def build(root: Path, old_bundle: Path) -> dict:
    root, old_bundle = root.resolve(), old_bundle.resolve()
    dataset = root / "dataset"
    output = root / "g_backtest"
    if output.exists():
        raise FileExistsError(f"Refusing to overwrite existing G output: {output}")
    manifest = json.loads((dataset / "dataset_manifest.json").read_text(encoding="utf-8"))
    for name, expected in manifest["output_sha256"].items():
        if _sha(dataset / name) != expected:
            raise ValueError(f"Dataset hash mismatch: {name}")
    days = [str(day) for day in manifest["days"]]
    cutoff = str(manifest["through_day"])
    if not days or max(days) != cutoff or cutoff <= str(
        json.loads((old_bundle / "g_backtest/run_metadata.json").read_text())["through_day"]
    ):
        raise ValueError("New dataset does not extend the old G cutoff")
    old_meta = json.loads((old_bundle / "g_backtest/run_metadata.json").read_text(encoding="utf-8"))
    config = Path(old_meta["frozen_g_config"])
    if _sha(config) != old_meta["frozen_g_config_sha256"]:
        raise ValueError("Frozen G configuration hash changed")
    settings = g.checked_settings(json.loads(config.read_text(encoding="utf-8")))
    signals = pd.read_parquet(dataset / "signals.parquet")
    base = g.v9.V9Config(portfolio_capital_rupees=1_000_000.,
                         capital_per_entry_rupees=100_000., leverage_factor=5.,
                         max_positions=None, cost_bps=5.)
    selection = g.selection_audit(signals, base, g.SelectionChange(**settings["selection_change"]),
                                  core_first=settings["core_first"],
                                  morning_slots=settings.get("morning_slots", False),
                                  two_bar_continuation=settings.get("two_bar_continuation", False))
    orders = selection.loc[selection.v9_selected].copy().reset_index(drop=True)
    with np.load(dataset / "paths.npz", allow_pickle=False) as archive:
        paths = {int(sid): {key: archive[f"{int(sid)}_{key}"].copy()
                            for key in ("timestamp_ns", "open", "high", "low", "close")}
                 for sid in orders.sid}
    g.v9.validate_paths(orders, paths)
    _, ledger, summary = g.evaluate(dict(signals=signals, orders=orders,
                                         paths=paths, v9_config=base), settings)
    old_ledger = pd.read_csv(old_bundle / "g_backtest/portfolio_trades.csv")
    _check_overlap(old_ledger, ledger)
    metrics = g.r.metric(ledger, days)
    if not math.isclose(metrics["net_profit_rupees"], summary["net_profit_rupees"], abs_tol=1e-7):
        raise ValueError("G summary and portfolio metrics disagree")
    features = pd.read_parquet(dataset / "all_5m_features.parquet")
    coverage = _coverage(features, days)
    output.mkdir(parents=True, exist_ok=False)
    ledger.to_csv(output / "portfolio_trades.csv", index=False)
    _write_json(output / "summary.json", summary)
    _write_json(output / "data_coverage_audit.json", coverage)
    _write_json(output / "run_metadata.json", {
        "through_day": cutoff, "first_session": min(days), "last_session": max(days),
        "session_count": len(days), "generated_at": datetime.now().astimezone().isoformat(),
        "frozen_g_config": str(config), "frozen_g_config_sha256": _sha(config),
        "historical_overlap_parity": True, "historical_overlap_cutoff": str(old_meta["through_day"]),
        "source_kind": "ISOLATED_FROZEN_G_RESEARCH_EXTENSION",
        "metrics": {"full_history": metrics},
    })
    return {"root": str(root), "through_day": cutoff, "session_count": len(days),
            "selected_orders": len(ledger), "net_profit_rupees": metrics["net_profit_rupees"],
            "historical_overlap_parity": True}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--old-bundle", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(build(args.root, args.old_bundle), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
