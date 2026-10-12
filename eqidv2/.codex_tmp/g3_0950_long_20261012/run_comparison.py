"""Isolated 46-session G3 experiment: add a 09:50 LONG cloned from 09:55.

This process changes the in-memory contract only. It never writes production
configuration, frozen evidence, scheduled jobs, or live state.
"""
from __future__ import annotations

import hashlib
import json
import sys
from dataclasses import asdict, replace
from datetime import date, datetime
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

import fno_v13_replay_source as source
import fno_v13_v10_g_3_contract as contract
import fno_v13_v10_g_3_daily_replay as replay
import fno_v13_v10_g_2_extend_results as ext
from research.g3_freeze import DEFAULT_FROZEN
from research.g3_six_confirmation_variants import _extension_pool

STUDY = Path(r"C:\TradingData\eqidv2\fno_oi\strategy_research\v13_corrected_v10_g_3\run_20261008_six_confirmation_variants")
OUT = Path(r"C:\TradingData\eqidv2\fno_oi\strategy_research\v13_v10_g_3_0950_long_20261012")
CACHE = Path(__file__).with_name("signals_cache.parquet")


def sha(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def load_signals() -> tuple[pd.DataFrame, dict, dict]:
    bundle = replay.load_sealed_source()
    provenance = json.loads((STUDY / "provenance.json").read_text(encoding="utf-8"))
    if provenance["days"] != [str(d) for d in bundle["days"]] + [x["day"] for x in provenance["extensions"]]:
        raise RuntimeError("Accepted 46-session dates differ from source")
    if CACHE.exists():
        signals = normalize_signals(pd.read_parquet(CACHE))
        if signals.sid.duplicated().any():
            raise RuntimeError("Cached source IDs overlap")
        print(f"Verified source cache: {len(signals)} rows", flush=True)
        return signals, bundle, provenance
    parts = [bundle["signals"]]
    for n, item in enumerate(provenance["extensions"]):
        day = date.fromisoformat(item["day"])
        run, snap = Path(item["run"]), Path(item["snapshot"])
        manifest = snap / "snapshot_manifest.json"
        if sha(manifest) != provenance["verified_snapshot_manifest_hashes"][str(manifest)]:
            raise RuntimeError(f"Extension snapshot manifest drift: {day}")
        raw = _extension_pool(day, run, snap)
        nifty = pd.to_numeric(raw.nifty_first_bar_return_pct, errors="coerce").dropna().unique()
        if len(nifty) != 1:
            raise RuntimeError(f"Missing NIFTY context: {day}")
        strict = source._strict_signals(raw, float(nifty[0]))
        strict["sid"] = strict.sid.astype(int) + 1_000_000 + n * 10_000
        parts.append(strict)
        print(f"Source {day}: {len(strict)} strict candidates", flush=True)
    signals = normalize_signals(pd.concat(parts, ignore_index=True, sort=False))
    if signals.sid.duplicated().any():
        raise RuntimeError("Duplicate source IDs")
    signals.to_parquet(CACHE, index=False)
    return signals, bundle, provenance


def normalize_signals(signals: pd.DataFrame) -> pd.DataFrame:
    """Align fields from old sealed rows and later daily feature ledgers."""
    signals = signals.copy()
    for name in ("ema9", "ema20", "ema50"):
        signals[name] = pd.to_numeric(signals[name], errors="coerce").fillna(
            pd.to_numeric(signals[f"v9_5m_{name}"], errors="coerce"))
    signals["source_1m_count"] = pd.to_numeric(signals.source_1m_count, errors="coerce").fillna(5)
    signals["signal_end"] = signals.signal_end.fillna(pd.to_datetime(signals.signal_ts).dt.strftime("%H:%M"))
    signals["v9_5m_feature_ts"] = signals.v9_5m_feature_ts.fillna(signals.signal_ts)
    signals["v9_feature_available_ts"] = signals.v9_feature_available_ts.fillna(signals.confirmation_ts)
    return signals


def select(signals: pd.DataFrame, *, extra: bool) -> pd.DataFrame:
    if extra:
        donor_core = contract._CORE_BY_ID["0956_LONG"]
        donor_active = contract._ACTIVE_BY_ID["0956_LONG"]
        core = replace(donor_core, signal_end="09:50", confirmation_end="09:51")
        active = replace(donor_active, signal_end="09:50", confirmation_end="09:51")
        assert core.setup_id == active.setup_id == "0951_LONG"
        contract.CORE_SETUPS = (*contract.CORE_SETUPS, core)
        contract.ACTIVE_SETUPS = (*contract.ACTIVE_SETUPS, active)
        contract._CORE_BY_ID[core.setup_id] = core
        contract._ACTIVE_BY_ID[active.setup_id] = active
    return contract.select_orders(signals)


def paths_for(orders: pd.DataFrame, bundle: dict, provenance: dict) -> dict:
    by_day = pd.to_datetime(orders.day).dt.date
    historical = orders.loc[by_day.isin(bundle["days"])]
    needed = set(historical.sid.astype(int))
    paths = {}
    with np.load(bundle["root"] / "paths.npz", allow_pickle=False) as archive:
        for name in archive.files:
            sid_text, field = name.split("_", 1)
            sid = int(sid_text)
            if sid in needed:
                paths.setdefault(sid, {})[field] = archive[name]
    if set(paths) != needed:
        raise RuntimeError(f"Historical paths missing for {sorted(needed - set(paths))}")
    for item in provenance["extensions"]:
        day = date.fromisoformat(item["day"])
        subset = orders.loc[by_day.eq(day)].copy()
        if len(subset):
            extension = ext._selected_paths(subset, day, Path(item["snapshot"]))
            if set(extension) != set(subset.sid.astype(int)):
                raise RuntimeError(f"Extension paths missing: {day}")
            paths.update(extension)
    return paths


def metrics(ledger: pd.DataFrame) -> dict:
    executed = ledger.loc[ledger.portfolio_executed.eq(True)]
    pnl = executed.portfolio_net_profit_rupees.astype(float)
    daily = executed.assign(day=pd.to_datetime(executed.day).dt.date).groupby("day").portfolio_net_profit_rupees.sum()
    equity = daily.reindex(DAYS, fill_value=0.).cumsum()
    return dict(selected_orders=len(ledger), fills=int(ledger.filled.eq(True).sum()),
                executed=len(executed), wins=int(pnl.gt(0).sum()), losses=int(pnl.lt(0).sum()),
                win_rate_pct=100 * float(pnl.gt(0).mean()) if len(pnl) else 0.,
                gross_pnl_rs=float(executed.portfolio_gross_profit_rupees.sum()),
                cost_rs=float(executed.portfolio_cost_rupees.sum()),
                net_pnl_rs=float(pnl.sum()),
                max_day_end_drawdown_rs=float((equity.cummax() - equity).max()))


def compare_to_frozen(observed: pd.DataFrame, frozen: pd.DataFrame) -> None:
    if set(observed.sid.astype(int)) != set(frozen.sid.astype(int)):
        raise RuntimeError("Baseline selections differ from accepted frozen G3")
    left = observed.set_index("sid").sort_index()
    right = frozen.set_index("sid").sort_index()
    for name in ("filled", "portfolio_executed", "exit_reason"):
        if not left[name].astype(str).equals(right[name].astype(str)):
            raise RuntimeError(f"Baseline frozen parity failed: {name}")
    if not np.allclose(left.portfolio_net_profit_rupees.astype(float),
                       right.portfolio_net_profit_rupees.astype(float), rtol=0, atol=1e-6):
        difference = abs(left.portfolio_net_profit_rupees.astype(float) - right.portfolio_net_profit_rupees.astype(float))
        raise RuntimeError(f"Baseline frozen P&L parity failed: max diff {difference.max()}")


def main() -> None:
    global DAYS
    if OUT.exists() and any(OUT.iterdir()):
        raise FileExistsError(f"Research output already exists: {OUT}")
    signals, bundle, provenance = load_signals()
    DAYS = [date.fromisoformat(x) for x in provenance["days"]]
    frozen = pd.read_csv(DEFAULT_FROZEN / "trades.csv")
    baseline_orders = select(signals, extra=False)
    if len(baseline_orders) != len(frozen) or set(baseline_orders.sid.astype(int)) != set(frozen.sid.astype(int)):
        extra = baseline_orders.loc[~baseline_orders.sid.isin(frozen.sid), ["sid", "day", "setup_id", "tradingsymbol"]]
        missing = frozen.loc[~frozen.sid.isin(baseline_orders.sid), ["sid", "day", "setup_id", "tradingsymbol"]]
        print(f"Baseline mismatch: {len(baseline_orders)} vs {len(frozen)}; extra={extra.to_dict('records')[:10]}; missing={missing.to_dict('records')[:10]}", flush=True)
        raise RuntimeError("Baseline selected orders differ from frozen 46-session ledger")
    print(f"Baseline selected: {len(baseline_orders)}", flush=True)
    experiment_orders = select(signals, extra=True)
    added_orders = experiment_orders.loc[~experiment_orders.sid.isin(baseline_orders.sid)]
    if len(experiment_orders) != len(baseline_orders) + len(added_orders) or not added_orders.setup_id.eq("0951_LONG").all():
        raise RuntimeError("Unexpected selected-order change outside 09:50 LONG")
    print(f"Added 09:50 LONG orders: {len(added_orders)}", flush=True)
    paths = paths_for(experiment_orders, bundle, provenance)
    baseline, _ = replay.simulate(baseline_orders, paths, DAYS)
    compare_to_frozen(baseline, frozen)
    print("Full 46-session baseline execution matches frozen trades and per-trade net P&L", flush=True)
    experiment, _ = replay.simulate(experiment_orders, paths, DAYS)
    base_metric, test_metric = metrics(baseline), metrics(experiment)
    base_daily = baseline.loc[baseline.portfolio_executed.eq(True)].groupby("day").portfolio_net_profit_rupees.sum()
    test_daily = experiment.loc[experiment.portfolio_executed.eq(True)].groupby("day").portfolio_net_profit_rupees.sum()
    daily = pd.DataFrame(dict(day=[str(d) for d in DAYS],
                              baseline_net_pnl_rs=[float(base_daily.get(d, 0)) for d in DAYS],
                              with_0950_long_net_pnl_rs=[float(test_daily.get(d, 0)) for d in DAYS]))
    daily["delta_rs"] = daily.with_0950_long_net_pnl_rs - daily.baseline_net_pnl_rs
    added = experiment.loc[experiment.setup_id.eq("0951_LONG")].copy()
    OUT.mkdir(parents=True)
    baseline.to_csv(OUT / "baseline_trades.csv", index=False)
    experiment.to_csv(OUT / "with_0950_long_trades.csv", index=False)
    daily.to_csv(OUT / "daily_comparison.csv", index=False)
    added.to_csv(OUT / "added_0950_long_orders.csv", index=False)
    setup = asdict(contract._ACTIVE_BY_ID["0951_LONG"])
    result = dict(period_start=str(DAYS[0]), period_end=str(DAYS[-1]), sessions=len(DAYS),
                  excluded_session="2026-10-01: incomplete immutable input",
                  source="same verified sealed feature pool and extension snapshots as accepted G3",
                  change="Add 09:50 LONG by cloning 09:55 LONG; existing slots unchanged",
                  setup=setup, baseline=base_metric, experiment=test_metric,
                  delta={key: test_metric[key]-base_metric[key] for key in base_metric},
                  baseline_frozen_parity=True, production_modified=False,
                  qualification="Exploratory reused history; not new out-of-sample validation")
    (OUT / "summary.json").write_text(json.dumps(result, indent=2, allow_nan=False), encoding="utf-8")
    evidence = dict(created_at=datetime.now(contract.IST).isoformat(),
                    code_sha256=sha(Path(__file__)), sealed_dataset_manifest_sha256=sha(bundle["root"] / "dataset_manifest.json"),
                    study_provenance_sha256=sha(STUDY / "provenance.json"),
                    frozen_trades_sha256=sha(DEFAULT_FROZEN / "trades.csv"),
                    source_signal_rows=len(signals), source_days=len(DAYS),
                    research_files={p.name: sha(p) for p in OUT.iterdir() if p.is_file()})
    (OUT / "source_evidence.json").write_text(json.dumps(evidence, indent=2), encoding="utf-8")
    print(json.dumps(result, indent=2), flush=True)
    print(OUT, flush=True)


if __name__ == "__main__":
    main()
