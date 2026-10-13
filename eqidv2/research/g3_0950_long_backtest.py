"""Backtesting-only G3 variant with the researched 09:50 LONG setup.

Rebuilds selections from verified pre-selection features, then runs the normal
G3 minute-entry, staged-stop, cost, and portfolio simulator. This module never
changes the production contract/configuration or publishes a live pointer.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
from contextlib import contextmanager
from dataclasses import asdict, replace
from datetime import date, datetime
from pathlib import Path

import numpy as np
import pandas as pd

import fno_v13_replay_source as source
import fno_v13_v10_g_2_extend_results as ext
from research import g3_v2_contract as contract
import fno_v13_v10_g_3_daily_replay as replay
from research.g3_freeze import DEFAULT_FROZEN
from research.g3_six_confirmation_variants import _extension_pool


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG = ROOT / "configs/strategies/v13_v10_g_3_backtest_0950_long_v1.json"
PREFERRED_CONFIG = ROOT / "configs/strategies/v13_v10_g_3_backtest_0950_long_target075_v1.json"
APPROVED_TARGETS = {
    "V13-V10-G-3-BT-0950-LONG-V1": 1.0,
    "V13-V10-G-3-BT-0950-LONG-T075-V1": 0.75,
}
DEFAULT_HISTORY_PROVENANCE = Path(
    r"C:\TradingData\eqidv2\fno_oi\strategy_research\v13_corrected_v10_g_3"
    r"\run_20261008_six_confirmation_variants\provenance.json"
)
DEFAULT_RECENT_PROVENANCE = Path(
    r"C:\TradingData\eqidv2\fno_oi\strategy_research\v13_v10_g_3_0950_long_20261012"
    r"\joint_0950_5m_1m\candidate_outcomes_provenance.json"
)


def sha(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def configuration(path: Path) -> dict:
    config = json.loads(Path(path).read_text(encoding="utf-8"))
    if (config.get("variant_id") not in APPROVED_TARGETS
        or config.get("scope") != "BACKTEST_ONLY"
        or config.get("base_strategy") != "V13-V10-G-3"
        or config.get("execution_authority") is not False
        or config.get("preserve_all_existing_g3_setups") is not True):
        raise ValueError("This runner accepts only the isolated G3 backtest variant")
    setup = config["new_setup"]
    fixed = dict(signal_end="09:50", confirmation_end="09:51", side="LONG",
        mode="FILTERED", max_entries=1, picker="max_liquidity",
        require_green_candle=True, require_close_above_signal_close=True,
        initial_stop_pct=1.25, tightened_stop_pct=1.,
        tighten_after_minutes=120, target_pct=APPROVED_TARGETS[config["variant_id"]])
    if any(setup.get(key) != value for key, value in fixed.items()):
        raise ValueError("09:50 LONG identity, quality, or agreed exit rule changed")
    fields = ("price_change_pct", "oi_change_pct", "five_minute_volume_ratio_min",
        "one_minute_body_ratio_min", "one_minute_upper_wick_ratio_max",
        "one_minute_volume_ratio_min")
    if any(not math.isfinite(float(setup[name])) or float(setup[name]) < 0 for name in fields):
        raise ValueError("All configured thresholds must be nonnegative and finite")
    if not (0 <= setup["one_minute_body_ratio_min"] <= 1
            and 0 <= setup["one_minute_upper_wick_ratio_max"] <= 1):
        raise ValueError("Body and wick ratios must lie in [0, 1]")
    return config


@contextmanager
def backtest_setup(config: dict):
    """Install the extra setup temporarily, scoped to research selection only."""
    old = (contract.CORE_SETUPS, contract.ACTIVE_SETUPS,
           dict(contract._CORE_BY_ID), dict(contract._ACTIVE_BY_ID),
           dict(contract.MIN_CONFIRMATION_VOLUME_BY_SETUP))
    spec = config["new_setup"]
    donor = contract._ACTIVE_BY_ID["0956_LONG"]
    setup = replace(donor, signal_end="09:50", confirmation_end="09:51",
        price_change_pct=float(spec["price_change_pct"]),
        oi_change_pct=float(spec["oi_change_pct"]),
        volume_ratio=float(spec["five_minute_volume_ratio_min"]),
        body_ratio=float(spec["one_minute_body_ratio_min"]),
        max_wick_ratio=float(spec["one_minute_upper_wick_ratio_max"]),
        stop_pct=float(spec["initial_stop_pct"]), target_pct=float(spec["target_pct"]))
    # V2 already contains this setup. Isolated studies replace it temporarily;
    # they must never append a duplicate or relax unrelated LONG thresholds.
    def replace_setup(setups):
        if any(s.setup_id == setup.setup_id for s in setups):
            return tuple(setup if s.setup_id == setup.setup_id else s for s in setups)
        return (*setups, setup)
    contract.CORE_SETUPS = replace_setup(contract.CORE_SETUPS)
    contract.ACTIVE_SETUPS = replace_setup(contract.ACTIVE_SETUPS)
    contract._CORE_BY_ID[setup.setup_id] = setup
    contract._ACTIVE_BY_ID[setup.setup_id] = setup
    contract.MIN_CONFIRMATION_VOLUME_BY_SETUP[setup.setup_id] = float(spec["one_minute_volume_ratio_min"])
    try:
        yield setup
    finally:
        (contract.CORE_SETUPS, contract.ACTIVE_SETUPS, core, active, volume) = old
        contract._CORE_BY_ID.clear()
        contract._CORE_BY_ID.update(core)
        contract._ACTIVE_BY_ID.clear()
        contract._ACTIVE_BY_ID.update(active)
        contract.MIN_CONFIRMATION_VOLUME_BY_SETUP.clear()
        contract.MIN_CONFIRMATION_VOLUME_BY_SETUP.update(volume)


def normalize_signals(signals: pd.DataFrame) -> pd.DataFrame:
    work = signals.copy()
    for name in ("ema9", "ema20", "ema50"):
        original = pd.to_numeric(work[name], errors="coerce") if name in work else pd.Series(np.nan, index=work.index)
        fallback = (pd.to_numeric(work[f"v9_5m_{name}"], errors="coerce")
                    if f"v9_5m_{name}" in work else pd.Series(np.nan,index=work.index))
        work[name] = original.fillna(fallback)
    count = pd.to_numeric(work.source_1m_count, errors="coerce") if "source_1m_count" in work else pd.Series(np.nan, index=work.index)
    work["source_1m_count"] = count.fillna(5)
    signal = pd.to_datetime(work.signal_ts, utc=True).dt.tz_convert(contract.IST)
    work["signal_end"] = work.signal_end.fillna(signal.dt.strftime("%H:%M")) if "signal_end" in work else signal.dt.strftime("%H:%M")
    for name, fallback in (("v9_5m_feature_ts", "signal_ts"),
                           ("v9_feature_available_ts", "confirmation_ts")):
        work[name] = work[name].fillna(work[fallback]) if name in work else work[fallback]
    return work


def _strict_extension(day: date, run: Path, snapshot: Path, sid_start: int) -> pd.DataFrame:
    raw = _extension_pool(day, run, snapshot)
    nifty = pd.to_numeric(raw.nifty_first_bar_return_pct, errors="coerce").dropna().unique()
    if len(nifty) != 1:
        raise RuntimeError(f"Missing exact NIFTY context for {day}")
    strict = source._strict_signals(raw, float(nifty[0]))
    if "sid" not in strict:
        raise RuntimeError(f"Missing native source IDs for {day}")
    strict["sid"] = pd.to_numeric(strict.sid,errors="raise").astype(int)+sid_start
    return normalize_signals(strict)


def load_sources(history_provenance: Path, recent_provenance: Path,
                 october12_run: Path) -> tuple[pd.DataFrame, list[date], dict, dict]:
    """Read all accepted 46 sessions and later completed G3 source pools."""
    bundle = replay.load_sealed_source()
    history = json.loads(Path(history_provenance).read_text(encoding="utf-8"))
    recent = json.loads(Path(recent_provenance).read_text(encoding="utf-8"))
    expected = [str(day) for day in bundle["days"]] + [item["day"] for item in history["extensions"]]
    if history["days"] != expected or len(expected) != 46:
        raise RuntimeError("Accepted historical G3 source days changed")
    source_parts = [normalize_signals(bundle["signals"])]
    print(f"Verified sealed G3 source: {len(bundle['days'])} sessions",flush=True)
    snapshots: dict[date, Path] = {}
    source_evidence = dict(sealed_dataset_manifest=str(bundle["root"] / "dataset_manifest.json"),
        sealed_dataset_manifest_sha256=sha(bundle["root"] / "dataset_manifest.json"),
        history_provenance=str(history_provenance),history_provenance_sha256=sha(history_provenance),
        recent_provenance=str(recent_provenance),recent_provenance_sha256=sha(recent_provenance))
    for n, item in enumerate(history["extensions"]):
        day = date.fromisoformat(item["day"])
        snapshot = Path(item["snapshot"])
        manifest = snapshot / "snapshot_manifest.json"
        if sha(manifest) != history["verified_snapshot_manifest_hashes"][str(manifest)]:
            raise RuntimeError(f"Historical snapshot drift: {day}")
        source_parts.append(_strict_extension(day,Path(item["run"]),snapshot,1_000_000+n*10_000))
        snapshots[day] = snapshot
        print(f"Verified historical extension: {day}",flush=True)
    recent_sources = recent["recent_sources"]
    for n, day in enumerate((date(2026,10,8),date(2026,10,9))):
        entry = recent_sources[str(day)]
        ledger = Path(entry["feature_ledger"])
        manifest = Path(entry["snapshot_manifest"])
        if sha(ledger) != entry["feature_ledger_sha256"] or sha(manifest) != entry["snapshot_manifest_sha256"]:
            raise RuntimeError(f"Later G3 source drift: {day}")
        source_parts.append(_strict_extension(day,ledger.parent,manifest.parent,2_000_000+n*10_000))
        snapshots[day] = manifest.parent
        print(f"Verified later source: {day}",flush=True)
    daily_run = Path(october12_run)
    daily_manifest_path = daily_run / "run_manifest.json"
    daily_manifest = json.loads(daily_manifest_path.read_text(encoding="utf-8"))
    if (daily_manifest.get("complete") is not True or daily_manifest.get("session_date") != "2026-10-12"
        or daily_manifest.get("execution_authority") is not False):
        raise RuntimeError("October 12 full G3 source replay is incomplete")
    candidate_path = daily_run / daily_manifest["artifacts"]["candidate_signals"]["path"]
    if sha(candidate_path) != daily_manifest["artifacts"]["candidate_signals"]["sha256"]:
        raise RuntimeError("October 12 G3 candidate pool artifact drift")
    source_manifest_path = daily_run / "source_manifest.json"
    source_manifest = json.loads(source_manifest_path.read_text(encoding="utf-8"))
    snapshot_manifest = Path(source_manifest["snapshot_manifest"])
    if sha(snapshot_manifest) != source_manifest["snapshot_manifest_sha256"]:
        raise RuntimeError("October 12 raw snapshot drift")
    current = pd.read_csv(candidate_path)
    current["sid"] = np.arange(3_000_000,3_000_000+len(current))
    source_parts.append(normalize_signals(current))
    snapshots[date(2026,10,12)] = snapshot_manifest.parent
    print("Verified full October 12 source",flush=True)
    source_evidence["october12_full_replay"] = str(daily_run)
    source_evidence["october12_run_manifest_sha256"] = sha(daily_manifest_path)
    source_evidence["october12_candidate_signals_sha256"] = sha(candidate_path)
    all_signals = pd.concat(source_parts,ignore_index=True,sort=False)
    if all_signals.sid.duplicated().any():
        raise RuntimeError("Source signal IDs overlap")
    days = [date.fromisoformat(value) for value in history["days"]] + [date(2026,10,8),date(2026,10,9),date(2026,10,12)]
    if len(days) != 49 or days != sorted(set(days)):
        raise RuntimeError("Expected exactly 49 ordered complete sessions")
    return all_signals,days,dict(bundle=bundle,snapshots=snapshots),source_evidence


def selected_paths(orders: pd.DataFrame, sources: dict) -> dict:
    bundle = sources["bundle"]
    by_day = pd.to_datetime(orders.day).dt.date
    sealed_orders = orders.loc[by_day.isin(bundle["days"])]
    required = set(sealed_orders.sid.astype(int))
    paths = {}
    with np.load(bundle["root"] / "paths.npz",allow_pickle=False) as archive:
        for name in archive.files:
            sid_text,field = name.split("_",1)
            sid = int(sid_text)
            if sid in required:
                paths.setdefault(sid,{})[field] = archive[name]
    if set(paths) != required:
        raise RuntimeError(f"Sealed execution paths missing: {sorted(required-set(paths))}")
    for day,snapshot in sources["snapshots"].items():
        subset = orders.loc[by_day.eq(day)].copy()
        if len(subset):
            paths.update(ext._selected_paths(subset,day,snapshot))
    if set(paths) != set(orders.sid.astype(int)):
        raise RuntimeError("Full-session execution paths are incomplete")
    return paths


def metrics(ledger: pd.DataFrame) -> dict:
    filled = ledger.loc[ledger.portfolio_executed.eq(True)]
    pnl = filled.portfolio_net_profit_rupees.astype(float)
    return dict(selected_orders=len(ledger),fills=len(filled),wins=int(pnl.gt(0).sum()),
        losses=int(pnl.lt(0).sum()),win_rate_pct=100*float(pnl.gt(0).mean()) if len(pnl) else None,
        net_pnl_rs=float(pnl.sum()))


def frozen_parity(ledger: pd.DataFrame) -> None:
    frozen = pd.read_csv(DEFAULT_FROZEN / "trades.csv")
    left = ledger.set_index("sid").sort_index()
    right = frozen.set_index("sid").sort_index()
    if len(left) != len(right) or not left.index.equals(right.index):
        raise RuntimeError("Accepted G3 baseline selected-order parity failed")
    for name in ("filled","portfolio_executed","exit_reason"):
        if not left[name].astype(str).equals(right[name].astype(str)):
            raise RuntimeError(f"Accepted G3 baseline trade drift: {name}")
    if not np.allclose(left.portfolio_net_profit_rupees.astype(float),
                       right.portfolio_net_profit_rupees.astype(float),rtol=0,atol=1e-6):
        raise RuntimeError("Accepted G3 baseline P&L drift")


def _simulate_reference(orders, paths, days):
    # This archived study selects with the pinned V2 contract. The shared
    # simulator's execution constants must remain identical before reusing it.
    names = ("CAPITAL_PER_ENTRY_RS", "LEVERAGE", "PORTFOLIO_CAPITAL_RS", "MAX_POSITIONS",
             "ROUND_TRIP_COST_BPS", "ENTRY_EXPIRY_MINUTES", "SQUARE_OFF",
             "INITIAL_STOP_PCT", "TIGHTENED_STOP_PCT", "TIGHTEN_AFTER_MINUTES")
    if any(getattr(contract, name) != getattr(replay.contract, name) for name in names):
        raise ValueError("Archived V2 simulation requires its original execution constants")
    ledger, metrics = replay.simulate(orders, paths, days)
    ledger["strategy_version"] = contract.STRATEGY_VERSION
    ledger["strategy_fingerprint"] = contract.strategy_fingerprint()
    return ledger, metrics


def run(config_path: Path,output_dir: Path,history_provenance: Path,
        recent_provenance: Path,october12_run: Path) -> dict:
    config = configuration(config_path)
    replay.verify_production_contract()
    output = Path(output_dir).resolve()
    if output.exists() and any(output.iterdir()):
        raise FileExistsError(f"Backtest output must be new or empty: {output}")
    signals,days,sources,evidence = load_sources(history_provenance,recent_provenance,october12_run)
    # Compare the same original fourteen setups against the added slot even
    # after production promotion. Saved historical evidence stays immutable.
    baseline_pool = signals.loc[~(signals.signal_end.eq("09:50") & signals.side.eq("LONG"))]
    baseline_orders = contract.select_orders(baseline_pool)
    first46 = baseline_orders.loc[pd.to_datetime(baseline_orders.day).dt.date.isin(days[:46])]
    if set(first46.sid.astype(int)) != set(pd.read_csv(DEFAULT_FROZEN / "trades.csv").sid.astype(int)):
        raise RuntimeError("Original G3 baseline selections no longer match frozen history")
    with backtest_setup(config) as setup:
        pool = signals.loc[signals.signal_end.eq("09:50") & signals.side.eq("LONG")].copy()
        audit = contract.selection_audit(pool)
        new_orders = audit.loc[audit.setup_id.eq(setup.setup_id) & audit.v9_selected.eq(True)].copy()
        if len(new_orders) != len(set(pd.to_datetime(new_orders.day).dt.date)):
            raise RuntimeError("09:50 backtest selected multiple orders in one session")
        setup_record = asdict(setup)
    all_orders = pd.concat((baseline_orders,new_orders),ignore_index=True)
    if all_orders.sid.duplicated().any() or all_orders.signal_id.duplicated().any():
        raise RuntimeError("Variant introduced duplicate order identities")
    paths = selected_paths(all_orders,sources)
    print(f"Verified minute execution paths for {len(all_orders)} orders",flush=True)
    baseline,_ = _simulate_reference(baseline_orders,paths,days)
    frozen_parity(baseline.loc[pd.to_datetime(baseline.day).dt.date.isin(days[:46])])
    print("Frozen 46-session baseline trade parity passed",flush=True)
    with backtest_setup(config):
        variant,_ = _simulate_reference(all_orders,paths,days)
    old = variant.loc[variant.sid.isin(baseline_orders.sid)]
    comparison = old.set_index("sid").sort_index()
    original = baseline.set_index("sid").sort_index()
    if not comparison.index.equals(original.index):
        raise RuntimeError("Existing G3 order identity changed")
    for name in ("filled","portfolio_executed","exit_reason"):
        if not comparison[name].astype(str).equals(original[name].astype(str)):
            raise RuntimeError(f"Existing G3 trade changed: {name}")
    if not np.allclose(comparison.portfolio_net_profit_rupees.astype(float),
                       original.portfolio_net_profit_rupees.astype(float),rtol=0,atol=1e-6):
        raise RuntimeError("Existing G3 P&L changed")
    added = variant.loc[variant.sid.isin(new_orders.sid)].copy()
    baseline_metric,variant_metric,added_metric = metrics(baseline),metrics(variant),metrics(added)
    if abs(variant_metric["net_pnl_rs"]-baseline_metric["net_pnl_rs"]-added_metric["net_pnl_rs"])>1e-6:
        raise RuntimeError("Variant P&L is not baseline plus new slot")
    print("Existing G3 trades unchanged; variant portfolio parity passed",flush=True)
    def day_subset(frame, subset):
        return metrics(frame.loc[pd.to_datetime(frame.day).dt.date.isin(subset)])
    daily = []
    for day in days:
        b = baseline.loc[pd.to_datetime(baseline.day).dt.date.eq(day)]
        v = variant.loc[pd.to_datetime(variant.day).dt.date.eq(day)]
        n = added.loc[pd.to_datetime(added.day).dt.date.eq(day)]
        bm,vm,nm = metrics(b),metrics(v),metrics(n)
        daily.append(dict(day=str(day),baseline_selected=bm["selected_orders"],
            baseline_fills=bm["fills"],baseline_wins=bm["wins"],baseline_net_pnl_rs=bm["net_pnl_rs"],
            variant_selected=vm["selected_orders"],variant_fills=vm["fills"],
            variant_wins=vm["wins"],variant_net_pnl_rs=vm["net_pnl_rs"],
            added_0950_selected=nm["selected_orders"],added_0950_fills=nm["fills"],
            added_0950_wins=nm["wins"],added_0950_net_pnl_rs=nm["net_pnl_rs"],
            delta_net_pnl_rs=vm["net_pnl_rs"]-bm["net_pnl_rs"]))
    summary = dict(variant_id=config["variant_id"],scope="BACKTEST_ONLY",
        execution_authority=False,created_at_ist=datetime.now(contract.IST).isoformat(),
        sessions=len(days),first_date=str(days[0]),last_date=str(days[-1]),
        excluded_session="2026-10-01: incomplete immutable input",
        selected_setup=setup_record,confirmation_volume_ratio_min=config["new_setup"]["one_minute_volume_ratio_min"],
        baseline=baseline_metric,with_0950_long=variant_metric,added_0950_long=added_metric,
        first46=dict(baseline=day_subset(baseline,days[:46]),
                     with_0950_long=day_subset(variant,days[:46]),
                     added_0950_long=day_subset(added,days[:46])),
        recent3=dict(baseline=day_subset(baseline,days[46:]),
                     with_0950_long=day_subset(variant,days[46:]),
                     added_0950_long=day_subset(added,days[46:])),
        frozen_baseline_parity=True,existing_g3_trades_unchanged=True,
        qualification="Retrospective parameter selection; observed win rate is not independent proof of future performance",
        sources=evidence,config_path=str(Path(config_path).resolve()),config_sha256=sha(config_path))
    output.mkdir(parents=True,exist_ok=True)
    artifacts = dict(baseline_orders=baseline_orders,selection_audit_0950_long=audit,
        added_0950_orders=new_orders,baseline_trades=baseline,
        variant_trades=variant,added_0950_trades=added,daily_results=pd.DataFrame(daily))
    for name,frame in artifacts.items():
        frame.to_csv(output / f"{name}.csv",index=False)
    (output / "config.json").write_text(json.dumps(config,indent=2)+"\n",encoding="utf-8")
    (output / "summary.json").write_text(json.dumps(summary,indent=2,allow_nan=False)+"\n",encoding="utf-8")
    manifest = dict(variant_id=config["variant_id"],execution_authority=False,
        code=str(Path(__file__).resolve()),code_sha256=sha(Path(__file__)),
        files={path.name:sha(path) for path in output.iterdir() if path.is_file()})
    (output / "manifest.json").write_text(json.dumps(manifest,indent=2)+"\n",encoding="utf-8")
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config",type=Path,default=DEFAULT_CONFIG)
    parser.add_argument("--output-dir",type=Path,required=True)
    parser.add_argument("--history-provenance",type=Path,default=DEFAULT_HISTORY_PROVENANCE)
    parser.add_argument("--recent-provenance",type=Path,default=DEFAULT_RECENT_PROVENANCE)
    parser.add_argument("--october12-run",type=Path,required=True)
    args = parser.parse_args()
    result = run(args.config,args.output_dir,args.history_provenance,
                 args.recent_provenance,args.october12_run)
    print(json.dumps({key:result[key] for key in ("sessions","baseline","with_0950_long",
        "added_0950_long","recent3","frozen_baseline_parity","existing_g3_trades_unchanged")},
        indent=2),flush=True)
    print(Path(args.output_dir).resolve(),flush=True)


if __name__ == "__main__":
    main()
