"""Independent G3 after-close replay from raw or sealed pre-selection inputs.

Neither completed G trades nor frozen G3 trades are execution inputs. Historical
feature bundles must contain the complete pre-quota candidate pool and sealed
minute paths; their manifest hashes and pool completeness are checked first.
"""
from __future__ import annotations

import argparse
import json
from datetime import date, datetime, time as day_time
from pathlib import Path

import numpy as np
import pandas as pd

import fno_v13_replay_source as source
from research.g3_backtest_profile import contract, CONFIG_PATH
from eqidv2_runtime_paths import runtime_dir

STRATEGY = "V13-V10-G-3"
SCHEMA_VERSION = "fno_g3_production_run_v1"
PRODUCTION_CONFIG_PATH = CONFIG_PATH
DEFAULT_SEALED_SOURCE = runtime_dir("fno_oi", "strategy_research", "v13_v10_g_full_history",
    "run_20260925_cutoff_corrected_through_20260923", "dataset")
DataRoots = source.DataRoots
sha256 = source._sha
json_ready = source._json_ready


def verify_production_contract() -> dict:
    contract.load_contract(PRODUCTION_CONFIG_PATH)
    return dict(path=str(PRODUCTION_CONFIG_PATH), sha256=sha256(PRODUCTION_CONFIG_PATH),
                strategy_fingerprint=contract.strategy_fingerprint(),
                accepted_frozen_config_sha256=contract.FROZEN_CONFIG_SHA256)


def require_closed_session(day: date, now: datetime | None = None) -> None:
    now = now or datetime.now(contract.IST)
    if day > now.date() or (day == now.date() and now.time().replace(tzinfo=None) < day_time(15, 35)):
        raise ValueError("Production replay requires a closed session (15:35 IST or later)")


def atomic_json(path: Path, payload: dict) -> None:
    source.common.atomic_write_json(path, json_ready(payload))


def artifact(path: Path, run_dir: Path) -> dict:
    return dict(path=path.resolve().relative_to(run_dir.resolve()).as_posix(), sha256=sha256(path))


def _csv(run_dir: Path, name: str, frame: pd.DataFrame) -> dict:
    path = run_dir / f"{name}.csv"
    if not len(frame.columns):
        frame = pd.DataFrame(columns=["day"])
    source.common.atomic_write_csv(frame, path)
    return artifact(path, run_dir)


def _code_provenance() -> list[dict]:
    import fno_v13_v10_g_2_backtest as simulator
    modules = [Path(__file__), Path(source.__file__), Path(contract.__file__), Path(contract.late_setups.__file__),
               Path(__file__).resolve().parent / "research/g3_backtest_profile.py",
               Path(source.opening_inputs.__file__), Path(__file__).with_name("fno_v13_v10_g_3_opening_oi.py"),
               Path(simulator.__file__), Path(simulator.g.v9.__file__),
               Path(simulator.g.v9.v5.__file__), Path(simulator.g.v9.v6.__file__),
               Path(simulator.g.r.__file__), Path(source.hybrid.__file__), Path(source.provenance.__file__),
               Path(source.features.__file__), Path(source.common.__file__), PRODUCTION_CONFIG_PATH,
               Path(__file__).with_name("fno_v13_v10_g_3_policy.py"),
               Path(__file__).with_name("fno_v13_v10_g_identity.py"),
               Path(__file__).with_name("eqidv2_runtime_paths.py"),
               Path(__file__).with_name("backtesting_result_v13_v10_g_3_daily.py")]
    return [dict(path=str(path.resolve()), sha256=sha256(path)) for path in modules]


def simulate(orders: pd.DataFrame, paths: dict, days: list[date]) -> tuple[pd.DataFrame, dict]:
    """Use the accepted minute simulator and fixed-capital portfolio clock."""
    import fno_v13_v10_g_2_backtest as simulator
    base = simulator.g.v9.V9Config(portfolio_capital_rupees=contract.PORTFOLIO_CAPITAL_RS,
        capital_per_entry_rupees=contract.CAPITAL_PER_ENTRY_RS, leverage_factor=contract.LEVERAGE,
        max_positions=contract.MAX_POSITIONS, cost_bps=contract.ROUND_TRIP_COST_BPS)
    if len(orders):
        simulator.g.v9.validate_paths(orders, paths)
        trades = simulator.simulate_staged(orders, paths, cost_bps=base.cost_bps,
            max_entry_delay_minutes=contract.ENTRY_EXPIRY_MINUTES)
    else:
        trades = orders.copy()
    for column, default in (("filled", False), ("entry_ts", pd.NaT), ("exit_ts", pd.NaT),
                            ("net_return_pct", np.nan), ("gross_return_pct", np.nan), ("cost_pct", np.nan)):
        if column not in trades:
            trades[column] = default
    trades = simulator.g.v9.v5.apply_fixed_capital_model(trades, base.capital_per_entry_rupees, base.leverage_factor)
    ledger, _ = simulator.g.v9.v6.apply_portfolio_constraints(trades, base.portfolio_config())
    for column in ("sid", "signal_id", "day", "hhmm", "side", "tradingsymbol", "setup_id", "configured_confirmation_end"):
        if column not in ledger:
            ledger[column] = pd.Series(index=ledger.index, dtype=object)
    if len(ledger) and (ledger.signal_id.duplicated().any() or not set(pd.to_datetime(ledger.day).dt.date).issubset(days)):
        raise ValueError("Replay identity/date escaped requested sessions")
    for name in ("entry_ts", "exit_ts"):
        stamps = pd.to_datetime(ledger[name], errors="coerce", utc=True).dt.tz_convert(contract.IST)
        if len(ledger) and not (stamps.isna() | stamps.dt.date.eq(pd.to_datetime(ledger.day).dt.date)).all():
            raise ValueError("Execution timestamp escaped its signal session")
    ledger["strategy"] = STRATEGY
    ledger["strategy_version"] = contract.STRATEGY_VERSION
    ledger["strategy_fingerprint"] = contract.strategy_fingerprint()
    metric = simulator.g.r.metric(ledger, days)
    metric.update(sessions=len(days), orders=metric["selected_orders"], fills=metric["trades"])
    metric["net_pnl_rupees"] = metric["net_profit_rupees"]
    return ledger, json_ready(metric)


def metrics_for_ledger(ledger: pd.DataFrame, days: list[date]) -> dict:
    import fno_v13_v10_g_2_backtest as simulator
    metric = simulator.g.r.metric(ledger, days)
    metric.update(sessions=len(days), orders=metric["selected_orders"], fills=metric["trades"])
    metric["net_pnl_rupees"] = metric["net_profit_rupees"]
    return json_ready(metric)


def _finish(run_dir: Path, *, days: list[date], role: str, source_kind: str,
            frames: dict[str, pd.DataFrame], provenance: dict, coverage: dict,
            complete: bool, paths: dict | None = None, ledger: pd.DataFrame | None = None) -> dict:
    contract_proof = verify_production_contract()
    run_dir.mkdir(parents=True, exist_ok=True)
    if (run_dir / "run_manifest.json").exists():
        raise FileExistsError("A sealed production run cannot be overwritten")
    artifacts = {name: _csv(run_dir, name, frame) for name, frame in frames.items()}
    metric = None
    if complete:
        if ledger is None:
            ledger, metric = simulate(frames["selected_orders"], paths or {}, days)
        else:
            # Historical revisions are independent cohorts. Never relabel an
            # old ledger with the active contract merely by sealing it again.
            for column, expected in (("strategy_version", contract.STRATEGY_VERSION),
                                     ("strategy_fingerprint", contract.strategy_fingerprint())):
                if column not in ledger or not ledger[column].eq(expected).all():
                    raise ValueError("Replay ledger contains another production strategy revision")
            metric = metrics_for_ledger(ledger, days)
        artifacts["portfolio_trades"] = _csv(run_dir, "portfolio_trades", ledger)
        daily = [dict(day=day.isoformat(), strategy=STRATEGY,
            strategy_version=contract.STRATEGY_VERSION, strategy_fingerprint=contract.strategy_fingerprint(),
            **metrics_for_ledger(ledger.loc[pd.to_datetime(ledger.day).dt.date.eq(day)], [day])) for day in days]
        artifacts["daily_results"] = _csv(run_dir, "daily_results", pd.DataFrame(daily))
    provenance.update(code=_code_provenance(), strategy_contract=contract.strategy_payload(), production_config=contract_proof)
    atomic_json(run_dir / "source_manifest.json", provenance)
    artifacts["source_manifest"] = artifact(run_dir / "source_manifest.json", run_dir)
    result = dict(schema_version=SCHEMA_VERSION, strategy=STRATEGY,
        strategy_version=contract.STRATEGY_VERSION, strategy_fingerprint=contract.strategy_fingerprint(),
        accepted_variant=contract.ACCEPTED_VARIANT, complete=complete,
        state=("SUCCESS_WITH_EXCLUDED_SESSIONS" if coverage.get("excluded_sessions") else "SUCCESS") if complete else "BLOCKED_INCOMPLETE_DATA", role=role,
        session_date=days[-1].isoformat() if days else None, days=[day.isoformat() for day in days],
        run_id=run_dir.name, created_at=datetime.now(contract.IST).isoformat(),
        source_kind=source_kind, execution_authority=False, metrics=metric, coverage=coverage,
        artifacts=artifacts)
    atomic_json(run_dir / "run_manifest.json", result)
    atomic_json(run_dir / "replay_result.json", {**result,
        "artifacts": {name: str((run_dir / item["path"]).resolve()) for name, item in artifacts.items()}})
    return result


def replay_day(day: date, output_dir: Path, *, roots: DataRoots | None = None,
               snapshot_root: Path | None = None) -> dict:
    """Offline reconstruction API; CLI/publisher enforce the closed-session gate.

    This function writes an isolated run only and never moves current pointers.
    Explicit synthetic dates remain supported for source/simulation tests.
    """
    if type(day) is not date:
        raise TypeError("An explicit date is required")
    verify_production_contract()
    output = Path(output_dir).resolve()
    if (output / "run_manifest.json").exists():
        raise FileExistsError("A sealed production run cannot be overwritten")
    snapshot = source.create_input_snapshot(day, snapshot_root or output / "input_snapshots", roots=roots)
    dataset = source.build_day_features(day, roots=snapshot.roots, signal_clocks=contract.SIGNAL_TO_CONFIRMATION)
    dataset["problems"][:0] = list(snapshot.problems)
    # The pure decision contract is causal. Stock-day OI exclusions above are
    # explicit retrospective data eligibility, never morning/live entry guards.
    audit = contract.selection_audit(dataset["signals"])
    orders = audit.loc[audit.v9_selected.eq(True)].copy()
    blockers = [p for p in dataset["problems"] if p["reason"] not in source.EXCLUDABLE_COVERAGE_REASONS]
    included = dataset["universe_count"] - len(dataset["excluded_stocks"])
    if included <= 0:
        blockers.append(dict(reason="NO_ELIGIBLE_STOCKS"))
    paths = source.selected_paths(orders, dataset["minutes"], day) if not blockers else {}
    complete = not blockers
    if complete:
        try:
            import fno_v13_v10_g_2_backtest as simulator
            simulator.g.v9.validate_paths(orders, paths)
        except RuntimeError as exc:
            complete = False
            blockers.append(dict(reason="INCOMPLETE_SELECTED_EXECUTION_PATH", detail=str(exc)))
    coverage = dict(problems=dataset["problems"], blocking_problems=blockers,
        opening_0920_exclusions=dataset.get("opening_0920_exclusions", []),
        opening_0920_policy="EXCLUDE_STOCK_FROM_0920_ONLY_AND_DISCLOSE; NO_OI_PROXY",
        excluded_stocks=dataset["excluded_stocks"], universe_count=dataset["universe_count"], included_stocks=included,
        eligibility_method="AFTER_CLOSE_STOCK_DAY_INPUT_ELIGIBILITY",
        missing_required_futures_oi_policy="EXCLUDE_ENTIRE_STOCK_DAY_AND_DISCLOSE_NOT_A_LIVE_CAUSAL_GATE")
    provenance = dict(source_kind="IMMUTABLE_RAW_MARKET_SNAPSHOT", session_date=day,
        snapshot_manifest=str(snapshot.manifest_path), snapshot_manifest_sha256=sha256(snapshot.manifest_path),
        snapshot_fingerprint=snapshot.fingerprint, input_sources=list(snapshot.sources),
        selection_input="COMPLETE_PRE_QUOTA_STRICT_CANDIDATE_POOL", completed_trade_outputs_consumed=False)
    return _finish(output, days=[day], role="production_daily", source_kind=provenance["source_kind"],
        frames=dict(candidate_signals=dataset["signals"], selection_audit=audit, selected_orders=orders,
            feature_ledger=dataset["feature_ledger"], coverage=pd.DataFrame(dataset["coverage_rows"])),
        provenance=provenance, coverage=coverage, complete=complete, paths=paths)


def load_sealed_source(root: Path = DEFAULT_SEALED_SOURCE) -> dict:
    """Verify all artifacts, then prove active-clock pool is pre-G threshold."""
    verify_production_contract()
    root = Path(root).resolve()
    manifest_path = root / "dataset_manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    required = {"signals.parquet", "all_5m_features.parquet", "paths.npz", "source_session_eligibility.csv"}
    if not required.issubset(manifest.get("output_sha256", {})):
        raise ValueError("Sealed source lacks complete feature/path/eligibility hashes")
    verified = []
    for name, expected in manifest["output_sha256"].items():
        path = (root / name).resolve()
        if not path.is_relative_to(root) or sha256(path) != expected:
            raise ValueError(f"Sealed feature source hash mismatch: {name}")
        verified.append(dict(path=str(path), sha256=expected))
    signals = pd.read_parquet(root / "signals.parquet")
    raw = pd.read_parquet(root / "all_5m_features.parquet")
    days = [date.fromisoformat(value) for value in manifest["days"]]
    eligibility = pd.read_csv(root / "source_session_eligibility.csv")
    eligible = eligibility.loc[eligibility.eligible.astype(str).str.lower().eq("true")]
    if not set(days).issubset(set(pd.to_datetime(eligible.day).dt.date)):
        raise ValueError("Sealed manifest contains an ineligible session")
    clocks = {int(clock.replace(":", "")) for clock in contract.SIGNAL_TO_CONFIRMATION}
    if "opening_0920_status" not in raw:
        # Validate an old source against the slots it actually supported.
        # It can supply eligibility/provenance, never the full V3 replay.
        clocks.discard(920)
    raw = raw.loc[raw.hhmm_int.isin(clocks)].copy()
    raw["day"] = pd.to_datetime(raw.day).dt.date
    signals["day"] = pd.to_datetime(signals.day).dt.date
    def keys(frame):
        return set(zip(pd.to_datetime(frame.signal_ts, utc=True).astype(str), frame.tradingsymbol.astype(str), frame.side.astype(str)))
    checks = []
    for day in days:
        observed = signals.loc[signals.day.eq(day) & signals.hhmm_int.isin(clocks)]
        contexts = pd.to_numeric(observed.nifty_first_bar_return_pct, errors="coerce").dropna().unique()
        if len(contexts) != 1:
            raise ValueError(f"Sealed NIFTY context missing/ambiguous: {day}")
        expected = source._strict_signals(raw.loc[raw.day.eq(day)], float(contexts[0]))
        missing, extra = keys(expected) - keys(observed), keys(observed) - keys(expected)
        if missing or extra:
            raise ValueError(f"Sealed pre-quota candidate pool mismatch {day}: missing={len(missing)}, extra={len(extra)}")
        checks.append(dict(day=day.isoformat(), reconstructed_strict_candidates=len(expected), sealed_candidates=len(observed)))
    if signals.sid.duplicated().any():
        raise ValueError("Sealed source contains duplicate native path IDs")
    opening_rows = raw.loc[raw.hhmm_int.eq(920)]
    opening_complete = (bool(len(opening_rows)) and "opening_0920_status" in opening_rows
        and opening_rows.opening_0920_status.eq("VALID").all()
        and set(opening_rows.day) == set(days))
    return dict(root=root, signals=signals, days=days, eligibility=eligibility,
        opening_0920_complete=opening_complete,
        provenance=dict(source_kind="HASH_VERIFIED_SEALED_PRE_SELECTION_FEATURES", manifest_path=str(manifest_path),
            manifest_sha256=sha256(manifest_path), verified_artifacts=verified, pool_completeness_checks=checks,
            long_candidates_below_legacy_120=int(((signals.side == "LONG") & (signals.v9_1m_volume_ratio < 1.2)).sum()),
            completed_trade_outputs_consumed=False, candidate_filter="STRICT_BASE_ONLY_NO_G_CONFIRMATION_THRESHOLD_OR_QUOTA"))


def replay_sealed_days(bundle: dict, days: list[date], output_dir: Path) -> dict:
    if "09:20" in contract.SIGNAL_TO_CONFIRMATION and not bundle.get("opening_0920_complete"):
        raise ValueError("Sealed source lacks verified 09:20 opening inputs; reconstruct raw sessions for current G3")
    if not set(days).issubset(bundle["days"]):
        raise ValueError("Session not eligible in verified sealed source")
    signals = bundle["signals"].loc[bundle["signals"].day.isin(days)].copy()
    audit = contract.selection_audit(signals)
    orders = audit.loc[audit.v9_selected.eq(True)].copy()
    needed = set(orders.sid.astype(int))
    paths = {}
    with np.load(bundle["root"] / "paths.npz", allow_pickle=False) as archive:
        for name in archive.files:
            sid_text, field = name.split("_", 1)
            sid = int(sid_text)
            if sid in needed:
                paths.setdefault(sid, {})[field] = archive[name]
    if set(paths) != needed:
        raise ValueError("Selected sealed execution paths are incomplete")
    return _finish(Path(output_dir), days=days, role="cumulative_history", source_kind=bundle["provenance"]["source_kind"],
        frames=dict(candidate_signals=signals, selection_audit=audit, selected_orders=orders, feature_ledger=signals),
        provenance=dict(bundle["provenance"]), coverage=dict(eligible_days=[str(d) for d in days], blocking_problems=[]),
        complete=True, paths=paths)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", type=date.fromisoformat, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    require_closed_session(args.date)
    result = replay_day(args.date, args.output_dir)
    print(json.dumps(json_ready(result), indent=2))
    return 0 if result["complete"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
