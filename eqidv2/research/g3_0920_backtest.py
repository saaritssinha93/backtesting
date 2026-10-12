"""Compare additive 09:20 G-3 research orders with the published V2 backtest.

The existing selected trades are hash-verified and their frozen execution
outcomes are reused. New 09:20 orders alone are executed on real one-minute
cash paths; both arms then pass through the same fixed-capital and portfolio
allocator. No current-backtest pointer, production setup, or live file changes.
"""
from __future__ import annotations

import argparse
from datetime import date, datetime, time
import hashlib
import json
import math
import os
from pathlib import Path

import numpy as np
import pandas as pd

import fno_v13_replay_source as source
import fno_v13_v10_g_2_backtest as simulator
import fno_v13_v10_g_3_contract as contract
import fno_oi_common as common
import fno_oi_backtest_provenance as provenance
import fno_oi_hybrid_data as hybrid
from dashboard_flow_opening_data import _first_minute
from eqidv2_runtime_paths import RUNTIME_ROOT
from research.g3_0920_feature_export import _is_flagged, _read_cash
from research import g3_0920_history as history
from research.g3_0920_strategy import (
    RESEARCH_VERSION, selection_audit_0920,
)


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_POINTER = Path(RUNTIME_ROOT) / "backtesting_result_v13_v10_g_3" / "latest" / "current_backtest.json"
DEFAULT_FEATURES = ROOT / "outputs" / "g3_0920_research_history" / "features.parquet"
DEFAULT_OUTPUT_ROOT = Path(RUNTIME_ROOT) / "backtesting_result_v13_v10_g_3" / "research_0920" / "runs"
EXPECTED_BASELINE_VARIANT = "V13-V10-G-3-BT-0950-LONG-T075-V1"


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _bool_column(frame: pd.DataFrame, column: str) -> pd.Series:
    if column not in frame:
        raise ValueError(f"Missing boolean trade column: {column}")
    text = frame[column].astype(str).str.strip().str.lower()
    if not text.isin(("true", "false")).all():
        raise ValueError(f"Invalid boolean trade column: {column}")
    return text.eq("true")


def _days(frame: pd.DataFrame) -> list[date]:
    if "day" not in frame:
        raise ValueError("Expected day column")
    parsed = pd.to_datetime(frame.day, errors="coerce")
    if parsed.isna().any():
        raise ValueError("Invalid baseline session date")
    return sorted(set(parsed.dt.date))


def _feature_days(frame: pd.DataFrame) -> list[date]:
    if "session_date" not in frame:
        raise ValueError("Expected 09:20 feature session_date column")
    parsed = pd.to_datetime(frame.session_date, errors="coerce")
    if parsed.isna().any():
        raise ValueError("Invalid 09:20 feature session date")
    return sorted(set(parsed.dt.date))


def load_feature_evidence(features_path: Path, *, require_sealed: bool = True) -> tuple[pd.DataFrame, dict, dict]:
    """Bind a feature file to the exporter's coverage and artifact hashes."""
    features_path = Path(features_path).resolve()
    if features_path.suffix.lower() not in {".parquet", ".csv"}:
        raise ValueError("Expected a parquet or CSV research feature file")
    manifest_path = features_path.parent / "run_manifest.json"
    coverage_path = features_path.parent / "coverage.json"
    if not manifest_path.is_file():
        if require_sealed:
            raise ValueError(f"09:20 features lack a sealed run_manifest.json: {features_path.parent}")
        manifest = {}
    else:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        if manifest.get("schema") != "g3_0920_research_history_run_v1":
            raise ValueError("Unexpected 09:20 export manifest schema")
        hashes = manifest.get("artifacts_sha256", {})
        if not {features_path.name, "coverage.json"}.issubset(hashes):
            raise ValueError("09:20 manifest omitted the selected features or coverage")
        for name in hashes:
            if Path(name).name != name:
                raise ValueError("09:20 manifest artifact path escaped export directory")
            path = features_path.parent / name
            if not path.is_file() or sha256(path) != hashes[name]:
                raise ValueError(f"09:20 research artifact hash changed: {name}")
    if not coverage_path.is_file():
        raise ValueError("09:20 feature export lacks coverage.json")
    coverage = json.loads(coverage_path.read_text(encoding="utf-8"))
    features = pd.read_parquet(features_path) if features_path.suffix.lower() == ".parquet" else pd.read_csv(features_path)
    if features.empty or features.duplicated(["session_date", "symbol"]).any():
        raise ValueError("No unique dated 09:20 features to compare")
    if int(coverage.get("stock_rows", -1)) != len(features):
        raise ValueError("09:20 feature count differs from exporter coverage")
    included = coverage.get("included_days") or [coverage.get("session_date")]
    if sorted(included) != [str(day) for day in _feature_days(features)]:
        raise ValueError("09:20 feature dates differ from exporter coverage")
    if manifest and (manifest.get("requested_calendar_start") != coverage.get("requested_calendar_start")
                     or manifest.get("requested_calendar_through") != coverage.get("requested_calendar_through")):
        raise ValueError("09:20 manifest and coverage calendar differ")
    evidence = dict(manifest=str(manifest_path) if manifest else None,
                    manifest_sha256=sha256(manifest_path) if manifest else None,
                    coverage=str(coverage_path), coverage_sha256=sha256(coverage_path),
                    sealed=bool(manifest))
    return features, coverage, evidence


def validate_feature_universe(features: pd.DataFrame, data_root: Path,
                              coverage: dict | None = None) -> dict:
    """Require every feature day to match its entire persisted dated universe."""
    root = Path(data_root).resolve()
    if coverage and coverage.get("data_root") and Path(coverage["data_root"]).resolve() != root:
        raise ValueError("09:20 feature data root differs from the replay root")
    per_day = {item["session_date"]: item for item in (coverage or {}).get("per_day", [])}
    if coverage and coverage.get("per_day") and set(per_day) != {str(day) for day in _feature_days(features)}:
        raise ValueError("09:20 per-day coverage differs from feature dates")
    proofs = []
    sealed_material = None
    for day in _feature_days(features):
        key = str(day)
        universe_path = root / "fno_oi" / "universe" / f"near_month_{key}.parquet"
        part = features.loc[features.session_date.astype(str).eq(key)]
        report = per_day.get(key) or (coverage if coverage and coverage.get("session_date") == key else {})
        proof = report.get("universe", {})
        month = proof.get("contract_month_filter")
        if not month:
            raise ValueError(f"09:20 universe month is not recorded for {key}")
        if proof.get("mapping_source") == "SEALED_MONTHLY_DATASET":
            if sealed_material is None:
                sealed_material = history._sealed_material(root)
            mapped_rows, observed, _ = history.load_sealed_mapping(day, root, _material=sealed_material)
            required_proof = (
                "mapping_source", "session_date", "monthly_universe_date",
                "monthly_universe_file_sha256", "bundle_manifest_sha256",
                "dataset_manifest_sha256", "all_5m_features_sha256",
                "source_session_eligibility_sha256", "sealed_session_mapping_sha256",
                "sealed_session_mapping_count", "nifty_contract", "nifty_expiry",
                "nifty_source_sha256", "incomplete_dated_universe_sha256",
                "mapped_universe_sha256", "mapped_symbol_set_sha256")
            if any(proof.get(field) != observed.get(field) for field in required_proof):
                raise ValueError(f"Sealed 09:20 mapping proof changed for {key}")
        else:
            mapped, observed = provenance.load_backtest_universe(
                universe_path=universe_path, universe_date=day, contract_month_contains=month,
                require_persisted_mapping=True,
                expected_file_sha256=proof.get("file_sha256", ""),
                expected_universe_sha256=proof.get("universe_sha256", ""),
                expected_mapped_universe_sha256=proof.get("mapped_universe_sha256", ""),
                expected_mapped_symbol_set_sha256=proof.get("mapped_symbol_set_sha256", ""))
            mapped_rows = mapped.to_dict("records")
        expected = {(hybrid.resolve_backtest_equity_symbol(
            str(row["equity_symbol"]), root=root / "stocks_indicators_1min_eq"),
            str(row["futures_tradingsymbol"])) for row in mapped_rows}
        observed_pairs = set(zip(part.symbol.astype(str), part.contract.astype(str)))
        if (len(part) != len(expected) or len(observed_pairs) != len(part)
                or observed_pairs != expected or int(report.get("stock_rows", -1)) != len(part)):
            missing = sorted(expected - observed_pairs)
            extra = sorted(observed_pairs - expected)
            raise ValueError(f"Incomplete dated 09:20 stock universe {key}: "
                             f"missing={missing[:5]}, extra={extra[:5]}")
        proofs.append(dict(session_date=key, stock_rows=len(part),
                           mapping_source=observed.get("mapping_source", "PERSISTED_DATED_UNIVERSE_ONLY"),
                           universe_file_sha256=observed["file_sha256"],
                           mapped_universe_sha256=observed["mapped_universe_sha256"]))
    return dict(complete_dated_universes=len(proofs), per_day=proofs)


def _config():
    return simulator.g.v9.V9Config(
        portfolio_capital_rupees=contract.PORTFOLIO_CAPITAL_RS,
        capital_per_entry_rupees=contract.CAPITAL_PER_ENTRY_RS,
        leverage_factor=contract.LEVERAGE,
        max_positions=contract.MAX_POSITIONS,
        cost_bps=contract.ROUND_TRIP_COST_BPS,
    )


def allocate(source_trades: pd.DataFrame) -> pd.DataFrame:
    """Recompute fixed amounts and portfolio acceptance from raw trade returns."""
    if source_trades.index.has_duplicates:
        raise ValueError("Trade ledger indexes overlap")
    if source_trades.sid.duplicated().any() or source_trades.signal_id.duplicated().any():
        raise ValueError("Trade identities overlap")
    frame = source_trades.copy()
    frame["filled"] = _bool_column(frame, "filled")
    frame["day"] = pd.to_datetime(frame.day, errors="raise").dt.date.astype(str)
    base = _config()
    amounts = simulator.g.v9.v5.apply_fixed_capital_model(
        frame, base.capital_per_entry_rupees, base.leverage_factor)
    ledger, _ = simulator.g.v9.v6.apply_portfolio_constraints(amounts, base.portfolio_config())
    return ledger


def _same_replayed_baseline(published: pd.DataFrame, replayed: pd.DataFrame) -> None:
    if len(published) != len(replayed) or not published.sid.equals(replayed.sid):
        raise RuntimeError("Published G-3 baseline order identities changed")
    for column in ("signal_id", "filled", "portfolio_executed", "portfolio_status", "exit_reason"):
        if not published[column].astype(str).equals(replayed[column].astype(str)):
            raise RuntimeError(f"Published G-3 baseline drift: {column}")
    for column in ("entry_price", "exit_price", "portfolio_net_profit_rupees"):
        a = pd.to_numeric(published[column], errors="coerce").to_numpy(float)
        b = pd.to_numeric(replayed[column], errors="coerce").to_numpy(float)
        if not np.allclose(a, b, rtol=0, atol=1e-6, equal_nan=True):
            raise RuntimeError(f"Published G-3 baseline drift: {column}")
    for column in ("entry_ts", "exit_ts", "confirmation_ts"):
        a = pd.to_datetime(published[column], errors="coerce", utc=True)
        b = pd.to_datetime(replayed[column], errors="coerce", utc=True)
        if not a.equals(b):
            raise RuntimeError(f"Published G-3 baseline timestamp drift: {column}")


def load_published(pointer_path: Path = DEFAULT_POINTER) -> tuple[pd.DataFrame, list[date], dict]:
    """Load only the current hash-verified 49-session V2-equivalent baseline."""
    pointer_path = Path(pointer_path).resolve()
    root = pointer_path.parent.parent
    pointer = json.loads(pointer_path.read_text(encoding="utf-8"))
    if (pointer.get("variant_id") != EXPECTED_BASELINE_VARIANT
            or pointer.get("execution_authority") is not False):
        raise ValueError("Current pointer is not the expected G-3 V2-equivalent backtest")
    run_dir = (root / str(pointer["run_dir"])).resolve()
    if not run_dir.is_relative_to(root.resolve()):
        raise ValueError("Current G-3 pointer escaped its result root")
    manifest_path = run_dir / "manifest.json"
    if sha256(manifest_path) != pointer.get("manifest_sha256"):
        raise ValueError("Published G-3 manifest hash changed")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if (manifest.get("variant_id") != EXPECTED_BASELINE_VARIANT
            or manifest.get("execution_authority") is not False):
        raise ValueError("Published G-3 manifest identity changed")
    for name, expected in manifest.get("files", {}).items():
        path = (run_dir / name).resolve()
        if path.parent != run_dir or sha256(path) != expected:
            raise ValueError(f"Published G-3 artifact hash changed: {name}")
    if "variant_trades.csv" not in manifest.get("files", {}) or "daily_results.csv" not in manifest["files"]:
        raise ValueError("Published G-3 baseline artifacts missing")
    published = pd.read_csv(run_dir / "variant_trades.csv")
    daily = pd.read_csv(run_dir / "daily_results.csv")
    summary = json.loads((run_dir / "summary.json").read_text(encoding="utf-8"))
    days = _days(daily)
    if (len(days) != 49 or str(days[0]) != "2026-07-29" or str(days[-1]) != "2026-10-12"
            or summary.get("sessions") != 49 or not set(_days(published)).issubset(days)):
        raise ValueError("Published G-3 baseline session cohort changed")
    if (len(published) != 123 or published.sid.duplicated().any()
            or published.signal_id.duplicated().any()):
        raise ValueError("Published G-3 baseline order pool changed")
    if not published.strategy_version.eq(contract.STRATEGY_VERSION).all():
        raise ValueError("Published G-3 strategy revision changed")
    published["filled"] = _bool_column(published, "filled")
    published["portfolio_executed"] = _bool_column(published, "portfolio_executed")
    replayed = allocate(published.reset_index(drop=True))
    _same_replayed_baseline(published, replayed)
    expected = float(summary["with_0950_long"]["net_pnl_rs"])
    if (int(replayed.portfolio_executed.sum()) != 112
            or not math.isclose(float(replayed.portfolio_net_profit_rupees.sum()), expected,
                                rel_tol=0, abs_tol=1e-6)):
        raise RuntimeError("Published G-3 49-session P&L parity failed")
    evidence = dict(pointer=str(pointer_path), pointer_sha256=sha256(pointer_path),
                    run_dir=str(run_dir), manifest=str(manifest_path),
                    manifest_sha256=sha256(manifest_path),
                    variant_trades_sha256=manifest["files"]["variant_trades.csv"],
                    published_summary=summary["with_0950_long"])
    return published.reset_index(drop=True), days, evidence


def _path_for_symbol_day(symbol: str, day: date, root: Path) -> tuple[pd.DataFrame, dict]:
    path = hybrid.equity_one_minute_path(symbol, root / "stocks_indicators_1min_eq")
    minute = _read_cash(path)
    selected = minute.loc[minute.ts.dt.date.eq(day)].copy()
    start = pd.Timestamp(datetime.combine(day, time(9, 22), contract.IST))
    end = pd.Timestamp(datetime.combine(day, time(15, 15), contract.IST))
    required = pd.date_range(start, end, freq="min")
    observed = pd.DatetimeIndex(selected.loc[selected.ts.between(start, end), "ts"])
    if not observed.equals(required):
        missing = required.difference(observed)
        raise ValueError(f"Incomplete genuine one-minute execution path {day} {symbol}: "
                         f"missing={len(missing)}, extra_or_disordered={len(observed) != len(required)}")
    return selected, dict(day=str(day), symbol=symbol, path=str(path.resolve()),
                          source_sha256=sha256(path), first_bar=start.isoformat(),
                          last_bar=end.isoformat(), bars=len(required))


def _opening_source_parity(order: pd.Series, minute: pd.DataFrame, day: date,
                           root: Path) -> dict:
    """Reject selection/execution drift in the real opening price and OI inputs."""
    symbol, futures_contract = str(order.tradingsymbol), str(order.futures_tradingsymbol)
    first_start = pd.Timestamp(datetime.combine(day, time(9, 16), contract.IST))
    first_end = pd.Timestamp(datetime.combine(day, time(9, 20), contract.IST))
    confirmation = first_end + pd.Timedelta(minutes=1)
    first = minute.loc[minute.ts.between(first_start, first_end)]
    required = pd.date_range(first_start, first_end, freq="min")
    if not pd.DatetimeIndex(first.ts).equals(required):
        raise ValueError(f"09:20 selected cash opening minute drift: {day} {symbol}")
    next_minute = minute.loc[minute.ts.eq(confirmation)]
    if len(next_minute) != 1:
        raise ValueError(f"09:21 selected cash confirmation drift: {day} {symbol}")
    expected = {
        "cash_open": float(first.iloc[0].open),
        "cash_high": float(first.high.max()),
        "cash_low": float(first.low.min()),
        "cash_close": float(first.iloc[-1].close),
        "cash_volume": float(first.volume.sum()),
        "confirmation_open": float(next_minute.iloc[0].open),
        "confirmation_high": float(next_minute.iloc[0].high),
        "confirmation_low": float(next_minute.iloc[0].low),
        "confirmation_close": float(next_minute.iloc[0].close),
        "confirmation_volume": float(next_minute.iloc[0].volume),
    }
    expected["price_first_5m_pct"] = (expected["cash_close"] / expected["cash_open"] - 1) * 100
    for name, value in expected.items():
        recorded = pd.to_numeric(order.get(name), errors="coerce")
        if not math.isfinite(recorded) or not math.isclose(float(recorded), value, rel_tol=1e-9, abs_tol=1e-7):
            raise ValueError(f"09:20 selected cash source drift {day} {symbol}: {name}")
    opening = _first_minute(root, str(day), symbol, futures_contract)
    recorded_opening = pd.to_numeric(order.get("opening_0916_oi"), errors="coerce")
    if (opening is None or str(opening.get("contract")) != futures_contract
            or not math.isclose(float(opening.get("oi", math.nan)), float(recorded_opening),
                                rel_tol=1e-10, abs_tol=1e-8)):
        raise ValueError(f"09:16 selected futures OI source drift: {day} {symbol}")
    futures_path = root / "fno_oi" / "raw_contracts_5m" / f"{common.safe_contract_stem(futures_contract)}_5minute.parquet"
    if not futures_path.is_file():
        raise ValueError(f"09:20 selected futures source missing: {day} {futures_contract}")
    futures = pd.read_parquet(futures_path)
    times = hybrid._to_ist(futures.timestamp)
    current = futures.loc[times.eq(first_end)]
    recorded_0920 = pd.to_numeric(order.get("futures_0920_oi"), errors="coerce")
    if (len(current) != 1 or str(current.iloc[0].get("tradingsymbol")) != futures_contract
            or _is_flagged(current.iloc[0].to_dict())
            or not math.isclose(float(current.iloc[0].get("oi", math.nan)), float(recorded_0920),
                                rel_tol=1e-10, abs_tol=1e-8)):
        raise ValueError(f"09:20 selected futures OI source drift: {day} {symbol}")
    return dict(contract=futures_contract, path=str(futures_path.resolve()),
                source_sha256=sha256(futures_path), opening_0916_oi=float(recorded_opening),
                completed_0920_oi=float(recorded_0920))


def new_execution_paths(orders: pd.DataFrame, data_root: Path) -> tuple[dict, list[dict]]:
    """Validate every new execution minute and build only new SID paths."""
    paths, evidence = {}, []
    if orders.empty:
        return paths, evidence
    for day_value, group in orders.groupby("day", sort=True):
        day = date.fromisoformat(str(day_value))
        minutes = {}
        for symbol in sorted(group.tradingsymbol.unique()):
            minutes[symbol], item = _path_for_symbol_day(symbol, day, Path(data_root))
            for _, order in group.loc[group.tradingsymbol.eq(symbol)].iterrows():
                future_proof = _opening_source_parity(order, minutes[symbol], day, Path(data_root))
                if "futures" in item and item["futures"] != future_proof:
                    raise ValueError(f"Different futures evidence for one selected stock: {day} {symbol}")
                item["futures"] = future_proof
            evidence.append(item)
        paths.update(source.selected_paths(group, minutes, day))
    if set(paths) != set(orders.sid.astype(int)):
        raise RuntimeError("09:20 execution paths missing a selected order")
    simulator.g.v9.validate_paths(orders, paths)
    return paths, evidence


def _stats(frame: pd.DataFrame, days: list[date]) -> dict:
    executed = frame.loc[_bool_column(frame, "portfolio_executed")]
    pnl = pd.to_numeric(executed.portfolio_net_profit_rupees, errors="raise")
    daily = frame.groupby(frame.day.astype(str), sort=True).portfolio_net_profit_rupees.sum()
    curve = np.r_[0.0, daily.reindex([str(day) for day in days], fill_value=0).cumsum().to_numpy(float)]
    high = np.maximum.accumulate(curve)
    return dict(selected_orders=len(frame), native_fills=int(_bool_column(frame, "filled").sum()),
                executed_trades=len(executed), wins=int(pnl.gt(0).sum()),
                losses=int(pnl.lt(0).sum()),
                win_rate_pct=100 * float(pnl.gt(0).mean()) if len(pnl) else None,
                gross_pnl_rs=float(pd.to_numeric(executed.portfolio_gross_profit_rupees).sum()),
                costs_rs=float(pd.to_numeric(executed.portfolio_cost_rupees).sum()),
                net_pnl_rs=float(pnl.sum()), max_drawdown_rs=float((high - curve).max()))


def compare_ledgers(baseline: pd.DataFrame, variant: pd.DataFrame,
                    days: list[date], eligible_days: list[date], new_ids: set[int]) -> tuple[dict, pd.DataFrame]:
    before = baseline.set_index("sid", drop=False)
    old = variant.loc[variant.sid.isin(before.index)].set_index("sid", drop=False)
    if not old.index.equals(before.index) or not old.signal_id.equals(before.signal_id):
        raise RuntimeError("Variant altered an existing G-3 order identity")
    for column in ("filled", "entry_ts", "exit_ts", "entry_price", "exit_price", "net_return_pct"):
        a, b = before[column], old[column]
        if column in ("entry_price", "exit_price", "net_return_pct"):
            same = np.allclose(pd.to_numeric(a, errors="coerce"), pd.to_numeric(b, errors="coerce"),
                               rtol=0, atol=1e-8, equal_nan=True)
        elif column.endswith("_ts"):
            same = pd.to_datetime(a, utc=True, errors="coerce").equals(
                pd.to_datetime(b, utc=True, errors="coerce"))
        else:
            same = a.astype(str).equals(b.astype(str))
        if not same:
            raise RuntimeError(f"Variant altered an existing G-3 raw trade outcome: {column}")
    added = variant.loc[variant.sid.isin(new_ids)]
    lost = before.portfolio_executed.astype(bool) & ~old.portfolio_executed.astype(bool)
    gained = ~before.portfolio_executed.astype(bool) & old.portfolio_executed.astype(bool)
    base_all, var_all = _stats(baseline, days), _stats(variant, days)
    base_eligible = baseline.loc[baseline.day.astype(str).isin({str(d) for d in eligible_days})]
    var_eligible = variant.loc[variant.day.astype(str).isin({str(d) for d in eligible_days})]
    add_all = _stats(added, days)
    old_delta = float(old.portfolio_net_profit_rupees.sum() - before.portfolio_net_profit_rupees.sum())
    delta = var_all["net_pnl_rs"] - base_all["net_pnl_rs"]
    if not math.isclose(delta, add_all["net_pnl_rs"] + old_delta, rel_tol=0, abs_tol=1e-5):
        raise RuntimeError("Paired P&L reconciliation failed")
    all_days = [str(day) for day in days]
    base_daily = baseline.groupby(baseline.day.astype(str)).portfolio_net_profit_rupees.sum().reindex(all_days, fill_value=0)
    var_daily = variant.groupby(variant.day.astype(str)).portfolio_net_profit_rupees.sum().reindex(all_days, fill_value=0)
    added_daily = added.groupby(added.day.astype(str)).portfolio_net_profit_rupees.sum().reindex(all_days, fill_value=0)
    daily = pd.DataFrame(dict(day=all_days, eligible_for_0920=[day in {str(d) for d in eligible_days} for day in all_days],
                              baseline_net_pnl_rs=base_daily.to_numpy(float),
                              variant_net_pnl_rs=var_daily.to_numpy(float),
                              added_0920_net_pnl_rs=added_daily.to_numpy(float),
                              delta_net_pnl_rs=(var_daily - base_daily).to_numpy(float)))
    summary = dict(baseline=base_all, with_0920=var_all,
                   delta_net_pnl_rs=delta, added_0920=add_all,
                   eligible_window=dict(sessions=len(eligible_days),
                                        baseline=_stats(base_eligible, eligible_days),
                                        with_0920=_stats(var_eligible, eligible_days)),
                   old_trade_effect=dict(displaced_count=int(lost.sum()), newly_accepted_count=int(gained.sum()),
                                         net_pnl_change_rs=old_delta,
                                         displaced_signal_ids=before.loc[lost, "signal_id"].astype(str).tolist(),
                                         newly_accepted_signal_ids=before.loc[gained, "signal_id"].astype(str).tolist()))
    summary["eligible_window"]["delta_net_pnl_rs"] = (
        summary["eligible_window"]["with_0920"]["net_pnl_rs"]
        - summary["eligible_window"]["baseline"]["net_pnl_rs"])
    for side in ("LONG", "SHORT"):
        summary[f"added_0920_{side.lower()}"] = _stats(added.loc[added.side.eq(side)], days)
    return summary, daily


def run(features_path: Path, output_dir: Path, *, pointer_path: Path = DEFAULT_POINTER,
        data_root: Path = RUNTIME_ROOT, require_sealed_features: bool = True,
        require_full_baseline_dates: bool = True) -> dict:
    output_dir = Path(output_dir).resolve()
    if output_dir.exists() and any(output_dir.iterdir()):
        raise FileExistsError(f"Research output must be new or empty: {output_dir}")
    published, days, evidence = load_published(pointer_path)
    features_path = Path(features_path).resolve()
    all_features, exporter_coverage, feature_evidence = load_feature_evidence(
        features_path, require_sealed=require_sealed_features)
    dated_universe_evidence = validate_feature_universe(all_features, data_root, exporter_coverage)
    exported_days = _feature_days(all_features)
    baseline_day_set = set(days)
    excluded_exported_days = sorted(set(exported_days) - baseline_day_set)
    features = all_features.loc[all_features.session_date.astype(str).isin(
        {str(day) for day in baseline_day_set})].copy()
    eligible_days = _feature_days(features)
    if not eligible_days:
        raise ValueError("No 09:20 feature date overlaps the published G-3 baseline")
    missing_baseline = sorted(baseline_day_set - set(eligible_days))
    if require_full_baseline_dates and missing_baseline:
        raise ValueError(f"09:20 feature export omitted published G-3 sessions: {missing_baseline}")
    audit = selection_audit_0920(features)
    orders = audit.loc[audit.v9_selected.eq(True)].copy().reset_index(drop=True)
    if set(orders.sid.astype(int)) & set(published.sid.astype(int)):
        raise ValueError("New 09:20 SIDs overlap the published baseline")
    if set(orders.signal_id.astype(str)) & set(published.signal_id.astype(str)):
        raise ValueError("New 09:20 signal IDs overlap the published baseline")
    paths, path_evidence = new_execution_paths(orders, Path(data_root))
    if len(orders):
        new_trades = simulator.simulate_staged(orders, paths,
            cost_bps=contract.ROUND_TRIP_COST_BPS,
            max_entry_delay_minutes=contract.ENTRY_EXPIRY_MINUTES)
    else:
        new_trades = orders.copy()
        new_trades["filled"] = pd.Series(dtype=bool)
    new_trades["strategy"] = RESEARCH_VERSION
    new_trades["strategy_version"] = RESEARCH_VERSION
    new_trades["scope"] = "BACKTEST_ONLY"
    new_trades["execution_authority"] = False
    combined = pd.concat((published, new_trades), ignore_index=True, sort=False)
    variant = allocate(combined)
    baseline = allocate(published)
    comparison, daily = compare_ledgers(baseline, variant, days, eligible_days,
                                        set(orders.sid.astype(int)))
    coverage = dict(research_stock_days=len(features),
                    valid_0920_oi_stock_days=int(features.oi_change_pct.notna().sum()),
                    complete_research_stock_days=int(features.research_feature_status.eq("COMPLETE").sum()),
                    eligible_dates=[str(day) for day in eligible_days],
                    exported_dates_not_in_baseline=[str(day) for day in excluded_exported_days],
                    baseline_dates_without_dated_0920_features=[str(day) for day in days if day not in eligible_days],
                    selected_0920_orders=len(orders),
                    selected_0920_live_equivalent=int(_bool_column(orders, "complete_decision_evidence_by_0922").sum()) if len(orders) else 0)
    result = dict(variant_id=RESEARCH_VERSION, scope="BACKTEST_ONLY", execution_authority=False,
                  method="PUBLISHED_G3_V2_FROZEN_TRADE_OUTCOMES_PLUS_FRESH_0920_EXECUTION_AND_PAIRED_PORTFOLIO_REPLAY",
                  created_at_ist=datetime.now(contract.IST).isoformat(), sessions=len(days),
                  period_start=str(days[0]), period_end=str(days[-1]),
                  retrospective_opening_oi=True, live_tradability_verified=False,
                  opening_oi_interval_minutes=4,
                  price_basis="09:15_CASH_OPEN_TO_09:20_CASH_CLOSE",
                  baseline_provenance=evidence, features_path=str(features_path),
                  features_sha256=sha256(features_path),
                  feature_export_provenance=feature_evidence,
                  dated_universe_provenance=dated_universe_evidence,
                  feature_export_coverage=exporter_coverage,
                  selected_path_provenance=path_evidence, coverage=coverage,
                  **comparison)
    output_dir.mkdir(parents=True, exist_ok=True)
    artifacts = dict(selection_audit_0920=audit, added_0920_orders=orders,
                     baseline_trades=baseline, added_0920_trades=variant.loc[variant.sid.isin(orders.sid)].copy(),
                     variant_trades=variant, daily_comparison=daily)
    for name, frame in artifacts.items():
        temporary = output_dir / f".{name}.csv.part"
        frame.to_csv(temporary, index=False)
        os.replace(temporary, output_dir / f"{name}.csv")
    (output_dir / "summary.json").write_text(json.dumps(result, indent=2, allow_nan=False, default=str) + "\n", encoding="utf-8")
    code = (Path(__file__).resolve(), Path(__file__).with_name("g3_0920_strategy.py"),
            Path(__file__).with_name("g3_0920_history.py"),
            Path(__file__).with_name("g3_0920_feature_export.py"))
    manifest = dict(variant_id=RESEARCH_VERSION, execution_authority=False,
                    code_sha256={path.name: sha256(path) for path in code},
                    files={p.name: sha256(p) for p in output_dir.iterdir() if p.is_file()})
    (output_dir / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    return result


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--features", type=Path, default=DEFAULT_FEATURES)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--pointer", type=Path, default=DEFAULT_POINTER)
    parser.add_argument("--data-root", type=Path, default=RUNTIME_ROOT)
    parser.add_argument("--allow-unsealed-features", action="store_true",
                        help="Explicitly allow an individual-day feature export without a hash manifest")
    parser.add_argument("--allow-partial-baseline-dates", action="store_true",
                        help="Explicitly allow a research sample omitting published G-3 sessions")
    args = parser.parse_args(argv)
    result = run(args.features, args.output_dir, pointer_path=args.pointer,
                 data_root=args.data_root, require_sealed_features=not args.allow_unsealed_features,
                 require_full_baseline_dates=not args.allow_partial_baseline_dates)
    print(json.dumps({key: result[key] for key in (
        "sessions", "baseline", "with_0920", "delta_net_pnl_rs",
        "added_0920_long", "added_0920_short", "old_trade_effect")}, indent=2))
    print(str(Path(args.output_dir).resolve()))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
