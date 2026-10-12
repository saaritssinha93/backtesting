"""Preregistered comparison and isolated artifacts for the cash G-3 adaptation."""
from __future__ import annotations

import argparse
import hashlib
import html
import inspect
import json
import os
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from . import baseline, data, selection
from .features import build_features, clean_minutes, participation_mask, strict_signals, variants
from .index_data import load_nifty_context
from .path_evidence import assess, EVIDENCE_VERSION

PROTOCOL_VERSION = "V14_CASH_PARTICIPATION_STUDY_V2"
TRAIN_END = "2026-08-27"
VALIDATION_END = "2026-09-10"
COMMON_FEATURES = ["interval_rvol", "cumulative_rvol", "relative_turnover", "cmf10", "signed_volume10", "mfi5"]


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def write_json(path: Path, value: Any) -> None:
    def clean(x):
        if isinstance(x, dict):
            return {str(k): clean(v) for k, v in x.items()}
        if isinstance(x, (list, tuple)):
            return [clean(v) for v in x]
        if isinstance(x, (np.integer, np.floating, np.bool_)):
            return clean(x.item())
        if isinstance(x, float) and not np.isfinite(x):
            return None
        return x
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temp.write_text(json.dumps(clean(value), indent=2, default=str, allow_nan=False), encoding="utf-8")
    os.replace(temp, path)


def split_for_day(day: str) -> str:
    return "TRAIN" if day <= TRAIN_END else "VALIDATION" if day <= VALIDATION_END else "TEST"


def protocol() -> dict:
    return dict(version=PROTOCOL_VERSION, variants=[v.to_dict() for v in variants()],
        train_end=TRAIN_END, validation_end=VALIDATION_END,
        test_rule="All later requested sessions; never used to select or retune the replacement",
        historical_evidence="G3 reused this history; test is held out for V14 replacement only, not for the inherited strategy",
        reference_sessions=20, reference_statistic="Median of the prior20 recorded rows at the identical clock; all20 source values must be valid. Entirely absent clock rows are skipped; incomplete cumulative rows invalidate that window",
        feature_clock="Replacement measured at completed5m signal; native exact+1m confirmation remains",
        common_feature_coverage=COMMON_FEATURES,
        selection="Highest VALIDATION net P&L after native5bps among TRAIN trades>=20/net>0 and VALIDATION trades>=10/net>0; tie lower validation drawdown then grid order",
        failed_screen="Keep highest-validation research candidate with qualified=false; no claim of profitable validation",
        test_failure="Selected candidate stays frozen; no switch to a test winner",
        ranking="Native14 setup eligibility and rank metadata; select every eligible candidate without per-slot quotas",
        slot_selection_policy=selection.UNLIMITED_SELECTION,
        friction_stress_additional_roundtrip_bps=[0., 5., 10., 20.],
        path_policy=EVIDENCE_VERSION,
        missing_path_policy="Select first, never refill; require real uninterrupted bars through an observed native terminal exit or all10 unfilled-entry minutes; unresolved grid-selected outcomes exclude the entire date",
        protocol_revision="V2 observed-terminal-prefix data contract with an explicit slot-selection policy; new V14 runs remove per-slot quotas, while historical replay retains its recorded capped policy",
        common_history_policy="All variants compared on candidates with finite past-only values for all6 replacement families",
        minimum_session_symbol_presence=.95,
        benchmark="No-OI control on identical candidate/date coverage",
        execution_authority=False)


def preregister(output: Path) -> None:
    path = output / "protocol.json"
    if path.exists():
        existing = json.loads(path.read_text(encoding="utf-8"))
        if existing["protocol"] != protocol():
            raise RuntimeError("Existing run uses a different protocol; create a new output directory")
    else:
        write_json(path, dict(recorded_at=datetime.now(timezone.utc).isoformat(), protocol=protocol(),
                              source_sha256={p.name: sha256(p) for p in Path(__file__).parent.glob("*.py")}))


def choose_variant(metrics: pd.DataFrame) -> dict:
    """No TEST or ALL column enters selection. Failure remains explicit."""
    source = metrics.loc[metrics["split"].isin(["TRAIN", "VALIDATION"])].copy()
    if "additional_bps" in source:
        source = source.loc[source.additional_bps.eq(0)]
    order = {v.name: i for i, v in enumerate(variants())}
    options = []
    for name, group in source.groupby("variant", sort=False):
        indexed = group.set_index("split")
        if not {"TRAIN", "VALIDATION"}.issubset(indexed.index):
            continue
        train, validation = indexed.loc["TRAIN"], indexed.loc["VALIDATION"]
        if isinstance(train, pd.DataFrame) or isinstance(validation, pd.DataFrame):
            raise ValueError("Duplicate candidate split metrics")
        qualified = bool(train.trades >= 20 and train.net_pnl > 0 and validation.trades >= 10 and validation.net_pnl > 0)
        options.append(dict(name=name, qualified=qualified, train_net=float(train.net_pnl),
            validation_net=float(validation.net_pnl), validation_drawdown=float(validation.max_drawdown),
            train_trades=int(train.trades), validation_trades=int(validation.trades), grid_order=order.get(name, 999)))
    if not options:
        raise ValueError("No candidate has training and validation metrics")
    eligible = [x for x in options if x["qualified"]]
    selected = sorted(eligible or options, key=lambda x: (-x["validation_net"], x["validation_drawdown"], x["grid_order"]))[0]
    return {**selected, "qualified_candidates": len(eligible), "candidate_count": len(options),
            "selection_used_test": False, "reason": "VALIDATION_NET_WINNER" if eligible else "NO_CANDIDATE_PASSED_SCREEN_RESEARCH_ONLY"}


def daily_metrics(ledger: pd.DataFrame, days: list[str], additional_bps: float = 0.) -> pd.DataFrame:
    executed = ledger.loc[ledger.portfolio_executed.eq(True)].copy()
    executed["day"] = executed.day.astype(str)
    if len(executed):
        # Cost stress is a P&L haircut on identical baseline fills and size;
        # it is explicitly not a measured spread or alternative fill model.
        gross = pd.to_numeric(executed.portfolio_gross_profit_rupees, errors="raise")
        cost = pd.to_numeric(executed.portfolio_cost_rupees, errors="raise")
        notional = cost / (baseline.frozen_config()["cost_bps"] / 10000.)
        executed["stress_cost"] = cost + notional * additional_bps / 10000.
        executed["stress_net"] = gross - executed.stress_cost
        grouped = executed.groupby("day").agg(trades=("sid", "size"),
            wins=("stress_net", lambda x: int(x.gt(0).sum())),
            net_pnl=("stress_net", "sum"), gross_pnl=("portfolio_gross_profit_rupees", "sum"), cost=("stress_cost", "sum"))
    else:
        grouped = pd.DataFrame(columns=["trades", "wins", "net_pnl", "gross_pnl", "cost"])
    daily = grouped.reindex(days, fill_value=0).fillna(0).rename_axis("day").reset_index()
    daily["split"] = daily.day.map(split_for_day)
    daily["cumulative_net_pnl"] = daily.net_pnl.cumsum()
    daily["drawdown"] = daily.cumulative_net_pnl.cummax().clip(lower=0) - daily.cumulative_net_pnl
    return daily


def metric(ledger: pd.DataFrame, days: list[str], *, additional_bps: float = 0.) -> dict:
    daily = daily_metrics(ledger, days, additional_bps)
    trades = ledger.loc[ledger.portfolio_executed.eq(True) & ledger.day.astype(str).isin(days)].copy()
    if len(trades):
        costs = trades.portfolio_cost_rupees.astype(float)
        pnl = trades.portfolio_net_profit_rupees.astype(float) - costs / (baseline.frozen_config()["cost_bps"] / 10000.) * additional_bps / 10000.
    else:
        pnl = pd.Series(dtype=float)
    negative = float(-pnl.loc[pnl.lt(0)].sum())
    return dict(sessions=len(days), trades=len(trades), wins=int(pnl.gt(0).sum()),
                win_rate_pct=float(pnl.gt(0).mean() * 100) if len(pnl) else 0.,
                net_pnl=float(pnl.sum()), gross_pnl=float(daily.gross_pnl.sum()), cost=float(daily.cost.sum()),
                profit_factor=float(pnl.loc[pnl.gt(0)].sum() / negative) if negative else None,
                max_drawdown=float(daily.drawdown.max()) if len(daily) else 0.,
                positive_days=int(daily.net_pnl.gt(0).sum()), average_trade=float(pnl.mean()) if len(pnl) else 0.)


def _feature_worker(task: tuple) -> tuple[pd.DataFrame, dict]:
    root_text, cache_text, symbol, allowed_days, context, source_hash, code_hash = task
    root, cache = Path(root_text), Path(cache_text)
    source = root / "data/1m" / f"{symbol}.parquet"
    key = hashlib.sha256(json.dumps(dict(source=source_hash, code=code_hash,
        days=allowed_days, index=context), sort_keys=True, allow_nan=False).encode()).hexdigest()
    target, metadata = cache / f"{symbol}.parquet", cache / f"{symbol}.json"
    if sha256(source) != source_hash:
        raise RuntimeError(f"Cash input changed during feature build: {symbol}")
    if target.exists() and metadata.exists():
        record = json.loads(metadata.read_text(encoding="utf-8"))
        if record["key"] == key and record["sha256"] == sha256(target):
            return pd.read_parquet(target), record["audit"]
    minute = data.load_symbol_1m(symbol, root=root)
    if sha256(source) != source_hash:
        raise RuntimeError(f"Cash input changed while loading features: {symbol}")
    features = build_features(minute, symbol)
    if features.empty:
        strict = pd.DataFrame()
        audit = dict(symbol=symbol, raw_candidates=0, strict_candidates=0, common_candidates=0)
    else:
        features = features.loc[features.day.isin(allowed_days)].copy()
        features["nifty_first_bar_return_pct"] = features.day.map(context)
        strict = strict_signals(features)
        common = np.isfinite(strict[COMMON_FEATURES]).all(axis=1)
        strict["common_feature_coverage"] = common
        audit = dict(symbol=symbol, raw_candidates=len(features), strict_candidates=len(strict), common_candidates=int(common.sum()))
    audit["session_coverage"] = data.coverage(minute, allowed_days)
    strict.to_parquet(target, index=False)
    write_json(metadata, dict(key=key, sha256=sha256(target), audit=audit))
    return strict, audit


def feature_code_signature() -> str:
    """Bind cached features to their full construction and loader contract."""
    inputs = {name: sha256(Path(__file__).with_name(name)) for name in ("features.py", "data.py")}
    inputs["worker"] = inspect.getsource(_feature_worker)
    inputs["common_features"] = COMMON_FEATURES
    return hashlib.sha256(json.dumps(inputs, sort_keys=True).encode()).hexdigest()


def build_dataset(root: Path, output: Path) -> tuple[pd.DataFrame, dict, list[str]]:
    if (root / "data/fetch.lock").exists():
        raise RuntimeError("Cash data preparation is still active; finish it before starting the study")
    if (output / "precompute.lock").exists():
        raise RuntimeError("Feature precomputation is still active; finish it before starting the study")
    manifest_path = root / "data/manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    universe_path = root / "data/universe_manifest.json"
    if sha256(universe_path) != manifest["universe_sha256"]:
        raise RuntimeError("Data and universe manifests differ; finish data preparation first")
    universe = json.loads(universe_path.read_text(encoding="utf-8"))
    days = manifest["study_sessions"]
    index = load_nifty_context(days, root=root)
    index.to_csv(output / "nifty_context.csv", index=False)
    context = {day: float(value) if pd.notna(value) else None
               for day, value in index.set_index("day")["nifty_first_bar_return_pct"].items()}
    memberships = {day: set(universe["sessions"][day]["symbols"]) for day in days}
    files = {r["symbol"]: r for r in manifest["files"]}
    for symbol in manifest["symbols"]:
        if sha256(root / "data/1m" / f"{symbol}.parquet") != files[symbol]["sha256"]:
            raise RuntimeError(f"Data changed since the manifest: {symbol}")
    existing_meta = output / "feature_manifest.json"
    if existing_meta.exists():
        cached = json.loads(existing_meta.read_text(encoding="utf-8"))
        if (cached["data_manifest_sha256"] == sha256(manifest_path)
            and cached.get("feature_construction_sha256") == feature_code_signature()
            and cached.get("session_coverage_sha256") == sha256(output / "session_data_coverage.csv")
            and cached.get("verified_symbol_coverage_sha256") == sha256(output / "verified_symbol_coverage.json")
            and cached["features_code_sha256"] == sha256(Path(__file__).with_name("features.py"))
            and cached.get("nifty_context_sha256") == sha256(output / "nifty_context.csv")
            and cached.get("index_code_sha256") == sha256(Path(__file__).with_name("index_data.py"))
            and cached["feature_sha256"] == sha256(output / "feature_ledger.parquet")):
            return pd.read_parquet(output / "feature_ledger.parquet"), manifest, days
    parts, audit = [], []
    cache = output / "feature_cache"
    cache.mkdir(exist_ok=True)
    code_hash = feature_code_signature()
    tasks = [(str(root), str(cache), symbol, [day for day in days if symbol in memberships[day]],
              context, files[symbol]["sha256"], code_hash) for symbol in manifest["symbols"]]
    with ProcessPoolExecutor(max_workers=3) as workers:
        for i, (strict, symbol_audit) in enumerate(workers.map(_feature_worker, tasks, chunksize=1), 1):
            audit.append(symbol_audit)
            if not strict.empty:
                parts.append(strict)
            if i == 1 or i % 50 == 0 or i == len(manifest["symbols"]):
                print(f"FEATURES {i}/{len(manifest['symbols'])} {symbol_audit['symbol']}", flush=True)
    pool = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()
    if pool.empty:
        raise RuntimeError("No cash candidates; inspect coverage rather than selecting an empty winner")
    pool = pool.sort_values(["day", "signal_ts", "tradingsymbol", "side"], kind="stable").reset_index(drop=True)
    pool["sid"] = np.arange(len(pool))
    pool.to_parquet(output / "feature_ledger.parquet", index=False)
    present, full = {day: 0 for day in days}, {day: 0 for day in days}
    for record in audit:
        for day, coverage in record["session_coverage"].items():
            present[day] += int(coverage.get("observed_minutes", 0) > 0)
            full[day] += int(coverage.get("complete", False))
    pd.DataFrame([dict(day=day, expected_symbols=len(memberships[day]),
        present_symbols=present[day], complete_symbols=full[day],
        presence_fraction=present[day]/len(memberships[day]) if memberships[day] else 0.)
        for day in days]).to_csv(output / "session_data_coverage.csv", index=False)
    pd.DataFrame([{k:v for k,v in record.items() if k != "session_coverage"} for record in audit]).to_csv(output / "feature_coverage.csv", index=False)
    write_json(output / "verified_symbol_coverage.json", {record["symbol"]: record["session_coverage"] for record in audit})
    write_json(existing_meta, dict(data_manifest_sha256=sha256(manifest_path),
        feature_construction_sha256=code_hash,
        session_coverage_sha256=sha256(output / "session_data_coverage.csv"),
        verified_symbol_coverage_sha256=sha256(output / "verified_symbol_coverage.json"),
        features_code_sha256=sha256(Path(__file__).with_name("features.py")),
        index_code_sha256=sha256(Path(__file__).with_name("index_data.py")),
        feature_sha256=sha256(output / "feature_ledger.parquet"), rows=len(pool),
        common_rows=int(pool.common_feature_coverage.sum()), symbols=len(manifest["symbols"]),
        nifty_context_sha256=sha256(output / "nifty_context.csv")))
    return pool, manifest, days


def path_is_complete(path: dict, confirmation) -> bool:
    stamp = pd.Timestamp(confirmation)
    expected = pd.date_range(stamp + pd.Timedelta(minutes=1),
        stamp.normalize() + pd.Timedelta(hours=15, minutes=15), freq="min").asi8
    return bool(np.array_equal(path["timestamp_ns"], expected)
                and all(np.isfinite(path[c]).all() for c in ("open", "high", "low", "close")))


def run_study(root: Path, output: Path) -> dict:
    root, output = Path(root), Path(output)
    output.mkdir(parents=True, exist_ok=True)
    if (output / "summary.json").exists():
        raise FileExistsError("Completed study is immutable; use a new output directory")
    preregister(output)
    baseline.verify_snapshot()
    write_json(output / "executed_source_hashes.json", {
        str(p.relative_to(Path(__file__).parent)): sha256(p)
        for p in Path(__file__).parent.glob("*.py")})
    for filename in ("manifest.json", "universe_manifest.json"):
        (output / f"input_{filename}").write_bytes((root / "data" / filename).read_bytes())
    pool, manifest, days = build_dataset(root, output)
    common = pool.loc[pool.common_feature_coverage].copy()
    selected = {v.name: selection.select_orders(common,
                lambda rows, setup, variant=v: participation_mask(rows, variant),
                selection_policy=selection.UNLIMITED_SELECTION) for v in variants()}
    for name, orders in selected.items():
        orders.to_csv(output / f"selected_{name}.csv", index=False)
    union = pd.concat(selected.values(), ignore_index=True).drop_duplicates("sid")
    paths, unknown, evidence, incomplete_days = {}, [], {}, set()
    files = {record["symbol"]: record for record in manifest["files"]}
    for symbol, group in union.groupby("tradingsymbol", sort=True):
        source = root / "data/1m" / f"{symbol}.parquet"
        if sha256(source) != files[symbol]["sha256"]:
            raise RuntimeError(f"Cash input changed after features: {symbol}")
        minute = clean_minutes(data.load_symbol_1m(symbol, root=root))
        if sha256(source) != files[symbol]["sha256"]:
            raise RuntimeError(f"Cash input changed while loading execution: {symbol}")
        for row in group.itertuples(index=False):
            path = baseline.native_paths(minute, row.confirmation_ts, row.day)
            if not path_is_complete(path, row.confirmation_ts):
                incomplete_days.add(str(row.day))
            proof = assess(row, path)
            evidence[str(row.sid)] = dict(sid=int(row.sid), day=str(row.day), symbol=symbol,
                resolved=proof["resolved"], reason=proof["reason"], **proof["evidence"])
            if proof["resolved"]:
                paths[int(row.sid)] = proof["path"]
            else:
                unknown.append(dict(sid=int(row.sid), day=str(row.day), symbol=symbol,
                    setup_id=row.setup_id, path_minutes=len(path["timestamp_ns"]), reason=proof["reason"]))
    write_json(output / "path_evidence.json", evidence)
    unknown_frame = pd.DataFrame(unknown, columns=["sid", "day", "symbol", "setup_id", "path_minutes", "reason"])
    unknown_frame.to_csv(output / "unknown_outcomes.csv", index=False)
    unknown_ids = set(unknown_frame.sid)
    excluded_days = set(unknown_frame.day)
    missing_index = set(index_day for index_day in days if index_day not in set(
        pd.read_csv(output / "nifty_context.csv").dropna(subset=["nifty_first_bar_return_pct"]).day))
    excluded_days |= missing_index
    session_coverage = pd.read_csv(output / "session_data_coverage.csv")
    unavailable_days = set(session_coverage.loc[
        session_coverage.presence_fraction.lt(protocol()["minimum_session_symbol_presence"]), "day"])
    excluded_days |= unavailable_days
    eval_days = [day for day in days if day not in excluded_days]
    write_json(output / "evaluation_coverage.json", dict(requested_days=days, evaluated_days=eval_days,
        excluded_days=sorted(excluded_days), missing_index_days=sorted(missing_index),
        insufficient_cash_coverage_days=sorted(unavailable_days),
        unknown_selected_paths=len(unknown), raw_strict_candidates=len(pool), common_candidates=len(common),
        path_policy=EVIDENCE_VERSION, slot_selection_policy=selection.UNLIMITED_SELECTION,
        path_evidence_sha256=sha256(output / "path_evidence.json"),
        strict_full_session_evaluated_days=[day for day in eval_days if day not in incomplete_days],
        recovered_dates_with_proven_terminal_prefixes=[day for day in eval_days if day in incomplete_days],
        unknown_by_variant={name: int(orders.sid.isin(unknown_ids).sum()) for name, orders in selected.items()},
        policy=protocol()["missing_path_policy"]))
    ledgers, metric_rows = {}, []
    for name, orders in selected.items():
        # No unknown order is ever replaced by a lower-ranked stock. The whole
        # affected date is excluded for every variant before comparison.
        evaluable = orders.loc[orders.day.astype(str).isin(eval_days)].copy()
        ledger = baseline.simulate(evaluable, paths, allow_observed_prefix=True)
        ledger["variant"] = name
        ledger["split"] = ledger.day.astype(str).map(split_for_day)
        ledgers[name] = ledger
        ledger.to_csv(output / f"trades_{name}.csv", index=False)
        for stress in protocol()["friction_stress_additional_roundtrip_bps"]:
            for split in ("TRAIN", "VALIDATION", "TEST", "ALL"):
                split_days = eval_days if split == "ALL" else [d for d in eval_days if split_for_day(d) == split]
                metric_rows.append(dict(variant=name, split=split, additional_bps=stress,
                                        **metric(ledger, split_days, additional_bps=stress)))
        print(f"STUDY {name} selected={len(orders)} evaluable={len(evaluable)}", flush=True)
    metrics = pd.DataFrame(metric_rows)
    metrics.to_csv(output / "variant_comparison.csv", index=False)
    winner = choose_variant(metrics)
    name = winner["name"]
    selected_variant = next(v for v in variants() if v.name == name)
    ledger = ledgers[name]
    ledger.to_csv(output / "trades.csv", index=False)
    from .monitoring import attach_monitoring
    monitor_summary = attach_monitoring(root, output)
    daily = daily_metrics(ledger, eval_days)
    daily.to_csv(output / "daily_results.csv", index=False)
    selected[name].to_csv(output / "selected_orders.csv", index=False)
    frozen = baseline.frozen_config()
    profile = dict(version="V14", baseline=frozen["version"], selected_variant=selected_variant.to_dict(),
        selection=winner, common_feature_coverage=COMMON_FEATURES, path_policy=EVIDENCE_VERSION,
        slot_selection_policy=selection.UNLIMITED_SELECTION,
        capital_per_entry_rupees=frozen["capital_per_entry_rupees"],
        portfolio_capital_rupees=frozen["portfolio_capital_rupees"], leverage_factor=frozen["leverage_factor"],
        cost_bps=frozen["cost_bps"], execution_authority=False,
        state="PROVISIONAL_RESEARCH_CANDIDATE_PENDING_DATA" if manifest.get("fetch_status") == "BROKER_UNAVAILABLE" else "FROZEN_RESEARCH_CANDIDATE", output_dir=str(output.resolve()),
        protocol_sha256=sha256(output / "protocol.json"), data_manifest_sha256=sha256(root / "data/manifest.json"),
        warning="Snapshot MIS membership and modeled5x leverage; current eligibility and actual fills not established")
    write_json(output / "selected_profile.json", profile)
    selected_metrics = metrics.loc[metrics.variant.eq(name) & metrics.additional_bps.eq(0)]
    holdout = selected_metrics.loc[selected_metrics.split.eq("TEST")].iloc[0].to_dict()
    retrospective = metrics.loc[metrics.split.eq("ALL") & metrics.additional_bps.eq(0)].sort_values("net_pnl", ascending=False).iloc[0].to_dict()
    summary = dict(status="COMPLETE_RESEARCH_PARTIAL_DATA" if excluded_days or manifest.get("fetch_status") == "BROKER_UNAVAILABLE" else "COMPLETE_RESEARCH", version="V14", selection=winner,
        selected_variant=selected_variant.to_dict(), evaluated_sessions=len(eval_days), path_policy=EVIDENCE_VERSION,
        slot_selection_policy=selection.UNLIMITED_SELECTION,
        requested_sessions=len(days), excluded_days=sorted(excluded_days), symbols=manifest["symbol_count"],
        holdout=holdout, retrospective_all_date_winner=retrospective,
        heldout_profitable=bool(holdout["net_pnl"] > 0) if holdout["sessions"] else None,
        data_fetch_status=manifest.get("fetch_status", "LOCAL_STAGING"),
        broker_error=manifest.get("broker_error"),
        baseline_preserved=dict(setups=14, confirmation="exact_next_minute_LONG1.10_SHORT1.20",
            stop="1.25% tightens1.00% after120minutes", exits="unchanged retained targets and15:15 squareoff"),
        limitations=manifest["limitations"] + [
            "All returns are historical modeled outcomes, not guaranteed executable profitability",
            "Flat 5 bps inherited cost and 5x leverage may understate non-F&O friction/margin constraints",
            "Test dates are held out for the replacement study; inherited G3 used overlapping history",
            "Common candidate feature availability restricts even the no-OI control to a fair comparison",
            "Unresolved selected outcomes remove whole dates for all alternatives; no lower-ranked stock substitutions",
            "Observed terminal prefixes preserve exact trade P&L; gaps before resolution remain unresolved and may cause coverage-selection bias"],
        data_manifest=str(root / "data/manifest.json"), execution_authority=False)
    summary["monitoring"] = monitor_summary
    write_report(output, summary, metrics, daily)
    write_json(output / "summary.json", summary)
    write_json(root / "selected_profile.json", profile)
    write_json(root / "latest.json", dict(run=str(output.resolve()), report=str((output / "report.html").resolve()),
        selected_variant=name, status=summary["status"], completed_at=datetime.now(timezone.utc).isoformat()))
    print(f"RESULT {name} validation={winner['validation_net']:.2f} test={holdout['net_pnl']:.2f}", flush=True)
    return summary


def write_report(output: Path, summary: dict, metrics: pd.DataFrame, daily: pd.DataFrame) -> None:
    name = summary["selection"]["name"]
    table = metrics.loc[metrics.additional_bps.eq(0)].pivot(index="variant", columns="split", values="net_pnl")
    table = table.sort_values("VALIDATION", ascending=False).reindex(columns=["TRAIN", "VALIDATION", "TEST", "ALL"])
    stress = metrics.loc[metrics.variant.eq(name) & metrics.split.isin(["TEST", "ALL"]),
                         ["split", "additional_bps", "net_pnl", "trades", "max_drawdown", "profit_factor"]]
    values = daily.cumulative_net_pnl.to_numpy(float)
    if len(values):
        low, high = min(0., float(values.min())), max(1., float(values.max()))
        points = " ".join(f"{30 + i * 900/max(1,len(values)-1):.1f},{220-(v-low)*190/(high-low):.1f}" for i, v in enumerate(values))
        chart = f'<svg viewBox="0 0 960 250" role="img" aria-label="Cumulative historical net profit"><polyline fill="none" stroke="#2dd4bf" stroke-width="3" points="{points}"/><text x="30" y="245" fill="#cbd5e1">{html.escape(daily.day.iloc[0])}</text><text x="830" y="245" fill="#cbd5e1">{html.escape(daily.day.iloc[-1])}</text></svg>'
    else:
        chart = "<p>No fully evaluable sessions.</p>"
    limits = "".join(f"<li>{html.escape(x)}</li>" for x in summary["limitations"])
    rows = metrics.loc[metrics.variant.eq(name) & metrics.additional_bps.eq(0) & metrics.split.eq("ALL")].iloc[0]
    holdout_display = f"₹{summary['holdout']['net_pnl']:,.2f}" if summary['holdout']['sessions'] else "Unavailable"
    validation_class = "number negative" if summary['selection']['validation_net'] < 0 else "number"
    holdout_class = "number negative" if summary['holdout']['net_pnl'] < 0 else "number"
    all_class = "number negative" if rows.net_pnl < 0 else "number"
    qualification = ("Passed training and validation screens" if summary['selection']['qualified']
                     else "No alternative passed the training and validation screens; retained candidate is unqualified")
    markup = f'''<!doctype html><html lang="en"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>V14 cash participation study</title>
<style>body{{margin:0;background:#0b1220;color:#e2e8f0;font:16px/1.6 system-ui}}main{{max-width:1200px;margin:auto;padding:36px}}h1{{font-size:36px}}h2{{margin-top:38px}}.muted{{color:#94a3b8}}.cards{{display:flex;gap:20px;flex-wrap:wrap}}.card{{background:#172235;padding:20px;border-radius:12px;min-width:210px}}.number{{font-size:27px;color:#5eead4}}table{{border-collapse:collapse;width:100%;font-size:14px}}th,td{{padding:10px;border-bottom:1px solid #334155;text-align:right}}th:first-child,td:first-child{{text-align:left}}a{{color:#5eead4}}.scroll{{overflow:auto}}svg{{width:100%;max-height:300px;background:#111c2d;border-radius:12px}}.notice{{border-left:4px solid #fbbf24;padding:10px 20px;background:#1c2533}}</style>
<main><p class="muted">G-3 baseline · MIS working universe excluding dated F&O membership · Research only</p><h1>V14 cash participation study</h1>
<p>Slot selection: {html.escape(summary['slot_selection_policy'])}. Every eligible stock is selected at each configured time; existing portfolio capital constraints still govern modeled executions.</p>
<p>Selected replacement: <strong>{html.escape(name)}</strong>. Chosen on validation P&amp;L; test results did not influence selection.</p>
<style>.number.negative{{color:#fda4af}}</style>
<div class="cards"><div class="card">Validation net<div class="{validation_class}">₹{summary['selection']['validation_net']:,.2f}</div></div><div class="card">Held-out net<div class="{holdout_class}">{holdout_display}</div>{int(summary['holdout']['sessions'])} evaluated test sessions</div><div class="card">All evaluated sessions<div class="{all_class}">₹{rows.net_pnl:,.2f}</div>{int(rows.trades)} executed trades</div><div class="card">Coverage<div class="number">{summary['evaluated_sessions']}/{summary['requested_sessions']} days</div>{summary['symbols']} working-universe stocks</div></div>
<p class="notice">{qualification}. Costs use the inherited 5 bps round trip and modeled 5× leverage. Current broker MIS availability, spread and fill capacity are not established for every stock.</p>
<p>Run status: {html.escape(summary['status'].replace('_',' ').title())}. Market-data fetch: {html.escape(summary.get('data_fetch_status','UNKNOWN').replace('_',' ').title())}. {html.escape(summary.get('broker_error') or '')}</p>
<h2>Selected cumulative net profit</h2>{chart}
<h2>All alternatives · net rupees after baseline costs</h2><p>Train through 27 Aug; validation 28 Aug–10 Sep; test after 10 Sep. Common candidate coverage and identical evaluated dates.</p><div class="scroll">{table.to_html(float_format=lambda x:f'{x:,.2f}',border=0)}</div>
<h2>Additional friction sensitivity</h2><p>Additional round-trip basis points on the same modeled fills. This is a cost sensitivity, not measured market slippage.</p><div class="scroll">{stress.to_html(index=False,float_format=lambda x:f'{x:,.2f}',border=0)}</div>
<h2>Daywise selected results</h2><div class="scroll">{daily.to_html(index=False,float_format=lambda x:f'{x:,.2f}',border=0)}</div>
<h2>Coverage and interpretation</h2><ul>{limits}</ul><p>Excluded dates: {html.escape(', '.join(summary['excluded_days']) or 'None')}.</p>
<p>Retrospective all-date maximum: <strong>{html.escape(summary['retrospective_all_date_winner']['variant'])}</strong>; retained for transparency, not used to switch the held-out selection.</p>
<p><a href="daily_results.csv">Daily CSV</a> · <a href="trades.csv">Trade ledger</a> · <a href="variant_comparison.csv">All metrics</a> · <a href="pullback_report.html">Pullback observations</a> · <a href="selected_profile.json">Frozen selected profile</a> · <a href="evaluation_coverage.json">Coverage</a> · <a href="protocol.json">Recorded protocol</a></p></main></html>'''
    (output / "report.html").write_text(markup, encoding="utf-8")


def replay_selected(root: Path, output: Path, *, start_date: str, end_date: str,
                    profile_path: Path | None = None, fetch_missing: bool = False,
                    warmup_sessions: int = 30) -> dict:
    from .replay import replay_selected as replay
    return replay(root, output, start_date=start_date, end_date=end_date,
                  profile_path=profile_path, fetch_missing=fetch_missing,
                  warmup_sessions=warmup_sessions)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=data.DEFAULT_ROOT)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--preregister-only", action="store_true")
    args = parser.parse_args()
    output = args.output or args.root / "runs" / datetime.now().strftime("%Y%m%dT%H%M%S")
    output.mkdir(parents=True, exist_ok=True)
    if args.preregister_only:
        preregister(output)
        print(output / "protocol.json")
    else:
        run_study(args.root, output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
