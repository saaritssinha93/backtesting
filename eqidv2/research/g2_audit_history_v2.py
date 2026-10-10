"""Isolated frozen-G2 history sensitivity, with explicit source/coverage limits.

Only post-strict-signal filters are changed. No source artifact, baseline,
dashboard, live process, or broker is written. CSVs are research intermediates.
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import fno_v13_v10_g_2_backtest as g2
import fno_v13_v10_g_2_extend_results as ext
import fno_v13_v10_g_daily_replay as replay
from research import g2_session_forensic_audit as audit

POLICIES = {
    "frozen_g2": {"confirmation_volume_long": 1.2, "confirmation_volume_short": 1.2},
    "long_volume_1p10": {"confirmation_volume_long": 1.1, "confirmation_volume_short": 1.2},
    "both_volume_1p10": {"confirmation_volume_long": 1.1, "confirmation_volume_short": 1.1},
    "both_volume_1p00": {"confirmation_volume_long": 1.0, "confirmation_volume_short": 1.0},
    "short0935_setup_oi_0p10": {"confirmation_volume_long": 1.2, "confirmation_volume_short": 1.2,
                                "setup_oi_short0935": 0.1},
}
ARCHIVE = Path(r"C:\TradingData\eqidv2\fno_oi\strategy_research\v13_corrected_v10_g_2") / "run_20261005_staged125_to100_120m_through_20260930_rerun_20261005_172651"


def dump(path, value):
    path.write_text(json.dumps(ext._finite_json(value), indent=2, default=str, allow_nan=False), encoding="utf-8")


def select(signals, bundle, policy):
    work = signals.copy()
    work["research_original_1m_volume_ratio"] = work.v9_1m_volume_ratio
    work["research_original_oi_change_pct"] = work.oi_change_pct
    thresholds = np.where(work.side.eq("LONG"), policy["confirmation_volume_long"], policy["confirmation_volume_short"])
    # This is an exact adapter around a hardcoded >=1.20 gate. Ratio is not a
    # ranking input. Preserve genuine observations in all written outputs.
    work["v9_1m_volume_ratio"] = work.v9_1m_volume_ratio * (1.2 / thresholds)
    if "setup_oi_short0935" in policy:
        patch = work.side.eq("SHORT") & work.hhmm_int.eq(935) & work.oi_change_pct.ge(policy["setup_oi_short0935"]) & work.oi_change_pct.lt(.5)
        work.loc[patch, "oi_change_pct"] = .5
    source = bundle["source_g"]
    selected = g2.g.select_orders(work, bundle["v9_config"], g2.g.SelectionChange(**source["selection_change"]),
                                 core_first=source["core_first"], morning_slots=source.get("morning_slots", False),
                                 two_bar_continuation=source.get("two_bar_continuation", False))
    if selected.empty:
        # Native empty fallback repeats metadata labels present in a raw ledger.
        # Dropping duplicate empty columns changes no observation or decision.
        selected = selected.loc[:, ~selected.columns.duplicated()].copy()
    elif selected.columns.duplicated().any():
        raise ValueError("Nonempty native selection contains duplicate column labels")
    if len(selected):
        selected["v9_1m_volume_ratio"] = selected.research_original_1m_volume_ratio
        selected["oi_change_pct"] = selected.research_original_oi_change_pct
    return selected


def simulate(orders, paths, source):
    if orders.empty:
        return pd.DataFrame()
    g2.g.v9.validate_paths(orders, paths)
    trades = g2.simulate_staged(ext._apply_retained_g_exits(orders, source), paths,
                              cost_bps=source["cost_bps"], max_entry_delay_minutes=10)
    base = ext._portfolio_config(source)
    trades = g2.g.v9.v5.apply_fixed_capital_model(trades, base.capital_per_entry_rupees, base.leverage_factor)
    return g2.g.v9.v6.apply_portfolio_constraints(trades, base.portfolio_config())[0]


def executed(ledger):
    return ledger.loc[ledger.portfolio_executed.eq(True)].copy() if len(ledger) else ledger.copy()


def metrics(ledger, days):
    result = audit.metrics(ledger)
    e = executed(ledger)
    daily = e.groupby(e.day.astype(str)).portfolio_net_profit_rupees.sum() if len(e) else pd.Series(dtype=float)
    daily = daily.reindex([str(d) for d in sorted(days)], fill_value=0.)
    equity = pd.Series([0., *daily.cumsum().tolist()])
    result.update(selected_orders=len(ledger), fills=int(ledger.filled.eq(True).sum()) if len(ledger) else 0,
                  day_close_drawdown=float((equity.cummax()-equity).max()), sessions=len(days),
                  gross_pnl=float(e.portfolio_gross_profit_rupees.sum()) if len(e) else 0.,
                  positive_days=int(daily.gt(0).sum()), negative_days=int(daily.lt(0).sum()), zero_days=int(daily.eq(0).sum()))
    return result


def key(frame):
    return frame.day.astype(str) + "|" + frame.tradingsymbol.astype(str) + "|" + frame.setup_id.astype(str)


def normalize_candidates(frame):
    frame = frame.copy()
    frame["day"] = pd.to_datetime(frame.day).dt.date
    for field in ("signal_ts", "confirmation_ts", "v9_1m_feature_ts"):
        frame[field] = pd.to_datetime(frame[field], utc=True).dt.tz_convert(audit.IST)
    return frame


def finalize(output):
    """Publish simple report-facing aliases and explicit earlier source exclusions."""
    provenance = g2.read_json(output / "history_provenance.json")
    eligibility = pd.read_csv(Path(provenance["source_bundle"]) / "dataset/source_session_eligibility.csv")
    included = set(provenance["days"])
    eligibility["included_in_this_replay"] = eligibility.day.astype(str).isin(included)
    eligibility.to_csv(output / "sealed_source_session_eligibility.csv", index=False)
    source_exclusions = eligibility.loc[~eligibility.included_in_this_replay].to_dict("records")
    provenance["earlier_sealed_source_exclusions"] = source_exclusions
    provenance["source_eligibility_columns"] = list(eligibility.columns)
    known_absent = {str(row["day"]):row for row in source_exclusions}
    for record in provenance["unrepresented_weekdays"]:
        source_record = known_absent.get(record["date"])
        if source_record:
            record["note"] = str(source_record.get("reason")) + "; " + str(source_record.get("v13_v5_eligibility_reason"))
    pd.DataFrame(provenance["unrepresented_weekdays"]).to_csv(output / "unrepresented_dates.csv", index=False)
    provenance["code_sha256"][str(Path(__file__).resolve())] = g2.sha256(Path(__file__))
    provenance["execution_script_note"] = "Finalization adds report aliases and prior-source exclusions; native replay functions unchanged."
    dataset_manifest = g2.read_json(Path(provenance["source_bundle"]) / "dataset/dataset_manifest.json")
    provenance["sealed_universe_snapshots"] = [dict(month=r.get("month"), universe=r.get("payload", {}).get("universe"),
        days=r.get("payload", {}).get("days", [])) for r in dataset_manifest.get("native_cache_records", [])]
    membership_limit = "Sealed monthly history uses Aug21 membership for Jul29-Aug21 and Sep24 membership for Aug26-Sep23. Frozen replay preserves this archive; independently reconstructed daily point-in-time membership and survivorship-free coverage are not established."
    if membership_limit not in provenance["limitations"]:
        provenance["limitations"].append(membership_limit)
    for original, alias in [("historical_comparison.csv", "comparison.csv"), ("historical_daywise.csv", "daywise.csv"),
                            ("historical_added_removed.csv", "added_removed_trades.csv")]:
        pd.read_csv(output / original).to_csv(output / alias, index=False)
    comparisons = pd.read_csv(output / "comparison.csv")
    daily_results = pd.read_csv(output / "daywise.csv")
    validation = []
    for record in comparisons.to_dict("records"):
        variant = record["variant"]
        ledger = pd.read_csv(output / f"trades_{variant}.csv")
        if key(ledger).duplicated().any():
            raise ValueError("Duplicated historical selected trade identity: " + variant)
        daily_sum = daily_results.loc[daily_results.variant.eq(variant), "net_pnl"].sum()
        delta = record["added_net_pnl"]-record["removed_net_pnl"]
        if not np.isclose(daily_sum, record["net_pnl"], rtol=0, atol=1e-6):
            raise ValueError("Daily PnL sum reconciliation failed: " + variant)
        if not np.isclose(delta, record["net_delta_vs_baseline"], rtol=0, atol=1e-6):
            raise ValueError("Added-minus-removed PnL reconciliation failed: " + variant)
        validation.append(dict(variant=variant, unique_trade_identities=True,
            daywise_pnl_reconciled=True, added_removed_pnl_reconciled=True, tolerance_rupees=1e-6))
    provenance["independent_output_checks"] = validation
    coverage = []
    for row in eligibility.loc[eligibility.included_in_this_replay].to_dict("records"):
        coverage.append(dict(date=row["day"], segment="SEALED_38", universe_stocks=row["universe_size"],
            contracts_with_data=row["contracts_with_data"], source_coverage=row["coverage"],
            source_scope="Sealed source-eligible session; source min contract coverage 99%, not assumed213", reason=row["reason"]))
    for record in provenance["extensions"]:
        c=record.get("coverage", {})
        coverage.append(dict(date=record["day"], segment="DATED_EXTENSION", universe_stocks=c.get("universe_stocks"),
            contracts_with_data=c.get("included_stocks"), excluded_stocks=json.dumps(c.get("excluded_stocks", [])),
            source_scope=record["source_mode"], reason="COMPLETE_UNDER_DAILY_REPLAY_CONTRACT", source_run=record["run"]))
    pd.DataFrame(coverage).sort_values("date").to_csv(output / "historical_session_coverage.csv", index=False)
    dump(output / "provenance.json", provenance)
    dump(output / "summary.json", dict(state=provenance["state"], summary=provenance["summary"],
        sessions=len(provenance["days"]), days=provenance["days"], first_day=min(provenance["days"]),
        last_day=max(provenance["days"]), validation_level="REUSED_HISTORY_POST_STRICT_FILTER_SENSITIVITY_NOT_OUT_OF_SAMPLE",
        excluded_dated_extensions=provenance["exclusions"], unrepresented_weekdays=provenance["unrepresented_weekdays"],
        earlier_source_exclusions=source_exclusions, archive_parity=provenance["archive_parity"],
        historical_opportunity_capture="UNAVAILABLE_OFFICIAL_CLOSE_MOVEMENT_RECONCILIATION_NOT_RUN_EVERY_DATE",
        limitations=provenance["limitations"], files={"comparison":"comparison.csv", "daywise":"daywise.csv",
        "added_removed":"added_removed_trades.csv", "coverage":"historical_session_coverage.csv", "provenance":"provenance.json"}))
    print("FINALIZED", output, flush=True)


def run(output, through):
    if (output / "history_provenance.json").exists():
        raise FileExistsError("Completed research output already exists")
    output.mkdir(parents=True, exist_ok=True)
    dump(output / "prespecified_policies.json", {"policies": POLICIES, "through": through,
         "hypothesis": "Lower confirmation-volume floor or one setup OI floor may admit additional strict candidates.",
         "selection_scope": "All source-eligible stock candidates, no ticker exceptions or future-return inputs.",
         "validation": "Retrospective sensitivity, not untouched out-of-sample proof."})
    print("Verifying sealed38 archive and original selection identity", flush=True)
    bundle = g2.load_bundle(g2.DEFAULT_SOURCE_BUNDLE, g2.DEFAULT_G_CONFIG)
    source = bundle["source_g"]
    settings = g2.config(source)
    archived_settings = g2.read_json(ARCHIVE / "frozen_config.json")
    if archived_settings != settings:
        raise ValueError("Archived standard G2 is not native staged-stop baseline")
    archive_provenance = ext._verify_output(ARCHIVE)
    days = list(bundle["days"])
    if max(days) > through:
        raise ValueError("Requested through date precedes sealed history; explicit truncation required")
    ledgers = {name: [] for name in POLICIES}
    selections = {name: select(bundle["signals"], bundle, p) for name, p in POLICIES.items()}
    assert g2._selection_keys(selections["frozen_g2"]) == g2._selection_keys(bundle["orders"])
    needed = set(int(v) for frame in selections.values() for v in frame.sid)
    paths = {}
    with np.load(bundle["source"] / "dataset/paths.npz", allow_pickle=False) as archive:
        for fieldname in archive.files:
            sid, field = fieldname.split("_", 1)
            if int(sid) in needed:
                paths.setdefault(int(sid), {})[field] = archive[fieldname]
    for name, orders in selections.items():
        ledger = simulate(orders, paths, source)
        ledgers[name].append(ledger.assign(segment="SEALED_38"))
        print("Sealed", name, metrics(ledger, days), flush=True)
    provenance = []
    exclusions = []
    coverage_frames = []
    verified_snapshots = {}
    extension_folders = sorted(p for p in ext.DEFAULT_DAILY_ROOT.iterdir() if p.is_dir() and max(days).isoformat() < p.name <= through.isoformat())
    for folder in extension_folders:
        day = date.fromisoformat(folder.name)
        print("Verifying extension", day, flush=True)
        try:
            run_path, result = ext._latest_successful_run(ext.DEFAULT_DAILY_ROOT, day)
            manifest_path, snapshot_day = ext._snapshot_for_day(ext.DEFAULT_DAILY_ROOT, day, run_path, result)
            snapshot_manifest = g2.read_json(manifest_path)
            if snapshot_manifest.get("complete") is not True:
                raise ValueError("Input snapshot incomplete")
            if str(manifest_path) not in verified_snapshots:
                replay._verify_input_snapshot(manifest_path.parent, snapshot_manifest)
                verified_snapshots[str(manifest_path)] = g2.sha256(manifest_path)
            manifest = g2.read_json(run_path / "source_manifest.json")
            if manifest["frozen_config_sha256"] != g2.sha256(g2.DEFAULT_G_CONFIG):
                raise ValueError("Frozen G configuration hash drift")
            coverage = pd.read_csv(run_path / "coverage.csv")
            required = ["missing_equity_minutes", "missing_futures_bars"]
            bad = coverage.loc[coverage[required].ne(0).any(axis=1)]
            excluded_symbols = {item["symbol"] for item in result.get("coverage", {}).get("excluded_stocks", [])}
            undeclared_bad = bad.loc[~bad.symbol.isin(excluded_symbols)]
            if len(undeclared_bad):
                raise ValueError("Undeclared incomplete required data for " + ",".join(undeclared_bad.symbol.astype(str)))
            problems = result.get("coverage", {}).get("problems", [])
            if problems:
                raise ValueError("Daily replay has nonempty coverage problems")
            for record in manifest["sources"]:
                if record["role"] == "DATED_UNIVERSE" and g2.sha256(Path(record["path"])) != record["sha256"]:
                    raise ValueError("Dated universe drift")
            candidates_path = run_path / "candidate_signals.csv"
            candidates = normalize_candidates(pd.read_csv(candidates_path))
            features_path = run_path / "feature_ledger.csv"
            source_mode = "SEALED_STRICT_CANDIDATES_ONLY"
            feature_hash = None
            if features_path.exists():
                feature_hash = g2.sha256(features_path)
                feature_manifest = g2.read_json(run_path / "feature_ledger.csv.manifest.json")
                if feature_hash != feature_manifest["artifact_sha256"]:
                    raise ValueError("Feature ledger artifact drift")
                raw = audit.normalize(pd.read_csv(features_path))
                if not raw.day.eq(day).all():
                    raise ValueError("Feature ledger wrong date")
                # Rebuild raw strict branch. Do not inherit October6 relaxed0925 policy.
                nifty = float(raw.nifty_first_bar_return_pct.dropna().iloc[0])
                signals = replay._strict_signals(raw, nifty)
                source_mode = "SEALED_RAW_FEATURE_LEDGER_REBUILT_STRICT_SIGNALS"
                expected = set(zip(candidates.tradingsymbol, candidates.hhmm_int, candidates.side))
                actual = set(zip(signals.tradingsymbol, signals.hhmm_int, signals.side))
                if expected != actual:
                    raise ValueError("Raw-ledger strict signal identity differs from archived strict candidates")
            else:
                signals = candidates
            if set(signals.tradingsymbol).intersection(excluded_symbols):
                raise ValueError("Excluded symbols leaked into strict candidate pool")
            selected = {name: select(signals, bundle, p) for name, p in POLICIES.items()}
            # Pre-policy-change days can also compare frozen selection to published daily G.
            if day < date(2026, 10, 6):
                official = pd.read_csv(run_path / "selected_orders.csv")
                if len(official) != len(selected["frozen_g2"]):
                    raise ValueError("Pre-policy daily selection count parity failed")
                if len(official):
                    if set(key(official)) != set(key(selected["frozen_g2"])):
                        raise ValueError("Pre-policy daily selection identity parity failed")
            union = pd.concat(selected.values(), ignore_index=True).drop_duplicates("sid")
            paths = ext._selected_paths(union, day, manifest_path.parent) if len(union) else {}
            for name, orders in selected.items():
                ledger = simulate(orders, paths, source)
                if len(ledger):
                    ledgers[name].append(ledger.assign(segment=str(day)))
            days.append(day)
            coverage_frames.append(coverage.assign(date=str(day), source_run=str(run_path)))
            provenance.append(dict(day=str(day), run=str(run_path), source_mode=source_mode,
                source_manifest_sha256=g2.sha256(run_path / "source_manifest.json"), snapshot_manifest=str(manifest_path),
                snapshot_day=snapshot_day, candidate_signals_sha256=g2.sha256(candidates_path),
                feature_ledger_sha256=feature_hash, coverage=result.get("coverage", {}),
                strict_candidates=len(signals), selected_orders={k: len(v) for k, v in selected.items()},
                published_daily_policy=result.get("strategy_policy")))
            print("Extension", day, "orders", {name: len(v) for name,v in selected.items()}, flush=True)
        except (FileNotFoundError, RuntimeError, ValueError, KeyError) as exc:
            exclusions.append(dict(date=str(day), reason=str(exc), status="EXCLUDED_NO_VERIFIABLE_COMPLETE_EXTENSION"))
            print("Excluded", day, str(exc), flush=True)
        dump(output / "extension_progress.json", dict(included=provenance, excluded=exclusions))
    all_ledgers = {name: pd.concat(frames, ignore_index=True) for name, frames in ledgers.items()}
    archived = pd.read_csv(ARCHIVE / "portfolio_trades.csv")
    archive_days = sorted(pd.read_csv(ARCHIVE / "daily_results.csv").day.astype(str))
    baseline = all_ledgers["frozen_g2"]
    subset = baseline.loc[baseline.day.astype(str).isin(archive_days)].copy()
    if set(key(subset)) != set(key(archived)):
        raise ValueError("Published standard G2 archived selection identity mismatch")
    bcheck = subset.assign(_key=key(subset)).set_index("_key")
    acheck = archived.assign(_key=key(archived)).set_index("_key").reindex(bcheck.index)
    for field in ("filled", "portfolio_executed", "exit_reason"):
        if not bcheck[field].astype(str).str.lower().equals(acheck[field].astype(str).str.lower()):
            raise ValueError("Published standard G2 archived parity failed: " + field)
    if not np.allclose(bcheck.portfolio_net_profit_rupees.fillna(0), acheck.portfolio_net_profit_rupees.fillna(0), atol=1e-6, rtol=0):
        raise ValueError("Published standard G2 archived trade PnL parity failed")
    b_executed = executed(baseline)
    b_keys = set(key(b_executed))
    result_rows, daily_rows, changes = [], [], []
    for name, ledger in all_ledgers.items():
        ledger["trade_key"] = key(ledger)
        e = executed(ledger)
        added = e.loc[~e.trade_key.isin(b_keys)].copy()
        removed = b_executed.loc[~key(b_executed).isin(set(e.trade_key))].copy()
        result = metrics(ledger, days)
        result.update(variant=name, net_delta_vs_baseline=result["net_pnl"]-metrics(baseline, days)["net_pnl"],
            added_trades=len(added), added_winners=int(added.portfolio_net_profit_rupees.gt(0).sum()),
            added_losers=int(added.portfolio_net_profit_rupees.lt(0).sum()),
            added_net_pnl=float(added.portfolio_net_profit_rupees.sum()), removed_trades=len(removed),
            removed_winners=int(removed.portfolio_net_profit_rupees.gt(0).sum()),
            removed_losers=int(removed.portfolio_net_profit_rupees.lt(0).sum()),
            removed_net_pnl=float(removed.portfolio_net_profit_rupees.sum()),
            first_day=str(min(days)), last_day=str(max(days)))
        result_rows.append(result)
        ledger.to_csv(output / f"trades_{name}.csv", index=False)
        for day in sorted(days):
            d = ledger.loc[ledger.day.astype(str).eq(str(day))]
            daily_rows.append(dict(date=str(day), variant=name, **metrics(d, [day])))
        if len(added): changes.append(added.assign(variant=name, change="ADDED"))
        if len(removed): changes.append(removed.assign(variant=name, change="REMOVED"))
    daily = pd.DataFrame(daily_rows)
    base_daily = daily.loc[daily.variant.eq("frozen_g2")].set_index("date").net_pnl
    daily["net_delta_vs_baseline"] = daily.net_pnl-daily.date.map(base_daily)
    pd.DataFrame(result_rows).to_csv(output / "historical_comparison.csv", index=False)
    daily.to_csv(output / "historical_daywise.csv", index=False)
    pd.concat(changes, ignore_index=True).to_csv(output / "historical_added_removed.csv", index=False)
    pd.concat(coverage_frames, ignore_index=True).to_csv(output / "extension_coverage.csv", index=False)
    weekdays = pd.date_range(min(days), through, freq="B")
    unrepresented = [{"date":str(d.date()), "status":"NOT_IN_SEALED_COMPLETE_HISTORY", "note":"Not assumed to be a trading day: exchange holidays not independently classified here."}
                     for d in weekdays if d.date() not in days]
    pd.DataFrame(exclusions+unrepresented).to_csv(output / "unrepresented_dates.csv", index=False)
    scripts = [Path(__file__), Path(g2.__file__), Path(g2.g.__file__), Path(ext.__file__), Path(replay.__file__), Path(audit.__file__),
               Path(g2.g.v9.__file__), Path(g2.g.v9.v5.__file__), Path(g2.g.v9.v6.__file__)]
    result = dict(state="COMPLETE_AVAILABLE_SOURCE_ELIGIBLE_HISTORY_SENSITIVITY", policies=POLICIES,
        summary=result_rows, days=[str(d) for d in sorted(days)], exclusions=exclusions,
        unrepresented_weekdays=unrepresented, source_bundle=str(bundle["source"]),
        source_bundle_manifest_sha256=g2.sha256(bundle["source"] / "bundle_manifest.json"),
        dataset_manifest_sha256=g2.sha256(bundle["source"] / "dataset/dataset_manifest.json"),
        source_configuration_sha256=g2.sha256(g2.DEFAULT_G_CONFIG), frozen_g2_settings=settings,
        archived_g2_baseline=str(ARCHIVE), archived_g2_provenance_sha256=g2.sha256(ARCHIVE / "provenance.json"),
        archive_parity={"status":"EXACT_SELECTION_FILL_EXIT_AND_PNL_PARITY", "orders":len(subset), "days":archive_days,
                        "metrics":metrics(subset, archive_days)},
        verified_snapshots=verified_snapshots, extensions=provenance,
        code_sha256={str(p):g2.sha256(p) for p in scripts},
        sealed_history_eligibility_scope=bundle["dataset_manifest"].get("eligibility_scope"),
        limitations=["Historical membership is the sealed source-eligible universe, not today's 213 stocks projected backward.",
            "Daily SUCCESS can have explicitly excluded symbols. Those declared exclusions are retained, verified not to enter candidate selection, and reported per date; complete means usable-source complete, not everydatedsymbol complete.",
            "Post-strict candidate sensitivity: confirmation volume and 09:35 SHORT setup OI changes only. These fields do not change original strict preselection.",
            "This does not validate arbitrary EMA, base-OI, price, directional-confirmation, or new-slot policy changes; raw preselection and newly admitted paths require a separate full replay.",
            "All available verifiable complete extensions through requested date included. Dates absent from archive not synthesized; exclusion table distinguishes unavailable dates.",
            "Historical +2%/-2% capture and missed-opportunity counts not computed: official daily close/high/low reconciliation absent for every historical date. Added/removed trades are not a substitute.",
            "Retrospective reused-history sensitivity, not out-of-sample profit proof or a recommendation to deploy.",
            "Native costs 5bps round trip, fixed100000 capital x5 leverage, entry expiry10min, unchanged setup targets and15:15 square-off. No queue/liquidity/market-impact model.",
            "Closed-trade and day-close drawdowns are realized-equity measures; neither is intratrade mark-to-market drawdown.",
            "Frozen G and standard G2 untouched. Research adapter restores genuine feature observations after selection."],
        unvalidated_exact_top20_policies="Not replayed by this bounded diagnostic. Require exact policy definition and all preselection data/path coverage across historical dates.")
    dump(output / "history_provenance.json", result)
    print(pd.DataFrame(result_rows).to_string(index=False), flush=True)
    print("OUTPUT", output, flush=True)
    finalize(output)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("output", type=Path)
    parser.add_argument("--through", type=date.fromisoformat, default=date(2026, 10, 9))
    parser.add_argument("--finalize-only", action="store_true")
    args = parser.parse_args()
    finalize(args.output) if args.finalize_only else run(args.output, args.through)
