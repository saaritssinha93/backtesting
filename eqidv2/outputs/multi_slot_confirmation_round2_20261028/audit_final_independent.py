"""Independent end-of-run reconciliation; does not modify sealed artifacts."""
from __future__ import annotations
from datetime import datetime
import hashlib
import json
from pathlib import Path
from zoneinfo import ZoneInfo
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent
FINAL = ROOT / "consolidated"
ACCEPTED = Path(r"C:/TradingData/eqidv2/backtesting_result_v13_v10_g_7/v4_lock_backtest/runs/2026-10-28/20261028T143059782924_f86f1197_0926_short_wick020_lock2")
GROUPS = ("early", "midday", "late")


def sha(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for block in iter(lambda: handle.read(1024*1024), b""):
            digest.update(block)
    return digest.hexdigest()


def read(path):
    return json.loads(Path(path).read_text(encoding="utf-8-sig"))


def flags(series):
    values = series.astype(str).str.strip().str.lower()
    assert values.isin(["true", "false", "1", "0"]).all()
    return values.isin(["true", "1"])


def stats(frame, days):
    chosen = frame.loc[frame.day.astype(str).str[:10].isin(days)].copy()
    executed = chosen.loc[flags(chosen.portfolio_executed)]
    pnl = pd.to_numeric(executed.portfolio_net_profit_rupees, errors="raise")
    assert pnl.notna().all()
    return {"trades": len(executed), "wins": int(pnl.gt(1e-8).sum()),
            "losses": int(pnl.lt(-1e-8).sum()), "net_pnl_rs": float(pnl.sum()),
            "negative_trade_sum_abs_rs": float(-pnl.loc[pnl.lt(-1e-8)].sum())}


def equal_stats(actual, expected):
    for field in ("trades", "wins", "losses", "net_pnl_rs", "negative_trade_sum_abs_rs"):
        assert np.isclose(float(actual[field]), float(expected[field]), atol=1e-6, rtol=0), (field, actual[field], expected[field])


def gate(result, baseline, portfolio, base_portfolio):
    return (result.trades >= 8 and result.win_rate_pct > 75 and
            result.net_pnl_rs >= baseline.net_pnl_rs-1e-6 and
            result.negative_trade_sum_abs_rs < baseline.negative_trade_sum_abs_rs-1e-6 and
            portfolio.net_pnl_rs >= base_portfolio.net_pnl_rs-1e-6)


def main():
    evidence, manifest_members = [], 0
    for path in [ROOT/"feature_manifest.json", *[ROOT/g/"artifact_manifest.json" for g in GROUPS],
                 ROOT/"diagnosis"/"artifact_manifest.json", FINAL/"artifact_manifest.json"]:
        manifest = read(path)
        files = manifest.get("files", manifest)
        for name, digest in files.items():
            member = (path.parent/name).resolve()
            assert member.is_relative_to(path.parent.resolve())
            assert sha(member) == digest, str(member)
            manifest_members += 1
        evidence.append({"path": str(path), "sha256": sha(path), "members": len(files)})
    consolidated_proof = read(FINAL/"source_provenance.json")
    for path, expected in consolidated_proof["files"].items():
        assert sha(path) == expected
    protocol = read(ROOT/"protocol.json")
    variants = [r["variant"] for r in protocol["variants"]]
    setups = protocol["setups"]
    strategies = ["G3", "G7_V4_LOCK2"]
    baseline_parity_count = 0
    periods = None
    for group in GROUPS:
        proof = read(ROOT/group/"source_provenance.json")
        assert proof["protocol_sha256"] == sha(ROOT/"protocol.json")
        assert proof["candidate_pool_proof"]["feature_manifest_sha256"] == sha(ROOT/"feature_manifest.json")
        if periods is None:
            periods = proof["periods"]
        else:
            assert periods == proof["periods"]
        parity = read(ROOT/group/"baseline_reproduction.json")
        assert parity and all(item["passed"] is True for item in parity)
        baseline_parity_count += len(parity)
    assert len(periods["all"]) == 59 and periods["all"][-1] == "2026-10-27"
    assert periods["all"] == sorted(set(periods["all"]))
    assert periods["development"] == periods["all"][:35]
    assert periods["validation"] == periods["all"][35:49]
    assert periods["assessment"] == periods["all"][49:57]
    assert periods["final_sessions"] == periods["all"][57:]
    assert periods["later"] == periods["all"][35:]
    assert len(periods["october"]) == 16
    metrics = pd.read_csv(FINAL/"all_variant_results.csv")
    key = ["setup_id", "strategy", "variant", "scope", "cost_bps", "period"]
    assert len(metrics) == 7560 and not metrics.duplicated(key).any()
    indexed = metrics.set_index(key)
    def metric(setup, strategy, variant, period, scope="SETUP", cost=5):
        return indexed.loc[(setup, strategy, variant, scope, cost, period)]
    decisions = pd.read_csv(FINAL/"promotion_decisions.csv")
    assert len(decisions) == 20 and not decisions.duplicated(["setup_id", "strategy"]).any()
    dev_gates = pd.read_csv(FINAL/"development_gates.csv").set_index(["setup_id", "strategy", "variant"])
    assert len(dev_gates) == 160
    independently_chosen = []
    for setup in setups:
        for strategy in strategies:
            bs = metric(setup, strategy, "baseline", "development")
            bp = metric(setup, strategy, "baseline", "development", "FULL_PORTFOLIO")
            eligible = []
            for position, variant in enumerate(variants):
                if variant == "baseline":
                    continue
                result = metric(setup, strategy, variant, "development")
                portfolio = metric(setup, strategy, variant, "development", "FULL_PORTFOLIO")
                passed = bool(gate(result, bs, portfolio, bp))
                assert passed == bool(dev_gates.loc[(setup, strategy, variant)].eligible)
                if passed:
                    eligible.append((result.net_pnl_rs, -result.losses, -position, variant))
            chosen = max(eligible)[3] if eligible else "baseline"
            later = metric(setup, strategy, chosen, "later")
            base_later = metric(setup, strategy, "baseline", "later")
            passed = bool(gate(later, base_later, metric(setup,strategy,chosen,"later","FULL_PORTFOLIO"),
                               metric(setup,strategy,"baseline","later","FULL_PORTFOLIO")))
            status = "NO_DEVELOPMENT_QUALIFIER" if chosen == "baseline" else "REQUIRES_COMBINED_REPLAY" if passed else "REJECTED_LATER"
            decision = decisions.loc[decisions.setup_id.eq(setup)&decisions.strategy.eq(strategy)].iloc[0]
            assert decision.development_choice == chosen and decision.status == status
            assert decision.final_main_config == "UNCHANGED_ACCEPTED_BASELINE"
            independently_chosen.append({"setup_id":setup,"strategy":strategy,"choice":chosen,"status":status,
                "later_trades":int(later.trades),"later_wins":int(later.wins),"later_losses":int(later.losses),
                "later_win_rate_pct":float(later.win_rate_pct),"later_net_change_rs":float(later.net_pnl_rs-base_later.net_pnl_rs)})
    # Verify the CSV statistics against each saved actual 5bps portfolio book,
    # rather than merely checking one summary against another summary.
    book_checks = 0
    for group in GROUPS:
        for path in sorted((ROOT/group/"books").glob("*.pkl")):
            strategy, setup, variant = path.stem.split("__")
            frame = pd.read_pickle(path)
            for period, days in periods.items():
                equal_stats(metric(setup,strategy,variant,period), stats(frame.loc[frame.setup_id.eq(setup)],days))
                equal_stats(metric(setup,strategy,variant,period,"FULL_PORTFOLIO"),stats(frame,days))
                book_checks += 2
    assert book_checks == 2520
    ledger = {"G3":pd.read_csv(ACCEPTED/"g3_reference_portfolio_trades.csv"),
              "G7_V4_LOCK2":pd.read_csv(ACCEPTED/"portfolio_trades.csv")}
    accepted_checks = 0
    for strategy, frame in ledger.items():
        for setup in setups:
            for period, days in periods.items():
                equal_stats(metric(setup,strategy,"baseline",period),stats(frame.loc[frame.setup_id.eq(setup)],days))
                accepted_checks += 1
    assert accepted_checks == 140
    daywise = pd.read_csv(FINAL/"october_slot_daywise.csv")
    aggregate = pd.read_csv(FINAL/"october_requested_slots_daywise.csv")
    full = pd.read_csv(FINAL/"october_full_portfolio_daywise.csv")
    assert len(daywise) == 320 and len(aggregate) == 32 and len(full) == 32
    assert not daywise.duplicated(["day","setup_id","strategy"]).any()
    assert not aggregate.duplicated(["day","strategy"]).any()
    assert not full.duplicated(["day","strategy"]).any()
    for frame in (daywise,aggregate,full):
        assert frame.result_kind.eq("ACCEPTED_BASELINE_UNCHANGED").all()
    assert daywise.final_locked_variant.eq("baseline").all()
    for row in daywise.to_dict("records"):
        equal_stats(row, stats(ledger[row["strategy"]].loc[ledger[row["strategy"]].setup_id.eq(row["setup_id"])],[row["day"]]))
    for row in aggregate.to_dict("records"):
        equal_stats(row, stats(ledger[row["strategy"]].loc[ledger[row["strategy"]].setup_id.isin(setups)],[row["day"]]))
    for row in full.to_dict("records"):
        equal_stats(row, stats(ledger[row["strategy"]],[row["day"]]))
    overall = pd.read_csv(FINAL/"accepted_overall_results.csv")
    assert len(overall) == 8 and not overall.duplicated(["strategy","scope","period"]).any()
    assert overall.result_kind.eq("ACCEPTED_BASELINE_UNCHANGED").all()
    for row in overall.to_dict("records"):
        frame = ledger[row["strategy"]]
        if row["scope"] == "REQUESTED_10_SLOTS":
            frame = frame.loc[frame.setup_id.isin(setups)]
        else:
            assert row["scope"] == "FULL_PORTFOLIO"
        equal_stats(row, stats(frame, periods[row["period"]]))
    paired = pd.read_csv(FINAL/"october_requested_slots_paired.csv")
    assert len(paired) == 16 and paired.day.is_unique
    for row in paired.to_dict("records"):
        for strategy in strategies:
            expected = aggregate.loc[aggregate.day.eq(row["day"])&aggregate.strategy.eq(strategy)].iloc[0]
            for field in ("trades","wins","losses","net_pnl_rs"):
                assert np.isclose(row[strategy+"_"+field],expected[field],rtol=0,atol=1e-6)
    portfolio = pd.read_csv(FINAL/"full_portfolio_baseline_results.csv")
    assert len(portfolio) == 42 and not portfolio.duplicated(["strategy","cost_bps","period"]).any()
    assert portfolio.setup_id.eq("ALL_PORTFOLIO_SLOTS").all()
    assert pd.read_csv(FINAL/"baseline_cost_stress.csv").scope.eq("SETUP").all()
    diagnostic = pd.read_csv(FINAL/"development_selected_october_daywise.csv")
    assert diagnostic.result_kind.eq("DEVELOPMENT_SELECTED_DIAGNOSTIC_UNCOMBINED").all()
    assert set(diagnostic.setup_id) == {"1051_LONG"} and len(diagnostic) == 32
    summary = read(FINAL/"validation_summary.json")
    assert summary["status"] == "PASS" and summary["research_complete"] is True
    assert summary["survived_later"] == 0 and summary["combined_replay_performed"] is False
    assert summary["development_choices"] == 2 and summary["accepted_period_checks"] == accepted_checks
    assert summary["baseline_parity_checks"] == baseline_parity_count
    assert summary["main_files_changed"] is False and summary["independent_holdout"] is False
    report = (FINAL/"report.md").read_text(encoding="utf-8")
    assert "not independent validation" in report
    assert "isolated improvements must not be summed" in report
    assert "0 choices passed the later screening gates" in report
    assert "All main files remain at the accepted baseline" in report
    assert "not an exhaustive search" in report and "original real-time arrival" in report
    assert "not live realized returns" in report and "not proof of a specific market cause" in report
    assert "dates absent from the source are not manufactured" in report
    diagnosis = {}
    for strategy, frame in ledger.items():
        requested = frame.loc[frame.setup_id.isin(setups)&flags(frame.portfolio_executed)].copy()
        losses = requested.loc[pd.to_numeric(requested.portfolio_net_profit_rupees).lt(-1e-8)]
        times = losses.exit_reason.astype(str).str.contains("TIME",case=False)
        diagnosis[strategy] = {"losses":len(losses),"time_exit_losses":int(times.sum()),
                              "median_losing_hold_minutes":float(pd.to_numeric(losses.holding_minutes).median())}
        if strategy == "G7_V4_LOCK2":
            diagnosis[strategy]["protection_armed_losses"] = int(pd.to_numeric(losses.protection_armed_path_index,errors="coerce").notna().sum())
    assert diagnosis["G3"] == {"losses":48,"time_exit_losses":28,"median_losing_hold_minutes":132.0}
    assert diagnosis["G7_V4_LOCK2"] == {"losses":33,"time_exit_losses":20,"median_losing_hold_minutes":133.0,"protection_armed_losses":0}
    chosen_g7 = metric("1051_LONG","G7_V4_LOCK2","r2_close_location_ge_0p8","all")
    assert chosen_g7.trades == 11 and chosen_g7.wins == 9
    assert np.isclose(chosen_g7.net_change_vs_baseline_rs,20.88,atol=.005)
    assert np.isclose(chosen_g7.wilson_95_lower_pct,52.30,atol=.005)
    late_exception = metric("1301_SHORT","G7_V4_LOCK2","r2_close_location_ge_0p8","all")
    assert late_exception.trades == 10 and late_exception.wins == 8
    assert np.isclose(late_exception.net_change_vs_baseline_rs,2992.08,atol=.005)
    result = {"status":"PASS","auditor":"independent_round2_causal_audit",
        "created_at_ist":datetime.now(ZoneInfo("Asia/Kolkata")).isoformat(),
        "sealed_artifacts_modified":False,"source_code_sha256":sha(Path(__file__)),
        "manifest_members_verified":manifest_members,"manifests":evidence,
        "source_provenance_hashes_verified":len(consolidated_proof["files"]),
        "metric_rows":len(metrics),"development_gates_independently_recomputed":160,
        "setup_strategy_choices_independently_recomputed":20,"saved_book_period_scope_checks":book_checks,
        "accepted_period_checks":accepted_checks,"baseline_parity_checks":baseline_parity_count,
        "october_slot_rows_verified":len(daywise),"october_requested_aggregate_rows_verified":len(aggregate),
        "october_fullportfolio_rows_verified":len(full),"fullportfolio_baseline_metric_rows_unique":len(portfolio),
        "accepted_overall_ledger_rows_verified":len(overall),"paired_october_rows_verified":len(paired),
        "report_loss_diagnosis_independently_verified":diagnosis,
        "diagnostic_october_rows_separately_labelled":len(diagnostic),
        "development_choices":[x for x in independently_chosen if x["choice"]!="baseline"],
        "combined_replay_required":False,"reason":"All development choices failed frozen later gates",
        "wording_audit":"Baseline, isolated diagnostics, and full portfolio remain separate; no uncombined aggregate gains claimed",
        "issues":[]}
    (ROOT/"final_independent_audit.json").write_text(json.dumps(result,indent=2,allow_nan=False)+"\n",encoding="utf-8")
    print(json.dumps(result,indent=2,allow_nan=False))


if __name__ == "__main__":
    main()
