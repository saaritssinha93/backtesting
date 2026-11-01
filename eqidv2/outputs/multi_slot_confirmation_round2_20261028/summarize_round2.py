"""Reconcile the frozen confirmation experiment, retaining all rejected evidence."""
from __future__ import annotations

import hashlib
import importlib.util
import json
from pathlib import Path
from statistics import NormalDist

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent
OUT = ROOT / "consolidated"
PRIOR = ROOT.parent / "multi_slot_short_iteration_20261028" / "consolidated"
spec = importlib.util.spec_from_file_location("prior_summary", PRIOR / "build_consolidated.py")
old = importlib.util.module_from_spec(spec)
spec.loader.exec_module(old)
STRATEGIES = ("G3", "G7_V4_LOCK2")
GROUPS = ("early", "midday", "late")


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def write_json(path, value):
    Path(path).write_text(json.dumps(value, indent=2, default=str) + "\n", encoding="utf-8")


def verify_manifest(path):
    data = json.loads(Path(path).read_text(encoding="utf-8-sig"))
    members = data["files"] if "files" in data else data
    for name, expected in members.items():
        if sha(Path(path).parent / name) != expected:
            raise ValueError(f"Changed manifest member: {path}/{name}")
    return len(members)


def gates(result, baseline, portfolio, base_portfolio):
    return {
        "minimum_8_trades": bool(result.trades >= 8),
        "win_rate_gt_75": bool(result.win_rate_pct > 75),
        "setup_net_preserved": bool(result.net_pnl_rs >= baseline.net_pnl_rs - 1e-6),
        "losing_amount_reduced": bool(result.negative_trade_sum_abs_rs < baseline.negative_trade_sum_abs_rs - 1e-6),
        "portfolio_net_preserved": bool(portfolio.net_pnl_rs >= base_portfolio.net_pnl_rs - 1e-6),
    }


def main():
    OUT.mkdir(exist_ok=True)
    protocol = json.loads((ROOT / "protocol.json").read_text())
    setups = protocol["setups"]
    order = {r["variant"]: i for i, r in enumerate(protocol["variants"])}
    metrics, reported, parity, manifests, period_map = [], [], [], [], None
    verify_manifest(ROOT / "feature_manifest.json")
    for group in GROUPS:
        directory = ROOT / group
        count = verify_manifest(directory / "artifact_manifest.json")
        manifests.append({"group": group, "files_verified": count, "manifest_sha256": sha(directory / "artifact_manifest.json")})
        controls = json.loads((directory / "baseline_reproduction.json").read_text())
        assert all(r["passed"] for r in controls)
        parity.extend(controls)
        provenance = json.loads((directory / "source_provenance.json").read_text())
        assert provenance["protocol_sha256"] == sha(ROOT / "protocol.json")
        assert provenance["candidate_pool_proof"]["feature_manifest_sha256"] == sha(ROOT / "feature_manifest.json")
        if period_map is None:
            period_map = provenance["periods"]
        else:
            assert period_map == provenance["periods"]
        for scope, filename in [("SETUP", "variant_setup_metrics.csv"), ("FULL_PORTFOLIO", "variant_portfolio_metrics.csv")]:
            frame = pd.read_csv(directory / filename)
            frame["scope"] = scope
            frame["group"] = group
            metrics.append(frame)
        reported.append(pd.read_csv(directory / "development_selection.csv"))
    all_metrics = pd.concat(metrics, ignore_index=True)
    reported = pd.concat(reported, ignore_index=True)
    keys = ["setup_id", "strategy", "variant", "scope", "cost_bps", "period"]
    assert not all_metrics.duplicated(keys).any()
    assert len(all_metrics) == 10 * 2 * 9 * 2 * 3 * 7
    assert len(reported) == 160
    assert period_map["all"][-1] == "2026-10-27" and len(period_map["all"]) == 59
    all_metrics["selection_status"] = "EXPLORATORY_NOT_SELECTED_ON_ALL_HISTORY"
    # Context interval accounts for the 220 previous and 160 current comparisons.
    z = NormalDist().inv_cdf(1 - 0.05 / (2 * 380))
    n = all_metrics.trades.to_numpy(float)
    p = np.divide(all_metrics.wins.to_numpy(float), n, out=np.full_like(n, np.nan), where=n > 0)
    nonzero_n = np.where(n > 0, n, np.nan)
    all_metrics["wilson_familywise_380_lower_pct"] = 100 * (p + z*z/(2*nonzero_n) - z*np.sqrt(p*(1-p)/nonzero_n + z*z/(4*nonzero_n*nonzero_n))) / (1+z*z/nonzero_n)

    gate_rows, choices, final_rows, diagnostics = [], [], [], []
    def metric(setup, strategy, variant, period, scope="SETUP", cost=5):
        q = all_metrics.loc[(all_metrics.setup_id == setup) & (all_metrics.strategy == strategy) &
            (all_metrics.variant == variant) & (all_metrics.period == period) &
            (all_metrics.scope == scope) & (all_metrics.cost_bps == cost)]
        assert len(q) == 1
        return q.iloc[0]

    for setup in setups:
        for strategy in STRATEGIES:
            bs = metric(setup, strategy, "baseline", "development")
            bp = metric(setup, strategy, "baseline", "development", "FULL_PORTFOLIO")
            eligible = []
            for variant in list(order)[1:]:
                row = metric(setup, strategy, variant, "development")
                port = metric(setup, strategy, variant, "development", "FULL_PORTFOLIO")
                checks = gates(row, bs, port, bp)
                expected = reported.loc[(reported.setup_id == setup) & (reported.strategy == strategy) & (reported.variant == variant)].iloc[0]
                assert all(checks.values()) == bool(expected.eligible)
                gate_rows.append({"setup_id": setup, "strategy": strategy, "variant": variant, **checks,
                    "gates_passed": sum(checks.values()), "eligible": all(checks.values()),
                    "trades": row.trades, "wins": row.wins, "losses": row.losses, "win_rate_pct": row.win_rate_pct,
                    "wilson_95_lower_pct": row.wilson_95_lower_pct,
                    "setup_net_change_rs": row.net_pnl_rs-bs.net_pnl_rs,
                    "losing_amount_reduction_rs": bs.negative_trade_sum_abs_rs-row.negative_trade_sum_abs_rs,
                    "portfolio_net_change_rs": port.net_pnl_rs-bp.net_pnl_rs})
                if all(checks.values()):
                    eligible.append((row.net_pnl_rs, -row.losses, -order[variant], variant))
            chosen = max(eligible)[3] if eligible else "baseline"
            rep_selected = reported.loc[(reported.setup_id == setup) & (reported.strategy == strategy) & reported.selected_from_development]
            assert (len(rep_selected) == 0 and chosen == "baseline") or (len(rep_selected) == 1 and rep_selected.iloc[0].variant == chosen)
            result = metric(setup, strategy, chosen, "later")
            baseline = metric(setup, strategy, "baseline", "later")
            port = metric(setup, strategy, chosen, "later", "FULL_PORTFOLIO")
            baseport = metric(setup, strategy, "baseline", "later", "FULL_PORTFOLIO")
            checks = gates(result, baseline, port, baseport)
            selected_all = metric(setup, strategy, chosen, "all")
            status = "NO_DEVELOPMENT_QUALIFIER" if chosen == "baseline" else ("REQUIRES_COMBINED_REPLAY" if all(checks.values()) else "REJECTED_LATER")
            choices.append({"setup_id": setup, "strategy": strategy, "development_choice": chosen,
                "status": status, **{"later_"+k:v for k,v in checks.items()},
                "later_trades": result.trades, "later_wins": result.wins, "later_win_rate_pct": result.win_rate_pct,
                "later_net_change_rs": result.net_pnl_rs-baseline.net_pnl_rs,
                "all_trades": selected_all.trades, "all_wins": selected_all.wins,
                "all_win_rate_pct": selected_all.win_rate_pct, "all_wilson_lower_pct": selected_all.wilson_95_lower_pct,
                "all_net_change_rs": selected_all.net_change_vs_baseline_rs,
                "final_main_config": "UNCHANGED_ACCEPTED_BASELINE"})
            if chosen != "baseline":
                diagnostics.append(all_metrics.loc[(all_metrics.setup_id==setup)&(all_metrics.strategy==strategy)&(all_metrics.variant==chosen)].copy())
            b = metric(setup, strategy, "baseline", "all")
            octb = metric(setup, strategy, "baseline", "october")
            devb = metric(setup, strategy, "baseline", "development")
            final_rows.append({"setup_id": setup, "strategy": strategy, "baseline_trades": b.trades,
                "baseline_wins": b.wins, "baseline_losses": b.losses, "baseline_win_rate_pct": b.win_rate_pct,
                "baseline_net_pnl_rs": b.net_pnl_rs, "development_choice": chosen,
                "candidate_dev_trades": metric(setup,strategy,chosen,"development").trades,
                "candidate_dev_win_rate_pct": metric(setup,strategy,chosen,"development").win_rate_pct,
                "candidate_later_win_rate_pct": result.win_rate_pct,
                "candidate_all_win_rate_pct": selected_all.win_rate_pct,
                "candidate_all_net_pnl_rs": selected_all.net_pnl_rs, "status": status,
                "baseline_october_trades": octb.trades, "baseline_october_wins": octb.wins,
                "baseline_october_losses": octb.losses, "baseline_october_net_pnl_rs": octb.net_pnl_rs,
                "baseline_dev_trades": devb.trades})
    decisions = pd.DataFrame(choices)
    gate_table = pd.DataFrame(gate_rows)
    all_metrics.to_csv(OUT / "all_variant_results.csv", index=False)
    gate_table.to_csv(OUT / "development_gates.csv", index=False)
    decisions.to_csv(OUT / "promotion_decisions.csv", index=False)
    final = pd.DataFrame(final_rows)
    final.to_csv(OUT / "slot_comparison.csv", index=False)
    pd.concat(diagnostics,ignore_index=True).to_csv(OUT / "development_selected_diagnostics.csv",index=False) if diagnostics else all_metrics.iloc[:0].to_csv(OUT / "development_selected_diagnostics.csv",index=False)
    # Reconcile all 20 baseline pairs, 7 periods with the accepted immutable ledger.
    ledgers = {s:old.accepted_ledger(old.ACCEPTED/f,s) for s,f in [("G3","g3_reference_portfolio_trades.csv"),("G7_V4_LOCK2","portfolio_trades.csv")]}
    checks = 0
    for setup in setups:
        for s in STRATEGIES:
            for period, days in period_map.items():
                truth=old.ledger_metrics(ledgers[s].loc[ledgers[s].setup_id==setup],days)
                row=metric(setup,s,"baseline",period)
                for c in ["trades","wins","losses","net_pnl_rs","negative_trade_sum_abs_rs"]:
                    assert np.isclose(float(row[c]),float(truth[c]),atol=1e-6,rtol=0),(setup,s,period,c)
                checks+=1
    days=period_map["october"]
    final_variants={(setup,s):"baseline" for setup in setups for s in STRATEGIES}
    daywise=old.october_setup_daywise(ledgers,days,final_variants)
    daywise["result_kind"]="ACCEPTED_BASELINE_UNCHANGED"
    daywise.to_csv(OUT/"october_slot_daywise.csv",index=False)
    requested_days = old.aggregate_daywise(daywise).assign(result_kind="ACCEPTED_BASELINE_UNCHANGED")
    requested_days.to_csv(OUT/"october_requested_slots_daywise.csv",index=False)
    paired_days = requested_days.pivot(index="day", columns="strategy", values=["trades", "wins", "losses", "net_pnl_rs"])
    paired_days.columns = [f"{strategy}_{measure}" for measure, strategy in paired_days.columns]
    paired_days.to_csv(OUT/"october_requested_slots_paired.csv")
    old.full_portfolio_daywise(ledgers,days).assign(result_kind="ACCEPTED_BASELINE_UNCHANGED").to_csv(OUT/"october_full_portfolio_daywise.csv",index=False)
    totals = []
    for strategy, ledger in ledgers.items():
        for scope in ["REQUESTED_10_SLOTS", "FULL_PORTFOLIO"]:
            rows = ledger.loc[ledger.setup_id.isin(setups)] if scope == "REQUESTED_10_SLOTS" else ledger
            for period in ["all", "october"]:
                totals.append({"strategy": strategy, "scope": scope, "period": period,
                    "cost_bps": 5, "result_kind": "ACCEPTED_BASELINE_UNCHANGED",
                    **old.ledger_metrics(rows, period_map[period])})
    totals = pd.DataFrame(totals)
    totals.to_csv(OUT/"accepted_overall_results.csv", index=False)
    all_metrics.loc[(all_metrics.variant=="baseline")&(all_metrics.scope=="SETUP")&(all_metrics.period.isin(["all","october"]))].to_csv(OUT/"baseline_cost_stress.csv",index=False)
    ports=all_metrics.loc[(all_metrics.variant=="baseline")&(all_metrics.scope=="FULL_PORTFOLIO")]
    for _,group in ports.groupby(["strategy","cost_bps","period"]):
        for col in ["trades","wins","losses","net_pnl_rs","negative_trade_sum_abs_rs","daily_close_drawdown_rs"]:
            assert np.allclose(group[col],group.iloc[0][col],rtol=0,atol=1e-5)
    ports=ports.drop_duplicates(["strategy","cost_bps","period"]).copy()
    ports["setup_id"]="ALL_PORTFOLIO_SLOTS"
    ports.to_csv(OUT/"full_portfolio_baseline_results.csv",index=False)
    diagnostic_days=[]
    for group in GROUPS:
        table=pd.read_csv(ROOT/group/"selected_variant_daywise.csv")
        table=table.loc[table.day.astype(str).str.startswith("2026-10") & table.variant.ne("baseline")].copy()
        table["result_kind"]="DEVELOPMENT_SELECTED_DIAGNOSTIC_UNCOMBINED"
        diagnostic_days.append(table)
    pd.concat(diagnostic_days,ignore_index=True).to_csv(OUT/"development_selected_october_daywise.csv",index=False)
    surviving=int(decisions.status.eq("REQUIRES_COMBINED_REPLAY").sum())
    summary={"status":"PENDING_COMBINED_REPLAY" if surviving else "PASS", "research_complete":not bool(surviving),
        "combined_replay_performed":False,"frozen_data_end":"2026-10-27","oct28_status":"EXCLUDED_FROM_FROZEN_COMPARISON",
        "prior_comparisons":220,"round2_comparisons":160,"cumulative_comparisons":380,
        "feature_candidates":259,"batch_files_verified":sum(r["files_verified"] for r in manifests),
        "baseline_parity_checks":len(parity),"accepted_period_checks":checks,
        "development_eligible_rules":int(gate_table.eligible.sum()),
        "development_choices":int(decisions.development_choice.ne("baseline").sum()),
        "survived_later":surviving,
        "main_files_changed":False,"independent_holdout":False,"batch_manifests":manifests,
        "protocol_sha256":sha(ROOT/"protocol.json"),"feature_manifest_sha256":sha(ROOT/"feature_manifest.json")}
    write_json(OUT/"validation_summary.json",summary)
    verify_manifest(ROOT/"diagnosis"/"artifact_manifest.json")
    provenance_files=[Path(__file__),ROOT/"run_round2.py",ROOT/"protocol.json",ROOT/"feature_manifest.json",ROOT/"diagnosis"/"artifact_manifest.json",
        PRIOR/"build_consolidated.py",old.ACCEPTED/"g3_reference_portfolio_trades.csv",old.ACCEPTED/"portfolio_trades.csv"]
    write_json(OUT/"source_provenance.json",{"files":{str(p.resolve()):sha(p) for p in provenance_files},
        "batch_manifests":manifests,"main_files_changed":False,"independent_holdout":False})
    lines=["# Confirmation rules: second research round","",
        "Frozen evidence: 59 sessions from 2026-07-29 through 2026-10-27. October 28 is excluded to preserve the accepted baseline comparison; this is not a statement about its current collection status.","",
        f"Eight additional confirmation rules produced 160 new comparisons, following 220 previous comparisons (380 cumulative). {summary['development_eligible_rules']} rule rows passed the fixed development gates; {summary['development_choices']} choices were fixed from development. {summary['survived_later']} choices passed the later screening gates.","",
        "All main files remain at the accepted baseline. Development-only selections and attractive retrospective rows are disclosed as diagnostics. Every later block has already been inspected, so this is not independent validation.","",
        "Rules: confirmation volume versus previous 10 minutes at 1.0/1.5; session EMA5/13 alignment and EMA5 slope; directional VWAP distance in 0..0.5%/1.0%; 10-minute directional efficiency >=0.3; previous 3-minute breakout; directional candle close location >=0.8. All features use completed bars at confirmation and apply before ranking/quota.","",
        "The duplicate 09:31 SHORT request is evaluated once. Additional filters tighten the original eligible candidate pool; lowering the original eligibility thresholds was not tested. The experiment is therefore not an exhaustive search of all possible confirmations.","",
        "Development and later screening each require at least 8 executed trades, win rate strictly above 75%, setup and full-portfolio net at least baseline, and strictly reduced losing amount. Eight trades is not statistical proof. Wilson bounds remain descriptive under repeated selection.","",
        f"Development dates: {period_map['development'][0]} to {period_map['development'][-1]} ({len(period_map['development'])} sessions). Later dates: {period_map['later'][0]} to {period_map['later'][-1]} ({len(period_map['later'])} sessions). Rules were fixed before this round's outcome calculations, and development choices were not replaced after inspecting later results.","",
        "All rupee amounts below are after the modeled 5 bps round-trip charge. Stress replays at 9 and 15 bps are included. The nominal sizing is Rs 100,000 capital at 5x leverage per entry; G7 uses integer quantity. These are modeled fills and costs, not live realized returns.","",
        "| Slot | Strategy | Baseline W/L, rate | Development choice | Dev rate | Later rate | All candidate rate | Decision |",
        "|---|---|---:|---|---:|---:|---:|---|"]
    def pct(v): return "n/a" if pd.isna(v) else f"{v:.2f}%"
    for r in final.itertuples():
        lines.append(f"| {r.setup_id} | {r.strategy} | {int(r.baseline_wins)}/{int(r.baseline_losses)}, {pct(r.baseline_win_rate_pct)} | {r.development_choice} | {pct(r.candidate_dev_win_rate_pct)} | {pct(r.candidate_later_win_rate_pct)} | {pct(r.candidate_all_win_rate_pct)} | {r.status} |")
    lines += ["", "Only 10:51 LONG's top-20%-of-candle close passed development in both strategies. G3: 11 wins/14 trades (78.57%) in development, then 1/2 (50%) later, with later net Rs 4,300 below baseline. G7: 8/9 (88.89%), then 1/2 (50%), with later net Rs 904.44 below baseline. Both are rejected. G7's attractive all-history 9/11 (81.82%) adds only Rs 20.88 overall and has a 95% Wilson lower bound of 52.30%; it does not establish a reliable >75% win rate.", "",
        "The late-slot retrospective exception is G7 13:01 SHORT with close location >=0.8: 8/10 wins overall and Rs 2,992.08 extra net. Its development result is only 4/6 wins (66.67%) and later 4/4 is too small; it was not selected. Reporting it as a validated improvement would conceal those failures.", "",
        "| Scope | Period | Strategy | Trades | Wins | Losses | Win rate | Net Rs |",
        "|---|---|---|---:|---:|---:|---:|---:|"]
    for r in totals.itertuples():
        lines.append(f"| {r.scope} | {r.period} | {r.strategy} | {int(r.trades)} | {int(r.wins)} | {int(r.losses)} | {pct(r.win_rate_pct)} | {r.net_pnl_rs:,.2f} |")
    lines += ["", "Loss diagnosis from the accepted requested-slot trades: G3 had 48 losses, including 28 time exits; G7 had 33 losses, including 20 time exits. Median losing holds were 132 and 133 minutes. Holding time does not establish when a reversal began. None of G7's 33 losses armed its completed-close +0.6% protection. The protection cannot rescue trades that never reach its arming condition.", "",
        "Requested-slot October win rates fell to 20/45 (44.44%) for G3 and 14/31 (45.16%) for G7, versus 79/102 (77.45%) and 54/70 (77.14%) earlier. This is a deterioration in this sample, not proof of a specific market cause. The detailed diagnosis also records the intrabar limitations of maximum-favorable-excursion measurements.", "",
        "| Available October date | Requested slots G3 net Rs | Requested slots G7 net Rs |",
        "|---|---:|---:|"]
    for day, row in paired_days.iterrows():
        lines.append(f"| {day} | {row.G3_net_pnl_rs:,.2f} | {row.G7_V4_LOCK2_net_pnl_rs:,.2f} |")
    lines += ["", "Daywise tables contain the 16 available October sessions (October 5-27); dates absent from the source are not manufactured as zero-result sessions. All accepted result tables remain baseline because no change survived. Development-selected daywise rows are separate rejected diagnostics.", "",
        "All 259 candidate features were reconstructed using bars at or before confirmation. Exact minute coverage, source flags, candle matching and future-bar invariance checks passed. Sources include immutable snapshots and historical parquet files; timestamp cutoff proves calculation causality, not the original real-time arrival or revision status of every bar."]
    lines += ["",f"Validation: {len(parity)} exact baseline replay checks, {checks} accepted-ledger period reconciliations, and {summary['batch_files_verified']} batch member hashes passed. October has 320 slot/strategy/day rows including zero-trade sessions.","",
        "See slot_comparison.csv, development_gates.csv, all_variant_results.csv, promotion_decisions.csv, baseline_cost_stress.csv, and October daywise files. If any choice is marked REQUIRES_COMBINED_REPLAY it is not deployable and its isolated improvements must not be summed."]
    (OUT/"report.md").write_text("\n".join(lines)+"\n",encoding="utf-8")
    write_json(OUT/"artifact_manifest.json",{"files":{p.name:sha(p) for p in OUT.iterdir() if p.is_file() and p.name!="artifact_manifest.json"},"scope":"BACKTEST_ONLY","main_files_changed":False})
    print(json.dumps(summary,indent=2))
    print(decisions[["setup_id","strategy","development_choice","status","later_win_rate_pct","all_win_rate_pct"]].to_string(index=False))


if __name__ == "__main__":
    main()
