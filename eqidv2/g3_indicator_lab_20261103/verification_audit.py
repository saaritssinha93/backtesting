"""Independent reconciliation audit; writes only VERIFICATION.md/json."""
from __future__ import annotations
import argparse
import json
from pathlib import Path
from datetime import datetime, timezone
import numpy as np
import pandas as pd
from sklearn.metrics import roc_auc_score, log_loss, brier_score_loss
from g3_v18_20261102.run import sha, dump, guard_writes
from .run import control_parity
from .finalize_partition import COMPUTATIONAL

PACKAGE = Path(__file__).resolve().parent


def close(a, b, label):
    if not np.isclose(float(a), float(b), rtol=1e-10, atol=1e-5, equal_nan=True):
        raise AssertionError(f"{label}: {a} != {b}")


def stats(book, days, cost):
    rows = book.loc[book.day.isin(days)]
    done = rows.loc[rows.portfolio_executed.eq(True)].copy()
    pnl = done.portfolio_gross_profit_rupees - done.portfolio_trade_exposure_rupees * cost / 10000.
    daypnl = pnl.groupby(done.day).sum().reindex(days, fill_value=0.)
    cumulative = np.r_[0., daypnl.cumsum()]
    gains, losses = pnl.clip(lower=0).sum(), -pnl.clip(upper=0).sum()
    return dict(sessions=len(days), selected_orders=len(rows), native_fills=int(rows.filled.eq(True).sum()),
                trades=len(done), wins=int(pnl.gt(0).sum()), losses=int(pnl.lt(0).sum()),
                net_pnl_rs=float(pnl.sum()), costs_rs=float(done.portfolio_trade_exposure_rupees.sum()*cost/10000.),
                win_rate_pct=float(pnl.gt(0).mean()*100) if len(done) else np.nan,
                profit_factor=float(gains/losses) if losses else np.nan,
                average_trade_rs=float(pnl.mean()) if len(done) else np.nan,
                losing_days=int(daypnl.lt(0).sum()), winning_days=int(daypnl.gt(0).sum()),
                worst_day_rs=float(daypnl.min()), best_day_rs=float(daypnl.max()),
                daily_close_drawdown_rs=float((np.maximum.accumulate(cumulative)-cumulative).max()))


def concentration(book, daily, variant):
    wide = daily.loc[daily.cost_bps.eq(5)].pivot(index="day", columns="variant", values="net_pnl_rs")
    delta = wide[variant]-wide.control
    done = book.loc[book.portfolio_executed.eq(True)]
    key = ["day", "setup_id", "tradingsymbol"]
    base = done.loc[done.variant.eq("control")].set_index(key).portfolio_net_profit_rupees
    arm = done.loc[done.variant.eq(variant)].set_index(key).portfolio_net_profit_rupees
    joined = pd.concat([base.rename("base"), arm.rename("arm")], axis=1).fillna(0.)
    joined["delta"] = joined.arm-joined.base
    close(joined.delta.sum(), delta.sum(), "trade attribution sums to paired-day delta")
    cost_totals = daily.groupby(["variant", "cost_bps"]).net_pnl_rs.sum().unstack()
    return dict(variant=variant, delta_net_rs=float(delta.sum()), improved_days=int(delta.gt(1e-6).sum()),
                worsened_days=int(delta.lt(-1e-6).sum()), monthly_delta_rs=delta.groupby(delta.index.str[:7]).sum().to_dict(),
                cost_delta_rs=(cost_totals.loc[variant]-cost_totals.loc["control"]).to_dict(),
                largest_day_improvements_rs=delta.nlargest(5).to_dict(), largest_day_deteriorations_rs=delta.nsmallest(3).to_dict(),
                largest_trade_contributions=joined.nlargest(5,"delta").reset_index().to_dict("records"),
                total_positive_day_delta_rs=float(delta.clip(lower=0).sum()),
                total_negative_day_delta_rs=float(delta.clip(upper=0).sum()),
                without_best_day_delta_rs=float(delta.sum()-delta.nlargest(1).sum()),
                without_best_two_days_delta_rs=float(delta.sum()-delta.nlargest(2).sum()),
                without_best_trade_contribution_delta_rs=float(delta.sum()-joined.delta.nlargest(1).sum()),
                without_best_two_trade_contributions_delta_rs=float(delta.sum()-joined.delta.nlargest(2).sum()),
                note="Contribution deletion is an attribution sensitivity, not a fresh portfolio replay; capital reuse could change after removing an order.")


def audit(result):
    result = result.resolve()
    if not result.is_relative_to(PACKAGE):
        raise ValueError("Lab result required")
    guard_writes(PACKAGE)
    config = json.loads((result/"config.json").read_text())
    days = config["days"]
    protocol = config["protocol"]
    manifest = json.loads((PACKAGE/"inputs/manifest.json").read_text())
    assert days == sorted(set(days)) == manifest["days"] and len(days)==63
    assert sha(PACKAGE/"inputs/manifest.json")==config["input_manifest_sha256"]
    assert sha(PACKAGE/"inputs/protocol.json")==manifest["protocol_sha256"]
    assert json.loads((PACKAGE/"inputs/protocol.json").read_text())==protocol
    code_hashes = {}
    for name in sorted(COMPUTATIONAL):
        code_hashes[name] = sha(PACKAGE/name)
        assert code_hashes[name]==config["code_hashes"][name], name
    partition_days=[]
    for part in config["partitions"]:
        path=Path(part["path"])
        assert sha(path/"config.json")==part["config_sha256"]
        c=json.loads((path/"config.json").read_text())
        assert c["protocol"]==protocol and c["input_manifest_sha256"]==config["input_manifest_sha256"]
        assert {k:c["code_hashes"][k] for k in COMPUTATIONAL}==code_hashes
        partition_days.extend(c["days"])
    assert sorted(partition_days)==days
    rules=pd.read_parquet(result/"trades.parquet")
    rules["day"]=rules.day.astype(str).str[:10]
    rules_daily=pd.read_csv(result/"daily_results.csv")
    parities=json.loads((result/"control_parity.json").read_text())
    assert len(parities)==63 and {r["day"] for r in parities}==set(days) and all(r["passed"] for r in parities)
    daymap=json.loads((result/"day_artifacts.json").read_text())
    assert set(daymap)==set(days)
    fresh_parities=[]; expansions=[]
    for rec in manifest["sessions"]:
        day=rec["day"]
        for name,key in (("raw.parquet","raw_sha256"),("minutes.parquet","minutes_sha256")):
            assert sha(PACKAGE/"inputs"/day/name)==rec[key], (day,name)
        raw=pd.read_parquet(PACKAGE/"inputs"/day/"raw.parquet")
        control=rules.loc[rules.day.eq(day)&rules.variant.eq("control")]
        fresh_parities.append(control_parity(raw,control,control))
        folder=Path(daymap[day])
        original=pd.read_parquet(folder/"trades.parquet")
        merged=rules.loc[rules.day.eq(day)]
        cols=["variant","sid","portfolio_executed","portfolio_net_profit_rupees","entry_price","exit_price"]
        pd.testing.assert_frame_equal(merged[cols].sort_values(["variant","sid"]).reset_index(drop=True),
            original[cols].sort_values(["variant","sid"]).reset_index(drop=True),check_dtype=False)
        if (folder/"slot_expansion.json").exists():
            expansion=json.loads((folder/"slot_expansion.json").read_text())
            assert sha(Path(expansion["path"]))==expansion["sha256"]
            supplemental=pd.read_parquet(expansion["path"])
            assert len(supplemental)==expansion["extra_rows"] and set(supplemental.tradingsymbol).issubset(set(raw.tradingsymbol))
            consumed=pd.read_parquet(folder/"all51_gates.parquet")
            extra_ids=manifest["days"].index(day)*1_000_000+900_000+np.arange(len(supplemental))
            assert set(consumed.loc[consumed.sid.ge(extra_ids.min()),"sid"]).issubset(set(extra_ids))
            expansions.append(dict(day=day,**expansion,current_hash_matches_consumed=True))
        if len(fresh_parities)%10==0:
            print(f"Audit: {len(fresh_parities)}/63 original-control selections, saved ledgers, and frozen inputs verified",flush=True)
    assert len(expansions)==4
    # Read only sealed model and report artifacts; never manufacture zero rows.
    assert (result/"models/complete.json").exists(), "Model allocation not complete"
    assert (result/"summary.json").exists(), "Report not complete"
    model=pd.read_parquet(result/"models/trades.parquet"); model["day"]=model.day.astype(str).str[:10]
    model_daily=pd.read_csv(result/"models/daily_results.csv")
    books=pd.concat([rules,model],ignore_index=True)
    daily=pd.concat([rules_daily,model_daily],ignore_index=True)
    primary=protocol["rules"]+protocol["models"]
    stress=[v+"__trigger1bp" for v in ("control","or_market","group_score","or_group_score")]
    assert len(primary)==len(set(primary))==24 and len(set(primary+stress))==28
    assert set(books.variant)==set(primary+stress)
    expected={(d,v,c) for d in days for v in primary for c in (5,9,15)}|{(d,v,5) for d in days for v in stress}
    assert set(map(tuple,daily[["day","variant","cost_bps"]].to_numpy()))==expected
    assert not daily.duplicated(["day","variant","cost_bps"]).any() and len(daily)==len(expected)==4788
    assert not books.duplicated(["variant","sid"]).any()
    assert not books.duplicated(["variant","day","setup_id","tradingsymbol"]).any()
    assert not books.execution_authority.eq(True).any()
    checked=0
    by_variant={v:f for v,f in books.groupby("variant")}
    for row in daily.itertuples():
        calculated=stats(by_variant[row.variant],[row.day],row.cost_bps)
        for field in ("selected_orders","native_fills","trades","wins","losses","net_pnl_rs","costs_rs","win_rate_pct","profit_factor","average_trade_rs"):
            close(calculated[field],getattr(row,field),f"daily {row.day}/{row.variant}/{row.cost_bps}/{field}")
            checked+=1
    folds=pd.read_csv(result/"models/model_folds.csv")
    pool=pd.read_parquet(result/"model_pool.parquet");pool["day"]=pool.day.astype(str).str[:10]
    predictions=pd.read_parquet(result/"models/predictions.parquet")
    assert not pool.sid.duplicated().any() and not predictions.sid.duplicated().any() and set(pool.sid)==set(predictions.sid)
    ready_days=[]
    for fold in folds.itertuples():
        assert fold.train_sessions==20+5*fold.block_id and fold.test_sessions<=5
        assert fold.train_last_day<fold.test_first_day
        if fold.status=="READY":
            assert pd.Timestamp(fold.max_train_exit)<pd.Timestamp(fold.first_test_signal)
            assert fold.train_filled_labels>=100 and fold.train_positive_labels>=15 and fold.train_negative_labels>=15
            ready_days.extend(d for d in days if fold.test_first_day<=d<=fold.test_last_day)
    ready_days=sorted(ready_days)
    assert len(ready_days)==len(set(ready_days))
    for variant in protocol["models"]:
        assert np.isfinite(predictions.loc[predictions.model_ready,variant]).all()
        assert predictions.loc[~predictions.model_ready,variant].isna().all()
        represented=model_daily.loc[model_daily.variant.eq(variant)&model_daily.cost_bps.eq(5)]
        assert set(represented.loc[represented.model_ready.eq(True),"day"])==set(ready_days)
        fallback=model.loc[model.variant.eq(variant)&~model.day.isin(ready_days)]
        baseline=rules.loc[rules.variant.eq("control")&~rules.day.isin(ready_days)]
        fields=["day","sid","portfolio_executed","portfolio_net_profit_rupees"]
        pd.testing.assert_frame_equal(fallback[fields].sort_values("sid").reset_index(drop=True),baseline[fields].sort_values("sid").reset_index(drop=True),check_dtype=False)
    joined=pool.merge(predictions,on=["sid","day"],validate="one_to_one",suffixes=("","_prediction"))
    labelled=joined.loc[joined.model_ready&joined.filled.eq(True)].copy()
    y=labelled.net_return_pct.gt(0).astype(int)
    diagnostics=pd.read_csv(result/"models/model_metrics.csv")
    for variant in protocol["models"]:
        row=diagnostics.loc[diagnostics.model.eq(variant)&diagnostics.block_id.eq("ALL_READY")].iloc[0]
        for field,value in {"roc_auc":roc_auc_score(y,labelled[variant]),"log_loss":log_loss(y,labelled[variant]),
                            "brier":brier_score_loss(y,labelled[variant]),
                            "constant_log_loss":log_loss(y,labelled.training_base_rate),
                            "constant_brier":brier_score_loss(y,labelled.training_base_rate)}.items():
            close(value,row[field],"model predictive "+variant+"/"+field)
    periods={"all":days,"october":[d for d in days if d.startswith("2026-10")],
             "post_policy_descriptive":[d for d in days if d>"2026-10-27"],"model_evaluation":ready_days,"latest":[days[-1]]}
    periods.update({"month_"+m:[d for d in days if d.startswith(m)] for m in sorted({d[:7] for d in days})})
    summary=pd.read_csv(result/"summary_comparison.csv")
    assert not summary.duplicated(["variant","period","cost_bps"]).any()
    assert set(map(tuple,summary[["variant","period","cost_bps"]].to_numpy()))=={(v,p,c) for v in primary for p in periods for c in (5,9,15)}
    summary_checks=0
    for row in summary.itertuples():
        calculated=stats(by_variant[row.variant],periods[row.period],row.cost_bps)
        for field,value in calculated.items():
            close(value,getattr(row,field),f"summary {row.variant}/{row.period}/{row.cost_bps}/{field}")
            summary_checks+=1
        ref=stats(by_variant["control"],periods[row.period],row.cost_bps)
        close(calculated["net_pnl_rs"]-ref["net_pnl_rs"],row.delta_net_rs,"summary delta")
    wide=pd.read_csv(result/"daywise_comparison.csv").set_index("day")
    assert list(wide.index)==days and not wide.isna().any().any()
    for variant in primary:
        ref=daily.loc[daily.variant.eq(variant)&daily.cost_bps.eq(5)].set_index("day").net_pnl_rs.reindex(days)
        assert np.allclose(wide[variant],ref,atol=1e-6,rtol=0)
        if variant!="control":assert np.allclose(wide[variant+"__delta"],wide[variant]-wide.control,atol=1e-6,rtol=0)
    g7=pd.read_csv(result/"g7_saved_reference.csv").set_index("day")
    assert not g7.index.duplicated().any() and set(g7.index)==set(days)
    assert np.allclose(wide.G7_V4_LOCK_SAVED_REFERENCE,g7.net_pnl_rs.reindex(days),atol=1e-6,rtol=0)
    for row in pd.read_csv(result/"model_vs_same_pool.csv").itertuples():
        arm=stats(by_variant[row.variant],ready_days,row.cost_bps)["net_pnl_rs"]
        ref=stats(by_variant["or_market"],ready_days,row.cost_bps)["net_pnl_rs"]
        close(arm,row.net_pnl_rs,"same-pool model");close(ref,row.reference_net_pnl_rs,"same-pool reference")
        close(arm-ref,row.delta_net_rs,"same-pool delta")
    concentrations=[concentration(books,daily,v) for v in ("or_market_strict","all51_slots")]
    limitations=["This validates internal consistency and reproduction, not future profitability or data truth at the exchange.",
                 "All history through October 27 reused policy-development outcomes; the four later sessions were previously inspected. No untouched strategy holdout exists.",
                 "Opening OI/universe evidence is inherited; October 28 and November 2 use restricted cohorts. Represented sessions are not every exchange date.",
                 "Intrabar order flow, executable spreads, market impact, and timestamp-complete historical news are unobserved. Stop-first OHLC and conservative timing remain assumptions.",
                 "The within-day drawdown is measured at minute closes, not tick extrema; the daily drawdown is a different series.",
                 "Models are trained chronologically, but their base slot policy was selected retrospectively. Predictions are ranking scores, not validated live win probabilities.",
                 "Largest-contribution removal is attribution sensitivity only, not a rerun with changed capital availability.",
                 "Comparing 28 scenarios and parameter neighborhoods on reused history creates selection bias; bootstrap intervals do not remove it."]
    result_json=dict(status="PASS",audited_utc=datetime.now(timezone.utc).isoformat(),audit_script_sha256=sha(Path(__file__)),
                     days=days,primary_variants=primary,total_scenarios=28,rule_order_rows=len(rules),model_order_rows=len(model),
                     combined_selected_order_rows=len(books),executed_rows_across_scenarios=int(books.portfolio_executed.eq(True).sum()),
                     unique_underlying_order_sids=int(books.sid.nunique()),daily_rows=len(daily),daily_numeric_checks=checked,
                     summary_rows=len(summary),summary_numeric_checks=summary_checks,control_parity=fresh_parities,
                     logical_trade_key_duplicates=0,variant_sid_duplicates=0,costs_bps=[5,9,15],
                     frozen_computational_hashes=code_hashes,input_manifest_sha256=config["input_manifest_sha256"],
                     expansion_hashes=expansions,model_ready_days=ready_days,model_folds=len(folds),
                     model_candidates=len(pool),model_ready_filled_labels=len(labelled),concentrations=concentrations,
                     reference_calendars_complete=True,missing_reference_values=0,limitations=limitations,
                     verified_artifact_hashes={str(p.relative_to(result)):sha(p) for p in
                        [result/"config.json",result/"trades.parquet",result/"daily_results.csv",result/"models/trades.parquet",
                         result/"models/daily_results.csv",result/"summary_comparison.csv",result/"daywise_comparison.csv",
                         result/"g7_saved_reference.csv",result/"model_vs_same_pool.csv",result/"models/model_metrics.csv"]})
    lines=["# Independent numerical verification", "", "Status: **PASS**. No strategy, frozen input, replay output, or report was modified by this audit.", "",
           f"Verified {len(days)} represented sessions ({days[0]} through {days[-1]}), 24 primary variants and four fill-stress scenarios.", "",
           f"The combined books contain **{len(books):,} selected-order rows** ({len(rules):,} rule/stress; {len(model):,} models), not unique market trades. There are {int(books.portfolio_executed.eq(True).sum()):,} executed rows across mutually exclusive experiments and {books.sid.nunique():,} distinct underlying candidate IDs. These counts must not be interpreted as one portfolio.", "",
           "Checks completed:", "",
           "- Fresh original-control selection and saved-ledger parity passed on all 63 days.",
           "- All six frozen computational hashes, every raw/minute input hash, partition configuration hash, and all four consumed supplemental-slot hashes match.",
           f"- {len(daily):,} day/variant/cost rows form the exact grid: 24 primary variants at 5/9/15 bps plus four stress variants at 5 bps. {checked:,} daily numeric comparisons passed.",
           "- No duplicate (variant, sid) or (variant, day, setup, stock) order keys; merged ledgers match their day artifacts.",
           f"- Independently recomputed {len(summary):,} summary rows ({summary_checks:,} metric checks), daily comparisons, and all deltas.",
           "- G3, every experiment, and the separate G7 reference share complete 63-day calendars; no missing references were silently filled with zero.",
           f"- Model training precedes evaluation in all {len(folds)} frozen folds; {len(ready_days)} evaluation sessions. Prior-data thresholds, finite ready scores, control fallback, same-pool comparison, and overall predictive diagnostics reconcile.", "",
           "## Apparent improvements and concentration", "",
           "| Variant | Delta at 5 bps | Delta at 9 bps | Delta at 15 bps | October delta | Delta without best day | Delta without best trade contribution |",
           "|---|---:|---:|---:|---:|---:|---:|"]
    for c in concentrations:
        cost=c["cost_delta_rs"]
        lines.append(f"| {c['variant']} | {cost[5]:,.2f} | {cost[9]:,.2f} | {cost[15]:,.2f} | {c['monthly_delta_rs']['2026-10']:,.2f} | {c['without_best_day_delta_rs']:,.2f} | {c['without_best_trade_contribution_delta_rs']:,.2f} |")
    lines.extend(["", "Amounts are rupees relative to control. Strict OR's small aggregate improvement disappears at 15 bps and after removal of its largest positive day/trade contribution. All51 has broader historical gains and survives these sensitivity checks, but deteriorates in October and on November 2. Neither result establishes a production improvement.", "", "## Limits", ""])
    lines.extend("- "+v for v in limitations)
    lines.extend(["", "Exact hashes, counts, per-day parity, month/cost changes, and largest contributions are in [VERIFICATION.json](VERIFICATION.json).", ""])
    dump(result/"VERIFICATION.json",result_json)
    (result/"VERIFICATION.md").write_text("\n".join(lines),encoding="utf-8")
    print(f"AUDIT PASS: {len(days)} days, {len(books)} selected-order rows, {checked} daily checks, {summary_checks} summary checks",flush=True)
    return result_json


if __name__=="__main__":
    parser=argparse.ArgumentParser();parser.add_argument("--results",type=Path,default=PACKAGE/"results")
    audit(parser.parse_args().results)
