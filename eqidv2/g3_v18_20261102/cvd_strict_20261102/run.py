"""Replay six predeclared strict CVD gates with availability-matched controls."""
from __future__ import annotations

import argparse
from datetime import date, datetime
import hashlib
import html
import io
import json
from pathlib import Path
import sys
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

from g3_v18_20261102 import run as common
from g3_v18_20261102.cvd_kite_proxy_20261102 import run as checks
from g3_v18_20261102.cvd_advanced_20261102 import run as advanced
from . import selection

PACKAGE = Path(__file__).resolve().parent
REFERENCE = PACKAGE.parent / "results_20261102_complete"
SOURCE = PACKAGE.parent / "cvd_advanced_20261102/calculated_20261102"
EXPECTED_FEATURE_SHA256 = "5c368ea9a4150a75bf923390e89dff005e68f1748ab8b27af0a47568b21ddae9"
IST = ZoneInfo("Asia/Kolkata")
COSTS = (5, 9, 15)
DECISION_COLUMNS = ("variant","day","setup_id","native_eligible_candidates","eligible_after_gate",
    "original_selected_orders","selected_orders","selected_changed","replacements","deletions","added","removed",
    "original_symbols","selected_symbols","gate_reasons")


def fresh_output(path):
    path = Path(path).resolve()
    if not path.is_relative_to(PACKAGE) or path == PACKAGE:
        raise ValueError("Output must be a NEW child of the strict CVD directory")
    if path.exists():
        raise FileExistsError(f"Refusing to overwrite existing output: {path}")
    return path


def verified_inputs(feature_path, provenance_path):
    if feature_path.resolve() != (SOURCE/"features.parquet").resolve() or provenance_path.resolve() != (SOURCE/"configuration.json").resolve():
        raise ValueError("This frozen test must reuse the verified advanced experiment feature bundle")
    manifest_bytes = (SOURCE/"manifest.json").read_bytes()
    manifest = json.loads(manifest_bytes)
    for name, digest in manifest["files"].items():
        path = (SOURCE/name).resolve()
        if path.parent != SOURCE.resolve() or common.sha(path) != digest:
            raise ValueError("Frozen advanced feature bundle hash mismatch")
    feature_bytes, provenance_bytes = feature_path.read_bytes(), provenance_path.read_bytes()
    if hashlib.sha256(feature_bytes).hexdigest() != EXPECTED_FEATURE_SHA256:
        raise ValueError("Feature input differs from the preregistered source snapshot")
    return feature_bytes, provenance_bytes, manifest_bytes


def verify_audit(original, changed, variant):
    mutable = {"v9_selected", "v9_filter_pass", "v10_g_f_core", "v9_decision", "v9_rank_in_setup_day",
               "strategy_version", "strategy_fingerprint", "signal_id", "source_signal_id", "source_strategy_version",
               "research_variant", "execution_authority", "scope"}
    a,b = (frame.set_index("sid").sort_index() for frame in (original,changed))
    if not a.index.equals(b.index):
        raise ValueError("Strict gates must retain every original candidate in the audit")
    for name in a:
        if name not in mutable:
            try:
                pd.testing.assert_series_equal(a[name],b[name],check_dtype=False,check_names=False,check_exact=True)
            except AssertionError as error:
                raise ValueError(f"Strict gate altered original input: {name}") from error
    permitted,_ = selection.gate(b,variant)
    expected_filter = common.flags(a.v9_filter_pass) & permitted
    if not common.flags(b.v9_filter_pass).equals(expected_filter):
        raise ValueError("Strict gate changed native eligibility beyond its declared feature gate")
    if (common.flags(b.v9_selected) & ~expected_filter).any():
        raise ValueError("Gate-rejected or native-rejected candidate selected")
    for key,group in b.groupby(["day","setup_id"],sort=False):
        setup = selection.native.control._ACTIVE_BY_ID[key[1]]
        core = selection.native.control._CORE_BY_ID[key[1]]
        eligible = group.loc[common.flags(group.v9_filter_pass)]
        rows = {i:row for i,row in eligible.iterrows()}
        ranked = sorted(rows,key=lambda i:selection.native.production._rank_key(rows[i],setup))
        day = date.fromisoformat(str(key[0])[:10])
        core_ids = [i for i in ranked if selection.native.control.passes_selected_filters(rows[i],core,session_date=day)][:core.max_entries]
        expected = core_ids + [i for i in ranked if i not in core_ids][:setup.max_entries-len(core_ids)]
        actual = group.index[common.flags(group.v9_selected)].tolist()
        if set(actual) != set(expected):
            raise ValueError("Native ranking/core/extras quota was not preserved after gating")
    return True


def decision_groups(original,audit,variant):
    old = {key:frame for key,frame in original.groupby(["day","setup_id"],sort=False)}
    rows=[]
    for key,group in audit.groupby(["day","setup_id"],sort=False):
        prior=old[key]
        before=sorted(prior.loc[common.flags(prior.v9_selected),"tradingsymbol"].astype(str))
        after=sorted(group.loc[common.flags(group.v9_selected),"tradingsymbol"].astype(str))
        added,removed=sorted(set(after)-set(before)),sorted(set(before)-set(after))
        reasons=group.strict_gate_reason.value_counts().to_dict() if "strict_gate_reason" in group else {"BASELINE_UNFILTERED":len(group)}
        rows.append(dict(variant=variant,day=str(key[0])[:10],setup_id=key[1],
            native_eligible_candidates=int(common.flags(prior.v9_filter_pass).sum()),
            eligible_after_gate=int(common.flags(group.v9_filter_pass).sum()),
            original_selected_orders=len(before),selected_orders=len(after),selected_changed=bool(added or removed),
            replacements=min(len(added),len(removed)),deletions=max(0,len(removed)-len(added)),
            added=len(added),removed=len(removed),original_symbols=json.dumps(before),selected_symbols=json.dumps(after),
            gate_reasons=json.dumps(reasons,sort_keys=True)))
    return rows


def qualification(overall,matched_groups):
    october=overall.loc[overall.period.eq("october") & overall.cost_bps.eq(5)].set_index("variant")
    if october.empty:
        return []
    rows=[]
    for variant,reference in selection.MATCHED_CONTROLS.items():
        own,ref=october.loc[variant],october.loc[reference]
        changed=matched_groups.loc[matched_groups.variant.eq(variant) & matched_groups.day.str.startswith("2026-10") & matched_groups.selected_changed]
        rules=dict(nonempty=bool(own.trades>0 and ref.trades>0),pnl_noninferior=bool(own.net_pnl_rs>=ref.net_pnl_rs-1e-6),
            drawdown_noninferior=bool(own.daily_close_drawdown_rs<=ref.daily_close_drawdown_rs+1e-6),
            trade_retention=bool(own.trades>=max(20,.50*ref.trades)),actual_selection_changes=bool(len(changed)),
            strict_improvement=bool(own.net_pnl_rs>ref.net_pnl_rs+1e-6 or own.daily_close_drawdown_rs<ref.daily_close_drawdown_rs-1e-6))
        rows.append(dict(variant=variant,reference_variant=reference,**rules,descriptive_screen_pass=all(rules.values()),
            october_changed_groups=len(changed),october_pnl_delta_rs=float(own.net_pnl_rs-ref.net_pnl_rs),
            october_drawdown_delta_rs=float(own.daily_close_drawdown_rs-ref.daily_close_drawdown_rs),
            executed_trade_fraction=float(own.trades/ref.trades) if ref.trades else None))
    return rows


def attribution(reference_book,book,variant,reference,audit):
    if reference_book.empty and book.empty:
        # The frozen older helper returns no columns when both books are empty.
        # Keep this case local so initial RVOL warmup remains a visible zero-day
        # comparison without modifying the previous experiment.
        detail=pd.DataFrame({name:pd.Series(dtype=kind) for kind,names in (
            (object,("variant","day","setup_id","tradingsymbol","signal_ts","effect")),
            ("int64",("sid",)),
            (bool,("baseline_selected","variant_selected","baseline_filled","variant_filled","baseline_executed","variant_executed")),
            (float,("baseline_net_pnl_rs","variant_net_pnl_rs","pnl_delta_rs"))) for name in names})
        proof=dict(variant=variant,attribution_pnl_delta_rs=0.,headline_pnl_delta_rs=0.,reconciled=True)
    else:
        detail,proof=advanced.pnl_attribution(reference_book,book,variant)
    detail["reference_variant"]=reference
    by_sid=audit.set_index("sid")
    detail["gate_reason"]=detail.sid.map(by_sid.strict_gate_reason) if "strict_gate_reason" in by_sid else "BASELINE_UNFILTERED"
    for prefix,source in (("baseline",reference_book),("variant",book)):
        indexed=source.set_index("sid")
        executed=common.flags(indexed.portfolio_executed)
        for field,column in (("gross_pnl_rs","portfolio_gross_profit_rupees"),("cost_rs","portfolio_cost_rupees")):
            values=pd.to_numeric(indexed.get(column,pd.Series(0.,index=indexed.index)),errors="raise").where(executed,0.)
            detail[prefix+"_"+field]=detail.sid.map(values).fillna(0.)
    detail["gross_pnl_delta_rs"]=detail.variant_gross_pnl_rs-detail.baseline_gross_pnl_rs
    detail["cost_delta_rs"]=detail.variant_cost_rs-detail.baseline_cost_rs
    if not np.allclose(detail.gross_pnl_delta_rs-detail.cost_delta_rs,detail.pnl_delta_rs,rtol=0,atol=1e-6):
        raise ValueError("Gross/net/cost attribution does not reconcile")
    return detail,{**proof,"reference_variant":reference,"gross_net_cost_reconciled":True}


def trade_change_analysis(detail,pairs=None):
    rows=[]
    comparisons=pairs if pairs is not None else list(detail.groupby(["variant","reference_variant"],sort=False).groups)
    for variant,reference in comparisons:
        part=detail.loc[detail.variant.eq(variant) & detail.reference_variant.eq(reference)]
        for period,pool in (("all",part),("october",part.loc[part.day.str.startswith("2026-10")]),
                            ("post_slot_selection",part.loc[part.day.gt("2026-10-27")])):
            old=pool.loc[pool.baseline_executed]
            new=pool.loc[pool.variant_executed]
            removed=pool.loc[pool.effect.eq("REMOVED_SELECTION") & pool.baseline_executed]
            added=pool.loc[pool.effect.eq("ADDED_SELECTION")]
            rows.append(dict(variant=variant,reference_variant=reference,period=period,reference_trades=len(old),trades=len(new),
                executed_trade_fraction=len(new)/len(old) if len(old) else None,status="NO_EXECUTED_TRADES" if new.empty else "EXECUTED_TRADES_PRESENT",
                removed_executed_trades=len(removed),removed_winning_trades=int(removed.baseline_net_pnl_rs.gt(0).sum()),
                removed_losing_trades=int(removed.baseline_net_pnl_rs.lt(0).sum()),
                removed_early_warmup_trades=int(removed.gate_reason.eq("BEFORE_0945_OR_INVALID_SIGNAL").sum()),
                removed_unavailable_feature_trades=int(removed.gate_reason.isin(("MISSING_EXACT_FEATURE","CVD_FEATURE_UNAVAILABLE","RVOL_FEATURE_UNAVAILABLE")).sum()),
                removed_gross_pnl_rs=float(removed.baseline_gross_pnl_rs.sum()),removed_net_pnl_rs=float(removed.baseline_net_pnl_rs.sum()),
                added_selected_orders=len(added),added_fills=int(added.variant_filled.sum()),added_executed=int(added.variant_executed.sum()),
                added_winning_trades=int((added.variant_executed & added.variant_net_pnl_rs.gt(0)).sum()),
                added_losing_trades=int((added.variant_executed & added.variant_net_pnl_rs.lt(0)).sum()),
                added_gross_pnl_rs=float(added.variant_gross_pnl_rs.sum()),added_net_pnl_rs=float(added.variant_net_pnl_rs.sum()),
                shared_portfolio_effects=int(pool.effect.eq("SHARED_PORTFOLIO_EFFECT").sum()),pnl_delta_rs=float(pool.pnl_delta_rs.sum())))
    return pd.DataFrame(rows)


def report(output,summary,overall,daily,monthly,groups,analysis,decisions):
    def table(frame):
        return frame.to_html(index=False,border=0,na_rep="Unavailable",float_format=lambda n:f"{n:,.2f}")
    cols=["variant","trades","win_rate_pct","net_pnl_rs","profit_factor","daily_close_drawdown_rs"]
    group_counts=groups.groupby("variant",as_index=False).agg(groups=("setup_id","size"),changed_groups=("selected_changed","sum"),replacements=("replacements","sum"),deletions=("deletions","sum"))
    body=f'''<!doctype html><html lang="en"><meta charset="utf-8"><meta name="viewport" content="width=device-width"><title>G3 strict CVD gate test</title><style>body{{font:16px/1.55 system-ui,sans-serif;max-width:1280px;margin:32px auto;padding:0 20px;color:#183044;background:#f5f8fb}}.notice{{padding:18px;background:#fff1ce;border-left:5px solid #c98c16}}table{{border-collapse:collapse;background:white;font-size:14px;display:block;overflow:auto}}th,td{{padding:9px 12px;border-bottom:1px solid #dce5eb;text-align:right}}th:first-child,td:first-child{{text-align:left}}a{{color:#075cb6}}</style>
<h1>G3 V18: strict estimated CVD gates</h1><p class="notice"><strong>{html.escape(summary['conclusion'])}</strong> This is another exploratory test on previously viewed history. No subset is an independent holdout, and rejecting trades does not itself demonstrate an improvement.</p>
<p>{summary['sessions']} sealed sessions, {summary['first_date']} through {summary['last_date']}. Requested through {summary['requested_through']}; unsealed November2 execution results are excluded. The verified existing one-minute-derived features were reused without a new fetch or recalculation.</p>
<p>These gates reject individual candidates before native ranking, including a pool with only one stock. Native ranking then selects replacements where available. The original filters, raw inputs, core priority, quotas, confirmations, exits and capital model remain.</p>
<p><strong>Aligned:</strong> directional recent15 pressure&gt;0 and raw acceleration&gt;0. <strong>Strict:</strong> directional pressure≥0.10 and raw acceleration≥0.05. <strong>Strict+RVOL:</strong> also requires same-clock RVOL20≥1.20. Missing features fail closed; the earliest signal is09:45. Acceleration is the current15-minute normalized pressure minus the preceding15-minute pressure, with no division by2.</p>
<p>Compare aligned/strict with <strong>cvd_ready_control</strong>; compare strict+RVOL with <strong>rvol_ready_control</strong>. These controls isolate the effect of removing early bars and unavailable20-session RVOL history. The full original G3 control remains visible as the overall benchmark.</p>
<h2>All {summary['sessions']} sessions at5 bps round-trip cost</h2>{table(overall.loc[overall.period.eq('all') & overall.cost_bps.eq(5),cols])}
{common.chart(daily.loc[daily.cost_bps.eq(5)])}<h2>October descriptive screen against matched controls</h2>{table(overall.loc[overall.period.eq('october') & overall.cost_bps.eq(5),cols])}{table(decisions)}
<p>The frozen screen requires no worse October net P&amp;L or daily realized drawdown, at least max(20,50% of matched-control trades), an actual changed selection, and a strict P&amp;L or drawdown improvement. Empty variants cannot qualify. This is a descriptive research screen only.</p>
<h2>Trade retention and removed/added trades</h2>{table(analysis.loc[analysis.period.eq('all')])}
<p>Removed winning and losing trades use after-cost results of the named reference. Early warmup and unavailable-feature removals are separate from threshold effects. Complete gross/net/cost attribution includes shared trades affected by portfolio constraints.</p>
<h2>Replacements versus deletions relative to original G3</h2>{table(group_counts)}
<h2>Monthly results</h2>{table(monthly.loc[monthly.cost_bps.eq(5)])}<h2>PostOctober27: already-viewed descriptive subset</h2>{table(overall.loc[overall.period.eq('post_slot_selection') & overall.cost_bps.eq(5),cols])}
<h2>5/9/15 bps sensitivity</h2>{table(overall.loc[overall.period.eq('all'),['variant','cost_bps','trades','net_pnl_rs','daily_close_drawdown_rs']])}
<ul><li>CVD remains an estimate from one-minute OHLCV close location; it is not observed buyer/seller aggressor volume.</li><li>The historical slot policy used outcomes through October27, and all62 sessions were inspected in earlier experiments. Inherited historical universe/OI and retrospective vendor-data limitations remain.</li><li>October28 is the inherited restricted215-stock cohort. Readiness controls include every requested zero-trade day, including initial RVOL warmup.</li><li>Drawdown is daily realized P&amp;L, not intraday mark-to-market. Costs are total modeled round-trip friction.</li><li>Thresholds and variants were frozen before this replay and were not changed after viewing its outcomes. There is no live trading authority.</li></ul>
<p><a href="overall_comparison.csv">All results</a> · <a href="trade_change_analysis.csv">Trade retention/removals</a> · <a href="pnl_attribution.csv">Complete attribution</a> · <a href="decision_groups.csv">All selection competitions</a> · <a href="matched_decision_groups.csv">Matched-control competitions</a> · <a href="gate_reasons.csv">Gate reasons</a> · <a href="summary.json">Summary</a> · <a href="methodology.json">Frozen methodology</a> · <a href="verification.json">Verification</a></p></html>'''
    (output/"report.html").write_text(body,encoding="utf-8")


def run(args):
    from g3_v18_20261102 import data
    from g3_v18_20261102.cvd_advanced_20261102 import features
    from fno_v13_v10_g_3_daily_replay import simulate
    from research.g3_0920_backtest import allocate
    output=fresh_output(args.output)
    input_bytes,provenance_bytes,bundle_bytes=verified_inputs(args.features,args.feature_provenance)
    reference_summary=json.loads((REFERENCE/"summary.json").read_text(encoding="utf-8"))
    days=sorted(set(data.available_days(args.through)).intersection(reference_summary["days"]))
    if args.days:
        if set(args.days)-set(days):
            raise ValueError("Requested day lacks a sealed source and pinned control")
        days=[day for day in days if day in args.days]
    if not days:
        raise ValueError("No sealed replay sessions")
    enriched=checks.validate_feature_calendar(pd.read_parquet(io.BytesIO(input_bytes)),days)
    reference_path=REFERENCE/"control_trades.csv"
    reference_bytes=reference_path.read_bytes()
    saved=pd.read_csv(io.BytesIO(reference_bytes),low_memory=False)
    methodology_path=PACKAGE/"methodology.json"
    methodology_bytes=methodology_path.read_bytes()
    consumed={str(Path(m.__file__).resolve()):common.sha(m.__file__) for name,m in list(sys.modules.items())
        if name.startswith(("fno_v13","research.","g3_v18_20261102","strategy_slot_selection","g3_one_minute","eqidv2_runtime"))
        and getattr(m,"__file__",None) and Path(m.__file__).suffix==".py"}
    consumed[str(Path(__file__).resolve())]=common.sha(__file__)
    config={**selection.configuration(),"requested_through":args.through,"planned_calendar":days,"costs_bps":list(COSTS),
        "input_path":str(args.features.resolve()),"input_sha256":hashlib.sha256(input_bytes).hexdigest(),
        "input_bundle_manifest_sha256":hashlib.sha256(bundle_bytes).hexdigest(),"input_bundle_verified":True,
        "reference_control_sha256":hashlib.sha256(reference_bytes).hexdigest(),"source_code_sha256":consumed,
        "methodology_sha256":hashlib.sha256(methodology_bytes).hexdigest(),"methodology":json.loads(methodology_bytes),
        "feature_provenance_sha256":hashlib.sha256(provenance_bytes).hexdigest(),"scope":"BACKTEST_ONLY"}
    common.guard_writes(PACKAGE)
    output.mkdir(parents=True)
    common.dump(output/"config.json",config)
    common.dump(output/"status.json",dict(state="RUNNING",performance_complete=False))
    for name,content in (("input_features.parquet",input_bytes),("feature_provenance.json",provenance_bytes),
                         ("input_bundle_manifest.json",bundle_bytes),("methodology.json",methodology_bytes)):
        (output/name).write_bytes(content)
    fingerprint=common.sha(output/"config.json")
    ledgers,audits=({v:[] for v in selection.VARIANTS} for _ in range(2))
    proofs,parity_daily,groups,matched_groups,coverage,reasons=[],[],[],[],[],[]
    by_day={str(day):frame for day,frame in enriched.groupby(enriched.bar_end.dt.strftime("%Y-%m-%d"),sort=False)}
    for number,day in enumerate(days):
        raw,minutes,proof=data.load_day(day)
        raw=raw.reset_index(drop=True)
        raw["sid"]=number*1_000_000+np.arange(len(raw),dtype=np.int64)
        raw=features.attach_features(raw,by_day[day])
        results=selection.select_all(raw)
        original=results["base_control"][1]
        proofs.append({**proof,"day":day})
        coverage.append(dict(day=day,source_rows=len(raw),source_symbols=raw.tradingsymbol.nunique(),
            exact_features=int(raw.adv_feature_ts.notna().sum()),cvd_ready=int(common.flags(raw.adv_ready).sum()),
            rvol_ready=int(common.flags(raw.adv_rvol_ready).sum()),restricted_oct28=day=="2026-10-28"))
        counts=[]
        for variant,(orders,audit) in results.items():
            if variant!="base_control":
                verify_audit(original,audit,variant)
                for reason,part in audit.groupby("strict_gate_reason",sort=False):
                    reasons.append(dict(variant=variant,day=day,reason=reason,candidates=len(part),
                        native_eligible=int(common.flags(part.strict_native_filter_pass).sum()),
                        originally_selected=int(common.flags(part.strict_original_selected).sum())))
            groups.extend(decision_groups(original,audit,variant))
            if variant in selection.MATCHED_CONTROLS:
                reference=selection.MATCHED_CONTROLS[variant]
                matched_groups.extend({**record,"reference_variant":reference} for record in decision_groups(results[reference][1],audit,variant))
            ledger,_=simulate(orders,data.execution_paths(orders,minutes),[date.fromisoformat(day)])
            ledger["variant"],ledger["strategy_version"],ledger["strategy_fingerprint"],ledger["execution_authority"]=variant,selection.VERSION,fingerprint,False
            ledgers[variant].append(ledger)
            audits[variant].append(audit)
            counts.append(f"{variant}={len(ledger)}")
        parity_daily.append({"day":day,**checks.baseline_parity(ledgers["base_control"][-1],saved,[day])})
        print(f"{day} [{number+1}/{len(days)}] "+" ".join(counts),flush=True)
    books={v:pd.concat(parts,ignore_index=True,sort=False) for v,parts in ledgers.items()}
    all_audits={v:pd.concat(parts,ignore_index=True,sort=False) for v,parts in audits.items()}
    parity=checks.baseline_parity(books["base_control"],saved,days)
    periods={"all":days,"before_october":[d for d in days if d<"2026-10-01"],"october":[d for d in days if d.startswith("2026-10")],
        "october_without_restricted":[d for d in days if d.startswith("2026-10") and d!="2026-10-28"],
        "post_slot_selection":[d for d in days if d>"2026-10-27"]}
    overall_rows,daily_rows,monthly_rows=[],[],[]
    for variant,book in books.items():
        book.to_csv(output/f"{variant}_trades.csv",index=False)
        all_audits[variant].to_csv(output/f"{variant}_candidate_audit.csv",index=False)
        for cost in COSTS:
            stressed=book if cost==5 else checks.stressed_book(book,cost,allocate)
            for period,cohort in periods.items():
                if cohort:
                    overall_rows.append(dict(variant=variant,cost_bps=cost,period=period,**common.metrics(stressed,cohort)))
            daily_rows.extend(dict(variant=variant,cost_bps=cost,day=day,**common.metrics(stressed,[day])) for day in days)
            monthly_rows.extend(dict(variant=variant,cost_bps=cost,month=month,**common.metrics(stressed,[d for d in days if d.startswith(month)])) for month in sorted({d[:7] for d in days}))
    overall,daily,monthly=pd.DataFrame(overall_rows),pd.DataFrame(daily_rows),pd.DataFrame(monthly_rows)
    accounting=checks.verify_accounting(books,overall,daily,monthly,days)
    group_frame=pd.DataFrame(groups,columns=DECISION_COLUMNS)
    matched_frame=pd.DataFrame(matched_groups,columns=(*DECISION_COLUMNS,"reference_variant"))
    decisions=pd.DataFrame(qualification(overall,matched_frame))
    attribution_parts,attribution_checks=[],[]
    pairs=[(v,"base_control") for v in selection.VARIANTS[1:]]+list(selection.MATCHED_CONTROLS.items())
    for variant,reference in pairs:
        detail,proof=attribution(books[reference],books[variant],variant,reference,all_audits[variant])
        attribution_parts.append(detail)
        attribution_checks.append(proof)
    attributed=pd.concat(attribution_parts,ignore_index=True)
    analysis=trade_change_analysis(attributed,pairs=pairs)
    for name,frame in (("overall_comparison",overall),("daily_results",daily),("monthly_results",monthly),
        ("coverage",pd.DataFrame(coverage)),("gate_reasons",pd.DataFrame(reasons,columns=["variant","day","reason","candidates","native_eligible","originally_selected"])),("comparison_decisions",decisions),
        ("decision_groups",group_frame),("matched_decision_groups",matched_frame),("pnl_attribution",attributed),("trade_change_analysis",analysis)):
        frame.to_csv(output/f"{name}.csv",index=False)
    changes=[name for name,digest in consumed.items() if common.sha(name)!=digest]
    if changes or common.sha(reference_path)!=config["reference_control_sha256"] or common.sha(methodology_path)!=config["methodology_sha256"] or common.sha(args.features)!=EXPECTED_FEATURE_SHA256:
        raise ValueError(f"Code, methodology or immutable source changed during replay: {changes}")
    passing=decisions.loc[decisions.descriptive_screen_pass,"variant"].tolist() if len(decisions) else []
    summary=dict(version=selection.VERSION,execution_authority=False,scope="BACKTEST_ONLY",created_at_ist=datetime.now(IST).isoformat(),
        requested_through=args.through,days=days,sessions=len(days),first_date=days[0],last_date=days[-1],requested_day_included=args.through in days,
        variants=list(selection.VARIANTS),source_type=config["source_type"],matched_controls=selection.MATCHED_CONTROLS,
        independent_holdout=False,all_sessions_previously_viewed=True,baseline_parity=parity,config_sha256=fingerprint,
        original_source_metrics_unchanged=True,native_ranking_and_quotas_preserved=True,existing_consumed_files_unchanged=True,
        descriptive_screen_passing_variants=passing,
        conclusion=("Passed the frozen matched-control descriptive screen: "+", ".join(passing)+". Prospective evidence is still required."
                    if passing else "No strict gate passed every frozen October comparison against its matched control."),
        overall_metrics=overall_rows,comparisons=decisions.to_dict("records"))
    common.dump(output/"source_provenance.json",dict(sessions=proofs,consumed_code=consumed,feature_sha256=EXPECTED_FEATURE_SHA256,
        input_bundle_manifest_sha256=config["input_bundle_manifest_sha256"],methodology_sha256=config["methodology_sha256"],reference_control_sha256=config["reference_control_sha256"]))
    common.dump(output/"baseline_parity.json",dict(overall=parity,daily=parity_daily))
    common.dump(output/"verification.json",dict(passed=True,checks=accounting,original_control_parity=parity,
        original_source_fields_unchanged=True,gate_before_native_ranking_verified=True,native_core_extras_quotas_preserved=True,
        exact_feature_source_bundle_verified=True,existing_consumed_files_unchanged=True,complete_pnl_attribution=attribution_checks))
    common.dump(output/"summary.json",summary)
    report(output,summary,overall,daily,monthly,group_frame,analysis,decisions)
    common.dump(output/"status.json",dict(state="COMPLETE",performance_complete=True))
    common.dump(output/"manifest.json",dict(files={p.name:common.sha(p) for p in output.iterdir() if p.is_file()},execution_authority=False))
    print(json.dumps(dict(output=str(output),sessions=len(days),through=days[-1],baseline_parity=True,conclusion=summary["conclusion"]),indent=2),flush=True)
    return summary


def parser():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument("--features",type=Path,default=SOURCE/"features.parquet")
    p.add_argument("--feature-provenance",type=Path,default=SOURCE/"configuration.json")
    p.add_argument("--through",default="2026-11-02",type=lambda s:date.fromisoformat(s).isoformat())
    p.add_argument("--days",nargs="+")
    p.add_argument("--output",type=Path,default=PACKAGE/("results_"+datetime.now(IST).strftime("%Y%m%d_%H%M%S")))
    return p


if __name__=="__main__":
    args=parser().parse_args()
    existed_before=args.output.exists()
    try:
        run(args)
    except Exception as error:
        if not existed_before and args.output.resolve().is_relative_to(PACKAGE) and (args.output/"status.json").is_file():
            common.dump(args.output/"status.json",dict(state="FAILED",performance_complete=False,error=str(error)))
        raise
