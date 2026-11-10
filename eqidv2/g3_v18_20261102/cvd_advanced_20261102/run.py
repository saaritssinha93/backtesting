"""Isolated G3 replay of a frozen, bounded acceleration ranking adjustment."""
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
from . import selection

PACKAGE = Path(__file__).resolve().parent
REFERENCE = PACKAGE.parent / "results_20261102_complete"
IST = ZoneInfo("Asia/Kolkata")
COSTS = (5, 9, 15)


def fresh_output(path):
    path = Path(path).resolve()
    if not path.is_relative_to(PACKAGE) or path == PACKAGE:
        raise ValueError("Output must be a NEW child of the advanced CVD directory")
    if path.exists():
        raise FileExistsError(f"Refusing to overwrite existing output: {path}")
    return path


def verify_audit(original, changed):
    """Prove ranking alone changed: every other native field is identical."""
    mutable = {"v9_selected", "v10_g_f_core", "v9_decision", "v9_rank_in_setup_day", "strategy_version",
               "strategy_fingerprint", "signal_id", "source_signal_id", "source_strategy_version",
               "research_variant", "execution_authority", "scope"}
    a, b = (frame.set_index("sid").sort_index() for frame in (original, changed))
    if not a.index.equals(b.index):
        raise ValueError("Ranking must preserve the entire native audit pool")
    for name in a.columns:
        if name not in mutable and not a[name].equals(b[name]):
            raise ValueError(f"Ranking altered original source/filter field: {name}")
    if not changed.adv_rank_adjustment.abs().le(selection.WEIGHT + 1e-12).all():
        raise ValueError("Ranking adjustment exceeded frozen weight")
    for key, group in b.groupby(["day", "setup_id"], sort=False):
        old = a.loc[(a.day == key[0]) & (a.setup_id == key[1])]
        chosen, prior = common.flags(group.v9_selected), common.flags(old.v9_selected)
        if chosen.sum() != prior.sum():
            raise ValueError("Ranking changed the native selection quota")
        if not group.adv_rank_group_applied.any() and not chosen.equals(prior):
            raise ValueError("Neutral group changed its original selection")
    return True


def decision_groups(original, audit, variant):
    original_by_group = {key: frame for key, frame in original.groupby(["day", "setup_id"], sort=False)}
    rows = []
    for key, group in audit.groupby(["day", "setup_id"], sort=False):
        prior = original_by_group[key]
        before = sorted(prior.loc[common.flags(prior.v9_selected), "tradingsymbol"].astype(str))
        after = sorted(group.loc[common.flags(group.v9_selected), "tradingsymbol"].astype(str))
        added, removed = sorted(set(after) - set(before)), sorted(set(before) - set(after))
        rows.append(dict(variant=variant, day=str(key[0])[:10], setup_id=key[1],
            eligible_candidates=int(common.flags(group.v9_filter_pass).sum()), selected_orders=len(after),
            ranking_applied=bool(group.adv_rank_group_applied.any()) if "adv_rank_group_applied" in group else False,
            reason=str(group.adv_rank_reason.iloc[0]) if "adv_rank_reason" in group else "NATIVE_CONTROL",
            selected_changed=bool(added or removed), substitutions=len(added),
            original_symbols=json.dumps(before), selected_symbols=json.dumps(after),
            added_symbols=json.dumps(added), removed_symbols=json.dumps(removed)))
    return rows


def qualification(overall, groups):
    october = overall.loc[overall.period.eq("october") & overall.cost_bps.eq(5)].set_index("variant")
    if october.empty:
        return []
    ref = october.loc["base_control"]
    rows = []
    for variant in selection.VARIANTS[1:]:
        row = october.loc[variant]
        changed = groups.loc[groups.variant.eq(variant) & groups.day.str.startswith("2026-10") & groups.selected_changed]
        rules = dict(pnl_noninferior=bool(row.net_pnl_rs >= ref.net_pnl_rs - 1e-6),
            drawdown_noninferior=bool(row.daily_close_drawdown_rs <= ref.daily_close_drawdown_rs + 1e-6),
            trade_retention=bool(row.trades >= .90 * ref.trades),
            actual_selection_changes=bool(len(changed)),
            strict_improvement=bool(row.net_pnl_rs > ref.net_pnl_rs + 1e-6 or
                                    row.daily_close_drawdown_rs < ref.daily_close_drawdown_rs - 1e-6))
        rows.append(dict(variant=variant, **rules, descriptive_screen_pass=all(rules.values()),
            october_changed_groups=len(changed), october_substitutions=int(changed.substitutions.sum()),
            october_pnl_delta_rs=float(row.net_pnl_rs-ref.net_pnl_rs),
            october_drawdown_delta_rs=float(row.daily_close_drawdown_rs-ref.daily_close_drawdown_rs)))
    return rows


def pnl_attribution(baseline, variant_book, variant):
    """Account for all net differences, including shared portfolio acceptance."""
    a, b = (frame.set_index("sid", drop=False) for frame in (baseline, variant_book))
    rows = []
    for sid in sorted(set(a.index) | set(b.index)):
        before = a.loc[sid] if sid in a.index else None
        after = b.loc[sid] if sid in b.index else None
        identity = after if after is not None else before
        def value(row, name, default=None):
            return row[name] if row is not None and name in row else default
        def flag(row, name):
            return str(value(row, name, False)).lower() in ("true", "1", "1.0")
        before_executed, after_executed = flag(before, "portfolio_executed"), flag(after, "portfolio_executed")
        old_pnl = float(value(before, "portfolio_net_profit_rupees", 0.)) if before_executed else 0.
        new_pnl = float(value(after, "portfolio_net_profit_rupees", 0.)) if after_executed else 0.
        effect = ("ADDED_SELECTION" if before is None else "REMOVED_SELECTION" if after is None else
                  "SHARED_PORTFOLIO_EFFECT" if before_executed != after_executed or abs(new_pnl-old_pnl) > 1e-6 else "SHARED_UNCHANGED")
        rows.append(dict(variant=variant, sid=sid, day=str(identity.day)[:10], setup_id=identity.setup_id,
            tradingsymbol=identity.tradingsymbol, signal_ts=identity.signal_ts, effect=effect,
            baseline_selected=before is not None, variant_selected=after is not None,
            baseline_filled=flag(before, "filled"), variant_filled=flag(after, "filled"),
            baseline_executed=before_executed, variant_executed=after_executed,
            baseline_net_pnl_rs=old_pnl, variant_net_pnl_rs=new_pnl, pnl_delta_rs=new_pnl-old_pnl))
    out = pd.DataFrame(rows)
    old_total = float(baseline.loc[common.flags(baseline.portfolio_executed), "portfolio_net_profit_rupees"].sum())
    new_total = float(variant_book.loc[common.flags(variant_book.portfolio_executed), "portfolio_net_profit_rupees"].sum())
    actual = float(out.pnl_delta_rs.sum()) if len(out) else 0.
    if not np.isclose(actual, new_total-old_total, rtol=0, atol=1e-6):
        raise ValueError("Per-trade attribution does not reconcile to headline P&L difference")
    return out, dict(variant=variant, attribution_pnl_delta_rs=actual, headline_pnl_delta_rs=new_total-old_total,
                     reconciled=True)


def report(output, summary, overall, daily, monthly, groups, differences, decisions, attribution):
    def table(frame):
        return frame.to_html(index=False, border=0, na_rep="Unavailable", float_format=lambda n: f"{n:,.2f}")
    cols = ["variant", "trades", "win_rate_pct", "net_pnl_rs", "profit_factor", "daily_close_drawdown_rs"]
    coverage = groups.groupby(["variant", "reason"], as_index=False).agg(
        groups=("setup_id", "size"), changed_groups=("selected_changed", "sum"), substitutions=("substitutions", "sum"))
    body = f'''<!doctype html><html lang="en"><meta charset="utf-8"><meta name="viewport" content="width=device-width">
<title>G3 advanced CVD ranking test</title><style>body{{font:16px/1.55 system-ui,sans-serif;max-width:1200px;margin:32px auto;padding:0 20px;color:#183044;background:#f5f8fb}}h1{{font-size:30px}}.notice{{padding:18px;background:#fff1ce;border-left:5px solid #c98c16}}table{{border-collapse:collapse;background:white;font-size:14px;display:block;overflow:auto}}th,td{{padding:9px 12px;border-bottom:1px solid #dce5eb;text-align:right}}th:first-child,td:first-child{{text-align:left}}a{{color:#075cb6}}</style>
<h1>G3 V18: advanced estimated CVD ranking</h1>
<p class="notice"><strong>{html.escape(summary['conclusion'])}</strong> This is a retrospective research test. All 62 sessions and the final three sessions were previously viewed; there is no unseen holdout.</p>
<p>{summary['sessions']} sealed sessions, {summary['first_date']} through {summary['last_date']}. Requested through {summary['requested_through']}. Unsealed sessions have no execution P&amp;L.</p>
<p>The estimate weights each completed one-minute volume by its candle close location: volume × (2 × close − high − low)/(high − low), neutral for zero range. Acceleration compares volume-normalized pressure in the last 15 minutes with the preceding 15 minutes. It is an OHLCV estimate, not buyer/seller aggressor volume.</p>
<p>The unchanged G3 picker is divided by the maximum among eligible competitors. A fixed 0.10 × directional acceleration/2 adjustment changes ranking only. The RVOL variant scales that adjustment by clip(RVOL20−1,0,1). Original raw metrics, eligibility, core priority, quotas and execution rules are preserved.</p>
<p>Before 09:45, while any competitor lacks a required exact-clock feature, during RVOL history warmup, and without selection competition, the original selection remains. Missing features never remove an otherwise valid G3 candidate.</p>
<h2>Results at 5 bps round-trip cost</h2>{table(overall.loc[overall.period.eq('all') & overall.cost_bps.eq(5), cols])}
{common.chart(daily.loc[daily.cost_bps.eq(5)])}
<h2>October: descriptive comparison</h2>{table(overall.loc[overall.period.eq('october') & overall.cost_bps.eq(5), cols])}
{table(decisions)}
<p>The frozen descriptive screen requires no worse October net P&amp;L or daily realized drawdown, at least 90% of baseline trades, at least one changed October selection, and a strict improvement in P&amp;L or drawdown. Tiny effects and few substitutions do not establish repeatability.</p>
<h2>After October 27: previously viewed, descriptive only</h2>{table(overall.loc[overall.period.eq('post_slot_selection') & overall.cost_bps.eq(5), cols])}
<h2>Monthly results</h2>{table(monthly.loc[monthly.cost_bps.eq(5)])}
<h2>5/9/15 bps cost sensitivity</h2>{table(overall.loc[overall.period.eq('all'), ['variant','cost_bps','trades','net_pnl_rs','daily_close_drawdown_rs']])}
<h2>Ranking coverage and actual substitutions</h2>{table(coverage)}
<h2>Where the P&amp;L difference came from</h2>{table(attribution.groupby(['variant','effect'], as_index=False).agg(trades=('sid','size'), pnl_delta_rs=('pnl_delta_rs','sum')))}
<p>Added and removed selections plus any changed portfolio acceptance on shared selections reconcile to the full reported difference. Every shared unchanged trade remains in the attribution download.</p>
<h2>One-minute-derived OHLCV versus original G3 candles</h2>{table(differences)}
<p>Source comparison uses ₹0.005 absolute OHLC and 0.5-share volume tolerance plus relative tolerance 1e-8. Original G3 inputs remain untouched; new minute-derived features are attached by exact completed signal timestamp.</p>
<ul><li>The existing slot policy used outcomes through October 27; historical universe/OI availability limitations remain. October 28 is the inherited restricted 215-stock cohort.</li><li>RVOL uses 20 strictly prior same-clock session windows. Initial or incomplete history is neutral; no future observations or alternate-clock fills are used.</li><li>Drawdown is daily realized P&amp;L, not intraday mark-to-market. Costs are modeled total round-trip friction, not an itemized broker tax model.</li><li>No thresholds, weights or variants were changed after viewing these results. No live trading authority is granted.</li></ul>
<p><a href="overall_comparison.csv">All results</a> · <a href="decision_groups.csv">Every selection competition</a> · <a href="selection_substitutions.csv">Trade substitutions</a> · <a href="pnl_attribution.csv">Complete P&amp;L attribution</a> · <a href="daily_results.csv">Daily results</a> · <a href="summary.json">Summary</a> · <a href="config.json">Frozen configuration</a> · <a href="methodology.json">Methodology</a> · <a href="verification.json">Verification</a></p></html>'''
    (output / "report.html").write_text(body, encoding="utf-8")


def run(args):
    from g3_v18_20261102 import data
    from . import features
    from fno_v13_v10_g_3_daily_replay import simulate
    from research.g3_0920_backtest import allocate
    output = fresh_output(args.output)
    input_bytes = args.features.read_bytes()
    enriched = pd.read_parquet(io.BytesIO(input_bytes))
    reference_summary = json.loads((REFERENCE / "summary.json").read_text(encoding="utf-8"))
    days = sorted(set(data.available_days(args.through)).intersection(reference_summary["days"]))
    if args.days:
        if set(args.days) - set(days):
            raise ValueError("Requested day lacks a sealed G3 source and pinned baseline")
        days = [day for day in days if day in args.days]
    if not days:
        raise ValueError("No sealed replay sessions")
    enriched = checks.validate_feature_calendar(enriched, days)
    reference_path = REFERENCE / "control_trades.csv"
    reference_bytes = reference_path.read_bytes()
    saved = pd.read_csv(io.BytesIO(reference_bytes), low_memory=False)
    methodology_path = PACKAGE / "methodology.json"
    methodology_bytes = methodology_path.read_bytes()
    provenance_bytes = args.feature_provenance.read_bytes() if args.feature_provenance else None
    consumed = {str(Path(module.__file__).resolve()): common.sha(module.__file__)
        for name, module in list(sys.modules.items())
        if name.startswith(("fno_v13", "research.", "g3_v18_20261102", "strategy_slot_selection", "g3_one_minute", "eqidv2_runtime"))
        and getattr(module, "__file__", None) and Path(module.__file__).suffix == ".py"}
    consumed[str(Path(__file__).resolve())] = common.sha(__file__)
    config = {**selection.configuration(), "requested_through": args.through, "planned_calendar": days,
        "input_path": str(args.features.resolve()), "input_sha256": hashlib.sha256(input_bytes).hexdigest(),
        "reference_control_sha256": hashlib.sha256(reference_bytes).hexdigest(), "source_code_sha256": consumed,
        "methodology_sha256": hashlib.sha256(methodology_bytes).hexdigest(), "costs_bps": list(COSTS),
        "feature_provenance_sha256": hashlib.sha256(provenance_bytes).hexdigest() if provenance_bytes else None,
        "scope": "BACKTEST_ONLY", "qualification": json.loads(methodology_bytes)}
    common.guard_writes(PACKAGE)
    output.mkdir(parents=True)
    common.dump(output / "config.json", config)
    common.dump(output / "status.json", dict(state="RUNNING", performance_complete=False))
    (output / "input_features.parquet").write_bytes(input_bytes)
    (output / "methodology.json").write_bytes(methodology_bytes)
    if provenance_bytes:
        (output / "feature_provenance.json").write_bytes(provenance_bytes)
    fingerprint = common.sha(output / "config.json")
    ledgers, audits = ({v: [] for v in selection.VARIANTS} for _ in range(2))
    differences, proofs, parity_daily, groups, coverage = [], [], [], [], []
    by_day = {str(day): frame for day, frame in enriched.groupby(enriched.bar_end.dt.strftime("%Y-%m-%d"), sort=False)}
    for number, day in enumerate(days):
        raw, minutes, proof = data.load_day(day)
        raw = raw.reset_index(drop=True)
        raw["sid"] = number * 1_000_000 + np.arange(len(raw), dtype=np.int64)
        differences.append(checks.source_comparison(raw, by_day[day], day))
        raw = features.attach_features(raw, by_day[day])
        results = selection.select_all(raw)
        original = results["base_control"][1]
        proofs.append({**proof, "day": day})
        coverage.append(dict(day=day, source_rows=len(raw), source_symbols=raw.tradingsymbol.nunique(),
            exact_minute_feature_rows=int(raw.adv_feature_ts.notna().sum()),
            acceleration_ready_rows=int(common.flags(raw.adv_ready).sum()),
            rvol_ready_rows=int(common.flags(raw.adv_rvol_ready).sum()), restricted_oct28=day == "2026-10-28"))
        counts = []
        for variant, (orders, audit) in results.items():
            if variant != "base_control":
                verify_audit(original, audit)
            groups.extend(decision_groups(original, audit, variant))
            ledger, _ = simulate(orders, data.execution_paths(orders, minutes), [date.fromisoformat(day)])
            ledger["variant"] = variant
            ledger["strategy_version"] = selection.VERSION
            ledger["strategy_fingerprint"] = fingerprint
            ledger["execution_authority"] = False
            ledgers[variant].append(ledger)
            audits[variant].append(audit)
            counts.append(f"{variant}={len(ledger)}")
        parity_daily.append({"day": day, **checks.baseline_parity(ledgers["base_control"][-1], saved, [day])})
        print(f"{day} [{number+1}/{len(days)}] " + " ".join(counts), flush=True)
    books = {variant: pd.concat(parts, ignore_index=True, sort=False) for variant, parts in ledgers.items()}
    parity = checks.baseline_parity(books["base_control"], saved, days)
    group_frame = pd.DataFrame(groups)
    periods = {"all": days, "before_october": [d for d in days if d < "2026-10-01"],
        "october": [d for d in days if d.startswith("2026-10")],
        "october_without_restricted": [d for d in days if d.startswith("2026-10") and d != "2026-10-28"],
        "post_slot_selection": [d for d in days if d > "2026-10-27"]}
    overall_rows, daily_rows, monthly_rows, substitutions = [], [], [], []
    for variant, book in books.items():
        book.to_csv(output / f"{variant}_trades.csv", index=False)
        pd.concat(audits[variant], ignore_index=True, sort=False).to_csv(output / f"{variant}_candidate_scores.csv", index=False)
        if variant != "base_control":
            prior = books["base_control"]
            for change, source, other in (("REMOVED", prior, book), ("ADDED", book, prior)):
                for record in source.loc[~source.sid.isin(other.sid)].to_dict("records"):
                    substitutions.append({**record, "comparison_variant": variant, "selection_change": change})
        for cost in COSTS:
            stressed = book if cost == 5 else checks.stressed_book(book, cost, allocate)
            for period, cohort in periods.items():
                if cohort:
                    overall_rows.append(dict(variant=variant, cost_bps=cost, period=period, **common.metrics(stressed, cohort)))
            daily_rows.extend(dict(variant=variant, cost_bps=cost, day=day, **common.metrics(stressed, [day])) for day in days)
            monthly_rows.extend(dict(variant=variant, cost_bps=cost, month=month, **common.metrics(stressed, [d for d in days if d.startswith(month)]))
                                for month in sorted({d[:7] for d in days}))
    overall, daily, monthly = pd.DataFrame(overall_rows), pd.DataFrame(daily_rows), pd.DataFrame(monthly_rows)
    accounting = checks.verify_accounting(books, overall, daily, monthly, days)
    decisions = pd.DataFrame(qualification(overall, group_frame))
    attribution_parts, attribution_checks = [], []
    for variant in selection.VARIANTS[1:]:
        detail, proof = pnl_attribution(books["base_control"], books[variant], variant)
        attribution_parts.append(detail)
        attribution_checks.append(proof)
    attribution = pd.concat(attribution_parts, ignore_index=True)
    for name, frame in (("overall_comparison", overall), ("daily_results", daily), ("monthly_results", monthly),
        ("coverage", pd.DataFrame(coverage)), ("candle_source_comparison", pd.DataFrame(differences)),
        ("comparison_decisions", decisions), ("decision_groups", group_frame),
        ("pnl_attribution", attribution),
        ("selection_substitutions", pd.DataFrame(substitutions, columns=list(substitutions[0]) if substitutions else ["comparison_variant", "selection_change", "day", "setup_id", "tradingsymbol"]))):
        frame.to_csv(output / f"{name}.csv", index=False)
    changed_files = [name for name, digest in consumed.items() if common.sha(name) != digest]
    if changed_files or common.sha(reference_path) != config["reference_control_sha256"] or common.sha(methodology_path) != config["methodology_sha256"]:
        raise ValueError(f"Consumed code, baseline or methodology changed during replay: {changed_files}")
    passing = decisions.loc[decisions.descriptive_screen_pass, "variant"].tolist() if len(decisions) else []
    summary = dict(version=selection.VERSION, execution_authority=False, scope="BACKTEST_ONLY",
        created_at_ist=datetime.now(IST).isoformat(), requested_through=args.through,
        days=days, sessions=len(days), first_date=days[0], last_date=days[-1], requested_day_included=args.through in days,
        variants=list(selection.VARIANTS), source_type=config["source_type"], independent_holdout=False,
        last_three_sessions_previously_viewed=True, baseline_parity=parity, config_sha256=fingerprint,
        original_inputs_and_eligibility_unchanged=True, existing_consumed_files_unchanged=True,
        descriptive_screen_passing_variants=passing,
        conclusion=("Passed the frozen descriptive screen: " + ", ".join(passing) + ". This merits prospective evaluation only."
                    if passing else "No ranking variant passed every frozen October descriptive comparison."),
        overall_metrics=overall_rows, comparisons=decisions.to_dict("records"),
        changed_groups_by_variant=group_frame.groupby("variant").selected_changed.sum().to_dict())
    common.dump(output / "source_provenance.json", dict(sessions=proofs, consumed_code=consumed,
        feature_sha256=config["input_sha256"], methodology_sha256=config["methodology_sha256"],
        reference_control_sha256=config["reference_control_sha256"]))
    common.dump(output / "baseline_parity.json", dict(overall=parity, daily=parity_daily))
    common.dump(output / "verification.json", dict(passed=True, checks=accounting, original_control_parity=parity,
        original_source_metrics_and_filter_flags_unchanged=True, native_quotas_preserved=True,
        neutral_groups_identical=True, bounded_weight=selection.WEIGHT, existing_consumed_files_unchanged=True,
        complete_pnl_attribution=attribution_checks))
    common.dump(output / "summary.json", summary)
    report(output, summary, overall, daily, monthly, group_frame, pd.DataFrame(differences), decisions, attribution)
    common.dump(output / "status.json", dict(state="COMPLETE", performance_complete=True))
    common.dump(output / "manifest.json", dict(files={p.name: common.sha(p) for p in output.iterdir() if p.is_file()}, execution_authority=False))
    print(json.dumps(dict(output=str(output), sessions=len(days), through=days[-1], baseline_parity=True,
                          conclusion=summary["conclusion"]), indent=2), flush=True)
    return summary


def parser():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--features", type=Path, required=True)
    p.add_argument("--feature-provenance", type=Path)
    p.add_argument("--through", default="2026-11-02", type=lambda s: date.fromisoformat(s).isoformat())
    p.add_argument("--days", nargs="+")
    p.add_argument("--output", type=Path, default=PACKAGE / ("results_" + datetime.now(IST).strftime("%Y%m%d_%H%M%S")))
    return p


if __name__ == "__main__":
    args = parser().parse_args()
    existed_before = args.output.exists()
    try:
        run(args)
    except Exception as error:
        if not existed_before and args.output.resolve().is_relative_to(PACKAGE) and (args.output / "status.json").is_file():
            common.dump(args.output / "status.json", dict(state="FAILED", performance_complete=False, error=str(error)))
        raise
