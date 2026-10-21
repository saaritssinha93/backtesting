"""Predeclared G4 ATR trailing grid on the sealed 51-setup research cohort.

This is an offline experiment. It fixes native entries, targets, exposure,
portfolio rules and five-bps costs. Policy selection reads only the first 35
sessions; the later 17 are reported as chronological diagnostics on previously
used G4 history. Nothing updates an active strategy or result pointer.
"""
from __future__ import annotations

import argparse
from datetime import datetime
import html
import itertools
import json
from pathlib import Path
import secrets
import shutil
import sys

import numpy as np
import pandas as pd

from research import g4_iterative_exit_backtest as core
from research import g4_separate_sl_variants as prior
from research import g4_trade_monitor_backtest as allocator
from research import g4_g3_mirror_backtest as base


VERSION = "G4-ATR-TRAIL-GRID-V1"
RESEARCH_ROOT = (base.G4_ROOT / "research").resolve()
GRID = {
    "activation_target_fraction": (.5, .7, .9),
    "atr_multiple": (1., 1.5, 2., 3., 4., 6.),
    "protected_profit_pct": (None, .1, .2),
}
FIXED = dict(initial_stop_pct=1.2, tighten_after_minutes=120,
             tightened_stop_pct=1., activation_floor_pct=.4,
             cost_bps_round_trip=5.)
SELECTION = dict(minimum_changed_development_trades=3,
                 minimum_original_winner_profit_retention_pct=98.,
                 require_development_net_gain=True,
                 require_development_loss_money_reduction=True,
                 rank="maximum loss money saved, then maximum net gain, then policy id")


def catalog() -> list[dict]:
    """A fixed grid; no price or outcome data enters its construction."""
    candidates = []
    combinations = itertools.product(*(GRID[key] for key in GRID))
    for number, (fraction, multiple, floor) in enumerate(combinations, 1):
        parameters = dict(FIXED, activation_target_fraction=fraction,
                          atr_multiple=multiple, protected_profit_pct=floor)
        candidates.append(dict(policy=f"atr_{number:03d}", parameters=parameters,
                               family="atr_only" if floor is None else "atr_with_profit_floor"))
    return candidates


def eligible(row: pd.Series | dict) -> bool:
    return bool(int(row["changed_trades"]) >= SELECTION["minimum_changed_development_trades"]
        and float(row["original_winner_profit_retention_pct"]) >= SELECTION["minimum_original_winner_profit_retention_pct"] - 1e-8
        and float(row["net_change_rs"]) > 1e-7
        and float(row["loss_saved_rs"]) > 1e-7)


def choose(development: pd.DataFrame) -> str | None:
    accepted = development.loc[development.apply(eligible, axis=1)]
    if accepted.empty:
        return None
    return str(accepted.sort_values(["loss_saved_rs", "net_change_rs", "policy"],
        ascending=[False, False, True], kind="stable").iloc[0].policy)


def _simulate(row: dict, context: pd.DataFrame, path: dict, parameters: dict) -> dict:
    return prior.simulate_atr_trailing_exit(path, context, int(row["entry_path_index"]),
        float(row["entry_price"]), row["side"] == "LONG",
        float(row["native_target_pct"]), parameters)


def _outcomes(native: pd.DataFrame, contexts: dict, paths: dict, candidates: list[dict]):
    fixed = native.loc[native.portfolio_executed].set_index("sid", drop=False)
    before = pd.to_numeric(fixed.portfolio_net_profit_rupees)
    columns = {"baseline": before.copy()}
    records = fixed.to_dict("records")
    for index, candidate in enumerate(candidates, 1):
        values = {}
        for row in records:
            sid = int(row["sid"])
            outcome = _simulate(row, contexts[sid], paths[sid], candidate["parameters"])
            values[sid] = (outcome["gross_return_pct"] - .05) * 5000.
        columns[candidate["policy"]] = pd.Series(values, dtype=float).reindex(before.index)
        if index % 12 == 0 or index == len(candidates):
            print(f"Causal ATR candidates: {index}/{len(candidates)}", flush=True)
    frame = pd.DataFrame(columns, index=before.index)
    if not np.allclose(frame.baseline, pd.to_numeric(fixed.net_return_pct) * 5000., atol=1e-6, rtol=0):
        raise ValueError("Sealed baseline accounting differs from fixed exposure")
    return fixed, frame


def _comparison(frame: pd.DataFrame, meta: pd.DataFrame, days: list[str], candidates: list[dict]):
    periods = {"all": days, "development35": days[:35],
               f"later{len(days)-35}": days[35:], "evaluation15": days[35:50],
               f"latest{len(days)-50}": days[50:]}
    rows = []
    for period, cohort in periods.items():
        for candidate in candidates:
            name = candidate["policy"]
            result = core.metrics(frame.baseline, frame[name], meta, cohort)
            rows.append(dict(period=period, policy=name, family=candidate["family"],
                             eligible_on_development=eligible(result) if period == "development35" else False,
                             **result))
    return pd.DataFrame(rows)


def _replay(native: pd.DataFrame, contexts: dict, paths: dict, candidate: dict,
            expected: pd.Series, output: Path):
    parameters = candidate["parameters"]
    records, changes = [], []
    for original in native.to_dict("records"):
        row = original.copy()
        row.update(monitor_exit=False, monitor_flags="[]", stop_updates_json="[]")
        if bool(original["filled"]):
            sid = int(original["sid"])
            control = prior._baseline_outcome(original, paths[sid])
            if (not np.isclose(control["exit_price"], original["exit_price"], atol=1e-8, rtol=0)
                    or pd.Timestamp(control["exit_ts"]) != pd.Timestamp(original["exit_ts"])):
                raise ValueError(f"Original exit parity differs for sid {sid}")
            result = _simulate(original, contexts[sid], paths[sid], parameters)
            changed = (not np.isclose(result["exit_price"], control["exit_price"], atol=1e-8, rtol=0)
                or pd.Timestamp(result["exit_ts"]) != pd.Timestamp(control["exit_ts"]))
            row.update(result)
            row.update(net_return_pct=result["gross_return_pct"]-.05, cost_pct=.05,
                       monitor_exit=changed, monitor_flags=json.dumps([candidate["policy"]]) if changed else "[]")
            if changed:
                changes.append(dict(sid=sid, day=str(original["day"])[:10],
                    setup_id=original["setup_id"], symbol=original["tradingsymbol"],
                    side=original["side"], originally_executed=bool(original["portfolio_executed"]),
                    baseline_net_rs=(control["gross_return_pct"]-.05)*5000.,
                    candidate_net_rs=(result["gross_return_pct"]-.05)*5000.,
                    delta_net_rs=(result["gross_return_pct"]-control["gross_return_pct"])*5000.,
                    baseline_exit_reason=control["exit_reason"], candidate_exit_reason=result["exit_reason"],
                    original_exit_ts=str(control["exit_ts"]), candidate_exit_ts=str(result["exit_ts"]),
                    active_stop_price=result["active_stop_price"],
                    exit_gap_through=result["exit_gap_through"],
                    same_bar_ambiguous=result["same_bar_ambiguous"]))
        records.append(row)
    raw = pd.DataFrame(records)
    book = allocator.allocate(raw, candidate["policy"])
    saved = book.set_index("sid")
    if not np.allclose(saved.loc[expected.index, "net_profit_rupees"], expected, atol=1e-6, rtol=0):
        raise ValueError("Selected portfolio accounting differs from candidate matrix")
    book.to_csv(output / "replayed_portfolio_ledger.csv", index=False)
    changes_frame = pd.DataFrame(changes)
    changes_frame.to_csv(output / "replayed_changed_orders.csv", index=False)
    changes_frame.loc[changes_frame.originally_executed].to_csv(output / "replayed_changed_original_trades.csv", index=False)
    return book, changes_frame


def _report(output: Path, summary: dict, comparison: pd.DataFrame):
    development = comparison.loc[comparison.period.eq("development35")]
    top = development.sort_values(["loss_saved_rs", "net_change_rs"], ascending=False).head(12)
    selected = summary["selected_policy"]
    names = [selected] if selected else []
    if summary["best_full_history_both_positive_policy"] not in names:
        names.append(summary["best_full_history_both_positive_policy"])
    names = [name for name in names if name]
    shown = comparison.loc[comparison.policy.isin(names),
        ["period", "policy", "trades", "wins", "losses", "negative_trade_sum_abs_rs",
         "loss_reduction_pct", "net_pnl_rs", "net_change_rs", "changed_trades",
         "original_winner_profit_retention_pct", "daily_close_drawdown_rs"]]
    page = "<!doctype html><html lang='en'><head><meta charset='utf-8'><meta name='viewport' content='width=device-width,initial-scale=1'><title>G4 trailing SL parameter study</title><style>body{background:#0d1726;color:#e9f0f8;font:15px/1.55 system-ui;margin:30px}main{max-width:1500px;margin:auto}section{background:#17253a;padding:22px;border-radius:9px;margin:20px 0}.scroll{overflow:auto}table{border-collapse:collapse;white-space:nowrap;font-size:12px}td,th{border-bottom:1px solid #3a5066;padding:8px;text-align:right}td:first-child,th:first-child{text-align:left}a{color:#82d4ff}code{color:#b5ebd0}</style></head><body><main>"
    page += "<h1>G4: ATR trailing stop parameter study</h1>"
    page += f"<p>{summary['first_day']} to {summary['last_day']} · {summary['sessions']} sessions · {summary['original_executed_trades']} originally executed trades · backtest only</p>"
    page += "<section><h2>Decision</h2><p>" + html.escape(summary["conclusion"]) + "</p><p>Selection used the first 35 sessions only. The later 17 sessions were examined afterward, but the entire history was already used for G4 development; this is not untouched future validation.</p></section>"
    page += "<section><h2>Grid and selection</h2><p>Activation at max(0.40%, 50%, 70% or 90% of the target); ATR multiples 1, 1.5, 2, 3, 4 or 6; no fixed floor or a +0.10% or +0.20% gross floor. Stops retain the native 1.20% initial limit and 1.00% limit after 120 minutes. All new stops start on the following minute, tighten only, use observed opens for gap-through and resolve same-minute stop/target ambiguity stop-first.</p><p>Qualification requires at least 3 changed development trades, at least 98% of original winners' aggregate profit retained, higher net profit and lower total loss amount on those first 35 sessions. Rank by loss amount saved, then net gain. Full-history winners are not a selection input.</p></section>"
    def render(frame):
        return '<div class="scroll">' + frame.to_html(index=False, border=0,
            float_format=lambda value: f"{value:,.2f}") + '</div>'
    page += "<section><h2>Selected and diagnostic candidates</h2>" + render(shown) + "</section>"
    page += "<section><h2>Highest development loss savings, including failures</h2>" + render(top[["policy", "family", "changed_trades", "loss_saved_rs", "net_change_rs", "original_winner_profit_retention_pct", "eligible_on_development"]]) + "</section>"
    page += "<section><h2>Evidence and rollback</h2><p>Configuration, source hashes, every grid result and the replayed trade/portfolio evidence are recorded here. The replay is labelled retrospective if it was chosen by full-history inspection. No existing strategy config, live state, or latest pointer was changed. To reverse this implementation, archive the three new files named in <code>rollback.json</code> after checking their hashes; retain the result folder. The earlier separate/combined SL study and its rollback tool remain intact.</p><ul>"
    for file, label in [("summary.json", "Summary"), ("protocol.json", "Frozen grid and rules"),
        ("comparison.csv", "All policies and periods"), ("candidate_returns.csv", "Every original trade and policy"),
        ("selected_policy.json", "Selection decision"), ("replayed_changed_original_trades.csv", "Changed trades"),
        ("replayed_portfolio_ledger.csv", "Full portfolio replay"), ("daily_comparison.csv", "Daily outcomes"),
        ("cost_stress.csv", "Cost sensitivity"), ("rollback.json", "Rollback inventory")]:
        page += f'<li><a href="{file}">{html.escape(label)}</a></li>'
    page += "</ul><p>One-minute bars cannot establish exact intrabar order, spreads, queueing or broker latency. The flat 5 bps cost model excludes market impact; drawdown uses daily realized P&amp;L.</p></section></main></body></html>"
    (output / "report.html").write_text(page, encoding="utf-8")


def run(output: Path, data_dir: Path = core.DEFAULT_DATA) -> Path:
    output = Path(output).resolve()
    if output.exists() or output.parent != RESEARCH_ROOT:
        raise ValueError("Fresh output must be a direct child of the G4 research directory")
    catalog_items = catalog()
    source_files = [Path(__file__).resolve(),
                    Path(__file__).resolve().parents[1] / "tests/test_g4_atr_trail_sweep.py",
                    Path(__file__).resolve().parents[0] / "g4_atr_trail_current_dataset.py"]
    hashes = {str(path): base.sha256(path) for path in source_files}
    pointers = [base.G4_ROOT / "latest/bounded_exit.json", base.G4_ROOT / "latest/g3_mirror.json"]
    pointer_hashes = {str(path): base.sha256(path) for path in pointers if path.exists()}
    dataset_hash = base.sha256(Path(data_dir) / "manifest.json")
    output.mkdir(parents=True)
    core.json_write(output / "protocol.json", dict(version=VERSION, scope="BACKTEST_ONLY",
        execution_authority=False, grid=GRID, fixed=FIXED, selection=SELECTION,
        catalog=catalog_items, dataset_manifest_sha256=dataset_hash,
        source_sha256=hashes, previous_sl_engine_sha256=base.sha256(Path(prior.__file__)),
        pre_run_pointer_sha256=pointer_hashes,
        history_reused_for_prior_g4_experiments=True))
    native, contexts, paths, proof = core.dataset.load(data_dir)
    days = list(proof["days"])
    native["day"] = native.day.astype(str).str[:10]
    native = native.sort_values(["day", "confirmation_ts", "sid"], kind="stable").reset_index(drop=True)
    meta, frame = _outcomes(native, contexts, paths, catalog_items)
    frame.reset_index().to_csv(output / "candidate_returns.csv", index=False)
    comparison = _comparison(frame, meta, days, catalog_items)
    comparison.to_csv(output / "comparison.csv", index=False)
    development = comparison.loc[comparison.period.eq("development35")]
    selected_name = choose(development)
    by_name = {item["policy"]: item for item in catalog_items}
    # Full-history positive rows are explicitly diagnostic, not used to choose.
    all_period = comparison.loc[comparison.period.eq("all")]
    positive = all_period.loc[(all_period.net_change_rs > 1e-7) & (all_period.loss_saved_rs > 1e-7)]
    best_full = (str(positive.sort_values(["loss_saved_rs", "net_change_rs", "policy"],
        ascending=[False, False, True]).iloc[0].policy) if not positive.empty else None)
    picked = by_name[selected_name] if selected_name else None
    replay_name = selected_name or best_full
    replayed = by_name[replay_name] if replay_name else None
    replay_basis = ("development35_qualified" if selected_name else
                    "retrospective_full_history_diagnostic" if best_full else "native_baseline")
    core.json_write(output / "selected_policy.json", dict(selected_policy=picked,
        selected_on="development35", qualified=bool(picked),
        later_sessions_used_for_formal_selection=False,
        all_sessions_used_for_diagnostic_replay=selected_name is None and best_full is not None,
        best_full_history_positive_diagnostic=by_name[best_full] if best_full else None,
        replayed_policy=replayed, replay_basis=replay_basis))
    baseline = allocator.allocate(native.copy(), "baseline")
    core.sealed.parity(baseline, native)
    if replayed:
        book, changes = _replay(native, contexts, paths, replayed, frame[replay_name], output)
        if not np.allclose(book.set_index("sid").loc[meta.index, "net_profit_rupees"],
                           frame[replay_name], atol=1e-6, rtol=0):
            raise ValueError("Full portfolio and fixed-cohort outcomes differ")
        full_stats = prior.portfolio_stats(book, days)
        new_admissions = int((book.set_index("sid").portfolio_executed & ~native.set_index("sid").portfolio_executed).sum())
        new_rejections = int((~book.set_index("sid").portfolio_executed & native.set_index("sid").portfolio_executed).sum())
    else:
        book, changes = baseline, pd.DataFrame()
        full_stats = prior.portfolio_stats(baseline, days)
        new_admissions = new_rejections = 0
        baseline.to_csv(output / "replayed_portfolio_ledger.csv", index=False)
        pd.DataFrame(columns=["sid", "day", "delta_net_rs"]).to_csv(output / "replayed_changed_original_trades.csv", index=False)
    before = frame.baseline
    after = frame[replay_name] if replay_name else before
    daily = pd.DataFrame([dict(day=day, **core.metrics(before, after, meta, [day])) for day in days])
    daily.to_csv(output / "daily_comparison.csv", index=False)
    changed = (after-before).abs().gt(1e-7)
    stresses = []
    for scenario, base_pnl, variant_pnl in (
        ("original_5bps", before, after),
        ("extra_5bps_changed_exits", before, after - 250.*changed),
        ("10bps_all_trades", before-250., after-250.),
    ):
        for period, cohort in {"all": days, "development35": days[:35],
                               f"later{len(days)-35}": days[35:]}.items():
            stresses.append(dict(scenario=scenario, period=period,
                                 **core.metrics(base_pnl, variant_pnl, meta, cohort)))
    pd.DataFrame(stresses).to_csv(output / "cost_stress.csv", index=False)
    chosen_all = core.metrics(before, after, meta, days)
    chosen_dev = core.metrics(before, after, meta, days[:35])
    chosen_later = core.metrics(before, after, meta, days[35:])
    conclusion = (f"Development-qualified {selected_name}: all-history net change Rs{chosen_all['net_change_rs']:,.2f}, "
                  f"loss amount saved Rs{chosen_all['loss_saved_rs']:,.2f}, winner profit retained "
                  f"{chosen_all['original_winner_profit_retention_pct']:.2f}%. "
                  f"Later{len(days)-35} net change Rs{chosen_later['net_change_rs']:,.2f}, "
                  f"loss amount saved Rs{chosen_later['loss_saved_rs']:,.2f}."
                  if selected_name else
                  (f"No tested ATR trailing parameter met the prespecified development screen. "
                   f"Retrospective {best_full}: all-history net change Rs{chosen_all['net_change_rs']:,.2f}, "
                   f"loss amount saved Rs{chosen_all['loss_saved_rs']:,.2f}; first35 net change "
                   f"Rs{chosen_dev['net_change_rs']:,.2f}. This is an implemented research candidate, "
                   f"not a qualified replacement for native G4."
                   if best_full else
                   "No tested ATR trailing parameter met the prespecified development screen or improved both objectives on the full history. The native G4 exit remains the selected fallback."))
    summary = dict(variant_id=VERSION, scope="BACKTEST_ONLY", execution_authority=False,
        created_at_ist=datetime.now(base.production.IST).isoformat(),
        first_day=days[0], last_day=days[-1], sessions=len(days),
        selected_orders=len(native), original_executed_trades=len(meta), tested_policies=len(catalog_items),
        development_qualified=int(development.eligible_on_development.sum()),
        selected_policy=selected_name, selected_parameters=picked,
        best_full_history_both_positive_policy=best_full,
        replayed_policy=replay_name, replay_basis=replay_basis,
        replayed_fixed_cohort_all=chosen_all, replayed_development35=chosen_dev,
        replayed_later_sessions=chosen_later, replayed_full_portfolio=full_stats,
        baseline_full_portfolio=prior.portfolio_stats(native, days),
        newly_admitted=new_admissions, originally_executed_now_rejected=new_rejections,
        changed_filled_orders=len(changes), dataset=str(Path(data_dir).resolve()),
        dataset_manifest_sha256=dataset_hash,
        source_sha256=hashes, pre_run_pointer_sha256=pointer_hashes,
        previous_sl_engine_sha256=base.sha256(Path(prior.__file__)),
        conclusion=conclusion,
        limitations=[f"The {len(days)} G4 sessions had already been used for entry and exit research; later sessions are chronological diagnostics, not an untouched prospective test.",
                     "Testing 54 related candidate settings increases historical selection bias.",
                     "The primary comparison fixes the original executed trades; separate full portfolio replay allows allocation changes.",
                     "One-minute OHLC cannot establish tick-level sequencing or real broker stop fills; five bps costs omit actual spread/market impact."])
    core.json_write(output / "summary.json", summary)
    rollback = dict(new_repository_files=[str(path) for path in source_files],
        new_repository_file_sha256=hashes, generated_result_directory=str(output),
        existing_strategy_or_latest_files_changed=False,
        rollback="Archive the three listed new study files after confirming their hashes; leave this result directory intact. Restore by moving the same archived files back only if destinations are absent. The prior G4 SL study and rollback utility are unaffected.")
    core.json_write(output / "rollback.json", rollback)
    _report(output, summary, comparison)
    for filename, digest in {**hashes, **pointer_hashes}.items():
        if base.sha256(Path(filename)) != digest:
            raise ValueError("Research source or latest pointer changed during replay: " + filename)
    if base.sha256(Path(prior.__file__)) != summary["previous_sl_engine_sha256"]:
        raise ValueError("Previous tested exit engine changed during replay")
    if base.sha256(Path(data_dir) / "manifest.json") != dataset_hash:
        raise ValueError("Pinned dataset changed during replay")
    manifest = {path.relative_to(output).as_posix(): base.sha256(path)
                for path in output.rglob("*") if path.is_file()}
    core.json_write(output / "manifest.json", dict(version=VERSION, files=manifest))
    print(conclusion, flush=True)
    print(output, flush=True)
    return output


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--dataset", type=Path, default=core.DEFAULT_DATA)
    args = parser.parse_args()
    stamp = datetime.now(base.production.IST)
    destination = args.output_dir or RESEARCH_ROOT / f"{stamp:%Y%m%dT%H%M%S}_{secrets.token_hex(3)}_atr_trail_grid_v1"
    run(destination, args.dataset)
