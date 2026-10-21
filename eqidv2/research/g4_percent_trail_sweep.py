"""Research-only grid of percentage trailing stops on a sealed G4 cohort."""
from __future__ import annotations

import argparse
from datetime import datetime
import html
import itertools
import json
from pathlib import Path
import secrets

import numpy as np
import pandas as pd

from research import g4_atr_trail_sweep as earlier
from research import g4_g3_mirror_backtest as base
from research import g4_iterative_exit_backtest as core
from research import g4_separate_sl_variants as prior
from research import g4_trade_monitor_backtest as allocator
from research.g3_sl_target_simulator import simulate_exit
from research.g4_percent_trail_exit import simulate_percentage_trailing_exit


VERSION = "G4-PERCENT-TRAIL-GRID-V1"
RESEARCH_ROOT = (base.G4_ROOT / "research").resolve()
GRID = dict(activation_target_fraction=(.3, .5, .7, .9),
            trail_distance_pct=(.15, .25, .4, .6, .8, 1.2),
            protected_profit_pct=(None, .1, .2))
FIXED = dict(initial_stop_pct=1.2, tighten_after_minutes=120,
             tightened_stop_pct=1., activation_floor_pct=.4,
             cost_bps_round_trip=5.)


def catalog() -> list[dict]:
    items = []
    for number, (fraction, distance, floor) in enumerate(
        itertools.product(*(GRID[key] for key in GRID)), 1
    ):
        items.append(dict(policy=f"pct_{number:03d}",
            family="percentage_only" if floor is None else "percentage_with_profit_floor",
            parameters=dict(FIXED, activation_target_fraction=fraction,
                            trail_distance_pct=distance, protected_profit_pct=floor)))
    return items


def _simulate(row: dict, path: dict, parameters: dict) -> dict:
    return simulate_percentage_trailing_exit(path, int(row["entry_path_index"]),
        float(row["entry_price"]), row["side"] == "LONG",
        float(row["native_target_pct"]), parameters)


def _outcomes(native: pd.DataFrame, paths: dict, candidates: list[dict]):
    fixed = native.loc[native.portfolio_executed].set_index("sid", drop=False)
    before = pd.to_numeric(fixed.portfolio_net_profit_rupees)
    columns = {"baseline": before.copy()}
    records = fixed.to_dict("records")
    for number, item in enumerate(candidates, 1):
        parameters = item["parameters"]
        values = {}
        for row in records:
            result = _simulate(row, paths[int(row["sid"])], parameters)
            values[int(row["sid"])] = (result["gross_return_pct"] - .05) * 5000.
        columns[item["policy"]] = pd.Series(values, dtype=float).reindex(before.index)
        if number % 12 == 0 or number == len(candidates):
            print(f"Causal percentage candidates: {number}/{len(candidates)}", flush=True)
    frame = pd.DataFrame(columns, index=before.index)
    if not np.allclose(frame.baseline, pd.to_numeric(fixed.net_return_pct) * 5000.,
                       atol=1e-6, rtol=0):
        raise ValueError("Sealed baseline accounting differs from fixed exposure")
    return fixed, frame


def _replay(native: pd.DataFrame, paths: dict, item: dict,
            expected: pd.Series, output: Path, prefix: str = "replayed"):
    records, changes = [], []
    for original in native.to_dict("records"):
        row = original.copy()
        row.update(monitor_exit=False, monitor_flags="[]", stop_updates_json="[]")
        if bool(original["filled"]):
            sid = int(original["sid"])
            control = prior._baseline_outcome(original, paths[sid])
            if (not np.isclose(control["exit_price"], original["exit_price"], atol=1e-8, rtol=0)
                    or pd.Timestamp(control["exit_ts"]) != pd.Timestamp(original["exit_ts"])
                    or control["exit_reason"] != original["exit_reason"]):
                raise ValueError(f"Original exit parity differs for sid {sid}")
            result = _simulate(original, paths[sid], item["parameters"])
            changed = (not np.isclose(result["exit_price"], control["exit_price"], atol=1e-8, rtol=0)
                       or pd.Timestamp(result["exit_ts"]) != pd.Timestamp(control["exit_ts"]))
            row.update(result)
            row.update(net_return_pct=result["gross_return_pct"] - .05, cost_pct=.05,
                       monitor_exit=changed,
                       monitor_flags=json.dumps([item["policy"]]) if changed else "[]")
            if changed:
                changes.append(dict(sid=sid, day=str(original["day"])[:10],
                    setup_id=original["setup_id"], symbol=original["tradingsymbol"],
                    side=original["side"], originally_executed=bool(original["portfolio_executed"]),
                    baseline_net_rs=(control["gross_return_pct"] - .05) * 5000.,
                    candidate_net_rs=(result["gross_return_pct"] - .05) * 5000.,
                    delta_net_rs=(result["gross_return_pct"] - control["gross_return_pct"]) * 5000.,
                    baseline_exit_reason=control["exit_reason"], candidate_exit_reason=result["exit_reason"],
                    original_exit_ts=str(control["exit_ts"]), candidate_exit_ts=str(result["exit_ts"]),
                    active_stop_price=result["active_stop_price"],
                    exit_gap_through=result["exit_gap_through"],
                    same_bar_ambiguous=result["same_bar_ambiguous"]))
        records.append(row)
    book = allocator.allocate(pd.DataFrame(records), item["policy"])
    if not np.allclose(book.set_index("sid").loc[expected.index, "net_profit_rupees"],
                       expected, atol=1e-6, rtol=0):
        raise ValueError("Replayed portfolio accounting differs from policy matrix")
    book.to_csv(output / f"{prefix}_portfolio_ledger.csv", index=False)
    changes_frame = pd.DataFrame(changes, columns=[
        "sid", "day", "setup_id", "symbol", "side", "originally_executed",
        "baseline_net_rs", "candidate_net_rs", "delta_net_rs", "baseline_exit_reason",
        "candidate_exit_reason", "original_exit_ts", "candidate_exit_ts",
        "active_stop_price", "exit_gap_through", "same_bar_ambiguous"])
    changes_frame.to_csv(output / f"{prefix}_changed_orders.csv", index=False)
    changes_frame.loc[changes_frame.originally_executed].to_csv(
        output / f"{prefix}_changed_original_trades.csv", index=False)
    return book, changes_frame


def _floor_only_returns(meta: pd.DataFrame, paths: dict, parameters: dict) -> pd.Series:
    """Isolate the fixed floor of a combined policy at the same activation."""
    activation_fraction = float(parameters["activation_target_fraction"])
    floor = parameters["protected_profit_pct"]
    if floor is None:
        raise ValueError("Floor attribution requires a combined policy")
    values = {}
    for row in meta.to_dict("records"):
        activation = max(float(parameters["activation_floor_pct"]),
                         activation_fraction * float(row["native_target_pct"]))
        outcome = simulate_exit(paths[int(row["sid"])], int(row["entry_path_index"]),
            float(row["entry_price"]), row["side"] == "LONG",
            float(parameters["initial_stop_pct"]), float(row["native_target_pct"]),
            protect_after_close_pct=activation, protected_profit_pct=float(floor),
            tighten_after_minutes=float(parameters["tighten_after_minutes"]),
            tightened_stop_pct=float(parameters["tightened_stop_pct"]))
        values[int(row["sid"])] = (outcome["gross_return_pct"] - .05) * 5000.
    return pd.Series(values, dtype=float).reindex(meta.index)


def _stats(book: pd.DataFrame, days: list[str]) -> dict:
    stats = prior.portfolio_stats(book, days)
    trades = book.loc[book.portfolio_executed & book.day.isin(days)]
    stats["protective_stop_exits"] = int(trades.exit_reason.eq("PERCENT_TRAILING_STOP").sum())
    stats["all_stop_exits"] = stats["native_loss_stop_exits"] + stats["protective_stop_exits"]
    return stats


def _report(output: Path, summary: dict, comparison: pd.DataFrame):
    selected = summary["selected_policy"]
    diagnostic = summary["best_full_history_both_positive_policy"]
    standalone = summary["best_standalone_by_net_policy"]
    names = {name for name in (selected, diagnostic, standalone) if name}
    columns = ["period", "policy", "trades", "wins", "losses", "net_pnl_rs",
               "net_change_rs", "negative_trade_sum_abs_rs", "loss_saved_rs",
               "changed_trades", "original_winner_profit_retention_pct",
               "daily_close_drawdown_rs"]
    shown = comparison.loc[comparison.policy.isin(names), columns]
    development = comparison.loc[comparison.period.eq("development35")]
    top = development.sort_values(["loss_saved_rs", "net_change_rs"], ascending=False).head(12)
    def table(frame):
        return '<div class="scroll">' + frame.to_html(index=False, border=0,
            float_format=lambda value: f"{value:,.2f}") + "</div>"
    page = "<!doctype html><html lang='en'><head><meta charset='utf-8'><meta name='viewport' content='width=device-width,initial-scale=1'><title>G4 percentage trailing stop study</title><style>body{background:#0d1726;color:#e9f0f8;font:15px/1.55 system-ui;margin:30px}main{max-width:1500px;margin:auto}section{background:#17253a;padding:22px;border-radius:9px;margin:20px 0}.scroll{overflow:auto}table{border-collapse:collapse;white-space:nowrap;font-size:12px}td,th{border-bottom:1px solid #3a5066;padding:8px;text-align:right}td:first-child,th:first-child{text-align:left}a{color:#82d4ff}code{color:#b5ebd0}</style></head><body><main>"
    page += "<h1>G4: percentage trailing stop study</h1>"
    page += f"<p>{summary['first_day']} to {summary['last_day']} · {summary['sessions']} sessions · {summary['original_executed_trades']} originally executed trades · backtest only</p>"
    page += "<section><h2>Decision</h2><p>" + html.escape(summary["conclusion"]) + f"</p><p>Baseline net profit: Rs{summary['baseline_full_portfolio']['net_pnl_rs']:,.2f}. Replayed policy net profit: Rs{summary['replayed_full_portfolio']['net_pnl_rs']:,.2f}. The later sessions were not used for formal development selection, but this entire G4 history has been used for prior research. If a candidate is labelled retrospective, it was chosen after inspecting all sessions and is not an independent validation result.</p></section>"
    if summary["floor_attribution"] is not None:
        attribution = summary["floor_attribution"]
        page += ("<section><h2>What produced the gain?</h2><p>The combined candidate's "
            f"fixed-floor-only change was Rs{attribution['floor_only_net_change_rs']:,.2f}; "
            f"the percentage trail added Rs{attribution['incremental_trail_net_change_rs']:,.2f} "
            "on the original executed trades. This attribution uses the same activation and floor, "
            "then removes only the trailing ratchet.</p></section>")
    page += "<section><h2>Prespecified grid</h2><p>Activation at max(0.40%, 30%, 50%, 70% or 90% of the native target); trail 0.15%, 0.25%, 0.40%, 0.60%, 0.80% or 1.20% behind the best completed close; optional gross +0.10% or +0.20% floor. Native 1.20% initial stop, 1.00% after 120 minutes, target, exposure and five-bps round-trip cost are unchanged. Close-based stop changes begin at the next candle. Gaps use observed opens; stop/target ties are stop-first.</p><p>Qualification: on the first 35 sessions, at least three changed trades, at least 98% of original winners' aggregate profit retained, higher net P&amp;L and smaller aggregate losses. Qualified policies rank by loss amount saved, then net gain.</p></section>"
    page += "<section><h2>Selected and retrospective candidates</h2>" + table(shown) + "</section>"
    page += "<section><h2>Largest development loss savings, including failures</h2>" + table(top[["policy", "family", "changed_trades", "loss_saved_rs", "net_change_rs", "original_winner_profit_retention_pct", "eligible_on_development"]]) + "</section>"
    page += "<section><h2>Evidence and rollback</h2><p>All candidate results and the replayed trades are saved below. Existing G4 strategy configuration, live state and latest result pointers were not changed. The three new research files can be archived after confirming their hashes in rollback.json; retain this result folder.</p><ul>"
    for file, label in [("summary.json", "Summary"), ("protocol.json", "Frozen protocol"),
        ("comparison.csv", "All policies and periods"), ("candidate_returns.csv", "Every fixed-cohort trade"),
        ("selected_policy.json", "Decision and exact parameters"),
        ("replayed_changed_original_trades.csv", "Changed executed trades"),
        ("replayed_portfolio_ledger.csv", "Full portfolio replay"),
        ("standalone_net_changed_original_trades.csv", "Best standalone percentage changes"),
        ("standalone_net_portfolio_ledger.csv", "Best standalone percentage replay"),
        ("daily_comparison.csv", "Daily comparison"), ("cost_stress.csv", "Cost sensitivity"),
        ("rollback.json", "Rollback inventory")]:
        page += f'<li><a href="{file}">{html.escape(label)}</a></li>'
    page += "</ul><p>One-minute OHLC cannot establish tick-level sequencing or actual broker stop fills. Five bps does not capture actual spread or market impact; drawdown is based on daily realized P&amp;L.</p></section></main></body></html>"
    (output / "report.html").write_text(page, encoding="utf-8")


def run(output: Path, data_dir: Path) -> Path:
    output, data_dir = Path(output).resolve(), Path(data_dir).resolve()
    if output.exists() or output.parent != RESEARCH_ROOT:
        raise ValueError("Fresh output must be a direct child of the G4 research directory")
    candidates = catalog()
    source_files = [Path(__file__).resolve(), Path(__file__).with_name("g4_percent_trail_exit.py"),
                    Path(__file__).resolve().parents[1] / "tests/test_g4_percent_trail_sweep.py"]
    hashes = {str(path): base.sha256(path) for path in source_files}
    dependencies = [Path(earlier.__file__), Path(prior.__file__)]
    dependency_hashes = {str(path): base.sha256(path) for path in dependencies}
    pointers = [base.G4_ROOT / "latest/bounded_exit.json", base.G4_ROOT / "latest/g3_mirror.json"]
    pointer_hashes = {str(path): base.sha256(path) for path in pointers if path.exists()}
    dataset_hash = base.sha256(data_dir / "manifest.json")
    output.mkdir(parents=True)
    core.json_write(output / "protocol.json", dict(version=VERSION, scope="BACKTEST_ONLY",
        execution_authority=False, grid=GRID, fixed=FIXED, selection=earlier.SELECTION,
        catalog=candidates, dataset_manifest_sha256=dataset_hash,
        source_sha256=hashes, dependency_sha256=dependency_hashes,
        pre_run_pointer_sha256=pointer_hashes,
        history_reused_for_prior_g4_experiments=True))
    native, _contexts, paths, proof = core.dataset.load(data_dir)
    days = list(proof["days"])
    native["day"] = native.day.astype(str).str[:10]
    native = native.sort_values(["day", "confirmation_ts", "sid"], kind="stable").reset_index(drop=True)
    meta, frame = _outcomes(native, paths, candidates)
    frame.reset_index().to_csv(output / "candidate_returns.csv", index=False)
    comparison = earlier._comparison(frame, meta, days, candidates)
    comparison.to_csv(output / "comparison.csv", index=False)
    development = comparison.loc[comparison.period.eq("development35")]
    selected_name = earlier.choose(development)
    by_name = {item["policy"]: item for item in candidates}
    all_period = comparison.loc[comparison.period.eq("all")]
    positive = all_period.loc[(all_period.net_change_rs > 1e-7) & (all_period.loss_saved_rs > 1e-7)]
    best_full = (str(positive.sort_values(["loss_saved_rs", "net_change_rs", "policy"],
        ascending=[False, False, True], kind="stable").iloc[0].policy) if not positive.empty else None)
    standalone_positive = positive.loc[positive.family.eq("percentage_only")]
    pure_net = (str(standalone_positive.sort_values(["net_change_rs", "loss_saved_rs", "policy"],
        ascending=[False, False, True], kind="stable").iloc[0].policy)
        if not standalone_positive.empty else None)
    replay_name = selected_name or best_full
    replayed = by_name[replay_name] if replay_name else None
    replay_basis = ("development35_qualified" if selected_name else
                    "retrospective_full_history_diagnostic" if best_full else "native_baseline")
    core.json_write(output / "selected_policy.json", dict(selected_policy=by_name[selected_name] if selected_name else None,
        selected_on="development35", qualified=bool(selected_name),
        later_sessions_used_for_formal_selection=False,
        all_sessions_used_for_diagnostic_replay=selected_name is None and best_full is not None,
        best_full_history_positive_diagnostic=by_name[best_full] if best_full else None,
        best_standalone_by_net_diagnostic=by_name[pure_net] if pure_net else None,
        replayed_policy=replayed, replay_basis=replay_basis))
    baseline = allocator.allocate(native.copy(), "baseline")
    core.sealed.parity(baseline, native)
    if replayed:
        book, changes = _replay(native, paths, replayed, frame[replay_name], output)
        new_admissions = int((book.set_index("sid").portfolio_executed & ~native.set_index("sid").portfolio_executed).sum())
        new_rejections = int((~book.set_index("sid").portfolio_executed & native.set_index("sid").portfolio_executed).sum())
    else:
        book, changes = baseline, pd.DataFrame()
        new_admissions = new_rejections = 0
        baseline.to_csv(output / "replayed_portfolio_ledger.csv", index=False)
        pd.DataFrame(columns=["sid", "day", "delta_net_rs"]).to_csv(output / "replayed_changed_original_trades.csv", index=False)
        pd.DataFrame(columns=["sid", "day", "delta_net_rs"]).to_csv(output / "replayed_changed_orders.csv", index=False)
    if pure_net:
        pure_book, pure_changes = _replay(native, paths, by_name[pure_net], frame[pure_net],
                                          output, prefix="standalone_net")
        pure_full_stats = _stats(pure_book, days)
        pure_fixed_stats = core.metrics(frame.baseline, frame[pure_net], meta, days)
        pure_development_stats = core.metrics(frame.baseline, frame[pure_net], meta, days[:35])
        pure_later_stats = core.metrics(frame.baseline, frame[pure_net], meta, days[35:])
    else:
        pure_changes = pd.DataFrame()
        pure_full_stats = pure_fixed_stats = pure_development_stats = pure_later_stats = None
        baseline.to_csv(output / "standalone_net_portfolio_ledger.csv", index=False)
        pd.DataFrame(columns=["sid", "day", "delta_net_rs"]).to_csv(
            output / "standalone_net_changed_original_trades.csv", index=False)
        pd.DataFrame(columns=["sid", "day", "delta_net_rs"]).to_csv(
            output / "standalone_net_changed_orders.csv", index=False)
    before, after = frame.baseline, frame[replay_name] if replay_name else frame.baseline
    floor_attribution = None
    if replayed and replayed["parameters"]["protected_profit_pct"] is not None:
        floor_only = _floor_only_returns(meta, paths, replayed["parameters"])
        floor_attribution = dict(
            floor_only_net_change_rs=float((floor_only - before).sum()),
            incremental_trail_net_change_rs=float((after - floor_only).sum()),
            changed_trades_floor_only=int((floor_only - before).abs().gt(1e-7).sum()),
            changed_trades_incremental_trail=int((after - floor_only).abs().gt(1e-7).sum()))
    pd.DataFrame([dict(day=day, **core.metrics(before, after, meta, [day]))
                  for day in days]).to_csv(output / "daily_comparison.csv", index=False)
    changed = (after - before).abs().gt(1e-7)
    stresses = []
    for scenario, base_pnl, policy_pnl in (
        ("original_5bps", before, after),
        ("extra_5bps_changed_exits", before, after - 250. * changed),
        ("10bps_all_trades", before - 250., after - 250.),
    ):
        for period, cohort in {"all": days, "development35": days[:35],
                               f"later{len(days)-35}": days[35:]}.items():
            stresses.append(dict(scenario=scenario, period=period,
                                 **core.metrics(base_pnl, policy_pnl, meta, cohort)))
    pd.DataFrame(stresses).to_csv(output / "cost_stress.csv", index=False)
    all_stats = core.metrics(before, after, meta, days)
    development_stats = core.metrics(before, after, meta, days[:35])
    later_stats = core.metrics(before, after, meta, days[35:])
    if selected_name:
        conclusion = (f"Development-qualified {selected_name}: net change Rs{all_stats['net_change_rs']:,.2f}, "
            f"aggregate losses saved Rs{all_stats['loss_saved_rs']:,.2f}; first35 net change "
            f"Rs{development_stats['net_change_rs']:,.2f}, later{len(days)-35} net change "
            f"Rs{later_stats['net_change_rs']:,.2f}. This remains research-only, not a live G4 promotion.")
    elif best_full:
        conclusion = (f"No percentage trail passed the prespecified development screen. "
            f"Retrospective {best_full}: net change Rs{all_stats['net_change_rs']:,.2f}, "
            f"aggregate losses saved Rs{all_stats['loss_saved_rs']:,.2f}; first35 net change "
            f"Rs{development_stats['net_change_rs']:,.2f}. It is not a qualified replacement for native G4.")
    else:
        conclusion = ("No percentage trail passed the prespecified development screen or "
                      "improved both full-history objectives. Native G4 remains the selected fallback.")
    summary = dict(variant_id=VERSION, scope="BACKTEST_ONLY", execution_authority=False,
        created_at_ist=datetime.now(base.production.IST).isoformat(),
        first_day=days[0], last_day=days[-1], sessions=len(days),
        selected_orders=len(native), original_executed_trades=len(meta),
        tested_policies=len(candidates), development_qualified=int(development.eligible_on_development.sum()),
        selected_policy=selected_name, selected_parameters=by_name[selected_name] if selected_name else None,
        best_full_history_both_positive_policy=best_full,
        best_standalone_by_net_policy=pure_net,
        best_standalone_by_net_parameters=by_name[pure_net] if pure_net else None,
        best_standalone_by_net_fixed_cohort=pure_fixed_stats,
        best_standalone_by_net_development35=pure_development_stats,
        best_standalone_by_net_later_sessions=pure_later_stats,
        best_standalone_by_net_full_portfolio=pure_full_stats,
        best_standalone_by_net_changed_filled_orders=len(pure_changes),
        floor_attribution=floor_attribution,
        replayed_policy=replay_name, replay_basis=replay_basis,
        replayed_fixed_cohort_all=all_stats, replayed_development35=development_stats,
        replayed_later_sessions=later_stats, replayed_full_portfolio=_stats(book, days),
        baseline_full_portfolio=_stats(native, days), newly_admitted=new_admissions,
        originally_executed_now_rejected=new_rejections, changed_filled_orders=len(changes),
        dataset=str(data_dir), dataset_manifest_sha256=dataset_hash,
        source_sha256=hashes, dependency_sha256=dependency_hashes,
        pre_run_pointer_sha256=pointer_hashes, conclusion=conclusion,
        limitations=[f"The {len(days)} G4 sessions were already used for other entry and exit research; chronological later sessions are not untouched prospective validation.",
            f"Testing {len(candidates)} related settings introduces historical selection bias.",
            "Fixed-cohort comparisons hold original executed trades; full portfolio replay checks allocation changes.",
            "One-minute OHLC cannot establish tick-level stop fills; flat five-bps costs omit actual spread and market impact."])
    core.json_write(output / "summary.json", summary)
    core.json_write(output / "rollback.json", dict(new_repository_files=[str(path) for path in source_files],
        new_repository_file_sha256=hashes, generated_result_directory=str(output),
        existing_strategy_or_latest_files_changed=False,
        rollback="Archive the three listed new research/test files only after confirming their hashes; retain this result directory. Restore only if destinations are absent. Prior G4 exit studies are unaffected."))
    _report(output, summary, comparison)
    for filename, digest in {**hashes, **dependency_hashes, **pointer_hashes}.items():
        if base.sha256(Path(filename)) != digest:
            raise ValueError("Research source or latest pointer changed during replay: " + filename)
    if base.sha256(data_dir / "manifest.json") != dataset_hash:
        raise ValueError("Pinned dataset changed during replay")
    manifest = {path.relative_to(output).as_posix(): base.sha256(path)
                for path in output.rglob("*") if path.is_file()}
    core.json_write(output / "manifest.json", dict(version=VERSION, files=manifest))
    print(conclusion, flush=True)
    print(output, flush=True)
    return output


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path)
    args = parser.parse_args()
    stamp = datetime.now(base.production.IST)
    destination = args.output_dir or RESEARCH_ROOT / f"{stamp:%Y%m%dT%H%M%S}_{secrets.token_hex(3)}_percent_trail_grid_v1"
    run(destination, args.dataset)
