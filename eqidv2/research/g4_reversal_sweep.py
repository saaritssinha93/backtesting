"""Predeclared, research-only G4 reversal exit study on a sealed dataset."""
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

from research import g4_g3_mirror_backtest as base
from research import g4_iterative_exit_backtest as core
from research import g4_separate_sl_variants as prior
from research import g4_trade_monitor_backtest as allocator
from research import g4_failed_breakout_exit as earlier
from research.g4_reversal_exit import simulate_reversal_exit


VERSION = "G4-REVERSAL-EXIT-GRID-V1"
RESEARCH_ROOT = (base.G4_ROOT / "research").resolve()
GRID = dict(adverse_loss_threshold_pct=(.25, .4, .6),
            adverse_consecutive_closes=(2, 3),
            pullback_activation_profit_pct=(.4, .7),
            pullback_from_best_close_pct=(.2, .35, .5),
            pullback_min_signal_profit_pct=(.1, .2))
SELECTION = dict(minimum_changed_development_trades=3,
    require_fewer_development_losing_trades=True,
    require_development_net_gain=True,
    require_development_loss_amount_reduction=True,
    minimum_original_winner_profit_retention_pct=98.,
    rank="fewest losing trades, then most net gain, then policy id")


def catalog() -> list[dict]:
    items = []
    for threshold, count in itertools.product(
        GRID["adverse_loss_threshold_pct"], GRID["adverse_consecutive_closes"]
    ):
        items.append(dict(policy=f"rev_{len(items)+1:03d}", family="adverse_persistence",
            parameters=dict(kind="adverse_persistence", loss_threshold_pct=threshold,
                            consecutive_closes=count)))
    for activation, pullback, minimum in itertools.product(
        GRID["pullback_activation_profit_pct"],
        GRID["pullback_from_best_close_pct"],
        GRID["pullback_min_signal_profit_pct"],
    ):
        items.append(dict(policy=f"rev_{len(items)+1:03d}", family="profitable_pullback",
            parameters=dict(kind="profitable_pullback", activation_profit_pct=activation,
                            pullback_pct=pullback, min_signal_profit_pct=minimum)))
    return items


def eligible(row: pd.Series | dict) -> bool:
    return bool(int(row["changed_trades"]) >= 3
        and int(row["losses"]) < int(row["baseline_losses"])
        and float(row["net_change_rs"]) > 1e-7
        and float(row["loss_saved_rs"]) > 1e-7
        and float(row["original_winner_profit_retention_pct"]) >= 98. - 1e-8)


def choose(development: pd.DataFrame) -> str | None:
    accepted = development.loc[development.apply(eligible, axis=1)]
    if accepted.empty:
        return None
    return str(accepted.sort_values(["losses", "net_change_rs", "policy"],
        ascending=[True, False, True], kind="stable").iloc[0].policy)


def _simulate(row: dict, path: dict, params: dict) -> dict:
    return simulate_reversal_exit(path, int(row["entry_path_index"]),
        float(row["entry_price"]), row["side"] == "LONG",
        float(row["native_target_pct"]), params)


def _outcomes(native: pd.DataFrame, paths: dict, items: list[dict]):
    fixed = native.loc[native.portfolio_executed].set_index("sid", drop=False)
    baseline = pd.to_numeric(fixed.portfolio_net_profit_rupees)
    columns = {"baseline": baseline.copy()}
    records = fixed.to_dict("records")
    for number, item in enumerate(items, 1):
        values = {}
        for row in records:
            result = _simulate(row, paths[int(row["sid"])], item["parameters"])
            values[int(row["sid"])] = (result["gross_return_pct"] - .05) * 5000.
        columns[item["policy"]] = pd.Series(values, dtype=float).reindex(baseline.index)
        if number % 6 == 0 or number == len(items):
            print(f"Causal reversal candidates: {number}/{len(items)}", flush=True)
    frame = pd.DataFrame(columns, index=baseline.index)
    if not np.allclose(frame.baseline, pd.to_numeric(fixed.net_return_pct) * 5000.,
                       atol=1e-6, rtol=0):
        raise ValueError("Sealed baseline accounting differs from fixed exposure")
    return fixed, frame


def _comparison(frame: pd.DataFrame, meta: pd.DataFrame,
                days: list[str], items: list[dict]) -> pd.DataFrame:
    periods = {"all": days, "development35": days[:35], f"later{len(days)-35}": days[35:],
               "evaluation15": days[35:50], f"latest{len(days)-50}": days[50:]}
    rows = []
    for period, cohort in periods.items():
        for item in items:
            result = core.metrics(frame.baseline, frame[item["policy"]], meta, cohort)
            rows.append(dict(period=period, policy=item["policy"], family=item["family"],
                eligible_on_development=eligible(result) if period == "development35" else False,
                losing_trades_avoided=result["baseline_losses"] - result["losses"], **result))
    return pd.DataFrame(rows)


def _replay(native: pd.DataFrame, paths: dict, item: dict,
            expected: pd.Series, output: Path):
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
                raise ValueError(f"Native parity differs for sid {sid}")
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
                    signal_path_index=result.get("reversal_signal_path_index"),
                    original_exit_ts=str(control["exit_ts"]), candidate_exit_ts=str(result["exit_ts"])))
        records.append(row)
    book = allocator.allocate(pd.DataFrame(records), item["policy"])
    if not np.allclose(book.set_index("sid").loc[expected.index, "net_profit_rupees"],
                       expected, atol=1e-6, rtol=0):
        raise ValueError("Replayed accounting differs from policy matrix")
    book.to_csv(output / "replayed_portfolio_ledger.csv", index=False)
    changes_frame = pd.DataFrame(changes, columns=["sid", "day", "setup_id", "symbol",
        "side", "originally_executed", "baseline_net_rs", "candidate_net_rs",
        "delta_net_rs", "baseline_exit_reason", "candidate_exit_reason",
        "signal_path_index", "original_exit_ts", "candidate_exit_ts"])
    changes_frame.to_csv(output / "replayed_changed_orders.csv", index=False)
    changes_frame.loc[changes_frame.originally_executed].to_csv(
        output / "replayed_changed_original_trades.csv", index=False)
    return book, changes_frame


def _stats(book: pd.DataFrame, days: list[str]) -> dict:
    stats = prior.portfolio_stats(book, days)
    trades = book.loc[book.portfolio_executed & book.day.isin(days)]
    stats["reversal_exits"] = int(trades.exit_reason.eq("REVERSAL_EXIT").sum())
    return stats


def _report(output: Path, summary: dict, comparison: pd.DataFrame):
    names = {name for name in (summary["selected_policy"],
                               summary["diagnostic_replayed_policy"],
                               summary["best_loser_count_policy"]) if name}
    columns = ["period", "policy", "family", "trades", "wins", "losses",
        "losing_trades_avoided", "net_pnl_rs", "net_change_rs", "loss_saved_rs",
        "changed_trades", "original_winners_turned_losses",
        "original_winner_profit_retention_pct", "daily_close_drawdown_rs"]
    chosen = comparison.loc[comparison.policy.isin(names), columns]
    top = comparison.loc[comparison.period.eq("all")].sort_values(
        ["losing_trades_avoided", "net_change_rs"], ascending=False).head(12)
    def table(frame):
        return '<div class="scroll">' + frame.to_html(index=False, border=0,
            float_format=lambda x: f"{x:,.2f}") + '</div>'
    page = "<!doctype html><html lang='en'><head><meta charset='utf-8'><meta name='viewport' content='width=device-width,initial-scale=1'><title>G4 reversal exits</title><style>body{background:#0d1726;color:#e9f0f8;font:15px/1.55 system-ui;margin:30px}main{max-width:1500px;margin:auto}section{background:#17253a;padding:22px;border-radius:9px;margin:20px 0}.scroll{overflow:auto}table{border-collapse:collapse;white-space:nowrap;font-size:12px}td,th{border-bottom:1px solid #3a5066;padding:8px;text-align:right}td:first-child,th:first-child{text-align:left}a{color:#82d4ff}code{color:#b5ebd0}</style></head><body><main>"
    page += "<h1>G4: reversal-confirmed early exit study</h1>"
    page += f"<p>{summary['first_day']} to {summary['last_day']} · {summary['sessions']} sessions · {summary['original_executed_trades']} originally executed trades · backtest only</p>"
    page += "<section><h2>Decision</h2><p>" + html.escape(summary["conclusion"]) + f"</p><p>Native G4 baseline: Rs{summary['baseline_full_portfolio']['net_pnl_rs']:,.2f} net, {summary['baseline_full_portfolio']['losses']} losing trades. A lower loss count is not automatically a better strategy when the exit also cuts winning trades. Later sessions are chronological diagnostics, not an untouched prospective test; all G4 history has supported previous development.</p></section>"
    page += "<section><h2>Predeclared signals</h2><p>Adverse persistence: two or three completed post-entry closes at least 0.25%, 0.40% or 0.60% against entry. Profitable pullback: first reach +0.40% or +0.70% on a completed close, then retreat 0.20%, 0.35% or 0.50% from the best completed close while still at least +0.10% or +0.20% above entry. If a signal survives the native stop/target, exit at the next observed open; any native resting stop gap or marketable target takes precedence. This is a modeled market exit, not a guaranteed broker stop fill.</p><p>Qualification on first 35 sessions requires at least three changed trades, fewer losing trades, higher net profit, lower aggregate loss amount and at least 98% retention of original winners' profit. Qualified candidates rank by fewer losers then net gain.</p></section>"
    page += "<section><h2>Selected or retrospective diagnostics</h2>" + table(chosen) + "</section>"
    page += "<section><h2>Largest full-history loser-count reductions, including failures</h2>" + table(top[["policy", "family", "losing_trades_avoided", "net_change_rs", "loss_saved_rs", "changed_trades", "original_winners_turned_losses"]]) + "</section>"
    page += "<section><h2>Evidence and rollback</h2><p>Existing G4 strategy, live state and latest result pointers were not changed. Archive the three new research/test files listed in rollback.json after confirming their hashes to undo this implementation; keep the results.</p><ul>"
    for name, label in [("summary.json", "Summary"), ("protocol.json", "Frozen protocol"),
        ("comparison.csv", "Every policy and period"), ("candidate_returns.csv", "Every fixed-cohort trade"),
        ("selected_policy.json", "Decision and parameters"),
        ("replayed_changed_original_trades.csv", "Changed executed trades"),
        ("replayed_portfolio_ledger.csv", "Full portfolio replay"),
        ("daily_comparison.csv", "Daily comparison"), ("rollback.json", "Rollback inventory")]:
        page += f'<li><a href="{name}">{html.escape(label)}</a></li>'
    page += "</ul><p>One-minute OHLC does not capture broker latency, actual spread or intraminute order sequencing. Five-bps round-trip costs omit market impact.</p></section></main></body></html>"
    (output / "report.html").write_text(page, encoding="utf-8")


def run(output: Path, data_dir: Path) -> Path:
    output, data_dir = Path(output).resolve(), Path(data_dir).resolve()
    if output.exists() or output.parent != RESEARCH_ROOT:
        raise ValueError("Fresh output must be a direct child of the G4 research directory")
    items = catalog()
    source_files = [Path(__file__).resolve(), Path(__file__).with_name("g4_reversal_exit.py"),
                    Path(__file__).resolve().parents[1] / "tests/test_g4_reversal_sweep.py"]
    source_hashes = {str(path): base.sha256(path) for path in source_files}
    dependencies = [Path(earlier.__file__), Path(prior.__file__)]
    dependency_hashes = {str(path): base.sha256(path) for path in dependencies}
    pointers = [base.G4_ROOT / "latest/bounded_exit.json", base.G4_ROOT / "latest/g3_mirror.json"]
    pointer_hashes = {str(path): base.sha256(path) for path in pointers if path.exists()}
    dataset_hash = base.sha256(data_dir / "manifest.json")
    output.mkdir(parents=True)
    core.json_write(output / "protocol.json", dict(version=VERSION, scope="BACKTEST_ONLY",
        execution_authority=False, grid=GRID, selection=SELECTION, catalog=items,
        dataset_manifest_sha256=dataset_hash, source_sha256=source_hashes,
        dependency_sha256=dependency_hashes, pre_run_pointer_sha256=pointer_hashes,
        history_reused_for_prior_g4_experiments=True))
    native, _contexts, paths, proof = core.dataset.load(data_dir)
    days = list(proof["days"])
    native["day"] = native.day.astype(str).str[:10]
    native = native.sort_values(["day", "confirmation_ts", "sid"], kind="stable").reset_index(drop=True)
    meta, frame = _outcomes(native, paths, items)
    frame.reset_index().to_csv(output / "candidate_returns.csv", index=False)
    comparison = _comparison(frame, meta, days, items)
    comparison.to_csv(output / "comparison.csv", index=False)
    development = comparison.loc[comparison.period.eq("development35")]
    selected_name = choose(development)
    all_period = comparison.loc[comparison.period.eq("all")]
    positive = all_period.loc[(all_period.losing_trades_avoided > 0)
        & (all_period.net_change_rs > 1e-7) & (all_period.loss_saved_rs > 1e-7)]
    diagnostic_name = (str(positive.sort_values(["losing_trades_avoided", "net_change_rs", "policy"],
        ascending=[False, False, True], kind="stable").iloc[0].policy) if not positive.empty else None)
    best_by_net = str(all_period.sort_values(["net_change_rs", "policy"],
        ascending=[False, True], kind="stable").iloc[0].policy)
    best_by_losers = str(all_period.sort_values(["losing_trades_avoided", "net_change_rs", "policy"],
        ascending=[False, False, True], kind="stable").iloc[0].policy)
    replay_name = selected_name or diagnostic_name or best_by_net
    by_name = {item["policy"]: item for item in items}
    replayed = by_name[replay_name] if replay_name else None
    replay_basis = ("development35_qualified" if selected_name else
                    "retrospective_full_history_positive_diagnostic" if diagnostic_name else
                    "retrospective_full_history_failure_diagnostic")
    core.json_write(output / "selected_policy.json", dict(
        selected_policy=by_name[selected_name] if selected_name else None,
        selected_on="development35", qualified=bool(selected_name),
        research_fallback="native_g4" if not selected_name else None,
        later_sessions_used_for_formal_selection=False,
        all_sessions_used_for_diagnostic_replay=selected_name is None,
        diagnostic_replayed_policy=by_name[replay_name] if not selected_name else None,
        best_loser_count_diagnostic=by_name[best_by_losers],
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
        pd.DataFrame(columns=["sid", "day", "delta_net_rs"]).to_csv(
            output / "replayed_changed_original_trades.csv", index=False)
        pd.DataFrame(columns=["sid", "day", "delta_net_rs"]).to_csv(
            output / "replayed_changed_orders.csv", index=False)
    before, after = frame.baseline, frame[replay_name] if replay_name else frame.baseline
    pd.DataFrame([dict(day=day, **core.metrics(before, after, meta, [day]))
                  for day in days]).to_csv(output / "daily_comparison.csv", index=False)
    all_stats = core.metrics(before, after, meta, days)
    dev_stats = core.metrics(before, after, meta, days[:35])
    later_stats = core.metrics(before, after, meta, days[35:])
    if selected_name:
        conclusion = (f"Development-qualified {selected_name}: full-history losing trades "
            f"{all_stats['baseline_losses']} to {all_stats['losses']}, net change "
            f"Rs{all_stats['net_change_rs']:,.2f}; later{len(days)-35} net change "
            f"Rs{later_stats['net_change_rs']:,.2f}. Research-only, not live promoted.")
    elif diagnostic_name:
        conclusion = (f"No reversal rule passed the prespecified development screen. "
            f"Retrospective {diagnostic_name}: losing trades {all_stats['baseline_losses']} "
            f"to {all_stats['losses']}, net change Rs{all_stats['net_change_rs']:,.2f}; "
            f"first35 net change Rs{dev_stats['net_change_rs']:,.2f}. Not a qualified G4 replacement.")
    else:
        best_row = all_period.loc[all_period.policy.eq(best_by_net)].iloc[0]
        count_row = all_period.loc[all_period.policy.eq(best_by_losers)].iloc[0]
        conclusion = ("No reversal rule passed the development screen or improved net profit "
            f"on the full history. Best net result {best_by_net} changed Rs{best_row.net_change_rs:,.2f} "
            f"and avoided {int(best_row.losing_trades_avoided)} losing trades. "
            f"Maximum loser reduction {best_by_losers} avoided {int(count_row.losing_trades_avoided)} "
            f"but changed net by Rs{count_row.net_change_rs:,.2f}. Native G4 remains the fallback.")
    summary = dict(variant_id=VERSION, scope="BACKTEST_ONLY", execution_authority=False,
        created_at_ist=datetime.now(base.production.IST).isoformat(),
        first_day=days[0], last_day=days[-1], sessions=len(days),
        selected_orders=len(native), original_executed_trades=len(meta),
        tested_policies=len(items), development_qualified=int(development.eligible_on_development.sum()),
        selected_policy=selected_name, diagnostic_replayed_policy=replay_name if not selected_name else None,
        best_net_policy=best_by_net, best_loser_count_policy=best_by_losers,
        replayed_policy=replay_name, replay_basis=replay_basis,
        replayed_fixed_cohort_all=all_stats, replayed_development35=dev_stats,
        replayed_later_sessions=later_stats, replayed_full_portfolio=_stats(book, days),
        baseline_full_portfolio=_stats(native, days),
        newly_admitted=new_admissions, originally_executed_now_rejected=new_rejections,
        changed_filled_orders=len(changes), dataset=str(data_dir),
        dataset_manifest_sha256=dataset_hash, source_sha256=source_hashes,
        dependency_sha256=dependency_hashes, pre_run_pointer_sha256=pointer_hashes,
        conclusion=conclusion,
        limitations=["Later sessions are chronological diagnostics, not untouched validation; this G4 history has informed prior research.",
            f"Testing {len(items)} related signals creates historical selection bias.",
            "Early exits at next observed open assume negligible order-decision latency; actual market fills can differ.",
            "The fixed-cohort comparison holds original trades; full portfolio replay checks allocation changes.",
            "One-minute OHLC and five-bps flat costs omit actual spread, market impact and broker latency."])
    core.json_write(output / "summary.json", summary)
    core.json_write(output / "rollback.json", dict(new_repository_files=[str(path) for path in source_files],
        new_repository_file_sha256=source_hashes, generated_result_directory=str(output),
        existing_strategy_or_latest_files_changed=False,
        rollback="Archive the three new research/test files after confirming their hashes; leave results intact. Restore only if destinations are absent."))
    _report(output, summary, comparison)
    for filename, digest in {**source_hashes, **dependency_hashes, **pointer_hashes}.items():
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
    destination = args.output_dir or RESEARCH_ROOT / f"{stamp:%Y%m%dT%H%M%S}_{secrets.token_hex(3)}_reversal_grid_v1"
    run(destination, args.dataset)
