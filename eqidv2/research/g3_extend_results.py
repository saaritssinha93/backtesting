"""Append sealed dated G-3 replays and unchanged observer models to an archive.

The archived book and model selection are immutable inputs. New completed
sessions retain the frozen LONG 1.10x/+1 minute rule and G-2 staged exits.
This writes a new research run; it has no live or scheduled execution path.
"""
from __future__ import annotations

import argparse
from datetime import date
import json
from pathlib import Path
import sys

import joblib
import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import fno_oi_hybrid_data as hybrid
from research import g3_daywise_backtest as daywise
from research import g3_freeze as freeze
from research import g3_pullback_followthrough as follow
from research import g3_pullback_monitor as monitor
from research import g3_pullback_selective as selective
from research.g3_pullback_selective_features import FEATURES, build_features


TABLES = (
    "daily_results.csv", "trades_with_pullbacks.csv",
    "pullback_evaluation_windows.csv", "actual_pullback_windows.csv",
    "warning_alerts.csv", "pullback_followthrough.csv",
    "trade_path_summary.csv", "minute_trade_paths.csv",
)


def read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def verify_hashes(root: Path) -> dict[str, str]:
    hashes = read_json(root / "artifact_hashes.json")
    for relative, expected in hashes.items():
        path = (root / relative).resolve()
        if not path.is_relative_to(root.resolve()) or daywise.sha(path) != expected:
            raise ValueError(f"Archived artifact drift: {relative}")
    return hashes


def reconcile(daily: pd.DataFrame, trades: pd.DataFrame, expected_days: list[str]) -> None:
    if daily.day.astype(str).tolist() != expected_days or len(set(expected_days)) != len(expected_days):
        raise ValueError("Daily result calendar differs from verified source sessions")
    if trades.trade_id.duplicated().any() or not set(trades.day).issubset(expected_days):
        raise ValueError("Duplicated trade identity or trade outside verified sessions")
    for base in daily.itertuples(index=False):
        part = trades.loc[trades.day.eq(base.day)]
        if len(part) != int(base.trades):
            raise ValueError(f"Daily trade count mismatch: {base.day}")
        for column in ("net_pnl", "cost", "gross_pnl"):
            if not np.isclose(part[column].sum(), getattr(base, column), rtol=0, atol=1e-7):
                raise ValueError(f"Daily {column} mismatch: {base.day}")
    if not np.allclose(daily.cumulative_net_pnl, daily.net_pnl.cumsum(), rtol=0, atol=1e-7):
        raise ValueError("Cumulative daily P&L mismatch")


def read_replay(folder: Path, frozen: dict) -> tuple[dict, pd.DataFrame]:
    summary = read_json(folder / "summary.json")
    if (summary.get("status") != "COMPLETE_RESEARCH_REPLAY"
            or summary.get("execution_authority") is not False
            or summary.get("strategy") != frozen["config"]["version"]):
        raise ValueError(f"Not a completed frozen G-3 replay: {folder}")
    proof = summary["provenance"]
    for path, expected in (
        (Path(proof["source_g_daily_run"]) / "replay_result.json", proof["source_g_daily_result_sha256"]),
        (Path(proof["feature_ledger"]), proof["feature_ledger_sha256"]),
        (Path(proof["snapshot_manifest"]), proof["snapshot_manifest_sha256"]),
        (Path(proof["frozen_g3_manifest"]), proof["frozen_g3_manifest_sha256"]),
        (Path(proof["g2_source_bundle"]) / "bundle_manifest.json", proof["g2_source_bundle_manifest_sha256"]),
    ):
        if daywise.sha(path) != expected:
            raise ValueError(f"Dated replay source drift: {path}")
    source = read_json(Path(proof["source_g_daily_run"]) / "replay_result.json")
    if (source.get("state") != "SUCCESS" or source.get("complete") is not True
            or source.get("session_date") != summary["session_date"]):
        raise ValueError("Missing completed same-day source")
    path = folder / "trades_G3_W1_V1p1.csv"
    try:
        trades = pd.read_csv(path)
    except pd.errors.EmptyDataError:
        # pandas writes a columnless empty replay as a platform newline. Its
        # byte length is two on Windows; cardinality is checked below.
        trades = frozen["trades"].iloc[:0].copy()
    if len(trades):
        trades["day"] = trades.day.astype(str)
        if set(trades.day) != {summary["session_date"]}:
            raise ValueError("Dated ledger contains another session")
        for column in ("filled", "portfolio_executed"):
            trades[column] = freeze._bool(trades[column])
        for column in ("entry_ts", "exit_ts", "confirmation_ts", "signal_ts", "exit_bar_end_ts", "exit_execution_ts"):
            if column in trades:
                trades[column] = pd.to_datetime(trades[column], utc=True)
    executed = trades.loc[trades.portfolio_executed].copy()
    metrics = summary["G3_W1_V1p1"]
    if (len(trades) != int(metrics["selected_orders"]) or len(executed) != int(metrics["trades"])
            or not np.isclose(executed.portfolio_net_profit_rupees.sum(), metrics["net_pnl"], atol=1e-7, rtol=0)
            or not np.isclose(executed.portfolio_cost_rupees.sum(), metrics["cost"], atol=1e-7, rtol=0)):
        raise ValueError("Dated G-3 ledger does not reconcile to its replay")
    return summary, executed


def build(archive: Path, replay_root: Path, through_day: date, output: Path) -> dict:
    if output.exists():
        raise FileExistsError(f"Refusing to overwrite a run: {output}")
    archive, replay_root = archive.resolve(), replay_root.resolve()
    original_hashes = verify_hashes(archive)
    summary = read_json(archive / "summary.json")
    frozen = freeze.load_frozen(Path(summary["source_frozen"]))
    model_root = Path(summary["source_selective"])
    model_hashes = verify_hashes(model_root)
    locked = read_json(model_root / "locked_selection.json")
    models = joblib.load(model_root / "research_models.joblib")
    if models["selection"] != locked or tuple(models["features"]) != FEATURES or models["observer_only"] is not True:
        raise ValueError("Observer model selection or feature contract changed")
    old = {name: pd.read_csv(archive / name) for name in TABLES}
    old_days = old["daily_results.csv"].day.astype(str).tolist()
    if old_days != frozen["days"]:
        raise ValueError("Archive calendar differs from the frozen G-3 book")
    sources = []
    ledgers = []
    input_hashes = {}
    for folder in sorted(replay_root.iterdir()):
        if not folder.is_dir() or not (folder / "summary.json").is_file():
            continue
        source_summary = read_json(folder / "summary.json")
        day = source_summary.get("session_date", "")
        if day <= max(old_days) or day > through_day.isoformat():
            continue
        source_summary, executed = read_replay(folder, frozen)
        sources.append(source_summary)
        ledgers.append(executed)
        for name in ("summary.json", "selected_G3_W1_V1p1.csv", "trades_G3_W1_V1p1.csv"):
            input_hashes[str(folder / name)] = daywise.sha(folder / name)
    new_days = sorted(source["session_date"] for source in sources)
    if not new_days or new_days[-1] != through_day.isoformat() or len(new_days) != len(set(new_days)):
        raise ValueError("Requested cutoff needs one completed G-3 replay per added session")
    executed = pd.concat(ledgers, ignore_index=True)
    context = []
    context_quality = []
    for source in sources:
        day = source["session_date"]
        snapshot = Path(source["provenance"]["snapshot_manifest"]).parent
        for symbol in sorted(executed.loc[executed.day.eq(day), "tradingsymbol"].unique()):
            path = hybrid.equity_one_minute_path(symbol, snapshot / "equity_1m")
            minute = freeze._minute_input(path, day)
            retained, quality = freeze._context(minute, day, symbol)
            context.append(retained)
            context_quality.append(quality)
            input_hashes[str(path)] = daywise.sha(path)
    all_days = old_days + new_days
    metadata = read_json(model_root / "summary.json")
    metadata["evaluation_days"] = metadata["evaluation_days"] + new_days
    if len(executed):
        minutes = pd.concat(context, ignore_index=True)
        dense, _, trade_report = monitor.make_predictions(executed, minutes, all_days)
        if len(dense):
            features = build_features(dense, trade_report)
            features["day_split"] = "EXTENSION"
            predicted = selective.apply_models(features, locked, models["models"])
        else:
            predicted = pd.read_csv(model_root / "predictions_1m.csv", nrows=0)
        for field in ("decision_ts", "event_ts"):
            predicted[field] = pd.to_datetime(predicted[field], utc=True)
        new_trades = daywise.build_trade_results(executed, predicted, metadata)
        new_trades["split"] = "EXTENSION"
        grid, actual, alerts = daywise.detail_rows(predicted)
    else:
        minutes = pd.DataFrame()
        predicted = pd.read_csv(model_root / "predictions_1m.csv", nrows=0)
        new_trades = old["trades_with_pullbacks.csv"].iloc[:0].copy()
        grid = old["pullback_evaluation_windows.csv"].iloc[:0].copy()
        actual = old["actual_pullback_windows.csv"].iloc[:0].copy()
        alerts = old["warning_alerts.csv"].iloc[:0].copy()
    event_rows, path_rows, path_summary = [], [], []
    contexts = {key: part for key, part in minutes.groupby(["day", "tradingsymbol"])} if len(minutes) else {}
    windows = predicted.loc[predicted.is_grid & predicted.outcome_available & predicted.event]
    for trade in executed.to_dict("records"):
        trade_id = f"{trade['day']}|{trade['setup_id']}|{trade['tradingsymbol']}|{trade['sid']}"
        path = follow.build_path(trade, contexts[(trade["day"], trade["tradingsymbol"])])
        events = [follow.analyze_window(trade, row, path)
                  for row in windows.loc[windows.trade_id.eq(trade_id)].to_dict("records")]
        path_summary.append(follow.summarize_path(trade, path, events))
        event_rows.extend(events)
        path["trade_id"], path["day"] = trade_id, trade["day"]
        for name, source in (("time_ist", "ts"), ("bar_start_time_ist", "bar_start"), ("bar_end_time_ist", "bar_end")):
            path[name] = path[source].map(daywise.local_time)
        path_rows.append(path.drop(columns=["ts", "bar_start", "bar_end"]))
    new_paths = pd.concat(path_rows, ignore_index=True) if path_rows else old["minute_trade_paths.csv"].iloc[:0].copy()
    new_summaries = pd.DataFrame(path_summary) if path_summary else old["trade_path_summary.csv"].iloc[:0].copy()
    if path_summary:
        new_trades = new_trades.merge(new_summaries.drop(columns="day"), on="trade_id", validate="one_to_one")
    new_events = pd.DataFrame(event_rows) if event_rows else old["pullback_followthrough.csv"].iloc[:0].copy()
    if event_rows:
        new_events = new_events.sort_values(["day", "trade_id", "window_start_time_ist", "horizon_minutes"]).reset_index(drop=True)
        offset = len(old["pullback_followthrough.csv"])
        new_events.insert(0, "event_id", [f"PB{offset + i + 1:04d}" for i in range(len(new_events))])
    base_daily = pd.DataFrame([dict(day=s["session_date"], **s["G3_W1_V1p1"]) for s in sources])
    new_daily = daywise.build_daily_results(base_daily, new_trades, metadata)
    new_daily["split"] = "EXTENSION"
    for index, row in new_daily.iterrows():
        part = new_trades.loc[new_trades.day.eq(row.day)]
        has = part.actual_pullback_windows.gt(0)
        counts = dict(target_exits=int(part.exit_reason.eq("TARGET").sum()), stop_exits=int(part.exit_reason.eq("STOP").sum()),
            tightened_stop_exits=int(part.exit_reason.eq("TIGHTENED_STOP").sum()), time_exits=int(part.exit_reason.eq("TIME_EXIT_1515").sum()),
            sideways_final30_time_exits=int(part.trade_exit_outcome.eq("TIME_EXIT_SIDEWAYS_FINAL30").sum()),
            trades_with_pullbacks_ending_sl=int((has & part.exit_reason.isin(["STOP", "TIGHTENED_STOP"])).sum()),
            trades_with_pullbacks_ending_target=int((has & part.exit_reason.eq("TARGET")).sum()),
            trades_without_observed_pullback=int((~has).sum()))
        for name, value in counts.items():
            new_daily.loc[index, name] = value
    additions = dict(zip(TABLES, [new_daily, new_trades, grid, actual, alerts, new_events, new_summaries, new_paths]))
    tables = {name: pd.concat([old[name], additions[name]], ignore_index=True) for name in TABLES}
    daily, trades = tables["daily_results.csv"], tables["trades_with_pullbacks.csv"]
    daily["cumulative_net_pnl"] = daily.net_pnl.cumsum()
    reconcile(daily, trades, all_days)
    for name, frame in tables.items():
        pd.testing.assert_frame_equal(old[name], frame.iloc[:len(old[name])].reset_index(drop=True),
                                      check_dtype=False, check_exact=False, rtol=0, atol=1e-7)
    actual_all, alerts_all = tables["actual_pullback_windows.csv"], tables["warning_alerts.csv"]
    for horizon, prefix in ((5, "fast"), (30, "slow")):
        if int(daily[f"{prefix}_pullback_windows"].sum()) != int(actual_all.horizon_minutes.eq(horizon).sum()):
            raise ValueError("Observed pullback count does not reconcile")
        if int(daily[f"{prefix}_watch_alerts"].sum()) != int(alerts_all.horizon_minutes.eq(horizon).sum()):
            raise ValueError("Watch alert count does not reconcile")
    if len(tables["pullback_followthrough.csv"]) != len(actual_all):
        raise ValueError("Followthrough does not cover every actual pullback window")
    if not tables["pullback_followthrough.csv"].min_stop_headroom_pct_entry.ge(-1e-5).all():
        raise ValueError("Known price path crossed a frozen stop before exit")
    high = np.maximum.accumulate(np.r_[0., daily.cumulative_net_pnl.to_numpy()])[1:]
    summary.update(period_end=max(all_days), sessions=len(daily), trades=len(trades),
        monitored_trades=int(trades.monitoring_minutes.gt(0).sum()), wins=int(trades.net_pnl.gt(0).sum()),
        losses=int(trades.net_pnl.lt(0).sum()), win_rate_pct=float(100 * trades.net_pnl.gt(0).mean()),
        gross_pnl=float(trades.gross_pnl.sum()), cost=float(trades.cost.sum()), net_pnl=float(trades.net_pnl.sum()),
        max_day_end_drawdown=float((high - daily.cumulative_net_pnl).max()),
        no_signal_trades=int(trades.signal_status.eq("NO_QUALIFYING_SIGNAL").sum()),
        mode="VERIFIED_ARCHIVE_PLUS_DATED_REPLAYS_WITH_FROZEN_OBSERVER", new_signals_recomputed=True,
        trade_actions_changed=False, live_orders_enabled=False, extension_days=new_days,
        monitor_model="Unchanged selective V2: original first20 fit / next10 selection; no extension-date fitting or tuning",
        source_archive=str(archive), extension_replays=sources)
    summary["limitations"] = [text for text in summary["limitations"]
                              if "Data ends October 7" not in text and "No new full-universe backtest" not in text]
    summary["limitations"] += [
        "October 1 remains excluded because the archived source session was incomplete.",
        "New sessions use their completed, sealed daily G feature ledger and source coverage exclusions; the frozen G-3 selection and exit rules are unchanged.",
        "EXTENSION dates are scored by the already locked observer models. Warnings never alter trades or P&L.",
    ]
    summary["followthrough"]["window_outcomes"] = tables["pullback_followthrough.csv"].outcome_code.value_counts().to_dict()
    summary["followthrough"]["trade_exit_outcomes"] = tables["trade_path_summary.csv"].trade_exit_outcome.value_counts().to_dict()
    summary["report_artifacts"] = {"daily": "daily_results.csv", "trades": "trades_with_pullbacks.csv",
        "windows": "actual_pullback_windows.csv", "alerts": "warning_alerts.csv", "summary": "summary.json",
        "validation": "validation.json", "followthrough": "pullback_followthrough.csv", "paths": "minute_trade_paths.csv",
        "path_summary": "trade_path_summary.csv"}
    if verify_hashes(archive) != original_hashes or verify_hashes(model_root) != model_hashes:
        raise ValueError("An archived input changed during extension")
    for path, expected in input_hashes.items():
        if daywise.sha(path) != expected:
            raise ValueError(f"Replay input changed during extension: {path}")
    output.mkdir(parents=True, exist_ok=False)
    for name, frame in tables.items():
        frame.to_csv(output / name, index=False)
    executed.to_csv(output / "extension_executed_trades.csv", index=False)
    predicted.to_csv(output / "extension_predictions_1m.csv", index=False)
    pd.DataFrame(context_quality).to_csv(output / "extension_minute_coverage.csv", index=False)
    daywise.write_json(output / "summary.json", summary)
    daywise.write_json(output / "validation.json", dict(status="PASS", sessions=len(daily), trades=len(trades),
        extension_days=new_days, extension_trades=len(executed), archive_hashes_verified=True,
        archived_rows_unchanged=True, frozen_strategy_verified=True, observer_model_hashes_verified=True,
        observer_refitted=False, daily_pnl_cost_and_counts_reconciled=True,
        pullback_windows_and_warning_episodes_reconciled=True, source_files_changed=False,
        input_hashes=input_hashes, archive_artifact_hashes_sha256=daywise.sha(archive / "artifact_hashes.json"),
        observer_artifact_hashes_sha256=daywise.sha(model_root / "artifact_hashes.json"),
        source_code_sha256=daywise.sha(Path(__file__))))
    # Use a compact report whose text accurately describes the extended period.
    from research.g3_daywise_report import write_report
    report = write_report(output, summary, daily, trades, actual_all, alerts_all)
    html = report.read_text(encoding="utf-8")
    html = html.replace("This report packages the verified archived backtest; it is not a new full-universe replay.",
        "This report preserves the verified archive and adds completed dated replays through " + max(all_days) + ".")
    html = html.replace("The workbook separates daily results, trades, observed windows, follow-through and alerts.",
        "The CSV downloads separate daily results, trades, observed windows, follow-through and alerts.")
    html = html.replace("<strong>Evaluation · final 16:</strong>", "<strong>Archived evaluation · 16:</strong>")
    html = html.replace("Each outcome stops at the earlier", 
        "Extension sessions use the previously locked observer models without fitting or threshold changes. Each outcome stops at the earlier")
    report.write_text(html, encoding="utf-8")
    daywise.write_json(output / "artifact_hashes.json", {p.name: daywise.sha(p) for p in output.iterdir() if p.is_file()})
    return summary


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--archive", type=Path, required=True)
    parser.add_argument("--replay-root", type=Path, required=True)
    parser.add_argument("--through-day", type=date.fromisoformat, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = build(args.archive, args.replay_root, args.through_day, args.output)
    print(json.dumps({key: result[key] for key in ("period_end", "sessions", "trades", "net_pnl", "extension_days")}, indent=2))
