"""Extend the current G3 historical simulation with verified completed replays.

The retained historical cohort is immutable. Later sessions use the same
contract and minute simulator, with raw after-close eligibility disclosed.
No PAPER order files or earlier strategy revisions enter this result.
"""
from __future__ import annotations

from datetime import date, datetime
import json
from pathlib import Path

import numpy as np
import pandas as pd

import fno_v13_v10_g_3_contract as contract
import fno_v13_v10_g_3_daily_replay as replay
from research.g3_current_backtest import verified_package as verify_base
from research.g3_0920_backtest import _stats

SCHEMA = "fno_g3_current_backtest_v3"
FILES = {"config.json", "summary.json", "daily_results.csv", "portfolio_trades.csv", "report.html"}


def _read(path):
    return json.loads(Path(path).read_text(encoding="utf-8-sig"))


def _checked_source(root, record):
    directory = (root / record["run_dir"]).resolve()
    if not directory.is_relative_to(root.resolve()):
        raise ValueError("History source escaped result root")
    path = directory / record["manifest_name"]
    if replay.sha256(path) != record["manifest_sha256"]:
        raise ValueError("History source manifest changed")
    return directory


def _source(root, directory, name):
    return dict(run_dir=directory.resolve().relative_to(root.resolve()).as_posix(),
                manifest_name=name, manifest_sha256=replay.sha256(directory / name))


def _validate_ledger(ledger, days):
    if ledger.signal_id.isna().any() or ledger.signal_id.duplicated().any():
        raise ValueError("Duplicate or missing historical signal identity")
    if not set(ledger.day.astype(str)).issubset(days):
        raise ValueError("Trade outside verified session calendar")
    for key, expected in (("strategy_version", contract.STRATEGY_VERSION),
                          ("strategy_fingerprint", contract.strategy_fingerprint())):
        if not ledger[key].eq(expected).all():
            raise ValueError("Historical ledger mixes strategy revisions")
    amounts = {}
    for key in ("portfolio_net_profit_rupees", "portfolio_gross_profit_rupees", "portfolio_cost_rupees"):
        amounts[key] = pd.to_numeric(ledger[key], errors="raise").to_numpy(dtype=float)
        if not np.isfinite(amounts[key]).all():
            raise ValueError("Non-finite historical accounting")
    if not np.allclose(amounts["portfolio_gross_profit_rupees"] - amounts["portfolio_cost_rupees"],
                       amounts["portfolio_net_profit_rupees"], atol=1e-6, rtol=0):
        raise ValueError("Historical gross/cost/net mismatch")


def _daily_table(ledger, days):
    rows = []
    for day in days:
        stats = _stats(ledger.loc[ledger.day.astype(str).eq(day)], [date.fromisoformat(day)])
        rows.append(dict(day=day, trades=stats["executed_trades"], wins=stats["wins"],
                         losses=stats["losses"], net_pnl=stats["net_pnl_rs"],
                         gross_pnl=stats["gross_pnl_rs"], cost=stats["costs_rs"]))
    frame = pd.DataFrame(rows)
    frame["cumulative_net_pnl"] = frame.net_pnl.cumsum()
    return frame


def verified_package(directory, root):
    directory, root = Path(directory).resolve(), Path(root).resolve()
    manifest = _read(directory / "manifest.json")
    if (manifest.get("schema_version") != SCHEMA or manifest.get("execution_authority") is not False
            or set(manifest.get("files", {})) != FILES):
        raise ValueError("Invalid extended history manifest")
    for name, digest in manifest["files"].items():
        if replay.sha256(directory / name) != digest:
            raise ValueError("Extended history artifact changed")
    summary = _read(directory / "summary.json")
    if (_read(directory / "config.json") != contract.strategy_payload()
            or not contract.supported_identity(summary.get("strategy_version"), summary.get("strategy_fingerprint"))
            or summary.get("execution_authority") is not False):
        raise ValueError("Extended history differs from current configuration")
    days = summary["days"]
    if (not days or days != sorted(set(days)) or summary["sessions"] != len(days)
            or summary["first_date"] != days[0] or summary["last_date"] != days[-1]):
        raise ValueError("Extended history calendar is invalid")
    # Keep the complete provenance chain auditable, including the base package.
    base = _checked_source(root, summary["base_source"])
    base_summary, _ = verify_base(base)
    if base_summary["strategy_fingerprint"] != summary["strategy_fingerprint"]:
        raise ValueError("Historical base configuration differs")
    base_days = pd.read_csv(base / "daily_results.csv").day.astype(str).tolist()
    source_frames = [pd.read_csv(base / "variant_trades.csv")]
    from backtesting_result_v13_v10_g_3_daily import _validate_package
    member_days = []
    for record in summary["daily_sources"]:
        member = _checked_source(root, record)
        payload = _read(member / "run_manifest.json")
        _validate_package(root, member, payload, role="production_daily", require_complete=True)
        for artifact in payload["artifacts"].values():
            path = (member / artifact["path"]).resolve()
            if not path.is_relative_to(member) or replay.sha256(path) != artifact["sha256"]:
                raise ValueError("Daily extension artifact changed")
        member_days.extend(payload["days"])
        member_ledger = pd.read_csv(member / payload["artifacts"]["portfolio_trades"]["path"])
        member_daily = pd.read_csv(member / payload["artifacts"]["daily_results"]["path"])
        expected_daily = _daily_table(member_ledger, payload["days"])
        if member_daily.day.astype(str).tolist() != payload["days"]:
            raise ValueError("Daily extension calendar does not reconcile")
        for key in ("trades", "wins", "losses", "net_pnl"):
            source_key = "net_profit_rupees" if key == "net_pnl" and "net_profit_rupees" in member_daily else key
            if not np.allclose(member_daily[source_key], expected_daily[key], atol=1e-6, rtol=1e-12):
                raise ValueError("Daily extension accounting does not reconcile")
        source_frames.append(member_ledger)
    if (any(day <= base_days[-1] for day in member_days) or len(set(member_days)) != len(member_days)
            or days != sorted(base_days + member_days)):
        raise ValueError("Overlapping or missing extension sessions")
    ledger = pd.read_csv(directory / "portfolio_trades.csv")
    expected = pd.concat(source_frames, ignore_index=True, sort=False)
    pd.testing.assert_frame_equal(ledger, expected, check_dtype=False, check_exact=False, atol=1e-8, rtol=1e-12)
    _validate_ledger(ledger, days)
    daily = pd.read_csv(directory / "daily_results.csv")
    pd.testing.assert_frame_equal(daily, _daily_table(ledger, days), check_dtype=False, atol=1e-6, rtol=1e-12)
    stats = _stats(ledger, [date.fromisoformat(day) for day in days])
    for key, value in stats.items():
        saved = summary["current_g3"][key]
        if value is None and saved is None:
            continue
        if not np.isclose(value, saved, atol=1e-6, rtol=1e-12):
            raise ValueError("Extended summary does not reconcile")
    return summary, manifest


def current_base(root):
    pointer = _read(root / "latest/current_backtest.json")
    directory = _checked_source(root, {**pointer, "manifest_name": "manifest.json"})
    if pointer["schema_version"] == SCHEMA:
        summary, _ = verified_package(directory, root)
        return _checked_source(root, summary["base_source"])
    if pointer["schema_version"] != "fno_g3_current_backtest_v2":
        raise ValueError("Current full V4 history must exist before extending it")
    verify_base(directory)
    return directory


def build(root, output, through, *, base=None, roots=None):
    """Reuse verified full-cohort evidence and replay any missing later day."""
    from backtesting_result_v13_v10_g_3_daily import _verified_daily, _run_id, _session_coverage
    root, output = Path(root).resolve(), Path(output).resolve()
    replay.require_closed_session(through)
    if output.exists():
        raise FileExistsError("Extended history requires a new output directory")
    base = Path(base).resolve() if base is not None else current_base(root)
    base_summary, _ = verify_base(base)
    days = pd.read_csv(base / "daily_results.csv").day.astype(str).tolist()
    if through < date.fromisoformat(days[-1]):
        raise ValueError("Requested cutoff precedes current historical base")
    roots = roots or replay.DataRoots()
    available = sorted(path.stem.removeprefix("near_month_") for path in roots.universe.glob("near_month_*.parquet")
                       if days[-1] < path.stem.removeprefix("near_month_") <= str(through))
    if str(through) != days[-1] and (not available or available[-1] != str(through)):
        raise ValueError("Requested cutoff has no dated market universe; history was not advanced")
    frames = [pd.read_csv(base / "variant_trades.csv")]
    members, coverage = [], []
    for day in available:
        member = _verified_daily(root, date.fromisoformat(day))
        if member is None:
            directory = root / "production/runs" / day / _run_id()
            print(f"[G3 full history] replaying {day} from raw inputs", flush=True)
            payload = replay.replay_day(date.fromisoformat(day), directory, roots=roots)
            if not payload["complete"]:
                raise ValueError(f"Incomplete replay for {day}; full history was not published")
            member = (directory, payload)
        directory, payload = member
        print(f"[G3 full history] verified completed replay {day}", flush=True)
        frames.append(pd.read_csv(directory / payload["artifacts"]["portfolio_trades"]["path"]))
        members.append(_source(root, directory, "run_manifest.json"))
        coverage.extend(_session_coverage(root, directory, payload))
    days += available
    ledger = pd.concat(frames, ignore_index=True, sort=False)
    _validate_ledger(ledger, days)
    stats = _stats(ledger, [date.fromisoformat(day) for day in days])
    summary = dict(schema_version=SCHEMA, execution_authority=False,
        strategy_version=contract.STRATEGY_VERSION, strategy_fingerprint=contract.strategy_fingerprint(),
        active_setup_count=len(contract.ACTIVE_SETUPS), created_at_ist=datetime.now(contract.IST).isoformat(),
        first_date=days[0], last_date=days[-1], sessions=len(days), days=days, current_g3=stats,
        base_source=_source(root, base, "manifest.json"), daily_sources=members,
        historical_base_through=base_summary["last_date"],
        coverage=dict(excluded_sessions=[dict(day="2026-10-01", reason="Incomplete immutable historical input")],
            session_coverage=coverage, historical_method="Verified V4 historical simulation; retrospective opening OI",
            extension_method="Same-contract raw after-close replay; stock-day exclusions disclosed",
            history_scope="FULL_CURRENT_STRATEGY_HISTORY_WITH_DAILY_EXTENSIONS"))
    output.mkdir(parents=True)
    daily = _daily_table(ledger, days)
    ledger.to_csv(output / "portfolio_trades.csv", index=False)
    daily.to_csv(output / "daily_results.csv", index=False)
    replay.atomic_json(output / "config.json", contract.strategy_payload())
    replay.atomic_json(output / "summary.json", summary)
    report = ('<!doctype html><html><meta charset="utf-8"><title>G-3 full historical backtest</title>'
              '<style>body{font:16px system-ui;margin:30px}td,th{padding:8px;text-align:right}table{border-collapse:collapse}</style>'
              f'<h1>G-3: {days[0]} through {days[-1]}</h1><p>{len(days)} sessions; '
              f'{stats["executed_trades"]} trades; net P&amp;L Rs {stats["net_pnl_rs"]:,.2f}.</p>'
              '<p>October 1 excluded: incomplete immutable input. Historical targets were fitted to the base cohort; '
              'opening OI is retrospective. Later sessions use verified raw after-close replay with disclosed stock-day exclusions.</p>'
              + daily.to_html(index=False, float_format=lambda value: f"{value:,.2f}") + '</html>')
    (output / "report.html").write_text(report, encoding="utf-8")
    replay.atomic_json(output / "manifest.json", dict(schema_version=SCHEMA, execution_authority=False,
        files={name: replay.sha256(output / name) for name in sorted(FILES)}))
    verified_package(output, root)
    return summary


def publish(root, directory):
    root, directory = Path(root).resolve(), Path(directory).resolve()
    summary, _ = verified_package(directory, root)
    relative = directory.relative_to(root)
    if relative.parts[:2] != ("backtests", "runs") or len(relative.parts) != 4 or relative.parts[2] != summary["last_date"]:
        raise ValueError("Extended history is outside its canonical destination")
    current = root / "latest/current_backtest.json"
    if current.exists():
        prior = _read(current)
        if (prior.get("strategy_fingerprint") == summary["strategy_fingerprint"]
                and prior.get("session_date", "") > summary["last_date"]):
            raise ValueError("An older replay cannot replace the latest full history")
    pointer = dict(schema_version=SCHEMA, role="backtest_history", execution_authority=False,
        run_dir=relative.as_posix(), manifest_sha256=replay.sha256(directory / "manifest.json"),
        strategy_version=summary["strategy_version"], strategy_fingerprint=summary["strategy_fingerprint"],
        session_date=summary["last_date"], created_at_ist=summary["created_at_ist"])
    replay.atomic_json(root / "latest/current_backtest.json", pointer)
    return pointer
