"""Current replay compatibility API and read-only verification of V7 history.

New builds always reconstruct candidates using the current backtest builder.
Saved V7 packages retain their archived contract and provenance verification.
"""
from __future__ import annotations

from datetime import date
import json
from pathlib import Path

import numpy as np
import pandas as pd

from research.g3_backtest_profile import production as contract
from research.archived_g3_v7 import contract as archived_contract
import fno_v13_v10_g_3_daily_replay as replay
from research.g3_0920_backtest import _stats

SCHEMA = "fno_g3_current_backtest_v3"
LEGACY_FILES = {"config.json", "summary.json", "daily_results.csv", "portfolio_trades.csv", "report.html"}
FILES = LEGACY_FILES | {"setup_results.csv", "monthly_results.csv", "period_results.csv"}


def verify_base(directory):
    from research.g3_afternoon_backtest import verified_package as verify_selected
    return verify_selected(directory)


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


def _validate_ledger(ledger, days, *, profile=None):
    profile = contract if profile is None else profile
    if ledger.signal_id.isna().any() or ledger.signal_id.duplicated().any():
        raise ValueError("Duplicate or missing historical signal identity")
    if not set(ledger.day.astype(str)).issubset(days):
        raise ValueError("Trade outside verified session calendar")
    for key, expected in (("strategy_version", profile.STRATEGY_VERSION),
                          ("strategy_fingerprint", profile.strategy_fingerprint())):
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


def verified_legacy_package(directory, root):
    """Verify an immutable V7 extension without applying the current profile."""
    directory, root = Path(directory).resolve(), Path(root).resolve()
    manifest = _read(directory / "manifest.json")
    if (manifest.get("schema_version") != SCHEMA or manifest.get("execution_authority") is not False
            or set(manifest.get("files", {})) not in (FILES, LEGACY_FILES)):
        raise ValueError("Invalid extended history manifest")
    for name, digest in manifest["files"].items():
        if replay.sha256(directory / name) != digest:
            raise ValueError("Extended history artifact changed")
    summary = _read(directory / "summary.json")
    if (_read(directory / "config.json") != archived_contract.strategy_payload()
            or not archived_contract.supported_identity(summary.get("strategy_version"), summary.get("strategy_fingerprint"))
            or summary.get("active_setup_count") != len(archived_contract.ACTIVE_SETUPS)
            or summary.get("execution_authority") is not False):
        raise ValueError("Extended history differs from its archived V7 configuration")
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
        _validate_package(root, member, payload, role="production_daily", require_complete=True, allow_legacy=True)
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
    _validate_ledger(ledger, days, profile=archived_contract)
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


def verified_package(directory, root):
    """Dispatch by saved schema; archived evidence never becomes a current run."""
    from research import g3_target_cutoff_backtest as current
    manifest = _read(Path(directory) / "manifest.json")
    if manifest.get("schema_version") == current.SCHEMA:
        return current.verified_package(directory, root)
    if manifest.get("schema_version") == SCHEMA:
        return verified_legacy_package(directory, root)
    if manifest.get("variant_id") == archived_contract.STRATEGY_VERSION:
        return verify_base(directory)
    raise ValueError("Unrecognized G3 historical package schema")


def current_base(root):
    """Resolve a verified saved run without generating or migrating old orders."""
    root = Path(root).resolve()
    pointer = _read(root / "latest/current_backtest.json")
    directory = _checked_source(root, {**pointer, "manifest_name": "manifest.json"})
    summary, manifest = verified_package(directory, root)
    if any(pointer.get(key) != summary.get(key)
           for key in ("strategy_version", "strategy_fingerprint")):
        raise ValueError("Current G3 historical pointer differs from its package")
    if manifest.get("schema_version") == SCHEMA:
        return _checked_source(root, summary["base_source"])
    return directory


def build(root, output, through, *, base=None, roots=None):
    """Use the full current candidate reconstruction, including newly enabled slots."""
    from research.g3_target_cutoff_backtest import build as build_current
    return build_current(root, output, through, base=base, roots=roots)


def publish(root, directory):
    """Publish only the current backtest profile through its guarded publisher."""
    from research.g3_target_cutoff_backtest import publish as publish_current
    return publish_current(root, directory)
