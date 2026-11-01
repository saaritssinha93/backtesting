"""Verified, read-only dashboard evidence for the frozen >=70% slot selection.

Publishing copies a completed replay into a fresh package. It never changes
trading state, archived ledgers, or a broker connection.
"""
from __future__ import annotations

import csv
from datetime import date, datetime, timezone
import hashlib
import json
import math
from pathlib import Path
from uuid import uuid4


SCHEMA = "slot_win70_dashboard_v1"
ROOT_NAME = "backtesting_result_slot_win70"
STRATEGIES = {
    "G3": ("V13-V10-G-3", "G-3"),
    "G7_V4_LOCK2": ("V13-V10-G-7-V4-LOCK", "G-7 V4 two-loss lock"),
}
NOTICE = (
    "The historical >=70% win-rate cutoff applies to every represented slot/direction pair "
    "separately for each strategy. Slots with zero executed trades are excluded. "
    "Seventeen G3 pairs and nine G7 V4 lock pairs are disabled. "
    "Selection uses results through 27 October 2026. These dates informed selection; "
    "the new portfolio replay is retrospective, not independent validation. "
    "Replay win rates can differ after allocation and the two-loss lock. "
    "Results include modeled 5 bps round-trip costs."
)


def _json(path):
    return json.loads(Path(path).read_text(encoding="utf-8-sig"))


def _write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True, allow_nan=False), encoding="utf-8")


def _sha(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _inside(path, root):
    return Path(path).resolve().is_relative_to(Path(root).resolve())


def _contract(strategy):
    import strategy_slot_selection as selection
    return {
        "policy": selection.policy_payload(strategy),
        "policy_fingerprint": selection.policy_fingerprint(strategy),
        "enabled_setup_ids": sorted(selection.enabled_setup_ids(strategy)),
    }


def configured_policy_summary():
    """Current configuration only; never infer activation or rewrite old packets."""
    import strategy_slot_selection as selection
    return selection.dashboard_summary()


def _number(row, key):
    value = float(row[key])
    if not math.isfinite(value):
        raise ValueError(f"Non-finite {key}")
    return value


def _executed(row):
    value = str(row["portfolio_executed"]).strip().lower()
    if value not in {"true", "false", "1", "0", "1.0", "0.0"}:
        raise ValueError("Invalid portfolio execution flag")
    return value in {"true", "1", "1.0"}


def _rows(path):
    with path.open(newline="", encoding="utf-8-sig") as stream:
        return list(csv.DictReader(stream))


def _daily(rows, days, enabled):
    """Reconcile the entire opportunity book, retaining every zero-trade day."""
    if (not days or days != sorted(set(days))
            or any(date.fromisoformat(day).isoformat() != day for day in days)):
        raise ValueError("Invalid replay calendar")
    daily = {day: dict(day=day, trades=0, wins=0, losses=0, net_pnl=0.0, gross_pnl=0.0, cost=0.0) for day in days}
    ids = set()
    for row in rows:
        day = str(row["day"])[:10]
        if day not in daily:
            raise ValueError("Opportunity outside the saved calendar")
        sid = str(row.get("sid") or row.get("signal_id") or "")
        if not sid or sid in ids:
            raise ValueError("Missing or duplicate opportunity identity")
        ids.add(sid)
        if not _executed(row):
            continue
        if row["setup_id"] not in enabled:
            raise ValueError("Disabled setup executed in selected-slot replay")
        net = _number(row, "portfolio_net_profit_rupees")
        gross = _number(row, "portfolio_gross_profit_rupees")
        cost = _number(row, "portfolio_cost_rupees")
        if cost < 0 or not math.isclose(gross - cost, net, abs_tol=1e-5, rel_tol=1e-9):
            raise ValueError("Trade costs do not reconcile")
        value = daily[day]
        value["trades"] += 1
        value["wins"] += int(net > 1e-8)
        value["losses"] += int(net < -1e-8)
        value["net_pnl"] += net
        value["gross_pnl"] += gross
        value["cost"] += cost
    return list(daily.values())


def _totals(daily):
    value = {key: sum(row[key] for row in daily) for key in ("trades", "wins", "losses", "net_pnl", "gross_pnl", "cost")}
    value["win_rate_pct"] = 100 * value["wins"] / value["trades"] if value["trades"] else None
    return value


def _write_csv(path, rows):
    with path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def _read_verified(run, root, pointer):
    if pointer.get("schema_version") != SCHEMA or pointer.get("execution_authority") is not False:
        raise ValueError("Invalid dashboard pointer identity")
    manifest_path = run / "manifest.json"
    if not _inside(run, root) or _sha(manifest_path) != pointer.get("manifest_sha256"):
        raise ValueError("Changed replay manifest")
    manifest = _json(manifest_path)
    files = manifest.get("files", {})
    if manifest.get("schema_version") != SCHEMA or not isinstance(files, dict) or not files:
        raise ValueError("Invalid replay manifest")
    for name, digest in files.items():
        relative = Path(name)
        path = run / relative
        if (relative.is_absolute() or ".." in relative.parts or not _inside(path, run)
                or _sha(path) != digest):
            raise ValueError("Changed or invalid replay member")
    required = {f"{strategy}/{name}" for strategy in STRATEGIES
                for name in ("config.json", "summary.json", "portfolio_trades.csv", "daily_results.csv")}
    if not required.issubset(files):
        raise ValueError("Incomplete selected-slot package")
    packages = []
    for strategy, (public_name, label) in STRATEGIES.items():
        folder = run / strategy
        config = _json(folder / "config.json")
        summary = _json(folder / "summary.json")
        contract = _contract(strategy)
        if (config != dict(schema_version=SCHEMA, strategy=strategy, **contract)
                or summary.get("schema_version") != SCHEMA
                or summary.get("strategy") != strategy
                or summary.get("policy_fingerprint") != contract["policy_fingerprint"]
                or summary.get("execution_authority") is not False
                or summary.get("selection_is_retrospective") is not True
                or summary.get("cost_bps") != 5.0):
            raise ValueError("Saved policy differs from configured slot selection")
        days = summary["days"]
        expected = _daily(_rows(folder / "portfolio_trades.csv"), days, set(contract["enabled_setup_ids"]))
        actual = _rows(folder / "daily_results.csv")
        if len(expected) != len(actual):
            raise ValueError("Daily calendar differs from ledger")
        for wanted, saved in zip(expected, actual):
            if wanted["day"] != saved["day"] or any(
                not math.isclose(wanted[key], _number(saved, key), abs_tol=1e-5, rel_tol=1e-9)
                for key in wanted if key != "day"
            ):
                raise ValueError("Daily results do not reconcile with trades")
        totals = _totals(expected)
        if any(summary.get(key) != value for key, value in totals.items()):
            raise ValueError("Summary does not reconcile with trades")
        if (summary.get("sessions") != len(days) or summary.get("first_date") != days[0]
                or summary.get("last_date") != days[-1]
                or summary.get("active_setup_count") != len(contract["enabled_setup_ids"])):
            raise ValueError("Summary scope differs from saved calendar or slots")
        identifier = hashlib.sha256((run.relative_to(root).as_posix() + "/" + strategy).encode()).hexdigest()[:16]
        metadata = dict(
            id="slot70-" + identifier, label=f"{label}: >=70% cutoff on all slots",
            strategy=public_name, run_name=run.name,
            source_label=f"{label} verified selected-slot full portfolio replay",
            kind="backtest", role="backtest_history", verified=True, integrity_verified=True,
            current_full_strategy=True, current_slot_selection=True,
            strategy_matches_active=False, strategy_matches_backtest=True,
            strategy_version=strategy + "-SLOT-WIN70-20261028",
            strategy_fingerprint=contract["policy_fingerprint"],
            slot_selection_fingerprint=contract["policy_fingerprint"],
            enabled_setup_ids=contract["enabled_setup_ids"],
            active_setup_count=len(contract["enabled_setup_ids"]),
            selection_threshold_pct=70, selection_is_retrospective=True,
            selection_scope="ALL_REPRESENTED_SLOTS", unchanged_setup_count=0,
            reviewed_setup_ids=contract["policy"].get("reviewed_setup_ids", []),
            scope="BACKTEST_ONLY", execution_authority=False,
            period_start=days[0], period_end=days[-1], sessions=len(days),
            cost_bps=5.0, _run_dir=str(folder), _ledger="portfolio_trades.csv",
        )
        packages.append((metadata, folder / "daily_results.csv"))
    return packages


def discover(data_root):
    """Reject incomplete, changed, out-of-root or stale-policy evidence."""
    root = Path(data_root) / ROOT_NAME
    try:
        pointer_path = root / "latest/current.json"
        if not _inside(pointer_path, root):
            return []
        pointer = _json(pointer_path)
        relative = Path(pointer["run_dir"])
        if (relative.is_absolute() or ".." in relative.parts or len(relative.parts) != 3
                or relative.parts[0] != "runs"):
            return []
        return _read_verified(root / relative, root, pointer)
    except (OSError, ValueError, KeyError, TypeError, AttributeError, ImportError):
        return []


def publish_replay(run_path, books, days, data_root=Path(r"C:/TradingData/eqidv2")):
    """Publish completed replay books; no runtime service or order is started."""
    source = Path(run_path).resolve()
    if not source.is_dir() or set(books) != set(STRATEGIES):
        raise ValueError("A completed replay and both strategy books are required")
    root = Path(data_root) / ROOT_NAME
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%f")
    run = root / "runs" / date.today().isoformat() / f"{stamp}_{uuid4().hex[:8]}_slot_win70"
    run.mkdir(parents=True, exist_ok=False)
    for strategy in STRATEGIES:
        folder = run / strategy
        folder.mkdir()
        contract = _contract(strategy)
        ledger = folder / "portfolio_trades.csv"
        books[strategy].to_csv(ledger, index=False)
        daily = _daily(_rows(ledger), list(days), set(contract["enabled_setup_ids"]))
        _write_csv(folder / "daily_results.csv", daily)
        _write_json(folder / "config.json", dict(schema_version=SCHEMA, strategy=strategy, **contract))
        _write_json(folder / "summary.json", dict(
            schema_version=SCHEMA, strategy=strategy, **_totals(daily),
            days=list(days), sessions=len(days), first_date=days[0], last_date=days[-1],
            active_setup_count=len(contract["enabled_setup_ids"]),
            policy_fingerprint=contract["policy_fingerprint"],
            selection_is_retrospective=True, execution_authority=False, cost_bps=5.0,
        ))
    _write_json(run / "source_replay.json", {
        "path": str(source), "notice": NOTICE,
        "files": {path.name: _sha(path) for path in sorted(source.iterdir()) if path.is_file()},
    })
    _write_json(run / "manifest.json", {
        "schema_version": SCHEMA,
        "files": {path.relative_to(run).as_posix(): _sha(path)
                  for path in sorted(run.rglob("*")) if path.is_file()},
    })
    pointer = dict(schema_version=SCHEMA, execution_authority=False,
                   run_dir=run.relative_to(root).as_posix(), manifest_sha256=_sha(run / "manifest.json"))
    _read_verified(run, root, pointer)
    temporary = root / "latest" / f".current_{uuid4().hex}.json"
    _write_json(temporary, pointer)
    temporary.replace(root / "latest/current.json")
    return {"run_dir": str(run), "pointer": str(root / "latest/current.json"),
            "manifest_sha256": pointer["manifest_sha256"]}
