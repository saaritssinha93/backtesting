"""Publish independent G3 daily replay and verified usable-session history.

All outputs use new production/history trees. Existing research runs, frozen
ledgers and legacy publishers are read-only and never serve as execution input.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import date, datetime
import json
import os
from pathlib import Path
import time
from uuid import uuid4

import pandas as pd

import fno_v13_v10_g_3_contract as contract
import fno_v13_v10_g_3_daily_replay as replay

SESSION = "backtesting_result_v13_v10_g_3"
DEFAULT_OUTPUT_ROOT = replay.runtime_dir(SESSION)


def _run_id() -> str:
    return datetime.now(contract.IST).strftime("%Y%m%dT%H%M%S%f") + "_" + uuid4().hex[:8]


def _status(root: Path, payload: dict, *, history: bool = False) -> None:
    name = "history_status.json" if history else "status.json"
    stamp = datetime.now(contract.IST).isoformat()
    replay.atomic_json(root / name, {**payload, "status": payload.get("state"), "ts": stamp, "updated_at": stamp,
        "strategy": replay.STRATEGY, "strategy_version": contract.STRATEGY_VERSION,
        "strategy_fingerprint": contract.strategy_fingerprint(), "execution_authority": False})


@contextmanager
def _lock(root: Path):
    root.mkdir(parents=True, exist_ok=True)
    path = root / ".production_publisher.lock"
    try:
        fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    except FileExistsError as exc:
        raise RuntimeError(f"Another G3 publisher owns {path}; review a stale lock before retrying") from exc
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            json.dump(dict(pid=os.getpid(), created_at=datetime.now(contract.IST).isoformat()), stream)
        yield
    finally:
        path.unlink(missing_ok=True)


def _require_closed(day: date, now: datetime | None = None) -> None:
    replay.require_closed_session(day, now)


def render_report(result: dict) -> str:
    metrics = result.get("metrics") or {}
    lines = [f"# V13-V10-G-3 backtesting: {result.get('session_date')}", "",
        f"State: {result['state']}", f"Role: {result['role']}",
        f"Strategy version: {result['strategy_version']}",
        f"Strategy fingerprint: {result['strategy_fingerprint']}", "",
        f"Sessions: {metrics.get('sessions', 0)}", f"Selected orders: {metrics.get('selected_orders', 0)}",
        f"Trades: {metrics.get('trades', 0)}", f"Net P&L (rupees): {metrics.get('net_pnl_rupees', metrics.get('net_profit_rupees', 'unavailable'))}", "",
        "This is an after-close simulation with no broker execution authority."]
    if not result.get("complete"):
        lines += ["", "Incomplete market inputs: no performance result is published."]
    excluded = result.get("coverage", {}).get("excluded_sessions", [])
    if excluded:
        lines += ["", "Excluded incomplete sessions: " + ", ".join(row["day"] for row in excluded)]
    return "\n".join(lines) + "\n"


def _validate_package(root: Path, directory: Path, manifest: dict, *, role: str | None = None,
                      require_complete: bool = False) -> None:
    role = role or manifest.get("role")
    if (manifest.get("schema_version") != replay.SCHEMA_VERSION or manifest.get("strategy") != replay.STRATEGY
            or manifest.get("strategy_version") != contract.STRATEGY_VERSION
            or manifest.get("strategy_fingerprint") != contract.strategy_fingerprint()
            or manifest.get("execution_authority") is not False or manifest.get("role") != role
            or role not in ("production_daily", "cumulative_history")):
        raise ValueError("Package schema, role, authority, or strategy identity is invalid")
    days = manifest.get("days", [])
    if not days or days != sorted(set(days)) or manifest.get("session_date") != days[-1]:
        raise ValueError("Package session identity is invalid")
    for day in days:
        date.fromisoformat(day)
    expected = root.resolve() / ("production" if role == "production_daily" else "history") / "runs"
    directory = directory.resolve()
    if not directory.is_relative_to(expected) or len(directory.relative_to(expected).parts) != 2:
        raise ValueError("Package is outside its canonical production/history run tree")
    folder_date = (manifest.get("coverage", {}).get("requested_through", days[-1])
                   if role == "cumulative_history" else days[-1])
    if directory.parent.name != folder_date or directory.name != manifest.get("run_id"):
        raise ValueError("Package path and run/session identity disagree")
    if role == "production_daily" and len(days) != 1:
        raise ValueError("Daily package contains multiple sessions")
    if manifest.get("complete") is True:
        allowed = {"SUCCESS"} if role == "production_daily" else {"SUCCESS", "SUCCESS_WITH_EXCLUDED_SESSIONS"}
        if manifest.get("state") not in allowed or not {"portfolio_trades", "daily_results"}.issubset(manifest.get("artifacts", {})):
            raise ValueError("Complete package lacks successful canonical outputs")
        if manifest["state"] == "SUCCESS_WITH_EXCLUDED_SESSIONS" and not manifest.get("coverage", {}).get("excluded_sessions"):
            raise ValueError("Excluded-session state requires explicit gap evidence")
    elif require_complete or manifest.get("state") != "BLOCKED_INCOMPLETE_DATA":
        raise ValueError("Package is not a valid complete production result")


def publish_result(root: Path, run_dir: Path, result: dict, *, history: bool = False) -> dict:
    """Switch a current pointer only after a complete hash-sealed generation."""
    root, run_dir = Path(root).resolve(), Path(run_dir).resolve()
    if not run_dir.is_relative_to(root):
        raise ValueError("Production run must be inside its publication root")
    manifest = run_dir / "run_manifest.json"
    sealed = json.loads(manifest.read_text(encoding="utf-8"))
    _validate_package(root, run_dir, sealed, role="cumulative_history" if history else "production_daily")
    if sealed != replay.json_ready(result):
        raise ValueError("Publication result differs from its sealed manifest")
    for value in sealed["artifacts"].values():
        path = (run_dir / value["path"]).resolve()
        if not path.is_relative_to(run_dir) or replay.sha256(path) != value["sha256"]:
            raise ValueError("Production artifact failed its manifest hash")
    latest = root / "latest"
    pointer_name = "current_history.json" if history else "current_run.json"
    summary = {**result, "run_dir": str(run_dir), "run_manifest": str(manifest),
               "manifest_sha256": replay.sha256(manifest)}
    report_dir = root / "reports" / str(result["session_date"]) / run_dir.name
    report_dir.mkdir(parents=True, exist_ok=True)
    replay.source.common.atomic_write_text(report_dir / "report.md", render_report(result))
    replay.atomic_json(report_dir / "report.json", summary)
    summary["report_markdown"] = str(report_dir / "report.md")
    summary["report_json"] = str(report_dir / "report.json")
    if result["complete"]:
        pointer = dict(run_dir=run_dir.relative_to(root).as_posix(), manifest_sha256=replay.sha256(manifest),
            role=result["role"], strategy_version=result["strategy_version"],
            strategy_fingerprint=result["strategy_fingerprint"], session_date=result["session_date"])
        old_path = latest / pointer_name
        old = json.loads(old_path.read_text(encoding="utf-8")) if old_path.exists() else {}
        if str(old.get("session_date", "")) <= str(result["session_date"]):
            # The pointer is the commit record. Readers verify the manifest and
            # artifact hashes; mutable presentation summaries are not authority.
            suffix = "history" if history else SESSION
            replay.atomic_json(latest / f"latest_{suffix}.json", summary)
            replay.source.common.atomic_write_text(latest / f"latest_{suffix}.md", render_report(result))
            replay.atomic_json(old_path, pointer)
    _status(root, {"state": result["state"], "complete": result["complete"],
        "session_date": result["session_date"], "run_dir": str(run_dir),
        "manifest_sha256": replay.sha256(manifest), "metrics": result.get("metrics"),
        "coverage": result.get("coverage", {})}, history=history)
    return summary


def _read_verified_pointer(root: Path, name: str, role: str) -> tuple[Path, dict] | None:
    pointer_path = root / "latest" / name
    if not pointer_path.is_file():
        return None
    pointer = json.loads(pointer_path.read_text(encoding="utf-8"))
    directory = (root / pointer["run_dir"]).resolve()
    manifest_path = directory / "run_manifest.json"
    if not directory.is_relative_to(root.resolve()) or replay.sha256(manifest_path) != pointer.get("manifest_sha256"):
        raise ValueError("Current history pointer/manifest integrity failed")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    _validate_package(root, directory, manifest, role=role, require_complete=True)
    if (not manifest.get("complete") or manifest.get("role") != role or pointer.get("role") != role
            or manifest.get("strategy_version") != contract.STRATEGY_VERSION
            or manifest.get("strategy_fingerprint") != contract.strategy_fingerprint()
            or pointer.get("strategy_version") != manifest["strategy_version"]
            or pointer.get("strategy_fingerprint") != manifest["strategy_fingerprint"]
            or pointer.get("session_date") != manifest["session_date"]):
        raise ValueError("Current history does not match this production contract")
    for value in manifest["artifacts"].values():
        path = (directory / value["path"]).resolve()
        if not path.is_relative_to(directory) or replay.sha256(path) != value["sha256"]:
            raise ValueError("Current history artifact integrity failed")
    return directory, manifest


HISTORICAL_ELIGIBILITY_POLICY = (
    "RAW_AFTER_CLOSE: any missing required futures-OI clock excludes the entire stock-day; "
    "other required equity/source gaps block the session. This is retrospective data eligibility, "
    "not a morning/live causal entry gate. SEALED_HISTORY: original hash-verified contract/session "
    "eligibility plus independently checked complete pre-quota candidate pool."
)


def _session_coverage(root: Path, directory: Path, manifest: dict) -> list[dict]:
    """Keep raw stock-day exclusions and sealed historical eligibility distinct."""
    existing = manifest.get("coverage", {}).get("session_coverage")
    if existing is not None:
        return list(existing)
    if manifest["role"] == "production_daily":
        quality = manifest.get("coverage", {})
        record = manifest["artifacts"].get("coverage")
        return [dict(day=manifest["days"][0], method="RAW_AFTER_CLOSE_STOCK_DAY_ELIGIBILITY",
            universe_count=quality.get("universe_count"), included_stocks=quality.get("included_stocks"),
            excluded_stocks=quality.get("excluded_stocks", []), problems=quality.get("problems", []),
            coverage_artifact={"path": str(directory / record["path"]), "sha256": record["sha256"]} if record else None,
            run_manifest={"path": str(directory / "run_manifest.json"), "sha256": replay.sha256(directory / "run_manifest.json")})]
    record = manifest["artifacts"]["source_manifest"]
    source_path = (directory / record["path"]).resolve()
    if not source_path.is_relative_to(directory.resolve()) or replay.sha256(source_path) != record["sha256"]:
        raise ValueError("History source provenance integrity failed")
    provenance = json.loads(source_path.read_text(encoding="utf-8"))
    if manifest["source_kind"] == "HASH_VERIFIED_SEALED_PRE_SELECTION_FEATURES":
        eligibility = next(item for item in provenance["verified_artifacts"]
                           if Path(item["path"]).name == "source_session_eligibility.csv")
        path = Path(eligibility["path"])
        if replay.sha256(path) != eligibility["sha256"]:
            raise ValueError("Sealed eligibility source changed")
        rows = pd.read_csv(path)
        rows = rows.loc[rows.day.astype(str).isin(manifest["days"])]
        return [dict(day=str(row["day"]), method="SEALED_CONTRACT_SESSION_ELIGIBILITY_AND_COMPLETE_CANDIDATE_POOL",
            universe_count=row.get("universe_size"), contracts_with_data=row.get("contracts_with_data"),
            contract_coverage=row.get("coverage"), eligibility_reason=row.get("reason"),
            stock_day_full_clock_check="NOT_RECONSTRUCTED_BY_THIS_HISTORICAL_METHOD",
            source_eligibility_artifact=eligibility) for row in rows.to_dict("records")]
    members = provenance.get("members")
    if members is None:
        members = [provenance[key] for key in ("previous_history", "daily_member") if key in provenance]
    if not members:
        raise ValueError("History lacks member coverage provenance")
    by_day = {}
    for member in members:
        child_dir = Path(member["run_dir"]).resolve()
        path = child_dir / "run_manifest.json"
        if not child_dir.is_relative_to(root.resolve()) or replay.sha256(path) != member["manifest_sha256"]:
            raise ValueError("History coverage member integrity failed")
        child = json.loads(path.read_text(encoding="utf-8"))
        for row in _session_coverage(root, child_dir, child):
            by_day[row["day"]] = row
    return [by_day[day] for day in sorted(by_day)]


def refresh_history(root: Path, daily_dir: Path, daily: dict) -> dict | None:
    """Replace exactly one day in a verified history; caller holds publisher lock.

    Canonical ledgers and feature/selection evidence travel together. A valid
    zero-trade day remains in the denominator, and a blocked day never enters.
    Older generations and their source chain remain immutable.
    """
    root, daily_dir = Path(root).resolve(), Path(daily_dir).resolve()
    if not daily.get("complete"):
        return None
    if daily.get("role") != "production_daily" or len(daily.get("days", [])) != 1:
        raise ValueError("History refresh needs exactly one complete production day")
    previous = _read_verified_pointer(root, "current_history.json", "cumulative_history")
    if previous is None:
        return None
    previous_dir, history = previous
    day = date.fromisoformat(daily["days"][0])
    if (daily.get("strategy_version") != contract.STRATEGY_VERSION
            or daily.get("strategy_fingerprint") != contract.strategy_fingerprint()):
        raise ValueError("Daily replay contract does not match current history")
    sealed_daily = json.loads((daily_dir / "run_manifest.json").read_text(encoding="utf-8"))
    _validate_package(root, daily_dir, sealed_daily, role="production_daily", require_complete=True)
    if sealed_daily != replay.json_ready(daily):
        raise ValueError("Daily history member differs from its manifest")
    for value in daily["artifacts"].values():
        path = (daily_dir / value["path"]).resolve()
        if not path.is_relative_to(daily_dir) or replay.sha256(path) != value["sha256"]:
            raise ValueError("Daily history member failed artifact verification")
    def replaced(name):
        pieces, columns = [], []
        for directory, manifest, old in ((previous_dir, history, True), (daily_dir, daily, False)):
            item = manifest["artifacts"].get(name)
            if item is None:
                if name == "portfolio_trades":
                    raise ValueError("History member has no canonical portfolio ledger")
                continue
            frame = pd.read_csv(directory / item["path"])
            columns = list(dict.fromkeys([*columns, *frame.columns]))
            if "day" not in frame:
                raise ValueError(f"History artifact has no session identity: {name}")
            dates = pd.to_datetime(frame.day, errors="raise").dt.date
            if old:
                frame = frame.loc[dates.ne(day)].copy()
            elif len(frame) and not dates.eq(day).all():
                raise ValueError("Daily artifact contains another session")
            if len(frame):
                pieces.append(frame)
        return pd.concat(pieces, ignore_index=True, sort=False) if pieces else pd.DataFrame(columns=columns or ["day"])
    ledger = replaced("portfolio_trades")
    if len(ledger) and ledger.signal_id.duplicated().any():
        raise ValueError("Duplicate canonical identity in incremental history")
    days = sorted({date.fromisoformat(value) for value in history["days"]} | {day})
    coverage = dict(history.get("coverage", {}))
    replaces_existing = str(day) in history["days"]
    session_coverage = {row["day"]: row for row in _session_coverage(root, previous_dir, history)}
    session_coverage.update({row["day"]: row for row in _session_coverage(root, daily_dir, daily)})
    coverage.update(eligible_days=[str(value) for value in days], requested_through=str(days[-1]),
        incremental_update=True, replacement_day=str(day), complete_usable_session_aggregate=True,
        all_available_dated_universes_attempted=bool(replaces_existing and coverage.get("all_available_dated_universes_attempted")),
        excluded_sessions=[row for row in coverage.get("excluded_sessions", []) if row["day"] != str(day)],
        session_coverage=[session_coverage[key] for key in sorted(session_coverage)],
        historical_eligibility_policy=HISTORICAL_ELIGIBILITY_POLICY)
    coverage["requested_calendar_coverage_complete"] = bool(
        replaces_existing and coverage.get("requested_calendar_coverage_complete") and not coverage["excluded_sessions"])
    run_dir = root / "history" / "runs" / str(days[-1]) / _run_id()
    result = replay._finish(run_dir, days=days, role="cumulative_history",
        source_kind="INCREMENTAL_VERIFIED_G3_PRODUCTION_HISTORY",
        frames={name: replaced(name) for name in ("candidate_signals", "selected_orders", "selection_audit", "feature_ledger")},
        provenance=dict(source_kind="INCREMENTAL_VERIFIED_G3_PRODUCTION_HISTORY", completed_legacy_trade_outputs_consumed=False,
            previous_history=dict(run_dir=str(previous_dir), manifest_sha256=replay.sha256(previous_dir / "run_manifest.json")),
            daily_member=dict(run_dir=str(daily_dir), manifest_sha256=replay.sha256(daily_dir / "run_manifest.json")),
            replaced_day=str(day)), coverage=coverage, complete=True, ledger=ledger)
    return publish_result(root, run_dir, result, history=True)


def run_daily(day: date, output_root: Path = DEFAULT_OUTPUT_ROOT, *, roots=None) -> dict:
    root = Path(output_root).resolve()
    _require_closed(day)
    replay.verify_production_contract()
    with _lock(root):
        run_dir = root / "production" / "runs" / day.isoformat() / _run_id()
        _status(root, dict(state="RUNNING", complete=False, session_date=str(day), run_dir=str(run_dir)))
        try:
            result = replay.replay_day(day, run_dir, roots=roots)
            summary = publish_result(root, run_dir, result)
            history = refresh_history(root, run_dir, result)
            if history is not None:
                summary["history_refresh"] = {"state": history["state"], "run_dir": history["run_dir"],
                                               "manifest_sha256": history["manifest_sha256"]}
            return summary
        except Exception as exc:
            _status(root, dict(state="FAILED", complete=False, session_date=str(day), run_dir=str(run_dir),
                               error=f"{type(exc).__name__}: {exc}"))
            raise


def _verified_daily(root: Path, day: date) -> tuple[Path, dict] | None:
    """Reuse only this production engine's immutable matching generation."""
    base = root / "production" / "runs" / day.isoformat()
    for path in sorted(base.glob("*/run_manifest.json"), reverse=True):
        item = json.loads(path.read_text(encoding="utf-8"))
        try:
            _validate_package(root, path.parent, item, role="production_daily", require_complete=True)
        except (KeyError, TypeError, ValueError):
            continue
        if (item.get("complete") and item.get("role") == "production_daily"
                and item.get("strategy_version") == contract.STRATEGY_VERSION
                and item.get("strategy_fingerprint") == contract.strategy_fingerprint()
                and item.get("days") == [str(day)]):
            valid = True
            for value in item["artifacts"].values():
                target = (path.parent / value["path"]).resolve()
                if not target.is_relative_to(path.parent.resolve()) or not target.is_file() or replay.sha256(target) != value["sha256"]:
                    valid = False
                    break
            if valid:
                provenance_path = path.parent / item["artifacts"]["source_manifest"]["path"]
                provenance = json.loads(provenance_path.read_text(encoding="utf-8"))
                expected_code = replay._code_provenance()
                observed_code = {str(Path(row["path"]).resolve()): row["sha256"] for row in provenance.get("code", [])}
                valid = all(observed_code.get(row["path"]) == row["sha256"] for row in expected_code)
            if valid:
                return path.parent, item
    return None


def _history_raw_worker(day: date, child_dir: Path, roots: replay.DataRoots) -> dict:
    return replay.replay_day(day, child_dir, roots=roots)


def run_history(through: date, output_root: Path = DEFAULT_OUTPUT_ROOT, *, roots=None,
                sealed_source: Path = replay.DEFAULT_SEALED_SOURCE, jobs: int = 2) -> dict:
    """Recompute sealed sessions and every other available dated-universe day.

    A blocked day's manifest is retained, but it contributes neither zero
    trades nor a session denominator. Current-history reports list all gaps.
    """
    root = Path(output_root).resolve()
    roots = roots or replay.DataRoots()
    replay.verify_production_contract()
    if jobs not in (1, 2, 3):
        raise ValueError("History workers must be 1, 2, or 3")
    _require_closed(through)
    with _lock(root):
        run_dir = root / "history" / "runs" / through.isoformat() / _run_id()
        _status(root, dict(state="RUNNING", complete=False, session_date=str(through), run_dir=str(run_dir)), history=True)
        try:
            bundle = replay.load_sealed_source(sealed_source)
            sealed_days = sorted(day for day in bundle["days"] if day <= through)
            if not sealed_days:
                raise ValueError("No eligible sealed sessions through requested date")
            sealed_dir = run_dir / "sealed_input_replay"
            sealed = replay.replay_sealed_days(bundle, sealed_days, sealed_dir)
            members = [(sealed_dir, sealed)]
            excluded = []
            available = set()
            for path in roots.universe.glob("near_month_*.parquet"):
                try:
                    day = date.fromisoformat(path.stem.removeprefix("near_month_"))
                except ValueError:
                    continue
                if sealed_days[0] <= day <= through:
                    available.add(day)
            pending = []
            def retain(day, child_dir, child):
                if child["complete"]:
                    members.append((child_dir, child))
                else:
                    excluded.append(dict(day=str(day), state=child["state"], run_dir=str(child_dir),
                        manifest_sha256=replay.sha256(child_dir / "run_manifest.json"),
                        blocking_problems=child["coverage"]["blocking_problems"]))
                _status(root, dict(state="RUNNING", complete=False, session_date=str(through),
                    last_completed_day=str(day), run_dir=str(run_dir), excluded_sessions=excluded), history=True)
            for day in sorted(available - set(sealed_days)):
                reused = _verified_daily(root, day)
                if reused:
                    child_dir, child = reused
                    print(f"[G3 history] verified existing production replay {day}", flush=True)
                    retain(day, child_dir, child)
                else:
                    child_dir = root / "production" / "runs" / str(day) / _run_id()
                    print(f"[G3 history] reconstructing raw inputs {day}", flush=True)
                    pending.append((day, child_dir))
            if jobs == 1:
                for day, child_dir in pending:
                    retain(day, child_dir, _history_raw_worker(day, child_dir, roots))
            elif pending:
                with ProcessPoolExecutor(max_workers=jobs) as pool:
                    futures = {pool.submit(_history_raw_worker, day, child_dir, roots): (day, child_dir)
                               for day, child_dir in pending}
                    for future in as_completed(futures):
                        day, child_dir = futures[future]
                        retain(day, child_dir, future.result())
            members.sort(key=lambda member: member[1]["days"][0])
            for row in bundle["eligibility"].to_dict("records"):
                day = date.fromisoformat(str(row["day"]))
                if sealed_days[0] <= day <= through and str(row["eligible"]).lower() != "true" and day not in available:
                    excluded.append(dict(day=str(day), state="BLOCKED_SOURCE_INELIGIBLE",
                        source="VERIFIED_SEALED_SOURCE_ELIGIBILITY", blocking_problems=[dict(reason=row.get("reason"))]))
            excluded.sort(key=lambda row: row["day"])
            def combine(name):
                frames = []
                columns = []
                for directory, item in members:
                    record = item["artifacts"].get(name)
                    if record:
                        frame = pd.read_csv(directory / record["path"])
                        columns = list(dict.fromkeys([*columns, *frame.columns]))
                        if len(frame):
                            frame["source_run_dir"] = str(directory)
                            frames.append(frame)
                return pd.concat(frames, ignore_index=True, sort=False) if frames else pd.DataFrame(columns=columns or ["day"])
            ledger = combine("portfolio_trades")
            days = sorted({date.fromisoformat(day) for _, item in members for day in item["days"]})
            if len(ledger) and ledger.signal_id.duplicated().any():
                raise ValueError("Duplicate canonical signal identity in history members")
            provenance = dict(source_kind="INDEPENDENT_G3_SEALED_AND_RAW_REPLAY", through_date=str(through),
                completed_legacy_trade_outputs_consumed=False,
                members=[dict(run_dir=str(directory), manifest_sha256=replay.sha256(directory / "run_manifest.json"),
                              days=item["days"], source_kind=item["source_kind"]) for directory, item in members],
                sealed_source_eligibility=bundle["eligibility"].to_dict("records"))
            result = replay._finish(run_dir, days=days, role="cumulative_history", source_kind=provenance["source_kind"],
                frames={name: combine(name) for name in ("candidate_signals", "selected_orders", "selection_audit", "feature_ledger")},
                provenance=provenance, coverage=dict(eligible_days=[str(d) for d in days],
                    requested_through=str(through), excluded_sessions=excluded,
                    all_available_dated_universes_attempted=True,
                    blocking_problems=[], complete_usable_session_aggregate=True,
                    requested_calendar_coverage_complete=not excluded,
                    session_coverage=[row for directory, item in members for row in _session_coverage(root, directory, item)],
                    historical_eligibility_policy=HISTORICAL_ELIGIBILITY_POLICY), complete=True, ledger=ledger)
            return publish_result(root, run_dir, result, history=True)
        except Exception as exc:
            _status(root, dict(state="FAILED", complete=False, session_date=str(through), run_dir=str(run_dir),
                error=f"{type(exc).__name__}: {exc}"), history=True)
            raise


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--date", type=date.fromisoformat)
    group.add_argument("--history-through", type=date.fromisoformat)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--sealed-source", type=Path, default=replay.DEFAULT_SEALED_SOURCE)
    parser.add_argument("--jobs", type=int, choices=(1, 2, 3), default=2)
    parser.add_argument("--wait-for-data", action="store_true")
    parser.add_argument("--wait-minutes", type=float, default=30.)
    parser.add_argument("--poll-seconds", type=float, default=30.)
    args = parser.parse_args(argv)
    if args.wait_minutes < 0 or args.poll_seconds <= 0:
        parser.error("Wait duration must be nonnegative; poll seconds must be positive")
    end = time.monotonic() + args.wait_minutes * 60
    while True:
        result = (run_history(args.history_through, args.output_root, sealed_source=args.sealed_source, jobs=args.jobs)
                  if args.history_through else run_daily(args.date, args.output_root))
        print(json.dumps(replay.json_ready({key: result.get(key) for key in
            ("state", "complete", "session_date", "run_dir", "run_manifest", "metrics", "coverage")}), indent=2))
        if result["complete"] or not args.wait_for_data or time.monotonic() >= end:
            return 0 if result["complete"] else 2
        time.sleep(min(args.poll_seconds, max(0., end - time.monotonic())))


if __name__ == "__main__":
    raise SystemExit(main())
