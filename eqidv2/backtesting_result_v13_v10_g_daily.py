"""Publish only retained V13-V10-G backtesting for one explicit IST session."""
from __future__ import annotations

import argparse
import csv
import json
import math
import os
import tempfile
import threading
import time
from contextlib import contextmanager
from datetime import date, datetime, time as day_time
from pathlib import Path
from typing import Any

import fno_oi_common as common
import fno_v13_v10_g_live_config as config
from eqidv2_runtime_paths import runtime_dir

SESSION = "backtesting_result_v13_v10_g"
TITLE = "Backtesting result v13-v10-G"
STRATEGY = "V13-V10-G"
DEFAULT_OUTPUT_ROOT = runtime_dir(SESSION)
_OBSERVABILITY_RUNTIME: Any | None = None
_OBSERVABILITY_INITIALIZED = False


def _observability_runtime() -> Any | None:
    global _OBSERVABILITY_RUNTIME, _OBSERVABILITY_INITIALIZED
    configured = os.getenv("EQIDV2_OBSERVABILITY_ENABLED", "").strip().lower()
    enabled = (
        configured in {"1", "true", "yes", "on"}
        if configured
        else bool(os.getenv("EQIDV2_OBS_RUN_ID", "").strip())
    )
    if not enabled or _OBSERVABILITY_INITIALIZED:
        return _OBSERVABILITY_RUNTIME
    _OBSERVABILITY_INITIALIZED = True
    try:
        from ai_platform.observability.runtime import create_observability

        root = runtime_dir("observability")
        _OBSERVABILITY_RUNTIME = create_observability(
            "v13-v10-g-daily-replay",
            log_path=root / "logs" / "v13-v10-g-daily-replay.jsonl",
            journal_path=root / "journals" / "v13-v10-g-daily-replay-events.jsonl",
        )
    except Exception:
        _OBSERVABILITY_RUNTIME = None
    return _OBSERVABILITY_RUNTIME


def _flush_observability_metrics(runtime: Any | None = None) -> None:
    observed = runtime or _observability_runtime()
    if observed is None:
        return
    try:
        path = runtime_dir("observability", "metrics") / f"{SESSION}_{os.getpid()}.prom"
        common.atomic_write_text(path, observed.metrics.render_prometheus())
    except Exception:
        try:
            observed.standard_metrics.telemetry_dropped_total.inc(
                component="textfile", reason="write_failure"
            )
        except Exception:
            pass


def _observed_call(
    runtime: Any | None,
    span_name: str,
    day: date,
    operation: Any,
) -> Any:
    """Run one authoritative operation exactly once despite telemetry faults."""

    if runtime is None:
        return operation()
    started = False
    completed = False
    value: Any = None
    business_error: Exception | None = None
    business_traceback = None
    try:
        with runtime.bind(
            profile="V13_V10_G",
            strategy_version=config.STRATEGY_VERSION,
            strategy_fingerprint=config.strategy_fingerprint(),
            mode="replay",
            session_date=day.isoformat(),
            run_id=common.PROCESS_RUN_ID,
            replay_id=common.PROCESS_RUN_ID,
        ):
            with runtime.span(
                span_name,
                attributes={"session_date": day.isoformat(), "strategy": STRATEGY},
            ):
                started = True
                try:
                    value = operation()
                except Exception as exc:
                    business_error = exc
                    business_traceback = exc.__traceback__
                    raise
                else:
                    completed = True
    except Exception as telemetry_or_business_error:
        if business_error is not None:
            raise business_error.with_traceback(business_traceback)
        try:
            runtime.standard_metrics.telemetry_dropped_total.inc(
                component="daily_replay_span",
                reason=type(telemetry_or_business_error).__name__,
            )
        except Exception:
            pass
        if completed:
            # An __exit__ failure happened after authoritative work completed.
            return value
        if not started:
            # Instrumentation failed before entering the business operation.
            return operation()
        raise
    if business_error is not None:
        # Preserve a business exception even if a defective telemetry context
        # manager incorrectly suppresses it from the with statement.
        raise business_error.with_traceback(business_traceback)
    return value


@contextmanager
def _running_heartbeat(day: date, interval: float = 30.):
    """Keep a long local replay responsive; stop before publishing its final state."""
    stop = threading.Event()

    def pulse():
        while not stop.wait(interval):
            try:
                common.publish_heartbeat(SESSION, "RUNNING", session_date=day.isoformat(),
                                         phase="VERIFYING_OR_REPLAYING_G")
            except OSError as exc:
                print(f"[{SESSION}] Heartbeat write failed: {exc}", flush=True)

    worker = threading.Thread(target=pulse, name="g-daily-heartbeat", daemon=True)
    worker.start()
    try:
        yield
    finally:
        stop.set()
        worker.join()


def _publish(day: date, root: Path, state: str, reason: str = "", result: dict | None = None) -> dict:
    payload = dict(session=SESSION, session_date=day.isoformat(), strategy=STRATEGY,
                   run_id=common.PROCESS_RUN_ID, replay_id=common.PROCESS_RUN_ID,
                   strategy_version=config.STRATEGY_VERSION, strategy_fingerprint=config.strategy_fingerprint(),
                   status=state, phase=state, updated_at_ist=common.now_ist().isoformat(), reason=reason)
    if result is not None:
        payload["result"] = result
    common.atomic_write_json(root / "status.json", payload)
    common.atomic_write_json(root / "latest" / "latest_backtesting_result_v13_v10_g.json", payload)
    metrics = (result or {}).get("metrics") or {}
    activity = {key: metrics[key] for key in ("fills", "win_rate_pct", "net_profit_rupees") if key in metrics}
    if "profit_factor" in metrics:
        activity["trade_pf"] = metrics["profit_factor"]
    common.publish_status(SESSION, state, session_date=day.isoformat(), strategy_version=config.STRATEGY_VERSION,
                          strategy_fingerprint=config.strategy_fingerprint(), phase=state, reason=reason, **activity)
    report = (render_report(result) if state == "SUCCESS" and result is not None else
              f"# {TITLE}\n\nSession date: **{day.isoformat()} (IST)**\n\nStrategy: **{STRATEGY} only**\n\n"
              f"Status: **{state}**\n\n{reason}\n\nNo completed backtest result is claimed for this session.\n")
    if state != "SUCCESS" and result:
        problems = result.get("coverage_problems") or result.get("coverage", {}).get("problems", [])
        if problems:
            summaries = []
            for item in problems:
                if isinstance(item, dict):
                    item = {key: value for key, value in item.items() if key != "timestamps"}
                summaries.append(f"- {_display(item)}")
            report += "\nCoverage problems (full timestamp details in source manifest)\n\n" + "\n".join(summaries) + "\n"
        report += "\n" + "\n".join(f"- {name}: `{path}`" for name, path in result.get("artifacts", {}).items()) + "\n"
    common.atomic_write_text(root / "latest" / "latest_backtesting_result_v13_v10_g.md", report)
    common.atomic_write_text(root / "reports" / day.isoformat() / "backtesting_result_v13_v10_g.md", report)
    try:
        runtime = _observability_runtime()
        if runtime is not None:
            with runtime.bind(
                profile="V13_V10_G",
                strategy_version=config.STRATEGY_VERSION,
                strategy_fingerprint=config.strategy_fingerprint(),
                mode="replay",
                session_date=day.isoformat(),
                run_id=common.PROCESS_RUN_ID,
                replay_id=common.PROCESS_RUN_ID,
            ):
                runtime.event(
                    "replay.status",
                    severity=(
                        "ERROR"
                        if state.startswith(("BLOCKED", "FAILED"))
                        else "INFO"
                    ),
                    state=state,
                    reason=reason,
                    metrics=(result or {}).get("metrics") or {},
                )
                runtime.standard_metrics.replay_due.set(
                    0 if state == "SUCCESS" else 1,
                    profile="v13-v10-g",
                    replay_kind="finalized",
                )
                if state == "SUCCESS":
                    runtime.standard_metrics.replay_success_timestamp_seconds.set(
                        time.time(),
                        profile="v13-v10-g",
                        replay_kind="finalized",
                    )
            _flush_observability_metrics(runtime)
    except Exception:
        pass
    print(f"[{SESSION}] {day} {state}: {reason}", flush=True)
    return payload


def wait_for_data(day: date, args: argparse.Namespace) -> bool:
    """Wait for the dated 15:45 producer; exact G source checks follow this gate."""
    from wait_for_data_backtesting_ready import _data_job_status
    deadline = time.monotonic() + args.timeout_sec
    while True:
        state, note = _data_job_status(day.isoformat())
        if state == "PASS":
            return True
        if state == "FAIL" or time.monotonic() >= deadline:
            _publish(day, args.output_root, "BLOCKED_DATA_NOT_READY", note)
            return False
        _publish(day, args.output_root, "WAITING_FOR_DATA", note)
        time.sleep(min(args.poll_sec, max(0., deadline - time.monotonic())))


def verify_data(day: date, root: Path) -> dict:
    """Generate a fresh dated FnO-only coarse proof in this session's own root."""
    import data_for_backtesting_verify as verifier
    verification_root = root / "verification"
    verification_root.mkdir(parents=True, exist_ok=True)
    directory = Path(tempfile.mkdtemp(prefix=f"{day}_", dir=verification_root))
    original = verifier.VERIFY_DIR
    try:
        verifier.VERIFY_DIR = directory
        code = verifier.run_verify(day.isoformat(), scope="fno")
    finally:
        verifier.VERIFY_DIR = original
    payload = json.loads((directory / f"data_verify_{day}.json").read_text(encoding="utf-8"))
    if (code != 0 or payload.get("overall_exit_code") != 0 or payload.get("overall_status") != "PASS"
            or payload.get("date") != day.isoformat() or payload.get("scope") != "fno"):
        raise ValueError("Fresh dated FnO data verification did not pass for the requested session")
    return payload


def run_replay(day: date, output: Path) -> dict:
    from fno_v13_v10_g_daily_replay import replay_day
    return replay_day(day, output)


def validate_result(result: dict, day: date) -> None:
    if result.get("strategy") != STRATEGY or result.get("session_date") != day.isoformat():
        raise ValueError("Replay strategy/date differs from the requested G session")
    if result.get("state") != "SUCCESS":
        return
    if (result.get("complete") is not True or result.get("strategy_version") != config.STRATEGY_VERSION
            or result.get("days") != [day.isoformat()]):
        raise ValueError("A successful G daily result must be complete, current, and restricted to the requested day")
    metrics = result.get("metrics")
    if not isinstance(metrics, dict) or not metrics or metrics.get("sessions") != 1:
        raise ValueError("A successful G daily result must contain exactly one session")
    for name, raw_path in result.get("artifacts", {}).items():
        path = Path(raw_path)
        if path.suffix.lower() != ".csv":
            continue
        with path.open(encoding="utf-8-sig", newline="") as handle:
            for row in csv.DictReader(handle):
                observed = row.get("day") or row.get("session_date")
                if observed and str(observed)[:10] != day.isoformat():
                    raise ValueError(f"Different session found in G artifact {name}")


def _display(value: Any) -> str:
    if value is None:
        return "N/A"
    if isinstance(value, float):
        return f"{value:,.2f}" if math.isfinite(value) else ("infinite" if value > 0 else "N/A")
    return str(value).replace("|", "/").replace("\n", " ")


def render_report(result: dict) -> str:
    from fno_v13_v10_g_policy import policy_for_day
    policy = policy_for_day(date.fromisoformat(result["session_date"]))
    selection = ("09:25 LONG: OI <=1.20%, 5m volume >=1.75x, confirmation body >=54%, EMA bypass; "
                 "original G selections retain priority. All other setups and targets are unchanged."
                 if policy["relaxed_0925_long"] else "Original retained G selection and exits for this historical session.")
    stops = ("Stop starts at 1.25% and tightens once to 1.00% after 120 minutes from entry."
             if policy["staged_stop"] else "Original per-setup fixed stops apply.")
    lines = [f"# {TITLE}", "", f"Session date: **{result['session_date']} (IST)**", "",
             f"Strategy: **{STRATEGY} only**", "", "Status: **SUCCESS**", "",
             selection, stops,
             "Allocation: Rs 1,00,000 per trade; modeled exposure: 5x; portfolio capital: Rs 10,00,000.",
             "Full exits, no partial exit or break-even rule; pending entry expiry: 10 minutes; square-off: 15:15 IST.",
             f"Modeled round-trip trading cost: {config.ROUND_TRIP_COST_BPS:g} bps.",
             "", "Metric | Value", "--- | ---:"]
    for key, value in result["metrics"].items():
        if not isinstance(value, (dict, list)):
            lines.append(f"{key.replace('_', ' ')} | {_display(value)}")
    excluded = result.get("coverage", {}).get("excluded_stocks", [])
    if excluded:
        lines += ["", "Excluded stocks", "",
                  "Stocks missing required futures OI bars were omitted from candidate generation and execution.", ""]
        lines.extend(f"- {row['symbol']}: {', '.join(row.get('reasons', []))}" for row in excluded)
    artifacts = result.get("artifacts", {})
    trade_path = next((Path(value) for name, value in artifacts.items()
                       if "portfolio_trades" in name or name == "trades"), None)
    if trade_path is not None and trade_path.is_file():
        with trade_path.open(encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle)
            rows = list(reader)
            truth = lambda value: str(value).strip().lower() in {"true", "1", "1.0"}
            rows = [row for row in rows if truth(row.get("portfolio_executed", row.get("filled", False)))
                    and truth(row.get("filled", True))]
            requested = ["tradingsymbol", "side", "setup_id", "confirmation_ts", "entry_time", "exit_time",
                         "entry_ts", "exit_ts", "entry_price", "exit_price", "v10_g_stop_pct", "v10_g_target_pct",
                         "native_stop_pct", "native_target_pct",
                         "initial_stop_pct", "active_stop_pct_at_exit", "relaxed_0925_added",
                         "portfolio_status", "status", "exit_reason", "portfolio_net_profit_rupees"]
            fields = [field for field in requested if field in (reader.fieldnames or [])]
        if fields:
            lines += ["", "Executed trade details", "", " | ".join(fields), " | ".join("---" for _ in fields)]
            lines.extend(" | ".join(_display(row.get(field, "")) for field in fields) for row in rows)
    lines += ["", "Artifacts", ""]
    lines.extend(f"- {name}: `{value}`" for name, value in artifacts.items())
    lines += ["", "Historical modeled execution; actual paper/broker fills can differ.", ""]
    return "\n".join(lines)


def run(args: argparse.Namespace) -> int:
    day = args.date or common.now_ist().date()
    root = Path(args.output_root)
    args.output_root = root
    now = common.now_ist()
    if not common.is_trading_day(day, common.load_holidays()):
        _publish(day, root, "SKIPPED_NON_TRADING_DAY", "No regular NSE session. The date is not moved to an earlier trading day.")
        return 0
    if day > now.date():
        _publish(day, root, "BLOCKED_FUTURE_DATE", "The requested session has not occurred.")
        return 2
    if day == now.date() and now.time().replace(tzinfo=None) < day_time(15, 30):
        _publish(day, root, "WAITING_FOR_SESSION_CLOSE", "A complete daily backtest requires the session to close.")
        return 2
    try:
        observability = _observability_runtime()
        config.validate_strategy()
        config.attest_selected_backtest()
        if args.wait_for_data and day == now.date() and not wait_for_data(day, args):
            return 2
        _publish(day, root, "RUNNING", "Verifying this session's FnO data before the G-only replay.")
        with _running_heartbeat(day):
            proof = _observed_call(
                observability,
                "replay.verify_data",
                day,
                lambda: verify_data(day, root),
            )
            output = root / "runs" / day.isoformat() / common.now_ist().strftime("%Y%m%dT%H%M%S%f")
            result = _observed_call(
                observability,
                "replay.execute",
                day,
                lambda: run_replay(day, output),
            )
        validate_result(result, day)
        result["data_verification"] = dict(date=proof["date"], scope=proof["scope"], status=proof["overall_status"])
        if result.get("state") != "SUCCESS":
            _publish(day, root, str(result.get("state") or "BLOCKED_INCOMPLETE_DATA"),
                     "G source or execution-path coverage is incomplete; see result coverage problems.", result)
            return 2
        _publish(day, root, "SUCCESS", "Only the requested day's retained G strategy was replayed.", result)
        return 0
    except Exception as exc:
        _publish(day, root, "BLOCKED_BACKTEST", f"{type(exc).__name__}: {exc}")
        return 2


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", type=date.fromisoformat, help="One IST session date; defaults to today without rollback.")
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--wait-for-data", action="store_true")
    parser.add_argument("--timeout-sec", type=int, default=5400)
    parser.add_argument("--poll-sec", type=float, default=15.)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.timeout_sec < 0 or not 0 < args.poll_sec <= 60:
        raise ValueError("Use a nonnegative timeout and a polling interval in (0, 60] seconds")
    return run(args)


if __name__ == "__main__":
    raise SystemExit(main())
