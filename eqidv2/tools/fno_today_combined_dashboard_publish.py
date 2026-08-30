"""Run and publish the frozen 2026-08-31 five-strategy FnO dashboard report.

This is intentionally a one-shot publisher.  The underlying replay is bound to
the frozen 2026-08-31 candidate frames, so this module must not be used as a
generic daily scheduled replacement for ``backtesting_result_v11_daily.py``.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable


try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except (AttributeError, OSError):
    pass


BASE_DIR = Path(__file__).resolve().parents[1]
REPLAY_SCRIPT = BASE_DIR / "tools" / "fno_today_current_refresh_replay.py"
SESSION_DATE = "2026-08-31"
EXPECTED_CUTOFF = "2026-08-31T15:15:00+05:30"
EXPECTED_STRATEGIES = (
    "V6_CONTROL",
    "V8_COMBINED",
    "V10_STAGE7_0935_LONG_MAX_050_GAP2",
    "V11_STAGE10_FROZEN",
    "V12_SELECTED",
)
EXPECTED_SYMBOLS = ("MCX", "KAYNES", "HINDPETRO", "PRESTIGE", "KFINTECH")
FRIENDLY_NAMES = {
    "V6_CONTROL": "V6 Control",
    "V8_COMBINED": "V8 Combined",
    "V10_STAGE7_0935_LONG_MAX_050_GAP2": "V10 .50 + Gap2",
    "V11_STAGE10_FROZEN": "V11 Stage 10 Frozen",
    "V12_SELECTED": "V12 Selected",
}
STRATEGY_DIRS = {
    "V6_CONTROL": "v6_control",
    "V8_COMBINED": "v8_combined",
    "V10_STAGE7_0935_LONG_MAX_050_GAP2": "v10_stage7_0935_long_max_050_gap2",
    "V11_STAGE10_FROZEN": "v11_stage10_frozen",
    "V12_SELECTED": "v12_selected",
}
DEFAULT_OUTPUT_ROOT = Path(
    r"C:\TradingData\eqidv2\fno_oi\strategy_research"
    r"\dashboard_v6_v8_v10_v11_v12_20260831"
)
DEFAULT_DASHBOARD_ROOT = Path(
    os.environ.get("EQIDV2_RUNTIME_ROOT", r"C:\TradingData\eqidv2")
) / "backtesting_result_v11"
DEFAULT_LOG_DIR = BASE_DIR / "logs"


class PublicationError(RuntimeError):
    """Raised when a replay is unsafe to publish to the dashboard."""


def _now_ist_text() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")


def _atomic_write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp.{os.getpid()}")
    try:
        temporary.write_text(text, encoding="utf-8", newline="\n")
        os.replace(temporary, path)
    finally:
        try:
            temporary.unlink(missing_ok=True)
        except OSError:
            pass


def _atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    _atomic_write_text(path, json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n")


def _read_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise PublicationError(f"invalid JSON artifact: {path}: {exc}") from exc


def _read_csv(path: Path) -> list[dict[str, str]]:
    try:
        with path.open("r", encoding="utf-8-sig", newline="") as handle:
            return list(csv.DictReader(handle))
    except OSError as exc:
        raise PublicationError(f"cannot read CSV artifact: {path}: {exc}") from exc


def _normalise_timestamp(value: object) -> str:
    return str(value or "").strip().replace(" ", "T", 1)


def _float(value: object) -> float:
    try:
        return float(value)
    except (TypeError, ValueError) as exc:
        raise PublicationError(f"expected numeric value, received {value!r}") from exc


def _clock(value: object) -> str:
    text = str(value or "")
    return text[11:16] if len(text) >= 16 else text


def validate_run(
    run_root: Path,
    *,
    expected_date: str = SESSION_DATE,
) -> tuple[dict[str, Any], list[dict[str, str]], dict[str, list[dict[str, Any]]]]:
    """Validate the source-bound replay before any dashboard report is replaced."""

    required = ("manifest.json", "comparison.csv", "trade_contracts.json")
    missing = [name for name in required if not (run_root / name).is_file()]
    if missing:
        raise PublicationError(f"missing required artifacts in {run_root}: {', '.join(missing)}")

    manifest = _read_json(run_root / "manifest.json")
    comparison = _read_csv(run_root / "comparison.csv")
    contracts = _read_json(run_root / "trade_contracts.json")

    if manifest.get("complete") is not True:
        raise PublicationError("manifest complete is not true")
    if str(manifest.get("session_date")) != expected_date:
        raise PublicationError(
            f"manifest session date {manifest.get('session_date')!r} != {expected_date}"
        )
    if _normalise_timestamp(manifest.get("explicit_uniform_cutoff_ist")) != EXPECTED_CUTOFF:
        raise PublicationError("manifest does not use the expected uniform 15:15 IST cutoff")
    if str(manifest.get("schema_version")) != "fno_today_current_refresh_replay_v1":
        raise PublicationError("unexpected replay manifest schema")

    actual_strategies = tuple(row.get("strategy", "") for row in comparison)
    if len(comparison) != len(EXPECTED_STRATEGIES) or set(actual_strategies) != set(EXPECTED_STRATEGIES):
        raise PublicationError(
            f"strategy set mismatch: expected {EXPECTED_STRATEGIES}, received {actual_strategies}"
        )
    for row in comparison:
        if row.get("session_date") != expected_date:
            raise PublicationError(f"comparison row has wrong date: {row}")
        if _normalise_timestamp(row.get("explicit_uniform_cutoff_ist")) != EXPECTED_CUTOFF:
            raise PublicationError(f"comparison row has wrong cutoff: {row.get('strategy')}")
        for field in ("candidates", "fills", "wins", "losses", "win_rate_pct", "profit_factor", "net_return_points", "net_pnl_rs"):
            _float(row.get(field))

    if not isinstance(contracts, dict) or set(contracts) != set(EXPECTED_STRATEGIES):
        raise PublicationError("trade contract strategy set mismatch")
    for row in comparison:
        strategy = row["strategy"]
        strategy_contracts = contracts.get(strategy)
        if not isinstance(strategy_contracts, list):
            raise PublicationError(f"trade contracts are not a list for {strategy}")
        if len(strategy_contracts) != int(_float(row["fills"])):
            raise PublicationError(f"trade contract count does not match fills for {strategy}")

    evidence = manifest.get("source_evidence")
    if not isinstance(evidence, dict) or set(evidence) != set(EXPECTED_SYMBOLS):
        raise PublicationError("source evidence does not contain the five required symbols")
    for symbol in EXPECTED_SYMBOLS:
        item = evidence[symbol]
        if int(item.get("used_rows", 0)) != 360:
            raise PublicationError(f"{symbol} does not have 360 used one-minute rows")
        if _normalise_timestamp(item.get("used_max_ist")) != EXPECTED_CUTOFF:
            raise PublicationError(f"{symbol} does not reach the uniform cutoff")

    checks = manifest.get("checks_vs_1144")
    if not isinstance(checks, dict) or set(checks) != set(EXPECTED_STRATEGIES):
        raise PublicationError("causal identity checks are missing")
    for strategy in EXPECTED_STRATEGIES:
        check = checks[strategy]
        if check.get("same_fill_identity") is not True:
            raise PublicationError(f"fill identity changed for {strategy}")
        if check.get("confirmation_entry_stop_target_unchanged") is not True:
            raise PublicationError(f"entry contract changed for {strategy}")

    return manifest, comparison, contracts


def _audit_outcomes(run_root: Path, strategy: str) -> tuple[int, int]:
    path = run_root / "strategies" / STRATEGY_DIRS[strategy] / "candidate_order_audit.csv"
    if not path.is_file():
        return 0, 0
    rows = _read_csv(path)
    no_confirmation = sum(row.get("status") == "NO_CONFIRMATION" for row in rows)
    entry_expired = sum(row.get("status") == "WINDOW_EXPIRED" for row in rows)
    return no_confirmation, entry_expired


def render_report(
    run_root: Path,
    manifest: dict[str, Any],
    comparison: list[dict[str, str]],
    contracts: dict[str, list[dict[str, Any]]],
    *,
    published_at: str,
) -> str:
    by_strategy = {row["strategy"]: row for row in comparison}
    ranked = sorted(comparison, key=lambda row: _float(row["net_pnl_rs"]), reverse=True)
    winner = ranked[0]
    v6 = by_strategy["V6_CONTROL"]
    v8 = by_strategy["V8_COMBINED"]
    v12 = by_strategy["V12_SELECTED"]

    lines = [
        f"# Backtesting result v6/v8/v10/v11/v12 — {SESSION_DATE}",
        "",
        "**Session status:** COMPLETED AND VALIDATED",
        "",
        f"Published: `{published_at}`  ",
        f"Uniform execution cutoff: `{manifest['explicit_uniform_cutoff_ist']}`  ",
        "Mode: frozen causal 5-minute candidates with refreshed cash-equity 1-minute execution paths.",
        "",
        "## Strategy comparison",
        "",
        "| Rank | Strategy | Candidates | Fills | W/L | WR | PF | Net return points | Net P&L |",
        "|---:|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for rank, row in enumerate(ranked, start=1):
        candidates = row["candidates"]
        if row["strategy"] == "V12_SELECTED" and row.get("all_input_candidates"):
            candidates = f"{int(_float(row['candidates']))}/{int(_float(row['all_input_candidates']))}"
        lines.append(
            "| "
            + " | ".join(
                (
                    str(rank),
                    FRIENDLY_NAMES[row["strategy"]],
                    candidates,
                    str(int(_float(row["fills"]))),
                    f"{int(_float(row['wins']))}/{int(_float(row['losses']))}",
                    f"{_float(row['win_rate_pct']):.2f}%",
                    f"{_float(row['profit_factor']):.6f}",
                    f"{_float(row['net_return_points']):+.6f}",
                    f"₹{_float(row['net_pnl_rs']):+,.2f}",
                )
            )
            + " |"
        )

    lines.extend(
        [
            "",
            "## Filled trades",
            "",
            "| Strategy | Candidate | Confirm | Entry | SL | Target | Exit | Result |",
            "|---|---|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for strategy in EXPECTED_STRATEGIES:
        for trade in contracts[strategy]:
            lines.append(
                "| "
                + " | ".join(
                    (
                        FRIENDLY_NAMES[strategy],
                        str(trade.get("candidate_id", "")),
                        _clock(trade.get("confirmation_time")),
                        f"{_clock(trade.get('entry_time'))} @ {_float(trade.get('entry_price')):,.2f}",
                        f"{_float(trade.get('stop_price')):,.2f}",
                        f"{_float(trade.get('target_price')):,.2f}",
                        f"{_clock(trade.get('exit_time'))} {trade.get('exit_reason', '')}",
                        f"{_float(trade.get('net_return_pct')):+.6f}% / ₹{_float(trade.get('net_pnl_rs')):+,.2f}",
                    )
                )
                + " |"
            )

    lines.extend(
        [
            "",
            "## Selection and execution audit",
            "",
            "| Strategy | No confirmation | Entry window expired |",
            "|---|---:|---:|",
        ]
    )
    for strategy in EXPECTED_STRATEGIES:
        no_confirmation, entry_expired = _audit_outcomes(run_root, strategy)
        lines.append(f"| {FRIENDLY_NAMES[strategy]} | {no_confirmation} | {entry_expired} |")

    v6_lead = _float(v6["net_pnl_rs"]) - _float(v8["net_pnl_rs"])
    v12_gap = _float(v8["net_pnl_rs"]) - _float(v12["net_pnl_rs"])
    lines.extend(
        [
            "",
            "## Comparison conclusion",
            "",
            f"- **{FRIENDLY_NAMES[winner['strategy']]} leads today at ₹{_float(winner['net_pnl_rs']):+,.2f}.**",
            f"- V6 leads V8/V10/V11 by `₹{v6_lead:,.6f}`.",
            "- V8, V10 and V11 produced exactly identical fills and P&L today.",
            f"- V12 trails V8/V10/V11 by `₹{v12_gap:,.6f}` after its 09:40 volume filter selected KFINTECH instead of PRESTIGE.",
            "- A single day, especially V6's two-trade sample, is not sufficient to establish historical superiority.",
            "",
            "## Economics and integrity",
            "",
            f"- Costs: `{manifest['economics']['cost_bps']} bps`; added slippage: `{manifest['economics']['slippage_bps']} bps`; target exposure: `₹{manifest['economics']['target_exposure_per_entry_rs']:,.0f}` per entry.",
            "- Each of MCX, KAYNES, HINDPETRO, PRESTIGE and KFINTECH has 360 continuous one-minute rows through 15:15 IST.",
            "- Candidate identity and confirmation/entry/SL/target fields match the earlier causal replay.",
            f"- `complete={str(manifest.get('complete')).lower()}`, `source_complete={str(manifest.get('source_complete')).lower()}`, `headline_valid={str(manifest.get('headline_valid')).lower()}`, `research_only={str(manifest.get('research_only')).lower()}`.",
            "- HINDPETRO uses the last real 15:15 bar, so its terminal result remains last-real-bar sensitive.",
            "- This is a cash-equity execution proxy over frozen FnO selections, not actual futures fill or live-trading proof.",
            "",
            "## Artifacts",
            "",
            f"- Run root: `{run_root}`",
            f"- Comparison: `{run_root / 'comparison.csv'}`",
            f"- Trade contracts: `{run_root / 'trade_contracts.json'}`",
            f"- Manifest: `{run_root / 'manifest.json'}`",
            "",
        ]
    )
    return "\n".join(lines)


def publish_validated_run(
    run_root: Path,
    *,
    dashboard_root: Path = DEFAULT_DASHBOARD_ROOT,
    log_dir: Path = DEFAULT_LOG_DIR,
    console_log: str = "",
    expected_date: str = SESSION_DATE,
) -> dict[str, Path]:
    manifest, comparison, contracts = validate_run(run_root, expected_date=expected_date)
    published_at = _now_ist_text()
    report = render_report(
        run_root,
        manifest,
        comparison,
        contracts,
        published_at=published_at,
    )

    reports_dir = dashboard_root / "reports"
    latest_dir = dashboard_root / "latest"
    dated_report = reports_dir / f"backtesting_result_v6_v8_v10_v11_v12_{expected_date}.md"
    latest_report = latest_dir / "latest_backtesting_result_v11.md"
    combined_json = latest_dir / "latest_backtesting_result_v6_v8_v10_v11_v12.json"
    dated_log = log_dir / f"backtesting_result_v11_{expected_date}.log"
    latest_log = log_dir / "backtesting_result_v11_latest.log"

    payload = {
        "schema_version": "fno_combined_dashboard_publication_v1",
        "published_at_ist": published_at,
        "session_date": expected_date,
        "run_root": str(run_root),
        "strategies": comparison,
        "manifest_flags": {
            key: manifest.get(key)
            for key in ("complete", "source_complete", "headline_valid", "research_only", "promotion_eligible")
        },
        "report": str(dated_report),
    }
    log_text = console_log.rstrip() + "\n\n" + report

    # The validation above is deliberately completed before any currently
    # displayed successful report is replaced.
    _atomic_write_text(dated_report, report)
    _atomic_write_text(latest_report, report)
    _atomic_write_json(combined_json, payload)
    _atomic_write_text(dated_log, log_text)
    _atomic_write_text(latest_log, log_text)
    return {
        "dated_report": dated_report,
        "latest_report": latest_report,
        "combined_json": combined_json,
        "dated_log": dated_log,
        "latest_log": latest_log,
    }


def _find_run_root(stdout: str) -> Path:
    for line in stdout.splitlines():
        candidate = Path(line.strip())
        if candidate.is_dir() and (candidate / "manifest.json").is_file():
            return candidate
    raise PublicationError("replay output did not identify a completed run directory")


def run_and_publish(
    *,
    session_date: str,
    output_root: Path,
    dashboard_root: Path,
    log_dir: Path,
    replay_script: Path = REPLAY_SCRIPT,
) -> dict[str, Path]:
    if session_date != SESSION_DATE:
        raise PublicationError(
            f"this source-bound publisher supports only {SESSION_DATE}; requested {session_date}"
        )
    command = [sys.executable, "-u", str(replay_script), "--output-root", str(output_root)]
    started_at = _now_ist_text()
    start = time.perf_counter()
    completed = subprocess.run(
        command,
        cwd=BASE_DIR,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
    )
    elapsed = time.perf_counter() - start
    console_parts = [
        f"[{started_at}] START Backtesting result v6/v8/v10/v11/v12",
        "COMMAND " + subprocess.list2cmdline(command),
        completed.stdout.rstrip(),
    ]
    if completed.stderr.strip():
        console_parts.extend(("STDERR", completed.stderr.rstrip()))
    console_parts.append(f"END exit={completed.returncode} elapsed_sec={elapsed:.3f}")
    console_log = "\n".join(part for part in console_parts if part) + "\n"
    print(console_log, end="", flush=True)
    if completed.returncode != 0:
        # Failure logs are useful, but an existing successful Markdown report
        # must remain intact.
        _atomic_write_text(log_dir / f"backtesting_result_v11_{session_date}.log", console_log)
        _atomic_write_text(log_dir / "backtesting_result_v11_latest.log", console_log)
        raise PublicationError(f"combined replay failed with exit {completed.returncode}")

    run_root = _find_run_root(completed.stdout)
    paths = publish_validated_run(
        run_root,
        dashboard_root=dashboard_root,
        log_dir=log_dir,
        console_log=console_log,
        expected_date=session_date,
    )
    print(paths["latest_report"].read_text(encoding="utf-8"), flush=True)
    print(f"PUBLISHED_DASHBOARD_REPORT={paths['latest_report']}", flush=True)
    return paths


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", default=SESSION_DATE)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--dashboard-root", type=Path, default=DEFAULT_DASHBOARD_ROOT)
    parser.add_argument("--log-dir", type=Path, default=DEFAULT_LOG_DIR)
    return parser


def main(argv: Iterable[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        run_and_publish(
            session_date=args.date,
            output_root=args.output_root,
            dashboard_root=args.dashboard_root,
            log_dir=args.log_dir,
        )
    except PublicationError as exc:
        print(f"PUBLICATION_FAILED: {exc}", file=sys.stderr, flush=True)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
