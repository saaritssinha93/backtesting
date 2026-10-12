"""G3 execution diagnostics from sealed replay and observed PAPER evidence.

This report does not replay path stresses, calibrate a model, or authorize
execution. PAPER quote-distance observations remain separate from historical
OHLC fills and broker slippage. Imports are safe for the read-only API venv.
"""
from __future__ import annotations

import csv
import hashlib
import json
import math
import os
from collections import Counter
from datetime import date, datetime, time
from pathlib import Path
from statistics import fmean, median
from uuid import uuid4

from ai_platform.observability.g3_analysis import verified_package
from ai_platform.observability.strategy_research import performance
from fno_v13_v10_g_3_contract import (
    IST, STRATEGY_PROFILE, STRATEGY_VERSION, canonical_signal_id, setup_for, strategy_fingerprint,
)

SCHEMA_VERSION = "eqidv2.v13_v10_g_3.execution_research.v1"
REPORT_FILENAME = "latest_fno_v13_v10_g_3_execution_realism.md"
TERMINAL = {"CLOSED", "CANCELLED", "NO_FILL", "BLOCKED_SIZING", "BLOCKED_PORTFOLIO"}


def _sha(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _number(value) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def _stamp(value) -> datetime | None:
    try:
        stamp = datetime.fromisoformat(str(value))
    except (TypeError, ValueError):
        return None
    return stamp.astimezone(IST) if stamp.tzinfo is not None else None


def _summary(values: list[float]) -> dict:
    ordered = sorted(values)
    p90 = None
    if ordered:
        position = (len(ordered) - 1) * .9
        lower = math.floor(position)
        upper = math.ceil(position)
        p90 = ordered[lower] + (ordered[upper] - ordered[lower]) * (position - lower)
    return dict(count=len(values), mean=fmean(values) if values else None,
                median=median(values) if values else None, p90=p90,
                maximum=max(values) if values else None)


def collect_g3_paper_calibration(paper_root: Path | None, *, now: datetime | None = None) -> dict:
    """Describe identity-verified terminal states from already closed sessions.

    Missing, mixed-profile, invalid and nonterminal records never become zero
    latency or fills. Contradictory duplicate identities fail the whole report.
    The source hashes record the exact observed order-state bytes.
    """
    clock = now or datetime.now(IST)
    if clock.tzinfo is None:
        raise ValueError("now must be timezone-aware")
    clock = clock.astimezone(IST)
    root = Path(paper_root).resolve() if paper_root is not None else None
    excluded: Counter = Counter()
    reasons: Counter = Counter()
    missing: Counter = Counter()
    observed = {}
    rows = []
    source_artifacts = {}
    files = sorted(root.rglob("*.json")) if root and root.is_dir() else []
    if len(files) > 20_000:
        raise ValueError("G3 PAPER evidence exceeds the bounded diagnostic scan")
    for path in files:
        try:
            resolved = path.resolve()
            if not resolved.is_relative_to(root) or path.stat().st_size > 2 * 1024 * 1024:
                raise ValueError("PAPER source boundary or size invalid")
            before = path.stat()
            raw = path.read_bytes()
            after = path.stat()
            if (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns):
                raise ValueError("PAPER source changed while reading")
            row = json.loads(raw.decode("utf-8-sig"))
            if not isinstance(row, dict):
                raise ValueError("PAPER state must be an object")
            if (row.get("schema_version") != "fno_v6_equity_order_state_v2"
                    or row.get("strategy_version") != STRATEGY_VERSION
                    or row.get("strategy_fingerprint") != strategy_fingerprint()
                    or row.get("mode") != "PAPER"):
                excluded["WRONG_SCHEMA_PROFILE_FINGERPRINT_OR_MODE"] += 1
                continue
            day = date.fromisoformat(str(row.get("session_date", "")))
            if day > clock.date() or (day == clock.date() and clock.time().replace(tzinfo=None) < time(15, 35)):
                excluded["SESSION_NOT_CLOSED"] += 1
                continue
            if str(row.get("status", "")) not in TERMINAL:
                excluded["NONTERMINAL_STATE"] += 1
                continue
            setup = setup_for(str(row.get("signal_end", "")), str(row.get("side", "")))
            if setup is None or row.get("confirmation_end") != setup.confirmation_end:
                raise ValueError("PAPER slot is not in the G3 contract")
            identity = canonical_signal_id(STRATEGY_VERSION, day, setup.signal_end,
                                           setup.confirmation_end, setup.side, str(row.get("tradingsymbol", "")))
            if row.get("signal_id") != identity:
                raise ValueError("PAPER canonical identity mismatch")
            entered = _stamp(row.get("entry_at_ist"))
            entry = _number(row.get("entry_price"))
            confirmation = datetime.combine(day, time.fromisoformat(setup.confirmation_end), tzinfo=IST)
            if row["status"] == "CLOSED" and (entered is None or entered.date() != day or entered < confirmation
                    or entered > clock or entry is None or entry <= 0):
                raise ValueError("Closed PAPER fill lacks dated positive entry evidence")
            digest = hashlib.sha256(raw).hexdigest()
            if identity in observed:
                if observed[identity] != digest:
                    raise RuntimeError(f"Conflicting G3 PAPER states for {identity}")
                excluded["IDENTICAL_DUPLICATE_STATE"] += 1
                continue
            observed[identity] = digest
            rows.append(row)
            reasons[str(row.get("status_reason") or row["status"])] += 1
            source_artifacts[resolved.relative_to(root).as_posix()] = dict(sha256=digest, bytes=len(raw))
        except (OSError, UnicodeError, ValueError, TypeError):
            excluded["INVALID_OR_CHANGING_ORDER_STATE"] += 1
    activation, trigger_wait, distance = [], [], []
    fill_ids = []
    for row in rows:
        scheduled = datetime.fromisoformat(f"{row['session_date']}T{row['confirmation_end']}:00").replace(tzinfo=IST)
        activated = _stamp(row.get("entry_order_activated_at_ist"))
        entered = _stamp(row.get("entry_at_ist"))
        if activated is not None and activated.date() == scheduled.date() and activated >= scheduled:
            activation.append((activated - scheduled).total_seconds())
        else:
            missing["activation_latency_seconds"] += 1
        if row["status"] != "CLOSED":
            continue
        fill_ids.append(row["signal_id"])
        if activated is not None and entered >= activated and activated.date() == entered.date():
            trigger_wait.append((entered - activated).total_seconds())
        else:
            missing["activation_to_fill_seconds"] += 1
        trigger = _number(row.get("trigger_price"))
        entry = _number(row.get("entry_price"))
        if trigger is not None and trigger > 0:
            adverse = (entry - trigger) if row["side"] == "LONG" else (trigger - entry)
            distance.append(adverse * 10_000. / trigger)
        else:
            missing["trigger_to_fill_distance_bps"] += 1
    return dict(state="DESCRIPTIVE_G3_PAPER_SAMPLE" if fill_ids else "AWAITING_G3_PAPER_CALIBRATION",
        calibrated=False, source_root=str(root) if root else None, terminal_orders=len(rows), fills=len(fill_ids),
        terminal_order_fill_ratio_pct=len(fill_ids)*100./len(rows) if rows else None,
        session_dates=sorted({row["session_date"] for row in rows}),
        filled_signal_ids=sorted(fill_ids), status_reason_counts=dict(sorted(reasons.items())),
        activation_latency_seconds=_summary(activation), activation_to_fill_seconds=_summary(trigger_wait),
        trigger_to_fill_distance_bps=_summary(distance), missing_measurements=dict(sorted(missing.items())),
        excluded_artifact_counts=dict(sorted(excluded.items())), source_artifacts=source_artifacts,
        interpretation="Terminal observed PAPER states only. Quote distance is not broker slippage. No cohort completeness or calibrated execution model is inferred.")


def _atomic(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + "." + uuid4().hex + ".tmp")
    temporary.write_text(content, encoding="utf-8", newline="\n")
    os.replace(temporary, path)


def generate_g3_execution_analysis(*, source_root: Path, paper_root: Path | None,
                                  output_root: Path, now: datetime | None = None) -> dict:
    run, manifest, paths = verified_package(source_root)
    with paths["portfolio_trades"].open(encoding="utf-8-sig", newline="") as stream:
        ledger = list(csv.DictReader(stream))
    fingerprint = strategy_fingerprint()
    if any(row.get("strategy_version") != STRATEGY_VERSION or row.get("strategy_fingerprint") != fingerprint for row in ledger):
        raise ValueError("G3 historical ledger identity mismatch")
    baseline = performance(ledger)
    metrics = manifest["metrics"]
    declared = next((metrics[k] for k in ("net_pnl_rupees", "net_profit_rupees", "net_pnl") if k in metrics), None)
    if declared is None or not math.isclose(baseline["net_profit_rupees"], float(declared), abs_tol=.02):
        raise ValueError("G3 execution baseline P&L differs from its sealed manifest")
    if baseline["executed_trades"] != int(metrics.get("trades", metrics.get("fills", -1))):
        raise ValueError("G3 execution baseline count differs from its sealed manifest")
    paper = collect_g3_paper_calibration(paper_root, now=now)
    overlap = len(set(paper["filled_signal_ids"]) & {row.get("signal_id") for row in ledger})
    state = "INSUFFICIENT_G3_PATH_STRESS_EVIDENCE" if paper["fills"] else "AWAITING_G3_PAPER_CALIBRATION"
    generated = (now or datetime.now(IST)).astimezone(IST).isoformat()
    payload = dict(schema_version=SCHEMA_VERSION, strategy_profile=STRATEGY_PROFILE,
        strategy_version=STRATEGY_VERSION, strategy_fingerprint=fingerprint, state=state,
        mode="READ_ONLY_EXECUTION_DIAGNOSTICS", execution_authority=False, broker_access=False,
        safe_for_live_selection=False, live_configuration_changed=False, calibrated=False,
        conclusion="INSUFFICIENT_EVIDENCE_FOR_LIVE_CHANGE", generated_at_ist=generated,
        source_run=str(run), source_manifest_sha256=_sha(run / "run_manifest.json"),
        source_role=manifest["role"], baseline=baseline, paper_calibration=paper,
        source_overlap=dict(filled_paper_signals_in_historical_ledger=overlap), scenarios=[],
        path_stress=dict(state="NOT_RUN", calibrated=False,
            reason="No G3-specific, hash-bound path-stress scenarios and reconciled PAPER calibration have been completed. Historical baseline fills do not establish live execution equivalence."))
    out = Path(output_root).resolve() / "runs" / (datetime.now(IST).strftime("%Y%m%dT%H%M%S%f") + "_" + uuid4().hex[:8])
    out.mkdir(parents=True, exist_ok=False)
    report = f"""# V13-V10-G-3 Execution Realism

- State: **{state}**
- Strategy: `{STRATEGY_VERSION}`
- Fingerprint: `{fingerprint}`
- Generated: `{generated}`
- Source run: `{run}`
- Execution authority: `false`

Historical baseline: {baseline['selected_orders']} selected orders, {baseline['executed_trades']} portfolio executions, INR {baseline['net_profit_rupees']:,.2f} net P&L.

Observed G3 PAPER sample: {paper['terminal_orders']} terminal orders and {paper['fills']} fills across {len(paper['session_dates'])} dates. These counts do not establish complete prospective sessions.

## PAPER measurements

```json
{json.dumps({key: paper[key] for key in ('activation_latency_seconds','activation_to_fill_seconds','trigger_to_fill_distance_bps','missing_measurements','excluded_artifact_counts')}, indent=2)}
```

Trigger-to-fill distance is an observed quote-distance proxy, not broker slippage. Only closed-session terminal PAPER states with the exact G3 version, fingerprint and canonical signal identity contribute. OPEN, pending, wrong-profile and malformed states are excluded and counted. Missing measurements remain unavailable.

## Calibration and path stresses

No G3 path-stress scenarios have been run by this report. No historical G stress, old G PAPER calibration, broker fills or shadow outcomes are reused. Historical candle fills and actual PAPER quote crossings use different execution clocks. PAPER timing statistics alone cannot prove fill or stop equivalence.

A separately verified G3 path-stress replay and reconciled prospective PAPER cohort are required before calibration readiness can be claimed. This report never changes strategy or grants execution authority.
"""
    _atomic(out / REPORT_FILENAME, report)
    payload.update(run_dir=str(out), report_filename=REPORT_FILENAME,
                   reports={REPORT_FILENAME:dict(path=REPORT_FILENAME,sha256=_sha(out / REPORT_FILENAME))})
    encoded = json.dumps(payload, indent=2, sort_keys=True, allow_nan=False) + "\n"
    _atomic(out / "manifest.json", encoded)
    latest = Path(output_root).resolve() / "latest"
    _atomic(latest / REPORT_FILENAME, report)
    _atomic(latest / "manifest.json", encoded)
    return payload
