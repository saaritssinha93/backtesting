"""Read-only, path-aware execution sensitivity for retained V13-V10-G.

This module deliberately does not import a live worker or broker client.  It
replays already-selected frozen-G orders over the immutable one-minute paths in
the historical source bundle.  PAPER observations calibrate *diagnostic
sensitivities* only: trigger-to-fill distance is not represented as broker
slippage, and the one-minute activation delay is a coarse stress rather than an
estimate of an observed eight-second delay.
"""

from __future__ import annotations

import csv
import hashlib
import json
import math
import os
import shutil
import tempfile
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from statistics import fmean, median
from typing import Any, Iterable, Mapping, Sequence

import numpy as np
import pandas as pd

import fno_v13_corrected_v5_backtest as v5
import fno_v13_v6_portfolio_backtest as v6


SCHEMA_VERSION = "eqidv2.v13_v10_g.execution_research.v1"
COUNTERFACTUAL_SCHEMA_VERSION = (
    "eqidv2.v13_v10_g.missed_entry_counterfactual.v1"
)
REPORT_FILENAME = "latest_fno_v13_v10_g_execution_realism.md"
SCENARIOS_FILENAME = "v13_v10_g_execution_scenarios.csv"
MISSED_FILENAME = "v13_v10_g_missed_entry_counterfactuals.csv"


@dataclass(frozen=True)
class ExecutionResearchBundle:
    run_id: str
    run_dir: Path
    latest_dir: Path
    manifest_path: Path
    state: str
    paper_orders: int
    paper_fills: int
    baseline_net_rupees: float
    stress_net_rupees: float | None

    def as_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "run_dir": str(self.run_dir),
            "latest_dir": str(self.latest_dir),
            "manifest_path": str(self.manifest_path),
            "state": self.state,
            "paper_orders": self.paper_orders,
            "paper_fills": self.paper_fills,
            "baseline_net_rupees": self.baseline_net_rupees,
            "stress_net_rupees": self.stress_net_rupees,
            "mode": "READ_ONLY_RESEARCH",
            "execution_authority": False,
            "live_configuration_changed": False,
        }


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _canonical_sha256(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
            default=str,
        ).encode("utf-8")
    ).hexdigest()


def _atomic_bytes(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=str(path.parent)
    )
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_name, path)
    except BaseException:
        try:
            os.unlink(temporary_name)
        except OSError:
            pass
        raise


def _atomic_text(path: Path, text: str) -> None:
    _atomic_bytes(path, text.encode("utf-8"))


def _atomic_json(path: Path, value: Mapping[str, Any]) -> None:
    _atomic_text(
        path,
        json.dumps(value, indent=2, sort_keys=True, ensure_ascii=True) + "\n",
    )


def _as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def _as_float(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _timestamp(value: Any) -> pd.Timestamp | None:
    try:
        stamp = pd.Timestamp(value)
    except (TypeError, ValueError):
        return None
    if pd.isna(stamp):
        return None
    if stamp.tzinfo is None:
        stamp = stamp.tz_localize("Asia/Kolkata")
    else:
        stamp = stamp.tz_convert("Asia/Kolkata")
    return stamp


def _quantile(values: Sequence[float], probability: float) -> float | None:
    if not values:
        return None
    return float(np.quantile(np.asarray(values, dtype=float), probability))


def collect_paper_calibration(paper_root: Path | None) -> dict[str, Any]:
    """Read terminal G PAPER order states and describe observable execution.

    The result intentionally calls trigger-to-fill movement a distance proxy,
    not slippage.  Pending/open rows are excluded so an intraday refresh cannot
    silently turn partial sessions into completed calibration evidence.
    """

    rows: list[dict[str, Any]] = []
    artifacts: dict[str, dict[str, Any]] = {}
    errors: list[str] = []
    terminal = {"CLOSED", "CANCELLED", "NO_FILL", "BLOCKED_SIZING", "BLOCKED_PORTFOLIO"}
    root = paper_root.resolve() if paper_root and paper_root.is_dir() else None
    if root:
        for path in sorted(root.rglob("*.json")):
            try:
                raw = path.read_bytes()
                value = json.loads(raw.decode("utf-8-sig"))
                if not isinstance(value, dict):
                    raise ValueError("order state is not an object")
                if str(value.get("mode", "")).upper() != "PAPER":
                    continue
                if str(value.get("status", "")).upper() not in terminal:
                    continue
                rows.append(value)
                relative = path.resolve().relative_to(root).as_posix()
                artifacts[relative] = {
                    "bytes": len(raw),
                    "sha256": hashlib.sha256(raw).hexdigest(),
                }
            except (OSError, UnicodeError, ValueError, json.JSONDecodeError) as exc:
                errors.append(f"{path.name}:{type(exc).__name__}")

    activation_seconds: list[float] = []
    trigger_seconds: list[float] = []
    distance_bps: list[float] = []
    fills = 0
    reasons: Counter[str] = Counter()
    dates: set[str] = set()
    for row in rows:
        dates.add(str(row.get("session_date", "")))
        reasons[str(row.get("status_reason") or "UNAVAILABLE")] += 1
        day = str(row.get("session_date") or "")
        confirmation = str(row.get("confirmation_end") or "")
        scheduled = _timestamp(
            f"{day}T{confirmation}:00+05:30"
            if day and len(confirmation) == 5 and ":" in confirmation
            else row.get("created_at_ist")
        )
        activated = _timestamp(row.get("entry_order_activated_at_ist"))
        entered = _timestamp(row.get("entry_at_ist"))
        if scheduled is not None and activated is not None and activated >= scheduled:
            activation_seconds.append(float((activated - scheduled).total_seconds()))
        if activated is not None and entered is not None and entered >= activated:
            trigger_seconds.append(float((entered - activated).total_seconds()))
        if entered is None:
            continue
        fills += 1
        trigger = _as_float(row.get("trigger_price"))
        entry = _as_float(row.get("entry_price"))
        if trigger is None or entry is None or trigger <= 0 or entry <= 0:
            continue
        adverse = entry - trigger if str(row.get("side", "")).upper() == "LONG" else trigger - entry
        distance_bps.append(adverse * 10_000.0 / trigger)

    def summary(values: Sequence[float]) -> dict[str, float | int | None]:
        return {
            "count": len(values),
            "mean": fmean(values) if values else None,
            "median": median(values) if values else None,
            "p90": _quantile(values, 0.9),
            "maximum": max(values) if values else None,
        }

    return {
        "state": "READY" if rows and not errors else "PARTIAL" if rows else "UNAVAILABLE",
        "terminal_orders": len(rows),
        "fills": fills,
        "fill_ratio_pct": fills * 100.0 / len(rows) if rows else None,
        "session_dates": sorted(day for day in dates if day),
        "status_reason_counts": dict(reasons.most_common()),
        "activation_latency_seconds": summary(activation_seconds),
        "trigger_latency_seconds": summary(trigger_seconds),
        "trigger_to_fill_distance_bps": summary(distance_bps),
        "source_artifacts": artifacts,
        "errors": errors,
        "interpretation": (
            "Trigger-to-fill distance is an observed quote-distance proxy, not broker slippage. "
            "Trigger-window expiries are already represented by the frozen ten-minute entry rule."
        ),
    }


def _load_paths(path: Path, sids: Iterable[int]) -> dict[int, dict[str, np.ndarray]]:
    requested = set(int(value) for value in sids)
    result: dict[int, dict[str, np.ndarray]] = {}
    with np.load(path, allow_pickle=False) as archive:
        for name in archive.files:
            sid_text, field = name.split("_", 1)
            sid = int(sid_text)
            if sid in requested:
                result.setdefault(sid, {})[field] = archive[name]
    required_fields = {"timestamp_ns", "open", "high", "low", "close"}
    for sid in requested:
        fields = result.get(sid, {})
        missing = sorted(required_fields.difference(fields))
        if missing:
            raise ValueError(f"execution path sid={sid} is missing: {', '.join(missing)}")
        lengths = {len(fields[name]) for name in required_fields}
        if lengths == {0} or len(lengths) != 1:
            raise ValueError(f"execution path sid={sid} has inconsistent arrays")
    return result


def _metrics(ledger: pd.DataFrame) -> dict[str, Any]:
    executed = ledger.loc[ledger["portfolio_executed"].astype(bool)].copy()
    pnl = pd.to_numeric(executed["portfolio_net_profit_rupees"], errors="coerce").fillna(0.0)
    gross = pd.to_numeric(executed["portfolio_gross_profit_rupees"], errors="coerce").fillna(0.0)
    costs = pd.to_numeric(executed["portfolio_cost_rupees"], errors="coerce").fillna(0.0)
    gains = float(pnl.loc[pnl > 0].sum())
    losses = float(-pnl.loc[pnl < 0].sum())
    daily = (
        pd.DataFrame({"day": executed.get("day", pd.Series(dtype=str)).astype(str), "pnl": pnl})
        .groupby("day", sort=True)["pnl"]
        .sum()
    )
    curve = daily.cumsum()
    drawdown = curve.cummax() - curve
    return {
        "selected_orders": int(len(ledger)),
        "mechanical_fills": int(ledger["filled"].astype(bool).sum()),
        "executed_trades": int(len(executed)),
        "wins": int((pnl > 0).sum()),
        "losses": int((pnl < 0).sum()),
        "win_rate_pct": float((pnl > 0).mean() * 100.0) if len(pnl) else 0.0,
        "gross_profit_rupees": float(gross.sum()),
        "cost_rupees": float(costs.sum()),
        "net_profit_rupees": float(pnl.sum()),
        "profit_factor": gains / losses if losses else (math.inf if gains else None),
        "daily_close_drawdown_rupees": float(drawdown.max()) if len(drawdown) else 0.0,
    }


def _scenario(
    orders: pd.DataFrame,
    paths: Mapping[int, dict[str, np.ndarray]],
    *,
    scenario: str,
    cost_bps: float,
    entry_expiry_minutes: int,
    capital_per_entry_rupees: float,
    leverage_factor: float,
    portfolio_capital_rupees: float,
    max_positions: int | None,
    delay_bars: int,
    distance_proxy_bps: float,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    simulated = v5.simulate_native(
        orders,
        dict(paths),
        cost_bps=cost_bps,
        delay_bars=delay_bars,
        worse_fill_bps=distance_proxy_bps,
        max_entry_delay_minutes=entry_expiry_minutes,
    )
    capitalized = v5.apply_fixed_capital_model(
        simulated, capital_per_entry_rupees, leverage_factor
    )
    ledger, _ = v6.apply_portfolio_constraints(
        capitalized,
        v6.PortfolioConfig(
            portfolio_capital_rupees=portfolio_capital_rupees,
            max_positions=max_positions,
        ),
    )
    metrics = _metrics(ledger)
    metrics.update(
        scenario=scenario,
        delay_bars=delay_bars,
        distance_proxy_bps=distance_proxy_bps,
    )
    return ledger, metrics


def missed_entry_counterfactuals(
    baseline: pd.DataFrame,
    paths: Mapping[int, dict[str, np.ndarray]],
    *,
    entry_expiry_minutes: int,
) -> list[dict[str, Any]]:
    """Describe finalized after-expiry paths without inventing a fill.

    These rows are outcome-time counterfactual diagnostics.  They must never be
    joined back into signal selection or called observed PAPER execution.
    """

    rows: list[dict[str, Any]] = []
    for row in baseline.loc[~baseline["filled"].astype(bool)].itertuples(index=False):
        path = paths[int(row.sid)]
        is_long = str(row.side).upper() == "LONG"
        trigger = float(row.trigger)
        window_end = min(entry_expiry_minutes, len(path["close"]))
        within_high = path["high"][:window_end]
        within_low = path["low"][:window_end]
        if is_long:
            closest_bps = float((within_high.max() / trigger - 1.0) * 10_000.0)
        else:
            closest_bps = float((trigger / within_low.min() - 1.0) * 10_000.0)

        after_high = path["high"][window_end:]
        after_low = path["low"][window_end:]
        hits = (
            np.flatnonzero(after_high >= trigger)
            if is_long
            else np.flatnonzero(after_low <= trigger)
        )
        touched = bool(hits.size)
        first_index: int | None = int(hits[0]) + window_end if touched else None
        entry: float | None = None
        mfe: float | None = None
        mae: float | None = None
        first_touch_at = ""
        if first_index is not None:
            bar_open = float(path["open"][first_index])
            entry = max(trigger, bar_open) if is_long else min(trigger, bar_open)
            high = path["high"][first_index:]
            low = path["low"][first_index:]
            if is_long:
                mfe = float((high.max() / entry - 1.0) * 100.0)
                mae = float((low.min() / entry - 1.0) * 100.0)
            else:
                mfe = float((1.0 - low.min() / entry) * 100.0)
                mae = float((1.0 - high.max() / entry) * 100.0)
            first_touch_at = pd.Timestamp(
                int(path["timestamp_ns"][first_index]), tz="UTC"
            ).tz_convert("Asia/Kolkata").isoformat()
        rows.append(
            {
                "schema_version": COUNTERFACTUAL_SCHEMA_VERSION,
                "day": str(row.day),
                "sid": int(row.sid),
                "tradingsymbol": str(row.tradingsymbol),
                "side": str(row.side),
                "setup_id": str(row.setup_id),
                "trigger": trigger,
                "entry_expiry_minutes": entry_expiry_minutes,
                "entry_window_closest_favorable_bps": closest_bps,
                "post_expiry_trigger_touched": touched,
                "post_expiry_first_touch_at_ist": first_touch_at,
                "counterfactual_entry_price": entry,
                "post_expiry_mfe_pct": mfe,
                "post_expiry_mae_pct": mae,
                "evidence_view": "FINALIZED_1M_COUNTERFACTUAL",
                "safe_for_selection": False,
            }
        )
    return rows


def _write_csv(path: Path, rows: Sequence[Mapping[str, Any]], fields: Sequence[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=str(path.parent)
    )
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(fields), extrasaction="ignore")
            writer.writeheader()
            writer.writerows(rows)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_name, path)
    except BaseException:
        try:
            os.unlink(temporary_name)
        except OSError:
            pass
        raise


def _fmt(value: Any, digits: int = 2) -> str:
    number = _as_float(value)
    if number is None:
        return "UNAVAILABLE"
    return f"{number:,.{digits}f}"


def generate_execution_research(
    *,
    source_run: Path,
    paper_root: Path | None,
    output_root: Path,
    now: datetime | None = None,
) -> ExecutionResearchBundle:
    """Generate immutable execution-sensitivity and missed-entry artifacts."""

    source_run = source_run.resolve()
    portfolio_path = source_run / "g_backtest" / "portfolio_trades.csv"
    paths_path = source_run / "dataset" / "paths.npz"
    dataset_manifest_path = source_run / "dataset" / "dataset_manifest.json"
    metadata_path = source_run / "g_backtest" / "run_metadata.json"
    for path in (portfolio_path, paths_path, dataset_manifest_path, metadata_path):
        if not path.is_file():
            raise FileNotFoundError(f"required execution-research input missing: {path}")

    dataset_manifest = json.loads(dataset_manifest_path.read_text(encoding="utf-8"))
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    declared_path_hash = str(
        dataset_manifest.get("output_sha256", {}).get("paths.npz", "")
    )
    observed_path_hash = sha256_file(paths_path)
    if not declared_path_hash or observed_path_hash != declared_path_hash:
        raise ValueError("dataset/paths.npz does not match the dataset manifest")

    config_path = Path(str(metadata.get("frozen_g_config", ""))).resolve()
    if not config_path.is_file():
        raise FileNotFoundError("frozen G configuration referenced by metadata is missing")
    expected_config_hash = str(metadata.get("frozen_g_config_sha256", ""))
    if sha256_file(config_path) != expected_config_hash:
        raise ValueError("frozen G configuration hash mismatch")
    config = json.loads(config_path.read_text(encoding="utf-8"))

    orders = pd.read_csv(portfolio_path)
    required_columns = {
        "sid", "day", "tradingsymbol", "side", "setup_id", "trigger",
        "native_stop_pct", "native_target_pct", "filled",
    }
    missing_columns = sorted(required_columns.difference(orders.columns))
    if missing_columns:
        raise ValueError("portfolio ledger missing columns: " + ", ".join(missing_columns))
    if orders["sid"].duplicated().any():
        raise ValueError("portfolio ledger contains duplicate sid values")
    orders["sid"] = pd.to_numeric(orders["sid"], errors="raise").astype(int)
    paths = _load_paths(paths_path, orders["sid"].tolist())

    cost_bps = float(config["cost_bps"])
    expiry = int(config["entry_expiry_minutes"])
    capital = float(config["capital_per_entry_rupees"])
    leverage = float(config["leverage_factor"])
    portfolio_capital = float(config["portfolio_capital_rupees"])
    max_positions_raw = config.get("max_positions")
    max_positions = int(max_positions_raw) if max_positions_raw is not None else None

    calibration = collect_paper_calibration(paper_root)
    distance = calibration["trigger_to_fill_distance_bps"]
    observed_median = max(0.0, float(distance["median"])) if distance["median"] is not None else None
    observed_p90 = max(0.0, float(distance["p90"])) if distance["p90"] is not None else None

    scenarios: list[tuple[str, int, float, str]] = [
        ("FROZEN_BASELINE", 0, 0.0, "Exact frozen entry model"),
    ]
    if observed_median is not None:
        scenarios.extend([
            (
                "OBSERVED_MEDIAN_DISTANCE_PROXY",
                0,
                observed_median,
                "Adverse entry-distance sensitivity; not broker slippage",
            ),
            (
                "COARSE_1M_DELAY_PLUS_MEDIAN_DISTANCE",
                1,
                observed_median,
                "One-minute-bar activation stress; not an estimate of seconds-level latency",
            ),
        ])
    if observed_p90 is not None:
        scenarios.append((
            "OBSERVED_P90_DISTANCE_PROXY",
            0,
            observed_p90,
            "Tail adverse entry-distance sensitivity; not broker slippage",
        ))

    scenario_rows: list[dict[str, Any]] = []
    ledgers: dict[str, pd.DataFrame] = {}
    for name, delay_bars, proxy_bps, interpretation in scenarios:
        ledger, metrics = _scenario(
            orders,
            paths,
            scenario=name,
            cost_bps=cost_bps,
            entry_expiry_minutes=expiry,
            capital_per_entry_rupees=capital,
            leverage_factor=leverage,
            portfolio_capital_rupees=portfolio_capital,
            max_positions=max_positions,
            delay_bars=delay_bars,
            distance_proxy_bps=proxy_bps,
        )
        metrics["interpretation"] = interpretation
        ledgers[name] = ledger
        scenario_rows.append(metrics)

    baseline = scenario_rows[0]
    declared = metadata.get("metrics", {}).get("full_history", {})
    reconciliation = {
        "selected_orders": int(declared.get("selected_orders", -1))
        == baseline["selected_orders"],
        "executed_trades": int(declared.get("trades", -1))
        == baseline["executed_trades"],
        "net_profit_rupees": math.isclose(
            float(declared.get("net_profit_rupees", math.nan)),
            float(baseline["net_profit_rupees"]),
            rel_tol=1e-9,
            abs_tol=0.01,
        ),
    }
    if not all(reconciliation.values()):
        raise ValueError(f"frozen baseline failed reconciliation: {reconciliation}")

    for row in scenario_rows:
        row["net_delta_vs_baseline_rupees"] = (
            float(row["net_profit_rupees"]) - float(baseline["net_profit_rupees"])
        )
        row["evidence_quality"] = "SENSITIVITY_ONLY_SMALL_OBSERVED_SAMPLE"
        row["safe_for_live_selection"] = False

    missed = missed_entry_counterfactuals(
        ledgers["FROZEN_BASELINE"], paths, entry_expiry_minutes=expiry
    )

    now_utc = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    run_id = now_utc.strftime("%Y%m%dT%H%M%S%fZ")
    root = output_root.resolve()
    run_dir = root / "runs" / run_id
    latest_dir = root / "latest"
    run_dir.mkdir(parents=True, exist_ok=False)

    scenario_fields = [
        "scenario", "delay_bars", "distance_proxy_bps", "selected_orders",
        "mechanical_fills", "executed_trades", "wins", "losses", "win_rate_pct",
        "gross_profit_rupees", "cost_rupees", "net_profit_rupees", "profit_factor",
        "daily_close_drawdown_rupees", "net_delta_vs_baseline_rupees",
        "evidence_quality", "safe_for_live_selection", "interpretation",
    ]
    missed_fields = [
        "schema_version", "day", "sid", "tradingsymbol", "side", "setup_id",
        "trigger", "entry_expiry_minutes", "entry_window_closest_favorable_bps",
        "post_expiry_trigger_touched", "post_expiry_first_touch_at_ist",
        "counterfactual_entry_price", "post_expiry_mfe_pct", "post_expiry_mae_pct",
        "evidence_view", "safe_for_selection",
    ]
    scenarios_path = run_dir / SCENARIOS_FILENAME
    missed_path = run_dir / MISSED_FILENAME
    _write_csv(scenarios_path, scenario_rows, scenario_fields)
    _write_csv(missed_path, missed, missed_fields)

    stressed = next(
        (row for row in scenario_rows if row["scenario"] == "COARSE_1M_DELAY_PLUS_MEDIAN_DISTANCE"),
        None,
    )
    report_lines = [
        "# V13-V10-G Execution-Realistic Diagnostic",
        "",
        f"- Generated UTC: `{now_utc.isoformat()}`",
        f"- Source through: `{metadata.get('through_day', 'UNAVAILABLE')}`",
        "- Mode: **READ-ONLY RESEARCH**",
        "- Execution authority: **NO**",
        "- Live configuration changed: **NO**",
        f"- Frozen baseline reconciliation: **{'PASS' if all(reconciliation.values()) else 'FAIL'}**",
        f"- PAPER terminal sample: `{calibration['terminal_orders']}` orders / `{calibration['fills']}` fills",
        f"- PAPER fill ratio: `{_fmt(calibration['fill_ratio_pct'])}%`",
        f"- Observed entry-distance median / p90: `{_fmt(distance['median'], 4)}` / `{_fmt(distance['p90'], 4)}` bps",
        "",
        "## Path-aware scenarios",
        "",
        "| Scenario | Delay bars | Distance proxy bps | Fills | Trades | Net INR | Delta INR | PF | Daily-close DD INR |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in scenario_rows:
        report_lines.append(
            f"| {row['scenario']} | {row['delay_bars']} | {_fmt(row['distance_proxy_bps'], 4)} | "
            f"{row['mechanical_fills']} | {row['executed_trades']} | {_fmt(row['net_profit_rupees'])} | "
            f"{_fmt(row['net_delta_vs_baseline_rupees'])} | {_fmt(row['profit_factor'], 3)} | "
            f"{_fmt(row['daily_close_drawdown_rupees'])} |"
        )
    late_touches = sum(bool(row["post_expiry_trigger_touched"]) for row in missed)
    report_lines += [
        "",
        "## Missed-entry diagnostics",
        "",
        f"- Frozen unfilled selections: `{len(missed)}`",
        f"- Trigger touched only after the frozen entry window: `{late_touches}`",
        "- Post-expiry MFE/MAE is a finalized-data counterfactual. It is not an observed fill and is never used by selection.",
        "",
        "## Interpretation guardrails",
        "",
        "- The PAPER sample is small and reused; it is insufficient for live parameter changes.",
        "- Trigger-to-fill distance is not broker quote slippage because quote-at-submit evidence is absent.",
        "- The one-minute delay deliberately overstates the observed seconds-level activation latency at the available bar resolution.",
        "- Trigger-window expiries are not randomly applied again; doing so would double-count a mechanism already present in the frozen replay.",
        "- These scenarios diagnose execution sensitivity. They do not optimize entries or promote a strategy.",
    ]
    report_path = run_dir / REPORT_FILENAME
    _atomic_text(report_path, "\n".join(report_lines) + "\n")

    source_artifacts = {
        "g_backtest/portfolio_trades.csv": {
            "bytes": portfolio_path.stat().st_size,
            "sha256": sha256_file(portfolio_path),
        },
        "dataset/paths.npz": {
            "bytes": paths_path.stat().st_size,
            "sha256": observed_path_hash,
        },
        "dataset/dataset_manifest.json": {
            "bytes": dataset_manifest_path.stat().st_size,
            "sha256": sha256_file(dataset_manifest_path),
        },
        "g_backtest/run_metadata.json": {
            "bytes": metadata_path.stat().st_size,
            "sha256": sha256_file(metadata_path),
        },
        "frozen_config.json": {
            "path": str(config_path),
            "bytes": config_path.stat().st_size,
            "sha256": expected_config_hash,
        },
        "execution_engine.py": {
            "path": str(Path(v5.__file__).resolve()),
            "sha256": sha256_file(Path(v5.__file__).resolve()),
        },
        "portfolio_engine.py": {
            "path": str(Path(v6.__file__).resolve()),
            "sha256": sha256_file(Path(v6.__file__).resolve()),
        },
    }
    manifest: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "run_id": run_id,
        "generated_at_utc": now_utc.isoformat().replace("+00:00", "Z"),
        "mode": "READ_ONLY_RESEARCH",
        "execution_authority": False,
        "live_configuration_changed": False,
        "source_run": str(source_run),
        "source_through_day": metadata.get("through_day"),
        "source_artifacts": source_artifacts,
        "paper_calibration": calibration,
        "frozen_baseline_reconciliation": reconciliation,
        "scenarios": scenario_rows,
        "missed_entry_counterfactuals": {
            "rows": len(missed),
            "late_trigger_touches": late_touches,
            "safe_for_selection": False,
        },
        "artifacts": {
            "report": {"filename": REPORT_FILENAME, "sha256": sha256_file(report_path)},
            "scenarios": {"filename": SCENARIOS_FILENAME, "sha256": sha256_file(scenarios_path)},
            "missed_entries": {"filename": MISSED_FILENAME, "sha256": sha256_file(missed_path)},
        },
        "conclusion": "INSUFFICIENT_EVIDENCE_FOR_LIVE_CHANGE",
        "safe_for_live_selection": False,
    }
    manifest["content_sha256"] = _canonical_sha256(manifest)
    _atomic_json(run_dir / "manifest.json", manifest)

    latest_dir.mkdir(parents=True, exist_ok=True)
    for filename in (REPORT_FILENAME, SCENARIOS_FILENAME, MISSED_FILENAME, "manifest.json"):
        source = run_dir / filename
        temporary = latest_dir / f".{filename}.{os.getpid()}.tmp"
        shutil.copyfile(source, temporary)
        os.replace(temporary, latest_dir / filename)

    return ExecutionResearchBundle(
        run_id=run_id,
        run_dir=run_dir,
        latest_dir=latest_dir,
        manifest_path=latest_dir / "manifest.json",
        state="READY_DIAGNOSTIC_ONLY",
        paper_orders=int(calibration["terminal_orders"]),
        paper_fills=int(calibration["fills"]),
        baseline_net_rupees=float(baseline["net_profit_rupees"]),
        stress_net_rupees=(
            float(stressed["net_profit_rupees"]) if stressed is not None else None
        ),
    )


__all__ = [
    "COUNTERFACTUAL_SCHEMA_VERSION",
    "ExecutionResearchBundle",
    "MISSED_FILENAME",
    "REPORT_FILENAME",
    "SCENARIOS_FILENAME",
    "SCHEMA_VERSION",
    "collect_paper_calibration",
    "generate_execution_research",
    "missed_entry_counterfactuals",
    "sha256_file",
]
