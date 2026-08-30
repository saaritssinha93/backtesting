"""Generate a study-grade report for the locked standalone FNO V12 run.

The generator never mutates the sealed backtest directory.  It validates the
standalone provenance, recomputes the reported economics from the sealed CSV
artifacts, optionally validates the V12 research lineage and frozen V10/V11
comparators, and writes a Markdown study plus supporting tables and charts.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

import fno_oi_common as common
import fno_v10_backtest as v10
import fno_v10_full_historical_report as study
import fno_v10_recent_detailed_report as recent
import fno_v11_backtest as v11
import fno_v11_full_historical_report as v11_report
import fno_v12_backtest as v12
import fno_v12_staged_backtest as staged
import fno_v12_variant_registry as registry
import fno_v8_windowed_1m_entry_backtest as engine


SCHEMA_VERSION = "fno_v12_full_historical_study_report_v1"
DEFAULT_LINEAGE_ROOT = common.FNO_ROOT / "strategy_research" / "v12_fno_staged_research_v1"
BOOTSTRAP_REPLICATES = 10_000
RNG_SEED = 12_120


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _numbers(frame: pd.DataFrame, column: str) -> pd.Series:
    return v11_report._numbers(frame, column)


def _bools(frame: pd.DataFrame, column: str) -> pd.Series:
    return v11_report._bools(frame, column)


def _days(frame: pd.DataFrame) -> pd.Series:
    return v11_report._days(frame)


def _resolve_latest(root: Path) -> Path | None:
    latest = root / "latest.json"
    if not latest.is_file():
        return None
    payload = json.loads(latest.read_text(encoding="utf-8"))
    run_dir = Path(str(payload.get("run_dir", ""))).expanduser().resolve()
    return run_dir if run_dir.is_dir() else None


def _normalise_decisions(run_dir: Path) -> pd.DataFrame:
    all_candidates = pd.read_csv(run_dir / "all_input_candidates.csv", low_memory=False)
    raw = pd.read_csv(run_dir / "selection_decisions.csv", low_memory=False)
    if all_candidates["candidate_id"].duplicated().any() or raw["candidate_id"].duplicated().any():
        raise AssertionError("V12 candidate decisions must have unique candidate IDs")
    if set(all_candidates["candidate_id"].astype(str)) != set(raw["candidate_id"].astype(str)):
        raise AssertionError("V12 decisions do not cover every base candidate exactly once")
    metadata = raw.drop(columns=[column for column in raw if column in all_candidates and column != "candidate_id"])
    out = all_candidates.merge(metadata, on="candidate_id", how="left", validate="one_to_one")
    out["selection_passed"] = _bools(out, "kept")
    out["selection_reason"] = out["reason"].astype(str)
    for resolved, target in (
        ("resolved_picker", "picker"),
        ("resolved_picker_value", "picker_value"),
        ("resolved_frozen_rank", "frozen_rank"),
    ):
        if resolved in out:
            out[target] = out[resolved].where(out[resolved].notna(), out.get(target))
    return out


def _setup_parameters(resolved_profile: Mapping[str, Any]) -> pd.DataFrame:
    # The inherited entry-policy columns are resolved by the same helper used
    # for V10/V11.  The two V12 setup patches are then re-bound from the sealed
    # profile rather than inferred from mutable module state.
    table = recent._setup_parameter_table().copy()
    sealed = {
        f"{item['signal_end']}_{item['side']}": dict(item)
        for item in resolved_profile["setups"]
    }
    if set(table["setup_id"].astype(str)) != set(sealed):
        raise AssertionError("sealed V12 setup book differs from inherited setup identities")
    direct_fields = (
        "signal_end",
        "side",
        "max_entries",
        "picker",
        "price_change_pct",
        "oi_change_pct",
        "volume_ratio",
        "body_ratio",
        "max_wick_ratio",
        "min_traded_value",
        "stop_pct",
        "target_pct",
        "entry_conf_minute",
        "entry_buffer_bps",
        "entry_midpoint",
        "entry_clv",
    )
    for index, row in table.iterrows():
        payload = sealed[str(row["setup_id"])]
        for field in direct_fields:
            table.at[index, field] = payload.get(field)
        table.at[index, "five_minute_volume_ratio_min"] = payload["volume_ratio"]
    table["five_minute_side_aware_move_max_pct"] = math.nan
    table.loc[
        table["setup_id"].astype(str).eq("09:40_LONG"),
        "five_minute_side_aware_move_min_pct",
    ] = 0.40
    table.loc[
        table["setup_id"].astype(str).eq("09:35_LONG"),
        "five_minute_side_aware_move_max_pct",
    ] = 0.50
    table["effective_move_rule"] = table.apply(
        lambda row: (
            f">= +{float(row['five_minute_side_aware_move_min_pct']):.2f}%"
            + (
                f" and <= +{float(row['five_minute_side_aware_move_max_pct']):.2f}%"
                if pd.notna(row["five_minute_side_aware_move_max_pct"])
                else ""
            )
            if str(row["side"]) == "LONG"
            else f"<= -{float(row['five_minute_side_aware_move_min_pct']):.2f}%"
        ),
        axis=1,
    )
    table["five_minute_traded_value_min_cr"] = (
        pd.to_numeric(table["five_minute_traded_value_min"], errors="coerce")
        / 10_000_000.0
    )
    table["v11_entry_not_before_minute"] = np.where(
        table["setup_id"].astype(str).eq("09:30_SHORT"), 3, 1
    )
    table["effective_earliest_fill_minute"] = np.where(
        table["setup_id"].astype(str).eq("09:30_SHORT"), 3, 2
    )
    table["v12_changed_field"] = np.where(
        table["setup_id"].astype(str).isin(["09:40_SHORT", "09:45_SHORT"]),
        "volume_ratio raised 1.00 -> 1.50",
        "inherited unchanged",
    )
    return table


def _scenario_group_metrics(run_dir: Path, group: str) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for scenario, _, _ in v12.EXPECTED_SCENARIOS:
        trades = v11_report._add_features(
            pd.read_csv(run_dir / "scenarios" / scenario.lower() / "closed_trades.csv", low_memory=False)
        )
        part = v11_report._group_metrics(trades, group)
        part.insert(0, "scenario", scenario)
        frames.append(part)
    return pd.concat(frames, ignore_index=True)


def _daily_setup_detail(audit: pd.DataFrame) -> pd.DataFrame:
    records: list[dict[str, Any]] = []
    for (session, setup_id), part in audit.groupby(["session_date", "setup_id"], sort=True):
        trades = part.loc[_bools(part, "filled")]
        records.append(
            {
                "session_date": session,
                "setup_id": setup_id,
                "side": str(part["side"].iloc[0]),
                "signal_end": str(part["signal_end"].iloc[0]),
                "selected": len(part),
                "confirmed": int(_numbers(part, "confirmation_minute").notna().sum()),
                **v11_report._trade_metrics(trades),
            }
        )
    return pd.DataFrame(records)


def _selection_execution_detail(audit: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "candidate_id", "session_date", "signal_end", "setup_id", "side", "symbol",
        "futures_symbol", "frozen_rank", "picker", "picker_value", "price_change_pct",
        "ema9", "ema20", "ema50", "oi", "prev_oi", "oi_change_pct", "volume_ratio",
        "traded_value", "five_min_open", "five_min_high", "five_min_low", "five_min_close",
        "confirmation_minute", "confirmation_time", "confirmation_open", "confirmation_high",
        "confirmation_low", "confirmation_close", "confirmation_volume",
        "confirmation_body_ratio", "confirmation_adverse_wick_ratio",
        "confirmation_close_location", "confirmation_rejection_codes", "entry_minute",
        "entry_time", "trigger", "trigger_distance_c5_bps", "entry_price", "gap_fill",
        "stop_price", "target_price", "exit_time", "exit_price", "exit_reason",
        "gross_return_pct", "net_return_pct", "quantity", "position_notional_rs",
        "estimated_cost_rs", "net_pnl_rs", "mfe_pct_ohlc_lower_bound",
        "mfe_pct_ohlc_upper_bound", "mae_pct_ohlc_lower_bound", "mae_pct_ohlc_upper_bound",
        "portfolio_decision", "portfolio_reject_reason", "status", "reason",
    ]
    return audit[[column for column in columns if column in audit]].copy()


def _selected_path_terminal_coverage(
    selected: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    _, minute_paths, _, _, _, _ = v10._load_all_usable_max050_gap2_history()
    selected_ids = set(selected["candidate_id"].astype(str))
    paths = minute_paths.loc[
        minute_paths["candidate_id"].astype(str).isin(selected_ids)
    ].copy()
    observed_ids = set(paths["candidate_id"].astype(str))
    if observed_ids != selected_ids:
        missing = sorted(selected_ids - observed_ids)
        raise AssertionError(f"selected V12 candidates lack stored minute paths: {missing[:5]}")
    paths["bar_ts"] = pd.to_datetime(paths["bar_ts"], errors="raise", utc=True).dt.tz_convert(
        "Asia/Kolkata"
    )
    detail = (
        paths.groupby("candidate_id", as_index=False)
        .agg(
            session_date=("session_date", "first"),
            setup_id=("setup_id", "first"),
            side=("side", "first"),
            symbol=("symbol", "first"),
            path_rows=("bar_ts", "size"),
            first_path_bar=("bar_ts", "min"),
            terminal_path_bar=("bar_ts", "max"),
        )
        .sort_values(["session_date", "setup_id", "symbol"])
    )
    detail["terminal_clock"] = detail["terminal_path_bar"].dt.strftime("%H:%M")
    summary = (
        detail.groupby("terminal_clock", as_index=False)
        .agg(
            selected_candidates=("candidate_id", "size"),
            sessions=("session_date", "nunique"),
            earliest_session=("session_date", "min"),
            latest_session=("session_date", "max"),
        )
        .sort_values("terminal_clock")
    )
    return summary, detail


def _paired_comparison(
    source_run: Path, v12_daywise: pd.DataFrame, v12_closed: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, Path | None]:
    control_run = _resolve_latest(v11.OUTPUT_ROOT)
    if control_run is None:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), None
    v11.validate_run_provenance(control_run / "provenance.json")
    control_day = v11_report._prepare_daywise(
        pd.read_csv(control_run / "scenarios" / "reference_15_0" / "daywise.csv")
    )
    selected_day = v12_daywise.copy()
    paired = control_day[["session_date", "fills", "net_return_pct", "net_pnl_rs"]].merge(
        selected_day[["session_date", "fills", "net_return_pct", "net_pnl_rs"]],
        on="session_date", how="outer", suffixes=("_v11", "_v12"), validate="one_to_one"
    ).fillna(0)
    for metric in ("fills", "net_return_pct", "net_pnl_rs"):
        paired[f"delta_{metric}"] = paired[f"{metric}_v12"] - paired[f"{metric}_v11"]
    paired = paired.rename(columns={"delta_net_return_pct": "delta_net_return_points"})
    paired["cumulative_delta_net_return_points"] = paired["delta_net_return_points"].cumsum()
    paired["cumulative_delta_net_pnl_rs"] = paired["delta_net_pnl_rs"].cumsum()

    control_audit = v11_report._add_features(
        pd.read_csv(control_run / "scenarios" / "reference_15_0" / "candidate_order_audit.csv", low_memory=False)
    )
    control_closed = control_audit.loc[_bools(control_audit, "filled")].copy()
    identity = ["candidate_id", "session_date", "setup_id", "side", "symbol"]
    economics = ["entry_time", "exit_time", "exit_reason", "net_return_pct", "net_pnl_rs"]
    left = control_closed[identity + economics].copy()
    right = v12_closed[identity + economics].copy()
    changed_trades = left.merge(
        right,
        on="candidate_id",
        how="outer",
        suffixes=("_v11", "_v12"),
        indicator=True,
        validate="one_to_one",
    )
    for column in ("session_date", "setup_id", "side", "symbol"):
        changed_trades[column] = changed_trades[f"{column}_v12"].combine_first(
            changed_trades[f"{column}_v11"]
        )
    changed_trades["delta_net_return_points"] = (
        _numbers(changed_trades, "net_return_pct_v12").fillna(0)
        - _numbers(changed_trades, "net_return_pct_v11").fillna(0)
    )
    changed_trades["delta_net_pnl_rs"] = (
        _numbers(changed_trades, "net_pnl_rs_v12").fillna(0)
        - _numbers(changed_trades, "net_pnl_rs_v11").fillna(0)
    )
    changed_trades["change_type"] = np.select(
        [
            changed_trades["_merge"].eq("left_only"),
            changed_trades["_merge"].eq("right_only"),
        ],
        ["V11_FILL_REMOVED_IN_V12", "V12_FILL_ADDED_VS_V11"],
        default="COMMON_FILL_ECONOMICS_CHANGED",
    )
    changed_trades = changed_trades.loc[
        ~changed_trades["_merge"].eq("both")
        | changed_trades["delta_net_return_points"].abs().gt(1e-12)
        | changed_trades["delta_net_pnl_rs"].abs().gt(1e-8)
    ].drop(columns="_merge").sort_values(["session_date", "candidate_id"])

    selected_ids = set(
        pd.read_csv(source_run / "selected_candidates.csv", usecols=["candidate_id"])["candidate_id"].astype(str)
    )
    excluded = control_audit.loc[~control_audit["candidate_id"].astype(str).isin(selected_ids)].copy()
    excluded["v11_was_filled"] = _bools(excluded, "filled")
    exclusion_groups: list[dict[str, Any]] = []
    for setup_id, part in excluded.groupby("setup_id", sort=True):
        trades = part.loc[_bools(part, "filled")]
        exclusion_groups.append(
            {
                "setup_id": setup_id,
                "v11_candidates_removed_by_v12": len(part),
                "v11_confirmed": int(_numbers(part, "confirmation_minute").notna().sum()),
                **v11_report._trade_metrics(trades),
            }
        )
    return paired, changed_trades, excluded, pd.DataFrame(exclusion_groups), control_run


def _comparator_summary(v12_headline: Mapping[str, Any], control_run: Path | None) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    v10_run = _resolve_latest(v10.MAX050_GAP2_OUTPUT_ROOT)
    if v10_run is not None:
        v10_provenance = json.loads((v10_run / "provenance.json").read_text(encoding="utf-8"))
        v10_benchmark = json.loads(
            (v10_run / "current_mixed_benchmark_verification.json").read_text(encoding="utf-8")
        )
        if (
            v10_provenance.get("schema_version") == v10.MAX050_GAP2_SCHEMA_VERSION
            and bool(v10_benchmark.get("verified"))
        ):
            summary = json.loads(
                (v10_run / "scenarios" / "reference_15_0" / "summary.json").read_text(encoding="utf-8")
            )
            rows.append({"strategy": "V10 frozen", "run_dir": str(v10_run), **summary})
    if control_run is not None:
        v11.validate_run_provenance(control_run / "provenance.json")
        summary = json.loads(
            (control_run / "scenarios" / "reference_15_0" / "summary.json").read_text(encoding="utf-8")
        )
        rows.append({"strategy": "V11 frozen", "run_dir": str(control_run), **summary})
    v12_summary = dict(v12.EXPECTED_FULL_USABLE["REFERENCE_15_0"])
    v12_summary.update(dict(v12_headline))
    rows.append({"strategy": "V12 selected", "run_dir": "current sealed source", **v12_summary})
    return pd.DataFrame(rows)


def _load_lineage(
    lineage_run: Path | None, headline: Mapping[str, Any]
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, Mapping[str, Any] | None]:
    if lineage_run is None:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), None
    run_dir = lineage_run.expanduser().resolve()
    provenance_path = run_dir / "provenance.json"
    payload = json.loads(provenance_path.read_text(encoding="utf-8"))
    if payload.get("schema_version") != staged.SCHEMA_VERSION or not payload.get("complete"):
        raise AssertionError("V12 staged lineage is incomplete or has the wrong schema")
    inventory = staged._validate_inventory(run_dir, run_dir / "artifact_inventory.json")
    if not inventory.get("validated"):
        raise AssertionError("V12 staged lineage inventory did not validate")
    metrics = pd.read_csv(run_dir / "all_period_metrics.csv", low_memory=False)
    chosen = metrics.loc[
        metrics["variant_id"].astype(str).eq(v12.PROFILE_ID)
        & metrics["period"].astype(str).eq("FULL_USABLE")
    ].copy()
    reference = chosen.loc[chosen["scenario"].astype(str).eq("REFERENCE_15_0")]
    if len(reference) != 1:
        raise AssertionError("V12 staged lineage lacks one selected reference result")
    for field in ("fills", "wins", "losses", "profit_factor", "net_return_points", "net_pnl_rs"):
        if not math.isclose(float(reference.iloc[0][field]), float(headline[field]), rel_tol=0, abs_tol=1e-9):
            raise AssertionError(f"V12 staged/standalone result differs: {field}")
    gates = pd.read_csv(run_dir / "development_gates.csv", low_memory=False)
    selected_gate = gates.loc[gates["variant_id"].astype(str).eq(v12.PROFILE_ID)].copy()
    bootstrap = pd.read_csv(run_dir / "bootstrap_and_concentration.csv", low_memory=False)
    selected_bootstrap = bootstrap.loc[bootstrap["variant_id"].astype(str).eq(v12.PROFILE_ID)].copy()
    reference_metrics = metrics.loc[
        metrics["period"].astype(str).eq("FULL_USABLE")
        & metrics["scenario"].astype(str).eq("REFERENCE_15_0")
    ].copy()
    reference_metrics = reference_metrics.merge(
        gates[["variant_id", "gate_status", "observed_rank", "gate_passing_rank"]],
        on="variant_id", how="left"
    ).sort_values(["net_return_points", "profit_factor"], ascending=False).reset_index(drop=True)
    return chosen, selected_gate, selected_bootstrap, reference_metrics, payload


def _plots(
    assets_dir: Path,
    daywise: pd.DataFrame,
    setup_metrics: pd.DataFrame,
    monthly: pd.DataFrame,
    scenario_metrics: pd.DataFrame,
    funnel: Mapping[str, int],
    paired: pd.DataFrame,
) -> list[Path]:
    plt.style.use("seaborn-v0_8-whitegrid")
    paths: list[Path] = []

    path = assets_dir / "equity_and_drawdown.png"
    fig, axes = plt.subplots(2, 1, figsize=(12, 7), sharex=True)
    axes[0].plot(daywise["session_date"], daywise["cumulative_net_pnl_rs"], color="#1769aa", linewidth=2)
    axes[0].axhline(0, color="#555555", linewidth=0.8)
    axes[0].set_ylabel("Cumulative net P&L (Rs)")
    axes[0].set_title("V12 selected reference scenario")
    axes[1].fill_between(daywise["session_date"], daywise["drawdown_pnl_rs"], 0, color="#c62828", alpha=0.35)
    axes[1].set_ylabel("Drawdown (Rs)")
    axes[1].set_xlabel("Session")
    fig.autofmt_xdate()
    fig.tight_layout()
    fig.savefig(path, dpi=170)
    plt.close(fig)
    paths.append(path)

    for filename, frame, key, title in (
        ("setup_net_pnl.png", setup_metrics, "setup_id", "Net contribution by setup"),
        ("monthly_net_pnl.png", monthly, "period", "Monthly stability; May and August are partial"),
        ("cost_scenarios.png", scenario_metrics, "scenario", "Cost and slippage sensitivity"),
    ):
        path = assets_dir / filename
        ordered = frame.sort_values("net_pnl_rs") if filename == "setup_net_pnl.png" else frame
        colors = ["#2e7d32" if value >= 0 else "#c62828" for value in _numbers(ordered, "net_pnl_rs")]
        fig, ax = plt.subplots(figsize=(10, 6))
        if filename == "setup_net_pnl.png":
            ax.barh(ordered[key], ordered["net_pnl_rs"], color=colors)
            ax.set_xlabel("Net P&L (Rs)")
        else:
            ax.bar(ordered[key], ordered["net_pnl_rs"], color=colors)
            ax.set_ylabel("Net P&L (Rs)")
            ax.tick_params(axis="x", rotation=15)
        ax.axhline(0, color="#333333", linewidth=0.8) if filename != "setup_net_pnl.png" else ax.axvline(0, color="#333333", linewidth=0.8)
        ax.set_title(title)
        fig.tight_layout()
        fig.savefig(path, dpi=170)
        plt.close(fig)
        paths.append(path)

    path = assets_dir / "selection_funnel.png"
    fig, ax = plt.subplots(figsize=(9, 5))
    labels, values = list(funnel), list(funnel.values())
    bars = ax.bar(labels, values, color=["#455a64", "#1976d2", "#7b1fa2", "#ef6c00", "#2e7d32"])
    ax.set_ylabel("Candidates / trades")
    ax.set_title("Five-minute selection to closed winners")
    ax.tick_params(axis="x", rotation=18)
    for bar, value in zip(bars, values, strict=True):
        ax.text(bar.get_x() + bar.get_width() / 2, value, str(value), ha="center", va="bottom")
    fig.tight_layout()
    fig.savefig(path, dpi=170)
    plt.close(fig)
    paths.append(path)

    if not paired.empty:
        path = assets_dir / "v12_minus_v11_cumulative_delta.png"
        fig, ax = plt.subplots(figsize=(10, 5))
        ax.plot(paired["session_date"], paired["cumulative_delta_net_return_points"], color="#6a1b9a", linewidth=2)
        ax.axhline(0, color="#333333", linewidth=0.8)
        ax.set_ylabel("Cumulative delta points")
        ax.set_title("V12 minus V11: paired daily cumulative difference")
        fig.autofmt_xdate()
        fig.tight_layout()
        fig.savefig(path, dpi=170)
        plt.close(fig)
        paths.append(path)
    return paths


def build_report(
    *,
    source_run: Path,
    report_path: Path,
    assets_dir: Path,
    lineage_run: Path | None,
) -> dict[str, Any]:
    source_run = source_run.expanduser().resolve()
    report_path = report_path.expanduser().resolve()
    assets_dir = assets_dir.expanduser().resolve()
    if report_path.is_relative_to(source_run) or assets_dir.is_relative_to(source_run):
        raise ValueError("study outputs must remain outside the sealed V12 run")
    assets_dir.mkdir(parents=True, exist_ok=True)

    provenance_path = source_run / "provenance.json"
    validation = v12.validate_run_provenance(provenance_path)
    provenance = json.loads(provenance_path.read_text(encoding="utf-8"))
    resolved_profile = json.loads((source_run / "resolved_profile.json").read_text(encoding="utf-8"))
    benchmark = json.loads((source_run / "benchmark_verification.json").read_text(encoding="utf-8"))
    if not bool(benchmark.get("verified")):
        raise AssertionError("standalone V12 benchmark verification is false")

    decisions = _normalise_decisions(source_run)
    selected = pd.read_csv(source_run / "selected_candidates.csv", low_memory=False)
    path_terminal_summary, path_terminal_detail = _selected_path_terminal_coverage(selected)
    audit_raw = pd.read_csv(
        source_run / "scenarios" / "reference_15_0" / "candidate_order_audit.csv", low_memory=False
    )
    closed_file = pd.read_csv(
        source_run / "scenarios" / "reference_15_0" / "closed_trades.csv", low_memory=False
    )
    if set(selected["candidate_id"].astype(str)) != set(audit_raw["candidate_id"].astype(str)):
        raise AssertionError("selected candidates differ from reference audit")
    audit = v11_report._add_features(audit_raw)
    closed = audit.loc[_bools(audit, "filled")].copy()
    if set(closed["candidate_id"].astype(str)) != set(closed_file["candidate_id"].astype(str)):
        raise AssertionError("filled audit IDs differ from closed-trade artifact")
    expected_fingerprint = provenance["closed_trade_economic_fingerprints"]["REFERENCE_15_0"]
    if v12._closed_trade_economic_fingerprint(closed_file) != expected_fingerprint:
        raise AssertionError("V12 reference closed-trade fingerprint drifted")

    headline = v11_report._trade_metrics(closed)
    expected = benchmark["benchmarks"]["REFERENCE_15_0"]["observed"]
    for field in ("fills", "wins", "losses", "win_rate_pct", "profit_factor", "net_return_points", "net_pnl_rs"):
        if not math.isclose(float(headline[field]), float(expected[field]), rel_tol=0, abs_tol=1e-9):
            raise AssertionError(f"V12 report reconciliation failed: {field}")

    daywise = v11_report._prepare_daywise(
        pd.read_csv(source_run / "scenarios" / "reference_15_0" / "daywise.csv")
    )
    sessions = daywise["session_date"].tolist()
    expected_span = engine.expected_regular_session_dates(min(sessions), max(sessions))
    missing_sessions = sorted(set(expected_span) - set(sessions))
    if [item.isoformat() for item in missing_sessions] != provenance["missing_regular_session_dates"]:
        raise AssertionError("V12 report calendar gap differs from provenance")

    setup_parameters = _setup_parameters(resolved_profile)
    setup_metrics = study._funnel_by(decisions, audit, closed, "setup_id").merge(
        setup_parameters[["setup_id", "max_entries", "picker"]], on="setup_id", how="left", validate="one_to_one"
    ).sort_values("setup_id").reset_index(drop=True)
    side_metrics = study._funnel_by(decisions, audit, closed, "side")
    slot_metrics = study._funnel_by(decisions, audit, closed, "signal_end").sort_values("signal_end")
    picker_metrics = study._funnel_by(decisions, audit, closed, "picker")

    audit_rank = _numbers(audit, "frozen_rank")
    audit["rank_bucket"] = np.select(
        [audit_rank.eq(1), audit_rank.eq(2), audit_rank.eq(3), audit_rank.eq(4), audit_rank.eq(5)],
        ["1", "2", "3", "4", "5"], default="6+"
    )
    closed = audit.loc[_bools(audit, "filled")].copy()
    rank_metrics = v11_report._group_metrics(closed, "rank_bucket")
    rank_funnel = audit.groupby("rank_bucket", as_index=False, sort=False).agg(
        selected=("candidate_id", "size"),
        confirmed=("confirmation_minute", lambda values: pd.to_numeric(values, errors="coerce").notna().sum()),
        fills=("filled", lambda values: pd.Series(values).fillna(False).astype(bool).sum()),
    ).merge(rank_metrics, on=["rank_bucket", "fills"], how="left")

    month_labels = {session: session.strftime("%Y-%m") for session in sessions}
    week_labels = {session: f"{session.isocalendar().year}-W{session.isocalendar().week:02d}" for session in sessions}
    weekday_labels = {session: session.strftime("%A") for session in sessions}
    monthly = study._calendar_group_table(decisions, audit, closed, sessions, month_labels)
    weekly = study._calendar_group_table(decisions, audit, closed, sessions, week_labels)
    weekday = study._calendar_group_table(decisions, audit, closed, sessions, weekday_labels)
    weekday_order = {name: index for index, name in enumerate(["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"])}
    weekday = weekday.assign(_order=weekday["period"].map(weekday_order)).sort_values("_order").drop(columns="_order")
    blocks = study._stability_blocks(decisions, audit, closed, sessions)
    rolling = study._rolling_table(daywise, closed, 10)
    periods = pd.DataFrame(
        [
            study._period_funnel(decisions, audit, closed, sessions, "FULL_65"),
            study._period_funnel(decisions, audit, closed, [day for day in sessions if day < date(2026, 8, 20)], "CORE_59"),
            study._period_funnel(decisions, audit, closed, [day for day in sessions if day >= date(2026, 8, 20)], "FORWARD_6"),
            study._period_funnel(decisions, audit, closed, sessions[:32], "FIRST_HALF_32"),
            study._period_funnel(decisions, audit, closed, sessions[32:], "SECOND_HALF_33"),
            study._period_funnel(decisions, audit, closed, sessions[-14:], "LAST_14_USABLE"),
        ]
    )
    daily_detail = v11_report._daily_detail(decisions, audit, closed, daywise)
    daily_setup = _daily_setup_detail(audit)

    all_period_metrics = pd.read_csv(source_run / "all_period_metrics.csv")
    scenario_metrics = all_period_metrics.loc[all_period_metrics["period"].eq("FULL_USABLE")].copy()
    scenario_order = [name for name, _, _ in v12.EXPECTED_SCENARIOS]
    scenario_metrics = scenario_metrics.set_index("scenario").loc[scenario_order].reset_index()
    reference_pnl = float(scenario_metrics.loc[scenario_metrics["scenario"].eq("REFERENCE_15_0"), "net_pnl_rs"].iloc[0])
    scenario_metrics["net_pnl_change_vs_reference_rs"] = _numbers(scenario_metrics, "net_pnl_rs") - reference_pnl
    scenario_metrics["net_pnl_retained_vs_reference_pct"] = _numbers(scenario_metrics, "net_pnl_rs") / reference_pnl * 100
    scenario_setup = _scenario_group_metrics(source_run, "setup_id")
    scenario_side = _scenario_group_metrics(source_run, "side")
    scenario_confirmation = _scenario_group_metrics(source_run, "confirmation_minute")
    scenario_entry = _scenario_group_metrics(source_run, "entry_minute")
    scenario_exit = _scenario_group_metrics(source_run, "exit_reason")
    outcome_transitions = v11_report._scenario_transitions(source_run)

    reference_setup = scenario_setup.loc[scenario_setup["scenario"].eq("REFERENCE_15_0")]
    harsh_setup = scenario_setup.loc[scenario_setup["scenario"].eq("STRESS_25_5")]
    setup_robustness = reference_setup.merge(
        harsh_setup, on="setup_id", suffixes=("_reference", "_harsh"), validate="one_to_one"
    )

    status_counts = audit["status"].value_counts(dropna=False).rename_axis("status").reset_index(name="count")
    status_counts["share_pct"] = status_counts["count"] / len(audit) * 100
    reason_counts = audit["reason"].value_counts(dropna=False).rename_axis("reason").reset_index(name="count")
    reason_counts["share_pct"] = reason_counts["count"] / len(audit) * 100
    filter_rejections = decisions.loc[~_bools(decisions, "selection_passed")].copy()
    rejection_summary = filter_rejections.groupby(["selection_reason", "setup_id"], as_index=False).agg(
        rejections=("candidate_id", "size"), affected_sessions=("session_date", "nunique"),
        median_price_change_pct=("price_change_pct", "median"), median_volume_ratio=("volume_ratio", "median")
    )
    confirmation_checks = recent._confirmation_detail(audit)
    rejection_codes: list[str] = []
    for value in confirmation_checks["rejection_codes"].fillna(""):
        rejection_codes.extend(code for code in str(value).split(" | ") if code)
    one_minute_rejections = pd.Series(rejection_codes, dtype=object).value_counts().rename_axis("reason").reset_index(name="occurrences")

    indicator_names = [
        "directional_move_pct", "directional_five_min_body_pct", "oi_change_pct", "volume_ratio",
        "traded_value_cr", "five_min_range_pct", "five_min_body_ratio",
        "five_min_adverse_wick_ratio", "five_min_directional_close_location", "ema_fast_gap_pct",
        "ema_slow_gap_pct", "ema_total_gap_pct", "directional_close_ema9_pct",
        "confirmation_volume_ratio", "confirmation_body_ratio", "confirmation_adverse_wick_ratio",
        "confirmation_close_location", "trigger_distance_c5_bps", "confirmation_minute", "entry_minute",
    ]
    indicator_bins = v11_report._indicator_bins(audit)
    indicator_cohorts = study._indicator_cohorts(audit)
    quartiles = v11_report._quartile_analysis(audit, indicator_names)
    winner_loser = v11_report._winner_loser_table(closed, indicator_names)
    correlations = v11_report._correlations(closed, indicator_names + ["holding_minutes", "initial_stop_risk_pct"])
    confirmed_mask = _numbers(audit, "confirmation_minute").notna()
    filled_mask = _bools(audit, "filled")
    indicator_tests = pd.concat(
        [
            v11_report._binary_indicator_tests(audit, indicator_names[:15], positive=confirmed_mask, negative=~confirmed_mask, comparison="CONFIRMED_VS_NOT_CONFIRMED"),
            v11_report._binary_indicator_tests(audit, indicator_names[:15], positive=filled_mask, negative=~filled_mask, comparison="FILLED_VS_NOT_FILLED"),
            v11_report._binary_indicator_tests(closed, indicator_names, positive=_numbers(closed, "net_return_pct").gt(0), negative=_numbers(closed, "net_return_pct").lt(0), comparison="WINNER_VS_LOSER"),
        ], ignore_index=True
    )
    oi_quality_anomalies = closed.loc[_numbers(closed, "oi_change_pct").gt(20)].copy()

    confirmation_metrics = v11_report._group_metrics(
        closed.assign(confirmation_minute=_numbers(closed, "confirmation_minute").astype("Int64").astype(str)),
        "confirmation_minute"
    ).sort_values("confirmation_minute")
    entry_metrics = v11_report._group_metrics(
        closed.assign(entry_minute=_numbers(closed, "entry_minute").astype("Int64").astype(str)), "entry_minute"
    ).sort_values("entry_minute")
    setup_confirmation = v11_report._group_metrics(
        closed.assign(setup_confirmation=closed["setup_id"].astype(str) + " / M" + _numbers(closed, "confirmation_minute").astype("Int64").astype(str)),
        "setup_confirmation"
    ).sort_values("setup_confirmation")

    closed["gap_group"] = np.where(_bools(closed, "gap_fill"), "GAP_FILL_ACCEPTED", "TRIGGER_TOUCH")
    gap_fill_metrics = v11_report._group_metrics(closed, "gap_group")
    audit["gap_guard_path"] = np.select(
        [_bools(audit, "gap_guard_rejected"), _bools(audit, "gap_guard_observed")],
        ["GAP_REJECTED", "GAP_ACCEPTED"], default="NO_GAP_OBSERVED"
    )
    gap_records: list[dict[str, Any]] = []
    for key, part in audit.groupby("gap_guard_path", sort=False):
        trades = part.loc[_bools(part, "filled")]
        gap_values = _numbers(part, "gap_guard_adverse_bps").dropna()
        gap_records.append({
            "gap_guard_path": key, "candidates": len(part),
            "median_adverse_gap_bps": float(gap_values.median()) if len(gap_values) else math.nan,
            **v11_report._trade_metrics(trades),
        })
    gap_path_metrics = pd.DataFrame(gap_records)
    gap_rejections = audit.loc[_bools(audit, "gap_guard_rejected")].copy()

    unconstrained = audit.loc[_numbers(audit, "unconstrained_net_return_pct").notna()].copy()
    for column in ("status", "net_return_pct", "net_pnl_rs", "gross_return_pct", "gross_pnl_rs", "estimated_cost_rs", "position_notional_rs"):
        unconstrained[column] = unconstrained[f"unconstrained_{column}"]
    portfolio_comparison = pd.DataFrame([
        {"portfolio_view": "ACTUAL_CAP2_LEDGER", **v11_report._trade_metrics(closed)},
        {"portfolio_view": "UNCONSTRAINED_CANDIDATE_OUTCOMES", **v11_report._trade_metrics(unconstrained)},
    ])
    portfolio_decisions = audit["portfolio_decision"].value_counts(dropna=False).rename_axis("portfolio_decision").reset_index(name="count")
    portfolio_rejections = audit.loc[audit["portfolio_decision"].astype(str).eq("REJECTED")].copy()
    portfolio_timeline = v11_report._portfolio_timeline(closed, sessions)
    portfolio_exposure = pd.DataFrame([{
        "maximum_open_positions": int(_numbers(portfolio_timeline, "open_positions").max()),
        "maximum_deployed_cash_equivalent_notional_rs": float(_numbers(portfolio_timeline, "deployed_notional_rs").max()),
        "median_deployed_notional_when_active_rs": float(_numbers(portfolio_timeline.loc[_numbers(portfolio_timeline, "open_positions").gt(0)], "deployed_notional_rs").median()),
        "modeled_capital_rs": 120_000.0, "margin_reservation_per_entry_rs": 10_000.0,
        "maximum_global_reservations": 12, "same_symbol_same_side_limit": 2,
    }])

    exit_metrics = v11_report._group_metrics(closed, "exit_reason").sort_values("net_pnl_rs", ascending=False)
    exit_time_metrics = v11_report._group_metrics(closed, "exit_clock").sort_values("exit_clock")
    closed["holding_bin"] = pd.cut(
        _numbers(closed, "holding_minutes"), [-math.inf, 5, 15, 30, 60, 120, math.inf],
        labels=["<=5m", "6-15m", "16-30m", "31-60m", "61-120m", "120m+"], include_lowest=True
    ).astype(str)
    holding_metrics = v11_report._group_metrics(closed, "holding_bin")
    closed["ambiguity_group"] = np.select(
        [_bools(closed, "excursion_boundary_ambiguous"), _bools(closed, "ambiguous_entry_bar")],
        ["EXCURSION_BOUNDARY_AMBIGUOUS", "ENTRY_BAR_AMBIGUOUS"], default="NO_RECORDED_AMBIGUITY"
    )
    ambiguity_metrics = v11_report._group_metrics(closed, "ambiguity_group")
    excursion_by_outcome = closed.groupby("outcome", as_index=False).agg(
        trades=("candidate_id", "size"),
        median_mfe_lower_pct=("mfe_pct_ohlc_lower_bound", "median"),
        median_mfe_upper_pct=("mfe_pct_ohlc_upper_bound", "median"),
        median_mae_lower_pct=("mae_pct_ohlc_lower_bound", "median"),
        median_mae_upper_pct=("mae_pct_ohlc_upper_bound", "median"),
        median_net_r=("net_r_multiple", "median"), median_holding_minutes=("holding_minutes", "median")
    )
    loser_rows = closed.loc[closed["outcome"].eq("LOSS")]
    stopped_rows = closed.loc[closed["exit_reason"].astype(str).eq("STOP")]
    excursion_records: list[dict[str, Any]] = []
    for threshold in (0.25, 0.5, 0.75, 1.0, 1.5):
        for cohort, frame, feature in (
            ("ALL_LOSERS_UPPER_BOUND", loser_rows, "mfe_upper_r"),
            ("STOP_EXITS_LOWER_BOUND", stopped_rows, "mfe_lower_r"),
        ):
            reached = _numbers(frame, feature).ge(threshold)
            excursion_records.append(
                {
                    "cohort": cohort,
                    "mfe_bound": feature,
                    "threshold_r": threshold,
                    "trades": int(reached.sum()),
                    "cohort_trades": len(frame),
                    "share_of_cohort_pct": float(reached.mean() * 100) if len(frame) else math.nan,
                }
            )
    excursion_thresholds = pd.DataFrame(excursion_records)
    terminal_paths = closed.loc[closed["exit_reason"].astype(str).eq("LAST_REAL_BAR_SENSITIVITY")].groupby("exit_clock", as_index=False).agg(
        fills=("candidate_id", "size"), wins=("net_return_pct", lambda values: (values > 0).sum()),
        losses=("net_return_pct", lambda values: (values < 0).sum()),
        net_return_points=("net_return_pct", "sum"), net_pnl_rs=("net_pnl_rs", "sum")
    )
    excursion_quality = pd.DataFrame([{
        "fills": len(closed),
        "entry_bar_ambiguous": int(_bools(closed, "excursion_entry_bar_ambiguous").sum()),
        "exit_bar_ambiguous": int(_bools(closed, "excursion_exit_bar_ambiguous").sum()),
        "boundary_ambiguous": int(_bools(closed, "excursion_boundary_ambiguous").sum()),
        "median_mfe_bound_width_pct": float((_numbers(closed, "mfe_pct_ohlc_upper_bound") - _numbers(closed, "mfe_pct_ohlc_lower_bound")).median()),
        "median_mae_bound_width_pct": float((_numbers(closed, "mae_pct_ohlc_upper_bound") - _numbers(closed, "mae_pct_ohlc_lower_bound")).median()),
    }])

    symbol_metrics = v11_report._group_metrics(closed, "symbol").sort_values("net_pnl_rs", ascending=False).reset_index(drop=True)
    setup_symbol_metrics = v11_report._group_metrics(
        closed.assign(setup_symbol=closed["setup_id"].astype(str) + " / " + closed["symbol"].astype(str)), "setup_symbol"
    ).sort_values("net_pnl_rs", ascending=False)
    best_trades = closed.nlargest(15, "net_pnl_rs").assign(extreme="BEST_15")
    worst_trades = closed.nsmallest(15, "net_pnl_rs").assign(extreme="WORST_15")
    extreme_trades = pd.concat([best_trades, worst_trades], ignore_index=True)
    positive_symbol = _numbers(symbol_metrics, "net_pnl_rs").clip(lower=0)
    positive_symbol_points = _numbers(symbol_metrics, "net_return_points").clip(lower=0)
    symbol_abs_points = _numbers(symbol_metrics, "net_return_points").abs()
    concentration = pd.DataFrame([{
        "unique_symbols": int(closed["symbol"].nunique()),
        "positive_symbols": int(_numbers(symbol_metrics, "net_pnl_rs").gt(0).sum()),
        "negative_symbols": int(_numbers(symbol_metrics, "net_pnl_rs").lt(0).sum()),
        "one_fill_symbols": int(_numbers(symbol_metrics, "fills").eq(1).sum()),
        "top_5_positive_symbols_share_of_net_pct": float(positive_symbol.nlargest(5).sum() / headline["net_pnl_rs"] * 100),
        "top_5_positive_symbols_share_of_net_points_pct": float(positive_symbol_points.nlargest(5).sum() / headline["net_return_points"] * 100),
        "best_5_days_share_of_net_pct": float(_numbers(daywise, "net_pnl_rs").nlargest(5).sum() / headline["net_pnl_rs"] * 100),
        "best_5_days_share_of_net_points_pct": float(_numbers(daywise, "net_return_pct").nlargest(5).sum() / headline["net_return_points"] * 100),
        "best_10_trades_share_of_net_pct": float(_numbers(closed, "net_pnl_rs").nlargest(10).sum() / headline["net_pnl_rs"] * 100),
        "best_10_trades_share_of_net_points_pct": float(_numbers(closed, "net_return_pct").nlargest(10).sum() / headline["net_return_points"] * 100),
        "absolute_symbol_points_hhi": float(((symbol_abs_points / symbol_abs_points.sum()) ** 2).sum()),
    }])
    daily_signs = np.sign(_numbers(daywise, "net_pnl_rs")).astype(int).tolist()
    ordered_trades = closed.sort_values(["entry_time", "candidate_id"], kind="stable")
    trade_signs = np.sign(_numbers(ordered_trades, "net_return_pct")).astype(int).tolist()
    win_ci = v11_report._wilson_interval(int(headline["wins"]), int(headline["fills"]))
    total_notional = float(_numbers(closed, "position_notional_rs").sum())
    risk_summary = pd.DataFrame([{
        "best_pnl_day": daywise.loc[_numbers(daywise, "net_pnl_rs").idxmax(), "session_date"],
        "best_day_pnl_rs": float(_numbers(daywise, "net_pnl_rs").max()),
        "worst_pnl_day": daywise.loc[_numbers(daywise, "net_pnl_rs").idxmin(), "session_date"],
        "worst_day_pnl_rs": float(_numbers(daywise, "net_pnl_rs").min()),
        "average_daily_pnl_rs": float(_numbers(daywise, "net_pnl_rs").mean()),
        "median_daily_pnl_rs": float(_numbers(daywise, "net_pnl_rs").median()),
        "daily_pnl_std_rs": float(_numbers(daywise, "net_pnl_rs").std(ddof=1)),
        "max_consecutive_positive_days": v11_report._streak(daily_signs, 1),
        "max_consecutive_negative_days": v11_report._streak(daily_signs, -1),
        "max_consecutive_winning_trades": v11_report._streak(trade_signs, 1),
        "max_consecutive_losing_trades": v11_report._streak(trade_signs, -1),
        "max_drawdown_points": float(-_numbers(daywise, "drawdown_return_points").min()),
        "max_drawdown_pnl_rs": float(-_numbers(daywise, "drawdown_pnl_rs").min()),
        "win_rate_wilson_95_low_pct": win_ci[0], "win_rate_wilson_95_high_pct": win_ci[1],
        "extra_break_even_cost_bps_on_fixed_notional": float(headline["net_pnl_rs"] / total_notional * 10_000),
    }])
    drawdown_episodes = v11_report._drawdown_episodes(daywise)
    bootstrap = v11_report._bootstrap_scenarios(source_run)
    order_drawdown = v11_report._order_drawdown(daywise)
    daily_regimes = v11_report._daily_regimes(audit, closed, daywise)

    source_segments = v11_report._source_segment_table(provenance)
    source_incomplete = int(_numbers(source_segments, "source_incomplete_symbol_sessions").sum())
    source_expected = int(_numbers(source_segments, "expected_symbol_sessions").sum())
    blocked_tests = pd.DataFrame([record.payload() for record in registry.BLOCKED_TESTS])
    terminal_1515_count = int(
        _numbers(
            path_terminal_summary.loc[path_terminal_summary["terminal_clock"].eq("15:15")],
            "selected_candidates",
        ).sum()
    )
    blocked_tests.loc[
        blocked_tests["test_id"].eq("UNIFORM_EXACT_1530_PATHS"), "reason"
    ] = (
        f"{terminal_1515_count} selected paths stop at 15:15 rather than the intended 15:30."
    )
    profile = dict(provenance["profile"])
    global_parameters = pd.DataFrame([
        {"layer": "Identity", "parameter": "V12 profile", "value": v12.PROFILE_ID, "scope": "locked standalone"},
        {"layer": "Identity", "parameter": "Profile SHA-256", "value": v12.LOCKED_PROFILE_SHA256, "scope": "entire profile"},
        {"layer": "Selection", "parameter": "09:40 SHORT minimum volume ratio", "value": "1.50 inclusive", "scope": "V12 change"},
        {"layer": "Selection", "parameter": "09:45 SHORT minimum volume ratio", "value": "1.50 inclusive", "scope": "V12 change"},
        {"layer": "Selection", "parameter": "09:40 LONG directional move floor", "value": "0.40% inclusive", "scope": "inherited V11"},
        {"layer": "Selection", "parameter": "09:35 LONG directional move ceiling", "value": "0.50% inclusive", "scope": "inherited V11"},
        {"layer": "Ranking", "parameter": "Rerank after selection", "value": "True", "scope": "each setup/side/slot"},
        {"layer": "1m timing", "parameter": "09:30 SHORT earliest trigger-fill", "value": "S+3", "scope": "inherited V11"},
        {"layer": "Gap", "parameter": "Maximum adverse trigger gap", "value": "2 bps", "scope": "strong-identity gap events"},
        {"layer": "Portfolio", "parameter": "Same symbol + same side concurrent limit", "value": "2", "scope": "all setups"},
        {"layer": "Portfolio", "parameter": "Same symbol + opposite side", "value": "Prohibited", "scope": "all setups"},
        {"layer": "Portfolio", "parameter": "Modeled capital", "value": "Rs 120,000", "scope": "proxy global ledger"},
        {"layer": "Portfolio", "parameter": "Margin reservation per entry", "value": "Rs 10,000", "scope": "proxy global ledger"},
        {"layer": "Sizing", "parameter": "Target cash-equivalent exposure", "value": "Rs 50,000", "scope": "quantity=floor(exposure/entry)"},
        {"layer": "Exit", "parameter": "Same-bar collision", "value": "STOP_FIRST", "scope": "conservative OHLC rule"},
        {"layer": "Exit", "parameter": "Square-off clock", "value": v12.SQUARE_OFF, "scope": "when a real bar exists"},
        {"layer": "Exit", "parameter": "Terminal policy", "value": v12.EOD_POLICY, "scope": "partial-path sensitivity"},
        {"layer": "Costs", "parameter": "Reference", "value": "15 bps + 0 bps entry slippage", "scope": "headline"},
        {"layer": "Costs", "parameter": "Stress", "value": "20 bps + 2 bps entry slippage", "scope": "sensitivity"},
        {"layer": "Costs", "parameter": "Harsh", "value": "25 bps + 5 bps entry slippage", "scope": "sensitivity"},
    ])
    formula_reference = pd.DataFrame([
        {"feature": "5m construction", "formula": "exact five valid end-labelled 1m rows; O/H/L/C/V = first/max/min/last/sum", "causal_note": "completed slot only"},
        {"feature": "EMA9/20/50", "formula": "pandas EWM(close, span=N, adjust=False)", "causal_note": "cash-equity 5m closes through S"},
        {"feature": "price_change_pct", "formula": "100 * (C[S] / C[S-5m] - 1)", "causal_note": "side-aware threshold"},
        {"feature": "OI change pct", "formula": "100 * (OI[S] / OI[S-5m] - 1)", "causal_note": "exact preceding futures 5m timestamp"},
        {"feature": "volume_ratio", "formula": "V[S] / mean(V[S-20..S-1]); min_periods=5", "causal_note": "current volume excluded from denominator"},
        {"feature": "traded_value", "formula": "cash-equity C[S] * V[S]", "causal_note": "used for liquidity picker/minimum"},
        {"feature": "broad base gates", "formula": "directional move >=0.10%, OI change >=0.05%, volume ratio >=0.80", "causal_note": "all setup thresholds are equal or stricter"},
        {"feature": "confirmation body ratio", "formula": "abs(C-O)/(H-L)", "causal_note": "completed S+N 1m candle"},
        {"feature": "LONG adverse wick", "formula": "(H-max(O,C))/(H-L)", "causal_note": "SHORT mirrors on lower wick"},
        {"feature": "directional close location", "formula": "LONG (C-L)/(H-L); SHORT (H-C)/(H-L)", "causal_note": "higher is stronger"},
        {"feature": "entry trigger", "formula": "LONG confirmation H + buffer; SHORT confirmation L - buffer; tick-rounded", "causal_note": "cannot fill on confirmation bar"},
        {"feature": "stop/target", "formula": "actual fill * (1 +/- setup stop/target pct), adversely tick-rounded", "causal_note": "STOP_FIRST if both touch in one OHLC bar"},
        {"feature": "quantity", "formula": "floor(Rs 50,000 / cash-equity entry price)", "causal_note": "not futures lot sizing"},
        {"feature": "net return", "formula": "side-aware gross return pct - cost_bps/100", "causal_note": "configured slippage affects entry only"},
        {"feature": "PF", "formula": "sum(positive net-return points) / abs(sum(negative net-return points))", "causal_note": "trade-return points, not account PF"},
    ])

    paired, changed_trades, v11_excluded, exclusion_groups, control_run = _paired_comparison(source_run, daywise, closed)
    comparator_summary = _comparator_summary(headline, control_run)
    lineage_metrics, selected_gate, selected_bootstrap, variant_leaderboard, lineage_payload = _load_lineage(lineage_run, headline)

    removed_changes = changed_trades.loc[
        changed_trades["change_type"].eq("V11_FILL_REMOVED_IN_V12")
    ].copy()
    removed_metric_frame = removed_changes.rename(
        columns={"net_return_pct_v11": "net_return_pct", "net_pnl_rs_v11": "net_pnl_rs"}
    )
    removed_metrics = v11_report._trade_metrics(removed_metric_frame)
    mechanism_summary = pd.DataFrame(
        [
            {
                "mechanism": "V11 selected candidates excluded by V12 late-SHORT filters",
                "count": len(v11_excluded),
                "net_return_points_effect": math.nan,
                "net_pnl_rs_effect": math.nan,
            },
            {
                "mechanism": "V11 fills removed by V12",
                "count": len(removed_changes),
                "wins": removed_metrics["wins"],
                "losses": removed_metrics["losses"],
                "profit_factor": removed_metrics["profit_factor"],
                "net_return_points_effect": -float(removed_metrics["net_return_points"]),
                "net_pnl_rs_effect": -float(removed_metrics["net_pnl_rs"]),
            },
            {
                "mechanism": "V12 fills added versus V11",
                "count": int(changed_trades["change_type"].eq("V12_FILL_ADDED_VS_V11").sum()),
                "net_return_points_effect": float(
                    changed_trades.loc[
                        changed_trades["change_type"].eq("V12_FILL_ADDED_VS_V11"),
                        "delta_net_return_points",
                    ].sum()
                ),
                "net_pnl_rs_effect": float(
                    changed_trades.loc[
                        changed_trades["change_type"].eq("V12_FILL_ADDED_VS_V11"),
                        "delta_net_pnl_rs",
                    ].sum()
                ),
            },
            {
                "mechanism": "Common-fill economics changed after reranking/ledger ordering",
                "count": int(changed_trades["change_type"].eq("COMMON_FILL_ECONOMICS_CHANGED").sum()),
                "net_return_points_effect": float(
                    changed_trades.loc[
                        changed_trades["change_type"].eq("COMMON_FILL_ECONOMICS_CHANGED"),
                        "delta_net_return_points",
                    ].sum()
                ),
                "net_pnl_rs_effect": float(
                    changed_trades.loc[
                        changed_trades["change_type"].eq("COMMON_FILL_ECONOMICS_CHANGED"),
                        "delta_net_pnl_rs",
                    ].sum()
                ),
            },
            {
                "mechanism": "Total V12 minus V11 (changed sessions)",
                "count": int(_numbers(paired, "delta_net_return_points").abs().gt(1e-12).sum()),
                "net_return_points_effect": float(_numbers(paired, "delta_net_return_points").sum()),
                "net_pnl_rs_effect": float(_numbers(paired, "delta_net_pnl_rs").sum()),
            },
        ]
    )

    terminal_dependency_records: list[dict[str, Any]] = []
    terminal_slices = {
        "ALL_REFERENCE_TRADES": closed,
        "TARGET_AND_STOP_ONLY": closed.loc[
            ~closed["exit_reason"].astype(str).eq("LAST_REAL_BAR_SENSITIVITY")
        ],
        "ALL_LAST_REAL_BAR": closed.loc[
            closed["exit_reason"].astype(str).eq("LAST_REAL_BAR_SENSITIVITY")
        ],
        "LAST_REAL_BAR_AT_1530": closed.loc[
            closed["exit_reason"].astype(str).eq("LAST_REAL_BAR_SENSITIVITY")
            & closed["exit_clock"].astype(str).eq("15:30")
        ],
        "LAST_REAL_BAR_AT_1515": closed.loc[
            closed["exit_reason"].astype(str).eq("LAST_REAL_BAR_SENSITIVITY")
            & closed["exit_clock"].astype(str).eq("15:15")
        ],
    }
    for label, frame in terminal_slices.items():
        terminal_dependency_records.append(
            {"terminal_view": label, **v11_report._trade_metrics(frame)}
        )
    terminal_dependency = pd.DataFrame(terminal_dependency_records)

    funnel = {
        "Base 5m": len(decisions), "After V12 filters": len(audit),
        "1m confirmed": int(_numbers(audit, "confirmation_minute").notna().sum()),
        "Filled": len(closed), "Winners": int(headline["wins"]),
    }
    chart_paths = _plots(assets_dir, daywise, setup_metrics, monthly, scenario_metrics, funnel, paired)

    asset_frames: dict[str, pd.DataFrame] = {
        "daily_performance.csv": daily_detail,
        "daily_setup_performance.csv": daily_setup,
        "selection_entry_exit_detail.csv": _selection_execution_detail(audit),
        "closed_trade_detail.csv": _selection_execution_detail(closed),
        "five_minute_filter_rejections.csv": filter_rejections,
        "period_comparison.csv": periods,
        "monthly_performance.csv": monthly,
        "weekly_performance.csv": weekly,
        "weekday_performance.csv": weekday,
        "ten_session_blocks.csv": blocks,
        "rolling_10_session_metrics.csv": rolling,
        "all_period_all_cost_metrics.csv": all_period_metrics,
        "cost_scenario_summary.csv": scenario_metrics,
        "cost_outcome_transitions.csv": outcome_transitions,
        "setup_metrics.csv": setup_metrics,
        "setup_cost_robustness.csv": setup_robustness,
        "setup_all_scenarios.csv": scenario_setup,
        "side_metrics.csv": side_metrics,
        "side_all_scenarios.csv": scenario_side,
        "slot_metrics.csv": slot_metrics,
        "picker_metrics.csv": picker_metrics,
        "rank_metrics.csv": rank_funnel,
        "confirmation_minute_all_scenarios.csv": scenario_confirmation,
        "entry_minute_all_scenarios.csv": scenario_entry,
        "exit_reason_all_scenarios.csv": scenario_exit,
        "candidate_status_counts.csv": status_counts,
        "candidate_reason_counts.csv": reason_counts,
        "selection_rejection_summary.csv": rejection_summary,
        "confirmation_checks_expanded.csv": confirmation_checks,
        "one_minute_rejection_codes.csv": one_minute_rejections,
        "indicator_bins.csv": indicator_bins,
        "indicator_cohort_summary.csv": indicator_cohorts,
        "indicator_quartiles.csv": quartiles,
        "indicator_winner_loser.csv": winner_loser,
        "indicator_correlations.csv": correlations,
        "indicator_binary_tests_bh.csv": indicator_tests,
        "oi_low_base_anomalies.csv": oi_quality_anomalies,
        "confirmation_minute_metrics.csv": confirmation_metrics,
        "entry_minute_metrics.csv": entry_metrics,
        "setup_confirmation_minute_metrics.csv": setup_confirmation,
        "gap_fill_metrics.csv": gap_fill_metrics,
        "gap_guard_path_metrics.csv": gap_path_metrics,
        "gap_guard_rejections.csv": gap_rejections,
        "portfolio_actual_vs_unconstrained.csv": portfolio_comparison,
        "portfolio_decisions.csv": portfolio_decisions,
        "portfolio_rejections.csv": portfolio_rejections,
        "portfolio_minute_timeline.csv": portfolio_timeline,
        "portfolio_exposure_summary.csv": portfolio_exposure,
        "exit_reason_metrics.csv": exit_metrics,
        "exit_time_metrics.csv": exit_time_metrics,
        "holding_period_metrics.csv": holding_metrics,
        "ambiguity_metrics.csv": ambiguity_metrics,
        "excursion_by_outcome.csv": excursion_by_outcome,
        "loser_mfe_thresholds.csv": excursion_thresholds,
        "terminal_path_summary.csv": terminal_paths,
        "selected_path_terminal_coverage.csv": path_terminal_summary,
        "selected_path_terminal_detail.csv": path_terminal_detail,
        "terminal_dependency_summary.csv": terminal_dependency,
        "excursion_quality_summary.csv": excursion_quality,
        "symbol_metrics.csv": symbol_metrics,
        "setup_symbol_metrics.csv": setup_symbol_metrics,
        "extreme_trades.csv": extreme_trades,
        "concentration_summary.csv": concentration,
        "risk_summary.csv": risk_summary,
        "drawdown_episodes.csv": drawdown_episodes,
        "session_bootstrap.csv": bootstrap,
        "random_order_drawdown.csv": order_drawdown,
        "daily_regime_diagnostics.csv": daily_regimes,
        "source_segments.csv": source_segments,
        "setup_parameter_reference.csv": setup_parameters,
        "global_parameter_reference.csv": global_parameters,
        "indicator_formula_reference.csv": formula_reference,
        "blocked_validity_tests.csv": blocked_tests,
        "v12_vs_v11_daywise.csv": paired,
        "v12_vs_v11_changed_fills.csv": changed_trades,
        "v11_candidates_excluded_by_v12.csv": v11_excluded,
        "v12_filter_counterfactual_by_setup.csv": exclusion_groups,
        "v12_vs_v11_mechanism_summary.csv": mechanism_summary,
        "v10_v11_v12_comparison.csv": comparator_summary,
        "v12_lineage_metrics.csv": lineage_metrics,
        "v12_selected_development_gate.csv": selected_gate,
        "v12_selected_paired_bootstrap.csv": selected_bootstrap,
        "v12_variant_leaderboard.csv": variant_leaderboard,
    }
    output_paths = v11_report._write_frames(assets_dir, asset_frames) + chart_paths

    reference_row = scenario_metrics.loc[scenario_metrics["scenario"].eq("REFERENCE_15_0")].iloc[0]
    harsh_row = scenario_metrics.loc[scenario_metrics["scenario"].eq("STRESS_25_5")].iloc[0]
    forward_row = periods.loc[periods["period"].eq("FORWARD_6")].iloc[0]
    july_row = monthly.loc[monthly["period"].eq("2026-07")].iloc[0]
    eod = exit_metrics.loc[exit_metrics["exit_reason"].astype(str).eq("LAST_REAL_BAR_SENSITIVITY")]
    eod_fills = int(eod["fills"].iloc[0]) if len(eod) else 0
    eod_points = float(eod["net_return_points"].iloc[0]) if len(eod) else 0.0
    eod_share = eod_points / float(headline["net_return_points"]) * 100 if headline["net_return_points"] else math.nan
    significant_winner = indicator_tests.loc[
        indicator_tests["comparison"].eq("WINNER_VS_LOSER") & _numbers(indicator_tests, "bh_q_value").lt(0.05)
    ]
    significant_fill = indicator_tests.loc[
        indicator_tests["comparison"].eq("FILLED_VS_NOT_FILLED") & _numbers(indicator_tests, "bh_q_value").lt(0.05)
    ]
    affected_days = paired.loc[_numbers(paired, "delta_net_return_points").abs().gt(1e-12)] if not paired.empty else paired
    selected_reference_bootstrap = selected_bootstrap.loc[selected_bootstrap["scenario"].eq("REFERENCE_15_0")] if not selected_bootstrap.empty else selected_bootstrap

    lines: list[str] = []

    def add(items: Iterable[str]) -> None:
        lines.extend(items)

    add([
        "# V12 FNO selected strategy — full historical deep-study report", "",
        f"Generated: {datetime.now(common.IST).isoformat()}",
        f"Validated standalone run: `{source_run}`",
        f"Profile: `{v12.PROFILE_ID}`",
        f"Profile SHA-256: `{v12.LOCKED_PROFILE_SHA256}`",
        f"Historical input binding: `{provenance['input_binding_sha256']}`", "",
        "> **Research boundary:** `headline_valid=false`, `research_only=true`, `promotion_eligible=false`, and `live_or_paper_authority=false`. The figures are reproducible cash-equity execution proxies selected after V12 research; they are not live futures evidence.", "",
        "## Executive conclusion", "",
        f"The sealed replay covers **{len(sessions)} usable sessions** from **{min(sessions)} through {max(sessions)}** and records **{int(headline['fills'])} fills, {int(headline['wins'])}-{int(headline['losses'])}, WR {headline['win_rate_pct']:.2f}%, PF {headline['profit_factor']:.4f}, {headline['net_return_points']:+.4f} net points and Rs {headline['net_pnl_rs']:+,.2f} modeled P&L**. Daily MDD is {float(reference_row['max_daily_drawdown_points']):.4f} points.", "",
        f"Under 25 bps costs plus 5 bps entry slippage, the result remains positive: PF {float(harsh_row['profit_factor']):.4f}, {float(harsh_row['net_return_points']):+.4f} points and Rs {float(harsh_row['net_pnl_rs']):+,.2f}.", "",
        f"The evidence is concentrated: July supplies {float(july_row['net_return_points']) / float(headline['net_return_points']) * 100:.2f}% of net points, and {eod_fills} last-real-bar exits supply {eod_share:.2f}%. The six-session extension earns {float(forward_row['net_return_points']):+.4f} points but contributed to variant selection.", "",
        f"V12 changes only two five-minute filters: 09:40 SHORT and 09:45 SHORT require volume ratio >= 1.50. Versus V11, only {len(affected_days)} of 65 daily results change. The staged paired bootstrap therefore matters more than the headline improvement.", "",
        f"Across the corrected exploratory tests, **{len(significant_winner)}** numeric features separate winners from losers at BH q < 0.05, while **{len(significant_fill)}** separate filled from non-filled candidates. These are post-selection associations, not permission to change thresholds.", "",
        "## 1. Integrity, data contract and scope", "",
        f"- All **{validation['artifact_inventory']['artifact_count']}** inventoried standalone artifacts passed size, hash and file-set validation.",
        f"- Profile, registry, resolved configuration and input bindings revalidated: `{validation['profile_sha256']}`, `{validation['registry_sha256']}`, `{validation['resolved_config_sha256']}`, `{validation['input_binding_sha256']}`.",
        f"- Calendar span contains {len(expected_span)} expected regular sessions; missing validated session: **{', '.join(item.isoformat() for item in missing_sessions)}**.",
        f"- Strict source completeness failed for **{source_incomplete:,} of {source_expected:,} symbol-sessions ({source_incomplete / source_expected * 100:.2f}%)**. This is universe/path coverage, not the selected-candidate `data_incomplete_candidates`, which is zero.",
        f"- Every selected V12 candidate has a stored path; exact terminal coverage is {', '.join(f'{int(row.selected_candidates)} at {row.terminal_clock}' for row in path_terminal_summary.itertuples())}.",
        "- The candidate cache contains base-qualified five-minute rows; it does not retain every symbol that failed the base screen. Full filter counterfactuals require rebuilding the complete source stream.",
        "- Futures OI drives selection, while price, EMA, volume, confirmation, entry, stop, target and P&L use NSE cash-equity bars.", "",
        "### Source segments", "",
    ])
    add(v11_report._table(source_segments, ["segment_id", "from_day", "through_day", "contract_month", "universe_master_date", "sessions", "candidates", "expected_symbol_sessions", "source_incomplete_symbol_sessions", "source_incomplete_pct", "headline_source_complete"]))
    add(["", "### Validity tests blocked by the current data contract", ""])
    add(v11_report._table(blocked_tests, ["stage_id", "test_id", "status", "reason"]))

    add(["", "## 2. Exact strategy and parameter values", "", "### Global overlays and economics", ""])
    add(v11_report._table(global_parameters, list(global_parameters.columns)))
    add(["", "### Five-minute selection book", ""])
    add(v11_report._table(setup_parameters, ["setup_id", "signal_end", "side", "max_entries", "picker", "five_minute_ema_rule", "effective_move_rule", "five_minute_oi_change_min_pct", "five_minute_volume_ratio_min", "five_minute_traded_value_min_cr", "v12_changed_field"]))
    add(["", "`max_entries` is a setup/side/slot cap, not a daily cap. LONG and SHORT buckets are independent. Candidates are ranked by the setup picker, then the portfolio ledger applies chronological reservations.", "", "### One-minute confirmation and trade book", ""])
    add(v11_report._table(setup_parameters, ["setup_id", "one_minute_confirmation_body_ratio_min", "one_minute_confirmation_adverse_wick_ratio_max", "effective_close_location_min", "effective_max_confirmation_minute", "effective_earliest_fill_minute", "effective_buffer_bps", "effective_midpoint_invalidation", "entry_expiry_minute", "stop_pct", "target_pct", "post_confirmation_cancel", "allow_cap_reassignment", "same_bar_policy"]))
    add(["", "### Indicator definitions and causality", ""])
    add(v11_report._table(formula_reference, list(formula_reference.columns)))

    add(["", "## 3. Selection-to-exit funnel", "", "### Overall funnel", ""])
    add(v11_report._table(pd.DataFrame([{"step": key, "count": value} for key, value in funnel.items()]), ["step", "count"]))
    add(["", "### By setup", ""])
    add(v11_report._table(setup_metrics, ["setup_id", "raw_base_5m_candidates", "post_overlay_selected", "one_minute_confirmed", "fills", "wins", "losses", "win_rate_pct", "profit_factor", "net_return_points", "net_pnl_rs", "max_entries", "picker"]))
    add(["", "### By side", ""])
    add(v11_report._table(side_metrics, ["side", "raw_base_5m_candidates", "post_overlay_selected", "one_minute_confirmed", "fills", "wins", "losses", "win_rate_pct", "profit_factor", "net_return_points", "net_pnl_rs"]))
    add(["", "### By five-minute signal time", ""])
    add(v11_report._table(slot_metrics, ["signal_end", "raw_base_5m_candidates", "post_overlay_selected", "one_minute_confirmed", "fills", "wins", "losses", "win_rate_pct", "profit_factor", "net_return_points", "net_pnl_rs"]))
    add(["", "### By picker", ""])
    add(v11_report._table(picker_metrics, ["picker", "raw_base_5m_candidates", "post_overlay_selected", "one_minute_confirmed", "fills", "wins", "losses", "win_rate_pct", "profit_factor", "net_return_points", "net_pnl_rs"]))
    add(["", "### By frozen rank", ""])
    add(v11_report._table(rank_funnel, ["rank_bucket", "selected", "confirmed", "fills", "wins", "losses", "win_rate_pct", "profit_factor", "net_return_points", "net_pnl_rs"]))
    add(["", "Rank performance is non-monotonic. A weak observed rank cannot safely become a blacklist when later ranks remain profitable; any rank-margin hypothesis needs setup-stratified prospective replay."])
    add(["", "### V12 five-minute filter rejections", ""])
    add(v11_report._table(rejection_summary, ["selection_reason", "setup_id", "rejections", "affected_sessions", "median_price_change_pct", "median_volume_ratio"]))
    add(["", "### Candidate state and reason counts", ""])
    add(v11_report._table(status_counts, ["status", "count", "share_pct"]))
    add(v11_report._table(reason_counts, ["reason", "count", "share_pct"]))

    add(["", "## 4. Complete day-wise results", ""])
    add(v11_report._table(daily_detail, ["session_date", "raw_base_5m_candidates", "post_overlay_selected", "one_minute_confirmed", "fills", "wins", "losses", "win_rate_pct", "profit_factor", "net_return_points", "net_pnl_rs", "cumulative_net_return_points", "cumulative_net_pnl_rs", "drawdown_return_points", "drawdown_pnl_rs"]))
    add(["", "The supporting `daily_setup_performance.csv` expands every day into each five-minute setup. `selection_entry_exit_detail.csv` contains all selected candidates with five-minute indicators, confirmation candles, entry state and exits."])

    add(["", "## 5. Stability by period, month, week and weekday", "", "### Period slices", ""])
    add(v11_report._table(periods, list(periods.columns)))
    add(["", "### Monthly", ""])
    add(v11_report._table(monthly, list(monthly.columns)))
    add(["", "### Weekly", ""])
    add(v11_report._table(weekly, list(weekly.columns)))
    add(["", "### Weekday", ""])
    add(v11_report._table(weekday, list(weekday.columns)))
    add(["", "### Consecutive ten-session blocks", ""])
    add(v11_report._table(blocks, list(blocks.columns)))
    add(["", "### Daily activity/range/side regimes", ""])
    add(v11_report._table(daily_regimes, list(daily_regimes.columns)))
    add(["", "Higher candidate activity coincides with materially better results in this sample. That is a market-regime hypothesis, not evidence for a same-history minimum-breadth threshold."])
    if not rolling.empty:
        add(["", f"Rolling ten-session net P&L ranges from Rs {_numbers(rolling, 'net_pnl_rs').min():+,.2f} to Rs {_numbers(rolling, 'net_pnl_rs').max():+,.2f}. Full windows are in `rolling_10_session_metrics.csv`."])

    add(["", "## 6. V12 selection mechanism and comparison", "", "### Frozen comparators", ""])
    add(v11_report._table(comparator_summary, ["strategy", "sessions", "fills", "wins", "losses", "win_rate_pct", "profit_factor", "net_return_points", "net_pnl_rs", "max_daily_drawdown_points"]))
    if not paired.empty:
        add(["", "### Day-wise V12 minus V11 — changed sessions", ""])
        add(v11_report._table(affected_days, ["session_date", "fills_v11", "fills_v12", "net_return_pct_v11", "net_return_pct_v12", "delta_net_return_points", "net_pnl_rs_v11", "net_pnl_rs_v12", "delta_net_pnl_rs", "cumulative_delta_net_return_points"]))
        add(["", "### Exact mechanism accounting", ""])
        add(v11_report._table(mechanism_summary, ["mechanism", "count", "wins", "losses", "profit_factor", "net_return_points_effect", "net_pnl_rs_effect"]))
        add(["", "### Changed fills/economics", ""])
        add(v11_report._table(changed_trades, ["candidate_id", "session_date", "setup_id", "side", "symbol", "change_type", "net_return_pct_v11", "net_return_pct_v12", "delta_net_return_points", "net_pnl_rs_v11", "net_pnl_rs_v12", "delta_net_pnl_rs"]))
    if not exclusion_groups.empty:
        add(["", "### V11 counterfactual outcomes among candidates excluded by V12", ""])
        add(v11_report._table(exclusion_groups, ["setup_id", "v11_candidates_removed_by_v12", "v11_confirmed", "fills", "wins", "losses", "win_rate_pct", "profit_factor", "net_return_points", "net_pnl_rs"]))
        add(["", "These are V11-control outcomes for candidates removed by V12, not outcomes observed after rejection. Portfolio displacement means their arithmetic total is descriptive rather than a causal decomposition."])
    if not selected_gate.empty:
        add(["", "### Predeclared development gate for selected V12", ""])
        gate_columns = ["variant_id", "affected_decisions", "net_ratio_reference_15_0", "pf_delta_reference_15_0", "net_ratio_stress_25_5", "pf_delta_stress_25_5", "reference_mdd_ratio", "reference_fill_retention", "ex_july_delta_points", "forward_extension_delta_points", "both_sides_harsh_positive", "gate_status", "observed_rank", "gate_passing_rank"]
        add(v11_report._table(selected_gate, gate_columns))
    if not selected_reference_bootstrap.empty:
        add(["", "### Paired V12-minus-V11 uncertainty", ""])
        add(v11_report._table(selected_reference_bootstrap, ["scenario", "paired_sessions", "observed_delta_net_points", "observed_delta_net_pnl_rs", "bootstrap_delta_sum_p025", "bootstrap_delta_sum_median", "bootstrap_delta_sum_p975", "bootstrap_probability_delta_positive", "positive_delta_sessions", "negative_delta_sessions", "zero_delta_sessions", "max_cumulative_delta_drawdown_points"]))
        add(["", "The interval crossing zero means V12's incremental advantage over V11 is not statistically decisive. This bootstrap is conditional on the selected history and does not correct the 39-challenger winner-selection process."])
    if not variant_leaderboard.empty:
        add(["", "### Top observed V12 variants on the development history", ""])
        add(v11_report._table(variant_leaderboard.head(15), ["variant_id", "stage_id", "family", "fills", "win_rate_pct", "profit_factor", "net_return_points", "net_pnl_rs", "max_daily_drawdown_points", "gate_status", "observed_rank", "gate_passing_rank"]))

    add(["", "## 7. Cost and slippage robustness", ""])
    add(v11_report._table(scenario_metrics, ["scenario", "cost_bps", "slippage_bps", "fills", "wins", "losses", "win_rate_pct", "profit_factor", "net_return_points", "net_pnl_rs", "max_daily_drawdown_points", "net_pnl_retained_vs_reference_pct"]))
    add(["", "### Setup-level reference versus harsh stress", ""])
    add(v11_report._table(setup_robustness, ["setup_id", "fills_reference", "wins_reference", "losses_reference", "profit_factor_reference", "net_return_points_reference", "profit_factor_harsh", "net_return_points_harsh", "net_pnl_rs_harsh"]))
    add(["", "Fixed bps cases do not reproduce bid/ask spread, futures basis, depth, latency, partial fills, rejects or exit impact."])

    add(["", "## 8. Five-minute indicator study", "", "### Cohort distributions", ""])
    add(v11_report._table(indicator_cohorts, ["indicator", "cohort", "observations", "mean", "median", "p25", "p75"]))
    add(["", "### Winner versus loser medians", ""])
    add(v11_report._table(winner_loser, list(winner_loser.columns)))
    add(["", "### Multiple-test-corrected comparisons", ""])
    add(v11_report._table(indicator_tests, ["comparison", "indicator", "positive_observations", "negative_observations", "positive_median", "negative_median", "auc_positive_higher", "p_value_two_sided", "bh_q_value"]))
    add(["", "AUC around 0.5 indicates weak univariate separation. The pooled fill tests can also reflect differences in setup/time composition; fill probability is not accuracy. Quartiles and fixed bins are exploratory; selecting a favorable boundary from these tables and reporting it on the same history would be leakage.", "", "### Correlation with realized net return", ""])
    add(v11_report._table(correlations, list(correlations.columns)))
    add(["", "### Data-derived quartiles", ""])
    add(v11_report._table(quartiles, ["indicator", "quartile", "observed_range", "selected", "confirmed", "fills", "fill_rate_pct", "wins", "losses", "win_rate_pct", "profit_factor", "net_return_points", "net_pnl_rs"]))
    for indicator in indicator_bins["indicator"].drop_duplicates().tolist():
        add(["", f"### Fixed bins — `{indicator}`", ""])
        add(v11_report._table(indicator_bins.loc[indicator_bins["indicator"].eq(indicator)], ["bin", "selected", "confirmed", "fills", "confirmation_rate_pct", "fill_rate_pct", "wins", "losses", "win_rate_pct", "profit_factor", "net_return_points", "net_pnl_rs"]))

    add(["", "## 9. One-minute confirmation and entry timing", "", "### Confirmation minute", ""])
    add(v11_report._table(confirmation_metrics, list(confirmation_metrics.columns)))
    add(["", "### Entry minute", ""])
    add(v11_report._table(entry_metrics, list(entry_metrics.columns)))
    add(["", "### Setup by confirmation minute", ""])
    add(v11_report._table(setup_confirmation, ["setup_confirmation", "fills", "wins", "losses", "win_rate_pct", "profit_factor", "net_return_points", "net_pnl_rs"]))
    add(["", "### One-minute rejection codes", ""])
    add(v11_report._table(one_minute_rejections, list(one_minute_rejections.columns)))
    add(["", "Counts are failed-check occurrences across monitored candles; codes can overlap and one candidate can contribute more than once. Confirmation and entry minute are causal features, but any new timing rule must be replayed inside each setup. A global minute ban can remove profitable legs along with weak ones."])

    add(["", "## 10. Gap guard, portfolio and exposure", "", "### Gap paths", ""])
    add(v11_report._table(gap_path_metrics, ["gap_guard_path", "candidates", "fills", "median_adverse_gap_bps", "wins", "losses", "win_rate_pct", "profit_factor", "net_return_points", "net_pnl_rs"]))
    add(["", "A real resting stop-market order cannot reject a gap after the opening price is observed. The 2 bps Gap2 rule needs an explicitly executable synthetic-trigger or stop-limit design before live use.", "", "### Portfolio actual versus unconstrained", ""])
    add(v11_report._table(portfolio_comparison, list(portfolio_comparison.columns)))
    add(["", "### Portfolio rejections", ""])
    add(v11_report._table(portfolio_rejections, ["candidate_id", "session_date", "setup_id", "side", "symbol", "portfolio_reject_reason", "unconstrained_status", "unconstrained_net_return_pct", "unconstrained_net_pnl_rs"]))
    add(["", "### Exposure", ""])
    add(v11_report._table(portfolio_exposure, list(portfolio_exposure.columns)))
    add(["", "These exposure figures are cash-equivalent proxies, not futures capital or margin usage."])

    add(["", "## 11. Exits, holding time and excursions", "", "### Exit reason", ""])
    add(v11_report._table(exit_metrics, list(exit_metrics.columns)))
    add(["", f"The {eod_fills} last-real-bar exits contribute {eod_points:+.4f} points ({eod_share:.2f}% of total). Exit reason, holding time and MFE/MAE are realized outcomes and cannot be used directly as entry filters.", "", "### Selected-candidate source path terminal coverage", ""])
    add(v11_report._table(path_terminal_summary, list(path_terminal_summary.columns)))
    add(["", "### Economic dependence on terminal policy", ""])
    add(v11_report._table(terminal_dependency, ["terminal_view", "fills", "wins", "losses", "win_rate_pct", "profit_factor", "net_return_points", "net_pnl_rs"]))
    add(["", "`TARGET_AND_STOP_ONLY` remains positive, but materially weaker. The 15:15 slice measures the direct effect of incomplete terminal coverage; all last-real-bar exits additionally depend on the decision to hold unresolved positions to the known terminal close.", "", "### Terminal clock among last-real-bar exits", ""])
    add(v11_report._table(terminal_paths, list(terminal_paths.columns)))
    add(["", "### Holding duration", ""])
    add(v11_report._table(holding_metrics, list(holding_metrics.columns)))
    add(["", "### MFE/MAE bounds by outcome", ""])
    add(v11_report._table(excursion_by_outcome, list(excursion_by_outcome.columns)))
    add(["", "### Losing trades that reached favorable R thresholds", ""])
    add(v11_report._table(excursion_thresholds, list(excursion_thresholds.columns)))
    add(["", "### OHLC excursion quality", ""])
    add(v11_report._table(excursion_quality, list(excursion_quality.columns)))
    add(["", "Minute OHLC cannot reveal the exact high/low sequence around entry and exits. Excursion-based stop or trailing research should wait for repaired tick/event paths."])

    add(["", "## 12. Symbols, concentration and extreme trades", ""])
    add(v11_report._table(concentration, list(concentration.columns)))
    add(["", "### Top 15 symbols", ""])
    add(v11_report._table(symbol_metrics.head(15), ["symbol", "fills", "wins", "losses", "win_rate_pct", "profit_factor", "net_return_points", "net_pnl_rs"]))
    add(["", "### Bottom 15 symbols", ""])
    add(v11_report._table(symbol_metrics.tail(15).sort_values("net_pnl_rs"), ["symbol", "fills", "wins", "losses", "win_rate_pct", "profit_factor", "net_return_points", "net_pnl_rs"]))
    add(["", "### Best 15 trades", ""])
    add(v11_report._table(best_trades, ["session_date", "setup_id", "side", "symbol", "entry_time", "exit_time", "exit_reason", "net_return_pct", "net_pnl_rs", "mfe_pct_ohlc_lower_bound", "mae_pct_ohlc_upper_bound"]))
    add(["", "### Worst 15 trades", ""])
    add(v11_report._table(worst_trades, ["session_date", "setup_id", "side", "symbol", "entry_time", "exit_time", "exit_reason", "net_return_pct", "net_pnl_rs", "mfe_pct_ohlc_lower_bound", "mae_pct_ohlc_upper_bound"]))
    add(["", "Symbol blacklists or whitelists are not supported by small, selected samples and would introduce survivorship risk."])

    add(["", "## 13. Risk and statistical uncertainty", "", "### Risk summary", ""])
    add(v11_report._table(risk_summary, list(risk_summary.columns)))
    add(["", "### Drawdown episodes", ""])
    add(v11_report._table(drawdown_episodes, list(drawdown_episodes.columns)))
    add(["", "### IID session bootstrap — conditional on this sample", ""])
    add(v11_report._table(bootstrap, list(bootstrap.columns)))
    add(["", "### Random ordering of the realized daily P&Ls", ""])
    add(v11_report._table(order_drawdown, list(order_drawdown.columns)))
    add(["", "Resampling quantifies conditional sample/order uncertainty only. It does not repair model selection, static-universe bias, missing paths, cash-versus-futures mismatch or live execution risk."])

    add(["", "## 14. What is supported and what is not", "",
         "- **Supported descriptively:** the sealed V12 run reproduces exactly; all three cost cases remain positive; the late-SHORT volume filter reduces observed drawdown and slightly improves the selected-history result versus V11.",
         "- **Not established:** live futures profitability, untouched out-of-sample accuracy, causal superiority over V11, or the profitability of rejected candidates after portfolio displacement.",
         "- **Main statistical risk:** V12 was chosen after 39 isolated challengers; only eight daily results differ from V11 and the paired reference interval crosses zero.",
         "- **Main data risk:** incomplete symbol-session coverage, one missing regular session, static/potentially future-known universes and mixed terminal times.",
         "- **Main execution risk:** cash-equity paths and proxy sizing replace rolling futures contracts, lots, margins, spread and market impact.",
         "- **Indicators not present:** ATR, RSI, ADX, VWAP, point-in-time index/sector regime, opening breadth and order-book liquidity are not part of frozen V12.",
         "- **Main report discipline:** indicator bins, symbol tables, exit reasons, MFE and MAE are hypothesis generators, not post-hoc filters."])

    add(["", "## 15. Safe staged improvement plan", "",
         "### Stage A — freeze the comparator set", "",
         "1. Preserve the exact V10, V11 and V12 hashes. Register every future test before reading its result.",
         "2. Use V11 as control and V12 as challenger. Do not replace the control because V12 has the best observed drawdown.", "",
         "### Stage B — repair market-data validity", "",
         "1. Reconstruct daily point-in-time F&O membership and deterministic front-month rolls.",
         "2. Bind actual futures one-minute/tick price and OI, dated lots/ticks/margins, complete session paths and a verified pre-close exit.",
         "3. Re-run V10/V11/V12 on the common repaired input and reject improvements that disappear.", "",
         "### Stage C — prospective mechanism validation", "",
         "1. Freeze volume ratio 1.50 and collect genuinely new sessions without tuning.",
         "2. Record V11 and V12 decisions side by side, especially the late-SHORT exclusions and all portfolio displacement.",
         "3. Require enough affected decisions, not merely 100 total fills; most V11/V12 trades are identical.", "",
         "### Stage D — five-minute quality research", "",
         "1. Treat the 1.50 late-SHORT volume rule as the only active hypothesis. Do not select another threshold from this report.",
         "2. If new data supports it, predeclare one setup-specific test involving prior-OI quality, relative rank margin or market/sector context. Use point-in-time inputs only.",
         "3. Apply multiple-testing control and preserve the complete candidate stream so rejected-candidate counterfactuals remain available.", "",
         "### Stage E — one-minute entry research", "",
         "1. Test setup-specific confirmation/entry timing only after reviewing prospective V11/V12 parity.",
         "2. Keep confirmation-bar non-fill, tick rounding, cancellations and portfolio reservations identical between replay and paper.", "",
         "### Stage F — executable gap, cost and risk model", "",
         "1. Replace Gap2 with an executable policy: accept stop-market gaps, model stop-limit non-fills, or use a synthetic trigger with measured latency.",
         "2. Model both entry and exit spread/impact, partial fills, rejects, broker margins and actual futures lots.",
         "3. Add daily-loss, gross exposure, sector concentration and kill-switch gates to a separate fail-closed paper adapter.", "",
         "### Stage G — exit research after path repair", "",
         "1. Resolve the mixed 15:15/15:30 boundary before testing time stops, break-even or trailing rules.",
         "2. Use tick/event paths for intrabar ordering and predeclare each exit hypothesis.", "",
         "### Promotion rule", "",
         "Promote nothing unless V12 beats V11 on untouched repaired futures data, remains positive in both stress cases, preserves drawdown and concentration, and achieves decision/fill parity in shadow and paper execution. Otherwise retain V11/V12 as research controls."])

    add(["", "## 16. Reproducibility and supporting evidence", "", "Backtest command used:", "", "```powershell",
         f'cd "{Path(__file__).resolve().parent}"',
         "python -u fno_v12_backtest.py run --all-usable-history", "```", "", "Validation command:", "", "```powershell",
         f'python -u fno_v12_backtest.py validate --provenance "{provenance_path}"', "```", "", "Report command:", "", "```powershell",
         f'python -u fno_v12_full_historical_report.py --source-run "{source_run}" --lineage-run "{lineage_run}" --report "{report_path}" --assets-dir "{assets_dir}"', "```", "",
         f"Supporting tables and charts: `{assets_dir}`.",
         f"The sealed run was validated and read but not modified. This report writes **{len(asset_frames)} CSV tables** and **{len(chart_paths)} charts** outside it.", "",
         "## 17. Glossary", "",
         "- **Net return points:** arithmetic sum of per-trade net percentage returns; not compounded portfolio return.",
         "- **PF:** gross positive net-return points divided by absolute gross negative net-return points.",
         "- **MDD:** maximum peak-to-trough drawdown of cumulative daily summed return points unless marked Rs.",
         "- **WR:** winning closed trades divided by closed trades.",
         "- **S+N:** the Nth completed one-minute bar after the five-minute signal closes.",
         "- **MFE/MAE:** bounded favorable/adverse excursion after entry; future outcome data, not an entry feature.",
         "- **BH q-value:** multiple-test-adjusted p-value; low q reduces but does not eliminate false-discovery risk.",
         "- **Research-only:** reproducible hypothesis evidence without paper/live authority or a claim of achievable returns."])

    common.atomic_write_text(report_path, "\n".join(lines) + "\n")
    output_paths.append(report_path)
    manifest_path = assets_dir / "report_manifest.json"
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "generated_at_ist": datetime.now(common.IST),
        "source_run": str(source_run),
        "source_provenance_sha256": _sha256_file(provenance_path),
        "source_validation": validation,
        "profile_id": v12.PROFILE_ID,
        "profile_sha256": v12.LOCKED_PROFILE_SHA256,
        "input_binding_sha256": provenance["input_binding_sha256"],
        "lineage_run": str(lineage_run) if lineage_run else None,
        "lineage_provenance_sha256": _sha256_file(lineage_run / "provenance.json") if lineage_run else None,
        "report": str(report_path),
        "report_sha256": _sha256_file(report_path),
        "outputs": [
            {"path": str(path.resolve()), "size_bytes": path.stat().st_size, "sha256": _sha256_file(path)}
            for path in output_paths
        ],
        "research_only": True,
        "promotion_eligible": False,
        "live_or_paper_authority": False,
    }
    common.atomic_write_json(manifest_path, v11_report.gaps._json_ready(manifest))
    return {
        "report": report_path, "assets_dir": assets_dir, "manifest": manifest_path,
        "csv_tables": len(asset_frames), "charts": len(chart_paths), "headline": headline,
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-run", type=Path)
    parser.add_argument("--lineage-run", type=Path)
    parser.add_argument("--skip-lineage", action="store_true")
    parser.add_argument("--report", type=Path, default=Path("report_v12.md"))
    parser.add_argument("--assets-dir", type=Path, default=Path("report_v12_assets"))
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    source_run = args.source_run or _resolve_latest(v12.OUTPUT_ROOT)
    if source_run is None:
        raise FileNotFoundError("no completed standalone V12 run was found")
    lineage_run = None if args.skip_lineage else (args.lineage_run or _resolve_latest(DEFAULT_LINEAGE_ROOT))
    result = build_report(
        source_run=source_run, report_path=args.report, assets_dir=args.assets_dir, lineage_run=lineage_run
    )
    print(f"[V12-FULL-REPORT] complete: {result['report']}", flush=True)
    print(f"[V12-FULL-REPORT] supporting outputs: {result['csv_tables']} CSV + {result['charts']} charts", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
