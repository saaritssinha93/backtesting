"""Freeze and audit the frequency-balanced one-month LONG research candidate.

This delivery step does not run another search.  It selects the highest-scoring
distinct trade list from the completed 200,000-rule post-hoc ledger that also
has a median of at least three trades per session and is active on all 22
sessions.  All outputs remain research-only and production-disabled.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict
from pathlib import Path
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd

import research_v12_one_month_long_logic_optimizer_v9 as v9
import research_v12_one_month_long_logic_posthoc_v10 as v10


V9_DIR = v9.OUTPUT_DIR
V10_DIR = v10.OUTPUT_DIR
TOTAL_SEARCHED_CONFIGURATIONS = 300_000


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _verify_integrity_manifest(root: Path) -> dict[str, Any]:
    path = root / "integrity_manifest.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    failures: list[str] = []
    checked = 0
    for relative, metadata in payload.get("artifacts", {}).items():
        target = root / relative
        checked += 1
        if (
            not target.exists()
            or _sha256(target) != metadata.get("sha256")
            or target.stat().st_size != int(metadata.get("bytes", -1))
        ):
            failures.append(str(target))
    for target_text, expected_hash in payload.get("sources", {}).items():
        target = Path(target_text)
        checked += 1
        if not target.exists() or _sha256(target) != expected_hash:
            failures.append(str(target))
    return {"checked": checked, "failures": failures, "passed": not failures}


def _period_rows(
    trades: pd.DataFrame,
    splits: Mapping[str, Sequence[str]],
) -> tuple[dict[str, dict[str, Any]], pd.DataFrame]:
    periods = {
        "development_slice": v9.detailed_performance(trades, splits["development"]),
        "validation_slice": v9.detailed_performance(trades, splits["validation"]),
        "formerly_locked_test_slice": v9.detailed_performance(trades, splits["test"]),
        "full_month_in_sample": v9.detailed_performance(trades, splits["all"]),
    }
    rows = []
    for label, metric in periods.items():
        rows.append({"period": label, **metric})
    return periods, pd.DataFrame(rows)


def _weekly_rows(
    arrays: v9.SearchArrays,
    selected: np.ndarray,
) -> pd.DataFrame:
    bounds = ((0, 5), (5, 10), (10, 15), (15, 20), (20, 22))
    rows = []
    for ordinal, (start, end) in enumerate(bounds, 1):
        metric = v9.performance_from_indices(arrays, selected, range(start, end))
        rows.append({
            "block": ordinal,
            "start_date": arrays.sessions[start],
            "end_date": arrays.sessions[end - 1],
            **metric,
        })
    return pd.DataFrame(rows)


def _candidate_snapshot(row: pd.Series, label: str) -> dict[str, Any]:
    return {
        "candidate_role": label,
        "config_id": str(row["config_id"]),
        "trades": int(row["full_trades"]),
        "trades_per_session": float(row["full_trades_per_session"]),
        "median_trades_per_session": float(row["full_median_trades_per_session"]),
        "active_days": int(row["full_active_days"]),
        "net_pnl_rs": float(row["full_net_pnl_rs"]),
        "profit_factor": float(row["full_profit_factor"]),
        "max_drawdown_rs": float(row["full_max_drawdown_rs"]),
        "positive_weeks": int(row["positive_weeks"]),
        "profit_factor_at_1p5x_cost": float(row["cost1p5_profit_factor"]),
        "robust_score": float(row["robust_score"]),
    }


def _filter_audit(trades: pd.DataFrame, config: v9.RuleConfig) -> dict[str, int]:
    numeric = lambda name: pd.to_numeric(trades[name], errors="coerce")
    checks = {
        "primary_side_not_long": ~trades["primary_side"].astype(str).str.upper().eq("LONG"),
        "rank_outside_range": ~numeric("selection_rank").between(config.rank_min, config.rank_max),
        "signal_minute_outside_range": ~numeric("signal_minute").between(
            config.signal_minute_min, config.signal_minute_max
        ),
        "atr_below_minimum": numeric("atr_pct").lt(config.atr_pct_min),
        "session_return_below_minimum": numeric("session_return_so_far_pct").lt(
            config.session_return_min
        ),
        "vwap_distance_below_minimum": numeric("vwap_dist_atr").lt(config.vwap_dist_atr_min),
        "close_position_below_minimum": numeric("close_position_in_bar").lt(
            config.close_position_min
        ),
        "range_below_minimum": numeric("range_pct").lt(float(config.range_pct_min)),
        "ret_5m_below_minimum": numeric("ret_5m_pct").lt(float(config.ret_5m_min)),
        "ret_5m_above_maximum": numeric("ret_5m_pct").gt(float(config.ret_5m_max)),
        "ema20_distance_below_minimum": numeric("ema20_dist_atr").lt(
            float(config.ema20_dist_atr_min)
        ),
        "score_margin_below_minimum": numeric("score_margin").lt(float(config.score_margin_min)),
        "previous_ret_5m_above_maximum": numeric("previous_ret_5m_pct").gt(
            float(config.previous_ret_5m_max)
        ),
        "previous_bar_not_contiguous": ~trades["contiguous_previous"].fillna(False).astype(bool),
    }
    if config.ret_30m_min is not None:
        checks["ret_30m_below_minimum"] = numeric("ret_30m_pct").lt(
            float(config.ret_30m_min)
        )
    return {name: int(mask.fillna(True).sum()) for name, mask in checks.items()}


def _markdown_table(frame: pd.DataFrame, columns: Sequence[str]) -> str:
    labels = list(columns)
    header = "| " + " | ".join(labels) + " |"
    separator = "|" + "|".join(["---"] * len(labels)) + "|"
    rows = [header, separator]
    for _, record in frame.loc[:, labels].iterrows():
        rows.append("| " + " | ".join(str(value) for value in record.tolist()) + " |")
    return "\n".join(rows)


def main() -> int:
    splits = v9.session_calendar()
    exact_path = V9_DIR / "exact_candidate_universe.parquet"
    raw_path = V9_DIR / "entry_engine_raw.parquet"
    cache_manifest_path = V9_DIR / "exact_cache_manifest.json"
    exact = pd.read_parquet(exact_path)
    raw = pd.read_parquet(raw_path)
    v9.validate_exact_cache_manifest(cache_manifest_path, exact, raw)

    ledger = pd.read_parquet(V10_DIR / "posthoc_local_trial_ledger.parquet")
    distinct = (
        ledger.loc[ledger["robustness_gate"].eq(True)]
        .sort_values(["robust_score", "full_profit_factor"], ascending=False)
        .drop_duplicates("entry_signature")
    )
    balanced = distinct.loc[
        distinct["full_median_trades_per_session"].ge(3.0)
        & distinct["full_active_days"].eq(22)
    ]
    if balanced.empty:
        raise RuntimeError("no all-session candidate with median >=3 trades/session")
    balanced_row = balanced.iloc[0]
    config = v10._normalise_config_row(balanced_row)

    arrays = v9.SearchArrays(exact, splits["all"])
    selected = arrays.selected_indices(config)
    if v9._signature(selected) != str(balanced_row["entry_signature"]):
        raise RuntimeError("frequency-balanced ledger signature does not reproduce")
    trades = v9.selected_trade_frame(exact, selected)
    daily = v9.daily_results(trades, splits["all"])
    periods, period_frame = _period_rows(trades, splits)
    full = periods["full_month_in_sample"]
    weekly = _weekly_rows(arrays, selected)
    cost_stress = pd.DataFrame([
        v9.detailed_performance(trades, splits["all"], cost_multiplier=value)
        for value in (1.0, 1.25, 1.5, 2.0)
    ])
    perturbations = []
    for label, candidate in v9.perturbation_configs(config):
        candidate_trades = v9.selected_trade_frame(exact, arrays.selected_indices(candidate))
        perturbations.append({
            "stress": label,
            "config_sha256": v9.config_hash(candidate),
            **v9.detailed_performance(candidate_trades, splits["all"]),
        })
    perturbation_frame = pd.DataFrame(perturbations)
    bootstrap = v10.day_block_bootstrap(
        trades,
        splits["all"],
        searched_configurations=TOTAL_SEARCHED_CONFIGURATIONS,
    )

    pf_first = distinct.iloc[0]
    high_frequency_pool = distinct.loc[
        distinct["full_trades_per_session"].ge(4.0)
        & distinct["full_median_trades_per_session"].ge(3.0)
        & distinct["full_active_days"].eq(22)
    ]
    if high_frequency_pool.empty:
        raise RuntimeError("no robust higher-frequency alternative")
    high_frequency = high_frequency_pool.iloc[0]
    comparison = pd.DataFrame([
        _candidate_snapshot(pf_first, "PF-first numerical fit"),
        _candidate_snapshot(balanced_row, "Recommended frequency-balanced fit"),
        _candidate_snapshot(high_frequency, "Higher-frequency alternative"),
    ])

    signal_time = pd.to_datetime(trades["signal_time_ist"], errors="raise", utc=True).dt.tz_convert(
        "Asia/Kolkata"
    )
    hourly = (
        trades.assign(signal_hour_ist=signal_time.dt.hour)
        .groupby("signal_hour_ist", as_index=False)
        .agg(
            trades=("ticker", "size"),
            net_pnl_rs=("net_pnl_rs", "sum"),
            winners=("net_pnl_rs", lambda values: int((pd.to_numeric(values) > 0).sum())),
        )
    )
    hourly["win_rate_pct"] = hourly["winners"] / hourly["trades"] * 100.0

    pnl = pd.to_numeric(trades["net_pnl_rs"], errors="raise")
    expected_gross = (
        pd.to_numeric(trades["exit_price"], errors="raise")
        - pd.to_numeric(trades["entry_price"], errors="raise")
    ) * pd.to_numeric(trades["quantity"], errors="raise")
    expected_net = pd.to_numeric(trades["gross_pnl_rs"], errors="raise") - pd.to_numeric(
        trades["cost_rs"], errors="raise"
    )
    entry_time = pd.to_datetime(trades["entry_time_ist"], errors="raise", utc=True)
    exit_time = pd.to_datetime(trades["exit_time_ist"], errors="raise", utc=True)
    signal_timestamp = pd.to_datetime(trades["signal_time_ist"], errors="raise", utc=True)
    notional = pd.to_numeric(trades["entry_price"], errors="raise") * pd.to_numeric(
        trades["quantity"], errors="raise"
    )
    margin = notional / float(v9.v12.V7_INTRADAY_LEVERAGE)
    filter_violations = _filter_audit(trades, config)
    v9_integrity = _verify_integrity_manifest(V9_DIR)
    v10_integrity = _verify_integrity_manifest(V10_DIR)
    audit = {
        "all_checks_passed": False,
        "manifest_checks": {"v9": v9_integrity, "v10": v10_integrity},
        "reconciliation": {
            "trade_rows": len(trades),
            "unique_ticker_days": int(trades.drop_duplicates(["trade_date", "ticker"]).shape[0]),
            "active_sessions": int((daily["trades"] > 0).sum()),
            "maximum_trades_in_session": int(daily["trades"].max()),
            "gross_pnl_identity_max_abs_error": float(
                (expected_gross - pd.to_numeric(trades["gross_pnl_rs"], errors="raise")).abs().max()
            ),
            "net_pnl_identity_max_abs_error": float(
                (expected_net - pd.to_numeric(trades["net_pnl_rs"], errors="raise")).abs().max()
            ),
            "recomputed_net_pnl_rs": float(pnl.sum()),
            "reported_net_pnl_rs": float(full["net_pnl_rs"]),
            "entry_after_completed_signal_all_rows": bool((entry_time > signal_timestamp).all()),
            "exit_after_entry_all_rows": bool((exit_time >= entry_time).all()),
            "path_resolution_valid_all_rows": bool(trades["path_resolution_valid"].all()),
            "one_ticker_per_day": not trades.duplicated(["trade_date", "ticker"]).any(),
            "daily_cap_respected": bool(daily["trades"].le(v9.DAILY_CAP).all()),
            "selected_five_minute_fallback_rows": int(trades["path_fallback_applied"].sum()),
            "selected_source_max_window_incomplete_rows": int(
                trades["max_window_complete"].eq(False).sum()
            ),
        },
        "filter_violation_counts": filter_violations,
        "capital": {
            "intraday_leverage": float(v9.v12.V7_INTRADAY_LEVERAGE),
            "position_notional_min_rs": float(notional.min()),
            "position_notional_median_rs": float(notional.median()),
            "position_notional_max_rs": float(notional.max()),
            "estimated_margin_min_rs": float(margin.min()),
            "estimated_margin_median_rs": float(margin.median()),
            "estimated_margin_max_rs": float(margin.max()),
        },
        "search": {
            "generated_configurations": TOTAL_SEARCHED_CONFIGURATIONS,
            "distinct_robust_trade_lists": int(len(distinct)),
            "balanced_distinct_trade_lists": int(len(balanced)),
            "selection_uses_full_month": True,
        },
    }
    audit["all_checks_passed"] = bool(
        v9_integrity["passed"]
        and v10_integrity["passed"]
        and not any(filter_violations.values())
        and audit["reconciliation"]["unique_ticker_days"] == len(trades)
        and audit["reconciliation"]["active_sessions"] == 22
        and audit["reconciliation"]["daily_cap_respected"]
        and audit["reconciliation"]["entry_after_completed_signal_all_rows"]
        and audit["reconciliation"]["exit_after_entry_all_rows"]
        and audit["reconciliation"]["path_resolution_valid_all_rows"]
        and audit["reconciliation"]["gross_pnl_identity_max_abs_error"] < 1e-6
        and audit["reconciliation"]["net_pnl_identity_max_abs_error"] < 1e-3
        and abs(
            audit["reconciliation"]["recomputed_net_pnl_rs"]
            - audit["reconciliation"]["reported_net_pnl_rs"]
        ) < 1e-3
    )
    if not audit["all_checks_passed"]:
        raise RuntimeError("frequency-balanced delivery audit failed")

    honest = json.loads((V9_DIR / "summary.json").read_text(encoding="utf-8"))
    summary = {
        "research_only": True,
        "production_approved": False,
        "posthoc_in_sample": True,
        "requires_fresh_holdout": True,
        "verdict": "POSTHOC_FREQUENCY_BALANCED_CANDIDATE_NOT_VALIDATED",
        "selection_reason": (
            "Highest post-hoc robust score among distinct rules with median >=3 "
            "trades/session and activity on all 22 sessions."
        ),
        "searched_configurations": TOTAL_SEARCHED_CONFIGURATIONS,
        "champion": {**v9.json_safe(asdict(config)), "config_sha256": v9.config_hash(config)},
        "period_results": periods,
        "cost_stress": v9.json_safe(cost_stress.to_dict("records")),
        "weekly_results": v9.json_safe(weekly.to_dict("records")),
        "bootstrap": bootstrap,
        "perturbation_count": len(perturbation_frame),
        "perturbations_pf_ge_1p2": int(perturbation_frame["profit_factor"].ge(1.2).sum()),
        "execution_contract": {
            "hourly_prefilter_changed": False,
            "prefilter_primary_side": "LONG",
            "signal": "completed_5min",
            "entry": "exact_next_available_1min",
            "exit": "exact_1min_with_conservative_5min_gap_fallback",
            "stop_loss_pct": v9.STOP_LOSS_PCT,
            "target_pct": v9.TARGET_PCT,
            "statutory_costs": True,
            "one_ticker_per_day": True,
            "daily_cap": v9.DAILY_CAP,
            "same_bar_collision_policy": "STOP_FIRST",
            "max_drawdown_basis": "realized_exit_order",
        },
        "honest_search_verdict": honest["verdict"],
        "audit_passed": audit["all_checks_passed"],
    }

    trades.to_csv(V10_DIR / "balanced_candidate_trades.csv", index=False)
    daily.to_csv(V10_DIR / "balanced_daily_results.csv", index=False)
    hourly.to_csv(V10_DIR / "balanced_hourly_results.csv", index=False)
    weekly.to_csv(V10_DIR / "balanced_weekly_results.csv", index=False)
    period_frame.to_csv(V10_DIR / "balanced_period_results.csv", index=False)
    cost_stress.to_csv(V10_DIR / "balanced_cost_stress.csv", index=False)
    perturbation_frame.to_csv(V10_DIR / "balanced_logic_perturbations.csv", index=False)
    comparison.to_csv(V10_DIR / "candidate_tradeoff_comparison.csv", index=False)
    v10.write_posthoc_config(V10_DIR / "balanced_one_month_long_setup_conf.py", config)
    (V10_DIR / "balanced_summary.json").write_text(
        json.dumps(v9.json_safe(summary), indent=2), encoding="utf-8"
    )
    (V10_DIR / "final_validation_checks.json").write_text(
        json.dumps(v9.json_safe(audit), indent=2), encoding="utf-8"
    )

    honest_full = honest["results"]["full_month"]
    honest_test = honest["results"]["locked_test"]
    report_comparison = comparison.copy()
    for column in ("trades_per_session", "median_trades_per_session", "profit_factor"):
        report_comparison[column] = report_comparison[column].map(lambda value: f"{value:.3f}")
    report_comparison["net_pnl_rs"] = report_comparison["net_pnl_rs"].map(lambda value: f"Rs {value:,.0f}")
    report_comparison["max_drawdown_rs"] = report_comparison["max_drawdown_rs"].map(
        lambda value: f"Rs {value:,.0f}"
    )
    ret30_rule_text = (
        f"30-minute return >= **{config.ret_30m_min:.2f}%**"
        if config.ret_30m_min is not None
        else "no additional 30-minute return threshold"
    )
    report = f"""# Final one-month V12 LONG entry research

## Decision

The recommended numerical fit is `{config.config_id}`.  It produced **{full['trades']}
trades**, **{full['trades_per_session']:.2f}/session**, a **median of
{full['median_trades_per_session']:.1f}/session**, activity on **{full['active_days']}/22
sessions**, **PF {full['profit_factor']:.3f}**, **net Rs {full['net_pnl_rs']:,.0f}**, and
realized-exit-order max drawdown of **Rs {full['max_drawdown_rs']:,.0f}**.

This is **post-hoc, full-month, in-sample research** selected after
{TOTAL_SEARCHED_CONFIGURATIONS:,} configurations.  It is not a validated or
production-ready edge.  `PRODUCTION_APPROVED=False` and a genuinely fresh
forward holdout is mandatory.

## Why this candidate

The absolute PF-first fit had PF {float(pf_first['full_profit_factor']):.3f}, but
only a 2-trade median and one zero-trade day.  The recommended candidate gives
up only {float(pf_first['full_profit_factor']) - full['profit_factor']:.3f} PF while
raising the median to 3 and covering every session.

{_markdown_table(report_comparison, ['candidate_role', 'config_id', 'trades', 'trades_per_session', 'median_trades_per_session', 'active_days', 'net_pnl_rs', 'profit_factor', 'max_drawdown_rs'])}

## Entry configuration

- Existing hourly prefilter is unchanged; use only stocks marked `LONG` in that
  hour, ranks **{config.rank_min}-{config.rank_max}**.
- Completed 5-minute signal between **10:00 and 13:00 IST**.
- ATR >= **{config.atr_pct_min:.2f}%**; session return so far >=
  **{config.session_return_min:.2f}%**.
- Close at least **{config.vwap_dist_atr_min:.2f} ATR above VWAP** and
  **{config.ema20_dist_atr_min:.2f} ATR above EMA20**.
- Close position inside the signal bar >= **{config.close_position_min:.2f}**;
  signal-bar range >= **{config.range_pct_min:.2f}%**.
- Current 5-minute return between **{config.ret_5m_min:.2f}% and
  {config.ret_5m_max:.2f}%**; {ret30_rule_text}.
- LONG score margin >= **{config.score_margin_min:.3f}**.
- Previous contiguous 5-minute return <= **{config.previous_ret_5m_max:.2f}%**.
- Take the first chronological passing signal per ticker/day.  Cap at
  **{v9.DAILY_CAP} trades/day**.

This is a pullback-bounce structure: the previous completed bar is negative,
the current bar turns modestly positive, while the stock already has positive
session momentum and trades well above VWAP/EMA20.

## Execution contract

- Entry: exact next available 1-minute fill after the completed 5-minute signal.
- Exit: 1-minute path, fixed **1% stop / 2% target**; EOD exit if neither hits.
- Incomplete 1-minute grids: recompute the entire path on 5-minute bars;
  stop wins if a fallback bar touches stop and target together.
- Statutory costs, V12 risk sizing, {float(v9.v12.V7_INTRADAY_LEVERAGE):g}x
  intraday leverage, one ticker/day, and realized-exit-order drawdown.
- {int(trades['path_fallback_applied'].sum())}/{len(trades)} selected trades used
  the conservative 5-minute path fallback.

## Honest chronological result

The separate 100,000-rule chronology selected only on development/validation
was rejected.  Its full-month result was {honest_full['trades']} trades,
{honest_full['trades_per_session']:.2f}/session, PF
{honest_full['profit_factor']:.3f}, and net Rs {honest_full['net_pnl_rs']:,.0f}.
Most importantly, its frozen five-session test lost Rs
{abs(honest_test['net_pnl_rs']):,.0f} at PF {honest_test['profit_factor']:.3f}.
That failed holdout is the reliable conclusion; the stronger balanced result
above is a candidate to test next, not proof of future profitability.

## Stress and integrity

- At 1.5x costs: PF {float(cost_stress.loc[cost_stress['cost_multiplier'].eq(1.5), 'profit_factor'].iloc[0]):.3f},
  net Rs {float(cost_stress.loc[cost_stress['cost_multiplier'].eq(1.5), 'net_pnl_rs'].iloc[0]):,.0f}.
- At 2.0x costs: PF {float(cost_stress.loc[cost_stress['cost_multiplier'].eq(2.0), 'profit_factor'].iloc[0]):.3f},
  net Rs {float(cost_stress.loc[cost_stress['cost_multiplier'].eq(2.0), 'net_pnl_rs'].iloc[0]):,.0f}.
- {int(weekly['net_pnl_rs'].gt(0).sum())}/5 calendar blocks were profitable.
- {int(perturbation_frame['profit_factor'].ge(1.2).sum())}/{len(perturbation_frame)}
  threshold perturbations retained PF >=1.2.
- All filter, P&L identity, path, sizing, one-ticker/day, cap, cache-provenance,
  and artifact-integrity checks passed.

## Files

- `balanced_one_month_long_setup_conf.py`: exact research configuration.
- `balanced_candidate_trades.csv`: all selected trades and entry/exit details.
- `balanced_daily_results.csv`: all 22 session counts and P&L.
- `balanced_summary.json`: complete metrics and execution contract.
- `final_validation_checks.json`: independent reconciliation and integrity audit.
"""
    (V10_DIR / "FINAL_ONE_MONTH_LONG_RESEARCH_REPORT.md").write_text(
        report, encoding="utf-8"
    )

    artifact_names = [
        "balanced_candidate_trades.csv",
        "balanced_daily_results.csv",
        "balanced_hourly_results.csv",
        "balanced_weekly_results.csv",
        "balanced_period_results.csv",
        "balanced_cost_stress.csv",
        "balanced_logic_perturbations.csv",
        "candidate_tradeoff_comparison.csv",
        "balanced_one_month_long_setup_conf.py",
        "balanced_summary.json",
        "final_validation_checks.json",
        "FINAL_ONE_MONTH_LONG_RESEARCH_REPORT.md",
    ]
    delivery_manifest = {
        "artifacts": {
            name: {"sha256": _sha256(V10_DIR / name), "bytes": (V10_DIR / name).stat().st_size}
            for name in artifact_names
        },
        "sources": {
            str(Path(__file__).resolve()): _sha256(Path(__file__).resolve()),
            str(Path(v9.__file__).resolve()): _sha256(Path(v9.__file__).resolve()),
            str(Path(v10.__file__).resolve()): _sha256(Path(v10.__file__).resolve()),
            str(exact_path.resolve()): _sha256(exact_path),
            str(cache_manifest_path.resolve()): _sha256(cache_manifest_path),
        },
        "audit_passed": True,
    }
    (V10_DIR / "final_delivery_manifest.json").write_text(
        json.dumps(v9.json_safe(delivery_manifest), indent=2), encoding="utf-8"
    )
    print(json.dumps(v9.json_safe(summary), indent=2), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
