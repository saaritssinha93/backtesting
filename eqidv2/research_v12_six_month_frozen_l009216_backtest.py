"""Exact six-month V12 replay of frozen L009216_PULLBACK_BOUNCE.

The configuration is loaded from the previously frozen research artifact and
is never optimized on the six-month outcomes.  The replay uses completed 5m
signals, exact next-available 1m entries, V12 risk sizing, statutory costs,
1m exits, and the same conservative 5m fallback for incomplete 1m paths.

The run fails closed unless its 2026-07-06 through 2026-08-04 subset exactly
reproduces the previously audited 66-trade result.
"""

from __future__ import annotations

import hashlib
import json
import runpy
from dataclasses import asdict, fields
from functools import lru_cache
from pathlib import Path
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd

import research_v12_one_month_long_logic_optimizer_v9 as v9
import research_v12_path_aware_long_rebuild as v2


PRODUCTION_APPROVED = False
RESEARCH_ONLY = True
OPTIMIZATION_PERFORMED = False

START_DATE = "2026-02-05"
END_DATE = "2026-08-04"
SELECTION_MONTH_START = "2026-07-06"
EXPECTED_SESSIONS = 120
EXPECTED_SOURCE_ROWS = 440_837
EXPECTED_FROZEN_SIGNAL_ROWS = 502
EXPECTED_FROZEN_TICKER_DAYS = 450
EXPECTED_FROZEN_ACTIVE_SESSIONS = 117
P_AND_L_ROUNDING_TOLERANCE_RS = 0.0001
EXPECTED_CONFIG_ID = "L009216_PULLBACK_BOUNCE"
EXPECTED_CONFIG_SHA256 = "7d5f02566c6bffc649395d22cc925dd3083079b3d63b308cf0d12258c3438310"

CONFIG_SOURCE = Path(
    r"C:\TradingData\eqidv2_experiments"
    r"\v12_one_month_long_logic_posthoc_v10_20260706_20260804"
    r"\balanced_one_month_long_setup_conf.py"
)
REFERENCE_SUMMARY = CONFIG_SOURCE.with_name("balanced_summary.json")
REFERENCE_TRADES = CONFIG_SOURCE.with_name("balanced_candidate_trades.csv")

OUTPUT_DIR = (
    Path(__file__).resolve().parent
    / "Train_and_Test"
    / "six_month_frozen_l009216_20260205_20260804"
)


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def load_frozen_configuration() -> tuple[v9.RuleConfig, dict[str, Any]]:
    if not CONFIG_SOURCE.exists():
        raise RuntimeError(f"frozen configuration is missing: {CONFIG_SOURCE}")
    contract = runpy.run_path(str(CONFIG_SOURCE))
    required = {
        "PRODUCTION_APPROVED": False,
        "RESEARCH_ONLY": True,
        "CONFIG_ID": EXPECTED_CONFIG_ID,
        "CONFIG_SHA256": EXPECTED_CONFIG_SHA256,
        "PREFILTER_JOB_CHANGED": False,
        "PREFILTER_PRIMARY_SIDE": "LONG",
        "SIGNAL_TIMEFRAME": "5min_completed_bar",
        "ENTRY_TIMEFRAME": "exact_next_available_1min",
        "EXIT_TIMEFRAME": "exact_1min_with_conservative_5min_gap_fallback",
        "STOP_LOSS_PCT": 1.0,
        "TARGET_PCT": 2.0,
        "ONE_TICKER_PER_DAY": True,
        "DAILY_CAP": 15,
        "STATUTORY_COSTS": True,
        "V12_RISK_SIZING": True,
        "PAPER_ENTRY_SLIPPAGE_BPS": 5.0,
        "INTRADAY_LEVERAGE": 5.0,
        "STOP_TARGET_SAME_BAR_POLICY": "STOP_FIRST",
        "ONE_MINUTE_GAP_POLICY": "CONSERVATIVE_5MIN_FALLBACK",
        "MISSING_FEATURE_POLICY": "FAIL_CLOSED",
    }
    mismatches = {
        key: {"expected": expected, "actual": contract.get(key)}
        for key, expected in required.items()
        if contract.get(key) != expected
    }
    if mismatches:
        raise RuntimeError(f"frozen execution contract mismatch: {mismatches}")
    rule = contract.get("RULE")
    if not isinstance(rule, Mapping):
        raise RuntimeError("frozen RULE mapping is missing")
    values = {field.name: rule.get(field.name) for field in fields(v9.RuleConfig)}
    config = v9.RuleConfig(**values)
    if v9.config_hash(config) != EXPECTED_CONFIG_SHA256:
        raise RuntimeError("frozen rule hash mismatch")
    if config.config_id != EXPECTED_CONFIG_ID:
        raise RuntimeError("frozen configuration ID mismatch")
    return config, contract


def session_calendar() -> list[str]:
    source = pd.read_csv(v2.SESSION_SOURCE)
    sessions = sorted(source["trade_date"].astype(str).unique())
    sessions = [day for day in sessions if START_DATE <= day <= END_DATE]
    if len(sessions) != EXPECTED_SESSIONS:
        raise RuntimeError(f"expected {EXPECTED_SESSIONS} sessions, found {len(sessions)}")
    if sessions[0] != START_DATE or sessions[-1] != END_DATE:
        raise RuntimeError(f"unexpected session bounds: {sessions[0]} through {sessions[-1]}")
    return sessions


def _num(frame: pd.DataFrame, column: str) -> pd.Series:
    return pd.to_numeric(frame[column], errors="coerce")


def frozen_rule_mask(frame: pd.DataFrame, config: v9.RuleConfig) -> pd.Series:
    mask = frame["pre_entry_data_invalid"].eq(False)
    mask &= frame["primary_side"].astype(str).str.upper().eq("LONG")
    mask &= _num(frame, "selection_rank").between(config.rank_min, config.rank_max)
    mask &= _num(frame, "signal_minute").between(
        config.signal_minute_min, config.signal_minute_max
    )
    mask &= _num(frame, "atr_pct").ge(config.atr_pct_min)
    mask &= _num(frame, "session_return_so_far_pct").ge(config.session_return_min)
    mask &= _num(frame, "vwap_dist_atr").ge(config.vwap_dist_atr_min)
    mask &= _num(frame, "close_position_in_bar").ge(config.close_position_min)
    thresholds = (
        ("range_pct", config.range_pct_min, ">="),
        ("ret_5m_pct", config.ret_5m_min, ">="),
        ("ret_5m_pct", config.ret_5m_max, "<="),
        ("ret_15m_pct", config.ret_15m_min, ">="),
        ("ret_30m_pct", config.ret_30m_min, ">="),
        ("ret_60m_pct", config.ret_60m_min, ">="),
        ("return_acceleration_5_vs_15", config.return_acceleration_min, ">="),
        ("ADX", config.adx_min, ">="),
        ("RSI", config.rsi_min, ">="),
        ("RSI", config.rsi_max, "<="),
        ("volume_ratio20", config.volume_ratio20_min, ">="),
        ("upper_wick_pct", config.upper_wick_pct_max, "<="),
        (
            "distance_from_running_session_high_atr",
            config.running_high_distance_atr_min,
            ">=",
        ),
        (
            "distance_from_running_session_high_atr",
            config.running_high_distance_atr_max,
            "<=",
        ),
        ("ema20_dist_atr", config.ema20_dist_atr_min, ">="),
        ("ema20_dist_atr", config.ema20_dist_atr_max, "<="),
        ("ema50_dist_atr", config.ema50_dist_atr_min, ">="),
        ("score_margin", config.score_margin_min, ">="),
        ("previous_ret_5m_pct", config.previous_ret_5m_max, "<="),
        ("previous_vwap_dist_atr", config.previous_vwap_dist_atr_max, "<="),
    )
    for column, threshold, operator in thresholds:
        if threshold is None:
            continue
        values = _num(frame, column)
        mask &= values.ge(float(threshold)) if operator == ">=" else values.le(float(threshold))
    if config.require_contiguous_previous:
        mask &= frame["contiguous_previous"].fillna(False).astype(bool)
    if config.require_bullish_reversal:
        mask &= frame["bullish_reversal"].fillna(False).astype(bool)
    if config.require_vwap_reclaim:
        mask &= frame["vwap_reclaim"].fillna(False).astype(bool)
    return mask.fillna(False)


def load_frozen_candidates(config: v9.RuleConfig) -> tuple[pd.DataFrame, pd.DataFrame]:
    source = pd.read_parquet(v2.SOURCE, columns=list(v9.SOURCE_COLUMNS))
    source = source.loc[
        source["trade_date"].astype(str).between(START_DATE, END_DATE)
    ].copy()
    funnel = [{"stage": "six_month_source_rows", "rows": len(source)}]
    if source["trade_date"].astype(str).nunique() != EXPECTED_SESSIONS:
        raise RuntimeError("source does not cover all six-month sessions")
    if len(source) != EXPECTED_SOURCE_ROWS:
        raise RuntimeError(
            f"six-month source row count changed: {len(source)} != {EXPECTED_SOURCE_ROWS}"
        )
    featured = v9.add_causal_features(source)
    candidates = featured.loc[frozen_rule_mask(featured, config)].copy()
    candidate_ticker_days = int(
        candidates[["trade_date", "ticker"]].drop_duplicates().shape[0]
    )
    candidate_active_sessions = int(candidates["trade_date"].astype(str).nunique())
    if len(candidates) != EXPECTED_FROZEN_SIGNAL_ROWS:
        raise RuntimeError("frozen-rule signal count changed")
    if candidate_ticker_days != EXPECTED_FROZEN_TICKER_DAYS:
        raise RuntimeError("frozen-rule ticker-day count changed")
    if candidate_active_sessions != EXPECTED_FROZEN_ACTIVE_SESSIONS:
        raise RuntimeError("frozen-rule active-session count changed")
    candidates["setup"] = v9.SETUP
    candidates["side"] = "LONG"
    candidates["bar_time_ist"] = candidates["signal_time_ist"]
    candidates["decision_ready_at_ist"] = candidates["signal_time_ist"]
    candidates["decision_ready_source"] = "completed_5min_signal_bar"
    candidates["quality_score"] = 301.0 - _num(candidates, "selection_rank")
    candidates["score"] = candidates["quality_score"]
    candidates["research_source_entry_time_ist"] = candidates[
        "entry_execution_time_ist"
    ]
    candidates["research_source_entry_price"] = candidates["entry_price"]
    candidates = candidates.sort_values(
        ["trade_date", "signal_time_ist", "selection_rank", "ticker"],
        kind="mergesort",
    ).reset_index(drop=True)
    candidates["_optimizer_row_id"] = np.arange(len(candidates), dtype=int)
    funnel.extend([
        {"stage": "frozen_rule_passing_5m_rows", "rows": len(candidates)},
        {
            "stage": "frozen_rule_unique_ticker_days",
            "rows": candidate_ticker_days,
        },
        {
            "stage": "frozen_rule_active_sessions_before_execution",
            "rows": candidate_active_sessions,
        },
    ])
    return candidates, pd.DataFrame(funnel)


def build_exact_six_month_universe(
    candidates: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    old_start, old_end = v9.START_DATE, v9.END_DATE
    old_fallback = v9.apply_one_minute_coverage_fallback
    try:
        v9.START_DATE = START_DATE
        v9.END_DATE = END_DATE
        v9.apply_one_minute_coverage_fallback = apply_six_month_coverage_fallback
        return v9.build_exact_universe(candidates)
    finally:
        v9.START_DATE = old_start
        v9.END_DATE = old_end
        v9.apply_one_minute_coverage_fallback = old_fallback


def apply_six_month_coverage_fallback(
    exact: pd.DataFrame,
    one_minute_loader: Any,
) -> pd.DataFrame:
    """V9 fallback with explicit paise-subunit cost-rounding tolerance.

    The statutory-cost helper stores gross P&L to four decimal places.  Older
    float32 five-minute EOD closes can therefore differ from the unrounded
    price-times-quantity identity by up to Rs 0.00005.  This function preserves
    V9's path logic exactly and accepts only that bounded storage rounding.
    """

    start = pd.Timestamp(START_DATE, tz="Asia/Kolkata")
    end = pd.Timestamp(END_DATE, tz="Asia/Kolkata") + pd.Timedelta(days=1)

    @lru_cache(maxsize=None)
    def load_five_minute(ticker: str) -> pd.DataFrame | None:
        path = (
            v9.v12.V7_HIST_INDICATORS_5M_DIR
            / f"{str(ticker).upper().strip()}_stocks_indicators_5min.parquet"
        )
        if not path.exists():
            return None
        columns = ["date", "open", "high", "low", "close", "volume"]
        try:
            frame = pd.read_parquet(
                path,
                columns=columns,
                filters=[("date", ">=", start), ("date", "<", end)],
            )
        except Exception:
            frame = pd.read_parquet(path, columns=columns)
        bars = v9.v12._normalise_bars_date_index(frame, naive_tz="UTC")
        if bars is None or bars.empty:
            return None
        bars = bars.loc[(bars.index >= start) & (bars.index < end)].copy()
        return bars if not bars.empty else None

    work = exact.copy()
    work["original_1m_outcome"] = work["outcome"].astype(str)
    work["original_1m_exit_time_ist"] = work["exit_time_ist"]
    work["original_1m_exit_price"] = _num(work, "exit_price")
    work["original_1m_net_pnl_rs"] = _num(work, "net_pnl_rs")
    records: list[dict[str, Any]] = []
    for row_index, row in work.iterrows():
        ticker = str(row["ticker"]).upper().strip()
        entry = v9.v12._normalise_ts(row["entry_time_ist"])
        original_exit = v9.v12._normalise_ts(row["exit_time_ist"])
        one = one_minute_loader(ticker)
        if one is None or one.empty or pd.isna(entry) or pd.isna(original_exit):
            gap_count = -1
        else:
            day_one = one.loc[
                (one.index.normalize() == entry.normalize())
                & (one.index >= entry)
                & (one.index <= original_exit)
            ]
            expected = pd.date_range(
                entry.floor("min"), original_exit.floor("min"), freq="1min"
            )
            observed = pd.DatetimeIndex(day_one.index).floor("min").unique()
            gap_count = int(len(expected.difference(observed)))
        record = {
            "one_minute_gap_count_before_original_exit": gap_count,
            "path_fallback_applied": False,
            "path_resolution_source": "EXACT_1MIN_COMPLETE_GRID",
            "fallback_5m_gap_count_before_exit": 0,
            "path_resolution_valid": gap_count == 0,
        }
        if gap_count == 0:
            records.append(record)
            continue
        five = load_five_minute(ticker)
        result = v9.v12.er.resolve(
            bars=five,
            side="LONG",
            entry_price=float(row["entry_price"]),
            entry_time_ist=entry,
            sl_pct=float(row["sl_pct"]),
            tgt_pct=float(row["tgt_pct"]),
            exit_policy=None,
        )
        if result is None:
            record["path_resolution_source"] = "UNRESOLVED_NO_5MIN_FALLBACK"
            records.append(record)
            continue
        exit_time = v9.v12._normalise_ts(result.exit_time_ist)
        expected_five = pd.date_range(
            entry.ceil("5min"), exit_time.floor("5min"), freq="5min"
        )
        if five is None or five.empty:
            five_gap_count = len(expected_five)
        else:
            observed_five = pd.DatetimeIndex(
                five.loc[
                    (five.index.normalize() == entry.normalize())
                    & (five.index >= entry.ceil("5min"))
                    & (five.index <= exit_time)
                ].index
            ).floor("min").unique()
            five_gap_count = int(len(expected_five.difference(observed_five)))
        costs = v9.nse.intraday_equity_costs(
            float(row["entry_price"]),
            float(result.exit_price),
            int(row["quantity"]),
            "LONG",
        )
        work.at[row_index, "outcome"] = str(result.outcome)
        work.at[row_index, "exit_time_ist"] = exit_time
        work.at[row_index, "exit_price"] = float(result.exit_price)
        work.at[row_index, "bars_held"] = int(result.bars_held)
        work.at[row_index, "gross_pnl_rs"] = float(costs.gross_pnl)
        work.at[row_index, "cost_rs"] = float(costs.total_cost)
        work.at[row_index, "net_pnl_rs"] = float(costs.net_pnl)
        record.update({
            "path_fallback_applied": True,
            "path_resolution_source": "CONSERVATIVE_5MIN_FALLBACK_FOR_1MIN_GAPS",
            "fallback_5m_gap_count_before_exit": five_gap_count,
            "path_resolution_valid": True,
        })
        records.append(record)
    coverage = pd.DataFrame(records)
    if len(coverage) != len(work):
        raise RuntimeError("path-coverage audit row-count mismatch")
    for column in coverage:
        work[column] = coverage[column].to_numpy()
    if not work["path_resolution_valid"].all():
        unresolved = int((~work["path_resolution_valid"]).sum())
        raise RuntimeError(f"{unresolved} rows lack complete 1m or fallback 5m paths")
    expected_gross = (
        _num(work, "exit_price") - _num(work, "entry_price")
    ) * _num(work, "quantity")
    difference = expected_gross - _num(work, "gross_pnl_rs")
    if difference.abs().max() >= P_AND_L_ROUNDING_TOLERANCE_RS:
        raise RuntimeError(
            "fallback-adjusted gross-P&L exceeds four-decimal storage tolerance: "
            f"{difference.abs().max()}"
        )
    work["gross_pnl_storage_rounding_difference_rs"] = difference
    return work


def daily_results(trades: pd.DataFrame, sessions: Sequence[str]) -> pd.DataFrame:
    day = pd.DataFrame({"trade_date": list(sessions)})
    grouped = trades.groupby("trade_date", as_index=False).agg(
        trades=("ticker", "size"),
        winners=("net_pnl_rs", lambda values: int(pd.to_numeric(values).gt(0).sum())),
        gross_pnl_rs=("gross_pnl_rs", "sum"),
        cost_rs=("cost_rs", "sum"),
        net_pnl_rs=("net_pnl_rs", "sum"),
    )
    day = day.merge(grouped, on="trade_date", how="left").fillna(0)
    day["trades"] = day["trades"].astype(int)
    day["winners"] = day["winners"].astype(int)
    day["cumulative_net_pnl_rs"] = day["net_pnl_rs"].cumsum()
    day["month"] = day["trade_date"].str[:7]
    day["selection_status"] = np.where(
        day["trade_date"].ge(SELECTION_MONTH_START),
        "JULY_AUGUST_SELECTION_MONTH",
        "EARLIER_HISTORICAL_REPLAY",
    )
    return day


def monthly_results(trades: pd.DataFrame, sessions: Sequence[str]) -> pd.DataFrame:
    months = sorted({day[:7] for day in sessions})
    rows = []
    for month in months:
        month_sessions = [day for day in sessions if day.startswith(month)]
        rows.append({"month": month, **v9.detailed_performance(trades, month_sessions)})
    return pd.DataFrame(rows)


def period_results(trades: pd.DataFrame, sessions: Sequence[str]) -> pd.DataFrame:
    periods = {
        "FULL_SIX_MONTHS": list(sessions),
        "EARLIER_FIVE_MONTHS": [day for day in sessions if day < SELECTION_MONTH_START],
        "ORIGINAL_SELECTION_MONTH": [day for day in sessions if day >= SELECTION_MONTH_START],
        "FIRST_60_SESSIONS": list(sessions[:60]),
        "LAST_60_SESSIONS": list(sessions[60:]),
    }
    return pd.DataFrame([
        {
            "period": label,
            "start_date": days[0],
            "end_date": days[-1],
            **v9.detailed_performance(trades, days),
        }
        for label, days in periods.items()
    ])


def block_results(
    trades: pd.DataFrame,
    sessions: Sequence[str],
    block_size: int = 20,
) -> pd.DataFrame:
    rows = []
    for number, start in enumerate(range(0, len(sessions), block_size), 1):
        days = list(sessions[start : start + block_size])
        rows.append({
            "block": number,
            "start_date": days[0],
            "end_date": days[-1],
            **v9.detailed_performance(trades, days),
        })
    return pd.DataFrame(rows)


def hourly_results(trades: pd.DataFrame) -> pd.DataFrame:
    work = trades.copy()
    signal = pd.to_datetime(work["signal_time_ist"], errors="raise", utc=True).dt.tz_convert(
        "Asia/Kolkata"
    )
    work["signal_hour_ist"] = signal.dt.hour
    rows = []
    for hour, group in work.groupby("signal_hour_ist", sort=True):
        pnl = _num(group, "net_pnl_rs")
        gains = float(pnl.loc[pnl > 0].sum())
        losses = float(-pnl.loc[pnl < 0].sum())
        rows.append({
            "signal_hour_ist": int(hour),
            "trades": len(group),
            "net_pnl_rs": float(pnl.sum()),
            "profit_factor": gains / losses if losses else (float("inf") if gains else 0.0),
            "win_rate_pct": float(pnl.gt(0).mean() * 100.0),
        })
    return pd.DataFrame(rows)


def outcome_results(trades: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for outcome, group in trades.groupby("outcome", dropna=False):
        pnl = _num(group, "net_pnl_rs")
        rows.append({
            "outcome": str(outcome),
            "trades": len(group),
            "net_pnl_rs": float(pnl.sum()),
            "average_net_pnl_rs": float(pnl.mean()),
        })
    return pd.DataFrame(rows).sort_values("outcome").reset_index(drop=True)


def compare_original_month(
    trades: pd.DataFrame,
    sessions: Sequence[str],
) -> dict[str, Any]:
    if not REFERENCE_SUMMARY.exists() or not REFERENCE_TRADES.exists():
        raise RuntimeError("original one-month reference artifacts are missing")
    reference_summary = json.loads(REFERENCE_SUMMARY.read_text(encoding="utf-8"))
    expected = reference_summary["period_results"]["full_month_in_sample"]
    selection_days = [day for day in sessions if day >= SELECTION_MONTH_START]
    replay = trades.loc[trades["trade_date"].isin(selection_days)].copy()
    actual = v9.detailed_performance(replay, selection_days)
    reference = pd.read_csv(REFERENCE_TRADES)

    def with_keys(frame: pd.DataFrame) -> pd.DataFrame:
        work = frame.copy()
        work["signal_ns"] = pd.to_datetime(
            work["signal_time_ist"], errors="raise", utc=True
        ).astype("int64")
        work["entry_ns"] = pd.to_datetime(
            work["entry_time_ist"], errors="raise", utc=True
        ).astype("int64")
        work["exit_ns"] = pd.to_datetime(
            work["exit_time_ist"], errors="raise", utc=True
        ).astype("int64")
        return work

    replay_keyed = with_keys(replay)
    reference_keyed = with_keys(reference)
    key_columns = ["trade_date", "ticker", "signal_ns"]
    merged = replay_keyed.merge(
        reference_keyed,
        on=key_columns,
        how="outer",
        suffixes=("_replay", "_reference"),
        indicator=True,
        validate="one_to_one",
    )
    key_mismatches = int(merged["_merge"].ne("both").sum())
    matched = merged.loc[merged["_merge"].eq("both")]
    time_mismatches = int(
        (
            matched["entry_ns_replay"].ne(matched["entry_ns_reference"])
            | matched["exit_ns_replay"].ne(matched["exit_ns_reference"])
        ).sum()
    )
    outcome_mismatches = int(
        matched["outcome_replay"].astype(str).ne(matched["outcome_reference"].astype(str)).sum()
    )
    max_pnl_difference = float(
        (
            pd.to_numeric(matched["net_pnl_rs_replay"], errors="raise")
            - pd.to_numeric(matched["net_pnl_rs_reference"], errors="raise")
        ).abs().max()
    )
    metric_differences = {
        "trades": int(actual["trades"]) - int(expected["trades"]),
        "net_pnl_rs": float(actual["net_pnl_rs"]) - float(expected["net_pnl_rs"]),
        "profit_factor": float(actual["profit_factor"]) - float(expected["profit_factor"]),
        "max_drawdown_rs": float(actual["max_drawdown_rs"])
        - float(expected["max_drawdown_rs"]),
    }
    passed = bool(
        key_mismatches == 0
        and time_mismatches == 0
        and outcome_mismatches == 0
        and max_pnl_difference < 1e-3
        and metric_differences["trades"] == 0
        and abs(metric_differences["net_pnl_rs"]) < 1e-3
        and abs(metric_differences["profit_factor"]) < 1e-9
        and abs(metric_differences["max_drawdown_rs"]) < 1e-3
    )
    if not passed:
        raise RuntimeError(
            "six-month pipeline failed exact one-month reproduction: "
            f"keys={key_mismatches}, times={time_mismatches}, "
            f"outcomes={outcome_mismatches}, pnl={max_pnl_difference}, "
            f"metrics={metric_differences}"
        )
    return {
        "passed": passed,
        "reference_trades": len(reference),
        "replay_trades": len(replay),
        "trade_key_mismatches": key_mismatches,
        "entry_or_exit_time_mismatches": time_mismatches,
        "outcome_mismatches": outcome_mismatches,
        "max_trade_net_pnl_difference_rs": max_pnl_difference,
        "metric_differences": metric_differences,
        "actual_metrics": actual,
    }


def audit_replay(
    config: v9.RuleConfig,
    exact: pd.DataFrame,
    trades: pd.DataFrame,
    daily: pd.DataFrame,
    reproduction: Mapping[str, Any],
) -> dict[str, Any]:
    expected_gross = (
        _num(trades, "exit_price") - _num(trades, "entry_price")
    ) * _num(trades, "quantity")
    expected_net = _num(trades, "gross_pnl_rs") - _num(trades, "cost_rs")
    signal = pd.to_datetime(trades["signal_time_ist"], errors="raise", utc=True)
    entry = pd.to_datetime(trades["entry_time_ist"], errors="raise", utc=True)
    exit_time = pd.to_datetime(trades["exit_time_ist"], errors="raise", utc=True)
    checks = {
        "frozen_config_hash_matches": v9.config_hash(config) == EXPECTED_CONFIG_SHA256,
        "all_selected_rows_pass_frozen_rule": bool(frozen_rule_mask(trades, config).all()),
        "one_ticker_per_day": not trades.duplicated(["trade_date", "ticker"]).any(),
        "daily_cap_respected": bool(daily["trades"].le(15).all()),
        "entry_after_completed_signal": bool((entry > signal).all()),
        "exit_not_before_entry": bool((exit_time >= entry).all()),
        "all_exit_paths_valid": bool(trades["path_resolution_valid"].all()),
        "original_month_exactly_reproduced": bool(reproduction["passed"]),
        "gross_pnl_identity": bool(
            np.allclose(
                expected_gross,
                _num(trades, "gross_pnl_rs"),
                atol=P_AND_L_ROUNDING_TOLERANCE_RS,
                rtol=0.0,
            )
        ),
        "net_pnl_identity": bool(
            np.allclose(expected_net, _num(trades, "net_pnl_rs"), atol=1e-3, rtol=0.0)
        ),
    }
    audit = {
        "all_checks_passed": bool(all(checks.values())),
        "checks": checks,
        "candidate_rows_exactly_resolved": len(exact),
        "selected_trades": len(trades),
        "selected_five_minute_fallback_rows": int(trades["path_fallback_applied"].sum()),
        "selected_source_max_window_incomplete_rows": int(
            trades["max_window_complete"].eq(False).sum()
        ),
        "selected_source_five_minute_day_incomplete_rows": int(
            trades["five_minute_day_complete"].eq(False).sum()
        ),
        "gross_pnl_identity_max_abs_error": float(
            (expected_gross - _num(trades, "gross_pnl_rs")).abs().max()
        ),
        "net_pnl_identity_max_abs_error": float(
            (expected_net - _num(trades, "net_pnl_rs")).abs().max()
        ),
        "one_month_reproduction": reproduction,
    }
    if not audit["all_checks_passed"]:
        raise RuntimeError(f"six-month replay audit failed: {checks}")
    return audit


def write_report(path: Path, summary: Mapping[str, Any]) -> None:
    full = summary["full_six_month_results"]
    earlier = summary["earlier_five_month_results"]
    selection = summary["original_selection_month_results"]
    months = summary["monthly_results"]
    monthly_lines = "\n".join(
        f"- {row['month']}: {row['trades']} trades, {row['trades_per_session']:.2f}/session, "
        f"net Rs {row['net_pnl_rs']:,.2f}, PF {row['profit_factor']:.3f}."
        for row in months
    )
    text = f"""# Six-month frozen L009216 V12 backtest

## Result

- Window: {START_DATE} through {END_DATE}, {full['sessions']} sessions.
- Trades: {full['trades']} ({full['trades_per_session']:.2f}/session; median
  {full['median_trades_per_session']:.1f}).
- Active sessions: {full['active_days']}/{full['sessions']}.
- Net P&L: Rs {full['net_pnl_rs']:,.2f}.
- Profit factor: {full['profit_factor']:.3f}.
- Win rate: {full['win_rate_pct']:.1f}%.
- Max drawdown: Rs {full['max_drawdown_rs']:,.2f}, realized-exit order.

## Configuration fidelity

`{EXPECTED_CONFIG_ID}` was loaded from the frozen configuration artifact.  No
threshold, rank, time window, stop, target, sizing, cost, or fallback setting
was optimized or changed.  The original July 6-August 4 result reproduced
exactly: {selection['trades']} trades, net Rs {selection['net_pnl_rs']:,.2f},
PF {selection['profit_factor']:.3f}.

## Earlier period versus selected month

- Earlier February 5-July 3: {earlier['trades']} trades,
  {earlier['trades_per_session']:.2f}/session, net Rs
  {earlier['net_pnl_rs']:,.2f}, PF {earlier['profit_factor']:.3f}.
- Original July 6-August 4 selection month: {selection['trades']} trades,
  {selection['trades_per_session']:.2f}/session, net Rs
  {selection['net_pnl_rs']:,.2f}, PF {selection['profit_factor']:.3f}.

## Monthly results

{monthly_lines}

## Interpretation

This is a fixed historical replay, not a new optimization.  It is still not a
genuinely fresh forward holdout because earlier six-month research influenced
the broader strategy-development process.  The reconstructed prefilter used a
static current 1,237-stock universe because no point-in-time universe was
available, creating survivorship-bias risk.  The April 9 and April 10 09:20
prefilter slots ended at rank 243 rather than 300; all other 838 hourly slots
had the complete 101-stock rank band.  `PRODUCTION_APPROVED=False`; no live or
production process was changed, enabled, or restarted.
"""
    path.write_text(text, encoding="utf-8")


def main() -> int:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    config, contract = load_frozen_configuration()
    sessions = session_calendar()
    candidates, funnel = load_frozen_candidates(config)
    exact, raw, rejects, prewarm = build_exact_six_month_universe(candidates)
    arrays = v9.SearchArrays(exact, sessions)
    selected = arrays.selected_indices(config)
    trades = v9.selected_trade_frame(exact, selected)
    daily = daily_results(trades, sessions)
    monthly = monthly_results(trades, sessions)
    periods = period_results(trades, sessions)
    blocks = block_results(trades, sessions)
    hourly = hourly_results(trades)
    outcomes = outcome_results(trades)
    cost_stress = pd.DataFrame([
        v9.detailed_performance(trades, sessions, cost_multiplier=value)
        for value in (1.0, 1.25, 1.5, 2.0)
    ])
    reproduction = compare_original_month(trades, sessions)
    audit = audit_replay(config, exact, trades, daily, reproduction)

    period_lookup = periods.set_index("period").to_dict("index")
    full = period_lookup["FULL_SIX_MONTHS"]
    summary = {
        "research_only": True,
        "production_approved": False,
        "optimization_performed": False,
        "verdict": "FROZEN_SIX_MONTH_HISTORICAL_REPLAY_NOT_FRESH_HOLDOUT",
        "setup_name": contract["SETUP_NAME"],
        "config_id": config.config_id,
        "config_sha256": v9.config_hash(config),
        "window": {"start": START_DATE, "end": END_DATE, "sessions": len(sessions)},
        "full_six_month_results": full,
        "earlier_five_month_results": period_lookup["EARLIER_FIVE_MONTHS"],
        "original_selection_month_results": period_lookup["ORIGINAL_SELECTION_MONTH"],
        "first_60_session_results": period_lookup["FIRST_60_SESSIONS"],
        "last_60_session_results": period_lookup["LAST_60_SESSIONS"],
        "monthly_results": monthly.to_dict("records"),
        "twenty_session_blocks": blocks.to_dict("records"),
        "cost_stress": cost_stress.to_dict("records"),
        "execution_contract": {
            "prefilter_job_changed": False,
            "required_primary_side": "LONG",
            "rank_band_inclusive": [config.rank_min, config.rank_max],
            "completed_5m_signal": True,
            "signal_minute_range": [config.signal_minute_min, config.signal_minute_max],
            "exact_next_available_1m_entry": True,
            "exact_1m_exit_with_conservative_5m_gap_fallback": True,
            "stop_loss_pct": contract["STOP_LOSS_PCT"],
            "target_pct": contract["TARGET_PCT"],
            "entry_slippage_bps": contract["PAPER_ENTRY_SLIPPAGE_BPS"],
            "statutory_costs": True,
            "v12_risk_sizing": True,
            "intraday_leverage": contract["INTRADAY_LEVERAGE"],
            "one_ticker_per_day": True,
            "daily_cap": contract["DAILY_CAP"],
            "same_bar_policy": contract["STOP_TARGET_SAME_BAR_POLICY"],
        },
        "candidate_counts": {
            "frozen_rule_passing_5m_rows": len(candidates),
            "exact_executable_rows": len(exact),
            "entry_engine_rejects": len(rejects),
            "selected_trades": len(trades),
        },
        "prewarm_1m": prewarm,
        "audit_passed": audit["all_checks_passed"],
        "no_production_mutation": True,
        "selection_bias_note": (
            "The configuration was selected on July 6-August 4 and the broader "
            "strategy-development process had already inspected earlier six-month data."
        ),
        "data_quality_limitations": {
            "point_in_time_universe_available": False,
            "survivorship_bias_risk": True,
            "prefilter_universe": "current static 1,237-symbol manifest",
            "hourly_slots_expected": 840,
            "full_rank200_300_slots": 838,
            "underfilled_slots": [
                {"trade_date": "2026-04-09", "membership_slot": "09:20", "last_rank": 243},
                {"trade_date": "2026-04-10", "membership_slot": "09:20", "last_rank": 243},
            ],
            "stale_memberships_fail_closed": 31,
        },
    }

    artifacts: dict[str, pd.DataFrame] = {
        "trades.csv": trades,
        "daily_results.csv": daily,
        "monthly_results.csv": monthly,
        "period_results.csv": periods,
        "twenty_session_blocks.csv": blocks,
        "hourly_results.csv": hourly,
        "outcome_results.csv": outcomes,
        "cost_stress.csv": cost_stress,
        "candidate_funnel.csv": funnel,
        "entry_engine_rejects.csv": rejects,
    }
    for name, frame in artifacts.items():
        frame.to_csv(OUTPUT_DIR / name, index=False)
    exact.to_parquet(OUTPUT_DIR / "exact_candidate_universe.parquet", index=False)
    raw.to_parquet(OUTPUT_DIR / "entry_engine_raw.parquet", index=False)
    (OUTPUT_DIR / "summary.json").write_text(
        json.dumps(v9.json_safe(summary), indent=2), encoding="utf-8"
    )
    (OUTPUT_DIR / "audit.json").write_text(
        json.dumps(v9.json_safe(audit), indent=2), encoding="utf-8"
    )
    (OUTPUT_DIR / "frozen_setup_conf.py").write_text(
        CONFIG_SOURCE.read_text(encoding="utf-8"), encoding="utf-8"
    )
    write_report(OUTPUT_DIR / "RESEARCH_REPORT.md", summary)

    artifact_names = list(artifacts) + [
        "exact_candidate_universe.parquet",
        "entry_engine_raw.parquet",
        "summary.json",
        "audit.json",
        "frozen_setup_conf.py",
        "RESEARCH_REPORT.md",
    ]
    manifest = {
        "artifacts": {
            name: {
                "bytes": (OUTPUT_DIR / name).stat().st_size,
                "sha256": sha256(OUTPUT_DIR / name),
            }
            for name in artifact_names
        },
        "sources": {
            str(Path(__file__).resolve()): sha256(Path(__file__).resolve()),
            str(CONFIG_SOURCE.resolve()): sha256(CONFIG_SOURCE),
            str(REFERENCE_SUMMARY.resolve()): sha256(REFERENCE_SUMMARY),
            str(REFERENCE_TRADES.resolve()): sha256(REFERENCE_TRADES),
            str(v2.SOURCE.resolve()): sha256(v2.SOURCE),
            str(v2.SESSION_SOURCE.resolve()): sha256(v2.SESSION_SOURCE),
            str(Path(v9.__file__).resolve()): sha256(Path(v9.__file__).resolve()),
        },
        "audit_passed": True,
    }
    (OUTPUT_DIR / "integrity_manifest.json").write_text(
        json.dumps(v9.json_safe(manifest), indent=2), encoding="utf-8"
    )
    print(json.dumps(v9.json_safe(summary), indent=2), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
