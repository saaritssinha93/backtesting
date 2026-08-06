"""Six-month backcast of the frozen frequency-balanced V12 LONG rule.

The configuration is loaded unchanged from the one-month L009216 research
artifact.  This module performs no threshold search or model fitting.  It
replays every eligible state from 2026-02-05 through 2026-08-04 using the same
completed-5m signal, exact next-available 1m entry, statutory costs, V12 risk
sizing, 1% stop, 2% target, one-ticker/day rule, daily cap, and conservative
5m fallback for incomplete 1m paths.

Because the rule was selected using the final month before this earlier period
was replayed, this is a historical backcast, not a fresh forward holdout.
Nothing in this module modifies or enables production configuration.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from contextlib import contextmanager
from dataclasses import asdict, fields
from functools import lru_cache
from pathlib import Path
from typing import Any, Iterator, Mapping, Sequence

import numpy as np
import pandas as pd

import research_v12_one_month_long_logic_optimizer_v9 as v9
import research_v12_path_aware_long_rebuild as v2


PRODUCTION_APPROVED = False
RESEARCH_ONLY = True
START_DATE = "2026-02-05"
END_DATE = "2026-08-04"
EXPECTED_SESSIONS = 120
SELECTION_MONTH_START = "2026-07-06"
EARLIER_BACKCAST_END = "2026-07-03"
EXPECTED_CONFIG_ID = "L009216_PULLBACK_BOUNCE"
EXPECTED_CONFIG_SHA256 = "7d5f02566c6bffc649395d22cc925dd3083079b3d63b308cf0d12258c3438310"
SOURCE_SUMMARY = Path(
    r"C:\TradingData\eqidv2_experiments"
    r"\v12_one_month_long_logic_posthoc_v10_20260706_20260804"
    r"\balanced_summary.json"
)
SOURCE_CONFIG = SOURCE_SUMMARY.with_name("balanced_one_month_long_setup_conf.py")
SOURCE_TRADES = SOURCE_SUMMARY.with_name("balanced_candidate_trades.csv")
SOURCE_DELIVERY_MANIFEST = SOURCE_SUMMARY.with_name("final_delivery_manifest.json")
EXPECTED_SOURCE_CONFIG_FILE_SHA256 = "cce7c83cfdbb3c3532fcef11258cbaa7909b50679f9e21baf429a3ba928900a7"
EXPECTED_SOURCE_SUMMARY_FILE_SHA256 = "af866cc2801919d6cef5e297e417f67ba4c882b249a066edc19e11ba1e79a5bb"
EXPECTED_SOURCE_TRADES_FILE_SHA256 = "8ccc4dc60e1965203057ab734c6d5d171cbf4e422fadf69e17d835f1ab5d5eb6"
EXPECTED_REFERENCE_TRADES = 66
PREFILTER_SOURCE_MANIFEST = v2.SOURCE.with_name("causal_entry_opportunities_v2_manifest.json")
PREFILTER_INTEGRITY_REPORT = v2.SOURCE.with_name("integrity_report.json")
PREFILTER_REPLAY_SUMMARY = Path(
    r"C:\TradingData\eqidv2_experiments"
    r"\prefilter_six_month_replay_20260205_20260804_k300"
    r"\rank200_300_summary.json"
)
OUTPUT_DIR = Path(
    r"C:\TradingData\eqidv2_experiments"
    r"\v12_six_month_frozen_balanced_long_v11_20260205_20260804"
)
REPLAY_SETUP = f"SIX_MONTH_FROZEN_{EXPECTED_CONFIG_SHA256[:12]}"
P_AND_L_STORAGE_TOLERANCE_RS = 5.1e-5


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def load_frozen_config() -> tuple[v9.RuleConfig, dict[str, Any]]:
    pinned_files = {
        SOURCE_CONFIG: EXPECTED_SOURCE_CONFIG_FILE_SHA256,
        SOURCE_SUMMARY: EXPECTED_SOURCE_SUMMARY_FILE_SHA256,
        SOURCE_TRADES: EXPECTED_SOURCE_TRADES_FILE_SHA256,
    }
    for path, expected_hash in pinned_files.items():
        if not path.exists() or sha256(path) != expected_hash:
            raise RuntimeError(f"pinned one-month reference changed: {path}")
    payload = json.loads(SOURCE_SUMMARY.read_text(encoding="utf-8"))
    champion = payload["champion"]
    values = {field.name: champion[field.name] for field in fields(v9.RuleConfig)}
    config = v9.RuleConfig(**values)
    if config.config_id != EXPECTED_CONFIG_ID:
        raise RuntimeError(f"frozen config id changed: {config.config_id}")
    if v9.config_hash(config) != EXPECTED_CONFIG_SHA256:
        raise RuntimeError("frozen configuration hash changed")
    if bool(payload.get("production_approved", True)):
        raise RuntimeError("source research configuration unexpectedly production-approved")
    if not SOURCE_CONFIG.exists() or "PRODUCTION_APPROVED = False" not in SOURCE_CONFIG.read_text(
        encoding="utf-8"
    ):
        raise RuntimeError("source configuration safety marker missing")
    reference_trades = pd.read_csv(SOURCE_TRADES)
    if len(reference_trades) != EXPECTED_REFERENCE_TRADES:
        raise RuntimeError("one-month reference trade count changed")
    delivery = json.loads(SOURCE_DELIVERY_MANIFEST.read_text(encoding="utf-8"))
    for name, path in (
        ("balanced_one_month_long_setup_conf.py", SOURCE_CONFIG),
        ("balanced_summary.json", SOURCE_SUMMARY),
        ("balanced_candidate_trades.csv", SOURCE_TRADES),
    ):
        expected = delivery.get("artifacts", {}).get(name, {}).get("sha256")
        if expected != sha256(path):
            raise RuntimeError(f"one-month delivery manifest mismatch: {name}")
    return config, payload


def assert_frozen_runtime_contract() -> dict[str, Any]:
    expected = {
        "risk_sizing_enabled": True,
        "risk_equity_rs": 200_000.0,
        "risk_pct_per_trade": 0.25,
        "risk_min_notional_rs": 50_000.0,
        "risk_max_notional_rs": 150_000.0,
        "intraday_leverage": 5.0,
        "paper_entry_slippage_pct": 0.0005,
    }
    actual = {
        "risk_sizing_enabled": bool(v9.v12.RISK_SIZING_ENABLED),
        "risk_equity_rs": float(v9.v12.RISK_EQUITY_RS),
        "risk_pct_per_trade": float(v9.v12.RISK_PCT_PER_TRADE),
        "risk_min_notional_rs": float(v9.v12.RISK_MIN_NOTIONAL_RS),
        "risk_max_notional_rs": float(v9.v12.RISK_MAX_NOTIONAL_RS),
        "intraday_leverage": float(v9.v12.V7_INTRADAY_LEVERAGE),
        "paper_entry_slippage_pct": float(v9.v12.V7_PAPER_SLIPPAGE_PCT),
    }
    for key, expected_value in expected.items():
        actual_value = actual[key]
        if isinstance(expected_value, bool):
            valid = actual_value is expected_value
        else:
            valid = bool(np.isclose(actual_value, expected_value, atol=1e-12, rtol=0.0))
        if not valid:
            raise RuntimeError(
                f"frozen V12 runtime contract changed at {key}: "
                f"{actual_value!r} != {expected_value!r}"
            )
    roots = {
        "one_minute_root": str(Path(v9.v12.v6.DATA_1M_DIR).resolve()),
        "five_minute_root": str(Path(v9.v12.V7_HIST_INDICATORS_5M_DIR).resolve()),
        "resolver_source": str(Path(v9.v12.er.__file__).resolve()),
    }
    for label, path_text in roots.items():
        if label.endswith("root") and not Path(path_text).is_dir():
            raise RuntimeError(f"frozen data root missing: {path_text}")
        if label == "resolver_source" and not Path(path_text).is_file():
            raise RuntimeError(f"frozen resolver source missing: {path_text}")

    collision_time = pd.Timestamp("2026-02-05 10:05", tz="Asia/Kolkata")
    collision_bar = pd.DataFrame(
        {"open": [100.0], "high": [102.5], "low": [98.5], "close": [100.0]},
        index=pd.DatetimeIndex([collision_time]),
    )
    result = v9.v12.er.resolve(
        collision_bar, "LONG", 100.0, collision_time, 1.0, 2.0, None
    )
    if result is None or str(result.outcome) != "SL":
        raise RuntimeError("frozen stop-first collision policy changed")
    return {**actual, **roots, "same_bar_collision_policy": "STOP_FIRST"}


def session_calendar() -> list[str]:
    source = pd.read_csv(v2.SESSION_SOURCE)
    days = sorted(source["trade_date"].astype(str).unique())
    days = [day for day in days if START_DATE <= day <= END_DATE]
    if len(days) != EXPECTED_SESSIONS:
        raise RuntimeError(f"expected {EXPECTED_SESSIONS} sessions, found {len(days)}")
    if days[0] != START_DATE or days[-1] != END_DATE:
        raise RuntimeError(f"unexpected session bounds: {days[:1]} through {days[-1:]}")
    return days


@contextmanager
def six_month_v9_contract() -> Iterator[None]:
    names = (
        "START_DATE",
        "END_DATE",
        "SETUP",
        "PREFILTER_RANK_MIN",
        "PREFILTER_RANK_MAX",
    )
    old = {name: getattr(v9, name) for name in names}
    old_fallback = v9.apply_one_minute_coverage_fallback
    v9.START_DATE = START_DATE
    v9.END_DATE = END_DATE
    v9.SETUP = REPLAY_SETUP
    v9.PREFILTER_RANK_MIN = 200
    v9.PREFILTER_RANK_MAX = 300
    v9.apply_one_minute_coverage_fallback = apply_storage_tolerant_coverage_fallback
    try:
        yield
    finally:
        for name, value in old.items():
            setattr(v9, name, value)
        v9.apply_one_minute_coverage_fallback = old_fallback


def apply_storage_tolerant_coverage_fallback(
    exact: pd.DataFrame,
    one_minute_loader: Any,
) -> pd.DataFrame:
    """V9 coverage resolver with only four-decimal P&L storage tolerance.

    Path admissibility is deliberately *not* trusted here.  The returned frame
    is subsequently passed through :func:`strict_path_filter`, which rejects
    incomplete/synthetic 5m grids and delayed-entry fallback bars.  This adapter
    exists only because the statutory cost record stores gross P&L to four
    decimal places while price-times-quantity is computed at full precision.
    """

    start = pd.Timestamp(START_DATE, tz="Asia/Kolkata")
    end = pd.Timestamp(END_DATE, tz="Asia/Kolkata") + pd.Timedelta(days=1)

    @lru_cache(maxsize=None)
    def load_five_minute(ticker: str) -> pd.DataFrame | None:
        path = (
            Path(v9.v12.V7_HIST_INDICATORS_5M_DIR)
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
    work["original_1m_exit_price"] = pd.to_numeric(work["exit_price"], errors="coerce")
    work["original_1m_net_pnl_rs"] = pd.to_numeric(work["net_pnl_rs"], errors="coerce")
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
        pd.to_numeric(work["exit_price"], errors="raise")
        - pd.to_numeric(work["entry_price"], errors="raise")
    ) * pd.to_numeric(work["quantity"], errors="raise")
    difference = expected_gross - pd.to_numeric(work["gross_pnl_rs"], errors="raise")
    max_error = float(difference.abs().max())
    if max_error >= P_AND_L_STORAGE_TOLERANCE_RS:
        raise RuntimeError(
            "fallback-adjusted gross P&L exceeds four-decimal storage tolerance: "
            f"{max_error}"
        )
    work["gross_pnl_storage_rounding_difference_rs"] = difference
    return work


def rule_mask(
    frame: pd.DataFrame,
    config: v9.RuleConfig,
    sessions: Sequence[str],
) -> np.ndarray:
    """Apply the exact V9 feature mask before expensive path resolution."""

    probe = frame.copy()
    probe["net_pnl_rs"] = 0.0
    probe["gross_pnl_rs"] = 0.0
    probe["cost_rs"] = 0.0
    probe["exit_time_ist"] = pd.to_datetime(
        probe["signal_time_ist"], errors="raise", utc=True
    ) + pd.Timedelta(minutes=1)
    arrays = v9.SearchArrays(probe, sessions)
    return arrays.mask(config)


def load_frozen_candidates(
    config: v9.RuleConfig,
    sessions: Sequence[str],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    candidates, funnel = v9.load_union_candidates()
    mask = rule_mask(candidates, config, sessions)
    passing = candidates.loc[mask].copy().reset_index(drop=True)
    if passing.empty:
        raise RuntimeError("frozen rule has no six-month candidates")
    if not rule_mask(passing, config, sessions).all():
        raise RuntimeError("pre-resolution frozen-rule mask parity failed")
    extras = pd.DataFrame([
        {"stage": "frozen_rule_passing_5m_states", "rows": len(passing)},
        {
            "stage": "frozen_rule_unique_ticker_days",
            "rows": int(passing[["trade_date", "ticker"]].drop_duplicates().shape[0]),
        },
        {"stage": "frozen_rule_active_sessions", "rows": int(passing["trade_date"].nunique())},
        {"stage": "frozen_rule_unique_tickers", "rows": int(passing["ticker"].nunique())},
    ])
    return passing, pd.concat([funnel, extras], ignore_index=True)


def feature_coverage_audit(
    config: v9.RuleConfig,
) -> tuple[dict[str, Any], pd.DataFrame]:
    source = pd.read_parquet(v2.SOURCE, columns=list(v9.SOURCE_COLUMNS))
    source = source.loc[
        source["trade_date"].astype(str).between(START_DATE, END_DATE)
    ].copy()
    source = v9.add_causal_features(source)
    if source.duplicated(["ticker", "trade_date", "signal_time_ist"]).any():
        raise RuntimeError("duplicate five-minute source states after causal normalization")
    numeric = lambda name: pd.to_numeric(source[name], errors="coerce")
    scope = source["pre_entry_data_invalid"].eq(False)
    scope &= source["primary_side"].astype(str).str.upper().eq("LONG")
    scope &= numeric("selection_rank").between(config.rank_min, config.rank_max)
    scope &= numeric("signal_minute").between(
        config.signal_minute_min, config.signal_minute_max
    )
    other = scope.copy()
    other &= numeric("session_return_so_far_pct").ge(config.session_return_min)
    other &= numeric("close_position_in_bar").ge(config.close_position_min)
    other &= numeric("range_pct").ge(float(config.range_pct_min))
    other &= numeric("ret_5m_pct").between(
        float(config.ret_5m_min), float(config.ret_5m_max)
    )
    other &= numeric("score_margin").ge(float(config.score_margin_min))
    other &= numeric("previous_ret_5m_pct").le(float(config.previous_ret_5m_max))
    other &= source["contiguous_previous"].fillna(False).astype(bool)
    technical = ("atr_pct", "vwap_dist_atr", "ema20_dist_atr")
    technical_missing = source.loc[:, technical].isna().any(axis=1)
    observed = other & ~technical_missing
    observed &= numeric("atr_pct").ge(config.atr_pct_min)
    observed &= numeric("vwap_dist_atr").ge(config.vwap_dist_atr_min)
    observed &= numeric("ema20_dist_atr").ge(float(config.ema20_dist_atr_min))
    unknown = other & technical_missing

    source["month"] = source["trade_date"].astype(str).str[:7]
    rows = []
    for month in sorted(source["month"].unique()):
        month_mask = source["month"].eq(month)
        relevant = scope & month_mask
        relevant_count = int(relevant.sum())
        missing_count = int((relevant & technical_missing).sum())
        rows.append({
            "month": month,
            "relevant_scope_rows": relevant_count,
            "technical_triplet_missing_rows": missing_count,
            "technical_triplet_missing_pct": (
                missing_count / relevant_count * 100.0 if relevant_count else 0.0
            ),
            "close_position_missing_rows": int(
                (relevant & source["close_position_in_bar"].isna()).sum()
            ),
            "otherwise_qualifying_unknown_rows": int((unknown & month_mask).sum()),
            "observed_frozen_signal_rows": int((observed & month_mask).sum()),
        })
    frame = pd.DataFrame(rows)
    if int(observed.sum()) != 502:
        raise RuntimeError(
            f"frozen source-mask reference changed: {int(observed.sum())} != 502"
        )
    return {
        "source_rows": len(source),
        "source_sessions": int(source["trade_date"].nunique()),
        "required_technical_fields": list(technical),
        "technical_triplet_missing_rows_in_scope": int((scope & technical_missing).sum()),
        "otherwise_qualifying_unknown_rows": int(unknown.sum()),
        "observed_frozen_signal_rows": int(observed.sum()),
        "missing_indicator_policy": "FAIL_CLOSED_NO_IMPUTATION",
    }, frame


def _load_audit_bars(ticker: str, timeframe: str) -> pd.DataFrame | None:
    symbol = str(ticker).upper().strip()
    if timeframe == "1min":
        root = Path(v9.v12.v6.DATA_1M_DIR)
        path = root / f"{symbol}_stocks_indicators_1min.parquet"
        columns = ["date", "open", "high", "low", "close"]
    elif timeframe == "5min":
        root = Path(v9.v12.V7_HIST_INDICATORS_5M_DIR)
        path = root / f"{symbol}_stocks_indicators_5min.parquet"
        columns = ["date", "open", "high", "low", "close", "gap_filled"]
    else:  # pragma: no cover - internal contract
        raise ValueError(timeframe)
    if not path.exists():
        return None
    try:
        frame = pd.read_parquet(path, columns=columns)
    except Exception:
        frame = pd.read_parquet(path)
    bars = v9.v12._normalise_bars_date_index(frame, naive_tz="UTC")
    if bars is None or bars.empty:
        return None
    start = pd.Timestamp(START_DATE, tz="Asia/Kolkata")
    end = pd.Timestamp(END_DATE, tz="Asia/Kolkata") + pd.Timedelta(days=1)
    return bars.loc[(bars.index >= start) & (bars.index < end)].copy()


def _grid_validation_reasons(
    bars: pd.DataFrame | None,
    start: pd.Timestamp,
    end: pd.Timestamp,
    frequency: str,
    *,
    reject_gap_filled: bool,
) -> list[str]:
    if bars is None or bars.empty:
        return ["BAR_FILE_MISSING_OR_EMPTY"]
    if start > end:
        return ["EMPTY_EXPECTED_PATH"]
    sub = bars.loc[(bars.index >= start) & (bars.index <= end)].copy()
    reasons: list[str] = []
    if sub.empty:
        return ["PATH_EMPTY"]
    observed = pd.DatetimeIndex(sub.index).floor("min")
    expected = pd.date_range(start.floor("min"), end.floor("min"), freq=frequency)
    if observed.duplicated().any():
        reasons.append("DUPLICATE_TIMESTAMPS")
    unique_observed = observed.unique()
    if len(unique_observed) != len(expected) or len(expected.difference(unique_observed)):
        reasons.append("INCOMPLETE_TIMESTAMP_GRID")
    ohlc = sub[["open", "high", "low", "close"]].apply(
        pd.to_numeric, errors="coerce"
    )
    values = ohlc.to_numpy(dtype=float)
    if not np.isfinite(values).all() or (values <= 0).any():
        reasons.append("INVALID_NONPOSITIVE_OR_NONFINITE_OHLC")
    else:
        high_floor = ohlc[["open", "low", "close"]].max(axis=1)
        low_ceiling = ohlc[["open", "high", "close"]].min(axis=1)
        if ohlc["high"].lt(high_floor).any() or ohlc["low"].gt(low_ceiling).any():
            reasons.append("INCONSISTENT_OHLC_BOUNDS")
    if reject_gap_filled and "gap_filled" in sub:
        values = sub["gap_filled"]
        flagged = values.eq(True) | values.astype(str).str.strip().str.lower().isin(
            {"true", "1", "yes"}
        )
        if flagged.fillna(False).any():
            reasons.append("SYNTHETIC_GAP_FILLED_BAR")
    return reasons


def strict_path_filter(exact: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    rejects: list[dict[str, Any]] = []
    valid_indices: list[int] = []
    for ticker, ticker_rows in exact.groupby("ticker", sort=False):
        need_one = (~ticker_rows["path_fallback_applied"].astype(bool)).any()
        need_five = ticker_rows["path_fallback_applied"].astype(bool).any()
        one = _load_audit_bars(str(ticker), "1min") if need_one else None
        five = _load_audit_bars(str(ticker), "5min") if need_five else None
        for index, row in ticker_rows.iterrows():
            signal = v9.v12._normalise_ts(row["signal_time_ist"])
            entry = v9.v12._normalise_ts(row["entry_time_ist"])
            exit_time = v9.v12._normalise_ts(row["exit_time_ist"])
            reasons: list[str] = []
            if bool(row["path_fallback_applied"]):
                delay_seconds = (entry - signal).total_seconds()
                if not np.isclose(delay_seconds, 60.0, atol=1e-6, rtol=0.0):
                    reasons.append("FALLBACK_ENTRY_NOT_SIGNAL_PLUS_1MIN")
                if int(row["fallback_5m_gap_count_before_exit"]) != 0:
                    reasons.append("RECORDED_FIVE_MINUTE_GAP")
                reasons.extend(_grid_validation_reasons(
                    five,
                    entry.ceil("5min"),
                    exit_time.floor("5min"),
                    "5min",
                    reject_gap_filled=True,
                ))
            else:
                if int(row["one_minute_gap_count_before_original_exit"]) != 0:
                    reasons.append("RECORDED_ONE_MINUTE_GAP_WITHOUT_FALLBACK")
                reasons.extend(_grid_validation_reasons(
                    one,
                    entry.floor("min"),
                    exit_time.floor("min"),
                    "1min",
                    reject_gap_filled=False,
                ))
            reasons = sorted(set(reasons))
            if reasons:
                rejects.append({
                    "_optimizer_row_id": int(row["_optimizer_row_id"]),
                    "ticker": str(row["ticker"]),
                    "trade_date": str(row["trade_date"]),
                    "signal_time_ist": row["signal_time_ist"],
                    "entry_time_ist": row["entry_time_ist"],
                    "exit_time_ist": row["exit_time_ist"],
                    "path_resolution_source": row["path_resolution_source"],
                    "strict_rejection_reasons": "|".join(reasons),
                })
            else:
                valid_indices.append(index)
    filtered = exact.loc[valid_indices].copy().reset_index(drop=True)
    filtered["strict_path_valid"] = True
    filtered["strict_path_policy"] = np.where(
        filtered["path_fallback_applied"].astype(bool),
        "COMPLETE_NONSYNTHETIC_5MIN_SIGNAL_PLUS_1_ONLY",
        "COMPLETE_VALID_1MIN_GRID",
    )
    reject_columns = [
        "_optimizer_row_id", "ticker", "trade_date", "signal_time_ist",
        "entry_time_ist", "exit_time_ist", "path_resolution_source",
        "strict_rejection_reasons",
    ]
    return filtered, pd.DataFrame(rejects, columns=reject_columns)


def daily_results(trades: pd.DataFrame, sessions: Sequence[str]) -> pd.DataFrame:
    base = pd.DataFrame({"trade_date": list(sessions)})
    if trades.empty:
        grouped = pd.DataFrame(columns=[
            "trade_date", "trades", "winners", "gross_pnl_rs", "cost_rs", "net_pnl_rs"
        ])
    else:
        grouped = (
            trades.assign(winner=pd.to_numeric(trades["net_pnl_rs"], errors="raise").gt(0))
            .groupby("trade_date", as_index=False)
            .agg(
                trades=("ticker", "size"),
                winners=("winner", "sum"),
                gross_pnl_rs=("gross_pnl_rs", "sum"),
                cost_rs=("cost_rs", "sum"),
                net_pnl_rs=("net_pnl_rs", "sum"),
            )
        )
    result = base.merge(grouped, on="trade_date", how="left", validate="one_to_one")
    result[["trades", "winners"]] = result[["trades", "winners"]].fillna(0).astype(int)
    result[["gross_pnl_rs", "cost_rs", "net_pnl_rs"]] = result[
        ["gross_pnl_rs", "cost_rs", "net_pnl_rs"]
    ].fillna(0.0)
    result["cumulative_net_pnl_rs"] = result["net_pnl_rs"].cumsum()
    result["month"] = result["trade_date"].str[:7]
    return result


def period_frame(
    trades: pd.DataFrame,
    periods: Mapping[str, Sequence[str]],
) -> tuple[dict[str, dict[str, Any]], pd.DataFrame]:
    metrics = {
        label: v9.detailed_performance(trades, days)
        for label, days in periods.items()
    }
    rows = [{"period": label, **value} for label, value in metrics.items()]
    return metrics, pd.DataFrame(rows)


def month_periods(sessions: Sequence[str]) -> dict[str, list[str]]:
    months = sorted({day[:7] for day in sessions})
    return {month: [day for day in sessions if day.startswith(month)] for month in months}


def block_periods(sessions: Sequence[str], size: int = 20) -> dict[str, list[str]]:
    days = list(sessions)
    return {
        f"BLOCK_{index // size + 1:02d}": days[index : index + size]
        for index in range(0, len(days), size)
    }


def hourly_results(trades: pd.DataFrame) -> pd.DataFrame:
    if trades.empty:
        return pd.DataFrame(columns=["signal_hour_ist", "trades", "winners", "net_pnl_rs", "profit_factor"])
    work = trades.copy()
    work["signal_hour_ist"] = pd.to_datetime(
        work["signal_time_ist"], errors="raise", utc=True
    ).dt.tz_convert("Asia/Kolkata").dt.hour
    rows = []
    for hour, group in work.groupby("signal_hour_ist", sort=True):
        pnl = pd.to_numeric(group["net_pnl_rs"], errors="raise")
        gains = float(pnl[pnl > 0].sum())
        losses = float(-pnl[pnl < 0].sum())
        rows.append({
            "signal_hour_ist": int(hour),
            "trades": len(group),
            "winners": int(pnl.gt(0).sum()),
            "win_rate_pct": float(pnl.gt(0).mean() * 100.0),
            "net_pnl_rs": float(pnl.sum()),
            "profit_factor": gains / losses if losses else (float("inf") if gains else 0.0),
        })
    return pd.DataFrame(rows)


def audit_replay(
    exact: pd.DataFrame,
    trades: pd.DataFrame,
    daily: pd.DataFrame,
    config: v9.RuleConfig,
    sessions: Sequence[str],
) -> dict[str, Any]:
    arrays = v9.SearchArrays(exact, sessions)
    exact_mask_violations = int((~arrays.mask(config)).sum())
    entry = pd.to_datetime(trades["entry_time_ist"], errors="raise", utc=True)
    signal = pd.to_datetime(trades["signal_time_ist"], errors="raise", utc=True)
    exit_time = pd.to_datetime(trades["exit_time_ist"], errors="raise", utc=True)
    entry_price = pd.to_numeric(trades["entry_price"], errors="raise")
    exit_price = pd.to_numeric(trades["exit_price"], errors="raise")
    quantity = pd.to_numeric(trades["quantity"], errors="raise")
    gross = pd.to_numeric(trades["gross_pnl_rs"], errors="raise")
    cost = pd.to_numeric(trades["cost_rs"], errors="raise")
    net = pd.to_numeric(trades["net_pnl_rs"], errors="raise")
    expected_gross = (exit_price - entry_price) * quantity
    expected_net = gross - cost
    checks = {
        "session_count_is_120": len(sessions) == EXPECTED_SESSIONS,
        "exact_rows_all_pass_frozen_rule": exact_mask_violations == 0,
        "one_ticker_per_day": not trades.duplicated(["trade_date", "ticker"]).any(),
        "daily_cap_respected": bool(daily["trades"].le(v9.DAILY_CAP).all()),
        "entry_after_completed_signal": bool((entry > signal).all()),
        "exit_not_before_entry": bool((exit_time >= entry).all()),
        "path_resolution_valid": bool(exact["path_resolution_valid"].all()),
        "strict_path_validation_passed": bool(exact["strict_path_valid"].all()),
        "stop_is_1pct": bool(pd.to_numeric(trades["sl_pct"], errors="raise").eq(1.0).all()),
        "target_is_2pct": bool(pd.to_numeric(trades["tgt_pct"], errors="raise").eq(2.0).all()),
        "gross_identity": bool(np.allclose(
            expected_gross,
            gross,
            atol=P_AND_L_STORAGE_TOLERANCE_RS,
            rtol=0.0,
        )),
        "net_identity": bool(np.allclose(expected_net, net, atol=1e-3, rtol=0.0)),
        "config_hash_frozen": v9.config_hash(config) == EXPECTED_CONFIG_SHA256,
    }
    notional = entry_price * quantity
    leverage = float(v9.v12.V7_INTRADAY_LEVERAGE)
    return {
        "passed": bool(all(checks.values())),
        "checks": checks,
        "exact_rule_mask_violations": exact_mask_violations,
        "gross_identity_max_abs_error": float((expected_gross - gross).abs().max()),
        "gross_pnl_storage_tolerance_rs": P_AND_L_STORAGE_TOLERANCE_RS,
        "net_identity_max_abs_error": float((expected_net - net).abs().max()),
        "selected_five_minute_fallback_rows": int(trades["path_fallback_applied"].sum()),
        "universe_five_minute_fallback_rows": int(exact["path_fallback_applied"].sum()),
        "selected_source_max_window_incomplete_rows": int(
            trades["max_window_complete"].eq(False).sum()
        ),
        "position_notional_min_rs": float(notional.min()),
        "position_notional_median_rs": float(notional.median()),
        "position_notional_max_rs": float(notional.max()),
        "intraday_leverage": leverage,
        "estimated_margin_median_rs": float((notional / leverage).median()),
    }


def write_replay_config(path: Path, config: v9.RuleConfig) -> None:
    text = f'''"""Frozen six-month backcast configuration; research only."""

PRODUCTION_APPROVED = False
RESEARCH_ONLY = True
BACKCAST_NOT_FORWARD_HOLDOUT = True
SOURCE_CONFIG_ID = {config.config_id!r}
SOURCE_CONFIG_SHA256 = {v9.config_hash(config)!r}
SOURCE_CONFIG_PATH = {str(SOURCE_CONFIG)!r}
REPLAY_WINDOW = ({START_DATE!r}, {END_DATE!r})

PREFILTER_JOB_CHANGED = False
PREFILTER_PRIMARY_SIDE = "LONG"
PREFILTER_RANK_MIN = {config.rank_min}
PREFILTER_RANK_MAX = {config.rank_max}
SIGNAL_TIMEFRAME = "5min_completed_bar"
ENTRY_TIMEFRAME = "exact_next_available_1min"
EXIT_TIMEFRAME = "exact_1min_with_conservative_5min_gap_fallback"
STOP_LOSS_PCT = {v9.STOP_LOSS_PCT!r}
TARGET_PCT = {v9.TARGET_PCT!r}
ONE_TICKER_PER_DAY = True
DAILY_CAP = {v9.DAILY_CAP}
STATUTORY_COSTS = True
V12_RISK_SIZING = True
PAPER_ENTRY_SLIPPAGE_BPS = {float(v9.v12.V7_PAPER_SLIPPAGE_PCT) * 10_000.0!r}
INTRADAY_LEVERAGE = {float(v9.v12.V7_INTRADAY_LEVERAGE)!r}
STOP_TARGET_SAME_BAR_POLICY = "STOP_FIRST"
ONE_MINUTE_GAP_POLICY = "CONSERVATIVE_5MIN_FALLBACK"
MISSING_FEATURE_POLICY = "FAIL_CLOSED"
ENTRY_SELECTION = "first chronological passing signal per ticker/day"
ENTRY_TIE_BREAK = ("signal_time_ist", "selection_rank", "ticker")

RULE = {v9.json_safe(asdict(config))!r}
'''
    path.write_text(text, encoding="utf-8")


def write_report(path: Path, summary: Mapping[str, Any]) -> None:
    full = summary["full_period"]
    earlier = summary["backcast_vs_selection"]["EARLIER_98_SESSIONS"]
    selection = summary["backcast_vs_selection"]["ORIGINAL_SELECTION_22_SESSIONS"]
    months = pd.DataFrame(summary["monthly_results"])
    table_rows = [
        "| Month | Sessions | Trades | Trades/day | Net P&L | PF | Win rate | Max DD |",
        "|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for _, row in months.iterrows():
        table_rows.append(
            f"| {row['period']} | {int(row['sessions'])} | {int(row['trades'])} | "
            f"{row['trades_per_session']:.2f} | Rs {row['net_pnl_rs']:,.0f} | "
            f"{row['profit_factor']:.3f} | {row['win_rate_pct']:.1f}% | "
            f"Rs {row['max_drawdown_rs']:,.0f} |"
        )
    report = f"""# Six-month frozen V12 LONG setup replay

## Result

The unchanged `{summary['config_id']}` rule was replayed over all
{summary['source_sessions']} sessions from {START_DATE} through {END_DATE}. It
produced **{full['trades']} trades**, **{full['trades_per_session']:.2f}/session**
(median **{full['median_trades_per_session']:.1f}**), net P&L **Rs
{full['net_pnl_rs']:,.0f}**, PF **{full['profit_factor']:.3f}**, win rate
**{full['win_rate_pct']:.1f}%**, and realized-exit-order max drawdown **Rs
{full['max_drawdown_rs']:,.0f}**.

This run performed **no optimization**.  Nevertheless, it is a backward replay
of dates preceding the one-month period used to select the rule, so it is not a
fresh forward holdout and cannot establish expected live profitability.

The separation is decisive: the **98 sessions before the optimized month**
produced **{earlier['trades']} trades**, net **Rs {earlier['net_pnl_rs']:,.0f}**,
and PF **{earlier['profit_factor']:.3f}**.  The original 22-session selection
month reproduced exactly at **{selection['trades']} trades**, net **Rs
{selection['net_pnl_rs']:,.0f}**, and PF **{selection['profit_factor']:.3f}**.
This confirms execution parity and shows that the apparent edge is concentrated
in the month used to choose the rule.

## Monthly results

{chr(10).join(table_rows)}

## Execution

- Existing hourly prefilter unchanged; only LONG ranks 200-300.
- Completed five-minute signal, exact next-available one-minute entry.
- Fixed 1% stop / 2% target, statutory costs, V12 sizing and 5x leverage.
- One ticker/day and a 15-trade daily cap.
- Incomplete one-minute paths use a conservative five-minute fallback with
  stop-first same-bar collision handling.
- `{summary['execution']['selected_five_minute_fallback_rows']}` selected trades
  used the fallback.

## Data portability diagnostics

- `{summary['feature_coverage']['otherwise_qualifying_unknown_rows']}` source
  rows pass every nontechnical condition but have missing ATR/VWAP/EMA20; they
  remain fail-closed with no imputation.
- `{summary['membership_boundary']['selected_plus_60_minute_entries']}` selected
  entries occur exactly 60 minutes after the hourly membership snapshot.  The
  frozen research convention includes that boundary.
- Excluding the +60-minute boundary would produce
  `{summary['membership_boundary']['exclude_plus60_stress']['trades']}` trades,
  PF `{summary['membership_boundary']['exclude_plus60_stress']['profit_factor']:.3f}`,
  and net Rs
  `{summary['membership_boundary']['exclude_plus60_stress']['net_pnl_rs']:,.0f}`.
- This research rule also requires a stateful live feature builder for
  `previous_ret_5m_pct`, `contiguous_previous`, and `score_margin`; it is not a
  drop-in production setup.
- The historical prefilter used a static current-universe manifest because a
  point-in-time universe was unavailable, so survivorship-bias risk remains.
- Two opening prefilter slots (April 9-10 at 09:20) were underfilled.  Excluding
  both complete sessions still gives PF
  `{summary['data_quality_sensitivity']['exclude_underfilled_slot_dates']['profit_factor']:.3f}`
  and net Rs
  `{summary['data_quality_sensitivity']['exclude_underfilled_slot_dates']['net_pnl_rs']:,.0f}`.

## Safety and interpretation

`PRODUCTION_APPROVED=False`.  No setup was enabled or restarted.  Use the
monthly/20-session stability and the full-period result to decide whether the
rule deserves a genuinely fresh forward test; do not treat this backcast as
promotion evidence.
"""
    path.write_text(report, encoding="utf-8")


def _write_contract(
    path: Path,
    config: v9.RuleConfig,
    candidates_path: Path,
    exact_manifest_path: Path,
) -> None:
    payload = {
        "config_id": config.config_id,
        "config_sha256": v9.config_hash(config),
        "window": [START_DATE, END_DATE, EXPECTED_SESSIONS],
        "candidate_sha256": sha256(candidates_path),
        "exact_cache_manifest_sha256": sha256(exact_manifest_path),
        "source_summary_sha256": sha256(SOURCE_SUMMARY),
        "source_config_sha256": sha256(SOURCE_CONFIG),
        "runner_sha256": sha256(Path(__file__).resolve()),
    }
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _validate_contract(
    path: Path,
    config: v9.RuleConfig,
    candidates_path: Path,
    exact_manifest_path: Path,
) -> None:
    if not path.exists():
        raise RuntimeError("frozen replay contract missing; rebuild exact cache")
    payload = json.loads(path.read_text(encoding="utf-8"))
    expected = {
        "config_sha256": v9.config_hash(config),
        "candidate_sha256": sha256(candidates_path),
        "exact_cache_manifest_sha256": sha256(exact_manifest_path),
        "source_summary_sha256": sha256(SOURCE_SUMMARY),
        "source_config_sha256": sha256(SOURCE_CONFIG),
        "runner_sha256": sha256(Path(__file__).resolve()),
    }
    for key, value in expected.items():
        if payload.get(key) != value:
            raise RuntimeError(f"frozen replay contract changed at {key}; rebuild exact cache")


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, default=OUTPUT_DIR)
    parser.add_argument("--reuse-exact", action="store_true")
    args = parser.parse_args(argv)
    out = args.output_dir
    out.mkdir(parents=True, exist_ok=True)
    config, source_payload = load_frozen_config()
    sessions = session_calendar()
    runtime_contract = assert_frozen_runtime_contract()
    feature_coverage, feature_coverage_frame = feature_coverage_audit(config)
    prefilter_replay = json.loads(PREFILTER_REPLAY_SUMMARY.read_text(encoding="utf-8"))
    if prefilter_replay.get("window", {}).get("trading_days") != EXPECTED_SESSIONS:
        raise RuntimeError("prefilter replay session coverage changed")

    candidate_path = out / "frozen_rule_candidates.parquet"
    funnel_path = out / "candidate_funnel.csv"
    exact_path = out / "exact_candidate_universe.parquet"
    raw_path = out / "entry_engine_raw.parquet"
    rejects_path = out / "entry_engine_rejects.csv"
    exact_manifest_path = out / "exact_cache_manifest.json"
    contract_path = out / "frozen_replay_contract.json"
    strict_rejects_path = out / "strict_path_rejects.csv"

    with six_month_v9_contract():
        can_reuse = all(path.exists() for path in (
            candidate_path, funnel_path, exact_path, raw_path, exact_manifest_path,
            contract_path, strict_rejects_path,
        ))
        if args.reuse_exact and can_reuse:
            candidates = pd.read_parquet(candidate_path)
            funnel = pd.read_csv(funnel_path)
            exact = pd.read_parquet(exact_path)
            raw = pd.read_parquet(raw_path)
            rejects = pd.read_csv(rejects_path) if rejects_path.exists() else pd.DataFrame()
            strict_rejects = pd.read_csv(strict_rejects_path)
            v9.validate_exact_cache_manifest(exact_manifest_path, exact, raw)
            _validate_contract(contract_path, config, candidate_path, exact_manifest_path)
            if "strict_path_valid" not in exact or not exact["strict_path_valid"].all():
                raise RuntimeError("reused exact cache lacks strict path validation")
            prewarm = {"reused_exact_cache": True}
        else:
            candidates, funnel = load_frozen_candidates(config, sessions)
            exact_pre_strict, raw, rejects, prewarm = v9.build_exact_universe(candidates)
            exact, strict_rejects = strict_path_filter(exact_pre_strict)
            funnel = pd.concat([
                funnel,
                pd.DataFrame([
                    {"stage": "entry_engine_exact_rows_before_strict_path_audit", "rows": len(exact_pre_strict)},
                    {"stage": "strict_path_rejects", "rows": len(strict_rejects)},
                    {"stage": "strict_valid_exact_rows", "rows": len(exact)},
                ]),
            ], ignore_index=True)
            candidates.to_parquet(candidate_path, index=False)
            funnel.to_csv(funnel_path, index=False)
            exact.to_parquet(exact_path, index=False)
            raw.to_parquet(raw_path, index=False)
            rejects.to_csv(rejects_path, index=False)
            strict_rejects.to_csv(strict_rejects_path, index=False)
            v9.write_exact_cache_manifest(exact_manifest_path, exact, raw)
            _write_contract(contract_path, config, candidate_path, exact_manifest_path)

    arrays = v9.SearchArrays(exact, sessions)
    selected = arrays.selected_indices(config)
    trades = v9.selected_trade_frame(exact, selected)
    daily = daily_results(trades, sessions)
    full = v9.detailed_performance(trades, sessions)
    earlier_days = [day for day in sessions if day <= EARLIER_BACKCAST_END]
    selection_days = [day for day in sessions if day >= SELECTION_MONTH_START]
    if len(earlier_days) != 98 or len(selection_days) != 22:
        raise RuntimeError("backcast/selection period boundary changed")
    backcast_vs_selection, backcast_selection_frame = period_frame(trades, {
        "EARLIER_98_SESSIONS": earlier_days,
        "ORIGINAL_SELECTION_22_SESSIONS": selection_days,
    })
    reference = source_payload["period_results"]["full_month_in_sample"]
    reproduced = backcast_vs_selection["ORIGINAL_SELECTION_22_SESSIONS"]
    reference_parity = {
        "expected_trades": int(reference["trades"]),
        "actual_trades": int(reproduced["trades"]),
        "net_pnl_difference_rs": float(reproduced["net_pnl_rs"] - reference["net_pnl_rs"]),
        "profit_factor_difference": float(
            reproduced["profit_factor"] - reference["profit_factor"]
        ),
        "max_drawdown_difference_rs": float(
            reproduced["max_drawdown_rs"] - reference["max_drawdown_rs"]
        ),
    }
    reference_parity["passed"] = bool(
        reference_parity["actual_trades"] == reference_parity["expected_trades"]
        and abs(reference_parity["net_pnl_difference_rs"]) < 1e-6
        and abs(reference_parity["profit_factor_difference"]) < 1e-12
        and abs(reference_parity["max_drawdown_difference_rs"]) < 1e-6
    )
    if not reference_parity["passed"]:
        raise RuntimeError("frozen one-month reference parity failed")
    halves, half_frame = period_frame(trades, {
        "FIRST_60_SESSIONS": sessions[:60],
        "LAST_60_SESSIONS": sessions[60:],
    })
    monthly, monthly_frame = period_frame(trades, month_periods(sessions))
    blocks, block_frame = period_frame(trades, block_periods(sessions))
    hourly = hourly_results(trades)
    cost_stress = pd.DataFrame([
        v9.detailed_performance(trades, sessions, cost_multiplier=value)
        for value in (1.0, 1.25, 1.5, 2.0)
    ])
    membership_time = pd.to_datetime(
        exact["membership_slot_ist"], errors="raise", utc=True
    )
    signal_time = pd.to_datetime(exact["signal_time_ist"], errors="raise", utc=True)
    exact_lag_minutes = (signal_time - membership_time).dt.total_seconds().div(60.0)
    if not exact_lag_minutes.between(10.0, 60.0).all():
        raise RuntimeError("hourly membership lag fell outside frozen inclusive convention")
    exact_without_plus60 = exact.loc[exact_lag_minutes.lt(60.0)].reset_index(drop=True)
    no60_arrays = v9.SearchArrays(exact_without_plus60, sessions)
    no60_trades = v9.selected_trade_frame(
        exact_without_plus60, no60_arrays.selected_indices(config)
    )
    no60_metrics = v9.detailed_performance(no60_trades, sessions)
    selected_membership = pd.to_datetime(
        trades["membership_slot_ist"], errors="raise", utc=True
    )
    selected_signal = pd.to_datetime(trades["signal_time_ist"], errors="raise", utc=True)
    selected_lag = (selected_signal - selected_membership).dt.total_seconds().div(60.0)
    membership_boundary = {
        "inclusive_plus60_policy": True,
        "exact_plus60_candidate_states": int(exact_lag_minutes.eq(60.0).sum()),
        "selected_plus_60_minute_entries": int(selected_lag.eq(60.0).sum()),
        "exclude_plus60_stress": no60_metrics,
    }
    underfilled_slot_dates = {"2026-04-09", "2026-04-10"}
    sensitivity_days = [day for day in sessions if day not in underfilled_slot_dates]
    data_quality_sensitivity = {
        "underfilled_prefilter_slots": [
            "2026-04-09T09:20:00+05:30",
            "2026-04-10T09:20:00+05:30",
        ],
        "exclude_underfilled_slot_dates": v9.detailed_performance(
            trades, sensitivity_days
        ),
    }
    audit = audit_replay(exact, trades, daily, config, sessions)
    audit["checks"]["one_month_reference_reproduced"] = reference_parity["passed"]
    audit["passed"] = bool(all(audit["checks"].values()))
    if not audit["passed"]:
        raise RuntimeError("six-month replay audit failed")

    summary = {
        "research_only": True,
        "production_approved": False,
        "verdict": "HISTORICAL_BACKCAST_NOT_FORWARD_VALIDATION",
        "config_id": config.config_id,
        "config_sha256": v9.config_hash(config),
        "configuration_unchanged": True,
        "optimization_or_refit_in_this_run": False,
        "backcast_not_forward_holdout": True,
        "source_selection_window": source_payload.get("period_results", {}).get(
            "full_month_in_sample", {}
        ),
        "window": [START_DATE, END_DATE],
        "source_sessions": len(sessions),
        "candidate_counts": {
            "passing_5m_states": len(candidates),
            "passing_unique_ticker_days": int(
                candidates[["trade_date", "ticker"]].drop_duplicates().shape[0]
            ),
            "passing_active_sessions": int(candidates["trade_date"].nunique()),
            "exact_executable_states": len(exact),
            "entry_rejects": len(rejects),
            "strict_path_rejects": len(strict_rejects),
        },
        "full_period": full,
        "backcast_vs_selection": backcast_vs_selection,
        "reference_parity": reference_parity,
        "half_results": halves,
        "monthly_results": v9.json_safe(monthly_frame.to_dict("records")),
        "twenty_session_block_results": v9.json_safe(block_frame.to_dict("records")),
        "cost_stress": v9.json_safe(cost_stress.to_dict("records")),
        "feature_coverage": feature_coverage,
        "prefilter_provenance": {
            "mode": prefilter_replay.get("mode"),
            "selection_occurrences": prefilter_replay.get("selection_occurrences"),
            "slots": prefilter_replay.get("slots"),
            "underfilled_slots": prefilter_replay.get("underfilled_slots", []),
            "historical_universe_limitations": prefilter_replay.get(
                "historical_universe_limitations", {}
            ),
        },
        "membership_boundary": membership_boundary,
        "data_quality_sensitivity": data_quality_sensitivity,
        "runtime_contract": runtime_contract,
        "execution": {
            "hourly_prefilter_changed": False,
            "prefilter_primary_side": "LONG",
            "rank_band": [config.rank_min, config.rank_max],
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
            **{key: value for key, value in audit.items() if key not in {"passed", "checks"}},
        },
        "audit": audit,
        "prewarm": prewarm,
        "no_production_mutation": True,
    }

    output_frames = {
        "frozen_setup_six_month_trades.csv": trades,
        "frozen_setup_six_month_daily.csv": daily,
        "frozen_setup_six_month_monthly.csv": monthly_frame,
        "frozen_setup_six_month_20session_blocks.csv": block_frame,
        "frozen_setup_six_month_halves.csv": half_frame,
        "frozen_setup_backcast_vs_selection.csv": backcast_selection_frame,
        "frozen_setup_six_month_hourly.csv": hourly,
        "frozen_setup_six_month_cost_stress.csv": cost_stress,
        "frozen_setup_feature_coverage_by_month.csv": feature_coverage_frame,
        "frozen_setup_exclude_plus60_trades.csv": no60_trades,
    }
    for name, frame in output_frames.items():
        frame.to_csv(out / name, index=False)
    write_replay_config(out / "frozen_six_month_long_setup_conf.py", config)
    (out / "summary.json").write_text(
        json.dumps(v9.json_safe(summary), indent=2), encoding="utf-8"
    )
    (out / "validation_checks.json").write_text(
        json.dumps(v9.json_safe(audit), indent=2), encoding="utf-8"
    )
    write_report(out / "SIX_MONTH_REPLAY_REPORT.md", summary)

    artifact_names = [
        "frozen_rule_candidates.parquet",
        "candidate_funnel.csv",
        "exact_candidate_universe.parquet",
        "entry_engine_raw.parquet",
        "entry_engine_rejects.csv",
        "strict_path_rejects.csv",
        "exact_cache_manifest.json",
        "frozen_replay_contract.json",
        *output_frames.keys(),
        "frozen_six_month_long_setup_conf.py",
        "summary.json",
        "validation_checks.json",
        "SIX_MONTH_REPLAY_REPORT.md",
    ]
    manifest = {
        "production_approved": False,
        "artifacts": {
            name: {"sha256": sha256(out / name), "bytes": (out / name).stat().st_size}
            for name in artifact_names if (out / name).exists()
        },
        "sources": {
            str(Path(__file__).resolve()): sha256(Path(__file__).resolve()),
            str(Path(v9.__file__).resolve()): sha256(Path(v9.__file__).resolve()),
            str(SOURCE_SUMMARY.resolve()): sha256(SOURCE_SUMMARY),
            str(SOURCE_CONFIG.resolve()): sha256(SOURCE_CONFIG),
            str(SOURCE_TRADES.resolve()): sha256(SOURCE_TRADES),
            str(SOURCE_DELIVERY_MANIFEST.resolve()): sha256(SOURCE_DELIVERY_MANIFEST),
            str(v2.SOURCE.resolve()): sha256(v2.SOURCE),
            str(v2.SESSION_SOURCE.resolve()): sha256(v2.SESSION_SOURCE),
            **(
                {str(PREFILTER_SOURCE_MANIFEST.resolve()): sha256(PREFILTER_SOURCE_MANIFEST)}
                if PREFILTER_SOURCE_MANIFEST.exists() else {}
            ),
            **(
                {str(PREFILTER_INTEGRITY_REPORT.resolve()): sha256(PREFILTER_INTEGRITY_REPORT)}
                if PREFILTER_INTEGRITY_REPORT.exists() else {}
            ),
            **(
                {str(PREFILTER_REPLAY_SUMMARY.resolve()): sha256(PREFILTER_REPLAY_SUMMARY)}
                if PREFILTER_REPLAY_SUMMARY.exists() else {}
            ),
        },
    }
    (out / "integrity_manifest.json").write_text(
        json.dumps(v9.json_safe(manifest), indent=2), encoding="utf-8"
    )
    print(json.dumps(v9.json_safe({
        "output_dir": str(out),
        "verdict": summary["verdict"],
        "config_id": config.config_id,
        "candidate_counts": summary["candidate_counts"],
        "full_period": full,
        "monthly_results": summary["monthly_results"],
        "cost_stress": summary["cost_stress"],
        "audit_passed": audit["passed"],
    }), indent=2), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
