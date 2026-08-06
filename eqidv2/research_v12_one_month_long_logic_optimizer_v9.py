"""Leakage-controlled one-month V12 LONG setup-logic optimizer.

This research-only driver searches deterministic, causal entry rules on
2026-07-06 through 2026-07-21, uses 2026-07-22 through 2026-07-28 for
selection validation, freezes one configuration, and opens the untouched
2026-07-29 through 2026-08-04 test exactly once.  The hourly prefilter job is
not changed: only stocks marked LONG at that hour and ranks 200-300 in the
existing research cache are eligible.

Entry signals are completed five-minute bars.  Fills are the exact next
available one-minute V12 entry; exits use exact one-minute paths, statutory
costs, V12 risk sizing, one ticker/day, and a 15-trade daily cap.  The exit is
fixed at 1% stop / 2% target during entry-rule search so entry and exit effects
are not confounded.

Nothing in this module imports, modifies, enables, or restarts production
configuration.  Generated configurations remain PRODUCTION_APPROVED=False.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
from dataclasses import asdict, dataclass, fields, replace
from functools import lru_cache
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

import numpy as np
import pandas as pd

import avwap_5min_ID_v12_backtesting as v12
import nse_intraday_costs as nse
import research_v12_path_aware_long_rebuild as v2
import research_v12_prefilter_train_test_optimizer as optimizer


SETUP = "ONE_MONTH_PREFILTER_LONG_V9"
PRODUCTION_APPROVED = False
RESEARCH_ONLY = True

START_DATE = "2026-07-06"
DEVELOPMENT_END = "2026-07-21"
VALIDATION_START = "2026-07-22"
VALIDATION_END = "2026-07-28"
TEST_START = "2026-07-29"
END_DATE = "2026-08-04"

EXPECTED_SESSION_COUNTS = {"development": 12, "validation": 5, "test": 5}
PREFILTER_RANK_MIN = 200
PREFILTER_RANK_MAX = 300
DAILY_CAP = 15
STOP_LOSS_PCT = 1.0
TARGET_PCT = 2.0
SEARCH_SEED = 20260805
DEFAULT_SEARCH_TRIALS = 100_000
MAX_VALIDATION_FINALISTS = 10
EXACT_CACHE_SCHEMA_VERSION = 2

# Outcome-blind union-pool guards.  These are deliberately permissive and are
# also emitted into the final configuration; every searched rule can only
# tighten them.
UNION_GUARDS = {
    "signal_minute_min": 570,
    "signal_minute_max": 855,
    "atr_pct_min": 0.35,
    "session_return_so_far_pct_min": 0.0,
    "vwap_dist_atr_min": -0.50,
    "close_position_in_bar_min": 0.35,
}

OUTPUT_DIR = Path(
    r"C:\TradingData\eqidv2_experiments"
    r"\v12_one_month_long_logic_optimizer_v9_20260706_20260804"
)

SOURCE_COLUMNS = tuple(dict.fromkeys((
    "ticker", "membership_slot_ist", "membership_hour", "selection_rank",
    "selection_bucket", "primary_side", "primary_family", "selection_reason",
    "overall_score", "long_score", "short_score", "activity_score",
    "staleness_seconds", "signal_time_ist", "trade_date", "signal_open",
    "signal_high", "signal_low", "signal_close", "signal_volume",
    "signal_minute", "RSI", "ADX", "CCI", "MFI", "atr_pct",
    "vwap_dist_atr", "ema20_dist_atr", "ema50_dist_atr",
    "ema200_dist_atr", "ret_5m_pct", "ret_15m_pct", "ret_30m_pct",
    "ret_60m_pct", "session_return_so_far_pct", "range_pct", "body_pct",
    "upper_wick_pct", "lower_wick_pct", "close_position_in_bar",
    "volume_ratio20", "traded_value_rs",
    "distance_from_running_session_high_atr", "rebound_from_session_low_pct",
    "ema_long_stack", "entry_execution_time_ist", "entry_price",
    "pre_entry_data_invalid", "max_window_complete", "five_minute_day_complete",
)))


@dataclass(frozen=True)
class RuleConfig:
    config_id: str
    family: str
    rank_min: int
    rank_max: int
    signal_minute_min: int
    signal_minute_max: int
    atr_pct_min: float
    session_return_min: float
    vwap_dist_atr_min: float
    close_position_min: float
    range_pct_min: float | None = None
    ret_5m_min: float | None = None
    ret_5m_max: float | None = None
    ret_15m_min: float | None = None
    ret_30m_min: float | None = None
    ret_60m_min: float | None = None
    return_acceleration_min: float | None = None
    adx_min: float | None = None
    rsi_min: float | None = None
    rsi_max: float | None = None
    volume_ratio20_min: float | None = None
    upper_wick_pct_max: float | None = None
    running_high_distance_atr_min: float | None = None
    running_high_distance_atr_max: float | None = None
    ema20_dist_atr_min: float | None = None
    ema20_dist_atr_max: float | None = None
    ema50_dist_atr_min: float | None = None
    score_margin_min: float | None = None
    previous_ret_5m_max: float | None = None
    previous_vwap_dist_atr_max: float | None = None
    require_contiguous_previous: bool = False
    require_bullish_reversal: bool = False
    require_vwap_reclaim: bool = False


def json_safe(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_safe(item) for item in value]
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating, float)):
        number = float(value)
        return number if math.isfinite(number) else None
    if isinstance(value, (np.bool_, bool)):
        return bool(value)
    if isinstance(value, (pd.Timestamp,)):
        return value.isoformat()
    return value


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def config_hash(config: RuleConfig) -> str:
    payload = json.dumps(json_safe(asdict(config)), sort_keys=True).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def exact_cache_contract(raw: pd.DataFrame) -> dict[str, Any]:
    tickers = sorted(raw["ticker"].astype(str).str.upper().str.strip().unique())
    dependencies = [
        Path(__file__).resolve(),
        Path(v12.__file__).resolve(),
        Path(optimizer.__file__).resolve(),
        Path(v2.__file__).resolve(),
        Path(nse.__file__).resolve(),
        Path(v12.er.__file__).resolve(),
        Path(v12.v6.__file__).resolve(),
        Path(__file__).with_name("eqidv2_v7_position_sizing.py").resolve(),
        v2.SOURCE.resolve(),
        v2.SESSION_SOURCE.resolve(),
    ]
    source_hashes = {
        str(path): sha256(path) for path in dependencies if path.exists()
    }
    input_stats: list[tuple[str, int, int]] = []
    for ticker in tickers:
        for path in (
            v12.v6.DATA_1M_DIR / f"{ticker}_stocks_indicators_1min.parquet",
            v12.V7_HIST_INDICATORS_5M_DIR / f"{ticker}_stocks_indicators_5min.parquet",
        ):
            if path.exists():
                stat = path.stat()
                input_stats.append((str(path), int(stat.st_size), int(stat.st_mtime_ns)))
            else:
                input_stats.append((str(path), -1, -1))
    input_fingerprint = hashlib.sha256(
        json.dumps(input_stats, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    contract = {
        "schema_version": EXACT_CACHE_SCHEMA_VERSION,
        "window": [START_DATE, END_DATE],
        "setup": SETUP,
        "sl_pct": STOP_LOSS_PCT,
        "target_pct": TARGET_PCT,
        "ticker_count": len(tickers),
        "input_file_count": len(input_stats),
        "one_and_five_minute_input_stat_fingerprint": input_fingerprint,
        "source_sha256": source_hashes,
        "execution_constants": {
            "paper_entry_slippage_pct": float(v12.V7_PAPER_SLIPPAGE_PCT),
            "risk_sizing_enabled": bool(v12.RISK_SIZING_ENABLED),
            "risk_equity_rs": float(v12.RISK_EQUITY_RS),
            "risk_pct_per_trade": float(v12.RISK_PCT_PER_TRADE),
            "risk_min_notional_rs": float(v12.RISK_MIN_NOTIONAL_RS),
            "risk_max_notional_rs": float(v12.RISK_MAX_NOTIONAL_RS),
            "intraday_leverage": float(v12.V7_INTRADAY_LEVERAGE),
            "one_minute_gap_policy": "CONSERVATIVE_5MIN_FALLBACK",
            "same_bar_collision_policy": "STOP_FIRST",
        },
    }
    contract["contract_sha256"] = hashlib.sha256(
        json.dumps(json_safe(contract), sort_keys=True).encode("utf-8")
    ).hexdigest()
    return contract


def write_exact_cache_manifest(path: Path, exact: pd.DataFrame, raw: pd.DataFrame) -> None:
    contract = exact_cache_contract(raw)
    payload = {
        "contract": contract,
        "exact_rows": len(exact),
        "raw_rows": len(raw),
        "exact_sha256": sha256(path.parent / "exact_candidate_universe.parquet"),
        "raw_sha256": sha256(path.parent / "entry_engine_raw.parquet"),
        "fallback_rows": int(exact["path_fallback_applied"].sum()),
        "unresolved_rows": int((~exact["path_resolution_valid"]).sum()),
    }
    path.write_text(json.dumps(json_safe(payload), indent=2), encoding="utf-8")


def validate_exact_cache_manifest(path: Path, exact: pd.DataFrame, raw: pd.DataFrame) -> None:
    if not path.exists():
        raise RuntimeError("exact cache manifest missing; rebuild without --reuse-exact")
    payload = json.loads(path.read_text(encoding="utf-8"))
    current = exact_cache_contract(raw)
    if payload.get("contract", {}).get("contract_sha256") != current["contract_sha256"]:
        raise RuntimeError("exact cache provenance changed; rebuild without --reuse-exact")
    if payload.get("exact_sha256") != sha256(path.parent / "exact_candidate_universe.parquet"):
        raise RuntimeError("exact cache content hash mismatch")
    if payload.get("raw_sha256") != sha256(path.parent / "entry_engine_raw.parquet"):
        raise RuntimeError("raw entry cache content hash mismatch")
    if int(payload.get("exact_rows", -1)) != len(exact) or int(payload.get("raw_rows", -1)) != len(raw):
        raise RuntimeError("exact cache row-count mismatch")
    if "path_resolution_valid" not in exact or not exact["path_resolution_valid"].all():
        raise RuntimeError("exact cache contains unresolved exit paths")


def session_calendar() -> dict[str, list[str]]:
    source = pd.read_csv(v2.SESSION_SOURCE)
    days = sorted(source["trade_date"].astype(str).unique())
    days = [day for day in days if START_DATE <= day <= END_DATE]
    expected_total = sum(EXPECTED_SESSION_COUNTS.values())
    if len(days) != expected_total or days[0] != START_DATE or days[-1] != END_DATE:
        raise RuntimeError(
            f"unexpected one-month calendar: {len(days)} sessions, "
            f"bounds={days[:1]}..{days[-1:]}"
        )
    development = [day for day in days if day <= DEVELOPMENT_END]
    validation = [day for day in days if VALIDATION_START <= day <= VALIDATION_END]
    test = [day for day in days if day >= TEST_START]
    splits = {"all": days, "development": development, "validation": validation, "test": test}
    for label, expected in EXPECTED_SESSION_COUNTS.items():
        if len(splits[label]) != expected:
            raise RuntimeError(f"unexpected {label} session count: {len(splits[label])}")
    if set(development) & set(validation) or set(development) & set(test) or set(validation) & set(test):
        raise RuntimeError("chronological split overlap")
    return splits


def _num(frame: pd.DataFrame, column: str) -> pd.Series:
    return pd.to_numeric(frame[column], errors="coerce")


def add_causal_features(frame: pd.DataFrame) -> pd.DataFrame:
    work = frame.copy()
    work["ticker"] = work["ticker"].astype(str).str.upper().str.strip()
    work["trade_date"] = work["trade_date"].astype(str)
    work["signal_time_ist"] = pd.to_datetime(work["signal_time_ist"], errors="coerce", utc=True).dt.tz_convert("Asia/Kolkata")
    work = work.sort_values(
        ["ticker", "trade_date", "signal_time_ist", "selection_rank"],
        kind="mergesort",
    ).drop_duplicates(["ticker", "trade_date", "signal_time_ist"], keep="first")
    grouped = work.groupby(["ticker", "trade_date"], sort=False)
    previous_time = grouped["signal_time_ist"].shift(1)
    contiguous = (work["signal_time_ist"] - previous_time).dt.total_seconds().div(60).eq(5)
    work["contiguous_previous"] = contiguous.fillna(False)
    for column in ("ret_5m_pct", "vwap_dist_atr", "close_position_in_bar", "body_pct", "range_pct"):
        previous = grouped[column].shift(1)
        work[f"previous_{column}"] = previous.where(work["contiguous_previous"])
    ret5 = _num(work, "ret_5m_pct")
    ret15 = _num(work, "ret_15m_pct")
    work["return_acceleration_5_vs_15"] = ret5 - ret15.div(3.0)
    work["score_margin"] = _num(work, "long_score") - pd.concat(
        [_num(work, "short_score"), _num(work, "activity_score")], axis=1
    ).max(axis=1)
    work["bullish_reversal"] = (
        work["contiguous_previous"]
        & _num(work, "previous_ret_5m_pct").le(0.0)
        & ret5.gt(0.0)
    )
    work["vwap_reclaim"] = (
        work["contiguous_previous"]
        & _num(work, "previous_vwap_dist_atr").lt(0.0)
        & _num(work, "vwap_dist_atr").ge(0.0)
    )
    return work.reset_index(drop=True)


def load_union_candidates() -> tuple[pd.DataFrame, pd.DataFrame]:
    source = pd.read_parquet(v2.SOURCE, columns=list(SOURCE_COLUMNS))
    source = source.loc[source["trade_date"].astype(str).between(START_DATE, END_DATE)].copy()
    funnel: list[dict[str, Any]] = [{"stage": "one_month_source", "rows": len(source)}]
    source = add_causal_features(source)
    mask = source["pre_entry_data_invalid"].eq(False)
    mask &= source["primary_side"].astype(str).str.upper().eq("LONG")
    mask &= _num(source, "selection_rank").between(PREFILTER_RANK_MIN, PREFILTER_RANK_MAX)
    mask &= _num(source, "signal_minute").between(
        UNION_GUARDS["signal_minute_min"], UNION_GUARDS["signal_minute_max"]
    )
    mask &= _num(source, "atr_pct").ge(UNION_GUARDS["atr_pct_min"])
    mask &= _num(source, "session_return_so_far_pct").ge(
        UNION_GUARDS["session_return_so_far_pct_min"]
    )
    mask &= _num(source, "vwap_dist_atr").ge(UNION_GUARDS["vwap_dist_atr_min"])
    mask &= _num(source, "close_position_in_bar").ge(
        UNION_GUARDS["close_position_in_bar_min"]
    )
    candidates = source.loc[mask.fillna(False)].copy()
    candidates["setup"] = SETUP
    candidates["side"] = "LONG"
    candidates["bar_time_ist"] = candidates["signal_time_ist"]
    candidates["decision_ready_at_ist"] = candidates["signal_time_ist"]
    candidates["decision_ready_source"] = "completed_5min_signal_bar"
    candidates["quality_score"] = 301.0 - _num(candidates, "selection_rank")
    candidates["score"] = candidates["quality_score"]
    candidates["research_source_entry_time_ist"] = candidates["entry_execution_time_ist"]
    candidates["research_source_entry_price"] = candidates["entry_price"]
    candidates = candidates.sort_values(
        ["trade_date", "signal_time_ist", "selection_rank", "ticker"],
        kind="mergesort",
    ).reset_index(drop=True)
    candidates["_optimizer_row_id"] = np.arange(len(candidates), dtype=int)
    funnel.extend([
        {"stage": "causal_long_rank200_300_union_guards", "rows": len(candidates)},
        {
            "stage": "union_unique_ticker_days",
            "rows": int(candidates[["trade_date", "ticker"]].drop_duplicates().shape[0]),
        },
        {"stage": "union_active_sessions", "rows": int(candidates["trade_date"].nunique())},
    ])
    return candidates, pd.DataFrame(funnel)


def build_exact_universe(
    candidates: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    old_state = {
        "load_1m": getattr(v12, "_load_1m_with_open", None),
        "entry_bars": getattr(v12, "_entry_bars_for_signal", None),
        "day_loader": getattr(v12, "_optimizer_load_1m_day", None),
        "exact_parity": getattr(v12, "_V11_EXACT_LIVE_PARITY", None),
        "cost_model": getattr(v12, "_V11_COST_MODEL", None),
        "slippage_bps": getattr(v12, "_V11_SLIPPAGE_BPS", None),
        "exit_rule": v12.v6.SETUP_EXIT_RULES.get(SETUP),
    }
    loader = optimizer.install_windowed_1m_loader(
        v12, start_date=START_DATE, end_date=END_DATE
    )
    try:
        prewarm = optimizer.prewarm_windowed_1m_loader(loader, candidates["ticker"], workers=8)
        optimizer.install_day_1m_adapter(v12, loader)
        v12._V11_EXACT_LIVE_PARITY = False
        v12._V11_COST_MODEL = "statutory"
        v12._V11_SLIPPAGE_BPS = 0.0
        v12.v6.SETUP_EXIT_RULES[SETUP] = (STOP_LOSS_PCT, TARGET_PCT)
        raw, rejects = v12._v7_entry_engine_raw_rows(candidates)
        if raw.empty:
            raise RuntimeError("no executable rows in the union candidate pool")
        raw["_optimizer_row_id"] = pd.to_numeric(raw["_optimizer_row_id"], errors="raise").astype(int)
        outcomes = optimizer.resolve_exit_grid(
            raw,
            {SETUP: [(STOP_LOSS_PCT, TARGET_PCT)]},
            v12,
            progress_label="one-month-long-v9",
        )
    finally:
        if old_state["load_1m"] is not None:
            v12._load_1m_with_open = old_state["load_1m"]
        if old_state["entry_bars"] is not None:
            v12._entry_bars_for_signal = old_state["entry_bars"]
        if old_state["day_loader"] is None:
            if hasattr(v12, "_optimizer_load_1m_day"):
                delattr(v12, "_optimizer_load_1m_day")
        else:
            v12._optimizer_load_1m_day = old_state["day_loader"]
        for key, attribute in (
            ("exact_parity", "_V11_EXACT_LIVE_PARITY"),
            ("cost_model", "_V11_COST_MODEL"),
            ("slippage_bps", "_V11_SLIPPAGE_BPS"),
        ):
            if old_state[key] is not None:
                setattr(v12, attribute, old_state[key])
        if old_state["exit_rule"] is None:
            v12.v6.SETUP_EXIT_RULES.pop(SETUP, None)
        else:
            v12.v6.SETUP_EXIT_RULES[SETUP] = old_state["exit_rule"]

    if len(outcomes) != len(raw):
        raise RuntimeError(f"exact outcome coverage failure: {len(outcomes)}/{len(raw)}")
    outcome_fields = [
        "_optimizer_row_id", "entry_time_ist", "entry_price", "quantity",
        "sl_pct", "tgt_pct", "outcome", "exit_time_ist", "exit_price",
        "bars_held", "gross_pnl_rs", "cost_rs", "net_pnl_rs",
        "cost_rates_as_of",
    ]
    exact = raw.merge(
        outcomes[outcome_fields],
        on="_optimizer_row_id",
        how="inner",
        validate="one_to_one",
        suffixes=("", "_outcome"),
    )
    # Candidate/source prices and the entry-engine placeholder sizing collide
    # with the exact slipped fill and exact exit-pair quantity.  Canonical
    # trading columns must always mean the values used by the saved P&L.
    if "entry_price_outcome" not in exact or "quantity_outcome" not in exact:
        raise RuntimeError("exact fill/quantity columns missing after outcome merge")
    exact["entry_engine_candidate_entry_price"] = pd.to_numeric(
        exact["entry_price"], errors="coerce"
    )
    exact["entry_engine_default_quantity"] = pd.to_numeric(
        exact["quantity"], errors="coerce"
    )
    exact["entry_price"] = pd.to_numeric(
        exact["entry_price_outcome"], errors="raise"
    )
    exact["quantity"] = pd.to_numeric(
        exact["quantity_outcome"], errors="raise"
    ).astype(int)
    expected_gross = (
        pd.to_numeric(exact["exit_price"], errors="raise")
        - pd.to_numeric(exact["entry_price"], errors="raise")
    ) * pd.to_numeric(exact["quantity"], errors="raise")
    if not np.allclose(
        expected_gross.to_numpy(dtype=float),
        pd.to_numeric(exact["gross_pnl_rs"], errors="raise").to_numpy(dtype=float),
        atol=1e-6,
        rtol=0.0,
    ):
        raise RuntimeError("canonical exact-fill gross-P&L invariant failed")
    exact["trade_date"] = pd.to_datetime(exact["entry_time_ist"], utc=True).dt.tz_convert("Asia/Kolkata").dt.strftime("%Y-%m-%d")
    exact["signal_time_ist"] = pd.to_datetime(exact["signal_time_ist"], errors="coerce", utc=True).dt.tz_convert("Asia/Kolkata")
    exact["entry_time_ist"] = pd.to_datetime(exact["entry_time_ist"], errors="coerce", utc=True).dt.tz_convert("Asia/Kolkata")
    exact["exit_time_ist"] = pd.to_datetime(exact["exit_time_ist"], errors="coerce", utc=True).dt.tz_convert("Asia/Kolkata")
    exact = exact.sort_values(
        ["trade_date", "signal_time_ist", "selection_rank", "ticker"],
        kind="mergesort",
    ).reset_index(drop=True)
    exact = apply_one_minute_coverage_fallback(exact, loader)
    return exact, raw, rejects, prewarm


def apply_one_minute_coverage_fallback(
    exact: pd.DataFrame,
    one_minute_loader: Any,
) -> pd.DataFrame:
    """Use conservative 5m paths when a selected 1m path has minute gaps.

    The historical five-minute bars are timestamped at bar end.  Because every
    setup signal is a completed five-minute bar and the exact entry is the
    following minute, the first five-minute fallback bar contains only
    post-entry minutes.  The shared resolver is conservative when a single OHLC
    bar touches both stop and target: stop wins.
    """

    start = pd.Timestamp(START_DATE, tz="Asia/Kolkata")
    end = pd.Timestamp(END_DATE, tz="Asia/Kolkata") + pd.Timedelta(days=1)

    @lru_cache(maxsize=None)
    def load_five_minute(ticker: str) -> pd.DataFrame | None:
        path = (
            v12.V7_HIST_INDICATORS_5M_DIR
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
        bars = v12._normalise_bars_date_index(frame, naive_tz="UTC")
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
        entry = v12._normalise_ts(row["entry_time_ist"])
        original_exit = v12._normalise_ts(row["exit_time_ist"])
        one = one_minute_loader(ticker)
        if one is None or one.empty or pd.isna(entry) or pd.isna(original_exit):
            gap_count = -1
        else:
            day_one = one.loc[
                (one.index.normalize() == entry.normalize())
                & (one.index >= entry)
                & (one.index <= original_exit)
            ]
            expected = pd.date_range(entry.floor("min"), original_exit.floor("min"), freq="1min")
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
        result = v12.er.resolve(
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
        exit_time = v12._normalise_ts(result.exit_time_ist)
        expected_five = pd.date_range(entry.ceil("5min"), exit_time.floor("5min"), freq="5min")
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
        costs = nse.intraday_equity_costs(
            float(row["entry_price"]), float(result.exit_price), int(row["quantity"]), "LONG"
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
        raise RuntimeError(f"{unresolved} exact rows lack both complete 1m and 5m fallback paths")
    expected_gross = (
        pd.to_numeric(work["exit_price"], errors="raise")
        - pd.to_numeric(work["entry_price"], errors="raise")
    ) * pd.to_numeric(work["quantity"], errors="raise")
    if not np.allclose(
        expected_gross.to_numpy(dtype=float),
        pd.to_numeric(work["gross_pnl_rs"], errors="raise").to_numpy(dtype=float),
        atol=1e-6,
        rtol=0.0,
    ):
        raise RuntimeError("fallback-adjusted gross-P&L invariant failed")
    return work


def _choice(rng: np.random.Generator, values: Sequence[Any]) -> Any:
    value = values[int(rng.integers(0, len(values)))]
    return value.item() if isinstance(value, np.generic) else value


def _common_random(rng: np.random.Generator) -> dict[str, Any]:
    rank_bands = (
        (200, 240), (200, 260), (200, 280), (200, 300),
        (220, 260), (220, 280), (220, 300), (240, 280),
        (240, 300), (260, 300),
    )
    rank_min, rank_max = _choice(rng, rank_bands)
    start = int(_choice(rng, (570, 585, 600, 615, 630, 645, 660, 690)))
    valid_ends = [value for value in (720, 750, 780, 810, 840, 855) if value >= start + 60]
    return {
        "rank_min": rank_min,
        "rank_max": rank_max,
        "signal_minute_min": start,
        "signal_minute_max": int(_choice(rng, valid_ends)),
        "atr_pct_min": float(_choice(rng, (0.35, 0.45, 0.55, 0.65, 0.80, 1.00))),
        "session_return_min": float(_choice(rng, (0.0, 0.25, 0.50, 0.75, 1.0, 1.5, 2.0, 3.0))),
        "vwap_dist_atr_min": float(_choice(rng, (-0.50, -0.25, 0.0, 0.25, 0.50, 1.0))),
        "close_position_min": float(_choice(rng, (0.35, 0.45, 0.55, 0.65, 0.75, 0.85))),
        "range_pct_min": _choice(rng, (None, None, 0.20, 0.35, 0.50, 0.75, 1.0)),
        "score_margin_min": _choice(rng, (None, None, -0.10, -0.05, 0.0, 0.05)),
    }


def _random_config(rng: np.random.Generator, family: str, ordinal: int) -> RuleConfig:
    values: dict[str, Any] = {
        "config_id": f"R{ordinal:06d}_{family}",
        "family": family,
        **_common_random(rng),
    }
    if family == "MOMENTUM_CONTINUATION":
        values.update({
            "ret_5m_min": _choice(rng, (-0.10, 0.0, 0.10, 0.20, 0.35, 0.50)),
            "ret_15m_min": _choice(rng, (None, -0.20, 0.0, 0.20, 0.50, 0.80, 1.20)),
            "ret_30m_min": _choice(rng, (None, None, -0.20, 0.0, 0.40, 0.80, 1.20)),
            "adx_min": _choice(rng, (None, None, 20.0, 25.0, 30.0, 35.0, 40.0)),
            "rsi_min": _choice(rng, (None, 45.0, 50.0, 55.0, 60.0, 65.0)),
            "rsi_max": _choice(rng, (None, None, 75.0, 80.0, 85.0, 90.0)),
            "volume_ratio20_min": _choice(rng, (None, None, 0.20, 0.40, 0.70, 1.0, 1.5)),
        })
    elif family == "NEAR_HIGH_BREAKOUT":
        values.update({
            "ret_5m_min": _choice(rng, (0.0, 0.10, 0.20, 0.35, 0.50, 0.75)),
            "ret_15m_min": _choice(rng, (None, -0.20, 0.0, 0.20, 0.50, 0.80)),
            "running_high_distance_atr_min": _choice(rng, (-1.50, -1.0, -0.50, -0.25, 0.0, 0.25)),
            "upper_wick_pct_max": _choice(rng, (None, 0.10, 0.20, 0.30, 0.50, 0.80)),
            "volume_ratio20_min": _choice(rng, (None, None, 0.30, 0.60, 1.0, 1.5, 2.0)),
            "adx_min": _choice(rng, (None, None, 20.0, 25.0, 30.0, 35.0)),
        })
    elif family == "VWAP_RECLAIM":
        values.update({
            "vwap_dist_atr_min": float(_choice(rng, (0.0, 0.05, 0.10, 0.20))),
            "ret_5m_min": _choice(rng, (0.0, 0.05, 0.10, 0.20)),
            "ret_15m_min": _choice(rng, (None, -0.50, -0.20, 0.0, 0.20)),
            "previous_vwap_dist_atr_max": _choice(rng, (-0.20, -0.10, 0.0, 0.10)),
            "previous_ret_5m_max": _choice(rng, (None, -0.10, 0.0, 0.10)),
            "require_contiguous_previous": True,
            "require_vwap_reclaim": bool(_choice(rng, (False, True))),
            "volume_ratio20_min": _choice(rng, (None, None, 0.20, 0.50, 1.0)),
        })
    elif family == "PULLBACK_BOUNCE":
        values.update({
            "ret_5m_min": _choice(rng, (-0.10, 0.0, 0.05, 0.10)),
            "ret_5m_max": _choice(rng, (0.35, 0.50, 0.75, 1.0, None)),
            "ret_15m_min": _choice(rng, (None, -0.20, 0.0, 0.20, 0.50)),
            "ret_30m_min": _choice(rng, (None, 0.0, 0.30, 0.60, 1.0)),
            "previous_ret_5m_max": _choice(rng, (-0.20, -0.10, 0.0, 0.10)),
            "ema20_dist_atr_min": _choice(rng, (None, -0.50, 0.0, 0.50)),
            "ema20_dist_atr_max": _choice(rng, (None, 1.0, 1.5, 2.0, 3.0)),
            "require_contiguous_previous": True,
            "require_bullish_reversal": bool(_choice(rng, (False, True))),
        })
    elif family == "TREND_ACCELERATION":
        values.update({
            "ret_5m_min": _choice(rng, (-0.05, 0.0, 0.10, 0.20, 0.35)),
            "ret_15m_min": _choice(rng, (-0.20, 0.0, 0.20, 0.50, 0.80)),
            "ret_30m_min": _choice(rng, (None, -0.20, 0.0, 0.40, 0.80)),
            "return_acceleration_min": _choice(rng, (-0.20, -0.10, 0.0, 0.10, 0.20, 0.35)),
            "ema20_dist_atr_min": _choice(rng, (None, -0.50, 0.0, 0.50, 1.0)),
            "ema50_dist_atr_min": _choice(rng, (None, -0.50, 0.0, 0.50, 1.0)),
            "adx_min": _choice(rng, (None, None, 20.0, 25.0, 30.0, 35.0)),
            "rsi_min": _choice(rng, (None, 45.0, 50.0, 55.0, 60.0)),
            "rsi_max": _choice(rng, (None, None, 75.0, 80.0, 85.0)),
        })
    else:  # pragma: no cover - generator contract
        raise ValueError(f"unknown family: {family}")
    if values.get("rsi_min") is not None and values.get("rsi_max") is not None:
        if float(values["rsi_min"]) >= float(values["rsi_max"]):
            values["rsi_max"] = None
    if values.get("ema20_dist_atr_min") is not None and values.get("ema20_dist_atr_max") is not None:
        if float(values["ema20_dist_atr_min"]) >= float(values["ema20_dist_atr_max"]):
            values["ema20_dist_atr_max"] = None
    return RuleConfig(**values)


def generate_configurations(total: int) -> list[RuleConfig]:
    if total < 100:
        raise ValueError("at least 100 search trials are required")
    rng = np.random.default_rng(SEARCH_SEED)
    families = (
        "MOMENTUM_CONTINUATION", "NEAR_HIGH_BREAKOUT", "VWAP_RECLAIM",
        "PULLBACK_BOUNCE", "TREND_ACCELERATION",
    )
    configs: list[RuleConfig] = []
    seen: set[str] = set()
    ordinal = 1
    while len(configs) < total:
        family = families[(ordinal - 1) % len(families)]
        config = _random_config(rng, family, ordinal)
        canonical = asdict(config)
        canonical.pop("config_id")
        key = json.dumps(json_safe(canonical), sort_keys=True)
        if key not in seen:
            seen.add(key)
            configs.append(config)
        ordinal += 1
    return configs


class SearchArrays:
    def __init__(self, frame: pd.DataFrame, sessions: Sequence[str]):
        self.frame = frame.reset_index(drop=True)
        self.sessions = list(sessions)
        self.day_lookup = {day: index for index, day in enumerate(self.sessions)}
        self.day_code = self.frame["trade_date"].map(self.day_lookup).to_numpy(dtype=int)
        ticker_day = self.frame["trade_date"].astype(str) + "|" + self.frame["ticker"].astype(str)
        self.ticker_day_code = pd.factorize(ticker_day, sort=False)[0]
        self.ticker_code = pd.factorize(self.frame["ticker"].astype(str), sort=False)[0]
        self.pnl = _num(self.frame, "net_pnl_rs").to_numpy(dtype=float)
        self.gross = _num(self.frame, "gross_pnl_rs").to_numpy(dtype=float)
        self.cost = _num(self.frame, "cost_rs").to_numpy(dtype=float)
        exit_times = pd.to_datetime(self.frame["exit_time_ist"], errors="coerce", utc=True)
        if exit_times.isna().any():
            raise RuntimeError("exact search universe has missing exit timestamps")
        self.exit_time_ns = exit_times.astype("int64").to_numpy(dtype=np.int64)
        numeric_columns = {
            "selection_rank", "signal_minute", "atr_pct", "session_return_so_far_pct",
            "vwap_dist_atr", "close_position_in_bar", "range_pct", "ret_5m_pct",
            "ret_15m_pct", "ret_30m_pct", "ret_60m_pct",
            "return_acceleration_5_vs_15", "ADX", "RSI", "volume_ratio20",
            "upper_wick_pct", "distance_from_running_session_high_atr",
            "ema20_dist_atr", "ema50_dist_atr", "score_margin",
            "previous_ret_5m_pct", "previous_vwap_dist_atr",
        }
        self.values = {
            column: _num(self.frame, column).to_numpy(dtype=float)
            for column in numeric_columns
        }
        self.flags = {
            "contiguous_previous": self.frame["contiguous_previous"].fillna(False).to_numpy(dtype=bool),
            "bullish_reversal": self.frame["bullish_reversal"].fillna(False).to_numpy(dtype=bool),
            "vwap_reclaim": self.frame["vwap_reclaim"].fillna(False).to_numpy(dtype=bool),
        }

    def mask(self, config: RuleConfig) -> np.ndarray:
        v = self.values
        mask = (
            (v["selection_rank"] >= config.rank_min)
            & (v["selection_rank"] <= config.rank_max)
            & (v["signal_minute"] >= config.signal_minute_min)
            & (v["signal_minute"] <= config.signal_minute_max)
            & (v["atr_pct"] >= config.atr_pct_min)
            & (v["session_return_so_far_pct"] >= config.session_return_min)
            & (v["vwap_dist_atr"] >= config.vwap_dist_atr_min)
            & (v["close_position_in_bar"] >= config.close_position_min)
        )
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
            ("distance_from_running_session_high_atr", config.running_high_distance_atr_min, ">="),
            ("distance_from_running_session_high_atr", config.running_high_distance_atr_max, "<="),
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
            current = v[column]
            mask &= current >= float(threshold) if operator == ">=" else current <= float(threshold)
        if config.require_contiguous_previous:
            mask &= self.flags["contiguous_previous"]
        if config.require_bullish_reversal:
            mask &= self.flags["bullish_reversal"]
        if config.require_vwap_reclaim:
            mask &= self.flags["vwap_reclaim"]
        return mask

    def selected_indices(self, config: RuleConfig) -> np.ndarray:
        eligible = np.flatnonzero(self.mask(config))
        if not len(eligible):
            return np.asarray([], dtype=int)
        _, first_positions = np.unique(self.ticker_day_code[eligible], return_index=True)
        selected = eligible[np.sort(first_positions)]
        if not len(selected):
            return selected
        kept: list[np.ndarray] = []
        selected_days = self.day_code[selected]
        for day in np.unique(selected_days):
            day_rows = selected[selected_days == day]
            kept.append(day_rows[:DAILY_CAP])
        return np.concatenate(kept) if kept else np.asarray([], dtype=int)


def performance_from_indices(
    arrays: SearchArrays,
    indices: np.ndarray,
    session_positions: Sequence[int],
    *,
    cost_multiplier: float = 1.0,
) -> dict[str, Any]:
    positions = np.asarray(list(session_positions), dtype=int)
    wanted = np.isin(arrays.day_code[indices], positions) if len(indices) else np.asarray([], dtype=bool)
    chosen = indices[wanted] if len(indices) else np.asarray([], dtype=int)
    pnl = arrays.gross[chosen] - float(cost_multiplier) * arrays.cost[chosen]
    gains = float(pnl[pnl > 0].sum())
    losses = float(-pnl[pnl < 0].sum())
    profit_factor = gains / losses if losses > 0 else (float("inf") if gains > 0 else 0.0)
    median_abs = float(np.median(np.abs(pnl))) if len(pnl) else 0.0
    prior = 2.5 * median_abs
    shrunk_pf = (gains + prior) / (losses + prior) if losses + prior > 0 else 0.0
    daily_trades = np.zeros(len(positions), dtype=int)
    daily_pnl = np.zeros(len(positions), dtype=float)
    position_lookup = {day: offset for offset, day in enumerate(positions)}
    for idx, value in zip(chosen, pnl):
        offset = position_lookup[arrays.day_code[idx]]
        daily_trades[offset] += 1
        daily_pnl[offset] += float(value)
    realized_pnl = pnl[np.argsort(arrays.exit_time_ns[chosen], kind="stable")]
    equity = np.cumsum(realized_pnl)
    peaks = np.maximum.accumulate(np.concatenate(([0.0], equity)))[1:] if len(equity) else np.asarray([])
    drawdown = equity - peaks if len(equity) else np.asarray([])
    positive_days = daily_pnl[daily_pnl > 0]
    best_day_concentration = (
        float(positive_days.max() / positive_days.sum()) if len(positive_days) and positive_days.sum() > 0 else 0.0
    )
    return {
        "sessions": int(len(positions)),
        "trades": int(len(chosen)),
        "trades_per_session": float(len(chosen) / len(positions)) if len(positions) else 0.0,
        "median_trades_per_session": float(np.median(daily_trades)) if len(positions) else 0.0,
        "active_days": int(np.count_nonzero(daily_trades)),
        "zero_trade_days": int(np.count_nonzero(daily_trades == 0)),
        "net_pnl_rs": float(pnl.sum()),
        "gross_profit_rs": gains,
        "gross_loss_rs": losses,
        "profit_factor": profit_factor,
        "shrunk_profit_factor": float(shrunk_pf),
        "win_rate_pct": float(np.count_nonzero(pnl > 0) / len(pnl) * 100.0) if len(pnl) else 0.0,
        "max_drawdown_rs": float(drawdown.min()) if len(drawdown) else 0.0,
        "max_drawdown_basis": "realized_exit_order",
        "positive_days": int(np.count_nonzero(daily_pnl > 0)),
        "best_day_positive_pnl_share": best_day_concentration,
    }


def _stage_score(metric: Mapping[str, Any]) -> float:
    pf = max(float(metric["shrunk_profit_factor"]), 1e-9)
    frequency = min(float(metric["trades_per_session"]), 8.0)
    return float(math.log(pf) + 0.25 * math.log(max(frequency, 0.25) / 3.0))


def _signature(indices: np.ndarray) -> str:
    return hashlib.sha256(np.asarray(indices, dtype=np.int64).tobytes()).hexdigest()


def run_search(
    exact: pd.DataFrame,
    splits: Mapping[str, Sequence[str]],
    *,
    trials: int,
) -> tuple[pd.DataFrame, pd.DataFrame, RuleConfig, np.ndarray, dict[str, Any]]:
    arrays = SearchArrays(exact, splits["all"])
    configs = generate_configurations(trials)
    by_id = {config.config_id: config for config in configs}
    folds = (range(0, 4), range(4, 8), range(8, 12))
    ledger: dict[str, dict[str, Any]] = {
        config.config_id: {**json_safe(asdict(config)), "search_stage": "GENERATED"}
        for config in configs
    }

    stage1: list[tuple[float, str]] = []
    for count, config in enumerate(configs, 1):
        selected = arrays.selected_indices(config)
        metric = performance_from_indices(arrays, selected, folds[0])
        record = ledger[config.config_id]
        record.update({f"fold1_{key}": value for key, value in metric.items()})
        record["search_stage"] = "FOLD1"
        if metric["trades"] >= 8 and metric["active_days"] >= 3:
            stage1.append((_stage_score(metric), config.config_id))
        if count % 10_000 == 0 or count == len(configs):
            print(f"[search] fold1 {count:,}/{len(configs):,} eligible={len(stage1):,}", flush=True)
    stage1.sort(reverse=True)
    stage1 = stage1[: min(30_000, max(2_000, int(len(configs) * 0.30)))]

    stage2: list[tuple[float, str]] = []
    for count, (_, config_id) in enumerate(stage1, 1):
        config = by_id[config_id]
        selected = arrays.selected_indices(config)
        metric = performance_from_indices(arrays, selected, folds[1])
        ledger[config_id].update({f"fold2_{key}": value for key, value in metric.items()})
        ledger[config_id]["search_stage"] = "FOLD2"
        fold1_score = _stage_score({
            key.removeprefix("fold1_"): value
            for key, value in ledger[config_id].items() if key.startswith("fold1_")
        })
        robust = min(fold1_score, _stage_score(metric))
        if metric["trades"] >= 8 and metric["active_days"] >= 3:
            stage2.append((robust, config_id))
        if count % 5_000 == 0 or count == len(stage1):
            print(f"[search] fold2 {count:,}/{len(stage1):,} eligible={len(stage2):,}", flush=True)
    stage2.sort(reverse=True)
    stage2 = stage2[: min(8_000, max(1_000, int(len(stage1) * 0.30)))]

    development_positions = range(0, 12)
    dev_survivors: list[tuple[float, str, np.ndarray]] = []
    for count, (_, config_id) in enumerate(stage2, 1):
        config = by_id[config_id]
        selected = arrays.selected_indices(config)
        fold3 = performance_from_indices(arrays, selected, folds[2])
        development = performance_from_indices(arrays, selected, development_positions)
        ledger[config_id].update({f"fold3_{key}": value for key, value in fold3.items()})
        ledger[config_id].update({f"development_{key}": value for key, value in development.items()})
        ledger[config_id]["development_signature"] = _signature(
            selected[np.isin(arrays.day_code[selected], list(development_positions))]
        )
        ledger[config_id]["search_stage"] = "DEVELOPMENT_COMPLETE"
        fold_metrics = []
        for number in (1, 2):
            fold_metrics.append({
                key.removeprefix(f"fold{number}_"): value
                for key, value in ledger[config_id].items() if key.startswith(f"fold{number}_")
            })
        fold_metrics.append(fold3)
        fold_scores = [_stage_score(item) for item in fold_metrics]
        robust_score = float(np.median(fold_scores) - 0.5 * np.std(fold_scores))
        ledger[config_id]["development_robust_score"] = robust_score
        hard_gate = (
            development["trades"] >= 36
            and development["active_days"] >= 10
            and development["median_trades_per_session"] >= 3.0
            and development["net_pnl_rs"] > 0
            and all(
                item["trades"] >= 8
                and item["active_days"] >= 3
                and item["net_pnl_rs"] > 0
                and item["profit_factor"] >= 1.0
                for item in fold_metrics
            )
        )
        ledger[config_id]["development_gate"] = bool(hard_gate)
        if hard_gate:
            dev_survivors.append((robust_score, config_id, selected))
        if count % 1_000 == 0 or count == len(stage2):
            print(f"[search] development {count:,}/{len(stage2):,} gated={len(dev_survivors):,}", flush=True)

    if not dev_survivors:
        # Retain the best sufficiently active positive candidates instead of
        # silently weakening the gate; the final verdict will remain failed.
        fallback = []
        for _, config_id in stage2:
            record = ledger[config_id]
            if record.get("development_trades", 0) >= 24 and record.get("development_active_days", 0) >= 8:
                selected = arrays.selected_indices(by_id[config_id])
                fallback.append((float(record.get("development_robust_score", -999.0)), config_id, selected))
        dev_survivors = fallback
    if not dev_survivors:
        raise RuntimeError("search produced no development candidates with usable frequency")

    dev_survivors.sort(key=lambda item: item[0], reverse=True)
    deduped: list[tuple[float, str, np.ndarray]] = []
    seen_signatures: set[str] = set()
    family_counts: dict[str, int] = {}
    for item in dev_survivors:
        _, config_id, selected = item
        signature = str(ledger[config_id]["development_signature"])
        family = by_id[config_id].family
        if signature in seen_signatures or family_counts.get(family, 0) >= 3:
            continue
        # Avoid validating nearly identical entry lists.
        dev_ids = set(selected[np.isin(arrays.day_code[selected], list(development_positions))].tolist())
        too_close = False
        for _, _, prior_selected in deduped:
            prior_ids = set(prior_selected[np.isin(arrays.day_code[prior_selected], list(development_positions))].tolist())
            union = len(dev_ids | prior_ids)
            if union and len(dev_ids & prior_ids) / union > 0.90:
                too_close = True
                break
        if too_close:
            continue
        seen_signatures.add(signature)
        family_counts[family] = family_counts.get(family, 0) + 1
        deduped.append(item)
        if len(deduped) >= MAX_VALIDATION_FINALISTS:
            break
    if not deduped:
        raise RuntimeError("no structurally diverse validation finalists")

    validation_positions = range(12, 17)
    validation_rows: list[dict[str, Any]] = []
    champion_candidates: list[tuple[float, str, np.ndarray]] = []
    for dev_score, config_id, selected in deduped:
        metric = performance_from_indices(arrays, selected, validation_positions)
        record = ledger[config_id]
        record.update({f"validation_{key}": value for key, value in metric.items()})
        record["search_stage"] = "VALIDATION_COMPLETE"
        validation_gate = (
            metric["trades"] >= 15
            and metric["active_days"] >= 4
            and metric["net_pnl_rs"] > 0
            and metric["profit_factor"] >= 1.25
        )
        record["validation_gate"] = bool(validation_gate)
        combined_score = float(
            min(dev_score, _stage_score(metric))
            + 0.15 * math.log(max(metric["trades_per_session"], 0.25) / 3.0)
        )
        record["selection_score"] = combined_score
        validation_rows.append(record.copy())
        if validation_gate:
            champion_candidates.append((combined_score, config_id, selected))

    if not champion_candidates:
        # Lock the best validation result, but preserve the failed-gate verdict.
        champion_candidates = [
            (float(ledger[config_id].get("selection_score", -999.0)), config_id, selected)
            for _, config_id, selected in deduped
        ]
    champion_candidates.sort(key=lambda item: item[0], reverse=True)
    _, champion_id, champion_selected = champion_candidates[0]
    champion = by_id[champion_id]
    freeze = {
        "config": json_safe(asdict(champion)),
        "config_sha256": config_hash(champion),
        "selection_used_through": VALIDATION_END,
        "test_start": TEST_START,
        "test_was_not_used_for_selection": True,
        "development_gate_passed": bool(ledger[champion_id].get("development_gate", False)),
        "validation_gate_passed": bool(ledger[champion_id].get("validation_gate", False)),
    }
    return (
        pd.DataFrame(ledger.values()),
        pd.DataFrame(validation_rows),
        champion,
        champion_selected,
        freeze,
    )


def selected_trade_frame(exact: pd.DataFrame, indices: np.ndarray) -> pd.DataFrame:
    trades = exact.iloc[indices].copy()
    trades["daily_sequence"] = trades.groupby("trade_date", sort=False).cumcount() + 1
    if trades["daily_sequence"].gt(DAILY_CAP).any():
        raise RuntimeError("daily cap breach")
    if trades.duplicated(["trade_date", "ticker"]).any():
        raise RuntimeError("one-ticker/day breach")
    return trades.reset_index(drop=True)


def detailed_performance(trades: pd.DataFrame, sessions: Sequence[str], *, cost_multiplier: float = 1.0) -> dict[str, Any]:
    day = pd.DataFrame({"trade_date": list(sessions)})
    scoped = trades.loc[trades["trade_date"].isin(sessions)].copy()
    scoped["stress_net_pnl_rs"] = _num(scoped, "gross_pnl_rs") - float(cost_multiplier) * _num(scoped, "cost_rs")
    grouped = scoped.groupby("trade_date", as_index=False).agg(
        trades=("ticker", "size"), net_pnl_rs=("stress_net_pnl_rs", "sum")
    )
    day = day.merge(grouped, on="trade_date", how="left").fillna(0)
    pnl = scoped["stress_net_pnl_rs"]
    gains = float(pnl.loc[pnl > 0].sum())
    losses = float(-pnl.loc[pnl < 0].sum())
    pf = gains / losses if losses else (float("inf") if gains else 0.0)
    realized = scoped.sort_values(["exit_time_ist", "entry_time_ist", "ticker"], kind="mergesort")
    equity = realized["stress_net_pnl_rs"].cumsum()
    drawdown = equity - equity.cummax().clip(lower=0)
    positive_by_ticker = scoped.loc[scoped["stress_net_pnl_rs"] > 0].groupby("ticker")["stress_net_pnl_rs"].sum()
    ticker_share = float(positive_by_ticker.max() / positive_by_ticker.sum()) if len(positive_by_ticker) and positive_by_ticker.sum() > 0 else 0.0
    positive_days = day.loc[day["net_pnl_rs"] > 0, "net_pnl_rs"]
    day_share = float(positive_days.max() / positive_days.sum()) if len(positive_days) and positive_days.sum() > 0 else 0.0
    return {
        "sessions": len(sessions),
        "trades": len(scoped),
        "trades_per_session": float(len(scoped) / len(sessions)),
        "median_trades_per_session": float(day["trades"].median()),
        "active_days": int(day["trades"].gt(0).sum()),
        "zero_trade_days": int(day["trades"].eq(0).sum()),
        "net_pnl_rs": float(pnl.sum()),
        "profit_factor": pf,
        "win_rate_pct": float(pnl.gt(0).mean() * 100.0) if len(pnl) else 0.0,
        "max_drawdown_rs": float(drawdown.min()) if len(drawdown) else 0.0,
        "max_drawdown_basis": "realized_exit_order",
        "positive_days": int(day["net_pnl_rs"].gt(0).sum()),
        "largest_ticker_positive_pnl_share": ticker_share,
        "largest_day_positive_pnl_share": day_share,
        "cost_multiplier": float(cost_multiplier),
    }


def daily_results(trades: pd.DataFrame, sessions: Sequence[str]) -> pd.DataFrame:
    day = pd.DataFrame({"trade_date": list(sessions)})
    grouped = trades.groupby("trade_date", as_index=False).agg(
        trades=("ticker", "size"),
        winners=("net_pnl_rs", lambda values: int(pd.to_numeric(values, errors="coerce").gt(0).sum())),
        gross_pnl_rs=("gross_pnl_rs", "sum"),
        cost_rs=("cost_rs", "sum"),
        net_pnl_rs=("net_pnl_rs", "sum"),
    )
    day = day.merge(grouped, on="trade_date", how="left").fillna(0)
    day["trades"] = day["trades"].astype(int)
    day["winners"] = day["winners"].astype(int)
    day["cumulative_net_pnl_rs"] = day["net_pnl_rs"].cumsum()
    day["split"] = np.select(
        [day["trade_date"].le(DEVELOPMENT_END), day["trade_date"].le(VALIDATION_END)],
        ["DEVELOPMENT", "VALIDATION"],
        default="LOCKED_TEST",
    )
    return day


def perturbation_configs(config: RuleConfig) -> list[tuple[str, RuleConfig]]:
    rows: list[tuple[str, RuleConfig]] = [("BASE", config)]
    rows.extend([
        ("RANK_MINUS_10", replace(config, rank_min=max(PREFILTER_RANK_MIN, config.rank_min - 10), rank_max=max(config.rank_min, config.rank_max - 10))),
        ("RANK_PLUS_10", replace(config, rank_min=min(config.rank_max, config.rank_min + 10), rank_max=min(PREFILTER_RANK_MAX, config.rank_max + 10))),
        ("WINDOW_EARLIER_15", replace(config, signal_minute_min=max(570, config.signal_minute_min - 15), signal_minute_max=max(config.signal_minute_min, config.signal_minute_max - 15))),
        ("WINDOW_LATER_15", replace(config, signal_minute_min=min(config.signal_minute_max, config.signal_minute_min + 15), signal_minute_max=min(855, config.signal_minute_max + 15))),
    ])
    for name in (
        "atr_pct_min", "session_return_min", "vwap_dist_atr_min",
        "close_position_min", "ret_5m_min", "ret_15m_min", "adx_min",
        "volume_ratio20_min", "running_high_distance_atr_min",
    ):
        value = getattr(config, name)
        if value is None or float(value) == 0.0:
            continue
        delta = abs(float(value)) * 0.10
        for candidate_value, suffix in (
            (float(value) - delta, "LOOSER10"),
            (float(value) + delta, "TIGHTER10"),
        ):
            rows.append((f"{name.upper()}_{suffix}", replace(config, **{name: candidate_value})))
    unique: list[tuple[str, RuleConfig]] = []
    seen: set[str] = set()
    for label, candidate in rows:
        key = config_hash(candidate)
        if key not in seen:
            seen.add(key)
            unique.append((label, candidate))
    return unique


def write_config(path: Path, config: RuleConfig) -> None:
    conditions = []
    for name, value in asdict(config).items():
        if name in {"config_id", "family"} or value is None or name.startswith("require_"):
            continue
        conditions.append((name, value))
    text = f'''"""Frozen research-only V12 LONG setup candidate.

Selected using development through {DEVELOPMENT_END} and validation through
{VALIDATION_END}; the locked test starts {TEST_START}.  This file is not wired
into production.
"""

PRODUCTION_APPROVED = False
RESEARCH_ONLY = True
SETUP_NAME = {SETUP!r}
CONFIG_ID = {config.config_id!r}
CONFIG_SHA256 = {config_hash(config)!r}
FAMILY = {config.family!r}

PREFILTER_PRIMARY_SIDE = "LONG"
PREFILTER_RANK_MIN = {config.rank_min!r}
PREFILTER_RANK_MAX = {config.rank_max!r}
PREFILTER_JOB_CHANGED = False

SIGNAL_TIMEFRAME = "5min_completed_bar"
ENTRY_TIMEFRAME = "exact_next_available_1min"
EXIT_TIMEFRAME = "exact_1min"
ONE_TICKER_PER_DAY = True
DAILY_CAP = {DAILY_CAP}
STOP_LOSS_PCT = {STOP_LOSS_PCT!r}
TARGET_PCT = {TARGET_PCT!r}
STATUTORY_COSTS = True
V12_RISK_SIZING = True
PAPER_ENTRY_SLIPPAGE_BPS = {float(v12.V7_PAPER_SLIPPAGE_PCT) * 10_000.0!r}
RISK_EQUITY_RS = {float(v12.RISK_EQUITY_RS)!r}
RISK_PCT_PER_TRADE = {float(v12.RISK_PCT_PER_TRADE)!r}
RISK_MIN_NOTIONAL_RS = {float(v12.RISK_MIN_NOTIONAL_RS)!r}
RISK_MAX_NOTIONAL_RS = {float(v12.RISK_MAX_NOTIONAL_RS)!r}
INTRADAY_LEVERAGE = {float(v12.V7_INTRADAY_LEVERAGE)!r}

ENTRY_SELECTION = "first chronological passing signal per ticker/day"
ENTRY_TIE_BREAK = ("signal_time_ist", "selection_rank", "ticker")
STOP_TARGET_SAME_BAR_POLICY = "STOP_FIRST"
ONE_MINUTE_GAP_POLICY = "CONSERVATIVE_5MIN_FALLBACK"
MISSING_FEATURE_POLICY = "FAIL_CLOSED"
PREFILTER_MEMBERSHIP_POLICY = "LONG at signal hour; same hourly list valid within that hour"

CONDITIONS = {tuple(conditions)!r}
REQUIRE_CONTIGUOUS_PREVIOUS = {config.require_contiguous_previous!r}
REQUIRE_BULLISH_REVERSAL = {config.require_bullish_reversal!r}
REQUIRE_VWAP_RECLAIM = {config.require_vwap_reclaim!r}

SELECTION_WINDOW = ({START_DATE!r}, {VALIDATION_END!r})
LOCKED_TEST_WINDOW = ({TEST_START!r}, {END_DATE!r})
'''
    path.write_text(text, encoding="utf-8")


def write_report(path: Path, summary: Mapping[str, Any]) -> None:
    full = summary["results"]["full_month"]
    test = summary["results"]["locked_test"]
    verdict = summary["verdict"]
    text = f"""# One-month V12 LONG setup-logic research

## Outcome

- Verdict: **{verdict}**
- Search trials: {summary['search']['generated_trials']:,}
- Selected family/config: `{summary['champion']['family']}` / `{summary['champion']['config_id']}`
- Full 22 sessions: {full['trades']} trades ({full['trades_per_session']:.2f}/session), net Rs {full['net_pnl_rs']:,.2f}, PF {full['profit_factor']:.3f}, max drawdown Rs {full['max_drawdown_rs']:,.2f}.
- Locked 5-session test: {test['trades']} trades ({test['trades_per_session']:.2f}/session), net Rs {test['net_pnl_rs']:,.2f}, PF {test['profit_factor']:.3f}.

## Method

The hourly prefilter was left unchanged.  Only its LONG-marked rank-200-300
rows were eligible.  Entry-rule search used July 6-21, ten diverse finalists
at most were opened on July 22-28, one configuration was frozen, and July
29-August 4 was then opened once.  Search held exits fixed at SL 1% / target
2%.  Entries use completed 5-minute bars and exact next-available 1-minute
fills; exits use exact 1-minute paths, statutory costs, V12 sizing, one
ticker/day, and a 15-trade cap.

## Safety

`PRODUCTION_APPROVED=False`; no live configuration, job, or process was
changed, enabled, or restarted.  Even a passing five-session test is only a
research candidate and needs a fresh forward holdout.
"""
    path.write_text(text, encoding="utf-8")


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--trials", type=int, default=DEFAULT_SEARCH_TRIALS)
    parser.add_argument("--output-dir", type=Path, default=OUTPUT_DIR)
    parser.add_argument("--reuse-exact", action="store_true")
    args = parser.parse_args(argv)
    out = args.output_dir
    out.mkdir(parents=True, exist_ok=True)
    splits = session_calendar()

    exact_path = out / "exact_candidate_universe.parquet"
    raw_path = out / "entry_engine_raw.parquet"
    rejects_path = out / "entry_engine_rejects.csv"
    funnel_path = out / "candidate_funnel.csv"
    exact_cache_manifest_path = out / "exact_cache_manifest.json"
    if args.reuse_exact and exact_path.exists() and raw_path.exists():
        exact = pd.read_parquet(exact_path)
        raw = pd.read_parquet(raw_path)
        validate_exact_cache_manifest(exact_cache_manifest_path, exact, raw)
        rejects = pd.read_csv(rejects_path) if rejects_path.exists() else pd.DataFrame()
        funnel = pd.read_csv(funnel_path) if funnel_path.exists() else pd.DataFrame()
        prewarm = {"reused_exact_cache": True}
    else:
        candidates, funnel = load_union_candidates()
        exact, raw, rejects, prewarm = build_exact_universe(candidates)
        exact.to_parquet(exact_path, index=False)
        raw.to_parquet(raw_path, index=False)
        rejects.to_csv(rejects_path, index=False)
        funnel.to_csv(funnel_path, index=False)
        write_exact_cache_manifest(exact_cache_manifest_path, exact, raw)
    if set(exact["trade_date"].astype(str)) - set(splits["all"]):
        raise RuntimeError("exact universe contains an out-of-window trade date")

    ledger, finalists, champion, selected, freeze = run_search(
        exact, splits, trials=int(args.trials)
    )
    # Persist the freeze before any locked-test metric is computed.
    freeze_path = out / "champion_freeze_before_test.json"
    freeze_path.write_text(json.dumps(json_safe(freeze), indent=2), encoding="utf-8")

    trades = selected_trade_frame(exact, selected)
    results = {
        "development": detailed_performance(trades, splits["development"]),
        "validation": detailed_performance(trades, splits["validation"]),
        "locked_test": detailed_performance(trades, splits["test"]),
        "full_month": detailed_performance(trades, splits["all"]),
    }
    cost_stress = [
        detailed_performance(trades, splits["all"], cost_multiplier=value)
        for value in (1.0, 1.25, 1.50)
    ]
    arrays = SearchArrays(exact, splits["all"])
    perturbations = []
    for label, candidate in perturbation_configs(champion):
        candidate_selected = arrays.selected_indices(candidate)
        candidate_trades = selected_trade_frame(exact, candidate_selected)
        metric = detailed_performance(candidate_trades, splits["all"])
        perturbations.append({"stress": label, "config_sha256": config_hash(candidate), **metric})

    daily = daily_results(trades, splits["all"])
    full_gate = (
        bool(freeze["development_gate_passed"])
        and bool(freeze["validation_gate_passed"])
        and results["full_month"]["trades"] >= 66
        and results["full_month"]["active_days"] >= 17
        and results["full_month"]["profit_factor"] >= 1.25
        and results["development"]["net_pnl_rs"] > 0
        and results["validation"]["net_pnl_rs"] > 0
        and results["locked_test"]["trades"] >= 15
        and results["locked_test"]["active_days"] >= 4
        and results["locked_test"]["profit_factor"] >= 1.20
        and results["locked_test"]["net_pnl_rs"] > 0
    )
    verdict = "RESEARCH_CANDIDATE_PASSED_ONE_MONTH_GATE" if full_gate else "REJECTED_ONE_MONTH_ROBUSTNESS_GATE"
    summary = {
        "research_only": True,
        "production_approved": False,
        "verdict": verdict,
        "window": {key: value for key, value in splits.items()},
        "search": {
            "generated_trials": int(args.trials),
            "seed": SEARCH_SEED,
            "fixed_sl_pct": STOP_LOSS_PCT,
            "fixed_target_pct": TARGET_PCT,
            "validation_finalists": len(finalists),
            "test_used_for_selection": False,
        },
        "champion": {
            **json_safe(asdict(champion)),
            "config_sha256": config_hash(champion),
            "development_gate_passed": freeze["development_gate_passed"],
            "validation_gate_passed": freeze["validation_gate_passed"],
        },
        "results": results,
        "cost_stress": cost_stress,
        "perturbation_pass_count": int(sum(row["net_pnl_rs"] > 0 and row["profit_factor"] >= 1.20 for row in perturbations)),
        "perturbation_count": len(perturbations),
        "execution": {
            "candidate_rows": len(exact),
            "one_minute_complete_grid_rows": int((~exact["path_fallback_applied"]).sum()),
            "five_minute_fallback_rows": int(exact["path_fallback_applied"].sum()),
            "source_max_window_incomplete_rows": int(
                exact["max_window_complete"].eq(False).sum()
            ),
            "prewarm": prewarm,
            "entry_rejects": len(rejects),
            "entry_timeframe": "exact_next_available_1min",
            "exit_timeframe": "exact_1min",
            "cost_model": "statutory",
            "one_ticker_per_day": True,
            "daily_cap": DAILY_CAP,
        },
        "selection_bias_note": (
            "The configuration was optimized inside July 6-August 4.  The final "
            "five sessions were held out within this run, but this is not a future "
            "market holdout and cannot establish production profitability."
        ),
    }

    ledger.to_parquet(out / "complete_trial_ledger.parquet", index=False)
    ledger.sort_values("development_robust_score", ascending=False, na_position="last").head(5000).to_csv(
        out / "top_5000_development_trials.csv", index=False
    )
    finalists.to_csv(out / "validation_finalists.csv", index=False)
    trades.to_csv(out / "champion_trades.csv", index=False)
    daily.to_csv(out / "champion_daily_results.csv", index=False)
    pd.DataFrame(cost_stress).to_csv(out / "cost_stress.csv", index=False)
    pd.DataFrame(perturbations).to_csv(out / "logic_perturbation_stress.csv", index=False)
    write_config(out / "one_month_long_setup_conf.py", champion)
    (out / "summary.json").write_text(json.dumps(json_safe(summary), indent=2), encoding="utf-8")
    write_report(out / "RESEARCH_REPORT.md", summary)

    artifact_names = [
        "candidate_funnel.csv", "entry_engine_raw.parquet", "entry_engine_rejects.csv",
        "exact_cache_manifest.json",
        "exact_candidate_universe.parquet", "champion_freeze_before_test.json",
        "complete_trial_ledger.parquet", "top_5000_development_trials.csv",
        "validation_finalists.csv", "champion_trades.csv", "champion_daily_results.csv",
        "cost_stress.csv", "logic_perturbation_stress.csv", "one_month_long_setup_conf.py",
        "summary.json", "RESEARCH_REPORT.md",
    ]
    manifest = {
        "artifacts": {
            name: {"sha256": sha256(out / name), "bytes": (out / name).stat().st_size}
            for name in artifact_names if (out / name).exists()
        },
        "sources": {
            str(Path(__file__).resolve()): sha256(Path(__file__).resolve()),
            str(v2.SOURCE): sha256(v2.SOURCE),
        },
    }
    (out / "integrity_manifest.json").write_text(
        json.dumps(json_safe(manifest), indent=2), encoding="utf-8"
    )
    print(json.dumps(json_safe(summary), indent=2), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
