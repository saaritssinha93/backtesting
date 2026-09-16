"""Reconstruct and replay ONLY retained V13-V10-G for one explicit session.

Historical bars warm up causal indicators; candidates, selections, execution
paths and statistics are restricted to the requested date. This module does
not load frozen research signals, optimize parameters, fetch broker data or
run any other strategy. Stocks missing a required futures OI bar are excluded
and disclosed; other incomplete required-day coverage blocks publication.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

import fno_oi_backtest_provenance as provenance
import fno_oi_common as common
import fno_oi_hybrid_data as hybrid
import fno_v13_v10_g_backtest as g
import fno_v13_v10_g_live_config as config
import fno_v13_v9_data as features

SCHEMA_VERSION = "fno_v13_v10_g_daily_replay_v1"
STRATEGY = "V13-V10-G"
EXCLUDABLE_COVERAGE_REASONS = {"MISSING_SIGNAL_OR_PRIOR_FUTURES_OI_BAR"}


@dataclass(frozen=True)
class DataRoots:
    universe: Path = field(default_factory=lambda: common.UNIVERSE_DIR)
    equity_1m: Path = field(default_factory=lambda: hybrid.DEFAULT_BACKTEST_EQUITY_1M_DIR)
    futures_5m: Path = field(default_factory=lambda: common.RAW_CONTRACT_DIR)


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _json_ready(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_ready(v) for v in value]
    if isinstance(value, (date, datetime, pd.Timestamp)):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value.resolve())
    if isinstance(value, np.generic):
        value = value.item()
    if value is pd.NA or value is pd.NaT or (isinstance(value, float) and not math.isfinite(value)):
        return None
    return value


def _sha(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _record_source(path: Path, role: str, sources: list[dict]) -> None:
    path = path.resolve()
    if any(row["path"] == str(path) for row in sources):
        return
    exists = path.is_file()
    sources.append(dict(path=str(path), role=role, exists=exists,
                        sha256=_sha(path) if exists else None,
                        size_bytes=path.stat().st_size if exists else None))


def _problem(problems: list, symbol: str, reason: str, **details) -> None:
    problems.append(dict(symbol=symbol, reason=reason, **_json_ready(details)))


def _cutoff(day: date) -> pd.Timestamp:
    return pd.Timestamp(config.slot_datetime(day, config.SQUARE_OFF))


def _slot_times(day: date) -> tuple[pd.DatetimeIndex, pd.DatetimeIndex, pd.DatetimeIndex]:
    signal = pd.DatetimeIndex([config.slot_datetime(day, clock) for clock in config.SIGNAL_TO_CONFIRMATION])
    minute = set([_cutoff(day)])
    oi = set()
    for stamp in signal:
        minute.update(pd.date_range(stamp - pd.Timedelta(minutes=4), stamp + pd.Timedelta(minutes=1), freq="min"))
        oi.update([stamp - pd.Timedelta(minutes=5), stamp])
    return signal, pd.DatetimeIndex(sorted(minute)), pd.DatetimeIndex(sorted(oi))


def _load_minute(path: Path, day: date, problems: list, symbol: str) -> pd.DataFrame:
    names = set(pq.ParquetFile(path).schema.names)
    required = ["date", "open", "high", "low", "close", "volume"]
    absent = set(required) - names
    if absent:
        raise ValueError(f"Missing equity fields: {sorted(absent)}")
    optional = [x for x in ("gap_filled", "opening_snapshot", "provisional_stale") if x in names]
    frame = pd.read_parquet(path, columns=required + optional)
    frame["ts"] = hybrid._to_ist(frame["date"]).dt.as_unit("ns")
    target = frame.loc[frame.ts.dt.date.eq(day)]
    if target.ts.duplicated().any():
        _problem(problems, symbol, "DUPLICATE_REQUESTED_DAY_EQUITY_MINUTES")
    if frame.ts.isna().any():
        _problem(problems, symbol, "INVALID_EQUITY_TIMESTAMP")
    numeric = ["open", "high", "low", "close", "volume"]
    frame[numeric] = frame[numeric].apply(pd.to_numeric, errors="coerce")
    # Future dates never participate even indirectly in feature construction.
    return (frame.loc[frame.ts.le(_cutoff(day))].dropna(subset=["ts"])
            .sort_values("ts", kind="stable").drop_duplicates("ts", keep="last").reset_index(drop=True))


def _load_future(path: Path, day: date, problems: list, symbol: str) -> pd.DataFrame:
    frame = pd.read_parquet(path)
    required = {"timestamp", "open", "high", "low", "close", "volume", "oi"}
    if required.difference(frame):
        raise ValueError(f"Missing futures fields: {sorted(required.difference(frame))}")
    frame["ts"] = hybrid._to_ist(frame["timestamp"]).dt.as_unit("ns")
    if frame.loc[frame.ts.dt.date.eq(day), "ts"].duplicated().any():
        _problem(problems, symbol, "DUPLICATE_REQUESTED_DAY_FUTURES_BARS")
    return (frame.loc[frame.ts.le(_cutoff(day))].sort_values("ts", kind="stable")
            .drop_duplicates("ts", keep="last").reset_index(drop=True))


def _coverage(minute: pd.DataFrame, future: pd.DataFrame, day: date,
              symbol: str, problems: list) -> dict:
    signal_times, _, required_oi = _slot_times(day)
    required_minutes = pd.date_range(config.slot_datetime(day, "09:16"), _cutoff(day), freq="min")
    missing_minute = required_minutes.difference(pd.DatetimeIndex(minute.ts))
    missing_oi = required_oi.difference(pd.DatetimeIndex(future.ts))
    if len(missing_minute):
        _problem(problems, symbol, "MISSING_REQUIRED_EQUITY_MINUTES", count=len(missing_minute),
                 timestamps=[x.isoformat() for x in missing_minute])
    if len(missing_oi):
        _problem(problems, symbol, "MISSING_SIGNAL_OR_PRIOR_FUTURES_OI_BAR", count=len(missing_oi),
                 timestamps=[x.isoformat() for x in missing_oi])
    # Data completeness is deliberately stricter than the retained mathematical
    # rolling(20, min_periods=5) definition: never let a skipped download change
    # the volume denominator. Keep the native full history and fail explicitly
    # if that history supplies non-session or interrupted baseline bars.
    clock = minute.ts.dt.hour * 60 + minute.ts.dt.minute
    if (~clock.between(9 * 60 + 16, 15 * 60 + 30)).any():
        _problem(problems, symbol, "NON_REGULAR_EQUITY_HISTORY_MINUTES")
    warmup = minute.loc[minute.ts.lt(signal_times[0] + pd.Timedelta(minutes=1))].tail(20)
    if len(warmup) < 20:
        _problem(problems, symbol, "INSUFFICIENT_CONFIRMATION_VOLUME_WARMUP", observed=len(warmup), required=20)
    if len(warmup):
        same_session = warmup.ts.dt.date.eq(warmup.ts.shift().dt.date)
        gaps = same_session & warmup.ts.diff().ne(pd.Timedelta(minutes=1))
        if gaps.any():
            _problem(problems, symbol, "INTERRUPTED_CONFIRMATION_VOLUME_WARMUP", count=int(gaps.sum()))
        warmup_volume = pd.to_numeric(warmup.volume, errors="coerce").to_numpy(float)
        if (~np.isfinite(warmup_volume) | (warmup_volume < 0)).any() or not np.any(warmup_volume > 0):
            _problem(problems, symbol, "INVALID_CONFIRMATION_VOLUME_WARMUP")
    target = minute.loc[minute.ts.dt.date.eq(day)]
    values = target[["open", "high", "low", "close", "volume"]].to_numpy(float)
    if len(target):
        invalid = (~np.isfinite(values).all(axis=1) | (values[:, :4] <= 0).any(axis=1)
                   | (values[:, 4] < 0) | (values[:, 1] < np.maximum(values[:, 0], values[:, 3]))
                   | (values[:, 2] > np.minimum(values[:, 0], values[:, 3])))
        if invalid.any():
            _problem(problems, symbol, "INVALID_REQUESTED_DAY_EQUITY_OHLCV", count=int(invalid.sum()))
        for name in ("gap_filled", "opening_snapshot", "provisional_stale"):
            if name in target:
                flagged = pd.to_numeric(target[name], errors="coerce").fillna(0).ne(0) | target[name].astype(str).str.lower().isin(["true", "yes", "on"])
                if flagged.any():
                    _problem(problems, symbol, "FLAGGED_REQUESTED_DAY_EQUITY_SOURCE", field=name, count=int(flagged.sum()))
    needed = future.loc[future.ts.isin(required_oi)]
    oi_values = pd.to_numeric(needed.oi, errors="coerce").to_numpy(float)
    if (~np.isfinite(oi_values) | (oi_values <= 0)).any():
        _problem(problems, symbol, "INVALID_REQUIRED_FUTURES_OI")
    return dict(symbol=symbol, required_equity_minutes=len(required_minutes),
                missing_equity_minutes=len(missing_minute), required_futures_bars=len(required_oi),
                missing_futures_bars=len(missing_oi), observed_session_minutes=len(target),
                observed_warmup_minutes=int(minute.ts.lt(signal_times[0]).sum()),
                confirmation_prior_minutes=len(warmup))


def _strict_signals(pool: pd.DataFrame, nifty_return: float) -> pd.DataFrame:
    """Native V13 strict raw signal conditions, prior to G setup ranking."""
    out = pool.copy()
    bull, bear = out.v9_5m_ema_bull.fillna(False), out.v9_5m_ema_bear.fillna(False)
    common_gate = (out.oi.gt(out.prev_oi) & out.oi_change_pct.between(.05, 1.) & out.volume_ratio.ge(.80))
    longs = bull & out.price_change_pct.ge(.10)
    shorts = bear & out.price_change_pct.le(-.10)
    out["side"] = np.where(longs, "LONG", "SHORT")
    sign = np.where(longs, 1., -1.)
    strict = (common_gate & (longs | shorts) & out.v9_exact_confirmation_present
              & out.confirmation_high.gt(out.confirmation_low)
              & ((out.confirmation_close - out.confirmation_open) * sign).gt(0)
              & ((out.confirmation_close - out.signal_close) * sign).gt(0))
    out["nifty_first_bar_return_pct"] = nifty_return
    gated = out.hhmm_int.eq(925) & out.side.eq("SHORT")
    strict &= ~gated | (np.isfinite(nifty_return) and nifty_return <= -.05)
    out["wick_ratio"] = np.where(longs, out.v9_1m_upper_wick_ratio, out.v9_1m_lower_wick_ratio)
    out["trigger"] = np.where(longs, out.confirmation_high, out.confirmation_low)
    return out.loc[strict].copy()


def _observed_pool(minute: pd.DataFrame, future: pd.DataFrame, *, day: date,
                   symbol: str, future_symbol: str, month: str) -> pd.DataFrame:
    """Only retained G's consumed features, using native arithmetic/history.

    The general research builder also computes VWAP, RSI-like contexts and
    several 1m EMAs across the whole history. G does not consume those fields;
    excluding that unused work keeps a daily dashboard replay responsive.
    """
    five = hybrid.aggregate_equity_one_minute_to_five_minute(minute)
    if five.empty:
        return pd.DataFrame()
    five = hybrid.join_equity_price_with_futures_oi(five, future)
    signal_times, _, _ = _slot_times(day)
    five = five.loc[five.ts.isin(signal_times)].copy()
    if five.empty:
        return five
    five["signal_ts"] = five.ts
    five["confirmation_ts"] = five.ts + pd.Timedelta(minutes=1)
    five["day"] = day
    five["hhmm"] = five.ts.dt.strftime("%H%M")
    five["hhmm_int"] = five.hhmm.astype(int)
    five["signal_close"] = five.close
    five["tradingsymbol"] = symbol
    five["futures_tradingsymbol"] = future_symbol
    five["contract_month"] = month
    five["price_source"] = hybrid.BACKTEST_EQUITY_5M_CONSTRUCTION
    five["oi_source"] = "NFO_FUTURE"
    five["data_contract"] = hybrid.DATA_CONTRACT_VERSION
    for span in (9, 20, 50):
        five[f"v9_5m_ema{span}"] = five[f"ema{span}"]
    five["v9_5m_ema_bull"] = (five.ema9.gt(five.ema20) & five.ema20.gt(five.ema50)).astype("boolean")
    five["v9_5m_ema_bear"] = (five.ema9.lt(five.ema20) & five.ema20.lt(five.ema50)).astype("boolean")
    five["v9_5m_feature_ts"] = five.signal_ts
    volume = pd.to_numeric(minute.volume, errors="coerce")
    denominator = volume.shift(1).rolling(20, min_periods=5).mean()
    confirmation = minute[["ts", "open", "high", "low", "close", "volume"]].copy()
    confirmation["v9_1m_volume_ratio"] = volume.div(denominator.where(denominator.gt(0)))
    confirmation = confirmation.loc[confirmation.ts.isin(five.confirmation_ts)].copy()
    confirmation["v9_1m_feature_ts"] = confirmation.ts
    o, h, low, c = [confirmation[name].astype(float) for name in ("open", "high", "low", "close")]
    bar_range = (h - low).where(h.gt(low))
    confirmation["body_ratio"] = (c - o).abs().div(bar_range)
    confirmation["v9_1m_upper_wick_ratio"] = (h - pd.concat([o, c], axis=1).max(axis=1)).div(bar_range)
    confirmation["v9_1m_lower_wick_ratio"] = (pd.concat([o, c], axis=1).min(axis=1) - low).div(bar_range)
    confirmation = confirmation.rename(columns={"ts": "confirmation_ts", **{name: f"confirmation_{name}"
                                                   for name in ("open", "high", "low", "close", "volume")}})
    five = five.merge(confirmation, on="confirmation_ts", how="left", validate="one_to_one")
    five["v9_exact_confirmation_present"] = five.v9_1m_feature_ts.notna()
    five["v9_feature_available_ts"] = five.v9_1m_feature_ts
    features.assert_feature_chronology(five)
    return five.drop(columns=["date", "ts"], errors="ignore")


def _empty_signals() -> pd.DataFrame:
    columns = ["sid", "day", "signal_ts", "confirmation_ts", "hhmm", "hhmm_int", "side",
               "tradingsymbol", "futures_tradingsymbol", "price_change_pct", "oi_change_pct",
               "volume_ratio", "body_ratio", "wick_ratio", "traded_value", "trigger",
               "v9_1m_volume_ratio", "v9_1m_feature_ts"]
    return pd.DataFrame(columns=columns)


def _assert_day(frame: pd.DataFrame, day: date) -> None:
    if "day" not in frame or frame.empty:
        return
    observed = pd.to_datetime(frame.day, errors="coerce").dt.date
    if observed.isna().any() or not observed.eq(day).all():
        raise ValueError("Replay frame contains dates outside the requested session")


def build_day_dataset(day: date, *, roots: DataRoots | None = None) -> dict:
    if type(day) is not date:
        raise TypeError("An explicit datetime.date session is required")
    roots = roots or DataRoots()
    config.validate_strategy()
    sources, problems, coverage, pools, minutes = [], [], [], [], {}
    universe_path = roots.universe / f"near_month_{day.isoformat()}.parquet"
    _record_source(universe_path, "DATED_UNIVERSE", sources)
    for path in (Path(__file__), Path(config.__file__), Path(g.__file__), Path(features.__file__),
                 Path(hybrid.__file__), Path(g.v9.__file__), Path(g.v9.v5.__file__), Path(g.v9.v6.__file__),
                 Path(provenance.__file__), Path(common.__file__), config.CONFIG_PATH):
        _record_source(path, "CODE_OR_PINNED_CONFIGURATION", sources)
    result = dict(day=day, days=[day], sources=sources, problems=problems, coverage_rows=coverage,
                  excluded_stocks=[],
                  signals=_empty_signals(), orders=pd.DataFrame(columns=["day", "setup_id", "sid"]),
                  selection_audit=pd.DataFrame(columns=["day", "setup_id", "sid", "v9_selected"]),
                  paths={}, universe_count=0, mapped_universe=pd.DataFrame())
    try:
        full = pd.read_parquet(universe_path)
        stocks = full.loc[~full.is_index_future.fillna(False).astype(bool)]
        expiry = pd.to_datetime(stocks.expiry, errors="coerce").dropna().unique()
        if len(expiry) != 1:
            raise ValueError("Dated stock universe must have exactly one expiry")
        month = pd.Timestamp(expiry[0]).strftime("%y%b").upper()
        mapped, proof = provenance.load_backtest_universe(universe_path=universe_path, universe_date=day,
                         contract_month_contains=month, require_persisted_mapping=True)
        if mapped.equity_symbol.duplicated().any() or mapped.futures_tradingsymbol.duplicated().any():
            raise ValueError("Dated stock/equity mapping contains duplicates")
        result.update(universe_count=len(mapped), mapped_universe=mapped, universe_proof=proof)
        nifty = full.loc[full.underlying.astype(str).str.upper().eq("NIFTY")]
        if len(nifty) != 1 or pd.Timestamp(nifty.iloc[0].expiry) != pd.Timestamp(expiry[0]):
            raise ValueError("Dated near-month NIFTY futures mapping is missing or ambiguous")
        nifty_symbol = str(nifty.iloc[0].tradingsymbol)
        nifty_path = roots.futures_5m / f"{common.safe_contract_stem(nifty_symbol)}_5minute.parquet"
        _record_source(nifty_path, "NIFTY_FUTURES_CONTEXT", sources)
        nifty_frame = _load_future(nifty_path, day, problems, nifty_symbol)
        nifty_return = config.nifty_context_from_bars(nifty_frame, day)
        if not np.isfinite(nifty_return):
            _problem(problems, nifty_symbol, "MISSING_OR_INVALID_EXACT_0920_NIFTY_CONTEXT")
    except (OSError, ValueError, KeyError, TypeError) as exc:
        _problem(problems, "UNIVERSE_OR_NIFTY", "REQUIRED_SOURCE_UNAVAILABLE", detail=f"{type(exc).__name__}: {exc}")
        return result
    for number, contract in enumerate(mapped.to_dict("records"), 1):
        symbol = hybrid.resolve_backtest_equity_symbol(str(contract["equity_symbol"]), root=roots.equity_1m)
        future_symbol = str(contract["futures_tradingsymbol"])
        minute_path = hybrid.equity_one_minute_path(symbol, roots.equity_1m)
        future_path = roots.futures_5m / f"{common.safe_contract_stem(future_symbol)}_5minute.parquet"
        _record_source(minute_path, "EQUITY_ONE_MINUTE", sources)
        _record_source(future_path, "STOCK_FUTURES_OI", sources)
        try:
            minute = _load_minute(minute_path, day, problems, symbol)
            future = _load_future(future_path, day, problems, symbol)
            problem_start = len(problems)
            coverage.append(_coverage(minute, future, day, symbol, problems))
            new_reasons = {row["reason"] for row in problems[problem_start:]}
            if new_reasons & EXCLUDABLE_COVERAGE_REASONS:
                result["excluded_stocks"].append(dict(
                    symbol=symbol,
                    reasons=sorted(new_reasons & EXCLUDABLE_COVERAGE_REASONS),
                ))
                continue
            # Cache only target-day execution prices, not the warmup history.
            minutes[symbol] = minute.loc[minute.ts.dt.date.eq(day)].copy()
            pool = _observed_pool(minute, future, day=day, symbol=symbol,
                                  future_symbol=future_symbol, month=month)
            if pool.empty:
                _problem(problems, symbol, "NO_REQUESTED_DAY_COMPLETE_EQUITY_FIVE_MINUTE_BARS")
                continue
            pool = pool.loc[pool.hhmm_int.isin([int(x.replace(":", "")) for x in config.SIGNAL_TO_CONFIRMATION])].copy()
            if len(pool) != len(config.SIGNAL_TO_CONFIRMATION):
                _problem(problems, symbol, "INCOMPLETE_REQUIRED_FIVE_MINUTE_BAR_CONSTRUCTION", observed=len(pool))
            pool["instrument_token"] = int(contract["equity_instrument_token"])
            pool["futures_instrument_token"] = int(contract["futures_instrument_token"])
            pool["exchange"] = "NSE"
            pools.append(_strict_signals(pool, nifty_return))
        except (OSError, ValueError, KeyError, TypeError, AssertionError) as exc:
            _problem(problems, symbol, "STOCK_SOURCE_BUILD_FAILED", detail=f"{type(exc).__name__}: {exc}")
        if number % 25 == 0 or number == len(mapped):
            print(f"[V13-V10-G daily] {day}: reconstructed {number}/{len(mapped)} stocks", flush=True)
    if pools:
        signals = pd.concat(pools, ignore_index=True).sort_values(["tradingsymbol", "signal_ts", "side"], kind="stable").reset_index(drop=True)
        signals["sid"] = np.arange(len(signals), dtype=int)
        _assert_day(signals, day)
        result["signals"] = signals
    base = g.v9.V9Config(portfolio_capital_rupees=config.PORTFOLIO_CAPITAL_RS,
                        capital_per_entry_rupees=config.CAPITAL_PER_ENTRY_RS, leverage_factor=config.LEVERAGE,
                        max_positions=None, cost_bps=config.ROUND_TRIP_COST_BPS)
    settings = config.load_frozen_config()
    audit = g.selection_audit(result["signals"], base, g.SelectionChange(**settings["selection_change"]),
                             core_first=True, morning_slots=False, two_bar_continuation=False)
    orders = audit.loc[audit.v9_selected.eq(True)].copy()
    for row in orders.drop_duplicates("sid").itertuples(index=False):
        minute = minutes.get(row.tradingsymbol, pd.DataFrame())
        path_frame = minute.loc[minute.ts.gt(row.confirmation_ts) & minute.ts.le(_cutoff(day))]
        path = {"timestamp_ns": path_frame.ts.astype("int64").to_numpy(),
                **{key: path_frame[key].to_numpy(float) for key in ("open", "high", "low", "close")}}
        result["paths"][int(row.sid)] = path
        try:
            g.v9.validate_paths(orders.loc[orders.sid.eq(row.sid)], {int(row.sid): path})
        except RuntimeError as exc:
            _problem(problems, row.tradingsymbol, "INCOMPLETE_SELECTED_EXECUTION_PATH", detail=str(exc))
    _assert_day(audit, day)
    _assert_day(orders, day)
    result.update(orders=orders, selection_audit=audit, v9_config=base, settings=settings)
    # Atomic producer replacements during reconstruction invalidate the run.
    for source in sources:
        path = Path(source["path"])
        if path.is_file() != source["exists"] or (source["exists"] and _sha(path) != source["sha256"]):
            _problem(problems, path.name, "SOURCE_CHANGED_DURING_REPLAY", path=str(path))
    return result


def simulate_day(dataset: dict) -> tuple[pd.DataFrame, dict]:
    """Fixed G exits and capital, reusable for explicitly partial diagnostic parity."""
    day, base = dataset["day"], dataset["v9_config"]
    orders = dataset["orders"].copy()
    _assert_day(orders, day)
    exits = dataset["settings"]["exit"]["setups"]
    orders["native_stop_pct"] = orders.setup_id.map({key: value["stop_pct"] for key, value in exits.items()})
    orders["native_target_pct"] = orders.setup_id.map({key: value["target_pct"] for key, value in exits.items()})
    g.v9.validate_paths(orders, dataset["paths"])
    trades = g.v9.v5.simulate_native(orders, dataset["paths"], cost_bps=base.cost_bps, max_entry_delay_minutes=10)
    for column, default in (("filled", False), ("entry_ts", pd.NaT), ("exit_ts", pd.NaT),
                            ("net_return_pct", np.nan), ("gross_return_pct", np.nan), ("cost_pct", np.nan)):
        if column not in trades:
            trades[column] = default
    trades = g.v9.v5.apply_fixed_capital_model(trades, base.capital_per_entry_rupees, base.leverage_factor)
    ledger, _ = g.v9.v6.apply_portfolio_constraints(trades, base.portfolio_config())
    _assert_day(ledger, day)
    for field_name in ("entry_ts", "exit_ts"):
        stamps = pd.to_datetime(ledger[field_name], utc=True, errors="coerce").dt.tz_convert(common.IST)
        if not stamps.dropna().dt.date.eq(day).all():
            raise ValueError("Execution timestamp escaped the requested session")
    metric = g.r.metric(ledger, [day])
    metric.update(sessions=1, orders=metric["selected_orders"], fills=metric["trades"])
    return ledger, metric


def replay_day(day: date, output_dir: Path, *, roots: DataRoots | None = None) -> dict:
    output = Path(output_dir).resolve()
    output.mkdir(parents=True, exist_ok=True)
    dataset = build_day_dataset(day, roots=roots)
    sources = dataset["sources"]
    source_fingerprint = common.canonical_json_sha256(sources)
    artifacts = {}
    for name, frame in (("candidate_signals", dataset["signals"]),
                        ("selection_audit", dataset.get("selection_audit", pd.DataFrame())),
                        ("selected_orders", dataset.get("orders", pd.DataFrame())),
                        ("coverage", pd.DataFrame(dataset["coverage_rows"]) if dataset["coverage_rows"] else
                         pd.DataFrame(columns=["symbol", "missing_equity_minutes", "missing_futures_bars"]))):
        _assert_day(frame, day)
        path = output / f"{name}.csv"
        common.atomic_write_csv(frame, path)
        artifacts[name] = str(path)
    ignored_problems = [row for row in dataset["problems"]
                        if row.get("reason") in EXCLUDABLE_COVERAGE_REASONS]
    blocking_problems = [row for row in dataset["problems"]
                         if row.get("reason") not in EXCLUDABLE_COVERAGE_REASONS]
    included_stocks = dataset["universe_count"] - len(dataset.get("excluded_stocks", []))
    complete = not blocking_problems and included_stocks > 0
    metrics = None
    if complete:
        ledger, metrics = simulate_day(dataset)
        ledger["strategy"] = STRATEGY
        path = output / "portfolio_trades.csv"
        common.atomic_write_csv(ledger, path)
        artifacts["portfolio_trades"] = str(path)
        daily = output / "daily_results.csv"
        common.atomic_write_csv(pd.DataFrame([dict(day=day.isoformat(), strategy=STRATEGY, **metrics)]), daily)
        artifacts["daily_results"] = str(daily)
    manifest_path = output / "source_manifest.json"
    manifest = dict(schema_version=SCHEMA_VERSION, session_date=day.isoformat(), days=[day.isoformat()],
                    strategy=STRATEGY, frozen_config_sha256=config.CONFIG_SHA256,
                    source_fingerprint=source_fingerprint, sources=sources,
                    universe=dataset.get("universe_proof", {}),
                    chronology="Warmup history ends no later than requested cutoff; all candidate/order/exit rows use only requested session.",
                    coverage_policy={"equity_session": "Every completed minute end 09:16 through 15:15",
                                     "confirmation_warmup": "Prior 20 observed regular-session minutes required; no within-session gaps",
                                     "volume_calculation": "Unchanged native rolling20/min5; completeness guard requires20",
                                     "indicators": "Native full causal history; no truncated EMA approximation"},
                    selection="Pinned retained G only; no optimization or alternative strategies",
                    complete=complete, coverage_problems=blocking_problems,
                    ignored_coverage_problems=ignored_problems,
                    excluded_stocks=dataset.get("excluded_stocks", []))
    common.atomic_write_json(manifest_path, _json_ready(manifest))
    artifacts["source_manifest"] = str(manifest_path)
    result = dict(schema_version=SCHEMA_VERSION, strategy=STRATEGY, strategy_version=config.STRATEGY_VERSION,
                  session_date=day.isoformat(), days=[day.isoformat()], complete=complete,
                  state="SUCCESS" if complete else "BLOCKED_INCOMPLETE_DATA", metrics=metrics,
                  artifacts=artifacts, source_fingerprint=source_fingerprint,
                  coverage=dict(universe_stocks=dataset["universe_count"], included_stocks=included_stocks,
                                checked_stocks=len(dataset["coverage_rows"]), problems=blocking_problems,
                                ignored_problems=ignored_problems,
                                excluded_stocks=dataset.get("excluded_stocks", [])),
                  partial_diagnostics=dict(observed_candidates=len(dataset["signals"]),
                                           selected_from_available_data=len(dataset.get("orders", [])),
                                           publishable=complete))
    result = _json_ready(result)
    common.atomic_write_json(output / "replay_result.json", result)
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--session-date", required=True, type=date.fromisoformat)
    parser.add_argument("--output-dir", required=True, type=Path)
    args = parser.parse_args()
    result = replay_day(args.session_date, args.output_dir)
    print(json.dumps(result, indent=2))
    return 0 if result["complete"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
