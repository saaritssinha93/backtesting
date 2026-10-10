"""Exact execution and portfolio functions extracted from the sealed G-3 source.

The source snapshot remains byte-for-byte intact in source/. Only inert helper
functions are loaded here; no original imports, CLI, live policy or data paths
are executed. The wiring below supplies their original dependency names.
"""
from __future__ import annotations
from dataclasses import asdict, dataclass
from types import SimpleNamespace
from typing import Any
import numpy as np
import pandas as pd
common = SimpleNamespace(IST="Asia/Kolkata")
DEFAULT_LEVERAGE_FACTOR = 5.0
INITIAL_STOP_PCT = 1.25
TIGHTENED_STOP_PCT = 1.0
TIGHTEN_AFTER_MINUTES = 120
MINUTE_NS = 60_000_000_000
SCHEMA_VERSION = "FNO_V13_V6_PORTFOLIO_V1"


# Source: fno_v13_corrected_v5_backtest.py:687
def _to_ist_timestamp(value: Any) -> pd.Timestamp:
    stamp = pd.Timestamp(value)
    if stamp.tzinfo is None:
        return stamp.tz_localize(common.IST)
    return stamp.tz_convert(common.IST)


# Source: fno_v13_corrected_v5_backtest.py:759
def _entry(
    row: Any,
    path: dict[str, np.ndarray],
    *,
    delay_bars: int,
    trigger_buffer_pct: float,
    worse_fill_bps: float,
    max_entry_delay_minutes: int | None = None,
) -> tuple[int, float, float, bool, float] | None:
    is_long = row.side == "LONG"
    raw_trigger = float(row.trigger)
    trigger = raw_trigger * (
        1.0 + trigger_buffer_pct / 100.0
        if is_long
        else 1.0 - trigger_buffer_pct / 100.0
    )
    high = path["high"]
    low = path["low"]
    # path[0] is the bar immediately after the confirmation candle's close
    # (see materialize_raw_paths), so a hit at offset k is k+1 minutes after
    # confirmation. A hard window_end therefore caps entry delay in minutes.
    window_end = (
        delay_bars + max_entry_delay_minutes
        if max_entry_delay_minutes is not None
        else None
    )
    hits = np.flatnonzero(high[delay_bars:window_end] >= trigger) if is_long else np.flatnonzero(low[delay_bars:window_end] <= trigger)
    if not hits.size:
        return None
    entry_index = int(hits[0]) + delay_bars
    bar_open = float(path["open"][entry_index])
    gap_through = bool(bar_open > trigger if is_long else bar_open < trigger)
    entry = bar_open if gap_through else trigger
    entry *= 1.0 + worse_fill_bps / 10_000.0 if is_long else 1.0 - worse_fill_bps / 10_000.0
    overshoot_bps = (
        (entry / raw_trigger - 1.0) * 10_000.0
        if is_long
        else (raw_trigger / entry - 1.0) * 10_000.0
    )
    return entry_index, entry, trigger, gap_through, overshoot_bps


# Source: fno_v13_corrected_v5_backtest.py:801
def _excursions(
    path: dict[str, np.ndarray], entry: float, start: int, end: int, is_long: bool
) -> tuple[float, float]:
    high = path["high"][start : end + 1]
    low = path["low"][start : end + 1]
    if is_long:
        return float((high.max() / entry - 1.0) * 100.0), float((low.min() / entry - 1.0) * 100.0)
    return float((1.0 - low.min() / entry) * 100.0), float((1.0 - high.max() / entry) * 100.0)


# Source: fno_v13_corrected_v5_backtest.py:811
def _adverse_stop_fill(
    path: dict[str, np.ndarray],
    exit_index: int,
    stop_level: float,
    is_long: bool,
    *,
    activation_index: int,
) -> tuple[float, bool, float]:
    """Fill a stop at a worse later-bar open when price gaps through it.

    The activation bar's open precedes the intrabar trigger, so it cannot be
    used as a post-entry/post-T1 stop fill. That ambiguous bar stays at the
    stop level under the explicit pessimistic stop-first convention.
    """

    if exit_index <= activation_index:
        return stop_level, False, 0.0
    bar_open = float(path["open"][exit_index])
    adverse = bar_open < stop_level if is_long else bar_open > stop_level
    if not adverse:
        return stop_level, False, 0.0
    adverse_bps = abs(bar_open / stop_level - 1.0) * 10_000.0
    return bar_open, True, adverse_bps


# Source: fno_v13_corrected_v5_backtest.py:836
def simulate_native(
    orders: pd.DataFrame,
    paths: dict[int, dict[str, np.ndarray]],
    *,
    cost_bps: float,
    delay_bars: int = 0,
    trigger_buffer_pct: float = 0.0,
    worse_fill_bps: float = 0.0,
    max_entry_delay_minutes: int | None = None,
) -> pd.DataFrame:
    # Default stays unlimited: this path exists only to reproduce V13-v3's
    # published figures exactly, and must not silently change under it.
    result = orders.copy()
    records: list[dict[str, Any]] = []
    missing = np.iinfo(np.int32).max
    for row in result.itertuples(index=False):
        path = paths[int(row.sid)]
        found = _entry(
            row,
            path,
            delay_bars=delay_bars,
            trigger_buffer_pct=trigger_buffer_pct,
            worse_fill_bps=worse_fill_bps,
            max_entry_delay_minutes=max_entry_delay_minutes,
        )
        if found is None:
            records.append({"filled": False, "exit_reason": "UNFILLED"})
            continue
        entry_index, entry, trigger, gap, overshoot = found
        is_long = row.side == "LONG"
        stop_pct = float(row.native_stop_pct)
        target_pct = float(row.native_target_pct)
        stop = entry * (1.0 - stop_pct / 100.0) if is_long else entry * (1.0 + stop_pct / 100.0)
        target = entry * (1.0 + target_pct / 100.0) if is_long else entry * (1.0 - target_pct / 100.0)
        stop_hits = np.flatnonzero(path["low"][entry_index:] <= stop) if is_long else np.flatnonzero(path["high"][entry_index:] >= stop)
        target_hits = np.flatnonzero(path["high"][entry_index:] >= target) if is_long else np.flatnonzero(path["low"][entry_index:] <= target)
        stop_i = int(stop_hits[0]) if stop_hits.size else missing
        target_i = int(target_hits[0]) if target_hits.size else missing
        ambiguous = stop_i == target_i and stop_i < missing
        exit_gap_through = False
        exit_gap_bps = 0.0
        if stop_i == target_i == missing:
            exit_index = len(path["close"]) - 1
            exit_price = float(path["close"][-1])
            reason = "TIME_EXIT_1515"
        elif stop_i <= target_i:
            exit_index = entry_index + stop_i
            exit_price, exit_gap_through, exit_gap_bps = _adverse_stop_fill(
                path,
                exit_index,
                stop,
                is_long,
                activation_index=entry_index,
            )
            reason = "STOP"
        else:
            exit_index = entry_index + target_i
            exit_price = target
            reason = "TARGET"
        gross = (exit_price / entry - 1.0) * 100.0 if is_long else (1.0 - exit_price / entry) * 100.0
        mfe, mae = _excursions(path, entry, entry_index, exit_index, is_long)
        entry_ts = pd.Timestamp(int(path["timestamp_ns"][entry_index]), tz="UTC").tz_convert(common.IST)
        exit_ts = pd.Timestamp(int(path["timestamp_ns"][exit_index]), tz="UTC").tz_convert(common.IST)
        records.append(
            {
                "filled": True,
                "trigger_used": trigger,
                "entry_price": entry,
                "entry_ts": entry_ts,
                "entry_path_index": entry_index,
                "entry_gap_through": gap,
                "entry_overshoot_bps": overshoot,
                "exit_gap_through": exit_gap_through,
                "exit_gap_bps": exit_gap_bps,
                "exit_price": exit_price,
                "exit_ts": exit_ts,
                "exit_path_index": exit_index,
                "holding_minutes": float((exit_ts - entry_ts).total_seconds() / 60.0),
                "gross_return_pct": gross,
                "cost_pct": cost_bps / 100.0,
                "net_return_pct": gross - cost_bps / 100.0,
                "exit_reason": reason,
                "target_hit": reason == "TARGET",
                "first_target_hit": reason == "TARGET",
                "runner_target_hit": reason == "TARGET",
                "stop_hit": reason == "STOP",
                "same_bar_ambiguous": ambiguous,
                "mfe_pct": mfe,
                "mae_pct": mae,
                "initial_stop_pct": stop_pct,
                "first_target_pct": target_pct,
                "partial_pct": 1.0,
                "runner_target_pct": target_pct,
                "runner_stop": "NONE",
            }
        )
    details = pd.DataFrame(records)
    for column in details.columns:
        result[column] = details[column].to_numpy()
    return result


# Source: fno_v13_corrected_v5_backtest.py:198
def apply_fixed_capital_model(
    audit: pd.DataFrame,
    capital_per_entry_rupees: float,
    leverage_factor: float = DEFAULT_LEVERAGE_FACTOR,
) -> pd.DataFrame:
    if not np.isfinite(capital_per_entry_rupees) or capital_per_entry_rupees <= 0:
        raise ValueError("capital_per_entry_rupees must be a positive finite value.")
    if not np.isfinite(leverage_factor) or leverage_factor <= 0:
        raise ValueError("leverage_factor must be a positive finite value.")
    out = audit.copy()
    filled = out.get("filled", pd.Series(False, index=out.index)).astype(bool)
    net_return = pd.to_numeric(
        out.get("net_return_pct", pd.Series(np.nan, index=out.index)),
        errors="coerce",
    )
    gross_return = pd.to_numeric(
        out.get("gross_return_pct", pd.Series(np.nan, index=out.index)),
        errors="coerce",
    )
    cost_pct = pd.to_numeric(
        out.get("cost_pct", pd.Series(np.nan, index=out.index)),
        errors="coerce",
    )
    exposure_per_entry_rupees = capital_per_entry_rupees * leverage_factor
    out["capital_model"] = "FIXED_CAPITAL_PER_FILLED_TRADE_WITH_LEVERAGE_NON_COMPOUNDED"
    out["capital_per_entry_rupees"] = np.where(filled, capital_per_entry_rupees, 0.0)
    out["leverage_factor"] = leverage_factor
    out["exposure_per_entry_rupees"] = np.where(filled, exposure_per_entry_rupees, 0.0)
    out["gross_return_on_capital_pct"] = np.where(
        filled, gross_return * leverage_factor, np.nan
    )
    out["net_return_on_capital_pct"] = np.where(
        filled, net_return * leverage_factor, np.nan
    )
    out["unleveraged_pre_cost_profit_rupees"] = np.where(
        filled, gross_return / 100.0 * capital_per_entry_rupees, 0.0
    )
    out["unleveraged_cost_rupees"] = np.where(
        filled, cost_pct / 100.0 * capital_per_entry_rupees, 0.0
    )
    out["unleveraged_net_profit_rupees"] = np.where(
        filled, net_return / 100.0 * capital_per_entry_rupees, 0.0
    )
    out["pre_cost_profit_rupees"] = np.where(
        filled, gross_return / 100.0 * exposure_per_entry_rupees, 0.0
    )
    out["cost_rupees"] = np.where(
        filled, cost_pct / 100.0 * exposure_per_entry_rupees, 0.0
    )
    out["net_profit_rupees"] = np.where(
        filled, net_return / 100.0 * exposure_per_entry_rupees, 0.0
    )
    return out


v5 = SimpleNamespace(_to_ist_timestamp=_to_ist_timestamp, simulate_native=simulate_native, _excursions=_excursions)
g = SimpleNamespace(v9=SimpleNamespace(v5=v5))


# Source: fno_v13_v9_backtest.py:324
def validate_paths(orders: pd.DataFrame, paths: dict[int, dict[str, np.ndarray]]) -> None:
    """Fail closed on missing minutes; missing prices never remove losing trades."""
    for row in orders.drop_duplicates("sid").itertuples(index=False):
        sid = int(row.sid)
        if sid not in paths:
            raise RuntimeError(f"Missing raw execution path for selected sid={sid}")
        path = paths[sid]
        required = {"timestamp_ns", "open", "high", "low", "close"}
        if required.difference(path):
            raise RuntimeError(f"Incomplete path fields for sid={sid}")
        stamp = np.asarray(path["timestamp_ns"], dtype=np.int64)
        confirmation = v5._to_ist_timestamp(row.confirmation_ts)
        cutoff = confirmation.normalize() + pd.Timedelta(hours=15, minutes=15)
        expected = pd.date_range(confirmation + pd.Timedelta(minutes=1), cutoff, freq="min").asi8
        if not len(stamp) or not np.array_equal(stamp, expected):
            raise RuntimeError(f"Non-continuous or mistimed path for sid={sid}; expected confirmation+1 through exact 15:15")
        prices = [np.asarray(path[field], dtype=float) for field in ("open", "high", "low", "close")]
        if any(len(values) != len(stamp) for values in prices):
            raise RuntimeError(f"Mismatched path array lengths for sid={sid}")
        if any(not np.isfinite(values).all() or (values <= 0).any() for values in prices):
            raise RuntimeError(f"Invalid OHLC price in path for sid={sid}")
        opening, high, low, close = prices
        if ((high < np.maximum(opening, close)) | (low > np.minimum(opening, close)) | (high < low)).any():
            raise RuntimeError(f"Inconsistent OHLC bars for sid={sid}")


# Source: fno_v13_v10_g_2_backtest.py:307
def staged_exit(path, entry_index: int, entry: float, is_long: bool, target_pct: float):
    """Causal minute replay. Times label candle ends, not candle opens.

    Use the entry candle end as the conservative timer origin (actual fill can
    be up to one minute earlier). Never apply a later candle close to its open.
    Retain the native stop-first convention for ambiguous intrabar touches.
    """
    sign = 1 if is_long else -1
    target = entry * (1 + sign * target_pct / 100)
    activation = int(path["timestamp_ns"][entry_index]) + TIGHTEN_AFTER_MINUTES * MINUTE_NS
    active_stop = INITIAL_STOP_PCT
    for j in range(entry_index, len(path["close"])):
        if int(path["timestamp_ns"][j]) - MINUTE_NS >= activation:
            active_stop = TIGHTENED_STOP_PCT
        stop = entry * (1 - sign * active_stop / 100)
        op, hi, lo = (float(path[key][j]) for key in ("open", "high", "low"))
        stop_open = op <= stop if is_long else op >= stop
        stop_hit = lo <= stop if is_long else hi >= stop
        target_hit = hi >= target if is_long else lo <= target
        if stop_hit:
            at_open = j > entry_index and stop_open
            return (j, op if at_open else stop,
                    "TIGHTENED_STOP" if active_stop < INITIAL_STOP_PCT else "STOP",
                    active_stop, "OPEN" if at_open else "INTRABAR")
        if target_hit:
            return j, target, "TARGET", active_stop, "INTRABAR"
    return len(path["close"]) - 1, float(path["close"][-1]), "TIME_EXIT_1515", active_stop, "CLOSE"


# Source: fno_v13_v10_g_2_backtest.py:336
def simulate_staged(orders, paths, *, cost_bps: float, max_entry_delay_minutes: int):
    """Reuse native entry fills, then recompute all exit-dependent fields."""
    work = orders.copy()
    work["native_stop_pct"] = INITIAL_STOP_PCT
    native = g.v9.v5.simulate_native(
        work, paths, cost_bps=cost_bps, max_entry_delay_minutes=max_entry_delay_minutes
    )
    frame = native.copy()
    for ix, row in native.iterrows():
        if not row.filled:
            continue
        path = paths[int(row.sid)]
        entry, start = float(row.entry_price), int(row.entry_path_index)
        is_long = row.side == "LONG"
        sign = 1 if is_long else -1
        j, price, reason, active_stop, event = staged_exit(
            path, start, entry, is_long, float(row.native_target_pct)
        )
        gross = sign * (price / entry - 1) * 100
        excursion_end = max(start, j - 1) if event == "OPEN" else j
        mfe, mae = g.v9.v5._excursions(path, entry, start, excursion_end, is_long)
        bar_end = pd.Timestamp(int(path["timestamp_ns"][j]), tz="UTC").tz_convert("Asia/Kolkata")
        exit_ts = bar_end - pd.Timedelta(minutes=1) if event == "OPEN" else bar_end
        stop_hit = reason in ("STOP", "TIGHTENED_STOP")
        stop_level = entry * (1 - sign * active_stop / 100)
        target_level = entry * (1 + sign * float(row.native_target_pct) / 100)
        target_in_bar = path["high"][j] >= target_level if is_long else path["low"][j] <= target_level
        gap = stop_hit and event == "OPEN" and abs(price - stop_level) > 1e-8
        changes = dict(
            exit_path_index=j, exit_price=price, exit_ts=exit_ts,
            exit_bar_end_ts=str(bar_end), exit_execution_ts=str(exit_ts), exit_event=event,
            exit_reason=reason, gross_return_pct=gross, net_return_pct=gross-cost_bps/100,
            mfe_pct=max(mfe, gross), mae_pct=min(mae, gross),
            holding_minutes=(exit_ts-pd.Timestamp(row.entry_ts)).total_seconds()/60,
            initial_stop_pct=INITIAL_STOP_PCT, active_stop_pct_at_exit=active_stop,
            stop_hit=stop_hit, target_hit=reason == "TARGET", first_target_hit=reason == "TARGET",
            runner_target_hit=reason == "TARGET",
            same_bar_ambiguous=bool(stop_hit and event == "INTRABAR" and target_in_bar),
            exit_gap_through=gap, exit_gap_bps=abs(price/stop_level-1)*10000 if gap else 0.0,
        )
        for key, value in changes.items():
            frame.loc[ix, key] = value
    return frame


# Source: fno_v13_v6_portfolio_backtest.py:38
@dataclass(frozen=True)
class PortfolioConfig:
    portfolio_capital_rupees: float
    max_positions: int | None = None
    max_positions_per_symbol: int | None = None
    max_gross_exposure_rupees: float | None = None
    max_open_risk_rupees: float | None = None

    def validate(self) -> None:
        if not np.isfinite(self.portfolio_capital_rupees) or self.portfolio_capital_rupees <= 0:
            raise ValueError("portfolio_capital_rupees must be positive and finite")
        for name in ("max_positions", "max_positions_per_symbol"):
            value = getattr(self, name)
            if value is not None and value <= 0:
                raise ValueError(f"{name} must be positive when configured")
        for name in ("max_gross_exposure_rupees", "max_open_risk_rupees"):
            value = getattr(self, name)
            if value is not None and (not np.isfinite(value) or value <= 0):
                raise ValueError(f"{name} must be positive and finite when configured")


# Source: fno_v13_v6_portfolio_backtest.py:67
def _timestamps(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    for column in ("entry_ts", "exit_ts", "confirmation_ts"):
        if column in out:
            out[column] = pd.to_datetime(out[column], errors="coerce", utc=True).dt.tz_convert(
                common.IST
            )
    return out


# Source: fno_v13_v6_portfolio_backtest.py:77
def _numeric(frame: pd.DataFrame, column: str, default: float = 0.0) -> pd.Series:
    if column not in frame:
        return pd.Series(default, index=frame.index, dtype=float)
    return pd.to_numeric(frame[column], errors="coerce").fillna(default)


# Source: fno_v13_v6_portfolio_backtest.py:83
def _picker_priority(frame: pd.DataFrame) -> pd.Series:
    """Recreate each setup's documented picker value for deterministic ties."""

    picker = frame.get("picker", pd.Series("", index=frame.index)).astype(str)
    liquidity = _numeric(frame, "traded_value")
    volume = _numeric(frame, "volume_ratio")
    move = _numeric(frame, "abs_price_change_pct")
    if "abs_price_change_pct" not in frame and "price_change_pct" in frame:
        move = _numeric(frame, "price_change_pct").abs()
    return pd.Series(
        np.select(
            [picker.eq("max_liquidity"), picker.eq("max_volume"), picker.eq("max_move")],
            [liquidity, volume, move],
            default=0.0,
        ),
        index=frame.index,
        dtype=float,
    )


# Source: fno_v13_v6_portfolio_backtest.py:103
def prepare_source_ledger(source: pd.DataFrame) -> pd.DataFrame:
    required = {
        "sid",
        "day",
        "tradingsymbol",
        "filled",
        "entry_ts",
        "exit_ts",
        "capital_per_entry_rupees",
        "exposure_per_entry_rupees",
        "net_profit_rupees",
    }
    missing = sorted(required.difference(source.columns))
    if missing:
        raise ValueError(f"V13-v5 ledger is missing required columns: {missing}")
    out = _timestamps(source)
    out["filled"] = out["filled"].astype(str).str.lower().eq("true") | source[
        "filled"
    ].eq(True)
    filled = out["filled"]
    if out.loc[filled, ["entry_ts", "exit_ts"]].isna().any().any():
        raise ValueError("Filled source trades require valid entry_ts and exit_ts")
    if (out.loc[filled, "exit_ts"] < out.loc[filled, "entry_ts"]).any():
        raise ValueError("Filled source trades cannot exit before entry")
    out["portfolio_priority_value"] = _picker_priority(out)
    out["portfolio_source_row"] = np.arange(len(out), dtype=int)
    return out


# Source: fno_v13_v6_portfolio_backtest.py:132
def _trade_amounts(row: Any) -> tuple[float, float, float]:
    capital = float(row.capital_per_entry_rupees)
    exposure = float(row.exposure_per_entry_rupees)
    stop_pct = float(getattr(row, "initial_stop_pct", 0.0) or 0.0)
    estimated_cost = float(getattr(row, "cost_rupees", 0.0) or 0.0)
    risk = exposure * stop_pct / 100.0 + max(0.0, estimated_cost)
    if not all(np.isfinite(value) and value >= 0 for value in (capital, exposure, risk)):
        raise ValueError(f"Non-finite portfolio amount for sid={row.sid}")
    return capital, exposure, risk


# Source: fno_v13_v6_portfolio_backtest.py:143
def apply_portfolio_constraints(
    source: pd.DataFrame, config: PortfolioConfig
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Accept/reject frozen V13-v5 fills using causal portfolio state."""

    config.validate()
    ledger = prepare_source_ledger(source)
    ledger["portfolio_executed"] = False
    ledger["portfolio_status"] = np.where(
        ledger["filled"], "PENDING_PORTFOLIO_CHECK", "SOURCE_UNFILLED"
    )
    ledger["portfolio_reject_reason"] = np.where(
        ledger["filled"], "", "SOURCE_UNFILLED"
    )
    state_columns = (
        "portfolio_open_positions_before",
        "portfolio_reserved_capital_before_rupees",
        "portfolio_gross_exposure_before_rupees",
        "portfolio_open_risk_before_rupees",
        "portfolio_trade_capital_rupees",
        "portfolio_trade_exposure_rupees",
        "portfolio_trade_initial_risk_rupees",
    )
    for column in state_columns:
        ledger[column] = 0.0

    candidates = ledger.loc[ledger["filled"]].copy()
    candidates["_confirmation_sort"] = candidates.get(
        "confirmation_ts", candidates["entry_ts"]
    ).fillna(candidates["entry_ts"])
    candidates["_setup_sort"] = candidates.get(
        "setup_id", pd.Series("", index=candidates.index)
    ).fillna("").astype(str)
    candidates = candidates.sort_values(
        [
            "entry_ts",
            "_confirmation_sort",
            "_setup_sort",
            "portfolio_priority_value",
            "tradingsymbol",
            "sid",
        ],
        ascending=[True, True, True, False, True, True],
        kind="stable",
    )

    active: list[dict[str, Any]] = []
    peaks = {"positions": 0, "capital": 0.0, "exposure": 0.0, "risk": 0.0}
    reject_counts: dict[str, int] = {}
    for row in candidates.itertuples(index=True):
        entry_ts = row.entry_ts
        active = [position for position in active if position["exit_ts"] > entry_ts]
        open_positions = len(active)
        reserved = float(sum(position["capital"] for position in active))
        gross_exposure = float(sum(position["exposure"] for position in active))
        open_risk = float(sum(position["risk"] for position in active))
        capital, exposure, risk = _trade_amounts(row)
        ledger.loc[row.Index, list(state_columns)] = [
            open_positions,
            reserved,
            gross_exposure,
            open_risk,
            capital,
            exposure,
            risk,
        ]

        symbol_positions = sum(
            position["symbol"] == str(row.tradingsymbol) for position in active
        )
        reason = ""
        if reserved + capital > config.portfolio_capital_rupees + 1e-9:
            reason = "INSUFFICIENT_PORTFOLIO_CAPITAL"
        elif config.max_positions is not None and open_positions >= config.max_positions:
            reason = "MAX_POSITIONS_REACHED"
        elif (
            config.max_positions_per_symbol is not None
            and symbol_positions >= config.max_positions_per_symbol
        ):
            reason = "MAX_SYMBOL_POSITIONS_REACHED"
        elif (
            config.max_gross_exposure_rupees is not None
            and gross_exposure + exposure > config.max_gross_exposure_rupees + 1e-9
        ):
            reason = "MAX_GROSS_EXPOSURE_REACHED"
        elif (
            config.max_open_risk_rupees is not None
            and open_risk + risk > config.max_open_risk_rupees + 1e-9
        ):
            reason = "MAX_OPEN_RISK_REACHED"

        if reason:
            ledger.loc[row.Index, "portfolio_status"] = "REJECTED"
            ledger.loc[row.Index, "portfolio_reject_reason"] = reason
            reject_counts[reason] = reject_counts.get(reason, 0) + 1
            continue

        ledger.loc[row.Index, "portfolio_executed"] = True
        ledger.loc[row.Index, "portfolio_status"] = "EXECUTED"
        active.append(
            {
                "exit_ts": row.exit_ts,
                "symbol": str(row.tradingsymbol),
                "capital": capital,
                "exposure": exposure,
                "risk": risk,
            }
        )
        peaks["positions"] = max(peaks["positions"], len(active))
        peaks["capital"] = max(peaks["capital"], reserved + capital)
        peaks["exposure"] = max(peaks["exposure"], gross_exposure + exposure)
        peaks["risk"] = max(peaks["risk"], open_risk + risk)

    ledger["portfolio_net_profit_rupees"] = np.where(
        ledger["portfolio_executed"], _numeric(ledger, "net_profit_rupees"), 0.0
    )
    ledger["portfolio_gross_profit_rupees"] = np.where(
        ledger["portfolio_executed"], _numeric(ledger, "pre_cost_profit_rupees"), 0.0
    )
    ledger["portfolio_cost_rupees"] = np.where(
        ledger["portfolio_executed"], _numeric(ledger, "cost_rupees"), 0.0
    )
    accepted = ledger.loc[ledger["portfolio_executed"]]
    pnl = _numeric(accepted, "net_profit_rupees")
    gains = float(pnl.loc[pnl > 0].sum())
    losses = float(-pnl.loc[pnl < 0].sum())
    daily = ledger.groupby("day", sort=True)["portfolio_net_profit_rupees"].sum()
    curve = daily.cumsum()
    drawdown = curve - np.maximum.accumulate(np.r_[0.0, curve.to_numpy(float)])[1:]
    summary = {
        "schema_version": SCHEMA_VERSION,
        "source_rows": int(len(ledger)),
        "source_filled_trades": int(ledger["filled"].sum()),
        "portfolio_executed_trades": int(ledger["portfolio_executed"].sum()),
        "portfolio_rejected_trades": int(ledger["portfolio_status"].eq("REJECTED").sum()),
        "wins": int((pnl > 0).sum()),
        "losses": int((pnl < 0).sum()),
        "net_profit_rupees": float(pnl.sum()),
        "profit_factor": gains / losses if losses else (float("inf") if gains else float("nan")),
        "maximum_drawdown_rupees": float(drawdown.min()) if len(drawdown) else 0.0,
        "peak_concurrent_positions": int(peaks["positions"]),
        "peak_reserved_capital_rupees": float(peaks["capital"]),
        "peak_gross_exposure_rupees": float(peaks["exposure"]),
        "peak_open_initial_risk_rupees": float(peaks["risk"]),
        "reject_counts": reject_counts,
        "config": asdict(config),
    }
    return ledger, summary
