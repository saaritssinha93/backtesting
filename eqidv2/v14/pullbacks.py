"""Uncalibrated, observational G-3 fixed-rule pullback monitoring for V14.

This is the original deterministic cash OHLCV monitor, not the learned selective
V2 classifier from the G-3 daywise report. Its thresholds are copied unchanged;
no model is trained and no warning changes entry, exit, capital or trade P&L.
Outcome columns are hindsight labels and never feed warning features.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

IST = "Asia/Kolkata"
MONITOR_VERSION = "G3_FIXED_RULES_UNCALIBRATED_ON_NON_NFO"
SOURCE_PATH = "research/g3_pullback_monitor.py"
SOURCE_SHA256 = "74a820e80b58f32c8c8a378c27cb18d395652e060a6904a28ce4efc112ea424e"
SPECS = {5: .30, 30: .50}


# Exact source function: research/g3_pullback_monitor.py:31
def features(minute: pd.DataFrame) -> pd.DataFrame:
    """Every value in a row depends only on that row and earlier candles."""
    m = minute.copy().sort_values("ts").reset_index(drop=True)
    if m.ts.duplicated().any():
        raise ValueError("Duplicate context minute")
    previous_close = m.close.shift(1)
    tr = pd.concat([m.high-m.low, (m.high-previous_close).abs(),
                    (m.low-previous_close).abs()], axis=1).max(axis=1)
    m["atr14"] = tr.rolling(14, min_periods=14).mean()
    m["sma5"] = m.close.rolling(5, min_periods=5).mean()
    m["sma13"] = m.close.rolling(13, min_periods=13).mean()
    m["prior3_low"] = m.low.shift(1).rolling(3, min_periods=3).min()
    m["prior3_high"] = m.high.shift(1).rolling(3, min_periods=3).max()
    denom = m.volume.shift(1).rolling(20, min_periods=20).mean()
    m["volume_ratio20"] = m.volume / denom.where(denom.gt(0))
    m["momentum3"] = m.close-m.close.shift(3)
    m["momentum5"] = m.close-m.close.shift(5)
    return m


# Exact source function: research/g3_pullback_monitor.py:51
def warning_features(row, sign: int, favorable_close: float) -> dict:
    candle_range = float(row.high-row.low)
    atr = float(row.atr14)
    required = [row.close, row.open, row.sma5, row.sma13, row.prior3_low,
                row.prior3_high, row.momentum3, row.momentum5, atr]
    ready = bool(np.isfinite(required).all() and atr > 0 and candle_range >= 0)
    if not ready:
        return dict(feature_ready=False, fast_warning=False, slow_warning=False,
                    baseline_warning=False)
    adverse_body = -sign*float(row.close-row.open)/candle_range if candle_range > 0 else 0.
    momentum3 = sign*float(row.momentum3)/atr
    momentum5 = sign*float(row.momentum5)/atr
    micro_break = bool(row.close < row.prior3_low if sign == 1 else row.close > row.prior3_high)
    trend_damage = bool(sign*(row.close-row.sma5) < 0 and sign*(row.sma5-row.sma13) < 0)
    giveback = sign*(favorable_close-float(row.close))/atr
    volume = float(row.volume_ratio20)
    fast = momentum3 <= -.5 and (micro_break or (adverse_body >= .5 and np.isfinite(volume) and volume >= 1.2))
    slow = trend_damage and momentum5 <= -.5 and giveback >= 1.
    return dict(feature_ready=True, fast_warning=bool(fast), slow_warning=bool(slow),
                baseline_warning=bool(sign*(row.close-row.open) < 0),
                adverse_body_fraction=adverse_body, momentum3_atr=momentum3,
                momentum5_atr=momentum5, micro_break=micro_break,
                trend_damage=trend_damage, giveback_atr=giveback,
                volume_ratio20=volume, atr14=atr, sma5=float(row.sma5), sma13=float(row.sma13))


# Exact source function: research/g3_pullback_monitor.py:77
def label_outcome(minute: pd.DataFrame, decision_ts: pd.Timestamp,
                  decision_close: float, sign: int, horizon: int, threshold: float,
                  exit_ts: pd.Timestamp, exit_bar_end: pd.Timestamp,
                  exit_price: float, exit_event: str = "INTRABAR") -> dict:
    """Use known temporal order; unresolved intrabar extremes stay unknown."""
    horizon_end = decision_ts + pd.Timedelta(minutes=horizon)
    end = min(horizon_end, exit_ts)
    bar_allowed = minute.ts.le(exit_bar_end) if exit_event == "CLOSE" else minute.ts.lt(exit_bar_end)
    future = minute.loc[minute.ts.gt(decision_ts) & minute.ts.le(end)
                        & bar_allowed]
    extreme = future.low if sign == 1 else future.high
    adverse = -sign*(extreme/decision_close-1)*100
    hit = future.loc[adverse.ge(threshold-1e-10)]
    event_ts = hit.ts.iloc[0] if len(hit) else pd.NaT
    event_source = "COMPLETED_BAR" if len(hit) else ""
    terminal_adverse = -sign*(exit_price/decision_close-1)*100
    exit_in_window = decision_ts < exit_ts <= horizon_end
    threshold_hit = lambda value: value >= threshold-1e-10
    exit_bar = minute.loc[minute.ts.eq(exit_bar_end)]
    opening_adverse = np.nan
    if exit_in_window and exit_event == "INTRABAR":
        if len(exit_bar) != 1:
            raise ValueError("Missing/duplicate intrabar exit candle")
        opening_ts = exit_bar_end-pd.Timedelta(minutes=1)
        opening_adverse = -sign*(float(exit_bar.open.iloc[0])/decision_close-1)*100
        if opening_ts >= decision_ts and threshold_hit(opening_adverse) and (pd.isna(event_ts) or opening_ts < event_ts):
            event_ts, event_source = opening_ts, "EXIT_CANDLE_OPEN"
    if exit_in_window and threshold_hit(terminal_adverse) and (pd.isna(event_ts) or exit_ts < event_ts):
        event_ts, event_source = exit_ts, "ACTUAL_EXIT_FILL"
    event = bool(pd.notna(event_ts))
    unknown = False
    if not event and exit_in_window and exit_event == "INTRABAR":
        adverse_extreme = float(exit_bar.low.iloc[0] if sign == 1 else exit_bar.high.iloc[0])
        unknown = bool(threshold_hit(-sign*(adverse_extreme/decision_close-1)*100))
    censored = bool(exit_ts < horizon_end and not event)
    recovered = False
    if event:
        after = future.loc[future.ts.gt(event_ts)]
        recovered = bool((sign*(after.close/decision_close-1)).ge(0).any())
        if event_ts < exit_ts <= horizon_end and sign*(exit_price/decision_close-1) >= 0:
            recovered = True
    all_adverse = [float(adverse.max())] if len(adverse) else []
    if exit_in_window:
        all_adverse.append(float(terminal_adverse))
    if np.isfinite(opening_adverse):
        all_adverse.append(float(opening_adverse))
    return dict(event=event, event_ts=event_ts, event_source=event_source,
        lead_minutes=float((event_ts-decision_ts).total_seconds()/60) if event else np.nan,
        observed_exposure_minutes=float((end-decision_ts).total_seconds()/60),
        max_future_adverse_pct=max([0., *all_adverse]),
        early_exit=bool(exit_ts < horizon_end), censored=censored,
        outcome_available=not unknown, unknown_reason="UNKNOWN_INTRABAR_ORDER" if unknown else "",
        full_horizon_eligible=not censored and not unknown, recovered_by_later_close=recovered)


def _minute_context(minute: pd.DataFrame, day: str) -> pd.DataFrame:
    """Match the archived monitor's 200 real preceding minutes and session."""
    start = pd.Timestamp(day + " 09:16", tz=IST)
    end = pd.Timestamp(day + " 15:15", tz=IST)
    prior = minute.loc[minute.ts.lt(start)].tail(200)
    session = minute.loc[minute.ts.between(start, end)]
    expected = pd.date_range(start, end, freq="min")
    if len(prior) < 200:
        raise ValueError("INSUFFICIENT_200_MINUTE_MONITOR_WARMUP")
    if session.ts.duplicated().any() or not np.array_equal(pd.DatetimeIndex(session.ts).asi8, expected.asi8):
        raise ValueError("INCOMPLETE_MONITOR_SESSION")
    result = pd.concat([prior, session], ignore_index=True)
    numeric = result[["open", "high", "low", "close", "volume"]].to_numpy(float)
    invalid = (~np.isfinite(numeric).all(axis=1) | (numeric[:, :4] <= 0).any(axis=1)
               | (numeric[:, 4] < 0) | (numeric[:, 1] < numeric[:, 2])
               | (numeric[:, 1] < np.maximum(numeric[:, 0], numeric[:, 3]))
               | (numeric[:, 2] > np.minimum(numeric[:, 0], numeric[:, 3])))
    if invalid.any():
        raise ValueError("INVALID_MONITOR_OHLCV")
    same_day = result.ts.dt.date.eq(result.ts.shift().dt.date)
    if (same_day & result.ts.diff().ne(pd.Timedelta(minutes=1))).any():
        raise ValueError("INTERRUPTED_MONITOR_CONTEXT")
    for flag in ("gap_filled", "opening_snapshot", "provisional_stale"):
        if flag in result:
            values = result[flag]
            flagged = pd.to_numeric(values, errors="coerce").fillna(0).ne(0)
            flagged |= values.astype(str).str.strip().str.lower().isin(["true", "1", "1.0", "yes", "on"])
            if flagged.any():
                raise ValueError("FLAGGED_MONITOR_CONTEXT")
    return features(result)


def _truth(values: pd.Series) -> pd.Series:
    return values.eq(True) | values.astype(str).str.strip().str.lower().isin(["true", "1", "1.0", "yes"])


def annotate_trades(ledger: pd.DataFrame, minute_by_symbol: dict[str, pd.DataFrame]) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Observe executed trades without changing any financial or signal field.

    The observations table contains the primary fixed non-overlapping windows
    and causal cooldown warning episodes. ``is_grid`` and ``emitted_warning``
    distinguish the two evaluation populations; one row can belong to both.
    Rows with ``outcome_available=False`` have unknown intrabar ordering and
    must be excluded from resolved event/confusion counts. Zero-warning periods
    are observations, not an assertion that a trade is safe.
    """
    trades = ledger.copy(deep=True)
    if not trades.index.is_unique:
        raise ValueError("Trade ledger row indices must be unique")
    trades["monitor_version"] = MONITOR_VERSION
    trades["monitor_status"] = "NOT_EXECUTED"
    trades["monitor_unavailable_reason"] = ""
    trades["first_monitor_ts"] = ""
    for column in ("monitoring_minutes", "fast_warnings", "slow_warnings", "fast_pullback_windows",
                   "slow_pullback_windows", "fast_unknown_windows", "slow_unknown_windows"):
        trades[column] = 0
    if trades.empty:
        return trades, pd.DataFrame(columns=["is_grid", "emitted_warning", "horizon_minutes", "warning", "event", "outcome_available"])
    required = {"sid", "day", "tradingsymbol", "setup_id", "side", "entry_ts", "exit_ts",
                "exit_bar_end_ts", "exit_event", "entry_price", "exit_price", "portfolio_executed"}
    missing = required.difference(trades)
    if missing:
        # All-unfilled baseline batches legitimately have no exit-bar metadata.
        if "portfolio_executed" in trades and not _truth(trades.portfolio_executed).any():
            return trades, pd.DataFrame(columns=["is_grid", "emitted_warning", "horizon_minutes", "warning", "event", "outcome_available"])
        raise ValueError(f"Missing executed-trade monitoring fields: {sorted(missing)}")
    prepared: dict[str, pd.DataFrame] = {}
    contexts: dict[tuple[str, str], pd.DataFrame | str] = {}
    observations = []
    for index, trade in trades.loc[_truth(trades.portfolio_executed)].iterrows():
        day, symbol = str(trade.day), str(trade.tradingsymbol)
        key = (day, symbol)
        if key not in contexts:
            try:
                if symbol not in prepared:
                    source = minute_by_symbol[symbol].copy()
                    source["ts"] = pd.to_datetime(source.ts, utc=True).dt.tz_convert(IST).dt.as_unit("ns")
                    clock = source.ts.dt.hour * 60 + source.ts.dt.minute
                    source = source.loc[clock.between(9 * 60 + 16, 15 * 60 + 30)]
                    prepared[symbol] = source.sort_values("ts", kind="stable").reset_index(drop=True)
                contexts[key] = _minute_context(prepared[symbol], day)
            except KeyError:
                contexts[key] = "MISSING_SYMBOL_MINUTE_CONTEXT"
            except (ValueError, TypeError) as exc:
                contexts[key] = str(exc)
        context = contexts[key]
        if isinstance(context, str):
            trades.at[index, "monitor_status"] = "MONITOR_UNAVAILABLE"
            trades.at[index, "monitor_unavailable_reason"] = context
            continue
        entry, exit_stamp, exit_end = [pd.Timestamp(trade[name]) for name in ("entry_ts", "exit_ts", "exit_bar_end_ts")]
        sign = 1 if trade.side == "LONG" else -1
        observed = context.loc[context.ts.gt(entry) & context.ts.lt(exit_stamp)]
        favorable = float(trade.entry_price)
        first = None
        decision_count = 0
        next_warning = {5: None, 30: None}
        trade_id = f"{day}|{trade.setup_id}|{symbol}|{trade.sid}"
        for candle in observed.itertuples(index=False):
            favorable = max(favorable, float(candle.close)) if sign == 1 else min(favorable, float(candle.close))
            feature = warning_features(candle, sign, favorable)
            if not feature["feature_ready"]:
                continue
            stamp = pd.Timestamp(candle.ts)
            if first is None:
                first = stamp
            elapsed = int((stamp - first).total_seconds() / 60)
            age = (stamp - entry).total_seconds() / 60
            decision_count += 1
            for horizon, threshold in SPECS.items():
                prefix = "fast" if horizon == 5 else "slow"
                warning = feature[f"{prefix}_warning"]
                emitted = bool(warning and (next_warning[horizon] is None or stamp >= next_warning[horizon]))
                if emitted:
                    next_warning[horizon] = stamp + pd.Timedelta(minutes=horizon)
                    trades.at[index, f"{prefix}_warnings"] += 1
                is_grid = elapsed % horizon == 0
                if not is_grid and not emitted:
                    continue
                outcome = label_outcome(context, stamp, float(candle.close), sign, horizon, threshold,
                                        exit_stamp, exit_end, float(trade.exit_price), str(trade.exit_event))
                observations.append(dict(trade_id=trade_id, sid=int(trade.sid), day=day, tradingsymbol=symbol,
                    setup_id=trade.setup_id, side=trade.side, monitor_version=MONITOR_VERSION,
                    decision_ts=stamp, decision_close=float(candle.close), trade_age_minutes=age,
                    phase="ENTRY_1_5" if age <= 5 else "EARLY_6_30" if age <= 30 else "LATE_31_PLUS",
                    horizon_minutes=horizon, threshold_pct=threshold, is_grid=is_grid, emitted_warning=emitted,
                    warning=warning, **feature, **outcome))
                if is_grid and outcome["outcome_available"]:
                    trades.at[index, f"{prefix}_pullback_windows"] += int(outcome["event"])
                elif is_grid:
                    trades.at[index, f"{prefix}_unknown_windows"] += 1
        trades.at[index, "monitoring_minutes"] = decision_count
        trades.at[index, "monitor_status"] = "OBSERVED_UNCALIBRATED" if decision_count else "MONITOR_UNAVAILABLE"
        trades.at[index, "first_monitor_ts"] = str(first) if first is not None else ""
        if not decision_count:
            trades.at[index, "monitor_unavailable_reason"] = "EXIT_BEFORE_FIRST_COMPLETE_POST_ENTRY_OBSERVATION"
    return trades, pd.DataFrame(observations, columns=None if observations else [
        "is_grid", "emitted_warning", "horizon_minutes", "warning", "event", "outcome_available"])


def summarize(observations: pd.DataFrame) -> pd.DataFrame:
    """Separate grid recall from alert precision; unknown labels stay explicit."""
    records = []
    for horizon in SPECS:
        window = observations.loc[observations.horizon_minutes.eq(horizon)]
        grid = window.loc[window.is_grid.eq(True)]
        resolved = grid.loc[grid.outcome_available.eq(True)]
        alerts = window.loc[window.emitted_warning.eq(True)]
        resolved_alerts = alerts.loc[alerts.outcome_available.eq(True)]
        tp = int((resolved.warning.eq(True) & resolved.event.eq(True)).sum())
        fp = int((resolved.warning.eq(True) & resolved.event.eq(False)).sum())
        fn = int((resolved.warning.eq(False) & resolved.event.eq(True)).sum())
        tn = int((resolved.warning.eq(False) & resolved.event.eq(False)).sum())
        records.append(dict(monitor_version=MONITOR_VERSION, horizon_minutes=horizon,
            grid_windows=len(grid), unresolved_windows=len(grid) - len(resolved),
            pullback_windows=tp + fn, warned_event_windows=tp, missed_event_windows=fn,
            grid_precision=tp / (tp + fp) if tp + fp else None,
            grid_recall=tp / (tp + fn) if tp + fn else None,
            grid_false_positive_rate=fp / (fp + tn) if fp + tn else None,
            warning_episodes=len(alerts), unresolved_warning_episodes=len(alerts) - len(resolved_alerts),
            alert_precision=float(resolved_alerts.event.mean()) if len(resolved_alerts) else None,
            interpretation="Observational fixed rules; uncalibrated on MIS non-NFO; no change to trade P&L"))
    return pd.DataFrame(records)
