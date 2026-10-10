"""Causal staged-stop replay used by the dated production G backtest policy."""
from __future__ import annotations

import pandas as pd

import fno_v13_corrected_v5_backtest as native
from fno_v13_v10_g_policy import INITIAL_STOP_PCT, TIGHTENED_STOP_PCT, TIGHTEN_AFTER_MINUTES

MINUTE_NS = 60_000_000_000


def staged_exit(path, entry_index: int, entry: float, is_long: bool, target_pct: float):
    """Entry candle end is the conservative timer origin; ambiguous ties stop first."""
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


def simulate_staged(orders, paths, *, cost_bps: float, max_entry_delay_minutes: int):
    """Keep native entries/targets and recompute every exit-dependent field."""
    work = orders.copy()
    work["native_stop_pct"] = INITIAL_STOP_PCT
    first_pass = native.simulate_native(work, paths, cost_bps=cost_bps,
                                       max_entry_delay_minutes=max_entry_delay_minutes)
    frame = first_pass.copy()
    for ix, row in first_pass.iterrows():
        if not row.filled:
            continue
        path = paths[int(row.sid)]
        entry, start = float(row.entry_price), int(row.entry_path_index)
        is_long = row.side == "LONG"
        sign = 1 if is_long else -1
        j, price, reason, active_stop, event = staged_exit(path, start, entry, is_long, float(row.native_target_pct))
        gross = sign * (price / entry - 1) * 100
        excursion_end = max(start, j - 1) if event == "OPEN" else j
        mfe, mae = native._excursions(path, entry, start, excursion_end, is_long)
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
