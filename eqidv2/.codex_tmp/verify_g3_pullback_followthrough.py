"""Independent raw-candle recalculation of frozen G-3 follow-through exports."""
import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd

FROZEN = Path(r"C:/TradingData/eqidv2/fno_oi/strategy_research/v13_corrected_v10_g_3/frozen_20261008_long110_nextminute_v1")
SELECTIVE = FROZEN.parent / "pullback_selective_v2_20261008_final"
OUT = Path(r"C:/TradingData/eqidv2/backtesting_result_v13_v10_g_3/runs/2026-10-08/20261008T164833_pullbacks")


def clock(value):
    return pd.Timestamp(value).tz_convert("Asia/Kolkata").strftime("%Y-%m-%d %H:%M:%S")


def same(a, b):
    assert np.isclose(float(a), float(b), atol=1e-8, rtol=0), (a, b)


def lateral(raw_points, ref, sign):
    # Extract known close/fill values in chronological order, retaining the
    # execution price when it shares a candle-end label with the close.
    values = {}
    for p in raw_points:
        if p[1] in ("CLOSE", "FILL"):
            values[p[0]] = p[2]
    stamps = sorted(values)
    duration = (stamps[-1]-stamps[0]).total_seconds()/60 if stamps else 0.
    span = (max(values.values())-min(values.values()))/ref*100 if stamps else np.nan
    move = sign*(values[stamps[-1]]-values[stamps[0]])/ref*100 if stamps else np.nan
    enough = duration >= 15 and len(stamps) >= 16
    return duration, span, move, enough, bool(enough and span <= .30+1e-10 and abs(move) <= .15+1e-10)


book = pd.read_csv(FROZEN / "trades.csv")
book = book.loc[book.portfolio_executed]
for col in ("entry_ts", "exit_ts", "exit_bar_end_ts"):
    book[col] = pd.to_datetime(book[col], utc=True)
minutes = pd.read_parquet(FROZEN / "minute_context.parquet")
minutes["ts"] = pd.to_datetime(minutes.ts, utc=True)
predictions = pd.read_csv(SELECTIVE / "predictions_1m.csv")
for col in ("decision_ts", "event_ts"):
    predictions[col] = pd.to_datetime(predictions[col], utc=True)
source = predictions.loc[predictions.is_grid & predictions.outcome_available & predictions.event]
events = pd.read_csv(OUT / "pullback_followthrough.csv").fillna("")
summaries = pd.read_csv(OUT / "trade_path_summary.csv").fillna("").set_index("trade_id")
trades = pd.read_csv(OUT / "trades_with_pullbacks.csv")
daily = pd.read_csv(OUT / "daily_results.csv")
frozen_daily = pd.read_csv(FROZEN / "daily_results.csv")
contexts = {(str(day), symbol): g.sort_values("ts") for (day, symbol), g in minutes.groupby(["day", "tradingsymbol"])}
counts = dict(event_rows_checked=0, trade_summaries_checked=0, event_bounds_checked=0,
              recovery_checks=0, depth_checks=0, stop_headroom_checks=0, sideways_checks=0)
for tr in book.to_dict("records"):
    tid = f"{tr['day']}|{tr['setup_id']}|{tr['tradingsymbol']}|{tr['sid']}"
    s = 1 if tr["side"] == "LONG" else -1
    entry, exit_, terminal = tr["entry_ts"], tr["exit_ts"], tr["exit_bar_end_ts"]
    ref = tr["entry_price"]
    def sl(stamp):
        pct = 1. if stamp >= entry+pd.Timedelta(minutes=120) else tr["initial_stop_pct"]
        return ref*(1-s*pct/100)
    # Tuple = clock, point kind, price, adverse bound, favorable bound,
    # in-force stop, interval start, interval end. Boundary wicks excluded.
    raw = [(entry, "ENTRY", ref, ref, ref, sl(entry-pd.Timedelta(minutes=1)), entry, entry)]
    m = contexts[(str(tr["day"]), tr["tradingsymbol"])]
    completed = m.loc[m.ts.ge(entry) & m.ts.le(exit_) &
                      (m.ts.le(terminal) if tr["exit_event"] == "CLOSE" else m.ts.lt(terminal))]
    for bar in completed.itertuples():
        if bar.ts == entry and exit_ <= entry:
            continue
        adv = bar.low if s == 1 else bar.high
        fav = bar.high if s == 1 else bar.low
        raw.append((bar.ts, "CLOSE", bar.close, adv if bar.ts > entry else bar.close,
                    fav if bar.ts > entry else bar.close, sl(bar.ts-pd.Timedelta(minutes=1)),
                    bar.ts-pd.Timedelta(minutes=1), bar.ts))
    if tr["exit_event"] != "CLOSE":
        bar = m.loc[m.ts.eq(terminal)].iloc[0]
        opening = terminal-pd.Timedelta(minutes=1)
        if entry <= opening <= exit_:
            raw.append((opening, "OPEN", bar.open, bar.open, bar.open, sl(opening), opening, opening))
    raw.append((exit_, "FILL", tr["exit_price"], tr["exit_price"], tr["exit_price"],
                ref*(1-s*tr["active_stop_pct_at_exit"]/100),
                terminal-pd.Timedelta(minutes=1) if tr["exit_event"] == "INTRABAR" else exit_,
                terminal if tr["exit_event"] == "INTRABAR" else exit_))
    raw.sort(key=lambda p: p[0])
    # Independently verify the per-trade excursion summary and final30 shape.
    summary = summaries.loc[tid]
    same(summary.observed_max_adverse_from_entry_pct, max([0., *[-s*(p[3]/ref-1)*100 for p in raw]]))
    same(summary.observed_max_favorable_from_entry_pct, max([0., *[s*(p[4]/ref-1)*100 for p in raw]]))
    final = [p for p in raw if p[0] >= exit_-pd.Timedelta(minutes=30)]
    duration, span, move, enough, flat = lateral(final, ref, s)
    same(summary.final30_observation_minutes, duration)
    same(summary.final30_close_range_pct, span)
    same(summary.final30_net_move_pct, move)
    assert bool(summary.final30_sideways) == flat
    assert summary.post_entry_path_points == sum(p[1] == "CLOSE" for p in raw)
    assert summary.stop_activation_time_ist == clock(entry+pd.Timedelta(minutes=120))
    counts["trade_summaries_checked"] += 1
    actual = events.loc[events.trade_id.eq(tid)]
    expected = source.loc[source.trade_id.eq(tid)]
    assert len(actual) == len(expected) == int(summary.actual_pullback_windows)
    for w in expected.to_dict("records"):
        result = actual.loc[actual.window_start_time_ist.eq(clock(w["decision_ts"])) &
                            actual.horizon_minutes.eq(w["horizon_minutes"])].iloc[0]
        decision, hit, anchor = w["decision_ts"], w["event_ts"], w["decision_close"]
        src = w["event_source"]
        if src == "COMPLETED_BAR":
            first, last = hit-pd.Timedelta(minutes=1), hit
        elif src == "ACTUAL_EXIT_FILL" and tr["exit_event"] == "INTRABAR":
            first, last = terminal-pd.Timedelta(minutes=1), terminal
        else:
            first = last = hit
        assert (result.threshold_hit_from_ist, result.threshold_hit_by_ist) == (clock(first), clock(last))
        counts["event_bounds_checked"] += 1
        recovered_index = None
        for index, p in enumerate(raw):
            eligible_close = p[1] == "CLOSE" and p[0] > decision and (p[0] >= hit if src == "COMPLETED_BAR" else p[0] > hit)
            eligible_fill = p[1] == "FILL" and p[0] >= hit
            if (eligible_close or eligible_fill) and s*(p[2]-anchor) >= -1e-9:
                recovered_index = index
                break
        recovery = raw[recovered_index] if recovered_index is not None else None
        assert result.recovery_time_ist == (clock(recovery[0]) if recovery else "")
        assert result.recovery_basis == ("COMPLETED_CLOSE" if recovery and recovery[1] == "CLOSE" else "EXIT_FILL" if recovery else "NO_CONFIRMED_CLOSE_RECOVERY")
        counts["recovery_checks"] += 1
        considered = [p for i, p in enumerate(raw) if (p[0] > decision or (p[1] == "OPEN" and p[0] >= decision)) and (recovered_index is None or i <= recovered_index)]
        worst = max(considered, key=lambda p: -s*(p[3]/anchor-1)*100)
        same(result.worst_price, worst[3])
        same(result.worst_adverse_pct_anchor, max(0., -s*(worst[3]/anchor-1)*100))
        counts["depth_checks"] += 1
        headroom = min(s*(p[3]-p[5])/ref*100 for p in considered)
        same(result.min_stop_headroom_pct_entry, headroom)
        assert headroom >= -1e-5
        counts["stop_headroom_checks"] += 1
        duration, span, move, enough, flat = lateral([p for p in raw if p[0] >= hit], anchor, s)
        same(result.sideways_observation_minutes, duration)
        same(result.sideways_close_range_pct, span)
        same(result.sideways_net_move_pct, move)
        assert bool(result.sideways_test_passed) == flat
        counts["sideways_checks"] += 1
        if tr["exit_reason"] in ("STOP", "TIGHTENED_STOP"):
            code = "RECOVERED_THEN_LATER_SL" if recovery and recovery[1] != "FILL" else "SL_BEFORE_CLOSE_RECOVERY"
        elif tr["exit_reason"] == "TARGET":
            code = "TARGET_AFTER_PULLBACK"
        else:
            code = "SIDEWAYS_TIME_EXIT" if flat else "TIME_EXIT_TOO_SHORT" if not enough else "TIME_EXIT_NOT_SIDEWAYS"
        assert result.outcome_code == code
        counts["event_rows_checked"] += 1

assert len(events) == len(source) == 343
assert len(summaries) == len(book) == len(trades) == 93
assert len(daily) == len(frozen_daily) == 46
for field in ("trades", "net_pnl", "cost"):
    for original, updated in zip(frozen_daily.sort_values("day")[field], daily.sort_values("day")[field]):
        same(original, updated)
same(trades.net_pnl.sum(), book.portfolio_net_profit_rupees.sum())
result = dict(status="PASS", independent_of_followthrough_module=True, **counts,
              frozen_net_pnl=float(book.portfolio_net_profit_rupees.sum()),
              day_rows_preserved=46, zero_trade_days_preserved=int(daily.trades.eq(0).sum()),
              source_hashes={str(p): hashlib.sha256(p.read_bytes()).hexdigest() for p in
                             [FROZEN/"manifest.json", SELECTIVE/"predictions_1m.csv"]})
(OUT/"independent_followthrough_validation.json").write_text(json.dumps(result, indent=2), encoding="utf-8")
print(json.dumps(result, indent=2))
