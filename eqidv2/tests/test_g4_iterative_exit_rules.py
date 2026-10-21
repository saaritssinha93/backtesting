"""Causal and symmetric checks for the predeclared G4 exit hypothesis grid."""
from __future__ import annotations

import unittest

import numpy as np
import pandas as pd

from research import g4_iterative_exit_rules as rules


def _case(close=None, *, short=False, entry_high=100.1):
    close = close or [100, 100, 100.2, 100.6, 100.8, 100.7, 100.5, 100.35, 100.2, 100.1, 100.05, 100]
    c = np.asarray(close, dtype=float)
    if short:
        c = 200 - c
    n = len(c)
    ts = pd.date_range("2026-10-01 09:30", periods=n, freq="min", tz="Asia/Kolkata")
    frame = pd.DataFrame(dict(ts=ts, open=c, high=c+.05, low=c-.05, close=c,
        atr14=.1, ema20=np.full(n, 101 if not short else 99),
        ema_slope3_atr=np.full(n, -1 if not short else 1), session_vwap=np.full(n, 101 if not short else 99),
        relvol_time=np.full(n, 1.5), relvol20=np.full(n, 1.5), upper_wick_ratio=.25, lower_wick_ratio=.25))
    frame.loc[1, "high"] = entry_high if not short else 200-entry_high
    frame.loc[1, "low"] = 200-entry_high if short else 99.9
    path = dict(timestamp_ns=ts[1:].astype("int64").to_numpy(), **{key:frame[key].iloc[1:].to_numpy() for key in ("open","high","low","close")})
    row = dict(sid=1, filled=True, side="SHORT" if short else "LONG", entry_price=100., entry_ts=ts[1], confirmation_ts=ts[0], entry_path_index=0,
        exit_ts=ts[-1], native_target_pct=2.)
    return row, frame, path


def _policy(family, **params):
    return next(p for p in rules.policy_catalog() if p["family"] == family and all(p["params"][key] == value for key,value in params.items()))


class G4IterativeRulesTests(unittest.TestCase):
    def test_grid_is_fixed_unique_and_has_explicit_rationales(self):
        first, second = rules.policy_catalog(), rules.policy_catalog()
        self.assertEqual(first, second)
        self.assertEqual(len(first), 281)
        self.assertEqual(len({p["id"] for p in first}), 281)
        self.assertEqual(sum(p["kind"] == "failure" for p in first), 28)
        self.assertTrue(all(p["rationale"] for p in first))
        first[1]["params"]["target_fraction"] = -10
        self.assertGreater(rules.policy_catalog()[1]["params"]["target_fraction"], 0)

    def test_entry_candle_extreme_and_close_cannot_arm_protection(self):
        row, session, path = _case(close=[100, 150, 100, 100, 100, 100, 100, 100], entry_high=151)
        arrays = rules.trade_arrays(row, session, path)
        self.assertTrue((arrays["best_close_profit_pct"] == 0).all())
        self.assertTrue(all(event is None for event in rules.events_for_trade(row, session, path).values()))

    def test_no_activation_and_no_premature_decision(self):
        row, session, path = _case(close=[100,100,100.05,100.1,100.2,100.15,100.1,100.05])
        self.assertTrue(all(value is None for value in rules.events_for_trade(row,session,path).values()))
        row,session,path = _case()
        for event in rules.events_for_trade(row,session,path).values():
            if event is not None:
                self.assertGreaterEqual(pd.Timestamp(event["decision_at_ist"]),pd.Timestamp(row["entry_ts"])+pd.Timedelta(minutes=4))

    def test_target_giveback_is_next_open_and_long_short_symmetric(self):
        policy = _policy("target_giveback",target_fraction=.4,activation_floor_pct=.3,retained_peak_fraction=.7,entry_atr_buffer=0.,confirmation="none")
        events=[]
        for short in (False,True):
            row,session,path = _case(short=short)
            event = rules.events_for_trade(row,session,path,[policy])[policy["id"]]
            self.assertIsNotNone(event)
            decision=pd.Timestamp(event["decision_at_ist"]);opened=pd.Timestamp(event["next_bar_open_at_ist"]);end=pd.Timestamp(event["modeled_execution_at_ist"])
            self.assertEqual(decision,opened)
            self.assertEqual(opened+pd.Timedelta(minutes=1),end)
            self.assertEqual(event["next_bar_open_price"],path["open"][event["execution_path_index"]])
            events.append(event)
        self.assertEqual(events[0]["execution_path_index"],events[1]["execution_path_index"])
        self.assertAlmostEqual(events[0]["signed_move_pct"],events[1]["signed_move_pct"])

    def test_native_exit_at_signal_open_has_precedence(self):
        policy=_policy("target_giveback",target_fraction=.4,activation_floor_pct=.3,retained_peak_fraction=.7,entry_atr_buffer=0.,confirmation="none")
        row,session,path = _case()
        event=rules.events_for_trade(row,session,path,[policy])[policy["id"]]
        row["exit_ts"]=pd.Timestamp(event["next_bar_open_at_ist"])
        self.assertIsNone(rules.events_for_trade(row,session,path,[policy])[policy["id"]])

    def test_future_changes_do_not_change_first_event_or_earlier_trace(self):
        policy=_policy("target_giveback",target_fraction=.4,activation_floor_pct=.3,retained_peak_fraction=.7,entry_atr_buffer=0.,confirmation="none")
        row,session,path=_case()
        before=rules.events_for_trade(row,session,path,[policy])
        event=before[policy["id"]]
        cutoff=pd.Timestamp(event["modeled_execution_at_ist"])
        changed=session.copy(); changed.loc[changed.ts>cutoff,["open","high","low","close"]]*=10
        changed.loc[changed.ts>cutoff,"relvol_time"]=1000
        path_changed={key:value.copy() for key,value in path.items()}
        for key in ("open","high","low","close"):
            path_changed[key][path["timestamp_ns"]>cutoff.value]*=10
        self.assertEqual(before,rules.events_for_trade(row,changed,path_changed,[policy]))

    def test_missing_volume_does_not_fabricate_volume_confirmation(self):
        row,session,path=_case(close=[100,100,99.95,99.9,99.85,99.8,99.75,99.7,99.65,99.6,99.55,99.5,99.45])
        session["relvol_time"]=np.nan
        policy=_policy("delayed_failure",delay_minutes=8,maximum_peak_profit_pct=.15,confirmation="volume")
        self.assertIsNone(rules.events_for_trade(row,session,path,[policy])[policy["id"]])

    def test_appending_future_candles_cannot_change_existing_events_or_arrays(self):
        row,session,path=_case()
        before_events=rules.events_for_trade(row,session,path)
        before_arrays=rules.trade_arrays(row,session,path)
        extra=session.iloc[[-1]].copy()
        extra["ts"]+=pd.Timedelta(minutes=1)
        extra[["open","high","low","close"]]*=20
        extended=pd.concat([session,extra],ignore_index=True)
        extended_path={"timestamp_ns":np.r_[path["timestamp_ns"],extra.ts.iloc[0].value],
                       **{key:np.r_[path[key],extra[key].iloc[0]] for key in ("open","high","low","close")}}
        self.assertEqual(before_events,rules.events_for_trade(row,extended,extended_path))
        after_arrays=rules.trade_arrays(row,extended,extended_path)
        for key in rules.ARRAY_KEYS:
            np.testing.assert_array_equal(before_arrays[key],after_arrays[key],err_msg=key)

    def test_unfilled_and_invalid_warmup_produce_no_events(self):
        row,session,path=_case(); row["filled"]=False
        self.assertTrue(all(x is None for x in rules.events_for_trade(row,session,path).values()))
        row["filled"]=True;session.loc[0,"atr14"]=np.nan
        self.assertTrue(all(x is None for x in rules.events_for_trade(row,session,path).values()))

    def test_prior_rolling_structure_excludes_current_bar(self):
        row,session,path=_case()
        arrays=rules.trade_arrays(row,session,path)
        # Fourth full post-entry close,09:35, compares09:32-09:34 lows.
        index=np.where(arrays["post_entry_bars"]==4)[0][0]
        expected=(session.close.iloc[5]-session.low.iloc[2:5].min())/.1
        self.assertAlmostEqual(arrays["rolling_low3_distance_atr"][index],expected)

    def test_mature_failure_waits_for_grace_and_requires_persistent_adverse_closes(self):
        close=[100,100]+list(np.linspace(99.99,99.1,80))
        row,session,path=_case(close=close)
        policy=_policy("mature_failure",delay_minutes=60,maximum_peak_target_fraction=.2,adverse_return_pct=.4,confirmation="context")
        event=rules.events_for_trade(row,session,path,[policy])[policy["id"]]
        self.assertIsNotNone(event)
        self.assertGreaterEqual(pd.Timestamp(event["decision_at_ist"]),row["entry_ts"]+pd.Timedelta(minutes=60))
        rising=[100,100]+list(np.linspace(100.01,100.9,80))
        row,session,path=_case(close=rising)
        self.assertIsNone(rules.events_for_trade(row,session,path,[policy])[policy["id"]])

    def test_late_session_trail_never_fires_before_fourteen_hundred(self):
        policy=_policy("late_session_trail",target_fraction=.4,entry_atr_buffer=0.,confirmation="none")
        row,session,path=_case()
        self.assertIsNone(rules.events_for_trade(row,session,path,[policy])[policy["id"]])
        shift=pd.Timedelta(hours=4,minutes=30)
        session["ts"]=session.ts+shift
        for key in ("entry_ts","confirmation_ts","exit_ts"):
            row[key]+=shift
        path["timestamp_ns"]+=shift.value
        event=rules.events_for_trade(row,session,path,[policy])[policy["id"]]
        self.assertIsNotNone(event)
        self.assertGreaterEqual(pd.Timestamp(event["decision_at_ist"]).hour,14)


if __name__ == "__main__":
    unittest.main()
