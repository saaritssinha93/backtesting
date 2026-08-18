from __future__ import annotations

import unittest
from datetime import date, datetime, timedelta, timezone

import numpy as np
import pandas as pd

import fno_oi_backfill_5min as bf5
import fno_oi_backfill_daily as bfd
import fno_oi_common as common


def _contract(
    symbol: str = "RELIANCE26AUGFUT",
    underlying: str = "RELIANCE",
    token: int = 101,
) -> pd.Series:
    return pd.Series(
        {
            "tradingsymbol": symbol,
            "underlying": underlying,
            "instrument_token": token,
            "exchange_token": token + 1000,
            "expiry": pd.Timestamp("2026-08-25"),
            "lot_size": 250,
            "tick_size": 0.05,
            "is_index_future": False,
        }
    )


# Kite returns fixed-offset aware datetimes (+05:30), so mirror that exactly
# rather than routing through ZoneInfo in the fixture.
IST_OFFSET = timezone(timedelta(hours=5, minutes=30))


def _daily_records(start: str, days: int, *, oi_start: float = 1000.0) -> list[dict]:
    out = []
    stamp = pd.Timestamp(start).tz_localize(IST_OFFSET)
    for i in range(days):
        out.append(
            {
                "date": stamp + timedelta(days=i),
                "open": 100.0 + i,
                "high": 101.0 + i,
                "low": 99.0 + i,
                "close": 100.5 + i,
                "volume": 1000 + i,
                "oi": oi_start + i * 10,
            }
        )
    return out


class PlanWindowsTests(unittest.TestCase):
    def test_daily_window_never_exceeds_kite_limit(self):
        windows = bfd.plan_windows(date(2019, 1, 22), date(2026, 8, 10))
        self.assertGreater(len(windows), 1)
        for start, stop in windows:
            self.assertLessEqual((stop - start).days + 1, bfd.MAX_REQUEST_DAYS)

    def test_five_min_window_never_exceeds_kite_limit(self):
        windows = bf5.plan_windows(date(2026, 1, 1), date(2026, 8, 10))
        self.assertGreater(len(windows), 1)
        for start, stop in windows:
            self.assertLessEqual((stop - start).days + 1, bf5.MAX_REQUEST_DAYS)

    def test_windows_are_contiguous_and_cover_the_range(self):
        start, end = date(2020, 3, 1), date(2026, 8, 10)
        windows = bfd.plan_windows(start, end)
        self.assertEqual(windows[0][0], start)
        self.assertEqual(windows[-1][1], end)
        for earlier, later in zip(windows, windows[1:]):
            self.assertEqual(later[0] - earlier[1], timedelta(days=1))

    def test_inverted_range_yields_nothing(self):
        self.assertEqual(bfd.plan_windows(date(2026, 8, 10), date(2026, 1, 1)), [])

    def test_single_day_range(self):
        windows = bfd.plan_windows(date(2026, 8, 10), date(2026, 8, 10))
        self.assertEqual(windows, [(date(2026, 8, 10), date(2026, 8, 10))])


class RollDetectionTests(unittest.TestCase):
    """Detection must be data-driven: NSE moved monthly expiry from the last
    Thursday to the last Tuesday (2026-08 expires 2026-08-25), so a weekday
    rule mislabels rolls on one side of the change."""

    def _series(self, dates: list[str], pcts: list[float]):
        d = pd.Series(pd.to_datetime(dates))
        return d, pd.Series(pcts, index=d.index)

    def test_flags_month_end_stitch(self):
        # Mirrors the real RELIANCE 2025-08 stitch: 4.5M -> 119M OI.
        days, pct = self._series(
            ["2026-08-24", "2026-08-25", "2026-08-26", "2026-08-27"],
            [1.2, -3.0, 2553.0, 0.9],
        )
        got = bfd.detect_roll_bars(days, pct)
        self.assertEqual(got.tolist(), [False, False, True, False])

    def test_flags_stitch_landing_on_the_first(self):
        # 2026-03-31 and 2026-06-30 are last Tuesdays, so the stitch appears on
        # the 1st of the next month. A month-end-only guard misses these.
        days, pct = self._series(["2026-04-01", "2026-07-01"], [1900.0, 2100.0])
        self.assertEqual(bfd.detect_roll_bars(days, pct).tolist(), [True, True])

    def test_ignores_a_big_jump_mid_month(self):
        days, pct = self._series(["2026-08-05", "2026-08-06"], [5.0, 900.0])
        self.assertEqual(bfd.detect_roll_bars(days, pct).tolist(), [False, False])

    def test_ignores_ordinary_month_end_moves(self):
        days, pct = self._series(["2026-08-25", "2026-08-26"], [4.0, -6.0])
        self.assertEqual(bfd.detect_roll_bars(days, pct).tolist(), [False, False])

    def test_tuesday_and_thursday_expiries_both_detected(self):
        # Thursday-era roll (2025-08-28) and Tuesday-era roll (2026-08-25).
        days, pct = self._series(["2025-08-28", "2026-08-25"], [2553.0, 1800.0])
        self.assertEqual(bfd.detect_roll_bars(days, pct).tolist(), [True, True])

    def test_widen_bars_expands_around_the_stitch(self):
        days, pct = self._series(
            ["2026-08-22", "2026-08-23", "2026-08-24", "2026-08-25", "2026-08-26"],
            [1.0, 1.0, 900.0, 1.0, 1.0],
        )
        narrow = bfd.detect_roll_bars(days, pct)
        wide = bfd.detect_roll_bars(days, pct, widen_bars=1)
        self.assertEqual(int(narrow.sum()), 1)
        self.assertEqual(int(wide.sum()), 3)

    def test_nan_pct_is_not_flagged(self):
        days, pct = self._series(["2026-08-25", "2026-08-26"], [float("nan"), 900.0])
        self.assertEqual(bfd.detect_roll_bars(days, pct).tolist(), [False, True])


class OiSignalTests(unittest.TestCase):
    def test_four_quadrants(self):
        price = pd.Series([1.0, -1.0, 1.0, -1.0])
        oi = pd.Series([1.0, 1.0, -1.0, -1.0])
        got = bfd.classify_oi_signal(price, oi).tolist()
        self.assertEqual(
            got,
            ["LONG_BUILDUP", "SHORT_BUILDUP", "SHORT_COVERING", "LONG_UNWINDING"],
        )

    def test_flat_is_neutral(self):
        got = bfd.classify_oi_signal(pd.Series([0.0]), pd.Series([0.0])).tolist()
        self.assertEqual(got, ["NEUTRAL"])


class NormalizeDailyTests(unittest.TestCase):
    def test_empty_records_yield_typed_empty_frame(self):
        frame = bfd.normalize_daily_records([], _contract())
        self.assertTrue(frame.empty)
        self.assertEqual(list(frame.columns), list(bfd.DAILY_COLUMNS))

    def test_schema_and_derived_columns(self):
        frame = bfd.normalize_daily_records(_daily_records("2026-06-01", 10), _contract())
        self.assertEqual(list(frame.columns), list(bfd.DAILY_COLUMNS))
        self.assertEqual(len(frame), 10)
        self.assertEqual(frame["underlying"].unique().tolist(), ["RELIANCE"])
        self.assertEqual(frame["data_version"].unique().tolist(), [bfd.DAILY_DATA_VERSION])
        # First bar has no predecessor, so deltas are undefined.
        self.assertTrue(pd.isna(frame.loc[0, "oi_change"]))
        self.assertTrue(pd.isna(frame.loc[0, "price_change"]))

    def test_oi_change_tracks_the_series(self):
        frame = bfd.normalize_daily_records(_daily_records("2026-06-01", 5), _contract())
        non_roll = frame.loc[~frame["roll_window"]]
        deltas = non_roll["oi_change"].dropna().unique().tolist()
        self.assertEqual(deltas, [10.0])

    def test_rows_are_deduplicated_and_sorted(self):
        records = _daily_records("2026-06-01", 5) + _daily_records("2026-06-01", 5)
        frame = bfd.normalize_daily_records(records, _contract())
        self.assertEqual(len(frame), 5)
        self.assertTrue(frame["date"].is_monotonic_increasing)

    def test_stitch_blanks_cross_expiry_deltas(self):
        records = _daily_records("2026-08-22", 4)
        records[2]["oi"] = records[1]["oi"] * 30  # the stitch to the new front month
        frame = bfd.normalize_daily_records(records, _contract())
        rolls = frame.loc[frame["roll_window"]]
        self.assertEqual(len(rolls), 1)
        self.assertTrue(rolls["oi_change"].isna().all())
        self.assertEqual(rolls["oi_signal"].unique().tolist(), ["ROLL"])

    def test_accepts_a_concatenated_frame_not_just_records(self):
        # backfill_one concatenates per-window frames before normalizing;
        # list(DataFrame) yields column names, which silently emptied the result.
        records = _daily_records("2026-06-01", 4)
        merged = pd.concat([pd.DataFrame(records)], ignore_index=True)
        frame = bfd.normalize_daily_records(merged, _contract())
        self.assertEqual(len(frame), 4)
        self.assertEqual(list(frame.columns), list(bfd.DAILY_COLUMNS))

    def test_dates_are_naive_normalized_midnight(self):
        frame = bfd.normalize_daily_records(_daily_records("2026-06-01", 3), _contract())
        self.assertIsNone(frame["date"].dt.tz)
        self.assertTrue((frame["date"] == frame["date"].dt.normalize()).all())


class MarketGuardTests(unittest.TestCase):
    def test_blocks_during_session(self):
        moment = datetime(2026, 8, 10, 11, 0, tzinfo=common.IST)  # Monday
        self.assertTrue(bfd.market_is_open(moment, set()))
        self.assertTrue(bf5.market_is_open(moment, set()))

    def test_allows_after_close(self):
        moment = datetime(2026, 8, 10, 16, 30, tzinfo=common.IST)
        self.assertFalse(bfd.market_is_open(moment, set()))
        self.assertFalse(bf5.market_is_open(moment, set()))

    def test_allows_before_open(self):
        moment = datetime(2026, 8, 10, 8, 0, tzinfo=common.IST)
        self.assertFalse(bfd.market_is_open(moment, set()))

    def test_allows_on_weekend(self):
        moment = datetime(2026, 8, 8, 11, 0, tzinfo=common.IST)  # Saturday
        self.assertFalse(bfd.market_is_open(moment, set()))

    def test_allows_on_holiday(self):
        moment = datetime(2026, 8, 10, 11, 0, tzinfo=common.IST)
        self.assertFalse(bfd.market_is_open(moment, {date(2026, 8, 10)}))


class ConstantsTests(unittest.TestCase):
    def test_measured_api_limits_are_pinned(self):
        # These mirror limits probed against the live API; if Kite changes them
        # the backfill silently truncates, so pin them explicitly.
        self.assertEqual(bfd.MAX_REQUEST_DAYS, 2000)
        self.assertEqual(bf5.MAX_REQUEST_DAYS, 100)
        self.assertEqual(bfd.OI_FLOOR, date(2019, 1, 22))

    def test_five_min_backfill_writes_into_the_live_store(self):
        # The ranker and EOD QC read raw_contract_path; the backfill must not
        # fork a parallel location.
        self.assertTrue(
            str(common.raw_contract_path("X26AUGFUT")).endswith("_5minute.parquet")
        )


if __name__ == "__main__":
    unittest.main()
