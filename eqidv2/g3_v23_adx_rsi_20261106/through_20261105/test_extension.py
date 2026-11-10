"""Guard chronological appends and exact entries in history extension."""
import unittest

import pandas as pd

from .run import append_day, assert_entries


def trade(sid, day, entry=100.):
    return dict(sid=sid, day=day, setup_id='sample_long', tradingsymbol='SAMPLE',
                side='LONG', entry_ts=day + ' 10:01:00+05:30', entry_price=entry)


class ExtensionTests(unittest.TestCase):
    def setUp(self):
        self.old = pd.DataFrame([trade(1, '2026-11-04')])
        self.new = pd.DataFrame([trade(65000001, '2026-11-05')])

    def test_append_preserves_old_rows_and_adds_only_new_day(self):
        combined = append_day(self.old, self.new)
        pd.testing.assert_frame_equal(combined.iloc[:1], self.old)
        self.assertEqual(list(combined.day), ['2026-11-04', '2026-11-05'])

    def test_reject_overlap_or_wrong_day(self):
        for wrong in [self.new.assign(sid=1), self.new.assign(day='2026-11-06')]:
            with self.assertRaises(ValueError):
                append_day(self.old, wrong)

    def test_zero_fill_day_does_not_change_old_trades(self):
        pd.testing.assert_frame_equal(append_day(self.old, self.new.iloc[:0]), self.old)

    def test_same_count_different_stock_is_not_entry_parity(self):
        with self.assertRaises(AssertionError):
            assert_entries(self.old, self.old.assign(tradingsymbol='DIFFERENT'))
        with self.assertRaises(ValueError):
            assert_entries(self.old, self.old.assign(entry_price=100.01))

    def test_serialization_precision_is_allowed_but_not_changed_times(self):
        assert_entries(self.old, self.old.assign(entry_price=100. + 3e-14))
        with self.assertRaises(AssertionError):
            assert_entries(self.old, self.old.assign(entry_ts='2026-11-04 10:02:00+05:30'))


if __name__ == '__main__':
    unittest.main()
