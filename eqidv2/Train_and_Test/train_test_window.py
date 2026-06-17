"""Dynamic train/test windows for the Train_and_Test pipeline.

Policy (relative to 'today', rolls forward every day):
    TEST  = the most recent `test_weeks` weeks      ->  [today - 2w + 1d, today]
    TRAIN = the `train_months` months before TEST    ->  [test_start - 3mo, test_start - 1d]

TRAIN and TEST are adjacent and non-overlapping. Both move forward each day, so the
out-of-sample TEST is always the latest 2 weeks and TRAIN is the 3 months before it.

Used as the DEFAULT window by setup_train_test.py and build_unified_pool.py; either
can still be pinned per-run via their CLI date args.
"""
from __future__ import annotations

import pandas as pd


def compute_windows(today=None, test_weeks: int = 2, train_months: int = 3) -> dict:
    """Return {'train': (start,end), 'test': (start,end), 'today':..., 'policy':...} (YYYY-MM-DD)."""
    today = pd.Timestamp.today().normalize() if today is None else pd.Timestamp(today).normalize()
    test_end = today
    # "2 weeks back" inclusive of today -> a 14-day window ending today
    test_start = today - pd.Timedelta(weeks=test_weeks) + pd.Timedelta(days=1)
    train_end = test_start - pd.Timedelta(days=1)             # adjacent, no overlap
    train_start = (train_end - pd.DateOffset(months=train_months) + pd.Timedelta(days=1)).normalize()

    def _f(t: pd.Timestamp) -> str:
        return t.strftime("%Y-%m-%d")

    return {
        "train": (_f(train_start), _f(train_end)),
        "test": (_f(test_start), _f(test_end)),
        "today": _f(today),
        "policy": f"TEST=last {test_weeks}w, TRAIN={train_months}mo before TEST (dynamic from today)",
    }


if __name__ == "__main__":
    import json
    print(json.dumps(compute_windows(), indent=2))
