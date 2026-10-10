from datetime import date, datetime
from zoneinfo import ZoneInfo

import pytest

from research.g3_dated_replay import _require_closed


IST = ZoneInfo("Asia/Kolkata")


def test_replay_rejects_live_session_before_post_close_boundary():
    day = date(2026, 10, 9)
    with pytest.raises(RuntimeError, match="post-close"):
        _require_closed(day, datetime(2026, 10, 9, 13, 0, tzinfo=IST))
    with pytest.raises(RuntimeError, match="post-close"):
        _require_closed(day, datetime(2026, 10, 9, 15, 34, tzinfo=IST))


def test_replay_allows_completed_session_but_not_future_day():
    now = datetime(2026, 10, 9, 15, 35, tzinfo=IST)
    _require_closed(date(2026, 10, 9), now)
    _require_closed(date(2026, 10, 8), now)
    with pytest.raises(RuntimeError, match="post-close"):
        _require_closed(date(2026, 10, 10), now)
