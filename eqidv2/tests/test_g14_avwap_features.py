"""Point-in-time, stored-parity and invalid-prefix checks for V14 AVWAP."""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from research.g14_avwap_features import enrich_day


DAY = "2026-11-02"
CLOCKS = ("09:20", "09:25", "09:30", "09:35", "09:40", "09:45")


def bars() -> pd.DataFrame:
    prices = (
        (100., 102., 99., 101., 100.),
        (101., 104., 100., 103., 200.),
        (103., 103., 101., 102., 150.),
        (102., 105., 102., 104., 175.),
        (104., 106., 103., 105., 180.),
        (105., 107., 104., 106., 190.),
    )
    return pd.DataFrame([
        dict(sid=i+1, day=DAY, signal_ts=pd.Timestamp(f"{DAY} {clock}", tz="Asia/Kolkata"),
             tradingsymbol="ABC", side="SHORT" if i == 2 else "LONG",
             open=o, high=h, low=low, close=c, volume=v, source_1m_count=5,
             v18_vwap=np.nan, v18_vwap_slope_pct=np.nan)
        for i, (clock, (o, h, low, c, v)) in enumerate(zip(CLOCKS, prices))
    ])


def test_open_anchor_slope_pressure_and_source_row_order() -> None:
    raw = bars().iloc[[3, 0, 5, 2, 1, 4]].copy()
    out = enrich_day(raw, DAY)
    assert out.index.tolist() == raw.index.tolist()
    assert out.sid.tolist() == raw.sid.tolist()
    by_sid = out.set_index("sid")
    first = (102 + 99 + 101) / 3
    second = (104 + 100 + 103) / 3
    second_avwap = (100*first + 200*second) / 300
    assert by_sid.loc[1, "flow_avwap"] == pytest.approx(first)
    assert by_sid.loc[2, "flow_avwap"] == pytest.approx(second_avwap)
    assert by_sid.loc[2, "flow_avwap_slope_signed_pct"] == pytest.approx(
        100 * (second_avwap / first - 1))
    assert np.isnan(by_sid.loc[1, "flow_avwap_slope_signed_pct"])
    assert not bool(by_sid.loc[1, "flow_avwap_slope_ready"])
    first_location = (2*101-102-99)/(102-99)
    second_location = (2*103-104-100)/(104-100)
    assert by_sid.loc[2, "flow_pressure3_signed"] == pytest.approx(
        (100*first_location + 200*second_location)/300)
    assert by_sid.loc[3, "flow_avwap_distance_signed_pct"] == pytest.approx(
        -100*(102/by_sid.loc[3, "flow_avwap"]-1))
    assert by_sid.flow_avwap_ready.all()
    assert by_sid.flow_avwap_source.eq("FROZEN_5M_RECONSTRUCTED").all()


def test_future_bar_change_cannot_change_prior_feature() -> None:
    raw = bars()
    left = enrich_day(raw, DAY)
    changed = raw.copy()
    changed.loc[changed.sid.eq(6), ["high", "close", "volume"]] = [200., 199., 10000.]
    right = enrich_day(changed, DAY)
    for field in ("flow_avwap", "flow_avwap_distance_signed_pct", "flow_pressure3_signed"):
        np.testing.assert_allclose(left.loc[left.sid.lt(6), field], right.loc[right.sid.lt(6), field],
                                   rtol=0, atol=1e-12)


def test_missing_middle_bar_breaks_all_later_prefixes() -> None:
    raw = bars().loc[lambda x: x.sid.ne(2)].copy()
    out = enrich_day(raw, DAY).set_index("sid")
    assert bool(out.loc[1, "flow_avwap_ready"])
    assert not out.loc[3:, "flow_avwap_ready"].any()
    assert out.loc[3:, "flow_avwap"].isna().all()
    assert out.loc[3:, "flow_pressure3_signed"].isna().all()


def test_zero_volume_or_bad_minute_count_fails_closed() -> None:
    raw = bars()
    raw.loc[raw.sid.eq(2), "volume"] = 0.
    out = enrich_day(raw, DAY).set_index("sid")
    assert bool(out.loc[1, "flow_avwap_ready"])
    assert not out.loc[2:, "flow_avwap_ready"].any()
    raw = bars()
    raw.loc[raw.sid.eq(4), "source_1m_count"] = 4
    out = enrich_day(raw, DAY).set_index("sid")
    assert out.loc[1:3, "flow_avwap_ready"].all()
    assert not out.loc[4:, "flow_avwap_ready"].any()


@pytest.mark.parametrize("flag", (
    "gap_filled", "opening_snapshot", "provisional_stale",
    "is_synthetic", "synthetic",
))
def test_flagged_prior_bar_breaks_later_cumulative_prefix(flag: str) -> None:
    raw = bars()
    raw[flag] = False
    raw.loc[raw.sid.eq(2), flag] = True
    out = enrich_day(raw, DAY).set_index("sid")
    assert bool(out.loc[1, "flow_avwap_ready"])
    assert not out.loc[2:, "flow_avwap_ready"].any()
    assert out.loc[2:, "flow_avwap"].isna().all()
    assert out.loc[2:, "flow_pressure3_signed"].isna().all()


def test_stored_v18_parity_and_missing_late_reconstruction() -> None:
    raw = bars()
    expected = enrich_day(raw, DAY)
    raw.loc[raw.sid.le(4), "v18_vwap"] = expected.loc[expected.sid.le(4), "flow_avwap"].to_numpy()
    raw.loc[raw.sid.between(2, 4), "v18_vwap_slope_pct"] = (
        expected.loc[expected.sid.between(2, 4), "flow_avwap_slope_signed_pct"].to_numpy()
        * np.where(raw.loc[raw.sid.between(2, 4), "side"].eq("LONG"), 1., -1.))
    out = enrich_day(raw, DAY).set_index("sid")
    assert out.loc[1:4, "flow_avwap_source"].eq("V18_VALIDATED").all()
    assert out.loc[5:6, "flow_avwap_source"].eq("FROZEN_5M_RECONSTRUCTED").all()
    assert out.loc[5:6, "flow_avwap_slope_ready"].all()


def test_mismatched_stored_v18_rejected() -> None:
    raw = bars()
    raw.loc[raw.sid.eq(2), "v18_vwap"] = 999.
    with pytest.raises(ValueError, match="VWAP parity failed"):
        enrich_day(raw, DAY)


def test_naive_signal_timestamp_rejected() -> None:
    raw = bars()
    raw["signal_ts"] = raw.signal_ts.astype(object)
    raw.at[0, "signal_ts"] = pd.Timestamp("2026-11-02 09:20:00")
    with pytest.raises(ValueError, match="timezone-aware"):
        enrich_day(raw, DAY)
