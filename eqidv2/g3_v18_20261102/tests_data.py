"""Execution-clock and input-integrity regression tests for isolated loader."""
import numpy as np
import pandas as pd
import pytest

from g3_v18_20261102 import data


def minute_session():
    return pd.DataFrame(dict(
        tradingsymbol="EXAMPLE", ts=pd.date_range("2026-10-28 09:16", periods=360, freq="min", tz=data.IST),
        open=100., high=101., low=99., close=100.5, volume=10.))


@pytest.mark.parametrize("mutation", ["gap", "duplicate", "synthetic", "negative_volume"])
def test_ineligible_execution_data_rejected(mutation):
    frame = minute_session()
    if mutation == "gap":
        frame = frame.iloc[1:]
    elif mutation == "duplicate":
        frame = pd.concat([frame, frame.iloc[:1]], ignore_index=True)
    elif mutation == "synthetic":
        frame["is_synthetic"] = False
        frame.loc[0, "is_synthetic"] = True
    else:
        frame.loc[0, "volume"] = -1
    with pytest.raises(ValueError):
        data._normalize_minutes(frame, "2026-10-28")


def test_execution_path_starts_after_confirmation_and_uses_ns():
    frame = data._normalize_minutes(minute_session(), "2026-10-28")
    confirmed = pd.Timestamp("2026-10-28 09:21", tz=data.IST)
    orders = pd.DataFrame([dict(sid=123, tradingsymbol="EXAMPLE", confirmation_ts=confirmed)])
    path = data.execution_paths(orders, frame)[123]
    assert path["timestamp_ns"][0] == (confirmed + pd.Timedelta(minutes=1)).value
    assert path["timestamp_ns"][-1] == pd.Timestamp("2026-10-28 15:15", tz=data.IST).value
    assert len(path["close"]) == 354


def test_context_resamples_exact_five_minutes_with_right_labels():
    frame = data._normalize_minutes(minute_session(), "2026-10-28")
    bars = data.context_bars(pd.DataFrame(), frame)
    assert len(bars) == 72
    assert bars.signal_ts.iloc[0] == pd.Timestamp("2026-10-28 09:20", tz=data.IST)
    assert np.array_equal(bars.source_1m_count.to_numpy(), np.full(72, 5))
    assert np.array_equal(bars.volume.to_numpy(), np.full(72, 50))
