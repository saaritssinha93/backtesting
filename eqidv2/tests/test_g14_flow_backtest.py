"""Check V14 flow-gate warmup and missing-data behavior."""
from __future__ import annotations

import numpy as np
import pandas as pd

from research import g14_flow_backtest as flow


def test_flow_gates_and_rvol_matched_readiness() -> None:
    frame = pd.DataFrame({
        "flow_avwap": [100., 100., np.nan],
        "flow_avwap_distance_signed_pct": [.10, -.10, np.nan],
        "flow_avwap_slope_signed_pct": [np.nan, .01, np.nan],
        "flow_avwap_ready": [True, True, False],
        "flow_avwap_slope_ready": [False, True, False],
        "flow_avwap_source": ["V18_VALIDATED", "V18_VALIDATED", "INVALID_OR_MISSING_PREFIX"],
        "flow_anchor_clock": ["09:20"] * 3,
        "flow_pressure3_signed": [.20, -.20, np.nan],
        "flow_pressure3_ready": [True, True, False],
        "flow_feature_ts": pd.to_datetime(["2026-08-05 09:20+05:30"] * 3),
        "flow_tod_rvol": [1.20, .80, np.nan],
        "flow_tod_history_count": [5, 5, 4],
        "flow_tod_ready": [True, True, False],
        "flow_tod_current_valid": [True, True, True],
        "flow_tod_prior_median_volume": [100., 100., np.nan],
        "flow_tod_source": ["FROZEN_5M_VOLUME_PRIOR_CLOCK_MEDIAN"] * 3,
    })
    cfg = flow.config()
    assert flow.volume_mask(frame, "avwap_side", day_index=5, cfg=cfg).tolist() == [True, False, False]
    assert flow.volume_mask(frame, "pressure3_positive", day_index=5, cfg=cfg).tolist() == [True, False, False]
    assert flow.volume_mask(frame, "tod_rvol_ready", day_index=5, cfg=cfg).tolist() == [True, True, False]
    assert flow.volume_mask(frame, "tod_rvol_one", day_index=5, cfg=cfg).tolist() == [True, False, False]
    assert flow.volume_mask(frame, "tod_rvol_one", day_index=4, cfg=cfg).tolist() == [True, True, True]
