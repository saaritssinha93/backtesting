"""Focused rule and missing-evidence checks for the V14 momentum experiment."""
from __future__ import annotations

import numpy as np
import pandas as pd

from research import g14_momentum_backtest as study


def test_momentum_rules_use_side_signed_values_and_fail_closed() -> None:
    day = "2026-11-02"
    frame = pd.DataFrame({
        "signal_ts": pd.to_datetime([
            f"{day} 09:20+05:30", f"{day} 09:25+05:30",
            f"{day} 09:30+05:30", f"{day} 09:35+05:30",
        ]),
        "mom_nifty_session_signed_pct": [.04, .06, -.01, np.nan],
        "mom_nifty_bar_signed_pct": [.06, -.01, .03, np.nan],
        "mom_stock_bar_signed_pct": [.20, .20, .20, .20],
        "mom_stock_prev_bar_signed_pct": [np.nan, .10, -.20, .30],
        "mom_stock_relative_open_signed_pct": [.30, .10, .25, .50],
    })
    cfg = study.config()
    assert study.momentum_mask(frame, "nifty_session_005", cfg).tolist() == [False, True, False, False]
    assert study.momentum_mask(frame, "nifty_bar_005", cfg).tolist() == [True, False, False, False]
    assert study.momentum_mask(frame, "nifty_both_align", cfg).tolist() == [True, False, False, False]
    assert study.momentum_mask(frame, "stock_relative_open", cfg).tolist() == [True, True, True, False]
    assert study.momentum_mask(frame, "stock_two_bar", cfg).tolist() == [True, True, False, False]
    assert study.momentum_mask(frame, "nifty_or_stock_relative", cfg).tolist() == [True, True, True, False]
