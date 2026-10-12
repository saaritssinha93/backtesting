import pandas as pd
import pytest

from tools.extend_v13_v10_g2_dated_results import _historical_rows_equal


def test_dated_g2_extension_preserves_prior_rows_after_csv_normalization():
    old = pd.DataFrame({"day": ["2026-10-09"], "portfolio_net_profit_rupees": [0.0],
                        "confirmation_ts": ["2026-10-09 09:26:00+05:30"]})
    current = pd.concat([old, pd.DataFrame({
        "day": ["2026-10-12"], "portfolio_net_profit_rupees": [4600.0],
        "confirmation_ts": [pd.Timestamp("2026-10-12 09:26", tz="Asia/Kolkata")],
    })], ignore_index=True)
    _historical_rows_equal(old, current)
    current.loc[0, "portfolio_net_profit_rupees"] = 1.0
    with pytest.raises(AssertionError):
        _historical_rows_equal(old, current)
