from __future__ import annotations

import research_v12_rank200_240_six_month_walkforward_sl0p75_t1p5_v8 as subject


def test_only_requested_exit_pair_changes() -> None:
    config = subject.FROZEN_CONFIG
    assert config.feature_family == "LEVEL12_SEQ8"
    assert config.sl_pct == 0.75
    assert config.tgt_pct == 1.5
    assert config.rolling_fraction == 0.25
    assert subject.PRODUCTION_APPROVED is False
