from __future__ import annotations

import research_v12_rank200_240_six_month_walkforward_t1_v7 as subject


def test_only_exit_change_is_one_percent_target_with_one_percent_stop() -> None:
    config = subject.FROZEN_CONFIG
    assert config.feature_family == "LEVEL12_SEQ8"
    assert config.sl_pct == 1.0
    assert config.tgt_pct == 1.0
    assert config.rolling_fraction == 0.25
    assert subject.PRODUCTION_APPROVED is False
