"""Run the frozen rank-200-240 V12 long experiment with a 1%/1% exit.

This thin wrapper deliberately reuses the audited V6 causal walk-forward
protocol.  Only the target changes from +2% to +1%; the stop remains -1%.
It writes to a separate research directory and cannot approve production.
"""

from __future__ import annotations

from pathlib import Path

import research_v12_rank200_240_six_month_walkforward_v6 as v6
import research_v12_two_stage_long_rebuild_v5 as v5


PRODUCTION_APPROVED = False
FROZEN_CONFIG = v5.Config(
    config_id="LEVEL12_SEQ8_SL1p0_T1p0_F0p25",
    feature_family="LEVEL12_SEQ8",
    sl_pct=1.0,
    tgt_pct=1.0,
    rolling_fraction=0.25,
)
OUTPUT_DIR = Path(
    r"C:\TradingData\eqidv2_experiments"
    r"\v12_rank200_240_six_month_walkforward_t1_v7_20260205_20260804"
)
SELECTION_BIAS_NOTE = (
    "rank 200-240 was selected in-window, and the 1% target was requested "
    "after reviewing the prior 1%/2% result from these same dates"
)


def main() -> int:
    if FROZEN_CONFIG.sl_pct != 1.0 or FROZEN_CONFIG.tgt_pct != 1.0:
        raise RuntimeError("the requested 1%/1% exit contract changed")
    v6.FROZEN_CONFIG = FROZEN_CONFIG
    v6.OUTPUT_DIR = OUTPUT_DIR
    v6.SELECTION_BIAS_NOTE = SELECTION_BIAS_NOTE
    return v6.main()


if __name__ == "__main__":
    raise SystemExit(main())
