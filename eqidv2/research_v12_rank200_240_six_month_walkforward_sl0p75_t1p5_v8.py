"""Run the rank-200-240 V12 long experiment with a 0.75%/1.5% exit.

Only the stop and target change.  The audited V6 walk-forward protocol,
features, filters, thresholds, V12 execution, costs, and portfolio rules are
reused unchanged.  Outputs are research-only and isolated from production.
"""

from __future__ import annotations

from pathlib import Path

import research_v12_rank200_240_six_month_walkforward_v6 as v6
import research_v12_two_stage_long_rebuild_v5 as v5


PRODUCTION_APPROVED = False
FROZEN_CONFIG = v5.Config(
    config_id="LEVEL12_SEQ8_SL0p75_T1p5_F0p25",
    feature_family="LEVEL12_SEQ8",
    sl_pct=0.75,
    tgt_pct=1.5,
    rolling_fraction=0.25,
)
OUTPUT_DIR = Path(
    r"C:\TradingData\eqidv2_experiments"
    r"\v12_rank200_240_six_month_walkforward_sl0p75_t1p5_v8_20260205_20260804"
)
SELECTION_BIAS_NOTE = (
    "rank 200-240 was selected in-window, and the 0.75%/1.5% exit was "
    "requested after reviewing earlier exit variants from these same dates"
)


def main() -> int:
    if FROZEN_CONFIG.sl_pct != 0.75 or FROZEN_CONFIG.tgt_pct != 1.5:
        raise RuntimeError("the requested 0.75%/1.5% exit contract changed")
    v6.FROZEN_CONFIG = FROZEN_CONFIG
    v6.OUTPUT_DIR = OUTPUT_DIR
    v6.SELECTION_BIAS_NOTE = SELECTION_BIAS_NOTE
    return v6.main()


if __name__ == "__main__":
    raise SystemExit(main())
