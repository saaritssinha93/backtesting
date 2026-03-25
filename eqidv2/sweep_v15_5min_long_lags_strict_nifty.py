from __future__ import annotations

from pathlib import Path

import avwap_combined_runner_v15_5min as r
import sweep_v15_5min_long_lags as s


# Backtesting-only stricter NIFTY guard for long lag sweep.
r.NIFTY_CONTEXT_MIN_DAYMOVE_PCT = 0.50
r.NIFTY_RS_THRESHOLD_PCT = 0.30
r.NIFTY_RS_BOTH_MODE_THRESHOLD_LONG_PCT = 0.15
r.NIFTY_RS_BOTH_MODE_THRESHOLD_PCT = r.NIFTY_RS_BOTH_MODE_THRESHOLD_LONG_PCT

STRICT_OUTPUT_ROOT = Path(r"C:\TradingData\eqidv2_v15_5min_long_lag_sweep_strict_nifty_20260324")
s.OUTPUT_ROOT = STRICT_OUTPUT_ROOT
s.SUMMARY_CSV = STRICT_OUTPUT_ROOT / "summary.csv"
s.SUMMARY_JSON = STRICT_OUTPUT_ROOT / "summary.json"
s.BEST_TRADES_CSV = STRICT_OUTPUT_ROOT / "best_long_trades.csv"
s.BEST_DAYWISE_CSV = STRICT_OUTPUT_ROOT / "best_long_daywise.csv"


def main() -> None:
    print(f"[STRICT] NIFTY_CONTEXT_MIN_DAYMOVE_PCT={r.NIFTY_CONTEXT_MIN_DAYMOVE_PCT}")
    print(f"[STRICT] NIFTY_RS_THRESHOLD_PCT={r.NIFTY_RS_THRESHOLD_PCT}")
    print(
        "[STRICT] "
        f"NIFTY_RS_BOTH_MODE_THRESHOLD_LONG_PCT={r.NIFTY_RS_BOTH_MODE_THRESHOLD_LONG_PCT}"
    )
    s.main()


if __name__ == "__main__":
    main()
