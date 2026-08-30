# FNO V10 unified 5-minute + 1-minute backtester

V10 is an isolated research launcher over the proven V8 state-machine engine.
Its first variant, `V10B`, is intentionally trade-for-trade equivalent to the
V8-Combined ten-leg baseline. This provides a clean control before adding any
new selection or confirmation filters.

## What one V10 leg contains

- Completed 5-minute cash OHLCV, EMA structure, price move, volume ratio and
  traded-value selection.
- Matched completed 5-minute near-month futures OI and exact prior-5-minute OI.
- Setup-specific ranking and maximum-entry cap.
- Exact real 1-minute confirmation bars, candle-body and adverse-wick checks,
  optional close-location and midpoint rules, and a directionally rounded
  stop-entry trigger.
- Entry expiry, post-confirmation cancellation, stop, target, square-off,
  same-bar stop-first handling, costs, exposure sizing and the global portfolio
  ledger.
- Candidate/order audit, state events, daily results, diagnostic breakdowns,
  source coverage, setup book, report and tamper-checked provenance.

The 09:50 and 09:55 LONG/SHORT research legs remain fail-closed because none
passed the prior independent TRAIN guards. They are recorded in the V10
contract but cannot generate candidates or orders.

## Commands

Create a point-in-time source snapshot, then run or validate:

```powershell
python fno_v10_unified_5m_1m_backtest.py snapshot
python fno_v10_unified_5m_1m_backtest.py run --source-snapshot <manifest.json> --from-day 2026-08-12 --through-day 2026-08-21 --cost-bps 15 --slippage-bps 0 --square-off 15:30 --eod-policy EXACT_SQUARE_OFF --rebuild-cache
python fno_v10_unified_5m_1m_backtest.py validate --provenance <run-directory>\provenance.json
```

For the retrospectively reconstructed September rollover universe on August
24-25, use `fno_v10_unified_rollover_diagnostic.py`. Its outputs remain
diagnostic-only and preserve the rollover limitations in provenance.
