# Hilega Milega Iterative Research Report

## Status

- Research-only.
- Not approved for live, paper, or v11 setup-book promotion.
- The best development result failed the untouched holdout.

## Correctness Repairs

- Removed every 09:15 opening-snapshot row before calculating indicators or
  setup flags. In this store, 09:15 is not a completed five-minute candle.
- Entries occur at the next available five-minute open after a completed
  signal bar.
- Same-candle stop/target collisions are resolved as stops.
- Results include estimated Zerodha/NSE equity-intraday charges and 5 bps of
  slippage per side.
- Added completed 5, 15, and 60-minute signal bars; fixed, signal-candle, ATR,
  and Bollinger exits; causal ADX; risk bounds; time windows; setup filters;
  side filters; and optional F&O-universe selection.

## Iterations Rejected

- Raw 5-minute 1% stop / 1% target: net PF 0.53 for 2026-08-03 through
  2026-08-07 on the full 1,295-symbol store.
- Timeframe and exit-only sweeps: no long or short variant reached net PF 1.0
  robustly after costs and slippage.
- Higher-timeframe regime confirmation improved short gross quality but did
  not overcome turnover costs.
- Hourly F&O shorts with ADX >= 30 reached net PF 1.98 during the August week,
  but failed validation: May 0.67, June 0.91, and July 0.75. This filter was
  rejected as regime-specific.
- Long-side market, ADX, RSI, risk, time, and higher-timeframe filters produced
  no configuration profitable in all four May-August periods.

## Best Development Candidate

Universe:

- Current near-month F&O stock underlyings: 206 historical files matched.
- This introduces survivorship bias because the August 2026 universe is
  applied to earlier months.

Rules:

- Side: short only.
- Setup: `S_HM_RSI50_REVERSAL` only.
- Signal timeframe: 60 minutes.
- Signal window: 12:15 through 14:15 IST.
- RSI(9): at or below 47.
- RSI/EMA/WMA line distance: at least 6 RSI points.
- Entry: next five-minute open.
- Stop: completed signal-candle high.
- Initial risk: 1.00% through 1.25% of entry price.
- Target: 1.35 times initial risk.
- Exit unresolved positions at end of day.
- Capital: Rs 10,000 per trade with 5x exposure (Rs 50,000 notional).
- Costs: estimated equity-intraday charges plus 5 bps slippage per side.

Development results (used during selection):

| Period | Trades | Net PF | Net P&L (Rs) |
| --- | ---: | ---: | ---: |
| May 2026 | 22 | 1.52 | 1,745 |
| June 2026 | 27 | 1.43 | 1,403 |
| July 2026 | 21 | 1.59 | 1,804 |
| Aug 3-7, 2026 | 6 | 2.24 | 1,037 |
| Combined development | 76 | 1.57 | 5,990 |

The nearby 1.30R and 1.40R variants produced net PF 1.54 and 1.56,
respectively, so the development result was not an isolated one-tick peak.

## Untouched Holdout

The 1.35R rule above was frozen before evaluating January-April 2026.

| Period | Trades | Net PF | Net P&L (Rs) |
| --- | ---: | ---: | ---: |
| January 2026 | 30 | 0.38 | -4,459 |
| February 2026 | 17 | 0.69 | -1,177 |
| March 2026 | 12 | Infinite (no losing trades) | 3,492 |
| April 2026 | 20 | 1.83 | 1,507 |
| Combined holdout | 79 | 0.95 | -637 |

Combined January-August result:

- 155 trades.
- Net PF: 1.23.
- Net P&L: Rs 5,353.

## Verdict

The strategy was improved substantially in development, but it did not retain
PF greater than 1.5 out of sample. The long side has no viable candidate. The
best short candidate is positive over the combined eight-month sample but is
too regime-dependent and too small-sample to promote.

Further threshold search on the same data should stop. Any future work should
use a new, historically point-in-time F&O universe and a later untouched date
range, then paper trade the frozen rule before considering promotion.
