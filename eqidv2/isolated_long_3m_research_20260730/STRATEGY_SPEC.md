# Profitable research strategy specification

## Status

**Forward-paper candidate only — not production approved.**

This compression breakout was profitable in TRAIN, VALIDATION, and the frozen
TEST, including with 50% additional exit slippage. The untouched TEST contains
only eight trades across three days, however, so the result is too small and
concentrated to establish a production-quality edge.

## Signal window

- Evaluate completed 5-minute candles from 14:00 through 14:29 IST.
- Use one trade at most per ticker per day.
- Market regime must not be bearish:
  - Nifty close above its causal EMA20;
  - Nifty EMA9 above EMA20; and
  - at least 45% of the eligible stock universe above causal session AVWAP.

## Mandatory structure

1. Bar is valid OHLCV, not gap-filled, and not an opening snapshot.
2. Close is at least INR 50.
3. Five-minute traded value is at least INR 1,000,000.
4. ATR14/close is between 0.15% and 0.90%.
5. Close is at or above causal session AVWAP and no more than 0.60% above it.
6. EMA9 is above EMA20.
7. EMA20 is higher than three completed 5-minute bars earlier.
8. Signal range is no more than 2.2 ATR.
9. Upper wick is no more than 45% of the candle range.
10. The previous completed candle was in Bollinger compression: its 20-bar
    bandwidth was at or below the causal 25th percentile of the preceding
    20 bandwidth observations in the same session.
11. Signal close is above the previous 10 completed same-session candle highs,
    but no more than 0.80% above that breakout level.

## Confirmation score

Require at least 6 of these 8:

1. ADX14 between 14 and 28.
2. ADX increased across three consecutive completed candles.
3. RSI14 between 52 and 68.
4. RSI increased across two consecutive completed candles.
5. Stochastic K is above D and K is between 20 and 88.
6. Causal time-of-day relative volume is at least 1.25.
7. Five-bar same-session OBV direction is positive.
8. Close is in the upper 40% of the signal range.

## Entry

- Planned trigger: signal high plus one INR 0.05 tick, rounded to the tick grid.
- Valid only during the next three 1-minute candles.
- Cancel if price reaches or falls below the higher of the signal low and the
  10-bar breakout level before triggering.
- If trigger and cancellation are both possible in one 1-minute candle, cancel.
- Reject when the executable opening gap is over 0.20% above the trigger.
- Apply 5 bps adverse entry slippage.

## Exit

- Target: **+0.75%**
- Stop: **-0.70%**
- Time exit: 60 minutes after entry
- Forced exit: 15:15 IST, whichever comes first
- Same-minute target/stop tie: stop first
- Stop gap: worse 1-minute open
- Target: resting limit price
- Non-target exits: 5 bps adverse slippage
- Apply the full 2026 NSE intraday statutory cost model

## Results at approximately INR 100,000 notional per trade

| Split | Trades | PF | Expectancy | Net P&L |
|---|---:|---:|---:|---:|
| TRAIN | 38 | 1.262 | INR 67.48 | INR 2,564.10 |
| VALIDATION | 12 | 1.263 | INR 44.35 | INR 532.19 |
| TEST | 8 | 3.163 | INR 226.58 | INR 1,812.63 |
| TEST, 150% exit slippage | 8 | 2.833 | INR 214.35 | INR 1,714.80 |

All nine neighboring combinations formed from targets 0.65%/0.75%/0.85% and
stops 0.60%/0.70%/0.80% were profitable in every split.

## Wiring status

Wired into the shared V11 backtester and V7 paper/live chains on 2026-07-30 at
the user's direction. This changes the implementation status, not the evidence
classification: keep it at forward-paper size until 100–300 new trades have
been collected and day/ticker concentration has been rechecked.
