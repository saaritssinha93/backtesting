# D_EMA20_BOUNCE From-Scratch Logic Review

## What It Tries To Capture
Long trend-continuation bounce when an uptrend stack retests EMA20 and closes back strong.

## Why The Previous Approach Failed
This is not in the active conf book and appears as an overlay/leak candidate in the setup-card cross-check. The live survival audit saw only one recent leaked trade and it lost money. Older production-core filters were thin and not part of the current gate of record.

## Fresh Read
The raw idea is tradeable only when the retest is a real hold, not a slow rollover. Useful filters should look for rising pre-entry momentum, sane distance from VWAP/EMA, and avoid late or exhausted bounces.

## Review Findings

- Entry logic is structurally simple but needs confirmation from volume, candle quality, and pre-entry movement.
- Filters that only select a market-regime pocket are treated as suspect unless they also hold in FIT and VAL.
- SL/target values were swept broadly rather than only near prior values.
- TEST was not used to choose thresholds; it was only run for the baseline and full-TRAIN-band candidates.
