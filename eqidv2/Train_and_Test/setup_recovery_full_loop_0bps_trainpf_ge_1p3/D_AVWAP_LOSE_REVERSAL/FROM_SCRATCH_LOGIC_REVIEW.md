# D_AVWAP_LOSE_REVERSAL From-Scratch Logic Review

## What It Tries To Capture
Short reversal when a stock that was above session VWAP loses VWAP on a strong down bar.

## Why The Previous Approach Failed
Prior evidence was a small-sample short-mine: the first gate looked strong on only 26 trades, but the deeper 82-trade mine collapsed to about train PF 1.06. The high-PF pockets were mostly down-market conditioned, which is a regime bet rather than a clean setup edge.

## Fresh Read
The raw detector is structurally sensible, but it fires in the middle of a crowded VWAP-loss universe. It needs either clean fresh sell pressure, non-climax volatility, and weak relative strength, or it becomes a late short after the move is already extended.

## Review Findings

- Entry logic is structurally simple but needs confirmation from volume, candle quality, and pre-entry movement.
- Filters that only select a market-regime pocket are treated as suspect unless they also hold in FIT and VAL.
- SL/target values were swept broadly rather than only near prior values.
- TEST was not used to choose thresholds; it was only run for the baseline and full-TRAIN-band candidates.
