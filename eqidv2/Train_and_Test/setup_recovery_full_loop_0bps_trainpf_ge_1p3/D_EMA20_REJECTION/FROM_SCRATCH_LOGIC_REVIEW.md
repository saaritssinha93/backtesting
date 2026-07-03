# D_EMA20_REJECTION From-Scratch Logic Review

## What It Tries To Capture
Short trend-continuation rejection when a downtrend stack retests EMA20 and resumes lower.

## Why The Previous Approach Failed
The setup-card says the pre-momentum gate is the edge. The later force-promoted Optuna version was explicitly screen-only/firehose-derived, with top_n not enforced by the live conf-mask path and a dominance warning. That is the failure mode to avoid.

## Fresh Read
This is the cleanest structural thesis of the three, but it is sample-thin and month-unstable. The rescue should prefer simple ADX/RSI/pre-momentum confirmation and reject tiny top_n screens that cannot be reproduced live.

## Review Findings

- Entry logic is structurally simple but needs confirmation from volume, candle quality, and pre-entry movement.
- Filters that only select a market-regime pocket are treated as suspect unless they also hold in FIT and VAL.
- SL/target values were swept broadly rather than only near prior values.
- TEST was not used to choose thresholds; it was only run for the baseline and full-TRAIN-band candidates.
