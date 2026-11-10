# V14 OI-day OR entry selector

`research/g14_oi_day_or_selection.py` is an isolated backtest selector. It reads
the frozen full prefilter stock pool and does not modify the V14 V1 entrypoint,
V14 V2 experiment, or paper/live trading. The proposed overlay is
`configs/strategies/v13_v10_g_14_oi_day_or_v3.json`.

## Exact requested change

For each of V14's six morning signal clocks and LONG/SHORT setups, the new OI
condition replaces the old per-setup **minimum** OI change:

* `OI day % > 1.00%` using current OI versus the first completed 09:20 OI of
  the same dated futures contract; **OR**
* latest completed-bar OI increased strictly (`current OI > immediately
  preceding 5m OI`). At 09:20 only, this is the observed 09:16–09:20 **4-minute**
  comparison; missing opening evidence fails.

The day branch does not require a positive latest-bar change. The rising branch
does not require a valid 09:20 day baseline. Each branch checks the contract
identity relevant to its comparison. A source `prev_oi` from an older available
bar is **not** an exact latest-bar comparison: for example, HINDZINC at 09:35
on September 18 has a stale prior OI from 09:25, no 09:30 OI, and day OI
+0.308%; both new OR branches fail. Its archived V14 V1 trade remains in the
comparator as recorded.

V14's old **upper** OI-change cap (1.00%) is a separate retained gate. If raw
latest OI-change evidence is entirely absent, a valid day branch can be true
but the overall selection fails closed on that upper cap; no H>1% row in the
63-day frozen source has this conflict. This is disclosed rather than silently
dropping the cap.

The `remove_body_wick` and `combined` arms remove the setup minimum candle-body
ratio and maximum directional one-minute wick ratio from **active and core**
gates. The special 09:26 SHORT minimum lower wick of 0.20 remains, since the
request only removed the maximum wick. EMA, price, five- and one-minute volume,
confirmation, extension, OI upper cap, pickers, setup quotas and targets remain
as in V14 V1. Strict +1.00% day OI does **not** pass; the code compares OI
levels with `100*current > 101*opening` to avoid floating-point rounding at
the boundary.

## Four isolated arms

`select_day(candidates, arm)` returns `(selected, audit)` for one dated full
prefilter candidate frame. Arms are `baseline_reconstructed`,
`remove_body_wick`, `oi_day_or`, and `combined`. `oi_day_or_branches(frame)`
returns the independent branch decisions by source SID. This selector does not
read trade outcomes.

The candidate file's V14 V2 `integrity_ok` is intentionally **not** used: it
requires official PDC data, which is unrelated to the V14 V1 strategy and this
OI-only addition. Before adding G7 post-selection permissions, the reconstructed
baseline yields 119 orders versus 105 archived V14 V1 opportunities. It
contains **all 105** archived keys and 14 additional full-pool keys. Those
extras must be attributed/audited by the runner; the reconstructed control
cannot be labelled an exact V14 V1 reproduction. The current selector also
does not silently approximate G7's Nifty-futures two-direction or
low-efficiency veto; the runner must apply them with causal evidence or mark
this as a comparability limit.

Full pre-postselection smoke across the 63 dated sessions: reconstructed
baseline 119 orders, body/wick removal 177, OI OR replacement 196, combined
282. These counts are **selected orders before minute fills, portfolio limits
and G7 post-selection permissions**, not trades or performance results.
