# Future long/short paper setup research — 2026-07-29

## Decision

**NO-GO: there is not yet an evidence-backed “better” long and short pair to
run as a promoted paper book.**

The dashboard research sessions are useful advisory and monitoring jobs, but
they do not currently produce a governed paper-ready configuration:

- latest multi-window advisor: `0 REAL_CHANGE_READY`;
- governed full-pipeline v2 miner: `0` survivors;
- strict walk-forward gate: `INSUFFICIENT_HISTORY`, 26 source days, 0 promotes;
- qualification: `NOT QUALIFIED`, 233 trades, net PF 0.4132.

## Pair tested and rejected

The best apparent pair from the narrower reports was:

- LONG `E_ORB_BREAKOUT_LONG`;
- SHORT `A_MOD_BREAK_C1_LOW`.

It was frozen from the lab-prune2 configuration and replayed against the broad
cached all-setups history with statutory NSE intraday costs, risk-based
quantity, and 5 bps of adverse slippage per leg.

Replay source:

`C:\TradingData\eqidv2\outputs_ID_v11_all_setups_with_ab_probe_from_v10_raw_q250`

Replay output:

`C:\TradingData\eqidv2\outputs_ID_v11_future_paper_pair_20260729`

Result:

| Side/setup | Trades | Trading days | Net PF | Net P&L |
|---|---:|---:|---:|---:|
| LONG `E_ORB_BREAKOUT_LONG` | 32 | 28 | 0.561 | -Rs 11,185.03 |
| SHORT `A_MOD_BREAK_C1_LOW` | 0 | 0 | not measurable | Rs 0 |

The cache spans 245 dates from 2025-06-02 through 2026-05-29. It did not
produce eligible short rows under the frozen pre-momentum gate, so it cannot
validate the short. The long independently fails. The pair is rejected and no
launcher or candidate config is retained.

## Important replay defect fixed

Cached `final_setup_conf` replay was previously reusing the stop/target stored
in the cached row instead of the active setup book’s exit. For the tested long,
that silently replayed 0.80%/1.20% rather than the configured 1.00%/2.75%.

`avwap_5min_ID_v11_backtesting._selected_exit_override` now reads the active
final-conf exit from `v6.SETUP_EXIT_RULES` when final-conf mode is active. The
corrected replay above uses 1.00%/2.75%.

## What the dashboard sessions need before a future promotion

1. Split the 09:17 research loop and 16:15 Suggestions job into separate
   dashboard/session identities. They currently share status, heartbeat,
   scheduler mapping, and restart identity.
2. Stop treating overlapping 3/5/7/11/13/15-session windows containing the same
   ten paper trades as independent confirmation.
3. Fix the open-trade setup merge that turns setup names into
   `setup_x`/`setup_y` and reports concentration under `UNKNOWN`.
4. Emit a frozen candidate artifact with exact setup/mask/exit, config hash,
   evidence windows, cost assumptions, and `SHADOW_ONLY`/`PAPER_READY`/`REJECT`
   verdict.
5. Keep baseline and challenger signal/state/paper outputs in separate
   namespaces before any side-by-side run.

## Next valid research hypothesis

A symmetric residual-strength opening-range-break-and-retest setup is a better
research direction than adding another narrow threshold to the current mined
setups:

- rank liquid NSE F&O names by point-in-time market/sector residual momentum;
- LONG only after an opening-range-high break, AVWAP hold, and failed retest
  back below;
- SHORT only after the exact mirror: opening-range-low break, AVWAP rejection,
  and failed retest back above;
- freeze thresholds before evaluation and use completed bars only;
- risk at most 0.10% of paper equity per trade, 0.40% simultaneous open risk,
  and a -0.50% daily entry brake.

This remains `SHADOW_ONLY` until there are at least 60 untouched sessions and
50 filled trades per side. A paper-ready verdict should require net PF at least
1.10 on each side and 1.15 combined under 10 bps/leg stress, plus positive P&L
after removing the best day.

