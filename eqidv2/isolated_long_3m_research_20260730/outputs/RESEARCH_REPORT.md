# Three-month intraday LONG research

## Decision: NO_STRATEGY_PASSED_ALL_GATES

This isolated harness does not import existing repository code for execution and writes only beneath its own directory. Unrelated concurrent workspace changes are left untouched.

## Chronological split

- TRAIN: 2026-05-05 to 2026-07-01 (40 sessions)
- VALIDATION: 2026-07-02 to 2026-07-15 (10 sessions)
- Untouched TEST: 2026-07-16 to 2026-07-29 (10 sessions)

Entry configurations and broad target/stop/time-exit regions were screened on TRAIN. VALIDATION selected one frozen finalist per strategy family. TEST was evaluated only afterward.

## Execution assumptions

- Completed five-minute signals only; entry can begin one minute later.
- Trigger is signal high plus one NSE tick, valid for three one-minute bars.
- Cancel first when trigger and invalidation are ambiguous in the same minute.
- Reject gaps over 0.20%; apply 5 bps adverse entry slippage.
- Resolve target/stop using one-minute OHLC; same-minute ties go to stop.
- Stop gaps fill at the worse open; non-target exits receive 5 bps adverse slippage.
- Forced exit no later than 15:15 and Zerodha-style 2026 statutory costs.
- Constant approximately Rs 100,000 notional per trade for comparable strategy returns.

## Frozen finalists

| Strategy | Profile/variant | Target | Stop | Time | Train PF | Validation PF | Test trades | Test PF | Test expectancy | Test net | 150% slip PF | Verdict |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| pullback | loose/pullback | 0.85% | 0.80% | 40m | 0.593 | 0.544 | 839 | 0.590 | Rs -119.08 | Rs -99,911.30 | 0.544 | REJECT |
| orb | loose/or15 | 0.85% | 0.90% | 50m | 0.568 | 0.554 | 661 | 0.466 | Rs -199.12 | Rs -131,618.59 | 0.437 | REJECT |
| compression | strict/bb10 | 0.75% | 0.80% | 60m | 1.078 | 0.844 | 47 | 0.488 | Rs -134.11 | Rs -6,303.28 | 0.446 | REJECT |

Acceptance requires untouched TEST PF above 1.40, positive expectancy after all costs, at least 100 TEST trades, low ticker/day concentration, and survival under 50% extra slippage.

## Search coverage

- Accepted ticker files: 1240
- Broad structural candidate rows: 1190398
- Rows in the union of all selectable configurations: 189145
- Entry configurations: 144
- Shortlisted entry configurations: 54
- TRAIN target/stop/time combinations: 5832
- VALIDATION combinations: 60
- Fixed exit grid: targets 0.35%-0.85%, stops 0.40%-0.90%, three nearby time exits per family.
- ATR-adjusted and structural stops are reported separately for each frozen TEST finalist.

Detailed configurations, metrics, rejected entry counts, trades, daily summaries, ticker summaries, and the full TRAIN/VALIDATION grids are stored beside this report.