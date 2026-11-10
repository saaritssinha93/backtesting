# V25: full-universe momentum entry research

This plan is recorded before the new strategy results are inspected. The objective is to test a new entry path for stocks that G3 never admits, and measure whether it captures useful momentum without buying/selling near a completed move's end. A favorable result is not promised.

## Scope and data

- Isolated offline V25; existing V18–V24 results and live/paper settings are preserved.
- All 68 available completed sessions through November 9, 2026, using `g3_v18_20261102.data.load_day()` full-stock minute histories. The reduced V24 candidate cache is not the scanner universe.
- Completed end-labeled minutes, 09:16–15:15 IST. Signals every five minutes from 09:35 through 14:00. All features use only completed observations; no future rank or daily extreme enters selection.
- Price-only scanner: eight source dates lack minute volume. No synthetic volume, substituted CVD or uneven volume gate is introduced.
- The historical universe and complete-stock-day availability are inherited and not strictly point-in-time. These dates have been inspected in previous research; the chronological checks are not genuinely unseen validation.

## Four fixed entry families

1. **Breakout control:** close beyond the preceding five one-minute highs/lows, a directional current candle, and positive signed five-minute return.
2. **Leader breakout:** breakout control, plus top-20% directional session-return percentile across every available stock at that clock, ten-minute directional efficiency at least 0.50, and directional distance above/below EMA20 from zero through two ATR14.
3. **Leader pullback:** top-20% leader with the same extension bound. An impulse from t−15 to t−5 must be at least one ATR at t−5. The intervening t−4..t−1 pullback must be 0.25–1.00 anchor ATR and at most half the impulse. The last five closes remain on the directional side of EMA20; the current close reclaims the preceding two-bar high/low with positive signed three-minute return and a directional candle.
4. **Leader union:** either of the two leader paths.

ATR14 is the arithmetic mean of the last 14 one-minute true ranges. EMA20 starts at the first observed session close and requires 20 observed bars before eligibility. Efficiency is signed ten-minute displacement divided by the sum of absolute intervening one-minute close changes. Cross-sectional ranks are calculated before signal gates. Equal average of directional session-return and five-minute-return percentiles ranks passing stocks; symbol breaks ties.

Each family admits at most one order per clock/direction, six orders per direction/day, and one order per stock/direction/day. An unfilled/cancelled order consumes its admission allowance; there is no replacement using later price evidence. Capital is unlimited, with fixed exposure per fill. The daily cap is an explicit new strategy assumption, not a capital shortage.

## Execution and exits

Enter at the exact next minute's open. Cancel if the adverse directional gap from confirmation close exceeds 0.15%; never chase a later minute. Keep the inherited conservative stop-first convention for same-bar target/stop ambiguity.

Each family has two declared targets: **0.80% primary** and **1.50% sensitivity**. Both retain the 1.50% initial stop and adopted close-confirmed 80%-of-target breakeven protection, applied from the next bar; all positions close by 15:15. Exposure is Rs 500,000 and modeled round-trip costs are Rs 250. A separate stress replay adds two basis points of adverse slippage on each entry and exit.

The published V24 baseline remains the main reference. Additional matched-target controls replay the same original G3 fills with 0.80% and 1.50% targets to distinguish exit-policy changes from entry changes. Different new-entry execution assumptions remain explicit; matched targets do not isolate every causal effect.

## Chronological evaluation and selection

Use the first 44 sessions for development descriptions, next 15 for selecting among the eight declared family/target variants, and last nine as a frozen historical check. Do not tune thresholds from the last nine dates. No additional parameter sweep is planned.

A variant qualifies for a replacement shortlist only if validation has at least 30 fills across eight active dates, positive net PnL, average net PnL per fill and profit factor no worse than the published baseline on those same dates, and daily-close drawdown no larger. Select the highest validation net PnL among qualifying variants, with variant name as deterministic tie-break. If none qualify, record that explicitly; the highest-net variant may still be shown as exploratory, without being adopted. Assess the frozen choice on the last nine dates and under the slippage stress test.

## Mover evaluation

Construct all-stock daily long and short extreme moves from the 09:16 open through 15:15, then label up to five per direction reaching at least 3%, and the top-two subset. These hindsight labels are joined only after signals and trades are fixed.

Report selected orders, actual fills, entry before the first 3% crossing, entry before the final extreme, remaining favorable price excursion and realized positive PnL separately. A useful mover capture requires an entry before the extreme, at least 0.8% room to that extreme and positive realized PnL. Entering before the final extreme alone is insufficient. Also report profitable trades outside the top-five list: those trades must not be assumed undesirable simply because they are not the largest daily movers.

Publish full-history and day-wise PnL, win rate, mean/median trade and daily PnL, losing-trade PnL and daily-close drawdown; all daily top-five cases with baseline/new-entry outcomes; source hashes, code fingerprints, tests and limitations. No automatic live deployment.
