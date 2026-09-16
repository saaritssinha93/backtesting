# V13 V5/V6/V7/V8 Comparison Through 2026-09-08

## Scope and interpretation

- Stock comparison: 28 eligible sessions from 2026-07-29 through 2026-09-08, with 92 filled V13-V5 control trades.
- September comparison: six eligible sessions: September 1, 2, 3, 4, 7 and 8.
- Options comparison: only contracts with fetched, usable minute data. Absolute V5/V6 option P&L is not directly comparable because legacy V5 used one lot while V6 uses three lots and stricter execution.
- V6 capacity arms, V7 time exit, the option selector and V8 features are research/shadow results, not promoted strategy changes.

## Stock results - full available sample

| Version | Portfolio constraint | Trades | W/L | Net P&L | PF | Maximum drawdown |
|---|---:|---:|---:|---:|---:|---:|
| V13-V5 control | Unconstrained | 92 | 65/27 | Rs 237,394.31 | 2.777 | Rs 24,309.18 |
| V13-V6 | 1 slot / Rs 100k | 30 | 21/9 | Rs 56,432.94 | 2.079 | Rs 16,873.20 |
| V13-V6 | 2 slots / Rs 200k | 52 | 38/14 | Rs 149,897.51 | 2.884 | Rs 18,306.06 |
| V13-V6 | 3 slots / Rs 300k | 68 | 50/18 | Rs 193,131.45 | 3.189 | Rs 17,397.69 |
| V13-V6 | 5 slots / Rs 500k | 86 | 64/22 | Rs 257,474.06 | 3.537 | Rs 15,750.00 |
| V13-V7 180-minute shadow | Unconstrained | 92 | 58/34 | Rs 223,618.20 | 3.074 | Rs 10,847.61 |
| V13-V7 180-minute shadow | 3 slots / Rs 300k | 68 | 44/24 | Rs 169,529.47 | 3.208 | Rs 15,126.27 |

The five-slot V6 arm has the strongest in-sample full-period result, but it was one of several retrospectively compared capacity arms. It is evidence for capacity control, not an unbiased selected winner.

## Stock results - September only

| Version | Trades | Avg trades/session | W/L | Net P&L | PF | Maximum drawdown |
|---|---:|---:|---:|---:|---:|---:|
| V13-V5 control | 13 | 2.17 | 7/6 | -Rs 3,187.51 | 0.902 | Rs 24,309.18 |
| V13-V6 1 slot | 6 | 1.00 | 3/3 | -Rs 6,328.33 | 0.598 | Rs 15,750.00 |
| V13-V6 2 slots | 8 | 1.33 | 4/4 | Rs 2,815.61 | 1.151 | Rs 18,306.06 |
| V13-V6 3 slots | 9 | 1.50 | 5/4 | Rs 5,283.44 | 1.284 | Rs 15,838.24 |
| V13-V6 5 slots | 11 | 1.83 | 7/4 | Rs 10,606.62 | 1.570 | Rs 15,750.00 |
| V13-V7 180-minute shadow | 13 | 2.17 | 6/7 | Rs 6,938.65 | 1.375 | Rs 8,345.91 |
| V13-V7 180-minute + 3 slots | 9 | 1.50 | 5/4 | Rs 15,291.85 | 2.638 | Rs 8,345.91 |

## September stock P&L by date

| Date | V5 control | V6 3 slots | V6 5 slots | V7 180-minute + 3 slots |
|---|---:|---:|---:|---:|
| 2026-09-01 | Rs 15,300.72 | Rs 15,300.72 | Rs 15,300.72 | Rs 10,938.33 |
| 2026-09-02 | -Rs 7,750.00 | -Rs 7,750.00 | -Rs 7,750.00 | -Rs 925.45 |
| 2026-09-03 | -Rs 250.00 | -Rs 250.00 | -Rs 250.00 | -Rs 16.74 |
| 2026-09-04 | -Rs 7,750.00 | -Rs 7,750.00 | -Rs 7,750.00 | -Rs 7,403.73 |
| 2026-09-07 | -Rs 8,559.18 | -Rs 88.24 | Rs 5,234.94 | Rs 9,294.51 |
| 2026-09-08 | Rs 5,820.95 | Rs 5,820.95 | Rs 5,820.95 | Rs 3,404.92 |

September's V5 loss is mainly a crowding/exit problem. Capacity limits remove several simultaneous September 7 trades, while the 180-minute cap improves September 2, 3 and 7. The cap also gives back profit on September 1, 4 and 8.

## Regime trade-off: August versus September

| Version | August P&L | August PF | September P&L | September PF |
|---|---:|---:|---:|---:|
| V13-V5 control | Rs 173,318.73 | 2.725 | -Rs 3,187.51 | 0.902 |
| V13-V6 3 slots | Rs 144,559.93 | 3.097 | Rs 5,283.44 | 1.284 |
| V13-V6 5 slots | Rs 179,604.35 | 3.185 | Rs 10,606.62 | 1.570 |
| V13-V7 180-minute shadow | Rs 157,055.71 | 2.785 | Rs 6,938.65 | 1.375 |
| V13-V7 180-minute + 3 slots | Rs 118,588.77 | 2.795 | Rs 15,291.85 | 2.638 |

The V7 time cap is strongly helpful in September but reduces August profit by Rs 16,263 unconstrained and Rs 25,971 in the three-slot portfolio. It is therefore regime-sensitive rather than a universal improvement.

## Options comparison

Headline arm: full option lot exits at +22.5%, with a -17.5% stop.

| Version | Data window / sizing | Trading days | Trades | Avg trades/day | W/L | Net P&L | PF |
|---|---|---:|---:|---:|---:|---:|---:|
| Legacy V13-V5 ATM | Aug 26-Sep 3; one lot | 7 | 18 | 2.57 | 14/4 | Rs 56,963.26 | 4.257 |
| V13-V6 native ATM | Aug 26-Sep 8; three lots | 9 | 23 | 2.56 | 17/6 | Rs 182,355.60 | 3.623 |
| V13-V6 liquidity selector | Aug 26-Sep 8; three lots | 10 | 25 | 2.50 | 18/7 | Rs 152,316.49 | 2.595 |

### Comparable execution subset

On the 16 contracts executed by both legacy V5 and V6:

| Model | Normalized one-lot P&L |
|---|---:|
| Legacy V5 | Rs 58,118.68 |
| V6 gap/capacity/two-sided-cost execution | Rs 55,648.02 |
| V6 difference | -Rs 2,470.66 (-4.25%) |

This is the fairest V5-versus-V6 execution comparison. V6 is more conservative because it charges both entry and exit, respects volume capacity and gaps stops through at adverse opens. V6 also rejects the PRESTIGE and MANAPPURAM entries where three-lot entry capacity was unavailable.

### September options through September 8

| Option mapping | Trades | W/L | Net P&L | PF |
|---|---:|---:|---:|---:|
| Native ATM V6 | 9 | 5/4 | Rs 4,411.62 | 1.081 |
| Liquidity-selector shadow | 10 | 5/5 | -Rs 33,927.96 | 0.568 |

The partial-fill model recovered the MPHASIS exit in two fills for Rs 10,671.91, turning the prior fail-closed September result into a small profit. The liquidity selector should not be promoted: it added a MANAPPURAM fill losing Rs 20,505 and changed September 7 strikes enough to reduce that day's result.

## V13-V8 feature coverage

| Month | Ready | Insufficient causal history | Missing ready futures coverage |
|---|---:|---:|---:|
| 2026-07 | 11 | 0 | 1 |
| 2026-08 | 19 | 18 | 36 |
| 2026-09 | 0 | 12 | 4 |

V13-V8 has no valid September P&L comparison yet. The current September futures files usually contain only one earlier same-contract session, below the pre-registered minimum of three. Producing a score anyway would create an inconsistent or non-causal comparison.

## Decision summary

1. Keep V13-V5 signal generation unchanged as the control.
2. Continue V13-V6 portfolio constraints in shadow. The three-slot arm is the clean Rs 300k-capital comparison; the five-slot arm is the strongest retrospective result but needs forward validation.
3. Continue the V13-V7 180-minute arm only as a paired shadow because it helps September but hurts August and full-period P&L.
4. Use native ATM selection for options research. Reject the current liquidity-selector rule.
5. Do not evaluate V13-V8 on September until sufficient earlier futures sessions are fetched.
6. No version is ready for production promotion without bid/ask or calibrated spread modelling and an untouched forward sample.
