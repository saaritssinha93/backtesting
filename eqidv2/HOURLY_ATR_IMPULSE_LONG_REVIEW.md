# Hourly ATR-impulse LONG review

## Decision

**REJECT for live trading. Production approved: false.**

The immediate two-bar LONG premise does not have a historical edge in the
available V12 data. The apparent PF above 2 in June-August 2026 is an
in-sample, 19-trade regime result. The unchanged signal applied before that
discovery interval produced net PF 0.559 after the repaired execution model.

## Results

| Evaluation | Role | Sessions | Trades | Gross PF | Net PF | Net P&L |
|---|---|---:|---:|---:|---:|---:|
| Loose two-bar setup, 2026-06-05..2026-08-04 | Original requested baseline | 42 | 413 | 0.710 | 0.578 | -Rs 63,466.86 |
| Pullback/reclaim experiment, same dates | Rejected redesign | 42 | 13 | — | 1.223 | — |
| ATR-impulse, 2026-06-05..2026-08-04 | In-sample discovery only | 42 | 19 | 2.999 | 2.292 | +Rs 3,532.78 |
| ATR-impulse, 2025-06-16..2026-06-04 | Backward validation | 239 | 96 | 0.729 | 0.559 | -Rs 12,459.61 |

Backward-validation halves both lost: PF 0.531 / -Rs 6,395.51 and PF 0.586 /
-Rs 6,064.10. The one-sided 95% day-cluster bootstrap lower PF was 0.365;
PF after removing the five best trades was 0.434; PF with costs increased 25%
was 0.524. The result fails the locked robustness gate.

## Where the premise fails

1. Two gains of 0.50%-1.50% mean buying after roughly 1.00%-3.02% compounded
   appreciation in ten minutes. In this sample that behaved like late chase
   and exhaustion, not persistent continuation.
2. ADX, stochastic, DMI, RVOL, VWAP slope, and candle direction mostly describe
   the same move that created the trigger. Loose context removed no candidates
   in the original two-month funnel, while tighter versions still lost.
3. Costs are not the root cause. Backward gross PF was already below one at
   0.729; statutory costs reduced net PF to 0.559.
4. Thresholds selected from 19 trades are unstable. All pre-discovery months
   from June 2025 through May 2026 lost; the strong June-July 2026 pocket did
   not repeat the prior regime.
5. The historical universe uses a static current ticker manifest and lacks
   point-in-time constituents, quoted spreads, and order-book depth. Those
   limitations make a passing result less certain; they cannot explain away
   a PF far below one.

## Fixed research contract

- Six exact K300 hourly snapshots are actionable: 09:20 through 14:20.
- A list activates at slot+5 and owns signals through slot+60 inclusive.
- Therefore a 10:20 signal belongs to the 09:20 list; the new 10:20 list first
  controls 10:25.
- If the ticker remains LONG in both lists, eligibility and detector state are
  continuous. If it is absent from the refreshed list, new-signal evaluation
  stops at slot+5. An already-open trade persists under its own exit plan.
- Two completed 5-minute returns must each be 0.50%-1.50%.
- Compounded displacement/current causal ATR% must be at least 2.50, price must
  be at/above causal session VWAP, and completed traded value must be at least
  Rs 5 million. Price >= Rs 80, positive ATR, and range <= 3.5 ATR are explicit
  data/execution gates. ADX/stochastic/RVOL remain audit fields, not gates.
- Entry uses the next available 1-minute open with 5 bp adverse slippage;
  structural stop is signal low minus 0.10 ATR; maximum entry gap is 0.20 ATR;
  maximum stop distance is 1.25 ATR.
- Quantity is capped to 2% of causal expected one-minute volume, estimated from
  the completed signal five-minute volume divided by five.
- Target is 1.5R. The 15-minute no-follow-through decision fills at the next
  minute open. A two-completed-five-minute-low trail activates after 1R.

This configuration is retained only as a disabled shadow/research artifact.
No approved setup or live process was changed.

## Honest promotion gate for any replacement

- at least 300 trades, 60 sessions, and 40 active days;
- net PF >= 1.60;
- both chronological halves PF >= 1.10 and positive net P&L;
- one-sided 95% day-cluster bootstrap lower PF >= 1.20;
- PF >= 1.20 after removing the five best trades;
- PF >= 1.20 with costs increased 25%; and
- subsequent untouched forward shadow validation. A configuration change
  resets the forward clock.

The next sensible research direction is not another indicator stack on the
same immediate chase. Test a separately preregistered level-breakout or
impulse-pullback-reclaim design, then hold out later dates before inspecting
its results.
