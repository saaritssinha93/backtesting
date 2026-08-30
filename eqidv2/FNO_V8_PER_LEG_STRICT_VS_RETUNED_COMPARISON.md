# FNO V8 per-leg configuration comparison

Generated 2026-08-20 IST. This compares the existing **V8-Strict** and
**retuned V8** books at 15 bps round-trip cost over the same 40 official
sessions (2026-06-24 through 2026-08-19).

Selection uses the first 30 sessions through 2026-08-05. The last 10 sessions
(2026-08-06 through 2026-08-19) are shown as a stability check. Because the
retuned book and its historical results were already inspected, these are
retrospective diagnostics, not a fresh out-of-sample promotion test.

## Per-leg result

| 5m leg | Train winner | Train fills | Train win % | Train PF | Train net pts | Test fills | Test win % | Test PF | Test net pts | Honest verdict |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 09:25 LONG | **Retuned** | 35 | 54.3% | **1.814** | +7.216 | 10 | 60.0% | **2.295** | +2.869 | Best-supported leg |
| 09:25 SHORT | **Retuned** | 42 | 45.2% | **2.071** | +16.092 | 9 | 33.3% | 1.354 | +1.164 | Profitable, but test PF below 1.5 |
| 09:30 LONG | Same config | 7 | 57.1% | **3.082** | +5.915 | 1 | 100.0% | Inf. | +2.335 | Too few test trades |
| 09:30 SHORT | **Retuned** | 20 | 55.0% | **1.704** | +6.544 | 1 | 0.0% | 0.000 | -1.157 | Train winner; failed test |
| 09:35 LONG | Same config | 14 | 57.1% | 1.490 | +2.898 | 3 | 33.3% | 0.124 | -2.051 | Reject/repair |
| 09:35 SHORT | Same config | 5-6 | 66.7-80.0% | 4.941-8.017 | +8.077 to +9.084 | 0 | N/A | N/A | 0.000 | Too thin; no test evidence |
| 09:40 LONG | Same config | 10 | 40.0% | **2.221** | +3.903 | 3 | 33.3% | **1.800** | +1.044 | Positive but thin |
| 09:40 SHORT | **Strict is less weak** | 10 | 40.0% | 0.846 | -0.939 | 4 | 50.0% | 1.879 | +2.025 | No train config qualifies |
| 09:45 LONG | Same config | 4 | 50.0% | 1.672 | +0.803 | 0 | N/A | N/A | 0.000 | Too thin; no test evidence |
| 09:45 SHORT | Same config | 5-6 | 66.7-80.0% | 6.310-19.872 | +2.140 to +2.416 | 0 | N/A | N/A | 0.000 | Too thin; no test evidence |

The ranges on identical 09:35 SHORT and 09:45 SHORT configurations arise
from global duplicate/capital interactions with the other legs in each book;
they are not different leg configurations.

## Train-selected configuration for each leg

`Px`, `OI`, and `Vol` are the five-minute minimum absolute directional price
change, OI change, and volume-ratio filters. `Body` is the minimum one-minute
body/range ratio and `Wick` the maximum adverse-wick/range ratio.

| Leg | Source | Cap / picker | Px / OI / Vol | Body / Wick | SL / target | 1m confirmation policy |
|---|---|---|---|---|---|---|
| 09:25 LONG | **Retuned** | 4 / max move | .30 / .10 / 3.0 | .00 / .50 | .40% / 1.00% | Through S+3, raw trigger, no midpoint/CLV |
| 09:25 SHORT | **Retuned** | 4 / max move | .20 / .10 / 1.5 | .60 / .60 | .50% / 3.00% | Through S+3, 2 bps buffer, no midpoint/CLV; min value Rs25m |
| 09:30 LONG | Same | 1 / max move | .65 / .10 / 1.0 | .50 / .50 | 1.00% / 2.50% | S+1, raw trigger |
| 09:30 SHORT | **Retuned** | 4 / max volume | .20 / 1.00 / 1.0 | .45 / .30 | 1.00% / 4.00% | Through S+3, raw trigger, midpoint on, CLV .50; min value Rs25m |
| 09:35 LONG | Same | 1 / max liquidity | .20 / .10 / 1.0 | .60 / .50 | 1.00% / 2.50% | S+1, raw trigger |
| 09:35 SHORT | Same | 2 / max liquidity | .50 / 1.00 / 1.0 | .40 / .50 | 1.00% / 3.00% | S+1, raw trigger |
| 09:40 LONG | Same | 1 / max liquidity | .20 / .10 / 2.0 | .50 / .50 | .50% / 2.50% | S+1, raw trigger |
| 09:40 SHORT | **Strict (diagnostic only)** | 1 / max move | .20 / .10 / 1.0 | .40 / .50 | 1.00% / 3.00% | S+1, raw trigger |
| 09:45 LONG | Same | 1 / max move | .65 / .10 / 1.0 | .40 / .50 | 1.00% / 3.00% | S+1, raw trigger |
| 09:45 SHORT | Same | 1 / max volume | .20 / .75 / 1.0 | .40 / .30 | 1.00% / 2.00% | S+1, raw trigger |

## What should actually be retained

- **09:25 LONG retuned** is the only differing configuration with meaningful
  fills and PF above 1.5 in both segments.
- **09:25 SHORT retuned** is the best frequency/net choice, but its test PF is
  only 1.354.
- **09:30 SHORT retuned** wins training but fails its tiny test sample. It is
  not validated.
- Neither 09:40 SHORT configuration has positive training expectancy. Strict
  is merely less weak; the honest action is to keep this leg in shadow or
  disable it in a separately preregistered experiment.
- The later-slot legs generally have too few fills to justify individual
  optimization. Very high PF values from four to six fills are not reliable.

No combined hybrid-book result is claimed here. Combining per-leg winners
changes global duplicate/capital interactions and requires a separately
fingerprinted chronological replay. It must not be estimated by summing these
rows.

## Validity limitation

Both source snapshots expose the same 40-session coverage limitation: 3,538
of 8,320 expected symbol-sessions are incomplete. Official headline PF/P&L is
therefore N/A; all values above are `LAST_REAL_BAR_SENSITIVITY` diagnostics.

Both input runs passed their immutable provenance validators:

- Retuned V8 fingerprint: `aabdf0eafd045cb98c8dd58b242efdd2cf4255f86fa3ef08bdf529fe48a5e435`
- V8-Strict fingerprint: `7099855b76960d05e31549209ce87d78437dde122dc0f6dd01841bb07f97ff52`
