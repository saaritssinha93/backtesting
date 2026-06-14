# Three More New Setups (GAP_DOWN_FADE / FIRST_HOUR trap-reversals) — Honest Diagnosis (v11)
*Designed + tested 2026-06-13, same pipeline. Engines: new3_setups_scan_v11.py, ...search --phase new3,
new3_validate.py. NET of cost. These were chosen for DAY-SPREAD (fire on 138-215 days) after GAP_UP/
POWER_HOUR died on day-concentration.*

## Verdict: BOTH LONGS REJECT (day-concentration). SHORT = closest of all 5 new ideas but REJECT (fragile 4-term).

| Setup | side | n (tr/te) | best gate | verdict |
|---|---|---|---|---|
| GAP_DOWN_FADE_RECLAIM | LONG | 1526 (1425/101) | rs≥2.82 & pre1_adx≥28 & rsi_dir≤74, 0.9/1.5 | REJECT — test n13/4d, **top1day 68%**; most configs test PF<1.0 |
| FIRST_HOUR_LOW_RECLAIM | LONG | 4672 (2000/2672) | rs≤−0.94 & minute≤735 & quality≥56, 1.1/1.25 | REJECT — fires on 205 days but gated winners **top1day 72–164%** (concentrate) |
| FIRST_HOUR_HIGH_FAIL | SHORT | 3926 (2000/1926) | ema20_slope≥0.37 & pre1_adx≥33.6 & pre3_close_pos≤0.667 & minute≥720, 0.9/2.0 | **REJECT (closest)** — real spread (top1d 36–48%, 4/5 exits p0.026–0.066) BUT a **fragile 4-term**: dropping any term → insignificant (p0.16–0.25) + concentrated (top1d 57–92%) |

## FIRST_HOUR_HIGH_FAIL — the closest, but honest REJECT
This was the **first new short with real day-spread** (top1day 36–48%, not a single crash day) — clean 4-term
gate `ema20_slope≥0.37 & pre1_adx≥33.6 & pre3_close_pos≤0.667 & signal_minute≥720`, exit 0.9/2.0 → train
2.76 [3.06/2.66] / test 2.73 (n16/7d), sig at 4/5 exits, 80% months. Coherent mechanism: a strong uptrend
(rising EMA + high ADX) breaks the first-hour high, FAILS, closes weak in the afternoon = topping/distribution.

**Why REJECT anyway (honest):**
- **Fragile 4-term, all load-bearing.** Drop ema20_slope/pre1_adx/pre3_close_pos → train collapses to
  0.78/0.86/0.90. Drop the timing term (signal_minute≥720) → the 3-term core tests **p 0.158–0.249** and
  **top1day 57–92%** (insignificant + concentrated). The edge exists ONLY at the full 4-term conjunction —
  the exact fragility profile the book REJECTED for `T_TREND_DAY_EMA_STAIR_LONG` (4-term, drop-out damning).
- **Higher-PF variants depend on a BROKEN feature** (see below).
- train PF 2.76 over the 2.0 band; test n=16/7d; mechanism-odd (rising EMA on a short, though defensible as a topping trap).
- **Verdict: RESEARCH-WATCH, not promote.** Closest of 5, but no robust ≤3-term core.

## ⚠️ Data-quality finding: `vwap_dist_atr` is BROKEN in these scans
For FIRST_HOUR_HIGH_FAIL the `vwap_dist_atr` distribution runs **−168 … +136** with only **5.9% in the sane
[−3,3] range** (median 0.2, p01 −168, p99 +137) — i.e. ATR is frequently ~0, so the (close−VWAP)/ATR ratio
explodes. **Every search gate using `vwap_dist_atr≤−15` (the highest test-PF configs) is an ATR-scaling
artifact** (drop-out confirms it's load-bearing there). Any v11 setup research that gates on `vwap_dist_atr`
from these standalone structural scans is unreliable and must be re-checked / the feature recomputed.

## Tally across the 5 brand-new ideas (new2 + new3): 0 clean passes
GAP_UP_HOLD_BREAK, POWER_HOUR_LAGGARD_BREAKDOWN (single crash day 2026-05-12), GAP_DOWN_FADE_RECLAIM,
FIRST_HOUR_LOW_RECLAIM — all day-concentrated. FIRST_HOUR_HIGH_FAIL — real spread but fragile 4-term.
The day-concentration metric + term drop-out + the feature-distribution check did the discriminating.
**Nothing added to final_setup_conf.py.** Book stays at 10 active. Honest base rate holds: novel ideas mostly fail.
