# C_SHORT_CONTINUATION_BREAK — FROM_SCRATCH_LOGIC_REVIEW

_Generated 2026-07-03. Research-only._

## 1. What the setup is trying to capture

Downtrend continuation: after at least two successively lower closes, a bearish candle below
session VWAP breaks the rolling 20-bar pivot low on elevated volume → ride the next leg down.

Exact detector (`avwap_5min_ID_v2_backtesting._scan_day`, reason
`short_continuation_pivot_low_break`):

```
short_struct AND below_vwap AND isfinite(rl) AND close < rl
AND prev.close < prev2.close AND vol_ratio >= 1.4
```

with `short_struct = close < open AND close_loc <= 0.40` and `rl` = prior 20-bar rolling low
(shifted). Catalog scan starts ≈ 11:00 IST like all `_scan_day` setups.

## 2. The defining structural fact: TOTAL collapse shadowing

This label has **never produced a single row** in any production or research pool
(master unified pool: 0; fresh tail scans: 0; the B-family as-promoted research scan
03-01..07-01 with widened allowlist: 0 collapsed rows). Reason: the per-(ticker,candle)
collapse (`_dedupe_candidate_frame`: best quality_score, ties alphabetical) always finds a
higher-priority label on the same candle — any candle breaking a 20-bar low below VWAP on
volume also fires C_OR_BREAKDOWN (alphabetically earlier) and/or G_LOWER_LOW_BREAK /
A_MOD / B_* variants.

**Consequences:**
1. The previous "optimization failure" is not a tuning failure — the setup has literally
   never been evaluated or traded; it is dead code in the live path.
2. The only researchable basis is the **pre-collapse per-label universe** (as-promoted scan
   saving pre-collapse target rows). This campaign uses that basis and says so everywhere.
3. Even a PASSING candidate could not fire live without a collapse-priority change
   (e.g. explicit SETUP_PRIORITY entry or quality-score boost) — an approval would be
   conditional on that engineering change.

## 3. Overlap hypothesis

The pre-collapse universe is expected to overlap heavily with C_OR_BREAKDOWN (REJECTED
today: raw PF 0.28) — same short_struct/below-VWAP/volume skeleton; the differences are the
**level** (20-bar pivot low vs opening-range low), the **2-lower-closes** context
requirement, and a looser volume floor (1.4 vs 1.5). The campaign will quantify the overlap
(share of C_SHORT rows whose ticker/candle also fired C_OR_BREAKDOWN) before spending
iterations: if the population is ≥80% the same candles, the C_OR_BREAKDOWN verdict
transfers and the loop focuses only on the non-overlapping slice.

## 4. Redesign axes (mirrors the C_OR_BREAKDOWN plan, SHORT side)

Freshness (first fire of day), time windows, volume band vs climax exclusion, candle
quality (body%, wick), not-overextended (vwap_dist_atr floor), bear-tape mask, top-N
ranking, broad ADX pause gate, exit grid vs measured MAE/MFE.

## 5. Pool / backtest integrity

Same engine as the other two campaigns: 5-min detection, 1-min next-open entry + slippage,
1-min SL/target/EOD path to 15:20, statutory costs, 15 bps search slippage. Pre-collapse
basis is clearly labelled in every artifact.
