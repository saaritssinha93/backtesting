# A_MOD_BREAK_C1_HIGH — Redesigned Setup Ideas

_Generated 2026-07-03. Each redesign keeps the core intent (RS-positive moderate-impulse
continuation above VWAP) and changes ONE structural element, grounded in a measured failure.
All evaluated by the validated 1-minute path engine (`scripts/path_engine.py`)._

## R1 — Structural base (carried from campaign 2)

First signal per (ticker, day) + break must be a genuine 20-bar high.
*Why:* dedupe doubled book PF; is_20bar_high was the only VAL-stable term. All redesigns build on R1.

## R2 — Confirmation entry (Block B)

Stop-buy above the 5-min signal-bar high, valid K∈{5,10,15,30} minutes; gap-aware fill
max(bar open, level) + 15bps.
*Why:* never-confirmed signals (10%) have median EOD −0.93% and MFE 0.11 — pure losers that a
signal-bar feature cannot identify. This entry both skips them and enters on proof of demand.
*Cost:* pays up vs next-open; fewer fills.

## R3 — Retest-limit entry (Block C)

Resting limit at signal_close − depth·ATR (depth ∈ {0.15, 0.30, 0.50}), armed ≥1 min after
signal, valid K∈{15,30,60} min; conservative fill AT the level + slippage.
*Why:* 54% of signals retest ≥0.25% within 30m — half the population is buyable ~0.3% cheaper,
directly attacking the cost-toll failure.
*Risk:* adverse selection (fills concentrate in weaker signals) — measured, not assumed.

## R4 — MFE/MAE-matched brackets (Block A)

SL grid extended DOWN to 0.35-0.55% (matching MAE-before-MFE p25-p50), targets 0.8-1.25%
(matching MFE p50), instead of the noise-level 0.70/1.00.

## R5 — Non-bracket exits (Block A add-ons)

Time-cap exits (60/120/180 min — EOD drift is zero, so cap the hold), breakeven jump
(SL→entry after +0.3/0.5%), trailing stop (0.6/0.9/1.25% off the running high, with and
without a far target).

## R6 — Session-window split (Block D)

Same best entry/exit but restricted: ≤11:05 / 11:05-13:30 / ≥13:00 / exclude last 30m /
exclude first 45m. Morning book was cleaner in both campaigns.

## R7 — Context masks (Block E)

Single defensible masks on the best family: vol_ratio ≥2.2/3.0, break_margin ≥0.1 ATR,
gap direction, day_ret ≥1%, EMA stack, VWAP-hold ≥6 bars, RSI ≥60, ADX ≥25, pre-volume ≥1.4,
range compression, near-VWAP ≤2 ATR, regime ≠ BEAR, quality ≥120.

## R8 — Crowding/risk guards (Block F)

top-N per slot (by vwap_dist or vol_ratio), max trades/day 10/20/40, daily loss stop 4k,
max open 10.

## R9 — Combinations (Block G)

Only pieces that individually hold FIT AND VAL vs the best family are combined (max 2 add-ons).

Acceptance for every candidate: full-TRAIN PF 1.30-1.80 (n≥60), TEST once, TEST PF>1.40 with
positive net (n≥15), domination + day-p checks, avg-loss sanity.
