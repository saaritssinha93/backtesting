# A_MOD_CLOSE_CONTINUATION_BREAK (LONG) — REDESIGNED_SETUP_IDEAS

_Generated 2026-07-03. Research-only. All versions keep the card's core intent:
"moderate-impulse bar closing near its high, breaking the prior bar's high, above session
VWAP, RS-positive, on volume — join the continuation on the next tick"._

Common base (every version):
- entry trigger: 5-min bar with `close > open`, `close_loc >= 0.75`, `close > prev_bar_high`,
  range in [0.60, 2.20] x ATR, above causal session VWAP, `rs_pct > 0` vs NIFTYBEES,
  `vol_ratio >= 1.4` (common floor 1.5 binds), liquidity floors (px >= 80, bar >= Rs1M,
  day >= Rs20M after 10:00), quality >= 6.8; scan window 10:00–14:30 (earliest causal bar).
- entry: next 1-min open + 15 bps/leg; exits: first-touch SL/target on 1-min, else EOD 15:20;
  statutory NSE costs; family dedupe one-per-ticker-day; 20-position overlay.
- deployment path if approved: flag-gated detector extension (S9/DOC5D pattern) — the
  production collapse is NOT modified.

| id | version | added logic | why it makes sense |
|---|---|---|---|
| R1 | uncollapsed card | none | true baseline of the card across ALL regimes (previously 96.8% BEAR-only) |
| R2 | not-bear | `regime != BEAR` | continuation longs shouldn't fight index beta — the habitat the collapse stole |
| R2b | bull/trend | `regime in {BULL, TREND}` | strictest tape alignment |
| R3 | first-break | `x_first_break_of_day == 1` | trade the FIRST qualifying break per ticker-day; later breaks are chases |
| R4 | fresh-break | `x_fresh_break == 1` (prior bar had NOT broken its own prior high) | anti-chase: join the start of the leg, not bar #3 of a run |
| R5 | pullback-then-break | `x_prev_pullback == 1` (prior bar red or close_loc<0.5) | two-stage spring: rest -> break, the classic continuation shape |
| R6 | morning | guard `max_slot 11:30` | continuation follow-through is front-loaded; avoid lunch chop |
| R7 | aligned thrust | not-bear + `vol_ratio >= q70` + fresh | confluence version: tape + participation + freshness |
| R8 | ranked top-1 | guard `top_n 1, max_slot 12:30` | strongest signal per slot only — overtrading guard |
| R23 | R2+R3 | not-bear + first break | combined |
| R24 | R2+R4 | not-bear + fresh break | combined |
| R26 | R2+R6 | not-bear + morning | combined |
| R2b4 | R2b+R4 | bull/trend + fresh | combined |
| R35 | R3+R5 | first break following a pullback bar | tightest two-stage |

Each version is scored at exit anchors SL0.70/T1.50 (production), SL1.00/T2.00 (v1 grid
winner), SL0.50/T1.00 (scalp) on FIT/VAL first; survivors go through the standard sweep ->
TPE -> full-TRAIN band [1.30, 1.80] -> single TEST scoring -> domination/robustness gate.
SL/target is re-derived from the 1-min MFE/MAE study (`mfe_mae_study.json`) rather than
assumed.

Explicitly out of scope (would break the setup's identity or the pipeline's realism):
trailing/breakeven stops (not supported by the resolver), shorting the failure, multi-day
holds, universe changes beyond the existing liquidity floors.
