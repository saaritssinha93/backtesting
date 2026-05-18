# Codex Plan

Date: 2026-04-11

## V18 Entry Expansion Plan

Goal: increase the number of `v18` trades per session per day while keeping the strategy structure recognizable and controllable.

### Plan A

Best first step. Highest chance of increasing entries without distorting the strategy too much.

Changes:
- use `combined` shortlist for both long and short instead of side-specific shortlist
- extend short entry cutoff from `13:30` to `14:15`

Why:
- short side is currently starved because it only sees the `short` shortlist, which can be much smaller than combined
- afternoon short window is set till `14:30`, but strategy cutoff still stops it at `13:30`

Expected effect:
- clear increase in short trades
- moderate increase in total trades
- least invasive change

Risk:
- some drop in short quality, but still controlled because shortlist remains session-based

### Plan B

Balanced expansion after Plan A.

Changes:
- everything in Plan A
- relax short filters a bit:
  - `ADX 30 -> 28`
  - `volume_min_ratio 0.90 -> 0.80`
  - `RSI max 58 -> 60`
  - `signal_avwap_dist_atr_max 2.00 -> 2.10`
- relax long profile back toward baseline:
  - `ADX 24 -> 22`
  - `volume_min_ratio 0.90 -> 0.80`
  - `RSI min 52 -> 50`
  - `quality_score_min 4.5 -> 4.0`

Why:
- current `v18` is inheriting a safer long pack and stricter short pack
- that makes sense for quality, but not for increasing trades from a smaller shortlist universe

Expected effect:
- noticeable increase in both long and short trades
- better session utilization
- still keeps the core strategy shape intact

Risk:
- more drawdown than current `v18`
- some lower-quality entries re-enter

### Plan C

Aggressive expansion.

Changes:
- everything in Plan B
- relax V16 anti-exhaustion filters:
  - short RSI dead zone `35-40` -> maybe `36-39` or disable
  - long QS dead zone `7.5-8.0` -> narrow it
  - long AVWAP dead zone `1.0-1.5 ATR` -> narrow or disable
  - long volume exhaustion `>4x` -> raise to `>5x` or disable
- relax NIFTY context:
  - `daymove 0.35 -> 0.25`
  - BOTH-mode RS thresholds `0.75 -> 0.50`

Why:
- if shortlist-based scanning still gives too few entries after A/B, this is where the bigger unlocks are

Expected effect:
- large increase in entries
- broader day participation

Risk:
- biggest quality drop
- more false positives on weak days

## Recommendation Order

1. Run `Plan A` first.
2. If still too few trades, move to `Plan B`.
3. Use `Plan C` only if a much larger trade count is still needed after A/B.

## Strongest Recommendation

If the goal is simply "more trades per session per day" with the least regret:
- `Plan A` first

If the goal is "materially more trades, not just a small lift":
- `Plan B`

## V18 Entry Count Diagnosis Add-On

These are the additional plans to append after a numbered `Plan 1-5` diagnosis note.

### Plan 6 — Use Combined Shortlist For Both Sides

Changes:
- use the session `combined` shortlist for both long and short trades instead of using `long` and `short` shortlist buckets separately

Why:
- side-specific shortlists can be much smaller than the combined session shortlist
- this is especially restrictive on the short side
- it is the cleanest immediate way to increase candidate names without abandoning the session/date-aware shortlist design

Expected effect:
- significant short-side trade lift
- moderate total trade increase

Risk:
- some side-purity reduction
- still much safer than abandoning shortlist gating completely

### Plan 7 — Fix Time-Gating Mismatch Before Extending Windows

Changes:
- extend short entry cutoff from `13:30` to `14:15` or `14:30`
- only after that, consider wider session windows

Why:
- the afternoon scan window may run later, but the current short strategy still cuts off entries too early
- this silently suppresses valid later-session short trades

Expected effect:
- immediate increase in afternoon short entries
- very small structural change

Risk:
- later entries have less time to hit target before EOD exit
- may need tighter exit handling if pushed all the way to `14:30`

### Plan 8 — Revert Safer V18 Pack Toward Baseline

Changes:
- relax long profile toward baseline:
  - `ADX 24 -> 22`
  - `volume_min_ratio 0.90 -> 0.80`
  - `RSI min 52 -> 50`
  - `quality_score_min 4.5 -> 4.0`
- relax short profile moderately:
  - `ADX 30 -> 28`
  - `volume_min_ratio 0.90 -> 0.80`
  - `RSI max 58 -> 60`
  - `signal_avwap_dist_atr_max 2.00 -> 2.10`

Why:
- `v18` currently inherits a safer/stricter profile that is better for trade quality than trade count
- this makes more sense on the full universe than on a reduced session shortlist universe

Expected effect:
- noticeable increase on both long and short entries
- better use of shortlisted names

Risk:
- higher drawdown than the current stricter `v18`
- some lower-quality trades return

### Plan 9 — Relax Anti-Exhaustion And NIFTY Context Only After Plans 6-8

Changes:
- only after trying Plans 6-8, consider:
  - narrow or disable short RSI dead zone `35-40`
  - narrow long QS dead zone `7.5-8.0`
  - narrow long AVWAP dead zone `1.0-1.5 ATR`
  - raise long volume exhaustion threshold `>4x -> >5x`
  - relax NIFTY `daymove 0.35 -> 0.25`
  - relax BOTH-mode RS thresholds `0.75 -> 0.50`

Why:
- these are strong trade suppressors
- they can add a lot of entries, but they also change strategy character more than Plans 6-8

Expected effect:
- larger trade-count increase than Plans 6-8 alone

Risk:
- bigger quality drop
- weaker day selection

### Plan 10 — Save Shortlist Scores For Score-Tiered Entry Logic

Changes:
- enhance the shortlist history generator so each date/session stores:
  - ticker score
  - rank
  - maybe raw factor snapshot

Why:
- a score-tiered model is strong, but it needs metadata, not just ticker sets
- this enables ideas like:
  - Tier 1 = strict AVWAP logic
  - Tier 2 = relaxed AVWAP confirmation
  - Tier 3 = alternate signal family only

Expected effect:
- enables a cleaner and more controlled version of score-tiered entry expansion

Risk:
- requires shortlist-history format upgrade before strategy changes

## Added Recommendation Order

If working off a numbered `Plan 1-5` diagnosis note, the recommended continuation is:

1. `Plan 6`
2. `Plan 7`
3. `Plan 8`
4. `Plan 9`
5. `Plan 10`
