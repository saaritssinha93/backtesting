# Phase 1 — Setup Early-Emit Map

Date: 2026-04-22
Source of truth:
- Short scanner: `eqidv2/avwap_v11_refactored/avwap_short_strategy_v11.py::scan_one_day` (lines 345–704).
- Long scanner: `eqidv2/avwap_v11_refactored/avwap_long_strategy_v9_sweep.py::scan_one_day` (lines 319–800).
- Lag table: `eqidv2/avwap_combined_runner_v16_5min.py` lines 223–230.

Convention: bar indices use scanner-local indexing where `i` = C1 bar. `date` stamps in parquet are CLOSE-stamped (bar `[HH:MM-5, HH:MM)` is stamped `HH:MM`). `5min` = 5 minutes.

Legend:
- **trigger_iso**: close stamp of the bar where SE emits CANDIDATE.
- **Emit preconds**: MUST be checkable at trigger_iso close using bars ≤ trigger_iso only.
- **break_level**: the exact price the entry bar must cross (close-confirmed).
- **entry_window**: [start, end) expressed as offsets from trigger_iso.
- **DE replay gates**: the scanner gates DE re-verifies at entry-bar close (via v16_runner replay).

---

## 1. A_MOD_BREAK_C1_HIGH  (LONG, lag=1)

| Field | Value |
|---|---|
| Source | long scanner lines 518–557 |
| trigger_iso anchor | **C1 close** |
| Emit preconds (checkable at C1 close) | 1. `classify_green_impulse(C1) == "MODERATE"` <br> 2. `in_signal_window(C1.date)` <br> 3. `ATR15(C1) > 0`, `close > 0` <br> 4. `atr_pct ≥ atr_pct_min` (if cfg.use_atr_pct_filter) <br> 5. `volume_filter_pass(C1)` <br> 6. `_trend_filter_long(df, i, C1)` <br> 7. `has_recent_liquidity_sweep(df, i, "LONG")` <br> 8. `not avwap_no_trade_zone_block(C1, impulse, has_sweep)` |
| break_level | `C1.high + entry_buffer(C1.high)` |
| entry_window | `[trigger_iso + 5min, trigger_iso + 10min)` — exactly the C2 bar |
| CONFIRMED condition | Entry bar high > break_level AND close confirm (close > break_level) AND signal-window at entry AND bars_left_ok AND `avwap_support_pass(df, i, entry_idx)` AND `_make_trade` quality gates (avwap_dist_atr, ema_gap_atr, qscore, max_entry_slip) |
| DE replay gates | Same as CONFIRMED — runner does it all |
| EXPIRED | C2 close reached without `high > break_level` |
| INVALIDATED | C2.low < (C1.high − 1.0*ATR) — structural failure (optional; can be treated as EXPIRED in Phase 2) |

## 2. A_PULLBACK_C2_THEN_BREAK_C2_HIGH  (LONG, lag=2)

| Field | Value |
|---|---|
| Source | long scanner lines 598–650 |
| trigger_iso anchor | **C2 close** (NOT C1) — requires pullback shape known only at C2 close |
| Emit preconds (checkable at C2 close) | All A_MOD_* preconds at C1 PLUS: <br> 1. `cfg.enable_setup_a_pullback_c2_break == True` (gate) <br> 2. `C2.close < C2.open` (small red) <br> 3. `abs(C2.close - C2.open) ≤ cfg.small_counter_max_atr * ATR15(C2)` <br> 4. `C2.close > C2.AVWAP` (above AVWAP) <br> 5. `i + 2 < len(df_day)` (room for entry) |
| break_level | `C2.high + entry_buffer(C2.high)` |
| entry_window | `[trigger_iso + 5min, trigger_iso + 10min)` — exactly the C3 bar |
| CONFIRMED condition | C3 high > break_level AND close confirm AND signal-window AND bars_left AND `avwap_support_pass` AND `_make_trade` quality gates |
| DE replay gates | Same (runner replay) |
| EXPIRED | C3 close reached without break |

## 3. A_MOD_CLOSE_CONTINUATION_BREAK  (LONG, lag=cfg.lag_bars_long_a_close_continuation_break; typically 1–2)

| Field | Value |
|---|---|
| Source | long scanner lines 559–597 |
| trigger_iso anchor | **C1 close** (same C1 impulse as A_MOD_BREAK_C1_HIGH, different break_level) |
| Emit preconds | Same as A_MOD_BREAK_C1_HIGH preconds PLUS `cfg.enable_setup_a_close_continuation_break == True` |
| break_level | `C1.close + entry_buffer(C1.close)` |
| entry_window | `[trigger_iso + 5min, trigger_iso + (lag+1)*5min)` — single bar |
| CONFIRMED condition | Entry bar high > break_level AND close confirm AND signal-window AND bars_left AND `avwap_support_pass` AND `_make_trade` quality gates |
| DE replay gates | Same |
| Note | Must NOT emit this candidate if A_MOD_BREAK_C1_HIGH already produced a CONFIRMED on the same C1 (scanner code does `continue` after confirming; SE must mirror via dedup — two CANDIDATE rows are allowed but executor must not double-fire. DE naturally handles via scanner's own dedup.) |

## 4. B_HUGE_PULLBACK_HOLD_BREAK  (LONG, lag=999 → effectively disabled in V16)

| Field | Value |
|---|---|
| Source | long scanner lines 658–743 |
| Status | **Skip in Phase 2**. `LONG_LAG_BARS_B_HUGE_PULLBACK_HOLD_BREAK = 999` means entry_idx computes to `i + 999` which is always past len(df_day) → scanner never emits. No point building early-emit for a dead setup. |
| Re-enable trigger | If runner constant flips back below ~5, revisit. |

## 5. B_HUGE_C1_CLOSE_RECLAIM_BREAK  (LONG, lag=2)

| Field | Value |
|---|---|
| Source | long scanner lines 745–791 |
| trigger_iso anchor | **C1 close** |
| Emit preconds | 1. `classify_green_impulse(C1) == "HUGE"` <br> 2. `in_signal_window(C1.date)` <br> 3. `ATR15(C1) > 0` <br> 4. `volume_filter_pass(C1)` <br> 5. `_trend_filter_long(df, i, C1)` <br> 6. `has_recent_liquidity_sweep(df, i, "LONG")` <br> 7. `not avwap_no_trade_zone_block` <br> 8. `cfg.enable_setup_b_huge_c1_close_reclaim_break == True` |
| break_level | `C1.close + entry_buffer(C1.close)` |
| entry_window | `[trigger_iso + 5min, trigger_iso + 15min)` — lag=2, single bar at C3 (per scanner: `j_fixed = i + lag_reclaim`; dynamic mode uses `range(i+1, i+7)` but V16 runner sets fixed lag=2) |
| CONFIRMED condition | Entry bar close > AVWAP AND high > break_level AND close confirm AND bars_left AND `avwap_support_pass` AND `_make_trade` quality gates |
| DE replay gates | Same |
| Empirical ref | Pool `pending_signals_2026-04-22_v16_5min.json`: trigger=09:35, source_slot=09:45, entry=09:45 — exactly trigger + 10min = lag*5min |

## 6. A_MOD_BREAK_C1_LOW  (SHORT, lag=1)

| Field | Value |
|---|---|
| Source | short scanner lines 560–583 |
| trigger_iso anchor | **C1 close** |
| Emit preconds (checkable at C1 close) | 1. `classify_red_impulse(C1) == "MODERATE"` <br> 2. `in_signal_window(C1.date)` <br> 3. `market_regime_pass(C1.date, "SHORT")` <br> 4. `ATR15(C1) > 0`, `close > 0` <br> 5. `atr_pct ≥ atr_pct_min` (if enabled) <br> 6. `volume_filter_pass(C1)` <br> 7. `_trend_filter_short` OR `_reversal_filter_short` (per day_mode) <br> 8. If `cfg.enable_liquidity_sweep_filter` or reversal_requires_sweep: `has_recent_liquidity_sweep(..., "SHORT")` <br> 9. `not avwap_no_trade_zone_block` <br> 10. `signal_avwap_dist_atr ≤ cfg.signal_avwap_dist_atr_max` |
| break_level | `C1.low - entry_buffer(C1.low)` |
| entry_window | `[trigger_iso + 5min, trigger_iso + 10min)` — the C2 bar |
| CONFIRMED condition | Entry bar low < break_level AND close confirm (close < break_level) AND signal-window at entry AND `_entry_time_ok` AND bars_left AND `avwap_rejection_pass(df, i, entry_idx)` AND `avwap_distance_pass(df, entry_idx)` |
| DE replay gates | Same (runner replay) |
| EXPIRED | C2 close reached without break |

## 7. A_PULLBACK_C2_THEN_BREAK_C2_LOW  (SHORT, lag=2)

| Field | Value |
|---|---|
| Source | short scanner lines 585–618 |
| trigger_iso anchor | **C2 close** |
| Emit preconds (checkable at C2 close) | All A_MOD preconds at C1 PLUS: <br> 1. `C2.close > C2.open` (small green) <br> 2. `abs(C2.close - C2.open) ≤ small_counter_max_atr * ATR15(C2)` <br> 3. `C2.close < C2.AVWAP` <br> 4. `i + 2 < len(df_day)` |
| break_level | `C2.low - entry_buffer(C2.low)` |
| entry_window | `[trigger_iso + 5min, trigger_iso + 10min)` — the C3 bar |
| CONFIRMED condition | C3 low < break_level AND close confirm AND signal-window AND `_entry_time_ok` AND bars_left AND `avwap_rejection_pass` AND `avwap_distance_pass` |
| DE replay gates | Same |

## 8. B_HUGE_RED_FAILED_BOUNCE  (SHORT, lag=-1 → dynamic)

| Field | Value |
|---|---|
| Source | short scanner lines 626–701 |
| Canonical name | `B_HUGE_FAILED_BOUNCE` (constant) / `B_HUGE_RED_FAILED_BOUNCE` (trade.setup field) |
| trigger_iso anchor | **bounce_end close = C4 = (i+3)** — bounce shape requires 3 bars post-C1 |
| Emit preconds (checkable at bounce_end close = i+3) | All A_MOD preconds at C1 PLUS `impulse == "HUGE"` AND: <br> 1. `cfg.enable_setup_b_huge_failed_bounce == True` AND `PACK2_ENABLE_SHORT_SETUP_B_HUGE_FAILED_BOUNCE == True` <br> 2. At least one bar in bounce window (i+1..i+3) is small green (`body ≤ small_counter_max_atr * ATR15`) <br> 3. If `cfg.require_avwap_rule and cfg.avwap_touch`: some bar j in bounce has `high ≥ AVWAP(j)` AND `close(j) < AVWAP(j)` (touch-fail) <br> 4. `bounce_low = min(bounce.low)` is finite |
| break_level | `bounce_low - entry_buffer(bounce_low)` |
| entry_window | `[trigger_iso + 5min, trigger_iso + 40min)` — dynamic; scanner walks j ∈ [bounce_end+1, len(df_day)). Hard cap = 7 bars post-trigger (~session cutoff coverage). |
| CONFIRMED condition (any bar in window) | `close(j) < AVWAP(j)` (else break the walk — not just continue) AND `low(j) < break_level` AND close confirm AND `_entry_time_ok` AND bars_left AND `avwap_distance_pass(j)` AND `avwap_rejection_pass(i, j)` |
| DE replay gates | Same |
| Abort condition mid-window | `close(j) ≥ AVWAP(j)` → INVALIDATED (scanner `break`s on this) |

---

## Cross-cutting notes

### N1. Setup→lag table (verified)
```
SHORT:
  A_MOD_BREAK_C1_LOW                  lag=1
  A_PULLBACK_C2_THEN_BREAK_C2_LOW     lag=2
  B_HUGE_FAILED_BOUNCE                lag=-1 (dynamic)
LONG:
  A_MOD_BREAK_C1_HIGH                 lag=1
  A_PULLBACK_C2_THEN_BREAK_C2_HIGH    lag=2
  A_MOD_CLOSE_CONTINUATION_BREAK      lag=cfg (typically 1 or 2)
  B_HUGE_PULLBACK_HOLD_BREAK          lag=999 (DEAD)
  B_HUGE_C1_CLOSE_RECLAIM_BREAK       lag=2
```

### N2. trigger_iso ≠ C1 for three setups
- A_PULLBACK (both sides): trigger_iso = C2 close.
- B_HUGE_FAILED_BOUNCE: trigger_iso = bounce_end close (C4 = i+3).
- B_HUGE_PULLBACK_HOLD_BREAK: trigger_iso = pull_end close (C4 = i+3). (But setup is dead.)

### N3. entry_time computation from trigger_iso
For fixed-lag setups in this map:
- A_MOD_BREAK_C1_*: entry = trigger + 1*5min
- A_PULLBACK_*: entry = trigger + 1*5min (trigger is C2, entry is C3)
- A_MOD_CLOSE_CONTINUATION_BREAK: entry = trigger + lag*5min
- B_HUGE_C1_CLOSE_RECLAIM_BREAK: entry = trigger + 2*5min
- B_HUGE_FAILED_BOUNCE: entry = trigger + k*5min, k ∈ [1, 7]

Note: this differs from today's source_slot convention (source_slot = entry close stamp). Phase 2 must rename the pool field to `trigger_iso` and add a computed `expected_entry_iso` field.

### N4. Break-level is "known at trigger_iso" by construction
Because trigger_iso was chosen as the earliest bar where ALL preconditions (including the C2 pullback shape or bounce_low) are determinable, `break_level` is always a deterministic function of bars ≤ trigger_iso.

### N5. Runner replay is the only way
DE does NOT implement scanner gates directly. Phase 4 DE takes the candidate's `(ticker, side, trigger_iso, setup, break_level)`, runs the same `_scan_partition_worker` path against the entry bar, and checks if the scanner's output contains a matching trade row. If yes → CONFIRMED. If no → EXPIRED (no break or gate rejected). Distinguishing "no break" from "gate rejected" requires one extra signal from the scanner (Phase 4 design point; may need scanner instrumentation).

### N6. Scanner post-filters applied AFTER scan
The runner also applies `_apply_v16_post_scan_filters`, `apply_live_parity_profile`, and NIFTY RS filtering. These run on the trade DataFrame AFTER scanning. For DE parity, the scan result MUST pass through the same post-filter chain before confirming. This is a Phase 4 wiring detail.

---

## Open questions deferred to Phase 2

1. **Q2.1**: For setups where trigger_iso = C1 but break_level requires an additional bar shape check (not applicable to current setups but may re-emerge if new setups are added), do we back-shift the emit to the earliest determinable bar or prohibit ambiguous setups? → Current setups are clean; defer.
2. **Q2.2**: When C1 triggers BOTH A_MOD_BREAK and A_MOD_CLOSE_CONTINUATION at different break_levels, do we emit two candidate rows with same trigger_iso + different `setup` field? → YES (different signal_id via setup field). Executor treats them independently; in practice A_MOD_BREAK fires first and the `continue` in the scanner prevents A_MOD_CLOSE from being evaluated.
3. **Q2.3**: Does `prepare_session_bars_for_scan` (applied before `scan_all_days_for_ticker_prepared`) alter `date`/`close` in ways that affect trigger_iso alignment? → Need to verify in Phase 2 that `df_prepared.date` matches parquet `date` 1:1. If it drops bars, SE emit timing shifts.
4. **Q2.4**: `day_mode` selection (`select_day_mode`) is per-day and uses `prev_close`. This is known at C1 close. Confirm prev_close is populated correctly in live mode via `_load_ticker_intrabar_cache` / fetcher history.

---

## Phase 1 deliverables (this doc)

- Setup → (trigger_iso, preconds, break_level, entry_window) mapping for all 8 configured setups.
- 1 setup flagged as dead and skipped (B_HUGE_PULLBACK_HOLD_BREAK).
- Cross-cutting notes on lag, trigger_iso placement, scanner-replay requirement, post-filter requirement.

**Phase 2 unblock**: this map is sufficient to start coding the SE early-emit path (write CANDIDATE with fields `ticker, side, setup, trigger_iso, break_level, entry_window_start_iso, entry_window_end_iso`).

**Phase 2 entry criterion**: confirm Q2.3 and Q2.4 before writing code.
