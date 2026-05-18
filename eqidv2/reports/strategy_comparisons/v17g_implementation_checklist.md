# V17G Implementation Checklist

Per-toggle code changes required to implement the v17g proposal. This is a tight engineering checklist, not a design doc — for design rationale see `v17f_new_design_proposal.md`.

## File Layout

New file: `eqidv2/avwap_combined_runner_v17g_5min.py`

Pattern: same import-and-patch shape as `avwap_combined_runner_v17f_5min.py`:
- import `avwap_combined_runner_v17f_5min` as `_v17f` and `avwap_combined_runner_v16_5min` as `_base`
- expose env-driven knobs `EQIDV17G_*` for each toggle
- patch base module functions only where v17g changes behavior
- route outputs to `outputs_v17g_5min/`

Config flag scaffold (defaults reflect v17g-final, all ON):

```python
V17G_FIX_LONG_PULLBACK_LAG     = _env_bool("EQIDV17G_FIX_LONG_PULLBACK_LAG", True)
V17G_ENABLE_LONG_REVERSAL      = _env_bool("EQIDV17G_ENABLE_LONG_REVERSAL", True)
V17G_SETUP_LEVEL_SIZING        = _env_bool("EQIDV17G_SETUP_LEVEL_SIZING", True)
V17G_CONSOLIDATE_SHORT_POCKETS = _env_bool("EQIDV17G_CONSOLIDATE_SHORT_POCKETS", True)
V17G_LONG_AVWAP_DIST_CAP_ON    = _env_bool("EQIDV17G_LONG_AVWAP_DIST_CAP_ON", True)
```

## Change 1 — Fix LONG pullback lag bug

**Target file**: `eqidv2/avwap_v11_refactored/avwap_long_strategy_v9_sweep.py` (or wherever the pullback-lag config is bound).

**Current**: `setup_lag_map["A_PULLBACK_C2_THEN_BREAK_C2_HIGH"] = 1`

**Change**: `setup_lag_map["A_PULLBACK_C2_THEN_BREAK_C2_HIGH"] = 2`

**Additional quality constraints to add** (only when `V17G_FIX_LONG_PULLBACK_LAG` is on):
- `C2.volume < C1.volume` — pullback must be lower-participation than impulse
- `C2.close >= C2.low + 0.5 * (C2.high - C2.low)` — pullback being bought (close in upper half of range)

**Implementation**: do the change inside the v17g runner via either:
- monkey-patch the lag map at import time, or
- pass an override config from `_v17g_adjust_long_cfg(cfg)`

Prefer the override-config approach for symmetry with the existing v17f short-side pattern.

**Verify**: count of `A_PULLBACK_C2_THEN_BREAK_C2_HIGH` rows in raw scan output goes from ~0 to non-zero on the same dataset.

## Change 2 — Add LONG reversal setup `B_AVWAP_RECLAIM_REVERSAL`

**Target file**: `eqidv2/avwap_v11_refactored/avwap_long_strategy_v9_sweep.py` (long-side scanner).

**New setup branch in scanner** (only emitted when `V17G_ENABLE_LONG_REVERSAL` is on):

Conditions on the candidate reclaim bar `R`:
- prior bar(s) `R-1` or `R-2` have `low <= AVWAP`
- `R.close > AVWAP`
- `R.body_atr >= 0.30` where `body_atr = abs(R.close - R.open) / atr`
- `R.close >= R.low + 0.6 * (R.high - R.low)` — close in upper 40% of range
- `StochK > StochD` at `R`
- `RSI[R] >= 37` and `RSI[R] > RSI[R-1]`
- `ADX[R] >= 22`
- `R.volume >= 0.90 * vol_20bar_avg` (shared-gate parity)
- no requirement for `EMA20 > EMA50` (reversal mode is regime-flexible)

Entry on `R+1`:
- `R+1.high > R.high + entry_buffer`
- `R+1.close > R.high + entry_buffer`

Regime restriction (applied at scan time):
- only emit if `nifty_5b_ret_pct < +0.30` at `R`
- only emit if `R.time < 13:00`

Setup row population:
- `setup_name = "B_AVWAP_RECLAIM_REVERSAL"`
- `module_tag = "REVERSAL"`
- `side = "LONG"`

**Decision deferred to backtest**: whether to keep `lag = 1` fixed or also add a `lag = -1` dynamic variant (first valid breakout bar within 3 bars). Implement both behind a sub-flag and pick during A/B.

## Change 3 — Setup-level position sizing

**Target**: position-sizing layer (likely in `_base` or in the executor that consumes scan output).

Define multiplier table:

```python
V17G_SIZE_MULTIPLIERS = {
    # Core
    "A_MOD_BREAK_C1_HIGH":               1.00,
    "A_MOD_BREAK_C1_LOW":                1.00,
    # Core-subtype
    "A_MOD_CLOSE_CONTINUATION_BREAK":    0.85,
    # Pullback (newly fixed)
    "A_PULLBACK_C2_THEN_BREAK_C2_HIGH":  0.75,
    # Event
    "B_HUGE_C1_CLOSE_RECLAIM_BREAK":     0.60,
    "B_HUGE_RED_FAILED_BOUNCE":          0.60,
    # Reversal (new)
    "B_AVWAP_RECLAIM_REVERSAL":          0.50,
}
V17G_SIZE_DEFAULT = 1.00
```

Implementation:
- in the entry-quantity computation, multiply baseline qty by `V17G_SIZE_MULTIPLIERS.get(setup_name, V17G_SIZE_DEFAULT)`
- only apply when `V17G_SETUP_LEVEL_SIZING` is on
- log the multiplier on each entry for downstream reporting

**Verify in output CSV**: new column `size_multiplier`. Per-setup average qty matches the table.

## Change 4 — Consolidate fragmented SHORT time-pocket filters

**Target**: `_v17f_apply_post_scan_filters` in `avwap_combined_runner_v17f_5min.py`. v17g overrides this function.

Replace these three time-pocket bans:
- BOTH at `10:30-11:00` (inherited from v17d)
- BOTH at `11:30-12:00` (inherited from v17d)
- BOTH at `12:15-12:45` (v17f)

With one causal rule:
- drop BOTH-mode shorts when `nifty_5b_ret_pct > -0.10` AND `stock_vol_ratio_vs_20bar_signal < 1.0`

**Keep unchanged:**
- entry cutoff `< 14:00`
- after `13:30`: `SHORT_ONLY` or `RS <= -1.0%`
- RSI dead-zone, NIFTY RS missing, ADX chop, BOTH AVWAP dead-zone, v17f ATR%, SHORT_ONLY-RS rules — all retained

**Implementation note**: the consolidation must override BOTH the v17d and v17f time-pocket clauses. Copy the v17f filter function verbatim, comment out the three time-pocket blocks, and append the new causal rule. Wrap in `if V17G_CONSOLIDATE_SHORT_POCKETS:` so the off-state cleanly falls back to the v17f stack.

**Verify**: per-day SHORT trade count in the consolidated config should be within ±5% of the fragmented config's daily mean. Per-bucket distribution (10:30-11:00, 11:30-12:00, 12:15-12:45) should now show non-zero counts that pass the new causal gate.

## Change 5 — LONG AVWAP distance cap

**Target**: long-side scan filter (likely `avwap_long_strategy_v9_sweep.py` or its config).

Add gate:
- when `V17G_LONG_AVWAP_DIST_CAP_ON`, drop LONG signals where `signal_avwap_dist_atr > 2.25`

**Implementation**: set `cfg.signal_avwap_dist_atr_max = 2.25` inside `_v17g_adjust_long_cfg(cfg)` (mirror of how v17f adjusts the short config). If no such field exists on the long config, add one and have the scanner respect it.

**Verify**: long row count drops by ~3-5%; dropped rows have `signal_avwap_dist_atr > 2.25`.

## Output Routing

Add `_v17g_runtime_dir` patch in the same shape as `_v17f_runtime_dir`:
- replace `v17f_5min`, `v17d_5min`, `v17c_5min`, `v17b_5min`, `v16_5min` with `v17g_5min` in path parts
- bind `_base.runtime_dir = _v17g_runtime_dir`
- output folder: `outputs_v17g_5min/`

## Filter-Reason Strings

Extend `_v17g_get_filter_reason(row, side)` so each new drop has a distinct reason string for forensic auditing. Required strings:
- `"v17g long cleanup: signal_avwap_dist_atr > 2.25"`
- `"v17g short cleanup: BOTH replaced pockets — NIFTY not weak AND stock not active"`

## CLI / Env Driver

Standard interface:

```bash
EQIDV17G_FIX_LONG_PULLBACK_LAG=1 \
EQIDV17G_ENABLE_LONG_REVERSAL=1 \
EQIDV17G_SETUP_LEVEL_SIZING=1 \
EQIDV17G_CONSOLIDATE_SHORT_POCKETS=1 \
EQIDV17G_LONG_AVWAP_DIST_CAP_ON=1 \
py -3.12 eqidv2/avwap_combined_runner_v17g_5min.py
```

Each toggle is independently flippable for the A/B matrix in `v17g_ab_runner.py`.

## Acceptance Smoke Tests (Pre-Backtest)

1. `EQIDV17G_FIX_LONG_PULLBACK_LAG=1`, all others off: scan output contains non-zero `A_PULLBACK_C2_THEN_BREAK_C2_HIGH` rows.
2. `EQIDV17G_ENABLE_LONG_REVERSAL=1`, all others off: scan output contains non-zero `B_AVWAP_RECLAIM_REVERSAL` rows on at least 10% of trading days.
3. `EQIDV17G_SETUP_LEVEL_SIZING=1`, all others off: every entry row carries `size_multiplier` column with values matching the table.
4. `EQIDV17G_CONSOLIDATE_SHORT_POCKETS=1`, all others off: zero rows dropped with reason containing `"weak pocket"` (the old v17d/v17f reasons should no longer fire); non-zero rows dropped with the new causal-rule reason.
5. `EQIDV17G_LONG_AVWAP_DIST_CAP_ON=1`, all others off: long-row count drops; all dropped rows have `signal_avwap_dist_atr > 2.25`.
6. All toggles ON: previous five effects all observed; output folder is `outputs_v17g_5min/`.

## Out-Of-Scope (Do Not Implement In v17g)

- huge-green-pullback re-enable
- short pullback re-enable
- regime router
- quality-score model
- new setups beyond `B_AVWAP_RECLAIM_REVERSAL`
- entry buffer change
- volume gate change
- exit / SL / target parameter change
- early-entry mode

These are tracked in `v17h_parked_items.md`.
