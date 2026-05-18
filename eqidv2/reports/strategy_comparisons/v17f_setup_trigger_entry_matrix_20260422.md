# V17F Setup Trigger And Entry Matrix - 2026-04-22

## Scope

This report maps the resolved `v17f` setup logic into a setup-by-setup matrix for:
- signal trigger conditions
- signal entry conditions
- indicator and structure gates that must already be true
- post-scan cleanup layers that still remove otherwise valid raw signals

Resolved config basis:
- start from `apply_live_parity_profile(default_short_config(), default_long_config_v9())`
- then apply `_v17f_adjust_short_cfg()` on the short side

Legend:
- `C1` = impulse or signal bar
- `C2` = next bar after `C1`
- `entry buffer` = `max(0.05, 0.02% of reference price)`
- `active` = reachable in current `v17f`
- `dormant` = code path exists but current lag / implementation makes it practically unreachable
- `disabled` = explicitly filtered out by inherited `v17d` / `v17f` cleanup

## Shared Gates

| Layer | LONG in `v17f` | SHORT in `v17f` |
|---|---|---|
| Signal windows | `09:15-12:00`, `12:00-13:00` | `09:15-12:00`, `12:00-14:00` |
| Entry cutoff | No extra cutoff beyond long windows | Explicit cutoff at `< 14:00` |
| Moderate impulse | Green candle; body `0.30-1.00 ATR`; close near high | Red candle; body `0.25-1.00 ATR`; close near low |
| Huge impulse | Body `>= 1.60 ATR` or range `>= 2.00 ATR` | Body `>= 1.60 ATR` or range `>= 2.00 ATR` |
| Trend gate | `ADX >= 24`, rising 2 bars, slope `>= 0.50`; `RSI >= 52`, rising 2 bars; `StochK 15-95`, rising, `K > D`; `EMA20 > EMA50`; close above `EMA20` and `AVWAP` | On trend days: `ADX >= 25`, rising 2 bars, slope `>= 0.40`; `RSI <= 58`, falling 2 bars; `StochK <= 90`, falling, `K < D`; `EMA20 < EMA50`; close below `EMA20` and `AVWAP` |
| Reversal gate | Not used | On reversal days: `high >= AVWAP` and `close < AVWAP`; body `>= 0.30 ATR`; `K < D`; `RSI <= 63` |
| Volume gate | Impulse bar volume `>= 0.90x` 20-bar average | Impulse bar volume `>= 0.90x` 20-bar average |
| Liquidity sweep | Required before entry | Required only on reversal-mode days |
| AVWAP evidence before entry | Touch-support (`low <= AVWAP` and `close > AVWAP`) or at least 1 consecutive close above `AVWAP`; entry close must be at least `0.25 ATR` above `AVWAP` | Touch-fail (`high >= AVWAP` and `close < AVWAP`) or at least 1 consecutive close below `AVWAP`; entry close must be at least `0.25 ATR` below `AVWAP` |
| Quality gates | `quality_score >= 4.5`; `ATR% >= 0.25%` | No explicit quality-score floor; short signal AVWAP distance cap `<= 2.10 ATR` |
| Entry confirmation | Entry bar close must confirm above trigger | Entry bar close must confirm below trigger |

## Setup Matrix

| Side | Setup | Status in `v17f` | Signal trigger logic | Signal entry logic | Indicator / structure notes |
|---|---|---|---|---|---|
| LONG | `A_MOD_BREAK_C1_HIGH` | active | `C1` must be a valid moderate green impulse. Trigger is `C1 high + entry buffer`. | Enter on configured lag `1` (`C1+1`) if that bar's high breaks the trigger and the bar closes above the trigger. | Uses all shared long gates. This is still the main long continuation setup. |
| LONG | `A_MOD_CLOSE_CONTINUATION_BREAK` | active | Same moderate green `C1`. Trigger is `C1 close + entry buffer`. | Enter on configured lag `2` (`C1+2`) if that bar's high breaks the trigger and the bar closes above the trigger. | Faster continuation variant than strict `C1` high breakout. Uses the same shared long gates. |
| LONG | `A_PULLBACK_C2_THEN_BREAK_C2_HIGH` | dormant | After a valid moderate green `C1`, `C2` must be a small red pullback with body `<= 0.20 ATR` and close above `AVWAP`. Trigger is `C2 high + entry buffer`. | Configured lag is `1`, so the code checks breakout on `C2` itself rather than `C3`. That makes the current comparison effectively unreachable because it asks whether `C2 high > C2 high + buffer`. | Code path exists and is enabled in config, but it is not practically participating in current `v17f`. |
| LONG | `B_HUGE_GREEN_PULLBACK_HOLD_THEN_BREAK` | disabled | Huge green `C1`; next `1-3` bars must include at least one small red pullback and must hold above `C1` midpoint or above `AVWAP`. Trigger is `pullback-window high + entry buffer`. | Configured lag is `999`, so there is no practical fixed-bar entry in `v17f`. | Present in code, intentionally suppressed by lag configuration. |
| LONG | `B_HUGE_C1_CLOSE_RECLAIM_BREAK` | active | Huge green `C1`. Trigger is `C1 close + entry buffer`. Later bar must stay structurally above `AVWAP` and reclaim through the trigger. | Configured reclaim lag is `2`, so `v17f` checks from `C1+2`; breakout bar must close above the trigger. | This is the active huge-long branch in `v17f` after the hold-then-break path is effectively off. |
| SHORT | `A_MOD_BREAK_C1_LOW` | active | `C1` must be a valid moderate red impulse. Trigger is `C1 low - entry buffer`. | Enter on configured lag `1` (`C1+1`) if that bar's low breaks the trigger, the bar closes below the trigger, entry time is before `14:00`, and short AVWAP rejection and distance tests pass. | This is the main live short continuation setup in `v17f`. |
| SHORT | `A_PULLBACK_C2_THEN_BREAK_C2_LOW` | disabled | Base logic still exists: after a valid moderate red `C1`, `C2` must be a small green pullback with body `<= 0.20 ATR` and close below `AVWAP`. Trigger is `C2 low - entry buffer`. | Base scanner would enter on configured lag `2` (`C1+2`) if that bar's low breaks the trigger and the close confirms below it. | Inherited `v17d` cleanup removes this setup entirely, so it does not survive in final `v17f` output. |
| SHORT | `B_HUGE_RED_FAILED_BOUNCE` | active but rare | Huge red `C1`; next `1-3` bars must include at least one small green bounce. If AVWAP rule is on, bounce window must also show touch-fail evidence (`high >= AVWAP` and `close < AVWAP`). Trigger is `bounce-window low - entry buffer`. | Configured lag is `-1`, so entry is dynamic: first later bar that breaks below the bounce low, closes below the trigger, stays below `AVWAP`, and passes short AVWAP distance and rejection checks. | Code-enabled in `v17f`; it appears much less often than `A_MOD_BREAK_C1_LOW`. |

## Post-Scan Overlay On Top Of Raw Setup Logic

| Layer | Side | Rule in current `v17f` |
|---|---|---|
| Base `v16` cleanup | SHORT | Drop RSI dead-zone shorts in `[30, 40)` after `v17b` lowered the floor from `35` to `30`. |
| Base `v16` cleanup | LONG | Drop long quality-score dead zone `(7.6, 7.9]`, drop long `QS > 12`, drop long AVWAP dead zone `[1.0, 1.5) ATR`, and drop late volume-exhausted longs when `entry_bar_vol_ratio > 5.0x` after minimum bars-from-open. |
| `v17c` / `v17d` inherited long anti-chase | LONG | Remove late `A_MOD_BREAK_C1_HIGH` longs when `bars_from_open >= 12` and `entry_bar_vol_ratio >= 3.5x`. |
| `v17d` cleanup | SHORT | Remove `A_PULLBACK_C2_THEN_BREAK_C2_LOW`; drop missing NIFTY RS; drop `RS <= -2.0%`; drop `ADX` chop `[20, 25)` for `SHORT_ONLY`; drop exhausted `ADX >= 50`; drop BOTH-mode AVWAP dead zone `[0.50, 1.00) ATR`; drop BOTH weak pockets `10:30-11:00` and `11:30-12:00`; after `13:30`, allow only `SHORT_ONLY` or `RS <= -1.0%`. |
| `v17f` extra cleanup | SHORT | Drop `SHORT_ONLY` when `RS > -0.25%`; drop `SHORT_ONLY` when `ATR% >= 0.70%`; drop BOTH-mode shorts in `12:15-12:45`. |

## Bottom Line

The live `v17f` setup set is mostly:
- LONG: `A_MOD_BREAK_C1_HIGH`, `A_MOD_CLOSE_CONTINUATION_BREAK`, `B_HUGE_C1_CLOSE_RECLAIM_BREAK`
- SHORT: `A_MOD_BREAK_C1_LOW`, plus occasional `B_HUGE_RED_FAILED_BOUNCE`

The two setups that look available on paper but are not meaningfully live in current `v17f` are:
- `A_PULLBACK_C2_THEN_BREAK_C2_HIGH` on the long side, because the current lag wiring makes the breakout test self-referential
- `A_PULLBACK_C2_THEN_BREAK_C2_LOW` on the short side, because `v17d` cleanup removes it post-scan

## Source Files

- `eqidv2/avwap_combined_runner_v17f_5min.py`
- `eqidv2/avwap_combined_runner_v17d_5min.py`
- `eqidv2/avwap_combined_runner_v17c_5min.py`
- `eqidv2/avwap_combined_runner_v17b_5min.py`
- `eqidv2/avwap_combined_runner_v16_5min.py`
- `eqidv2/avwap_v11_refactored/avwap_long_strategy_v9_sweep.py`
- `eqidv2/avwap_v11_refactored/avwap_short_strategy_v11.py`
- `eqidv2/avwap_v11_refactored/avwap_common_v11.py`
