# V17Q 5-Min Strategy Map

Generated: 2026-04-27

Primary runner: `avwap_combined_runner_v17q_5min.py`

Latest referenced run:
`C:\TradingData\eqidv2\outputs_v17q_5min\avwap_combined_runner_20260427_172331.txt`

Latest referenced trade CSV:
`C:\TradingData\eqidv2\outputs_v17q_5min\avwap_longshort_trades_v16_5min_ALL_DAYS_20260427_172331.csv`

## 1. What v17q Is

v17q is the "honesty layer" on top of the v17p strategy stack.

It inherits the full signal/filter stack:

```text
v17q
  -> v17p
    -> v17o
      -> v17n
        -> v17m
          -> v17k
            -> v17j
              -> v17i
                -> v17h
                  -> v17g
                    -> v17f
                      -> v17d
                        -> v17c
                          -> v17b
                            -> v16
```

The important distinction:

- v17p is the optimistic/high-performing production-style filter stack.
- v17q keeps that stack but tightens execution realism.
- v17q promotes several anti-lookahead fixes by default.
- v17q latest full-universe result is much less profitable than v17p because it exposes same-entry-bar stops, removes 5-minute fallback exits, and disables close-confirm lookahead.

The source banner still says "Day 0 bootstrap / clone of v17p", but the current defaults are no longer a pure clone. The promoted v17q fixes are active by default.

## 2. High-Level Pipeline

```text
1. Load 5-minute signal parquet data.
2. Build SHORT and LONG strategy configs through the inherited stack.
3. Scan all tickers for 5-minute signals.
4. Apply NIFTY intraday context and relative-strength filters.
5. Apply v16 anti-exhaustion filters.
6. Apply v17b/v17c/v17d/v17f/v17g/v17h/v17i/v17j/v17k/v17m/v17n/v17o/v17p post-scan filters.
7. Apply v17p one-ticker-per-day and sizing tags.
8. Apply v17q hardened one-ticker-per-day invariant.
9. Re-resolve all exits on 1-minute data.
10. Apply v17q entry-bar-aware exit correction.
11. Drop unresolved 5-minute fallback exits.
12. Run strict post-run audit.
13. Write trades, reports, charts, and console log into outputs_v17q_5min.
```

## 3. Data And Output

Signal data:

```text
C:\TradingData\eqidv2\stocks_indicators_5min_eq_live2
```

Exit data:

```text
C:\TradingData\eqidv2\stocks_indicators_1min_eq
```

Latest run coverage:

```text
2025-06-02 09:20 IST -> 2026-04-24 15:30 IST
5-minute parquet files: 1044
1-minute parquet files: 1041
```

Output folder:

```text
C:\TradingData\eqidv2\outputs_v17q_5min
```

## 4. Capital, Costs, And Exit Model

Per-trade margin:

```text
SHORT: Rs.20,000
LONG : Rs.20,000
```

Leverage:

```text
SHORT: 5.0x
LONG : 5.0x
```

Notional per trade:

```text
SHORT: Rs.100,000
LONG : Rs.100,000
```

Costs:

```text
Slippage  : 5 bps
Commission: 3 bps
Round-trip cost in pnl_pct: approximately 0.80% levered-margin ROI
Stop extra slippage: 3 bps on stop exits
```

Latest runtime target/SL:

```text
SHORT SL  = 0.75%
SHORT TGT = 0.80%
LONG  SL  = 0.75%
LONG  TGT = 0.80%
```

Base v16 constants still show 1.00% targets, but v17i/v17m override both sides to 0.80% in the current inherited stack. The latest v17q log confirms the active target override:

```text
[TEST] Target override active: SHORT=0.8%, LONG=0.8%
```

Final outcomes:

```text
TARGET / SL / EOD only
No BE
No partial exits
No trailing exits
EOD cutoff: 15:20
```

Primary reported path:

```text
Pessimistic stressed path
Stops include extra stop-fill slippage
```

## 5. Signal Windows And Lags

Latest runtime windows:

```text
SHORT: 09:15-12:00, 12:00-14:00
LONG : 09:15-12:00, 12:00-13:00
```

Latest runtime lags:

```text
SHORT:
  A_MOD      = 1
  A_PULLBACK = 2
  B_HUGE     = -1 legacy/dynamic

LONG:
  A_MOD      = 1
  A_CONT     = 2
  A_PULLBACK = 1 in base, fixed by v17g where applicable
  B_HUGE_HOLD= 999
  B_RECLAIM  = 2
```

v17q F11 disables `require_entry_close_confirm`, so a signal is not filtered using the close of the same bar after the intrabar trigger already filled.

v17q F14 can floor zero-lag config attributes to at least 1, but it is off by default:

```text
EQIDV17Q_FLOOR_ZERO_LAG=False
```

The latest run still had 40 rows with signal-to-entry lag below 5 minutes, warned by audit.

## 6. NIFTY Context

NIFTY context sources:

```text
NIFTYBEES
NIFTY50
NIFTY_50
NIFTY
```

Core NIFTY settings:

```text
OR end / confirm time: 09:20 in latest live-parity chain
Minimum day move: 0.35%
Relative-strength lookback: 4 bars
Base RS threshold: 0.20%
BOTH-mode LONG threshold: +0.75%
BOTH-mode SHORT threshold: -0.50% in latest v17q runtime
```

Latest run NIFTY map:

```text
Source=NIFTYBEES
Mapped bars=16670
LONG_ONLY=3568
SHORT_ONLY=4064
BOTH=23840
```

Latest run NIFTY filtering:

```text
SHORT: 37093 -> 15572
LONG : 57015 -> 23331
```

v17q F7 can shift NIFTY regime/RS lookup back one 5-minute bar to avoid same-bar context lookahead:

```text
EQIDV17Q_NIFTY_LOOKUP_PREV_BAR=False by default
```

## 7. Setup Universe

LONG setups present in latest v17q full result:

```text
A_MOD_BREAK_C1_HIGH
A_MOD_CLOSE_CONTINUATION_BREAK
B_AVWAP_RECLAIM_REVERSAL
B_HUGE_C1_CLOSE_RECLAIM_BREAK
C_OR_BREAKOUT
D_EMA20_BOUNCE
E_VWAP_BAND_FADE
G_HIGHER_HIGH_BREAK
```

SHORT setups present in latest v17q full result:

```text
A_MOD_BREAK_C1_LOW
B_HUGE_RED_FAILED_BOUNCE
C_OR_BREAKDOWN
D_AVWAP_LOSE_REVERSAL
D_EMA20_REJECTION
E_VWAP_BAND_FADE
G_LOWER_LOW_BREAK
```

Setup family meanings:

| Family | LONG | SHORT | Meaning |
|---|---|---|---|
| A_MOD | `A_MOD_BREAK_C1_HIGH`, `A_MOD_CLOSE_CONTINUATION_BREAK` | `A_MOD_BREAK_C1_LOW` | Moderate impulse continuation through the first/confirmation candle. |
| B_HUGE | `B_HUGE_C1_CLOSE_RECLAIM_BREAK` | `B_HUGE_RED_FAILED_BOUNCE` | Huge impulse/reclaim/failure continuation patterns. |
| B/D_AVWAP reversal | `B_AVWAP_RECLAIM_REVERSAL` | `D_AVWAP_LOSE_REVERSAL` | AVWAP reclaim/lose reversal after prior-side excursion. |
| C_OR | `C_OR_BREAKOUT` | `C_OR_BREAKDOWN` | Opening-range breakout/breakdown. |
| D_EMA20 | `D_EMA20_BOUNCE` | `D_EMA20_REJECTION` | Pullback to EMA20 and directional rejection. |
| E_VWAP_BAND | `E_VWAP_BAND_FADE` | `E_VWAP_BAND_FADE` | VWAP band fade in aligned context. |
| G_STRUCTURE | `G_HIGHER_HIGH_BREAK` | `G_LOWER_LOW_BREAK` | Structure break through recent highs/lows. |

## 8. Base v16 Filters

v16 filters run after NIFTY context and before the v17 stack.

SHORT:

```text
Drop RSI dead zone: 35 <= rsi_signal < 40
Opening-range gate disabled
```

LONG:

```text
Drop quality_score dead zone: 7.5 < QS <= 8.0
Drop quality_score exhaustion: QS > 10.0
Drop AVWAP distance chase: avwap_dist_atr_signal > 3.0
Drop AVWAP distance dead zone: 1.0 <= avwap_dist_atr_signal < 1.5
Drop volume exhaustion: entry_bar_vol_ratio > 4.0 and bars_from_open >= 3
Opening-range gate disabled
```

Current inherited v17b quality bundle relaxes several v16 long thresholds in practice:

```text
QS absolute max raised to 12
Volume exhaustion threshold raised to 5x
QS dead zone narrowed to roughly 7.6-7.9
```

The latest v17q log confirms the runtime v16 filter print:

```text
SHORT: 15572 -> 12111
LONG : 23331 -> 15259

LONG drops:
  264 QS 7.6-7.9 dead + QS > 12
  668 distance > 3.0 ATR
  6141 distance 1.0-1.5 ATR dead
  999 volume > 5x exhaustion
```

## 9. Inherited v17 Filter Stack

### v17b / v17c

Live-parity and first quality bundle:

```text
Data directory switched to stocks_indicators_5min_eq_live2.
BE and trailing stop disabled.
NIFTY confirm time aligned with live detection.
SHORT_ONLY RSI 21-28 pocket blocked.
SHORT_ONLY ADX >= 44 blocked.
Short pullback setup blocked except v17c exception rules.
Late/high-volume LONG A_MOD anti-chase blocked.
v17c anti-chase: A_MOD long bars_from_open >= 12 and volume >= 3.5x.
```

### v17d

Controlled short expansion plus cleanup:

```text
SHORT windows expanded to 09:15-12:00 and 12:00-14:00.
SHORT BOTH-mode RS relaxed to 0.60 default, later v17f moves runtime to 0.50.
SHORT ADX min relaxed to 25.
SHORT moderate impulse min ATR relaxed to 0.25.
SHORT volume min ratio 0.90.
SHORT AVWAP distance cap 2.10.
```

v17d cleanup:

```text
Drop short pullback setup.
Drop RS-NaN shorts.
Drop extreme downside RS <= -2.0%.
Drop ADX chop 20 <= adx < 25.
Drop exhausted ADX >= 50.
Drop BOTH-mode AVWAP dead zone 0.50 <= avwap_dist_atr < 1.00.
Drop BOTH-mode weak time pockets 10:30-11:00 and 11:30-12:00.
After 13:30, keep late shorts only if SHORT_ONLY or nifty_rs <= -1.0%.
```

Latest v17q v17d attrition:

```text
SHORT: 12111 -> 5693
LONG : 15259 -> 15245
```

### v17f

Extra short cleanup:

```text
BOTH-mode short RS threshold moved to 0.50 in latest runtime.
Drop SHORT_ONLY shorts when nifty_rs > -0.25%.
Drop SHORT_ONLY high-ATR shorts when atr_pct >= 0.007.
Drop BOTH-mode shorts in 12:15-12:45 pocket.
```

Latest v17q v17f attrition:

```text
SHORT: 5693 -> 5001
```

### v17g

Architecture and setup additions:

```text
Fix LONG pullback lag from 1 to 2 where applicable.
Enable LONG B_AVWAP_RECLAIM_REVERSAL.
Add setup-level size_multiplier tags.
Replace multiple short time pockets with causal BOTH-mode rule:
  drop BOTH shorts when adx_signal < 25 and atr_pct_signal < 0.005.
Add LONG AVWAP distance cap default 2.25.
```

Reversal scan tuning:

```text
Body ATR min: 0.50
Close upper pct: 0.40
RSI min: 50
ADX min: 28
Volume min ratio: 1.20
Max hour: 12 in v17g, extended later by v17i
Require both prior bars below/above side condition
Require close relative to EMA20
```

Latest v17q v17g attrition:

```text
SHORT: 5001 -> 4695
```

### v17h

LONG B_AVWAP_RECLAIM_REVERSAL Patch B:

```text
reversal_volume_max_ratio <= 3.0
reversal_avwap_dist_atr_min >= 1.0
Require nifty_context_mode == BOTH
Require nifty_rel_strength_pct >= 1.0
```

Latest v17q v17h attrition:

```text
LONG: 15245 -> 14752
```

### v17i

LONG target and reversal rebalance:

```text
LONG target overridden to 0.80%.
Drop B_AVWAP_RECLAIM_REVERSAL RSI dead zone: 55 <= rsi_signal < 65.
LONG max trades per ticker per day raised to 2.
B_AVWAP_RECLAIM_REVERSAL max hour extended to 13.
```

Latest v17q v17i attrition:

```text
LONG: 14752 -> 14683
```

### v17j

SHORT D_AVWAP_LOSE_REVERSAL added:

```text
Enable short AVWAP lose reversal.
short_reversal_max_hour_ist = 13
short_reversal_volume_max_ratio <= 3.0
short_reversal_avwap_dist_atr_min >= 1.0
Drop LONG_ONLY mode.
Require nifty_rel_strength_pct <= -1.0.
SHORT max trades per ticker per day = 2.
```

Latest v17q v17j attrition:

```text
SHORT: 4695 -> 4506
```

### v17k

Adds broader setup universe:

LONG:

```text
A_PULLBACK_C2_THEN_BREAK_C2_HIGH
C_OR_BREAKOUT
D_EMA20_BOUNCE
E_VWAP_BAND_FADE
G_HIGHER_HIGH_BREAK
```

SHORT:

```text
C_OR_BREAKDOWN
D_EMA20_REJECTION
E_VWAP_BAND_FADE
G_LOWER_LOW_BREAK
```

Key scan tunables:

```text
LONG C_OR max hour: 11
LONG C_OR volume min ratio: 1.50
LONG C_OR OR width: 0.50% to 2.50%
LONG D_EMA20 max hour: 14
SHORT C_OR max hour: 11
SHORT D_EMA20 max hour: 14
VWAP band fade max hour: 14
G structure max hour: 14
G structure lookback: 5 bars
```

New-setup post-scan gate:

```text
LONG new setups require nifty_context_mode == BOTH and nifty_rs >= +1.0%.
SHORT new setups drop LONG_ONLY and require nifty_rs <= -1.0%.
```

Latest v17q v17k attrition:

```text
LONG : 14683 -> 7646
SHORT: 4506  -> 1835
```

### v17m

Target and low-win setup cleanup:

```text
SHORT target overridden to 0.80%.
```

LONG setup-specific filters:

| Setup | Keep condition |
|---|---|
| `A_MOD_BREAK_C1_HIGH` | `atr_pct_signal >= 0.00355` and `bars_from_open <= 17` |
| `A_MOD_CLOSE_CONTINUATION_BREAK` | `atr_pct_signal <= 0.00770` and `quality_score >= 5.70` |
| `C_OR_BREAKOUT` | `entry_bar_vol_ratio <= 3.76` and `nifty_rel_strength_pct >= 1.075` |
| `D_EMA20_BOUNCE` | `entry_bar_vol_ratio <= 4.35` and `avwap_dist_atr_signal >= 0.75` |
| `G_HIGHER_HIGH_BREAK` | `nifty_rel_strength_pct >= 1.138` and `quality_score >= 0.324` |

SHORT setup-specific filters:

| Setup | Keep condition |
|---|---|
| `A_MOD_BREAK_C1_LOW` | `entry_hour >= 9.6667` and `ema20_gap_atr_signal <= 3.27` |
| `C_OR_BREAKDOWN` | `entry_hour <= 10.25` and `adx_signal >= 26.45` |
| `D_AVWAP_LOSE_REVERSAL` | `adx_signal >= 31.60` |
| `D_EMA20_REJECTION` | `rsi_signal >= 41.17` and `ema20_gap_atr_signal <= 1.32` |
| `E_VWAP_BAND_FADE` | `adx_signal >= 28.13` |
| `G_LOWER_LOW_BREAK` | `avwap_dist_atr_signal >= 0.192` |

Latest v17q v17m attrition:

```text
LONG : 7646 -> 5699
SHORT: 1835 -> 1370
```

### v17n

Codex setup filters:

| Setup | Keep condition |
|---|---|
| LONG `G_HIGHER_HIGH_BREAK` | `quality_score >= 1.254` |
| LONG `D_EMA20_BOUNCE` | `nifty_rel_strength_pct >= 1.083` and `entry_bar_vol_ratio <= 4.081` |
| LONG `A_MOD_CLOSE_CONTINUATION_BREAK` | `rsi_signal >= 65.54` |
| SHORT `G_LOWER_LOW_BREAK` | `avwap_dist_atr_signal >= 1.117` |
| SHORT `D_EMA20_REJECTION` | `ema20_gap_atr_signal <= 0.941` |
| SHORT `C_OR_BREAKDOWN` | `rsi_signal <= 47.36` |

Same-bar/near-bar dedup:

```text
Base v17n default window: 10 minutes
v17p override used by v17q: 30 minutes
Group: ticker + time bucket + side
Keep highest setup priority, then highest quality_score, then earliest signal time
```

Priority ladder:

```text
E_VWAP_BAND_FADE
C_OR_BREAKDOWN
A_MOD_BREAK_C1_LOW
B_AVWAP_RECLAIM_REVERSAL
B_HUGE_C1_CLOSE_RECLAIM_BREAK
A_MOD_CLOSE_CONTINUATION_BREAK
C_OR_BREAKOUT
D_AVWAP_LOSE_REVERSAL
A_MOD_BREAK_C1_HIGH
D_EMA20_BOUNCE
G_HIGHER_HIGH_BREAK
D_EMA20_REJECTION
G_LOWER_LOW_BREAK
```

Latest v17q v17n attrition:

```text
LONG codex: 5699 -> 5034
SHORT codex: 1370 -> 1176
LONG dedup: 5034 -> 5020
SHORT dedup: 1176 -> 1175
```

### v17o

Top-cut setup filters:

| Setup | Keep condition |
|---|---|
| LONG `A_MOD_BREAK_C1_HIGH` | `atr_pct_signal >= 0.004` |
| LONG `B_AVWAP_RECLAIM_REVERSAL` | `nifty_rel_strength_pct >= 1.167` |
| LONG `B_HUGE_C1_CLOSE_RECLAIM_BREAK` | `rsi_signal >= 70.82` |
| LONG `C_OR_BREAKOUT` | `entry_bar_vol_ratio <= 3.26` |
| LONG `E_VWAP_BAND_FADE` | `nifty_rel_strength_pct >= 1.10` |
| SHORT `C_OR_BREAKDOWN` | `quality_score <= 0.84` |
| SHORT `D_AVWAP_LOSE_REVERSAL` | `adx_signal >= 33.13` |
| SHORT `E_VWAP_BAND_FADE` | `stochk_signal >= 64.71` |

Latest v17q v17o attrition:

```text
LONG : 5020 -> 4714
SHORT: 1175 -> 1125
```

### v17p

Stage 1 light filters:

| Setup | Keep condition |
|---|---|
| LONG `A_MOD_CLOSE_CONTINUATION_BREAK` | `rsi_signal <= 82.3` and `quality_score <= 8.26` |
| SHORT `A_MOD_BREAK_C1_LOW` | `stochk_signal >= 3.65` |
| SHORT `G_LOWER_LOW_BREAK` | `atr_pct_signal <= 0.0085` |

Stage 0 one-ticker-per-day:

```text
Group: trade_date + ticker + side
Keep highest setup priority, then highest quality_score, then earliest signal_time_ist
```

Stage 2 sizing multipliers:

| Setup | Multiplier |
|---|---:|
| `E_VWAP_BAND_FADE` | 1.50 |
| `B_AVWAP_RECLAIM_REVERSAL` | 1.25 |
| `C_OR_BREAKDOWN` | 1.25 |
| `B_HUGE_C1_CLOSE_RECLAIM_BREAK` | 1.25 |
| `D_AVWAP_LOSE_REVERSAL` | 1.25 |
| `A_MOD_BREAK_C1_LOW` | 1.25 |
| `A_MOD_BREAK_C1_HIGH` | 1.10 |
| `C_OR_BREAKOUT` | 1.10 |
| `D_EMA20_BOUNCE` | 1.10 |
| `A_MOD_CLOSE_CONTINUATION_BREAK` | 1.10 |
| `G_HIGHER_HIGH_BREAK` | 1.00 |
| `D_EMA20_REJECTION` | 0.50 |
| `G_LOWER_LOW_BREAK` | 0.50 |

Latest v17q v17p attrition:

```text
Stage 1 LONG : 4714 -> 4698
Stage 1 SHORT: 1125 -> 1039
Stage 0 LONG : 4698 -> 4572
Stage 0 SHORT: 1039 -> 1039
```

Note: latest final CSV notional/position fields looked effectively constant in the comparison work. Treat Stage 2 as an intended/reporting sizing layer unless verifying the final PnL columns for the specific CSV.

## 10. v17q-Specific Honesty Fixes

### F1: Hardened Stage 0

Default:

```text
EQIDV17Q_STAGE0_HARDEN=True
```

Purpose:

```text
Assert one-ticker-per-day per side actually runs.
Require needed columns instead of silently skipping.
Use v17n priority, quality_score descending, timestamp ascending.
```

Latest attrition:

```text
LONG : 4572 -> 4572
SHORT: 1039 -> 1039
```

### F4: Strict Audit

Default:

```text
EQIDV17Q_AUDIT_STRICT=True
```

Checks:

```text
No duplicate signal/entry rows.
No duplicate trade_date + ticker + side.
Exit time is valid relative to entry time.
TARGET rows have positive pnl_pct_price.
SL rows have negative pnl_pct_price.
Stop-fill penalty appears only on SL rows.
No 5M_FALLBACK exits when 1-minute exits are required.
Warn on lag below 5 minutes.
```

### F6: Prior-Bar Volume Ratio

Default:

```text
EQIDV17Q_VOL_RATIO_NO_LOOKAHEAD=False
```

Latest run appears to have this enabled based on log:

```text
[V17Q_F6] vol-ratio (prior-bar avg) computed for 23315/23331 LONG trades
```

Purpose:

```text
Avoid using full-day future volume when computing entry_bar_vol_ratio.
When enabled, average volume uses bars from session open through the entry bar only.
```

### F7: Previous-Bar NIFTY Lookup

Default:

```text
EQIDV17Q_NIFTY_LOOKUP_PREV_BAR=False
```

Purpose:

```text
Shift entry_time_ist back 5 minutes before NIFTY context/RS lookup.
This avoids using the close of the same signal/fill bar as regime information.
```

### F11: Disable Entry Close Confirmation Lookahead

Default:

```text
EQIDV17Q_NO_CLOSE_CONFIRM_LOOKAHEAD=True
```

Purpose:

```text
Disable require_entry_close_confirm on both configs.
Avoid accepting an intrabar fill but then requiring the same bar's close to confirm the fill.
That close is future data relative to the fill moment.
```

Latest log:

```text
[V17Q_F11] disabled require_entry_close_confirm for SHORT and LONG
```

### F12: Entry-Bar-Aware 1-Minute Exits

Default:

```text
EQIDV17Q_ENTRY_BAR_AWARE_EXITS=True
```

Purpose:

```text
Base resolver looks strictly after entry_time_ist.
F12 checks the 1-minute bars inside the entry 5-minute candle:
  (entry_time_ist - 5 minutes, entry_time_ist]
If the fill and stop/target both happen inside that entry candle, F12 corrects the outcome.
If both stop and target are touched in the same 1-minute bar, main path is pessimistic SL.
```

Latest run:

```text
SHORT scanned=1037, flipped_to_SL=1, flipped_to_TARGET=7, reaffirmed=29
LONG  scanned=4568, flipped_to_SL=65, flipped_to_TARGET=61, reaffirmed=315
```

### F15: Require 1-Minute Exits

Default:

```text
EQIDV17Q_REQUIRE_1MIN_EXITS=True
```

Purpose:

```text
Drop any row whose exit_resolution_case starts with 5M_FALLBACK.
This avoids mixing precise 1-minute exits with rough 5-minute fallback exits.
```

Latest run:

```text
SHORT: dropped 2 5M_FALLBACK rows
LONG : dropped 4 5M_FALLBACK rows
Final fallback rows: 0
```

## 11. v17q Toggle Defaults

| Toggle | Default | Meaning |
|---|---:|---|
| `EQIDV17Q_STAGE0_HARDEN` | True | Strict one-ticker-per-day invariant. |
| `EQIDV17Q_DEDUP_WINDOW_MIN` | 0 | No extra override beyond inherited v17p/v17n behavior. |
| `EQIDV17Q_ZERO_LAG_POLICY` | `keep` | Keep zero/same-time lag rows unless other fixes catch them. |
| `EQIDV17Q_AUDIT_STRICT` | True | Fail run on audit failures. |
| `EQIDV17Q_REQUIRE_1MIN_EXITS` | True | Drop 5-minute fallback exits. |
| `EQIDV17Q_STAMP_METADATA` | False | Do not write metadata JSON by default. |
| `EQIDV17Q_VOL_RATIO_NO_LOOKAHEAD` | False | Prior-bar-only volume average optional. |
| `EQIDV17Q_NIFTY_LOOKUP_PREV_BAR` | False | Previous-bar NIFTY lookup optional. |
| `EQIDV17Q_NIFTY_CONTEXT_FULL_SESSION` | False | Keep inherited session context behavior. |
| `EQIDV17Q_PARQUET_NAIVE_TZ` | `legacy` | Keep legacy timezone behavior. |
| `EQIDV17Q_STAGE2_PNL_ORDERED` | False | Do not move Stage 2 after PnL calculation. |
| `EQIDV17Q_NO_CLOSE_CONFIRM_LOOKAHEAD` | True | Disable close confirmation lookahead. |
| `EQIDV17Q_ENTRY_BAR_AWARE_EXITS` | True | Correct entry-bar exits using 1-minute bars. |
| `EQIDV17Q_ENTRY_AT_NEXT_OPEN` | False | Alternative fill model not active. |
| `EQIDV17Q_FLOOR_ZERO_LAG` | False | Zero-lag flooring optional. |
| `EQIDV17Q_RUN5_OPTIMIZED` | False | Concentrated Run 5 optimized subset optional. |
| `EQIDV17Q_RUN5_PRO` | False | Per-setup Run 5 pro filter set optional. |

## 12. Optional Run 5 Modes

These are coded in v17q but not active in the latest full-universe result unless the env toggles are turned on.

### RUN5_OPTIMIZED

Toggle:

```text
EQIDV17Q_RUN5_OPTIMIZED=1
```

Rules:

```text
LONG:
  setup in {B_AVWAP_RECLAIM_REVERSAL}
  50 <= rsi_signal < 75

SHORT:
  setup in {A_MOD_BREAK_C1_LOW}
  25 <= rsi_signal < 50
```

Backtested Run 5 honest subset from `RUN5_OPTIMIZATION_REPORT.md`:

```text
Trades: 104
Win rate: 71.15%
PF: 1.794
Levered sum PnL: +101.63%
Day-win: 68.85%
MaxDD price-return: 3.72%
```

This is much cleaner but far below the user's desired 3500-ish trade count.

### RUN5_PRO

Toggle:

```text
EQIDV17Q_RUN5_PRO=1
```

Per-setup filter table:

| Side | Setup | Filters |
|---|---|---|
| LONG | `B_HUGE_C1_CLOSE_RECLAIM_BREAK` | RSI 50-75, ADX >= 30, hour <= 11.5, ATR% 0.003-0.012 |
| LONG | `B_AVWAP_RECLAIM_REVERSAL` | RSI 50-75, ADX >= 30, QS >= 5 |
| LONG | `A_MOD_BREAK_C1_HIGH` | ADX >= 30, QS >= 7, ATR% 0.003-0.012 |
| LONG | `C_OR_BREAKOUT` | RSI 45-100, ADX >= 30, QS >= 3 |
| LONG | `G_HIGHER_HIGH_BREAK` | RSI 50-75, ADX >= 30, QS >= 3 |
| SHORT | `A_MOD_BREAK_C1_LOW` | RSI 30-50, hour <= 13.0, ATR% 0.003-0.012 |
| SHORT | `G_LOWER_LOW_BREAK` | RSI 30-50, ADX >= 30, ATR% 0.003-0.012 |
| SHORT | `D_EMA20_REJECTION` | RSI 0-45, ADX >= 30, hour <= 11.5, ATR% 0.003-0.012 |
| SHORT | `C_OR_BREAKDOWN` | RSI 20-45, ADX >= 30, ATR% 0.004-0.020 |
| SHORT | `D_AVWAP_LOSE_REVERSAL` | RSI 25-50, ATR% 0.004-0.020 |

Dropped by default in RUN5_PRO:

```text
LONG  A_MOD_CLOSE_CONTINUATION_BREAK
LONG  D_EMA20_BOUNCE
SHORT B_HUGE_RED_FAILED_BOUNCE
SHORT E_VWAP_BAND_FADE
```

Run 5 pro comment metrics:

```text
Trades: 353
Win rate: 68.84%
PF: 1.518
Levered sum PnL: +263.7%
Day-win: 65.45%
MaxDD price-return: 8.34%
```

This is also cleaner than full v17q but still far below 3500-ish trades.

## 13. Latest Full v17q Results

Latest completed run:

```text
20260427_172331
```

Overall:

| Metric | Value |
|---|---:|
| Trades | 5605 |
| Unique trade days | 222 |
| TARGET hits | 3348 |
| TARGET hit-rate | 59.73% |
| SL hits | 2119 |
| SL rate | 37.81% |
| EOD exits | 138 |
| EOD rate | 2.46% |
| Avg net PnL / trade | +0.1156% |
| Sum net PnL | +648.1667% |
| Profit factor | 1.064 |
| Day-win rate | 54.05% |
| MaxDD cumulative PnL | 246.1628% |
| Notional PnL | Rs.129,633 |

Side split:

| Side | Trades | Hit % | SL % | EOD % | Sum PnL % | Avg PnL % | PF | Day-Win % | MaxDD % |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| LONG | 4568 | 58.65 | 39.34 | 2.01 | +51.6177 | +0.0113 | 1.006 | 46.36 | 270.9088 |
| SHORT | 1037 | 64.51 | 31.05 | 4.44 | +596.5490 | +0.5753 | 1.384 | 60.41 | 55.1612 |
| COMBINED | 5605 | 59.73 | 37.81 | 2.46 | +648.1667 | +0.1156 | 1.064 | 54.05 | 246.1628 |

Exit realism:

```text
Resolved on 1-minute data: 5605 / 5605
5-minute fallback after F15: 0
Ambiguous same-bar exits: 18 / 5605
Stressed stop exits: 2119
```

Base vs pessimistic vs optimistic:

| Path | PnL % | PF | Day-Win % | MaxDD % |
|---|---:|---:|---:|---:|
| Base | +964.3573 | 1.098 | 57.66 | 211.8406 |
| Pessimistic reported | +648.1667 | 1.064 | 54.05 | 246.1628 |
| Optimistic | +790.3465 | 1.079 | 54.50 | 230.3651 |

## 14. Latest Full v17q By Setup

LONG:

| Setup | Trades | Hit % | Sum PnL % | PF | Read |
|---|---:|---:|---:|---:|---|
| `E_VWAP_BAND_FADE` | 36 | 83.33 | +67.81 | 3.41 | Strong but tiny sample. |
| `B_AVWAP_RECLAIM_REVERSAL` | 232 | 75.86 | +305.01 | 2.18 | Best meaningful LONG edge. |
| `A_MOD_BREAK_C1_HIGH` | 351 | 64.96 | +165.26 | 1.29 | Positive but weaker under v17q realism. |
| `A_MOD_CLOSE_CONTINUATION_BREAK` | 78 | 60.26 | +19.31 | 1.15 | Barely useful. |
| `D_EMA20_BOUNCE` | 851 | 59.22 | +65.69 | 1.04 | Flat, high volume, low edge. |
| `G_HIGHER_HIGH_BREAK` | 1761 | 59.00 | +23.42 | 1.01 | Massive volume, no real edge. |
| `B_HUGE_C1_CLOSE_RECLAIM_BREAK` | 181 | 57.46 | -4.34 | 0.99 | Breakeven/slightly negative. |
| `C_OR_BREAKOUT` | 1078 | 51.11 | -590.55 | 0.75 | Main LONG damage source. |

SHORT:

| Setup | Trades | Hit % | Sum PnL % | PF | Read |
|---|---:|---:|---:|---:|---|
| `E_VWAP_BAND_FADE` | 63 | 82.54 | +114.69 | 3.22 | Strong but modest sample. |
| `A_MOD_BREAK_C1_LOW` | 151 | 70.86 | +190.08 | 2.21 | Best meaningful SHORT edge. |
| `D_AVWAP_LOSE_REVERSAL` | 107 | 71.96 | +110.08 | 1.81 | Good edge. |
| `D_EMA20_REJECTION` | 169 | 63.91 | +95.27 | 1.38 | Positive but not elite. |
| `C_OR_BREAKDOWN` | 115 | 63.48 | +43.33 | 1.23 | Positive but modest. |
| `G_LOWER_LOW_BREAK` | 430 | 58.37 | +44.61 | 1.06 | Almost flat, high volume. |
| `B_HUGE_RED_FAILED_BOUNCE` | 2 | 50.00 | -1.50 | 0.68 | Too small and negative. |

## 15. Honest Diagnosis

The latest full v17q strategy is realistic but not yet strong enough as a full-universe strategy.

The core issue is not the short side. SHORT is usable:

```text
1037 trades
64.51% hit-rate
PF 1.384
+596.55% net PnL
```

The weak point is LONG volume dilution:

```text
4568 trades
58.65% hit-rate
PF 1.006
+51.62% net PnL
```

Most LONG edge comes from a few setups:

```text
B_AVWAP_RECLAIM_REVERSAL
E_VWAP_BAND_FADE
A_MOD_BREAK_C1_HIGH to a lesser degree
```

Most LONG drag comes from:

```text
C_OR_BREAKOUT
G_HIGHER_HIGH_BREAK
D_EMA20_BOUNCE
B_HUGE_C1_CLOSE_RECLAIM_BREAK
```

For the user's optimization goal of roughly 3500 trades, the likely path is not RUN5_OPTIMIZED or RUN5_PRO because those cut trade count too hard. The better path is a full-universe cleanup:

```text
Keep strong SHORTs mostly intact.
Cut or heavily gate LONG C_OR_BREAKOUT.
Cut or heavily gate LONG G_HIGHER_HIGH_BREAK.
Tighten LONG D_EMA20_BOUNCE.
Keep B_AVWAP_RECLAIM_REVERSAL as the primary LONG engine.
Keep total count near 3500 by removing roughly 1500-2000 low-edge LONG trades.
```

## 16. Files To Use

Primary source:

```text
avwap_combined_runner_v17q_5min.py
```

Inherited sources:

```text
avwap_combined_runner_v17p_5min.py
avwap_combined_runner_v17o_5min.py
avwap_combined_runner_v17n_5min.py
avwap_combined_runner_v17m_5min.py
avwap_combined_runner_v17k_5min.py
avwap_combined_runner_v17j_5min.py
avwap_combined_runner_v17i_5min.py
avwap_combined_runner_v17h_5min.py
avwap_combined_runner_v17g_5min.py
avwap_combined_runner_v17f_5min.py
avwap_combined_runner_v17d_5min.py
avwap_combined_runner_v17c_5min.py
avwap_combined_runner_v17b_5min.py
avwap_combined_runner_v16_5min.py
```

Optimization notes:

```text
RUN5_OPTIMIZATION_REPORT.md
_v17q_run5_optimizer.py
_v17q_run5_per_setup_optimizer.py
_v17q_run5_max_optimizer.py
```

