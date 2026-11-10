# Full slot configuration: G‑3 V17, G‑7 V4 Lock, and G‑3 V18 research

**Snapshot:** 3 November 2026. **Scope:** backtesting configuration. The open `START_HERE.html` describes G‑3 V18, which is a separate research version. Its `control` arm uses the G‑3 V17 rules below. G‑7 V4 Lock is included because it was the other strategy in the preceding comparison. This file does not change PAPER, LIVE, or a current backtest pointer.

For the assessment of indicator necessity, AND/OR combinations, weighted ranking and news, see [the indicator review](INDICATOR_REVIEW_2026-11-03.md). Its proposed experiments are not active settings.

## How to read the slot tables

Each row is one **signal slot and direction**. A five-minute bar ending at the first clock is checked, then the next one-minute confirmation ending at the second clock. `ON` means the slot can be selected; `OFF` lists its donor configuration for audit **but cannot place a backtest entry**. Both tables show configured minimum directional 5m price movement: LONG requires at least the shown positive percentage; SHORT requires a move at least that large downward. OI is a positive percentage increase. `5m vol ×` and `1m vol ×` are ratios to their respective historical volume reference; zero in a minimum column disables that particular threshold. `Value min ₹` records the configured traded-value threshold; the G‑7 12:26 SHORT threshold is applied **after ranking** as shown in Extra. Body and wick values are fractions of the confirmation candle's range. The wick maximum is the **upper wick for LONG** and **lower wick for SHORT**. `Quota / rank` is the maximum candidates per setup per day, chosen by the displayed rank key; `max_move` ranks by absolute directional move. All percentages are percentages, not decimal fractions.

### Shared integrity and timing checks

- Five-minute signal features must be complete and available at the signal bar close; one-minute features must be available at the exact next confirmation close. No later candle may contribute to selection. A five-minute source bar requires five valid one-minute components. Gaps, synthetic/opening snapshots, and stale confirmation inputs fail the relevant data check.
- The base directional gate requires EMA9 > EMA20 > EMA50 for LONG or EMA9 < EMA20 < EMA50 for SHORT, an absolute price change of at least 0.10%, increasing futures OI, OI change between **0.05% and 1.00%**, and five-minute relative volume at least **0.80×**. Per-row thresholds can be stricter. An eligible one-minute confirmation must close above both its open and signal close for LONG, or below both for SHORT.
- The 09:20 signal uses **09:15 cash open → 09:20 cash close** and a verified 09:16→09:20 near-month futures OI comparison; ordinary prior-bar OI is not substituted. For 09:20/09:25 SHORT signals, the dated near-month Nifty futures 09:20 first-bar return must be **≤−0.05%**.
- The confirmation-volume reference is the previous **20 observed completed one-minute bars**, excluding the current minute, with a five-observation research minimum. Live warmup rules may require 20 prior observations; this table is the backtest contract. A signal is ranked only after its entry filters pass; the core-first quota may prioritize core-rule candidates before relaxed candidates.
- G‑3's explicit global switches are `strict_0925_long=true`, `morning_slots=false`, and `two_bar_continuation=false`. Its opening-data rule excludes a stock from 09:20 if that stock's required OI baseline is missing or late; if all opening inputs are missing, it blocks the 09:20 cycle. These are configuration switches, not extra per-row thresholds.
- Entry is triggered at the confirmation high for LONG or confirmation low for SHORT, from the next minute, with the original **10-minute** trigger expiry. The historical backtest uses one-minute OHLC and conservative stop-first ordering if stop and target are both touched in one bar. Runtime's 90-second activation grace is a separate clock, not a five-minute wait or a change to this historical entry window.

## G‑3 V17 backtest — all 61 configured rows, 34 enabled

The table includes all 61 production donor rows so disabled slot/direction pairs are explicit. `ON` rows are the effective V17 backtest slots. An `OFF` row's stop and target are donor values only. G‑3 V17 disabled slots below the frozen historical net-win-rate cutoff or without executed evidence, and retained three explicitly restored low-target setups. That allowlist was chosen on past outcomes through 27 October; it is retrospective, not an independent forward validation.

**Core-first preference:** the move column gives the minimum expanded eligibility threshold. Candidates satisfying the preferred core rules are considered first for the setup quota. Five SHORT setups have a stricter core move threshold, shown in Extra: 09:26, 09:31, 09:51 and 11:21 prefer **0.20%** downward movement, and 09:36 prefers **0.50%**. A candidate clearing only the lower table threshold can therefore lose its place to a core candidate; clearing the minimum alone does not guarantee selection.

| ID | 5m→1m IST | Side | State | 5m move % | 5m OI min % | 5m vol × | Value min ₹ | 1m vol × | 1m body min | 1m wick max | Quota / rank | SL % | Target % | Extra guard / reason |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0921_LONG | 09:20→09:21 | LONG | ON | 0.3 | 0.1 | 3 | 0 | 1.8 | 0.75 | 0.35 | 1 / max_liquidity | 1.25 | 3 | verified opening OI |
| 0921_SHORT | 09:20→09:21 | SHORT | OFF | 0.13 | 0.05 | 1.5 | 0 | 1.2 | 0.4 | 0.6 | 2 / max_volume | 1.25 | 1.5 | opening Nifty≤−0.05%; verified opening OI; SLOT_WIN_RATE_BELOW70_OR_NO_TRADES |
| 0926_LONG | 09:25→09:26 | LONG | ON | 0.3 | 0.1 | 1.2 | 0 | 1.1 | 0.6 | 0.6 | 1 / max_liquidity | 1.25 | 0.97 | — |
| 0926_SHORT | 09:25→09:26 | SHORT | ON | 0.13 | 0.05 | 1.5 | 0 | 1.2 | 0.4 | 0.6 | 2 / max_volume | 1.25 | 2.67 | preferred core move≥0.2%; lower wick≥0.2; opening Nifty≤−0.05% |
| 0931_LONG | 09:30→09:31 | LONG | OFF | 0.65 | 0.1 | 1 | 0 | 1.1 | 0.5 | 0.6 | 1 / max_move | 1.25 | 2 | SLOT_WIN_RATE_BELOW70_OR_NO_TRADES |
| 0931_SHORT | 09:30→09:31 | SHORT | ON | 0.13 | 0.125 | 1 | 0 | 1.2 | 0.4 | 0.6 | 1 / max_move | 1.25 | 2 | preferred core move≥0.2% |
| 0936_LONG | 09:35→09:36 | LONG | OFF | 0.2 | 0.15 | 1 | 0 | 1.1 | 0.6 | 0.6 | 1 / max_liquidity | 1.25 | 1.73 | SLOT_WIN_RATE_BELOW70_OR_NO_TRADES |
| 0936_SHORT | 09:35→09:36 | SHORT | ON | 0.325 | 0.5 | 1 | 0 | 1.2 | 0.4 | 0.6 | 2 / max_liquidity | 1.25 | 1.23 | preferred core move≥0.5% |
| 0941_LONG | 09:40→09:41 | LONG | OFF | 0.2 | 0.075 | 2 | 0 | 1.1 | 0.5 | 0.6 | 1 / max_liquidity | 1.25 | 3 | SLOT_WIN_RATE_BELOW70_OR_NO_TRADES |
| 0941_SHORT | 09:40→09:41 | SHORT | OFF | 0.13 | 0.05 | 1 | 0 | 1.2 | 0.4 | 0.6 | 1 / max_move | 1.25 | 2.43 | SLOT_WIN_RATE_BELOW70_OR_NO_TRADES |
| 0946_LONG | 09:45→09:46 | LONG | OFF | 0.65 | 0.1 | 1 | 0 | 1.1 | 0.4 | 0.6 | 1 / max_move | 1.25 | 1.23 | SLOT_WIN_RATE_BELOW70_OR_NO_TRADES |
| 0946_SHORT | 09:45→09:46 | SHORT | OFF | 0.13 | 0.375 | 1 | 0 | 1.2 | 0.4 | 0.4 | 1 / max_volume | 1.25 | 1.23 | SLOT_WIN_RATE_BELOW70_OR_NO_TRADES |
| 0951_LONG | 09:50→09:51 | LONG | ON | 0.1 | 0.05 | 1.3 | 0 | 0.6 | 0.5 | 0.5 | 1 / max_liquidity | 1.25 | 0.75 | — |
| 0951_SHORT | 09:50→09:51 | SHORT | ON | 0.13 | 0.05 | 1 | 0 | 1.2 | 0.4 | 0.6 | 1 / max_liquidity | 1.25 | 0.93 | preferred core move≥0.2% |
| 0956_LONG | 09:55→09:56 | LONG | ON | 0.2 | 0.1 | 1 | 0 | 1.1 | 0.4 | 0.6 | 1 / max_liquidity | 1.25 | 0.9 | — |
| 1001_LONG | 10:00→10:01 | LONG | OFF | 0.4 | 0.05 | 1 | 0 | 1.1 | 0.4 | 0.6 | 1 / max_liquidity | 1.25 | 3 | SLOT_WIN_RATE_BELOW70_OR_NO_TRADES |
| 1006_LONG | 10:05→10:06 | LONG | ON | 0.1 | 0.05 | 1.3 | 0 | 0.6 | 0.7 | 0.5 | 1 / max_liquidity | 1.25 | 0.56 | — |
| 1011_LONG | 10:10→10:11 | LONG | OFF | 0.1 | 0.05 | 1.3 | 0 | 1.1 | 0.5 | 0.5 | 1 / max_liquidity | 1.25 | 1.5 | SLOT_WIN_RATE_BELOW70_OR_NO_TRADES |
| 1011_SHORT | 10:10→10:11 | SHORT | ON | 0.11 | 0.05 | 1 | 0 | 1.2 | 0.4 | 0.6 | 1 / max_liquidity | 1.25 | 1.56 | — |
| 1016_LONG | 10:15→10:16 | LONG | ON | 0.3 | 0.05 | 1.3 | 0 | 0.6 | 0.5 | 0.5 | 1 / max_liquidity | 1.25 | 0.96 | — |
| 1016_SHORT | 10:15→10:16 | SHORT | ON | 0.13 | 0.05 | 1 | 0 | 1.2 | 0.4 | 0.6 | 1 / max_liquidity | 1.25 | 0.68 | — |
| 1026_SHORT | 10:25→10:26 | SHORT | ON | 0.13 | 0.05 | 1 | 0 | 1.2 | 0.4 | 0.6 | 1 / max_liquidity | 1.25 | 2.85 | — |
| 1031_SHORT | 10:30→10:31 | SHORT | ON | 0.13 | 0.05 | 1 | 0 | 1.2 | 0.4 | 0.6 | 1 / max_liquidity | 1.25 | 1.66 | — |
| 1036_LONG | 10:35→10:36 | LONG | ON | 0.1 | 0.05 | 1.3 | 0 | 0.6 | 0.5 | 0.5 | 1 / max_liquidity | 1.25 | 2.12 | 1m extension≤3 ATR |
| 1036_SHORT | 10:35→10:36 | SHORT | ON | 0.13 | 0.05 | 1 | 0 | 0.4 | 0.3 | 0.6 | 1 / max_liquidity | 1.25 | 2.86 | 1m extension≤3 ATR |
| 1041_LONG | 10:40→10:41 | LONG | ON | 0.1 | 0.05 | 1.5 | 0 | 0.6 | 0.5 | 0.5 | 1 / max_liquidity | 1.25 | 1.71 | — |
| 1046_LONG | 10:45→10:46 | LONG | ON | 0.1 | 0.05 | 1.5 | 0 | 0.6 | 0.5 | 0.5 | 1 / max_liquidity | 1.25 | 0.79 | — |
| 1046_SHORT | 10:45→10:46 | SHORT | ON | 0.13 | 0.05 | 0.8 | 0 | 0.8 | 0 | 0.6 | 1 / max_liquidity | 1.25 | 1.01 | — |
| 1051_LONG | 10:50→10:51 | LONG | ON | 0.1 | 0.05 | 0.8 | 0 | 0.4 | 0 | 0.5 | 1 / max_liquidity | 1.25 | 0.77 | — |
| 1056_SHORT | 10:55→10:56 | SHORT | ON | 0.13 | 0.05 | 1 | 0 | 0.8 | 0 | 1 | 1 / max_liquidity | 1.25 | 1.53 | — |
| 1111_LONG | 11:10→11:11 | LONG | ON | 0.1 | 0.15 | 1.3 | 0 | 0.6 | 0.5 | 0.5 | 1 / max_liquidity | 1.25 | 1.76 | — |
| 1111_SHORT | 11:10→11:11 | SHORT | OFF | 0.13 | 0.05 | 1 | 0 | 1.2 | 0.3 | 0.6 | 1 / max_liquidity | 1.25 | 1.84 | SLOT_WIN_RATE_BELOW70_OR_NO_TRADES |
| 1121_SHORT | 11:20→11:21 | SHORT | ON | 0.13 | 0.05 | 1 | 0 | 1.2 | 0.4 | 0.6 | 1 / max_liquidity | 1.25 | 2 | preferred core move≥0.2% |
| 1126_SHORT | 11:25→11:26 | SHORT | ON | 0.13 | 0.05 | 1 | 0 | 0.4 | 0 | 1 | 1 / max_liquidity | 1.25 | 4.2 | — |
| 1136_LONG | 11:35→11:36 | LONG | ON | 0.1 | 0.05 | 1.3 | 0 | 1 | 0.5 | 0.5 | 1 / max_liquidity | 1.25 | 7.15 | — |
| 1136_SHORT | 11:35→11:36 | SHORT | ON | 0.13 | 0.05 | 1 | 0 | 0.4 | 0.15 | 0.6 | 1 / max_liquidity | 1.25 | 0.87 | — |
| 1141_LONG | 11:40→11:41 | LONG | ON | 0.1 | 0.05 | 1.3 | 0 | 0 | 0.5 | 0.5 | 1 / max_liquidity | 1.25 | 4.03 | — |
| 1141_SHORT | 11:40→11:41 | SHORT | OFF | 0.13 | 0.05 | 1 | 0 | 1.2 | 0 | 1 | 1 / max_liquidity | 1.25 | 1.19 | SLOT_WIN_RATE_BELOW70_OR_NO_TRADES |
| 1146_SHORT | 11:45→11:46 | SHORT | OFF | 0.13 | 0.05 | 1 | 0 | 1.2 | 0 | 0.6 | 1 / max_liquidity | 1.25 | 4.95 | SLOT_WIN_RATE_BELOW70_OR_NO_TRADES |
| 1206_LONG | 12:05→12:06 | LONG | ON | 0.1 | 0.05 | 1.3 | 0 | 0.6 | 0.7 | 0.5 | 1 / max_liquidity | 1.25 | 1.4 | — |
| 1211_SHORT | 12:10→12:11 | SHORT | ON | 0.13 | 0.05 | 1 | 0 | 0 | 0.3 | 0.6 | 1 / max_liquidity | 1.25 | 0.79 | — |
| 1216_LONG | 12:15→12:16 | LONG | ON | 0.1 | 0.05 | 1.3 | 0 | 1 | 0.5 | 0.5 | 1 / max_liquidity | 1.25 | 1.24 | — |
| 1216_SHORT | 12:15→12:16 | SHORT | OFF | 0.1 | 0.05 | 1 | 0 | 1.2 | 0.15 | 0.75 | 1 / max_liquidity | 1.25 | 3.49 | SLOT_WIN_RATE_BELOW70_OR_NO_TRADES |
| 1226_SHORT | 12:25→12:26 | SHORT | OFF | 0.13 | 0.05 | 1 | 0 | 1.2 | 0.15 | 0.6 | 1 / max_liquidity | 1.25 | 2.13 | SLOT_WIN_RATE_BELOW70_OR_NO_TRADES |
| 1231_LONG | 12:30→12:31 | LONG | ON | 0.1 | 0.05 | 1.3 | 0 | 0.6 | 0.5 | 0.5 | 1 / max_liquidity | 1.25 | 2.53 | — |
| 1231_SHORT | 12:30→12:31 | SHORT | OFF | 0.13 | 0.05 | 1 | 0 | 1.2 | 0 | 0.75 | 1 / max_liquidity | 1.25 | 1.85 | SLOT_WIN_RATE_BELOW70_OR_NO_TRADES |
| 1236_LONG | 12:35→12:36 | LONG | OFF | 0.1 | 0.05 | 1.3 | 0 | 1.1 | 0.3 | 0.5 | 1 / max_liquidity | 1.25 | 0.37 | TARGET_LE_075 |
| 1236_SHORT | 12:35→12:36 | SHORT | ON | 0.1 | 0.05 | 0.9 | 0 | 0 | 0.15 | 1 | 1 / max_liquidity | 1.25 | 1.24 | — |
| 1246_LONG | 12:45→12:46 | LONG | OFF | 0.1 | 0.05 | 1.3 | 0 | 1.1 | 0 | 0.75 | 1 / max_liquidity | 1.25 | 0.23 | TARGET_LE_075 |
| 1246_SHORT | 12:45→12:46 | SHORT | OFF | 0.13 | 0.05 | 1 | 0 | 1.2 | 0.15 | 0.75 | 1 / max_liquidity | 1.25 | 0.49 | TARGET_LE_075 |
| 1251_LONG | 12:50→12:51 | LONG | OFF | 0.1 | 0.05 | 1.3 | 0 | 1.1 | 0 | 0.5 | 1 / max_liquidity | 1.25 | 0.26 | TARGET_LE_075 |
| 1251_SHORT | 12:50→12:51 | SHORT | OFF | 0.1 | 0.05 | 0.9 | 0 | 1.2 | 0 | 0.75 | 1 / max_liquidity | 1.25 | 0.14 | TARGET_LE_075 |
| 1256_SHORT | 12:55→12:56 | SHORT | OFF | 0.13 | 0.05 | 1 | 0 | 1.2 | 0.15 | 0.6 | 1 / max_liquidity | 1.25 | 0.14 | TARGET_LE_075 |
| 1301_SHORT | 13:00→13:01 | SHORT | ON | 0.1 | 0.05 | 0.8 | 0 | 0.8 | 0 | 0.75 | 1 / max_liquidity | 1.25 | 0.88 | — |
| 1306_LONG | 13:05→13:06 | LONG | OFF | 0.1 | 0.05 | 1 | 0 | 1.1 | 0.3 | 0.75 | 1 / max_liquidity | 1.25 | 0.18 | TARGET_LE_075 |
| 1316_LONG | 13:15→13:16 | LONG | OFF | 0.1 | 0.05 | 1 | 0 | 1.1 | 0 | 0.5 | 1 / max_liquidity | 1.25 | 0.17 | TARGET_LE_075 |
| 1316_SHORT | 13:15→13:16 | SHORT | OFF | 0.18 | 0.07 | 1.2 | 0 | 1.2 | 0.5 | 0.5 | 1 / max_liquidity | 1.25 | 0.33 | TARGET_LE_075 |
| 1321_SHORT | 13:20→13:21 | SHORT | OFF | 0.13 | 0.05 | 1 | 0 | 1.2 | 0.15 | 0.6 | 1 / max_liquidity | 1.25 | 1.01 | SLOT_WIN_RATE_BELOW70_OR_NO_TRADES |
| 1326_LONG | 13:25→13:26 | LONG | OFF | 0.1 | 0.05 | 1.3 | 0 | 1.1 | 0 | 0.75 | 1 / max_liquidity | 1.25 | 0.41 | TARGET_LE_075 |
| 1326_SHORT | 13:25→13:26 | SHORT | OFF | 0.13 | 0.05 | 1 | 0 | 1.2 | 0.4 | 0.6 | 1 / max_liquidity | 1.25 | 0.76 | SLOT_WIN_RATE_BELOW70_OR_NO_TRADES |
| 1331_LONG | 13:30→13:31 | LONG | ON | 0.15 | 0.07 | 1.4 | 0 | 0.8 | 0.55 | 0.45 | 1 / max_liquidity | 1.25 | 1.17 | — |

**G‑3 exits and capital.** Full-position target is the row target; initial SL is **1.25%**, tightened to **1.00%** after **120 minutes**. No partial exit or breakeven stop is configured. Positions square off at the **15:15** completed minute. The modeled portfolio has **₹10,00,000** capital, **₹1,00,000** allocated per entry with **5×** exposure (₹5,00,000 target exposure), and **5 bps round-trip** modeled cost. `max_positions` is unset; allocation still observes available portfolio capital. The 10:35 LONG/SHORT confirmation has the additional directional close-to-EMA20 extension limit of **3 ATR**. The 09:26 SHORT confirmation needs at least **0.20 lower-wick ratio** before ranking.

## G‑7 V4 Lock backtest — all 51 configured rows, 42 enabled

`OFF` rows are excluded by the fixed G‑7 slot allowlist. The base G‑7 configuration supplies the per-row entry thresholds and full targets; V4 Lock adds the guards below. `postrank` in the Extra column means the ranked order is vetoed if it misses the stronger threshold; the quota is **not** filled by a replacement in that particular rule. The 09:26 SHORT lower-wick rule is different: it runs **before** max-volume ranking and allows the next eligible candidate to replace a rejected one.

| ID | 5m→1m IST | Side | State | 5m move % | 5m OI min % | 5m vol × | Value min ₹ | 1m vol × | 1m body min | 1m wick max | Quota / rank | SL % | Target % | Extra guard / reason |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0921_LONG | 09:20→09:21 | LONG | ON | 0.3 | 0.1 | 3 | 0 | 1.8 | 0.75 | 0.35 | 1 / max_liquidity | 1.2 | 2 | verified opening OI |
| 0921_SHORT | 09:20→09:21 | SHORT | OFF | 0.13 | 0.05 | 1.5 | 0 | 1.2 | 0.4 | 0.6 | 2 / max_volume | 1.2 | 1.5 | opening Nifty≤−0.05%; verified opening OI; frozen slot allowlist rejects |
| 0926_LONG | 09:25→09:26 | LONG | ON | 0.3 | 0.1 | 1.2 | 0 | 1.1 | 0.6 | 0.6 | 1 / max_liquidity | 1.2 | 0.97 | — |
| 0926_SHORT | 09:25→09:26 | SHORT | ON | 0.13 | 0.05 | 1.5 | 0 | 1.2 | 0.4 | 0.6 | 2 / max_volume | 1.2 | 2 | 1m extension≤4 ATR; lower wick≥0.2; opening Nifty≤−0.05% |
| 0931_LONG | 09:30→09:31 | LONG | OFF | 0.65 | 0.1 | 1 | 0 | 1.1 | 0.5 | 0.6 | 1 / max_move | 1.2 | 2 | frozen slot allowlist rejects |
| 0931_SHORT | 09:30→09:31 | SHORT | ON | 0.13 | 0.125 | 1 | 0 | 1.2 | 0.4 | 0.6 | 1 / max_move | 1.2 | 2 | — |
| 0936_LONG | 09:35→09:36 | LONG | ON | 0.2 | 0.15 | 1 | 0 | 1.1 | 0.6 | 0.6 | 1 / max_liquidity | 1.2 | 1.73 | 1m extension≤3.5 ATR |
| 0936_SHORT | 09:35→09:36 | SHORT | OFF | 0.325 | 0.5 | 1 | 0 | 1.2 | 0.4 | 0.6 | 2 / max_liquidity | 1.2 | 1.23 | frozen slot allowlist rejects |
| 0941_LONG | 09:40→09:41 | LONG | ON | 0.2 | 0.075 | 2 | 0 | 0.8 | 0.5 | 0.6 | 1 / max_liquidity | 1.2 | 2 | — |
| 0941_SHORT | 09:40→09:41 | SHORT | ON | 0.13 | 0.05 | 1 | 0 | 1.2 | 0.4 | 0.6 | 1 / max_move | 1.2 | 2 | — |
| 0946_LONG | 09:45→09:46 | LONG | ON | 0.65 | 0.1 | 1 | 0 | 1.1 | 0.4 | 0.6 | 1 / max_move | 1.2 | 1.23 | 1m extension≤3 ATR |
| 0946_SHORT | 09:45→09:46 | SHORT | ON | 0.13 | 0.375 | 1 | 0 | 1.2 | 0.4 | 0.4 | 1 / max_volume | 1.2 | 1.23 | 1m extension≤3 ATR |
| 0951_LONG | 09:50→09:51 | LONG | ON | 0.1 | 0.05 | 1.3 | 0 | 0.6 | 0.5 | 0.5 | 1 / max_liquidity | 1.2 | 0.75 | — |
| 0951_SHORT | 09:50→09:51 | SHORT | ON | 0.13 | 0.05 | 1 | 0 | 1.2 | 0.4 | 0.6 | 1 / max_liquidity | 1.2 | 0.93 | — |
| 0956_LONG | 09:55→09:56 | LONG | ON | 0.2 | 0.1 | 1 | 0 | 1.1 | 0.4 | 0.6 | 1 / max_liquidity | 1.2 | 0.9 | — |
| 1001_LONG | 10:00→10:01 | LONG | OFF | 0.4 | 0.05 | 1 | 0 | 1.1 | 0.4 | 0.6 | 1 / max_liquidity | 1.2 | 2 | 1m extension≤4 ATR; frozen slot allowlist rejects |
| 1006_LONG | 10:05→10:06 | LONG | ON | 0.1 | 0.05 | 1.3 | 0 | 0.6 | 0.7 | 0.5 | 1 / max_liquidity | 1.2 | 0.6 | — |
| 1011_LONG | 10:10→10:11 | LONG | ON | 0.1 | 0.125 | 1.3 | 0 | 0.6 | 0.5 | 0.5 | 1 / max_liquidity | 1.2 | 1.5 | postrank OI≥0.125% |
| 1011_SHORT | 10:10→10:11 | SHORT | ON | 0.11 | 0.05 | 1 | 0 | 1.2 | 0.4 | 0.6 | 1 / max_liquidity | 1.2 | 1.56 | — |
| 1016_LONG | 10:15→10:16 | LONG | ON | 0.3 | 0.05 | 1.3 | 0 | 0.6 | 0.5 | 0.5 | 1 / max_liquidity | 1.2 | 0.96 | — |
| 1016_SHORT | 10:15→10:16 | SHORT | ON | 0.13 | 0.05 | 1 | 0 | 1.2 | 0.4 | 0.6 | 1 / max_liquidity | 1.2 | 0.68 | — |
| 1026_SHORT | 10:25→10:26 | SHORT | ON | 0.13 | 0.05 | 1 | 0 | 1.2 | 0.4 | 0.6 | 1 / max_liquidity | 1.2 | 2 | — |
| 1031_SHORT | 10:30→10:31 | SHORT | OFF | 0.13 | 0.05 | 1 | 0 | 1.2 | 0.4 | 0.6 | 1 / max_liquidity | 1.2 | 1.66 | frozen slot allowlist rejects |
| 1036_LONG | 10:35→10:36 | LONG | ON | 0.1 | 0.05 | 1.3 | 0 | 0.6 | 0.5 | 0.5 | 1 / max_liquidity | 1.2 | 2 | 1m extension≤3 ATR |
| 1036_SHORT | 10:35→10:36 | SHORT | ON | 0.13 | 0.05 | 1 | 0 | 0.4 | 0.3 | 0.6 | 1 / max_liquidity | 1.2 | 2 | 1m extension≤3 ATR |
| 1041_LONG | 10:40→10:41 | LONG | ON | 0.1 | 0.05 | 1.5 | 0 | 0.6 | 0.5 | 0.5 | 1 / max_liquidity | 1.2 | 1.71 | — |
| 1046_LONG | 10:45→10:46 | LONG | ON | 0.1 | 0.05 | 1.5 | 0 | 0.6 | 0.5 | 0.5 | 1 / max_liquidity | 1.2 | 0.79 | — |
| 1046_SHORT | 10:45→10:46 | SHORT | ON | 0.13 | 0.05 | 0.8 | 0 | 0.8 | 0 | 0.6 | 1 / max_liquidity | 1.2 | 1.01 | — |
| 1051_LONG | 10:50→10:51 | LONG | ON | 0.1 | 0.05 | 0.8 | 0 | 0.4 | 0 | 0.5 | 1 / max_liquidity | 1.2 | 0.77 | — |
| 1056_SHORT | 10:55→10:56 | SHORT | ON | 0.13 | 0.05 | 1 | 0 | 0.8 | 0 | 1 | 1 / max_liquidity | 1.2 | 1.53 | — |
| 1111_LONG | 11:10→11:11 | LONG | ON | 0.1 | 0.15 | 1.3 | 0 | 0.6 | 0.5 | 0.5 | 1 / max_liquidity | 1.2 | 1.76 | — |
| 1111_SHORT | 11:10→11:11 | SHORT | OFF | 0.13 | 0.05 | 1 | 0 | 0.8 | 0.3 | 0.6 | 1 / max_liquidity | 1.2 | 1.84 | frozen slot allowlist rejects |
| 1121_SHORT | 11:20→11:21 | SHORT | ON | 0.13 | 0.05 | 1 | 0 | 1.2 | 0.4 | 0.6 | 1 / max_liquidity | 1.2 | 2 | — |
| 1126_SHORT | 11:25→11:26 | SHORT | ON | 0.13 | 0.05 | 1 | 0 | 0.4 | 0 | 1 | 1 / max_liquidity | 1.2 | 2 | — |
| 1136_LONG | 11:35→11:36 | LONG | ON | 0.1 | 0.05 | 1.3 | 0 | 1 | 0.5 | 0.5 | 1 / max_liquidity | 1.2 | 2 | 1m extension≤3.5 ATR |
| 1136_SHORT | 11:35→11:36 | SHORT | ON | 0.13 | 0.05 | 1 | 0 | 0.4 | 0.15 | 0.6 | 1 / max_liquidity | 1.2 | 0.87 | — |
| 1141_LONG | 11:40→11:41 | LONG | ON | 0.1 | 0.05 | 1.3 | 0 | 0 | 0.5 | 0.5 | 1 / max_liquidity | 1.2 | 2 | — |
| 1141_SHORT | 11:40→11:41 | SHORT | ON | 0.13 | 0.05 | 1 | 0 | 1.2 | 0 | 1 | 1 / max_liquidity | 1.2 | 1.19 | — |
| 1146_SHORT | 11:45→11:46 | SHORT | OFF | 0.13 | 0.05 | 1 | 0 | 0 | 0 | 0.6 | 1 / max_liquidity | 1.2 | 2 | 1m extension≤4 ATR; frozen slot allowlist rejects |
| 1206_LONG | 12:05→12:06 | LONG | ON | 0.1 | 0.1 | 1.3 | 0 | 0.6 | 0.7 | 0.5 | 1 / max_liquidity | 1.2 | 1.4 | postrank OI≥0.1% |
| 1211_SHORT | 12:10→12:11 | SHORT | ON | 0.13 | 0.05 | 1 | 0 | 0 | 0.3 | 0.6 | 1 / max_liquidity | 1.2 | 0.79 | — |
| 1216_LONG | 12:15→12:16 | LONG | ON | 0.1 | 0.05 | 1.3 | 0 | 1 | 0.5 | 0.5 | 1 / max_liquidity | 1.2 | 1.24 | — |
| 1216_SHORT | 12:15→12:16 | SHORT | ON | 0.1 | 0.05 | 1 | 0 | 0 | 0.15 | 0.75 | 1 / max_liquidity | 1.2 | 2 | — |
| 1226_SHORT | 12:25→12:26 | SHORT | ON | 0.13 | 0.05 | 1 | 10000000 | 0 | 0.15 | 0.6 | 1 / max_liquidity | 1.2 | 2 | postrank value≥₹10,000,000 |
| 1231_LONG | 12:30→12:31 | LONG | ON | 0.1 | 0.05 | 1.3 | 0 | 0.6 | 0.5 | 0.5 | 1 / max_liquidity | 1.2 | 2 | — |
| 1231_SHORT | 12:30→12:31 | SHORT | OFF | 0.13 | 0.05 | 1 | 0 | 0.4 | 0 | 0.75 | 1 / max_liquidity | 1.2 | 1.85 | frozen slot allowlist rejects |
| 1236_SHORT | 12:35→12:36 | SHORT | ON | 0.1 | 0.05 | 0.9 | 0 | 0 | 0.15 | 1 | 1 / max_liquidity | 1.2 | 1.24 | 1m extension≤3 ATR |
| 1301_SHORT | 13:00→13:01 | SHORT | ON | 0.1 | 0.05 | 0.8 | 0 | 0.8 | 0 | 0.75 | 1 / max_liquidity | 1.2 | 0.88 | 1m extension≤3.5 ATR |
| 1321_SHORT | 13:20→13:21 | SHORT | OFF | 0.13 | 0.05 | 1 | 0 | 0 | 0.15 | 0.6 | 1 / max_liquidity | 1.2 | 1.01 | frozen slot allowlist rejects |
| 1326_SHORT | 13:25→13:26 | SHORT | ON | 0.13 | 0.05 | 1 | 0 | 1.2 | 0.4 | 0.6 | 1 / max_liquidity | 1.2 | 0.76 | 1m extension≤3 ATR |
| 1331_LONG | 13:30→13:31 | LONG | ON | 0.15 | 0.07 | 1.4 | 0 | 0.8 | 0.55 | 0.45 | 1 / max_liquidity | 1.2 | 1.17 | — |

**G‑7 direction and entry guards.** The selected direction policy uses dated near-month **Nifty futures**. It rejects a trade only if **both** the exact latest completed five-minute candle's close-versus-open direction and that close-versus-the-09:20-bar's-open direction oppose the trade. This uses completed five-minute evidence, not the confirmation minute's latest quote. Missing or invalid required Nifty evidence fails the run. A separate V4 veto rejects an entry only when all three conditions hold at confirmation: a directional VWAP z-score **>2.5** *or* a prior same-clock historical **95th-percentile extreme**; 10-change price efficiency **<0.30**; and RSI14 weakening over **three completed minutes**. With fewer than **30 valid session one-minute bars**, this V4 indicator veto preserves the entry. The 09:26 SHORT also requires lower wick **≥0.20** and confirmation extension **≤4 ATR**. Other per-slot extension limits are in the table.

**G‑7 effective V4 exits and capital.** V4 Lock replays a **single full-position target** at the row percentage. Its initial SL is **1.20%**, tightened to **1.00%** after **120 minutes**. A completed post-entry one-minute close reaching **+0.60% gross** arms a stop protecting **+0.10% gross**, executable from the **next** one-minute open; the active stop never loosens and a gap can fill worse. This V4 replay supersedes the earlier G‑7 V3 half-at-half-target exit plan. At 15:15, remaining positions square off. Capital is **₹10,00,000**, with **₹1,00,000** per entry, **5×** target exposure, **integer cash-share sizing**, one active position per stock, a conservative entry clock, and **5 bps round-trip** modeled cost. After the **second realized net losing exit** in a day, new admissions are blocked; existing positions continue to be managed. The fixed slot allowlist is applied before fresh portfolio allocation and loss-lock replay.

## G‑3 V18 research overlays (not the active V17 contract)

The V18 `control` retains every V17 slot, quota, entry and exit setting above. Each other V18 arm applies its extra rule **before** ranking so another eligible stock may replace a rejected one. These are separate research arms; none is promoted to LIVE or PAPER.

| V18 arm | Additional or removed rule |
|---|---|
| `control` | Exact G‑3 V17 selection and execution. |
| `market_only` | Directional breadth from session open **≥50%**, with dated-universe coverage **≥90%** and valid exact-clock data. |
| `strength_only` | Signed stock return since open minus Nifty spot return since open **≥0** in the trade direction. |
| `vwap_pressure` | Signed five-minute approximate VWAP distance **≥0**, VWAP slope **≥0**, and six-bar candle/volume pressure **≥0**. |
| `advanced` | All three preceding added groups together. |
| `simplified` | Removes EMA9/20/50 stacking and the minimum 1m body / maximum 1m wick thresholds, including the 09:26 SHORT lower-wick minimum; keeps price, OI, volume, directional confirmation, timing, ranking, quotas, stops and targets. |
| `simplified_advanced` | `simplified` plus all three added groups. |

V18 uses only completed signal-bar market/stock features for its new filters. Missing required features fail closed; there is no first-bar VWAP-slope substitute. Its VWAP is a typical-price approximation and pressure is a candle/volume proxy, not measured aggressor flow. The V18 results in `START_HERE.html` end on **30 October 2026** and did **not** qualify any changed arm for promotion under its predeclared October check.

## Versioned source files

- `configs/strategies/v13_v10_g_3_backtest_slot_win70_v17.json` — SHA-256 `5ab945d8bb84cb15c5aef5d9770f7792262f6cc9664493e6fb6f57e3ba6e715f`
- `configs/strategies/v13_v10_g_7_g6_replica_v3.json` — SHA-256 `189e51d96bca4aa4ee8cece37e1c7aa5c0436834e677d4e6031e9af250b3d213`
- `configs/strategies/v13_v10_g_7_v4_low_efficiency_backtest.json` — SHA-256 `b7fbd848db48b4298f0b48bf9682ed44cfaf116f01437318e03efdea2b3aaa91`
- `configs/strategies/v13_v10_g_7_v4_lock_backtest.json` — SHA-256 `451118c7c3e363cb79f73dad4f5973493ff6523110726b3685b2cf331e10fd6d`
- `configs/strategies/slot_win70_20261028.json` — SHA-256 `be35260f2278502d72c58ac56a2a54d7ac59719c9e3e78eb1adb10309e8bd86a`
- `fno_v13_v10_g_3_contract.py` — SHA-256 `9779b665b22ef8aa373ec868c62fc30d2098bc1a5fa50b115f83c51d841619d9`
- `research/g3_slot_win70_contract.py` — SHA-256 `4d6b04d30253d27fbb0fca246ad991d86a9809e7204f1efb44088da60431556f`
- `research/g7_g6_replica_backtest.py` — SHA-256 `d9b5c214dbde1f1b4df9deb9515011eef04edf68a8e057adb51a50bf11a82952`
- `g3_v18_20261102/selection.py` — SHA-256 `477083c21f796075c03534d0cb46b30196c5b46420b56ae1ec7f1eed3ec6b37d`

The tables were generated from these checked-in contracts. For exact implementation details, inspect the source files and the saved versioned configuration JSON rather than assuming an `OFF` row is tradable.
