# V7 Live Strategy, Flow, And Plan

Last updated: 2026-05-29

This document describes the current `Signal discovery v7 5mins ID` live paper system, including scanner logic, entry flow, paper execution, research outputs, current early-mode tightening, and future investigation priorities.

## 1. Scope

V7 live is a paper-trading research/live-validation pipeline for intraday 5-minute setups.

It is not one single file. It is a staged system:

1. 5-minute candidate discovery writes candidate tickers only.
2. 1-minute entry engine converts candidates into executable signal rows.
3. Paper executor simulates paper trades from the signal rows.
4. Research layer reconciles raw candidates, gated candidates, entries, live signals, paper trades, missed trades, and 1-minute exit paths.

The current live research policy is:

- No v7/v9 backtesting comparison for live suggestions.
- Use live paper trades plus live-generated missed candidates.
- Use stored 1-minute data for EOD exit research.
- Treat ranker and exit lab as research/shadow unless explicitly promoted.

## 2. Main Files

### Live Scanner

- File: `eqidv2_signal_discovery_v7_5min_id_persistent.py`
- Candidate scanner helper: `avwap_5min_ID_v7_candidate_scan.py`
- Runner: `bat/run_eqidv2_signal_discovery_v7_5min_id_persistent.bat`
- Runtime root: `C:\TradingData\eqidv2\signal_discovery_v7_5mins_ID`

### 1-Minute Entry Engine

- File: `eqidv2_entry_engine_1min_v5_id.py`
- Runner: `bat/run_eqidv2_entry_engine_1min_v5_id.bat`
- Runtime root: `C:\TradingData\eqidv2\entry_engine_1min_v5_ID`

### Paper Executor

- File: `avwap_trade_execution_PAPER_TRADE_TRUE_id_5min_v7.py`
- Runner: `bat/run_avwap_trade_execution_PAPER_TRADE_TRUE_id_5min_v7.bat`
- Signal directory: `live_signals`

### Research Layer

- File: `v7_research_layer/eqidv2_v7_research_layer.py`
- Runner: `bat/run_eqidv2_v7_research_layer.bat`
- Runtime root: `C:\TradingData\eqidv2\live_research_v7_research_layer`

### Replay Tool

- File: `tools/replay_v7_early_scanner_5sessions.py`
- Purpose: reproducible 5-session early scanner replay using stored 1-minute exits.
- Latest verified tight replay:
  `C:\TradingData\eqidv2\scanner_replay_v7_early_5sessions_tight_v3`

## 3. Data Sources

### 5-Minute Live Stock Data

Scanner source:

`C:\TradingData\eqidv2\stocks_indicators_5min_eq_live`

The scanner reads per-ticker files like:

`<TICKER>_stocks_indicators_5min.parquet`

### Stored 1-Minute Indicator Data

Research and replay source:

`C:\TradingData\eqidv2\stocks_indicators_1min_eq`

Files look like:

`<TICKER>_stocks_indicators_1min.parquet`

These are updated around 16:00 daily and are used by the research layer and replay tool to resolve missed trades and exits.

### Entry-Time Raw 1-Minute Data

The live entry engine fetches raw 1-minute data around the signal slot and stores it under:

`C:\TradingData\eqidv2\stocks_raw_1min_entry_v5_id_live`

## 4. Daily Timing

Market open: `09:15 IST`

Current V7 flow:

- Signal discovery runner starts scheduling from `09:15`.
- Practical signal evaluation starts at `09:30`.
- Early mode is active from `09:30` to `11:00`.
- Normal V7 5-minute setup scanning runs during the standard intraday signal window.
- Signal discovery stops around `15:00` and hard-stops at `15:30`.
- Entry engine scans candidate outputs, with a 60-second delay and a 5-minute entry search window.
- Paper executor runner cutoff is `15:40`.
- 1-minute data files are normally complete around `16:00`.
- Research/EOD reports should run around `16:15`.

## 5. High-Level Flow

### Step 1: Signal Discovery

The scanner evaluates completed 5-minute candles.

It writes:

- Raw candidates:
  `C:\TradingData\eqidv2\signal_discovery_v7_5mins_ID\csv\raw_candidate_tickers_<YYYY-MM-DD>.csv`
- Gated/live candidates:
  `C:\TradingData\eqidv2\signal_discovery_v7_5mins_ID\csv\candidate_tickers_<YYYY-MM-DD>.csv`
- Research-filter rejections:
  `C:\TradingData\eqidv2\signal_discovery_v7_5mins_ID\csv\research_filter_rejected_candidate_tickers_<YYYY-MM-DD>.csv`
- Latest snapshots:
  `C:\TradingData\eqidv2\signal_discovery_v7_5mins_ID\latest`

The scanner writes candidate tickers only. It does not attach entry price or execute trades.

Important live behavior:

- It dedupes candidates by `candidate_id`.
- It also keeps only one candidate per ticker per day in the daily live candidate CSV.
- Early candidates have `selection_mode = early_v1`.
- Early candidates have `candidate_family = EARLY`.

### Step 2: Live Gate

Standard candidates pass through the V8 live gate:

`C:\TradingData\eqidv2\outputs_ID_v8_5min_research_restore\accepted_rules.csv`

Early candidates use a separate early live gate:

- Minimum early quality score: `95`
- Maximum per side per slot: `4`
- Maximum per slot: `8`

### Step 3: Research Live Filters

Current active research filters:

- Long anti-chase:
  - Reject long candidates when `close_loc > 0.88` and `vwap_dist_atr > 0.52`.
- `B_AVWAP_RECLAIM_REVERSAL`:
  - Require `ranker_score >= 0.65`.
- `L_TREND_PULLBACK`:
  - Active probation block.

These filters are configured in:

`bat/run_eqidv2_signal_discovery_v7_5min_id_persistent.bat`

### Step 4: 1-Minute Entry Engine

The entry engine consumes latest candidate ticker snapshots and writes executable signal rows.

Inputs:

- `latest_candidate_tickers.json`
- `latest_candidate_tickers.csv`

Entry logic:

- Waits about `60` seconds after signal slot.
- Searches for first 1-minute bar from signal time through `+5` minutes.
- Uses the 1-minute bar open as entry price.
- Attaches setup-specific SL/target rule from `avwap_5min_ID_v6_backtesting.py`.

Outputs:

- `live_signals/signals_<YYYY-MM-DD>_id_5min_v7_short.csv`
- `live_signals/signals_<YYYY-MM-DD>_id_5min_v7_long.csv`

### Step 5: Paper Executor

The paper executor reads the long and short signal CSVs and simulates paper trades.

Runner defaults from BAT:

- Default paper position size: `Rs 10,000`
- Long stop: `0.75%`
- Short stop: `0.75%`
- Long target: `1.00%`
- Short target: `1.00%`
- Max trades argument: `20`
- Max concurrent/open positions configured to `20`
- Entry price source: `ltp_on_signal`

Outputs:

- `live_signals/paper_trades_<YYYY-MM-DD>_id_5min_v7.csv`
- `live_signals/paper_trade_summary_id_5min_v7.json`
- `live_signals/executed_signals_paper_id_5min_v7.json`

### Step 6: Research Layer

The research layer reconciles the full funnel:

1. Raw scanner candidates.
2. Gated candidates.
3. Research-filter rejected candidates.
4. Entry engine raw/selected rows.
5. Live signal rows.
6. Paper trade rows.
7. Forward MFE/MAE from 1-minute data.
8. Exit strategy lab results.

Main outputs:

- Truth table:
  `C:\TradingData\eqidv2\live_research_v7_research_layer\truth_table`
- Reality gap report:
  `C:\TradingData\eqidv2\live_research_v7_research_layer\reports`
- Candidate ranker:
  `C:\TradingData\eqidv2\live_research_v7_research_layer\ranker`
- Suggestions:
  `C:\TradingData\eqidv2\live_research_v7_research_layer\suggestions`
- Exit lab:
  `C:\TradingData\eqidv2\live_research_v7_research_layer\exit_lab`
- Latest copies:
  `C:\TradingData\eqidv2\live_research_v7_research_layer\latest`

## 6. Current Early Strategy

Early mode was added to address the lack of trades between `09:30` and `11:00`.

The broad early scanner was tested on the last 5 valid sessions including `2026-05-29`. It produced too many trades and weak PF. It was then tightened.

### Active Early Setups

Only these early setups are currently allowed live:

- `E_ORB_BREAKOUT_LONG`
- `E_ORB_BREAKOUT_SHORT`
- `E_VWAP_LOSE_EARLY_SHORT`

### Blocked/Probation Early Setups

These are currently blocked in live early mode:

- `E_RS_FIRST_HOUR_BREAK_LONG`
- `E_RS_FIRST_HOUR_BREAK_SHORT`
- `E_VWAP_RECLAIM_EARLY_LONG`
- `E_FAILED_OR_BREAKOUT_TRAP_SHORT`
- `E_ORB_RETEST_HOLD_SHORT`
- `E_ORB_RETEST_HOLD_LONG`
- `E_FAILED_OR_BREAKDOWN_TRAP_LONG`
- `E_GAP_HOLD_CONTINUATION_LONG`
- `E_GAP_HOLD_CONTINUATION_SHORT`
- `E_OPENING_DRIVE_CONTINUATION_LONG`
- `E_OPENING_DRIVE_CONTINUATION_SHORT`

### Early ORB Long Filter

For `E_ORB_BREAKOUT_LONG`, require:

- `vol_ratio <= 2.00`
- `rs_pct >= 4.00`
- `vwap_dist_atr <= 1.80`

Purpose:

- Avoid overextended long chases.
- Prefer strong relative strength with controlled volume/extension.

### Early ORB Short Filter

For `E_ORB_BREAKOUT_SHORT`, require:

- `rs_pct >= -1.50`
- `atr_pct <= 0.0065`
- `body_pct >= 0.82`

Purpose:

- Avoid shorting already exhausted downside moves.
- Keep decisive breakdown candles.
- Avoid very high ATR whipsaw names.

### Early VWAP Lose Short Filter

For `E_VWAP_LOSE_EARLY_SHORT`, require:

- `rs_pct >= -1.20`
- `close_loc >= 0.08`
- `atr_pct <= 0.008`

Purpose:

- Avoid shorting a candle already pinned at the absolute low.
- Keep controlled VWAP failure shorts with room for follow-through.

## 7. Early Scanner Replay Validation

Replay command:

```powershell
python tools\replay_v7_early_scanner_5sessions.py --workers 8 --output-root C:\TradingData\eqidv2\scanner_replay_v7_early_5sessions_tight_v3
```

Sessions used:

- `2026-05-22`
- `2026-05-25`
- `2026-05-26`
- `2026-05-27`
- `2026-05-29`

Result before tightening:

- Trades: `554`
- PF: `0.75`
- Net PnL: `Rs -37,398.71`
- Win rate: `42.24%`

Result after tightening:

- Raw early candidates: `339`
- Gated before daily dedupe: `190`
- Live-like candidates: `183`
- Resolved trades: `183`
- Entry/exit rejects: `0`
- PF: `1.50`
- Net PnL: `Rs 16,292.50`
- Win rate: `58.47%`
- Target rate: `48.09%`
- SL rate: `34.43%`
- EOD rate: `17.49%`

Setup breakdown after tightening:

| side | setup | trades | PF | net PnL |
|---|---|---:|---:|---:|
| SHORT | `E_ORB_BREAKOUT_SHORT` | 119 | 1.42 | Rs 9,109.82 |
| LONG | `E_ORB_BREAKOUT_LONG` | 34 | 1.60 | Rs 3,885.00 |
| SHORT | `E_VWAP_LOSE_EARLY_SHORT` | 30 | 1.77 | Rs 3,297.68 |

Latest report:

`C:\TradingData\eqidv2\scanner_replay_v7_early_5sessions_tight_v3\v7_early_scanner_replay_report_2026-05-22_to_2026-05-29.md`

## 8. Normal V7 Setup Guidance

Current research conclusions:

### Keep/Watch

- `D_EMA20_REJECTION`
  - Multi-window evidence: 15 trades, PF 2.19, net Rs 4,036.14.
  - Keep unchanged for now.

- `G_HIGHER_HIGH_BREAK`
  - Recent paper results healthy.
  - Keep unchanged unless setup-specific exit research proves better.

- `D_EMA20_BOUNCE`
  - Recent 3-session evidence was good.
  - Keep unchanged, shadow only.

### Probation/Restricted

- `L_TREND_PULLBACK`
  - Weak multi-window evidence.
  - Active probation block in live scanner.

- `B_AVWAP_RECLAIM_REVERSAL`
  - Requires `ranker_score >= 0.65`.
  - Future work should search for a narrow rescue condition, not allow all.

### Global Long Anti-Chase

Long entries are weak when stretched.

Current active filter:

- Reject/shadow long candidates where:
  `close_loc > 0.88 AND vwap_dist_atr > 0.52`

## 9. Exit Strategy Research

The exit lab uses stored 1-minute bars:

`C:\TradingData\eqidv2\stocks_indicators_1min_eq`

Current interpretation:

- One global exit rule is not enough.
- Exit behavior differs by setup and cohort.
- Dynamic profiles are research candidates, not automatic live changes.

Latest multi-window exit lab:

- Actual paper best overall:
  `dynamic_be_0p60_model_target`
- Rejected missed best overall:
  `static_wide_1p00_1p50`
- `D_EMA20_REJECTION` likes a dynamic/trailing style.
- `G_HIGHER_HIGH_BREAK` looked better with `static_balanced_0p70_1p00`.

Future exit work:

1. Build setup-specific exit profiles.
2. Keep same-bar SL/target conflict conservative, SL first.
3. Do not promote exit rules from incomplete 1-minute coverage.
4. Test paper/shadow first before live active.

## 10. Missed Trade Research

The reality-gap and exit-lab reports show many rejected candidates with good forward 1-minute follow-through.

The strongest future investigation area is missed shorts.

Priority missed-short families:

- `A_MOD_BREAK_C1_LOW`
- `A_PULLBACK_C2_THEN_BREAK_C2_LOW`
- `C_OR_BREAKDOWN`
- `D_AVWAP_LOSE_REVERSAL`
- `G_LOWER_LOW_BREAK`

Research direction:

- Do not add them blindly.
- Build a shadow lane for rejected short winners.
- Add filters for:
  - clean 1-minute MFE/MAE,
  - VWAP direction,
  - ATR containment,
  - market regime,
  - controlled extension,
  - not already at exhausted candle low.

## 11. Candidate Ranker Status

The ranker is not ready to become a hard live gate.

Latest evidence:

- Ranker top 10 clean rate: `0.0%`
- Ranker top 20 clean rate: `30.0%`
- V8 accepted clean rate: `25.0%`

Current rule:

- Keep ranker as research/shadow.
- Use ranker only where already explicitly approved, such as `B_AVWAP_RECLAIM_REVERSAL >= 0.65`.

Future ranker work:

- Reweight using 1-minute MFE/MAE labels.
- Add time-of-day features.
- Add regime features.
- Add extension and anti-chase penalties.
- Validate over at least 5 clean sessions before active promotion.

## 12. Operational Checklist

### Before Market

1. Confirm auth/token sessions are healthy.
2. Confirm 5-minute data fetchers are running.
3. Start or confirm the v7 signal discovery session.
4. Start or confirm the 1-minute entry engine.
5. Start or confirm the paper executor.
6. Confirm dashboard cards are not stale.

### During Early Window

Watch `09:30` to `11:00`:

- Early candidates should appear only when strict conditions pass.
- Expect fewer trades than the broad early scanner.
- It is normal to have quiet days.
- Most early trades should come from:
  - `E_ORB_BREAKOUT_SHORT`
  - `E_ORB_BREAKOUT_LONG`
  - `E_VWAP_LOSE_EARLY_SHORT`

### During Normal Window

Watch:

- Candidate count.
- V8 gate pass count.
- Entry engine selected rows.
- Live signal CSV rows.
- Paper trades.
- Any `no_entry_row` issues.

### EOD

At or after `16:15`, run/review:

- Reality gap report.
- Candidate ranker report.
- Multi-window suggestions.
- 1-minute exit lab.
- EOD action plan.

## 13. Promotion Rules

Use this promotion ladder:

1. Virtual research.
2. Paper experiment.
3. Scanner shadow.
4. Scanner active.
5. Live false.
6. Live true only after clean sustained proof.

Do not promote a change when:

- It is based on a single noisy day.
- 5-minute or 1-minute data coverage is incomplete.
- PF improves only by leaving too few trades.
- It removes too many winners.
- It improves backtest/replay but fails live paper validation.

Minimum preferred proof:

- 5 clean sessions for scanner/paper filter changes.
- 10 clean sessions for high-impact live gate changes.
- Setup-level sample should ideally be at least 20 trades before treating it as stable.

## 14. Future Investigation Backlog

### A. Validate Tight Early Mode

Run current early mode live for the next 5 to 10 clean sessions.

Track:

- Trades per day.
- PF.
- Net PnL.
- Win rate.
- Setup-level distribution.
- Whether `09:30` to `11:00` now contributes useful trades.

Do not reopen blocked early setups until this validation is complete.

### B. Missed Short Shadow Scanner

Build a shadow report/lane for strong rejected shorts.

Candidate families:

- `A_MOD_BREAK_C1_LOW`
- `A_PULLBACK_C2_THEN_BREAK_C2_LOW`
- `C_OR_BREAKDOWN`
- `D_AVWAP_LOSE_REVERSAL`
- `G_LOWER_LOW_BREAK`

Goal:

- Find conditions where rejected shorts had clean 1-minute follow-through and low MAE.

### C. Setup-Specific Exit Lab

Move from global exit profiles to setup-specific exit profiles.

Priority setups:

- `D_EMA20_REJECTION`
- `G_HIGHER_HIGH_BREAK`
- `E_ORB_BREAKOUT_SHORT`
- `E_ORB_BREAKOUT_LONG`
- `E_VWAP_LOSE_EARLY_SHORT`

### D. Ranker Rework

Ranker needs new labels and feature weights.

Use:

- 1-minute MFE/MAE.
- Clean move label.
- Bad move label.
- Regime.
- Time-of-day.
- Extension/chase features.

### E. Reality Gap Reduction

Current bottleneck is often:

raw candidates -> V8 gate -> selected entries

Future reports should separate:

- high-score rejected winners,
- high-score rejected losers,
- passed-not-traded,
- no-entry-row,
- paper skipped/rejected,
- live signal not paper executed.

### F. Data Health

No scanner improvement should be promoted from a day with stale/incomplete data.

Research reports should keep a hard context guard for:

- 5-minute data completeness.
- NIFTY context completeness.
- 1-minute exit-path coverage.
- fetch SLA warnings.

## 15. Current Known Good Replay Command

Use this to reproduce the current early-mode replay:

```powershell
python tools\replay_v7_early_scanner_5sessions.py --workers 8 --output-root C:\TradingData\eqidv2\scanner_replay_v7_early_5sessions_tight_v3
```

Expected historical result with current code and unchanged data:

- Trades: `183`
- PF: `1.50`
- Net PnL: `Rs 16,292.50`
- Win rate: `58.47%`

## 16. Current Live Expectation

Will V7 now get early entries?

Yes, after the v7 live sessions are restarted/started with the current code and BAT configuration.

Expected behavior:

- Early entries can occur from `09:30` to `11:00`.
- Trade count should be lower than the broad early scanner.
- Quality should be better.
- No trade is forced. Quiet early windows are acceptable.
- The three active early setups should dominate early paper trades.

