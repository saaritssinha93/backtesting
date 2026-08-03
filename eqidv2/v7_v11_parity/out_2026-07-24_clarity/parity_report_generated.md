# V7-live vs V11-backtest — Parity Reconciliation (auto-generated tables)

Sessions: 2026-07-24  
Verdict: **FAIL**

## Per-day

| date | live_signals | bt_signals | live_trades | bt_trades | matched | live_only | live_stale | bt_only | live_net_stat_rs | bt_net_stat_rs | live_recorded_net_rs |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 2026-07-24 | 0 | 8 | 0 | 8 | 0 | 0 | 0 | 8 | 0 | 162.5 | 0 |

## Signal reconciliation

| date | live_signals | bt_signals | matched | live_only | bt_only |
| --- | --- | --- | --- | --- | --- |
| 2026-07-24 | 0 | 8 | 0 | 0 | 8 |

## Per-setup (ranked by unmatched)

| setup | live_n | bt_n | matched | live_only | bt_only | live_net | bt_net |
| --- | --- | --- | --- | --- | --- | --- | --- |
| G_HIGHER_HIGH_BREAK | 0 | 6 | 0 | 0 | 6 | 0.0 | 2421.7 |
| A_PULLBACK_C2_THEN_BREAK_C2_LOW | 0 | 1 | 0 | 0 | 1 | 0.0 | -1278.0 |
| C_OR_BREAKDOWN | 0 | 1 | 0 | 0 | 1 | 0.0 | -981.2 |

## Root-cause tally

| cause | count |
| --- | --- |
| backtest_only_live_zero_day | 8 |

## Matched trades (entry/exit/net diffs)

_No matched trades._

## Sample LIVE-ONLY (up to 15)

| date | ticker | side | setup | signal_bar | outcome | _cause | _note |
| --- | --- | --- | --- | --- | --- | --- | --- |

## Sample BACKTEST-ONLY (up to 25)

| date | ticker | side | setup | signal_bar | outcome | bt_gross_rs | _cause | _note |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 2026-07-24 | MOTILALOFS | SHORT | A_PULLBACK_C2_THEN_BREAK_C2_LOW | 2026-07-24 11:15:00 | SL | -1195.3399999999995 | backtest_only_live_zero_day | Live produced zero trades this session while the 5-min scanner accepted candidates; primary suspect is the entry-engine handoff freshness race (scanner writes latest_candidate_tickers.json at slot+45-60s vs the 30s max_signal_handoff_lag_sec deadline) -> raw_entry_rows=0 every slot. Verify against the day's entry-engine log. |
| 2026-07-24 | KAJARIACER | LONG | G_HIGHER_HIGH_BREAK | 2026-07-24 11:25:00 | EOD_CLOSE | 984.0040039062537 | backtest_only_live_zero_day | Live produced zero trades this session while the 5-min scanner accepted candidates; primary suspect is the entry-engine handoff freshness race (scanner writes latest_candidate_tickers.json at slot+45-60s vs the 30s max_signal_handoff_lag_sec deadline) -> raw_entry_rows=0 every slot. Verify against the day's entry-engine log. |
| 2026-07-24 | ETERNAL | SHORT | C_OR_BREAKDOWN | 2026-07-24 11:45:00 | SL | -898.5799999999967 | backtest_only_live_zero_day | Live produced zero trades this session while the 5-min scanner accepted candidates; primary suspect is the entry-engine handoff freshness race (scanner writes latest_candidate_tickers.json at slot+45-60s vs the 30s max_signal_handoff_lag_sec deadline) -> raw_entry_rows=0 every slot. Verify against the day's entry-engine log. |
| 2026-07-24 | FEDFINA | LONG | G_HIGHER_HIGH_BREAK | 2026-07-24 11:45:00 | EOD_CLOSE | 159.59675292969052 | backtest_only_live_zero_day | Live produced zero trades this session while the 5-min scanner accepted candidates; primary suspect is the entry-engine handoff freshness race (scanner writes latest_candidate_tickers.json at slot+45-60s vs the 30s max_signal_handoff_lag_sec deadline) -> raw_entry_rows=0 every slot. Verify against the day's entry-engine log. |
| 2026-07-24 | PRIVISCL | LONG | G_HIGHER_HIGH_BREAK | 2026-07-24 11:45:00 | EOD_CLOSE | 437.6673632812485 | backtest_only_live_zero_day | Live produced zero trades this session while the 5-min scanner accepted candidates; primary suspect is the entry-engine handoff freshness race (scanner writes latest_candidate_tickers.json at slot+45-60s vs the 30s max_signal_handoff_lag_sec deadline) -> raw_entry_rows=0 every slot. Verify against the day's entry-engine log. |
| 2026-07-24 | J&KBANK | LONG | G_HIGHER_HIGH_BREAK | 2026-07-24 12:30:00 | EOD_CLOSE | -426.9828173828204 | backtest_only_live_zero_day | Live produced zero trades this session while the 5-min scanner accepted candidates; primary suspect is the entry-engine handoff freshness race (scanner writes latest_candidate_tickers.json at slot+45-60s vs the 30s max_signal_handoff_lag_sec deadline) -> raw_entry_rows=0 every slot. Verify against the day's entry-engine log. |
| 2026-07-24 | SAKSOFT | LONG | G_HIGHER_HIGH_BREAK | 2026-07-24 12:30:00 | TARGET | 1999.2400000000014 | backtest_only_live_zero_day | Live produced zero trades this session while the 5-min scanner accepted candidates; primary suspect is the entry-engine handoff freshness race (scanner writes latest_candidate_tickers.json at slot+45-60s vs the 30s max_signal_handoff_lag_sec deadline) -> raw_entry_rows=0 every slot. Verify against the day's entry-engine log. |
| 2026-07-24 | NEWGEN | LONG | G_HIGHER_HIGH_BREAK | 2026-07-24 14:05:00 | EOD_CLOSE | -236.40240478515625 | backtest_only_live_zero_day | Live produced zero trades this session while the 5-min scanner accepted candidates; primary suspect is the entry-engine handoff freshness race (scanner writes latest_candidate_tickers.json at slot+45-60s vs the 30s max_signal_handoff_lag_sec deadline) -> raw_entry_rows=0 every slot. Verify against the day's entry-engine log. |
