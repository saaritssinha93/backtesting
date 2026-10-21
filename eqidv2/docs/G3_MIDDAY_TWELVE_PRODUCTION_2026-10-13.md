# G3 V6: approved midday twelve, October 14 preparation

The existing 32 G3 setups plus exactly the twelve requested midday setups now
share `V13-V10-G-3-PRODUCTION-V6` in normal historical backtesting, the main
dashboard daily backtest, PAPER, and LIVE configuration. The authoritative
configuration is `configs/strategies/v13_v10_g_3_production_v6.json`.

**LIVE is prepared but not activated.** The real-money task remains disabled,
the deployment remains `PAPER_VALIDATION`, and no dated arm was written.
The actual October 14 coordinator readiness check returned `READY_DISARMED`.
No trading workers or orders were started. PAPER tasks remain enabled.

## Exact configuration and results

Normal historical results now cover July 29-October 13, 2026, 50 complete
sessions. October 1 is excluded for incomplete immutable input. The old
32 setups retain their filters, targets, selections, executions and P&L.

| Scope | Trades | Wins / losses | Win rate | Net P&L (Rs) |
| --- | ---: | --- | ---: | ---: |
| Existing 32 G3 setups | 257 | 192 / 65 | 74.71% | 743,590.44 |
| Approved twelve additions | 131 | 103 / 28 | 78.63% | 381,314.30 |
| Combined 44 G3 setups | 388 | 295 / 93 | 76.03% | 1,124,904.74 |
| October 13 combined daily | 11 | 6 / 5 | 54.55% | 2,607.29 |
| October 13 additions only | 5 | 2 / 3 | 40.00% | -7,360.97 |

Full-history gross P&L is Rs1,221,904.74 and modeled costs are Rs97,000.
Daily realized maximum drawdown is Rs12,374.10; profit factor is 4.30.
The 391 native fills become 388 executions under the shared capital limit.
There are 43 profitable sessions and seven losing sessions.

| Signal | Side | Target | Trades | Win rate | Net P&L (Rs) |
| --- | --- | ---: | ---: | ---: | ---: |
| 11:35 | LONG | 7.15% | 7 | 71.43% | 44,987.89 |
| 11:35 | SHORT | 0.87% | 18 | 83.33% | 37,593.01 |
| 11:40 | LONG | 4.03% | 8 | 75.00% | 26,243.53 |
| 11:40 | SHORT | 1.19% | 10 | 80.00% | 21,358.15 |
| 11:45 | SHORT | 4.95% | 15 | **66.67%** | 44,186.60 |
| 12:05 | LONG | 1.40% | 6 | 83.33% | 17,704.04 |
| 12:10 | SHORT | 0.79% | 14 | 78.57% | 24,094.16 |
| 12:15 | LONG | 1.24% | 7 | 85.71% | 18,191.94 |
| 12:15 | SHORT | 3.49% | 18 | 77.78% | 42,361.40 |
| 12:25 | SHORT | 2.13% | 9 | 77.78% | 36,621.92 |
| 12:30 | LONG | 2.53% | 9 | 88.89% | 46,034.48 |
| 12:30 | SHORT | 1.85% | 10 | 80.00% | 21,937.19 |

All twelve passed 70% in the prior 49-session sample. October 13's loss
brings 11:45 SHORT to 10 wins from 15 trades, or 66.67%. It remains enabled
because the user selected this exact configuration. No setup or target was
changed after observing the new day. The prior target search reused those
49 historical sessions; it does not establish a prospective minimum win rate.

## Shared execution rules

- Exact next-minute confirmation, one highest-liquidity stock per new setup,
  ten-minute entry expiry, 15:15 square-off.
- Initial stop 1.25%, tightening to 1.00% after 120 minutes.
- SHORT thresholds are copied as effective values; no second legacy
  multiplier applies. The 12:15 SHORT threshold is 0.10%; the other new
  SHORT price thresholds are 0.13%.
- Zero minimum confirmation-volume thresholds still require finite causal
  volume features and the existing runtime warmup; missing data is rejected.
- PAPER/backtest: Rs100,000 capital per entry, 5x exposure, Rs1,000,000
  portfolio capital, five basis points round-trip modeled costs.
- LIVE: the existing separate Rs10,000 margin / Rs50,000 exposure policy
  and whole-share sizing remain unchanged. Backtest rupee P&L is not a LIVE
  P&L estimate. No sizing, emergency, ownership or approval guard was loosened.

## Runtime and dashboard preparation

The 44 setups span 30 signal clocks. The last signal is 12:30, confirmation
is 12:31, its publication grace ends 12:32:30, and scanner/feed/confirmation
pipeline cutoff is 12:33. Runtime and monitoring derive these clocks from
the shared contract. Batch and session wrappers already pin G3 dynamically;
they inherit the V6 configuration without parallel hand-maintained rule sets.

Main dashboard 8787 and flow dashboard 8790 were reloaded and verified through
authenticated APIs. Both select the final 44-setup, 50-session history. Their
daily view and the actual main-dashboard daily card show October 13 V6.
October 14 PAPER and LIVE show no trading orders or events. The existing main
dashboard supervisor retains process ownership; no duplicate server was added.

The actual normal historical CLI succeeded:

```powershell
python fno_v13_v10_g_3_backtest.py --through 2026-10-13
```

Final historical run:
`C:/TradingData/eqidv2/backtesting_result_v13_v10_g_3/backtests/runs/2026-10-13/20261013_midday_twelve_v6_normal_cli`

Its `report.html` links `daily_results.csv`, `monthly_results.csv`,
`setup_results.csv`, `period_results.csv`, `portfolio_trades.csv`, the exact
config, summary and manifest. The normal daily publisher completed from raw
October 13 inputs, archived old V5 pointers, and published V6 daily/cumulative
results. Earlier G3 and G4 evidence remains preserved. Execution and strategy
analysis artifacts were refreshed; execution calibration is awaiting V6 PAPER
evidence rather than relabeling old sessions.

## October 14 operational state

The captured 16 scheduled-task definitions and enabled states are unchanged.
G3 PAPER/feed tasks remain scheduled for 09:15, the upstream futures feed for
09:05, opening recovery for 17:00 and daily backtest/history refresh for 17:15.
The daily launcher retains its `_1620` filename for compatibility with the
installed 17:15 trigger. The real-money coordinator task remains disabled.

Fresh read-only checks on all eight broker applications confirmed access,
positive funds, no open net positions and no pending orders. These checks did
not submit orders and do not replace tomorrow's authentication/readiness gate.
The separate options ATM feed has an existing stale-heartbeat supervisor
failure; equity setup readiness passed, but options-feed health is not claimed.

The existing operator-only activation procedure is documented in
`docs/G3_LIVE_OPERATOR_ACTIVATION_2026-10-13.md`; its plan now enumerates all
44 V6 rules and exact twelve additions. It still requires dated approval,
strategy/sizing fingerprints, broker reconciliation, emergency checks and
the existing PAPER-validation or explicit override policy. No activation or
override was applied during this migration.

## Validation and provenance

- 437 core contract/runtime/history/dashboard tests passed.
- 36 API/registry tests passed in the API environment.
- 143 G4 and retained simulator regression tests passed.
- Independent heap-based allocation and report accounting passed 745 checks.
- The historical agent's separate artifact/accounting audit passed 753 checks.
- Daily reconstruction validation passed 705 checks, including 641 snapshot
  input hashes. All twelve scanner/PAPER/LIVE configuration fixtures passed.
- All source and artifact hashes, baseline outcomes, report links, dashboard
  views, task states and inactive trading controls were verified.

Strategy fingerprint:
`7d01e80f60cc874e7c7b94a894785d134b70df71b27be98fdf3c302a96f44b4a`

Final historical manifest:
`b15d6f566f4a6439ed6949a8670383cee455222ff839918002320edabf2fe7a0`

Receipts and backups are under `.codex_tmp/g3_midday_production_20261013/`,
`.codex_tmp/g3_midday_promotion_20261013/`, and
`.codex_tmp/g3_midday_readiness_20261013/`. The archived V5 Python readers and
production JSON retain their original identities for historical verification.
