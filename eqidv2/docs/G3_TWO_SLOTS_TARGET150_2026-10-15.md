# G3: 09:20 SHORT and 10:10 LONG

User instruction on 2026-10-15: enable both setups throughout current G3 backtesting, PAPER and LIVE configurations, with a **1.50% target for each**. Entry rules use the existing baseline donors: 09:25 SHORT and 09:50 LONG respectively.

| Rule | 09:20 SHORT | 10:10 LONG |
| --- | --- | --- |
| Setup ID | `0921_SHORT` | `1011_LONG` |
| Exact confirmation, IST | 09:21 | 10:11 |
| Target | 1.50% | 1.50% |
| Initial stop | 1.25% | 1.25% |
| Active price-change threshold | decline >= 0.13% | rise >= 0.10% |
| Core price-change threshold | decline >= 0.20% | rise >= 0.10% |
| OI increase | 0.05% through 1.00% | 0.05% through 1.00% |
| 5-minute volume ratio | >= 1.50 | >= 1.30 |
| 1-minute volume ratio | >= 1.20 | >= 0.60 |
| Candle body / range | >= 40% | >= 50% |
| Adverse wick / range | <= 60% | <= 50% |
| Maximum selected entries | 2 | 1 |
| Existing ranking rule | max_volume, with core preference | max_liquidity |
| NIFTY opening return | <= -0.05% | not applicable |

Existing EMA alignment, directional confirmation, source validity, exact clock, capital allocation and execution checks continue to apply. The SHORT price threshold retains the existing 65% expansion. Targets are price percentages; costs and leverage are separate. Enabling a setup does not guarantee a fill.

The 09:20 setup requires same-session cash open-to-09:20 price movement and valid opening futures OI evidence. The standard OI interval is 09:16 to 09:20. Existing provisional full-quote support retains its receipt-time and freshness checks. Missing or invalid source data rejects the setup. The already completed NIFTY opening candle is required for both 09:20 SHORT and the existing 09:25 SHORT.

At 09:20, the futures producer now gives the required NIFTY candle the existing bounded retry budget even if every stock candle has already arrived. Optional index contracts do not independently extend retries. The fetch marker distinguishes READY, MISSING_UNIVERSE, MISSING_AFTER_BOUNDED_RETRIES and NOT_REQUIRED; missing NIFTY context still blocks SHORT. Stock readiness and confirmation deadlines are unchanged.

Both setups retain the 10-minute entry expiry, stop tightening to 1.00% after 120 minutes, and 15:15 square-off.

## Configuration and session handoff

This migration first produced production V10 and backtest V12. A concurrent subsequent change on 2026-10-15 advanced the active configurations to production V11 and backtest V13 for the 09:25 LONG five-minute volume threshold of 1.20. Both requested slots and both 1.50% targets are retained. The V10/V12 identities and report receipts below describe the completed two-slot migration, not the newer configuration's report generation.

- Initial production/PAPER/LIVE rules: `V13-V10-G-3-PRODUCTION-V10`, **61 setups**, fingerprint `04697846e334466cf902d7977cefe186c04cfafdb66b4a6582d250405f7b9e7d`.
- Initial daily/historical backtest: `V13-V10-G-3-BACKTEST-TARGET-GT075-RESTORED3-V12`, **51 setups**, fingerprint `2c85fb7c1aebbea8cfe1c5ef9ab2e9f3b22aedb0bd824bc69e1fc468db96c01a`.
- The existing 10 backtest exclusions and three restored low-target setups remain unchanged. Both new setups are included.
- Existing PAPER workers retain V9 for 2026-10-15 so open positions finish under their original contract. Scheduled fresh workers load the current configuration, including both slots, on **2026-10-16**.
- The dashboard may display the pinned, verified V9 identity for exactly 2026-10-15. This is a read-only display exception; execution fingerprint validation remains strict.
- The installed G3 launchers point to this workspace. The operator preview for 2026-10-16 contains both setups and their 1.50% targets.
- Deployment remains `PAPER_VALIDATION`; real-money activation is still disabled. The LIVE rule book has both setups, but this change does not arm trading or submit orders.

The production V9 and backtest V11 JSON files and all saved session evidence remain intact. New historical reports reconstruct both slots from pre-ranking market features and verified minute paths, then rerun execution and portfolio allocation. They do not reuse a filtered list of old selected trades for the new slots. Historical opening source exclusions are disclosed in report provenance; retrospective availability is not proof of live receipt at that time.

## RELIANCE at 10:11 on 2026-10-15

The recorded V9 decision had no configured 10:10 LONG setup. Market features and the 10:11 candle were fetched. The dashboard now separates the missing setup from checks that cannot apply without that setup, instead of presenting eight independent failures.

With the new setup, RELIANCE's recorded body ratio of 17.65% still fails the 50% minimum, and its 52.94% upper-wick ratio exceeds the 50% maximum. Enabling the slot does not retrospectively select RELIANCE or rewrite today's decision.

## Verification

Focused checks cover donor-rule parity, 1.50% targets, preservation of all 59 existing production setups, strict confirmation timing, opening OI/NIFTY rejection, historical candidate reconstruction, archived report identities, dashboard failure semantics, and deployment readiness. Build receipts and operational verification are stored under `.codex_tmp/g3_two_slots_20261015/`.

The refreshed historical run covers 51 sessions through 2026-10-14, with 474 executed trades. Both new slots are present in its selected-order ledger: five `0921_SHORT` orders and nine `1011_LONG` orders, all with a 1.50% target. These counts describe selected orders, not guaranteed fills. The Oct 14 daily replay was separately published from the same verified source generation.

Historical report: `C:/TradingData/eqidv2/backtesting_result_v13_v10_g_3/backtests/runs/2026-10-14/20261015T145055736482_65805609_current_full_history/report.html`.

Current generic selected/history Python entrypoints use the same complete builder as the CLI. The old afternoon generator rejects attempts to label its original 59-setup candidate list as current; archived V7 reports retain their own verifier. G4's separate existing research remains bound to its verified V11 G3 donor and 49-setup inventory.

Dashboard API verification confirmed the new configured V10 identity, all 14 recorded PAPER orders, five then-open positions, and matching V9 evidence for today's session. Only the dashboard process was refreshed; all nine trading and monitoring worker PIDs were unchanged. The configuration JSON files used by this migration retain LF line endings in Git because their integrity checks pin file bytes.

A read-only follow-up at 15:17 IST confirmed all 14 PAPER orders were terminal: 13 CLOSED and one CANCELLED, with no open positions. Their recorded strategy identity remained V9.

Producer verification passed 81 tests plus 13 subtests. Two legacy scheduler assertions expecting retired V6 task names fail identically against the original HEAD producer and the modified producer; they are not regressions from the NIFTY retry change. The comparison is recorded in `opening_nifty_retry_verification.json` in the verification directory.

The separate fixed advisory-exit monitor report reconstructed the 544 selected orders and 474 executions, then correctly refused publication because the concurrent V11/V13 configuration change altered its execution-code proof during the run. No mixed-generation monitor output was published. Its existing saved report remains historical; a fresh monitor report must use a completed, consistent source generation after the concurrent migration finishes.

Final verification against the subsequent V11/V13 configuration passed 41 slot, daily replay, deployment, runtime and display tests. The monitor's historical-path pruning fix passed 44 monitor/source/parameter-search tests, including a later daily order legally reusing the ID of an original order removed from the current profile. Current preflight is ready and both target values remain 1.50%; see `final_configuration_verification.json` in the verification directory. These checks verify the two-slot change, not completion of the concurrent 09:25 LONG historical reselection/report migration.
