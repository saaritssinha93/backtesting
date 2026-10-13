# G3 V5: selected-ten promotion for October 14, 2026

Normal historical backtests, daily backtests, PAPER and LIVE configuration now share `V13-V10-G-3-PRODUCTION-V5`: the existing 22 setups plus exactly ten selected additions. Other proposed times/sides are absent. The production configuration is `configs/strategies/v13_v10_g_3_production_v5.json`.

**Operational state:** PAPER and daily backtest tasks are enabled for October 14. LIVE configuration is prepared and its readiness-only coordinator passed, but real-money execution is **not activated**. The deployment remains `PAPER_VALIDATION`; the LIVE task is disabled. No arm, emergency switch, broker order or PAPER order was changed.

## Exact additions

| Signal | Side | Confirmation | Target % | Historical trades | Win rate % | Net P&L Rs |
|---|---|---|---:|---:|---:|---:|
| 10:35 | LONG | 10:36 | 2.12 | 7 | 71.43 | 20,455.89 |
| 10:35 | SHORT | 10:36 | 2.86 | 12 | 75.00 | 32,393.71 |
| 10:40 | LONG | 10:41 | 1.71 | 5 | 80.00 | 13,863.01 |
| 10:45 | LONG | 10:46 | 0.79 | 5 | 100.00 | 18,500.00 |
| 10:45 | SHORT | 10:46 | 1.01 | 10 | 80.00 | 24,281.33 |
| 10:50 | LONG | 10:51 | 0.77 | 18 | 72.22 | 29,830.24 |
| 10:55 | SHORT | 10:56 | 1.53 | 16 | 75.00 | 59,670.35 |
| 11:10 | LONG | 11:11 | 1.76 | 5 | 80.00 | 17,684.65 |
| 11:10 | SHORT | 11:11 | 1.84 | 8 | 75.00 | 22,494.20 |
| 11:25 | SHORT | 11:26 | 4.20 | 14 | 71.43 | 58,577.17 |

Filters and targets match the previously selected research configuration exactly. Stops remain 1.25%, tightening to 1% after 120 minutes. Each addition permits one highest-liquidity qualifying stock per session, exact next-minute confirmation, a ten-minute trigger expiry and 15:15 square-off. Effective SHORT price thresholds remain 0.13%; no second legacy multiplier is applied.

PAPER/backtest sizing remains Rs100,000 margin per entry, 5x exposure (Rs500,000), Rs1,000,000 portfolio capital and 5 bps round-trip cost. LIVE remains Rs10,000 margin per entry, 5x target exposure (Rs50,000), whole-share floor sizing and existing broker/portfolio guards. Historical rupee P&L is not a LIVE P&L estimate.

## Configuration and runtime checks

- Strategy fingerprint: `fcb7c981b518b8c9e427155cdfb06e3cc34c6ed16525e78514e6ecf3afab2aea`.
- LIVE sizing fingerprint: `ec621fc330b7f31bb82964387987822ef44e2923a8b03deb24cad643fe23c702`.
- The 32 setups span 22 unique signal clocks. Last signal: 11:25 SHORT; exact confirmation: 11:26; publication grace ends 11:27:30. Scanner/feed/confirmation cutoff now derives from those clocks and is 11:28.
- The dashboard, API profile registry, execution/strategy analysis, scanner and confirmation health checks use the current identity and clocks. Prior V4 and offline selected-ten packages remain immutable and archived.
- Batch launchers and session wrappers route to G3 dynamically. PAPER remains explicitly PAPER; LIVE retains dated acknowledgement, strategy/policy fingerprints, entry ownership, broker reconciliation and emergency controls.
- Main dashboard 8787 and flow dashboard 8790 were restarted and verified by authenticated HTTP API. Duplicate main-dashboard processes were removed; the original batch supervisor retains ownership of the reloaded main server. Both show the same V5 full-history result; the daily selection independently shows October 13. Tomorrow's PAPER/LIVE cockpit correctly has no trading evidence yet.
- Eight broker applications passed read-only access checks: positive funds, zero open net positions and zero pending orders. No orders were submitted. Read access does not establish order acceptance; tomorrow's authentication/funds/readiness are checked again by the existing launch gates.

## October 14 schedule (IST)

| Session | Time | State |
|---|---|---|
| Existing broker authentication | 08:30 | Enabled |
| Dated universe | 08:50 | Enabled |
| Opening OI observer | 08:55 | Enabled |
| Futures OI feed | 09:05 | Enabled |
| G3 scanner, 1m feed, confirmation, PAPER entries/loggers/reports | 09:15 | Enabled |
| G3 real-money LIVE coordinator | 09:15 | Disabled; operator activation required |
| Opening-history recovery | 17:00 | Enabled |
| Daily backtest, then full-history refresh and analysis | 17:15 | Enabled |

The daily task/launcher retains its historical `_1620` filename for compatibility; its actual installed trigger is 17:15. This promotion did not change any scheduled-task trigger or enable LIVE.

## Verified results

| Scope | Sessions | Trades | Wins / losses | Win rate | Net P&L Rs |
|---|---:|---:|---|---:|---:|
| Full G3 | 50 | 257 | 192 / 65 | 74.71% | 743,590.44 |
| Ten added setups | 50 | 100 | 76 / 24 | 76.00% | 297,750.56 |
| October 13 daily | 1 | 6 | 4 / 2 | 66.67% | 9,968.26 |

The full period is July 29 through October 13; October 1 is excluded because immutable source input was incomplete. Maximum daily realized drawdown is Rs15,750. The first 49 sessions use the sealed historical cohort with retrospective opening OI; October 13 was rebuilt from raw inputs. Targets were chosen after inspecting the historical cohort. These are fitted historical results, not a prospective 70% guarantee.

Full result directory: `C:\TradingData\eqidv2\backtesting_result_v13_v10_g_3\backtests\runs\2026-10-13\20261013T183246545061_8115bc95_current_full_history`. Its `report.html` links all 32 setup results, daily/monthly/period tables, the complete trade ledger, config and manifest.

All 280 selected orders matched the previous offline 32-setup result across 302 comparable non-identity columns, including entry/exit, target/stop and portfolio P&L. The existing 22 setups retained their rules and outcomes. All ten additions individually remain at least 70% in this historical sample.

## Validation evidence

- 220 core contract/runtime/history/operator tests passed, including all ten added PAPER fills and LIVE order configurations, exact timing, quotas and sizing. Follow-up changed-area suite: 97 passed.
- Dashboard/feed/options/execution analysis integration: 346 passed initially; the one outdated V2-subset expectation was corrected and its complete 13-test suite passed.
- API and monitoring: 88 passed with the API virtual environment, explicit legacy G for legacy fixtures and explicit G3 for current-profile cases. Includes missed 10:35/11:25 slots and rejection of old terminal slot counts.
- Dashboard JavaScript: 31 passed. Other route/cockpit/installer checks: 78 passed; the same outdated subset expectation above was resolved.
- Final local preflight: deployment, API registry, all 32 rules, shared backtest/runtime config and required launchers match. Current daily, cumulative and full-history pointers verified; report links and accounting verified.

Evidence and reversible backups: `.codex_tmp/g3_selected_ten_production_20261013/`. Relevant receipts: `validation.json`, `preflight.json`, `tasks_20261014.json`, `dashboard_validation.json`, `live_plan_20261014.json`, `broker_readiness.log`, `live_readiness.log` and the test logs. Broker read-only report: `logs/kite_order_readiness/latest.json`.

## LIVE operator handoff

The remaining step enables real-money automation and must be performed by the operator. The prepared command for October 14, before 09:15, is:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File bat/approve_g3_live.ps1 -SessionDate 2026-10-14 -Apply
```

Without `-Apply`, the same command only displays the verified plan. Applying rechecks all 32 setup IDs, strategy and sizing identities, calendar, emergency switches and fresh broker reconciliation before writing dated approval and enabling the task. It does not start the task immediately. There is no automatic daily rearming. The operator path explicitly records any override of the three-session PAPER validation requirement; it does not invent validation evidence. Details: `docs/G3_LIVE_OPERATOR_ACTIVATION_2026-10-13.md`.

Backups contain prior code/configuration, result pointers, control hashes and task XML. Restore only deliberately while workers are stopped and account/position ownership is reviewed; do not overwrite historical ledgers or active position state.
