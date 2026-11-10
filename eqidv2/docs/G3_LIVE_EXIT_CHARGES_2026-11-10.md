# G3 live exit timing and avoidable charges

The 3–10 November 2026 equity P&L export contains INR 590 in broker auto square-off charges (10 charge units at INR 59). G3 previously targeted 15:15 IST. Runtime records show seven rejected exits at approximately 15:12 on November 3, 5 and 6, matching INR 413 of those charges. The remaining INR 177 on November 9 needs broker order history for attribution.

Zerodha's November 9 bulletin specifies 15:12 for CAS cash stocks, which include cash shares eligible for F&O. Its auto square-off charge is INR 50 plus 18% GST per order. Normal brokerage and statutory charges still apply to a self-managed exit.

- [Current broker square-off bulletin](https://zerodha.com/marketintel/bulletin/249809/latest-intraday-leverages-mis-bo-co)
- [Auto square-off fees](https://support.zerodha.com/category/account-opening/resident-individual/ri-charges/articles/auto-square-off)
- [CAS scope](https://support.zerodha.com/category/trading-and-markets/trading-faqs/general/articles/closing-auction-session)

## Implemented behavior

The separate `g3_live_exit_schedule.py` policy applies to G3 LIVE on verified regular NSE sessions:

| Time (IST) | Behavior |
|---|---|
| 15:05 | Stop admitting new entries, cancel outstanding entries and initiate closing remaining positions. An earlier strategy cutoff takes precedence. |
| 15:10 | Require fresh broker position/order evidence and completed local reconciliation. Open positions or working orders produce a degraded reconciliation status. |
| 15:12 | Current broker CAS boundary, retained as visible schedule metadata. |

`fno_v5_live.py` uses the earlier deadline in the existing execution state machine. It retains cancellation acknowledgments, cumulative fills by order ID, residual-only exits, deterministic retry tags, and uncertain-submission handling. It does not convert MIS positions to CNC.

The operator plan displays the live schedule separately and rejects new approval plans from the protective cutoff. Existing launch validation continues to permit an already-authorized protective recovery worker to restart after the entry cutoff.

## Verification evidence

Each successful broker reconciliation reports:

- `live_exit_schedule` and `verification_due`.
- `snapshot_fresh`, requiring the correct session date and reads completed within 60 seconds.
- `broker_nonzero_positions` for attributed NSE MIS symbols; other NSE MIS exposure remains in `unscoped_nonzero_positions`.
- `local_nonzero_positions`, `local_open_state_count`, and active tagged order counts.
- `all_flat`, which requires zero remaining broker exposure, zero unfinished local states, zero working tagged orders, complete scope, matching evidence, and a fresh snapshot.
- `exit_verification`: `VERIFIED_FLAT`, `PENDING`, or `ACTION_REQUIRED`; broker-read failure publishes `UNVERIFIED` and `all_flat=false` in worker health.

Matching open broker/local positions is insufficient for end-of-day success. Missing or malformed position identity/quantity is rejected. Short quantities remain signed. Uncertain submissions and externally closed positions require reconciliation; the patch does not invent exit prices or P&L to make local states appear closed.

## Deployment and research implications

The installed scheduled task points to this workspace's approved launcher. A fresh authorized launch imports the updated runtime and helper. Already-running Python workers do not hot-reload. No broker orders, task changes, process restarts, or authorization changes were performed while preparing this fix.

The historical decision/PAPER contract retains its 15:15 exit, strategy identity, sizing and authorization fingerprints. LIVE now has an explicit earlier protective exit, so historical 15:15 results are not evidence of exact live exit parity. Earlier exits can change gross P&L. The schedule applies to regular sessions only; special sessions require their separately verified schedule.

The 15:05 start provides a buffer, not a guarantee of execution during outages, rejected orders or unresolved fills. Check the live worker health and fresh `all_flat` evidence by 15:10. Future broker cutoff changes require reviewing this explicit schedule.

## Additional cost measures

Removing INR 590 in fees while holding trade P&L unchanged would raise the reviewed period's net from INR 2,213.65 to INR 2,803.65. This is a fee-only counterfactual, not a prediction of earlier-exit returns.

Kite Connect is a monthly INR 500 cost in the reviewed export. Paid users with at least INR 2,000 calendar-month brokerage may receive INR 500 developer credits for the next month's renewal under the current broker program. Account eligibility was not checked and subscriptions were not changed. Do not add trades to reach the threshold or remove data access needed by G3.

- [Official API plans and renewal credits](https://support.zerodha.com/category/trading-and-markets/general-kite/kite-api/articles/what-are-the-charges-for-kite-apis)

The separate flat 5-basis-point cost estimate still understates ordinary costs at INR 50,000 live exposure. Updating that estimate improves reporting and strategy evaluation but does not itself reduce the actual bill; it is outside this exit-fee fix.

## Offline checks

New schedule/operator and runtime tests cover exact cutoff boundaries, aware clocks, wrong dates, regular-session gates, long/short exits, partial entry fills during cancellation, restart without duplicate exits, earlier strategy cutoffs, PAPER behavior, broker-flat verification, malformed broker data and stale snapshots.

Existing partial-fill, runtime, operator, standing-authorization, sizing and observability regressions are also run. Tests use mocked broker interfaces; the new suites prohibit network access.

The combined run completed with **473 passed, 5 subtests passed, and 20 failures**. All 20 failures are in the older live-execution-policy suite, which still expects the previous strategy fingerprint and a removed entry-slot fixture. An isolated comparison using the saved pre-change runtime and the current runtime produced exactly the same failing test IDs (25 passed and 20 failed in each run), with no new failures. Those unrelated stale tests were left unchanged.
