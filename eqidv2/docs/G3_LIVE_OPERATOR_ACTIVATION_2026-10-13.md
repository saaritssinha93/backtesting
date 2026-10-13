# G3 V5 LIVE activation: operator handoff

The user's authorization covers all 32 current G3 V5 setups, including the previous six
late additions and the selected ten additions. The LIVE sizing policy remains Rs10,000 per trade at 5x
exposure. Targets, stops, ranking, entry expiry and square-off are unchanged.

The activation command has been prepared and tested. **It has not been applied
to the real deployment.** The current deployment remains PAPER_VALIDATION and
the real LIVE scheduled task remains disabled. No broker orders were submitted.
The operator must perform the step that enables real-money execution.

## Command

From the repository terminal, for October 14:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File bat/approve_g3_live.ps1 -SessionDate 2026-10-14 -Apply
```

Use the intended trading date; omitting `-SessionDate` selects the current
India trading date. Run before the existing 09:15 start. Removing `-Apply`
shows a read-only plan. The command does not start a task immediately. If
09:15 has already passed, start the named task yourself from Task Scheduler
after reviewing the approved session and current account state.

A separate dated approval is required for each session. There is no automatic
rearming. Approval for October 14 does not authorize October 13 or October 15.

## What applying does

1. Verify the canonical strategy, all 32 setup IDs, LIVE sizing fingerprint,
   requested regular trading date and existing emergency switches.
2. Refuse to change a running G3 LIVE task or detected G3 LIVE workers.
3. Repeat read-only broker checks. All configured apps must have verified
   access and equity capabilities, positive funds, zero net open positions
   and zero pending orders. The report must be less than 60 seconds old.
4. Back up the current deployment, arm record and exact scheduled-task XML.
5. Register the existing task disabled with the approved launcher, retaining
   its weekday trigger and user. Persist `LIVE_APPROVED` plus a dated approval
   and arm bound to both strategy and execution-policy fingerprints.
6. Enable `EQIDV2_fno_v13_v10_g_3_live_kite_qty1_0915` for its normal 09:15
   trigger. On launch, recheck the current date, every setup, both fingerprints
   and emergency switches before passing the acknowledgement only to the
   existing supervised LIVE coordinator.

The operator's Apply explicitly accepts proceeding without three validated current-profile
PAPER sessions. This is recorded as `OPERATOR_OVERRIDE_NOT_ATTESTED`; the script
does not invent successful PAPER sessions or change the fitted historical
results. Source readiness, account ownership, broker reconciliation, sizing,
stop management and dated arming checks remain in the execution engine.

The command leaves PAPER and daily-backtest tasks in place. Only the selected
32 setups are enabled. The latest signal is 11:25 SHORT, confirmation 11:26;
the data/confirmation pipeline deadline is 11:28.

## Verification and recovery

Read-only checks on October 13 passed for all eight configured broker apps:
zero open net positions, zero pending orders and positive funds. Read access
does not prove order acceptance, static-IP order permission or the margin
available for a future trade; those were not tested with real orders.

Current promotion checks and result reconciliation are recorded in
`docs/G3_SELECTED_TEN_PRODUCTION_2026-10-13.md`. The operator-script success and failure paths were
executed only with fake scheduler/Python functions and disposable files.
Tests cover dated/fingerprint mismatch, omitted setups, emergency switches,
stale reconciliation, pending orders, sizing preservation and failure rollback.

Apply backups are stored in `.codex_tmp/g3_live_activation_<timestamp>/`.
If registration/approval/enabling fails, the wrapper restores the deployment,
arm and original task XML. It does not delete position or broker state.

Once orders exist, stopping new entries and managing existing positions must
be handled separately. Do not restore an old strategy's files over active
positions or stop its protective-order manager merely to revert approval.

Prepared files: `tools/g3_live_operator.py`, `bat/approve_g3_live.ps1`,
`bat/run_g3_approved_live.bat`, `tests/test_g3_live_operator.py`.
