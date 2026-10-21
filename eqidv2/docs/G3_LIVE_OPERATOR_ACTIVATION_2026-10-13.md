# G3 V7 LIVE activation: operator handoff

The current production configuration contains 59 G3 V7 setups: the prior 44
plus exactly the fifteen approved afternoon additions. Historical and daily
backtesting, PAPER, and LIVE configuration share
`configs/strategies/v13_v10_g_3_production_v7.json`. The LIVE sizing policy
remains Rs10,000 per trade at 5x exposure. The new targets and filters are
copied from the selected G4 result; the prior 44 setup rules are preserved.

The activation command has been prepared and tested. **It has not been applied
to the real deployment.** The current deployment remains PAPER_VALIDATION and
the real LIVE scheduled task remains disabled. No broker orders were submitted.
The actual October 14 readiness invocation returned `READY_DISARMED` under
V7, with zero signals, order states, workers or broker requests. The existing
dated operator step below enables real-money execution; it was not run as
part of this configuration promotion.

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

1. Verify the canonical strategy, all 59 setup IDs, LIVE sizing fingerprint,
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

The command leaves PAPER and daily-backtest tasks in place. The 59 configured
setups span 40 signal clocks. The latest signal is 13:30 LONG, confirmation
13:31; its publication grace ends 13:32:30 and the data/confirmation pipeline
deadline is 13:33. Stops remain 1.25% initially, tightening to 1.00% after
120 minutes from actual entry, with ten-minute entry expiry and 15:15 square-off.

The existing PAPER/feed tasks remain enabled for 09:15, the fast production
futures feed for 09:05, and opening-history recovery for 17:00. The installed
daily/history task runs at 17:15; the `_1620` launcher filename is retained
for compatibility. The captured 19 task enabled states, triggers and actions
are unchanged. The LIVE task is disabled and no dated LIVE arm exists.

## Verification and recovery

Read-only checks on October 13 passed for all eight configured broker apps:
zero open net positions, zero pending orders and positive funds. Read access
does not prove order acceptance, static-IP order permission or the margin
available for a future trade; those were not tested with real orders.

Current promotion checks and result reconciliation are recorded in
[G3 afternoon production notes](G3_AFTERNOON_FIFTEEN_PRODUCTION_2026-10-13.md).
The current local preflight and October 14 plan are recorded under
`.codex_tmp/g3_afternoon_ops_20261013/`; they enumerate all 59 rules and the
fifteen additions under strategy fingerprint
`68e842bb99002c950304cc40a41aa40e93a6394b131199ade872ec5e852a281f`.
The operator-script success and failure paths were
executed only with fake scheduler/Python functions and disposable files.
Tests cover dated/fingerprint mismatch, omitted setups, emergency switches,
stale reconciliation, pending orders, sizing preservation and failure rollback.

The separate options ATM feed had a pre-existing October 13 supervisor
failure after stale-heartbeat restarts (`restart_count=21`, `max=20`). Equity
configuration readiness passes; this handoff does not attest options-feed
health or successful future broker authentication. The previous daily task
exit 2 belongs to its old 16:20 execution; the installed trigger is now 17:15
after opening-history recovery, and the later manual launcher completed
successfully.

Apply backups are stored in `.codex_tmp/g3_live_activation_<timestamp>/`.
If registration/approval/enabling fails, the wrapper restores the deployment,
arm and original task XML. It does not delete position or broker state.

Once orders exist, stopping new entries and managing existing positions must
be handled separately. Do not restore an old strategy's files over active
positions or stop its protective-order manager merely to revert approval.

Prepared files: `tools/g3_live_operator.py`, `bat/approve_g3_live.ps1`,
`bat/run_g3_approved_live.bat`, `tests/test_g3_live_operator.py`.
