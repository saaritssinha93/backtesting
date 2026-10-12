# G3 issue fixes — 12 October 2026

## Scanner rejected every base candidate

The G3 scanner passed a joined market-data row with a `ts` field into a decision contract requiring `signal_timestamp`. This caused candidate rejection even when the recorded base filters passed. The runtime adapter now maps the already selected slot timestamp to the expected field. The decision contract, thresholds and accepted backtest fingerprint are unchanged.

The saved 09:25 packet contains zero candidates despite five LONG base-pass rows; the 09:30 packet contains zero candidates despite fifteen LONG base-pass rows. These are affected confirmation candidates, not proven missed trades. The completed packets were preserved. This morning's PAPER record must not be treated as an uninterrupted, fully validated session.

The scanner was reloaded in its existing PAPER task at **09:32:35 IST**. An offline recovery test verifies that completed 09:25/09:30 packets remain unchanged and the next eligible slot is 09:35. No late entry or replacement historical packet was manufactured.

## Options PAPER identity warnings

The four options PAPER workers did not include `strategy_fingerprint` in their status/heartbeat publications. The dashboard incorrectly classified missing evidence as a fingerprint mismatch while displaying the correct strategy version.

New publications contain the source equity fingerprint. Older publications without a fingerprint remain explicitly unverified (`DECLARED_MATCH` / `NOT_PUBLISHED`) when their exact versions, PAPER mode, role and declared options risk settings match. A supplied wrong fingerprint remains a mismatch. The options lot size and trading logic are unchanged.

The four existing options PAPER tasks were reloaded at **09:32:36–09:32:40 IST**. Authenticated dashboard reads verified `RUNNING`, `strategy_identity_state: MATCH`, and `fingerprint_evidence: MATCH` for all four workers.

## Manual test error handling

The manual order tool now records an interrupted submission wait as `UNKNOWN_ACCEPTANCE`, preserves the consumed intent, and never automatically retries the broker request. A broken console or result-write failure after submission cannot be misreported as a setup failure with no submission. Preview HTTP protocol failures produce a sanitized error before a writer is created. All regression tests use fake transports; no real order was submitted.

## Verification and deployment

- Combined isolated checks: **542 runtime/dashboard/manual-tool tests and 116 API/observability tests passed**. One unchanged archived selection oracle was excluded from this combined run.
- Both dashboards restarted at 09:32 IST. LIVE remains disabled, with the user-selected ₹10,000 capital × 5 policy recorded for future authorized operation.
- Only the scanner and four options PAPER tasks were reloaded. No task was enabled, no LIVE worker was started, and no arm/kill controls were changed.
- Reload and verification artifacts are in `.codex_tmp/g3_migration_plan_20261011/`: `paper_service_fix_reload.json`, `fix_issues_dashboard_identity.json`, and `scanner_pre_fix_slots.json`.

### Observed after deployment

The 09:35 scan completed successfully at **09:35:41 IST**, publishing **8 candidates: 4 LONG and 4 SHORT**. Hash verification confirmed the earlier 09:25/09:30 packets were unchanged. The 09:36 confirmation received all eight completed one-minute bars and finished with zero errors. One candidate failed the direction rule and seven failed their required volume thresholds; no signal was selected for entry. The observed evidence is saved in `scanner_fix_operational_check.json` beside the other verification artifacts.
