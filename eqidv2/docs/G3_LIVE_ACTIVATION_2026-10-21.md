# G3 LIVE activation — 2026-10-21

This is the date-bound execution handoff explicitly approved for the regular
NSE session on Wednesday, 21 October 2026. It does not approve later sessions.

## Bound execution policy

- Strategy: `V13-V10-G-3-PRODUCTION-V11`
- Capital per admitted trade: INR 10,000
- Leverage / target exposure: 5x / up to INR 50,000 per trade
- Quantity: `floor(50000 / signal trigger price)` whole NSE equity shares
- Maximum simultaneously reserved/open entries: 2
- Maximum admitted entries in the session: 4
- Maximum reserved gross exposure: INR 100,000
- Policy fingerprint: `979556c444f951ea1e79ec4a90b7570b010dc931f2af674aa0849da6c64f3362`

Reservations are written atomically before a broker entry call. LONG and SHORT
workers, including alternate execution namespaces, share the same strict daily
risk book. Corrupt, ambiguous, stale-policy, or unclassified evidence blocks new
entries. Terminal orders release concurrent/gross capacity but continue to count
toward the four-entry daily limit. An uncertain broker submission remains
reserved until reconciled.

## Date-bound scheduler handoff

- One-time approval task: `EQIDV2_g3_live_approve_20261021_0910`
- Trigger: 2026-10-21 09:10 IST
- Catch-up: disabled
- Task retries: disabled
- LIVE target: `EQIDV2_fno_v13_v10_g_3_live_kite_qty1_0915`
- LIVE launch trigger: 09:15 IST; catch-up is disabled when approved

The LIVE target remains disabled and readiness-only before the one-time gate.
The 09:10 gate requires the exact date/time window, same-session successful
authentication, a fresh pre-open report with no failures (the single expected
pre-09:20 marker warning is allowed), no kill switch, no existing G3 LIVE
worker, fresh broker read access, positive funds, supported equity order types,
and zero broker open positions/nonterminal orders. It also rechecks a 09:12:30
enable deadline before any enable operation.

If any check fails, the target remains disabled and no order worker is started.
The activation log is `logs/g3_live_activation_2026-10-21.log`.

Every later trading date still requires a new explicit dated approval and a new
same-session broker/health reconciliation.
