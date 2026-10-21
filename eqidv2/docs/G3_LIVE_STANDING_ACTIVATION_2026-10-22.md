# G3 LIVE authorization from 2026-10-22

The operator authorization is bound to `V13-V10-G-3-PRODUCTION-V11` and its
ordered 61-setup set. October 22 uses a one-time explicit gate. The reviewed
standing policy becomes effective on October 23 and may mint only a same-day
approval on a verified regular NSE session.

## Per-trade sizing and portfolio admissions

- Capital reserved per admitted trade: INR 10,000
- Target exposure: up to INR 50,000 per trade (5x)
- Quantity: `floor(50000 / signal trigger price)` whole NSE equity shares
- Concurrent reserved/open entry limit: unlimited (`null` in the bound policy)
- Admissions-per-session limit: unlimited (`null` in the bound policy)
- Aggregate reserved-gross-exposure limit: unlimited (`null` in the bound policy)
- Required available live balance at approval and launch: at least INR 20,000
- Execution-policy fingerprint: `cc9210d610252534af34ee812407ba8bb46546ec466b35e09388bb4815eadc7e`

The INR 20,000 readiness check is an independent operational preflight, not a
portfolio-admission or aggregate-capital limit. It does not persist the actual
broker balance and is not an instrument-specific margin quote. Available funds,
actual MIS margin and other broker/exchange rules may still reject an entry.

## Scheduler handoff

- October 22 gate: `EQIDV2_g3_live_approve_20261022_0910`, once at 09:10 IST
- Standing gate: `EQIDV2_g3_live_standing_approval_0910`, weekdays at 09:10 IST,
  beginning October 23
- LIVE target: `EQIDV2_fno_v13_v10_g_3_live_kite_qty1_0915`, weekdays at 09:15 IST
- Catch-up and retries: disabled
- Overlap policy: `IgnoreNew`
- Principal: interactive user, limited run level

The standing gate disables the 09:15 target before evaluation and enables it
only as the final commit. It never starts LIVE itself. It requires a same-day
successful eight-app authentication, a freshly generated pre-open health
report, a verified regular-session calendar, clear kill switches, no existing
G3 LIVE process, HTTPS clock skew within 30 seconds, exactly eight readable and
flat broker apps, no nonterminal broker orders, the required equity/MIS/order
capabilities, and at least INR 20,000 available live balance. A failure leaves
the target disabled and restores the prior profile and arm if approval writes
had begun.

At 09:15 the approved launcher revalidates the dated profile and arm, refreshes
the exact eight-app read-only broker reconciliation again, requires the account
to remain flat with no pending orders, rereads unchanged authorization files,
and only then starts the existing supervised coordinator. This closes the
five-minute approval-to-launch evidence gap.

The recurring task requires the machine to be on and the configured user to be
signed in. Missing authentication, stale data, broker/API failure, a holiday or
unverified calendar, insufficient available balance, a kill switch, clock
drift, or any identity/policy change means no LIVE launch. Calendar years not
installed and verified also fail closed.

## PAPER relationship

PAPER and LIVE consume the same canonical G3 signal IDs and strategy
fingerprint. Their sizing, order IDs, fills, positions, and runtime roots remain
separate. PAPER decisions are an input/evidence path; PAPER fills are never
copied into LIVE.

The standing policy is
`configs/authorizations/g3_live_standing_policy_v1.json`, policy ID
`G3_LIVE_STANDING_20261023_V1`. Setting it disabled or revoked, changing its
fingerprint-bound strategy/setup/risk fields, or disabling either scheduler
task prevents future automatic approvals.
