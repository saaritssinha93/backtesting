# G3 LIVE standing reactivation for 2026-10-29

The October 23 standing gate succeeded under the then-current V12 strategy.
The October 23 migration disarmed LIVE, and the October 26–28 gates rejected a
disabled V13 policy. The October 29 V3 standing policy now binds the current
`V13-V10-G-3-PRODUCTION-V15` strategy and its exact ordered 34-setup set.
The historical V1 and disabled V2 artifacts remain separate.

The existing weekday 09:10 IST task `EQIDV2_g3_live_standing_approval_0910`
uses `bat/activate_g3_live_standing.ps1`. It disables the 09:15 LIVE target
first, then requires the exact policy and strategy, a verified regular NSE
session, successful same-day authentication, pre-open health, a clock check,
clear kill switches, no G3 LIVE worker, and a fresh read-only reconciliation
of eight broker apps. Each app must have no open net position or nonterminal
order, required permissions, and at least INR 20,000 available balance. Only
after the dated profile and arm are checked does the gate enable the 09:15
target. The gate never starts the target or submits an order.

The weekday 09:15 IST task `EQIDV2_fno_v13_v10_g_3_live_kite_qty1_0915`
still invokes `bat/run_g3_approved_live.bat`. It rechecks the dated approval,
arm, policy, kill switches, and fresh broker state before starting the
supervised LIVE coordinator. The target stays disabled until a successful
same-day gate. There is no retry or catch-up if a trigger is missed.

This reactivation follows the October 23 standing-policy behavior: the
three-session current-profile PAPER validation is **not attested** and is
explicitly overridden in each dated LIVE approval. The reviewed sizing is
INR 10,000 capital and up to INR 50,000 target exposure per trade at 5x;
local concurrent-position, daily-entry, and aggregate-exposure limits are
unbounded. The INR 20,000 check is a broker-readiness floor, not a portfolio
cap. Broker and exchange rejection or loss remains possible.

To stop subsequent automatic authorization, disable the 09:10 standing task
or set the current V3 policy to `enabled: false`, then disable the 09:15
target. The usual G3 LIVE kill switch remains available for an active
session. Do not run the standing gate manually outside its 09:09:00–09:12:29
IST window: its first action disables the target.
