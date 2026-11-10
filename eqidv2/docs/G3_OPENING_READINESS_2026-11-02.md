# G3 opening readiness: verified missing-stock quarantine

## Scope and approval

On 2 November 2026 the user approved: "Yes—skip those stocks and check the rest" for the 09:20 signal / 09:21 confirmation cycle. This changes only opening data-readiness acceptance. It does not relax a stock's entry conditions, targets, stops, selection quota, or execution authorization.

## Recorded cause

The scanner was running before the opening boundary. Cash data was ready at 09:20:10 IST. The final futures marker published at 09:20:24 contained 215 of 218 stock contracts (98.623853% coverage). KARURVYSYA26NOVFUT, KAYNES26NOVFUT and SAIL26NOVFUT each had three recorded attempts and three observations confirming an absent opening candle. There were no API or invalid-data failures. The index contract NIFTYFPI26NOVFUT was also absent; stock coverage excludes index contracts.

The producer's 99% / maximum-two-stock policy correctly left its marker PARTIAL. That prevented the consumer from scanning any stocks before the 09:22:30 deadline. The terminal message incorrectly described this as a scanner restart rather than a readiness wait.

Original evidence is preserved under `C:/TradingData/eqidv2/fno_oi/v13_v10_g_3_live/evidence/2026-11-02/slot_0920/`. No historical signal or order was rewritten.

## Approved behavior

- G3 only; exact 09:20 opening signal only.
- At most three repeatedly verified missing stock candles and at least 98.5% actual stock coverage.
- Missing stocks are excluded from scanning. OI is never fabricated, filled forward, or borrowed from another contract.
- Retain producer identity and policy declarations, dated universe and symbol-partition verification, repeated observation requirements, and zero API/invalid/unverified failure requirements.
- All cash-readiness and exact one-minute confirmation checks remain in place.
- The original producer marker remains PARTIAL. A completed eligible-stock scan may report SUCCESS with separate `opening_readiness` metadata recording degradation, quarantined symbols, source marker identity, and observed coverage.
- All later slots and other profiles retain the existing readiness rules.
- The 90-second confirmation publication deadline is unchanged. Late starts and late scan completions remain blocked.
- Future deadline failures record the last readiness blocker rather than incorrectly attributing every timeout to a restart.

## Activation and rollback

`OPENING_VERIFIED_NO_CANDLE_QUARANTINE_ENABLED = True` in `fno_v13_v10_g_3_live_config.py` enables the exception for newly started workers. The existing daily scheduled scanner loads this repository at startup. No running trading worker was restarted for this change; no past opening order was submitted. The next normal opening is 3 November 2026, subject to the usual scheduler, data and authorization checks.

Rollback: set only that flag to `False` and allow the next normal worker startup to load it. Do not reset or discard unrelated changes in the working tree.

This fix prevents the specific verified-three-absence condition from blocking healthy stocks. It does not guarantee opening data availability, a qualifying trade, a timely fill, or profitability.

## Verification

- 225 offline tests passed (180 existing regression tests plus 45 quarantine tests) across G3 runtime and opening-OI tests, shared V6 runtime tests, and opening Nifty fetch-retry tests.
- The archived 09:20:24 marker and its matching dated universe were checked in memory: the original strict rule rejects it; the new opening-only rule accepts the 215 healthy stocks and returns exactly the three quarantined stock contracts. The original marker was unchanged.
- Tests retain rejection for four missing stocks, sub-98.5% coverage, unverified absences, API/invalid-data failures, broken identities/partitions, missing cash data, missing confirmation, and expired publication deadlines.
