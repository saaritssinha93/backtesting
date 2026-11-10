# V14 OI-day OR and body/wick removal: verification contract

Research-only change requested on 2026-11-03. Keep V14 V1 and the six-field V2 artifacts unchanged. The 12 morning slots, targets, exits, execution costs, risk limits, and minute paths remain the comparison basis.

## Exact entry change to verify

Let `H = 100 × (current same-contract futures OI / first completed 09:20 OI − 1)` and `D = 100 × (current OI / preceding completed OI − 1)`. Evaluate only after the corresponding signal bar completes. Replace the positive per-setup lower OI-Δ threshold with **`H > 1.00 OR D > 0`**. The two branches have separate evidence requirements: `H` needs positive current and 09:20 same-contract OI; `D` needs positive current and exact previous-completed-bar same-contract OI. If one branch's baseline is absent, a valid other branch can still pass. Current OI, dated contract identity, and the evidence used by the passing branch must be available by the signal. Keep the existing OI upper cap if its source is verified. The original OI lower floors are positive for every slot; retaining them as an AND gate would make the proposed OR ineffective.

At 09:20, `H` is necessarily zero. The available `D` is an archived 09:16→09:20 **four-minute** comparison. `D > 0` alone can pass the new gate; missing or stale four-minute evidence fails. Do not impute OI from a later bar. An OI-day value exactly **+1.00%** fails the strict `>1%` limb unless `D>0` passes the other limb.

Remove the signal five-minute minimum **body ratio** and the directional one-minute confirmation maximum **wick ratio** admission gates. The separate special minimum lower wick for 09:25 SHORT is a different inherited guard and must be explicitly reported as retained or removed; the user did not request it removed. Do not quietly alter confirmation volume, EMA, price, Nifty, extension, existing rankers, quotas, targets, stops, or portfolio controls.

## Boundary cases for unit tests

| H (%) | D (%) | Expected OR | Why |
|---:|---:|:---:|---|
| 1.0000 | 0 | Reject | Both strict comparisons fail. |
| 1.0001 | −0.20 | Admit | OI day alone passes, even with falling latest OI. |
| 0.50 | 0.0001 | Admit | Latest OI alone passes. |
| 0.50 | 0 | Reject | Neither passes. |
| 0 | +0.01 | Admit | Valid 09:20 four-minute increase. |
| 0 | −0.01 | Reject | 09:20 day OI is always zero. |
| missing | +0.01 | Admit | Exact latest-bar branch is valid even if opening OI is unavailable. |
| 1.50 | missing | Admit | Same-contract opening/current OI branch is valid even if previous bar is unavailable. |

These expected values refer to the **OR predicate**. A separately retained upper OI-Δ cap can still block a row with missing or excessive `D`; disclose that as a separate veto. Repeat for LONG and SHORT: OI signs are **not** direction-flipped. A candidate below the old body minimum or above the old maximum wick must be eligible when every retained gate passes. A 09:25 SHORT below the inherited minimum *lower* wick stays excluded if that guard is retained. Ranking ties must be deterministic, and selected counts must not exceed each V14 slot/side `max_entries`.

## Backtest controls and comparison

1. Rebuild candidates from the 63-day frozen **full prefilter pool** and the archived 09:40/09:45 supplements for October 28, 29, 30, and November 2. The existing V14 V1 ledger contains only G7-selected opportunities; filtering that ledger cannot test newly admitted stocks.
2. Replay a full-pool unchanged-rule control first. Compare day/slot/symbol selection, fill, exit, and portfolio P&L with archived V14 V1. If exact parity fails, disclose every difference and use the new full-pool control for causal comparisons; label archived V14 V1 as a separate external reference.
3. Use four on-source ablations with identical rank/execution: unchanged full-pool control; OI OR only; body/max-wick removal only; combined. This separates the effect of each request and their interaction. Counts and results must include newly selected winners **and** losers.
4. Choose no parameters from October–November results. Report July–September and October–November separately, plus all 63 sessions, at 5/9/15 bps modeled round-trip cost. Include trade count, win rate, net ₹, profit factor, losing days, worst day, daily-close drawdown, slot/side and day-wise CSVs, selected-order ledger, source exclusions and hashes.
5. Reject any entry before the completed confirmation candle. Recompute fills, stop-first exits, protection, capital allocation and the two-realized-loss admission lock on the entire new selected book. Never reuse the selected-only V14 V1 fills for a newly admitted symbol.

`research/g14_six_feature_prepare.py` provides same-contract OI-day fields, but its `integrity_ok` requires **both** OI branches and official PDC. This OI-only OR must use branch-specific validity, and PDC is irrelevant. Do not silently use V2's `integrity_ok` as a V3 admission gate. Historical source eligibility and some OI receipt timestamps are retrospective; do not claim proven live point-in-time availability for those rows.
