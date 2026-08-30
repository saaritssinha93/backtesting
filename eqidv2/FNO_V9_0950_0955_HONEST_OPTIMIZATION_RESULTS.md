# FNO V9 09:50/09:55 honest optimization results

## Decision

**No 09:50 or 09:55 leg qualifies. Keep all four legs disabled.**

The sealed search tested 48 preregistered configurations independently for each
of `09:50 LONG`, `09:50 SHORT`, `09:55 LONG`, and `09:55 SHORT` (192 visible
TRAIN hypotheses). No configuration passed the TRAIN evidence guards. Therefore:

- no leg was selected;
- no four-leg or partial extended-slot book was formed;
- VALIDATION and TEST trade outcomes remained locked and were not evaluated;
- no profit-factor claim is eligible; and
- none of these extended entries should be added to V8-Combined or deployed.

This is a **diagnostic, availability-selected, TRAIN-only result**. It is not a
full-universe backtest and not a production result. The full diagnostic watermark
is:

`EXPLICIT_DIAGNOSTIC_RESEARCH; NOT_QUALIFYING; VALIDATION_AND_TEST_LOCKED; NO_PROMOTION_OR_DEPLOYMENT_CLAIM; TRAIN_DERIVED_RECTANGULAR_PANEL; SOURCE_AVAILABILITY_SELECTED_POPULATION; NOT_FULL_UNIVERSE; LEGACY_LINEAGE_FLAGS_ABSENT; PROSPECTIVE_CLEAN_SOURCE_REQUIRED`

## What was actually tested

| Item | Frozen contract |
|---|---|
| Search run | `search_20260820T235156342242+0530_335057a1588c` |
| Slots and sides | 09:50 LONG, 09:50 SHORT, 09:55 LONG, 09:55 SHORT |
| Selection unit | Each slot-side leg independently; no pooled qualification |
| Grid | 48 configurations per leg; 192 visible leg hypotheses |
| Objective | Maximize TRAIN closed fills subject to every leg guard |
| Base economics | 15 bps cost + 1 bp slippage |
| Stress economics | 20 bps cost + 2 bps slippage |
| Target sizing proxy | Approximately Rs50,000 per filled cash-equity position |
| Entry capacity | Maximum 2 entries per leg/day |
| Confirmation window | Up to minute 4, using the V8 same-session state machine |
| Full source universe | 208 symbols from the static 2026-08-11 universe |
| Diagnostic panel used | 35 symbols, selected by TRAIN source availability (16.83% of the universe) |
| Joint book attempts | 0 |
| Diagnostic portfolio replays | 0 |

The broad candidate cache was deliberately looser than every trial gate: price
move 0.10%, OI change 0.05%, and volume ratio 0.80. This allowed the frozen
trial grid to be evaluated without rebuilding candidates for each configuration.
The four legs were optimized independently so a strong side could not hide a
weak side.

### Chronological split

| Stage | Date range | Sessions | Use in this run |
|---|---|---:|---|
| TRAIN | 2026-05-27 through 2026-07-09 | 30 | Grid search and leg selection only |
| VALIDATION | 2026-07-10 through 2026-07-23 | 10 | Locked; no trade outcomes accessed |
| TEST | 2026-07-24 through 2026-07-31 | 6 | Locked; no trade outcomes accessed |

### Actual implemented evidence guards

Every condition in the applicable row had to pass for an individual leg.

| Guard | TRAIN | One-shot VALIDATION | One-shot TEST |
|---|---:|---:|---:|
| Minimum closed fills | 40 | 15 | 10 |
| Minimum active days | 15 | 8 | 6 |
| Minimum PF | 1.50 | 1.50 | 1.50 |
| Positive additive net | Required | Required | Required |
| Robust PF after excluding best day | >=1.20 | Not required | Not required |
| Maximum top-day share of positive daily gross | 25% | 35% | 45% |
| Positive implemented chronological blocks | At least 2 of 3 | At least 2 of 3 | At least 2 of 3 |
| Stress PF | >=1.00 | >=1.00 | >=1.00 |
| Positive stress net | Required | Required | Required |
| Incomplete candidates/unresolved fills | Must both be zero | Must both be zero | Must both be zero |

VALIDATION could only be opened for a TRAIN-qualified leg. TEST could only be
opened after that same frozen leg passed VALIDATION. Since no TRAIN leg passed,
the later stages correctly remained inaccessible.

### Methodology-conformance discrepancy found in the outcome audit

The sealed implementation does not exactly match the previously stated block
protocol. This is a real methodology defect and is another reason the run must
remain diagnostic:

| Item | Previously stated protocol | What the sealed code actually did | Effect on this result |
|---|---|---|---|
| TRAIN time stability | At least 4 positive non-overlapping five-session blocks out of 6 | Split the 30 TRAIN sessions into 3 contiguous ten-session blocks and required at least 2 positive blocks | No change to rejection: every leg already failed the 40-fill and 15-active-day gates |
| VALIDATION/TEST gates | Stated later-stage protocol did not designate stress, positive-block, and concentration checks as additional gates | Code would also impose stress PF/net, positive-block, and top-day-concentration checks | No outcomes were accessed because TRAIN failed; later stages remained locked |

Thus the results below describe the sealed implementation faithfully, but the
run is not an exact conformance test of the stated protocol. Before any rerun
that could unlock VALIDATION, the block definition and later-stage guard set must
be corrected, explicitly frozen, and tested. The decision is unaffected here:
the per-leg maxima were only 7/6/3/3 fills on 6/4/3/3 active days versus required
minimums of 40 fills and 15 active days.

## Historical-data repair outcome

The exact-grid repair work improved the evidence available for diagnosis, but it
did **not** create a qualifying repaired full-universe snapshot.

### Original snapshot audit, 2026-05-27 through 2026-07-31

| Audit measure | Result |
|---|---:|
| Calendar sessions | 46 |
| Mapped symbols | 208 |
| Expected symbol-sessions | 9,568 |
| Complete symbol-sessions across both source roles | 5,986 (62.56%) |
| Incomplete symbol-sessions | 3,582 (37.44%) |
| Expected bars | 4,305,600 |
| Valid bars | 4,252,937 |
| Missing bars | 43,703 |
| Invalid bars | 8,960 |
| Suspect flat/zero-volume synthetic cash rows | 6,426 |
| Duplicate rows / off-grid rows / mixed-timezone files | 0 / 0 / 0 |
| Repair targets | 52,663 |
| Headline source complete | **No** |

| Source role | Role-symbol-sessions | Complete | Incomplete | Missing rows | Invalid rows | Suspect synthetic rows |
|---|---:|---:|---:|---:|---:|---:|
| NSE equity 1-minute | 9,568 | 8,810 | 758 | 0 | 6,426 | 6,426 |
| NFO futures 5-minute | 9,568 | 6,348 | 3,220 | 43,703 | 2,534 | 0 |

### Repeated provider evidence

| Evidence state | Targets | Share of 52,663 | Meaning |
|---|---:|---:|---|
| API state `CANDLE` | 7,192 | 13.66% | Exact-timestamp response row; not by itself publication-valid |
| Invalid API data | 2,539 | 4.82% | Response existed but did not meet the exact validity contract |
| Verified no candle | 42,932 | 81.52% | Three successful observations found no qualifying candle; this is **not** valid exchange coverage |

The provider returned a `CANDLE` row for all 6,426 cash targets, but direct audit
of those rows found **all 6,426 were still flat OHLC with zero volume**. They
therefore reproduced the suspect morphology rather than repairing it, and the
strict publication check rejected them. Futures evidence contained 766
`CANDLE` states, 2,539 invalid targets, and 42,932 repeated no-candle targets.
There were no provider transport-failure symbols; the blocker was historical
contract availability/validity and unusable returned data, not failed API calls.

The evidence manifest explicitly records:

- `all_targets_evidenced = false`;
- `all_targets_filled = false`;
- `verified_no_candle_is_valid_exchange_coverage = false`; and
- provenance as `RECONSTRUCTED_CURRENT_HISTORICAL_API_RESPONSE_NOT_ORIGINAL_LIVE_AS_OF`.

Consequently, strict publication failed and no sealed publication manifest
exists for the provisional repair directory. The directory is unsealed and must
not be treated as a repaired snapshot. The optimizer provenance instead pins the original physical snapshot
`snapshot_20260820T124734626995+0530_mnofor_c` with fingerprint
`6734204d53315d386a2c3949f01b272d4399a8a1d3b44b9cfd556a1b859190cc`.

After this fail-closed check exposed the classification mismatch, repair policy
v2 was hardened to reject flat zero-volume API rows as
`suspect_api_flat_zero_volume` before they can be recorded as usable repair
candles. The sealed evidence above remains the original v1 diagnostic artifact;
it was not rewritten. This hardening does not change the conclusion because the
v1 publication had already been rejected and the remaining historical absences
still prevent an exact full-universe snapshot.

### Coverage used by the diagnostic search

The optimizer only needs the exact completed cash grid plus futures OI at the
09:45 predecessor and 09:50/09:55 signal clocks. Under this narrower entry-time
contract, 8,277 of 9,568 full-universe symbol-sessions were complete and 1,291
were incomplete. Full-universe exact-grid qualification therefore still failed.

The search fell back to the preregistered `TRAIN_ONLY_SOURCE_COMPLETE_PANEL_V1`:
35 symbols that were complete in TRAIN. Those same symbols had exact grids for
1,050/1,050 TRAIN, 350/350 VALIDATION, and 210/210 TEST symbol-sessions. This
does not cure selection bias: membership was determined by source availability,
and legacy row-lineage flags were absent for all 35 symbols. Each split therefore
has `qualifying_pass = false` even though its narrow exact-grid count passes.

## TRAIN search results

These metrics are independent, pre-portfolio TRAIN leg diagnostics. They are not
portfolio results and must not be summed into a V9 book. PF is an in-sample
selection statistic.

For the “max-trade” representative below, the first priority is the largest fill
count; ties are shown using the highest finite PF, then robust PF. “Best PF” means
the largest finite PF among non-empty trials. It does **not** mean the row passed
the evidence guards. Sparse configurations often produce impressive but
meaningless PFs.

### Search breadth and sample ceiling

| Leg | Trial configs | Distinct observed behaviors | Maximum TRAIN fills | Configs tied at max fills | Fills required | Best possible sample result |
|---|---:|---:|---:|---:|---:|---|
| 09:50 LONG | 48 | 29 | 7 | 6 | 40 | 17.5% of required fills |
| 09:50 SHORT | 48 | 30 | 6 | 3 | 40 | 15.0% of required fills |
| 09:55 LONG | 48 | 18 | 3 | 3 | 40 | 7.5% of required fills |
| 09:55 SHORT | 48 | 10 | 3 | 3 | 40 | 7.5% of required fills |

The sample-count guard alone eliminates every configuration. Later-slot scarcity
also causes many nominally different parameter sets to collapse to identical
behavior.

### Core metrics

| Leg | Diagnostic choice | Fills | Active days | W-L | Win % | PF | Net points | Rs sizing-proxy net | Max DD points |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 09:50 LONG | Max-trade representative | 7 | 6 | 2-5 | 28.57% | 0.987 | -0.075 | +Rs80.41 | 3.460 |
| 09:50 LONG | Highest finite PF | 4 | 4 | 2-2 | 50.00% | 2.468 | +3.383 | +Rs1,768.41 | 1.154 |
| 09:50 SHORT | Max-trade / highest finite PF | 6 | 4 | 0-6 | 0.00% | 0.000 | -3.986 | -Rs1,879.87 | 3.986 |
| 09:55 LONG | Max-trade / highest finite PF | 3 | 3 | 2-1 | 66.67% | 1.458 | +0.529 | +Rs113.51 | 1.155 |
| 09:55 SHORT | Max-trade representative | 3 | 3 | 1-2 | 33.33% | 1.020 | +0.026 | +Rs18.40 | 1.318 |
| 09:55 SHORT | Highest finite PF | 2 | 2 | 1-1 | 50.00% | 3.014 | +0.469 | +Rs235.61 | 0.233 |

“Net points” is the equal-weight additive percentage-point statistic used by the
guards. The rupee column is a separate approximately Rs50,000 position-sizing
proxy; quantity rounding and per-trade weighting can make it differ from the
equal-weight points result.

### Stability, stress, and exact rejection reasons

| Leg | Diagnostic choice | Robust PF ex-best-day | Stress PF | Stress net points | Top-day share | Positive thirds | TRAIN result |
|---|---|---:|---:|---:|---:|---:|---|
| 09:50 LONG | Max-trade | 0.493 | 0.929 | -0.426 | 50.01% | 1/3 | **Fail:** fills, days, PF, robust PF, concentration, blocks, base net, stress PF, stress net |
| 09:50 LONG | Highest PF | 1.233 | 2.323 | +3.182 | 50.02% | 2/3 | **Fail:** only 4 fills, only 4 days, top-day concentration |
| 09:50 SHORT | Max-trade / highest finite PF | 0.000 | 0.000 | -4.297 | N/A; no positive day | 0/3 | **Fail:** fills, days, PF, robust PF, concentration, blocks, base net, stress PF, stress net |
| 09:55 LONG | Max-trade / highest PF | 0.131 | 1.293 | +0.353 | 91.04% | 2/3 | **Fail:** fills, days, PF, robust PF, top-day concentration |
| 09:55 SHORT | Max-trade | 0.000 | 0.913 | -0.123 | 100.00% | 1/3 | **Fail:** fills, days, PF, robust PF, concentration, blocks, stress PF, stress net |
| 09:55 SHORT | Highest PF | 0.000 | 2.170 | +0.344 | 100.00% | 1/3 | **Fail:** only 2 fills, only 2 days, robust PF, concentration, blocks |

All rows passed the local “data clean” execution check within the 35-symbol
panel: zero data-incomplete candidates and zero filled trades lacking terminal
economics. That does not override the panel-level availability and lineage
limitations.

### Representative configurations

Uniform settings in all rows: maximum confirmation minute 4, maximum 2 entries,
no minimum traded-value filter, and no close-location threshold.

| Leg / choice | Gate thresholds: price / OI / volume | 1-minute entry rule | Picker | Morphology and buffer | Stop / target | Config hash |
|---|---|---|---|---|---|---|
| 09:50 LONG max-trade | BROAD: 0.20% / 0.10% / 1.00x | Window directional | Maximum liquidity | Body 0.00, wick 1.00, 0 bps, no midpoint invalidation | 1.0% / 3.0% | `b881a68aefda8f04ae03f29501476c7a3cd23ee0449a8dacfedbd3472195bab3` |
| 09:50 LONG highest PF | MOVE: 0.40% / 0.10% / 1.00x | Window buffer + midpoint | Maximum liquidity | Body 0.40, wick 0.50, 2 bps, midpoint invalidation | 1.0% / 3.0% | `42e075ef90334eda17c408e169ab0d691f5269a7a07e3cef9a531af44884ddb8` |
| 09:50 SHORT max-trade / highest finite PF | BROAD: 0.20% / 0.10% / 1.00x | Window directional | Maximum volume | Body 0.00, wick 1.00, 0 bps, no midpoint invalidation | 0.5% / 1.5% | `8910099dcbb4008fc788a9c9c9c472e0b70db9af1566d74c4453c5ec58aa6db6` |
| 09:55 LONG max-trade / highest PF | BROAD: 0.20% / 0.10% / 1.00x | Window directional | Maximum liquidity | Body 0.00, wick 1.00, 0 bps, no midpoint invalidation | 1.0% / 2.5% | `522e4f5842bc94ca91986200baad5daeae59685e78e5d0d33d12d76a5f5aa6d8` |
| 09:55 SHORT max-trade | BROAD: 0.20% / 0.10% / 1.00x | Window directional | Maximum volume | Body 0.00, wick 1.00, 0 bps, no midpoint invalidation | 0.5% / 1.5% | `00399e73ae1843ab3202878a60ce6a6e20472fa1c4cee3c89d703e6f966d2c7d` |
| 09:55 SHORT highest PF | BROAD: 0.20% / 0.10% / 1.00x | Window strict | Maximum volume | Body 0.40, wick 0.50, 0 bps, no midpoint invalidation | 1.0% / 3.0% | `ac599cdb55bec67991023f6ee10f42b09402a142f86658b510fba12ca22dfb3d` |

The direction-aware price/OI thresholds above are magnitudes; LONG and SHORT
apply the corresponding directional sign. The 09:50 SHORT grid produced no wins
in any non-empty finite-PF behavior, so it has no meaningful “best profitable
PF” configuration. The apparent 2.468 PF for 09:50 LONG and 3.014 PF for 09:55
SHORT are precisely why sample and concentration guards are needed: they rest on
four and two fills respectively.

## Leg status

| Leg | TRAIN-qualified config | VALIDATION | TEST | Final status |
|---|---|---|---|---|
| 09:50 LONG | None | Locked/not run | Locked/not run | Disabled |
| 09:50 SHORT | None | Locked/not run | Locked/not run | Disabled |
| 09:55 LONG | None | Locked/not run | Locked/not run | Disabled |
| 09:55 SHORT | None | Locked/not run | Locked/not run | Disabled |

Selection status is `NO_QUALIFYING_TRAIN_LEGS`; `eligible_for_validation` and
`promotion_eligible` are both false. No selected book, rank, PF claim, or
portfolio metric exists.

## Relationship to V8-Combined

V8-Combined remains the existing 09:25-09:45 paper/shadow baseline. This search
does not alter its ten-leg setup book and supplies no valid 09:50/09:55 addition.

| Item | V8-Combined diagnostic baseline | V9 09:50/09:55 search |
|---|---|---|
| Window | 2026-06-24 to 2026-08-19, 40 sessions | TRAIN 2026-05-27 to 2026-07-09, 30 sessions |
| Economics | 15 bps cost, 0 bps slippage | 15 bps cost, 1 bp slippage |
| Population/EOD | Incomplete 208-symbol source; last-real-bar sensitivity | 35-symbol availability-selected exact-grid panel |
| Portfolio fills | 184 diagnostic fills | No portfolio was formed |
| Win rate | 50.54% diagnostic | N/A |
| PF | 1.892 diagnostic | N/A; no eligible PF claim |
| Net / max DD | +60.16 / 6.00 points diagnostic | N/A |
| Held-out result | 31 fills, PF 1.494 | VALIDATION and TEST locked |
| Status | Paper/shadow research only | All four extended legs disabled |

These columns are context, not an apples-to-apples performance comparison. The
windows, costs, populations, EOD handling, and selection stages differ. In
particular, it would be invalid to add the sparse “highest PF” TRAIN rows to the
V8-Combined result and call the sum V9 performance.

### Final V9 launcher parity replay

The independent `fno_v9_honest_v8_backtest.py` launcher was also replayed on
the same 35-symbol panel for all 46 sessions, using 15 bps cost, 1 bp adverse
slippage, exact 15:30 square-off, and the ten active V8-Combined legs. Because
no 09:50/09:55 leg qualified, an independently cached V8-Combined replay with
identical inputs produced the exact same metrics.

| Metric | V9-Honest | Independent V8-Combined parity run |
|---|---:|---:|
| Candidates | 223 | 223 |
| Closed fills | 57 | 57 |
| Wins / losses | 28 / 29 | 28 / 29 |
| Win rate | 49.12% | 49.12% |
| Diagnostic PF | 1.485 | 1.485 |
| Diagnostic net | +10.852 points | +10.852 points |
| Sizing-proxy net P&L | +Rs5,674.04 | +Rs5,674.04 |
| Maximum cumulative daily drawdown | 7.919 points | 7.919 points |
| TRAIN, 30 sessions | 31 fills, PF 1.643, +8.383 points | Same |
| Later 16 sessions | 26 fills, PF 1.264, +2.469 points | Same |

This exact equality is the expected honest outcome: V9 enabled no new leg. The
official headline fields are still `N/A`, not the diagnostic figures above,
because the ten-leg baseline requires broader early futures/OI coverage than
the optimizer's narrow late-slot panel; 152 of 1,610 panel symbol-sessions were
incomplete under that broader source contract. The run is also availability
selected and lacks certified row lineage.

- V9 run: `fno_v8_vh_20260821T001048354219+0530_d147ed2fc985`
- Validated V9 provenance fingerprint:
  `d147ed2fc985` (full value stored in the run provenance)
- Source-complete engine smoke fingerprint:
  `f960b249b5da7d0f72b0c98b5592f89939e671290d8f875f30d3be954d895677`

## Why this is the honest result

1. The optimizer did not force a PF >=1.50 answer. The highest trade count was
   only 3-7 fills per leg against a 40-fill minimum.
2. High PFs based on two or four trades were rejected by sample size, active-day,
   concentration, and robustness guards.
3. Each LONG/SHORT slot-side leg had to qualify independently; sides were never
   pooled to conceal a weak leg.
4. VALIDATION and TEST stayed untouched after TRAIN failed, preventing repeated
   peeking or retuning on held-out outcomes.
5. The repair evidence distinguishes a returned candle from repeated
   `VERIFIED_NO_CANDLE`; absence was not silently converted into valid coverage.
6. The 35-symbol rectangular panel and missing lineage flags are disclosed as
   diagnostic limitations rather than promoted as a full-universe result.

## Production conclusion and next evidence required

**Production decision: do not enable 09:50 LONG, 09:50 SHORT, 09:55 LONG, or
09:55 SHORT. Keep the extended slots disabled.** V8-Combined itself should also
remain paper/shadow research, not live production, under its existing source and
held-out limitations.

Before another promotion attempt:

1. rebuild May-July history from the contracts that were actually near-month on
   each day, rather than applying 26AUG futures retrospectively;
2. use a point-in-time F&O universe rather than the static 2026-08-11 universe;
3. seal a full-universe exact-grid snapshot with certified row lineage;
4. rerun the preregistered TRAIN search once, then one-shot VALIDATION and TEST
   only for genuinely qualified frozen legs; and
5. after historical gates pass, require at least 20 prospective sessions and 100
   prospective fills before any live-production decision.

Changing filters solely to manufacture 40 fills or PF >=1.50 on this same
availability-selected TRAIN panel would add overfitting, not evidence.

## Artifact lineage

| Artifact | Identifier / path |
|---|---|
| Sealed diagnostic search | `C:\TradingData\eqidv2\fno_oi\strategy_research\v9_0950_0955_honest_v1\optimizer_runs\search_20260820T235156342242+0530_335057a1588c` |
| Search fingerprint | `335057a1588c66762c1d20fceb8690ae132db7308aa100174e8ad0554f02b0c7` |
| Grid family SHA-256 | `071c6040a32877ec8a9e082db2c36239101ae0f43a53afd8d5a6d6292c14ce7f` |
| Candidate-cache fingerprint | `ff5371157ed75cfc31a291e4a035624572fd269d6e4ed8f4f3a5d0a7842a60a5` |
| Original source snapshot | `C:\TradingData\eqidv2\fno_oi\strategy_research\v8_windowed_strict_v1\snapshots\snapshot_20260820T124734626995+0530_mnofor_c\manifest.json` |
| Source snapshot fingerprint | `6734204d53315d386a2c3949f01b272d4399a8a1d3b44b9cfd556a1b859190cc` |
| Repair audit manifest | `C:\TradingData\eqidv2\fno_oi\historical_repair\v9_honest_v1\audits\audit_20260820T230716495187+0530_c8870913ee7a_4b5f1159\manifest.json` |
| Repair audit fingerprint | `c8870913ee7aad064da56ea7da3d73fe0363273cf5900e7c4a7c2806edb7fd04` |
| Provider-evidence manifest | `C:\TradingData\eqidv2\fno_oi\historical_repair\v9_honest_v1\evidence\evidence_20260820T230741905042+0530_c8870913ee7a_d528ef60\manifest.json` |
| Evidence fingerprint | `79ce8188a5327cba3b71d1ca8adcd0ac8859d4bb1aa7dad57d2a9d67272f39fc` |
| V8-Combined literal book SHA-256 | `ee97e86d3689767df95d1bbe8cb71215cf8a0cb40934f4e08d4c0805d90ee675` |
| V8-Combined 15 bps reference run | `fno_v8_vc_20260820T174309351502+0530_af9cdf2ca31b` |
