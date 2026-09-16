# Causal NIFTY First-Bar Alignment Ablation

## Design

- Candidate pool: 84 orders = 79 published V13-v3 orders plus 5 contemporaneously rejected 09:25 SHORT orders.
- Context is the completed 09:15-09:20 near-month NIFTY futures bar, already available before each tested signal/confirmation. The value is constant within each session.
- Every extension changes only one slot/side rule on top of the published 09:25 SHORT <= -0.05% gate. Native V3 and published V4 exits are both replayed.
- Selection code physically drops ALL and PSEUDO_TEST columns before ranking. The pseudo-test is still not truly untouched historically because V13-v3 itself was designed after viewing this 25-session history.

## Published gate versus ablation

| Exit | Rule | Dev fills | Dev win % | Dev PF | Dev net % | Pseudo fills | Pseudo win % | Pseudo PF | Pseudo net % |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| V3_NATIVE | current | 62 | 56.452 | 2.498 | 31.424 | 16 | 56.250 | 3.902 | 14.611 |
| V3_NATIVE | gate off | 63 | 55.556 | 2.407 | 30.624 | 20 | 45.000 | 2.386 | 11.411 |
| V4 | current | 62 | 69.355 | 2.625 | 30.809 | 16 | 75.000 | 4.306 | 11.191 |
| V4 | gate off | 63 | 68.254 | 2.427 | 29.259 | 20 | 60.000 | 1.587 | 5.392 |

## Development-frozen reveal set

The following experiment names were frozen from TRAIN+VALIDATION only before pseudo-test metrics were exposed:

- `CURRENT_0925S_REQ_0.050`
- `ABLATE_0925S_OFF`
- `REPLACE_0925S_REQ_0.150`
- `REPLACE_0925S_REQ_0.000`

## Development-only family representatives

| Family | Frozen representative | Kept orders | V3 dev PF | V4 dev PF | Cross-engine/split PF floor |
|---|---|---:|---:|---:|---:|
| 0925_LONG_SYMMETRY | ADD_0925L_REQ_0.075 | 69 | 2.421 | 2.470 | 1.828 |
| 0925_SHORT_THRESHOLD | REPLACE_0925S_REQ_0.150 | 73 | 2.810 | 2.683 | 2.150 |
| ONE_SLOT_SIDE_STRICT | ADD_0930_LONG_REQ_0.050 | 76 | 2.538 | 2.769 | 1.813 |
| ONE_SLOT_SIDE_ANTI_OPPOSITION | ADD_0940_LONG_ANTI_0.100 | 76 | 2.574 | 2.865 | 1.732 |
| ONE_SLOT_SYMMETRIC | ADD_0925_SYMMETRIC_REQ_0.100 | 65 | 2.517 | 2.418 | 2.106 |
| ONE_SLOT_SYMMETRIC_ANTI_OPPOSITION | ADD_0940_SYMMETRIC_ANTI_0.100 | 75 | 2.548 | 2.835 | 1.732 |

## Decision readout

- Turning the gate off adds 5 fills, but all five added trades lose under both exit engines. With V4, all-history fills rise 78->83, while win rate falls 70.513%->66.265%, PF 2.880->2.167, net 42.000%->34.651%, and DD -2.867%->-5.849%.
- The development winner, stricter 09:25 SHORT 0.15%, cuts total orders to 73; on pseudo-test it underperforms current V4 (PF 3.597 vs 4.306, net 8.791% vs 11.191%). Keep 0.05%, do not tighten.
- Adding symmetric 09:25 LONG >=+0.075% is not compelling under V4: all-history fills 68, WR 70.588%, PF 2.635, net 33.732% versus current 78/70.513%/2.880/42.000%.
- Development-only hypotheses `ADD_0930_LONG_REQ_0.050` (V4 all PF 3.022, net 41.554%) and `ADD_0940_LONG_ANTI_0.100` (PF 3.101, net 43.485%) make no different decision in pseudo-test, so that segment provides zero confirmation.
- Global strict symmetric alignment is destructive to count: at 0.05% it keeps only 35 of 84 ungated orders and 30 development fills versus 62 current.

Full exact metrics are in `nifty_firstbar_ablation_all.csv`; the ranking and family-champion tables contain no pseudo-test/all columns. Pseudo-test results for both the core reveal set and development-frozen family representatives are stored separately.

## Interpretation cautions

- Strict alignment gates can improve PF/win rate only by removing trades; they cannot increase trade count. The gate-off ablation is the only tested first-bar change that can restore the five excluded orders.
- Slot/side cells are tiny. Even one-factor rules face a large multiple-testing burden across thresholds and time slots; development winners are hypotheses, not deployable evidence.
- NIFTY alignment may improve win rate when market direction is causally informative, but there is no general guarantee. Require neighborhood stability and genuinely new forward sessions.
