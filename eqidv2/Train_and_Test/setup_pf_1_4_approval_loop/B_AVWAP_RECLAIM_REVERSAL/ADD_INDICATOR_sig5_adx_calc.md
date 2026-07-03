# ADD_INDICATOR — B_AVWAP_RECLAIM_REVERSAL: add `sig5_adx_calc >= X` to the anchor config

Net of cost @ 15 bps/leg. Each row ADDS only `sig5_adx_calc>=threshold` to the anchor; everything else fixed. score = min(FIT_PF,VAL_PF) (−1 if a fold < 6 trades). ✓ on TRAIN = inside [1.30,1.70]. TEST shown for reference only.

- TRAIN 2026-05-18..2026-06-16 (20) · TEST 2026-06-22..2026-06-24 (2)  | feature `sig5_adx_calc` is a pre-momentum term

| added condition | FIT n/PF | VAL n/PF | TRAIN n/PF | TEST n/PF | score |
|---|---|---|---|---|---|
| (anchor — no sig5_adx_calc) *(anchor)* | 11/1.586 | 19/1.382 | 30/1.436✓ | 4/0.441 | 1.382 |
| sig5_adx_calc>=12 ✅best | 9/1.351 | 18/1.563 | 27/1.501✓ | 4/0.441 | 1.351 |
| sig5_adx_calc>=12.3534 | 9/1.351 | 18/1.563 | 27/1.501✓ | 4/0.441 | 1.351 |
| sig5_adx_calc>=14.853 | 9/1.351 | 15/1.234 | 24/1.271 | 4/0.441 | 1.234 |
| sig5_adx_calc>=15 | 9/1.351 | 15/1.234 | 24/1.271 | 4/0.441 | 1.234 |
| sig5_adx_calc>=16.5331 | 7/1.357 | 12/0.97 | 19/1.073 | 3/0.0 | 0.97 |
| sig5_adx_calc>=18 | 7/1.357 | 12/0.97 | 19/1.073 | 3/0.0 | 0.97 |
| sig5_adx_calc>=18.1111 | 7/1.357 | 12/0.97 | 19/1.073 | 3/0.0 | 0.97 |
| sig5_adx_calc>=20 | 7/1.357 | 10/0.789 | 17/0.941 | 1/0.0 | 0.789 |
| sig5_adx_calc>=20.2498 | 7/1.357 | 9/0.458 | 16/0.699 | 1/0.0 | 0.458 |
| sig5_adx_calc>=22 | 7/1.357 | 9/0.458 | 16/0.699 | 1/0.0 | 0.458 |
| sig5_adx_calc>=22.3463 | 7/1.357 | 9/0.458 | 16/0.699 | 1/0.0 | 0.458 |
| sig5_adx_calc>=24.6429 | 6/1.56 | 8/0.554 | 14/0.834 | 1/0.0 | 0.554 |
| sig5_adx_calc>=25 | 6/1.56 | 6/0.048 | 12/0.527 | 1/0.0 | 0.048 |
| sig5_adx_calc>=27.7712 | 4/1.199 | 3/0.19 | 7/0.736 | 1/0.0 | -1.0 |
| sig5_adx_calc>=28 | 4/1.199 | 3/0.19 | 7/0.736 | 1/0.0 | -1.0 |
| sig5_adx_calc>=30 | 2/0.554 | 1/0.0 | 3/0.277 | 0/0.0 | -1.0 |
| sig5_adx_calc>=32.7251 | 1/0.0 | 1/0.0 | 2/0.0 | 0/0.0 | -1.0 |
| sig5_adx_calc>=33 | 1/0.0 | 1/0.0 | 2/0.0 | 0/0.0 | -1.0 |
| sig5_adx_calc>=35 | 1/0.0 | 1/0.0 | 2/0.0 | 0/0.0 | -1.0 |
| sig5_adx_calc>=40 | 1/0.0 | 0/0.0 | 1/0.0 | 0/0.0 | -1.0 |

## Verdict
- anchor score 1.382 (TEST PF 0.441, n 4).
- best `sig5_adx_calc>=12`: score 1.351, TRAIN n27 PF1.501 (in-band), **TEST n4 PF0.441**.
- adding `sig5_adx_calc` does NOT improve the FIT/VAL score; TEST PF goes 0.441 → 0.441 on n 4 → 4.
- new config written to `config_with_sig5_adx_calc.json` (for the coordinate re-sweep).