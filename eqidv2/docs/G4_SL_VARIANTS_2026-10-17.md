# G-4 separate and combined protective stops

The research runner `research/g4_separate_sl_variants.py` compares three optional protective stops against the native exit on the same pinned historical trades. The original targets remain in effect.

| Variant | Rule after activation |
| --- | --- |
| Profit floor | Resting stop at +0.20% gross profit from entry |
| ATR trailing | Stop 1.5 ATR behind the best completed close, without a profit floor |
| Combined | Tighter of the +0.20% floor and the 1.5 ATR trailing stop |

All three activate when a completed post-entry close reaches `max(0.40%, 90% of the trade's target)` in favourable price movement. A newly armed or tightened stop becomes effective on the next candle, and stops can only tighten. LONG and SHORT directions are handled symmetrically. Native protection starts at 1.20% and tightens to 1.00% after 120 minutes. Existing target and session-end exits still apply. A gap through a stop uses the observed open; same-candle stop/target ambiguity uses stop-first handling.

The pinned comparison covers the previously used 52 sessions from 29 July through 15 October 2026 and 466 executed trades, using the archived V12 source. It is not a replay of the latest V13 configuration. The primary comparison holds those 466 executions fixed. A separate full portfolio replay reapplies the original capital allocator to all 533 selected orders, allowing earlier exits to affect later admissions. Results remain research evidence from a previously examined sample.

The three JSON configurations are `configs/research/g4_profit_floor_020_v1.json`, `configs/research/g4_atr_trailing_150_v1.json`, and `configs/research/g4_profit_floor_atr_150_v1.json`. Production settings and latest-run pointers are preserved. Generated results include a baseline comparison, per-trade and per-day evidence, run metadata, and `rollback.json`.

## Results and reproduction

The completed report is at `C:\TradingData\eqidv2\backtesting_result_v13_v10_g_4\research\20261017_three_sl_variants_final\report.html`.

| Policy | Wins / losses | Net P&L (Rs) | Change vs baseline (Rs) | Total losing-trade amount (Rs) |
| --- | --- | ---: | ---: | ---: |
| Baseline | 356 / 110 | 1,156,400.76 | 0.00 | 400,878.32 |
| Profit floor | 358 / 108 | 1,168,400.76 | +12,000.00 | 390,378.32 |
| ATR trailing | 358 / 108 | 1,143,723.59 | -12,677.18 | 390,378.32 |
| Combined | 358 / 108 | 1,143,723.59 | -12,677.18 | 390,378.32 |

All variants save Rs10,500 of losing-trade money (2.62%). The floor changes two executions and harms no original winners. ATR and combined change 28 executions and reduce profit on 24 original winners. ATR and combined match because all 230 proposed ATR updates already protect more than +0.20% gross; the floor never adds a tighter stop in this sample. The worst trade remains -Rs6,250 and daily-close drawdown remains Rs18,081.38. Full portfolio replay has zero new admissions/rejections, so its totals match the fixed-cohort comparison.

On the later 17 sessions, floor improves net P&L by Rs6,000 and ATR/combined by Rs6,100.03. This does not remove the full-history profit sacrifice or the reused-history limitation. Extra 5 bps on changed exits leaves net improvements of +Rs11,500 for the floor and -Rs19,677.18 for ATR/combined. No variant was activated in the current strategy.

To reproduce all three variants, run from the repository root. Each execution creates a new result directory:

```powershell
python -m research.g4_separate_sl_variants
```

Execution and rollback checks:

```powershell
python -m pytest tests/test_g4_separate_sl_variants.py tests/test_g4_sl_variants_rollback.py tests/test_g4_iterative_protective_stops.py tests/test_g3_sl_target_simulator.py -q
```

## Reversible rollback

Set the exact generated run folder, then preview the rollback from the repository root:

```powershell
$g4Run = 'C:\TradingData\eqidv2\backtesting_result_v13_v10_g_4\research\20261017_three_sl_variants_final'
python tools/g4_sl_variants_rollback.py --run-dir $g4Run
```

Apply the reviewed plan:

```powershell
python tools/g4_sl_variants_rollback.py --run-dir $g4Run --apply
```

The utility moves only the six new study files listed in `rollback.json` into that run's `rollback_archive/<archive-id>/` folder. It retains research results, the rollback utility and its tests. It refuses files whose current hash differs from the manifest, preserving later edits. It never runs Git clean/reset/stash, deletes files, or changes production configuration. If rollback refuses a modified file, keep the edit and inspect the difference before deciding how to proceed.

The output gives an absolute `receipt.json` path. To undo the rollback, preview the restore and then apply it:

```powershell
$g4Receipt = '<absolute receipt.json path printed by rollback>'
python tools/g4_sl_variants_rollback.py --run-dir $g4Run --restore $g4Receipt
python tools/g4_sl_variants_rollback.py --run-dir $g4Run --restore $g4Receipt --apply
```

Restore validates the receipt, archive paths and content hashes, and refuses to overwrite any existing workspace file. Receipts also permit recovery of files already moved if an archive/restore operation is interrupted. Keep the generated run folder and its original `rollback.json` with the archive until the rollback decision is final.
