# Approved G4 12:16 SHORT comparison

The user approved comparison-only integration on 17 October 2026. Original G4
parameters, primary selection and primary profit/loss calculations are unchanged.

The normal `fno_v13_v10_g_4_backtest.py` report now includes a separate approved
historical comparison section. It is explicitly frozen to 29 July–16 October
2026 (53 sessions); subsequent report dates do not extend this study.

| Parameter | Original G4 | Comparison |
|---|---:|---:|
| Active/core minimum OI change | 0.05% | 0.0625% |
| Initial stop | 1.20% | 1.00% |
| Target | 2.00% | 2.00% |

For `1216_SHORT`, the frozen comparison has 18 executed trades instead of 19,
net profit of Rs 40,844.76 instead of Rs 37,083.12, and 15 winning trades in both
cases. The entire Rs 3,761.64 improvement occurs in training. Training win rate
is 66.67%; validation and October are unchanged. This is a research comparison,
not evidence of a stable future 75% win rate.

Generate the approved comparison by itself:

```powershell
python fno_v13_v10_g_4_backtest.py --approved-comparison 1216_SHORT
```

This command writes a fresh directory under the G4 backtest result root and does
not update the primary G4 result pointer. Configuration/source overrides and a
different `--through` date are rejected for this frozen comparison.

Normal G4 runs retain the existing command and strategy configuration:

```powershell
python fno_v13_v10_g_4_backtest.py
```

Both report forms contain `approved_1216_configuration.csv`,
`approved_1216_comparison.csv`, `approved_1216_study_baseline.csv`,
`approved_1216_study_portfolio.csv`, and `approved_1216_evidence.json`. The
comparison includes both setup-level and full-portfolio metrics, with the study
calendar distinct from the normal run's calendar.

The evidence loader pins the study manifest
`f05ef5241cbf13551aaf2f41ae177d2a389af71c8c1430779af690d7bfdb70c2`, verifies input
hashes and approved parameters, reconciles all ten scope/period result rows,
and checks that other setups' trades and results are unchanged. The earlier
study and its approval proposal remain immutable; the new evidence records
the subsequent user approval separately.
