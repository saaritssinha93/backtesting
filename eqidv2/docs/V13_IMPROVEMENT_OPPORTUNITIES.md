# V13 Improvement Opportunities

The dashboard's **Research & Suggestions > V13 Improvement Opportunities** subgroup
contains the read-only **V13 Evidence-Based Improvement Opportunities** report.
It refreshes with `tools/v13_strategy_research.py` and the existing
`bat/run_v13_strategy_research_refresh.bat` workflow.

## What the section provides

Ten stable opportunities cover execution availability, live/finalized signal
parity, execution costs, data coverage, indicator thresholds and ranking, weak
setups, decision-time regimes, risk sizing, prediction validation, and prospective
evaluation. Each contains observed evidence, links to source cards, potential
benefit, a proposed test, acceptance criteria and limitations.

The evidence is recalculated from each research bundle. For example, a setup
appears in the weak-setup observations only while its supplied executed-trade
sample has negative net P&L. No setup exclusion, numeric indicator adjustment,
experiment registration or trading change is performed by this report.

## Evidence interpretation

- `OBSERVED_ISSUE`: an operational or data problem is present in the supplied
  evidence; its potential profit effect remains unquantified.
- `RESEARCH_HYPOTHESIS`: a proposed experiment, with no demonstrated improvement.
- `SENSITIVITY_ONLY`: a verified simulation scenario rather than realized broker
  execution or recoverable profit.
- `MEASUREMENT_GAP` / `UNAVAILABLE`: required measurement or sample is missing.
- `MONITOR` / `NO_CURRENT_CANDIDATE`: the supplied sample does not establish the
  particular issue or negative-net setup candidate.
- `BLOCKED` / `MANUAL_REVIEW_REQUIRED`: the supplied prospective gate state;
  neither state confers trading authority.

Ordinary market non-fills are not automatically execution failures. Read errors
remain visible. Missing measurements are not printed as zero failures, empty
promotion gates are unavailable, and delayed execution scenarios are excluded
from the sensitivity table pending absolute-expiry verification. First-reason
journal coverage does not necessarily mean first-failure coverage.

## Artifacts and implementation

The report is published as
`C:/TradingData/eqidv2/v13_v10_g_strategy_research/latest/latest_fno_v13_v10_g_research_improvements.md`.
An immutable run copy and report SHA-256 are stored by the existing research
publisher. Structured evidence is saved under
`manifest.json -> analysis -> improvement_opportunities`.

`ai_platform/observability/improvement_opportunities.py` builds and renders the
proposals. The generator and dashboard maintain a separate improvement-card
mapping; the existing eight research and six observability cards are unchanged.
Internal proof links navigate only to known dashboard card anchors. This view
has no process, scheduler, restart or trading controls.

Historical source-through date, report generation time and operational snapshot
time are displayed separately. All opportunities retain false execution and
selection authority, `PROPOSED_NOT_APPLIED`, and no estimated rupee uplift.

## Verification

Focused tests cover dynamic evidence, missing measurements, normal cancellations,
unverified/delayed scenarios, nonfinite values, source/report hash publication,
unchanged inputs, read-only card routing, and escaped internal proof links.

```powershell
py -3.12 -m pytest -q tests/test_observability_improvement_opportunities.py tests/test_observability_strategy_research.py tests/test_dashboard_fno_v13_v10_g_improvement_views.py tests/test_dashboard_fno_v13_v10_g_research_views.py tests/test_dashboard_fno_v13_v10_g_observability_views.py
```
