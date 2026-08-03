# V11 Research Book Selection — 2026-07-22

## Decision

**No forward-shadow candidate is selected.**

Book A reduced core is retained as the research benchmark. Book C 120-minute
is retained as an exit challenger, but neither clears the predefined gate.
Book B, Book C 90-minute, Book C break-even, and Book C trailing are rejected
from the current selection pass.

## Gate Matrix

| Gate | Required | Book A | Book C 120-minute |
|---|---:|---:|---:|
| Positive train P&L | Yes | Pass | Pass |
| Positive validation P&L | Yes | Pass | Pass |
| Positive diagnostic-window P&L | Yes | Pass | Pass |
| Validation PF | >= 1.20 | Fail: 1.135 | Fail: 1.152 |
| Diagnostic PF | >= 1.20 | Pass: 1.202 | Pass: 1.254 |
| Validation P&L without best day | > 0 | Fail: Rs -2,240 | Fail: Rs -1,171 |
| Diagnostic P&L without best day | > 0 | Pass: Rs 590 | Pass: Rs 1,142 |
| 95% day-bootstrap lower bound | > 0 | Fail | Fail |
| Net PF after full costs | >= 1.30 | Not available (deferred) | Not available (deferred) |

## Daily Bootstrap

Day-block bootstrap with 200,000 resamples and seed 20260722:

| Window | Book | Probability total P&L > 0 | 95% interval (Rs) |
|---|---|---:|---:|
| Train | Book A | 90.1% | -10,715 to 53,908 |
| Train | C120 | 89.2% | -11,061 to 53,262 |
| Validation | Book A | 67.1% | -11,139 to 21,363 |
| Validation | C120 | 67.8% | -12,220 to 20,438 |
| Diagnostic | Book A | 74.0% | -9,354 to 18,577 |
| Diagnostic | C120 | 79.9% | -6,334 to 17,222 |

The paired C120-minus-Book-A difference is not reliable in any window. In the
diagnostic window C120 leads by only Rs 508, with a 95% paired-bootstrap range
from approximately Rs -4,001 to Rs 5,495.

## Setup Evidence

Across all 43 sessions, Book A produced:

| Setup | Trades | Net P&L | PF |
|---|---:|---:|---:|
| E_ORB_BREAKOUT_LONG | 17 | Rs 16,745 | 3.099 |
| G_HIGHER_HIGH_BREAK | 82 | Rs 15,038 | 1.405 |
| G_LOWER_LOW_BREAK | 18 | Rs 2,963 | 1.729 |
| C_OR_BREAKDOWN | 138 | Rs -5,242 | 0.891 |

The 120-minute policy improves C_OR_BREAKDOWN to approximately Rs -241/PF
0.994, but reduces G_HIGHER_HIGH_BREAK by approximately Rs 4,853. The portfolio
improvement over Book A is therefore negligible and statistically unresolved.

## Consequence

- Do not replace the working V11 baseline.
- Do not start a single-candidate promotion shadow yet.
- Preserve Book A as the simpler benchmark and C120 as a research challenger.
- Resume selection only after genuinely untouched sessions and cost-adjusted
  results are available.
- The next independent task is restoring and verifying V7 paper-result capture,
  which is required for live-parity evidence.
