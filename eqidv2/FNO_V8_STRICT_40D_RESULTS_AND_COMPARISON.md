# FNO V8-Strict — 40-session backtest and comparison

## Strategy created

V8-Strict is a separately fingerprinted launcher and artifact namespace. It uses:

- the literal original V6 ten-leg five-minute setup book, including thresholds, ranking, caps and brackets;
- exact S+1 strict direction/displacement/body/wick confirmation;
- raw confirmation high/low stop-entry trigger, directionally rounded to the equity tick;
- V8 same-session paths, S+5 order expiry, gap-aware fills, brackets from actual fill, stop-first ambiguity, post-confirmation cancellation, duplicate/portfolio controls, source coverage checks and immutable provenance;
- native 15 bps round-trip cost and zero additional slippage in the primary replay.

The launcher imports no V6/V7 strategy, optimizer, replay or cache module. Its strict setup-book hash is `5de61f611ad30b52d303b2075ee169421f1208c5026789a78ce4907f35c16919`.

## Evaluation window

- Full: 40 official sessions, 2026-06-24 through 2026-08-19.
- Train diagnostic: 30 sessions through 2026-08-05.
- Held-out replay diagnostic: 10 sessions, 2026-08-06 through 2026-08-19.
- Frozen physical source snapshot: `6734204d53315d386a2c3949f01b272d4399a8a1d3b44b9cfd556a1b859190cc`.

## V8-Strict primary result — native 15 bps

| Metric | Result |
|---|---:|
| Five-minute candidate rows | 1,054 |
| Finite closed fills | 136 (3.40/session) |
| Wins / losses | 69 / 67 |
| Diagnostic PF | **1.885** |
| Additive diagnostic net | **+49.81 points** |
| Cash-equity sizing proxy P&L | **+₹25,176** |
| Maximum cumulative daily drawdown | **5.23 points** |
| Positive / negative / flat days | 26 / 13 / 1 |
| Train fills / PF / net | 114 / **1.888** / +41.45 |
| Test fills / PF / net | 22 / **1.870** / +8.37 |

The official headline is N/A because the source panel is incomplete. The numbers above are `LAST_REAL_BAR_SENSITIVITY` diagnostics only.

## Full comparison — native/conservative 15 bps

V6/V7 rows are deterministic 15 bps cost sensitivities of their fixed native selections. V8 and V8-Strict natively use 15 bps. Cost normalization does not make the execution engines identical.

| Strategy | Fills | Win % | Fills/session | PF | Net points | P&L proxy | Max DD | Train PF | Test PF |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| V6 strict legacy execution | 176 | **52.84%** | 4.40 | **2.009** | **+72.04** | **+₹36,234** | 7.10 | **2.159** | 1.356 |
| V7 relaxed legacy execution | **298** | 45.97% | **7.45** | 1.374 | +53.84 | +₹26,129 | 15.08 | 1.262 | 1.838 |
| Retuned V8 B0 | 191 | 49.21% | 4.78 | 1.768 | +55.62 | +₹27,500 | **4.51** | 1.828 | 1.408 |
| **V8-Strict** | 136 | 50.74% | 3.40 | **1.885** | +49.81 | +₹25,176 | 5.23 | 1.888 | **1.870** |

V8-Strict has the strongest train/test PF consistency and the best held-out PF, but V6 retains the best full-window PF/net and retuned V8 has lower drawdown and more fills.

## Common-cost comparison — 5 bps

| Strategy | Fills | Win % | PF | Net points | P&L proxy | Max DD | Train PF | Test PF |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| V6 | 176 | **52.84%** | **2.425** | **+89.64** | **+₹44,768** | 5.60 | **2.606** | 1.633 |
| V7 | **298** | 45.97% | 1.655 | +83.64 | +₹40,506 | 8.48 | 1.523 | 2.207 |
| Retuned V8 | 191 | 49.21% | 2.187 | +74.72 | +₹36,751 | **3.10** | 2.255 | 1.776 |
| **V8-Strict** | 136 | 50.74% | 2.276 | +63.41 | +₹31,754 | 4.26 | 2.285 | **2.234** |

## V8-Strict LONG/SHORT — native 15 bps

| Period/side | Fills | PF | Net points | P&L proxy | Max DD |
|---|---:|---:|---:|---:|---:|
| Full LONG | 59 | **2.091** | +24.24 | +₹11,814 | 3.80 |
| Full SHORT | 77 | 1.751 | +25.58 | +₹13,362 | 5.45 |
| Train LONG | 48 | 1.944 | +16.91 | +₹8,210 | 3.80 |
| Train SHORT | 66 | 1.853 | +24.54 | +₹12,713 | 5.45 |
| Test LONG | 11 | **2.704** | +7.33 | +₹3,604 | 2.71 |
| Test SHORT | 11 | **1.196** | +1.04 | +₹649 | 3.48 |

The test SHORT result is below the proposed side PF floor of 1.25 at 15 bps. It is a warning, not authority to disable SHORT after seeing the result.

## Candidate funnel — native 15 bps

| Terminal state | Count |
|---|---:|
| No confirmation | 759 |
| Post-confirmation cancelled | 73 |
| Entry window expired | 68 |
| Duplicate rejected | 18 |
| Targeted | 24 |
| Stopped | 55 |
| Last-real sensitivity square-off | 57 |
| Unresolved filled trades | 0 |

## Validity

The same source limitation applies to every strategy in the comparison:

- expected symbol-sessions: 8,320;
- complete: 4,782 (57.48%);
- incomplete: 3,538 (42.52%);
- all 208 symbols have some incompleteness;
- the last 13 sessions lack genuine 15:16–15:30 equity bars across the panel.

V8-Strict therefore correctly sets official PF, P&L, return and drawdown to N/A. It is not promotion eligible.

## Conclusion

- V8-Strict improves on retuned V8's full PF (1.885 vs 1.768) and test PF (1.870 vs 1.408) at 15 bps.
- It sacrifices trade count (136 vs 191), total net (+49.81 vs +55.62), and slightly increases drawdown (5.23 vs 4.51).
- It is more execution-honest than native V6, but native V6's diagnostic remains stronger on full PF/net.
- V8-Strict is the best candidate for **prospective PAPER/shadow evaluation**, not LIVE capital. Repair source coverage first, then freeze the same policy for at least 20 new complete sessions and 100 fills.

## Artifacts

- Launcher: `fno_v8_strict_v6_logic_backtest.py`
- Tests: `tests/test_fno_v8_strict_v6_logic_backtest.py`
- Native 15 bps run: `C:\TradingData\eqidv2\fno_oi\strategy_research\v8_strict_v6_logic_v1\runs\fno_v8_vs_20260820T163546372042+0530_7099855b7696`
- Native validation fingerprint: `7099855b76960d05e31549209ce87d78437dde122dc0f6dd01841bb07f97ff52`
- Common-cost 5 bps run: `C:\TradingData\eqidv2\fno_oi\strategy_research\v8_strict_v6_logic_v1\runs\fno_v8_vs_20260820T163705777360+0530_ab72ecd05879`
