# G3: one active position per symbol

G3 now blocks a new entry when another G3 position already occupies that equity symbol, regardless of setup time, trade direction or execution namespace. Symbol comparison ignores case and surrounding whitespace. A later, distinct signal may enter after the earlier position is confirmed closed.

The 27 October PAPER session previously admitted HINDUNILVR shorts at 09:26:32 and 09:41:25 under different signal IDs. Replaying the second admission with the recorded positions now returns `SYMBOL_ALREADY_ACTIVE` and identifies the 09:26 signal as the blocker, despite sufficient available capital.

## Execution behavior

- PAPER checks the shared day book at fill time under its existing process lock. Unfilled PAPER triggers do not reserve a symbol. When two workers try to fill it, only the first succeeds. A rejected order retains zero entry price and no entry timestamp.
- LIVE checks the shared G3 book before broker submission under the existing entry-owner lock. Admitted pending entries, open positions, pending exits and uncertain submissions reserve their symbol. A definitive terminal state releases it; a terminal label with uncertain submission evidence does not.
- A rejection is saved as `BLOCKED_PORTFOLIO`, with the symbol and blocking signal IDs recorded. A rejected signal stays rejected; only a later distinct signal can re-enter after closure.
- Existing positions continue through protective exit management. Retrying the same durable LIVE reservation remains idempotent.
- Missing or invalid symbols in required active evidence block admission rather than allowing an unchecked entry.

The execution guard is identified separately as `g3_symbol_entry_guard_v1` / `ONE_ACTIVE_POSITION_PER_SYMBOL`. It does not change signal IDs, strategy targets/stops, sizing, other portfolio limits, LIVE authorization, or frozen historical results. Legacy G PAPER profiles retain their existing admission behavior. Future backtests intended to match this execution restriction must explicitly apply the same symbol rule; earlier results do not include it.

## Implementation and verification

`g3_symbol_admission.py` supplies symbol normalization and guard identifiers. `fno_v13_v10_g_paper.py` enforces the PAPER restriction. `g3_live_risk.py` enforces LIVE admission and records rejection evidence.

415 offline tests passed across symbol admission, PAPER admission, G3 LIVE risk/runtime/execution policy, shared LIVE execution, and operator/activation tests. Coverage includes both directions, concurrent PAPER processes, LIVE rejection before broker submission, uncertain orders, terminal release, malformed evidence, unchanged legacy capital limits, and protective management.

The recorded HINDUNILVR admission was independently replayed in memory; historical order files were not modified. No G3 entry workers were running at verification. Normal G3 launch scripts use this workspace and load the guard at their next start. This change does not launch or authorize LIVE trading.
