# AI platform stage tracker

| Stage | Status | Evidence |
|---|---|---|
| 0 — Baseline and safety boundary | VERIFIED | `BASELINE.md`, source/profile registries, build/service and auth-status inventory, import/write-path audit, fixture manifest |
| 1 — Contracts and read-only adapters | VERIFIED | Typed states, daily/equity/options/evidence/status adapters, coherent snapshots, accounting service, sanitized fixtures, focused and adjacent tests |
| 2 — FastAPI read-only service | VERIFIED | Pinned local environment, authenticated application factory, registered read routes, deterministic assistant sessions, contract tests and loopback HTTP smoke test |
| 3 — Bounded Codex explanation pilot | Not started | Depends on Stages 1 and 2 |
| 4 — Dashboard Assistant panel | Not started | Depends on Stage 2; Stage 3 for AI answers |
| 5 — Reconciliation and prospective evidence | Not started | Depends on Stages 1 and 2 |
| 6 — Durable research job service | Not started | Depends on Stages 2 and 5 |
| 7 — Specialist agent workflows | Not started | Depends on Stages 3, 5 and 6 |
| 8 — Bounded data-repair actions | Not started | Optional; depends on Stages 5 and 6 |
| 9 — Reliability hardening | Not started | Applies to enabled capabilities |
| 10 — Predictive shadow research | Not started | Optional; depends on Stages 5 and 6 |

Stage 1 is deliberately narrow: it establishes trustworthy inputs for the later API. It does not start a server or any market process.

## Verification record — 2026-09-17

- Sanitized Stage 1 contract tests: 10 passed.
- Existing daily replay tests: 36 passed.
- Existing one-lot option paper tests: 6 passed.
- Read-only real-artifact smoke test: completed daily replay validated; two equity records normalized; cancelled LIVE correctly yielded to the PAPER fill; one OPEN option remained mark-only; one immutable confirmation envelope passed its payload hash.
- Python compilation and all JSON registry/fixture parsing passed.
- No writes occurred under `C:/TradingData/eqidv2`; no process, task, strategy setting or order state was changed.

Stage 2 used a repository-local pinned environment because FastAPI/Uvicorn/Pydantic were absent from the original interpreter. The environment now exists as ignored local build output; port 8788 is checked by the operator before startup.

## Verification record — Stage 2

- Created `.venv-ai-platform` with the exact versions in `ai_platform/requirements.txt`; the system Python installation was not changed.
- Stage 2 API contract tests: 7 passed. Authentication, capabilities, validation, source allowlisting, path containment, redaction, pagination/accounting behavior and deterministic assistant session lifecycle are covered.
- Full focused verification: 7 Stage 2 tests, 10 Stage 1 tests and 42 adjacent existing strategy tests passed.
- Read-only real-artifact exercise: all seven primary GET routes returned HTTP 200. Across 35 in-process calls, observed p50 was 6.34 ms, p95 was 15.46 ms and maximum was 28.15 ms on this machine.
- Loopback exercise: started Uvicorn on `127.0.0.1:8788`, received LIVE and authenticated READY responses, read the complete 2026-09-17 result, and stopped that API process. Port 8788 is not left running.
- No scanner, fetcher, scheduler, dashboard, broker session or trading worker was started or changed by the API.

Stage 3 dependency: implement and containment-test the bounded Codex explanation backend. The current assistant session endpoint remains deterministic with `ai_enabled=false`.
