# V13-V10-G AI platform baseline

Captured on 2026-09-17 IST before adding any API or agent runtime. Repository branch `eqidv2` was at commit `c02d03dc082809ffc5990e2a8c34af5fd178fb1b` with the two plan documents untracked. No strategy process, scheduler, order file, evidence file, credential, or profile setting was changed during this baseline.

## Pinned identity

- Equity label: `V13-V10-G`
- Equity strategy version: `FNO_V13_V10_G_RETAINED_20260914`
- Strategy fingerprint: `a41f11737885e13c3cbe5c3310eed59d17bfab0da70784752fdee41e56c67f8f`
- Existing live transport generation: `v6`
- Frozen config SHA-256: `d8bcae37d7725279f8ac6e5c4c96a44e8a43f41b5e42d413b167fd806b52a127`
- Option paper version: `FNO_V13_V10_G_OPTIONS_ONE_LOT_ATM_SL30_T40P4_20260915`

The profile registry is the integration allow-list. Changing any identity value requires an explicit registry update and adapter-test update; an AI component must not infer or auto-promote a different profile.

## Observed runtime state

The latest daily replay for 2026-09-17 was `SUCCESS`, complete, data verification `PASS`, with 210 universe stocks, 208 included stocks, one selected/fill trade, and net replay P&L of Rs -3,250 at 5 bps cost. This is a dated observation, not a promise about later files.

The equity book contained a filled PAPER ATHERENERG trade and a matching cancelled LIVE quantity-one state. The selected execution source is therefore PAPER. A cancelled LIVE state with zero entry price is not an actual fill and must never suppress a filled PAPER state.

The option paper book contained an OPEN one-lot ATHERENERG option state. Its reported P&L is a mark and is not realized P&L. Closed option states may include guarded quote executions or `HISTORICAL_EXACT_5M_REPLAY`; these execution sources remain distinct in normalized records.

The scheduled names `options_live_long` and `options_live_short` describe continuously monitored directional roles; they do not mean broker LIVE execution. Their state contract says `mode=PAPER`, the one-lot engine writes only the isolated paper book, and it does not call a broker order method. Their results therefore cannot be combined with the historical three-lot study.

Four execution/research profiles are separate: LIVE equity is fixed at one share, PAPER equity is exposure-sized, current options PAPER is one lot, and the historical options reference is three lots over the available five-minute sample. The three-lot result reports 20 closed trades from 2026-08-26 through 2026-09-11 and is explicitly reused-history evidence, not an untouched test. It must not be added to the one-lot PAPER ledger.

Live confirmation fails closed until 20 prior completed one-minute observations exist. The historical research calculation accepts five minimum periods. Explanations must state which policy produced a decision and cannot attribute this difference to strategy alpha.

## Build and service inventory

- Python: `3.12.10`
- Codex CLI: `0.154.0-alpha.6.2`
- Codex authentication status: logged in using ChatGPT; only the supported status command was run and no authentication file or token content was inspected.
- Test runner: pytest `9.1.1`
- FastAPI, Uvicorn and Pydantic: not installed in the current Python environment. Stage 2 must use a separate pinned environment rather than changing global packages.
- Git repository: branch `eqidv2`, baseline commit `c02d03dc082809ffc5990e2a8c34af5fd178fb1b`.
- Port check: neither 8787 nor proposed 8788 was listening at the inspection time. The existing scheduled dashboard launch remained configured.

Relevant ready Windows tasks include the 16:20 daily replay; G scanner, confirmation feed, equity one-minute feed, equity PAPER long/short, quantity-one LIVE, equity logger/result, options PAPER long/short, options logger/result; and dashboard start/stop tasks. This is an inventory, not a health claim. The task list is not modified by the AI platform work.

| Concern | Existing producer | Current consumer or artifact |
|---|---|---|
| Signal/readiness evidence | G scanner, five-minute futures/OI fetch and one-minute confirmation feed | Immutable evidence tree and dashboard |
| Equity execution state | G PAPER roles and quantity-one LIVE role | Mode-specific order JSON, trade logger and net result |
| Option execution state | One-lot option PAPER roles | Isolated option order JSON, logger and net result |
| Completed replay result | 16:20 daily replay task | Status/latest JSON and Markdown, dated report |
| Platform reads | New pure adapters | Future FastAPI and Assistant; no producer imports |

The existing dashboard binds to port 8787 and has a `cloudflared` public-link launcher. The initial Stage 2 boundary is a separate authenticated loopback service on `127.0.0.1:8788`. It will not be added to the tunnel or the existing same-origin routes until the public-boundary review is complete.

## Pilot limits and fallback

Permitted Stage 0/1 data roots are this Git workspace for code/registries/fixtures and the registered read-only paths under `C:/TradingData/eqidv2`. Arbitrary paths, credential/session files, broker methods, schedulers and process control are excluded.

The future Codex pilot starts with one active request, a maximum queue of three, a two-minute request timeout, a 256 KiB sanitized evidence bundle, and no paid API fallback. Its deterministic fallback is the Stage 1 adapter/service result. These are safety and load limits; precise currency usage remains unknown until Stage 3 measures the supported account path.

## Safety boundary for Stages 0 and 1

All new code is read-only. It reads explicit files, validates declared schemas and identities, calculates hashes, and returns immutable records. It does not import live strategy, dashboard, broker, or option-runner modules because several of those modules load runtime configuration or create directories during import. It does not place orders, start fetchers, change Windows tasks, or write into `C:/TradingData/eqidv2`.

Incomplete daily results expose no performance metrics. Realized P&L includes only `CLOSED` states. An OPEN option mark is reported separately. Evidence is usable only when its envelope schema and canonical payload hash validate, and optional as-of selection excludes observations after the requested timestamp.

## Stage 0 acceptance

- Source and profile registries exist as machine-readable JSON.
- Existing producers and write paths have an import/side-effect audit.
- Test fixtures contain synthetic identifiers and no credentials or broker account data.
- The baseline records the exact starting strategy identity and the source-precedence/accounting rules.
