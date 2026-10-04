# Import and write-path audit

| Existing module or source | Relevant behavior | AI integration decision |
|---|---|---|
| `fno_v13_v10_g_live_config.py` | Loads and hash-validates the frozen config at import; imports other strategy modules. | Do not import from API/agent readers. Identity is pinned in `profile_registry.json`; the frozen file is independently hashed. |
| `fno_v13_v10_g_options_paper.py` | Creates option/output directories at module import and owns mutable paper states. | Do not import. Read order JSON through the isolated option adapter. |
| `backtesting_result_v13_v10_g_daily.py` | Publishes mutable status/latest files and imports strategy code. | Do not import. Read the JSON envelope and expose metrics only after complete-success validation. |
| `fno_live_evidence.py` | Writes append-only evidence envelopes; verifies a canonical payload SHA-256. | Do not import. The new evidence adapter duplicates the small, stable read/verify contract without producer side effects. |
| Equity order JSON | Written by PAPER and LIVE execution roles. | Read only. LIVE wins only when its state records an actual fill (`OPEN`/`CLOSED`, entry price greater than zero); otherwise use the actual PAPER fill. |
| Option order JSON | Mutable while an option paper trade is open. | Read only. OPEN `net_pnl_rs` is a mark; only CLOSED P&L is realized. Preserve `execution_source`. |
| Dashboard on port 8787 | Existing UI and readers are coupled to a broad runtime module. | Treat as an observable consumer, not as the initial API foundation. Later FastAPI work will call the new adapters. |
| Broker/session/credential files | May contain secrets or enable market actions. | Outside Stage 0/1 source registry and fixtures. Never copy into prompts, fixtures, logs, or API responses. |

The new `ai_platform` package has no import-time filesystem writes. Its only file operations are explicit reads and hashing initiated by a caller.

`backtesting_result_v13_v10_g_daily.py` is the canonical daily pointer writer: it atomically overwrites `status.json`, the latest JSON, and the latest Markdown, then writes a dated report. The AI layer therefore snapshots pointers with before/after hashes and bounded retries. It never treats a stable filename as immutable evidence.
