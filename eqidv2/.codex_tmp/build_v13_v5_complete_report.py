from __future__ import annotations

import hashlib
import html
import json
import os
import re
import sys
from dataclasses import asdict
from datetime import datetime
from pathlib import Path

import pandas as pd
from bs4 import BeautifulSoup
from markdown_it import MarkdownIt

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import fno_v13_corrected_v5_backtest as v5


WORKSPACE = Path(__file__).resolve().parents[1]
RESULT = Path(r"C:\TradingData\eqidv2\fno_oi\strategy_research\v13_corrected_v5")
PROFILE = "higher_frequency"
PFX = RESULT / PROFILE / f"fno_v13_corrected_v5_{PROFILE}"
OUT_MD = WORKSPACE / "V13_V5_FNO_COMPLETE_REPORT.md"
OUT_HTML = WORKSPACE / "V13_V5_FNO_COMPLETE_REPORT.html"
ATTACHMENT = Path(r"C:\Users\Saarit\.codex\attachments\3aa93c8e-78aa-4c7f-a1f9-f228d543f67e\pasted-text.txt")


def sha(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for block in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def fmt(value, digits: int = 3) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return "N/A"
    try:
        n = float(value)
    except (TypeError, ValueError):
        return str(value)
    if n == float("inf"):
        return "∞"
    if n == float("-inf"):
        return "−∞"
    return f"{n:.{digits}f}"


def md_table(headers: list[str], rows: list[list[object]]) -> str:
    def cell(value: object) -> str:
        return str(value).replace("|", "\\|").replace("\n", "<br>")

    lines = ["| " + " | ".join(map(cell, headers)) + " |"]
    lines.append("| " + " | ".join("---" for _ in headers) + " |")
    lines.extend("| " + " | ".join(cell(x) for x in row) + " |" for row in rows)
    return "\n".join(lines)


provenance_path = RESULT / "fno_v13_corrected_v5_provenance.json"
provenance = json.loads(provenance_path.read_text(encoding="utf-8"))
comparison = pd.read_csv(RESULT / "fno_v13_corrected_v5_profile_comparison.csv")
periods = pd.read_csv(RESULT / "fno_v13_corrected_v5_period_metrics.csv")
daily = pd.read_csv(PFX.with_name(PFX.name + "_daily.csv"))
setups_result = pd.read_csv(PFX.with_name(PFX.name + "_setups.csv"))
breakdowns = pd.read_csv(PFX.with_name(PFX.name + "_breakdowns.csv"))
stress = pd.read_csv(PFX.with_name(PFX.name + "_stress.csv"))
sensitivity = pd.read_csv(PFX.with_name(PFX.name + "_sensitivity.csv"))
walk_forward = pd.read_csv(PFX.with_name(PFX.name + "_walk_forward.csv"))
bootstrap = pd.read_csv(PFX.with_name(PFX.name + "_bootstrap.csv"))
trades = pd.read_csv(PFX.with_name(PFX.name + "_trades.csv"))

generated = datetime.now().astimezone().isoformat(timespec="seconds")
source_path = Path(v5.__file__).resolve()
source_hash = sha(source_path)
prov_hash = sha(provenance_path)


setup_rows = []
for s in v5.profile_setups(v5.PROFILES[PROFILE]):
    d = asdict(s)
    result_row = setups_result.loc[setups_result["signal_end"].eq(d["signal_end"]) & setups_result["side"].eq(d["side"])]
    if len(result_row):
        rr = result_row.iloc[0]
        observed = f"{int(rr.orders)}/{int(rr.fills)}; WR {fmt(rr.win_rate_pct, 1)}%; T1 {fmt(rr.target_hit_rate_pct, 1)}%; PF {fmt(rr.profit_factor, 2)}; net {fmt(rr.net_profit_pct, 3)}%"
    else:
        observed = "No matching result row"
    setup_rows.append([
        d["signal_end"], d["confirmation_end"], d["side"], d["max_entries"], d["picker"],
        fmt(d["price_change_pct"], 3), fmt(d["oi_change_pct"], 3), fmt(d["volume_ratio"], 2),
        fmt(d["body_ratio"], 2), fmt(d["max_wick_ratio"], 2), fmt(d["min_traded_value"], 0), observed,
    ])


profile_rows = []
for label in ["V13_V3_OFFICIAL_PUBLISHED", "V13_V3_CORRECTED_UNIFORM_1515", "V13_V5_BALANCED", "V13_V5_CONSERVATIVE", "V13_V5_HIGHER_FREQUENCY"]:
    r = comparison.loc[comparison.label.eq(label)].iloc[0]
    profile_rows.append([
        label, int(r.configured_setup_rules), int(r.executed_trades), fmt(r.average_trades_per_day, 2),
        fmt(r.win_rate_pct, 2), fmt(r.target_hit_rate_pct, 2), fmt(r.runner_target_hit_rate_pct, 2),
        fmt(r.profit_factor, 3), fmt(r.net_profit_pct, 3), fmt(r.maximum_drawdown_pct, 3),
    ])


period_rows = []
for name in ["TRAIN", "VALIDATION", "PSEUDO_TEST", "ALL"]:
    r = periods.loc[periods.strategy.eq("V13_V5_HIGHER_FREQUENCY") & periods.period.eq(name)].iloc[0]
    period_rows.append([
        name, int(r.sessions), int(r.executed_trades), fmt(r.win_rate_pct, 2), fmt(r.target_hit_rate_pct, 2),
        fmt(r.profit_factor, 3), fmt(r.net_profit_pct, 3), fmt(r.expectancy_pct, 3), fmt(r.maximum_drawdown_pct, 3),
    ])


def breakdown_rows(dimension: str) -> list[list[object]]:
    d = breakdowns.loc[breakdowns.dimension.eq(dimension)].copy()
    rows = []
    for r in d.itertuples(index=False):
        rows.append([r.value, int(r.fills), int(r.wins), int(r.losses), fmt(r.win_rate_pct, 2),
                     fmt(r.target_hit_rate_pct, 2), fmt(r.profit_factor, 3), fmt(r.net_profit_pct, 3)])
    return rows


daily_rows = []
for r in daily.itertuples(index=False):
    daily_rows.append([r.day, r.period, int(r.trades), int(r.wins), int(r.losses), int(r.target_hits), int(r.stop_hits),
                       fmt(r.net_return_pct, 3), fmt(r.cumulative_net_return_pct, 3), fmt(r.drawdown_pct, 3)])


stress_rows = []
for key in ["COST_0BPS", "COST_5BPS", "COST_10BPS", "COST_20BPS", "COST_30BPS", "ENTRY_DELAY_1M", "WORSE_FILL_5BPS", "WORSE_FILL_10BPS", "REMOVE_BEST_5_TRADES"]:
    r = stress.loc[stress.stress.eq(key)].iloc[0]
    stress_rows.append([key, int(r.executed_trades), fmt(r.win_rate_pct, 2), fmt(r.target_hit_rate_pct, 2),
                        fmt(r.profit_factor, 3), fmt(r.net_profit_pct, 3), fmt(r.maximum_drawdown_pct, 3)])


sensitivity_rows = []
for key in ["BASE", "T1_1.02", "T1_1.12", "STOP_1.40", "STOP_1.60", "RUNNER_2.50", "RUNNER_2.70", "PARTIAL_0.05", "PARTIAL_0.15"]:
    r = sensitivity.loc[sensitivity.experiment.eq(key)].iloc[0]
    sensitivity_rows.append([key, fmt(r.initial_stop_pct, 3), fmt(r.first_target_pct, 3), fmt(r.partial_pct, 2),
                             fmt(r.runner_target_pct, 2), fmt(r.win_rate_pct, 2), fmt(r.target_hit_rate_pct, 2),
                             fmt(r.profit_factor, 3), fmt(r.net_profit_pct, 3), fmt(r.pseudo_test_net_profit_pct, 3)])


walk_rows = []
for r in walk_forward.itertuples(index=False):
    walk_rows.append([int(r.fold), r.test_start, r.test_end, int(r.executed_trades), fmt(r.win_rate_pct, 2),
                      fmt(r.target_hit_rate_pct, 2), fmt(r.profit_factor, 3), fmt(r.net_profit_pct, 3), fmt(r.maximum_drawdown_pct, 3)])


source_rows = [
    ["SRC-V5-MAIN", "fno_v13_corrected_v5_backtest.py", "41–70, 74–145, 177–245, 418–588, 591–713, 726–990, 993–1150, 1195–1300, 1435–1543, 2078–2135", "Version/profile/cache, selection, entry/fill, exits, metrics, validation and CLI"],
    ["SRC-V3", "fno_v13_corrected_v3_backtest.py", "41–60, 126–181, 184–267, 420–452", "Inherited setup book, NIFTY first-bar context/gate"],
    ["SRC-V2", "fno_v13_corrected_v2_backtest.py", "80–130, 170–257", "OI policy, global 1.00% cap, added 09:55 template"],
    ["SRC-V6", "fno_v6_corrected_backtest.py", "183–271, 383–408", "Point-in-time rolling near-month session eligibility and regime concatenation"],
    ["SRC-SIGNAL", "fno_oi_ema_confirm_sweep.py", "50–71, 77–102, 169–357", "Loose causal candidate cache, EMA/OI direction, strict S+1 confirmation, S+2 path"],
    ["SRC-DATA", "fno_oi_hybrid_data.py", "18–39, 316–377, 391–457", "NSE 1m → exact end-labelled 5m; features; futures-OI-only join"],
    ["SRC-SELECT", "fno_v5_hybrid_backtest.py", "1–73", "Threshold predicates, deterministic ranking and per-day max entries"],
    ["SRC-PROV", "fno_oi_backtest_provenance.py", "86–168, 239–336", "Dated universe resolution and stored-data provenance"],
    ["SRC-COMMON", "fno_oi_common.py", "25–29, 438–468, 502–577", "Runtime F&O roots, registry, dated universe and raw contract paths"],
    ["SRC-PATHS", "eqidv2_runtime_paths.py", "7–63", "Environment-controlled runtime roots"],
    ["SRC-TEST", "tests/test_fno_v13_corrected_v5_backtest.py", "1–end", "Hash pinning, exact profiles, gap fills, stop-first ambiguity, path completeness, metric meaning"],
    ["SRC-PAPER", "fno_multi_paper_profiles.py; fno_multi_paper_session.py; fno_multi_paper_engine.py", "profiles 20, 119; session 44, 800–840; engine state/order sections", "Reusable V10/V11/V12 paper-only patterns; not wired to V13-V5"],
    ["SRC-LIVE", "fno_v6_live.py; fno_v6_live_config.py; fno_v6_live_kite_session.py", "config 21, 80–81, 117–321; session/order callbacks", "Existing V6 safety/arming/Kite patterns; not V13-V5"],
    ["SRC-DASH", "log_dashboard_server.py", "9076–9079, 9928–9958, 10639–10658, 12101–12251", "Existing multi-paper cards, kill controls, status and heartbeat; no V13 adapter"],
    ["SRC-SCHED", "eqidv2_eod_scheduler_for_1min_data_live.py", "29, 253–284, 326–412", "Existing centralized slot builder/status heartbeat; not V13 lifecycle"],
]


toc = "\n".join(f"{i}. [{name}](#{i}-{slug})" for i, (name, slug) in enumerate([
    ("Title page", "title-page"), ("Document metadata", "document-metadata"), ("Executive summary", "executive-summary"),
    ("One-page strategy brief", "one-page-strategy-brief"), ("Repository and source map", "repository-and-source-map"),
    ("System architecture", "system-architecture"), ("Complete end-to-end flow", "complete-end-to-end-flow"),
    ("Data and candle construction", "data-and-candle-construction"), ("Five-minute setup logic", "five-minute-setup-logic"),
    ("One-minute entry logic", "one-minute-entry-logic"), ("Instrument and strike selection", "instrument-and-strike-selection"),
    ("Entry execution model", "entry-execution-model"), ("Position sizing and capital model", "position-sizing-and-capital-model"),
    ("Stop-loss, target, trailing, and exit logic", "stop-loss-target-trailing-and-exit-logic"),
    ("Guards and safety rails", "guards-and-safety-rails"), ("Trade-state machine", "trade-state-machine"),
    ("Timing and session behavior", "timing-and-session-behavior"), ("Backtesting implementation", "backtesting-implementation"),
    ("Backtest results and validation", "backtest-results-and-validation"), ("Trade-level worked examples", "trade-level-worked-examples"),
    ("Inaccuracies, bugs, and problem register", "inaccuracies-bugs-and-problem-register"),
    ("Backtest versus paper versus live differences", "backtest-versus-paper-versus-live-differences"),
    ("Kite Connect integration design", "kite-connect-integration-design"), ("Multi-session dashboard design", "multi-session-dashboard-design"),
    ("Proposed micro-file and service architecture", "proposed-micro-file-and-service-architecture"),
    ("Time-bound scheduling and lifecycle management", "time-bound-scheduling-and-lifecycle-management"),
    ("Monitoring, alerts, and reconciliation", "monitoring-alerts-and-reconciliation"),
    ("Deployment and rollback plan", "deployment-and-rollback-plan"), ("Improvement roadmap", "improvement-roadmap"),
    ("Final readiness assessment", "final-readiness-assessment"), ("Glossary", "glossary"), ("Appendices", "appendices"),
], start=1))


report = f"""# Corrected V13-V5 F&O Trading System

## 1. Title page

**Complete technical, operational, and results audit**  
Strategy: `FNO_V13_CORRECTED_V5_RESEARCH_20260904`  
Configured default: `higher_frequency`  
Evidence status: **EXPERIMENTAL_SHADOW_NOT_PRODUCTION_PROMOTED**  
Report version: 1.0  
Generated: {generated}  

> **Risk warning:** This document describes historical research and a proposed safety architecture. It does not guarantee future results, authorize trading, or recommend deploying capital. No broker login or order was performed.

## 2. Document metadata

| Field | Value |
| --- | --- |
| Exact main source | `{source_path}` |
| Main source SHA-256 | `{source_hash}` |
| Frozen V13-V3 SHA-256 | `{provenance['v13_v3_source_sha256_after']}` |
| Result provenance | `{provenance_path}` |
| Provenance SHA-256 | `{prov_hash}` |
| Artifact generated at | `{provenance['generated_at_ist']}` |
| Data through | `{provenance['through_day']}` |
| Backtest rerun for this report | **No**. Existing V13-V5 artifacts were read and cross-checked; the relevant unit/contract suite was rerun. |
| Verification run | **38 tests + 12 subtests passed** on 2026-09-04; includes V13-V5, inherited V13/V6, hybrid candle, and confirmation-policy tests. |
| Confirmed bugs/config defects | **1** |
| Live-readiness blockers | **11** |
| Overall verdict | **Not ready for paper trading**; suitable only for offline replay/research until P0/P1 gates close. |

### Table of contents

{toc}

### Evidence labels used

- **Verified:** observed directly in code, checksummed artifact, or passing test.
- **Inferred:** logical consequence of verified code, explicitly labelled.
- **Comment-only:** statement found only in prose/comment, not executable behavior.
- **Missing / not implemented:** required capability was not found in V13-V5.
- **Recommended:** future design; it is not current behavior.

## 3. Executive summary

V13-V5 is an **intraday stock-direction research backtest**, not a complete options trading system. It uses NSE cash-equity one-minute OHLCV for price, confirmation, entry and exit; it joins only open interest from a mapped near-month NFO stock future. A bullish or bearish five-minute EMA-aligned move with rising OI and volume becomes a candidate. A strict directional one-minute candle immediately after the five-minute candle creates a stop-entry trigger. The default higher-frequency profile ranks candidates within each configured time/side cell, allows the trigger to remain active until 15:15, then simulates a two-stage exit: 10% at +1.075%, move the 90% runner stop immediately to entry, seek +2.60%, and otherwise close at 15:15. [SRC-DATA], [SRC-SIGNAL], [SRC-V5-MAIN]

On 25 sessions from 2026-07-29 through 2026-09-03, the default profile produced 91 selected orders, 89 fills, 65 wins, a 73.03% win rate, a 56.18% first-target rate, PF 3.312, and +52.298 percentage points of arithmetic net trade returns after a flat 5 bps per fill assumption. Maximum drawdown on the summed daily-return curve was −2.913 percentage points. These are **not account returns**: there is no quantity, capital, margin, compounding, portfolio overlap, or rupee P&L model. [SRC-V5-MAIN: `metrics`, lines 1016–1126]

Strengths are causal S+1 confirmation, exact S+2-to-15:15 continuous minute paths, point-in-time contract regimes, hash-pinned inherited code, isolated caches, pessimistic stop-first handling for same-bar ambiguity, gap-aware actual-open entry and adverse stop fills, and unusually explicit result provenance. The most serious weaknesses are the absence of a tradable contract/strike model, position sizing and portfolio risk, realistic costs/spreads/partial fills, a bounded entry window, and any V13-specific paper/live state and reconciliation engine. The apparent validation is only 25 already-inspected sessions: the last six are labelled pseudo-test, not untouched test.

**Readiness:** not ready for paper trading as a faithful V13-V5 strategy because a paper broker cannot know what contract or quantity to simulate, and critical lifecycle/risk rules are absent. It is ready for offline historical replay. It is not ready for live trading.

**Next three actions:** (1) freeze a genuinely unseen dataset and pre-register metrics; (2) decide and test the executable instrument contract—cash equity, stock future, or options with explicit expiry/strike/premium mapping; (3) implement a locked paper adapter with risk sizing, trigger expiry, order-state persistence, reconciliation and kill switches, then prove replay parity.

## 4. One-page strategy brief

### V13-V5 in 60 seconds

| Question | Answer |
| --- | --- |
| What is scanned? | Point-in-time mapped F&O underlyings, using NSE cash-equity OHLCV and NFO near-month futures OI. |
| What makes a setup? | EMA9/20/50 trend alignment, directional five-minute price change, positive OI change, volume ratio, then timing-specific body/wick thresholds. |
| What confirms entry? | The exact next one-minute candle: bullish and above the 5m close for LONG; bearish and below it for SHORT. |
| Trigger | Confirmation high for LONG, confirmation low for SHORT. |
| Selection | Per timing/side/day deterministic rank (`max_liquidity`, `max_move`, or `max_volume`) and `max_entries` 1–2. |
| Special market guard | Only the 09:25 SHORT cell requires NIFTY near-month first-bar return ≤ −0.05%. |
| Default exit | 1.50% stop; 10% at +1.075%; 90% runner at +2.60% with immediate breakeven stop; 15:15 time exit. |
| Current headline | 89 fills, 73.03% wins, 56.18% T1 hits, PF 3.312, +52.298 summed net return points, −2.913 max drawdown points. |
| What is missing? | Contract/strike, option premium, lot size, quantity, margin/capital, spread/depth, broker order state, recovery, reconciliation and production risk controls. |

Plain-language sequence: the five-minute layer decides **which stock and direction are interesting**; the exact following minute decides **the breakout level**; subsequent one-minute cash bars decide **if and when the synthetic order fills and exits**. The exit is not a trailing stop: it is a fixed breakeven runner stop. [SRC-SIGNAL: lines 271–351; SRC-V5-MAIN: lines 683–713 and 849–985]

## 5. Repository and source map

### Source catalog

{md_table(['ID', 'File', 'Location', 'Runtime role'], source_rows)}

### Relationship map

`fno_v13_corrected_v5_backtest.py` imports V13-V3, V13-V2, corrected V6, the signal builder, hybrid data adapter, selector/replay helper, provenance and common paths. The main source pins V13-V3 by SHA-256 and refuses drift; V13-V3 in turn pins V13-V2, which pins corrected V6. Outputs are isolated under `C:/TradingData/eqidv2/fno_oi/strategy_research/v13_corrected_v5`. [SRC-V5-MAIN: lines 35–70, 177–190]

Data/configuration sources:

- `EQIDV2_RUNTIME_ROOT` defaults to `C:/TradingData/eqidv2`; `EQIDV2_DATA_5M_DIR`, `EQIDV2_DATA_1MIN_DIR`, `EQIDV2_REPORTS_DIR`, `EQIDV2_LIVE_SIGNALS_DIR`, cache/status variables may override subpaths. [SRC-PATHS]
- V13 hybrid research additionally reads `EQIDV2_FNO_V5_BACKTEST_EQUITY_5M_DIR` and `EQIDV2_FNO_V5_BACKTEST_EQUITY_1M_DIR`, defaulting under the runtime root. [SRC-DATA: lines 24–37]
- Strategy parameters are dataclasses/constants and CLI flags (`--profile`, `--through-day`, `--cost-bps`, `--cutoff`, cache/eligibility refresh flags), not a separate V13-V5 config file. `--cutoff` must normalize to `1515`. [SRC-V5-MAIN: lines 74–145, 2078–2105]
- Instrument masters are dated `near_month_YYYY-MM-DD.parquet` files plus `contract_registry.parquet`; raw futures/NIFTY data are under `fno_oi/raw_contracts_5m`. [SRC-COMMON], [SRC-PROV]
- V13-V5 has no runtime log schema, paper runner, live runner, dashboard adapter, batch launcher, scheduled task or service definition. Existing V6/V8/multi-paper/dashboard/scheduler code is version-specific reference material only. [SRC-PAPER], [SRC-LIVE], [SRC-DASH], [SRC-SCHED]
- Credential-named files exist in the workspace but were deliberately not opened. No secret is needed or appropriate for this report.

### Authoritative result artifacts

The authoritative headline is `fno_v13_corrected_v5_profile_comparison.csv`, tied to the provenance JSON and per-profile trades/daily/setups/breakdowns/stress/sensitivity/walk-forward/bootstrap CSVs. Research folders contain 1,529 recorded experiments plus rejection ledgers; they are supporting research, not independent test results. The two verified cache regimes contain 2,824 (`26AUG`) and 1,201 (`26SEP`) strict candidate rows. [Result provenance JSON]

## 6. System architecture

```mermaid
flowchart LR
  U[Dated near-month universe] --> F[NFO future 5m OI]
  E[NSE equity 1m OHLCV] --> A[Exact causal 5m aggregation]
  A --> J[Cash features + futures OI join]
  F --> J
  N[NIFTY future 5m] --> G[09:25 SHORT gate]
  J --> S[5m setup cells]
  G --> S
  E --> C[Exact S+1 confirmation and S+2 path]
  S --> C --> R[Rank/select] --> X[Scale-out simulator]
  X --> O[Trades, daily, metrics, stress, reports]
```

The executable architecture is a single offline Python process. Price and volume never come from the future; future data contributes only OI. Candidate selection is followed by raw one-minute path rematerialization before execution. There is no broker or dashboard node in the current V13-V5 runtime. [SRC-DATA], [SRC-V5-MAIN: lines 517–588, 2114–2135]

## 7. Complete end-to-end flow

```mermaid
flowchart TD
  A[Resolve eligible session and dated contract] --> B[Load NSE 1m and NFO future 5m]
  B --> C[Build complete end-labelled cash 5m]
  C --> D[EMA, price, volume and OI features]
  D --> E{{Loose directional candidate?}}
  E -- no --> Z[Reject]
  E -- yes --> F{{Exact S+1 directional candle?}}
  F -- no --> Z
  F -- yes --> G[Apply NIFTY gate and global OI cap]
  G --> H[Cell thresholds and deterministic rank]
  H --> I[Trigger at confirm high/low]
  I --> J{{Touched before 15:15?}}
  J -- no --> U[UNFILLED]
  J -- yes --> K[Fill at worse of trigger or gap open]
  K --> L[Stop vs T1, stop first on tie]
  L --> M[Partial T1 + runner BE/target]
  M --> N[15:15/max-hold close]
  N --> P[Subtract flat cost; log and aggregate]
```

Stage contract: raw inputs must contain finite complete OHLCV; output is rejected/selected order/fill/trade. Missing exact confirmation is skipped during candidate construction; selected execution paths fail closed if confirmation, first forward minute, continuous sequence or exact 15:15 is missing. Ranking ties resolve by traded value then symbol. [SRC-DATA: 316–377; SRC-SIGNAL: 271–351; SRC-SELECT; SRC-V5-MAIN: 618–680]

### Source-faithful pseudocode

```text
for eligible session using its point-in-time near-month mapping:
    cash_5m = aggregate exactly five real NSE 1m rows, end-labelled
    features = EMA9/20/50, prior-close return, prior-20-volume ratio, traded value
    join future OI at identical timestamp; require positive current/prior OI
    for 09:25..15:00 loose bullish/bearish candidates:
        require exact next minute S+1 and positive range
        LONG: close(S+1) > open(S+1) and close(S+1) > close(S)
        SHORT: close(S+1) < open(S+1) and close(S+1) < close(S)
        trigger = high(S+1) for LONG else low(S+1)
apply 09:25 SHORT NIFTY first-bar <= -0.05% gate
apply global OI change <= 1.00%
for each configured time/side cell:
    filter by price, OI, volume, body, directional wick, traded-value thresholds
    rank within day and retain cell max_entries
for each selected order:
    rebuild exact cash path S+2..15:15
    first trigger-touch fills at trigger, unless bar opens through trigger (use open)
    create 1.50% stop, 1.075% T1, 2.60% runner target from actual fill
    stop wins any T1 tie; if T1 first, realize 10%, move 90% runner stop to entry
    runner stop wins runner-target tie; otherwise exit at target or final close
    net_return_pct = weighted gross_return_pct - cost_bps / 100
aggregate arithmetic trade and daily percentage points; do not infer rupee/account P&L
```

## 8. Data and candle construction

The cash one-minute timestamp is treated as **candle end** in IST. Minute offsets 1–375 from 09:15 are eligible. A five-minute bar ending 09:25 is exactly 09:21–09:25; its OHLCV is first/max/min/last/sum. Groups not containing exactly five rows with first `end−4m` and last `end` are dropped. Rows flagged `gap_filled`, `opening_snapshot`, or `provisional_stale`, non-finite OHLCV, adjacent copied OHLCV, and invalid OI pairs are excluded. [SRC-DATA: lines 300–377]

Features:

```text
EMA_s[t] = EWM(close, span=s, adjust=False), s ∈ {{9,20,50}}
price_change_pct[t] = (close[t] / close[t-1] - 1) × 100
volume_ratio[t] = volume[t] / mean(volume[t-20:t-1]), min 5 prior bars
traded_value[t] = close[t] × volume[t]
oi_change_pct[t] = (OI[t] / OI[t-1] - 1) × 100
```

Verified limitation: the volume rolling window is not grouped/reset by session, so early bars can use prior-session bars. This is causal but may not match an intended same-session relative-volume definition. The join is an exact inner one-to-one timestamp merge. Timezone-naive inputs are localized to IST; aware inputs are converted. Data arrival lateness is not simulated; incomplete five-minute bars drop, while incomplete selected exit paths abort the run. [SRC-DATA: 398–441]

## 9. Five-minute setup logic

General direction gate before profile thresholds: LONG requires `EMA9 > EMA20 > EMA50`, rising valid OI, `oi_change_pct ≥ 0.05`, `volume_ratio ≥ 0.8`, and price change ≥ +0.10%; SHORT mirrors EMA order and price ≤ −0.10%. This loose gate builds a reusable superset. Profile filters below are stricter. [SRC-SIGNAL: lines 50–71, 231–240]

{md_table(['5m end', 'Confirm end', 'Side', 'Max/day', 'Picker', '|Price| %', 'OI min %', 'Vol ratio', 'Body min', 'Wick max', 'Min value', 'Observed orders/fills and results'], setup_rows)}

All times are **candle-end/evaluation times**, not opens. The confirm candle is the exact next minute; entry starts one minute after confirmation. The default profile adds 09:50 SHORT and 11:20 SHORT, and adds +0.10 to inherited wick caps (capped at 1.0). The global OI cap remains ≤1.00% before cell ranking. Only 09:25 SHORT has the NIFTY guard. [SRC-V5-MAIN: `profile_setups`; SRC-V3: `annotate_nifty_gate`; SRC-V2: `apply_policy`]

There is no cross-cell cooldown, global daily cap, direction lock or duplicate-symbol suppression. A symbol may be selected in more than one cell, and each trade is simulated independently. The sum of per-cell maxima is 16 potential orders/day; observed maximum was 8. The 09:35 SHORT rule has OI minimum 1.00% and global maximum 1.00%, so only exact-boundary values can pass; it produced zero orders. No expiry-day special logic exists, and all 89 observed fills were non-expiry-day.

## 10. One-minute entry logic

```mermaid
sequenceDiagram
  participant M5 as Five-minute bar S
  participant M1 as One-minute series
  participant Sel as Cell selector
  participant Sim as Simulator
  M5->>M1: Require exact S+1 candle
  M1->>M1: Direction, displacement, positive range
  M1->>Sel: Trigger = high/low(S+1)
  Sel->>Sel: Cell filters, rank, max entries/day
  Sel->>Sim: Materialize continuous S+2..15:15 path
  loop Every later 1m bar
    Sim->>Sim: First high≥trigger or low≤trigger
  end
  Sim-->>Sel: Fill or UNFILLED
```

Chronology is exact: a 09:25 five-minute signal uses only data through 09:25; 09:26 must be present and is the strict confirmation; first entry-eligible bar is 09:27. LONG confirmation requires green candle and close above the signal close; SHORT requires red candle and close below. Body ratio is `abs(close-open)/(high-low)`. Directional wick ratio is upper wick/range for LONG and lower wick/range for SHORT. These morphology fields are **selection filters on the confirmation candle**, not additional later confirmations. [SRC-SIGNAL: 271–351]

Not implemented: finite entry window, explicit signal expiration, pullback/retest/reclaim, trigger buffer, anti-chase maximum, ATR control, one-minute volume requirement, VWAP/AVWAP/reference-distance guard, premium/spread/depth, second chance, re-entry state, duplicate-order lock. Default trigger buffer and worse-fill slippage are 0; optional stress parameters exist but are not the headline run. [SRC-V5-MAIN: 683–713, 849–858]

Observed trigger delay: median/75th percentile = 1 minute; mean 5.12; 8/89 fills waited >5 minutes, 3 waited >60 minutes, maximum 137 minutes. This follows directly from scanning the full S+2..15:15 path for the first touch.

### Funnel

| Stage | Count | Definition |
| --- | ---: | --- |
| Strict-confirmed cache rows | 4,025 | 2,824 AUG-regime + 1,201 SEP-regime candidate records; broad reusable time/side superset, not unique configured setups. |
| Configured setup rules | 14 | Higher-frequency time/side cells. |
| Selected synthetic orders | 91 | After NIFTY/OI/cell thresholds and per-day ranking. |
| Valid continuous entry paths | 91 | All selected rows passed exact S+2..15:15 rematerialization. |
| Filled trades | 89 | Trigger touched; two remained `UNFILLED`. |
| Winners / losers | 65 / 24 | Net trade return >0 / <0 after flat cost. |
| First-target hits | 50 | 56.18% of fills; includes runner, T1→BE and T1→time outcomes. |
| Runner-target hits | 20 | 22.47% of fills. |

## 11. Instrument and strike selection

**Missing / not implemented.** The universe maps an equity symbol/token to a near-month futures symbol/token for OI. The selected/entered/exited instrument remains the NSE cash equity. The output explicitly writes `instrument_kind=NSE_CASH_EQUITY_WITH_FUTURES_OI`, `ce_pe=NOT_APPLICABLE`, and `strike_type=NOT_APPLICABLE`. There is no option expiry, strike, CE/PE, premium, lot size, option spread or option liquidity selection. [SRC-SIGNAL: 321–350; SRC-V5-MAIN: 1208–1255]

Therefore “successful CE/long” and “PE/short” below mean **directional analogues only**. A future implementation must first choose one contract: (a) NSE cash equity execution, (b) NFO stock future execution, or (c) long/short option execution. These are economically different strategies; mapping the current percentage stop/targets onto option premium would alter behavior and requires a new validation, not a wiring-only change.

## 12. Entry execution model

For LONG, a bar touches when `high ≥ trigger`; SHORT when `low ≤ trigger`. Fill equals trigger unless the eligible bar opens through it; then actual open is used. Optional worse-fill stress adjusts LONG upward and SHORT downward. The first touch is accepted without volume-at-price, queue, spread, tick-size or market-depth validation. [SRC-V5-MAIN: `_entry`, 683–713]

`entry_price = gap_open if adverse breakout gap else buffered_trigger`. Brackets are rebased to actual fill. A later stop gap fills at the adverse bar open; on the activation bar, the stop level is used because OHLC cannot order the open and intrabar trigger. Target gaps still fill at the target level, not a favorable open. This is conservative for favorable gaps and stop-first ambiguity, but optimistic about stop-entry liquidity. [SRC-V5-MAIN: 726–748, 849–985; SRC-TEST]

Same-minute entry/exit is allowed because exit scans include the entry bar. If its range covers stop and T1, stop wins. No tick is required at the trigger/target; OHLC touch is treated as executable. There is no order rejection, partial fill, cancellation, manual intervention or restart behavior in backtest.

## 13. Position sizing and capital model

Current implementation has **no quantity, capital, margin, lot, compounding or rupee P&L**. `net_profit` is deliberately `N/A_NO_CAPITAL_OR_POSITION_SIZE_MODEL`; “net_profit_pct” is a simple sum of per-trade percentage returns. Maximum observed synthetic concurrency was 7 positions, but capital was not reserved for them. [SRC-V5-MAIN: `metrics`, lines 1090–1105]

Recommended formulas for a future executable instrument:

```text
risk_budget_per_trade = available_capital × permitted_risk_fraction
risk_per_unit = abs(entry_price - protective_stop_price)
risk_per_lot = risk_per_unit × lot_size + estimated_round_trip_charges + adverse_slippage
permitted_lots = floor(risk_budget_per_trade / risk_per_lot)
quantity = permitted_lots × lot_size
required_capital = broker_margin_or_premium + simultaneous_position_reserve
                  + stress_drawdown_reserve + operational_cash_buffer
```

Illustrative scenarios only—not recommendations:

| Scenario | Hypothetical capital | Trade risk | Daily risk | Stop/lot/extra costs | Formula result | Other constraints |
| --- | ---: | ---: | ---: | --- | --- | --- |
| Conservative | ₹10,00,000 | 0.25% = ₹2,500 | 0.75% = ₹7,500 | ₹12 × 100 + ₹300 = ₹1,500/lot | floor(2,500/1,500) = 1 lot | Max 2 positions; 15% cash buffer; ≥10% stress-DD reserve. |
| Moderate | ₹20,00,000 | 0.50% = ₹10,000 | 1.50% = ₹30,000 | ₹10 × 200 + ₹400 = ₹2,400/lot | floor(10,000/2,400) = 4 lots | Max 3 positions; margin API pre-check; reduce if correlated. |
| Aggressive | ₹30,00,000 | 0.75% = ₹22,500 | 2.25% = ₹67,500 | ₹15 × 300 + ₹600 = ₹5,100/lot | floor(22,500/5,100) = 4 lots | Research illustration only; higher gap/slippage and capacity risk. |

If buying options, use the option-premium stop distance and lot size; if selling options/futures, broker margin and gap loss can dominate nominal stop risk. Query current order/basket margins and charges immediately before approval. Expiry-day gamma/liquidity requires a larger reserve or explicit exclusion. Scaling can worsen queue, slippage, freeze-quantity slicing and market impact; position size must follow risk, not desired profit.

## 14. Stop-loss, target, trailing, and exit logic

Default higher-frequency `ExitSpec`: initial stop 1.50%, T1 +1.075%, partial 10%, runner target +2.60%, runner stop `BREAKEVEN`, no maximum holding minutes. LONG levels multiply entry by `1−/+pct`; SHORT reverses signs. Weighted gross after T1 is `0.10×1.075 + 0.90×runner_return`. Net subtracts `cost_bps/100`, i.e. 5 bps becomes 0.05 percentage point once per filled trade. [SRC-V5-MAIN: 74–145, 849–985]

Exact priority:

1. Before T1: no stop and no T1 → final close; stop index ≤ T1 index → full stop; otherwise T1.
2. After T1: runner stop is actual entry immediately. Neither runner event → final close; BE index ≤ runner target index → breakeven runner; otherwise fixed runner target.
3. A later adverse stop gap uses the bar open. A target uses the exact target level.
4. Higher-frequency always uses exact 15:15 final close. Conservative may end at 180 minutes.

“Trailing stop” is **not implemented** (`trailing_stop_exits=0`). No stop ratchet follows favorable price. Emergency exit, data-failure exit, broker rejection, manual intervention and restart recovery are absent. Per-cell `stop_pct`/`target_pct` are stored as `native_*` comparator fields but the V5 scale-out headline ignores them in favor of the global profile exit—an important configuration clarity issue.

## 15. Guards and safety rails

{md_table(['Guardrail', 'Current?', 'Location / threshold', 'Purpose and adequacy', 'Recommended enhancement'], [
['Session eligibility', 'Yes', 'V5 `load_market`; coverage ≥0.99, mapped contract and stored future rows', 'Fails closed at session selection; good for stored OI presence, not execution data quality', 'Require per-symbol cash/future freshness/completeness manifest'],
['Exact candle completion', 'Yes', 'Hybrid exact five real 1m rows', 'Prevents provisional/gap-filled 5m bars', 'Mirror identical live seal rules and late-data quarantine'],
['Confirmation/path completeness', 'Yes', 'Exact S+1; selected S+2..15:15 continuous', 'Strong offline fail-closed check', 'Live stale-clock SLA and gap recovery'],
['Trading session limit', 'Partial', 'Signals configured 09:25–11:20; exit cutoff 15:15', 'No explicit new-entry cutoff or bounded trigger life', 'Cell-specific expiry, e.g. pre-registered N minutes; forbid new fills near close'],
['Maximum entries/day', 'Per cell only', '`max_entries` 1–2; sum 16; observed max 8', 'Does not cap portfolio load', 'Global per-session/per-symbol cap'],
['Maximum open positions', 'No', 'Observed overlap 7; not modeled', 'Capital/risk can stack', 'Atomic portfolio exposure gate'],
['Direction lock', 'No', 'No portfolio direction state', 'Can mix/add correlated exposures', 'Optional net/gross beta and side caps'],
['Duplicate order prevention', 'Config only', 'Duplicate time/side rule rejected', 'Does not prevent duplicate symbol/order request', 'Idempotency key + unique DB constraint'],
['Re-entry/cooldown', 'No', 'Independent setup cells', 'Repeated same symbol possible', 'Define symbol/day re-entry and cool-off'],
['Daily loss', 'No', 'Not modeled', 'Critical capital safety gap', 'Hard realized+unrealized daily stop'],
['Strategy drawdown', 'No', 'Metric only; no runtime brake', 'Cannot halt degradation', 'Rolling drawdown circuit breaker with approval reset'],
['Per-trade risk', 'No', 'No quantity/capital', 'Critical', 'Risk-budget sizing and broker margin preflight'],
['Max allocation/quantity', 'No', 'No quantities', 'Critical', 'Per-account, session, symbol and order caps'],
['Liquidity/spread', 'No', '`min_traded_value=0`; cash OHLC touch only', 'Inadequate', 'Executable instrument depth, spread, ADV and impact gates'],
['Stale data', 'Offline partial', 'Flagged stored rows excluded', 'No live heartbeat/staleness clock', 'Fail closed after SLA; cancel triggers; position-only mode'],
['Missing candle', 'Yes offline', 'Drop incomplete 5m; abort selected incomplete path', 'Good offline', 'Live backfill then quarantine; never synthesize signal bars'],
['Extreme price/tick size', 'No', 'Finite/positive range only', 'No band/tick validation', 'Instrument-master tick rounding, circuit/price-band checks'],
['Broker connectivity/rate limits', 'No', 'No broker in V13', 'Blocker', 'Token bucket, timeout, reconnect, read-only fallback'],
['Order status/partial/rejection', 'No', 'No order state', 'Blocker', 'Order update stream + polling reconciliation state machine'],
['Position reconciliation', 'No', 'No broker state', 'Blocker', 'Startup and periodic broker/local/ledger three-way recon'],
['Kill switch/manual pause', 'No V13', 'Generic dashboard patterns are not wired', 'Blocker', 'Persisted global/session/symbol kill scopes, two-step UI confirmation'],
['Automatic shutdown', 'No V13', 'Backtest terminates only', 'Blocker', 'Deadline-driven position-only → square-off → reconcile → stop'],
['Expiry restrictions', 'No', 'All evidence non-expiry day', 'Unable to verify', 'Explicit DTE/expiry allowlist and separate validation'],
['End-of-day square-off', 'Backtest only', 'Synthetic 15:15 close', 'No live order/confirmation', 'Submit before broker cutoff, confirm flat, escalate mismatch'],
['Credentials/security', 'N/A to backtest', 'No V13 live component', 'Unable to verify operational security', 'Secret store, least privilege, token redaction/rotation'],
])}

## 16. Trade-state machine

```mermaid
stateDiagram-v2
  [*] --> Candidate
  Candidate --> Rejected: guard/filter fails
  Candidate --> Selected: ranked in cell
  Selected --> Unfilled: trigger never touched
  Selected --> Open: first touch/gap fill
  Open --> FullStop: stop first or tie
  Open --> T1: T1 first
  Open --> TimeExit: cutoff/max hold
  T1 --> RunnerBE: entry stop first or tie
  T1 --> RunnerTarget: fixed target first
  T1 --> T1TimeExit: cutoff/max hold
  Rejected --> [*]
  Unfilled --> [*]
  FullStop --> [*]
  TimeExit --> [*]
  RunnerBE --> [*]
  RunnerTarget --> [*]
  T1TimeExit --> [*]
```

This is a replay state machine inferred from branch order in `simulate_scaleout`; no persistent runtime state machine exists. Every state lives only in the loop and emitted row. A production design must add submitted, acknowledged, partially-filled, cancel-pending, rejected, unknown and reconciliation-blocked states. [SRC-V5-MAIN: 849–990]

## 17. Timing and session behavior

| Event | Verified time/meaning |
| --- | --- |
| Cash session anchor | 09:15 IST; stored 1m bars end-labelled. |
| Earliest 5m evaluation | 09:25; 09:20 lacks prior 5m close for return. |
| Default profile setup ends | 09:25, 09:30, 09:35, 09:40, 09:45, 09:50, 09:55, 10:00, 11:20 depending on side. |
| Confirmation | Exactly one minute after each 5m end. |
| Earliest fill | Confirmation +1 minute. |
| Trigger expiry | None before 15:15 path end. |
| New-entry cutoff | Not separately implemented. |
| Forced exit | Backtest final 15:15 close; conservative profile can exit at 180 minutes. |

Historical data are converted/localized to IST, but there is no centralized live exchange clock. Late/incomplete 5m bars are omitted; exact selected paths abort on incompleteness. No holiday/market-status live check exists in V13; point-in-time eligibility supplies the historical session list. [SRC-DATA], [SRC-V5-MAIN]

## 18. Backtesting implementation

The driver validates pinned sources, resolves point-in-time session regimes, loads or builds checksum-bound caches, gates/ranks signals, rematerializes opens/timestamps from raw cash one-minute data, simulates three profiles and baselines, annotates context, then writes isolated CSV/JSON/Markdown outputs. Prior-version caches may seed read-only, but V5 writes only to its own cache. Cache payload binds contract universe hash, days, cutoff, max bars, confirmation policy, data-contract version and source hashes. [SRC-V5-MAIN: 177–190, 418–514]

No-look-ahead assessment: five-minute features use current/past completed bars; S+1 confirmation occurs after S; entry path starts S+2; session dates are enforced; selection happens before exit scanning. The original selection cache omits timestamps/opens from forward HLC, so V5 rebuilds them and validates continuity. This is strong causal construction. Remaining OHLC ambiguity concerns within a one-minute bar, not future leakage.

Reproduction command (read-only historical run, but it writes isolated outputs):

```powershell
python fno_v13_corrected_v5_backtest.py --profile higher_frequency --through-day 2026-09-03 --cost-bps 5 --cutoff 15:15
```

Do not pass `--rebuild-cache` or `--refresh-eligibility` when exact artifact reproduction is desired unless source data snapshots and hashes are independently frozen. This report did not rerun that command; it reran 38 tests + 12 subtests.

## 19. Backtest results and validation

### Profile comparison

{md_table(['Strategy/profile', 'Rules', 'Fills', 'Trades/day', 'Win %', 'T1 %', 'Runner %', 'PF', 'Net points %', 'Max DD %'], profile_rows)}

### Headline detail: higher-frequency at 5 bps

| Metric | Value | Meaning |
| --- | ---: | --- |
| Sessions / period | 25 / 2026-07-29 to 2026-09-03 | 17 positive, 7 negative, 1 flat day. |
| Selected / filled | 91 / 89 | 2 unfilled; 3 entry gap-through fills. |
| Wins / losses / breakeven | 65 / 24 / 0 | EOD profitable exits count as wins; wins are not target hits. |
| Win / T1 / runner hit rate | 73.034% / 56.180% / 22.472% | T1 numerator includes any first-stage touch. |
| Full stops / time-family exits / T1→BE | 9 / 49 / 11 | Stop 10.112%; no trailing exits. |
| Gross profitable / gross losing returns | +74.922 / −22.623 points | Both computed from **after-cost** trade returns despite “gross” labels. |
| Pre-cost / flat cost / net | +56.748 / −4.450 / +52.298 points | Arithmetic, not account return. |
| PF / expectancy | 3.312 / +0.588% per fill | PF = sum positive net / absolute sum negative net. |
| Avg win / avg loss / payoff | +1.153% / −0.943% / 1.223 | Percentage of synthetic cash-equity fill. |
| Max DD / duration | −2.913 points / 3 sessions | On cumulative daily arithmetic percentage points. |
| Max win/loss streak | 13 / 3 | Trade order. |
| Avg / median hold | 241.25 / 288 minutes | Long holding is driven by runner/time exits. |
| Avg MFE / MAE | +1.398% / −0.611% | From entry through selected exit bar. |

### Chronological periods

{md_table(['Period', 'Days', 'Fills', 'Win %', 'T1 %', 'PF', 'Net % points', 'Expectancy %', 'Max DD %'], period_rows)}

The split was used during research; all dates were inspected. `PSEUDO_TEST` is descriptive, not untouched test. The training edge of the higher-frequency additions fails above 5 bps according to the profile evidence string. There is no independent holdout, nested model selection or prospective trial.

### Breakdowns

Month:

{md_table(['Value', 'Fills', 'Wins', 'Losses', 'Win %', 'T1 %', 'PF', 'Net %'], breakdown_rows('month'))}

Weekday:

{md_table(['Value', 'Fills', 'Wins', 'Losses', 'Win %', 'T1 %', 'PF', 'Net %'], breakdown_rows('day_of_week'))}

Direction:

{md_table(['Value', 'Fills', 'Wins', 'Losses', 'Win %', 'T1 %', 'PF', 'Net %'], breakdown_rows('side'))}

Volatility regime:

{md_table(['Value', 'Fills', 'Wins', 'Losses', 'Win %', 'T1 %', 'PF', 'Net %'], breakdown_rows('volatility_regime'))}

Expiry status and CE/PE:

{md_table(['Value', 'Fills', 'Wins', 'Losses', 'Win %', 'T1 %', 'PF', 'Net %'], breakdown_rows('expiry_status'))}

{md_table(['Value', 'Fills', 'Wins', 'Losses', 'Win %', 'T1 %', 'PF', 'Net %'], breakdown_rows('ce_pe'))}

Exit reason:

{md_table(['Value', 'Fills', 'Wins', 'Losses', 'Win %', 'T1 %', 'PF', 'Net %'], breakdown_rows('exit_reason'))}

Timing/setup results are in section 9. Actual fill-time breakdown follows; setup end remains the more stable causal grouping because some triggers fill much later.

Entry time:

{md_table(['Value', 'Fills', 'Wins', 'Losses', 'Win %', 'T1 %', 'PF', 'Net %'], breakdown_rows('entry_time'))}

### Stress and local sensitivity

{md_table(['Stress', 'Fills', 'Win %', 'T1 %', 'PF', 'Net %', 'Max DD %'], stress_rows)}

{md_table(['Experiment', 'SL', 'T1', 'Partial', 'Runner', 'Win %', 'T1 %', 'PF', 'Net %', 'Pseudo net %'], sensitivity_rows)}

{md_table(['Fold', 'Start', 'End', 'Fills', 'Win %', 'T1 %', 'PF', 'Net %', 'Max DD %'], walk_rows)}

Bootstrap: 10,000 session-resamples with seed 20260904 gave 2.5/50/97.5 percentiles of {fmt(bootstrap.iloc[0].p02_5_net_pct, 3)} / {fmt(bootstrap.iloc[0].p50_net_pct, 3)} / {fmt(bootstrap.iloc[0].p97_5_net_pct, 3)} points and zero non-positive samples. This only resamples the same 25 inspected days and is not future-proof.

### Validation verdicts

| Check | Verdict | Evidence / limitation |
| --- | --- | --- |
| Look-ahead/future leakage | No confirmed leakage | S complete → exact S+1 confirm → S+2 path; point-in-time contract regime. |
| Candle boundaries | Verified | Exact 5-row end-labelled aggregation; selected paths continuous to 15:15. |
| Chronology | Verified at 1m granularity | OHLC cannot order events inside one minute; stop-first tie rule. |
| Realistic fills | Not verified | No queue/depth/spread/volume; target exact-touch fills. Gap entries/stops improved. |
| Costs/charges | Inadequate | Flat 5 bps once/trade, not turnover/legs/taxes/partial fills. |
| PF | Verified | Positive net sum / absolute negative net sum. |
| Net-profit denominator | Missing | Metric is arithmetic sum, not capital return. |
| Missing data | Strong offline fail-closed | Incomplete 5m dropped; selected path incomplete aborts. |
| Train/validation/test | Inadequate | Train/validation/pseudo-test only; no untouched test remains. |

### Complete day-wise ledger

{md_table(['Day', 'Period', 'Trades', 'Wins', 'Losses', 'T1', 'Stops', 'Net %', 'Cum %', 'DD %'], daily_rows)}

## 20. Trade-level worked examples

All examples are historical replay or synthetic; none is a live/paper order. Quantity, strike, option premium and rupee P&L are N/A because they do not exist in V13-V5.

### Example 1 — successful long directional analogue (SWIGGY, 2026-07-29)

09:30 cash 5m: price change +0.658953%, OI +0.215203%, volume ratio 2.150759, close 276.489990. EMA/directional cache and cell thresholds passed. Exact 09:31 confirmation: O 276.329987, H 277.399994, L 276.329987, C 277.279999; body ratio 0.887856, upper-wick ratio 0.112144. Trigger 277.399994. The 09:32 bar opened through at 277.600006, so actual open became fill. Stop 273.436006; T1 280.584706; runner 284.817606. T1 was realized on 10%; runner target exited 90% at 10:18. Gross weighted return 2.4475%; flat cost 0.05%; net 2.3975%; MFE 2.7197%, MAE −0.0360%, hold 46m. Contract/strike/quantity/rupee charges: N/A. [Trade CSV; SRC-V5-MAIN `_entry`, `simulate_scaleout`]

### Example 2 — successful short directional analogue (PFC, 2026-08-10)

09:40 cash 5m: price −0.258720%, OI +0.122328%, volume ratio 3.193180, close 404.799988. 09:41 confirmation O/H/L/C 404.800/404.800/403.950/404.000; body 0.94119, lower wick 0.05881. Trigger/fill 403.950012 at 09:42. Stop 410.009262; T1 399.607549; runner 393.447312. Runner target at 11:42. Gross 2.4475%, flat cost 0.05%, net 2.3975%, MFE 3.5524%, MAE −0.3218%, 120m. This is a SHORT cash-price replay, not a purchased PE. [Trade CSV]

### Example 3 — full stop (INDIANB, 2026-08-03)

09:55 setup: price +0.206113%, OI +0.2443%, volume 1.82194, signal close 850.8. Confirmation at 09:56 O/H/L/C 851.0/851.5/850.75/851.5; body 0.6667, upper wick 0. Trigger/fill 851.5 at 09:57. Stop 838.7275 (−1.50%) was reached at 15:12 before T1. Gross −1.50%, cost −0.05%, net −1.55%, MFE +0.2701%, MAE −1.5854%, hold 315m. [Trade CSV]

### Example 4 — guardrail rejection (DIXON, 2026-08-03; counterfactual only)

The 09:25 SHORT candidate passed stock-side prerequisites but NIFTY first-bar return was +0.0671%, above the required ≤−0.05%. `nifty_first_bar_gate_pass=False`; no V13-V5 order existed. The research ablation reports the removed gate admitted five trades and all lost. Any outcome quoted for DIXON without the gate is counterfactual and not part of headline V13-V5. [SRC-V3: 184–267; NIFTY_FIRSTBAR_ABLATION.md]

### Example 5 — missed entry (MOTHERSON, 2026-08-21)

11:20 SHORT passed; 11:21 O/H/L/C 168.39/168.40/168.35/168.37, body 0.40018 and lower wick 0.39988, trigger 168.35. No subsequent minute low through 15:15 touched the trigger, so `UNFILLED`; P&L and cost are absent. There is no second-chance rule beyond the same trigger remaining open. [Trade CSV]

### Synthetic same-candle ambiguity and “trailing” cases

If a LONG fills at 100 and one minute has high 101.10 and low 98.90 with stop/T1 at ±1%, both levels are possible; code chooses full stop −1% and flags ambiguity. If a T1 bar also touches entry and the runner target, runner breakeven wins. These behaviors are unit-tested. A trailing example is not supported: only immediate fixed breakeven is implemented. [SRC-TEST]

## 21. Inaccuracies, bugs, and problem register

The register contains **1 confirmed configuration defect** and **11 findings marked as live blockers**. “Blocker” means the capability must close before a live pilot; it does not necessarily mean the historical Python function is broken.

{md_table(['ID', 'Class / severity', 'Blocker', 'Evidence and consequence', 'Correction / test / result impact'], [
['R01', 'Live-readiness blocker / Critical', 'Yes', 'No executable instrument/expiry/strike/CE-PE; cash price + future OI only. Cannot construct faithful broker order.', 'Specify contract semantics; parity/backtest anew. Current results describe cash percentages, not options.'],
['R02', 'Missing guardrail / Critical', 'Yes', 'No capital, quantity, lot, margin or rupee risk. Up to 7 observed overlaps are costless.', 'Risk engine + margin/charges preflight + portfolio tests. Reported account return is unavailable.'],
['R03', 'Missing guardrail / High', 'Yes', 'Trigger remains active to 15:15; max observed delay 137m.', 'Pre-register per-cell expiry and rerun untouched OOS; will affect 8 fills >5m.'],
['R04', 'Unrealistic assumption / High', 'Yes', 'Trades simulate independently; no cash/margin/correlation contention.', 'Event-driven portfolio replay with atomic reservations. Headline could change materially.'],
['R05', 'Unrealistic assumption / High', 'Yes', 'Flat 5 bps once/trade ignores entry, partial T1, runner exit, taxes and turnover.', 'Instrument-aware per-leg fee/slippage model; reconcile broker contract notes. PF/net likely overstated.'],
['R06', 'Execution risk / High', 'Yes', '`min_traded_value=0`; no executable-contract spread/depth/volume/impact.', 'Spread/ADV/depth guards; tick replay. Fill count and edge may fall.'],
['R07', 'Live-readiness blocker / Critical', 'Yes', 'No V13 paper/live broker, persistent order/position state, restart or reconciliation.', 'Shared core + locked adapters + crash/reconcile integration suite. No effect on offline arithmetic.'],
['R08', 'Missing guardrail / High', 'Yes', 'No global daily loss, open-position, allocation, order-quantity or kill-switch enforcement.', 'Fail-closed risk service; property/chaos tests. Capital safety blocker.'],
['R09', 'Overfitting risk / High', 'Yes', 'Only 25 inspected sessions; no untouched test; sparse added legs ~5 fills each.', 'Freeze prospective holdout and promotion thresholds. Headline uncertainty is high.'],
['R10', 'Confirmed bug/config defect / Medium', 'No', '09:35 SHORT OI min=1.00% while global cap≤1.00%; only exact equality passes; zero orders.', 'Clarify intended bound and add reachability test. No current headline trades from cell.'],
['R11', 'Data-quality risk / Medium', 'No', 'Relative-volume rolling window is not session-grouped.', 'Decide intended definition; golden-bar test. Recalculation could change selection/results.'],
['R12', 'Execution risk / Medium', 'No', 'Entry and exit can share a 1m OHLC bar; event sequence unknown. Stop-first is pessimistic.', 'Tick replay or conservative bar exclusion. Zero ambiguous headline trades currently.'],
['R13', 'Unrealistic fill / High', 'Yes', 'Any OHLC trigger/target touch fills full size; no queue, volume-at-price or partial fills.', 'Marketable-limit simulator and broker sandbox/paper calibration. Could lower fills/targets.'],
['R14', 'Maintainability / Low', 'No', 'Per-cell native stop/target stored but global V5 `ExitSpec` controls headline.', 'Rename comparator fields or separate entry/exit configs; snapshot test. Current results use global exit.'],
['R15', 'Metric inaccuracy / Medium', 'No', '`net_profit_pct` is arithmetic sum without capital denominator/compounding.', 'Rename to summed_net_return_points until portfolio model. Values numerically correct but easy to misread.'],
['R16', 'Validation gap / Medium', 'No', 'No expiry-day fills and no HIGH-volatility fills.', 'Stratified data extension. Behavior in those regimes unable to verify.'],
['R17', 'Data-lineage risk / High', 'Yes', 'Env roots can change inputs; cache binds universe/source but not a manifest hash for every cash/future file.', 'Immutable snapshot manifest with every input hash. Reproduction can drift outside verified caches.'],
['R18', 'Documentation gap / Low', 'No', '“F&O”, “gross profit” and “target hit” labels can be misread.', 'Use explicit cash+future-OI, after-cost positive sum, and first-target labels. No numeric impact.'],
])}

No confirmed look-ahead, candle-boundary, timezone, duplicate time/side rule, state leakage between profile outputs, or PF formula bug was found. Silent broker exceptions, race conditions and dashboard/broker divergence are not assessable because no V13 operational implementation exists.

## 22. Backtest versus paper versus live differences

| Area | Backtest (current) | Paper (required; not implemented) | Live (required; not implemented) |
| --- | --- | --- | --- |
| Data source | Stored NSE 1m + NFO future/NIFTY 5m | Same sealed live feed, captured immutable | Exchange/broker stream + controlled fallback |
| Clock | Historical timestamps | Central exchange clock | Central exchange clock + NTP drift alarm |
| Candle completion | Exact stored rows | Seal after lateness SLA | Same seal; never revise traded signal silently |
| Signal timing | Immediate loop | Scheduled event | Scheduled event with latency budget |
| Entry price | Trigger or gap bar open | Synthetic marketable-limit fill model | Broker average price from fills |
| Fill/slippage | OHLC touch; stress optional | Spread/depth/latency model | Actual fills/partial fills |
| Rejection/partial | None | Injected states | OMS/order updates + reconciliation |
| Charges | Flat 5 bps/trade | Estimated per leg | Actual broker/exchange/tax ledger |
| Position state | Row-local | Persistent simulated ledger | Broker + local + audit ledger |
| Restart recovery | N/A | Reload state and replay events | Broker-first reconcile before action |
| Exit handling | 1m stop-first OHLC | Same logic through paper adapter | Protective/order manager + confirmed fills |
| Risk controls | None portfolio-level | Mandatory simulated risk engine | Same engine plus broker limits/kill switch |
| Logging/dashboard | CSV/Markdown after run | Event/heartbeat/session views | Same plus broker/recon/alerts |

```mermaid
flowchart LR
  Core[Pure V13-V5 decision core] --> HB[Historical replay adapter]
  Core --> PB[Paper broker adapter]
  Core --> KB[Kite live adapter]
  Clock[Exchange clock] --> Core
  Risk[Shared risk engine] --> PB
  Risk --> KB
  HB --> Ledger[Canonical event ledger]
  PB --> Ledger
  KB --> Ledger
  Ledger --> Metrics[Metrics and dashboard]
```

Identical decisions diverge because arrival order, latency, spread, partial fills, rejection, charges and state recovery differ. The shared core must emit intents, never call Kite directly. Each adapter must implement the same order/fill interface; historical replay must not expose future bars or backtest-only “instant fill” objects to paper/live code.

## 23. Kite Connect integration design

```mermaid
sequenceDiagram
  participant Feed as KiteTicker
  participant Core as Candle/core
  participant Risk as Risk gate
  participant OMS as Kite orders
  participant Recon as Reconciler
  Feed->>Core: ticks + exchange timestamps
  Core->>Core: seal 1m/5m; emit intent
  Core->>Risk: intent + config/session snapshot
  Risk-->>Core: approve/reject with reason
  Risk->>OMS: idempotent marketable-limit intent
  OMS-->>Risk: order_id (not a fill)
  OMS-->>Recon: order updates / partial fills
  Recon->>OMS: order history + positions fallback
  Recon->>Core: canonical filled quantity/average price
  Core->>Risk: exit intent
  Risk->>OMS: exit; confirm flat
```

Verified against the current official Kite Connect v3 documentation and installed official Python client `kiteconnect 5.0.1`:

- Authentication is a user-mediated login → short-lived `request_token` → checksum token exchange → `access_token`; the secret/token must not be exposed client-side. Access tokens normally expire at 06:00 next day. [Official authentication](https://kite.trade/docs/connect/v3/user/)
- `place_order` acceptance returns an order ID, **not execution confirmation**; inspect order history/details or asynchronous order updates. Regular orders support exchange, symbol, transaction, order type, quantity, product, validity, price/trigger, tag, market protection and autoslice. [Official orders](https://kite.trade/docs/connect/v3/orders/)
- WebSocket provides quotes and order updates; official limits say up to 3 connections/API key and 3,000 instruments/connection. The Python client auto-reconnects with exponential backoff. [Official WebSocket](https://kite.trade/docs/connect/v3/websocket/), [official Python SDK](https://kite.trade/docs/pykiteconnect/v4/)
- Refresh the instrument dump daily; use tokens only for the trading day mapping and persist tradingsymbol/exchange as durable identity. [Official instruments/quotes](https://kite.trade/docs/connect/v3/market-quotes/)
- Positions are available from portfolio APIs; order/basket margin and charges endpoints exist. [Official portfolio](https://kite.trade/docs/connect/v3/portfolio/), [official margins/charges](https://kite.trade/docs/connect/v3/margins/)
- Current documented limits: quote 1 req/s, historical 3 req/s, order placement and other endpoints 10 req/s; also 400 orders/minute, 5,000 orders/day and 25 modifications/order. Treat HTTP 429 as a fail/slowdown event, not a retry storm. [Official errors/rate limits](https://kite.trade/docs/connect/v3/exceptions/)

Recommended mode enum: `BACKTEST`, `PAPER`, `LIVE_LOCKED`, `LIVE_ENABLED`. Default `LIVE_LOCKED`. `LIVE_ENABLED` requires an immutable approved config hash, account allowlist, same-day human challenge/acknowledgement, passing preflight, zero unresolved reconciliation items and a time-limited arm lease. No report-generation process may arm it.

Order construction must explicitly choose `regular` variety; `NSE` or `NFO` exchange; `BUY`/`SELL`; a marketable `LIMIT` by default (or separately approved `MARKET`/`SL`/`SL-M` policy); `MIS` or `NRML` only after validating the chosen cash/future/option contract; `DAY` validity; tick-rounded price/trigger; lot-rounded quantity; and `autoslice`/manual slices for the verified freeze limit. No such defaults exist in V13-V5 today. Order lifecycle: validate instrument/token/tick/lot/freeze; query margin/charges; reserve risk atomically; create `client_order_key=session+intent+attempt`; submit once; persist request and response; confirm via updates/history; handle partial fill quantities; cancel/modify within state and rate limits; attach protective exit policy; periodically compare orders/trades/positions; at cutoff cancel entries, flatten, confirm flat, archive. Use an order `tag` for session traceability within its documented length.

Historical fallback may fill a **data gap only before a candle is sealed**; after a signal candle is sealed, correction requires quarantine and audit, not rewriting a live decision. On WebSocket reconnect, resubscribe, rebuild missing interval, reconcile broker, and remain position-only until freshness is restored. API timeouts are “unknown outcome”: query orderbook by idempotency context before any resubmit.

Staged rollout: historical validation → deterministic market replay → shadow mode → paper trading → read-only broker reconciliation → minimum-size live pilot → controlled scale-up. Every promotion requires signed evidence and rollback criteria; none is activated here.

## 24. Multi-session dashboard design

```mermaid
flowchart TB
  UI[Responsive dashboard] --> API[Read-only dashboard API]
  API --> Sessions[(sessions)]
  API --> Signals[(signals)]
  API --> Orders[(orders/fills)]
  API --> Positions[(positions)]
  API --> Metrics[(metrics/risk)]
  API --> Health[(heartbeats/alerts/recon)]
  C1[V13-V5 PAPER session A] --> Bus[Event bus / append-only log]
  C2[V13-V5 BACKTEST session B] --> Bus
  C3[Other strategy session C] --> Bus
  Bus --> Sessions
  Bus --> Signals
  Bus --> Orders
  Bus --> Positions
  Bus --> Metrics
  Bus --> Health
```

Session ID proposal: `v13v5-{{mode}}-{{profile}}-{{account_alias}}-{{YYYYMMDD}}-{{8char_uuid}}`. Every table key includes `session_id`; configuration snapshot/hash, mode, broker alias (never account secret), process ID, start/end, heartbeat, error, kill state and code/data hashes are immutable session attributes. Existing `log_dashboard_server.py` offers reusable card/heartbeat/kill-control patterns but no V13 schema or adapter. [SRC-DASH]

Core relational model:

```sql
sessions(session_id PK, strategy_version, mode, profile, account_alias,
         config_json, config_sha256, code_sha256, data_manifest_sha256,
         state, started_at, ended_at, heartbeat_at, kill_state, error_code)
signals(signal_id PK, session_id FK, symbol, side, setup_id, signal_ts,
        confirmation_ts, trigger, state, rejection_reason, features_json)
orders(order_pk PK, session_id FK, intent_key UNIQUE, broker_order_id,
       instrument, side, quantity, order_type, state, submitted_at, updated_at)
fills(fill_pk PK, order_pk FK, broker_trade_id UNIQUE, quantity, price, ts, charges)
positions(session_id FK, instrument, net_qty, avg_price, stop, target, state, updated_at)
risk_snapshots(session_id FK, ts, realised, unrealised, exposure, margin, drawdown)
heartbeats(session_id FK, component, ts, status, lag_ms, data_age_ms)
reconciliations(recon_id PK, session_id FK, ts, local_json, broker_json, outcome)
alerts(alert_id PK, session_id FK, severity, code, message, opened_at, ack_at, closed_at)
audit_events(event_id PK, session_id FK, ts, actor, event_type, payload_json, hash_chain)
```

Views: session overview, active signals, open positions, pending orders, completed trades, daily P&L/drawdown/risk, data freshness, service health, reconciliation, errors/alerts, exact configuration, and comparable backtests. To add a session, create a new immutable row/namespace and credentials alias, then launch a process whose write token is scoped to that session. Never reuse file names, in-memory globals or broker-order tags across sessions. Kill commands are scoped and audited; “kill all” requires separate confirmation.

## 25. Proposed micro-file and service architecture

```mermaid
flowchart LR
  Config[config/models/clock] --> Data[market_data/candle_builder/indicators]
  Data --> Setup[setup_engine_5m]
  Setup --> Entry[entry_engine_1m]
  Entry --> Instr[instrument_selector]
  Instr --> Size[position_sizer/risk_engine]
  Size --> OM[order_manager]
  BT[backtest_broker] --> OM
  Paper[paper_broker] --> OM
  Kite[kite_broker] --> OM
  OM --> Pos[position_monitor/exit_engine]
  Pos --> Recon[reconciliation/persistence]
  Recon --> Metrics[metrics/reporting/dashboard]
  Health[alerts/health/audit] --> Metrics
  Session[session_manager/scheduler/main] --> Data
  Session --> OM
```

Recommended decomposition; this is a migration plan, not implemented code:

{md_table(['File/service', 'Responsibility & interface', 'State/dependencies', 'Lifecycle/retry/failure/health/tests'], [
['config.py', 'Load/freeze typed config; `load_config()->FrozenConfig`', 'Owns no mutable state; env/secret aliases', 'Pre-market once; fail closed; schema/hash/golden tests'],
['models.py', 'Canonical Candle/Signal/Intent/Order/Fill/Position types', 'None', 'Import-time; compatibility/serialization tests'],
['clock.py', 'IST exchange calendar/deadlines; `now`, `schedule`', 'Clock offset', 'Always; NTP/calendar fail closes; fake-clock tests'],
['session_manager.py', 'Create/transition isolated sessions', 'Session lifecycle', '08:45–post-close; transactional retries; isolation tests'],
['scheduler.py', 'Deadline/event dispatcher', 'Job leases', '≤1s tick; bounded catch-up; missed-deadline tests'],
['market_data.py', 'Stream/backfill immutable observations', 'Subscriptions/cursors', 'Pre-open–close; exponential reconnect; freshness health/chaos tests'],
['candle_builder.py', 'Seal exact 1m/5m candles', 'Open buckets', 'Per tick; seal deadline; quarantine gaps; boundary tests'],
['indicator_engine.py', 'EMA/return/volume/OI features', 'Rolling windows', 'On sealed 5m; deterministic; parity/golden tests'],
['setup_engine_5m.py', 'Pure configured cell evaluation/rank', 'Daily cell counts', 'At setup ends; no retry after deadline; V5 parity tests'],
['entry_engine_1m.py', 'Strict confirm, trigger, expiry', 'Active triggers', 'Each sealed 1m; expire exactly; chronology tests'],
['instrument_selector.py', 'Map intent to cash/future/option', 'Daily master snapshot', 'Refresh pre-open; fail on ambiguity; expiry/tick/lot tests'],
['position_sizer.py', 'Risk-budget to quantity/lots', 'Reservations', 'Per intent; atomic; property tests'],
['risk_engine.py', 'Trade/daily/portfolio/kill gates', 'Risk counters/leases', 'Continuous; no permissive retry; invariant tests'],
['order_models.py', 'Broker-neutral state enum/transitions', 'None', 'Always; transition property tests'],
['backtest_broker.py', 'Causal replay fills', 'Historical queue', 'Replay only; deterministic; leak/fill tests'],
['paper_broker.py', 'Latency/spread/partial synthetic fills', 'Paper order book', 'Market hours; injected failures; calibration tests'],
['kite_broker.py', 'Kite REST/WebSocket adapter only', 'Connection/order cursor', 'Pre-open–flat; bounded retry; sandbox/contract tests'],
['order_manager.py', 'Idempotent submit/cancel/modify', 'Intent/order mapping', 'Continuous; unknown→reconcile; crash/replay tests'],
['position_monitor.py', 'Mark positions and generate exit intents', 'Open positions', 'Tick/1s; stale→risk; parity tests'],
['exit_engine.py', 'Pure stop/T1/runner/time priority', 'Per-position stage', 'Continuous/deadlines; stop-first; golden tests'],
['reconciliation.py', 'Broker/local/ledger comparison and repair plan', 'Mismatch cases', 'Startup + 30s + EOD; never auto-invent fills; scenario tests'],
['persistence.py', 'Transactions, outbox, snapshots', 'Canonical durable state', 'Always; retry with idempotency; power-loss tests'],
['metrics.py', 'Capital-aware P&L/PF/DD', 'Aggregates', 'On event/daily; recomputable; denominator tests'],
['reporting.py', 'Daily/evidence bundles', 'None', 'Post-recon; retry safe; snapshot tests'],
['dashboard_api.py', 'Read-only session views/command queue', 'No trading state', '08:30–17:00; auth/rate limit; isolation tests'],
['alerts.py', 'Dedup/escalate/ack alerts', 'Alert lifecycle', 'Continuous; backoff; delivery tests'],
['health.py', 'Heartbeat/readiness/liveness', 'Component health', '5s; stale→degraded; watchdog tests'],
['audit_log.py', 'Append-only hash-chained events', 'Audit chain', 'Every transition; fail closed on trade-write failure; tamper tests'],
['main.py', 'Composition root and mode lock', 'Process only', 'Scheduled; graceful deadline shutdown; mode/arming tests'],
])}

Migration: first freeze current behavior with golden trade rows and config/source hashes; extract pure data/setup/entry/exit functions without changing outputs; add backtest adapter parity; add event ledger; add locked paper adapter; introduce risk/instrument selector only behind new version/config; compare every historical intent/exit; then deprecate monolith. Rollback is selecting the prior immutable backtest package—not mixing schemas or silently switching execution logic.

## 26. Time-bound scheduling and lifecycle management

```mermaid
gantt
  title Proposed V13-V5 IST trading-day lifecycle
  dateFormat HH:mm
  axisFormat %H:%M
  section Control
  Init/config/master       :08:45, 25m
  Auth read-only check     :09:00, 10m
  section Data
  Connect/warm-up          :09:05, 10m
  Build/seal candles       :09:15, 360m
  section Strategy
  Setup/entry windows      :09:25, 120m
  Position-only monitoring :11:25, 230m
  Forced exit/reconcile    :15:10, 25m
  section Close
  Reports/archive/shutdown :15:35, 55m
```

Proposed IST schedule (not current V13 behavior):

| Component | Earliest/deadline/recurrence | Runtime/timeout/retry | Dependencies and late/missed behavior |
| --- | --- | --- | --- |
| Init/config | 08:45 / 08:55 / once | 5m / 30s / 0 on hash mismatch | Files/DB; late start blocks new entries. |
| Instrument refresh | 08:50 / 09:05 / daily | 10m / 60s / 3 backoff | Auth/network; missed blocks instrument-dependent mode. |
| Auth/profile read check | 09:00 / 09:10 / daily | 5m / 10s / 2 | Human token already provisioned; failure stays `LIVE_LOCKED`. |
| Warm-up/history | 09:00 / 09:14:30 / once | 10m / 30s / 3 | Master/data; missing bars quarantine symbols. |
| WebSocket | 09:05 / 09:14 / continuous | Until flat / 30s connect / exponential bounded | Auth/master; late connection blocks signals until warm. |
| Candle builder | 09:15 / per bar + lateness SLA / tick-driven | Continuous / 2s seal / no fabricated retry | Feed/clock; missed bar invalidates dependent setup. |
| 5m evaluations | Configured ends / +2s / event | <500ms / no post-deadline retry | Sealed candle/features; late result rejected/audited. |
| 1m confirmation | S+1 end / +2s / once per setup | <500ms / no stale retry | Sealed 1m; missing confirmation rejects setup. |
| Trigger monitoring | S+2 / explicit expiry / tick-driven | Until expiry / data SLA | Fresh feed/risk; stale cancels entry intent. |
| New-entry cutoff | Recommended 14:45 or earlier after research | Atomic transition | All trigger services; cancel pending entries, retain exits. |
| Position-only | Cutoff / 15:10 / continuous | Until flat | Broker/data/risk; stale escalates protective exit policy. |
| Forced square-off | 15:10 / 15:15 / repeated recon | 5m / 5s / bounded 3 | Open positions; mismatch pages human and blocks shutdown. |
| Reconciliation | Startup, every 30s, 15:16 | <10s / 5s / 3 | Broker+DB; unresolved = `RECONCILIATION_BLOCKED`. |
| Reports/archive | 15:20 / 16:00 / once | 20m / 5m / retry idempotent | Flat/recon; late report does not reopen session. |
| Shutdown | After flat+archive / 16:15 | 2m | Must prove flat or stay supervised position-only. |

Use one exchange-clock service and deadline queue. Local `sleep()` loops may wait only until a clock-owned deadline; they may not define market truth. Jobs require leases/idempotency so restart cannot double-run. Existing scheduler slot/heartbeat patterns are reusable but not V13-wired. [SRC-SCHED]

## 27. Monitoring, alerts, and reconciliation

```mermaid
flowchart TD
  H{{Heartbeat/data fresh?}} -- no --> P[Pause new entries]
  H -- yes --> R[Reconcile local ledger, orders, broker positions]
  R --> M{{Exact match?}}
  M -- yes --> C[Continue and record proof]
  M -- no --> U[Mark RECONCILIATION_BLOCKED]
  U --> K{{Capital at risk?}}
  K -- no --> A[Alert, cancel pending entries, investigate]
  K -- yes --> X[Activate scoped kill/square-off policy]
  X --> V[Confirm broker flat]
  V --> Q{{Flat confirmed?}}
  Q -- yes --> S[Seal session and incident]
  Q -- no --> E[Escalate human; keep supervised loop]
```

Metrics/SLOs: tick age, last sealed 1m/5m age, clock drift, queue latency, signal-to-intent latency, REST/WebSocket errors, reconnect count, order acknowledgement/fill latency, reject/partial rate, position/order/ledger mismatch, open risk, daily realized/unrealized P&L, drawdown, margin headroom, cutoff progress and heartbeat age. Warn on soft SLA; critical on stale data with open risk, unknown order outcome, reconciliation mismatch, kill failure or not-flat deadline.

Reconciliation rules: broker fills are authoritative for live quantity/average price; local intent is authoritative only for why an order was attempted; append-only ledger records both. If broker has position/local does not, pause entries and manage the broker position under emergency policy after human/automatic risk gate. If local has position/broker does not, mark closed only after order/trade/position history proof; never submit a blind “exit” that could reverse position. Unknown submission must be searched before retry. Every manual command records actor, reason, before/after and config hash.

## 28. Deployment and rollback plan

1. Package current V13-V5 report artifacts and golden replay with source/data hashes; no trading capability.
2. Extract shared core; acceptance is byte/row parity for all 91 orders and 89 fills at 5 bps.
3. Add event store and backtest adapter; migrate read-only dashboard.
4. Add `PAPER` adapter and failure injection; remain broker-disconnected.
5. Add `LIVE_LOCKED` read-only Kite adapter and reconciliation using secret store; orders impossible at type/config/network policy layers.
6. After all P0/P1/P2 acceptance, separately approve a minimum-size pilot with timed arm lease.

Deploy immutable versioned directory/container, DB migration transaction, config hash, health/readiness gate and canary session. Rollback cancels new entries, reconciles/handles any open broker risk, disables arm lease, restores prior read-only package/schema view and preserves the event log. Never roll back database/state by deleting records or launching an older binary against a newer incompatible order schema.

## 29. Improvement roadmap

| Priority | Problem/evidence | Proposed change / expected benefit | Risk & effort | Test / rollback / acceptance |
| --- | --- | --- | --- | --- |
| P0 | Instrument semantics absent (R01) | Freeze cash/future/option contract design; makes orders defined | High research, medium code | New version only; historical+prospective validation; acceptance complete trace from signal to valid contract. |
| P0 | No sizing/portfolio safety (R02/R04/R08) | Risk engine, reservations, daily/position/kill caps | High | Property/chaos tests; feature flag rollback; no order bypasses gate. |
| P0 | No durable order/recon/restart (R07) | Event ledger and broker-neutral state machine | High | Crash-at-every-transition tests; LIVE remains locked; exact recovery/recon. |
| P1 | Unbounded trigger life (R03) | Pre-register expiry per cell | Medium; can reduce trades | Compare frozen holdout; rollback config version; zero fill after expiry. |
| P1 | Unrealistic costs/liquidity/fills (R05/R06/R13) | Per-leg cost, depth/spread, partial/marketable-limit model | High | Calibrate paper/sandbox; conservative acceptance deltas documented. |
| P1 | No untouched test (R09) | Freeze prospective sessions and promotion gates | Time-bound | No tuning after reveal; acceptance minimum sample/regime coverage. |
| P1 | Data manifest incomplete (R17) | Hash every raw file and effective env/config | Medium | Reproduction on clean machine yields same ledger. |
| P2 | V13 paper/live adapters absent | Shared core + PAPER + LIVE_LOCKED read-only | High | Replay parity, sandbox, kill/recon drills; rollback to offline only. |
| P2 | Monitoring/recovery absent | Session dashboard, SLIs, alerts, runbooks | Medium | Failure drills and not-flat escalation; acceptance alert/recovery SLOs. |
| P3 | Dead 09:35 SHORT cell (R10) | Clarify/remove/fix only in new researched version | Low | Reachability and out-of-sample test; do not claim profit. |
| P3 | Relative volume/session semantics (R11) | Explicit grouped or cross-session definition | Low/medium | Golden feature rows; new data-contract version if changed. |
| P3 | Native/global exit config ambiguity (R14) | Separate comparator and active exit models | Low | Config snapshot tests; no result delta. |
| P4 | Regime/NIFTY/OI research | Pre-registered ablations on larger history | Research risk | Nested walk-forward and multiplicity control; no promotion on in-sample PF. |

No expected benefit is labelled profitable without untouched evidence. Any logic change creates a new strategy version and requires result regeneration.

## 30. Final readiness assessment

| Dimension | Status | Rationale |
| --- | --- | --- |
| Strategy validity | Conditionally ready | Logic is precise and reproducible; instrument economics and independent validation unresolved. |
| Data reliability | Conditionally ready | Strong candle/path checks; small horizon and incomplete full-file manifest. |
| Backtest credibility | Conditionally ready | Causal paths/gap fixes/tests; fills/costs/capital/OOS limitations. |
| Paper-trading readiness | Not ready | No executable contract, size, paper adapter, persistent state or risk/recon. |
| Live-execution readiness | Not ready | 11 blockers; no V13 order system. |
| Risk controls | Not ready | No portfolio/daily/capital/kill enforcement. |
| Monitoring | Not ready | Existing generic dashboard is not V13-connected. |
| Recovery | Not ready | No V13 restart/reconciliation implementation. |
| Security | Unable to verify | No V13 live secret/auth boundary; credential files deliberately not inspected. |
| Operational maintainability | Conditionally ready | Good provenance/tests; monolith and inherited/shadowed configuration need extraction. |

**Explicit verdict: Not ready for paper trading.** It is appropriate for controlled offline replay/research. It may move to paper only after a tradable instrument contract, risk sizing, bounded entries, realistic execution/cost model, persistent session/order state, reconciliation, kill switches and an untouched validation plan are implemented and tested. It must not move to live while any Critical/High blocker remains.

## 31. Glossary

| Term | Meaning here |
| --- | --- |
| OI | Open interest from the mapped NFO stock future; not cash volume. |
| PF | Profit factor: sum positive net trade returns / absolute sum negative net returns. |
| T1 | First target, +1.075% from actual fill in default profile. |
| Runner | Remaining 90% after T1; BE stop and +2.60% target. |
| BE | Breakeven at actual fill, not after costs. |
| MFE/MAE | Maximum favorable/adverse percentage excursion during the simulated holding interval. |
| End-labelled | Timestamp is the candle's final minute. |
| S/S+1/S+2 | 5m signal end / next 1m confirmation / first entry-eligible minute. |
| Pseudo-test | Last six sessions shown separately but already inspected during research; not untouched. |
| Gap-through | Eligible bar opens beyond trigger or later stop; actual open used when adverse as coded. |
| Cash+future-OI | Cash-equity OHLCV execution model with only OI fields joined from an NFO future. |
| Shadow | Research-only output not authorized for production. |

## 32. Appendices

### Appendix A — Result provenance and hashes

| Artifact | Value |
| --- | --- |
| Strategy version | `{provenance['strategy_version']}` |
| Evidence | `{provenance['evidence_status']}` |
| Default/requested profile | `{provenance['configured_default_profile']}` / `{provenance['requested_profile']}` |
| Sessions | {len(provenance['sessions'])} |
| Cost | {provenance['cost_bps']} bps flat proxy |
| Official cutoff | {provenance['official_cutoff']} |
| V13-V5 source SHA | `{provenance['v13_v5_source_sha256']}` |
| V13-V3 unchanged | `{provenance['v13_v3_source_unchanged']}` |
| Provenance JSON SHA | `{prov_hash}` |

Verified cache records:

{md_table(['Month', 'Source', 'Rows', 'Universe SHA', 'Cache parquet SHA', 'Manifest SHA'], [[
    x['month'], x['source'], x['rows'], x['payload']['universe_sha256'], x['v5_cache_parquet_sha256'], x['v5_cache_manifest_sha256']
] for x in provenance['cache_records']])}

### Appendix B — Official Kite references consulted

- [Authentication and user session](https://kite.trade/docs/connect/v3/user/)
- [Orders, states, tags, protection and autoslice](https://kite.trade/docs/connect/v3/orders/)
- [WebSocket market data and order updates](https://kite.trade/docs/connect/v3/websocket/)
- [Market quotes and instrument dump](https://kite.trade/docs/connect/v3/market-quotes/)
- [Portfolio positions](https://kite.trade/docs/connect/v3/portfolio/)
- [Margins and charges](https://kite.trade/docs/connect/v3/margins/)
- [Errors and API rate limits](https://kite.trade/docs/connect/v3/exceptions/)
- [Official Python SDK reference](https://kite.trade/docs/pykiteconnect/v4/)

Documentation was checked on 2026-09-04. Broker/exchange rules, products, margins, freeze quantities and rate limits can change; re-verify official documentation immediately before implementation.

### Appendix C — Evidence boundaries and unverified items

Verified from code/artifacts: setup/confirmation/entry/exit formulas, data convention, 25-session ledger, profile metrics, gap/ambiguity behavior, cache/source hashes and passing tests. Inferred: operational consequences of independent trade simulation and missing state. Comment-only evidence was not treated as runtime behavior. Missing/unverified: actual option/future tradability, charges, margins, historical bid/ask/depth, lot/freeze quantities, expiry-day performance, high-volatility performance, unseen future performance, broker reconciliation, service deployment/security and live dashboard correctness.

The attachment defining this report was read as a specification only: `{ATTACHMENT}`. No strategy source, result CSV, scheduled task, broker session, credentials, or production configuration was modified.

<details><summary>HTML/print note</summary><p>The companion standalone HTML contains the same substantive text, local navigation, embedded styles, accessible severity/status badges, table overflow handling, print rules, the Mermaid source, and locally rendered diagram equivalents with no CDN or tracking.</p></details>
"""


DIAGRAMS = [
    ("Complete system architecture", [["Dated universe", "NFO future OI", "Cash+OI join", "Setup cells"], ["NSE equity 1m", "Exact 5m candles", "S+1 confirm/S+2 path", "Scale-out/results"]]),
    ("Signal-to-exit flow", [["Load/validate", "5m candidate", "S+1 confirm", "Gate & rank", "Trigger touch"], ["Actual fill", "Stop vs T1", "Runner BE/target", "15:15 exit", "Costs/ledger"]]),
    ("Five-minute to one-minute sequence", [["5m S closes", "1m S+1 confirms", "Rank/select", "S+2 path starts", "Fill or expire"]]),
    ("Trade-state machine", [["Candidate", "Selected", "Open", "T1", "Runner target / BE / time"], ["Candidate", "Rejected", "End"], ["Selected", "Unfilled", "End"], ["Open", "Full stop / time", "End"]]),
    ("Backtest/paper/live adapters", [["Pure decision core", "Historical adapter", "Canonical ledger"], ["Pure decision core", "Paper adapter + risk", "Canonical ledger"], ["Pure decision core", "Kite adapter + risk", "Canonical ledger"]]),
    ("Kite market-data and order lifecycle", [["KiteTicker", "Candle/core", "Risk gate", "Order submit", "Updates/history", "Reconcile/exit"]]),
    ("Multi-session dashboard", [["Session A", "Append-only events", "Session-scoped DB", "Read-only API", "Dashboard"], ["Session B", "Append-only events", "Session-scoped DB", "Read-only API", "Dashboard"]]),
    ("Microservice dependency", [["Clock/config", "Data/candles", "Indicators/setup", "Entry/instrument", "Risk/orders", "Position/recon", "Metrics/UI"]]),
    ("Daily operational timeline", [["08:45 init", "09:05 connect", "09:15 candles", "09:25 setups", "entry cutoff", "15:10 exit", "15:20 reconcile/report"]]),
    ("Failure, reconciliation and kill-switch", [["Freshness check", "Pause entries", "Three-way reconcile", "Mismatch?", "Scoped kill", "Confirm flat", "Escalate/archive"]]),
]


def flow_html(title: str, rows: list[list[str]], mermaid_source: str) -> str:
    row_html = []
    for row in rows:
        bits = []
        for idx, node in enumerate(row):
            if idx:
                bits.append('<span class="arrow" aria-hidden="true">→</span>')
            bits.append(f'<span class="node">{html.escape(node)}</span>')
        row_html.append('<div class="diagram-row">' + "".join(bits) + '</div>')
    return (
        f'<figure class="rendered-diagram" role="img" aria-label="{html.escape(title)}">'
        f'<figcaption>{html.escape(title)}</figcaption>{"".join(row_html)}'
        f'<details><summary>Mermaid source</summary><pre><code>{html.escape(mermaid_source)}</code></pre></details></figure>'
    )


mermaid_blocks: list[str] = []


def replace_mermaid(match: re.Match[str]) -> str:
    source = match.group(1).strip()
    idx = len(mermaid_blocks)
    mermaid_blocks.append(source)
    title, rows = DIAGRAMS[idx]
    return "\n" + flow_html(title, rows, source) + "\n"


OUT_MD.write_text(report, encoding="utf-8")
html_markdown = re.sub(r"```mermaid\s*\n(.*?)```", replace_mermaid, report, flags=re.S)
md = MarkdownIt("commonmark", {"html": True, "linkify": False, "typographer": True}).enable(["table", "strikethrough"])
body = md.render(html_markdown)
soup = BeautifulSoup(body, "html.parser")

used: set[str] = set()
nav_items = []
for heading in soup.find_all(["h2", "h3"]):
    text_value = heading.get_text(" ", strip=True)
    slug = re.sub(r"[^a-z0-9]+", "-", text_value.lower()).strip("-") or "section"
    base = slug
    n = 2
    while slug in used:
        slug = f"{base}-{n}"
        n += 1
    used.add(slug)
    heading["id"] = slug
    if heading.name == "h2":
        nav_items.append((slug, text_value))

for table in list(soup.find_all("table")):
    wrapper = soup.new_tag("div", attrs={"class": "table-wrap"})
    table.wrap(wrapper)

badge_values = {
    "Critical": "critical", "High": "high", "Medium": "medium", "Low": "low",
    "Ready": "ready", "Conditionally ready": "conditional", "Not ready": "not-ready",
    "Unable to verify": "unknown", "Yes": "not-ready", "No": "ready",
}
for td in soup.find_all("td"):
    val = td.get_text(" ", strip=True)
    if val in badge_values:
        span = soup.new_tag("span", attrs={"class": f"badge {badge_values[val]}"})
        span.string = val
        td.clear()
        td.append(span)
    else:
        for severity in ("Critical", "High", "Medium", "Low"):
            suffix = " / " + severity
            if val.endswith(suffix):
                prefix = val[: -len(suffix)] + " / "
                span = soup.new_tag("span", attrs={"class": f"badge {badge_values[severity]}"})
                span.string = severity
                td.clear()
                td.append(prefix)
                td.append(span)
                break

nav = "".join(f'<a href="#{html.escape(slug)}">{html.escape(label)}</a>' for slug, label in nav_items)

css = r"""
:root{--ink:#172033;--muted:#5c667a;--bg:#f4f7fb;--paper:#fff;--line:#d8dfeb;--nav:#101827;--accent:#2563eb;--teal:#087f8c;--ok:#137a43;--warn:#8a5700;--bad:#b42318;--code:#0d1726}*{box-sizing:border-box}html{scroll-behavior:smooth}body{margin:0;overflow-x:hidden;background:var(--bg);color:var(--ink);font:15.5px/1.58 Inter,Segoe UI,Arial,sans-serif}.layout{display:grid;grid-template-columns:290px minmax(0,1fr);min-height:100vh}.sidebar{position:sticky;top:0;height:100vh;overflow:auto;background:var(--nav);color:#fff;padding:24px 18px}.sidebar h2{font-size:16px;margin:0 0 6px}.sidebar p{font-size:12px;color:#b9c4d7;margin:0 0 16px}.sidebar a{display:block;color:#d9e5f8;text-decoration:none;padding:5px 8px;border-left:2px solid transparent;font-size:12.5px}.sidebar a:hover,.sidebar a:focus{background:#1d2a40;border-left-color:#60a5fa}.content{min-width:0;width:min(1180px,calc(100% - 48px));margin:28px auto 80px;background:var(--paper);padding:52px 60px;border:1px solid var(--line);box-shadow:0 12px 36px rgba(24,39,75,.08);border-radius:12px}.content p,.content li,.content td{overflow-wrap:anywhere}h1{font-size:38px;line-height:1.15;margin:0 0 28px;color:#0d1b35}h2{font-size:25px;margin:54px 0 18px;padding-top:8px;border-bottom:2px solid #dce6f5;padding-bottom:8px;color:#102b55}h3{font-size:19px;margin:32px 0 12px;color:#164e63}h4{font-size:16px}p{margin:9px 0 14px}a{color:#1559b7}blockquote{margin:18px 0;padding:14px 18px;border-left:4px solid #d97706;background:#fff8e6;color:#533b00}.table-wrap{max-width:100%;overflow:auto;margin:16px 0 24px;border:1px solid var(--line);border-radius:8px}table{border-collapse:collapse;width:100%;min-width:680px;font-size:13px}th{position:sticky;top:0;background:#eaf1fb;color:#16335e;text-align:left}th,td{border-bottom:1px solid var(--line);padding:9px 10px;vertical-align:top}tbody tr:nth-child(even){background:#f8fafc}code{font-family:Cascadia Code,Consolas,monospace;font-size:.91em;background:#edf2f8;padding:.12em .3em;border-radius:4px}pre{max-width:100%;overflow:auto;background:var(--code);color:#d9e6f6;padding:16px;border-radius:8px;line-height:1.45}pre code{background:transparent;padding:0}.badge{display:inline-block;padding:2px 8px;border-radius:999px;font-weight:700;font-size:11px;white-space:nowrap}.badge.critical,.badge.not-ready{background:#fee4e2;color:#8f160f}.badge.high{background:#ffead5;color:#9a3412}.badge.medium,.badge.conditional{background:#fff1bd;color:#704600}.badge.low,.badge.unknown{background:#e8edf4;color:#445168}.badge.ready{background:#dcfce7;color:#116233}.rendered-diagram{max-width:100%;margin:24px 0;padding:18px;border:1px solid #bfd2ea;border-radius:10px;background:linear-gradient(145deg,#f7fbff,#eef5fb);overflow:auto}.rendered-diagram figcaption{font-weight:800;color:#123a63;margin-bottom:14px}.diagram-row{display:flex;align-items:center;gap:8px;min-width:max-content;margin:9px 0}.node{display:inline-flex;align-items:center;justify-content:center;min-height:46px;max-width:180px;padding:8px 12px;text-align:center;border:1px solid #7ba6cf;border-radius:8px;background:#fff;box-shadow:0 2px 8px rgba(20,70,110,.08)}.arrow{font-size:22px;color:var(--teal);font-weight:800}.rendered-diagram details{margin-top:14px}.rendered-diagram summary{cursor:pointer;color:#315b83;font-size:12px}details{border:1px solid var(--line);border-radius:7px;padding:8px 12px;margin:14px 0}summary{font-weight:700;cursor:pointer}.stamp{padding:10px 14px;background:#e8f2ff;border-left:4px solid var(--accent);font-size:13px;margin-bottom:24px}@media(max-width:900px){.layout{display:block}.sidebar{position:relative;height:auto;max-height:320px}.content{width:calc(100% - 20px);margin:10px;padding:28px 20px}h1{font-size:30px}h2{font-size:22px}}@media print{body{background:#fff;font-size:10.5pt}.layout{display:block}.sidebar{display:none}.content{width:100%;margin:0;padding:0;border:0;box-shadow:none}.table-wrap{overflow:visible;border:0}table{min-width:0;font-size:8pt}h2{break-before:page;margin-top:0;padding-top:12px}h2#2-document-metadata{break-before:auto}h3,figure,table,pre,blockquote{break-inside:avoid}.rendered-diagram{background:#fff}.rendered-diagram details{display:none}a{color:inherit;text-decoration:none}@page{size:A4;margin:14mm}}
"""

standalone = f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<meta name="robots" content="noindex,nofollow"><title>Corrected V13-V5 F&O Complete Report</title>
<style>{css}</style></head><body><div class="layout"><nav class="sidebar" aria-label="Report sections">
<h2>V13-V5 Complete Audit</h2><p>Standalone · generated {html.escape(generated)}<br>No live authorization</p>{nav}</nav>
<main class="content"><div class="stamp"><strong>Research only.</strong> Historical results do not guarantee future performance. No broker login or order occurred.</div>{str(soup)}</main>
</div></body></html>"""
OUT_HTML.write_text(standalone, encoding="utf-8")

print(json.dumps({
    "markdown": str(OUT_MD), "html": str(OUT_HTML),
    "markdown_sha256": sha(OUT_MD), "html_sha256": sha(OUT_HTML),
    "markdown_bytes": OUT_MD.stat().st_size, "html_bytes": OUT_HTML.stat().st_size,
    "mermaid_blocks": len(mermaid_blocks), "sections": len(nav_items),
}, indent=2))
