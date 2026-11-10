"""Rebuild G3 indicator comparisons into a NEW output folder only.

Run from the repository root with python -B -m g3_v18_20261102.run.
No publication, broker connection, or modification of existing G3 artifacts.
"""
from __future__ import annotations

import argparse
from datetime import date, datetime
import hashlib
import html
import io
import json
import os
from pathlib import Path
import sys
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

PACKAGE = Path(__file__).resolve().parent
ROOT = PACKAGE.parent
VERSION = "G3-V18-MARKET-PARTICIPATION-RESEARCH-20261102"
IST = ZoneInfo("Asia/Kolkata")
SPOT = Path("C:/TradingData/eqidv2/stocks_indicators_5min_eq_live/NIFTY50_INDEX_stocks_indicators_5min.parquet")


def sha(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(4 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def clean(value):
    if isinstance(value, dict):
        return {str(k): clean(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [clean(v) for v in value]
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.bool_,)):
        return bool(value)
    if isinstance(value, (float, np.floating)):
        return float(value) if np.isfinite(value) else None
    if isinstance(value, (date, datetime, Path)):
        return str(value)
    return value


def dump(path, value):
    Path(path).write_text(json.dumps(clean(value), indent=2, allow_nan=False, default=str) + "\n", encoding="utf-8")


def guard_writes(allowed):
    """Deny Python filesystem mutations outside this newly created package."""
    allowed = allowed.resolve()

    def check(path):
        if isinstance(path, (str, bytes, os.PathLike)):
            resolved = Path(os.fsdecode(path)).resolve()
            if not resolved.is_relative_to(allowed):
                raise PermissionError(f"V18 isolation: write outside new package denied: {resolved}")

    def audit(event, args):
        if event == "open":
            path, mode, flags = args
            if (mode and any(c in mode for c in "wax+")) or (flags and flags & (os.O_WRONLY | os.O_RDWR | os.O_CREAT | os.O_TRUNC | os.O_APPEND)):
                check(path)
        elif event in ("os.mkdir", "os.remove", "os.rmdir", "os.chmod", "os.utime", "os.truncate"):
            check(args[0])
        elif event in ("os.rename", "os.link", "os.symlink"):
            check(args[0])
            check(args[1])
    sys.addaudithook(audit)


def flags(series):
    return series.astype(str).str.lower().isin(("true", "1", "1.0"))


def metrics(book, days):
    """Zero-day aware after-cost results; drawdown is daily realized P&L."""
    cohort = book.loc[book.day.astype(str).str[:10].isin(days)].copy() if len(book) else book.copy()
    execution = flags(cohort.portfolio_executed) if "portfolio_executed" in cohort else pd.Series(False, index=cohort.index)
    fills = flags(cohort.filled) if "filled" in cohort else pd.Series(False, index=cohort.index)
    trades = cohort.loc[execution]
    pnl = pd.to_numeric(trades.get("portfolio_net_profit_rupees", pd.Series(dtype=float)), errors="raise")
    daily = (trades.assign(_pnl=pnl).groupby(trades.day.astype(str).str[:10])._pnl.sum()
             if len(trades) else pd.Series(dtype=float))
    daily = daily.reindex(days, fill_value=0.)
    curve = np.r_[0., daily.cumsum().to_numpy(float)]
    gains = float(pnl.clip(lower=0).sum())
    losses = float(-pnl.clip(upper=0).sum())
    return dict(sessions=len(days), selected_orders=len(cohort), native_fills=int(fills.sum()),
                trades=len(trades), wins=int(pnl.gt(0).sum()), losses=int(pnl.lt(0).sum()),
                win_rate_pct=100 * float(pnl.gt(0).mean()) if len(pnl) else None,
                net_pnl_rs=float(pnl.sum()), profit_factor=gains / losses if losses else None,
                average_trade_rs=float(pnl.mean()) if len(pnl) else None,
                daily_close_drawdown_rs=float((np.maximum.accumulate(curve) - curve).max()),
                worst_day_rs=float(daily.min()) if len(daily) else 0,
                best_day_rs=float(daily.max()) if len(daily) else 0,
                costs_rs=float(pd.to_numeric(trades.get("portfolio_cost_rupees", pd.Series(dtype=float))).sum()))


def changed_existing_files():
    before = json.loads((PACKAGE / "existing_files_before.json").read_text())
    changed = []
    for name, prior in before.items():
        path = Path(name)
        if not path.exists():
            changed.append(dict(path=name, status="missing"))
        elif sha(path) != prior["sha256"] or path.stat().st_mtime_ns != prior["mtime_ns"]:
            changed.append(dict(path=name, status="changed_since_task_start"))
    return dict(checked_files=len(before), unchanged=not changed, changes=changed,
                write_policy="Python writes restricted to new g3_v18_20261102 folder; existing inputs read only")


def chart(daily):
    width, height, pad = 1000, 300, 38
    variants = list(daily.variant.unique())
    curves = {v: daily.loc[daily.variant.eq(v)].sort_values("day").net_pnl_rs.cumsum().to_numpy() for v in variants}
    vals = np.concatenate([np.r_[0., x] for x in curves.values()])
    lo, hi = min(float(vals.min()), 0), max(float(vals.max()), 1)
    colors = ("#aab7c4", "#48d5bf", "#85acff", "#e9bd69", "#d39df2", "#fb8e9f", "#f0e7ad", "#62b7cc")
    lines = []
    for index, (name, curve) in enumerate(curves.items()):
        curve = np.r_[0., curve]
        points = " ".join(f"{pad+i*(width-2*pad)/max(1,len(curve)-1):.1f},{height-pad-(v-lo)*(height-2*pad)/(hi-lo):.1f}" for i, v in enumerate(curve))
        color = colors[index % len(colors)]
        lines.append(f'<polyline fill="none" stroke="{color}" stroke-width="2" points="{points}"><title>{html.escape(name)}</title></polyline>')
    legend = " ".join(f'<span style="color:{colors[i % len(colors)]}">&#9679; {html.escape(v)}</span>' for i, v in enumerate(variants))
    return f'<svg role="img" aria-label="Cumulative modeled net P and L" viewBox="0 0 {width} {height}"><text x="5" y="18" fill="#aab7c4">₹{hi:,.0f}</text>{"".join(lines)}<text x="5" y="295" fill="#aab7c4">₹{lo:,.0f}</text></svg><div class="legend">{legend}</div>'


def report(output, summary, overall, daily, monthly, coverage, candidates, decisions):
    def table(frame):
        return '<div class="table">' + frame.to_html(index=False, border=0, float_format=lambda v: f"{v:,.2f}", na_rep="—") + '</div>'
    main = overall.loc[overall.period.eq("all") & overall.cost_bps.eq(5),
                       ["variant", "trades", "win_rate_pct", "net_pnl_rs", "profit_factor", "daily_close_drawdown_rs", "average_trade_rs"]]
    title = "G3 V18 · Market participation research"
    coverage_note = (f"The requested {summary['requested_through']} session is not reported as complete because sealed execution inputs are missing."
                     if summary['requested_through'] not in summary['days'] else "The requested session is included from a complete sealed source.")
    restricted_note = ("October 28 is a separately declared restricted research cohort: 215 of 218 stocks; COROMANDEL, KARURVYSYA and SAIL are excluded because exact OI is missing or invalid. The failed original full-universe replay is preserved. No OI is fabricated."
                       if "2026-10-28" in summary.get("restricted_sessions", {}) else "")
    body = f'''<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><title>{title}</title><style>
body{{margin:0;background:#0c131e;color:#e8edf4;font:15px/1.6 system-ui,sans-serif}}main{{max-width:1380px;margin:auto;padding:40px 30px}}h1{{font-size:34px;line-height:1.2}}h2{{margin-top:36px;color:#d6e5fa}}p{{max-width:1100px}}.eyebrow{{color:#62d7c3;letter-spacing:2px;font-size:12px}}.notice{{padding:18px 22px;background:#17263a;border-left:4px solid #e9bd69;border-radius:4px}}.table{{overflow:auto}}table{{border-collapse:collapse;white-space:nowrap;width:100%;font-size:13px}}th{{text-align:left;background:#1b2b40;color:#a9c2e5}}td,th{{padding:9px 12px;border-bottom:1px solid #28384c}}tr:nth-child(even){{background:#111d2c}}a{{color:#71c7ff}}svg{{width:100%;background:#101d2d;border-radius:12px}}.legend{{display:flex;gap:16px;flex-wrap:wrap;font-size:12px}}code{{color:#9ad9ce}}.muted{{color:#9cadc3}}li{{margin:8px 0}}.cards{{display:flex;flex-wrap:wrap;gap:16px}}.card{{padding:20px;background:#142237;min-width:190px;border-radius:10px}}.card strong{{display:block;font-size:23px}}</style></head><body><main>
<div class="eyebrow">NEW ISOLATED VERSION · BACKTEST ONLY · NO LIVE AUTHORITY</div><h1>{title}</h1>
<p class="muted">Requested through {summary['requested_through']} · Created {html.escape(summary['created_at_ist'])}</p>
<div class="cards"><div class="card">Replayed sessions<strong>{summary['sessions']}</strong></div><div class="card">First result<strong>{summary['first_date']}</strong></div><div class="card">Latest complete result<strong>{summary['last_date']}</strong></div><div class="card">Fixed comparisons<strong>{len(summary['variants'])}</strong></div></div>
<p class="notice">{html.escape(summary['conclusion'])} {html.escape(coverage_note)} Missing days are listed below. No existing strategy, live settings, current pointers, or historical result files were edited by this runner.</p>
<p class="notice">{html.escape(restricted_note)} The headline totals include this restricted session where present; October without it is also reported below.</p>
<h2>What changed</h2><p>The candidate version adds market participation and stock leadership measures: same-clock F&amp;O breadth from the session open; stock return relative to true Nifty 50 spot; cumulative session VWAP and slope; volume-weighted close-location pressure. These are measurable proxies for market activity, not proof of institutional buying or guaranteed causes of price changes. Relative volume, price movement, and contract-specific OI remain in the decision rules.</p>
<p>The simplified branches remove EMA 9/20/50 stacking and hard candle body/wick thresholds, then reselect from the full pre-filter candidate pool. Directional one-minute confirmation, data validity, OI, volume, price, timing, position limits, targets and stops remain. Removal is tested, not assumed beneficial. Time-of-day relative volume, efficiency and range extension are exported as diagnostics, not additional tuned gates.</p>
<h2>Results after modeled costs</h2><p>₹10,00,000 portfolio, ₹1,00,000 allocated per entry, 5× exposure, 5 bps round-trip cost; unchanged G3 entry expiry, targets and staged stops. Drawdown below is based on daily realized P&amp;L, not intraday mark-to-market equity.</p>{table(main)}
<h2>Cumulative net P&amp;L</h2>{chart(daily)}
<h2>Predeclared comparison decisions</h2><p>A candidate qualifies for further paper evaluation only if October P&amp;L is at least the control, October daily-close drawdown is no worse, at least 50% of October control trades remain, and at least 20 October trades execute. This is a research screen, not statistical validation or permission to deploy.</p>{table(decisions)}
<h2>October and recent sessions</h2>{table(overall.loc[overall.period.ne('all') & overall.cost_bps.eq(5)])}
<h2>Higher-cost sensitivity</h2>{table(overall.loc[overall.period.eq('all'), ['variant','cost_bps','trades','win_rate_pct','net_pnl_rs','profit_factor','daily_close_drawdown_rs']])}
<h2>Monthly results</h2>{table(monthly)}
<h2>Data coverage and exclusions</h2>{table(coverage)}
<h2>Interpretation limits</h2><ul><li>The existing V17 slot allowlist was selected with outcomes through October 27. Earlier history and October comparisons reuse that history; they are not independent holdout results. Later sessions are a very small descriptive check.</li><li>Archived early universes and some stock-day completeness exclusions were determined after the event. Historical opening OI includes retrospective observations. These inherited limitations remain disclosed in source_provenance.json.</li><li>VWAP uses five-minute typical-price weighting, not tick-level exchange VWAP. Pressure is a close-location volume proxy, not bid/ask delta. Breadth is for the source F&amp;O cohort, not the entire NSE; it compares with today's open, not yesterday's close.</li><li>Missing feature history fails the corresponding gate. The 09:20 opening bar has no previous session VWAP slope; VWAP variants cannot admit that clock. The original control remains available.</li><li>Realistic execution still depends on spread, liquidity and slippage. The 9/15 bps comparisons stress total modeled friction; they are not a complete broker tax/fee model. Minute OHLC ambiguity follows the existing conservative stop-first simulator.</li><li>No timestamped corporate announcements, earnings, news, order book, historical sector membership or FII/DII intraday flows were available as validated causal inputs. They were not invented or backfilled.</li></ul>
<h2>Downloads and reproducibility</h2><p><a href="overall_comparison.csv">Overall comparison</a> · <a href="daily_results.csv">Day-by-day</a> · <a href="monthly_results.csv">Monthly</a> · <a href="setup_results.csv">Per setup</a> · <a href="cost_sensitivity.csv">Cost sensitivity</a> · <a href="coverage.csv">Coverage</a> · <a href="summary.json">Summary</a> · <a href="config.json">Frozen run configuration</a> · <a href="source_provenance.json">Input provenance</a> · <a href="preservation_check.json">Existing-file preservation check</a></p>
<p>Each variant has its own trade ledger and selection audit. Re-run with <code>python -B -m g3_v18_20261102.run --output g3_v18_20261102/results_NEW --through 2026-11-02</code>. Output must be a fresh directory under the new package; existing result folders are refused.</p>
<h2>Research basis</h2><p><a href="https://www.cmegroup.com/education/lessons/open-interest">CME: open interest</a> explains OI as outstanding contracts and a contextual trend-confirmation measure; it cannot identify the aggressor or a named investor. <a href="https://www.nseindia.com/market-data/analysis-and-tools-capital-market-snapshot">NSE market snapshot</a> publishes advances/declines as a market-participation view. <a href="https://nsearchives.nseindia.com/content/circulars/FAOP71777.pdf">NSE 2026 holiday circular</a> identifies the three exchange holidays in the missing-weekday audit. The specific feature definitions and thresholds here are testable research choices, not exchange-endorsed trading rules.</p>
</main></body></html>'''
    (output / "report.html").write_text(body, encoding="utf-8")


def run(args):
    # Import existing modules before installing the write guard: the legacy
    # runtime-path module calls mkdir(exist_ok=True) for existing directories.
    from g3_v18_20261102 import data, features, selection
    from fno_v13_v10_g_3_daily_replay import simulate
    from research.g3_0920_backtest import allocate
    from research import g3_slot_win70_contract as control
    output = args.output.resolve()
    if not output.is_relative_to(PACKAGE) or output == PACKAGE:
        raise ValueError("Output must be a new child directory of g3_v18_20261102")
    if output.exists():
        raise FileExistsError(f"Refusing to overwrite a result: {output}")
    guard_writes(PACKAGE)
    output.mkdir(parents=True)
    variants = list(selection.VARIANTS)
    days = [str(d) for d in data.available_days(str(args.through))]
    for pending in args.include_pending_day:
        if pending > str(args.through):
            raise ValueError("Pending source day exceeds requested cutoff")
        days = sorted(set(days) | {pending})
    if args.days:
        missing = set(args.days) - set(days)
        if missing:
            raise ValueError(f"Requested unavailable days: {sorted(missing)}")
        days = [d for d in days if d in args.days]
    if not days:
        raise ValueError("No complete input sessions")
    config = dict(version=VERSION, execution_authority=False, scope="BACKTEST_ONLY", variants=variants,
                  variant_definitions={v: selection.configuration(v) for v in variants},
                  defaults=dict(research_variant="simplified_advanced", production_replacement=False),
                  reference_contract=control.strategy_payload(), requested_through=str(args.through),
                  costs_bps=[5, 9, 15], thresholds_fitted_to_outcomes=False,
                  qualification_rule="October PNL>=control; daily DD<=control; trades>=50% control and>=20",
                  period_split="all; before October; October reused validation; post-October27 descriptive only")
    config["planned_calendar"] = days
    dump(output / "config.json", config)  # Frozen before looking at outcomes.
    fingerprint = sha(output / "config.json")
    spot_bytes = SPOT.read_bytes()
    spot_digest = hashlib.sha256(spot_bytes).hexdigest()
    (output / "nifty_spot_input.parquet").write_bytes(spot_bytes)
    spot = pd.read_parquet(io.BytesIO(spot_bytes))
    spot["ts"] = pd.to_datetime(spot.date, utc=True).dt.tz_convert(IST)
    ledgers, audits, proofs, feature_tables, day_coverage = {v: [] for v in variants}, {v: [] for v in variants}, [], [], []
    for day_number, day in enumerate(days):
        raw, minutes, proof = data.load_day(day)
        raw = raw.reset_index(drop=True)
        raw["sid"] = day_number * 1_000_000 + np.arange(len(raw), dtype=np.int64)
        context = data.context_bars(raw, minutes)
        universe = proof.get("breadth_universe", {}).get("count") or proof.get("universe", {}).get("universe_symbols") or context.tradingsymbol.nunique()
        enriched = features.enrich_day(context, spot.loc[spot.ts.dt.strftime("%Y-%m-%d").eq(day)], expected_universe=int(universe))
        extra_cols = [c for c in enriched if c.startswith("v18_")]
        enrichment = enriched[["tradingsymbol", "signal_ts", *extra_cols]].drop_duplicates(["tradingsymbol", "signal_ts"])
        if len(enrichment) != len(enriched):
            raise ValueError("Duplicate enrichment keys")
        raw["signal_ts"] = pd.to_datetime(raw.signal_ts, utc=True).dt.tz_convert(IST)
        raw = raw.merge(enrichment, on=["tradingsymbol", "signal_ts"], how="left", validate="one_to_one")
        raw["v18_breadth_universe_basis"] = proof.get("breadth_universe", {}).get("policy", "SOURCE_COHORT")
        feature_tables.append(raw[["sid", "day", "tradingsymbol", "signal_ts", "volume", *extra_cols]])
        restricted = proof.get("coverage", {}).get("restricted_research_cohort")
        day_coverage.append(dict(day=day, status="REPLAYED_RESTRICTED_RESEARCH_COHORT" if restricted else "REPLAYED_STANDARD_SOURCE", source_rows=len(raw),
                                 symbols=raw.tradingsymbol.nunique(), expected_universe=int(universe),
                                 note="215/218 eligible; COROMANDEL, KARURVYSYA and SAIL excluded for invalid/missing OI" if restricted else "Inherited source eligibility; see provenance",
                                 stock_features_ready=int(flags(raw.v18_stock_ready).sum()),
                                 market_features_ready=int(flags(raw.v18_market_ready).sum())))
        proofs.append({**proof, "day": day})
        for variant in variants:
            orders, audit = selection.select(raw, variant)
            paths = data.execution_paths(orders, minutes)
            ledger, _ = simulate(orders, paths, [date.fromisoformat(day)])
            ledger["strategy"] = "G3_V18_RESEARCH"
            ledger["strategy_version"] = VERSION
            ledger["strategy_fingerprint"] = fingerprint
            ledger["variant"] = variant
            ledger["execution_authority"] = False
            ledgers[variant].append(ledger)
            audits[variant].append(audit)
        print(f"{day} [{day_number+1}/{len(days)}] raw={len(raw)} " + " ".join(f"{v}={len(ledgers[v][-1])}" for v in variants), flush=True)
    return finalize(args, output, config, days, ledgers, audits, proofs, feature_tables, day_coverage, spot_digest)


def finalize(args, output, config, days, ledgers, audits, proofs, feature_tables, day_coverage, spot_digest):
    from research.g3_0920_backtest import allocate
    from g3_v18_20261102 import data, features
    variants = list(config["variants"])
    fingerprint = sha(output / "config.json")
    books = {v: pd.concat(rows, ignore_index=True, sort=False) for v, rows in ledgers.items()}
    restricted_sessions = {p["day"]: p["coverage"]["restricted_research_cohort"] for p in proofs
                           if p.get("coverage", {}).get("restricted_research_cohort")}
    periods = dict(all=days, before_october=[d for d in days if d < "2026-10-01"],
                   october=[d for d in days if "2026-10-01" <= d <= "2026-10-31"],
                   october_without_restricted=[d for d in days if "2026-10-01" <= d <= "2026-10-31" and d not in restricted_sessions],
                   post_slot_selection=[d for d in days if d > "2026-10-27"])
    overall_rows, daily_rows, monthly_rows, setup_rows = [], [], [], []
    for variant, book in books.items():
        book.to_csv(output / f"{variant}_trades.csv", index=False)
        pd.concat(audits[variant], ignore_index=True, sort=False).to_csv(output / f"{variant}_selection_audit.csv", index=False)
        for cost in config["costs_bps"]:
            stressed = book.copy()
            if cost != 5:
                filled = flags(stressed.filled)
                stressed["cost_pct"] = np.where(filled, cost / 100., np.nan)
                stressed["net_return_pct"] = np.where(filled, pd.to_numeric(stressed.gross_return_pct) - cost / 100., np.nan)
                stressed = allocate(stressed)
            for period, cohort_days in periods.items():
                if cohort_days:
                    overall_rows.append(dict(variant=variant, cost_bps=cost, period=period, **metrics(stressed, cohort_days)))
        for day in days:
            daily_rows.append(dict(variant=variant, day=day, **metrics(book, [day])))
        for month in sorted(set(d[:7] for d in days)):
            monthly_rows.append(dict(variant=variant, month=month, **metrics(book, [d for d in days if d.startswith(month)])))
        for setup in sorted(book.setup_id.unique()):
            setup_rows.append(dict(variant=variant, setup_id=setup, **metrics(book.loc[book.setup_id.eq(setup)], days)))
    overall, daily, monthly = pd.DataFrame(overall_rows), pd.DataFrame(daily_rows), pd.DataFrame(monthly_rows)
    october = overall.loc[overall.period.eq("october") & overall.cost_bps.eq(5)].set_index("variant")
    decisions = []
    if len(october):
        base = october.loc["control"]
        for variant, row in october.iterrows():
            if variant == "control":
                continue
            checks = dict(pnl_pass=row.net_pnl_rs >= base.net_pnl_rs,
                          drawdown_pass=row.daily_close_drawdown_rs <= base.daily_close_drawdown_rs + 1e-6,
                          sample_pass=row.trades >= max(20, base.trades * .5))
            decisions.append(dict(variant=variant, **checks, qualifies_for_more_paper_evaluation=all(checks.values()),
                                  october_pnl_delta_rs=row.net_pnl_rs - base.net_pnl_rs))
    decision_frame = pd.DataFrame(decisions)
    passing = [row["variant"] for row in decisions if row["qualifies_for_more_paper_evaluation"]]
    conclusion = ("The following fixed candidates passed the predeclared October research screen: " + ", ".join(passing) + ". They still need prospective paper evaluation."
                  if passing else "None of the added-indicator or simplified candidates passed all predeclared October improvement checks. Keep V17 as the reference; V18 remains an experimental comparison, not a proven upgrade.")
    coverage_info = data.coverage_status(today=str(args.through))
    coverage = pd.DataFrame(day_coverage)
    dump(output / "coverage_status.json", coverage_info)
    excluded = [dict(day=d, status="NO_COMPLETE_SOURCE_OR_EXCHANGE_HOLIDAY",
                     note=coverage_info.get("explicit_source_exclusions", {}).get(d, "Not assigned zero P&L"))
                for d in coverage_info.get("weekday_dates_without_complete_source", [])]
    holidays = {"2026-09-14": "Ganesh Chaturthi", "2026-10-02": "Mahatma Gandhi Jayanti", "2026-10-20": "Dussehra"}
    for item in excluded:
        if item["day"] in holidays:
            item.update(status="EXCHANGE_HOLIDAY", note=holidays[item["day"]])
        else:
            item["status"] = "NO_VALIDATED_COMPLETE_SOURCE"
    if str(args.through) not in days:
        excluded.append(dict(day=str(args.through), status=coverage_info["requested_day_state"],
                             note="Current requested session has no complete sealed cash-minute replay; P&L unavailable"))
    if excluded:
        coverage = pd.concat([coverage, pd.DataFrame([x if isinstance(x, dict) else dict(day=x, status="UNAVAILABLE") for x in excluded])], ignore_index=True)
    summary = dict(version=VERSION, scope="BACKTEST_ONLY", execution_authority=False,
                   created_at_ist=datetime.now(IST).isoformat(), requested_through=str(args.through),
                   first_date=days[0], last_date=days[-1], sessions=len(days), days=days, variants=variants,
                   restricted_sessions=restricted_sessions, standard_source_sessions=len(days)-len(restricted_sessions),
                   conclusion=conclusion, qualifying_research_variants=passing,
                   independent_holdout=False, existing_slot_policy_selected_through="2026-10-27",
                   config_sha256=fingerprint, coverage=coverage_info, overall_metrics=overall_rows)
    for name, frame in dict(overall_comparison=overall, daily_results=daily, monthly_results=monthly,
                            setup_results=pd.DataFrame(setup_rows), cost_sensitivity=overall.loc[overall.period.eq("all")],
                            comparison_decisions=decision_frame, coverage=coverage).items():
        frame.to_csv(output / f"{name}.csv", index=False)
    diagnostic = pd.concat(feature_tables, ignore_index=True)
    diagnostic = features.add_time_of_day_rvol(diagnostic)
    diagnostic.to_parquet(output / "candidate_features.parquet", index=False)
    legacy_code = {str(Path(module.__file__).resolve()): sha(module.__file__)
                   for name, module in list(sys.modules.items())
                   if name.startswith(("fno_v13", "research.", "strategy_slot_selection", "g3_one_minute", "eqidv2_runtime"))
                   and getattr(module, "__file__", None) and Path(module.__file__).suffix == ".py"}
    dump(output / "source_provenance.json", dict(sessions=proofs, nifty_spot=dict(path=str(SPOT), sha256=spot_digest,
                                                  frozen_copy="nifty_spot_input.parquet"),
                                                  new_code={p.name: sha(p) for p in PACKAGE.glob("*.py")},
                                                  legacy_code=legacy_code,
                                                  slot_policy_sha256=sha(ROOT / "configs/strategies/slot_win70_20261028.json")))
    preservation = changed_existing_files()
    dump(output / "preservation_check.json", preservation)
    summary["preservation_check"] = preservation
    dump(output / "summary.json", summary)
    report(output, summary, overall, daily, monthly, coverage, diagnostic, decision_frame)
    dump(output / "manifest.json", dict(version=VERSION, execution_authority=False, files={p.name: sha(p) for p in output.iterdir() if p.is_file()}))
    print(json.dumps(clean(dict(output=str(output), sessions=len(days), latest=days[-1], conclusion=conclusion,
                               existing_files_unchanged=preservation["unchanged"])), indent=2), flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=PACKAGE / "results_20261102")
    parser.add_argument("--through", type=date.fromisoformat, default=datetime.now(IST).date())
    parser.add_argument("--days", nargs="+")
    parser.add_argument("--include-pending-day", action="append", default=[],
                        help="Plan a date whose isolated source is being sealed; it must be complete when read")
    run(parser.parse_args())


if __name__ == "__main__":
    main()
