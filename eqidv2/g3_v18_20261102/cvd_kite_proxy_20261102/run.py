"""Backtest fixed 5m candle-volume CVD estimates against the native G3 control.

Reads a fully prepared feature Parquet and sealed G3 execution inputs. Writes
only to a fresh output folder in this isolated experiment. No live orders.
"""
from __future__ import annotations

import argparse
from datetime import date, datetime
import hashlib
import html
import io
import json
from pathlib import Path
import sys
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

from g3_v18_20261102 import run as common
from . import selection

PACKAGE = Path(__file__).resolve().parent
REFERENCE = PACKAGE.parent / "results_20261102_complete"
IST = ZoneInfo("Asia/Kolkata")
COSTS = (5, 9, 15)
PRICE_ATOL = .005
VOLUME_ATOL = .5


def fresh_output(path: Path) -> Path:
    path = path.resolve()
    if not path.is_relative_to(PACKAGE) or path == PACKAGE:
        raise ValueError("Output must be a NEW child of the isolated proxy directory")
    if path.exists():
        raise FileExistsError(f"Refusing to overwrite existing output: {path}")
    return path


def validate_feature_calendar(enriched: pd.DataFrame, days: list[str]) -> pd.DataFrame:
    """Fail before output creation when the feature clock or calendar is unusable."""
    required = {"symbol", "bar_end", "open", "high", "low", "close", "volume"}
    if required - set(enriched):
        raise ValueError("Proxy features must retain canonical symbol/bar_end and source OHLCV")
    if not isinstance(enriched.bar_end.dtype, pd.DatetimeTZDtype):
        if any(pd.isna(value) or pd.Timestamp(value).tzinfo is None for value in enriched.bar_end):
            raise ValueError("Proxy bar_end timestamps must be nonmissing and timezone-aware")
    out = enriched.copy()
    out["bar_end"] = pd.to_datetime(out.bar_end, utc=True).dt.tz_convert(IST)
    if out.bar_end.isna().any() or out.symbol.isna().any() or out.duplicated(["symbol", "bar_end"]).any():
        raise ValueError("Missing or duplicate proxy feature keys")
    if not days or not set(days).issubset(set(out.bar_end.dt.strftime("%Y-%m-%d"))):
        raise ValueError("Proxy features must represent every requested replay session")
    return out


def baseline_parity(actual: pd.DataFrame, saved: pd.DataFrame, days: list[str]) -> dict:
    keys = ["day", "tradingsymbol", "setup_id", "signal_ts"]
    def prepare(frame):
        out = frame.copy()
        out["day"] = out.day.astype(str).str[:10]
        out = out.loc[out.day.isin(days)]
        out["signal_ts"] = pd.to_datetime(out.signal_ts, utc=True).astype(str)
        if out.duplicated(keys).any():
            raise ValueError("Duplicate native control identity")
        return out.sort_values(keys, kind="stable").reset_index(drop=True)
    left, right = prepare(actual), prepare(saved)
    if not left[keys].astype(str).equals(right[keys].astype(str)):
        raise ValueError("Native control differs from saved V18 order identities")
    for column in ("filled", "portfolio_executed"):
        if not common.flags(left[column]).equals(common.flags(right[column])):
            raise ValueError(f"Native control differs from saved V18: {column}")
    for column in ("signal_id", "exit_reason", "portfolio_status"):
        if column in left and column in right and not left[column].astype(str).equals(right[column].astype(str)):
            raise ValueError(f"Native control differs from saved V18: {column}")
    for column in ("entry_price", "exit_price", "portfolio_net_profit_rupees"):
        a = pd.to_numeric(left.get(column, pd.Series(np.nan, index=left.index)), errors="raise")
        b = pd.to_numeric(right.get(column, pd.Series(np.nan, index=right.index)), errors="raise")
        if not np.allclose(a, b, rtol=0, atol=1e-6, equal_nan=True):
            raise ValueError(f"Native control differs from saved V18: {column}")
    for column in ("entry_ts", "exit_ts"):
        a, b = (pd.to_datetime(frame[column], utc=True) for frame in (left, right))
        if not (a.eq(b) | (a.isna() & b.isna())).all():
            raise ValueError(f"Native control differs from saved V18: {column}")
    return dict(passed=True, selected_orders=len(left), sessions=len(days),
                reference="results_20261102_complete/control_trades.csv")


def source_comparison(raw: pd.DataFrame, enriched: pd.DataFrame, day: str):
    """Report independently fetched candle differences without changing G3."""
    fields = [name for name in ("open", "high", "low", "close", "volume") if name in raw and name in enriched]
    left = raw[["tradingsymbol", "signal_ts", *fields]].copy()
    left["signal_ts"] = pd.to_datetime(left.signal_ts, utc=True)
    right = enriched[["symbol", "bar_end", *fields]].copy()
    right["bar_end"] = pd.to_datetime(right.bar_end, utc=True)
    merged = left.merge(right, left_on=["tradingsymbol", "signal_ts"], right_on=["symbol", "bar_end"],
                        how="left", validate="one_to_one", suffixes=("_g3", "_proxy"))
    matched = merged.bar_end.notna()
    result = dict(day=day, g3_rows=len(raw), matched_candle_rows=int(matched.sum()),
                  missing_candle_rows=int((~matched).sum()), price_absolute_tolerance=PRICE_ATOL,
                  volume_absolute_tolerance=VOLUME_ATOL, relative_tolerance=1e-8)
    any_mismatch = pd.Series(False, index=merged.index)
    for field in fields:
        old = pd.to_numeric(merged[field + "_g3"], errors="coerce")
        new = pd.to_numeric(merged[field + "_proxy"], errors="coerce")
        usable = matched & np.isfinite(old) & np.isfinite(new)
        tolerance = VOLUME_ATOL if field == "volume" else PRICE_ATOL
        mismatch = usable & ~np.isclose(old, new, rtol=1e-8, atol=tolerance)
        any_mismatch |= mismatch
        result[field + "_mismatch_rows"] = int(mismatch.sum())
        result[field + "_max_absolute_difference"] = float((new[usable] - old[usable]).abs().max()) if usable.any() else None
    result["any_ohlcv_mismatch_rows"] = int(any_mismatch.sum())
    return result


def stressed_book(book: pd.DataFrame, cost: int, allocator) -> pd.DataFrame:
    out = book.copy()
    filled = common.flags(out.filled)
    out["cost_pct"] = np.where(filled, cost / 100., np.nan)
    out["net_return_pct"] = np.where(filled, pd.to_numeric(out.gross_return_pct, errors="raise") - cost / 100., np.nan)
    return allocator(out)


def verify_accounting(books, overall, daily, monthly, days):
    checks = []
    for variant, book in books.items():
        if book.sid.duplicated().any() or book.signal_id.duplicated().any():
            raise ValueError(f"Duplicate trade identity: {variant}")
        filled, executed = common.flags(book.filled), common.flags(book.portfolio_executed)
        signal = pd.to_datetime(book.signal_ts, utc=True)
        confirm = pd.to_datetime(book.confirmation_ts, utc=True)
        entry = pd.to_datetime(book.entry_ts, utc=True)
        if not confirm.eq(signal + pd.Timedelta(minutes=1)).all() or not entry.loc[filled].gt(confirm.loc[filled]).all():
            raise ValueError(f"Execution chronology failed: {variant}")
        if (executed & ~filled).any():
            raise ValueError(f"Unfilled order executed: {variant}")
        actual = float(book.loc[executed, "portfolio_net_profit_rupees"].sum())
        own = overall.loc[overall.variant.eq(variant) & overall.period.eq("all")].set_index("cost_bps")
        d = daily.loc[daily.variant.eq(variant)]
        m = monthly.loc[monthly.variant.eq(variant)]
        if "cost_bps" in d:
            d = d.loc[d.cost_bps.eq(5)]
            m = m.loc[m.cost_bps.eq(5)]
        if d.day.tolist() != days or not np.allclose([own.loc[5, "net_pnl_rs"], d.net_pnl_rs.sum(), m.net_pnl_rs.sum()], actual, atol=1e-6, rtol=0):
            raise ValueError(f"P&L accounting failed: {variant}")
        for cost in COSTS:
            expected = actual - int(executed.sum()) * 500_000 * (cost - 5) / 10_000
            if own.loc[cost, "trades"] != int(executed.sum()) or not np.isclose(own.loc[cost, "net_pnl_rs"], expected, atol=1e-6, rtol=0):
                raise ValueError(f"Fixed-cost reconciliation failed: {variant}/{cost}")
            if "cost_bps" in daily:
                cost_days = daily.loc[daily.variant.eq(variant) & daily.cost_bps.eq(cost)]
                cost_months = monthly.loc[monthly.variant.eq(variant) & monthly.cost_bps.eq(cost)]
                if cost_days.day.tolist() != days or not np.allclose(
                        [cost_days.net_pnl_rs.sum(), cost_months.net_pnl_rs.sum()], expected, atol=1e-6, rtol=0):
                    raise ValueError(f"Cost daily/monthly accounting failed: {variant}/{cost}")
        checks.append(dict(variant=variant, trades=int(executed.sum()), net_pnl_rs=actual,
                           chronology=True, accounting=True, cost_sensitivity=True))
    return checks


def render_report(output, summary, overall, daily, coverage, differences):
    def table(frame):
        return frame.to_html(index=False, border=0, na_rep="Unavailable", float_format=lambda n: f"{n:,.2f}")
    columns = ["variant", "trades", "win_rate_pct", "net_pnl_rs", "profit_factor", "daily_close_drawdown_rs"]
    full = overall.loc[overall.period.eq("all") & overall.cost_bps.eq(5), columns]
    october = overall.loc[overall.period.eq("october") & overall.cost_bps.eq(5), columns]
    late = overall.loc[overall.period.eq("post_slot_selection") & overall.cost_bps.eq(5), columns]
    text = f'''<!doctype html><html lang="en"><meta charset="utf-8"><meta name="viewport" content="width=device-width">
<title>G3 V18 — estimated CVD comparison</title><style>
body{{font:16px/1.55 system-ui,sans-serif;max-width:1200px;margin:32px auto;padding:0 20px;color:#183044;background:#f5f8fb}}
h1{{font-size:30px}} .notice{{padding:18px;background:#fff1ce;border-left:5px solid #c98c16}} table{{border-collapse:collapse;background:white;font-size:14px;display:block;overflow:auto}}
th,td{{padding:9px 12px;border-bottom:1px solid #dce5eb;text-align:right}}th:first-child,td:first-child{{text-align:left}} a{{color:#075cb6}}</style>
<h1>G3 V18: five-minute estimated CVD</h1>
<p class="notice"><strong>Candle-volume estimate, not executed buyer/seller order flow.</strong>
The primary estimate is volume × (2 × close − high − low) / (high − low): closing near the high gives positive estimated delta; closing near the low gives negative estimated delta. Zero-range candles receive zero delta. The separate body/range sensitivity uses volume × (close − open) / (high − low).
This research comparison neither identifies institutions nor places live orders.</p>
<p>{len(summary['days'])} completed sessions, {html.escape(summary['days'][0])} through {html.escape(summary['days'][-1])}.
Requested through {html.escape(summary['requested_through'])}. Unsealed sessions are excluded from execution P&amp;L.</p>
<h2>Results at 5 bps round-trip cost</h2>{table(full)}
<p>Compare proxy variants with <strong>covered_control</strong>, which uses identical data readiness and warmup.
<strong>base_control</strong> retains original G3 availability. All ranking, quotas, confirmations, capital and exits remain native G3.</p>
{common.chart(daily.loc[daily.cost_bps.eq(5)])}
<h2>October comparison</h2>{table(october)}
<h2>After October 27: small descriptive check</h2>{table(late)}
<h2>Higher-cost sensitivity</h2>{table(overall.loc[overall.period.eq('all'), ['variant','cost_bps','trades','net_pnl_rs','daily_close_drawdown_rs']])}
<h2>Coverage</h2>{table(coverage)}
<h2>Fetched candles versus original G3 signal candles</h2>{table(differences)}
<p>Differences are reported with absolute tolerances of ₹{PRICE_ATOL} for OHLC and {VOLUME_ATOL} shares for volume, plus relative tolerance 1e-8.
Fetched bars supply only the new proxy features; original G3 signal features and execution minutes are not replaced. Vendor corrections or adjustment changes can affect this comparison.</p>
<h2>Interpretation limits</h2><ul>
<li>Session estimates reset daily. Recent estimates use three completed five-minute bars. All directional thresholds are fixed at greater than zero; no outcome-driven tuning was performed.</li>
<li>Missing, flagged or discontinuous candles fail the common readiness gate. Each covered variant has the same three-bar warmup.</li>
<li>The slot policy was selected using results through October 27. Earlier results and October are reused research history, not an independent holdout.</li>
<li>Historical fetched candles were obtained retrospectively. Original delivery latency and subsequent vendor corrections are unverified.</li>
<li>Existing source universe and OI limitations remain. October 28 uses the restricted 215-stock cohort when included.</li>
<li>Drawdown is daily realized P&amp;L, not intraday mark-to-market. Costs of 5, 9 and 15 bps are total modeled round-trip friction, not an itemized broker-tax model.</li></ul>
<p><a href="overall_comparison.csv">All results</a> · <a href="daily_results.csv">Daily results</a> · <a href="monthly_results.csv">Monthly results</a> · <a href="summary.json">Summary</a> · <a href="config.json">Frozen configuration</a> · <a href="verification.json">Verification</a></p></html>'''
    (output / "report.html").write_text(text, encoding="utf-8")


def run(args):
    from g3_v18_20261102 import data
    from . import features
    from fno_v13_v10_g_3_daily_replay import simulate
    from research.g3_0920_backtest import allocate
    output = fresh_output(args.output)
    input_bytes = args.features.read_bytes()
    enriched = pd.read_parquet(io.BytesIO(input_bytes))
    reference_summary = json.loads((REFERENCE / "summary.json").read_text(encoding="utf-8"))
    days = sorted(set(data.available_days(args.through)).intersection(reference_summary["days"]))
    if args.days:
        if set(args.days) - set(days):
            raise ValueError("Requested day lacks a sealed G3 source and pinned baseline")
        days = [day for day in days if day in args.days]
    if not days:
        raise ValueError("No sealed replay sessions")
    enriched = validate_feature_calendar(enriched, days)
    reference_path = REFERENCE / "control_trades.csv"
    reference_bytes = reference_path.read_bytes()
    saved = pd.read_csv(io.BytesIO(reference_bytes), low_memory=False)
    consumed = {str(Path(module.__file__).resolve()): common.sha(module.__file__)
        for name, module in list(sys.modules.items())
        if name.startswith(("fno_v13", "research.", "g3_v18_20261102", "strategy_slot_selection", "g3_one_minute", "eqidv2_runtime"))
        and getattr(module, "__file__", None) and Path(module.__file__).suffix == ".py"}
    # The CLI executes this file as __main__, outside the package-name filter.
    consumed[str(Path(__file__).resolve())] = common.sha(__file__)
    config = {**selection.configuration(), "requested_through": args.through, "planned_calendar": days,
        "input_path": str(args.features.resolve()), "input_sha256": hashlib.sha256(input_bytes).hexdigest(),
        "interval_minutes": 5, "costs_bps": list(COSTS), "reference_control_sha256": hashlib.sha256(reference_bytes).hexdigest(),
        "scope": "BACKTEST_ONLY", "source_code_sha256": consumed,
        "candle_comparison_tolerances": {"ohlc_absolute": PRICE_ATOL, "volume_absolute": VOLUME_ATOL, "relative": 1e-8},
        "qualification_rule": "October PNL >= covered control; daily DD <= covered control; trades >= max(20, half covered control)",
        "no_independent_holdout": True}
    if args.feature_provenance:
        provenance_bytes = args.feature_provenance.read_bytes()
        config["feature_provenance_path"] = str(args.feature_provenance.resolve())
        config["feature_provenance_sha256"] = hashlib.sha256(provenance_bytes).hexdigest()
    else:
        provenance_bytes = None
    common.guard_writes(PACKAGE)
    output.mkdir(parents=True)
    common.dump(output / "config.json", config)
    common.dump(output / "status.json", dict(state="RUNNING", performance_complete=False))
    (output / "proxy_input_features.parquet").write_bytes(input_bytes)
    if provenance_bytes is not None:
        (output / "feature_provenance.json").write_bytes(provenance_bytes)
    fingerprint = common.sha(output / "config.json")
    ledgers, audits = ({variant: [] for variant in selection.VARIANTS} for _ in range(2))
    feature_tables, proofs, coverage, differences, parity_checks = [], [], [], [], []
    by_day = {str(day): frame for day, frame in enriched.groupby(enriched.bar_end.dt.strftime("%Y-%m-%d"), sort=False)}
    for number, day in enumerate(days):
        raw, minutes, proof = data.load_day(day)
        raw = raw.reset_index(drop=True)
        raw["sid"] = number * 1_000_000 + np.arange(len(raw), dtype=np.int64)
        day_features = by_day[day]
        differences.append(source_comparison(raw, day_features, day))
        raw = features.attach_features(raw, day_features)
        results = selection.select_all(raw)
        original_audit = results["base_control"][1]
        covered = selection.gate(raw, "covered_control")
        if not covered.any():
            raise ValueError(f"No proxy-ready candidate rows on {day}; cannot count missing data as zero P&L")
        feature_cols = [name for name in raw if name.startswith("cvd_proxy_")]
        feature_tables.append(raw[["sid", "day", "tradingsymbol", "signal_ts", "volume", *feature_cols]])
        coverage.append(dict(day=day, source_rows=len(raw), source_symbols=raw.tradingsymbol.nunique(),
            fetched_symbols=day_features.symbol.nunique(), proxy_ready_rows=int(covered.sum()),
            proxy_ready_symbols=raw.loc[covered, "tradingsymbol"].nunique(),
            native_audit_candidates=len(original_audit),
            covered_audit_candidates=int(selection.gate(original_audit, "covered_control").sum()),
            restricted_oct28=day == "2026-10-28"))
        proofs.append({**proof, "day": day})
        counts = []
        for variant, (orders, audit) in results.items():
            ledger, _ = simulate(orders, data.execution_paths(orders, minutes), [date.fromisoformat(day)])
            ledger["variant"] = variant
            ledger["strategy_version"] = selection.VERSION
            ledger["strategy_fingerprint"] = fingerprint
            ledger["execution_authority"] = False
            ledgers[variant].append(ledger)
            audits[variant].append(audit)
            counts.append(f"{variant}={len(ledger)}")
        parity_checks.append({"day": day, **baseline_parity(ledgers["base_control"][-1], saved, [day])})
        print(f"{day} [{number+1}/{len(days)}] " + " ".join(counts), flush=True)
    books = {variant: pd.concat(parts, ignore_index=True, sort=False) for variant, parts in ledgers.items()}
    parity = baseline_parity(books["base_control"], saved, days)
    periods = {"all": days, "before_october": [d for d in days if d < "2026-10-01"],
        "october": [d for d in days if d.startswith("2026-10")],
        "october_without_restricted": [d for d in days if d.startswith("2026-10") and d != "2026-10-28"],
        "post_slot_selection": [d for d in days if d > "2026-10-27"]}
    overall_rows, daily_rows, monthly_rows = [], [], []
    for variant, book in books.items():
        book.to_csv(output / f"{variant}_trades.csv", index=False)
        pd.concat(audits[variant], ignore_index=True, sort=False).to_csv(output / f"{variant}_selection_audit.csv", index=False)
        for cost in COSTS:
            stressed = book if cost == 5 else stressed_book(book, cost, allocate)
            for period, cohort in periods.items():
                if cohort:
                    overall_rows.append(dict(variant=variant, cost_bps=cost, period=period, **common.metrics(stressed, cohort)))
            daily_rows.extend(dict(variant=variant, cost_bps=cost, day=day, **common.metrics(stressed, [day])) for day in days)
            monthly_rows.extend(dict(variant=variant, cost_bps=cost, month=month, **common.metrics(stressed, [d for d in days if d.startswith(month)]))
                                for month in sorted({d[:7] for d in days}))
    overall, daily, monthly = pd.DataFrame(overall_rows), pd.DataFrame(daily_rows), pd.DataFrame(monthly_rows)
    checks = verify_accounting(books, overall, daily, monthly, days)
    decisions = []
    october = overall.loc[overall.period.eq("october") & overall.cost_bps.eq(5)].set_index("variant")
    if len(october):
        ref = october.loc["covered_control"]
        for variant in selection.VARIANTS[2:]:
            row = october.loc[variant]
            conditions = dict(pnl_pass=bool(row.net_pnl_rs >= ref.net_pnl_rs),
                drawdown_pass=bool(row.daily_close_drawdown_rs <= ref.daily_close_drawdown_rs + 1e-6),
                sample_pass=bool(row.trades >= max(20, ref.trades * .5)))
            decisions.append(dict(variant=variant, **conditions, research_screen_pass=all(conditions.values()),
                                  october_pnl_delta_rs=row.net_pnl_rs - ref.net_pnl_rs))
    for name, frame in (("overall_comparison", overall), ("daily_results", daily), ("monthly_results", monthly),
                        ("coverage", pd.DataFrame(coverage)), ("candle_source_comparison", pd.DataFrame(differences)),
                        ("comparison_decisions", pd.DataFrame(decisions))):
        frame.to_csv(output / f"{name}.csv", index=False)
    pd.concat(feature_tables, ignore_index=True).to_parquet(output / "candidate_proxy_features.parquet", index=False)
    changes = [name for name, digest in consumed.items() if common.sha(name) != digest]
    if changes or common.sha(reference_path) != config["reference_control_sha256"]:
        raise ValueError(f"Consumed code or reference changed during replay: {changes}")
    passing = [item["variant"] for item in decisions if item["research_screen_pass"]]
    summary = dict(version=selection.VERSION, execution_authority=False, scope="BACKTEST_ONLY",
        created_at_ist=datetime.now(IST).isoformat(), requested_through=args.through,
        days=days, sessions=len(days), first_date=days[0], last_date=days[-1],
        requested_day_included=args.through in days, variants=list(selection.VARIANTS),
        source_type=config["source_type"], independent_holdout=False, slot_policy_selected_through="2026-10-27",
        matched_reference="covered_control", baseline_parity=parity, config_sha256=fingerprint,
        existing_consumed_files_unchanged=True, research_screen_passing_variants=passing,
        conclusion=("Passed the fixed October descriptive screen: " + ", ".join(passing) + ". Prospective evidence still required."
                    if passing else "No proxy variant passed every fixed October descriptive comparison against the covered control."),
        overall_metrics=overall_rows)
    common.dump(output / "source_provenance.json", dict(sessions=proofs, consumed_code=consumed,
        feature_sha256=config["input_sha256"], reference_control_sha256=config["reference_control_sha256"]))
    common.dump(output / "baseline_parity.json", dict(overall=parity, daily=parity_checks))
    common.dump(output / "verification.json", dict(passed=True, checks=checks, original_control_parity=parity,
                                                   existing_consumed_files_unchanged=True))
    common.dump(output / "summary.json", summary)
    render_report(output, summary, overall, daily, pd.DataFrame(coverage), pd.DataFrame(differences))
    common.dump(output / "status.json", dict(state="COMPLETE", performance_complete=True))
    common.dump(output / "manifest.json", dict(files={p.name: common.sha(p) for p in output.iterdir() if p.is_file()},
                                              execution_authority=False))
    print(json.dumps(dict(output=str(output), sessions=len(days), through=days[-1], baseline_parity=True,
                          conclusion=summary["conclusion"]), indent=2), flush=True)
    return summary


def parser():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--features", type=Path, required=True, help="Prepared proxy features including source OHLCV")
    p.add_argument("--feature-provenance", type=Path)
    p.add_argument("--through", default="2026-11-02", type=lambda s: date.fromisoformat(s).isoformat())
    p.add_argument("--days", nargs="+")
    p.add_argument("--output", type=Path, default=PACKAGE / ("results_" + datetime.now(IST).strftime("%Y%m%d_%H%M%S")))
    return p


if __name__ == "__main__":
    args = parser().parse_args()
    existed_before = args.output.exists()
    try:
        run(args)
    except Exception as error:
        # Only mark a new folder created by this invocation; existing outputs
        # remain untouched even when overwrite prevention raises an error.
        if not existed_before and args.output.resolve().is_relative_to(PACKAGE) and (args.output / "status.json").is_file():
            common.dump(args.output / "status.json", dict(state="FAILED", performance_complete=False, error=str(error)))
        raise
