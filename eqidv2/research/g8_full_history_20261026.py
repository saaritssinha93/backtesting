"""Append 26 October to the verified adopted G8 V2 baseline through 23 October.

Backtest-only report: preserves the adopted persistent_direction_30m_2.0 gate,
original G7 V2 targets and single exits. No parameter selection or pointer writes.
"""
from __future__ import annotations

import argparse
from copy import deepcopy
from datetime import datetime, timezone
import json
from pathlib import Path
import shutil

import numpy as np
import pandas as pd

from research import g7_daily_backtest as daily
from research import g8_daily_exit_comparison as prior
from research import g8_main_baseline as adopted
from research import g8_vix_selection_research as core
from research.g8_exit_engine import metrics
from research.g8_vix_data import VixData

ROOT = Path(__file__).resolve().parents[1]
DAY = "2026-10-26"
SCHEMA = "g8_adopted_v2_full_history_20261026_v1"
DEFAULT_PRIOR = core.OUTPUT_ROOT / "daily_comparison/2026-10-23/20261023T125505562512_g8_baseline_vs_shock"
DEFAULT_G7 = daily.DAILY_ROOT / "2026-10-26/20261026T194254854688_5b5a77f8_g7_nifty_futures"
DEFAULT_VIX = core.OUTPUT_ROOT / "inputs/daily/2026-10-26/20261026T141304221336_audited_vix"
DEFAULT_G3 = daily.g7.bounded.G3_ROOT / "backtests/runs/2026-10-26/20261026T172124497640_01b1f49e_current_full_history"


def read(path):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def calendar_audit(days):
    calendar = read(ROOT / "configs/markets/nse_equity_calendar_2026.json")
    holidays = set(calendar["holidays"])
    records = []
    for stamp in pd.date_range(min(days), DAY, freq="D"):
        day = stamp.strftime("%Y-%m-%d")
        expected = stamp.weekday() < 5 and day not in holidays
        status = "INCLUDED" if day in days else "MISSING_SOURCE_SESSION" if expected else "CLOSED"
        records.append(dict(day=day, regular_session_expected=expected,
                            included=day in days, status=status))
    return pd.DataFrame(records)


def verify(output):
    out = Path(output).resolve()
    manifest = read(out / "manifest.json")
    if manifest["schema_version"] != SCHEMA:
        raise ValueError("Wrong full-history report schema")
    for name, digest in manifest["files"].items():
        path = (out / name).resolve()
        if not path.is_relative_to(out) or core.sha(path) != digest:
            raise ValueError("Changed full-history member: " + name)
    for name, digest in manifest["code"].items():
        if core.sha(ROOT / name) != digest:
            raise ValueError("Changed full-history implementation: " + name)
    summary = read(out / "summary.json")
    for source in summary["source_packages"]:
        if core.sha(Path(source["path"]) / source["manifest_file"]) != source["manifest_sha256"]:
            raise ValueError("Changed source manifest: " + source["path"])
    book = pd.read_csv(out / "portfolio_trades.csv")
    if book.sid.duplicated().any() or not set(book.day).issubset(summary["days"]):
        raise ValueError("Duplicate trade identity or unexpected session")
    daily_rows = pd.read_csv(out / "daywise_results.csv")
    if daily_rows.day.tolist() != summary["days"] or daily_rows.day.iloc[-1] != DAY:
        raise ValueError("Incomplete available-session calendar")
    overall = pd.read_csv(out / "overall_results.csv").set_index("period")
    for period, days in summary["periods"].items():
        actual = metrics(book, days)
        for field in ("sessions", "trades", "wins", "losses", "net_pnl_rs", "cost_rs", "gross_pnl_rs"):
            if not np.isclose(actual[field], overall.loc[period, field], atol=1e-6, rtol=0):
                raise ValueError("Full-history accounting mismatch: " + period + "/" + field)
    for row in daily_rows.itertuples():
        if not np.isclose(metrics(book, [row.day])["net_pnl_rs"], row.net_pnl_rs, atol=1e-6, rtol=0):
            raise ValueError("Daily accounting mismatch: " + row.day)
    features = pd.read_csv(out / "today_vix_nifty_decisions.csv").set_index("sid", drop=False)
    if not (pd.to_datetime(features.vix_available_at, utc=True)
            <= pd.to_datetime(features.confirmation_ts, utc=True)).all():
        raise ValueError("VIX lookahead")
    if not np.array_equal(core.allowed_for(features, summary["gate"]), features.g8_allowed):
        raise ValueError("Adopted G8 gate mismatch")
    today_book = book.loc[book.day.eq(DAY)].set_index("sid")
    if (today_book.portfolio_executed & ~features.g8_allowed.reindex(today_book.index)).any():
        raise ValueError("Rejected candidate received capital")
    return True


def run(g7_run=DEFAULT_G7, prior_run=DEFAULT_PRIOR, vix_dir=DEFAULT_VIX,
        g3_full_run=DEFAULT_G3, output=None):
    g7_run, prior_run, vix_dir, g3_full_run = map(Path, (g7_run, prior_run, vix_dir, g3_full_run))
    descriptor, gate = adopted.definition()
    if descriptor["variant_id"] != "V13-V10-G-8-G7V2-NIFTY-VIX-V2" or gate["id"] != adopted.POLICY:
        raise ValueError("This extension requires the unchanged adopted G8 V2 baseline")
    prior.verify(prior_run)
    historical = read(prior_run / "summary.json")
    if historical["through"] != "2026-10-23" or historical["baseline_policy"] != adopted.POLICY:
        raise ValueError("Expected adopted baseline through 23 October")
    g7_summary = daily.verify(g7_run)
    g7_config = read(g7_run / "config.json")
    if (g7_summary["session_date"] != DAY or g7_summary["variant_id"] != daily.g7.V2_VERSION
            or g7_config != daily.g7.configuration(daily.g7.V2_CONFIG)):
        raise ValueError("Current day must use the exact G7 V2 single-exit configuration")
    vix_proof = read(vix_dir / "vix_provenance.json")
    if vix_proof["session_date"] != DAY or not vix_proof["independently_cross_checked"]:
        raise ValueError("Current VIX session has not passed independent interval audit")
    for name, digest in vix_proof["files"].items():
        if core.sha(vix_dir / name) != digest:
            raise ValueError("Current VIX source changed")
    days = historical["periods"]["all"] + [DAY]
    g3_summary = read(g3_full_run / "summary.json")
    if days != g3_summary["days"] or len(set(days)) != len(days):
        raise ValueError("G8 and G3 available historical calendars differ")
    out = Path(output or core.OUTPUT_ROOT / "full_history" / DAY /
               (datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%f") + "_g8_v2_adopted"))
    out.mkdir(parents=True, exist_ok=False)
    original = daily.g7.read_frozen_csv(g7_run / "g7_unfiltered/portfolio_trades.csv")
    audit = pd.read_csv(g7_run / "nifty_direction_audit.csv")
    # Historical G8 SIDs use both small cache keys and date-based appended keys.
    remap = {int(sid): int(DAY.replace("-", "")) * 10000 + i for i, sid in enumerate(original.sid)}
    for frame in (original, audit):
        frame["source_sid"] = frame.sid
        frame["sid"] = frame.sid.map(remap).astype("int64")
    old_vix = pd.read_csv(prior_run / "extended_vix.csv")
    new_vix = pd.read_csv(vix_dir / "vix_5minute.csv")
    extended_vix = pd.concat([old_vix, new_vix], ignore_index=True).sort_values("date")
    if pd.to_datetime(extended_vix.date, utc=True).duplicated().any():
        raise ValueError("Duplicate VIX timestamps")
    extended_vix.to_csv(out / "extended_vix.csv", index=False)
    for name in ("vix_5minute.csv", "vix_15minute.csv", "vix_provenance.json"):
        shutil.copyfile(vix_dir / name, out / name)
    source = dict(unfiltered=original, audit=audit, config=g7_config)
    features = core.build_features(source, VixData(out / "extended_vix.csv"))
    features["g8_allowed"] = core.allowed_for(features, gate)
    features["vix_30m_fallback_used"] = features.vix_30m_pct.isna()
    config = deepcopy(g7_config)
    config.update(strategy="V13-V10-G-8", variant_id=descriptor["variant_id"], scope="BACKTEST_ONLY",
                  execution_authority=False, g8_selected_policy=gate, g8_baseline_definition=descriptor)
    today = core.FastSelection(original, config).full_book(features.g8_allowed, gate["id"])
    history = pd.read_csv(prior_run / "baseline/combined_portfolio_trades.csv")
    combined = pd.concat([history, today], ignore_index=True, sort=False)
    if combined.sid.duplicated().any():
        raise ValueError("Historical and current trade identities overlap")
    combined.to_csv(out / "portfolio_trades.csv", index=False)
    today.to_csv(out / "today_portfolio_trades.csv", index=False)
    features.to_csv(out / "today_vix_nifty_decisions.csv", index=False)
    original.to_csv(out / "today_g7_v2_unfiltered.csv", index=False)
    audit.to_csv(out / "today_nifty_audit.csv", index=False)
    periods = dict(all=days, october=[d for d in days if d.startswith("2026-10-")], today=[DAY])
    overall = pd.DataFrame([dict(period=period, **metrics(combined, dates)) for period, dates in periods.items()])
    daywise = pd.DataFrame([dict(day=day, **metrics(combined, [day])) for day in days])
    overall.to_csv(out / "overall_results.csv", index=False)
    daywise.to_csv(out / "daywise_results.csv", index=False)
    slots = pd.DataFrame([dict(setup_id=s["setup_id"], side=s["side"],
                              **metrics(combined.loc[combined.setup_id.eq(s["setup_id"])], days))
                          for s in g7_config["active_setups"]])
    slots.to_csv(out / "slot_results.csv", index=False)
    coverage = calendar_audit(days)
    coverage.to_csv(out / "calendar_coverage.csv", index=False)
    missing = coverage.loc[coverage.status.eq("MISSING_SOURCE_SESSION"), "day"].tolist()
    packages = [dict(path=str(p.resolve()), manifest_file=name, manifest_sha256=core.sha(p / name))
                for p, name in ((prior_run, "manifest.json"), (g7_run, "manifest.json"),
                                (g3_full_run, "manifest.json"))]
    summary = dict(schema_version=SCHEMA, variant_id=descriptor["variant_id"], selected_policy=gate["id"],
        gate=gate, exit_mode=descriptor["exit_mode"], scope="BACKTEST_ONLY", execution_authority=False,
        first_date=min(days), through=DAY, days=days, sessions=len(days), active_setups=len(g7_config["active_setups"]),
        periods=periods, complete_for_available_source_calendar=True,
        complete_regular_session_coverage=not missing, missing_source_sessions=missing,
        source_packages=packages, vix_input_path=str(vix_dir.resolve()),
        vix_provenance_sha256=core.sha(vix_dir / "vix_provenance.json"),
        selected_metrics=metrics(combined, days), today_metrics=metrics(today, [DAY]),
        historical_metrics=metrics(history, historical["periods"]["all"]),
        inherited_historical_source_coverage=historical["coverage"], today_source_coverage=g7_summary["source_coverage"],
        independent_holdout=False, parameter_search=False, baseline_promotion=False,
        limitations=["Historical targets and adopted VIX policy were chosen after inspecting earlier results.",
                     "Missing source sessions are excluded and disclosed; they are not zero-profit sessions.",
                     "Inherited after-close stock-day input eligibility is not a live causal filter.",
                     "Fixed INR100000 per-entry capital,5x exposure,INR1000000 portfolio capital,5bps modeled round-trip costs,fractional quantities.",
                     "Original G7 V2 single exits are used; inherited V3-named diagnostic columns do not imply partial exits."])
    core.dump(out / "summary.json", summary)
    core.dump(out / "config.json", config)
    core.dump(out / "source_g7_v2_config.json", g7_config)
    html = "<!doctype html><meta charset='utf-8'><title>G8 V2 through 26 October 2026</title><style>body{font:15px system-ui;margin:30px}table{border-collapse:collapse}td,th{padding:7px;border:1px solid #ddd}</style>"
    html += "<h1>G8 V2 adopted baseline through 26 October 2026</h1><p>persistent_direction_30m_2.0. Original G7 V2 targets and single exits; no parameter search.</p>"
    html += "<p>58 available sessions from 29 July. Missing source sessions: " + ", ".join(missing) + ". Historical research, not an independent holdout.</p>"
    html += overall.to_html(index=False, float_format=lambda x: f"{x:,.2f}")
    html += "<h2>Daily results</h2><a href='daywise_results.csv'>CSV</a>" + daywise[["day", "trades", "wins", "losses", "net_pnl_rs"]].to_html(index=False, float_format=lambda x: f"{x:,.2f}")
    (out / "report.html").write_text(html, encoding="utf-8")
    code_files = [Path(__file__), Path(core.__file__), Path(adopted.__file__), ROOT / "research/g8_exit_engine.py",
                  ROOT / "research/g8_vix_data.py", adopted.CONFIG]
    core.dump(out / "manifest.json", dict(schema_version=SCHEMA,
        code={str(p.relative_to(ROOT)): core.sha(p) for p in code_files},
        files={str(p.relative_to(out)): core.sha(p) for p in out.rglob("*") if p.is_file()}))
    verify(out)
    reference = dict(strategy="G8", variant_id=descriptor["variant_id"], selected_policy=gate["id"],
                     package=str(out.resolve()), schema_version=SCHEMA, ledger=str((out / "portfolio_trades.csv").resolve()),
                     days=days, through=DAY, manifest_sha256=core.sha(out / "manifest.json"),
                     missing_source_sessions=missing, metrics=summary["selected_metrics"], today_metrics=summary["today_metrics"])
    reference_dir = ROOT / "outputs/full_backtest_comparison_20261026"
    reference_dir.mkdir(parents=True, exist_ok=True)
    core.dump(reference_dir / "g8_source.json", reference)
    print(json.dumps(reference, indent=2))
    return out


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--g7-run", default=DEFAULT_G7)
    parser.add_argument("--prior-run", default=DEFAULT_PRIOR)
    parser.add_argument("--vix-dir", default=DEFAULT_VIX)
    parser.add_argument("--g3-full-run", default=DEFAULT_G3)
    parser.add_argument("--output")
    parser.add_argument("--verify")
    args = parser.parse_args()
    if args.verify:
        print("verified", verify(args.verify))
    else:
        run(args.g7_run, args.prior_run, args.vix_dir, args.g3_full_run, args.output)
