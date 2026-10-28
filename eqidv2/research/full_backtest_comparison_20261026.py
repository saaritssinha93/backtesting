"""Verify and package comparable full available histories through 26 Oct 2026.

G3/G4/G6/G8 references point to independently verified same-version packages.
G7 explicitly uses V2, not the V3 default. Missing historical sessions remain
missing rather than becoming zero-return days. No live configuration is used.
"""
from __future__ import annotations

import argparse
import hashlib
import html
import json
from pathlib import Path
import shutil

import numpy as np
import pandas as pd

from research.g8_exit_engine import _flags, metrics

ROOT = Path(__file__).resolve().parents[1]
DATA = Path("C:/TradingData/eqidv2")
REFERENCES = ROOT / "outputs/full_backtest_comparison_20261026"
RESULT_ROOT = DATA / "backtesting_comparison_g3_g4_g6_g7v2_g8"
THROUGH = "2026-10-26"
STRATEGIES = ("G3", "G4", "G6", "G7V2", "G8")
VERSIONS = {
    "G3": "V13-V10-G-3-BACKTEST-TARGET-GT075-RESTORED3-V14",
    "G4": "V13-V10-G-4-BOUNDED-EXIT-51-SETUPS-V14",
    "G6": "V13-V10-G-6-G4-REPLICA-V2",
    "G7V2": "V13-V10-G-7-G6-REPLICA-V2",
    "G8": "V13-V10-G-8-G7V2-NIFTY-VIX-V2",
}
SELECTED_POLICIES = {"G4":"g4_bounded_exit", "G6":"g6_nifty_futures_both_counter",
                     "G7V2":"g7_nifty_futures_both_counter", "G8":"persistent_direction_30m_2.0"}
G7_DAILIES = (
    DATA / "backtesting_result_v13_v10_g_7/daily/runs/2026-10-23/20261023T183148514804_bbcb21e1_g7_nifty_futures",
    DATA / "backtesting_result_v13_v10_g_7/daily/runs/2026-10-26/20261026T194254854688_5b5a77f8_g7_nifty_futures",
)
POLICIES = {
    "G3": "Current G3 backtest V14: 51 active setups",
    "G4": "G4 V14: bounded single exits; no additional Nifty direction gate",
    "G6": "G6 V2: Nifty futures slot + session counter-direction gate",
    "G7V2": "G7 V2: restored setup targets + three slot vetoes + Nifty gate; single exit",
    "G8": "G8 V2: G7 V2 plus adopted persistent_direction_30m_2.0 VIX gate",
}


def read(path):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def sha(path):
    with Path(path).open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def dump(path, value):
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    Path(path).write_text(json.dumps(value, indent=2, default=str, allow_nan=False)+"\n", encoding="utf-8")


def verify_members(directory, expected_digest=None):
    directory = Path(directory).resolve()
    manifest = directory / "manifest.json"
    if expected_digest is not None and sha(manifest) != expected_digest:
        raise ValueError("Source manifest changed: " + str(directory))
    payload = read(manifest)
    if not payload.get("files"):
        raise ValueError("Missing package members")
    for name, digest in payload["files"].items():
        member = (directory/name).resolve()
        if not member.is_relative_to(directory) or sha(member) != digest:
            raise ValueError("Source member changed: " + str(member))
    return payload


def normalize_book(book, days):
    result = book.copy()
    result["day"] = result.day.astype(str).str[:10]
    if not set(result.day).issubset(days) or len(days) != len(set(days)) or days != sorted(days):
        raise ValueError("Invalid book/session calendar")
    if result.sid.isna().any() or result.sid.duplicated().any():
        raise ValueError("Duplicate or missing order SID")
    if result.duplicated(["day", "setup_id", "tradingsymbol", "side"]).any():
        raise ValueError("Duplicate selected trade identity")
    if not result.side.isin(["LONG", "SHORT"]).all():
        raise ValueError("Invalid side")
    for field in ("filled", "portfolio_executed"):
        result[field] = _flags(result[field], field)
    if (result.portfolio_executed & ~result.filled).any():
        raise ValueError("Executed order had no permitted fill")
    money = ["portfolio_gross_profit_rupees", "portfolio_cost_rupees", "portfolio_net_profit_rupees"]
    result[money] = result[money].apply(pd.to_numeric, errors="raise")
    if not np.isfinite(result[money]).all().all():
        raise ValueError("Invalid portfolio money values")
    if not np.allclose(result[money[0]]-result[money[1]], result[money[2]], atol=1e-6, rtol=0):
        raise ValueError("Gross minus costs does not equal net")
    if not np.allclose(result.loc[~result.portfolio_executed, money], 0., atol=1e-8, rtol=0):
        raise ValueError("Rejected candidate contributed portfolio P&L")
    fills = result.loc[result.portfolio_executed]
    entry, exit_ = (pd.to_datetime(fills[field], utc=True) for field in ("entry_ts", "exit_ts"))
    if entry.isna().any() or exit_.isna().any() or (exit_ < entry).any():
        raise ValueError("Invalid execution chronology")
    if any(not np.array_equal(times.dt.tz_convert("Asia/Kolkata").dt.strftime("%Y-%m-%d"), fills.day)
           for times in (entry, exit_)):
        raise ValueError("Overnight position violates the comparison contract")
    if not np.allclose(fills.portfolio_cost_rupees, 250., atol=1e-6, rtol=0):
        raise ValueError("Comparative runs must retain Rs500000 exposure and 5bps modeled cost")
    return result


def verify_profile(name, config, summary):
    for payload in (config, summary):
        if payload.get("variant_id", payload.get("strategy_version")) != VERSIONS[name]:
            raise ValueError("Wrong exact strategy version: " + name)
        if payload.get("scope") != "BACKTEST_ONLY" or payload.get("execution_authority") is not False:
            raise ValueError("Source must have research-only authority")
    if name in SELECTED_POLICIES and summary.get("selected_policy") != SELECTED_POLICIES[name]:
        raise ValueError("Wrong selected policy: " + name)
    if len(config.get("active_setups", [])) != 51:
        raise ValueError("Comparison requires all 51 configured setups")
    fields = (("capital_per_entry_rs", "capital_per_entry_rupees", 100000.),
              ("portfolio_capital_rs", "portfolio_capital_rupees", 1000000.),
              ("leverage", "leverage_factor", 5.), ("model_cost_bps_round_trip", "cost_bps", 5.))
    for first, second, expected in fields:
        if config.get(first, config.get(second)) != expected:
            raise ValueError("Incomparable capital/cost configuration: " + name + "/" + first)


def stats(book, days):
    result = metrics(book, days)
    selected = book.loc[book.day.isin(days)]
    traded = selected.loc[selected.portfolio_executed]
    result["qualified_fills"] = int(selected.filled.sum())
    result["return_on_fixed_10lakh_pct"] = result["net_pnl_rs"]/1_000_000*100
    result["stop_exits"] = int(traded.exit_reason.astype(str).str.contains("STOP", regex=False).sum())
    result["long_trades"] = int(traded.side.eq("LONG").sum())
    result["short_trades"] = int(traded.side.eq("SHORT").sum())
    return result


def build_g7():
    from research import g7_g6_replica_backtest as g7, g8_sources
    from research.g46_full_history_20261026 import allocate, check_allocation

    output = g7.G7_ROOT / "full_history/runs/2026-10-26/20261026_g7_v2_verified_full_history"
    reference_path = REFERENCES / "g7v2_source.json"
    if output.exists():
        verify_members(output)
        if not reference_path.is_file() or read(reference_path)["manifest_sha256"] != sha(output/"manifest.json"):
            raise ValueError("Existing G7 package does not match its reference")
        return read(reference_path)
    verify_members(g8_sources.CACHE, "c0a52d928a3b7b6ac3a270a5908001a71f6740634c77ee41e7ee2244a7e0a6a9")
    cached = g8_sources.load_sources()
    config = g7.configuration(g7.V2_CONFIG)
    if config != cached["config"]:
        raise ValueError("G7 baseline cache must be exactly V2")
    parts, days = [cached["baseline"].copy()], list(cached["days"])
    parts[0]["full_history_source"] = str(g8_sources.CACHE)
    proof = [dict(path=str(g8_sources.CACHE), manifest_sha256=sha(g8_sources.CACHE/"manifest.json"),
                  days=days.copy(), role="VERIFIED_G7_V2_CACHE_THROUGH_22")]
    next_sid = int(parts[0].sid.max())+1
    for directory in G7_DAILIES:
        verify_members(directory)
        summary = read(directory/"summary.json")
        if (read(directory/"config.json") != config or summary["variant_id"] != g7.V2_VERSION
                or summary["selected_policy"] != g7.NIFTY_POLICY or summary.get("complete") is not True):
            raise ValueError("Daily G7 package is not complete V2")
        added_days = summary["days"]
        if set(added_days) & set(days):
            raise ValueError("Overlapping G7 historical source sessions")
        part = pd.read_csv(directory/g7.NIFTY_POLICY/"portfolio_trades.csv", low_memory=False)
        part["full_history_source_sid"] = part.sid
        part["full_history_source"] = str(directory)
        part["sid"] = np.arange(next_sid, next_sid+len(part))
        next_sid += len(part)
        parts.append(part)
        days.extend(added_days)
        proof.append(dict(path=str(directory), manifest_sha256=sha(directory/"manifest.json"),
                          days=added_days, role="SAME_CONFIG_VERIFIED_DAILY_REPLAY"))
    expected = normalize_book(pd.concat(parts, ignore_index=True, sort=False), days)
    replayed, allocation = allocate(expected, config)
    check_allocation(replayed, expected)
    book = normalize_book(replayed, days)
    if len(days) != 58 or days[-1] != THROUGH or len(book) != 596:
        raise ValueError("Unexpected G7 V2 complete inventory")
    output.mkdir(parents=True)
    book.to_csv(output/"portfolio_trades.csv", index=False)
    daily = pd.DataFrame([dict(day=d, **stats(book, [d])) for d in days])
    daily.to_csv(output/"daily_results.csv", index=False)
    dump(output/"config.json", config)
    summary = dict(schema_version="g7_v2_full_history_20261026_v1", variant_id=g7.V2_VERSION,
                   selected_policy=g7.NIFTY_POLICY, first_date=days[0], last_date=days[-1], days=days,
                   sessions=len(days), active_setups=51, complete=True, scope="BACKTEST_ONLY",
                   execution_authority=False, result=stats(book, days), today=stats(book, days[-1:]),
                   allocation_replayed_and_reconciled=True, allocation=allocation,
                   missing_source_sessions=["2026-08-24", "2026-08-25", "2026-10-01"],
                   exit_mode="G7_V2_SINGLE_EXIT_ORIGINAL_SETUP_TARGETS", sources=proof)
    dump(output/"summary.json", summary)
    dump(output/"manifest.json", dict(schema_version=summary["schema_version"],
         files={p.name:sha(p) for p in output.iterdir() if p.is_file()},
         code={str(Path(__file__).resolve()):sha(__file__)}))
    reference = dict(strategy="G7V2", run_dir=str(output), ledger_path=str(output/"portfolio_trades.csv"),
                     config_path=str(output/"config.json"), manifest_sha256=sha(output/"manifest.json"),
                     variant_id=g7.V2_VERSION, selected_policy=g7.NIFTY_POLICY, days=days,
                     metrics=summary["result"], today_metrics=summary["today"])
    dump(reference_path, reference)
    print("G7 V2 FULL VERIFIED", summary["result"]["trades"], summary["result"]["net_pnl_rs"], flush=True)
    return reference


def load_reference(name):
    ref = read(REFERENCES/(name.lower()+"_source.json"))
    directory = Path(ref.get("run_dir", ref.get("package", ""))).resolve()
    manifest = verify_members(directory, ref["manifest_sha256"])
    ledger = Path(ref.get("ledger_path", ref.get("ledger", directory/"portfolio_trades.csv"))).resolve()
    config_path = Path(ref.get("config_path", directory/"config.json")).resolve()
    if not ledger.is_relative_to(directory) or not config_path.is_relative_to(directory):
        raise ValueError("Reference member escaped package")
    for path in (ledger, config_path, directory/"summary.json"):
        if path.relative_to(directory).as_posix() not in manifest["files"]:
            raise ValueError("Reference is not a sealed manifest member")
    source_summary = read(directory/"summary.json")
    verify_profile(name, read(config_path), source_summary)
    days = ref["days"]
    if days != source_summary["days"] or days[-1] != THROUGH:
        raise ValueError(name + " does not include today's complete session")
    book = normalize_book(pd.read_csv(ledger, low_memory=False), days)
    return dict(reference=ref, directory=directory, book=book, days=days, config_path=config_path)


def write_report(output, tables, summary):
    cols = ["strategy", "trades", "wins", "losses", "win_rate_pct", "net_pnl_rs", "profit_factor", "max_drawdown_rs"]
    table = lambda frame: "<div class='scroll'>"+frame.to_html(index=False, border=0, float_format=lambda x:f"{x:,.2f}")+"</div>"
    details = "".join(f"<li><b>{name}</b>: {html.escape(POLICIES[name])} — <a href='{name}/portfolio_trades.csv'>all candidates/trades</a>, "
                      f"<a href='{name}/config.json'>exact configuration</a></li>" for name in STRATEGIES)
    body = ("<!doctype html><html><head><meta charset='utf-8'><meta name='viewport' content='width=device-width,initial-scale=1'>"
            "<title>G3 / G4 / G6 / G7 V2 / G8 — 26 October 2026</title><style>"
            "body{font:15px/1.5 system-ui;margin:30px;color:#142b43;background:#f4f7fa}h1{font-size:28px}"
            "table{border-collapse:collapse;background:white;font-size:13px}td,th{padding:9px;border-bottom:1px solid #ccd7e3;text-align:right;white-space:nowrap}"
            "th{background:#e2ebf5}td:first-child,th:first-child{text-align:left}.scroll{overflow:auto}"
            ".notice{background:#fff2cf;padding:15px;border-left:4px solid #e3a52e}a{color:#125daf}</style></head><body>"
            "<h1>Full available backtesting results through 26 October 2026</h1>"
            "<p>29 July–26 October 2026 · 58 identical source sessions · all five strategies include today's completed session.</p>"
            "<p class='notice'><b>Historical coverage gaps:</b> 24–25 August and 1 October have no complete approved source packages. "
            "Some raw candles exist, but dated universe/contract-roll or complete immutable snapshot evidence is incomplete. "
            "These dates are excluded for every model and are not counted as zero-return days.</p>"
            "<p>Fixed portfolio capital ₹10,00,000; ₹1,00,000 margin per entry; 5× exposure; maximum 10 concurrent positions; "
            "5 bps modeled round-trip cost (₹250 per executed trade); no compounding. Drawdown is measured at daily close.</p>"
            "<p><a href='daywise_with_coverage.csv'>Daywise comparison including coverage gaps</a> · <a href='overall_comparison.csv'>Overall metrics</a> · "
            "<a href='monthly_comparison.csv'>Monthly</a> · <a href='slot_side_comparison.csv'>Every setup</a> · "
            "<a href='all_selected_candidates.csv'>All selected candidates</a> · <a href='all_executed_trades.csv'>Executed trades</a></p>"
            "<h2>Full available period</h2>"+table(tables["overall_comparison"][cols])+
            "<h2>Today: 26 October 2026</h2>"+table(tables["today_comparison"][cols])+
            "<h2>Monthly comparison</h2>"+table(tables["monthly_comparison"][["strategy", "month", "sessions", "trades", "win_rate_pct", "net_pnl_rs", "profit_factor", "max_drawdown_rs"]])+
            "<h2>Long and short</h2>"+table(tables["side_comparison"][["strategy", "side", "trades", "win_rate_pct", "net_pnl_rs", "profit_factor"]])+
            "<h2>Exact versions and source files</h2><ul>"+details+"</ul>"
            "<h2>Validation and interpretation</h2><ul>"
            "<li>Manifest hashes checked; no duplicate sessions or trades; gross minus modeled costs reconciles to net for every row.</li>"
            "<li>Portfolio allocation retains all selected candidates, including rejected fills. G4/G6/G7 V2 combined allocations were replayed and matched their sealed sources.</li>"
            "<li>G7 is explicitly V2 with single exits. G8 uses the adopted 30-minute VIX policy, not a shock-exit experiment.</li>"
            "<li>G3 is the current V14 backtest profile. It is not the independently configured live production profile.</li>"
            "<li>Setups/targets were previously tuned on inspected history. These are exploratory historical results, not an untouched validation sample.</li>"
            "<li>Inherited after-close eligibility exclusions and opening-data restrictions remain. Fractional theoretical quantities and modeled costs do not establish live fill performance.</li>"
            "<li>Missing dates and source-specific coverage are documented in <a href='source_references.json'>source references</a>. No live settings changed.</li>"
            "</ul><p><a href='summary.json'>Summary and assumptions</a> · <a href='verification.json'>Verification</a></p></body></html>")
    (output/"report.html").write_text(body, encoding="utf-8")


def run(output):
    output = Path(output).resolve()
    if output.exists() or not output.is_relative_to(RESULT_ROOT.resolve()):
        raise ValueError("Require a new isolated comparison output directory")
    sources = {name:load_reference(name) for name in STRATEGIES}
    days = sources["G3"]["days"]
    if any(s["days"] != days for s in sources.values()) or len(days) != 58:
        raise ValueError("The five full histories do not have exactly the same 58-session calendar")
    daily, monthly, sides, slots, all_rows, overall, today = [], [], [], [], [], [], []
    output.mkdir(parents=True)
    for name, src in sources.items():
        book = src["book"]
        book["comparison_strategy"] = name
        overall.append(dict(strategy=name, **stats(book, days)))
        today.append(dict(strategy=name, **stats(book, [THROUGH])))
        daily.extend(dict(strategy=name, day=d, **stats(book, [d])) for d in days)
        monthly.extend(dict(strategy=name, month=m, **stats(book, [d for d in days if d.startswith(m)]))
                       for m in sorted({d[:7] for d in days}))
        sides.extend(dict(strategy=name, side=side, **stats(book.loc[book.side.eq(side)], days)) for side in ("LONG", "SHORT"))
        config = read(src["config_path"])
        configured = config.get("active_setups", [])
        setup_ids = {x.get("setup_id", x.get("confirmation_end", "").replace(":", "")+"_"+x.get("side", "")) for x in configured}
        setup_ids |= set(book.setup_id)
        setup_ids.discard("")
        slots.extend(dict(strategy=name, setup_id=sid, **stats(book.loc[book.setup_id.eq(sid)], days)) for sid in sorted(setup_ids))
        folder = output/name
        folder.mkdir()
        book.to_csv(folder/"portfolio_trades.csv", index=False)
        book.loc[book.portfolio_executed].to_csv(folder/"executed_trades.csv", index=False)
        book.loc[book.day.eq(THROUGH)].to_csv(folder/"today_candidates.csv", index=False)
        shutil.copyfile(src["config_path"], folder/"config.json")
        all_rows.append(book)
    all_candidates = pd.concat(all_rows, ignore_index=True, sort=False)
    daily = pd.DataFrame(daily)
    wide = daily.pivot(index="day", columns="strategy", values=["net_pnl_rs", "trades", "wins", "losses", "win_rate_pct"])
    wide.columns = [name+"_"+field for field, name in wide.columns]
    wide = wide.reset_index()
    coverage = sources["G3"]["reference"]["coverage"]
    if sha(coverage["calendar_path"]) != coverage["calendar_sha256"]:
        raise ValueError("Reviewed source calendar changed")
    missing_days = coverage["missing_regular_sessions"]
    full_calendar = sorted(set(days) | set(missing_days))
    with_coverage = wide.set_index("day").reindex(full_calendar).reset_index()
    with_coverage.insert(1, "coverage_status", np.where(with_coverage.day.isin(days),
                         "VERIFIED_AVAILABLE_SESSION", "UNAVAILABLE_INCOMPLETE_HISTORICAL_EVIDENCE"))
    if len(full_calendar) != coverage["expected_regular_sessions_from_local_reviewed_calendar"]:
        raise ValueError("Expected regular session coverage differs")
    equity = daily.pivot(index="day", columns="strategy", values="net_pnl_rs").cumsum()
    equity.columns = [name+"_cumulative_net_pnl_rs" for name in equity]
    tables = dict(overall_comparison=pd.DataFrame(overall), today_comparison=pd.DataFrame(today),
                  daywise_all_models=daily, daywise_comparison=wide, daywise_with_coverage=with_coverage,
                  monthly_comparison=pd.DataFrame(monthly),
                  side_comparison=pd.DataFrame(sides), slot_side_comparison=pd.DataFrame(slots),
                  equity_curve=equity.reset_index(), all_selected_candidates=all_candidates,
                  all_executed_trades=all_candidates.loc[all_candidates.portfolio_executed])
    for name, frame in tables.items():
        frame.to_csv(output/(name+".csv"), index=False)
    references = {name:src["reference"] for name, src in sources.items()}
    dump(output/"source_references.json", references)
    summary = dict(schema_version="g3_g4_g6_g7v2_g8_full_comparison_v1", requested_through=THROUGH,
                   first_date=days[0], last_date=days[-1], days=days, sessions=58, same_calendar_verified=True,
                   gap_free_history=False, missing_source_sessions=["2026-08-24", "2026-08-25", "2026-10-01"],
                   strategy_definitions=POLICIES, scope="BACKTEST_ONLY", execution_authority=False,
                   assumptions=dict(portfolio_capital_rs=1_000_000, capital_per_entry_rs=100_000,
                                    leverage=5, cost_bps_round_trip=5, max_positions=10, compounding=False,
                                    drawdown="DAILY_CLOSE", quantities="THEORETICAL_FRACTIONAL"),
                   comparison=overall, today=today, untouched_holdout=False)
    dump(output/"summary.json", summary)
    verification = dict(manifest_members_verified=True, identical_calendar=True, unique_selected_trade_identity=True,
                        gross_minus_cost_equals_net=True, rejected_candidate_pnl_zero=True,
                        no_overnight_executions=True, each_executed_trade_modeled_cost_rs=250.,
                        missing_dates_not_zero_filled=True, all_five_models_include_today=True)
    dump(output/"verification.json", verification)
    write_report(output, tables, summary)
    dump(output/"manifest.json", dict(schema_version=summary["schema_version"],
         code={str(Path(__file__).resolve()):sha(__file__)},
         files={p.relative_to(output).as_posix():sha(p) for p in output.rglob("*") if p.is_file()}))
    verify(output)
    print(tables["overall_comparison"][["strategy", "trades", "wins", "losses", "win_rate_pct", "net_pnl_rs", "profit_factor", "max_drawdown_rs"]].to_string(index=False))
    print("VERIFIED", output, flush=True)
    return output


def verify(output):
    output = Path(output).resolve()
    verify_members(output)
    summary = read(output/"summary.json")
    overall, daily, monthly = (pd.read_csv(output/(n+".csv")) for n in ("overall_comparison", "daywise_all_models", "monthly_comparison"))
    references = read(output/"source_references.json")
    for name in STRATEGIES:
        ref = references[name]
        source = Path(ref.get("run_dir", ref.get("package", "")))
        verify_members(source, ref["manifest_sha256"])
        verify_profile(name, read(output/name/"config.json"), read(source/"summary.json"))
        book = normalize_book(pd.read_csv(output/name/"portfolio_trades.csv", low_memory=False), summary["days"])
        actual = stats(book, summary["days"])
        expected = overall.loc[overall.strategy.eq(name)].iloc[0]
        for key in ("trades", "wins", "losses", "net_pnl_rs", "gross_pnl_rs", "cost_rs", "profit_factor", "max_drawdown_rs"):
            if not np.isclose(actual[key], expected[key], atol=1e-6, rtol=0):
                raise ValueError("Comparison ledger mismatch: " + name + "/" + key)
        for table in (daily, monthly):
            group = table.loc[table.strategy.eq(name)]
            if (not np.isclose(group.net_pnl_rs.sum(), actual["net_pnl_rs"], atol=1e-6, rtol=0)
                    or group.trades.sum() != actual["trades"]):
                raise ValueError("Daily/monthly reconciliation failed")
        for row in daily.loc[daily.strategy.eq(name)].itertuples():
            if not np.isclose(stats(book, [row.day])["net_pnl_rs"], row.net_pnl_rs, atol=1e-6, rtol=0):
                raise ValueError("Daily trade reconciliation failed")
    return True


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--build-g7", action="store_true")
    parser.add_argument("--verify", type=Path)
    parser.add_argument("--output-dir", type=Path)
    args = parser.parse_args()
    if args.verify:
        verify(args.verify)
        print("VERIFIED", args.verify)
        return
    if args.build_g7:
        build_g7()
    now = pd.Timestamp.now(tz="Asia/Kolkata")
    output = args.output_dir or RESULT_ROOT / "runs" / THROUGH / (now.strftime("%Y%m%dT%H%M%S%f")+"_verified_all_models")
    run(output)


if __name__ == "__main__":
    main()
