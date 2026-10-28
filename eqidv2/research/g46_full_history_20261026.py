"""Extend approved G4 V14/G6 V2 using sealed, same-configuration sessions.

Historical exits remain their verified results. All selected candidates are
retained and portfolio allocation is replayed across the complete calendar.
No historical result, latest pointer, strategy setting or live file is changed.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd

from research import g4_complete_inventory_backtest as g4
from research import g4_daily_backtest as g4_daily
from research import g6_g4_replica_backtest as g6
from research import g6_daily_backtest as g6_daily
from research.g8_exit_engine import ENGINE, _flags, metrics


DATA = Path("C:/TradingData/eqidv2")
THROUGH = "2026-10-26"
G3 = DATA / "backtesting_result_v13_v10_g_3/backtests/runs/2026-10-26/20261026T172124497640_01b1f49e_current_full_history"
G3_MANIFEST = "d457163431aca6ac48c3390bf5fe6ade026da9247e942b56e0cc4dfe15a4a811"
SOURCES = {
    4: ["runs/2026-10-21/20261021T192354896814_1ce271c2_late_slots",
        "daily/runs/2026-10-22/20261022T213111340459_24761024_g4_bounded_exit",
        "daily/runs/2026-10-23/20261023_compare_6b96653a_g4",
        "daily/runs/2026-10-26/20261026_g4_v14_full_history_extension"],
    6: ["runs/2026-10-21/20261021T190634213322_1fc497f3_g6_nifty_futures",
        "daily/runs/2026-10-22/20261022T213111274162_4b42c575_g6_nifty_futures",
        "daily/runs/2026-10-23/20261023T183148480808_985a0e9e_g6_nifty_futures",
        "daily/runs/2026-10-26/20261026_g6_v2_full_history_extension"],
}


def sha(path):
    with Path(path).open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def read(path):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def write(path, value):
    Path(path).write_text(json.dumps(value, indent=2, default=str, allow_nan=False)+"\n", encoding="utf-8")


def package(directory):
    directory = Path(directory).resolve()
    manifest = read(directory / "manifest.json")
    for name, digest in manifest["files"].items():
        member = (directory / name).resolve()
        if not member.is_relative_to(directory) or sha(member) != digest:
            raise ValueError("Source package member changed: " + str(member))
    return dict(path=str(directory), manifest_sha256=sha(directory / "manifest.json"))


def allocate(book, config):
    work = book.copy(deep=True)
    work["filled"] = _flags(work.filled, "filled")
    work = work.drop(columns=[field for field in work if field.startswith("portfolio_")])
    sized = ENGINE.v5.apply_fixed_capital_model(work, config["capital_per_entry_rs"], config["leverage"])
    constraints = ENGINE.v6.PortfolioConfig(
        portfolio_capital_rupees=config["portfolio_capital_rs"],
        max_positions=config.get("max_positions", 10),
        max_positions_per_symbol=config.get("max_positions_per_symbol"),
        max_gross_exposure_rupees=config.get("max_gross_exposure_rs"),
        max_open_risk_rupees=config.get("max_open_risk_rs"))
    result, allocation = ENGINE.v6.apply_portfolio_constraints(sized, constraints)
    result["day"] = result.day.astype(str).str[:10]
    return result, allocation


def check_allocation(actual, expected):
    if not _flags(actual.portfolio_executed, "actual_executed").equals(
            _flags(expected.portfolio_executed, "source_executed")):
        raise ValueError("Full-history execution allocation differs from sealed sessions")
    for field in ("portfolio_net_profit_rupees", "portfolio_gross_profit_rupees", "portfolio_cost_rupees"):
        np.testing.assert_allclose(actual[field], expected[field], rtol=0, atol=1e-6,
                                   err_msg="Full-history allocation parity: " + field)


def run(strategy):
    module = g4 if strategy == 4 else g6
    daily = g4_daily if strategy == 4 else g6_daily
    root = DATA / f"backtesting_result_v13_v10_g_{strategy}"
    policy = "g4_bounded_exit" if strategy == 4 else g6.NIFTY_POLICY
    config = module.configuration()
    output = root / "runs/2026-10-26" / f"20261026_g{strategy}_verified_full_history"
    if output.exists():
        raise ValueError("Fresh output directory required: " + str(output))
    if sha(G3 / "manifest.json") != G3_MANIFEST:
        raise ValueError("Pinned complete G3 calendar manifest changed")
    calendar = read(G3 / "summary.json")["days"]
    if calendar != sorted(set(calendar)) or len(calendar) != 58 or calendar[-1] != THROUGH:
        raise ValueError("Unexpected authoritative complete calendar")
    protected = {str(p): sha(p) for n in (3, 4, 6)
                 for p in (DATA / f"backtesting_result_v13_v10_g_{n}" / "latest").glob("*.json")}
    frames, orders, days, evidence = [], [], [], []
    for number, relative in enumerate(SOURCES[strategy]):
        directory = root / relative
        proof = package(directory)
        summary = read(directory / "summary.json")
        if read(directory / "config.json") != config or summary["variant_id"] != config["variant_id"]:
            raise ValueError("Cannot combine different strategy configurations")
        if summary["execution_authority"] is not False:
            raise ValueError("Expected backtest-only package")
        selected = summary.get("selected_policy", summary.get("primary_policy"))
        if selected != policy:
            raise ValueError("Historical entry policy differs")
        if number:
            daily.verify(directory)
            if summary.get("complete") is not True:
                raise ValueError("Daily source is incomplete")
        cohort = summary["days"]
        if set(cohort) & set(days):
            raise ValueError("Overlapping session packages")
        days.extend(cohort)
        ledger_path = directory / ("portfolio_trades.csv" if number and strategy == 4
                                   else policy + "/portfolio_trades.csv")
        book = pd.read_csv(ledger_path, low_memory=False)
        chosen = pd.read_csv(directory / "selected_orders.csv", low_memory=False)
        if book.sid.duplicated().any() or set(book.sid) != set(chosen.sid):
            raise ValueError("Source package lost selected candidates")
        book["filled"] = _flags(book.filled, "filled")
        book["portfolio_executed"] = _flags(book.portfolio_executed, "portfolio_executed")
        book["source_sid"], book["source_package"] = book.sid, str(directory)
        chosen["source_sid"], chosen["source_package"] = chosen.sid, str(directory)
        mapping = {sid: sid_new for sid_new, sid in enumerate(book.sid, 1 + sum(map(len, frames)))}
        book["sid"], chosen["sid"] = book.sid.map(mapping), chosen.sid.map(mapping)
        if not set(book.day.astype(str).str[:10]).issubset(cohort):
            raise ValueError("Ledger dates escaped source calendar")
        proof.update(days=cohort, selected_orders=len(book), config_sha256=sha(directory / "config.json"),
                     source_variant=summary["variant_id"], selected_policy=selected,
                     daily_price_exit_replay_verified=bool(number))
        evidence.append(proof)
        frames.append(book)
        orders.append(chosen)
    if days != calendar:
        raise ValueError("Combined strategy does not cover the complete G3 calendar")
    expected = pd.concat(frames, ignore_index=True, sort=False)
    if expected.duplicated(["day", "setup_id", "tradingsymbol", "side"]).any():
        raise ValueError("Duplicate full-history candidate identity")
    book, allocation = allocate(expected, config)
    check_allocation(book, expected)
    if strategy == 6 and (book.portfolio_executed & ~_flags(book.g6_nifty_allowed, "g6_nifty_allowed")).any():
        raise ValueError("Nifty rejected candidate received capital")
    result = metrics(book, days)
    result["return_on_fixed_capital_pct"] = 100 * result["net_pnl_rs"] / config["portfolio_capital_rs"]
    daywise = pd.DataFrame([dict(day=d, **metrics(book, [d])) for d in days])
    monthly = pd.DataFrame([dict(month=m, **metrics(book, [d for d in days if d.startswith(m)]))
                            for m in sorted({d[:7] for d in days})])
    summary = dict(schema_version="g46_verified_full_history_20261026_v1", strategy=f"G{strategy}",
        variant_id=config["variant_id"], selected_policy=policy, scope="BACKTEST_ONLY", execution_authority=False,
        complete=True, first_date=days[0], last_date=days[-1], through=THROUGH, sessions=len(days), days=days,
        active_setups=51, result=result, latest_session_result=metrics(book, [days[-1]]),
        source_packages=evidence, authoritative_calendar=dict(path=str(G3), manifest_sha256=G3_MANIFEST),
        full_candidate_inventory=True, portfolio_allocation_replayed=True, all_source_allocation_parity=True,
        no_source_settings_or_pointers_changed=True,
        limitations=["58 available complete sessions; inherited missing sessions are not counted as zero-profit days.",
                     "Inherited setups/targets were selected after historical inspection; this is not an untouched holdout.",
                     "Inherited after-close stock-day eligibility exclusions are retained.",
                     "Fixed INR1,000,000 capital, INR100,000 margin per trade, 5x exposure and modeled 5bps round-trip costs; fractional theoretical shares.",
                     "Historical verified price exits are reused; appended daily price exits and full-history capital allocation are replay-verified.",
                     "Daily-close drawdown is reported, not intraday mark-to-market drawdown."])
    output.mkdir(parents=True)
    write(output / "config.json", config)
    write(output / "summary.json", summary)
    write(output / "allocation_summary.json", allocation)
    pd.concat(orders, ignore_index=True, sort=False).to_csv(output / "selected_orders.csv", index=False)
    book.to_csv(output / "portfolio_trades.csv", index=False)
    daywise.to_csv(output / "daywise_comparison.csv", index=False)
    monthly.to_csv(output / "monthly_comparison.csv", index=False)
    pd.DataFrame([dict(policy=policy, **result)]).to_csv(output / "overall_comparison.csv", index=False)
    (output / "report.html").write_text(
        "<!doctype html><meta charset='utf-8'><title>G"+str(strategy)+" full history</title>"
        "<style>body{font:15px system-ui;margin:30px}td,th{padding:7px;border:1px solid #ccc}table{border-collapse:collapse}</style>"
        f"<h1>G{strategy}: verified full history through {THROUGH}</h1><p>{config['variant_id']} · {policy} · {len(days)} complete sessions</p>"
        + pd.DataFrame([result]).to_html(index=False) + "<h2>Daily results</h2>" + daywise.to_html(index=False)
        + "<p>Historical research only. Source assumptions and all limitations are recorded in summary.json.</p>", encoding="utf-8")
    for p, digest in protected.items():
        if sha(p) != digest:
            raise ValueError("A protected result pointer changed during assembly")
    write(output / "manifest.json", dict(schema_version=summary["schema_version"],
        code_sha256={str(Path(__file__).resolve()):sha(__file__)},
        files={p.relative_to(output).as_posix():sha(p) for p in output.rglob("*") if p.is_file()}))
    verify(output)
    print(json.dumps(dict(output=str(output), **result), indent=2), flush=True)
    return summary


def verify(output):
    output = Path(output)
    package(output)
    summary, config = read(output / "summary.json"), read(output / "config.json")
    recorded = pd.read_csv(output / "portfolio_trades.csv", low_memory=False)
    rebuilt, _ = allocate(recorded, config)
    check_allocation(rebuilt, recorded)
    actual = metrics(rebuilt, summary["days"])
    daily = pd.read_csv(output / "daywise_comparison.csv")
    if daily.day.tolist() != summary["days"]:
        raise ValueError("Daily output calendar mismatch")
    for field in ("trades", "wins", "losses", "net_pnl_rs", "gross_pnl_rs", "cost_rs"):
        if not np.isclose(actual[field], summary["result"][field], atol=1e-6, rtol=0):
            raise ValueError("Full-history summary mismatch: " + field)
        if not np.isclose(daily[field].sum(), actual[field], atol=1e-6, rtol=0):
            raise ValueError("Daily reconciliation failed: " + field)
    return True


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--strategy", type=int, choices=[4, 6])
    parser.add_argument("--verify", type=Path)
    args = parser.parse_args()
    if args.verify:
        verify(args.verify)
    elif args.strategy:
        run(args.strategy)
    else:
        parser.error("Choose --strategy or --verify")
