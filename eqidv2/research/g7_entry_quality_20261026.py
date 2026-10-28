"""Bounded, retrospective entry-quality experiment; never activates trading.

Run: python -m research.g7_entry_quality_20261026
All variants preserve G7 V2 targets, stops, selected stock inventory and capital.
The protocol is fixed separately; today's known result cannot select a rule.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path

import numpy as np
import pandas as pd

from research import g7_daily_backtest as daily
from research import g8_sources as sources
from research import g8_stock_context as context
from research import g8_vix_selection_research as core
from research.g8_vix_data import VixData
from research.g8_exit_engine import allocate_variant, metrics, _flags
from research.g4_entry_veto_rules import pending_invalidation
from research.g4_delayed_entry_research import replay as delayed_replay

ROOT = Path(__file__).resolve().parents[1]
PROTOCOL = ROOT / "research/g7_entry_quality_protocol_20261026.json"
DAY = "2026-10-26"
OUT_ROOT = sources.G8_ROOT / "entry_quality_research" / DAY
DAILIES = [
    (daily.DAILY_ROOT / "2026-10-23/20261023T183148514804_bbcb21e1_g7_nifty_futures",
     "b4bbc3fe061e9af20feb5d7d9fea869c0d36f171884e40d97f4263d8e5b67acc"),
    (daily.DAILY_ROOT / "2026-10-26/20261026T194254854688_5b5a77f8_g7_nifty_futures",
     "bf0c8f2fd778a90d81fe4bbe86616ad11136c43a7f616b8fab8c3578b78da0e9")]
G8 = sources.G8_ROOT / "full_history/2026-10-26/20261026T141620055325_g8_v2_adopted"


def read(path):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def verify_members(directory, expected=None):
    directory = Path(directory).resolve()
    manifest_path = directory / "manifest.json"
    if expected and core.sha(manifest_path) != expected:
        raise ValueError("Source manifest changed: " + str(directory))
    manifest = read(manifest_path)
    for name, digest in manifest["files"].items():
        path = (directory / name).resolve()
        if not path.is_relative_to(directory) or core.sha(path) != digest:
            raise ValueError("Source member changed: " + name)
    return dict(path=str(directory), manifest_sha256=core.sha(manifest_path))


def load_dataset():
    source = sources.load_sources()
    if source["manifest_sha256"] != "c0a52d928a3b7b6ac3a270a5908001a71f6740634c77ee41e7ee2244a7e0a6a9":
        raise ValueError("Unexpected historical cache")
    stock = [context.load_stock_context(source).reset_index(drop=True)]
    proof = [dict(path=str(source["cache"]), manifest_sha256=source["manifest_sha256"]),
             dict(path=str(context.CACHE), manifest_sha256=core.sha(context.CACHE / "manifest.json"))]
    parts = {k: [source[k]] for k in ("orders", "unfiltered", "baseline", "audit")}
    paths, days = dict(source["paths"]), list(source["days"])
    for directory, expected in DAILIES:
        proof.append(verify_members(directory, expected))
        orders = daily.g7.read_frozen_csv(directory / "selected_orders.csv")
        native = daily.g7.read_frozen_csv(directory / "g7_unfiltered/portfolio_trades.csv")
        baseline = daily.g7.read_frozen_csv(directory / "g7_nifty_futures_both_counter/portfolio_trades.csv")
        audit = pd.read_csv(directory / "nifty_direction_audit.csv")
        config = read(directory / "config.json")
        if config != source["config"]:
            raise ValueError("Daily G7 configuration differs")
        day = str(orders.day.iloc[0])[:10]
        remap = {int(sid): int(day.replace("-", ""))*10000+i for i, sid in enumerate(orders.sid)}
        raw_paths = pd.read_parquet(directory / "minute_paths.parquet")
        for sid, group in raw_paths.groupby("sid", sort=False):
            paths[remap[int(sid)]] = {f: group[f].to_numpy() for f in sources.PATH_FIELDS}
        for key, frame in (("orders", orders), ("unfiltered", native), ("baseline", baseline), ("audit", audit)):
            frame["source_sid"] = frame.sid
            frame["sid"] = frame.sid.map(remap).astype("int64")
            frame["day"] = frame.day.astype(str).str[:10]
            parts[key].append(frame)
        price_sources = read(directory / "price_sources.json")
        prices = {Path(p).name: (p, info["sha256"]) for p, info in price_sources.items()}
        lookup = audit.set_index("sid")
        extra = []
        for row in orders.to_dict("records"):
            path, digest = prices[row["tradingsymbol"]+"_stocks_indicators_1min.parquet"]
            raw = context._read_daily(path, digest)
            features = context.stock_context(raw, row, lookup.loc[row["sid"]])
            features.update(stock_source_path=path, stock_source_sha256=digest)
            extra.append(features)
        stock.append(pd.DataFrame(extra))
        days.append(day)
    source.update({k: pd.concat(v, ignore_index=True, sort=False) for k, v in parts.items()})
    source.update(paths=paths, days=sorted(days))
    if len(days) != 58 or len(source["orders"]) != 596 or source["orders"].sid.duplicated().any():
        raise ValueError("Wrong source inventory")
    if any(set(source[k].sid) != set(paths) for k in parts):
        raise ValueError("Missing candidates or paths")
    proof.append(verify_members(G8, "29d50518f4d96294fbe11f78f4251a8acb4a5475eda4609517e7cb9bea2516fa"))
    f = core.build_features(source, VixData(G8 / "extended_vix.csv"))
    extra = pd.concat(stock, ignore_index=True).set_index("sid")
    f = f.join(extra, validate="one_to_one")
    pending = pd.DataFrame([dict(sid=int(row["sid"]), **pending_invalidation(
        row, paths[int(row["sid"])], 10)) for row in source["orders"].to_dict("records")]).set_index("sid")
    f = f.join(pending, validate="one_to_one")
    source.update(features=f, source_proof=proof)
    return source


def decisions(f):
    """Only past feature columns influence these decisions; missing data rejects."""
    stamp = pd.to_datetime(f.confirmation_ts, utc=True)
    for name in ("feature_available_at", "stock_context_available_at", "vix_available_at"):
        available = pd.to_datetime(f[name], utc=True)
        if available.isna().any() or (available > stamp).any():
            raise ValueError("Feature not available by confirmation: " + name)
    base = _flags(f.nifty_base_allowed, "nifty_base_allowed")
    signed_slot = f.side_sign * f.nifty_slot_pct
    signed_session = f.side_sign * f.nifty_session_pct
    valid_market = np.isfinite(signed_slot) & np.isfinite(signed_session)
    close = (f.confirmation_close_position.ge(.75) & f.confirmation_body_direction.gt(0)
             & np.isfinite(f.confirmation_close_position) & np.isfinite(f.confirmation_body_direction))
    relative5 = (f.stock_relative5_status.eq("OK") & np.isfinite(f.stock_relative5_pct))
    relative_session = (f.stock_relative_session_status.eq("OK") & np.isfinite(f.stock_relative_session_pct))
    deadband = valid_market & ~(signed_slot.lt(-.10) & signed_session.lt(-.10))
    override = (relative5 & relative_session & f.stock_relative5_pct.ge(.30)
                & f.stock_relative_session_pct.ge(.30) & close)
    pending = ~_flags(f.pending_cancelled, "pending_cancelled")
    local = stamp.dt.tz_convert("Asia/Kolkata")
    return dict(
        g7_v2=base,
        g8_v2=core.allowed_for(f, dict(mode="persistent_direction", window=30, threshold=2.)),
        no_index_filter=pd.Series(True, index=f.index),
        close75=base & close,
        relative5=base & relative5 & f.stock_relative5_pct.ge(0),
        relative_session=base & relative_session & f.stock_relative_session_pct.ge(0),
        after0930=base & (local.dt.hour*60+local.dt.minute).ge(570),
        pending_cancel=base & pending,
        nifty_deadband010=deadband,
        stock_override=valid_market & (base | override),
        deadband_pending=deadband & pending,
        wait1_hold=base,
    )


def same_book(actual, expected):
    keys = ["day", "setup_id", "tradingsymbol"]
    a, b = [v.set_index(keys).sort_index() for v in (actual, expected)]
    if not a.index.equals(b.index) or not a.index.is_unique:
        raise ValueError("Baseline order inventory differs")
    if not np.array_equal(_flags(a.portfolio_executed, "actual"), _flags(b.portfolio_executed, "expected")):
        raise ValueError("Baseline allocations differ")
    executed = _flags(a.portfolio_executed, "executed")
    for name in ("entry_price", "exit_price", "portfolio_net_profit_rupees"):
        if not np.allclose(a.loc[executed, name], b.loc[executed, name], atol=1e-6, rtol=0):
            raise ValueError("Baseline replay differs: " + name)
    for name in ("entry_ts", "exit_ts"):
        if not pd.to_datetime(a.loc[executed, name], utc=True).equals(pd.to_datetime(b.loc[executed, name], utc=True)):
            raise ValueError("Baseline times differ")


def choose(comparison, candidates):
    """Selection deliberately cannot read assessment or today's rows."""
    train = comparison.loc[comparison.period.isin(["development", "validation"])].set_index(["policy", "period"])
    eligible, audit = [], []
    for name in candidates:
        failures = []
        changes = 0
        for period in ("development", "validation"):
            r, b = train.loc[(name, period)], train.loc[("g7_v2", period)]
            if r.net_pnl_rs <= b.net_pnl_rs+1e-6:
                failures.append(period+": no net improvement")
            if r.trades < .70*b.trades:
                failures.append(period+": less than70% trade retention")
            if r.daily_close_drawdown_rs > b.daily_close_drawdown_rs+1e-6:
                failures.append(period+": larger drawdown")
            changes += r.changed_executions
        if changes < 5 or sum(train.loc[(name, p), "changed_days"] for p in ("development", "validation")) < 3:
            failures.append("insufficient changed trades/days")
        score = sum(train.loc[(name, p), "net_change_rs"] for p in ("development", "validation"))
        audit.append(dict(policy=name, eligible=not failures, failure_reasons="; ".join(failures), score=score))
        if not failures:
            eligible.append((score, name))
    return (max(eligible)[1] if eligible else None), audit


def verify(output):
    """Recheck sealed files, implementations, source bytes, decisions and ledgers."""
    out = Path(output).resolve()
    manifest = read(out / "manifest.json")
    verify_members(out)
    for name, digest in manifest["code"].items():
        if core.sha(ROOT / name) != digest:
            raise ValueError("Research implementation changed: "+name)
    summary = read(out / "summary.json")
    for package in summary["source_packages"]:
        verify_members(package["path"], package["manifest_sha256"])
    f = pd.read_csv(out / "entry_features_and_decisions.csv").set_index("sid", drop=False)
    prices = f[["stock_source_path", "stock_source_sha256"]].drop_duplicates()
    for path, digest in prices.itertuples(index=False, name=None):
        if core.sha(Path(path)) != digest:
            raise ValueError("Original stock price evidence changed")
    masks = decisions(f)
    expected = pd.read_csv(out / "comparison.csv").set_index(["policy", "period"])
    for name, mask in masks.items():
        if not np.array_equal(mask, _flags(f["allowed_"+name], "saved_decision")):
            raise ValueError("Saved decision mismatch")
        book = pd.read_csv(out / name / "portfolio_trades.csv")
        if set(book.sid) != set(f.sid) or book.sid.duplicated().any():
            raise ValueError("Candidate inventory mismatch")
        if (_flags(book.portfolio_executed, "executed") & ~book.sid.map(mask)).any():
            raise ValueError("Disallowed trade received capital")
        for period, days in summary["periods"].items():
            actual = metrics(book, days)
            for key in ("trades", "wins", "losses", "net_pnl_rs", "daily_close_drawdown_rs", "cost_rs"):
                if not np.isclose(actual[key], expected.loc[(name, period), key], atol=1e-6, rtol=0):
                    raise ValueError("Saved accounting mismatch: "+name+"/"+key)
    selected, _ = choose(expected.reset_index(), read(out / "protocol.json")["candidates"])
    if selected != summary["selected_policy"]:
        raise ValueError("Chronological selection changed")
    return True


def run(output=None):
    protocol = read(PROTOCOL)
    protocol_digest = core.sha(PROTOCOL)
    protected = [ROOT / "configs/strategies/active_profile.json"]
    protected += list(daily.g7.G7_ROOT.glob("latest/*.json"))
    guard = {str(p): core.sha(p) for p in protected if p.is_file()}
    print("Loading exact archived candidates and pre-entry context", flush=True)
    source = load_dataset()
    f, original, config, days = source["features"], source["unfiltered"], source["config"], source["days"]
    masks = decisions(f)
    if set(masks) != set(protocol["controls"]+protocol["candidates"]):
        raise ValueError("Protocol and scenarios differ")
    periods = dict(all=days, development=days[:35], validation=days[35:49], assessment=days[49:-1], today=[DAY])
    if len(periods["assessment"]) != 8 or days[-1] != DAY:
        raise ValueError("Chronology changed")
    out = Path(output or OUT_ROOT / (datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%f")+"_fixed_rules"))
    if out.exists():
        raise ValueError("Fresh output directory required")
    out.mkdir(parents=True)
    core.dump(out / "protocol.json", protocol)
    rows, daily_rows, books = [], [], {}
    for name, allowed in masks.items():
        native = original
        if name == "wait1_hold":
            native = delayed_replay(original, source["paths"], delay_bars=1, require_hold=True, scenario=name)
            native["g8_baseline_filled"] = native.filled
        book = allocate_variant(native, allowed, config, name)
        books[name] = book
        if name == "g7_v2":
            same_book(book, source["baseline"])
        if name == "g8_v2":
            same_book(book, pd.read_csv(G8 / "portfolio_trades.csv"))
        base = books["g7_v2"].set_index("sid")
        current = book.set_index("sid").reindex(base.index)
        changed = (current.portfolio_executed.ne(base.portfolio_executed)
                   | (current.portfolio_executed & ~np.isclose(current.entry_price, base.entry_price, equal_nan=True)))
        for period, dates in periods.items():
            stats = metrics(book, dates)
            selected = base.day.isin(dates)
            rows.append(dict(policy=name, period=period, **stats,
                net_change_rs=stats["net_pnl_rs"]-metrics(books["g7_v2"], dates)["net_pnl_rs"],
                changed_executions=int((changed & selected).sum()),
                changed_days=int(base.loc[changed & selected, "day"].nunique())))
        for day in days:
            daily_rows.append(dict(policy=name, day=day, **metrics(book, [day])))
        folder = out / name
        folder.mkdir()
        book.to_csv(folder / "portfolio_trades.csv", index=False)
        same_book(pd.read_csv(folder / "portfolio_trades.csv"), book)
        f["allowed_"+name] = allowed
        print(name, round(rows[-len(periods)]["net_pnl_rs"], 2), "today", round(rows[-1]["net_pnl_rs"], 2), flush=True)
    comparison = pd.DataFrame(rows)
    chosen, selection_audit = choose(comparison, protocol["candidates"])
    assessment_passed = False
    if chosen:
        a = comparison.loc[(comparison.policy.eq(chosen)) & comparison.period.eq("assessment")].iloc[0]
        b = comparison.loc[(comparison.policy.eq("g7_v2")) & comparison.period.eq("assessment")].iloc[0]
        assessment_passed = bool(a.net_change_rs > 0 and a.daily_close_drawdown_rs <= b.daily_close_drawdown_rs+1e-6)
    costs = []
    for name, book in books.items():
        for cost in protocol["cost_bps_round_trip"]:
            for period, dates in periods.items():
                stat = metrics(book, dates)
                net = stat["gross_pnl_rs"]-stat["trades"]*config["capital_per_entry_rs"]*config["leverage"]*cost/10000.
                costs.append(dict(policy=name, period=period, cost_bps=cost, net_pnl_rs=net, trades=stat["trades"]))
    daywise = pd.DataFrame(daily_rows)
    wide = daywise.pivot(index="day", columns="policy", values="net_pnl_rs")
    rng = np.random.default_rng(protocol["bootstrap"]["seed"])
    uncertainty = []
    for name in protocol["candidates"]:
        values = (wide[name]-wide.g7_v2).loc[wide.index < DAY].to_numpy()
        draws = rng.choice(values, size=(protocol["bootstrap"]["replications"], len(values)), replace=True).mean(axis=1)
        uncertainty.append(dict(policy=name, sessions=len(values), mean_daily_delta_rs=float(values.mean()),
            mean_daily_delta_ci95_low=float(np.quantile(draws, .025)), mean_daily_delta_ci95_high=float(np.quantile(draws, .975)),
            better_days=int((values>1e-6).sum()), worse_days=int((values < -1e-6).sum())))
    today_audit = f.loc[f.day.eq(DAY)].copy()
    for name, book in books.items():
        mapped = book.set_index("sid")
        today_audit[name+"_executed"] = mapped.portfolio_executed.reindex(today_audit.index)
        today_audit[name+"_net_rs"] = mapped.portfolio_net_profit_rupees.reindex(today_audit.index)
    compact = today_audit[["tradingsymbol", "side", "confirmation_ts", "signed_nifty_slot",
        "signed_nifty_session", "confirmation_close_position", "stock_relative5_pct",
        "stock_relative_session_pct", "g7_v2_executed", "g7_v2_net_rs",
        "nifty_deadband010_executed", "nifty_deadband010_net_rs"]].copy()
    compact["original_entry_decision"] = np.where(today_audit.allowed_g7_v2,
        "Index gate permits; stock must still touch trigger before expiry", "Both Nifty returns oppose side")
    compact["deadband_decision"] = np.where(today_audit.allowed_nifty_deadband010,
        "Permitted: at least one Nifty move is aligned or within0.10pct neutral band", "Blocked: both Nifty moves oppose by more than0.10pct")
    compact.to_csv(out / "today_explanation.csv", index=False)
    comparison.to_csv(out / "comparison.csv", index=False)
    daywise.to_csv(out / "daywise_comparison.csv", index=False)
    pd.DataFrame(selection_audit).to_csv(out / "selection_audit.csv", index=False)
    pd.DataFrame(costs).to_csv(out / "cost_sensitivity.csv", index=False)
    pd.DataFrame(uncertainty).to_csv(out / "paired_day_uncertainty.csv", index=False)
    f.to_csv(out / "entry_features_and_decisions.csv", index=False)
    today_audit.to_csv(out / "today_entry_audit.csv", index=False)
    summary = dict(version=protocol["version"], scope="BACKTEST_ONLY", execution_authority=False,
        protocol_sha256=protocol_digest, periods=periods, selected_policy=chosen, assessment_passed=assessment_passed,
        promotion=False, independent_holdout=False, baseline_reproduced=True, g8_baseline_reproduced=True,
        selected_orders=len(original), source_packages=source["source_proof"], protected_files_unchanged=guard,
        missing_sessions=["2026-08-24", "2026-08-25", "2026-10-01"],
        limitations=["All history is retrospective, inherited targets were previously inspected; today is known and excluded from selection.",
            "Three missing source sessions are excluded, not treated as zero P&L.",
            "Inherited after-close stock-day data eligibility is not a validated live causal universe.",
            "Minute OHLC and instantaneous modeled order changes cannot establish queue fills or actual latency.",
            "Fixed INR100000 margin and5x exposure per entry, fractional theoretical shares, INR1000000 capital,5bps costs.",
            "Bootstrap is descriptive and unadjusted for multiple comparisons; eight assessment sessions are limited.",
            "Wait-one-bar uses a completed waiting candle and conservative native candle-end fill timestamps."])
    if core.sha(PROTOCOL) != protocol_digest or any(core.sha(Path(p)) != h for p, h in guard.items()):
        raise ValueError("Protocol or protected active configuration changed")
    core.dump(out / "summary.json", summary)
    html = "<!doctype html><meta charset='utf-8'><title>Entry-quality research</title><style>body{font:15px system-ui;margin:32px;max-width:1500px}table{border-collapse:collapse}td,th{border:1px solid #ddd;padding:7px}th{background:#eee}</style>"
    html += "<h1>Entry-quality research through26 October2026</h1><p>Backtest only. Nine fixed candidates; all596 selected opportunities and58 available sessions. Missing:24/25 August,1 October. Existing G7 V2 and G8 V2 reproduced exactly.</p>"
    html += f"<p>Chronological selection: {chosen or 'NONE; retain baseline'}. Later-eight-session assessment passed: {assessment_passed}. No live or paper settings changed.</p>"
    html += "<p>All history has been inspected previously. These are retrospective diagnostics, not independent validation. Capital:10lakh;1lakh margin and5x exposure per entry;5bps modeled round-trip costs.</p>"
    html += "<h2>Why26 October lost</h2><p>BANDHANBNK and CAMS were early shorts aligned with a falling Nifty, then reversed. KFINTECH and GODREJPROP were longs aligned with a rising Nifty. All four had positive directional stock-relative5-minute strength and valid confirmations. A breakout touch can still reverse; these filters cannot establish future continuation.</p>"
    html += "<p>BSE and TATAPOWER were blocked because both Nifty return signs opposed the trade, even though the latest5-minute changes were only-0.0102% and+0.0482%. A fixed0.10% neutral band restores those two winners today but also admits more historical losers. Its validation-period loss and higher drawdown disqualify it under this protocol.</p>"
    html += compact[["tradingsymbol", "side", "g7_v2_executed", "g7_v2_net_rs", "nifty_deadband010_executed", "nifty_deadband010_net_rs"]].to_html(index=False, float_format=lambda v: f"{v:,.2f}")
    columns = ["policy", "trades", "win_rate_pct", "net_pnl_rs", "net_change_rs", "daily_close_drawdown_rs"]
    for period in periods:
        html += "<h2>"+period+"</h2>"+comparison.loc[comparison.period.eq(period), columns].to_html(index=False, float_format=lambda v: f"{v:,.2f}")
    html += "<h2>Selection failures</h2>"+pd.DataFrame(selection_audit).to_html(index=False)
    html += "<p><a href='today_explanation.csv'>Today's concise entry explanations</a> | <a href='today_entry_audit.csv'>Full feature audit</a> | <a href='daywise_comparison.csv'>Daily results</a> | <a href='paired_day_uncertainty.csv'>Uncertainty</a> | <a href='cost_sensitivity.csv'>Cost sensitivity</a></p>"
    (out / "report.html").write_text(html, encoding="utf-8")
    code = [Path(__file__), PROTOCOL, Path(core.__file__), Path(context.__file__),
            ROOT / "research/g4_entry_veto_rules.py", ROOT / "research/g4_delayed_entry_research.py",
            ROOT / "research/g8_exit_engine.py", ROOT / "research/g3_sl_target_simulator.py",
            ROOT / "research/g8_vix_data.py", ROOT / "research/g8_nifty_vix_backtest.py",
            ROOT / "research/g8_sources.py", ROOT / "fno_v13_corrected_v5_backtest.py"]
    core.dump(out / "manifest.json", dict(files={p.relative_to(out).as_posix(): core.sha(p) for p in out.rglob("*") if p.is_file()},
        code={str(p.relative_to(ROOT)): core.sha(p) for p in code}))
    verify(out)
    print("OUTPUT", out, "SELECTED", chosen, "ASSESSMENT", assessment_passed, flush=True)
    return out


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output")
    parser.add_argument("--verify")
    args = parser.parse_args()
    if args.verify:
        print("Verified", verify(args.verify))
    else:
        run(args.output)
