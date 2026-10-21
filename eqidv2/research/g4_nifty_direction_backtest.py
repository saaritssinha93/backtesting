"""G4 all-setup Nifty alignment research with confirmation-time decisions.

The primary filter reads the latest completed five-minute Nifty futures candle
and blocks SHORT on a positive candle and LONG on a negative candle. Flat is
allowed. Close-to-close and session-open variants are reported independently.
The slot/side decision is shared by every stock at that slot, so filtering the
selected orders cannot hide a replacement of the same side at that slot.
"""
from __future__ import annotations

import argparse
from datetime import date, datetime
import html
import json
from pathlib import Path
import shutil

import numpy as np
import pandas as pd

from research import g4_bounded_exit_backtest as bounded
from research import g4_complete_inventory_backtest as complete
from research import g4_failed_breakout_research as sealed
from research import g4_g3_mirror_backtest as base
from research import g4_delayed_entry_research as replay
from research import g4_iterative_dataset as dataset

VERSION = "G4-ALL-SETUP-NIFTY-DIRECTION-V1"
MODES = ("control", "slot_candle", "close_to_close", "session_open")


def dump(path, value):
    Path(path).write_text(json.dumps(value, indent=2, allow_nan=False, default=str) + "\n", encoding="utf-8")


def load_baseline():
    """Refresh all G4 sessions from verified G3 evidence without publishing pointers."""
    config = complete.configuration()
    frozen, _, frozen_paths, frozen_proof = dataset.load(replay.DEFAULT_DATASET)
    old, old_summary, current, summary, pointer = bounded.checked_sources(bounded.configuration(), None)
    prices = complete.Prices(summary)
    print("Verified frozen 53-session cache; selecting appended sessions", flush=True)
    retained, identity, changes, sources, _ = bounded.extended_orders(old, old_summary, current, summary, prices)
    g3 = base.read_frozen_csv(current / "selected_orders.csv")
    combined = pd.concat([retained, g3.loc[g3.setup_id.isin(complete.ADDED_IDS)]], ignore_index=True, sort=False)
    if combined.duplicated(["day", "setup_id", "tradingsymbol"]).any():
        raise ValueError("Duplicate G4 order")
    orders = complete.Identity(config).identify(combined)
    days = summary["days"]
    historical_orders = orders.loc[orders.day.isin(frozen_proof["days"])]
    left = historical_orders.sort_values(["day", "setup_id", "tradingsymbol"]).reset_index(drop=True)
    right = frozen.sort_values(["day", "setup_id", "tradingsymbol"]).reset_index(drop=True)
    # Keep the accepted frozen G4 history. Updated G3 source selection must not
    # rewrite previously sealed G4 trades during a new filter comparison.
    keys = ["day", "setup_id", "tradingsymbol"]
    drift = right[keys].merge(left[keys], on=keys, how="outer", indicator=True)
    source_drift = drift.loc[drift._merge.ne("both")].astype(str).to_dict("records")
    summary["g4_frozen_vs_current_g3_reconstruction_drift"] = source_drift
    paths = frozen_paths.copy()
    historical_native = frozen.copy()
    extra_days = [day for day in days if day not in frozen_proof["days"]]
    extra = orders.loc[orders.day.isin(extra_days)].copy()
    extra["sid"] = np.arange(110_000_000, 110_000_000 + len(extra))
    print(f"Retained frozen G4 history ({len(source_drift)} current-source selection differences); loading {len(extra)} orders for {extra_days}", flush=True)
    paths.update(prices.paths(extra.sort_values(["tradingsymbol", "day", "confirmation_ts"])))
    extra_native = bounded.history._simulate(extra, paths,
        [date.fromisoformat(day) for day in extra_days], complete.Identity(config))
    original = complete._bounded(pd.concat([historical_native, extra_native], ignore_index=True, sort=False), paths, config)
    prior = original.loc[original.day.isin(frozen_proof["days"])].sort_values(
        ["day", "setup_id", "tradingsymbol"]).reset_index(drop=True)
    for field in ("filled", "entry_price", "exit_price", "portfolio_executed", "portfolio_net_profit_rupees"):
        pd.testing.assert_series_equal(prior[field], right[field], check_names=False, check_dtype=False)
    print(f"G4 control verified: {len(days)} sessions, {len(original)} orders", flush=True)
    return original, paths, config, days, prices, summary, current, pointer, sources


def nifty_day(day, month, prices):
    """Prefer the matching immutable input snapshot; record live historical fallback."""
    symbol = f"NIFTY{month}FUT"
    directory = prices.snapshots.get(date.fromisoformat(day))
    if directory is not None:
        path = directory / "futures_5m" / f"{symbol}_5minute.parquet"
    else:
        path = base.source.DataRoots().futures_5m / f"{symbol}_5minute.parquet"
    digest = base.sha256(path)
    if directory is not None and prices.expected.get(str(path.resolve())) != digest:
        raise ValueError("Nifty immutable snapshot mismatch: " + str(path))
    raw = pd.read_parquet(path)
    raw["ts"] = pd.to_datetime(raw.timestamp, utc=True).dt.tz_convert("Asia/Kolkata").dt.as_unit("ns")
    frame = raw.loc[raw.ts.dt.strftime("%Y-%m-%d").eq(day)].sort_values("ts").reset_index(drop=True)
    if frame.empty or frame.ts.duplicated().any():
        raise ValueError("Missing or duplicate Nifty session: " + day)
    if not frame.tradingsymbol.eq(symbol).all():
        raise ValueError("Wrong Nifty futures contract")
    if base.sha256(path) != digest:
        raise ValueError("Nifty file changed while reading")
    proof = dict(day=day, contract=symbol, path=str(path.resolve()), sha256=digest,
        source_kind="IMMUTABLE_DAY_SNAPSHOT" if directory is not None else "CURRENT_HISTORICAL_PARQUET",
        first_bar=str(frame.ts.iloc[0]), last_bar=str(frame.ts.iloc[-1]), bars=len(frame))
    return frame, proof


def normalize_month(value):
    text = str(value).strip().upper()
    if len(text) == 5 and text[:2].isdigit() and text[2:].isalpha():
        return text
    stamp = pd.to_datetime(text + "-01", format="%Y-%m-%d", errors="raise")
    return stamp.strftime("%y%b").upper()


def direction_at(frame, decision):
    """Only exact completed bar clocks; no forward fill over missing bars."""
    decision = pd.Timestamp(decision)
    if decision.tzinfo is None:
        raise ValueError("Timezone-aware decision required")
    decision = decision.tz_convert("Asia/Kolkata")
    expected = decision.floor("5min")
    observed = frame.loc[frame.ts.le(decision)].sort_values("ts")
    exact = observed.loc[observed.ts.eq(expected)]
    opening = observed.loc[observed.ts.eq(decision.normalize() + pd.Timedelta(hours=9, minutes=20))]
    if len(exact) != 1 or len(opening) != 1:
        return dict(status="MISSING_EXACT_NIFTY_BAR", nifty_bar_ts=str(expected),
            slot_candle=np.nan, close_to_close=np.nan, session_open=np.nan)
    bar = exact.iloc[0]
    if "quality_state" in exact and not str(bar.quality_state).upper() == "VALID":
        return dict(status="INVALID_NIFTY_BAR", nifty_bar_ts=str(expected),
            slot_candle=np.nan, close_to_close=np.nan, session_open=np.nan)
    values = [float(bar.open), float(bar.close), float(opening.iloc[0].open)]
    if not np.isfinite(values).all() or min(values) <= 0:
        raise ValueError("Invalid Nifty price")
    previous = observed.loc[observed.ts.eq(expected - pd.Timedelta(minutes=5))]
    first = expected.hour == 9 and expected.minute == 20
    previous_close = float(bar.open) if first else float(previous.iloc[0].close) if len(previous) == 1 else np.nan
    close = float(bar.close)
    return dict(status="OK" if np.isfinite(previous_close) else "MISSING_PREVIOUS_NIFTY_BAR",
        nifty_bar_ts=str(expected), nifty_open=float(bar.open), nifty_close=close,
        nifty_previous_close=previous_close, nifty_session_open=float(opening.iloc[0].open),
        slot_candle=(close / values[0] - 1) * 100,
        close_to_close=(close / previous_close - 1) * 100 if np.isfinite(previous_close) else np.nan,
        session_open=(close / values[2] - 1) * 100)


def allow_direction(side, change):
    if side not in ("LONG", "SHORT") or not np.isfinite(change):
        raise ValueError("Valid side and finite Nifty direction required")
    return bool(change >= 0 if side == "LONG" else change <= 0)


def filter_book(original, audit, mode, config):
    work = original.copy()
    allowed = pd.Series(True, index=original.index) if mode == "control" else original.sid.map(
        audit.set_index("sid")[mode + "_allowed"])
    if allowed.isna().any():
        raise ValueError("Missing order decision")
    blocked = ~allowed.astype(bool)
    work["g4_native_filled"] = work.filled.eq(True)
    work["g4_nifty_allowed"] = allowed.to_numpy(bool)
    work["filled"] = work.filled.eq(True) & allowed
    engine = sealed.bounded.history.simulator.g.v9
    sized = engine.v5.apply_fixed_capital_model(work, config["capital_per_entry_rs"], config["leverage"])
    settings = engine.V9Config(portfolio_capital_rupees=config["portfolio_capital_rs"],
        capital_per_entry_rupees=config["capital_per_entry_rs"], leverage_factor=config["leverage"],
        max_positions=10, cost_bps=5.)
    book, portfolio = engine.v6.apply_portfolio_constraints(sized, settings.portfolio_config())
    book.loc[blocked, "portfolio_status"] = "NIFTY_DIRECTION_BLOCKED"
    book.loc[blocked, "portfolio_reject_reason"] = "COUNTER_NIFTY_DIRECTION"
    book["strategy_version"] = VERSION + "-" + mode
    book["execution_authority"] = False
    book["scope"] = "BACKTEST_ONLY"
    return book, portfolio


def run(output):
    output = Path(output).resolve()
    if output.exists() or not output.is_relative_to((base.G4_ROOT / "research").resolve()):
        raise ValueError("Require a fresh isolated G4 research directory")
    original, paths, config, days, prices, source_summary, current, pointer, sources = load_baseline()
    # Every order uses the contract month from the original frozen stock selection.
    records, proofs = [], []
    for day, group in original.groupby("day", sort=True):
        months = group.contract_month.dropna().map(normalize_month).unique()
        if len(months) != 1:
            raise ValueError("Ambiguous dated contract month: " + day)
        frame, proof = nifty_day(day, months[0], prices)
        proofs.append(proof)
        for row in group.itertuples():
            values = direction_at(frame, row.confirmation_ts)
            record = dict(sid=int(row.sid), day=day, setup_id=row.setup_id,
                tradingsymbol=row.tradingsymbol, side=row.side, confirmation_ts=str(row.confirmation_ts),
                nifty_contract=proof["contract"], nifty_source_path=proof["path"], **values)
            for mode in MODES[1:]:
                record[mode + "_allowed"] = allow_direction(row.side, values[mode]) if np.isfinite(values[mode]) else False
            records.append(record)
    audit = pd.DataFrame(records)
    missing = audit.loc[~np.isfinite(audit.slot_candle) | ~np.isfinite(audit.session_open)]
    if len(missing):
        print(missing[["day", "setup_id", "status", "nifty_bar_ts"]].to_string(index=False), flush=True)
        raise ValueError(f"Nifty coverage missing for {len(missing)} selected orders; refusing an incomplete full-history comparison")
    omitted_modes = [mode for mode in MODES[1:] if not np.isfinite(audit[mode]).all()]
    enabled_modes = [mode for mode in MODES if mode not in omitted_modes]
    for proof in proofs:
        if base.sha256(Path(proof["path"])) != proof["sha256"]:
            raise ValueError("Nifty source changed during audit")
    output.mkdir(parents=True)
    dump(output / "config.json", dict(variant_id=VERSION, scope="BACKTEST_ONLY", execution_authority=False,
        primary_mode="slot_candle", rules={"slot_candle":"last completed 5m close versus its open",
        "close_to_close":"last completed 5m close versus prior completed 5m close; first candle uses its open",
        "session_open":"last completed 5m close versus exact 09:20 candle open"},
        neutral="Flat permits either side", missing="Fail full run, never silently neutral",
        source="Dated near-month Nifty futures, not cash index", base_g4_config=config))
    audit.to_csv(output / "nifty_order_audit.csv", index=False)
    pd.DataFrame(proofs).to_csv(output / "nifty_source_proof.csv", index=False)
    comparison, daily, setups, changes = [], [], [], []
    periods = dict(all=days, october_15=["2026-10-15"], october_16=["2026-10-16"], last_day=[days[-1]])
    for mode in enabled_modes:
        book, allocation = filter_book(original, audit, mode, config)
        if mode == "control":
            sealed.parity(book, original)
        delta = replay._changes(original, book, mode)
        changes.append(delta)
        for period, cohort in periods.items():
            metrics, baseline = bounded.stats(book, cohort), bounded.stats(original, cohort)
            part = delta.loc[delta.day_baseline.isin(cohort)]
            baseline_winner = part.portfolio_net_profit_rupees_baseline.gt(0)
            baseline_loser = part.portfolio_net_profit_rupees_baseline.lt(0)
            skipped = part.portfolio_executed_baseline.eq(True) & ~part.portfolio_executed_variant.eq(True)
            comparison.append(dict(mode=mode, period=period, **metrics,
                net_change_rs=metrics["net_pnl_rs"] - baseline["net_pnl_rs"],
                baseline_losers_avoided=int((baseline_loser & skipped).sum()),
                baseline_winners_missed=int((baseline_winner & skipped).sum()),
                baseline_loss_avoided_rs=float(-part.loc[baseline_loser & skipped, "portfolio_net_profit_rupees_baseline"].sum()),
                baseline_winner_profit_missed_rs=float(part.loc[baseline_winner & skipped, "portfolio_net_profit_rupees_baseline"].sum()),
                newly_allocated_trades=int((~part.portfolio_executed_baseline.eq(True) & part.portfolio_executed_variant.eq(True)).sum())))
        daily.extend(dict(mode=mode, day=day, **bounded.stats(book, [day])) for day in days)
        for setup in config["active_setups"]:
            subset = book.loc[book.setup_id.eq(setup["setup_id"])]
            setups.append(dict(mode=mode, setup_id=setup["setup_id"], signal_end=setup["signal_end"],
                side=setup["side"], **bounded.stats(subset, days)))
        folder = output / mode
        folder.mkdir()
        book.to_csv(folder / "portfolio_trades.csv", index=False)
        dump(folder / "allocation.json", allocation)
        saved = base.read_frozen_csv(folder / "portfolio_trades.csv")
        for field, value in bounded.stats(book, days).items():
            if not np.isclose(bounded.stats(saved, days)[field], value, atol=1e-6, rtol=0):
                raise ValueError("Saved portfolio accounting mismatch")
        print(mode, bounded.stats(book, days), flush=True)
    comparison, daily = pd.DataFrame(comparison), pd.DataFrame(daily)
    comparison.to_csv(output / "comparison.csv", index=False)
    daily.to_csv(output / "daily_comparison.csv", index=False)
    pd.DataFrame(setups).to_csv(output / "setup_results.csv", index=False)
    pd.concat(changes, ignore_index=True).to_csv(output / "trade_changes.csv", index=False)
    original.to_csv(output / "baseline_native.csv", index=False)
    # Seal the exact Nifty observations used by each dated contract, independent of future source updates.
    snapshot_dir = output / "nifty_inputs"
    snapshot_dir.mkdir()
    for proof in proofs:
        frame, _ = nifty_day(proof["day"], proof["contract"][5:-3], prices)
        frame.to_parquet(snapshot_dir / (proof["day"] + ".parquet"), index=False)
    for path, evidence in prices.proof.items():
        if base.sha256(Path(path)) != evidence["sha256"]:
            raise ValueError("Equity source changed during replay")
    if base.sha256(current / "manifest.json") != pointer["manifest_sha256"]:
        raise ValueError("Baseline source manifest changed")
    summary = dict(variant_id=VERSION, scope="BACKTEST_ONLY", execution_authority=False,
        baseline_default_changed=False, first_date=days[0], last_date=days[-1], sessions=len(days), days=days,
        active_setups=51, source_g3_run=str(current), source_g3_manifest_sha256=pointer["manifest_sha256"],
        baseline_prior_53_sessions_parity_verified=True, baseline_control_parity_verified=True,
        historical_baseline="Accepted sealed 53-session G4, with only later verified sessions appended",
        historical_g3_reconstruction_drift=source_summary["g4_frozen_vs_current_g3_reconstruction_drift"],
        order_audit_count=len(audit), missing_nifty_order_contexts=0, nifty_proofs=proofs,
        enabled_modes=enabled_modes, omitted_modes_missing_context=omitted_modes,
        missing_secondary_context=audit.loc[~np.isfinite(audit.close_to_close),
            ["sid", "day", "setup_id", "nifty_bar_ts", "status"]].to_dict("records"),
        equity_source_proofs=prices.proof, appended_order_sources=sources,
        comparison=comparison.to_dict("records"),
        limitations=["Latest verified full-history source ends 2026-10-19; October 20 replay is blocked by missing source data; October 21 is not a completed verified session in this source.",
            "Nifty direction is measured on near-month futures; cash index and constituent-specific relative strength are not evaluated.",
            "Completed five-minute timestamps are causal bar boundaries, not proof of actual feed receipt before order submission.",
            "Existing G4 eligibility, stops, targets, 5bps costs and 1-lakh capital/5x exposure/10-lakh portfolio are retained.",
            "Current G3 source reconstruction differs from accepted frozen G4 historical selection; frozen G4 history is retained and the differences are disclosed in the summary.",
            "Close-to-close sensitivity is omitted if any preceding Nifty candle is missing; primary candle-direction and session-open checks require complete coverage.",
            "The direction gate is common to all stocks in the same setup/confirmation slot and side; rejected slots have no eligible same-side replacement.",
            "Older Nifty/equity history without a session snapshot uses hash-recorded current historical parquets; revised historical data and data-snooping remain limitations.",
            "Previously examined historical sessions are not untouched validation; daily-close drawdown excludes intraday extremes."])
    dump(output / "summary.json", summary)
    columns = ["mode", "period", "trades", "wins", "losses", "win_rate_pct", "net_pnl_rs", "net_change_rs", "negative_trade_sum_abs_rs", "daily_close_drawdown_rs"]
    table = lambda value: '<div class="scroll">' + value.to_html(index=False, border=0, float_format=lambda x: f"{x:,.2f}") + '</div>'
    report = ('<!doctype html><html><head><meta charset="utf-8"><title>G4 Nifty direction test</title>'
        '<style>body{background:#101927;color:#e9f1ff;font:15px/1.6 system-ui;max-width:1600px;margin:auto;padding:25px}a{color:#8ed6ff}'
        'table{border-collapse:collapse;font-size:13px}td,th{padding:8px;border-bottom:1px solid #476078;white-space:nowrap;text-align:right}.scroll{overflow:auto}</style></head><body>'
        f'<h1>G4: Nifty direction across all 51 setups</h1><p>{days[0]} through {days[-1]} · {len(days)} sessions · research only.</p>'
        '<p>Primary rule: reject SHORT when the latest completed 5-minute Nifty futures candle closes above its open; reject LONG when it closes below its open. Flat allows either side. Exact confirmation-time context is required. Full portfolio allocation is replayed.</p>'
        '<h2>Full history</h2>' + table(comparison.loc[comparison.period.eq("all"), columns]) +
        '<h2>15 / 16 October and latest verified day</h2>' + table(comparison.loc[comparison.period.ne("all"), columns]) +
        '<h2>Every setup</h2>' + table(pd.DataFrame(setups)) + '<h2>Daily results</h2>' + table(daily) +
        '<h2>Limits</h2><ul>' + ''.join('<li>' + html.escape(x) + '</li>' for x in summary["limitations"]) +
        '</ul><p><a href="comparison.csv">Comparisons</a> · <a href="daily_comparison.csv">Daily results</a> · '
        '<a href="setup_results.csv">All setups</a> · <a href="nifty_order_audit.csv">Nifty decision evidence</a> · '
        '<a href="trade_changes.csv">Every trade change</a></p></body></html>')
    (output / "report.html").write_text(report, encoding="utf-8")
    (output / "ROLLBACK.txt").write_text("Research-only run. No production/default configuration or latest-result pointer changed. The control folder contains the unchanged G4 benchmark. Keep the run for audit; use existing G4 CLI for original behavior.\n", encoding="utf-8")
    code = [Path(__file__), Path(complete.__file__), complete.DEFAULT_CONFIG, Path(bounded.__file__),
        Path(base.__file__), Path(bounded.history.__file__), Path(base.source.__file__),
        Path(bounded.simulate_exit.__code__.co_filename)]
    (output / "code").mkdir()
    for path in code:
        shutil.copy2(path, output / "code" / path.name)
    dump(output / "manifest.json", dict(files={p.relative_to(output).as_posix(): base.sha256(p)
        for p in output.rglob("*") if p.is_file()}, code_sha256={str(p.resolve()): base.sha256(p) for p in code}))
    print(comparison[columns].to_string(index=False), flush=True)
    print(output, flush=True)
    return comparison


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path)
    args = parser.parse_args()
    output = args.output_dir or base.G4_ROOT / "research" / datetime.now(base.production.IST).strftime("%Y%m%dT%H%M%S_nifty_direction_v1")
    run(output)


if __name__ == "__main__":
    main()
