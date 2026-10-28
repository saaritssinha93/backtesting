"""Isolated causal early-entry study. Never imports a live order interface.

All experiment rules apply to every stock in each dated universe. Named stocks
are used only when exporting the requested diagnosis, never for selection.
"""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date
import hashlib
import heapq
import html
import json
from pathlib import Path
import sys

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import fno_v13_replay_source as source
import fno_v13_v10_g_2_backtest as execution
import fno_v13_v10_g_3_daily_replay as replay
from research.g3_backtest_profile import contract

HISTORY = Path(r"C:\TradingData\eqidv2\backtesting_result_v13_v10_g_3\backtests\runs\2026-10-23\20261023T172057919135_0b57cd86_current_full_history")
OUTPUT = ROOT / "outputs/g3_early_entry_20261023/universe"
REQUESTED = {"SWIGGY": "SHORT", "MOTILALOFS": "SHORT", "BANDHANBNK": "SHORT",
             "COLPAL": "SHORT", "DELHIVERY": "SHORT", "ETERNAL": "LONG"}
CLOCKS = pd.date_range("2000-01-01 09:20", "2000-01-01 13:30", freq="5min").strftime("%H:%M").tolist()
BUILD_VERSION = "sealed_opening_ema_seed_exact_recurrence_v1"


def sha(path):
    return source._sha(Path(path))


def write_json(path, value):
    Path(path).write_text(json.dumps(source._json_ready(value), indent=2), encoding="utf-8")


def build_day(evidence, output):
    run = Path(evidence["run_dir"])
    day = date.fromisoformat(run.parent.name)
    folder = output / "cache" / str(day)
    folder.mkdir(parents=True, exist_ok=True)
    feature_path = folder / "features.parquet"
    minute_path = folder / "minutes.parquet"
    meta_path = folder / "metadata.json"
    manifest = Path(evidence["snapshot_manifest"])
    manifest_hash = sha(manifest)
    if manifest_hash != evidence["snapshot_manifest_sha256"]:
        raise ValueError(f"Snapshot identity mismatch: {day}")
    seed_path = run / "feature_ledger.csv"
    run_manifest = json.loads((run / "run_manifest.json").read_text())
    seed_hash = sha(seed_path)
    if seed_hash != run_manifest["artifacts"]["feature_ledger"]["sha256"]:
        raise ValueError(f"Sealed opening seed integrity mismatch: {day}")
    dependencies = {str(Path(module.__file__).resolve()): sha(module.__file__)
                    for module in (source, source.hybrid, source.common)}
    if feature_path.exists() and minute_path.exists() and meta_path.exists():
        meta = json.loads(meta_path.read_text())
        if (meta.get("snapshot_sha256") == manifest_hash and meta.get("clocks") == CLOCKS
                and meta.get("build_version") == BUILD_VERSION
                and meta.get("seed_sha256") == seed_hash
                and meta.get("builder_dependencies") == dependencies
                and sha(feature_path) == meta.get("features_sha256")
                and sha(minute_path) == meta.get("minutes_sha256")):
            return day, pd.read_parquet(feature_path), pd.read_parquet(minute_path), meta
    snapshot = manifest.parent
    source._verify_input_snapshot(snapshot, json.loads(manifest.read_text()))
    old = pd.read_csv(seed_path)
    rebuilt=[]; minute_parts=[]; problems=[]; excluded=[]
    for symbol, original in old.groupby("tradingsymbol", sort=True):
        first = original.loc[original.hhmm_int.eq(920)]
        if len(first) != 1:
            raise ValueError(f"Missing unique causal opening seed {day} {symbol}")
        first = first.iloc[0]
        path = snapshot / "equity_1m" / f"{symbol}_stocks_indicators_1min.parquet"
        names = set(pq.ParquetFile(path).schema.names)
        fields = ["date", "open", "high", "low", "close", "volume"]
        fields += [x for x in ("gap_filled", "opening_snapshot", "provisional_stale") if x in names]
        cutoff = pd.Timestamp(f"{day} 15:15", tz="Asia/Kolkata")
        start = pd.Timestamp(day, tz="Asia/Kolkata")-pd.Timedelta(days=10)
        minute = pd.read_parquet(path, columns=fields, filters=[("date", ">=", start), ("date", "<=", cutoff)])
        minute["ts"] = source.hybrid._to_ist(minute.date).dt.as_unit("ns")
        if minute.ts.duplicated().any():
            raise ValueError(f"Duplicate minute {symbol} {day}")
        minute = minute.sort_values("ts").reset_index(drop=True)
        future_symbol = str(first.futures_tradingsymbol)
        before = len(problems)
        future = source._load_future(snapshot / "futures_5m" / f"{source.common.safe_contract_stem(future_symbol)}_5minute.parquet", day, problems, symbol)
        source._coverage(minute, future, day, symbol, problems, signal_clocks=CLOCKS)
        errors = problems[before:]
        if any(x["reason"] not in source.EXCLUDABLE_COVERAGE_REASONS for x in errors):
            raise ValueError(f"Blocking coverage error {day} {symbol}: {errors}")
        if errors:
            excluded.append(dict(symbol=symbol, errors=errors)); continue
        pool = source._observed_pool(minute, future, day=day, symbol=symbol,
            future_symbol=future_symbol, month=str(first.contract_month), signal_clocks=CLOCKS)
        if len(pool) != len(CLOCKS):
            raise ValueError(f"Incomplete full-clock pool {day} {symbol}: {len(pool)}")
        pool = pool.sort_values("signal_ts").reset_index(drop=True)
        # Exact recursive EMA continuation from the SEALED 09:20 feature,
        # already known at 09:20. No later-day feature seeds the recurrence.
        for span in (9, 20, 50):
            previous = float(first[f"ema{span}"]); values=[previous]
            alpha = 2/(span+1)
            for close in pool.close.iloc[1:]:
                previous = (1-alpha)*previous+alpha*float(close)
                values.append(previous)
            pool[f"ema{span}"] = values; pool[f"v9_5m_ema{span}"] = values
        pool["v9_5m_ema_bull"] = (pool.ema9 > pool.ema20) & (pool.ema20 > pool.ema50)
        pool["v9_5m_ema_bear"] = (pool.ema9 < pool.ema20) & (pool.ema20 < pool.ema50)
        for key in ("oi", "prev_oi", "oi_change_pct", "price_change_pct", "opening_0920_status", "price_change_basis", "oi_change_source"):
            if key not in pool: pool[key] = None
            if key in ("oi", "prev_oi", "oi_change_pct", "price_change_pct"):
                pool[key] = pd.to_numeric(pool[key], errors="coerce").astype(float)
            pool.loc[0, key] = first.get(key)
        for key in ("instrument_token", "futures_instrument_token", "exchange", "nifty_first_bar_return_pct"):
            pool[key] = first[key]
        rebuilt.append(pool)
        minute_parts.append(minute.loc[minute.ts.dt.date.eq(day), ["ts", "open", "high", "low", "close"]].assign(tradingsymbol=symbol))
        if len(rebuilt) % 50 == 0:
            print(f"[EARLY] reconstruct {day}: {len(rebuilt)} stocks", flush=True)
    features = pd.concat(rebuilt, ignore_index=True)
    old = old.loc[old.tradingsymbol.isin(features.tradingsymbol.unique())].copy()
    # Reconcile every observed overlapping input with the original seal.
    for f in (features, old):
        f["signal_key"] = pd.to_datetime(f.signal_ts, utc=True).astype(str)
    overlap = features.merge(old, on=["tradingsymbol", "signal_key"], suffixes=("_new", "_old"))
    if len(overlap) != len(old) or not len(overlap):
        raise ValueError(f"Incomplete sealed-row parity {day}: {len(overlap)} != {len(old)}")
    checks = {}
    for name in ("close", "ema9", "ema20", "ema50", "price_change_pct", "oi_change_pct", "volume_ratio", "confirmation_close", "v9_1m_volume_ratio"):
        a = pd.to_numeric(overlap[name + "_new"], errors="coerce").to_numpy(float)
        b = pd.to_numeric(overlap[name + "_old"], errors="coerce").to_numpy(float)
        ok = np.isclose(a, b, rtol=1e-7, atol=1e-7, equal_nan=True)
        checks[name] = {"rows": len(a), "mismatches": int((~ok).sum())}
        if not ok.all():
            raise ValueError(f"Reconstruction mismatch {day} {name}: {(~ok).sum()}")
    minutes = pd.concat(minute_parts, ignore_index=True)
    meta = dict(day=str(day), snapshot_manifest=str(manifest), snapshot_sha256=manifest_hash,
                feature_source=str(seed_path), seed_sha256=seed_hash,
                builder_dependencies=dependencies, clocks=CLOCKS,
                reconstructed_stocks=int(features.tradingsymbol.nunique()), rows=len(features),
                reconstruction_checks=checks, problems=problems, excluded_stocks=excluded,
                build_version=BUILD_VERSION, raw_snapshot_members_verified=True,
                ema_seed_policy="Exact09:20sealedEMA; subsequent completed5m closes only")
    features.to_parquet(feature_path, index=False)
    minutes.to_parquet(minute_path, index=False)
    meta.update(features_sha256=sha(feature_path), minutes_sha256=sha(minute_path))
    write_json(meta_path, meta)
    print(f"[EARLY] cached {day}: {meta['reconstructed_stocks']} stocks, {len(features)} rows", flush=True)
    return day, features, minutes, meta


def qualify(frame, trend, min_volume):
    f = frame.copy()
    sign = np.where(f.price_change_pct.ge(0), 1., -1.)
    f["side"] = np.where(sign > 0, "LONG", "SHORT")
    if trend == "aligned":
        directional = np.where(sign > 0, (f.ema9 > f.ema20) & (f.ema20 > f.ema50),
                               (f.ema9 < f.ema20) & (f.ema20 < f.ema50))
    else:
        directional = np.where(sign > 0, (f.ema9 > f.ema20) & (f.close > f.ema50),
                               (f.ema9 < f.ema20) & (f.close < f.ema50))
    f["wick_ratio"] = np.where(sign > 0, f.v9_1m_upper_wick_ratio, f.v9_1m_lower_wick_ratio)
    opening = f.hhmm_int.isin([920, 925])
    nifty_ok = ~opening | ((f.nifty_first_bar_return_pct * sign) >= -.05)
    valid = (f.price_change_pct.abs().ge(.20) & f.volume_ratio.ge(min_volume)
             & f.body_ratio.ge(.30) & f.body_ratio.le(1) & f.wick_ratio.between(0, .60)
             & f.v9_1m_volume_ratio.ge(.50) & directional & nifty_ok
             & ((f.confirmation_close-f.confirmation_open)*sign > 0)
             & ((f.confirmation_close-f.signal_close)*sign > 0)
             & f.confirmation_high.gt(f.confirmation_low)
             & f.v9_exact_confirmation_present.fillna(False))
    # OI is retained as a measured diagnostic, not an entry-direction veto.
    # No future return, named symbol, or best-performing slot is used here.
    out = f.loc[valid].copy()
    out["trigger"] = np.where(out.side.eq("LONG"), out.confirmation_high, out.confirmation_low)
    out["signal_id"] = out.signal_ts.astype(str) + "_" + out.tradingsymbol + "_" + out.side
    out["setup_id"] = pd.to_datetime(out.confirmation_ts).dt.strftime("%H%M") + "_" + out.side
    out["native_stop_pct"] = 1.25
    out["native_target_pct"] = 1.50
    out["picker"] = "max_move"
    out["sid"] = np.arange(len(out))
    return out


def market_entry_replay(orders, paths):
    out = orders.copy()
    records = []
    for row in out.itertuples(index=False):
        path = paths[int(row.sid)]
        if not len(path["open"]):
            records.append(dict(filled=False, exit_reason="UNFILLED")); continue
        price = float(path["open"][0])
        long = row.side == "LONG"
        j, exit_price, reason, stop, event = execution.staged_exit(path, 0, price, long, 1.50)
        entry_label = pd.Timestamp(int(path["timestamp_ns"][0]), tz="UTC").tz_convert("Asia/Kolkata")
        exit_label = pd.Timestamp(int(path["timestamp_ns"][j]), tz="UTC").tz_convert("Asia/Kolkata")
        exit_time = exit_label - pd.Timedelta(minutes=1) if event == "OPEN" else exit_label
        gross = (1 if long else -1)*(exit_price/price-1)*100
        records.append(dict(filled=True, entry_price=price, entry_ts=entry_label,
            entry_open_ts=entry_label-pd.Timedelta(minutes=1), entry_path_index=0,
            exit_ts=exit_time, exit_price=exit_price, exit_reason=reason,
            gross_return_pct=gross, net_return_pct=gross-.05, cost_pct=.05,
            initial_stop_pct=1.25, active_stop_pct_at_exit=stop))
    return pd.concat([out.reset_index(drop=True), pd.DataFrame(records)], axis=1)


def allocate(frame, quota, max_positions=10):
    """Chronological selection, pending orders, fills, exits, and capital.

    Rank uses completed 5m move, then liquidity and symbol. One executed entry
    per symbol/day. Unfilled or capacity-rejected symbols can signal again.
    No future fill/outcome is consulted when choosing a slot's orders.
    max_positions=None removes only the capital/concurrent-position limit.
    """
    if max_positions is not None and (isinstance(max_positions, bool)
            or not isinstance(max_positions, int) or max_positions < 1):
        raise ValueError("max_positions must be a positive integer or None")
    if frame.empty:
        empty = frame.copy()
        empty['portfolio_status'] = pd.Series(dtype=object)
        empty['portfolio_executed'] = pd.Series(dtype=bool)
        empty['portfolio_net_profit_rupees'] = pd.Series(dtype=float)
        return empty
    f = frame.reset_index(drop=True)
    events = []; serial = 0
    def add(ts, priority, kind, payload):
        nonlocal serial
        serial += 1
        heapq.heappush(events, (pd.Timestamp(ts).value, priority, serial, kind, payload))
    for t, group in f.groupby("confirmation_ts", sort=True):
        add(t, 3, "confirm", group.index.tolist())
    pending = {}; traded = set(); active = {}; rows = []
    while events:
        _, _, _, kind, payload = heapq.heappop(events)
        if kind == "exit":
            active.pop(payload, None); continue
        if kind in ("expire", "fill"):
            order_id = payload; r = rows[order_id]; symbol = r["tradingsymbol"]
            if pending.get(symbol) != order_id:
                continue
            pending.pop(symbol, None)
            if kind == "expire":
                r["portfolio_status"] = "UNFILLED_EXPIRED"; continue
            if max_positions is not None and len(active) >= max_positions:
                r["portfolio_status"] = "CAPITAL_REJECTED"; continue
            r["portfolio_status"] = "EXECUTED"
            r["portfolio_executed"] = True
            r["portfolio_net_profit_rupees"] = float(r["net_return_pct"])*5000.
            traded.add(symbol); active[order_id] = symbol
            fill_clock = r.get("entry_open_ts")
            fill_clock = pd.Timestamp(fill_clock) if pd.notna(fill_clock) else pd.Timestamp(r["entry_ts"])
            exit_time = max(pd.Timestamp(r["exit_ts"]), fill_clock+pd.Timedelta(nanoseconds=1))
            add(exit_time, 0, "exit", order_id)
            continue
        candidates = f.loc[payload]
        candidates = candidates.loc[~candidates.tradingsymbol.isin(traded | set(pending))].copy()
        candidates["move_rank"] = candidates.price_change_pct.abs()
        candidates = candidates.sort_values(["move_rank", "traded_value", "tradingsymbol"], ascending=[False, False, True])
        for side, group in candidates.groupby("side", sort=True):
            chosen = group if quota == 0 else group.head(quota)
            for _, candidate in chosen.iterrows():
                record = candidate.to_dict()
                record.update(portfolio_status="PENDING", portfolio_executed=False, portfolio_net_profit_rupees=0.)
                order_id = len(rows); rows.append(record)
                pending[record["tradingsymbol"]] = order_id
                if bool(record.get("filled", False)):
                    fill_clock = record.get("entry_open_ts")
                    add(fill_clock if pd.notna(fill_clock) else record["entry_ts"], 1, "fill", order_id)
                else:
                    add(pd.Timestamp(record["confirmation_ts"])+pd.Timedelta(minutes=10), 2, "expire", order_id)
    return pd.DataFrame(rows)


def metrics(frame, label, day):
    taken = frame.loc[frame.portfolio_executed.eq(True)] if len(frame) else frame
    pnl = taken.portfolio_net_profit_rupees if len(taken) else pd.Series(dtype=float)
    return dict(variant=label, day=str(day), orders=len(frame), trades=len(taken),
                wins=int((pnl > 0).sum()), losses=int((pnl < 0).sum()),
                win_rate_pct=float((pnl > 0).mean()*100) if len(pnl) else 0.,
                net_pnl_rupees=float(pnl.sum()), portfolio_return_pct=float(pnl.sum()/10000),
                requested_trades=int(sum(REQUESTED.get(r.tradingsymbol)==r.side for r in taken.itertuples())) if str(day)=="2026-10-23" else None)


def evaluate_day(day, features, minute_frame, output):
    minutes = {s: f.sort_values("ts").copy() for s, f in minute_frame.groupby("tradingsymbol")}
    rows = []; books = []
    baseline = contract.select_orders(features)
    # Compare actual selections, not only feature values, against the saved
    # full-history run whose profile is being used as the control.
    saved = pd.read_csv(HISTORY / "selected_orders.csv", usecols=[
        "day", "tradingsymbol", "setup_id", "side", "trigger", "native_target_pct"])
    saved = saved.loc[saved.day.eq(str(day))]
    keys = ["tradingsymbol", "setup_id", "side"]
    left = baseline[keys + ["trigger", "native_target_pct"]].sort_values(keys).reset_index(drop=True)
    right = saved[keys + ["trigger", "native_target_pct"]].sort_values(keys).reset_index(drop=True)
    pd.testing.assert_frame_equal(left, right, check_dtype=False, check_exact=False, rtol=1e-7, atol=1e-7)
    paths = source.selected_paths(baseline, minutes, day)
    for label, fixed in [("G3_V14_original", False), ("G3_V14_same_1p5_target", True)]:
        orders = baseline.copy()
        if fixed:
            orders["native_target_pct"] = 1.5
        ledger, _ = replay.simulate(orders, paths, [day])
        ledger["variant"] = label
        rows.append(metrics(ledger, label, day)); books.append(ledger)
    # Matched exits and duplicate/capital treatment isolate selection effects
    # more closely than the original G3 book, which permits repeat symbols.
    matched = baseline.copy(); matched["native_target_pct"] = 1.5
    matched = execution.simulate_staged(matched, paths, cost_bps=5, max_entry_delay_minutes=10)
    ledger = allocate(matched, 0)
    label = "G3_V14_same_1p5_target_one_symbol"
    ledger["variant"] = label
    rows.append(metrics(ledger, label, day)); books.append(ledger)
    for trend in ("aligned", "turn"):
        for volume in (.8, .4):
            orders = qualify(features, trend, volume)
            paths = source.selected_paths(orders, minutes, day)
            execution.g.v9.validate_paths(orders, paths)
            for mode in ("breakout", "next_open"):
                simulated = (execution.simulate_staged(orders, paths, cost_bps=5., max_entry_delay_minutes=10)
                             if mode == "breakout" else market_entry_replay(orders, paths))
                for quota in (1, 2, 0):
                    label = f"{trend}_vol{volume:g}_{mode}_q{quota or 'all'}"
                    ledger = allocate(simulated, quota)
                    ledger["variant"] = label
                    rows.append(metrics(ledger, label, day)); books.append(ledger)
    combined = pd.concat(books, ignore_index=True)
    combined.to_csv(output / f"orders_{day}.csv", index=False)
    return rows, combined.loc[combined.tradingsymbol.isin(REQUESTED)].copy()


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--output", type=Path, default=OUTPUT)
    p.add_argument("--workers", type=int, default=2)
    p.add_argument("--today-only", action="store_true")
    p.add_argument("--report-only", action="store_true", help="Rebuild the report from this study's saved results")
    args = p.parse_args(); args.output.mkdir(parents=True, exist_ok=True)
    evidence = json.loads((HISTORY / "summary.json").read_text())["daily_path_evidence"]
    if args.today_only:
        evidence = [x for x in evidence if Path(x["run_dir"]).parent.name == "2026-10-23"]
    config = dict(scope="RESEARCH_ONLY", execution_authority=False, live_files_changed=False,
        baseline_version=contract.STRATEGY_VERSION, baseline_fingerprint=contract.strategy_fingerprint(),
        signal_clocks=CLOCKS, move_min_pct=.20, volume5m_min=[.8, .4], confirmation_volume_min=.50,
        confirmation_body_min=.30, confirmation_wick_max=.60, oi_policy="DIAGNOSTIC_ONLY",
        opening_nifty_opposition_veto_pct=.05, initial_stop_pct=1.25, tightened_stop_pct=1.,
        tighten_after_minutes=120, target_pct=1.5, cost_bps=5., entry_expiry_minutes=10,
        capital_per_trade=100000, leverage=5, portfolio_capital=1000000,
        quota_per_side_per_slot=[1, 2, "ALL"], max_executed_trades_per_symbol_per_day=1,
        ranking="ABS_5M_MOVE_THEN_LIQUIDITY_THEN_SYMBOL", trials=24,
        entry_timestamp_convention="BREAKOUT_CANDLE_END; NEXT_OPEN_ALLOCATED_AT_OPEN_ECONOMIC_TIMESTAMP",
        execution_limitations=["Minute OHLC cannot establish crossing order among simultaneous triggers",
            "Capital released at an exit minute-end is available to fills at the same timestamp",
            "Pending signals reserve no capital; a trigger-time gate enforces the ten-position limit",
            "Idealized fills, fixed 5bps costs, no additional latency/slippage or share rounding"],
        evaluation_note="October23 requested-stock diagnosis; prior seven days are retrospective checks, not untouched holdout",
        source_history=str(HISTORY), code_sha256=sha(__file__))
    write_json(args.output / "research_config.json", config)
    all_metrics=[]; target_books=[]; provenance=[]
    if args.report_only:
        all_metrics = pd.read_csv(args.output / "daywise_comparison.csv").to_dict("records")
        provenance = json.loads((args.output / "provenance.json").read_text())
    else:
        with ThreadPoolExecutor(max_workers=args.workers) as executor:
            jobs = [executor.submit(build_day, x, args.output) for x in evidence]
            for job in as_completed(jobs):
                day, features, minutes, meta = job.result()
                print(f"[EARLY] evaluate {day}", flush=True)
                rows, targets = evaluate_day(day, features, minutes, args.output)
                all_metrics.extend(rows); target_books.append(targets); provenance.append(meta)
                pd.DataFrame(all_metrics).to_csv(args.output / "daywise_comparison.csv", index=False)
    daily = pd.DataFrame(all_metrics).sort_values(["variant", "day"])
    control = pd.read_csv(HISTORY / "daily_results.csv")
    check = daily.loc[daily.variant.eq("G3_V14_original")].merge(control, on="day", suffixes=("_new", "_saved"), validate="one_to_one")
    assert len(check) == daily.day.nunique(), "Missing saved baseline control day"
    for name in ("trades", "wins", "losses"):
        assert check[name + "_new"].eq(check[name + "_saved"]).all(), name
    assert np.allclose(check.net_pnl_rupees, check.net_pnl, rtol=1e-9, atol=1e-6), "Baseline PnL mismatch"
    write_json(args.output / "validation.json", dict(baseline_control_days=len(check),
        baseline_pnl_max_abs_difference=float((check.net_pnl_rupees-check.net_pnl).abs().max()),
        baseline_selection_parity="Exact keys; trigger/target tolerance1e-7 checked during replay",
        unit_tests="tests/test_g3_early_entry_study.py: 7 passed"))
    daily.to_csv(args.output / "daywise_comparison.csv", index=False)
    if target_books:
        pd.concat(target_books).to_csv(args.output / "requested_stock_comparison.csv", index=False)
    summary=[]
    for label, book in daily.groupby("variant"):
        for period, frame in [("all_available", book), ("prior_days", book[book.day.lt("2026-10-23")]), ("2026-10-23", book[book.day.eq("2026-10-23")])]:
            if frame.empty: continue
            trades=int(frame.trades.sum()); wins=int(frame.wins.sum())
            summary.append(dict(variant=label, period=period, days=len(frame), trades=trades,
                wins=wins, losses=int(frame.losses.sum()), win_rate_pct=wins/trades*100 if trades else 0,
                net_pnl_rupees=float(frame.net_pnl_rupees.sum()), losing_days=int(frame.net_pnl_rupees.lt(0).sum()),
                worst_day_rupees=float(frame.net_pnl_rupees.min()), requested_trades=int(frame.requested_trades.fillna(0).sum())))
    summary=pd.DataFrame(summary)
    summary.to_csv(args.output / "summary.csv", index=False)
    write_json(args.output / "provenance.json", provenance)
    body = '<html><meta charset="utf-8"><title>G3 early entry research</title><style>body{font:14px system-ui;margin:30px}table{border-collapse:collapse;margin-bottom:30px}th,td{border:1px solid #ccc;padding:5px;text-align:right}th:first-child,td:first-child{text-align:left}</style><h1>G3 early entry research, 23 October 2026</h1>'
    body += '<p>Twenty-four generic selection and execution variants across all dated-universe stocks. Research only. Earlier days are retrospective robustness checks. Experimental exits use a fixed 1.5% target, 1.25% stop and 5bps cost. Original G3, G3 with the same target, and G3 with matched one-symbol-per-day allocation are shown. Experiments allow ten concurrent positions and one executed trade per symbol/day; original G3 permits repeated symbols. Breakout timestamps label minute ends; next-open allocation uses economic open time one minute earlier. Next-open assumes zero latency.</p>'
    body += '<p>Capital: Rs 1,000,000 portfolio; Rs 100,000 margin and Rs 500,000 exposure per trade. Source controls reproduce the saved G3 V14 selections and daily P&amp;L. This is not a reproduction of the older V12 configuration used live earlier on 23 October.</p>'
    body += '<h2>Conclusion</h2><p>Earlier entries exist for the requested stocks, but broadly relaxing the filters is not supported as a replacement for G3. Compare the seven earlier sessions separately from the requested day. The named-stock watchlist uses hindsight and is not evidence of automatic selection performance. Live and paper profiles remain unchanged.</p>'
    individual_path = args.output.parent / 'individual/earliest_fills.csv'
    if individual_path.exists():
        individual = pd.read_csv(individual_path)
        six = individual.loc[individual.variant.eq('reversal_vol040_strict_breakout')].copy()
        six['entry_interval_IST'] = pd.to_datetime(six.entry_bar_start_ts).dt.strftime('%H:%M') + '-' + pd.to_datetime(six.entry_bar_end_ts).dt.strftime('%H:%M')
        six['net_pnl_rupees'] = six.net_return_pct*5000
        body += '<h2>Six requested stocks: earliest eligible individual fills</h2><p>These stocks were named after observing the session. This is a hypothetical watchlist result, not the automatic whole-universe portfolio result. Each earliest fill is chosen by time, never by profit. ETERNAL uses the early-turn EMA branch. Peak concurrent positions: four.</p>'
        body += six[['tradingsymbol','side','entry_interval_IST','entry_price','oi_change_pct','volume_ratio','exit_reason','net_return_pct','net_pnl_rupees']].round(4).to_html(index=False)
        body += '<p>Six-name net total Rs 43,500 at this exposure, after Rs 1,500 modeled costs. Full-universe turn_vol0.4_breakout_qall executes five of these; DELHIVERY is capacity-rejected. Immediate next-open SWIGGY loses 1.30% while its later confirmed breakout earns 1.45%.</p>'
    selected = ['G3_V14_original','G3_V14_same_1p5_target_one_symbol','turn_vol0.4_breakout_qall','aligned_vol0.8_breakout_qall','turn_vol0.4_breakout_q1']
    body += '<h2>Main comparison</h2>'+summary.loc[summary.variant.isin(selected)].round(2).to_html(index=False)
    body += '<h2>Daywise net P&amp;L (rupees)</h2>'+daily.loc[daily.variant.isin(selected)].pivot(index='day', columns='variant', values='net_pnl_rupees').round(2).to_html()
    body += '<h2>Rules tested</h2><p>Scan every 5 minutes from 09:20 to 13:30. OI is diagnostic, not a veto. Absolute 5-minute move at least 0.20%; volume ratio at least 0.40 or 0.80. Directional 1-minute confirmation: body at least 0.30, adverse wick no more than 0.60, volume ratio at least 0.50, close beyond both its open and the signal close. Aligned mode requires EMA9/20/50 in order. Turn mode requires EMA9 above EMA20 and price above EMA50 for longs, reversed for shorts. Opening Nifty opposition veto 0.05%. Breakout entries require a strict confirmation high/low break within 10 minutes; unfilled orders expire. Quotas 1, 2, or all per side, ranked by absolute move then liquidity. One executed trade per stock per day. Stop starts at 1.25%, tightens to 1% after 120 minutes; target 1.5%.</p>'
    body += '<h2>Execution and research limitations</h2><ul>'+''.join('<li>'+html.escape(item)+'</li>' for item in config['execution_limitations'])+'</ul><p>Same-minute trigger ordering cannot be proven without ticks. In the 23 October broad-rule simulation, ADANIENSOL takes the last capital slot before DELHIVERY by deterministic rank. Trigger-time rejection requires a capacity-controlled execution engine, not submission of every pending stop order. These are 24 retrospective experiments, not an untouched validation set. No live or paper profile was changed.</p>'
    body += '<details><summary>All variant summaries</summary>'+summary.round(2).to_html(index=False)+'</details>'
    body += '<details><summary>All daywise results</summary>'+daily.round(2).to_html(index=False)+'</details>'
    body += '<p>CSV files in this folder contain all chosen orders and requested-stock outcomes. Missing or failed signals are not converted into profitable entries.</p></html>'
    (args.output / "report.html").write_text(body, encoding="utf-8")
    print(summary[summary.period.eq("2026-10-23")].sort_values("net_pnl_rupees", ascending=False).to_string(index=False))


if __name__ == "__main__":
    main()
