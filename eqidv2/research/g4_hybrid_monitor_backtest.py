"""Isolated G4 entry-quality and completed-minute price/volume experiments.

Run with ``python -m research.g4_hybrid_monitor_backtest``. All hypotheses are
frozen in the saved config before any policy result is evaluated. No strategy,
dashboard, paper/live setting, or latest-result pointer is written.
"""
from __future__ import annotations

import argparse
from datetime import datetime
import html
import json
from pathlib import Path
import secrets
import shutil

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

from research import g4_hybrid_features as features
from research import g4_hybrid_monitor_rules as rules
from research import g4_trade_monitor_backtest as previous
from research import g4_failed_breakout_research as sealed
from research import g4_bounded_exit_backtest as bounded
from research import g4_g3_mirror_backtest as base
from research import g3_trade_monitor_backtest as exit_helper

VERSION = "V13-V10-G-4-HYBRID-PRICE-VOLUME-RESEARCH-V1"
DEFAULT_MONITOR = base.G4_ROOT / "research/20261016T153442_one_minute_trade_monitor_v1"
DEFAULT_MANIFEST_HASH = "5fe22bca247160a8f0d69589dfd91d62025391f4f384b6e8d9bf8cb7cd1b7cae"


def configuration():
    return dict(variant_id=VERSION, rules_version=rules.VERSION, scope="BACKTEST_ONLY",
        execution_authority=False, parameters=rules.PARAMETERS, policies=rules.POLICIES,
        entry_ranking="Keep selected G4 orders; veto never substitutes another candidate",
        volume="Incremental equity volume; same-HH:MM preceding20 sessions minimum5",
        indicators="Raw OHLCV recomputed; session HLC3 VWAP; EMA20; simple TR ATR14",
        entry_timing="Confirmation close only; never use entry candle to veto an existing fill",
        decision_timing="Completed full minutes after the native entry-bar end",
        execution="Following observed minute open, capital reserved through its end",
        native_precedence="Native exit at or before the advisory open wins; later extrema do not",
        native_stop_pct=1.2, tighten_after_minutes=120, tightened_stop_pct=1.0,
        native_targets="Original G4 V12 targets, 0.60 through 2.00 percent",
        capital_per_entry_rs=100000., leverage=5., portfolio_capital_rs=1000000.,
        round_trip_cost_bps=5., development_sessions=35, diagnostic_sessions=15,
        diagnostic_label="Retrospective chronological comparison; history already inspected",
        selection_rule="On first35 sessions: loss count and loss amount improve, retain >=90% baseline net; choose greatest loss-amount reduction, then net",
        selection_minimum_net_retention=.90, bootstrap_repetitions=3000, bootstrap_seed=271828,
        parameter_search=False)


def load_paths(path):
    result = {}
    with np.load(path, allow_pickle=False) as archive:
        for key in archive.files:
            sid, field = key.split("_", 1)
            result.setdefault(int(sid), {})[field] = archive[key]
    return result


def _feature_contexts(native, paths, monitor_summary, source_summary):
    """Recover OHLCV from the exact original bytes, not today's price files."""
    g3 = Path(source_summary["current_g3_source_run"])
    if base.sha256(g3 / "manifest.json") != source_summary["current_g3_manifest_sha256"]:
        raise ValueError("Pinned G3 source manifest changed")
    resolver = previous.CurrentSealedPrices(base.read(g3 / "summary.json"), source_summary["price_sources"])
    groups = {}
    for row in native.to_dict("records"):
        day = pd.Timestamp(row["day"]).date()
        directory = resolver.snapshots.get(day)
        original = ((directory / "equity_1m") if directory else base.source.DataRoots().equity_1m) / f'{row["tradingsymbol"]}_stocks_indicators_1min.parquet'
        key = str(original.resolve())
        proof = monitor_summary["price_sources"].get(key)
        if proof is None or not proof["exact_original_bytes"]:
            raise ValueError("No exact sealed price proof for " + key)
        groups.setdefault((proof["resolved_path"], proof["sha256"]), []).append(row)
    contexts, evidence = {}, []
    for count, ((filename, digest), rows) in enumerate(groups.items(), 1):
        path = Path(filename)
        if base.sha256(path) != digest:
            raise ValueError("Sealed OHLCV input changed: " + filename)
        available = set(pq.ParquetFile(path).schema_arrow.names)
        columns = ["date", "open", "high", "low", "close", "volume"]
        columns += [name for name in ("gap_filled", "opening_snapshot", "provisional_stale") if name in available]
        raw = pd.read_parquet(path, columns=columns)
        raw["ts"] = base.source.hybrid._to_ist(raw.date).dt.as_unit("ns")
        last_day = max(pd.Timestamp(row["day"]).normalize() for row in rows)
        raw = raw.loc[raw.ts.dt.tz_localize(None).dt.normalize().le(last_day)].drop(columns="date")
        enriched = features.build_features(raw)
        for row in rows:
            stamp = pd.Timestamp(row["confirmation_ts"]).tz_convert("Asia/Kolkata")
            day = stamp.normalize()
            session = enriched.loc[enriched.ts.dt.normalize().eq(day)].reset_index(drop=True)
            relevant = session.loc[session.ts.le(day + pd.Timedelta(hours=15, minutes=15))]
            expected = pd.date_range(day + pd.Timedelta(hours=9, minutes=16), day + pd.Timedelta(hours=15, minutes=15), freq="min")
            if not pd.DatetimeIndex(relevant.ts).equals(expected):
                raise ValueError(f'Incomplete OHLCV session: {row["day"]} {row["tradingsymbol"]}')
            for flag in ("gap_filled", "opening_snapshot", "provisional_stale"):
                if flag in relevant:
                    words = relevant[flag].astype(str).str.lower().str.strip()
                    numeric = pd.to_numeric(relevant[flag], errors="coerce")
                    if ((numeric.notna() & numeric.ne(0)) | words.isin(["true", "yes", "on"])).any():
                        raise ValueError("Flagged target-session OHLCV")
            confirmation = session.loc[session.ts.eq(stamp)]
            if len(confirmation) != 1 or any(not np.isclose(float(confirmation.iloc[0][name]), float(row["confirmation_" + name]), rtol=0, atol=.001) for name in ("open", "high", "low", "close")):
                raise ValueError("Confirmation differs from pinned order")
            sid = int(row["sid"])
            execution = session.loc[session.ts.gt(stamp) & session.ts.le(expected[-1])]
            native_path = paths[sid]
            if not np.array_equal(execution.ts.astype("int64").to_numpy(), native_path["timestamp_ns"]):
                raise ValueError("Execution and volume timestamps differ")
            if any(not np.array_equal(execution[name].to_numpy(float), native_path[name]) for name in ("open", "high", "low", "close")):
                raise ValueError("Execution and volume OHLC differ")
            contexts[sid] = session
        if base.sha256(path) != digest:
            raise ValueError("OHLCV source changed while reading")
        evidence.append(dict(resolved_path=str(path.resolve()), sha256=digest, orders=len(rows), exact_original_bytes=True))
        if count % 25 == 0 or count == len(groups):
            print(f'Exact OHLCV features: {count}/{len(groups)} files, {len(contexts)}/{len(native)} orders', flush=True)
    return contexts, evidence


def _first_events(native, contexts, paths):
    entries, observations = [], []
    for row in native.to_dict("records"):
        sid = int(row["sid"])
        entry = rules.entry_assessment(row, contexts[sid])
        entries.append(dict(sid=sid, day=row["day"], setup_id=row["setup_id"], tradingsymbol=row["tradingsymbol"], side=row["side"], **entry))
        states = rules.trade_observations(row, contexts[sid], paths[sid])
        for item in states:
            observations.append({**item, "flags": json.dumps(item["flags"])})
    observations = pd.DataFrame(observations)
    return pd.DataFrame(entries), observations, events_from_observations(native, paths, observations)


def events_from_observations(native, paths, observations):
    """Reconstruct next-open orders from a verified causal feature audit."""
    rows = native.set_index("sid")
    events = {int(sid): {} for sid in rows.index}
    for item in observations.to_dict("records"):
        sid = int(item["sid"])
        row = rows.loc[sid]
        flags = json.loads(item["flags"])
        decision_index = int(item["path_index"])
        decision = pd.Timestamp(item["decision_at_ist"])
        if (not bool(row.filled) or decision_index <= int(row.entry_path_index) or
                decision.value != int(paths[sid]["timestamp_ns"][decision_index]) or
                decision - pd.Timedelta(minutes=1) < pd.Timestamp(row.entry_ts) or
                decision >= pd.Timestamp(row.exit_ts)):
            raise ValueError("Invalid cached observation timing")
        if not flags:
            continue
        index = decision_index + 1
        end = pd.Timestamp(int(paths[sid]["timestamp_ns"][index]), tz="UTC").tz_convert(base.production.IST)
        open_at = end - pd.Timedelta(minutes=1)
        if pd.Timestamp(row.exit_ts) <= open_at:
            continue
        event = dict(execution_path_index=index, modeled_execution_at_ist=end.isoformat(),
            next_bar_open_at_ist=open_at.isoformat(), decision_at_ist=decision.isoformat(),
            next_bar_open_price=float(paths[sid]["open"][index]), flags=json.dumps(flags))
        for flag in flags:
            events[sid].setdefault(flag, event)
    return events


def apply_policies(native, paths, entries, events):
    books = {}
    entry_map = entries.set_index("sid").entry_veto.to_dict()
    for name, policy in rules.POLICIES.items():
        work = native.copy()
        work["monitor_exit"] = False
        work["monitor_flags"] = "[]"
        work["monitor_decision_at_ist"] = ""
        work["monitor_open_observed_at_ist"] = ""
        work["entry_vetoed"] = work.sid.map(entry_map).astype(bool) & policy["entry_veto"]
        for index, row in work.iterrows():
            if bool(row.entry_vetoed):
                work.loc[index, ["filled", "position_open"]] = False
                work.loc[index, ["entry_ts", "exit_ts"]] = pd.NaT
                work.loc[index, ["entry_price", "entry_path_index", "exit_price", "exit_path_index", "gross_return_pct", "net_return_pct", "cost_pct"]] = np.nan
                work.loc[index, "exit_reason"] = "ENTRY_VETO_EXHAUSTION"
                for field in ("stop_hit", "target_hit", "first_target_hit", "runner_target_hit", "same_bar_ambiguous", "exit_gap_through", "entry_gap_through", "protection_applied"):
                    if field in work:
                        work.loc[index, field] = False
                for field in ("holding_minutes", "mfe_pct", "mae_pct", "active_stop_pct_at_exit", "active_stop_price", "entry_overshoot_bps", "exit_gap_bps", "protection_armed_path_index", "protection_effective_path_index"):
                    if field in work:
                        work.loc[index, field] = np.nan
                for field in ("exit_event", "exit_execution_ts", "exit_bar_end_ts"):
                    if field in work:
                        work.loc[index, field] = None
                continue
            choices = [event for flag, event in events[int(row.sid)].items() if flag in policy["exit_flags"]]
            if not choices:
                continue
            event = min(choices, key=lambda value: value["execution_path_index"])
            exit_helper._apply_monitor_exit(work, index, event, paths[int(row.sid)], name)
            actual_open = pd.Timestamp(event["next_bar_open_at_ist"])
            active = 1.0 if actual_open >= pd.Timestamp(row.entry_ts) + pd.Timedelta(minutes=120) else 1.2
            sign = 1 if row.side == "LONG" else -1
            work.loc[index, "active_stop_pct_at_exit"] = active
            work.loc[index, "active_stop_price"] = float(row.entry_price) * (1 - sign * active / 100)
        book = previous.allocate(work, name)
        book["strategy_version"] = VERSION
        book["strategy_fingerprint"] = base.fingerprint(dict(configuration=configuration(), policy=name))
        books[name] = book
    sealed.parity(books["baseline"], native)
    return books


def comparisons(books, days):
    metrics, daily, changes = previous.compare(books, days)
    baseline = books["baseline"].set_index("sid")
    extra = []
    periods = dict(all=days, development35=days[:35], evaluation15=days[35:50], later_extensions=days[50:], last_day=[days[-1]])
    for name, book in books.items():
        current = book.set_index("sid").reindex(baseline.index)
        changes.loc[changes.policy.eq(name), "entry_vetoed"] = changes.loc[changes.policy.eq(name), "sid"].map(current.entry_vetoed).to_numpy()
        for period, cohort in periods.items():
            if not cohort:
                continue
            selected = baseline.day.isin(cohort)
            b = baseline.loc[selected]
            p = current.loc[selected]
            removed = b.portfolio_executed & ~p.portfolio_executed
            both = b.portfolio_executed & p.portfolio_executed
            base_stats = bounded.stats(books["baseline"], cohort)
            policy_stats = bounded.stats(book, cohort)
            extra.append(dict(policy=name, period=period,
                vetoed_orders=int(p.entry_vetoed.sum()),
                removed_baseline_winners=int((removed & b.portfolio_net_profit_rupees.gt(0)).sum()),
                removed_baseline_losers=int((removed & b.portfolio_net_profit_rupees.lt(0)).sum()),
                baseline_loser_turned_winner=int((both & b.portfolio_net_profit_rupees.lt(0) & p.portfolio_net_profit_rupees.gt(0)).sum()),
                losing_trades_reduced=base_stats["losses"] - policy_stats["losses"],
                loss_amount_reduced_rs=base_stats["negative_trade_sum_abs_rs"] - policy_stats["negative_trade_sum_abs_rs"],
                profit_retained_pct=100 * policy_stats["net_pnl_rs"] / base_stats["net_pnl_rs"] if base_stats["net_pnl_rs"] > 0 else np.nan))
    metrics = metrics.merge(pd.DataFrame(extra), on=["policy", "period"], validate="one_to_one")
    # Sample sessions rather than minute observations or correlated individual trades.
    rng = np.random.default_rng(configuration()["bootstrap_seed"])
    indices = rng.integers(0, len(days), size=(configuration()["bootstrap_repetitions"], len(days)))
    bday = daily.loc[daily.policy.eq("baseline")].set_index("day").reindex(days).net_pnl_rs.to_numpy(float)
    for name in books:
        pday = daily.loc[daily.policy.eq(name)].set_index("day").reindex(days).net_pnl_rs.to_numpy(float)
        deltas = (pday - bday)[indices].sum(axis=1)
        lower, upper = np.quantile(deltas, [.025, .975])
        mask = metrics.period.eq("all") & metrics.policy.eq(name)
        metrics.loc[mask, "paired_session_bootstrap_net_delta_low_rs"] = lower
        metrics.loc[mask, "paired_session_bootstrap_net_delta_high_rs"] = upper
    development = metrics.loc[metrics.period.eq("development35") & metrics.policy.ne("baseline")]
    qualifying = development.loc[development.loss_amount_reduced_rs.gt(0) & development.losing_trades_reduced.ge(0) & development.profit_retained_pct.ge(90)]
    chosen = None if qualifying.empty else qualifying.sort_values(["loss_amount_reduced_rs", "net_pnl_rs"], ascending=False).iloc[0].policy
    return metrics, daily, changes, chosen


def _report(output, summary, metrics, daily, entries, observations):
    all_rows = metrics.loc[metrics.period.eq("all")]
    selected = summary["development_selected_policy"]
    columns = ["period", "policy", "trades", "losses", "losing_days", "negative_trade_sum_abs_rs", "net_pnl_rs", "net_change_rs", "loss_amount_reduced_rs", "profit_retained_pct", "daily_close_drawdown_rs", "monitor_exits", "vetoed_orders", "baseline_winners_turned_losses", "removed_baseline_winners", "removed_baseline_losers"]
    css = 'body{font:15px/1.55 system-ui;margin:0;color:#17283b;background:#eef2f7}main{max-width:1550px;margin:auto;padding:32px}header{background:#102e49;color:white;padding:25px;border-radius:12px}h1{margin:0 0 10px}.box{background:white;padding:22px;margin:20px 0;border-radius:12px}.scroll{overflow:auto}table{border-collapse:collapse;width:100%;font-size:13px}th,td{padding:9px;border-bottom:1px solid #d8e3eb;text-align:left;white-space:nowrap}th{background:#e8f0f7}a{color:#135e9f}code{background:#e8edf2;padding:2px 5px}'
    doc = f'<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>G4 hybrid price and volume research</title><style>{css}</style></head><body><main>'
    doc += f'<header><h1>G4: entry quality and one-minute monitoring</h1><p>{summary["first_date"]} to {summary["last_date"]} · {summary["sessions"]} completed sessions · 51 setups · 9 fixed policies</p><p>Research simulation. Baseline entries/exits reproduced exactly. No strategy settings or result pointers changed.</p></header>'
    doc += '<section class="box"><h2>All-session results</h2><p>Loss amount means the absolute sum of negative net trade P&amp;L, not total portfolio loss. Profit retention compares net P&amp;L with the matched original G4 baseline and is omitted when baseline net P&amp;L is zero or negative.</p><div class="scroll">' + all_rows[columns].to_html(index=False, border=0, float_format=lambda x:f'{x:,.2f}') + '</div></section>'
    doc += f'<section class="box"><h2>Chronological diagnostics</h2><p>Policy selected on first 35 sessions using the declared loss-reduction / 90% net-retention rule: <strong>{html.escape(str(selected or "None qualified"))}</strong>. The remaining 15+2 sessions were already inspected in earlier strategy research; these are retrospective checks, not untouched holdout evidence.</p><div class="scroll">' + metrics[columns].to_html(index=False, border=0, float_format=lambda x:f'{x:,.2f}') + '</div></section>'
    doc += '<section class="box"><h2>Fixed hypotheses</h2><p>Entry veto: extension at least 3 ATR with rejection wick, or weak time-adjusted volume and weak price progress. Structure failure: two closes beyond the frozen five-minute pre-entry boundary, at least 0.30% adverse to entry, below the directional EMA20 and adverse three-minute movement of at least one current ATR. Volume confirmation: three-minute mean same-time relative volume at least 1.25. Context confirmation: adverse VWAP position and EMA slope. Profit protection: completed-close profit reaches the greater of 0.60% or half the target, then gives back the greater of 0.30 percentage points or two entry ATRs, with persistent adverse EMA/momentum and volume or VWAP context. Its decision must still show at least 0.10% gross profit.</p><p>All decisions observe completed bars and execute at the following observed open. The native stop/target can execute first. An adverse move is a probabilistic warning; these states are not ground-truth reversal labels.</p></section>'
    doc += '<section class="box"><h2>Data and limitations</h2><ul>' + ''.join('<li>'+html.escape(item)+'</li>' for item in summary["limitations"]) + '</ul><p>Entry checks: '+str(len(entries))+'; entry veto flags: '+str(int(entries.entry_veto.sum()))+'; one-minute observations: '+str(len(observations))+'.</p></section>'
    doc += '<section class="box"><h2>Evidence</h2><p><a href="comparison.csv">Policy results</a> · <a href="daily_comparison.csv">Daywise comparison</a> · <a href="trade_changes.csv">Paired trade changes</a> · <a href="entry_checks.csv">Entry checks</a> · <a href="observations.csv">Minute states</a> · <a href="config.json">Frozen parameters</a> · <a href="summary.json">Summary and provenance</a></p></section></main></body></html>'
    (output / "report.html").write_text(doc, encoding="utf-8")


def run(output, source=DEFAULT_MONITOR, expected_hash=DEFAULT_MANIFEST_HASH, *, reuse_feature_run=None):
    output, source = Path(output).resolve(), Path(source).resolve()
    if output.exists() or not output.is_relative_to((base.G4_ROOT / "research").resolve()):
        raise ValueError("A fresh isolated G4 research directory is required")
    if base.sha256(source / "manifest.json") != expected_hash:
        raise ValueError("Pinned monitor manifest changed")
    monitor_summary = sealed.package(source)
    source_run = Path(monitor_summary["source_run"])
    if base.sha256(source_run / "manifest.json") != monitor_summary["source_manifest_sha256"]:
        raise ValueError("Pinned current G4 manifest changed")
    source_summary = sealed.package(source_run)
    if monitor_summary["source_variant_id"] != "V13-V10-G-4-BOUNDED-EXIT-51-SETUPS-V12" or monitor_summary["active_setups"] != 51:
        raise ValueError("Expected the current complete 51-setup G4 baseline")
    code_modules = [__file__, rules.__file__, features.__file__, previous.__file__, sealed.__file__, bounded.__file__, base.__file__, exit_helper.__file__, sealed.simulate_exit.__code__.co_filename,
        bounded.history.simulator.g.v9.__file__, bounded.history.simulator.g.v9.v5.__file__, bounded.history.simulator.g.v9.v6.__file__]
    code = {str(Path(name).resolve()): base.sha256(Path(name)) for name in code_modules}
    pointer_paths = [base.G4_ROOT / "latest/bounded_exit.json", base.G4_ROOT / "latest/g3_mirror.json"]
    pointer_hashes = {str(path): base.sha256(path) for path in pointer_paths if path.exists()}
    output.mkdir(parents=True)
    (output / "code").mkdir()
    for filename in code:
        shutil.copyfile(filename, output / "code" / Path(filename).name)
    (output / "config.json").write_text(json.dumps(configuration(), indent=2) + "\n", encoding="utf-8")
    config_hash = base.sha256(output / "config.json")
    paths = load_paths(source / "historical_paths.npz")
    original = base.read_frozen_csv(source_run / "g4_bounded_exit/portfolio_trades.csv")
    native = sealed.replay(original, paths, "current_g4")
    sealed.parity(native, original)
    sealed.parity(native, base.read_frozen_csv(source / "baseline/portfolio_trades.csv"))
    print("Native G4 entries, exits, costs and allocation reproduced exactly", flush=True)
    feature_cache_proof = None
    if reuse_feature_run is None:
        contexts, proof = _feature_contexts(native, paths, monitor_summary, source_summary)
        entries, observations, events = _first_events(native, contexts, paths)
    else:
        cache = Path(reuse_feature_run).resolve()
        cache_summary = sealed.package(cache)
        if (base.read(cache / "config.json") != configuration() or
                cache_summary["source_manifest_sha256"] != monitor_summary["source_manifest_sha256"] or
                cache_summary["historical_paths_sha256"] != base.sha256(source / "historical_paths.npz") or
                any(cache_summary["code"].get(str(Path(module).resolve())) != base.sha256(Path(module)) for module in (rules.__file__, features.__file__))):
            raise ValueError("Cached features have different rules, sources or parameters")
        entries = base.read_frozen_csv(cache / "entry_checks.csv")
        observations = base.read_frozen_csv(cache / "observations.csv")
        if entries.sid.duplicated().any() or set(entries.sid) != set(native.sid):
            raise ValueError("Cached entry checks do not match the selected candidates")
        events = events_from_observations(native, paths, observations)
        proof = cache_summary["price_sources"]
        feature_cache_proof = dict(run=str(cache), manifest_sha256=base.sha256(cache / "manifest.json"),
            reuse="Exact frozen feature audits only; all entries, exits, allocation and metrics replayed")
        print("Reused verified feature audits; recomputing all policies and allocation", flush=True)
    print(f'Entry flags: {int(entries.entry_veto.sum())}; minute observations: {len(observations)}', flush=True)
    books = apply_policies(native, paths, entries, events)
    days = list(monitor_summary["days"])
    metrics, daily, changes, chosen = comparisons(books, days)
    if reuse_feature_run is not None:
        before = base.read_frozen_csv(Path(reuse_feature_run) / "comparison.csv").set_index(["period", "policy"]).sort_index()
        after = metrics.set_index(["period", "policy"]).sort_index()
        numerical = before.select_dtypes(include="number").columns.difference(["profit_retained_pct"])
        if not np.allclose(before[numerical], after[numerical], atol=1e-6, rtol=0, equal_nan=True):
            raise ValueError("Replayed policy results differ from the frozen feature run")
    for name, book in books.items():
        folder = output / "ledgers"
        folder.mkdir(exist_ok=True)
        book.to_csv(folder / (name + ".csv"), index=False)
        actual = bounded.stats(base.read_frozen_csv(folder / (name + ".csv")), days)
        recorded = metrics.loc[metrics.period.eq("all") & metrics.policy.eq(name)].iloc[0]
        if any(not np.isclose(value, recorded[key], atol=1e-6, rtol=0) for key, value in actual.items()):
            raise ValueError("Saved ledger statistics differ")
    metrics.to_csv(output / "comparison.csv", index=False)
    daily.to_csv(output / "daily_comparison.csv", index=False)
    changes.to_csv(output / "trade_changes.csv", index=False)
    entries.to_csv(output / "entry_checks.csv", index=False)
    observations.to_csv(output / "observations.csv", index=False)
    summary = dict(variant_id=VERSION, scope="BACKTEST_ONLY", execution_authority=False,
        created_at_ist=datetime.now(base.production.IST).isoformat(),
        first_date=days[0], last_date=days[-1], sessions=len(days), days=days, active_setups=51,
        native_control_exactly_reproduced=True, selected_orders=len(native), source_run=str(source_run),
        source_manifest_sha256=monitor_summary["source_manifest_sha256"],
        pinned_monitor_run=str(source), pinned_monitor_manifest_sha256=expected_hash,
        historical_paths_sha256=base.sha256(source / "historical_paths.npz"), config_sha256=config_hash,
        code=code, code_snapshots="code/ contains the exact evaluated Python sources", price_sources=proof,
        reused_feature_audit=feature_cache_proof, unchanged_pointers=pointer_hashes,
        development_selected_policy=chosen, policy_totals=metrics.loc[metrics.period.eq("all")].to_dict("records"),
        entry_feature_status_counts=entries.entry_feature_status.value_counts().to_dict(),
        state_counts=observations.state.value_counts().to_dict(),
        limitations=[
            "Retrospective exploratory hypotheses; no policy has untouched future validation.",
            "Nine policies were specified before evaluation, without adjusting thresholds to their results.",
            "The original G4 strategy and its setups were selected using this history; first35/next15/later2 are chronological diagnostics only.",
            "One-minute OHLC cannot identify intraminute paths or actual buyer/seller-initiated order flow. Candle volume is total traded volume.",
            "VWAP uses minute HLC3 times incremental volume, an approximation to transaction-level VWAP.",
            "Same-time relative volume uses the preceding20 observed sessions with minimum5 usable matching minutes; missing warmup is never synthesized.",
            "Selected-order entry vetoes skip the selected candidate and never substitute the next-ranked stock.",
            "Native entry candle is excluded from post-entry decision windows. Native stops/targets still operate before each close decision.",
            "Following-open exits reserve portfolio capital through that minute end, matching the previous monitor convention.",
            "Five basis points round-trip costs; additional spread, broker latency and market impact are not modeled. Additional5bps sensitivity is included in the summary.",
            "Daily-close drawdown omits intraday portfolio drawdown. Bootstrap intervals are descriptive, not proof of an edge.",
            "The completed cohort ends October15; partial October16 data is excluded."])
    for item in summary["policy_totals"]:
        item["net_pnl_extra_5bps_rs"] = item["net_pnl_rs"] - item["trades"] * 250.
    (output / "summary.json").write_text(json.dumps(base.source._json_ready(summary), indent=2, allow_nan=False) + "\n", encoding="utf-8")
    _report(output, summary, metrics, daily, entries, observations)
    for filename, digest in code.items():
        if base.sha256(Path(filename)) != digest:
            raise ValueError("Research code changed during evaluation")
    if base.sha256(output / "config.json") != config_hash:
        raise ValueError("Frozen parameters changed during evaluation")
    for filename, digest in pointer_hashes.items():
        if base.sha256(Path(filename)) != digest:
            raise ValueError("Strategy result pointer changed during evaluation")
    if base.sha256(source / "manifest.json") != expected_hash:
        raise ValueError("Source monitor manifest changed during evaluation")
    sealed.package(source)
    sealed.package(source_run)
    for item in proof:
        if base.sha256(Path(item["resolved_path"])) != item["sha256"]:
            raise ValueError("Sealed feature input changed during evaluation")
    manifest = dict(variant_id=VERSION, execution_authority=False,
        files={path.relative_to(output).as_posix(): base.sha256(path) for path in output.rglob("*") if path.is_file()})
    (output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    return output, metrics


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--reuse-feature-run", type=Path, help="Verified identical-rule feature audits; replay all policy outcomes afresh")
    args = parser.parse_args()
    now = datetime.now(base.production.IST)
    output = args.output_dir or base.G4_ROOT / "research" / f'{now:%Y%m%dT%H%M%S}_{secrets.token_hex(3)}_hybrid_price_volume_v1'
    directory, metrics = run(output, reuse_feature_run=args.reuse_feature_run)
    print(metrics.loc[metrics.period.eq("all"), ["policy", "trades", "losses", "losing_days", "negative_trade_sum_abs_rs", "net_pnl_rs", "net_change_rs", "monitor_exits", "vetoed_orders"]].to_string(index=False), flush=True)
    print(directory, flush=True)


if __name__ == "__main__":
    main()
