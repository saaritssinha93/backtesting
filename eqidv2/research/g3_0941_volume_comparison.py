"""Isolated 09:41 LONG one-minute volume threshold study; never publishes a pointer."""
import argparse
from datetime import date, datetime
import json
from pathlib import Path
from unittest.mock import patch

import numpy as np
import pandas as pd

from research import g3_target_cutoff_backtest as bt
from research import g3_0950_long_backtest as inputs
from research.g3_0920_backtest import allocate, _stats
from fno_v13_v10_g_3_backtest import OCTOBER12_SOURCE, BACKTEST_ROOT

SLOT = "0941_LONG"


def keys(frame):
    return set(zip(pd.to_datetime(frame.day).dt.strftime("%Y-%m-%d"), frame.tradingsymbol))


def run(variant_minimum: float | None = None):
    root = BACKTEST_ROOT
    pointer_path = root / "latest/current_backtest.json"
    pointer_bytes = pointer_path.read_bytes()
    production_fingerprint = bt.production.strategy_fingerprint()
    minimum = bt.production.confirmation_volume_minimum("LONG", setup_id=SLOT)
    if variant_minimum is not None and not (0 <= variant_minimum <= minimum):
        raise ValueError(f"Variant minimum must be between zero and the current {minimum:.2f}")
    pointer = json.loads(pointer_bytes)
    current = root / pointer["run_dir"]
    summary, manifest = bt.verified_package(current, root)
    baseline = bt.read_frozen_csv(current / "portfolio_trades.csv")
    saved_slot = baseline.loc[baseline.setup_id.eq(SLOT)].copy()
    days = [date.fromisoformat(d) for d in summary["days"]]
    suffix = ("no_1m_volume" if variant_minimum is None else
              f"1m_volume_min_{int(round(variant_minimum * 100)):03d}")
    out = root / "research" / f"{datetime.now(bt.production.IST):%Y%m%dT%H%M%S}_0941_long_{suffix}"
    out.mkdir(parents=True, exist_ok=False)
    print(f"Baseline verified: {len(days)} sessions, {len(saved_slot)} slot orders", flush=True)

    signals, historical_days, sources, proof = inputs.load_sources(
        inputs.DEFAULT_HISTORY_PROVENANCE, inputs.DEFAULT_RECENT_PROVENANCE, OCTOBER12_SOURCE)
    pool = signals.loc[signals.signal_end.eq("09:40") & signals.side.eq("LONG")].copy()
    _, _, pinned_days, members, baseline_proof = bt._baseline(root)
    assert days == pinned_days and historical_days == days[:49]
    member_map = {}
    for directory, payload, _ in members:
        candidates = bt.read_frozen_csv(directory / payload["artifacts"]["candidate_signals"]["path"])
        candidates = inputs.normalize_signals(candidates)
        candidates = candidates.loc[candidates.signal_end.eq("09:40") & candidates.side.eq("LONG")]
        pool = pd.concat([pool, candidates], ignore_index=True, sort=False)
        member_map[payload["session_date"]] = (directory, payload)
    assert not pool.duplicated(["day", "tradingsymbol"]).any()
    # Candidate SIDs are local to their source; use a unique audit index while
    # retaining the original path identity separately.
    pool["input_sid"] = pool.sid
    pool["sid"] = np.arange(90_000_000, 90_000_000 + len(pool))
    normal_audit = bt.production.selection_audit(pool)
    normal = normal_audit.loc[normal_audit.v9_selected.eq(True)].copy()
    if keys(normal) != keys(saved_slot):
        raise ValueError(f"Original selection parity failed: expected-only={keys(saved_slot)-keys(normal)}, actual-only={keys(normal)-keys(saved_slot)}")
    original_filter = bt.production.passes_selected_filters
    def with_variant_volume(candidate, setup, **kwargs):
        if setup.setup_id == SLOT and (variant_minimum is None or
                                       float(candidate.get("v9_1m_volume_ratio", float("nan"))) >= variant_minimum):
            candidate = {**candidate, "v9_1m_volume_ratio": minimum}
        return original_filter(candidate, setup, **kwargs)
    with patch.object(bt.production, "passes_selected_filters", with_variant_volume):
        variant_audit = bt.production.selection_audit(pool)
    variant = variant_audit.loc[variant_audit.v9_selected.eq(True)].copy()
    assert bt.production.strategy_fingerprint() == production_fingerprint
    pool.to_csv(out / "candidate_pool.csv", index=False)
    normal_audit.to_csv(out / "baseline_selection_audit.csv", index=False)
    variant_audit.to_csv(out / "variant_selection_audit.csv", index=False)
    print(f"Verified original selection; slot orders {len(normal)} -> {len(variant)}", flush=True)

    union = pd.concat([normal, variant]).drop_duplicates("sid").copy()
    native = union.copy()
    native["sid"] = native.input_sid.astype(int)
    hist = native.loc[pd.to_datetime(native.day).dt.date.isin(historical_days)]
    historical_paths = inputs.selected_paths(hist, sources)
    paths = {int(row.sid): historical_paths[int(row.input_sid)] for row in
             union.loc[pd.to_datetime(union.day).dt.date.isin(historical_days)].itertuples()}
    for day, (directory, payload) in member_map.items():
        selected = native.loc[pd.to_datetime(native.day).dt.strftime("%Y-%m-%d").eq(day)]
        if not len(selected):
            continue
        daily_paths, daily_proof = bt._daily_paths(directory, payload, selected)
        proof[day] = daily_proof
        for row in union.loc[pd.to_datetime(union.day).dt.strftime("%Y-%m-%d").eq(day)].itertuples():
            paths[int(row.sid)] = daily_paths[int(row.input_sid)]
    bt.simulator.g.v9.validate_paths(union, paths)
    replayed = bt.simulator.simulate_staged(union, paths,
        cost_bps=bt.contract.ROUND_TRIP_COST_BPS,
        max_entry_delay_minutes=bt.contract.ENTRY_EXPIRY_MINUTES)
    replayed["day"] = pd.to_datetime(replayed.day).dt.strftime("%Y-%m-%d")
    saved_by_key = saved_slot.assign(key=saved_slot.day.astype(str) + "|" + saved_slot.tradingsymbol).set_index("key")
    normal_raw = replayed.loc[replayed.sid.isin(normal.sid)].copy()
    normal_raw["key"] = normal_raw.day + "|" + normal_raw.tradingsymbol
    normal_raw["sid"] = normal_raw.key.map(saved_by_key.sid).astype(int)
    normal_raw["signal_id"] = normal_raw.key.map(saved_by_key.signal_id)
    bt._same_native(normal_raw, saved_slot)
    unchanged = baseline.loc[~baseline.setup_id.eq(SLOT)].copy()
    reconstructed = allocate(pd.concat([unchanged, normal_raw], ignore_index=True, sort=False))
    bt._same_allocation(reconstructed, baseline)
    print("Original minute outcomes and full portfolio allocation reproduce saved baseline", flush=True)

    new_raw = replayed.loc[replayed.sid.isin(variant.sid)].copy()
    new_raw["key"] = new_raw.day + "|" + new_raw.tradingsymbol
    for index, row in new_raw.iterrows():
        if row.key in saved_by_key.index:
            new_raw.loc[index, "sid"] = saved_by_key.loc[row.key, "sid"]
            new_raw.loc[index, "signal_id"] = saved_by_key.loc[row.key, "signal_id"]
        else:
            new_raw.loc[index, "signal_id"] = f"G3_0941_{suffix.upper()}|{row.key}"
    variant_ledger = allocate(pd.concat([unchanged, new_raw], ignore_index=True, sort=False))
    comparison = []
    for label, frame in (("baseline", baseline), (suffix, variant_ledger)):
        for scope in ("portfolio", SLOT):
            selected = frame if scope == "portfolio" else frame.loc[frame.setup_id.eq(SLOT)]
            comparison.append(dict(policy=label, scope=scope, **_stats(selected, days)))
    daily = bt._daily(baseline, days).merge(bt._daily(variant_ledger, days), on="day", suffixes=("_before", "_after"))
    daily["net_pnl_delta"] = daily.net_pnl_after - daily.net_pnl_before
    before = baseline.set_index("signal_id")
    after = variant_ledger.set_index("signal_id")
    changed = []
    for sid in sorted(set(before.index) | set(after.index)):
        a = before.loc[sid] if sid in before.index else None
        b = after.loc[sid] if sid in after.index else None
        pnl_a = float(a.portfolio_net_profit_rupees) if a is not None else 0.
        pnl_b = float(b.portfolio_net_profit_rupees) if b is not None else 0.
        if a is None or b is None or not np.isclose(pnl_a, pnl_b, atol=1e-6):
            row = b if b is not None else a
            changed.append(dict(day=str(row.day), setup_id=row.setup_id, symbol=row.tradingsymbol,
                change="added" if a is None else "removed" if b is None else "allocation_changed",
                volume_ratio_1m=row.v9_1m_volume_ratio, filled=row.filled,
                executed_before=bool(a.portfolio_executed) if a is not None else False,
                executed_after=bool(b.portfolio_executed) if b is not None else False,
                net_before=pnl_a, net_after=pnl_b, net_delta=pnl_b-pnl_a,
                entry_ts=str(row.entry_ts), exit_ts=str(row.exit_ts), exit_reason=row.exit_reason))
    report = dict(scope="BACKTEST_ONLY", execution_authority=False, baseline=str(current),
        baseline_manifest_sha256=bt.sha256(current / "manifest.json"),
        strategy_version=bt.contract.STRATEGY_VERSION, first_date=str(days[0]), last_date=str(days[-1]),
        sessions=len(days), changed_rule=("Remove only 0941_LONG one-minute volume requirement, including the ratio availability check"
                                         if variant_minimum is None else
                                         f"Set only 0941_LONG one-minute volume ratio minimum to {variant_minimum:.2f}"),
        original_volume_minimum=minimum, variant_volume_minimum=variant_minimum,
        missing_volume_ratio_candidates=int(pool.v9_1m_volume_ratio.isna().sum()),
        selection_parity=True, native_execution_parity=True, portfolio_allocation_parity=True,
        unaffected_native_executions="Retained sealed native outcomes; full portfolio acceptance and costs recalculated",
        comparison=comparison, changed_trades=changed, source_proof=proof,
        excluded_session="2026-10-01: incomplete immutable historical input")
    pd.DataFrame(comparison).to_csv(out / "comparison.csv", index=False)
    pd.DataFrame(changed).to_csv(out / "changed_trades.csv", index=False)
    daily.to_csv(out / "daily_comparison.csv", index=False)
    variant_ledger.to_csv(out / "variant_portfolio_trades.csv", index=False)
    baseline.to_csv(out / "baseline_portfolio_trades.csv", index=False)
    bt.replay.atomic_json(out / "summary.json", report)
    assert pointer_path.read_bytes() == pointer_bytes
    assert bt.production.strategy_fingerprint() == production_fingerprint
    print(json.dumps({"output":str(out), "comparison":comparison, "changed_trades":changed}, indent=2, default=str), flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--minimum", type=float, default=None,
                        help="One-minute volume ratio floor for 0941_LONG; omitted means no floor")
    run(parser.parse_args().minimum)
