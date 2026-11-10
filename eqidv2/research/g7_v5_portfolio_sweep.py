"""Replacement-aware G7 V5 research replay over the frozen V4 calendar.

This deliberately keeps the V4 profit protection, Nifty-futures permission,
integer cash sizing, two-loss admission lock, and slot allowlist. Only the
09:51/10:46 LONG pre-quota selection is changed by the companion sweep.
"""
from __future__ import annotations

import argparse
from datetime import date
import json
from pathlib import Path

import numpy as np
import pandas as pd

import fno_v13_v10_g_2_backtest as native_simulator
import strategy_slot_selection as slot_selection
from research import g4_g3_mirror_backtest as g4
from research import g7_g6_replica_backtest as g7
from research import g7_nifty_direction as nifty
from research import g7_v4_extremes as features_v4
from research import g7_v4_indicator_iteration as indicators
from research import g7_v4_filtered_backtest as g7_v4
from research import g7_v4_practical_research as practical
from research import g7_v4_lock_backtest as locked
from research import slot_win70_backtest as slot
from research.g8_exit_engine import metrics


V4_PUBLISHED = Path("C:/TradingData/eqidv2/backtesting_result_v13_v10_g_7/v4_lock_backtest/runs/2026-10-29/20261029T082128219945_full_current_config/portfolio_trades.csv")
G3_SUMMARY = locked.G3_RUN / "summary.json"
COSTS = (5., 10., 20.)


def key(row) -> tuple[str, str, str]:
    return (str(row.day)[:10], str(row.setup_id), str(row.tradingsymbol))


def simulate_new(selected: pd.DataFrame, baseline_keys: set[tuple[str, str, str]],
                 base_config: dict) -> tuple[pd.DataFrame, dict]:
    all_new = selected.loc[selected.apply(lambda r: key(r) not in baseline_keys, axis=1)].copy()
    all_new = all_new.drop_duplicates(["day", "setup_id", "tradingsymbol"]).sort_values(
        ["day", "setup_id", "tradingsymbol"], kind="stable").reset_index(drop=True)
    if all_new.empty:
        return pd.DataFrame(), dict(new_candidates=0)
    all_new["sid"] = np.arange(410_000_000, 410_000_000 + len(all_new), dtype=np.int64)
    if all_new.sid.duplicated().any():
        raise ValueError("Duplicate new candidate ID")
    config = g7.configuration()
    specs = {s["setup_id"]: s for s in config["active_setups"]}
    all_new["native_stop_pct"] = 1.2
    all_new["native_target_pct"] = all_new.setup_id.map(lambda s: specs[s]["target_pct"])
    all_new["stop_pct"] = all_new.native_stop_pct
    all_new["target_pct"] = all_new.native_target_pct
    all_new["initial_stop_pct"] = 1.2
    all_new["tightened_stop_pct"] = 1.0
    all_new["tighten_after_minutes"] = 120
    all_new["signal_id"] = all_new.apply(lambda r: "g7_v5_research:" + ":".join(key(r)), axis=1)
    summary = json.loads(G3_SUMMARY.read_text(encoding="utf-8"))
    prices = g4.Prices(summary)
    paths = prices.paths(all_new)
    native = native_simulator.g.v9.v5.simulate_native(
        all_new, paths, cost_bps=5., max_entry_delay_minutes=10)
    protected = g7_v4.previous.replay(native, paths, base_config,
                                      g7_v4.protection_settings(json.loads(g7_v4.CONFIG.read_text(encoding="utf-8"))))
    protected["day"] = protected.day.astype(str).str[:10]
    nifty_audit, nifty_proof, _ = nifty.order_audit(protected, prices)
    permitted = nifty_audit.set_index("sid").both_counter_allowed
    feature_rows = []
    for row in protected.to_dict("records"):
        day = str(row["day"])[:10]
        raw, path = prices.frame(date.fromisoformat(day), row["tradingsymbol"])
        # Prices uses zoneinfo while V4's strict completed-minute comparison
        # builds a pytz index. Normalize the timezone representation only;
        # preserve every UTC nanosecond and OHLCV value.
        raw = raw.copy()
        raw["ts"] = pd.to_datetime(raw.ts.astype("int64"), unit="ns", utc=True).dt.tz_convert(features_v4.IST)
        days = {str(d): group for d, group in raw.groupby(raw.ts.dt.strftime("%Y-%m-%d"), sort=False)}
        try:
            v4 = features_v4.feature_row(days, row)
            indicator = indicators.indicator_row(days[day], row)
        except ValueError as exc:
            stamp = features_v4.timestamp(row["confirmation_ts"])
            start = stamp.normalize() + pd.Timedelta(hours=9, minutes=16)
            prefix = days[day].loc[days[day].ts.ge(start) & days[day].ts.le(stamp)]
            expected = pd.date_range(start, stamp, freq="min")
            raise ValueError(f"V4 feature reconstruction unavailable for {day} "
                             f"{row['setup_id']} {row['tradingsymbol']}: {exc}; "
                             f"prefix={len(prefix)}, expected={len(expected)}, "
                             f"first={prefix.ts.min()}, last={prefix.ts.max()}") from exc
        feature_rows.append({**v4, **indicator, "stock_source_path": path})
    feature_frame = pd.DataFrame(feature_rows).set_index("sid")
    risk = indicators.decisions(feature_frame)["low_efficiency"]
    protected["base_entry_allowed"] = [bool(permitted.loc[int(sid)] and not risk.loc[int(sid)])
                                       for sid in protected.sid]
    protected["g8_baseline_filled"] = protected.filled.astype(bool)
    protected["scope"] = "BACKTEST_ONLY"
    protected["execution_authority"] = False
    proof = dict(new_candidates=len(all_new), new_native_fills=int(native.filled.sum()),
        new_nifty_allowed=int(permitted.sum()), new_v4_risk_vetoed=int(risk.sum()),
        stock_sources=prices.proof,
        nifty_sources=nifty_proof.to_dict("records"))
    return protected, proof


def run(selection_path: Path, output: Path) -> dict:
    if output.exists():
        raise FileExistsError(output)
    books, days, base_config = slot.source_inputs()
    source = books["G7_V4_LOCK2"]
    enabled = slot_selection.enabled_setup_ids("G7_V4_LOCK2")
    original = slot.apply_slot_policy(source, "G7_V4_LOCK2", enabled, base_config, cost=5.)
    published = pd.read_csv(V4_PUBLISHED, low_memory=False)
    control_parity = slot.verify_control(original, published, "G7 V4 locked control")
    if not control_parity["passed"]:
        raise ValueError("Published V4 control did not reproduce")
    selected = pd.read_pickle(selection_path)
    if not set(selected.variant.unique()).issuperset({"baseline", "oi_0_unconditional"}):
        raise ValueError("Incomplete V5 selection grid")
    protocol = json.loads((selection_path.parent / "protocol.json").read_text(encoding="utf-8"))
    target_slots = frozenset(protocol["slots"])
    if not target_slots or not target_slots.issubset(set(source.setup_id)):
        raise ValueError("Selected G7 V5 slots are not in the V4 source")
    source_target = source.loc[source.setup_id.isin(target_slots)]
    source_keys = set(map(key, source_target.itertuples()))
    selected_base = selected.loc[selected.variant.eq("baseline")]
    if source_keys != set(map(key, selected_base.itertuples())):
        raise ValueError("V5 selection baseline differs from V4 source")
    fresh, fresh_proof = simulate_new(selected, source_keys, base_config)
    lookup = {key(row): row._asdict() for row in fresh.itertuples(index=False)}
    original_lookup = {key(row): row._asdict() for row in source_target.itertuples(index=False)}
    rest = source.loc[~source.setup_id.isin(target_slots)].copy()
    comparisons, daywise, change_rows, final_books = [], [], [], {}
    controls = {cost: slot.apply_slot_policy(source, "G7_V4_LOCK2", enabled, base_config, cost=cost)
                for cost in COSTS}
    for variant in selected.variant.unique():
        chosen = selected.loc[selected.variant.eq(variant)]
        keys = set(map(key, chosen.itertuples()))
        missing = keys - set(lookup) - set(original_lookup)
        if missing:
            raise ValueError(f"Unsimulated V5 selected candidate: {sorted(missing)[:3]}")
        rows = [original_lookup[k] if k in original_lookup else lookup[k] for k in sorted(keys)]
        replacement = pd.DataFrame(rows)
        combined = pd.concat([rest, replacement], ignore_index=True, sort=False)
        if combined.sid.duplicated().any():
            raise ValueError("Duplicate G7 V5 portfolio opportunity ID")
        for cost in COSTS:
            book = slot.apply_slot_policy(combined, "G7_V4_LOCK2", enabled, base_config, cost=cost)
            if variant == "baseline":
                slot.verify_control(book, controls[cost], f"V5 baseline at {cost:g} bps")
            for period, calendar in (("all", days), ("october", [d for d in days if d.startswith("2026-10")])):
                result = metrics(book, calendar)
                previous = metrics(controls[cost], calendar)
                comparisons.append(dict(variant=variant, cost_bps=cost, period=period,
                    **result, delta_net_pnl_rs=result["net_pnl_rs"]-previous["net_pnl_rs"],
                    delta_losses=result["losses"]-previous["losses"],
                    delta_losing_days=result["losing_days"]-previous["losing_days"]))
            if cost == 5.:
                final_books[variant] = book
                for day in days:
                    result = metrics(book, [day]); previous = metrics(controls[cost], [day])
                    daywise.append(dict(variant=variant, day=day, **result,
                        delta_net_pnl_rs=result["net_pnl_rs"]-previous["net_pnl_rs"]))
                old = controls[cost].set_index("sid")
                new = book.set_index("sid")
                for sid in sorted(set(old.index) | set(new.index)):
                    old_exec = bool(old.loc[sid,"portfolio_executed"]) if sid in old.index else False
                    new_exec = bool(new.loc[sid,"portfolio_executed"]) if sid in new.index else False
                    if old_exec != new_exec:
                        row = new.loc[sid] if sid in new.index else old.loc[sid]
                        change_rows.append(dict(variant=variant, sid=sid, day=str(row.day)[:10],
                            setup_id=row.setup_id, tradingsymbol=row.tradingsymbol,
                            before_executed=old_exec, after_executed=new_exec,
                            before_pnl_rs=float(old.loc[sid,"portfolio_net_profit_rupees"]) if old_exec else 0.,
                            after_pnl_rs=float(new.loc[sid,"portfolio_net_profit_rupees"]) if new_exec else 0.))
        print("PORTFOLIO", variant, "new selections", len(keys-source_keys), flush=True)
    output.mkdir(parents=True)
    table = pd.DataFrame(comparisons)
    table.to_csv(output/"overall_comparison.csv", index=False)
    pd.DataFrame(daywise).to_csv(output/"daywise_comparison.csv", index=False)
    pd.DataFrame(change_rows).to_csv(output/"changed_executions.csv", index=False)
    fresh.to_csv(output/"new_opportunity_outcomes.csv", index=False)
    for variant, book in final_books.items():
        book.to_csv(output/(variant+"_portfolio_trades.csv"), index=False)
    summary = dict(first_day=days[0], last_day=days[-1], sessions=len(days),
        baseline_parity=control_parity, source_proof=fresh_proof,
        selection_protocol=str(selection_path.parent / "protocol.json"),
        costs_bps=COSTS, variants=list(final_books), scope="BACKTEST_ONLY",
        execution_authority=False, independent_holdout=False,
        limitation="The frozen V4 source ends 2026-10-27; incomplete 2026-10-28/30 cash-minute coverage prevents a full through-Friday result.")
    (output/"summary.json").write_text(json.dumps(summary, indent=2, default=str), encoding="utf-8")
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--selection", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(run(args.selection, args.output), indent=2, default=str), flush=True)


if __name__ == "__main__":
    main()
