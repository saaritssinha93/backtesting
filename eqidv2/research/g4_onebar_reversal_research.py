"""Research-only one-bar G4 entry delay with causal first-bar validity gates.

All selected orders wait one complete minute. The original trigger can first
fill in minute two and still expires at the original minute-ten deadline.
"""
from __future__ import annotations

import argparse
from datetime import datetime
import json
from pathlib import Path

import numpy as np
import pandas as pd

from research import g4_bounded_exit_backtest as bounded
from research import g4_delayed_entry_research as prior
from research import g4_failed_breakout_research as sealed
from research import g4_g3_mirror_backtest as base
from research import g4_iterative_dataset as dataset
from research.g3_sl_target_simulator import simulate_exit

VERSION = "G4-ALL-ORDER-ONEBAR-REVERSAL-RESEARCH-V1"
GATES = ("none", "no_break", "midpoint", "directional_bar")


def first_bar_passes(row, path, gate: str) -> bool:
    """Decide at first post-confirmation candle close; never inspect bar two."""
    if gate not in GATES:
        raise ValueError("Unknown first-bar gate")
    if gate == "none":
        return True
    side = row["side"]
    if side not in ("LONG", "SHORT") or len(path["close"]) < 2:
        raise ValueError("A valid G4 side and two-bar path are required")
    sign = 1 if side == "LONG" else -1
    close, opening = float(path["close"][0]), float(path["open"][0])
    high, low = float(row["confirmation_high"]), float(row["confirmation_low"])
    if gate == "no_break":
        return close >= low if sign == 1 else close <= high
    if gate == "midpoint":
        return sign * (close - (high + low) / 2) >= 0
    return sign * (close - opening) >= 0


def replay(original: pd.DataFrame, paths: dict, gate: str) -> pd.DataFrame:
    """Recalculate entries, bounded exits, costs and complete portfolio."""
    engine = sealed.bounded.history.simulator.g.v9
    records = []
    for source in original.to_dict("records"):
        row = source.copy()
        path = paths[int(row["sid"])]
        allowed = first_bar_passes(row, path, gate)
        found = prior.delayed_entry(row, path, delay_bars=1) if allowed else None
        row.update(filled=False, position_open=False, entry_ts=pd.NaT,
            entry_price=np.nan, entry_path_index=np.nan, entry_gap_through=False,
            entry_overshoot_bps=np.nan, exit_ts=pd.NaT, exit_price=np.nan,
            exit_path_index=np.nan, gross_return_pct=np.nan, net_return_pct=np.nan,
            cost_pct=np.nan, exit_reason="UNFILLED")
        if found is not None:
            index, entry, trigger, gap, overshoot = found
            row.update(filled=True, entry_path_index=index, entry_price=entry,
                trigger_used=trigger, entry_gap_through=gap,
                entry_overshoot_bps=overshoot,
                entry_ts=pd.Timestamp(int(path["timestamp_ns"][index]), tz="UTC")
                    .tz_convert(base.production.IST))
            outcome = simulate_exit(path, index, entry, row["side"] == "LONG",
                1.2, float(row["native_target_pct"]),
                tighten_after_minutes=120, tightened_stop_pct=1.)
            row.update(outcome)
            row.update(cost_pct=.05, net_return_pct=outcome["gross_return_pct"]-.05)
        records.append(row)
    work = pd.DataFrame(records)
    settings = engine.V9Config(portfolio_capital_rupees=1000000.,
        capital_per_entry_rupees=100000., leverage_factor=5.,
        max_positions=10, cost_bps=5.)
    sized = engine.v5.apply_fixed_capital_model(work, 100000., 5.)
    ledger, _ = engine.v6.apply_portfolio_constraints(
        sized, settings.portfolio_config())
    ledger["day"] = ledger.day.astype(str).str[:10]
    ledger["strategy_version"] = VERSION + "-" + gate
    ledger["scope"] = "BACKTEST_ONLY"
    ledger["execution_authority"] = False
    return ledger


def run(output: Path):
    output = Path(output).resolve()
    if output.exists() or not output.is_relative_to((base.G4_ROOT / "research").resolve()):
        raise ValueError("Require a fresh isolated G4 research directory")
    source = prior.DEFAULT_DATASET
    original, _contexts, paths, proof = dataset.load(source)
    if len(proof["days"]) != 53 or len(original) != 543:
        raise ValueError("Unexpected sealed G4 cohort")
    days = list(proof["days"])
    reference = prior.replay(original, paths, delay_bars=1,
        require_hold=False, scenario="wait_1_bar")
    comparison, daily, changes, ledgers = [], [], [], {}
    periods = {"all": days, "october_15": ["2026-10-15"],
        "october_16": ["2026-10-16"]}
    for gate in GATES:
        book = replay(original, paths, gate)
        if gate == "none":
            bounded.history._same_native(book, reference)
            bounded.history._same_allocation(book, reference)
        ledgers[gate] = book
        delta = prior._changes(original, book, gate)
        delta["first_bar_passed"] = [first_bar_passes(row, paths[int(row["sid"])], gate)
            for row in original.to_dict("records")]
        changes.append(delta)
        for period, cohort in periods.items():
            metrics = bounded.stats(book, cohort)
            baseline = bounded.stats(original, cohort)
            subset = delta.loc[delta.day_baseline.isin(cohort)]
            comparison.append(dict(gate=gate, period=period, **metrics,
                net_change_rs=metrics["net_pnl_rs"]-baseline["net_pnl_rs"],
                skipped_baseline_winners=int((
                    subset.portfolio_net_profit_rupees_baseline.gt(0) &
                    ~subset.portfolio_executed_variant.eq(True)).sum()),
                skipped_baseline_losers=int((
                    subset.portfolio_net_profit_rupees_baseline.lt(0) &
                    ~subset.portfolio_executed_variant.eq(True)).sum())))
        daily.extend(dict(gate=gate, day=day, **bounded.stats(book, [day]))
            for day in days)
        print(gate, comparison[-len(periods)]["net_pnl_rs"], flush=True)
    output.mkdir(parents=True)
    pd.DataFrame(comparison).to_csv(output / "comparison.csv", index=False)
    pd.DataFrame(daily).to_csv(output / "daily_comparison.csv", index=False)
    pd.concat(changes, ignore_index=True).to_csv(output / "trade_changes.csv", index=False)
    for gate, book in ledgers.items():
        folder = output / gate
        folder.mkdir()
        book.to_csv(folder / "portfolio_trades.csv", index=False)
        saved = base.read_frozen_csv(folder / "portfolio_trades.csv")
        expected, observed = bounded.stats(book, days), bounded.stats(saved, days)
        if any(not np.isclose(observed[key], value, rtol=0, atol=1e-6)
               for key, value in expected.items()):
            raise ValueError("Saved ledger does not reconcile")
    summary = dict(variant_id=VERSION, scope="BACKTEST_ONLY",
        execution_authority=False, default_configuration_changed=False,
        source_dataset=str(source), source_manifest_sha256=base.sha256(source / "manifest.json"),
        method="All selected G4 orders wait one completed minute; causal first-bar gate then arms original trigger for minutes two through ten. Native gap fills, bounded exits, costs and full allocation replayed without candidate replacement.",
        gates={"none":"No first-bar validity test",
            "no_break":"First close does not breach opposite confirmation-candle extreme",
            "midpoint":"First close remains on breakout side of confirmation-candle midpoint",
            "directional_bar":"First candle closes in breakout direction relative to its open"},
        comparison=comparison,
        limitations=["Retrospective, previously examined 53 sessions; no untouched validation.",
            "The inspected G3 runtime uses 90 seconds for confirmation publication and a separate ten-minute trigger deadline; G4 live deployment and latency have not been validated.",
            "No intraminute order sequencing or queue model; first-bar close cannot cancel an already-filled first-bar order, so this research policy never arms in that bar.",
            "No candidate replacement after a failed gate."])
    (output / "summary.json").write_text(json.dumps(summary, indent=2)+"\n", encoding="utf-8")
    code = (Path(__file__), Path(prior.__file__), Path(sealed.__file__),
        Path(dataset.__file__), Path(bounded.__file__))
    manifest = dict(files={p.relative_to(output).as_posix(): base.sha256(p)
        for p in output.rglob("*") if p.is_file()},
        code_sha256={str(p.resolve()): base.sha256(p) for p in code})
    (output / "manifest.json").write_text(json.dumps(manifest, indent=2)+"\n", encoding="utf-8")
    return pd.DataFrame(comparison)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path)
    args = parser.parse_args()
    output = args.output_dir or (base.G4_ROOT / "research" /
        datetime.now().strftime("%Y%m%dT%H%M%S_all_order_onebar_reversal_v1"))
    result = run(output)
    print(result.to_string(index=False), flush=True)
    print(output, flush=True)


if __name__ == "__main__":
    main()
