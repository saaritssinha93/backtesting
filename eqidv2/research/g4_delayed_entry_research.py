"""Research-only G4 replay with no immediately active post-confirmation order.

Each selected order keeps its original trigger and ten-minute expiry. A wait of
one bar means the first possible fill is the *second* post-confirmation minute.
The optional hold gate reads only completed waiting-bar closes before arming.
No production configuration or latest-result pointer is changed.
"""
from __future__ import annotations

import argparse
from datetime import datetime
import json
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pandas as pd

import fno_v13_corrected_v5_backtest as native
from research import g4_bounded_exit_backtest as bounded
from research import g4_failed_breakout_research as sealed
from research import g4_g3_mirror_backtest as base
from research import g4_iterative_dataset as dataset
from research import g3_sl_target_simulator as exit_sim
from research.g3_sl_target_simulator import simulate_exit


VERSION = "G4-ALL-ORDER-DELAYED-ENTRY-RESEARCH-V1"
DEFAULT_DATASET = (base.G4_ROOT / "research" /
    "20261017_g4_53_session_sealed_for_atr")
SCENARIOS = {
    "current_g4": (0, False),
    "wait_1_bar": (1, False),
    "wait_1_bar_hold": (1, True),
    "wait_2_bars": (2, False),
    "wait_2_bars_hold": (2, True),
}


def delayed_entry(row, path, delay_bars: int, require_hold: bool = False):
    """Return the native trigger fill, after completed waiting bars only.

    `path[0]` is the first bar after the confirmation candle. The original
    ten-minute deadline is fixed, not moved forward by the waiting period.
    A hold gate requires every skipped bar to close beyond the original trigger
    in the trade direction; it cannot inspect the eventual fill bar's close.
    """
    if type(delay_bars) is not int or not 0 <= delay_bars < 10:
        raise ValueError("delay_bars must be an integer from zero to nine")
    if require_hold and delay_bars == 0:
        raise ValueError("A hold check requires at least one completed waiting bar")
    side = row["side"] if isinstance(row, dict) else row.side
    trigger = float(row["trigger"] if isinstance(row, dict) else row.trigger)
    if side not in ("LONG", "SHORT") or not np.isfinite(trigger) or trigger <= 0:
        raise ValueError("A valid G4 side and trigger are required")
    if len(path["close"]) <= delay_bars:
        return None
    if require_hold:
        observed = np.asarray(path["close"][:delay_bars], dtype=float)
        holding = observed >= trigger if side == "LONG" else observed <= trigger
        if not bool(np.all(holding)):
            return None
    native_row = SimpleNamespace(**row) if isinstance(row, dict) else row
    return native._entry(
        native_row, path, delay_bars=delay_bars, trigger_buffer_pct=0.,
        worse_fill_bps=0., max_entry_delay_minutes=10-delay_bars)


def replay(original: pd.DataFrame, paths: dict, *, delay_bars: int,
           require_hold: bool, scenario: str) -> pd.DataFrame:
    """Reprice every altered entry/exit and reallocate the complete portfolio."""
    engine = sealed.bounded.history.simulator.g.v9
    records = []
    for source in original.to_dict("records"):
        row = source.copy()
        path = paths[int(row["sid"])]
        found = delayed_entry(row, path, delay_bars, require_hold)
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
    ledger["strategy_version"] = VERSION + "-" + scenario
    ledger["scope"] = "BACKTEST_ONLY"
    ledger["execution_authority"] = False
    return ledger


def _changes(before: pd.DataFrame, after: pd.DataFrame,
             scenario: str) -> pd.DataFrame:
    fields = ["sid", "day", "setup_id", "tradingsymbol", "side", "filled",
        "entry_ts", "entry_price", "exit_ts", "exit_reason",
        "portfolio_executed", "portfolio_net_profit_rupees"]
    result = before[fields].merge(after[fields], on="sid", validate="one_to_one",
        suffixes=("_baseline", "_variant"))
    result["scenario"] = scenario
    result["net_change_rs"] = (result.portfolio_net_profit_rupees_variant -
        result.portfolio_net_profit_rupees_baseline)
    return result


def run(output: Path, *, source: Path = DEFAULT_DATASET) -> pd.DataFrame:
    output = Path(output).resolve()
    source = Path(source).resolve()
    if output.exists() or not output.is_relative_to((base.G4_ROOT / "research").resolve()):
        raise ValueError("Require a fresh isolated G4 research output directory")
    original, _contexts, paths, proof = dataset.load(source)
    days = list(proof["days"])
    if len(days) != 53 or len(original) != 543 or set(paths) != set(original.sid.astype(int)):
        raise ValueError("Unexpected sealed G4 cohort")
    if not proof.get("native_control_exactly_reproduced"):
        raise ValueError("Unverified baseline cache")
    ledgers = {}
    comparison = []
    daily = []
    changes = []
    periods = {"all": days, "october_15": ["2026-10-15"],
        "october_16": ["2026-10-16"]}
    for name, (delay, hold) in SCENARIOS.items():
        book = replay(original, paths, delay_bars=delay,
            require_hold=hold, scenario=name)
        if name == "current_g4":
            sealed.parity(book, original)
        else:
            fills = book.loc[book.filled.eq(True)]
            if (fills.entry_path_index.lt(delay).any() or
                    fills.entry_path_index.ge(10).any()):
                raise ValueError("A delayed fill escaped its allowed entry window")
        ledgers[name] = book
        delta = _changes(original, book, name)
        changes.append(delta)
        for period, cohort in periods.items():
            metrics = bounded.stats(book, cohort)
            reference = bounded.stats(original, cohort)
            subset = delta.loc[delta.day_baseline.isin(cohort)]
            comparison.append(dict(scenario=name, period=period, **metrics,
                net_change_rs=metrics["net_pnl_rs"]-reference["net_pnl_rs"],
                avoided_baseline_losers=int((
                    subset.portfolio_net_profit_rupees_baseline.lt(0) &
                    ~subset.portfolio_executed_variant.eq(True)).sum()),
                missed_baseline_winners=int((
                    subset.portfolio_net_profit_rupees_baseline.gt(0) &
                    ~subset.portfolio_executed_variant.eq(True)).sum()),
                new_allocations=int((
                    ~subset.portfolio_executed_baseline.eq(True) &
                    subset.portfolio_executed_variant.eq(True)).sum())))
        daily.extend(dict(scenario=name, day=day,
            **bounded.stats(book, [day])) for day in days)
        print(f"{name}: {comparison[-len(periods)]['net_pnl_rs']:,.2f} net",
              flush=True)
    output.mkdir(parents=True)
    pd.DataFrame(comparison).to_csv(output / "comparison.csv", index=False)
    pd.DataFrame(daily).to_csv(output / "daily_comparison.csv", index=False)
    pd.concat(changes, ignore_index=True).to_csv(
        output / "trade_changes.csv", index=False)
    for name, book in ledgers.items():
        folder = output / name
        folder.mkdir()
        book.to_csv(folder / "portfolio_trades.csv", index=False)
        saved = base.read_frozen_csv(folder / "portfolio_trades.csv")
        expected = bounded.stats(book, days)
        observed = bounded.stats(saved, days)
        for field, value in expected.items():
            if not np.isclose(observed[field], value, rtol=0, atol=1e-6):
                raise ValueError("Saved delayed-entry ledger failed accounting parity")
    summary = dict(variant_id=VERSION, scope="BACKTEST_ONLY",
        execution_authority=False, default_configuration_changed=False,
        source_dataset=str(source),
        source_manifest_sha256=base.sha256(source / "manifest.json"),
        days=days, selected_orders=len(original),
        original_accounting_reproduced=True,
        methods="Every selected G4 order waits zero, one or two full minute bars; optional hold requires each skipped close beyond the original trigger. Original trigger and ten-minute expiry, native gap-through fills, exits, costs and full portfolio reallocation are retained. No replacement after a missed fill.",
        limitations=[
            "This is retrospective research; 15/16 October are diagnostics, not holdouts.",
            "The inspected G3 runtime uses 90 seconds for confirmation publication and a separate ten-minute trigger deadline; G4 live delayed-entry timing and order handling have not been validated.",
            "OHLC minute bars cannot resolve intraminute sequences; conservative native stop-first convention is retained.",
            "Order cancellation/publication latency and exchange queue position are not modeled.",
            "This selected-order timing test does not re-rank the candidate pool after a missed fill."],
        comparison=comparison)
    (output / "summary.json").write_text(json.dumps(summary, indent=2)+"\n",
                                         encoding="utf-8")
    code = (Path(__file__), Path(native.__file__), Path(sealed.__file__),
        Path(dataset.__file__), Path(bounded.__file__), Path(exit_sim.__file__))
    manifest = dict(files={p.relative_to(output).as_posix(): base.sha256(p)
        for p in output.rglob("*") if p.is_file()},
        code_sha256={str(p.resolve()): base.sha256(p) for p in code})
    (output / "manifest.json").write_text(json.dumps(manifest, indent=2)+"\n",
                                           encoding="utf-8")
    return pd.DataFrame(comparison)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--source", type=Path, default=DEFAULT_DATASET)
    args = parser.parse_args()
    output = args.output_dir or (base.G4_ROOT / "research" /
        datetime.now().strftime("%Y%m%dT%H%M%S_all_order_delayed_entry_v1"))
    comparison = run(output, source=args.source)
    print(comparison.loc[comparison.period.isin(
        ["all", "october_15", "october_16"])].to_string(index=False),
        flush=True)
    print(output, flush=True)


if __name__ == "__main__":
    main()
