"""Verify saved G6 source provenance, decisions, allocations and reports."""
from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd

from research import g6_nifty_risk_backtest as study


def verify(directory):
    directory = Path(directory).resolve()
    manifest = study.source.base.read(directory/"manifest.json")
    for name, digest in manifest["files"].items():
        if study.source.base.sha256(directory/name) != digest:
            raise ValueError("Artifact hash mismatch: " + name)
    for name, digest in manifest["code_sha256"].items():
        if study.source.base.sha256(Path(name)) != digest:
            raise ValueError("Code changed: " + name)
    config = study.source.base.read(directory/"config.json")
    summary = study.source.base.read(directory/"summary.json")
    source = Path(config["source_run"])
    if study.source.base.sha256(source/"manifest.json") != config["source_manifest_sha256"]:
        raise ValueError("Source manifest changed")
    study.source_verify.verify(source)
    native = study.source.base.read_frozen_csv(source/"baseline_native.csv")
    base_config = study.source.base.read(source/"config.json")["base_g4_config"]
    features = pd.read_csv(directory/"nifty_features.csv").set_index("sid", drop=False)
    # Reconstruct feature decisions from sealed candles, independently of saved flags.
    reconstructed = study.build_features(native, source, config).sort_index()
    pd.testing.assert_frame_equal(features.sort_index(), reconstructed, check_dtype=False)
    for day, group in native.groupby("day"):
        frame = pd.read_parquet(source/"nifty_inputs"/(day+".parquet"))
        for row in group.itertuples():
            prefix = frame.loc[frame.ts.le(row.confirmation_ts)]
            actual = study.features_at(prefix, row.side, row.confirmation_ts, config)
            expected = reconstructed.loc[row.sid]
            for flag in ("slot_counter", "session_counter", "both_counter", "persistent_counter"):
                if actual[flag] != expected[flag]:
                    raise ValueError("Prefix-only decision mismatch")
    comparison = pd.read_csv(directory/"comparison.csv")
    daily = pd.read_csv(directory/"daily_comparison.csv")
    setups = pd.read_csv(directory/"setup_results.csv")
    decisions = pd.read_csv(directory/"decision_audit.csv")
    days = summary["days"]
    periods = {"all":days, "first_30":days[:30], "later_24":days[30:],
        "block_1":days[:18], "block_2":days[18:36], "block_3":days[36:],
        "october_15":["2026-10-15"], "october_16":["2026-10-16"], "last_day":[days[-1]]}
    governor_checks, ledgers = 0, 0
    for cost in config["cost_bps_round_trip"]:
        control = study.source.base.read_frozen_csv(directory/f"cost_{cost:g}bps"/"control"/"portfolio_trades.csv")
        for variant in config["variants"]:
            book = study.source.base.read_frozen_csv(directory/f"cost_{cost:g}bps"/variant/"portfolio_trades.csv")
            audit = decisions.loc[decisions.cost_bps.eq(cost) & decisions.variant.eq(variant)].set_index("sid")
            if len(book) != len(native) or book.sid.duplicated().any() or set(book.sid) != set(native.sid) or set(audit.index) != set(native.sid):
                raise ValueError("Incomplete ledger/decision coverage")
            current = book.set_index("sid").reindex(native.sid)
            original = native.set_index("sid")
            for field in ("entry_ts", "exit_ts", "entry_price", "exit_price", "gross_return_pct", "exit_reason"):
                pd.testing.assert_series_equal(current[field], original[field], check_dtype=False, check_names=False)
            static = study.static_factors(features, variant)
            for sid, decision in audit.iterrows():
                factor = static.loc[sid]
                if variant in study.DYNAMIC:
                    stamp = pd.Timestamp(decision.confirmation_ts)
                    exits = pd.to_datetime(book.exit_ts, utc=True, errors="coerce")
                    closed = book.loc[book.portfolio_executed & book.day.eq(stamp.strftime("%Y-%m-%d")) & exits.lt(stamp)]
                    closed_returns = closed.groupby("exit_ts").portfolio_net_profit_rupees.sum().sort_index()
                    floor = min([0., *closed_returns.cumsum().tolist()])
                    threshold = config["risk_unit_rs"] * (2 if variant == "daily_2r_halt" else 1)
                    triggered = floor <= -threshold
                    if triggered:
                        factor = min(factor, 0. if variant == "daily_2r_halt" else .5)
                    np.testing.assert_allclose([closed_returns.sum(), floor],
                        [decision.realised_before_rs, decision.realised_floor_before_rs], atol=1e-6, rtol=0)
                    if triggered != decision.governor_triggered or len(closed) != decision.closed_trades_before:
                        raise ValueError("Daily governor evidence mismatch")
                    governor_checks += 1
                if factor != decision.multiplier or factor != current.loc[sid,"g6_multiplier"]:
                    raise ValueError("Incorrect risk multiplier")
            replayed, _ = study.allocate(native, audit.multiplier, base_config, cost, variant)
            pd.testing.assert_series_equal(replayed.set_index("sid").portfolio_executed,
                book.set_index("sid").portfolio_executed, check_names=False)
            exposure = base_config["capital_per_entry_rs"]*base_config["leverage"]*book.g6_multiplier
            expected_cost = np.where(book.portfolio_executed, exposure*cost/10000, 0.)
            expected_gross = np.where(book.portfolio_executed, exposure*book.gross_return_pct/100, 0.)
            np.testing.assert_allclose(book.portfolio_cost_rupees, expected_cost, atol=1e-6, rtol=0)
            np.testing.assert_allclose(book.portfolio_net_profit_rupees, expected_gross-expected_cost, atol=1e-6, rtol=0)
            for row in comparison.loc[comparison.cost_bps.eq(cost) & comparison.variant.eq(variant)].itertuples():
                actual = study.metrics(book, control, periods[row.period])
                for key, value in actual.items():
                    if value is None:
                        if not pd.isna(getattr(row,key)):
                            raise ValueError("Expected missing ratio")
                    else:
                        np.testing.assert_allclose(getattr(row,key), value, atol=1e-6, rtol=0)
            saved_day = daily.loc[daily.cost_bps.eq(cost) & daily.variant.eq(variant)].set_index("day").reindex(days)
            np.testing.assert_allclose(study.daily_values(book,days), saved_day.net_pnl_rs, atol=1e-6, rtol=0)
            setup_rows = setups.loc[setups.cost_bps.eq(cost) & setups.variant.eq(variant)]
            if len(setup_rows) != 51:
                raise ValueError("Missing setup")
            np.testing.assert_allclose(setup_rows.net_pnl_rs.sum(), book.portfolio_net_profit_rupees.sum(), atol=1e-6, rtol=0)
            if cost == 5 and variant in ("control", "slot_candle", "session_open"):
                study.source.sealed.parity(book, study.source.base.read_frozen_csv(source/variant/"portfolio_trades.csv"))
            ledgers += 1
    return dict(status="PASS", sessions=len(days), setups=51, orders=len(native), ledgers=ledgers,
        verified_artifacts=len(manifest["files"]), verified_code_files=len(manifest["code_sha256"]),
        governor_decisions_checked=governor_checks, total_decisions=len(decisions),
        native_entries_exits_unchanged=True, exact_existing_filter_parity=True,
        features_confirmed_on_truncated_history=True, allocation_and_cost_accounting=True,
        verification_code_sha256=study.source.base.sha256(Path(__file__)),
        manifest_sha256=study.source.base.sha256(directory/"manifest.json"))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.output and args.output.exists():
        raise ValueError("Require a fresh verification file")
    result = verify(args.directory)
    if args.output:
        study.source.dump(args.output, result)
    print(result)
