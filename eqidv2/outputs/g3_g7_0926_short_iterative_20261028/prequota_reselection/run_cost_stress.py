"""Bounded transaction-cost stress for the frozen 09:26 SHORT audit.

Only the already frozen baseline and wick>=0.20 selections are evaluated.
The 5 bps current model is replayed as a parity control; 9 and 15 bps are the
two requested stresses.  Costs are applied to every filled opportunity in the
complete portfolio.  G7 V4's two-loss lock is recomputed at each cost so that
an after-cost sign change can alter later admissions on the same day.
"""
from __future__ import annotations

from datetime import datetime
import hashlib
import importlib.util
import json
import math
from pathlib import Path
import sys

import numpy as np
import pandas as pd


OUT = Path(__file__).resolve().parent
GRID_SCRIPT = OUT / "run_prequota_reselection.py"
COSTS_BPS = (5.0, 9.0, 15.0)
VARIANTS = ("baseline", "wick_ge_0p20")
SETUP = "0926_SHORT"


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def load_grid_module():
    spec = importlib.util.spec_from_file_location("prequota_grid_audit", GRID_SCRIPT)
    if spec is None or spec.loader is None:
        raise RuntimeError("Cannot load the frozen grid replay module")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def dump_json(path: Path, value) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True, default=str), encoding="utf-8")


def cost_accounting(book: pd.DataFrame, period_days: list[str], grid) -> dict:
    cohort = book.loc[book.day.astype(str).str[:10].isin(set(period_days))].copy()
    executed = grid.bools(cohort.portfolio_executed)
    gross = pd.to_numeric(cohort.portfolio_gross_profit_rupees, errors="coerce").fillna(0.0)
    cost = pd.to_numeric(cohort.portfolio_cost_rupees, errors="coerce").fillna(0.0)
    return {
        "gross_pnl_rs": float(gross.loc[executed].sum()),
        "modeled_cost_rs": float(cost.loc[executed].sum()),
    }


def lock_accounting(book: pd.DataFrame, period_days: list[str], grid) -> dict:
    if "daily_loss_lock_blocked" not in book:
        return {"portfolio_loss_lock_days": 0, "portfolio_loss_lock_blocked_opportunities": 0}
    cohort = book.loc[book.day.astype(str).str[:10].isin(set(period_days))].copy()
    if cohort.daily_loss_lock_blocked.isna().all():
        return {"portfolio_loss_lock_days": 0, "portfolio_loss_lock_blocked_opportunities": 0}
    blocked = grid.bools(cohort.daily_loss_lock_blocked.fillna(False))
    locked = pd.to_datetime(cohort.loss_lock_ts, errors="coerce", utc=True).notna()
    return {
        "portfolio_loss_lock_days": int(cohort.loc[locked, "day"].astype(str).str[:10].nunique()),
        "portfolio_loss_lock_blocked_opportunities": int(blocked.sum()),
    }


def status_sets(book: pd.DataFrame, period_days: list[str], grid) -> dict[str, set[int]]:
    cohort = book.loc[book.day.astype(str).str[:10].isin(set(period_days))].copy()
    if "g8_baseline_filled" in cohort and not cohort.g8_baseline_filled.isna().all():
        native_filled = grid.bools(cohort.g8_baseline_filled.fillna(False))
    else:
        native_filled = grid.bools(cohort.filled.fillna(False))
    final_filled = grid.bools(cohort.filled.fillna(False))
    executed = grid.bools(cohort.portfolio_executed.fillna(False))
    sid = cohort.sid.astype(int)
    return {
        "native_filled": set(sid.loc[native_filled]),
        "final_filled": set(sid.loc[final_filled]),
        "executed": set(sid.loc[executed]),
    }


def reprice_g3(raw: pd.DataFrame, cost_bps: float, allocate_g3, grid) -> pd.DataFrame:
    work = raw.copy(deep=True)
    filled = grid.bools(work.filled)
    gross = pd.to_numeric(work.gross_return_pct, errors="coerce")
    work["cost_pct"] = np.where(filled, float(cost_bps) / 100.0, np.nan)
    work["net_return_pct"] = np.where(filled, gross - float(cost_bps) / 100.0, np.nan)
    return allocate_g3(work)


def assert_metric_parity(actual: dict, expected: pd.Series, label: str) -> dict:
    differences = []
    for field in ("trades", "wins", "losses"):
        if int(actual[field]) != int(expected[field]):
            differences.append(field)
    for field in ("net_pnl_rs", "negative_trade_sum_abs_rs"):
        if not math.isclose(float(actual[field]), float(expected[field]), rel_tol=0.0, abs_tol=1e-6):
            differences.append(field)
    return {"label": label, "passed": not differences, "differences": differences}


def main() -> None:
    grid = load_grid_module()
    from research.g3_0920_backtest import allocate as allocate_g3
    from research import g7_v4_backtest as g7_cache_module
    from research import g7_v4_filtered_backtest as g7_current
    from research.g7_v4_practical_research import execute as execute_g7
    from research.g7_entry_quality_20261026 import verify_members

    protocol = grid.read_json(OUT / "grid_protocol.json")
    rules = {row["variant"]: row for row in protocol["variants"]}
    if set(VARIANTS) - set(rules) or rules["wick_ge_0p20"] != {
            "variant": "wick_ge_0p20", "wick_ratio_min": 0.2}:
        raise ValueError("Frozen wick rule changed")

    details = pd.read_csv(OUT / "variant_0926_trade_details.csv")
    expected_portfolio = pd.read_csv(OUT / "variant_portfolio_metrics.csv")
    expected_setup = pd.read_csv(OUT / "variant_setup_metrics.csv")
    source_provenance = grid.read_json(OUT / "source_provenance.json")
    periods = source_provenance["calendars"]
    period_order = [
        "all", "development", "validation", "assessment", "known_oct26",
        "final_oct27", "post_development", "october",
    ]
    if set(period_order) != set(periods):
        raise ValueError("Unexpected source chronology")

    g3_saved = pd.read_csv(grid.G3_RUN / "portfolio_trades.csv")
    g7_saved = pd.read_csv(grid.G7_LOCK / "portfolio_trades.csv")
    base_config = None

    g7_settings = grid.read_json(g7_current.CONFIG)
    g7_current.validate_settings(g7_settings)
    g7_native, _, _, g7_cache_paths, g7_proof = g7_cache_module.load_cache(grid.G7_CACHE)
    base_config = g7_proof["base_config"]
    feature_source = Path(g7_settings["source_research"]["output"]).resolve()
    verify_members(feature_source, g7_settings["source_research"]["manifest_sha256"])
    g7_features = pd.read_csv(feature_source / "entry_features_and_decisions.csv").set_index("sid", drop=False)
    if set(g7_features.index.astype(int)) != set(g7_native.sid.astype(int)):
        raise ValueError("G7 feature inventory differs from selected opportunities")
    g7_risk = g7_current.model.decisions(g7_features)["low_efficiency"]
    g7_nifty = grid.bools(g7_features.allowed_g7_v2)
    g7_allowed_current = g7_nifty & ~g7_risk
    g7_raw_current = g7_current.previous.replay(
        g7_native, g7_cache_paths, base_config, g7_current.protection_settings(g7_settings)
    )

    raw_books: dict[str, dict[str, tuple[pd.DataFrame, pd.Series | None, pd.DataFrame]]] = {
        "G3": {}, "G7_V4_LOCK2": {},
    }
    for variant in VARIANTS:
        g3_slot = details.loc[
            details.audit_strategy.eq("G3") & details.audit_variant.eq(variant)
        ].copy()
        g7_slot = details.loc[
            details.audit_strategy.eq("G7_V4_LOCK2") & details.audit_variant.eq(variant)
        ].copy()
        if variant == "baseline":
            g3_raw = g3_saved.copy(deep=True)
            g7_raw = g7_raw_current.copy(deep=True)
            g7_allowed = g7_allowed_current.copy()
        else:
            g3_raw = pd.concat([
                g3_saved.loc[~g3_saved.setup_id.eq(SETUP)].copy(), g3_slot,
            ], ignore_index=True, sort=False)
            g7_raw = pd.concat([
                g7_raw_current.loc[~g7_raw_current.setup_id.eq(SETUP)].copy(), g7_slot,
            ], ignore_index=True, sort=False)
            other_sids = g7_native.loc[~g7_native.setup_id.eq(SETUP), "sid"].astype(int)
            slot_allowed = pd.Series(
                grid.bools(g7_slot.base_entry_allowed).to_numpy(bool),
                index=g7_slot.sid.astype(int),
            )
            g7_allowed = pd.concat([g7_allowed_current.loc[other_sids], slot_allowed])
        for label, raw in (("G3", g3_raw), ("G7", g7_raw)):
            if raw.sid.duplicated().any() or raw.signal_id.duplicated().any():
                raise ValueError(f"{label} {variant} raw identities overlap")
        if set(g7_allowed.index.astype(int)) != set(g7_raw.sid.astype(int)):
            raise ValueError(f"G7 {variant} permission inventory differs from raw book")
        raw_books["G3"][variant] = (g3_raw, None, g3_slot)
        raw_books["G7_V4_LOCK2"][variant] = (g7_raw, g7_allowed, g7_slot)

    books: dict[tuple[str, str, float], pd.DataFrame] = {}
    for cost_bps in COSTS_BPS:
        for variant in VARIANTS:
            g3_raw, _, _ = raw_books["G3"][variant]
            books[("G3", variant, cost_bps)] = reprice_g3(
                g3_raw, cost_bps, allocate_g3, grid
            )
            g7_raw, g7_allowed, _ = raw_books["G7_V4_LOCK2"][variant]
            books[("G7_V4_LOCK2", variant, cost_bps)] = execute_g7(
                dict(raw=g7_raw, allowed=g7_allowed, integer=True, clock=True, limit=2),
                base_config, f"cost_stress_{variant}", cost=cost_bps,
            )

    parity = [
        grid.same_execution(
            books[("G3", "baseline", 5.0)], g3_saved,
            "G3 5bps full-portfolio baseline control",
        ),
        grid.same_execution(
            books[("G7_V4_LOCK2", "baseline", 5.0)], g7_saved,
            "G7 V4 lock2 5bps full-portfolio baseline control",
        ),
    ]
    for strategy in ("G3", "G7_V4_LOCK2"):
        for variant in VARIANTS:
            book = books[(strategy, variant, 5.0)]
            slot = raw_books[strategy][variant][2]
            parity.append(grid.same_execution(
                book.loc[book.setup_id.eq(SETUP)].copy(), slot,
                f"{strategy} {variant} 5bps slot control",
            ))
            for scope, source, expected_table in (
                    ("0926_SHORT", book.loc[book.setup_id.eq(SETUP)].copy(), expected_setup),
                    ("full_portfolio", book, expected_portfolio)):
                actual = grid.metrics(source, periods["all"], slot if scope == "0926_SHORT" else None)
                expected = expected_table.loc[
                    expected_table.strategy.eq(strategy)
                    & expected_table.variant.eq(variant)
                    & expected_table.period.eq("all")
                ].iloc[0]
                parity.append(assert_metric_parity(
                    actual, expected, f"{strategy} {variant} 5bps {scope} metrics",
                ))
    if not all(row["passed"] for row in parity):
        dump_json(OUT / "cost_stress_provenance.json", {"parity": parity, "passed": False})
        raise ValueError("5 bps controls failed before cost stress")

    rows = []
    for (strategy, variant, cost_bps), book in books.items():
        slot = raw_books[strategy][variant][2]
        for scope, source, selected in (
                ("0926_SHORT", book.loc[book.setup_id.eq(SETUP)].copy(), slot),
                ("full_portfolio", book, None)):
            reference_book = books[(strategy, variant, 5.0)]
            reference_source = (reference_book.loc[reference_book.setup_id.eq(SETUP)].copy()
                                if scope == "0926_SHORT" else reference_book)
            for period in period_order:
                period_days = periods[period]
                current_status = status_sets(source, period_days, grid)
                reference_status = status_sets(reference_source, period_days, grid)
                row = {
                    "strategy": strategy,
                    "variant": variant,
                    "cost_bps_round_trip": cost_bps,
                    "cost_5bps_reference": cost_bps == 5.0,
                    "scope": scope,
                    "period": period,
                    **grid.metrics(source, period_days, selected),
                    **cost_accounting(source, period_days, grid),
                    **lock_accounting(book, period_days, grid),
                    "native_filled_opportunities": len(current_status["native_filled"]),
                    "final_filled_opportunities": len(current_status["final_filled"]),
                    "native_filled_sids_added_vs_5bps": len(
                        current_status["native_filled"] - reference_status["native_filled"]
                    ),
                    "native_filled_sids_removed_vs_5bps": len(
                        reference_status["native_filled"] - current_status["native_filled"]
                    ),
                    "final_filled_sids_added_vs_5bps": len(
                        current_status["final_filled"] - reference_status["final_filled"]
                    ),
                    "final_filled_sids_removed_vs_5bps": len(
                        reference_status["final_filled"] - current_status["final_filled"]
                    ),
                    "executed_sids_added_vs_5bps": len(
                        current_status["executed"] - reference_status["executed"]
                    ),
                    "executed_sids_removed_vs_5bps": len(
                        reference_status["executed"] - current_status["executed"]
                    ),
                }
                rows.append(row)
    result = pd.DataFrame(rows)
    keys = ["strategy", "variant", "scope", "period"]
    reference = result.loc[result.cost_bps_round_trip.eq(5.0), keys + [
        "trades", "wins", "losses", "net_pnl_rs", "negative_trade_sum_abs_rs",
    ]].rename(columns={
        "trades": "trades_at_5bps", "wins": "wins_at_5bps", "losses": "losses_at_5bps",
        "net_pnl_rs": "net_pnl_at_5bps_rs",
        "negative_trade_sum_abs_rs": "negative_trade_sum_abs_at_5bps_rs",
    })
    result = result.merge(reference, on=keys, how="left", validate="many_to_one")
    same_cost_baseline = result.loc[result.variant.eq("baseline"), [
        "strategy", "cost_bps_round_trip", "scope", "period", "trades", "wins",
        "net_pnl_rs", "negative_trade_sum_abs_rs",
    ]].rename(columns={
        "trades": "baseline_same_cost_trades", "wins": "baseline_same_cost_wins",
        "net_pnl_rs": "baseline_same_cost_net_pnl_rs",
        "negative_trade_sum_abs_rs": "baseline_same_cost_negative_trade_sum_abs_rs",
    })
    result = result.merge(
        same_cost_baseline,
        on=["strategy", "cost_bps_round_trip", "scope", "period"],
        how="left", validate="many_to_one",
    )
    result["trade_delta_vs_5bps"] = result.trades - result.trades_at_5bps
    result["win_delta_vs_5bps"] = result.wins - result.wins_at_5bps
    result["loss_delta_vs_5bps"] = result.losses - result.losses_at_5bps
    result["net_delta_vs_5bps_rs"] = result.net_pnl_rs - result.net_pnl_at_5bps_rs
    result["loss_amount_increase_vs_5bps_rs"] = (
        result.negative_trade_sum_abs_rs - result.negative_trade_sum_abs_at_5bps_rs
    )
    result["net_delta_vs_baseline_same_cost_rs"] = (
        result.net_pnl_rs - result.baseline_same_cost_net_pnl_rs
    )
    result["loss_amount_reduction_vs_baseline_same_cost_rs"] = (
        result.baseline_same_cost_negative_trade_sum_abs_rs - result.negative_trade_sum_abs_rs
    )
    result["trade_retention_vs_baseline_same_cost_pct"] = np.where(
        result.baseline_same_cost_trades.gt(0),
        100.0 * result.trades / result.baseline_same_cost_trades, np.nan,
    )
    result["winner_retention_vs_baseline_same_cost_pct"] = np.where(
        result.baseline_same_cost_wins.gt(0),
        100.0 * result.wins / result.baseline_same_cost_wins, np.nan,
    )
    result = result.sort_values(
        ["strategy", "scope", "period", "variant", "cost_bps_round_trip"], kind="stable"
    ).reset_index(drop=True)
    metrics_path = OUT / "cost_stress_metrics.csv"
    result.to_csv(metrics_path, index=False)

    lock_rows = []
    for (strategy, variant, cost_bps), book in books.items():
        if strategy != "G7_V4_LOCK2":
            continue
        for day, cohort in book.groupby(book.day.astype(str).str[:10], sort=True):
            lock_values = pd.to_datetime(cohort.loss_lock_ts, errors="coerce", utc=True).dropna().unique()
            blocked = grid.bools(cohort.daily_loss_lock_blocked)
            if not len(lock_values) and not blocked.any():
                continue
            executed = grid.bools(cohort.portfolio_executed)
            pnl = pd.to_numeric(cohort.portfolio_net_profit_rupees, errors="coerce").fillna(0.0)
            blocked_rows = cohort.loc[blocked]
            lock_rows.append({
                "strategy": strategy,
                "variant": variant,
                "cost_bps_round_trip": cost_bps,
                "day": day,
                "loss_lock_ts": str(lock_values[0]) if len(lock_values) else None,
                "executed_trades_after_lock": int(executed.sum()),
                "executed_net_losses_after_cost": int((executed & pnl.lt(-1e-8)).sum()),
                "blocked_opportunities": int(blocked.sum()),
                "blocked_sids": "|".join(blocked_rows.sid.astype(str)),
                "blocked_symbols": "|".join(blocked_rows.tradingsymbol.astype(str)),
            })
    locks = pd.DataFrame(lock_rows)
    if len(locks):
        reference_locks = {
            (row.variant, row.day): set(str(row.blocked_sids).split("|")) if row.blocked_sids else set()
            for row in locks.loc[locks.cost_bps_round_trip.eq(5.0)].itertuples()
        }
        added, removed = [], []
        for row in locks.itertuples():
            current = set(str(row.blocked_sids).split("|")) if row.blocked_sids else set()
            control = reference_locks.get((row.variant, row.day), set())
            added.append("|".join(sorted(current - control)))
            removed.append("|".join(sorted(control - current)))
        locks["blocked_sids_added_vs_5bps"] = added
        locks["blocked_sids_removed_vs_5bps"] = removed
    locks_path = OUT / "cost_stress_g7_lock_events.csv"
    locks.to_csv(locks_path, index=False)

    input_files = [
        Path(__file__), GRID_SCRIPT, OUT / "grid_protocol.json",
        OUT / "variant_0926_trade_details.csv", OUT / "variant_setup_metrics.csv",
        OUT / "variant_portfolio_metrics.csv", OUT / "source_provenance.json",
        grid.G3_RUN / "portfolio_trades.csv", grid.G7_LOCK / "portfolio_trades.csv",
        grid.G7_CACHE / "manifest.json", grid.G7_CACHE / "minute_paths.npz",
        feature_source / "manifest.json", feature_source / "entry_features_and_decisions.csv",
        Path(g7_current.CONFIG), Path(allocate_g3.__module__.replace(".", "/") + ".py"),
    ]
    # Resolve the imported allocator's source robustly; the module-name path above is
    # replaced before hashing and is never treated as provenance on its own.
    import inspect
    input_files[-1] = Path(inspect.getsourcefile(allocate_g3)).resolve()
    provenance = {
        "created_at_ist": datetime.now().astimezone().isoformat(),
        "scope": "BACKTEST_ONLY",
        "execution_authority": False,
        "active_configs_modified": False,
        "setup_id": SETUP,
        "variants": [
            {"variant": "baseline", "rule": {}},
            {"variant": "wick_ge_0p20", "rule": {"wick_ratio_min": 0.20}},
        ],
        "costs_bps_round_trip": list(COSTS_BPS),
        "stress_costs_bps_round_trip": [9.0, 15.0],
        "reference_cost_bps_round_trip": 5.0,
        "grid_frozen_before_outcomes": True,
        "new_thresholds_tested": False,
        "candidate_selection_recomputed_at_cost": False,
        "cost_application": "Absolute round-trip cost applied to every filled opportunity in the complete strategy portfolio before allocation/accounting.",
        "g3_method": "Set cost_pct=cost_bps/100 and net_return_pct=gross_return_pct-cost_pct for every filled raw opportunity, then rerun the complete G3 allocator.",
        "g7_method": "Keep gross path exits and causal entry permissions fixed, then rerun integer sizing, complete portfolio allocation, and the two-net-loss daily lock independently at each cost.",
        "g7_loss_lock_recomputed_at_each_cost": True,
        "five_bps_parity": parity,
        "five_bps_parity_passed": all(row["passed"] for row in parity),
        "calendars": periods,
        "inputs_and_code": {str(path.resolve()): sha256(path) for path in input_files},
        "outputs": {
            metrics_path.name: sha256(metrics_path),
            locks_path.name: sha256(locks_path),
        },
    }
    provenance_path = OUT / "cost_stress_provenance.json"
    dump_json(provenance_path, provenance)

    generated = [path for path in OUT.iterdir() if path.is_file() and path.name != "artifact_manifest.json"]
    grid.dump_json(OUT / "artifact_manifest.json", {
        "created_at_ist": datetime.now().astimezone().isoformat(),
        "scope": "BACKTEST_ONLY",
        "execution_authority": False,
        "files": {path.name: sha256(path) for path in sorted(generated)},
    })

    shown = result.loc[
        result.period.eq("all") & result.cost_bps_round_trip.isin((9.0, 15.0)),
        ["strategy", "variant", "cost_bps_round_trip", "scope", "trades", "wins", "losses",
         "win_rate_pct", "net_pnl_rs", "negative_trade_sum_abs_rs",
         "net_delta_vs_5bps_rs", "net_delta_vs_baseline_same_cost_rs",
         "portfolio_loss_lock_days", "portfolio_loss_lock_blocked_opportunities",
         "native_filled_sids_removed_vs_5bps", "final_filled_sids_removed_vs_5bps",
         "executed_sids_removed_vs_5bps"],
    ]
    print(shown.to_string(index=False), flush=True)
    print("COST STRESS OUTPUT", OUT, flush=True)


if __name__ == "__main__":
    main()
