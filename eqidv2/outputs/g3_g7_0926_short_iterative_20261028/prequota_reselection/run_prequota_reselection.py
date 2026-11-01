"""Frozen-grid, replacement-aware 09:26 SHORT audit for G3 and G7 V4 lock2.

This script is backtest-only.  It reconstructs the sealed pre-quota candidate
pool, applies every grid rule before max-volume ranking and the two-entry quota,
freshly simulates any replacement orders from one-minute paths, and reallocates
the complete G3 or G7 portfolio.  It never writes an active configuration.
"""
from __future__ import annotations

from dataclasses import replace
from datetime import date, datetime
import hashlib
import json
import math
from pathlib import Path
import sys

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
OUT = Path(__file__).resolve().parent
PROTOCOL = OUT / "grid_protocol.json"
SETUP = "0926_SHORT"

G3_RUN = Path(
    r"C:/TradingData/eqidv2/backtesting_result_v13_v10_g_3/backtests/runs/2026-10-27/"
    r"20261027T172043456343_3d50ca01_current_full_history"
)
G7_CACHE = Path(
    r"C:/TradingData/eqidv2/backtesting_result_v13_v10_g_7/v4_research/runs/2026-10-27/"
    r"20261027T165517683392_v4_extremes/inputs"
)
G7_LOCK = Path(
    r"C:/TradingData/eqidv2/backtesting_result_v13_v10_g_7/v4_research/practical_research/"
    r"runs/2026-10-27/20261027T185748273567_five_fixed_tests/loss_lock2"
)


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def read_json(path: Path):
    return json.loads(Path(path).read_text(encoding="utf-8-sig"))


def dump_json(path: Path, value) -> None:
    Path(path).write_text(json.dumps(value, indent=2, sort_keys=True, default=str), encoding="utf-8")


def bools(values: pd.Series) -> pd.Series:
    if pd.api.types.is_bool_dtype(values):
        return values.fillna(False).astype(bool)
    text = values.astype(str).str.strip().str.lower()
    if not text.isin(("true", "false")).all():
        bad = sorted(text.loc[~text.isin(("true", "false"))].unique())[:5]
        raise ValueError(f"Invalid boolean values: {bad}")
    return text.eq("true")


def key_frame(frame: pd.DataFrame) -> set[tuple[str, str, str]]:
    return set(zip(frame.day.astype(str).str[:10], frame.setup_id.astype(str), frame.tradingsymbol.astype(str)))


def same_execution(actual: pd.DataFrame, expected: pd.DataFrame, label: str) -> dict:
    left = actual.sort_values("sid").reset_index(drop=True)
    right = expected.sort_values("sid").reset_index(drop=True)
    result = {"label": label, "rows": len(left), "passed": True, "differences": []}
    if len(left) != len(right) or set(left.sid.astype(int)) != set(right.sid.astype(int)):
        result.update(passed=False, differences=["sid_inventory"])
        return result
    right = right.set_index(right.sid.astype(int)).loc[left.sid.astype(int)].reset_index(drop=True)
    for column in ("filled", "portfolio_executed"):
        if column in left and column in right and not np.array_equal(bools(left[column]), bools(right[column])):
            result["differences"].append(column)
    for column in ("entry_price", "exit_price", "gross_return_pct", "net_return_pct",
                   "portfolio_net_profit_rupees"):
        if column in left and column in right:
            a = pd.to_numeric(left[column], errors="coerce").to_numpy(float)
            b = pd.to_numeric(right[column], errors="coerce").to_numpy(float)
            if not np.allclose(a, b, rtol=0, atol=1e-6, equal_nan=True):
                result["differences"].append(column)
    for column in ("entry_ts", "exit_ts"):
        if column in left and column in right:
            a = pd.to_datetime(left[column], errors="coerce", utc=True)
            b = pd.to_datetime(right[column], errors="coerce", utc=True)
            if not a.equals(b):
                result["differences"].append(column)
    result["passed"] = not result["differences"]
    return result


def wilson_lower(wins: int, trades: int, z: float = 1.959963984540054) -> float:
    if trades <= 0:
        return math.nan
    p = wins / trades
    denominator = 1 + z * z / trades
    center = p + z * z / (2 * trades)
    spread = z * math.sqrt(p * (1 - p) / trades + z * z / (4 * trades * trades))
    return 100 * (center - spread) / denominator


def metrics(book: pd.DataFrame, days: list[str], selected: pd.DataFrame | None = None) -> dict:
    selected_days = set(days)
    subset = book.loc[book.day.astype(str).str[:10].isin(selected_days)].copy()
    executed = bools(subset.portfolio_executed)
    pnl = pd.to_numeric(subset.portfolio_net_profit_rupees, errors="coerce").fillna(0.0)
    wins = int((executed & pnl.gt(1e-8)).sum())
    losses = int((executed & pnl.lt(-1e-8)).sum())
    trades = int(executed.sum())
    day_pnl = subset.loc[executed].assign(_pnl=pnl.loc[executed]).groupby(
        subset.loc[executed, "day"].astype(str).str[:10], sort=True
    )._pnl.sum().reindex(days, fill_value=0.0)
    equity = day_pnl.cumsum()
    drawdown = equity.cummax().sub(equity)
    selected_count = None
    if selected is not None:
        selected_count = int(selected.day.astype(str).str[:10].isin(selected_days).sum())
    return {
        "sessions": len(days),
        "selected_opportunities": selected_count,
        "trades": trades,
        "wins": wins,
        "losses": losses,
        "flats": trades - wins - losses,
        "win_rate_pct": 100 * wins / trades if trades else math.nan,
        "wilson_95_lower_pct": wilson_lower(wins, trades),
        "win_rate_gt_75": bool(trades and wins / trades > 0.75),
        "net_pnl_rs": float(pnl.loc[executed].sum()),
        "negative_trade_sum_abs_rs": float(-pnl.loc[executed & pnl.lt(0)].sum()),
        "daily_close_drawdown_rs": float(drawdown.max()) if len(drawdown) else 0.0,
    }


def filter_variant(frame: pd.DataFrame, rule: dict) -> pd.Series:
    allowed = pd.Series(True, index=frame.index)
    if "extension_atr_max" in rule:
        allowed &= pd.to_numeric(frame.research_extension_atr, errors="coerce").le(rule["extension_atr_max"])
    if "downward_move_magnitude_pct_max" in rule:
        allowed &= pd.to_numeric(frame.price_change_pct, errors="coerce").ge(
            -float(rule["downward_move_magnitude_pct_max"])
        )
    if "body_ratio_max" in rule:
        allowed &= pd.to_numeric(frame.body_ratio, errors="coerce").le(rule["body_ratio_max"])
    if "wick_ratio_min" in rule:
        allowed &= pd.to_numeric(frame.wick_ratio, errors="coerce").ge(rule["wick_ratio_min"])
    return allowed


def select_variant(pool: pd.DataFrame, strategy: str, rule: dict, production,
                   current_ids: dict[tuple[str, str, str], int]) -> tuple[pd.DataFrame, pd.DataFrame]:
    base_setup = production._ACTIVE_BY_ID[SETUP]
    setup = base_setup if strategy == "G3" else replace(
        base_setup, stop_pct=1.2, target_pct=2.0, source_version="G7_V4_LOCK2_PREQUOTA_AUDIT"
    )
    decision_parts, selected_parts = [], []
    for day_value, source in pool.groupby(pool.day.astype(str).str[:10], sort=True):
        day = date.fromisoformat(day_value)
        work = source.copy()
        base_pass = pd.Series([
            production.passes_selected_filters(row, setup, session_date=day)
            for row in work.to_dict("records")
        ], index=work.index)
        if strategy == "G7_V4_LOCK2":
            base_pass &= pd.to_numeric(work.research_extension_atr, errors="coerce").le(4.0)
        variant_pass = base_pass & filter_variant(work, rule)
        eligible = work.loc[variant_pass].copy()
        chosen = production.rank_candidates(eligible.to_dict("records"), setup, session_date=day)
        chosen_symbols = {str(row["tradingsymbol"]) for row in chosen}
        ranked = sorted(eligible.to_dict("records"), key=lambda row: production._rank_key(row, setup))
        ranks = {str(row["tradingsymbol"]): rank for rank, row in enumerate(ranked, 1)}
        audit = work[["sid", "day", "setup_id", "tradingsymbol", "price_change_pct", "oi_change_pct",
                      "volume_ratio", "body_ratio", "wick_ratio", "research_extension_atr"]].copy()
        audit["strategy"] = strategy
        audit["variant"] = rule["variant"]
        audit["base_gate_pass"] = base_pass.to_numpy(bool)
        audit["variant_gate_pass"] = variant_pass.to_numpy(bool)
        audit["rank_after_variant"] = audit.tradingsymbol.map(ranks).astype("Int64")
        audit["selected"] = audit.tradingsymbol.astype(str).isin(chosen_symbols)
        decision_parts.append(audit)
        if chosen:
            selected_parts.append(pd.DataFrame(chosen))
    selected = pd.concat(selected_parts, ignore_index=True, sort=False) if selected_parts else pool.iloc[0:0].copy()
    selected["day"] = selected.day.astype(str).str[:10]
    selected["setup_id"] = SETUP
    selected["native_stop_pct"] = setup.stop_pct
    selected["native_target_pct"] = setup.target_pct
    selected["stop_pct"] = setup.stop_pct
    selected["target_pct"] = setup.target_pct
    selected["initial_stop_pct"] = 1.2 if strategy == "G7_V4_LOCK2" else production.INITIAL_STOP_PCT
    selected["tightened_stop_pct"] = 1.0 if strategy == "G7_V4_LOCK2" else production.TIGHTENED_STOP_PCT
    selected["tighten_after_minutes"] = 120 if strategy == "G7_V4_LOCK2" else production.TIGHTEN_AFTER_MINUTES
    selected["configured_confirmation_end"] = "09:26"
    selected["picker"] = "max_volume"
    selected["max_entries"] = 2
    selected["v9_selected"] = True
    selected["v9_filter_pass"] = True
    selected["strategy_version"] = "G3_PREQUOTA_AUDIT" if strategy == "G3" else "G7_V4_LOCK2_PREQUOTA_AUDIT"
    selected["strategy_fingerprint"] = hashlib.sha256(
        (strategy + ":" + rule["variant"]).encode("utf-8")
    ).hexdigest()
    ids = []
    signals = []
    for row_number, row in enumerate(selected.itertuples(), 1):
        key = (str(row.day)[:10], SETUP, str(row.tradingsymbol))
        sid = current_ids.get(key)
        if sid is None:
            sid = (190_000_000 if strategy == "G3" else 200_000_000) + int(row.sid)
        ids.append(int(sid))
        signals.append(f"audit:{strategy}:{row.day}:{SETUP}:{row.tradingsymbol}")
    selected["sid"] = ids
    selected["signal_id"] = signals
    if selected.sid.duplicated().any() or selected.signal_id.duplicated().any():
        raise ValueError(f"Duplicate selected identity for {strategy} {rule['variant']}")
    return selected.reset_index(drop=True), pd.concat(decision_parts, ignore_index=True, sort=False)


def selection_changes(selected_by_variant: dict[str, pd.DataFrame], strategy: str) -> pd.DataFrame:
    baseline = key_frame(selected_by_variant["baseline"])
    rows = []
    for variant, selected in selected_by_variant.items():
        current = key_frame(selected)
        for key in sorted(baseline | current):
            rows.append({
                "strategy": strategy,
                "variant": variant,
                "day": key[0],
                "setup_id": key[1],
                "tradingsymbol": key[2],
                "change_vs_baseline": "RETAINED" if key in baseline and key in current else
                                      "ADDED" if key in current else "REMOVED",
            })
    return pd.DataFrame(rows)


def main() -> None:
    from research import g4_g3_mirror_backtest as pool_builder
    from research import g3_target_cutoff_contract as g3_identity
    from research.g3_target_cutoff_backtest import _simulate
    from research.g3_0920_backtest import allocate as allocate_g3
    import fno_v13_v10_g_3_contract as production
    import fno_v13_v10_g_2_backtest as native_simulator
    from research import g7_v4_backtest as g7_cache_module
    from research import g7_v4_filtered_backtest as g7_current
    from research.g7_v4_practical_research import execute as execute_g7
    from research.g7_entry_quality_20261026 import verify_members

    protocol = read_json(PROTOCOL)
    variants = protocol["variants"]
    if [row["variant"] for row in variants] != [
        "baseline", "extension_le_3p5", "extension_le_3p0", "extension_le_2p5",
        "down_move_le_0p75", "down_move_le_0p50", "body_le_0p85", "wick_ge_0p20",
        "extension_le_3p0_and_down_move_le_0p75", "extension_le_3p0_and_wick_ge_0p20",
    ]:
        raise ValueError("Frozen grid changed")

    print("Loading sealed current ledgers", flush=True)
    g3_summary = read_json(G3_RUN / "summary.json")
    g3_orders = pd.read_csv(G3_RUN / "selected_orders.csv")
    g3_saved = pd.read_csv(G3_RUN / "portfolio_trades.csv")
    days = [str(value) for value in g3_summary["days"]]
    if len(days) != 59 or days[0] != "2026-07-29" or days[-1] != "2026-10-27":
        raise ValueError("Unexpected G3 calendar")

    print("Loading hash-verified G7 V4 feature and selected-opportunity cache", flush=True)
    g7_settings = read_json(g7_current.CONFIG)
    g7_current.validate_settings(g7_settings)
    g7_native, _, _, g7_cache_paths, g7_proof = g7_cache_module.load_cache(G7_CACHE)
    feature_source = Path(g7_settings["source_research"]["output"]).resolve()
    verify_members(feature_source, g7_settings["source_research"]["manifest_sha256"])
    feature_summary = read_json(feature_source / "summary.json")
    if Path(feature_summary["cache"]).resolve() != G7_CACHE.resolve():
        raise ValueError("G7 V4 sealed features refer to a different opportunity cache")
    g7_features = pd.read_csv(feature_source / "entry_features_and_decisions.csv").set_index("sid", drop=False)
    if set(g7_features.index.astype(int)) != set(g7_native.sid.astype(int)):
        raise ValueError("G7 V4 feature inventory differs from the selected opportunities")
    g7_saved = pd.read_csv(G7_LOCK / "portfolio_trades.csv")
    if g7_proof["periods"]["all"] != days:
        raise ValueError("G3/G7 calendars differ")
    periods = dict(g7_proof["periods"])
    periods["post_development"] = [d for d in days if d not in set(periods["development"])]
    periods["october"] = [d for d in days if d.startswith("2026-10")]

    prices = pool_builder.Prices(g3_summary)
    # Archived source columns intentionally retain mixed timestamp encodings.
    # Pickle preserves those exact values; Arrow/Parquet tries to coerce them.
    pool_cache = OUT / "candidate_pool_full.pkl"
    proof_path = OUT / "candidate_pool_proof.json"
    if pool_cache.is_file() and proof_path.is_file():
        print("Using cached reconstructed candidate pool", flush=True)
        pool = pd.read_pickle(pool_cache)
        pool_proof = read_json(proof_path)
    else:
        print("Reconstructing and enriching the complete 59-session 09:26 pre-quota pool", flush=True)
        pool, pool_proof = pool_builder.candidate_pool(
            g3_summary, g3_orders, prices, {"frozen_grid": {SETUP: 4.0}}
        )
        print(f"Reconstructed raw pool: {len(pool)} rows", flush=True)
        pool.to_pickle(pool_cache)
        dump_json(proof_path, pool_proof)
    if set(pool.setup_id.astype(str)) != {SETUP}:
        raise ValueError("Candidate pool escaped 09:26 SHORT")
    if not np.isfinite(pd.to_numeric(pool.research_extension_atr, errors="coerce")).all():
        raise ValueError("Candidate extension feature is incomplete")
    print(f"Candidate pool ready: {len(pool)} rows, {pool.day.astype(str).str[:10].nunique()} candidate days", flush=True)

    current_g3 = g3_orders.loc[g3_orders.setup_id.eq(SETUP)].copy()
    current_g7 = g7_native.loc[g7_native.setup_id.eq(SETUP)].copy()
    g3_ids = {(str(r.day)[:10], SETUP, str(r.tradingsymbol)): int(r.sid) for r in current_g3.itertuples()}
    g7_ids = {(str(r.day)[:10], SETUP, str(r.tradingsymbol)): int(r.sid) for r in current_g7.itertuples()}

    selections: dict[str, dict[str, pd.DataFrame]] = {"G3": {}, "G7_V4_LOCK2": {}}
    decision_frames = []
    for strategy, ids in (("G3", g3_ids), ("G7_V4_LOCK2", g7_ids)):
        for rule in variants:
            selected, decisions = select_variant(pool, strategy, rule, production, ids)
            selections[strategy][rule["variant"]] = selected
            decision_frames.append(decisions)
    if key_frame(selections["G3"]["baseline"]) != key_frame(current_g3):
        raise ValueError("G3 baseline pre-quota selection did not reproduce current selected orders")
    if key_frame(selections["G7_V4_LOCK2"]["baseline"]) != key_frame(current_g7):
        raise ValueError("G7 extension<=4 pre-quota selection did not reproduce current selected opportunities")

    candidate_cols = [
        "sid", "day", "setup_id", "tradingsymbol", "side", "confirmation_ts", "trigger",
        "price_change_pct", "oi_change_pct", "volume_ratio", "body_ratio", "wick_ratio",
        "traded_value", "v9_1m_volume_ratio", "nifty_first_bar_return_pct",
        "research_ema20", "research_atr14", "research_extension_atr", "research_feature_source",
    ]
    inventory = pool[[column for column in candidate_cols if column in pool]].copy()
    inventory["g3_base_gate_pass"] = [
        production.passes_selected_filters(row, production._ACTIVE_BY_ID[SETUP],
                                           session_date=date.fromisoformat(str(row["day"])[:10]))
        for row in pool.to_dict("records")
    ]
    inventory["g7_base_gate_pass"] = inventory.g3_base_gate_pass & pd.to_numeric(
        inventory.research_extension_atr, errors="coerce"
    ).le(4.0)
    inventory.to_csv(OUT / "candidate_pool_inventory.csv", index=False)
    pd.concat(decision_frames, ignore_index=True, sort=False).to_csv(
        OUT / "candidate_variant_decisions.csv", index=False
    )
    pd.concat([
        selection_changes(selections["G3"], "G3"),
        selection_changes(selections["G7_V4_LOCK2"], "G7_V4_LOCK2"),
    ], ignore_index=True).to_csv(OUT / "selection_changes.csv", index=False)

    print("Loading union of newly selected one-minute paths", flush=True)
    union_orders = {}
    union_paths = {}
    for strategy in selections:
        merged = pd.concat(list(selections[strategy].values()), ignore_index=True, sort=False)
        merged = merged.drop_duplicates("sid", keep="first").reset_index(drop=True)
        union_orders[strategy] = merged
        union_paths[strategy] = prices.paths(merged)

    parity = []
    g3_control = allocate_g3(g3_saved.copy())
    parity.append(same_execution(g3_control, g3_saved, "G3 full-portfolio allocator control"))

    base_config = g7_proof["base_config"]
    g7_risk = g7_current.model.decisions(g7_features)["low_efficiency"]
    g7_nifty = bools(g7_features.allowed_g7_v2)
    g7_allowed_current = g7_nifty & ~g7_risk
    g7_raw_current = g7_current.previous.replay(
        g7_native, g7_cache_paths, base_config, g7_current.protection_settings(g7_settings)
    )
    g7_control = execute_g7(
        dict(raw=g7_raw_current, allowed=g7_allowed_current, integer=True, clock=True, limit=2),
        base_config, "loss_lock2", cost=5.0,
    )
    parity.append(same_execution(g7_control, g7_saved, "G7 V4 lock2 full-portfolio control"))
    if not all(row["passed"] for row in parity):
        dump_json(OUT / "baseline_reproduction.json", parity)
        raise ValueError("Published full-portfolio controls did not reproduce")

    setup_rows, portfolio_rows, trade_details = [], [], []
    books: dict[str, dict[str, pd.DataFrame]] = {"G3": {}, "G7_V4_LOCK2": {}}
    for strategy in ("G3", "G7_V4_LOCK2"):
        for rule in variants:
            variant = rule["variant"]
            orders = selections[strategy][variant]
            paths = {
                int(sid): (g7_cache_paths[int(sid)] if strategy == "G7_V4_LOCK2" and int(sid) in g7_cache_paths
                           else union_paths[strategy][int(sid)])
                for sid in orders.sid
            }
            if strategy == "G3":
                native_slot = _simulate(orders, paths, [date.fromisoformat(d) for d in days], g3_identity)
                combined = pd.concat([
                    g3_saved.loc[~g3_saved.setup_id.eq(SETUP)].copy(), native_slot,
                ], ignore_index=True, sort=False)
                book = allocate_g3(combined)
            else:
                native_slot = native_simulator.g.v9.v5.simulate_native(
                    orders.copy(), paths,
                    cost_bps=float(base_config["model_cost_bps_round_trip"]),
                    max_entry_delay_minutes=int(production.ENTRY_EXPIRY_MINUTES),
                )
                protected_slot = g7_current.previous.replay(
                    native_slot, paths, base_config, g7_current.protection_settings(g7_settings)
                )
                raw = pd.concat([
                    g7_raw_current.loc[~g7_raw_current.setup_id.eq(SETUP)].copy(), protected_slot,
                ], ignore_index=True, sort=False)
                other_sids = g7_native.loc[~g7_native.setup_id.eq(SETUP), "sid"].astype(int)
                old_allowed = g7_allowed_current.loc[other_sids].copy()
                # At 09:26 the low-efficiency filter has fewer than 30 session bars and preserves
                # every entry.  For this first five-minute slot, G7's slot and session Nifty
                # directions are identical; SHORT permission is close<=09:20 open, represented
                # by the sealed first-bar return available on each candidate.
                slot_allowed = pd.Series(
                    pd.to_numeric(orders.nifty_first_bar_return_pct, errors="coerce").le(0).to_numpy(bool),
                    index=orders.sid.astype(int),
                )
                allowed = pd.concat([old_allowed, slot_allowed])
                if allowed.index.duplicated().any() or set(allowed.index.astype(int)) != set(raw.sid.astype(int)):
                    raise ValueError("G7 permission inventory differs from replacement-aware raw book")
                book = execute_g7(
                    dict(raw=raw, allowed=allowed, integer=True, clock=True, limit=2),
                    base_config, "loss_lock2", cost=5.0,
                )
            books[strategy][variant] = book
            setup_book = book.loc[book.setup_id.eq(SETUP)].copy()
            features = orders[["sid", "price_change_pct", "oi_change_pct", "volume_ratio", "body_ratio",
                               "wick_ratio", "research_extension_atr"]].copy()
            detail = setup_book.merge(features, on="sid", how="left", validate="one_to_one", suffixes=("", "_candidate"))
            detail.insert(0, "audit_variant", variant)
            detail.insert(0, "audit_strategy", strategy)
            trade_details.append(detail)
            for period, period_days in periods.items():
                setup_rows.append({"strategy": strategy, "variant": variant, "period": period,
                                   **metrics(setup_book, period_days, orders)})
                portfolio_rows.append({"strategy": strategy, "variant": variant, "period": period,
                                       **metrics(book, period_days, None)})

    parity.append(same_execution(
        books["G3"]["baseline"], g3_saved, "G3 reconstructed baseline selection + paths + portfolio"
    ))
    parity.append(same_execution(
        books["G7_V4_LOCK2"]["baseline"], g7_saved,
        "G7 reconstructed extension<=4 selection + paths + V4 protection + lock2 portfolio",
    ))
    dump_json(OUT / "baseline_reproduction.json", parity)
    if not all(row["passed"] for row in parity):
        raise ValueError("Replacement-aware baseline did not reproduce a published portfolio")

    setup_table = pd.DataFrame(setup_rows)
    portfolio_table = pd.DataFrame(portfolio_rows)
    for table in (setup_table, portfolio_table):
        baseline = table.loc[table.variant.eq("baseline"), ["strategy", "period", "trades", "wins",
                                                                "net_pnl_rs", "negative_trade_sum_abs_rs"]].rename(
            columns={column: "baseline_" + column for column in
                     ("trades", "wins", "net_pnl_rs", "negative_trade_sum_abs_rs")}
        )
        merged = table.merge(baseline, on=["strategy", "period"], how="left", validate="many_to_one")
        table["net_change_vs_baseline_rs"] = merged.net_pnl_rs - merged.baseline_net_pnl_rs
        table["loss_amount_reduction_vs_baseline_rs"] = (
            merged.baseline_negative_trade_sum_abs_rs - merged.negative_trade_sum_abs_rs
        )
        # These are aggregate count ratios. Variants can admit replacements, so
        # calling them identity "retention" would be misleading.
        table["executed_trade_count_ratio_vs_baseline_pct"] = np.where(
            merged.baseline_trades.gt(0), 100 * merged.trades / merged.baseline_trades, np.nan
        )
        table["winner_count_ratio_vs_baseline_pct"] = np.where(
            merged.baseline_wins.gt(0), 100 * merged.wins / merged.baseline_wins, np.nan
        )
    setup_table.to_csv(OUT / "variant_setup_metrics.csv", index=False)
    portfolio_table.to_csv(OUT / "variant_portfolio_metrics.csv", index=False)
    pd.concat(trade_details, ignore_index=True, sort=False).to_csv(
        OUT / "variant_0926_trade_details.csv", index=False
    )

    provenance_files = [
        PROTOCOL, Path(__file__), G3_RUN / "manifest.json", G3_RUN / "summary.json",
        G3_RUN / "config.json", G3_RUN / "selected_orders.csv", G3_RUN / "portfolio_trades.csv",
        G7_CACHE / "manifest.json", G7_CACHE / "provenance.json", G7_CACHE / "native.csv",
        G7_CACHE / "features.csv", G7_CACHE / "minute_paths.npz", G7_LOCK.parent / "manifest.json",
        G7_LOCK / "portfolio_trades.csv", feature_source / "manifest.json",
        feature_source / "entry_features_and_decisions.csv", Path(g7_current.CONFIG),
        Path(pool_builder.__file__), Path(production.__file__),
        Path(g3_identity.__file__), Path(g7_cache_module.__file__), Path(g7_current.__file__),
    ]
    provenance = {
        "protocol_sha256": sha256(PROTOCOL),
        "candidate_pool_proof": pool_proof,
        "price_source_proof": prices.proof,
        "inputs_and_code": {str(path.resolve()): sha256(path) for path in provenance_files},
        "candidate_rows": len(pool),
        "candidate_days": int(pool.day.astype(str).str[:10].nunique()),
        "calendars": periods,
        "g3_current_selected_0926": len(current_g3),
        "g7_current_selected_0926": len(current_g7),
        "active_configs_modified": False,
    }
    dump_json(OUT / "source_provenance.json", provenance)

    generated = [path for path in OUT.iterdir() if path.is_file() and path.name != "artifact_manifest.json"]
    dump_json(OUT / "artifact_manifest.json", {
        "created_at_ist": datetime.now().astimezone().isoformat(),
        "scope": "BACKTEST_ONLY",
        "execution_authority": False,
        "files": {path.name: sha256(path) for path in sorted(generated)},
    })
    shown = setup_table.loc[
        setup_table.period.isin(["all", "development", "post_development", "assessment", "known_oct26", "final_oct27"]),
        ["strategy", "variant", "period", "selected_opportunities", "trades", "wins", "losses",
         "win_rate_pct", "net_pnl_rs", "net_change_vs_baseline_rs"],
    ]
    print(shown.to_string(index=False), flush=True)
    print("AUDIT OUTPUT", OUT, flush=True)


if __name__ == "__main__":
    main()
