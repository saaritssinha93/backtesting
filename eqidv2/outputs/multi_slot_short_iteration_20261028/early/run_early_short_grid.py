"""Frozen-grid, replacement-aware 09:31/12:26 SHORT audit.

Research only.  The shared multi-slot protocol is the sole selection protocol.
Trials run before native ranking/quota, replacements are freshly simulated,
and complete G3 / G7 V4-protected two-loss-lock portfolios are reallocated.
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
PROTOCOL = OUT.parent / "grid_protocol.json"
SETUPS = ("0931_SHORT", "1226_SHORT")
SOURCE_V2 = ROOT / "outputs/g4_setup_candidate_source_v2_20261016"
ACCEPTED = Path(
    r"C:/TradingData/eqidv2/backtesting_result_v13_v10_g_7/v4_lock_backtest/runs/2026-10-28/"
    r"20261028T143059782924_f86f1197_0926_short_wick020_lock2"
)
G3_RUN = Path(
    r"C:/TradingData/eqidv2/backtesting_result_v13_v10_g_3/backtests/runs/2026-10-27/"
    r"20261027T172043456343_3d50ca01_current_full_history"
)
G7_CACHE = Path(
    r"C:/TradingData/eqidv2/backtesting_result_v13_v10_g_7/v4_research/runs/2026-10-27/"
    r"20261027T165517683392_v4_extremes/inputs"
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
        raise ValueError("Invalid boolean values: " + repr(sorted(text.loc[~text.isin(("true", "false"))].unique())[:5]))
    return text.eq("true")


def keys(frame: pd.DataFrame) -> set[tuple[str, str, str]]:
    return set(zip(frame.day.astype(str).str[:10], frame.setup_id.astype(str), frame.tradingsymbol.astype(str)))


def wilson(wins: int, trades: int, z: float = 1.959963984540054) -> tuple[float, float]:
    if trades <= 0:
        return math.nan, math.nan
    p = wins / trades
    denominator = 1 + z * z / trades
    center = (p + z * z / (2 * trades)) / denominator
    spread = z * math.sqrt(p * (1 - p) / trades + z * z / (4 * trades * trades)) / denominator
    return 100 * (center - spread), 100 * (center + spread)


def metrics(book: pd.DataFrame, days: list[str], selected: pd.DataFrame | None = None) -> dict:
    wanted = set(days)
    subset = book.loc[book.day.astype(str).str[:10].isin(wanted)].copy()
    executed = bools(subset.portfolio_executed)
    pnl = pd.to_numeric(subset.portfolio_net_profit_rupees, errors="coerce").fillna(0.0)
    wins = int((executed & pnl.gt(1e-8)).sum())
    losses = int((executed & pnl.lt(-1e-8)).sum())
    trades = int(executed.sum())
    low, high = wilson(wins, trades)
    day_pnl = subset.loc[executed].assign(_pnl=pnl.loc[executed]).groupby(
        subset.loc[executed, "day"].astype(str).str[:10], sort=True
    )._pnl.sum().reindex(days, fill_value=0.0)
    equity = day_pnl.cumsum()
    drawdown = equity.cummax().sub(equity)
    return {
        "sessions": len(days),
        "selected_opportunities": (int(selected.day.astype(str).str[:10].isin(wanted).sum())
                                   if selected is not None else None),
        "trades": trades,
        "wins": wins,
        "losses": losses,
        "flats": trades - wins - losses,
        "win_rate_pct": 100 * wins / trades if trades else math.nan,
        "wilson_95_lower_pct": low,
        "wilson_95_upper_pct": high,
        "point_win_rate_gt_75": bool(trades and wins / trades > 0.75),
        "wilson_lower_gt_75": bool(trades and low > 75),
        "net_pnl_rs": float(pnl.loc[executed].sum()),
        "negative_trade_sum_abs_rs": float(-pnl.loc[executed & pnl.lt(-1e-8)].sum()),
        "daily_close_drawdown_rs": float(drawdown.max()) if len(drawdown) else 0.0,
    }


def same_execution(actual: pd.DataFrame, expected: pd.DataFrame, label: str) -> dict:
    left = actual.sort_values("sid").reset_index(drop=True)
    right = expected.sort_values("sid").reset_index(drop=True)
    result = {"label": label, "rows": len(left), "passed": True, "differences": []}
    if len(left) != len(right) or set(left.sid.astype(int)) != set(right.sid.astype(int)):
        result.update(passed=False, differences=["sid_inventory"])
        return result
    right = right.set_index(right.sid.astype(int)).loc[left.sid.astype(int)].reset_index(drop=True)
    for column in ("filled", "portfolio_executed"):
        if not np.array_equal(bools(left[column]), bools(right[column])):
            result["differences"].append(column)
    for column in ("entry_price", "exit_price", "gross_return_pct", "net_return_pct",
                   "portfolio_net_profit_rupees"):
        a = pd.to_numeric(left[column], errors="coerce").to_numpy(float)
        b = pd.to_numeric(right[column], errors="coerce").to_numpy(float)
        if not np.allclose(a, b, rtol=0, atol=1e-6, equal_nan=True):
            result["differences"].append(column)
    for column in ("entry_ts", "exit_ts"):
        if not pd.to_datetime(left[column], errors="coerce", utc=True).equals(
                pd.to_datetime(right[column], errors="coerce", utc=True)):
            result["differences"].append(column)
    result["passed"] = not result["differences"]
    return result


def variant_mask(frame: pd.DataFrame, rule: dict) -> pd.Series:
    allowed = pd.Series(True, index=frame.index)
    if "wick_ratio_min" in rule:
        allowed &= pd.to_numeric(frame.wick_ratio, errors="coerce").ge(float(rule["wick_ratio_min"]))
    if "extension_atr_max" in rule:
        allowed &= pd.to_numeric(frame.research_extension_atr, errors="coerce").le(float(rule["extension_atr_max"]))
    if "body_ratio_max" in rule:
        allowed &= pd.to_numeric(frame.body_ratio, errors="coerce").le(float(rule["body_ratio_max"]))
    if "downward_move_magnitude_pct_max" in rule:
        allowed &= pd.to_numeric(frame.price_change_pct, errors="coerce").ge(
            -float(rule["downward_move_magnitude_pct_max"])
        )
    return allowed


def build_pool(summary: dict, production, identity, prices) -> tuple[pd.DataFrame, dict]:
    from research import g3_0950_long_backtest as morning
    # The source package is immutable evidence.  Some of its upstream live-history
    # parquets have since acquired later sessions, so verify the saved package
    # members themselves rather than asking the old loader to re-hash mutable
    # upstream paths before reading the already sealed pool.
    source_manifest = read_json(SOURCE_V2 / "manifest.json")
    if source_manifest.get("complete") is not True:
        raise ValueError("Incomplete sealed candidate package")
    for name, digest in source_manifest["files"].items():
        member = SOURCE_V2 / name
        if sha256(member) != digest:
            raise ValueError("Sealed candidate package member changed: " + name)
    source = pd.read_pickle(SOURCE_V2 / "pool.pkl")
    source_proof = read_json(SOURCE_V2 / "proof.json")
    historic = source.loc[
        source.setup_id.isin(SETUPS) & bools(source.baseline_filter_pass) &
        source.day.astype(str).str[:10].le("2026-10-16")
    ].copy()
    daily_proof = []
    recent = []
    for item in summary["daily_path_evidence"]:
        day_value = str(Path(item["run_dir"]).parent.name)
        if day_value <= "2026-10-16":
            continue
        directory = Path(item["run_dir"])
        manifest = read_json(directory / "run_manifest.json")
        record = manifest["artifacts"]["candidate_signals"]
        path = directory / record["path"]
        if sha256(path) != record["sha256"]:
            raise ValueError("Daily candidate evidence changed: " + str(path))
        frame = morning.normalize_signals(pd.read_csv(path))
        audit = identity.selection_audit(frame)
        part = audit.loc[audit.setup_id.isin(SETUPS) & bools(audit.v9_filter_pass)].copy()
        recent.append(part)
        daily_proof.append({"day": day_value, "path": str(path), "sha256": record["sha256"],
                            "rows": len(part)})
    pool = pd.concat([historic, *recent], ignore_index=True, sort=False)
    pool["day"] = pool.day.astype(str).str[:10]
    pool = pool.sort_values(["day", "setup_id", "tradingsymbol"], kind="stable").reset_index(drop=True)
    if pool.duplicated(["day", "setup_id", "tradingsymbol"]).any():
        raise ValueError("Duplicate target candidate")
    pool["source_sid"] = pool.sid
    pool["candidate_uid"] = np.arange(1, len(pool) + 1, dtype=np.int64)
    pool["sid"] = 350_000_000 + pool.candidate_uid
    pool = prices.enrich(pool)
    if not np.isfinite(pd.to_numeric(pool.research_extension_atr, errors="coerce")).all():
        raise ValueError("Extension ATR is incomplete")
    base_pass = []
    for row in pool.to_dict("records"):
        setup = production._ACTIVE_BY_ID[row["setup_id"]]
        base_pass.append(production.passes_selected_filters(
            row, setup, session_date=date.fromisoformat(str(row["day"])[:10])))
    if not all(base_pass):
        raise ValueError("Native-eligible pool contains a baseline rejection")
    return pool, {
        "source_v2_manifest": str(SOURCE_V2 / "manifest.json"),
        "source_v2_manifest_sha256": sha256(SOURCE_V2 / "manifest.json"),
        "source_v2_proof": source_proof,
        "daily": daily_proof,
        "rows": len(pool),
        "by_setup": pool.groupby("setup_id").size().astype(int).to_dict(),
        "candidate_days_by_setup": pool.groupby("setup_id").day.nunique().astype(int).to_dict(),
    }


def select_variant(pool: pd.DataFrame, setup_id: str, strategy: str, rule: dict,
                   production, reference: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    setup = production._ACTIVE_BY_ID[setup_id]
    ref_map = {(str(r.day)[:10], r.setup_id, r.tradingsymbol): r for r in reference.itertuples()}
    decisions, selected_parts = [], []
    for day_value, source in pool.loc[pool.setup_id.eq(setup_id)].groupby("day", sort=True):
        work = source.copy()
        trial = variant_mask(work, rule)
        eligible = work.loc[trial].copy()
        chosen = production.rank_candidates(
            eligible.to_dict("records"), setup, session_date=date.fromisoformat(day_value)
        )
        chosen_symbols = {str(row["tradingsymbol"]) for row in chosen}
        ranked = sorted(eligible.to_dict("records"), key=lambda row: production._rank_key(row, setup))
        ranks = {str(row["tradingsymbol"]): n for n, row in enumerate(ranked, 1)}
        audit = work[["candidate_uid", "source_sid", "day", "setup_id", "tradingsymbol", "side",
                      "price_change_pct", "oi_change_pct", "volume_ratio", "body_ratio", "wick_ratio",
                      "traded_value", "research_extension_atr"]].copy()
        audit["strategy"] = strategy
        audit["variant"] = rule["variant"]
        audit["variant_gate_pass"] = trial.to_numpy(bool)
        audit["rank_after_variant"] = audit.tradingsymbol.map(ranks).astype("Int64")
        audit["selected_before_g7_post_rank_rule"] = audit.tradingsymbol.astype(str).isin(chosen_symbols)
        decisions.append(audit)
        if chosen:
            selected_parts.append(pd.DataFrame(chosen))
    selected = (pd.concat(selected_parts, ignore_index=True, sort=False)
                if selected_parts else pool.iloc[0:0].copy())
    selected["day"] = selected.day.astype(str).str[:10]
    # Existing G7 configuration applies this after native ranking, without replacement.
    if strategy == "G7_V4_LOCK2" and setup_id == "1226_SHORT":
        selected = selected.loc[pd.to_numeric(selected.traded_value, errors="coerce").ge(10_000_000)].copy()
    stop = setup.stop_pct
    target = setup.target_pct
    if strategy == "G7_V4_LOCK2" and len(reference):
        stop = float(pd.to_numeric(reference.native_stop_pct, errors="coerce").dropna().iloc[0])
        target = float(pd.to_numeric(reference.native_target_pct, errors="coerce").dropna().iloc[0])
    selected["native_stop_pct"] = stop
    selected["native_target_pct"] = target
    selected["stop_pct"] = stop
    selected["target_pct"] = target
    selected["initial_stop_pct"] = 1.2 if strategy == "G7_V4_LOCK2" else production.INITIAL_STOP_PCT
    selected["tightened_stop_pct"] = 1.0 if strategy == "G7_V4_LOCK2" else production.TIGHTENED_STOP_PCT
    selected["tighten_after_minutes"] = 120 if strategy == "G7_V4_LOCK2" else production.TIGHTEN_AFTER_MINUTES
    selected["configured_confirmation_end"] = setup.confirmation_end
    selected["picker"] = setup.picker
    selected["max_entries"] = setup.max_entries
    selected["v9_selected"] = True
    selected["v9_filter_pass"] = True
    selected["strategy_version"] = "PREQUOTA_AUDIT_" + strategy
    selected["strategy_fingerprint"] = hashlib.sha256(
        (strategy + "|" + setup_id + "|" + rule["variant"]).encode()).hexdigest()
    mapped_ids, signals, triggers = [], [], []
    for row in selected.itertuples():
        key = (str(row.day)[:10], setup_id, str(row.tradingsymbol))
        pinned = ref_map.get(key)
        mapped_ids.append(int(pinned.sid) if pinned is not None else
                          (360_000_000 if strategy == "G3" else 370_000_000) + int(row.candidate_uid))
        triggers.append(float(pinned.trigger) if pinned is not None else float(row.trigger))
        signals.append(f"audit:{strategy}:{row.day}:{setup_id}:{row.tradingsymbol}")
    selected["sid"] = mapped_ids
    selected["trigger"] = triggers
    selected["signal_id"] = signals
    if selected.sid.duplicated().any():
        raise ValueError("Duplicate selected identity")
    selected_keys = keys(selected)
    for audit in decisions:
        audit["selected_after_g7_post_rank_rule"] = [
            (str(r.day)[:10], setup_id, str(r.tradingsymbol)) in selected_keys for r in audit.itertuples()
        ]
    return selected.reset_index(drop=True), pd.concat(decisions, ignore_index=True, sort=False)


def build_g7_permissions(orders: pd.DataFrame, prices) -> tuple[pd.Series, pd.DataFrame]:
    from research import g7_daily_backtest as g7_daily
    from research import g7_v4_extremes as extremes
    from research import g7_v4_indicator_iteration as indicator
    orders = orders.copy()
    # Some sealed Sep/Oct candidate rows retain the dated futures symbol but
    # have an empty convenience month column.  Recover the same dated contract
    # from that symbol; this is identity metadata, not an outcome feature.
    missing_month = orders.contract_month.isna() | orders.contract_month.astype(str).str.strip().isin(("", "nan"))
    inferred = orders.futures_tradingsymbol.astype(str).str.extract(r"(\d{2}[A-Z]{3})FUT$", expand=False)
    orders.loc[missing_month, "contract_month"] = inferred.loc[missing_month]
    if orders.contract_month.isna().any():
        raise ValueError("Cannot identify dated Nifty contract month")
    audit, _, _ = g7_daily.nifty.order_audit(orders, prices)
    nifty = audit.set_index("sid").both_counter_allowed.astype(bool)
    records = []
    for row in orders.to_dict("records"):
        raw, source_path = prices.frame(pd.Timestamp(row["day"]).date(), row["tradingsymbol"])
        raw = raw.copy()
        # Prices uses zoneinfo while the sealed G7 feature engine constructs a
        # pytz Asia/Kolkata index.  Normalize the equivalent instants so the
        # engine's exact-minute equality check compares identical tz dtypes.
        raw["ts"] = pd.to_datetime(raw.ts, utc=True).dt.tz_convert(extremes.IST)
        all_days = {str(day): group.sort_values("ts", kind="stable")
                    for day, group in raw.groupby(raw.ts.dt.strftime("%Y-%m-%d"), sort=False)}
        try:
            base = extremes.feature_row(all_days, row, {})
            extra = indicator.indicator_row(raw, row)
        except ValueError as exc:
            raise ValueError(f"G7 causal feature failure {row['day']} {row['setup_id']} "
                             f"{row['tradingsymbol']}: {exc}") from exc
        base.update({key: value for key, value in extra.items() if key != "sid"})
        base["stock_source_path"] = source_path
        records.append(base)
    features = pd.DataFrame(records).set_index("sid", drop=False).loc[orders.sid.astype(int)]
    risk = indicator.decisions(features)["low_efficiency"]
    allowed = nifty.reindex(features.index) & ~risk
    if allowed.isna().any():
        raise ValueError("Missing G7 permission")
    features["nifty_allowed"] = nifty.reindex(features.index).to_numpy(bool)
    features["low_efficiency_veto"] = risk.to_numpy(bool)
    features["base_entry_allowed"] = allowed.to_numpy(bool)
    return allowed.astype(bool), features.reset_index(drop=True)


def reprice_g3(raw: pd.DataFrame, cost_bps: float, allocate_g3) -> pd.DataFrame:
    work = raw.copy(deep=True)
    filled = bools(work.filled)
    gross = pd.to_numeric(work.gross_return_pct, errors="coerce")
    work["cost_pct"] = np.where(filled, float(cost_bps) / 100.0, np.nan)
    work["net_return_pct"] = np.where(filled, gross - float(cost_bps) / 100.0, np.nan)
    return allocate_g3(work)


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

    protocol = read_json(PROTOCOL)
    if protocol.get("protocol_id") != "G3_G7_V4_LOCK_MULTI_SHORT_PREQUOTA_V1":
        raise ValueError("Shared protocol identity changed")
    variants = protocol["variants"]
    costs = [float(value) for value in protocol["later_evaluation"]["cost_stress_bps"]]
    if costs != [5.0, 9.0, 15.0] or not set(SETUPS).issubset(protocol["setups"]):
        raise ValueError("Shared protocol scope changed")

    print("Loading accepted 09:26-adjusted reference portfolios", flush=True)
    summary = read_json(G3_RUN / "summary.json")
    days = [str(value) for value in summary["days"]]
    if len(days) != 59 or days[0] != "2026-07-29" or days[-1] != "2026-10-27":
        raise ValueError("Unexpected calendar")
    g3_orders = pd.read_csv(G3_RUN / "selected_orders.csv")
    g3_base = pd.read_csv(ACCEPTED / "g3_reference_portfolio_trades.csv")
    g7_base = pd.read_csv(ACCEPTED / "portfolio_trades.csv")
    g7_native, _, g7_features, g7_cache_paths, g7_proof = g7_cache_module.load_cache(G7_CACHE)
    periods = dict(g7_proof["periods"])
    periods["final_sessions"] = periods.pop("known_oct26") + periods.pop("final_oct27")
    periods["later"] = [day for day in days if day not in set(periods["development"])]
    periods["october"] = [day for day in days if day.startswith("2026-10")]
    if periods["all"] != days:
        raise ValueError("G3/G7 calendars differ")
    prices = pool_builder.Prices(summary)
    pool_cache = OUT / "candidate_pool.pkl"
    proof_cache = OUT / "candidate_pool_proof.json"
    if pool_cache.is_file() and proof_cache.is_file():
        pool = pd.read_pickle(pool_cache)
        pool_proof = read_json(proof_cache)
    else:
        print("Building sealed pre-quota candidate pools", flush=True)
        pool, pool_proof = build_pool(summary, production, g3_identity, prices)
        pool.to_pickle(pool_cache)
        dump_json(proof_cache, pool_proof)
    print("Candidate inventory", pool.groupby("setup_id").size().to_dict(), flush=True)

    references = {
        "G3": {setup: g3_orders.loc[g3_orders.setup_id.eq(setup)].copy() for setup in SETUPS},
        "G7_V4_LOCK2": {setup: g7_native.loc[g7_native.setup_id.eq(setup)].copy() for setup in SETUPS},
    }
    selections: dict[tuple[str, str, str], pd.DataFrame] = {}
    decision_parts = []
    for strategy in ("G3", "G7_V4_LOCK2"):
        for setup in SETUPS:
            for rule in variants:
                selected, audit = select_variant(
                    pool, setup, strategy, rule, production, references[strategy][setup])
                selections[(strategy, setup, rule["variant"])] = selected
                decision_parts.append(audit)
            actual = keys(selections[(strategy, setup, "baseline")])
            expected = keys(references[strategy][setup])
            if actual != expected:
                raise ValueError(f"Baseline selection mismatch {strategy}/{setup}: missing={expected-actual}, extra={actual-expected}")
    decisions = pd.concat(decision_parts, ignore_index=True, sort=False)
    decisions.to_csv(OUT / "candidate_variant_decisions.csv", index=False)
    inventory_columns = ["candidate_uid", "source_sid", "day", "setup_id", "tradingsymbol", "side",
                         "price_change_pct", "oi_change_pct", "volume_ratio", "body_ratio", "wick_ratio",
                         "traded_value", "research_extension_atr", "research_ema20", "research_atr14",
                         "research_feature_source"]
    pool[[column for column in inventory_columns if column in pool]].to_csv(
        OUT / "candidate_pool_inventory.csv", index=False)

    print("Loading union paths and causal G7 permissions", flush=True)
    union_paths: dict[str, dict[int, dict]] = {}
    g7_permission: dict[int, bool] = {}
    g7_feature_parts = []
    for strategy in ("G3", "G7_V4_LOCK2"):
        merged = pd.concat([selections[(strategy, setup, rule["variant"])]
                            for setup in SETUPS for rule in variants], ignore_index=True, sort=False)
        merged = merged.drop_duplicates("sid", keep="first").reset_index(drop=True)
        union_paths[strategy] = prices.paths(merged)
        if strategy == "G7_V4_LOCK2":
            allowed, features = build_g7_permissions(merged, prices)
            g7_permission.update({int(sid): bool(value) for sid, value in allowed.items()})
            g7_feature_parts.append(features)
    pd.concat(g7_feature_parts, ignore_index=True).to_csv(OUT / "g7_candidate_features.csv", index=False)

    base_config = g7_proof["base_config"]
    parity = []
    parity.append(same_execution(allocate_g3(g3_base.copy()), g3_base,
                                 "accepted G3 allocator control"))
    base_allowed = pd.Series(bools(g7_base.base_entry_allowed).to_numpy(), index=g7_base.sid.astype(int))
    parity.append(same_execution(execute_g7(
        dict(raw=g7_base.copy(), allowed=base_allowed, integer=True, clock=True, limit=2),
        base_config, "accepted_control", cost=5.0), g7_base,
        "accepted G7 V4 protection + lock2 control"))

    setup_rows, portfolio_rows, trade_details, books = [], [], [], {}
    for strategy in ("G3", "G7_V4_LOCK2"):
        for setup in SETUPS:
            for rule in variants:
                variant = rule["variant"]
                orders = selections[(strategy, setup, variant)]
                paths = {int(sid): (g7_cache_paths[int(sid)]
                                    if strategy == "G7_V4_LOCK2" and int(sid) in g7_cache_paths
                                    else union_paths[strategy][int(sid)])
                         for sid in orders.sid}
                if strategy == "G3":
                    slot = _simulate(orders, paths, [date.fromisoformat(day) for day in days], g3_identity)
                    raw = pd.concat([g3_base.loc[~g3_base.setup_id.eq(setup)].copy(), slot],
                                    ignore_index=True, sort=False)
                else:
                    native_slot = native_simulator.g.v9.v5.simulate_native(
                        orders.copy(), paths,
                        cost_bps=float(base_config["model_cost_bps_round_trip"]),
                        max_entry_delay_minutes=int(production.ENTRY_EXPIRY_MINUTES))
                    protected_slot = g7_current.previous.replay(
                        native_slot, paths, base_config, g7_current.protection_settings(
                            read_json(g7_current.CONFIG)))
                    raw = pd.concat([g7_base.loc[~g7_base.setup_id.eq(setup)].copy(), protected_slot],
                                    ignore_index=True, sort=False)
                    other = g7_base.loc[~g7_base.setup_id.eq(setup), ["sid", "base_entry_allowed"]]
                    allowed = pd.Series(bools(other.base_entry_allowed).to_numpy(), index=other.sid.astype(int))
                    slot_allowed = pd.Series([g7_permission[int(sid)] for sid in orders.sid],
                                             index=orders.sid.astype(int), dtype=bool)
                    allowed = pd.concat([allowed, slot_allowed])
                    if allowed.index.duplicated().any() or set(allowed.index) != set(raw.sid.astype(int)):
                        raise ValueError("G7 permission inventory mismatch")
                for cost in costs:
                    if strategy == "G3":
                        book = reprice_g3(raw, cost, allocate_g3)
                    else:
                        book = execute_g7(
                            dict(raw=raw.copy(), allowed=allowed, integer=True, clock=True, limit=2),
                            base_config, "loss_lock2", cost=cost)
                    books[(strategy, setup, variant, cost)] = book
                    setup_book = book.loc[book.setup_id.eq(setup)].copy()
                    for period, period_days in periods.items():
                        setup_rows.append({"strategy": strategy, "setup_id": setup,
                                           "variant": variant, "cost_bps": cost, "period": period,
                                           **metrics(setup_book, period_days, orders)})
                        portfolio_rows.append({"strategy": strategy, "setup_id": setup,
                                               "variant": variant, "cost_bps": cost, "period": period,
                                               **metrics(book, period_days, None)})
                detail = books[(strategy, setup, variant, 5.0)].loc[
                    lambda frame: frame.setup_id.eq(setup)].copy()
                feature_columns = ["sid", "price_change_pct", "oi_change_pct", "volume_ratio",
                                   "body_ratio", "wick_ratio", "traded_value", "research_extension_atr"]
                detail = detail.merge(orders[feature_columns], on="sid", how="left",
                                      validate="one_to_one", suffixes=("", "_candidate"))
                detail.insert(0, "audit_variant", variant)
                detail.insert(0, "audit_setup_id", setup)
                detail.insert(0, "audit_strategy", strategy)
                trade_details.append(detail)

    # Fresh baseline target replays must reproduce the accepted adjusted portfolios.
    for strategy, expected in (("G3", g3_base), ("G7_V4_LOCK2", g7_base)):
        for setup in SETUPS:
            parity.append(same_execution(books[(strategy, setup, "baseline", 5.0)], expected,
                                         f"fresh baseline {strategy}/{setup}"))
    dump_json(OUT / "baseline_reproduction.json", parity)
    if not all(row["passed"] for row in parity):
        raise ValueError("Baseline replay did not reproduce")

    setup_table = pd.DataFrame(setup_rows)
    portfolio_table = pd.DataFrame(portfolio_rows)
    for table in (setup_table, portfolio_table):
        base = table.loc[table.variant.eq("baseline"), [
            "strategy", "setup_id", "cost_bps", "period", "trades", "wins", "losses",
            "net_pnl_rs", "negative_trade_sum_abs_rs", "daily_close_drawdown_rs",
        ]].rename(columns={name: "baseline_" + name for name in (
            "trades", "wins", "losses", "net_pnl_rs", "negative_trade_sum_abs_rs",
            "daily_close_drawdown_rs")})
        merged = table.merge(base, on=["strategy", "setup_id", "cost_bps", "period"],
                             how="left", validate="many_to_one")
        table["net_change_vs_baseline_rs"] = merged.net_pnl_rs - merged.baseline_net_pnl_rs
        table["loss_amount_reduction_vs_baseline_rs"] = (
            merged.baseline_negative_trade_sum_abs_rs - merged.negative_trade_sum_abs_rs)
        table["trade_count_ratio_vs_baseline_pct"] = np.where(
            merged.baseline_trades.gt(0), 100 * merged.trades / merged.baseline_trades, np.nan)
    setup_table.to_csv(OUT / "variant_setup_metrics.csv", index=False)
    portfolio_table.to_csv(OUT / "variant_portfolio_metrics.csv", index=False)
    pd.concat(trade_details, ignore_index=True, sort=False).to_csv(
        OUT / "variant_trade_details.csv", index=False)

    # Apply only the shared development gates and shared tie-break.
    audit_rows = []
    selected_variants = {}
    dev_setup = setup_table.loc[setup_table.cost_bps.eq(5) & setup_table.period.eq("development")]
    dev_port = portfolio_table.loc[portfolio_table.cost_bps.eq(5) & portfolio_table.period.eq("development")]
    for setup in SETUPS:
        for strategy in ("G3", "G7_V4_LOCK2"):
            s = dev_setup.loc[(dev_setup.setup_id == setup) & (dev_setup.strategy == strategy)].set_index("variant")
            p = dev_port.loc[(dev_port.setup_id == setup) & (dev_port.strategy == strategy)].set_index("variant")
            baseline_s, baseline_p = s.loc["baseline"], p.loc["baseline"]
            eligible = []
            for order, rule in enumerate(variants):
                variant = rule["variant"]
                if variant == "baseline":
                    continue
                row = s.loc[variant]
                failures = []
                if row.trades < int(protocol["development_selection"]["minimum_executed_trades"]):
                    failures.append("fewer than 8 executed development trades")
                if not row.win_rate_pct > float(protocol["development_selection"]["point_win_rate_strictly_above_pct"]):
                    failures.append("development point win rate is not strictly above 75%")
                if row.net_pnl_rs < baseline_s.net_pnl_rs - 1e-6:
                    failures.append("development setup net PnL below baseline")
                if row.negative_trade_sum_abs_rs >= baseline_s.negative_trade_sum_abs_rs - 1e-6:
                    failures.append("development losing-trade amount not below baseline")
                if p.loc[variant].net_pnl_rs < baseline_p.net_pnl_rs - 1e-6:
                    failures.append("development full-portfolio net PnL below baseline")
                audit_rows.append({"setup_id": setup, "strategy": strategy, "variant": variant,
                                   "eligible": not failures, "failure_reasons": "; ".join(failures),
                                   "development_trades": int(row.trades),
                                   "development_wins": int(row.wins),
                                   "development_losses": int(row.losses),
                                   "development_win_rate_pct": float(row.win_rate_pct),
                                   "development_wilson_95_lower_pct": float(row.wilson_95_lower_pct),
                                   "development_setup_net_pnl_rs": float(row.net_pnl_rs),
                                   "development_setup_net_change_rs": float(row.net_pnl_rs - baseline_s.net_pnl_rs),
                                   "development_loss_amount_reduction_rs": float(
                                       baseline_s.negative_trade_sum_abs_rs - row.negative_trade_sum_abs_rs),
                                   "development_portfolio_net_change_rs": float(
                                       p.loc[variant].net_pnl_rs - baseline_p.net_pnl_rs),
                                   "variant_order": order})
                if not failures:
                    eligible.append((float(row.net_pnl_rs), -int(row.losses), -order, variant))
            selected_variants[(setup, strategy)] = max(eligible)[3] if eligible else None
    selection_audit = pd.DataFrame(audit_rows)
    selection_audit["selected_from_development"] = [
        selected_variants[(row.setup_id, row.strategy)] == row.variant for row in selection_audit.itertuples()
    ]
    selection_audit.to_csv(OUT / "development_selection.csv", index=False)

    joint_rows = []
    for setup in SETUPS:
        for rule in variants[1:]:
            variant = rule["variant"]
            rows = selection_audit.loc[(selection_audit.setup_id == setup) &
                                       (selection_audit.variant == variant)]
            joint_rows.append({"setup_id": setup, "variant": variant,
                               "eligible_in_both_strategies": bool(len(rows) == 2 and rows.eligible.all()),
                               "g3_eligible": bool(rows.loc[rows.strategy.eq("G3"), "eligible"].iloc[0]),
                               "g7_eligible": bool(rows.loc[rows.strategy.eq("G7_V4_LOCK2"), "eligible"].iloc[0]),
                               "minimum_development_win_rate_pct": float(rows.development_win_rate_pct.min()),
                               "minimum_development_wilson_lower_pct": float(rows.development_wilson_95_lower_pct.min()),
                               "combined_portfolio_net_change_rs": float(rows.development_portfolio_net_change_rs.sum())})
    joint = pd.DataFrame(joint_rows)
    joint.to_csv(OUT / "joint_development_selection.csv", index=False)

    selection_changes = []
    for strategy in ("G3", "G7_V4_LOCK2"):
        for setup in SETUPS:
            base_keys = keys(selections[(strategy, setup, "baseline")])
            for rule in variants:
                variant = rule["variant"]
                current = keys(selections[(strategy, setup, variant)])
                for key in sorted(base_keys | current):
                    selection_changes.append({"strategy": strategy, "setup_id": setup, "variant": variant,
                                              "day": key[0], "tradingsymbol": key[2],
                                              "change_vs_baseline": ("RETAINED" if key in base_keys and key in current
                                                                     else "ADDED" if key in current else "REMOVED")})
    pd.DataFrame(selection_changes).to_csv(OUT / "selection_changes.csv", index=False)

    chosen_rows = []
    daywise_rows = []
    for setup in SETUPS:
        for strategy in ("G3", "G7_V4_LOCK2"):
            chosen = selected_variants[(setup, strategy)] or "baseline"
            for cost in costs:
                for period, period_days in periods.items():
                    slot = setup_table.loc[(setup_table.strategy == strategy) &
                                           (setup_table.setup_id == setup) &
                                           (setup_table.variant == chosen) &
                                           (setup_table.cost_bps == cost) &
                                           (setup_table.period == period)].iloc[0]
                    port = portfolio_table.loc[(portfolio_table.strategy == strategy) &
                                               (portfolio_table.setup_id == setup) &
                                               (portfolio_table.variant == chosen) &
                                               (portfolio_table.cost_bps == cost) &
                                               (portfolio_table.period == period)].iloc[0]
                    chosen_rows.append({"setup_id": setup, "strategy": strategy,
                                        "development_selected_variant": selected_variants[(setup, strategy)],
                                        "reported_variant": chosen, "cost_bps": cost, "period": period,
                                        **{"setup_" + k: slot[k] for k in (
                                            "trades", "wins", "losses", "win_rate_pct",
                                            "wilson_95_lower_pct", "wilson_95_upper_pct", "net_pnl_rs",
                                            "negative_trade_sum_abs_rs", "net_change_vs_baseline_rs")},
                                        **{"portfolio_" + k: port[k] for k in (
                                            "trades", "wins", "losses", "win_rate_pct", "net_pnl_rs",
                                            "negative_trade_sum_abs_rs", "daily_close_drawdown_rs",
                                            "net_change_vs_baseline_rs")}})
            for day_value in days:
                variant_book = books[(strategy, setup, chosen, 5.0)]
                baseline_book = books[(strategy, setup, "baseline", 5.0)]
                current = metrics(variant_book.loc[variant_book.setup_id.eq(setup)], [day_value], None)
                baseline = metrics(baseline_book.loc[baseline_book.setup_id.eq(setup)], [day_value], None)
                daywise_rows.append({"setup_id": setup, "strategy": strategy, "variant": chosen,
                                     "day": day_value, "trades": current["trades"], "wins": current["wins"],
                                     "losses": current["losses"], "net_pnl_rs": current["net_pnl_rs"],
                                     "baseline_net_pnl_rs": baseline["net_pnl_rs"],
                                     "net_change_vs_baseline_rs": current["net_pnl_rs"] - baseline["net_pnl_rs"]})
    chosen_table = pd.DataFrame(chosen_rows)
    chosen_table.to_csv(OUT / "selected_variant_results.csv", index=False)
    pd.DataFrame(daywise_rows).to_csv(OUT / "selected_variant_daywise.csv", index=False)

    report = [
        "# 09:31 and 12:26 SHORT replacement-aware audit",
        "",
        "The shared fixed grid was applied before native ranking and quota. Replacements were simulated from one-minute paths. G7 then applied its current Nifty permission, 12:26 traded-value veto, low-efficiency veto, +0.6/+0.1 protection, and causal two-loss lock.",
        "",
        "No result is an independent holdout. A point win rate above 75% is descriptive when the Wilson lower bound remains below 75%.",
        "",
        "## Development choices",
        "",
    ]
    for setup in SETUPS:
        for strategy in ("G3", "G7_V4_LOCK2"):
            choice = selected_variants[(setup, strategy)]
            report.append(f"- {setup} {strategy}: {choice or 'NO PREDECLARED VARIANT PASSED; retain baseline'}")
    report += ["", "## Shared-rule check", ""]
    for setup in SETUPS:
        both = joint.loc[(joint.setup_id == setup) & joint.eligible_in_both_strategies]
        report.append(f"- {setup}: " + (", ".join(both.variant) if len(both) else
                                       "no same predeclared rule passed the development gates in both strategies"))
    report += ["", "See `development_selection.csv`, `selected_variant_results.csv`, and `variant_setup_metrics.csv` for every result.", ""]
    (OUT / "report.md").write_text("\n".join(report), encoding="utf-8")

    provenance_files = [PROTOCOL, Path(__file__), SOURCE_V2 / "manifest.json",
                        G3_RUN / "manifest.json", G3_RUN / "summary.json", G3_RUN / "selected_orders.csv",
                        ACCEPTED / "artifact_manifest.json", ACCEPTED / "g3_reference_portfolio_trades.csv",
                        ACCEPTED / "portfolio_trades.csv", G7_CACHE / "manifest.json",
                        Path(production.__file__), Path(g3_identity.__file__), Path(g7_current.__file__)]
    dump_json(OUT / "source_provenance.json", {
        "protocol": str(PROTOCOL), "protocol_sha256": sha256(PROTOCOL),
        "candidate_pool_proof": pool_proof, "price_source_proof": prices.proof,
        "inputs_and_code": {str(path.resolve()): sha256(path) for path in provenance_files},
        "periods": periods, "accepted_baseline_includes_0926_wick_rule": True,
        "active_configs_modified": False, "independent_holdout": False,
    })
    generated = [path for path in OUT.iterdir() if path.is_file() and path.name != "artifact_manifest.json"]
    dump_json(OUT / "artifact_manifest.json", {
        "created_at_ist": datetime.now().astimezone().isoformat(),
        "scope": "BACKTEST_ONLY", "execution_authority": False,
        "files": {path.name: sha256(path) for path in sorted(generated)},
    })
    print(chosen_table.loc[(chosen_table.cost_bps == 5) &
                           chosen_table.period.isin(["all", "development", "validation", "assessment", "october"]),
                           ["setup_id", "strategy", "reported_variant", "period", "setup_trades", "setup_wins",
                            "setup_losses", "setup_win_rate_pct", "setup_wilson_95_lower_pct", "setup_net_pnl_rs",
                            "portfolio_net_change_vs_baseline_rs"]].to_string(index=False), flush=True)
    print("OUTPUT", OUT, flush=True)


if __name__ == "__main__":
    main()
