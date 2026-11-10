"""Frozen-grid, replacement-aware 13:21 SHORT audit for G3 and G7 V4 lock2.

All entry rules are applied to the complete pre-quota candidate pool before
the existing max-volume rank and two-entry quota.  Replacement orders receive
fresh one-minute execution paths.  G7 replacements also receive fresh causal
Nifty permission and V4 low-efficiency features at confirmation, then the
complete portfolio is reallocated with profit protection and the causal
two-realized-loss lock.  This module is research-only and never writes active
configuration.
"""
from __future__ import annotations

from dataclasses import replace
from datetime import date, datetime
import hashlib
import json
import math
from pathlib import Path
from statistics import NormalDist
import sys

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
OUT = Path(__file__).resolve().parent
PROTOCOL = OUT.parent / "grid_protocol.json"
SETUP = "1321_SHORT"
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
COSTS = (5.0, 9.0, 15.0)


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def read_json(path: Path):
    return json.loads(Path(path).read_text(encoding="utf-8-sig"))


def dump_json(path: Path, value) -> None:
    Path(path).write_text(
        json.dumps(value, indent=2, sort_keys=True, default=str, allow_nan=False),
        encoding="utf-8",
    )


def bools(values: pd.Series) -> pd.Series:
    if pd.api.types.is_bool_dtype(values):
        return values.fillna(False).astype(bool)
    text = values.astype(str).str.strip().str.lower()
    if not text.isin(("true", "false")).all():
        raise ValueError("Non-boolean values: " + repr(sorted(text.loc[~text.isin(("true", "false"))].unique())[:5]))
    return text.eq("true")


def key_frame(frame: pd.DataFrame) -> set[tuple[str, str, str]]:
    return set(zip(
        frame.day.astype(str).str[:10], frame.setup_id.astype(str), frame.tradingsymbol.astype(str)
    ))


def wilson(wins: int, trades: int, z: float) -> tuple[float, float]:
    if trades <= 0:
        return math.nan, math.nan
    p = wins / trades
    denominator = 1 + z * z / trades
    center = p + z * z / (2 * trades)
    spread = z * math.sqrt(p * (1 - p) / trades + z * z / (4 * trades * trades))
    return 100 * (center - spread) / denominator, 100 * (center + spread) / denominator


Z95 = NormalDist().inv_cdf(0.975)
PLANNED_FAMILY = 5 * 2 * 11
Z_FAMILY = NormalDist().inv_cdf(1 - 0.05 / (2 * PLANNED_FAMILY))


def metrics(book: pd.DataFrame, days: list[str], selected: pd.DataFrame | None = None) -> dict:
    wanted = set(days)
    cohort = book.loc[book.day.astype(str).str[:10].isin(wanted)].copy()
    executed = bools(cohort.portfolio_executed)
    pnl = pd.to_numeric(cohort.portfolio_net_profit_rupees, errors="coerce").fillna(0.0)
    wins = int((executed & pnl.gt(1e-8)).sum())
    losses = int((executed & pnl.lt(-1e-8)).sum())
    trades = int(executed.sum())
    low, high = wilson(wins, trades, Z95)
    family_low, family_high = wilson(wins, trades, Z_FAMILY)
    daily = cohort.loc[executed].assign(_pnl=pnl.loc[executed]).groupby(
        cohort.loc[executed, "day"].astype(str).str[:10], sort=True
    )._pnl.sum().reindex(days, fill_value=0.0)
    equity = daily.cumsum()
    drawdown = equity.cummax().sub(equity)
    selected_count = None
    if selected is not None:
        selected_count = int(selected.day.astype(str).str[:10].isin(wanted).sum())
    return {
        "sessions": len(days),
        "selected_opportunities": selected_count,
        "trades": trades,
        "wins": wins,
        "losses": losses,
        "flats": trades - wins - losses,
        "win_rate_pct": 100 * wins / trades if trades else math.nan,
        "wilson_95_lower_pct": low,
        "wilson_95_upper_pct": high,
        "wilson_familywise_110_lower_pct": family_low,
        "wilson_familywise_110_upper_pct": family_high,
        "point_win_rate_gt_75": bool(trades and wins / trades > 0.75),
        "wilson_95_lower_gt_75": bool(trades and low > 75),
        "net_pnl_rs": float(pnl.loc[executed].sum()),
        "negative_trade_sum_abs_rs": float(-pnl.loc[executed & pnl.lt(0)].sum()),
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


def build_pool(summary: dict, production, identity, prices) -> tuple[pd.DataFrame, dict]:
    """Join the sealed through-Oct16 pool to later immutable daily ledgers."""
    from research import g3_0950_long_backtest as morning

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
        source.setup_id.eq(SETUP) & bools(source.baseline_filter_pass)
        & source.day.astype(str).str[:10].le("2026-10-16")
    ].copy()
    recent, daily_proof = [], []
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
        part = audit.loc[audit.setup_id.eq(SETUP) & bools(audit.v9_filter_pass)].copy()
        recent.append(part)
        daily_proof.append({
            "day": day_value, "path": str(path), "sha256": record["sha256"], "rows": len(part)
        })
    pool = pd.concat([historic, *recent], ignore_index=True, sort=False)
    pool["day"] = pool.day.astype(str).str[:10]
    pool = pool.sort_values(["day", "setup_id", "tradingsymbol"], kind="stable").reset_index(drop=True)
    if pool.duplicated(["day", "setup_id", "tradingsymbol"]).any():
        raise ValueError("Duplicate 1321 candidate")
    pool["source_sid"] = pool.sid
    pool["candidate_uid"] = np.arange(1, len(pool) + 1, dtype=np.int64)
    pool["sid"] = 350_000_000 + pool.candidate_uid
    pool = prices.enrich(pool)
    base_pass = [
        production.passes_selected_filters(
            row, production._ACTIVE_BY_ID[SETUP],
            session_date=date.fromisoformat(str(row["day"])[:10]),
        )
        for row in pool.to_dict("records")
    ]
    if not all(base_pass):
        raise ValueError("Native-eligible pool contains a baseline rejection")
    return pool, {
        "source_v2_manifest": str(SOURCE_V2 / "manifest.json"),
        "source_v2_manifest_sha256": sha256(SOURCE_V2 / "manifest.json"),
        "source_v2_proof": source_proof,
        "daily": daily_proof,
        "rows": len(pool),
        "candidate_days": int(pool.day.nunique()),
    }


def select_variant(pool: pd.DataFrame, strategy: str, rule: dict, production,
                   current_ids: dict[tuple[str, str, str], tuple[int, float]], g7_stop: float,
                   g7_target: float) -> tuple[pd.DataFrame, pd.DataFrame]:
    base = production._ACTIVE_BY_ID[SETUP]
    setup = base if strategy == "G3" else replace(
        base, stop_pct=g7_stop, target_pct=g7_target,
        source_version="G7_V4_LOCK2_1321_PREQUOTA_AUDIT",
    )
    selected_parts, audit_parts = [], []
    for day_value, source in pool.groupby(pool.day.astype(str).str[:10], sort=True):
        session = date.fromisoformat(day_value)
        work = source.copy()
        base_pass = pd.Series([
            production.passes_selected_filters(row, setup, session_date=session)
            for row in work.to_dict("records")
        ], index=work.index)
        variant_pass = base_pass & filter_variant(work, rule)
        eligible = work.loc[variant_pass].copy()
        chosen = production.rank_candidates(eligible.to_dict("records"), setup, session_date=session)
        ranked = sorted(eligible.to_dict("records"), key=lambda row: production._rank_key(row, setup))
        rank = {str(row["tradingsymbol"]): number for number, row in enumerate(ranked, 1)}
        symbols = {str(row["tradingsymbol"]) for row in chosen}
        cols = ["sid", "day", "setup_id", "tradingsymbol", "price_change_pct", "oi_change_pct",
                "volume_ratio", "body_ratio", "wick_ratio", "research_extension_atr"]
        audit = work[cols].copy()
        audit["strategy"] = strategy
        audit["variant"] = rule["variant"]
        audit["base_gate_pass"] = base_pass.to_numpy(bool)
        audit["variant_gate_pass"] = variant_pass.to_numpy(bool)
        audit["rank_after_variant"] = audit.tradingsymbol.map(rank).astype("Int64")
        audit["selected"] = audit.tradingsymbol.astype(str).isin(symbols)
        audit_parts.append(audit)
        if chosen:
            selected_parts.append(pd.DataFrame(chosen))
    selected = pd.concat(selected_parts, ignore_index=True, sort=False) if selected_parts else pool.iloc[0:0].copy()
    selected["day"] = selected.day.astype(str).str[:10]
    selected["setup_id"] = SETUP
    selected["native_stop_pct"] = setup.stop_pct
    selected["native_target_pct"] = setup.target_pct
    selected["stop_pct"] = setup.stop_pct
    selected["target_pct"] = setup.target_pct
    selected["initial_stop_pct"] = g7_stop if strategy != "G3" else production.INITIAL_STOP_PCT
    selected["tightened_stop_pct"] = 1.0 if strategy != "G3" else production.TIGHTENED_STOP_PCT
    selected["tighten_after_minutes"] = 120 if strategy != "G3" else production.TIGHTEN_AFTER_MINUTES
    selected["configured_confirmation_end"] = setup.confirmation_end
    selected["picker"] = setup.picker
    selected["max_entries"] = setup.max_entries
    selected["v9_selected"] = True
    selected["v9_filter_pass"] = True
    selected["strategy_version"] = strategy + "_1321_PREQUOTA_AUDIT"
    selected["strategy_fingerprint"] = hashlib.sha256(
        (strategy + ":" + rule["variant"] + ":1321").encode("utf-8")
    ).hexdigest()
    ids, signals = [], []
    for row in selected.itertuples():
        key = (str(row.day)[:10], SETUP, str(row.tradingsymbol))
        pinned = current_ids.get(key)
        sid = pinned[0] if pinned is not None else None
        if sid is None:
            sid = (190_000_000 if strategy == "G3" else 200_000_000) + int(row.sid)
        ids.append(int(sid))
        signals.append(f"audit:{strategy}:{row.day}:{SETUP}:{row.tradingsymbol}")
    selected["sid"] = ids
    selected["trigger"] = [
        current_ids.get((str(row.day)[:10], SETUP, str(row.tradingsymbol)), (0, float(row.trigger)))[1]
        for row in selected.itertuples()
    ]
    selected["signal_id"] = signals
    if selected.sid.duplicated().any() or selected.signal_id.duplicated().any():
        raise ValueError(f"Duplicate selection identity: {strategy}/{rule['variant']}")
    return selected.reset_index(drop=True), pd.concat(audit_parts, ignore_index=True, sort=False)


def selection_changes(selections: dict[str, pd.DataFrame], strategy: str) -> pd.DataFrame:
    baseline = key_frame(selections["baseline"])
    rows = []
    for variant, selected in selections.items():
        current = key_frame(selected)
        for key in sorted(baseline | current):
            rows.append({
                "strategy": strategy, "variant": variant, "day": key[0], "setup_id": key[1],
                "tradingsymbol": key[2],
                "change_vs_baseline": "RETAINED" if key in baseline and key in current else
                                      "ADDED" if key in current else "REMOVED",
            })
    return pd.DataFrame(rows)


def checked_session_value_exact(frame: pd.DataFrame, stamp: pd.Timestamp, extremes) -> pd.DataFrame:
    """Current feature check with timestamp values compared independently of Index.name.

    pandas 2.2 includes ``DatetimeIndex.name`` in ``equals``.  The feature code
    compares a column-derived index named ``ts`` with an unnamed date_range,
    so identical nanosecond sequences can fail.  All validation semantics are
    retained here; only metadata-insensitive timestamp equality is used.
    """
    start = stamp.normalize() + pd.Timedelta(hours=9, minutes=16)
    window = frame.loc[frame.ts.ge(start) & frame.ts.le(stamp)].sort_values("ts")
    expected = pd.date_range(start, stamp, freq="min")
    observed = pd.DatetimeIndex(window.ts)
    if not np.array_equal(observed.asi8, expected.asi8):
        raise ValueError("Missing or duplicate completed minute")
    values = window[extremes.FIELDS].apply(pd.to_numeric, errors="coerce").to_numpy(float)
    if (not len(values) or not np.isfinite(values).all() or (values[:, :4] <= 0).any()
            or (values[:, 4] < 0).any() or values[:, 4].sum() <= 0
            or (values[:, 1] < values[:, [0, 2, 3]].max(axis=1)).any()
            or (values[:, 2] > values[:, [0, 1, 3]].min(axis=1)).any()):
        raise ValueError("Invalid OHLCV")
    extremes._check_flags(window)
    if "is_synthetic" in window and extremes._flags(
            window.is_synthetic.where(window.is_synthetic.notna(), False), "synthetic").any():
        raise ValueError("Synthetic candle")
    return window


def causal_g7_features(orders: pd.DataFrame, prices, existing: pd.DataFrame,
                       nifty_audit: pd.DataFrame, extremes, indicators) -> pd.DataFrame:
    permission = nifty_audit.set_index("sid").both_counter_allowed.astype(bool)
    rows = []
    memo_by_source: dict[str, dict] = {}
    for row in orders.to_dict("records"):
        sid = int(row["sid"])
        if sid in existing.index:
            record = existing.loc[sid].to_dict()
            prior = bool(record["allowed_g7_v2"])
            if prior != bool(permission.loc[sid]):
                raise ValueError(f"Recomputed Nifty permission differs for retained SID {sid}")
        else:
            session = date.fromisoformat(str(row["day"])[:10])
            raw, key = prices.frame(session, str(row["tradingsymbol"]))
            raw = raw.sort_values("ts", kind="stable")
            days = {str(k): part.sort_values("ts") for k, part in raw.groupby(raw.ts.dt.date, sort=True)}
            memo = memo_by_source.setdefault(key, {})
            record = dict(row)
            try:
                record.update(extremes.feature_row(days, row, memo))
                record.update(indicators.indicator_row(raw, row))
            except ValueError as exc:
                raise ValueError(
                    f"G7 causal feature failure {row['day']} {row['setup_id']} "
                    f"{row['tradingsymbol']} sid={sid}: {exc}"
                ) from exc
            record["stock_source_path"] = key
            record["stock_source_sha256"] = prices.proof[key]["sha256"]
        record["sid"] = sid
        record["allowed_g7_v2"] = bool(permission.loc[sid])
        rows.append(record)
    result = pd.DataFrame(rows).set_index("sid", drop=False).loc[orders.sid.astype(int)]
    for available in ("feature_available_at", "indicator_available_at"):
        if (pd.to_datetime(result[available], utc=True) > pd.to_datetime(result.confirmation_ts, utc=True)).any():
            raise ValueError("Future information in replacement feature: " + available)
    risk = indicators.decisions(result)["low_efficiency"]
    result["recomputed_low_efficiency_veto"] = risk.to_numpy(bool)
    result["replacement_feature_recomputed"] = ~result.sid.astype(int).isin(existing.index.astype(int))
    return result


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
    from research import g7_v4_extremes as g7_extremes
    from research import g7_v4_indicator_iteration as g7_indicators
    from research import g7_v4_filtered_backtest as g7_current
    from research import g7_nifty_direction as g7_nifty
    from research.g7_v4_practical_research import execute as execute_g7
    from research.g7_entry_quality_20261026 import verify_members

    OUT.mkdir(parents=True, exist_ok=True)
    protocol = read_json(PROTOCOL)
    if SETUP not in protocol["setups"] or not protocol["frozen_before_slot_outcomes"]:
        raise ValueError("1321 SHORT is absent from the frozen shared protocol")
    variants = protocol["variants"]
    expected_names = [
        "baseline", "wick_ge_0p10", "wick_ge_0p20", "wick_ge_0p30",
        "extension_le_3p5", "extension_le_3p0", "extension_le_2p5",
        "body_le_0p90", "body_le_0p80", "down_move_le_0p75", "down_move_le_0p50",
        "wick_ge_0p20_and_extension_le_3p0",
    ]
    if [row["variant"] for row in variants] != expected_names:
        raise ValueError("Frozen variant grid changed")

    print("Loading sealed ledgers and feature caches", flush=True)
    g3_summary = read_json(G3_RUN / "summary.json")
    g3_orders = pd.read_csv(G3_RUN / "selected_orders.csv")
    # The current main references already contain the accepted 09:26 SHORT
    # wick>=0.20 change.  All 13:21 trials replace only their own setup rows.
    g3_saved = pd.read_csv(ACCEPTED / "g3_reference_portfolio_trades.csv")
    days = [str(value) for value in g3_summary["days"]]
    if len(days) != 59 or days[0] != "2026-07-29" or days[-1] != "2026-10-27":
        raise ValueError("Unexpected 59-session calendar")
    periods = {
        "all": days,
        "development": days[:35],
        "validation": days[35:49],
        "assessment": days[49:57],
        "final_sessions": days[57:],
        "october": [day for day in days if day.startswith("2026-10")],
    }
    if [len(periods[name]) for name in ("development", "validation", "assessment", "final_sessions")] != [35, 14, 8, 2]:
        raise ValueError("Chronological period partition changed")

    settings = read_json(g7_current.CONFIG)
    g7_current.validate_settings(settings)
    g7_native, _, _, g7_paths, g7_proof = g7_cache_module.load_cache(G7_CACHE)
    base_config = g7_proof["base_config"]
    if g7_proof["periods"]["all"] != days:
        raise ValueError("G3 and G7 calendars differ")
    feature_source = Path(settings["source_research"]["output"]).resolve()
    verify_members(feature_source, settings["source_research"]["manifest_sha256"])
    g7_features = pd.read_csv(feature_source / "entry_features_and_decisions.csv").set_index("sid", drop=False)
    if set(g7_features.index.astype(int)) != set(g7_native.sid.astype(int)):
        raise ValueError("G7 feature inventory differs from selected opportunities")
    g7_saved = pd.read_csv(ACCEPTED / "portfolio_trades.csv")
    current_g3 = g3_orders.loc[g3_orders.setup_id.eq(SETUP)].copy()
    current_g7 = g7_native.loc[g7_native.setup_id.eq(SETUP)].copy()
    if key_frame(current_g3) != key_frame(current_g7):
        raise ValueError("Published G3 and G7 1321 selections differ before entry filters")
    g7_stop = float(pd.to_numeric(current_g7.native_stop_pct).drop_duplicates().item())
    g7_target = float(pd.to_numeric(current_g7.native_target_pct).drop_duplicates().item())

    prices = pool_builder.Prices(g3_summary)
    pool_path = OUT / "candidate_pool_full.pkl"
    pool_proof_path = OUT / "candidate_pool_proof.json"
    if pool_path.is_file() and pool_proof_path.is_file():
        pool = pd.read_pickle(pool_path)
        pool_proof = read_json(pool_proof_path)
    else:
        print("Joining sealed and immutable-daily 13:21 SHORT pre-quota pools", flush=True)
        pool, pool_proof = build_pool(g3_summary, production, g3_identity, prices)
        pool.to_pickle(pool_path)
        dump_json(pool_proof_path, pool_proof)
    if set(pool.setup_id.astype(str)) != {SETUP}:
        raise ValueError("Candidate pool escaped 1321 SHORT")
    if not np.isfinite(pd.to_numeric(pool.research_extension_atr, errors="coerce")).all():
        raise ValueError("Candidate extension is incomplete")
    print(f"Candidate pool: {len(pool)} rows over {pool.day.astype(str).str[:10].nunique()} days", flush=True)

    ids_g3 = {(str(r.day)[:10], SETUP, str(r.tradingsymbol)): (int(r.sid), float(r.trigger))
              for r in current_g3.itertuples()}
    ids_g7 = {(str(r.day)[:10], SETUP, str(r.tradingsymbol)): (int(r.sid), float(r.trigger))
              for r in current_g7.itertuples()}
    selections: dict[str, dict[str, pd.DataFrame]] = {"G3": {}, "G7_V4_LOCK2": {}}
    audits = []
    for strategy, ids in (("G3", ids_g3), ("G7_V4_LOCK2", ids_g7)):
        for rule in variants:
            chosen, audit = select_variant(pool, strategy, rule, production, ids, g7_stop, g7_target)
            selections[strategy][rule["variant"]] = chosen
            audits.append(audit)
    if key_frame(selections["G3"]["baseline"]) != key_frame(current_g3):
        raise ValueError("G3 pre-quota baseline failed to reproduce")
    if key_frame(selections["G7_V4_LOCK2"]["baseline"]) != key_frame(current_g7):
        raise ValueError("G7 pre-quota baseline failed to reproduce")
    pd.concat(audits, ignore_index=True, sort=False).to_csv(OUT / "candidate_variant_decisions.csv", index=False)
    pd.concat([
        selection_changes(selections["G3"], "G3"),
        selection_changes(selections["G7_V4_LOCK2"], "G7_V4_LOCK2"),
    ], ignore_index=True).to_csv(OUT / "selection_changes.csv", index=False)
    pool.to_csv(OUT / "candidate_pool_inventory.csv", index=False)

    print("Loading paths and rebuilding causal G7 replacement features", flush=True)
    union_paths: dict[str, dict[int, dict]] = {}
    for strategy in selections:
        union = pd.concat(list(selections[strategy].values()), ignore_index=True, sort=False).drop_duplicates("sid")
        union_paths[strategy] = prices.paths(union)
    g7_union = pd.concat(list(selections["G7_V4_LOCK2"].values()), ignore_index=True, sort=False).drop_duplicates("sid")
    nifty_audit, nifty_proof, _ = g7_nifty.order_audit(g7_union, prices)
    original_checked_session = g7_extremes.checked_session
    g7_extremes.checked_session = lambda frame, stamp: checked_session_value_exact(
        frame, stamp, g7_extremes
    )
    try:
        causal_features = causal_g7_features(
            g7_union, prices, g7_features, nifty_audit, g7_extremes, g7_indicators
        )
    finally:
        g7_extremes.checked_session = original_checked_session
    dump_json(OUT / "feature_compatibility_audit.json", {
        "issue": "pandas DatetimeIndex.equals includes Index.name",
        "original_comparison": "named ts index versus unnamed date_range",
        "replacement_comparison": "exact timezone-aware nanosecond timestamp sequence",
        "feature_logic_changed": False,
        "ohlcv_flag_and_synthetic_validation_preserved": True,
        "first_observed_false_failure": {
            "sid": 550000007, "day": "2026-08-12", "symbol": "PIIND",
            "observed_minutes": 246, "expected_minutes": 246,
            "missing_timestamps": [], "extra_timestamps": [],
        },
    })
    pd.DataFrame(columns=["sid", "day", "tradingsymbol", "failure", "affected_variants"]).to_csv(
        OUT / "unprovable_replacements.csv", index=False
    )
    causal_features.to_csv(OUT / "g7_causal_features.csv", index=False)
    nifty_audit.to_csv(OUT / "g7_nifty_decisions.csv", index=False)
    nifty_proof.to_csv(OUT / "g7_nifty_source_proof.csv", index=False)
    union_risk = causal_features.recomputed_low_efficiency_veto.astype(bool)
    union_nifty = causal_features.allowed_g7_v2.astype(bool)

    controls = [same_execution(allocate_g3(g3_saved.copy()), g3_saved, "G3 allocator control")]
    accepted_allowed = pd.Series(
        bools(g7_saved.base_entry_allowed).to_numpy(bool), index=g7_saved.sid.astype(int)
    )
    g7_control = execute_g7(
        dict(raw=g7_saved.copy(), allowed=accepted_allowed, integer=True, clock=True, limit=2),
        base_config, "accepted_0926_control", cost=5.0,
    )
    controls.append(same_execution(
        g7_control, g7_saved, "accepted G7 V4 protection + 09:26 wick rule + lock2 control"
    ))
    if not all(row["passed"] for row in controls):
        dump_json(OUT / "baseline_reproduction.json", controls)
        raise ValueError("Published allocator controls failed")

    print("Freshly executing every fixed-grid selection", flush=True)
    books: dict[str, dict[str, pd.DataFrame]] = {"G3": {}, "G7_V4_LOCK2": {}}
    raw_books: dict[str, dict[str, pd.DataFrame]] = {"G3": {}, "G7_V4_LOCK2": {}}
    allowed_books: dict[str, pd.Series] = {}
    detail_frames, grid_rows = [], []
    for strategy in ("G3", "G7_V4_LOCK2"):
        for rule in variants:
            variant = rule["variant"]
            orders = selections[strategy][variant]
            paths = {
                int(sid): (g7_paths[int(sid)] if strategy == "G7_V4_LOCK2" and int(sid) in g7_paths
                           else union_paths[strategy][int(sid)])
                for sid in orders.sid
            }
            if strategy == "G3":
                slot = _simulate(orders, paths, [date.fromisoformat(day) for day in days], g3_identity)
                raw = pd.concat([g3_saved.loc[~g3_saved.setup_id.eq(SETUP)].copy(), slot],
                                ignore_index=True, sort=False)
                book = allocate_g3(raw)
            else:
                native_slot = native_simulator.g.v9.v5.simulate_native(
                    orders.copy(), paths,
                    cost_bps=float(base_config["model_cost_bps_round_trip"]),
                    max_entry_delay_minutes=int(production.ENTRY_EXPIRY_MINUTES),
                )
                slot = g7_current.previous.replay(
                    native_slot, paths, base_config, g7_current.protection_settings(settings)
                )
                raw = pd.concat([g7_saved.loc[~g7_saved.setup_id.eq(SETUP)].copy(), slot],
                                ignore_index=True, sort=False)
                other_sids = g7_saved.loc[~g7_saved.setup_id.eq(SETUP), "sid"].astype(int)
                slot_sids = orders.sid.astype(int)
                slot_allowed = union_nifty.loc[slot_sids] & ~union_risk.loc[slot_sids]
                allowed = pd.concat([accepted_allowed.loc[other_sids], slot_allowed])
                if allowed.index.duplicated().any() or set(allowed.index.astype(int)) != set(raw.sid.astype(int)):
                    raise ValueError("G7 permission inventory differs from raw portfolio")
                allowed_books[variant] = allowed
                book = execute_g7(
                    dict(raw=raw, allowed=allowed, integer=True, clock=True, limit=2),
                    base_config, "loss_lock2_1321_" + variant, cost=5.0,
                )
            books[strategy][variant] = book
            raw_books[strategy][variant] = raw
            setup_book = book.loc[book.setup_id.eq(SETUP)].copy()
            detail = setup_book.merge(
                orders[["sid", "price_change_pct", "oi_change_pct", "volume_ratio", "body_ratio",
                        "wick_ratio", "research_extension_atr"]],
                on="sid", how="left", validate="one_to_one", suffixes=("", "_candidate"),
            )
            detail.insert(0, "audit_variant", variant)
            detail.insert(0, "audit_strategy", strategy)
            if strategy == "G7_V4_LOCK2":
                detail["nifty_allowed_at_confirmation"] = detail.sid.map(union_nifty)
                detail["v4_low_efficiency_veto"] = detail.sid.map(union_risk)
                detail["base_entry_allowed"] = detail.nifty_allowed_at_confirmation & ~detail.v4_low_efficiency_veto
            detail_frames.append(detail)
            for period, period_days in periods.items():
                grid_rows.append({"strategy": strategy, "variant": variant, "scope": SETUP,
                                  "period": period, "cost_bps_round_trip": 5.0,
                                  **metrics(setup_book, period_days, orders)})
                grid_rows.append({"strategy": strategy, "variant": variant, "scope": "FULL_PORTFOLIO",
                                  "period": period, "cost_bps_round_trip": 5.0,
                                  **metrics(book, period_days, None)})

    controls.extend([
        same_execution(books["G3"]["baseline"], g3_saved,
                       "G3 replacement-aware baseline selection + execution + allocation"),
        same_execution(books["G7_V4_LOCK2"]["baseline"], g7_saved,
                       "G7 replacement-aware baseline + Nifty + V4 + lock2"),
    ])
    dump_json(OUT / "baseline_reproduction.json", controls)
    if not all(row["passed"] for row in controls):
        raise ValueError("Replacement-aware baseline failed")

    grid = pd.DataFrame(grid_rows)
    baseline = grid.loc[grid.variant.eq("baseline"), [
        "strategy", "scope", "period", "trades", "wins", "losses", "net_pnl_rs",
        "negative_trade_sum_abs_rs",
    ]].rename(columns={name: "baseline_" + name for name in (
        "trades", "wins", "losses", "net_pnl_rs", "negative_trade_sum_abs_rs"
    )})
    grid = grid.merge(baseline, on=["strategy", "scope", "period"], how="left", validate="many_to_one")
    grid["net_change_vs_baseline_rs"] = grid.net_pnl_rs - grid.baseline_net_pnl_rs
    grid["loss_amount_reduction_vs_baseline_rs"] = (
        grid.baseline_negative_trade_sum_abs_rs - grid.negative_trade_sum_abs_rs
    )
    grid.to_csv(OUT / "fixed_grid_metrics.csv", index=False)
    pd.concat(detail_frames, ignore_index=True, sort=False).to_csv(OUT / "variant_trade_details.csv", index=False)

    gate = protocol["development_selection"]
    order_index = {row["variant"]: number for number, row in enumerate(variants)}
    selection_rows, selected_by_strategy = [], {}
    for strategy in ("G3", "G7_V4_LOCK2"):
        setup_dev = grid.loc[(grid.strategy.eq(strategy)) & (grid.scope.eq(SETUP)) &
                             (grid.period.eq("development"))].set_index("variant")
        portfolio_dev = grid.loc[(grid.strategy.eq(strategy)) & (grid.scope.eq("FULL_PORTFOLIO")) &
                                 (grid.period.eq("development"))].set_index("variant")
        base_setup = setup_dev.loc["baseline"]
        base_portfolio = portfolio_dev.loc["baseline"]
        eligible = []
        for rule in variants[1:]:
            variant = rule["variant"]
            row = setup_dev.loc[variant]
            whole = portfolio_dev.loc[variant]
            failures = []
            if int(row.trades) < int(gate["minimum_executed_trades"]):
                failures.append("fewer_than_8_development_trades")
            if not float(row.win_rate_pct) > float(gate["point_win_rate_strictly_above_pct"]):
                failures.append("development_point_win_rate_not_strictly_above_75")
            if float(row.net_pnl_rs) < float(base_setup.net_pnl_rs) - 1e-6:
                failures.append("setup_net_pnl_below_baseline")
            if float(row.negative_trade_sum_abs_rs) >= float(base_setup.negative_trade_sum_abs_rs) - 1e-6:
                failures.append("setup_losing_amount_not_strictly_below_baseline")
            if float(whole.net_pnl_rs) < float(base_portfolio.net_pnl_rs) - 1e-6:
                failures.append("full_portfolio_net_pnl_below_baseline")
            selection_rows.append({
                "strategy": strategy, "variant": variant, "eligible": not failures,
                "failure_reasons": ";".join(failures), "development_trades": int(row.trades),
                "development_wins": int(row.wins), "development_losses": int(row.losses),
                "development_win_rate_pct": float(row.win_rate_pct),
                "development_wilson_95_lower_pct": float(row.wilson_95_lower_pct),
                "development_setup_net_pnl_rs": float(row.net_pnl_rs),
                "development_full_portfolio_net_pnl_rs": float(whole.net_pnl_rs),
            })
            if not failures:
                eligible.append((float(row.net_pnl_rs), -int(row.losses), -order_index[variant], variant))
        selected_by_strategy[strategy] = max(eligible)[3] if eligible else "baseline"
    selection_table = pd.DataFrame(selection_rows)
    selection_table.to_csv(OUT / "development_selection.csv", index=False)

    print("Repricing chosen development rules at 5/9/15 bps", flush=True)
    cost_rows = []
    for strategy in ("G3", "G7_V4_LOCK2"):
        chosen = selected_by_strategy[strategy]
        for variant in dict.fromkeys(["baseline", chosen]):
            for cost in COSTS:
                if strategy == "G3":
                    book = reprice_g3(raw_books[strategy][variant], cost, allocate_g3)
                else:
                    book = execute_g7(
                        dict(raw=raw_books[strategy][variant], allowed=allowed_books[variant],
                             integer=True, clock=True, limit=2),
                        base_config, f"loss_lock2_1321_{variant}_{cost:g}bps", cost=cost,
                    )
                orders = selections[strategy][variant]
                for scope, source, selected in (
                    (SETUP, book.loc[book.setup_id.eq(SETUP)].copy(), orders),
                    ("FULL_PORTFOLIO", book, None),
                ):
                    for period, period_days in periods.items():
                        cost_rows.append({
                            "strategy": strategy, "selected_from_development": chosen,
                            "variant": variant, "scope": scope, "period": period,
                            "cost_bps_round_trip": cost, **metrics(source, period_days, selected),
                        })
    costs = pd.DataFrame(cost_rows)
    costs.to_csv(OUT / "selected_cost_stress.csv", index=False)

    chronology = costs.loc[
        costs.apply(lambda row: row.variant == selected_by_strategy[row.strategy], axis=1)
        & costs.cost_bps_round_trip.eq(5.0)
    ].copy()
    chronology.to_csv(OUT / "selected_chronology_results.csv", index=False)
    daywise = []
    for strategy in ("G3", "G7_V4_LOCK2"):
        chosen = selected_by_strategy[strategy]
        for variant in dict.fromkeys(["baseline", chosen]):
            book = books[strategy][variant]
            for day_value in periods["october"]:
                daywise.append({
                    "strategy": strategy, "selected_from_development": chosen, "variant": variant,
                    "day": day_value, **metrics(book, [day_value], None),
                })
    pd.DataFrame(daywise).to_csv(OUT / "october_daywise_portfolio.csv", index=False)

    selected_summary = {}
    for strategy, chosen in selected_by_strategy.items():
        selected_summary[strategy] = {
            "selected_from_development": chosen,
            "development_gate_passed": chosen != "baseline",
            "periods_at_5bps": chronology.loc[
                chronology.strategy.eq(strategy),
                ["scope", "period", "trades", "wins", "losses", "win_rate_pct",
                 "wilson_95_lower_pct", "wilson_95_upper_pct", "wilson_familywise_110_lower_pct",
                 "net_pnl_rs", "negative_trade_sum_abs_rs", "daily_close_drawdown_rs"],
            ].to_dict("records"),
        }
    methodology = {
        "fixed_grid_nonbaseline_variants_per_slot": 11,
        "requested_short_slots": 5,
        "strategies": 2,
        "planned_nonbaseline_rule_comparisons": PLANNED_FAMILY,
        "development_gate_applied_per_strategy": True,
        "later_periods_cannot_select_replacement": True,
        "unadjusted_wilson_95_is_descriptive_after_selection": True,
        "bonferroni_familywise_wilson_context_reported": True,
        "independent_holdout": False,
        "combined_rule_warning": (
            "Individually chosen slot rules must not be summed or described as a combined improvement. "
            "A separate simultaneous pre-quota full-portfolio replay is required because replacements, "
            "capital contention, same-symbol exclusion, G7 Nifty/V4 gates, and the daily loss lock interact."
        ),
        "sample_size_warning": (
            "Point win rate above 75% does not establish a true rate above 75% when the Wilson lower bound "
            "is below 75%; repeated inspection of the same 59 sessions further weakens generalization."
        ),
        "protocol_ambiguity": (
            "The frozen protocol specifies development selection and later reporting but no numeric later-period "
            "acceptance gate. Later figures are evaluation only and are not relabeled as a formal qualification."
        ),
    }
    dump_json(OUT / "methodology_audit.json", methodology)

    provenance_files = [
        PROTOCOL, Path(__file__), G3_RUN / "manifest.json", G3_RUN / "summary.json",
        G3_RUN / "selected_orders.csv", G3_RUN / "portfolio_trades.csv",
        G7_CACHE / "manifest.json", G7_CACHE / "provenance.json", G7_CACHE / "native.csv",
        G7_CACHE / "minute_paths.npz", ACCEPTED / "artifact_manifest.json",
        ACCEPTED / "g3_reference_portfolio_trades.csv", ACCEPTED / "portfolio_trades.csv",
        SOURCE_V2 / "manifest.json", feature_source / "manifest.json",
        feature_source / "entry_features_and_decisions.csv", Path(g7_current.CONFIG),
        Path(pool_builder.__file__), Path(production.__file__), Path(g3_identity.__file__),
        Path(g7_extremes.__file__), Path(g7_indicators.__file__), Path(g7_nifty.__file__),
    ]
    provenance = {
        "scope": "BACKTEST_ONLY", "execution_authority": False,
        "active_configs_modified": False, "setup_id": SETUP,
        "accepted_baseline_includes_0926_wick_rule": True,
        "protocol_sha256": sha256(PROTOCOL), "candidate_pool_proof": pool_proof,
        "price_source_proof": prices.proof,
        "inputs_and_code": {str(path.resolve()): sha256(path) for path in provenance_files},
        "periods": periods, "candidate_rows": len(pool),
        "candidate_days": int(pool.day.astype(str).str[:10].nunique()),
        "replacement_feature_rows": int(causal_features.replacement_feature_recomputed.sum()),
        "selected_variants": selected_by_strategy,
    }
    dump_json(OUT / "source_provenance.json", provenance)
    dump_json(OUT / "summary.json", {
        "setup_id": SETUP, "scope": "BACKTEST_ONLY", "execution_authority": False,
        "baseline_reproduction_passed": all(row["passed"] for row in controls),
        "prequota_replacement_aware": True, "fresh_execution_and_full_allocation": True,
        "g7_causal_nifty_recomputed": True, "g7_v4_low_efficiency_recomputed_for_replacements": True,
        "g7_profit_protection_replayed": True, "g7_two_realized_loss_lock_replayed": True,
        "selected": selected_summary, "methodology": methodology,
    })
    generated = [path for path in OUT.iterdir() if path.is_file() and path.name != "artifact_manifest.json"]
    dump_json(OUT / "artifact_manifest.json", {
        "created_at_ist": datetime.now().astimezone().isoformat(),
        "scope": "BACKTEST_ONLY", "execution_authority": False,
        "files": {path.name: sha256(path) for path in sorted(generated)},
    })
    print("SELECTED", json.dumps(selected_by_strategy, sort_keys=True), flush=True)
    print(chronology.loc[chronology.scope.eq(SETUP), [
        "strategy", "variant", "period", "trades", "wins", "losses", "win_rate_pct",
        "wilson_95_lower_pct", "net_pnl_rs",
    ]].to_string(index=False), flush=True)
    print("OUTPUT", OUT, flush=True)


if __name__ == "__main__":
    main()
