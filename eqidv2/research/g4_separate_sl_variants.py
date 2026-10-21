"""Isolated G4 comparison of a profit floor, ATR trail, and their combination.

The three variants are configured and reported separately. They preserve the
sealed G4 entries, native targets, position sizing, portfolio constraints and
5 bps cost model. Nothing in this module can update a live configuration or a
``latest`` result pointer.
"""
from __future__ import annotations

import argparse
from datetime import datetime
import html
import json
import math
from pathlib import Path
import secrets
import shutil
import sys
from collections.abc import Mapping

import numpy as np
import pandas as pd

from research import g4_iterative_dataset as dataset
from research import g4_iterative_exit_backtest as core
from research import g4_g3_mirror_backtest as base
from research import g4_trade_monitor_backtest as allocator
from research.g3_sl_target_simulator import simulate_exit


VERSION = "G4-SEPARATE-AND-COMBINED-SL-V1"
WORKSPACE = Path(__file__).resolve().parents[1]
CONFIG_DIR = WORKSPACE / "configs" / "research"
DEFAULT_CONFIGS = (
    CONFIG_DIR / "g4_profit_floor_020_v1.json",
    CONFIG_DIR / "g4_atr_trailing_150_v1.json",
    CONFIG_DIR / "g4_profit_floor_atr_150_v1.json",
)
VARIANT_NAMES = {"G4-PROFIT-FLOOR-020-V1": "profit_floor",
                 "G4-ATR-TRAILING-150-V1": "atr_trailing",
                 "G4-PROFIT-FLOOR-ATR-150-V1": "combined"}
MINUTE_NS = 60_000_000_000
IST = "Asia/Kolkata"


def _load_config(path: Path) -> dict:
    config = json.loads(Path(path).read_text(encoding="utf-8"))
    required = {
        "variant_id", "scope", "execution_authority", "initial_stop_pct",
        "tighten_after_minutes", "tightened_stop_pct", "activation_floor_pct",
        "activation_target_fraction", "protected_profit_pct", "cost_bps_round_trip",
    }
    missing = required - set(config)
    if missing:
        raise ValueError(f"Missing config fields: {sorted(missing)}")
    if config["scope"] != "BACKTEST_ONLY" or config["execution_authority"] is not False:
        raise ValueError("G4 SL experiments must remain backtest-only")
    numeric = [float(config[key]) for key in required if key not in {
        "variant_id", "scope", "execution_authority"} and config[key] is not None]
    if "atr_multiple" in config:
        numeric.append(float(config["atr_multiple"]))
    if not all(math.isfinite(value) for value in numeric):
        raise ValueError("All numeric parameters must be finite")
    if (config["protected_profit_pct"] is not None
            and not 0 <= config["protected_profit_pct"] < config["activation_floor_pct"]):
        raise ValueError("Protected profit must be below the activation floor")
    if not 0 < config["tightened_stop_pct"] <= config["initial_stop_pct"]:
        raise ValueError("Timed stop must tighten the initial stop")
    if not 0 < config["activation_target_fraction"] <= 1:
        raise ValueError("Target activation fraction must be in (0, 1]")
    if config["cost_bps_round_trip"] != 5.0:
        raise ValueError("This comparison requires the native five-bps cost")
    name = VARIANT_NAMES.get(config["variant_id"])
    if name is None:
        raise ValueError("Unknown variant id")
    if (name == "atr_trailing") != (config["protected_profit_pct"] is None):
        raise ValueError("Only the standalone ATR variant omits a profit floor")
    if name != "profit_floor" and not config.get("atr_multiple", 0) > 0:
        raise ValueError("ATR variants need a positive ATR multiple")
    if not (0 < config["initial_stop_pct"] < 100 and config["tighten_after_minutes"] > 0
            and config["activation_floor_pct"] > 0):
        raise ValueError("Invalid stop or activation parameters")
    return config


def _context_atr(context: pd.DataFrame) -> dict[int, float]:
    if "ts" not in context or "atr14" not in context:
        raise ValueError("ATR trailing requires timestamped atr14 context")
    stamps = pd.to_datetime(context.ts)
    if stamps.dt.tz is None or stamps.isna().any() or stamps.duplicated().any():
        raise ValueError("ATR context timestamps must be unique and timezone-aware")
    return dict(zip(stamps.dt.as_unit("ns").astype("int64"),
                    pd.to_numeric(context.atr14, errors="coerce")))


def simulate_atr_trailing_exit(
    path: Mapping,
    context: pd.DataFrame,
    entry_index: int,
    entry_price: float,
    is_long: bool,
    target_pct: float,
    config: Mapping,
) -> dict:
    """Replay the declared resting ATR trail using only completed closes.

    A close can arm or ratchet the stop only for the following candle. Gaps fill
    at the observed open and stop/target ambiguity is resolved stop-first.
    """
    entry = float(entry_price)
    target_pct = float(target_pct)
    initial = float(config["initial_stop_pct"])
    timed = float(config["tightened_stop_pct"])
    delay = float(config["tighten_after_minutes"])
    activation = max(float(config["activation_floor_pct"]),
                     float(config["activation_target_fraction"]) * target_pct)
    protected = config["protected_profit_pct"]
    protected = None if protected is None else float(protected)
    multiple = float(config["atr_multiple"])
    if not (entry > 0 and 0 < initial < 100 and 0 < timed <= initial
            and 0 < target_pct < 100 and (protected is None or 0 <= protected < activation)
            and delay > 0 and multiple > 0):
        raise ValueError("Invalid ATR trailing parameters")
    if not all(math.isfinite(value) for value in (entry, initial, timed, delay, activation, multiple)):
        raise ValueError("ATR parameters must be finite")
    if not isinstance(is_long, (bool, np.bool_)):
        raise ValueError("is_long must be boolean")
    if not isinstance(entry_index, (int, np.integer)) or isinstance(entry_index, (bool, np.bool_)):
        raise ValueError("entry_index must be an integer")
    stamps = np.asarray(path["timestamp_ns"])
    values = {name: np.asarray(path[name], dtype=float)
              for name in ("open", "high", "low", "close")}
    if (stamps.ndim != 1 or not np.issubdtype(stamps.dtype, np.integer)
            or any(array.ndim != 1 or len(array) != len(stamps) for array in values.values())
            or not 0 <= entry_index < len(stamps)):
        raise ValueError("Invalid one-minute execution path")
    if np.any(np.diff(stamps) <= 0) or any(not np.isfinite(array).all() or np.any(array <= 0)
                                        for array in values.values()):
        raise ValueError("Execution path must be finite and increasing")
    if (np.any(values["low"] > np.minimum(values["open"], values["close"]))
            or np.any(values["high"] < np.maximum(values["open"], values["close"]))):
        raise ValueError("Invalid OHLC candle")
    atr_by_stamp = _context_atr(context)
    entry_end = pd.Timestamp(int(stamps[entry_index]), tz="UTC").tz_convert(IST)
    squareoff_ns = int((entry_end.normalize() + pd.Timedelta(hours=15, minutes=15)).value)
    finish = int(np.searchsorted(stamps, squareoff_ns, side="right"))
    if entry_end.value > squareoff_ns:
        raise ValueError("Entry is later than native 15:15 square-off")
    if np.any(np.diff(stamps[entry_index:finish]) != MINUTE_NS):
        raise ValueError("ATR replay requires contiguous one-minute candles")

    sign = 1 if is_long else -1
    target = entry * (1 + sign * target_pct / 100)
    timer_ns = int(stamps[entry_index]) + round(delay * MINUTE_NS)
    loss_stop = entry * (1 - sign * initial / 100)
    stop_reason = "STOP"
    pending_trail = None
    trail_active = False
    armed_index = None
    effective_index = None
    best_close = None
    updates = []

    def tighter(current: float, candidate: float) -> float:
        return max(current, candidate) if is_long else min(current, candidate)

    def result(index: int, price: float, reason: str, event: str, *, ambiguous=False) -> dict:
        end = pd.Timestamp(int(stamps[index]), tz="UTC").tz_convert(IST)
        executed = end - pd.Timedelta(minutes=1) if event == "OPEN" else end
        gross = sign * (price / entry - 1) * 100
        last = index - 1 if event == "OPEN" else index
        if last >= entry_index:
            favorable = values["high" if is_long else "low"][entry_index:last + 1]
            adverse = values["low" if is_long else "high"][entry_index:last + 1]
            mfe = max(0.0, gross, float(np.max(sign * (favorable / entry - 1) * 100)))
            mae = min(0.0, gross, float(np.min(sign * (adverse / entry - 1) * 100)))
        else:
            mfe, mae = max(0.0, gross), min(0.0, gross)
        active_pct = sign * (1 - loss_stop / entry) * 100
        stopped = reason in {"STOP", "TIGHTENED_STOP", "ATR_TRAILING_STOP", "COMBINED_STOP"}
        gap = bool(stopped and event == "OPEN" and abs(price - loss_stop) > 1e-8)
        return {
            "exit_path_index": int(index), "exit_price": float(price),
            "exit_reason": reason, "exit_event": event, "exit_ts": executed,
            "exit_execution_ts": str(executed), "exit_bar_end_ts": str(end),
            "active_stop_pct_at_exit": float(active_pct),
            "active_stop_price": float(loss_stop), "initial_stop_pct": initial,
            "gross_return_pct": float(gross), "mfe_pct": float(mfe), "mae_pct": float(mae),
            "holding_minutes": (executed - entry_end).total_seconds() / 60,
            "stop_hit": stopped, "target_hit": reason == "TARGET",
            "first_target_hit": reason == "TARGET", "runner_target_hit": reason == "TARGET",
            "same_bar_ambiguous": bool(ambiguous), "exit_gap_through": gap,
            "exit_gap_bps": abs(price / loss_stop - 1) * 10000 if gap else 0.0,
            "protection_armed_path_index": armed_index,
            "protection_effective_path_index": effective_index,
            "protection_applied": trail_active,
            "stop_updates_json": json.dumps(updates),
        }

    for index in range(entry_index, finish):
        open_ns = int(stamps[index]) - MINUTE_NS
        if open_ns >= timer_ns:
            revised = tighter(loss_stop, entry * (1 - sign * timed / 100))
            if sign * (revised - loss_stop) > 1e-10:
                loss_stop, stop_reason = revised, "TIGHTENED_STOP"
        if pending_trail is not None:
            revised = tighter(loss_stop, pending_trail)
            if sign * (revised - loss_stop) > 1e-10:
                loss_stop = revised
                stop_reason = "ATR_TRAILING_STOP" if protected is None else "COMBINED_STOP"
                trail_active = True
                if effective_index is None:
                    effective_index = index
            pending_trail = None
        op, high, low, close = (float(values[name][index])
                                for name in ("open", "high", "low", "close"))
        stop_open = op <= loss_stop if is_long else op >= loss_stop
        stop_hit = low <= loss_stop if is_long else high >= loss_stop
        target_hit = high >= target if is_long else low <= target
        if stop_hit:
            at_open = index > entry_index and stop_open
            return result(index, op if at_open else loss_stop, stop_reason,
                          "OPEN" if at_open else "INTRABAR",
                          ambiguous=not at_open and target_hit)
        if target_hit:
            return result(index, target, "TARGET", "INTRABAR")
        if int(stamps[index]) == squareoff_ns:
            return result(index, close, "TIME_EXIT_1515", "CLOSE")
        if index <= entry_index:
            continue
        best_close = close if best_close is None else (
            max(best_close, close) if is_long else min(best_close, close))
        activation_price = entry * (1 + sign * activation / 100)
        if armed_index is None and (close >= activation_price if is_long else close <= activation_price):
            armed_index = index
        if armed_index is not None:
            atr = float(atr_by_stamp.get(int(stamps[index]), np.nan))
            if not np.isfinite(atr) or atr <= 0:
                raise ValueError("Missing or invalid ATR at a required trailing update")
            candidate = best_close - sign * multiple * atr
            if protected is not None:
                floor = entry * (1 + sign * protected / 100)
                candidate = tighter(candidate, floor)
            pending_trail = tighter(loss_stop, candidate)
            updates.append(dict(decision_path_index=index, effective_path_index=index + 1,
                best_completed_close=best_close, atr=atr, proposed_stop=candidate,
                ratcheted_stop=pending_trail))
    raise ValueError("Position survives a path missing its native 15:15 square-off candle")


def _baseline_outcome(row: Mapping, path: Mapping) -> dict:
    return simulate_exit(path, int(row["entry_path_index"]), float(row["entry_price"]),
        row["side"] == "LONG", 1.2, float(row["native_target_pct"]),
        tighten_after_minutes=120, tightened_stop_pct=1.0)


def _variant_outcome(row: Mapping, path: Mapping, context: pd.DataFrame, config: Mapping) -> dict:
    if config["variant_id"] == "G4-PROFIT-FLOOR-020-V1":
        activation = max(float(config["activation_floor_pct"]),
                         float(config["activation_target_fraction"]) * float(row["native_target_pct"]))
        return simulate_exit(path, int(row["entry_path_index"]), float(row["entry_price"]),
            row["side"] == "LONG", float(config["initial_stop_pct"]),
            float(row["native_target_pct"]), protect_after_close_pct=activation,
            protected_profit_pct=float(config["protected_profit_pct"]),
            tighten_after_minutes=float(config["tighten_after_minutes"]),
            tightened_stop_pct=float(config["tightened_stop_pct"]))
    if config["variant_id"] in {"G4-ATR-TRAILING-150-V1", "G4-PROFIT-FLOOR-ATR-150-V1"}:
        return simulate_atr_trailing_exit(path, context, int(row["entry_path_index"]),
            float(row["entry_price"]), row["side"] == "LONG",
            float(row["native_target_pct"]), config)
    raise ValueError(f"Unknown variant: {config['variant_id']}")


def _apply_outcomes(native: pd.DataFrame, contexts: Mapping, paths: Mapping,
                    config: Mapping) -> tuple[pd.DataFrame, pd.DataFrame]:
    records = []
    changes = []
    for _, row in native.iterrows():
        record = row.to_dict()
        record.update(monitor_exit=False, monitor_flags="[]", stop_updates_json="[]")
        if not bool(row.filled):
            records.append(record)
            continue
        sid = int(row.sid)
        baseline = _baseline_outcome(row, paths[sid])
        if (not np.isclose(baseline["exit_price"], float(row.exit_price), atol=1e-8, rtol=0)
                or pd.Timestamp(baseline["exit_ts"]) != pd.Timestamp(row.exit_ts)
                or baseline["exit_reason"] != row.exit_reason
                or not np.isclose(baseline["gross_return_pct"], float(row.gross_return_pct), atol=1e-10, rtol=0)):
            raise ValueError(f"Native parity failed for sid {sid}")
        outcome = _variant_outcome(row, paths[sid], contexts[sid], config)
        changed = (not np.isclose(outcome["exit_price"], baseline["exit_price"], atol=1e-8, rtol=0)
                   or pd.Timestamp(outcome["exit_ts"]) != pd.Timestamp(baseline["exit_ts"]))
        record.update(outcome)
        record.update(net_return_pct=outcome["gross_return_pct"] - .05, cost_pct=.05,
                      monitor_exit=changed,
                      monitor_flags=json.dumps([config["variant_id"]]) if changed else "[]")
        records.append(record)
        changes.append({
            "variant_id": config["variant_id"], "sid": sid, "day": str(row.day)[:10],
            "setup_id": row.setup_id, "symbol": row.tradingsymbol, "side": row.side,
            "changed": changed, "baseline_exit_reason": baseline["exit_reason"],
            "originally_executed": bool(row.portfolio_executed),
            "variant_exit_reason": outcome["exit_reason"],
            "baseline_exit_ts": str(baseline["exit_ts"]), "variant_exit_ts": str(outcome["exit_ts"]),
            "baseline_net_rs": (baseline["gross_return_pct"] - .05) * 5000,
            "variant_net_rs": (outcome["gross_return_pct"] - .05) * 5000,
            "delta_net_rs": (outcome["gross_return_pct"] - baseline["gross_return_pct"]) * 5000,
            "protection_armed_path_index": outcome.get("protection_armed_path_index"),
            "protection_effective_path_index": outcome.get("protection_effective_path_index"),
            "active_stop_price": outcome["active_stop_price"],
            "exit_gap_through": outcome["exit_gap_through"],
            "same_bar_ambiguous": outcome["same_bar_ambiguous"],
        })
    return pd.DataFrame(records), pd.DataFrame(changes)


def portfolio_stats(book: pd.DataFrame, days: list[str]) -> dict:
    stats = core.bounded.stats(book, days)
    trades = book.loc[book.portfolio_executed & book.day.isin(days)]
    pnl = pd.to_numeric(trades.portfolio_net_profit_rupees)
    stats["native_loss_stop_exits"] = stats.pop("stop_exits")
    stats["protective_stop_exits"] = int(trades.exit_reason.isin(
        ["PROTECTED_STOP", "ATR_TRAILING_STOP", "COMBINED_STOP"]).sum())
    stats["all_stop_exits"] = stats["native_loss_stop_exits"] + stats["protective_stop_exits"]
    stats["worst_trade_rs"] = float(pnl.min()) if len(pnl) else 0.0
    stats["average_loss_rs"] = float(pnl.loc[pnl < 0].mean()) if (pnl < 0).any() else 0.0
    stats["profit_factor"] = float(pnl.loc[pnl > 0].sum() / -pnl.loc[pnl < 0].sum()) if (pnl < 0).any() else None
    stats["ambiguous_exits"] = int(trades.same_bar_ambiguous.sum())
    stats["gap_through_exits"] = int(trades.exit_gap_through.sum())
    return stats


def _daily(book: pd.DataFrame, days: list[str]) -> pd.DataFrame:
    executed = book.loc[book.portfolio_executed].copy()
    values = executed.groupby("day").portfolio_net_profit_rupees.sum().reindex(days, fill_value=0.0)
    return pd.DataFrame({"day": days, "net_pnl_rs": values.to_numpy(),
                         "cumulative_net_pnl_rs": values.cumsum().to_numpy()})


def _write_variant(directory: Path, config: dict, native: pd.DataFrame,
                   contexts: Mapping, paths: Mapping, days: list[str]) -> tuple[dict, pd.DataFrame, pd.DataFrame]:
    directory.mkdir()
    raw, changes = _apply_outcomes(native, contexts, paths, config)
    book = allocator.allocate(raw, config["variant_id"])
    book.to_csv(directory / "portfolio_ledger.csv", index=False)
    changes.to_csv(directory / "trade_changes.csv", index=False)
    changes.loc[changes.changed & changes.originally_executed].to_csv(
        directory / "changed_executed_trades.csv", index=False)
    _daily(book, days).to_csv(directory / "daily_results.csv", index=False)
    (directory / "config.json").write_text(json.dumps(config, indent=2) + "\n", encoding="utf-8")
    original = native.set_index("sid")
    result = book.set_index("sid")
    meta = native.loc[native.portfolio_executed].set_index("sid", drop=False)
    base_returns = pd.to_numeric(meta.net_return_pct) * 5000
    candidate_returns = pd.to_numeric(raw.set_index("sid").loc[meta.index, "net_return_pct"]) * 5000
    if not np.allclose(result.loc[meta.index, "net_profit_rupees"], candidate_returns, atol=1e-6, rtol=0):
        raise ValueError("Portfolio sizing differs from fixed-cohort accounting")
    fixed = core.metrics(base_returns, candidate_returns, meta, days)
    fixed_later = core.metrics(base_returns, candidate_returns, meta, days[35:])
    portfolio = portfolio_stats(book, days)
    baseline_portfolio = portfolio_stats(native, days)
    periods = {"all": days, "first35": days[:35], "later17": days[35:]}
    period_rows = [{"period": period, **core.metrics(base_returns, candidate_returns, meta, cohort)}
                   for period, cohort in periods.items()]
    pd.DataFrame(period_rows).to_csv(directory / "period_results.csv", index=False)
    setup_rows = []
    for setup in sorted(native.setup_id.unique()):
        members = meta.loc[meta.setup_id.eq(setup)]
        setup_rows.append(dict(setup_id=setup, **core.metrics(
            base_returns.loc[members.index], candidate_returns.loc[members.index], members, days)))
    pd.DataFrame(setup_rows).to_csv(directory / "setup_results.csv", index=False)
    extra = changes.set_index("sid").loc[meta.index, "changed"].astype(float) * 250.
    stresses = []
    for scenario, before, after in [
        ("original_5bps", base_returns, candidate_returns),
        ("extra_5bps_changed_exits", base_returns, candidate_returns - extra),
        ("10bps_all_trades", base_returns - 250., candidate_returns - 250.),
    ]:
        for period, cohort in periods.items():
            stresses.append(dict(scenario=scenario, period=period,
                                  **core.metrics(before, after, meta, cohort)))
    pd.DataFrame(stresses).to_csv(directory / "cost_stress.csv", index=False)
    daily_fixed = pd.DataFrame([dict(day=day, **core.metrics(base_returns, candidate_returns, meta, [day]))
                                for day in days])
    daily_fixed.to_csv(directory / "daily_fixed_cohort.csv", index=False)
    summary = {
        "variant_id": config["variant_id"], "scope": "BACKTEST_ONLY",
        "execution_authority": False, "original_executed_cohort": fixed,
        "later17_original_executed_cohort": fixed_later,
        "full_portfolio": portfolio, "baseline_full_portfolio": baseline_portfolio,
        "newly_admitted": int((result.portfolio_executed & ~original.portfolio_executed).sum()),
        "originally_executed_now_rejected": int((~result.portfolio_executed & original.portfolio_executed).sum()),
        "changed_filled_orders": int(changes.changed.sum()),
        "changed_original_executed_trades": int((changes.changed & changes.originally_executed).sum()),
        "paired_day_bootstrap": core.bootstrap_delta(base_returns, candidate_returns, meta, days),
    }
    (directory / "summary.json").write_text(
        json.dumps(base.source._json_ready(summary), indent=2, allow_nan=False) + "\n", encoding="utf-8")
    return summary, book, changes


def run(output: Path, data_dir: Path = core.DEFAULT_DATA) -> Path:
    output = Path(output).resolve()
    research_root = (base.G4_ROOT / "research").resolve()
    if output.exists() or output.parent != research_root:
        raise ValueError("Output must be a fresh directory directly below the G4 research root")
    configs = [_load_config(path) for path in DEFAULT_CONFIGS]
    dataset_hash = base.sha256(Path(data_dir) / "manifest.json")
    native, contexts, paths, proof = dataset.load(data_dir)
    native["day"] = native.day.astype(str).str[:10]
    native = native.sort_values(["day", "confirmation_ts", "sid"], kind="stable").reset_index(drop=True)
    days = list(proof["days"])
    pointer_paths = [base.G4_ROOT / "latest" / "bounded_exit.json",
                     base.G4_ROOT / "latest" / "g3_mirror.json"]
    pointer_hashes = {str(path): base.sha256(path) for path in pointer_paths if path.exists()}
    protected_files = [*(WORKSPACE / "configs" / "strategies").glob("*g_4*.json"),
                       WORKSPACE / "configs/strategies/active_profile.json",
                       WORKSPACE / "fno_v13_v10_g_4_backtest.py"]
    protected_hashes = {str(path): base.sha256(path) for path in protected_files if path.exists()}
    source_files = [Path(__file__).resolve(), *DEFAULT_CONFIGS,
                    WORKSPACE / "tests/test_g4_separate_sl_variants.py",
                    WORKSPACE / "docs/G4_SL_VARIANTS_2026-10-17.md"]
    source_hashes = {str(path): base.sha256(path) for path in source_files}
    support_files = [WORKSPACE / "tools/g4_sl_variants_rollback.py",
                     WORKSPACE / "tests/test_g4_sl_variants_rollback.py"]
    support_hashes = {str(path): base.sha256(path) for path in support_files}
    dependencies = set()
    for module in list(sys.modules.values()):
        filename = getattr(module, "__file__", None)
        if filename:
            path = Path(filename).resolve()
            if path.is_relative_to(WORKSPACE) and path.is_file() and path.suffix == ".py":
                dependencies.add(path)
    dependency_hashes = {str(path): base.sha256(path) for path in sorted(dependencies)}
    output.mkdir(parents=True)
    for path in set(source_files) | set(support_files) | dependencies:
        snapshot = output / "source_snapshot" / path.relative_to(WORKSPACE)
        snapshot.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(path, snapshot)
    core.json_write(output / "protocol.json", dict(
        version=VERSION, scope="BACKTEST_ONLY", execution_authority=False,
        configs=configs, dataset_manifest_sha256=dataset_hash,
        declared_before_this_replay=True, parameter_optimization=False,
        primary_cohort="Same originally executed 466 trades; secondary full 533-order portfolio replay",
        common_activation="max(0.40%, 90% of native target) from fully completed post-entry close",
        atr_definition="14-bar simple mean of true range, from causal sealed context; includes completed signal bar",
        timing="Next-candle updates; never loosen; gap-through at open; stop-first on same-bar target ambiguity",
        source_sha256=source_hashes, dependency_sha256=dependency_hashes, rollback_support_sha256=support_hashes,
        existing_strategy_sha256=protected_hashes, latest_pointer_sha256=pointer_hashes))
    control = allocator.allocate(native.copy(), "baseline")
    core.sealed.parity(control, native)
    (output / "baseline").mkdir()
    control.to_csv(output / "baseline/portfolio_ledger.csv", index=False)
    _daily(control, days).to_csv(output / "baseline/daily_results.csv", index=False)
    print(f"Baseline allocation reproduced: {len(native)} orders / {int(native.portfolio_executed.sum())} executions", flush=True)
    summaries, books = {}, {}
    for config in configs:
        name = VARIANT_NAMES[config["variant_id"]]
        print(f"Replaying {name}", flush=True)
        summary, book, _ = _write_variant(output / name, config, native, contexts, paths, days)
        summaries[name] = summary
        books[name] = book
    baseline_stats = portfolio_stats(native, days)
    rows = [{"variant": "baseline", **baseline_stats}]
    rows += [{"variant": name, **summary["full_portfolio"]} for name, summary in summaries.items()]
    pd.DataFrame(rows).to_csv(output / "portfolio_comparison.csv", index=False)
    meta = native.loc[native.portfolio_executed].set_index("sid", drop=False)
    before = pd.to_numeric(meta.net_return_pct) * 5000.
    fixed_rows = [{"variant": "baseline", **core.metrics(before, before, meta, days)}]
    for name, summary in summaries.items():
        fixed_rows.append({"variant": name, **summary["original_executed_cohort"]})
    pd.DataFrame(fixed_rows).to_csv(output / "fixed_cohort_comparison.csv", index=False)
    daily_comparison = _daily(control, days).rename(columns={"net_pnl_rs": "baseline_net_rs"})[["day", "baseline_net_rs"]]
    for name, book in books.items():
        daily_comparison[name + "_net_rs"] = _daily(book, days).net_pnl_rs
        daily_comparison[name + "_delta_rs"] = daily_comparison[name + "_net_rs"] - daily_comparison.baseline_net_rs
    daily_comparison.to_csv(output / "daily_comparison.csv", index=False)
    later_rows = [{"variant": "baseline", **core.metrics(before, before, meta, days[35:])}]
    later_rows += [{"variant": name, **summary["later17_original_executed_cohort"]} for name, summary in summaries.items()]
    pd.DataFrame(later_rows).to_csv(output / "later17_comparison.csv", index=False)
    atr_book, combined_book = (books[name].set_index("sid") for name in ("atr_trailing", "combined"))
    filled = atr_book.filled.eq(True)
    differing_exits = ((atr_book.loc[filled, "exit_price"] - combined_book.loc[filled, "exit_price"]).abs().gt(1e-8)
                       | (atr_book.loc[filled, "exit_ts"] != combined_book.loc[filled, "exit_ts"]))
    updates = [(float(row.entry_price), 1 if row.side == "LONG" else -1, update)
               for row in atr_book.loc[filled].itertuples()
               for update in json.loads(row.stop_updates_json)]
    combined_comparison = dict(
        different_exit_outcomes=int(differing_exits.sum()), atr_updates=len(updates),
        atr_proposals_below_fixed_floor=sum(sign * (update["proposed_stop"] / entry - 1) * 100 < .2 - 1e-9
                                           for entry, sign, update in updates))
    rollback = {
        "workspace_root": str(WORKSPACE),
        "production_or_latest_files_changed": False,
        "pre_run_pointer_sha256": pointer_hashes,
        "new_repository_files": [str(path) for path in source_files],
        "new_repository_file_sha256": source_hashes,
        "generated_result_directory": str(output),
        "rollback": "Use tools/g4_sl_variants_rollback.py --run-dir <this directory> to preview, add --apply to archive these six new files. Results and rollback utility remain available; --restore <receipt.json> --apply restores archived files after hash checks."
    }
    (output / "rollback.json").write_text(json.dumps(rollback, indent=2) + "\n", encoding="utf-8")
    combined = {
        "variant_id": VERSION, "scope": "BACKTEST_ONLY", "execution_authority": False,
        "created_at_ist": datetime.now(base.production.IST).isoformat(),
        "dataset": str(Path(data_dir).resolve()), "sessions": len(days),
        "dataset_manifest_sha256": dataset_hash, "first_day": days[0], "last_day": days[-1],
        "original_source": proof["original_source"],
        "original_manifest_sha256": proof["original_manifest_sha256"],
        "orders": len(native), "original_executed_trades": int(native.portfolio_executed.sum()),
        "source_sha256": source_hashes, "dependency_sha256": dependency_hashes,
        "rollback_support_sha256": support_hashes,
        "pre_run_pointer_sha256": pointer_hashes, "existing_strategy_sha256": protected_hashes,
        "results": summaries, "baseline": baseline_stats,
        "native_control_exactly_reproduced": True,
        "combined_vs_atr": combined_comparison,
        "interpretation": "Fixed floor, standalone ATR, and combined floor+ATR are three independently replayed policies. All retain native targets and stops. Primary results use the original trades; secondary results reapply the original G4 capital allocator.",
    }
    (output / "summary.json").write_text(
        json.dumps(base.source._json_ready(combined), indent=2, allow_nan=False) + "\n", encoding="utf-8")
    _report(output, combined, configs, fixed_rows, later_rows, rows)
    for path, digest in {**pointer_hashes, **protected_hashes, **source_hashes, **dependency_hashes, **support_hashes}.items():
        if base.sha256(Path(path)) != digest:
            raise ValueError("Source, dependency, or existing configuration changed during replay: " + path)
    if base.sha256(Path(data_dir) / "manifest.json") != dataset_hash:
        raise ValueError("Dataset manifest changed during replay")
    core.json_write(output / "validation.json", dict(
        native_and_portfolio_parity=True, existing_strategy_and_latest_hashes_unchanged=True,
        frozen_source_and_dependencies_unchanged=True, dataset_manifest_unchanged=True,
        rules=len(configs), entries_targets_and_costs_preserved=True))
    manifest = {path.relative_to(output).as_posix(): base.sha256(path)
                for path in output.rglob("*") if path.is_file()}
    (output / "manifest.json").write_text(json.dumps({"files": manifest}, indent=2) + "\n", encoding="utf-8")
    print(output, flush=True)
    return output


def _report(output, summary, configs, fixed_rows, later_rows, portfolio_rows):
    labels = {"variant": "Variant", "wins": "Wins", "losses": "Losses", "trades": "Trades",
        "win_rate_pct": "Win %", "negative_trade_sum_abs_rs": "Total losing P&L (Rs)",
        "loss_reduction_pct": "Loss reduction %", "net_pnl_rs": "Net P&L (Rs)",
        "net_change_rs": "Net change (Rs)", "original_winner_profit_retention_pct": "Winner profit retained %",
        "changed_trades": "P&L changed trades", "daily_close_drawdown_rs": "Daily-close drawdown (Rs)"}
    columns = list(labels)
    def table(rows, selected):
        return '<div class="scroll">' + pd.DataFrame(rows)[selected].rename(columns=labels).to_html(
            index=False, border=0, float_format=lambda value: f"{value:,.2f}") + '</div>'
    doc = "<!doctype html><html lang='en'><head><meta charset='utf-8'><meta name='viewport' content='width=device-width,initial-scale=1'><title>G4: floor, ATR and combined SL results</title>"
    doc += "<style>body{font:15px/1.55 system-ui;margin:28px;background:#101827;color:#e6edf7}main{max-width:1600px;margin:auto}section{background:#18243a;padding:22px;margin:18px 0;border-radius:10px}.scroll{overflow:auto}table{border-collapse:collapse;font-size:13px;white-space:nowrap}th,td{padding:10px;border-bottom:1px solid #40506a;text-align:right}th:first-child,td:first-child{text-align:left}a{color:#8dd1ff}pre{white-space:pre-wrap;overflow-wrap:anywhere;background:#101827;padding:14px}h1{font-size:28px}code{color:#b6e8cd}</style></head><body><main>"
    doc += "<h1>G4: fixed profit floor, ATR trailing, and combined protection</h1>"
    doc += f"<p>{summary['first_day']} to {summary['last_day']} | {summary['sessions']} sessions | {summary['orders']} selected orders | {summary['original_executed_trades']} original executions</p>"
    doc += "<section><h2>What was implemented</h2><p>Three research configurations, each preserving the native 1.20% SL, tightening to 1.00% after 120 minutes, native targets and 15:15 square-off. All protection activates after a completed post-entry close reaches max(0.40%, 90% of the target). Parameters were fixed before this replay; there was no new optimization.</p><ul>"
    for config in configs:
        doc += '<li><strong>' + html.escape(VARIANT_NAMES[config['variant_id']]) + ':</strong> ' + html.escape(config['description']) + '</li>'
    doc += "</ul><p>ATR means the 14-bar simple average of true range in the existing causal feature cache. Stop updates apply on the following candle, never loosen, fill adverse gaps at observed opens and use stop-first ordering for ambiguous candles.</p></section>"
    doc += '<section><h2>Same original trades: full history</h2><p>Rs500,000 exposure per trade; flat 5 bps round-trip model cost. Total losing P&amp;L is the absolute sum of negative trades, distinct from maximum drawdown.</p>' + table(fixed_rows, columns) + '</section>'
    evidence = summary['combined_vs_atr']
    doc += '<section><h2>What explains the difference</h2>'
    for name, result in summary['results'].items():
        item = result['original_executed_cohort']
        doc += f"<p><strong>{name}</strong>: net change Rs{item['net_change_rs']:,.2f}; losing-trade amount reduced by Rs{item['loss_saved_rs']:,.2f}; {item['changed_trades']} changed trade P&amp;Ls; {item['original_winners_harmed']} original winners earned less, and {item['original_winners_turned_losses']} became losses.</p>"
    doc += f"<p>ATR and combined have {evidence['different_exit_outcomes']} differing exits. Of {evidence['atr_updates']} ATR updates, {evidence['atr_proposals_below_fixed_floor']} proposed a stop below +0.20% gross. When ATR protection is already tighter, the fixed floor adds no further protection.</p></section>"
    doc += '<section><h2>Later 17 sessions</h2><p>Chronological diagnostic on reused history; these sessions are not an untouched test.</p>' + table(later_rows, columns) + '</section>'
    doc += '<section><h2>Full portfolio replay</h2><p>Reapplies the original Rs1,000,000 capital / 10-position allocator to all 533 orders, allowing earlier exits to affect later admissions.</p>' + table(portfolio_rows, ["variant", "trades", "wins", "losses", "net_pnl_rs", "negative_trade_sum_abs_rs", "daily_close_drawdown_rs"]) + '<ul>'
    for name, item in summary['results'].items():
        doc += f"<li>{name}: {item['newly_admitted']} new admissions; {item['originally_executed_now_rejected']} original trades rejected.</li>"
    doc += '</ul></section><section><h2>Trade and daily evidence</h2><ul>'
    for name in summary['results']:
        doc += f"<li>{name}: <a href='{name}/changed_executed_trades.csv'>changed original trades</a> | <a href='{name}/portfolio_ledger.csv'>complete ledger</a> | <a href='{name}/setup_results.csv'>all setups</a> | <a href='{name}/cost_stress.csv'>cost stress</a> | <a href='{name}/summary.json'>summary</a></li>"
    doc += "<li><a href='daily_comparison.csv'>Daily comparison</a> | <a href='fixed_cohort_comparison.csv'>Fixed-cohort comparison</a> | <a href='portfolio_comparison.csv'>Portfolio comparison</a></li></ul></section>"
    cmd = f'python tools/g4_sl_variants_rollback.py --run-dir "{output}"'
    doc += '<section><h2>Rollback</h2><p>Existing G4 configurations and latest pointers passed before/after hash checks. Preview archiving the six added research/config/test/doc files:</p><pre>' + html.escape(cmd) + '</pre><p>Add <code>--apply</code> to archive them. Results remain available. To restore, keep the same command and add <code>--restore &lt;absolute receipt.json path&gt; --apply</code>. Modified files and overwrite conflicts are refused.</p><p><a href="rollback.json">Rollback inventory and hashes</a> | <a href="validation.json">Validation</a> | <a href="protocol.json">Frozen protocol</a></p></section>'
    doc += '<section><h2>Limits of these results</h2><p>The pinned 51-setup cohort ends 15 October and was already used for G4 development; this report does not claim to evaluate later entries or the latest changed entry profile. These are implemented backtest variants, with no live/paper activation. One-minute OHLC cannot establish intrabar tick sequence or actual stop execution. The 5 bps cost model omits market impact and broker-specific charges; extra-cost stress files show sensitivity. Daily-close drawdown excludes intraday open-position drawdown. A result affecting only a few trades remains weak evidence even when profitable.</p></section></main></body></html>'
    (output / 'report.html').write_text(doc, encoding='utf-8')


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--dataset", type=Path, default=core.DEFAULT_DATA)
    args = parser.parse_args()
    stamp = datetime.now(base.production.IST)
    destination = args.output_dir or base.G4_ROOT / "research" / f"{stamp:%Y%m%dT%H%M%S}_{secrets.token_hex(3)}_separate_sl_v1"
    run(destination, args.dataset)
