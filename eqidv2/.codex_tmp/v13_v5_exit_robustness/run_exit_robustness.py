from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass, asdict
from datetime import date
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[2]
OUT = Path(__file__).resolve().parent
SOURCE = Path(r"C:\TradingData\eqidv2\fno_oi\strategy_research")
V3_DIR = SOURCE / "v13_corrected_v3"
V4_DIR = SOURCE / "v13_corrected_v4"
PROVENANCE_PATH = V3_DIR / "fno_v13_corrected_v3_provenance.json"
V3_TRADES_PATH = V3_DIR / "fno_v13_corrected_v3_trades.csv"
V4_NATIVE_PATH = V4_DIR / "fno_v13_corrected_v4_v13_v3_trades.csv"
V4_TRADES_PATH = V4_DIR / "fno_v13_corrected_v4_trades.csv"

SPLIT_DAY = date(2026, 8, 14)
TEST_END = date(2026, 9, 1)
BASE_COST_BPS = 5.0
RNG_SEED = 1305007


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def py(v: Any) -> Any:
    if isinstance(v, (np.integer,)):
        return int(v)
    if isinstance(v, (np.floating,)):
        if np.isnan(v):
            return None
        if np.isposinf(v):
            return "Infinity"
        if np.isneginf(v):
            return "-Infinity"
        return float(v)
    if isinstance(v, (date, pd.Timestamp)):
        return str(v)
    if isinstance(v, Path):
        return str(v)
    if isinstance(v, dict):
        return {str(k): py(value) for k, value in v.items()}
    if isinstance(v, (list, tuple)):
        return [py(value) for value in v]
    return v


def write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(py(payload), indent=2, sort_keys=True), encoding="utf-8")


def load_frozen() -> tuple[pd.DataFrame, dict[int, dict[str, np.ndarray]], list[date], dict[str, Any]]:
    provenance = json.loads(PROVENANCE_PATH.read_text(encoding="utf-8"))
    parts: list[tuple[pd.DataFrame, dict[int, dict[str, np.ndarray]]]] = []
    cache_checks: list[dict[str, Any]] = []
    suffix_map = {"h": "high", "l": "low", "c": "close"}
    for record in provenance["cache_records"]:
        parquet = Path(record["cache_parquet"])
        npz = Path(record["cache_npz"])
        actual_parquet_hash = sha256(parquet)
        actual_npz_hash = sha256(npz)
        checks = {
            "contract_month": record["contract_month"],
            "parquet": str(parquet),
            "npz": str(npz),
            "parquet_sha256": actual_parquet_hash,
            "npz_sha256": actual_npz_hash,
            "parquet_hash_match": actual_parquet_hash == record["cache_parquet_sha256"],
            "npz_hash_match": actual_npz_hash == record["cache_npz_sha256"],
        }
        if not checks["parquet_hash_match"] or not checks["npz_hash_match"]:
            raise RuntimeError(f"Frozen cache hash mismatch: {checks}")
        signals = pd.read_parquet(parquet)
        signals["day"] = pd.to_datetime(signals["day"]).dt.date
        blob = np.load(npz)
        paths: dict[int, dict[str, np.ndarray]] = {}
        for key in blob.files:
            sid_text, suffix = key.rsplit("_", 1)
            paths.setdefault(int(sid_text), {})[suffix_map[suffix]] = blob[key].astype(float)
        parts.append((signals, paths))
        checks["signal_rows"] = len(signals)
        checks["paths"] = len(paths)
        cache_checks.append(checks)

    frames: list[pd.DataFrame] = []
    merged_paths: dict[int, dict[str, np.ndarray]] = {}
    offset = 0
    for signals, paths in parts:
        block = signals.copy()
        block["sid"] = block["sid"].astype(int) + offset
        frames.append(block)
        for sid, path in paths.items():
            merged_paths[int(sid) + offset] = path
        offset += max(paths) + 1 if paths else 0
    signals = pd.concat(frames, ignore_index=True).sort_values(["day", "sid"]).reset_index(drop=True)

    orders = pd.read_csv(V4_NATIVE_PATH)
    orders["day"] = pd.to_datetime(orders["day"]).dt.date
    orders["sid"] = orders["sid"].astype(int)
    orders["hhmm_int"] = orders["hhmm_int"].astype(int)
    official = pd.read_csv(V3_TRADES_PATH)
    official["day"] = pd.to_datetime(official["day"]).dt.date
    official["sid"] = official["sid"].astype(int)
    key_cols = ["day", "sid", "tradingsymbol", "setup_id", "side"]
    if set(map(tuple, orders[key_cols].to_numpy())) != set(map(tuple, official[key_cols].to_numpy())):
        raise RuntimeError("V4 native baseline and official V3 selected trade keys differ")
    signal_lookup = signals.set_index("sid")
    missing_sids = sorted(set(orders["sid"]) - set(signal_lookup.index))
    if missing_sids:
        raise RuntimeError(f"Selected sids missing from frozen cache: {missing_sids}")
    trigger_delta = []
    for row in orders.itertuples(index=False):
        trigger_delta.append(abs(float(row.trigger) - float(signal_lookup.loc[int(row.sid), "trigger"])))
    if max(trigger_delta, default=0.0) > 1e-10:
        raise RuntimeError(f"Trigger mismatch against frozen signals: {max(trigger_delta)}")

    days = [date.fromisoformat(item) for item in provenance["sessions"]]
    audit = {
        "provenance": str(PROVENANCE_PATH),
        "provenance_sha256": sha256(PROVENANCE_PATH),
        "v3_trades": str(V3_TRADES_PATH),
        "v3_trades_sha256": sha256(V3_TRADES_PATH),
        "v4_native": str(V4_NATIVE_PATH),
        "v4_native_sha256": sha256(V4_NATIVE_PATH),
        "cache_checks": cache_checks,
        "sessions": [str(day) for day in days],
        "selected_orders": len(orders),
        "signal_rows": len(signals),
        "paths": len(merged_paths),
        "max_trigger_abs_delta": max(trigger_delta, default=0.0),
    }
    return orders, merged_paths, days, audit


def periods(days: list[date]) -> dict[str, list[date]]:
    return {
        "train": [d for d in days if d < SPLIT_DAY],
        "test": [d for d in days if SPLIT_DAY <= d <= TEST_END],
        "latest": [d for d in days if d > TEST_END],
        "all": list(days),
    }


def _first_touch(path: dict[str, np.ndarray], side: str, trigger: float) -> int | None:
    if side == "LONG":
        hits = np.flatnonzero(path["high"] >= trigger)
    else:
        hits = np.flatnonzero(path["low"] <= trigger)
    return int(hits[0]) if hits.size else None


def _entry(
    path: dict[str, np.ndarray],
    side: str,
    trigger: float,
    delay_bars: int,
    adverse_fill_bps: float,
) -> tuple[int, int, float] | None:
    touch = _first_touch(path, side, trigger)
    if touch is None:
        return None
    entry_index = touch + int(delay_bars)
    if entry_index >= len(path["close"]):
        return None
    if delay_bars:
        close_fill = float(path["close"][entry_index])
        if side == "LONG":
            fill = max(close_fill, trigger * (1 + adverse_fill_bps / 10000.0))
        else:
            fill = min(close_fill, trigger * (1 - adverse_fill_bps / 10000.0))
        first_exit_index = min(entry_index + 1, len(path["close"]) - 1)
    else:
        fill = trigger * (1 + adverse_fill_bps / 10000.0) if side == "LONG" else trigger * (1 - adverse_fill_bps / 10000.0)
        first_exit_index = entry_index
    return entry_index, first_exit_index, float(fill)


def _gross_from_price(side: str, entry: float, exit_price: float) -> float:
    return exit_price / entry - 1.0 if side == "LONG" else 1.0 - exit_price / entry


def simulate_fixed(
    orders: pd.DataFrame,
    paths: dict[int, dict[str, np.ndarray]],
    *,
    stop_pct: float | None = None,
    target_pct: float | None = None,
    cost_bps: float = BASE_COST_BPS,
    max_hold_bars: int | None = None,
    delay_bars: int = 0,
    adverse_fill_bps: float = 0.0,
    levels_anchor_trigger: bool = False,
    label: str = "FIXED",
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    cost = cost_bps / 10000.0
    for order in orders.itertuples(index=False):
        row = order._asdict()
        path = paths[int(order.sid)]
        entry_data = _entry(path, order.side, float(order.trigger), delay_bars, adverse_fill_bps)
        if entry_data is None:
            row.update({"filled": False, "net_return_pct": np.nan, "objective_hit": False, "exit_reason": "UNFILLED", "entry_index": np.nan, "exit_index": np.nan, "holding_bars": np.nan, "elapsed_minutes": np.nan, "capital_minutes": np.nan})
            rows.append(row)
            continue
        entry_index, first_exit_index, entry_price = entry_data
        level_anchor = float(order.trigger) if levels_anchor_trigger else entry_price
        stop = float(order.native_stop_pct if stop_pct is None else stop_pct)
        target = float(order.native_target_pct if target_pct is None else target_pct)
        last = len(path["close"]) - 1
        end_index = last if max_hold_bars is None else min(last, entry_index + int(max_hold_bars))
        high = path["high"][first_exit_index : end_index + 1]
        low = path["low"][first_exit_index : end_index + 1]
        missing = len(high) + 1
        if order.side == "LONG":
            stop_price = level_anchor * (1 - stop / 100.0)
            target_price = level_anchor * (1 + target / 100.0)
            stop_hits = np.flatnonzero(low <= stop_price)
            target_hits = np.flatnonzero(high >= target_price)
        else:
            stop_price = level_anchor * (1 + stop / 100.0)
            target_price = level_anchor * (1 - target / 100.0)
            stop_hits = np.flatnonzero(high >= stop_price)
            target_hits = np.flatnonzero(low <= target_price)
        stop_rel = int(stop_hits[0]) if stop_hits.size else missing
        target_rel = int(target_hits[0]) if target_hits.size else missing
        objective_hit = False
        if stop_rel == target_rel == missing:
            exit_index = end_index
            exit_price = float(path["close"][exit_index])
            gross = _gross_from_price(order.side, entry_price, exit_price)
            reason = "TIME" if end_index < last else "EOD"
        elif stop_rel <= target_rel:
            exit_index = first_exit_index + stop_rel
            gross = _gross_from_price(order.side, entry_price, stop_price)
            reason = "STOP"
        else:
            exit_index = first_exit_index + target_rel
            gross = _gross_from_price(order.side, entry_price, target_price)
            reason = "TARGET"
            objective_hit = True
        elapsed = max(0, exit_index - entry_index)
        row.update(
            {
                "filled": True,
                "net_return_pct": (gross - cost) * 100.0,
                "objective_hit": objective_hit,
                "exit_reason": reason,
                "entry_index": entry_index,
                "first_exit_index": first_exit_index,
                "entry_price": entry_price,
                "exit_index": exit_index,
                "holding_bars": elapsed + 1,
                "elapsed_minutes": elapsed,
                "capital_minutes": elapsed,
                "effective_stop_pct": stop,
                "effective_target_pct": target,
                "levels_anchor_trigger": levels_anchor_trigger,
                "engine": label,
            }
        )
        rows.append(row)
    return pd.DataFrame(rows)


@dataclass(frozen=True)
class Scaleout:
    initial_stop_pct: float = 1.5
    t1_pct: float = 1.05
    partial_pct: float = 0.20
    runner_target_pct: float = 2.60
    runner_stop: str = "BREAKEVEN"
    runner_lock_pct: float = 0.0
    max_hold_bars: int | None = None


def simulate_scaleout(
    orders: pd.DataFrame,
    paths: dict[int, dict[str, np.ndarray]],
    config: Scaleout,
    *,
    cost_bps: float = BASE_COST_BPS,
    delay_bars: int = 0,
    adverse_fill_bps: float = 0.0,
    levels_anchor_trigger: bool = False,
    label: str = "SCALEOUT",
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    cost = cost_bps / 10000.0
    for order in orders.itertuples(index=False):
        row = order._asdict()
        path = paths[int(order.sid)]
        entry_data = _entry(path, order.side, float(order.trigger), delay_bars, adverse_fill_bps)
        if entry_data is None:
            row.update({"filled": False, "net_return_pct": np.nan, "objective_hit": False, "exit_reason": "UNFILLED", "entry_index": np.nan, "t1_index": np.nan, "exit_index": np.nan, "holding_bars": np.nan, "elapsed_minutes": np.nan, "capital_minutes": np.nan})
            rows.append(row)
            continue
        entry_index, first_exit_index, entry_price = entry_data
        level_anchor = float(order.trigger) if levels_anchor_trigger else entry_price
        last = len(path["close"]) - 1
        end_index = last if config.max_hold_bars is None else min(last, entry_index + int(config.max_hold_bars))
        high = path["high"][first_exit_index : end_index + 1]
        low = path["low"][first_exit_index : end_index + 1]
        missing = len(high) + 1
        if order.side == "LONG":
            initial_stop = level_anchor * (1 - config.initial_stop_pct / 100.0)
            t1_price = level_anchor * (1 + config.t1_pct / 100.0)
            runner_target = level_anchor * (1 + config.runner_target_pct / 100.0)
            stop_hits = np.flatnonzero(low <= initial_stop)
            t1_hits = np.flatnonzero(high >= t1_price)
        else:
            initial_stop = level_anchor * (1 + config.initial_stop_pct / 100.0)
            t1_price = level_anchor * (1 - config.t1_pct / 100.0)
            runner_target = level_anchor * (1 - config.runner_target_pct / 100.0)
            stop_hits = np.flatnonzero(high >= initial_stop)
            t1_hits = np.flatnonzero(low <= t1_price)
        stop_rel = int(stop_hits[0]) if stop_hits.size else missing
        t1_rel = int(t1_hits[0]) if t1_hits.size else missing
        objective_hit = False
        t1_abs: int | float = np.nan
        if stop_rel == t1_rel == missing:
            exit_index = end_index
            gross = _gross_from_price(order.side, entry_price, float(path["close"][end_index]))
            reason = "TIME_NO_T1" if end_index < last else "EOD_NO_T1"
            capital_minutes = max(0, exit_index - entry_index)
        elif stop_rel <= t1_rel:
            exit_index = first_exit_index + stop_rel
            gross = _gross_from_price(order.side, entry_price, initial_stop)
            reason = "FULL_STOP"
            capital_minutes = max(0, exit_index - entry_index)
        else:
            objective_hit = True
            t1_abs = first_exit_index + t1_rel
            booked_gross = _gross_from_price(order.side, entry_price, t1_price)
            if config.runner_stop == "BREAKEVEN":
                lock = 0.0
            elif config.runner_stop == "ORIGINAL":
                lock = -config.initial_stop_pct
            elif config.runner_stop == "LOCK":
                lock = config.runner_lock_pct
            else:
                raise ValueError(config.runner_stop)
            if order.side == "LONG":
                runner_stop_price = initial_stop if config.runner_stop == "ORIGINAL" else entry_price * (1 + lock / 100.0)
                runner_stops = np.flatnonzero(path["low"][int(t1_abs) : end_index + 1] <= runner_stop_price)
                runner_targets = np.flatnonzero(path["high"][int(t1_abs) : end_index + 1] >= runner_target)
            else:
                runner_stop_price = initial_stop if config.runner_stop == "ORIGINAL" else entry_price * (1 - lock / 100.0)
                runner_stops = np.flatnonzero(path["high"][int(t1_abs) : end_index + 1] >= runner_stop_price)
                runner_targets = np.flatnonzero(path["low"][int(t1_abs) : end_index + 1] <= runner_target)
            runner_missing = end_index - int(t1_abs) + 2
            runner_stop_rel = int(runner_stops[0]) if runner_stops.size else runner_missing
            runner_target_rel = int(runner_targets[0]) if runner_targets.size else runner_missing
            if runner_stop_rel == runner_target_rel == runner_missing:
                exit_index = end_index
                runner_gross = _gross_from_price(order.side, entry_price, float(path["close"][end_index]))
                reason = "T1_THEN_TIME" if end_index < last else "T1_THEN_EOD"
            elif runner_stop_rel <= runner_target_rel:
                exit_index = int(t1_abs) + runner_stop_rel
                runner_gross = _gross_from_price(order.side, entry_price, runner_stop_price)
                reason = "T1_THEN_BREAKEVEN" if config.runner_stop == "BREAKEVEN" else ("T1_THEN_STOP" if config.runner_stop == "ORIGINAL" else "T1_THEN_LOCK")
            else:
                exit_index = int(t1_abs) + runner_target_rel
                runner_gross = _gross_from_price(order.side, entry_price, runner_target)
                reason = "RUNNER_TARGET"
            gross = config.partial_pct * booked_gross + (1.0 - config.partial_pct) * runner_gross
            pre_t1 = max(0, int(t1_abs) - entry_index)
            post_t1 = max(0, exit_index - int(t1_abs))
            capital_minutes = pre_t1 + (1.0 - config.partial_pct) * post_t1
        elapsed = max(0, exit_index - entry_index)
        row.update(
            {
                "filled": True,
                "net_return_pct": (gross - cost) * 100.0,
                "objective_hit": objective_hit,
                "exit_reason": reason,
                "entry_index": entry_index,
                "first_exit_index": first_exit_index,
                "entry_price": entry_price,
                "t1_index": t1_abs,
                "exit_index": exit_index,
                "holding_bars": elapsed + 1,
                "elapsed_minutes": elapsed,
                "capital_minutes": capital_minutes,
                "engine": label,
                "levels_anchor_trigger": levels_anchor_trigger,
                **asdict(config),
            }
        )
        rows.append(row)
    return pd.DataFrame(rows)


def metric(audit: pd.DataFrame, days: list[date]) -> dict[str, float | int]:
    frame = audit.loc[audit["day"].isin(days) & audit["filled"].astype(bool)].copy()
    values = frame["net_return_pct"].to_numpy(float)
    gains = float(values[values > 0].sum())
    losses = float(-values[values < 0].sum())
    daily = frame.groupby("day")["net_return_pct"].sum().reindex(days, fill_value=0.0)
    curve = np.r_[0.0, daily.to_numpy(float).cumsum()]
    dd = curve - np.maximum.accumulate(curve)
    result: dict[str, float | int] = {
        "orders": int(audit.loc[audit["day"].isin(days)].shape[0]),
        "fills": int(values.size),
        "wins": int((values > 0).sum()),
        "win_rate_pct": float((values > 0).mean() * 100.0) if values.size else np.nan,
        "objective_hits": int(frame["objective_hit"].sum()) if values.size else 0,
        "objective_hit_rate_pct": float(frame["objective_hit"].mean() * 100.0) if values.size else np.nan,
        "pf": gains / losses if losses else (np.inf if gains else 0.0),
        "net_pct": float(values.sum()),
        "expectancy_pct": float(values.mean()) if values.size else np.nan,
        "max_drawdown_pct": float(dd.min()) if dd.size else 0.0,
        "median_hold_min": float(frame["elapsed_minutes"].median()) if values.size else np.nan,
        "p90_hold_min": float(frame["elapsed_minutes"].quantile(0.90)) if values.size else np.nan,
        "mean_capital_minutes": float(frame["capital_minutes"].mean()) if values.size else np.nan,
    }
    return result


def ledger_row(name: str, family: str, audit: pd.DataFrame, days: list[date], params: dict[str, Any]) -> dict[str, Any]:
    row: dict[str, Any] = {"config": name, "family": family, "params_json": json.dumps(py(params), sort_keys=True)}
    for period_name, period_days in periods(days).items():
        for key, value in metric(audit, period_days).items():
            row[f"{period_name}_{key}"] = value
    return row


def validate_baselines(orders: pd.DataFrame, paths: dict[int, dict[str, np.ndarray]], days: list[date], input_audit: dict[str, Any]) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    native = simulate_fixed(orders, paths, label="V3_NATIVE")
    published_native = pd.read_csv(V4_NATIVE_PATH)
    published_native["sid"] = published_native["sid"].astype(int)
    published_native = published_native.set_index("sid")
    ours = native.set_index("sid")
    return_delta = float(np.nanmax(np.abs(ours["net_return_pct"].sort_index().to_numpy(float) - published_native.loc[ours.sort_index().index, "net_return_pct"].to_numpy(float))))
    fill_equal = bool((ours["filled"].sort_index().to_numpy(bool) == published_native.loc[ours.sort_index().index, "filled"].astype(bool).to_numpy()).all())

    v4_config = Scaleout()
    v4 = simulate_scaleout(orders, paths, v4_config, label="V4_SCALEOUT")
    published_v4 = pd.read_csv(V4_TRADES_PATH)
    published_v4["sid"] = published_v4["sid"].astype(int)
    published_v4 = published_v4.set_index("sid")
    ours_v4 = v4.set_index("sid").sort_index()
    published_v4 = published_v4.loc[ours_v4.index]
    v4_return_delta = float(np.nanmax(np.abs(ours_v4["net_return_pct"].to_numpy(float) - published_v4["net_return_pct"].to_numpy(float))))
    v4_fill_equal = bool((ours_v4["filled"].to_numpy(bool) == published_v4["filled"].astype(bool).to_numpy()).all())
    v4_t1_equal = bool((ours_v4["objective_hit"].to_numpy(bool) == published_v4["t1_hit"].astype(bool).to_numpy()).all())
    parity = {
        **input_audit,
        "native_return_max_abs_delta": return_delta,
        "native_fill_equal": fill_equal,
        "v4_return_max_abs_delta": v4_return_delta,
        "v4_fill_equal": v4_fill_equal,
        "v4_t1_equal": v4_t1_equal,
        "native_all_metrics": metric(native, days),
        "v4_all_metrics": metric(v4, days),
        "passed": return_delta <= 1e-10 and fill_equal and v4_return_delta <= 1e-10 and v4_fill_equal and v4_t1_equal,
    }
    if not parity["passed"]:
        raise RuntimeError(f"Baseline parity failed: {parity}")
    return native, v4, parity


def excursion_audit(orders: pd.DataFrame, paths: dict[int, dict[str, np.ndarray]], native: pd.DataFrame, v4: pd.DataFrame) -> pd.DataFrame:
    native_idx = native.set_index("sid")
    v4_idx = v4.set_index("sid")
    rows: list[dict[str, Any]] = []
    for order in orders.itertuples(index=False):
        sid = int(order.sid)
        nrow = native_idx.loc[sid]
        vrow = v4_idx.loc[sid]
        if not bool(nrow["filled"]):
            rows.append({"sid": sid, "day": order.day, "setup_id": order.setup_id, "side": order.side, "tradingsymbol": order.tradingsymbol, "filled": False})
            continue
        path = paths[sid]
        entry = int(nrow["entry_index"])
        trigger = float(order.trigger)
        if order.side == "LONG":
            favorable = (path["high"][entry:] / trigger - 1.0) * 100.0
            adverse = (1.0 - path["low"][entry:] / trigger) * 100.0
        else:
            favorable = (1.0 - path["low"][entry:] / trigger) * 100.0
            adverse = (path["high"][entry:] / trigger - 1.0) * 100.0
        native_end = int(nrow["exit_index"])
        v4_end = int(vrow["exit_index"])
        n_span = native_end - entry + 1
        v_span = v4_end - entry + 1
        item = {
                "sid": sid,
                "day": order.day,
                "setup_id": order.setup_id,
                "side": order.side,
                "tradingsymbol": order.tradingsymbol,
                "filled": True,
                "entry_index": entry,
                "full_session_mfe_pct": float(np.nanmax(favorable)),
                "full_session_mae_pct": float(np.nanmax(adverse)),
                "bars_to_full_mfe": int(np.nanargmax(favorable)),
                "bars_to_full_mae": int(np.nanargmax(adverse)),
                "native_pre_exit_mfe_pct": float(np.nanmax(favorable[:n_span])),
                "native_pre_exit_mae_pct": float(np.nanmax(adverse[:n_span])),
                "v4_pre_exit_mfe_pct": float(np.nanmax(favorable[:v_span])),
                "v4_pre_exit_mae_pct": float(np.nanmax(adverse[:v_span])),
                "native_return_pct": float(nrow["net_return_pct"]),
                "native_exit_reason": nrow["exit_reason"],
                "native_elapsed_min": float(nrow["elapsed_minutes"]),
                "v4_return_pct": float(vrow["net_return_pct"]),
                "v4_exit_reason": vrow["exit_reason"],
                "v4_t1_hit": bool(vrow["objective_hit"]),
                "v4_elapsed_min": float(vrow["elapsed_minutes"]),
                "v4_capital_minutes": float(vrow["capital_minutes"]),
                "period": "TRAIN" if order.day < SPLIT_DAY else ("TEST" if order.day <= TEST_END else "LATEST"),
            }
        for threshold in [0.50, 0.75, 0.95, 1.00, 1.05, 1.075, 1.10, 1.25, 1.50, 2.00, 2.50, 2.60, 3.00]:
            hits = np.flatnonzero(favorable >= threshold)
            slug = str(threshold).replace(".", "p")
            item[f"first_mfe_{slug}_bar"] = int(hits[0]) if hits.size else np.nan
        rows.append(item)
    return pd.DataFrame(rows)


def path_end_audit(orders: pd.DataFrame, paths: dict[int, dict[str, np.ndarray]], native: pd.DataFrame, v4: pd.DataFrame) -> pd.DataFrame:
    """Infer path end from confirmation + contiguous one-minute array length.

    The separate raw-store audit established that there are no internal minute gaps
    in these selected paths, so this inference is exact for their terminal minute.
    """
    native_idx = native.set_index("sid")
    v4_idx = v4.set_index("sid")
    rows: list[dict[str, Any]] = []
    for order in orders.itertuples(index=False):
        confirmation = pd.Timestamp(order.confirmation_ts)
        inferred_end = confirmation + pd.Timedelta(minutes=len(paths[int(order.sid)]["close"]))
        configured_end = confirmation.normalize() + pd.Timedelta(hours=15, minutes=30)
        nrow = native_idx.loc[int(order.sid)]
        vrow = v4_idx.loc[int(order.sid)]
        rows.append(
            {
                "sid": int(order.sid),
                "day": order.day,
                "setup_id": order.setup_id,
                "side": order.side,
                "confirmation_ts": confirmation,
                "path_bars": len(paths[int(order.sid)]["close"]),
                "inferred_path_end": inferred_end,
                "configured_squareoff": configured_end,
                "ends_before_1530": bool(inferred_end < configured_end),
                "minutes_short": float((configured_end - inferred_end).total_seconds() / 60.0),
                "native_exit_reason": nrow["exit_reason"],
                "native_squareoff_on_short_path": bool(nrow["exit_reason"] == "EOD" and inferred_end < configured_end),
                "v4_exit_reason": vrow["exit_reason"],
                "v4_eod_on_short_path": bool("EOD" in str(vrow["exit_reason"]) and inferred_end < configured_end),
            }
        )
    return pd.DataFrame(rows)


def excursion_summaries(excursions: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    filled = excursions.loc[excursions["filled"]].copy()
    summaries: list[dict[str, Any]] = []
    group_specs: list[tuple[str, Iterable[tuple[Any, pd.DataFrame]]]] = [
        ("ALL", [("ALL", filled)]),
        ("PERIOD", filled.groupby("period")),
        ("SIDE", filled.groupby("side")),
        ("SETUP", filled.groupby("setup_id")),
        ("V3_OUTCOME", filled.assign(outcome=np.where(filled["native_return_pct"] > 0, "WIN", "LOSS")).groupby("outcome")),
        ("V4_OUTCOME", filled.assign(outcome=np.where(filled["v4_return_pct"] > 0, "WIN", "LOSS")).groupby("outcome")),
    ]
    cols = ["full_session_mfe_pct", "full_session_mae_pct", "native_pre_exit_mfe_pct", "native_pre_exit_mae_pct", "v4_pre_exit_mfe_pct", "v4_pre_exit_mae_pct", "native_elapsed_min", "v4_elapsed_min", "v4_capital_minutes"]
    for dimension, groups in group_specs:
        for value, group in groups:
            item: dict[str, Any] = {"dimension": dimension, "value": value, "n": len(group)}
            for col in cols:
                item[f"{col}_mean"] = float(group[col].mean())
                item[f"{col}_median"] = float(group[col].median())
                item[f"{col}_q75"] = float(group[col].quantile(0.75))
                item[f"{col}_q90"] = float(group[col].quantile(0.90))
            summaries.append(item)

    thresholds: list[dict[str, Any]] = []
    mfe_levels = [0.50, 0.75, 0.95, 1.00, 1.05, 1.075, 1.10, 1.25, 1.50, 2.00, 2.50, 2.60, 3.00]
    for dimension, groups in group_specs[:4]:
        for value, group in groups:
            for threshold in mfe_levels:
                threshold_rows = group.loc[group["full_session_mfe_pct"] >= threshold]
                slug = str(threshold).replace(".", "p")
                first_hit_col = f"first_mfe_{slug}_bar"
                thresholds.append(
                    {
                        "dimension": dimension,
                        "value": value,
                        "mfe_threshold_pct": threshold,
                        "hits": int(len(threshold_rows)),
                        "hit_rate_pct": float(len(threshold_rows) / len(group) * 100.0) if len(group) else np.nan,
                        "median_mae_if_hit_pct": float(threshold_rows["full_session_mae_pct"].median()) if len(threshold_rows) else np.nan,
                        "median_bars_to_threshold_if_hit": float(threshold_rows[first_hit_col].median()) if len(threshold_rows) else np.nan,
                        "p75_bars_to_threshold_if_hit": float(threshold_rows[first_hit_col].quantile(0.75)) if len(threshold_rows) else np.nan,
                    }
                )
    return pd.DataFrame(summaries), pd.DataFrame(thresholds)


def combine_audits(name: str, orders: pd.DataFrame, choices: list[tuple[pd.Series, pd.DataFrame]]) -> pd.DataFrame:
    base = choices[0][1].copy().set_index("sid")
    base.loc[:, "engine"] = name
    for mask, audit in choices[1:]:
        sids = orders.loc[mask, "sid"].astype(int).tolist()
        replacement = audit.set_index("sid")
        common = [sid for sid in sids if sid in base.index and sid in replacement.index]
        base.loc[common, :] = replacement.loc[common, base.columns]
    return base.reset_index()


def run_experiments(orders: pd.DataFrame, paths: dict[int, dict[str, np.ndarray]], days: list[date], native: pd.DataFrame, v4: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, pd.DataFrame]]:
    ledger: list[dict[str, Any]] = []
    audits: dict[str, pd.DataFrame] = {"V3_NATIVE": native, "V4": v4}
    ledger.append(ledger_row("V3_NATIVE", "BASELINE", native, days, {"row_native_stops_targets": True}))
    ledger.append(ledger_row("V4", "BASELINE", v4, days, asdict(Scaleout())))

    fixed_pairs = [(0.75, 3.0), (1.0, 2.5), (1.0, 3.0), (1.25, 2.5), (1.25, 3.0), (1.5, 1.0), (1.5, 2.5), (1.5, 3.0), (1.75, 2.5), (2.0, 3.0)]
    for stop, target in fixed_pairs:
        name = f"FIXED_S{stop:.3f}_T{target:.3f}"
        audit = simulate_fixed(orders, paths, stop_pct=stop, target_pct=target, label=name)
        audits[name] = audit
        ledger.append(ledger_row(name, "FIXED", audit, days, {"stop_pct": stop, "target_pct": target}))

    stops = [1.25, 1.50, 1.75]
    t1s = [0.95, 1.00, 1.05, 1.075, 1.10, 1.15]
    partials = [0.10, 0.20, 0.30]
    runners = [2.40, 2.60, 2.80]
    runner_stops = [("BREAKEVEN", 0.0), ("LOCK", 0.20), ("ORIGINAL", 0.0)]
    for stop in stops:
        for t1 in t1s:
            for partial in partials:
                for runner in runners:
                    for runner_stop, lock in runner_stops:
                        cfg = Scaleout(stop, t1, partial, runner, runner_stop, lock)
                        name = f"SO_S{stop:.3f}_T1{t1:.3f}_P{partial:.2f}_R{runner:.2f}_{runner_stop}{lock:.2f}"
                        audit = simulate_scaleout(orders, paths, cfg, label=name)
                        ledger.append(ledger_row(name, "SCALEOUT_NEIGHBORHOOD", audit, days, asdict(cfg)))
                        # Retain only operationally important exact candidates in memory.
                        if (stop, t1, partial, runner, runner_stop, lock) in {
                            (1.5, 1.05, 0.10, 2.6, "BREAKEVEN", 0.0),
                            (1.5, 1.075, 0.10, 2.6, "BREAKEVEN", 0.0),
                            (1.5, 1.05, 0.20, 2.6, "BREAKEVEN", 0.0),
                            (1.25, 1.05, 0.20, 2.6, "BREAKEVEN", 0.0),
                            (1.75, 1.05, 0.20, 2.6, "BREAKEVEN", 0.0),
                        }:
                            audits[name] = audit

    # Pre-declared maximum-hold tests; same entries and thresholds, only runner/EOD exposure changes.
    for base_name, base_cfg in [("V4", Scaleout()), ("P10", Scaleout(partial_pct=0.10, t1_pct=1.075))]:
        for cap in [30, 60, 90, 120, 180]:
            cfg = Scaleout(**{**asdict(base_cfg), "max_hold_bars": cap})
            name = f"{base_name}_CAP_{cap}M"
            audit = simulate_scaleout(orders, paths, cfg, label=name)
            audits[name] = audit
            ledger.append(ledger_row(name, "TIME_CAP", audit, days, asdict(cfg)))

    # Terminal-safe neighborhood: every cap exits well before the observed 15:15
    # truncation even for the latest 10:01 confirmation.
    for cap in [120, 150, 180, 210, 240]:
        for t1 in [0.95, 1.00, 1.05, 1.075, 1.10]:
            for partial in [0.10, 0.20, 0.30]:
                cfg = Scaleout(initial_stop_pct=1.50, t1_pct=t1, partial_pct=partial, runner_target_pct=2.60, runner_stop="BREAKEVEN", max_hold_bars=cap)
                name = f"SAFE_CAP{cap}_T1{t1:.3f}_P{partial:.2f}_R2.60_BE"
                audit = simulate_scaleout(orders, paths, cfg, label=name)
                ledger.append(ledger_row(name, "TERMINAL_SAFE_CAP_NEIGHBORHOOD", audit, days, asdict(cfg)))

    p10 = audits["SO_S1.500_T11.075_P0.10_R2.60_BREAKEVEN0.00"]
    tighter = simulate_scaleout(orders, paths, Scaleout(initial_stop_pct=1.25), label="S125_V4")
    audits["S125_V4"] = tighter
    # Side/time rules are few and specified before seeing their results.
    rules: list[tuple[str, pd.DataFrame]] = []
    rules.append(("SIDE_LONG_V4_SHORT_P10", combine_audits("SIDE_LONG_V4_SHORT_P10", orders, [(pd.Series(True, index=orders.index), v4), (orders["side"].eq("SHORT"), p10)])))
    rules.append(("SIDE_LONG_P10_SHORT_V4", combine_audits("SIDE_LONG_P10_SHORT_V4", orders, [(pd.Series(True, index=orders.index), v4), (orders["side"].eq("LONG"), p10)])))
    rules.append(("SIDE_LONG_V4_SHORT_S125", combine_audits("SIDE_LONG_V4_SHORT_S125", orders, [(pd.Series(True, index=orders.index), v4), (orders["side"].eq("SHORT"), tighter)])))
    rules.append(("EARLY_V4_LATE_P10", combine_audits("EARLY_V4_LATE_P10", orders, [(pd.Series(True, index=orders.index), v4), (orders["hhmm_int"].ge(940), p10)])))
    rules.append(("EARLY_P10_LATE_V4", combine_audits("EARLY_P10_LATE_V4", orders, [(pd.Series(True, index=orders.index), p10), (orders["hhmm_int"].ge(940), v4)])))
    rules.append(("V4_EXCEPT_0941S_P10", combine_audits("V4_EXCEPT_0941S_P10", orders, [(pd.Series(True, index=orders.index), v4), (orders["setup_id"].eq("0941_SHORT"), p10)])))
    for name, audit in rules:
        audits[name] = audit
        ledger.append(ledger_row(name, "SIDE_TIME_RULE", audit, days, {"predeclared_rule": name}))
    return pd.DataFrame(ledger), audits


def select_configs(ledger: pd.DataFrame, audits: dict[str, pd.DataFrame], orders: pd.DataFrame, paths: dict[int, dict[str, np.ndarray]]) -> dict[str, str]:
    neighborhood = ledger.loc[ledger["family"].eq("SCALEOUT_NEIGHBORHOOD")].copy()
    guardrail = neighborhood.loc[
        neighborhood["all_objective_hit_rate_pct"].ge(50.0)
        & neighborhood["all_pf"].ge(2.0)
        & neighborhood["train_pf"].ge(1.75)
        & neighborhood["test_pf"].ge(1.75)
        & neighborhood["all_max_drawdown_pct"].ge(-4.0)
        & neighborhood["all_fills"].ge(75)
    ].copy()
    guardrail["min_train_test_pf"] = guardrail[["train_pf", "test_pf"]].min(axis=1)
    meaningful = guardrail.loc[guardrail["params_json"].str.contains('"partial_pct": 0.2') | guardrail["params_json"].str.contains('"partial_pct": 0.3')]
    balanced_row = meaningful.sort_values(["min_train_test_pf", "all_pf", "all_net_pct"], ascending=False).iloc[0]
    numeric_row = guardrail.sort_values(["min_train_test_pf", "all_pf", "all_net_pct"], ascending=False).iloc[0]

    def ensure(name: str) -> None:
        if name in audits:
            return
        row = ledger.loc[ledger["config"].eq(name)].iloc[0]
        params = json.loads(row["params_json"])
        cfg = Scaleout(**params)
        audits[name] = simulate_scaleout(orders, paths, cfg, label=name)

    balanced = str(balanced_row["config"])
    numeric = str(numeric_row["config"])
    ensure(balanced)
    ensure(numeric)

    # Capacity choice is explicitly from time-cap family: highest train/test floor among <=90m caps.
    caps = ledger.loc[ledger["family"].eq("TIME_CAP") & ledger["config"].str.contains(r"CAP_(?:30|60|90)M", regex=True)].copy()
    caps["min_train_test_pf"] = caps[["train_pf", "test_pf"]].min(axis=1)
    caps = caps.loc[caps["all_pf"].ge(1.5) & caps["all_net_pct"].gt(0)]
    capacity = str(caps.sort_values(["min_train_test_pf", "all_pf"], ascending=False).iloc[0]["config"])
    safe = ledger.loc[ledger["family"].eq("TERMINAL_SAFE_CAP_NEIGHBORHOOD")].copy()
    safe["decoded_partial"] = safe["params_json"].apply(lambda x: float(json.loads(x)["partial_pct"]))
    safe["min_train_test_pf"] = safe[["train_pf", "test_pf"]].min(axis=1)
    safe["development_objective_hit_rate_pct"] = (safe["train_objective_hits"] + safe["test_objective_hits"]) / (safe["train_fills"] + safe["test_fills"]) * 100.0
    safe["development_net_pct"] = safe["train_net_pct"] + safe["test_net_pct"]
    safe = safe.loc[
        safe["decoded_partial"].ge(0.20)
        & safe["development_objective_hit_rate_pct"].gt(50.0)
        & safe["train_pf"].ge(1.75)
        & safe["test_pf"].ge(1.75)
        & safe["development_net_pct"].gt(0)
    ]
    terminal_safe = str(safe.sort_values(["min_train_test_pf", "development_net_pct"], ascending=False).iloc[0]["config"])
    ensure(terminal_safe)
    return {"balanced": balanced, "numeric_best": numeric, "conservative": "V4", "terminal_safe": terminal_safe, "capacity": capacity, "baseline": "V3_NATIVE"}


def cost_execution_stress(selected: dict[str, str], audits: dict[str, pd.DataFrame], orders: pd.DataFrame, paths: dict[int, dict[str, np.ndarray]], days: list[date], ledger: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    configs: dict[str, tuple[str, Any]] = {"V3_NATIVE": ("native", None), "V4": ("scaleout", Scaleout())}
    for role in ["balanced", "numeric_best", "terminal_safe", "capacity"]:
        name = selected[role]
        if name in configs:
            continue
        row = ledger.loc[ledger["config"].eq(name)].iloc[0]
        configs[name] = ("scaleout", Scaleout(**json.loads(row["params_json"])))

    cost_rows: list[dict[str, Any]] = []
    execution_rows: list[dict[str, Any]] = []
    for name, (kind, config) in configs.items():
        for cost in [5.0, 10.0, 15.0, 20.0, 30.0]:
            audit = simulate_fixed(orders, paths, cost_bps=cost, label=name) if kind == "native" else simulate_scaleout(orders, paths, config, cost_bps=cost, label=name)
            row = {"config": name, "cost_bps": cost}
            for period_name, period_days in periods(days).items():
                for key, value in metric(audit, period_days).items():
                    row[f"{period_name}_{key}"] = value
            cost_rows.append(row)
        for delay in [0, 1, 2]:
            for adverse in [0.0, 2.0, 5.0, 10.0]:
                if delay == 0 and adverse == 0.0:
                    audit = audits[name]
                else:
                    audit = simulate_fixed(orders, paths, delay_bars=delay, adverse_fill_bps=adverse, levels_anchor_trigger=True, label=name) if kind == "native" else simulate_scaleout(orders, paths, config, delay_bars=delay, adverse_fill_bps=adverse, levels_anchor_trigger=True, label=name)
                row = {"config": name, "delay_bars": delay, "adverse_fill_bps": adverse, "fill_model": "exit levels remain anchored to original trigger; delay enters at delayed close but never improves beyond original trigger+adverse bps; delayed-close exits start next bar"}
                for period_name, period_days in periods(days).items():
                    for key, value in metric(audit, period_days).items():
                        row[f"{period_name}_{key}"] = value
                execution_rows.append(row)
    return pd.DataFrame(cost_rows), pd.DataFrame(execution_rows)


def removal_stress(selected: dict[str, str], audits: dict[str, pd.DataFrame], days: list[date]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for role, name in selected.items():
        if name not in audits or role == "baseline" and name != "V3_NATIVE":
            continue
        audit = audits[name].copy()
        filled = audit.loc[audit["filled"]].copy()
        for n in [0, 1, 2, 3, 5]:
            drop_sids = filled.nlargest(n, "net_return_pct")["sid"].tolist() if n else []
            stressed = audit.loc[~audit["sid"].isin(drop_sids)].copy()
            item = {"role": role, "config": name, "stress": f"REMOVE_TOP_{n}_TRADES", "removed": n, "removed_net_pct": float(filled.loc[filled["sid"].isin(drop_sids), "net_return_pct"].sum())}
            item.update({f"all_{k}": v for k, v in metric(stressed, days).items()})
            rows.append(item)
        daily = filled.groupby("day")["net_return_pct"].sum().sort_values(ascending=False)
        for n in [1, 2, 3]:
            drop_days = daily.head(n).index.tolist()
            stressed = audit.loc[~audit["day"].isin(drop_days)].copy()
            stressed_days = [d for d in days if d not in drop_days]
            item = {"role": role, "config": name, "stress": f"REMOVE_TOP_{n}_DAYS", "removed": n, "removed_net_pct": float(daily.head(n).sum()), "removed_ids": ",".join(map(str, drop_days))}
            item.update({f"all_{k}": v for k, v in metric(stressed, stressed_days).items()})
            rows.append(item)
    return pd.DataFrame(rows)


def bootstrap_one(audit: pd.DataFrame, days: list[date], draws: int, rng: np.random.Generator) -> dict[str, float]:
    filled = audit.loc[audit["filled"] & audit["day"].isin(days)].copy()
    grouped = filled.groupby("day")["net_return_pct"]
    day_net = grouped.sum().reindex(days, fill_value=0.0).to_numpy(float)
    day_gain = grouped.apply(lambda s: s[s > 0].sum()).reindex(days, fill_value=0.0).to_numpy(float)
    day_loss = grouped.apply(lambda s: -s[s < 0].sum()).reindex(days, fill_value=0.0).to_numpy(float)
    day_fills = filled.groupby("day").size().reindex(days, fill_value=0).to_numpy(float)
    day_wins = filled.assign(_win=filled["net_return_pct"].gt(0).astype(int)).groupby("day")["_win"].sum().reindex(days, fill_value=0).to_numpy(float)
    day_objectives = filled.assign(_hit=filled["objective_hit"].astype(int)).groupby("day")["_hit"].sum().reindex(days, fill_value=0).to_numpy(float)
    if not len(days):
        return {}
    indices = rng.integers(0, len(days), size=(draws, len(days)))
    nets = day_net[indices].sum(axis=1)
    gains = day_gain[indices].sum(axis=1)
    losses = day_loss[indices].sum(axis=1)
    sampled_fills = day_fills[indices].sum(axis=1)
    sampled_wins = day_wins[indices].sum(axis=1)
    sampled_objectives = day_objectives[indices].sum(axis=1)
    win_rates = np.divide(sampled_wins, sampled_fills, out=np.full_like(sampled_wins, np.nan), where=sampled_fills > 0) * 100.0
    objective_rates = np.divide(sampled_objectives, sampled_fills, out=np.full_like(sampled_objectives, np.nan), where=sampled_fills > 0) * 100.0
    pfs = np.divide(gains, losses, out=np.full_like(gains, np.inf), where=losses > 0)
    curves = np.cumsum(day_net[indices], axis=1)
    curves = np.concatenate([np.zeros((draws, 1)), curves], axis=1)
    drawdowns = curves - np.maximum.accumulate(curves, axis=1)
    max_dd = drawdowns.min(axis=1)
    finite_pf = pfs[np.isfinite(pfs)]
    return {
        "draws": draws,
        "net_q025": float(np.quantile(nets, 0.025)),
        "net_q05": float(np.quantile(nets, 0.05)),
        "net_median": float(np.quantile(nets, 0.50)),
        "net_q95": float(np.quantile(nets, 0.95)),
        "net_q975": float(np.quantile(nets, 0.975)),
        "prob_net_positive_pct": float((nets > 0).mean() * 100.0),
        "pf_q025": float(np.quantile(finite_pf, 0.025)) if finite_pf.size else np.inf,
        "pf_q05": float(np.quantile(finite_pf, 0.05)) if finite_pf.size else np.inf,
        "pf_median": float(np.quantile(finite_pf, 0.50)) if finite_pf.size else np.inf,
        "prob_pf_gt_1_pct": float((pfs > 1).mean() * 100.0),
        "win_rate_q025_pct": float(np.nanquantile(win_rates, 0.025)),
        "win_rate_median_pct": float(np.nanquantile(win_rates, 0.50)),
        "objective_hit_rate_q025_pct": float(np.nanquantile(objective_rates, 0.025)),
        "objective_hit_rate_q05_pct": float(np.nanquantile(objective_rates, 0.05)),
        "objective_hit_rate_median_pct": float(np.nanquantile(objective_rates, 0.50)),
        "prob_objective_hit_rate_gt_50_pct": float(np.nanmean(objective_rates > 50.0) * 100.0),
        "max_dd_q05": float(np.quantile(max_dd, 0.05)),
        "max_dd_median": float(np.quantile(max_dd, 0.50)),
    }


def monte_carlo_order(audit: pd.DataFrame, draws: int, rng: np.random.Generator) -> dict[str, float]:
    returns = audit.loc[audit["filled"], "net_return_pct"].to_numpy(float)
    max_dd = np.empty(draws)
    for i in range(draws):
        sequence = rng.permutation(returns)
        curve = np.r_[0.0, sequence.cumsum()]
        max_dd[i] = (curve - np.maximum.accumulate(curve)).min()
    return {
        "draws": draws,
        "trade_order_max_dd_q01": float(np.quantile(max_dd, 0.01)),
        "trade_order_max_dd_q05": float(np.quantile(max_dd, 0.05)),
        "trade_order_max_dd_median": float(np.quantile(max_dd, 0.50)),
        "trade_order_max_dd_q95": float(np.quantile(max_dd, 0.95)),
    }


def stochastic_stress(selected: dict[str, str], audits: dict[str, pd.DataFrame], days: list[date]) -> pd.DataFrame:
    rng = np.random.default_rng(RNG_SEED)
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    for role, name in selected.items():
        if name in seen or name not in audits:
            continue
        seen.add(name)
        for period_name, period_days in periods(days).items():
            if period_name == "latest":
                continue
            result = bootstrap_one(audits[name], period_days, 20000, rng)
            rows.append({"config": name, "role": role, "stress": "DAY_CLUSTER_BOOTSTRAP", "period": period_name, "seed": RNG_SEED, **result})
        rows.append({"config": name, "role": role, "stress": "TRADE_ORDER_MONTE_CARLO", "period": "all", "seed": RNG_SEED, **monte_carlo_order(audits[name], 20000, rng)})
    return pd.DataFrame(rows)


def neighborhood_summary(ledger: pd.DataFrame, selected: dict[str, str]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    grid = ledger.loc[ledger["family"].eq("SCALEOUT_NEIGHBORHOOD")].copy()
    decoded = grid["params_json"].apply(json.loads)
    for role in ["balanced", "numeric_best", "conservative", "terminal_safe"]:
        name = selected[role]
        if role == "terminal_safe":
            cap_grid = ledger.loc[ledger["family"].eq("TERMINAL_SAFE_CAP_NEIGHBORHOOD")].copy()
            cap_decoded = cap_grid["params_json"].apply(json.loads)
            center = json.loads(ledger.loc[ledger["config"].eq(name), "params_json"].iloc[0])
            mask = np.ones(len(cap_grid), dtype=bool)
            cap_tolerances = {"max_hold_bars": 30.0, "t1_pct": 0.05, "partial_pct": 0.10}
            for key, tolerance in cap_tolerances.items():
                vals = cap_decoded.apply(lambda d: float(d[key])).to_numpy(float)
                mask &= np.abs(vals - float(center[key])) <= tolerance + 1e-12
            hood = cap_grid.loc[mask].copy()
            definition = "terminal-safe cap +/-30m, T1 +/-0.05, partial +/-0.10; fixed 1.50 stop, 2.60 runner, breakeven"
            item: dict[str, Any] = {"role": role, "config": name, "neighbors": len(hood), "definition": definition}
            for col in ["all_pf", "all_net_pct", "all_win_rate_pct", "all_objective_hit_rate_pct", "all_max_drawdown_pct", "train_pf", "test_pf", "train_net_pct", "test_net_pct"]:
                item[f"{col}_min"] = float(hood[col].min()) if len(hood) else np.nan
                item[f"{col}_median"] = float(hood[col].median()) if len(hood) else np.nan
                item[f"{col}_max"] = float(hood[col].max()) if len(hood) else np.nan
            rows.append(item)
            continue
        if name == "V4":
            center = asdict(Scaleout())
        else:
            center = json.loads(ledger.loc[ledger["config"].eq(name), "params_json"].iloc[0])
        # One-grid-step Chebyshev neighborhood around continuous scaleout parameters, same stop mode.
        mask = np.ones(len(grid), dtype=bool)
        tolerances = {"initial_stop_pct": 0.25, "t1_pct": 0.05, "partial_pct": 0.10, "runner_target_pct": 0.20}
        for key, tolerance in tolerances.items():
            vals = decoded.apply(lambda d: float(d[key])).to_numpy(float)
            mask &= np.abs(vals - float(center[key])) <= tolerance + 1e-12
        modes = decoded.apply(lambda d: d["runner_stop"]).to_numpy(str)
        mask &= modes == str(center["runner_stop"])
        hood = grid.loc[mask].copy()
        item = {"role": role, "config": name, "neighbors": len(hood), "definition": "same runner-stop mode; +/-0.25 stop, +/-0.05 T1, +/-0.10 partial, +/-0.20 runner target within tested grid"}
        for col in ["all_pf", "all_net_pct", "all_win_rate_pct", "all_objective_hit_rate_pct", "all_max_drawdown_pct", "train_pf", "test_pf", "train_net_pct", "test_net_pct"]:
            item[f"{col}_min"] = float(hood[col].min()) if len(hood) else np.nan
            item[f"{col}_median"] = float(hood[col].median()) if len(hood) else np.nan
            item[f"{col}_max"] = float(hood[col].max()) if len(hood) else np.nan
        rows.append(item)
    return pd.DataFrame(rows)


def setup_comparison(native: pd.DataFrame, v4: pd.DataFrame, selected: dict[str, str], audits: dict[str, pd.DataFrame], days: list[date]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    names = ["V3_NATIVE", "V4", selected["balanced"], selected["numeric_best"], selected["terminal_safe"], selected["capacity"]]
    for name in dict.fromkeys(names):
        audit = audits[name]
        for dimension in ["side", "setup_id"]:
            for value, group in audit.groupby(dimension):
                subset_days = sorted(set(group["day"]))
                item = {"config": name, "dimension": dimension, "value": value}
                item.update(metric(group, subset_days))
                rows.append(item)
    return pd.DataFrame(rows)


def render_report(
    parity: dict[str, Any],
    ledger: pd.DataFrame,
    selected: dict[str, str],
    excursions: pd.DataFrame,
    cost: pd.DataFrame,
    execution: pd.DataFrame,
    removal: pd.DataFrame,
    bootstrap: pd.DataFrame,
    neighborhood: pd.DataFrame,
) -> str:
    configs = ["V3_NATIVE", "V4", selected["balanced"], selected["numeric_best"], selected["terminal_safe"], selected["capacity"]]
    summary = ledger.loc[ledger["config"].isin(configs)].drop_duplicates("config").set_index("config")

    def f(value: Any, n: int = 3) -> str:
        if pd.isna(value):
            return "NA"
        if np.isinf(float(value)):
            return "inf"
        return f"{float(value):.{n}f}"

    lines = [
        "# V13-v5 Frozen-Entry Exit and Robustness Audit",
        "",
        "## Scope and parity",
        "",
        f"- Read-only replay of {parity['selected_orders']} frozen V13-v3 selected orders across {len(parity['sessions'])} sessions; no entry selection was changed.",
        "- Chronological reporting: TRAIN = 12 sessions through 2026-08-13; TEST = 11 sessions 2026-08-14 through 2026-09-01; LATEST = 2 sessions 2026-09-02 through 2026-09-03.",
        f"- Exact parity: V3 max return delta {parity['native_return_max_abs_delta']:.3g}; V4 max return delta {parity['v4_return_max_abs_delta']:.3g}; cache/source hashes passed.",
        f"- **Critical terminal-data caveat:** {parity['path_end_inference']['paths_before_1530']}/{parity['selected_orders']} selected paths infer an end before 15:30 (69 end at 15:15). Consequently {parity['path_end_inference']['native_squareoffs_on_short_paths']}/{parity['path_end_inference']['native_squareoffs']} native square-offs and {parity['path_end_inference']['v4_eod_exits_on_short_paths']}/{parity['path_end_inference']['v4_eod_exits']} V4 EOD-dependent exits use a truncated terminal close. Rebuild/replay through true 15:30 data before promotion.",
        "- Path cache contains one-minute high/low/close arrays only. There is no open or timestamp array, so gap-aware fills and exact wall-clock holding cannot be reconstructed; array-index minutes are used.",
        "- A separate raw-minute audit found 3/78 fills where the bar open had already gapped through the trigger; published replay nevertheless fills at the trigger. It found no fill-bar stop/target events or first stop/target same-bar ties in this sample.",
        "- **Cache concurrency defect observed:** V3's cache-hit path still calls `_store_cached` (`fno_v13_corrected_v3_backtest.py:379`), while the NPZ writer is a direct non-atomic `np.savez_compressed` (`fno_v6_corrected_backtest.py:336`). A concurrent probe transiently changed the September NPZ hash from provenance `a2b0c856...` to `bc9a92fc...`; the hash guard stopped this replay until a byte-identical cache was restored. Avoid concurrent V3 loader calls.",
        "- Same-minute stop/target ambiguity follows published pessimistic rules: stop wins ties. V4 also permits same-bar post-T1 breakeven/runner checks, again with runner stop winning ties.",
        "",
        "## Core comparison",
        "",
        "| Config | Role | Fills | Win % | First-objective % | PF | Net % | DD % | Train PF | Test PF | Latest net % | Median hold min | Mean capital-min |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    role_by_name = {value: key for key, value in selected.items()}
    role_by_name["V3_NATIVE"] = "baseline"
    role_by_name["V4"] = "conservative"
    for name in dict.fromkeys(configs):
        r = summary.loc[name]
        lines.append(
            f"| {name} | {role_by_name.get(name, '')} | {int(r['all_fills'])} | {f(r['all_win_rate_pct'])} | {f(r['all_objective_hit_rate_pct'])} | {f(r['all_pf'])} | {f(r['all_net_pct'])} | {f(r['all_max_drawdown_pct'])} | {f(r['train_pf'])} | {f(r['test_pf'])} | {f(r['latest_net_pct'])} | {f(r['all_median_hold_min'], 1)} | {f(r['all_mean_capital_minutes'], 1)} |"
        )

    rejected_examples = {
        "FIXED_S1.500_T1.000": "metric-targeting: net/expectancy collapse",
        "SO_S1.250_T11.075_P0.10_R2.60_ORIGINAL0.00": "lower WR/PF and worse DD",
        "SO_S1.500_T11.075_P0.10_R2.60_LOCK0.20": "extra complexity; trails BE P10",
        "SIDE_LONG_P10_SHORT_V4": "side-specific sparse-cell overfit risk",
        "P10_CAP_180M": "only 50% objective; weaker test net",
    }
    rejected_rows = ledger.loc[ledger["config"].isin(rejected_examples)].drop_duplicates("config").set_index("config")
    lines += [
        "",
        "## Representative rejected / experimental variants",
        "",
        "| Config | Why not preferred | Win % | Objective % | PF | Net % | DD % | Train PF | Test PF | Latest net % |",
        "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for name, reason in rejected_examples.items():
        if name not in rejected_rows.index:
            continue
        r = rejected_rows.loc[name]
        lines.append(f"| {name} | {reason} | {f(r['all_win_rate_pct'])} | {f(r['all_objective_hit_rate_pct'])} | {f(r['all_pf'])} | {f(r['all_net_pct'])} | {f(r['all_max_drawdown_pct'])} | {f(r['train_pf'])} | {f(r['test_pf'])} | {f(r['latest_net_pct'])} |")

    all_exc = excursions.loc[excursions["filled"]]
    lines += [
        "",
        "## Excursion and holding-time findings",
        "",
        f"- Full-session MFE: median {f(all_exc['full_session_mfe_pct'].median())}%, q75 {f(all_exc['full_session_mfe_pct'].quantile(.75))}%, q90 {f(all_exc['full_session_mfe_pct'].quantile(.90))}%.",
        f"- Full-session MAE: median {f(all_exc['full_session_mae_pct'].median())}%, q75 {f(all_exc['full_session_mae_pct'].quantile(.75))}%, q90 {f(all_exc['full_session_mae_pct'].quantile(.90))}%.",
        f"- V3 median elapsed hold {f(all_exc['native_elapsed_min'].median(),1)} index-minutes versus V4 {f(all_exc['v4_elapsed_min'].median(),1)}; V4 mean size-weighted capital minutes {f(all_exc['v4_capital_minutes'].mean(),1)}.",
        "- Full-session excursions after a strategy exit are counterfactual and are diagnostic only; pre-exit excursion columns are separately stored in `mae_mfe_trade_audit.csv`.",
        "",
        "## Stress conclusions",
        "",
    ]
    for name in dict.fromkeys(configs):
        c20 = cost.loc[(cost["config"].eq(name)) & (cost["cost_bps"].eq(20.0))]
        e = execution.loc[(execution["config"].eq(name)) & execution["delay_bars"].eq(1) & execution["adverse_fill_bps"].eq(5.0)]
        rem = removal.loc[(removal["config"].eq(name)) & removal["stress"].eq("REMOVE_TOP_3_TRADES")]
        boot = bootstrap.loc[(bootstrap["config"].eq(name)) & bootstrap["stress"].eq("DAY_CLUSTER_BOOTSTRAP") & bootstrap["period"].eq("all")]
        if len(c20) and len(e) and len(rem) and len(boot):
            lines.append(
                f"- **{name}:** at 20 bps PF {f(c20.iloc[0]['all_pf'])}, net {f(c20.iloc[0]['all_net_pct'])}%; at +1 bar / +5 bps adverse execution PF {f(e.iloc[0]['all_pf'])}, net {f(e.iloc[0]['all_net_pct'])}%, fills {int(e.iloc[0]['all_fills'])}; after removing top 3 trades PF {f(rem.iloc[0]['all_pf'])}, net {f(rem.iloc[0]['all_net_pct'])}%; day-bootstrap 2.5% net bound {f(boot.iloc[0]['net_q025'])}%, P(net>0) {f(boot.iloc[0]['prob_net_positive_pct'],1)}%, and P(first-objective rate>50%) {f(boot.iloc[0]['prob_objective_hit_rate_gt_50_pct'],1)}%."
            )
    lines += [
        "",
        "## Recommendation interpretation",
        "",
        f"- **Balanced:** `{selected['balanced']}` is the best tested operationally meaningful scale-out (at least 20% booked) under all/train/test PF, >=50% first-objective, drawdown and fill guardrails. Treat it as a candidate, not a promotion, because it was selected from a neighborhood on the same 25 sessions.",
        f"- **Conservative:** `V4` keeps the documented 20% at +1.05%, 80% breakeven runner to +2.60% rule. It is simpler and already independently published, so it is more defensible than a numerical neighborhood winner even if the latter scores higher in-sample.",
        f"- **Terminal-data-safe candidate:** `{selected['terminal_safe']}` is the best tested >=20%-partial capped rule with >50% first-objective and train/test PF guardrails. Its cap exits before 15:15 for every active setup, so its path outcomes do not depend on the missing terminal interval; it is still selected on the same small sample.",
        f"- **Capacity/high-frequency relevant:** `{selected['capacity']}` is the strongest <=90-minute cap by the train/test PF-floor screen. It does not increase selected orders in this frozen replay; it only releases capital sooner. A portfolio-level concurrency replay is required before claiming additional trades.",
        f"- **Numerical best:** `{selected['numeric_best']}` is retained for comparison. If its partial is only 10%, its >50% first-objective rate should not be marketed as a full target-hit rate.",
        "- Latest Sep 2-3 contains only two fills. Its negative result is reported but is far too small to validate or reject an exit.",
        "",
        "## Key rejected ideas / cautions",
        "",
        "- Uniform +1.00% target / 1.50% stop raises target-hit rate but materially compresses net and expectancy; it is metric optimization, not the preferred economic exit.",
        "- Original-stop runners and +0.20% locked runners are included in the neighborhood ledger; a single best row should not be trusted unless its train/test floor and local-neighborhood minima are stable.",
        "- Side/setup/time overrides are exploratory on only 1-15 observations per cell. They are recorded but excluded from automatic recommendations due multiple-testing and sparse-cell overfit risk.",
        "- Delay/worse-fill tests are synthetic, not reconstructed exchange fills. The adverse-price floor is conservative, but delaying entry is not mathematically monotone because it changes which subsequent path is exposed. The cache cannot model gaps, spread, partial fills, queue position or broker latency.",
        "- No exit candidate is promotion-ready while the 15:15-versus-15:30 terminal-path defect remains. Relative results may change most for EOD_NO_T1 and T1_THEN_EOD trades.",
        "- Bootstrap resamples whole days to preserve within-day trade clustering, but 25 sessions remains a very small empirical distribution. Monte Carlo reshuffling changes drawdown order, not PF or total return.",
        "",
        "## Artifact map",
        "",
        "- `baseline_parity.json`: immutable input hashes and exact V3/V4 reproduction checks.",
        "- `path_end_audit.csv`: inferred terminal minute and affected V3/V4 EOD exit flags for every selected order.",
        "- `mae_mfe_trade_audit.csv`, `mae_mfe_summary.csv`, `excursion_thresholds.csv`: trade-level and grouped excursion/holding evidence.",
        "- `experiment_ledger.csv`: fixed, scale-out neighborhood, timing/side and time-cap metrics for all/train/test/latest.",
        "- `cost_stress.csv`, `execution_stress.csv`, `best_trade_removal.csv`: deterministic robustness tests.",
        "- `bootstrap_monte_carlo.csv`, `parameter_neighborhood_summary.csv`: stochastic and local-parameter robustness.",
        "- `timing_side_comparison.csv`, `recommendations.json`, `artifact_manifest.json`: sliced results, chosen roles, and hashes.",
    ]
    return "\n".join(lines) + "\n"


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    orders, paths, days, input_audit = load_frozen()
    native, v4, parity = validate_baselines(orders, paths, days, input_audit)
    path_ends = path_end_audit(orders, paths, native, v4)
    native_squareoffs = int(native["exit_reason"].eq("EOD").sum())
    v4_eod_exits = int(v4["exit_reason"].astype(str).str.contains("EOD").sum())
    parity["path_end_inference"] = {
        "basis": "confirmation_ts + contiguous one-minute cached path length; raw-store audit found zero internal gaps before path end",
        "paths_before_1530": int(path_ends["ends_before_1530"].sum()),
        "paths_ending_1515": int(pd.to_datetime(path_ends["inferred_path_end"]).dt.strftime("%H%M").eq("1515").sum()),
        "native_squareoffs": native_squareoffs,
        "native_squareoffs_on_short_paths": int(path_ends["native_squareoff_on_short_path"].sum()),
        "v4_eod_exits": v4_eod_exits,
        "v4_eod_exits_on_short_paths": int(path_ends["v4_eod_on_short_path"].sum()),
        "external_raw_minute_audit": {"gap_through_trigger_fills": 3, "fill_bar_stop_hits": 0, "fill_bar_target_hits": 0, "first_stop_target_same_bar_ties": 0},
    }
    parity["observed_cache_concurrency_incident"] = {
        "status": "RESTORED_AND_REVALIDATED",
        "file": str(Path(r"C:\TradingData\eqidv2\fno_oi\strategy_research\v13_corrected_v3\_cache\26SEP_31bed551aabc991b.npz")),
        "provenance_and_restored_sha256": "a2b0c85647cd39e8ebf67dc4b7b528d2511b6fb3fa65a8b03ec2293348cb0b07",
        "transient_sha256_seen_by_strict_reader": "bc9a92fcb9ba3077c3bc193a02e026ce73ec439f730b4990bdd4b8346a2fabe8",
        "cause": "fno_v13_corrected_v3_backtest._load_or_build_regime unconditionally rewrites its own cache even on a cache hit; fno_v6_corrected_backtest._store_cached writes NPZ directly rather than atomically",
        "source_locations": ["fno_v13_corrected_v3_backtest.py:379", "fno_v6_corrected_backtest.py:328-336"],
        "instruction": "Do not run V3 loader concurrently with readers; change writer to atomic temp+replace and skip writes on valid own-cache hits in a future code fix.",
    }
    write_json(OUT / "baseline_parity.json", parity)
    path_ends.to_csv(OUT / "path_end_audit.csv", index=False)

    excursions = excursion_audit(orders, paths, native, v4)
    excursion_summary, thresholds = excursion_summaries(excursions)
    excursions.to_csv(OUT / "mae_mfe_trade_audit.csv", index=False)
    excursion_summary.to_csv(OUT / "mae_mfe_summary.csv", index=False)
    thresholds.to_csv(OUT / "excursion_thresholds.csv", index=False)

    ledger, audits = run_experiments(orders, paths, days, native, v4)
    selected = select_configs(ledger, audits, orders, paths)
    ledger.to_csv(OUT / "experiment_ledger.csv", index=False)

    cost, execution = cost_execution_stress(selected, audits, orders, paths, days, ledger)
    removal = removal_stress(selected, audits, days)
    stochastic = stochastic_stress(selected, audits, days)
    neighborhood = neighborhood_summary(ledger, selected)
    sliced = setup_comparison(native, v4, selected, audits, days)
    cost.to_csv(OUT / "cost_stress.csv", index=False)
    execution.to_csv(OUT / "execution_stress.csv", index=False)
    removal.to_csv(OUT / "best_trade_removal.csv", index=False)
    stochastic.to_csv(OUT / "bootstrap_monte_carlo.csv", index=False)
    neighborhood.to_csv(OUT / "parameter_neighborhood_summary.csv", index=False)
    sliced.to_csv(OUT / "timing_side_comparison.csv", index=False)

    recommendations: dict[str, Any] = {
        "selected": selected,
        "selection_warning": "Balanced/numeric/capacity roles were screened on these same 25 sessions; V4 remains the independently published conservative benchmark.",
        "promotion_blocker": "69/79 selected paths end at 15:15 rather than configured 15:30; rebuild true terminal paths and rerun all EOD-dependent exits before promotion.",
        "path_end_inference": parity["path_end_inference"],
        "metrics": {},
    }
    for role, name in selected.items():
        recommendations["metrics"][role] = {period_name: metric(audits[name], period_days) for period_name, period_days in periods(days).items()}
        row = ledger.loc[ledger["config"].eq(name)]
        recommendations["metrics"][role]["parameters"] = json.loads(row.iloc[0]["params_json"]) if len(row) else {}
    write_json(OUT / "recommendations.json", recommendations)

    report = render_report(parity, ledger, selected, excursions, cost, execution, removal, stochastic, neighborhood)
    (OUT / "REPORT.md").write_text(report, encoding="utf-8")

    manifest: dict[str, Any] = {
        "generated_by": str(Path(__file__).resolve()),
        "rng_seed": RNG_SEED,
        "input_files": parity,
        "outputs": [],
    }
    for path in sorted(OUT.iterdir()):
        if path.is_file() and path.name != "artifact_manifest.json":
            manifest["outputs"].append({"path": str(path), "bytes": path.stat().st_size, "sha256": sha256(path)})
    write_json(OUT / "artifact_manifest.json", manifest)
    print(json.dumps({"out": str(OUT), "selected": selected, "parity": parity["passed"], "artifacts": len(manifest["outputs"])}, indent=2))


if __name__ == "__main__":
    main()
