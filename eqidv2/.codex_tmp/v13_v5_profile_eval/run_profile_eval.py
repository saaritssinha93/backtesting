from __future__ import annotations

import hashlib
import importlib.util
import json
import sys
from dataclasses import asdict, dataclass, replace
from datetime import date
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import pandas as pd

WORKSPACE = Path(__file__).resolve().parents[2]
if str(WORKSPACE) not in sys.path:
    sys.path.insert(0, str(WORKSPACE))

import fno_oi_hybrid_data as hybrid
import fno_v13_corrected_v2_backtest as v2
import fno_v13_corrected_v3_backtest as v3
import fno_v5_hybrid_backtest as selector


OUT = Path(__file__).resolve().parent
V3_DIR = Path(r"C:\TradingData\eqidv2\fno_oi\strategy_research\v13_corrected_v3")
PROVENANCE_PATH = V3_DIR / "fno_v13_corrected_v3_provenance.json"
V3_TRADES_PATH = V3_DIR / "fno_v13_corrected_v3_trades.csv"
CORRECTED_REPLAY_PATH = (
    WORKSPACE / ".codex_tmp" / "v13_v5_validation_audit" / "corrected_execution_replay.py"
)

CUTOFF = "1515"
MAX_FORWARD_BARS = 400
COSTS_BPS = (5.0, 10.0, 20.0, 30.0)
T1_GRID = (1.05, 1.075, 1.10, 1.125)
PARTIAL_GRID = (0.10, 0.20)
RUNNER_GRID = (2.50, 2.60, 2.70)
INITIAL_STOP_PCT = 1.50
RUNNER_STOP = "BREAKEVEN"
BASE_POLICY = v2.POLICIES[v3.BASE_POLICY_NAME]


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def clean(value: Any) -> Any:
    if isinstance(value, np.integer):
        return int(value)
    if isinstance(value, np.bool_):
        return bool(value)
    if isinstance(value, (float, np.floating)):
        if np.isnan(value):
            return None
        if np.isposinf(value):
            return "INF"
        if np.isneginf(value):
            return "-INF"
        return float(value)
    if isinstance(value, (date, pd.Timestamp, Path)):
        return str(value)
    if isinstance(value, dict):
        return {str(key): clean(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [clean(item) for item in value]
    return value


def write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(clean(payload), indent=2, sort_keys=True), encoding="utf-8")


def load_frozen_cache() -> tuple[pd.DataFrame, dict[int, dict[str, np.ndarray]], dict[str, Any]]:
    provenance = json.loads(PROVENANCE_PATH.read_text(encoding="utf-8"))
    suffix = {"h": "high", "l": "low", "c": "close"}
    parts: list[tuple[pd.DataFrame, dict[int, dict[str, np.ndarray]]]] = []
    checks: list[dict[str, Any]] = []
    for record in provenance["cache_records"]:
        parquet = Path(record["cache_parquet"])
        npz = Path(record["cache_npz"])
        observed_parquet = sha256(parquet)
        observed_npz = sha256(npz)
        if observed_parquet != record["cache_parquet_sha256"]:
            raise RuntimeError(f"Frozen parquet hash mismatch: {parquet}")
        if observed_npz != record["cache_npz_sha256"]:
            raise RuntimeError(f"Frozen path hash mismatch: {npz}")
        signals = pd.read_parquet(parquet)
        signals["day"] = pd.to_datetime(signals["day"]).dt.date
        blob = np.load(npz)
        paths: dict[int, dict[str, np.ndarray]] = {}
        for key in blob.files:
            sid_text, code = key.rsplit("_", 1)
            paths.setdefault(int(sid_text), {})[suffix[code]] = blob[key].astype(float)
        parts.append((signals, paths))
        checks.append(
            {
                "contract_month": record["contract_month"],
                "parquet": str(parquet.resolve()),
                "parquet_sha256": observed_parquet,
                "npz": str(npz.resolve()),
                "npz_sha256": observed_npz,
                "rows": len(signals),
                "paths": len(paths),
            }
        )

    frames: list[pd.DataFrame] = []
    merged: dict[int, dict[str, np.ndarray]] = {}
    offset = 0
    for signals, paths in parts:
        block = signals.copy()
        block["sid"] = block["sid"].astype(int) + offset
        frames.append(block)
        for sid, path in paths.items():
            merged[int(sid) + offset] = path
        offset += max(paths) + 1 if paths else 0
    signals = (
        pd.concat(frames, ignore_index=True)
        .sort_values(["day", "sid"], kind="stable")
        .reset_index(drop=True)
    )
    return signals, merged, {"provenance": provenance, "cache_checks": checks}


def assert_cache_unchanged(cache_checks: list[dict[str, Any]]) -> None:
    for record in cache_checks:
        if sha256(Path(record["parquet"])) != record["parquet_sha256"]:
            raise RuntimeError(f"Cache changed during run: {record['parquet']}")
        if sha256(Path(record["npz"])) != record["npz_sha256"]:
            raise RuntimeError(f"Cache changed during run: {record['npz']}")


def modal_short(signal_end: str) -> Any:
    return replace(
        v2._modal_long_setup(signal_end),
        side="SHORT",
        source_version="V13_V5_PROFILE_CONTROLLED_MODAL_SHORT",
    )


@dataclass(frozen=True)
class Profile:
    name: str
    add_1120_short: bool
    wick_plus_010: bool
    add_0950_short: bool
    role: str


PROFILES = (
    Profile("P0_V3", False, False, False, "BASELINE_ABLATION"),
    Profile("A_V3_1120S", True, False, False, "REQUESTED_A"),
    Profile("W_V3_WICK010", False, True, False, "SINGLE_COMPONENT_ABLATION"),
    Profile("H_V3_0950S", False, False, True, "SINGLE_COMPONENT_ABLATION"),
    Profile("B_V3_1120S_WICK010", True, True, False, "REQUESTED_B"),
    Profile("AH_V3_1120S_0950S", True, False, True, "PAIR_ABLATION"),
    Profile("WH_V3_WICK010_0950S", False, True, True, "PAIR_ABLATION"),
    Profile("C_V3_1120S_WICK010_0950S", True, True, True, "REQUESTED_C"),
)


def profile_setups(profile: Profile) -> tuple[Any, ...]:
    setups = list(v3.active_setups())
    if profile.add_1120_short:
        setups.append(modal_short("11:20"))
    if profile.add_0950_short:
        setups.append(modal_short("09:50"))
    if profile.wick_plus_010:
        setups = [
            replace(setup, max_wick_ratio=min(1.0, float(setup.max_wick_ratio) + 0.10))
            for setup in setups
        ]
    keys = [(setup.signal_end, setup.side) for setup in setups]
    if len(keys) != len(set(keys)):
        raise RuntimeError(f"Duplicate setup keys in {profile.name}")
    return tuple(setups)


def select_profiles(signals: pd.DataFrame) -> tuple[dict[str, pd.DataFrame], pd.DataFrame]:
    context = v3.load_nifty_first_bar_context(signals["contract_month"].unique())
    annotated = v3.annotate_nifty_gate(signals, context)
    gated = annotated.loc[annotated["nifty_first_bar_gate_pass"]].copy()
    source = v2.apply_policy(gated, BASE_POLICY)
    selected: dict[str, pd.DataFrame] = {}
    setup_registry: list[dict[str, Any]] = []
    for profile in PROFILES:
        chunks: list[pd.DataFrame] = []
        for setup in profile_setups(profile):
            rows = selector.select_setup_rows(source, setup).reset_index(drop=True)
            if rows.empty:
                continue
            rows["setup_id"] = setup.setup_id
            rows["confirmation_end"] = setup.confirmation_end
            rows["picker"] = setup.picker
            rows["max_entries"] = setup.max_entries
            rows["native_stop_pct"] = setup.stop_pct
            rows["native_target_pct"] = setup.target_pct
            chunks.append(rows)
            setup_registry.append({"profile": profile.name, **asdict(setup)})
        frame = pd.concat(chunks, ignore_index=True, sort=False)
        frame["profile"] = profile.name
        frame["profile_role"] = profile.role
        frame["component_1120_short"] = profile.add_1120_short
        frame["component_wick_plus_010"] = profile.wick_plus_010
        frame["component_0950_short"] = profile.add_0950_short
        frame = frame.sort_values(
            ["day", "hhmm_int", "side", "setup_id", "tradingsymbol"], kind="stable"
        ).reset_index(drop=True)
        selected[profile.name] = frame
    return selected, pd.DataFrame(setup_registry).drop_duplicates().reset_index(drop=True)


def as_ist(value: Any) -> pd.Timestamp:
    stamp = pd.Timestamp(value)
    if stamp.tzinfo is None:
        return stamp.tz_localize("Asia/Kolkata")
    return stamp.tz_convert("Asia/Kolkata")


def build_raw_paths(
    unique_orders: pd.DataFrame,
    frozen_paths: dict[int, dict[str, np.ndarray]],
) -> tuple[dict[int, dict[str, np.ndarray]], pd.DataFrame, list[dict[str, Any]]]:
    raw_paths: dict[int, dict[str, np.ndarray]] = {}
    audits: list[dict[str, Any]] = []
    raw_files: list[dict[str, Any]] = []
    for symbol, group in unique_orders.groupby("tradingsymbol", sort=True):
        resolved = hybrid.resolve_backtest_equity_symbol(str(symbol))
        raw_file = hybrid.equity_one_minute_path(
            resolved, hybrid.DEFAULT_BACKTEST_EQUITY_1M_DIR
        )
        raw_files.append(
            {
                "symbol": str(symbol),
                "resolved_symbol": resolved,
                "path": str(raw_file.resolve()),
                "sha256": sha256(raw_file),
                "bytes": raw_file.stat().st_size,
            }
        )
        minute = hybrid.load_equity_one_minute(resolved)
        minute = (
            minute.sort_values("ts", kind="stable")
            .drop_duplicates("ts", keep="last")
            .reset_index(drop=True)
        )
        minute_ns = minute["ts"].astype("int64").to_numpy()
        for order in group.itertuples(index=False):
            confirmation = as_ist(order.confirmation_ts)
            idx = int(np.searchsorted(minute_ns, confirmation.value))
            if idx >= len(minute_ns) or minute_ns[idx] != confirmation.value:
                raise RuntimeError(
                    f"Missing exact confirmation raw minute: {symbol} {confirmation}"
                )
            end_idx = min(idx + 1 + MAX_FORWARD_BARS, len(minute))
            path = minute.iloc[idx + 1 : end_idx].copy()
            path = path.loc[
                path["ts"].dt.date.eq(confirmation.date())
                & path["ts"].dt.strftime("%H%M").le(CUTOFF)
            ].reset_index(drop=True)
            if path.empty:
                raise RuntimeError(f"Empty 15:15 path: {symbol} {confirmation}")
            sid = int(order.sid)
            raw_paths[sid] = {
                "open": path["open"].to_numpy(float),
                "high": path["high"].to_numpy(float),
                "low": path["low"].to_numpy(float),
                "close": path["close"].to_numpy(float),
                "ts_ns": path["ts"].astype("int64").to_numpy(),
            }
            cached = frozen_paths[sid]
            count = min(len(path), len(cached["close"]))
            max_hlc_delta = max(
                float(np.max(np.abs(raw_paths[sid][field][:count] - cached[field][:count])))
                for field in ("high", "low", "close")
            )
            if max_hlc_delta > 1e-9:
                raise RuntimeError(f"Raw/cache HLC mismatch sid={sid}: {max_hlc_delta}")
            audits.append(
                {
                    "sid": sid,
                    "day": order.day,
                    "tradingsymbol": symbol,
                    "confirmation_ts": str(confirmation),
                    "first_entry_check_ts": str(path.iloc[0]["ts"]),
                    "last_path_ts": str(path.iloc[-1]["ts"]),
                    "path_bars_1515": len(path),
                    "cached_path_bars": len(cached["close"]),
                    "max_raw_cache_hlc_delta": max_hlc_delta,
                }
            )
    return raw_paths, pd.DataFrame(audits), raw_files


def gross_from_price(side: str, entry: float, exit_price: float) -> float:
    return exit_price / entry - 1.0 if side == "LONG" else 1.0 - exit_price / entry


def first_entry(path: dict[str, np.ndarray], side: str, trigger: float) -> tuple[int, float, bool] | None:
    touches = (
        np.flatnonzero(path["high"] >= trigger)
        if side == "LONG"
        else np.flatnonzero(path["low"] <= trigger)
    )
    if not touches.size:
        return None
    index = int(touches[0])
    bar_open = float(path["open"][index])
    gap = bool(bar_open > trigger if side == "LONG" else bar_open < trigger)
    return index, (bar_open if gap else trigger), gap


def hit_indices(
    path: dict[str, np.ndarray], side: str, start: int, stop: float, target: float
) -> tuple[int, int]:
    if side == "LONG":
        stop_hits = np.flatnonzero(path["low"][start:] <= stop)
        target_hits = np.flatnonzero(path["high"][start:] >= target)
    else:
        stop_hits = np.flatnonzero(path["high"][start:] >= stop)
        target_hits = np.flatnonzero(path["low"][start:] <= target)
    missing = np.iinfo(np.int32).max
    return (
        int(stop_hits[0]) if stop_hits.size else int(missing),
        int(target_hits[0]) if target_hits.size else int(missing),
    )


def simulate_native(
    orders: pd.DataFrame, paths: dict[int, dict[str, np.ndarray]], cost_bps: float
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for order in orders.itertuples(index=False):
        result = order._asdict()
        path = paths[int(order.sid)]
        entry_data = first_entry(path, order.side, float(order.trigger))
        if entry_data is None:
            result.update(
                filled=False,
                gross_return_pct=np.nan,
                net_return_pct=np.nan,
                exit_reason="UNFILLED",
                objective_hit=False,
                entry_gap=False,
                entry_index=np.nan,
                exit_index=np.nan,
            )
            rows.append(result)
            continue
        entry_index, entry, gap = entry_data
        stop_pct = float(order.native_stop_pct)
        target_pct = float(order.native_target_pct)
        if order.side == "LONG":
            stop = entry * (1.0 - stop_pct / 100.0)
            target = entry * (1.0 + target_pct / 100.0)
        else:
            stop = entry * (1.0 + stop_pct / 100.0)
            target = entry * (1.0 - target_pct / 100.0)
        stop_rel, target_rel = hit_indices(path, order.side, entry_index, stop, target)
        never = np.iinfo(np.int32).max
        if stop_rel == target_rel == never:
            exit_index = len(path["close"]) - 1
            exit_price = float(path["close"][exit_index])
            reason = "SQUAREOFF_1515"
            objective = False
        elif stop_rel <= target_rel:
            exit_index = entry_index + stop_rel
            exit_price = stop
            reason = "STOP"
            objective = False
        else:
            exit_index = entry_index + target_rel
            exit_price = target
            reason = "TARGET"
            objective = True
        gross_pct = gross_from_price(order.side, entry, exit_price) * 100.0
        result.update(
            filled=True,
            gross_return_pct=gross_pct,
            net_return_pct=gross_pct - cost_bps / 100.0,
            exit_reason=reason,
            objective_hit=objective,
            entry_gap=gap,
            entry_price=entry,
            entry_index=entry_index,
            exit_index=exit_index,
            entry_ts=pd.Timestamp(path["ts_ns"][entry_index], tz="UTC").tz_convert("Asia/Kolkata"),
            exit_ts=pd.Timestamp(path["ts_ns"][exit_index], tz="UTC").tz_convert("Asia/Kolkata"),
            holding_bars=exit_index - entry_index + 1,
        )
        rows.append(result)
    return pd.DataFrame(rows)


@dataclass(frozen=True)
class Scaleout:
    initial_stop_pct: float
    t1_pct: float
    partial_pct: float
    runner_target_pct: float
    runner_stop: str = RUNNER_STOP

    @property
    def config_id(self) -> str:
        return (
            f"S{self.initial_stop_pct:.3f}_T1{self.t1_pct:.3f}_"
            f"P{self.partial_pct:.2f}_R{self.runner_target_pct:.2f}_BE"
        )


def simulate_scaleout_gross(
    orders: pd.DataFrame, paths: dict[int, dict[str, np.ndarray]], config: Scaleout
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for order in orders.itertuples(index=False):
        result = order._asdict()
        path = paths[int(order.sid)]
        entry_data = first_entry(path, order.side, float(order.trigger))
        if entry_data is None:
            result.update(
                filled=False,
                gross_return_pct=np.nan,
                net_return_pct=np.nan,
                exit_reason="UNFILLED",
                objective_hit=False,
                entry_gap=False,
                entry_index=np.nan,
                t1_index=np.nan,
                exit_index=np.nan,
                config_id=config.config_id,
            )
            rows.append(result)
            continue
        entry_index, entry, gap = entry_data
        if order.side == "LONG":
            initial_stop = entry * (1.0 - config.initial_stop_pct / 100.0)
            t1_price = entry * (1.0 + config.t1_pct / 100.0)
            runner_target = entry * (1.0 + config.runner_target_pct / 100.0)
        else:
            initial_stop = entry * (1.0 + config.initial_stop_pct / 100.0)
            t1_price = entry * (1.0 - config.t1_pct / 100.0)
            runner_target = entry * (1.0 - config.runner_target_pct / 100.0)
        stop_rel, t1_rel = hit_indices(path, order.side, entry_index, initial_stop, t1_price)
        never = np.iinfo(np.int32).max
        t1_index: int | float = np.nan
        if stop_rel == t1_rel == never:
            exit_index = len(path["close"]) - 1
            gross = gross_from_price(order.side, entry, float(path["close"][exit_index]))
            reason = "NO_T1_SQUAREOFF_1515"
            objective = False
        elif stop_rel <= t1_rel:
            exit_index = entry_index + stop_rel
            gross = gross_from_price(order.side, entry, initial_stop)
            reason = "FULL_STOP"
            objective = False
        else:
            objective = True
            t1_index = entry_index + t1_rel
            booked = gross_from_price(order.side, entry, t1_price)
            runner_stop = entry
            runner_stop_rel, runner_target_rel = hit_indices(
                path, order.side, int(t1_index), runner_stop, runner_target
            )
            if runner_stop_rel == runner_target_rel == never:
                exit_index = len(path["close"]) - 1
                runner_gross = gross_from_price(
                    order.side, entry, float(path["close"][exit_index])
                )
                reason = "T1_THEN_SQUAREOFF_1515"
            elif runner_stop_rel <= runner_target_rel:
                exit_index = int(t1_index) + runner_stop_rel
                runner_gross = 0.0
                reason = "T1_THEN_BREAKEVEN"
            else:
                exit_index = int(t1_index) + runner_target_rel
                runner_gross = gross_from_price(order.side, entry, runner_target)
                reason = "RUNNER_TARGET"
            gross = config.partial_pct * booked + (1.0 - config.partial_pct) * runner_gross
        gross_pct = gross * 100.0
        result.update(
            filled=True,
            gross_return_pct=gross_pct,
            net_return_pct=gross_pct,
            exit_reason=reason,
            objective_hit=objective,
            entry_gap=gap,
            entry_price=entry,
            entry_index=entry_index,
            t1_index=t1_index,
            exit_index=exit_index,
            entry_ts=pd.Timestamp(path["ts_ns"][entry_index], tz="UTC").tz_convert("Asia/Kolkata"),
            exit_ts=pd.Timestamp(path["ts_ns"][exit_index], tz="UTC").tz_convert("Asia/Kolkata"),
            holding_bars=exit_index - entry_index + 1,
            config_id=config.config_id,
            **asdict(config),
        )
        rows.append(result)
    return pd.DataFrame(rows)


def with_cost(gross_audit: pd.DataFrame, cost_bps: float) -> pd.DataFrame:
    audit = gross_audit.copy()
    audit["cost_bps"] = float(cost_bps)
    audit["net_return_pct"] = np.where(
        audit["filled"].astype(bool),
        audit["gross_return_pct"] - float(cost_bps) / 100.0,
        np.nan,
    )
    return audit


def profit_factor(values: np.ndarray) -> float:
    values = values[np.isfinite(values)]
    profit = float(values[values > 0].sum())
    loss = float(-values[values < 0].sum())
    if loss > 0:
        return profit / loss
    return float("inf") if profit > 0 else np.nan


def metrics(audit: pd.DataFrame, days: list[date]) -> dict[str, Any]:
    scope = audit.loc[audit["day"].isin(set(days))].copy()
    fills = scope.loc[scope["filled"].astype(bool)].copy()
    values = fills["net_return_pct"].to_numpy(float)
    daily = (
        fills.groupby("day")["net_return_pct"]
        .sum()
        .reindex(days, fill_value=0.0)
        .to_numpy(float)
    )
    curve = np.r_[0.0, np.cumsum(daily)]
    drawdown = curve - np.maximum.accumulate(curve)
    return {
        "sessions": len(days),
        "orders": len(scope),
        "fills": len(fills),
        "wins": int((values > 0).sum()),
        "losses": int((values < 0).sum()),
        "win_rate_pct": float((values > 0).mean() * 100.0) if values.size else np.nan,
        "pf": profit_factor(values),
        "net_pct": float(values.sum()) if values.size else 0.0,
        "expectancy_pct": float(values.mean()) if values.size else np.nan,
        "max_drawdown_pct": float(drawdown.min()),
        "t1_hits": int(fills["objective_hit"].sum()) if values.size else 0,
        "t1_hit_rate_pct": float(fills["objective_hit"].mean() * 100.0)
        if values.size
        else np.nan,
        "entry_gap_fills": int(fills["entry_gap"].sum()) if values.size else 0,
        "median_holding_bars": float(fills["holding_bars"].median())
        if values.size
        else np.nan,
    }


def flatten_metrics(audit: pd.DataFrame, periods: dict[str, list[date]]) -> dict[str, Any]:
    row: dict[str, Any] = {}
    for period, days in periods.items():
        for key, value in metrics(audit, days).items():
            row[f"{period}_{key}"] = value
    return row


def headline_nonworse(row: pd.Series, baseline: pd.Series, prefix: str) -> bool:
    return all(
        pd.notna(row[f"{prefix}_{metric}"])
        and float(row[f"{prefix}_{metric}"]) >= float(baseline[f"{prefix}_{metric}"]) - 1e-12
        for metric in ("fills", "pf", "win_rate_pct", "net_pct")
    )


def pareto_front(frame: pd.DataFrame, columns: list[str]) -> pd.Series:
    flags = pd.Series(False, index=frame.index)
    for index, row in frame.iterrows():
        values = np.array(
            [1e12 if np.isposinf(row[column]) else float(row[column]) for column in columns]
        )
        dominated = False
        for other_index, other in frame.iterrows():
            if index == other_index:
                continue
            other_values = np.array(
                [1e12 if np.isposinf(other[column]) else float(other[column]) for column in columns]
            )
            if np.all(other_values >= values - 1e-12) and np.any(
                other_values > values + 1e-12
            ):
                dominated = True
                break
        flags.loc[index] = not dominated
    return flags


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    signals, frozen_paths, input_audit = load_frozen_cache()
    all_days = sorted(signals["day"].unique().tolist())
    if len(all_days) != 25:
        raise RuntimeError(f"Expected 25 sessions, got {len(all_days)}")
    periods = {
        "train": all_days[:12],
        "validation": all_days[12:19],
        "development": all_days[:19],
        "pseudo_test": all_days[19:],
        "all": all_days,
    }
    profile_orders, setup_registry = select_profiles(signals)

    official = pd.read_csv(V3_TRADES_PATH)
    official["day"] = pd.to_datetime(official["day"]).dt.date
    official["sid"] = official["sid"].astype(int)
    official_keys = set(
        map(
            tuple,
            official[["day", "sid", "tradingsymbol", "setup_id", "side"]].to_numpy(),
        )
    )
    p0_keys = set(
        map(
            tuple,
            profile_orders["P0_V3"][
                ["day", "sid", "tradingsymbol", "setup_id", "side"]
            ].to_numpy(),
        )
    )
    if official_keys != p0_keys:
        raise RuntimeError("Selected P0 V3 keys do not match official V3 orders")

    unique_orders = (
        pd.concat(profile_orders.values(), ignore_index=True, sort=False)
        .sort_values(["sid", "profile"], kind="stable")
        .drop_duplicates("sid", keep="first")
        .reset_index(drop=True)
    )
    raw_paths, path_audit, raw_files = build_raw_paths(unique_orders, frozen_paths)
    assert_cache_unchanged(input_audit["cache_checks"])

    p0_native_gross = simulate_native(profile_orders["P0_V3"], raw_paths, cost_bps=0.0)
    p0_native_5 = with_cost(p0_native_gross, 5.0)

    # Independent parity against validation_audit's corrected engine.
    module_spec = importlib.util.spec_from_file_location("corrected_execution_replay", CORRECTED_REPLAY_PATH)
    if module_spec is None or module_spec.loader is None:
        raise RuntimeError("Unable to load corrected execution replay")
    corrected_module = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(corrected_module)
    independent = corrected_module.replay(CUTOFF, gap_open_fill=True)
    independent = independent.merge(
        official.reset_index(names="source_index")[["source_index", "sid"]],
        on="source_index",
        how="left",
        validate="one_to_one",
    )
    parity = p0_native_5[["sid", "filled", "net_return_pct"]].merge(
        independent[["sid", "filled", "net_return_pct"]],
        on="sid",
        suffixes=("_profile_engine", "_independent"),
        validate="one_to_one",
    )
    parity_result = {
        "orders": len(parity),
        "fill_mismatches": int(
            (
                parity["filled_profile_engine"].astype(bool)
                != parity["filled_independent"].astype(bool)
            ).sum()
        ),
        "max_abs_return_delta": float(
            np.nanmax(
                np.abs(
                    parity["net_return_pct_profile_engine"]
                    - parity["net_return_pct_independent"]
                )
            )
        ),
    }
    if parity_result["fill_mismatches"] or parity_result["max_abs_return_delta"] > 1e-9:
        raise RuntimeError(f"Corrected baseline parity failed: {parity_result}")

    baseline_rows: list[dict[str, Any]] = []
    baseline_by_cost: dict[float, pd.DataFrame] = {}
    for cost in COSTS_BPS:
        audit = with_cost(p0_native_gross, cost)
        baseline_by_cost[cost] = audit
        baseline_rows.append(
            {
                "strategy": "CORRECTED_V3_NATIVE_1515_GAP",
                "cost_bps": cost,
                **flatten_metrics(audit, periods),
            }
        )
    baseline_metrics = pd.DataFrame(baseline_rows)
    baseline_metrics.to_csv(OUT / "corrected_v3_baseline_metrics.csv", index=False)

    configs = [
        Scaleout(INITIAL_STOP_PCT, t1, partial, runner)
        for t1 in T1_GRID
        for partial in PARTIAL_GRID
        for runner in RUNNER_GRID
    ]
    ledger: list[dict[str, Any]] = []
    five_bps_audits: list[pd.DataFrame] = []
    gross_audits: dict[tuple[str, str], pd.DataFrame] = {}
    for profile in PROFILES:
        orders = profile_orders[profile.name]
        for config in configs:
            gross = simulate_scaleout_gross(orders, raw_paths, config)
            gross_audits[(profile.name, config.config_id)] = gross
            for cost in COSTS_BPS:
                audit = with_cost(gross, cost)
                ledger.append(
                    {
                        "profile": profile.name,
                        "profile_role": profile.role,
                        "component_1120_short": profile.add_1120_short,
                        "component_wick_plus_010": profile.wick_plus_010,
                        "component_0950_short": profile.add_0950_short,
                        "config_id": config.config_id,
                        **asdict(config),
                        "cost_bps": cost,
                        **flatten_metrics(audit, periods),
                    }
                )
                if cost == 5.0:
                    copy = audit.copy()
                    copy["profile"] = profile.name
                    copy["config_id"] = config.config_id
                    five_bps_audits.append(copy)
    grid = pd.DataFrame(ledger)
    base5 = baseline_metrics.loc[baseline_metrics["cost_bps"].eq(5.0)].iloc[0]
    grid5 = grid.loc[grid["cost_bps"].eq(5.0)].copy()
    grid5["train_validation_guardrail"] = grid5.apply(
        lambda row: (
            row["train_fills"] >= 2
            and row["validation_fills"] >= 2
            and row["train_pf"] > 1.0
            and row["validation_pf"] > 1.0
            and row["train_net_pct"] > 0.0
            and row["validation_net_pct"] > 0.0
            and row["development_t1_hit_rate_pct"] >= 50.0
        ),
        axis=1,
    )
    grid5["development_beats_corrected_v3_all_four"] = grid5.apply(
        lambda row: headline_nonworse(row, base5, "development")
        and any(
            row[f"development_{metric}"] > base5[f"development_{metric}"] + 1e-12
            for metric in ("fills", "pf", "win_rate_pct", "net_pct")
        ),
        axis=1,
    )
    eligible = grid5.loc[grid5["train_validation_guardrail"]].copy()
    eligible["development_pareto"] = pareto_front(
        eligible,
        [
            "development_fills",
            "development_pf",
            "development_win_rate_pct",
            "development_net_pct",
            "development_max_drawdown_pct",
        ],
    )
    grid5["development_pareto"] = False
    grid5.loc[eligible.index, "development_pareto"] = eligible["development_pareto"]

    # Freeze one champion per profile using development only.
    champions: list[pd.Series] = []
    profile_decisions: list[dict[str, Any]] = []
    for profile in PROFILES:
        subset = grid5.loc[grid5["profile"].eq(profile.name)].copy()
        candidates = subset.loc[
            subset["train_validation_guardrail"]
            & subset["development_beats_corrected_v3_all_four"]
        ].sort_values(
            ["development_net_pct", "development_pf", "development_win_rate_pct"],
            ascending=False,
            kind="stable",
        )
        if candidates.empty:
            best = subset.sort_values(
                ["development_net_pct", "development_pf"], ascending=False, kind="stable"
            ).iloc[0]
            profile_decisions.append(
                {
                    "profile": profile.name,
                    "profile_role": profile.role,
                    "selected_before_pseudo_test": False,
                    "rejection_reason": "NO_CONFIG_BEATS_CORRECTED_V3_ON_DEVELOPMENT_FILLS_PF_WIN_RATE_NET_WITH_TRAIN_VALIDATION_GUARDRAILS",
                    "best_rejected_config_id": best["config_id"],
                    **{
                        column: best[column]
                        for column in best.index
                        if column.startswith(("train_", "validation_", "development_"))
                    },
                }
            )
            continue
        winner = candidates.iloc[0].copy()
        winner["selection_role"] = "NUMERIC_DEVELOPMENT_CHAMPION"
        champions.append(winner)
        conservative = candidates.loc[candidates["partial_pct"].eq(0.20)].sort_values(
            ["development_net_pct", "development_pf", "development_win_rate_pct"],
            ascending=False,
            kind="stable",
        )
        conservative_config_id: str | None = None
        if profile.role in {"REQUESTED_A", "REQUESTED_B", "REQUESTED_C"} and not conservative.empty:
            conservative_winner = conservative.iloc[0].copy()
            conservative_winner["selection_role"] = "CONSERVATIVE_P20_DEVELOPMENT_CHAMPION"
            champions.append(conservative_winner)
            conservative_config_id = str(conservative_winner["config_id"])
        profile_decisions.append(
            {
                "profile": profile.name,
                "profile_role": profile.role,
                "selected_before_pseudo_test": True,
                "selection_rule": "MAX_DEVELOPMENT_NET_THEN_PF_THEN_WIN_RATE_AMONG_ALL_FOUR_BEATERS",
                "selected_config_id": winner["config_id"],
                "selected_conservative_p20_config_id": conservative_config_id,
                **{
                    column: winner[column]
                    for column in winner.index
                    if column.startswith(("train_", "validation_", "development_"))
                },
            }
        )
    decisions = pd.DataFrame(profile_decisions)
    decisions.to_csv(OUT / "profile_development_decisions.csv", index=False)

    selected = pd.DataFrame(champions).reset_index(drop=True) if champions else pd.DataFrame()
    reveal_rows: list[dict[str, Any]] = []
    cost_rows: list[dict[str, Any]] = []
    selected_trade_audits: list[pd.DataFrame] = []
    if not selected.empty:
        for chosen in selected.itertuples(index=False):
            profile = str(chosen.profile)
            config_id = str(chosen.config_id)
            gross = gross_audits[(profile, config_id)]
            audit5 = with_cost(gross, 5.0)
            selected_copy = audit5.copy()
            selected_copy["profile"] = profile
            selected_copy["config_id"] = config_id
            selected_trade_audits.append(selected_copy)
            reveal = chosen._asdict()
            reveal["selection_basis"] = "TRAIN_PLUS_VALIDATION_ONLY"
            reveal["pseudo_test_used_for_selection"] = False
            reveal["pseudo_test_beats_corrected_v3_all_four"] = headline_nonworse(
                pd.Series(reveal), base5, "pseudo_test"
            )
            reveal["all_beats_corrected_v3_all_four"] = headline_nonworse(
                pd.Series(reveal), base5, "all"
            )
            reveal_rows.append(reveal)
            for cost in COSTS_BPS:
                candidate_row = grid.loc[
                    grid["profile"].eq(profile)
                    & grid["config_id"].eq(config_id)
                    & grid["cost_bps"].eq(cost)
                ].iloc[0]
                baseline_row = baseline_metrics.loc[
                    baseline_metrics["cost_bps"].eq(cost)
                ].iloc[0]
                item = candidate_row.to_dict()
                for period in ("train", "validation", "development", "pseudo_test", "all"):
                    item[f"{period}_beats_baseline_all_four"] = headline_nonworse(
                        candidate_row, baseline_row, period
                    )
                    for metric in ("fills", "pf", "win_rate_pct", "net_pct"):
                        item[f"delta_{period}_{metric}"] = (
                            candidate_row[f"{period}_{metric}"]
                            - baseline_row[f"{period}_{metric}"]
                        )
                cost_rows.append(item)
    reveal_frame = pd.DataFrame(reveal_rows)
    reveal_frame.to_csv(OUT / "pseudo_test_reveal.csv", index=False)
    cost_stress = pd.DataFrame(cost_rows)
    if not cost_stress.empty:
        robust_flags = (
            cost_stress.groupby(["profile", "config_id"])[
                [
                    "train_beats_baseline_all_four",
                    "validation_beats_baseline_all_four",
                    "development_beats_baseline_all_four",
                    "pseudo_test_beats_baseline_all_four",
                    "all_beats_baseline_all_four",
                ]
            ]
            .all()
            .reset_index()
            .rename(
                columns={
                    "train_beats_baseline_all_four": "all_costs_train_all_four",
                    "validation_beats_baseline_all_four": "all_costs_validation_all_four",
                    "development_beats_baseline_all_four": "all_costs_development_all_four",
                    "pseudo_test_beats_baseline_all_four": "all_costs_pseudo_test_all_four",
                    "all_beats_baseline_all_four": "all_costs_all_sample_all_four",
                }
            )
        )
        robust_flags["aggregate_robust_beats_corrected_v3"] = robust_flags[
            [
                "all_costs_development_all_four",
                "all_costs_pseudo_test_all_four",
                "all_costs_all_sample_all_four",
            ]
        ].all(axis=1)
        robust_flags["strict_every_split_and_cost_beats_corrected_v3"] = robust_flags[
            [
                "all_costs_train_all_four",
                "all_costs_validation_all_four",
                "all_costs_pseudo_test_all_four",
                "all_costs_all_sample_all_four",
            ]
        ].all(axis=1)
        robust_flags["truly_beats_corrected_v3"] = robust_flags[
            "strict_every_split_and_cost_beats_corrected_v3"
        ]
        cost_stress = cost_stress.merge(
            robust_flags, on=["profile", "config_id"], how="left", validate="many_to_one"
        )
    cost_stress.to_csv(OUT / "selected_cost_stress.csv", index=False)

    headline_rows: list[dict[str, Any]] = [
        {
            "row_type": "CORRECTED_BASELINE",
            "profile": "CORRECTED_V3_NATIVE_1515_GAP",
            "selection_role": "COMPARATOR",
            "config_id": "NATIVE_PER_SETUP_EXITS",
            **{
                column: base5[column]
                for column in base5.index
                if column.startswith(("train_", "validation_", "development_", "pseudo_test_", "all_"))
            },
        }
    ]
    for reveal in reveal_rows:
        item = {
            "row_type": "DEVELOPMENT_SELECTED_THEN_PSEUDO_REVEALED",
            **reveal,
        }
        for period in ("train", "validation", "development", "pseudo_test", "all"):
            for metric_name in ("fills", "pf", "win_rate_pct", "net_pct"):
                item[f"delta_{period}_{metric_name}"] = (
                    item[f"{period}_{metric_name}"] - base5[f"{period}_{metric_name}"]
                )
        headline_rows.append(item)
    headline_comparison = pd.DataFrame(headline_rows)
    headline_comparison.to_csv(OUT / "selected_headline_comparison_5bps.csv", index=False)

    grid5 = grid5.merge(
        reveal_frame[
            [
                "profile",
                "config_id",
                "pseudo_test_beats_corrected_v3_all_four",
                "all_beats_corrected_v3_all_four",
            ]
        ]
        if not reveal_frame.empty
        else pd.DataFrame(
            columns=[
                "profile",
                "config_id",
                "pseudo_test_beats_corrected_v3_all_four",
                "all_beats_corrected_v3_all_four",
            ]
        ),
        on=["profile", "config_id"],
        how="left",
    )
    grid5["selected_before_pseudo_test"] = grid5.set_index(
        ["profile", "config_id"]
    ).index.isin(
        reveal_frame.set_index(["profile", "config_id"]).index
        if not reveal_frame.empty
        else []
    )

    def rejection_reason(row: pd.Series) -> str:
        reasons: list[str] = []
        if not row["train_validation_guardrail"]:
            reasons.append("TRAIN_VALIDATION_GUARDRAIL_FAIL")
        if not row["development_beats_corrected_v3_all_four"]:
            reasons.append("NOT_ALL_FOUR_DEVELOPMENT_BEATER")
        if (
            row["train_validation_guardrail"]
            and row["development_beats_corrected_v3_all_four"]
            and not row["selected_before_pseudo_test"]
        ):
            reasons.append("LOWER_DEVELOPMENT_NET_THAN_PROFILE_CHAMPION")
        return "|".join(reasons) or "NONE"

    grid5["development_rejection_reason"] = grid5.apply(rejection_reason, axis=1)
    grid5.to_csv(OUT / "grid_metrics_5bps.csv", index=False)
    c_neighborhood = grid5.loc[
        grid5["profile"].eq("C_V3_1120S_WICK010_0950S")
        & (
            (
                grid5["partial_pct"].eq(0.10)
                & grid5["runner_target_pct"].eq(2.60)
                & grid5["t1_pct"].isin(T1_GRID)
            )
            | (
                grid5["t1_pct"].eq(1.075)
                & grid5["partial_pct"].eq(0.20)
                & grid5["runner_target_pct"].eq(2.60)
            )
            | (
                grid5["t1_pct"].eq(1.075)
                & grid5["partial_pct"].eq(0.10)
                & grid5["runner_target_pct"].isin((2.50, 2.70))
            )
        )
    ].copy()
    c_neighborhood.to_csv(OUT / "c_candidate_neighborhood_5bps.csv", index=False)
    grid.to_csv(OUT / "grid_all_costs.csv", index=False)
    grid5.loc[~grid5["selected_before_pseudo_test"]].to_csv(
        OUT / "rejected_experiments.csv", index=False
    )
    if five_bps_audits:
        pd.concat(five_bps_audits, ignore_index=True, sort=False).to_csv(
            OUT / "all_trade_audits_5bps.csv", index=False
        )
    if selected_trade_audits:
        pd.concat(selected_trade_audits, ignore_index=True, sort=False).to_csv(
            OUT / "selected_trade_audits_5bps.csv", index=False
        )

    # Entry order and full factorial component-ablation summaries.
    orders_out = pd.concat(profile_orders.values(), ignore_index=True, sort=False)
    orders_out.to_csv(OUT / "profile_entry_orders.csv", index=False)
    entry_summaries: list[dict[str, Any]] = []
    p0_sids = set(profile_orders["P0_V3"]["sid"].astype(int))
    for profile in PROFILES:
        frame = profile_orders[profile.name]
        raw = simulate_native(frame, raw_paths, cost_bps=5.0)
        entry_summaries.append(
            {
                "profile": profile.name,
                "profile_role": profile.role,
                "component_1120_short": profile.add_1120_short,
                "component_wick_plus_010": profile.wick_plus_010,
                "component_0950_short": profile.add_0950_short,
                "selected_orders": len(frame),
                "unique_sids": frame["sid"].nunique(),
                "baseline_sid_overlap": len(set(frame["sid"].astype(int)) & p0_sids),
                "new_sids_vs_baseline": len(set(frame["sid"].astype(int)) - p0_sids),
                **flatten_metrics(raw, periods),
            }
        )
    pd.DataFrame(entry_summaries).to_csv(OUT / "profile_entry_summary.csv", index=False)
    setup_registry.to_csv(OUT / "profile_setup_registry.csv", index=False)
    path_audit.to_csv(OUT / "raw_path_audit.csv", index=False)
    parity.to_csv(OUT / "corrected_baseline_trade_parity.csv", index=False)

    # Exact component deltas holding the exit configuration fixed.
    ablation_pairs = [
        ("P0_V3", "A_V3_1120S", "ADD_1120S"),
        ("W_V3_WICK010", "B_V3_1120S_WICK010", "ADD_1120S"),
        ("H_V3_0950S", "AH_V3_1120S_0950S", "ADD_1120S"),
        ("WH_V3_WICK010_0950S", "C_V3_1120S_WICK010_0950S", "ADD_1120S"),
        ("P0_V3", "W_V3_WICK010", "ADD_WICK010"),
        ("A_V3_1120S", "B_V3_1120S_WICK010", "ADD_WICK010"),
        ("H_V3_0950S", "WH_V3_WICK010_0950S", "ADD_WICK010"),
        ("AH_V3_1120S_0950S", "C_V3_1120S_WICK010_0950S", "ADD_WICK010"),
        ("P0_V3", "H_V3_0950S", "ADD_0950S"),
        ("A_V3_1120S", "AH_V3_1120S_0950S", "ADD_0950S"),
        ("W_V3_WICK010", "WH_V3_WICK010_0950S", "ADD_0950S"),
        ("B_V3_1120S_WICK010", "C_V3_1120S_WICK010_0950S", "ADD_0950S"),
    ]
    lookup = grid5.set_index(["profile", "config_id"])
    ablations: list[dict[str, Any]] = []
    for base_profile, changed_profile, component in ablation_pairs:
        for config in configs:
            left = lookup.loc[(base_profile, config.config_id)]
            right = lookup.loc[(changed_profile, config.config_id)]
            item: dict[str, Any] = {
                "base_profile": base_profile,
                "changed_profile": changed_profile,
                "component": component,
                "config_id": config.config_id,
                "cost_bps": 5.0,
            }
            for period in periods:
                for metric_name in ("orders", "fills", "pf", "win_rate_pct", "net_pct"):
                    item[f"delta_{period}_{metric_name}"] = (
                        right[f"{period}_{metric_name}"] - left[f"{period}_{metric_name}"]
                    )
            ablations.append(item)
    pd.DataFrame(ablations).to_csv(OUT / "component_ablation_deltas_5bps.csv", index=False)

    assert_cache_unchanged(input_audit["cache_checks"])
    metadata = {
        "analysis_id": "V13_V5_PROFILE_TWO_STAGE_1515_GAP_V1",
        "warning_no_untouched_test": (
            "PSEUDO_TEST is a chronological reveal only, not genuinely untouched; V13-v3 and "
            "the candidate entry ideas were developed after inspecting parts or all of these 25 sessions."
        ),
        "execution_contract": {
            "signal_and_confirmation": "unchanged V13-v3 strict exact S+1 confirmation",
            "first_entry_check": "next end-labelled 1m candle, S+2",
            "cutoff": "15:15 inclusive for every profile and comparator",
            "entry_gap": "if first touched bar opens adversely beyond stop-entry trigger, fill at actual open",
            "levels": "initial stop, T1, runner target and breakeven rebase to actual entry price",
            "same_bar": "initial stop wins stop/T1 ties; after T1 the runner stop wins stop/target ties; T1 bar is immediately eligible for runner checks",
            "runner_stop": RUNNER_STOP,
            "cost": "one configured full-position round-trip bps deduction from weighted gross return",
            "exit_gap": "stop/target orders fill at their levels; only trigger-gap entry correction is in scope, matching validation_audit",
        },
        "split": {name: [str(day) for day in days] for name, days in periods.items()},
        "profile_definitions": [asdict(profile) for profile in PROFILES],
        "exit_grid": {
            "initial_stop_pct": INITIAL_STOP_PCT,
            "t1_pct": T1_GRID,
            "partial_pct": PARTIAL_GRID,
            "runner_target_pct": RUNNER_GRID,
            "runner_stop": RUNNER_STOP,
            "configs_per_profile": len(configs),
        },
        "costs_bps": COSTS_BPS,
        "selection_protocol": (
            "At 5 bps, require positive PF/net separately in TRAIN and VALIDATION and >=50% pooled "
            "development T1 rate, "
            "and non-worse development fills/PF/win-rate/net versus corrected V3; select maximum "
            "development net per profile, tie-breaking PF then win rate. Reveal PSEUDO_TEST afterward."
        ),
        "baseline_parity": parity_result,
        "input_audit": input_audit,
        "raw_one_minute_inputs": raw_files,
        "source_files": [
            {"path": str(Path(v3.__file__).resolve()), "sha256": sha256(Path(v3.__file__))},
            {"path": str(Path(v2.__file__).resolve()), "sha256": sha256(Path(v2.__file__))},
            {"path": str(Path(selector.__file__).resolve()), "sha256": sha256(Path(selector.__file__))},
            {"path": str(Path(hybrid.__file__).resolve()), "sha256": sha256(Path(hybrid.__file__))},
            {"path": str(CORRECTED_REPLAY_PATH.resolve()), "sha256": sha256(CORRECTED_REPLAY_PATH)},
            {"path": str(Path(__file__).resolve()), "sha256": sha256(Path(__file__))},
        ],
    }
    write_json(OUT / "metadata.json", metadata)

    robust = (
        cost_stress[
            [
                "profile",
                "config_id",
                "all_costs_train_all_four",
                "all_costs_validation_all_four",
                "all_costs_development_all_four",
                "all_costs_pseudo_test_all_four",
                "all_costs_all_sample_all_four",
                "aggregate_robust_beats_corrected_v3",
                "strict_every_split_and_cost_beats_corrected_v3",
                "truly_beats_corrected_v3",
            ]
        ].drop_duplicates()
        if not cost_stress.empty
        else pd.DataFrame()
    )
    summary = {
        "warning_no_untouched_test": metadata["warning_no_untouched_test"],
        "corrected_v3_baseline_5bps": baseline_metrics.loc[
            baseline_metrics["cost_bps"].eq(5.0)
        ].iloc[0].to_dict(),
        "profile_entry_summary": pd.DataFrame(entry_summaries).to_dict("records"),
        "profiles": len(PROFILES),
        "exit_configs_per_profile": len(configs),
        "grid_rows_5bps": len(grid5),
        "selected_before_pseudo_test": reveal_frame.to_dict("records"),
        "robust_all_costs_flags": robust.to_dict("records"),
        "selected_headline_comparison_5bps": headline_comparison.to_dict("records"),
        "any_profile_truly_beats_corrected_v3": bool(
            robust["truly_beats_corrected_v3"].any()
        )
        if not robust.empty
        else False,
        "any_profile_aggregate_robust_beats_corrected_v3": bool(
            robust["aggregate_robust_beats_corrected_v3"].any()
        )
        if not robust.empty
        else False,
        "baseline_parity": parity_result,
    }
    write_json(OUT / "summary.json", summary)

    # Final manifest excludes itself so its hashes are stable.
    manifest: list[dict[str, Any]] = []
    for path in sorted(OUT.iterdir()):
        if not path.is_file() or path.name == "artifact_manifest.csv":
            continue
        item = {
            "file": path.name,
            "bytes": path.stat().st_size,
            "sha256": sha256(path),
            "type": path.suffix.lower().lstrip("."),
        }
        if path.suffix.lower() == ".csv":
            try:
                frame = pd.read_csv(path)
                item.update(rows=len(frame), columns=len(frame.columns))
            except pd.errors.EmptyDataError:
                item.update(rows=0, columns=0)
        manifest.append(item)
    pd.DataFrame(manifest).to_csv(OUT / "artifact_manifest.csv", index=False)

    print(
        json.dumps(
            clean(
                {
                    "baseline_parity": parity_result,
                    "baseline_5bps": summary["corrected_v3_baseline_5bps"],
                    "entry_profiles": [
                        {
                            "profile": item["profile"],
                            "orders": item["selected_orders"],
                            "all_fills": item["all_fills"],
                            "all_pf": item["all_pf"],
                            "all_win_rate_pct": item["all_win_rate_pct"],
                            "all_net_pct": item["all_net_pct"],
                        }
                        for item in entry_summaries
                    ],
                    "selected": reveal_frame[
                        [
                            "profile",
                            "config_id",
                            "development_fills",
                            "development_pf",
                            "development_win_rate_pct",
                            "development_net_pct",
                            "pseudo_test_fills",
                            "pseudo_test_pf",
                            "pseudo_test_win_rate_pct",
                            "pseudo_test_net_pct",
                            "all_fills",
                            "all_pf",
                            "all_win_rate_pct",
                            "all_net_pct",
                        ]
                    ].to_dict("records")
                    if not reveal_frame.empty
                    else [],
                    "robust": robust.to_dict("records"),
                }
            ),
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
