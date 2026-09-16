"""Causal ATM±2 option-liquidity selector for V13-v6 shadow research.

The selector uses only candles strictly before the planned option entry time.
It never changes the V13-v5 control ledger.  A candidate must meet absolute
liquidity floors; candidates retaining at least half of the best observed
pre-entry volume are then ranked by proximity to the original ATM contract.
All candidates and the resulting shadow coverage are written for audit.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

import fno_oi_common as common
import fno_v13_v5_options_backtest as legacy
import fno_v13_v6_options_backtest as execution


SCHEMA_VERSION = "FNO_V13_V6_CAUSAL_OPTION_SELECTOR_V1"
DEFAULT_OUTPUT_ROOT = common.FNO_ROOT / "strategy_research" / "v13_corrected_v6_option_selector"


@dataclass(frozen=True)
class LiquiditySelectorConfig:
    lookback_minutes: int = 30
    min_traded_bars: int = 3
    min_window_lot_equivalents: float = 1.0
    relative_volume_floor: float = 0.50
    max_reference_delay_minutes: float = 5.0

    def validate(self) -> None:
        if self.lookback_minutes <= 0:
            raise ValueError("lookback_minutes must be positive")
        if self.min_traded_bars < 1:
            raise ValueError("min_traded_bars must be at least one")
        if self.min_window_lot_equivalents < 0:
            raise ValueError("min_window_lot_equivalents cannot be negative")
        if not 0 < self.relative_volume_floor <= 1:
            raise ValueError("relative_volume_floor must be in (0, 1]")
        if self.max_reference_delay_minutes < 0:
            raise ValueError("max_reference_delay_minutes cannot be negative")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _session_membership(value: Any, day: str) -> bool:
    sessions = {item.strip() for item in str(value or "").split("|") if item.strip()}
    return day in sessions


def load_fetch_plans(data_roots: list[Path]) -> tuple[pd.DataFrame, list[dict[str, str]]]:
    frames: list[pd.DataFrame] = []
    sources: list[dict[str, str]] = []
    for priority, supplied_root in enumerate(data_roots):
        root = supplied_root.resolve()
        plan_path = root / "audit" / "option_fetch_plan.csv"
        if not plan_path.is_file():
            raise FileNotFoundError(f"Missing option fetch plan: {plan_path}")
        plan = pd.read_csv(plan_path)
        required = {
            "underlying",
            "tradingsymbol",
            "expiry",
            "strike",
            "instrument_type",
            "lot_size",
            "tick_size",
            "required_sessions",
        }
        missing = sorted(required.difference(plan.columns))
        if missing:
            raise ValueError(f"Fetch plan lacks columns {missing}: {plan_path}")
        plan["_source_priority"] = priority
        plan["_data_root"] = str(root)
        plan["_raw_options_dir"] = str(root / "raw_options_1m")
        frames.append(plan)
        sources.append({"path": str(plan_path), "sha256": _sha256(plan_path)})
    return pd.concat(frames, ignore_index=True), sources


def _candidate_stats(
    candles: pd.DataFrame,
    *,
    entry_at: pd.Timestamp,
    lot_size: int,
    lookback_minutes: int,
) -> dict[str, float]:
    if candles.empty:
        return {
            "pre_entry_traded_bars": 0,
            "pre_entry_volume": 0.0,
            "pre_entry_volume_lots": 0.0,
            "pre_entry_last_oi": 0.0,
            "pre_entry_last_oi_lots": 0.0,
            "pre_entry_last_close": np.nan,
        }
    start = entry_at - pd.Timedelta(minutes=lookback_minutes)
    window = candles.loc[
        candles["timestamp"].ge(start)
        & candles["timestamp"].lt(entry_at)
        & candles["volume"].gt(0)
    ]
    if window.empty:
        return {
            "pre_entry_traded_bars": 0,
            "pre_entry_volume": 0.0,
            "pre_entry_volume_lots": 0.0,
            "pre_entry_last_oi": 0.0,
            "pre_entry_last_oi_lots": 0.0,
            "pre_entry_last_close": np.nan,
        }
    volume = float(pd.to_numeric(window["volume"], errors="coerce").fillna(0).sum())
    last_oi = float(pd.to_numeric(window["oi"], errors="coerce").fillna(0).iloc[-1]) if "oi" in window else 0.0
    return {
        "pre_entry_traded_bars": int(len(window)),
        "pre_entry_volume": volume,
        "pre_entry_volume_lots": volume / lot_size,
        "pre_entry_last_oi": last_oi,
        "pre_entry_last_oi_lots": last_oi / lot_size,
        "pre_entry_last_close": float(window["close"].iloc[-1]),
    }


def _first_traded_bar(
    candles: pd.DataFrame, at: pd.Timestamp
) -> tuple[pd.Series | None, float | None]:
    if candles.empty:
        return None, None
    candidates = candles.loc[
        candles["timestamp"].dt.date.eq(at.date())
        & candles["timestamp"].ge(at)
        & candles["volume"].gt(0)
    ]
    if candidates.empty:
        return None, None
    bar = candidates.iloc[0]
    return bar, float((bar["timestamp"] - at).total_seconds() / 60.0)


def build_liquidity_shadow(
    coverage: pd.DataFrame,
    plans: pd.DataFrame,
    config: LiquiditySelectorConfig,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    config.validate()
    cache: dict[str, pd.DataFrame] = {}
    audit_rows: list[dict[str, Any]] = []
    selected_rows: list[dict[str, Any]] = []

    for source_row in coverage.to_dict("records"):
        row = dict(source_row)
        day = pd.Timestamp(row["day"]).date().isoformat()
        primary_symbol = str(row.get("option_tradingsymbol", "")).strip().upper()
        primary_strike = pd.to_numeric(row.get("option_strike"), errors="coerce")
        entry_at = legacy._as_ist(row["equity_entry_ts"]) + pd.Timedelta(minutes=1)
        candidates = plans.loc[
            plans["underlying"].astype(str).str.upper().eq(str(row.get("equity_symbol", "")).upper())
            & plans["instrument_type"].astype(str).str.upper().eq(str(row.get("required_option_type", "")).upper())
            & pd.to_datetime(plans["expiry"], errors="coerce").dt.date.eq(
                pd.Timestamp(row.get("required_option_expiry")).date()
            )
            & plans["required_sessions"].map(lambda value: _session_membership(value, day))
        ].copy()
        candidates = candidates.sort_values("_source_priority", kind="stable").drop_duplicates(
            "tradingsymbol", keep="last"
        )
        if candidates.empty:
            row["selector_status"] = "NO_FETCHED_LADDER_FOR_TRADE"
            row["selector_reason"] = "NO_CANDIDATES"
            selected_rows.append(row)
            continue

        strikes = sorted(pd.to_numeric(candidates["strike"], errors="coerce").dropna().unique())
        if pd.isna(primary_strike):
            primary_index = 0
        else:
            primary_index = min(range(len(strikes)), key=lambda idx: abs(strikes[idx] - float(primary_strike)))
        candidate_records: list[dict[str, Any]] = []
        for contract in candidates.to_dict("records"):
            symbol = str(contract["tradingsymbol"]).strip().upper()
            raw_dir = Path(str(contract["_raw_options_dir"]))
            candles = execution.load_option_candles(symbol, raw_dir, cache)
            lot_size = int(contract["lot_size"])
            stats = _candidate_stats(
                candles,
                entry_at=entry_at,
                lot_size=lot_size,
                lookback_minutes=config.lookback_minutes,
            )
            strike = float(contract["strike"])
            strike_index = min(range(len(strikes)), key=lambda idx: abs(strikes[idx] - strike))
            record = {
                "trade_id": str(row.get("trade_id", "")),
                "day": day,
                "equity_symbol": row.get("equity_symbol"),
                "entry_cutoff_exclusive": entry_at,
                "primary_option_tradingsymbol": primary_symbol,
                "candidate_tradingsymbol": symbol,
                "instrument_token": contract.get("instrument_token"),
                "expiry": contract["expiry"],
                "option_type": contract["instrument_type"],
                "strike": strike,
                "strike_distance_steps": abs(strike_index - primary_index),
                "lot_size": lot_size,
                "tick_size": float(contract["tick_size"]),
                "raw_options_dir": str(raw_dir),
                **stats,
            }
            record["absolute_liquidity_eligible"] = bool(
                stats["pre_entry_traded_bars"] >= config.min_traded_bars
                and stats["pre_entry_volume_lots"] >= config.min_window_lot_equivalents
            )
            candidate_records.append(record)

        viable = [record for record in candidate_records if record["absolute_liquidity_eligible"]]
        if viable:
            best_volume = max(record["pre_entry_volume_lots"] for record in viable)
            relative_floor = best_volume * config.relative_volume_floor
            qualified = [
                record for record in viable if record["pre_entry_volume_lots"] >= relative_floor
            ]
            selected = sorted(
                qualified,
                key=lambda record: (
                    record["strike_distance_steps"],
                    -record["pre_entry_volume_lots"],
                    -record["pre_entry_last_oi_lots"],
                    record["candidate_tradingsymbol"],
                ),
            )[0]
            selector_status = "SELECTED_CAUSAL_LIQUIDITY"
            selector_reason = (
                "PRIMARY_RETAINED_WITHIN_RELATIVE_LIQUIDITY_FLOOR"
                if selected["candidate_tradingsymbol"] == primary_symbol
                else "NEAR_ATM_LIQUIDITY_OVERRIDE"
            )
        else:
            primary_matches = [
                record for record in candidate_records
                if record["candidate_tradingsymbol"] == primary_symbol
            ]
            selected = primary_matches[0] if primary_matches else sorted(
                candidate_records,
                key=lambda record: (record["strike_distance_steps"], record["candidate_tradingsymbol"]),
            )[0]
            relative_floor = np.nan
            selector_status = "FALLBACK_NO_LIQUID_CANDIDATE"
            selector_reason = "PRIMARY_FALLBACK" if primary_matches else "NEAREST_FETCHED_FALLBACK"

        for candidate in candidate_records:
            candidate["relative_volume_floor_lots"] = relative_floor
            candidate["selected"] = candidate["candidate_tradingsymbol"] == selected["candidate_tradingsymbol"]
            candidate["selector_status"] = selector_status
            candidate["selector_reason"] = selector_reason if candidate["selected"] else "NOT_SELECTED"
            audit_rows.append(candidate)

        selected_symbol = selected["candidate_tradingsymbol"]
        selected_candles = execution.load_option_candles(
            selected_symbol, Path(selected["raw_options_dir"]), cache
        )
        exit_at = legacy._as_ist(row["equity_exit_ts"]) + pd.Timedelta(minutes=1)
        entry_bar, entry_delay = _first_traded_bar(selected_candles, entry_at)
        exit_bar, exit_delay = _first_traded_bar(selected_candles, exit_at)
        row.update(
            {
                "selector_status": selector_status,
                "selector_reason": selector_reason,
                "selector_entry_cutoff_exclusive": entry_at,
                "selector_pre_entry_traded_bars": selected["pre_entry_traded_bars"],
                "selector_pre_entry_volume_lots": selected["pre_entry_volume_lots"],
                "selector_pre_entry_oi_lots": selected["pre_entry_last_oi_lots"],
                "original_option_tradingsymbol": primary_symbol,
                "option_tradingsymbol": selected_symbol,
                "option_instrument_token": selected["instrument_token"],
                "option_strike": selected["strike"],
                "option_tick_size": selected["tick_size"],
                "lot_size": selected["lot_size"],
                "quantity": selected["lot_size"],
                "mapping_status": "MAPPED_LIQUIDITY_SHADOW",
                "_raw_options_dir": selected["raw_options_dir"],
                "maximum_reference_delay_minutes": config.max_reference_delay_minutes,
            }
        )
        if entry_bar is None:
            row["coverage_state"] = "NO_TRADED_REFERENCE_ENTRY_BAR"
        elif entry_delay is not None and entry_delay > config.max_reference_delay_minutes:
            row["coverage_state"] = "ENTRY_LIQUIDITY_DELAY_EXCEEDS_LIMIT"
        elif exit_bar is None:
            row["coverage_state"] = "NO_TRADED_REFERENCE_EXIT_BAR"
        elif exit_delay is not None and exit_delay > config.max_reference_delay_minutes:
            row["coverage_state"] = "EXIT_LIQUIDITY_DELAY_EXCEEDS_LIMIT"
        else:
            row["coverage_state"] = "READY"
        if entry_bar is not None:
            entry_premium = float(entry_bar["open"])
            row.update(
                reference_entry_bar=entry_bar["timestamp"],
                reference_entry_delay_min=entry_delay,
                reference_entry_premium=entry_premium,
                entry_bar_volume=float(entry_bar["volume"]),
                entry_bar_oi=float(entry_bar.get("oi", np.nan)),
                one_lot_premium_outlay_rupees=entry_premium * int(selected["lot_size"]),
            )
        if exit_bar is not None:
            row.update(
                reference_exit_bar=exit_bar["timestamp"],
                reference_exit_delay_min=exit_delay,
                reference_exit_premium=float(exit_bar["open"]),
            )
        selected_rows.append(row)

    return pd.DataFrame(selected_rows), pd.DataFrame(audit_rows)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-root", type=Path, action="append", dest="data_roots")
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--lookback-minutes", type=int, default=30)
    parser.add_argument("--min-traded-bars", type=int, default=3)
    parser.add_argument("--min-window-lot-equivalents", type=float, default=1.0)
    parser.add_argument("--relative-volume-floor", type=float, default=0.5)
    parser.add_argument("--run-id")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    config = LiquiditySelectorConfig(
        lookback_minutes=args.lookback_minutes,
        min_traded_bars=args.min_traded_bars,
        min_window_lot_equivalents=args.min_window_lot_equivalents,
        relative_volume_floor=args.relative_volume_floor,
    )
    roots = args.data_roots or [execution.DEFAULT_DATA_ROOT]
    coverage, coverage_sources = execution.load_coverage_roots(roots)
    plans, plan_sources = load_fetch_plans(roots)
    selected, audit = build_liquidity_shadow(coverage, plans, config)

    generated = common.now_ist()
    run_id = args.run_id or generated.strftime("v13_v6_selector_%Y%m%dT%H%M%S_IST")
    if any(character not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-" for character in run_id):
        raise ValueError("run-id may contain only letters, digits, underscore and hyphen")
    run_dir = args.output_root.resolve() / run_id
    if run_dir.exists():
        raise FileExistsError(f"Refusing to overwrite selector run: {run_dir}")
    run_dir.mkdir(parents=True)
    selected_path = run_dir / "fno_v13_v6_liquidity_selected_coverage.csv"
    audit_path = run_dir / "fno_v13_v6_liquidity_candidate_audit.csv"
    manifest_path = run_dir / "manifest.json"
    common.atomic_write_csv(selected, selected_path)
    common.atomic_write_csv(audit, audit_path)
    data_days = pd.to_datetime(selected.get("day"), errors="coerce")
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "complete": True,
        "shadow_only": True,
        "run_id": run_id,
        "generated_at_ist": generated.isoformat(timespec="seconds"),
        "data_through_date": data_days.max().date().isoformat() if data_days.notna().any() else None,
        "config": asdict(config),
        "coverage_sources": coverage_sources,
        "fetch_plan_sources": plan_sources,
        "outputs": {
            "selected_coverage": {"path": str(selected_path), "sha256": _sha256(selected_path)},
            "candidate_audit": {"path": str(audit_path), "sha256": _sha256(audit_path)},
        },
    }
    common.atomic_write_json(manifest_path, manifest)
    counts = selected.get("selector_reason", pd.Series(dtype=str)).value_counts(dropna=False)
    print(counts.to_string())
    print(f"[V13-v6 selector][SHADOW RUN] {run_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
