"""Extend the V13-v10-G-2 1% stop study with sealed daily G replays.

The published complete result ends on 2026-09-30.  October 1 is emitted only
as a separate partial diagnostic because the live producer stopped before the
strategy's 11:20 signal slot.  The observed 09:51 MCX selection is replayed
with a cached Yahoo minute path that is cross-checked against the overlapping
Kite 5-minute archive, but it is never included in the complete totals.
"""
from __future__ import annotations

import argparse
import json
import math
import os
import shutil
import tempfile
import urllib.request
from dataclasses import replace
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

import fno_oi_common as common
import fno_oi_hybrid_data as hybrid
import fno_v13_v10_g_2_backtest as g2
import fno_v13_v10_g_daily_replay as daily_replay


VERSION = "V13-v10-G-2"
COMPLETE_EXTENSION_DAYS = (
    date(2026, 9, 24),
    date(2026, 9, 25),
    date(2026, 9, 28),
    date(2026, 9, 29),
    date(2026, 9, 30),
)
OCT1 = date(2026, 10, 1)
DEFAULT_BASE_G2 = Path(
    r"C:\TradingData\eqidv2\fno_oi\strategy_research\v13_corrected_v10_g_2"
) / "run_20261004_sl100_through_20260923"
DEFAULT_DAILY_ROOT = Path(r"C:\TradingData\eqidv2\backtesting_result_v13_v10_g\runs")
DEFAULT_OUTPUT = Path(
    r"C:\TradingData\eqidv2\fno_oi\strategy_research\v13_corrected_v10_g_2"
) / "run_20261004_sl100_complete_to_20260930_oct1_partial_v1"
OCT1_SIGNAL = Path(
    r"C:\TradingData\eqidv2\fno_oi\v13_v10_g_live\signals\2026-10-01"
) / "20261001_0951_SHORT_MCX_a4fd741b9080.json"
OCT1_PAPER_ORDER = Path(
    r"C:\TradingData\eqidv2\fno_oi\v13_v10_g_live\orders\PAPER\2026-10-01"
) / "20261001_0951_SHORT_MCX_a4fd741b9080.json"
OCT1_SCANNER = Path(
    r"C:\TradingData\eqidv2\fno_oi\v13_v10_g_live\scanner_5m\2026-10-01"
)
OCT1_LOCAL_MCX_5M = Path(
    r"C:\TradingData\eqidv2\stocks_indicators_5min_eq_live"
) / "MCX_stocks_indicators_5min.parquet"


def _read_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"Expected JSON object: {path}")
    return value


def _write_json(path: Path, value: Any) -> None:
    path.write_text(
        json.dumps(value, indent=2, sort_keys=True, default=str, allow_nan=False) + "\n",
        encoding="utf-8",
    )


def _finite_json(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _finite_json(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_finite_json(item) for item in value]
    if isinstance(value, np.generic):
        value = value.item()
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if isinstance(value, (date, datetime, pd.Timestamp, Path)):
        return str(value)
    return value


def _verify_output(root: Path) -> dict[str, Any]:
    provenance = _read_json(root / "provenance.json")
    mismatches = []
    for name, record in provenance.get("artifacts", {}).items():
        path = root / name
        if not path.is_file() or g2.sha256(path) != record.get("sha256"):
            mismatches.append(name)
    if mismatches:
        raise RuntimeError(f"Base G-2 output drift: {', '.join(mismatches)}")
    return provenance


def _portfolio_config(source_g: dict[str, Any]):
    base = replace(
        g2.g.v9.V9Config(),
        portfolio_capital_rupees=float(source_g["portfolio_capital_rupees"]),
        capital_per_entry_rupees=float(source_g["capital_per_entry_rupees"]),
        leverage_factor=float(source_g["leverage_factor"]),
        max_positions=source_g["max_positions"],
        cost_bps=float(source_g["cost_bps"]),
    )
    base.validate()
    return base


def _load_base(base_g2: Path) -> dict[str, Any]:
    source = g2.DEFAULT_SOURCE_BUNDLE.resolve()
    manifest = g2.verify_bundle(source)
    provenance = _verify_output(base_g2)
    source_g = _read_json(g2.DEFAULT_G_CONFIG)
    g2.g.checked_settings(source_g)
    settings = g2.checked_settings(_read_json(base_g2 / "frozen_config.json"), source_g)
    run_metadata = _read_json(source / "g_backtest/run_metadata.json")
    if g2.sha256(g2.DEFAULT_G_CONFIG) != run_metadata.get("frozen_g_config_sha256"):
        raise RuntimeError("Sealed base G configuration hash mismatch")
    dataset_manifest = _read_json(source / "dataset/dataset_manifest.json")
    days = [date.fromisoformat(value) for value in dataset_manifest["days"]]
    g_trades = pd.read_csv(source / "g_backtest/selected_trades.csv")
    g2_trades = pd.read_csv(base_g2 / "selected_trades.csv")
    if len(g_trades) != len(g2_trades):
        raise RuntimeError("Base G/G-2 selection count mismatch")
    identity = ["day", "sid", "setup_id", "tradingsymbol", "side"]
    if not g_trades[identity].astype(str).equals(g2_trades[identity].astype(str)):
        raise RuntimeError("Base G/G-2 selection identity mismatch")
    return {
        "source": source,
        "manifest": manifest,
        "base_provenance": provenance,
        "source_g": source_g,
        "settings": settings,
        "base": _portfolio_config(source_g),
        "days": days,
        "g_trades": g_trades,
        "g2_trades": g2_trades,
    }


def _latest_successful_run(root: Path, day: date) -> tuple[Path, dict[str, Any]]:
    folder = root / day.isoformat()
    if not folder.is_dir():
        raise FileNotFoundError(f"Missing daily replay directory: {folder}")
    candidates = []
    for run in sorted((item for item in folder.iterdir() if item.is_dir()), reverse=True):
        result_path = run / "replay_result.json"
        if not result_path.is_file():
            continue
        result = _read_json(result_path)
        if (
            result.get("state") == "SUCCESS"
            and result.get("complete") is True
            and result.get("session_date") == day.isoformat()
        ):
            candidates.append((run, result))
    if not candidates:
        raise RuntimeError(f"No complete successful daily G replay for {day}")
    return candidates[0]


def _snapshot_for_day(
    daily_root: Path,
    day: date,
    run: Path,
    result: dict[str, Any],
) -> tuple[Path, str]:
    """Return a sealed path source, allowing a later snapshot for old replay format.

    The first official daily replay (September 24) predates input-snapshot
    publication.  September 25's immutable snapshot contains the same complete
    September 24 minute history, so it is safe to use for execution-path parity
    while retaining September 24's own selection ledger as selection authority.
    """

    direct = result.get("artifacts", {}).get("input_snapshot_manifest")
    if direct:
        return Path(direct), day.isoformat()
    for candidate_day in COMPLETE_EXTENSION_DAYS:
        if candidate_day <= day:
            continue
        candidate_run, candidate_result = _latest_successful_run(daily_root, candidate_day)
        candidate = candidate_result.get("artifacts", {}).get("input_snapshot_manifest")
        if candidate:
            return Path(candidate), candidate_day.isoformat()
    raise RuntimeError(f"No sealed execution-path snapshot is available for {day} ({run})")


def _selected_paths(
    orders: pd.DataFrame,
    day: date,
    snapshot_root: Path,
) -> dict[int, dict[str, np.ndarray]]:
    paths: dict[int, dict[str, np.ndarray]] = {}
    problems: list[dict[str, Any]] = []
    orders = orders.copy()
    orders["confirmation_ts"] = pd.to_datetime(orders["confirmation_ts"])
    for row in orders.itertuples(index=False):
        path = hybrid.equity_one_minute_path(str(row.tradingsymbol), snapshot_root / "equity_1m")
        minute = daily_replay._load_minute(path, day, problems, str(row.tradingsymbol))
        selected = minute.loc[
            minute.ts.gt(pd.Timestamp(row.confirmation_ts))
            & minute.ts.le(daily_replay._cutoff(day))
        ]
        paths[int(row.sid)] = {
            "timestamp_ns": selected.ts.astype("int64").to_numpy(),
            **{
                field: selected[field].to_numpy(float)
                for field in ("open", "high", "low", "close")
            },
        }
    if problems:
        raise RuntimeError(f"Selected path quality failures for {day}: {problems}")
    g2.g.v9.validate_paths(orders, paths)
    return paths


def _simulate(
    orders: pd.DataFrame,
    paths: dict[int, dict[str, np.ndarray]],
    base: Any,
    *,
    stop_pct: float | None,
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    work = orders.copy()
    if stop_pct is not None:
        work["native_stop_pct"] = float(stop_pct)
    trades = g2.g.v9.v5.simulate_native(
        work,
        paths,
        cost_bps=base.cost_bps,
        max_entry_delay_minutes=10,
    )
    trades = g2.g.v9.v5.apply_fixed_capital_model(
        trades,
        base.capital_per_entry_rupees,
        base.leverage_factor,
    )
    if stop_pct is not None:
        trades["v10_g_2_stop_pct"] = float(stop_pct)
        trades["v10_g_2_target_pct"] = pd.to_numeric(
            trades["native_target_pct"], errors="coerce"
        )
        trades["v10_g_2_reward_risk"] = (
            trades["v10_g_2_target_pct"] / float(stop_pct)
        )
    ledger, summary = g2.g.v9.v6.apply_portfolio_constraints(
        trades, base.portfolio_config()
    )
    return trades, ledger, summary


def _apply_retained_g_exits(
    orders: pd.DataFrame, source_g: dict[str, Any]
) -> pd.DataFrame:
    work = orders.copy()
    exits = source_g["exit"]
    stop_map = {
        key: float(value["stop_pct"]) for key, value in exits["setups"].items()
    }
    target_map = {
        key: float(value["target_pct"]) for key, value in exits["setups"].items()
    }
    work["native_stop_pct"] = work["setup_id"].map(stop_map).fillna(
        float(exits["default"]["stop_pct"])
    )
    work["native_target_pct"] = work["setup_id"].map(target_map).fillna(
        float(exits["default"]["target_pct"])
    )
    return work


def _assert_daily_parity(observed: pd.DataFrame, official: pd.DataFrame, day: date) -> None:
    keys = ["sid", "setup_id", "tradingsymbol", "side"]
    fields = ["filled", "exit_reason", "portfolio_executed"]
    merged = official[keys + fields + ["portfolio_net_profit_rupees"]].merge(
        observed[keys + fields + ["portfolio_net_profit_rupees"]],
        on=keys,
        how="outer",
        suffixes=("_official", "_observed"),
        indicator=True,
        validate="one_to_one",
    )
    if not merged["_merge"].eq("both").all():
        raise RuntimeError(f"Daily G replay identity mismatch for {day}")
    for field in fields:
        left = merged[f"{field}_official"].astype(str).str.lower()
        right = merged[f"{field}_observed"].astype(str).str.lower()
        if not left.equals(right):
            raise RuntimeError(f"Daily G replay {field} mismatch for {day}")
    left = pd.to_numeric(merged["portfolio_net_profit_rupees_official"], errors="coerce")
    right = pd.to_numeric(merged["portfolio_net_profit_rupees_observed"], errors="coerce")
    if not np.allclose(left.fillna(0.0), right.fillna(0.0), atol=1e-6, rtol=0.0):
        raise RuntimeError(f"Daily G replay P&L mismatch for {day}")


def _load_daily_extensions(dataset: dict[str, Any], daily_root: Path) -> dict[str, Any]:
    g_frames: list[pd.DataFrame] = []
    g2_frames: list[pd.DataFrame] = []
    evidence = []
    for day in COMPLETE_EXTENSION_DAYS:
        run, result = _latest_successful_run(daily_root, day)
        source_manifest = _read_json(run / "source_manifest.json")
        expected_config_hash = g2.sha256(g2.DEFAULT_G_CONFIG)
        if source_manifest.get("frozen_config_sha256") != expected_config_hash:
            raise RuntimeError(f"Daily replay configuration mismatch for {day}")
        if source_manifest.get("source_fingerprint") != result.get("source_fingerprint"):
            raise RuntimeError(f"Daily replay source fingerprint mismatch for {day}")
        snapshot_manifest_path, snapshot_source_day = _snapshot_for_day(
            daily_root, day, run, result
        )
        snapshot_manifest = _read_json(snapshot_manifest_path)
        if snapshot_manifest.get("complete") is not True:
            raise RuntimeError(f"Daily replay snapshot is incomplete for {day}")
        daily_replay._verify_input_snapshot(snapshot_manifest_path.parent, snapshot_manifest)
        orders = _apply_retained_g_exits(
            pd.read_csv(run / "selected_orders.csv"), dataset["source_g"]
        )
        paths = _selected_paths(orders, day, snapshot_manifest_path.parent)
        g_trades, g_ledger, _ = _simulate(orders, paths, dataset["base"], stop_pct=None)
        official = pd.read_csv(run / "portfolio_trades.csv")
        _assert_daily_parity(g_ledger, official, day)
        g2_trades, _, _ = _simulate(orders, paths, dataset["base"], stop_pct=g2.STOP_PCT)
        for frame in (g_trades, g2_trades):
            frame["selection_evidence"] = "COMPLETE_OFFICIAL_DAILY_G_REPLAY"
            frame["execution_path_evidence"] = "HASH_VERIFIED_DAILY_INPUT_SNAPSHOT"
        g_frames.append(g_trades)
        g2_frames.append(g2_trades)
        evidence.append(
            {
                "session_date": day.isoformat(),
                "run": str(run.resolve()),
                "replay_result_sha256": g2.sha256(run / "replay_result.json"),
                "source_manifest_sha256": g2.sha256(run / "source_manifest.json"),
                "selected_orders_sha256": g2.sha256(run / "selected_orders.csv"),
                "snapshot_manifest": str(snapshot_manifest_path.resolve()),
                "snapshot_manifest_sha256": g2.sha256(snapshot_manifest_path),
                "snapshot_fingerprint": snapshot_manifest["snapshot_fingerprint"],
                "execution_path_snapshot_source_day": snapshot_source_day,
                "selection_count": int(len(orders)),
                "g_parity_verified": True,
            }
        )
    return {
        "g": pd.concat(g_frames, ignore_index=True, sort=False),
        "g2": pd.concat(g2_frames, ignore_index=True, sort=False),
        "evidence": evidence,
    }


def _yahoo_url() -> str:
    start = int(pd.Timestamp("2026-10-01 00:00", tz=common.IST).timestamp())
    end = int(pd.Timestamp("2026-10-02 00:00", tz=common.IST).timestamp())
    return (
        "https://query1.finance.yahoo.com/v8/finance/chart/MCX.NS"
        f"?period1={start}&period2={end}&interval=1m&includePrePost=false&events=history"
    )


def _download_yahoo(path: Path) -> str:
    url = _yahoo_url()
    request = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(request, timeout=30) as response:
        payload = response.read()
    parsed = json.loads(payload)
    error = parsed.get("chart", {}).get("error")
    if error:
        raise RuntimeError(f"Yahoo chart response error: {error}")
    path.write_bytes(payload)
    return url


def _yahoo_minutes(path: Path) -> pd.DataFrame:
    payload = _read_json(path)
    result = payload["chart"]["result"][0]
    quote = result["indicators"]["quote"][0]
    frame = pd.DataFrame(
        {
            "start": pd.to_datetime(result["timestamp"], unit="s", utc=True).tz_convert(
                common.IST
            ),
            "open": quote["open"],
            "high": quote["high"],
            "low": quote["low"],
            "close": quote["close"],
            "volume": quote["volume"],
        }
    ).dropna(subset=["open", "high", "low", "close"])
    frame["ts"] = frame["start"] + pd.Timedelta(minutes=1)
    cutoff = pd.Timestamp("2026-10-01 15:15", tz=common.IST)
    frame = frame.loc[frame.ts.dt.date.eq(OCT1) & frame.ts.le(cutoff)].copy()
    expected = pd.date_range(
        pd.Timestamp("2026-10-01 09:16", tz=common.IST), cutoff, freq="min"
    )
    missing = expected.difference(pd.DatetimeIndex(frame.ts))
    if len(missing):
        raise RuntimeError(f"Yahoo MCX path is missing {len(missing)} required minutes")
    return frame.sort_values("ts", kind="stable").reset_index(drop=True)


def _cross_validate_oct1(minutes: pd.DataFrame) -> dict[str, Any]:
    local = pd.read_parquet(
        OCT1_LOCAL_MCX_5M,
        columns=["date", "open", "high", "low", "close"],
    )
    local["date"] = hybrid._to_ist(local["date"])
    local = local.loc[
        local.date.dt.date.eq(OCT1)
        & local.date.between(
            pd.Timestamp("2026-10-01 09:55", tz=common.IST),
            pd.Timestamp("2026-10-01 10:50", tz=common.IST),
        )
    ].set_index("date")
    external = (
        minutes.set_index("ts")
        .resample("5min", closed="right", label="right")
        .agg({"open": "first", "high": "max", "low": "min", "close": "last"})
    )
    joined = local.join(external, lsuffix="_kite", rsuffix="_yahoo", how="inner")
    fields = ("open", "high", "low", "close")
    left = joined[[f"{field}_kite" for field in fields]].to_numpy(float)
    right = joined[[f"{field}_yahoo" for field in fields]].to_numpy(float)
    max_abs = float(np.max(np.abs(left - right))) if len(joined) else float("inf")
    if len(joined) != 12 or max_abs > 1.0:
        raise RuntimeError(
            f"October 1 external MCX path failed overlap validation: rows={len(joined)}, "
            f"max_abs_difference={max_abs}"
        )
    return {
        "overlap_bars": int(len(joined)),
        "overlap_window": "2026-10-01 09:55 through 10:50 IST",
        "fields": list(fields),
        "maximum_absolute_price_difference_rupees": max_abs,
        "tolerance_rupees": 1.0,
        "passed": True,
        "local_archive": str(OCT1_LOCAL_MCX_5M.resolve()),
        "local_archive_sha256": g2.sha256(OCT1_LOCAL_MCX_5M),
    }


def _oct1_partial(staging: Path, base: Any) -> dict[str, Any]:
    signal = _read_json(OCT1_SIGNAL)
    paper = _read_json(OCT1_PAPER_ORDER)
    expected_slots = sorted(config_slot.replace(":", "") for config_slot in daily_replay.config.SIGNAL_TO_CONFIRMATION)
    observed_slots = sorted(path.stem.removeprefix("slot_") for path in OCT1_SCANNER.glob("slot_*.json"))
    missing_slots = sorted(set(expected_slots) - set(observed_slots))
    if missing_slots != ["1120"]:
        raise RuntimeError(f"Unexpected October 1 scanner coverage: missing={missing_slots}")
    yahoo_path = staging / "oct1_yahoo_mcx_1m.json"
    url = _download_yahoo(yahoo_path)
    minutes = _yahoo_minutes(yahoo_path)
    validation = _cross_validate_oct1(minutes)
    confirmation = pd.Timestamp(signal["confirmation_timestamp"])
    selected = minutes.loc[minutes.ts.gt(confirmation)].copy()
    path = {
        "timestamp_ns": selected.ts.astype("int64").to_numpy(),
        **{field: selected[field].to_numpy(float) for field in ("open", "high", "low", "close")},
    }
    order = pd.DataFrame(
        [
            {
                "sid": 10_010_951,
                "day": signal["session_date"],
                "tradingsymbol": signal["tradingsymbol"],
                "instrument_token": signal["instrument_token"],
                "futures_tradingsymbol": signal["futures_tradingsymbol"],
                "futures_instrument_token": signal["futures_instrument_token"],
                "exchange": signal["exchange"],
                "side": signal["side"],
                "setup_id": signal["setup_id"],
                "picker": signal["picker"],
                "traded_value": signal["traded_value"],
                "volume_ratio": signal["volume_ratio"],
                "price_change_pct": signal["price_change_pct"],
                "abs_price_change_pct": abs(float(signal["price_change_pct"])),
                "confirmation_ts": signal["confirmation_timestamp"],
                "trigger": signal["trigger"],
                "native_stop_pct": float(signal["stop_pct"]),
                "native_target_pct": float(signal["target_pct"]),
                "signal_id": signal["signal_id"],
                "selection_evidence": "ARCHIVED_CAUSAL_LIVE_G_SIGNAL",
                "execution_path_evidence": "YAHOO_1M_CROSS_VALIDATED_TO_KITE_5M_OVERLAP",
            }
        ]
    )
    paths = {int(order.iloc[0].sid): path}
    g_trades, g_ledger, g_summary = _simulate(order, paths, base, stop_pct=None)
    candidate, g2_ledger, g2_summary = _simulate(order, paths, base, stop_pct=g2.STOP_PCT)
    observed = g_ledger.iloc[0]
    if not bool(observed["filled"]) or observed["exit_reason"] != "STOP":
        raise RuntimeError("External path does not reproduce the archived G stop outcome")
    if abs(float(observed["entry_price"]) - float(paper["entry_price"])) > 0.11:
        raise RuntimeError("External path does not reproduce the archived paper entry")
    candidate_row = g2_ledger.iloc[0]
    if candidate_row["exit_reason"] != "TARGET":
        raise RuntimeError("Expected the October 1 G-2 MCX trade to reach its retained target")
    stop_level = float(candidate_row["entry_price"]) * 1.01
    target_level = float(candidate_row["entry_price"]) * (1.0 - float(signal["target_pct"]) / 100.0)
    target_bar_low = float(
        selected.loc[selected.ts.eq(pd.Timestamp(candidate_row["exit_ts"])), "low"].iloc[0]
    )
    g_trades.to_csv(staging / "oct1_partial_g_selected_trade.csv", index=False)
    g_ledger.to_csv(staging / "oct1_partial_g_portfolio_trade.csv", index=False)
    candidate.to_csv(staging / "oct1_partial_g2_selected_trade.csv", index=False)
    g2_ledger.to_csv(staging / "oct1_partial_g2_portfolio_trade.csv", index=False)
    return {
        "state": "PARTIAL_NOT_INCLUDED_IN_COMPLETE_TOTALS",
        "selection_window_coverage": {
            "expected_slots": expected_slots,
            "observed_slots": observed_slots,
            "missing_slots": missing_slots,
            "complete": False,
        },
        "known_selection": {
            "signal_id": signal["signal_id"],
            "symbol": signal["tradingsymbol"],
            "side": signal["side"],
            "setup_id": signal["setup_id"],
            "trigger": float(signal["trigger"]),
            "g_stop_pct": float(signal["stop_pct"]),
            "g2_stop_pct": g2.STOP_PCT,
            "target_pct": float(signal["target_pct"]),
            "g_exit": str(g_ledger.iloc[0]["exit_reason"]),
            "g_net_pnl_rupees": float(g_ledger.iloc[0]["portfolio_net_profit_rupees"]),
            "g2_entry_ts": str(candidate_row["entry_ts"]),
            "g2_exit_ts": str(candidate_row["exit_ts"]),
            "g2_exit": str(candidate_row["exit_reason"]),
            "g2_net_pnl_rupees": float(candidate_row["portfolio_net_profit_rupees"]),
            "g2_stop_level": stop_level,
            "g2_target_level": target_level,
            "target_bar_low": target_bar_low,
            "target_cross_margin_rupees": target_level - target_bar_low,
        },
        "g_summary": g_summary,
        "g2_summary": g2_summary,
        "signal_path": str(OCT1_SIGNAL.resolve()),
        "signal_sha256": g2.sha256(OCT1_SIGNAL),
        "paper_order_path": str(OCT1_PAPER_ORDER.resolve()),
        "paper_order_sha256": g2.sha256(OCT1_PAPER_ORDER),
        "yahoo_url": url,
        "yahoo_response_sha256": g2.sha256(yahoo_path),
        "cross_validation": validation,
    }


def _metric_rows(ledger: pd.DataFrame, days: list[date]) -> dict[str, Any]:
    metric = g2.g.r.metric(ledger, days)
    executed = ledger.loc[ledger.portfolio_executed.eq(True)]
    metric.update(
        gross_pnl_rupees=float(
            pd.to_numeric(executed.portfolio_gross_profit_rupees, errors="coerce").fillna(0.0).sum()
        ),
        costs_rupees=float(
            pd.to_numeric(executed.portfolio_cost_rupees, errors="coerce").fillna(0.0).sum()
        ),
        average_trade_rupees=float(
            pd.to_numeric(executed.portfolio_net_profit_rupees, errors="coerce").mean()
        ) if len(executed) else 0.0,
    )
    return metric


def _trade_details(ledger: pd.DataFrame) -> pd.DataFrame:
    frame = ledger.copy()
    entry = pd.to_numeric(frame.get("entry_price"), errors="coerce")
    stop_pct = pd.to_numeric(frame.get("native_stop_pct"), errors="coerce")
    target_pct = pd.to_numeric(frame.get("native_target_pct"), errors="coerce")
    is_long = frame.side.eq("LONG")
    frame["planned_stop_price"] = np.where(
        is_long, entry * (1.0 - stop_pct / 100.0), entry * (1.0 + stop_pct / 100.0)
    )
    frame["planned_target_price"] = np.where(
        is_long, entry * (1.0 + target_pct / 100.0), entry * (1.0 - target_pct / 100.0)
    )
    columns = [
        "day", "tradingsymbol", "side", "setup_id", "filled", "portfolio_executed",
        "confirmation_ts", "trigger", "entry_ts", "entry_price", "native_stop_pct",
        "planned_stop_price", "native_target_pct", "planned_target_price", "exit_ts",
        "exit_price", "exit_reason", "holding_minutes", "gross_return_pct", "cost_pct",
        "net_return_pct", "portfolio_gross_profit_rupees", "portfolio_cost_rupees",
        "portfolio_net_profit_rupees", "mfe_pct", "mae_pct", "same_bar_ambiguous",
        "selection_evidence", "execution_path_evidence",
    ]
    for column in columns:
        if column not in frame:
            frame[column] = np.nan
    return frame[columns].sort_values(
        ["day", "confirmation_ts", "tradingsymbol"], kind="stable"
    )


def _money(value: float) -> str:
    sign = "-" if value < 0 else ""
    return f"{sign}Rs {abs(value):,.2f}"


def _pf(value: Any) -> str:
    if value is None or not np.isfinite(float(value)):
        return "N/A"
    return f"{float(value):.4f}"


def _build_report(
    staging: Path,
    days: list[date],
    g_ledger: pd.DataFrame,
    g2_ledger: pd.DataFrame,
    g_summary: dict[str, Any],
    g2_summary: dict[str, Any],
    oct1: dict[str, Any],
) -> Path:
    g_metric = _metric_rows(g_ledger, days)
    g2_metric = _metric_rows(g2_ledger, days)
    breakdowns = g2.build_breakdowns(g2_ledger, days)
    for name, frame in breakdowns.items():
        frame.to_csv(staging / f"{name}_results.csv", index=False)
    detail = _trade_details(g2_ledger)
    detail.to_csv(staging / "daywise_trade_details.csv", index=False)
    compare = pd.DataFrame([
        {"strategy": "V13-v10-G", **g_metric},
        {"strategy": VERSION, **g2_metric},
    ])
    compare["net_delta_vs_g_rupees"] = (
        compare.net_profit_rupees - float(g_metric["net_profit_rupees"])
    )
    compare.to_csv(staging / "g_vs_g2_comparison.csv", index=False)
    daily = breakdowns["daily"]
    known = oct1["known_selection"]
    provisional_g2_net = float(g2_metric["net_profit_rupees"]) + float(known["g2_net_pnl_rupees"])
    lines = [
        "# V13-v10-G-2 backtest requested through 2026-10-01",
        "",
        f"Generated: {datetime.now(timezone.utc).isoformat()}",
        "",
        "**Publication status: complete and hash-verified through 2026-09-30; "
        "October 1 is partial and is excluded from the official totals.**",
        "",
        "October 1 has archived selection evidence for every configured scan through 10:00, "
        "but the data producer stopped before the final 11:20 signal slot. The known 09:51 "
        "MCX trade is reported separately; treating the missing slot as a zero-trade result "
        "would be an unsupported assumption.",
        "",
        "## Complete headline results through September 30",
        "",
        "| Metric | V13-v10-G | V13-v10-G-2 (1% SL) |",
        "|---|---:|---:|",
        f"| Sessions | {len(days)} | {len(days)} |",
        f"| Selected orders | {g_metric['selected_orders']} | {g2_metric['selected_orders']} |",
        f"| Executed trades | {g_metric['trades']} | {g2_metric['trades']} |",
        f"| Wins | {g_metric['wins']} | {g2_metric['wins']} |",
        f"| Losses | {g_metric['losses']} | {g2_metric['losses']} |",
        f"| Win rate | {g_metric['win_rate_pct']:.2f}% | {g2_metric['win_rate_pct']:.2f}% |",
        f"| Profit factor | {_pf(g_metric['profit_factor'])} | {_pf(g2_metric['profit_factor'])} |",
        f"| Gross P&L | {_money(g_metric['gross_pnl_rupees'])} | {_money(g2_metric['gross_pnl_rupees'])} |",
        f"| Modeled costs | {_money(g_metric['costs_rupees'])} | {_money(g2_metric['costs_rupees'])} |",
        f"| Net P&L | {_money(g_metric['net_profit_rupees'])} | {_money(g2_metric['net_profit_rupees'])} |",
        f"| Net delta vs G | {_money(0.0)} | {_money(g2_metric['net_profit_rupees'] - g_metric['net_profit_rupees'])} |",
        f"| Daily-close max drawdown | {_money(g_metric['daily_close_drawdown_rupees'])} | {_money(g2_metric['daily_close_drawdown_rupees'])} |",
        f"| Median trade | {_money(g_metric['median_trade_rupees'])} | {_money(g2_metric['median_trade_rupees'])} |",
        f"| Average trade | {_money(g_metric['average_trade_rupees'])} | {_money(g2_metric['average_trade_rupees'])} |",
        f"| Peak concurrent positions | {g_summary['peak_concurrent_positions']} | {g2_summary['peak_concurrent_positions']} |",
        f"| Peak open initial risk | {_money(float(g_summary['peak_open_initial_risk_rupees']))} | {_money(float(g2_summary['peak_open_initial_risk_rupees']))} |",
        "",
        "## Daywise complete results",
        "",
        "| Date | Selected | Trades | W-L | Gross P&L | Costs | Net P&L | Cumulative | Drawdown |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in daily.itertuples(index=False):
        lines.append(
            f"| {pd.Timestamp(row.day).date()} | {row.selected_orders} | {row.trades} | "
            f"{row.wins}-{row.losses} | {_money(row.gross_pnl_rupees)} | "
            f"{_money(row.cost_rupees)} | {_money(row.net_pnl_rupees)} | "
            f"{_money(row.cumulative_net_pnl_rupees)} | {_money(row.drawdown_rupees)} |"
        )
    lines.extend([
        "",
        "## October 1 partial diagnostic (not in totals)",
        "",
        "| Field | Result |",
        "|---|---:|",
        f"| Observed selection slots | {len(oct1['selection_window_coverage']['observed_slots'])}/9 |",
        "| Missing selection slot | 11:20 signal / 11:21 confirmation |",
        f"| Known trade | {known['setup_id']} {known['side']} {known['symbol']} |",
        f"| Entry | {_money(known['trigger'])} at {known['g2_entry_ts']} |",
        f"| Original G result | {known['g_exit']}, {_money(known['g_net_pnl_rupees'])} |",
        f"| G-2 1% SL | {_money(known['g2_stop_level'])} |",
        f"| Unchanged target | {_money(known['g2_target_level'])} |",
        f"| G-2 result | {known['g2_exit']} at {known['g2_exit_ts']}, {_money(known['g2_net_pnl_rupees'])} |",
        f"| Provisional G-2 net if this were the only Oct 1 order | {_money(provisional_g2_net)} |",
        "",
        "The external MCX minute path matched 12 overlapping archived Kite 5-minute bars "
        f"within Rs {oct1['cross_validation']['maximum_absolute_price_difference_rupees']:.2f}; "
        f"the target-crossing bar moved Rs {known['target_cross_margin_rupees']:.2f} beyond "
        "the target, so the known trade's target classification is not a boundary/tick ambiguity.",
        "",
        "## Assumptions",
        "",
        "- Every active setup stop is exactly 1.00%; G targets are unchanged.",
        "- Rs 1,00,000 capital per filled trade, 5x modeled exposure, Rs 10,00,000 portfolio capital.",
        "- Flat 5 bps round-trip cost, 10-minute entry expiry, 15:15 IST square-off.",
        "- Full exits only; no partial exit or breakeven stop.",
        "- The complete totals use only the immutable base bundle and five complete daily input snapshots.",
        "- The October 1 MCX diagnostic uses the archived causal G signal and a cross-validated external execution path.",
        "- October 1 cannot become an official complete session until the missing 11:20 futures-OI selection input is supplied.",
        "- This is retrospective research and does not change live or paper configuration.",
        "",
        "## Detailed artifacts",
        "",
        "- `daily_results.csv`: every complete session, including zero-selection days.",
        "- `daywise_trade_details.csv`: every selected order with entry, 1% stop, target, exit, excursion, cost, and P&L.",
        "- `portfolio_trades.csv` and `selected_trades.csv`: complete G-2 ledgers through September 30.",
        "- `g_control_portfolio_trades.csv`: matching retained-G control ledger.",
        "- `oct1_partial_g2_portfolio_trade.csv`: known October 1 MCX diagnostic.",
        "- `oct1_completeness.json`: exact missing-slot evidence and data hashes.",
        "- `provenance.json`: input/output hashes.",
        "",
    ])
    report = staging / "V13_V10_G_2_THROUGH_OCT1_DAYWISE.md"
    report.write_text("\n".join(lines), encoding="utf-8")

    detail_lines = [
        "# V13-v10-G-2 detailed daywise trade ledger (complete through 2026-09-30)",
        "",
        "Unfilled selections are retained so each day's selection and execution path is auditable.",
    ]
    for day_value, group in detail.groupby("day", sort=True):
        detail_lines.extend([
            "",
            f"## {pd.Timestamp(day_value).date()}",
            "",
            "| Symbol | Side | Setup | Filled | Entry time | Entry | 1% stop | Target % | Target | Exit time | Exit | Reason | Net P&L | MFE % | MAE % |",
            "|---|---|---|---:|---|---:|---:|---:|---:|---|---:|---|---:|---:|---:|",
        ])
        for row in group.itertuples(index=False):
            filled = str(row.filled).lower() == "true" or row.filled is True
            def num(value: Any) -> str:
                return "-" if pd.isna(value) else f"{float(value):.2f}"
            detail_lines.append(
                f"| {row.tradingsymbol} | {row.side} | {row.setup_id} | "
                f"{'yes' if filled else 'no'} | {row.entry_ts if filled else '-'} | "
                f"{num(row.entry_price)} | {num(row.planned_stop_price)} | "
                f"{num(row.native_target_pct)} | {num(row.planned_target_price)} | "
                f"{row.exit_ts if filled else '-'} | {num(row.exit_price)} | {row.exit_reason} | "
                f"{_money(float(row.portfolio_net_profit_rupees))} | {num(row.mfe_pct)} | {num(row.mae_pct)} |"
            )
    (staging / "V13_V10_G_2_DAYWISE_TRADE_DETAIL.md").write_text(
        "\n".join(detail_lines) + "\n", encoding="utf-8"
    )
    return report


def run(base_g2: Path, daily_root: Path, output: Path) -> Path:
    if output.exists():
        raise FileExistsError(f"Refusing to overwrite existing output: {output}")
    output.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=f".{output.name}.", dir=output.parent))
    try:
        base_g2_root = base_g2.resolve()
        dataset = _load_base(base_g2_root)
        extension = _load_daily_extensions(dataset, daily_root.resolve())
        complete_days = [*dataset["days"], *COMPLETE_EXTENSION_DAYS]
        base_g = dataset["g_trades"].copy()
        base_g2_trades = dataset["g2_trades"].copy()
        for frame in (base_g, base_g2_trades):
            frame["selection_evidence"] = "IMMUTABLE_CORRECTED_FULL_HISTORY_BUNDLE"
            frame["execution_path_evidence"] = "HASH_VERIFIED_BUNDLE_PATHS"
        all_g = pd.concat([base_g, extension["g"]], ignore_index=True, sort=False)
        all_g2 = pd.concat([base_g2_trades, extension["g2"]], ignore_index=True, sort=False)
        g_ledger, g_summary = g2.g.v9.v6.apply_portfolio_constraints(
            all_g, dataset["base"].portfolio_config()
        )
        g2_ledger, g2_summary = g2.g.v9.v6.apply_portfolio_constraints(
            all_g2, dataset["base"].portfolio_config()
        )
        if len(g_ledger) != len(g2_ledger):
            raise RuntimeError("Complete G/G-2 selection count mismatch")
        g2.g.r.save(staging, all_g2, g2_ledger, g2_summary)
        all_g.to_csv(staging / "g_control_selected_trades.csv", index=False)
        g_ledger.to_csv(staging / "g_control_portfolio_trades.csv", index=False)
        _write_json(staging / "g_control_summary.json", _finite_json(g_summary))
        _write_json(staging / "frozen_config.json", dataset["settings"])
        oct1 = _oct1_partial(staging, dataset["base"])
        _write_json(staging / "oct1_completeness.json", _finite_json(oct1))
        report = _build_report(
            staging,
            complete_days,
            g_ledger,
            g2_ledger,
            g_summary,
            g2_summary,
            oct1,
        )
        artifacts = sorted(
            path for path in staging.iterdir() if path.is_file() and path.name != "provenance.json"
        )
        provenance = {
            "schema_version": "V13_V10_G_2_EXTENDED_RESULT_V1",
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "strategy": VERSION,
            "publication_status": "COMPLETE_THROUGH_2026-09-30_OCT1_PARTIAL",
            "complete_window": [str(complete_days[0]), str(complete_days[-1])],
            "complete_sessions": len(complete_days),
            "requested_through": str(OCT1),
            "source_bundle": str(dataset["source"]),
            "source_bundle_manifest_sha256": g2.sha256(dataset["source"] / "bundle_manifest.json"),
            "base_g2_output": str(base_g2_root),
            "base_g2_provenance_sha256": g2.sha256(base_g2_root / "provenance.json"),
            "daily_extensions": extension["evidence"],
            "oct1": oct1,
            "code": str(Path(__file__).resolve()),
            "code_sha256": g2.sha256(Path(__file__).resolve()),
            "live_configuration_changed": False,
            "execution_authority": False,
            "artifacts": {
                path.name: {"bytes": path.stat().st_size, "sha256": g2.sha256(path)}
                for path in artifacts
            },
        }
        _write_json(staging / "provenance.json", _finite_json(provenance))
        os.replace(staging, output)
        return output / report.name
    except Exception:
        shutil.rmtree(staging, ignore_errors=True)
        raise


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-g2", type=Path, default=DEFAULT_BASE_G2)
    parser.add_argument("--daily-root", type=Path, default=DEFAULT_DAILY_ROOT)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()
    report = run(args.base_g2, args.daily_root, args.output_dir)
    summary = _read_json(args.output_dir / "summary.json")
    oct1 = _read_json(args.output_dir / "oct1_completeness.json")
    print(json.dumps({
        "publication_status": "COMPLETE_THROUGH_2026-09-30_OCT1_PARTIAL",
        "complete_summary": summary,
        "oct1_known_selection": oct1["known_selection"],
        "report": str(report),
        "output_dir": str(args.output_dir.resolve()),
    }, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
