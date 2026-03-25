from __future__ import annotations

import argparse
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

import eqidv2_live_combined_analyser_csv_v15 as base_v15
import eqidv2_live_combined_analyser_csv_v15_long as long_v15
import eqidv2_live_combined_analyser_csv_v15_short as short_v15
import avwap_combined_runner_v15 as v15_runner
from eqidv2_runtime_paths import RUNTIME_ROOT
from live_v15_slot_snapshot_v15_new import (
    normalize_slot_ist,
    read_shard_snapshot,
    slot_context_path,
    slot_key,
)


OUTPUT_SUBDIR = "live_v15_new_scan"
DEFAULT_SCAN_WORKERS = int(os.getenv("EQIDV15_NEW_SCAN_MAX_WORKERS", "1"))
_V15_NEW_ENGINE_READY = False


def _ensure_v15_new_engine_overrides() -> None:
    global _V15_NEW_ENGINE_READY
    if _V15_NEW_ENGINE_READY:
        return
    apply_strategy = getattr(short_v15, "_apply_v15_strategy_engine_overrides", None)
    if callable(apply_strategy):
        apply_strategy()
    # Match the live v15 wrappers' runtime signal model so v15_new parity
    # tracks the production short/long CSV behavior, not the older base defaults.
    base_v15.USE_KITE_LTP_FOR_SIGNAL_CSV = False
    base_v15.SHORT_STOP_PCT = float(short_v15.SHORT_STOP_PCT)
    base_v15.SHORT_TARGET_PCT = float(short_v15.SHORT_TARGET_PCT)
    base_v15.LONG_STOP_PCT = float(long_v15.LONG_STOP_PCT)
    base_v15.LONG_TARGET_PCT = float(long_v15.LONG_TARGET_PCT)
    base_v15.END_TIME = base_v15.dtime(15, 0)
    base_v15.SESSION_END = base_v15.dtime(15, 0)
    _V15_NEW_ENGINE_READY = True


def output_root(runtime_root: Optional[Path | str] = None) -> Path:
    root = Path(runtime_root) if runtime_root is not None else Path(RUNTIME_ROOT)
    return root / OUTPUT_SUBDIR


def output_slot_dir(slot_ts: Any, runtime_root: Optional[Path | str] = None) -> Path:
    out_dir = output_root(runtime_root) / slot_key(slot_ts)
    out_dir.mkdir(parents=True, exist_ok=True)
    return out_dir


def shard_output_dir(
    slot_ts: Any,
    shard_id: int | str,
    runtime_root: Optional[Path | str] = None,
) -> Path:
    out_dir = output_slot_dir(slot_ts, runtime_root) / f"shard_{int(str(shard_id).strip()):02d}"
    out_dir.mkdir(parents=True, exist_ok=True)
    return out_dir


def _signal_to_row(signal: Any) -> Dict[str, Any]:
    diagnostics = dict(getattr(signal, "diagnostics", {}) or {})
    return {
        "ticker": str(getattr(signal, "ticker", "")).upper(),
        "side": str(getattr(signal, "side", "")).upper(),
        "bar_time_ist": pd.Timestamp(getattr(signal, "bar_time_ist", pd.NaT)),
        "setup": str(getattr(signal, "setup", "")),
        "entry_price": float(getattr(signal, "entry_price", 0.0) or 0.0),
        "sl_price": float(getattr(signal, "sl_price", 0.0) or 0.0),
        "target_price": float(getattr(signal, "target_price", 0.0) or 0.0),
        "score": float(getattr(signal, "score", 0.0) or 0.0),
        "diagnostics_json": json.dumps(diagnostics, default=str),
    }


def _state_subset_for_ticker(state: Dict[str, Any], ticker: str) -> Dict[str, Any]:
    ticker_key_prefix = f"{str(ticker).strip().upper()}|"
    out = {"count": {}, "last_signal": {}}
    for day_key, day_counts in dict(state.get("count", {}) or {}).items():
        subset = {
            str(key): int(val)
            for key, val in dict(day_counts or {}).items()
            if str(key).upper().startswith(ticker_key_prefix)
        }
        if subset:
            out["count"][str(day_key)] = subset
    for key, val in dict(state.get("last_signal", {}) or {}).items():
        if str(key).upper().startswith(ticker_key_prefix):
            out["last_signal"][str(key)] = val
    return out


def _scan_one_ticker(
    ticker: str,
    df_ticker: pd.DataFrame,
    slot_ts: pd.Timestamp,
    state_subset: Optional[Dict[str, Any]] = None,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    local_state = state_subset if state_subset is not None else {"count": {}, "last_signal": {}}
    signals, checks = base_v15._latest_entry_signals_for_ticker(
        ticker=str(ticker).upper(),
        df_raw=df_ticker,
        state=local_state,
        target_slot_ist=slot_ts,
    )
    signal_rows = [_signal_to_row(sig) for sig in (signals or [])]
    return list(checks or []), signal_rows


def _load_slot_context_payload(slot_ts: pd.Timestamp, runtime_root: Optional[Path | str]) -> Dict[str, Any]:
    path = slot_context_path(slot_ts, runtime_root)
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _apply_slot_context_payload(slot_payload: Dict[str, Any], slot_ts: pd.Timestamp) -> bool:
    if not slot_payload:
        return False

    nifty_payload = dict(slot_payload.get("nifty_context", {}) or {})
    regime_payload = dict(slot_payload.get("market_regime", {}) or {})

    if not nifty_payload and not regime_payload:
        return False

    short_cfg = short_v15._build_effective_v15_short_cfg()

    mode_map = {str(k): str(v) for k, v in dict(nifty_payload.get("mode_map", {}) or {}).items()}
    ret_map = {
        str(k): float(v)
        for k, v in dict(nifty_payload.get("ret_map", {}) or {}).items()
    }
    nifty_source = str(nifty_payload.get("source", "") or "")

    short_v15._NIFTY_CONTEXT_MODE_MAP = dict(mode_map)
    short_v15._NIFTY_CONTEXT_RET_MAP = dict(ret_map)
    short_v15._NIFTY_CONTEXT_SOURCE = nifty_source
    short_v15._NIFTY_CONTEXT_CFG = short_cfg
    short_v15._NIFTY_STOCK_RET_CACHE = {}

    long_v15._NIFTY_CONTEXT_MODE_MAP = dict(mode_map)
    long_v15._NIFTY_CONTEXT_RET_MAP = dict(ret_map)
    long_v15._NIFTY_CONTEXT_SOURCE = nifty_source
    long_v15._NIFTY_CONTEXT_CFG = short_cfg
    long_v15._NIFTY_STOCK_RET_CACHE = {}

    regime_map = {
        str(k): int(v)
        for k, v in dict(regime_payload.get("map", {}) or {}).items()
    }
    regime_source = str(regime_payload.get("source", "") or "")
    if regime_map:
        base_v15._seed_cached_market_regime_map(
            short_cfg,
            target_slot_ist=slot_ts,
            regime_map=regime_map,
            regime_source=regime_source,
            runner_cfg=v15_runner,
        )
    return True


def _write_scan_outputs(
    slot_ts: pd.Timestamp,
    shard_id: int,
    runtime_root: Optional[Path | str],
    checks_df: pd.DataFrame,
    short_signals_df: pd.DataFrame,
    long_signals_df: pd.DataFrame,
    summary: Dict[str, Any],
) -> None:
    out_dir = shard_output_dir(slot_ts, shard_id, runtime_root)
    checks_df.to_parquet(out_dir / "checks.parquet", index=False)
    short_signals_df.to_parquet(out_dir / "short_signals.parquet", index=False)
    long_signals_df.to_parquet(out_dir / "long_signals.parquet", index=False)
    (out_dir / "summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")


def scan_combined_shard_once(
    slot_ts: Any,
    *,
    shard_id: int,
    runtime_root: Optional[Path | str] = None,
    scan_workers: int = DEFAULT_SCAN_WORKERS,
    use_state: bool = False,
    write_outputs: bool = True,
) -> Dict[str, Any]:
    _ensure_v15_new_engine_overrides()
    slot_ist = normalize_slot_ist(slot_ts)
    snapshot_df = read_shard_snapshot(slot_ist, shard_id, runtime_root=runtime_root)
    assigned_tickers = sorted({str(t).strip().upper() for t in snapshot_df.get("ticker", pd.Series(dtype=str)).tolist()})
    slot_payload = _load_slot_context_payload(slot_ist, runtime_root)
    context_seeded = _apply_slot_context_payload(slot_payload, slot_ist)
    if not context_seeded:
        short_v15._refresh_v15_nifty_context()
        long_v15._refresh_v15_nifty_context()

    started = time.perf_counter()
    raw_checks: List[Dict[str, Any]] = []
    raw_signal_rows: List[Dict[str, Any]] = []
    full_state = base_v15._load_state() if use_state else {"count": {}, "last_signal": {}}

    ticker_groups: List[Tuple[str, pd.DataFrame]] = []
    if snapshot_df is not None and not snapshot_df.empty and "ticker" in snapshot_df.columns:
        for ticker, grp in snapshot_df.groupby("ticker", sort=True):
            ticker_groups.append((str(ticker).strip().upper(), grp.reset_index(drop=True)))

    workers = max(1, int(scan_workers))
    if workers > 1 and len(ticker_groups) > 1:
        with ThreadPoolExecutor(max_workers=min(workers, len(ticker_groups))) as executor:
            futures = {
                executor.submit(
                    _scan_one_ticker,
                    ticker,
                    df_ticker,
                    slot_ist,
                    _state_subset_for_ticker(full_state, ticker) if use_state else None,
                ): ticker
                for ticker, df_ticker in ticker_groups
            }
            for future in as_completed(futures):
                checks_rows, signal_rows = future.result()
                raw_checks.extend(checks_rows)
                raw_signal_rows.extend(signal_rows)
                if use_state and signal_rows:
                    base_v15._persist_signal_rows_to_state(full_state, signal_rows)
    else:
        for ticker, df_ticker in ticker_groups:
            checks_rows, signal_rows = _scan_one_ticker(
                ticker,
                df_ticker,
                slot_ist,
                _state_subset_for_ticker(full_state, ticker) if use_state else None,
            )
            raw_checks.extend(checks_rows)
            raw_signal_rows.extend(signal_rows)
            if use_state and signal_rows:
                base_v15._persist_signal_rows_to_state(full_state, signal_rows)

    if use_state:
        base_v15._save_state(full_state)

    checks_df = pd.DataFrame(raw_checks)
    signals_df = pd.DataFrame(raw_signal_rows)

    short_checks_df = short_v15._filter_short_checks_df(checks_df)
    long_checks_df = long_v15._filter_long_checks_df(checks_df)
    short_signals_df = short_v15._filter_short_signals_post(signals_df)
    long_signals_df = long_v15._filter_long_signals_post(signals_df)

    elapsed = time.perf_counter() - started
    summary = {
        "slot_ist": slot_ist.strftime("%Y-%m-%d %H:%M:%S%z"),
        "slot_key": slot_key(slot_ist),
        "shard_id": f"{int(shard_id):02d}",
        "scan_workers": int(workers),
        "use_state": bool(use_state),
        "slot_context_seeded": bool(context_seeded),
        "assigned_tickers": int(len(assigned_tickers)),
        "snapshot_rows": int(0 if snapshot_df is None else len(snapshot_df)),
        "raw_checks_rows": int(len(checks_df)),
        "raw_signals_rows": int(len(signals_df)),
        "short_checks_rows": int(len(short_checks_df)),
        "long_checks_rows": int(len(long_checks_df)),
        "short_signals_rows": int(len(short_signals_df)),
        "long_signals_rows": int(len(long_signals_df)),
        "elapsed_sec": round(elapsed, 3),
    }

    if write_outputs:
        _write_scan_outputs(
            slot_ist,
            int(shard_id),
            runtime_root,
            checks_df,
            short_signals_df,
            long_signals_df,
            summary,
        )
    return summary


def _parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Run a combined short+long v15_new shard scan from a slot snapshot.")
    ap.add_argument("--slot", required=True, help="Slot timestamp in IST, e.g. 2026-03-23 09:30:00+05:30")
    ap.add_argument("--shard-id", type=int, required=True)
    ap.add_argument("--runtime-root", default=str(RUNTIME_ROOT))
    ap.add_argument("--scan-workers", type=int, default=DEFAULT_SCAN_WORKERS)
    ap.add_argument("--use-state", action="store_true")
    ap.add_argument("--no-write-outputs", action="store_true")
    return ap.parse_args()


def main() -> None:
    args = _parse_args()
    summary = scan_combined_shard_once(
        args.slot,
        shard_id=int(args.shard_id),
        runtime_root=args.runtime_root,
        scan_workers=int(args.scan_workers),
        use_state=bool(args.use_state),
        write_outputs=not bool(args.no_write_outputs),
    )
    print(json.dumps(summary, indent=2, sort_keys=True), flush=True)


if __name__ == "__main__":
    main()
