from __future__ import annotations

import argparse
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

import eqidv2_live_combined_analyser_csv_v15 as base_v15
import avwap_combined_runner_v16_5min as v16_runner
from avwap_combined_runner_v16_5min import (
    NIFTY_CONTEXT_MIN_DAYMOVE_PCT,
    NIFTY_RS_BOTH_MODE_THRESHOLD_LONG_PCT,
    NIFTY_RS_BOTH_MODE_THRESHOLD_SHORT_PCT,
    NIFTY_RS_LOOKBACK_BARS,
)
from avwap_v11_refactored.avwap_common_v11_v15 import default_short_config, list_tickers_15m
from eqidv2_runtime_paths import DATA_5M_DIR, RUNTIME_ROOT


SNAPSHOT_SUBDIR = "live_v16_5min_snapshots"
DEFAULT_TAIL_ROWS = int(os.getenv("EQIDV16_5MIN_TAIL_ROWS", "260"))
DEFAULT_SNAPSHOT_MAX_WORKERS = int(os.getenv("EQIDV16_5MIN_SNAPSHOT_MAX_WORKERS", "8"))
DEFAULT_INCREMENTAL_REFRESH_ROWS = int(os.getenv("EQIDV16_5MIN_INCREMENTAL_REFRESH_ROWS", "8"))
DEFAULT_USE_ROLLING_CACHE = str(os.getenv("EQIDV16_5MIN_USE_ROLLING_CACHE", "1")).strip().lower() not in {
    "0",
    "false",
    "no",
    "off",
}
DEFAULT_BUILD_SLOT_CONTEXT = str(os.getenv("EQIDV16_5MIN_BUILD_SLOT_CONTEXT", "1")).strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}
DEFAULT_NEUTRALIZE_WEAK_NIFTY_CONTEXT = str(
    os.getenv("EQIDV16_5MIN_NEUTRALIZE_WEAK_NIFTY_CONTEXT", "1")
).strip().lower() not in {
    "0",
    "false",
    "no",
    "off",
}
DEFAULT_DIRECTIONAL_NIFTY_CONTEXT_FALLBACK = str(
    os.getenv("EQIDV16_5MIN_DIRECTIONAL_NIFTY_CONTEXT_FALLBACK", "1")
).strip().lower() not in {
    "0",
    "false",
    "no",
    "off",
}
DEFAULT_NEUTRALIZE_PARTIAL_NIFTY_SESSION = str(
    os.getenv("EQIDV16_5MIN_NEUTRALIZE_PARTIAL_NIFTY_SESSION", "1")
).strip().lower() not in {
    "0",
    "false",
    "no",
    "off",
}

IST = base_v15.IST
END_5M = "_stocks_indicators_5min.parquet"
NIFTYBEES_TICKER = "NIFTYBEES"


@dataclass
class _RollingShardCache:
    cache_key: Tuple[str, int, int]
    source_root: Path
    shard_count: int
    tail_rows: int
    created_slot_key: str
    last_slot_key: str
    source_max_slot_key: str
    warm_elapsed_sec: float
    refresh_elapsed_sec: float
    refresh_rows_added: int
    ticker_count: int
    shard_full_frames: Dict[str, pd.DataFrame]
    rows_by_shard: Dict[str, int]
    tickers_by_shard: Dict[str, int]
    ticker_to_shard: Dict[str, str]
    last_seen_by_ticker: Dict[str, pd.Timestamp]
    errored_tickers: List[str]


_ROLLING_SHARD_CACHE: Dict[Tuple[str, int, int], _RollingShardCache] = {}


def normalize_slot_ist(slot_ts: Any) -> pd.Timestamp:
    ts = pd.Timestamp(slot_ts)
    if ts.tzinfo is None:
        ts = ts.tz_localize(IST)
    else:
        ts = ts.tz_convert(IST)
    return ts.floor("min")


def slot_key(slot_ts: Any) -> str:
    return normalize_slot_ist(slot_ts).strftime("%Y%m%d_%H%M")


def snapshot_root(runtime_root: Optional[Path | str] = None) -> Path:
    root = Path(runtime_root) if runtime_root is not None else Path(RUNTIME_ROOT)
    out_dir = root / SNAPSHOT_SUBDIR
    out_dir.mkdir(parents=True, exist_ok=True)
    return out_dir


def snapshot_slot_dir(slot_ts: Any, runtime_root: Optional[Path | str] = None) -> Path:
    out_dir = snapshot_root(runtime_root) / slot_key(slot_ts)
    out_dir.mkdir(parents=True, exist_ok=True)
    return out_dir


def snapshot_meta_path(slot_ts: Any, runtime_root: Optional[Path | str] = None) -> Path:
    return snapshot_slot_dir(slot_ts, runtime_root) / "meta.json"


def snapshot_parquet_path(
    slot_ts: Any,
    shard_id: int | str,
    runtime_root: Optional[Path | str] = None,
) -> Path:
    shard_num = int(str(shard_id).strip())
    return snapshot_slot_dir(slot_ts, runtime_root) / f"snapshot_shard_{shard_num:02d}.parquet"


def slot_context_path(slot_ts: Any, runtime_root: Optional[Path | str] = None) -> Path:
    return snapshot_slot_dir(slot_ts, runtime_root) / "slot_context.json"


def load_tickers_5m(source_dir: Optional[Path | str] = None) -> List[str]:
    root = str(Path(source_dir) if source_dir is not None else Path(DATA_5M_DIR))
    return list_tickers_15m(root, END_5M)


def clear_rolling_cache() -> None:
    _ROLLING_SHARD_CACHE.clear()


def _split_tickers_for_shards(tickers: List[str], shard_count: int) -> Dict[str, List[str]]:
    ordered = sorted({str(t).strip().upper() for t in tickers if str(t).strip()})
    shard_count = max(1, int(shard_count))
    base_size, remainder = divmod(len(ordered), shard_count)
    out: Dict[str, List[str]] = {}
    start = 0
    for idx in range(shard_count):
        size = base_size + (1 if idx < remainder else 0)
        end = start + size
        out[f"{idx + 1:02d}"] = ordered[start:end]
        start = end
    return out


def _read_one_ticker_tail(
    ticker: str,
    source_dir: Path,
    tail_rows: int,
) -> Tuple[str, pd.DataFrame, Dict[str, Any]]:
    started = time.perf_counter()
    ticker_name = str(ticker).strip().upper()
    path = source_dir / f"{ticker_name}{END_5M}"
    meta = {
        "ticker": ticker_name,
        "rows": 0,
        "elapsed_sec": 0.0,
        "error": "",
        "source_path": str(path),
    }
    if not path.exists():
        meta["error"] = "missing_file"
        meta["elapsed_sec"] = round(time.perf_counter() - started, 6)
        return ticker_name, pd.DataFrame(), meta

    try:
        df = base_v15.read_parquet_tail(str(path), n=int(tail_rows))
        if df is None or df.empty:
            meta["elapsed_sec"] = round(time.perf_counter() - started, 6)
            return ticker_name, pd.DataFrame(), meta

        df = base_v15.normalize_dates(df)
        if df.empty:
            meta["elapsed_sec"] = round(time.perf_counter() - started, 6)
            return ticker_name, pd.DataFrame(), meta

        df = df.sort_values("date").tail(int(tail_rows)).reset_index(drop=True)
        if "ticker" not in df.columns:
            df["ticker"] = ticker_name
        else:
            df["ticker"] = ticker_name

        meta["rows"] = int(len(df))
        if "date" in df.columns and not df.empty:
            try:
                meta["max_slot_key"] = slot_key(df["date"].iloc[-1])
            except Exception:
                meta["max_slot_key"] = ""
        meta["elapsed_sec"] = round(time.perf_counter() - started, 6)
        return ticker_name, df, meta
    except Exception as exc:
        meta["error"] = repr(exc)
        meta["elapsed_sec"] = round(time.perf_counter() - started, 6)
        return ticker_name, pd.DataFrame(), meta


def _clip_shard_frame_to_slot(shard_df: pd.DataFrame, slot_ts: pd.Timestamp) -> pd.DataFrame:
    if shard_df is None or shard_df.empty or "date" not in shard_df.columns:
        return pd.DataFrame(columns=["ticker", "date"])
    clipped = shard_df.loc[shard_df["date"] <= slot_ts].copy()
    if clipped.empty:
        return pd.DataFrame(columns=shard_df.columns)
    sort_cols = [c for c in ["ticker", "date"] if c in clipped.columns]
    if sort_cols:
        clipped = clipped.sort_values(sort_cols).reset_index(drop=True)
    return clipped


def _trim_frame_to_ticker_tail(shard_df: pd.DataFrame, tail_rows: int) -> pd.DataFrame:
    if shard_df is None or shard_df.empty:
        return pd.DataFrame(columns=["ticker", "date"])
    if "ticker" not in shard_df.columns or "date" not in shard_df.columns:
        return shard_df.reset_index(drop=True)
    trimmed = shard_df.sort_values(["ticker", "date"]).reset_index(drop=True)
    trimmed = (
        trimmed.groupby("ticker", group_keys=False, sort=False)
        .tail(int(tail_rows))
        .reset_index(drop=True)
    )
    return trimmed


def _build_slot_context_payload(
    slot_ist: pd.Timestamp,
    source_root: Path,
    tail_rows: int,
) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "slot_ist": slot_ist.strftime("%Y-%m-%d %H:%M:%S%z"),
        "nifty_context": {},
    }
    try:
        cfg = default_short_config()
        cfg.dir_15m = str(source_root)
        cfg.end_15m = END_5M
        mode_map, nifty_ret_map, source, counts = v16_runner._build_nifty_intraday_context(cfg)
        if not mode_map:
            payload["nifty_context"] = {
                "allow_long": True,
                "allow_short": True,
                "mode": "BOTH",
                "rs_pct": 0.0,
                "source": str(source or NIFTYBEES_TICKER),
                "decision_reason": "backtest_intraday_context_unavailable",
            }
            return payload

        ts_key = v16_runner._ts_to_key_local(slot_ist)
        mode = str(mode_map.get(ts_key, "BOTH")).strip().upper() or "BOTH"
        rs_val = nifty_ret_map.get(ts_key)
        rs_pct = float(rs_val) if rs_val is not None and pd.notna(rs_val) else 0.0

        allow_long = mode != "SHORT_ONLY"
        allow_short = mode != "LONG_ONLY"
        payload["nifty_context"] = {
            "allow_long": bool(allow_long),
            "allow_short": bool(allow_short),
            "mode": mode,
            "rs_pct": float(rs_pct),
            "source": str(source or NIFTYBEES_TICKER),
            "decision_reason": "backtest_intraday_context",
            "context_counts": {str(k): int(v) for k, v in (counts or {}).items()},
        }
    except Exception as exc:
        payload["error"] = repr(exc)
    return payload


def _get_or_build_rolling_shard_cache(
    *,
    slot_ist: pd.Timestamp,
    source_root: Path,
    shard_count: int,
    tail_rows: int,
    max_workers: int,
    force_rebuild: bool = False,
) -> _RollingShardCache:
    cache_key = (str(source_root), int(shard_count), int(tail_rows))
    cached = None if force_rebuild else _ROLLING_SHARD_CACHE.get(cache_key)
    if cached is not None:
        current_slot_key = slot_key(slot_ist)
        if current_slot_key > str(cached.last_slot_key or ""):
            _refresh_rolling_shard_cache_for_slot(
                cached,
                slot_ist=slot_ist,
                max_workers=int(max_workers),
                incremental_rows=DEFAULT_INCREMENTAL_REFRESH_ROWS,
            )
        return cached

    started = time.perf_counter()
    shard_map = _split_tickers_for_shards(load_tickers_5m(source_root), shard_count=shard_count)
    ticker_to_shard = {
        str(ticker).strip().upper(): shard_id
        for shard_id, tickers in shard_map.items()
        for ticker in tickers
    }
    frames_by_shard: Dict[str, List[pd.DataFrame]] = {shard_id: [] for shard_id in shard_map}
    ticker_meta: Dict[str, Dict[str, Any]] = {}
    last_seen_by_ticker: Dict[str, pd.Timestamp] = {}
    workers = max(1, min(int(max_workers), len(ticker_to_shard)))

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(_read_one_ticker_tail, ticker, source_root, int(tail_rows)): ticker
            for ticker in sorted(ticker_to_shard)
        }
        for future in as_completed(futures):
            ticker_name, df_ticker, meta = future.result()
            ticker_meta[ticker_name] = meta
            shard_id = ticker_to_shard[ticker_name]
            if df_ticker is not None and not df_ticker.empty:
                df_ticker = _clip_shard_frame_to_slot(df_ticker, slot_ist)
                if not df_ticker.empty:
                    frames_by_shard[shard_id].append(df_ticker)
                    try:
                        last_seen_by_ticker[ticker_name] = pd.Timestamp(df_ticker["date"].max())
                    except Exception:
                        pass

    shard_full_frames: Dict[str, pd.DataFrame] = {}
    rows_by_shard: Dict[str, int] = {}
    tickers_by_shard: Dict[str, int] = {}
    max_slot_keys = [slot_key(ts) for ts in last_seen_by_ticker.values() if not pd.isna(ts)]
    source_max_slot_key = max(max_slot_keys) if max_slot_keys else slot_key(slot_ist)

    for shard_id, tickers in shard_map.items():
        if frames_by_shard[shard_id]:
            shard_df = pd.concat(frames_by_shard[shard_id], ignore_index=True)
            shard_df = _trim_frame_to_ticker_tail(shard_df, int(tail_rows))
        else:
            shard_df = pd.DataFrame(columns=["ticker", "date"])
        shard_full_frames[shard_id] = shard_df
        rows_by_shard[shard_id] = int(len(shard_df))
        tickers_by_shard[shard_id] = int(len(tickers))

    cache = _RollingShardCache(
        cache_key=cache_key,
        source_root=source_root,
        shard_count=int(shard_count),
        tail_rows=int(tail_rows),
        created_slot_key=slot_key(slot_ist),
        last_slot_key=slot_key(slot_ist),
        source_max_slot_key=source_max_slot_key,
        warm_elapsed_sec=round(time.perf_counter() - started, 3),
        refresh_elapsed_sec=0.0,
        refresh_rows_added=0,
        ticker_count=int(len(ticker_to_shard)),
        shard_full_frames=shard_full_frames,
        rows_by_shard=rows_by_shard,
        tickers_by_shard=tickers_by_shard,
        ticker_to_shard=ticker_to_shard,
        last_seen_by_ticker=last_seen_by_ticker,
        errored_tickers=sorted(
            [ticker for ticker, info in ticker_meta.items() if str(info.get("error", ""))]
        ),
    )
    _ROLLING_SHARD_CACHE[cache_key] = cache
    return cache


def _refresh_rolling_shard_cache_for_slot(
    cache: _RollingShardCache,
    *,
    slot_ist: pd.Timestamp,
    max_workers: int,
    incremental_rows: int,
) -> None:
    started = time.perf_counter()
    workers = max(1, min(int(max_workers), len(cache.ticker_to_shard)))
    recent_rows = max(2, int(incremental_rows))
    updates_by_shard: Dict[str, List[pd.DataFrame]] = {
        shard_id: [] for shard_id in cache.shard_full_frames
    }
    latest_seen_by_ticker: Dict[str, pd.Timestamp] = {}
    errored = set(str(t).upper() for t in cache.errored_tickers)
    added_rows = 0

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(_read_one_ticker_tail, ticker, cache.source_root, recent_rows): ticker
            for ticker in sorted(cache.ticker_to_shard)
        }
        for future in as_completed(futures):
            ticker_name, df_ticker, meta = future.result()
            ticker_name = str(ticker_name).strip().upper()
            shard_id = cache.ticker_to_shard[ticker_name]
            if str(meta.get("error", "")):
                errored.add(ticker_name)
                continue
            errored.discard(ticker_name)
            if df_ticker is None or df_ticker.empty or "date" not in df_ticker.columns:
                continue

            clipped = _clip_shard_frame_to_slot(df_ticker, slot_ist)
            last_seen = cache.last_seen_by_ticker.get(ticker_name)
            if last_seen is not None and not pd.isna(last_seen):
                last_seen_ts = pd.Timestamp(last_seen)
                needs_catchup = slot_ist > last_seen_ts
            else:
                last_seen_ts = pd.NaT
                needs_catchup = True

            latest_from_recent = pd.NaT
            if not clipped.empty and "date" in clipped.columns:
                try:
                    latest_from_recent = pd.Timestamp(clipped["date"].max())
                except Exception:
                    latest_from_recent = pd.NaT

            if needs_catchup and (
                clipped.empty or pd.isna(latest_from_recent) or latest_from_recent <= last_seen_ts
            ):
                _, df_full, meta_full = _read_one_ticker_tail(
                    ticker_name,
                    cache.source_root,
                    int(cache.tail_rows),
                )
                if not str(meta_full.get("error", "")) and df_full is not None and not df_full.empty:
                    clipped_full = _clip_shard_frame_to_slot(df_full, slot_ist)
                    if not clipped_full.empty:
                        clipped = clipped_full
                        try:
                            latest_from_recent = pd.Timestamp(clipped["date"].max())
                        except Exception:
                            latest_from_recent = pd.NaT

            if clipped.empty:
                continue

            if not pd.isna(latest_from_recent):
                latest_seen_by_ticker[ticker_name] = latest_from_recent

            if last_seen is not None and not pd.isna(last_seen):
                clipped = clipped.loc[clipped["date"] > pd.Timestamp(last_seen)].copy()
            if clipped.empty:
                continue
            updates_by_shard[shard_id].append(clipped)
            added_rows += int(len(clipped))

    for shard_id, update_frames in updates_by_shard.items():
        if not update_frames:
            continue
        base_df = cache.shard_full_frames.get(shard_id, pd.DataFrame(columns=["ticker", "date"]))
        merged = pd.concat([base_df] + update_frames, ignore_index=True)
        if "ticker" in merged.columns and "date" in merged.columns:
            merged = (
                merged.sort_values(["ticker", "date"])
                .drop_duplicates(subset=["ticker", "date"], keep="last")
                .reset_index(drop=True)
            )
            merged = _trim_frame_to_ticker_tail(merged, cache.tail_rows)
        cache.shard_full_frames[shard_id] = merged
        cache.rows_by_shard[shard_id] = int(len(merged))

    if latest_seen_by_ticker:
        cache.last_seen_by_ticker.update(latest_seen_by_ticker)
        max_keys = [slot_key(ts) for ts in cache.last_seen_by_ticker.values() if not pd.isna(ts)]
        if max_keys:
            cache.source_max_slot_key = max(max_keys)
    cache.last_slot_key = slot_key(slot_ist)
    cache.refresh_rows_added = int(added_rows)
    cache.refresh_elapsed_sec = round(time.perf_counter() - started, 3)
    cache.errored_tickers = sorted(errored)


def build_slot_snapshots(
    slot_ts: Any,
    *,
    runtime_root: Optional[Path | str] = None,
    source_dir: Optional[Path | str] = None,
    shard_count: int = 10,
    tail_rows: int = DEFAULT_TAIL_ROWS,
    max_workers: int = DEFAULT_SNAPSHOT_MAX_WORKERS,
    use_rolling_cache: bool = DEFAULT_USE_ROLLING_CACHE,
    force_rebuild_cache: bool = False,
    build_slot_context: bool = DEFAULT_BUILD_SLOT_CONTEXT,
) -> Dict[str, Any]:
    slot_ist = normalize_slot_ist(slot_ts)
    started = time.perf_counter()
    source_root = Path(source_dir) if source_dir is not None else Path(DATA_5M_DIR)
    shard_map = _split_tickers_for_shards(load_tickers_5m(source_root), shard_count=shard_count)
    workers = 0
    cache_meta: Dict[str, Any] = {
        "mode": "cold",
        "used": False,
        "warm_elapsed_sec": 0.0,
        "source_max_slot_key": "",
        "created_slot_key": "",
    }

    if bool(use_rolling_cache):
        load_started = time.perf_counter()
        rolling_cache = _get_or_build_rolling_shard_cache(
            slot_ist=slot_ist,
            source_root=source_root,
            shard_count=int(shard_count),
            tail_rows=int(tail_rows),
            max_workers=int(max_workers),
            force_rebuild=bool(force_rebuild_cache),
        )
        load_elapsed = time.perf_counter() - load_started
        workers = int(max_workers)
        cache_meta = {
            "mode": "rolling",
            "used": True,
            "warm_elapsed_sec": float(rolling_cache.warm_elapsed_sec),
            "refresh_elapsed_sec": float(rolling_cache.refresh_elapsed_sec),
            "refresh_rows_added": int(rolling_cache.refresh_rows_added),
            "source_max_slot_key": str(rolling_cache.source_max_slot_key or ""),
            "created_slot_key": str(rolling_cache.created_slot_key or ""),
            "last_slot_key": str(rolling_cache.last_slot_key or ""),
            "reused": not bool(force_rebuild_cache),
        }
        shard_rows_full = dict(rolling_cache.rows_by_shard)
        tickers_by_shard = dict(rolling_cache.tickers_by_shard)
        errored_tickers = list(rolling_cache.errored_tickers)
    else:
        ticker_to_shard = {
            str(ticker).strip().upper(): shard_id
            for shard_id, tickers in shard_map.items()
            for ticker in tickers
        }
        frames_by_shard: Dict[str, List[pd.DataFrame]] = {shard_id: [] for shard_id in shard_map}
        ticker_meta: Dict[str, Dict[str, Any]] = {}
        load_started = time.perf_counter()

        workers = max(1, min(int(max_workers), len(ticker_to_shard)))
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {
                executor.submit(_read_one_ticker_tail, ticker, source_root, int(tail_rows)): ticker
                for ticker in sorted(ticker_to_shard)
            }
            for future in as_completed(futures):
                ticker_name, df_ticker, meta = future.result()
                ticker_meta[ticker_name] = meta
                shard_id = ticker_to_shard[ticker_name]
                if df_ticker is not None and not df_ticker.empty:
                    df_ticker = _clip_shard_frame_to_slot(df_ticker, slot_ist)
                    if not df_ticker.empty:
                        frames_by_shard[shard_id].append(df_ticker)

        load_elapsed = time.perf_counter() - load_started
        shard_rows_full = {}
        tickers_by_shard = {shard_id: int(len(tickers)) for shard_id, tickers in shard_map.items()}
        errored_tickers = sorted(
            [ticker for ticker, info in ticker_meta.items() if str(info.get("error", ""))]
        )

    write_started = time.perf_counter()
    shard_rows: Dict[str, int] = {}

    for shard_id in shard_map:
        shard_path = snapshot_parquet_path(slot_ist, shard_id, runtime_root)
        if bool(use_rolling_cache):
            shard_df = _clip_shard_frame_to_slot(
                rolling_cache.shard_full_frames.get(shard_id, pd.DataFrame()),
                slot_ist,
            )
        else:
            if frames_by_shard[shard_id]:
                shard_df = pd.concat(frames_by_shard[shard_id], ignore_index=True)
                sort_cols = [c for c in ["ticker", "date"] if c in shard_df.columns]
                if sort_cols:
                    shard_df = shard_df.sort_values(sort_cols).reset_index(drop=True)
            else:
                shard_df = pd.DataFrame(columns=["ticker", "date"])
        shard_df.to_parquet(shard_path, index=False)
        shard_rows[shard_id] = int(len(shard_df))

    write_elapsed = time.perf_counter() - write_started
    context_elapsed = 0.0
    slot_context_meta: Dict[str, Any] = {"enabled": bool(build_slot_context)}
    if bool(build_slot_context):
        context_started = time.perf_counter()
        slot_context = _build_slot_context_payload(slot_ist, source_root, int(tail_rows))
        slot_context_path(slot_ist, runtime_root).write_text(
            json.dumps(slot_context, indent=2, sort_keys=True),
            encoding="utf-8",
        )
        context_elapsed = time.perf_counter() - context_started
        slot_context_meta["path"] = str(slot_context_path(slot_ist, runtime_root))

    total_elapsed = time.perf_counter() - started
    meta = {
        "slot_ist": slot_ist.strftime("%Y-%m-%d %H:%M:%S%z"),
        "slot_key": slot_key(slot_ist),
        "source_dir": str(source_root),
        "runtime_root": str(Path(runtime_root) if runtime_root is not None else Path(RUNTIME_ROOT)),
        "tail_rows": int(tail_rows),
        "snapshot_workers": int(workers),
        "ticker_count": int(sum(int(len(tickers)) for tickers in shard_map.values())),
        "errored_tickers": errored_tickers,
        "load_elapsed_sec": round(load_elapsed, 3),
        "write_elapsed_sec": round(write_elapsed, 3),
        "context_elapsed_sec": round(context_elapsed, 3),
        "total_elapsed_sec": round(total_elapsed, 3),
        "rows_by_shard": shard_rows,
        "rows_by_shard_full": shard_rows_full,
        "tickers_by_shard": tickers_by_shard,
        "snapshot_cache": cache_meta,
        "slot_context": slot_context_meta,
        "snapshot_paths": {
            shard_id: str(snapshot_parquet_path(slot_ist, shard_id, runtime_root))
            for shard_id in shard_map
        },
    }
    snapshot_meta_path(slot_ist, runtime_root).write_text(
        json.dumps(meta, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return meta


def read_shard_snapshot(
    slot_ts: Any,
    shard_id: int | str,
    *,
    runtime_root: Optional[Path | str] = None,
) -> pd.DataFrame:
    path = snapshot_parquet_path(slot_ts, shard_id, runtime_root)
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_parquet(path)
    except Exception:
        return pd.DataFrame()


def _parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Build per-slot combined shard snapshots for v16_5min.")
    ap.add_argument("--slot", required=True, help="Slot timestamp in IST, e.g. 2026-03-31 14:45:00+05:30")
    ap.add_argument("--runtime-root", default=str(RUNTIME_ROOT))
    ap.add_argument("--source-dir", default=str(DATA_5M_DIR))
    ap.add_argument("--shard-count", type=int, default=10)
    ap.add_argument("--tail-rows", type=int, default=DEFAULT_TAIL_ROWS)
    ap.add_argument("--max-workers", type=int, default=DEFAULT_SNAPSHOT_MAX_WORKERS)
    ap.add_argument("--no-rolling-cache", action="store_true")
    ap.add_argument("--force-rebuild-cache", action="store_true")
    ap.add_argument("--build-slot-context", action="store_true")
    return ap.parse_args()


def main() -> None:
    args = _parse_args()
    meta = build_slot_snapshots(
        args.slot,
        runtime_root=args.runtime_root,
        source_dir=args.source_dir,
        shard_count=int(args.shard_count),
        tail_rows=int(args.tail_rows),
        max_workers=int(args.max_workers),
        use_rolling_cache=not bool(args.no_rolling_cache),
        force_rebuild_cache=bool(args.force_rebuild_cache),
        build_slot_context=bool(args.build_slot_context),
    )
    print(json.dumps(meta, indent=2, sort_keys=True), flush=True)


if __name__ == "__main__":
    main()
