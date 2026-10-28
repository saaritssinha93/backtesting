"""Read-only G7 V2 sources for the separate G9 dual-market experiment.

The cache contains every *selected G7 opportunity*, including native fills
subsequently rejected by the portfolio. It is not the full exchange candidate
universe. No old G9/G8 runner, retrieval, cache builder or result pointer runs.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd

from research import g8_sources
from research.g8_exit_engine import _flags, allocate_variant, metrics


ROOT = Path(__file__).resolve().parents[1]
DATA_ROOT = Path("C:/TradingData/eqidv2")
CACHE = DATA_ROOT / "backtesting_result_v13_v10_g_8/inputs/baseline_v2"
DAILY = DATA_ROOT / ("backtesting_result_v13_v10_g_7/daily/runs/2026-10-23/"
                     "20261023T183148514804_bbcb21e1_g7_nifty_futures")
CACHE_MANIFEST = "c0a52d928a3b7b6ac3a270a5908001a71f6740634c77ee41e7ee2244a7e0a6a9"
DAILY_MANIFEST = "b4bbc3fe061e9af20feb5d7d9fea869c0d36f171884e40d97f4263d8e5b67acc"
THROUGH = "2026-10-23"
VARIANT = "V13-V10-G-7-G6-REPLICA-V2"
POLICY = "g7_nifty_futures_both_counter"
PATH_FIELDS = ("timestamp_ns", "open", "high", "low", "close")


def sha(path):
    with Path(path).open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def _read(path):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def _verify_package(directory, expected):
    directory = Path(directory).resolve()
    manifest_path = directory / "manifest.json"
    if not manifest_path.is_file():
        raise ValueError("Required existing G7 source package is missing: " + str(directory))
    if sha(manifest_path) != expected:
        raise ValueError("Pinned G7 source manifest changed: " + str(directory))
    manifest = _read(manifest_path)
    if not isinstance(manifest.get("files"), dict) or not manifest["files"]:
        raise ValueError("Empty G7 source manifest")
    for name, digest in manifest["files"].items():
        member = (directory / name).resolve()
        if not member.is_relative_to(directory) or not member.is_file() or sha(member) != digest:
            raise ValueError("Pinned G7 source member changed: " + name)
    return manifest


def _validate_inventory(frames, paths, days):
    orders = frames["orders"]
    keys = set(orders.sid)
    if orders.sid.isna().any() or orders.sid.duplicated().any():
        raise ValueError("G7 selected order IDs are missing or duplicated")
    if orders.duplicated(["day", "setup_id", "tradingsymbol", "side"]).any():
        raise ValueError("Duplicate G7 selected opportunity")
    if set(paths) != keys or days != sorted(set(days)):
        raise ValueError("G7 paths or session calendar differ from selected orders")
    for name, frame in frames.items():
        if (frame.sid.isna().any() or frame.sid.duplicated().any()
                or set(frame.sid) != keys or not set(frame.day).issubset(days)):
            raise ValueError("G7 candidate inventory differs: " + name)
    for row in orders.itertuples():
        path = paths[int(row.sid)]
        lengths = {len(path[field]) for field in PATH_FIELDS}
        if len(lengths) != 1:
            raise ValueError("Unequal minute-path array lengths")
        times = np.asarray(path["timestamp_ns"], dtype=np.int64)
        if len(times):
            confirmation = pd.Timestamp(row.confirmation_ts).value
            if np.any(np.diff(times) <= 0) or times[0] <= confirmation:
                raise ValueError("Noncausal or duplicated G7 minute path")
            prices = np.array([path[field] for field in PATH_FIELDS[1:]], dtype=float)
            if not np.isfinite(prices).all() or (prices <= 0).any():
                raise ValueError("Invalid G7 minute-path prices")


def capability_audit():
    """Inspect representative actual source schemas, without inventing flows.

    The audit is explicitly of these observed input stores; it is not a claim
    that every file on the computer was inspected. Historical fetch timestamps
    do not establish that quotes or OI were received in real time at a signal.
    """
    samples = (
        ("FUTURES", DATA_ROOT / "fno_oi/raw_contracts_5m/JSWSTEEL26AUGFUT_5minute.parquet"),
        ("SPOT", DATA_ROOT / "stocks_indicators_1min_eq/JSWSTEEL_stocks_indicators_1min.parquet"),
    )
    records = []
    for market, path in samples:
        present = path.is_file()
        columns, count, first, last, digest = [], 0, "", "", ""
        if present:
            import pyarrow.parquet as parquet
            file = parquet.ParquetFile(path)
            columns = file.schema_arrow.names
            count = file.metadata.num_rows
            field = next((col for col in ("timestamp", "ts", "date") if col in columns), None)
            if field:
                timestamps = pd.to_datetime(pd.read_parquet(path, columns=[field])[field], utc=True)
                first, last = str(timestamps.min()), str(timestamps.max())
            digest = sha(path)
        records.append(dict(
            market=market, path=str(path), present=present, sha256=digest,
            rows=count, first_timestamp_utc=first, last_timestamp_utc=last,
            columns=json.dumps(columns), source_kind="HISTORICAL_OHLCV_CANDLES",
            oi_present="oi" in columns, true_ofi_available=False, true_cvd_available=False,
            real_time_receive_evidence=False,
            limitation="Observed schema has no sequential bid/ask events or aggressor-classified trade prints. "
                       "Historical candle fetch time does not prove real-time OI freshness.",
        ))
    return pd.DataFrame(records)


def futures_context(orders):
    """Read exact completed same-contract candles; never infer missing inputs.

    Returns a SID-indexed feature frame (also retaining the ``sid`` column),
    plus file-hash provenance. ``futures_ret_5m`` is a decimal return; its
    ``_pct`` counterpart is percentage points. Availability means historical
    candle-asof availability only: historical fetch timestamps cannot prove
    that OI or prices were received before a live decision.
    """
    required = {"sid", "signal_ts", "futures_tradingsymbol", "oi", "prev_oi"}
    if not required.issubset(orders):
        raise ValueError("Missing futures-context order fields: " + ", ".join(sorted(required - set(orders))))
    if orders.sid.isna().any() or orders.sid.duplicated().any():
        raise ValueError("Futures-context SID must be nonmissing and unique")
    directory = (DATA_ROOT / "fno_oi/raw_contracts_5m").resolve()
    loaded, evidence, records = {}, {}, []
    required_columns = {"timestamp", "candle_start", "tradingsymbol", "close", "oi", "quality_state"}
    flags = ("gap_filled", "is_synthetic", "synthetic", "provisional_stale", "opening_snapshot")

    def read_contract(contract):
        path = (directory / (contract + "_5minute.parquet")).resolve()
        if not path.is_relative_to(directory) or path.parent != directory:
            raise ValueError("Invalid futures contract filename")
        if path in loaded:
            return path, loaded[path]
        if not path.is_file():
            loaded[path] = (None, "", "MISSING_CONTRACT_FILE")
            evidence[str(path)] = dict(present=False)
            return path, loaded[path]
        digest = sha(path)
        frame = pd.read_parquet(path)
        if sha(path) != digest:
            raise ValueError("Futures source changed while reading: " + str(path))
        evidence[str(path)] = dict(present=True, sha256=digest, rows=len(frame), columns=list(frame.columns))
        if not required_columns.issubset(frame):
            loaded[path] = (None, digest, "MISSING_REQUIRED_CANDLE_COLUMNS")
        else:
            frame = frame.copy()
            frame["timestamp"] = pd.to_datetime(frame.timestamp, utc=True, errors="coerce")
            frame["candle_start"] = pd.to_datetime(frame.candle_start, utc=True, errors="coerce")
            loaded[path] = (frame.set_index("timestamp", drop=False), digest, "")
        return path, loaded[path]

    for order in orders.to_dict("records"):
        row = dict(sid=order["sid"], futures_context_available=False,
                   futures_ret_5m=np.nan, futures_ret_5m_pct=np.nan,
                   futures_oi=np.nan, futures_prev_oi=np.nan, futures_oi_change_pct=np.nan,
                   futures_oi_matches_candidate=False, futures_context_reason="",
                   futures_bar_close_ts="", futures_previous_bar_close_ts="",
                   futures_source_path="", futures_source_sha256="",
                   futures_timing_basis="HISTORICAL_COMPLETED_CANDLE_ASOF_ONLY",
                   futures_receive_time_verified=False)
        contract = order["futures_tradingsymbol"]
        if pd.isna(contract) or not str(contract).strip():
            row["futures_context_reason"] = "MISSING_CONTRACT_MAPPING"
            records.append(row)
            continue
        try:
            signal = pd.Timestamp(order["signal_ts"])
        except (ValueError, TypeError):
            signal = pd.NaT
        if pd.isna(signal) or signal.tzinfo is None:
            row["futures_context_reason"] = "MISSING_OR_NAIVE_SIGNAL_TIMESTAMP"
            records.append(row)
            continue
        signal = signal.tz_convert("UTC")
        previous = signal - pd.Timedelta(minutes=5)
        row.update(futures_bar_close_ts=str(signal), futures_previous_bar_close_ts=str(previous))
        path, (frame, digest, error) = read_contract(str(contract))
        row.update(futures_source_path=str(path), futures_source_sha256=digest)
        if error:
            row["futures_context_reason"] = error
        elif signal not in frame.index:
            row["futures_context_reason"] = "MISSING_EXACT_SIGNAL_BAR"
        elif previous not in frame.index:
            row["futures_context_reason"] = "MISSING_EXACT_PREVIOUS_BAR"
        else:
            pair = frame.loc[frame.index.isin([previous, signal])].sort_index()
            if len(pair) != 2 or not pair.index.is_unique:
                row["futures_context_reason"] = "DUPLICATED_CANDLE_TIMESTAMP"
            elif not pair.tradingsymbol.eq(str(contract)).all():
                row["futures_context_reason"] = "CONTRACT_IDENTITY_MISMATCH"
            elif not (pair.timestamp - pair.candle_start).eq(pd.Timedelta(minutes=5)).all():
                row["futures_context_reason"] = "INVALID_CANDLE_INTERVAL"
            elif not pair.quality_state.astype(str).str.upper().eq("VALID").all():
                row["futures_context_reason"] = "INVALID_CANDLE_QUALITY"
            elif any(not pair[field].astype(str).str.strip().str.lower().isin(["false", "0", "0.0"]).all()
                     for field in flags if field in pair):
                row["futures_context_reason"] = "SYNTHETIC_GAP_OR_STALE_CANDLE"
            elif "expiry" in pair and (pair.expiry.isna().any() or pair.expiry.nunique() != 1):
                row["futures_context_reason"] = "CONTRACT_EXPIRY_MISMATCH"
            else:
                numbers = pair[["close", "oi"]].apply(pd.to_numeric, errors="coerce").to_numpy(float)
                candidate_oi = pd.to_numeric(pd.Series([order["prev_oi"], order["oi"]]), errors="coerce").to_numpy(float)
                if not np.isfinite(numbers).all() or (numbers <= 0).any():
                    row["futures_context_reason"] = "INVALID_PRICE_OR_OI"
                else:
                    prior_close, prior_oi = numbers[0]
                    close, oi = numbers[1]
                    ret = close / prior_close - 1.
                    matched = bool(np.isfinite(candidate_oi).all() and
                                   np.allclose(numbers[:, 1], candidate_oi, atol=1e-8, rtol=0))
                    row.update(futures_ret_5m=ret, futures_ret_5m_pct=100 * ret,
                               futures_oi=float(oi), futures_prev_oi=float(prior_oi),
                               futures_oi_change_pct=100 * (oi / prior_oi - 1.),
                               futures_oi_matches_candidate=matched, futures_context_available=matched,
                               futures_context_reason="OK" if matched else "CANDIDATE_OI_MISMATCH")
        records.append(row)
    if records:
        result = pd.DataFrame(records).set_index("sid", drop=False)
    else:
        result = pd.DataFrame(columns=["sid", "futures_context_available", "futures_ret_5m",
                                       "futures_ret_5m_pct", "futures_context_reason"]).set_index("sid", drop=False)
    provenance = dict(
        schema_version="g9_dual_market_exact_futures_candles_v1", source_files=evidence,
        rows=len(result), available=int(result.futures_context_available.sum()),
        reason_counts=result.futures_context_reason.value_counts().to_dict(),
        timing_basis="HISTORICAL_COMPLETED_CANDLE_ASOF_ONLY", real_time_receive_time_verified=False,
        oi_freshness_verified=False, true_ofi_available=False, true_cvd_available=False,
        limitation="Only completed bars exactly at signal time and five minutes earlier are read. "
                   "No forward filling or contract splicing. Historical fetch time is not real-time receive evidence; "
                   "candle OI does not prove intra-bar OI freshness. Only unflagged VALID candles are eligible.",
    )
    return result, provenance


def load_sources(*, through=THROUGH):
    """Load and reproduce exact G7 V2, through 23 October; never write sources."""
    if str(through) != THROUGH:
        raise ValueError("This pinned G7 V2 source revision ends at " + THROUGH)
    # Check the existing cache *before* invoking its reader, whose legacy missing
    # cache branch would otherwise build and publish a cache.
    cache_manifest = _verify_package(CACHE, CACHE_MANIFEST)
    daily_manifest = _verify_package(DAILY, DAILY_MANIFEST)
    cached = g8_sources.load_sources(cache=CACHE, through="2026-10-22")
    summary, daily_config = _read(DAILY / "summary.json"), _read(DAILY / "config.json")
    config = cached["config"]
    if (daily_config != config or config.get("variant_id") != VARIANT
            or len(config.get("active_setups", [])) != 51
            or summary.get("variant_id") != VARIANT or summary.get("complete") is not True
            or summary.get("selected_policy") != POLICY or summary.get("days") != [THROUGH]
            or summary.get("execution_authority") is not False
            or config.get("execution_authority") is not False):
        raise ValueError("Unexpected G7 V2 daily source identity or configuration")
    daily_files = dict(orders="selected_orders.csv", unfiltered="g7_unfiltered/portfolio_trades.csv",
                       baseline=POLICY + "/portfolio_trades.csv", audit="nifty_direction_audit.csv")
    daily_frames = {name: pd.read_csv(DAILY / filename, low_memory=False)
                    for name, filename in daily_files.items()}
    daily_orders = daily_frames["orders"]
    if daily_orders.sid.isna().any() or daily_orders.sid.duplicated().any():
        raise ValueError("Duplicate G7 daily source order IDs")
    next_sid = int(cached["orders"].sid.max()) + 1
    remap = {int(sid): next_sid + index for index, sid in enumerate(daily_orders.sid)}
    frames = {}
    for name, frame in daily_frames.items():
        if set(frame.sid) != set(remap) or frame.sid.duplicated().any():
            raise ValueError("Daily source omitted selected G7 candidates: " + name)
        frame = frame.copy()
        frame["g8_source_sid"] = frame.sid.astype(int)
        frame["g8_source_package"] = DAILY.name
        frame["sid"] = frame.g8_source_sid.map(remap).astype(int)
        frames[name] = pd.concat([cached[name], frame], ignore_index=True, sort=False)
        frames[name]["day"] = frames[name].day.astype(str).str[:10]
    raw = pd.read_parquet(DAILY / "minute_paths.parquet")
    if set(raw.sid) != set(remap):
        raise ValueError("Daily minute paths omitted selected G7 candidates")
    paths = dict(cached["paths"])
    for sid, group in raw.groupby("sid", sort=False):
        paths[remap[int(sid)]] = {field: group[field].to_numpy(copy=True) for field in PATH_FIELDS}
    days = cached["days"] + [THROUGH]
    _validate_inventory(frames, paths, days)
    for name in ("unfiltered", "baseline"):
        for field in ("filled", "portfolio_executed"):
            frames[name][field] = _flags(frames[name][field], field)
    frames["audit"]["both_counter_allowed"] = _flags(frames["audit"].both_counter_allowed,
                                                    "both_counter_allowed")
    allowed = frames["audit"].set_index("sid").both_counter_allowed
    reproduced = allocate_variant(frames["unfiltered"], allowed, config, "g9_exact_g7_v2_parity")
    expected = frames["baseline"].set_index("sid").reindex(reproduced.sid)
    for field in ("filled", "portfolio_executed"):
        if not np.array_equal(reproduced[field].to_numpy(), expected[field].to_numpy()):
            raise ValueError("G7 V2 allocation parity differs: " + field)
    for field in ("portfolio_gross_profit_rupees", "portfolio_net_profit_rupees", "portfolio_cost_rupees"):
        if not np.allclose(reproduced[field], expected[field], atol=1e-6, rtol=0, equal_nan=True):
            raise ValueError("G7 V2 accounting parity differs: " + field)
    stats = metrics(reproduced, days)
    if (len(frames["orders"]), len(days), stats["trades"], stats["wins"], stats["losses"]) != (582, 57, 437, 329, 108):
        raise ValueError("Pinned G7 V2 combined source inventory changed")
    if not np.isclose(stats["net_pnl_rs"], 1069645.3676661553, atol=1e-6, rtol=0):
        raise ValueError("Pinned G7 V2 combined net profit changed")
    provenance = dict(
        schema_version="g9_dual_market_g7_v2_sources_v1", source_variant=VARIANT, through=THROUGH,
        scope="BACKTEST_ONLY", execution_authority=False, days=days, sessions=len(days),
        selected_orders=len(frames["orders"]), all_selected_opportunities_retained=True,
        full_exchange_candidate_universe=False, baseline_allocation_reproduced=True,
        baseline_stats=stats, source_manifest_sha256={str(CACHE): CACHE_MANIFEST, str(DAILY): DAILY_MANIFEST},
        packages=[dict(path=str(CACHE), manifest_sha256=CACHE_MANIFEST, files=cache_manifest["files"]),
                  dict(path=str(DAILY), manifest_sha256=DAILY_MANIFEST, files=daily_manifest["files"])],
        cached_source_provenance=cached["provenance"], daily_summary=summary,
        limitations=[
            "G7 V2 selected opportunities only; this does not rerank the full contemporaneous F&O universe.",
            "Inherited setups and targets were selected after historical inspection; dates are not untouched holdout data.",
            "Inherited after-close stock-day eligibility exclusions do not establish causal live eligibility.",
            "Order price_change_pct is the spot price return; OI fields refer to the mapped futures contract.",
            "Minute-path labels are bar closes; the first bar ends one minute after G7 confirmation.",
            "No sequential spot/futures book or classified trade feed is supplied by these source packages.",
        ],
    )
    return dict(config=config, **frames, paths=paths, days=days, provenance=provenance,
                cache=CACHE, manifest_sha256=CACHE_MANIFEST, baseline_metrics=stats,
                reproduced_baseline=reproduced, capability_audit=capability_audit())
