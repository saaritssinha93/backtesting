"""Point-in-time Market/Sector Context study for a frozen V11 replay week.

This script does not scan setups, generate trades, or alter V11 execution.  It
loads frozen artifacts produced by the canonical
``avwap_5min_ID_v11_backtesting.py`` engine with the approved final setup book,
attaches Market Context and Sector Intelligence as of each signal timestamp,
and writes model-ready/enrichment and feature-audit artifacts.

The chronological shadow study is deliberately labelled exploratory.  It uses
the first three sessions for a one-feature direction/median and reports the
unchanged rule on the final two sessions.  A five-session sample is too small
for promotion or a profitability claim.
"""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict
from hashlib import sha256
import json
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

import numpy as np
import pandas as pd

from market_context_engine import (
    MarketContextConfig,
    MarketContextEngine,
    MarketContextResult,
    attach_context_asof,
    context_feature_columns,
    load_sector_map,
)
from sector_intelligence import (
    SectorIntelligenceConfig,
    SectorIntelligenceEngine,
    SectorIntelligenceResult,
    attach_sector_intelligence_asof,
    sector_intelligence_feature_columns,
)


IST = "Asia/Kolkata"
DEFAULT_DATA_5M = Path(
    r"C:\TradingData\eqidv2\stocks_indicators_5min_eq_live2"
)
DEFAULT_SECTOR_MAP = Path(__file__).resolve().parent / "configs" / "sector_etf_map.json"
BASE_COLUMNS = (
    "date",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "EMA_20",
    "EMA_50",
    "gap_filled",
    "opening_snapshot",
)
INDEX_EXTRA_COLUMNS = ("ATR",)
MODEL_BASELINE_COLUMNS = (
    "ranker_score",
    "quality_score",
    "rs_pct",
    "market_ret_pct",
    "vol_ratio",
    "atr_pct",
    "body_pct",
    "close_loc",
    "vwap_dist_atr",
)
CONTEXT_CACHE_SCHEMA_VERSION = "v11-context-week-v2"


def _to_ist(values: pd.Series) -> pd.Series:
    parsed = pd.to_datetime(values, errors="coerce")
    if getattr(parsed.dt, "tz", None) is None:
        return parsed.dt.tz_localize(
            IST, ambiguous="NaT", nonexistent="shift_forward"
        )
    return parsed.dt.tz_convert(IST)


def _normalise_ticker(value: Any) -> str:
    return str(value).upper().replace(".NS", "").strip()


def _load_v11_universe(data_dir: Path) -> list[str]:
    try:
        from filtered_stocks_MIS import selected_stocks

        raw = selected_stocks
    except Exception:
        raw = []
    if not raw:
        raw = [
            path.name.replace("_stocks_indicators_5min.parquet", "")
            for path in data_dir.glob("*_stocks_indicators_5min.parquet")
        ]
    return sorted(
        {
            ticker
            for ticker in (_normalise_ticker(value) for value in raw)
            if ticker
        }
    )


def _read_filtered_parquet(
    path: Path,
    *,
    ticker: str,
    start: pd.Timestamp,
    end_exclusive: pd.Timestamp,
    extra_columns: Sequence[str] = (),
) -> pd.DataFrame:
    columns = list(dict.fromkeys([*BASE_COLUMNS, *extra_columns]))
    filters = [("date", ">=", start), ("date", "<", end_exclusive)]
    try:
        frame = pd.read_parquet(path, columns=columns, filters=filters)
    except Exception:
        required = ["date", "open", "high", "low", "close", "volume"]
        frame = pd.read_parquet(path, columns=required, filters=filters)
    if frame.empty:
        return frame
    frame = frame.copy()
    frame["date"] = _to_ist(frame["date"])
    frame = (
        frame.dropna(subset=["date"])
        .sort_values("date")
        .drop_duplicates("date", keep="last")
    )
    for column in BASE_COLUMNS:
        if column not in frame:
            if column in {"gap_filled", "opening_snapshot"}:
                frame[column] = 0
            else:
                frame[column] = np.nan
    frame["ticker"] = ticker
    return frame


def load_stock_panel(
    data_dir: Path,
    tickers: Sequence[str],
    *,
    start: pd.Timestamp,
    end_exclusive: pd.Timestamp,
    workers: int,
) -> tuple[pd.DataFrame, list[str]]:
    def load(ticker: str) -> tuple[str, pd.DataFrame]:
        path = data_dir / f"{ticker}_stocks_indicators_5min.parquet"
        if not path.exists():
            return ticker, pd.DataFrame()
        return ticker, _read_filtered_parquet(
            path,
            ticker=ticker,
            start=start,
            end_exclusive=end_exclusive,
        )

    with ThreadPoolExecutor(max_workers=max(1, int(workers))) as executor:
        loaded = list(executor.map(load, tickers))
    available = [ticker for ticker, frame in loaded if not frame.empty]
    frames = [frame for _, frame in loaded if not frame.empty]
    panel = pd.concat(frames, ignore_index=True, sort=False) if frames else pd.DataFrame()
    if not panel.empty:
        panel = panel.sort_values(["date", "ticker"]).reset_index(drop=True)
    return panel, available


def _session_window(
    index_path: Path,
    *,
    start_date: str,
    end_date: str,
    warmup_sessions: int,
) -> tuple[pd.Timestamp, pd.Timestamp, list[str]]:
    dates = pd.read_parquet(index_path, columns=["date"])["date"]
    timestamps = _to_ist(dates).dropna().sort_values()
    sessions = sorted(timestamps.dt.strftime("%Y-%m-%d").unique().tolist())
    eligible = [day for day in sessions if day <= end_date]
    if start_date not in eligible:
        raise ValueError(f"start date {start_date} is absent from {index_path.name}")
    start_index = eligible.index(start_date)
    warmup_index = max(0, start_index - int(warmup_sessions))
    included_sessions = eligible[warmup_index : eligible.index(end_date) + 1]
    warmup_start = pd.Timestamp(included_sessions[0], tz=IST)
    end_exclusive = pd.Timestamp(end_date, tz=IST) + pd.Timedelta(days=1)
    return warmup_start, end_exclusive, included_sessions


def _read_index_bars(
    data_dir: Path,
    *,
    start: pd.Timestamp,
    end_exclusive: pd.Timestamp,
) -> Mapping[str, pd.DataFrame]:
    for ticker in ("NIFTY50_INDEX", "NIFTY50", "NIFTY_50", "NIFTY"):
        path = data_dir / f"{ticker}_stocks_indicators_5min.parquet"
        if not path.exists():
            continue
        frame = _read_filtered_parquet(
            path,
            ticker=ticker,
            start=start,
            end_exclusive=end_exclusive,
            extra_columns=INDEX_EXTRA_COLUMNS,
        )
        if not frame.empty:
            return {ticker: frame}
    return {}


def _sha256_file(path: Path) -> str:
    digest = sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _context_cache_key(
    *,
    data_dir: Path,
    sector_map_path: Path,
    start_date: str,
    end_date: str,
    warmup_sessions: int,
    publish_delay_seconds: int,
) -> dict[str, Any]:
    index_path = data_dir / "NIFTY50_INDEX_stocks_indicators_5min.parquet"
    if not index_path.exists():
        raise FileNotFoundError(f"NIFTY index file not found: {index_path}")
    index_stat = index_path.stat()
    source_paths = [
        Path(__file__).resolve().parent / "market_context_engine.py",
        Path(__file__).resolve().parent / "sector_intelligence.py",
    ]
    return {
        "schema_version": CONTEXT_CACHE_SCHEMA_VERSION,
        "start_date": start_date,
        "end_date": end_date,
        "warmup_sessions": int(warmup_sessions),
        "publish_delay_seconds": int(publish_delay_seconds),
        "data_dir": str(data_dir.resolve()),
        "sector_map_path": str(sector_map_path.resolve()),
        "sector_map_sha256": _sha256_file(sector_map_path),
        "index_size": int(index_stat.st_size),
        "index_mtime_ns": int(index_stat.st_mtime_ns),
        "source_sha256": {
            path.name: _sha256_file(path) for path in source_paths
        },
    }


def _load_context_cache(
    cache_dir: Path, expected_key: Mapping[str, Any]
) -> tuple[MarketContextResult, SectorIntelligenceResult, dict[str, Any]] | None:
    paths = {
        "market": cache_dir / "market_context_market.parquet",
        "market_sectors": cache_dir / "market_context_sectors.parquet",
        "si_sectors": cache_dir / "sector_intelligence_sectors.parquet",
        "si_stocks": cache_dir / "sector_intelligence_stocks.parquet",
    }
    manifest_path = cache_dir / "context_manifest.json"
    if not all(path.exists() for path in paths.values()) or not manifest_path.exists():
        return None
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    actual_key = dict(manifest.get("cache_key", {}))
    expected = dict(expected_key)
    # Feature values are versioned by the two engines plus the explicit cache
    # schema/config.  Ignore the old orchestrator hash so report/export-only
    # edits do not force another 6.2-million-row context computation.
    for key in (actual_key, expected):
        source_hashes = dict(key.get("source_sha256", {}))
        source_hashes.pop("research_v11_context_week.py", None)
        key["source_sha256"] = source_hashes
    if actual_key != expected:
        return None
    return (
        MarketContextResult(
            market=pd.read_parquet(paths["market"]),
            sectors=pd.read_parquet(paths["market_sectors"]),
        ),
        SectorIntelligenceResult(
            sectors=pd.read_parquet(paths["si_sectors"]),
            stocks=pd.read_parquet(paths["si_stocks"]),
        ),
        manifest,
    )


def build_context(
    *,
    data_dir: Path,
    sector_map_path: Path,
    cache_dir: Path,
    start_date: str,
    end_date: str,
    warmup_sessions: int,
    publish_delay_seconds: int,
    workers: int,
    rebuild: bool,
) -> tuple[MarketContextResult, SectorIntelligenceResult, dict[str, Any]]:
    cache_dir.mkdir(parents=True, exist_ok=True)
    cache_key = _context_cache_key(
        data_dir=data_dir,
        sector_map_path=sector_map_path,
        start_date=start_date,
        end_date=end_date,
        warmup_sessions=warmup_sessions,
        publish_delay_seconds=publish_delay_seconds,
    )
    if not rebuild:
        cached = _load_context_cache(cache_dir, cache_key)
        if cached is not None:
            return cached

    index_path = data_dir / "NIFTY50_INDEX_stocks_indicators_5min.parquet"
    if not index_path.exists():
        raise FileNotFoundError(f"NIFTY index file not found: {index_path}")
    warmup_start, end_exclusive, sessions = _session_window(
        index_path,
        start_date=start_date,
        end_date=end_date,
        warmup_sessions=warmup_sessions,
    )
    universe = _load_v11_universe(data_dir)
    sector_map = load_sector_map(sector_map_path)
    stock_panel, available = load_stock_panel(
        data_dir,
        universe,
        start=warmup_start,
        end_exclusive=end_exclusive,
        workers=workers,
    )
    if stock_panel.empty:
        raise RuntimeError("no stock bars were loaded")
    index_bars = _read_index_bars(
        data_dir, start=warmup_start, end_exclusive=end_exclusive
    )

    market_config = MarketContextConfig(
        expected_universe_size=len(universe),
        publish_delay_seconds=publish_delay_seconds,
        relative_volume_sessions=20,
        relative_volume_min_sessions=20,
        regime_baseline_sessions=max(60, warmup_sessions),
        regime_min_sessions=20,
        min_market_coverage=0.70,
        min_sector_coverage=0.70,
    )
    market_context = MarketContextEngine(
        market_config, sector_map=sector_map
    ).compute(stock_panel, index_bars)

    stock_bar_rows = len(stock_panel)
    mapped_tickers = sorted(set(available).intersection(sector_map))
    mapped_panel = stock_panel.loc[stock_panel["ticker"].isin(mapped_tickers)].copy()
    del stock_panel
    sector_config = SectorIntelligenceConfig(
        # Deliberately use the full V11 universe.  The available static map is
        # only a small subset, so quality/coverage flags must fail closed rather
        # than making the mapped subset look like a complete NSE universe.
        expected_universe_size=max(1, len(universe)),
        expected_sector_count=max(1, len(set(sector_map.values()))),
        publish_delay_seconds=publish_delay_seconds,
        relative_volume_sessions=20,
        relative_volume_min_sessions=20,
        regime_baseline_sessions=max(60, warmup_sessions),
        regime_min_sessions=20,
        min_sector_members=5,
        min_sector_data_coverage=0.70,
        min_market_coverage=0.70,
        min_cross_sector_coverage=0.70,
    )
    sector_context = SectorIntelligenceEngine(
        sector_config, sector_map=sector_map
    ).compute(mapped_panel)

    output_start = pd.Timestamp(start_date, tz=IST)
    market_context = MarketContextResult(
        market=market_context.market.loc[
            market_context.market["timestamp"].ge(output_start)
        ].reset_index(drop=True),
        sectors=market_context.sectors.loc[
            market_context.sectors["timestamp"].ge(output_start)
        ].reset_index(drop=True),
    )
    sector_context = SectorIntelligenceResult(
        sectors=sector_context.sectors.loc[
            sector_context.sectors["timestamp"].ge(output_start)
        ].reset_index(drop=True),
        stocks=sector_context.stocks.loc[
            sector_context.stocks["timestamp"].ge(output_start)
        ].reset_index(drop=True),
    )

    market_context.market.to_parquet(cache_dir / "market_context_market.parquet")
    market_context.sectors.to_parquet(cache_dir / "market_context_sectors.parquet")
    sector_context.sectors.to_parquet(
        cache_dir / "sector_intelligence_sectors.parquet"
    )
    sector_context.stocks.to_parquet(
        cache_dir / "sector_intelligence_stocks.parquet"
    )
    manifest = {
        "cache_key": cache_key,
        "start_date": start_date,
        "end_date": end_date,
        "warmup_start": warmup_start.isoformat(),
        "warmup_sessions_requested": int(warmup_sessions),
        "sessions_loaded": sessions,
        "configured_universe_tickers": len(universe),
        "available_universe_tickers": len(available),
        "stock_bar_rows": stock_bar_rows,
        "sector_map_tickers": len(sector_map),
        "mapped_available_tickers": len(mapped_tickers),
        "sector_map_coverage_vs_configured_universe_pct": (
            100.0 * len(mapped_tickers) / len(universe) if universe else np.nan
        ),
        "mapped_sector_count": len(set(sector_map.values())),
        "index_sources": list(index_bars),
        "bank_nifty_source_available": bool(
            market_context.market.get(
                "bank_nifty_source_ready", pd.Series(dtype=bool)
            ).fillna(False).any()
        ),
        "midcap_source_available": bool(
            market_context.market.get(
                "midcap_source_ready", pd.Series(dtype=bool)
            ).fillna(False).any()
        ),
        "market_config": asdict(market_config),
        "sector_config": asdict(sector_config),
    }
    (cache_dir / "context_manifest.json").write_text(
        json.dumps(manifest, indent=2, default=str), encoding="utf-8"
    )
    return market_context, sector_context, manifest


def _profit_factor(values: Iterable[float]) -> float:
    pnl = pd.to_numeric(pd.Series(list(values)), errors="coerce").dropna()
    gain = float(pnl.loc[pnl.gt(0)].sum())
    loss = float(-pnl.loc[pnl.lt(0)].sum())
    if loss <= 0:
        return float("inf") if gain > 0 else np.nan
    return gain / loss


def trade_metrics(frame: pd.DataFrame, pnl_col: str) -> dict[str, float | int]:
    ordered = frame
    time_column = next(
        (
            column
            for column in (
                "signal_time_ist",
                "signal_time_v8",
                "entry_time_ist",
                "trade_date",
            )
            if column in frame
        ),
        None,
    )
    if time_column is not None:
        ordered = frame.assign(
            _economic_order_time=pd.to_datetime(
                frame[time_column], errors="coerce", utc=True
            )
        ).sort_values("_economic_order_time", kind="mergesort")
    pnl = pd.to_numeric(ordered.get(pnl_col), errors="coerce").dropna()
    if pnl.empty:
        return {
            "trades": 0,
            "net_pnl_rs": 0.0,
            "profit_factor": np.nan,
            "win_rate_pct": np.nan,
            "average_pnl_rs": np.nan,
            "max_trade_sequence_drawdown_rs": np.nan,
        }
    equity = pnl.cumsum()
    drawdown = equity - equity.cummax()
    return {
        "trades": int(len(pnl)),
        "net_pnl_rs": float(pnl.sum()),
        "profit_factor": float(_profit_factor(pnl)),
        "win_rate_pct": float(100.0 * pnl.gt(0).mean()),
        "average_pnl_rs": float(pnl.mean()),
        "max_trade_sequence_drawdown_rs": float(drawdown.min()),
    }


def _candidate_checksum(frame: pd.DataFrame) -> str:
    if "candidate_id" in frame:
        values = frame["candidate_id"].fillna("").astype(str)
    else:
        values = (
            frame.get("ticker", "").astype(str)
            + "|"
            + frame.get("side", "").astype(str)
            + "|"
            + frame.get("setup", "").astype(str)
            + "|"
            + frame.get("signal_time_ist", "").astype(str)
        )
    return sha256("\n".join(values.tolist()).encode("utf-8")).hexdigest()


def _resolve_pnl_column(frame: pd.DataFrame) -> str:
    for column in (
        "v6_net_pnl_rs",
        "net_pnl_rs",
        "pnl_rs",
        "pnl",
    ):
        if column in frame:
            return column
    raise ValueError("resolved V11 trades contain no supported P&L column")


def enrich_candidates(
    frame: pd.DataFrame,
    *,
    market_context: MarketContextResult,
    sector_context: SectorIntelligenceResult,
    sector_map: Mapping[str, str],
    fallback_decision_delay_seconds: int,
) -> pd.DataFrame:
    if frame.empty:
        return frame.copy()
    signal_time_column = next(
        (
            column
            for column in (
                "signal_time_ist",
                "signal_time_v8",
                "bar_time_ist",
                "signal_datetime",
            )
            if column in frame
        ),
        None,
    )
    if signal_time_column is None:
        raise ValueError("candidate artifact has no signal timestamp")
    original_checksum = _candidate_checksum(frame)
    decision_time = pd.Series(pd.NaT, index=frame.index, dtype=f"datetime64[ns, {IST}]")
    decision_source = pd.Series("signal_plus_fallback_delay", index=frame.index)
    if "decision_ready_at_ist" in frame:
        explicit = _to_ist(frame["decision_ready_at_ist"])
        has_explicit = explicit.notna()
        decision_time.loc[has_explicit] = explicit.loc[has_explicit]
        decision_source.loc[has_explicit] = "decision_ready_at_ist"
    fallback = _to_ist(frame[signal_time_column]) + pd.Timedelta(
        seconds=int(fallback_decision_delay_seconds)
    )
    decision_time = decision_time.fillna(fallback)
    work = frame.copy()
    work["context_decision_time_ist"] = decision_time
    work["context_decision_time_source"] = decision_source
    enriched = attach_context_asof(
        work,
        market_context,
        candidate_time_col="context_decision_time_ist",
        ticker_col="ticker",
        sector_map=sector_map,
        prefix="mce_",
        max_staleness_minutes=7,
    )
    enriched = attach_sector_intelligence_asof(
        enriched,
        sector_context,
        candidate_time_col="context_decision_time_ist",
        ticker_col="ticker",
        prefix="si_",
        max_staleness_minutes=7,
    )
    if len(enriched) != len(frame):
        raise AssertionError("context attachment changed candidate count")
    if _candidate_checksum(enriched) != original_checksum:
        raise AssertionError("context attachment changed candidate identity/order")
    decision = _to_ist(enriched["context_decision_time_ist"])
    for available_column in (
        "mce_available_at",
        "si_stock_available_at",
        "si_sector_available_at",
    ):
        if available_column not in enriched:
            continue
        available = _to_ist(enriched[available_column])
        if (available.notna() & decision.notna() & available.gt(decision)).any():
            raise AssertionError(
                f"point-in-time violation: {available_column} is after decision time"
            )
    return enriched


def feature_audit(
    trades: pd.DataFrame,
    feature_columns: Sequence[str],
    *,
    pnl_col: str,
    group_columns: Sequence[str] = ("side",),
    minimum_rows: int = 4,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    group_specs: list[tuple[str, pd.DataFrame]] = [("ALL", trades)]
    for group_column in group_columns:
        if group_column not in trades:
            continue
        for value, group in trades.groupby(group_column, dropna=False, sort=True):
            group_specs.append((f"{group_column}={value}", group))
    for group_name, group in group_specs:
        pnl = pd.to_numeric(group[pnl_col], errors="coerce")
        for feature in feature_columns:
            if feature not in group:
                continue
            values = pd.to_numeric(group[feature], errors="coerce").replace(
                [np.inf, -np.inf], np.nan
            )
            valid = values.notna() & pnl.notna()
            record: dict[str, Any] = {
                "group": group_name,
                "feature": feature,
                "group_rows": int(len(group)),
                "valid_rows": int(valid.sum()),
                "coverage_pct": float(100.0 * valid.mean()) if len(group) else np.nan,
                "unique_values": int(values.loc[valid].nunique()),
                "spearman_pnl": np.nan,
                "median_threshold": np.nan,
                "low_rows": 0,
                "low_net_pnl_rs": np.nan,
                "low_profit_factor": np.nan,
                "low_average_pnl_rs": np.nan,
                "high_rows": 0,
                "high_net_pnl_rs": np.nan,
                "high_profit_factor": np.nan,
                "high_average_pnl_rs": np.nan,
                "high_minus_low_average_pnl_rs": np.nan,
            }
            if valid.sum() < minimum_rows or values.loc[valid].nunique() < 2:
                rows.append(record)
                continue
            x = values.loc[valid]
            y = pnl.loc[valid]
            threshold = float(x.median())
            high = x.ge(threshold)
            low = ~high
            high_pnl = y.loc[high]
            low_pnl = y.loc[low]
            record.update(
                {
                    "spearman_pnl": float(x.corr(y, method="spearman")),
                    "median_threshold": threshold,
                    "low_rows": int(len(low_pnl)),
                    "low_net_pnl_rs": float(low_pnl.sum()),
                    "low_profit_factor": float(_profit_factor(low_pnl)),
                    "low_average_pnl_rs": float(low_pnl.mean())
                    if len(low_pnl)
                    else np.nan,
                    "high_rows": int(len(high_pnl)),
                    "high_net_pnl_rs": float(high_pnl.sum()),
                    "high_profit_factor": float(_profit_factor(high_pnl)),
                    "high_average_pnl_rs": float(high_pnl.mean())
                    if len(high_pnl)
                    else np.nan,
                    "high_minus_low_average_pnl_rs": float(
                        high_pnl.mean() - low_pnl.mean()
                    )
                    if len(high_pnl) and len(low_pnl)
                    else np.nan,
                }
            )
            rows.append(record)
    return pd.DataFrame(rows)


def chronological_shadow_audit(
    trades: pd.DataFrame,
    feature_columns: Sequence[str],
    *,
    pnl_col: str,
    minimum_train_rows: int = 6,
    minimum_holdout_rows: int = 2,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    work = trades.copy()
    time_column = next(
        column
        for column in ("signal_time_ist", "signal_time_v8", "trade_date")
        if column in work
    )
    work["_context_session"] = _to_ist(work[time_column]).dt.strftime("%Y-%m-%d")
    sessions = sorted(work["_context_session"].dropna().unique().tolist())
    if len(sessions) < 5:
        return pd.DataFrame(), {
            "sessions": sessions,
            "reason": "fewer than five sessions",
        }
    discovery_sessions = sessions[:-2]
    holdout_sessions = sessions[-2:]
    rows: list[dict[str, Any]] = []
    unique_sides = (
        work["side"].dropna().astype(str).unique().tolist()
        if "side" in work
        else []
    )
    side_groups = [
        (unique_sides[0], work) if len(unique_sides) == 1 else ("ALL", work)
    ]
    if "side" in work and len(unique_sides) > 1:
        side_groups.extend(
            (str(side), group)
            for side, group in work.groupby("side", dropna=False, sort=True)
        )
    for side, group in side_groups:
        train = group.loc[group["_context_session"].isin(discovery_sessions)]
        holdout = group.loc[group["_context_session"].isin(holdout_sessions)]
        for feature in feature_columns:
            if feature not in group:
                continue
            x_train = pd.to_numeric(train[feature], errors="coerce").replace(
                [np.inf, -np.inf], np.nan
            )
            y_train = pd.to_numeric(train[pnl_col], errors="coerce")
            train_valid = x_train.notna() & y_train.notna()
            x_holdout = pd.to_numeric(holdout[feature], errors="coerce").replace(
                [np.inf, -np.inf], np.nan
            )
            y_holdout = pd.to_numeric(holdout[pnl_col], errors="coerce")
            holdout_valid = x_holdout.notna() & y_holdout.notna()
            if (
                train_valid.sum() < minimum_train_rows
                or holdout_valid.sum() < minimum_holdout_rows
                or x_train.loc[train_valid].nunique() < 2
            ):
                continue
            correlation = x_train.loc[train_valid].corr(
                y_train.loc[train_valid], method="spearman"
            )
            if not np.isfinite(correlation) or abs(correlation) <= 1e-12:
                continue
            threshold = float(x_train.loc[train_valid].median())
            direction = "HIGH" if correlation > 0 else "LOW"
            eligible = holdout.loc[holdout_valid].copy()
            eligible_values = x_holdout.loc[holdout_valid]
            selected_mask = (
                eligible_values.ge(threshold)
                if direction == "HIGH"
                else eligible_values.le(threshold)
            )
            selected = eligible.loc[selected_mask]
            if selected.empty:
                continue
            baseline_metrics = trade_metrics(eligible, pnl_col)
            selected_metrics = trade_metrics(selected, pnl_col)
            rows.append(
                {
                    "side": side,
                    "feature": feature,
                    "discovery_sessions": ",".join(discovery_sessions),
                    "holdout_sessions": ",".join(holdout_sessions),
                    "train_rows": int(train_valid.sum()),
                    "train_spearman_pnl": float(correlation),
                    "direction": direction,
                    "threshold": threshold,
                    "holdout_feature_valid_rows": int(len(eligible)),
                    "holdout_selected_rows": int(len(selected)),
                    "holdout_keep_pct": float(100.0 * len(selected) / len(eligible)),
                    "holdout_baseline_net_pnl_rs": baseline_metrics["net_pnl_rs"],
                    "holdout_baseline_profit_factor": baseline_metrics[
                        "profit_factor"
                    ],
                    "holdout_baseline_average_pnl_rs": baseline_metrics[
                        "average_pnl_rs"
                    ],
                    "holdout_selected_net_pnl_rs": selected_metrics["net_pnl_rs"],
                    "holdout_selected_profit_factor": selected_metrics[
                        "profit_factor"
                    ],
                    "holdout_selected_average_pnl_rs": selected_metrics[
                        "average_pnl_rs"
                    ],
                    "holdout_delta_average_pnl_rs": float(
                        selected_metrics["average_pnl_rs"]
                        - baseline_metrics["average_pnl_rs"]
                    ),
                }
            )
    return pd.DataFrame(rows), {
        "sessions": sessions,
        "discovery_sessions": discovery_sessions,
        "holdout_sessions": holdout_sessions,
        "warning": "exploratory five-session shadow study; not promotion evidence",
    }


def _stage_summary(frame: pd.DataFrame, stage: str) -> dict[str, Any]:
    return {
        "stage": stage,
        "rows": int(len(frame)),
        "candidate_checksum": _candidate_checksum(frame) if not frame.empty else "",
        "market_context_pct": float(
            100.0 * frame.get("mce_timestamp", pd.Series(index=frame.index)).notna().mean()
        )
        if len(frame)
        else np.nan,
        "sector_stock_snapshot_pct": float(
            100.0
            * frame.get("si_stock_timestamp", pd.Series(index=frame.index)).notna().mean()
        )
        if len(frame)
        else np.nan,
        "sector_mapped_pct": float(
            100.0
            * pd.to_numeric(
                frame.get(
                    "si_stock_sector_mapped_flag", pd.Series(index=frame.index)
                ),
                errors="coerce",
            ).fillna(0.0).mean()
        )
        if len(frame)
        else np.nan,
    }


def _format_metric(value: Any, digits: int = 2) -> str:
    try:
        numeric = float(value)
    except Exception:
        return "NA"
    if np.isinf(numeric):
        return "inf"
    if not np.isfinite(numeric):
        return "NA"
    return f"{numeric:,.{digits}f}"


def _context_quality_summary(frame: pd.DataFrame) -> dict[str, float]:
    def mean_pct(column: str) -> float:
        values = pd.to_numeric(frame.get(column), errors="coerce")
        return float(100.0 * values.mean()) if values.notna().any() else np.nan

    market_coverage = pd.to_numeric(
        frame.get("mce_market_coverage"), errors="coerce"
    )
    return {
        "market_fresh_coverage_mean_pct": mean_pct("mce_market_coverage"),
        "market_fresh_coverage_min_pct": (
            float(100.0 * market_coverage.min())
            if market_coverage.notna().any()
            else np.nan
        ),
        "nifty_source_ready_pct": mean_pct("mce_nifty_source_ready"),
        "bank_nifty_source_ready_pct": mean_pct("mce_bank_nifty_source_ready"),
        "midcap_source_ready_pct": mean_pct("mce_midcap_source_ready"),
        "full_context_complete_pct": mean_pct("mce_context_complete"),
        "sector_mapping_coverage_mean_pct": mean_pct(
            "mce_sector_mapping_coverage"
        ),
    }


def _write_report(
    path: Path,
    *,
    start_date: str,
    end_date: str,
    baseline_metrics: Mapping[str, Any],
    all_side_metrics: Mapping[str, Any],
    manifest: Mapping[str, Any],
    quality_summary: Mapping[str, Any],
    stages: pd.DataFrame,
    feature_audit_frame: pd.DataFrame,
    shadow_frame: pd.DataFrame,
    shadow_manifest: Mapping[str, Any],
    setup_summary: pd.DataFrame,
    model_features: Sequence[str],
) -> None:
    lines = [
        "# V11 one-week Market/Sector Context study",
        "",
        f"Evaluation window: **{start_date} to {end_date}**.",
        "",
        "The authoritative V11 replay candidate population, entry engine, exits, costs,",
        "and P&L are frozen. Context is attached after resolution and never generates",
        "or removes a trade in the baseline artifact.",
        "",
        "## Baseline (LONG-only primary study)",
        "",
        f"- Trades: {baseline_metrics['trades']}",
        f"- Net P&L: Rs {_format_metric(baseline_metrics['net_pnl_rs'])}",
        f"- Profit factor: {_format_metric(baseline_metrics['profit_factor'], 3)}",
        f"- Win rate: {_format_metric(baseline_metrics['win_rate_pct'])}%",
        f"- Average P&L/trade: Rs {_format_metric(baseline_metrics['average_pnl_rs'])}",
        f"- Trade-sequence max drawdown: Rs {_format_metric(baseline_metrics['max_trade_sequence_drawdown_rs'])}",
        f"- Frozen all-side appendix: {all_side_metrics['trades']} trades, "
        f"Rs {_format_metric(all_side_metrics['net_pnl_rs'])} net, "
        f"PF {_format_metric(all_side_metrics['profit_factor'], 3)}",
        "",
        "## Context coverage",
        "",
        f"- V11 configured universe: {manifest.get('configured_universe_tickers', 'NA')} tickers",
        f"- Universe files with data: {manifest.get('available_universe_tickers', 'NA')}",
        f"- Static sector map: {manifest.get('sector_map_tickers', 'NA')} tickers",
        f"- Mapped available tickers: {manifest.get('mapped_available_tickers', 'NA')}",
        f"- Sector map coverage vs full V11 universe: "
        f"{_format_metric(manifest.get('sector_map_coverage_vs_configured_universe_pct'))}%",
        f"- Bank Nifty source available: {manifest.get('bank_nifty_source_available', False)}",
        f"- Midcap source available: {manifest.get('midcap_source_available', False)}",
        f"- Mean fresh market breadth coverage: "
        f"{_format_metric(quality_summary.get('market_fresh_coverage_mean_pct'))}% "
        f"(minimum {_format_metric(quality_summary.get('market_fresh_coverage_min_pct'))}%)",
        f"- Nifty source ready at candidate timestamps: "
        f"{_format_metric(quality_summary.get('nifty_source_ready_pct'))}%",
        f"- Full requested context complete: "
        f"{_format_metric(quality_summary.get('full_context_complete_pct'))}% "
        "(Bank Nifty/Midcap absent and sector map incomplete)",
        f"- Numerical model candidates: {len(model_features)}",
        "",
    ]
    if not stages.empty:
        lines.extend(
            [
                "| Stage | Rows | Market context | Sector snapshot | Sector mapped |",
                "|---|---:|---:|---:|---:|",
            ]
        )
        for row in stages.itertuples(index=False):
            lines.append(
                f"| {row.stage} | {row.rows} | {_format_metric(row.market_context_pct)}% "
                f"| {_format_metric(row.sector_stock_snapshot_pct)}% "
                f"| {_format_metric(row.sector_mapped_pct)}% |"
            )
        lines.append("")

    lines.extend(
        [
            "## LONG setup results (unchanged baseline trades)",
            "",
            "| Setup | Side | Trades | Net Rs | PF | Win % | Avg Rs |",
            "|---|---|---:|---:|---:|---:|---:|",
        ]
    )
    for row in setup_summary.itertuples(index=False):
        lines.append(
            f"| {row.setup} | {row.side} | {row.trades} "
            f"| {_format_metric(row.net_pnl_rs)} "
            f"| {_format_metric(row.profit_factor, 3)} "
            f"| {_format_metric(row.win_rate_pct)} "
            f"| {_format_metric(row.average_pnl_rs)} |"
        )

    lines.extend(
        [
            "",
            "## Exploratory context findings",
            "",
            "The following are diagnostics, not approved setup filters. The descriptive",
            "feature table uses the same one-week outcomes, so it is in-sample. The shadow",
            "table fixes direction/threshold on the first three sessions and applies it to",
            "the final two, but the sample remains extremely small and many features are tried.",
            "",
        ]
    )
    descriptive = feature_audit_frame.loc[
        feature_audit_frame["group"].eq("ALL")
        & feature_audit_frame["spearman_pnl"].notna()
    ].copy()
    if not descriptive.empty:
        descriptive["_abs"] = descriptive["spearman_pnl"].abs()
        descriptive = descriptive.sort_values("_abs", ascending=False).head(10)
        lines.extend(
            [
                "Strongest one-week descriptive associations:",
                "",
                "| Feature | Valid n | Coverage | Spearman(P&L) | High-low avg Rs |",
                "|---|---:|---:|---:|---:|",
            ]
        )
        for row in descriptive.itertuples(index=False):
            lines.append(
                f"| {row.feature} | {row.valid_rows} | {_format_metric(row.coverage_pct)}% "
                f"| {_format_metric(row.spearman_pnl, 3)} "
                f"| {_format_metric(row.high_minus_low_average_pnl_rs)} |"
            )
        lines.append("")

    if not shadow_frame.empty:
        top_shadow = shadow_frame.sort_values(
            "holdout_delta_average_pnl_rs", ascending=False
        ).head(10)
        lines.extend(
            [
                "Chronological two-session shadow results (exploratory):",
                "",
                "| Side | Feature | Direction | Holdout n | Kept | Baseline avg | Selected avg | Delta |",
                "|---|---|---|---:|---:|---:|---:|---:|",
            ]
        )
        for row in top_shadow.itertuples(index=False):
            lines.append(
                f"| {row.side} | {row.feature} | {row.direction} "
                f"| {row.holdout_feature_valid_rows} | {row.holdout_selected_rows} "
                f"| {_format_metric(row.holdout_baseline_average_pnl_rs)} "
                f"| {_format_metric(row.holdout_selected_average_pnl_rs)} "
                f"| {_format_metric(row.holdout_delta_average_pnl_rs)} |"
            )
        lines.append("")

    lines.extend(
        [
            "## Decision",
            "",
            "This run creates a context-enriched, model-ready V11 research artifact but",
            "does **not** authorize a live gate or claim improved profitability. Promotion",
            "requires a longer pre-period, purged walk-forward testing, multiple-testing",
            "control, a complete point-in-time NSE sector master, and live shadow parity.",
            "Bank Nifty and Midcap scores remain missing because those index bars are absent;",
            "they are never imputed to zero or relabelled from an equity basket.",
            "",
            f"Shadow split: `{json.dumps(shadow_manifest, default=str)}`",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _read_artifact(path: Path, start_date: str, end_date: str) -> pd.DataFrame:
    if not path.exists() or path.stat().st_size <= 2:
        return pd.DataFrame()
    frame = pd.read_csv(path, low_memory=False)
    time_column = next(
        (
            column
            for column in (
                "signal_time_ist",
                "signal_time_v8",
                "trade_date",
                "date",
            )
            if column in frame
        ),
        None,
    )
    if time_column is None:
        return frame
    parsed = pd.to_datetime(frame[time_column], errors="coerce")
    dates = parsed.dt.strftime("%Y-%m-%d")
    return frame.loc[dates.between(start_date, end_date)].reset_index(drop=True)


def _validate_frozen_v11_baseline(
    baseline_dir: Path,
    trades: pd.DataFrame,
    *,
    start_date: str,
    end_date: str,
) -> dict[str, Any]:
    inputs_path = baseline_dir / "inputs.txt"
    if not inputs_path.exists():
        raise FileNotFoundError(
            f"V11 baseline provenance is missing: {inputs_path}"
        )
    entries: dict[str, str] = {}
    for line in inputs_path.read_text(encoding="utf-8").splitlines():
        if "=" in line:
            key, value = line.split("=", 1)
            entries[key.strip()] = value.strip()
    required = {
        "mode": "historical_all_available",
        "first_date": start_date,
        "last_date": end_date,
        "entry_fill_model": "ltp_on_signal_1m_open",
        "selected_strategy_profile": "final_setup_conf",
        "ab_gate_profile": "quality_top_slot",
    }
    mismatches = {
        key: {"expected": expected, "actual": entries.get(key)}
        for key, expected in required.items()
        if entries.get(key) != expected
    }
    if "statutory" not in entries.get("pnl_model", "").lower():
        mismatches["pnl_model"] = {
            "expected": "PAPER_TRUE NSE statutory costs",
            "actual": entries.get("pnl_model"),
        }
    if mismatches:
        raise RuntimeError(
            "baseline is not the canonical final-conf/statutory V11 replay: "
            + json.dumps(mismatches, default=str)
        )

    import final_setup_conf_v11_working as final_conf

    approved_setups = sorted(final_conf.FINAL_SETUP_CONF)
    observed_setups = sorted(
        trades.get("setup", pd.Series(dtype="string")).dropna().astype(str).unique()
    )
    unexpected = sorted(set(observed_setups).difference(approved_setups))
    if unexpected:
        raise RuntimeError(
            f"baseline contains setups outside final_setup_conf_v11_working: {unexpected}"
        )
    conf_path = Path(final_conf.__file__).resolve()
    return {
        "inputs_path": str(inputs_path),
        "mode": entries["mode"],
        "entry_fill_model": entries["entry_fill_model"],
        "selected_strategy_profile": entries["selected_strategy_profile"],
        "ab_gate_profile": entries["ab_gate_profile"],
        "pnl_model": entries["pnl_model"],
        "final_setup_conf_module": "final_setup_conf_v11_working",
        "final_setup_conf_sha256": _sha256_file(conf_path),
        "approved_setup_count": len(approved_setups),
        "approved_setups": approved_setups,
        "observed_setups": observed_setups,
    }


def run(args: argparse.Namespace) -> int:
    baseline_dir = Path(args.baseline_out).resolve()
    output_dir = Path(args.output).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    data_dir = Path(args.data_5m_dir).resolve()
    sector_map_path = Path(args.sector_map).resolve()
    sector_map = load_sector_map(sector_map_path)

    trades_path = baseline_dir / "trades.csv"
    signals_path = baseline_dir / "historical_all_available_selected_strategy_signals.csv"
    candidates_path = (
        baseline_dir / "historical_all_available_pre_dedupe_live_candidates.csv"
    )
    if not trades_path.exists():
        raise FileNotFoundError(
            f"authoritative V11 resolved trades are missing: {trades_path}"
        )
    trades = _read_artifact(trades_path, args.start_date, args.end_date)
    signals = _read_artifact(signals_path, args.start_date, args.end_date)
    candidates = _read_artifact(candidates_path, args.start_date, args.end_date)
    if trades.empty:
        raise RuntimeError("the V11 replay produced no resolved trades in the week")
    baseline_validation = _validate_frozen_v11_baseline(
        baseline_dir,
        trades,
        start_date=args.start_date,
        end_date=args.end_date,
    )

    market_context, sector_context, manifest = build_context(
        data_dir=data_dir,
        sector_map_path=sector_map_path,
        cache_dir=output_dir / "context_cache",
        start_date=args.start_date,
        end_date=args.end_date,
        warmup_sessions=args.warmup_sessions,
        publish_delay_seconds=args.publish_delay_seconds,
        workers=args.workers,
        rebuild=args.rebuild_context,
    )
    enriched_trades = enrich_candidates(
        trades,
        market_context=market_context,
        sector_context=sector_context,
        sector_map=sector_map,
        fallback_decision_delay_seconds=args.decision_delay_seconds,
    )
    enriched_signals = enrich_candidates(
        signals,
        market_context=market_context,
        sector_context=sector_context,
        sector_map=sector_map,
        fallback_decision_delay_seconds=args.decision_delay_seconds,
    ) if not signals.empty else signals
    enriched_candidates = enrich_candidates(
        candidates,
        market_context=market_context,
        sector_context=sector_context,
        sector_map=sector_map,
        fallback_decision_delay_seconds=args.decision_delay_seconds,
    ) if not candidates.empty else candidates

    side_values = enriched_trades.get(
        "side", pd.Series("", index=enriched_trades.index)
    ).astype(str).str.upper()
    analysis_trades = enriched_trades.loc[side_values.eq("LONG")].copy()
    if analysis_trades.empty:
        raise RuntimeError(
            "the frozen V11 week has no LONG trades for the requested long-only study"
        )

    market_alpha_features = [
        f"mce_{column}" for column in context_feature_columns(market_context)
    ]
    market_all_features = [
        f"mce_{column}"
        for column in context_feature_columns(
            market_context, include_quality_metadata=True
        )
    ]
    sector_alpha_features = sector_intelligence_feature_columns(sector_context)
    sector_all_features = sector_intelligence_feature_columns(
        sector_context, include_quality_metadata=True
    )
    alpha_features = [*market_alpha_features, *sector_alpha_features]
    model_features = [
        column
        for column in [*market_all_features, *sector_all_features]
        if column in analysis_trades
        and pd.api.types.is_numeric_dtype(analysis_trades[column])
    ]
    audit_features = [column for column in alpha_features if column in model_features]
    quality_features = [column for column in model_features if column not in audit_features]
    pnl_col = _resolve_pnl_column(enriched_trades)
    baseline_metrics = trade_metrics(analysis_trades, pnl_col)
    all_side_metrics = trade_metrics(enriched_trades, pnl_col)

    audit = feature_audit(
        analysis_trades,
        audit_features,
        pnl_col=pnl_col,
        group_columns=("side", "setup"),
    )
    shadow, shadow_manifest = chronological_shadow_audit(
        analysis_trades, audit_features, pnl_col=pnl_col
    )
    setup_rows: list[dict[str, Any]] = []
    for (setup, side), group in analysis_trades.groupby(
        ["setup", "side"], dropna=False, sort=True
    ):
        setup_rows.append(
            {"setup": setup, "side": side, **trade_metrics(group, pnl_col)}
        )
    setup_summary = pd.DataFrame(setup_rows)
    stages = pd.DataFrame(
        [
            _stage_summary(enriched_candidates, "pre_dedupe_live_candidates"),
            _stage_summary(enriched_signals, "selected_strategy_signals"),
            _stage_summary(enriched_trades, "resolved_trades_all_sides"),
            _stage_summary(analysis_trades, "resolved_trades_long_primary"),
        ]
    )
    quality_source = (
        enriched_candidates if not enriched_candidates.empty else enriched_trades
    )
    quality_summary = _context_quality_summary(quality_source)

    enriched_trades.to_csv(output_dir / "v11_context_enriched_trades.csv", index=False)
    analysis_trades.to_csv(
        output_dir / "v11_context_enriched_long_trades.csv", index=False
    )
    if not enriched_candidates.empty:
        enriched_candidates.to_csv(
            output_dir / "v11_context_enriched_pre_dedupe_candidates.csv",
            index=False,
        )
        candidate_sides = enriched_candidates.get(
            "side", pd.Series("", index=enriched_candidates.index)
        ).astype(str).str.upper()
        enriched_candidates.loc[candidate_sides.eq("LONG")].to_csv(
            output_dir / "v11_context_enriched_long_pre_dedupe_candidates.csv",
            index=False,
        )
    if not enriched_signals.empty:
        enriched_signals.to_csv(
            output_dir / "v11_context_enriched_selected_signals.csv", index=False
        )
    audit.to_csv(output_dir / "v11_context_feature_audit.csv", index=False)
    shadow.to_csv(output_dir / "v11_context_chronological_shadow.csv", index=False)
    setup_summary.to_csv(output_dir / "v11_context_setup_summary.csv", index=False)
    stages.to_csv(output_dir / "v11_context_stage_coverage.csv", index=False)
    (output_dir / "v11_context_model_features.json").write_text(
        json.dumps(
            {
                "baseline_columns": list(MODEL_BASELINE_COLUMNS),
                "context_columns": model_features,
                "context_alpha_columns": audit_features,
                "context_quality_columns": quality_features,
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    run_manifest = {
        "start_date": args.start_date,
        "end_date": args.end_date,
        "baseline_dir": str(baseline_dir),
        "baseline_validation": baseline_validation,
        "baseline_trade_checksum": _candidate_checksum(trades),
        "enriched_trade_checksum": _candidate_checksum(enriched_trades),
        "baseline_candidate_checksum": (
            _candidate_checksum(candidates) if not candidates.empty else ""
        ),
        "enriched_candidate_checksum": (
            _candidate_checksum(enriched_candidates)
            if not enriched_candidates.empty
            else ""
        ),
        "candidate_population_preserved": bool(
            len(trades) == len(enriched_trades)
            and _candidate_checksum(trades) == _candidate_checksum(enriched_trades)
            and len(candidates) == len(enriched_candidates)
            and (
                candidates.empty
                or _candidate_checksum(candidates)
                == _candidate_checksum(enriched_candidates)
            )
        ),
        "pnl_column": pnl_col,
        "primary_analysis_side": "LONG",
        "baseline_metrics": baseline_metrics,
        "all_side_appendix_metrics": all_side_metrics,
        "context_manifest": manifest,
        "context_quality_summary": quality_summary,
        "shadow_manifest": shadow_manifest,
        "context_feature_count": len(model_features),
        "context_alpha_feature_count": len(audit_features),
        "context_quality_feature_count": len(quality_features),
    }
    (output_dir / "v11_context_run_manifest.json").write_text(
        json.dumps(run_manifest, indent=2, default=str), encoding="utf-8"
    )
    _write_report(
        output_dir / "V11_CONTEXT_WEEK_REPORT.md",
        start_date=args.start_date,
        end_date=args.end_date,
        baseline_metrics=baseline_metrics,
        all_side_metrics=all_side_metrics,
        manifest=manifest,
        quality_summary=quality_summary,
        stages=stages,
        feature_audit_frame=audit,
        shadow_frame=shadow,
        shadow_manifest=shadow_manifest,
        setup_summary=setup_summary,
        model_features=model_features,
    )
    print(json.dumps(run_manifest, indent=2, default=str))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Attach point-in-time context to a frozen V11 replay week"
    )
    parser.add_argument("--baseline-out", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--data-5m-dir", default=str(DEFAULT_DATA_5M))
    parser.add_argument("--sector-map", default=str(DEFAULT_SECTOR_MAP))
    parser.add_argument("--warmup-sessions", type=int, default=60)
    parser.add_argument(
        "--publish-delay-seconds",
        type=int,
        default=60,
        help="Delay from completed bar timestamp to context availability",
    )
    parser.add_argument(
        "--decision-delay-seconds",
        type=int,
        default=60,
        help=(
            "Fallback delay after signal timestamp when decision_ready_at_ist is absent"
        ),
    )
    parser.add_argument("--workers", type=int, default=12)
    parser.add_argument("--rebuild-context", action="store_true")
    return parser


def main() -> int:
    return run(build_parser().parse_args())


if __name__ == "__main__":
    raise SystemExit(main())
