"""
V7 causality audit for VWAP, day-value-so-far, and market-return features.

The live data directory can contain missing or stale indicator columns. This
report checks the stored source values, then verifies the actual V7 prepared
feature path recomputes the critical features causally from 5-minute OHLCV.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

import avwap_5min_ID_v2_backtesting as v2
from eqidv2_runtime_paths import RUNTIME_STATUS_DIR, runtime_dir


SESSION_NAME = "V7 Causality Audit"
SESSION_SLUG = "v7_causality_audit"
SESSION_ROOT = runtime_dir(SESSION_SLUG)
LATEST_DIR = SESSION_ROOT / "latest"
REPORTS_DIR = SESSION_ROOT / "reports"
HEARTBEAT_DIR = SESSION_ROOT / "heartbeat"

DEFAULT_LIVE_5M_DIR = Path(
    os.getenv(
        "EQIDV2_V7_CAUSALITY_DATA_ROOT",
        os.getenv(
            "EQIDV2_ID5MIN_V7_LIVE_5M_DIR",
            str(runtime_dir("stocks_indicators_5min_eq_live")),
        ),
    )
)
MARKET_TICKERS = tuple(getattr(v2, "MARKET_TICKERS", ("NIFTYBEES", "NIFTY")))
TICKER_FILE_ALIASES = {
    "NIFTY 50": ("NIFTY_50", "NIFTY50", "NIFTY"),
    "NIFTY_50": ("NIFTY_50", "NIFTY50", "NIFTY"),
    "NIFTY50": ("NIFTY50", "NIFTY_50", "NIFTY"),
}

VWAP_ABS_TOL = 0.01
VWAP_BPS_TOL = 1.0
DAY_VALUE_ABS_TOL = 1.0
DAY_VALUE_REL_TOL = 1e-6
MRET_ABS_TOL = 1e-9

for _path in (SESSION_ROOT, LATEST_DIR, REPORTS_DIR, HEARTBEAT_DIR, RUNTIME_STATUS_DIR):
    _path.mkdir(parents=True, exist_ok=True)


def _now_ist() -> pd.Timestamp:
    return pd.Timestamp.now(tz="Asia/Kolkata")


def _fmt_ts(value: Any) -> str:
    ts = pd.to_datetime(value, errors="coerce")
    if pd.isna(ts):
        return ""
    if ts.tzinfo is None:
        ts = ts.tz_localize("Asia/Kolkata")
    else:
        ts = ts.tz_convert("Asia/Kolkata")
    offset = ts.strftime("%z")
    return f"{ts.strftime('%Y-%m-%d %H:%M:%S')}{offset[:3]}:{offset[3:]}"


def _json_default(value: Any) -> Any:
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        x = float(value)
        return None if not np.isfinite(x) else x
    if isinstance(value, (pd.Timestamp,)):
        return _fmt_ts(value)
    if isinstance(value, Path):
        return str(value)
    return str(value)


def _write_text_atomic(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f"{path.name}.tmp.{os.getpid()}")
    tmp.write_text(text, encoding="utf-8")
    tmp.replace(path)


def _write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    _write_text_atomic(path, json.dumps(payload, indent=2, sort_keys=True, default=_json_default))


def _write_status(state: str, **extra: Any) -> None:
    payload = {
        "session": SESSION_NAME,
        "session_slug": SESSION_SLUG,
        "status": state,
        "pid": os.getpid(),
        "updated_at_ist": _fmt_ts(_now_ist()),
        **extra,
    }
    text = "\n".join(f"{k}={v}" for k, v in payload.items()) + "\n"
    _write_text_atomic(RUNTIME_STATUS_DIR / f"{SESSION_SLUG}.status", text)
    _write_text_atomic(RUNTIME_STATUS_DIR / f"{SESSION_SLUG}.heartbeat", text)
    _write_json_atomic(HEARTBEAT_DIR / f"{SESSION_SLUG}.status.json", payload)
    _write_json_atomic(HEARTBEAT_DIR / f"{SESSION_SLUG}.heartbeat.json", payload)


def _safe_float(value: Any, default: float = np.nan) -> float:
    try:
        x = float(value)
    except Exception:
        return default
    return x if np.isfinite(x) else default


def _ticker_file_candidates(ticker: str) -> list[str]:
    key = str(ticker).strip().upper().replace(".NS", "")
    candidates = [key, *TICKER_FILE_ALIASES.get(key, ())]
    if " " in key:
        candidates.append(key.replace(" ", "_"))
        candidates.append(key.replace(" ", ""))
    seen: set[str] = set()
    out: list[str] = []
    for candidate in candidates:
        candidate = candidate.strip().upper()
        if candidate and candidate not in seen:
            seen.add(candidate)
            out.append(candidate)
    return out


def _resolve_ticker_path(ticker: str, data_root: Path) -> Path:
    candidates = _ticker_file_candidates(ticker)
    for candidate in candidates:
        path = data_root / f"{candidate}_stocks_indicators_5min.parquet"
        if path.exists():
            return path
    fallback = candidates[0] if candidates else str(ticker).strip().upper()
    return data_root / f"{fallback}_stocks_indicators_5min.parquet"


def _normalise_dates(df: pd.DataFrame, path: Path) -> pd.DataFrame:
    out = df.copy()
    if "date" not in out.columns:
        if isinstance(out.index, pd.DatetimeIndex):
            out = out.reset_index().rename(columns={out.index.name or "index": "date"})
        else:
            raise ValueError(f"{path} has no date column")
    out["date"] = pd.to_datetime(out["date"], errors="coerce")
    out = out[out["date"].notna()].copy()
    if out.empty:
        return out
    if getattr(out["date"].dt, "tz", None) is None:
        out["date"] = out["date"].dt.tz_localize("Asia/Kolkata")
    else:
        out["date"] = out["date"].dt.tz_convert("Asia/Kolkata")
    out = out.sort_values("date").reset_index(drop=True)
    out["date_only"] = out["date"].dt.date
    return out


def _read_parquet_any(path: Path) -> pd.DataFrame:
    return _normalise_dates(pd.read_parquet(path), path)


def _day_slice(df: pd.DataFrame, day: pd.Timestamp | pd.Timestamp.date) -> pd.DataFrame:
    day_date = pd.Timestamp(day).date()
    return df[df["date_only"] == day_date].copy().reset_index(drop=True)


def _causal_session_vwap(day_df: pd.DataFrame) -> pd.Series:
    typical = (
        pd.to_numeric(day_df["high"], errors="coerce")
        + pd.to_numeric(day_df["low"], errors="coerce")
        + pd.to_numeric(day_df["close"], errors="coerce")
    ) / 3.0
    volume = pd.to_numeric(day_df["volume"], errors="coerce").fillna(0.0)
    pv = typical * volume.clip(lower=0)
    vol_cum = volume.cumsum().replace(0, np.nan)
    return pv.cumsum() / vol_cum


def _max_abs(series: pd.Series) -> float:
    values = pd.to_numeric(series, errors="coerce").dropna()
    return float(values.abs().max()) if not values.empty else np.nan


def _max_bps(diff: pd.Series, close: pd.Series) -> float:
    close_num = pd.to_numeric(close, errors="coerce").abs().replace(0, np.nan)
    bps = pd.to_numeric(diff, errors="coerce").abs() / close_num * 10000.0
    bps = bps.dropna()
    return float(bps.max()) if not bps.empty else np.nan


def _vwap_bad_mask(observed: pd.Series, expected: pd.Series, close: pd.Series) -> pd.Series:
    observed_num = pd.to_numeric(observed, errors="coerce")
    expected_num = pd.to_numeric(expected, errors="coerce")
    close_num = pd.to_numeric(close, errors="coerce").abs()
    diff = (observed_num - expected_num).abs()
    bps_limit = close_num * (VWAP_BPS_TOL / 10000.0)
    limit = pd.concat(
        [pd.Series(VWAP_ABS_TOL, index=diff.index), bps_limit.fillna(0.0)],
        axis=1,
    ).max(axis=1)
    return diff > limit


def _compare_vwap(observed: pd.Series, expected: pd.Series, close: pd.Series) -> dict[str, Any]:
    diff = pd.to_numeric(observed, errors="coerce") - pd.to_numeric(expected, errors="coerce")
    bad = _vwap_bad_mask(observed, expected, close)
    return {
        "bad_rows": int(bad.fillna(False).sum()),
        "max_abs_diff": _max_abs(diff),
        "max_diff_bps": _max_bps(diff, close),
        "status": "PASS" if int(bad.fillna(False).sum()) == 0 else "FAIL",
    }


def _compare_day_value(prepared_day: pd.DataFrame) -> dict[str, Any]:
    expected = (
        pd.to_numeric(prepared_day["close"], errors="coerce")
        * pd.to_numeric(prepared_day["volume"], errors="coerce")
    ).cumsum()
    observed = pd.to_numeric(prepared_day.get("day_value_so_far_rs"), errors="coerce")
    diff = observed - expected
    limit = pd.concat(
        [
            pd.Series(DAY_VALUE_ABS_TOL, index=diff.index),
            expected.abs().fillna(0.0) * DAY_VALUE_REL_TOL,
        ],
        axis=1,
    ).max(axis=1)
    bad = diff.abs() > limit
    return {
        "bad_rows": int(bad.fillna(False).sum()),
        "max_abs_diff": _max_abs(diff),
        "status": "PASS" if int(bad.fillna(False).sum()) == 0 else "FAIL",
    }


def _compare_market_return(raw_all: pd.DataFrame, day_raw: pd.DataFrame, ticker: str) -> dict[str, Any]:
    if ticker.upper() not in {t.upper() for t in MARKET_TICKERS}:
        return {"status": "N/A", "bad_rows": 0, "max_abs_diff": np.nan}
    if day_raw.empty:
        return {"status": "NO_ROWS", "bad_rows": 0, "max_abs_diff": np.nan}

    ctx = v2._market_context_from_df(raw_all)
    day_key = str(day_raw["date_only"].iloc[0])
    by_ts = ctx.get(day_key, {})
    day_open = _safe_float(day_raw["open"].iloc[0])
    diffs: list[float] = []
    bad_rows = 0
    for _, row in day_raw.iterrows():
        expected = 0.0 if day_open <= 0 else (_safe_float(row["close"]) / day_open - 1.0) * 100.0
        got = by_ts.get(pd.Timestamp(row["date"]), {}).get("market_ret_pct", np.nan)
        diff = abs(_safe_float(got) - expected)
        diffs.append(diff)
        if not np.isfinite(diff) or diff > MRET_ABS_TOL:
            bad_rows += 1
    return {
        "status": "PASS" if bad_rows == 0 else "FAIL",
        "bad_rows": int(bad_rows),
        "max_abs_diff": float(np.nanmax(diffs)) if diffs else np.nan,
    }


def _source_vwap_status(day_raw: pd.DataFrame, causal_vwap: pd.Series) -> dict[str, Any]:
    if "VWAP" not in day_raw.columns:
        return {
            "status": "MISSING_COLUMN",
            "bad_rows": int(len(day_raw)),
            "nonnull_rows": 0,
            "max_abs_diff": np.nan,
            "max_diff_bps": np.nan,
        }
    source = pd.to_numeric(day_raw["VWAP"], errors="coerce")
    nonnull = int(source.notna().sum())
    if nonnull == 0:
        return {
            "status": "ALL_NAN",
            "bad_rows": int(len(day_raw)),
            "nonnull_rows": 0,
            "max_abs_diff": np.nan,
            "max_diff_bps": np.nan,
        }
    cmp = _compare_vwap(source, causal_vwap, day_raw["close"])
    return {
        "status": cmp["status"],
        "bad_rows": cmp["bad_rows"],
        "nonnull_rows": nonnull,
        "max_abs_diff": cmp["max_abs_diff"],
        "max_diff_bps": cmp["max_diff_bps"],
    }


def _data_staleness_minutes(path: Path) -> float:
    """Minutes since parquet file was last modified. NaN if path does not exist."""
    try:
        import time as _time
        mtime = path.stat().st_mtime
        return max(0.0, (_time.time() - mtime) / 60.0)
    except Exception:
        return np.nan


def _check_v8_feature_ranges(prepared_day: pd.DataFrame) -> dict[str, Any]:
    """Sanity-check V8 gate feature values against expected intraday equity ranges."""
    checks: dict[str, Any] = {}
    warn_reasons: list[str] = []

    def _check_col(col: str, lo: float, hi: float, label: str) -> None:
        if col not in prepared_day.columns:
            checks[label] = "MISSING"
            warn_reasons.append(f"{col}_missing")
            return
        vals = pd.to_numeric(prepared_day[col], errors="coerce").dropna()
        if vals.empty:
            checks[label] = "ALL_NAN"
            warn_reasons.append(f"{col}_all_nan")
            return
        bad = int(((vals < lo) | (vals > hi)).sum())
        checks[label] = "PASS" if bad == 0 else f"WARN({bad})"
        if bad:
            warn_reasons.append(f"{col}_bad({bad})")

    _check_col("atr_pct", 0.0005, 0.15, "atr_pct_range")
    _check_col("vol_ratio", 0.05, 50.0, "vol_ratio_range")
    _check_col("body_pct", 0.0, 1.001, "body_pct_range")
    _check_col("close_loc", -0.01, 1.01, "close_loc_range")
    _check_col("vwap_dist_atr", -20.0, 20.0, "vwap_dist_atr_range")

    if "rs_pct" in prepared_day.columns:
        vals = pd.to_numeric(prepared_day["rs_pct"], errors="coerce").dropna()
        bad = int(((vals < -10.0) | (vals > 10.0)).sum()) if not vals.empty else 0
        checks["rs_pct_range"] = "PASS" if bad == 0 else f"WARN({bad})"
        if bad:
            warn_reasons.append(f"rs_pct_bad({bad})")
    else:
        checks["rs_pct_range"] = "MISSING"

    if "quality_score" in prepared_day.columns:
        vals = pd.to_numeric(prepared_day["quality_score"], errors="coerce").dropna()
        if not vals.empty:
            bad = int(((vals < 0.0) | (vals > 1.001)).sum())
            checks["quality_score_range"] = "PASS" if bad == 0 else f"WARN({bad})"
            if bad:
                warn_reasons.append(f"quality_score_bad({bad})")
    else:
        checks["quality_score_range"] = "MISSING"

    checks["v8_features_status"] = "WARN" if warn_reasons else "PASS"
    if warn_reasons:
        checks["v8_features_warn_reasons"] = "; ".join(warn_reasons[:5])
    return checks


def _feature_medians(prepared_day: pd.DataFrame) -> dict[str, float]:
    """Median values for key V8 gate features — use for cross-day drift monitoring."""
    result: dict[str, float] = {}
    for col in ("atr_pct", "vol_ratio", "rs_pct", "vwap_dist_atr", "body_pct", "close_loc"):
        if col in prepared_day.columns:
            vals = pd.to_numeric(prepared_day[col], errors="coerce").dropna()
            result[f"median_{col}"] = float(vals.median()) if not vals.empty else np.nan
        else:
            result[f"median_{col}"] = np.nan
    return result


def _audit_ticker(ticker: str, data_root: Path, day: pd.Timestamp) -> dict[str, Any]:
    ticker = ticker.upper().strip()
    path = _resolve_ticker_path(ticker, data_root)
    base: dict[str, Any] = {
        "ticker": ticker,
        "path": str(path),
        "rows": 0,
        "first_bar": "",
        "last_bar": "",
        "feature_path_status": "FAIL",
        "source_vwap_status": "MISSING_FILE",
        "prepared_vwap_status": "NOT_RUN",
        "day_value_status": "NOT_RUN",
        "market_ret_status": "N/A",
    }
    if not path.exists():
        return base

    raw_all = _read_parquet_any(path)
    day_raw = _day_slice(raw_all, day)
    if day_raw.empty:
        base.update(
            {
                "feature_path_status": "NO_DAY_ROWS",
                "source_vwap_status": "NO_DAY_ROWS",
                "prepared_vwap_status": "NO_DAY_ROWS",
                "day_value_status": "NO_DAY_ROWS",
                "market_ret_status": "NO_DAY_ROWS" if ticker in MARKET_TICKERS else "N/A",
            }
        )
        return base

    prepared_all = v2._prepare_5m(raw_all)
    prepared_day = _day_slice(prepared_all, day)
    causal_vwap = _causal_session_vwap(day_raw)

    source = _source_vwap_status(day_raw, causal_vwap)
    prepared_cmp = _compare_vwap(prepared_day["VWAP"], causal_vwap, prepared_day["close"])
    day_value = _compare_day_value(prepared_day)
    market_ret = _compare_market_return(raw_all, day_raw, ticker)

    source_dist_delta = np.nan
    if "VWAP" in day_raw.columns and "ATR" in prepared_day.columns:
        atr = pd.to_numeric(prepared_day["ATR"], errors="coerce").replace(0, np.nan)
        source_vwap = pd.to_numeric(day_raw["VWAP"], errors="coerce")
        source_dist = (pd.to_numeric(day_raw["close"], errors="coerce") - source_vwap) / atr
        prepared_dist = pd.to_numeric(prepared_day["vwap_dist_atr"], errors="coerce")
        source_dist_delta = _max_abs(prepared_dist - source_dist)

    v8_checks = _check_v8_feature_ranges(prepared_day)
    medians = _feature_medians(prepared_day)
    staleness_min = _data_staleness_minutes(path)

    core_feature_checks = [
        prepared_cmp["status"],
        day_value["status"],
        market_ret["status"],
    ]
    feature_status = "PASS" if all(s in {"PASS", "N/A"} for s in core_feature_checks) else "FAIL"

    base.update(
        {
            "rows": int(len(day_raw)),
            "first_bar": _fmt_ts(day_raw["date"].iloc[0]),
            "last_bar": _fmt_ts(day_raw["date"].iloc[-1]),
            "feature_path_status": feature_status,
            "file_staleness_min": round(staleness_min, 1) if np.isfinite(staleness_min) else np.nan,
            "source_vwap_status": source["status"],
            "source_vwap_nonnull_rows": source["nonnull_rows"],
            "source_vwap_bad_rows": source["bad_rows"],
            "source_vwap_max_abs_diff": source["max_abs_diff"],
            "source_vwap_max_diff_bps": source["max_diff_bps"],
            "prepared_vwap_status": prepared_cmp["status"],
            "prepared_vwap_bad_rows": prepared_cmp["bad_rows"],
            "prepared_vwap_max_abs_diff": prepared_cmp["max_abs_diff"],
            "prepared_vwap_max_diff_bps": prepared_cmp["max_diff_bps"],
            "day_value_status": day_value["status"],
            "day_value_bad_rows": day_value["bad_rows"],
            "day_value_max_abs_diff_rs": day_value["max_abs_diff"],
            "market_ret_status": market_ret["status"],
            "market_ret_bad_rows": market_ret["bad_rows"],
            "market_ret_max_abs_diff_pct": market_ret["max_abs_diff"],
            "source_vs_prepared_vwap_dist_atr_max_delta": source_dist_delta,
            "v8_features_status": v8_checks.get("v8_features_status", "PASS"),
            "v8_atr_pct_range": v8_checks.get("atr_pct_range", "MISSING"),
            "v8_vol_ratio_range": v8_checks.get("vol_ratio_range", "MISSING"),
            "v8_rs_pct_range": v8_checks.get("rs_pct_range", "MISSING"),
            "v8_features_warn": v8_checks.get("v8_features_warn_reasons", ""),
            **medians,
        }
    )
    return base


def _candidate_csv_tickers(day: str) -> list[str]:
    roots = [
        runtime_dir("signal_discovery_v7_5mins_ID") / "csv",
        runtime_dir("signal_discovery_v7_5mins_ID") / "latest",
        runtime_dir("live_signals"),
    ]
    names = [
        f"raw_candidate_tickers_{day}.csv",
        f"candidate_tickers_{day}.csv",
        f"gated_candidate_tickers_{day}.csv",
        f"signals_{day}_id_5min_v7_short.csv",
        "raw_candidate_tickers_latest.csv",
        "candidate_tickers_latest.csv",
        "gated_candidate_tickers_latest.csv",
    ]
    out: list[str] = []
    for root in roots:
        for name in names:
            path = root / name
            if not path.exists() or path.stat().st_size <= 2:
                continue
            try:
                df = pd.read_csv(path, nrows=250)
            except Exception:
                continue
            for col in ("ticker", "symbol", "stock"):
                if col in df.columns:
                    out.extend(str(v).strip().upper() for v in df[col].dropna().tolist())
                    break
    return out


def _parse_tickers(values: list[str] | None) -> list[str]:
    if not values:
        return []
    tickers: list[str] = []
    for value in values:
        tickers.extend(part.strip().upper() for part in str(value).split(",") if part.strip())
    return tickers


def _resolve_tickers(data_root: Path, day: str, requested: list[str] | None, max_tickers: int) -> list[str]:
    ordered: list[str] = []
    requested_tickers = _parse_tickers(requested)
    seed = requested_tickers if requested_tickers else [*MARKET_TICKERS, *_candidate_csv_tickers(day)]
    for ticker in seed:
        ticker = str(ticker).strip().upper()
        if ticker and ticker not in ordered:
            ordered.append(ticker)
    if not requested_tickers and len(ordered) < max_tickers and data_root.exists():
        suffix = "_stocks_indicators_5min.parquet"
        for path in sorted(data_root.glob(f"*{suffix}")):
            ticker = path.name.removesuffix(suffix).upper()
            if ticker and ticker not in ordered:
                ordered.append(ticker)
            if len(ordered) >= max_tickers:
                break
    return ordered[:max_tickers]


def _markdown_table(df: pd.DataFrame, cols: list[str]) -> str:
    if df.empty:
        return "_No rows._"
    work = df[cols].copy()
    for col in work.columns:
        work[col] = work[col].map(lambda v: "" if pd.isna(v) else str(v))
    widths = {col: max(len(col), int(work[col].map(len).max())) for col in work.columns}
    header = "| " + " | ".join(col.ljust(widths[col]) for col in work.columns) + " |"
    sep = "| " + " | ".join("-" * widths[col] for col in work.columns) + " |"
    rows = [
        "| " + " | ".join(str(row[col]).ljust(widths[col]) for col in work.columns) + " |"
        for _, row in work.iterrows()
    ]
    return "\n".join([header, sep, *rows])


def _render_markdown(summary: dict[str, Any], rows: pd.DataFrame) -> str:
    core_cols = [
        "ticker",
        "rows",
        "feature_path_status",
        "v8_features_status",
        "source_vwap_status",
        "prepared_vwap_status",
        "day_value_status",
        "market_ret_status",
        "file_staleness_min",
        "source_vwap_bad_rows",
        "prepared_vwap_bad_rows",
        "source_vwap_max_diff_bps",
        "source_vs_prepared_vwap_dist_atr_max_delta",
        "v8_features_warn",
    ]
    stale_cols = [
        "ticker",
        "file_staleness_min",
        "v8_atr_pct_range",
        "v8_vol_ratio_range",
        "v8_rs_pct_range",
        "median_atr_pct",
        "median_vol_ratio",
        "median_rs_pct",
        "median_vwap_dist_atr",
    ]
    v8_warnings = int((rows.get("v8_features_status", pd.Series(dtype=str)) == "WARN").sum()) if not rows.empty else 0
    stale_threshold = 120.0
    stale_count = 0
    if "file_staleness_min" in rows.columns and not rows.empty:
        stale_count = int(
            (pd.to_numeric(rows["file_staleness_min"], errors="coerce") > stale_threshold).sum()
        )
    lines = [
        "# V7 Causality Audit",
        "",
        f"- Day: {summary['day']}",
        f"- Data root: `{summary['data_root']}`",
        f"- Overall status: **{summary['overall_status']}**",
        f"- Feature-path failures: {summary['feature_path_failures']}",
        f"- Source VWAP warnings: {summary['source_vwap_warnings']}",
        f"- V8 feature range warnings: {v8_warnings}",
        f"- Stale files (>{stale_threshold:.0f} min): {stale_count}",
        f"- Tickers checked: {summary['tickers_checked']}",
        f"- Generated: {summary['generated_at_ist']}",
        "",
        "**V8 gate features checked**: `atr_pct`, `vol_ratio`, `rs_pct`, `vwap_dist_atr`, `body_pct`, `close_loc`, `quality_score`.",
        "`file_staleness_min` = minutes since parquet last written; >120 = potential data feed gap.",
        "The production V7 feature path is considered safe when `prepared_vwap_status`,",
        "`day_value_status`, and `market_ret_status` all pass. V8 feature range issues remain warnings.",
        "",
        _markdown_table(rows, [c for c in core_cols if c in rows.columns]),
        "",
        "## V8 Feature Ranges & Medians",
        "",
        _markdown_table(rows, [c for c in stale_cols if c in rows.columns]),
        "",
    ]
    return "\n".join(lines)


def run_audit(day: str, data_root: Path, tickers: list[str] | None, max_tickers: int) -> tuple[dict[str, Any], pd.DataFrame]:
    day_ts = pd.Timestamp(day)
    resolved = _resolve_tickers(data_root, day, tickers, max_tickers)
    records = [_audit_ticker(ticker, data_root, day_ts) for ticker in resolved]
    rows = pd.DataFrame(records)
    feature_failures = int((rows.get("feature_path_status", pd.Series(dtype=str)) == "FAIL").sum())
    source_warnings = int(
        rows.get("source_vwap_status", pd.Series(dtype=str))
        .isin(["MISSING_COLUMN", "ALL_NAN", "FAIL", "MISSING_FILE"])
        .sum()
    )
    v8_warnings = int((rows.get("v8_features_status", pd.Series(dtype=str)) == "WARN").sum()) if "v8_features_status" in rows.columns else 0
    stale_files = int(
        (pd.to_numeric(rows.get("file_staleness_min", pd.Series(dtype=float)), errors="coerce") > 120.0).sum()
    ) if "file_staleness_min" in rows.columns else 0
    if feature_failures:
        overall = "FAIL"
    elif source_warnings or v8_warnings or stale_files:
        overall = "PASS_WITH_WARNINGS"
    else:
        overall = "PASS"
    summary = {
        "session": SESSION_NAME,
        "session_slug": SESSION_SLUG,
        "day": day,
        "data_root": str(data_root),
        "overall_status": overall,
        "feature_path_failures": feature_failures,
        "source_vwap_warnings": source_warnings,
        "v8_feature_warnings": v8_warnings,
        "stale_files_gt120min": stale_files,
        "tickers_checked": int(len(rows)),
        "generated_at_ist": _fmt_ts(_now_ist()),
    }
    return summary, rows


def _write_outputs(day: str, summary: dict[str, Any], rows: pd.DataFrame) -> dict[str, str]:
    stamp = _now_ist().strftime("%Y%m%d_%H%M%S")
    csv_text = rows.to_csv(index=False)
    md_text = _render_markdown(summary, rows)
    latest_csv = LATEST_DIR / "v7_causality_audit.csv"
    latest_json = LATEST_DIR / "v7_causality_audit.json"
    latest_md = LATEST_DIR / "v7_causality_audit.md"
    report_csv = REPORTS_DIR / f"v7_causality_audit_{day}_{stamp}.csv"
    report_json = REPORTS_DIR / f"v7_causality_audit_{day}_{stamp}.json"
    report_md = REPORTS_DIR / f"v7_causality_audit_{day}_{stamp}.md"
    payload = {"summary": summary, "rows": rows.to_dict(orient="records")}
    _write_text_atomic(latest_csv, csv_text)
    _write_json_atomic(latest_json, payload)
    _write_text_atomic(latest_md, md_text)
    _write_text_atomic(report_csv, csv_text)
    _write_json_atomic(report_json, payload)
    _write_text_atomic(report_md, md_text)
    return {
        "latest_csv": str(latest_csv),
        "latest_json": str(latest_json),
        "latest_md": str(latest_md),
        "report_csv": str(report_csv),
        "report_json": str(report_json),
        "report_md": str(report_md),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Audit V7 live feature causality")
    parser.add_argument("--date", default=str(_now_ist().date()))
    parser.add_argument("--data-root", default=str(DEFAULT_LIVE_5M_DIR))
    parser.add_argument("--tickers", nargs="*", default=None, help="Optional tickers, comma-separated or space-separated")
    parser.add_argument("--max-tickers", type=int, default=50)
    parser.add_argument("--fail-on-source-warning", action="store_true")
    args = parser.parse_args(argv)

    data_root = Path(args.data_root)
    _write_status("RUNNING", phase="START", day=args.date, data_root=str(data_root))
    try:
        summary, rows = run_audit(args.date, data_root, args.tickers, int(args.max_tickers))
        outputs = _write_outputs(args.date, summary, rows)
        _write_status("STOPPED", phase="DONE", **summary, **outputs)
        print(json.dumps({"summary": summary, "outputs": outputs}, indent=2, default=_json_default))
        if summary["overall_status"] == "FAIL":
            return 2
        if args.fail_on_source_warning and summary["source_vwap_warnings"] > 0:
            return 3
        return 0
    except Exception as exc:
        _write_status("ERROR", phase="FAILED", day=args.date, data_root=str(data_root), error=f"{type(exc).__name__}: {exc}")
        raise


if __name__ == "__main__":
    raise SystemExit(main())
