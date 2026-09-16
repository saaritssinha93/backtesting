"""V13-v9 selection/rejection research, with explicit counterfactual limits.

This module never selects a strategy. Native setup candidates are replayed on
actual cash-equity one-minute bars. Their overlapping hypothetical returns are
descriptive observations, not an investable portfolio or incremental profit.
The fixed bracket is registered independently of observed candidate outcomes.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import time
from pathlib import Path
from typing import Any, Callable

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

import fno_oi_common as common
import fno_oi_hybrid_data as hybrid
import fno_v13_corrected_v5_backtest as v5


SCHEMA_VERSION = "FNO_V13_V9_SELECTION_DIAGNOSTICS_V1"
COUNTERFACTUAL_RULE = "DIAGNOSTIC_NATIVE_TRIGGER_S10_STOP1.5_TARGET2.6_EOD1515"
NATIVE_COUNTERFACTUAL_RULE = "UNCHANGED_V13_HIGHER_FREQUENCY_STOP1.5_T11.075_PARTIAL10PCT_RUNNER2.6_BREAKEVEN_S10_EOD1515"
OUTCOME_LIMITATION = "OVERLAPPING_COUNTERFACTUALS_NOT_PORTFOLIO_PROFIT"
TRAIN_END = "2026-08-13"
VALIDATION_END = "2026-08-26"
MINIMUM_STABILITY_COUNT = 10
MinuteLoader = Callable[[str], pd.DataFrame]

# Fixed bins, shared across periods, never fit to future outcomes or quantiles.
FEATURE_BINS: dict[str, list[float]] = {
    "volume_ratio": [-np.inf, 1.0, 1.5, 2.0, 3.0, np.inf],
    "body_ratio": [-np.inf, 0.4, 0.6, 0.8, np.inf],
    "wick_ratio": [-np.inf, 0.2, 0.35, 0.5, np.inf],
    "oi_change_pct": [-np.inf, 0.05, 0.1, 0.25, 0.5, 1.0, np.inf],
    "price_change_pct": [-np.inf, -1.0, -0.4, 0.0, 0.4, 1.0, np.inf],
    "v9_5m_range_pct": [-np.inf, 0.5, 1.0, 1.5, 2.0, np.inf],
    "v9_5m_body_ratio": [-np.inf, 0.4, 0.6, 0.8, np.inf],
    "v9_5m_distance_vwap_pct": [-np.inf, -1.5, -0.5, 0.0, 0.5, 1.5, np.inf],
    "v9_1m_range_pct": [-np.inf, 0.1, 0.25, 0.5, 1.0, np.inf],
    "v9_1m_body_ratio": [-np.inf, 0.4, 0.6, 0.8, np.inf],
    "v9_1m_wick_ratio": [-np.inf, 0.2, 0.35, 0.5, np.inf],
    "v9_5m_ema_spread_pct": [-np.inf, -1.0, -0.5, 0.0, 0.5, 1.0, np.inf],
    "v9_1m_ema_spread_pct": [-np.inf, -0.5, -0.2, 0.0, 0.2, 0.5, np.inf],
    "v9_5m_volume_ratio": [-np.inf, 1.0, 1.5, 2.0, 3.0, np.inf],
    "v9_1m_volume_ratio": [-np.inf, 0.5, 1.0, 1.5, 2.0, 3.0, np.inf],
    "v9_5m_gap_pct": [-np.inf, -1.0, -0.5, 0.0, 0.5, 1.0, np.inf],
    "v9_5m_signed_ema9_20_spread_pct": [-np.inf, 0.0, 0.05, 0.1, 0.2, 0.5, np.inf],
    "v9_1m_signed_ema9_20_spread_pct": [-np.inf, 0.0, 0.02, 0.05, 0.1, 0.2, np.inf],
    "v9_5m_signed_distance_vwap_pct": [-np.inf, 0.0, 0.5, 1.0, 1.5, 2.0, np.inf],
}


def load_raw_equity_minutes(symbol: str) -> pd.DataFrame:
    """Read retained raw rows before the native loader removes duplicates."""
    path = hybrid.equity_one_minute_path(symbol, hybrid.DEFAULT_BACKTEST_EQUITY_1M_DIR)
    available = set(pq.ParquetFile(path).schema.names)
    wanted = ["date", "open", "high", "low", "close"]
    missing = set(wanted) - available
    if missing:
        raise ValueError(f"Raw equity history missing {sorted(missing)}: {path}")
    wanted += [name for name in ("volume", "gap_filled", "opening_snapshot", "provisional_stale") if name in available]
    return pd.read_parquet(path, columns=wanted)


def as_bool(values: pd.Series) -> pd.Series:
    """CSV-safe booleans: the string 'False' must never count as true."""
    return values.astype("string").fillna("false").str.strip().str.lower().isin(
        {"true", "1", "1.0", "yes"}
    )


def period_labels(days: pd.Series) -> pd.Series:
    stamps = pd.to_datetime(days, errors="coerce").dt.strftime("%Y-%m-%d")
    return pd.Series(
        np.select(
            [stamps.le(TRAIN_END).fillna(False), stamps.le(VALIDATION_END).fillna(False)],
            ["TRAIN", "VALIDATION"],
            default="PREVIOUSLY_SEEN_LATER",
        ),
        index=days.index,
    )


def _ist(value: Any) -> pd.Timestamp:
    if value is None or pd.isna(value):
        return pd.NaT
    stamp = pd.Timestamp(value)
    if pd.isna(stamp):
        return pd.NaT
    return stamp.tz_localize(common.IST) if stamp.tzinfo is None else stamp.tz_convert(common.IST)


def _source_real(frame: pd.DataFrame) -> np.ndarray:
    real = np.ones(len(frame), dtype=bool)
    for column in ("gap_filled", "opening_snapshot", "provisional_stale"):
        if column in frame:
            numeric = pd.to_numeric(frame[column], errors="coerce").fillna(0).ne(0)
            real &= ~(numeric | as_bool(frame[column])).to_numpy()
    return real


def _normalize_minutes(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    if "ts" not in out:
        source = "timestamp" if "timestamp" in out else "date"
        if source not in out:
            return pd.DataFrame(columns=["ts", "open", "high", "low", "close"])
        out["ts"] = out[source]
    out["ts"] = common._to_ist(out["ts"])
    for name in ("open", "high", "low", "close"):
        out[name] = pd.to_numeric(out.get(name, np.nan), errors="coerce")
    # Duplicate observations are not silently converted into verified paths.
    out["_duplicate_ts"] = out["ts"].duplicated(keep=False)
    out["_source_real"] = _source_real(out)
    return out.loc[out["ts"].notna()].sort_values("ts", kind="stable").reset_index(drop=True)


def _valid_ohlc(frame: pd.DataFrame) -> np.ndarray:
    ohlc = frame[["open", "high", "low", "close"]].to_numpy(float)
    return (
        np.isfinite(ohlc).all(axis=1)
        & (ohlc > 0).all(axis=1)
        & (ohlc[:, 1] >= np.maximum(ohlc[:, 0], ohlc[:, 3]))
        & (ohlc[:, 2] <= np.minimum(ohlc[:, 0], ohlc[:, 3]))
        & (ohlc[:, 1] >= ohlc[:, 2])
    )


def checked_candidate_paths(
    candidates: pd.DataFrame,
    *,
    minute_loader: MinuteLoader = load_raw_equity_minutes,
) -> tuple[pd.DataFrame, dict[int, dict[str, np.ndarray]], pd.DataFrame]:
    """Materialize complete end-labelled paths; missing history stays unresolved.

    A setup ending S confirms on S+1 and can first enter on S+2. Only real,
    valid, exact confirmation candles and continuous paths through 15:15 are
    replayable. Missing later bars invalidate even an apparently earlier exit,
    matching the native V13 complete-path evidence contract.
    """
    orders = candidates.copy().reset_index(drop=True)
    if orders.empty:
        return orders, {}, pd.DataFrame(columns=["sid", "counterfactual_status"])
    orders["source_sid"] = orders.get("sid", pd.Series(np.nan, index=orders.index))
    orders["sid"] = np.arange(len(orders), dtype=np.int64)
    quality: list[dict[str, Any]] = []
    paths: dict[int, dict[str, np.ndarray]] = {}
    for symbol, group in orders.groupby("tradingsymbol", sort=True, dropna=False):
        try:
            minute = _normalize_minutes(minute_loader(str(symbol)))
            load_error = ""
        except (FileNotFoundError, ValueError, KeyError, OSError) as exc:
            minute = pd.DataFrame()
            load_error = f"{type(exc).__name__}: {exc}"
        ns = minute["ts"].astype("int64").to_numpy() if not minute.empty else np.array([], dtype=np.int64)
        values = {name: minute[name].to_numpy(float) for name in ("open", "high", "low", "close")} if not minute.empty else {}
        valid = _valid_ohlc(minute) if not minute.empty else np.array([], dtype=bool)
        real = minute["_source_real"].to_numpy(bool) if not minute.empty else np.array([], dtype=bool)
        duplicate = minute["_duplicate_ts"].to_numpy(bool) if not minute.empty else np.array([], dtype=bool)
        for row in group.to_dict("records"):
            sid = int(row["sid"])
            confirmation = _ist(row.get("confirmation_ts"))
            item: dict[str, Any] = {
                "sid": sid,
                "source_sid": row.get("source_sid"),
                "candidate_id": row.get("candidate_id", sid),
                "counterfactual_status": "UNRESOLVED",
                "path_expected_rows": 0,
                "path_actual_rows": 0,
                "path_missing_minutes": 0,
                "path_invalid_rows": 0,
                "path_flagged_rows": 0,
                "path_duplicate_rows": 0,
                "source_error": load_error,
            }
            def finish(status: str) -> None:
                item["counterfactual_status"] = status
                quality.append(item)

            if pd.isna(confirmation):
                finish("MISSING_CONFIRMATION_TIMESTAMP")
                continue
            signal = _ist(row.get("signal_ts"))
            if not pd.isna(signal) and confirmation != signal + pd.Timedelta(minutes=1):
                finish("CONFIRMATION_NOT_EXACTLY_SETUP_PLUS_ONE_MINUTE")
                continue
            side = str(row.get("side", "")).upper()
            trigger = pd.to_numeric(row.get("trigger"), errors="coerce")
            if side not in {"LONG", "SHORT"}:
                finish("INVALID_SIDE")
                continue
            if not len(ns):
                finish("MISSING_MINUTE_SOURCE")
                continue
            ci = int(np.searchsorted(ns, confirmation.value))
            if ci >= len(ns) or ns[ci] != confirmation.value:
                finish("MISSING_EXACT_CONFIRMATION_MINUTE")
                continue
            if duplicate[ci] or not valid[ci] or not real[ci]:
                finish("INVALID_OR_NONREAL_CONFIRMATION_MINUTE")
                continue
            if values["high"][ci] <= values["low"][ci]:
                finish("ZERO_RANGE_CONFIRMATION_MINUTE")
                continue
            if pd.isna(trigger) or not np.isfinite(trigger) or trigger <= 0:
                finish("INVALID_TRIGGER")
                continue
            expected_trigger = values["high"][ci] if side == "LONG" else values["low"][ci]
            if not np.isclose(trigger, expected_trigger, atol=1e-8, rtol=1e-10):
                finish("TRIGGER_CONFIRMATION_MISMATCH")
                continue
            cutoff = confirmation.normalize() + pd.Timedelta(hours=15, minutes=15)
            first = confirmation + pd.Timedelta(minutes=1)
            if first > cutoff:
                finish("NO_FORWARD_TIME_BEFORE_CUTOFF")
                continue
            left = int(np.searchsorted(ns, first.value))
            right = int(np.searchsorted(ns, cutoff.value, side="right"))
            expected = np.arange(first.value, cutoff.value + 1, pd.Timedelta(minutes=1).value, dtype=np.int64)
            actual = ns[left:right]
            item.update(
                path_expected_rows=len(expected),
                path_actual_rows=len(actual),
                path_missing_minutes=int(len(expected) - np.isin(expected, actual).sum()),
                path_invalid_rows=int((~valid[left:right]).sum()),
                path_flagged_rows=int((~real[left:right]).sum()),
                path_duplicate_rows=int(duplicate[left:right].sum()),
            )
            if not np.array_equal(actual, expected):
                finish("INCOMPLETE_OR_DUPLICATE_FORWARD_PATH")
                continue
            if item["path_invalid_rows"] or item["path_flagged_rows"]:
                finish("INVALID_OR_NONREAL_FORWARD_PATH")
                continue
            paths[sid] = {
                "timestamp_ns": actual,
                **{name: array[left:right] for name, array in values.items()},
            }
            finish("COMPLETE_PATH")
    quality_frame = pd.DataFrame(quality)
    if len(quality_frame) != len(orders) or quality_frame["sid"].nunique() != len(orders):
        raise AssertionError("Every candidate must have exactly one explicit path-quality outcome")
    return orders, paths, quality_frame


def fixed_counterfactuals(
    candidates: pd.DataFrame,
    *,
    minute_loader: MinuteLoader = load_raw_equity_minutes,
    cost_bps: float = 5.0,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Hypothetical 1.5/2.6 bracket on valid confirmation-bar trigger + S10.

    This intentionally also describes candidates that failed native thresholds
    or directional confirmation. It does not claim those orders were signalled.
    """
    orders, paths, quality = checked_candidate_paths(candidates, minute_loader=minute_loader)
    return _replay_checked_candidates(orders, paths, quality, cost_bps=cost_bps), quality


def _replay_checked_candidates(
    orders: pd.DataFrame,
    paths: dict[int, dict[str, np.ndarray]],
    quality: pd.DataFrame,
    *,
    cost_bps: float = 5.0,
    native_scaleout: bool = False,
) -> pd.DataFrame:
    """Apply either frozen payoff to the same broad candidate/path population."""
    if orders.empty:
        return orders.copy()
    orders = orders.copy()
    ready = orders.loc[orders["sid"].isin(paths)].copy()
    if not ready.empty:
        if native_scaleout:
            replay = v5.simulate_scaleout(
                ready, paths, v5.PROFILES["higher_frequency"].exit,
                cost_bps=cost_bps, max_entry_delay_minutes=10,
            )
        else:
            ready["native_stop_pct"] = 1.5
            ready["native_target_pct"] = 2.6
            replay = v5.simulate_native(ready, paths, cost_bps=cost_bps, max_entry_delay_minutes=10)
        added = [c for c in replay if c not in orders or c == "sid"]
        orders = orders.merge(replay[added], on="sid", how="left", validate="one_to_one")
    orders = orders.merge(quality.drop(columns=["source_sid", "candidate_id"]), on="sid", how="left", validate="one_to_one")
    orders["counterfactual_rule"] = NATIVE_COUNTERFACTUAL_RULE if native_scaleout else COUNTERFACTUAL_RULE
    orders["outcome_limitations"] = OUTCOME_LIMITATION
    orders["period"] = period_labels(orders["day"])
    complete = orders["counterfactual_status"].eq("COMPLETE_PATH")
    filled = as_bool(orders.get("filled", pd.Series(False, index=orders.index)))
    orders.loc[complete & filled, "counterfactual_status"] = "RESOLVED_FILLED"
    orders.loc[complete & ~filled, "counterfactual_status"] = "RESOLVED_NO_TRIGGER_WITHIN_S10"
    return orders


def _feature_columns(frame: pd.DataFrame) -> list[str]:
    return [name for name in frame if name in FEATURE_BINS or (
        name.startswith(("v9_5m_", "v9_1m_"))
        and name.endswith(("_pct", "_ratio", "_bull", "_bear"))
    )]


def add_decision_feature_views(frame: pd.DataFrame) -> pd.DataFrame:
    """Expose the exact side-aligned inputs of the registered EMA/VWAP arms."""
    out = frame.copy()
    sign = pd.Series(np.where(out["side"].eq("LONG"), 1.0, -1.0), index=out.index)
    for prefix, close_column in (("v9_5m_", "signal_close"), ("v9_1m_", "confirmation_close")):
        if {prefix + "ema9", prefix + "ema20", close_column}.issubset(out.columns):
            close = pd.to_numeric(out[close_column], errors="coerce")
            spread = pd.to_numeric(out[prefix + "ema9"], errors="coerce") - pd.to_numeric(out[prefix + "ema20"], errors="coerce")
            out[prefix + "signed_ema9_20_spread_pct"] = spread.div(close.where(close.gt(0))) * 100.0 * sign
    if "v9_5m_distance_vwap_pct" in out:
        out["v9_5m_signed_distance_vwap_pct"] = pd.to_numeric(out["v9_5m_distance_vwap_pct"], errors="coerce") * sign
    return out


def outcome_summary(frame: pd.DataFrame, group_columns: list[str]) -> pd.DataFrame:
    """Summaries of observations; deliberately omit any sum of returns or P&L."""
    if frame.empty:
        return pd.DataFrame()
    work = frame.copy()
    for name in group_columns:
        if name not in work:
            work[name] = "UNKNOWN"
    work["_net"] = pd.to_numeric(work.get("net_return_pct", np.nan), errors="coerce")
    work["_filled"] = as_bool(work.get("filled", pd.Series(False, index=work.index)))
    rows = []
    for key, group in work.groupby(group_columns, dropna=False, sort=True):
        keys = key if isinstance(key, tuple) else (key,)
        outcomes = group.loc[group["_filled"] & group["_net"].notna(), "_net"]
        rows.append({
            **dict(zip(group_columns, keys)),
            "observations": len(group),
            "sessions": int(group["day"].nunique()) if "day" in group else 0,
            "resolved_fills": len(outcomes),
            "wins": int(outcomes.gt(0).sum()),
            "losses": int(outcomes.lt(0).sum()),
            "win_rate_pct": float(outcomes.gt(0).mean() * 100) if len(outcomes) else np.nan,
            "mean_net_return_pct": float(outcomes.mean()) if len(outcomes) else np.nan,
            "median_net_return_pct": float(outcomes.median()) if len(outcomes) else np.nan,
            "outcome_limitations": OUTCOME_LIMITATION,
        })
    return pd.DataFrame(rows)


def feature_descriptions(frame: pd.DataFrame, group_columns: list[str]) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame()
    rows: list[dict[str, Any]] = []
    group_columns = [name for name in group_columns if name in frame]
    for key, group in frame.groupby(group_columns, dropna=False, sort=True):
        keys = key if isinstance(key, tuple) else (key,)
        for feature in _feature_columns(frame):
            values = pd.to_numeric(group[feature], errors="coerce").astype(float).replace([np.inf, -np.inf], np.nan).dropna()
            if values.empty:
                continue
            rows.append({
                **dict(zip(group_columns, keys)), "feature": feature,
                "observations": len(group), "known_values": len(values),
                "missing_values": int(len(group) - len(values)),
                "mean": float(values.mean()), "p10": float(values.quantile(.1)),
                "median": float(values.median()), "p90": float(values.quantile(.9)),
            })
    return pd.DataFrame(rows)


def feature_bin_outcomes(frame: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Development-only fixed-bin outcome and sign-stability descriptions.

    Rows from the already-seen later period cannot influence these tables.
    Ten fills per period is a descriptive sufficiency floor, not significance.
    """
    if frame.empty:
        return pd.DataFrame(), pd.DataFrame()
    work = frame.copy()
    work["period"] = period_labels(work["day"])
    work = work.loc[work["period"].isin(["TRAIN", "VALIDATION"])]
    parts = []
    for feature, edges in FEATURE_BINS.items():
        if feature not in work:
            continue
        view = work.copy()
        values = pd.to_numeric(view[feature], errors="coerce").replace([np.inf, -np.inf], np.nan)
        view["feature_bin"] = pd.cut(values, edges, right=False).astype(str)
        view.loc[values.isna(), "feature_bin"] = "MISSING"
        view["feature"] = feature
        parts.append(outcome_summary(view, ["period", "setup_id", "selection_stage", "feature", "feature_bin"]))
    bins = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()
    stable = []
    if not bins.empty:
        for key, group in bins.groupby(["setup_id", "selection_stage", "feature", "feature_bin"], dropna=False):
            periods = group.set_index("period")
            train = periods.loc["TRAIN"] if "TRAIN" in periods.index else None
            validation = periods.loc["VALIDATION"] if "VALIDATION" in periods.index else None
            tn = int(train["resolved_fills"]) if train is not None else 0
            vn = int(validation["resolved_fills"]) if validation is not None else 0
            tm = float(train["mean_net_return_pct"]) if train is not None else np.nan
            vm = float(validation["mean_net_return_pct"]) if validation is not None else np.nan
            enough = tn >= MINIMUM_STABILITY_COUNT and vn >= MINIMUM_STABILITY_COUNT
            stable.append({
                **dict(zip(["setup_id", "selection_stage", "feature", "feature_bin"], key)),
                "train_fills": tn, "validation_fills": vn,
                "train_mean_net_return_pct": tm, "validation_mean_net_return_pct": vm,
                "same_mean_sign": bool(np.isfinite(tm) and np.isfinite(vm) and np.sign(tm) == np.sign(vm)),
                "sample_status": "DESCRIPTIVE_SUFFICIENT" if enough else "INSUFFICIENT_SAMPLE",
                "outcome_limitations": OUTCOME_LIMITATION,
            })
    return bins, pd.DataFrame(stable)


def v8_coverage_context(candidates: pd.DataFrame, output_dir: Path) -> dict[str, Any]:
    """V8 is a futures-feature shadow, not an independently profitable engine.

    Recompute its frozen features for all strict eligible candidates at the
    earliest legal entry boundary, before any future trigger or return is known.
    Coverage is descriptive only and supplies no additional V9 selection arm.
    """
    import fno_v13_v8_feature_shadow as v8

    roots = [
        common.FNO_ROOT / "strategy_research" / "v13_corrected_v5" / "derivative_market_data" / "raw_futures_1m",
        common.FNO_ROOT / "raw_contracts_1m",
        common.FNO_ROOT / "raw_contracts_1m_hist",
    ]
    rows: list[dict[str, Any]] = []
    sources: list[dict[str, Any]] = []
    for contract, group in candidates.groupby("futures_tradingsymbol", sort=True):
        parts = []
        for root in roots:
            path = root / f"{contract}_1minute.parquet"
            if path.is_file():
                digest = hashlib.sha256(path.read_bytes()).hexdigest()
                part = v8.normalize_futures_candles(pd.read_parquet(path))
                sources.append({"path": str(path.resolve()), "sha256": digest, "rows": len(part)})
                parts.append(part)
        # Same named futures contract only. Existing retained history has final
        # precedence on exact duplicate timestamps; sources remain hash-audited.
        history = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()
        if not history.empty:
            history = v8.normalize_futures_candles(history)
        for candidate in group.to_dict("records"):
            record = {name: candidate.get(name) for name in (
                "sid", "day", "tradingsymbol", "side", "setup_id", "futures_tradingsymbol", "confirmation_ts", "v9_selected"
            )}
            cutoff = _ist(candidate["confirmation_ts"]) + pd.Timedelta(minutes=1)
            record["v8_observation_boundary"] = cutoff
            record["v8_context_use"] = "COVERAGE_ONLY_NOT_A_V9_SELECTION_FILTER"
            if history.empty:
                record["v8_feature_status"] = "MISSING_FUTURES_MINUTE_SOURCE"
            else:
                record.update(v8.causal_futures_features(
                    history, entry_ts=cutoff,
                    equity_price=float(candidate["confirmation_close"]),
                    side=str(candidate["side"]), config=v8.FuturesFeatureConfig(),
                ))
            rows.append(record)
    features = pd.DataFrame(rows)
    if features.empty:
        return {"candidate_rows": 0, "status_counts": {}, "sources": []}
    features["period"] = period_labels(features["day"])
    features.to_parquet(output_dir / "v8_strict_candidate_causal_coverage.parquet", index=False)
    coverage = features.groupby(["period", "v8_feature_status"], dropna=False).size().rename("candidates").reset_index()
    coverage.to_csv(output_dir / "v8_causal_coverage_summary.csv", index=False)
    pd.DataFrame(sources).to_csv(output_dir / "v8_futures_source_manifest.csv", index=False)
    return {
        "candidate_rows": len(features),
        "status_counts": features["v8_feature_status"].value_counts().to_dict(),
        "v8_feature_schema": v8.SCHEMA_VERSION,
        "v8_source_sha256": hashlib.sha256(Path(v8.__file__).read_bytes()).hexdigest(),
        "role": "EXISTING_V8_IS_A_CAUSAL_FUTURES_FEATURE_SHADOW_NOT_AN_INDEPENDENT_PNL_ENGINE",
        "causal_boundary": "confirmation close + 1 minute, input futures timestamps strictly before boundary",
        "use": "COVERAGE_ONLY_NO_NEW_SELECTION_HYPOTHESIS",
        "sources": sources,
    }


def run_diagnostics(
    dataset: dict[str, Any],
    output_dir: Path,
    *,
    minute_loader: MinuteLoader = load_raw_equity_minutes,
) -> dict[str, Any]:
    """Write selection audits and counterfactual evidence without strategy tuning."""
    started = time.perf_counter()
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    audit = add_decision_feature_views(dataset["setup_audit"])
    print(f"[V13-v9 DIAGNOSTICS] {len(audit):,} complete setup observations; describing causal filters", flush=True)
    audit["period"] = period_labels(audit["day"])
    if "selection_stage" not in audit:
        if "selection_status" in audit:
            audit["selection_stage"] = audit["selection_status"].fillna("UNKNOWN")
            rejected = audit["selection_stage"].eq("FILTER_REJECTED")
            first = audit["causal_rejection_reasons"].fillna("").str.split("|").str[0]
            audit.loc[rejected, "selection_stage"] = "REJECT_" + first.loc[rejected]
        elif "first_rejection_reason" in audit:
            audit["selection_stage"] = audit["first_rejection_reason"].fillna("UNKNOWN")
        elif "rejection_reason" in audit:
            audit["selection_stage"] = audit["rejection_reason"].fillna("UNKNOWN")
        else:
            raise ValueError("setup_audit requires selection_stage or first_rejection_reason")
        selected = as_bool(audit.get("baseline_selected", pd.Series(False, index=audit.index)))
        audit.loc[selected, "selection_stage"] = "BASELINE_SELECTED"
    audit.groupby(["period", "setup_id", "selection_stage"], dropna=False).size().rename("observations").reset_index().to_csv(output_dir / "selection_rejection_funnel.csv", index=False)
    descriptions = feature_descriptions(audit, ["period", "setup_id", "selection_stage"])
    descriptions.to_csv(output_dir / "causal_feature_descriptions.csv", index=False)
    if "causal_rejection_reasons" in audit:
        failures = audit[["period", "setup_id", "causal_rejection_reasons"]].copy()
        failures["failed_check"] = failures["causal_rejection_reasons"].fillna("").str.split("|")
        failures = failures.explode("failed_check")
        failures = failures.loc[failures["failed_check"].ne("")]
        failures.groupby(["period", "setup_id", "failed_check"], dropna=False).size().rename("candidates_failing_check_nonexclusive").reset_index().to_csv(output_dir / "all_causal_rejection_reasons.csv", index=False)
    observed = dataset.get("all_5m_features", pd.DataFrame()).copy()
    if not observed.empty:
        observed["period"] = period_labels(observed["day"])
        observed["missing_exact_oi"] = pd.to_numeric(observed["oi"], errors="coerce").isna()
        observed["missing_oi_change"] = pd.to_numeric(observed["oi_change_pct"], errors="coerce").isna()
        observed["missing_exact_confirmation"] = ~as_bool(observed["v9_exact_confirmation_present"])
        native_times = set(audit["hhmm_int"].astype(int))
        observed["outside_configured_setup_times"] = ~observed["hhmm_int"].isin(native_times)
        observed.groupby(["period", "hhmm_int"], dropna=False).agg(
            observations=("tradingsymbol", "size"), symbols=("tradingsymbol", "nunique"),
            sessions=("day", "nunique"), missing_exact_oi=("missing_exact_oi", "sum"),
            missing_oi_change=("missing_oi_change", "sum"),
            missing_exact_confirmation=("missing_exact_confirmation", "sum"),
            outside_configured_setup_times=("outside_configured_setup_times", "sum"),
        ).reset_index().to_csv(output_dir / "all_5m_observation_coverage.csv", index=False)
    print("[V13-v9 DIAGNOSTICS] Checking actual raw confirmation and forward minutes for every setup row", flush=True)
    raw_orders, raw_paths, quality = checked_candidate_paths(audit, minute_loader=minute_loader)
    print(f"[V13-v9 DIAGNOSTICS] {len(raw_paths):,} verified paths; replaying fixed and native scaleout payoffs", flush=True)
    broad = _replay_checked_candidates(raw_orders, raw_paths, quality)
    matched_native = _replay_checked_candidates(raw_orders, raw_paths, quality, native_scaleout=True)
    del raw_orders, raw_paths
    broad.to_parquet(output_dir / "fixed_bracket_counterfactuals.parquet", index=False)
    matched_native.to_parquet(output_dir / "native_scaleout_all_setup_counterfactuals.parquet", index=False)
    quality.to_csv(output_dir / "counterfactual_path_quality.csv", index=False)
    outcome_summary(broad, ["period", "setup_id", "selection_stage", "counterfactual_status"]).to_csv(output_dir / "fixed_bracket_outcome_summary.csv", index=False)
    outcome_summary(matched_native, ["period", "setup_id", "selection_stage", "counterfactual_status"]).to_csv(output_dir / "native_scaleout_all_setup_outcome_summary.csv", index=False)
    bins, stability = feature_bin_outcomes(broad)
    bins.to_csv(output_dir / "development_feature_bin_outcomes.csv", index=False)
    stability.to_csv(output_dir / "development_feature_bin_stability.csv", index=False)
    matched_bins, matched_stability = feature_bin_outcomes(matched_native)
    matched_bins.to_csv(output_dir / "matched_native_development_feature_bin_outcomes.csv", index=False)
    matched_stability.to_csv(output_dir / "matched_native_development_feature_bin_stability.csv", index=False)
    print("[V13-v9 DIAGNOSTICS] Broad counterfactuals written; comparing native selected and ranked-out candidates", flush=True)
    # The primary comparison retains the EXACT V13 partial+runner payoff.
    # It includes every strict setup-eligible row, not only the top-N winners.
    import fno_v13_v9_backtest as engine

    config = engine.V9Config(name="DIAGNOSTIC_ALL_NATIVE_ELIGIBLE", leverage_factor=1.0)
    eligible = add_decision_feature_views(engine.selection_audit(dataset["signals"], config))
    quality_by_source = {
        int(row["source_sid"]): row["counterfactual_status"]
        for row in broad[["source_sid", "counterfactual_status"]].to_dict("records")
        if pd.notna(row.get("source_sid"))
    }
    eligible["counterfactual_status"] = eligible["sid"].map(quality_by_source).fillna("MISSING_CHECKED_NATIVE_PATH")
    complete = eligible["counterfactual_status"].str.startswith("RESOLVED_")
    available = eligible.loc[complete].copy()
    replayed = engine.replay_candidates(available, dataset["paths"], config)
    added = [name for name in replayed if name not in eligible or name == "sid"]
    strict = eligible.merge(replayed[added], on="sid", how="left", validate="one_to_one")
    strict["filled"] = as_bool(strict["filled"])
    strict.loc[complete & strict["filled"], "counterfactual_status"] = "RESOLVED_FILLED"
    strict.loc[complete & ~strict["filled"], "counterfactual_status"] = "RESOLVED_NO_TRIGGER_WITHIN_S10"
    # Monetary columns on overlapping observations are particularly easy to
    # misread as portfolio P&L, so retain only per-trade return fields here.
    strict = strict.drop(columns=[name for name in strict if "rupees" in name or "return_on_capital" in name], errors="ignore")
    strict["selection_stage"] = np.where(as_bool(strict["v9_selected"]), "BASELINE_SELECTED", "RANKED_OUT")
    strict["period"] = period_labels(strict["day"])
    strict["outcome_limitations"] = OUTCOME_LIMITATION
    strict.to_parquet(output_dir / "native_scaleout_eligible_counterfactuals.parquet", index=False)
    outcome_summary(strict, ["period", "setup_id", "selection_stage", "counterfactual_status"]).to_csv(output_dir / "native_scaleout_outcome_summary.csv", index=False)
    native_bins, native_stability = feature_bin_outcomes(strict)
    native_bins.to_csv(output_dir / "native_development_feature_bin_outcomes.csv", index=False)
    native_stability.to_csv(output_dir / "native_development_feature_bin_stability.csv", index=False)
    winners = strict.loc[as_bool(strict["filled"])].copy()
    winners["outcome_class"] = np.where(pd.to_numeric(winners["net_return_pct"], errors="coerce").gt(0), "WINNER", "NON_WINNER")
    feature_descriptions(winners, ["period", "setup_id", "selection_stage", "outcome_class"]).to_csv(output_dir / "native_winner_loser_feature_descriptions.csv", index=False)
    v8_context = v8_coverage_context(eligible, output_dir)
    result = {
        "schema_version": SCHEMA_VERSION,
        "source_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "dataset_source_fingerprint": dataset.get("manifest", {}).get("source_fingerprint"),
        "counterfactual_rule": COUNTERFACTUAL_RULE,
        "stop_pct": 1.5, "target_pct": 2.6, "cost_bps": 5.0,
        "entry_window_minutes_after_confirmation": 10,
        "first_entry_minute": "confirmation_ts + 1 minute; never confirmation candle",
        "exit_cutoff": "15:15 Asia/Kolkata end-labelled minute",
        "ambiguity_policy": "STOP_FIRST; adverse later-bar opening gaps charged",
        "data_policy": "Exact valid real confirmation and complete real minute path required",
        "selection_definition": "BASELINE_SELECTED is native per-setup top-N before portfolio capital allocation; this audit ignores portfolio acceptance",
        "broad_rejection_definition": "Hypothetical orders at a valid exact confirmation-bar extreme even when directional confirmation or other native filters failed; not claims of actual signals",
        "outcome_limitations": OUTCOME_LIMITATION,
        "evidence_status": "ALL_HISTORY_PREVIOUSLY_SEEN_NO_UNTOUCHED_TEST",
        "feature_bins_use": "TRAIN_AND_VALIDATION_ONLY_FIXED_BINS",
        "setup_rows": len(audit),
        "all_5m_observations": len(observed),
        "strict_eligible_counterfactual_rows": len(strict),
        "strict_eligible_selected": int(as_bool(strict["v9_selected"]).sum()),
        "strict_eligible_ranked_out": int((~as_bool(strict["v9_selected"])).sum()),
        "strict_counterfactual_status_counts": strict["counterfactual_status"].value_counts().to_dict(),
        "native_counterfactual_rule": NATIVE_COUNTERFACTUAL_RULE,
        "primary_broad_payoff": "native_scaleout_all_setup_counterfactuals.parquet; SAME partial+runner exits for selections and ALL valid active-slot rejects",
        "secondary_broad_payoff": "fixed_bracket_counterfactuals.parquet; descriptive fixed 1.5/2.6 bracket only",
        "v8_context": v8_context,
        "diagnostics_runtime_seconds": time.perf_counter() - started,
        "counterfactual_status_counts": broad["counterfactual_status"].value_counts().to_dict(),
        "output_dir": str(output_dir.resolve()),
    }
    (output_dir / "diagnostics_manifest.json").write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(f"[V13-v9 DIAGNOSTICS] Finished all {len(audit):,} setup observations in {result['diagnostics_runtime_seconds']:.1f}s", flush=True)
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--through-day", default="2026-09-11")
    args = parser.parse_args(argv)
    import fno_v13_v9_data as data

    dataset = data.build_dataset(args.dataset_dir, through_day=args.through_day)
    result = run_diagnostics(dataset, args.output_dir)
    print(json.dumps(result, indent=2), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
