"""Research-only V13-v8 causal futures feature layer.

Features are computed at the stock entry boundary from completed futures
minutes only. Time-of-day baselines use the same minute window on strictly
earlier sessions of the same futures contract. Outcomes are joined only after
feature construction for descriptive shadow analysis; this module cannot
promote or alter the V13-v5 control strategy.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

import fno_oi_common as common
import fno_v13_v6_portfolio_backtest as portfolio


SCHEMA_VERSION = "FNO_V13_V8_CAUSAL_FUTURES_FEATURE_SHADOW_V1"
DEFAULT_STOCK_SOURCE = portfolio.DEFAULT_SOURCE
DEFAULT_DERIVATIVE_ROOT = (
    common.FNO_ROOT / "strategy_research" / "v13_corrected_v5" / "derivative_market_data"
)
DEFAULT_OUTPUT_ROOT = common.FNO_ROOT / "strategy_research" / "v13_corrected_v8_feature_shadow"


@dataclass(frozen=True)
class FuturesFeatureConfig:
    window_minutes: int = 5
    prior_sessions: int = 20
    minimum_prior_sessions: int = 3
    volume_floor_ratio: float = 0.50
    maximum_abs_oi_robust_z: float = 4.0
    minimum_signed_return_pct: float = -0.10
    maximum_abs_basis_pct: float = 3.0

    def validate(self) -> None:
        if self.window_minutes <= 0:
            raise ValueError("window_minutes must be positive")
        if self.prior_sessions < self.minimum_prior_sessions:
            raise ValueError("prior_sessions must cover minimum_prior_sessions")
        if self.minimum_prior_sessions < 1:
            raise ValueError("minimum_prior_sessions must be positive")
        if self.volume_floor_ratio < 0:
            raise ValueError("volume_floor_ratio cannot be negative")
        if self.maximum_abs_oi_robust_z <= 0 or self.maximum_abs_basis_pct <= 0:
            raise ValueError("absolute feature limits must be positive")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def normalize_futures_candles(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    source = "timestamp" if "timestamp" in out else "date" if "date" in out else "ts"
    out["timestamp"] = common._to_ist(out[source])
    for column in ("open", "high", "low", "close", "volume", "oi"):
        if column not in out:
            out[column] = np.nan
        out[column] = pd.to_numeric(out[column], errors="coerce")
    return (
        out.loc[out["timestamp"].notna() & out[["open", "close"]].notna().all(axis=1)]
        .sort_values("timestamp", kind="stable")
        .drop_duplicates("timestamp", keep="last")
        .reset_index(drop=True)
    )


def _minute_of_day(series: pd.Series) -> pd.Series:
    return series.dt.hour * 60 + series.dt.minute


def _window_metrics(frame: pd.DataFrame) -> dict[str, float]:
    if frame.empty:
        return {"volume": np.nan, "oi_change_pct": np.nan, "return_pct": np.nan}
    volume = float(frame["volume"].fillna(0).clip(lower=0).sum())
    first_oi, last_oi = float(frame["oi"].iloc[0]), float(frame["oi"].iloc[-1])
    first_close, last_close = float(frame["close"].iloc[0]), float(frame["close"].iloc[-1])
    return {
        "volume": volume,
        "oi_change_pct": (
            (last_oi / first_oi - 1.0) * 100.0
            if np.isfinite(first_oi) and first_oi > 0 and np.isfinite(last_oi)
            else np.nan
        ),
        "return_pct": (
            (last_close / first_close - 1.0) * 100.0
            if np.isfinite(first_close) and first_close > 0 and np.isfinite(last_close)
            else np.nan
        ),
    }


def _robust_z(value: float, history: pd.Series) -> float:
    clean = pd.to_numeric(history, errors="coerce").dropna()
    if not np.isfinite(value) or clean.empty:
        return np.nan
    median = float(clean.median())
    mad = float((clean - median).abs().median())
    if mad <= 1e-12:
        return 0.0 if abs(value - median) <= 1e-12 else np.sign(value - median) * np.inf
    return float((value - median) / (1.4826 * mad))


def causal_futures_features(
    candles: pd.DataFrame,
    *,
    entry_ts: pd.Timestamp,
    equity_price: float,
    side: str,
    config: FuturesFeatureConfig,
) -> dict[str, Any]:
    config.validate()
    entry_ts = common._to_ist(pd.Series([entry_ts])).iloc[0]
    history = normalize_futures_candles(candles)
    day = entry_ts.date()
    cutoff_minute = entry_ts.hour * 60 + entry_ts.minute
    start_minute = cutoff_minute - config.window_minutes
    minutes = _minute_of_day(history["timestamp"])
    current = history.loc[
        history["timestamp"].dt.date.eq(day)
        & minutes.ge(start_minute)
        & minutes.lt(cutoff_minute)
        & history["timestamp"].lt(entry_ts)
    ]
    prior = history.loc[
        history["timestamp"].dt.date.lt(day)
        & minutes.ge(start_minute)
        & minutes.lt(cutoff_minute)
    ].copy()
    prior_days = sorted(prior["timestamp"].dt.date.unique(), reverse=True)[: config.prior_sessions]
    prior = prior.loc[prior["timestamp"].dt.date.isin(prior_days)]
    baseline_rows = [
        {"day": prior_day, **_window_metrics(group)}
        for prior_day, group in prior.groupby(prior["timestamp"].dt.date, sort=True)
        if len(group) >= config.window_minutes
    ]
    baseline = pd.DataFrame(baseline_rows)
    current_metrics = _window_metrics(current)
    baseline_count = len(baseline)
    volume_median = (
        float(baseline["volume"].median()) if baseline_count else np.nan
    )
    volume_ratio = (
        current_metrics["volume"] / volume_median
        if np.isfinite(current_metrics["volume"])
        and np.isfinite(volume_median)
        and volume_median > 0
        else np.nan
    )
    last_futures_close = float(current["close"].iloc[-1]) if not current.empty else np.nan
    basis_pct = (
        (last_futures_close / equity_price - 1.0) * 100.0
        if np.isfinite(last_futures_close) and np.isfinite(equity_price) and equity_price > 0
        else np.nan
    )
    side_sign = 1.0 if str(side).upper() == "LONG" else -1.0
    oi_z = (
        _robust_z(current_metrics["oi_change_pct"], baseline["oi_change_pct"])
        if baseline_count
        else np.nan
    )
    return_z = (
        _robust_z(current_metrics["return_pct"], baseline["return_pct"])
        if baseline_count
        else np.nan
    )
    ready = bool(
        len(current) >= config.window_minutes
        and baseline_count >= config.minimum_prior_sessions
        and np.isfinite(volume_ratio)
        and np.isfinite(oi_z)
        and np.isfinite(basis_pct)
    )
    checks = {
        "v8_volume_not_collapsing": bool(ready and volume_ratio >= config.volume_floor_ratio),
        "v8_oi_impulse_not_extreme": bool(ready and abs(oi_z) <= config.maximum_abs_oi_robust_z),
        "v8_futures_price_side_aligned": bool(
            ready and side_sign * current_metrics["return_pct"] >= config.minimum_signed_return_pct
        ),
        "v8_basis_not_extreme": bool(ready and abs(basis_pct) <= config.maximum_abs_basis_pct),
    }
    return {
        "v8_feature_status": "READY" if ready else "INSUFFICIENT_CAUSAL_HISTORY",
        "v8_feature_cutoff_exclusive": entry_ts,
        "v8_feature_max_timestamp": current["timestamp"].max() if not current.empty else pd.NaT,
        "v8_baseline_max_day": max(prior_days).isoformat() if prior_days else None,
        "v8_current_window_rows": int(len(current)),
        "v8_prior_session_count": int(baseline_count),
        "fut_volume_window": current_metrics["volume"],
        "fut_volume_tod_median_prior": volume_median,
        "fut_volume_tod_ratio": volume_ratio,
        "fut_oi_change_window_pct": current_metrics["oi_change_pct"],
        "fut_oi_change_tod_robust_z": oi_z,
        "fut_return_window_pct": current_metrics["return_pct"],
        "fut_return_tod_robust_z": return_z,
        "fut_last_close": last_futures_close,
        "fut_cash_basis_pct": basis_pct,
        "fut_cash_basis_side_signed_pct": side_sign * basis_pct,
        **checks,
        "v8_shadow_quality_points": int(sum(checks.values())) if ready else 0,
        "v8_shadow_pass": bool(ready and all(checks.values())),
    }


def load_futures_coverage(data_roots: list[Path]) -> tuple[pd.DataFrame, list[dict[str, str]]]:
    parts: list[pd.DataFrame] = []
    sources: list[dict[str, str]] = []
    for priority, supplied in enumerate(data_roots):
        root = supplied.resolve()
        path = root / "audit" / "futures_trade_coverage_and_capital.csv"
        if not path.is_file():
            raise FileNotFoundError(f"Missing futures coverage: {path}")
        frame = pd.read_csv(path)
        frame["_source_priority"] = priority
        frame["_data_root"] = str(root)
        parts.append(frame)
        sources.append({"path": str(path), "sha256": _sha256(path)})
    combined = pd.concat(parts, ignore_index=True).sort_values("_source_priority", kind="stable")
    combined = combined.drop_duplicates("trade_id", keep="last").reset_index(drop=True)
    return combined, sources


def build_feature_dataset(
    stock_trades: pd.DataFrame,
    futures_coverage: pd.DataFrame,
    config: FuturesFeatureConfig,
) -> pd.DataFrame:
    coverage_by_key = {
        (
            pd.Timestamp(row["day"]).date().isoformat(),
            str(row.get("equity_symbol", "")).upper(),
            common._to_ist(pd.Series([row["equity_entry_ts"]])).iloc[0].isoformat(),
        ): row
        for row in futures_coverage.to_dict("records")
    }
    cache: dict[str, pd.DataFrame] = {}
    records: list[dict[str, Any]] = []
    for stock in stock_trades.to_dict("records"):
        record = {
            "sid": stock.get("sid"),
            "day": stock.get("day"),
            "tradingsymbol": stock.get("tradingsymbol"),
            "side": stock.get("side"),
            "entry_ts": stock.get("entry_ts"),
            "filled": stock.get("filled"),
        }
        coverage = coverage_by_key.get(
            (
                pd.Timestamp(stock["day"]).date().isoformat(),
                str(stock.get("tradingsymbol", "")).upper(),
                common._to_ist(pd.Series([stock["entry_ts"]])).iloc[0].isoformat(),
            )
        )
        if coverage is None or str(coverage.get("coverage_state")) != "READY":
            record["v8_feature_status"] = "MISSING_READY_FUTURES_COVERAGE"
        else:
            preferred_path = Path(str(coverage.get("retained_local_1m_path", "")))
            package_path = Path(str(coverage.get("minute_package_path", "")))
            candle_path = preferred_path if preferred_path.is_file() else package_path
            if not candle_path.is_file():
                record["v8_feature_status"] = "MISSING_FUTURES_CANDLE_FILE"
            else:
                cache_key = str(candle_path.resolve())
                if cache_key not in cache:
                    cache[cache_key] = pd.read_parquet(candle_path)
                equity_price = float(
                    pd.to_numeric(stock.get("confirmation_close"), errors="coerce")
                )
                if not np.isfinite(equity_price):
                    equity_price = float(pd.to_numeric(stock.get("entry_price"), errors="coerce"))
                record.update(
                    causal_futures_features(
                        cache[cache_key],
                        entry_ts=pd.Timestamp(stock["entry_ts"]),
                        equity_price=equity_price,
                        side=str(stock.get("side", "")),
                        config=config,
                    )
                )
                record["futures_candle_path"] = str(candle_path.resolve())
                record["futures_tradingsymbol"] = coverage.get("futures_tradingsymbol")

        # Outcome columns are appended after causal feature computation and are
        # never inputs to the shadow checks above.
        record.update(
            {
                "outcome_net_profit_rupees": pd.to_numeric(
                    stock.get("net_profit_rupees"), errors="coerce"
                ),
                "outcome_net_return_on_capital_pct": pd.to_numeric(
                    stock.get("net_return_on_capital_pct"), errors="coerce"
                ),
                "outcome_exit_reason": stock.get("exit_reason"),
            }
        )
        records.append(record)
    return pd.DataFrame(records)


def descriptive_summary(features: pd.DataFrame) -> pd.DataFrame:
    ready = features.loc[features["v8_feature_status"].eq("READY")].copy()
    if ready.empty:
        return pd.DataFrame()
    ready["period"] = pd.to_datetime(ready["day"]).dt.to_period("M").astype(str)
    rows: list[dict[str, Any]] = []
    for (period, shadow_pass), group in ready.groupby(["period", "v8_shadow_pass"], dropna=False):
        pnl = pd.to_numeric(group["outcome_net_profit_rupees"], errors="coerce").dropna()
        wins = pnl.loc[pnl > 0]
        losses = pnl.loc[pnl < 0]
        rows.append(
            {
                "period": period,
                "v8_shadow_pass": bool(shadow_pass),
                "trades": int(len(pnl)),
                "wins": int((pnl > 0).sum()),
                "losses": int((pnl < 0).sum()),
                "win_rate_pct": float((pnl > 0).mean() * 100.0) if len(pnl) else np.nan,
                "net_profit_rupees": float(pnl.sum()),
                "profit_factor": (
                    float(wins.sum() / abs(losses.sum())) if not losses.empty else np.inf
                ),
                "average_pnl_rupees": float(pnl.mean()) if len(pnl) else np.nan,
            }
        )
    return pd.DataFrame(rows).sort_values(["period", "v8_shadow_pass"])


def render_report(features: pd.DataFrame, summary: pd.DataFrame) -> str:
    status_counts = features["v8_feature_status"].value_counts(dropna=False).to_dict()
    lines = [
        "# V13-v8 Causal Futures Feature Shadow",
        "",
        "Status: **RESEARCH ONLY — NOT PROMOTED**",
        "",
        "Features use completed futures minutes before equity entry. Time-of-day baselines use only earlier sessions of the same contract. Outcome P&L is attached afterward for descriptive analysis.",
        "",
        f"- Input trades: {len(features)}",
        f"- Feature status counts: `{json.dumps(status_counts, sort_keys=True)}`",
        "",
    ]
    if not summary.empty:
        lines.extend([summary.to_markdown(index=False), ""])
    lines.extend(
        [
            "The shadow pass thresholds are engineering guardrails, not an optimized strategy. They require walk-forward validation before any live or backtest selection change.",
            "",
        ]
    )
    return "\n".join(lines)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--stock-source", type=Path, default=DEFAULT_STOCK_SOURCE)
    parser.add_argument("--data-root", type=Path, action="append", dest="data_roots")
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--run-id")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    config = FuturesFeatureConfig()
    source_path = args.stock_source.resolve()
    roots = args.data_roots or [DEFAULT_DERIVATIVE_ROOT]
    stock = pd.read_csv(source_path)
    coverage, coverage_sources = load_futures_coverage(roots)
    features = build_feature_dataset(stock, coverage, config)
    summary = descriptive_summary(features)

    generated = common.now_ist()
    run_id = args.run_id or generated.strftime("v13_v8_features_%Y%m%dT%H%M%S_IST")
    if any(character not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-" for character in run_id):
        raise ValueError("run-id may contain only letters, digits, underscore and hyphen")
    run_dir = args.output_root.resolve() / run_id
    if run_dir.exists():
        raise FileExistsError(f"Refusing to overwrite V13-v8 feature run: {run_dir}")
    run_dir.mkdir(parents=True)
    features_path = run_dir / "fno_v13_v8_causal_features.csv"
    summary_path = run_dir / "fno_v13_v8_descriptive_summary.csv"
    report_path = run_dir / "V13_V8_CAUSAL_FEATURE_REPORT.md"
    common.atomic_write_csv(features, features_path)
    common.atomic_write_csv(summary, summary_path)
    common.atomic_write_text(report_path, render_report(features, summary))
    days = pd.to_datetime(features.get("day"), errors="coerce")
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "complete": True,
        "promotion_status": "RESEARCH_ONLY_NOT_PROMOTED",
        "run_id": run_id,
        "generated_at_ist": generated.isoformat(timespec="seconds"),
        "data_through_date": days.max().date().isoformat() if days.notna().any() else None,
        "source": str(source_path),
        "source_sha256": _sha256(source_path),
        "coverage_sources": coverage_sources,
        "config": asdict(config),
        "outputs": {
            "features": {"path": str(features_path), "sha256": _sha256(features_path)},
            "summary": {"path": str(summary_path), "sha256": _sha256(summary_path)},
            "report": {"path": str(report_path), "sha256": _sha256(report_path)},
        },
    }
    common.atomic_write_json(run_dir / "manifest.json", manifest)
    print(features["v8_feature_status"].value_counts(dropna=False).to_string())
    print(summary.to_string(index=False))
    print(f"[V13-v8 features][SHADOW RUN] {run_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
