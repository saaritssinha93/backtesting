"""Ungated OI ablation for the point-in-time corrected V6 data set.

This file is deliberately separate from ``fno_v6_corrected_backtest.py``.
It rebuilds the V6 candidate table for the same 23 rolling-near-month
sessions, but captures candidates *before* the historical hard-coded
``oi > prev_oi`` and ``oi_change_pct >= 0.05`` signal-builder gate.  Every
other signal, confirmation, entry, exit, setup, and cost rule is imported
from corrected V6.

The small policy list is declared in advance and is an ablation, not a large
parameter search:

* no OI rule;
* valid current/previous OI only;
* positive versus negative OI (the price/OI quadrants, after V6 fixes price
  direction by LONG/SHORT);
* current corrected-V6 OI thresholds;
* two consecutive positive completed 5-minute OI changes;
* current V6 thresholds plus a positive immediately-prior 5-minute change;
* positive causal ten-minute cumulative OI change.

Nothing in this program writes to the corrected-V6 source or result folder.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import time
from contextlib import contextmanager
from dataclasses import replace
from datetime import date
from pathlib import Path
from typing import Any, Iterator

import numpy as np
import pandas as pd

import fno_oi_backtest_provenance as provenance
import fno_oi_common as common
import fno_oi_ema_confirm_sweep as signal_builder
import fno_oi_hybrid_data as hybrid
import fno_v5_hybrid_backtest as replay
import fno_v6_corrected_backtest as corrected


RESEARCH_VERSION = "V13_CORRECTED_UNGATED_OI_ABLATION_V1"
EXPECTED_V6_SHA256 = (
    "06baf32c33156f21bce1dc786e5687a250b9711a1bca3a186283c824edfcf62d"
)
PINNED_POSITIVE_CACHE_STEMS = {
    "26AUG": "26AUG_4ade722c23dcf5c5",
    "26SEP": "26SEP_16fbb1d141241ff3",
}
SPLIT_DAY = date(2026, 8, 14)
SQUARE_OFF = "1530"
MAX_FORWARD_BARS = 400
COST_BPS = 5.0

RESULT_DIR = common.FNO_ROOT / "strategy_research" / "v13_corrected_ungated_oi_research"
CACHE_DIR = RESULT_DIR / "_cache"
SIGNALS_PATH = RESULT_DIR / "ungated_candidate_signals.parquet"
PATHS_PATH = RESULT_DIR / "ungated_forward_paths.npz"
METRICS_PATH = RESULT_DIR / "oi_policy_metrics.csv"
TRADES_PATH = RESULT_DIR / "oi_policy_trades.csv"
DAILY_PATH = RESULT_DIR / "oi_policy_daily.csv"
PARITY_PATH = RESULT_DIR / "current_v6_parity.json"
MANIFEST_PATH = RESULT_DIR / "manifest.json"
REPORT_PATH = RESULT_DIR / "ungated_oi_ablation_report.md"


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _json_safe(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, (date, pd.Timestamp)):
        return str(value)
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        value = float(value)
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return "INF" if value > 0 else ("-INF" if value < 0 else None)
    return value


def _load_reference_days() -> tuple[dict[str, list[date]], dict[str, str]]:
    """Use the exact days represented by the already-validated V6 caches."""
    days_by_month: dict[str, list[date]] = {}
    hashes: dict[str, str] = {}
    for month, stem_name in PINNED_POSITIVE_CACHE_STEMS.items():
        stem = corrected.CACHE_DIR / stem_name
        for suffix in (".parquet", ".npz"):
            path = stem.with_suffix(suffix)
            if not path.exists():
                raise FileNotFoundError(f"Corrected-V6 reference cache missing: {path}")
            hashes[path.name] = _sha256(path)
        cached = corrected._load_cached(stem)
        if cached is None:
            raise RuntimeError(f"Incomplete corrected-V6 reference cache: {stem}")
        frame, _ = cached
        days_by_month[month] = sorted(set(pd.to_datetime(frame["day"]).dt.date))
    all_days = sorted({day for values in days_by_month.values() for day in values})
    if len(all_days) != 23:
        raise AssertionError(f"Expected 23 corrected-V6 sessions, found {len(all_days)}")
    return days_by_month, hashes


def _oi_lookup(symbol: str, futures: pd.DataFrame) -> pd.DataFrame:
    """Causal OI features, with exact-bar continuity explicit."""
    if futures.empty:
        return pd.DataFrame()
    out = futures[["ts", "oi", "volume", "close"]].copy().sort_values("ts")
    out["futures_tradingsymbol"] = symbol
    out["oi"] = pd.to_numeric(out["oi"], errors="coerce")
    out["futures_volume_5m"] = pd.to_numeric(out["volume"], errors="coerce")

    # Legacy values reproduce the corrected-V6 implementation exactly.
    out["prev_oi"] = out["oi"].shift(1)
    legacy_valid = (
        out["oi"].gt(0)
        & out["prev_oi"].gt(0)
        & np.isfinite(out["oi"])
        & np.isfinite(out["prev_oi"])
    )
    out["oi_valid"] = legacy_valid
    out["oi_delta"] = np.where(legacy_valid, out["oi"] - out["prev_oi"], np.nan)
    out["oi_change_pct"] = np.where(
        legacy_valid, (out["oi"] / out["prev_oi"] - 1.0) * 100.0, np.nan
    )

    ts = pd.to_datetime(out["ts"], utc=True).dt.tz_convert("Asia/Kolkata")
    exact_one = ts.sub(ts.shift(1)).eq(pd.Timedelta(minutes=5))
    same_day_one = ts.dt.date == ts.shift(1).dt.date
    contiguous_one = exact_one & same_day_one & legacy_valid
    out["oi_contiguous_5m"] = contiguous_one
    out["oi_change_pct_contiguous"] = np.where(
        contiguous_one, (out["oi"] / out["prev_oi"] - 1.0) * 100.0, np.nan
    )

    prior_change = out["oi_change_pct_contiguous"].shift(1)
    prior_exact = exact_one & exact_one.shift(1, fill_value=False)
    out["oi_change_prev5_pct"] = np.where(prior_exact, prior_change, np.nan)

    prev2 = out["oi"].shift(2)
    exact_two = ts.sub(ts.shift(2)).eq(pd.Timedelta(minutes=10))
    same_day_two = ts.dt.date == ts.shift(2).dt.date
    valid_two = (
        exact_two
        & same_day_two
        & exact_one
        & exact_one.shift(1, fill_value=False)
        & out["oi"].gt(0)
        & prev2.gt(0)
        & np.isfinite(out["oi"])
        & np.isfinite(prev2)
    )
    out["oi_contiguous_10m"] = valid_two
    out["oi_change_10m_pct"] = np.where(
        valid_two, (out["oi"] / prev2 - 1.0) * 100.0, np.nan
    )

    return out[
        [
            "futures_tradingsymbol",
            "ts",
            "oi",
            "prev_oi",
            "oi_valid",
            "oi_delta",
            "oi_change_pct",
            "oi_contiguous_5m",
            "oi_change_pct_contiguous",
            "oi_change_prev5_pct",
            "oi_contiguous_10m",
            "oi_change_10m_pct",
            "futures_volume_5m",
        ]
    ]


@contextmanager
def _ungated_builder_patch() -> Iterator[dict[str, pd.DataFrame]]:
    """Temporarily neutralize only the builder's hard OI prefilter.

    Real OI is restored from ``captured`` immediately after signal creation.
    The patch is process-local and is always undone by ``finally``.
    """
    original_join = hybrid.join_equity_price_with_futures_oi
    original_load = signal_builder.load_five_minute_history
    original_last_slot = signal_builder.LAST_SIGNAL_SLOT
    captured: dict[str, pd.DataFrame] = {}

    def capturing_load(symbol: str, *, root: Path | None = None) -> pd.DataFrame:
        frame = original_load(symbol, root=root)
        captured[str(symbol)] = frame.copy()
        return frame

    def ungated_join(equity_frame: pd.DataFrame, futures_frame: pd.DataFrame) -> pd.DataFrame:
        merged = original_join(equity_frame, futures_frame)
        if merged.empty:
            return merged
        merged = merged.copy()
        # Sentinel values pass the builder's two OI clauses. They are never
        # persisted: _enrich_real_oi replaces all three fields before caching.
        merged["oi"] = 2.0
        merged["prev_oi"] = 1.0
        merged["oi_change_pct"] = 100.0
        return merged

    signal_builder.load_five_minute_history = capturing_load
    hybrid.join_equity_price_with_futures_oi = ungated_join
    signal_builder.LAST_SIGNAL_SLOT = "0945"
    try:
        yield captured
    finally:
        signal_builder.load_five_minute_history = original_load
        hybrid.join_equity_price_with_futures_oi = original_join
        signal_builder.LAST_SIGNAL_SLOT = original_last_slot


def _enrich_real_oi(
    signals: pd.DataFrame, captured: dict[str, pd.DataFrame]
) -> pd.DataFrame:
    lookups = [_oi_lookup(symbol, frame) for symbol, frame in captured.items()]
    lookups = [frame for frame in lookups if not frame.empty]
    if not lookups:
        raise RuntimeError("No futures OI rows were captured during ungated build.")
    lookup = pd.concat(lookups, ignore_index=True)
    lookup["signal_ts_ns"] = pd.to_datetime(lookup.pop("ts"), utc=True).astype("int64")

    out = signals.drop(columns=["oi", "prev_oi", "oi_change_pct"]).copy()
    signal_ts = pd.to_datetime(out["confirmation_ts"], utc=True) - pd.Timedelta(minutes=1)
    out["signal_ts_ns"] = signal_ts.astype("int64")
    out = out.merge(
        lookup,
        on=["futures_tradingsymbol", "signal_ts_ns"],
        how="left",
        validate="many_to_one",
    )
    if out["oi_valid"].isna().any():
        missing = int(out["oi_valid"].isna().sum())
        raise AssertionError(f"Could not restore real OI for {missing} ungated candidates.")
    out["day"] = pd.to_datetime(out["day"]).dt.date
    return out.sort_values(["day", "sid"], kind="stable").reset_index(drop=True)


def _save_paths(paths: dict[int, dict[str, np.ndarray]], destination: Path) -> None:
    flat: dict[str, np.ndarray] = {}
    for sid, path in paths.items():
        flat[f"{int(sid)}_h"] = path["high"]
        flat[f"{int(sid)}_l"] = path["low"]
        flat[f"{int(sid)}_c"] = path["close"]
    destination.parent.mkdir(parents=True, exist_ok=True)
    np.savez_compressed(destination, **flat)


def _load_paths(source: Path) -> dict[int, dict[str, np.ndarray]]:
    blob = np.load(source)
    suffixes = {"h": "high", "l": "low", "c": "close"}
    paths: dict[int, dict[str, np.ndarray]] = {}
    for key in blob.files:
        sid_text, suffix = key.rsplit("_", 1)
        paths.setdefault(int(sid_text), {})[suffixes[suffix]] = blob[key]
    return paths


def _build_ungated(
    days_by_month: dict[str, list[date]], *, rebuild: bool
) -> tuple[pd.DataFrame, dict[int, dict[str, np.ndarray]], list[dict[str, Any]]]:
    if SIGNALS_PATH.exists() and PATHS_PATH.exists() and not rebuild:
        signals = pd.read_parquet(SIGNALS_PATH)
        signals["day"] = pd.to_datetime(signals["day"]).dt.date
        return signals, _load_paths(PATHS_PATH), [{"cache": "reused"}]

    regimes = corrected.regime_universe_paths()
    parts: list[tuple[pd.DataFrame, dict[int, dict[str, np.ndarray]]]] = []
    records: list[dict[str, Any]] = []
    with _ungated_builder_patch() as captured:
        for month in sorted(days_by_month):
            if month not in regimes:
                raise FileNotFoundError(f"No point-in-time universe for {month}")
            mapped, universe_record = provenance.load_backtest_universe(
                universe_path=regimes[month], contract_month_contains=month
            )
            print(
                f"[BUILD] {month}: {len(mapped)} contracts, "
                f"{len(days_by_month[month])} sessions, OI prefilter disabled",
                flush=True,
            )
            month_signals, month_paths = signal_builder.build_signal_table(
                set(days_by_month[month]),
                square_off=SQUARE_OFF,
                max_forward_bars=MAX_FORWARD_BARS,
                mapped_universe=mapped,
                confirmation_policy=signal_builder.CONFIRMATION_POLICY_V6_STRICT,
            )
            month_signals = _enrich_real_oi(month_signals, captured)
            month_signals["contract_month"] = month
            parts.append((month_signals, month_paths))
            records.append(
                {
                    "contract_month": month,
                    "sessions": len(days_by_month[month]),
                    "universe_path": str(regimes[month].resolve()),
                    "universe_sha256": _sha256(regimes[month]),
                    "universe_record": universe_record,
                    "candidates": len(month_signals),
                }
            )
            # Captures from earlier regimes are harmless, but clearing avoids
            # unnecessarily concatenating their lookup rows in the next pass.
            captured.clear()

    signals, paths = corrected.concat_regimes(parts)
    if signals.empty or not paths:
        raise RuntimeError("Ungated build produced no candidates.")
    if not signals["sid"].is_unique or set(signals["sid"].astype(int)) != set(paths):
        raise AssertionError("Ungated signal/path IDs are not one-to-one.")
    common.atomic_write_parquet(signals, SIGNALS_PATH)
    _save_paths(paths, PATHS_PATH)
    return signals, paths, records


POLICIES: tuple[tuple[str, str], ...] = (
    ("NO_OI", "Ignore OI, including invalid/missing OI pairs"),
    ("VALID_OI_ONLY", "Require finite positive current and previous OI"),
    ("QUADRANT_BUILDUP", "Price-direction signal with positive OI change"),
    ("QUADRANT_UNWINDING", "Price-direction signal with negative OI change"),
    ("CURRENT_V6", "Corrected-V6 per-setup OI percentage thresholds"),
    ("TWO_BAR_POSITIVE", "Two consecutive exact positive 5-minute OI changes"),
    (
        "CURRENT_V6_PLUS_PRIOR_POSITIVE",
        "Current V6 threshold plus an exact positive prior 5-minute OI change",
    ),
    ("TEN_MIN_POSITIVE", "Positive exact ten-minute cumulative OI change"),
)


def _policy_input(
    signals: pd.DataFrame, policy: str
) -> tuple[pd.DataFrame, tuple[Any, ...]]:
    raw = signals["oi_change_pct"]
    if policy == "NO_OI":
        keep = pd.Series(True, index=signals.index)
        use_v6_threshold = False
    elif policy == "VALID_OI_ONLY":
        keep = signals["oi_valid"].astype(bool)
        use_v6_threshold = False
    elif policy == "QUADRANT_BUILDUP":
        keep = signals["oi_valid"].astype(bool) & raw.gt(0)
        use_v6_threshold = False
    elif policy == "QUADRANT_UNWINDING":
        keep = signals["oi_valid"].astype(bool) & raw.lt(0)
        use_v6_threshold = False
    elif policy == "CURRENT_V6":
        keep = pd.Series(True, index=signals.index)
        use_v6_threshold = True
    elif policy == "TWO_BAR_POSITIVE":
        keep = (
            signals["oi_change_pct_contiguous"].gt(0)
            & signals["oi_change_prev5_pct"].gt(0)
        )
        use_v6_threshold = False
    elif policy == "CURRENT_V6_PLUS_PRIOR_POSITIVE":
        keep = signals["oi_change_prev5_pct"].gt(0)
        use_v6_threshold = True
    elif policy == "TEN_MIN_POSITIVE":
        keep = signals["oi_change_10m_pct"].gt(0)
        use_v6_threshold = False
    else:
        raise ValueError(policy)

    trial = signals.loc[keep].copy()
    trial["oi_change_pct_raw"] = trial["oi_change_pct"]
    if use_v6_threshold:
        setups = tuple(corrected.ACTIVE_SETUPS)
    else:
        # No active setup uses max_oi as picker, so neutralising the threshold
        # does not alter any non-OI ranking rule.
        if any(setup.picker == "max_oi" for setup in corrected.ACTIVE_SETUPS):
            raise AssertionError("Cannot neutralise OI: active setup has max_oi picker.")
        trial["oi_change_pct"] = trial["oi_change_pct"].fillna(0.0)
        setups = tuple(replace(setup, oi_change_pct=-1.0e12) for setup in corrected.ACTIVE_SETUPS)
    return trial, setups


def _period_metrics(
    policy: str,
    audit: pd.DataFrame,
    days: list[date],
    period: str,
) -> dict[str, Any]:
    day_set = set(days)
    subset = audit.loc[audit["day"].isin(day_set)].copy()
    values = subset.loc[subset["filled"], "net_return_pct"].to_numpy(float)
    profit = float(values[values > 0].sum()) if values.size else 0.0
    loss = float(-values[values < 0].sum()) if values.size else 0.0
    pf = profit / loss if loss > 0 else (float("inf") if profit > 0 else float("nan"))
    daily_values = (
        subset.loc[subset["filled"]]
        .groupby("day")["net_return_pct"]
        .sum()
        .reindex(days, fill_value=0.0)
        .to_numpy(float)
    )
    curve = np.r_[0.0, np.cumsum(daily_values)]
    drawdown = curve - np.maximum.accumulate(curve)
    positive = values[values > 0]
    top2 = float(np.sort(positive)[-2:].sum()) if positive.size else 0.0
    return {
        "policy": policy,
        "period": period,
        "sessions": len(days),
        "orders": len(subset),
        "fills": int(subset["filled"].sum()) if not subset.empty else 0,
        "wins": int((values > 0).sum()),
        "losses": int((values < 0).sum()),
        "win_rate": float((values > 0).mean()) if values.size else float("nan"),
        "trade_pf": pf,
        "net_pct": float(values.sum()) if values.size else 0.0,
        "expectancy_pct": float(values.mean()) if values.size else float("nan"),
        "max_drawdown_pct": float(drawdown.min()),
        "positive_days": int((daily_values > 0).sum()),
        "negative_days": int((daily_values < 0).sum()),
        "top2_profit_share": top2 / profit if profit > 0 else float("nan"),
    }


def _parity(current: pd.DataFrame) -> dict[str, Any]:
    published_path = corrected.AUDIT_OUTPUT_PATH
    published = pd.read_csv(published_path)
    for frame in (current, published):
        frame["day"] = pd.to_datetime(frame["day"]).dt.date
    keys = ["day", "setup_id", "side", "tradingsymbol"]
    left = current[keys + ["filled", "net_return_pct"]].sort_values(keys).reset_index(drop=True)
    right = published[keys + ["filled", "net_return_pct"]].sort_values(keys).reset_index(drop=True)
    keys_equal = left[keys].equals(right[keys])
    fills_equal = len(left) == len(right) and np.array_equal(
        left["filled"].to_numpy(bool), right["filled"].to_numpy(bool)
    )
    returns_equal = len(left) == len(right) and np.allclose(
        left["net_return_pct"].to_numpy(float),
        right["net_return_pct"].to_numpy(float),
        rtol=0.0,
        atol=1e-12,
        equal_nan=True,
    )
    result = {
        "published_path": str(published_path.resolve()),
        "published_sha256": _sha256(published_path),
        "ungated_current_v6_orders": len(left),
        "published_orders": len(right),
        "trade_keys_equal": keys_equal,
        "fills_equal": fills_equal,
        "returns_equal_at_1e_12": returns_equal,
        "passed": bool(keys_equal and fills_equal and returns_equal),
    }
    if not result["passed"]:
        raise AssertionError(f"Ungated CURRENT_V6 failed published parity: {result}")
    return result


def _fmt(value: Any) -> str:
    value = float(value)
    if math.isinf(value):
        return "INF"
    if math.isnan(value):
        return ""
    return f"{value:.3f}"


def _report(metrics: pd.DataFrame, manifest: dict[str, Any]) -> str:
    lookup = metrics.set_index(["policy", "period"])
    lines = [
        "# Corrected V6 ungated OI ablation",
        "",
        "## Verdict",
        "",
        "This is a research ablation on the same 23 point-in-time rolling-near-month sessions. "
        "Corrected V6 was not edited. No policy should be promoted from this sample unless it "
        "improves the chronological TRAIN and untouched TEST segments with adequate changed-trade "
        "counts and remains stable on future data.",
        "",
        "The candidate table was rebuilt before the historical positive-OI/0.05% prefilter. "
        "All price, volume, EMA, confirmation, trigger, bracket, cost, per-day picker and entry-cap "
        "rules remain those of corrected V6.",
        "",
        "## Policy results",
        "",
        "| Policy | Period | Sessions | Orders/fills | PF | Net % | Win % | Expectancy % | MDD % | Top-2 profit share |",
        "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for policy, description in POLICIES:
        for period in ("TRAIN", "TEST", "ALL"):
            row = lookup.loc[(policy, period)]
            lines.append(
                f"| {policy} | {period} | {int(row['sessions'])} | "
                f"{int(row['orders'])}/{int(row['fills'])} | {_fmt(row['trade_pf'])} | "
                f"{float(row['net_pct']):+.3f} | {100 * float(row['win_rate']):.1f} | "
                f"{float(row['expectancy_pct']):+.3f} | {float(row['max_drawdown_pct']):+.3f} | "
                f"{float(row['top2_profit_share']):.3f} |"
            )
    lines.extend(["", "## Exact policy definitions", ""])
    for policy, description in POLICIES:
        lines.append(f"- `{policy}`: {description}.")
    lines.extend(
        [
            "",
            "`QUADRANT_BUILDUP` is price up + OI up for LONG and price down + OI up for SHORT. "
            "`QUADRANT_UNWINDING` is price up + OI down for LONG and price down + OI down for SHORT. "
            "Price direction is already enforced by each V6 side's cash-price filter.",
            "",
            "Persistence features require exact adjacent completed five-minute timestamps in the "
            "same session. Current V6 parity intentionally uses its legacy one-row shift so that the "
            "ablation comparator reproduces the published result exactly.",
            "",
            "## Integrity",
            "",
            f"- Corrected-V6 source SHA-256 before/after: `{manifest['v6_sha256_before']}` / "
            f"`{manifest['v6_sha256_after']}`.",
            f"- Ungated candidates/paths: {manifest['candidate_signals']:,} / "
            f"{manifest['forward_paths']:,}.",
            f"- Sessions: {manifest['sessions']} ({manifest['train_sessions']} TRAIN, "
            f"{manifest['test_sessions']} TEST), split `{SPLIT_DAY}`.",
            f"- CURRENT_V6 published-trade parity: `{manifest['parity']['passed']}`.",
            "- Returns are additive per-trade percentages, matching corrected V6; this is not a "
            "capital-constrained lot-sized portfolio simulation.",
            "",
        ]
    )
    return "\n".join(lines)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rebuild-cache", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    started = time.monotonic()
    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    v6_source = Path(corrected.__file__).resolve()
    v6_hash_before = _sha256(v6_source)
    if v6_hash_before != EXPECTED_V6_SHA256:
        raise RuntimeError(
            "Corrected V6 source drifted; refusing to call this an exact comparator. "
            f"Expected {EXPECTED_V6_SHA256}, got {v6_hash_before}."
        )

    days_by_month, reference_hashes = _load_reference_days()
    signals, paths, regime_records = _build_ungated(days_by_month, rebuild=args.rebuild_cache)
    days = sorted(set(signals["day"]))
    train_days = [day for day in days if day < SPLIT_DAY]
    test_days = [day for day in days if day >= SPLIT_DAY]
    if len(days) != 23 or len(train_days) != 12 or len(test_days) != 11:
        raise AssertionError(
            f"Session split drift: all/train/test={len(days)}/{len(train_days)}/{len(test_days)}"
        )

    metric_rows: list[dict[str, Any]] = []
    audits: list[pd.DataFrame] = []
    daily_parts: list[pd.DataFrame] = []
    current_audit: pd.DataFrame | None = None
    for policy, _ in POLICIES:
        policy_signals, setups = _policy_input(signals, policy)
        audit = replay.replay_setups(
            policy_signals, paths, cost_bps=COST_BPS, setups=setups
        )
        audit["policy"] = policy
        audits.append(audit)
        daily = replay.build_daily_curve(audit, days, split_day=SPLIT_DAY)
        daily["policy"] = policy
        daily_parts.append(daily)
        for period, period_days in (
            ("TRAIN", train_days),
            ("TEST", test_days),
            ("ALL", days),
        ):
            metric_rows.append(_period_metrics(policy, audit, period_days, period))
        if policy == "CURRENT_V6":
            current_audit = audit.copy()

    if current_audit is None:
        raise AssertionError("CURRENT_V6 policy was not evaluated.")
    parity = _parity(current_audit)
    v6_hash_after = _sha256(v6_source)
    if v6_hash_after != v6_hash_before:
        raise AssertionError("Corrected V6 source changed during the research run.")

    metrics = pd.DataFrame(metric_rows)
    trades = pd.concat(audits, ignore_index=True, sort=False)
    daily = pd.concat(daily_parts, ignore_index=True, sort=False)
    common.atomic_write_csv(metrics, METRICS_PATH)
    common.atomic_write_csv(trades, TRADES_PATH)
    common.atomic_write_csv(daily, DAILY_PATH)
    common.atomic_write_json(PARITY_PATH, parity)

    manifest = {
        "research_version": RESEARCH_VERSION,
        "created_at_utc": pd.Timestamp.now(tz="UTC").isoformat(),
        "v6_source": str(v6_source),
        "v6_sha256_before": v6_hash_before,
        "v6_sha256_after": v6_hash_after,
        "reference_cache_sha256": reference_hashes,
        "candidate_signals": len(signals),
        "forward_paths": len(paths),
        "sessions": len(days),
        "train_sessions": len(train_days),
        "test_sessions": len(test_days),
        "split_day": SPLIT_DAY,
        "cost_bps": COST_BPS,
        "square_off": SQUARE_OFF,
        "max_forward_bars": MAX_FORWARD_BARS,
        "regimes": regime_records,
        "parity": parity,
        "elapsed_seconds": time.monotonic() - started,
        "outputs": [
            SIGNALS_PATH,
            PATHS_PATH,
            METRICS_PATH,
            TRADES_PATH,
            DAILY_PATH,
            PARITY_PATH,
            MANIFEST_PATH,
            REPORT_PATH,
        ],
    }
    common.atomic_write_json(
        MANIFEST_PATH,
        json.loads(json.dumps(manifest, default=_json_safe)),
    )
    common.atomic_write_text(REPORT_PATH, _report(metrics, manifest))

    print(metrics.to_string(index=False), flush=True)
    print(f"[PARITY] {parity}", flush=True)
    print(f"[WROTE] {REPORT_PATH}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
