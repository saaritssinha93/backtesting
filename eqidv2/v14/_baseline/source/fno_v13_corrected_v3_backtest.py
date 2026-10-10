"""V13 corrected v3: V13-v2 plus a causal NIFTY gate and 10:00 LONG.

V13-v3 is an isolated shadow challenger.  It preserves V13-v2's corrected
rolling-near-month data, bounded-OI policy, setup book, confirmation, execution,
exits, and costs.  It makes exactly two strategy changes:

* A 09:25 SHORT setup is eligible only when the first completed NIFTY
  near-month futures candle (09:15-09:20, stamped 09:20) returned <= -0.05%.
  Missing NIFTY context fails closed.  No other side or slot is gated.
* Add one 10:00 LONG setup confirmed at 10:01, using max-liquidity selection,
  stock price change >= +0.40%, five-minute OI change >= +0.05%, volume ratio
  >= 1.0, body ratio >= 0.4, wick ratio <= 0.5, 1.0% stop, and 3.0% target.

The first-bar gate is known before the 09:25 stock signal completes, so it is
causal.  This remains research-only: the configuration was selected after
inspecting a small history and must be judged on genuinely new sessions.
"""

from __future__ import annotations

import argparse
import hashlib
import time
from dataclasses import asdict, replace
from datetime import date
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import pandas as pd

import fno_oi_backtest_provenance as provenance
import fno_oi_common as common
import fno_oi_ema_confirm_sweep as sweep
import fno_oi_hybrid_data as hybrid
import fno_v5_hybrid_backtest as replay
import fno_v6_corrected_backtest as v6
import fno_v13_corrected_v2_backtest as v13_v2


STRATEGY_VERSION = "FNO_V13_CORRECTED_V3_NIFTY_FIRSTBAR_S005_PLUS_1000L"
EVIDENCE_STATUS = "EXPERIMENTAL_SHADOW_NOT_PROMOTED"
BASE_POLICY_NAME = "V13_V2_COMBINED_SHADOW"
EXPECTED_V13_V2_SOURCE_SHA256 = (
    "5368fd36a2b67ce9b2513d3d1ae5ec3201baff93e9a01df25861c1df085c8a9a"
)

ORIGINAL_SPLIT_DAY = date(2026, 8, 14)
ORIGINAL_TEST_END = date(2026, 9, 1)
NIFTY_FIRST_BAR_HHMM = 920
GATED_SIGNAL_HHMM = 925
NIFTY_FIRST_BAR_MAX_RETURN_PCT = -0.05
NIFTY_REQUIRED_ALIGNMENT_PCT = 0.05

RESULT_DIR = common.FNO_ROOT / "strategy_research" / "v13_corrected_v3"
CACHE_DIR = RESULT_DIR / "_cache"
NIFTY_ROOT = common.FNO_ROOT / "raw_contracts_5m"

ELIGIBILITY_PATH = RESULT_DIR / "fno_v13_corrected_v3_session_eligibility.csv"
TRADES_PATH = RESULT_DIR / "fno_v13_corrected_v3_trades.csv"
DAILY_PATH = RESULT_DIR / "fno_v13_corrected_v3_daily.csv"
SETUPS_PATH = RESULT_DIR / "fno_v13_corrected_v3_setups.csv"
BASELINE_TRADES_PATH = RESULT_DIR / "fno_v13_corrected_v3_v13_v2_parity_trades.csv"
BASELINE_DAILY_PATH = RESULT_DIR / "fno_v13_corrected_v3_v13_v2_parity_daily.csv"
DAYWISE_PATH = RESULT_DIR / "fno_v13_corrected_v3_daywise_comparison.csv"
GATE_AUDIT_PATH = RESULT_DIR / "fno_v13_corrected_v3_nifty_gate_audit.csv"
REJECTED_PATH = RESULT_DIR / "fno_v13_corrected_v3_rejected_v13_v2_trades.csv"
EXTRA_1000_PATH = RESULT_DIR / "fno_v13_corrected_v3_1000_long_trades.csv"
PERIOD_PATH = RESULT_DIR / "fno_v13_corrected_v3_period_metrics.csv"
COST_STRESS_PATH = RESULT_DIR / "fno_v13_corrected_v3_cost_stress.csv"
REPORT_PATH = RESULT_DIR / "FNO_V13_CORRECTED_V3_DETAILED_RESULTS.md"
WORKSPACE_REPORT_PATH = Path(__file__).with_name(
    "FNO_V13_CORRECTED_V3_DETAILED_RESULTS.md"
)
PROVENANCE_PATH = RESULT_DIR / "fno_v13_corrected_v3_provenance.json"


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _profit_factor(values: np.ndarray) -> float:
    values = values[np.isfinite(values)]
    profit = float(values[values > 0].sum()) if values.size else 0.0
    loss = float(-values[values < 0].sum()) if values.size else 0.0
    if loss > 0:
        return profit / loss
    return float("inf") if profit > 0 else float("nan")


def _periods(days: list[date]) -> dict[str, list[date]]:
    return {
        "ORIGINAL_TRAIN": [day for day in days if day < ORIGINAL_SPLIT_DAY],
        "ORIGINAL_TEST": [
            day
            for day in days
            if ORIGINAL_SPLIT_DAY <= day <= ORIGINAL_TEST_END
        ],
        "SEP02_PLUS": [day for day in days if day > ORIGINAL_TEST_END],
        "ALL": list(days),
    }


def _metrics(audit: pd.DataFrame, days: list[date]) -> dict[str, Any]:
    subset = audit.loc[audit["day"].isin(days)].copy()
    daily = replay.build_daily_curve(subset, days, split_day=ORIGINAL_SPLIT_DAY)
    stats = replay.summary_stats(daily, subset)
    fills = subset.loc[subset["filled"], "net_return_pct"].to_numpy(float)
    day_values = daily["portfolio_net_return_pct"].to_numpy(float)
    curve = np.r_[0.0, np.cumsum(day_values)]
    drawdown = curve - np.maximum.accumulate(curve)
    stats.update(
        {
            "win_rate": float((fills > 0).mean()) if fills.size else np.nan,
            "expectancy_pct": float(fills.mean()) if fills.size else np.nan,
            "max_drawdown_pct": float(drawdown.min()) if drawdown.size else 0.0,
        }
    )
    return stats


def extra_1000_long():
    """The frozen additive V13-v3 setup."""

    return replace(
        v13_v2._modal_long_setup("10:00"),
        price_change_pct=0.40,
        oi_change_pct=0.05,
        volume_ratio=1.00,
        body_ratio=0.40,
        max_wick_ratio=0.50,
        stop_pct=1.00,
        target_pct=3.00,
        source_version=STRATEGY_VERSION,
    )


def active_setups() -> tuple[Any, ...]:
    base = v13_v2.policy_setups(v13_v2.POLICIES[BASE_POLICY_NAME])
    return tuple(base) + (extra_1000_long(),)


def validate_configuration() -> None:
    observed = _sha256(Path(v13_v2.__file__).resolve())
    if observed != EXPECTED_V13_V2_SOURCE_SHA256:
        raise RuntimeError(
            "V13-v2 source drifted; review V13-v3 before running. "
            f"Expected {EXPECTED_V13_V2_SOURCE_SHA256}, observed {observed}."
        )
    v13_v2.validate_configuration()
    if RESULT_DIR.resolve() in {v13_v2.RESULT_DIR.resolve(), v6.RESULT_DIR.resolve()}:
        raise AssertionError("V13-v3 outputs must be isolated from V13-v2 and V6.")
    setups = active_setups()
    keys = [(setup.signal_end, setup.side) for setup in setups]
    if len(keys) != len(set(keys)):
        raise AssertionError("V13-v3 contains a duplicate time/side setup.")
    extra = setups[-1]
    expected = {
        "signal_end": "10:00",
        "confirmation_end": "10:01",
        "side": "LONG",
        "max_entries": 1,
        "picker": "max_liquidity",
        "price_change_pct": 0.40,
        "oi_change_pct": 0.05,
        "volume_ratio": 1.00,
        "body_ratio": 0.40,
        "max_wick_ratio": 0.50,
        "stop_pct": 1.00,
        "target_pct": 3.00,
    }
    for field, value in expected.items():
        if getattr(extra, field) != value:
            raise AssertionError(f"Unexpected 10:00 setup field {field}.")
    policy = v13_v2.POLICIES[BASE_POLICY_NAME]
    if policy.max_oi_change_pct != 1.00:
        raise AssertionError("V13-v3 requires V13-v2's frozen 1.00% OI cap.")


def load_nifty_first_bar_context(months: Iterable[str]) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for month in sorted(set(map(str, months))):
        path = NIFTY_ROOT / f"NIFTY{month}FUT_5minute.parquet"
        if not path.is_file():
            print(f"[V13-v3][WARN] missing NIFTY context: {path}", flush=True)
            continue
        frame = pd.read_parquet(path, columns=["timestamp", "open", "close"])
        stamps = pd.to_datetime(frame["timestamp"], errors="coerce")
        if stamps.dt.tz is None:
            stamps = stamps.dt.tz_localize(common.IST)
        else:
            stamps = stamps.dt.tz_convert(common.IST)
        frame["day"] = stamps.dt.date
        frame["hhmm_int"] = stamps.dt.strftime("%H%M").astype(int)
        frame["open"] = pd.to_numeric(frame["open"], errors="coerce")
        frame["close"] = pd.to_numeric(frame["close"], errors="coerce")
        frame = frame.loc[frame["hhmm_int"].eq(NIFTY_FIRST_BAR_HHMM)].copy()
        frame["nifty_first_bar_return_pct"] = (
            frame["close"] / frame["open"] - 1.0
        ) * 100.0
        frame["nifty_first_bar_alignment_pct"] = -frame[
            "nifty_first_bar_return_pct"
        ]
        frame["contract_month"] = month
        frames.append(
            frame[
                [
                    "contract_month",
                    "day",
                    "nifty_first_bar_return_pct",
                    "nifty_first_bar_alignment_pct",
                ]
            ]
        )
    if not frames:
        return pd.DataFrame(
            columns=[
                "contract_month",
                "day",
                "nifty_first_bar_return_pct",
                "nifty_first_bar_alignment_pct",
            ]
        )
    return (
        pd.concat(frames, ignore_index=True)
        .drop_duplicates(["contract_month", "day"], keep="last")
        .sort_values(["contract_month", "day"])
        .reset_index(drop=True)
    )


def annotate_nifty_gate(
    signals: pd.DataFrame, context: pd.DataFrame
) -> pd.DataFrame:
    annotated = signals.merge(
        context,
        on=["contract_month", "day"],
        how="left",
        validate="many_to_one",
    )
    applies = annotated["hhmm_int"].eq(GATED_SIGNAL_HHMM) & annotated[
        "side"
    ].eq("SHORT")
    annotated["nifty_first_bar_gate_applies"] = applies
    annotated["nifty_first_bar_gate_pass"] = (~applies) | (
        annotated["nifty_first_bar_return_pct"].notna()
        & annotated["nifty_first_bar_return_pct"].le(
            NIFTY_FIRST_BAR_MAX_RETURN_PCT
        )
    )
    annotated["nifty_first_bar_gate_reason"] = np.select(
        [
            ~applies,
            applies & annotated["nifty_first_bar_return_pct"].isna(),
            annotated["nifty_first_bar_gate_pass"],
        ],
        ["NOT_APPLICABLE", "MISSING_NIFTY_CONTEXT", "PASS"],
        default="NIFTY_FIRST_BAR_NOT_BEARISH_ENOUGH",
    )
    return annotated


def _cache_payload(
    month: str,
    universe_path: Path,
    days: list[date],
    square_off: str,
    max_forward_bars: int,
) -> dict[str, Any]:
    return {
        "cache_schema": "V13_CORRECTED_V3_SIGNAL_CACHE_V1",
        "contract_month": month,
        "universe_path": str(universe_path.resolve()),
        "universe_sha256": _sha256(universe_path),
        "days": [str(day) for day in days],
        "square_off": square_off,
        "max_forward_bars": int(max_forward_bars),
        "confirmation_policy": sweep.CONFIRMATION_POLICY_V6_STRICT,
        "data_contract": hybrid.DATA_CONTRACT_VERSION,
        "v13_v2_source_sha256": EXPECTED_V13_V2_SOURCE_SHA256,
    }


def _cache_files(stem: Path) -> tuple[Path, Path]:
    return stem.with_suffix(".parquet"), stem.with_suffix(".npz")


def _load_or_build_regime(
    month: str,
    universe_path: Path,
    days: list[date],
    *,
    square_off: str,
    max_forward_bars: int,
    rebuild: bool,
) -> tuple[pd.DataFrame, dict, dict[str, Any]]:
    payload = _cache_payload(
        month, universe_path, days, square_off, max_forward_bars
    )
    key = common.canonical_json_sha256(payload)[:16]
    own_stem = CACHE_DIR / f"{month}_{key}"
    loaded = None
    source = ""
    selected_stem: Path | None = None

    if not rebuild:
        choices: list[tuple[tuple[int, int, int, int], str, Path, Any]] = []
        cache_dirs = (
            (CACHE_DIR, "V13_V3_CACHE", 3),
            (v13_v2.CACHE_DIR, "READ_ONLY_V13_V2_CACHE_SEED", 2),
            (v6.CACHE_DIR, "READ_ONLY_V6_CACHE_SEED", 1),
        )
        requested_days = set(days)
        for directory, label, priority in cache_dirs:
            if not directory.is_dir():
                continue
            for parquet_path in directory.glob(f"{month}_*.parquet"):
                stem = parquet_path.with_suffix("")
                if not stem.with_suffix(".npz").is_file():
                    continue
                candidate = v6._load_cached(stem)
                if candidate is None:
                    continue
                candidate_signals, _ = candidate
                candidate_days = pd.to_datetime(
                    candidate_signals["day"]
                ).dt.date
                requested = candidate_signals.loc[
                    candidate_days.isin(requested_days)
                ]
                score = (
                    int(pd.to_datetime(requested["day"]).dt.date.nunique()),
                    int(len(requested)),
                    int(priority),
                    int(parquet_path.stat().st_mtime_ns),
                )
                choices.append((score, label, stem, candidate))
        if choices:
            _, source, selected_stem, loaded = max(
                choices, key=lambda item: item[0]
            )

    mapped, universe_record = provenance.load_backtest_universe(
        universe_path=universe_path,
        contract_month_contains=month,
    )
    if loaded is None:
        print(
            f"[V13-v3][BUILD] {month}: {len(mapped)} contracts, "
            f"{len(days)} sessions",
            flush=True,
        )
        signals, paths = sweep.build_signal_table(
            set(days),
            square_off=square_off,
            max_forward_bars=max_forward_bars,
            mapped_universe=mapped,
            confirmation_policy=sweep.CONFIRMATION_POLICY_V6_STRICT,
        )
        source = "V13_V3_REBUILD"
    else:
        signals, paths = loaded
        print(f"[V13-v3][CACHE] {month}: {source}", flush=True)

    signals = signals.copy()
    signals["day"] = pd.to_datetime(signals["day"]).dt.date
    signals = signals.loc[signals["day"].isin(set(days))].copy()
    signals["contract_month"] = month
    kept_sids = set(signals["sid"].astype(int)) if not signals.empty else set()
    paths = {
        int(sid): value for sid, value in paths.items() if int(sid) in kept_sids
    }

    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    v6._store_cached(own_stem, signals, paths)
    parquet_path, npz_path = _cache_files(own_stem)
    return signals, paths, {
        "contract_month": month,
        "sessions": [str(day) for day in days],
        "source": source,
        "selected_seed_stem": str(selected_stem.resolve()) if selected_stem else None,
        "cache_payload": payload,
        "cache_parquet": str(parquet_path.resolve()),
        "cache_npz": str(npz_path.resolve()),
        "cache_parquet_sha256": _sha256(parquet_path),
        "cache_npz_sha256": _sha256(npz_path),
        "universe_record": universe_record,
    }


def _load_eligibility(refresh: bool, min_coverage: float):
    regimes = v6.regime_universe_paths()
    if not refresh and v6.ELIGIBILITY_PATH.is_file():
        eligibility = pd.read_csv(v6.ELIGIBILITY_PATH)
        eligibility["day"] = pd.to_datetime(eligibility["day"]).dt.date
        eligibility["eligible"] = eligibility["eligible"].astype(str).str.lower().eq(
            "true"
        )
        first, last = eligibility["day"].min(), eligibility["day"].max()
        calendar, origin = v6.build_expiry_calendar(first, last)
        source = "READ_ONLY_V6_ELIGIBILITY_SEED"
    else:
        eligibility, calendar, origin = v6.build_eligibility(
            regimes, min_coverage=min_coverage
        )
        source = "V13_V3_FRESH_SCAN"
    return eligibility, calendar, origin, regimes, source


def _run_v13_v2(
    signals: pd.DataFrame,
    paths: dict,
    days: list[date],
    *,
    cost_bps: float,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    policy = v13_v2.POLICIES[BASE_POLICY_NAME]
    policy_signals = v13_v2.apply_policy(signals, policy)
    audit = replay.replay_setups(
        policy_signals,
        paths,
        cost_bps=cost_bps,
        setups=v13_v2.policy_setups(policy),
    )
    daily = replay.build_daily_curve(audit, days, split_day=ORIGINAL_SPLIT_DAY)
    return audit, daily


def _run_v3(
    annotated: pd.DataFrame,
    paths: dict,
    days: list[date],
    *,
    cost_bps: float,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    policy = v13_v2.POLICIES[BASE_POLICY_NAME]
    gated = annotated.loc[annotated["nifty_first_bar_gate_pass"]].copy()
    policy_signals = v13_v2.apply_policy(gated, policy)
    audit = replay.replay_setups(
        policy_signals,
        paths,
        cost_bps=cost_bps,
        setups=active_setups(),
    )
    audit = audit.copy()
    audit["strategy_version"] = STRATEGY_VERSION
    audit["evidence_status"] = EVIDENCE_STATUS
    daily = replay.build_daily_curve(audit, days, split_day=ORIGINAL_SPLIT_DAY)
    daily["strategy_version"] = STRATEGY_VERSION
    daily["evidence_status"] = EVIDENCE_STATUS
    return audit, daily


def _setup_summary(audit: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for setup in active_setups():
        selected = audit.loc[audit["setup_id"].eq(setup.setup_id)]
        fills = selected.loc[selected["filled"], "net_return_pct"].to_numpy(float)
        rows.append(
            {
                **asdict(setup),
                "orders": int(len(selected)),
                "fills": int(selected["filled"].sum()) if not selected.empty else 0,
                "wins": int((fills > 0).sum()),
                "losses": int((fills < 0).sum()),
                "win_rate": float((fills > 0).mean()) if fills.size else np.nan,
                "trade_pf": _profit_factor(fills),
                "net_pct": float(fills.sum()) if fills.size else 0.0,
                "strategy_version": STRATEGY_VERSION,
            }
        )
    return pd.DataFrame(rows)


def _day_trade_counts(audit: pd.DataFrame, prefix: str) -> pd.DataFrame:
    frame = audit.copy()
    frame["win"] = frame["filled"] & frame["net_return_pct"].gt(0)
    frame["loss"] = frame["filled"] & frame["net_return_pct"].lt(0)
    return (
        frame.groupby("day", as_index=False)
        .agg(
            **{
                f"{prefix}_wins": ("win", "sum"),
                f"{prefix}_losses": ("loss", "sum"),
            }
        )
    )


def build_daywise(
    baseline_audit: pd.DataFrame,
    baseline_daily: pd.DataFrame,
    audit: pd.DataFrame,
    daily: pd.DataFrame,
    annotated_signals: pd.DataFrame,
) -> pd.DataFrame:
    base = baseline_daily[
        ["day", "selections", "fills", "portfolio_net_return_pct"]
    ].rename(
        columns={
            "selections": "v13_v2_selections",
            "fills": "v13_v2_fills",
            "portfolio_net_return_pct": "v13_v2_net_pct",
        }
    )
    current = daily[
        ["day", "selections", "fills", "portfolio_net_return_pct"]
    ].rename(
        columns={
            "selections": "v13_v3_selections",
            "fills": "v13_v3_fills",
            "portfolio_net_return_pct": "v13_v3_net_pct",
        }
    )
    out = base.merge(current, on="day", how="outer")
    out = out.merge(_day_trade_counts(baseline_audit, "v13_v2"), on="day", how="left")
    out = out.merge(_day_trade_counts(audit, "v13_v3"), on="day", how="left")
    # ``annotated_signals`` has already been joined on both day and the exact
    # rolling contract used by that session.  Deduplicating the raw context by
    # day alone can otherwise attribute an overlapping far-month candle.
    day_context = annotated_signals.drop_duplicates("day", keep="last")[
        ["day", "contract_month", "nifty_first_bar_return_pct"]
    ]
    out = out.merge(day_context, on="day", how="left")
    numeric = [
        column
        for column in out.columns
        if column.endswith(("_selections", "_fills", "_wins", "_losses", "_net_pct"))
    ]
    out[numeric] = out[numeric].fillna(0)
    for column in [c for c in numeric if not c.endswith("_net_pct")]:
        out[column] = out[column].astype(int)
    out["delta_net_pct"] = out["v13_v3_net_pct"] - out["v13_v2_net_pct"]
    out["v13_v2_cumulative_net_pct"] = out["v13_v2_net_pct"].cumsum()
    out["v13_v3_cumulative_net_pct"] = out["v13_v3_net_pct"].cumsum()
    return out.sort_values("day").reset_index(drop=True)


def _parity_vs_published(audit: pd.DataFrame) -> dict[str, Any]:
    if not v13_v2.TRADES_PATH.is_file():
        return {"published_found": False, "passed": False}
    reference = pd.read_csv(v13_v2.TRADES_PATH)
    reference["day"] = pd.to_datetime(reference["day"]).dt.date
    shared_days = sorted(set(audit["day"]) & set(reference["day"]))
    left = audit.loc[audit["day"].isin(shared_days)].copy()
    right = reference.loc[reference["day"].isin(shared_days)].copy()
    key = ["day", "hhmm_int", "side", "setup_id", "tradingsymbol"]
    left = left.sort_values(key, kind="stable").reset_index(drop=True)
    right = right.sort_values(key, kind="stable").reset_index(drop=True)
    keys_equal = left[key].equals(right[key])
    fills_equal = bool(
        keys_equal
        and left["filled"].astype(str).str.lower().equals(
            right["filled"].astype(str).str.lower()
        )
    )
    returns_equal = bool(
        keys_equal
        and np.allclose(
            pd.to_numeric(left["net_return_pct"], errors="coerce"),
            pd.to_numeric(right["net_return_pct"], errors="coerce"),
            rtol=0.0,
            atol=1e-12,
            equal_nan=True,
        )
    )
    return {
        "published_found": True,
        "published_path": str(v13_v2.TRADES_PATH.resolve()),
        "published_sha256": _sha256(v13_v2.TRADES_PATH),
        "shared_days": len(shared_days),
        "orders_compared": len(left),
        "trade_keys_equal": bool(keys_equal),
        "fills_equal": fills_equal,
        "returns_equal_at_1e_12": returns_equal,
        "passed": bool(keys_equal and fills_equal and returns_equal),
    }


def _markdown(frame: pd.DataFrame, columns: list[str]) -> str:
    if frame.empty:
        return "_No rows._"
    selected = frame.loc[:, [column for column in columns if column in frame.columns]].copy()
    return selected.to_markdown(index=False, floatfmt=".3f")


def _display_trades(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    out["outcome"] = np.select(
        [
            ~out["filled"].astype(bool),
            out["net_return_pct"].gt(0),
            out["net_return_pct"].lt(0),
        ],
        ["UNFILLED", "WIN", "LOSS"],
        default="FLAT",
    )
    return out


def render_report(
    *,
    days: list[date],
    cost_bps: float,
    baseline_stats: dict[str, Any],
    stats: dict[str, Any],
    periods: pd.DataFrame,
    costs: pd.DataFrame,
    setups: pd.DataFrame,
    daywise: pd.DataFrame,
    rejected: pd.DataFrame,
    extra: pd.DataFrame,
    audit: pd.DataFrame,
    parity: dict[str, Any],
    cache_records: list[dict[str, Any]],
) -> str:
    comparison = pd.DataFrame(
        [
            {"metric": "sessions", "v13_v2": baseline_stats["sessions"], "v13_v3": stats["sessions"]},
            {"metric": "orders", "v13_v2": baseline_stats["orders"], "v13_v3": stats["orders"]},
            {"metric": "fills", "v13_v2": baseline_stats["fills"], "v13_v3": stats["fills"]},
            {"metric": "wins", "v13_v2": baseline_stats["wins"], "v13_v3": stats["wins"]},
            {"metric": "losses", "v13_v2": baseline_stats["losses"], "v13_v3": stats["losses"]},
            {"metric": "win_rate", "v13_v2": baseline_stats["win_rate"], "v13_v3": stats["win_rate"]},
            {"metric": "trade_pf", "v13_v2": baseline_stats["trade_pf"], "v13_v3": stats["trade_pf"]},
            {"metric": "day_pf", "v13_v2": baseline_stats["day_pf"], "v13_v3": stats["day_pf"]},
            {"metric": "net_pct", "v13_v2": baseline_stats["net_pct"], "v13_v3": stats["net_pct"]},
            {"metric": "max_drawdown_pct", "v13_v2": baseline_stats["max_drawdown_pct"], "v13_v3": stats["max_drawdown_pct"]},
        ]
    )
    comparison["delta"] = comparison["v13_v3"] - comparison["v13_v2"]
    trade_columns = [
        "day",
        "hhmm_int",
        "tradingsymbol",
        "side",
        "setup_id",
        "price_change_pct",
        "oi_change_pct",
        "volume_ratio",
        "body_ratio",
        "nifty_first_bar_return_pct",
        "filled",
        "net_return_pct",
        "outcome",
    ]
    lines = [
        "# FNO V13 corrected v3 - detailed historical results",
        "",
        "## Verdict",
        "",
        (
            f"Across **{len(days)}** corrected-data sessions ({days[0]} through {days[-1]}), "
            f"V13-v3 produced **{stats['fills']} fills**, **{stats['win_rate'] * 100:.2f}%** "
            f"win rate, **{stats['trade_pf']:.3f} PF**, and **{stats['net_pct']:+.3f}%** "
            f"summed net return at {cost_bps:.1f} bps. The V13-v2 comparator produced "
            f"{baseline_stats['fills']} fills, {baseline_stats['win_rate'] * 100:.2f}% "
            f"win rate, {baseline_stats['trade_pf']:.3f} PF, and "
            f"{baseline_stats['net_pct']:+.3f}%."
        ),
        "",
        (
            "This is the requested frozen shadow configuration, not a production promotion. "
            "Its NIFTY rule and 10:00 leg were selected after inspecting this short history."
        ),
        "",
        "## Exact configuration changes from V13-v2",
        "",
        "1. Keep `V13_V2_COMBINED_SHADOW`, including its 1.00% global OI cap.",
        "2. Gate only 09:25 SHORT: NIFTY's completed 09:15-09:20 near-month futures candle must return <= -0.05%.",
        "3. Add 10:00 LONG / 10:01 confirmation: price >= +0.40%, OI >= +0.05%, volume >= 1.0x, body >= 0.40, wick <= 0.50, max-liquidity, one entry, 1.00% stop, 3.00% target.",
        "4. No change to V13-v2's roll, stock/futures data contract, existing setup thresholds, confirmation, entry simulation, or costs.",
        "",
        "## Headline comparison",
        "",
        _markdown(comparison, ["metric", "v13_v2", "v13_v3", "delta"]),
        "",
        "## Period results",
        "",
        _markdown(
            periods,
            ["strategy", "period", "sessions", "fills", "wins", "losses", "win_rate", "trade_pf", "net_pct", "max_drawdown_pct"],
        ),
        "",
        "## Cost stress",
        "",
        _markdown(costs, ["strategy", "cost_bps", "fills", "win_rate", "trade_pf", "net_pct", "max_drawdown_pct"]),
        "",
        "## Per-setup results",
        "",
        _markdown(
            setups,
            ["signal_end", "confirmation_end", "side", "max_entries", "picker", "price_change_pct", "oi_change_pct", "volume_ratio", "body_ratio", "max_wick_ratio", "stop_pct", "target_pct", "orders", "fills", "wins", "losses", "win_rate", "trade_pf", "net_pct"],
        ),
        "",
        "## Day-wise V13-v2 versus V13-v3",
        "",
        _markdown(
            daywise,
            ["day", "contract_month", "nifty_first_bar_return_pct", "v13_v2_fills", "v13_v2_wins", "v13_v2_losses", "v13_v2_net_pct", "v13_v3_fills", "v13_v3_wins", "v13_v3_losses", "v13_v3_net_pct", "delta_net_pct", "v13_v3_cumulative_net_pct"],
        ),
        "",
        "## V13-v2 trades rejected by the NIFTY first-bar gate",
        "",
        _markdown(_display_trades(rejected), trade_columns),
        "",
        "## Additive 10:00 LONG selections",
        "",
        _markdown(_display_trades(extra), trade_columns),
        "",
        "## Complete V13-v3 trade ledger",
        "",
        _markdown(_display_trades(audit), trade_columns),
        "",
        "## Integrity and interpretation",
        "",
        f"- V13-v2 published parity passed: {parity.get('passed')}.",
        f"- V13-v2 source SHA-256: `{EXPECTED_V13_V2_SOURCE_SHA256}`.",
        f"- Signal cache regimes: {len(cache_records)}.",
        f"- Strategy version: `{STRATEGY_VERSION}`.",
        f"- Evidence status: `{EVIDENCE_STATUS}`.",
        "- Net return is the sum of filled-trade percentage returns; this is not a capital-constrained or lot-sized portfolio simulation.",
        "- A signal with `filled=False` never traded through its stop-entry trigger and contributes no return.",
        "- Multiple positions and repeated symbols can coexist under the native V13 replay rules.",
        "- Forward-test unchanged for at least 20-40 genuinely new sessions and another expiry regime before considering promotion.",
        "",
    ]
    return "\n".join(lines)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--through-day",
        default="",
        help="inclusive YYYY-MM-DD; blank uses the latest eligible stored session",
    )
    parser.add_argument("--cost-bps", type=float, default=5.0)
    parser.add_argument("--square-off", default="1530")
    parser.add_argument("--max-forward-bars", type=int, default=400)
    parser.add_argument("--min-contract-coverage", type=float, default=0.99)
    parser.add_argument("--rebuild-cache", action="store_true")
    parser.add_argument("--refresh-eligibility", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    started = time.monotonic()
    validate_configuration()
    source_before = _sha256(Path(v13_v2.__file__).resolve())
    RESULT_DIR.mkdir(parents=True, exist_ok=True)

    eligibility, calendar, origin, regimes, eligibility_source = _load_eligibility(
        args.refresh_eligibility, args.min_contract_coverage
    )
    if args.through_day:
        through_day = pd.Timestamp(args.through_day).date()
        eligibility = eligibility.loc[eligibility["day"].le(through_day)].copy()
    elif eligibility.empty:
        raise RuntimeError("V13-v3 found no stored eligibility rows.")
    else:
        through_day = max(eligibility["day"])
    common.atomic_write_csv(eligibility, ELIGIBILITY_PATH)
    ok = eligibility.loc[eligibility["eligible"]].copy()
    if ok.empty:
        raise RuntimeError("V13-v3 has no eligible sessions.")

    days_by_month: dict[str, list[date]] = {}
    for row in ok.to_dict("records"):
        month = str(row["required_contract"])
        if month in regimes:
            days_by_month.setdefault(month, []).append(row["day"])

    parts: list[tuple[pd.DataFrame, dict]] = []
    cache_records: list[dict[str, Any]] = []
    for month in sorted(days_by_month, key=lambda value: calendar[value]):
        regime_days = sorted(days_by_month[month])
        signals, paths, record = _load_or_build_regime(
            month,
            regimes[month],
            regime_days,
            square_off=args.square_off,
            max_forward_bars=args.max_forward_bars,
            rebuild=args.rebuild_cache,
        )
        parts.append((signals, paths))
        cache_records.append(record)

    signals, paths = v6.concat_regimes(parts)
    if signals.empty:
        raise RuntimeError("V13-v3 produced no candidate signals.")
    days = sorted(set(signals["day"]))

    context = load_nifty_first_bar_context(signals["contract_month"].unique())
    annotated = annotate_nifty_gate(signals, context)
    baseline_audit, baseline_daily = _run_v13_v2(
        signals, paths, days, cost_bps=args.cost_bps
    )
    audit, daily = _run_v3(annotated, paths, days, cost_bps=args.cost_bps)
    if baseline_audit.empty or audit.empty:
        raise RuntimeError("V13-v2 comparator or V13-v3 selected no orders.")

    parity = _parity_vs_published(baseline_audit)
    if parity.get("published_found") and not parity.get("passed"):
        raise AssertionError("V13-v3 comparator failed parity against published V13-v2.")

    baseline_stats = _metrics(baseline_audit, days)
    stats = _metrics(audit, days)
    period_rows: list[dict[str, Any]] = []
    for period, period_days in _periods(days).items():
        period_rows.append(
            {"strategy": "V13-v2", "period": period, **_metrics(baseline_audit, period_days)}
        )
        period_rows.append(
            {"strategy": "V13-v3", "period": period, **_metrics(audit, period_days)}
        )
    period_frame = pd.DataFrame(period_rows)

    cost_rows: list[dict[str, Any]] = []
    for cost in (5.0, 10.0, 15.0, 20.0):
        base_cost_audit, _ = _run_v13_v2(signals, paths, days, cost_bps=cost)
        cost_audit, _ = _run_v3(annotated, paths, days, cost_bps=cost)
        cost_rows.append(
            {"strategy": "V13-v2", "cost_bps": cost, **_metrics(base_cost_audit, days)}
        )
        cost_rows.append(
            {"strategy": "V13-v3", "cost_bps": cost, **_metrics(cost_audit, days)}
        )
    cost_frame = pd.DataFrame(cost_rows)
    setup_frame = _setup_summary(audit)
    daywise = build_daywise(
        baseline_audit, baseline_daily, audit, daily, annotated
    )

    inherited = audit.loc[~audit["setup_id"].eq(extra_1000_long().setup_id)]
    inherited_keys = set(
        zip(
            inherited["day"],
            inherited["hhmm_int"],
            inherited["side"],
            inherited["setup_id"],
            inherited["tradingsymbol"],
        )
    )
    rejected_mask = [
        key not in inherited_keys
        for key in zip(
            baseline_audit["day"],
            baseline_audit["hhmm_int"],
            baseline_audit["side"],
            baseline_audit["setup_id"],
            baseline_audit["tradingsymbol"],
        )
    ]
    gate_lookup = annotated[
        [
            "sid",
            "nifty_first_bar_return_pct",
            "nifty_first_bar_alignment_pct",
            "nifty_first_bar_gate_reason",
        ]
    ].drop_duplicates("sid")
    rejected = baseline_audit.loc[rejected_mask].merge(
        gate_lookup, on="sid", how="left", validate="many_to_one"
    )
    extra = audit.loc[audit["setup_id"].eq(extra_1000_long().setup_id)].copy()

    common.atomic_write_csv(baseline_audit, BASELINE_TRADES_PATH)
    common.atomic_write_csv(baseline_daily, BASELINE_DAILY_PATH)
    common.atomic_write_csv(audit, TRADES_PATH)
    common.atomic_write_csv(daily, DAILY_PATH)
    common.atomic_write_csv(setup_frame, SETUPS_PATH)
    common.atomic_write_csv(daywise, DAYWISE_PATH)
    common.atomic_write_csv(annotated, GATE_AUDIT_PATH)
    common.atomic_write_csv(rejected, REJECTED_PATH)
    common.atomic_write_csv(extra, EXTRA_1000_PATH)
    common.atomic_write_csv(period_frame, PERIOD_PATH)
    common.atomic_write_csv(cost_frame, COST_STRESS_PATH)

    report = render_report(
        days=days,
        cost_bps=args.cost_bps,
        baseline_stats=baseline_stats,
        stats=stats,
        periods=period_frame,
        costs=cost_frame,
        setups=setup_frame,
        daywise=daywise,
        rejected=rejected,
        extra=extra,
        audit=audit,
        parity=parity,
        cache_records=cache_records,
    )
    common.atomic_write_text(REPORT_PATH, report)
    common.atomic_write_text(WORKSPACE_REPORT_PATH, report)

    source_after = _sha256(Path(v13_v2.__file__).resolve())
    if source_after != source_before:
        raise AssertionError("V13-v2 source changed during V13-v3 execution.")
    common.atomic_write_json(
        PROVENANCE_PATH,
        {
            "strategy_version": STRATEGY_VERSION,
            "evidence_status": EVIDENCE_STATUS,
            "generated_at_ist": common.now_ist().isoformat(timespec="seconds"),
            "through_day": str(through_day),
            "sessions": [str(day) for day in days],
            "base_policy": BASE_POLICY_NAME,
            "v13_v2_source": str(Path(v13_v2.__file__).resolve()),
            "v13_v2_source_sha256": source_after,
            "v13_v2_parity": parity,
            "nifty_gate": {
                "instrument": "point-in-time near-month NIFTY futures",
                "bar": "09:15-09:20 completed bar, timestamp 09:20",
                "applies_to": "09:25 SHORT only",
                "first_bar_return_max_pct": NIFTY_FIRST_BAR_MAX_RETURN_PCT,
                "required_short_alignment_pct": NIFTY_REQUIRED_ALIGNMENT_PCT,
                "missing_context_policy": "FAIL_CLOSED",
            },
            "extra_setup": asdict(extra_1000_long()),
            "parameters": {
                "cost_bps": float(args.cost_bps),
                "square_off": args.square_off,
                "max_forward_bars": int(args.max_forward_bars),
                "min_contract_coverage": float(args.min_contract_coverage),
            },
            "roll_policy": v6.ROLL_POLICY,
            "confirmation_policy": sweep.CONFIRMATION_POLICY_V6_STRICT,
            "data_contract": hybrid.DATA_CONTRACT_VERSION,
            "eligibility_source": eligibility_source,
            "expiry_calendar": {key: str(value) for key, value in calendar.items()},
            "expiry_origin": origin,
            "cache_records": cache_records,
            "outputs": {
                "report": str(REPORT_PATH.resolve()),
                "workspace_report": str(WORKSPACE_REPORT_PATH.resolve()),
                "trades": str(TRADES_PATH.resolve()),
                "daily": str(DAILY_PATH.resolve()),
                "daywise": str(DAYWISE_PATH.resolve()),
            },
            "headline": stats,
        },
    )

    duration = time.monotonic() - started
    print(
        f"[V13-v3] sessions={len(days)} orders={stats['orders']} fills={stats['fills']} "
        f"wins={stats['wins']} win_rate={stats['win_rate'] * 100:.3f}% "
        f"PF={stats['trade_pf']:.6f} net={stats['net_pct']:+.6f}% "
        f"maxDD={stats['max_drawdown_pct']:.6f}%",
        flush=True,
    )
    print(
        f"[V13-v2] fills={baseline_stats['fills']} "
        f"win_rate={baseline_stats['win_rate'] * 100:.3f}% "
        f"PF={baseline_stats['trade_pf']:.6f} "
        f"net={baseline_stats['net_pct']:+.6f}%",
        flush=True,
    )
    print(f"[REPORT] {REPORT_PATH}", flush=True)
    print(f"[DONE] {duration:.1f}s", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
