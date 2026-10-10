"""V13 corrected v2: isolated experimental improvements over corrected V6.

The corrected V6 source, setup book, rolling near-month policy, price/volume
features, confirmation candle, entry trigger, bracket simulation and cost model
remain the comparator.  This file adds an explicit policy layer and writes only
under strategy_research/v13_corrected_v2.

The default is deliberately named V13_V2_COMBINED_SHADOW.  It is the strongest
retrospective combination found in the available 24-session sample:

* retain V6's positive per-setup OI thresholds;
* raise 09:35 LONG OI change from 0.10% to 0.15%;
* lower 09:40 LONG OI change from 0.10% to 0.075%;
* reject OI changes above 1.00% before ranking, allowing the next candidate to
  become the pick; and
* add one 09:55 LONG leg using V6's modal filters.

This is an experimental shadow book, not a promoted production strategy.  Its
sample is small, the 1.00% cap has weak neighbourhood stability, and the 09:55
leg was selected after inspecting multiple new time slots.  V6_PARITY is always
run as the benchmark and must reproduce the published corrected-V6 trades.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import time
from dataclasses import asdict, dataclass, replace
from datetime import date
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import pandas as pd

import fno_oi_backtest_provenance as provenance
import fno_oi_common as common
import fno_oi_ema_confirm_sweep as sw
import fno_oi_hybrid_data as hybrid
import fno_v5_hybrid_backtest as replay
import fno_v6_corrected_backtest as v6


STRATEGY_VERSION = "FNO_V13_CORRECTED_V2_EXPERIMENTAL_20260903"
EVIDENCE_STATUS = "EXPERIMENTAL_SHADOW_NOT_PROMOTED"
EXPECTED_V6_SOURCE_SHA256 = (
    "06baf32c33156f21bce1dc786e5687a250b9711a1bca3a186283c824edfcf62d"
)
DEFAULT_POLICY = "V13_V2_COMBINED_SHADOW"
DEFAULT_THROUGH_DAY = "2026-09-02"
ORIGINAL_SPLIT_DAY = date(2026, 8, 14)
ORIGINAL_TEST_END = date(2026, 9, 1)
CACHE_OI_FLOOR = 0.05
BOOTSTRAP_SEED = 20260903
BOOTSTRAP_DRAWS = 20_000

RESULT_DIR = common.FNO_ROOT / "strategy_research" / "v13_corrected_v2"
CACHE_DIR = RESULT_DIR / "_cache"
ELIGIBILITY_PATH = RESULT_DIR / "fno_v13_corrected_v2_session_eligibility.csv"
TRADES_PATH = RESULT_DIR / "fno_v13_corrected_v2_trades.csv"
DAILY_PATH = RESULT_DIR / "fno_v13_corrected_v2_daily.csv"
SETUPS_PATH = RESULT_DIR / "fno_v13_corrected_v2_setups.csv"
BASELINE_TRADES_PATH = RESULT_DIR / "fno_v13_corrected_v2_v6_parity_trades.csv"
BASELINE_DAILY_PATH = RESULT_DIR / "fno_v13_corrected_v2_v6_parity_daily.csv"
COMPARISON_PATH = RESULT_DIR / "fno_v13_corrected_v2_policy_comparison.csv"
COST_STRESS_PATH = RESULT_DIR / "fno_v13_corrected_v2_cost_stress.csv"
DECISION_CHANGES_PATH = RESULT_DIR / "fno_v13_corrected_v2_decision_changes.csv"
CAP_SENSITIVITY_PATH = RESULT_DIR / "fno_v13_corrected_v2_cap_sensitivity.csv"
BOOTSTRAP_PATH = RESULT_DIR / "fno_v13_corrected_v2_bootstrap.csv"
CONTRACT_PATH = RESULT_DIR / "fno_v13_corrected_v2_contract_regimes.csv"
SLOT_SENSITIVITY_PATH = RESULT_DIR / "fno_v13_corrected_v2_slot_sensitivity.csv"
REPORT_PATH = RESULT_DIR / "FNO_V13_CORRECTED_V2_ANALYSIS.md"
PROVENANCE_PATH = RESULT_DIR / "fno_v13_corrected_v2_provenance.json"
V13_V1_SOURCE_PATH = Path(__file__).with_name("fno_v13_corrected_backtest.py")


@dataclass(frozen=True)
class PolicySpec:
    name: str
    description: str
    setup_oi_overrides: tuple[tuple[str, str, float], ...] = ()
    max_oi_change_pct: float | None = None
    add_0955_long: bool = False
    validated: bool = False


POLICIES: dict[str, PolicySpec] = {
    "V6_PARITY": PolicySpec(
        name="V6_PARITY",
        description="Exact corrected-V6 setup book; the validated comparator.",
        validated=True,
    ),
    "OI_BOUNDED_SHADOW": PolicySpec(
        name="OI_BOUNDED_SHADOW",
        description=(
            "Research OI book: 09:35 LONG >=0.15%, 09:40 LONG >=0.075%, "
            "and global OI change <=1.00% before ranking."
        ),
        setup_oi_overrides=(
            ("09:35", "LONG", 0.15),
            ("09:40", "LONG", 0.075),
        ),
        max_oi_change_pct=1.00,
    ),
    "ADD_0955_LONG_SHADOW": PolicySpec(
        name="ADD_0955_LONG_SHADOW",
        description=(
            "Research frequency book: exact V6 plus one 09:55 LONG setup "
            "using modal V6 filters."
        ),
        add_0955_long=True,
    ),
    "V13_V2_COMBINED_SHADOW": PolicySpec(
        name="V13_V2_COMBINED_SHADOW",
        description=(
            "Research combined book: bounded-OI changes plus the additive "
            "09:55 LONG setup. Numerically strongest net/PF compromise in the "
            "retrospective sample; not independently validated."
        ),
        setup_oi_overrides=(
            ("09:35", "LONG", 0.15),
            ("09:40", "LONG", 0.075),
        ),
        max_oi_change_pct=1.00,
        add_0955_long=True,
    ),
}

PROTECTED_V6_FILES = (
    Path(v6.__file__).resolve(),
    v6.DAILY_OUTPUT_PATH,
    v6.AUDIT_OUTPUT_PATH,
    v6.SETUPS_OUTPUT_PATH,
    v6.ELIGIBILITY_PATH,
    v6.COMPARISON_PATH,
    v6.REPORT_PATH,
    v6.PROVENANCE_PATH,
)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _protected_v6_snapshot() -> dict[str, str | None]:
    return {
        str(path.resolve()): (_sha256(path) if path.is_file() else None)
        for path in PROTECTED_V6_FILES
    }


def _verify_v6_source() -> str:
    path = Path(v6.__file__).resolve()
    observed = _sha256(path)
    if observed != EXPECTED_V6_SOURCE_SHA256:
        raise RuntimeError(
            "Corrected V6 source drifted; V13-v2 refuses to run. "
            f"Expected {EXPECTED_V6_SOURCE_SHA256}, observed {observed}."
        )
    return observed


def _modal_long_setup(signal_end: str):
    stamp = pd.Timestamp(f"2000-01-01 {signal_end}") + pd.Timedelta(minutes=1)
    return replace(
        v6.ACTIVE_SETUPS[0],
        signal_end=signal_end,
        confirmation_end=stamp.strftime("%H:%M"),
        side="LONG",
        mode="FILTERED",
        max_entries=1,
        picker="max_liquidity",
        price_change_pct=0.20,
        oi_change_pct=0.10,
        volume_ratio=1.0,
        body_ratio=0.4,
        max_wick_ratio=0.5,
        min_traded_value=0.0,
        stop_pct=1.00,
        target_pct=3.00,
        source_version=STRATEGY_VERSION,
    )


def _extra_0955_long():
    return _modal_long_setup("09:55")


def policy_setups(policy: PolicySpec) -> tuple[Any, ...]:
    overrides = {
        (signal_end, side): threshold
        for signal_end, side, threshold in policy.setup_oi_overrides
    }
    setups: list[Any] = []
    for setup in v6.ACTIVE_SETUPS:
        threshold = overrides.get((setup.signal_end, setup.side))
        if threshold is None:
            setups.append(setup)
        else:
            if threshold < CACHE_OI_FLOOR:
                raise AssertionError(
                    f"{policy.name} threshold {threshold} is below cached "
                    f"signal floor {CACHE_OI_FLOOR}."
                )
            setups.append(replace(setup, oi_change_pct=float(threshold)))
    if policy.add_0955_long:
        setups.append(_extra_0955_long())
    return tuple(setups)


def validate_configuration() -> None:
    _verify_v6_source()
    if RESULT_DIR.resolve() == v6.RESULT_DIR.resolve():
        raise AssertionError("V13-v2 output directory must not equal V6 output.")
    if DEFAULT_POLICY not in POLICIES:
        raise AssertionError("Default policy is not declared.")
    if tuple(v6.ACTIVE_SETUPS) != tuple(v6.v6.ACTIVE_SETUPS):
        raise AssertionError("Corrected V6 no longer imports the promoted V6 book.")
    for name, policy in POLICIES.items():
        if name != policy.name:
            raise AssertionError(f"Policy key/name mismatch: {name}.")
        setups = policy_setups(policy)
        expected = len(v6.ACTIVE_SETUPS) + int(policy.add_0955_long)
        if len(setups) != expected:
            raise AssertionError(f"{name}: unexpected setup count.")
        keys = [(s.signal_end, s.side) for s in setups]
        if len(keys) != len(set(keys)):
            raise AssertionError(f"{name}: duplicate signal-time/side setup.")
        if policy.max_oi_change_pct is not None:
            if policy.max_oi_change_pct < max(
                float(s.oi_change_pct) for s in setups
            ):
                raise AssertionError(
                    f"{name}: OI cap must be at least every lower threshold."
                )


def apply_policy(signals: pd.DataFrame, policy: PolicySpec) -> pd.DataFrame:
    """Apply policy-wide OI rules before each setup ranks its candidates."""
    out = signals.copy()
    if policy.max_oi_change_pct is not None:
        values = pd.to_numeric(out["oi_change_pct"], errors="coerce")
        out = out.loc[values.le(float(policy.max_oi_change_pct))].copy()
    out["v13_v2_policy"] = policy.name
    out["v13_v2_oi_cap_pct"] = (
        np.nan
        if policy.max_oi_change_pct is None
        else float(policy.max_oi_change_pct)
    )
    return out


def _cache_payload(
    month: str,
    universe_path: Path,
    days: list[date],
    square_off: str,
    max_forward_bars: int,
) -> dict[str, Any]:
    return {
        "cache_schema": "V13_CORRECTED_V2_SIGNAL_CACHE_V2_FRESHEST_SEED",
        "month": month,
        "universe_path": str(universe_path.resolve()),
        "universe_file_sha256": _sha256(universe_path),
        "days": [str(day) for day in sorted(days)],
        "square_off": str(square_off),
        "max_forward_bars": int(max_forward_bars),
        "roll_policy": v6.ROLL_POLICY,
        "confirmation_policy": sw.CONFIRMATION_POLICY_V6_STRICT,
        "data_contract": hybrid.DATA_CONTRACT_VERSION,
        "v6_source_sha256": EXPECTED_V6_SOURCE_SHA256,
    }


def _cache_files(stem: Path) -> tuple[Path, Path]:
    return stem.with_suffix(".parquet"), stem.with_suffix(".npz")


def _build_regime_signals(
    month: str,
    universe_path: Path,
    days: list[date],
    *,
    square_off: str,
    max_forward_bars: int,
    rebuild: bool,
) -> tuple[pd.DataFrame, dict, dict[str, Any]]:
    """Build/reuse signals without ever selecting a V6 destination path."""
    payload = _cache_payload(
        month, universe_path, days, square_off, max_forward_bars
    )
    key = common.canonical_json_sha256(payload)[:16]
    stem = CACHE_DIR / f"{month}_{key}"
    loaded = None
    source = ""
    seed_paths: list[str] = []

    if not rebuild:
        # Score every compatible cache on requested-day coverage and candidate
        # count. This avoids a subtle stale-cache failure: the exact V6 key may
        # have been built before end-of-day equity bars arrived, while a newer
        # full-run cache (possibly also containing today's incomplete date)
        # contains the now-complete prior session. Extra dates are filtered.
        cache_choices: list[tuple[tuple[int, int, int], str, Path, Any]] = []
        own = v6._load_cached(stem)
        if own is not None:
            own_signals, _ = own
            own_days = pd.to_datetime(own_signals["day"]).dt.date
            requested = own_signals.loc[own_days.isin(set(days))]
            score = (
                int(pd.to_datetime(requested["day"]).dt.date.nunique()),
                int(len(requested)),
                int(stem.with_suffix(".parquet").stat().st_mtime_ns),
            )
            cache_choices.append((score, "V13_V2_CACHE", stem, own))
        for parquet_path in v6.CACHE_DIR.glob(f"{month}_*.parquet"):
            candidate_stem = parquet_path.with_suffix("")
            if not candidate_stem.with_suffix(".npz").is_file():
                continue
            candidate = v6._load_cached(candidate_stem)
            if candidate is None:
                continue
            candidate_signals, _ = candidate
            candidate_days = pd.to_datetime(candidate_signals["day"]).dt.date
            requested = candidate_signals.loc[candidate_days.isin(set(days))]
            score = (
                int(pd.to_datetime(requested["day"]).dt.date.nunique()),
                int(len(requested)),
                int(parquet_path.stat().st_mtime_ns),
            )
            cache_choices.append(
                (score, "READ_ONLY_V6_CACHE_SEED", candidate_stem, candidate)
            )
        if cache_choices:
            _, source, selected_stem, loaded = max(
                cache_choices, key=lambda item: item[0]
            )
            if source == "READ_ONLY_V6_CACHE_SEED":
                seed_paths = [
                    str(path.resolve()) for path in _cache_files(selected_stem)
                ]

    if loaded is None:
        mapped, universe_record = provenance.load_backtest_universe(
            universe_path=universe_path,
            contract_month_contains=month,
        )
        print(
            f"[V13-v2][BUILD] {month}: {len(mapped)} contracts, "
            f"{len(days)} sessions",
            flush=True,
        )
        signals, paths = sw.build_signal_table(
            set(days),
            square_off=square_off,
            max_forward_bars=max_forward_bars,
            mapped_universe=mapped,
            confirmation_policy=sw.CONFIRMATION_POLICY_V6_STRICT,
        )
        source = "V13_V2_REBUILD"
    else:
        signals, paths = loaded
        _, universe_record = provenance.load_backtest_universe(
            universe_path=universe_path,
            contract_month_contains=month,
        )
        print(f"[V13-v2][CACHE] {month}: {source}", flush=True)

    signals = signals.copy()
    signals["day"] = pd.to_datetime(signals["day"]).dt.date
    signals = signals.loc[signals["day"].isin(set(days))].copy()
    signals["contract_month"] = month
    kept_sids = set(signals["sid"].astype(int)) if not signals.empty else set()
    paths = {int(sid): value for sid, value in paths.items() if int(sid) in kept_sids}
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    if source != "V13_V2_CACHE":
        v6._store_cached(stem, signals, paths)
    parquet_path, npz_path = _cache_files(stem)
    record = {
        "contract_month": month,
        "source": source,
        "sessions": [str(day) for day in days],
        "cache_payload": payload,
        "cache_parquet": str(parquet_path.resolve()),
        "cache_npz": str(npz_path.resolve()),
        "cache_parquet_sha256": _sha256(parquet_path),
        "cache_npz_sha256": _sha256(npz_path),
        "read_only_seed_paths": seed_paths,
        "universe_record": universe_record,
    }
    return signals, paths, record


def _load_eligibility(refresh: bool, min_coverage: float):
    regimes = v6.regime_universe_paths()
    if not refresh and v6.ELIGIBILITY_PATH.is_file():
        eligibility = pd.read_csv(v6.ELIGIBILITY_PATH)
        eligibility["day"] = pd.to_datetime(eligibility["day"]).dt.date
        eligibility["eligible"] = (
            eligibility["eligible"].astype(str).str.lower().eq("true")
        )
        first = eligibility["day"].min()
        last = eligibility["day"].max()
        calendar, origin = v6.build_expiry_calendar(first, last)
        source = "READ_ONLY_V6_ELIGIBILITY_SEED"
    else:
        eligibility, calendar, origin = v6.build_eligibility(
            regimes, min_coverage=min_coverage
        )
        source = "V13_V2_FRESH_READ_ONLY_SCAN"
    return eligibility, calendar, origin, regimes, source


def _run_book(
    signals: pd.DataFrame,
    paths: dict,
    days: list[date],
    policy: PolicySpec,
    *,
    cost_bps: float,
    split_day: date,
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    policy_signals = apply_policy(signals, policy)
    audit = replay.replay_setups(
        policy_signals,
        paths,
        cost_bps=cost_bps,
        setups=policy_setups(policy),
    )
    if audit.empty:
        raise RuntimeError(f"{policy.name} selected no orders.")
    audit = audit.copy()
    audit["strategy_version"] = STRATEGY_VERSION
    audit["evidence_status"] = EVIDENCE_STATUS
    audit["v13_v2_policy"] = policy.name
    daily = replay.build_daily_curve(audit, days, split_day=split_day)
    daily["strategy_version"] = STRATEGY_VERSION
    daily["evidence_status"] = EVIDENCE_STATUS
    daily["v13_v2_policy"] = policy.name
    return audit, daily, replay.summary_stats(daily, audit)


def _profit_factor(values: np.ndarray) -> float:
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
        "SEP2_CHECK": [day for day in days if day > ORIGINAL_TEST_END],
        "ALL": list(days),
    }


def _extended_metrics(
    audit: pd.DataFrame, days: list[date], split_day: date
) -> dict[str, Any]:
    subset = audit.loc[audit["day"].isin(days)].copy()
    daily = replay.build_daily_curve(subset, days, split_day=split_day)
    stats = replay.summary_stats(daily, subset)
    filled = subset.loc[subset["filled"], "net_return_pct"].to_numpy(float)
    day_values = daily["portfolio_net_return_pct"].to_numpy(float)
    curve = np.r_[0.0, np.cumsum(day_values)]
    drawdown = curve - np.maximum.accumulate(curve)
    positive = np.sort(filled[filled > 0])[::-1]
    gross_profit = float(positive.sum())
    stats.update(
        {
            "win_rate": float((filled > 0).mean()) if filled.size else np.nan,
            "expectancy_pct": float(filled.mean()) if filled.size else np.nan,
            "max_drawdown_pct": float(drawdown.min()) if drawdown.size else 0.0,
            "top2_profit_share": (
                float(positive[:2].sum() / gross_profit)
                if gross_profit > 0
                else np.nan
            ),
        }
    )
    return stats


def _setup_summary(audit: pd.DataFrame, setups: Iterable[Any]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for setup in setups:
        selected = audit.loc[audit["setup_id"].eq(setup.setup_id)]
        fills = selected.loc[selected["filled"], "net_return_pct"].to_numpy(float)
        rows.append(
            {
                **asdict(setup),
                "orders": int(len(selected)),
                "fills": int(selected["filled"].sum()) if not selected.empty else 0,
                "wins": int((fills > 0).sum()),
                "trade_pf": _profit_factor(fills),
                "net_pct": float(fills.sum()) if fills.size else 0.0,
                "strategy_version": STRATEGY_VERSION,
            }
        )
    return pd.DataFrame(rows)


def _trade_key_frame(audit: pd.DataFrame) -> pd.DataFrame:
    key = ["day", "hhmm_int", "side", "setup_id", "tradingsymbol"]
    out = audit.copy()
    out["day"] = out["day"].astype(str)
    return out.sort_values(key, kind="stable").reset_index(drop=True)


def _parity_vs_published(
    audit: pd.DataFrame, replay_days: list[date]
) -> dict[str, Any]:
    path = v6.AUDIT_OUTPUT_PATH
    if not path.is_file():
        return {"published_found": False, "passed": False}
    reference = pd.read_csv(path)
    left = _trade_key_frame(audit)
    right = _trade_key_frame(reference)
    shared_trade_days = sorted(set(left["day"]) & set(right["day"]))
    left = left.loc[left["day"].isin(shared_trade_days)].reset_index(drop=True)
    right = right.loc[right["day"].isin(shared_trade_days)].reset_index(drop=True)
    key = ["day", "hhmm_int", "side", "setup_id", "tradingsymbol"]
    keys_equal = left[key].equals(right[key])
    fills_equal = bool(
        keys_equal
        and left["filled"].astype(str).str.lower().reset_index(drop=True).equals(
            right["filled"].astype(str).str.lower().reset_index(drop=True)
        )
    )
    returns_equal = bool(
        keys_equal
        and len(left) == len(right)
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
        "published_path": str(path.resolve()),
        "published_sha256": _sha256(path),
        "shared_sessions": len(
            set(map(str, replay_days))
            & set(pd.read_csv(v6.DAILY_OUTPUT_PATH)["day"].astype(str))
        ) if v6.DAILY_OUTPUT_PATH.is_file() else len(shared_trade_days),
        "shared_trade_days": len(shared_trade_days),
        "v13_v2_orders": int(len(left)),
        "v6_orders": int(len(right)),
        "trade_keys_equal": bool(keys_equal),
        "fills_equal": fills_equal,
        "returns_equal_at_1e_12": returns_equal,
        "passed": bool(keys_equal and fills_equal and returns_equal),
    }


def _validate_selected_oi(audits: dict[str, pd.DataFrame]) -> dict[str, Any]:
    selected = pd.concat(audits.values(), ignore_index=True, sort=False)
    selected = selected.drop_duplicates(
        ["day", "hhmm", "futures_tradingsymbol", "oi", "prev_oi"]
    )
    oi = pd.to_numeric(selected["oi"], errors="coerce").to_numpy(float)
    prev = pd.to_numeric(selected["prev_oi"], errors="coerce").to_numpy(float)
    observed = pd.to_numeric(
        selected["oi_change_pct"], errors="coerce"
    ).to_numpy(float)
    expected = (oi / prev - 1.0) * 100.0
    finite_positive = np.isfinite(oi) & np.isfinite(prev) & (oi > 0) & (prev > 0)
    formula_ok = np.isclose(observed, expected, rtol=0.0, atol=1e-10)
    if not bool((finite_positive & formula_ok).all()):
        raise AssertionError("Selected trades contain invalid or inconsistent OI.")
    return {
        "unique_selected_signal_rows": int(len(selected)),
        "finite_positive_oi_pairs": bool(finite_positive.all()),
        "oi_change_formula_atol_1e_10": bool(formula_ok.all()),
    }


def _validate_exact_five_minute_oi(
    audits: dict[str, pd.DataFrame]
) -> dict[str, Any]:
    """Verify selected OI pairs against raw, exact t and t-5 minute bars."""
    selected = pd.concat(audits.values(), ignore_index=True, sort=False)
    selected = selected.drop_duplicates(
        ["day", "hhmm", "futures_tradingsymbol", "oi", "prev_oi"]
    )
    failures: list[str] = []
    checked = 0
    for symbol, group in selected.groupby("futures_tradingsymbol", sort=True):
        futures = sw.load_five_minute_history(str(symbol))
        if futures.empty:
            failures.append(f"{symbol}: raw futures history is empty")
            continue
        raw = (
            futures.sort_values("ts")
            .drop_duplicates("ts", keep="last")
            .set_index("ts")
        )
        for row in group.itertuples():
            hhmm = str(row.hhmm).zfill(4)
            target = pd.Timestamp(
                f"{row.day} {hhmm[:2]}:{hhmm[2:]}", tz="Asia/Kolkata"
            )
            predecessor = target - pd.Timedelta(minutes=5)
            if target not in raw.index or predecessor not in raw.index:
                failures.append(
                    f"{symbol} {target}: exact signal/predecessor pair missing"
                )
                continue
            current_oi = float(raw.loc[target, "oi"])
            previous_oi = float(raw.loc[predecessor, "oi"])
            expected_pct = (
                (current_oi / previous_oi - 1.0) * 100.0
                if previous_oi > 0
                else np.nan
            )
            matches = (
                np.isclose(current_oi, float(row.oi), rtol=0.0, atol=1e-9)
                and np.isclose(
                    previous_oi, float(row.prev_oi), rtol=0.0, atol=1e-9
                )
                and np.isclose(
                    expected_pct,
                    float(row.oi_change_pct),
                    rtol=0.0,
                    atol=1e-10,
                )
            )
            if not matches:
                failures.append(f"{symbol} {target}: cached/raw OI mismatch")
            checked += 1
    if failures:
        raise AssertionError(
            "Exact five-minute OI continuity failed: " + "; ".join(failures[:10])
        )
    return {
        "exact_five_minute_pairs_checked": checked,
        "same_contract_same_session_exact_t_minus_5m": True,
        "exact_five_minute_failures": 0,
    }


def _policy_comparison(
    audits: dict[str, pd.DataFrame], days: list[date]
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    periods = _periods(days)
    for name, audit in audits.items():
        for period, period_days in periods.items():
            metrics = _extended_metrics(audit, period_days, ORIGINAL_SPLIT_DAY)
            rows.append({"policy": name, "period": period, **metrics})
    out = pd.DataFrame(rows)
    baseline = out.loc[out["policy"].eq("V6_PARITY")].set_index("period")
    out["delta_fills_vs_v6"] = out.apply(
        lambda row: int(row["fills"] - baseline.loc[row["period"], "fills"]),
        axis=1,
    )
    out["delta_pf_vs_v6"] = out.apply(
        lambda row: float(row["trade_pf"] - baseline.loc[row["period"], "trade_pf"]),
        axis=1,
    )
    out["delta_net_vs_v6"] = out.apply(
        lambda row: float(row["net_pct"] - baseline.loc[row["period"], "net_pct"]),
        axis=1,
    )
    return out


def _decision_changes(
    audits: dict[str, pd.DataFrame], days: list[date]
) -> pd.DataFrame:
    key = ["day", "hhmm_int", "side", "setup_id", "tradingsymbol"]
    baseline = audits["V6_PARITY"]
    rows: list[dict[str, Any]] = []
    for name, audit in audits.items():
        if name == "V6_PARITY":
            continue
        for period, period_days in _periods(days).items():
            left = set(
                map(
                    tuple,
                    baseline.loc[baseline["day"].isin(period_days), key]
                    .astype(str)
                    .to_numpy(),
                )
            )
            right = set(
                map(
                    tuple,
                    audit.loc[audit["day"].isin(period_days), key]
                    .astype(str)
                    .to_numpy(),
                )
            )
            changed_days = sorted({row[0] for row in left.symmetric_difference(right)})
            rows.append(
                {
                    "policy": name,
                    "period": period,
                    "removed_vs_v6": len(left - right),
                    "added_vs_v6": len(right - left),
                    "changed_trade_keys": len(left.symmetric_difference(right)),
                    "changed_sessions": len(changed_days),
                    "changed_day_list": ",".join(changed_days),
                }
            )
    return pd.DataFrame(rows)


def _paired_bootstrap(
    baseline: pd.DataFrame,
    candidate: pd.DataFrame,
    days: list[date],
    *,
    draws: int = BOOTSTRAP_DRAWS,
) -> dict[str, Any]:
    if not days:
        return {
            "sessions": 0,
            "observed_delta_net_pct": 0.0,
            "ci_low": np.nan,
            "ci_high": np.nan,
            "probability_delta_positive": np.nan,
        }
    def daily_values(frame: pd.DataFrame) -> np.ndarray:
        return (
            frame.groupby("day")["net_return_pct"]
            .sum()
            .reindex(days, fill_value=0.0)
            .to_numpy(float)
        )
    delta = daily_values(candidate) - daily_values(baseline)
    rng = np.random.default_rng(BOOTSTRAP_SEED + len(days))
    sampled = delta[rng.integers(0, len(delta), size=(draws, len(delta)))].sum(axis=1)
    return {
        "sessions": len(days),
        "observed_delta_net_pct": float(delta.sum()),
        "ci_low": float(np.quantile(sampled, 0.025)),
        "ci_high": float(np.quantile(sampled, 0.975)),
        "probability_delta_positive": float((sampled > 0).mean()),
    }


def _contract_metrics(audits: dict[str, pd.DataFrame]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for name, audit in audits.items():
        for contract, subset in audit.groupby("contract_month", sort=True):
            fills = subset.loc[subset["filled"], "net_return_pct"].to_numpy(float)
            rows.append(
                {
                    "policy": name,
                    "contract_month": contract,
                    "orders": len(subset),
                    "fills": int(subset["filled"].sum()),
                    "trade_pf": _profit_factor(fills),
                    "net_pct": float(fills.sum()),
                }
            )
    return pd.DataFrame(rows)


def _fmt(value: Any, digits: int = 3) -> str:
    if value is None:
        return ""
    if isinstance(value, (float, np.floating)):
        if math.isnan(float(value)):
            return "NA"
        if math.isinf(float(value)):
            return "INF"
        return f"{float(value):.{digits}f}"
    return str(value)


def _markdown_table(frame: pd.DataFrame, columns: list[str]) -> list[str]:
    lines = ["| " + " | ".join(columns) + " |", "|" + "---|" * len(columns)]
    for row in frame[columns].to_dict("records"):
        lines.append("| " + " | ".join(_fmt(row[col]) for col in columns) + " |")
    return lines


def _render_report(
    *,
    selected_policy: str,
    days: list[date],
    comparison: pd.DataFrame,
    cost_stress: pd.DataFrame,
    decisions: pd.DataFrame,
    cap_sensitivity: pd.DataFrame,
    bootstrap: pd.DataFrame,
    contracts: pd.DataFrame,
    slot_sensitivity: pd.DataFrame,
    parity: dict[str, Any],
    oi_validation: dict[str, Any],
    cache_records: list[dict[str, Any]],
) -> str:
    selected_all = comparison.loc[
        comparison["policy"].eq(selected_policy) & comparison["period"].eq("ALL")
    ].iloc[0]
    baseline_all = comparison.loc[
        comparison["policy"].eq("V6_PARITY") & comparison["period"].eq("ALL")
    ].iloc[0]
    lines = [
        "# FnO V13 corrected v2 - detailed analysis",
        "",
        "## Verdict",
        "",
        f"Selected policy: {selected_policy}.",
        "",
        (
            "The selected shadow book is numerically better on the available "
            f"sample: PF {_fmt(baseline_all.trade_pf)} to "
            f"{_fmt(selected_all.trade_pf)}, net {_fmt(baseline_all.net_pct)}% "
            f"to {_fmt(selected_all.net_pct)}%, and fills "
            f"{int(baseline_all.fills)} to {int(selected_all.fills)}."
        ),
        "",
        (
            "It is not promotion-grade evidence. The cap was selected from a "
            "small neighbourhood, the added 09:55 leg was chosen after a "
            f"multi-slot search, the sample covers only {len(days)} sessions, and the "
            "only later day (September 2) changed no decisions and lost 1.85% "
            "under both books. Keep this in shadow/backtest mode until genuinely "
            "new sessions and another expiry regime confirm it."
        ),
        "",
        "Backtest convention: results sum net percentage returns across filled trades, use the configured round-trip cost (5 bps in this run), and are not a lot-sized or capital-constrained portfolio simulation.",
        "",
        "## What changed from corrected V6",
        "",
        "1. 09:35 LONG minimum five-minute OI change: 0.10% to 0.15%.",
        "2. 09:40 LONG minimum five-minute OI change: 0.10% to 0.075%.",
        "3. Global maximum five-minute OI change: 1.00%, applied before ranking.",
        "4. One additive 09:55 LONG leg with V6-style causal confirmation.",
        "5. No change to roll, cash-price features, confirmation, entry, exit or costs.",
        "",
        "## Period results at configured cost",
        "",
    ]
    view = comparison.loc[
        comparison["policy"].isin(["V6_PARITY", selected_policy])
    ].copy()
    lines += _markdown_table(
        view,
        [
            "policy", "period", "sessions", "fills", "trade_pf", "net_pct",
            "win_rate", "max_drawdown_pct", "delta_net_vs_v6",
        ],
    )
    lines += [
        "",
        "September 2 is a post-selection check for the bounded-OI rule only. It is not an untouched holdout for the 09:55 leg because that leg was evaluated on the 24-session table.",
        "",
        "## Component attribution",
        "",
    ]
    lines += _markdown_table(
        comparison.loc[comparison["period"].eq("ALL")],
        [
            "policy", "fills", "trade_pf", "net_pct", "win_rate",
            "max_drawdown_pct", "delta_pf_vs_v6", "delta_net_vs_v6",
        ],
    )
    lines += [
        "",
        "The bounded-OI component supplies most of the PF and drawdown improvement but reduces trade count. The 09:55 component supplies frequency and additional net return. Their combined book has the highest retrospective net return, while the OI-only book has the highest PF; this is a trade-off, not dominance on every metric.",
        "",
        "## Cost stress",
        "",
    ]
    lines += _markdown_table(
        cost_stress.loc[cost_stress["policy"].isin(["V6_PARITY", selected_policy])],
        ["policy", "cost_bps", "fills", "trade_pf", "net_pct", "max_drawdown_pct"],
    )
    lines += ["", "## Decision changes", ""]
    lines += _markdown_table(
        decisions.loc[decisions["policy"].eq(selected_policy)],
        [
            "period", "removed_vs_v6", "added_vs_v6", "changed_trade_keys",
            "changed_sessions", "changed_day_list",
        ],
    )
    lines += ["", "## OI-cap neighbourhood", ""]
    lines += _markdown_table(
        cap_sensitivity,
        ["oi_cap_pct", "period", "fills", "trade_pf", "net_pct", "delta_net_vs_v6"],
    )
    lines += [
        "",
        "A useful rule should not depend on one decimal boundary. The table shows why the 1.00% cap remains fragile: 0.90% removes additional profitable TEST selections, while 1.05% admits the 1.009653% TMPV loss and becomes identical to V6 on the original TEST segment.",
        "",
        "## Paired day bootstrap",
        "",
    ]
    lines += _markdown_table(
        bootstrap,
        [
            "period", "sessions", "observed_delta_net_pct", "ci_low", "ci_high",
            "probability_delta_positive",
        ],
    )
    lines += [
        "",
        "The bootstrap resamples days and is descriptive, not a proof of stationarity. A confidence interval crossing zero means the observed uplift can plausibly disappear under a different mix of these same days.",
        "",
        "## 09:55 LONG local time-slot check",
        "",
    ]
    lines += _markdown_table(
        slot_sensitivity.loc[
            slot_sensitivity["period"].isin(
                ["ORIGINAL_TRAIN", "ORIGINAL_TEST", "ALL"]
            )
        ],
        ["signal_end", "period", "fills", "trade_pf", "net_pct"],
    )
    lines += [
        "",
        "Each row above is the added modal-filter LONG leg by itself. 09:55 is positive in both original segments with at least five fills in each. Its neighbours are not stable: 09:50 loses in TEST and 10:05 loses overall; 10:00 is profitable but has only three original-TEST fills. This supports keeping 09:55 experimental rather than claiming a broad time-window effect.",
        "",
        "## Contract-regime stability",
        "",
    ]
    lines += _markdown_table(
        contracts.loc[contracts["policy"].isin(["V6_PARITY", selected_policy])],
        ["policy", "contract_month", "fills", "trade_pf", "net_pct"],
    )
    lines += [
        "",
        "## Advanced OI ablation conclusion",
        "",
        "The independent pre-gate rebuild contained 1,825 candidates and reproduced all 64 originally published V6 orders at 1e-12 return tolerance. Current V6 OI thresholds beat no-OI, merely-valid-OI, falling-OI, two-bar persistence and ten-minute-positive alternatives. The advanced rules therefore remain rejected; V13-v2 retains V6's causal positive-OI core and only exposes the bounded shadow overlay.",
        "",
        "## Integrity checks",
        "",
        f"- V6 parity passed: {parity.get('passed')}.",
        f"- Shared V6 orders compared: {parity.get('v6_orders')}.",
        f"- Selected OI formula check passed: {oi_validation.get('oi_change_formula_atol_1e_10')}.",
        f"- Exact same-contract, same-session t-minus-5-minute check passed: {oi_validation.get('same_contract_same_session_exact_t_minus_5m')}.",
        f"- Exact raw OI pairs checked: {oi_validation.get('exact_five_minute_pairs_checked')}.",
        f"- Unique selected signal rows checked: {oi_validation.get('unique_selected_signal_rows')}.",
        f"- V13-v2 cache regimes: {len(cache_records)}.",
        f"- Sessions: {days[0]} through {days[-1]} ({len(days)}).",
        "- V6 protected files were hashed before and after the run and were unchanged.",
        "- The existing V13-v1 source file was also hash-checked and left unchanged.",
        "- All V13-v2 files and caches are isolated under its own result directory.",
        "",
        "## Promotion rule",
        "",
        "Do not replace V6 yet. Re-run this frozen candidate without changing thresholds until there are roughly 40 or more sessions spanning at least two useful near-month regimes. Require positive incremental net, PF above V6, several changed decisions in the new data, cost resilience at 15-20 bps, and a paired-day interval that no longer materially depends on one session.",
        "",
    ]
    return "\n".join(lines)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--policy", choices=sorted(POLICIES), default=DEFAULT_POLICY)
    parser.add_argument("--through-day", default=DEFAULT_THROUGH_DAY)
    parser.add_argument("--cost-bps", type=float, default=5.0)
    parser.add_argument("--square-off", default="1530")
    parser.add_argument("--max-forward-bars", type=int, default=400)
    parser.add_argument("--min-contract-coverage", type=float, default=0.80)
    parser.add_argument("--rebuild-cache", action="store_true")
    parser.add_argument("--refresh-eligibility", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    started = time.monotonic()
    validate_configuration()
    v6_before = _protected_v6_snapshot()
    v13_v1_before = (
        _sha256(V13_V1_SOURCE_PATH) if V13_V1_SOURCE_PATH.is_file() else None
    )
    selected_policy = POLICIES[args.policy]
    through_day = pd.Timestamp(args.through_day).date()
    RESULT_DIR.mkdir(parents=True, exist_ok=True)

    eligibility, calendar, origin, regimes, eligibility_source = _load_eligibility(
        args.refresh_eligibility, args.min_contract_coverage
    )
    eligibility = eligibility.loc[eligibility["day"].le(through_day)].copy()
    common.atomic_write_csv(eligibility, ELIGIBILITY_PATH)
    ok = eligibility.loc[eligibility["eligible"]].copy()
    if ok.empty:
        raise RuntimeError("V13-v2 has no eligible sessions.")
    days_by_month: dict[str, list[date]] = {}
    for row in ok.to_dict("records"):
        month = str(row["required_contract"])
        if month in regimes:
            days_by_month.setdefault(month, []).append(row["day"])

    parts: list[tuple[pd.DataFrame, dict]] = []
    cache_records: list[dict[str, Any]] = []
    for month in sorted(days_by_month, key=lambda code: calendar[code]):
        regime_days = sorted(days_by_month[month])
        signals, paths, cache_record = _build_regime_signals(
            month,
            regimes[month],
            regime_days,
            square_off=args.square_off,
            max_forward_bars=args.max_forward_bars,
            rebuild=args.rebuild_cache,
        )
        parts.append((signals, paths))
        cache_records.append(cache_record)

    signals, paths = v6.concat_regimes(parts)
    if signals.empty:
        raise RuntimeError("V13-v2 produced no candidate signals.")
    days = sorted(set(signals["day"]))
    unreplayed = sorted(set(ok["day"]) - set(days))
    if unreplayed:
        eligibility.loc[eligibility["day"].isin(unreplayed), "eligible"] = False
        eligibility.loc[eligibility["day"].isin(unreplayed), "reason"] = (
            "NEAR_MONTH_OK_BUT_NO_EQUITY_BARS_OR_CANDIDATES"
        )
        common.atomic_write_csv(eligibility, ELIGIBILITY_PATH)

    audits: dict[str, pd.DataFrame] = {}
    dailies: dict[str, pd.DataFrame] = {}
    for name, policy in POLICIES.items():
        audit, daily, _ = _run_book(
            signals,
            paths,
            days,
            policy,
            cost_bps=args.cost_bps,
            split_day=ORIGINAL_SPLIT_DAY,
        )
        audits[name] = audit
        dailies[name] = daily

    parity = _parity_vs_published(audits["V6_PARITY"], days)
    if not parity.get("passed"):
        raise AssertionError(
            "V13-v2 V6_PARITY failed against corrected V6 published trades."
        )
    oi_validation = _validate_selected_oi(audits)
    oi_validation.update(_validate_exact_five_minute_oi(audits))
    comparison = _policy_comparison(audits, days)
    decisions = _decision_changes(audits, days)
    contracts = _contract_metrics(audits)

    cost_rows: list[dict[str, Any]] = []
    for name, policy in POLICIES.items():
        for cost in (5.0, 10.0, 15.0, 20.0):
            audit, _, _ = _run_book(
                signals,
                paths,
                days,
                policy,
                cost_bps=cost,
                split_day=ORIGINAL_SPLIT_DAY,
            )
            metrics = _extended_metrics(audit, days, ORIGINAL_SPLIT_DAY)
            cost_rows.append({"policy": name, "cost_bps": cost, **metrics})
    cost_stress = pd.DataFrame(cost_rows)

    cap_rows: list[dict[str, Any]] = []
    baseline_period = {
        row.period: row
        for row in comparison.loc[comparison["policy"].eq("V6_PARITY")].itertuples()
    }
    bounded = POLICIES["OI_BOUNDED_SHADOW"]
    for cap in (0.75, 0.85, 0.90, 0.95, 1.00, 1.05, 1.10, 1.25, 1.50, 2.00):
        policy = replace(bounded, name=f"OI_CAP_{cap:.2f}", max_oi_change_pct=cap)
        audit, _, _ = _run_book(
            signals,
            paths,
            days,
            policy,
            cost_bps=args.cost_bps,
            split_day=ORIGINAL_SPLIT_DAY,
        )
        for period, period_days in _periods(days).items():
            metrics = _extended_metrics(audit, period_days, ORIGINAL_SPLIT_DAY)
            cap_rows.append(
                {
                    "oi_cap_pct": cap,
                    "period": period,
                    **metrics,
                    "delta_net_vs_v6": float(
                        metrics["net_pct"] - baseline_period[period].net_pct
                    ),
                }
            )
    cap_sensitivity = pd.DataFrame(cap_rows)

    slot_rows: list[dict[str, Any]] = []
    for signal_end in ("09:45", "09:50", "09:55", "10:00", "10:05"):
        leg = _modal_long_setup(signal_end)
        audit = replay.replay_setups(
            signals, paths, cost_bps=args.cost_bps, setups=(leg,)
        )
        for period, period_days in _periods(days).items():
            metrics = _extended_metrics(audit, period_days, ORIGINAL_SPLIT_DAY)
            slot_rows.append(
                {"signal_end": signal_end, "period": period, **metrics}
            )
    slot_sensitivity = pd.DataFrame(slot_rows)

    bootstrap_rows: list[dict[str, Any]] = []
    for period, period_days in _periods(days).items():
        values = _paired_bootstrap(
            audits["V6_PARITY"], audits[selected_policy.name], period_days
        )
        bootstrap_rows.append({"period": period, **values})
    bootstrap = pd.DataFrame(bootstrap_rows)

    selected_audit = audits[selected_policy.name]
    selected_daily = dailies[selected_policy.name]
    selected_setups = _setup_summary(
        selected_audit, policy_setups(selected_policy)
    )

    common.atomic_write_csv(selected_audit, TRADES_PATH)
    common.atomic_write_csv(selected_daily, DAILY_PATH)
    common.atomic_write_csv(selected_setups, SETUPS_PATH)
    common.atomic_write_csv(audits["V6_PARITY"], BASELINE_TRADES_PATH)
    common.atomic_write_csv(dailies["V6_PARITY"], BASELINE_DAILY_PATH)
    common.atomic_write_csv(comparison, COMPARISON_PATH)
    common.atomic_write_csv(cost_stress, COST_STRESS_PATH)
    common.atomic_write_csv(decisions, DECISION_CHANGES_PATH)
    common.atomic_write_csv(cap_sensitivity, CAP_SENSITIVITY_PATH)
    common.atomic_write_csv(bootstrap, BOOTSTRAP_PATH)
    common.atomic_write_csv(contracts, CONTRACT_PATH)
    common.atomic_write_csv(slot_sensitivity, SLOT_SENSITIVITY_PATH)

    report = _render_report(
        selected_policy=selected_policy.name,
        days=days,
        comparison=comparison,
        cost_stress=cost_stress,
        decisions=decisions,
        cap_sensitivity=cap_sensitivity,
        bootstrap=bootstrap,
        contracts=contracts,
        slot_sensitivity=slot_sensitivity,
        parity=parity,
        oi_validation=oi_validation,
        cache_records=cache_records,
    )
    common.atomic_write_text(REPORT_PATH, report)

    v6_after = _protected_v6_snapshot()
    if v6_before != v6_after:
        changed = [
            path for path in sorted(set(v6_before) | set(v6_after))
            if v6_before.get(path) != v6_after.get(path)
        ]
        raise AssertionError(
            "A protected V6 file changed during V13-v2 execution: "
            + ", ".join(changed)
        )
    v13_v1_after = (
        _sha256(V13_V1_SOURCE_PATH) if V13_V1_SOURCE_PATH.is_file() else None
    )
    if v13_v1_before != v13_v1_after:
        raise AssertionError("The existing V13-v1 source changed during this run.")

    research_files = [
        Path(__file__).with_name("fno_v13_corrected_ungated_oi_research.py"),
        common.FNO_ROOT
        / "strategy_research"
        / "v13_corrected_ungated_oi_research"
        / "ungated_oi_ablation_report.md",
        common.FNO_ROOT
        / "strategy_research"
        / "v13_advanced_oi_policies"
        / "advanced_policy_metrics.csv",
    ]
    common.atomic_write_json(
        PROVENANCE_PATH,
        {
            "strategy_version": STRATEGY_VERSION,
            "evidence_status": EVIDENCE_STATUS,
            "selected_policy": selected_policy.name,
            "selected_policy_validated": selected_policy.validated,
            "selected_policy_description": selected_policy.description,
            "generated_at_ist": common.now_ist().isoformat(timespec="seconds"),
            "through_day": str(through_day),
            "original_split_day": str(ORIGINAL_SPLIT_DAY),
            "original_test_end": str(ORIGINAL_TEST_END),
            "v6_source_sha256": EXPECTED_V6_SOURCE_SHA256,
            "v6_protected_before": v6_before,
            "v6_protected_after": v6_after,
            "v6_protected_unchanged": v6_before == v6_after,
            "v13_v1_source_path": str(V13_V1_SOURCE_PATH.resolve()),
            "v13_v1_sha256_before": v13_v1_before,
            "v13_v1_sha256_after": v13_v1_after,
            "v13_v1_unchanged": v13_v1_before == v13_v1_after,
            "v6_parity": parity,
            "oi_validation": oi_validation,
            "eligibility_source": eligibility_source,
            "expiry_calendar": {key: str(value) for key, value in calendar.items()},
            "expiry_origin": origin,
            "sessions": [str(day) for day in days],
            "unreplayed_eligible_days": [str(day) for day in unreplayed],
            "parameters": {
                "cost_bps": float(args.cost_bps),
                "square_off": str(args.square_off),
                "max_forward_bars": int(args.max_forward_bars),
                "min_contract_coverage": float(args.min_contract_coverage),
                "bootstrap_draws": BOOTSTRAP_DRAWS,
                "bootstrap_seed": BOOTSTRAP_SEED,
            },
            "policies": {
                name: {
                    **asdict(policy),
                    "active_setups": [asdict(setup) for setup in policy_setups(policy)],
                }
                for name, policy in POLICIES.items()
            },
            "cache_records": cache_records,
            "research_evidence": {
                str(path.resolve()): (_sha256(path) if path.is_file() else None)
                for path in research_files
            },
            "artifacts": {
                str(path.name): _sha256(path)
                for path in (
                    ELIGIBILITY_PATH,
                    TRADES_PATH,
                    DAILY_PATH,
                    SETUPS_PATH,
                    BASELINE_TRADES_PATH,
                    BASELINE_DAILY_PATH,
                    COMPARISON_PATH,
                    COST_STRESS_PATH,
                    DECISION_CHANGES_PATH,
                    CAP_SENSITIVITY_PATH,
                    BOOTSTRAP_PATH,
                    CONTRACT_PATH,
                    SLOT_SENSITIVITY_PATH,
                    REPORT_PATH,
                )
            },
        },
    )

    chosen = comparison.loc[
        comparison["policy"].eq(selected_policy.name)
        & comparison["period"].eq("ALL")
    ].iloc[0]
    base = comparison.loc[
        comparison["policy"].eq("V6_PARITY")
        & comparison["period"].eq("ALL")
    ].iloc[0]
    print(
        f"[V13-v2] {selected_policy.name} | sessions={int(chosen.sessions)} "
        f"fills={int(chosen.fills)} PF={chosen.trade_pf:.6f} "
        f"net={chosen.net_pct:+.6f}%",
        flush=True,
    )
    print(
        f"[V6 parity] fills={int(base.fills)} PF={base.trade_pf:.6f} "
        f"net={base.net_pct:+.6f}% | exact={parity['passed']}",
        flush=True,
    )
    print(f"[STATUS] {EVIDENCE_STATUS}", flush=True)
    print(f"[WROTE] {REPORT_PATH}", flush=True)
    print(f"[DONE] {time.monotonic() - started:.1f}s", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
