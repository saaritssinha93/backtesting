"""V13-v5 research backtest with corrected execution and three frozen profiles.

This is an independently runnable, research-only wrapper around the immutable
V13-v3 signal definition.  It deliberately does not place orders or modify any
live configuration.  The higher-frequency profile is the configured default;
balanced and conservative remain available as research comparators.

Important data-contract fact: prices, volume, EMA, confirmation, entry and
exit are NSE cash-equity observations.  A mapped stock future supplies OI only.
There is no option premium, strike, CE/PE or option-expiry execution in this
backtest despite the historical ``FNO`` filename.

Entry-delay cap (added 2026-09-04): the original ``_entry()`` scanned the
entire remaining forward path for the first trigger touch, with no time-box -
a signal confirmed at S+1 could fill hours later on an unrelated intraday
move.  A grid search over S+1..S+60 on the higher_frequency profile's real
25-session trades, selected on TRAIN+VALIDATION only, found S+10 minutes
after the confirmation candle close is the best-performing finite cutoff
(VALIDATION PF 2.72 vs 2.38 uncapped; strictly dominates every wider cutoff
tested).  It does not beat the uncapped baseline on TRAIN+VALIDATION combined
(36.09 vs 36.36) - this is a deliberate, requested change to the entry rule
itself, not a validated improvement.  Applied via ``MAX_ENTRY_DELAY_MINUTES``
to ``simulate_scaleout`` (all three profiles).  ``simulate_native`` keeps the
original unlimited window by default, since it exists to reproduce V13-v3's
published figures exactly.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import tempfile
import time
from dataclasses import asdict, dataclass, replace
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
import fno_v13_corrected_v3_backtest as v13_v3


STRATEGY_VERSION = "FNO_V13_CORRECTED_V5_RESEARCH_20260904_ENTRY_S10"
EVIDENCE_STATUS = "EXPERIMENTAL_SHADOW_NOT_PRODUCTION_PROMOTED"
EXPECTED_V13_V3_SOURCE_SHA256 = (
    "85c2ff1c37a342e8e0bc4b73eb115de8ebc5aa7e990db261e46036b68aeafbab"
)
BASE_POLICY_NAME = v13_v3.BASE_POLICY_NAME

DEFAULT_THROUGH_DAY = "2026-09-03"
DEFAULT_COST_BPS = 5.0
DEFAULT_CAPITAL_PER_ENTRY_RUPEES = 100_000.0
DEFAULT_LEVERAGE_FACTOR = 5.0
DEFAULT_PROFILE = "higher_frequency"
OFFICIAL_CUTOFF = "1515"
MAX_FORWARD_BARS = 400
# Entry must trigger within this many minutes of the confirmation candle's
# close, or the candidate is treated as never filled. Applied to
# simulate_scaleout (the three live profiles); simulate_native keeps the
# original unlimited window so it still reproduces V13-v3 exactly.
MAX_ENTRY_DELAY_MINUTES = 10
MIN_CONTRACT_COVERAGE = 0.99

TRAIN_END = date(2026, 8, 13)
VALIDATION_END = date(2026, 8, 26)
BOOTSTRAP_SEED = 20260904

RESULT_DIR = common.FNO_ROOT / "strategy_research" / "v13_corrected_v5"
CACHE_DIR = RESULT_DIR / "_cache"
REPORT_PATH = RESULT_DIR / "FNO_V13_CORRECTED_V5_DETAILED_RESULTS.md"
WORKSPACE_REPORT_PATH = Path(__file__).with_name(
    "FNO_V13_CORRECTED_V5_DETAILED_RESULTS.md"
)
COMPARISON_PATH = RESULT_DIR / "fno_v13_corrected_v5_profile_comparison.csv"
PARAMETER_REGISTRY_PATH = RESULT_DIR / "fno_v13_corrected_v5_parameter_registry.csv"
PROVENANCE_PATH = RESULT_DIR / "fno_v13_corrected_v5_provenance.json"
ELIGIBILITY_PATH = RESULT_DIR / "fno_v13_corrected_v5_session_eligibility.csv"
RESEARCH_DIR = RESULT_DIR / "research"
RESEARCH_REPORT_PATH = RESEARCH_DIR / "FNO_V13_V5_RESEARCH_AUDIT.md"
RESEARCH_MANIFEST_PATH = RESEARCH_DIR / "fno_v13_v5_research_manifest.csv"


@dataclass(frozen=True)
class ExitSpec:
    initial_stop_pct: float
    first_target_pct: float
    partial_pct: float
    runner_target_pct: float
    runner_stop: str = "BREAKEVEN"
    maximum_holding_minutes: int | None = None


@dataclass(frozen=True)
class ProfileSpec:
    name: str
    description: str
    excluded_setup_ids: tuple[str, ...]
    add_0950_short: bool
    add_1120_short: bool
    wick_cap_delta: float
    exit: ExitSpec
    evidence: str


PROFILES: dict[str, ProfileSpec] = {
    "balanced": ProfileSpec(
        name="balanced",
        description=(
            "V13-v3 entries plus exploratory 11:20 SHORT; 10% first-stage trim "
            "keeps most exposure for the 2.60% runner while avoiding the "
            "train-only wick relaxation."
        ),
        excluded_setup_ids=(),
        add_0950_short=False,
        add_1120_short=True,
        wick_cap_delta=0.0,
        exit=ExitSpec(1.50, 1.075, 0.10, 2.60),
        evidence=(
            "DEVELOPMENT_SELECTED_PARETO_SHADOW;_1120_LEG_ONLY_5_FILLS;_"
            "NO_UNTOUCHED_TEST"
        ),
    ),
    "conservative": ProfileSpec(
        name="conservative",
        description=(
            "Remove the two development-negative setup cells (09:35 LONG and "
            "09:45 LONG), book 20% at T1, and cap holding time at 180 minutes "
            "to reduce exposure and drawdown."
        ),
        excluded_setup_ids=("0936_LONG", "0946_LONG"),
        add_0950_short=False,
        add_1120_short=False,
        wick_cap_delta=0.0,
        exit=ExitSpec(1.50, 1.075, 0.20, 2.60, maximum_holding_minutes=180),
        evidence="DEVELOPMENT_RISK_ABLATION;_LOWER_RETURN_FOR_LOWER_DRAWDOWN",
    ),
    "higher_frequency": ProfileSpec(
        name="higher_frequency",
        description=(
            "V13-v3 entries plus the previously absent 09:50 SHORT and 11:20 "
            "SHORT cells, with a +0.10 wick-cap relaxation. Both timing legs and "
            "the wick relaxation remain sparse and shadow-only."
        ),
        excluded_setup_ids=(),
        add_0950_short=True,
        add_1120_short=True,
        wick_cap_delta=0.10,
        exit=ExitSpec(1.50, 1.075, 0.10, 2.60),
        evidence=(
            "AGGREGATE_COST_ROBUST_FREQUENCY_SHADOW;_TRAIN_NET_EDGE_FAILS_"
            "ABOVE_5BPS;_NO_UNTOUCHED_TEST"
        ),
    ),
}


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _pf(values: np.ndarray) -> float:
    values = values[np.isfinite(values)]
    gains = float(values[values > 0].sum()) if values.size else 0.0
    losses = float(-values[values < 0].sum()) if values.size else 0.0
    if losses:
        return gains / losses
    return float("inf") if gains else float("nan")


def _fmt(value: Any, digits: int = 3) -> str:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return ""
    if math.isnan(number):
        return "N/A"
    if math.isinf(number):
        return "INF"
    return f"{number:.{digits}f}"


def apply_fixed_capital_model(
    audit: pd.DataFrame,
    capital_per_entry_rupees: float,
    leverage_factor: float = DEFAULT_LEVERAGE_FACTOR,
) -> pd.DataFrame:
    if not np.isfinite(capital_per_entry_rupees) or capital_per_entry_rupees <= 0:
        raise ValueError("capital_per_entry_rupees must be a positive finite value.")
    if not np.isfinite(leverage_factor) or leverage_factor <= 0:
        raise ValueError("leverage_factor must be a positive finite value.")
    out = audit.copy()
    filled = out.get("filled", pd.Series(False, index=out.index)).astype(bool)
    net_return = pd.to_numeric(
        out.get("net_return_pct", pd.Series(np.nan, index=out.index)),
        errors="coerce",
    )
    gross_return = pd.to_numeric(
        out.get("gross_return_pct", pd.Series(np.nan, index=out.index)),
        errors="coerce",
    )
    cost_pct = pd.to_numeric(
        out.get("cost_pct", pd.Series(np.nan, index=out.index)),
        errors="coerce",
    )
    exposure_per_entry_rupees = capital_per_entry_rupees * leverage_factor
    out["capital_model"] = "FIXED_CAPITAL_PER_FILLED_TRADE_WITH_LEVERAGE_NON_COMPOUNDED"
    out["capital_per_entry_rupees"] = np.where(filled, capital_per_entry_rupees, 0.0)
    out["leverage_factor"] = leverage_factor
    out["exposure_per_entry_rupees"] = np.where(filled, exposure_per_entry_rupees, 0.0)
    out["gross_return_on_capital_pct"] = np.where(
        filled, gross_return * leverage_factor, np.nan
    )
    out["net_return_on_capital_pct"] = np.where(
        filled, net_return * leverage_factor, np.nan
    )
    out["unleveraged_pre_cost_profit_rupees"] = np.where(
        filled, gross_return / 100.0 * capital_per_entry_rupees, 0.0
    )
    out["unleveraged_cost_rupees"] = np.where(
        filled, cost_pct / 100.0 * capital_per_entry_rupees, 0.0
    )
    out["unleveraged_net_profit_rupees"] = np.where(
        filled, net_return / 100.0 * capital_per_entry_rupees, 0.0
    )
    out["pre_cost_profit_rupees"] = np.where(
        filled, gross_return / 100.0 * exposure_per_entry_rupees, 0.0
    )
    out["cost_rupees"] = np.where(
        filled, cost_pct / 100.0 * exposure_per_entry_rupees, 0.0
    )
    out["net_profit_rupees"] = np.where(
        filled, net_return / 100.0 * exposure_per_entry_rupees, 0.0
    )
    return out


def validate_configuration() -> None:
    observed = _sha256(Path(v13_v3.__file__).resolve())
    if observed != EXPECTED_V13_V3_SOURCE_SHA256:
        raise RuntimeError(
            "V13-v3 source drifted; V13-v5 refuses to run. "
            f"Expected {EXPECTED_V13_V3_SOURCE_SHA256}, observed {observed}."
        )
    v13_v3.validate_configuration()
    if RESULT_DIR.resolve() in {
        v13_v3.RESULT_DIR.resolve(),
        v13_v2.RESULT_DIR.resolve(),
        v6.RESULT_DIR.resolve(),
    }:
        raise AssertionError("V13-v5 output must be isolated from prior versions.")
    for profile in PROFILES.values():
        exit_spec = profile.exit
        if exit_spec.runner_stop != "BREAKEVEN":
            raise AssertionError("Only the audited immediate-breakeven runner is frozen.")
        if not 0.0 < exit_spec.partial_pct < 1.0:
            raise AssertionError("Partial fraction must be strictly between zero and one.")
    if DEFAULT_PROFILE not in PROFILES:
        raise AssertionError(f"Unknown configured default profile: {DEFAULT_PROFILE}.")


def added_short_setup(signal_end: str):
    """Frozen modal timing probe used for the two sparse added SHORT cells."""

    return replace(
        v13_v2._modal_long_setup(signal_end),
        side="SHORT",
        price_change_pct=0.20,
        oi_change_pct=0.10,
        volume_ratio=1.00,
        body_ratio=0.40,
        max_wick_ratio=0.50,
        min_traded_value=0.0,
        max_entries=1,
        picker="max_liquidity",
        stop_pct=1.00,
        target_pct=3.00,
        source_version=STRATEGY_VERSION,
    )


def profile_setups(profile: ProfileSpec) -> tuple[Any, ...]:
    setups = [
        setup
        for setup in v13_v3.active_setups()
        if setup.setup_id not in profile.excluded_setup_ids
    ]
    if profile.add_0950_short:
        setups.append(added_short_setup("09:50"))
    if profile.add_1120_short:
        setups.append(added_short_setup("11:20"))
    if profile.wick_cap_delta:
        setups = [
            replace(
                setup,
                max_wick_ratio=min(
                    1.0, setup.max_wick_ratio + profile.wick_cap_delta
                ),
                source_version=STRATEGY_VERSION,
            )
            for setup in setups
        ]
    keys = [(setup.signal_end, setup.side) for setup in setups]
    if len(keys) != len(set(keys)):
        raise AssertionError(f"Duplicate time/side setup in profile {profile.name}.")
    return tuple(setups)


def _cache_files(stem: Path) -> tuple[Path, Path, Path]:
    return (
        stem.with_suffix(".parquet"),
        stem.with_suffix(".npz"),
        stem.with_suffix(".json"),
    )


def _load_verified_v5_cache(
    stem: Path, payload: dict[str, Any]
) -> tuple[tuple[pd.DataFrame, dict[int, dict[str, np.ndarray]]], dict[str, Any]] | None:
    parquet_path, npz_path, manifest_path = _cache_files(stem)
    if not all(path.is_file() for path in (parquet_path, npz_path, manifest_path)):
        return None
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if manifest.get("payload") != payload:
        return None
    if manifest.get("parquet_sha256") != _sha256(parquet_path):
        return None
    if manifest.get("npz_sha256") != _sha256(npz_path):
        return None
    loaded = v6._load_cached(stem)
    return (loaded, manifest) if loaded is not None else None


def _trim_hlc_paths_to_cutoff(
    signals: pd.DataFrame,
    paths: dict[int, dict[str, np.ndarray]],
) -> dict[int, dict[str, np.ndarray]]:
    """Trim inherited H/L/C cache paths to 15:15.

    These timestamp-free paths are retained only so the cache remains compatible
    with the inherited signal-table format. V13-v5 execution always rematerializes
    O/H/L/C/timestamps from raw one-minute data and validates continuity.
    """

    limits: dict[int, int] = {}
    for row in signals.drop_duplicates("sid").itertuples(index=False):
        confirmation = pd.Timestamp(row.confirmation_ts)
        if confirmation.tzinfo is None:
            confirmation = confirmation.tz_localize(common.IST)
        else:
            confirmation = confirmation.tz_convert(common.IST)
        cutoff = confirmation.normalize() + pd.Timedelta(hours=15, minutes=15)
        limits[int(row.sid)] = max(
            0,
            min(
                MAX_FORWARD_BARS,
                int((cutoff - confirmation).total_seconds() // 60),
            ),
        )
    trimmed: dict[int, dict[str, np.ndarray]] = {}
    for sid, path in paths.items():
        limit = limits.get(int(sid), 0)
        if limit <= 0:
            continue
        trimmed[int(sid)] = {
            field: np.asarray(path[field], dtype=float)[:limit]
            for field in ("high", "low", "close")
        }
    return trimmed


def _store_v5_cache(
    stem: Path,
    signals: pd.DataFrame,
    paths: dict[int, dict[str, np.ndarray]],
    *,
    payload: dict[str, Any],
    source: str,
    source_record: dict[str, Any] | None,
) -> dict[str, Any]:
    """Publish a cache atomically; the checksum manifest is committed last."""

    parquet_path, npz_path, manifest_path = _cache_files(stem)
    stem.parent.mkdir(parents=True, exist_ok=True)
    common.atomic_write_parquet(signals, parquet_path)
    flat: dict[str, np.ndarray] = {}
    suffix = {"high": "h", "low": "l", "close": "c"}
    for sid, path in paths.items():
        for field, code in suffix.items():
            flat[f"{sid}_{code}"] = np.asarray(path[field], dtype=float)
    temporary: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            prefix=f".{npz_path.name}.",
            suffix=".tmp.npz",
            dir=str(npz_path.parent),
            delete=False,
        ) as handle:
            temporary = Path(handle.name)
        np.savez_compressed(temporary, **flat)
        os.replace(temporary, npz_path)
        temporary = None
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)
    manifest = {
        "schema": "V13_V5_VERIFIED_CACHE_MANIFEST_V1",
        "payload": payload,
        "source": source,
        "source_record": source_record,
        "path_contract": (
            "TIMESTAMP_FREE_HLC_SELECTION_CACHE_TRIMMED_TO_1515;_"
            "NOT_USED_FOR_V13_V5_EXECUTION"
        ),
        "rows": int(len(signals)),
        "paths": int(len(paths)),
        "parquet_sha256": _sha256(parquet_path),
        "npz_sha256": _sha256(npz_path),
    }
    common.atomic_write_json(manifest_path, manifest)
    return manifest


def _load_verified_v3_seed(
    month: str,
    universe_path: Path,
    days: list[date],
) -> tuple[tuple[pd.DataFrame, dict[int, dict[str, np.ndarray]]], dict[str, Any]] | None:
    """Use only the checksum-pinned cache recorded by V13-v3 provenance."""

    if not v13_v3.PROVENANCE_PATH.is_file():
        return None
    try:
        published = json.loads(v13_v3.PROVENANCE_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    requested_days = set(map(str, days))
    for record in published.get("cache_records", []):
        cache_payload = record.get("cache_payload", {})
        if str(record.get("contract_month")) != month:
            continue
        if cache_payload.get("confirmation_policy") != sweep.CONFIRMATION_POLICY_V6_STRICT:
            continue
        if cache_payload.get("data_contract") != hybrid.DATA_CONTRACT_VERSION:
            continue
        if int(cache_payload.get("max_forward_bars", -1)) != MAX_FORWARD_BARS:
            continue
        if Path(str(cache_payload.get("universe_path", ""))).resolve() != universe_path.resolve():
            continue
        if cache_payload.get("universe_sha256") != _sha256(universe_path):
            continue
        if not requested_days.issubset(set(map(str, record.get("sessions", [])))):
            continue
        parquet_path = Path(str(record.get("cache_parquet", "")))
        npz_path = Path(str(record.get("cache_npz", "")))
        if not parquet_path.is_file() or not npz_path.is_file():
            continue
        if record.get("cache_parquet_sha256") != _sha256(parquet_path):
            continue
        if record.get("cache_npz_sha256") != _sha256(npz_path):
            continue
        loaded = v6._load_cached(parquet_path.with_suffix(""))
        if loaded is None:
            continue
        return loaded, {
            "v13_v3_provenance": str(v13_v3.PROVENANCE_PATH.resolve()),
            "v13_v3_provenance_sha256": _sha256(v13_v3.PROVENANCE_PATH),
            "cache_parquet": str(parquet_path.resolve()),
            "cache_parquet_sha256": record["cache_parquet_sha256"],
            "cache_npz": str(npz_path.resolve()),
            "cache_npz_sha256": record["cache_npz_sha256"],
        }
    return None


def _load_signal_regime(
    month: str,
    universe_path: Path,
    days: list[date],
    *,
    rebuild: bool,
) -> tuple[pd.DataFrame, dict[int, dict[str, np.ndarray]], dict[str, Any]]:
    """Load a V5-owned cache, seed read-only from old caches, or build raw.

    Unlike V13-v3's loader, this function never rewrites a prior-version cache.
    """

    payload = {
        "schema": "V13_V5_STRICT_SIGNAL_CACHE_V1",
        "month": month,
        "universe": str(universe_path.resolve()),
        "universe_sha256": _sha256(universe_path),
        "days": [str(day) for day in days],
        "square_off": OFFICIAL_CUTOFF,
        "max_forward_bars": MAX_FORWARD_BARS,
        "confirmation_policy": sweep.CONFIRMATION_POLICY_V6_STRICT,
        "data_contract": hybrid.DATA_CONTRACT_VERSION,
        "source_sha256": {
            "v13_v3": EXPECTED_V13_V3_SOURCE_SHA256,
            "v13_v2": _sha256(Path(v13_v2.__file__).resolve()),
            "sweep": _sha256(Path(sweep.__file__).resolve()),
            "hybrid": _sha256(Path(hybrid.__file__).resolve()),
            "selector": _sha256(Path(replay.__file__).resolve()),
        },
    }
    key = common.canonical_json_sha256(payload)[:16]
    own_stem = CACHE_DIR / f"{month}_{key}"
    verified = None if rebuild else _load_verified_v5_cache(own_stem, payload)
    loaded = verified[0] if verified is not None else None
    cache_manifest = verified[1] if verified is not None else None
    source = "VERIFIED_V13_V5_CACHE" if loaded is not None else ""
    source_record: dict[str, Any] | None = None

    if loaded is None and not rebuild:
        seed = _load_verified_v3_seed(month, universe_path, days)
        if seed is not None:
            loaded, source_record = seed
            source = "CHECKSUM_VERIFIED_READ_ONLY_V13_V3_SEED"

    if loaded is None:
        mapped, _ = provenance.load_backtest_universe(
            universe_path=universe_path,
            contract_month_contains=month,
        )
        print(
            f"[V13-v5][BUILD] {month}: {len(mapped)} contracts/{len(days)} sessions",
            flush=True,
        )
        signals, paths = sweep.build_signal_table(
            set(days),
            square_off=OFFICIAL_CUTOFF,
            max_forward_bars=MAX_FORWARD_BARS,
            mapped_universe=mapped,
            confirmation_policy=sweep.CONFIRMATION_POLICY_V6_STRICT,
        )
        source = "V13_V5_RAW_BUILD"
    else:
        signals, paths = loaded
        print(f"[V13-v5][CACHE] {month}: {source}", flush=True)

    signals = signals.copy()
    signals["day"] = pd.to_datetime(signals["day"]).dt.date
    signals = signals.loc[signals["day"].isin(set(days))].copy()
    signals["contract_month"] = month
    kept_sids = set(signals["sid"].astype(int))
    paths = {int(sid): value for sid, value in paths.items() if int(sid) in kept_sids}
    paths = _trim_hlc_paths_to_cutoff(signals, paths)

    # Publish only inside the isolated V5 cache. Prior caches are never opened for write.
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    if source != "VERIFIED_V13_V5_CACHE":
        cache_manifest = _store_v5_cache(
            own_stem,
            signals,
            paths,
            payload=payload,
            source=source,
            source_record=source_record,
        )
    parquet_path, npz_path, manifest_path = _cache_files(own_stem)
    return signals, paths, {
        "month": month,
        "source": source,
        "v5_cache_stem": str(own_stem.resolve()),
        "v5_cache_parquet_sha256": _sha256(parquet_path),
        "v5_cache_npz_sha256": _sha256(npz_path),
        "v5_cache_manifest": str(manifest_path.resolve()),
        "v5_cache_manifest_sha256": _sha256(manifest_path),
        "rows": int(len(signals)),
        "payload": payload,
        "manifest": cache_manifest,
    }


def load_market(
    through_day: date,
    *,
    rebuild_cache: bool,
    refresh_eligibility: bool,
) -> tuple[
    pd.DataFrame,
    dict[int, dict[str, np.ndarray]],
    list[date],
    dict[str, date],
    list[dict[str, Any]],
    pd.DataFrame,
    pd.DataFrame,
]:
    eligibility, calendar, _, regimes, eligibility_source = v13_v3._load_eligibility(
        refresh_eligibility, MIN_CONTRACT_COVERAGE
    )
    eligibility = eligibility.copy()
    eligibility["seed_eligible"] = eligibility["eligible"].astype(bool)
    eligibility["coverage"] = pd.to_numeric(
        eligibility["coverage"], errors="coerce"
    ).fillna(0.0)
    eligibility["v13_v5_min_contract_coverage"] = MIN_CONTRACT_COVERAGE
    coverage_pass = eligibility["coverage"].ge(MIN_CONTRACT_COVERAGE)
    has_contract = eligibility["required_contract"].astype(str).isin(regimes)
    has_rows = pd.to_numeric(
        eligibility["contracts_with_data"], errors="coerce"
    ).fillna(0).gt(0)
    eligibility["eligible"] = coverage_pass & has_contract & has_rows
    eligibility["v13_v5_eligibility_reason"] = np.select(
        [~has_contract, ~has_rows, ~coverage_pass],
        [
            "REQUIRED_CONTRACT_NOT_CAPTURED",
            "NO_STORED_FUTURES_ROWS",
            "BELOW_V13_V5_COVERAGE_THRESHOLD",
        ],
        default="OK_REVALIDATED",
    )
    eligibility_source = (
        f"{eligibility_source}_REVALIDATED_AT_{MIN_CONTRACT_COVERAGE:.4f}"
    )
    eligibility_audit = eligibility.copy()
    eligible = eligibility.loc[
        eligibility["eligible"] & eligibility["day"].le(through_day)
    ].copy()
    by_month: dict[str, list[date]] = {}
    for row in eligible.to_dict("records"):
        month = str(row["required_contract"])
        if month in regimes:
            by_month.setdefault(month, []).append(row["day"])
    if not by_month:
        raise RuntimeError("No eligible point-in-time rolling-contract sessions.")
    parts = []
    cache_records = []
    for month in sorted(by_month, key=lambda value: calendar[value]):
        signals, paths, record = _load_signal_regime(
            month,
            regimes[month],
            sorted(by_month[month]),
            rebuild=rebuild_cache,
        )
        parts.append((signals, paths))
        cache_records.append(record)
    signals, cached_paths = v6.concat_regimes(parts)
    context = v13_v3.load_nifty_first_bar_context(signals["contract_month"].unique())
    annotated = v13_v3.annotate_nifty_gate(signals, context)
    gated = annotated.loc[annotated["nifty_first_bar_gate_pass"]].copy()
    gated = v13_v2.apply_policy(gated, v13_v2.POLICIES[BASE_POLICY_NAME])
    days = sorted(set(signals["day"]))
    for record in cache_records:
        record["eligibility_source"] = eligibility_source
    return gated, cached_paths, days, calendar, cache_records, annotated, eligibility_audit


def select_orders(signals: pd.DataFrame, setups: Iterable[Any]) -> pd.DataFrame:
    parts: list[pd.DataFrame] = []
    for setup in setups:
        selected = replay.select_setup_rows(signals, setup).copy()
        if selected.empty:
            continue
        selected["setup_id"] = setup.setup_id
        selected["native_stop_pct"] = setup.stop_pct
        selected["native_target_pct"] = setup.target_pct
        selected["configured_confirmation_end"] = setup.confirmation_end
        selected["picker"] = setup.picker
        selected["max_entries"] = setup.max_entries
        parts.append(selected)
    if not parts:
        return pd.DataFrame()
    return pd.concat(parts, ignore_index=True).sort_values(
        ["day", "hhmm_int", "side", "setup_id", "tradingsymbol"], kind="stable"
    ).reset_index(drop=True)


def _to_ist_timestamp(value: Any) -> pd.Timestamp:
    stamp = pd.Timestamp(value)
    if stamp.tzinfo is None:
        return stamp.tz_localize(common.IST)
    return stamp.tz_convert(common.IST)


def materialize_raw_paths(
    orders: pd.DataFrame,
    *,
    cutoff: str,
) -> tuple[dict[int, dict[str, np.ndarray]], pd.DataFrame]:
    """Rebuild selected paths with opens/timestamps and enforce exact cutoff."""

    result: dict[int, dict[str, np.ndarray]] = {}
    quality_rows: list[dict[str, Any]] = []
    for symbol, group in orders.groupby("tradingsymbol", sort=True):
        minute = hybrid.load_equity_one_minute(str(symbol))
        minute = minute.sort_values("ts").drop_duplicates("ts", keep="last").reset_index(drop=True)
        minute_ns = minute["ts"].astype("int64").to_numpy()
        for row in group.drop_duplicates("sid").itertuples(index=False):
            confirmation = _to_ist_timestamp(row.confirmation_ts)
            index = int(np.searchsorted(minute_ns, confirmation.value))
            exact_confirmation = bool(
                index < len(minute_ns) and minute_ns[index] == confirmation.value
            )
            if not exact_confirmation:
                raise RuntimeError(
                    f"Missing exact confirmation minute: {symbol} {confirmation}."
                )
            path = minute.iloc[index + 1 : index + 1 + MAX_FORWARD_BARS].copy()
            path = path.loc[
                path["ts"].dt.date.eq(confirmation.date())
                & path["ts"].dt.strftime("%H%M").le(cutoff)
            ].reset_index(drop=True)
            if path.empty:
                raise RuntimeError(f"Empty forward path: {symbol} {confirmation}.")
            expected_first = confirmation + pd.Timedelta(minutes=1)
            first_forward = bool(path.iloc[0]["ts"] == expected_first)
            terminal = str(path.iloc[-1]["ts"].strftime("%H%M"))
            exact_terminal = terminal == cutoff
            deltas = path["ts"].diff().dropna()
            continuous = bool(deltas.eq(pd.Timedelta(minutes=1)).all())
            quality_rows.append(
                {
                    "sid": int(row.sid),
                    "day": row.day,
                    "tradingsymbol": symbol,
                    "confirmation_ts": confirmation,
                    "path_rows": int(len(path)),
                    "first_forward_minute_present": first_forward,
                    "last_path_hhmm": terminal,
                    "exact_cutoff_present": exact_terminal,
                    "continuous_one_minute_path": continuous,
                }
            )
            if not first_forward or not exact_terminal or not continuous:
                raise RuntimeError(
                    f"Incomplete V13-v5 path: {symbol} {row.day}, "
                    f"terminal={terminal}, continuous={continuous}, "
                    f"first_forward={first_forward}."
                )
            result[int(row.sid)] = {
                "timestamp_ns": path["ts"].astype("int64").to_numpy(),
                "open": path["open"].to_numpy(float),
                "high": path["high"].to_numpy(float),
                "low": path["low"].to_numpy(float),
                "close": path["close"].to_numpy(float),
            }
    return result, pd.DataFrame(quality_rows)


def _entry(
    row: Any,
    path: dict[str, np.ndarray],
    *,
    delay_bars: int,
    trigger_buffer_pct: float,
    worse_fill_bps: float,
    max_entry_delay_minutes: int | None = None,
) -> tuple[int, float, float, bool, float] | None:
    is_long = row.side == "LONG"
    raw_trigger = float(row.trigger)
    trigger = raw_trigger * (
        1.0 + trigger_buffer_pct / 100.0
        if is_long
        else 1.0 - trigger_buffer_pct / 100.0
    )
    high = path["high"]
    low = path["low"]
    # path[0] is the bar immediately after the confirmation candle's close
    # (see materialize_raw_paths), so a hit at offset k is k+1 minutes after
    # confirmation. A hard window_end therefore caps entry delay in minutes.
    window_end = (
        delay_bars + max_entry_delay_minutes
        if max_entry_delay_minutes is not None
        else None
    )
    hits = np.flatnonzero(high[delay_bars:window_end] >= trigger) if is_long else np.flatnonzero(low[delay_bars:window_end] <= trigger)
    if not hits.size:
        return None
    entry_index = int(hits[0]) + delay_bars
    bar_open = float(path["open"][entry_index])
    gap_through = bool(bar_open > trigger if is_long else bar_open < trigger)
    entry = bar_open if gap_through else trigger
    entry *= 1.0 + worse_fill_bps / 10_000.0 if is_long else 1.0 - worse_fill_bps / 10_000.0
    overshoot_bps = (
        (entry / raw_trigger - 1.0) * 10_000.0
        if is_long
        else (raw_trigger / entry - 1.0) * 10_000.0
    )
    return entry_index, entry, trigger, gap_through, overshoot_bps


def _excursions(
    path: dict[str, np.ndarray], entry: float, start: int, end: int, is_long: bool
) -> tuple[float, float]:
    high = path["high"][start : end + 1]
    low = path["low"][start : end + 1]
    if is_long:
        return float((high.max() / entry - 1.0) * 100.0), float((low.min() / entry - 1.0) * 100.0)
    return float((1.0 - low.min() / entry) * 100.0), float((1.0 - high.max() / entry) * 100.0)


def _adverse_stop_fill(
    path: dict[str, np.ndarray],
    exit_index: int,
    stop_level: float,
    is_long: bool,
    *,
    activation_index: int,
) -> tuple[float, bool, float]:
    """Fill a stop at a worse later-bar open when price gaps through it.

    The activation bar's open precedes the intrabar trigger, so it cannot be
    used as a post-entry/post-T1 stop fill. That ambiguous bar stays at the
    stop level under the explicit pessimistic stop-first convention.
    """

    if exit_index <= activation_index:
        return stop_level, False, 0.0
    bar_open = float(path["open"][exit_index])
    adverse = bar_open < stop_level if is_long else bar_open > stop_level
    if not adverse:
        return stop_level, False, 0.0
    adverse_bps = abs(bar_open / stop_level - 1.0) * 10_000.0
    return bar_open, True, adverse_bps


def simulate_native(
    orders: pd.DataFrame,
    paths: dict[int, dict[str, np.ndarray]],
    *,
    cost_bps: float,
    delay_bars: int = 0,
    trigger_buffer_pct: float = 0.0,
    worse_fill_bps: float = 0.0,
    max_entry_delay_minutes: int | None = None,
) -> pd.DataFrame:
    # Default stays unlimited: this path exists only to reproduce V13-v3's
    # published figures exactly, and must not silently change under it.
    result = orders.copy()
    records: list[dict[str, Any]] = []
    missing = np.iinfo(np.int32).max
    for row in result.itertuples(index=False):
        path = paths[int(row.sid)]
        found = _entry(
            row,
            path,
            delay_bars=delay_bars,
            trigger_buffer_pct=trigger_buffer_pct,
            worse_fill_bps=worse_fill_bps,
            max_entry_delay_minutes=max_entry_delay_minutes,
        )
        if found is None:
            records.append({"filled": False, "exit_reason": "UNFILLED"})
            continue
        entry_index, entry, trigger, gap, overshoot = found
        is_long = row.side == "LONG"
        stop_pct = float(row.native_stop_pct)
        target_pct = float(row.native_target_pct)
        stop = entry * (1.0 - stop_pct / 100.0) if is_long else entry * (1.0 + stop_pct / 100.0)
        target = entry * (1.0 + target_pct / 100.0) if is_long else entry * (1.0 - target_pct / 100.0)
        stop_hits = np.flatnonzero(path["low"][entry_index:] <= stop) if is_long else np.flatnonzero(path["high"][entry_index:] >= stop)
        target_hits = np.flatnonzero(path["high"][entry_index:] >= target) if is_long else np.flatnonzero(path["low"][entry_index:] <= target)
        stop_i = int(stop_hits[0]) if stop_hits.size else missing
        target_i = int(target_hits[0]) if target_hits.size else missing
        ambiguous = stop_i == target_i and stop_i < missing
        exit_gap_through = False
        exit_gap_bps = 0.0
        if stop_i == target_i == missing:
            exit_index = len(path["close"]) - 1
            exit_price = float(path["close"][-1])
            reason = "TIME_EXIT_1515"
        elif stop_i <= target_i:
            exit_index = entry_index + stop_i
            exit_price, exit_gap_through, exit_gap_bps = _adverse_stop_fill(
                path,
                exit_index,
                stop,
                is_long,
                activation_index=entry_index,
            )
            reason = "STOP"
        else:
            exit_index = entry_index + target_i
            exit_price = target
            reason = "TARGET"
        gross = (exit_price / entry - 1.0) * 100.0 if is_long else (1.0 - exit_price / entry) * 100.0
        mfe, mae = _excursions(path, entry, entry_index, exit_index, is_long)
        entry_ts = pd.Timestamp(int(path["timestamp_ns"][entry_index]), tz="UTC").tz_convert(common.IST)
        exit_ts = pd.Timestamp(int(path["timestamp_ns"][exit_index]), tz="UTC").tz_convert(common.IST)
        records.append(
            {
                "filled": True,
                "trigger_used": trigger,
                "entry_price": entry,
                "entry_ts": entry_ts,
                "entry_path_index": entry_index,
                "entry_gap_through": gap,
                "entry_overshoot_bps": overshoot,
                "exit_gap_through": exit_gap_through,
                "exit_gap_bps": exit_gap_bps,
                "exit_price": exit_price,
                "exit_ts": exit_ts,
                "exit_path_index": exit_index,
                "holding_minutes": float((exit_ts - entry_ts).total_seconds() / 60.0),
                "gross_return_pct": gross,
                "cost_pct": cost_bps / 100.0,
                "net_return_pct": gross - cost_bps / 100.0,
                "exit_reason": reason,
                "target_hit": reason == "TARGET",
                "first_target_hit": reason == "TARGET",
                "runner_target_hit": reason == "TARGET",
                "stop_hit": reason == "STOP",
                "same_bar_ambiguous": ambiguous,
                "mfe_pct": mfe,
                "mae_pct": mae,
                "initial_stop_pct": stop_pct,
                "first_target_pct": target_pct,
                "partial_pct": 1.0,
                "runner_target_pct": target_pct,
                "runner_stop": "NONE",
            }
        )
    details = pd.DataFrame(records)
    for column in details.columns:
        result[column] = details[column].to_numpy()
    return result


def simulate_scaleout(
    orders: pd.DataFrame,
    paths: dict[int, dict[str, np.ndarray]],
    exit_spec: ExitSpec,
    *,
    cost_bps: float,
    delay_bars: int = 0,
    trigger_buffer_pct: float = 0.0,
    worse_fill_bps: float = 0.0,
    max_entry_delay_minutes: int | None = MAX_ENTRY_DELAY_MINUTES,
) -> pd.DataFrame:
    result = orders.copy()
    records: list[dict[str, Any]] = []
    missing = np.iinfo(np.int32).max
    for row in result.itertuples(index=False):
        path = paths[int(row.sid)]
        found = _entry(
            row,
            path,
            delay_bars=delay_bars,
            trigger_buffer_pct=trigger_buffer_pct,
            worse_fill_bps=worse_fill_bps,
            max_entry_delay_minutes=max_entry_delay_minutes,
        )
        if found is None:
            records.append({"filled": False, "exit_reason": "UNFILLED"})
            continue
        entry_index, entry, trigger, gap, overshoot = found
        is_long = row.side == "LONG"
        stop = entry * (1.0 - exit_spec.initial_stop_pct / 100.0) if is_long else entry * (1.0 + exit_spec.initial_stop_pct / 100.0)
        t1 = entry * (1.0 + exit_spec.first_target_pct / 100.0) if is_long else entry * (1.0 - exit_spec.first_target_pct / 100.0)
        runner_target = entry * (1.0 + exit_spec.runner_target_pct / 100.0) if is_long else entry * (1.0 - exit_spec.runner_target_pct / 100.0)
        end_limit = len(path["close"]) - 1
        if exit_spec.maximum_holding_minutes is not None:
            end_limit = min(end_limit, entry_index + exit_spec.maximum_holding_minutes)
        high = path["high"][: end_limit + 1]
        low = path["low"][: end_limit + 1]
        stop_hits = np.flatnonzero(low[entry_index:] <= stop) if is_long else np.flatnonzero(high[entry_index:] >= stop)
        t1_hits = np.flatnonzero(high[entry_index:] >= t1) if is_long else np.flatnonzero(low[entry_index:] <= t1)
        stop_i = int(stop_hits[0]) if stop_hits.size else missing
        t1_i = int(t1_hits[0]) if t1_hits.size else missing
        ambiguous = stop_i == t1_i and stop_i < missing
        first_target_hit = False
        runner_target_hit = False
        stop_hit = False
        exit_gap_through = False
        exit_gap_bps = 0.0
        if stop_i == t1_i == missing:
            exit_index = end_limit
            exit_price = float(path["close"][exit_index])
            runner_gross = (exit_price / entry - 1.0) * 100.0 if is_long else (1.0 - exit_price / entry) * 100.0
            gross = runner_gross
            reason = "TIME_EXIT_1515_NO_T1" if end_limit == len(path["close"]) - 1 else "MAX_HOLD_NO_T1"
        elif stop_i <= t1_i:
            exit_index = entry_index + stop_i
            exit_price, exit_gap_through, exit_gap_bps = _adverse_stop_fill(
                path,
                exit_index,
                stop,
                is_long,
                activation_index=entry_index,
            )
            gross = (
                (exit_price / entry - 1.0) * 100.0
                if is_long
                else (1.0 - exit_price / entry) * 100.0
            )
            reason = "FULL_STOP"
            stop_hit = True
        else:
            first_target_hit = True
            t1_abs = entry_index + t1_i
            runner_stop = entry
            runner_stop_hits = np.flatnonzero(low[t1_abs : end_limit + 1] <= runner_stop) if is_long else np.flatnonzero(high[t1_abs : end_limit + 1] >= runner_stop)
            runner_target_hits = np.flatnonzero(high[t1_abs : end_limit + 1] >= runner_target) if is_long else np.flatnonzero(low[t1_abs : end_limit + 1] <= runner_target)
            runner_stop_i = int(runner_stop_hits[0]) if runner_stop_hits.size else missing
            runner_target_i = int(runner_target_hits[0]) if runner_target_hits.size else missing
            ambiguous |= runner_stop_i == runner_target_i and runner_stop_i < missing
            if runner_stop_i == runner_target_i == missing:
                exit_index = end_limit
                exit_price = float(path["close"][exit_index])
                runner_gross = (exit_price / entry - 1.0) * 100.0 if is_long else (1.0 - exit_price / entry) * 100.0
                reason = "T1_THEN_TIME_EXIT_1515" if end_limit == len(path["close"]) - 1 else "T1_THEN_MAX_HOLD"
            elif runner_stop_i <= runner_target_i:
                exit_index = t1_abs + runner_stop_i
                exit_price, exit_gap_through, exit_gap_bps = _adverse_stop_fill(
                    path,
                    exit_index,
                    runner_stop,
                    is_long,
                    activation_index=t1_abs,
                )
                runner_gross = (
                    (exit_price / entry - 1.0) * 100.0
                    if is_long
                    else (1.0 - exit_price / entry) * 100.0
                )
                reason = "T1_THEN_BREAKEVEN"
            else:
                exit_index = t1_abs + runner_target_i
                exit_price = runner_target
                runner_gross = exit_spec.runner_target_pct
                reason = "RUNNER_TARGET"
                runner_target_hit = True
            gross = (
                exit_spec.partial_pct * exit_spec.first_target_pct
                + (1.0 - exit_spec.partial_pct) * runner_gross
            )
        mfe, mae = _excursions(path, entry, entry_index, exit_index, is_long)
        entry_ts = pd.Timestamp(int(path["timestamp_ns"][entry_index]), tz="UTC").tz_convert(common.IST)
        exit_ts = pd.Timestamp(int(path["timestamp_ns"][exit_index]), tz="UTC").tz_convert(common.IST)
        records.append(
            {
                "filled": True,
                "trigger_used": trigger,
                "entry_price": entry,
                "entry_ts": entry_ts,
                "entry_path_index": entry_index,
                "entry_gap_through": gap,
                "entry_overshoot_bps": overshoot,
                "exit_gap_through": exit_gap_through,
                "exit_gap_bps": exit_gap_bps,
                "exit_price": exit_price,
                "exit_ts": exit_ts,
                "exit_path_index": exit_index,
                "holding_minutes": float((exit_ts - entry_ts).total_seconds() / 60.0),
                "gross_return_pct": gross,
                "cost_pct": cost_bps / 100.0,
                "net_return_pct": gross - cost_bps / 100.0,
                "exit_reason": reason,
                "target_hit": first_target_hit,
                "first_target_hit": first_target_hit,
                "runner_target_hit": runner_target_hit,
                "stop_hit": stop_hit,
                "same_bar_ambiguous": ambiguous,
                "mfe_pct": mfe,
                "mae_pct": mae,
                **asdict(exit_spec),
            }
        )
    details = pd.DataFrame(records)
    for column in details.columns:
        result[column] = details[column].to_numpy()
    return result


def split_days(days: list[date]) -> dict[str, list[date]]:
    return {
        "TRAIN": [day for day in days if day <= TRAIN_END],
        "VALIDATION": [day for day in days if TRAIN_END < day <= VALIDATION_END],
        "PSEUDO_TEST": [day for day in days if day > VALIDATION_END],
        "ALL": list(days),
    }


def _maximum_streak(flags: Iterable[bool]) -> int:
    best = current = 0
    for flag in flags:
        current = current + 1 if bool(flag) else 0
        best = max(best, current)
    return best


def _drawdown_duration(values: np.ndarray) -> int:
    equity = np.r_[0.0, np.cumsum(values)]
    high = np.maximum.accumulate(equity)
    return _maximum_streak(equity[1:] < high[1:] - 1e-12)


def metrics(
    audit: pd.DataFrame,
    days: list[date],
    *,
    label: str,
) -> dict[str, Any]:
    subset = audit.loc[audit["day"].isin(days)].copy()
    filled = subset.loc[subset["filled"]].copy()
    net_return = pd.to_numeric(filled["net_return_pct"], errors="coerce")
    values = net_return.to_numpy(float)
    gross_return = pd.to_numeric(filled["gross_return_pct"], errors="coerce")
    gross = gross_return.to_numpy(float)
    capital = pd.to_numeric(
        filled.get(
            "capital_per_entry_rupees",
            pd.Series(DEFAULT_CAPITAL_PER_ENTRY_RUPEES, index=filled.index),
        ),
        errors="coerce",
    ).fillna(0.0)
    leverage = pd.to_numeric(
        filled.get(
            "leverage_factor",
            pd.Series(DEFAULT_LEVERAGE_FACTOR, index=filled.index),
        ),
        errors="coerce",
    ).fillna(DEFAULT_LEVERAGE_FACTOR)
    exposure = pd.to_numeric(
        filled.get("exposure_per_entry_rupees", capital * leverage),
        errors="coerce",
    ).fillna(0.0)
    unleveraged_pnl = net_return / 100.0 * capital
    pnl = net_return / 100.0 * exposure
    pnl_values = pnl.to_numpy(float)
    return_on_capital = net_return * leverage
    daily = (
        filled.groupby("day", sort=True)["net_return_pct"]
        .sum()
        .reindex(days, fill_value=0.0)
    )
    daily_values = daily.to_numpy(float)
    curve = np.r_[0.0, np.cumsum(daily_values)]
    drawdown = curve - np.maximum.accumulate(curve)
    wins = values > 1e-12
    losses = values < -1e-12
    breakeven = ~(wins | losses)
    average_win = float(values[wins].mean()) if wins.any() else np.nan
    average_loss = float(values[losses].mean()) if losses.any() else np.nan
    average_win_rupees = float(pnl_values[wins].mean()) if wins.any() else np.nan
    average_loss_rupees = float(pnl_values[losses].mean()) if losses.any() else np.nan
    positive_pnl = pnl_values[(pnl_values > 0) & np.isfinite(pnl_values)]
    negative_pnl = pnl_values[(pnl_values < 0) & np.isfinite(pnl_values)]
    daily_std = float(daily_values.std(ddof=1)) if len(daily_values) > 1 else np.nan
    sharpe = (
        float(daily_values.mean() / daily_std * np.sqrt(252.0))
        if daily_std and np.isfinite(daily_std)
        else np.nan
    )
    target_hits = int(filled.get("target_hit", False).astype(bool).sum())
    stop_hits = int(filled.get("stop_hit", False).astype(bool).sum())
    count = int(values.size)
    holding_values = pd.to_numeric(
        filled.get("holding_minutes", pd.Series(dtype=float)), errors="coerce"
    ).dropna()
    mfe_values = pd.to_numeric(
        filled.get("mfe_pct", pd.Series(dtype=float)), errors="coerce"
    ).dropna()
    mae_values = pd.to_numeric(
        filled.get("mae_pct", pd.Series(dtype=float)), errors="coerce"
    ).dropna()
    configured = (
        pd.to_numeric(audit["configured_setup_rules"], errors="coerce")
        .dropna()
        .unique()
        if "configured_setup_rules" in audit
        else np.array([], dtype=float)
    )
    if configured.size > 1:
        raise AssertionError("Audit mixes different configured setup-rule counts.")
    return {
        "label": label,
        "sessions": int(len(days)),
        "configured_setup_rules": int(configured[0]) if configured.size else np.nan,
        "selected_orders": int(len(subset)),
        "eligible_entries": int(len(subset)),
        "executed_trades": count,
        "average_trades_per_day": count / len(days) if days else np.nan,
        "wins": int(wins.sum()),
        "losses": int(losses.sum()),
        "breakeven": int(breakeven.sum()),
        "win_rate_pct": float(wins.mean() * 100.0) if count else np.nan,
        "target_hits": target_hits,
        "target_hit_rate_pct": target_hits / count * 100.0 if count else np.nan,
        "runner_target_hits": int(filled.get("runner_target_hit", False).astype(bool).sum()),
        "runner_target_hit_rate_pct": (
            float(filled.get("runner_target_hit", False).astype(bool).mean() * 100.0)
            if count
            else np.nan
        ),
        "stop_hits": stop_hits,
        "stop_hit_rate_pct": stop_hits / count * 100.0 if count else np.nan,
        "time_exits": int(filled["exit_reason"].astype(str).str.contains("TIME_EXIT|MAX_HOLD").sum()),
        "trailing_stop_exits": 0,
        "gross_profit_pct": float(values[values > 0].sum()) if count else 0.0,
        "gross_loss_pct": float(-values[values < 0].sum()) if count else 0.0,
        "gross_profit_definition": "SUM_POSITIVE_TRADE_RETURNS_AFTER_COSTS",
        "gross_loss_definition": "ABS_SUM_NEGATIVE_TRADE_RETURNS_AFTER_COSTS",
        "pre_cost_return_pct": float(gross.sum()) if count else 0.0,
        "total_cost_pct": float(filled["cost_pct"].sum()) if count else 0.0,
        "capital_model": "FIXED_CAPITAL_PER_FILLED_TRADE_WITH_LEVERAGE_NON_COMPOUNDED",
        "capital_per_entry_rupees": (
            float(capital.loc[capital > 0].iloc[0])
            if count and (capital > 0).any()
            else DEFAULT_CAPITAL_PER_ENTRY_RUPEES
        ),
        "leverage_factor": (
            float(leverage.loc[leverage > 0].iloc[0])
            if count and (leverage > 0).any()
            else DEFAULT_LEVERAGE_FACTOR
        ),
        "exposure_per_entry_rupees": (
            float(exposure.loc[exposure > 0].iloc[0])
            if count and (exposure > 0).any()
            else DEFAULT_CAPITAL_PER_ENTRY_RUPEES * DEFAULT_LEVERAGE_FACTOR
        ),
        "total_deployed_capital_rupees": float(capital.sum()) if count else 0.0,
        "total_exposure_rupees": float(exposure.sum()) if count else 0.0,
        "gross_profit_rupees": float(positive_pnl.sum()) if count else 0.0,
        "gross_loss_rupees": float(-negative_pnl.sum()) if count else 0.0,
        "pre_cost_profit_rupees": (
            float((gross_return / 100.0 * exposure).sum()) if count else 0.0
        ),
        "total_cost_rupees": (
            float((pd.to_numeric(filled["cost_pct"], errors="coerce") / 100.0 * exposure).sum())
            if count
            else 0.0
        ),
        "unleveraged_net_profit_rupees": float(unleveraged_pnl.sum()) if count else 0.0,
        "unleveraged_average_profit_per_trade_rupees": (
            float(unleveraged_pnl.mean()) if count else np.nan
        ),
        "net_profit": float(pnl.sum()) if count else 0.0,
        "net_profit_rupees": float(pnl.sum()) if count else 0.0,
        "net_profit_pct": float(values.sum()) if count else 0.0,
        "net_profit_on_capital_pct": float(return_on_capital.sum()) if count else 0.0,
        "profit_factor": _pf(values),
        "average_profit_per_trade_pct": float(values.mean()) if count else np.nan,
        "average_profit_per_trade_on_capital_pct": (
            float(return_on_capital.mean()) if count else np.nan
        ),
        "average_profit_per_trade_rupees": float(pnl.mean()) if count else np.nan,
        "average_winning_trade_pct": average_win,
        "average_winning_trade_rupees": average_win_rupees,
        "average_losing_trade_pct": average_loss,
        "average_losing_trade_rupees": average_loss_rupees,
        "payoff_ratio": average_win / abs(average_loss) if np.isfinite(average_loss) and average_loss else np.nan,
        "expectancy_pct": float(values.mean()) if count else np.nan,
        "expectancy_rupees": float(pnl.mean()) if count else np.nan,
        "maximum_drawdown_pct": float(drawdown.min()) if drawdown.size else 0.0,
        "drawdown_duration_sessions": _drawdown_duration(daily_values),
        "maximum_consecutive_wins": _maximum_streak(wins),
        "maximum_consecutive_losses": _maximum_streak(losses),
        "breakeven_stop_exits": int(
            filled["exit_reason"].astype(str).str.contains("BREAKEVEN").sum()
        ),
        "average_holding_minutes": float(holding_values.mean()) if len(holding_values) else np.nan,
        "median_holding_minutes": float(holding_values.median()) if len(holding_values) else np.nan,
        "average_mfe_pct": float(mfe_values.mean()) if len(mfe_values) else np.nan,
        "average_mae_pct": float(mae_values.mean()) if len(mae_values) else np.nan,
        "daily_sharpe_zero_rf": sharpe,
        "positive_days": int((daily_values > 0).sum()),
        "negative_days": int((daily_values < 0).sum()),
        "flat_days": int((daily_values == 0).sum()),
        "same_bar_ambiguous_trades": int(filled["same_bar_ambiguous"].astype(bool).sum()),
        "gap_through_fills": int(filled["entry_gap_through"].astype(bool).sum()),
        "gap_through_stop_exits": int(
            filled.get(
                "exit_gap_through", pd.Series(False, index=filled.index)
            ).eq(True).sum()
        ),
    }


def daily_frame(audit: pd.DataFrame, days: list[date], profile: str) -> pd.DataFrame:
    filled = audit.loc[audit["filled"]].copy()
    required_capital_columns = {
        "capital_per_entry_rupees",
        "leverage_factor",
        "exposure_per_entry_rupees",
        "net_return_on_capital_pct",
        "unleveraged_net_profit_rupees",
        "pre_cost_profit_rupees",
        "cost_rupees",
        "net_profit_rupees",
    }
    if not required_capital_columns.issubset(filled.columns):
        filled = apply_fixed_capital_model(
            filled,
            DEFAULT_CAPITAL_PER_ENTRY_RUPEES,
            DEFAULT_LEVERAGE_FACTOR,
        )
    grouped = filled.groupby("day", sort=True).agg(
        trades=("sid", "size"),
        wins=("net_return_pct", lambda x: int((x > 0).sum())),
        losses=("net_return_pct", lambda x: int((x < 0).sum())),
        target_hits=("target_hit", lambda x: int(pd.Series(x).astype(bool).sum())),
        stop_hits=("stop_hit", lambda x: int(pd.Series(x).astype(bool).sum())),
        net_return_pct=("net_return_pct", "sum"),
        net_return_on_capital_pct=("net_return_on_capital_pct", "sum"),
        capital_deployed_rupees=("capital_per_entry_rupees", "sum"),
        exposure_deployed_rupees=("exposure_per_entry_rupees", "sum"),
        unleveraged_net_profit_rupees=("unleveraged_net_profit_rupees", "sum"),
        pre_cost_profit_rupees=("pre_cost_profit_rupees", "sum"),
        cost_rupees=("cost_rupees", "sum"),
        net_profit_rupees=("net_profit_rupees", "sum"),
    )
    out = grouped.reindex(days, fill_value=0).reset_index().rename(columns={"index": "day"})
    out["average_profit_per_trade_rupees"] = np.where(
        out["trades"].gt(0),
        out["net_profit_rupees"] / out["trades"],
        np.nan,
    )
    out["average_profit_per_trade_on_capital_pct"] = np.where(
        out["trades"].gt(0),
        out["net_return_on_capital_pct"] / out["trades"],
        np.nan,
    )
    out["cumulative_net_return_pct"] = out["net_return_pct"].cumsum()
    out["cumulative_net_return_on_capital_pct"] = out[
        "net_return_on_capital_pct"
    ].cumsum()
    curve = out["cumulative_net_return_pct"].to_numpy(float)
    out["drawdown_pct"] = curve - np.maximum.accumulate(np.r_[0.0, curve])[1:]
    capital_curve = out["cumulative_net_return_on_capital_pct"].to_numpy(float)
    out["drawdown_on_capital_pct"] = (
        capital_curve - np.maximum.accumulate(np.r_[0.0, capital_curve])[1:]
    )
    out["cumulative_net_profit_rupees"] = out["net_profit_rupees"].cumsum()
    rupee_curve = out["cumulative_net_profit_rupees"].to_numpy(float)
    out["drawdown_rupees"] = (
        rupee_curve - np.maximum.accumulate(np.r_[0.0, rupee_curve])[1:]
    )
    out["period"] = np.select(
        [out["day"].le(TRAIN_END), out["day"].le(VALIDATION_END)],
        ["TRAIN", "VALIDATION"],
        default="PSEUDO_TEST",
    )
    out["profile"] = profile
    return out


def session_context(months: Iterable[str], cutoff: str) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for month in sorted(set(map(str, months))):
        path = v13_v3.NIFTY_ROOT / f"NIFTY{month}FUT_5minute.parquet"
        if not path.is_file():
            continue
        frame = pd.read_parquet(path, columns=["timestamp", "open", "high", "low", "close"])
        stamps = pd.to_datetime(frame["timestamp"], errors="coerce")
        if stamps.dt.tz is None:
            stamps = stamps.dt.tz_localize(common.IST)
        else:
            stamps = stamps.dt.tz_convert(common.IST)
        frame["ts"] = stamps
        frame["day"] = stamps.dt.date
        frame["hhmm"] = stamps.dt.strftime("%H%M")
        frame = frame.loc[frame["hhmm"].between("0920", cutoff)].copy()
        daily_records = []
        for day, group in frame.groupby("day", sort=True):
            group = group.sort_values("ts")
            day_open = float(group.iloc[0]["open"])
            day_close = float(group.iloc[-1]["close"])
            day_high = float(group["high"].max())
            day_low = float(group["low"].min())
            daily_records.append(
                {
                    "contract_month": month,
                    "day": day,
                    "nifty_open": day_open,
                    "nifty_close_1515": day_close,
                    "nifty_range_pct": (day_high / day_low - 1.0) * 100.0 if day_low else np.nan,
                    "nifty_day_return_pct": (day_close / day_open - 1.0) * 100.0 if day_open else np.nan,
                    "nifty_efficiency": abs(day_close - day_open) / (day_high - day_low) if day_high > day_low else 0.0,
                }
            )
        current = pd.DataFrame(daily_records)
        if current.empty:
            continue
        current["prior_close"] = current["nifty_close_1515"].shift(1)
        current["opening_gap_pct"] = (current["nifty_open"] / current["prior_close"] - 1.0) * 100.0
        rows.extend(current.to_dict("records"))
    return pd.DataFrame(rows)


def annotate_trade_context(
    audit: pd.DataFrame,
    calendar: dict[str, date],
    market_context: pd.DataFrame,
) -> pd.DataFrame:
    out = audit.copy()
    if not market_context.empty:
        out = out.merge(
            market_context,
            on=["contract_month", "day"],
            how="left",
            validate="many_to_one",
        )
    out["expiry_date"] = out["contract_month"].map(calendar)
    out["days_to_expiry"] = [
        (expiry - day).days if pd.notna(expiry) else np.nan
        for expiry, day in zip(out["expiry_date"], out["day"])
    ]
    out["expiry_status"] = np.where(out["days_to_expiry"].eq(0), "EXPIRY_DAY", "NON_EXPIRY_DAY")
    out["dte_bucket"] = pd.cut(
        pd.to_numeric(out["days_to_expiry"], errors="coerce"),
        bins=[-np.inf, 0, 3, 7, 14, np.inf],
        labels=["EXPIRED_OR_DAY0", "DTE_1_3", "DTE_4_7", "DTE_8_14", "DTE_15_PLUS"],
    ).astype(str)
    out["week"] = pd.to_datetime(out["day"]).dt.to_period("W").astype(str)
    out["month"] = pd.to_datetime(out["day"]).dt.to_period("M").astype(str)
    out["day_of_week"] = pd.to_datetime(out["day"]).dt.day_name()
    out["entry_time"] = pd.to_datetime(out["entry_ts"], errors="coerce").dt.strftime("%H:%M")
    out["exit_time"] = pd.to_datetime(out["exit_ts"], errors="coerce").dt.strftime("%H:%M")
    out["volatility_regime"] = pd.cut(
        pd.to_numeric(out.get("nifty_range_pct"), errors="coerce"),
        bins=[-np.inf, 0.75, 1.50, np.inf],
        labels=["LOW", "MEDIUM", "HIGH"],
    ).astype(str)
    out["trend_regime"] = np.where(
        pd.to_numeric(out.get("nifty_efficiency"), errors="coerce").ge(0.50),
        "TRENDING",
        "RANGING",
    )
    gap = pd.to_numeric(out.get("opening_gap_pct"), errors="coerce")
    out["opening_gap_condition"] = np.select(
        [gap.ge(0.25), gap.le(-0.25), gap.notna()],
        ["GAP_UP", "GAP_DOWN", "FLAT_OPEN"],
        default="UNKNOWN",
    )
    first = pd.to_numeric(out.get("nifty_first_bar_return_pct"), errors="coerce")
    aligned = (
        (out["side"].eq("LONG") & first.ge(0.05))
        | (out["side"].eq("SHORT") & first.le(-0.05))
    )
    opposed = (
        (out["side"].eq("LONG") & first.le(-0.05))
        | (out["side"].eq("SHORT") & first.ge(0.05))
    )
    out["nifty_alignment"] = np.select(
        [aligned, opposed, first.notna()], ["ALIGNED", "OPPOSED", "NEUTRAL"], default="MISSING"
    )
    out["instrument_kind"] = "NSE_CASH_EQUITY_WITH_FUTURES_OI"
    out["ce_pe"] = "NOT_APPLICABLE"
    out["strike_type"] = "NOT_APPLICABLE"
    out["stop_loss_type"] = "FIXED_PERCENT_FROM_ACTUAL_FILL"
    out["target_configuration"] = (
        "T1_" + out["first_target_pct"].astype(str)
        + "_PARTIAL_" + out["partial_pct"].astype(str)
        + "_RUNNER_" + out["runner_target_pct"].astype(str)
    )
    return out


def _group_metrics(frame: pd.DataFrame, dimension: str) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for value, group in frame.groupby(dimension, dropna=False, sort=True):
        filled = group.loc[group["filled"]]
        returns = filled["net_return_pct"].to_numpy(float)
        rows.append(
            {
                "dimension": dimension,
                "value": str(value),
                "orders": int(len(group)),
                "fills": int(len(filled)),
                "wins": int((returns > 0).sum()),
                "losses": int((returns < 0).sum()),
                "win_rate_pct": float((returns > 0).mean() * 100.0) if returns.size else np.nan,
                "target_hits": int(filled["target_hit"].astype(bool).sum()),
                "target_hit_rate_pct": float(filled["target_hit"].mean() * 100.0) if len(filled) else np.nan,
                "stop_hit_rate_pct": float(filled["stop_hit"].mean() * 100.0) if len(filled) else np.nan,
                "net_profit_pct": float(returns.sum()),
                "profit_factor": _pf(returns),
                "expectancy_pct": float(returns.mean()) if returns.size else np.nan,
                "average_mfe_pct": float(filled["mfe_pct"].mean()) if len(filled) else np.nan,
                "average_mae_pct": float(filled["mae_pct"].mean()) if len(filled) else np.nan,
                "average_holding_minutes": float(filled["holding_minutes"].mean()) if len(filled) else np.nan,
            }
        )
    return pd.DataFrame(rows)


def breakdowns(frame: pd.DataFrame) -> pd.DataFrame:
    dimensions = [
        "day", "week", "month", "day_of_week", "hhmm_int", "entry_time",
        "exit_time", "side", "ce_pe", "setup_id", "strike_type",
        "expiry_status", "dte_bucket", "volatility_regime", "trend_regime",
        "opening_gap_condition", "nifty_alignment", "stop_loss_type",
        "target_configuration", "exit_reason", "contract_month",
    ]
    return pd.concat([_group_metrics(frame, dimension) for dimension in dimensions], ignore_index=True)


def setup_summary(audit: pd.DataFrame, setups: Iterable[Any]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for setup in setups:
        group = audit.loc[audit["setup_id"].eq(setup.setup_id)]
        filled = group.loc[group["filled"]]
        values = filled["net_return_pct"].to_numpy(float)
        rows.append(
            {
                **asdict(setup),
                "orders": int(len(group)),
                "fills": int(len(filled)),
                "wins": int((values > 0).sum()),
                "losses": int((values < 0).sum()),
                "win_rate_pct": float((values > 0).mean() * 100.0) if values.size else np.nan,
                "target_hit_rate_pct": float(filled["target_hit"].mean() * 100.0) if len(filled) else np.nan,
                "profit_factor": _pf(values),
                "net_profit_pct": float(values.sum()),
                "expectancy_pct": float(values.mean()) if values.size else np.nan,
            }
        )
    return pd.DataFrame(rows)


def parameter_registry(
    profile_orders: dict[str, pd.DataFrame],
    *,
    cost_bps: float,
    capital_per_entry_rupees: float,
    leverage_factor: float,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []

    def add(
        name: str,
        file: str,
        owner: str,
        current: Any,
        default: Any,
        runtime: Any,
        purpose: str,
        valid_range: str,
        effect: str,
        affected: Any,
        status: str,
        experiment_range: str,
        env: str = "NONE",
    ) -> None:
        rows.append(
            {
                "parameter_or_condition": name,
                "file": file,
                "function_or_class": owner,
                "current_value": current,
                "default_value": default,
                "runtime_value": runtime,
                "environment_override": env,
                "purpose": purpose,
                "valid_range": valid_range,
                "current_effect": effect,
                "trades_affected": affected,
                "status": status,
                "recommended_experiment_range": experiment_range,
            }
        )

    all_orders = max((len(frame) for frame in profile_orders.values()), default=0)
    add(
        "default_profile", __file__, "DEFAULT_PROFILE", DEFAULT_PROFILE,
        DEFAULT_PROFILE, DEFAULT_PROFILE, "Select the no-argument V13-v5 profile",
        ",".join(PROFILES), "Higher-frequency is explicitly user-selected",
        len(profile_orders.get(DEFAULT_PROFILE, pd.DataFrame())),
        "ACTIVE_DEFAULT_PROFILE", "explicit --profile override",
    )
    add("price_change_pct_loose_floor", "fno_oi_ema_confirm_sweep.py", "LOOSE", 0.10, 0.10, 0.10, "Raw 5m candidate floor", ">=0", "Applied before cache persistence", "UNKNOWN_PRE_CONFIRM", "ACTIVE", "0.10-0.30")
    add("oi_change_pct_loose_floor", "fno_oi_ema_confirm_sweep.py", "LOOSE", 0.05, 0.05, 0.05, "Raw 5m OI floor", ">=0", "Also requires current OI > previous OI", "UNKNOWN_PRE_CONFIRM", "ACTIVE", "0.05-0.15")
    add("volume_ratio_loose_floor", "fno_oi_ema_confirm_sweep.py", "LOOSE", 0.80, 0.80, 0.80, "Raw 5m volume floor", ">=0", "Uses prior rolling-20 bars", "UNKNOWN_PRE_CONFIRM", "ACTIVE", "0.8-1.2")
    add("ema_spans", "fno_oi_hybrid_data.py", "add_equity_five_minute_features", "9,20,50", "9,20,50", "9,20,50", "Direction alignment", "positive integers", "Strict EMA stack by side", "ALL_CACHE", "ACTIVE", "FROZEN; avoid search on 25 sessions")
    add("volume_lookback", "fno_oi_hybrid_data.py", "add_equity_five_minute_features", 20, 20, 20, "Prior-volume normalization", ">=5 bars", "Shifted rolling mean across sessions", "ALL_CACHE", "ACTIVE", "time-of-day baseline needs new research")
    add("confirmation_policy", "fno_oi_ema_confirm_sweep.py", "build_signal_table", "v6_strict", "v6_strict", "v6_strict", "Exact S+1 directional candle", "v6_strict/v7_high_low_breakout", "Rejects non-directional confirmations", "5910_V7_VALID_CANDLES", "ACTIVE", "V7 tested and rejected")
    add("first_signal_slot", "fno_oi_ema_confirm_sweep.py", "build_signal_table", "09:25", "09:25", "09:25", "Avoid slot without prior 5m change", "09:20-15:00", "Cache begins at 09:25", "ALL_CACHE", "ACTIVE", "FROZEN")
    add("last_signal_slot", "fno_oi_ema_confirm_sweep.py", "build_signal_table", "15:00", "15:00", "15:00", "Raw scan endpoint", "09:25-15:00", "Later cells researched but not promoted", "ALL_CACHE", "ACTIVE", "all 5m slots audited")
    add("nifty_first_bar_gate", "fno_v13_corrected_v3_backtest.py", "annotate_nifty_gate", "09:25 SHORT <= -0.05%", "same", "same", "Causal index alignment", "return threshold", "Removed five losing V3-comparator trades", 5, "ACTIVE", "off,-0.025,-0.05,-0.10,-0.15")
    add("global_oi_cap_pct", "fno_v13_corrected_v2_backtest.py", "apply_policy", 1.00, 1.00, 1.00, "Exclude extreme OI jumps", ">=loose OI floor", "Applied before setup selection", "ALL_ORDERS", "ACTIVE", "0.75-1.25")
    add("five_minute_label", "fno_oi_hybrid_data.py", "aggregate_equity_one_minute_to_five_minute", "CANDLE_END", "CANDLE_END", "CANDLE_END", "Timestamp convention", "fixed", "09:25 is completed 09:21-09:25 bar", all_orders, "ACTIVE", "FROZEN")
    add("entry_first_eligible_minute", __file__, "materialize_raw_paths", "S+2", "S+2", "S+2", "Prevent setup/confirmation leakage", "after completed S+1", "09:25 setup ->09:26 confirm ->09:27 entry", all_orders, "ACTIVE", "FROZEN")
    add("minimum_futures_contract_coverage", __file__, "load_market", MIN_CONTRACT_COVERAGE, MIN_CONTRACT_COVERAGE, MIN_CONTRACT_COVERAGE, "Session-level mapped-futures file coverage", "0..1", "Revalidated even when V6 eligibility CSV is reused", "ALL_SESSIONS", "CORRECTNESS_FIX", "0.95-1.00")
    add("square_off_cutoff", __file__, "materialize_raw_paths", "15:15", "15:15", "15:15", "Uniform terminal bar", "<=15:15 for stored history", "Corrects V3's mixed 15:15/15:30 tail", all_orders, "CORRECTNESS_FIX", "15:30 only after backfill")
    add("gap_fill_policy", __file__, "_entry", "ACTUAL_OPEN_IF_GAPPED", "ACTUAL_OPEN_IF_GAPPED", "ACTUAL_OPEN_IF_GAPPED", "Avoid trigger-price optimism", "trigger or worse", "Three V3 fills re-priced; bracket returns unchanged", 3, "CORRECTNESS_FIX", "plus 2-10bp adverse stress")
    add("stop_gap_fill_policy", __file__, "_adverse_stop_fill", "ACTUAL_OPEN_IF_LATER_BAR_GAPS", "ACTUAL_OPEN_IF_LATER_BAR_GAPS", "ACTUAL_OPEN_IF_LATER_BAR_GAPS", "Avoid optimistic stop fills", "stop or worse", "Activation-bar open is excluded because it precedes the trigger", "AUDITED_PER_PROFILE", "CORRECTNESS_FIX", "FROZEN")
    add("same_bar_tie_policy", __file__, "simulate_scaleout", "STOP_FIRST", "STOP_FIRST", "STOP_FIRST", "Resolve unknowable intrabar order", "STOP_FIRST", "Pessimistic for stop/target and BE/runner ties", all_orders, "ACTIVE", "FROZEN")
    add("round_trip_cost_bps", __file__, "main", DEFAULT_COST_BPS, DEFAULT_COST_BPS, cost_bps, "Flat all-in cost proxy", ">=0", "Charged once on total initial notional", all_orders, "ACTIVE_CONFIGURABLE", "5,10,15,20,30")
    add("capital_per_entry_rupees", __file__, "apply_fixed_capital_model", DEFAULT_CAPITAL_PER_ENTRY_RUPEES, DEFAULT_CAPITAL_PER_ENTRY_RUPEES, capital_per_entry_rupees, "Fixed capital/margin base per filled trade", ">0", "P&L uses this capital multiplied by leverage factor; no compounding or reservation", all_orders, "REPORTING_ONLY", "use --capital-per-entry-rupees")
    add("leverage_factor", __file__, "apply_fixed_capital_model", DEFAULT_LEVERAGE_FACTOR, DEFAULT_LEVERAGE_FACTOR, leverage_factor, "Exposure multiplier for rupee P&L reporting", ">0", "5x means INR 100000 capital creates INR 500000 exposure per filled trade", all_orders, "REPORTING_ONLY", "use --leverage-factor")
    add("maximum_positions", __file__, "portfolio_model", "UNBOUNDED", "UNBOUNDED", "UNBOUNDED", "Portfolio capacity", "N/A", "No portfolio capital constraint; repeated/overlapping symbols allowed", all_orders, "MISSING_NOT_SILENT", "requires lot/capital model")
    add("reentry_cooldown", __file__, "selection", "NONE", "NONE", "NONE", "Re-entry control", "N/A", "No cooldown or direction lock", all_orders, "INACTIVE", "future portfolio research")
    add("option_strike_and_premium", __file__, "data_contract", "NOT_APPLICABLE", "NOT_APPLICABLE", "NOT_APPLICABLE", "Option execution", "N/A", "No options data are traded", 0, "ABSENT", "requires new options dataset")
    add("equity_1m_root", "fno_oi_hybrid_data.py", "DEFAULT_BACKTEST_EQUITY_1M_DIR", str(hybrid.DEFAULT_BACKTEST_EQUITY_1M_DIR), str(hybrid.DEFAULT_BACKTEST_EQUITY_1M_DIR), str(hybrid.DEFAULT_BACKTEST_EQUITY_1M_DIR), "Raw price path", "existing directory", "Can change entire sample if overridden", all_orders, "ACTIVE", "FROZEN_SNAPSHOT_RECOMMENDED", "EQIDV2_FNO_V5_BACKTEST_EQUITY_1M_DIR")

    for profile_name, profile in PROFILES.items():
        count = len(profile_orders.get(profile_name, pd.DataFrame()))
        profile_status = (
            "ACTIVE_DEFAULT_PROFILE"
            if profile_name == DEFAULT_PROFILE
            else "ACTIVE_COMPARATOR_PROFILE"
        )
        for field, value in asdict(profile.exit).items():
            add(
                f"{profile_name}.exit.{field}", __file__, "ExitSpec", value, value, value,
                f"{profile_name} exit configuration", "non-negative; partial in (0,1)",
                profile.description, count, profile_status, "nearby values in sensitivity ledger"
            )
        add(f"{profile_name}.add_0950_short", __file__, "ProfileSpec", profile.add_0950_short, profile.add_0950_short, profile.add_0950_short, "Missing-slot frequency probe", "boolean", profile.evidence, count, "ACTIVE_PROFILE" if profile.add_0950_short else "INACTIVE_PROFILE", "boolean")
        add(f"{profile_name}.add_1120_short", __file__, "ProfileSpec", profile.add_1120_short, profile.add_1120_short, profile.add_1120_short, "Late SHORT frequency probe", "boolean", profile.evidence, count, "ACTIVE_PROFILE" if profile.add_1120_short else "INACTIVE_PROFILE", "boolean")
        add(f"{profile_name}.excluded_setup_ids", __file__, "ProfileSpec", ",".join(profile.excluded_setup_ids) or "NONE", "NONE", ",".join(profile.excluded_setup_ids) or "NONE", "Development ablation", "known setup IDs", profile.description, count, "ACTIVE_PROFILE", "one-cell ablations only")

    for setup in v13_v3.active_setups() + (added_short_setup("09:50"), added_short_setup("11:20")):
        setup_orders = sum(
            int(frame["setup_id"].eq(setup.setup_id).sum())
            for frame in profile_orders.values()
        )
        for field in (
            "signal_end", "confirmation_end", "side", "mode", "max_entries", "picker",
            "price_change_pct", "oi_change_pct", "volume_ratio", "body_ratio",
            "max_wick_ratio", "min_traded_value", "stop_pct", "target_pct",
        ):
            value = getattr(setup, field)
            add(
                f"setup.{setup.setup_id}.{field}",
                "fno_v13_corrected_v3_backtest.py" if setup.signal_end not in {"09:50", "11:20"} else __file__,
                "SetupSpec", value, value, value, "Setup selection/ranking field",
                "type-appropriate", f"Applies only to {setup.signal_end} {setup.side}",
                setup_orders, "ACTIVE_OR_PROFILE_SPECIFIC", "see experiment ledger",
            )
    return pd.DataFrame(rows)


def cost_and_execution_stress(
    orders: pd.DataFrame,
    paths: dict[int, dict[str, np.ndarray]],
    exit_spec: ExitSpec,
    days: list[date],
    base_audit: pd.DataFrame,
    *,
    base_cost_bps: float,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for cost in (0.0, 5.0, 10.0, 15.0, 20.0, 30.0):
        audit = simulate_scaleout(orders, paths, exit_spec, cost_bps=cost)
        rows.append({"stress": f"COST_{cost:.0f}BPS", **metrics(audit, days, label="stress")})
    for delay in (1,):
        audit = simulate_scaleout(
            orders, paths, exit_spec, cost_bps=base_cost_bps, delay_bars=delay
        )
        rows.append({"stress": f"ENTRY_DELAY_{delay}M", **metrics(audit, days, label="stress")})
    for slip in (2.0, 5.0, 10.0):
        audit = simulate_scaleout(
            orders, paths, exit_spec, cost_bps=base_cost_bps, worse_fill_bps=slip
        )
        rows.append({"stress": f"WORSE_FILL_{slip:.0f}BPS", **metrics(audit, days, label="stress")})
    filled = base_audit.loc[base_audit["filled"]].copy()
    for remove in (1, 3, 5):
        drop_index = filled.nlargest(remove, "net_return_pct").index
        reduced = base_audit.drop(index=drop_index)
        rows.append({"stress": f"REMOVE_BEST_{remove}_TRADES", **metrics(reduced, days, label="stress")})
    return pd.DataFrame(rows)


def parameter_sensitivity(
    orders: pd.DataFrame,
    paths: dict[int, dict[str, np.ndarray]],
    profile: ProfileSpec,
    days: list[date],
    *,
    cost_bps: float,
) -> pd.DataFrame:
    base = profile.exit
    candidates: list[tuple[str, ExitSpec]] = [("BASE", base)]
    for value in (base.first_target_pct - 0.05, base.first_target_pct + 0.05):
        candidates.append((f"T1_{value:.2f}", replace(base, first_target_pct=value)))
    for value in (1.40, 1.60):
        candidates.append((f"STOP_{value:.2f}", replace(base, initial_stop_pct=value)))
    for value in (2.50, 2.70):
        candidates.append((f"RUNNER_{value:.2f}", replace(base, runner_target_pct=value)))
    partial_neighbors = sorted(
        {max(0.05, base.partial_pct - 0.05), min(0.50, base.partial_pct + 0.05)}
    )
    for value in partial_neighbors:
        candidates.append((f"PARTIAL_{value:.2f}", replace(base, partial_pct=value)))
    rows = []
    periods = split_days(days)
    for experiment, exit_spec in candidates:
        audit = simulate_scaleout(orders, paths, exit_spec, cost_bps=cost_bps)
        row = {"experiment": experiment, **asdict(exit_spec), **metrics(audit, days, label="ALL")}
        for name in ("TRAIN", "VALIDATION", "PSEUDO_TEST"):
            current = metrics(audit, periods[name], label=name)
            for key in ("executed_trades", "win_rate_pct", "target_hit_rate_pct", "profit_factor", "net_profit_pct", "expectancy_pct", "maximum_drawdown_pct"):
                row[f"{name.lower()}_{key}"] = current[key]
        rows.append(row)
    return pd.DataFrame(rows)


def walk_forward_frame(audit: pd.DataFrame, days: list[date]) -> pd.DataFrame:
    rows = []
    for fold, start in enumerate((10, 15, 20), start=1):
        test_days = days[start : min(start + 5, len(days))]
        if not test_days:
            continue
        current = metrics(audit, test_days, label=f"WF{fold}")
        rows.append(
            {
                "fold": fold,
                "prior_sessions": start,
                "test_start": test_days[0],
                "test_end": test_days[-1],
                **current,
                "note": "Frozen-configuration expanding-window replay; no refit performed",
            }
        )
    return pd.DataFrame(rows)


def bootstrap_summary(audit: pd.DataFrame, days: list[date], draws: int = 10_000) -> pd.DataFrame:
    daily = (
        audit.loc[audit["filled"]]
        .groupby("day")["net_return_pct"]
        .sum()
        .reindex(days, fill_value=0.0)
        .to_numpy(float)
    )
    rng = np.random.default_rng(BOOTSTRAP_SEED)
    sampled = rng.choice(daily, size=(draws, len(daily)), replace=True).sum(axis=1)
    return pd.DataFrame(
        [
            {
                "seed": BOOTSTRAP_SEED,
                "draws": draws,
                "unit": "SESSION",
                "p02_5_net_pct": float(np.quantile(sampled, 0.025)),
                "p50_net_pct": float(np.quantile(sampled, 0.50)),
                "p97_5_net_pct": float(np.quantile(sampled, 0.975)),
                "probability_net_le_zero": float((sampled <= 0).mean()),
                "warning": "Descriptive resampling of the same 25 inspected sessions; not future proof",
            }
        ]
    )


def published_v3_overlay(
    corrected: pd.DataFrame,
    through_day: date,
    cost_bps: float,
    capital_per_entry_rupees: float,
    leverage_factor: float,
) -> pd.DataFrame | None:
    """Attach published V3 returns to the corrected audit for metric parity.

    This is used only to report the unchanged official baseline. Its timestamps,
    MAE and MFE remain the corrected 15:15 diagnostics and are not represented
    as original V3 execution fields.
    """

    if not v13_v3.TRADES_PATH.is_file():
        return None
    published = pd.read_csv(v13_v3.TRADES_PATH)
    published_cost_bps = DEFAULT_COST_BPS
    if v13_v3.PROVENANCE_PATH.is_file():
        try:
            published_provenance = json.loads(
                v13_v3.PROVENANCE_PATH.read_text(encoding="utf-8")
            )
            published_cost_bps = float(
                published_provenance.get("parameters", {}).get(
                    "cost_bps", DEFAULT_COST_BPS
                )
            )
        except (OSError, ValueError, TypeError, json.JSONDecodeError):
            return None
    published["day"] = pd.to_datetime(published["day"]).dt.date
    published = published.loc[published["day"].le(through_day)].copy()
    keys = ["sid", "day", "tradingsymbol", "side", "setup_id"]
    left = corrected[keys].reset_index(drop=True)
    right = published[keys].reset_index(drop=True)
    if len(left) != len(right) or not left.equals(right):
        return None
    out = corrected.copy()
    out["filled"] = published["filled"].astype(str).str.lower().eq("true").to_numpy()
    published_net = pd.to_numeric(
        published["net_return_pct"], errors="coerce"
    ).to_numpy(float)
    out["gross_return_pct"] = np.where(
        out["filled"], published_net + published_cost_bps / 100.0, np.nan
    )
    out["cost_pct"] = np.where(out["filled"], cost_bps / 100.0, np.nan)
    out["net_return_pct"] = out["gross_return_pct"] - out["cost_pct"]
    gross = pd.to_numeric(out["gross_return_pct"], errors="coerce").to_numpy(float)
    target = pd.to_numeric(out["native_target_pct"], errors="coerce").to_numpy(float)
    stop = pd.to_numeric(out["native_stop_pct"], errors="coerce").to_numpy(float)
    target_hit = out["filled"].to_numpy(bool) & np.isclose(gross, target, atol=1e-8)
    stop_hit = out["filled"].to_numpy(bool) & np.isclose(gross, -stop, atol=1e-8)
    out["target_hit"] = target_hit
    out["first_target_hit"] = target_hit
    out["runner_target_hit"] = target_hit
    out["stop_hit"] = stop_hit
    out["exit_reason"] = np.select(
        [~out["filled"].to_numpy(bool), target_hit, stop_hit],
        ["UNFILLED", "TARGET", "STOP"],
        default="TIME_EXIT_V3_PUBLISHED",
    )
    out["baseline_note"] = (
        "PUBLISHED_V3_GROSS_RETURN_WITH_VARIABLE_1515_1530_TAIL;_"
        f"PUBLISHED_COST_{published_cost_bps:g}BPS;_RESTRESSED_COST_{cost_bps:g}BPS;_"
        "HOLDING_AND_EXCURSION_FIELDS_ARE_CORRECTED_PATH_DIAGNOSTICS"
    )
    # V3 did not publish these fields. Do not present reconstructed V5-path
    # diagnostics as official V3 observations in the headline comparison.
    out[["holding_minutes", "mfe_pct", "mae_pct"]] = np.nan
    return apply_fixed_capital_model(out, capital_per_entry_rupees, leverage_factor)


def write_profile_outputs(
    profile: ProfileSpec,
    audit: pd.DataFrame,
    setups: tuple[Any, ...],
    days: list[date],
    paths: dict[int, dict[str, np.ndarray]],
    *,
    cost_bps: float,
) -> dict[str, str]:
    directory = RESULT_DIR / profile.name
    directory.mkdir(parents=True, exist_ok=True)
    daily = daily_frame(audit, days, profile.name)
    setup = setup_summary(audit, setups)
    detail = breakdowns(audit)
    stress = cost_and_execution_stress(
        audit[
            [column for column in audit.columns if column not in {
                "filled", "trigger_used", "entry_price", "entry_ts", "entry_path_index",
                "entry_gap_through", "entry_overshoot_bps", "exit_price", "exit_ts",
                "exit_gap_through", "exit_gap_bps",
                "exit_path_index", "holding_minutes", "gross_return_pct", "cost_pct",
                "net_return_pct", "exit_reason", "target_hit", "first_target_hit",
                "runner_target_hit", "stop_hit", "same_bar_ambiguous", "mfe_pct", "mae_pct",
                "initial_stop_pct", "first_target_pct", "partial_pct", "runner_target_pct",
                "runner_stop", "maximum_holding_minutes", "expiry_date", "days_to_expiry",
                "expiry_status", "dte_bucket", "week", "month", "day_of_week", "entry_time",
                "exit_time", "volatility_regime", "trend_regime", "opening_gap_condition",
                "nifty_alignment", "instrument_kind", "ce_pe", "strike_type",
                "stop_loss_type", "target_configuration", "nifty_open", "nifty_close_1515",
                "nifty_range_pct", "nifty_day_return_pct", "nifty_efficiency", "prior_close",
                "opening_gap_pct", "capital_model", "capital_per_entry_rupees",
                "leverage_factor", "exposure_per_entry_rupees",
                "gross_return_on_capital_pct", "net_return_on_capital_pct",
                "unleveraged_pre_cost_profit_rupees", "unleveraged_cost_rupees",
                "unleveraged_net_profit_rupees", "pre_cost_profit_rupees",
                "cost_rupees", "net_profit_rupees",
            }]
        ].copy(),
        paths,
        profile.exit,
        days,
        audit,
        base_cost_bps=cost_bps,
    )
    sensitivity = parameter_sensitivity(
        audit[
            [column for column in audit.columns if column not in {
                "filled", "trigger_used", "entry_price", "entry_ts", "entry_path_index",
                "entry_gap_through", "entry_overshoot_bps", "exit_price", "exit_ts",
                "exit_gap_through", "exit_gap_bps",
                "exit_path_index", "holding_minutes", "gross_return_pct", "cost_pct",
                "net_return_pct", "exit_reason", "target_hit", "first_target_hit",
                "runner_target_hit", "stop_hit", "same_bar_ambiguous", "mfe_pct", "mae_pct",
                "initial_stop_pct", "first_target_pct", "partial_pct", "runner_target_pct",
                "runner_stop", "maximum_holding_minutes", "expiry_date", "days_to_expiry",
                "expiry_status", "dte_bucket", "week", "month", "day_of_week", "entry_time",
                "exit_time", "volatility_regime", "trend_regime", "opening_gap_condition",
                "nifty_alignment", "instrument_kind", "ce_pe", "strike_type",
                "stop_loss_type", "target_configuration", "nifty_open", "nifty_close_1515",
                "nifty_range_pct", "nifty_day_return_pct", "nifty_efficiency", "prior_close",
                "opening_gap_pct", "capital_model", "capital_per_entry_rupees",
                "leverage_factor", "exposure_per_entry_rupees",
                "gross_return_on_capital_pct", "net_return_on_capital_pct",
                "unleveraged_pre_cost_profit_rupees", "unleveraged_cost_rupees",
                "unleveraged_net_profit_rupees", "pre_cost_profit_rupees",
                "cost_rupees", "net_profit_rupees",
            }]
        ].copy(),
        paths,
        profile,
        days,
        cost_bps=cost_bps,
    )
    walk = walk_forward_frame(audit, days)
    bootstrap = bootstrap_summary(audit, days)
    outputs = {
        "trades": directory / f"fno_v13_corrected_v5_{profile.name}_trades.csv",
        "daily": directory / f"fno_v13_corrected_v5_{profile.name}_daily.csv",
        "setups": directory / f"fno_v13_corrected_v5_{profile.name}_setups.csv",
        "breakdowns": directory / f"fno_v13_corrected_v5_{profile.name}_breakdowns.csv",
        "stress": directory / f"fno_v13_corrected_v5_{profile.name}_stress.csv",
        "sensitivity": directory / f"fno_v13_corrected_v5_{profile.name}_sensitivity.csv",
        "walk_forward": directory / f"fno_v13_corrected_v5_{profile.name}_walk_forward.csv",
        "bootstrap": directory / f"fno_v13_corrected_v5_{profile.name}_bootstrap.csv",
    }
    frames = {
        "trades": audit,
        "daily": daily,
        "setups": setup,
        "breakdowns": detail,
        "stress": stress,
        "sensitivity": sensitivity,
        "walk_forward": walk,
        "bootstrap": bootstrap,
    }
    for name, path in outputs.items():
        common.atomic_write_csv(frames[name], path)
    return {name: str(path.resolve()) for name, path in outputs.items()}


def render_report(
    *,
    days: list[date],
    cost_bps: float,
    capital_per_entry_rupees: float,
    leverage_factor: float,
    comparison: pd.DataFrame,
    periods: pd.DataFrame,
    profile_audits: dict[str, pd.DataFrame],
    stresses: dict[str, pd.DataFrame],
    output_map: dict[str, dict[str, str]],
    cache_records: list[dict[str, Any]],
    quality: pd.DataFrame,
    v3_hash_before: str,
    v3_hash_after: str,
) -> str:
    headline_columns = [
        "label",
        "configured_default",
        "configured_setup_rules",
        "selected_orders",
        "eligible_entries",
        "executed_trades",
        "average_trades_per_day",
        "wins",
        "losses",
        "breakeven",
        "win_rate_pct",
        "target_hit_rate_pct",
        "runner_target_hit_rate_pct",
        "stop_hit_rate_pct",
        "breakeven_stop_exits",
        "time_exits",
        "trailing_stop_exits",
        "profit_factor",
        "net_profit_pct",
        "net_profit_on_capital_pct",
        "net_profit_rupees",
        "average_profit_per_trade_rupees",
        "maximum_drawdown_pct",
    ]
    return_columns = [
        "label",
        "pre_cost_return_pct",
        "total_cost_pct",
        "gross_profit_pct",
        "gross_loss_pct",
        "expectancy_pct",
        "average_winning_trade_pct",
        "average_losing_trade_pct",
        "payoff_ratio",
        "drawdown_duration_sessions",
        "maximum_consecutive_wins",
        "maximum_consecutive_losses",
        "average_holding_minutes",
        "daily_sharpe_zero_rf",
    ]
    capital_columns = [
        "label",
        "executed_trades",
        "capital_per_entry_rupees",
        "leverage_factor",
        "exposure_per_entry_rupees",
        "total_deployed_capital_rupees",
        "total_exposure_rupees",
        "net_profit_pct",
        "net_profit_on_capital_pct",
        "unleveraged_net_profit_rupees",
        "net_profit_rupees",
        "average_profit_per_trade_pct",
        "average_profit_per_trade_on_capital_pct",
        "average_profit_per_trade_rupees",
        "gross_profit_rupees",
        "gross_loss_rupees",
        "profit_factor",
    ]
    period_columns = [
        "strategy",
        "configured_default",
        "period",
        "sessions",
        "executed_trades",
        "win_rate_pct",
        "target_hit_rate_pct",
        "profit_factor",
        "net_profit_pct",
        "net_profit_on_capital_pct",
        "net_profit_rupees",
        "expectancy_pct",
        "average_profit_per_trade_on_capital_pct",
        "average_profit_per_trade_rupees",
        "maximum_drawdown_pct",
    ]
    primary_audit = profile_audits[DEFAULT_PROFILE]
    primary_daily = daily_frame(primary_audit, days, DEFAULT_PROFILE)
    primary_gap_exits = int(
        primary_audit["exit_gap_through"].eq(True).sum()
    )
    return_table = comparison[return_columns].copy()
    return_table["average_holding_minutes"] = return_table[
        "average_holding_minutes"
    ].map(lambda value: "N/A" if pd.isna(value) else f"{float(value):.3f}")
    lines = [
        "# FNO V13 corrected v5 — rigorous research results",
        "",
        "## Verdict",
        "",
        (
            "**No configuration is promoted or proven for production.** The available "
            f"history is only {len(days)} sessions ({days[0]} through {days[-1]}), and "
            "all of it had already been inspected during earlier V13 research. The final "
            "six sessions are therefore a `PSEUDO_TEST`, not an untouched holdout."
        ),
        "",
        (
            "The higher-frequency profile is the explicitly configured V13-v5 default. It "
            "has the strongest aggregate numbers, but its extra components are sparse and "
            "its training net edge disappears above 5 bps. It remains a user-selected "
            "forward-shadow configuration, not evidence of future profitability. The runner "
            "still regenerates all three profiles so every default run retains its audit "
            "comparators."
        ),
        "",
        f"All headline rows below use the same {cost_bps:.1f} bps round-trip proxy. "
        "The official V3 return, win, target and stop statistics retain V3's published "
        "mixed terminal-path behavior; all other rows use the corrected, uniform 15:15 "
        "execution engine. V3 holding-time and excursion diagnostics are unavailable in "
        "its published ledger and are shown as N/A; auxiliary path-quality fields in the "
        "machine-readable comparison are corrected-path diagnostics, not official V3 "
        "measurements.",
        "",
        "## Headline comparison",
        "",
        comparison[headline_columns].to_markdown(index=False, floatfmt=".3f"),
        "",
        return_table.to_markdown(index=False, floatfmt=".3f"),
        "",
        "## Fixed-capital leveraged P&L",
        "",
        comparison[capital_columns].to_markdown(index=False, floatfmt=".3f"),
        "",
        f"Rupee P&L uses INR {capital_per_entry_rupees:,.0f} fresh capital per filled "
        f"trade with {leverage_factor:.2f}x leverage, so exposure per filled trade is "
        f"INR {capital_per_entry_rupees * leverage_factor:,.0f}. "
        "`net_profit_pct` remains the unleveraged, non-compounded sum of per-trade "
        "price-return points. `net_profit_on_capital_pct` is the leveraged return "
        "against the capital base. This is still a reporting model only: no compounding, "
        "portfolio reservation, broker margin check, lot sizing, futures margin or "
        "options premium is simulated here. "
        "`gross_profit_pct` and `gross_loss_pct` are respectively the positive and absolute "
        "negative trade-return sums after the cost proxy; PF uses those two values.",
        "",
        "For V5, a target hit means T1 was touched; only 10% (balanced/high-frequency) "
        "or 20% (conservative) is booked there. `runner_target_hit_rate_pct` is the share "
        "that reached +2.60% on the remaining position. V3's target is a full-position "
        "native target, so its target-hit percentage is not directly comparable.",
        "",
        "## Chronological stability",
        "",
        periods[period_columns].to_markdown(index=False, floatfmt=".3f"),
        "",
        "TRAIN: 2026-07-29…2026-08-13 (12 sessions). VALIDATION: "
        "2026-08-14…2026-08-26 (7). PSEUDO_TEST: 2026-08-27…2026-09-03 (6). "
        "Profile and exit selection used TRAIN+VALIDATION; the caveat remains that earlier "
        "V13 work had already exposed every date.",
        "",
        "Balanced does not beat the corrected V3 net in every segment: TRAIN is "
        "+26.961% versus +28.124%, and PSEUDO_TEST is +13.940% versus +14.611%; "
        "its gain is concentrated in VALIDATION. Higher-frequency improves aggregate "
        "TRAIN/VALIDATION/PSEUDO_TEST net at 5 bps, but its TRAIN edge is only about "
        "+0.103 percentage point after the corrected stop-gap replay and reverses under "
        "higher cost. Hence no all-condition robust improvement is claimed.",
        "",
        "## Frozen V13-v5 profiles",
        "",
        "Profile | Entry book | Exit | Status",
        "--- | --- | --- | ---",
    ]
    for profile in PROFILES.values():
        changes: list[str] = []
        if profile.add_0950_short:
            changes.append("+09:50 SHORT")
        if profile.add_1120_short:
            changes.append("+11:20 SHORT")
        if profile.wick_cap_delta:
            changes.append(f"wick cap +{profile.wick_cap_delta:.2f}")
        if profile.excluded_setup_ids:
            changes.append("exclude " + ",".join(profile.excluded_setup_ids))
        entry_book = "V3" + ("; " + "; ".join(changes) if changes else "")
        hold = (
            f"; max hold {profile.exit.maximum_holding_minutes}m"
            if profile.exit.maximum_holding_minutes is not None
            else "; 15:15 EOD"
        )
        exit_text = (
            f"SL {profile.exit.initial_stop_pct:.2f}%; book "
            f"{profile.exit.partial_pct * 100:.0f}% @ {profile.exit.first_target_pct:.3f}%; "
            f"runner BE / {profile.exit.runner_target_pct:.2f}%{hold}"
        )
        lines.append(f"{profile.name} | {entry_book} | {exit_text} | {profile.evidence}")
    lines.extend(
        [
            "",
            "Higher-frequency is the configured default at the user's direction, while "
            "remaining an **experimental research shadow**, not a production recommendation. "
            "Balanced remains the parsimonious development-selected alternative. Conservative "
            "intentionally sacrifices summed return for a shorter 180-minute exposure cap and "
            "lower observed drawdown.",
            "",
            "## Correctness defects found and V5 treatment",
            "",
            "- **Mixed terminal data:** 69/79 V3 selected orders have paths ending at "
            "15:15, while ten paths from the first three dates reach 15:30; 25/31 V3 time "
            "exits consequently use 15:15 despite a configured 15:30. V5 enforces one exact "
            "15:15 cutoff and fails closed on a missing terminal minute or any internal gap.",
            "- **Entry gaps:** V3 fills a touched stop-entry at the trigger. V5 fills at the "
            "adverse minute open when it has already crossed the trigger, then rebases every "
            "stop and target. Three baseline fills are affected.",
            "- **Stop gaps:** a later minute opening beyond a stop now fills at that worse "
            f"open. One corrected-native baseline trade is affected; the configured default "
            f"has {primary_gap_exits} "
            "such exits in this sample. The activation bar's open is never reused after an "
            "intrabar trigger.",
            "- **Intraminute ambiguity:** the entry minute is eligible for exits; stop wins "
            "a stop/T1 tie and breakeven wins a BE/runner-target tie. One-minute OHLC cannot "
            "reconstruct the true tick sequence, so this is deliberately pessimistic.",
            "- **Unsafe cache reuse:** V3 can select a cache without a manifest and rewrites "
            "its NPZ even on a cache hit. V5 accepts only checksum-pinned V3 provenance or a "
            "matching V5 manifest, publishes its NPZ atomically, and rematerializes all "
            "selected execution paths from raw O/H/L/C one-minute data.",
            "- **Coverage threshold provenance:** V3 can reuse a V6 eligibility table made "
            "under another threshold. V5 reapplies 99% to the stored coverage column. All 25 "
            "used sessions are 100%, but this covers futures-file presence—not equity-minute "
            "completeness. Selected equity paths are checked separately.",
            "- **Inactive/dead controls:** the 09:35 SHORT setup's OI minimum equals the "
            "global 1% cap, making it effectively boundary-only; no daily position cap, "
            "cooldown, duplicate-symbol lock or capital constraint exists.",
            "",
            "## Audited signal-to-exit flow",
            "",
            "1. A dated point-in-time universe maps cash symbols to the nearest unexpired "
            "stored stock-futures contract. Index futures are excluded from the stock universe.",
            "2. Five exact end-labelled NSE cash one-minute candles build each completed "
            "five-minute candle. Price, volume, EMA9/20/50, confirmation, trigger, entry and "
            "exit are all cash-equity data. Only current/prior OI is from the mapped NFO future.",
            "3. Loose gates are |5m price change| ≥0.10%, OI change ≥0.05% plus current OI "
            "> prior OI, volume ratio ≥0.8, and a strict directional EMA9/20/50 stack. The "
            "volume denominator is a shifted rolling-20 bar mean across sessions.",
            "4. The exact S+1 one-minute candle must close beyond both its open and the "
            "completed five-minute close in the setup direction. Its high/low is the stop-entry "
            "trigger; S+2 is the first possible entry minute.",
            "5. The 09:25 SHORT cell alone requires the already-completed 09:20 NIFTY "
            "near-month future return ≤−0.05%. Missing NIFTY context fails closed. A global "
            "OI-change cap of 1% is then applied.",
            "6. Per-cell filters and picker/max-entry limits choose orders. Positions may "
            "overlap, including repeated symbols; returns are therefore strategy-event returns, "
            "not an executable portfolio equity curve.",
            "7. V5 manages fills with the profile's two-stage exit and subtracts one flat "
            "round-trip bps proxy. Brokerage, STT, exchange fees, GST, stamp duty, spread, "
            "lot size and margin are not itemized.",
            "",
            "Despite the filename, this is **not an options/futures-price P&L backtest**. "
            "There is no CE/PE selection, strike, option premium, option spread or option "
            "expiry execution; those requested breakdowns correctly report `NOT_APPLICABLE`.",
            "",
            "## Funnel and one-minute experiments",
            "",
            "Raw reconstruction found 9,935 exact, positive-range S+1 candles at the loose "
            "five-minute gates. The strict directional confirmation retains 4,025 and rejects "
            "5,910. NIFTY/OI policy leaves 3,837; the 12 active V3 time/side cells contain "
            "921 survivors, which yield 79 selected orders and 78 fills. Sixty-four fills "
            "occur on the first forward minute.",
            "",
            "The broader high/low breakout confirmation raises the tested book to 121 orders "
            "and 114 fills, but two-stage VALIDATION is negative (PF 0.875, net -1.361%) "
            "and full drawdown worsens to -6.849%; it is rejected. A +0.10 wick-cap change "
            "adds only one TRAIN fill and none in validation/pseudo-test. A two-minute activation "
            "delay improves PF/net but removes three fills and is execution-sequence sensitive. "
            "A 0.02% trigger buffer removes two fills; larger buffers deteriorate. None is a "
            "standalone proven one-minute improvement.",
            "",
            "## Timing and NIFTY conclusions",
            "",
            "- Timestamps are candle ends: 09:25 represents 09:21–09:25, then 09:26 "
            "confirmation and 09:27 earliest entry.",
            "- 09:50 is absent because V3 defines no 09:50 cell, not because equality logic "
            "misses the bar. Its SHORT leg is positive only as a sparse experimental component; "
            "the isolated development profile fails the strict guardrail.",
            "- 11:20 SHORT is the only individual added timing with ≥2 fills and positive "
            "marginal evidence in both TRAIN and VALIDATION (five total fills). Adjacent 11:15 "
            "has no fills and 11:25 has one loss, so the result is knife-edge and shadow-only.",
            "- Every existing setup's max-entries +1 relaxation was tested. Five were inert; "
            "none improved fills, PF, win rate and net together.",
            "- Removing the 09:25 NIFTY gate adds five fills and all five lose; keep −0.05%. "
            "Tightening to −0.15% hurts pseudo-test; symmetric/global alignment removes too "
            "many trades or lacks forward confirmation.",
            "",
            "## Exit, MAE/MFE and target conclusions",
            "",
            "Median full-session MFE/MAE is about +1.315%/−0.700%. MFE reach is 75.64% "
            "at +0.50%, 55.13% at +1.05%, 53.85% at +1.10%, 50.00% at +1.25%, "
            "and 26.92% at +2.60%. Median first-hit time is 38 minutes near T1 and "
            "126 minutes at +2.60%.",
            "",
            "The development grid selected SL 1.50%, T1 1.075%, 10% partial, +2.60% "
            "runner and immediate BE. The 1.05% neighbor is nearly identical, supporting a "
            "small local plateau; 1.10/1.125 fall below the 50% pooled-development T1 guardrail. "
            "Because only 10% is booked at T1, the higher target-hit rate must not be confused "
            "with a full-position target hit.",
            "",
            "## Configured higher-frequency cost and execution stress",
            "",
            stresses[DEFAULT_PROFILE][[
                "stress",
                "executed_trades",
                "win_rate_pct",
                "target_hit_rate_pct",
                "profit_factor",
                "net_profit_pct",
                "maximum_drawdown_pct",
            ]].to_markdown(index=False, floatfmt=".3f"),
            "",
            "Each profile directory also contains parameter-neighbor sensitivity, three "
            "frozen expanding-window folds, session bootstrap diagnostics, setup metrics and "
            "all requested contextual breakdowns. Bootstrap probabilities are descriptive "
            "resampling of these same 25 sessions, not future probabilities.",
            "",
            "## Configured higher-frequency date-wise results",
            "",
            primary_daily[[
                "day",
                "period",
                "trades",
                "wins",
                "losses",
                "target_hits",
                "stop_hits",
                "net_return_pct",
                "net_return_on_capital_pct",
                "unleveraged_net_profit_rupees",
                "net_profit_rupees",
                "average_profit_per_trade_on_capital_pct",
                "average_profit_per_trade_rupees",
                "cumulative_net_return_pct",
                "cumulative_net_return_on_capital_pct",
                "cumulative_net_profit_rupees",
                "drawdown_pct",
                "drawdown_on_capital_pct",
                "drawdown_rupees",
            ]].to_markdown(index=False, floatfmt=".3f"),
            "",
            "## Change classification",
            "",
            "Change | Classification | Decision",
            "--- | --- | ---",
            "Uniform 15:15 cutoff, exact path continuity | Backtesting correctness fix | Accepted",
            "Adverse-open entry/stop fills and fill-relative brackets | Backtesting correctness fix | Accepted",
            "Checksum-pinned, atomic, isolated V5 cache | Backtesting correctness fix | Accepted",
            "99% eligibility revalidation | Backtesting correctness fix | Accepted; no current trade changed",
            "Two-stage T1/BE/runner exit | Risk-management improvement | Retain as shadow; target definition disclosed",
            "180-minute conservative cap | Risk-management improvement | Lower-return/lower-drawdown shadow",
            "11:20 SHORT | Trade-frequency improvement | Shadow-only; five fills and unstable neighbors",
            "09:50 SHORT | Experimental and not yet proven | Higher-frequency profile only",
            "+0.10 wick cap | Experimental and not yet proven | Higher-frequency profile only; one TRAIN fill",
            "Broad confirmation, extra entry slots, max entries, trigger buffers | Experimental and not yet proven | Rejected or investigate on new data",
            "Any proven strategy improvement | Proven strategy improvement | **None**—no untouched test exists",
            "",
            "## Remaining risks",
            "",
            "- Only 25 sessions, two contract labels and one market epoch; multiple testing "
            "and prior inspection make selection bias severe.",
            "- Validation T1 rates remain below 50% for the selected profiles even when the "
            "pooled development and full-history rates exceed 50%.",
            "- Added timing legs have five fills each; infinite marginal PF is a sample-size "
            "artifact, not evidence of zero future losses.",
            "- Flat costs and cash-equity execution cannot represent F&O premium behavior, "
            "spread, impact, margin, lot sizing or Indian transaction charges.",
            "- Ten same-symbol overlap pairs across six symbol-days and maximum observed "
            "concurrency of seven mean summed returns may be undeployable with finite capital.",
            "- Backfill 15:16–15:30 before testing a genuine 15:30 square-off. Collect new "
            "sessions and freeze the configuration before any promotion decision.",
            "",
            "## Reproduction",
            "",
            "```powershell",
            "# Immutable official baseline",
            "python fno_v13_corrected_v3_backtest.py --through-day 2026-09-03 --cost-bps 5",
            "# Configured V5 default: higher-frequency; all profiles are still reported",
            "python fno_v13_corrected_v5_backtest.py --profile higher_frequency --through-day 2026-09-03 --cost-bps 5",
            "# Consolidate the complete timing/entry/exit/profile experiment archive",
            "python fno_v13_v5_research.py",
            "# Focused and adjacent regression tests",
            "pytest -q tests/test_fno_v13_corrected_v5_backtest.py tests/test_fno_v13_corrected_v3_backtest.py tests/test_fno_v13_corrected_v4_backtest.py",
            "```",
            "",
            f"V13-v3 source SHA-256 before/after: `{v3_hash_before}` / `{v3_hash_after}`; "
            f"unchanged = **{v3_hash_before == v3_hash_after}**.",
        f"Selected-path checks: {len(quality)} unique paths; first S+2 minute = "
            f"{int(quality['first_forward_minute_present'].sum())}; exact 15:15 = "
            f"{int(quality['exact_cutoff_present'].sum())}; continuous one-minute = "
            f"{int(quality['continuous_one_minute_path'].sum())}.",
            "",
            "## Output locations",
            "",
            f"- Main report: `{REPORT_PATH}`",
            f"- Profile comparison: `{COMPARISON_PATH}`",
            f"- Parameter registry: `{PARAMETER_REGISTRY_PATH}`",
            f"- Session eligibility: `{ELIGIBILITY_PATH}`",
            f"- Provenance: `{PROVENANCE_PATH}`",
            f"- Research audit: `{RESEARCH_REPORT_PATH}`",
            f"- Research manifest: `{RESEARCH_MANIFEST_PATH}`",
        ]
    )
    for profile, outputs in output_map.items():
        lines.append(f"- {profile} outputs: `{Path(outputs['trades']).parent}`")
    lines.append("")
    return "\n".join(lines)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--profile",
        choices=["all", *PROFILES],
        default=DEFAULT_PROFILE,
        help=(
            f"marks the requested primary profile (default: {DEFAULT_PROFILE}); "
            "all three comparator artifacts are still regenerated for auditability"
        ),
    )
    parser.add_argument("--through-day", default=DEFAULT_THROUGH_DAY)
    parser.add_argument("--cost-bps", type=float, default=DEFAULT_COST_BPS)
    parser.add_argument(
        "--capital-per-entry-rupees",
        type=float,
        default=DEFAULT_CAPITAL_PER_ENTRY_RUPEES,
        help=(
            "Fresh fixed capital/margin base used for rupee P&L reporting on "
            "each filled trade; does not compound or reserve a portfolio balance."
        ),
    )
    parser.add_argument(
        "--leverage-factor",
        type=float,
        default=DEFAULT_LEVERAGE_FACTOR,
        help=(
            "Exposure multiplier for rupee P&L reporting. Example: 5x turns "
            "INR 100000 capital into INR 500000 exposure per filled trade."
        ),
    )
    parser.add_argument("--cutoff", default="15:15")
    parser.add_argument("--rebuild-cache", action="store_true")
    parser.add_argument("--refresh-eligibility", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    started = time.monotonic()
    cutoff = args.cutoff.replace(":", "")
    if cutoff != OFFICIAL_CUTOFF:
        raise ValueError(
            "Official V13-v5 is fixed to 15:15 because later raw bars are absent "
            "for 22/25 sessions. Backfill first before testing another cutoff."
        )
    through_day = pd.Timestamp(args.through_day).date()
    capital_per_entry = float(args.capital_per_entry_rupees)
    if not np.isfinite(capital_per_entry) or capital_per_entry <= 0:
        raise ValueError("--capital-per-entry-rupees must be a positive finite value.")
    leverage_factor = float(args.leverage_factor)
    if not np.isfinite(leverage_factor) or leverage_factor <= 0:
        raise ValueError("--leverage-factor must be a positive finite value.")
    v3_hash_before = _sha256(Path(v13_v3.__file__).resolve())
    validate_configuration()
    print(
        f"[V13-v5][DEFAULT_PROFILE] {DEFAULT_PROFILE} "
        f"(requested={args.profile})",
        flush=True,
    )
    signals, cached_paths, days, calendar, cache_records, _, eligibility = load_market(
        through_day,
        rebuild_cache=args.rebuild_cache,
        refresh_eligibility=args.refresh_eligibility,
    )
    baseline_orders = select_orders(signals, v13_v3.active_setups())
    wanted_names = list(PROFILES) if args.profile == "all" else [args.profile]
    profile_orders = {
        name: select_orders(signals, profile_setups(PROFILES[name]))
        for name in wanted_names
    }
    # Reports always compare all three profiles, even when a single profile is requested.
    if args.profile != "all":
        for name in PROFILES:
            profile_orders.setdefault(name, select_orders(signals, profile_setups(PROFILES[name])))
    union = pd.concat([baseline_orders, *profile_orders.values()], ignore_index=True)
    union = union.drop_duplicates("sid", keep="first")
    paths, quality = materialize_raw_paths(union, cutoff=cutoff)
    market = session_context(signals["contract_month"].unique(), cutoff)

    corrected_native = simulate_native(
        baseline_orders, paths, cost_bps=args.cost_bps
    )
    corrected_native["configured_setup_rules"] = len(v13_v3.active_setups())
    corrected_v3 = annotate_trade_context(
        corrected_native,
        calendar,
        market,
    )
    corrected_v3 = apply_fixed_capital_model(
        corrected_v3,
        capital_per_entry,
        leverage_factor,
    )
    audits: dict[str, pd.DataFrame] = {}
    outputs: dict[str, dict[str, str]] = {}
    stresses: dict[str, pd.DataFrame] = {}
    for name, orders in profile_orders.items():
        profile = PROFILES[name]
        audit = simulate_scaleout(
            orders, paths, profile.exit, cost_bps=args.cost_bps
        )
        audit["profile"] = name
        audit["strategy_version"] = STRATEGY_VERSION
        audit["evidence_status"] = profile.evidence
        audit["configured_setup_rules"] = len(profile_setups(profile))
        audit = annotate_trade_context(audit, calendar, market)
        audit = apply_fixed_capital_model(audit, capital_per_entry, leverage_factor)
        audits[name] = audit
        outputs[name] = write_profile_outputs(
            profile,
            audit,
            profile_setups(profile),
            days,
            paths,
            cost_bps=args.cost_bps,
        )
        stresses[name] = pd.read_csv(outputs[name]["stress"])

    comparison_rows = [metrics(corrected_v3, days, label="V13_V3_CORRECTED_UNIFORM_1515")]
    published = published_v3_overlay(
        corrected_v3,
        through_day,
        args.cost_bps,
        capital_per_entry,
        leverage_factor,
    )
    if published is not None:
        comparison_rows.insert(0, metrics(published, days, label="V13_V3_OFFICIAL_PUBLISHED"))
    comparison_rows.extend(
        metrics(audits[name], days, label=f"V13_V5_{name.upper()}")
        for name in PROFILES
    )
    comparison = pd.DataFrame(comparison_rows)
    comparison["configured_default"] = comparison["label"].eq(
        f"V13_V5_{DEFAULT_PROFILE.upper()}"
    )

    period_rows = []
    periods = split_days(days)
    all_audits = {
        "V13_V3_CORRECTED_UNIFORM_1515": corrected_v3,
        **{f"V13_V5_{k.upper()}": v for k, v in audits.items()},
    }
    for strategy, audit in all_audits.items():
        for period in ("TRAIN", "VALIDATION", "PSEUDO_TEST", "ALL"):
            period_rows.append(
                {"strategy": strategy, "period": period, **metrics(audit, periods[period], label=period)}
            )
    period_frame = pd.DataFrame(period_rows)
    period_frame["configured_default"] = period_frame["strategy"].eq(
        f"V13_V5_{DEFAULT_PROFILE.upper()}"
    )

    registry = parameter_registry(
        profile_orders,
        cost_bps=args.cost_bps,
        capital_per_entry_rupees=capital_per_entry,
        leverage_factor=leverage_factor,
    )
    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    common.atomic_write_csv(comparison, COMPARISON_PATH)
    common.atomic_write_csv(period_frame, RESULT_DIR / "fno_v13_corrected_v5_period_metrics.csv")
    common.atomic_write_csv(registry, PARAMETER_REGISTRY_PATH)
    common.atomic_write_csv(eligibility, ELIGIBILITY_PATH)
    common.atomic_write_csv(quality, RESULT_DIR / "fno_v13_corrected_v5_path_quality.csv")
    common.atomic_write_csv(corrected_v3, RESULT_DIR / "fno_v13_corrected_v5_corrected_v3_baseline_trades.csv")

    v3_hash_after = _sha256(Path(v13_v3.__file__).resolve())
    report = render_report(
        days=days,
        cost_bps=args.cost_bps,
        capital_per_entry_rupees=capital_per_entry,
        leverage_factor=leverage_factor,
        comparison=comparison,
        periods=period_frame,
        profile_audits=audits,
        stresses=stresses,
        output_map=outputs,
        cache_records=cache_records,
        quality=quality,
        v3_hash_before=v3_hash_before,
        v3_hash_after=v3_hash_after,
    )
    common.atomic_write_text(REPORT_PATH, report)
    common.atomic_write_text(WORKSPACE_REPORT_PATH, report)
    common.atomic_write_json(
        PROVENANCE_PATH,
        {
            "strategy_version": STRATEGY_VERSION,
            "evidence_status": EVIDENCE_STATUS,
            "generated_at_ist": common.now_ist().isoformat(timespec="seconds"),
            "through_day": str(through_day),
            "sessions": [str(day) for day in days],
            "chronological_split": {key: [str(day) for day in value] for key, value in periods.items()},
            "cost_bps": float(args.cost_bps),
            "capital_per_entry_rupees": capital_per_entry,
            "leverage_factor": leverage_factor,
            "exposure_per_entry_rupees": capital_per_entry * leverage_factor,
            "capital_model": "FIXED_CAPITAL_PER_FILLED_TRADE_WITH_LEVERAGE_NON_COMPOUNDED",
            "configured_default_profile": DEFAULT_PROFILE,
            "requested_profile": args.profile,
            "official_cutoff": OFFICIAL_CUTOFF,
            "minimum_contract_coverage": MIN_CONTRACT_COVERAGE,
            "eligibility_path": str(ELIGIBILITY_PATH.resolve()),
            "eligible_sessions": int(
                (
                    eligibility["eligible"].astype(bool)
                    & eligibility["day"].le(through_day)
                ).sum()
            ),
            "session_eligibility_warning": (
                "Coverage measures mapped futures files with rows; selected raw "
                "cash-equity minute paths are validated separately."
            ),
            "v13_v5_source": str(Path(__file__).resolve()),
            "v13_v5_source_sha256": _sha256(Path(__file__).resolve()),
            "v13_v3_source_sha256_before": v3_hash_before,
            "v13_v3_source_sha256_after": v3_hash_after,
            "v13_v3_source_unchanged": v3_hash_before == v3_hash_after,
            "profiles": {name: asdict(profile) for name, profile in PROFILES.items()},
            "configured_default_output": outputs[DEFAULT_PROFILE],
            "cache_records": cache_records,
            "comparison": comparison.to_dict("records"),
            "outputs": outputs,
            "warning": "No untouched test remains; all V5 profiles are research shadows.",
        },
    )
    if v3_hash_before != v3_hash_after:
        raise RuntimeError("V13-v3 source changed while V13-v5 ran.")
    for row in comparison.to_dict("records"):
        print(
            f"[V13-v5] {row['label']}: trades={row['executed_trades']} "
            f"WR={row['win_rate_pct']:.3f}% target={row['target_hit_rate_pct']:.3f}% "
            f"PF={row['profit_factor']:.6f} net={row['net_profit_pct']:+.6f}% "
            f"pnl_inr={row['net_profit_rupees']:+.2f} "
            f"avg_inr={row['average_profit_per_trade_rupees']:+.2f} "
            f"DD={row['maximum_drawdown_pct']:.6f}%",
            flush=True,
        )
    print(f"[V13-v5][REPORT] {REPORT_PATH}")
    print(f"[V13-v5][DONE] {time.monotonic() - started:.1f}s")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
