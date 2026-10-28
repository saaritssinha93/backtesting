"""Read-only, pinned G8 all-candidate inputs for isolated G9 sizing research.

The published combined book includes *all* selected candidates, not merely
executed trades.  Reusing its unchanged price exits is exact for sizing-only
experiments; every candidate must still pass the portfolio allocator again.
No source runner, market-data retrieval, or publication pointer is invoked.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd

from research.g8_exit_engine import _flags, allocate_variant, metrics


ROOT = Path(__file__).resolve().parents[1]
G8_ROOT = Path("C:/TradingData/eqidv2/backtesting_result_v13_v10_g_8")
DAILY = G8_ROOT / "daily_comparison/2026-10-23/20261023T125505562512_g8_baseline_vs_shock"
CACHE = G8_ROOT / "inputs/baseline_v2"
THROUGH = "2026-10-23"
POLICY = "persistent_direction_30m_2.0"
DAILY_MANIFEST = "5ced269cb37ccef4ae329062c5db67ef66a4cdd45c54c84a66e83984494ebf54"
CACHE_MANIFEST = "c0a52d928a3b7b6ac3a270a5908001a71f6740634c77ee41e7ee2244a7e0a6a9"


def sha(path):
    with Path(path).open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def _read(path):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def _verify_package(directory, expected_manifest):
    """Pin the manifest and verify members, without running legacy code."""
    directory = Path(directory).resolve()
    manifest_path = directory / "manifest.json"
    if sha(manifest_path) != expected_manifest:
        raise ValueError("Pinned G9 input manifest changed: " + str(directory))
    manifest = _read(manifest_path)
    if not isinstance(manifest.get("files"), dict) or not manifest["files"]:
        raise ValueError("G9 input manifest has no members")
    for name, digest in manifest["files"].items():
        member = (directory / name).resolve()
        if not member.is_relative_to(directory) or not member.is_file() or sha(member) != digest:
            raise ValueError("Pinned G9 input member changed: " + name)
    return manifest


def _validate_book(book, days):
    required = {"sid", "day", "setup_id", "tradingsymbol", "side", "filled",
        "g8_baseline_filled", "g8_direction_allowed", "g8_quantity_scale",
        "portfolio_executed", "entry_ts", "exit_ts", "confirmation_ts"}
    if not required.issubset(book):
        raise ValueError("Incomplete G8 candidate book: " + ", ".join(sorted(required - set(book))))
    result = book.copy(deep=True).reset_index(drop=True)
    result["day"] = result.day.astype(str).str[:10]
    if result.sid.isna().any() or result.sid.duplicated().any():
        raise ValueError("G8 candidate sid must be nonmissing and unique")
    if result.duplicated(["day", "setup_id", "tradingsymbol", "side"]).any():
        raise ValueError("Duplicate G8 candidate identity")
    if len(set(days)) != len(days) or not set(result.day).issubset(days):
        raise ValueError("G8 candidate/session calendar mismatch")
    if not result.side.isin(["LONG", "SHORT"]).all():
        raise ValueError("Invalid candidate side")
    for key in ("filled", "g8_baseline_filled", "g8_direction_allowed", "portfolio_executed"):
        result[key] = _flags(result[key], key)
    if not result.filled.equals(result.g8_baseline_filled & result.g8_direction_allowed):
        raise ValueError("G8 filtered fills do not preserve native fills and gate")
    if (result.portfolio_executed & ~result.filled).any():
        raise ValueError("G8 portfolio executed a disallowed/unfilled candidate")
    scale = pd.to_numeric(result.g8_quantity_scale, errors="raise")
    if not np.isfinite(scale).all() or not np.allclose(scale, 1., atol=0., rtol=0.):
        raise ValueError("G9 baseline must use unchanged full-size G8 quantities")
    native = result.loc[result.g8_baseline_filled]
    confirmation = pd.to_datetime(native.confirmation_ts, utc=True)
    entry, exit_ = (pd.to_datetime(native[key], utc=True) for key in ("entry_ts", "exit_ts"))
    if confirmation.isna().any() or entry.isna().any() or exit_.isna().any():
        raise ValueError("Missing G8 native fill timestamp")
    if (entry < confirmation).any() or (exit_ < entry).any():
        raise ValueError("Invalid G8 native fill chronology")
    return result


def load_sources(*, through=THROUGH):
    """Return config, ALL candidates, calendar, reference book and provenance.

    ``book`` and ``expected_baseline`` are independent DataFrames.  Both retain
    g8_baseline_filled (pre-gate fills), g8_direction_allowed, and the historical
    portfolio-executed flag.  For a new replay use a SID-indexed allowed Series
    and set g8_quantity_scale; never filter this book to executed rows first.
    """
    if through != THROUGH:
        raise ValueError("Pinned G9 source revision ends at " + THROUGH)
    daily_manifest = _verify_package(DAILY, DAILY_MANIFEST)
    cache_manifest = _verify_package(CACHE, CACHE_MANIFEST)
    summary = _read(DAILY / "summary.json")
    if (summary.get("through") != THROUGH or summary.get("complete") is not True
            or summary.get("execution_authority") is not False
            or summary.get("baseline_policy") != POLICY):
        raise ValueError("Unexpected pinned G8 baseline identity")
    parent = Path(summary["prior_run"])
    parent_manifest = _verify_package(parent, summary["prior_manifest_sha256"])
    config = _read(DAILY / "config.json")
    if config != _read(CACHE / "config.json"):
        raise ValueError("G8 historical and daily underlying configurations differ")
    if config.get("variant_id") != "V13-V10-G-7-G6-REPLICA-V2" or len(config["active_setups"]) != 51:
        raise ValueError("G9 requires the original G7 V2 51-setup exit configuration")
    days = summary["periods"]["all"]
    if days != sorted(set(days)) or len(days) != 57 or days[-1] != THROUGH:
        raise ValueError("Unexpected G8 combined calendar")
    book = _validate_book(pd.read_csv(DAILY / "baseline/combined_portfolio_trades.csv", low_memory=False), days)
    cached_orders = pd.read_csv(CACHE / "orders.csv", usecols=["sid", "day"])
    history = book.loc[book.day.lt(THROUGH)]
    if set(history.sid) != set(cached_orders.sid) or len(book) != 582:
        raise ValueError("G9 source omitted pre-portfolio candidates")
    parent_book = pd.read_csv(parent / "baseline/portfolio_trades.csv", low_memory=False).set_index("sid")
    check = history.set_index("sid").reindex(parent_book.index)
    if set(check.index) != set(history.sid):
        raise ValueError("G8 inherited baseline inventory differs")
    for column in ("entry_price", "exit_price", "gross_return_pct", "portfolio_net_profit_rupees"):
        if not np.allclose(pd.to_numeric(check[column]), pd.to_numeric(parent_book[column]),
                           atol=1e-8, rtol=0., equal_nan=True):
            raise ValueError("G8 inherited baseline changed: " + column)
    allowed = book.set_index("sid").g8_direction_allowed
    reproduced = allocate_variant(book, allowed, config, "g9_source_baseline_parity")
    for column in ("portfolio_executed", "g8_direction_allowed", "g8_baseline_filled"):
        if not reproduced[column].equals(book[column]):
            raise ValueError("G9 source allocation parity failure: " + column)
    for column in ("portfolio_net_profit_rupees", "portfolio_gross_profit_rupees", "portfolio_cost_rupees"):
        if not np.allclose(reproduced[column], book[column], atol=1e-6, rtol=0.):
            raise ValueError("G9 source accounting parity failure: " + column)
    baseline_stats = metrics(reproduced, days)
    if (baseline_stats["trades"], baseline_stats["wins"], baseline_stats["losses"]) != (432, 328, 104):
        raise ValueError("Pinned G8 executed trade inventory differs")
    if not np.isclose(baseline_stats["net_pnl_rs"], 1083789.4152852027, atol=1e-6, rtol=0.):
        raise ValueError("Pinned G8 full-history net profit differs")
    vix_path = DAILY / "extended_vix.csv"
    cache_provenance = _read(CACHE / "provenance.json")
    limitations = [
        "Backtesting only; no live/paper authority or source-pointer mutation.",
        "Inherited setups and targets were selected after historical inspection; not an untouched holdout.",
        "After-close stock-day eligibility exclusions are inherited and are not a causal live entry filter.",
        "Available source calendar only: 57 sessions from 2026-07-29 through 2026-10-23; missing sessions are not zero-return days.",
        "Original G8 price exits reused unchanged for sizing-only replay; no new stock-price path assumptions.",
        "Fractional theoretical share quantities, fixed capital, 5x leverage, 5bps modeled round-trip costs; no guarantee of live fills.",
        "The inherited allocator defaults to at most 10 concurrent positions; full capital allocation is replayed after scaling.",
    ]
    provenance = dict(schema_version="g9_pinned_g8_sources_v1", through=THROUGH,
        scope="BACKTEST_ONLY", execution_authority=False, baseline_policy=POLICY,
        all_candidates_retained=True, source_allocation_reproduced=True,
        selected_candidates=len(book), baseline_metrics=baseline_stats, days=days,
        packages=[dict(path=str(DAILY), manifest_sha256=DAILY_MANIFEST, files=daily_manifest["files"]),
                  dict(path=str(CACHE), manifest_sha256=CACHE_MANIFEST, files=cache_manifest["files"]),
                  dict(path=str(parent), manifest_sha256=summary["prior_manifest_sha256"], files=parent_manifest["files"])],
        used_paths={str(DAILY / "baseline/combined_portfolio_trades.csv"):sha(DAILY / "baseline/combined_portfolio_trades.csv"),
                   str(DAILY / "config.json"):sha(DAILY / "config.json"), str(vix_path):sha(vix_path)},
        inherited_source_provenance=cache_provenance,
        inherited_latest_day_coverage=summary["coverage"], limitations=limitations)
    return dict(config=config, book=book, days=days, expected_baseline=book.copy(deep=True),
                baseline_metrics=baseline_stats, vix_path=vix_path, provenance=provenance)
