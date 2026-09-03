"""Read-only OI-threshold sensitivity study for corrected V6.

This is deliberately separate from ``fno_v6_corrected_backtest.py``.  It
loads the already-built, point-in-time near-month signal caches, proves exact
trade parity with the published corrected V6 audit, and changes only the
``oi_change_pct`` field of copied SetupSpec objects in memory.

The existing caches are a loose *positive-OI* superset: signal construction
already required ``oi > prev_oi`` and ``oi_change_pct >= 0.05``.  Therefore
this program can compare stricter positive thresholds, but it cannot answer a
true no-OI/flat-OI/falling-OI ablation without a new, separately built cache.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
from dataclasses import asdict, replace
from datetime import date
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import pandas as pd

import fno_oi_common as common
import fno_v5_hybrid_backtest as replay
import fno_v6_corrected_backtest as corrected


RESEARCH_VERSION = "V6_CORRECTED_OI_THRESHOLD_SENSITIVITY_V1"
EXPECTED_CORRECTED_SHA256 = (
    "06baf32c33156f21bce1dc786e5687a250b9711a1bca3a186283c824edfcf62d"
)
PINNED_CACHE_SHA256 = {
    "26AUG_4ade722c23dcf5c5.parquet": (
        "32eb160f3ad776565b53a550c21efa061f2db21b5d76eee924ef9004b9629b87"
    ),
    "26AUG_4ade722c23dcf5c5.npz": (
        "c363fe96c524b35cb55d481af3f2f664b07c616f99e09d0737e197d70b54a79e"
    ),
    "26SEP_16fbb1d141241ff3.parquet": (
        "a7d161eb8a6408b84b1a176aaeeeead6c125cc31f327e0d3dd78668ddb71ce05"
    ),
    "26SEP_16fbb1d141241ff3.npz": (
        "b44a265994b8e418eb5de12bf8c4c4deb205987e97302ff06bf23127b9039cd7"
    ),
}
PINNED_CACHE_STEMS = {
    "26AUG": "26AUG_4ade722c23dcf5c5",
    "26SEP": "26SEP_16fbb1d141241ff3",
}

DEFAULT_SPLIT_DAY = date(2026, 8, 14)
DEFAULT_COST_BPS = 5.0
THRESHOLD_GRID = (
    0.05,
    0.075,
    0.10,
    0.15,
    0.20,
    0.25,
    0.35,
    0.50,
    0.75,
    1.00,
    1.25,
    1.50,
    2.00,
    2.50,
)
MIN_POLICY_TRADE_RETENTION = 0.70
MIN_CHANGED_DAYS_PER_PERIOD = 3
BOOTSTRAP_REPLICATES = 20_000
BOOTSTRAP_SEED = 20260902

RESULT_DIR = (
    corrected.RESULT_DIR.parent / "v6_corrected_oi_threshold_research"
)
REPORT_PATH = RESULT_DIR / "fno_v6_oi_threshold_analysis.md"
MANIFEST_PATH = RESULT_DIR / "manifest.json"
PARITY_PATH = RESULT_DIR / "baseline_parity.json"
SWEEP_PATH = RESULT_DIR / "per_setup_threshold_sweep.csv"
OAT_PATH = RESULT_DIR / "one_at_a_time_portfolio.csv"
POLICY_SWEEP_PATH = RESULT_DIR / "uniform_and_side_policy_sweep.csv"
POLICY_RESULT_PATH = RESULT_DIR / "portfolio_policy_results.csv"
CHOICES_PATH = RESULT_DIR / "train_optimized_threshold_choices.csv"
BASELINE_SETUP_PATH = RESULT_DIR / "baseline_setup_results.csv"
SHADOW_BOOK_PATH = RESULT_DIR / "shadow_candidate_setup_book.json"
SHADOW_DAILY_PATH = RESULT_DIR / "shadow_candidate_vs_baseline_daily.csv"
SHADOW_TRADES_PATH = RESULT_DIR / "shadow_candidate_trades.csv"

# Hypotheses only. These values are never applied to corrected V6 by this file.
# They improved the earlier segment while changing zero later-period decisions.
SHADOW_THRESHOLD_CHOICES = {
    "0936_LONG": 0.15,
    "0941_LONG": 0.075,
}


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _pf(profit: float, loss: float) -> float:
    if loss > 0:
        return float(profit / loss)
    return float("inf") if profit > 0 else float("nan")


def _finite_delta(candidate: float, baseline: float) -> float:
    if np.isfinite(candidate) and np.isfinite(baseline):
        return float(candidate - baseline)
    if candidate == baseline:
        return 0.0
    if np.isposinf(candidate):
        return float("inf")
    if np.isposinf(baseline):
        return float("-inf")
    return float("nan")


def _daily_returns(audit: pd.DataFrame, days: list[date]) -> np.ndarray:
    if audit.empty:
        return np.zeros(len(days), dtype=float)
    filled = audit.loc[audit["filled"]].copy()
    if filled.empty:
        return np.zeros(len(days), dtype=float)
    grouped = filled.groupby("day")["net_return_pct"].sum()
    return grouped.reindex(days, fill_value=0.0).to_numpy(float)


def _metrics(audit: pd.DataFrame, days: list[date]) -> dict[str, Any]:
    subset = audit.loc[audit["day"].isin(set(days))].copy() if not audit.empty else audit
    if subset.empty:
        values = np.array([], dtype=float)
        orders = fills = 0
    else:
        values = subset.loc[subset["filled"], "net_return_pct"].to_numpy(float)
        orders = int(len(subset))
        fills = int(subset["filled"].sum())
    profit = float(values[values > 0].sum()) if values.size else 0.0
    loss = float(-values[values < 0].sum()) if values.size else 0.0
    day_values = _daily_returns(subset, days)
    day_profit = float(day_values[day_values > 0].sum())
    day_loss = float(-day_values[day_values < 0].sum())
    curve = np.r_[0.0, np.cumsum(day_values)]
    drawdown = curve - np.maximum.accumulate(curve)
    top_two = float(np.sort(values[values > 0])[-2:].sum()) if (values > 0).any() else 0.0
    return {
        "sessions": int(len(days)),
        "orders": orders,
        "fills": fills,
        "wins": int((values > 0).sum()),
        "losses": int((values < 0).sum()),
        "win_rate": float((values > 0).mean()) if values.size else float("nan"),
        "trade_pf": _pf(profit, loss),
        "day_pf": _pf(day_profit, day_loss),
        "net_pct": float(values.sum()) if values.size else 0.0,
        "expectancy_pct": float(values.mean()) if values.size else float("nan"),
        "max_drawdown_pct": float(drawdown.min()),
        "positive_days": int((day_values > 0).sum()),
        "negative_days": int((day_values < 0).sum()),
        "top2_profit_share": float(top_two / profit) if profit > 0 else float("nan"),
    }


def _prefixed(metrics: dict[str, Any], prefix: str) -> dict[str, Any]:
    return {f"{prefix}_{key}": value for key, value in metrics.items()}


def _period_days(days: list[date], split_day: date) -> dict[str, list[date]]:
    return {
        "TRAIN": [day for day in days if day < split_day],
        "TEST": [day for day in days if day >= split_day],
        "ALL": list(days),
    }


def _load_pinned_inputs() -> tuple[pd.DataFrame, dict, list[date], dict[str, Any]]:
    source_path = Path(corrected.__file__).resolve()
    source_hash = _sha256(source_path)
    if source_hash != EXPECTED_CORRECTED_SHA256:
        raise RuntimeError(
            "Corrected V6 source hash changed; refusing to mix this study with a "
            f"different baseline. Expected {EXPECTED_CORRECTED_SHA256}, got {source_hash}."
        )

    actual_cache_hashes: dict[str, str] = {}
    parts: list[tuple[pd.DataFrame, dict]] = []
    for month, stem_name in PINNED_CACHE_STEMS.items():
        stem = corrected.CACHE_DIR / stem_name
        for suffix in (".parquet", ".npz"):
            path = stem.with_suffix(suffix)
            if not path.exists():
                raise FileNotFoundError(f"Pinned corrected cache is missing: {path}")
            actual = _sha256(path)
            expected = PINNED_CACHE_SHA256[path.name]
            if actual != expected:
                raise RuntimeError(
                    f"Pinned cache hash mismatch for {path.name}: expected {expected}, got {actual}"
                )
            actual_cache_hashes[path.name] = actual
        cached = corrected._load_cached(stem)
        if cached is None:
            raise RuntimeError(f"Could not load complete pinned cache pair: {stem}")
        signals, paths = cached
        signals = signals.copy()
        signals["contract_month"] = month
        parts.append((signals, paths))

    # This rebases SIDs. Both contract caches start at zero, so a plain concat
    # would silently connect September signals to August price paths.
    signals, paths = corrected.concat_regimes(parts)
    days = sorted(set(signals["day"]))
    if len(signals) != 3730 or len(paths) != 3730 or len(days) != 23:
        raise AssertionError(
            f"Pinned input cardinality drift: signals={len(signals)}, paths={len(paths)}, days={len(days)}"
        )
    if not signals["sid"].is_unique or set(signals["sid"].astype(int)) != set(paths):
        raise AssertionError("Signal/path SID mapping is not one-to-one after regime rebasing.")

    calculated = (signals["oi"] / signals["prev_oi"] - 1.0) * 100.0
    oi_match = np.isclose(
        calculated.to_numpy(float),
        signals["oi_change_pct"].to_numpy(float),
        rtol=0.0,
        atol=1e-10,
    )
    if not bool(oi_match.all()):
        raise AssertionError(f"Cached OI arithmetic mismatch in {(~oi_match).sum()} rows.")
    if float(signals["oi_change_pct"].min()) < 0.05 - 1e-12:
        raise AssertionError("Pinned cache unexpectedly contains an OI change below its loose gate.")

    metadata = {
        "source_path": str(source_path),
        "source_sha256": source_hash,
        "cache_sha256": actual_cache_hashes,
        "candidate_signals": int(len(signals)),
        "forward_paths": int(len(paths)),
        "sessions": int(len(days)),
        "first_day": str(days[0]),
        "last_day": str(days[-1]),
        "min_cached_oi_change_pct": float(signals["oi_change_pct"].min()),
    }
    return signals, paths, days, metadata


def _sort_audit(audit: pd.DataFrame) -> pd.DataFrame:
    if audit.empty:
        return audit
    return audit.sort_values(
        ["day", "hhmm_int", "side", "setup_id", "tradingsymbol"], kind="stable"
    ).reset_index(drop=True)


def _combine_audits(
    audit_by_threshold: dict[tuple[str, float], pd.DataFrame],
    choices: dict[str, float],
) -> pd.DataFrame:
    parts = [
        audit_by_threshold[(setup.setup_id, float(choices[setup.setup_id]))]
        for setup in corrected.ACTIVE_SETUPS
        if not audit_by_threshold[(setup.setup_id, float(choices[setup.setup_id]))].empty
    ]
    if not parts:
        return pd.DataFrame()
    return _sort_audit(pd.concat(parts, ignore_index=True, sort=False))


def _assert_baseline_parity(
    baseline: pd.DataFrame,
    metadata: dict[str, Any],
) -> dict[str, Any]:
    published = pd.read_csv(corrected.AUDIT_OUTPUT_PATH)
    published["day"] = pd.to_datetime(published["day"]).dt.date
    key_columns = ["day", "sid", "setup_id", "tradingsymbol"]
    left = baseline[key_columns + ["net_return_pct", "filled"]].sort_values(key_columns).reset_index(drop=True)
    right = published[key_columns + ["net_return_pct", "filled"]].sort_values(key_columns).reset_index(drop=True)
    keys_identical = left[key_columns].equals(right[key_columns])
    filled_identical = left["filled"].astype(bool).equals(right["filled"].astype(bool))
    max_return_difference = (
        float(np.nanmax(np.abs(left["net_return_pct"].to_numpy(float) - right["net_return_pct"].to_numpy(float))))
        if len(left) == len(right) and len(left)
        else float("inf")
    )
    parity = {
        "published_audit_path": str(corrected.AUDIT_OUTPUT_PATH.resolve()),
        "published_audit_sha256": _sha256(corrected.AUDIT_OUTPUT_PATH),
        "candidate_signals": metadata["candidate_signals"],
        "forward_paths": metadata["forward_paths"],
        "published_rows": int(len(right)),
        "replayed_rows": int(len(left)),
        "keys_identical": bool(keys_identical),
        "filled_identical": bool(filled_identical),
        "max_abs_return_difference": max_return_difference,
        "exact_with_tolerance": bool(
            len(left) == len(right)
            and keys_identical
            and filled_identical
            and max_return_difference <= 1e-12
        ),
    }
    if not parity["exact_with_tolerance"]:
        raise AssertionError(f"Research baseline does not reproduce corrected V6: {parity}")
    return parity


def _selection_change(
    baseline: pd.DataFrame,
    candidate: pd.DataFrame,
    days: Iterable[date],
    setup_id: str,
) -> tuple[int, int]:
    allowed = set(days)
    left = baseline.loc[
        baseline["setup_id"].eq(setup_id) & baseline["day"].isin(allowed)
    ]
    right = candidate.loc[
        candidate["setup_id"].eq(setup_id) & candidate["day"].isin(allowed)
    ]
    keys_left = set(zip(left["day"], left["sid"].astype(int)))
    keys_right = set(zip(right["day"], right["sid"].astype(int)))
    changed = keys_left.symmetric_difference(keys_right)
    return len(changed), len({item[0] for item in changed})


def _paired_bootstrap_total_ci(
    baseline: pd.DataFrame,
    candidate: pd.DataFrame,
    days: list[date],
    *,
    seed: int,
) -> tuple[float, float]:
    if not days:
        return float("nan"), float("nan")
    difference = _daily_returns(candidate, days) - _daily_returns(baseline, days)
    rng = np.random.default_rng(seed)
    sample_indices = rng.integers(0, len(days), size=(BOOTSTRAP_REPLICATES, len(days)))
    samples = difference[sample_indices].sum(axis=1)
    low, high = np.quantile(samples, [0.025, 0.975])
    return float(low), float(high)


def _candidate_rows_without_oi(signals: pd.DataFrame, setup: Any) -> pd.DataFrame:
    rows = signals.loc[
        signals["hhmm_int"].eq(int(setup.signal_end.replace(":", "")))
        & signals["side"].eq(setup.side)
    ].copy()
    price_ok = (
        rows["price_change_pct"].ge(setup.price_change_pct)
        if setup.side == "LONG"
        else rows["price_change_pct"].le(-setup.price_change_pct)
    )
    return rows.loc[
        price_ok
        & rows["volume_ratio"].ge(setup.volume_ratio)
        & rows["body_ratio"].ge(setup.body_ratio)
        & rows["wick_ratio"].le(setup.max_wick_ratio)
        & rows["traded_value"].ge(setup.min_traded_value)
    ].copy()


def _fmt(value: Any, digits: int = 3) -> str:
    if value is None:
        return ""
    try:
        number = float(value)
    except (TypeError, ValueError):
        return str(value)
    if math.isnan(number):
        return "NA"
    if math.isinf(number):
        return "INF" if number > 0 else "-INF"
    return f"{number:.{digits}f}"


def _write_csv(frame: pd.DataFrame, path: Path) -> None:
    common.atomic_write_csv(frame, path)


def _render_report(
    *,
    split_day: date,
    metadata: dict[str, Any],
    parity: dict[str, Any],
    baseline_results: pd.DataFrame,
    baseline_setup: pd.DataFrame,
    choices: pd.DataFrame,
    oat: pd.DataFrame,
    policy_results: pd.DataFrame,
    accepted: pd.DataFrame,
) -> str:
    lookup = baseline_results.set_index("period")
    baseline_all = lookup.loc["ALL"]
    baseline_train = lookup.loc["TRAIN"]
    baseline_test = lookup.loc["TEST"]
    tuned = policy_results.loc[
        policy_results["policy"].eq("PER_SETUP_MAX_TRAIN_NET")
    ].set_index("period")

    lines = [
        "# Corrected V6: 5-minute OI threshold analysis",
        "",
        "## Decision",
        "",
        "**DO NOT CHANGE `fno_v6_corrected_backtest.py`.** Separate long/short OI "
        "thresholds can improve historical PF mechanically, but this dataset provides "
        "no concrete evidence that a changed threshold book will improve future results.",
        "",
        f"- Baseline: {int(baseline_all['fills'])} fills, PF **{_fmt(baseline_all['trade_pf'])}**, "
        f"net **{float(baseline_all['net_pct']):+.3f}%** over {int(baseline_all['sessions'])} sessions.",
        f"- Thresholds chosen independently on the first {int(baseline_train['sessions'])} days raised "
        f"training PF from {_fmt(baseline_train['trade_pf'])} to {_fmt(tuned.loc['TRAIN', 'trade_pf'])} "
        f"and training net from {float(baseline_train['net_pct']):+.3f}% to "
        f"{float(tuned.loc['TRAIN', 'net_pct']):+.3f}%.",
        f"- Applied unchanged to the following {int(baseline_test['sessions'])} days, that tuned book "
        f"fell from baseline PF {_fmt(baseline_test['trade_pf'])} / net "
        f"{float(baseline_test['net_pct']):+.3f}% to PF {_fmt(tuned.loc['TEST', 'trade_pf'])} / "
        f"net {float(tuned.loc['TEST', 'net_pct']):+.3f}%.",
        f"- One-at-a-time threshold changes passing all conservative promotion checks: **{len(accepted)}**.",
        "",
        "That train-to-later-period reversal is direct evidence of overfitting risk, not evidence that "
        "OI is useless. It says these 23 already-inspected sessions cannot estimate ten separate OI knobs.",
        "",
        "## What was tested",
        "",
        f"- Corrected source SHA-256 (before and after): `{metadata['source_sha256']}`.",
        f"- Pinned inputs: {metadata['candidate_signals']:,} candidate signals and "
        f"{metadata['forward_paths']:,} exit paths, {metadata['first_day']} through "
        f"{metadata['last_day']}.",
        f"- Chronological diagnostic split: TRAIN before {split_day} and TEST from {split_day}.",
        f"- Fixed threshold grid (%): {', '.join(f'{x:g}' for x in THRESHOLD_GRID)}.",
        "- For every trial, only `oi_change_pct` changed. Price/volume/EMA/candle filters, "
        "ranking, entry caps, confirmation, stops, targets, point-in-time contract roll and 5-bps cost stayed fixed.",
        "- Each threshold was evaluated per timing and side (09:25/30/35/40/45 × LONG/SHORT), "
        "plus one common threshold and one LONG/one SHORT policy.",
        "- PF was never used alone: net, expectancy, fills, drawdown, changed decisions and "
        "day-block bootstrap diagnostics were retained.",
        "",
        "Baseline replay parity passed: the research replay has the same published trade keys, "
        f"and maximum absolute return difference is `{parity['max_abs_return_difference']:.3e}`.",
        "",
        "## Baseline result",
        "",
        "| Period | Sessions | Fills | Wins | Losses | PF | Net % | Expectancy % | Max DD % |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for period in ("TRAIN", "TEST", "ALL"):
        row = lookup.loc[period]
        lines.append(
            f"| {period} | {int(row['sessions'])} | {int(row['fills'])} | "
            f"{int(row['wins'])} | {int(row['losses'])} | {_fmt(row['trade_pf'])} | "
            f"{float(row['net_pct']):+.3f} | {_fmt(row['expectancy_pct'])} | "
            f"{_fmt(row['max_drawdown_pct'])} |"
        )

    lines += [
        "",
        "### Existing V6 thresholds and sample size",
        "",
        "| Signal | Side | Current OI % | Otherwise-eligible rows | Train fills | Test fills | All PF | All net % |",
        "|---|---|---:|---:|---:|---:|---:|---:|",
    ]
    for row in baseline_setup.to_dict("records"):
        lines.append(
            f"| {row['signal_end']} | {row['side']} | {row['baseline_threshold']:.3f} | "
            f"{int(row['otherwise_eligible_rows'])} | {int(row['train_fills'])} | "
            f"{int(row['test_fills'])} | {_fmt(row['all_trade_pf'])} | "
            f"{float(row['all_net_pct']):+.3f} |"
        )

    lines += [
        "",
        "Two cells currently have zero fills and one has only one fill. No cell has 20 fills. "
        "A high or infinite per-cell PF here can be created by one trade or by deleting one loser.",
        "",
        "## Train-selected per-slot thresholds: the overfit test",
        "",
        "For each of the ten cells, the threshold with the highest TRAIN net was selected; ties "
        "went to the value closest to V6. Because portfolio return is additive here, this also "
        "maximizes the ten-cell book's TRAIN net over this declared grid. TEST was not used to choose it.",
        "",
        "| Setup | V6 OI % | Train-selected OI % | Train fills/net % | Later fills/net % |",
        "|---|---:|---:|---:|---:|",
    ]
    for row in choices.to_dict("records"):
        lines.append(
            f"| {row['setup_id']} | {row['baseline_threshold']:.3f} | "
            f"{row['selected_threshold']:.3f} | {int(row['train_fills'])} / "
            f"{float(row['train_net_pct']):+.3f} | {int(row['test_fills'])} / "
            f"{float(row['test_net_pct']):+.3f} |"
        )

    lines += [
        "",
        "| Book | Period | Fills | PF | Net % | Expectancy % | Max DD % |",
        "|---|---|---:|---:|---:|---:|---:|",
    ]
    for policy in (
        "V6_BASELINE",
        "PER_SETUP_MAX_TRAIN_NET",
        "SHADOW_TWO_LONG_THRESHOLDS",
    ):
        rows = policy_results.loc[policy_results["policy"].eq(policy)]
        for period in ("TRAIN", "TEST", "ALL"):
            row = rows.loc[rows["period"].eq(period)].iloc[0]
            lines.append(
                f"| {policy} | {period} | {int(row['fills'])} | {_fmt(row['trade_pf'])} | "
                f"{float(row['net_pct']):+.3f} | {_fmt(row['expectancy_pct'])} | "
                f"{_fmt(row['max_drawdown_pct'])} |"
            )

    shadow_result = policy_results.loc[
        policy_results["policy"].eq("SHADOW_TWO_LONG_THRESHOLDS")
    ].set_index("period")
    shadow_rows = oat.loc[
        (
            oat["setup_id"].eq("0936_LONG")
            & np.isclose(oat["trial_threshold"], 0.15)
        )
        | (
            oat["setup_id"].eq("0941_LONG")
            & np.isclose(oat["trial_threshold"], 0.075)
        )
    ].sort_values("setup_id")
    lines += [
        "",
        "## Research-only shadow hypotheses",
        "",
        "Only two changes are sensible enough to shadow, but neither is approved for V6:",
        "",
        "| Setup | V6 to shadow OI % | Changed days train/test | Change train PF | Change train net % | Change later PF | Change later net % |",
        "|---|---:|---:|---:|---:|---:|---:|",
    ]
    for row in shadow_rows.to_dict("records"):
        lines.append(
            f"| {row['setup_id']} | {row['baseline_threshold']:.3f} to "
            f"{row['trial_threshold']:.3f} | "
            f"{int(row['train_changed_days'])}/{int(row['test_changed_days'])} | "
            f"{_fmt(row['train_pf_delta'])} | {float(row['train_net_delta']):+.3f} | "
            f"{_fmt(row['test_pf_delta'])} | {float(row['test_net_delta']):+.3f} |"
        )
    lines += [
        "",
        f"Combined, they move full-sample PF from {_fmt(baseline_all['trade_pf'])} to "
        f"{_fmt(shadow_result.loc['ALL', 'trade_pf'])} and net from "
        f"{float(baseline_all['net_pct']):+.3f}% to "
        f"{float(shadow_result.loc['ALL', 'net_pct']):+.3f}%. They changed only three "
        "earlier-period decisions and zero later-period decisions, so the later curve is "
        "bit-for-bit identical to V6. That is no prospective confirmation.",
        "",
        "The important failed relaxation is `0946_SHORT: 0.75% to 0.075%`: it added "
        "+0.222% in TRAIN but -4.200% in the later period. Do not lower it in V6.",
    ]

    train_positive = oat.loc[oat["train_net_delta"] > 1e-12].sort_values(
        ["train_net_delta", "train_pf_delta"], ascending=False
    ).head(10)
    lines += [
        "",
        "## Strongest one-at-a-time training improvements (overfit warning)",
        "",
        "These are hypothesis leads only. A later-period delta of zero usually means the threshold "
        "did not change any later decision; it is not positive validation.",
        "",
        "| Setup | V6→trial OI % | Changed days train/test | Δ train PF | Δ train net % | Δ later PF | Δ later net % | Later bootstrap 95% CI |",
        "|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    if train_positive.empty:
        lines.append("| none | | | | | | | |")
    else:
        for row in train_positive.to_dict("records"):
            lines.append(
                f"| {row['setup_id']} | {row['baseline_threshold']:.3f}→{row['trial_threshold']:.3f} | "
                f"{int(row['train_changed_days'])}/{int(row['test_changed_days'])} | "
                f"{_fmt(row['train_pf_delta'])} | {float(row['train_net_delta']):+.3f} | "
                f"{_fmt(row['test_pf_delta'])} | {float(row['test_net_delta']):+.3f} | "
                f"[{float(row['test_bootstrap_delta_low']):+.3f}, "
                f"{float(row['test_bootstrap_delta_high']):+.3f}] |"
            )

    simple = policy_results.loc[
        policy_results["policy"].isin(["BEST_UNIFORM_ON_TRAIN", "BEST_SIDE_PAIR_ON_TRAIN"])
    ]
    lines += [
        "",
        "## Simpler OI policies",
        "",
        "The best common threshold and best LONG/SHORT pair were selected by TRAIN net while "
        f"retaining at least {MIN_POLICY_TRADE_RETENTION:.0%} of baseline TRAIN fills.",
        "",
        "| Policy | Detail | Period | Fills | PF | Net % |",
        "|---|---|---|---:|---:|---:|",
    ]
    for row in simple.to_dict("records"):
        lines.append(
            f"| {row['policy']} | {row['detail']} | {row['period']} | {int(row['fills'])} | "
            f"{_fmt(row['trade_pf'])} | {float(row['net_pct']):+.3f} |"
        )

    lines += [
        "",
        "## Why this is not promotion evidence",
        "",
        "1. **Only 23 sessions / 64 baseline fills.** Ten separate threshold parameters are "
        "unsupported by the per-cell counts above.",
        "2. **The dates are not pristine out-of-sample data.** V6 came from a full-history "
        "optimizer completed through 2026-08-11; ten of the twelve labelled TRAIN days overlap "
        "that selection window. The later dates have also already been inspected, so TEST here "
        "is a chronological diagnostic, not a fresh final holdout.",
        "3. **Multiple testing is enormous.** Fourteen choices across ten cells imply "
        f"`14^10 = {14 ** 10:,}` possible books. Maximizing PF across them would manufacture winners.",
        "4. **The cache is OI-prefiltered.** It contains only rising OI of at least 0.05%; it "
        "cannot establish whether OI improves selection versus a true no-OI baseline.",
        "5. **PF rises when losers are deleted.** The one-at-a-time table records changed days "
        "so removal of one losing observation cannot masquerade as robust improvement.",
        "6. **Returns are additive trade percentages**, not a lot-sized, capital-constrained account curve.",
        "",
        "## Promotion rule and next test",
        "",
        "Keep V6 unchanged. If an OI candidate is frozen for prospective testing, require all of:",
        "",
        "- higher PF, net profit and expectancy than V6 on untouched forward dates;",
        "- at least 70% of baseline fills, adequate wins and losses, and no result driven by one day/stock;",
        "- positive paired day-block improvement with its lower confidence bound above zero;",
        "- robustness at neighbouring thresholds and at 10/15/20-bps costs;",
        "- similar improvement across at least two new contract-expiry regimes;",
        "- at least 20 forward fills in every changed cell and roughly 100 total forward fills;",
        "- no material drawdown deterioration; and explicit user approval before editing corrected V6.",
        "",
        "For the cleanest answer to whether OI itself adds value, build a separate ungated signal "
        "superset and pre-register four comparisons: no OI condition, rising OI only, one common "
        "threshold, and current V6. Do not edit the corrected production backtest for that experiment.",
        "",
        "## Artifacts",
        "",
        f"- `{SWEEP_PATH.name}`: every per-setup threshold and period.",
        f"- `{OAT_PATH.name}`: full-book effect of changing one cell at a time.",
        f"- `{POLICY_SWEEP_PATH.name}`: all uniform and LONG/SHORT threshold policies.",
        f"- `{POLICY_RESULT_PATH.name}`: baseline and selected diagnostic books.",
        f"- `{MANIFEST_PATH.name}` and `{PARITY_PATH.name}`: pinned hashes and exact replay proof.",
        f"- `{SHADOW_BOOK_PATH.name}`: frozen research-only shadow definitions; not approved for production.",
        f"- `{SHADOW_DAILY_PATH.name}` and `{SHADOW_TRADES_PATH.name}`: shadow comparison evidence.",
        "",
        "**Final verdict: OI-threshold tuning is useful for generating hypotheses, but no change "
        "currently has enough independent evidence to replace corrected V6.**",
        "",
    ]
    return "\n".join(lines)


def run(split_day: date, cost_bps: float) -> int:
    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    signals, paths, days, metadata = _load_pinned_inputs()
    periods = _period_days(days, split_day)

    setups = tuple(corrected.ACTIVE_SETUPS)
    baseline_choices = {setup.setup_id: float(setup.oi_change_pct) for setup in setups}
    thresholds_by_setup = {
        setup.setup_id: tuple(sorted(set(THRESHOLD_GRID + (float(setup.oi_change_pct),))))
        for setup in setups
    }

    audit_by_threshold: dict[tuple[str, float], pd.DataFrame] = {}
    sweep_rows: list[dict[str, Any]] = []
    print(f"[LOAD] {len(signals):,} pinned candidates, {len(days)} sessions", flush=True)
    for setup in setups:
        for threshold in thresholds_by_setup[setup.setup_id]:
            candidate_setup = replace(setup, oi_change_pct=float(threshold))
            audit = replay.replay_setups(
                signals,
                paths,
                cost_bps=cost_bps,
                setups=(candidate_setup,),
            )
            audit_by_threshold[(setup.setup_id, float(threshold))] = audit
            for period, period_day_list in periods.items():
                row = {
                    "setup_id": setup.setup_id,
                    "signal_end": setup.signal_end,
                    "confirmation_end": setup.confirmation_end,
                    "side": setup.side,
                    "baseline_threshold": float(setup.oi_change_pct),
                    "trial_threshold": float(threshold),
                    "period": period,
                }
                row.update(_metrics(audit, period_day_list))
                sweep_rows.append(row)
    sweep = pd.DataFrame(sweep_rows)

    baseline_audit = _combine_audits(audit_by_threshold, baseline_choices)
    parity = _assert_baseline_parity(baseline_audit, metadata)
    baseline_result_rows = []
    for period, period_day_list in periods.items():
        baseline_result_rows.append({"period": period, **_metrics(baseline_audit, period_day_list)})
    baseline_results = pd.DataFrame(baseline_result_rows)
    baseline_metric = baseline_results.set_index("period")

    baseline_setup_rows: list[dict[str, Any]] = []
    for setup in setups:
        base = audit_by_threshold[(setup.setup_id, float(setup.oi_change_pct))]
        no_oi_rows = _candidate_rows_without_oi(signals, setup)
        row: dict[str, Any] = {
            "setup_id": setup.setup_id,
            "signal_end": setup.signal_end,
            "confirmation_end": setup.confirmation_end,
            "side": setup.side,
            "baseline_threshold": float(setup.oi_change_pct),
            "otherwise_eligible_rows": int(len(no_oi_rows)),
            "otherwise_eligible_min_oi_pct": float(no_oi_rows["oi_change_pct"].min()) if len(no_oi_rows) else float("nan"),
            "otherwise_eligible_max_oi_pct": float(no_oi_rows["oi_change_pct"].max()) if len(no_oi_rows) else float("nan"),
        }
        for period, period_day_list in periods.items():
            row.update(_prefixed(_metrics(base, period_day_list), period.lower()))
        baseline_setup_rows.append(row)
    baseline_setup = pd.DataFrame(baseline_setup_rows)

    # Choose each threshold strictly on TRAIN net. Ties prefer the V6-nearest
    # value, so unchanged behavior does not create a gratuitous parameter move.
    choice_rows: list[dict[str, Any]] = []
    train_choices: dict[str, float] = {}
    for setup in setups:
        candidates = sweep.loc[
            sweep["setup_id"].eq(setup.setup_id) & sweep["period"].eq("TRAIN")
        ].copy()
        candidates["distance_from_v6"] = (
            candidates["trial_threshold"] - float(setup.oi_change_pct)
        ).abs()
        best_net = float(candidates["net_pct"].max())
        candidates = candidates.loc[
            np.isclose(candidates["net_pct"], best_net, rtol=0.0, atol=1e-10)
        ]
        selected = candidates.sort_values(
            ["distance_from_v6", "fills", "trial_threshold"],
            ascending=[True, False, True],
            kind="stable",
        ).iloc[0]
        threshold = float(selected["trial_threshold"])
        train_choices[setup.setup_id] = threshold
        train_metrics = _metrics(
            audit_by_threshold[(setup.setup_id, threshold)], periods["TRAIN"]
        )
        test_metrics = _metrics(
            audit_by_threshold[(setup.setup_id, threshold)], periods["TEST"]
        )
        choice_rows.append(
            {
                "setup_id": setup.setup_id,
                "signal_end": setup.signal_end,
                "side": setup.side,
                "baseline_threshold": float(setup.oi_change_pct),
                "selected_threshold": threshold,
                **_prefixed(train_metrics, "train"),
                **_prefixed(test_metrics, "test"),
            }
        )
    choices = pd.DataFrame(choice_rows)
    tuned_audit = _combine_audits(audit_by_threshold, train_choices)

    shadow_choices = dict(baseline_choices)
    shadow_choices.update(SHADOW_THRESHOLD_CHOICES)
    shadow_audit = _combine_audits(audit_by_threshold, shadow_choices)

    oat_rows: list[dict[str, Any]] = []
    candidate_index = 0
    for setup in setups:
        for threshold in thresholds_by_setup[setup.setup_id]:
            if np.isclose(threshold, setup.oi_change_pct, rtol=0.0, atol=1e-12):
                continue
            candidate_index += 1
            candidate_choices = dict(baseline_choices)
            candidate_choices[setup.setup_id] = float(threshold)
            candidate = _combine_audits(audit_by_threshold, candidate_choices)
            row = {
                "setup_id": setup.setup_id,
                "signal_end": setup.signal_end,
                "side": setup.side,
                "baseline_threshold": float(setup.oi_change_pct),
                "trial_threshold": float(threshold),
            }
            candidate_metrics: dict[str, dict[str, Any]] = {}
            for period, period_day_list in periods.items():
                metrics = _metrics(candidate, period_day_list)
                candidate_metrics[period] = metrics
                row.update(_prefixed(metrics, period.lower()))
                base = baseline_metric.loc[period]
                row[f"{period.lower()}_pf_delta"] = _finite_delta(
                    float(metrics["trade_pf"]), float(base["trade_pf"])
                )
                row[f"{period.lower()}_net_delta"] = float(metrics["net_pct"] - base["net_pct"])
                row[f"{period.lower()}_mdd_delta"] = float(
                    metrics["max_drawdown_pct"] - base["max_drawdown_pct"]
                )
                changed_selections, changed_days = _selection_change(
                    baseline_audit, candidate, period_day_list, setup.setup_id
                )
                row[f"{period.lower()}_changed_selections"] = changed_selections
                row[f"{period.lower()}_changed_days"] = changed_days
            train_ci = _paired_bootstrap_total_ci(
                baseline_audit,
                candidate,
                periods["TRAIN"],
                seed=BOOTSTRAP_SEED + candidate_index * 2,
            )
            test_ci = _paired_bootstrap_total_ci(
                baseline_audit,
                candidate,
                periods["TEST"],
                seed=BOOTSTRAP_SEED + candidate_index * 2 + 1,
            )
            row["train_bootstrap_delta_low"], row["train_bootstrap_delta_high"] = train_ci
            row["test_bootstrap_delta_low"], row["test_bootstrap_delta_high"] = test_ci
            row["train_pf_and_net_improve"] = bool(
                row["train_pf_delta"] > 0 and row["train_net_delta"] > 0
            )
            row["test_pf_net_mdd_improve"] = bool(
                row["test_pf_delta"] > 0
                and row["test_net_delta"] > 0
                and row["test_mdd_delta"] >= 0
            )
            row["enough_changed_days"] = bool(
                row["train_changed_days"] >= MIN_CHANGED_DAYS_PER_PERIOD
                and row["test_changed_days"] >= MIN_CHANGED_DAYS_PER_PERIOD
            )
            row["retains_70pct_test_fills"] = bool(
                candidate_metrics["TEST"]["fills"]
                >= math.ceil(MIN_POLICY_TRADE_RETENTION * float(baseline_metric.loc["TEST", "fills"]))
            )
            row["positive_test_bootstrap_lower_bound"] = bool(test_ci[0] > 0)
            row["passes_promotion_screen"] = bool(
                row["train_pf_and_net_improve"]
                and row["test_pf_net_mdd_improve"]
                and row["enough_changed_days"]
                and row["retains_70pct_test_fills"]
                and row["positive_test_bootstrap_lower_bound"]
            )
            oat_rows.append(row)
    oat = pd.DataFrame(oat_rows)
    accepted = oat.loc[oat["passes_promotion_screen"]].copy()

    # Simpler policies: one threshold for all cells, then one for each side.
    policy_sweep_rows: list[dict[str, Any]] = []
    for long_threshold in THRESHOLD_GRID:
        for short_threshold in THRESHOLD_GRID:
            policy_type = "UNIFORM" if long_threshold == short_threshold else "SIDE_PAIR"
            policy_choices = {
                setup.setup_id: float(long_threshold if setup.side == "LONG" else short_threshold)
                for setup in setups
            }
            audit = _combine_audits(audit_by_threshold, policy_choices)
            train = _metrics(audit, periods["TRAIN"])
            test = _metrics(audit, periods["TEST"])
            all_metrics = _metrics(audit, periods["ALL"])
            policy_sweep_rows.append(
                {
                    "policy_type": policy_type,
                    "long_threshold": float(long_threshold),
                    "short_threshold": float(short_threshold),
                    **_prefixed(train, "train"),
                    **_prefixed(test, "test"),
                    **_prefixed(all_metrics, "all"),
                }
            )
    policy_sweep = pd.DataFrame(policy_sweep_rows)
    min_train_fills = math.ceil(
        MIN_POLICY_TRADE_RETENTION * float(baseline_metric.loc["TRAIN", "fills"])
    )
    eligible_policy = policy_sweep.loc[policy_sweep["train_fills"] >= min_train_fills]
    uniform = eligible_policy.loc[eligible_policy["policy_type"].eq("UNIFORM")]
    side_pair = eligible_policy.loc[eligible_policy["policy_type"].eq("SIDE_PAIR")]
    best_uniform = uniform.sort_values(
        ["train_net_pct", "train_trade_pf"], ascending=False, kind="stable"
    ).iloc[0]
    best_side_pair = side_pair.sort_values(
        ["train_net_pct", "train_trade_pf"], ascending=False, kind="stable"
    ).iloc[0]

    policy_definitions = [
        ("V6_BASELINE", "distinct V6 thresholds", baseline_audit),
        (
            "PER_SETUP_MAX_TRAIN_NET",
            json.dumps(train_choices, sort_keys=True),
            tuned_audit,
        ),
        (
            "SHADOW_TWO_LONG_THRESHOLDS",
            "0936_LONG=0.15%, 0941_LONG=0.075%; research only",
            shadow_audit,
        ),
    ]
    for label, selected in (
        ("BEST_UNIFORM_ON_TRAIN", best_uniform),
        ("BEST_SIDE_PAIR_ON_TRAIN", best_side_pair),
    ):
        long_threshold = float(selected["long_threshold"])
        short_threshold = float(selected["short_threshold"])
        selected_choices = {
            setup.setup_id: (long_threshold if setup.side == "LONG" else short_threshold)
            for setup in setups
        }
        policy_definitions.append(
            (
                label,
                f"LONG={long_threshold:g}%, SHORT={short_threshold:g}%",
                _combine_audits(audit_by_threshold, selected_choices),
            )
        )
    policy_result_rows: list[dict[str, Any]] = []
    for policy, detail, audit in policy_definitions:
        for period, period_day_list in periods.items():
            policy_result_rows.append(
                {
                    "policy": policy,
                    "detail": detail,
                    "period": period,
                    **_metrics(audit, period_day_list),
                }
            )
    policy_results = pd.DataFrame(policy_result_rows)

    source_hash_after = _sha256(Path(corrected.__file__).resolve())
    if source_hash_after != metadata["source_sha256"]:
        raise AssertionError("Corrected V6 source changed while the research harness was running.")
    metadata["source_sha256_after"] = source_hash_after

    _write_csv(sweep, SWEEP_PATH)
    _write_csv(oat, OAT_PATH)
    _write_csv(policy_sweep, POLICY_SWEEP_PATH)
    _write_csv(policy_results, POLICY_RESULT_PATH)
    _write_csv(choices, CHOICES_PATH)
    _write_csv(baseline_setup, BASELINE_SETUP_PATH)
    shadow_trade_output = shadow_audit.copy()
    shadow_trade_output["research_policy"] = "SHADOW_TWO_LONG_THRESHOLDS_NOT_APPROVED"
    _write_csv(shadow_trade_output, SHADOW_TRADES_PATH)

    shadow_daily = pd.DataFrame({"day": days})
    shadow_daily["period"] = np.where(
        shadow_daily["day"].lt(split_day), "TRAIN", "TEST"
    )
    shadow_daily["baseline_net_pct"] = _daily_returns(baseline_audit, days)
    shadow_daily["shadow_net_pct"] = _daily_returns(shadow_audit, days)
    shadow_daily["incremental_net_pct"] = (
        shadow_daily["shadow_net_pct"] - shadow_daily["baseline_net_pct"]
    )
    _write_csv(shadow_daily, SHADOW_DAILY_PATH)

    shadow_setup_book = []
    for setup in setups:
        threshold = float(shadow_choices[setup.setup_id])
        payload = asdict(replace(setup, oi_change_pct=threshold))
        payload["status"] = "RESEARCH_ONLY_NOT_APPROVED"
        payload["changed_from_v6"] = not np.isclose(
            threshold, float(setup.oi_change_pct), rtol=0.0, atol=1e-12
        )
        shadow_setup_book.append(payload)
    common.atomic_write_json(
        SHADOW_BOOK_PATH,
        {
            "status": "RESEARCH_ONLY_NOT_APPROVED",
            "source_corrected_sha256": metadata["source_sha256"],
            "thresholds": shadow_choices,
            "setups": shadow_setup_book,
        },
    )
    common.atomic_write_json(PARITY_PATH, parity)

    manifest = {
        "research_version": RESEARCH_VERSION,
        "generated_at_ist": common.now_ist().isoformat(timespec="seconds"),
        "decision": "DO_NOT_CHANGE_CORRECTED_V6",
        "classification": "EXPLORATORY_SENSITIVITY_NOT_PRISTINE_OOS",
        "source": metadata,
        "parameters": {
            "split_day": str(split_day),
            "cost_bps": float(cost_bps),
            "threshold_grid_pct": list(THRESHOLD_GRID),
            "min_policy_trade_retention": MIN_POLICY_TRADE_RETENTION,
            "min_changed_days_per_period": MIN_CHANGED_DAYS_PER_PERIOD,
            "bootstrap_replicates": BOOTSTRAP_REPLICATES,
            "bootstrap_seed": BOOTSTRAP_SEED,
        },
        "only_parameter_changed": "SetupSpec.oi_change_pct",
        "cache_limit": "Already requires rising OI and oi_change_pct >= 0.05%; not a no-OI ablation.",
        "baseline_parity": parity,
        "baseline_thresholds": baseline_choices,
        "train_selected_thresholds": train_choices,
        "shadow_thresholds_not_approved": shadow_choices,
        "promotion_candidates_passing": int(len(accepted)),
        "artifacts": [
            str(path)
            for path in (
                REPORT_PATH,
                PARITY_PATH,
                SWEEP_PATH,
                OAT_PATH,
                POLICY_SWEEP_PATH,
                POLICY_RESULT_PATH,
                CHOICES_PATH,
                BASELINE_SETUP_PATH,
                SHADOW_BOOK_PATH,
                SHADOW_DAILY_PATH,
                SHADOW_TRADES_PATH,
            )
        ],
    }
    common.atomic_write_json(MANIFEST_PATH, manifest)
    common.atomic_write_text(
        REPORT_PATH,
        _render_report(
            split_day=split_day,
            metadata=metadata,
            parity=parity,
            baseline_results=baseline_results,
            baseline_setup=baseline_setup,
            choices=choices,
            oat=oat,
            policy_results=policy_results,
            accepted=accepted,
        ),
    )

    print(
        f"[BASELINE] fills={int(baseline_metric.loc['ALL', 'fills'])} "
        f"PF={baseline_metric.loc['ALL', 'trade_pf']:.6f} "
        f"net={baseline_metric.loc['ALL', 'net_pct']:+.6f}%",
        flush=True,
    )
    tuned_results = policy_results.loc[
        policy_results["policy"].eq("PER_SETUP_MAX_TRAIN_NET")
    ].set_index("period")
    print(
        f"[OVERFIT] train PF {baseline_metric.loc['TRAIN', 'trade_pf']:.3f} -> "
        f"{tuned_results.loc['TRAIN', 'trade_pf']:.3f}; later PF "
        f"{baseline_metric.loc['TEST', 'trade_pf']:.3f} -> "
        f"{tuned_results.loc['TEST', 'trade_pf']:.3f}",
        flush=True,
    )
    print(f"[PROMOTION] candidates passing={len(accepted)}", flush=True)
    print(f"[WROTE] {REPORT_PATH}", flush=True)
    return 0


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--split-day", default=str(DEFAULT_SPLIT_DAY))
    parser.add_argument("--cost-bps", type=float, default=DEFAULT_COST_BPS)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    return run(pd.Timestamp(args.split_day).date(), float(args.cost_bps))


if __name__ == "__main__":
    raise SystemExit(main())
