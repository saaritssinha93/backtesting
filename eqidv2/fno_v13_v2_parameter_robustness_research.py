"""Frozen, cache-only robustness screen around V13-v2's existing core.

This is deliberately a *research-only* companion to
``fno_v13_corrected_v2_backtest.py``.  It neither imports any writable V13-v2
paths nor changes V6/V13-v2 source/configuration.  It reads the already-built
strict-V6-confirmation signal/path caches and asks a bounded question:

    Can one causal, nearby five-minute selection or one-minute entry/exit
    parameter change improve BOTH profitability and trade count in both
    chronological segments?

The frozen evaluation data end on 2026-09-01.  September 2 is intentionally
absent from all candidate ranking and screening.  Results are descriptive:
V13-v2 itself contains research-selected elements, so its ORIGINAL_TEST is a
chronological consistency segment, not an untouched promotion holdout.

The cache contains only signals that already passed the V6 directional
one-minute confirmation.  Therefore this script can test causal filters using
stored confirmation values and deterministic stop-entry buffers, but it cannot
honestly claim to test a *relaxed* confirmation-direction rule.  That needs a
separate, fresh pre-gate cache rebuild.
"""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass, replace
from datetime import date
from pathlib import Path
from typing import Any, Callable, Iterable

import numpy as np
import pandas as pd

import fno_oi_common as common
import fno_oi_ema_confirm_sweep as sw
import fno_v5_hybrid_backtest as replay
import fno_v6_corrected_backtest as v6
import fno_v13_corrected_v2_backtest as v13


STUDY_VERSION = "FNO_V13_V2_PARAMETER_ROBUSTNESS_20260903"
BASELINE_POLICY_NAME = "V13_V2_COMBINED_SHADOW"
SPLIT_DAY = date(2026, 8, 14)
FROZEN_THROUGH_DAY = date(2026, 9, 1)
COSTS_BPS = (5.0, 10.0)
MIN_NET_IMPROVEMENT_PCT = 0.25
MIN_PF_IMPROVEMENT = 0.10
MAX_DD_WORSENING_PCT = 0.10
MIN_CHANGED_SESSIONS_PER_SEGMENT = 2
MIN_TOTAL_FILL_INCREASE = 2

RESULT_DIR = (
    common.FNO_ROOT
    / "strategy_research"
    / "v13_corrected_v2_parameter_robustness"
)
ALL_VARIANTS_PATH = RESULT_DIR / "all_bounded_variants.csv"
PASSED_VARIANTS_PATH = RESULT_DIR / "strictly_passed_variants.csv"
TRAIN_RANKED_PATH = RESULT_DIR / "train_ranked_apparent_winners.csv"
SUMMARY_PATH = RESULT_DIR / "FNO_V13_V2_PARAMETER_ROBUSTNESS.md"
PROVENANCE_PATH = RESULT_DIR / "parameter_robustness_provenance.json"


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def profit_factor(values: np.ndarray) -> float:
    profit = float(values[values > 0].sum()) if values.size else 0.0
    loss = float(-values[values < 0].sum()) if values.size else 0.0
    if loss > 0:
        return profit / loss
    return float("inf") if profit > 0 else float("nan")


def fmt(value: Any, digits: int = 3) -> str:
    if value is None:
        return ""
    if isinstance(value, (float, np.floating)):
        if math.isnan(float(value)):
            return "NA"
        if math.isinf(float(value)):
            return "INF"
        return f"{float(value):.{digits}f}"
    return str(value)


def markdown_table(frame: pd.DataFrame, columns: list[str]) -> list[str]:
    lines = ["| " + " | ".join(columns) + " |", "|" + "---|" * len(columns)]
    for row in frame[columns].to_dict("records"):
        lines.append("| " + " | ".join(fmt(row[col]) for col in columns) + " |")
    return lines


def load_frozen_signal_paths() -> tuple[pd.DataFrame, dict[int, dict[str, np.ndarray]], list[dict[str, Any]]]:
    """Read only the cache rows through 2026-09-01, choosing fullest seeds."""

    target_months = ("26AUG", "26SEP")
    parts: list[tuple[pd.DataFrame, dict[int, dict[str, np.ndarray]]]] = []
    records: list[dict[str, Any]] = []
    for month in target_months:
        choices: list[tuple[tuple[int, int, str], Path, pd.DataFrame, dict]] = []
        for parquet in sorted(v13.CACHE_DIR.glob(f"{month}_*.parquet")):
            stem = parquet.with_suffix("")
            if not stem.with_suffix(".npz").is_file():
                continue
            loaded = v6._load_cached(stem)
            if loaded is None:
                continue
            signals, paths = loaded
            days = pd.to_datetime(signals["day"]).dt.date
            keep = days.le(FROZEN_THROUGH_DAY)
            score = (int(days[keep].nunique()), int(keep.sum()), parquet.name)
            choices.append((score, stem, signals, paths))
        if not choices:
            raise RuntimeError(f"No usable V13-v2 {month} cache was found.")
        _, stem, signals, paths = max(choices, key=lambda item: item[0])
        signals = signals.copy()
        signals["day"] = pd.to_datetime(signals["day"]).dt.date
        signals = signals.loc[signals["day"].le(FROZEN_THROUGH_DAY)].copy()
        kept_sids = set(signals["sid"].astype(int))
        paths = {int(sid): value for sid, value in paths.items() if int(sid) in kept_sids}
        parts.append((signals, paths))
        records.append(
            {
                "contract_month": month,
                "cache_parquet": str(stem.with_suffix(".parquet").resolve()),
                "cache_npz": str(stem.with_suffix(".npz").resolve()),
                "cache_parquet_sha256": sha256(stem.with_suffix(".parquet")),
                "cache_npz_sha256": sha256(stem.with_suffix(".npz")),
                "candidate_rows_through_cutoff": int(len(signals)),
                "sessions_through_cutoff": sorted(map(str, signals["day"].unique())),
            }
        )
    signals, paths = v6.concat_regimes(parts)
    signals["day"] = pd.to_datetime(signals["day"]).dt.date
    days = sorted(signals["day"].unique())
    if not days or days[-1] != FROZEN_THROUGH_DAY or len(days) != 23:
        raise AssertionError(
            "Expected the frozen 23-session 2026-07-29..2026-09-01 sample; "
            f"got {days[0] if days else None}..{days[-1] if days else None} ({len(days)})."
        )
    return signals, paths, records


def periods(days: list[date]) -> dict[str, list[date]]:
    return {
        "ORIGINAL_TRAIN": [day for day in days if day < SPLIT_DAY],
        "ORIGINAL_TEST": [day for day in days if SPLIT_DAY <= day <= FROZEN_THROUGH_DAY],
        "ALL_FROZEN": list(days),
    }


def metric_row(audit: pd.DataFrame, days: list[date]) -> dict[str, Any]:
    if audit.empty:
        fills = np.array([], dtype=float)
        daily = np.zeros(len(days), dtype=float)
        orders = 0
    else:
        subset = audit.loc[audit["day"].isin(days)].copy()
        fills = subset.loc[subset["filled"], "net_return_pct"].to_numpy(float)
        daily = (
            subset.groupby("day")["net_return_pct"]
            .sum()
            .reindex(days, fill_value=0.0)
            .to_numpy(float)
        )
        orders = int(len(subset))
    curve = np.r_[0.0, np.cumsum(daily)]
    drawdown = curve - np.maximum.accumulate(curve)
    return {
        "orders": orders,
        "fills": int(fills.size),
        "wins": int((fills > 0).sum()),
        "trade_pf": profit_factor(fills),
        "net_pct": float(fills.sum()),
        "max_drawdown_pct": float(drawdown.min()) if drawdown.size else 0.0,
    }


def daily_returns(audit: pd.DataFrame, days: list[date]) -> np.ndarray:
    """Same daily accounting used by the screen, including zero-trade days."""

    if audit.empty:
        return np.zeros(len(days), dtype=float)
    return (
        audit.loc[audit["day"].isin(days)]
        .groupby("day")["net_return_pct"]
        .sum()
        .reindex(days, fill_value=0.0)
        .to_numpy(float)
    )


def exact_positive_signflip_pvalue(delta: np.ndarray) -> float:
    """Small-sample, one-sided paired sign-flip p-value for uplift.

    This is deliberately simple and conservative for the present exploratory
    screen. Twelve training sessions yield exactly 2**12 sign arrangements.
    It does not turn a backtest into proof; it merely makes the 213-way search
    penalty visible.
    """

    observed = float(delta.sum())
    if observed <= 0.0 or not len(delta):
        return 1.0
    masks = np.arange(1 << len(delta), dtype=np.uint32)[:, None]
    bits = (masks >> np.arange(len(delta), dtype=np.uint32)) & 1
    signed_sums = ((bits * 2.0 - 1.0) * delta[None, :]).sum(axis=1)
    return float((signed_sums >= observed - 1e-12).mean())


def benjamini_hochberg_qvalues(pvalues: pd.Series) -> pd.Series:
    """Return conservative BH-FDR q-values, preserving the input index."""

    count = int(len(pvalues))
    ordered = pvalues.sort_values(kind="stable")
    raw = ordered.to_numpy(float) * count / np.arange(1, count + 1, dtype=float)
    adjusted = np.minimum.accumulate(raw[::-1])[::-1]
    adjusted = np.clip(adjusted, 0.0, 1.0)
    return pd.Series(adjusted, index=ordered.index).reindex(pvalues.index)


def setup_with_change(
    base_setups: tuple[Any, ...], setup_id: str, field: str, value: Any
) -> tuple[Any, ...]:
    out: list[Any] = []
    seen = False
    for setup in base_setups:
        if setup.setup_id == setup_id:
            out.append(replace(setup, **{field: value}))
            seen = True
        else:
            out.append(setup)
    if not seen:
        raise KeyError(f"Unknown setup: {setup_id}")
    return tuple(out)


def select_signals(
    raw: pd.DataFrame,
    policy: v13.PolicySpec,
    filter_kind: str | None,
    filter_value: float | None,
) -> pd.DataFrame:
    frame = v13.apply_policy(raw, policy)
    if filter_kind is None:
        return frame
    if filter_kind == "confirmation_displacement":
        long_move = (frame["confirmation_close"] / frame["signal_close"] - 1.0) * 100.0
        short_move = (frame["signal_close"] / frame["confirmation_close"] - 1.0) * 100.0
        move = np.where(frame["side"].eq("LONG"), long_move, short_move)
        return frame.loc[pd.Series(move, index=frame.index).ge(float(filter_value))].copy()
    if filter_kind == "confirmation_body_floor":
        return frame.loc[frame["body_ratio"].ge(float(filter_value))].copy()
    if filter_kind == "confirmation_wick_cap":
        return frame.loc[frame["wick_ratio"].le(float(filter_value))].copy()
    raise ValueError(f"Unknown filter kind: {filter_kind}")


def replay_with_trigger_buffer(
    signals: pd.DataFrame,
    paths: dict[int, dict[str, np.ndarray]],
    setups: Iterable[Any],
    *,
    cost_bps: float,
    buffer_pct: float,
) -> pd.DataFrame:
    """Use a causal extra breakout buffer after the known confirmation bar."""

    parts: list[pd.DataFrame] = []
    fraction = float(buffer_pct) / 100.0
    for setup in setups:
        selected = replay.select_setup_rows(signals, setup).reset_index(drop=True)
        if selected.empty:
            continue
        selected = selected.copy()
        long_side = selected["side"].eq("LONG")
        selected["trigger"] = np.where(
            long_side,
            selected["confirmation_high"] * (1.0 + fraction),
            selected["confirmation_low"] * (1.0 - fraction),
        )
        selected["net_return_pct"] = sw.simulate_bracket(
            selected,
            paths,
            stop_pct=setup.stop_pct,
            target_pct=setup.target_pct,
            cost_bps=cost_bps,
        )
        selected["filled"] = selected["net_return_pct"].notna()
        selected["setup_id"] = setup.setup_id
        selected["entry_trigger_buffer_pct"] = float(buffer_pct)
        parts.append(selected)
    if not parts:
        return pd.DataFrame()
    return pd.concat(parts, ignore_index=True, sort=False).sort_values(
        ["day", "hhmm_int", "side", "setup_id", "tradingsymbol"], kind="stable"
    ).reset_index(drop=True)


@dataclass(frozen=True)
class Variant:
    name: str
    family: str
    description: str
    setup_id: str | None = None
    field: str | None = None
    value: Any = None
    policy_cap: float | None = None
    filter_kind: str | None = None
    filter_value: float | None = None
    trigger_buffer_pct: float | None = None


def build_variants(base_setups: tuple[Any, ...]) -> list[Variant]:
    """Pre-declared one-at-a-time neighbourhoods; no optimiser search."""

    variants: list[Variant] = []
    for setup in base_setups:
        sid = setup.setup_id
        fields: list[tuple[str, list[float], str]] = [
            (
                "price_change_pct",
                sorted({round(max(0.10, setup.price_change_pct - 0.10), 2), round(setup.price_change_pct + 0.10, 2)} - {round(setup.price_change_pct, 2)}),
                "5m signed price-move gate",
            ),
            (
                "oi_change_pct",
                sorted({round(max(0.05, setup.oi_change_pct - 0.05), 3), round(setup.oi_change_pct + 0.05, 3)} - {round(setup.oi_change_pct, 3)}),
                "5m OI-change gate",
            ),
            (
                "volume_ratio",
                sorted({round(max(0.80, setup.volume_ratio - 0.50), 2), round(setup.volume_ratio + 0.50, 2)} - {round(setup.volume_ratio, 2)}),
                "5m volume-ratio gate",
            ),
            (
                "body_ratio",
                sorted({round(max(0.10, setup.body_ratio - 0.10), 2), round(min(0.90, setup.body_ratio + 0.10), 2)} - {round(setup.body_ratio, 2)}),
                "1m confirmation body-ratio gate",
            ),
            (
                "max_wick_ratio",
                sorted({round(max(0.10, setup.max_wick_ratio - 0.10), 2), round(min(0.90, setup.max_wick_ratio + 0.10), 2)} - {round(setup.max_wick_ratio, 2)}),
                "1m confirmation adverse-wick cap",
            ),
        ]
        for field, values, label in fields:
            for value in values:
                variants.append(
                    Variant(
                        name=f"{sid}_{field}_{value}",
                        family="ONE_SETUP_FILTER_NEIGHBOUR",
                        description=f"{sid}: {label} -> {value}.",
                        setup_id=sid,
                        field=field,
                        value=float(value),
                    )
                )
        for picker in ("max_oi", "max_volume", "max_move", "max_liquidity"):
            if picker != setup.picker:
                variants.append(
                    Variant(
                        name=f"{sid}_picker_{picker}",
                        family="ONE_SETUP_RANKER",
                        description=f"{sid}: replace picker {setup.picker} with {picker}.",
                        setup_id=sid,
                        field="picker",
                        value=picker,
                    )
                )
        variants.append(
            Variant(
                name=f"{sid}_max_entries_{setup.max_entries + 1}",
                family="ONE_SETUP_FREQUENCY",
                description=(
                    f"{sid}: raise independent per-day selection count "
                    f"from {setup.max_entries} to {setup.max_entries + 1}."
                ),
                setup_id=sid,
                field="max_entries",
                value=int(setup.max_entries + 1),
            )
        )
        for field, delta, label in (
            ("stop_pct", -0.25, "tighter stop"),
            ("stop_pct", +0.25, "wider stop"),
            ("target_pct", -0.50, "nearer target"),
            ("target_pct", +0.50, "farther target"),
        ):
            value = round(max(0.25, float(getattr(setup, field)) + delta), 2)
            if value != float(getattr(setup, field)):
                variants.append(
                    Variant(
                        name=f"{sid}_{field}_{value}",
                        family="ONE_SETUP_BRACKET",
                        description=f"{sid}: {label}, {field} -> {value}%.",
                        setup_id=sid,
                        field=field,
                        value=value,
                    )
                )

    for cap in (0.90, 0.95, 1.05, 1.10):
        variants.append(
            Variant(
                name=f"global_oi_cap_{cap:.2f}",
                family="GLOBAL_OI_CAP_NEIGHBOUR",
                description=f"Global causal 5m OI cap -> {cap:.2f}% before ranking.",
                policy_cap=cap,
            )
        )
    for kind, values, label in (
        ("confirmation_displacement", (0.02, 0.05, 0.10), "side-signed confirmation displacement"),
        ("confirmation_body_floor", (0.50, 0.60, 0.70), "global confirmation body floor"),
        ("confirmation_wick_cap", (0.40, 0.30), "global confirmation adverse-wick cap"),
    ):
        for value in values:
            variants.append(
                Variant(
                    name=f"{kind}_{value:.2f}",
                    family="GLOBAL_CONFIRMATION_FILTER",
                    description=f"Causal 1m {label} -> {value:.2f}.",
                    filter_kind=kind,
                    filter_value=value,
                )
            )
    for buffer in (0.02, 0.05, 0.10):
        variants.append(
            Variant(
                name=f"entry_breakout_buffer_{buffer:.2f}",
                family="ONE_MINUTE_ENTRY_TRIGGER",
                description=(
                    "Require an additional causal breakout beyond the already "
                    f"known 1m confirmation extreme: {buffer:.2f}%."
                ),
                trigger_buffer_pct=buffer,
            )
        )
    names = [variant.name for variant in variants]
    if len(names) != len(set(names)):
        raise AssertionError("Variant names must be unique.")
    return variants


def run_variant(
    variant: Variant | None,
    raw_signals: pd.DataFrame,
    paths: dict[int, dict[str, np.ndarray]],
    base_policy: v13.PolicySpec,
    base_setups: tuple[Any, ...],
    *,
    cost_bps: float,
) -> pd.DataFrame:
    policy = base_policy
    setups = base_setups
    filter_kind = None
    filter_value = None
    trigger_buffer = None
    if variant is not None:
        if variant.policy_cap is not None:
            policy = replace(policy, max_oi_change_pct=float(variant.policy_cap))
        if variant.setup_id is not None:
            setups = setup_with_change(
                base_setups, variant.setup_id, str(variant.field), variant.value
            )
        filter_kind = variant.filter_kind
        filter_value = variant.filter_value
        trigger_buffer = variant.trigger_buffer_pct
    signals = select_signals(raw_signals, policy, filter_kind, filter_value)
    if trigger_buffer is not None:
        return replay_with_trigger_buffer(
            signals, paths, setups, cost_bps=cost_bps, buffer_pct=trigger_buffer
        )
    return replay.replay_setups(signals, paths, cost_bps=cost_bps, setups=setups)


def key_frame(audit: pd.DataFrame) -> pd.DataFrame:
    columns = ["day", "hhmm_int", "side", "setup_id", "tradingsymbol", "net_return_pct"]
    if audit.empty:
        return pd.DataFrame(columns=columns)
    frame = audit.loc[:, columns].copy()
    frame["day"] = frame["day"].astype(str)
    return frame.sort_values(columns[:-1], kind="stable").reset_index(drop=True)


def impact_metrics(base: pd.DataFrame, candidate: pd.DataFrame, days: list[date]) -> dict[str, int]:
    key_columns = ["day", "hhmm_int", "side", "setup_id", "tradingsymbol"]
    left = key_frame(base)
    right = key_frame(candidate)
    day_strings = {str(day) for day in days}
    left = left.loc[left["day"].isin(day_strings)]
    right = right.loc[right["day"].isin(day_strings)]
    left_indexed = left.set_index(key_columns)["net_return_pct"]
    right_indexed = right.set_index(key_columns)["net_return_pct"]
    left_keys = set(left_indexed.index)
    right_keys = set(right_indexed.index)
    changed = left_keys.symmetric_difference(right_keys)
    shared = left_keys & right_keys
    changed_returns = {
        item
        for item in shared
        if not np.isclose(
            float(left_indexed.loc[item]),
            float(right_indexed.loc[item]),
            rtol=0.0,
            atol=1e-12,
            equal_nan=True,
        )
    }
    changed_all = changed | changed_returns
    return {
        "changed_trade_keys": int(len(changed)),
        "changed_returns": int(len(changed_returns)),
        "affected_sessions": int(len({item[0] for item in changed_all})),
    }


def assess_variant(
    variant: Variant,
    baseline_5: pd.DataFrame,
    baseline_10: pd.DataFrame,
    candidate_5: pd.DataFrame,
    candidate_10: pd.DataFrame,
    segment_days: dict[str, list[date]],
) -> dict[str, Any]:
    row: dict[str, Any] = {
        "variant": variant.name,
        "family": variant.family,
        "description": variant.description,
        "setup_id": variant.setup_id,
        "field": variant.field,
        "value": variant.value,
        "policy_cap": variant.policy_cap,
        "filter_kind": variant.filter_kind,
        "filter_value": variant.filter_value,
        "trigger_buffer_pct": variant.trigger_buffer_pct,
    }
    passed_segments: list[bool] = []
    for segment, days in segment_days.items():
        base5 = metric_row(baseline_5, days)
        cand5 = metric_row(candidate_5, days)
        base10 = metric_row(baseline_10, days)
        cand10 = metric_row(candidate_10, days)
        impacts = impact_metrics(baseline_5, candidate_5, days)
        for prefix, values in (("base", base5), ("candidate", cand5)):
            for key, value in values.items():
                row[f"{segment}_{prefix}_{key}"] = value
        row[f"{segment}_delta_fills"] = cand5["fills"] - base5["fills"]
        row[f"{segment}_delta_pf"] = cand5["trade_pf"] - base5["trade_pf"]
        row[f"{segment}_delta_net_pct"] = cand5["net_pct"] - base5["net_pct"]
        row[f"{segment}_delta_dd_pct"] = (
            cand5["max_drawdown_pct"] - base5["max_drawdown_pct"]
        )
        row[f"{segment}_delta_net_10bps_pct"] = (
            cand10["net_pct"] - base10["net_pct"]
        )
        if segment == "ORIGINAL_TRAIN":
            delta = daily_returns(candidate_5, days) - daily_returns(baseline_5, days)
            row["ORIGINAL_TRAIN_signflip_pvalue"] = exact_positive_signflip_pvalue(delta)
        for key, value in impacts.items():
            row[f"{segment}_{key}"] = value
        if segment != "ALL_FROZEN":
            passed_segments.append(
                bool(
                    cand5["fills"] >= base5["fills"]
                    and cand5["net_pct"] >= base5["net_pct"] + MIN_NET_IMPROVEMENT_PCT
                    and cand5["trade_pf"] >= base5["trade_pf"] + MIN_PF_IMPROVEMENT
                    and cand5["max_drawdown_pct"] >= base5["max_drawdown_pct"] - MAX_DD_WORSENING_PCT
                    and cand10["net_pct"] > base10["net_pct"]
                    and impacts["affected_sessions"] >= MIN_CHANGED_SESSIONS_PER_SEGMENT
                )
            )
    all_fills_delta = row["ALL_FROZEN_delta_fills"]
    row["strict_pass"] = bool(
        all(passed_segments) and all_fills_delta >= MIN_TOTAL_FILL_INCREASE
    )
    return row


def neighbour_key(row: pd.Series) -> str:
    """Parameter neighbourhood, defined without looking at outcome metrics."""

    family = str(row["family"])
    if pd.notna(row.get("setup_id")) and pd.notna(row.get("field")):
        return f"{family}|{row['setup_id']}|{row['field']}"
    if pd.notna(row.get("filter_kind")):
        return f"{family}|{row['filter_kind']}"
    if pd.notna(row.get("policy_cap")):
        return f"{family}|policy_cap"
    if pd.notna(row.get("trigger_buffer_pct")):
        return f"{family}|trigger_buffer"
    return f"{family}|singleton|{row['variant']}"


def add_train_only_checks(results: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Rank solely on TRAIN, then append test and multiplicity diagnostics.

    No test metric participates in rank construction. The later test fields
    expose the chronological outcome after the TRAIN-only rank is frozen.
    """

    out = results.copy()
    out["train_rank_by_net_all_variants"] = (
        out["ORIGINAL_TRAIN_delta_net_pct"]
        .rank(method="first", ascending=False)
        .astype(int)
    )
    out["train_bh_fdr_qvalue"] = benjamini_hochberg_qvalues(
        out["ORIGINAL_TRAIN_signflip_pvalue"]
    )
    out["neighbour_key"] = out.apply(neighbour_key, axis=1)
    supportive = (
        out["ORIGINAL_TRAIN_delta_net_pct"].ge(MIN_NET_IMPROVEMENT_PCT)
        & out["ORIGINAL_TRAIN_delta_pf"].ge(0.0)
        & out["ORIGINAL_TRAIN_delta_fills"].ge(0)
    )
    out["neighbour_train_support_count"] = 0
    for _, group in out.groupby("neighbour_key", sort=False):
        for index in group.index:
            others = group.index.drop(index)
            out.loc[index, "neighbour_train_support_count"] = int(
                supportive.loc[others].sum()
            )
    out["neighbour_stability_pass"] = out["neighbour_train_support_count"].ge(1)
    out["train_preselect_pass"] = (
        out["ORIGINAL_TRAIN_delta_fills"].ge(1)
        & out["ORIGINAL_TRAIN_delta_net_pct"].ge(MIN_NET_IMPROVEMENT_PCT)
        & out["ORIGINAL_TRAIN_delta_pf"].ge(MIN_PF_IMPROVEMENT)
        & out["ORIGINAL_TRAIN_delta_dd_pct"].ge(-MAX_DD_WORSENING_PCT)
        & out["ORIGINAL_TRAIN_delta_net_10bps_pct"].gt(0.0)
    )
    # Assessed after the TRAIN-only rank; this is reporting, not a second
    # optimisation target.
    out["untouched_test_joint_confirm"] = (
        out["ORIGINAL_TEST_delta_fills"].ge(0)
        & out["ORIGINAL_TEST_delta_net_pct"].ge(MIN_NET_IMPROVEMENT_PCT)
        & out["ORIGINAL_TEST_delta_pf"].ge(MIN_PF_IMPROVEMENT)
        & out["ORIGINAL_TEST_delta_dd_pct"].ge(-MAX_DD_WORSENING_PCT)
        & out["ORIGINAL_TEST_delta_net_10bps_pct"].gt(0.0)
    )
    out["multiple_testing_pass"] = out["train_bh_fdr_qvalue"].le(0.10)
    out["exploratory_survivor"] = (
        out["train_preselect_pass"]
        & out["untouched_test_joint_confirm"]
        & out["neighbour_stability_pass"]
        & out["multiple_testing_pass"]
        & out["ALL_FROZEN_delta_fills"].ge(MIN_TOTAL_FILL_INCREASE)
    )
    train_ranked = out.sort_values(
        ["train_rank_by_net_all_variants", "variant"], kind="stable"
    ).head(15)
    return out, train_ranked


def render_report(
    baseline_rows: pd.DataFrame,
    results: pd.DataFrame,
    survivors: pd.DataFrame,
    train_ranked: pd.DataFrame,
    caches: list[dict[str, Any]],
) -> str:
    baseline_view = baseline_rows.loc[
        baseline_rows["segment"].isin(["ORIGINAL_TRAIN", "ORIGINAL_TEST", "ALL_FROZEN"])
    ]
    lines = [
        "# V13-v2 bounded parameter robustness screen",
        "",
        "## Verdict",
        "",
    ]
    if survivors.empty:
        lines += [
            "No tested change survived the exploratory train-rank, untouched-test, multiplicity and neighbourhood checks.",
            "",
            "That means no single nearby cached-data change supplied enough evidence to change V13-v2. This is an exploratory study, never a promotion decision.",
        ]
    else:
        lines += [
            f"{len(survivors)} candidate(s) survived the exploratory checks. They are not promotion-ready: the 213-way inspection and short history still require genuinely new data.",
        ]
    lines += [
        "",
        "## Frozen baseline",
        "",
        "Baseline is V13_V2_COMBINED_SHADOW, evaluated only through 2026-09-01 at 5 bps. September 2 was not used for candidate selection, ranking, or screening.",
        "",
    ]
    lines += markdown_table(
        baseline_view,
        ["segment", "sessions", "fills", "trade_pf", "net_pct", "max_drawdown_pct"],
    )
    lines += [
        "",
        "## Pre-declared test scope",
        "",
        f"- {len(results)} one-at-a-time variants: nearby 5m price/OI/volume gates, 1m body/wick/displacement gates, rankers, per-day entry counts, bracket distances, OI-cap neighbours and causal stop-entry buffers.",
        "- Every variant uses the identical cached candidate/path data and the same V13-v2 global OI policy unless the variant is explicitly an OI-cap neighbour.",
        "- No fit/optimizer selected a cross-parameter combination. The study does not test relaxed directional confirmation, because the frozen strict-V6 cache does not contain confirmation failures; doing so would be a new data rebuild, not an honest cache-only result.",
        "",
        "## Train-only ranking and checks",
        "",
        f"All {len(results)} variants are ranked by TRAIN net delta only. The rank does not use any test value. A train preselect requires +1 fill, at least +{MIN_NET_IMPROVEMENT_PCT:.2f}% net, +{MIN_PF_IMPROVEMENT:.2f} PF, no more than {MAX_DD_WORSENING_PCT:.2f}% extra drawdown, and a positive 10-bps net delta. The later ORIGINAL_TEST columns are an untouched chronological check after that train-only ranking.",
        "",
        "The simple multiplicity check is an exact 12-session paired sign-flip p-value on the TRAIN daily return delta, adjusted across all 213 variants using Benjamini-Hochberg FDR (q <= 0.10). The neighbourhood check requires a separate nearby setting from the same pre-declared parameter family to show non-negative TRAIN PF and at least +0.25% TRAIN net. Neither check establishes live tradability.",
        "",
    ]
    lines += ["## Apparent winners ranked by TRAIN only", ""]
    lines += markdown_table(
        train_ranked,
        [
            "train_rank_by_net_all_variants", "variant", "family",
            "ORIGINAL_TRAIN_delta_fills", "ORIGINAL_TRAIN_delta_pf",
            "ORIGINAL_TRAIN_delta_net_pct", "ORIGINAL_TRAIN_signflip_pvalue",
            "train_bh_fdr_qvalue", "neighbour_stability_pass",
            "ORIGINAL_TEST_delta_fills", "ORIGINAL_TEST_delta_pf",
            "ORIGINAL_TEST_delta_net_pct", "untouched_test_joint_confirm",
        ],
    )
    lines += [""]
    if not survivors.empty:
        lines += ["## Exploratory survivors", ""]
        lines += markdown_table(
            survivors,
            [
                "variant", "family", "ORIGINAL_TRAIN_delta_fills",
                "ORIGINAL_TRAIN_delta_pf", "ORIGINAL_TRAIN_delta_net_pct",
                "ORIGINAL_TEST_delta_fills", "ORIGINAL_TEST_delta_pf",
                "ORIGINAL_TEST_delta_net_pct", "ALL_FROZEN_delta_fills",
                "ALL_FROZEN_delta_net_pct",
            ],
        )
        lines += [""]
    lines += ["## Nearest misses (not candidates)", "", "The following table is diagnostic only. It does not relax the screen or justify a V13-v2 edit.", ""]
    near = results.copy()
    near["score"] = (
        near["ORIGINAL_TRAIN_delta_net_pct"].clip(lower=-5, upper=5)
        + near["ORIGINAL_TEST_delta_net_pct"].clip(lower=-5, upper=5)
        + 0.5 * near["ALL_FROZEN_delta_fills"].clip(lower=-10, upper=10)
    )
    near = near.sort_values("score", ascending=False).head(12)
    lines += markdown_table(
        near,
        [
            "variant", "family", "ORIGINAL_TRAIN_delta_fills",
            "ORIGINAL_TRAIN_delta_pf", "ORIGINAL_TRAIN_delta_net_pct",
            "ORIGINAL_TEST_delta_fills", "ORIGINAL_TEST_delta_pf",
            "ORIGINAL_TEST_delta_net_pct", "ALL_FROZEN_delta_fills",
            "ALL_FROZEN_delta_net_pct",
        ],
    )
    lines += [
        "",
        "## Logical interpretation",
        "",
        "The original gates are internally coherent: 5m EMA alignment, same-contract rising OI, signed price impulse and volume establish a directional candidate; the next 1m candle confirms direction and supplies a causal high/low stop-entry trigger; body/wick then reject indecisive confirmation candles; the bracket limits loss and captures continuation. A parameter is logical only if it preserves that causal sequence and has stable evidence across time, not just an attractive full-sample PF.",
        "",
        "A looser direction confirmation cannot be evaluated fairly from this cache because omitted failures have no path record. A market-at-confirmation-close entry also cannot be evaluated faithfully without next-bar open/slippage data. Those are separate research questions, not justification for a backtest-only improvement.",
        "",
        "## Integrity and reproducibility",
        "",
        f"- V13-v2 source and corrected-V6 source were hash-attested before and after this read-only study.",
        f"- Frozen sessions: 2026-07-29 through {FROZEN_THROUGH_DAY} (23 eligible sessions).",
        f"- Cache regimes read: {len(caches)}.",
        f"- Full results: `{ALL_VARIANTS_PATH}`.",
        f"- TRAIN-only ranking table: `{TRAIN_RANKED_PATH}`.",
        f"- Exploratory survivor file: `{PASSED_VARIANTS_PATH}`.",
        "",
    ]
    return "\n".join(lines)


def main() -> int:
    source_paths = {
        "v6_corrected": Path(v6.__file__).resolve(),
        "v13_v2": Path(v13.__file__).resolve(),
    }
    source_before = {name: sha256(path) for name, path in source_paths.items()}
    v13.validate_configuration()
    raw_signals, paths, cache_records = load_frozen_signal_paths()
    days = sorted(raw_signals["day"].unique())
    segment_days = periods(days)
    base_policy = v13.POLICIES[BASELINE_POLICY_NAME]
    base_setups = v13.policy_setups(base_policy)

    baseline_5 = run_variant(
        None, raw_signals, paths, base_policy, base_setups, cost_bps=5.0
    )
    baseline_10 = run_variant(
        None, raw_signals, paths, base_policy, base_setups, cost_bps=10.0
    )
    if int(baseline_5["filled"].sum()) != 70:
        raise AssertionError("Frozen V13-v2 baseline should reproduce 70 fills through Sep 1.")
    baseline_rows = pd.DataFrame(
        [
            {
                "segment": segment,
                "sessions": len(segment_days[segment]),
                **metric_row(baseline_5, segment_days[segment]),
            }
            for segment in segment_days
        ]
    )

    rows: list[dict[str, Any]] = []
    variants = build_variants(base_setups)
    for number, variant in enumerate(variants, start=1):
        candidate_5 = run_variant(
            variant, raw_signals, paths, base_policy, base_setups, cost_bps=5.0
        )
        candidate_10 = run_variant(
            variant, raw_signals, paths, base_policy, base_setups, cost_bps=10.0
        )
        rows.append(
            assess_variant(
                variant,
                baseline_5,
                baseline_10,
                candidate_5,
                candidate_10,
                segment_days,
            )
        )
        if number % 50 == 0:
            print(f"[ROBUSTNESS] {number}/{len(variants)} variants", flush=True)
    results = pd.DataFrame(rows).sort_values(["family", "variant"], kind="stable")
    results, train_ranked = add_train_only_checks(results)
    passed = results.loc[results["exploratory_survivor"]].copy()

    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    common.atomic_write_csv(results, ALL_VARIANTS_PATH)
    common.atomic_write_csv(passed, PASSED_VARIANTS_PATH)
    common.atomic_write_csv(train_ranked, TRAIN_RANKED_PATH)
    common.atomic_write_text(
        SUMMARY_PATH,
        render_report(baseline_rows, results, passed, train_ranked, cache_records),
    )
    source_after = {name: sha256(path) for name, path in source_paths.items()}
    if source_before != source_after:
        changed = [name for name in source_before if source_before[name] != source_after[name]]
        raise AssertionError("Protected source changed during research: " + ", ".join(changed))
    common.atomic_write_json(
        PROVENANCE_PATH,
        {
            "study_version": STUDY_VERSION,
            "baseline_policy": BASELINE_POLICY_NAME,
            "frozen_through_day": str(FROZEN_THROUGH_DAY),
            "split_day": str(SPLIT_DAY),
            "september_2_used_for_tuning": False,
            "costs_bps": list(COSTS_BPS),
            "train_only_preselection": {
                "min_train_net_improvement_pct": MIN_NET_IMPROVEMENT_PCT,
                "min_train_pf_improvement": MIN_PF_IMPROVEMENT,
                "max_train_drawdown_worsening_pct": MAX_DD_WORSENING_PCT,
                "min_total_fill_increase": MIN_TOTAL_FILL_INCREASE,
                "requires_positive_train_10bps_net_delta": True,
            },
            "test_reporting": "Metrics are reported after TRAIN-only ranking; they did not enter the rank.",
            "multiple_testing": "Exact paired daily sign-flip p-values on TRAIN, BH-FDR q <= 0.10.",
            "neighbour_check": "A separate nearby variant in same parameter family must have TRAIN PF delta >= 0 and TRAIN net delta >= +0.25%.",
            "baseline_metrics": baseline_rows.to_dict("records"),
            "variant_count": int(len(results)),
            "exploratory_survivor_count": int(len(passed)),
            "cache_records": cache_records,
            "source_before": source_before,
            "source_after": source_after,
            "sources_unchanged": source_before == source_after,
            "artifacts": {
                path.name: sha256(path)
                for path in (
                    ALL_VARIANTS_PATH,
                    PASSED_VARIANTS_PATH,
                    TRAIN_RANKED_PATH,
                    SUMMARY_PATH,
                )
            },
        },
    )
    print(
        f"[ROBUSTNESS] variants={len(results)} passed={len(passed)} "
        f"baseline_fills={int(baseline_5['filled'].sum())}",
        flush=True,
    )
    print(f"[WROTE] {SUMMARY_PATH}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
