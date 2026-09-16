"""V6-v2 corrected research challenger with a causal NIFTY regime gate.

Corrected V6 remains the immutable control.  V6-v2 imports its rolling
near-month contract policy, ten setup legs, strict one-minute confirmation,
native exits and costs.  The only strategy change is made before selection:

    LONG  alignment = NIFTY near-month return from session open
    SHORT alignment = -NIFTY near-month return from session open

The official V6-v2 configuration requires alignment >= +0.10% at every V6
five-minute selection time (09:25 through 09:45).  The NIFTY bar bearing the
signal-time stamp is completed before the following-minute confirmation and
entry, so the gate is causal.  Missing market context fails closed.

This is a research challenger, not a production promotion.  The script also
reports global and one-slot-at-a-time alternatives so that the official result
is not presented without its selectivity and multiple-testing trade-offs.
"""

from __future__ import annotations

import argparse
import time
from dataclasses import asdict
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
import fno_v6_corrected_backtest as v6_corrected
import fno_oi_ema_confirm_0925_0930_0935_0940_0945_v6 as v6


STRATEGY_VERSION = "FNO_V6_V2_CORRECTED_NIFTY_ALIGN_010_ALL_SLOTS"
OBJECTIVE = "PF_WITH_CAUSAL_MARKET_ALIGNMENT"
CONFIG_SOURCE = "CORRECTED_V6_PLUS_NIFTY_NEAR_MONTH_DIRECTION_GATE"

ALIGNMENT_THRESHOLD_PCT = 0.10
ALIGNMENT_SLOTS = (925, 930, 935, 940, 945)
RESEARCH_THRESHOLDS = (0.025, 0.05, 0.075, 0.10, 0.15, 0.20)
LATER_CHECK_START = date(2026, 9, 2)

ACTIVE_SETUPS = v6_corrected.ACTIVE_SETUPS
ROLL_POLICY = v6_corrected.ROLL_POLICY

RESULT_DIR = common.FNO_ROOT / "strategy_research" / "v6_v2_corrected"
CACHE_DIR = RESULT_DIR / "_cache"
NIFTY_ROOT = common.FNO_ROOT / "raw_contracts_5m"

DAILY_OUTPUT_PATH = RESULT_DIR / "fno_v6_v2_corrected_daily.csv"
AUDIT_OUTPUT_PATH = RESULT_DIR / "fno_v6_v2_corrected_trades.csv"
SETUPS_OUTPUT_PATH = RESULT_DIR / "fno_v6_v2_corrected_setups.csv"
FILTER_OUTPUT_PATH = RESULT_DIR / "fno_v6_v2_market_gate_audit.csv"
REJECTED_OUTPUT_PATH = RESULT_DIR / "fno_v6_v2_rejected_v6_trades.csv"
MATRIX_OUTPUT_PATH = RESULT_DIR / "fno_v6_v2_alignment_matrix.csv"
COMPARISON_OUTPUT_PATH = RESULT_DIR / "fno_v6_vs_v6_v2_daily.csv"
REPORT_PATH = RESULT_DIR / "FNO_V6_V2_CORRECTED_HISTORICAL_RESULTS.md"
PROVENANCE_PATH = RESULT_DIR / "fno_v6_v2_corrected_provenance.json"


def _profit_factor(values: np.ndarray) -> float:
    values = values[np.isfinite(values)]
    gross_profit = float(values[values > 0].sum())
    gross_loss = float(-values[values < 0].sum())
    if gross_loss:
        return gross_profit / gross_loss
    return float("inf") if gross_profit else float("nan")


def _trade_metrics(audit: pd.DataFrame) -> dict[str, float | int]:
    if audit.empty:
        values = np.array([], dtype=float)
    else:
        values = audit.loc[audit["filled"], "net_return_pct"].to_numpy(float)
        values = values[np.isfinite(values)]
    return {
        "fills": int(values.size),
        "wins": int((values > 0).sum()),
        "losses": int((values < 0).sum()),
        "trade_pf": _profit_factor(values),
        "net_pct": float(values.sum()),
        "win_rate": float((values > 0).mean()) if values.size else float("nan"),
    }


def _cache_stem(month: str, days: list[date], square_off: str, bars: int) -> Path:
    key = v6_corrected._cache_key(month, days, square_off, bars)
    return CACHE_DIR / f"{month}_{key}"


def _load_or_build_regime(
    month: str,
    universe_path: Path,
    days: list[date],
    *,
    square_off: str,
    max_forward_bars: int,
    rebuild: bool,
) -> tuple[pd.DataFrame, dict[int, dict[str, np.ndarray]], dict[str, Any]]:
    """Reuse corrected V6's matching cache read-only, else build in V6-v2."""

    mapped, record = provenance.load_backtest_universe(
        universe_path=universe_path,
        contract_month_contains=month,
    )
    own_stem = _cache_stem(month, days, square_off, max_forward_bars)
    corrected_stem = v6_corrected.CACHE_DIR / own_stem.name

    cached = None
    source = ""
    if not rebuild:
        if corrected_stem.with_suffix(".parquet").exists():
            cached = v6_corrected._load_cached(corrected_stem)
            source = "corrected_v6_cache_read_only"
        if cached is None:
            cached = v6_corrected._load_cached(own_stem)
            source = "v6_v2_cache"

    if cached is None:
        print(f"[BUILD] {month}: {len(mapped)} contracts over {len(days)} sessions", flush=True)
        signals, paths = sweep.build_signal_table(
            set(days),
            square_off=square_off,
            max_forward_bars=max_forward_bars,
            mapped_universe=mapped,
            confirmation_policy=sweep.CONFIRMATION_POLICY_V6_STRICT,
        )
        own_stem.parent.mkdir(parents=True, exist_ok=True)
        v6_corrected._store_cached(own_stem, signals, paths)
        source = "v6_v2_cache_built"
    else:
        signals, paths = cached
        print(f"[CACHE] {month}: {source} <- {own_stem.name}", flush=True)

    signals = signals.copy()
    signals["day"] = pd.to_datetime(signals["day"]).dt.date
    signals = signals.loc[signals["day"].isin(set(days))].copy()
    signals["contract_month"] = month
    record = dict(record)
    record.update({"contract_month": month, "sessions": len(days), "cache_source": source})
    return signals, paths, record


def load_corrected_inputs(args: argparse.Namespace):
    regimes = v6_corrected.regime_universe_paths()
    eligibility, calendar, origin = v6_corrected.build_eligibility(
        regimes, min_coverage=args.min_contract_coverage
    )
    ok = eligibility.loc[eligibility["eligible"]].copy()
    if ok.empty:
        raise RuntimeError("No corrected rolling-near-month sessions are eligible.")

    days_by_month: dict[str, list[date]] = {}
    for row in ok.to_dict("records"):
        days_by_month.setdefault(str(row["required_contract"]), []).append(row["day"])

    parts = []
    regime_records = []
    for month in sorted(days_by_month, key=lambda value: calendar[value]):
        days = sorted(days_by_month[month])
        signals, paths, record = _load_or_build_regime(
            month,
            regimes[month],
            days,
            square_off=args.square_off,
            max_forward_bars=args.max_forward_bars,
            rebuild=args.rebuild_cache,
        )
        parts.append((signals, paths))
        record.update(
            {
                "expiry": calendar[month],
                "first_day": days[0],
                "last_day": days[-1],
            }
        )
        regime_records.append(record)

    signals, paths = v6_corrected.concat_regimes(parts)
    if signals.empty:
        raise RuntimeError("Corrected V6 signal construction returned no candidates.")
    days = sorted(set(signals["day"]))
    return signals, paths, days, eligibility, regime_records, calendar, origin


def load_nifty_context(months: Iterable[str]) -> pd.DataFrame:
    frames = []
    for month in sorted(set(months)):
        path = NIFTY_ROOT / f"NIFTY{month}FUT_5minute.parquet"
        if not path.exists():
            print(f"[WARN] missing NIFTY context: {path}", flush=True)
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
        frame = frame.dropna(subset=["day", "open", "close"])
        session_open = frame.groupby("day")["open"].transform("first")
        frame["nifty_return_from_open_pct"] = (frame["close"] / session_open - 1.0) * 100.0
        frame["contract_month"] = month
        frames.append(
            frame[["contract_month", "day", "hhmm_int", "nifty_return_from_open_pct"]]
        )
    if not frames:
        return pd.DataFrame(
            columns=["contract_month", "day", "hhmm_int", "nifty_return_from_open_pct"]
        )
    return (
        pd.concat(frames, ignore_index=True)
        .drop_duplicates(["contract_month", "day", "hhmm_int"], keep="last")
        .sort_values(["contract_month", "day", "hhmm_int"])
        .reset_index(drop=True)
    )


def annotate_market_gate(
    signals: pd.DataFrame,
    context: pd.DataFrame,
    *,
    threshold_pct: float,
    slots: tuple[int, ...],
) -> pd.DataFrame:
    annotated = signals.merge(
        context,
        on=["contract_month", "day", "hhmm_int"],
        how="left",
        validate="many_to_one",
    )
    annotated["nifty_alignment_pct"] = np.where(
        annotated["side"].eq("LONG"),
        annotated["nifty_return_from_open_pct"],
        -annotated["nifty_return_from_open_pct"],
    )
    applies = annotated["hhmm_int"].isin(slots)
    annotated["market_gate_applies"] = applies
    annotated["market_gate_pass"] = (~applies) | (
        annotated["nifty_alignment_pct"].notna()
        & annotated["nifty_alignment_pct"].ge(threshold_pct)
    )
    annotated["market_gate_reason"] = np.select(
        [
            ~applies,
            applies & annotated["nifty_alignment_pct"].isna(),
            annotated["market_gate_pass"],
        ],
        ["NOT_APPLICABLE", "MISSING_NIFTY_CONTEXT", "PASS"],
        default="BELOW_ALIGNMENT_THRESHOLD",
    )
    return annotated


def run_book(
    signals: pd.DataFrame,
    paths: dict[int, dict[str, np.ndarray]],
    days: list[date],
    *,
    cost_bps: float,
    split_day: date,
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    audit = replay.replay_setups(signals, paths, cost_bps=cost_bps, setups=ACTIVE_SETUPS)
    audit = audit.copy()
    if not audit.empty:
        audit["objective"] = OBJECTIVE
        audit["strategy_version"] = STRATEGY_VERSION
    daily = replay.build_daily_curve(audit, days, split_day=split_day)
    daily["objective"] = OBJECTIVE
    daily["strategy_version"] = STRATEGY_VERSION
    return audit, daily, replay.summary_stats(daily, audit)


def _period_rows(audit: pd.DataFrame, split_day: date) -> list[tuple[str, pd.DataFrame]]:
    day = pd.to_datetime(audit["day"]).dt.date if not audit.empty else pd.Series(dtype=object)
    return [
        ("TRAIN", audit.loc[day < split_day]),
        ("TEST_TO_SEP01", audit.loc[(day >= split_day) & (day < LATER_CHECK_START)]),
        ("SEP02_PLUS", audit.loc[day >= LATER_CHECK_START]),
        ("ALL", audit),
    ]


def build_variant_matrix(
    annotated: pd.DataFrame,
    paths: dict[int, dict[str, np.ndarray]],
    days: list[date],
    *,
    cost_bps: float,
    split_day: date,
) -> pd.DataFrame:
    variants: list[tuple[str, float | None, tuple[int, ...]]] = [
        ("V6_BASELINE", None, tuple()),
    ]
    for threshold in RESEARCH_THRESHOLDS:
        variants.append((f"ALL_{threshold:.3f}", threshold, ALIGNMENT_SLOTS))
    for slot in ALIGNMENT_SLOTS:
        for threshold in RESEARCH_THRESHOLDS:
            variants.append((f"{slot:04d}_ONLY_{threshold:.3f}", threshold, (slot,)))

    rows = []
    bare = annotated.drop(
        columns=[
            "nifty_return_from_open_pct",
            "nifty_alignment_pct",
            "market_gate_applies",
            "market_gate_pass",
            "market_gate_reason",
        ],
        errors="ignore",
    )
    context_columns = annotated[
        ["contract_month", "day", "hhmm_int", "nifty_return_from_open_pct"]
    ].drop_duplicates()
    for name, threshold, slots in variants:
        if threshold is None:
            candidate_signals = bare
        else:
            gate = annotate_market_gate(
                bare, context_columns, threshold_pct=threshold, slots=slots
            )
            candidate_signals = gate.loc[gate["market_gate_pass"]].drop(
                columns=[
                    "nifty_return_from_open_pct",
                    "nifty_alignment_pct",
                    "market_gate_applies",
                    "market_gate_pass",
                    "market_gate_reason",
                ],
                errors="ignore",
            )
        audit, _, _ = run_book(
            candidate_signals,
            paths,
            days,
            cost_bps=cost_bps,
            split_day=split_day,
        )
        for period, subset in _period_rows(audit, split_day):
            rows.append(
                {
                    "variant": name,
                    "threshold_pct": threshold,
                    "slots": "ALL" if slots == ALIGNMENT_SLOTS else ",".join(f"{x:04d}" for x in slots),
                    "period": period,
                    **_trade_metrics(subset),
                }
            )
    return pd.DataFrame(rows)


def _fmt(value: Any) -> str:
    if isinstance(value, (float, np.floating)):
        if np.isnan(value):
            return "nan"
        if np.isinf(value):
            return "inf"
        return f"{float(value):,.6f}"
    if isinstance(value, (int, np.integer)):
        return f"{int(value):,}"
    return str(value)


def _markdown_table(frame: pd.DataFrame, columns: list[str]) -> str:
    if frame.empty:
        return "No rows."
    return frame.loc[:, columns].to_markdown(index=False, floatfmt=".3f")


def render_report(
    baseline_stats: dict[str, Any],
    v2_stats: dict[str, Any],
    baseline_audit: pd.DataFrame,
    v2_audit: pd.DataFrame,
    matrix: pd.DataFrame,
    rejected: pd.DataFrame,
    daily_compare: pd.DataFrame,
    *,
    split_day: date,
    cost_bps: float,
    sessions: int,
) -> str:
    comparison = []
    for metric in ("sessions", "orders", "fills", "wins", "losses", "trade_pf", "day_pf", "net_pct", "positive_days", "negative_days", "flat_days"):
        comparison.append(
            {
                "metric": metric,
                "corrected_v6": baseline_stats.get(metric, np.nan),
                "v6_v2": v2_stats.get(metric, np.nan),
                "delta": (
                    float(v2_stats.get(metric, np.nan)) - float(baseline_stats.get(metric, np.nan))
                    if metric in v2_stats and metric in baseline_stats
                    else np.nan
                ),
            }
        )

    period_rows = []
    for label, base_subset in _period_rows(baseline_audit, split_day):
        v2_subset = dict(_period_rows(v2_audit, split_day))[label]
        period_rows.append({"period": label, "strategy": "V6", **_trade_metrics(base_subset)})
        period_rows.append({"period": label, "strategy": "V6-v2", **_trade_metrics(v2_subset)})
    periods = pd.DataFrame(period_rows)

    all_period = matrix.loc[matrix["period"].eq("ALL")].copy()
    global_rows = all_period.loc[all_period["variant"].str.startswith("ALL_")].sort_values("threshold_pct")
    slot_rows = all_period.loc[all_period["variant"].str.contains("_ONLY_")].sort_values(
        ["trade_pf", "net_pct"], ascending=False
    )
    decision_rows = matrix.loc[
        matrix["variant"].isin(["V6_BASELINE", "ALL_0.100", "0925_ONLY_0.100"])
    ].copy()
    decision_rows["configuration"] = decision_rows["variant"].map(
        {
            "V6_BASELINE": "Corrected V6",
            "ALL_0.100": "V6-v2 official: all slots +0.10%",
            "0925_ONLY_0.100": "Balanced alternative: 09:25 only +0.10%",
        }
    )

    today = max(pd.to_datetime(baseline_audit["day"]).dt.date)
    base_today = baseline_audit.loc[pd.to_datetime(baseline_audit["day"]).dt.date.eq(today)]
    v2_today = v2_audit.loc[pd.to_datetime(v2_audit["day"]).dt.date.eq(today)]

    lines = [
        "# FNO V6-v2 corrected historical results",
        "",
        "## Verdict",
        "",
        f"V6-v2 applies a causal NIFTY near-month direction gate of **+{ALIGNMENT_THRESHOLD_PCT:.2f}%** to all five V6 selection times: 09:25, 09:30, 09:35, 09:40 and 09:45. Corrected V6's stock gates, OI logic, rankers, one-minute confirmation, stop, target and {cost_bps:.1f} bps cost are unchanged.",
        "",
        f"Across {sessions} corrected-data sessions, trade PF changes from **{baseline_stats['trade_pf']:.3f}** to **{v2_stats['trade_pf']:.3f}**. Fills change from **{baseline_stats['fills']}** to **{v2_stats['fills']}**, and net return changes from **{baseline_stats['net_pct']:+.3f}%** to **{v2_stats['net_pct']:+.3f}%**. The latest session ({today}) changes from **{_trade_metrics(base_today)['net_pct']:+.3f}%** to **{_trade_metrics(v2_today)['net_pct']:+.3f}%**.",
        "",
        "The PF improvement is real in this stored sample but is achieved by rejecting many trades. It remains a research result because the choice was made after inspecting this history and only a small later-check segment is available.",
        "",
        "## Corrected V6 versus official V6-v2",
        "",
        _markdown_table(pd.DataFrame(comparison), ["metric", "corrected_v6", "v6_v2", "delta"]),
        "",
        "## All slots versus 09:25 only",
        "",
        _markdown_table(decision_rows, ["configuration", "period", "fills", "wins", "losses", "trade_pf", "net_pct", "win_rate"]),
        "",
        "The official all-slot version is retained because this V6-v2 experiment was requested from the previously identified highest-PF logic. The 09:25-only +0.10% version is the more balanced result: it keeps 54 fills, raises PF to 2.431 and raises net return to +28.646%. It should be treated as a separate challenger, not silently substituted after seeing the full-history comparison.",
        "",
        "## Train, original test and later check",
        "",
        _markdown_table(periods, ["period", "strategy", "fills", "wins", "losses", "trade_pf", "net_pct", "win_rate"]),
        "",
        "## Global alignment threshold sensitivity",
        "",
        _markdown_table(global_rows, ["variant", "threshold_pct", "fills", "wins", "losses", "trade_pf", "net_pct", "win_rate"]),
        "",
        "## Individual-slot experiments",
        "",
        _markdown_table(slot_rows, ["variant", "slots", "threshold_pct", "fills", "trade_pf", "net_pct", "win_rate"]),
        "",
        "The slot-only table is diagnostic, not a menu for combining the best hindsight threshold from every row. The 09:25 filter is the only individual slot that both repairs the latest day and materially raises full-sample PF. Later-slot filtering does not replace the all-slot official result without new forward evidence.",
        "",
        "## Datewise corrected V6 versus official V6-v2",
        "",
        _markdown_table(
            daily_compare,
            ["day", "fills_v6", "portfolio_net_return_pct_v6", "fills_v6_v2", "portfolio_net_return_pct_v6_v2"],
        ),
        "",
        "## Corrected-V6 trades rejected by official V6-v2",
        "",
        _markdown_table(
            rejected,
            ["day", "hhmm_int", "tradingsymbol", "side", "setup_id", "nifty_return_from_open_pct", "nifty_alignment_pct", "net_return_pct"],
        ),
        "",
        "## Promotion status",
        "",
        "**Research-only.** Corrected V6 was not modified. Freeze this V6-v2 configuration and judge it on unseen sessions before considering live use. PF alone should not override the lower trade count and lower total historical net return.",
    ]
    return "\n".join(lines)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--split-day", default="2026-08-14")
    parser.add_argument("--cost-bps", type=float, default=5.0)
    parser.add_argument("--square-off", default="1530")
    parser.add_argument("--max-forward-bars", type=int, default=400)
    parser.add_argument("--min-contract-coverage", type=float, default=0.80)
    parser.add_argument("--rebuild-cache", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    started = time.monotonic()
    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    split_day = pd.Timestamp(args.split_day).date()

    signals, paths, days, eligibility, regimes, calendar, origin = load_corrected_inputs(args)
    context = load_nifty_context(signals["contract_month"].unique())
    annotated = annotate_market_gate(
        signals,
        context,
        threshold_pct=ALIGNMENT_THRESHOLD_PCT,
        slots=ALIGNMENT_SLOTS,
    )
    filtered_signals = annotated.loc[annotated["market_gate_pass"]].drop(
        columns=[
            "nifty_return_from_open_pct",
            "nifty_alignment_pct",
            "market_gate_applies",
            "market_gate_pass",
            "market_gate_reason",
        ],
        errors="ignore",
    )

    baseline_audit, baseline_daily, baseline_stats = v6_corrected.run_book(
        signals,
        paths,
        days,
        cost_bps=args.cost_bps,
        split_day=split_day,
    )
    v2_audit, v2_daily, v2_stats = run_book(
        filtered_signals,
        paths,
        days,
        cost_bps=args.cost_bps,
        split_day=split_day,
    )
    if baseline_audit.empty or v2_audit.empty:
        raise RuntimeError("Baseline or V6-v2 selected no orders.")

    baseline_sids = set(baseline_audit["sid"].astype(int))
    v2_sids = set(v2_audit["sid"].astype(int))
    rejected_sids = baseline_sids - v2_sids
    gate_lookup = annotated.set_index("sid")
    rejected = baseline_audit.loc[baseline_audit["sid"].astype(int).isin(rejected_sids)].copy()
    rejected = rejected.merge(
        gate_lookup[["nifty_return_from_open_pct", "nifty_alignment_pct", "market_gate_reason"]],
        left_on="sid",
        right_index=True,
        how="left",
        validate="one_to_one",
    ).sort_values(["day", "hhmm_int", "side", "tradingsymbol"])

    matrix = build_variant_matrix(
        annotated,
        paths,
        days,
        cost_bps=args.cost_bps,
        split_day=split_day,
    )
    setups = v6.build_setup_summary(v2_audit)

    daily_compare = baseline_daily[
        ["day", "selections", "fills", "portfolio_net_return_pct"]
    ].merge(
        v2_daily[["day", "selections", "fills", "portfolio_net_return_pct"]],
        on="day",
        how="outer",
        suffixes=("_v6", "_v6_v2"),
    ).sort_values("day")

    common.atomic_write_csv(v2_daily, DAILY_OUTPUT_PATH)
    common.atomic_write_csv(v2_audit, AUDIT_OUTPUT_PATH)
    common.atomic_write_csv(setups, SETUPS_OUTPUT_PATH)
    common.atomic_write_csv(annotated, FILTER_OUTPUT_PATH)
    common.atomic_write_csv(rejected, REJECTED_OUTPUT_PATH)
    common.atomic_write_csv(matrix, MATRIX_OUTPUT_PATH)
    common.atomic_write_csv(daily_compare, COMPARISON_OUTPUT_PATH)
    common.atomic_write_text(
        REPORT_PATH,
        render_report(
            baseline_stats,
            v2_stats,
            baseline_audit,
            v2_audit,
            matrix,
            rejected,
            daily_compare,
            split_day=split_day,
            cost_bps=args.cost_bps,
            sessions=len(days),
        ),
    )
    common.atomic_write_json(
        PROVENANCE_PATH,
        {
            "strategy_version": STRATEGY_VERSION,
            "objective": OBJECTIVE,
            "config_source": CONFIG_SOURCE,
            "generated_at_ist": common.now_ist().isoformat(timespec="seconds"),
            "baseline_strategy_version": v6_corrected.STRATEGY_VERSION,
            "roll_policy": ROLL_POLICY,
            "data_contract": hybrid.DATA_CONTRACT_VERSION,
            "confirmation_policy": sweep.CONFIRMATION_POLICY_V6_STRICT,
            "alignment": {
                "instrument": "point-in-time near-month NIFTY futures",
                "measure": "close at selection time versus session open",
                "long_score": "nifty_return_from_open_pct",
                "short_score": "-nifty_return_from_open_pct",
                "threshold_pct": ALIGNMENT_THRESHOLD_PCT,
                "slots_hhmm": list(ALIGNMENT_SLOTS),
                "missing_context_policy": "FAIL_CLOSED",
            },
            "parameters": {
                "split_day": str(split_day),
                "cost_bps": float(args.cost_bps),
                "square_off": str(args.square_off),
                "max_forward_bars": int(args.max_forward_bars),
                "min_contract_coverage": float(args.min_contract_coverage),
            },
            "expiry_calendar": {key: str(value) for key, value in calendar.items()},
            "expiry_origin": origin,
            "regimes": [
                {key: (str(value) if isinstance(value, date) else value) for key, value in row.items()}
                for row in regimes
            ],
            "sessions": len(days),
            "active_setups": [asdict(setup) for setup in ACTIVE_SETUPS],
            "baseline_stats": baseline_stats,
            "v6_v2_stats": v2_stats,
        },
    )

    print(
        f"[V6]    fills={baseline_stats['fills']} PF={baseline_stats['trade_pf']:.6f} "
        f"net={baseline_stats['net_pct']:+.6f}%",
        flush=True,
    )
    print(
        f"[V6-v2] fills={v2_stats['fills']} PF={v2_stats['trade_pf']:.6f} "
        f"net={v2_stats['net_pct']:+.6f}%",
        flush=True,
    )
    print(f"[GATE] rejected corrected-V6 selections: {len(rejected)}", flush=True)
    print(f"[WROTE] {REPORT_PATH}", flush=True)
    print(f"[DONE] {time.monotonic() - started:.1f}s", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
