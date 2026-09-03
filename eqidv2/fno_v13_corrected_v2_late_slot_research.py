"""Read-only late-slot research for the frozen FNO V13-corrected-v2 book.

This script exists to answer a narrow question without touching either
``fno_v6_corrected_backtest.py`` or ``fno_v13_corrected_v2_backtest.py``:

    Are 09:50 LONG, 09:50 SHORT, or 09:55 SHORT additions justified under
    the existing V6-style five-minute and one-minute logic?

Inputs are *only* the exact V13-v2 cache paths recorded in its provenance file
and the published V13-v2 trade file.  No source cache is rebuilt, no V6/V13
output is written, and hashes are taken before and after execution.  Results
are written solely below ``strategy_research/v13_corrected_v2_late_slot_research``.

The primary specifications are declared before their outcomes are computed:

* LONG: the V13-v2 modal long setup used for the existing 09:55 LONG leg;
* SHORT: the majority/modal V6 short thresholds (0.20% cash move, 0.10% OI
  rise, 1.0x volume, 0.40 body, 0.50 wick, 1.0% stop, 3.0% target,
  one entry).  V6 has no unique modal short picker, so its three existing
  picker choices are reported as a fixed three-member diagnostic family,
  not tuned or selected on result; and
* confirmation remains the next completed 1-minute bar (09:51 or 09:56).

The conservative screen is intentionally stricter than the small sample can
usually satisfy: at least eight fills and four active days in BOTH original
TRAIN and original TEST, positive net and PF >= 1.20 in both, positive
bootstrap lower bound in both, and positive net/PF > 1 after 20 bps costs.
Passing this screen would still be a shadow-result, not a production approval.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import zlib
from dataclasses import asdict, replace
from datetime import date
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import pandas as pd

import fno_oi_common as common
import fno_v5_hybrid_backtest as replay
import fno_v6_corrected_backtest as v6
import fno_v13_corrected_v2_backtest as v13


RESEARCH_VERSION = "FNO_V13_V2_LATE_SLOT_READ_ONLY_RESEARCH_20260903"
DEFAULT_THROUGH_DAY = "2026-09-02"
RESULT_DIR = (
    common.FNO_ROOT / "strategy_research" / "v13_corrected_v2_late_slot_research"
)
METRICS_PATH = RESULT_DIR / "late_slot_metrics.csv"
COST_STRESS_PATH = RESULT_DIR / "late_slot_cost_stress.csv"
COMBINED_PATH = RESULT_DIR / "late_slot_v13_v2_combined_comparison.csv"
TRADES_PATH = RESULT_DIR / "late_slot_5bps_trades.csv"
REPORT_PATH = RESULT_DIR / "FNO_V13_V2_LATE_SLOT_RESEARCH.md"
PROVENANCE_PATH = RESULT_DIR / "late_slot_research_provenance.json"

V13_PROVENANCE_PATH = v13.PROVENANCE_PATH
V13_PUBLISHED_TRADES_PATH = v13.TRADES_PATH
PRIMARY_V13_POLICY = "V13_V2_COMBINED_SHADOW"
BOOTSTRAP_DRAWS = 20_000
BOOTSTRAP_SEED = 20260903


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _snapshot_files(paths: Iterable[Path]) -> dict[str, str | None]:
    return {
        str(path.resolve()): (_sha256(path) if path.is_file() else None)
        for path in sorted({Path(path).resolve() for path in paths})
    }


def _v13_read_only_snapshot() -> dict[str, str | None]:
    """Include source plus every pre-existing V13-v2 artifact/cache file."""
    files = [Path(v6.__file__).resolve(), Path(v13.__file__).resolve()]
    if v13.RESULT_DIR.exists():
        files.extend(path for path in v13.RESULT_DIR.rglob("*") if path.is_file())
    return _snapshot_files(files)


def _modal_long(signal_end: str):
    """Exact V13-v2 modal LONG gating, just labelled as research."""
    return replace(v13._modal_long_setup(signal_end), source_version=RESEARCH_VERSION)


def _modal_short(signal_end: str, picker: str):
    """Predeclared modal V6 SHORT gates; only the historically tied picker varies."""
    confirmation_end = (
        pd.Timestamp(f"2000-01-01 {signal_end}") + pd.Timedelta(minutes=1)
    ).strftime("%H:%M")
    return replace(
        v6.ACTIVE_SETUPS[1],  # 09:25 SHORT is an existing valid V6 SetupSpec.
        signal_end=signal_end,
        confirmation_end=confirmation_end,
        side="SHORT",
        mode="FILTERED",
        max_entries=1,  # modal: three of five V6 SHORT setups use one entry.
        picker=picker,
        price_change_pct=0.20,  # modal: four of five.
        oi_change_pct=0.10,  # modal: three of five.
        volume_ratio=1.0,  # modal: four of five.
        body_ratio=0.40,  # modal: four of five.
        max_wick_ratio=0.50,  # modal: four of five.
        min_traded_value=0.0,
        stop_pct=1.0,  # modal: four of five.
        target_pct=3.0,  # modal: four of five.
        source_version=RESEARCH_VERSION,
    )


def declared_legs() -> dict[str, Any]:
    """No candidate parameters are discovered or changed after replaying data."""
    return {
        "0950_LONG_MODAL": _modal_long("09:50"),
        "0955_LONG_REFERENCE": _modal_long("09:55"),
        "0950_SHORT_MODAL_MAX_VOLUME": _modal_short("09:50", "max_volume"),
        "0950_SHORT_MODAL_MAX_MOVE": _modal_short("09:50", "max_move"),
        "0950_SHORT_MODAL_MAX_LIQUIDITY": _modal_short("09:50", "max_liquidity"),
        "0955_SHORT_MODAL_MAX_VOLUME": _modal_short("09:55", "max_volume"),
        "0955_SHORT_MODAL_MAX_MOVE": _modal_short("09:55", "max_move"),
        "0955_SHORT_MODAL_MAX_LIQUIDITY": _modal_short("09:55", "max_liquidity"),
    }


def _load_frozen_inputs(through_day: date) -> tuple[pd.DataFrame, dict, list[date], dict]:
    if not V13_PROVENANCE_PATH.is_file():
        raise FileNotFoundError(f"Missing V13-v2 provenance: {V13_PROVENANCE_PATH}")
    provenance = json.loads(V13_PROVENANCE_PATH.read_text(encoding="utf-8"))
    replay_days = [pd.Timestamp(value).date() for value in provenance["sessions"]]
    if through_day > max(replay_days):
        raise ValueError(
            f"{through_day} exceeds frozen V13-v2 data through {max(replay_days)}; "
            "this script refuses to rebuild a source cache."
        )
    requested_days = [day for day in replay_days if day <= through_day]
    parts: list[tuple[pd.DataFrame, dict]] = []
    for record in provenance["cache_records"]:
        parquet_path = Path(record["cache_parquet"])
        stem = parquet_path.with_suffix("")
        cached = v6._load_cached(stem)
        if cached is None:
            raise FileNotFoundError(f"Missing frozen V13-v2 cache pair for {stem}")
        signals, paths = cached
        signals = signals.loc[signals["day"].isin(set(requested_days))].copy()
        parts.append((signals, paths))
    signals, paths = v6.concat_regimes(parts)
    signals = signals.loc[signals["day"].isin(set(requested_days))].copy()
    days = sorted(set(pd.to_datetime(signals["day"]).dt.date))
    if days != requested_days:
        missing = sorted(set(requested_days) - set(days))
        raise AssertionError(f"Frozen cache lacks expected replay sessions: {missing}")
    return signals, paths, days, provenance


def _empty_audit_like(reference: pd.DataFrame) -> pd.DataFrame:
    return reference.iloc[0:0].copy()


def _profit_factor(values: np.ndarray) -> float:
    profit = float(values[values > 0].sum()) if values.size else 0.0
    loss = float(-values[values < 0].sum()) if values.size else 0.0
    if loss > 0:
        return profit / loss
    return float("inf") if profit > 0 else float("nan")


def _bootstrap_net_interval(daily_values: np.ndarray, token: str) -> tuple[float, float]:
    if not len(daily_values):
        return float("nan"), float("nan")
    seed = BOOTSTRAP_SEED + (zlib.crc32(token.encode("utf-8")) % 1_000_000)
    rng = np.random.default_rng(seed)
    index = rng.integers(0, len(daily_values), size=(BOOTSTRAP_DRAWS, len(daily_values)))
    totals = daily_values[index].sum(axis=1)
    low, high = np.quantile(totals, [0.025, 0.975])
    return float(low), float(high)


def _extended_metrics(audit: pd.DataFrame, days: list[date], token: str) -> dict[str, Any]:
    subset = audit.loc[audit["day"].isin(days)].copy()
    if subset.empty:
        subset = _empty_audit_like(audit)
    daily = replay.build_daily_curve(
        subset, days, split_day=v13.ORIGINAL_SPLIT_DAY
    )
    stats = replay.summary_stats(daily, subset)
    filled = subset.loc[subset["filled"], "net_return_pct"].to_numpy(float)
    day_values = daily["portfolio_net_return_pct"].to_numpy(float)
    curve = np.r_[0.0, np.cumsum(day_values)]
    drawdown = curve - np.maximum.accumulate(curve)
    positive_days = np.sort(day_values[day_values > 0])[::-1]
    positive_total = float(positive_days.sum())
    ci_low, ci_high = _bootstrap_net_interval(day_values, token)
    stats.update(
        {
            "active_days": int((day_values != 0.0).sum()),
            "max_drawdown_pct": float(drawdown.min()) if drawdown.size else 0.0,
            "top_positive_day_share": (
                float(positive_days[0] / positive_total)
                if positive_total > 0
                else float("nan")
            ),
            "bootstrap_95_net_low_pct": ci_low,
            "bootstrap_95_net_high_pct": ci_high,
        }
    )
    return stats


def _periods(days: list[date]) -> dict[str, list[date]]:
    return {
        "ORIGINAL_TRAIN": [day for day in days if day < v13.ORIGINAL_SPLIT_DAY],
        "ORIGINAL_TEST": [
            day
            for day in days
            if v13.ORIGINAL_SPLIT_DAY <= day <= v13.ORIGINAL_TEST_END
        ],
        "SEP2_CHECK": [day for day in days if day > v13.ORIGINAL_TEST_END],
        "ALL": list(days),
    }


def _screen(metrics_5bps: pd.DataFrame, cost_all: pd.DataFrame) -> tuple[bool, str]:
    """A predeclared conservative research screen; never a deployment approval."""
    train = metrics_5bps.loc[metrics_5bps["period"].eq("ORIGINAL_TRAIN")].iloc[0]
    test = metrics_5bps.loc[metrics_5bps["period"].eq("ORIGINAL_TEST")].iloc[0]
    stress = cost_all.loc[cost_all["cost_bps"].eq(20.0)].iloc[0]
    failures: list[str] = []
    for label, row in (("TRAIN", train), ("TEST", test)):
        if int(row["fills"]) < 8:
            failures.append(f"{label} fills < 8")
        if int(row["active_days"]) < 4:
            failures.append(f"{label} active days < 4")
        if not (float(row["net_pct"]) > 0.0):
            failures.append(f"{label} net <= 0")
        if not (float(row["trade_pf"]) >= 1.20):
            failures.append(f"{label} PF < 1.20")
        if not (float(row["bootstrap_95_net_low_pct"]) > 0.0):
            failures.append(f"{label} bootstrap lower <= 0")
    if not (float(stress["net_pct"]) > 0.0 and float(stress["trade_pf"]) > 1.0):
        failures.append("20 bps all-history stress fails")
    return (not failures, "; ".join(failures) if failures else "PASS")


def _trade_key_frame(audit: pd.DataFrame) -> pd.DataFrame:
    keys = ["day", "hhmm_int", "side", "setup_id", "tradingsymbol"]
    out = audit.copy()
    out["day"] = out["day"].astype(str)
    return out.sort_values(keys, kind="stable").reset_index(drop=True)


def _published_v13_parity(audit: pd.DataFrame) -> dict[str, Any]:
    if not V13_PUBLISHED_TRADES_PATH.is_file():
        return {"published_trade_file_found": False, "passed": False}
    reference = pd.read_csv(V13_PUBLISHED_TRADES_PATH)
    left = _trade_key_frame(audit)
    right = _trade_key_frame(reference)
    keys = ["day", "hhmm_int", "side", "setup_id", "tradingsymbol"]
    key_equal = len(left) == len(right) and left[keys].equals(right[keys])
    value_equal = key_equal and np.allclose(
        left["net_return_pct"].to_numpy(float),
        right["net_return_pct"].to_numpy(float),
        rtol=0.0,
        atol=1e-12,
        equal_nan=True,
    )
    return {
        "published_trade_file_found": True,
        "published_orders": int(len(right)),
        "direct_orders": int(len(left)),
        "keys_equal": bool(key_equal),
        "returns_equal_atol_1e_12": bool(value_equal),
        "passed": bool(value_equal),
    }


def _metrics_rows(
    audits: dict[str, pd.DataFrame],
    days: list[date],
    *,
    context: str,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for name, audit in audits.items():
        for period, period_days in _periods(days).items():
            rows.append(
                {
                    "context": context,
                    "leg": name,
                    "period": period,
                    "cost_bps": 5.0,
                    **_extended_metrics(audit, period_days, f"{context}:{name}:{period}"),
                }
            )
    return pd.DataFrame(rows)


def _cost_rows(
    signals: pd.DataFrame,
    paths: dict,
    legs: dict[str, Any],
    days: list[date],
    *,
    context: str,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for name, setup in legs.items():
        for cost_bps in (5.0, 10.0, 15.0, 20.0):
            audit = replay.replay_setups(
                signals, paths, cost_bps=cost_bps, setups=(setup,)
            )
            rows.append(
                {
                    "context": context,
                    "leg": name,
                    "cost_bps": cost_bps,
                    **_extended_metrics(audit, days, f"{context}:{name}:ALL:{cost_bps}"),
                }
            )
    return pd.DataFrame(rows)


def _render_report(
    *,
    days: list[date],
    metrics: pd.DataFrame,
    costs: pd.DataFrame,
    combined: pd.DataFrame,
    parity: dict[str, Any],
    unchanged: bool,
) -> str:
    main = metrics.loc[
        metrics["context"].eq("V13_V2_CONTEXT_OI_CAP_1PCT")
        & metrics["period"].isin(["ORIGINAL_TRAIN", "ORIGINAL_TEST", "SEP2_CHECK", "ALL"])
    ].copy()
    screen_rows: list[dict[str, Any]] = []
    for leg in main["leg"].drop_duplicates():
        subset = main.loc[main["leg"].eq(leg)]
        stress = costs.loc[
            costs["context"].eq("V13_V2_CONTEXT_OI_CAP_1PCT")
            & costs["leg"].eq(leg)
        ]
        passed, reason = _screen(subset, stress)
        screen_rows.append({"leg": leg, "passed": passed, "reason": reason})
    screens = pd.DataFrame(screen_rows)

    lines = [
        "# V13-v2 late-slot research: 09:50 and 09:55",
        "",
        "## Decision",
        "",
        "**No omitted late slot passes the declared conservative screen. Do not add "
        "09:50 LONG, 09:50 SHORT, or 09:55 SHORT to V13-v2.**",
        "",
        "The existing 09:55 LONG reference is positive in the simple uncapped "
        "V6-style replay, but it also fails the same strict screen due to only "
        "five TRAIN and five capped-TEST fills. It remains an experimental shadow "
        "leg, not evidence that later slots should be broadly enabled.",
        "",
        "## Frozen data and integrity",
        "",
        f"- Replay sessions: {days[0]} to {days[-1]} ({len(days)} sessions).",
        "- Original TRAIN: before 2026-08-14 (12 sessions); original TEST: "
        "2026-08-14 through 2026-09-01 (11 sessions); Sep 2 is a one-day, "
        "post-selection check.",
        f"- Direct V13-v2 cache replay matches published V13-v2: `{parity.get('passed')}` "
        f"({parity.get('direct_orders')} orders, 1e-12 return tolerance).",
        f"- V6 and V13-v2 source/output/cache hashes unchanged during research: `{unchanged}`.",
        "- Costs shown are round-trip bps using the existing V6/V13 bracket simulator.",
        "",
        "## Predeclared logic",
        "",
        "- LONG uses the exact V13-v2 modal long gates: +0.20% cash move, +0.10% "
        "five-minute OI, 1.0x volume, 0.40 body, <=0.50 wick, one max-liquidity "
        "selection, 1.0% stop / 3.0% target, then the next completed one-minute confirmation.",
        "- SHORT uses V6's majority/modal thresholds with the same 09:51/09:56 confirmation. "
        "V6 has no single modal short picker, so max-volume, max-move and max-liquidity "
        "are reported as a fixed diagnostic family. No picker is chosen from its P&L.",
        "- `V13_V2_CONTEXT_OI_CAP_1PCT` is the current V13-v2 pre-ranking 1.00% OI cap. "
        "`V6_STYLE_UNCAPPED` is the exact same cache without that policy-wide cap.",
        "",
        "## Screen result under current V13-v2 context",
        "",
        "The screen requires, in both TRAIN and TEST: >=8 fills, >=4 active days, "
        "positive net, PF >=1.20 and a positive 95% daily-bootstrap lower bound; it also "
        "requires positive net and PF >1 at 20 bps across all history.",
        "",
        "| leg | passed | reason |",
        "|---|---|---|",
    ]
    for row in screens.itertuples(index=False):
        lines.append(f"| {row.leg} | {row.passed} | {row.reason} |")

    lines.extend(
        [
            "",
            "## 5 bps standalone results in V13-v2 context",
            "",
            "| leg | TRAIN fills / PF / net | TEST fills / PF / net | Sep 2 | ALL fills / PF / net |",
            "|---|---|---|---|---|",
        ]
    )
    for leg in main["leg"].drop_duplicates():
        metrics_by_period = main.loc[main["leg"].eq(leg)].set_index("period")

        def cell(period: str) -> str:
            row = metrics_by_period.loc[period]
            pf = row.trade_pf
            pf_text = "inf" if math.isinf(pf) else ("n/a" if pd.isna(pf) else f"{pf:.3f}")
            return f"{int(row.fills)} / {pf_text} / {row.net_pct:+.3f}%"

        lines.append(
            f"| {leg} | {cell('ORIGINAL_TRAIN')} | {cell('ORIGINAL_TEST')} | "
            f"{cell('SEP2_CHECK')} | {cell('ALL')} |"
        )

    lines.extend(
        [
            "",
            "## Why 09:50 LONG is excluded",
            "",
            "With the exact long modal rules it produced 10 fills: +0.104% / PF 1.114 in "
            "TRAIN, then 0 wins from 5 fills in TEST (-3.888%, PF 0.000). The 1.00% cap "
            "does not change a single selected 09:50 LONG trade, so this is a direct "
            "negative result rather than a cap artefact.",
            "",
            "## Short-side reading",
            "",
            "09:50 SHORT's total result can look positive for max-move/max-liquidity, but it "
            "contains only five fills and loses in TRAIN (two fills). 09:55 SHORT makes "
            "+2.354% in its two TRAIN fills then loses -1.205% over five TEST fills. "
            "Neither has enough observations or cross-period stability to add safely. "
            "The three 09:55 picker variants select identical outcomes in this cache, so "
            "there is no hidden picker improvement to promote.",
            "",
            "## Cost stress: standalone legs, all sessions",
            "",
            "| leg | 5 bps net / PF | 10 bps net / PF | 15 bps net / PF | 20 bps net / PF |",
            "|---|---|---|---|---|",
        ]
    )
    stress = costs.loc[costs["context"].eq("V13_V2_CONTEXT_OI_CAP_1PCT")]
    for leg in stress["leg"].drop_duplicates():
        rows = stress.loc[stress["leg"].eq(leg)].set_index("cost_bps")
        cells = []
        for cost in (5.0, 10.0, 15.0, 20.0):
            row = rows.loc[cost]
            pf = row.trade_pf
            pf_text = "inf" if math.isinf(pf) else ("n/a" if pd.isna(pf) else f"{pf:.3f}")
            cells.append(f"{row.net_pct:+.3f}% / {pf_text}")
        lines.append(f"| {leg} | " + " | ".join(cells) + " |")

    lines.extend(
        [
            "",
            "## Incremental effect on the current V13-v2 book",
            "",
            "These rows replay the published V13-v2 combined shadow policy unchanged, then "
            "append exactly one declared leg. They are diagnostic only; no row changes the "
            "frozen strategy file.",
            "",
            "| book | ALL fills | ALL PF | ALL net | 20 bps PF | 20 bps net |",
            "|---|---:|---:|---:|---:|---:|",
        ]
    )
    for row in combined.itertuples(index=False):
        lines.append(
            f"| {row.book} | {int(row.fills)} | {row.trade_pf:.3f} | {row.net_pct:+.3f}% | "
            f"{row.pf_20bps:.3f} | {row.net_20bps:+.3f}% |"
        )

    lines.extend(
        [
            "",
            "## Interpretation",
            "",
            "The short variants that lift the all-history total do so with only five or seven "
            "fills and at least one failed historical period. This is precisely the small-sample "
            "pattern that creates attractive but non-repeatable backtests.  No late SHORT entry "
            "is included.  The 09:50 LONG result is decisively negative.  Keep the current "
            "V13-v2 source frozen; accumulate genuinely new sessions before reconsidering "
            "any late-slot rule.",
            "",
        ]
    )
    return "\n".join(lines)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--through-day", default=DEFAULT_THROUGH_DAY)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    through_day = pd.Timestamp(args.through_day).date()
    before = _v13_read_only_snapshot()
    signals, paths, days, frozen_provenance = _load_frozen_inputs(through_day)
    legs = declared_legs()

    # First prove that direct replay of the frozen cache is exactly V13-v2.
    v13_policy = v13.POLICIES[PRIMARY_V13_POLICY]
    capped_signals = v13.apply_policy(signals, v13_policy)
    v13_setups = v13.policy_setups(v13_policy)
    direct_v13_audit = replay.replay_setups(
        capped_signals, paths, cost_bps=5.0, setups=v13_setups
    )
    parity = _published_v13_parity(direct_v13_audit)
    if not parity["passed"]:
        raise AssertionError("Read-only cache replay does not match published V13-v2.")

    contexts = {
        "V6_STYLE_UNCAPPED": signals,
        "V13_V2_CONTEXT_OI_CAP_1PCT": capped_signals,
    }
    all_metrics: list[pd.DataFrame] = []
    all_costs: list[pd.DataFrame] = []
    trade_parts: list[pd.DataFrame] = []
    audits_by_context: dict[str, dict[str, pd.DataFrame]] = {}
    for context, context_signals in contexts.items():
        audits = {
            name: replay.replay_setups(
                context_signals, paths, cost_bps=5.0, setups=(setup,)
            )
            for name, setup in legs.items()
        }
        audits_by_context[context] = audits
        all_metrics.append(_metrics_rows(audits, days, context=context))
        all_costs.append(
            _cost_rows(context_signals, paths, legs, days, context=context)
        )
        for name, audit in audits.items():
            block = audit.copy()
            block.insert(0, "context", context)
            block.insert(1, "leg", name)
            trade_parts.append(block)
    metrics = pd.concat(all_metrics, ignore_index=True)
    costs = pd.concat(all_costs, ignore_index=True)

    # Full-book diagnostics use only the cap context used by the published V13-v2 policy.
    combined_specs: dict[str, tuple[Any, ...]] = {"V13_V2_CURRENT": tuple(v13_setups)}
    for name, setup in legs.items():
        if name == "0955_LONG_REFERENCE":
            continue  # It is already in V13-v2.
        combined_specs[f"V13_V2_PLUS_{name}"] = tuple(v13_setups) + (setup,)
    combined_rows: list[dict[str, Any]] = []
    for book, setups in combined_specs.items():
        audit_5 = replay.replay_setups(capped_signals, paths, cost_bps=5.0, setups=setups)
        stats_5 = _extended_metrics(audit_5, days, f"{book}:5")
        audit_20 = replay.replay_setups(capped_signals, paths, cost_bps=20.0, setups=setups)
        stats_20 = _extended_metrics(audit_20, days, f"{book}:20")
        combined_rows.append(
            {
                "book": book,
                **stats_5,
                "pf_20bps": stats_20["trade_pf"],
                "net_20bps": stats_20["net_pct"],
            }
        )
    combined = pd.DataFrame(combined_rows)

    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    common.atomic_write_csv(metrics, METRICS_PATH)
    common.atomic_write_csv(costs, COST_STRESS_PATH)
    common.atomic_write_csv(combined, COMBINED_PATH)
    common.atomic_write_csv(pd.concat(trade_parts, ignore_index=True), TRADES_PATH)
    after = _v13_read_only_snapshot()
    unchanged = before == after
    if not unchanged:
        changed = [
            path
            for path in sorted(set(before) | set(after))
            if before.get(path) != after.get(path)
        ]
        raise AssertionError(
            "A frozen V6/V13-v2 file changed during read-only research: "
            + ", ".join(changed)
        )
    common.atomic_write_text(
        REPORT_PATH,
        _render_report(
            days=days,
            metrics=metrics,
            costs=costs,
            combined=combined,
            parity=parity,
            unchanged=unchanged,
        ),
    )
    common.atomic_write_json(
        PROVENANCE_PATH,
        {
            "research_version": RESEARCH_VERSION,
            "purpose": "Read-only V13-v2 09:50/09:55 late-slot audit",
            "through_day": str(through_day),
            "sessions": [str(day) for day in days],
            "v13_policy_context": PRIMARY_V13_POLICY,
            "declared_legs": {name: asdict(setup) for name, setup in legs.items()},
            "screen": {
                "train_and_test_min_fills": 8,
                "train_and_test_min_active_days": 4,
                "train_and_test_min_pf": 1.20,
                "train_and_test_positive_net": True,
                "train_and_test_bootstrap_lower_bound_positive": True,
                "all_history_20bps_positive_net_and_pf_gt_1": True,
                "bootstrap_draws": BOOTSTRAP_DRAWS,
                "bootstrap_seed": BOOTSTRAP_SEED,
            },
            "published_v13_v2_parity": parity,
            "frozen_v13_provenance_sha256": _sha256(V13_PROVENANCE_PATH),
            "frozen_v13_provenance_sessions": frozen_provenance["sessions"],
            "frozen_files_before": before,
            "frozen_files_after": after,
            "frozen_files_unchanged": unchanged,
            "artifacts": {
                str(path.name): _sha256(path)
                for path in (METRICS_PATH, COST_STRESS_PATH, COMBINED_PATH, TRADES_PATH, REPORT_PATH)
            },
        },
    )
    print(
        f"[READ-ONLY LATE SLOT RESEARCH] {len(days)} sessions | "
        f"V13 parity={parity['passed']} | frozen unchanged={unchanged}",
        flush=True,
    )
    print(f"[WROTE] {REPORT_PATH}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
