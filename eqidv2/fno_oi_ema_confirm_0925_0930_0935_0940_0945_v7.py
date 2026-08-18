"""Replay V7: the frozen V6 setup book with high/low-only 1m entry.

V7 deliberately changes one strategy seam only.  A valid, exact completed
1-minute candle no longer has to match the trade colour, close beyond the
5-minute signal close, or satisfy body/wick morphology.  Its high (LONG) or
low (SHORT) becomes a stop-entry trigger, and a later 1-minute candle must
trade through that level before the order is filled.

All V6 five-minute thresholds, pickers, per-leg caps, brackets, sizing/cost
contract, dated universe, cash-equity price source and futures-OI source are
retained.  V6 artifacts are never overwritten.
"""

from __future__ import annotations

import argparse
import math
import time
from dataclasses import asdict, fields, replace
from datetime import date
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

import fno_oi_backtest_provenance as provenance
import fno_oi_common as common
import fno_oi_ema_confirm_0925_0930_0935_0940_0945_v5 as v5
import fno_oi_ema_confirm_0925_0930_0935_0940_0945_v6 as v6
import fno_oi_ema_confirm_sweep as sweep
import fno_oi_ema_confirm_v7_signal_cache as signal_cache
import fno_oi_hybrid_data as hybrid
import fno_v5_hybrid_backtest as replay
from fno_v5_live_config import SetupSpec


STRATEGY_VERSION = "FNO_V7_V6_CLONE_1M_HIGH_LOW_BREAKOUT_20260818"
OBJECTIVE = v6.OBJECTIVE
CONFIG_SOURCE = "V6_EXACT_COPY_EXCEPT_1M_ENTRY_CONFIRMATION"
COPIED_FROM_STRATEGY_VERSION = v6.STRATEGY_VERSION
COPIED_V6_SETUP_BOOK_SHA256 = (
    "3c3e59187768afbc015024b5735d1c1b62d91128e8d6888ccfaa6f1c6c15694a"
)
CONFIRMATION_POLICY = sweep.CONFIRMATION_POLICY_V7_BREAKOUT

BACKTEST_UNIVERSE_DATE = v6.BACKTEST_UNIVERSE_DATE
BACKTEST_UNIVERSE_PATH = v6.BACKTEST_UNIVERSE_PATH
BACKTEST_UNIVERSE_HASHES = dict(v6.BACKTEST_UNIVERSE_HASHES)
ROUND_TRIP_COST_BPS = 5.0
SQUARE_OFF = "15:30"
CAPITAL_PER_ENTRY_RS = 10_000.0
LEVERAGE = 5.0
TARGET_EXPOSURE_RS = CAPITAL_PER_ENTRY_RS * LEVERAGE

ONE_MIN_ENTRY_POLICY: dict[str, Any] = {
    "policy": CONFIRMATION_POLICY,
    "source": "EXACT_COMPLETED_NSE_EQUITY_1M",
    "finite_positive_ohlc_required": True,
    "valid_ohlc_geometry_required": True,
    "nonnegative_volume_required": True,
    "synthetic_or_stale_rows_allowed": False,
    "positive_range_required": True,
    "candle_colour_required": False,
    "close_beyond_five_minute_signal_close_required": False,
    "body_ratio_filter_enabled": False,
    "adverse_wick_ratio_filter_enabled": False,
    "long_trigger": "CONFIRMATION_CANDLE_HIGH",
    "short_trigger": "CONFIRMATION_CANDLE_LOW",
    "fill_timing": "LATER_1M_CANDLE_TRIGGER_TOUCH_ONLY",
    "same_confirmation_candle_fill_allowed": False,
}


def _v7_setup(source: SetupSpec) -> SetupSpec:
    return replace(
        source,
        body_ratio=0.0,
        max_wick_ratio=1.0,
        source_version=STRATEGY_VERSION,
    )


ACTIVE_SETUPS: tuple[SetupSpec, ...] = tuple(
    _v7_setup(setup) for setup in v6.ACTIVE_SETUPS
)

RESULT_DIR = common.FNO_ROOT / "strategy_research"
PROVENANCE_DIR = RESULT_DIR / "backtest_provenance"
REPORT_PATH = common.LATEST_DIR / "latest_fno_oi_ema_confirm_v7_extreme_break.md"
DAILY_OUTPUT_PATH = (
    RESULT_DIR
    / "ema_confirm_0925_0930_0935_0940_0945_v7_extreme_break_daily.csv"
)
AUDIT_OUTPUT_PATH = (
    RESULT_DIR
    / "ema_confirm_0925_0930_0935_0940_0945_v7_extreme_break_trades.csv"
)
SETUPS_OUTPUT_PATH = (
    RESULT_DIR
    / "ema_confirm_0925_0930_0935_0940_0945_v7_extreme_break_setups.csv"
)
CACHE_DIR = signal_cache.CACHE_DIR
CACHE_MANIFEST_PATH = signal_cache.CACHE_MANIFEST_PATH


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--split-day", default="2026-07-17")
    parser.add_argument("--through-day", default="")
    parser.add_argument("--cost-bps", type=float, default=ROUND_TRIP_COST_BPS)
    parser.add_argument("--square-off", default="1530")
    parser.add_argument("--max-forward-bars", type=int, default=400)
    parser.add_argument("--rebuild-cache", action="store_true")
    source_group = parser.add_mutually_exclusive_group()
    source_group.add_argument(
        "--freeze-sources",
        action="store_true",
        help=(
            "Physically copy and fingerprint every mapped source before the "
            "cache build. Use this while live source files are still changing."
        ),
    )
    source_group.add_argument(
        "--source-snapshot",
        type=Path,
        default=None,
        help="Reuse a previously completed V7 physical-source snapshot manifest.",
    )
    return parser.parse_args(argv)


def _output_paths() -> set[Path]:
    return {
        REPORT_PATH.resolve(),
        DAILY_OUTPUT_PATH.resolve(),
        AUDIT_OUTPUT_PATH.resolve(),
        SETUPS_OUTPUT_PATH.resolve(),
    }


def validate_configuration() -> None:
    """Prove that V7 differs from V6 only at the declared 1m seam."""

    v6.validate_configuration()
    if CONFIRMATION_POLICY != sweep.CONFIRMATION_POLICY_V7_BREAKOUT:
        raise AssertionError("V7 confirmation policy is not high/low breakout.")
    if len(ACTIVE_SETUPS) != len(v6.ACTIVE_SETUPS):
        raise AssertionError("V7 must copy every V6 setup leg.")
    observed_v6_setup_hash = common.canonical_json_sha256(
        [asdict(setup) for setup in v6.ACTIVE_SETUPS]
    )
    if observed_v6_setup_hash != COPIED_V6_SETUP_BOOK_SHA256:
        raise AssertionError(
            "The V6 setup book changed after V7 was versioned: "
            f"expected {COPIED_V6_SETUP_BOOK_SHA256}, "
            f"observed {observed_v6_setup_hash}."
        )

    changed_fields = {"body_ratio", "max_wick_ratio", "source_version"}
    setup_fields = {field.name for field in fields(SetupSpec)}
    for source, observed in zip(v6.ACTIVE_SETUPS, ACTIVE_SETUPS, strict=True):
        for field_name in setup_fields - changed_fields:
            if getattr(observed, field_name) != getattr(source, field_name):
                raise AssertionError(
                    f"V7 changed V6 setup field {field_name}: {source.setup_id}"
                )
        if observed.body_ratio != 0.0 or observed.max_wick_ratio != 1.0:
            raise AssertionError(
                f"V7 morphology filters are not disabled: {observed.setup_id}"
            )
        if observed.source_version != STRATEGY_VERSION:
            raise AssertionError(f"V7 source version mismatch: {observed.setup_id}")
        if observed.picker == "max_body":
            raise AssertionError("V7 cannot retain a morphology-based picker.")

    if BACKTEST_UNIVERSE_DATE != v6.BACKTEST_UNIVERSE_DATE:
        raise AssertionError("V7 changed the V6 dated universe.")
    if BACKTEST_UNIVERSE_PATH.resolve() != v6.BACKTEST_UNIVERSE_PATH.resolve():
        raise AssertionError("V7 changed the V6 universe path.")
    if BACKTEST_UNIVERSE_HASHES != v6.BACKTEST_UNIVERSE_HASHES:
        raise AssertionError("V7 changed the V6 universe hashes.")
    if CACHE_DIR.resolve() == v6.signal_cache.CACHE_DIR.resolve():
        raise AssertionError("V7 must not reuse the direction-filtered V6 cache.")
    if _output_paths() & {
        v6.REPORT_PATH.resolve(),
        v6.DAILY_OUTPUT_PATH.resolve(),
        v6.AUDIT_OUTPUT_PATH.resolve(),
        v6.SETUPS_OUTPUT_PATH.resolve(),
    }:
        raise AssertionError("V7 output paths overlap protected V6 outputs.")


def _profit_factor(values: np.ndarray) -> float:
    profit = float(values[values > 0].sum()) if values.size else 0.0
    loss = float(-values[values < 0].sum()) if values.size else 0.0
    if loss > 0:
        return profit / loss
    return float("inf") if profit > 0 else float("nan")


def build_setup_summary(audit: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for setup in ACTIVE_SETUPS:
        selected = audit.loc[audit["setup_id"].eq(setup.setup_id)]
        filled = selected.loc[selected["filled"], "net_return_pct"].to_numpy(float)
        rows.append(
            {
                **asdict(setup),
                "confirmation_policy": CONFIRMATION_POLICY,
                "orders": int(len(selected)),
                "fills": int(selected["filled"].sum()),
                "trade_pf": _profit_factor(filled),
                "net_pct": float(filled.sum()) if filled.size else 0.0,
                "objective": OBJECTIVE,
                "data_contract": hybrid.DATA_CONTRACT_VERSION,
            }
        )
    return pd.DataFrame(rows)


def _fmt(value: Any) -> str:
    number = float(value)
    if math.isnan(number):
        return ""
    if math.isinf(number):
        return "INF"
    return f"{number:.3f}"


def render_report(
    daily: pd.DataFrame,
    audit: pd.DataFrame,
    setups: pd.DataFrame,
    stats: dict[str, Any],
    *,
    split_day: date,
    cost_bps: float,
    backtest_input_fingerprint: str,
    provenance_path: Path,
    source_snapshot: dict[str, Any] | None,
) -> str:
    lines = [
        "# FNO V7 — V6 Clone with 1m High/Low Breakout",
        "",
        f"- Generated: {common.now_ist().isoformat(timespec='seconds')}",
        f"- Strategy: `{STRATEGY_VERSION}`",
        f"- Copied from: `{COPIED_FROM_STRATEGY_VERSION}`",
        f"- Configuration source: `{CONFIG_SOURCE}`",
        f"- Confirmation policy: `{CONFIRMATION_POLICY}`",
        f"- Data contract: `{hybrid.DATA_CONTRACT_VERSION}`",
        f"- Dated universe: `{BACKTEST_UNIVERSE_PATH}`",
        f"- Mapped universe SHA-256: `{BACKTEST_UNIVERSE_HASHES['mapped_universe_sha256']}`",
        f"- Backtest input fingerprint: `{backtest_input_fingerprint}`",
        f"- Immutable run provenance: `{provenance_path}`",
        f"- Physical source snapshot: `{(source_snapshot or {}).get('manifest_path', '') or 'NONE'}`",
        f"- Round-trip cost: {cost_bps:g} bps; train/test label split: {split_day.isoformat()}.",
        "- This is a research backtest, not a promoted live strategy.",
        "",
        "## Only V6 → V7 Strategy Change",
        "",
        "- A valid exact completed 1m candle is required.",
        "- Candle colour, close-vs-5m-close, body and wick filters are ignored.",
        "- LONG trigger = completed 1m high; SHORT trigger = completed 1m low.",
        "- Fill can occur only on a later 1m candle trading through the trigger.",
        "- V6 5m filters, picker/caps, stops and targets are unchanged.",
        "",
        "Metric | Value",
        "--- | ---:",
        f"Sessions | {stats['sessions']}",
        f"Orders / fills | {stats['orders']} / {stats['fills']}",
        f"Trade PF | {_fmt(stats['trade_pf'])}",
        f"Day PF | {_fmt(stats['day_pf'])}",
        f"Net return sum | {stats['net_pct']:+.3f}%",
        f"Positive / negative / flat days | {stats['positive_days']} / {stats['negative_days']} / {stats['flat_days']}",
        "",
        "## Setup Book",
        "",
        "Entry | Side | Max | Picker | Price | OI | Volume | Stop | Target | O/F | PF | Net %",
        "--- | --- | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---:",
    ]
    for row in setups.itertuples(index=False):
        lines.append(
            f"{row.confirmation_end} | {row.side} | {row.max_entries} | {row.picker} | "
            f"{row.price_change_pct:.2f} | {row.oi_change_pct:.2f} | "
            f"{row.volume_ratio:.2f} | {row.stop_pct:.2f}% | "
            f"{row.target_pct:.2f}% | {row.orders}/{row.fills} | "
            f"{_fmt(row.trade_pf)} | {row.net_pct:+.3f}%"
        )

    lines += [
        "",
        "## Daywise Entries",
        "",
        "Day | Period | O/F | Day % | Cumulative % | Equity entries",
        "--- | --- | ---: | ---: | ---: | ---",
    ]
    for row in daily.itertuples(index=False):
        selected = audit.loc[audit["day"].eq(row.day)].sort_values(
            ["confirmation_end", "side", "tradingsymbol"], kind="stable"
        )
        entries: list[str] = []
        for trade in selected.itertuples(index=False):
            result = (
                f"{float(trade.net_return_pct):+.3f}%"
                if bool(trade.filled)
                else "NO_FILL"
            )
            entries.append(
                f"{trade.confirmation_end} {trade.side[0]} "
                f"{trade.tradingsymbol} @{float(trade.trigger):.2f} {result}"
            )
        lines.append(
            f"{row.day} | {row.period} | {int(row.selections)}/{int(row.fills)} | "
            f"{row.portfolio_net_return_pct:+.3f}% | "
            f"{row.cumulative_net_pct:+.3f}% | "
            f"{'<br>'.join(entries) if entries else 'No entries'}"
        )

    lines += [
        "",
        "## Outputs",
        "",
        f"- Daily: `{DAILY_OUTPUT_PATH}`",
        f"- Trades: `{AUDIT_OUTPUT_PATH}`",
        f"- Setups: `{SETUPS_OUTPUT_PATH}`",
        f"- Cache: `{CACHE_DIR}`",
        "",
    ]
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    validate_configuration()
    started = time.monotonic()

    signals, paths, cache_manifest = signal_cache.load_signals(
        args.square_off,
        args.max_forward_bars,
        args.rebuild_cache,
        universe_path=BACKTEST_UNIVERSE_PATH,
        universe_date=BACKTEST_UNIVERSE_DATE,
        require_persisted_mapping=True,
        require_complete_sources=True,
        expected_universe_hashes=BACKTEST_UNIVERSE_HASHES,
        return_provenance=True,
        freeze_sources=bool(args.freeze_sources),
        source_snapshot_path=args.source_snapshot,
    )
    signals = signals.copy()
    signals["day"] = pd.to_datetime(signals["day"]).dt.date
    v5.validate_cash_equity_signal_contract(signals, paths)
    if args.through_day:
        through_day = pd.Timestamp(args.through_day).date()
        signals = signals.loc[signals["day"].le(through_day)].copy()
    days = sorted(set(signals["day"]))
    if not days:
        raise RuntimeError("V7 has no sessions to replay.")

    audit = replay.replay_setups(
        signals,
        paths,
        cost_bps=args.cost_bps,
        setups=ACTIVE_SETUPS,
    )
    if audit.empty:
        raise RuntimeError("V7 selected no orders.")
    audit["objective"] = OBJECTIVE
    audit["strategy_version"] = STRATEGY_VERSION
    audit["confirmation_policy"] = CONFIRMATION_POLICY

    split_day = pd.Timestamp(args.split_day).date()
    daily = replay.build_daily_curve(audit, days, split_day=split_day)
    daily["objective"] = OBJECTIVE
    daily["strategy_version"] = STRATEGY_VERSION
    daily["confirmation_policy"] = CONFIRMATION_POLICY
    stats = replay.summary_stats(daily, audit)
    setups = build_setup_summary(audit)

    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    PROVENANCE_DIR.mkdir(parents=True, exist_ok=True)
    common.LATEST_DIR.mkdir(parents=True, exist_ok=True)
    generated_at = common.now_ist()
    provenance_parameters = {
        "split_day": str(args.split_day),
        "through_day": str(args.through_day),
        "cost_bps": float(args.cost_bps),
        "square_off": str(args.square_off),
        "max_forward_bars": int(args.max_forward_bars),
    }
    provenance_strategy = {
        "strategy_version": STRATEGY_VERSION,
        "copied_from_strategy_version": COPIED_FROM_STRATEGY_VERSION,
        "objective": OBJECTIVE,
        "config_source": CONFIG_SOURCE,
        "copied_v6_setup_book_sha256": COPIED_V6_SETUP_BOOK_SHA256,
        "data_contract": hybrid.DATA_CONTRACT_VERSION,
        "one_min_entry_policy": ONE_MIN_ENTRY_POLICY,
        "active_setups": [asdict(setup) for setup in ACTIVE_SETUPS],
    }
    input_fingerprint = provenance.backtest_input_fingerprint(
        cache_manifest,
        strategy_payload=provenance_strategy,
        parameters=provenance_parameters,
    )
    stamp = generated_at.strftime("%Y%m%dT%H%M%S%f%z")
    run_provenance_path = PROVENANCE_DIR / (
        f"fno_v7_extreme_break_{stamp}_{input_fingerprint[:12]}.json"
    )

    common.atomic_write_csv(daily, DAILY_OUTPUT_PATH)
    common.atomic_write_csv(audit, AUDIT_OUTPUT_PATH)
    common.atomic_write_csv(setups, SETUPS_OUTPUT_PATH)
    common.atomic_write_text(
        REPORT_PATH,
        render_report(
            daily,
            audit,
            setups,
            stats,
            split_day=split_day,
            cost_bps=args.cost_bps,
            backtest_input_fingerprint=input_fingerprint,
            provenance_path=run_provenance_path,
            source_snapshot=cache_manifest.get("source_snapshot"),
        ),
    )
    run_provenance = provenance.build_run_provenance(
        generated_at=generated_at,
        strategy_version=STRATEGY_VERSION,
        objective=OBJECTIVE,
        strategy_payload=provenance_strategy,
        parameters=provenance_parameters,
        backtest_window={
            "first_session": days[0].isoformat(),
            "last_session": days[-1].isoformat(),
            "through_day_argument": str(args.through_day),
            "sessions": int(len(days)),
        },
        cache_manifest_path=CACHE_MANIFEST_PATH,
        cache_manifest=cache_manifest,
        output_paths={
            "daily": DAILY_OUTPUT_PATH,
            "trades": AUDIT_OUTPUT_PATH,
            "setups": SETUPS_OUTPUT_PATH,
            "report": REPORT_PATH,
        },
        results=stats,
    )
    provenance.write_immutable_json(run_provenance_path, run_provenance)
    print(
        f"[V7 EXTREME BREAK] sessions={stats['sessions']} "
        f"orders/fills={stats['orders']}/{stats['fills']} "
        f"PF={stats['trade_pf']:.3f} day PF={stats['day_pf']:.3f} "
        f"net={stats['net_pct']:+.3f}%",
        flush=True,
    )
    print(f"[DONE] {REPORT_PATH} ({time.monotonic() - started:.1f}s)", flush=True)
    print(f"[PROVENANCE] {run_provenance_path}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
