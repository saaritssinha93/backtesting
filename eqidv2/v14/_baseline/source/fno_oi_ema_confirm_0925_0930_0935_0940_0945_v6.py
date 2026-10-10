"""Replay the frozen V6 BEST_NET configuration on NSE cash equities.

This is the promoted configuration from the V5 full-history cash-equity
optimizer completed on 2026-08-11. NSE equity candles supply every price,
volume, indicator, confirmation, entry and exit value. Mapped NFO futures
supply only oi, prev_oi and oi_change_pct.
"""

from __future__ import annotations

import argparse
import math
import time
from dataclasses import asdict
from datetime import date
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

import fno_oi_common as common
import fno_oi_backtest_provenance as provenance
import fno_oi_ema_confirm_0925_0930_0935_0940_0945_v5 as v5
import fno_oi_ema_confirm_optimize as signal_cache
import fno_oi_hybrid_data as hybrid
import fno_v5_hybrid_backtest as replay
from fno_v5_live_config import SetupSpec


STRATEGY_VERSION = "FNO_V6_BEST_NET_CASH_EQUITY_20260811"
OBJECTIVE = "BEST_NET"
CONFIG_SOURCE = "V5_CASH_EQUITY_FULL_HISTORY_BEST_NET"
SELECTED_HISTORY_END = date(2026, 8, 11)
BACKTEST_UNIVERSE_DATE = date(2026, 8, 11)
BACKTEST_UNIVERSE_PATH = common.universe_paths(BACKTEST_UNIVERSE_DATE)[0]
BACKTEST_UNIVERSE_HASHES = {
    "file_sha256": "24170f39c7cf99021553396e40e0d88a435f857364b2423dcfbe9312539dbf09",
    "universe_sha256": "18c496bbf9e09b6914d073cba21c4c6c56305da1ed5759f4f91cc8cb66c19ad5",
    "mapped_universe_sha256": "2cc160189f87bff4eb987a15a4684d95619ee9c810db3cd37276b114ad5824bf",
    "mapped_symbol_set_sha256": "d42f87a9c5fc8ab1710b09b6c4c9832c9d19ecc440ef92b84cad6981499a05a3",
}

RESULT_DIR = common.FNO_ROOT / "strategy_research"
PROVENANCE_DIR = RESULT_DIR / "backtest_provenance"
REPORT_PATH = common.LATEST_DIR / "latest_fno_oi_ema_confirm_v6_best_net.md"
DAILY_OUTPUT_PATH = (
    RESULT_DIR
    / "ema_confirm_0925_0930_0935_0940_0945_v6_best_net_daily.csv"
)
AUDIT_OUTPUT_PATH = (
    RESULT_DIR
    / "ema_confirm_0925_0930_0935_0940_0945_v6_best_net_trades.csv"
)
SETUPS_OUTPUT_PATH = (
    RESULT_DIR
    / "ema_confirm_0925_0930_0935_0940_0945_v6_best_net_setups.csv"
)
SELECTED_DAILY_PROTECTED_PATH = (
    RESULT_DIR
    / "ema_confirm_0925_0930_0935_0940_0945_v6_best_net_selected_20260811.csv"
)
SELECTED_RECREATED_PROVENANCE_PATH = (
    RESULT_DIR
    / "ema_confirm_0925_0930_0935_0940_0945_v6_best_net_selected_20260811_recreated_current_source.provenance.json"
)
CURRENT_SOURCE_REPLAY_REVISION = "20260818_V1"
CURRENT_SOURCE_SELECTED_DAILY_PATH = (
    RESULT_DIR
    / "ema_confirm_0925_0930_0935_0940_0945_v6_best_net_selected_current_source_20260818_v1.csv"
)
CURRENT_SOURCE_SELECTED_PROVENANCE_PATH = (
    RESULT_DIR
    / "ema_confirm_0925_0930_0935_0940_0945_v6_best_net_selected_current_source_20260818_v1.provenance.json"
)
LEGACY_SELECTED_MISMATCH_AUDIT_PATH = (
    RESULT_DIR / "fno_v6_legacy_selected_mismatch_audit_20260818.json"
)
CURRENT_SOURCE_SELECTED_DAILY_SHA256 = (
    "7ba3426c16497f4d0aa1f18c3aa3d3cd42c5d8ee8090d154c4b261ed69ed85b7"
)

EXPECTED_SELECTED_HISTORY: dict[str, float | int] = {
    "sessions": 53,
    "orders": 206,
    "fills": 205,
    "trade_pf": 2.796361677509257,
    "day_pf": 5.968440636105603,
    "net_pct": 144.00315457745492,
}

CURRENT_SOURCE_PROMOTED_HISTORY: dict[str, float | int] = {
    "sessions": 53,
    "orders": 210,
    "fills": 209,
    "trade_pf": 2.811435346898863,
    "day_pf": 6.061863909031509,
    "net_pct": 146.71089469102625,
}


def _setup(
    signal_end: str,
    confirmation_end: str,
    side: str,
    max_entries: int,
    picker: str,
    price_change_pct: float,
    oi_change_pct: float,
    volume_ratio: float,
    body_ratio: float,
    max_wick_ratio: float,
    stop_pct: float,
    target_pct: float,
) -> SetupSpec:
    return SetupSpec(
        signal_end=signal_end,
        confirmation_end=confirmation_end,
        side=side,
        mode="FILTERED",
        max_entries=max_entries,
        picker=picker,
        price_change_pct=price_change_pct,
        oi_change_pct=oi_change_pct,
        volume_ratio=volume_ratio,
        body_ratio=body_ratio,
        max_wick_ratio=max_wick_ratio,
        min_traded_value=0.0,
        stop_pct=stop_pct,
        target_pct=target_pct,
        source_version=STRATEGY_VERSION,
    )


# Exact BEST_NET setup book selected by the corrected V5 full-history run.
ACTIVE_SETUPS: tuple[SetupSpec, ...] = (
    _setup("09:25", "09:26", "LONG", 1, "max_liquidity", 0.30, 0.10, 3.0, 0.6, 0.5, 0.50, 3.0),
    _setup("09:25", "09:26", "SHORT", 2, "max_volume", 0.20, 0.10, 1.5, 0.4, 0.5, 0.75, 3.0),
    _setup("09:30", "09:31", "LONG", 1, "max_move", 0.65, 0.10, 1.0, 0.5, 0.5, 1.00, 2.5),
    _setup("09:30", "09:31", "SHORT", 1, "max_move", 0.20, 0.25, 1.0, 0.4, 0.5, 1.00, 3.0),
    _setup("09:35", "09:36", "LONG", 1, "max_liquidity", 0.20, 0.10, 1.0, 0.6, 0.5, 1.00, 2.5),
    _setup("09:35", "09:36", "SHORT", 2, "max_liquidity", 0.50, 1.00, 1.0, 0.4, 0.5, 1.00, 3.0),
    _setup("09:40", "09:41", "LONG", 1, "max_liquidity", 0.20, 0.10, 2.0, 0.5, 0.5, 0.50, 2.5),
    _setup("09:40", "09:41", "SHORT", 1, "max_move", 0.20, 0.10, 1.0, 0.4, 0.5, 1.00, 3.0),
    _setup("09:45", "09:46", "LONG", 1, "max_move", 0.65, 0.10, 1.0, 0.4, 0.5, 1.00, 3.0),
    _setup("09:45", "09:46", "SHORT", 1, "max_volume", 0.20, 0.75, 1.0, 0.4, 0.3, 1.00, 2.0),
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--split-day", default="2026-07-17")
    parser.add_argument("--through-day", default="")
    parser.add_argument("--cost-bps", type=float, default=5.0)
    parser.add_argument("--square-off", default="1530")
    parser.add_argument("--max-forward-bars", type=int, default=400)
    parser.add_argument("--rebuild-cache", action="store_true")
    parser.add_argument(
        "--promote-selected-provenance",
        action="store_true",
        help=(
            "After an exact through-2026-08-11 replay, immutably publish a "
            "current-source provenance attestation for the protected selected curve."
        ),
    )
    parser.add_argument(
        "--promote-current-source-v1",
        action="store_true",
        help=(
            "Immutably publish the versioned 2026-08-18 current-source replay "
            "and provenance without replacing the legacy selected artifact."
        ),
    )
    return parser.parse_args(argv)


def validate_configuration() -> None:
    expected_confirmations = {
        "09:25": "09:26",
        "09:30": "09:31",
        "09:35": "09:36",
        "09:40": "09:41",
        "09:45": "09:46",
    }
    if len(ACTIVE_SETUPS) != 10:
        raise AssertionError("V6 BEST_NET must contain ten setup legs.")
    if BACKTEST_UNIVERSE_PATH.name != "near_month_2026-08-11.parquet":
        raise AssertionError(
            "Promoted V6 must use its dated 2026-08-11 universe, never latest."
        )
    if set(BACKTEST_UNIVERSE_HASHES) != {
        "file_sha256",
        "universe_sha256",
        "mapped_universe_sha256",
        "mapped_symbol_set_sha256",
    } or any(len(value) != 64 for value in BACKTEST_UNIVERSE_HASHES.values()):
        raise AssertionError("Promoted V6 dated-universe hashes are incomplete.")
    seen: set[tuple[str, str]] = set()
    for setup in ACTIVE_SETUPS:
        key = (setup.signal_end, setup.side)
        if key in seen:
            raise AssertionError(f"Duplicate V6 setup: {key}")
        seen.add(key)
        if setup.confirmation_end != expected_confirmations[setup.signal_end]:
            raise AssertionError(f"Invalid confirmation time: {setup.setup_id}")
        cap = 1 if setup.side == "LONG" else 2
        if setup.max_entries > cap:
            raise AssertionError(f"V6 setup exceeds its entry cap: {setup.setup_id}")


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
                "orders": int(len(selected)),
                "fills": int(selected["filled"].sum()),
                "trade_pf": _profit_factor(filled),
                "net_pct": float(filled.sum()) if filled.size else 0.0,
                "objective": OBJECTIVE,
                "data_contract": hybrid.DATA_CONTRACT_VERSION,
            }
        )
    return pd.DataFrame(rows)


def attest_selected_history(
    stats: dict[str, Any],
    days: list[date],
    *,
    cost_bps: float,
    through_day: str,
    required: bool = False,
) -> None:
    requested_through = (
        pd.Timestamp(through_day).date() if str(through_day).strip() else None
    )
    if (
        not days
        or days[-1] != SELECTED_HISTORY_END
        or requested_through not in (None, SELECTED_HISTORY_END)
        or not math.isclose(cost_bps, 5.0)
    ):
        if required:
            raise AssertionError(
                "Selected provenance promotion requires the exact protected "
                "history end and cost contract."
            )
        return
    if requested_through is not None and not required:
        return
    observed = {
        "sessions": int(stats["sessions"]),
        "orders": int(stats["orders"]),
        "fills": int(stats["fills"]),
        "trade_pf": float(stats["trade_pf"]),
        "day_pf": float(stats["day_pf"]),
        "net_pct": float(stats["net_pct"]),
    }
    for metric, expected in EXPECTED_SELECTED_HISTORY.items():
        tolerance = 0.0 if metric in {"sessions", "orders", "fills"} else 1e-9
        if abs(float(observed[metric]) - float(expected)) > tolerance:
            raise AssertionError(
                f"V6 BEST_NET attestation failed for {metric}: "
                f"expected {expected}, observed {observed[metric]}"
            )


def _assert_promotion_contract(args: argparse.Namespace) -> None:
    if (
        str(args.through_day).strip() != SELECTED_HISTORY_END.isoformat()
        or not math.isclose(float(args.cost_bps), 5.0)
        or str(args.square_off) != "1530"
        or int(args.max_forward_bars) != 400
        or str(args.split_day) != "2026-07-17"
    ):
        raise AssertionError(
            "V6 provenance promotion requires --through-day 2026-08-11, "
            "split 2026-07-17, 5 bps cost, 15:30 square-off, and 400 forward bars."
        )


def attest_current_source_promoted_history(stats: dict[str, Any]) -> None:
    for metric, expected in CURRENT_SOURCE_PROMOTED_HISTORY.items():
        observed = stats.get(metric)
        tolerance = 0.0 if metric in {"sessions", "orders", "fills"} else 1e-9
        if observed is None or abs(float(observed) - float(expected)) > tolerance:
            raise AssertionError(
                f"V6 {CURRENT_SOURCE_REPLAY_REVISION} attestation failed for {metric}: "
                f"expected {expected}, observed {observed}"
            )


def legacy_selected_mismatch_audit_payload() -> dict[str, Any]:
    return {
        "schema_version": "fno_v6_legacy_selected_mismatch_v1",
        "recorded_date": "2026-08-18",
        "decision": (
            "PRESERVE_LEGACY_AS_HISTORICAL_UNATTESTED; DO_NOT_OVERWRITE; "
            "VERSION_CURRENT_SOURCE_REPLAY_SEPARATELY"
        ),
        "legacy_selected": {
            "path": str(SELECTED_DAILY_PROTECTED_PATH.resolve()),
            "sha256": "677470bb890f53c73a5eb20d6aebe55ac830e160dbfd07c20c32c42baec97a6b",
            "results": EXPECTED_SELECTED_HISTORY,
            "source_provenance_available": False,
        },
        "recreated_current_source": {
            "daily_sha256": CURRENT_SOURCE_SELECTED_DAILY_SHA256,
            "results": CURRENT_SOURCE_PROMOTED_HISTORY,
            "unprotected_run_provenance_path": str(
                (
                    PROVENANCE_DIR
                    / "fno_v6_best_net_20260818T003119895681+0530_199effd6d7aa.json"
                ).resolve()
            ),
            "unprotected_run_provenance_sha256": (
                "6868ad15b439f1f1a8a126a22bee1a1ccaebfa8f6c068036d62182db9499fed8"
            ),
            "backtest_input_fingerprint": (
                "199effd6d7aa430444a33f43fff4530925b131c15e47da226b953cc27687178d"
            ),
        },
        "changed_daily_rows": [
            {
                "day": "2026-06-03",
                "legacy_orders_fills": "5/5",
                "current_orders_fills": "7/7",
                "legacy_net_pct": 3.969235714817116,
                "current_net_pct": 4.526975828388475,
            },
            {
                "day": "2026-06-19",
                "legacy_orders_fills": "2/2",
                "current_orders_fills": "3/3",
                "legacy_net_pct": 1.649999999999985,
                "current_net_pct": 0.8499999999999788,
            },
            {
                "day": "2026-06-23",
                "legacy_orders_fills": "3/3",
                "current_orders_fills": "4/4",
                "legacy_net_pct": 2.214513077038366,
                "current_net_pct": 5.164513077038369,
            },
        ],
    }


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
    backtest_input_fingerprint: str = "",
    provenance_path: Path | None = None,
) -> str:
    lines = [
        "# FNO V6 BEST_NET Cash-Equity Backtest",
        "",
        f"- Generated: {common.now_ist().isoformat(timespec='seconds')}",
        f"- Strategy: `{STRATEGY_VERSION}`",
        f"- Configuration source: `{CONFIG_SOURCE}`",
        f"- Data contract: `{hybrid.DATA_CONTRACT_VERSION}`",
        f"- Dated universe: `{BACKTEST_UNIVERSE_PATH}`",
        f"- Mapped universe SHA-256: `{BACKTEST_UNIVERSE_HASHES['mapped_universe_sha256']}`",
        f"- Backtest input fingerprint: `{backtest_input_fingerprint}`",
        f"- Immutable run provenance: `{provenance_path or ''}`",
        "- Provenance fingerprints whole source files, not a date-sliced extract; files may contain rows after the replay cutoff.",
        "- This is a recreated/current-source replay and is not represented as the original selection-run source provenance.",
        "- NSE equities provide price, volume, EMA9/20/50, confirmation, entry and exit paths.",
        "- Mapped NFO futures provide only oi, prev_oi and oi_change_pct.",
        f"- Round-trip cost: {cost_bps:g} bps; train/test label split: {split_day.isoformat()}.",
        "- This configuration was selected on the full displayed history and is therefore in-sample.",
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
        "Entry | Side | Max | Picker | Price | OI | Volume | Body | Wick | Stop | Target | O/F | PF | Net %",
        "--- | --- | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---:",
    ]
    for row in setups.itertuples(index=False):
        lines.append(
            f"{row.confirmation_end} | {row.side} | {row.max_entries} | {row.picker} | "
            f"{row.price_change_pct:.2f} | {row.oi_change_pct:.2f} | "
            f"{row.volume_ratio:.2f} | {row.body_ratio:.2f} | "
            f"{row.max_wick_ratio:.2f} | {row.stop_pct:.2f}% | "
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
        entries = []
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
        "",
    ]
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.promote_selected_provenance and args.promote_current_source_v1:
        raise AssertionError("Choose only one V6 provenance promotion mode.")
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
    )
    signals = signals.copy()
    signals["day"] = pd.to_datetime(signals["day"]).dt.date
    v5.validate_cash_equity_signal_contract(signals, paths)
    if args.through_day:
        through_day = pd.Timestamp(args.through_day).date()
        signals = signals.loc[signals["day"].le(through_day)].copy()
    days = sorted(set(signals["day"]))
    if not days:
        raise RuntimeError("V6 has no sessions to replay.")

    audit = replay.replay_setups(
        signals,
        paths,
        cost_bps=args.cost_bps,
        setups=ACTIVE_SETUPS,
    )
    if audit.empty:
        raise RuntimeError("V6 BEST_NET selected no orders.")
    audit["objective"] = OBJECTIVE
    audit["strategy_version"] = STRATEGY_VERSION
    split_day = pd.Timestamp(args.split_day).date()
    daily = replay.build_daily_curve(audit, days, split_day=split_day)
    daily["objective"] = OBJECTIVE
    daily["strategy_version"] = STRATEGY_VERSION
    stats = replay.summary_stats(daily, audit)
    setups = build_setup_summary(audit)
    attest_selected_history(
        stats,
        days,
        cost_bps=args.cost_bps,
        through_day=args.through_day,
        required=bool(args.promote_selected_provenance),
    )
    if args.promote_selected_provenance or args.promote_current_source_v1:
        _assert_promotion_contract(args)
    if args.promote_current_source_v1:
        attest_current_source_promoted_history(stats)

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
        "objective": OBJECTIVE,
        "config_source": CONFIG_SOURCE,
        "data_contract": hybrid.DATA_CONTRACT_VERSION,
        "active_setups": [asdict(setup) for setup in ACTIVE_SETUPS],
    }
    input_fingerprint = provenance.backtest_input_fingerprint(
        cache_manifest,
        strategy_payload=provenance_strategy,
        parameters=provenance_parameters,
    )
    stamp = generated_at.strftime("%Y%m%dT%H%M%S%f%z")
    run_provenance_path = PROVENANCE_DIR / (
        f"fno_v6_best_net_{stamp}_{input_fingerprint[:12]}.json"
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
        ),
    )
    if args.promote_current_source_v1:
        provenance.publish_immutable_copy(
            DAILY_OUTPUT_PATH,
            CURRENT_SOURCE_SELECTED_DAILY_PATH,
            expected_sha256=CURRENT_SOURCE_SELECTED_DAILY_SHA256,
        )
        provenance.write_immutable_json(
            LEGACY_SELECTED_MISMATCH_AUDIT_PATH,
            legacy_selected_mismatch_audit_payload(),
        )
    selected_output_paths: dict[str, Path] = {}
    if args.promote_selected_provenance:
        selected_output_paths["protected_selected_daily"] = (
            SELECTED_DAILY_PROTECTED_PATH
        )
    elif args.promote_current_source_v1:
        selected_output_paths["protected_selected_daily"] = (
            CURRENT_SOURCE_SELECTED_DAILY_PATH
        )
        selected_output_paths["legacy_mismatch_audit"] = (
            LEGACY_SELECTED_MISMATCH_AUDIT_PATH
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
        cache_manifest_path=signal_cache.CACHE_MANIFEST_PATH,
        cache_manifest=cache_manifest,
        output_paths={
            "daily": DAILY_OUTPUT_PATH,
            "trades": AUDIT_OUTPUT_PATH,
            "setups": SETUPS_OUTPUT_PATH,
            "report": REPORT_PATH,
            **selected_output_paths,
        },
        results=stats,
    )
    if args.promote_selected_provenance:
        generated_hash = provenance.sha256_file(DAILY_OUTPUT_PATH)
        protected_hash = provenance.sha256_file(SELECTED_DAILY_PROTECTED_PATH)
        if generated_hash != protected_hash:
            raise AssertionError(
                "Recreated daily curve does not byte-match the protected selected CSV: "
                f"generated={generated_hash}, protected={protected_hash}"
            )
    provenance.write_immutable_json(run_provenance_path, run_provenance)
    if args.promote_selected_provenance:
        provenance.write_immutable_json(
            SELECTED_RECREATED_PROVENANCE_PATH, run_provenance
        )
    elif args.promote_current_source_v1:
        provenance.write_immutable_json(
            CURRENT_SOURCE_SELECTED_PROVENANCE_PATH, run_provenance
        )
    print(
        f"[V6 BEST_NET] sessions={stats['sessions']} "
        f"orders/fills={stats['orders']}/{stats['fills']} "
        f"PF={stats['trade_pf']:.3f} day PF={stats['day_pf']:.3f} "
        f"net={stats['net_pct']:+.3f}%",
        flush=True,
    )
    print(f"[DONE] {REPORT_PATH} ({time.monotonic() - started:.1f}s)", flush=True)
    print(f"[PROVENANCE] {run_provenance_path}", flush=True)
    if args.promote_current_source_v1:
        print(
            "[PROMOTED CURRENT SOURCE] "
            f"daily={CURRENT_SOURCE_SELECTED_DAILY_PATH} "
            f"daily_sha256={CURRENT_SOURCE_SELECTED_DAILY_SHA256} "
            f"provenance={CURRENT_SOURCE_SELECTED_PROVENANCE_PATH} "
            f"provenance_sha256={provenance.sha256_file(CURRENT_SOURCE_SELECTED_PROVENANCE_PATH)} "
            f"input_fingerprint={input_fingerprint}",
            flush=True,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
