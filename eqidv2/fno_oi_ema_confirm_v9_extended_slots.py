"""V9: the V6 BEST_NET strategy replayed over an extended five-minute grid.

V6 scans five morning slots only (09:25 through 09:45).  V9 keeps V6's engine,
data contract, confirmation rule and exit mechanics completely unchanged and
widens the *slot grid* to two blocks:

    morning    09:25, 09:30, ... 10:30   (14 slots)
    afternoon  12:30, 12:35, ... 14:00   (19 slots)

That is 33 slots x 2 sides = 66 legs.

Nothing about the engine had to change to make this possible: the shared
signal cache built by ``fno_oi_ema_confirm_sweep.build_signal_table`` already
scans every slot from 0925 to 1500 at the loosest thresholds, and
``fno_v5_hybrid_backtest.select_setup_rows`` applies each leg's thresholds at
replay time.  V6 simply never selected the later slots.

Tuning honesty
--------------
The ten legs on V6's original five slots carry V6's exact optimizer-selected
parameters and are marked TUNED.  The 56 legs on the 28 new slots have never
been optimised; they carry a single documented baseline drawn from the
loosest value V6 uses for each field, and are marked UNTUNED.

That baseline is a starting point for a sweep, not a claim.  Because the
signal cache is built at the loosest thresholds and filtered at replay time,
those legs can be tightened later without rebuilding anything.  Read the
UNTUNED rows as "what does this slot look like at V6's loosest gate", never as
"this slot has an edge".
"""

from __future__ import annotations

import argparse
import time
from dataclasses import asdict, replace
from datetime import date, datetime, timedelta
from typing import Any

import numpy as np
import pandas as pd

import fno_oi_common as common
import fno_oi_backtest_provenance as provenance
import fno_oi_ema_confirm_0925_0930_0935_0940_0945_v6 as v6
import fno_oi_ema_confirm_0925_0930_0935_0940_0945_v5 as v5
import fno_oi_ema_confirm_optimize as signal_cache
import fno_oi_hybrid_data as hybrid
import fno_v5_hybrid_backtest as replay
from fno_v5_live_config import SetupSpec


STRATEGY_VERSION = "FNO_V9_EXTENDED_SLOT_GRID_20260820"
OBJECTIVE = "BEST_NET_EXTENDED_SLOTS"
CONFIG_SOURCE = "V6_BEST_NET_LEGS_PLUS_UNTUNED_EXTENDED_SLOT_BASELINE"

# V9 deliberately reuses V6's dated universe so the two are comparable.
BACKTEST_UNIVERSE_DATE = v6.BACKTEST_UNIVERSE_DATE
BACKTEST_UNIVERSE_PATH = v6.BACKTEST_UNIVERSE_PATH
BACKTEST_UNIVERSE_HASHES = dict(v6.BACKTEST_UNIVERSE_HASHES)

RESULT_DIR = common.FNO_ROOT / "strategy_research"
PROVENANCE_DIR = RESULT_DIR / "backtest_provenance"
REPORT_PATH = common.LATEST_DIR / "latest_fno_oi_ema_confirm_v9_extended_slots.md"
DAILY_OUTPUT_PATH = RESULT_DIR / "ema_confirm_v9_extended_slots_daily.csv"
AUDIT_OUTPUT_PATH = RESULT_DIR / "ema_confirm_v9_extended_slots_trades.csv"
SETUPS_OUTPUT_PATH = RESULT_DIR / "ema_confirm_v9_extended_slots_setups.csv"

# Slot blocks, inclusive of both endpoints, on a five-minute step.
SLOT_BLOCKS: tuple[tuple[str, str, str], ...] = (
    ("MORNING", "09:25", "10:30"),
    ("AFTERNOON", "12:30", "14:00"),
)
SLOT_STEP_MINUTES = 5

# The five slots V6 already covers.  These keep V6's tuned parameters.
V6_SLOTS: tuple[str, ...] = ("09:25", "09:30", "09:35", "09:40", "09:45")

TUNED = "TUNED_V6"
UNTUNED = "UNTUNED_EXTENDED"

# Loosest value V6 uses for each field, per side.  Deliberately permissive so
# a later sweep can tighten without rebuilding the signal cache.
UNTUNED_BASELINE: dict[str, dict[str, Any]] = {
    "LONG": {
        "max_entries": 1,
        "picker": "max_liquidity",
        "price_change_pct": 0.20,
        "oi_change_pct": 0.10,
        "volume_ratio": 1.0,
        "body_ratio": 0.4,
        "max_wick_ratio": 0.5,
        "stop_pct": 1.00,
        "target_pct": 2.5,
    },
    "SHORT": {
        "max_entries": 1,
        "picker": "max_volume",
        "price_change_pct": 0.20,
        "oi_change_pct": 0.10,
        "volume_ratio": 1.0,
        "body_ratio": 0.4,
        "max_wick_ratio": 0.5,
        "stop_pct": 1.00,
        "target_pct": 3.0,
    },
}


def _add_minutes(hhmm: str, minutes: int) -> str:
    moment = datetime.strptime(hhmm, "%H:%M") + timedelta(minutes=minutes)
    return moment.strftime("%H:%M")


def build_slot_grid() -> tuple[str, ...]:
    """Every five-minute signal slot V9 scans, in chronological order."""

    slots: list[str] = []
    for _, first, last in SLOT_BLOCKS:
        current = first
        while current <= last:
            slots.append(current)
            current = _add_minutes(current, SLOT_STEP_MINUTES)
    if len(slots) != len(set(slots)):
        raise AssertionError("V9 slot grid contains duplicates")
    return tuple(slots)


SLOT_GRID = build_slot_grid()


def slot_block(slot: str) -> str:
    for name, first, last in SLOT_BLOCKS:
        if first <= slot <= last:
            return name
    raise ValueError(f"slot {slot!r} lies outside every V9 block")


def _v6_leg(slot: str, side: str) -> SetupSpec:
    for setup in v6.ACTIVE_SETUPS:
        if setup.signal_end == slot and setup.side == side:
            return setup
    raise AssertionError(f"V6 has no leg for {slot} {side}")


def _extended_leg(slot: str, side: str) -> SetupSpec:
    values = UNTUNED_BASELINE[side]
    return SetupSpec(
        signal_end=slot,
        confirmation_end=_add_minutes(slot, 1),
        side=side,
        mode="FILTERED",
        max_entries=int(values["max_entries"]),
        picker=str(values["picker"]),
        price_change_pct=float(values["price_change_pct"]),
        oi_change_pct=float(values["oi_change_pct"]),
        volume_ratio=float(values["volume_ratio"]),
        body_ratio=float(values["body_ratio"]),
        max_wick_ratio=float(values["max_wick_ratio"]),
        min_traded_value=0.0,
        stop_pct=float(values["stop_pct"]),
        target_pct=float(values["target_pct"]),
        source_version=STRATEGY_VERSION,
    )


def build_active_setups() -> tuple[SetupSpec, ...]:
    setups: list[SetupSpec] = []
    for slot in SLOT_GRID:
        for side in ("LONG", "SHORT"):
            if slot in V6_SLOTS:
                # Reuse V6's tuned leg verbatim, only restamping the version so
                # the audit records which strategy replayed it.
                setups.append(
                    replace(_v6_leg(slot, side), source_version=STRATEGY_VERSION)
                )
            else:
                setups.append(_extended_leg(slot, side))
    return tuple(setups)


ACTIVE_SETUPS: tuple[SetupSpec, ...] = build_active_setups()

TUNING_STATE: dict[str, str] = {
    setup.setup_id: (TUNED if setup.signal_end in V6_SLOTS else UNTUNED)
    for setup in ACTIVE_SETUPS
}


def validate_configuration() -> None:
    expected_slots = build_slot_grid()
    if SLOT_GRID != expected_slots:
        raise AssertionError("V9 slot grid is not deterministic")
    if len(expected_slots) != 33:
        raise AssertionError(
            f"V9 must scan 33 five-minute slots, found {len(expected_slots)}"
        )
    if len(ACTIVE_SETUPS) != 66:
        raise AssertionError(
            f"V9 must contain 66 setup legs, found {len(ACTIVE_SETUPS)}"
        )
    morning = [s for s in expected_slots if slot_block(s) == "MORNING"]
    afternoon = [s for s in expected_slots if slot_block(s) == "AFTERNOON"]
    if morning[0] != "09:25" or morning[-1] != "10:30" or len(morning) != 14:
        raise AssertionError("V9 morning block must run 09:25..10:30 inclusive")
    if afternoon[0] != "12:30" or afternoon[-1] != "14:00" or len(afternoon) != 19:
        raise AssertionError("V9 afternoon block must run 12:30..14:00 inclusive")

    seen: set[tuple[str, str]] = set()
    for setup in ACTIVE_SETUPS:
        key = (setup.signal_end, setup.side)
        if key in seen:
            raise AssertionError(f"Duplicate V9 setup: {key}")
        seen.add(key)
        if setup.signal_end not in expected_slots:
            raise AssertionError(f"V9 setup outside the grid: {setup.setup_id}")
        if setup.confirmation_end != _add_minutes(setup.signal_end, 1):
            raise AssertionError(
                f"V9 confirmation must be signal+1min: {setup.setup_id}"
            )
        if setup.mode != "FILTERED":
            raise AssertionError(f"V9 legs must be FILTERED: {setup.setup_id}")
        if setup.max_entries < 1:
            raise AssertionError(f"V9 cap must be positive: {setup.setup_id}")

    # Every V6 slot must reproduce V6's tuned leg exactly, ignoring the
    # restamped source_version.  This is what makes V9 a superset of V6
    # rather than a different strategy wearing the same name.
    for slot in V6_SLOTS:
        for side in ("LONG", "SHORT"):
            ours = next(
                s for s in ACTIVE_SETUPS
                if s.signal_end == slot and s.side == side
            )
            theirs = _v6_leg(slot, side)
            if asdict(replace(ours, source_version="")) != asdict(
                replace(theirs, source_version="")
            ):
                raise AssertionError(
                    f"V9 changed a V6 tuned leg: {slot} {side}"
                )
    if BACKTEST_UNIVERSE_PATH.name != "near_month_2026-08-11.parquet":
        raise AssertionError("V9 must use V6's dated universe, never latest.")


def build_setup_summary(audit: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for setup in ACTIVE_SETUPS:
        selected = audit.loc[audit["setup_id"].eq(setup.setup_id)]
        filled = selected.loc[selected["filled"], "net_return_pct"].to_numpy(float)
        wins = int((filled > 0).sum())
        losses = int((filled < 0).sum())
        rows.append(
            {
                # asdict() omits setup_id because it is a property, not a
                # field; add it explicitly so downstream joins have a key.
                "setup_id": setup.setup_id,
                **asdict(setup),
                "block": slot_block(setup.signal_end),
                "tuning_state": TUNING_STATE[setup.setup_id],
                "orders": int(len(selected)),
                "fills": int(selected["filled"].sum()),
                "wins": wins,
                "losses": losses,
                "win_rate": (wins / len(filled)) if len(filled) else float("nan"),
                "trade_pf": v6._profit_factor(filled),
                "net_pct": float(filled.sum()) if filled.size else 0.0,
                "avg_win_pct": float(filled[filled > 0].mean()) if wins else 0.0,
                "avg_loss_pct": float(filled[filled < 0].mean()) if losses else 0.0,
                "objective": OBJECTIVE,
                "data_contract": hybrid.DATA_CONTRACT_VERSION,
            }
        )
    return pd.DataFrame(rows)


def _group_metrics(setups: pd.DataFrame, mask: pd.Series) -> dict[str, Any]:
    subset = setups.loc[mask]
    orders = int(subset["orders"].sum())
    fills = int(subset["fills"].sum())
    net = float(subset["net_pct"].sum())
    return {"legs": int(len(subset)), "orders": orders, "fills": fills, "net_pct": net}


def render_report(
    daily: pd.DataFrame,
    audit: pd.DataFrame,
    setups: pd.DataFrame,
    stats: dict[str, Any],
    *,
    split_day: date,
    cost_bps: float,
    backtest_input_fingerprint: str,
    provenance_path: Any,
) -> str:
    tuned = setups["tuning_state"].eq(TUNED)
    morning_new = setups["block"].eq("MORNING") & ~tuned
    afternoon = setups["block"].eq("AFTERNOON")
    groups = (
        ("V6 tuned slots 09:25-09:45", _group_metrics(setups, tuned)),
        ("Extended morning 09:50-10:30 (UNTUNED)", _group_metrics(setups, morning_new)),
        ("Afternoon 12:30-14:00 (UNTUNED)", _group_metrics(setups, afternoon)),
    )
    lines = [
        "# FNO V9 Extended Slot Grid",
        "",
        f"- Strategy: `{STRATEGY_VERSION}`",
        f"- Objective: `{OBJECTIVE}`",
        f"- Config source: `{CONFIG_SOURCE}`",
        f"- Slots: {len(SLOT_GRID)} (morning 09:25-10:30, afternoon 12:30-14:00)",
        f"- Legs: {len(ACTIVE_SETUPS)} ({int(tuned.sum())} tuned, "
        f"{int((~tuned).sum())} untuned)",
        f"- Cost: {cost_bps:g} bps round trip",
        f"- Split day: {split_day}",
        f"- Data contract: `{hybrid.DATA_CONTRACT_VERSION}`",
        f"- Cache input fingerprint: `{backtest_input_fingerprint}`",
        f"- Provenance: `{provenance_path}`",
        "",
        "## UNTUNED WARNING",
        "",
        "The 56 legs outside V6's original five slots have never been "
        "optimised. They run a single loose baseline drawn from V6's most "
        "permissive value per field. Their numbers describe what those slots "
        "do at that gate; they are not evidence of an edge and must not be "
        "promoted without a dedicated train/test sweep.",
        "",
        "## Headline",
        "",
        f"- Sessions: {stats.get('sessions')}",
        f"- Orders / fills: {stats.get('orders')} / {stats.get('fills')}",
        f"- Trade PF: {stats.get('trade_pf')}",
        f"- Day PF: {stats.get('day_pf')}",
        f"- Net: {stats.get('net_pct')} percentage points",
        "",
        "## By block",
        "",
        "| Group | Legs | Orders | Fills | Net points |",
        "|---|---:|---:|---:|---:|",
    ]
    for label, metrics in groups:
        lines.append(
            f"| {label} | {metrics['legs']} | {metrics['orders']} | "
            f"{metrics['fills']} | {metrics['net_pct']:.3f} |"
        )
    lines.extend(
        [
            "",
            "## Per five-minute candle",
            "",
            "| Slot | Block | State | Orders | Fills | Wins | Losses | "
            "Win % | PF | Net points |",
            "|---|---|---|---:|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for slot in SLOT_GRID:
        subset = setups.loc[setups["signal_end"].eq(slot)]
        state = TUNED if slot in V6_SLOTS else UNTUNED
        fills = int(subset["fills"].sum())
        wins = int(subset["wins"].sum())
        losses = int(subset["losses"].sum())
        slot_rows = audit.loc[audit["setup_id"].isin(subset["setup_id"])]
        realised = slot_rows.loc[slot_rows["filled"], "net_return_pct"].to_numpy(float)
        win_rate = f"{wins / fills:.1%}" if fills else "n/a"
        lines.append(
            f"| {slot} | {slot_block(slot)} | {state} | "
            f"{int(subset['orders'].sum())} | {fills} | {wins} | {losses} | "
            f"{win_rate} | {v6._profit_factor(realised):.3f} | "
            f"{float(subset['net_pct'].sum()):.3f} |"
        )
    lines.extend(
        [
            "",
            "## Outputs",
            "",
            f"- Daily: `{DAILY_OUTPUT_PATH}`",
            f"- Trades: `{AUDIT_OUTPUT_PATH}`",
            f"- Setups: `{SETUPS_OUTPUT_PATH}`",
            "",
        ]
    )
    return "\n".join(lines)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--split-day", default="2026-07-17")
    parser.add_argument("--through-day", default="")
    parser.add_argument("--cost-bps", type=float, default=15.0)
    parser.add_argument("--square-off", default="1530")
    parser.add_argument("--max-forward-bars", type=int, default=400)
    parser.add_argument("--rebuild-cache", action="store_true")
    parser.add_argument(
        "--blocks",
        default="all",
        choices=("all", "morning", "afternoon", "v6"),
        help=(
            "restrict the replay to one block. 'v6' runs only the five "
            "original tuned slots, which reproduces V6's leg selection."
        ),
    )
    return parser.parse_args(argv)


def _selected_setups(blocks: str) -> tuple[SetupSpec, ...]:
    key = str(blocks).lower()
    if key == "all":
        return ACTIVE_SETUPS
    if key == "v6":
        return tuple(s for s in ACTIVE_SETUPS if s.signal_end in V6_SLOTS)
    target = key.upper()
    return tuple(s for s in ACTIVE_SETUPS if slot_block(s.signal_end) == target)


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
    )
    signals = signals.copy()
    signals["day"] = pd.to_datetime(signals["day"]).dt.date
    v5.validate_cash_equity_signal_contract(signals, paths)
    if args.through_day:
        through_day = pd.Timestamp(args.through_day).date()
        signals = signals.loc[signals["day"].le(through_day)].copy()
    days = sorted(set(signals["day"]))
    if not days:
        raise RuntimeError("V9 has no sessions to replay.")

    setups_used = _selected_setups(args.blocks)
    audit = replay.replay_setups(
        signals, paths, cost_bps=args.cost_bps, setups=setups_used
    )
    if audit.empty:
        raise RuntimeError("V9 selected no orders.")
    audit["objective"] = OBJECTIVE
    audit["strategy_version"] = STRATEGY_VERSION
    audit["block"] = audit["setup_id"].map(
        {s.setup_id: slot_block(s.signal_end) for s in ACTIVE_SETUPS}
    )
    audit["tuning_state"] = audit["setup_id"].map(TUNING_STATE)

    split_day = pd.Timestamp(args.split_day).date()
    daily = replay.build_daily_curve(audit, days, split_day=split_day)
    daily["objective"] = OBJECTIVE
    daily["strategy_version"] = STRATEGY_VERSION
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
        "blocks": str(args.blocks),
    }
    provenance_strategy = {
        "strategy_version": STRATEGY_VERSION,
        "objective": OBJECTIVE,
        "config_source": CONFIG_SOURCE,
        "data_contract": hybrid.DATA_CONTRACT_VERSION,
        "slot_grid": list(SLOT_GRID),
        "tuning_state": TUNING_STATE,
        "active_setups": [asdict(setup) for setup in setups_used],
    }
    input_fingerprint = provenance.backtest_input_fingerprint(
        cache_manifest,
        strategy_payload=provenance_strategy,
        parameters=provenance_parameters,
    )
    stamp = generated_at.strftime("%Y%m%dT%H%M%S%f%z")
    run_provenance_path = PROVENANCE_DIR / (
        f"fno_v9_extended_slots_{stamp}_{input_fingerprint[:12]}.json"
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
    provenance.write_immutable_json(
        run_provenance_path,
        provenance.build_run_provenance(
            generated_at=generated_at,
            strategy_version=STRATEGY_VERSION,
            objective=OBJECTIVE,
            strategy_payload=provenance_strategy,
            parameters=provenance_parameters,
            backtest_window={
                "from_day": days[0].isoformat(),
                "through_day": days[-1].isoformat(),
                "sessions": len(days),
            },
            cache_manifest_path=signal_cache.CACHE_MANIFEST_PATH,
            cache_manifest=cache_manifest,
            output_paths={
                "daily": DAILY_OUTPUT_PATH,
                "trades": AUDIT_OUTPUT_PATH,
                "setups": SETUPS_OUTPUT_PATH,
                "report": REPORT_PATH,
            },
            results={
                **{k: v for k, v in stats.items()},
                "promotion_eligible": False,
                "untuned_legs": int(
                    sum(1 for s in setups_used if s.signal_end not in V6_SLOTS)
                ),
            },
        ),
    )
    print(
        f"[V9] {len(days)} sessions | {len(setups_used)} legs | "
        f"orders={stats.get('orders')} fills={stats.get('fills')} "
        f"net={stats.get('net_pct')} | {time.monotonic() - started:.1f}s",
        flush=True,
    )
    print(REPORT_PATH)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
