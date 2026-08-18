"""Train-only day-PF optimizer for the FNO V5 09:26 entry window.

This research module never changes the protected V5 setup book.  It searches
the 09:25 five-minute signal and 09:26 one-minute confirmation legs only,
using NSE-equity prices/volume/indicators and mapped futures OI under the same
hybrid data contract as live V5.
"""

from __future__ import annotations

import argparse
import heapq
import itertools
import math
import time
from dataclasses import asdict
from datetime import date
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import pandas as pd

import fno_oi_common as common
import fno_oi_ema_confirm_optimize as signal_cache
import fno_oi_ema_confirm_sweep as simulator
import fno_oi_hybrid_data as hybrid
import fno_v5_hybrid_backtest as replay
import fno_v5_hybrid_optimize as optimizer
import fno_v5_live_config as config


SESSION = "fno_v5_0926_day_pf_optimize"
OBJECTIVE = "TRAIN_ONLY_ROBUST_0926_DAY_PF"
SIGNAL_SLOT = 925
SIGNAL_END = "09:25"
CONFIRMATION_END = "09:26"
SIDES = ("LONG", "SHORT")

RESULT_DIR = common.FNO_ROOT / "strategy_research"
REPORT_PATH = common.LATEST_DIR / "latest_fno_v5_0926_day_pf_optimize.md"
RANKED_PATH = RESULT_DIR / "v5_0926_day_pf_ranked_portfolios.csv"
SETUPS_PATH = RESULT_DIR / "v5_0926_day_pf_primary_setups.csv"
TRADES_PATH = RESULT_DIR / "v5_0926_day_pf_primary_trades.csv"
DAILY_PATH = RESULT_DIR / "v5_0926_day_pf_primary_daily.csv"
MANIFEST_PATH = RESULT_DIR / "v5_0926_day_pf_manifest.json"
RESEARCH_SETUP_PATH = (
    RESULT_DIR
    / "ema_confirm_0925_0930_0935_0940_0945_v5_full_history_selected_setups.csv"
)


def _finite(value: Any, cap: float = 50.0) -> float:
    return optimizer.finite_rank(value, cap=cap)


def day_pf_key(
    state: optimizer.PortfolioState,
) -> tuple[float, ...]:
    """Rank only train metrics, with robust day PF as the first objective."""

    stats = state.train_metrics
    return (
        _finite(stats["robust_day_pf"]),
        _finite(stats["day_pf"]),
        _finite(stats["worst_fold_pf"]),
        _finite(stats["robust_trade_pf"]),
        float(stats["day_win_rate"]),
        _finite(stats["pf"]),
        float(stats["net_pct"]),
        int(stats["fills"]),
    )


def validate_setup_scope(setup: config.SetupSpec) -> None:
    if setup.signal_end != SIGNAL_END or setup.confirmation_end != CONFIRMATION_END:
        raise AssertionError(f"Non-09:26 setup entered focused search: {setup.setup_id}")
    cap = 1 if setup.side == "LONG" else 2
    if setup.max_entries > cap:
        raise AssertionError(f"Focused setup exceeds V5 cap: {setup.setup_id}")


def validate_output_isolation() -> None:
    protected = {
        config.SELECTED_DAILY_PATH.resolve(),
        Path(config.__file__).resolve(),
        (RESULT_DIR / "ema_confirm_0925_0930_0935_0940_0945_v5_selected_trades.csv").resolve(),
        (RESULT_DIR / "ema_confirm_0925_0930_0935_0940_0945_v5_selected_setups.csv").resolve(),
    }
    outputs = {
        REPORT_PATH.resolve(),
        RANKED_PATH.resolve(),
        SETUPS_PATH.resolve(),
        TRADES_PATH.resolve(),
        DAILY_PATH.resolve(),
        MANIFEST_PATH.resolve(),
    }
    overlap = protected & outputs
    if overlap:
        raise AssertionError(f"09:26 optimizer output overlaps protected V5: {overlap}")


def _push_state(
    heap: list[tuple[tuple[float, ...], int, optimizer.PortfolioState]],
    state: optimizer.PortfolioState,
    counter: int,
    keep_n: int,
) -> None:
    item = (day_pf_key(state), counter, state)
    if len(heap) < keep_n:
        heapq.heappush(heap, item)
    elif item[0] > heap[0][0]:
        heapq.heapreplace(heap, item)


def combine_long_short(
    long_candidates: list[optimizer.LegCandidate],
    short_candidates: list[optimizer.LegCandidate],
    train_days: list[date],
    guards: optimizer.OptimizerGuards,
    *,
    keep_n: int,
) -> tuple[list[optimizer.PortfolioState], int, int]:
    """Exhaust the retained LONG x SHORT pairs without retaining all states."""

    if not long_candidates or not short_candidates:
        raise RuntimeError("Both 09:26 LONG and SHORT candidate sets are required.")
    heap: list[tuple[tuple[float, ...], int, optimizer.PortfolioState]] = []
    evaluated = 0
    valid = 0
    counter = 0
    for long_no, long_choice in enumerate(long_candidates, start=1):
        for short_choice in short_candidates:
            evaluated += 1
            net = np.concatenate((long_choice.train_net, short_choice.train_net))
            day_idx = np.concatenate(
                (long_choice.train_day_idx, short_choice.train_day_idx)
            )
            orders = int(
                long_choice.train_metrics["orders"]
                + short_choice.train_metrics["orders"]
            )
            metrics = optimizer.score_vectors(
                net,
                day_idx,
                len(train_days),
                orders=orders,
            )
            if not optimizer.passes_portfolio_guards(metrics, guards):
                continue
            valid += 1
            counter += 1
            state = optimizer.PortfolioState(
                choices=(long_choice, short_choice),
                train_net=net,
                train_day_idx=day_idx,
                train_orders=orders,
                train_metrics=metrics,
            )
            _push_state(heap, state, counter, keep_n)
        if long_no % 25 == 0 or long_no == len(long_candidates):
            print(
                f"[PAIR] long={long_no}/{len(long_candidates)} "
                f"evaluated={evaluated:,} valid={valid:,}",
                flush=True,
            )
    states = [item[2] for item in heap]
    states.sort(key=day_pf_key, reverse=True)
    return states, evaluated, valid


def _periods_for_audit(
    audit: pd.DataFrame,
    train_days: list[date],
    test_days: list[date],
    all_days: list[date],
) -> dict[str, dict[str, Any]]:
    return {
        "TRAIN": optimizer.score_audit(audit, train_days),
        "TEST": optimizer.score_audit(audit, test_days),
        "ALL": optimizer.score_audit(audit, all_days),
    }


def replay_setup_book(
    signals: pd.DataFrame,
    paths: dict[int, dict[str, np.ndarray]],
    setups: Iterable[config.SetupSpec],
    train_days: list[date],
    test_days: list[date],
    all_days: list[date],
    *,
    cost_bps: float,
) -> tuple[pd.DataFrame, dict[str, dict[str, Any]]]:
    selected = tuple(setups)
    for setup in selected:
        validate_setup_scope(setup)
    audit = replay.replay_setups(
        signals,
        paths,
        cost_bps=cost_bps,
        setups=selected,
    )
    return audit, _periods_for_audit(audit, train_days, test_days, all_days)


def current_0926_setups() -> tuple[config.SetupSpec, ...]:
    setups = tuple(
        setup for setup in config.ACTIVE_SETUPS if setup.signal_end == SIGNAL_END
    )
    if {setup.side for setup in setups} != set(SIDES):
        raise RuntimeError("Current V5 does not contain both 09:26 legs.")
    return setups


def _setup_from_row(row: pd.Series) -> config.SetupSpec:
    return config.SetupSpec(
        signal_end=SIGNAL_END,
        confirmation_end=CONFIRMATION_END,
        side=str(row["side"]),
        mode=str(row["mode"]),
        max_entries=int(row["max_entries"]),
        picker=str(row["picker"]),
        price_change_pct=float(row["price_change_pct"]),
        oi_change_pct=float(row["oi_change_pct"]),
        volume_ratio=float(row["volume_ratio"]),
        body_ratio=float(row["body_ratio"]),
        max_wick_ratio=float(row["max_wick_ratio"]),
        min_traded_value=float(row.get("min_traded_value", 0.0)),
        stop_pct=float(row["stop_pct"]),
        target_pct=float(row["target_pct"]),
        source_version=str(row.get("source_version", "V5_FULL_HISTORY_BEST_TRADE_PF")),
    )


def research_pf_8590_setups() -> tuple[config.SetupSpec, ...]:
    if not RESEARCH_SETUP_PATH.exists():
        raise FileNotFoundError(f"PF-8.590 setup audit is missing: {RESEARCH_SETUP_PATH}")
    frame = pd.read_csv(RESEARCH_SETUP_PATH)
    selected = frame.loc[
        frame["objective"].eq("BEST_TRADE_PF")
        & frame["confirmation_end"].eq(CONFIRMATION_END)
    ].copy()
    setups = tuple(_setup_from_row(row) for _, row in selected.iterrows())
    if {setup.side for setup in setups} != set(SIDES):
        raise RuntimeError("PF-8.590 audit does not contain both 09:26 legs.")
    return setups


def setup_frame(
    state: optimizer.PortfolioState,
    train_days: list[date],
    test_days: list[date],
    all_days: list[date],
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for candidate in state.choices:
        if candidate is None:
            continue
        validate_setup_scope(candidate.setup)
        single = optimizer.PortfolioState(
            choices=(candidate,),
            train_net=candidate.train_net,
            train_day_idx=candidate.train_day_idx,
            train_orders=int(candidate.train_metrics["orders"]),
            train_metrics=candidate.train_metrics,
        )
        audit = optimizer.state_audit(single)
        periods = _periods_for_audit(audit, train_days, test_days, all_days)
        row: dict[str, Any] = {
            **asdict(candidate.setup),
            "candidate_id": candidate.candidate_id,
            "objective": OBJECTIVE,
            "data_contract": hybrid.DATA_CONTRACT_VERSION,
        }
        for period, stats in periods.items():
            for key in ("orders", "fills", "pf", "day_pf", "net_pct"):
                row[f"{period.lower()}_{key}"] = stats[key]
        rows.append(row)
    return pd.DataFrame(rows)


def ranked_frame(
    states: list[optimizer.PortfolioState],
    periods: list[dict[str, dict[str, Any]]],
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for rank, (state, scores) in enumerate(zip(states, periods), start=1):
        active = [choice for choice in state.choices if choice is not None]
        row: dict[str, Any] = {
            "train_rank": rank,
            "objective": OBJECTIVE,
            "setup_ids": ",".join(choice.setup.setup_id for choice in active),
            "candidate_ids": ",".join(choice.candidate_id for choice in active),
        }
        for period, stats in scores.items():
            for key in (
                "orders",
                "fills",
                "wins",
                "losses",
                "pf",
                "day_pf",
                "robust_trade_pf",
                "robust_day_pf",
                "net_pct",
                "active_days",
                "positive_days",
                "negative_days",
                "day_win_rate",
                "top_day_share",
                "worst_fold_pf",
            ):
                row[f"{period.lower()}_{key}"] = stats[key]
        rows.append(row)
    return pd.DataFrame(rows)


def _fmt(value: Any, digits: int = 3) -> str:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return ""
    if math.isnan(number):
        return ""
    if math.isinf(number):
        return "INF"
    return f"{number:.{digits}f}"


def _signed(value: Any, digits: int = 3) -> str:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return ""
    if not math.isfinite(number):
        return _fmt(number, digits)
    return f"{number:+.{digits}f}"


def _entry_details(audit: pd.DataFrame, day: date) -> str:
    rows = audit.loc[audit["day"].eq(day) & audit["filled"]]
    if rows.empty:
        return "-"
    return "; ".join(
        f"{row.side[0]} {row.tradingsymbol} {_signed(row.net_return_pct)}%"
        for row in rows.itertuples(index=False)
    )


def render_report(
    primary_periods: dict[str, dict[str, Any]],
    current_periods: dict[str, dict[str, Any]],
    research_periods: dict[str, dict[str, Any]],
    ranked: pd.DataFrame,
    setups: pd.DataFrame,
    daily: pd.DataFrame,
    audit: pd.DataFrame,
    *,
    split_day: date,
    guards: optimizer.OptimizerGuards,
    selection_evaluated: int,
    pair_evaluated: int,
    valid_pairs: int,
    cost_bps: float,
) -> str:
    lines = [
        "# FNO V5 09:26 Train-Only Day-PF Optimisation",
        "",
        f"Data contract: `{hybrid.DATA_CONTRACT_VERSION}`",
        f"Signal/entry: `{SIGNAL_END}` five-minute scan -> `{CONFIRMATION_END}` one-minute confirmation/entry",
        f"Train/test split: `{split_day.isoformat()}`",
        f"Round-trip cost: `{cost_bps:g} bps`",
        f"Objective: `{OBJECTIVE}`",
        f"Selection/bracket evaluations: `{selection_evaluated:,}`; retained pair evaluations: `{pair_evaluated:,}`; guard-valid pairs: `{valid_pairs:,}`",
        "",
        "The primary portfolio was selected using TRAIN metrics only. TEST was evaluated after the train ranking was frozen. Protected V5 and live configuration were not changed.",
        "",
        "## Total Comparison",
        "",
        "Strategy | Period | Orders/Fills | W/L | Trade PF | Day PF | Robust day PF | Net % | Active days",
        "--- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---:",
    ]
    comparisons = (
        ("Current protected V5 09:26", current_periods),
        ("PF-8.590 artifact 09:26", research_periods),
        ("Optimized 09:26 primary", primary_periods),
    )
    for label, periods in comparisons:
        for period in ("TRAIN", "TEST", "ALL"):
            stats = periods[period]
            lines.append(
                f"{label} | {period} | {stats['orders']}/{stats['fills']} | "
                f"{stats['wins']}/{stats['losses']} | {_fmt(stats['pf'])} | "
                f"{_fmt(stats['day_pf'])} | {_fmt(stats['robust_day_pf'])} | "
                f"{_signed(stats['net_pct'])}% | {stats['active_days']}"
            )

    lines += [
        "",
        "## Primary Parameters",
        "",
        "Side | Mode | Max | Picker | Price | OI | Volume | Body | Wick | Min value | Stop | Target | Train fills | Train day PF | Test fills | Test day PF",
        "--- | --- | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---:",
    ]
    for row in setups.itertuples(index=False):
        lines.append(
            f"{row.side} | {row.mode} | {row.max_entries} | {row.picker} | "
            f"{_fmt(row.price_change_pct, 2)} | {_fmt(row.oi_change_pct, 2)} | "
            f"{_fmt(row.volume_ratio, 2)} | {_fmt(row.body_ratio, 2)} | "
            f"{_fmt(row.max_wick_ratio, 2)} | {_fmt(row.min_traded_value, 0)} | "
            f"{_fmt(row.stop_pct, 2)}% | {_fmt(row.target_pct, 2)}% | "
            f"{row.train_fills} | {_fmt(row.train_day_pf)} | "
            f"{row.test_fills} | {_fmt(row.test_day_pf)}"
        )

    lines += [
        "",
        "## Frozen Train Shortlist",
        "",
        "Rank | Train O/F | Train PF | Train day PF | Robust day PF | Test O/F | Test PF | Test day PF | Test net % | All day PF",
        "---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---:",
    ]
    for row in ranked.head(20).itertuples(index=False):
        lines.append(
            f"{row.train_rank} | {row.train_orders}/{row.train_fills} | "
            f"{_fmt(row.train_pf)} | {_fmt(row.train_day_pf)} | "
            f"{_fmt(row.train_robust_day_pf)} | "
            f"{row.test_orders}/{row.test_fills} | {_fmt(row.test_pf)} | "
            f"{_fmt(row.test_day_pf)} | {_signed(row.test_net_pct)}% | "
            f"{_fmt(row.all_day_pf)}"
        )

    lines += [
        "",
        "## Day-wise Primary",
        "",
        "Day | Period | L O/F | Long % | S O/F | Short % | Total % | Cum % | Cum trade PF | Cum day PF | Entries",
        "--- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---",
    ]
    for row in daily.itertuples(index=False):
        lines.append(
            f"{row.day} | {row.period} | {row.long_selections}/{row.long_fills} | "
            f"{_signed(row.long_return_pct)}% | "
            f"{row.short_selections}/{row.short_fills} | "
            f"{_signed(row.short_return_pct)}% | "
            f"{_signed(row.portfolio_net_return_pct)}% | "
            f"{_signed(row.cumulative_net_pct)}% | "
            f"{_fmt(row.cumulative_trade_pf)} | "
            f"{_fmt(row.cumulative_day_pf)} | {_entry_details(audit, row.day)}"
        )

    lines += [
        "",
        "## Search Space and Guards",
        "",
        f"- Price thresholds: `{optimizer.FILTER_GRID['price_change_pct']}`.",
        f"- OI thresholds: `{optimizer.FILTER_GRID['oi_change_pct']}`.",
        f"- Volume thresholds: `{optimizer.FILTER_GRID['volume_ratio']}`.",
        f"- Body thresholds: `{optimizer.FILTER_GRID['body_ratio']}`; wick ceilings: `{optimizer.FILTER_GRID['max_wick_ratio']}`.",
        f"- Traded-value floors: `{optimizer.FILTER_GRID['min_traded_value']}`; pickers: `{optimizer.PICKERS}`.",
        f"- Stops: `{optimizer.STOP_PCTS}`; targets: `{optimizer.TARGET_PCTS}`; caps: 1 LONG and 2 SHORT.",
        f"- Each leg needs at least {guards.min_leg_train_fills} train fills. The portfolio needs at least {guards.min_portfolio_train_fills} fills across {guards.min_portfolio_train_days} active train days.",
        f"- Day win >= {guards.min_day_win:.0%}; best day <= {guards.max_top_day_share:.0%} of train net; all three train folds profitable; worst fold PF >= {guards.min_worst_fold_pf:.2f}.",
        "- Futures data supplies OI and OI percentage change only. NSE equity data supplies price, volume, indicators, confirmation, entry and exits.",
        "- Historical OI still uses the available 26AUG contract rather than a rolling near-month series. Fifty-four sessions remain a small sample.",
        "",
        "## Outputs",
        "",
        f"- Ranked portfolios: `{RANKED_PATH}`",
        f"- Primary setups: `{SETUPS_PATH}`",
        f"- Primary trades: `{TRADES_PATH}`",
        f"- Primary day-wise curve: `{DAILY_PATH}`",
        f"- Reproducibility manifest: `{MANIFEST_PATH}`",
    ]
    return "\n".join(lines) + "\n"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--split-day", default="2026-07-17")
    parser.add_argument("--cost-bps", type=float, default=5.0)
    parser.add_argument("--square-off", default="1530")
    parser.add_argument("--max-forward-bars", type=int, default=400)
    parser.add_argument("--rebuild-cache", action="store_true")
    parser.add_argument("--leg-retain-n", type=int, default=300)
    parser.add_argument("--top-n", type=int, default=50)
    parser.add_argument("--min-leg-train-fills", type=int, default=8)
    parser.add_argument("--min-portfolio-train-fills", type=int, default=20)
    parser.add_argument("--min-portfolio-train-days", type=int, default=15)
    parser.add_argument("--min-day-win", type=float, default=0.50)
    parser.add_argument("--max-top-day-share", type=float, default=0.35)
    parser.add_argument("--min-worst-fold-pf", type=float, default=1.00)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    validate_output_isolation()
    started = time.monotonic()
    common.publish_status(SESSION, "RUNNING")
    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    common.LATEST_DIR.mkdir(parents=True, exist_ok=True)

    guards = optimizer.OptimizerGuards(
        min_leg_train_fills=args.min_leg_train_fills,
        min_portfolio_train_fills=args.min_portfolio_train_fills,
        min_portfolio_train_days=args.min_portfolio_train_days,
        min_day_win=args.min_day_win,
        max_top_day_share=args.max_top_day_share,
        min_worst_fold_pf=args.min_worst_fold_pf,
    )
    signals, paths = signal_cache.load_signals(
        args.square_off,
        args.max_forward_bars,
        args.rebuild_cache,
    )
    contracts = set(signals["data_contract"].dropna().astype(str))
    if contracts != {hybrid.DATA_CONTRACT_VERSION}:
        raise RuntimeError(f"Unexpected hybrid signal contract: {sorted(contracts)}")
    all_days = sorted(set(signals["day"]))
    split_day = pd.Timestamp(args.split_day).date()
    train_days = [day for day in all_days if day < split_day]
    test_days = [day for day in all_days if day >= split_day]
    if not train_days or not test_days:
        raise RuntimeError("Both train and frozen test windows are required.")
    train_set = set(train_days)
    all_day_code = {day: idx for idx, day in enumerate(all_days)}
    slot_signals = signals.loc[signals["hhmm_int"].eq(SIGNAL_SLOT)].copy()
    slot_signals = slot_signals.sort_values(
        ["day", "tradingsymbol", "sid"], kind="stable"
    ).reset_index(drop=True)
    print(
        f"[DATA] 09:25 rows={len(slot_signals):,} sessions={len(all_days)} "
        f"train={len(train_days)} test={len(test_days)}",
        flush=True,
    )

    bracket_net = {
        (stop_pct, target_pct): simulator.simulate_bracket(
            slot_signals,
            paths,
            stop_pct=stop_pct,
            target_pct=target_pct,
            cost_bps=args.cost_bps,
        )
        for stop_pct, target_pct in itertools.product(
            optimizer.STOP_PCTS,
            optimizer.TARGET_PCTS,
        )
    }
    candidates: dict[str, list[optimizer.LegCandidate]] = {}
    selection_evaluated = 0
    for side in SIDES:
        side_candidates, evaluated = optimizer.optimise_leg(
            slot_signals,
            bracket_net,
            SIGNAL_SLOT,
            side,
            train_set,
            all_day_code,
            guards,
            retain_n=args.leg_retain_n,
            search_profile="full-grid",
            candidate_source_version="V5_0926_TRAIN_ONLY_DAY_PF_GRID",
        )
        for candidate in side_candidates:
            validate_setup_scope(candidate.setup)
        candidates[side] = side_candidates
        selection_evaluated += evaluated
        print(
            f"[LEG {side}] retained={len(side_candidates)} evaluated={evaluated:,}",
            flush=True,
        )

    states, pair_evaluated, valid_pairs = combine_long_short(
        candidates["LONG"],
        candidates["SHORT"],
        train_days,
        guards,
        keep_n=args.top_n,
    )
    if not states:
        raise RuntimeError("No 09:26 LONG/SHORT pair survived the train guards.")

    audits: list[pd.DataFrame] = []
    evaluations: list[dict[str, dict[str, Any]]] = []
    for state in states:
        audit, periods = optimizer.evaluate_state(
            state,
            train_days,
            test_days,
            all_days,
        )
        audit["objective"] = OBJECTIVE
        audits.append(audit)
        evaluations.append(periods)
    primary = states[0]
    primary_audit = audits[0]
    primary_periods = evaluations[0]

    _, current_periods = replay_setup_book(
        signals,
        paths,
        current_0926_setups(),
        train_days,
        test_days,
        all_days,
        cost_bps=args.cost_bps,
    )
    _, research_periods = replay_setup_book(
        signals,
        paths,
        research_pf_8590_setups(),
        train_days,
        test_days,
        all_days,
        cost_bps=args.cost_bps,
    )

    ranked = ranked_frame(states, evaluations)
    setups = setup_frame(primary, train_days, test_days, all_days)
    daily = replay.build_daily_curve(
        primary_audit,
        all_days,
        split_day=split_day,
    )
    daily["objective"] = OBJECTIVE
    common.atomic_write_csv(ranked, RANKED_PATH)
    common.atomic_write_csv(setups, SETUPS_PATH)
    common.atomic_write_csv(primary_audit, TRADES_PATH)
    common.atomic_write_csv(daily, DAILY_PATH)

    manifest = {
        "objective": OBJECTIVE,
        "data_contract": hybrid.DATA_CONTRACT_VERSION,
        "signal_end": SIGNAL_END,
        "confirmation_end": CONFIRMATION_END,
        "split_day": split_day.isoformat(),
        "first_session": all_days[0].isoformat(),
        "last_session": all_days[-1].isoformat(),
        "sessions": len(all_days),
        "round_trip_cost_bps": args.cost_bps,
        "guards": asdict(guards),
        "filter_grid": optimizer.FILTER_GRID,
        "pickers": optimizer.PICKERS,
        "stop_pcts": optimizer.STOP_PCTS,
        "target_pcts": optimizer.TARGET_PCTS,
        "selection_evaluated": selection_evaluated,
        "pair_evaluated": pair_evaluated,
        "valid_pairs": valid_pairs,
        "primary_setups": [
            asdict(choice.setup) for choice in primary.choices if choice is not None
        ],
        "primary_periods": primary_periods,
        "outputs": {
            "ranked": str(RANKED_PATH),
            "setups": str(SETUPS_PATH),
            "trades": str(TRADES_PATH),
            "daily": str(DAILY_PATH),
            "report": str(REPORT_PATH),
        },
    }
    common.atomic_write_json(MANIFEST_PATH, manifest)
    report = render_report(
        primary_periods,
        current_periods,
        research_periods,
        ranked,
        setups,
        daily,
        primary_audit,
        split_day=split_day,
        guards=guards,
        selection_evaluated=selection_evaluated,
        pair_evaluated=pair_evaluated,
        valid_pairs=valid_pairs,
        cost_bps=args.cost_bps,
    )
    common.atomic_write_text(REPORT_PATH, report)

    train_stats = primary_periods["TRAIN"]
    test_stats = primary_periods["TEST"]
    all_stats = primary_periods["ALL"]
    duration = time.monotonic() - started
    common.publish_status(
        SESSION,
        "SUCCESS",
        duration_sec=round(duration, 1),
        train_day_pf=train_stats["day_pf"],
        test_day_pf=test_stats["day_pf"],
        all_day_pf=all_stats["day_pf"],
    )
    print(
        f"[PRIMARY] TRAIN day PF={train_stats['day_pf']:.3f} "
        f"TEST day PF={test_stats['day_pf']:.3f} "
        f"ALL day PF={all_stats['day_pf']:.3f} "
        f"net={all_stats['net_pct']:+.3f}%",
        flush=True,
    )
    print(f"[DONE] {duration:.1f}s | {REPORT_PATH}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
