"""Research-only all-history day-PF ceiling for FNO V5 09:26 entries.

Unlike ``fno_v5_0926_day_pf_optimize.py``, this module deliberately fits on
every available session.  Its result is useful as a parameter-sweep ceiling,
not as out-of-sample evidence, and it writes to separate research artifacts.
"""

from __future__ import annotations

import argparse
import itertools
import math
import time
from dataclasses import asdict
from pathlib import Path
from typing import Any

import pandas as pd

import fno_oi_common as common
import fno_oi_ema_confirm_optimize as signal_cache
import fno_oi_ema_confirm_sweep as simulator
import fno_oi_hybrid_data as hybrid
import fno_v5_0926_day_pf_optimize as focused
import fno_v5_hybrid_backtest as replay
import fno_v5_hybrid_optimize as optimizer
import fno_v5_live_config as config


SESSION = "fno_v5_0926_all_history_day_pf_optimize"
OBJECTIVE = "ALL_HISTORY_0926_MAX_ROBUST_DAY_PF_RESEARCH"
RESULT_DIR = common.FNO_ROOT / "strategy_research"
REPORT_PATH = common.LATEST_DIR / "latest_fno_v5_0926_all_history_day_pf.md"
RANKED_PATH = RESULT_DIR / "v5_0926_all_history_day_pf_ranked_portfolios.csv"
SETUPS_PATH = RESULT_DIR / "v5_0926_all_history_day_pf_primary_setups.csv"
TRADES_PATH = RESULT_DIR / "v5_0926_all_history_day_pf_primary_trades.csv"
DAILY_PATH = RESULT_DIR / "v5_0926_all_history_day_pf_primary_daily.csv"
MANIFEST_PATH = RESULT_DIR / "v5_0926_all_history_day_pf_manifest.json"


def validate_output_isolation() -> None:
    protected = {
        config.SELECTED_DAILY_PATH.resolve(),
        Path(config.__file__).resolve(),
        focused.REPORT_PATH.resolve(),
        focused.RANKED_PATH.resolve(),
        focused.SETUPS_PATH.resolve(),
        focused.TRADES_PATH.resolve(),
        focused.DAILY_PATH.resolve(),
        focused.MANIFEST_PATH.resolve(),
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
        raise AssertionError(f"All-history 09:26 output overlap: {overlap}")


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


def _max_additive_drawdown(daily: pd.DataFrame) -> float:
    cumulative = daily["cumulative_net_pct"].to_numpy(float)
    peak = 0.0
    drawdown = 0.0
    for value in cumulative:
        peak = max(peak, float(value))
        drawdown = max(drawdown, peak - float(value))
    return drawdown


def render_report(
    periods: dict[str, dict[str, Any]],
    current_periods: dict[str, dict[str, Any]],
    artifact_periods: dict[str, dict[str, Any]],
    ranked: pd.DataFrame,
    setups: pd.DataFrame,
    daily: pd.DataFrame,
    audit: pd.DataFrame,
    *,
    split_day: pd.Timestamp,
    guards: optimizer.OptimizerGuards,
    selection_evaluated: int,
    pair_evaluated: int,
    valid_pairs: int,
    cost_bps: float,
) -> str:
    all_stats = periods["ALL"]
    lines = [
        "# FNO V5 09:26 All-History Day-PF Research Ceiling",
        "",
        "**Research-only:** every available session was used for parameter selection. TRAIN/TEST rows below are descriptive slices, not out-of-sample validation.",
        "",
        f"Data contract: `{hybrid.DATA_CONTRACT_VERSION}`",
        f"Signal/entry: `{focused.SIGNAL_END}` five-minute scan -> `{focused.CONFIRMATION_END}` one-minute confirmation/entry",
        f"Label split: `{split_day.date().isoformat()}`",
        f"Round-trip cost: `{cost_bps:g} bps`",
        f"Objective: `{OBJECTIVE}`",
        f"Selection/bracket evaluations: `{selection_evaluated:,}`; pair evaluations: `{pair_evaluated:,}`; guard-valid pairs: `{valid_pairs:,}`",
        "",
        "## Total Comparison",
        "",
        "Strategy | Orders/Fills | W/L | Trade PF | Day PF | Robust day PF | Net % | Active days",
        "--- | ---: | ---: | ---: | ---: | ---: | ---: | ---:",
    ]
    for label, scores in (
        ("Current protected V5 09:26", current_periods["ALL"]),
        ("PF-8.590 artifact 09:26", artifact_periods["ALL"]),
        ("All-history max-day-PF primary", all_stats),
    ):
        lines.append(
            f"{label} | {scores['orders']}/{scores['fills']} | "
            f"{scores['wins']}/{scores['losses']} | {_fmt(scores['pf'])} | "
            f"{_fmt(scores['day_pf'])} | {_fmt(scores['robust_day_pf'])} | "
            f"{_signed(scores['net_pct'])}% | {scores['active_days']}"
        )

    lines += [
        "",
        f"Maximum additive drawdown: **{_fmt(_max_additive_drawdown(daily))}%**.",
        "",
        "## Descriptive Time Slices",
        "",
        "Period | Orders/Fills | W/L | Trade PF | Day PF | Net % | Active days",
        "--- | ---: | ---: | ---: | ---: | ---: | ---:",
    ]
    for period in ("TRAIN", "TEST", "ALL"):
        stats = periods[period]
        lines.append(
            f"{period} | {stats['orders']}/{stats['fills']} | "
            f"{stats['wins']}/{stats['losses']} | {_fmt(stats['pf'])} | "
            f"{_fmt(stats['day_pf'])} | {_signed(stats['net_pct'])}% | "
            f"{stats['active_days']}"
        )

    lines += [
        "",
        "## Primary Parameters",
        "",
        "Side | Mode | Max | Picker | Price | OI | Volume | Body | Wick | Min value | Stop | Target | All fills | All PF | All day PF | All net %",
        "--- | --- | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---:",
    ]
    for row in setups.itertuples(index=False):
        lines.append(
            f"{row.side} | {row.mode} | {row.max_entries} | {row.picker} | "
            f"{_fmt(row.price_change_pct, 2)} | {_fmt(row.oi_change_pct, 2)} | "
            f"{_fmt(row.volume_ratio, 2)} | {_fmt(row.body_ratio, 2)} | "
            f"{_fmt(row.max_wick_ratio, 2)} | {_fmt(row.min_traded_value, 0)} | "
            f"{_fmt(row.stop_pct, 2)}% | {_fmt(row.target_pct, 2)}% | "
            f"{row.all_fills} | {_fmt(row.all_pf)} | {_fmt(row.all_day_pf)} | "
            f"{_signed(row.all_net_pct)}%"
        )

    lines += [
        "",
        "## Top Research Portfolios",
        "",
        "Rank | All O/F | Trade PF | Day PF | Robust day PF | Net % | Active days | TRAIN day PF | TEST day PF",
        "---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---:",
    ]
    for row in ranked.head(20).itertuples(index=False):
        lines.append(
            f"{row.train_rank} | {row.all_orders}/{row.all_fills} | "
            f"{_fmt(row.all_pf)} | {_fmt(row.all_day_pf)} | "
            f"{_fmt(row.all_robust_day_pf)} | {_signed(row.all_net_pct)}% | "
            f"{row.all_active_days} | {_fmt(row.train_day_pf)} | "
            f"{_fmt(row.test_day_pf)}"
        )

    lines += [
        "",
        "## Day-wise Primary",
        "",
        "Day | Label | L O/F | Long % | S O/F | Short % | Total % | Cum % | Cum trade PF | Cum day PF | Entries",
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
            f"{_fmt(row.cumulative_day_pf)} | "
            f"{focused._entry_details(audit, row.day)}"
        )

    lines += [
        "",
        "## Guardrails and Limits",
        "",
        f"- Minimum all-history fills: {guards.min_portfolio_train_fills}; active days: {guards.min_portfolio_train_days}; day win: {guards.min_day_win:.0%}.",
        f"- Best day <= {guards.max_top_day_share:.0%} of net; all three contiguous folds profitable; worst fold PF >= {guards.min_worst_fold_pf:.2f}.",
        "- The parameter grid varies price, OI, volume, body, wick, traded value, picker, entry cap, stop and target for both sides.",
        "- The all-history fit creates selection bias. It must not replace protected/live V5 without new forward evidence.",
        "- Historical OI uses the available 26AUG contract rather than a rolling near-month series.",
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
    parser.add_argument("--min-leg-fills", type=int, default=8)
    parser.add_argument("--min-portfolio-fills", type=int, default=20)
    parser.add_argument("--min-portfolio-days", type=int, default=15)
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
        min_leg_train_fills=args.min_leg_fills,
        min_portfolio_train_fills=args.min_portfolio_fills,
        min_portfolio_train_days=args.min_portfolio_days,
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
    split_day = pd.Timestamp(args.split_day)
    split_date = split_day.date()
    train_days = [day for day in all_days if day < split_date]
    test_days = [day for day in all_days if day >= split_date]
    fit_set = set(all_days)
    all_day_code = {day: idx for idx, day in enumerate(all_days)}
    slot_signals = signals.loc[signals["hhmm_int"].eq(focused.SIGNAL_SLOT)].copy()
    slot_signals = slot_signals.sort_values(
        ["day", "tradingsymbol", "sid"], kind="stable"
    ).reset_index(drop=True)
    print(
        f"[DATA] all-history fit rows={len(slot_signals):,} sessions={len(all_days)}",
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
    for side in focused.SIDES:
        side_candidates, evaluated = optimizer.optimise_leg(
            slot_signals,
            bracket_net,
            focused.SIGNAL_SLOT,
            side,
            fit_set,
            all_day_code,
            guards,
            retain_n=args.leg_retain_n,
            search_profile="full-grid",
            candidate_source_version="V5_0926_ALL_HISTORY_DAY_PF_GRID",
        )
        candidates[side] = side_candidates
        selection_evaluated += evaluated
        print(
            f"[LEG {side}] retained={len(side_candidates)} evaluated={evaluated:,}",
            flush=True,
        )

    states, pair_evaluated, valid_pairs = focused.combine_long_short(
        candidates["LONG"],
        candidates["SHORT"],
        all_days,
        guards,
        keep_n=args.top_n,
    )
    if not states:
        raise RuntimeError("No all-history 09:26 pair survived the guards.")

    audits: list[pd.DataFrame] = []
    periods: list[dict[str, dict[str, Any]]] = []
    for state in states:
        audit, scores = optimizer.evaluate_state(
            state,
            train_days,
            test_days,
            all_days,
        )
        audit["objective"] = OBJECTIVE
        audits.append(audit)
        periods.append(scores)
    primary = states[0]
    primary_audit = audits[0]
    primary_periods = periods[0]

    _, current_periods = focused.replay_setup_book(
        signals,
        paths,
        focused.current_0926_setups(),
        train_days,
        test_days,
        all_days,
        cost_bps=args.cost_bps,
    )
    _, artifact_periods = focused.replay_setup_book(
        signals,
        paths,
        focused.research_pf_8590_setups(),
        train_days,
        test_days,
        all_days,
        cost_bps=args.cost_bps,
    )

    ranked = focused.ranked_frame(states, periods)
    ranked["objective"] = OBJECTIVE
    setups = focused.setup_frame(primary, train_days, test_days, all_days)
    setups["objective"] = OBJECTIVE
    daily = replay.build_daily_curve(
        primary_audit,
        all_days,
        split_day=split_date,
    )
    daily["objective"] = OBJECTIVE
    common.atomic_write_csv(ranked, RANKED_PATH)
    common.atomic_write_csv(setups, SETUPS_PATH)
    common.atomic_write_csv(primary_audit, TRADES_PATH)
    common.atomic_write_csv(daily, DAILY_PATH)

    manifest = {
        "objective": OBJECTIVE,
        "selection_scope": "ALL_AVAILABLE_SESSIONS",
        "out_of_sample": False,
        "data_contract": hybrid.DATA_CONTRACT_VERSION,
        "signal_end": focused.SIGNAL_END,
        "confirmation_end": focused.CONFIRMATION_END,
        "first_session": all_days[0].isoformat(),
        "last_session": all_days[-1].isoformat(),
        "sessions": len(all_days),
        "label_split_day": split_date.isoformat(),
        "round_trip_cost_bps": args.cost_bps,
        "guards": asdict(guards),
        "filter_grid": optimizer.FILTER_GRID,
        "pickers": optimizer.PICKERS,
        "stop_pcts": optimizer.STOP_PCTS,
        "target_pcts": optimizer.TARGET_PCTS,
        "selection_evaluated": selection_evaluated,
        "pair_evaluated": pair_evaluated,
        "valid_pairs": valid_pairs,
        "primary_setups": [asdict(choice.setup) for choice in primary.choices],
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
        artifact_periods,
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

    stats = primary_periods["ALL"]
    duration = time.monotonic() - started
    common.publish_status(
        SESSION,
        "SUCCESS",
        duration_sec=round(duration, 1),
        all_day_pf=stats["day_pf"],
        all_trade_pf=stats["pf"],
        all_net_pct=stats["net_pct"],
    )
    print(
        f"[PRIMARY] fills={stats['fills']} trade PF={stats['pf']:.3f} "
        f"day PF={stats['day_pf']:.3f} net={stats['net_pct']:+.3f}%",
        flush=True,
    )
    print(f"[DONE] {duration:.1f}s | {REPORT_PATH}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
