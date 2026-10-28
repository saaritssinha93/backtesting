"""Independent completed-run reconciliation and unlimited trade-impact audit.

Reads completed research evidence only. Writes audit.json and impact.html in
the requested research output directory; never changes strategy/live inputs.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import html
import json
from pathlib import Path
import sys

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from research import g3_early_entry_study_20261023 as study

OUTPUT = study.OUTPUT.parent / "full_history_relaxation"
BASELINE = "G3_V14_original"
TARGETED = {"targeted_OI", "targeted_volume", "combined_morning", "combined_morning_two_0931"}
VARIANTS = {BASELINE, *TARGETED, "turn_vol0.4_breakout_qall", "turn_vol0.4_breakout_q1"}
CAPS = {"10_positions", "unlimited"}
KEYS = ["day", "tradingsymbol", "setup_id", "side"]


def read_json(path):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def close(actual, expected, context, *, atol=1e-6, rtol=1e-10):
    if not np.allclose(actual, expected, atol=atol, rtol=rtol, equal_nan=True):
        raise AssertionError(f"Metric mismatch: {context}; actual={actual}, expected={expected}")


def executed(frame):
    return frame.loc[frame.portfolio_executed.astype(str).str.lower().eq("true")].copy()


def peaks(taken):
    events = []
    for row in taken.to_dict("records"):
        opening = row.get("entry_open_ts")
        entry = pd.Timestamp(opening if pd.notna(opening) else row["entry_ts"])
        end = max(pd.Timestamp(row["exit_ts"]), entry + pd.Timedelta(nanoseconds=1))
        events.extend([(entry.value, 1), (end.value, -1)])
    values = []
    for entries_first in (False, True):
        count = high = 0
        for _, delta in sorted(events, key=lambda pair: (pair[0], -pair[1] if entries_first else pair[1])):
            count += delta
            if count < 0:
                raise AssertionError("Negative open-position count")
            high = max(high, count)
        if count:
            raise AssertionError("Unclosed position in capital accounting")
        values.append(high)
    return dict(peak_positions=values[0], same_minute_conservative_peak_positions=values[1],
                peak_margin_rupees=values[0]*100000, peak_gross_exposure_rupees=values[0]*500000)


def period_rows(book, period):
    if period == "all_available":
        return book
    if period == "through_2026-10-22":
        return book.loc[book.day.lt("2026-10-23")]
    if period == "2026-10-23":
        return book.loc[book.day.eq(period)]
    if len(period) == 7 and period.startswith("2026-"):
        return book.loc[book.day.str.startswith(period)]
    raise AssertionError(f"Unexpected summary period: {period}")


def reconcile(output, expected_days):
    config = read_json(output / "config.json")
    source_days = read_json(study.HISTORY / "summary.json")["days"]
    calendar = sorted(config["days"])
    if len(calendar) != expected_days or len(set(calendar)) != expected_days or calendar != sorted(source_days):
        raise AssertionError("Completed run does not cover the full pinned historical calendar")
    daily = pd.DataFrame(read_json(output / "daywise_results.json"))
    summaries = pd.DataFrame(read_json(output / "summary.json"))
    controls = read_json(output / "controls.json")
    proofs = read_json(output / "provenance.json")
    if len(controls) != expected_days or {r["day"] for r in controls} != set(calendar):
        raise AssertionError("Incomplete or duplicate baseline controls")
    if not all(r["selection"]["matches"] and r["pnl_matches"] for r in controls):
        raise AssertionError("A saved baseline control failed")
    if len(proofs) != expected_days or {r["day"] for r in proofs} != set(calendar):
        raise AssertionError("Incomplete source provenance")
    if daily.duplicated(["day", "variant", "capital_limit"]).any():
        raise AssertionError("Duplicate daily result")
    wanted = {(day, variant, cap) for day in calendar for variant in VARIANTS for cap in CAPS}
    actual = set(daily[["day", "variant", "capital_limit"]].itertuples(index=False, name=None))
    if actual != wanted:
        raise AssertionError("Daily results omit a date, variant or capital control")
    if summaries.duplicated(["variant", "capital_limit", "period"]).any():
        raise AssertionError("Duplicate summary period")
    unlimited = {variant: [] for variant in VARIANTS}
    ledger_rows = 0
    for day in calendar:
        ledger = pd.read_parquet(output / f"orders_{day}.parquet")
        ledger["day"] = ledger.day.astype(str).str[:10]
        if not ledger.day.eq(day).all():
            raise AssertionError(f"Ledger escaped requested day: {day}")
        if ledger.duplicated(["variant", "capital_limit", *KEYS]).any():
            raise AssertionError(f"Duplicate execution identity: {day}")
        ledger_rows += len(ledger)
        for variant in sorted(VARIANTS):
            for cap in sorted(CAPS):
                group = ledger.loc[ledger.variant.eq(variant) & ledger.capital_limit.eq(cap)]
                taken = executed(group)
                pnl = pd.to_numeric(taken.portfolio_net_profit_rupees)
                close(pnl.to_numpy(), pd.to_numeric(taken.net_return_pct).to_numpy()*5000,
                      f"{day} {variant} {cap} fixed-exposure trade PnL")
                rejected = group.loc[~group.portfolio_executed.astype(str).str.lower().eq("true")]
                close(rejected.portfolio_net_profit_rupees.to_numpy(float), 0., f"{day} rejected PnL")
                m = daily.loc[daily.day.eq(day) & daily.variant.eq(variant) & daily.capital_limit.eq(cap)].iloc[0]
                recomputed = dict(orders=len(group), trades=len(taken), wins=int(pnl.gt(0).sum()),
                    losses=int(pnl.lt(0).sum()), net_pnl_rupees=float(pnl.sum()),
                    winning_trade_pnl_rupees=float(pnl.loc[pnl.gt(0)].sum()),
                    losing_trade_pnl_rupees=float(pnl.loc[pnl.lt(0)].sum()), **peaks(taken))
                for field, expected in recomputed.items():
                    close(float(m[field]), expected, f"{day} {variant} {cap} {field}")
                if day == "2026-10-23":
                    requested_count = sum(study.REQUESTED.get(r.tradingsymbol) == r.side for r in taken.itertuples())
                    close(float(m["requested_trades"]), requested_count,
                          f"{day} {variant} {cap} requested symbol AND side coverage")
                if cap == "unlimited":
                    unlimited[variant].append(taken)
                    if variant in TARGETED | {BASELINE}:
                        if len(taken) != int(group.filled.astype(str).str.lower().eq("true").sum()):
                            raise AssertionError("Unlimited original-slot policy rejected a native fill")
                        limited = ledger.loc[ledger.variant.eq(variant) & ledger.capital_limit.eq("10_positions")]
                        if set(group.strategy_version.dropna()) != set(limited.strategy_version.dropna()):
                            raise AssertionError("Capital arms have inconsistent strategy versions")
                        if set(group.strategy_fingerprint.dropna()) != set(limited.strategy_fingerprint.dropna()):
                            raise AssertionError("Capital arms have inconsistent strategy fingerprints")
        print(f"[IMPACT AUDIT] reconciled {day}", flush=True)
    expected_periods = {"all_available", "through_2026-10-22", "2026-10-23", *[d[:7] for d in calendar]}
    expected_summaries = {(v, c, p) for v in VARIANTS for c in CAPS for p in expected_periods}
    if set(summaries[["variant", "capital_limit", "period"]].itertuples(index=False, name=None)) != expected_summaries:
        raise AssertionError("Incomplete summary-period coverage")
    for row in summaries.itertuples(index=False):
        book = daily.loc[daily.variant.eq(row.variant) & daily.capital_limit.eq(row.capital_limit)]
        f = period_rows(book, row.period).sort_values("day")
        trades, wins = int(f.trades.sum()), int(f.wins.sum())
        curve = f.net_pnl_rupees.cumsum()
        drawdown = curve.cummax().clip(lower=0)-curve
        gains, losses = f.winning_trade_pnl_rupees.sum(), f.losing_trade_pnl_rupees.sum()
        recalculated = dict(days=len(f), trades=trades, wins=wins, losses=int(f.losses.sum()),
            win_rate_pct=100*wins/trades if trades else 0., net_pnl_rupees=float(f.net_pnl_rupees.sum()),
            mean_trade_pnl_rupees=float(f.net_pnl_rupees.sum()/trades) if trades else 0.,
            losing_days=int(f.net_pnl_rupees.lt(0).sum()), worst_day_rupees=float(f.net_pnl_rupees.min()),
            maximum_closed_day_drawdown_rupees=float(drawdown.max()),
            peak_positions=int(f.peak_positions.max()), peak_margin_rupees=int(f.peak_margin_rupees.max()),
            requested_trades=int(f.requested_trades.fillna(0).sum()))
        for field, expected in recalculated.items():
            close(float(getattr(row, field)), expected, f"summary {row.variant} {row.capital_limit} {row.period} {field}")
        if losses:
            close(float(row.profit_factor), gains/abs(losses), f"summary profit factor {row.variant} {row.period}")
        elif not pd.isna(row.profit_factor):
            raise AssertionError("Zero-loss profit factor differs from report's null policy")
    pinned = pd.read_csv(study.HISTORY / "daily_results.csv").set_index("day")
    for row in daily.loc[daily.variant.eq(BASELINE) & daily.capital_limit.eq("10_positions")].itertuples():
        close(row.net_pnl_rupees, pinned.loc[row.day, "net_pnl"], f"pinned baseline {row.day}", atol=.01, rtol=1e-7)
    return daily, {v: pd.concat(parts, ignore_index=True) for v, parts in unlimited.items()}, dict(
        complete=True, sessions=expected_days, first_date=calendar[0], last_date=calendar[-1],
        daily_metric_rows=len(daily), summary_rows=len(summaries), ledger_rows=ledger_rows,
        baseline_controls=len(controls), baseline_max_abs_pnl_difference=max(abs(r["actual_pnl"]-r["saved_pnl"]) for r in controls),
        fixed_exposure_per_entry_rupees=500000, modeled_margin_per_entry_rupees=100000,
        validated="All exported execution PnLs, zero rejected PnL, daily metrics, peak margins, summary periods, closed-day drawdowns, pinned baseline controls and profile identities")


def contribution(frame):
    pnl = frame.portfolio_net_profit_rupees
    return dict(trades=len(frame), wins=int(pnl.gt(0).sum()), losses=int(pnl.lt(0).sum()),
                flat=int(pnl.eq(0).sum()), net_pnl_rupees=float(pnl.sum()),
                winners_pnl_rupees=float(pnl.loc[pnl.gt(0)].sum()), losers_pnl_rupees=float(pnl.loc[pnl.lt(0)].sum()))


def cost_sensitivity(daily):
    """Change round-trip cost only; do not imply a slippage/market-impact model."""
    rows = []
    for (variant, cap), book in daily.groupby(["variant", "capital_limit"]):
        for period in ("all_available", "through_2026-10-22", "2026-10-23"):
            frame = period_rows(book, period)
            trades = int(frame.trades.sum())
            original = float(frame.net_pnl_rupees.sum())
            rows.append(dict(variant=variant, capital_limit=cap, period=period,
                trades=trades, net_pnl_5bps_rupees=original,
                net_pnl_10bps_rupees=original-trades*250,
                net_pnl_20bps_rupees=original-trades*750))
    return rows


def requested_coverage(books):
    rows = []
    for variant, book in sorted(books.items()):
        today = book.loc[book.day.eq("2026-10-23")]
        desired = today.loc[today.apply(lambda r: study.REQUESTED.get(r.tradingsymbol) == r.side, axis=1)]
        symbols = sorted(desired.tradingsymbol.unique())
        rows.append(dict(variant=variant, requested_symbol_side_pairs=len(symbols),
            executions=len(desired), symbols=symbols,
            missing=[f"{symbol} {side}" for symbol, side in study.REQUESTED.items() if symbol not in symbols]))
    return rows


def impact(daily, books):
    baseline = books[BASELINE].set_index(KEYS, drop=False)
    base_daily = daily.loc[daily.variant.eq(BASELINE) & daily.capital_limit.eq("unlimited")].set_index("day").net_pnl_rupees
    results = []
    for variant in sorted(VARIANTS-{BASELINE}):
        book = books[variant].set_index(KEYS, drop=False)
        shared = baseline.index.intersection(book.index)
        added = book.loc[book.index.difference(baseline.index)]
        removed = baseline.loc[baseline.index.difference(book.index)]
        before, after = baseline.loc[shared].sort_index(), book.loc[shared].sort_index()
        delta = float(after.portfolio_net_profit_rupees.sum()-before.portfolio_net_profit_rupees.sum())
        per_trade_difference = (after.portfolio_net_profit_rupees-before.portfolio_net_profit_rupees).abs()
        if variant in TARGETED:
            for field in ("entry_price", "exit_price", "net_return_pct", "portfolio_net_profit_rupees", "native_target_pct"):
                close(after[field].to_numpy(float), before[field].to_numpy(float), f"unchanged targeted shared-trade {variant} {field}")
            for field in ("entry_ts", "exit_ts", "exit_reason"):
                if not after[field].astype(str).equals(before[field].astype(str)):
                    raise AssertionError(f"Targeted shared trade changed {field}: {variant}")
        comparison_daily = daily.loc[daily.variant.eq(variant) & daily.capital_limit.eq("unlimited")].set_index("day").net_pnl_rupees
        day_delta = comparison_daily-base_daily
        total_delta = float(book.portfolio_net_profit_rupees.sum()-baseline.portfolio_net_profit_rupees.sum())
        added_stats, removed_stats = contribution(added), contribution(removed)
        close(total_delta, added_stats["net_pnl_rupees"]-removed_stats["net_pnl_rupees"]+delta,
              f"added/removed/shared PnL decomposition {variant}")
        fields = KEYS + ["entry_ts", "entry_price", "exit_ts", "exit_reason", "net_return_pct", "portfolio_net_profit_rupees"]
        results.append(dict(variant=variant, baseline=BASELINE, capital_limit="unlimited",
            net_pnl_rupees=float(book.portfolio_net_profit_rupees.sum()), baseline_delta_rupees=total_delta,
            executed_trades=len(book), baseline_trades=len(baseline), shared_executed_trades=len(shared),
            shared_pnl_delta_rupees=delta, shared_max_abs_trade_pnl_difference=float(per_trade_difference.max()) if len(shared) else 0.,
            shared_targeted_exit_parity="PASS" if variant in TARGETED else "NOT_REQUIRED_DIFFERENT_EXIT_POLICY",
            added=added_stats, removed=removed_stats,
            paired_days_better=int(day_delta.gt(1e-6).sum()), paired_days_worse=int(day_delta.lt(-1e-6).sum()),
            paired_days_unchanged=int(day_delta.abs().le(1e-6).sum()),
            daywise_delta_rupees=[dict(day=str(day), delta_rupees=float(value)) for day,value in day_delta.sort_index().items()],
            added_trades=added[fields].reset_index(drop=True).to_dict("records"),
            removed_trades=removed[fields].reset_index(drop=True).to_dict("records")))
    return results


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=OUTPUT)
    parser.add_argument("--expected-days", type=int, default=57)
    args = parser.parse_args()
    output = args.output.resolve()
    if not output.is_relative_to((ROOT / "outputs/g3_early_entry_20261023").resolve()):
        raise ValueError("Audit output must stay inside the isolated research output tree")
    daily, books, checks = reconcile(output, args.expected_days)
    effects = impact(daily, books)
    costs = cost_sensitivity(daily)
    coverage = requested_coverage(books)
    payload = dict(scope="OFFLINE_RESEARCH_IMPACT_AUDIT", execution_authority=False,
        created_at=datetime.now(timezone.utc).isoformat(), audit_code_sha256=study.sha(__file__),
        run_config_sha256=study.sha(output / "config.json"),
        baseline_unlimited=contribution(books[BASELINE]), checks=checks, variants=effects,
        fixed_fill_cost_sensitivity=costs, requested_coverage_unlimited=coverage,
        notes=["Comparison key: day, symbol, setup and side. Only executed trades count.",
               "57 available baseline sessions, not every scheduled session: August 24, August 25 and October 1 are missing; 57 of 60 scheduled sessions are covered.",
               "The first eight sessions use an after-dated monthly universe; this is not a strict point-in-time-universe validation.",
               "Targeted variants must preserve entry/exit prices, timestamps, target and net PnL for every shared trade.",
               "Broad variants use different exits and one-trade-per-symbol rules; their shared-trade PnL may differ.",
               "Cost sensitivity changes only round-trip cost from 5 bps to 10 or 20 bps at fixed Rs 500,000 exposure, with unchanged fills and no market-impact model.",
               "Effects are retrospective. Existing universe and minute-OHLC limitations remain."])
    rows = [dict(variant=r["variant"], pnl=r["net_pnl_rupees"], delta=r["baseline_delta_rupees"],
        added_trades=r["added"]["trades"], added_wins=r["added"]["wins"], added_losses=r["added"]["losses"],
        added_pnl=r["added"]["net_pnl_rupees"], removed_trades=r["removed"]["trades"],
        removed_winners=r["removed"]["wins"], removed_losers=r["removed"]["losses"],
        removed_pnl=r["removed"]["net_pnl_rupees"], shared_pnl_delta=r["shared_pnl_delta_rupees"],
        better_days=r["paired_days_better"], worse_days=r["paired_days_worse"], unchanged_days=r["paired_days_unchanged"])
        for r in effects]
    table = pd.DataFrame(rows)
    page = '<!doctype html><meta charset="utf-8"><title>G3 slot-change impact audit</title><style>body{font:14px system-ui;margin:28px}table{border-collapse:collapse}td,th{border:1px solid #ccc;padding:6px;text-align:right}td:first-child,th:first-child{text-align:left}</style><h1>Executed-trade impact, unlimited capital</h1>'
    page += f'<p>Validated {checks["sessions"]} available baseline sessions, {html.escape(checks["first_date"])} through {html.escape(checks["last_date"])}. Baseline unlimited net P&amp;L: Rs {payload["baseline_unlimited"]["net_pnl_rupees"]:,.2f}. All saved baseline controls passed.</p>'
    page += '<p><strong>Coverage:</strong> 57 of 60 scheduled sessions, not every exchange session. Missing: August 24, August 25 and October 1. The first eight sessions use an after-dated monthly universe. This remains retrospective research, not strict point-in-time-universe validation.</p>'
    page += '<p>Net change = added-trade P&amp;L − removed-trade P&amp;L + shared-trade P&amp;L change. Removing a losing trade contributes positively. Original targets/exits were verified unchanged for every shared targeted-variant trade.</p>'
    page += table.round(2).to_html(index=False)
    page += '<p>Broad early-entry variants use a different exit policy and one entry per symbol/day. Their shared-trade contribution is shown separately. The JSON contains each added/removed execution and every paired daily change. No unlimited-capital return uses the old fixed-account denominator.</p>'
    page += '<h2>23 October requested coverage, unlimited capital</h2><p>A successful requested selection must match both the named stock and requested direction. A wrong-side execution does not count.</p>'
    page += pd.DataFrame(coverage).to_html(index=False)
    page += '<h2>Fixed-fill cost sensitivity, unlimited capital</h2><p>Round-trip cost only: 5 bps (current), 10 bps and 20 bps. Each additional basis point costs Rs 50 per trade at Rs 500,000 exposure. Fills are unchanged; this is not a market-impact model. Both capital arms and prior-period results are in the JSON.</p>'
    cost_table = pd.DataFrame(costs)
    page += cost_table.loc[cost_table.capital_limit.eq("unlimited") & cost_table.period.eq("all_available")].drop(columns=["capital_limit", "period"]).round(2).to_html(index=False)
    (output / "audit.json").write_text(json.dumps(study.source._json_ready(payload), indent=2), encoding="utf-8")
    (output / "impact.html").write_text(page, encoding="utf-8")
    print(json.dumps(checks, indent=2))
    print(table.round(2).to_string(index=False))


if __name__ == "__main__":
    main()
