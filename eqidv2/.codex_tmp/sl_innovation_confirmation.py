"""Research-only two-level hard stop plus completed-close confirmation exits."""
from __future__ import annotations

from functools import partial
import json
from pathlib import Path

import numpy as np
import pandas as pd

import sl_tradeoff_timing as timing
import sl_innovation_common as common

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "outputs/v13_sl_innovation_20261004/confirmation_analysis.json"
MINUTE = timing.MINUTE


def confirmation_exit(path, entry_index, entry, is_long, target_pct, hard_stop,
                      tighten_minutes=None, tighten_stop=1.0, timer_minutes=None,
                      timer_adverse=None, *, soft_stop=.75, required_closes=1,
                      interval_minutes=5):
    """Hard stop stays active; softer closes signal a following-open exit."""
    assert 0 < soft_stop <= hard_stop and required_closes >= 1
    assert interval_minutes in (1, 5)
    sign = 1 if is_long else -1
    hard_level = entry * (1 - sign * hard_stop / 100)
    target = entry * (1 + sign * target_pct / 100)
    entry_label = int(path["timestamp_ns"][entry_index])
    consecutive = 0
    last_eligible_label = None
    pending = False
    for j in range(entry_index, len(path["close"])):
        op, hi, lo = (float(path[name][j]) for name in ("open", "high", "low"))
        end_ns = int(path["timestamp_ns"][j])
        stop_open = op <= hard_level if is_long else op >= hard_level
        target_open = op >= target if is_long else op <= target
        if pending:
            if stop_open:
                return j, op, "STOP_GAP_BEFORE_TIMER", hard_stop, "OPEN"
            if target_open:
                return j, target, "TARGET", hard_stop, "OPEN"
            return j, op, "ADVERSE_TIMER_NEXT_OPEN", hard_stop, "OPEN"
        stop_hit = lo <= hard_level if is_long else hi >= hard_level
        target_hit = hi >= target if is_long else lo <= target
        if stop_hit:
            gap = j > entry_index and stop_open
            return j, op if gap else hard_level, "STOP", hard_stop, "OPEN" if gap else "INTRABAR"
        if target_hit:
            return j, target, "TARGET", hard_stop, "INTRABAR"
        # IST's 330-minute offset is divisible by five, so epoch minute modulo
        # five gives exchange-aligned 09:20/09:25/... completed five-minute closes.
        eligible = (end_ns // MINUTE) % interval_minutes == 0
        fully_after_entry = end_ns - interval_minutes * MINUTE >= entry_label
        if not (eligible and fully_after_entry):
            continue
        if last_eligible_label is not None and end_ns - last_eligible_label != interval_minutes * MINUTE:
            consecutive = 0
        close_return = sign * (float(path["close"][j]) / entry - 1) * 100
        consecutive = consecutive + 1 if close_return <= -soft_stop + 1e-10 else 0
        last_eligible_label = end_ns
        if consecutive >= required_closes and j < len(path["close"]) - 1:
            pending = True
    return len(path["close"]) - 1, float(path["close"][-1]), "TIME_EXIT_1515", hard_stop, "CLOSE"


def validate_synthetic():
    checks = []

    def make(start, end, short=False):
        ts = pd.date_range(f"2026-09-01 {start}", f"2026-09-01 {end}", freq="min", tz="Asia/Kolkata")
        close, high, low = (101, 101.1, 99.9) if short else (99, 100.1, 98.9)
        return {"timestamp_ns": ts.asi8, "open": np.full(len(ts), 100.0),
                "high": np.full(len(ts), high), "low": np.full(len(ts), low),
                "close": np.full(len(ts), float(close))}

    def test(name, condition):
        assert condition, name
        checks.append(name)

    p = make("09:42", "09:51")
    p["open"][-1] = 99.2
    result = confirmation_exit(p, 0, 100, True, 2, 1.25, soft_stop=1)
    test("partial pre-entry five-minute interval is ignored; following-open fill", result == (9, 99.2, "ADVERSE_TIMER_NEXT_OPEN", 1.25, "OPEN"))
    test("signal close99 is not fill99.2", result[1] != p["close"][-2])
    p["open"][-1] = 98
    test("pending long hard-stop gap priority", confirmation_exit(p, 0, 100, True, 2, 1.25, soft_stop=1)[:3] == (9, 98.0, "STOP_GAP_BEFORE_TIMER"))
    p["open"][-1] = 103
    test("pending long target gap priority", confirmation_exit(p, 0, 100, True, 2, 1.25, soft_stop=1)[:3] == (9, 102.0, "TARGET"))
    p = make("09:42", "09:51", short=True)
    p["open"][-1] = 100.8
    test("short following-open fill", confirmation_exit(p, 0, 100, False, 2, 1.25, soft_stop=1)[:3] == (9, 100.8, "ADVERSE_TIMER_NEXT_OPEN"))
    p["open"][-1] = 102
    test("pending short hard-stop gap priority", confirmation_exit(p, 0, 100, False, 2, 1.25, soft_stop=1)[:3] == (9, 102.0, "STOP_GAP_BEFORE_TIMER"))
    p["open"][-1] = 97
    test("pending short target gap priority", confirmation_exit(p, 0, 100, False, 2, 1.25, soft_stop=1)[:3] == (9, 98.0, "TARGET"))
    p = make("15:06", "15:15")
    test("session-end signal cannot fabricate next bar", confirmation_exit(p, 0, 100, True, 2, 1.25, soft_stop=1)[:3] == (9, 99.0, "TIME_EXIT_1515"))
    p = make("09:40", "10:01")
    p["close"][10] = 100
    p["open"][-1] = 99.1
    test("intervening healthy completed close resets consecutive count", confirmation_exit(p, 0, 100, True, 2, 1.25, soft_stop=1, required_closes=2)[:3] == (21, 99.1, "ADVERSE_TIMER_NEXT_OPEN"))
    p = make("09:40", "09:46")
    p["low"][3] = 98.5
    test("always-active hard stop precedes soft close confirmation", confirmation_exit(p, 0, 100, True, 2, 1.25, soft_stop=1)[:3] == (3, 98.75, "STOP"))
    p = make("09:40", "09:46")
    p["high"][3], p["low"][3] = 103, 98.5
    test("same-bar stop-target tie preserves pessimistic stop first", confirmation_exit(p, 0, 100, True, 2, 1.25, soft_stop=1)[:3] == (3, 98.75, "STOP"))
    p = make("09:40", "09:46")
    p["open"][4] = 99.3
    test("three one-minute closes excludes entry candle and uses fourth following open", confirmation_exit(p, 0, 100, True, 2, 1.25, soft_stop=1, required_closes=3, interval_minutes=1)[:3] == (4, 99.3, "ADVERSE_TIMER_NEXT_OPEN"))
    return checks


def metrics(ledger, portfolio_summary, segments, days, rule):
    ex = ledger[ledger.portfolio_executed.eq(True)].copy()
    pnl = ex.portfolio_net_profit_rupees.to_numpy(float)
    losses = pnl[pnl < 0]
    out = common.summarize(ledger, portfolio_summary, segments, days, rule)
    out.update(rule=rule, worst_loss_rupees=float(-pnl.min()), average_loss_magnitude_rupees=float(-losses.mean()),
               largest_five_loss_mean_rupees=float(-np.sort(pnl)[:5].mean()),
               minute_close_drawdown_rupees=timing.mark_to_market(ledger, segments, days),
               peak_open_initial_risk_rupees=float(portfolio_summary["peak_open_initial_risk_rupees"]),
               custom_exit_count=int(ex.exit_reason.isin(["TIGHTENED_STOP", "ADVERSE_TIMER_NEXT_OPEN", "STOP_GAP_BEFORE_TIMER"]).sum()),
               exit_counts=ex.exit_reason.value_counts().to_dict())
    day_names = list(map(str, days))
    day_rows = []
    cumulative, peak = 0.0, 0.0
    ex["day"] = ex.day.astype(str)
    for day in day_names:
        frame = ex[ex.day.eq(day)]
        profits = frame.portfolio_net_profit_rupees.to_numpy(float)
        net = float(profits.sum())
        cumulative += net
        peak = max(peak, cumulative)
        day_rows.append(dict(day=day, trades=len(frame), wins=int((profits > 0).sum()), losses=int((profits < 0).sum()),
                             net_profit_rupees=net, gross_profit_rupees=float(frame.portfolio_gross_profit_rupees.sum()),
                             cost_rupees=float(frame.portfolio_cost_rupees.sum()), cumulative_net_rupees=cumulative,
                             daily_drawdown_rupees=peak-cumulative))
    out["daily_detail"] = day_rows
    out["daily_net"] = {r["day"]: r["net_profit_rupees"] for r in day_rows}
    out["active_days"] = sum(r["trades"] > 0 for r in day_rows)
    out["positive_session_pct_active"] = 100 * out["positive_sessions"] / out["active_days"]
    out["chronological_slices"] = {}
    slice_defs = [("jul_aug", ex.day.lt("2026-09-01")), ("september", ex.day.ge("2026-09-01"))]
    for name, mask in slice_defs:
        p = ex.loc[mask, "portfolio_net_profit_rupees"].to_numpy(float)
        negative = p[p < 0]
        out["chronological_slices"][name] = dict(trades=len(p), wins=int((p > 0).sum()), win_rate_pct=float((p > 0).mean() * 100),
            net_profit_rupees=float(p.sum()), average_loss_magnitude_rupees=float(-negative.mean()),
            worst_loss_rupees=float(-p.min()))
    fields = ["segment", "day", "sid", "setup_id", "tradingsymbol", "side", "entry_ts", "entry_price",
              "exit_ts", "exit_execution_ts", "exit_bar_end_ts", "exit_event", "exit_price", "exit_reason",
              "native_target_pct", "initial_stop_pct", "active_stop_pct_at_exit", "holding_minutes",
              "portfolio_gross_profit_rupees", "portfolio_cost_rupees", "portfolio_net_profit_rupees", "mfe_pct", "mae_pct"]
    out["trade_pnl"] = ex[fields].to_dict("records")
    return out, ex


def main():
    synthetic = validate_synthetic()
    published, segments, days = timing.source.prepared_segments()
    original_exit = timing.exit_path
    baselines = [dict(name=f"STATIC_{s:.2f}", family="STATIC", hard_stop=s) for s in (1.0, 1.25, 2.75)]
    baselines.append(dict(name="TIGHTEN_1.25_TO_1.00_AFTER_120M", family="TIGHTEN", hard_stop=1.25, tighten_minutes=120))
    variants = [dict(name=f"HARD1.25_SOFT{s:.2f}_{n}X5M", family="CONFIRMED_CLOSE", hard_stop=1.25,
                     soft_stop=s, required_closes=n, interval_minutes=5) for s in (.75, 1.0) for n in (1, 2, 3)]
    variants += [dict(name=f"HARD1.25_SOFT1.00_{n}X1M", family="CONFIRMED_CLOSE", hard_stop=1.25,
                      soft_stop=1.0, required_closes=n, interval_minutes=1) for n in (3, 5)]
    results, stores = [], {}
    for rule in baselines + variants:
        try:
            timing.exit_path = (partial(confirmation_exit, soft_stop=rule["soft_stop"], required_closes=rule["required_closes"],
                                        interval_minutes=rule["interval_minutes"]) if rule["family"] == "CONFIRMED_CLOSE" else original_exit)
            ledger, summary = timing.simulate(segments, published["base"], rule)
        finally:
            timing.exit_path = original_exit
        result, ex = metrics(ledger, summary, segments, days, rule)
        assert len(ex) == 85
        stores[rule["name"]] = ex
        results.append(result)
        print(rule["name"], result["wins"], round(result["net_profit_rupees"], 2), round(result["minute_close_drawdown_rupees"], 2), flush=True)
    reference = stores["TIGHTEN_1.25_TO_1.00_AFTER_120M"]
    keys = ["segment", "day", "sid", "setup_id", "tradingsymbol", "side"]
    ref_profit = reference.set_index(keys).portfolio_net_profit_rupees
    for row in results:
        candidate = stores[row["rule"]["name"]]
        profits = candidate.set_index(keys).portfolio_net_profit_rupees.reindex(ref_profit.index)
        assert not profits.isna().any()
        delta = profits - ref_profit
        row["versus_staged"] = dict(net_delta_rupees=float(delta.sum()), improved_trades=int(delta.gt(1e-7).sum()),
            worsened_trades=int(delta.lt(-1e-7).sum()), rescued_winners=int(((profits > 0) & (ref_profit < 0)).sum()),
            lost_winners=int(((profits < 0) & (ref_profit > 0)).sum()))
        compare = candidate.merge(reference[keys + ["portfolio_net_profit_rupees", "exit_reason", "exit_ts"]], on=keys, suffixes=("", "_staged"))
        compare["net_delta_rupees"] = compare.portfolio_net_profit_rupees - compare.portfolio_net_profit_rupees_staged
        fields = keys + ["entry_ts", "entry_price", "exit_ts", "exit_price", "exit_reason", "portfolio_net_profit_rupees",
                         "exit_ts_staged", "exit_reason_staged", "portfolio_net_profit_rupees_staged", "net_delta_rupees"]
        row["changed_trades_vs_staged"] = compare.loc[compare.net_delta_rupees.abs().gt(1e-7), fields].to_dict("records")
    old = json.loads((ROOT / "outputs/v13_sl_tradeoff_20261004/timing_analysis.json").read_text(encoding="utf-8"))
    for result in results[:4]:
        previous = next(r for r in old["results"] if r["rule"]["name"] == result["rule"]["name"])
        assert result["trades"] == previous["trades"] == 85
        for field in ("net_profit_rupees", "daily_close_drawdown_rupees", "minute_close_drawdown_rupees"):
            assert abs(result[field] - previous[field]) < 1e-6, (result["rule"]["name"], field)
        assert all(abs(value - previous["daily_net"][day]) < 1e-6 for day, value in result["daily_net"].items())
    output = dict(status="RESEARCH_ONLY_REUSED_HISTORY_NO_UNTOUCHED_HOLDOUT", window=[str(days[0]), str(days[-1])],
        sessions=len(days), trades=85, baseline_count=4, confirmation_variants=len(variants), synthetic_checks_passed=len(synthetic),
        synthetic_checks=synthetic,
        methodology=["All native selections, entries, retained targets, position sizes, costs, and portfolio constraints are fixed.",
          "Hard1.25% remains always active and is never widened. Soft threshold is a completed-close condition, not a guaranteed loss cap.",
          "Five-minute closes align to exchange clock :00/:05/...; only intervals beginning at or after the conservative entry-end label are eligible. One-minute variants also exclude entry bar.",
          "Consecutive confirmation resets at a healthy close or missing expected interval. Hard-stop and target exits run first on every one-minute bar.",
          "A completed-close signal fills at the following minute open; resting hard-stop gap fills first at adverse open, then target gap fills at target. Final-session close cannot create an extra bar.",
          "Source minute timestamps are bar-end labels. Dynamic open executions subtract one minute; native entry label remains a conservative time proxy.",
          "Minute-close drawdown is marked with original timing.mark_to_market; it excludes intraminute extremes and attributes open exits by their source execution-candle end.",
          "Chronological slices reuse already seen data and are diagnostics, not untouched validation; October1 is excluded because incomplete."],
          controls=results[:4], results=results[4:])
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(timing.clean(output), indent=2, allow_nan=False, default=str) + "\n", encoding="utf-8")
    print(json.dumps({"output": str(OUT), "synthetic_checks": len(synthetic), "variants": len(variants)}, indent=2))


if __name__ == "__main__":
    main()
