"""Independent scalar cash/lot audit of the monthly options projection.

Reads the immutable historical ledger and generated projection artifacts. Writes
only independent_monthly_sizing_audit.json in the projection output directory. It does not
edit the source engine, dashboard, documentation or fixed-profile research.
"""
from __future__ import annotations

import hashlib
import importlib.util
import json
from pathlib import Path
import sys

import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO))
SOURCE = Path("C:/TradingData/eqidv2/fno_oi/strategy_research/v13_corrected_v10_g/options_3lots_5min_20260914_sl12p5_target25")
OUTPUT = SOURCE / "one_year_scenarios"
SNAPSHOT = OUTPUT / "fixed_only_snapshot_before_monthly" / "projection_payload.json"
SCENARIOS = {"reference": (1., 0.), "retain_75": (.75, 10.), "retain_50": (.5, 10.), "retain_20": (.2, 10.)}


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def fees(turnover, side):
    taxable = 20. + turnover * (.0003553 + .000001 + .000000001)
    return taxable * 1.18 + turnover * (.00003 if side == "BUY" else .0015)


def scalar_templates(trades, days):
    """Independent event construction from source records, with observable exits."""
    templates = []
    for day in days:
        frame = trades.loc[trades.day.eq(day)].sort_values(["entry_ts", "setup_id", "trade_id"], kind="stable")
        records, events = [], []
        for rank, trade in enumerate(frame.to_dict("records")):
            records.append(trade)
            entry = pd.Timestamp(trade["entry_ts"]).value
            release = pd.Timestamp(trade["exit_observed_ts"]).value
            events.append((entry, 1, rank * 2, "BUY", rank))
            events.append((release, 1 if release == entry else 0, rank * 2 + 1, "SELL", rank))
        templates.append((records, sorted(events)))
    return templates


def scalar_replay(templates, draws, opening, fraction, bps, sizing):
    n_paths, horizon = draws.shape
    result = {name: np.zeros((n_paths, horizon)) for name in ["pnl", "accepted", "rejected", "paused", "peak_premium", "lots", "lot_budget", "lot_increment"]}
    result["equity"] = np.zeros((n_paths, horizon + 1))
    minimum_cash = float("inf")
    for path in range(n_paths):
        equity, budget, increment = float(opening), 3., 0.
        result["equity"][path, 0] = equity
        for session, number in enumerate(draws[path]):
            if sizing == "monthly" and session % 21 == 0:
                previous_lots = int(np.floor(budget + 1e-10))
                budget = max(0., min(3. * equity / opening, budget * 1.10))
                increment = int(np.floor(budget + 1e-10)) - previous_lots
            if sizing == "stepup" and session % 21 == 0:
                increment = 0.
                if session and equity > result["equity"][path, session-21]:
                    increment = 1 + np.floor(max(0., equity-opening) / 500000.)
                    budget += increment
            lots = int(np.floor(budget + 1e-10)) if sizing in {"monthly", "stepup"} else 3
            result["lots"][path, session] = lots
            result["lot_budget"][path, session] = budget if sizing in {"monthly", "stepup"} else 3.
            result["lot_increment"][path, session] = increment
            cash, reserve, peak = equity, 0., 0.
            trades, events = templates[int(number)]
            flows = {}
            for _, _, _, side, rank in events:
                trade = trades[rank]
                if side == "BUY":
                    if lots == 0:
                        result["paused"][path, session] += 1
                        continue
                    quantity = lots * int(trade["lot_size"])
                    buy_price = float(trade["entry_price"])
                    price_change = float(trade["exit_price"]) - buy_price
                    sell_price = buy_price + price_change * (fraction if price_change > 0 else 1.)
                    debit = buy_price * quantity * (1 + bps / 10000) + fees(buy_price * quantity, "BUY")
                    receipt = sell_price * quantity * (1 - bps / 10000) - fees(sell_price * quantity, "SELL")
                    if cash < debit - 1e-8:
                        result["rejected"][path, session] += 1
                        continue
                    flows[rank] = (debit, receipt)
                    cash -= debit
                    reserve += debit
                    peak = max(peak, reserve)
                    result["accepted"][path, session] += 1
                elif rank in flows:
                    debit, receipt = flows.pop(rank)
                    cash += receipt
                    reserve -= debit
                minimum_cash = min(minimum_cash, cash)
            if flows or abs(reserve) > 1e-6:
                raise AssertionError("Independent scalar ledger did not close every entry")
            result["pnl"][path, session] = cash - equity
            result["peak_premium"][path, session] = peak
            result["equity"][path, session + 1] = cash
            equity = cash
    result["minimum_free_cash"] = minimum_cash
    return result


def run():
    checks, statistics = [], {}

    def check(name, okay, detail=None):
        row = {"name": name, "passed": bool(okay)}
        if detail is not None:
            row["detail"] = detail
        checks.append(row)
        if not okay:
            print("FAILED " + name, flush=True)

    def close(name, actual, expected, atol=1e-6):
        actual, expected = np.asarray(actual), np.asarray(expected)
        okay = actual.shape == expected.shape and np.allclose(actual, expected, rtol=1e-11, atol=atol, equal_nan=True)
        detail = None
        if not okay:
            detail = {"actual_shape": list(actual.shape), "expected_shape": list(expected.shape)}
            if actual.shape == expected.shape:
                detail["max_abs_difference"] = float(np.nanmax(np.abs(actual - expected)))
        check(name, okay, detail)

    spec = importlib.util.spec_from_file_location("options_projection_reviewed", REPO / "fno_v13_v10_g_options_projection.py")
    engine = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(engine)
    payload = json.loads((OUTPUT / "projection_payload.json").read_text(encoding="utf-8"))
    snapshot = json.loads(SNAPSHOT.read_text(encoding="utf-8"))
    meta = payload["meta"]
    check("12_annual_scenario_sizing_pairs", len(payload["annual"]) == 12)
    check("144_monthly_scenario_sizing_rows", len(payload["monthly"]) == 144)
    check("3036_daily_scenario_sizing_rows", len(payload["curves"]) == 12 * 253)
    check("5000_paths_252_sessions", meta["paths"] == 5000 and meta["horizon"] == 252)
    check("13_source_sessions_20_trades", meta["historical_sessions"] == 13 and meta["trades"] == 20)

    manifest = json.loads((SOURCE / "manifest.json").read_text())
    for filename in ["options_trades.csv", "options_daily.csv", "summary.json", "selected_option_profile.json"]:
        check("source_hash:" + filename, sha(SOURCE / filename) == manifest["artifacts"][filename])
    close("fixed_source_history_preserved", [r["net_pnl"] for r in payload["history"]], [r["net_pnl"] for r in snapshot["history"]])
    for section, keys in [("annual", ["scenario"]), ("monthly", ["scenario", "model_month"]), ("curves", ["scenario", "session"])]:
        old_rows = {tuple(row[k] for k in keys): row for row in snapshot[section] if row.get("sizing", "fixed") == "fixed"}
        fixed = {tuple(row[k] for k in keys): row for row in payload[section] if row.get("sizing") == "fixed"}
        check(section + ":fixed_row_keys_preserved", old_rows.keys() == fixed.keys())
        for key, old in old_rows.items():
            new = fixed.get(key, {})
            numeric = [name for name, value in old.items() if isinstance(value, (int, float)) and value is not None]
            close(section + ":fixed_numeric_parity:" + "/".join(map(str, key)), [new.get(n, np.nan) for n in numeric], [old[n] for n in numeric])

    trades = pd.read_csv(SOURCE / "options_trades.csv", float_precision="round_trip")
    trades = trades.loc[trades.portfolio_status.eq("ADMITTED") & trades.status.eq("CLOSED")].copy()
    for name in ["entry_ts", "exit_ts", "exit_observed_ts"]:
        trades[name] = pd.to_datetime(trades[name], utc=True).dt.tz_convert("Asia/Kolkata")
    days = [row["day"] for row in payload["history"]]
    independent_templates = scalar_templates(trades, days)
    templates = engine.day_templates(trades, days)
    rng = np.random.default_rng(meta["seed"])
    starts = rng.integers(0, len(days), size=(meta["paths"], int(np.ceil(meta["horizon"] / 5))))
    draws = ((starts[:, :, None] + np.arange(5)) % len(days)).reshape(meta["paths"], -1)[:, :meta["horizon"]]
    close("same_independent_seeded_circular_draws", draws, engine.block_draws(len(days), paths=meta["paths"], horizon=meta["horizon"], seed=meta["seed"]), atol=0)
    opening = float(meta["projection_start_equity"])
    close("projection_opening_after_historical_pnl", opening, 1_500_000. + trades.net_pnl.sum())
    boundary_profit = np.array([-100., 0., 499999.99, 500000., 999999.99, 1000000., 1500000.])
    profit_openings = opening + boundary_profit
    next_lots, increments = engine.profit_stepup_lots(np.full(7, 3), profit_openings, profit_openings-100., opening, 500000.)
    close("stepup_profit_milestone_increments_exclude_history", increments, [1, 1, 1, 2, 2, 3, 4], atol=0)
    close("stepup_profit_milestone_whole_lots", next_lots, [4, 4, 4, 5, 5, 6, 7], atol=0)
    held, increments = engine.profit_stepup_lots(np.array([9, 9]), np.array([opening+1000000., opening+1000000.]), np.array([opening+1000000., opening+1100000.]), opening, 500000.)
    close("stepup_flat_or_losing_month_holds_lots", held, [9, 9], atol=0)
    close("stepup_flat_or_losing_month_increment_zero", increments, [0, 0], atol=0)
    budget = np.array([3.])
    progression = []
    for _ in range(4):
        budget = engine.monthly_lot_budget(np.array([opening*10]), budget, opening)
        progression.append(float(budget[0]))
    close("monthly_fractional_capacity_carries_to_four_lots", progression, [3.3, 3.63, 3.993, 4.3923])
    close("monthly_decline_applies_without_growth_cap", engine.monthly_lot_budget([opening*.2], [4.3923], opening), [.6])
    historical = scalar_replay(independent_templates, np.arange(13)[None, :], 1_500_000., 1., 0., "fixed")
    close("independent_scalar_historical_pnl", historical["pnl"][0], [row["net_pnl"] for row in payload["history"]])
    # Deliberately cash-constrained fixture exercises branches uncommon in the
    # favorable sample: loss-driven zero lots versus holding unaffordable size.
    fixture = trades.iloc[:1].copy()
    fixture["entry_price"], fixture["exit_price"] = 100., 1.
    fixture["lot_size"], fixture["quantity"] = 100, 300
    fixture_days = fixture.day.tolist()
    fixture_draws = np.zeros((1, 42), dtype=int)
    for policy in ["monthly", "stepup"]:
        result = engine.simulate(engine.day_templates(fixture, fixture_days), fixture_draws, 30050., 1., 0., sizing=policy)
        expected = scalar_replay(scalar_templates(fixture, fixture_days), fixture_draws, 30050., 1., 0., policy)
        for name in ["equity", "pnl", "accepted", "rejected", "paused", "lots", "lot_budget", "lot_increment"]:
            close("constrained_fixture:" + policy + ":" + name, result[name], expected[name])
        check("constrained_fixture:" + policy + ":only_first_trade_admitted", result["accepted"].sum() == 1)
        if policy == "monthly":
            check("constrained_fixture:monthly_zero_lots_pay_no_fees", result["paused"].sum() == 21 and np.all(result["pnl"][:, 21:] == 0))
        else:
            check("constrained_fixture:stepup_no_partial_downsizing", np.all(result["lots"] == 3) and result["rejected"].sum() == 41)
    representative = np.unique(np.r_[np.arange(10), np.linspace(100, meta["paths"] - 1, 10, dtype=int)])

    for sizing, scenario in [(policy, scenario) for policy in ["monthly", "stepup"] for scenario in SCENARIOS]:
        fraction, bps = SCENARIOS[scenario]
        print("Auditing " + sizing + " " + scenario, flush=True)
        model = engine.simulate(templates, draws, opening, fraction, bps, sizing=sizing)
        scalar = scalar_replay(independent_templates, draws[representative], opening, fraction, bps, sizing)
        for name in ["equity", "pnl", "accepted", "rejected", "paused", "peak_premium", "lots", "lot_increment"]:
            close(scenario + ":scalar_replay:" + name, model[name][representative], scalar[name])
        budget_name = next((name for name in ["lot_budget", "budget", "fractional_lot_budget"] if name in model), None)
        if budget_name:
            close(scenario + ":scalar_replay:lot_budget", model[budget_name][representative], scalar["lot_budget"])
        check(scenario + ":scalar_cash_never_negative", scalar["minimum_free_cash"] >= -1e-7)
        eq, lots = model["equity"], model["lots"]
        close(scenario + ":daily_cash_equity_pnl", np.diff(eq, axis=1), model["pnl"])
        check(scenario + ":whole_nonnegative_lots", np.equal(lots, np.floor(lots)).all() and lots.min() >= 0)
        expected_budget = np.full(len(eq), 3.)
        for first in range(0, 252, 21):
            if sizing == "monthly":
                expected_budget = np.maximum(0., np.minimum(3. * eq[:, first] / opening, expected_budget * 1.10))
            elif first:
                increments = np.where(eq[:, first] > eq[:, first-21], 1 + np.floor(np.maximum(0., eq[:, first]-opening)/500000.), 0.)
                expected_budget += increments
            expected_lots = np.floor(expected_budget + 1e-10)
            close(scenario + ":month_open_equity_lots:" + str(first // 21 + 1), lots[:, first:first+21], np.repeat(expected_lots[:, None], 21, axis=1), atol=0)
            if budget_name:
                close(scenario + ":fractional_budget:" + str(first // 21 + 1), model[budget_name][:, first:first+21], np.repeat(expected_budget[:, None], 21, axis=1))

        annual = next(row for row in payload["annual"] if row["scenario"] == scenario and row["sizing"] == sizing)
        final, future = eq[:, -1], eq[:, -1] - opening
        dd = (np.maximum.accumulate(eq, axis=1) - eq).max(axis=1)
        values = dict(mean_future_pnl=future.mean(), mean_ending_equity=final.mean(),
                      p10_ending_equity=np.quantile(final, .1), p50_ending_equity=np.median(final), p90_ending_equity=np.quantile(final, .9),
                      p10_future_pnl=np.quantile(future, .1), p50_future_pnl=np.median(future), p90_future_pnl=np.quantile(future, .9),
                      mean_return_pct=future.mean()/opening*100, mean_cumulative_pnl=final.mean()-1_500_000.,
                      mean_total_return_pct=(final.mean()/1_500_000.-1)*100, probability_loss=np.mean(future < 0),
                      mean_max_drawdown=dd.mean(), p90_max_drawdown=np.quantile(dd, .9),
                      mean_trades=model["accepted"].sum(axis=1).mean(), mean_rejections=model["rejected"].sum(axis=1).mean(),
                      mean_paused_entries=model["paused"].sum(axis=1).mean(),
                      mean_lots=lots.mean(), mean_ending_month_lots=lots[:, -1].mean(),
                      p10_ending_month_lots=np.quantile(lots[:, -1], .1), p50_ending_month_lots=np.median(lots[:, -1]),
                      p90_ending_month_lots=np.quantile(lots[:, -1], .9),
                      mean_peak_premium=model["peak_premium"].max(axis=1).mean())
        close(scenario + ":annual_aggregation", [annual[k] for k in values], list(values.values()))
        months = sorted([r for r in payload["monthly"] if r["scenario"] == scenario and r["sizing"] == sizing], key=lambda r:r["model_month"])
        for row in months:
            first, end = (row["model_month"]-1)*21, row["model_month"]*21
            net = eq[:, end]-eq[:, first]
            monthly_values = dict(mean_opening_equity=eq[:, first].mean(), mean_equity=eq[:, end].mean(), mean_pnl=net.mean(),
                                  p10_pnl=np.quantile(net, .1), p50_pnl=np.median(net), p90_pnl=np.quantile(net, .9),
                                  p10_equity=np.quantile(eq[:, end], .1), p50_equity=np.median(eq[:, end]), p90_equity=np.quantile(eq[:, end], .9),
                                  mean_lots=lots[:, first].mean(), mean_trades=model["accepted"][:, first:end].sum(axis=1).mean(),
                                  mean_rejections=model["rejected"][:, first:end].sum(axis=1).mean(),
                                  mean_paused_entries=model["paused"][:, first:end].sum(axis=1).mean(),
                                  mean_peak_premium=model["peak_premium"][:, first:end].max(axis=1).mean(),
                                  mean_return_pct=np.nanmean(np.divide(net, eq[:, first], out=np.full(len(net), np.nan), where=eq[:, first]>0))*100)
            for series, field in [(lots[:, first], "lots"), (model["lot_budget"][:, first], "lot_budget"), (model["lot_increment"][:, first], "lot_increment")]:
                monthly_values.update({"mean_"+field:series.mean(), "p10_"+field:np.quantile(series, .1),
                                       "p50_"+field:np.median(series), "p90_"+field:np.quantile(series, .9)})
            close(scenario + ":month_aggregation:" + str(row["model_month"]), [row[k] for k in monthly_values], list(monthly_values.values()))
        close(scenario + ":monthly_annual_pnl_reconciles", sum(r["mean_pnl"] for r in months), annual["mean_future_pnl"])
        curves = sorted([r for r in payload["curves"] if r["scenario"] == scenario and r["sizing"] == sizing], key=lambda r:r["session"])
        close(scenario + ":curve_means", [r["mean_equity"] for r in curves], eq.mean(axis=0))
        for name, quant in [("p10_equity", .1), ("p50_equity", .5), ("p90_equity", .9)]:
            close(scenario + ":curve_" + name, [r[name] for r in curves], np.quantile(eq, quant, axis=0))
        check(scenario + ":curve_percentiles_ordered", all(r["p10_equity"] <= r["p50_equity"] <= r["p90_equity"] for r in curves))
        statistics[sizing + ":" + scenario] = dict(scalar_paths=len(representative), minimum_scalar_free_cash=scalar["minimum_free_cash"],
                                    minimum_lots=int(lots.min()), maximum_lots=int(lots.max()),
                                    mean_future_pnl=float(future.mean()), mean_ending_equity=float(final.mean()),
                                    mean_ending_lots=float(lots[:, -1].mean()), mean_all_session_lots=float(lots.mean()),
                                    paths_ever_zero_lots=int(np.any(lots == 0, axis=1).sum()))

    result = dict(passed=all(row["passed"] for row in checks), checks_count=len(checks), checks=checks,
                  payload_sha256=sha(OUTPUT / "projection_payload.json"),
                  scalar_paths_per_scenario=len(representative), scenarios=statistics,
                  input_hashes={str(path):sha(path) for path in [SOURCE/"options_trades.csv", SOURCE/"manifest.json", OUTPUT/"projection_payload.json", SNAPSHOT, REPO/"fno_v13_v10_g_options_projection.py"]},
                  limitations=["This audit checks projection accounting, not future trading performance or larger-order liquidity.",
                               "Monthly lot increases reuse source three-lot fills; actual market depth and larger-size price impact are not reconstructed.",
                               "A fractional sizing budget is retained between model months; orders always use whole lots.",
                               "Three lots is the initial monthly baseline; equity-based resizing may lower size after declining equity.",
                               "Profit step-up adds lots only after a profitable prior month. Each Rs5 lakh cumulative future gain adds one to the next increment.",
                               "Profit step-up holds size after a losing month; insufficient premium cash skips the complete planned entry."])
    (OUTPUT / "independent_monthly_sizing_audit.json").write_text(json.dumps(result, indent=2, allow_nan=False) + "\n", encoding="utf-8")
    print(json.dumps({k:result[k] for k in ["passed", "checks_count", "scalar_paths_per_scenario", "scenarios"]}, indent=2))
    if not result["passed"]:
        raise AssertionError([r for r in checks if not r["passed"]])


if __name__ == "__main__":
    run()
