"""Cash-constrained, fixed and monthly-sized hypothetical G option scenarios.

The 12.5% stop / 25% target source is a small, post-hoc backtest. These circular
five-session bootstrap scenarios describe conditional outcomes, not forecasts.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
from pathlib import Path

import numpy as np
import pandas as pd

from fno_v13_v10_g_options_execution import option_order_costs


SOURCE = Path("C:/TradingData/eqidv2/fno_oi/strategy_research/v13_corrected_v10_g/options_3lots_5min_20260914_sl12p5_target25")
OUTPUT = SOURCE / "one_year_scenarios"
INITIAL = 1_500_000.0
HORIZON = 252
PATHS = 5000
BLOCK = 5
MONTH_SESSIONS = 21
SEED = 20260914
LOTS = 3
SCENARIOS = [
    dict(id="reference", label="Historical pace reference", fraction=1., extra_cost_bps_per_side=0., color="#4B6F95"),
    dict(id="retain_75", label="75% of winning gross P&L", fraction=.75, extra_cost_bps_per_side=10., color="#A66C17"),
    dict(id="retain_50", label="50% of winning gross P&L", fraction=.50, extra_cost_bps_per_side=10., color="#008879"),
    dict(id="retain_20", label="20% of winning gross P&L", fraction=.20, extra_cost_bps_per_side=10., color="#C24D57"),
]


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def json_value(value):
    if isinstance(value, dict):
        return {str(k): json_value(v) for k, v in value.items()}
    if isinstance(value, (tuple, list)):
        return [json_value(v) for v in value]
    if isinstance(value, np.generic):
        return json_value(value.item())
    if isinstance(value, (pd.Timestamp, Path)):
        return str(value)
    if value is pd.NA or value is pd.NaT or isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def write_json(path, data):
    Path(path).write_text(json.dumps(json_value(data), indent=2, allow_nan=False) + "\n", encoding="utf-8")


def load_history(source=SOURCE):
    """Verify the frozen fixed-profile artifacts and execution fee implementation."""
    source = Path(source)
    manifest = json.loads((source / "manifest.json").read_text(encoding="utf-8"))
    if not manifest.get("complete"):
        raise ValueError("Source manifest is incomplete")
    verified = {}
    for name, expected in manifest["artifacts"].items():
        path = source / name.replace("\\", "/")
        if digest(path) != expected:
            raise ValueError(f"Source artifact drift: {name}")
        verified[str(path)] = expected
    code = Path(__file__).with_name("fno_v13_v10_g_options_execution.py")
    pinned = {Path(k).name: v for k, v in manifest["code_sha256"].items()}
    if digest(code) != pinned[code.name]:
        raise ValueError("Source options execution fee code drift")
    verified[str(code)] = digest(code)
    profile = json.loads((source / "selected_option_profile.json").read_text(encoding="utf-8"))
    if (profile["stop_pct"], profile["target_pct"], profile["lots"], profile["capital"]) != (12.5, 25., 3, INITIAL):
        raise ValueError("Expected the user-selected 12.5% SL / 25% target / three-lot profile")
    summary = json.loads((source / "summary.json").read_text(encoding="utf-8"))
    ledger = pd.read_csv(source / "options_trades.csv", float_precision="round_trip")
    daily_source = pd.read_csv(source / "options_daily.csv", float_precision="round_trip")
    trades = ledger.loc[ledger.portfolio_status.eq("ADMITTED")].copy()
    if len(trades) != 20 or not trades.status.eq("CLOSED").all():
        raise ValueError("Expected 20 completely resolved historical trades")
    if not ((trades.quantity == LOTS * trades.lot_size) & trades.lots.eq(LOTS)).all():
        raise ValueError("Historical entries must contain exactly three whole lots")
    for column in ("entry_ts", "exit_ts", "exit_observed_ts"):
        trades[column] = pd.to_datetime(trades[column], utc=True).dt.tz_convert("Asia/Kolkata")
    if trades.exit_observed_ts.isna().any() or not trades.exit_observed_ts.ge(trades.entry_ts).all():
        raise ValueError("Every exit must have a causal observed timestamp")
    for side, price, fees in [("BUY", "entry_price", "entry_costs"), ("SELL", "exit_price", "exit_costs")]:
        actual = [option_order_costs(row[price], int(row["quantity"]), side)["total"] for row in trades.to_dict("records")]
        np.testing.assert_allclose(actual, trades[fees], rtol=1e-12, atol=1e-8)
    np.testing.assert_allclose((trades.exit_price-trades.entry_price)*trades.quantity, trades.gross_pnl, atol=1e-8)
    np.testing.assert_allclose(trades.gross_pnl-trades.entry_costs-trades.exit_costs, trades.net_pnl, atol=1e-8)
    np.testing.assert_allclose(trades.net_pnl.sum(), summary["net_pnl"], atol=1e-8)

    # Calendar comes from source sessions, not only the ten days with fills.
    # Earlier unavailable August-expiry history is not synthesized as zero P&L.
    first_mapped = ledger.loc[ledger.mapping_status.str.startswith("MAPPED_", na=False), "day"].min()
    last_day = daily_source.day.max()
    daily = daily_source.loc[daily_source.day.between(first_mapped, last_day)].copy()
    if len(daily) != 13 or first_mapped != "2026-08-26" or last_day != "2026-09-11":
        raise ValueError("Unexpected covered calendar: expected 13 source sessions, Aug 26-Sep 11")
    grouped = trades.groupby("day").net_pnl.sum().reindex(daily.day, fill_value=0.)
    np.testing.assert_allclose(grouped, daily.net_pnl, atol=1e-8)
    daily["session"] = np.arange(1, len(daily)+1)
    daily["trades"] = daily.closed.astype(int)
    daily["cumulative_net_pnl"] = daily.net_pnl.cumsum()
    daily["opening_equity"] = INITIAL + daily.cumulative_net_pnl.shift(fill_value=0.)
    daily["closing_equity"] = INITIAL + daily.cumulative_net_pnl
    daily["return_pct"] = daily.net_pnl / daily.opening_equity * 100
    daily["cumulative_return_pct"] = daily.cumulative_net_pnl / INITIAL * 100
    np.testing.assert_allclose(daily.closing_equity.iloc[-1], summary["ending_free_cash"], atol=1e-8)
    verified[str(source / "manifest.json")] = digest(source / "manifest.json")
    return ledger, trades, daily, profile, summary, verified


def day_templates(trades, days):
    """Retain original entry priority, overlaps and observable exit cash releases."""
    templates = []
    for day in days:
        frame = trades.loc[trades.day.eq(day)].sort_values(["entry_ts", "setup_id", "trade_id"], kind="stable")
        entries, events = [], []
        for rank, row in enumerate(frame.to_dict("records")):
            quantity = int(row["quantity"])
            if quantity != LOTS * int(row["lot_size"]):
                raise ValueError("Templates require exactly three lots")
            entry = pd.Timestamp(row["entry_ts"]).value
            observed_exit = pd.Timestamp(row["exit_observed_ts"]).value
            if observed_exit < entry:
                raise ValueError("Exit cash cannot precede entry")
            if pd.Timestamp(row["exit_observed_ts"]).date() != pd.Timestamp(row["entry_ts"]).date():
                raise ValueError("All template positions must close within the sampled session")
            entries.append(dict(trade_id=row["trade_id"], quantity=quantity, lot_size=int(row["lot_size"]),
                                entry_price=float(row["entry_price"]), exit_price=float(row["exit_price"])))
            events.append((entry, 1, rank*2, "entry", rank))
            events.append((observed_exit, 1 if observed_exit == entry else 0, rank*2+1, "exit", rank))
        templates.append(dict(day=str(day), count=len(entries), trades=entries, events=sorted(events)))
    return templates


def block_draws(n_days, paths=PATHS, horizon=HORIZON, block=BLOCK, seed=SEED):
    if min(n_days, paths, horizon, block) < 1:
        raise ValueError("Calendar, paths, horizon and block must be positive")
    rng = np.random.default_rng(seed)
    starts = rng.integers(0, n_days, size=(paths, int(np.ceil(horizon / block))))
    return ((starts[:, :, None] + np.arange(block)) % n_days).reshape(paths, -1)[:, :horizon]


def scenario_cashflows(trade, fraction, extra_cost_bps_per_side, lots=LOTS):
    """Haircut gross winners; recompute fees on hypothetical sale turnover.

    Saved fills already include the historical adverse slippage. The additional
    premium-based execution buffer is a separate cash cost, charged once per side.
    Only entry price, quantity and the scenario's fixed buffer affect admission.
    """
    if not isinstance(lots, (int, np.integer)) or lots < 1:
        raise ValueError("Executed option orders require a positive whole-lot count")
    quantity = int(trade["lot_size"]) * int(lots)
    entry = trade["entry_price"]
    gross = (trade["exit_price"] - entry) * quantity
    scenario_gross = gross * fraction if gross > 0 else gross
    scenario_exit = entry + scenario_gross / quantity
    buy_fee = option_order_costs(entry, quantity, "BUY")["total"]
    sell_fee = option_order_costs(scenario_exit, quantity, "SELL")["total"]
    entry_impact = entry * quantity * extra_cost_bps_per_side / 10000
    exit_impact = scenario_exit * quantity * extra_cost_bps_per_side / 10000
    outlay = entry * quantity + buy_fee + entry_impact
    receipt = scenario_exit * quantity - sell_fee - exit_impact
    return dict(outlay=outlay, receipt=receipt, gross_pnl=scenario_gross,
                fees=buy_fee+sell_fee, extra_impact_cost=entry_impact+exit_impact,
                quantity=quantity, lots=int(lots), scenario_exit_price=scenario_exit)


def monthly_lot_budget(opening_equity, previous_budget, reference_equity):
    """Keep fractional sizing capacity; floor only when forming executable lots.

    The 10% increase cap applies to fractional capacity, so an integer 3-to-4
    transition can exceed 10%. Reductions apply immediately without a decline cap.
    """
    if not np.isfinite(reference_equity) or reference_equity <= 0:
        raise ValueError("Reference equity must be finite and positive")
    opening = np.asarray(opening_equity, dtype=float)
    previous = np.asarray(previous_budget, dtype=float)
    if not np.isfinite(opening).all() or not np.isfinite(previous).all() or (previous < 0).any():
        raise ValueError("Month opening equity and nonnegative previous budget must be finite")
    proportional = LOTS * np.maximum(0., opening) / reference_equity
    return np.minimum(proportional, previous * 1.10)


def profit_stepup_lots(previous_lots, opening_equity, previous_month_opening_equity,
                       reference_equity, profit_step_rupees=500_000.):
    """Increase whole lots after profitable months; accelerate with future profits.

    Previously accumulated lot count never decreases under this policy. The next
    increment is recalculated from cumulative additional realized P&L, so it can
    become smaller after a profit drawdown. Historical backtest profit is excluded.
    """
    if not np.isfinite(profit_step_rupees) or profit_step_rupees <= 0:
        raise ValueError("Profit step must be finite and positive")
    previous = np.asarray(previous_lots)
    opening = np.asarray(opening_equity, dtype=float)
    prior_opening = np.asarray(previous_month_opening_equity, dtype=float)
    if (previous < 0).any() or (previous != np.floor(previous)).any() or not np.isfinite(previous).all():
        raise ValueError("Previous lots must be nonnegative whole numbers")
    if not np.isfinite(opening).all() or not np.isfinite(prior_opening).all() or not np.isfinite(reference_equity):
        raise ValueError("Equity inputs must be finite")
    future_profit = np.maximum(0., opening-reference_equity)
    candidate = 1 + np.floor(future_profit/profit_step_rupees+1e-12).astype(np.int32)
    increment = np.where(opening > prior_opening+1e-8, candidate, 0).astype(np.int32)
    return previous.astype(np.int32)+increment, increment


def simulate(templates, draws, start_equity, fraction, extra_cost_bps_per_side, sizing="fixed", stepup_profit_step_rupees=500_000.):
    if not all(np.isfinite([start_equity, fraction, extra_cost_bps_per_side])) or start_equity < 0 or not 0 <= fraction <= 1 or extra_cost_bps_per_side < 0:
        raise ValueError("Invalid scenario assumption")
    if sizing not in ("fixed", "monthly", "stepup"):
        raise ValueError("Unknown sizing policy")
    if not np.isfinite(stepup_profit_step_rupees) or stepup_profit_step_rupees <= 0:
        raise ValueError("Monthly step-up profit threshold must be finite and positive")
    if sizing == "monthly" and start_equity <= 0:
        raise ValueError("Monthly sizing requires positive reference equity")
    draws = np.asarray(draws)
    if draws.ndim != 2 or not np.issubdtype(draws.dtype, np.integer) or draws.size == 0 or draws.min() < 0 or draws.max() >= len(templates):
        raise ValueError("Draws must contain valid integer template indexes")
    paths, horizon = draws.shape
    equity = np.empty((paths, horizon+1))
    equity[:, 0] = start_equity
    pnl = np.zeros((paths, horizon))
    accepted = np.zeros((paths, horizon), dtype=np.int16)
    rejected = np.zeros_like(accepted)
    paused = np.zeros_like(accepted)
    peak_premium = np.zeros_like(pnl)
    lots_by_session = np.empty((paths, horizon), dtype=np.int32)
    budget_by_session = np.empty_like(pnl)
    increment_by_session = np.zeros((paths, horizon), dtype=np.int32)
    budget = np.full(paths, float(LOTS))
    month_increment = np.zeros(paths, dtype=np.int32)
    # Fees include flat brokerage, so cache whole-lot computations rather than
    # scaling the original three-lot total cost. Every side uses its own turnover.
    flow_cache = {}
    for step in range(horizon):
        if sizing == "monthly" and step % MONTH_SESSIONS == 0:
            prior_lots = np.floor(budget+1e-10).astype(np.int32)
            budget = monthly_lot_budget(equity[:, step], budget, start_equity)
            month_increment = np.floor(budget+1e-10).astype(np.int32)-prior_lots
        elif sizing == "stepup" and step % MONTH_SESSIONS == 0:
            if step:
                new_lots, month_increment = profit_stepup_lots(budget, equity[:, step],
                    equity[:, step-MONTH_SESSIONS], start_equity, stepup_profit_step_rupees)
                budget = new_lots.astype(float)
        lots = np.floor(np.maximum(0., budget)+1e-10).astype(np.int32)
        lots_by_session[:, step] = lots
        budget_by_session[:, step] = budget
        increment_by_session[:, step] = month_increment
        for number, template in enumerate(templates):
            pick = np.flatnonzero(draws[:, step] == number)
            if not len(pick):
                continue
            cash = equity[pick, step].copy()
            reserved = np.zeros(len(pick))
            peak = np.zeros(len(pick))
            admitted = np.zeros((len(pick), template["count"]), dtype=bool)
            net = np.zeros(len(pick))
            local_lots = lots[pick]
            positive_lots = local_lots > 0
            outlays = np.zeros((len(pick), template["count"]))
            receipts = np.zeros_like(outlays)
            for count in np.unique(local_lots[positive_lots]):
                key = (number, int(count))
                if key not in flow_cache:
                    flow_cache[key] = [scenario_cashflows(trade, fraction, extra_cost_bps_per_side, int(count)) for trade in template["trades"]]
                selected = local_lots == count
                outlays[selected, :] = [flow["outlay"] for flow in flow_cache[key]]
                receipts[selected, :] = [flow["receipt"] for flow in flow_cache[key]]
            for _, _, _, kind, rank in template["events"]:
                outlay = outlays[:, rank]
                receipt = receipts[:, rank]
                if kind == "entry":
                    ok = positive_lots & (cash >= outlay - 1e-8)
                    admitted[:, rank] = ok
                    cash[ok] -= outlay[ok]
                    reserved[ok] += outlay[ok]
                    peak = np.maximum(peak, reserved)
                    accepted[pick, step] += ok
                    rejected[pick, step] += positive_lots & ~ok
                    paused[pick, step] += ~positive_lots
                else:
                    ok = admitted[:, rank]
                    cash[ok] += receipt[ok]
                    reserved[ok] -= outlay[ok]
                    net[ok] += receipt[ok]-outlay[ok]
                if np.any(cash < -1e-7):
                    raise AssertionError("Scenario generated negative free cash")
            np.testing.assert_allclose(reserved, 0., atol=1e-7)
            np.testing.assert_allclose(cash, equity[pick, step]+net, rtol=1e-12, atol=1e-7)
            equity[pick, step+1] = cash
            pnl[pick, step] = net
            peak_premium[pick, step] = peak
    return dict(equity=equity, pnl=pnl, accepted=accepted, rejected=rejected,
                paused=paused, peak_premium=peak_premium, lots=lots_by_session,
                lot_budget=budget_by_session, lot_increment=increment_by_session)


def monthly_rows(model, scenario, sizing="fixed"):
    rows = []
    eq = model["equity"]
    for start in range(0, model["pnl"].shape[1], MONTH_SESSIONS):
        end = min(start+MONTH_SESSIONS, model["pnl"].shape[1])
        pnl = model["pnl"][:, start:end].sum(axis=1)
        np.testing.assert_allclose(pnl, eq[:, end]-eq[:, start], atol=1e-6)
        returns = np.divide(pnl, eq[:, start], out=np.full(len(pnl), np.nan), where=eq[:, start] > 0)*100
        lots = model["lots"][:, start]
        budget = model["lot_budget"][:, start]
        increment = model["lot_increment"][:, start]
        rows.append(dict(scenario=scenario, sizing=sizing, model_month=start//MONTH_SESSIONS+1,
            first_additional_session=start+1, last_additional_session=end,
            mean_opening_equity=eq[:, start].mean(), mean_equity=eq[:, end].mean(), mean_pnl=pnl.mean(),
            p10_pnl=np.quantile(pnl, .1), p50_pnl=np.median(pnl), p90_pnl=np.quantile(pnl, .9),
            p10_equity=np.quantile(eq[:, end], .1), p50_equity=np.median(eq[:, end]), p90_equity=np.quantile(eq[:, end], .9),
            mean_lots=lots.mean(), p10_lots=np.quantile(lots, .1), p50_lots=np.median(lots), p90_lots=np.quantile(lots, .9),
            mean_lot_budget=budget.mean(), p10_lot_budget=np.quantile(budget, .1),
            p50_lot_budget=np.median(budget), p90_lot_budget=np.quantile(budget, .9),
            mean_lot_increment=increment.mean(), p10_lot_increment=np.quantile(increment, .1),
            p50_lot_increment=np.median(increment), p90_lot_increment=np.quantile(increment, .9),
            mean_trades=model["accepted"][:, start:end].sum(axis=1).mean(),
            mean_rejections=model["rejected"][:, start:end].sum(axis=1).mean(),
            mean_paused_entries=model["paused"][:, start:end].sum(axis=1).mean(),
            mean_peak_premium=model["peak_premium"][:, start:end].max(axis=1).mean(),
            mean_return_pct=np.nanmean(returns) if np.isfinite(returns).any() else None))
    return rows


def summarize(model, scenario, start_equity, sizing="fixed"):
    eq = model["equity"]
    final = eq[:, -1]
    future = final - start_equity
    max_drawdown = (np.maximum.accumulate(eq, axis=1) - eq).max(axis=1)
    ending_lots = model["lots"][:, -1]
    annual = dict(scenario=scenario, sizing=sizing, mean_future_pnl=future.mean(),
        mean_ending_equity=final.mean(), p10_ending_equity=np.quantile(final, .1),
        p50_ending_equity=np.median(final), p90_ending_equity=np.quantile(final, .9),
        p10_future_pnl=np.quantile(future, .1), p50_future_pnl=np.median(future), p90_future_pnl=np.quantile(future, .9),
        mean_return_pct=future.mean()/start_equity*100, mean_cumulative_pnl=final.mean()-INITIAL,
        mean_total_return_pct=(final.mean()/INITIAL-1)*100,
        probability_loss=np.mean(future < 0), mean_max_drawdown=max_drawdown.mean(),
        p90_max_drawdown=np.quantile(max_drawdown, .9),
        mean_trades=model["accepted"].sum(axis=1).mean(), mean_rejections=model["rejected"].sum(axis=1).mean(),
        mean_paused_entries=model["paused"].sum(axis=1).mean(),
        mean_lots=model["lots"].mean(), mean_ending_month_lots=ending_lots.mean(),
        p10_ending_month_lots=np.quantile(ending_lots, .1), p50_ending_month_lots=np.median(ending_lots),
        p90_ending_month_lots=np.quantile(ending_lots, .9),
        mean_peak_premium=model["peak_premium"].max(axis=1).mean())
    quant = np.quantile(eq, [.1, .5, .9], axis=0)
    curves = []
    for session, mean in enumerate(eq.mean(axis=0)):
        curves.append(dict(scenario=scenario, sizing=sizing, session=session,
            model_month=int(np.ceil(session/MONTH_SESSIONS)), mean_equity=mean,
            p10_equity=quant[0, session], p50_equity=quant[1, session], p90_equity=quant[2, session],
            mean_future_pnl=mean-start_equity, p10_future_pnl=quant[0, session]-start_equity,
            p50_future_pnl=quant[1, session]-start_equity, p90_future_pnl=quant[2, session]-start_equity,
            mean_cumulative_pnl=mean-INITIAL, mean_return_pct=(mean/start_equity-1)*100,
            mean_total_return_pct=(mean/INITIAL-1)*100))
    return annual, curves, monthly_rows(model, scenario, sizing)


def render_report(meta, annual, methodology):
    lines = ["# V13-V10-G options: one-year scenario projections", "",
        "**12.5% premium stop, 25% target; ATM CE for LONG and ATM PE for SHORT. Start with three lots; compare fixed, monthly equity-based sizing and profit-based monthly lot increases.**", "",
        f"Historical net Rs{meta['history_net_pnl']:,.2f} across {meta['trades']} closed trades, "
        f"{meta['historical_sessions']} source sessions ({meta['active_sessions']} active and {meta['zero_trade_sessions']} with zero trades), "
        f"{meta['historical_first_day']} to {meta['historical_last_day']}. Initial account Rs{INITIAL:,.0f}; "
        f"projection starts at the historical ending equity of Rs{meta['projection_start_equity']:,.2f}.", "",
        "These are hypothetical outcomes conditional on repeatedly sampling this very small, post-hoc history. "
        "Percentiles and loss frequencies describe the simulation, not calibrated probabilities of future performance.", "",
        f"## {meta['horizon']} additional sessions", "",
        "| Sizing | Scenario | Mean future net | Mean ending equity | P10-P90 ending equity | Future return | Simulation loss frequency | P90 realized drawdown |",
        "|---|---|---:|---:|---:|---:|---:|---:|"]
    labels = {s["id"]: s["label"] for s in SCENARIOS}
    sizing_labels = {s["id"]: s["label"] for s in meta["sizing_policies"]}
    for row in annual:
        lines.append(f"| {sizing_labels[row['sizing']]} | {labels[row['scenario']]} | Rs{row['mean_future_pnl']:,.0f} | Rs{row['mean_ending_equity']:,.0f} | "
                     f"Rs{row['p10_ending_equity']:,.0f} - Rs{row['p90_ending_equity']:,.0f} | {row['mean_return_pct']:.2f}% | "
                     f"{row['probability_loss']*100:.2f}% | Rs{row['p90_max_drawdown']:,.0f} |")
    lines += ["", "Future return divides additional P&L by projection opening equity. Ending equity includes the original "
              "Rs15 lakh plus historical and additional simulated P&L. All policies retain profits in the cash account. "
              "Fixed sizing stays at three lots; monthly equity sizing adjusts from each path's realized equity; profit-based sizing adds whole lots after profitable model months subject to full-premium affordability.", "",
              "## How the model works", ""]
    lines += [f"- {value}" for value in methodology["rules"]]
    lines += ["", "## Coverage and limits", ""]
    lines += [f"- {value}" for value in methodology["limitations"]]
    lines += ["", "## Reproduce and inspect", "", "`python -B fno_v13_v10_g_options_projection.py`", "",
              "[Full payload](projection_payload.json) | [Annual scenarios](annual_summary.csv) | "
              "[Monthly projections](monthly_projections.csv) | [Daily curves](daily_curves.csv) | "
              "[Observed history](historical_daily.csv) | [Methodology](methodology.json) | [Validation](validation.json)", ""]
    return "\n".join(lines)


def run(source=SOURCE, output=OUTPUT, paths=PATHS, horizon=HORIZON, seed=SEED, stepup_profit_step_rupees=500_000.):
    source, output = Path(source).resolve(), Path(output).resolve()
    if source == output or output in source.parents:
        raise ValueError("Projection output must not overwrite the historical source run")
    ledger, trades, history, profile, summary, verified = load_history(source)
    templates = day_templates(trades, history.day)
    start_equity = INITIAL + float(trades.net_pnl.sum())
    draws = block_draws(len(templates), paths=paths, horizon=horizon, seed=seed)
    parity = simulate(templates, np.arange(len(templates), dtype=int)[None, :], INITIAL, 1., 0.)
    np.testing.assert_allclose(parity["pnl"][0], history.net_pnl, rtol=1e-12, atol=1e-7)
    np.testing.assert_array_equal(parity["accepted"][0], history.trades)
    np.testing.assert_allclose(parity["peak_premium"].max(), summary["peak_reserved_premium_and_fees"], atol=1e-7)
    if parity["rejected"].sum():
        raise AssertionError("Historical cash parity unexpectedly rejected trades")
    annual, curves, monthly = [], [], []
    for sizing in ("fixed", "monthly", "stepup"):
        for scenario in SCENARIOS:
            model = simulate(templates, draws, start_equity, scenario["fraction"], scenario["extra_cost_bps_per_side"], sizing, stepup_profit_step_rupees)
            a, c, m = summarize(model, scenario["id"], start_equity, sizing)
            np.testing.assert_allclose(sum(row["mean_pnl"] for row in m), a["mean_future_pnl"], atol=1e-6)
            annual.append(a)
            curves.extend(c)
            monthly.extend(m)
    sizing_policies = [
        dict(id="fixed", label="Fixed three lots", initial_lots=LOTS, description="Exactly three lots for every accepted trade."),
        dict(id="monthly", label="Monthly equity-based lots", initial_lots=LOTS,
             reference_equity=start_equity, monthly_fractional_increase_cap_pct=10.,
             formula="budget = min(3 * month_opening_realized_equity / projection_opening_equity, previous_fractional_budget * 1.10); lots = floor(max(0, budget))",
             description="Rebalance from each path's realized opening equity every 21 sessions; retain fractional budget between months; reduce immediately after losses."),
        dict(id="stepup", label="Profit-based monthly lot increases", initial_lots=LOTS,
             profit_step_rupees=stepup_profit_step_rupees, reference_equity=start_equity,
             formula=f"if previous_month_realized_net > 0: lots = previous_lots + 1 + floor(max(0, month_opening_equity - projection_opening_equity) / {stepup_profit_step_rupees:g}); else: lots = previous_lots",
             description=f"After a profitable model month, add one lot plus one extra lot for each Rs{stepup_profit_step_rupees:,.0f} of cumulative future realized profit; hold size after flat or losing months. Lot count never decreases, and unaffordable entries are skipped."),
    ]
    meta = dict(schema="V13_V10_G_OPTIONS_SIZING_PROJECTION_V2", source_run=str(source),
        initial_capital=INITIAL, projection_start_equity=start_equity, history_net_pnl=float(trades.net_pnl.sum()),
        historical_first_day=history.day.iloc[0], historical_last_day=history.day.iloc[-1],
        historical_sessions=len(history), active_sessions=int(history.trades.gt(0).sum()),
        zero_trade_sessions=int(history.trades.eq(0).sum()),
        **{k: summary[k] for k in ("wins", "losses", "win_rate_pct", "profit_factor", "daily_realized_drawdown", "costs", "gross_pnl", "peak_reserved_premium_and_fees")},
        trades=len(trades), retrospective_metadata_trades=int(trades.metadata_retrospective.eq(True).sum()),
        missing_preexpiry_attempts=int(ledger.mapping_status.eq("MISSING_MONTHLY_OPTION_METADATA").sum()),
        lots=LOTS, stop_pct=profile["stop_pct"], target_pct=profile["target_pct"], paths=paths, horizon=horizon,
        block_sessions=BLOCK, model_month_sessions=MONTH_SESSIONS, seed=seed,
        sizing_policies=sizing_policies, default_sizing="stepup", stepup_profit_step_rupees=stepup_profit_step_rupees,
        historical_slippage_bps=profile["slippage_bps"], source_manifest_sha256=digest(source/"manifest.json"))
    methodology = dict(meta=meta, scenarios=SCENARIOS, rules=[
        f"Generate {paths:,} paths of {horizon} additional assumed trading sessions using circular five-session blocks, seed {seed}; every scenario uses identical draws.",
        "The sampling calendar is the 13 available source sessions from Aug 26 through Sep 11, including Sep 3, Sep 4 and Sep 8 with no executed trades. Earlier uncovered August-expiry sessions are excluded.",
        "Each sampled session preserves trade order, overlapping positions and original historical contract lot size. Fixed mode always plans three lots. The two monthly policies change the number of whole lots; there is no stock margin multiplier or leverage.",
        "Monthly equity mode sets fractional budget = min(3 x this path's month-opening realized equity / projection-opening equity, previous fractional budget x 1.10). It starts at three lots, retains the fractional budget between months and floors only the executable lot count. Reductions apply immediately. The 10% cap applies to fractional budget; whole-lot changes such as 3 to 4 may exceed 10%.",
        f"Profit-based step-up mode begins at three lots. After each profitable model month, next month's lot count increases by 1 + floor(max(0, month-opening equity - projection-opening equity) / Rs{stepup_profit_step_rupees:,.0f}). Flat or losing months hold size. The threshold is an explicit illustrative modeling assumption: Rs{stepup_profit_step_rupees:,.0f} cumulative future profit allows +2 lots; Rs{stepup_profit_step_rupees*2:,.0f} allows +3. Historical backtest profit is excluded. The increment is recalculated and may shrink after a profit drawdown; accumulated lot count never decreases. Unaffordable entries are skipped in full.",
        "Both monthly policies rebalance every 21 additional sessions, using only information available at that point. A zero-lot equity budget pauses candidates without an order or fee; paused candidates are reported separately from positive-size cash rejections.",
        "Entry pays full option premium, recomputed buy fees and any scenario execution buffer from available cash. Unaffordable entries are skipped entirely, with no fee or later P&L. Proceeds become spendable only at exit_observed_ts.",
        "The 100% reference retains source gross P&L. Other scenarios multiply positive gross P&L by 75%, 50% or 20%, while retaining negative gross P&L. The implied sale price is entry price plus scenario gross P&L divided by quantity; the 12.5%/25% source strategy itself is not reoptimized.",
        "Buy and sell fees are recomputed through the pinned original option_order_costs implementation at each actual whole-lot quantity, including fixed Rs20 per order and premium-turnover taxes. Three-lot fees are never scaled linearly. Saved source fills already include the original slippage, which is not charged a second time.",
        "75%, 50% and 20% cases additionally debit 10 bps of buy premium and 10 bps of hypothetical sell premium as separate execution-cost buffers. Reference has no extra buffer. Entry admission never uses the future sale price.",
        "A model month contains 21 additional sessions. Monthly P&L means reconcile to annual future P&L. Equity includes retained cash and realized P&L; drawdowns use daily realized closing equity and exclude intraday mark-to-market losses.",
        "Future returns use the Rs16.14943 lakh projection opening account; cumulative/total returns use the original Rs15 lakh. P10/P50/P90 are pointwise percentiles across bootstrap paths, not confidence limits on a calibrated forecast.",
    ], limitations=[
        "Only 20 executed trades are available over 13 source sessions, with ten active sessions and three sessions without fills. Repeated circular blocks cannot represent unseen regimes, expiry cycles or tail losses.",
        "Forty triggered source attempts lack earlier August-expiry metadata/history. They are excluded from projection calibration rather than counted as zero-return observed sessions.",
        "Nine executed August trades use contract metadata reconstructed from a later snapshot. Historical option lot sizes and premiums are reused as templates, not predictions of future contract specifications or premiums.",
        "The 12.5%/25% setting was requested after reviewing prior results. The source G selection also reused this history, so neither these projections nor the later source partition are untouched out-of-sample validation.",
        "The original five-minute fills retain bar ambiguity, volume proxy and absent bid/ask-depth limitations. The modeled fee schedule remains the source research assumption; it is not a verified future rate calendar.",
        "Larger projected orders reuse the historical template fill prices; no new depth, participation or market-impact capacity is modeled. The additional cost buffer does not establish that larger quantities could actually fill. Monthly sizing can increase cash rejections and realized losses.",
        "Haircut scenarios stress observed payoffs and execution cost; they do not generate a new option-bar backtest, preserve an actual future target-hit rate, or estimate future win probabilities.",
    ], source_hashes=verified)
    history_columns = ["session", "day", "net_pnl", "cumulative_net_pnl", "opening_equity", "closing_equity", "trades", "wins", "losses", "return_pct", "cumulative_return_pct", "costs", "gross_pnl", "option_data_present"]
    trade_columns = ["trade_id", "day", "setup_id", "option_type", "option_symbol", "entry_ts", "exit_ts", "exit_observed_ts", "lot_size", "lots", "quantity", "entry_price", "stop_price", "target_price", "exit_price", "reason", "gross_pnl", "total_costs", "net_pnl", "metadata_retrospective"]
    payload = dict(meta=meta, scenarios=SCENARIOS, annual=annual, curves=curves, monthly=monthly,
                   history=history[history_columns].to_dict("records"), historical_trades=trades[trade_columns].to_dict("records"))
    output.mkdir(parents=True, exist_ok=True)
    for filename, rows in [("annual_summary.csv", annual), ("daily_curves.csv", curves),
                           ("monthly_projections.csv", monthly), ("historical_daily.csv", payload["history"])]:
        pd.DataFrame(rows).to_csv(output / filename, index=False)
    write_json(output / "projection_payload.json", payload)
    write_json(output / "methodology.json", methodology)
    write_json(output / "validation.json", dict(passed=True, source_artifact_hashes_verified=len(verified)-2,
        execution_fee_code_hash_verified=True, historical_daily_cash_pnl_exact_replay=True,
        historical_trade_count_parity=True, historical_peak_premium_parity=True,
        source_calendar_sessions=len(history), zero_trade_sessions=int(history.trades.eq(0).sum()),
        monthly_annual_means_reconcile=True, fixed_lots=LOTS, sizing_policies=[p["id"] for p in sizing_policies],
        stepup_profit_step_rupees=stepup_profit_step_rupees, shared_draws_sha256=hashlib.sha256(draws.tobytes()).hexdigest()))
    (output / "V13_V10_G_OPTIONS_ONE_YEAR_SCENARIOS.md").write_text(render_report(meta, annual, methodology), encoding="utf-8")
    generated_names = ["annual_summary.csv", "daily_curves.csv", "monthly_projections.csv", "historical_daily.csv", "projection_payload.json", "methodology.json", "validation.json", "V13_V10_G_OPTIONS_ONE_YEAR_SCENARIOS.md"]
    write_json(output / "manifest.json", dict(complete=True, meta=meta, source_hashes=verified,
        code_sha256={str(Path(__file__).resolve()): digest(__file__)},
        artifacts={name: digest(output/name) for name in generated_names}))
    print(json.dumps(json_value(dict(output=str(output), meta=meta, annual=annual)), indent=2))
    return payload


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-run", type=Path, default=SOURCE)
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--paths", type=int, default=PATHS)
    parser.add_argument("--horizon", type=int, default=HORIZON)
    parser.add_argument("--seed", type=int, default=SEED)
    parser.add_argument("--stepup-profit-step-rupees", type=float, default=500_000.)
    args = parser.parse_args()
    run(args.source_run, args.output_dir or args.source_run/"one_year_scenarios", args.paths, args.horizon, args.seed, args.stepup_profit_step_rupees)


if __name__ == "__main__":
    main()
