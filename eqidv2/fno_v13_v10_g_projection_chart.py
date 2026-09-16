"""Historical G returns and cash-constrained, explicitly hypothetical one-year scenarios."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter, MaxNLocator
import numpy as np
import pandas as pd

SOURCE = Path("C:/TradingData/eqidv2/fno_oi/strategy_research/v13_corrected_v10_g/run_20260914_capital_3l_portfolio_15l")
OUTPUT = SOURCE / "one_year_scenarios"
INITIAL = 1_500_000.
MARGIN = 300_000.
NOTIONAL = MARGIN * 5
HORIZON = 252
SEED = 20260914
BLOCK = 5
MONTH_SESSIONS = 21
DEPLOYMENT_FRACTION = .80
MONTHLY_INCREASE_CAP = .10
MARGIN_INCREMENT = 10_000.
SCENARIOS = [
    dict(id="reference", label="Historical pace reference", fraction=1., cost_bps=5., color="#4B6F95"),
    dict(id="retain_75", label="75% of winning gross P&L", fraction=.75, cost_bps=10., color="#A66C17"),
    dict(id="retain_50", label="50% of winning gross P&L", fraction=.50, cost_bps=10., color="#008879"),
    dict(id="retain_20", label="20% of winning gross P&L", fraction=.20, cost_bps=10., color="#C24D57"),
]


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def load_history():
    manifest = json.loads((SOURCE / "manifest.json").read_text(encoding="utf-8"))
    names = ["result/portfolio_trades.csv", "daily_detailed.csv", "configuration.json"]
    expected = {k.replace("\\", "/"): v for k, v in manifest["artifacts"].items()}
    for name in names:
        if digest(SOURCE / name) != expected[name]:
            raise ValueError(f"Sizing replay source changed: {name}")
    ledger = pd.read_csv(SOURCE / names[0], float_precision="round_trip")
    trades = ledger.loc[ledger.portfolio_executed.eq(True)].copy()
    daily = pd.read_csv(SOURCE / names[1], float_precision="round_trip")
    if len(daily) != 31 or len(trades) != 66 or len(ledger) != 73:
        raise ValueError("Unexpected historical G sample")
    for col in ["entry_ts", "exit_ts", "confirmation_ts"]:
        trades[col] = pd.to_datetime(trades[col], utc=True).dt.tz_convert("Asia/Kolkata")
    if not trades.exposure_per_entry_rupees.eq(NOTIONAL).all():
        raise ValueError("Source exposure is not Rs3 lakh times five")
    np.testing.assert_allclose(trades.portfolio_gross_profit_rupees-trades.portfolio_cost_rupees,
                               trades.portfolio_net_profit_rupees, atol=1e-7)
    grouped = trades.groupby("day").portfolio_net_profit_rupees.sum().reindex(daily.day, fill_value=0)
    np.testing.assert_allclose(grouped, daily.net_profit_rupees, atol=1e-7)
    daily["session"] = np.arange(1, len(daily)+1)
    daily["opening_equity"] = INITIAL + daily.net_profit_rupees.cumsum().shift(fill_value=0)
    daily["closing_equity"] = daily.opening_equity + daily.net_profit_rupees
    daily["account_daily_return_pct"] = daily.net_profit_rupees / daily.opening_equity * 100
    daily["account_cumulative_return_pct"] = (daily.closing_equity / INITIAL - 1) * 100
    daily["peak_deployed_margin"] = [
        float((q.portfolio_reserved_capital_before_rupees+q.portfolio_trade_capital_rupees).max()) if len(q) else 0
        for day in daily.day for q in [trades.loc[trades.day.eq(day)]]
    ]
    daily["deployed_margin_daily_return_pct"] = daily.net_profit_rupees / daily.peak_deployed_margin.replace(0, np.nan)*100
    return ledger, trades, daily, {str(SOURCE/name): expected[name] for name in names}


def day_templates(trades, days):
    """Within each sampled day keep trade chronology, overlap and native entry priority."""
    result = []
    order_columns = ["entry_ts", "confirmation_ts", "setup_id", "portfolio_priority_value", "tradingsymbol", "sid"]
    for day in days:
        frame = trades.loc[trades.day.eq(day)].sort_values(order_columns,
            ascending=[True, True, True, False, True, True], kind="stable").reset_index(drop=True)
        events = []
        for rank, row in frame.iterrows():
            entry, close = row.entry_ts.value, row.exit_ts.value
            gross = float(row.portfolio_gross_profit_rupees)
            events.append((entry, 1, rank*2, "entry", rank, gross))
            # Prior exits release margin before new entries; zero-duration fills
            # reserve and release their own margin in native entry order.
            events.append((close, 1 if close == entry else 0, rank*2+1, "exit", rank, gross))
        events.sort(key=lambda event: event[:3])
        result.append(dict(events=events, count=len(frame)))
    return result


def block_draws(n_days, paths=5000, horizon=HORIZON, block=BLOCK, seed=SEED):
    rng = np.random.default_rng(seed)
    starts = rng.integers(0, n_days, size=(paths, int(np.ceil(horizon/block))))
    # Circular blocks give all 31 source sessions equal marginal weight.
    return ((starts[:, :, None]+np.arange(block)) % n_days).reshape(paths, -1)[:, :horizon]


def monthly_margin(opening_equity, previous_margin):
    """Rebalance from realized month-opening equity only; no scheduled forced growth."""
    affordable = np.maximum(0., np.asarray(opening_equity)) * DEPLOYMENT_FRACTION / 5
    capped = np.minimum(affordable, np.asarray(previous_margin) * (1 + MONTHLY_INCREASE_CAP))
    return np.floor((capped + 1e-8) / MARGIN_INCREMENT) * MARGIN_INCREMENT


def simulate(templates, draws, start_equity, fraction, cost_bps, sizing="fixed"):
    """Replay finite cash; optionally resize at the start of each 21-session model month."""
    if not 0 <= fraction <= 1 or cost_bps < 0 or start_equity < 0:
        raise ValueError("Invalid scenario assumption")
    if sizing not in ("fixed", "monthly"):
        raise ValueError("Unknown sizing policy")
    paths, horizon = draws.shape
    equity = np.empty((paths, horizon+1))
    equity[:, 0] = start_equity
    pnl = np.zeros((paths, horizon))
    accepted = np.zeros((paths, horizon), dtype=np.int16)
    rejected = np.zeros((paths, horizon), dtype=np.int16)
    margins = np.empty((paths, horizon))
    current_margin = np.full(paths, MARGIN)
    for step in range(horizon):
        if sizing == "monthly" and step % MONTH_SESSIONS == 0:
            current_margin = monthly_margin(equity[:, step], current_margin)
        margins[:, step] = current_margin
        for number, template in enumerate(templates):
            pick = np.flatnonzero(draws[:, step] == number)
            if not len(pick):
                continue
            cash = equity[pick, step].copy()
            margin = current_margin[pick]
            scale = margin / MARGIN
            half_cost = margin * 5 * cost_bps / 10000 / 2
            used = np.zeros(len(pick), dtype=np.int16)
            admitted = np.zeros((len(pick), template["count"]), dtype=bool)
            net = np.zeros(len(pick))
            for _, _, _, kind, rank, gross in template["events"]:
                if kind == "entry":
                    ok = (margin > 0) & (used < 5) & (cash >= margin + half_cost - 1e-8)
                    admitted[:, rank] = ok
                    cash[ok] -= margin[ok] + half_cost[ok]
                    net[ok] -= half_cost[ok]
                    used[ok] += 1
                    accepted[pick, step] += ok
                    rejected[pick, step] += ~ok
                else:
                    ok = admitted[:, rank]
                    scenario_gross = gross * fraction if gross > 0 else gross
                    cash[ok] += margin[ok] + scenario_gross * scale[ok] - half_cost[ok]
                    net[ok] += scenario_gross * scale[ok] - half_cost[ok]
                    used[ok] -= 1
            if np.any(used != 0):
                raise AssertionError("All positions must close by end of sampled day")
            np.testing.assert_allclose(cash, equity[pick, step]+net, atol=1e-6, rtol=1e-12)
            equity[pick, step+1] = cash
            pnl[pick, step] = net
    return dict(equity=equity, pnl=pnl, accepted=accepted, rejected=rejected, margin=margins)


def historical_months(daily, trades):
    rows = []
    for month, frame in daily.groupby(daily.day.str[:7], sort=True):
        ledger = trades.loc[trades.day.str.startswith(month)]
        gains = ledger.portfolio_net_profit_rupees.clip(lower=0).sum()
        losses = -ledger.portfolio_net_profit_rupees.clip(upper=0).sum()
        rows.append(dict(month=month, first_observed_day=frame.day.iloc[0], last_observed_day=frame.day.iloc[-1],
            observed_sessions=len(frame), selected=int(frame.selected.sum()), executed=len(ledger),
            wins=int(frame.wins.sum()), losses=int(frame.losses.sum()),
            win_rate_pct=frame.wins.sum()/len(ledger)*100 if len(ledger) else np.nan,
            profit_factor=gains/losses if losses else np.inf,
            opening_equity=float(frame.opening_equity.iloc[0]), closing_equity=float(frame.closing_equity.iloc[-1]),
            net_profit_rupees=float(frame.net_profit_rupees.sum()),
            account_month_return_pct=float(frame.net_profit_rupees.sum()/frame.opening_equity.iloc[0]*100)))
    return pd.DataFrame(rows)


def monthly_rows(model, scenario, sizing):
    rows = []
    eq = model["equity"]
    for start in range(0, model["pnl"].shape[1], MONTH_SESSIONS):
        end = min(start+MONTH_SESSIONS, model["pnl"].shape[1])
        pnl = model["pnl"][:, start:end].sum(axis=1)
        np.testing.assert_allclose(pnl, eq[:, end]-eq[:, start], atol=1e-6)
        margin = model["margin"][:, start]
        # Average each path's return; do not divide average P&L by average equity.
        returns = np.divide(pnl, eq[:, start], out=np.full(len(pnl), np.nan), where=eq[:, start] > 0)*100
        rows.append(dict(scenario=scenario, sizing=sizing, model_month=start//MONTH_SESSIONS+1,
            first_additional_session=start+1, last_additional_session=end,
            mean_opening_equity=eq[:, start].mean(), mean_ending_equity=eq[:, end].mean(),
            mean_monthly_net_profit=pnl.mean(), mean_monthly_account_return_pct=np.nanmean(returns),
            p10_monthly_net_profit=np.quantile(pnl, .1), median_monthly_net_profit=np.median(pnl),
            p90_monthly_net_profit=np.quantile(pnl, .9),
            mean_margin_per_trade=margin.mean(), median_margin_per_trade=np.median(margin),
            p10_margin_per_trade=np.quantile(margin, .1), p90_margin_per_trade=np.quantile(margin, .9),
            mean_notional_per_trade=margin.mean()*5,
            mean_executed_trades=model["accepted"][:, start:end].sum(axis=1).mean(),
            mean_rejected_trades=model["rejected"][:, start:end].sum(axis=1).mean(),
            p10_ending_equity=np.quantile(eq[:, end], .1), median_ending_equity=np.median(eq[:, end]),
            p90_ending_equity=np.quantile(eq[:, end], .9)))
    return rows


def charts(output, daily, results, historical_monthly, projected_monthly):
    plt.rcParams.update({"font.family": "DejaVu Sans", "font.size": 10,
                         "axes.spines.top": False, "axes.spines.right": False,
                         "axes.labelcolor": "#303B4A", "text.color": "#253043",
                         "xtick.color": "#596575", "ytick.color": "#596575"})
    fig = plt.figure(figsize=(18, 16.2), facecolor="white")
    gs = fig.add_gridspec(4, 2, height_ratios=[1.65, 1.0, .93, 1.], hspace=.65, wspace=.21)
    fig.subplots_adjust(left=.075, right=.92, top=.86, bottom=.075)
    fig.suptitle("V13–V10–G  |  Backtest and one-year scenario projections", x=.075, y=.977,
                 ha="left", fontsize=22, fontweight="bold")
    fig.text(.075, .944, "₹3 lakh margin per trade × 5 = ₹15 lakh exposure  •  Initial account ₹15 lakh  •  Fixed trade size; no reinvestment into larger positions", fontsize=11)
    fig.text(.075, .919, "Observed: 31 available sessions, 29 Jul–11 Sep 2026  |  Projection: 252 additional assumed trading sessions (roughly one year)", fontsize=10.5)
    ax = fig.add_subplot(gs[0, :])
    history_x = np.arange(len(daily)+1)
    history = np.r_[0, daily.cumulative_net_profit_rupees] / 100000
    future_x = np.arange(HORIZON+1)+len(daily)
    ax.plot(history_x, history, color="#142E46", lw=2.8, label="Observed backtest")
    ax.scatter([len(daily)], [history[-1]], color="#142E46", s=40, zorder=8)
    ax.axvline(len(daily), color="#83909C", lw=1.1, ls=":")
    ax.axhline(0, color="#65727F", lw=.8)
    reference = results["retain_50"]["equity"]
    quant = (np.quantile(reference, [.1, .9], axis=0)-INITIAL)/100000
    ax.fill_between(future_x, quant[0], quant[1], color="#008879", alpha=.12,
                    label="50% case: conditional P10–P90 band")
    for spec in SCENARIOS:
        paths = results[spec["id"]]["equity"]
        mean = (paths.mean(axis=0)-INITIAL)/100000
        ax.plot(future_x, mean, color=spec["color"], lw=2, ls="--", label=spec["label"])
        ax.annotate(f"{mean[-1]:+.2f}L", (future_x[-1], mean[-1]), xytext=(7, 0),
                    textcoords="offset points", fontsize=10.5, va="center", color=spec["color"])
    ax.annotate(f"Observed net ₹{history[-1]:.2f}L\n35.78% account return", (31, history[-1]),
                xytext=(4, 28), textcoords="offset points", fontsize=9.5,
                arrowprops=dict(arrowstyle="-", color="#65727F", lw=.8))
    ax.set_xlim(0, HORIZON+len(daily)+24)
    ax.set_ylabel("Cumulative net P&L (₹ lakh)")
    ax.set_xticks([0, 31, 94, 157, 220, 283],
                  ["Start", "Backtest end", "+63 sessions", "+126 sessions", "+189 sessions", "+252 ≈ 1 year"])
    ax.set_xlabel("Scenario lines = simulated mean account P&L; endpoint labels include the historical ₹5.37 lakh", labelpad=10)
    ax.yaxis.set_major_locator(MaxNLocator(6))
    ax.grid(axis="y", alpha=.19)
    right = ax.secondary_yaxis("right", functions=(lambda lakh: lakh/15*100, lambda pct:pct*15/100))
    right.set_ylabel("Total return on initial ₹15 lakh (%)", labelpad=10)
    handles, labels = ax.get_legend_handles_labels()
    ax.legend(handles, labels, loc="lower left", bbox_to_anchor=(0, 1.035), ncol=3,
              frameon=False, fontsize=9.5, handlelength=2.8)

    b = fig.add_subplot(gs[1, 0])
    c = fig.add_subplot(gs[1, 1])
    t = daily.session.to_numpy()
    colors = np.where(daily.net_profit_rupees.ge(0), "#008879", "#C24D57")
    b.bar(t, daily.net_profit_rupees/1000, color=colors, width=.75)
    b.axhline(0, color="#65727F", lw=.8)
    b.set_title("Observed daily profit / loss", loc="left", fontweight="bold", pad=12)
    b.set_ylabel("Daily net P&L (₹ thousand)")
    c.plot(t, daily.cumulative_net_profit_rupees/100000, color="#142E46", lw=2.2)
    c.fill_between(t, daily.cumulative_net_profit_rupees/100000, color="#142E46", alpha=.05)
    c.set_title("Observed cumulative profit", loc="left", fontweight="bold", pad=12)
    c.set_ylabel("Cumulative net P&L (₹ lakh)")
    c.text(.025, .91, "Final net ₹5,36,625.56", transform=c.transAxes, fontsize=10)
    for plot in (b, c):
        plot.set_xlim(.2, 31.8)
        ticks=[1, 7, 13, 19, 25, 31]
        plot.set_xticks(ticks, [pd.Timestamp(daily.day.iloc[i-1]).strftime("%d %b") for i in ticks])
        plot.set_xlabel("Available backtest sessions; 7 had no executions")
        plot.grid(axis="y", alpha=.15)
        plot.yaxis.set_major_locator(MaxNLocator(5))

    d = fig.add_subplot(gs[2, 0])
    d.plot(t, daily.account_daily_return_pct, color="#142E46", marker="o", ms=3,
           lw=1.7, label="Daily account return")
    d.plot(t, daily.deployed_margin_daily_return_pct, color="#A66C17", marker="s", ms=3,
           lw=1.3, label="Daily return on peak margin used")
    d.axhline(0, color="#65727F", lw=.8)
    d.set_title("Observed daily returns — two different denominators", loc="left", fontweight="bold", pad=12)
    d.set_ylabel("Daily return (%)")
    d.set_xlim(.2, 31.8)
    d.set_xticks([1,7,13,19,25,31], [pd.Timestamp(daily.day.iloc[i-1]).strftime("%d %b") for i in [1,7,13,19,25,31]])
    d.set_xlabel("Peak-margin returns are not added to calculate account return")
    d.grid(axis="y", alpha=.15)
    d.legend(loc="upper left", frameon=False, fontsize=8.8)
    e = fig.add_subplot(gs[2, 1])
    e.axis("off")
    e.set_title("Projection assumptions", loc="left", fontweight="bold", pad=12)
    e.text(0, .90,
           "Reference: original gross payoffs; 5 bps total cost/trade.\n"
           "Stress cases: retain 75%, 50% or 20% of positive gross P&L;\n"
           "losing gross P&L unchanged; 10 bps total cost/trade.\n\n"
           f"{len(reference):,} paths resample 5-session blocks, including zero-trade days.\n"
           "Cash checked at entry; margin released at exit; maximum 5 positions.\n"
           "Band shows only variability within the 50% scenario assumptions.\n"
           "No scenario probabilities or unseen-regime coverage are implied.",
           transform=e.transAxes, va="top", fontsize=10, linespacing=1.55)
    f = fig.add_subplot(gs[3, 0])
    months = historical_monthly
    bars = f.bar(np.arange(len(months)), months.net_profit_rupees/100000, color="#142E46", width=.5)
    f.bar_label(bars, labels=[f"₹{v:.2f}L" for v in months.net_profit_rupees/100000], padding=5, fontsize=10)
    f.set_xticks(np.arange(len(months)), ["Jul 29–31\n3 sessions", "August\n19 available sessions", "Sep 1–11\n9 sessions"])
    f.set_ylim(0, (months.net_profit_rupees/100000).max()*1.27)
    f.set_ylabel("Monthly net P&L (₹ lakh)")
    f.set_title("Observed calendar-month results — partial coverage", loc="left", fontweight="bold", pad=12)
    f.set_xlabel("August 24–25 absent from the retained sample")
    f.grid(axis="y", alpha=.15)
    g = fig.add_subplot(gs[3, 1])
    for spec in SCENARIOS:
        part = projected_monthly.loc[projected_monthly.scenario.eq(spec["id"]) & projected_monthly.sizing.eq("fixed")]
        g.plot(part.model_month, part.mean_monthly_net_profit/100000, color=spec["color"], marker="o", ms=3, lw=1.7,
               label=f"{spec['fraction']:.0%} retained")
    g.axhline(0, color="#65727F", lw=.8)
    g.set_xticks(range(1, 13), [f"M{i}" for i in range(1, 13)])
    g.set_ylabel("Mean monthly net P&L (₹ lakh)")
    g.set_title("Projected monthly P&L — fixed ₹3 lakh per trade", loc="left", fontweight="bold", pad=12)
    g.set_xlabel("Each model month = 21 assumed trading sessions")
    g.legend(frameon=False, ncol=2, fontsize=9)
    g.grid(axis="y", alpha=.15)
    fig.text(.075, .025,
             "31 reused development sessions cannot establish a one-year forecast. Scenarios are hypothetical; 252 sessions is an assumption, not a verified NSE calendar.",
             fontsize=10, color="#586270")
    for suffix in ("png", "svg"):
        fig.savefig(output / f"V13_V10_G_BACKTEST_AND_ONE_YEAR.{suffix}", dpi=170, facecolor="white")
    plt.close(fig)


def sizing_chart(output, daily, results, adaptive, monthly):
    """Compare equal sampled opportunities with fixed versus monthly allocation."""
    fig = plt.figure(figsize=(18, 14), facecolor="white")
    gs = fig.add_gridspec(3, 2, height_ratios=[1, 1, .92], hspace=.55, wspace=.23)
    fig.subplots_adjust(left=.075, right=.96, top=.86, bottom=.095)
    fig.suptitle("V13–V10–G  |  Monthly sizing and its effect on projections", x=.075, y=.974,
                 ha="left", fontsize=22, fontweight="bold")
    fig.text(.075, .943, "Month-opening allocation = min(16% of account equity, previous allocation × 1.10), rounded down to ₹10,000", fontsize=11)
    fig.text(.075, .918, "5× exposure; five slots; 20% month-opening margin buffer; losses can reduce size. No new deposits. Each model month = 21 sessions.", fontsize=10.5)
    fig.text(.075, .888, "Dark solid: observed backtest   •   Blue dashed: fixed ₹3L   •   Coloured solid: monthly rule   •   Shading: monthly-rule conditional P10–P90", fontsize=10)
    past_x = np.arange(len(daily)+1)
    future_x = np.arange(HORIZON+1)+len(daily)
    for index, spec in enumerate(SCENARIOS):
        ax = fig.add_subplot(gs[index//2, index % 2])
        fixed = (results[spec["id"]]["equity"].mean(axis=0)-INITIAL)/100000
        paths = adaptive[spec["id"]]["equity"]
        resized = (paths.mean(axis=0)-INITIAL)/100000
        lo, hi = (np.quantile(paths, [.1, .9], axis=0)-INITIAL)/100000
        ax.plot(past_x, np.r_[0, daily.cumulative_net_profit_rupees]/100000, color="#142E46", lw=2.4)
        ax.fill_between(future_x, lo, hi, color=spec["color"], alpha=.12)
        ax.plot(future_x, fixed, color="#557CA4", ls="--", lw=2)
        ax.plot(future_x, resized, color=spec["color"], lw=2.2)
        ax.axvline(len(daily), color="#82909C", ls=":", lw=1)
        ax.axhline(0, color="#65727F", lw=.7)
        ax.set_title(f"{spec['label']} · costs {spec['cost_bps']:.0f} bps", loc="left", fontsize=12, fontweight="bold", pad=12)
        ax.text(.025, .96, f"Mean cumulative P&L at M12: fixed ₹{fixed[-1]:.2f}L / monthly ₹{resized[-1]:.2f}L",
                transform=ax.transAxes, va="top", fontsize=9, bbox=dict(facecolor="white", edgecolor="none", alpha=.8))
        ax.set_xlim(0, 288)
        ax.set_xticks([0, 31, 94, 157, 220, 283], ["Start", "Backtest\nend", "M3", "M6", "M9", "M12"])
        ax.set_ylabel("Cumulative net P&L (₹ lakh)")
        ax.yaxis.set_major_locator(MaxNLocator(5))
        ax.grid(axis="y", alpha=.15)
        ax.margins(y=.20)
    ax = fig.add_subplot(gs[2, 0])
    for spec in SCENARIOS:
        q = monthly.loc[monthly.scenario.eq(spec["id"]) & monthly.sizing.eq("monthly")]
        ax.plot(q.model_month, q.mean_margin_per_trade/100000, lw=2, color=spec["color"], marker="o", ms=3,
                label=f"{spec['fraction']:.0%} retained")
    ax.axhline(3, color="#557CA4", lw=1.3, ls="--", label="Fixed ₹3 lakh")
    ax.set_title("Monthly margin per trade — simulation averages", loc="left", fontsize=12, fontweight="bold", pad=12)
    ax.set_ylabel("Cash / margin per trade (₹ lakh)")
    ax.set_xticks(range(1, 13), [f"M{i}" for i in range(1, 13)])
    ax.set_xlabel("Actual allocation uses your own month-opening equity")
    ax.legend(frameon=False, ncol=2, fontsize=9)
    ax.grid(axis="y", alpha=.15)
    ax = fig.add_subplot(gs[2, 1])
    q = monthly.loc[monthly.scenario.eq("retain_50")]
    x = np.arange(1, 13)
    for offset, policy, color, label in [(-.19, "fixed", "#557CA4", "Fixed ₹3 lakh"), (.19, "monthly", "#008879", "Monthly rule")]:
        values = q.loc[q.sizing.eq(policy), "mean_monthly_net_profit"].to_numpy()/100000
        ax.bar(x+offset, values, width=.36, color=color, label=label)
    ax.set_title("Monthly P&L — illustrative 50% payoff case", loc="left", fontsize=12, fontweight="bold", pad=12)
    ax.set_xticks(x, [f"M{i}" for i in x])
    ax.set_ylabel("Mean monthly net P&L (₹ lakh)")
    ax.set_xlabel("50% is gross-win payoff retention, not a win rate")
    ax.legend(frameon=False, fontsize=9)
    ax.grid(axis="y", alpha=.15)
    fig.text(.075, .046, "Endpoint P&L includes the observed ₹5.37 lakh. Projection starts with ₹20.37 lakh retained equity. Lines are simulated means, not forecasts.", fontsize=10)
    fig.text(.075, .024, "31 development sessions; no untouched test. Percentile bands exclude new-regime uncertainty. The margin buffer is set at rebalance, not maintained intraday.", fontsize=10, color="#586270")
    for suffix in ("png", "svg"):
        fig.savefig(output / f"V13_V10_G_MONTHLY_SIZING_COMPARISON.{suffix}", dpi=170, facecolor="white")
    plt.close(fig)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, default=OUTPUT)
    parser.add_argument("--paths", type=int, default=5000)
    args = parser.parse_args()
    if args.paths < 100:
        raise ValueError("At least 100 simulated paths required")
    output = args.output_dir
    output.mkdir(parents=True, exist_ok=True)
    _, trades, daily, sources = load_history()
    templates = day_templates(trades, daily.day)
    draws = block_draws(len(daily), args.paths)
    # Replaying the observed sequence with original payoffs must reproduce the
    # retained ledger under this stricter entry-cost cash accounting.
    actual = simulate(templates, np.arange(31)[None, :], INITIAL, 1., 5.)
    np.testing.assert_allclose(actual["pnl"][0], daily.net_profit_rupees, atol=1e-6)
    assert actual["rejected"].sum() == 0 and actual["accepted"].sum() == 66
    start = float(daily.closing_equity.iloc[-1])
    results, adaptive, summaries, projection_rows, month_rows = {}, {}, [], [], []
    for spec in SCENARIOS:
        for policy, destination in [("fixed", results), ("monthly", adaptive)]:
            print(f"Simulating {spec['label']}, {policy} sizing ({args.paths:,} paths)...", flush=True)
            model = simulate(templates, draws, start, spec["fraction"], spec["cost_bps"], sizing=policy)
            destination[spec["id"]] = model
            eq = model["equity"]
            p10, median, p90 = np.quantile(eq, [.1, .5, .9], axis=0)
            avg = eq.mean(axis=0)
            summaries.append(dict(scenario=spec["id"], sizing=policy, label=spec["label"], winning_gross_retained=spec["fraction"],
                cost_bps_total=spec["cost_bps"], simulated_mean_additional_profit=avg[-1]-start,
                simulated_mean_ending_equity=avg[-1], simulated_mean_cumulative_profit=avg[-1]-INITIAL,
                future_return_on_projection_opening_equity_pct=(avg[-1]/start-1)*100,
                total_return_on_original_capital_pct=(avg[-1]/INITIAL-1)*100,
                conditional_p10_ending_equity=p10[-1], conditional_median_ending_equity=median[-1],
                conditional_p90_ending_equity=p90[-1], mean_accepted_trades=float(model["accepted"].sum(axis=1).mean()),
                mean_cash_rejected_trades=float(model["rejected"].sum(axis=1).mean()),
                simulated_paths_with_cash_rejections=int((model["rejected"].sum(axis=1)>0).sum())))
            month_rows.extend(monthly_rows(model, spec["id"], policy))
            for i in range(HORIZON+1):
                projection_rows.append(dict(scenario=spec["id"], sizing=policy, additional_session=i,
                    simulated_mean_account_equity=avg[i], simulated_mean_cumulative_profit=avg[i]-INITIAL,
                    total_return_on_original_capital_pct=(avg[i]/INITIAL-1)*100,
                    p10_account_equity=p10[i], median_account_equity=median[i], p90_account_equity=p90[i],
                    simulated_mean_daily_profit=0 if i==0 else model["pnl"][:,i-1].mean()))
    daily.to_csv(output / "historical_daily_returns.csv", index=False)
    curves = pd.DataFrame(projection_rows)
    curves.loc[curves.sizing.eq("fixed")].to_csv(output / "one_year_scenario_curves.csv", index=False)
    curves.to_csv(output / "sizing_comparison_daily_curves.csv", index=False)
    summary = pd.DataFrame(summaries)
    summary.loc[summary.sizing.eq("fixed")].to_csv(output / "one_year_scenario_summary.csv", index=False)
    summary.to_csv(output / "sizing_comparison_summary.csv", index=False)
    monthly = pd.DataFrame(month_rows)
    monthly.to_csv(output / "monthly_scenario_projections.csv", index=False)
    historic_monthly = historical_months(daily, trades)
    historic_monthly.to_csv(output / "historical_monthly_results.csv", index=False)
    for (scenario, policy), group in monthly.groupby(["scenario", "sizing"]):
        annual = summary.loc[summary.scenario.eq(scenario) & summary.sizing.eq(policy)].iloc[0]
        np.testing.assert_allclose(group.mean_monthly_net_profit.sum(), annual.simulated_mean_additional_profit, atol=1e-6)
    charts(output, daily, results, historic_monthly, monthly)
    sizing_chart(output, daily, results, adaptive, monthly)
    config = dict(starting_capital=INITIAL, capital_per_trade=MARGIN, notional_per_trade=NOTIONAL,
        historical_sessions=31, historical_selected=73, historical_executed=66,
        historical_net_profit=float(daily.net_profit_rupees.sum()), projection_opening_equity=start,
        horizon_assumed_sessions=HORIZON, paths=args.paths, random_seed=SEED, circular_block_sessions=BLOCK,
        scenarios=SCENARIOS, sizing="Compare fixed Rs3 lakh versus monthly equity-based resizing; both retain 5x exposure and maximum five concurrent trades",
        monthly_sizing=dict(sessions_per_model_month=MONTH_SESSIONS, assumed_months=12,
            month_opening_margin_budget_fraction=DEPLOYMENT_FRACTION, max_monthly_increase_fraction=MONTHLY_INCREASE_CAP,
            round_down_margin_rupees=MARGIN_INCREMENT, first_month_margin=float(monthly_margin(start, MARGIN)),
            formula="floor_to_10000(min(0.80 * path_month_opening_equity / 5, previous_month_margin * 1.10))",
            decreases="Not capped; affordability is checked again at every entry. Zero allocation stops new trades for the remaining simulation.",
            reinvestment="Retained realized net P&L only; no deposits or withdrawals; historical sizes unchanged.",
            buffer="20% is a month-opening margin buffer, not a continuously enforced intraday cash floor.",
            calibration="Illustrative fixed assumptions, not optimized or proven optimal."),
        cash="Reserve margin and debit half modeled round-trip cost at entry; release margin and debit remaining cost at exit; reject unaffordable trades",
        observed_cash_replay_matches=True, sources=sources,
        limitations=["Previously reviewed development history only; no untouched test.",
            "252 sessions is an approximate modeled year, not dated exchange trading sessions.",
            "Cash accounting uses realized exits; unrealized MTM and broker-specific margin changes are not simulated.",
            "Known daily opportunity/payoff templates are resampled; hypothetical payoff haircuts do not recompute OHLC strategy signals or target/stop hit times.",
            "Scenario paths reuse equity-price outcomes with futures OI inputs; they are not forecasts of executable futures contracts.",
            "Notional and costs scale linearly with allocation; whole-share rounding, larger-order market impact and contract-note charges remain unmodeled.",
            "Percentile bands are conditional simulation dispersion, not a calibrated confidence interval; new regimes may lie outside them."],
        projection_context_source="https://www.investor.gov/introduction-investing/general-resources/news-alerts/alerts-bulletins/investor-bulletins-47")
    (output / "projection_methodology.json").write_text(json.dumps(config, indent=2), encoding="utf-8")
    columns=["label", "sizing", "cost_bps_total", "simulated_mean_additional_profit",
             "simulated_mean_ending_equity", "future_return_on_projection_opening_equity_pct", "mean_cash_rejected_trades"]
    comparison = []
    for spec in SCENARIOS:
        pair = summary.loc[summary.scenario.eq(spec["id"])].set_index("sizing")
        fixed, resized = pair.loc["fixed"], pair.loc["monthly"]
        comparison.append({"Gross wins retained": f"{spec['fraction']:.0%}",
            "Fixed: future P&L (lakh)": fixed.simulated_mean_additional_profit/100000,
            "Monthly: future P&L (lakh)": resized.simulated_mean_additional_profit/100000,
            "Change (lakh)": (resized.simulated_mean_additional_profit-fixed.simulated_mean_additional_profit)/100000,
            "Monthly: ending equity (lakh)": resized.simulated_mean_ending_equity/100000,
            "Monthly: future account return %": resized.future_return_on_projection_opening_equity_pct})
    example = monthly.loc[monthly.scenario.eq("retain_50") & monthly.sizing.eq("monthly")].copy().reset_index(drop=True)
    fixed_example = monthly.loc[monthly.scenario.eq("retain_50") & monthly.sizing.eq("fixed")].reset_index(drop=True)
    example["fixed_mean_monthly_net_profit"] = fixed_example.mean_monthly_net_profit
    example.to_csv(output / "monthly_sizing_50pct_example.csv", index=False)
    schedule = pd.DataFrame({"Model month": example.model_month,
        "Median margin/trade (lakh)": example.median_margin_per_trade/100000,
        "Median 5x exposure (lakh)": example.median_margin_per_trade*5/100000,
        "Fixed mean P&L (lakh)": example.fixed_mean_monthly_net_profit/100000,
        "Monthly rule mean P&L (lakh)": example.mean_monthly_net_profit/100000,
        "Monthly rule mean ending equity (lakh)": example.mean_ending_equity/100000})
    history_table = historic_monthly[["month", "observed_sessions", "executed", "wins", "losses", "win_rate_pct", "profit_factor", "net_profit_rupees", "account_month_return_pct"]]
    tail = summary.loc[summary.scenario.eq("retain_50") & summary.sizing.eq("monthly")].iloc[0]
    report = "\n\n".join([
        "# V13–V10–G: daily and monthly results, with one-year sizing scenarios",
        "![Historical and projected results](V13_V10_G_BACKTEST_AND_ONE_YEAR.png)",
        f"Historical: ₹{daily.net_profit_rupees.sum():,.2f} net from 66 fills across 31 observed sessions; account grows from ₹15,00,000 to ₹{start:,.2f}. Both future sizing policies start at this retained ending equity. Historical trades retain ₹3 lakh margin each. No new deposit or withdrawal is assumed; monthly resizing uses retained realized profits and losses.",
        "## Historical monthly results",
        history_table.to_markdown(index=False, floatfmt=".2f"),
        "July covers July 29–31, September covers September 1–11, and August contains 19 available sessions; August 24–25 are absent. These are observed-sample results, not claims of complete calendar-month coverage. Monthly account return = that month's net P&L / its opening equity. July has no losses, so its observed profit factor is infinite, based on just four trades.",
        "## Monthly allocation rule to test",
        "At each model month's start: **margin per trade = round down to ₹10,000 [min(16% × actual month-opening equity, 110% × previous month's allocation)].** Keep 5× modeled exposure and a maximum of five concurrent positions. This budgets at most 80% of opening equity to five trade margins, leaving at least 20% before fees at rebalance. The buffer may erode during the month; entry affordability is still checked trade by trade.",
        f"First model month: min(0.16 × ₹{start:,.2f}, 1.10 × ₹3,00,000) = ₹325,860.09 before rounding, giving **₹3,20,000 margin per trade and ₹16,00,000 exposure**. Five such margins reserve ₹16 lakh from ₹20.37 lakh. The rule permits increases of at most 10%; it does not force an increase. If the equity budget falls, cuts are not limited to 10%. A zero allocation stops new trades for the remaining simulation; no new capital is injected.",
        "The 20% buffer, 10% increase cap and ₹10,000 step are explicit illustrative choices, not optimized numbers or evidence of an optimal live policy. Position sizing also needs to respect the actual stop distance and account loss budget; see [CME's position-sizing guidance](https://www.cmegroup.com/education/courses/trade-and-risk-management/proper-position-size). The current strategy's selection, SL, targets and live settings are unchanged.",
        "![Monthly sizing and projections](V13_V10_G_MONTHLY_SIZING_COMPARISON.png)",
        "## Effect on the additional one-year result",
        pd.DataFrame(comparison).to_markdown(index=False, floatfmt=".2f"),
        "All monetary columns in this comparison are ₹ lakh and all results are simulation means. The charts' cumulative P&L includes historical profit; this table's future P&L covers only the next 252 assumed sessions. Future return uses ₹20.37 lakh projection-opening equity; total historical-plus-future return uses the original ₹15 lakh. Leverage is already included in trade P&L and is not applied a second time.",
        "## Monthwise example: 50% of positive gross P&L retained",
        schedule.to_markdown(index=False, floatfmt=".2f"),
        "M1–M12 each represent 21 assumed trading sessions, not named calendar months or verified exchange dates. Allocation values are cross-path medians, while P&L and ending balances are cross-path means. This table is not a single executable path or a predetermined investment schedule: calculate each future allocation from your own realized month-opening equity. The 50% case is shown for illustration, not designated as the most likely outcome.",
        f"For monthly resizing in that case, the conditional ending-equity P10–P90 interval is ₹{tail.conditional_p10_ending_equity/100000:.2f}–₹{tail.conditional_p90_ending_equity/100000:.2f} lakh. This measures variability within the chosen simulation assumptions, not a calibrated probability range for the market.",
        "## Calculation and verification",
        "The reference preserves historical gross payoffs and original 5 bps total costs. Stress cases retain 75%, 50% or 20% of positive gross P&L, preserve losing gross P&L, and use 10 bps total costs on the 5× position exposure. Fractions refer to payoff size, not win rate. At ₹3 lakh margin, 10 bps costs ₹1,500 per accepted trade; at ₹3.20 lakh it costs ₹1,600. Gross gains, gross losses and both fee halves scale with the actual path's allocation.",
        f"Each of {args.paths:,} paths draws circular five-session blocks from all 31 observed sessions with replacement, retaining seven zero-trade days and each sampled day's trade chronology. Both sizing policies and all scenarios use identical sampled days. Each monthly allocation uses that path's realized equity at the start of sessions 1, 22, ..., 232 and stays fixed within its model month. No future equity or ensemble average is used to select trade size. Fees are charged half on entry and half on exit; available cash and the five-position cap are enforced. Margin returns on exits; rejected trades produce no fee or P&L.",
        "The observed sequence was replayed with this stricter cash accounting: all 66 historical executions remain affordable and each day's P&L reconciles to the retained result. Projection percentile bands reflect only this resampling model and the specified payoff assumptions. The band on the first graph belongs to the 50% payoff case, with no market-probability claim.",
        "Daily account returns use opening account equity. Daily deployed-margin returns use the peak overlapping margin observed that day. They answer different questions; deployed-margin percentages are not summed or compounded into total account return.",
        "All projected monthly net profits reconcile to their corresponding annual P&L. The first graph retains the four fixed-sizing projections and adds observed monthly results and projected monthly P&L. The second graph compares fixed sizing against monthly resizing for all four cases, with each case's own conditional band.",
        "Detailed data: [historical monthly results](historical_monthly_results.csv), [all monthly projections and allocations](monthly_scenario_projections.csv), [50% monthly example](monthly_sizing_50pct_example.csv), [annual sizing comparison](sizing_comparison_summary.csv), [daily sizing curves](sizing_comparison_daily_curves.csv), [methodology](projection_methodology.json).",
        "## Limits", "\n".join(f"- {line}" for line in config["limitations"]),
        "Backtested performance and projected outcomes are hypothetical. [Investor.gov guidance on performance claims](https://www.investor.gov/introduction-investing/general-resources/news-alerts/alerts-bulletins/investor-bulletins-47).",
    ])+"\n"
    (output / "V13_V10_G_ONE_YEAR_SCENARIOS.md").write_text(report, encoding="utf-8")
    (output / "manifest.json").write_text(json.dumps(dict(complete=True, code_sha256=digest(__file__),
        source_hashes=sources, seed=SEED, artifacts={p.name:digest(p) for p in output.iterdir() if p.is_file() and p.name!="manifest.json"}), indent=2), encoding="utf-8")
    print(summary[columns].to_string(index=False))
    print(f"Graph: {output / 'V13_V10_G_BACKTEST_AND_ONE_YEAR.png'}")


if __name__ == "__main__":
    main()
