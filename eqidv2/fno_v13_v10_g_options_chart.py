"""Static, observed-history charts for the V13-v10-G option replay artifacts."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter, MaxNLocator
import numpy as np
import pandas as pd


BASELINE_COLOR = "#2458A6"
CHALLENGER_COLOR = "#C67818"
INK = "#172B46"
MUTED = "#526176"


def _lakh(value: float) -> str:
    return f"{'−' if value < 0 else ''}₹{abs(value) / 100000:.2f}L"


def render(output: Path) -> Path:
    """Read completed parent artifacts and save a PNG; never recompute policy.

    Primary options_daily.csv is the predeclared baseline. The separate
    challenger trade ledger contains the profile selected on the training split.
    Only admitted CLOSED challenger trades contribute realized daily P&L.
    """
    output = Path(output)
    daily = pd.read_csv(output / "options_daily.csv").sort_values("day")
    challenger = pd.read_csv(output / "challenger_options_trades.csv")
    spec = json.loads((output / "research_spec.json").read_text(encoding="utf-8"))
    cutoff = str(spec["train_cutoff"])
    required = {"day", "option_data_present", "net_pnl", "closed", "unresolved"}
    if not required.issubset(daily.columns):
        raise ValueError(f"Primary daily ledger missing columns: {sorted(required - set(daily.columns))}")
    available = daily.option_data_present.astype(str).str.lower().eq("true")
    if not available.any():
        raise ValueError("No observed option-data sessions to chart")
    first_day = daily.loc[available, "day"].min()
    last_day = daily.loc[available, "day"].max()
    daily = daily.loc[daily.day.between(first_day, last_day)].copy()
    available = daily.option_data_present.astype(str).str.lower().eq("true").to_numpy()
    closed = challenger.loc[
        challenger.portfolio_status.eq("ADMITTED") & challenger.status.eq("CLOSED")
    ].copy()
    unresolved_challenger = challenger.loc[
        challenger.portfolio_status.eq("ADMITTED") & challenger.status.eq("UNRESOLVED")
    ]
    baseline_pnl = pd.to_numeric(daily.net_pnl, errors="raise").to_numpy(dtype=float)
    challenger_pnl = closed.groupby("day").net_pnl.sum().reindex(daily.day, fill_value=0).to_numpy(dtype=float)
    if not np.isfinite(baseline_pnl).all() or not np.isfinite(challenger_pnl).all():
        raise ValueError("Chart realized net P&L must be finite")
    x = np.arange(len(daily))
    train = daily.day.le(cutoff).to_numpy()
    divider = int(train.sum()) - .5
    primary_count = int(pd.to_numeric(daily.closed).sum())
    challenger_count = len(closed)
    primary_unresolved = int(pd.to_numeric(daily.unresolved).sum())
    baseline_stop, baseline_target = spec.get("baseline", [.175, .225])
    first = pd.Timestamp(first_day)
    last = pd.Timestamp(last_day)
    range_label = f"{first:%d %b}–{last:%d %b %Y}"

    with plt.rc_context({
        "font.family": "DejaVu Sans", "font.size": 11,
        "axes.edgecolor": "#D7DEE7", "axes.labelcolor": MUTED,
        "xtick.color": MUTED, "ytick.color": MUTED,
        "text.color": INK, "axes.titleweight": "bold",
    }):
        fig = plt.figure(figsize=(14, 9.6), facecolor="#F7F9FC")
        grid = fig.add_gridspec(2, 1, height_ratios=[1.65, 1],
                               left=.085, right=.935, top=.78, bottom=.13, hspace=.49)
        cumulative = fig.add_subplot(grid[0])
        comparison = fig.add_subplot(grid[1])
        fig.text(.085, .936, "V13-v10-G  |  ATM options", fontsize=24, weight="bold", color=INK)
        fig.text(.085, .896, f"Three lots per entry · Five-minute execution · {range_label}",
                 fontsize=13, color=MUTED)
        fig.text(.085, .851,
                 f"Baseline  {_lakh(baseline_pnl.sum())}  /  {primary_count} closed trades",
                 fontsize=13, weight="bold", color=BASELINE_COLOR)
        fig.text(.53, .851,
                 f"Train-selected challenger  {_lakh(challenger_pnl.sum())}  /  {challenger_count} closed",
                 fontsize=13, weight="bold", color=CHALLENGER_COLOR)

        for axis in (cumulative, comparison):
            axis.set_facecolor("white")
            axis.spines[["top", "right"]].set_visible(False)
            axis.set_axisbelow(True)
            axis.grid(axis="y", color="#E5EAF1", linewidth=.8)
            axis.yaxis.set_major_formatter(FuncFormatter(lambda value, _: f"{value / 100000:.1f}"))
            axis.yaxis.set_major_locator(MaxNLocator(5))
            axis.set_ylabel("INR lakh", fontsize=10)
            axis.axhline(0, color="#94A3B8", linewidth=.9)

        baseline_label = f"Baseline: {baseline_stop:.1%} SL / {baseline_target:.1%} target"
        cumulative.plot(x, baseline_pnl.cumsum(), color=BASELINE_COLOR, linewidth=2.5,
                        marker="o", markersize=4, label=baseline_label, zorder=3)
        cumulative.plot(x, challenger_pnl.cumsum(), color=CHALLENGER_COLOR, linewidth=2.5,
                        marker="o", markersize=4, label="Train-selected challenger", zorder=3)
        cumulative.set_title("Cumulative realized net P&L", loc="left", fontsize=13, pad=22)
        cumulative.legend(loc="upper left", frameon=False, fontsize=10)
        cumulative.set_xlim(-.35, len(daily)-.65)
        cumulative.set_xticks(x)
        cumulative.set_xticklabels([pd.Timestamp(day).strftime("%d %b") for day in daily.day], fontsize=9)
        cumulative.margins(y=.22)
        if train.any() and (~train).any():
            cumulative.axvline(divider, color="#8996A8", linestyle=(0, (4, 4)), linewidth=1.2)
            cumulative.text(divider, 1.035, f"Training ends {pd.Timestamp(cutoff):%d %b}",
                            transform=cumulative.get_xaxis_transform(), ha="center", va="bottom",
                            fontsize=10, color=MUTED)
        for position in x[~available]:
            cumulative.axvspan(position-.42, position+.42, color="#CBD5E1", alpha=.45, zorder=0)

        baseline_periods = [baseline_pnl[train].sum(), baseline_pnl[~train].sum()]
        challenger_periods = [challenger_pnl[train].sum(), challenger_pnl[~train].sum()]
        period_x = np.arange(2)
        width = .24
        primary_bars = comparison.bar(period_x-width/2, baseline_periods, width,
                                      color=BASELINE_COLOR, zorder=3)
        challenger_bars = comparison.bar(period_x+width/2, challenger_periods, width,
                                         color=CHALLENGER_COLOR, zorder=3)
        comparison.set_title("Training versus later sessions", loc="left", fontsize=13, pad=12)
        comparison.set_xticks(period_x)
        comparison.set_xticklabels([f"Training · through {pd.Timestamp(cutoff):%d %b}",
                                    f"Later sessions · after {pd.Timestamp(cutoff):%d %b}"], fontsize=11)
        comparison.set_xlim(-.7, 1.7)
        comparison.margins(y=.3)
        for rectangle in [*primary_bars, *challenger_bars]:
            height = rectangle.get_height()
            comparison.annotate(_lakh(height),
                                (rectangle.get_x()+rectangle.get_width()/2, height),
                                xytext=(0, 5 if height >= 0 else -6), textcoords="offset points",
                                ha="center", va="bottom" if height >= 0 else "top",
                                fontsize=11, weight="bold", color=rectangle.get_facecolor())

        notes = "Net includes modeled slippage and all stated trading costs. Three exchange lots per entered trade."
        if primary_unresolved or len(unresolved_challenger):
            notes += f" Unresolved exits excluded from realized curves: {primary_unresolved} baseline / {len(unresolved_challenger)} challenger."
        fig.text(.085, .073, notes, fontsize=9.5, color=MUTED)
        caveat = "Small observed sample; the underlying G strategy already used this history. Later results are not an untouched strategy test."
        if not available.all():
            caveat += " Gray bands: no mapped option replay."
        fig.text(.085, .046, caveat, fontsize=9.5, color=MUTED)
        destination = output / "OPTIONS_RESULTS_COMPARISON.png"
        fig.savefig(destination, dpi=170, facecolor=fig.get_facecolor())
        plt.close(fig)
    return destination


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path, help="Completed options backtest output directory")
    print(render(parser.parse_args().output))
