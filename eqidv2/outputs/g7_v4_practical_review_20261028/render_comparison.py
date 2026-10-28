"""Plot sealed research CSVs without changing the source run or active config."""
from pathlib import Path
import hashlib
import json

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter
import numpy as np
import pandas as pd


SOURCE = Path("C:/TradingData/eqidv2/backtesting_result_v13_v10_g_7/v4_research/practical_research/runs/2026-10-27/20261027T185748273567_five_fixed_tests")
OUT = Path(__file__).resolve().parent
IMAGE = OUT / "g7_v4_practical_comparison.png"
PROOF = OUT / "input_manifest.json"


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    if IMAGE.exists() or PROOF.exists():
        raise FileExistsError("Review output already exists; choose a new filename before rerendering")
    summary = json.loads((SOURCE / "summary.json").read_text(encoding="utf-8"))
    manifest_hash = digest(SOURCE / "manifest.json")
    assert summary["selected_candidate"] is None
    assert not summary["default_changed"]
    totals = pd.read_csv(SOURCE / "all_period_totals.csv").set_index("policy")
    daily = pd.read_csv(SOURCE / "october_daywise_all.csv")
    names = ["published_g7_v4", "execution_control", "cmf20_confirm", "vwap_slope_failure",
             "close75_interaction", "gap_cancel10", "loss_lock2"]
    labels = ["Saved G7 V4\nfractional sizing", "Execution control\ninteger shares; reserve at minute open",
              "CMF20 confirmation", "VWAP slope + failed breakout", "Close location at least 75%",
              "Cancel pending gaps over 10 bps", "Stop entries after two realized losses"]
    amounts = totals.loc[names]
    plt.rcParams.update({"font.family": "DejaVu Sans", "font.size": 11,
                         "axes.spines.top": False, "axes.spines.right": False})
    fig, (ax, line) = plt.subplots(1, 2, figsize=(19, 10), gridspec_kw={"width_ratios": [1.1, 1]})
    fig.subplots_adjust(left=.215, right=.975, top=.79, bottom=.25, wspace=.3)
    fig.suptitle("G7 V4 practical improvements: NONE qualified", fontsize=23, fontweight="bold", y=.965)
    fig.text(.5, .913, "59 available sessions | 29 July–27 October 2026 | 5-bps modeled costs",
             ha="center", fontsize=14)
    fig.text(.5, .874, "Every experiment uses the same conservative execution control. Saved G7 V4 is shown separately.",
             ha="center", fontsize=12, color="#425166")
    y = np.arange(len(names))
    net = amounts.net_pnl_rs.to_numpy() / 1000
    losing = amounts.negative_trade_sum_abs_rs.to_numpy() / 1000
    ax.barh(y - .18, net, height=.31, color="#245f93", label="Net profit")
    ax.barh(y + .18, losing, height=.31, color="#c46b36", label="Amount lost on losing trades")
    for offset, values in ((-.18, net), (.18, losing)):
        for pos, value in zip(y, values):
            ax.text(value + 8, pos + offset, f"{value:,.1f}", va="center", fontsize=10)
    ax.set_yticks(y, labels)
    ax.invert_yaxis()
    ax.set_xlim(0, max(net) * 1.15)
    ax.set_title("All sessions: profit and losing-trade amount", fontsize=14, pad=18, loc="left")
    ax.set_xlabel("INR thousands")
    ax.xaxis.set_major_formatter(FuncFormatter(lambda x, pos: f"{x:,.0f}"))
    ax.grid(axis="x", alpha=.18)
    ax.set_axisbelow(True)
    ax.legend(loc="upper left", bbox_to_anchor=(0, -.14), frameon=False, fontsize=10, ncol=2)
    ax.tick_params(axis="y", length=0, pad=10)
    choices = [("execution_control", "Execution control", "#172c46"),
               ("loss_lock2", "Two-loss entry lock", "#b34b2c"),
               ("close75_interaction", "Close location at least 75%", "#417e63")]
    x = np.arange(len(daily) + 1)
    for policy, label, color in choices:
        curve = np.r_[0., daily[policy].cumsum().to_numpy() / 1000]
        line.plot(x, curve, label=label, color=color, linewidth=2.4, marker="o", markersize=4)
        line.annotate(f"{curve[-1]:,.1f}", xy=(x[-1], curve[-1]), xytext=(8, 0),
                      textcoords="offset points", color=color, fontsize=10, va="center")
    line.set_title("October cumulative net P&L\nDiagnostics only; neither experiment selected", fontsize=14, pad=18, loc="left")
    line.set_xticks(x, ["Start", *[pd.Timestamp(day).strftime("%d Oct") for day in daily.day]],
                    rotation=55, ha="right", fontsize=9)
    line.set_xlim(-.2, len(daily) + 1.25)
    line.set_ylim(bottom=-2)
    line.set_ylabel("INR thousands")
    line.grid(alpha=.18)
    line.set_axisbelow(True)
    line.yaxis.set_major_formatter(FuncFormatter(lambda value, pos: f"{value:,.0f}"))
    line.legend(loc="upper left", bbox_to_anchor=(0, -.2), frameon=False, fontsize=10)
    fig.text(.06, .108,
             "Loss reductions can remove profitable opportunities too: close-location filtering reduced total profit substantially.\n"
             "Control reserves capital and the symbol at entry-minute open, then rounds cash quantity down to whole shares.",
             fontsize=11, color="#34465d", va="top")
    fig.text(.06, .052,
             "Retrospective evidence: dates were examined repeatedly; stock selection remains conditional on after-close coverage.\n"
             "No independent holdout. October includes 16 available sessions; 1 October is missing. Active G7 V4 settings were not changed.",
             fontsize=10.5, color="#596675", va="top")
    fig.savefig(IMAGE, dpi=160, facecolor="white")
    plt.close(fig)
    if digest(SOURCE / "manifest.json") != manifest_hash:
        raise ValueError("Source manifest changed during plotting")
    proof = dict(source_run=str(SOURCE), source_manifest_sha256=manifest_hash,
                 inputs={name: digest(SOURCE / name) for name in
                         ("all_period_totals.csv", "october_daywise_all.csv", "summary.json")},
                 chart_sha256=digest(IMAGE), selected_candidate=None,
                 costs_bps=5, no_active_changes=True, source_run_modified=False,
                 scope="Standalone static research review; all experiments disclosed; diagnostic curves are not selected rules")
    with PROOF.open("x", encoding="utf-8") as handle:
        json.dump(proof, handle, indent=2)
    print(IMAGE)
    print(PROOF)


if __name__ == "__main__":
    main()
