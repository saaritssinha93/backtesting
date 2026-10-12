"""Attach original deterministic G-3 cash monitoring to a completed V14 ledger."""
from __future__ import annotations

import argparse
import html
import json
from pathlib import Path

import pandas as pd

from . import data, pullbacks
from .features import clean_minutes


def attach_monitoring(root: Path, output: Path) -> dict:
    ledger = pd.read_csv(output / "trades.csv")
    annotated, observations = [], []
    for symbol, group in ledger.groupby("tradingsymbol", sort=False):
        minutes = clean_minutes(data.load_symbol_1m(symbol, root=root))
        trades, observed = pullbacks.annotate_trades(group, {symbol: minutes})
        annotated.append(trades)
        if not observed.empty:
            observations.append(observed)
    trades = pd.concat(annotated).sort_index() if annotated else ledger
    observed = pd.concat(observations, ignore_index=True) if observations else pd.DataFrame(
        columns=["is_grid", "emitted_warning", "horizon_minutes", "warning", "event", "outcome_available"])
    summary = pullbacks.summarize(observed)
    trades.to_csv(output / "trades_with_pullbacks.csv", index=False)
    observed.to_parquet(output / "pullback_observations.parquet", index=False)
    summary.to_csv(output / "pullback_summary.csv", index=False)
    status = dict(monitor_version=pullbacks.MONITOR_VERSION, trades=len(ledger),
                  observation_rows=len(observed), alters_trades=False,
                  limitation="Original deterministic G3 rules; learned selective V2 classifier is not transferred or calibrated for non-F&O stocks")
    (output / "pullback_summary.json").write_text(json.dumps(status, indent=2), encoding="utf-8")
    selected = [c for c in ("day", "tradingsymbol", "side", "setup_id", "entry_ts", "exit_ts", "exit_reason",
        "portfolio_net_profit_rupees", "monitor_status", "monitor_unavailable_reason", "monitoring_minutes",
        "fast_warnings", "slow_warnings", "fast_pullback_windows", "slow_pullback_windows") if c in trades]
    body = f'''<!doctype html><html lang="en"><meta charset="utf-8"><title>V14 pullback observations</title>
<style>body{{font:15px/1.5 system-ui;background:#0b1220;color:#e2e8f0;margin:36px}}a{{color:#5eead4}}table{{border-collapse:collapse}}td,th{{padding:8px;border-bottom:1px solid #334155}}.scroll{{overflow:auto}}</style>
<h1>V14 pullback observations</h1><p>{html.escape(status['limitation'])}. These observations never change entries, exits or P&amp;L. Future outcome columns are labels, not trading signals.</p>
<p><a href="report.html">Return to backtest</a> · <a href="trades_with_pullbacks.csv">Annotated trades</a> · <a href="pullback_summary.csv">Monitor summary</a></p>
<h2>Resolved grid windows and separate warning episodes</h2><div class="scroll">{summary.to_html(index=False,border=0)}</div>
<h2>Trade observations</h2><div class="scroll">{trades[selected].to_html(index=False,border=0)}</div></html>'''
    (output / "pullback_report.html").write_text(body, encoding="utf-8")
    return status


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=data.DEFAULT_ROOT)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(attach_monitoring(args.root, args.output), indent=2))


if __name__ == "__main__":
    main()
