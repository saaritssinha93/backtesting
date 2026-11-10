"""Reproduce the backtest-only G7 V5 selection research from the V4 lock.

Runs two parity checks, a completed-bar Nifty/stock market ledger, and three
full-portfolio sweeps. The frozen V4 source currently ends on 2026-10-27.
"""
from __future__ import annotations

import argparse
from datetime import datetime
import hashlib
import json
from pathlib import Path
import subprocess
import sys
from zoneinfo import ZoneInfo

from research.g7_v5_report import run as report_run


ROOT = Path(__file__).resolve().parent
CONFIG = ROOT / "configs/strategies/v13_v10_g_7_v5_research_backtest.json"


def checked_step(module: str, *args: str) -> None:
    command = [sys.executable, "-W", "ignore::FutureWarning", "-m", module, *args]
    print("RUN", module, *args, flush=True)
    subprocess.run(command, cwd=ROOT, check=True)


def run(output: Path) -> dict:
    config = json.loads(CONFIG.read_text(encoding="utf-8"))
    if config["scope"] != "BACKTEST_ONLY" or config["execution_authority"] is not False:
        raise ValueError("G7 V5 research must have no execution authority")
    lock = ROOT / config["source_lock"]["path"]
    if hashlib.sha256(lock.read_bytes()).hexdigest() != config["source_lock"]["sha256"]:
        raise ValueError("The frozen V4 lock changed; a comparable V5 replay requires review")
    if output.exists():
        raise FileExistsError(output)
    output.mkdir(parents=True)
    frozen = output / "parity_intraday"
    opening = output / "parity_opening"
    market = output / "market"
    checked_step("research.g7_v5_candidate_audit", "--output", str(frozen))
    checked_step("research.g7_v5_candidate_audit", "--output", str(opening), "--setups",
                 *config["experiments"]["opening_slots"])
    checked_step("research.g7_v5_market_regime", "--calendar", str(frozen / "parity.json"),
                 "--output", str(market))
    trials = (("intraday", frozen / "parity.json", None),
              ("opening_025", opening / "parity.json", 0.25),
              ("opening_050", opening / "parity.json", 0.5))
    for name, parity, threshold in trials:
        selection = output / (name + "_selection")
        portfolio = output / (name + "_portfolio")
        command = ["--parity", str(parity), "--market", str(market / "market_regime.csv"),
                   "--output", str(selection)]
        if threshold is not None:
            command.extend(("--experiment", "opening", "--opening-day-gain", str(threshold)))
        checked_step("research.g7_v5_selection_sweep", *command)
        checked_step("research.g7_v5_portfolio_sweep", "--selection",
                     str(selection / "selected_opportunities.pkl"), "--output", str(portfolio))
    result = report_run(output, experiments={name: name + "_portfolio" for name, _, _ in trials})
    result["config"] = str(CONFIG)
    result["source_lock_sha256"] = config["source_lock"]["sha256"]
    (output / "run_summary.json").write_text(json.dumps(result, indent=2), encoding="utf-8")
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    default = ROOT / "outputs" / "g7_v5_runs" / datetime.now(ZoneInfo("Asia/Kolkata")).strftime("%Y%m%dT%H%M%S")
    parser.add_argument("--output", type=Path, default=default)
    args = parser.parse_args()
    print(json.dumps(run(args.output.resolve()), indent=2), flush=True)


if __name__ == "__main__":
    main()
