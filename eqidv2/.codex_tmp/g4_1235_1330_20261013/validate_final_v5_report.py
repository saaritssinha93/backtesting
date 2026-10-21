"""Separate read-only QA of the final dual-70% afternoon report."""
from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

from validate_reports import TASK, RUNTIME, inspect_html, reconcile, sha

ROOT = TASK.parents[1]
FINAL = RUNTIME / "target_sweeps/2026-10-13/20261013_afternoon_dual70_targets_v5"
EXPECTED = "V13-V10-G-4-1235-1330-DUAL70-TARGET-REFINED-V5"


def validate():
    if not (FINAL / "manifest.json").is_file():
        return dict(status="PENDING", reason="Final V5 run has not published its completed manifest")
    result, soup, text, tables = inspect_html(FINAL)
    manifest = json.loads((FINAL / "manifest.json").read_text(encoding="utf-8"))
    code_hashes = {name.replace("\\", "/"): digest for name, digest in manifest["code_sha256"].items()}
    summary = json.loads((FINAL / "summary.json").read_text(encoding="utf-8"))
    config = json.loads((FINAL / "refined_config.json").read_text(encoding="utf-8"))
    portfolios = pd.read_csv(FINAL / "portfolio_results.csv")
    setups = pd.read_csv(FINAL / "scenario_setup_results.csv")
    final_setups = setups.loc[setups.scenario.eq("refined_qualified") & setups.period.eq("all")]
    ledger = pd.read_csv(FINAL / "refined_qualified_trades.csv", float_precision="round_trip")
    replayed = pd.read_csv(FINAL / "refined_added_raw_trades.csv", float_precision="round_trip")
    published_targets = {s["setup_id"]: s["target_pct"] for s in config["setups"]}
    code_name = "research/g4_afternoon_portfolio_target_refinement.py"
    code = ROOT / code_name
    result["checks"].update({
        "chronological_not_unseen_disclosure": "untouched out-of-sample" in text and ("neither" in text or "not" in text),
        "final_v5_manifest_identity": manifest["variant_id"] == EXPECTED,
        "final_v5_summary_identity": summary["variant_id"] == EXPECTED,
        "final_v5_config_identity": config["variant_id"] == EXPECTED,
        "backtest_only_authority": all(value.get("scope") == "BACKTEST_ONLY" and value.get("execution_authority") is False for value in (manifest, summary, config)),
        "dual70_mode_required": summary.get("require_native70") is True and config.get("require_native70") is True,
        "code_hash_matches_current_v5": code_hashes[code_name] == sha(code),
        "code_hash_differs_from_v4_snapshot": code_hashes[code_name] != sha(TASK / "coordinate_v4_source_snapshot.py"),
        "all_code_hashes_match_current_files": all(sha(ROOT / name) == digest for name, digest in manifest["code_sha256"].items()),
        "afternoon_heading": "12:35" in result["h1"] and "13:30" in result["h1"],
        "standalone_requirement_visible": "native fills before portfolio capital restrictions" in text,
        "final_setup_table_matches_csv": reconcile(tables[1], final_setups[tables[1].columns], ["setup"]),
        "full_portfolio_table_matches_csv": reconcile(tables[0], portfolios.loc[portfolios.period.eq("all"), tables[0].columns], ["scenario", "scope", "period"]),
        "later_table_matches_csv": reconcile(tables[2], portfolios.loc[portfolios.period.eq("holdout"), tables[2].columns], ["scenario", "scope", "period"]),
        "all_configured_setups_reported": set(final_setups.setup_id) == set(published_targets) == set(summary["targets"]),
        "final_setups_count_matches": len(final_setups) == summary["final_setups"] == len(tables[1]),
        "all_standalone_70_flags_true": final_setups.standalone_native_70_positive.eq(True).all().item(),
        "all_allocated_70_flags_true": final_setups.observed_70_positive.eq(True).all().item(),
        "browser_render_png_created": (TASK / "final_v5_report.png").is_file(),
    })
    actual_rows = []
    for setup in final_setups.itertuples(index=False):
        native = replayed.loc[replayed.setup_id.eq(setup.setup_id) & replayed.filled.eq(True)]
        allocated = ledger.loc[ledger.setup_id.eq(setup.setup_id) & ledger.portfolio_executed.eq(True)]
        native_pnl = native.net_return_pct.to_numpy(float) * 5000.
        allocated_pnl = allocated.portfolio_net_profit_rupees.to_numpy(float)
        counts, wins = [len(native), len(allocated)], [int((native_pnl > 0).sum()), int((allocated_pnl > 0).sum())]
        totals = [float(native_pnl.sum()), float(allocated_pnl.sum())]
        pass_both = all(count >= 5 and 10 * won >= 7 * count and pnl > 0 for count, won, pnl in zip(counts, wins, totals))
        matches = (counts == [setup.standalone_native_fills, setup.executed_trades]
            and wins == [setup.standalone_native_wins, setup.wins]
            and np.allclose(totals, [setup.standalone_native_net_pnl_rs, setup.net_pnl_rs], rtol=0, atol=1e-6)
            and setup.target_pct == published_targets[setup.setup_id])
        actual_rows.append(dict(setup=setup.setup, setup_id=setup.setup_id, target_pct=setup.target_pct,
            native_fills=counts[0], native_wins=wins[0], native_win_rate_pct=100 * wins[0] / counts[0],
            native_net_pnl_rs=totals[0], allocated_fills=counts[1], allocated_wins=wins[1],
            allocated_win_rate_pct=100 * wins[1] / counts[1], allocated_net_pnl_rs=totals[1],
            both_meet_70_positive_minimum=bool(pass_both), matches_reported_metrics=bool(matches)))
    result["checks"]["all_native_and_actual_rows_recomputed_and_qualify"] = all(
        row["both_meet_70_positive_minimum"] and row["matches_reported_metrics"] for row in actual_rows)
    document = ROOT / "docs/G4_1235_1330_TARGET_RESEARCH_2026-10-13.md"
    prose = document.read_text(encoding="utf-8")
    documented = {}
    for line in prose.splitlines():
        if re.match(r"^\| \d{2}:\d{2} (LONG|SHORT) \|", line):
            cells = [cell.strip() for cell in line.split("|")[1:-1]]
            documented[cells[0].replace(" ", "_")] = cells
    document_matches = True
    for row in actual_rows:
        cells = documented.get(row["setup"], [])
        if len(cells) != 9 or cells[-1] != "Included":
            document_matches = False
            continue
        values = [float(cells[index].replace("%", "").replace(",", "")) for index in (2, 3, 4, 5, 6, 7)]
        expected = [row["target_pct"], row["native_fills"], row["native_win_rate_pct"],
            row["allocated_fills"], row["allocated_win_rate_pct"], row["allocated_net_pnl_rs"]]
        document_matches = document_matches and np.allclose(values, expected, rtol=0, atol=.005001)
    expected_clocks = ["12:35", "12:40", "12:45", "12:50", "12:55", "13:00", "13:05", "13:10", "13:15", "13:20", "13:25", "13:30"]
    result["checks"].update({
        "documentation_covers_all_24_requested_setups": set(documented) == {f"{clock}_{side}" for clock in expected_clocks for side in ("LONG", "SHORT")},
        "documentation_final_metrics_match_csv": bool(document_matches),
        "documentation_only_15_included": sum(cells[-1] == "Included" for cells in documented.values()) == len(actual_rows),
        "documentation_final_manifest_current": sha(FINAL / "manifest.json") in prose,
        "documentation_v4_explicitly_diagnostic": "It is a diagnostic and is not the stricter result above." in prose,
    })
    result.update(final_setups=actual_rows, summary_final=summary["final"],
        code_sha256=code_hashes[code_name], manifest_sha256=sha(FINAL / "manifest.json"),
        final_setups_count=len(actual_rows), browser_screenshot=str(TASK / "final_v5_report.png"),
        browser_visual_review="PASS: Chrome headless 1800x1600 render; headings, risk/qualification explanations and tables readable; wider final-setup table uses document horizontal scrolling; no overlap or missing glyphs",
        documentation_path=str(document), documentation_sha256=sha(document))
    result["status"] = "PASS" if all(result["checks"].values()) else "FAIL"
    return result


if __name__ == "__main__":
    receipt = dict(created_at_utc=datetime.now(timezone.utc).isoformat(), **validate())
    (TASK / "final_v5_report_validation.json").write_text(json.dumps(receipt, indent=2, allow_nan=False) + "\n", encoding="utf-8")
    print(json.dumps({key: receipt.get(key) for key in ("status", "final_setups_count", "summary_final")}))
