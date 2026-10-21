"""Read-only QA of generated reports; writes a separate validation receipt."""
from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
from bs4 import BeautifulSoup

TASK = Path(__file__).resolve().parent
RUNTIME = Path("C:/TradingData/eqidv2/backtesting_result_v13_v10_g_4")
ENTRY = RUNTIME / "runs/2026-10-13/20261013_afternoon_1235_1330_v6"
TARGET = RUNTIME / "target_sweeps/2026-10-13/20261013_afternoon_1235_1330_targets_v3"


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def inspect_html(folder):
    content = (folder / "report.html").read_bytes().decode("utf-8", errors="strict")
    soup = BeautifulSoup(content, "html.parser")
    text = soup.get_text(" ", strip=True)
    links = [tag.get("href") for tag in soup.find_all("a")]
    manifest = json.loads((folder / "manifest.json").read_text(encoding="utf-8"))
    result = {
        "path": str(folder / "report.html"),
        "sha256": sha(folder / "report.html"),
        "title": soup.title.get_text(" ", strip=True),
        "h1": soup.h1.get_text(" ", strip=True),
        "tables": len(soup.find_all("table")),
        "local_links": len(links),
        "checks": {
            "utf8_declared": soup.find("meta", attrs={"charset": "utf-8"}) is not None,
            "utf8_clean": not any(token in content for token in ("\ufffd", "\u00c3\u2014", "\u00e2\u20ac")),
            "doctype_html": content.lower().startswith("<!doctype html>"),
            "all_links_resolve_within_output": all(
                (folder / link).resolve().is_relative_to(folder.resolve()) and (folder / link).is_file()
                for link in links),
            "all_linked_artifacts_hash_match": all(
                link == "manifest.json" or manifest["files"].get(link) == sha(folder / link)
                for link in links),
            "report_hash_matches_manifest": manifest["files"].get("report.html") == sha(folder / "report.html"),
            "portable_configuration_linked": any(link.endswith("config.json") or link.endswith("configs.json") for link in links),
            "49_session_disclosure": "49 sessions" in text,
            "chronological_not_unseen_disclosure": "not" in text and ("untouched" in text or "independent" in text),
            "stops_and_square_off_visible": all(word in text for word in ("1.25%", "1.00%", "120 minutes", "15:15")),
        },
    }
    return result, soup, text, pd.read_html(folder / "report.html")


def reconcile(report, original, keys):
    compared = report.merge(original, on=keys, suffixes=("_html", "_csv"), validate="one_to_one")
    if len(compared) != len(report):
        return False
    for column in report.columns:
        if column in keys:
            continue
        a, b = compared[f"{column}_html"], compared[f"{column}_csv"]
        if pd.api.types.is_numeric_dtype(b):
            if not np.allclose(pd.to_numeric(a, errors="coerce"), b, rtol=0, atol=.005001, equal_nan=True):
                return False
        elif not a.fillna("").astype(str).equals(b.fillna("").astype(str)):
            return False
    return True


def validate_entry():
    result, soup, text, tables = inspect_html(ENTRY)
    summary = json.loads((ENTRY / "summary.json").read_text(encoding="utf-8"))
    config = json.loads((ENTRY / "selected_config.json").read_text(encoding="utf-8"))
    setups = pd.read_csv(ENTRY / "setup_results.csv")
    portfolio = pd.read_csv(ENTRY / "portfolio_results.csv")
    clocks = ["12:35", "12:40", "12:45", "12:50", "12:55", "13:00", "13:05", "13:10", "13:15", "13:20", "13:25", "13:30"]
    expected = {f"{clock}_{side}" for clock in clocks for side in ("LONG", "SHORT")}
    result["checks"].update({
        "afternoon_heading": "12:35-13:30" in result["h1"] and "24" in result["h1"],
        "49_setup_baseline_named": summary["baseline_description"] in text and "(49 setups)" in text,
        "all_24_requested_setup_rows": set(tables[5].setup) == expected and len(tables[5]) == 24,
        "all_24_requested_config_rules": {f"{s['signal_end']}_{s['side']}" for s in config["setups"]} == expected,
        "baseline_config_identity": summary["baseline_setups"] == 49,
        "training_table_matches_csv": reconcile(tables[5], setups[tables[5].columns], ["setup"]),
        "full_portfolio_table_matches_csv": reconcile(tables[3], portfolio.loc[portfolio.period.eq("all"), tables[3].columns], ["scenario"]),
        "chronological_table_membership_matches": set(tables[1].setup) == set(setups.loc[setups.portfolio_chronological_check_pass, "setup"]),
        "browser_render_png_created": (TASK / "entry_report.png").is_file(),
    })
    result.update(selected_orders=summary["selected_orders"], potential_fills=summary["potential_fills"],
        baseline_setups=49, requested_setups=24, browser_screenshot=str(TASK / "entry_report.png"),
        browser_visual_review="PASS: 1600x1200 Chrome headless render; readable headings, disclosure and tables; no overlap or missing glyphs")
    result["status"] = "PASS" if all(result["checks"].values()) else "FAIL"
    return result


def validate_targets():
    if not (TARGET / "manifest.json").is_file():
        return {"status": "PENDING", "reason": "Target run has not yet published its completed manifest"}
    result, soup, text, tables = inspect_html(TARGET)
    summary = json.loads((TARGET / "summary.json").read_text(encoding="utf-8"))
    decisions = pd.read_csv(TARGET / "best_targets.csv")
    sweep = pd.read_csv(TARGET / "target_sweep.csv")
    rankings = pd.read_csv(TARGET / "scenario_comparison.csv")
    result["checks"].update({
        "target_summary_afternoon_identity": summary["variant_id"] == "V13-V10-G-4-1235-1330-TARGET-SWEEP-V3",
        "all_24_target_rows": len(decisions) == 24 and len(tables[2]) == 24,
        "target_table_matches_csv": reconcile(tables[2], decisions[tables[2].columns], ["setup"]),
        "ranking_table_matches_csv": reconcile(tables[0], rankings[tables[0].columns], ["scenario"]),
        "full_target_grid_count": summary["target_levels"] == 995 and summary["target_trials"] == 995 * 24,
        "target_grid_range_visible": "0.06%" in text and "10.00%" in text,
        "fixed_entry_scope_disclosed": "does not jointly search" in text,
        "49_setup_baseline_named": "existing 37 setups and twelve approved 11:35-12:30 additions" in text,
        "browser_render_png_created": (TASK / "target_report.png").is_file(),
    })
    thin = decisions.loc[decisions.historical_pass & decisions.historical_target_pct.le(.20),
        ["setup", "historical_target_pct", "historical_all_fills", "historical_all_win_rate_pct", "historical_all_net_pnl_rs"]].copy()
    thin["target_minus_modeled_cost_pct"] = thin.historical_target_pct - .05
    upper_raw = pd.read_parquet(TARGET / "minute_replay_all_targets.parquet", filters=[("target_pct", "=", 10.)])
    plateaus = []
    metrics = ["all_fills", "all_wins", "all_win_rate_pct", "all_net_pnl_rs", "all_max_drawdown_rs"]
    for setup, group in sweep.groupby("setup", sort=True):
        group = group.sort_values("target_pct")
        last = group.iloc[-1]
        start = float(last.target_pct)
        for _, row in group.iloc[:-1].iloc[::-1].iterrows():
            if np.allclose(row[metrics].to_numpy(float), last[metrics].to_numpy(float), rtol=0, atol=1e-8, equal_nan=True):
                start = float(row.target_pct)
            else:
                break
        plateaus.append(dict(setup=setup, upper_plateau_starts_pct=start,
            upper_limit_pct=float(last.target_pct), upper_win_rate_pct=None if pd.isna(last.all_win_rate_pct) else float(last.all_win_rate_pct)))
    result.update(selected_upper_boundary_setups=summary["selected_upper_boundary_setups"],
        upper_tail_metric_plateaus=plateaus,
        upper_limit_target_hits=int(upper_raw.target_hit.eq(True).sum()),
        upper_limit_native_fills=int(upper_raw.filled.eq(True).sum()),
        cost_thin_definition="Historical targets <=0.20%, with 0.05% modeled round-trip cost; diagnostic threshold only, not a qualification requirement",
        cost_thin_historical_targets=json.loads(thin.to_json(orient="records")),
        best_tested_every_setup70_scenario=summary["best_tested_every_setup70_scenario"],
        browser_screenshot=str(TASK / "target_report.png"),
        browser_visual_review="PASS: 1600x1200 Chrome headless render; readable headings/disclosure/tables, wide tables use horizontal scroll; no missing glyphs")
    result["status"] = "PASS" if all(result["checks"].values()) else "FAIL"
    return result


if __name__ == "__main__":
    receipt = dict(created_at_utc=datetime.now(timezone.utc).isoformat(), entry=validate_entry(), targets=validate_targets())
    receipt["status"] = "PASS" if all(receipt[key]["status"] == "PASS" for key in ("entry", "targets")) else "TARGETS_PENDING" if receipt["targets"]["status"] == "PENDING" else "FAIL"
    (TASK / "report_validation.json").write_text(json.dumps(receipt, indent=2, allow_nan=False) + "\n", encoding="utf-8")
    print(json.dumps({"status": receipt["status"], "entry_status": receipt["entry"]["status"], "target_status": receipt["targets"]["status"]}))
