"""Reproduce the user-adopted G8 baseline without selecting another candidate."""
from __future__ import annotations

import argparse
from copy import deepcopy
from datetime import datetime, timezone
import html
import json
from pathlib import Path
import shutil

import numpy as np
import pandas as pd

from research import g8_vix_selection_research as core
from research.g8_exit_engine import metrics
from research.g8_sources import load_sources
from research.g8_vix_data import DEFAULT_5M, VixData

ROOT = Path(__file__).resolve().parents[1]
CONFIG = ROOT / "configs/strategies/v13_v10_g_8_g7_v2_nifty_vix_v2.json"
POLICY = "persistent_direction_30m_2.0"
SCHEMA = "g8_user_adopted_baseline_v1"


def definition(policy=None):
    cfg = json.loads(CONFIG.read_text(encoding="utf-8"))
    chosen = policy or cfg["baseline_policy"]
    if chosen == "nifty_base__original":
        chosen = core.BASELINE
    if chosen not in (POLICY, core.BASELINE):
        raise ValueError("Use the adopted 30-minute policy or baseline_nifty rollback")
    if cfg["gate"] != dict(id=POLICY, mode="persistent_direction", window=30, threshold=2.0):
        raise ValueError("Adopted gate changed; audit a new baseline revision first")
    spec = cfg["gate"] if chosen == POLICY else dict(id=core.BASELINE, mode="baseline")
    return cfg, deepcopy(spec)


def verify(directory):
    out = Path(directory).resolve()
    manifest = json.loads((out / "manifest.json").read_text(encoding="utf-8"))
    if manifest.get("schema_version") != SCHEMA:
        # Preserve verification of the original G8 research CLI's results.
        from research.g8_nifty_vix_backtest import verify as research_verify
        return research_verify(out)
    for name, digest in manifest["files"].items():
        path = (out / name).resolve()
        if not path.is_relative_to(out) or core.sha(path) != digest:
            raise ValueError("Changed baseline artifact: " + name)
    for name, digest in manifest["code"].items():
        path = (ROOT / name).resolve()
        if not path.is_relative_to(ROOT) or core.sha(path) != digest:
            raise ValueError("Changed baseline implementation: " + name)
    summary = json.loads((out / "summary.json").read_text(encoding="utf-8"))
    decisions = pd.read_csv(out / "vix_nifty_decisions.csv").set_index("sid", drop=False)
    if not (pd.to_datetime(decisions.vix_available_at, utc=True)
            <= pd.to_datetime(decisions.confirmation_ts, utc=True)).all():
        raise ValueError("Future VIX information")
    allowed = core.allowed_for(decisions, summary["gate"])
    if not np.array_equal(allowed, decisions.g8_allowed):
        raise ValueError("Published gate decisions differ from the configured policy")
    rows = pd.read_csv(out / "overall_comparison.csv").set_index("label")
    daily = pd.read_csv(out / "daywise_comparison.csv").set_index("day")
    for label in ("G7_V2_baseline", "G8_selected"):
        book = pd.read_csv(out / label / "portfolio_trades.csv")
        actual = metrics(book, summary["days"])
        for key in ("trades", "wins", "losses", "net_pnl_rs", "cost_rs", "gross_pnl_rs"):
            if not np.isclose(actual[key], rows.loc[label, key], atol=1e-6, rtol=0):
                raise ValueError("Ledger reconciliation: " + label + "/" + key)
        for day in summary["days"]:
            value = metrics(book.loc[book.day.eq(day)], [day])["net_pnl_rs"]
            if not np.isclose(value, daily.loc[day, label + "_net_pnl_rs"], atol=1e-6, rtol=0):
                raise ValueError("Daily reconciliation: " + day)
    return True


def run(through=core.CUTOFF, output=None, policy=None):
    descriptor, spec = definition(policy)
    if through != descriptor["through"]:
        raise ValueError("Sealed data ends at " + descriptor["through"] + "; a new date needs an input audit")
    source = load_sources(through=through)
    provenance = json.loads((DEFAULT_5M.parent / "india_vix_provenance.json").read_text(encoding="utf-8"))
    expected_vix = provenance["files"][DEFAULT_5M.name]
    if core.sha(DEFAULT_5M) != expected_vix:
        raise ValueError("VIX input differs from audited provenance")
    out = Path(output or core.OUTPUT_ROOT / "runs" / through /
               (datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%f") + "_g8_adopted_vix30"))
    out.mkdir(parents=True, exist_ok=False)
    code_paths = [Path(__file__), CONFIG, ROOT / "fno_v13_v10_g_8_backtest.py"] + [
        ROOT / "research" / name for name in ("g8_sources.py", "g8_exit_engine.py",
        "g8_vix_data.py", "g8_nifty_vix_backtest.py", "g8_vix_selection_research.py")]
    code = {str(p.relative_to(ROOT)): core.sha(p) for p in code_paths}
    frozen = out / "india_vix_5minute.csv"
    shutil.copyfile(DEFAULT_5M, frozen)
    if core.sha(frozen) != expected_vix:
        raise ValueError("VIX input changed during copy")
    frame = core.build_features(source, VixData(frozen))
    frame["g8_allowed"] = core.allowed_for(frame, spec)
    frame["vix_30m_fallback_used"] = frame.vix_30m_pct.isna()
    config = deepcopy(source["config"])
    config.update(strategy="V13-V10-G-8", variant_id=descriptor["variant_id"],
        scope="BACKTEST_ONLY", execution_authority=False, g8_selected_policy=spec,
        g8_baseline_definition=descriptor)
    fast = core.FastSelection(source["unfiltered"], config)
    books = {"G7_V2_baseline": source["baseline"],
             "G8_selected": fast.full_book(frame.g8_allowed, spec["id"])}
    days = source["days"]
    overall, daily, slot = [], [], []
    for label, book in books.items():
        (out / label).mkdir()
        book.to_csv(out / label / "portfolio_trades.csv", index=False)
        overall.append(dict(label=label, **metrics(book, days)))
        for day in days:
            daily.append(dict(label=label, day=day, **metrics(book.loc[book.day.eq(day)], [day])))
        for setup in config["active_setups"]:
            sid = setup["setup_id"]
            slot.append(dict(label=label, setup_id=sid, **metrics(book.loc[book.setup_id.eq(sid)], days)))
    overall = pd.DataFrame(overall)
    daily = pd.DataFrame(daily)
    pivot = daily.pivot(index="day", columns="label", values=["trades", "wins", "losses", "win_rate_pct", "net_pnl_rs"])
    pivot.columns = [label + "_" + field for field, label in pivot.columns]
    pivot["selected_net_change_rs"] = pivot.G8_selected_net_pnl_rs - pivot.G7_V2_baseline_net_pnl_rs
    overall.to_csv(out / "overall_comparison.csv", index=False)
    daily.to_csv(out / "daywise_all_policies.csv", index=False)
    pivot.to_csv(out / "daywise_comparison.csv")
    pd.DataFrame(slot).to_csv(out / "slot_side_comparison.csv", index=False)
    pd.DataFrame(config["active_setups"]).to_csv(out / "slot_settings.csv", index=False)
    frame.to_csv(out / "vix_nifty_decisions.csv", index=False)
    core.dump(out / "config.json", config)
    core.dump(out / "source_provenance.json", source["provenance"])
    # Preserve the prior publication pointers to make the adoption auditable.
    prior = {name: json.loads((core.OUTPUT_ROOT / name).read_text(encoding="utf-8"))
             for name in ("latest_research.json", "latest_baseline.json") if (core.OUTPUT_ROOT / name).exists()}
    core.dump(out / "previous_pointers.json", prior)
    summary = dict(schema_version=SCHEMA, selected_policy=spec["id"], gate=spec,
        selection_basis=descriptor["selection_basis"], manual_policy_override=policy,
        independent_holdout=False, scope="BACKTEST_ONLY", execution_authority=False,
        through=through, days=days, sessions=len(days), active_setups=len(config["active_setups"]),
        source_cache_manifest_sha256=source["manifest_sha256"], vix_sha256=expected_vix,
        selected_metrics=metrics(books["G8_selected"], days), baseline_metrics=metrics(books["G7_V2_baseline"], days),
        early_session_fallback=descriptor["early_session_fallback"],
        rollback_command="python fno_v13_v10_g_8_backtest.py --policy nifty_base__original",
        limitations=["User-adopted after historical research; not an independent validation pass.",
        "Fixed capital, 5x exposure, 5bps modeled round-trip cost, fractional quantities.",
        "Data availability is limited to the sealed 56 sessions through 22 October 2026."])
    core.dump(out / "summary.json", summary)
    report = "<!doctype html><meta charset='utf-8'><title>G8 adopted baseline</title><style>body{font:15px system-ui;margin:32px}table{border-collapse:collapse}td,th{padding:7px;border:1px solid #ddd;text-align:right}</style>"
    report += "<h1>G8 main backtesting baseline</h1><p>" + html.escape(spec["id"]) + "</p>"
    report += "<p>User-adopted on 23 October 2026. Historical research, not independent validation. Original G7 V2 slot targets and single exits.</p>"
    report += "<p>" + html.escape(descriptor["early_session_fallback"]) + "</p>"
    report += overall[["label", "trades", "wins", "losses", "win_rate_pct", "net_pnl_rs", "losing_days"]].to_html(index=False, float_format=lambda x: f"{x:,.2f}")
    report += "<h2>Daily comparison</h2><p><a href='daywise_comparison.csv'>CSV</a></p>" + pivot.to_html(float_format=lambda x: f"{x:,.2f}")
    (out / "report.html").write_text(report, encoding="utf-8")
    core.dump(out / "manifest.json", dict(schema_version=SCHEMA, code=code,
        files={str(p.relative_to(out)): core.sha(p) for p in out.rglob("*") if p.is_file()}))
    verify(out)
    pointer = dict(run_dir=str(out.resolve()), manifest_sha256=core.sha(out / "manifest.json"),
        selected_policy=spec["id"], selection_basis=descriptor["selection_basis"], execution_authority=False)
    core.dump(core.OUTPUT_ROOT / "latest_baseline.json", pointer)
    core.dump(core.OUTPUT_ROOT / "latest_research.json", pointer)
    print(json.dumps(dict(output=str(out), selected_policy=spec["id"], metrics=summary["selected_metrics"]), indent=2))
    return out


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--through", default=core.CUTOFF)
    parser.add_argument("--output")
    parser.add_argument("--policy")
    parser.add_argument("--verify")
    parser.add_argument("--plan", action="store_true")
    args = parser.parse_args()
    if args.verify:
        print("verified", verify(args.verify))
    elif args.plan:
        cfg, spec = definition(args.policy)
        print(json.dumps(dict(configuration=cfg, selected_gate=spec), indent=2))
    else:
        run(args.through, args.output, args.policy)
