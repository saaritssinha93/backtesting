"""Chronological, research-only parameter sweep for G4 09:20 SHORT.

The candidate grid is deliberately small.  Selection reads the first 35
sessions only.  Later sessions and October are reported after selection and
are not used to choose the candidate.
"""
from __future__ import annotations

import argparse
from dataclasses import asdict, dataclass
from datetime import datetime
import html
import json
from pathlib import Path
import shutil

import numpy as np
import pandas as pd

from research import g4_iterative_dataset as dataset
from research import g4_g3_mirror_backtest as base
from research import g4_failed_breakout_research as sealed
from research import g4_trade_monitor_backtest as portfolio
from research.g3_sl_target_simulator import simulate_exit


VERSION = "G4-0920-SHORT-PARAMETER-SEARCH-V1"
DATA = base.G4_ROOT / "research/20261016_iterative_sealed_dataset_v1"
DATA_MANIFEST_SHA256 = "4dbc07968e5c5a0ae313bdd83670ca12b388f3f4f223b35e613952aa25cd80b2"
SOURCE_MANIFEST_SHA256 = "4dc3c45a4a1217881ba899682750a127333388dd9a8813bb916884f6cb89228f"
SETUP = "0921_SHORT"
EPS = 1e-7
CURRENT_ENTRY_CONFIGURATION = dict(signal_end="09:20", confirmation_end="09:21",
    side="SHORT", max_entries=2, picker="max_volume", price_change_pct=.13,
    core_price_change_pct=.20, oi_change_pct=.05, maximum_oi_change_pct=1.0,
    volume_ratio=1.5, confirmation_volume_ratio=1.2, body_ratio=.4,
    maximum_wick_ratio=.6, nifty_first_bar_return_max_pct=-.05,
    entry_expiry_minutes=10, target_pct=1.5, initial_stop_pct=1.2,
    tightened_stop_pct=1.0, tighten_after_minutes=120)


@dataclass(frozen=True)
class Variant:
    name: str
    target_pct: float = 1.5
    stop_pct: float = 1.2
    tighten_after_minutes: int = 120
    tightened_stop_pct: float = 1.0
    protect_after_close_pct: float | None = None
    protected_profit_pct: float = 0.0

    @property
    def reward_after_cost_rs(self) -> float:
        return (self.target_pct - .05) * 5000.

    @property
    def loss_after_cost_rs(self) -> float:
        return (self.stop_pct + .05) * 5000.

    @property
    def target_break_even_win_rate_pct(self) -> float:
        return 100 * self.loss_after_cost_rs / (self.loss_after_cost_rs + self.reward_after_cost_rs)


# Baseline plus eleven declared alternatives. Tiny targets are included to
# expose the win-rate/payoff tradeoff; they are not automatically recommendable.
VARIANTS = (
    Variant("baseline"),
    Variant("target_011", target_pct=.11),
    Variant("target_020", target_pct=.20),
    Variant("target_030", target_pct=.30),
    Variant("target_050", target_pct=.50),
    Variant("target_075", target_pct=.75),
    Variant("target_100", target_pct=1.00),
    Variant("stop_080", stop_pct=.8, tightened_stop_pct=.8),
    Variant("stop_100", stop_pct=1.0),
    Variant("tighten_060", tighten_after_minutes=60),
    Variant("protect_030_floor_005", protect_after_close_pct=.30, protected_profit_pct=.05),
    Variant("protect_040_floor_010", protect_after_close_pct=.40, protected_profit_pct=.10),
)


def _same_outcome(row: pd.Series, outcome: dict) -> bool:
    return (np.isclose(float(row.exit_price), outcome["exit_price"], atol=1e-8, rtol=0)
            and pd.Timestamp(row.exit_ts) == pd.Timestamp(outcome["exit_ts"])
            and row.exit_reason == outcome["exit_reason"]
            and np.isclose(float(row.gross_return_pct), outcome["gross_return_pct"], atol=1e-8, rtol=0))


def replay(native: pd.DataFrame, paths: dict, variant: Variant) -> pd.DataFrame:
    raw = native.copy()
    mask = raw.setup_id.eq(SETUP) & raw.filled.eq(True)
    for index, row in raw.loc[mask].iterrows():
        outcome = simulate_exit(paths[int(row.sid)], int(row.entry_path_index),
            float(row.entry_price), False, variant.stop_pct, variant.target_pct,
            protect_after_close_pct=variant.protect_after_close_pct,
            protected_profit_pct=variant.protected_profit_pct,
            tighten_after_minutes=variant.tighten_after_minutes,
            tightened_stop_pct=variant.tightened_stop_pct)
        if variant.name == "baseline" and not _same_outcome(row, outcome):
            raise ValueError(f"Baseline minute replay changed SID {row.sid}")
        for key, value in outcome.items():
            raw.at[index, key] = value
        raw.at[index, "cost_pct"] = .05
        raw.at[index, "net_return_pct"] = outcome["gross_return_pct"] - .05
        raw.at[index, "target_pct"] = variant.target_pct
        raw.at[index, "native_target_pct"] = variant.target_pct
        raw.at[index, "stop_pct"] = variant.stop_pct
        raw.at[index, "native_stop_pct"] = variant.stop_pct
        raw.at[index, "initial_stop_pct"] = variant.stop_pct
        raw.at[index, "tightened_stop_pct"] = variant.tightened_stop_pct
        raw.at[index, "tighten_after_minutes"] = variant.tighten_after_minutes
    result = portfolio.allocate(raw, variant.name)
    if variant.name == "baseline":
        sealed.parity(result, native)
    return result


def metrics(before: pd.DataFrame, after: pd.DataFrame, days: list[str]) -> dict:
    b = before.set_index("sid").loc[lambda x: x.day.isin(days)]
    a = after.set_index("sid").loc[b.index]
    old = b.loc[b.portfolio_executed.eq(True)]
    new = a.loc[a.portfolio_executed.eq(True)]
    fixed = a.loc[old.index]
    old_setup = old.loc[old.setup_id.eq(SETUP)]
    new_setup = fixed.loc[old_setup.index]
    old_winners = old.portfolio_net_profit_rupees.gt(EPS)
    setup_winners = old_setup.portfolio_net_profit_rupees.gt(EPS)
    old_winner_profit = float(old.loc[old_winners, "portfolio_net_profit_rupees"].sum())
    new_winner_profit = float(fixed.loc[old_winners, "portfolio_net_profit_rupees"].sum())
    old_setup_winner_profit = float(old_setup.loc[setup_winners, "portfolio_net_profit_rupees"].sum())
    new_setup_winner_profit = float(new_setup.loc[setup_winners, "portfolio_net_profit_rupees"].sum())
    pnl = new.portfolio_net_profit_rupees
    old_pnl = old.portfolio_net_profit_rupees
    setup_pnl = new_setup.portfolio_net_profit_rupees
    old_setup_pnl = old_setup.portfolio_net_profit_rupees
    return dict(sessions=len(days), trades=len(new), baseline_trades=len(old),
        wins=int(pnl.gt(EPS).sum()), losses=int(pnl.lt(-EPS).sum()),
        win_rate_pct=float(100*pnl.gt(EPS).mean()) if len(pnl) else np.nan,
        baseline_win_rate_pct=float(100*old_pnl.gt(EPS).mean()) if len(old_pnl) else np.nan,
        net_pnl_rs=float(pnl.sum()), baseline_net_pnl_rs=float(old_pnl.sum()),
        net_delta_rs=float(pnl.sum()-old_pnl.sum()),
        loss_amount_rs=float(-pnl.loc[pnl.lt(-EPS)].sum()),
        baseline_loss_amount_rs=float(-old_pnl.loc[old_pnl.lt(-EPS)].sum()),
        original_winner_profit_retention_pct=(100*new_winner_profit/old_winner_profit if old_winner_profit else np.nan),
        setup_trades=len(new_setup), setup_wins=int(setup_pnl.gt(EPS).sum()),
        setup_losses=int(setup_pnl.lt(-EPS).sum()),
        setup_win_rate_pct=float(100*setup_pnl.gt(EPS).mean()) if len(setup_pnl) else np.nan,
        setup_baseline_wins=int(old_setup_pnl.gt(EPS).sum()),
        setup_baseline_losses=int(old_setup_pnl.lt(-EPS).sum()),
        setup_net_pnl_rs=float(setup_pnl.sum()),
        setup_baseline_net_pnl_rs=float(old_setup_pnl.sum()),
        setup_net_delta_rs=float(setup_pnl.sum()-old_setup_pnl.sum()),
        setup_original_winner_profit_retention_pct=(100*new_setup_winner_profit/old_setup_winner_profit
            if old_setup_winner_profit else np.nan),
        changed_setup_trades=int(((setup_pnl-old_setup_pnl).abs()>EPS).sum()),
        newly_admitted=int((~b.portfolio_executed & a.portfolio_executed).sum()),
        originally_executed_now_rejected=int((b.portfolio_executed & ~a.portfolio_executed).sum()))


def select_on_development(table: pd.DataFrame) -> tuple[str, str]:
    early = table.loc[table.period.eq("development35")].copy()
    eligible = early.loc[early.setup_win_rate_pct.ge(75.-1e-8)
        & early.win_rate_pct.ge(early.baseline_win_rate_pct-1e-8)
        & early.original_winner_profit_retention_pct.ge(90.-1e-8)
        & early.setup_original_winner_profit_retention_pct.ge(90.-1e-8)
        & early.loss_amount_rs.lt(early.baseline_loss_amount_rs-EPS)
        & early.net_delta_rs.gt(EPS)]
    if eligible.empty:
        return "baseline", "NO_DEVELOPMENT_CANDIDATE_MET_75_PERCENT_AND_ECONOMIC_GUARDS"
    chosen = eligible.sort_values(["net_delta_rs", "loss_amount_rs", "policy"],
        ascending=[False, True, True], kind="stable").iloc[0]
    return str(chosen.policy), "FIRST35_ONLY_SELECTION_WITH_75_PERCENT_AND_ECONOMIC_GUARDS"


def _json(path: Path, value: dict) -> None:
    path.write_text(json.dumps(base.source._json_ready(value), indent=2, allow_nan=False)+"\n", encoding="utf-8")


def _render(output: Path, table: pd.DataFrame, selected: str, summary: dict) -> None:
    cols = ["period","policy","target_pct","stop_pct","setup_trades","setup_wins","setup_losses",
        "setup_win_rate_pct","setup_net_pnl_rs","setup_net_delta_rs",
        "setup_original_winner_profit_retention_pct","target_break_even_win_rate_pct",
        "wins","losses","win_rate_pct","net_delta_rs","newly_admitted"]
    chosen = table.loc[table.policy.isin(dict.fromkeys(["baseline", selected])), cols]
    all_rows = table.loc[table.period.eq("all52"), cols]
    october = table.loc[table.period.eq("october"), cols]
    css = "body{font:15px/1.5 system-ui;margin:28px auto;max-width:1500px;background:#101927;color:#edf4ff}a{color:#8ed6ff}table{border-collapse:collapse;font-size:12px}td,th{padding:7px;border-bottom:1px solid #40526b;white-space:nowrap;text-align:right}th{background:#243449}.scroll{overflow:auto}section{margin:18px 0;padding:18px;background:#19283d;border-radius:8px}"
    doc = f'<!doctype html><html><head><meta charset="utf-8"><title>G4 09:20 SHORT parameters</title><style>{css}</style></head><body><h1>G4 09:20 SHORT parameter research</h1>'
    doc += '<section><p>'+html.escape(summary["decision"])+'. The first 35 sessions alone selected the policy. Later sessions and October were opened afterward.</p><p>Only five trades exist: three in development and two in October. This cannot validate a future 75% win rate.</p></section>'
    config_table = pd.DataFrame([dict(setting=k,current=v,recommended=v)
        for k,v in summary["current_configuration"].items()])
    doc += '<section><h2>Configuration decision</h2><p>No setting changes are recommended.</p><div class="scroll">'+config_table.to_html(index=False,border=0)+'</div></section>'
    doc += '<section><h2>Original versus selected</h2><div class="scroll">'+chosen.to_html(index=False,border=0,float_format=lambda x:f'{x:,.2f}')+'</div></section>'
    doc += '<section><h2>October</h2><div class="scroll">'+october.to_html(index=False,border=0,float_format=lambda x:f'{x:,.2f}')+'</div></section>'
    doc += '<section><h2>All declared variants</h2><div class="scroll">'+all_rows.to_html(index=False,border=0,float_format=lambda x:f'{x:,.2f}')+'</div></section>'
    doc += '<section><h2>Limits</h2><ul>'+''.join('<li>'+html.escape(x)+'</li>' for x in summary["limitations"])+ '</ul></section>'
    doc += '<section><a href="comparison.csv">All results</a> | <a href="setup_trade_changes.csv">Trade changes</a> | <a href="october_trades.csv">October trades</a> | <a href="config.json">Configuration grid</a> | <a href="summary.json">Summary</a></section></body></html>'
    (output/"report.html").write_text(doc,encoding="utf-8")


def run(output: Path) -> Path:
    output = Path(output).resolve()
    if output.exists() or not output.is_relative_to((base.G4_ROOT/"research").resolve()):
        raise ValueError("Require a fresh isolated G4 research output")
    if base.sha256(DATA/"manifest.json") != DATA_MANIFEST_SHA256:
        raise ValueError("Frozen one-minute dataset changed")
    native, _, paths, proof = dataset.load(DATA)
    if proof["original_manifest_sha256"] != SOURCE_MANIFEST_SHA256:
        raise ValueError("Original G4 source differs")
    if len(native) != 533 or int(native.portfolio_executed.sum()) != 466 or len(proof["days"]) != 52:
        raise ValueError("Original G4 cohort differs")
    setup = native.loc[native.setup_id.eq(SETUP)]
    if len(setup) != 5 or not set(setup.oi_change_availability.dropna()) == {"RETROSPECTIVE_RESEARCH"}:
        raise ValueError("09:20 SHORT cohort or OI evidence differs")
    if setup.complete_decision_evidence_by_0922.astype(str).str.lower().isin(["true","1","yes"]).any():
        raise ValueError("Unexpected decision-time opening evidence")
    days = list(proof["days"])
    books = {variant.name: replay(native, paths, variant) for variant in VARIANTS}
    baseline = books["baseline"]
    periods = {"development35":days[:35], "later17":days[35:], "october":[d for d in days if d >= "2026-10-01"], "all52":days}
    rows = []
    for period, cohort in periods.items():
        for variant in VARIANTS:
            rows.append(dict(period=period, policy=variant.name, **asdict(variant),
                target_break_even_win_rate_pct=variant.target_break_even_win_rate_pct,
                **metrics(baseline, books[variant.name], cohort)))
    table = pd.DataFrame(rows)
    selected, reason = select_on_development(table)
    chosen = books[selected]
    old = baseline.set_index("sid")
    trade_rows = []
    for variant in VARIANTS:
        new = books[variant.name].set_index("sid").loc[old.index]
        for sid, before in old.loc[old.setup_id.eq(SETUP)].iterrows():
            after = new.loc[sid]
            trade_rows.append(dict(policy=variant.name,sid=int(sid),day=before.day,symbol=before.tradingsymbol,
                baseline_exit_reason=before.exit_reason,new_exit_reason=after.exit_reason,
                baseline_net_rs=float(before.portfolio_net_profit_rupees),
                new_net_rs=float(after.portfolio_net_profit_rupees),
                delta_net_rs=float(after.portfolio_net_profit_rupees-before.portfolio_net_profit_rupees)))
    selected_trades = pd.DataFrame(trade_rows).loc[lambda x: x.policy.eq(selected)]
    october_trades = selected_trades.loc[selected_trades.day.ge("2026-10-01")]
    summary = dict(version=VERSION,scope="BACKTEST_ONLY",execution_authority=False,
        created_at_ist=datetime.now(base.production.IST).isoformat(),frozen_dataset=str(DATA),
        frozen_dataset_manifest_sha256=DATA_MANIFEST_SHA256,original_g4_source=proof["original_source"],
        original_g4_manifest_sha256=SOURCE_MANIFEST_SHA256,sessions=52,
        development_days=days[:35],chronological_diagnostic_days=days[35:],
        original_orders=533,original_executed_trades=466,setup_id=SETUP,
        setup_original_trades=5,setup_development_trades=3,setup_later_trades=2,
        selected_policy=selected,selection_reason=reason,
        decision=("No tested configuration passed the training and economic guards; retain the original G4 09:20 SHORT configuration"
            if selected == "baseline" else f"Training selected {selected}; the five-trade sample prevents promotion"),
        baseline_full=table.loc[table.period.eq("all52")&table.policy.eq("baseline")].iloc[0].to_dict(),
        selected_full=table.loc[table.period.eq("all52")&table.policy.eq(selected)].iloc[0].to_dict(),
        selected_october=table.loc[table.period.eq("october")&table.policy.eq(selected)].iloc[0].to_dict(),
        current_configuration=CURRENT_ENTRY_CONFIGURATION,
        recommended_configuration=CURRENT_ENTRY_CONFIGURATION,
        rejected_observed_alternatives={name:table.loc[table.period.eq("all52")&table.policy.eq(name)].iloc[0].to_dict()
            for name in ("target_011","target_030","protect_040_floor_010")},
        limitations=[
            "Only five 09:20 SHORT trades exist: three development trades and two later trades, both on October 6.",
            "This history and its October outcomes were already examined in earlier 09:20 research; the later block is chronological diagnosis, not a pristine holdout.",
            "All five opening OI comparisons are retrospective and lack complete decision evidence by 09:22, so historical fills do not establish live executability.",
            "A target can mechanically increase win rate while worsening payoff. The report shows the target-only break-even win rate after the same 5 bps modeled cost.",
            "The 75% screen also requires positive training net change, lower loss money, at least 90% of original winner profit, and no decrease in full-portfolio training win rate.",
            "Entries are unchanged. Each filled order uses Rs500,000 exposure, Rs250 round-trip modeled cost, one-minute stop-first ambiguity, observed-open gaps and 15:15 square-off.",
        ])
    output.mkdir(parents=True)
    (output/"code").mkdir()
    shutil.copyfile(__file__, output/"code"/Path(__file__).name)
    _json(output/"config.json", dict(version=VERSION,setup_id=SETUP,win_rate_goal_pct=75.,
        current_entry_configuration=CURRENT_ENTRY_CONFIGURATION,
        recommended_entry_configuration=CURRENT_ENTRY_CONFIGURATION,
        minimum_original_winner_profit_retention_pct=90.,variants=[asdict(v) for v in VARIANTS],
        selection="First35 only; setup win rate >=75%, portfolio training win rate not lower, winner profit retention >=90% for setup and portfolio, lower loss amount, positive net change."))
    table.to_csv(output/"comparison.csv",index=False)
    pd.DataFrame(trade_rows).to_csv(output/"setup_trade_changes.csv",index=False)
    october_trades.to_csv(output/"october_trades.csv",index=False)
    chosen.to_csv(output/"selected_portfolio_ledger.csv",index=False)
    _json(output/"summary.json",summary)
    _render(output,table,selected,summary)
    manifest = dict(version=VERSION,files={p.relative_to(output).as_posix():base.sha256(p)
        for p in output.rglob("*") if p.is_file()})
    _json(output/"manifest.json",manifest)
    print(output,flush=True)
    return output


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir",required=True,type=Path)
    run(parser.parse_args().output_dir)
