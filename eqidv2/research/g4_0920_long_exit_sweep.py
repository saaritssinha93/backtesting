"""Bounded, historical G4 09:20 LONG exit-parameter experiment.

The catalogue is fixed in this file. Only the first 35 sessions select a
candidate. The later 17 sessions were already inspected in earlier G3/G4
research and are chronology diagnostics, not untouched validation.
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


VERSION = "G4-0920-LONG-EXIT-PARAMETER-SEARCH-V1"
DATA = base.G4_ROOT / "research/20261016_iterative_sealed_dataset_v1"
DATA_MANIFEST_SHA256 = "4dbc07968e5c5a0ae313bdd83670ca12b388f3f4f223b35e613952aa25cd80b2"
SOURCE_MANIFEST_SHA256 = "4dc3c45a4a1217881ba899682750a127333388dd9a8813bb916884f6cb89228f"
SETUP = "0921_LONG"
EPS = 1e-7


@dataclass(frozen=True)
class Variant:
    name: str
    target_pct: float
    stop_pct: float
    tighten_after_minutes: int | None
    tightened_stop_pct: float | None
    protect_after_close_pct: float | None = None
    protected_profit_pct: float = 0.0

    @property
    def within_current_g4_bounds(self) -> bool:
        return self.target_pct <= 2.0 and self.stop_pct <= 1.2


# Baseline plus eleven small, declared alternatives. Targets above 2% and
# stops above 1.2% explicitly exceed today's bounded G4 profile. Protection
# can only arm after a full post-entry close, effective at the next open.
VARIANTS = (
    Variant("baseline", 2.0, 1.2, 120, 1.0),
    Variant("target_150", 1.5, 1.2, 120, 1.0),
    Variant("target_175", 1.75, 1.2, 120, 1.0),
    Variant("target_225", 2.25, 1.2, 120, 1.0),
    Variant("target_250", 2.5, 1.2, 120, 1.0),
    Variant("target_300", 3.0, 1.2, 120, 1.0),
    Variant("stop_080", 2.0, 0.8, 120, 0.8),
    Variant("stop_100", 2.0, 1.0, 120, 1.0),
    Variant("stop_140", 2.0, 1.4, 120, 1.0),
    Variant("tighten_060", 2.0, 1.2, 60, 1.0),
    Variant("protect_040_floor_000", 2.0, 1.2, 120, 1.0, 0.4, 0.0),
    Variant("protect_060_floor_020", 2.0, 1.2, 120, 1.0, 0.6, 0.2),
)


def _same_outcome(row: pd.Series, outcome: dict) -> bool:
    return (np.isclose(float(row.exit_price), outcome["exit_price"], atol=1e-8, rtol=0)
            and pd.Timestamp(row.exit_ts) == pd.Timestamp(outcome["exit_ts"])
            and row.exit_reason == outcome["exit_reason"]
            and np.isclose(float(row.gross_return_pct), outcome["gross_return_pct"], atol=1e-8, rtol=0))


def replay(native: pd.DataFrame, paths: dict, variant: Variant) -> pd.DataFrame:
    """Change 09:20 LONG exits only, then reapply the original capital model."""
    raw = native.copy()
    setup_mask = raw.setup_id.eq(SETUP)
    for index, row in raw.loc[setup_mask & raw.filled.eq(True)].iterrows():
        outcome = simulate_exit(paths[int(row.sid)], int(row.entry_path_index),
            float(row.entry_price), True, variant.stop_pct, variant.target_pct,
            protect_after_close_pct=variant.protect_after_close_pct,
            protected_profit_pct=variant.protected_profit_pct,
            tighten_after_minutes=variant.tighten_after_minutes,
            tightened_stop_pct=variant.tightened_stop_pct)
        if variant.name == "baseline" and not _same_outcome(row, outcome):
            raise ValueError(f"Baseline minute replay changed SID {row.sid}")
        for key, value in outcome.items():
            raw.at[index, key] = value
        raw.at[index, "net_return_pct"] = outcome["gross_return_pct"] - .05
        raw.at[index, "cost_pct"] = .05
        raw.at[index, "target_pct"] = variant.target_pct
        raw.at[index, "native_target_pct"] = variant.target_pct
        raw.at[index, "stop_pct"] = variant.stop_pct
        raw.at[index, "native_stop_pct"] = variant.stop_pct
        raw.at[index, "initial_stop_pct"] = variant.stop_pct
        raw.at[index, "tightened_stop_pct"] = (variant.tightened_stop_pct
            if variant.tightened_stop_pct is not None else np.nan)
        raw.at[index, "tighten_after_minutes"] = (variant.tighten_after_minutes
            if variant.tighten_after_minutes is not None else np.nan)
    result = portfolio.allocate(raw, variant.name)
    if variant.name == "baseline":
        sealed.parity(result, native)
    return result


def metrics(before: pd.DataFrame, after: pd.DataFrame, days: list[str]) -> dict:
    """Report both the full allocated portfolio and the fixed original cohort."""
    b = before.set_index("sid").loc[lambda x: x.day.isin(days)]
    a = after.set_index("sid").loc[b.index]
    if not b.index.is_unique or not a.index.is_unique or not b.day.equals(a.day):
        raise ValueError("Trade identity or calendar changed")
    original = b.loc[b.portfolio_executed.eq(True)]
    follow = a.loc[original.index]
    original_winners = original.portfolio_net_profit_rupees.gt(EPS)
    original_winner_profit = float(original.loc[original_winners, "portfolio_net_profit_rupees"].sum())
    retained_winner_profit = float(follow.loc[original_winners, "portfolio_net_profit_rupees"].sum())
    old_setup = original.loc[original.setup_id.eq(SETUP)]
    new_setup = follow.loc[old_setup.index]
    setup_winners = old_setup.portfolio_net_profit_rupees.gt(EPS)
    setup_winner_profit = float(old_setup.loc[setup_winners, "portfolio_net_profit_rupees"].sum())
    retained_setup_profit = float(new_setup.loc[setup_winners, "portfolio_net_profit_rupees"].sum())
    old_allocated = b.loc[b.portfolio_executed.eq(True)]
    allocated = a.loc[a.portfolio_executed.eq(True)]
    pnl = allocated.portfolio_net_profit_rupees
    old_pnl = old_allocated.portfolio_net_profit_rupees
    old_setup_pnl = old_setup.portfolio_net_profit_rupees
    setup_pnl = new_setup.portfolio_net_profit_rupees
    return dict(sessions=len(days), trades=len(allocated), baseline_trades=len(old_allocated),
        wins=int(pnl.gt(EPS).sum()), losses=int(pnl.lt(-EPS).sum()),
        win_rate_pct=float(100*pnl.gt(EPS).mean()) if len(pnl) else np.nan,
        baseline_win_rate_pct=float(100*old_pnl.gt(EPS).mean()) if len(old_pnl) else np.nan,
        net_pnl_rs=float(pnl.sum()), baseline_net_pnl_rs=float(old_pnl.sum()),
        net_delta_rs=float(pnl.sum()-old_pnl.sum()),
        loss_amount_rs=float(-pnl.loc[pnl.lt(-EPS)].sum()),
        baseline_loss_amount_rs=float(-old_pnl.loc[old_pnl.lt(-EPS)].sum()),
        original_winner_profit_retention_pct=float(100*retained_winner_profit/original_winner_profit)
            if original_winner_profit else np.nan,
        setup_trades=len(new_setup), setup_wins=int(setup_pnl.gt(EPS).sum()),
        setup_losses=int(setup_pnl.lt(-EPS).sum()),
        setup_win_rate_pct=float(100*setup_pnl.gt(EPS).mean()) if len(setup_pnl) else np.nan,
        setup_baseline_wins=int(old_setup_pnl.gt(EPS).sum()),
        setup_baseline_losses=int(old_setup_pnl.lt(-EPS).sum()),
        setup_net_pnl_rs=float(setup_pnl.sum()),
        setup_baseline_net_pnl_rs=float(old_setup_pnl.sum()),
        setup_net_delta_rs=float(setup_pnl.sum()-old_setup_pnl.sum()),
        setup_original_winner_profit_retention_pct=float(100*retained_setup_profit/setup_winner_profit)
            if setup_winner_profit else np.nan,
        changed_setup_trades=int(((setup_pnl-old_setup_pnl).abs()>EPS).sum()),
        original_winners_turned_losses=int((original_winners & follow.portfolio_net_profit_rupees.lt(-EPS)).sum()),
        newly_admitted=int((~b.portfolio_executed & a.portfolio_executed).sum()),
        originally_executed_now_rejected=int((b.portfolio_executed & ~a.portfolio_executed).sum()))


def select_on_development(table: pd.DataFrame) -> tuple[str, str]:
    """Demand improvement on the first 35 sessions; never rank on later rows."""
    early = table.loc[table.period.eq("development35")].copy()
    eligible = early.loc[early.within_current_g4_bounds
        & early.setup_win_rate_pct.ge(77.-1e-8)
        & early.win_rate_pct.ge(early.baseline_win_rate_pct-1e-8)
        & early.original_winner_profit_retention_pct.ge(90.-1e-8)
        & early.setup_original_winner_profit_retention_pct.ge(90.-1e-8)
        & early.setup_wins.ge(early.setup_baseline_wins)
        & early.loss_amount_rs.le(early.baseline_loss_amount_rs+EPS)
        & early.net_delta_rs.gt(EPS)]
    if eligible.empty:
        return "baseline", "NO_DEVELOPMENT_IMPROVEMENT_WITHIN_G4_BOUNDS_AND_90_PERCENT_RETENTION"
    chosen = eligible.sort_values(["net_delta_rs", "changed_setup_trades", "policy"],
        ascending=[False, True, True], kind="stable").iloc[0]
    return str(chosen.policy), "FIRST35_NET_GAIN_WITH_77_PERCENT_SETUP_WIN_RATE_AND_90_PERCENT_WINNER_RETENTION"


def _write_json(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(base.source._json_ready(payload), indent=2, allow_nan=False)+"\n",
                    encoding="utf-8")


def _render(output: Path, table: pd.DataFrame, selected: str, summary: dict) -> None:
    columns = ["period", "policy", "within_current_g4_bounds", "trades", "wins", "losses",
        "win_rate_pct", "net_pnl_rs", "net_delta_rs", "loss_amount_rs",
        "original_winner_profit_retention_pct", "setup_trades", "setup_wins", "setup_losses",
        "setup_net_pnl_rs", "setup_net_delta_rs", "setup_original_winner_profit_retention_pct",
        "newly_admitted"]
    section = table.loc[table.policy.isin(dict.fromkeys(["baseline",selected])), columns]
    all_variants = table.loc[table.period.eq("all52"), columns]
    css = "body{font:15px/1.5 system-ui;margin:28px auto;max-width:1500px;background:#101927;color:#edf4ff}a{color:#8ed6ff}table{border-collapse:collapse;font-size:12px}td,th{padding:7px;border-bottom:1px solid #40526b;white-space:nowrap;text-align:right}th{background:#243449}.scroll{overflow:auto}section{margin:18px 0;padding:18px;background:#19283d;border-radius:8px}"
    note = ("The first 35 sessions contain all seven original 09:20 LONG winners and no loser. "
            "The only original 09:20 LONG loss is October 12, in the later 17 sessions. "
            "Therefore this history cannot train a credible setup-specific loss filter or prove a 77% future portfolio win rate.")
    doc = f'<!doctype html><html><head><meta charset="utf-8"><title>G4 09:20 LONG exit parameters</title><style>{css}</style></head><body><h1>G4 09:20 LONG exit-parameter research</h1>'
    doc += '<section><p>'+html.escape(note)+'</p><p>'+html.escape(summary["decision"])+'. Selection used the first 35 sessions only. The later 17 were previously examined in G3/G4 research and are chronological diagnostics, not untouched validation.</p></section>'
    doc += '<section><h2>Original versus development-selected</h2><div class="scroll">'+section.to_html(index=False,border=0,float_format=lambda x:f'{x:,.2f}')+'</div></section>'
    doc += '<section><h2>Every declared variant over all 52 sessions</h2><div class="scroll">'+all_variants.to_html(index=False,border=0,float_format=lambda x:f'{x:,.2f}')+'</div></section>'
    doc += '<section><h2>Execution and evidence</h2><ul>'+''.join('<li>'+html.escape(x)+'</li>' for x in summary["limitations"])+ '</ul></section>'
    doc += '<section><a href="comparison.csv">All variants and periods</a> · <a href="setup_trade_changes.csv">Eight setup trades</a> · <a href="portfolio_changes.csv">Selected policy portfolio changes</a> · <a href="config.json">Declared grid</a> · <a href="summary.json">Provenance</a></section></body></html>'
    (output/"report.html").write_text(doc,encoding="utf-8")


def run(output: Path) -> Path:
    output = Path(output).resolve()
    if output.exists() or not output.is_relative_to((base.G4_ROOT/"research").resolve()):
        raise ValueError("Require fresh isolated G4 research output")
    if base.sha256(DATA/"manifest.json") != DATA_MANIFEST_SHA256:
        raise ValueError("Frozen 1-minute dataset changed")
    native, _, paths, proof = dataset.load(DATA)
    if proof["original_manifest_sha256"] != SOURCE_MANIFEST_SHA256:
        raise ValueError("Original G4 source differs")
    if len(native) != 533 or int(native.portfolio_executed.sum()) != 466 or len(proof["days"]) != 52:
        raise ValueError("Original G4 cohort differs")
    if not set(native.loc[native.setup_id.eq(SETUP),"oi_change_availability"].dropna()) == {"RETROSPECTIVE_RESEARCH"}:
        raise ValueError("Unexpected opening OI availability")
    evidence = native.loc[native.setup_id.eq(SETUP),"complete_decision_evidence_by_0922"]
    if evidence.astype(str).str.lower().isin(["true", "1", "yes"]).any():
        raise ValueError("Unexpected live-timed opening evidence")
    days = list(proof["days"])
    if len(VARIANTS) > 12 or len({v.name for v in VARIANTS}) != len(VARIANTS):
        raise ValueError("Variant catalogue drift")
    books = {v.name: replay(native,paths,v) for v in VARIANTS}
    baseline = books["baseline"]
    rows = []
    periods = {"development35":days[:35],"later17":days[35:],"all52":days}
    for period, cohort in periods.items():
        for variant in VARIANTS:
            rows.append(dict(period=period,policy=variant.name,
                within_current_g4_bounds=variant.within_current_g4_bounds,
                target_pct=variant.target_pct,stop_pct=variant.stop_pct,
                tighten_after_minutes=variant.tighten_after_minutes,
                tightened_stop_pct=variant.tightened_stop_pct,
                **metrics(baseline,books[variant.name],cohort)))
    table = pd.DataFrame(rows)
    selected, reason = select_on_development(table)
    chosen = books[selected]
    original = baseline.set_index("sid")
    revised = chosen.set_index("sid").loc[original.index]
    all_setup = []
    for variant in VARIANTS:
        result = books[variant.name].set_index("sid").loc[original.index]
        old_rows = original.loc[original.setup_id.eq(SETUP)]
        new_rows = result.loc[old_rows.index]
        for sid in old_rows.index:
            a, b = old_rows.loc[sid], new_rows.loc[sid]
            all_setup.append(dict(policy=variant.name,sid=int(sid),day=a.day,symbol=a.tradingsymbol,
                baseline_exit_reason=a.exit_reason,new_exit_reason=b.exit_reason,
                baseline_exit_ts=str(a.exit_ts),new_exit_ts=str(b.exit_ts),
                baseline_net_rs=float(a.portfolio_net_profit_rupees),
                new_net_rs=float(b.portfolio_net_profit_rupees),
                delta_net_rs=float(b.portfolio_net_profit_rupees-a.portfolio_net_profit_rupees)))
    changes = pd.DataFrame([dict(sid=int(sid),day=a.day,setup_id=a.setup_id,symbol=a.tradingsymbol,
        baseline_executed=bool(a.portfolio_executed),selected_executed=bool(revised.loc[sid].portfolio_executed),
        baseline_net_rs=float(a.portfolio_net_profit_rupees),
        selected_net_rs=float(revised.loc[sid].portfolio_net_profit_rupees),
        delta_net_rs=float(revised.loc[sid].portfolio_net_profit_rupees-a.portfolio_net_profit_rupees))
        for sid,a in original.iterrows()])
    original_setup = baseline.loc[baseline.setup_id.eq(SETUP)]
    if len(original_setup) != 8 or original_setup.day.isin(days[:35]).sum() != 7:
        raise ValueError("09:20 LONG cohort differs")
    summary = dict(version=VERSION,scope="BACKTEST_ONLY",execution_authority=False,
        created_at_ist=datetime.now(base.production.IST).isoformat(),
        frozen_dataset=str(DATA),frozen_dataset_manifest_sha256=DATA_MANIFEST_SHA256,
        original_g4_source=proof["original_source"],original_g4_manifest_sha256=SOURCE_MANIFEST_SHA256,
        sessions=52,development_days=days[:35],chronological_diagnostic_days=days[35:],
        original_orders=533,original_executed_trades=466,setup_id=SETUP,
        setup_original_trades=8,setup_development_trades=7,setup_later_trades=1,
        selected_policy=selected,selection_reason=reason,
        decision=("No parameter variant earned a recommendation; retain the original 09:20 LONG exit settings"
            if selected=="baseline" else f"Development selection chose {selected}; the small sample still prevents a live recommendation"),
        baseline_full=table.loc[table.period.eq("all52")&table.policy.eq("baseline")].iloc[0].to_dict(),
        selected_full=table.loc[table.period.eq("all52")&table.policy.eq(selected)].iloc[0].to_dict(),
        selected_later17=table.loc[table.period.eq("later17")&table.policy.eq(selected)].iloc[0].to_dict(),
        limitations=[
            "Only eight original 09:20 LONG trades exist. Development has seven wins and zero losses; later history has the one loss.",
            "Earlier G3 09:20 target research examined these same paths, including later sessions. No period here is untouched future validation.",
            "Every original 09:20 LONG opening-OI comparison is labelled RETROSPECTIVE_RESEARCH, and no row has complete decision evidence by 09:22. Historical fills are not proof of live executability.",
            "Targets above 2% and the 1.4% initial stop exceed the current G4 bounded profile. They are research exceptions and are excluded from development selection.",
            "Entry, one-minute price path, 5 bps round-trip cost, Rs500,000 exposure per filled order, and Rs1,000,000/ten-position portfolio constraints match original G4.",
            "Stops and targets use intrabar one-minute OHLC with stop-first ambiguous bars and observed-open gap fills. Profit protection arms only after a full post-entry candle close and takes effect at the next bar. No tick or broker-latency model is available.",
            "A 77% full-history win rate needs at least 359 wins among the original 466 executions; the original has 356. This setup has only one original loss to improve.",
        ])
    output.mkdir(parents=True)
    (output/"code").mkdir()
    shutil.copyfile(__file__,output/"code"/Path(__file__).name)
    _write_json(output/"config.json",dict(version=VERSION,variants=[asdict(v) for v in VARIANTS],
        selection="First35 only: current G4 target/stop bounds, at least 77% setup win rate, no decrease in overall G4 win rate, at least 90% original-winner profit retained for portfolio and setup, no fewer setup wins, no greater total loss amount, positive net gain; rank net gain then fewer altered setup trades. Overall77% is reported as a separate goal.",
        no_promotion_without_new_data=True))
    table.to_csv(output/"comparison.csv",index=False)
    pd.DataFrame(all_setup).to_csv(output/"setup_trade_changes.csv",index=False)
    changes.to_csv(output/"portfolio_changes.csv",index=False)
    chosen.to_csv(output/"selected_portfolio_ledger.csv",index=False)
    _write_json(output/"summary.json",summary)
    _render(output,table,selected,summary)
    if base.sha256(DATA/"manifest.json") != DATA_MANIFEST_SHA256:
        raise ValueError("Frozen source changed during sweep")
    manifest = dict(version=VERSION,source_manifest_sha256=SOURCE_MANIFEST_SHA256,
        files={p.relative_to(output).as_posix():base.sha256(p) for p in output.rglob("*") if p.is_file()})
    _write_json(output/"manifest.json",manifest)
    print(output,flush=True)
    return output


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir",required=True,type=Path)
    run(parser.parse_args().output_dir)
