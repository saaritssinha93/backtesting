"""Sensitivity grid for 09:51 LONG confirmation; research only, no production edits."""
from __future__ import annotations

import json
from datetime import date
from itertools import product
from pathlib import Path

import pandas as pd

import stepwise_confirmation_study as stepwise


study = stepwise.study
OUT = study.OUT / "grid_0950_long_1m_sensitivity_staged_125_to100_target100.json"
BODIES = (.50, .45, .40, .35, .30, .25)
WICKS = (.50, .55, .60)
VOLUMES = (1.30, 1.10, 1.00, .80, .60)


def recent_paths_for(rows, sources):
    paths = {}
    for sid, row in rows.items():
        day = pd.Timestamp(row["day"]).date()
        order = pd.DataFrame([row])
        if day in study.recent_replay.RUNS:
            snapshot = Path(sources[str(day)]["snapshot_manifest"]).parent
            paths.update(study.recent_replay.ext._selected_paths(order, day, snapshot))
        elif day == date(2026, 10, 12) and row["tradingsymbol"] == "SAIL":
            minutes = pd.read_parquet(study.OUT / "sail_oct12_minutes.parquet")
            path = minutes.loc[minutes.ts.gt(pd.Timestamp(row["confirmation_ts"])) &
                minutes.ts.le(pd.Timestamp("2026-10-12 15:15", tz=study.contract.IST))].copy()
            expected = pd.date_range("2026-10-12 09:52", "2026-10-12 15:15",
                                     freq="min", tz=study.contract.IST)
            if len(path) != len(expected) or not path.ts.reset_index(drop=True).eq(pd.Series(expected)).all():
                raise RuntimeError("SAIL October 12 minute path incomplete")
            paths[sid] = dict(timestamp_ns=path.ts.astype("int64").to_numpy(),
                **{name: path[name].to_numpy(float) for name in ("open", "high", "low", "close")})
        else:
            raise RuntimeError(f"No verified minute path for {day} {row['tradingsymbol']}")
    return paths


def simulate_individual(rows, paths):
    if not rows:
        return {}
    work = pd.DataFrame(rows.values()).copy()
    work["native_stop_pct"] = 1.25
    work["native_target_pct"] = 1.0
    study.g2.g.v9.validate_paths(work, paths)
    trades = study.g2.simulate_staged(work, paths, cost_bps=5., max_entry_delay_minutes=10)
    trades = study.g2.g.v9.v5.apply_fixed_capital_model(trades, 100_000., 5.)
    base = study.g2.g.v9.V9Config(portfolio_capital_rupees=1_000_000.,
        capital_per_entry_rupees=100_000., leverage_factor=5., max_positions=None, cost_bps=5.)
    ledger = study.g2.g.v9.v6.apply_portfolio_constraints(trades, base.portfolio_config())[0]
    return {int(row["sid"]): dict(date=str(row["day"]), symbol=row["tradingsymbol"],
        filled=bool(row["filled"]), executed=bool(row["portfolio_executed"]),
        reason=row["exit_reason"], net_pnl_rs=float(row["portfolio_net_profit_rupees"]))
        for row in ledger.to_dict("records")}


def metrics(ids, outcomes):
    rows = [outcomes[sid] for sid in ids]
    fills = [row for row in rows if row["executed"]]
    wins = sum(row["net_pnl_rs"] > 0 for row in fills)
    return dict(selections=len(ids), fills=len(fills), wins=wins, losses=sum(row["net_pnl_rs"] < 0 for row in fills),
        win_rate_pct=100*wins/len(fills) if fills else None,
        net_pnl_rs=sum(row["net_pnl_rs"] for row in fills))


def main():
    signals, bundle, provenance = study.previous.load_signals()
    days = [date.fromisoformat(value) for value in provenance["days"]]
    frozen = pd.read_csv(study.DEFAULT_FROZEN / "trades.csv")
    if set(study.contract.select_orders(signals).sid.astype(int)) != set(frozen.sid.astype(int)):
        raise RuntimeError("Frozen baseline selection parity failed")
    for item in provenance["extensions"]:
        manifest = Path(item["snapshot"]) / "snapshot_manifest.json"
        if study.sha(manifest) != provenance["verified_snapshot_manifest_hashes"][str(manifest)]:
            raise RuntimeError(f"Source manifest drift: {manifest}")
    historical = signals.loc[signals.signal_end.eq("09:50") & signals.side.eq("LONG")].copy()
    recent, recent_sources = study.recent_replay.candidates()
    hist_rows, recent_rows, scenarios = {}, {}, []
    for body, wick, volume in product(BODIES, WICKS, VOLUMES):
        stepwise.set_rule(body, wick, volume, False)
        selected = study.contract.selection_audit(historical)
        selected = selected.loc[selected.setup_id.eq("0951_LONG") & selected.v9_selected.eq(True)]
        new = study.contract.selection_audit(recent)
        new = new.loc[new.setup_id.eq("0951_LONG") & new.v9_selected.eq(True)]
        h_ids, r_ids = selected.sid.astype(int).tolist(), new.sid.astype(int).tolist()
        hist_rows.update({int(row["sid"]):row for row in selected.to_dict("records")})
        recent_rows.update({int(row["sid"]):row for row in new.to_dict("records")})
        scenarios.append(dict(body_min=body, wick_max=wick, volume_min=volume,
                              historical_ids=h_ids, recent_ids=r_ids))
    print("Grid selections", len(hist_rows), "historical", len(recent_rows), "recent", flush=True)
    hist_paths = study.previous.paths_for(pd.DataFrame(hist_rows.values()), bundle, provenance)
    recent_paths = recent_paths_for(recent_rows, recent_sources)
    hist_outcome = simulate_individual(hist_rows, hist_paths)
    recent_outcome = simulate_individual(recent_rows, recent_paths)
    first = set(days[:30])
    later = set(days[30:])
    for scenario in scenarios:
        ids, rids = scenario.pop("historical_ids"), scenario.pop("recent_ids")
        fids = [sid for sid in ids if date.fromisoformat(hist_outcome[sid]["date"]) in first]
        lids = [sid for sid in ids if date.fromisoformat(hist_outcome[sid]["date"]) in later]
        scenario["historical"] = metrics(ids, hist_outcome)
        scenario["first_30"] = metrics(fids, hist_outcome)
        scenario["later_16"] = metrics(lids, hist_outcome)
        scenario["recent_3"] = metrics(rids, recent_outcome)
        scenario["combined_49"] = metrics(ids+rids, hist_outcome | recent_outcome)
        scenario["selected_recent"] = [recent_outcome[sid] for sid in rids]
        scenario["selected_historical"] = [hist_outcome[sid] for sid in ids]
    more_and_70 = [s for s in scenarios if s["combined_49"]["fills"] > 4 and
                   s["combined_49"]["win_rate_pct"] >= 70]
    report = dict(study="Sensitivity grid: 09:50 LONG, only 09:51 body/wick/volume thresholds varied",
        fixed_five_minute_filter=dict(price_change_pct=.30, oi_change_pct=.15, volume_ratio=1.30),
        fixed_direction="green and close above 09:50 signal close",
        exit=dict(initial_stop_pct=1.25, tightened_stop_pct=1., tighten_after_minutes=120,
                  target_pct=1., cost_bps=5., capital_per_entry_rs=100000., leverage=5.),
        historical_sessions=46, recent_sessions=3, body_values=BODIES, wick_values=WICKS,
        confirmation_volume_values=VOLUMES, grid_count=len(scenarios),
        more_fills_and_70pct_combined_count=len(more_and_70),
        more_fills_and_70pct_combined=[{key:value for key,value in s.items()
            if key not in ("selected_recent", "selected_historical")} for s in more_and_70],
        recent_sources=recent_sources, scenarios=scenarios,
        assessment="Exploratory grid inspected after stepwise results; any winning threshold here is outcome-selected, not independent validation",
        production_modified=False)
    OUT.write_text(json.dumps(report, indent=2, allow_nan=False) + "\n", encoding="utf-8")
    print("More fills and at least 70% combined:",len(more_and_70),flush=True)
    for item in more_and_70:
        print(item["body_min"],item["wick_max"],item["volume_min"],
              item["combined_49"],item["recent_3"],flush=True)
    print(OUT,flush=True)


if __name__ == "__main__":
    main()
