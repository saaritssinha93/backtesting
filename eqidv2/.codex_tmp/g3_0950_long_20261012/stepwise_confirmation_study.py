"""Outcome-blind, stepwise 09:51 relaxation for the stricter 09:50 LONG."""
from __future__ import annotations

import json
import math
from dataclasses import asdict, replace
from datetime import date
from pathlib import Path

import pandas as pd

import strict_1pct_study as study


OUT = study.OUT / "stepwise_0950_long_1m_relax_staged_125_to100_target100.json"
ORIGINAL_VALID = study.contract._confirmation_valid

# Every row changes just one confirmation condition from its predecessor.
STEPS = [
    ("00_strict", .50, .50, 1.30, False),
    ("01_body_045", .45, .50, 1.30, False),
    ("02_body_040", .40, .50, 1.30, False),
    ("03_volume_110", .40, .50, 1.10, False),
    ("04_volume_100", .40, .50, 1.00, False),
    ("05_wick_055", .40, .55, 1.00, False),
    ("06_wick_060", .40, .60, 1.00, False),
    ("07_body_035", .35, .60, 1.00, False),
    ("08_body_030", .30, .60, 1.00, False),
    ("09_volume_080", .30, .60, .80, False),
    ("10_body_025", .25, .60, .80, False),
    ("11_volume_060", .25, .60, .60, False),
    ("12_green_only", .25, .60, .60, True),
]


def green_only(row, side):
    """Drop only the displacement above signal close; preserve all other validity checks."""
    if side != "LONG" or ORIGINAL_VALID(row, side):
        return ORIGINAL_VALID(row, side)
    candidate = dict(row)
    close = study.contract._number(candidate.get("confirm_close"))
    signal_close = study.contract._number(candidate.get("signal_close"))
    if not (math.isfinite(close) and math.isfinite(signal_close)):
        return False
    candidate["signal_close"] = min(signal_close, close * (1 - 1e-9))
    return ORIGINAL_VALID(candidate, side)


def set_rule(body, wick, volume, green):
    donor = study.contract._ACTIVE_BY_ID["0956_LONG"]
    setup = replace(donor, signal_end="09:50", confirmation_end="09:51",
                    price_change_pct=.30, oi_change_pct=.15, volume_ratio=1.30,
                    body_ratio=body, max_wick_ratio=wick,
                    stop_pct=1.25, target_pct=1.0)
    key = setup.setup_id
    study.contract.CORE_SETUPS = tuple(s for s in study.contract.CORE_SETUPS if s.setup_id != key) + (setup,)
    study.contract.ACTIVE_SETUPS = tuple(s for s in study.contract.ACTIVE_SETUPS if s.setup_id != key) + (setup,)
    study.contract._CORE_BY_ID[key] = setup
    study.contract._ACTIVE_BY_ID[key] = setup
    study.contract.MIN_CONFIRMATION_VOLUME_BY_SIDE["LONG"] = volume
    study.contract._confirmation_valid = green_only if green else ORIGINAL_VALID
    return setup


def metric(ledger, days):
    selected = ledger.loc[ledger.portfolio_executed.eq(True)]
    net = selected.portfolio_net_profit_rupees.astype(float)
    dates = pd.to_datetime(selected.day).dt.date
    def bucket(day_set):
        sub = selected.loc[dates.isin(day_set)]
        pnl = sub.portfolio_net_profit_rupees.astype(float)
        return dict(fills=len(sub), wins=int(pnl.gt(0).sum()), losses=int(pnl.lt(0).sum()),
                    win_rate_pct=100 * float(pnl.gt(0).mean()) if len(pnl) else None,
                    net_pnl_rs=float(pnl.sum()))
    return dict(selected_orders=len(ledger), fills=len(selected), wins=int(net.gt(0).sum()),
                losses=int(net.lt(0).sum()), win_rate_pct=100 * float(net.gt(0).mean()) if len(net) else None,
                net_pnl_rs=float(net.sum()), first_30=bucket(set(days[:30])),
                later_16=bucket(set(days[30:])))


def main():
    signals, bundle, provenance = study.previous.load_signals()
    days = [date.fromisoformat(value) for value in provenance["days"]]
    baseline = study.contract.select_orders(signals)
    frozen = pd.read_csv(study.DEFAULT_FROZEN / "trades.csv")
    if set(baseline.sid.astype(int)) != set(frozen.sid.astype(int)):
        raise RuntimeError("Frozen 46-session baseline selection parity failed")
    for item in provenance["extensions"]:
        manifest = Path(item["snapshot"]) / "snapshot_manifest.json"
        if study.sha(manifest) != provenance["verified_snapshot_manifest_hashes"][str(manifest)]:
            raise RuntimeError(f"Source manifest drift: {manifest}")
    historical = signals.loc[signals.signal_end.eq("09:50") & signals.side.eq("LONG")].copy()
    recent, recent_sources = study.recent_replay.candidates()
    chosen = []
    union = {}
    recent_union = {}
    for label, body, wick, volume, green in STEPS:
        setup = set_rule(body, wick, volume, green)
        audit = study.contract.selection_audit(historical)
        orders = audit.loc[audit.setup_id.eq("0951_LONG") & audit.v9_selected.eq(True)].copy()
        if len(orders) != len(set(pd.to_datetime(orders.day).dt.date)):
            raise RuntimeError(f"More than one historical order per session: {label}")
        for row in orders.to_dict("records"):
            union[int(row["sid"])] = row
        recent_audit = study.contract.selection_audit(recent)
        recent_orders = recent_audit.loc[recent_audit.setup_id.eq("0951_LONG") & recent_audit.v9_selected.eq(True)].copy()
        for row in recent_orders.to_dict("records"):
            recent_union[int(row["sid"])] = row
        recent_rows = [{"date": str(row["day"]), "symbol": row["tradingsymbol"],
                        "sid": int(row["sid"])} for row in recent_orders.to_dict("records")]
        chosen.append((label, setup, body, wick, volume, green, orders, recent_orders, recent_rows))
        print(label, "historical", len(orders), "recent", recent_rows, flush=True)
    all_orders = pd.DataFrame(union.values())
    paths = study.previous.paths_for(all_orders, bundle, provenance)
    recent_paths = {}
    for sid, row in recent_union.items():
        day = pd.Timestamp(row["day"]).date()
        order = pd.DataFrame([row])
        if day in study.recent_replay.RUNS:
            snapshot = Path(recent_sources[str(day)]["snapshot_manifest"]).parent
            recent_paths.update(study.recent_replay.ext._selected_paths(order, day, snapshot))
        elif day == date(2026, 10, 12) and row["tradingsymbol"] == "SAIL":
            minutes = pd.read_parquet(study.OUT / "sail_oct12_minutes.parquet")
            path = minutes.loc[minutes.ts.gt(pd.Timestamp(row["confirmation_ts"])) &
                minutes.ts.le(pd.Timestamp("2026-10-12 15:15", tz=study.contract.IST))].copy()
            expected = pd.date_range("2026-10-12 09:52", "2026-10-12 15:15",
                                     freq="min", tz=study.contract.IST)
            if len(path) != len(expected) or not path.ts.reset_index(drop=True).eq(pd.Series(expected)).all():
                raise RuntimeError("SAIL October 12 minute path incomplete")
            recent_paths[sid] = dict(timestamp_ns=path.ts.astype("int64").to_numpy(),
                **{name: path[name].to_numpy(float) for name in ("open", "high", "low", "close")})
        else:
            raise RuntimeError(f"No verified minute path for recent selection: {day} {row['tradingsymbol']}")
    base = study.g2.g.v9.V9Config(portfolio_capital_rupees=1_000_000.,
        capital_per_entry_rupees=100_000., leverage_factor=5., max_positions=None, cost_bps=5.)
    results = []
    def simulate(orders, source_paths):
        if len(orders):
            work = orders.copy()
            work["native_stop_pct"] = study.g2.INITIAL_STOP_PCT
            work["native_target_pct"] = 1.0
            needed = {int(sid): source_paths[int(sid)] for sid in work.sid}
            study.g2.g.v9.validate_paths(work, needed)
            trades = study.g2.simulate_staged(work, needed, cost_bps=5., max_entry_delay_minutes=10)
            trades = study.g2.g.v9.v5.apply_fixed_capital_model(trades, 100_000., 5.)
            return study.g2.g.v9.v6.apply_portfolio_constraints(trades, base.portfolio_config())[0]
        return pd.DataFrame()

    def trade_rows(ledger):
        return [dict(date=str(row["day"]), symbol=row["tradingsymbol"],
                     filled=bool(row["filled"]), executed=bool(row["portfolio_executed"]),
                     entry_time=str(row["entry_ts"]) if pd.notna(row["entry_ts"]) else None,
                     exit_time=str(row["exit_ts"]) if pd.notna(row["exit_ts"]) else None,
                     exit_reason=row["exit_reason"], net_pnl_rs=float(row["portfolio_net_profit_rupees"]))
                for row in ledger.to_dict("records")]

    for label, setup, body, wick, volume, green, orders, recent_orders, recent_rows in chosen:
        ledger = simulate(orders, paths)
        recent_ledger = simulate(recent_orders, recent_paths)
        m = metric(ledger, days) if len(ledger) else dict(selected_orders=0, fills=0,
            wins=0, losses=0, win_rate_pct=None, net_pnl_rs=0.,
            first_30=dict(fills=0, wins=0, losses=0, win_rate_pct=None, net_pnl_rs=0.),
            later_16=dict(fills=0, wins=0, losses=0, win_rate_pct=None, net_pnl_rs=0.))
        r = metric(recent_ledger, study.recent_replay.DAYS) if len(recent_ledger) else dict(
            selected_orders=0, fills=0, wins=0, losses=0, win_rate_pct=None, net_pnl_rs=0.)
        combined_fills = m["fills"] + r["fills"]
        combined_wins = m["wins"] + r["wins"]
        combined = dict(sessions=49, fills=combined_fills, wins=combined_wins,
            losses=m["losses"] + r["losses"],
            win_rate_pct=100 * combined_wins / combined_fills if combined_fills else None,
            net_pnl_rs=m["net_pnl_rs"] + r["net_pnl_rs"])
        results.append(dict(step=label, five_minute_filter=dict(price_change_pct=.30,
            oi_change_pct=.15, volume_ratio=1.30), confirmation=dict(body_ratio_min=body,
            wick_ratio_max=wick, volume_ratio_min=volume,
            direction="green_only" if green else "green_and_close_above_signal_close"),
            setup=asdict(setup), metrics=m, recent_selected=recent_rows,
            recent_replay={key: r[key] for key in ("selected_orders", "fills", "wins", "losses", "win_rate_pct", "net_pnl_rs")},
            combined_49_sessions=combined, trades=trade_rows(ledger),
            recent_trades=trade_rows(recent_ledger)))
        print(label, {key:m[key] for key in ("selected_orders", "fills", "wins", "losses",
            "win_rate_pct", "net_pnl_rs")}, "recent", r["fills"], r["wins"],
            "combined", combined["win_rate_pct"], flush=True)
    report = dict(study="Stepwise 09:51 confirmation relaxation for stricter 09:50 LONG",
        scope="New slot only; all 09:50 five-minute filters and picker fixed",
        sessions=len(days), first_date=str(days[0]), last_date=str(days[-1]),
        source_rows=len(historical), frozen_baseline_selection_parity=True,
        exit=dict(initial_stop_pct=1.25, tightened_stop_pct=1.0,
            tighten_after_minutes=120, target_pct=1.0,
            cost_bps=5., capital_per_entry_rs=100_000., leverage=5.),
        recent_sources=recent_sources, scenarios=results,
        interpretation="Exploratory thresholds tested on reused historical outcomes; later 16 sessions and Oct 8/9/12 are validation checks, not additional fitted data",
        production_modified=False)
    OUT.write_text(json.dumps(report, indent=2, allow_nan=False, default=str) + "\n", encoding="utf-8")
    print(OUT, flush=True)


if __name__ == "__main__":
    main()
