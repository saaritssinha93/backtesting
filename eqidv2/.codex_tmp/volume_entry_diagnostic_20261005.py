"""Bounded research diagnostic; does not edit or promote strategy settings."""
from __future__ import annotations

import json
import sys
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import fno_v13_v10_g_2_backtest as g2
import fno_v13_v10_g_2_extend_results as ext
import fno_v13_v10_g_daily_replay as replay

POLICIES = ("BASELINE", "LONG_FLOOR_1.00", "LONG_CONDITIONAL_0.80", "LONG_NO_VOLUME_GATE")


def select(signals, base, settings, policy):
    change = g2.g.SelectionChange(**settings["selection_change"])
    original = g2.g.select_orders(signals, base, change, core_first=True)
    original["volume_exception"] = False
    if policy == "BASELINE":
        return original
    proxy = signals.copy()
    ratio = pd.to_numeric(proxy.v9_1m_volume_ratio, errors="coerce")
    allowed = proxy.side.eq("LONG") & np.isfinite(ratio)
    if policy == "LONG_FLOOR_1.00":
        allowed &= ratio.ge(1.0)
    elif policy == "LONG_CONDITIONAL_0.80":
        allowed &= (ratio.ge(.8) & pd.to_numeric(proxy.volume_ratio).ge(1.5)
                    & pd.to_numeric(proxy.wick_ratio).le(.25))
    elif policy == "LONG_NO_VOLUME_GATE":
        allowed &= ratio.ge(0)
    else:
        raise ValueError(policy)
    proxy.loc[allowed, "v9_1m_volume_ratio"] = np.maximum(ratio[allowed], 1.2)
    audit = g2.g.selection_audit(proxy, base, change, core_first=True)
    if audit.empty:
        return original
    old_keys = set(zip(original.sid.astype(int), original.setup_id.astype(str)))
    additions = []
    setup_map = {s.setup_id: s for s in g2.g.v9.v5.profile_setups(g2.g.v9.v5.PROFILES["higher_frequency"])}
    for (day, setup_id), group in audit.groupby(["day", "setup_id"], sort=True):
        held = original.loc[original.day.eq(day) & original.setup_id.eq(setup_id)]
        core, setup = g2.g.setup_pair(setup_map[setup_id], change)
        capacity = setup.max_entries - len(held)
        if capacity <= 0:
            continue
        choices = group.loc[group.v9_filter_pass.eq(True)].copy()
        choices = choices.loc[[
            (int(row.sid), str(row.setup_id)) not in old_keys for row in choices.itertuples()
        ]].copy()
        picker = g2.g.v9.PICKER_COLUMNS[setup.picker]
        if picker == "abs_price_change_pct":
            choices[picker] = choices.price_change_pct.abs()
        choices = choices.sort_values(["v10_g_f_core", picker, "traded_value", "tradingsymbol"],
                                      ascending=[False, False, False, True], kind="stable").head(capacity)
        choices["volume_exception"] = True
        additions.append(choices)
    selected = pd.concat([original, *additions], ignore_index=True, sort=False)
    observed = signals.set_index("sid").v9_1m_volume_ratio
    selected["v9_1m_volume_ratio"] = selected.sid.map(observed)
    added = selected.loc[selected.volume_exception.eq(True)]
    assert added.empty or (added.side.eq("LONG") & added.v9_1m_volume_ratio.lt(1.2)).all()
    assert old_keys.issubset(set(zip(selected.sid.astype(int), selected.setup_id.astype(str))))
    return selected.sort_values(["day", "hhmm_int", "side", "setup_id", "tradingsymbol"], kind="stable").reset_index(drop=True)


def metric(ledger, days):
    result = g2.g.r.metric(ledger, days)
    ex = ledger.loc[ledger.portfolio_executed.eq(True)]
    p = ex.portfolio_net_profit_rupees
    losses = p[p.lt(0)]
    daily = ex.groupby(ex.day.astype(str)).portfolio_net_profit_rupees.sum().reindex(list(map(str, days)), fill_value=0)
    result.update(gross=float(ex.portfolio_gross_profit_rupees.sum()), costs=float(ex.portfolio_cost_rupees.sum()),
                  mean_loss=float(losses.mean()) if len(losses) else 0,
                  worst_trade=float(p.min()) if len(p) else 0,
                  positive_sessions=int(daily.gt(0).sum()), negative_sessions=int(daily.lt(0).sum()),
                  no_trade_sessions=int(len(days)-ex.day.nunique()), daily_net=daily.to_dict())
    return result


def main():
    sealed = g2.load_bundle(g2.DEFAULT_SOURCE_BUNDLE, g2.DEFAULT_G_CONFIG)
    base, settings = sealed["v9_config"], sealed["source_g"]
    segments = []
    s = sealed["signals"].copy()
    s["day"] = pd.to_datetime(s.day).dt.date
    choices = {rule: select(s, base, settings, rule) for rule in POLICIES}
    assert g2._selection_keys(choices["BASELINE"]) == g2._selection_keys(sealed["orders"])
    needed = set(pd.concat(list(choices.values())).sid.astype(int))
    paths = {}
    with np.load(sealed["source"] / "dataset/paths.npz", allow_pickle=False) as archive:
        for name in archive.files:
            sid, field = name.split("_", 1)
            if int(sid) in needed:
                paths.setdefault(int(sid), {})[field] = archive[name]
    segments.append(("BASE", choices, paths))
    days = list(sealed["days"])
    evidence = []
    for day in (*ext.COMPLETE_EXTENSION_DAYS, date(2026, 10, 5)):
        run, result = ext._latest_successful_run(ext.DEFAULT_DAILY_ROOT, day)
        manifest = ext._read_json(run / "source_manifest.json")
        assert manifest["frozen_config_sha256"] == g2.sha256(g2.DEFAULT_G_CONFIG)
        assert manifest["source_fingerprint"] == result["source_fingerprint"]
        snapshot_path, source_day = ext._snapshot_for_day(ext.DEFAULT_DAILY_ROOT, day, run, result)
        snapshot = ext._read_json(snapshot_path)
        assert snapshot["complete"] is True
        replay._verify_input_snapshot(snapshot_path.parent, snapshot)
        signals = pd.read_csv(run / "candidate_signals.csv")
        signals["day"] = pd.to_datetime(signals.day).dt.date
        choices = {rule: select(signals, base, settings, rule) for rule in POLICIES}
        assert g2._selection_keys(choices["BASELINE"]) == g2._selection_keys(pd.read_csv(run / "selected_orders.csv"))
        union = pd.concat(list(choices.values()), ignore_index=True).drop_duplicates("sid")
        paths = ext._selected_paths(union, day, snapshot_path.parent)
        segments.append((str(day), choices, paths))
        days.append(day)
        evidence.append(dict(day=str(day), run=str(run), snapshot=str(snapshot_path), source_day=source_day))
        print("prepared", day, {k: len(v) for k,v in choices.items()}, flush=True)
    outputs = {}
    baseline_keys = None
    for policy in POLICIES:
        frames = []
        for segment, choices, paths in segments:
            orders = ext._apply_retained_g_exits(choices[policy], settings)
            if orders.empty:
                continue
            g2.g.v9.validate_paths(orders, paths)
            frame = g2.simulate_staged(orders, paths, cost_bps=base.cost_bps, max_entry_delay_minutes=10)
            frame = g2.g.v9.v5.apply_fixed_capital_model(frame, base.capital_per_entry_rupees, base.leverage_factor)
            frame["segment"] = segment
            frames.append(frame)
        trades = pd.concat(frames, ignore_index=True, sort=False)
        ledger, portfolio = g2.g.v9.v6.apply_portfolio_constraints(trades, base.portfolio_config())
        ex = ledger.loc[ledger.portfolio_executed.eq(True)].copy()
        ex["key"] = ex.segment.astype(str) + ":" + ex.sid.astype(str) + ":" + ex.setup_id.astype(str)
        if policy == "BASELINE":
            baseline_keys = set(ex.key)
            assert len(ex) == 85 and abs(ex.portfolio_net_profit_rupees.sum()-229917.0395538971) < 1e-6
        additions = ex.loc[~ex.key.isin(baseline_keys)]
        pnl = additions.portfolio_net_profit_rupees
        gross_wins, gross_losses = pnl[pnl.gt(0)].sum(), -pnl[pnl.lt(0)].sum()
        columns = ["segment", "day", "sid", "setup_id", "tradingsymbol", "side", "entry_ts", "exit_ts", "exit_reason",
                   "v9_1m_volume_ratio", "volume_ratio", "body_ratio", "wick_ratio", "portfolio_net_profit_rupees"]
        outputs[policy] = dict(
            combined=metric(ledger, days),
            added=dict(selected=int(trades.volume_exception.eq(True).sum()), filled=len(additions),
                       wins=int(pnl.gt(0).sum()), losses=int(pnl.lt(0).sum()), net=float(pnl.sum()),
                       mean=float(pnl.mean()) if len(pnl) else 0,
                       profit_factor=float(gross_wins/gross_losses) if gross_losses else None,
                       mean_loss=float(pnl[pnl.lt(0)].mean()) if pnl.lt(0).any() else 0,
                       worst=float(pnl.min()) if len(pnl) else 0,
                       original_executions_missing=sorted(baseline_keys-set(ex.key)),
                       jul_aug_net=float(additions.loc[additions.day.astype(str).lt("2026-09-01"),"portfolio_net_profit_rupees"].sum()),
                       sep_oct_net=float(additions.loc[additions.day.astype(str).ge("2026-09-01"),"portfolio_net_profit_rupees"].sum()),
                       trades=additions[columns].to_dict("records")),
        )
        print(policy, json.dumps({k:v for k,v in outputs[policy]["added"].items() if k != "trades"}, default=str), flush=True)
    path = Path(__file__).with_suffix(".json")
    path.write_text(json.dumps(dict(status="REUSED_HISTORY_DIAGNOSTIC_NO_UNTOUCHED_HOLDOUT", days=list(map(str, days)),
                                   policies=outputs, evidence=evidence), indent=2, default=str, allow_nan=False)+"\n", encoding="utf-8")
    print("OUTPUT", path, flush=True)


if __name__ == "__main__":
    main()
