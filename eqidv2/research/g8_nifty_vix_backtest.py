"""Isolated G8 Nifty/VIX research from sealed G7 V2 orders, through 2026-10-22.

The candidate grid and promotion rules are declared before replay. Chronological
splits evaluate the overlay only: the inherited strategy was already inspected
on these dates and these are NOT independent, unseen strategy results.
"""
from __future__ import annotations

import argparse
from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import html
import json
from pathlib import Path

import numpy as np
import pandas as pd

from research import g7_g6_replica_backtest as g7
from research.g8_exit_engine import allocate_variant, metrics, replay_exits
from research.g8_vix_data import VixData, DEFAULT_5M

ROOT = Path(__file__).resolve().parents[1]
OUTPUT_ROOT = Path(r"C:\TradingData\eqidv2\backtesting_result_v13_v10_g_8")
VERSION = "V13-V10-G-8-G7V2-NIFTY-VIX-V1"
CUTOFF = "2026-10-22"
BASELINE = "nifty_base__original"


def dump(path, value):
    Path(path).write_text(json.dumps(value, indent=2, default=str, allow_nan=False) + "\n", encoding="utf-8")


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def configuration():
    config = deepcopy(g7.configuration(g7.V2_CONFIG))
    config.update(strategy="V13-V10-G-8", variant_id=VERSION,
                  replica_source_variant=g7.V2_VERSION,
                  scope="BACKTEST_ONLY", execution_authority=False)
    config["g8_research"] = {
        "through": CUTOFF,
        "entry_ranking": "Frozen V2 orders; no replacement of a vetoed ranked stock",
        "missing_vix": "FAIL_RUN",
        "vix_timestamp": "Completed 5-minute candle; start + 5 minutes <= confirmation",
        "vix_direction": "Session change and completed trailing 15-minute change; early-session fallback to session change",
        "vix_deadband_pct": [0.5, 1.0, 2.0],
        "chronology": "First 35 sessions development; next 10 validation; remaining 11 final internal test",
        "independent_holdout": False,
        "selection": "High win rate with >=90% baseline development net profit and >=65% trades; loss/drawdown guards",
        "slot_fit_minimum": 20,
        "risk": "No increased exposure; wider stops reduce size by min(1, 1.2 / stop_pct)",
        "guaranteed_profit": False,
    }
    return config


def gate_specs():
    result = [dict(id="nifty_base", mode="baseline", threshold=0.)]
    for threshold in (.5, 1., 2.):
        tag = str(threshold).replace(".", "p")
        for mode in ("stress_alignment", "long_caution", "pair_consistency"):
            result.append(dict(id=f"{mode}_{tag}", mode=mode, threshold=threshold))
    return result


def exit_specs():
    result = [dict(id="original", mode="fixed", stop=1.2, target_cap=None)]
    for stop in (.6, .9, 1.2):
        for cap in (.35, .5, .75, 1., None):
            if stop == 1.2 and cap is None:
                continue
            result.append(dict(id=f"fixed_s{stop}_t{cap or 'slot'}", mode="fixed", stop=stop, target_cap=cap))
    for mode in ("stress_wider", "vix_ratio"):
        for cap in (.5, .75, 1., None):
            result.append(dict(id=f"{mode}_t{cap or 'slot'}", mode=mode, stop=1.2, target_cap=cap))
    return result


def features(orders, audit, config, vix):
    if orders.sid.duplicated().any() or audit.sid.duplicated().any():
        raise ValueError("Duplicate feature SID")
    setups = {row["setup_id"]: row for row in config["active_setups"]}
    context = audit.set_index("sid")
    records = []
    for row in orders.itertuples():
        stamp = pd.Timestamp(row.confirmation_ts)
        setup = setups[row.setup_id]
        # One completed signal candle, not the following confirmation candle.
        slot_start = pd.Timestamp(str(row.day)[:10] + " " + setup["signal_end"]) - pd.Timedelta(minutes=5)
        vf = vix.features(stamp, slot_start)
        if vf is None:
            raise ValueError(f"Missing/stale causal VIX: {row.day}/{row.setup_id}")
        n = context.loc[row.sid]
        direction = 1 if row.side == "LONG" else -1
        slot, session = float(n.slot_candle), float(n.session_open)
        base = bool(slot * direction >= 0 or session * direction >= 0)
        if base != bool(n.both_counter_allowed):
            raise ValueError("Inherited Nifty decision mismatch")
        recent = vf["vix_15m_pct"]
        records.append(dict(sid=int(row.sid), day=str(row.day)[:10], setup_id=row.setup_id,
            side=row.side, tradingsymbol=row.tradingsymbol, confirmation_ts=str(stamp),
            nifty_slot_pct=slot, nifty_session_pct=session,
            nifty_base_allowed=base,
            nifty_both_aligned=bool(slot * direction > 0 and session * direction > 0),
            early_vix_fallback=recent is None,
            vix_recent_pct=vf["vix_session_pct"] if recent is None else recent,
            baseline_stop_pct=setup["stop_pct"], baseline_target_pct=setup["target_pct"], **vf))
    frame = pd.DataFrame(records).set_index("sid", drop=False)
    if set(frame.index) != set(orders.sid):
        raise ValueError("Incomplete features")
    return frame


def gate_decisions(frame, spec):
    base = frame.nifty_base_allowed.astype(bool)
    if spec["mode"] == "baseline":
        return base.copy()
    threshold = spec["threshold"]
    rising = (frame.vix_session_pct >= threshold) & (frame.vix_recent_pct >= threshold)
    falling = (frame.vix_session_pct <= -threshold) & (frame.vix_recent_pct <= -threshold)
    if spec["mode"] == "stress_alignment":
        return base & (~rising | frame.nifty_both_aligned)
    if spec["mode"] == "long_caution":
        return base & (~(rising & frame.side.eq("LONG")) | frame.nifty_both_aligned)
    if spec["mode"] == "pair_consistency":
        # Deliberately strict experiment, not an assumed VIX->price law.
        return base & ~(rising & frame.side.eq("LONG")) & ~(falling & frame.side.eq("SHORT"))
    raise ValueError("Unknown gate")


def exit_parameters(frame, spec):
    stop = pd.Series(float(spec["stop"]), index=frame.index)
    target = frame.baseline_target_pct.astype(float).copy()
    if spec["target_cap"] is not None:
        target = target.clip(upper=float(spec["target_cap"]))
    if spec["mode"] == "stress_wider":
        stress = (frame.vix_session_pct >= 1.) & (frame.vix_recent_pct >= .5)
        stop = stop * np.where(stress, 1.25, 1.)
    elif spec["mode"] == "vix_ratio":
        if frame.vix_previous_close_pct.isna().any():
            raise ValueError("Missing prior-session VIX close")
        ratio = (1. + frame.vix_previous_close_pct / 100.).clip(.85, 1.25)
        stop, target = stop * ratio, target * ratio
    elif spec["mode"] != "fixed":
        raise ValueError("Unknown exit mode")
    return stop, target


def measured(book, days, config):
    m = metrics(book, days)
    m["return_on_starting_capital_pct"] = 100. * m["net_pnl_rs"] / config["portfolio_capital_rs"]
    return m


def choose(records):
    """Predeclared selection, using only the supplied past-period aggregates."""
    table = pd.DataFrame(records).set_index("policy", drop=False)
    b = table.loc[BASELINE]
    qualifying = table.loc[
        (table.net_pnl_rs >= max(0., .9 * b.net_pnl_rs)) &
        (table.trades >= .65 * b.trades) &
        (table.win_rate_pct >= b.win_rate_pct) &
        (table.negative_trade_sum_abs_rs <= b.negative_trade_sum_abs_rs + 1e-6) &
        (table.daily_close_drawdown_rs <= max(1., 1.15 * b.daily_close_drawdown_rs)) &
        (table.losing_days <= b.losing_days)]
    if qualifying.empty:
        return BASELINE
    return str(qualifying.reset_index(drop=True).sort_values(
        ["win_rate_pct", "losing_days", "net_pnl_rs", "policy"],
        ascending=[False, True, False, True]).iloc[0].policy)


def validation_pass(candidate, baseline):
    return bool(candidate["net_pnl_rs"] >= .9 * baseline["net_pnl_rs"] and
        candidate["net_pnl_rs"] > 0 and candidate["trades"] >= .65 * baseline["trades"] and
        candidate["win_rate_pct"] >= baseline["win_rate_pct"] - 2 and
        candidate["daily_close_drawdown_rs"] <= max(1., 1.15 * baseline["daily_close_drawdown_rs"]) and
        candidate["losing_days"] <= baseline["losing_days"])


def slot_table(config, selected_spec=None, features_frame=None, development=None):
    exit_spec = selected_spec[1] if selected_spec else exit_specs()[0]
    gate = selected_spec[0]["id"] if selected_spec else "candidate_grid_pending"
    rows = []
    for setup in sorted(config["active_setups"], key=lambda s: s["setup_id"]):
        t = setup["confirmation_end"]
        band = "opening" if t <= "09:31" else "morning" if t <= "10:56" else "late_morning" if t <= "12:36" else "afternoon"
        target = min(setup["target_pct"], exit_spec["target_cap"]) if exit_spec["target_cap"] else setup["target_pct"]
        count = 0 if features_frame is None else int(((features_frame.setup_id == setup["setup_id"]) & features_frame.day.isin(development)).sum())
        rows.append(dict(setup_id=setup["setup_id"], slot=t, side=setup["side"], band=band,
            baseline_stop_pct=setup["stop_pct"], baseline_target_pct=setup["target_pct"],
            selected_gate=gate, selected_exit=exit_spec["id"],
            reference_stop_pct=exit_spec["stop"], reference_target_pct=target,
            stress_stop_pct=None if exit_spec["mode"] == "vix_ratio" else exit_spec["stop"] * (1.25 if exit_spec["mode"] == "stress_wider" else 1.),
            stress_target_pct=None if exit_spec["mode"] == "vix_ratio" else target,
            minimum_stop_pct=exit_spec["stop"] * (.85 if exit_spec["mode"] == "vix_ratio" else 1.),
            maximum_stop_pct=exit_spec["stop"] * (1.25 if exit_spec["mode"] in ("stress_wider", "vix_ratio") else 1.),
            minimum_target_pct=target * (.85 if exit_spec["mode"] == "vix_ratio" else 1.),
            maximum_target_pct=target * (1.25 if exit_spec["mode"] == "vix_ratio" else 1.),
            dynamic_formula="SL and target x clip(current_VIX/previous_close,0.85,1.25)" if exit_spec["mode"] == "vix_ratio" else "fixed at entry",
            timed_stop="after 120min, min(initial stop,1.00%)", square_off="15:15",
            development_selected_orders=count,
            calibration="SHARED_PARAMETERS_NO_SLOT_OPTIMIZATION",
            sample_warning="Limited slot evidence" if count < 20 else "Shared rule; no independent slot validation"))
    return pd.DataFrame(rows)


def plan(output=None):
    config = configuration()
    output = Path(output or OUTPUT_ROOT / "plans" / "2026-10-22")
    output.mkdir(parents=True, exist_ok=True)
    dump(output / "g8_copied_configuration.json", config)
    dump(output / "predeclared_experiment.json", dict(gates=gate_specs(), exits=exit_specs(),
        variants=len(gate_specs())*len(exit_specs()), selection=config["g8_research"],
        warning="No combination guarantees a profitable trade or day"))
    slot_table(config).to_csv(output / "all_51_slot_plan.csv", index=False)
    return output


def _table(frame, columns=None):
    return frame[columns].to_html(index=False, float_format=lambda x: f"{x:,.2f}") if columns else frame.to_html(index=False, float_format=lambda x: f"{x:,.2f}")


def report(output, summary, comparisons, slots, daily, trials):
    columns = ["label", "period", "trades", "wins", "losses", "win_rate_pct", "net_pnl_rs", "losing_days", "daily_close_drawdown_rs"]
    body = f"""<!doctype html><html><head><meta charset="utf-8"><title>G8 Nifty + VIX through {CUTOFF}</title>
    <style>body{{font:15px system-ui;margin:32px;color:#172b40}}h1,h2{{color:#173e63}}table{{border-collapse:collapse;font-size:13px;white-space:nowrap}}td,th{{padding:7px 10px;border:1px solid #d8e0e7;text-align:right}}th{{background:#e8f1fa}}.scroll{{overflow:auto}}.warning{{padding:16px;background:#fff2d3}}code{{background:#eef3f7}}</style></head><body>
    <h1>G8: Nifty direction + India VIX</h1><p>G7 V2 copied; single exit, not G7 V3. Data through {CUTOFF}. Backtest only; no live or paper changes.</p>
    <div class="warning">No definite profits: higher win rate can sacrifice net profit and still leave losing days.
    Inherited targets and entry rules were already inspected on this history. These chronological splits are internal robustness checks, not an untouched holdout.</div>
    <h2>Decision</h2><p>Development-selected: <code>{html.escape(summary['development_selected'])}</code>.
    Validation passed: <b>{summary['validation_passed']}</b>. Research default: <code>{html.escape(summary['selected_policy'])}</code>.
    Best full-sample profit and win-rate variants below are exploratory, not automatically selected.</p>
    <h2>Comparison</h2><div class="scroll">{_table(comparisons,columns)}</div>
    <h2>Entry logic</h2><p>Every policy first rejects a trade when BOTH completed Nifty-futures slot and session directions oppose it.
    VIX rising/falling means both the current-session move and the last 15-minute move exceed the tested deadband.
    At early slots without 15 minutes, the session move is used and flagged. Mixed/flat VIX is neutral.
    Stress-alignment requires both Nifty directions to agree during rising VIX; long-caution applies that only to longs;
    pair-consistency rejects longs during rising VIX and shorts during falling VIX. The last is a hypothesis, not a directional law.</p>
    <h2>Exit and risk logic</h2><p>Fixed targets are capped at 0.35/0.50/0.75/1.00% or retain the original slot target; SL 0.60/0.90/1.20%.
    Stress-wider sets SL to 1.50% when session VIX is up at least 1% and recent VIX up at least 0.5%, without raising the target.
    VIX-ratio multiplies SL and target by current VIX / previous session close, clipped to 0.85–1.25.
    A widened stop reduces exposure; it never increases leverage. These are index-volatility proxies, not stock-specific ATR estimates.</p>
    <h2>All 51 slot settings for the selected policy</h2><p>Settings are shared across slots, with original slot targets retained or capped.
    No 51-way per-slot search: small cell sizes cannot justify that precision. Actual per-trade percentages are in the ledger.</p>
    <div class="scroll">{_table(slots)}</div>
    <h2>Day-wise selected policy versus baseline</h2><div class="scroll">{_table(daily)}</div>
    <h2>Validation and accounting</h2><p>{len(trials)//4} predefined variants; development first35 sessions, validation next10, final test last11.
    A candidate needs at least90% development net profit,65% trade retention, no worse win rate, no extra losing days,
    no more aggregate trade losses, and drawdown within115% of baseline. Rank by win rate, then losing days, then net profit.
    Validation requires positive profit,90% profit retention,65% trades, win rate within2 percentage points, no extra losing days, drawdown within115%.
    Failed validation reverts to baseline. Final-test results never select parameters. Walk-forward refits use past sessions only, with a 7-session validation tail.</p>
    <p>Model capital INR1,000,000, INR100,000 per position,5x exposure, at most10 positions; fractional theoretical quantities.
    Round-trip cost5bps, with10/15bps sensitivity. One-minute replay: stop first on ambiguous bars, observed-open stop gaps,
    capital released when modeled exit timestamp is at/before a later entry timestamp,120-minute stop tightening,15:15 square-off.
    Drawdown is daily-close drawdown, not intraday mark-to-market drawdown. No same-candle ordering certainty or actual fill guarantee.</p>
    <p>Coverage:56 eligible sessions. 1 October lacks complete sealed source inputs;20 historical sessions lack valid opening OI for some opening setups.
    All other inherited selection/data limitations remain. Every VIX decision uses completed bars available by confirmation.
    India VIX measures expected volatility over30 calendar days, not a guaranteed direction or a stock-specific stop distance:
    <a href="https://www.nseindia.com/static/products-services/indices-indiavix-index">NSE methodology overview</a>.</p>
    <h2>Files</h2><p><a href="overall_comparison.csv">Overall</a> | <a href="daywise_comparison.csv">Day-wise</a> |
    <a href="slot_side_comparison.csv">Slot and side</a> | <a href="parameter_trials.csv">All trials</a> |
    <a href="slot_settings.csv">51 slot settings</a> | <a href="cost_sensitivity.csv">Cost sensitivity</a> |
    <a href="walk_forward_choices.csv">Walk-forward choices</a> | <a href="vix_nifty_decisions.csv">Causal decisions</a> |
    <a href="summary.json">Provenance and caveats</a></p></body></html>"""
    (output / "report.html").write_text(body, encoding="utf-8")


def run(through=CUTOFF, output=None, policy_override=None):
    if through != CUTOFF:
        raise ValueError(f"Sealed experiment supports through={CUTOFF} only; other dates require a new input audit")
    from research.g8_sources import load_sources, _same_native
    config = configuration()
    output = Path(output or OUTPUT_ROOT / "runs" / CUTOFF / (datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%f") + "_g8_nifty_vix"))
    output.mkdir(parents=True, exist_ok=False)
    code_paths = [Path(__file__), ROOT / "research/g8_exit_engine.py", ROOT / "research/g8_sources.py", ROOT / "research/g8_vix_data.py"]
    code_hashes = {str(path.relative_to(ROOT)): sha(path) for path in code_paths}
    plan(output / "plan")
    print("Loading and verifying frozen G7 V2 inputs", flush=True)
    source = load_sources()
    if source["config"] != g7.configuration(g7.V2_CONFIG):
        raise ValueError("Current V2 configuration differs from copied, sealed baseline")
    original, paths, days = source["unfiltered"], source["paths"], sorted(source["days"])
    if days[-1] != CUTOFF or len(days) != 56:
        raise ValueError("Unexpected date universe")
    vix_provenance = json.loads((DEFAULT_5M.parent / "india_vix_provenance.json").read_text(encoding="utf-8"))
    vix_hash = sha(DEFAULT_5M)
    if vix_hash != vix_provenance["files"][DEFAULT_5M.name]:
        raise ValueError("VIX input no longer matches independently validated provenance")
    # Freeze the exact bytes before constructing any feature, not just a hash at run end.
    frozen_vix = output / "india_vix_5minute.csv"
    frozen_vix.write_bytes(DEFAULT_5M.read_bytes())
    if sha(frozen_vix) != vix_hash:
        raise ValueError("VIX changed during snapshot")
    frame = features(original, source["audit"], config, VixData(frozen_vix))
    frame.to_csv(output / "vix_nifty_decisions.csv", index=False)
    splits = dict(development=days[:35], validation=days[35:45], final_test=days[45:], all=days)
    gates, exits = gate_specs(), exit_specs()
    books, specs, replayed, trials = {}, {}, {}, []
    baseline_stats = measured(source["baseline"], days, config)
    for number, spec in enumerate(exits):
        print(f"Replay exit {number+1}/{len(exits)}: {spec['id']}", flush=True)
        stops, targets = exit_parameters(frame, spec)
        replay = replay_exits(original, paths, stops, targets, config, risk_scale=True)
        if spec["id"] == "original":
            _same_native(replay, original)
        replayed[spec["id"]] = replay
        for gate in gates:
            name = gate["id"] + "__" + spec["id"]
            book = allocate_variant(replay, gate_decisions(frame, gate), config, name)
            if name == BASELINE:
                g7.bounded.history._same_allocation(book, source["baseline"])
            books[name], specs[name] = book, (gate, spec)
            for period, calendar in splits.items():
                trials.append(dict(policy=name, period=period, gate=gate["id"], exit=spec["id"], **measured(book, calendar, config)))
        # Checkpoints remain local to this run. They are not a publication pointer.
        pd.DataFrame(trials).to_csv(output / "parameter_trials.csv", index=False)
    baseline = books[BASELINE]
    for field in ("trades", "wins", "losses", "net_pnl_rs", "losing_days", "daily_close_drawdown_rs"):
        if not np.isclose(measured(baseline, days, config)[field], baseline_stats[field], atol=1e-6, rtol=0):
            raise ValueError("G7 V2 baseline does not reproduce: " + field)
    baseline_stats = measured(baseline, days, config)
    trial_frame = pd.DataFrame(trials)
    trial_frame.to_csv(output / "parameter_trials.csv", index=False)
    train_records = trial_frame.loc[trial_frame.period.eq("development")].to_dict("records")
    candidate = choose(train_records)
    passes = validation_pass(measured(books[candidate], splits["validation"], config), measured(baseline, splits["validation"], config))
    selected = candidate if passes else BASELINE
    if policy_override is not None:
        if policy_override not in books:
            raise ValueError("Unknown explicit policy override")
        selected = policy_override
    all_rows = trial_frame.loc[trial_frame.period.eq("all")]
    best_net = str(all_rows.sort_values(["net_pnl_rs", "win_rate_pct"], ascending=False).iloc[0].policy)
    eligible_hit = all_rows.loc[(all_rows.trades >= .65 * baseline_stats["trades"]) & (all_rows.net_pnl_rs >= .6 * baseline_stats["net_pnl_rs"])]
    best_hit = str(eligible_hit.sort_values(["win_rate_pct", "net_pnl_rs"], ascending=False).iloc[0].policy)
    labels = {"G7_V2_baseline": BASELINE, "development_candidate": candidate, "G8_selected": selected,
              "exploratory_best_net": best_net, "exploratory_high_win_rate": best_hit}
    comparisons = []
    for label, name in labels.items():
        for period, calendar in splits.items():
            comparisons.append(dict(label=label, policy=name, period=period, **measured(books[name], calendar, config)))
        folder = output / label
        folder.mkdir()
        books[name].to_csv(folder / "portfolio_trades.csv", index=False)
    # Genuine past-only overlay fitting; still internally reused inherited history.
    walk, choices = [], []
    for start in range(28, len(days), 7):
        past_train, past_validation, future = days[:start-7], days[start-7:start], days[start:start+7]
        fit = choose([dict(policy=name, **measured(book, past_train, config)) for name, book in books.items()])
        accepted = validation_pass(measured(books[fit], past_validation, config), measured(baseline, past_validation, config))
        use = fit if accepted else BASELINE
        walk.append(books[use].loc[books[use].day.isin(future)].copy())
        choices.append(dict(training_through=past_train[-1], validation_from=past_validation[0],
            validation_through=past_validation[-1], test_from=future[0], test_through=future[-1],
            fitted_policy=fit, validation_passed=accepted, selected_policy=use))
    walk_book = pd.concat(walk, ignore_index=True)
    walk_book.to_csv(output / "walk_forward_trades.csv", index=False)
    pd.DataFrame(choices).to_csv(output / "walk_forward_choices.csv", index=False)
    for label, book in (("past_only_walk_forward", walk_book), ("matched_walk_forward_baseline", baseline)):
        comparisons.append(dict(label=label, policy="varies" if label.startswith("past") else BASELINE, period="walk_forward_last28", **measured(book, days[28:], config)))
    comparisons = pd.DataFrame(comparisons)
    comparisons.to_csv(output / "overall_comparison.csv", index=False)
    daily_rows, slot_rows, side_rows, cost_rows = [], [], [], []
    for label, name in labels.items():
        book = books[name]
        for day in days:
            daily_rows.append(dict(label=label, day=day, **measured(book, [day], config)))
        for setup in config["active_setups"]:
            slot_rows.append(dict(label=label, setup_id=setup["setup_id"], slot=setup["confirmation_end"], side=setup["side"],
                **measured(book.loc[book.setup_id.eq(setup["setup_id"])], days, config)))
        for side in ("LONG", "SHORT"):
            side_rows.append(dict(label=label, side=side, **measured(book.loc[book.side.eq(side)], days, config)))
        gate, ex = specs[name]
        for cost in (5., 10., 15.):
            stress_book = allocate_variant(replayed[ex["id"]], gate_decisions(frame, gate), config, name, cost_bps=cost)
            for period in ("all", "final_test"):
                cost_rows.append(dict(label=label, period=period, cost_bps=cost, **measured(stress_book, splits[period], config)))
    daily_long = pd.DataFrame(daily_rows)
    daily_long.to_csv(output / "daywise_all_policies.csv", index=False)
    daily = daily_long.pivot(index="day", columns="label", values=["trades", "win_rate_pct", "net_pnl_rs"])
    daily.columns = [f"{label}_{metric}" for metric, label in daily.columns]
    daily = daily.reset_index()
    daily["selected_net_change_rs"] = daily.G8_selected_net_pnl_rs - daily.G7_V2_baseline_net_pnl_rs
    daily.to_csv(output / "daywise_comparison.csv", index=False)
    pd.DataFrame(slot_rows).to_csv(output / "slot_side_comparison.csv", index=False)
    pd.DataFrame(side_rows).to_csv(output / "long_short_comparison.csv", index=False)
    pd.DataFrame(cost_rows).to_csv(output / "cost_sensitivity.csv", index=False)
    slots = slot_table(config, specs[selected], frame, splits["development"])
    slots.to_csv(output / "slot_settings.csv", index=False)
    # Descriptive 4-way Nifty/VIX bins: these outcomes never choose gates.
    regime = baseline.merge(frame[["sid", "vix_session_pct", "vix_recent_pct", "nifty_slot_pct"]].reset_index(drop=True), on="sid", validate="one_to_one")
    regime["nifty_direction"] = np.where(regime.nifty_slot_pct > 0, "UP", np.where(regime.nifty_slot_pct < 0, "DOWN", "FLAT"))
    regime["vix_direction"] = np.where(regime.vix_recent_pct >= .5, "UP", np.where(regime.vix_recent_pct <= -.5, "DOWN", "NEUTRAL"))
    pd.DataFrame([dict(nifty_direction=n, vix_direction=v, side=s, **measured(group, days, config))
        for (n,v,s),group in regime.groupby(["nifty_direction", "vix_direction", "side"])]).to_csv(output / "regime_outcomes.csv", index=False)
    config["g8_selected_policy"] = dict(id=selected, gate=specs[selected][0], exit=specs[selected][1], validation_passed=passes)
    config["rollback"] = dict(policy=BASELINE, baseline_folder="G7_V2_baseline", command="python fno_v13_v10_g_8_backtest.py --policy nifty_base__original")
    dump(output / "config.json", config)
    summary = dict(variant=VERSION, through=CUTOFF, first_day=days[0], sessions=len(days),
        candidate_count=len(books), development_selected=candidate, validation_passed=passes, selected_policy=selected,
        exploratory_best_net=best_net, exploratory_high_win_rate=best_hit, splits=splits,
        selected_metrics=measured(books[selected], days, config), baseline_metrics=baseline_stats,
        provenance=source["provenance"], vix_sha256=vix_hash, vix_provenance=vix_provenance,
        source_cache_manifest_sha256=source["manifest_sha256"], manual_policy_override=policy_override,
        early_vix_fallback_orders=int(frame.early_vix_fallback.sum()),
        independent_holdout=False, execution_authority=False, scope="BACKTEST_ONLY",
        caveats=["Inherited configuration was tuned after inspecting these historical dates.",
          "Overlay selection is chronological; final test is not used for selection but is not independent of inherited strategy design.",
          "51 slots are configured; many have too few observations for separate optimization.",
          "Reported costs are modeled round-trip bps, not an audited broker charge schedule.",
          "Intrabar path is unknown; conservative stop-first conventions; fractional model quantities; daily-close drawdown only.",
          "No live or paper settings/pointers were changed. No profit guarantee."])
    dump(output / "summary.json", summary)
    report(output, summary, comparisons, slots, daily, trial_frame)
    if sha(frozen_vix) != vix_hash or any(sha(ROOT / path) != digest for path, digest in code_hashes.items()):
        raise ValueError("Inputs or implementation changed during the run; publication refused")
    files = {str(path.relative_to(output)).replace("\\", "/"): sha(path) for path in output.rglob("*") if path.is_file()}
    dump(output / "manifest.json", dict(files=files, code=code_hashes))
    verify(output)
    dump(OUTPUT_ROOT / "latest_research.json", dict(run_dir=str(output), manifest_sha256=sha(output / "manifest.json"), selected_policy=selected, execution_authority=False))
    print(json.dumps(dict(output=str(output), **{k:summary[k] for k in ("selected_policy", "validation_passed", "selected_metrics", "exploratory_best_net", "exploratory_high_win_rate")}), indent=2), flush=True)
    return output


def verify(output):
    output = Path(output).resolve()
    manifest = json.loads((output / "manifest.json").read_text(encoding="utf-8"))
    for name, digest in manifest["files"].items():
        member = (output / name).resolve()
        if not member.is_relative_to(output) or sha(member) != digest:
            raise ValueError("Changed result artifact: " + name)
    for name, digest in manifest["code"].items():
        member = (ROOT / name).resolve()
        if not member.is_relative_to(ROOT) or sha(member) != digest:
            raise ValueError("Changed implementation: " + name)
    summary = json.loads((output / "summary.json").read_text(encoding="utf-8"))
    frame = pd.read_csv(output / "vix_nifty_decisions.csv")
    if not (pd.to_datetime(frame.vix_available_at, utc=True) <= pd.to_datetime(frame.vix_decision_at, utc=True)).all():
        raise ValueError("Future VIX information")
    if frame.day.max() > CUTOFF:
        raise ValueError("Beyond-cutoff input")
    for label in ("G7_V2_baseline", "G8_selected", "development_candidate", "exploratory_best_net", "exploratory_high_win_rate"):
        book = pd.read_csv(output / label / "portfolio_trades.csv")
        stats = metrics(book, summary["splits"]["all"])
        comparisons = pd.read_csv(output / "overall_comparison.csv")
        expected = comparisons.loc[comparisons.label.eq(label) & comparisons.period.eq("all")].iloc[0]
        for key in ("trades", "wins", "losses", "net_pnl_rs", "gross_pnl_rs", "cost_rs"):
            if not np.isclose(stats[key], expected[key], atol=1e-6, rtol=0):
                raise ValueError("Ledger reconciliation failed: " + label + "/" + key)
        if not np.isclose(stats["gross_pnl_rs"]-stats["cost_rs"], stats["net_pnl_rs"], atol=1e-6):
            raise ValueError("Gross/cost/net reconciliation")
    return True


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--through", default=CUTOFF)
    parser.add_argument("--plan", action="store_true")
    parser.add_argument("--output")
    parser.add_argument("--verify")
    parser.add_argument("--policy", help="Explicit research-only policy override; use nifty_base__original to roll back G8")
    args = parser.parse_args()
    if args.verify:
        print("verified", verify(args.verify))
    elif args.plan:
        print(plan(args.output))
    else:
        run(args.through, args.output, args.policy)


if __name__ == "__main__":
    main()
