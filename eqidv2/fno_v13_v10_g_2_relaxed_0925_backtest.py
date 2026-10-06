"""Isolated hindsight-tuned 09:25 LONG exception. No live execution authority."""
from __future__ import annotations

import argparse
import json
from datetime import date, datetime
from pathlib import Path

import numpy as np
import pandas as pd

import fno_v13_v10_g_2_backtest as g2
import fno_v13_v10_g_2_extend_results as ext
import fno_v13_v10_g_daily_replay as replay
import fno_v13_v10_g_live_config as config

DEFAULT_RUN = Path("C:/TradingData/eqidv2/backtesting_result_v13_v10_g/runs/2026-10-05/20261005T162013242339")
OUTPUT_ROOT = Path("C:/TradingData/eqidv2/fno_oi/strategy_research/v13_g2_relaxed_0925")
SETUP_ID = "0926_LONG"
PARAMETERS = dict(oi_max_pct=1.20, volume_ratio_min=1.75, body_ratio_min=.54,
                  ignore_ema_alignment=True, scope="09:25 LONG ONLY",
                  other_setups="UNCHANGED", targets="UNCHANGED", ranking="CORE_FIRST_THEN_MAX_LIQUIDITY",
                  execution_authority=False, evidence="HINDSIGHT_TUNED_TO_KALYANKJIL_OCT5_NOT_VALIDATED")


def eligibility(ledger: pd.DataFrame) -> pd.DataFrame:
    """Re-evaluate raw features, never trust false downstream short-circuit flags."""
    setup = config.setup_for("09:25", "LONG")
    out = ledger.loc[ledger.signal_end.eq("09:25")].copy().reset_index(drop=True)
    numeric = ["oi", "prev_oi", "oi_change_pct", "price_change_pct", "volume_ratio",
               "signal_close", "confirmation_open", "confirmation_high", "confirmation_low",
               "confirmation_close", "body_ratio", "v9_1m_upper_wick_ratio",
               "v9_1m_volume_ratio", "traded_value"]
    out[numeric] = out[numeric].apply(pd.to_numeric, errors="coerce")
    out["signal_ts"] = pd.to_datetime(out.signal_ts, utc=True).dt.tz_convert("Asia/Kolkata")
    out["confirmation_ts"] = pd.to_datetime(out.confirmation_ts, utc=True).dt.tz_convert("Asia/Kolkata")
    checks = dict(
        finite=np.isfinite(out[numeric]).all(axis=1),
        exact_clock=out.signal_ts.dt.strftime("%H:%M:%S.%f").eq("09:25:00.000000")
                    & out.confirmation_ts.sub(out.signal_ts).eq(pd.Timedelta(minutes=1)),
        oi_increasing=out.prev_oi.gt(0) & out.oi.gt(out.prev_oi),
        oi_min=out.oi_change_pct.ge(max(config.BASE_OI_CHANGE_PCT, setup.oi_change_pct)),
        oi_max=out.oi_change_pct.le(PARAMETERS["oi_max_pct"]),
        price=out.price_change_pct.ge(max(config.BASE_PRICE_CHANGE_PCT, setup.price_change_pct)),
        volume=out.volume_ratio.ge(max(config.BASE_VOLUME_RATIO, PARAMETERS["volume_ratio_min"])),
        confirmation_range=out.confirmation_high.gt(out.confirmation_low),
        confirmation_direction=out.confirmation_close.gt(out.confirmation_open)
                               & out.confirmation_close.gt(out.signal_close),
        body=out.body_ratio.between(PARAMETERS["body_ratio_min"], 1.),
        wick=out.v9_1m_upper_wick_ratio.between(0., setup.max_wick_ratio),
        one_minute_volume=out.v9_1m_volume_ratio.ge(config.MIN_CONFIRMATION_VOLUME_RATIO),
        liquidity=out.traded_value.ge(setup.min_traded_value),
    )
    for key, value in checks.items():
        out[f"relaxed_gate_{key}"] = value.fillna(False)
    mask = pd.DataFrame(checks).fillna(False)
    out["relaxed_pass"] = mask.all(axis=1)
    out["relaxed_failed_rules"] = mask.apply(lambda row: ";".join(row.index[~row]), axis=1)
    out["original_core_pass"] = (out.relaxed_pass & out.oi_change_pct.le(config.MAX_OI_CHANGE_PCT)
                                & out.volume_ratio.ge(setup.volume_ratio) & out.body_ratio.ge(setup.body_ratio)
                                & out.ema9.gt(out.ema20) & out.ema20.gt(out.ema50))
    return out


def rank_orders(audit: pd.DataFrame, first_sid: int) -> tuple[pd.DataFrame, pd.DataFrame]:
    out = audit.copy()
    eligible = out.loc[out.relaxed_pass].sort_values(
        ["original_core_pass", "traded_value", "tradingsymbol"], ascending=[False, False, True], kind="stable")
    out["relaxed_rank"] = pd.Series(range(1, len(eligible)+1), index=eligible.index)
    quota = config.setup_for("09:25", "LONG").max_entries
    out["relaxed_selected"] = out.index.isin(eligible.head(quota).index)
    # Keep old gate diagnostics clearly distinct from this experiment's gates.
    for field in ("final_selected", "selection_decision", "first_failed_gate", "failed_gates"):
        if field in out:
            out = out.rename(columns={field: f"original_{field}"})
    orders = out.loc[out.relaxed_selected].copy()
    orders["sid"] = np.arange(first_sid, first_sid+len(orders))
    orders["day"] = orders.signal_ts.dt.date
    orders["side"] = "LONG"
    orders["setup_id"] = SETUP_ID
    orders["hhmm_int"] = 925
    orders["trigger"] = orders.confirmation_high
    orders["wick_ratio"] = orders.v9_1m_upper_wick_ratio
    orders["v9_1m_feature_ts"] = orders.confirmation_ts
    return orders.reset_index(drop=True), out


def simulate(orders, day, snapshot, source):
    orders = orders.copy()
    orders["native_target_pct"] = orders.setup_id.map({k: v["target_pct"] for k,v in source["exit"]["setups"].items()})
    if orders.native_target_pct.isna().any():
        raise ValueError("Missing original target; no fallback permitted")
    paths = ext._selected_paths(orders, day, snapshot)
    trades = g2.simulate_staged(orders, paths, cost_bps=source["cost_bps"], max_entry_delay_minutes=10)
    for c, default in (("filled", False), ("entry_ts", pd.NaT), ("exit_ts", pd.NaT),
                       ("gross_return_pct", np.nan), ("net_return_pct", np.nan), ("cost_pct", np.nan)):
        if c not in trades:
            trades[c] = default
    base = ext._portfolio_config(source)
    trades = g2.g.v9.v5.apply_fixed_capital_model(trades, base.capital_per_entry_rupees, base.leverage_factor)
    ledger, _ = g2.g.v9.v6.apply_portfolio_constraints(trades, base.portfolio_config())
    sign = np.where(ledger.side.eq("LONG"), -1., 1.)
    for c, pct in (("initial_stop_price", g2.INITIAL_STOP_PCT),
                   ("tightened_stop_price", g2.TIGHTENED_STOP_PCT)):
        ledger[c] = ledger.get("entry_price", pd.Series(np.nan, index=ledger.index))*(1+sign*pct/100)
    done = ledger.loc[ledger.portfolio_executed.eq(True)]
    p = done.portfolio_net_profit_rupees
    summary = dict(orders=len(orders), fills=len(done), wins=int(p.gt(0).sum()), losses=int(p.lt(0).sum()),
                   win_rate_pct=float(p.gt(0).mean()*100) if len(done) else None,
                   gross_profit_rupees=float(done.portfolio_gross_profit_rupees.sum()),
                   cost_rupees=float(done.portfolio_cost_rupees.sum()), net_profit_rupees=float(p.sum()))
    return ledger, summary


def run(source_run: Path, output: Path):
    if output.exists():
        raise FileExistsError("Fresh output directory required")
    result = g2.read_json(source_run / "replay_result.json")
    if not result.get("complete") or result.get("state") != "SUCCESS":
        raise ValueError("Incomplete source")
    day = date.fromisoformat(result["session_date"])
    source = config.load_frozen_config()
    snapshot_file = Path(result["artifacts"]["input_snapshot_manifest"])
    manifest = g2.read_json(snapshot_file)
    if manifest.get("complete") is not True or manifest["session_date"] != str(day):
        raise ValueError("Snapshot day mismatch")
    replay._verify_input_snapshot(snapshot_file.parent, manifest)
    feature_path = source_run / "feature_ledger.csv"
    feature_manifest = g2.read_json(source_run / "feature_ledger.csv.manifest.json")
    if g2.sha256(feature_path) != feature_manifest["artifact_sha256"]:
        raise ValueError("Recorded feature ledger hash mismatch")
    features = pd.read_csv(feature_path)
    if not features.session_date.eq(str(day)).all():
        raise ValueError("Feature day mismatch")
    candidates = pd.read_csv(source_run / "candidate_signals.csv")
    candidates["day"] = pd.to_datetime(candidates.day).dt.date
    base = ext._portfolio_config(source)
    baseline = g2.g.select_orders(candidates, base, g2.g.SelectionChange(**source["selection_change"]), core_first=True)
    official = pd.read_csv(source_run / "selected_orders.csv")
    if g2._selection_keys(baseline) != g2._selection_keys(official):
        raise ValueError("Baseline selection parity failure")
    selected, audit = rank_orders(eligibility(features), int(candidates.sid.max())+1 if len(candidates) else 0)
    unchanged = baseline.loc[baseline.setup_id.ne(SETUP_ID)]
    parts = [frame for frame in (unchanged, selected) if not frame.empty]
    relaxed = (pd.concat(parts, ignore_index=True, sort=False) if parts else baseline.iloc[:0].copy()).sort_values(
        ["day", "hhmm_int", "side", "setup_id", "tradingsymbol"]).reset_index(drop=True)
    output.mkdir(parents=True)
    summaries = {}
    for name, orders in (("BASELINE_G2", baseline), ("RELAXED_0925_LONG", relaxed)):
        ledger, metrics = simulate(orders, day, snapshot_file.parent, source)
        summaries[name] = metrics
        orders.to_csv(output / f"{name.lower()}_orders.csv", index=False)
        ledger.to_csv(output / f"{name.lower()}_trades.csv", index=False)
        print(name, json.dumps(metrics), flush=True)
        if name == "RELAXED_0925_LONG":
            cols = [c for c in ("tradingsymbol", "entry_ts", "entry_price", "initial_stop_price", "tightened_stop_price",
                                "native_target_pct", "exit_ts", "exit_price", "exit_reason", "portfolio_net_profit_rupees") if c in ledger]
            print(ledger[cols].to_string(index=False), flush=True)
    audit.to_csv(output / "all_0925_long_checks.csv", index=False)
    audit.loc[audit.relaxed_pass].sort_values("relaxed_rank").to_csv(output / "eligible_ranked_candidates.csv", index=False)
    pd.DataFrame([dict(day=str(day), experiment=k, **v) for k,v in summaries.items()]).to_csv(output / "daily_results.csv", index=False)
    report = dict(day=str(day), parameters=PARAMETERS, source_run=str(source_run),
                  source_snapshot=str(snapshot_file), snapshot_hash_verified=True,
                  feature_ledger_sha256=feature_manifest["artifact_sha256"],
                  source_config_sha256=g2.sha256(g2.DEFAULT_G_CONFIG), script_sha256=g2.sha256(Path(__file__)),
                  initial_stop_pct=g2.INITIAL_STOP_PCT, tightened_stop_pct=g2.TIGHTENED_STOP_PCT,
                  tighten_after_minutes=g2.TIGHTEN_AFTER_MINUTES, cost_bps=source["cost_bps"],
                  capital_per_entry_rupees=source["capital_per_entry_rupees"], leverage_factor=source["leverage_factor"],
                  target_pct_0926_long=source["exit"]["setups"][SETUP_ID]["target_pct"],
                  checked_symbols=len(audit), eligible_symbols=audit.loc[audit.relaxed_pass, "tradingsymbol"].tolist(),
                  selected_symbols=selected.tradingsymbol.tolist(), summaries=summaries,
                  limitations=["Hindsight thresholds chosen to admit a known winner; not independent validation.",
                               "EMA disabled only for 09:25 LONG; backtest EMA arithmetic was verified correct.",
                               "Unchanged G ranking, one-order quota, other setups, staged exits, and 15:15 cutoff.",
                               "Flat 5 bps round-trip cost, fixed exposure, no separate spread/impact model.",
                               "Original coverage exclusions retained; no missing OI is fabricated.",
                               "No production configuration, live feed, scheduler or broker order modified."])
    (output / "summary.json").write_text(json.dumps(report, indent=2, default=str, allow_nan=False), encoding="utf-8")
    print(f"RESULTS: {output}", flush=True)
    return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-run", type=Path, default=DEFAULT_RUN)
    parser.add_argument("--output-dir", type=Path)
    args = parser.parse_args()
    run(args.source_run, args.output_dir or OUTPUT_ROOT / f"run_{datetime.now():%Y%m%d_%H%M%S}")
