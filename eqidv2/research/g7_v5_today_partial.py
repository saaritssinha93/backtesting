"""G7 V5 guarded-OR versus AND, partial-day research with explicit open marks.

All entry signals use the sealed market snapshot at or before the cutoff.
The historical exit engine is padded with flat bars solely to evaluate genuine
events up to the cutoff; all later events are censored as OPEN_AT_CUTOFF.
"""
from __future__ import annotations

import argparse
from datetime import date
import json
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pandas as pd

import fno_v13_v10_g_2_backtest as native_engine
import fno_v13_replay_source as source
from research import g3_target_cutoff_contract as selection
from research import g4_partial_day_backtest as partial
from research import g7_g6_replica_backtest as g7
from research import g7_nifty_direction as nifty
from research import g7_v4_extremes as extremes
from research import g7_v4_filtered_backtest as v4
from research import g7_v4_indicator_iteration as indicator
from research import g7_v4_selected_backtest as protected_engine
from research import g7_v5_selection_sweep as v5
from research import slot_win70_backtest as slot
import strategy_slot_selection as slot_selection
from research.g3_sl_target_simulator import simulate_exit


SLOTS = {"0921_LONG": 920, "0926_LONG": 925, "0941_LONG": 940, "0951_LONG": 950}


def pad(path: dict, cutoff: pd.Timestamp) -> dict:
    if len(path["timestamp_ns"]) == 0:
        raise ValueError("An order has no post-confirmation execution path")
    if int(path["timestamp_ns"][-1]) != cutoff.value:
        raise ValueError("Order path does not end at the frozen cutoff")
    end = cutoff.normalize() + pd.Timedelta(hours=15, minutes=15)
    future = pd.date_range(cutoff + pd.Timedelta(minutes=1), end, freq="min")
    flat = float(path["close"][-1])
    return {"timestamp_ns": np.concatenate((path["timestamp_ns"], future.asi8)),
            **{field: np.concatenate((path[field], np.full(len(future), flat)))
               for field in ("open", "high", "low", "close")}}


def opening_state(features: pd.DataFrame, cutoff: pd.Timestamp) -> dict:
    day = str(cutoff.date())
    spot_path = Path("C:/TradingData/eqidv2/stocks_indicators_5min_eq_live/NIFTY50_INDEX_stocks_indicators_5min.parquet")
    spot_sha = source._sha(spot_path)
    spot = pd.read_parquet(spot_path)
    spot["ts"] = pd.to_datetime(spot.date, utc=True).dt.tz_convert("Asia/Kolkata")
    prior = spot.loc[spot.ts.eq(pd.Timestamp("2026-10-30 15:30", tz="Asia/Kolkata"))]
    first = spot.loc[spot.ts.eq(pd.Timestamp(day + " 09:20", tz="Asia/Kolkata"))]
    if len(prior) != 1 or len(first) != 1:
        raise ValueError("Exact Nifty spot prior close/opening bar unavailable")
    stock = features.loc[features.hhmm_int.eq(920)].copy()
    stock = stock.loc[stock.source_1m_count.eq(5) & stock.prev_close.gt(0) & stock.open.gt(0)]
    if stock.tradingsymbol.duplicated().any():
        raise ValueError("Duplicate opening stock breadth input")
    previous = float(prior.close.iloc[0]); candle = first.iloc[0]
    day_gain = 100 * (float(candle.close) / previous - 1)
    from_open = 100 * (float(candle.close) / float(candle.open) - 1)
    breadth_prior = 100 * float(stock.close.gt(stock.prev_close).mean())
    breadth_open = 100 * float(stock.close.gt(stock.open).mean())
    coverage = 100 * len(stock) / 218
    and_gate = bool(day_gain >= .25 and from_open > 0 and breadth_prior >= 60
                    and breadth_open >= 50 and coverage >= 95)
    or_gate = bool(coverage >= 95 and day_gain >= 0 and
                   ((day_gain >= .25 and from_open > 0) or
                    (breadth_prior >= 65 and breadth_open >= 60)))
    if source._sha(spot_path) != spot_sha:
        raise ValueError("Nifty spot file changed during opening-state calculation")
    return dict(day_gain_pct=day_gain, from_open_pct=from_open,
                breadth_above_prior_pct=breadth_prior, breadth_above_open_pct=breadth_open,
                breadth_stocks=len(stock), coverage_pct=coverage,
                and_gate=and_gate, guarded_or_gate=or_gate,
                spot_path=str(spot_path), spot_sha256=spot_sha)


def choose_orders(features: pd.DataFrame, signals: pd.DataFrame, market: dict) -> tuple[pd.DataFrame, dict]:
    native = selection.selection_audit(signals)
    control = native.loc[native.v9_selected.eq(True)].copy()
    if control.setup_id.eq("0926_SHORT").any():
        raise ValueError("09:26 SHORT V4 wick replacement requires separate prequota replay")
    day = str(features.day.iloc[0])[:10]
    and_map = {(day, clock): market["and_gate"] for clock in SLOTS.values()}
    or_map = {(day, clock): market["guarded_or_gate"] for clock in SLOTS.values()}
    variants = {}
    for name, regime in (("v5_and", and_map), ("v5_guarded_or", or_map)):
        target = v5.select_day(features, day, oi_floor=0., body_min=.35,
                               gate="moderate", regime=regime,
                               slots=SLOTS, slot_aware=True)
        other = control.loc[~control.setup_id.isin(SLOTS)]
        orders = pd.concat((other, target), ignore_index=True, sort=False)
        keys = set(zip(orders.setup_id, orders.tradingsymbol))
        if len(keys) != len(orders):
            raise ValueError("Duplicated selected setup/stock identity")
        variants[name] = orders
    baseline_keys = set(zip(control.setup_id, control.tradingsymbol))
    and_keys = set(zip(variants["v5_and"].setup_id, variants["v5_and"].tradingsymbol))
    or_keys = set(zip(variants["v5_guarded_or"].setup_id, variants["v5_guarded_or"].tradingsymbol))
    selection_proof = dict(strict_signals=len(signals), baseline_selected=len(control),
                           and_selected=len(and_keys), or_selected=len(or_keys),
                           and_added=sorted(and_keys-baseline_keys), and_removed=sorted(baseline_keys-and_keys),
                           or_added=sorted(or_keys-baseline_keys), or_removed=sorted(baseline_keys-or_keys),
                           or_vs_and_added=sorted(or_keys-and_keys), or_vs_and_removed=sorted(and_keys-or_keys))
    selected = variants["v5_guarded_or"].copy()
    selected = selected.sort_values(["confirmation_ts", "setup_id", "tradingsymbol"], kind="stable").reset_index(drop=True)
    selected["sid"] = np.arange(710_000_000, 710_000_000+len(selected), dtype=np.int64)
    selected["signal_id"] = selected.apply(
        lambda row: f"g7v5today:{day}:{row.setup_id}:{row.tradingsymbol}", axis=1)
    return selected, selection_proof


def reconcile_selected(selected: pd.DataFrame) -> pd.DataFrame:
    records = []
    root = Path("C:/TradingData/eqidv2/stocks_indicators_5min_eq_live")
    for row in selected.itertuples():
        path = root / f"{row.tradingsymbol}_stocks_indicators_5min.parquet"
        if not path.is_file():
            records.append(dict(sid=int(row.sid), setup_id=row.setup_id,
                                tradingsymbol=row.tradingsymbol, status="LIVE_SOURCE_MISSING"))
            continue
        digest = source._sha(path)
        live = pd.read_parquet(path, columns=["date", "open", "high", "low", "close", "volume", "source_1m_count"])
        live["ts"] = pd.to_datetime(live.date, utc=True).dt.tz_convert("Asia/Kolkata")
        exact = live.loc[live.ts.eq(pd.Timestamp(row.signal_ts))]
        if source._sha(path) != digest:
            raise ValueError(f"Live five-minute source changed while comparing {row.tradingsymbol}")
        if len(exact) != 1:
            records.append(dict(sid=int(row.sid), setup_id=row.setup_id,
                                tradingsymbol=row.tradingsymbol, status="LIVE_SIGNAL_BAR_MISSING"))
            continue
        fields = ("open", "high", "low", "close")
        differences = [abs(float(getattr(row, field))-float(exact.iloc[0][field])) for field in fields]
        records.append(dict(sid=int(row.sid), setup_id=row.setup_id,
                            tradingsymbol=row.tradingsymbol, status="COMPARED",
                            max_signal_price_difference=max(differences),
                            signal_price_conflict=max(differences) > 1e-6,
                            signal_volume_difference=abs(float(row.volume)-float(exact.iloc[0].volume)),
                            live_source_1m_count=float(exact.iloc[0].source_1m_count),
                            live_path=str(path), live_sha256=digest))
    return pd.DataFrame(records)


def replay(selected: pd.DataFrame, feature_proof: dict, cutoff: pd.Timestamp) -> tuple[pd.DataFrame, dict]:
    day = cutoff.date()
    snap = Path(feature_proof["snapshot"])
    roots = source.DataRoots(snap/"universe", snap/"equity_1m", snap/"futures_5m", snap/"opening_observer")
    prices = partial.SnapshotPrices(roots, day, cutoff)
    config = g7.configuration()
    specs = {x["setup_id"]: x for x in config["active_setups"]}
    selected = selected.copy()
    selected["native_stop_pct"] = 1.2
    selected["native_target_pct"] = selected.setup_id.map(lambda setup: float(specs[setup]["target_pct"]))
    selected["stop_pct"] = selected.native_stop_pct
    selected["target_pct"] = selected.native_target_pct
    selected["initial_stop_pct"] = 1.2
    selected["tightened_stop_pct"] = 1.0
    selected["tighten_after_minutes"] = 120
    actual_paths = prices.paths(selected)
    padded_paths = {sid: pad(path, cutoff) for sid, path in actual_paths.items()}
    native = native_engine.g.v9.v5.simulate_native(
        selected, padded_paths, cost_bps=5., max_entry_delay_minutes=10)
    books, _, base_config = slot.source_inputs()
    del books
    settings = v4.protection_settings(json.loads(v4.CONFIG.read_text(encoding="utf-8")))
    protected = protected_engine.replay(native, padded_paths, base_config, settings)
    protected["day"] = protected.day.astype(str).str[:10]
    universe = pd.read_parquet(snap/"universe"/f"near_month_{day}.parquet")
    nifty_contracts = universe.loc[universe.is_index_future.fillna(False).astype(bool)
                                   & universe.underlying.eq("NIFTY"), "tradingsymbol"]
    if len(nifty_contracts) != 1:
        raise ValueError("Expected one dated near-month Nifty futures contract")
    nifty_path = snap/"futures_5m"/f"{nifty_contracts.iloc[0]}_5minute.parquet"
    context = SimpleNamespace(snapshots={day:snap},
                              expected={str(nifty_path.resolve()):source._sha(nifty_path)})
    nifty_audit, nifty_proof, _ = nifty.order_audit(protected, context)
    permitted = nifty_audit.set_index("sid").both_counter_allowed
    features = []
    for row in protected.to_dict("records"):
        raw, path = prices.frame(day, row["tradingsymbol"])
        raw = raw.copy()
        raw["ts"] = pd.to_datetime(raw.ts.astype("int64"), unit="ns", utc=True).dt.tz_convert(extremes.IST)
        groups = {str(d): x for d,x in raw.groupby(raw.ts.dt.strftime("%Y-%m-%d"), sort=False)}
        old = extremes.feature_row(groups, row)
        new = indicator.indicator_row(groups[str(day)], row)
        features.append({**old, **new, "stock_source_path":path})
    feature_frame = pd.DataFrame(features).set_index("sid")
    veto = indicator.decisions(feature_frame)["low_efficiency"]
    protected["base_entry_allowed"] = [bool(permitted.loc[int(sid)] and not veto.loc[int(sid)])
                                        for sid in protected.sid]
    protected["g8_baseline_filled"] = protected.filled.astype(bool)
    protected["position_open"] = False
    protected["mark_price"] = np.nan
    protected["mark_ts"] = pd.NaT
    for index, row in protected.iterrows():
        if not bool(row.filled):
            continue
        entry = pd.Timestamp(row.entry_ts)
        if entry > cutoff:
            protected.loc[index, ["filled", "g8_baseline_filled"]] = False
            continue
        exit_at = pd.Timestamp(row.exit_ts)
        if exit_at <= cutoff:
            real = simulate_exit(actual_paths[int(row.sid)], int(row.entry_path_index),
                                 float(row.entry_price), row.side == "LONG", 1.2,
                                 float(row.native_target_pct),
                                 protect_after_close_pct=.6, protected_profit_pct=.1,
                                 tighten_after_minutes=120, tightened_stop_pct=1.)
            if pd.Timestamp(real["exit_ts"]) != exit_at or not np.isclose(real["exit_price"], row.exit_price):
                raise ValueError(f"Pre-cutoff V4 exit changed after flat path padding: {row.sid}")
            continue
        price = float(actual_paths[int(row.sid)]["close"][-1])
        sign = 1 if row.side == "LONG" else -1
        gross = sign * (price / float(row.entry_price) - 1) * 100
        protected.at[index,"position_open"] = True
        protected.at[index,"mark_price"] = price
        protected.at[index,"mark_ts"] = cutoff
        protected.at[index,"gross_return_pct"] = gross
        protected.at[index,"net_return_pct"] = gross - .05
        protected.at[index,"exit_price"] = price
        protected.at[index,"exit_reason"] = "OPEN_AT_CUTOFF"
    enabled = slot_selection.enabled_setup_ids("G7_V4_LOCK2")
    book = slot.apply_slot_policy(protected, "G7_V4_LOCK2", enabled, base_config, cost=5.)
    executed = book.portfolio_executed.astype(bool)
    opened = executed & book.position_open.astype(bool)
    closed = executed & ~book.position_open.astype(bool)
    book["realized_net_rs"] = np.where(closed, book.portfolio_net_profit_rupees, 0.)
    book["open_mtm_net_after_reserved_cost_rs"] = np.where(opened, book.portfolio_net_profit_rupees, 0.)
    book["position_state"] = np.select((opened, closed), ("OPEN", "CLOSED"), default="NOT_EXECUTED")
    book.loc[opened, "exit_ts"] = pd.NaT
    book.loc[opened, "exit_price"] = np.nan
    book["as_of_ist"] = str(cutoff)
    book["scope"] = "PARTIAL_DAY_BACKTEST_ONLY"
    book["execution_authority"] = False
    proof = dict(selected_orders=len(selected), native_fills=int(native.filled.sum()),
                 nifty_allowed=int(permitted.sum()), v4_risk_vetoes=int(veto.sum()),
                 nifty_source=nifty_proof.to_dict("records"),
                 executed_entries=int(executed.sum()), closed_trades=int(closed.sum()),
                 open_positions=int(opened.sum()), closed_wins=int((closed & book.realized_net_rs.gt(0)).sum()),
                 closed_losses=int((closed & book.realized_net_rs.lt(0)).sum()),
                 realized_net_rs=float(book.realized_net_rs.sum()),
                 open_mtm_net_after_reserved_cost_rs=float(book.open_mtm_net_after_reserved_cost_rs.sum()),
                 combined_net_including_open_mtm_rs=float(book.portfolio_net_profit_rupees.loc[executed].sum()))
    return book, proof


def run(features_path: Path, output: Path) -> dict:
    if output.exists():
        raise FileExistsError(output)
    proof = json.loads((features_path / "feature_provenance.json").read_text(encoding="utf-8"))
    if proof["blocking_problems"]:
        raise ValueError("The frozen feature ledger has blocking coverage problems")
    cutoff = pd.Timestamp(proof["cutoff"])
    if cutoff >= pd.Timestamp(f"{cutoff.date()} 15:15", tz="Asia/Kolkata"):
        raise ValueError("Partial runner requires a pre-squareoff cutoff")
    features = pd.read_parquet(features_path / "features.parquet")
    signals = pd.read_parquet(features_path / "strict_signals.parquet")
    market = opening_state(features, cutoff)
    chosen, selection_proof = choose_orders(features, signals, market)
    if selection_proof["or_vs_and_added"] or selection_proof["or_vs_and_removed"]:
        raise ValueError("AND and OR selected different orders; each needs its own portfolio replay")
    signal_reconciliation = reconcile_selected(chosen)
    book, portfolio = replay(chosen, proof, cutoff)
    output.mkdir(parents=True)
    book.to_csv(output/"portfolio_trades.csv", index=False)
    chosen.to_csv(output/"selected_orders.csv", index=False)
    pd.DataFrame([market]).to_csv(output/"opening_market_state.csv", index=False)
    signal_reconciliation.to_csv(output/"selected_signal_reconciliation.csv", index=False)
    comparison = []
    for variant in ("v5_and", "v5_guarded_or"):
        comparison.append(dict(variant=variant, cutoff=str(cutoff),
                               selected_orders=portfolio["selected_orders"],
                               executed_entries=portfolio["executed_entries"],
                               closed_trades=portfolio["closed_trades"],
                               open_positions=portfolio["open_positions"],
                               closed_wins=portfolio["closed_wins"],
                               closed_losses=portfolio["closed_losses"],
                               realized_net_rs=portfolio["realized_net_rs"],
                               open_mtm_net_after_reserved_cost_rs=portfolio["open_mtm_net_after_reserved_cost_rs"],
                               combined_net_including_open_mtm_rs=portfolio["combined_net_including_open_mtm_rs"]))
    pd.DataFrame(comparison).to_csv(output/"variant_comparison.csv", index=False)
    summary = dict(scope="PARTIAL_DAY_BACKTEST_ONLY", execution_authority=False,
                   day=str(cutoff.date()), cutoff=str(cutoff), final_day_result=False,
                   source_feature_proof=str(features_path/"feature_provenance.json"),
                   market=market, selection=selection_proof, portfolio=portfolio,
                   excluded_stocks=proof["excluded_stocks"],
                   opening_exclusions=len(proof["opening_exclusions"]),
                   retrospective_cash_live_5m_price_conflicts=proof["live_5m_price_conflict_stocks"],
                   selected_signal_price_conflicts=int(signal_reconciliation.signal_price_conflict.fillna(False).sum()),
                   limitations=["Retrospective broker cash bars differ from the archived live five-minute bars.",
                                "Open positions are valued at the last completed minute, with modeled round-trip cost reserved.",
                                "No post-cutoff price or exit event is treated as realized.",
                                "The V4 historical slot allowlist is retrospectively selected on earlier sessions."])
    (output/"summary.json").write_text(json.dumps(summary, indent=2, default=str), encoding="utf-8")
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--features", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(run(args.features, args.output), indent=2, default=str))


if __name__ == "__main__":
    main()
