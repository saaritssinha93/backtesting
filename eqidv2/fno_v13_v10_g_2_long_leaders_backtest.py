"""Research-only LONG leader continuation; never submits or schedules orders.

Two identical-rule experiments: a hindsight-conditioned top-10 watchlist and
an as-of top-10 ranking of the full dated universe. G2 exits are reused exactly.
This is a one-session hypothesis test, not a fitted or validated live strategy.
"""
from __future__ import annotations

import argparse
import json
from datetime import date, datetime
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

import fno_oi_hybrid_data as hybrid
import fno_v13_v10_g_2_backtest as g2
import fno_v13_v10_g_2_extend_results as ext
import fno_v13_v10_g_daily_replay as replay
import fno_v13_v10_g_live_config as source_config

VERSION = "V13-V10-G-2-LONG-LEADERS-RESEARCH-1"
DEFAULT_RUN = Path("C:/TradingData/eqidv2/backtesting_result_v13_v10_g/runs/2026-10-05/20261005T162013242339")
DEFAULT_OUTPUT_ROOT = Path("C:/TradingData/eqidv2/fno_oi/strategy_research/v13_g2_long_leaders")
MODES = ("HINDSIGHT_FIXED_TOP10", "CAUSAL_ASOF_TOP10")
# Frozen before the first outcome replay; no parameter search in this runner.
RULES = dict(top_n=10, max_orders_per_day=3, max_orders_per_slot=1,
             one_order_per_symbol=True, minimum_day_gain_pct=1.0,
             minimum_five_minute_volume_ratio=0.8,
             maximum_confirmation_vwap_extension_pct=1.5,
             minimum_confirmation_body_ratio=0.4,
             maximum_confirmation_upper_wick_ratio=0.35,
             opening_range_end="09:25", ema_warmup_calendar_days=14,
             oi_filter=False, one_minute_volume_filter=False,
             five_minute_price_rule="positive_close_to_close_only",
             ema_rule="signal_close > EMA9 > EMA20",
             confirmation_rule="bullish; close > signal_close and opening_10m_high",
             rank_rule="signal_close / previous_day_close - 1; descending; symbol tie-break",
             entry_rule="next-minute-or-later break of confirmation high; expires after 10 minutes",
             budget_rule="three submitted orders, including unfilled, not three hindsight fills")


def protocol(source_g: dict, session: date) -> dict:
    long_slots = {s.signal_end: dict(setup_id=s.setup_id, target_pct=s.target_pct)
                  for s in source_config.ACTIVE_SETUPS if s.side == "LONG"}
    for spec in long_slots.values():
        assert spec["target_pct"] == source_g["exit"]["setups"][spec["setup_id"]]["target_pct"]
    return dict(version=VERSION, session_date=str(session), rules=RULES,
                long_slots=long_slots, initial_stop_pct=g2.INITIAL_STOP_PCT,
                tightened_stop_pct=g2.TIGHTENED_STOP_PCT,
                tighten_after_minutes=g2.TIGHTEN_AFTER_MINUTES,
                cost_bps=source_g["cost_bps"],
                capital_per_entry_rupees=source_g["capital_per_entry_rupees"],
                leverage_factor=source_g["leverage_factor"],
                portfolio_capital_rupees=source_g["portfolio_capital_rupees"],
                square_off="15:15", execution_authority=False,
                selection_identity_unchanged=False,
                evidence="ONE_DAY_IN_SAMPLE_HYPOTHESIS; HINDSIGHT_MODE_NOT_CAUSAL")


def validate_bars(frame: pd.DataFrame) -> None:
    if frame.ts.isna().any() or frame.ts.duplicated().any():
        raise ValueError("Invalid or duplicate minute timestamps")
    values = frame[["open", "high", "low", "close", "volume"]].to_numpy(float)
    if not np.isfinite(values).all() or (values[:, :4] <= 0).any() or (values[:, 4] < 0).any():
        raise ValueError("Invalid OHLCV")
    if ((frame.high < frame[["open", "close"]].max(axis=1))
            | (frame.low > frame[["open", "close"]].min(axis=1))
            | (frame.high < frame.low)).any():
        raise ValueError("Inconsistent OHLC")
    for field in ("gap_filled", "opening_snapshot", "provisional_stale"):
        if field in frame:
            flags = frame[field].astype(str).str.strip().str.lower().isin(["true", "yes", "on"])
            flags |= pd.to_numeric(frame[field], errors="coerce").fillna(0).ne(0)
            if flags.any():
                raise ValueError(f"Flagged source bars: {field}")


def load_minutes(path: Path, session: date) -> pd.DataFrame:
    required = ["date", "open", "high", "low", "close", "volume", "Prev_Day_Close"]
    names = set(pq.ParquetFile(path).schema.names)
    if set(required) - names:
        raise ValueError(f"Missing columns in {path.name}: {set(required) - names}")
    optional = [c for c in ("gap_filled", "opening_snapshot", "provisional_stale") if c in names]
    frame = pd.read_parquet(path, columns=required + optional)
    frame["ts"] = hybrid._to_ist(frame.date).dt.as_unit("ns")
    start = pd.Timestamp(session, tz="Asia/Kolkata") - pd.Timedelta(days=RULES["ema_warmup_calendar_days"])
    end = pd.Timestamp(session, tz="Asia/Kolkata") + pd.Timedelta(hours=15, minutes=15)
    frame = frame.loc[frame.ts.between(start, end)].copy().sort_values("ts")
    for c in ("open", "high", "low", "close", "volume", "Prev_Day_Close"):
        frame[c] = pd.to_numeric(frame[c], errors="coerce")
    validate_bars(frame)
    today = frame.loc[frame.ts.dt.date.eq(session)]
    expected = pd.date_range(pd.Timestamp(session, tz="Asia/Kolkata") + pd.Timedelta(hours=9, minutes=16), end, freq="min")
    if not np.array_equal(today.ts.astype("int64").to_numpy(), expected.asi8):
        # Fail the run, never silently remove a symbol based on a bad future path.
        raise ValueError(f"Incomplete research-day prices: {path.name}")
    previous = today.Prev_Day_Close
    if previous.isna().any() or not np.isfinite(previous).all() or previous.le(0).any() or previous.nunique() != 1:
        raise ValueError(f"Invalid previous close: {path.name}")
    return frame.reset_index(drop=True)


def symbol_features(minute: pd.DataFrame, session: date, symbol: str, slots: dict) -> tuple[pd.DataFrame, pd.DataFrame]:
    """All decision fields are prefix-causal; no session peaks are computed here."""
    five = hybrid.add_equity_five_minute_features(hybrid.aggregate_equity_one_minute_to_five_minute(minute))
    today = minute.loc[minute.ts.dt.date.eq(session)].copy().reset_index(drop=True)
    typical = (today.high + today.low + today.close) / 3
    today["session_vwap"] = (typical * today.volume).cumsum() / today.volume.cumsum().replace(0, np.nan)
    opening = today.loc[today.ts.dt.strftime("%H:%M").le(RULES["opening_range_end"])]
    if len(opening) != 10:
        raise ValueError(f"Incomplete opening range for {symbol}")
    opening_high = float(opening.high.max())
    prev = float(today.Prev_Day_Close.iloc[0])
    records = []
    for clock, spec in sorted(slots.items()):
        ts = pd.Timestamp(f"{session} {clock}", tz="Asia/Kolkata")
        ct = ts + pd.Timedelta(minutes=1)
        signal = five.loc[five.ts.eq(ts)]
        confirmation = today.loc[today.ts.eq(ct)]
        if len(signal) != 1 or len(confirmation) != 1:
            continue  # Prefix tests may intentionally stop before a later decision.
        s, c = signal.iloc[0], confirmation.iloc[0]
        ran = float(c.high-c.low)
        body = float(abs(c.close-c.open)/ran) if ran > 0 else np.nan
        wick = float((c.high-max(c.close,c.open))/ran) if ran > 0 else np.nan
        extension = float((c.close/c.session_vwap-1)*100)
        gain = float((s.close/prev-1)*100)
        checks = dict(
            day_gain=gain >= RULES["minimum_day_gain_pct"],
            positive_five_minute_move=s.price_change_pct > 0,
            ema_trend=s.close > s.ema9 > s.ema20,
            opening_range_break=c.close > opening_high,
            vwap_extension=0 < extension <= RULES["maximum_confirmation_vwap_extension_pct"],
            five_minute_volume=s.volume_ratio >= RULES["minimum_five_minute_volume_ratio"],
            bullish_confirmation=c.close > c.open and c.close > s.close,
            confirmation_body=body >= RULES["minimum_confirmation_body_ratio"],
            confirmation_wick=wick <= RULES["maximum_confirmation_upper_wick_ratio"],
        )
        records.append(dict(day=session, tradingsymbol=symbol, side="LONG", signal_ts=ts,
                            confirmation_ts=ct, signal_end=clock, setup_id=spec["setup_id"],
                            hhmm_int=int(clock.replace(":", "")), trigger=float(c.high),
                            signal_close=float(s.close), previous_day_close=prev, day_gain_pct=gain,
                            price_change_pct=float(s.price_change_pct), volume_ratio=float(s.volume_ratio),
                            ema9=float(s.ema9), ema20=float(s.ema20),
                            opening_range_high=opening_high, session_vwap=float(c.session_vwap),
                            vwap_extension_pct=extension, body_ratio=body, wick_ratio=wick,
                            traded_value=float(s.traded_value),
                            native_stop_pct=g2.INITIAL_STOP_PCT, native_target_pct=spec["target_pct"],
                            signal_rule_pass=bool(all(checks.values())),
                            failed_rules=";".join(k for k,v in checks.items() if not v),
                            **{f"gate_{k}": bool(v) for k,v in checks.items()}))
    return pd.DataFrame(records), today


def select_orders(features: pd.DataFrame, mode: str, hindsight_symbols: list[str]) -> tuple[pd.DataFrame, pd.DataFrame]:
    if mode not in MODES:
        raise ValueError("Unknown experiment")
    audit = features.copy().sort_values(["signal_ts", "day_gain_pct", "tradingsymbol"], ascending=[True, False, True])
    audit["asof_rank"] = audit.groupby("signal_ts").cumcount() + 1
    audit["universe_pass"] = (audit.tradingsymbol.isin(hindsight_symbols) if mode == MODES[0]
                              else audit.asof_rank.le(RULES["top_n"]))
    audit["selected"] = False
    audit["selection_reason"] = np.where(~audit.universe_pass, "OUTSIDE_WATCHLIST_OR_ASOF_TOP10",
                                         np.where(~audit.signal_rule_pass, "ENTRY_RULES_FAILED", "ELIGIBLE_NOT_SELECTED"))
    seen = set()
    count = 0
    for _, group in audit.groupby("signal_ts", sort=True):
        slot_used = 0
        for ix, row in group.iterrows():
            if not row.universe_pass or not row.signal_rule_pass:
                continue
            if row.tradingsymbol in seen:
                audit.at[ix, "selection_reason"] = "SYMBOL_ALREADY_ORDERED"
            elif count >= RULES["max_orders_per_day"]:
                audit.at[ix, "selection_reason"] = "DAILY_ORDER_CAP"
            elif slot_used >= RULES["max_orders_per_slot"]:
                audit.at[ix, "selection_reason"] = "LOWER_RANK_THIS_SLOT"
            else:
                audit.at[ix, "selected"] = True
                audit.at[ix, "selection_reason"] = "SELECTED"
                seen.add(row.tradingsymbol)
                count += 1
                slot_used += 1
    audit["experiment"] = mode
    return audit.loc[audit.selected].reset_index(drop=True), audit.reset_index(drop=True)


def simulate(orders: pd.DataFrame, minutes: dict, source_g: dict) -> tuple[pd.DataFrame, dict]:
    paths = {}
    for row in orders.itertuples(index=False):
        future = minutes[row.tradingsymbol].loc[lambda x: x.ts.gt(row.confirmation_ts)]
        paths[int(row.sid)] = dict(timestamp_ns=future.ts.astype("int64").to_numpy(),
                                   **{c: future[c].to_numpy(float) for c in ("open", "high", "low", "close")})
    g2.g.v9.validate_paths(orders, paths)
    trades = g2.simulate_staged(orders, paths, cost_bps=float(source_g["cost_bps"]), max_entry_delay_minutes=10)
    for key, default in (("filled", False), ("entry_ts", pd.NaT), ("exit_ts", pd.NaT),
                         ("gross_return_pct", np.nan), ("net_return_pct", np.nan), ("cost_pct", np.nan)):
        if key not in trades:
            trades[key] = default
    base = ext._portfolio_config(source_g)
    trades = g2.g.v9.v5.apply_fixed_capital_model(trades, base.capital_per_entry_rupees, base.leverage_factor)
    ledger, _ = g2.g.v9.v6.apply_portfolio_constraints(trades, base.portfolio_config())
    done = ledger.loc[ledger.portfolio_executed.eq(True)]
    profit = done.portfolio_net_profit_rupees
    negative = profit[profit.lt(0)]
    summary = dict(orders=len(orders), fills=len(done), wins=int(profit.gt(0).sum()),
                   losses=int(profit.lt(0).sum()), breakeven=int(profit.eq(0).sum()),
                   win_rate_pct=float(profit.gt(0).mean()*100) if len(done) else None,
                   gross_profit_rupees=float(done.portfolio_gross_profit_rupees.sum()),
                   cost_rupees=float(done.portfolio_cost_rupees.sum()),
                   net_profit_rupees=float(profit.sum()),
                   profit_factor=float(profit[profit.gt(0)].sum()/-negative.sum()) if len(negative) else None,
                   portfolio_return_pct=float(profit.sum()/base.portfolio_capital_rupees*100),
                   configured_max_orders_per_day=RULES["max_orders_per_day"],
                   selected_symbols=orders.tradingsymbol.tolist())
    assert len(done) <= 3 and orders.tradingsymbol.nunique() == len(orders)
    return ledger, summary


def json_text(value: dict) -> str:
    return json.dumps(value, indent=2, default=str, allow_nan=False)


def run(run_root: Path, output: Path) -> dict:
    if output.exists():
        raise FileExistsError(f"Use a fresh output folder: {output}")
    result = json.loads((run_root / "replay_result.json").read_text(encoding="utf-8"))
    if result.get("state") != "SUCCESS" or result.get("complete") is not True:
        raise ValueError("Incomplete source daily replay")
    session = date.fromisoformat(result["session_date"])
    source_g = source_config.load_frozen_config()
    spec = protocol(source_g, session)
    output.mkdir(parents=True)
    (output / "frozen_protocol.json").write_text(json_text(spec), encoding="utf-8")
    snapshot_file = Path(result["artifacts"]["input_snapshot_manifest"])
    manifest = json.loads(snapshot_file.read_text(encoding="utf-8"))
    if manifest.get("complete") is not True or manifest["session_date"] != str(session):
        raise ValueError("Snapshot session mismatch or incomplete snapshot")
    print("Verifying immutable input snapshot", flush=True)
    replay._verify_input_snapshot(snapshot_file.parent, manifest)
    universe = pd.read_parquet(snapshot_file.parent / f"universe/near_month_{session}.parquet")
    symbols = sorted(universe.equity_symbol.dropna().unique())
    minutes, feature_frames, peaks = {}, [], []
    for ix, symbol in enumerate(symbols):
        path = snapshot_file.parent / "equity_1m" / f"{symbol}_stocks_indicators_1min.parquet"
        frame = load_minutes(path, session)
        features, today = symbol_features(frame, session, symbol, spec["long_slots"])
        if len(features) != len(spec["long_slots"]):
            raise ValueError(f"Missing signal/confirmation features: {symbol}")
        feature_frames.append(features)
        minutes[symbol] = today
        previous = float(today.Prev_Day_Close.iloc[0])
        peaks.append(dict(tradingsymbol=symbol, peak_gain_pct=float((today.high.max()/previous-1)*100)))
        if ix % 30 == 0:
            print(f"Prepared {ix+1}/{len(symbols)} symbols", flush=True)
    features = pd.concat(feature_frames, ignore_index=True).sort_values(["signal_ts", "tradingsymbol"]).reset_index(drop=True)
    features["sid"] = np.arange(len(features))
    peaks = pd.DataFrame(peaks).sort_values(["peak_gain_pct", "tradingsymbol"], ascending=[False, True]).reset_index(drop=True)
    peaks["hindsight_peak_rank"] = np.arange(1, len(peaks)+1)
    top10 = peaks.head(10).copy()
    hindsight_symbols = top10.tradingsymbol.tolist()
    summaries, all_audits, all_trades = {}, [], []
    for mode in MODES:
        orders, audit = select_orders(features, mode, hindsight_symbols)
        ledger, summary = simulate(orders, minutes, source_g)
        summary["eventual_top10_selected"] = sorted(set(orders.tradingsymbol) & set(hindsight_symbols))
        summaries[mode] = summary
        all_audits.append(audit)
        all_trades.append(ledger)
        orders.to_csv(output / f"{mode.lower()}_orders.csv", index=False)
        ledger.to_csv(output / f"{mode.lower()}_trades.csv", index=False)
        audit.to_csv(output / f"{mode.lower()}_decision_audit.csv", index=False)
        print(mode, json_text(summary), flush=True)
    combined_audit = pd.concat(all_audits, ignore_index=True)
    top_audit = combined_audit.loc[combined_audit.tradingsymbol.isin(hindsight_symbols)].merge(top10, on="tradingsymbol", validate="many_to_one")
    top_audit.to_csv(output / "top10_decision_audit.csv", index=False)
    top10.to_csv(output / "hindsight_top10.csv", index=False)
    pd.DataFrame([dict(session_date=str(session), experiment=k, **v) for k,v in summaries.items()]).to_csv(output / "daily_results.csv", index=False)
    # Automated prefix check: deleting every later bar leaves earlier decisions unchanged.
    prefix_checks = 0
    for symbol in hindsight_symbols:
        path = snapshot_file.parent / "equity_1m" / f"{symbol}_stocks_indicators_1min.parquet"
        frame = load_minutes(path, session)
        for clock in ("09:30", "09:45", "10:00"):
            cutoff = pd.Timestamp(f"{session} {clock}", tz="Asia/Kolkata") + pd.Timedelta(minutes=1)
            prefix, _ = symbol_features(frame.loc[frame.ts.le(cutoff)].copy(), session, symbol, spec["long_slots"])
            expected = features.loc[features.tradingsymbol.eq(symbol) & features.confirmation_ts.le(cutoff), prefix.columns]
            pd.testing.assert_frame_equal(prefix.reset_index(drop=True), expected.reset_index(drop=True), check_dtype=False)
            prefix_checks += 1
    report = dict(protocol=spec, source_run=str(run_root), snapshot=str(snapshot_file),
                  snapshot_fingerprint=manifest["snapshot_fingerprint"],
                  source_g_config_sha256=g2.sha256(g2.DEFAULT_G_CONFIG),
                  script_sha256=g2.sha256(Path(__file__)), universe_size=len(symbols),
                  original_g_selected_orders=result["metrics"]["selected_orders"],
                  original_g_net_profit_rupees=result["metrics"]["net_profit_rupees"],
                  source_snapshot_hash_verified=True, prefix_causality_checks_passed=prefix_checks,
                  policies=summaries, hindsight_top10=top10.to_dict("records"),
                  limitations=["One session used to formulate the hypothesis; not out-of-sample validation.",
                               "HINDSIGHT_FIXED_TOP10 knows the eventual session winners and cannot be traded as shown.",
                               "CAUSAL_ASOF_TOP10 only uses the dated universe and completed signal/confirmation information.",
                               "213-symbol price universe may include symbols excluded by original G for OI quality; this strategy does not use OI.",
                               "5 bps flat costs, idealized fixed exposure, no spread/impact or integer-share sizing.",
                               "15:15 research exit retained; not a validated Zerodha MIS implementation.",
                               "Profit factor is null without losses; an all-winning tiny sample does not establish reliability.",
                               "Maximum three orders includes unfilled orders; no hindsight replacement with later winners."])
    (output / "summary.json").write_text(json_text(report), encoding="utf-8")
    lines = [VERSION, f"Session: {session}", "RESEARCH ONLY. No scheduled task or broker order was created.",
             "Same entry rules across both experiments. See frozen_protocol.json for exact conditions.",
             "Initial SL 1.25%; tighten to 1.00% after 120 minutes from entry candle end; native LONG targets unchanged.",
             "", "RESULTS"]
    for mode, summary in summaries.items():
        lines += [mode, json_text(summary)]
        ledger = all_trades[MODES.index(mode)]
        cols = [c for c in ("tradingsymbol", "signal_end", "asof_rank", "entry_ts", "entry_price", "native_target_pct", "exit_ts", "exit_price", "exit_reason", "portfolio_cost_rupees", "portfolio_net_profit_rupees") if c in ledger]
        lines += [ledger[cols].to_string(index=False), ""]
    lines += ["LIMITATIONS", *report["limitations"]]
    (output / "RESULTS.txt").write_text("\n".join(lines), encoding="utf-8")
    print(f"RESULTS: {output}", flush=True)
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-run", type=Path, default=DEFAULT_RUN)
    parser.add_argument("--output-dir", type=Path)
    args = parser.parse_args()
    output = args.output_dir or DEFAULT_OUTPUT_ROOT / f"run_{datetime.now():%Y%m%d_%H%M%S}"
    run(args.source_run, output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
