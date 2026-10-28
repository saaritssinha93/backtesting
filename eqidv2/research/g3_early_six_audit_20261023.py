"""Causal, isolated earliest-entry diagnosis for six requested 2026-10-23 stocks.

Thresholds are fixed before evaluating P&L. Outputs are a six-name diagnostic,
not a full-universe selector or a validated strategy. No active settings change.
"""
from __future__ import annotations

import argparse
from datetime import date
import hashlib
import json
from pathlib import Path
import sys
from types import SimpleNamespace

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
import fno_v13_replay_source as source
import fno_v13_v10_g_2_backtest as simulator

DAY = date(2026, 10, 23)
DEFAULT_RUN = Path(r"C:\TradingData\eqidv2\backtesting_result_v13_v10_g_3\production\runs\2026-10-23\20261023T171521599663_b80e6def")
DEFAULT_SNAPSHOT = DEFAULT_RUN / "input_snapshots/31a25c7fe3e61600a96a9ae41180fff31c750e558a9dfb54e3e99442ff6ea611"
SIDES = {"SWIGGY": "SHORT", "MOTILALOFS": "SHORT", "BANDHANBNK": "SHORT",
         "COLPAL": "SHORT", "DELHIVERY": "SHORT", "ETERNAL": "LONG"}
FIELDS = ["tradingsymbol", "side", "signal_ts", "confirmation_ts", "price_change_pct",
          "oi_change_pct", "volume_ratio", "v9_1m_volume_ratio", "body_ratio", "wick_ratio",
          "ema9", "ema20", "ema50", "signal_close", "confirmation_open", "confirmation_close",
          "confirmation_high", "confirmation_low", "traded_value"]


def sha256(path):
    h = hashlib.sha256()
    with Path(path).open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def reconstruct(snapshot=DEFAULT_SNAPSHOT):
    """Use the production causal builder on every completed five-minute clock."""
    snapshot = Path(snapshot)
    manifest = json.loads((snapshot / "snapshot_manifest.json").read_text())
    expected = {r["snapshot_relative_path"]: r for r in manifest["sources"] if r.get("captured")}
    files = [snapshot / "snapshot_manifest.json"]
    clocks = [x.strftime("%H:%M") for x in pd.date_range(f"{DAY} 09:20", f"{DAY} 13:30", freq="5min")]
    frames, minutes, errors = [], {}, []
    for symbol, side in SIDES.items():
        contract = f"{symbol}26OCTFUT"
        mp = snapshot / f"equity_1m/{symbol}_stocks_indicators_1min.parquet"
        fp = snapshot / f"futures_5m/{contract}_5minute.parquet"
        ep = snapshot / f"opening_observer/historical/{DAY}/{contract}.json"
        files.extend([mp, fp, ep])
        minute = source._load_minute(mp, DAY, errors, symbol)
        future = source._load_future(fp, DAY, errors, symbol)
        coverage = source._coverage(minute, future, DAY, symbol, errors, signal_clocks=clocks)
        assert coverage["missing_equity_minutes"] == 0, (symbol, coverage)
        assert coverage["missing_futures_bars"] == 0, (symbol, coverage)
        pool = source._observed_pool(minute, future, day=DAY, symbol=symbol,
            future_symbol=contract, month="26OCT", signal_clocks=clocks)
        assert len(pool) == len(clocks), (symbol, len(pool))
        opening_mask = pool.hhmm_int.eq(920)
        opening = source.opening_inputs.build_opening_features(pool.loc[opening_mask].iloc[0], future,
            day=DAY, symbol=symbol, contract=contract, expiry=future.expiry.iloc[-1],
            root=snapshot / "opening_observer")
        assert opening["opening_0920_status"] == "VALID", (symbol, opening)
        for key, value in opening.items():
            if key not in pool:
                pool[key] = None
            pool.loc[opening_mask, key] = value
        pool["side"] = side
        pool["wick_ratio"] = pool.v9_1m_upper_wick_ratio if side == "LONG" else pool.v9_1m_lower_wick_ratio
        frames.append(pool)
        minutes[symbol] = minute.loc[minute.ts.dt.date.eq(DAY)].copy()
    nifty_path = snapshot / "futures_5m/NIFTY26OCTFUT_5minute.parquet"
    files.append(nifty_path)
    nifty = source._load_future(nifty_path, DAY, errors, "NIFTY")
    opening = nifty.loc[nifty.ts.eq(pd.Timestamp(f"{DAY} 09:20", tz="Asia/Kolkata"))].iloc[0]
    nifty_return = (float(opening.close) / float(opening.open) - 1) * 100
    provenance = []
    for path in files:
        observed = sha256(path)
        rel = path.relative_to(snapshot).as_posix()
        record = expected.get(rel)
        if record:
            assert observed == record["sha256"], f"Sealed input drift: {path}"
        provenance.append(dict(path=str(path), sha256=observed, sealed_hash_verified=bool(record)))
    return pd.concat(frames, ignore_index=True), minutes, nifty_return, provenance, errors


def checks(row, branch, minimum_volume, nifty_return):
    long = row.side == "LONG"
    sign = 1 if long else -1
    full = row.ema9 > row.ema20 > row.ema50 if long else row.ema9 < row.ema20 < row.ema50
    turn = (row.ema9 > row.ema20 and row.signal_close > row.ema50) if long else (row.ema9 < row.ema20 and row.signal_close < row.ema50)
    return dict(
        source_complete=int(row.source_1m_count) == 5,
        real_confirmation=bool(row.v9_exact_confirmation_present),
        trend=bool(full if branch == "continuation" else turn),
        move=sign * float(row.price_change_pct) >= .20,
        five_minute_volume=float(row.volume_ratio) >= minimum_volume,
        confirmation_direction=sign * (row.confirmation_close - row.confirmation_open) > 0 and sign * (row.confirmation_close - row.signal_close) > 0,
        confirmation_range=row.confirmation_high > row.confirmation_low,
        confirmation_body=.30 <= row.body_ratio <= 1,
        confirmation_wick=0 <= row.wick_ratio <= .60,
        one_minute_volume=row.v9_1m_volume_ratio >= .50,
        opening_market_gate=(row.hhmm_int not in (920, 925) or (nifty_return >= -.05 if long else nifty_return <= .05)),
    )


def execute(row, minute, mode, *, opening=False):
    """Fixed stop/target comparison using native fill and staged-exit arithmetic."""
    decision = row.signal_ts if opening else row.confirmation_ts
    frame = minute.loc[minute.ts.gt(decision) & minute.ts.le(pd.Timestamp(f"{DAY} 15:15", tz="Asia/Kolkata"))]
    assert len(frame) and frame.ts.iloc[0] == decision + pd.Timedelta(minutes=1)
    path = {"timestamp_ns": frame.ts.astype("int64").to_numpy(), **{k: frame[k].to_numpy(float) for k in ("open", "high", "low", "close")}}
    is_long = row.side == "LONG"
    trigger = float((row.high if is_long else row.low) if opening else (row.confirmation_high if is_long else row.confirmation_low))
    if mode == "strict_breakout":
        found = simulator.g.v9.v5._entry(SimpleNamespace(side=row.side, trigger=trigger), path,
            delay_bars=0, trigger_buffer_pct=0, worse_fill_bps=0, max_entry_delay_minutes=10)
        if found is None:
            return dict(filled=False, decision_ts=decision, trigger=trigger, exit_reason="UNFILLED")
        start, entry, _, gap, overshoot = found
        event = "OPEN" if gap else "INTRABAR"
    else:
        start, entry, gap, overshoot, event = 0, float(path["open"][0]), False, 0., "OPEN"
    bar_end = pd.Timestamp(int(path["timestamp_ns"][start]), tz="UTC").tz_convert("Asia/Kolkata")
    # An intrabar fill's precise tick is unknown; explicitly preserve its minute interval.
    execution_ts = bar_end - pd.Timedelta(minutes=1) if event == "OPEN" else bar_end
    j, exit_price, reason, active_stop, exit_event = simulator.staged_exit(path, start, entry, is_long, 1.50)
    exit_end = pd.Timestamp(int(path["timestamp_ns"][j]), tz="UTC").tz_convert("Asia/Kolkata")
    exit_ts = exit_end - pd.Timedelta(minutes=1) if exit_event == "OPEN" else exit_end
    gross = (1 if is_long else -1) * (exit_price / entry - 1) * 100
    excursion_end = max(start, j - 1) if exit_event == "OPEN" else j
    mfe, mae = simulator.g.v9.v5._excursions(path, entry, start, excursion_end, is_long)
    return dict(filled=True, decision_ts=decision, trigger=trigger, entry_price=entry,
        entry_bar_start_ts=bar_end-pd.Timedelta(minutes=1), entry_bar_end_ts=bar_end,
        entry_execution_ts=execution_ts, entry_event=event, entry_gap_through=gap,
        entry_overshoot_bps=overshoot, exit_price=exit_price, exit_ts=exit_ts,
        exit_bar_end_ts=exit_end, exit_reason=reason, exit_event=exit_event,
        active_stop_pct_at_exit=active_stop, target_pct=1.50, gross_return_pct=gross,
        cost_pct=.05, net_return_pct=gross-.05, mfe_pct=mfe, mae_pct=mae)


def run(snapshot=DEFAULT_SNAPSHOT, output=ROOT / "outputs/g3_early_entry_20261023/individual"):
    output = Path(output)
    output.mkdir(parents=True, exist_ok=True)
    pool, minutes, nifty, inputs, errors = reconstruct(snapshot)
    records = []
    for row in pool.itertuples(index=False):
        base = {key: getattr(row, key) for key in FIELDS}
        for branch in ("continuation", "reversal"):
            for volume in (.40, .80):
                gates = checks(row, branch, volume, nifty)
                passed = all(gates.values())
                for mode in ("strict_breakout", "next_open"):
                    variant = f"{branch}_vol{int(volume*100):03d}_{mode}"
                    rec = dict(base, variant=variant, rule_branch=branch, volume_minimum=volume,
                        execution_mode=mode, qualified=passed, failed_rules=";".join(k for k,v in gates.items() if not v))
                    rec.update(execute(row, minutes[row.tradingsymbol], mode) if passed else dict(filled=False, exit_reason="NOT_QUALIFIED"))
                    records.append(rec)
        if row.hhmm_int == 920:
            for mode in ("strict_breakout", "next_open"):
                passed = (1 if row.side == "LONG" else -1) * row.price_change_pct >= .20
                rec = dict(base, variant=f"opening_price_only_{mode}", rule_branch="opening_price_only",
                    volume_minimum=None, execution_mode=mode, qualified=passed,
                    failed_rules="" if passed else "opening_directional_move")
                rec.update(execute(row, minutes[row.tradingsymbol], mode, opening=True) if passed else dict(filled=False, exit_reason="NOT_QUALIFIED"))
                records.append(rec)
    audit = pd.DataFrame(records)
    fills = audit.loc[audit.filled].sort_values(["entry_bar_end_ts", "entry_execution_ts", "signal_ts"], kind="stable")
    earliest = fills.groupby(["variant", "tradingsymbol"], sort=False).head(1).copy()
    # Missing symbols remain visible rather than being silently omitted from the count.
    grid = pd.MultiIndex.from_product([audit.variant.unique(), SIDES], names=["variant", "tradingsymbol"]).to_frame(index=False)
    earliest = grid.merge(earliest, on=["variant", "tradingsymbol"], how="left")
    earliest["filled"] = earliest.filled.eq(True)
    summary = earliest.groupby("variant").agg(filled=("filled", "sum"),
        winners=("net_return_pct", lambda x: int((x > 0).sum())),
        losers=("net_return_pct", lambda x: int((x < 0).sum())),
        summed_trade_net_return_pct=("net_return_pct", "sum"),
        mean_trade_net_return_pct=("net_return_pct", "mean")).reset_index()
    pool.to_csv(output / "all_5minute_features.csv", index=False)
    audit.to_csv(output / "all_rule_evaluations.csv", index=False)
    earliest.to_csv(output / "earliest_fills.csv", index=False)
    summary.to_csv(output / "variant_summary.csv", index=False)
    production_ledger = Path(snapshot).parent.parent / "feature_ledger.csv"
    parity = None
    if production_ledger.exists():
        sealed = pd.read_csv(production_ledger)
        current = pool.copy()
        current["signal_ts"] = current.signal_ts.astype(str)
        compared = current.merge(sealed, on=["tradingsymbol", "signal_ts"], suffixes=("_new", "_sealed"))
        fields = ["price_change_pct", "oi_change_pct", "volume_ratio", "v9_1m_volume_ratio", "ema9", "ema20", "ema50", "body_ratio"]
        maximum_difference = {}
        for field in fields:
            left = pd.to_numeric(compared[f"{field}_new"], errors="coerce")
            right = pd.to_numeric(compared[f"{field}_sealed"], errors="coerce")
            assert np.allclose(left, right, atol=1e-7, rtol=1e-7, equal_nan=True), field
            maximum_difference[field] = float((left-right).abs().max())
        parity = dict(compared_rows=len(compared), maximum_absolute_difference=maximum_difference,
            tolerance=1e-7, explanation="Opening price-change float32 storage causes maximum4.3e-8percentage-point drift; remaining consumed fields match exactly.")
    metadata = dict(day=str(DAY), universe=SIDES, nifty_opening_return_pct=nifty,
        production_feature_parity=parity,
        input_provenance=inputs, code_provenance=[dict(path=str(p),sha256=sha256(p)) for p in
            (Path(__file__), Path(source.__file__), Path(simulator.__file__), Path(simulator.g.v9.v5.__file__), Path(source.hybrid.__file__))],
        errors=errors, rules=dict(oi="Diagnostic only; neither sign nor magnitude selects entries",
            move_minimum_pct=.20, volume_variants=[.40,.80], confirmation_body_minimum=.30,
            confirmation_volume_minimum=.50, directional_wick_maximum=.60,
            stop_pct=1.25, tightened_stop_pct=1.0, tighten_after_minutes=120, target_pct=1.50,
            round_trip_cost_bps=5, breakout_valid_minutes=10, scan_clocks="Every5min09:20to13:30"),
        selection="Earliest filled qualifying candidate per variant and requested stock, never selected by return. No cross-stock quota or portfolio allocator applied.",
        timing="Input timestamps label completed candle ends. Features use no later price. Strict breakout starts in the next candle, fill tick unknown within its start/end interval. Next-open assumes zero order-submission latency at confirmation close; optimistic for live. Opening-price-only uses completed09:20bar and begins09:20-09:21bar without looking at09:21confirmation.",
        limitations=["Six names were supplied after this day's outcomes: selection bias, not proof of robust performance.",
            "One-minute OHLC execution cannot reproduce spreads, queues, tick sequence, circuit-lock liquidity, or live latency.",
            "Native stop-first ambiguity policy and conservative entry-candle stop timing retained.",
            "MFE/MAE use complete entry/exit candles and can include price movement before fill or after exit within those candles.",
            "Opening OI is retrospectively reconstructed from sealed inputs and diagnostic only in these rules.",
            "Summed trade returns are neither a portfolio return nor an equal-capital return unless divided by total invested exposure."])
    (output / "audit_metadata.json").write_text(json.dumps(metadata, indent=2, default=str), encoding="utf-8")
    print(summary.to_string(index=False))
    print(earliest[["variant","tradingsymbol","signal_ts","entry_bar_end_ts","entry_price","exit_reason","net_return_pct"]].to_string(index=False))
    print(f"Artifacts: {output}")
    return earliest, summary


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--snapshot", type=Path, default=DEFAULT_SNAPSHOT)
    parser.add_argument("--output", type=Path, default=ROOT / "outputs/g3_early_entry_20261023/individual")
    args = parser.parse_args()
    run(args.snapshot, args.output)
