"""Entry-only, read-only preview for the frozen G-3 rule before day end.

The preview uses the native G feature and selection functions. It deliberately
does not simulate exits or report P&L from an incomplete execution path.
"""
from __future__ import annotations

import argparse
import json
from datetime import date, datetime
from pathlib import Path
from unittest.mock import patch

import numpy as np
import pandas as pd

import fno_oi_common as common
import fno_oi_hybrid_data as hybrid
import fno_oi_backtest_provenance as provenance
import fno_v13_v10_g_2_backtest as g2
import fno_v13_v10_g_daily_replay as replay
import fno_v13_v10_g_live_config as config
from research import g3_freeze
from research import g3_six_confirmation_variants as variants


def run(day: date, output: Path) -> dict:
    if output.exists() and any(output.iterdir()):
        raise FileExistsError(f"Refusing to overwrite preview: {output}")
    frozen = g3_freeze.load_frozen()
    bundle = g2.load_bundle(g2.DEFAULT_SOURCE_BUNDLE, g2.DEFAULT_G_CONFIG)
    if frozen["config"]["selection_change"] != bundle["source_g"]["selection_change"]:
        raise RuntimeError("G-3 frozen selection differs from sealed G-2 source")
    universe_path = common.UNIVERSE_DIR / f"near_month_{day}.parquet"
    universe = pd.read_parquet(universe_path)
    stocks = universe.loc[~universe.is_index_future.fillna(False).astype(bool)]
    expiry = pd.to_datetime(stocks.expiry, errors="coerce").dropna().unique()
    if len(expiry) != 1:
        raise RuntimeError("Expected one near-month stock expiry")
    month = pd.Timestamp(expiry[0]).strftime("%y%b").upper()
    mapped, _ = provenance.load_backtest_universe(
        universe_path=universe_path, universe_date=day,
        contract_month_contains=month, require_persisted_mapping=True)
    nifty = universe.loc[universe.underlying.astype(str).str.upper().eq("NIFTY")]
    if len(nifty) != 1:
        raise RuntimeError("Missing unique dated NIFTY context")
    nifty_path = common.RAW_CONTRACT_DIR / f"{common.safe_contract_stem(str(nifty.iloc[0].tradingsymbol))}_5minute.parquet"
    problems: list = []
    nifty_bars = replay._load_future(nifty_path, day, problems, "NIFTY")
    nifty_return = config.nifty_context_from_bars(nifty_bars, day)
    if not np.isfinite(nifty_return):
        raise RuntimeError("Missing first NIFTY 5-minute context")
    signals = []
    data_latest = []
    excluded_oi = []
    _, _, required_oi = replay._slot_times(day)
    final_signal = max(config.SIGNAL_TO_CONFIRMATION)
    last_entry_end = pd.Timestamp(config.slot_datetime(day, final_signal)) + pd.Timedelta(minutes=11)
    required_equity = pd.date_range(config.slot_datetime(day, "09:16"), last_entry_end, freq="min")
    # Effective-input hashes are telemetry, not strategy features. Suppressing
    # their repeated full-history calculation makes this entry-only read fast;
    # neither selection, confirmation nor trigger logic is patched.
    with patch.object(replay, "canonical_frame_sha256", return_value="ENTRY_PREVIEW_NO_TELEMETRY_HASH"), \
         patch.object(replay, "canonical_payload_sha256", return_value="ENTRY_PREVIEW_NO_TELEMETRY_HASH"):
        for number, contract in enumerate(mapped.to_dict("records"), 1):
            symbol = hybrid.resolve_backtest_equity_symbol(str(contract["equity_symbol"]))
            future_symbol = str(contract["futures_tradingsymbol"])
            minute = replay._load_minute(hybrid.equity_one_minute_path(symbol, hybrid.DEFAULT_BACKTEST_EQUITY_1M_DIR), day, problems, symbol)
            future = replay._load_future(
                common.RAW_CONTRACT_DIR / f"{common.safe_contract_stem(future_symbol)}_5minute.parquet",
                day, problems, symbol)
            missing_equity = required_equity.difference(pd.DatetimeIndex(minute.ts))
            missing_oi = required_oi.difference(pd.DatetimeIndex(future.ts))
            if len(missing_equity):
                raise RuntimeError(f"Entry-window equity data incomplete for {symbol}: {len(missing_equity)} minutes")
            if len(missing_oi):
                excluded_oi.append({"symbol": symbol, "missing_required_oi_bars": len(missing_oi)})
                continue
            data_latest.append(minute.loc[minute.ts.dt.date.eq(day), "ts"].max())
            pool = replay._observed_pool(minute, future, day=day, symbol=symbol,
                                         future_symbol=future_symbol, month=month)
            pool = pool.loc[pool.hhmm_int.isin(
                [int(clock.replace(":", "")) for clock in config.SIGNAL_TO_CONFIRMATION])].copy()
            if len(pool) != len(config.SIGNAL_TO_CONFIRMATION):
                raise RuntimeError(f"Incomplete signal-slot feature construction for {symbol}")
            signals.append(replay._strict_signals(pool, float(nifty_return)))
            if number % 25 == 0 or number == len(mapped):
                print(f"[G-3 entry preview] {number}/{len(mapped)} stocks", flush=True)
    strict = pd.concat(signals, ignore_index=True)
    strict = strict.sort_values(["tradingsymbol", "signal_ts", "side"], kind="stable").reset_index(drop=True)
    strict["sid"] = np.arange(len(strict), dtype=int)
    selected = variants._select(strict, bundle, 1.1)
    preview = []
    expiry_minutes = int(bundle["source_g"]["entry_expiry_minutes"])
    for row in selected.itertuples(index=False):
        minute = replay._load_minute(hybrid.equity_one_minute_path(str(row.tradingsymbol), hybrid.DEFAULT_BACKTEST_EQUITY_1M_DIR), day, problems,
                                     str(row.tradingsymbol))
        confirmation = pd.Timestamp(row.confirmation_ts)
        window = minute.loc[minute.ts.gt(confirmation) & minute.ts.le(
            confirmation + pd.Timedelta(minutes=expiry_minutes))]
        expected = pd.date_range(confirmation + pd.Timedelta(minutes=1),
                                 confirmation + pd.Timedelta(minutes=expiry_minutes), freq="min")
        if len(expected.difference(pd.DatetimeIndex(window.ts))):
            raise RuntimeError(f"Incomplete entry trigger window for {row.tradingsymbol} {row.setup_id}")
        path = {"timestamp_ns": window.ts.astype("int64").to_numpy(),
                **{name: window[name].to_numpy(float) for name in ("open", "high", "low", "close")}}
        fill = g2.g.v9.v5._entry(row, path, delay_bars=0, trigger_buffer_pct=0.,
                                 worse_fill_bps=0., max_entry_delay_minutes=expiry_minutes)
        preview.append({
            "symbol": str(row.tradingsymbol), "setup": str(row.setup_id), "side": str(row.side),
            "signal_ts": str(row.signal_ts), "confirmation_ts": str(row.confirmation_ts),
            "confirmation_volume_ratio": float(row.v9_1m_volume_ratio),
            "trigger": float(row.trigger), "trigger_hit": fill is not None,
            "entry_ts": str(pd.Timestamp(int(path["timestamp_ns"][fill[0]]), tz="UTC").tz_convert("Asia/Kolkata")) if fill else None,
            "entry_price": float(fill[1]) if fill else None,
            "gap_through": bool(fill[3]) if fill else None,
        })
    summary = {
        "status": "INTRADAY_ENTRY_ONLY_NOT_FINAL_BACKTEST", "day": str(day),
        "created_at": datetime.now().astimezone().isoformat(),
        "universe_stocks": len(mapped), "included_stocks": len(mapped) - len(excluded_oi),
        "excluded_oi_stocks": excluded_oi, "strict_signals": len(strict),
        "selected_orders": len(preview), "triggered_entries": sum(x["trigger_hit"] for x in preview),
        "common_equity_last_bar": str(min(data_latest)) if data_latest else None,
        "last_required_entry_bar": str(last_entry_end),
        "portfolio_execution_and_final_pnl": "NOT_DETERMINED_FROM_ENTRY_ONLY_PATH",
        "entries": preview,
    }
    output.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(preview).to_csv(output / "entry_preview.csv", index=False)
    (output / "summary.json").write_text(json.dumps(summary, indent=2, allow_nan=False), encoding="utf-8")
    return summary


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", type=date.fromisoformat, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(run(args.date, args.output_dir), indent=2))
