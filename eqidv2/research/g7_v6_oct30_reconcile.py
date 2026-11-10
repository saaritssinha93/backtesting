"""Compare recovered historical minutes with stored live five-minute entry bars."""
from __future__ import annotations

import json
from pathlib import Path
import time

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
CASH = ROOT / "outputs/g7_v6_20261102/oct30_retrieved_cash"
LIVE = Path("C:/TradingData/eqidv2/stocks_indicators_5min_eq_live")
DAY = pd.Timestamp("2026-10-30").date()


def main() -> None:
    rows = []
    for cash_path in sorted(CASH.glob("*.parquet")):
        symbol = cash_path.name.split("_stocks_indicators")[0]
        live_path = LIVE / f"{symbol}_stocks_indicators_5min.parquet"
        if not live_path.is_file():
            raise FileNotFoundError(live_path)
        cash = pd.read_parquet(cash_path, columns=["date", "open", "high", "low", "close", "volume"])
        cash = cash.loc[pd.to_datetime(cash.date).dt.date == DAY].set_index("date")
        agg = cash.resample("5min", label="right", closed="right").agg(
            {"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"}).dropna()
        for attempt in range(5):
            try:
                live = pd.read_parquet(live_path,
                    columns=["date", "open", "high", "low", "close", "volume", "source_1m_count"])
                break
            except PermissionError:
                if attempt == 4:
                    raise
                time.sleep(0.4)
        live = live.loc[(pd.to_datetime(live.date).dt.date == DAY) & live.source_1m_count.eq(5)].set_index("date")
        if len(agg) != 72 or len(live) != 72 or len(agg.index.intersection(live.index)) != 72:
            raise ValueError("Five-minute inventory incomplete: " + symbol)
        prices = ["open", "high", "low", "close"]
        price_delta = np.abs(agg[prices].to_numpy(float) - live[prices].to_numpy(float))
        volume_delta = np.abs(agg.volume.to_numpy(float) - live.volume.to_numpy(float))
        price_conflict = np.any(price_delta > 1e-6, axis=1)
        volume_conflict = volume_delta > 1e-3
        # The frozen G7 V4 opportunity book has confirmations as late as 13:31.
        entry_window = (agg.index.hour * 60 + agg.index.minute) <= 811
        if (price_conflict | volume_conflict).any():
            conflict_times = agg.index[price_conflict | volume_conflict]
            earliest = min(conflict_times.hour * 60 + conflict_times.minute)
            latest = max(conflict_times.hour * 60 + conflict_times.minute)
        else:
            earliest = latest = None
        rows.append(dict(symbol=symbol, price_conflict_bars=int(price_conflict.sum()),
                         volume_conflict_bars=int(volume_conflict.sum()),
                         entry_window_price_conflict_bars=int((price_conflict & entry_window).sum()),
                         entry_window_volume_conflict_bars=int((volume_conflict & entry_window).sum()),
                         max_price_difference=float(price_delta.max()),
                         max_volume_difference=float(volume_delta.max()),
                         earliest_conflict_minute_ist=earliest,
                         latest_conflict_minute_ist=latest))
    detail = pd.DataFrame(rows)
    detail.to_csv(CASH / "live_5m_reconciliation.csv", index=False)
    summary = {"stocks": len(detail), "price_conflict_stocks": int(detail.price_conflict_bars.gt(0).sum()),
               "volume_only_conflict_stocks": int((detail.price_conflict_bars.eq(0) &
                                                      detail.volume_conflict_bars.gt(0)).sum()),
               "price_conflict_bars": int(detail.price_conflict_bars.sum()),
               "volume_conflict_bars": int(detail.volume_conflict_bars.sum()),
               "entry_window_price_conflict_stocks": int(detail.entry_window_price_conflict_bars.gt(0).sum()),
               "entry_window_volume_conflict_stocks": int(detail.entry_window_volume_conflict_bars.gt(0).sum()),
               "entry_window_price_conflict_bars": int(detail.entry_window_price_conflict_bars.sum()),
               "entry_window_volume_conflict_bars": int(detail.entry_window_volume_conflict_bars.sum()),
               "method": "End-timestamped recovered 1m aggregate versus archived completed live 5m OHLCV; entry window through 13:31 IST",
               "entry_causality_implication": "Stocks with entry-window conflicts need signal-level validation before promotion."}
    (CASH / "live_5m_reconciliation.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
