"""Count-only reconciliation for the six-month LONG opportunity grid."""

from __future__ import annotations

from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

import numpy as np
import pandas as pd

import research_prefilter_long_5m_gt5pct as study


BAR_COLUMNS = ("date", "open", "high", "low", "close", "volume", "gap_filled")


def _audit_ticker(item: tuple[str, pd.DataFrame]) -> tuple[dict[str, int], pd.DataFrame]:
    ticker, memberships = item
    bars = study.read_parquet_window(
        study.DEFAULT_5M_DIR / f"{ticker}_stocks_indicators_5min.parquet",
        BAR_COLUMNS,
    )
    if bars is None or bars.empty:
        return {"nominal": 0, "current": 0}, pd.DataFrame()
    bars = study.filter_end_stamped_session(
        bars, first_label=study.SESSION_FIRST_5M_END
    )
    relevant = set(memberships["trade_date"].astype(str))
    bars = bars.loc[bars["date"].dt.strftime("%Y-%m-%d").isin(relevant)].copy()
    for column in BAR_COLUMNS[1:]:
        bars[column] = pd.to_numeric(bars[column], errors="coerce")
    bars = bars.sort_values("date").drop_duplicates("date", keep="last")
    gap = bars["gap_filled"].fillna(0.0)
    membership_real = bars["open"].gt(0) & bars["close"].gt(0) & gap.lt(0.5)
    memberships = memberships.loc[
        memberships["membership_slot_ist"].isin(set(bars.loc[membership_real, "date"]))
    ].copy()
    rows = [
        {
            "ticker": ticker,
            "membership_slot_ist": slot,
            "signal_time_ist": signal,
            "entry_price_source_bar_end_ist": signal + pd.Timedelta(minutes=5),
        }
        for slot in memberships["membership_slot_ist"]
        for signal in study.signal_schedule(slot)
    ]
    grid = pd.DataFrame(rows)
    if grid.empty:
        return {"nominal": 0, "current": 0}, pd.DataFrame()
    signal = bars.rename(columns={column: f"signal_{column}" for column in BAR_COLUMNS[1:]})
    signal = signal.rename(columns={"date": "signal_time_ist"})
    entry = bars.rename(columns={column: f"entry_{column}" for column in BAR_COLUMNS[1:]})
    entry = entry.rename(columns={"date": "entry_price_source_bar_end_ist"})
    grid = grid.merge(signal, on="signal_time_ist", how="left")
    grid = grid.merge(entry, on="entry_price_source_bar_end_ist", how="left")
    current = (
        grid["signal_close"].gt(0)
        & grid["entry_open"].gt(0)
        & grid["signal_gap_filled"].fillna(0.0).lt(0.5)
        & grid["entry_gap_filled"].fillna(0.0).lt(0.5)
    )
    signal_ohlc = (
        grid[["signal_open", "signal_high", "signal_low", "signal_close"]]
        .gt(0)
        .all(axis=1)
        & grid["signal_high"].ge(
            grid[["signal_open", "signal_low", "signal_close"]].max(axis=1)
        )
        & grid["signal_low"].le(
            grid[["signal_open", "signal_high", "signal_close"]].min(axis=1)
        )
    )
    entry_ohlc = (
        grid[["entry_open", "entry_high", "entry_low", "entry_close"]]
        .gt(0)
        .all(axis=1)
        & grid["entry_high"].ge(
            grid[["entry_open", "entry_low", "entry_close"]].max(axis=1)
        )
        & grid["entry_low"].le(
            grid[["entry_open", "entry_high", "entry_close"]].min(axis=1)
        )
    )
    strict_ohlc = current & signal_ohlc & entry_ohlc
    strict_volume_finite = strict_ohlc & np.isfinite(grid["signal_volume"]) & np.isfinite(
        grid["entry_volume"]
    )
    strict_volume_positive = strict_volume_finite & grid["signal_volume"].gt(0) & grid[
        "entry_volume"
    ].gt(0)
    delta = grid.loc[current & ~strict_volume_positive].copy()
    delta["signal_ohlc_valid"] = signal_ohlc.loc[delta.index]
    delta["entry_ohlc_valid"] = entry_ohlc.loc[delta.index]
    delta["signal_volume_finite"] = np.isfinite(delta["signal_volume"])
    delta["entry_volume_finite"] = np.isfinite(delta["entry_volume"])
    return {
        "nominal": len(grid),
        "current": int(current.sum()),
        "strict_ohlc": int(strict_ohlc.sum()),
        "strict_volume_finite": int(strict_volume_finite.sum()),
        "strict_volume_positive": int(strict_volume_positive.sum()),
    }, delta


def main() -> int:
    memberships, _ = study.load_memberships(study.DEFAULT_PREFILTER)
    groups = [(ticker, group.copy()) for ticker, group in memberships.groupby("ticker")]
    counts: list[dict[str, int]] = []
    deltas: list[pd.DataFrame] = []
    with ProcessPoolExecutor(max_workers=4) as executor:
        futures = [executor.submit(_audit_ticker, item) for item in groups]
        for future in as_completed(futures):
            count, delta = future.result()
            counts.append(count)
            if not delta.empty:
                deltas.append(delta)
    totals = pd.DataFrame(counts).sum().astype(int)
    print(totals.to_string())
    output = study.DEFAULT_OUT / "opportunity_count_reconciliation_rows.csv"
    output.parent.mkdir(parents=True, exist_ok=True)
    pd.concat(deltas, ignore_index=True).to_csv(output, index=False)
    print(output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
