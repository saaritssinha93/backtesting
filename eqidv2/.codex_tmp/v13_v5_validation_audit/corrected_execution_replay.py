"""Read-only V13-v3 execution replay using raw 1m opens and explicit cutoffs."""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd

import fno_oi_hybrid_data as hybrid


TRADES_PATH = Path(
    r"C:\TradingData\eqidv2\fno_oi\strategy_research\v13_corrected_v3"
) / "fno_v13_corrected_v3_trades.csv"
COST_BPS = 5.0


def replay(cutoff: str, gap_open_fill: bool) -> pd.DataFrame:
    trades = pd.read_csv(TRADES_PATH)
    records: list[dict[str, object]] = []
    for symbol, group in trades.groupby("tradingsymbol", sort=True):
        minute = hybrid.load_equity_one_minute(str(symbol))
        minute = minute.sort_values("ts").drop_duplicates("ts", keep="last").reset_index(drop=True)
        minute_ns = minute["ts"].astype("int64").to_numpy()
        for idx_out, row in group.iterrows():
            confirmation = pd.Timestamp(row["confirmation_ts"])
            if confirmation.tzinfo is None:
                confirmation = confirmation.tz_localize("Asia/Kolkata")
            else:
                confirmation = confirmation.tz_convert("Asia/Kolkata")
            idx = int(np.searchsorted(minute_ns, confirmation.value))
            if idx >= len(minute_ns) or minute_ns[idx] != confirmation.value:
                raise AssertionError(f"missing confirmation: {symbol} {confirmation}")
            end_idx = min(idx + 1 + 400, len(minute))
            path = minute.iloc[idx + 1 : end_idx].copy()
            path = path.loc[
                path["ts"].dt.date.eq(confirmation.date())
                & path["ts"].dt.strftime("%H%M").le(cutoff)
            ].reset_index(drop=True)
            side = str(row["side"])
            is_long = side == "LONG"
            trigger = float(row["trigger"])
            touched = (
                np.flatnonzero(path["high"].to_numpy(float) >= trigger)
                if is_long
                else np.flatnonzero(path["low"].to_numpy(float) <= trigger)
            )
            if not touched.size:
                records.append(
                    {
                        "source_index": int(idx_out),
                        "day": row["day"],
                        "symbol": symbol,
                        "setup_id": row["setup_id"],
                        "filled": False,
                        "net_return_pct": np.nan,
                        "entry_gap": False,
                        "entry_ts": None,
                        "exit_ts": None,
                        "exit_reason": "UNFILLED",
                    }
                )
                continue
            entry_i = int(touched[0])
            bar_open = float(path.iloc[entry_i]["open"])
            entry_gap = bool(bar_open > trigger if is_long else bar_open < trigger)
            entry = bar_open if gap_open_fill and entry_gap else trigger
            stop_pct = float(row["stop_pct"])
            target_pct = float(row["target_pct"])
            if is_long:
                stop = entry * (1.0 - stop_pct / 100.0)
                target = entry * (1.0 + target_pct / 100.0)
                stop_hits = np.flatnonzero(path["low"].to_numpy(float)[entry_i:] <= stop)
                target_hits = np.flatnonzero(path["high"].to_numpy(float)[entry_i:] >= target)
            else:
                stop = entry * (1.0 + stop_pct / 100.0)
                target = entry * (1.0 - target_pct / 100.0)
                stop_hits = np.flatnonzero(path["high"].to_numpy(float)[entry_i:] >= stop)
                target_hits = np.flatnonzero(path["low"].to_numpy(float)[entry_i:] <= target)
            never = np.iinfo(np.int32).max
            stop_i = int(stop_hits[0]) if stop_hits.size else int(never)
            target_i = int(target_hits[0]) if target_hits.size else int(never)
            if stop_i == target_i == never:
                exit_price = float(path.iloc[-1]["close"])
                exit_i = len(path) - 1
                reason = "SQUAREOFF"
            elif stop_i <= target_i:
                exit_price = stop
                exit_i = entry_i + stop_i
                reason = "STOP"
            else:
                exit_price = target
                exit_i = entry_i + target_i
                reason = "TARGET"
            gross = exit_price / entry - 1.0 if is_long else 1.0 - exit_price / entry
            records.append(
                {
                    "source_index": int(idx_out),
                    "day": row["day"],
                    "symbol": symbol,
                    "setup_id": row["setup_id"],
                    "filled": True,
                    "net_return_pct": (gross - COST_BPS / 10_000.0) * 100.0,
                    "entry_gap": entry_gap,
                    "entry_price": entry,
                    "trigger": trigger,
                    "entry_ts": str(path.iloc[entry_i]["ts"]),
                    "exit_ts": str(path.iloc[exit_i]["ts"]),
                    "exit_reason": reason,
                }
            )
    return pd.DataFrame(records).sort_values("source_index").reset_index(drop=True)


def metrics(frame: pd.DataFrame, days: set[str] | None = None) -> dict[str, object]:
    use = frame if days is None else frame.loc[frame["day"].isin(days)]
    values = use.loc[use["filled"], "net_return_pct"].to_numpy(float)
    profit = float(values[values > 0].sum())
    loss = float(-values[values < 0].sum())
    daily_values = (
        use.loc[use["filled"]].groupby("day", sort=True)["net_return_pct"].sum().to_numpy(float)
    )
    curve = np.r_[0.0, np.cumsum(daily_values)]
    drawdown = curve - np.maximum.accumulate(curve)
    return {
        "orders": int(len(use)),
        "fills": int(use["filled"].sum()),
        "wins": int((values > 0).sum()),
        "losses": int((values < 0).sum()),
        "win_rate_pct": float((values > 0).mean() * 100.0),
        "pf": profit / loss if loss else None,
        "net_pct": float(values.sum()),
        "expectancy_pct": float(values.mean()),
        "max_drawdown_pct": float(drawdown.min()),
    }


def main() -> None:
    published = pd.read_csv(TRADES_PATH)
    native = replay("1530", gap_open_fill=False)
    gap_corrected = replay("1530", gap_open_fill=True)
    uniform_1515 = replay("1515", gap_open_fill=True)
    train = set(published.loc[pd.to_datetime(published["day"]) < pd.Timestamp("2026-08-14"), "day"])
    test = set(
        published.loc[
            pd.to_datetime(published["day"]).between("2026-08-14", "2026-09-01"), "day"
        ]
    )
    latest = set(published.loc[pd.to_datetime(published["day"]) > pd.Timestamp("2026-09-01"), "day"])
    published_returns = pd.to_numeric(published["net_return_pct"], errors="coerce").to_numpy(float)
    gap_delta = gap_corrected["net_return_pct"].to_numpy(float) - published_returns
    cutoff_delta = uniform_1515["net_return_pct"].to_numpy(float) - published_returns
    result = {
        "native_max_abs_parity_delta": float(
            np.nanmax(np.abs(native["net_return_pct"].to_numpy(float) - published_returns))
        ),
        "gap_corrected": {
            "all": metrics(gap_corrected),
            "train": metrics(gap_corrected, train),
            "test": metrics(gap_corrected, test),
            "latest": metrics(gap_corrected, latest),
            "gap_fill_count": int(gap_corrected["entry_gap"].sum()),
            "changed_return_count_at_1e_10": int(np.nansum(np.abs(gap_delta) > 1e-10)),
            "max_abs_return_delta": float(np.nanmax(np.abs(gap_delta))),
            "gap_trades": gap_corrected.loc[
                gap_corrected["entry_gap"],
                ["day", "symbol", "setup_id", "trigger", "entry_price", "entry_ts", "exit_ts", "exit_reason", "net_return_pct"],
            ].to_dict("records"),
        },
        "uniform_1515_gap_corrected": {
            "all": metrics(uniform_1515),
            "train": metrics(uniform_1515, train),
            "test": metrics(uniform_1515, test),
            "latest": metrics(uniform_1515, latest),
            "changed_return_count_at_1e_10": int(np.nansum(np.abs(cutoff_delta) > 1e-10)),
            "changed_trades": uniform_1515.loc[
                np.nan_to_num(np.abs(cutoff_delta), nan=0.0) > 1e-10,
                ["day", "symbol", "setup_id", "entry_ts", "exit_ts", "exit_reason", "net_return_pct"],
            ].assign(published_net_pct=published.loc[np.nan_to_num(np.abs(cutoff_delta), nan=0.0) > 1e-10, "net_return_pct"].to_numpy()).to_dict("records"),
        },
    }
    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
