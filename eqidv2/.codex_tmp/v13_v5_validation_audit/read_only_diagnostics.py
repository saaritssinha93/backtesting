"""Read-only diagnostics for the frozen V13-v3 audit.

This helper does not write data.  It inspects the published CSV artifacts and
the current raw equity minute store to quantify execution-data ambiguities.
"""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd

import fno_oi_hybrid_data as hybrid


RESULT = Path(r"C:\TradingData\eqidv2\fno_oi\strategy_research\v13_corrected_v3")
TRADES = RESULT / "fno_v13_corrected_v3_trades.csv"
CANDIDATES = RESULT / "fno_v13_corrected_v3_nifty_gate_audit.csv"


def flagged(frame: pd.DataFrame) -> np.ndarray:
    out = np.zeros(len(frame), dtype=bool)
    for column in ("gap_filled", "opening_snapshot", "provisional_stale"):
        if column not in frame.columns:
            continue
        values = frame[column]
        out |= (
            pd.to_numeric(values, errors="coerce").fillna(0).ne(0)
            | values.astype(str).str.strip().str.lower().isin({"true", "yes", "on"})
        ).to_numpy(bool)
    return out


def inspect_rows(rows: pd.DataFrame, *, simulate: bool) -> dict[str, object]:
    counters: dict[str, int] = {
        "rows": len(rows),
        "missing_equity_file": 0,
        "missing_confirmation_timestamp": 0,
        "invalid_confirmation_ohlcv": 0,
        "flagged_confirmation": 0,
    }
    if simulate:
        counters.update(
            {
                "forward_path_has_minute_gap": 0,
                "forward_path_has_internal_gap": 0,
                "forward_path_missing_minutes_total": 0,
                "forward_path_has_flagged_row": 0,
                "forward_path_has_invalid_ohlcv": 0,
                "forward_path_ends_before_1530": 0,
                "engine_fills": 0,
                "engine_unfilled": 0,
                "entry_gap_through_trigger": 0,
                "fill_bar_also_hits_stop": 0,
                "fill_bar_also_hits_target": 0,
                "stop_and_target_first_hit_same_bar": 0,
                "squareoff_exits": 0,
                "squareoff_exits_before_1530": 0,
                "exits_with_pre_exit_minute_gap": 0,
                "published_return_mismatch": 0,
            }
        )

    details: list[dict[str, object]] = []
    for symbol, group in rows.groupby("tradingsymbol", sort=True):
        minute = hybrid.load_equity_one_minute(str(symbol))
        if minute.empty:
            counters["missing_equity_file"] += len(group)
            continue
        minute = minute.sort_values("ts").drop_duplicates("ts", keep="last").reset_index(drop=True)
        ts_ns = minute["ts"].astype("int64").to_numpy()
        src_flag = flagged(minute)
        o = minute["open"].to_numpy(float)
        h = minute["high"].to_numpy(float)
        l = minute["low"].to_numpy(float)
        c = minute["close"].to_numpy(float)
        v = minute["volume"].to_numpy(float)
        hhmm = minute["ts"].dt.strftime("%H%M").to_numpy()
        days = minute["ts"].dt.date.to_numpy()

        for row in group.itertuples(index=False):
            confirmation = pd.Timestamp(row.confirmation_ts)
            if confirmation.tzinfo is None:
                confirmation = confirmation.tz_localize("Asia/Kolkata")
            else:
                confirmation = confirmation.tz_convert("Asia/Kolkata")
            idx = int(np.searchsorted(ts_ns, confirmation.value))
            if idx >= len(ts_ns) or ts_ns[idx] != confirmation.value:
                counters["missing_confirmation_timestamp"] += 1
                continue
            confirm_values = np.array([o[idx], h[idx], l[idx], c[idx], v[idx]], dtype=float)
            confirm_valid = bool(
                np.isfinite(confirm_values).all()
                and l[idx] > 0
                and h[idx] > l[idx]
                and h[idx] >= max(o[idx], c[idx])
                and l[idx] <= min(o[idx], c[idx])
                and v[idx] >= 0
            )
            counters["invalid_confirmation_ohlcv"] += int(not confirm_valid)
            counters["flagged_confirmation"] += int(src_flag[idx])

            # The all-candidate pass is only a confirmation-lineage audit.  Full
            # path diagnostics are needed only for the 79 selected orders.
            if not simulate:
                continue

            stop_idx = idx + 1
            end_idx = min(stop_idx + 400, len(ts_ns))
            positions = np.arange(stop_idx, end_idx)
            keep = (days[positions] == confirmation.date()) & (hhmm[positions] <= "1530")
            positions = positions[keep]
            if not len(positions):
                continue
            path_ts = minute.loc[positions, "ts"].reset_index(drop=True)
            expected = pd.DatetimeIndex(pd.date_range(
                start=confirmation + pd.Timedelta(minutes=1),
                end=confirmation.normalize() + pd.Timedelta(hours=15, minutes=30),
                freq="min",
            ))
            observed = pd.DatetimeIndex(path_ts)
            expected_ns = expected.asi8
            observed_ns = observed.asi8
            one_minute_ns = 60_000_000_000
            missing = np.setdiff1d(expected_ns, observed_ns, assume_unique=True)
            has_gap = bool(len(missing))
            expected_to_last = np.arange(
                expected_ns[0], observed_ns[-1] + one_minute_ns, one_minute_ns
            )
            internal_missing = np.setdiff1d(
                expected_to_last, observed_ns, assume_unique=True
            )
            counters["forward_path_has_minute_gap"] += int(has_gap)
            counters["forward_path_has_internal_gap"] += int(bool(len(internal_missing)))
            counters["forward_path_missing_minutes_total"] += int(len(missing))
            counters["forward_path_has_flagged_row"] += int(src_flag[positions].any())
            path_values = np.column_stack([o[positions], h[positions], l[positions], c[positions], v[positions]])
            invalid_path = not bool(np.isfinite(path_values).all())
            counters["forward_path_has_invalid_ohlcv"] += int(invalid_path)
            ends_1530 = str(path_ts.iloc[-1].strftime("%H%M")) == "1530"
            counters["forward_path_ends_before_1530"] += int(not ends_1530)

            if not simulate:
                continue
            side = str(row.side)
            long_side = side == "LONG"
            trigger = float(row.trigger)
            touched = np.flatnonzero(h[positions] >= trigger) if long_side else np.flatnonzero(l[positions] <= trigger)
            if not touched.size:
                counters["engine_unfilled"] += 1
                published = pd.to_numeric(pd.Series([row.net_return_pct]), errors="coerce").iloc[0]
                counters["published_return_mismatch"] += int(pd.notna(published))
                continue
            counters["engine_fills"] += 1
            e = int(touched[0])
            entry_pos = positions[e]
            gap = o[entry_pos] > trigger if long_side else o[entry_pos] < trigger
            counters["entry_gap_through_trigger"] += int(gap)
            stop_pct = float(row.stop_pct)
            target_pct = float(row.target_pct)
            if long_side:
                stop = trigger * (1.0 - stop_pct / 100.0)
                target = trigger * (1.0 + target_pct / 100.0)
                hit_stop = np.flatnonzero(l[positions[e:]] <= stop)
                hit_target = np.flatnonzero(h[positions[e:]] >= target)
                fill_stop = l[entry_pos] <= stop
                fill_target = h[entry_pos] >= target
            else:
                stop = trigger * (1.0 + stop_pct / 100.0)
                target = trigger * (1.0 - target_pct / 100.0)
                hit_stop = np.flatnonzero(h[positions[e:]] >= stop)
                hit_target = np.flatnonzero(l[positions[e:]] <= target)
                fill_stop = h[entry_pos] >= stop
                fill_target = l[entry_pos] <= target
            counters["fill_bar_also_hits_stop"] += int(fill_stop)
            counters["fill_bar_also_hits_target"] += int(fill_target)
            never = np.iinfo(np.int32).max
            s_i = int(hit_stop[0]) if hit_stop.size else int(never)
            t_i = int(hit_target[0]) if hit_target.size else int(never)
            tie = s_i == t_i and s_i != never
            counters["stop_and_target_first_hit_same_bar"] += int(tie)
            if s_i == t_i == never:
                exit_price = c[positions[-1]]
                exit_kind = "SQUAREOFF"
                exit_rel = len(positions) - 1
                counters["squareoff_exits"] += 1
            elif s_i <= t_i:
                exit_price = stop
                exit_kind = "STOP"
                exit_rel = e + s_i
            else:
                exit_price = target
                exit_kind = "TARGET"
                exit_rel = e + t_i
            expected_to_exit = np.arange(
                expected_ns[0], observed_ns[exit_rel] + one_minute_ns, one_minute_ns
            )
            pre_exit_missing = np.setdiff1d(
                expected_to_exit, observed_ns[: exit_rel + 1], assume_unique=True
            )
            counters["exits_with_pre_exit_minute_gap"] += int(bool(len(pre_exit_missing)))
            counters["squareoff_exits_before_1530"] += int(
                exit_kind == "SQUAREOFF" and not ends_1530
            )
            gross = (exit_price / trigger - 1.0) if long_side else (1.0 - exit_price / trigger)
            calculated = (gross - 5.0 / 10000.0) * 100.0
            published = float(row.net_return_pct)
            mismatch = not np.isclose(calculated, published, rtol=0.0, atol=1e-10, equal_nan=True)
            counters["published_return_mismatch"] += int(mismatch)
            if gap or fill_stop or fill_target or tie or (exit_kind == "SQUAREOFF" and not ends_1530) or src_flag[idx]:
                details.append(
                    {
                        "day": str(row.day),
                        "symbol": symbol,
                        "setup_id": row.setup_id,
                        "gap": bool(gap),
                        "fill_bar_stop": bool(fill_stop),
                        "fill_bar_target": bool(fill_target),
                        "same_bar_stop_target": bool(tie),
                        "exit_kind": exit_kind,
                        "entry_ts": str(path_ts.iloc[e]),
                        "exit_ts": str(path_ts.iloc[exit_rel]),
                        "internal_path_gap": bool(len(internal_missing)),
                        "missing_minutes_before_exit": int(len(pre_exit_missing)),
                        "missing_minutes_full_path": int(len(missing)),
                        "confirmation_flagged": bool(src_flag[idx]),
                    }
                )
    return {"counts": counters, "ambiguous_or_quality_details": details}


def main() -> None:
    trades = pd.read_csv(TRADES)
    candidates = pd.read_csv(CANDIDATES)
    result = {
        "selected": inspect_rows(trades, simulate=True),
        "all_cached_candidates": inspect_rows(candidates, simulate=False),
        "duplicate_order_keys": int(
            trades.duplicated(["day", "hhmm_int", "side", "setup_id", "tradingsymbol"]).sum()
        ),
        "same_day_symbol_multiple_orders": int(
            trades.groupby(["day", "tradingsymbol"]).size().gt(1).sum()
        ),
        "max_orders_one_symbol_one_day": int(
            trades.groupby(["day", "tradingsymbol"]).size().max()
        ),
        "max_fills_in_day": int(trades.loc[trades["filled"]].groupby("day").size().max()),
    }
    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
