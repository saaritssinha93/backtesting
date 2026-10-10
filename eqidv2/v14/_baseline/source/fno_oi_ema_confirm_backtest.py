"""5-minute EMA/OI signal with a 1-minute confirmation-candle entry.

    LONG                                  SHORT
    5M: EMA9 > EMA20 > EMA50              5M: EMA9 < EMA20 < EMA50
        price change >= +0.20%                price change <= -0.20%
        OI change    >= +0.50%                OI change    >= +0.50%
        volume ratio >= 1.5                   volume ratio >= 1.5
        OI increasing                         OI increasing

    1M: close > open                      1M: close < open
        close > 5m signal close               close < 5m signal close
        body/range >= 0.50                    body/range >= 0.50
        no large upper wick                   no large lower wick

    entry: above the 1m candle high       entry: below the 1m candle low

Conventions and choices, since they change the answer:

* **Bars are labelled by candle END time.** A "9:25 5-minute signal" is the
  09:20-09:25 candle; the confirming 1-minute candle is 09:25-09:26, labelled
  09:26.
* **EMAs run continuously across sessions**, not reset daily. At 09:25 only two
  5-minute bars have printed today, so a daily reset would leave EMA20 and
  EMA50 undefined and the setup unusable at this time of day.
* **Entry is a stop order at the confirmation candle's extreme**, filled only if
  a later 1-minute bar trades through that level. Fill price is the trigger
  level itself. If the level never trades, there is no trade.
* **Exits are not part of the supplied spec**, so a structural default is used:
  stop at the opposite end of the confirmation candle, target at a multiple of
  that risk, and a square-off at 15:30. All three are CLI-configurable; nothing
  here is tuned.
* When a single 1-minute bar spans both target and stop, the **stop** is taken.

1-minute futures bars are not part of the live pipeline, so this module fetches
them on demand and caches them under ``fno_oi/raw_contracts_1m/``.
"""

from __future__ import annotations

import argparse
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any

import numpy as np
import pandas as pd

import fno_oi_common as common


SESSION = "fno_oi_ema_confirm_backtest"

MINUTE_DIR = common.FNO_ROOT / "raw_contracts_1m"
RESULT_DIR = common.FNO_ROOT / "strategy_research"
REPORT_PATH = common.LATEST_DIR / "latest_fno_oi_ema_confirm_backtest.md"

EMA_SPANS = (9, 20, 50)


@dataclass(frozen=True)
class Params:
    price_change_pct: float = 0.20
    oi_change_pct: float = 0.50
    volume_ratio: float = 1.5
    body_ratio: float = 0.50
    max_wick_ratio: float = 0.30
    risk_multiple: float = 2.0
    # Fixed brackets measured from the entry price. When either is > 0 it
    # replaces the confirmation-candle-derived level on that leg.
    stop_pct: float = 0.0
    target_pct: float = 0.0
    square_off: str = "1530"
    cost_bps: float = 5.0
    min_traded_value: float = 0.0


# ---------------------------------------------------------------------------
# 5-minute side
# ---------------------------------------------------------------------------

def load_five_minute(symbol: str) -> pd.DataFrame:
    path = common.raw_contract_path(symbol)
    if not path.exists():
        return pd.DataFrame()
    frame = pd.read_parquet(
        path, columns=["timestamp", "open", "high", "low", "close", "volume", "oi"]
    )
    frame["ts"] = pd.to_datetime(frame["timestamp"], utc=True).dt.tz_convert(common.IST)
    return frame.sort_values("ts").reset_index(drop=True)


def add_five_minute_features(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    for span in EMA_SPANS:
        out[f"ema{span}"] = out["close"].ewm(span=span, adjust=False).mean()
    out["prev_close"] = out["close"].shift(1)
    out["prev_oi"] = out["oi"].shift(1)
    out["price_change_pct"] = (out["close"] / out["prev_close"] - 1.0) * 100.0
    out["oi_change_pct"] = (out["oi"] / out["prev_oi"] - 1.0) * 100.0
    prior_volume = out["volume"].shift(1).rolling(20, min_periods=5).mean()
    out["volume_ratio"] = out["volume"].div(prior_volume.where(prior_volume.gt(0)))
    out["traded_value"] = out["close"] * out["volume"]
    return out


def five_minute_signal(row: pd.Series, params: Params) -> str | None:
    """Return 'LONG', 'SHORT' or None for one 5-minute bar."""

    needed = ("ema9", "ema20", "ema50", "price_change_pct", "oi_change_pct",
              "volume_ratio", "oi", "prev_oi")
    if any(pd.isna(row.get(k)) for k in needed):
        return None
    if row["traded_value"] < params.min_traded_value:
        return None
    oi_rising = row["oi"] > row["prev_oi"]
    if not oi_rising:
        return None
    if row["oi_change_pct"] < params.oi_change_pct:
        return None
    if row["volume_ratio"] < params.volume_ratio:
        return None

    bull_stack = row["ema9"] > row["ema20"] > row["ema50"]
    bear_stack = row["ema9"] < row["ema20"] < row["ema50"]
    if bull_stack and row["price_change_pct"] >= params.price_change_pct:
        return "LONG"
    if bear_stack and row["price_change_pct"] <= -params.price_change_pct:
        return "SHORT"
    return None


# ---------------------------------------------------------------------------
# 1-minute side
# ---------------------------------------------------------------------------

def minute_path(symbol: str, day: date) -> Any:
    return MINUTE_DIR / day.isoformat() / f"{common.safe_contract_stem(symbol)}_1minute.parquet"


def fetch_minute_bars(
    contracts: pd.DataFrame,
    day: date,
    *,
    max_apps: int,
    timeout_sec: float,
    request_interval_sec: float,
    refresh: bool,
) -> dict[str, pd.DataFrame]:
    """Fetch (and cache) 1-minute bars for the given contracts on one day."""

    (MINUTE_DIR / day.isoformat()).mkdir(parents=True, exist_ok=True)
    wanted = list(contracts.to_dict("records"))
    cached: dict[str, pd.DataFrame] = {}
    todo = []
    for row in wanted:
        path = minute_path(row["tradingsymbol"], day)
        if path.exists() and not refresh:
            try:
                cached[row["tradingsymbol"]] = pd.read_parquet(path)
                continue
            except Exception:
                pass
        todo.append(row)

    if todo:
        credentials = common.discover_kite_credentials(max_apps=max_apps)
        clients = [common.make_kite_client(c, timeout_sec=timeout_sec) for c in credentials]
        parts = [todo[i::len(clients)] for i in range(len(clients))]

        def work(args):
            client, chunk = args
            out = []
            for row in chunk:
                try:
                    time.sleep(max(0.34, request_interval_sec))
                    records = client.historical_data(
                        int(row["instrument_token"]), day, day, "minute",
                        continuous=False, oi=True,
                    )
                except Exception as exc:
                    print(f"[1M][WARN] {row['tradingsymbol']}: {exc}", flush=True)
                    continue
                if not records:
                    continue
                frame = pd.DataFrame(records)
                frame["ts"] = pd.to_datetime(frame["date"], utc=True).dt.tz_convert(common.IST)
                frame = frame.sort_values("ts").reset_index(drop=True)
                common.atomic_write_parquet(frame, minute_path(row["tradingsymbol"], day))
                out.append((row["tradingsymbol"], frame))
            return out

        with ThreadPoolExecutor(max_workers=len(clients)) as pool:
            for chunk_result in pool.map(work, zip(clients, parts)):
                for symbol, frame in chunk_result:
                    cached[symbol] = frame

    for symbol, frame in list(cached.items()):
        if "ts" not in frame.columns and "date" in frame.columns:
            frame = frame.copy()
            frame["ts"] = pd.to_datetime(frame["date"], utc=True).dt.tz_convert(common.IST)
            cached[symbol] = frame
    return cached


def confirmation_ok(bar: pd.Series, side: str, signal_close: float, params: Params) -> tuple[bool, str]:
    o, h, l, c = float(bar["open"]), float(bar["high"]), float(bar["low"]), float(bar["close"])
    rng = h - l
    if rng <= 0:
        return False, "zero_range"
    body = abs(c - o)
    body_ratio = body / rng
    upper_wick = h - max(o, c)
    lower_wick = min(o, c) - l

    if side == "LONG":
        if c <= o:
            return False, "close_not_above_open"
        if c <= signal_close:
            return False, "close_not_above_5m_close"
        if body_ratio < params.body_ratio:
            return False, f"body_ratio {body_ratio:.2f}"
        if upper_wick / rng > params.max_wick_ratio:
            return False, f"upper_wick {upper_wick / rng:.2f}"
        return True, "ok"

    if c >= o:
        return False, "close_not_below_open"
    if c >= signal_close:
        return False, "close_not_below_5m_close"
    if body_ratio < params.body_ratio:
        return False, f"body_ratio {body_ratio:.2f}"
    if lower_wick / rng > params.max_wick_ratio:
        return False, f"lower_wick {lower_wick / rng:.2f}"
    return True, "ok"


def simulate_entry(
    minute: pd.DataFrame,
    confirm_idx: int,
    side: str,
    params: Params,
) -> dict[str, Any] | None:
    """Stop entry at the confirmation candle's extreme, then bracket to exit."""

    confirm = minute.iloc[confirm_idx]
    long_side = side == "LONG"
    trigger = float(confirm["high"]) if long_side else float(confirm["low"])

    if params.stop_pct > 0:
        protective = (
            trigger * (1 - params.stop_pct / 100.0) if long_side
            else trigger * (1 + params.stop_pct / 100.0)
        )
    else:
        protective = float(confirm["low"]) if long_side else float(confirm["high"])
    risk = abs(trigger - protective)
    if risk <= 0:
        return None

    if params.target_pct > 0:
        target = (
            trigger * (1 + params.target_pct / 100.0) if long_side
            else trigger * (1 - params.target_pct / 100.0)
        )
    else:
        target = (
            trigger + params.risk_multiple * risk if long_side
            else trigger - params.risk_multiple * risk
        )
    square_off = params.square_off

    entry_idx = None
    for j in range(confirm_idx + 1, len(minute)):
        bar = minute.iloc[j]
        if bar["ts"].strftime("%H%M") > square_off:
            break
        if (long_side and float(bar["high"]) >= trigger) or (
            not long_side and float(bar["low"]) <= trigger
        ):
            entry_idx = j
            break
    if entry_idx is None:
        return {"filled": False}

    exit_price, exit_reason, exit_idx = np.nan, "SQUARE_OFF", entry_idx
    for j in range(entry_idx, len(minute)):
        bar = minute.iloc[j]
        if bar["ts"].strftime("%H%M") > square_off:
            exit_idx = j - 1
            exit_price = float(minute.iloc[exit_idx]["close"])
            exit_reason = "SQUARE_OFF"
            break
        hit_stop = (float(bar["low"]) <= protective) if long_side else (float(bar["high"]) >= protective)
        hit_target = (float(bar["high"]) >= target) if long_side else (float(bar["low"]) <= target)
        if hit_stop:  # pessimistic when a bar spans both
            exit_idx, exit_price, exit_reason = j, protective, "STOP"
            break
        if hit_target:
            exit_idx, exit_price, exit_reason = j, target, "TARGET"
            break
        exit_idx, exit_price = j, float(bar["close"])
    if not np.isfinite(exit_price):
        return {"filled": False}

    gross = (exit_price / trigger - 1.0) if long_side else (1.0 - exit_price / trigger)
    net = gross - params.cost_bps / 10000.0
    return {
        "filled": True,
        "entry": trigger,
        "stop": protective,
        "target": target,
        "risk_pct": risk / trigger * 100.0,
        "entry_ts": minute.iloc[entry_idx]["ts"],
        "exit_ts": minute.iloc[exit_idx]["ts"],
        "exit": exit_price,
        "exit_reason": exit_reason,
        "minutes_held": int(exit_idx - entry_idx + 1),
        "gross_ret_pct": gross * 100.0,
        "net_ret_pct": net * 100.0,
        "r_multiple": (gross * trigger) / risk if risk else np.nan,
    }


def run(
    day: date,
    signal_slot: str,
    params: Params,
    *,
    max_apps: int,
    timeout_sec: float,
    request_interval_sec: float,
    refresh_minute: bool,
    contract_month: str,
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    universe = pd.read_parquet(common.UNIVERSE_DIR / "latest_near_month.parquet")
    if contract_month:
        universe = universe.loc[
            universe["tradingsymbol"].str.contains(contract_month, case=False, na=False)
        ]

    slot_ts = pd.Timestamp(f"{day.isoformat()} {signal_slot[:2]}:{signal_slot[2:]}", tz=common.IST)
    candidates: list[dict[str, Any]] = []
    attrition = {"evaluated": 0, "bull_stack": 0, "bear_stack": 0, "long": 0, "short": 0}

    for symbol in universe["tradingsymbol"]:
        frame = load_five_minute(symbol)
        if frame.empty:
            continue
        frame = add_five_minute_features(frame)
        row = frame.loc[frame["ts"].eq(slot_ts)]
        if row.empty:
            continue
        row = row.iloc[0]
        attrition["evaluated"] += 1
        if pd.notna(row["ema50"]):
            if row["ema9"] > row["ema20"] > row["ema50"]:
                attrition["bull_stack"] += 1
            elif row["ema9"] < row["ema20"] < row["ema50"]:
                attrition["bear_stack"] += 1
        side = five_minute_signal(row, params)
        if side is None:
            continue
        attrition["long" if side == "LONG" else "short"] += 1
        candidates.append(
            {
                "tradingsymbol": symbol,
                "side": side,
                "signal_close": float(row["close"]),
                "price_change_pct": float(row["price_change_pct"]),
                "oi_change_pct": float(row["oi_change_pct"]),
                "volume_ratio": float(row["volume_ratio"]),
                "ema9": float(row["ema9"]),
                "ema20": float(row["ema20"]),
                "ema50": float(row["ema50"]),
            }
        )

    signals = pd.DataFrame(candidates)
    if signals.empty:
        return signals, pd.DataFrame(), attrition

    signals = signals.merge(
        universe[["tradingsymbol", "instrument_token", "underlying", "lot_size"]],
        on="tradingsymbol",
        how="left",
    )
    minute_bars = fetch_minute_bars(
        signals[["tradingsymbol", "instrument_token"]],
        day,
        max_apps=max_apps,
        timeout_sec=timeout_sec,
        request_interval_sec=request_interval_sec,
        refresh=refresh_minute,
    )

    confirm_ts = slot_ts + pd.Timedelta(minutes=1)
    results: list[dict[str, Any]] = []
    for _, sig in signals.iterrows():
        frame = minute_bars.get(sig["tradingsymbol"])
        record = dict(sig)
        if frame is None or frame.empty:
            record.update({"confirmed": False, "reject": "no_1m_data", "filled": False})
            results.append(record)
            continue
        frame = frame.sort_values("ts").reset_index(drop=True)
        # Kite labels 1-minute bars by start; the candle ending at 09:26 starts 09:25.
        idx = frame.index[frame["ts"].eq(confirm_ts - pd.Timedelta(minutes=1))]
        if len(idx) == 0:
            record.update({"confirmed": False, "reject": "no_confirm_bar", "filled": False})
            results.append(record)
            continue
        confirm_idx = int(idx[0])
        bar = frame.iloc[confirm_idx]
        ok, reason = confirmation_ok(bar, sig["side"], sig["signal_close"], params)
        record.update(
            {
                "confirm_open": float(bar["open"]),
                "confirm_high": float(bar["high"]),
                "confirm_low": float(bar["low"]),
                "confirm_close": float(bar["close"]),
                "confirm_volume": float(bar.get("volume", np.nan)),
                "confirmed": ok,
                "reject": reason,
            }
        )
        if not ok:
            record["filled"] = False
            results.append(record)
            continue
        trade = simulate_entry(frame, confirm_idx, sig["side"], params)
        if trade is None:
            record.update({"filled": False, "reject": "zero_risk"})
        else:
            record.update(trade)
        results.append(record)

    return signals, pd.DataFrame(results), attrition


def render_report(
    day: date, signal_slot: str, params: Params,
    signals: pd.DataFrame, results: pd.DataFrame, attrition: dict[str, Any],
) -> str:
    lines = [
        "# 5m EMA/OI Signal + 1m Confirmation -- Backtest",
        "",
        f"- Session: {day.isoformat()}",
        f"- 5-minute signal bar: ends {signal_slot[:2]}:{signal_slot[2:]}",
        f"- 1-minute confirmation bar: ends "
        f"{(pd.Timestamp(f'{day} {signal_slot[:2]}:{signal_slot[2:]}') + pd.Timedelta(minutes=1)).strftime('%H:%M')}",
        f"- Contracts evaluated: {attrition.get('evaluated', 0)}",
        f"- Round-trip cost: {params.cost_bps} bps | "
        + (f"stop {params.stop_pct}% / target {params.target_pct}%"
           if (params.stop_pct > 0 or params.target_pct > 0)
           else f"stop = confirm candle / target {params.risk_multiple}R")
        + f" | square-off {params.square_off[:2]}:{params.square_off[2:]}",
        "",
        "## 5-minute filter attrition",
        "",
        "| Stage | Count |",
        "| --- | ---: |",
        f"| evaluated | {attrition.get('evaluated', 0)} |",
        f"| EMA9>EMA20>EMA50 (bull stack) | {attrition.get('bull_stack', 0)} |",
        f"| EMA9<EMA20<EMA50 (bear stack) | {attrition.get('bear_stack', 0)} |",
        f"| **passed full LONG filter** | {attrition.get('long', 0)} |",
        f"| **passed full SHORT filter** | {attrition.get('short', 0)} |",
        "",
    ]

    if results.empty:
        lines += ["No 5-minute signals fired at this slot.", ""]
        return "\n".join(lines)

    lines += ["## Signals and outcomes", ""]
    for _, r in results.iterrows():
        lines += [
            f"### {r['tradingsymbol']} -- {r['side']}",
            "",
            f"- 5m: price {r['price_change_pct']:+.3f}% | OI {r['oi_change_pct']:+.3f}% | "
            f"vol ratio {r['volume_ratio']:.2f} | close {r['signal_close']:.2f}",
            f"- EMA9 {r['ema9']:.2f} / EMA20 {r['ema20']:.2f} / EMA50 {r['ema50']:.2f}",
        ]
        if "confirm_open" in r and pd.notna(r.get("confirm_open")):
            rng = r["confirm_high"] - r["confirm_low"]
            body = abs(r["confirm_close"] - r["confirm_open"])
            lines.append(
                f"- 1m confirm: O {r['confirm_open']:.2f} H {r['confirm_high']:.2f} "
                f"L {r['confirm_low']:.2f} C {r['confirm_close']:.2f} | "
                f"body/range {body / rng:.2f}" if rng > 0 else "- 1m confirm: zero range"
            )
        lines.append(f"- Confirmed: **{bool(r.get('confirmed'))}** ({r.get('reject')})")
        if r.get("filled"):
            lines += [
                f"- Entry {r['entry']:.2f} | stop {r['stop']:.2f} | target {r['target']:.2f} "
                f"(risk {r['risk_pct']:.2f}%)",
                f"- Exit {r['exit']:.2f} at {pd.Timestamp(r['exit_ts']).strftime('%H:%M')} "
                f"-- **{r['exit_reason']}**, held {r['minutes_held']}m",
                f"- Gross {r['gross_ret_pct']:+.3f}% | **Net {r['net_ret_pct']:+.3f}%** "
                f"| {r['r_multiple']:+.2f}R",
            ]
        elif r.get("confirmed"):
            lines.append("- Confirmed but the entry level never traded -- no fill.")
        lines.append("")

    filled = results.loc[results.get("filled", pd.Series(dtype=bool)).fillna(False)]
    if not filled.empty:
        net = filled["net_ret_pct"]
        lines += [
            "## Summary",
            "",
            "| Metric | Value |",
            "| --- | ---: |",
            f"| Signals | {len(results)} |",
            f"| Confirmed | {int(results['confirmed'].fillna(False).sum())} |",
            f"| Filled | {len(filled)} |",
            f"| Wins | {int(net.gt(0).sum())} |",
            f"| Net sum % | {net.sum():+.3f} |",
            f"| Net mean % | {net.mean():+.3f} |",
            "",
        ]
    return "\n".join(lines) + "\n"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", default="", help="Session date (default: today).")
    parser.add_argument("--signal-slot", default="0925", help="5-minute bar END time, HHMM.")
    parser.add_argument("--contract-month", default="26AUG", help="Substring filter, e.g. 26AUG.")
    parser.add_argument("--price-change-pct", type=float, default=0.20)
    parser.add_argument("--oi-change-pct", type=float, default=0.50)
    parser.add_argument("--volume-ratio", type=float, default=1.5)
    parser.add_argument("--body-ratio", type=float, default=0.50)
    parser.add_argument("--max-wick-ratio", type=float, default=0.30)
    parser.add_argument("--risk-multiple", type=float, default=2.0,
                        help="Target as a multiple of the confirmation-candle risk. "
                             "Ignored when --target-pct is set.")
    parser.add_argument("--stop-pct", type=float, default=0.0,
                        help="Fixed stop %% from entry. Overrides the confirmation-candle low/high.")
    parser.add_argument("--target-pct", type=float, default=0.0,
                        help="Fixed target %% from entry. Overrides --risk-multiple.")
    parser.add_argument("--square-off", default="1530")
    parser.add_argument("--cost-bps", type=float, default=5.0)
    parser.add_argument("--min-traded-value", type=float, default=0.0)
    parser.add_argument("--refresh-minute", action="store_true", help="Re-fetch cached 1m bars.")
    parser.add_argument("--max-apps", type=int, default=8)
    parser.add_argument("--timeout-sec", type=float, default=15.0)
    parser.add_argument("--request-interval-sec", type=float, default=0.36)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    day = pd.Timestamp(args.date).date() if args.date else common.now_ist().date()
    params = Params(
        price_change_pct=args.price_change_pct,
        oi_change_pct=args.oi_change_pct,
        volume_ratio=args.volume_ratio,
        body_ratio=args.body_ratio,
        max_wick_ratio=args.max_wick_ratio,
        risk_multiple=args.risk_multiple,
        stop_pct=args.stop_pct,
        target_pct=args.target_pct,
        square_off=args.square_off,
        cost_bps=args.cost_bps,
        min_traded_value=args.min_traded_value,
    )
    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    common.publish_status(SESSION, "RUNNING", day=day.isoformat(), slot=args.signal_slot)
    try:
        signals, results, attrition = run(
            day, args.signal_slot, params,
            max_apps=args.max_apps, timeout_sec=args.timeout_sec,
            request_interval_sec=args.request_interval_sec,
            refresh_minute=args.refresh_minute,
            contract_month=args.contract_month,
        )
        report = render_report(day, args.signal_slot, params, signals, results, attrition)
        common.atomic_write_text(REPORT_PATH, report)
        if not results.empty:
            common.atomic_write_csv(
                results, RESULT_DIR / f"ema_confirm_{day.isoformat()}_{args.signal_slot}.csv"
            )
        print(report, flush=True)
        print(f"[REPORT] {REPORT_PATH}", flush=True)
        common.publish_status(
            SESSION, "SUCCESS",
            signals=int(len(results)),
            filled=int(results["filled"].fillna(False).sum()) if not results.empty else 0,
        )
        return 0
    except Exception as exc:
        common.publish_status(SESSION, "FAILED", error=f"{type(exc).__name__}: {exc}")
        print(f"[FATAL] {type(exc).__name__}: {exc}", file=sys.stderr, flush=True)
        raise


if __name__ == "__main__":
    raise SystemExit(main())
