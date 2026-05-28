"""
AVWAP ID 5-min v8 backtester.

v8 keeps the v7 signal/setup selection flow intact, but removes the forced
next-5-minute entry. A setup is still confirmed on the completed 5-minute
signal candle; entry is then recalculated from the first available 1-minute
bar at/after the signal time, using that 1-minute bar's open.

Default output:
  C:\\TradingData\\eqidv2\\outputs_ID_v8_5min
"""

from __future__ import annotations

import argparse
import json
import time
from functools import lru_cache
from pathlib import Path

import numpy as np
import pandas as pd

import eqidv2_signal_discovery_v7_5min_id_persistent as live_discovery
import avwap_5min_ID_v5_backtesting as v5
import avwap_5min_ID_v6_backtesting as v6
import avwap_5min_ID_v7_backtesting as v7
import v17D_exit_resolver as er


OUT_ROOT = Path(r"C:\TradingData\eqidv2\outputs_ID_v8_5min")
LIVE_CANDIDATE_JSON_DIR = Path(r"C:\TradingData\eqidv2\signal_discovery_v7_5mins_ID\json")
PNL_COL = v7.PNL_COL
EXCLUDED_SETUPS = v7.EXCLUDED_SETUPS


@lru_cache(maxsize=None)
def _load_1m_with_open(ticker: str) -> pd.DataFrame | None:
    path = v6.DATA_1M_DIR / f"{str(ticker).upper()}_stocks_indicators_1min.parquet"
    if not path.exists():
        return None
    df = pd.read_parquet(path, columns=["date", "open", "high", "low", "close"])
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    if getattr(df["date"].dt, "tz", None) is None:
        df["date"] = df["date"].dt.tz_localize("UTC").dt.tz_convert("Asia/Kolkata")
    else:
        df["date"] = df["date"].dt.tz_convert("Asia/Kolkata")
    df = df.dropna(subset=["date"]).sort_values("date")
    return df.set_index("date")


def _normalise_ts(value) -> pd.Timestamp:
    ts = pd.to_datetime(value, errors="coerce")
    if pd.isna(ts):
        return pd.NaT
    if ts.tz is None:
        return ts.tz_localize("Asia/Kolkata")
    return ts.tz_convert("Asia/Kolkata")


def _signal_time(row: pd.Series) -> pd.Timestamp:
    for col in ("signal_time_ist", "signal_ts", "bar_time_ist"):
        if col in row.index and pd.notna(row.get(col)):
            ts = _normalise_ts(row.get(col))
            if pd.notna(ts):
                return ts
    entry = row.get("entry_time_v6", row.get("entry_time_ist", row.get("entry_ts", pd.NaT)))
    ts = _normalise_ts(entry)
    if pd.notna(ts):
        return ts - pd.Timedelta(minutes=5)
    return pd.NaT


def _first_1m_entry(
    bars: pd.DataFrame,
    signal_ts: pd.Timestamp,
    *,
    max_delay_minutes: int = 5,
) -> tuple[pd.Timestamp, float] | None:
    sig = _normalise_ts(signal_ts)
    if pd.isna(sig):
        return None
    latest_allowed = sig + pd.Timedelta(minutes=max_delay_minutes)
    sub = bars[(bars.index >= sig) & (bars.index <= latest_allowed)]
    if sub.empty:
        return None
    entry_ts = pd.Timestamp(sub.index[0])
    entry_px = float(sub.iloc[0]["open"])
    if not np.isfinite(entry_px) or entry_px <= 0:
        return None
    return entry_ts, entry_px


def _resolve_trade_1m_entry(row: pd.Series, cost_bps: float) -> dict | None:
    setup = str(row["setup"])
    if setup not in v6.SETUP_EXIT_RULES:
        raise ValueError(f"missing v8 exit rule for setup: {setup}")
    sl_pct, target_pct = v6.SETUP_EXIT_RULES[setup]
    if sl_pct < v6.MIN_SL_PCT:
        raise ValueError(f"SL for {setup} is below {v6.MIN_SL_PCT:.2f}%: {sl_pct}")

    bars = _load_1m_with_open(str(row["ticker"]))
    if bars is None or bars.empty:
        return None

    sig_ts = _signal_time(row)
    entry = _first_1m_entry(bars, sig_ts)
    if entry is None:
        return None
    entry_ts, entry_px = entry

    res = er.resolve(
        bars=bars,
        side=str(row["side"]),
        entry_price=entry_px,
        entry_time_ist=entry_ts,
        sl_pct=sl_pct,
        tgt_pct=target_pct,
    )
    if res is None:
        return None

    net, gross, cost = v6._net_pnl_rs(res.pnl_pct_price, res.outcome, cost_bps)
    old_entry_ts = row.get("entry_time_v6", row.get("entry_time_ist", row.get("entry_ts", pd.NaT)))
    old_entry_px = row.get("entry_price_v6", row.get("entry_price", row.get("entry_px", np.nan)))
    rec = row.to_dict()
    rec.update({
        "signal_time_v8": sig_ts,
        "old_entry_time_v7": old_entry_ts,
        "old_entry_price_v7": old_entry_px,
        "entry_time_v6": entry_ts,
        "entry_price_v6": float(entry_px),
        "trade_date": str(entry_ts.date()),
        "v8_entry_model": "FIRST_1MIN_OPEN_AT_OR_AFTER_SIGNAL",
        "v8_entry_delay_minutes": float((entry_ts - sig_ts).total_seconds() / 60.0) if pd.notna(sig_ts) else np.nan,
        "v6_sl_pct": sl_pct,
        "v6_target_pct": target_pct,
        "v6_outcome": res.outcome,
        "v6_exit_price": float(res.exit_price),
        "v6_exit_time_ist": res.exit_time_ist,
        "v6_bars_held": int(res.bars_held),
        "v6_pnl_pct_price": float(res.pnl_pct_price),
        "v6_gross_pnl_rs": float(gross),
        "v6_cost_rs": float(cost),
        "v6_net_pnl_rs": float(net),
        "capital_per_trade_rs": v6.CAPITAL_PER_TRADE,
        "leverage": v6.LEVERAGE,
        "notional_exposure_rs": v6.EFFECTIVE_NOTIONAL,
    })
    return rec


def _resolve_trades(trades: pd.DataFrame, cost_bps: float, label: str) -> pd.DataFrame:
    normalised = v6._normalise_trades(trades)
    normalised = normalised.loc[~normalised["setup"].astype(str).isin(EXCLUDED_SETUPS)].copy()
    missing = sorted(set(normalised["setup"].astype(str)) - set(v6.SETUP_EXIT_RULES))
    if missing:
        print(f"[v8] skipping {label} setups without setup-specific exits: {missing}")
        normalised = normalised.loc[normalised["setup"].astype(str).isin(v6.SETUP_EXIT_RULES)].copy()

    print(f"[v8] resolving {len(normalised):,} {label} trades with 1-min entry + 1-min exits")
    rows = []
    misses = 0
    t0 = time.time()
    for i, (_, row) in enumerate(normalised.iterrows(), 1):
        rec = _resolve_trade_1m_entry(row, cost_bps=cost_bps)
        if rec is None:
            misses += 1
        else:
            rows.append(rec)
        if i % 500 == 0 or i == len(normalised):
            print(f"  [v8 {label} {i:5d}/{len(normalised)}] resolved={len(rows)} misses={misses} elapsed={time.time()-t0:.1f}s")

    if not rows:
        raise SystemExit(f"[v8] no {label} trades resolved")
    out = pd.DataFrame(rows)
    out["v7_resolution_source"] = label
    out["v8_resolution_source"] = label
    return out


def _load_live_candidate_snapshots(json_dir: Path, day: str = "") -> pd.DataFrame:
    day_key = "".join(str(day or "").split("-"))
    patterns = []
    if day_key:
        patterns.extend([f"raw_candidate_tickers_{day_key}_*.json", f"candidate_tickers_{day_key}_*.json"])
    else:
        patterns.extend(["raw_candidate_tickers_*.json", "candidate_tickers_*.json"])

    rows: list[dict] = []
    seen_files: set[Path] = set()
    for pattern in patterns:
        for path in sorted(json_dir.glob(pattern)):
            if path in seen_files:
                continue
            seen_files.add(path)
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
            except Exception:
                continue
            # For new runs, raw_candidate_tickers_* is the pre-gate source.
            # For older runs, candidate_tickers_* may still be raw.
            for row in payload.get("candidates", []) or []:
                if isinstance(row, dict):
                    rows.append(row)
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    if day:
        sig = pd.to_datetime(df.get("signal_time_ist"), errors="coerce")
        df = df.loc[sig.dt.strftime("%Y-%m-%d").eq(str(day))].copy()
    return df.reset_index(drop=True)


def _normalise_live_candidates(candidates: pd.DataFrame) -> pd.DataFrame:
    if candidates is None or candidates.empty:
        return pd.DataFrame()
    out = candidates.copy()
    required = {"ticker", "side", "setup", "signal_time_ist"}
    if not required.issubset(out.columns):
        missing = sorted(required - set(out.columns))
        raise SystemExit(f"[v8 live_parity] missing candidate columns: {missing}")
    out["ticker"] = out["ticker"].astype(str).str.upper().str.strip()
    out["side"] = out["side"].astype(str).str.upper().str.strip()
    out["setup"] = out["setup"].astype(str).str.strip()
    out["signal_time_ist"] = pd.to_datetime(out["signal_time_ist"], errors="coerce")
    if getattr(out["signal_time_ist"].dt, "tz", None) is None:
        out["signal_time_ist"] = out["signal_time_ist"].dt.tz_localize("Asia/Kolkata")
    else:
        out["signal_time_ist"] = out["signal_time_ist"].dt.tz_convert("Asia/Kolkata")
    out = out.dropna(subset=["ticker", "side", "setup", "signal_time_ist"]).copy()
    out = out.loc[~out["setup"].isin(EXCLUDED_SETUPS)].copy()
    out = out.loc[out["setup"].isin(v6.SETUP_EXIT_RULES)].copy()
    out["signal_time_ist"] = out["signal_time_ist"].map(lambda x: x.strftime("%Y-%m-%d %H:%M:%S%z"))
    out["signal_time_ist"] = out["signal_time_ist"].str.replace(r"(\+\d{2})(\d{2})$", r"\1:\2", regex=True)
    out["quality_score"] = pd.to_numeric(out.get("quality_score", 0.0), errors="coerce").fillna(0.0)
    if "candidate_id" not in out.columns:
        out["candidate_id"] = (
            out["ticker"].astype(str)
            + "|"
            + out["side"].astype(str)
            + "|"
            + out["setup"].astype(str)
            + "|"
            + out["signal_time_ist"].astype(str)
        )
    out = (
        out.sort_values(["signal_time_ist", "quality_score", "ticker", "setup"], ascending=[True, False, True, True])
        .drop_duplicates(subset=["candidate_id"], keep="first")
        .drop_duplicates(subset=["signal_time_ist", "ticker"], keep="first")
        .reset_index(drop=True)
    )
    return out


def _resolve_live_parity_candidates(candidates: pd.DataFrame, cost_bps: float, label: str) -> pd.DataFrame:
    normalised = _normalise_live_candidates(candidates)
    print(f"[v8 live_parity] resolving {len(normalised):,} gated live candidates")
    rows = []
    misses = 0
    t0 = time.time()
    for i, (_, row) in enumerate(normalised.iterrows(), 1):
        rec = _resolve_trade_1m_entry(row, cost_bps=cost_bps)
        if rec is None:
            misses += 1
        else:
            rows.append(rec)
        if i % 250 == 0 or i == len(normalised):
            print(f"  [v8 live_parity {i:5d}/{len(normalised)}] resolved={len(rows)} misses={misses} elapsed={time.time()-t0:.1f}s")
    if not rows:
        raise SystemExit("[v8 live_parity] no trades resolved")
    out = pd.DataFrame(rows)
    out["v7_resolution_source"] = label
    out["v8_resolution_source"] = label
    out["trade_date"] = out["trade_date"].astype(str).str[:10]
    out["_entry_sort"] = pd.to_datetime(out["entry_time_v6"], errors="coerce")
    out["_score_sort"] = pd.to_numeric(out.get("quality_score", 0.0), errors="coerce").fillna(0.0)
    out = (
        out.sort_values(["trade_date", "_entry_sort", "_score_sort", "ticker"], ascending=[True, True, False, True])
        .drop_duplicates(subset=["trade_date", "ticker"], keep="first")
        .drop(columns=["_entry_sort", "_score_sort"], errors="ignore")
        .reset_index(drop=True)
    )
    return out


def _run_live_parity(args: argparse.Namespace) -> int:
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    source_dir = Path(args.live_candidate_json_dir)
    raw_candidates = _load_live_candidate_snapshots(source_dir, str(args.live_date or ""))
    if raw_candidates.empty:
        raise SystemExit(f"[v8 live_parity] no live candidate snapshots found in {source_dir} for date={args.live_date!r}")

    gated_candidates, gate_stats = live_discovery.apply_v8_live_gate(raw_candidates)
    raw_candidates.to_csv(out_dir / "live_parity_raw_candidates.csv", index=False)
    gated_candidates.to_csv(out_dir / "live_parity_gated_candidates.csv", index=False)
    pd.DataFrame([gate_stats]).to_csv(out_dir / "live_parity_gate_stats.csv", index=False)

    final = _resolve_live_parity_candidates(gated_candidates, float(args.cost_bps), "live_parity")
    accepted = pd.DataFrame([{
        "stage": "live_parity",
        "source": str(source_dir),
        "date": str(args.live_date or ""),
        **gate_stats,
    }])
    _write_outputs(out_dir, final, accepted, str(source_dir), accepted_filename="live_parity_rules_used.csv")
    (out_dir / "inputs.txt").write_text(
        f"mode=live_parity\nlive_candidate_json_dir={source_dir}\nlive_date={args.live_date}\n"
        "entry_model=FIRST_1MIN_OPEN_AT_OR_AFTER_SIGNAL\n"
        "candidate_source=signal_discovery_v7_5mins_ID snapshots\n"
        "gate=v8_live_safe_accepted_rules_only\n"
        "dedupe=one ticker per trade_date, matching live entry-engine intraday ticker guard\n",
        encoding="utf-8",
    )
    print(f"[v8 live_parity] wrote {out_dir}")
    return 0


def _write_outputs(
    out_dir: Path,
    trades: pd.DataFrame,
    accepted: pd.DataFrame,
    source: str,
    accepted_filename: str = "accepted_rules.csv",
) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    trades.to_csv(out_dir / "trades.csv", index=False)
    summary, daily, by_setup = v6._metrics(trades)
    daily.rename(columns={"_pnl": "net_pnl_rs"}).to_csv(out_dir / "daily.csv", index=False)
    by_setup.to_csv(out_dir / "by_setup.csv", index=False)
    accepted.to_csv(out_dir / accepted_filename, index=False)
    pd.DataFrame(
        [{"setup": k, "sl_pct": v[0], "target_pct": v[1]} for k, v in sorted(v6.SETUP_EXIT_RULES.items())]
    ).to_csv(out_dir / "setup_exit_rules.csv", index=False)

    entry_audit = trades[[
        c for c in [
            "ticker", "side", "setup", "signal_time_v8", "old_entry_time_v7",
            "entry_time_v6", "old_entry_price_v7", "entry_price_v6",
            "v8_entry_delay_minutes", "v6_outcome", "v6_net_pnl_rs",
        ] if c in trades.columns
    ]].copy()
    entry_audit.to_csv(out_dir / "entry_timing_audit.csv", index=False)

    text = v6._summary_text(summary, by_setup, Path(source))
    text = text.replace("AVWAP ID 5-min v6 backtest", "AVWAP ID 5-min v8 backtest")
    text = text.replace(
        "v5 trades re-resolved on 1-minute bars with setup-specific SL/target exits",
        "v7 signal flow + first 1-minute open entry + setup-specific 1-minute exits",
    )
    print(text)
    (out_dir / "summary.txt").write_text(text + "\n", encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser(description="AVWAP ID 5-min v8 with first-1min-open entries")
    ap.add_argument("--mode", choices=["live_parity", "research"], default="research")
    ap.add_argument("--out", type=str, default=str(OUT_ROOT))
    ap.add_argument("--live_candidate_json_dir", type=str, default=str(LIVE_CANDIDATE_JSON_DIR))
    ap.add_argument("--live_date", type=str, default="")
    ap.add_argument("--workers", type=int, default=v5.v2.DEFAULT_WORKERS)
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--cost_bps", type=float, default=v6.DEFAULT_COST_BPS)
    ap.add_argument("--max_trades_per_day", type=int, default=999_999)
    ap.add_argument("--max_same_side_per_day", type=int, default=999_999)
    ap.add_argument("--refresh_baseline", action="store_true")
    ap.add_argument("--use_v5_cache", action="store_true", help="reuse the v8/v5_stage or v7/v5_stage CSVs instead of rescanning raw data")
    ap.add_argument("--v7_cache_dir", type=str, default=str(v7.OUT_ROOT / "v5_stage"), help="fallback v7 stage cache to reuse")
    args = ap.parse_args()

    if args.mode == "live_parity":
        return _run_live_parity(args)

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    stage_dir = out_dir / "v5_stage"
    cache_stage = Path(args.v7_cache_dir)

    if args.use_v5_cache and not stage_dir.exists() and cache_stage.exists():
        stage_dir = cache_stage

    if not args.use_v5_cache:
        v5_args = argparse.Namespace(
            baseline_csv="",
            expansion_csv=str(v5.DEFAULT_EXPANSION_CACHE),
            out=str(stage_dir),
            refresh_baseline=bool(args.refresh_baseline),
            use_expansion_cache=False,
            v17_timeout_sec=0,
            limit=int(args.limit),
            workers=int(args.workers),
            cost_bps=float(args.cost_bps),
            max_trades_per_day=int(args.max_trades_per_day),
            max_same_side_per_day=int(args.max_same_side_per_day),
            min_pocket_pf=1.30,
            min_total_pf=1.62,
            min_trades=3,
            max_steps=30,
        )
        v5._run_v5(v5_args)

    v5_trades = stage_dir / "trades.csv"
    raw_trades = stage_dir / "native_all_setups_raw" / "trades.csv"
    if not v5_trades.exists() or not raw_trades.exists():
        raise SystemExit(f"[v8] missing v5 stage files under {stage_dir}; rerun without --use_v5_cache")

    base = _resolve_trades(pd.read_csv(v5_trades, low_memory=False), float(args.cost_bps), "base")
    raw = pd.read_csv(raw_trades, low_memory=False)
    raw_norm = v6._normalise_trades(raw)
    raw_norm = raw_norm.loc[~raw_norm["setup"].astype(str).isin(EXCLUDED_SETUPS)].copy()
    raw_norm = raw_norm.loc[~v7._trade_key(raw_norm).isin(set(v7._trade_key(base)))].copy()

    pf2_setups = [str(setup) for setup, group in base.groupby("setup") if v7._pf(group[PNL_COL]) > 2.0]
    raw_norm = raw_norm[raw_norm["setup"].isin(pf2_setups)].copy()
    extra = _resolve_trades(raw_norm, float(args.cost_bps), "extra_pf2_pool")
    extra.to_csv(out_dir / "extra_pf2_setup_candidates_resolved.csv", index=False)

    final, accepted = v7._apply_v7_expansion(base, extra)
    _write_outputs(out_dir, final, accepted, str(v5_trades))
    (out_dir / "inputs.txt").write_text(
        f"v5_trades={v5_trades}\nraw_trades={raw_trades}\nfull_scan={not args.use_v5_cache}\n"
        "entry_model=FIRST_1MIN_OPEN_AT_OR_AFTER_SIGNAL\n",
        encoding="utf-8",
    )
    print(f"[v8] wrote {out_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
