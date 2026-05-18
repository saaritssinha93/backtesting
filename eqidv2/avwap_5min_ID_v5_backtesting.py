"""
AVWAP ID 5-min v5 backtester.

Purpose:
  - Keep v2's profitable 11-setup cascade baseline fixed.
  - Scan the remaining/native setup universe from raw parquet when requested.
  - Apply the v3 accepted-pocket mining rules to build a 24-setup expanded
    result without enabling every raw setup blindly.

Default behavior runs the full native all-setups expansion scanner from raw
5-min/1-min parquet data. Use --use_expansion_cache only for quick reruns.

Outputs:
  C:\\TradingData\\eqidv2\\outputs_ID_v5_5min
"""

from __future__ import annotations

import argparse
import subprocess
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import asdict
from pathlib import Path

import pandas as pd

import avwap_5min_ID_v2_backtesting as v2
import avwap_5min_ID_v3_backtesting as v3


OUT_ROOT = Path(r"C:\TradingData\eqidv2\outputs_ID_v5_5min")
DEFAULT_EXPANSION_CACHE = Path("outputs_avwap_ID_5min_v2_all_setups_ungated/trades.csv")


def _refresh_v2_baseline(timeout_sec: int = 0) -> None:
    cmd = [
        sys.executable,
        "avwap_5min_ID_v2_backtesting.py",
        "--preset",
        "v17_cascade",
        "--out",
        str(v2.V17_CASCADE_OUT_ROOT),
        "--v17_timeout_sec",
        str(int(timeout_sec)),
    ]
    print("[v5] refreshing v2 cascade baseline...")
    subprocess.run(cmd, check=True)


def _run_native_all_setups(
    out_dir: Path,
    workers: int,
    limit: int,
    cost_bps: float,
    max_trades_per_day: int,
    max_same_side_per_day: int,
) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)

    overrides = {
        "ENABLE_NOISY_ADVANCED_SHORTS": True,
        "ENABLE_NATIVE_V2_MINED_FILTER": False,
    }
    v2._init_worker(overrides)

    universe = v2._load_universe()
    if limit:
        universe = universe[: int(limit)]

    print("[v5] loading market context for native all-setups scan...")
    market_ctx = v2._load_market_context()
    print(f"[v5] native scan universe={len(universe)} workers={workers} cost_bps={cost_bps}")

    t0 = time.time()
    all_candidates = []
    errors = []
    payloads = [(tk, market_ctx) for tk in universe]
    workers = max(1, min(int(workers), v2.MAX_WORKERS))

    if workers == 1:
        for k, payload in enumerate(payloads, 1):
            tk, candidates, err = v2._worker(payload)
            if err:
                errors.append((tk, err))
            else:
                all_candidates.extend(candidates)
            if k % 100 == 0 or k == len(payloads):
                print(f"  [{k:4d}/{len(payloads)}] candidates={len(all_candidates)} elapsed={time.time()-t0:.1f}s")
    else:
        with ProcessPoolExecutor(max_workers=workers, initializer=v2._init_worker, initargs=(overrides,)) as ex:
            futures = {ex.submit(v2._worker, payload): payload[0] for payload in payloads}
            done = 0
            for fut in as_completed(futures):
                tk, candidates, err = fut.result()
                done += 1
                if err:
                    errors.append((tk, err))
                else:
                    all_candidates.extend(candidates)
                if done % 100 == 0 or done == len(payloads):
                    print(f"  [{done:4d}/{len(payloads)}] candidates={len(all_candidates)} elapsed={time.time()-t0:.1f}s")

    if errors:
        print(f"[v5][WARN] errored tickers={len(errors)} first5={errors[:5]}")

    candidates_df = pd.DataFrame([asdict(c) for c in all_candidates])
    candidates_df.to_csv(out_dir / "candidates.csv", index=False)

    selected = v2._apply_daily_selection(
        all_candidates,
        max_trades=max(1, int(max_trades_per_day)),
        max_open=max(1, int(max_same_side_per_day)),
    )
    pd.DataFrame([asdict(c) for c in selected]).to_csv(out_dir / "selected_candidates.csv", index=False)

    trades = []
    selected_by_ticker = {}
    for c in selected:
        selected_by_ticker.setdefault(c.ticker, []).append(c)

    for k, (ticker, ticker_candidates) in enumerate(selected_by_ticker.items(), 1):
        needed_days = {c.date for c in ticker_candidates}
        day5_map, day1_map = v2._load_selected_day_maps(ticker, needed_days)
        for c in ticker_candidates:
            tr = v2._candidate_to_trade(
                c,
                day5_map.get(c.date, pd.DataFrame()),
                day1_map.get(c.date, pd.DataFrame()),
                cost_bps,
            )
            if tr is not None:
                trades.append(tr)
        if k % 100 == 0 or k == len(selected_by_ticker):
            print(f"  [v5 resolve {k:4d}/{len(selected_by_ticker)}] trades={len(trades)}")

    trades_df = pd.DataFrame([asdict(t) for t in trades])
    trades_path = out_dir / "trades.csv"
    trades_df.to_csv(trades_path, index=False, float_format="%.6f")

    summary, daily, by_setup = v2._metrics(trades_df)
    daily.to_csv(out_dir / "daily.csv", index=False, float_format="%.2f")
    by_setup.to_csv(out_dir / "by_setup.csv", index=False, float_format="%.4f")
    (out_dir / "summary.txt").write_text(v2._summary_text(summary, by_setup, argparse.Namespace(
        max_trades_per_day=max_trades_per_day,
        max_same_side_per_day=max_same_side_per_day,
        cost_bps=cost_bps,
    )) + "\n", encoding="utf-8")

    print(f"[v5] native all-setups trades written: {trades_path}")
    return trades_path


def _run_v5(args: argparse.Namespace) -> int:
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    if args.refresh_baseline:
        _refresh_v2_baseline(timeout_sec=int(args.v17_timeout_sec))

    if not args.use_expansion_cache:
        expansion_csv = _run_native_all_setups(
            out_dir=out_dir / "native_all_setups_raw",
            workers=int(args.workers),
            limit=int(args.limit),
            cost_bps=float(args.cost_bps),
            max_trades_per_day=int(args.max_trades_per_day),
            max_same_side_per_day=int(args.max_same_side_per_day),
        )
    else:
        expansion_csv = Path(args.expansion_csv)
        if not expansion_csv.exists():
            raise SystemExit(f"missing expansion cache: {expansion_csv}; rerun without --use_expansion_cache")

    baseline_csv = Path(args.baseline_csv) if args.baseline_csv else v3._latest_baseline_csv()

    baseline = v3._normalize_baseline(pd.read_csv(baseline_csv, low_memory=False), "fixed_v2_baseline")
    expansion = v3._normalize_expansion(pd.read_csv(expansion_csv, low_memory=False))
    baseline_summary, baseline_daily, baseline_by_setup = v3._metrics(baseline)

    combined, chosen = v3._mine_additions(
        baseline=baseline,
        expansion_pool=expansion,
        min_pocket_pf=float(args.min_pocket_pf),
        min_total_pf=float(args.min_total_pf),
        min_trades=max(1, int(args.min_trades)),
        max_steps=max(1, int(args.max_steps)),
    )
    summary, daily, by_setup = v3._metrics(combined)

    combined.to_csv(out_dir / "trades.csv", index=False)
    daily.to_csv(out_dir / "daily.csv", index=False)
    by_setup.to_csv(out_dir / "by_setup.csv", index=False)
    chosen.to_csv(out_dir / "accepted_pockets.csv", index=False)
    baseline_daily.to_csv(out_dir / "baseline_daily.csv", index=False)
    baseline_by_setup.to_csv(out_dir / "baseline_by_setup.csv", index=False)
    (out_dir / "inputs.txt").write_text(
        f"baseline_csv={baseline_csv}\nexpansion_csv={expansion_csv}\nfull_scan={not args.use_expansion_cache}\n",
        encoding="utf-8",
    )
    v3._write_summary(out_dir, baseline_summary, summary, by_setup, chosen)
    summary_path = out_dir / "summary.txt"
    text = summary_path.read_text(encoding="utf-8")
    text = text.replace("AVWAP ID 5-min v3 expansion backtest", "AVWAP ID 5-min v5 expanded backtest")
    text = text.replace("V3 expanded:", "V5 expanded:")
    summary_path.write_text(text, encoding="utf-8")
    print(f"\n[v5] wrote {out_dir}")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description="AVWAP ID 5-min v5 expanded runner")
    ap.add_argument("--baseline_csv", type=str, default="")
    ap.add_argument("--expansion_csv", type=str, default=str(DEFAULT_EXPANSION_CACHE))
    ap.add_argument("--out", type=str, default=str(OUT_ROOT))
    ap.add_argument("--refresh_baseline", action="store_true")
    ap.add_argument("--use_expansion_cache", action="store_true", help="skip full raw-data scan and reuse --expansion_csv")
    ap.add_argument("--v17_timeout_sec", type=int, default=0)
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--workers", type=int, default=v2.DEFAULT_WORKERS)
    ap.add_argument("--cost_bps", type=float, default=v2.DEFAULT_COST_BPS)
    ap.add_argument("--max_trades_per_day", type=int, default=v2.DEFAULT_MAX_TRADES_PER_DAY)
    ap.add_argument("--max_same_side_per_day", type=int, default=v2.DEFAULT_MAX_SAME_SIDE_PER_DAY)
    ap.add_argument("--min_pocket_pf", type=float, default=1.30)
    ap.add_argument("--min_total_pf", type=float, default=1.62)
    ap.add_argument("--min_trades", type=int, default=3)
    ap.add_argument("--max_steps", type=int, default=30)
    return _run_v5(ap.parse_args())


if __name__ == "__main__":
    raise SystemExit(main())
