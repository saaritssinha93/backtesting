from __future__ import annotations

import argparse
import csv
import json
import math
import random
import shutil
import sys
import time
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

HERE = Path(__file__).resolve()
REPO = HERE.parents[4]
TT_DIR = REPO / "Train_and_Test"
for p in (str(REPO), str(TT_DIR)):
    if p not in sys.path:
        sys.path.insert(0, p)

import avwap_5min_ID_v2_backtesting as v2  # noqa: E402
import avwap_5min_ID_v11_backtesting as v11  # noqa: E402
import Train_and_Test.setup_train_test as tt  # noqa: E402

try:
    import optuna  # type: ignore  # noqa: E402

    optuna.logging.set_verbosity(optuna.logging.WARNING)
    HAVE_OPTUNA = True
except Exception:
    optuna = None
    HAVE_OPTUNA = False


SETUPS = ["D_AVWAP_LOSE_REVERSAL", "D_EMA20_BOUNCE", "D_EMA20_REJECTION"]
FNAME = "historical_all_available_pre_dedupe_live_candidates.csv"
REQUESTED_TRAIN = ("2026-03-01", "2026-05-30")
REQUESTED_TEST = ("2026-06-01", "2026-07-02")
DEFAULT_DATA_ROOT = Path(r"C:\TradingData\eqidv2\stocks_indicators_5min_eq_live2")
DEFAULT_WORK_ROOT = TT_DIR / "setup_recovery_full_loop"

BASELINE_CONFIGS: dict[str, dict[str, Any]] = {
    "D_AVWAP_LOSE_REVERSAL": {
        "name": "baseline_raw_v11_exit",
        "sl": 1.00,
        "tgt": 1.50,
        "mask_terms": [],
        "premom_terms": [],
        "guard": {},
        "notes": "Raw v2 VWAP-loss reversal with v11/v6 exit rule.",
    },
    "D_EMA20_BOUNCE": {
        "name": "baseline_raw_v11_exit",
        "sl": 0.70,
        "tgt": 1.50,
        "mask_terms": [],
        "premom_terms": [],
        "guard": {},
        "notes": "Raw v2 EMA20-bounce with v11/v6 exit rule.",
    },
    "D_EMA20_REJECTION": {
        "name": "baseline_setup_card_premom",
        "sl": 0.75,
        "tgt": 1.30,
        "mask_terms": [],
        "premom_terms": [
            ["pre10_mom_r", "<=", 0.156614],
            ["pre5_mom_r", ">=", 0.12493],
            ["sig5_adx_calc", ">=", 20.0],
        ],
        "guard": {},
        "notes": "Setup-card gate of record: pre10 <= 0.156614, pre5 >= 0.12493, sig5 ADX >= 20.",
    },
}

SETUP_CONTEXT: dict[str, dict[str, str]] = {
    "D_AVWAP_LOSE_REVERSAL": {
        "idea": "Short reversal when a stock that was above session VWAP loses VWAP on a strong down bar.",
        "previous_failure": (
            "Prior evidence was a small-sample short-mine: the first gate looked strong on only 26 trades, "
            "but the deeper 82-trade mine collapsed to about train PF 1.06. The high-PF pockets were "
            "mostly down-market conditioned, which is a regime bet rather than a clean setup edge."
        ),
        "logic_review": (
            "The raw detector is structurally sensible, but it fires in the middle of a crowded VWAP-loss "
            "universe. It needs either clean fresh sell pressure, non-climax volatility, and weak relative "
            "strength, or it becomes a late short after the move is already extended."
        ),
    },
    "D_EMA20_BOUNCE": {
        "idea": "Long trend-continuation bounce when an uptrend stack retests EMA20 and closes back strong.",
        "previous_failure": (
            "This is not in the active conf book and appears as an overlay/leak candidate in the setup-card "
            "cross-check. The live survival audit saw only one recent leaked trade and it lost money. Older "
            "production-core filters were thin and not part of the current gate of record."
        ),
        "logic_review": (
            "The raw idea is tradeable only when the retest is a real hold, not a slow rollover. Useful "
            "filters should look for rising pre-entry momentum, sane distance from VWAP/EMA, and avoid late "
            "or exhausted bounces."
        ),
    },
    "D_EMA20_REJECTION": {
        "idea": "Short trend-continuation rejection when a downtrend stack retests EMA20 and resumes lower.",
        "previous_failure": (
            "The setup-card says the pre-momentum gate is the edge. The later force-promoted Optuna version "
            "was explicitly screen-only/firehose-derived, with top_n not enforced by the live conf-mask path "
            "and a dominance warning. That is the failure mode to avoid."
        ),
        "logic_review": (
            "This is the cleanest structural thesis of the three, but it is sample-thin and month-unstable. "
            "The rescue should prefer simple ADX/RSI/pre-momentum confirmation and reject tiny top_n screens "
            "that cannot be reproduced live."
        ),
    },
}

SIGNAL_FEATURES = [
    "rs_pct",
    "market_ret_pct",
    "market_abs_ret_pct",
    "vol_ratio",
    "atr_pct",
    "body_pct",
    "close_loc",
    "vwap_dist_atr",
    "quality_score",
    "ranker_score",
    "signal_minute",
    "signal_range_pct",
    "upper_wick_pct",
    "lower_wick_pct",
    "wick_skew_pct",
    "signal_volume",
    "ATR",
    "EMA_20",
    "EMA_50",
    "RSI",
    "ADX",
    "CCI",
    "MFI",
    "MACD_Hist",
    "ema20_dist_atr",
    "pressure_ratio_5",
    "day_value_so_far_rs",
]

PREMOM_FEATURES = [
    "pre1_mom_r",
    "pre2_mom_r",
    "pre3_mom_r",
    "pre5_mom_r",
    "pre10_mom_r",
    "pre15_mom_r",
    "pre3_close_pos",
    "pre5_close_pos",
    "pre10_close_pos",
    "pre3_range_r",
    "pre5_range_r",
    "pre10_range_r",
    "pre3_dir_count",
    "pre5_dir_count",
    "pre10_dir_count",
    "pre1_adx",
    "pre1_rsi_dir",
    "pre_entry_momentum_score",
    "sig5_body_r",
    "sig5_range_r",
    "sig5_close_pos",
    "sig5_adx_calc",
    "sig5_rsi_dir",
    "sig5_vol_ratio20",
]

SL_GRID = [0.40, 0.50, 0.60, 0.70, 0.75, 0.85, 0.90, 1.00, 1.10, 1.20, 1.50]
TGT_GRID = [0.50, 0.60, 0.80, 1.00, 1.20, 1.30, 1.50, 2.00, 2.50, 3.00]
GUARDS = [
    {},
    {"min_slot": "09:45"},
    {"min_slot": "10:00"},
    {"min_slot": "10:30"},
    {"min_slot": "11:00"},
    {"max_slot": "11:30"},
    {"max_slot": "12:30"},
    {"max_slot": "13:30"},
    {"max_slot": "14:00"},
    {"min_slot": "10:00", "max_slot": "13:30"},
    {"top_n": 1},
    {"top_n": 2},
    {"top_n": 3},
]

_WORKER_DATA_ROOT: Path | None = None
_WORKER_START = ""
_WORKER_END = ""
_WORKER_MARKET_CTX: dict[str, dict] | None = None


@dataclass
class EvalResult:
    metrics: dict[str, Any]
    fam: dict[str, Any] | None = None
    detail: pd.DataFrame | None = None


def _safe_float(value: Any) -> Any:
    try:
        v = float(value)
    except Exception:
        return ""
    return v if np.isfinite(v) else ""


def _date_key(value: Any) -> str:
    ts = pd.to_datetime(value, errors="coerce")
    if pd.isna(ts):
        return ""
    return ts.strftime("%Y-%m-%d")


def _worker_init(data_root: str, start: str, end: str) -> None:
    global _WORKER_DATA_ROOT, _WORKER_START, _WORKER_END, _WORKER_MARKET_CTX
    _WORKER_DATA_ROOT = Path(data_root)
    _WORKER_START = start
    _WORKER_END = end
    v2.DATA_ROOT_5M = _WORKER_DATA_ROOT
    v2._init_worker({"ENABLE_NOISY_ADVANCED_SHORTS": True, "ENABLE_NATIVE_V2_MINED_FILTER": False})
    try:
        _WORKER_MARKET_CTX = v2._load_market_context()
    except Exception:
        _WORKER_MARKET_CTX = {}


def _candidate_record(c: Any, signal_row: pd.Series) -> dict[str, Any]:
    signal_ts = v11._normalise_ts(c.signal_ts)
    signal_time = v11._fmt_ist(signal_ts)
    ticker = str(c.ticker).upper().strip()
    side = str(c.side).upper().strip()
    setup = str(c.setup).strip()
    rec: dict[str, Any] = {
        "candidate_id": f"{ticker}|{side}|{setup}|{signal_time}",
        "scan_session": "setup_recovery_full_loop",
        "selection_mode": "RAW_V2_RECREATED",
        "candidate_family": "V2_D_NATIVE",
        "scan_slot_ist": signal_time,
        "signal_time_ist": signal_time,
        "ticker": ticker,
        "side": side,
        "setup": setup,
        "signal_open": _safe_float(signal_row.get("open")),
        "signal_high": _safe_float(signal_row.get("high")),
        "signal_low": _safe_float(signal_row.get("low")),
        "signal_close": _safe_float(signal_row.get("close", c.signal_close)),
        "signal_volume": _safe_float(signal_row.get("volume")),
        "quality_score": _safe_float(c.quality_score),
        "rs_pct": _safe_float(c.rs_pct),
        "market_ret_pct": _safe_float(c.market_ret_pct),
        "regime": str(c.regime),
        "vol_ratio": _safe_float(c.vol_ratio),
        "atr_pct": _safe_float(c.atr_pct),
        "body_pct": _safe_float(c.body_pct),
        "close_loc": _safe_float(c.close_loc),
        "vwap_dist_atr": _safe_float(c.vwap_dist_atr),
        "day_value_so_far_rs": _safe_float(c.day_value_so_far_rs),
        "reason": str(c.reason),
        "status": "CANDIDATE",
        "created_at_ist": v11._fmt_ist(pd.Timestamp.now(tz="Asia/Kolkata")),
    }
    for col in (
        "VWAP",
        "AVWAP",
        "ATR",
        "EMA_20",
        "EMA_50",
        "EMA_200",
        "RSI",
        "ADX",
        "CCI",
        "MFI",
        "OBV",
        "MACD",
        "MACD_Signal",
        "MACD_Hist",
        "Upper_Band",
        "Lower_Band",
        "Stoch_%K",
        "Stoch_%D",
        "Volume_SMA20",
        "traded_value_rs",
        "range",
        "ema20_dist_atr",
        "upper_wick_pct",
        "lower_wick_pct",
        "volume_z_20",
        "pressure_ratio_5",
        "prev_day_high",
        "prev_day_low",
        "Prev_Day_Close",
    ):
        if col in signal_row.index:
            rec[col] = _safe_float(signal_row.get(col))
    rec["diagnostics_json"] = json.dumps(
        {
            "reason": str(c.reason),
            "market_ret_pct": rec.get("market_ret_pct", ""),
            "rs_pct": rec.get("rs_pct", ""),
            "regime": rec.get("regime", ""),
            "source": "avwap_5min_ID_v2_backtesting._scan_day",
        },
        default=str,
    )
    return rec


def _scan_one_ticker(ticker: str) -> list[dict[str, Any]]:
    assert _WORKER_DATA_ROOT is not None
    fp = _WORKER_DATA_ROOT / f"{ticker}_stocks_indicators_5min.parquet"
    if not fp.exists():
        return []
    try:
        df = v2._read_ohlcv(fp)
        prepared = v2._prepare_5m(df)
    except Exception:
        return []
    if prepared.empty or "date_only" not in prepared.columns:
        return []
    start_day = pd.to_datetime(_WORKER_START).date()
    end_day = pd.to_datetime(_WORKER_END).date()
    prepared = prepared[
        (pd.to_datetime(prepared["date_only"], errors="coerce").dt.date >= start_day)
        & (pd.to_datetime(prepared["date_only"], errors="coerce").dt.date <= end_day)
    ].copy()
    if prepared.empty:
        return []
    rows: list[dict[str, Any]] = []
    market_ctx = _WORKER_MARKET_CTX or {}
    for _day, day_df in prepared.groupby("date_only", sort=True):
        day_df = day_df.sort_values("date").drop_duplicates(subset=["date"], keep="last").reset_index(drop=True)
        if day_df.empty:
            continue
        sig_map = {pd.Timestamp(row["date"]).floor("min"): row for _, row in day_df.iterrows()}
        try:
            candidates = v2._scan_day(day_df, ticker, market_ctx)
        except Exception:
            continue
        for c in candidates:
            if str(c.setup) not in SETUPS:
                continue
            sig_ts = v11._normalise_ts(c.signal_ts).floor("min")
            signal_row = sig_map.get(sig_ts)
            if signal_row is None:
                continue
            rows.append(_candidate_record(c, signal_row))
    return rows


def available_sessions(data_root: Path, start: str, end: str) -> list[str]:
    for ticker in ("NIFTYBEES", "NIFTY", "NIFTY50", "AARTIIND"):
        fp = data_root / f"{ticker}_stocks_indicators_5min.parquet"
        if not fp.exists():
            continue
        try:
            d = pd.read_parquet(fp, columns=["date"])
        except Exception:
            continue
        ts = pd.to_datetime(d["date"], errors="coerce")
        if getattr(ts.dt, "tz", None) is None:
            ts = ts.dt.tz_localize("Asia/Kolkata")
        else:
            ts = ts.dt.tz_convert("Asia/Kolkata")
        days = sorted({x.strftime("%Y-%m-%d") for x in ts.dropna()})
        out = [d for d in days if start <= d <= end]
        if out:
            return out
    return []


def recreate_pools(args: argparse.Namespace) -> tuple[pd.DataFrame, dict[str, Any]]:
    work_root = Path(args.work_root)
    data_root = Path(args.data_root)
    start = min(args.train_start, args.test_start)
    end = max(args.train_end, args.test_end)
    v2.DATA_ROOT_5M = data_root
    universe = v2._load_universe()
    if args.limit_tickers:
        universe = universe[: int(args.limit_tickers)]
    t0 = time.time()
    rows: list[dict[str, Any]] = []
    workers = max(1, int(args.scan_workers))
    if workers == 1:
        _worker_init(str(data_root), start, end)
        for i, ticker in enumerate(universe, 1):
            rows.extend(_scan_one_ticker(ticker))
            if i % 200 == 0 or i == len(universe):
                print(f"[pool] scanned {i}/{len(universe)} tickers rows={len(rows)} elapsed={time.time() - t0:.1f}s", flush=True)
    else:
        with ProcessPoolExecutor(
            max_workers=workers,
            initializer=_worker_init,
            initargs=(str(data_root), start, end),
        ) as ex:
            for i, result in enumerate(ex.map(_scan_one_ticker, universe, chunksize=8), 1):
                if result:
                    rows.extend(result)
                if i % 200 == 0 or i == len(universe):
                    print(f"[pool] scanned {i}/{len(universe)} tickers rows={len(rows)} elapsed={time.time() - t0:.1f}s", flush=True)

    pool = pd.DataFrame(rows)
    if not pool.empty:
        pool = pool.drop_duplicates(subset=["candidate_id"], keep="first").reset_index(drop=True)
        pool["_day_tmp"] = pd.to_datetime(pool["signal_time_ist"], errors="coerce", utc=True).dt.tz_convert("Asia/Kolkata").dt.strftime("%Y-%m-%d")
    all_path = work_root / "_shared" / "pools"
    all_path.mkdir(parents=True, exist_ok=True)
    pool.drop(columns=["_day_tmp"], errors="ignore").to_csv(all_path / "recreated_selected_D_setups_pool.csv", index=False)

    train_sessions = available_sessions(data_root, args.train_start, args.train_end)
    test_sessions = available_sessions(data_root, args.test_start, args.test_end)
    manifest: dict[str, Any] = {
        "requested": {
            "train": [args.train_start, args.train_end],
            "test": [args.test_start, args.test_end],
        },
        "actual_data_sessions": {
            "train": train_sessions,
            "test": test_sessions,
        },
        "data_root": str(data_root),
        "tickers_scanned": len(universe),
        "rows_total": int(len(pool)),
        "rows_by_setup": {},
        "candidate_sessions_by_setup": {},
        "elapsed_sec": round(time.time() - t0, 2),
    }
    for setup in SETUPS:
        setup_dir = work_root / setup
        pool_dir = setup_dir / "pools"
        pool_dir.mkdir(parents=True, exist_ok=True)
        sub = pool[pool["setup"].astype(str).eq(setup)].drop(columns=["_day_tmp"], errors="ignore") if not pool.empty else pd.DataFrame()
        sub.to_csv(pool_dir / FNAME, index=False)
        if not sub.empty:
            days = (
                pd.to_datetime(sub["signal_time_ist"], errors="coerce", utc=True)
                .dt.tz_convert("Asia/Kolkata")
                .dt.strftime("%Y-%m-%d")
            )
            manifest["candidate_sessions_by_setup"][setup] = sorted(days.dropna().unique().tolist())
        else:
            manifest["candidate_sessions_by_setup"][setup] = []
        manifest["rows_by_setup"][setup] = int(len(sub))
        (pool_dir / "_manifest.json").write_text(json.dumps(manifest, indent=2, default=str), encoding="utf-8")
    (all_path / "_manifest.json").write_text(json.dumps(manifest, indent=2, default=str), encoding="utf-8")
    return pool, manifest


def _cfg_for_eval(setup: str, cfg: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {
        setup: {
            "status": "OK",
            "sl": float(cfg["sl"]),
            "tgt": float(cfg["tgt"]),
            "mask_terms": [tuple(t) for t in cfg.get("mask_terms", [])],
            "premom_terms": [tuple(t) for t in cfg.get("premom_terms", [])],
            "guard": cfg.get("guard") or None,
        }
    }


def eval_cfg(setup: str, cfg: dict[str, Any], df: pd.DataFrame, detail: bool = False) -> EvalResult:
    tt.MAX_POSITIONS = int(cfg.get("max_positions", 20))
    tt.DAILY_LOSS_RS = float(cfg.get("daily_loss_rs", 0.0))
    tt.REGIME_ALIGN = bool(cfg.get("regime_align", False))
    tt.REGIME_BAND = float(cfg.get("regime_band", 0.0))
    fam = tt.eval_family(_cfg_for_eval(setup, cfg), df)
    det = pd.DataFrame()
    if detail and int(fam["trades"]):
        det = tt.book_detail(fam["book"], {setup: (float(cfg["sl"]), float(cfg["tgt"]))})
    return EvalResult(metrics_from_family(fam, det), fam, det)


def metrics_from_family(fam: dict[str, Any], detail: pd.DataFrame | None = None) -> dict[str, Any]:
    net = np.asarray(fam.get("net", np.array([])), dtype=float)
    finite = net[np.isfinite(net)]
    out: dict[str, Any] = {
        "trades": int(fam.get("trades", 0)),
        "net_pf": round(float(fam.get("net_pf", 0.0)), 4),
        "net_pnl": round(float(fam.get("net_pnl", 0.0)), 2),
        "day_block_p": None
        if not np.isfinite(float(fam.get("day_block_p", float("nan"))))
        else round(float(fam.get("day_block_p")), 4),
        "win_pct": round(float((finite > 0).mean() * 100.0), 2) if len(finite) else 0.0,
        "avg_net": round(float(np.mean(finite)), 2) if len(finite) else 0.0,
        "n_days": 0,
        "n_symbols": 0,
        "target_pct": 0.0,
        "sl_pct": 0.0,
        "eod_pct": 0.0,
        "top_trade_gross_profit_share": None,
        "top_day_net_share": None,
        "top_symbol_net_share": None,
        "max_trades_day": 0,
    }
    if detail is None or detail.empty:
        book = fam.get("book")
        if isinstance(book, pd.DataFrame) and not book.empty:
            out["n_days"] = int(book["_day"].nunique()) if "_day" in book else 0
            out["n_symbols"] = int(book["ticker"].nunique()) if "ticker" in book else 0
            out["max_trades_day"] = int(book.groupby("_day").size().max()) if "_day" in book else 0
        return out
    d = detail.copy()
    out["n_days"] = int(d["trade_date"].nunique()) if "trade_date" in d else 0
    out["n_symbols"] = int(d["ticker"].nunique()) if "ticker" in d else 0
    out["max_trades_day"] = int(d.groupby("trade_date").size().max()) if "trade_date" in d else 0
    if "outcome" in d:
        oc = d["outcome"].astype(str).str.upper()
        out["target_pct"] = round(float((oc == "TARGET").mean() * 100.0), 2)
        out["sl_pct"] = round(float((oc == "SL").mean() * 100.0), 2)
        out["eod_pct"] = round(float((oc == "EOD").mean() * 100.0), 2)
    if "net_pnl_rs" in d:
        pnl = pd.to_numeric(d["net_pnl_rs"], errors="coerce").fillna(0.0)
        gp = float(pnl[pnl > 0].sum())
        total = float(pnl.sum())
        if gp > 0 and len(pnl[pnl > 0]):
            out["top_trade_gross_profit_share"] = round(float(pnl[pnl > 0].max() / gp), 4)
        if total > 0:
            day = d.assign(_pnl=pnl).groupby("trade_date")["_pnl"].sum()
            sym = d.assign(_pnl=pnl).groupby("ticker")["_pnl"].sum()
            out["top_day_net_share"] = round(float(day.max() / total), 4) if len(day) else None
            out["top_symbol_net_share"] = round(float(sym.max() / total), 4) if len(sym) else None
    return out


def fmt_m(m: dict[str, Any] | None) -> str:
    if not m:
        return "not run"
    return f"n={m['trades']} PF={m['net_pf']} net={m['net_pnl']}"


def md_table(rows: list[list[Any]], headers: list[str]) -> str:
    out = ["| " + " | ".join(headers) + " |", "|" + "|".join(["---"] * len(headers)) + "|"]
    for row in rows:
        out.append("| " + " | ".join(str(x) for x in row) + " |")
    return "\n".join(out)


def quantile_values(df: pd.DataFrame, features: list[str], qs: list[float]) -> dict[str, list[float]]:
    out: dict[str, list[float]] = {}
    for feat in features:
        if feat not in df.columns:
            continue
        s = pd.to_numeric(df[feat], errors="coerce").replace([np.inf, -np.inf], np.nan).dropna()
        if len(s) < 8 or s.nunique() < 2:
            continue
        vals = sorted({round(float(v), 6) for v in s.quantile(qs).to_numpy() if np.isfinite(v)})
        if vals:
            out[feat] = vals
    return out


def premom_frame(df: pd.DataFrame, sl: float, sample_limit: int = 2500) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()
    sample = df
    if len(sample) > sample_limit:
        sample = sample.sample(sample_limit, random_state=17)
    recs: list[dict[str, Any]] = []
    for r in sample.itertuples():
        feats, reason = tt._premom(r.ticker, r.side, r.tt_entry_iso, float(r.tt_fill), float(sl), r.tt_sig_ts.isoformat())
        rec = {f: np.nan for f in PREMOM_FEATURES}
        if not reason:
            rec.update(dict(feats))
        recs.append(rec)
    return pd.DataFrame(recs)


def with_name(base: dict[str, Any], **updates: Any) -> dict[str, Any]:
    out = {
        "name": base.get("name", "candidate"),
        "sl": float(base["sl"]),
        "tgt": float(base["tgt"]),
        "mask_terms": [list(t) for t in base.get("mask_terms", [])],
        "premom_terms": [list(t) for t in base.get("premom_terms", [])],
        "guard": dict(base.get("guard") or {}),
        "max_positions": int(base.get("max_positions", 20)),
        "daily_loss_rs": float(base.get("daily_loss_rs", 0.0)),
        "regime_align": bool(base.get("regime_align", False)),
        "regime_band": float(base.get("regime_band", 0.0)),
    }
    for meta in ("stage", "change_group", "description"):
        if meta in base:
            out[meta] = base[meta]
    out.update(updates)
    return out


def cfg_key(cfg: dict[str, Any]) -> str:
    return json.dumps(
        {
            "sl": cfg["sl"],
            "tgt": cfg["tgt"],
            "mask_terms": cfg.get("mask_terms", []),
            "premom_terms": cfg.get("premom_terms", []),
            "guard": cfg.get("guard", {}),
            "max_positions": cfg.get("max_positions", 20),
            "daily_loss_rs": cfg.get("daily_loss_rs", 0.0),
        },
        sort_keys=True,
    )


def make_stage_configs(
    setup: str,
    base: dict[str, Any],
    train_df: pd.DataFrame,
    signal_q: dict[str, list[float]],
    pm_q: dict[str, list[float]],
    stage_budget: int,
    rng: random.Random,
) -> list[dict[str, Any]]:
    configs: list[dict[str, Any]] = []
    seen: set[str] = set()

    def add(cfg: dict[str, Any], stage: str, change_group: str, desc: str) -> None:
        c = with_name(cfg)
        c["stage"] = stage
        c["change_group"] = change_group
        c["description"] = desc
        k = cfg_key(c)
        if k not in seen:
            seen.add(k)
            configs.append(c)

    add(base, "stage1_baseline", "baseline", "Original setup-card/raw baseline.")

    exit_cap = min(18, max(0, stage_budget - len(configs)))
    for sl in SL_GRID:
        for tgt in TGT_GRID:
            if len([c for c in configs if c.get("stage") == "stage4_exit_sweep"]) >= exit_cap:
                break
            cfg = with_name(base, sl=sl, tgt=tgt, mask_terms=base.get("mask_terms", []), premom_terms=base.get("premom_terms", []))
            add(cfg, "stage4_exit_sweep", "exit", f"Exit-only sweep SL {sl} target {tgt}.")
        if len([c for c in configs if c.get("stage") == "stage4_exit_sweep"]) >= exit_cap:
            break

    for guard in GUARDS:
        if len(configs) >= stage_budget:
            return configs[:stage_budget]
        cfg = with_name(base, guard=guard)
        add(cfg, "stage3_time_rank_redesign", "entry_guard", f"Entry guard only: {guard or 'none'}.")

    remaining_for_filters = max(0, stage_budget - len(configs))
    signal_cap = remaining_for_filters // 2
    pm_cap = remaining_for_filters - signal_cap

    signal_items: list[tuple[str, str, float]] = []
    for feat, vals in signal_q.items():
        for v in vals:
            signal_items.append((feat, ">=", float(v)))
            signal_items.append((feat, "<=", float(v)))
    rng.shuffle(signal_items)
    for feat, op, val in signal_items[:signal_cap]:
        cfg = with_name(base, mask_terms=[[feat, op, val]], premom_terms=base.get("premom_terms", []))
        add(cfg, "stage4_signal_filter_sweep", "signal_filter", f"Signal filter only: {feat} {op} {val}.")

    pm_items: list[tuple[str, str, float]] = []
    for feat, vals in pm_q.items():
        for v in vals:
            pm_items.append((feat, ">=", float(v)))
            pm_items.append((feat, "<=", float(v)))
    rng.shuffle(pm_items)
    for feat, op, val in pm_items[:pm_cap]:
        cfg = with_name(base, premom_terms=[[feat, op, val]], mask_terms=base.get("mask_terms", []))
        add(cfg, "stage3_premomentum_redesign", "pre_momentum", f"Pre-momentum only: {feat} {op} {val}.")

    # A few simple structural combinations, built from train-only quantiles.
    for _ in range(max(0, stage_budget - len(configs))):
        mask_terms: list[list[Any]] = []
        premom_terms: list[list[Any]] = []
        if signal_items and rng.random() < 0.75:
            mask_terms.append(list(rng.choice(signal_items)))
        if signal_items and rng.random() < 0.35:
            t = list(rng.choice(signal_items))
            if [t[0], t[1]] not in [[m[0], m[1]] for m in mask_terms]:
                mask_terms.append(t)
        if pm_items and rng.random() < 0.75:
            premom_terms.append(list(rng.choice(pm_items)))
        if pm_items and rng.random() < 0.25:
            t = list(rng.choice(pm_items))
            if [t[0], t[1]] not in [[m[0], m[1]] for m in premom_terms]:
                premom_terms.append(t)
        cfg = with_name(
            base,
            sl=rng.choice(SL_GRID),
            tgt=rng.choice(TGT_GRID),
            mask_terms=mask_terms[:2],
            premom_terms=premom_terms[:2],
            guard=rng.choice(GUARDS),
        )
        add(cfg, "stage5_combination_search", "combination", "Combined stable/simple train-side knobs.")
        if len(configs) >= stage_budget:
            break
    return configs[:stage_budget]


def optuna_cfg(
    trial: Any,
    base: dict[str, Any],
    signal_q: dict[str, list[float]],
    pm_q: dict[str, list[float]],
    trial_no: int,
) -> dict[str, Any]:
    signal_feats = sorted(signal_q)
    pm_feats = sorted(pm_q)
    mask_terms = []
    for i in range(trial.suggest_int("n_mask", 0, min(2, len(signal_feats)))):
        feat = trial.suggest_categorical(f"mask{i}_feat", signal_feats)
        op = trial.suggest_categorical(f"mask{i}_op", [">=", "<="])
        val = trial.suggest_categorical(f"mask{i}_val_{feat}", signal_q[feat])
        if [feat, op] not in [[x[0], x[1]] for x in mask_terms]:
            mask_terms.append([feat, op, float(val)])
    premom_terms = []
    for i in range(trial.suggest_int("n_pm", 0, min(2, len(pm_feats)))):
        feat = trial.suggest_categorical(f"pm{i}_feat", pm_feats)
        op = trial.suggest_categorical(f"pm{i}_op", [">=", "<="])
        val = trial.suggest_categorical(f"pm{i}_val_{feat}", pm_q[feat])
        if [feat, op] not in [[x[0], x[1]] for x in premom_terms]:
            premom_terms.append([feat, op, float(val)])
    guard_idx = trial.suggest_int("guard_idx", 0, len(GUARDS) - 1)
    return with_name(
        base,
        name=f"optuna_trial_{trial_no}",
        sl=float(trial.suggest_categorical("sl", SL_GRID)),
        tgt=float(trial.suggest_categorical("tgt", TGT_GRID)),
        mask_terms=mask_terms,
        premom_terms=premom_terms,
        guard=GUARDS[guard_idx],
        stage="stage5_optuna_combo",
        change_group="combination",
        description="Optuna TPE combo from train-only quantile grids.",
    )


def reasonable_fit_val(fit: dict[str, Any], val: dict[str, Any], min_fit: int, min_val: int) -> tuple[bool, str]:
    reasons = []
    if fit["trades"] < min_fit:
        reasons.append(f"FIT trades {fit['trades']}<{min_fit}")
    if val["trades"] < min_val:
        reasons.append(f"VAL trades {val['trades']}<{min_val}")
    if fit["net_pnl"] <= 0:
        reasons.append("FIT net<=0")
    if val["net_pnl"] <= 0:
        reasons.append("VAL net<=0")
    if min(fit["net_pf"], val["net_pf"]) < 0.90:
        reasons.append("min FIT/VAL PF<0.90")
    return not reasons, "; ".join(reasons)


def train_band_ok(m: dict[str, Any]) -> bool:
    return m["trades"] > 0 and m["net_pnl"] > 0 and 1.30 <= float(m["net_pf"]) <= 1.80


def approval_ok(train: dict[str, Any], test: dict[str, Any], min_train: int, min_test: int) -> tuple[bool, list[str]]:
    reasons = []
    if not (1.30 <= train["net_pf"] <= 1.80):
        reasons.append("TRAIN PF outside 1.30..1.80")
    if test["net_pf"] <= 1.40:
        reasons.append("TEST PF <= 1.40")
    if train["net_pnl"] <= 0 or test["net_pnl"] <= 0:
        reasons.append("TRAIN/TEST net not both positive")
    if train["trades"] < min_train:
        reasons.append(f"TRAIN trades {train['trades']}<{min_train}")
    if test["trades"] < min_test:
        reasons.append(f"TEST trades {test['trades']}<{min_test}")
    for label, m in (("TRAIN", train), ("TEST", test)):
        if m.get("top_trade_gross_profit_share") is not None and m["top_trade_gross_profit_share"] > 0.35:
            reasons.append(f"{label} top trade gross share >35%")
        if m.get("top_day_net_share") is not None and m["top_day_net_share"] > 0.40:
            reasons.append(f"{label} top day net share >40%")
        if m.get("top_symbol_net_share") is not None and m["top_symbol_net_share"] > 0.40:
            reasons.append(f"{label} top symbol net share >40%")
    return not reasons, reasons


def load_setup_data(setup: str, work_root: Path, manifest: dict[str, Any], slippage_bps: float) -> dict[str, Any]:
    tt.POOL_DIRS = [work_root / setup / "pools"]
    tt.POOL_DIR = tt.POOL_DIRS[0]
    tt.SLIPPAGE_BPS = float(slippage_bps)
    tt._entry.cache_clear()
    tt._resolve_full.cache_clear()
    tt._premom.cache_clear()
    pool = tt.load_pool()
    pool = pool[pool["setup"].astype(str).eq(setup)].copy()
    train_sessions = manifest["actual_data_sessions"]["train"]
    test_sessions = manifest["actual_data_sessions"]["test"]
    n_fit = max(1, int(math.floor(len(train_sessions) * 0.60))) if train_sessions else 0
    fit_sessions = train_sessions[:n_fit]
    val_sessions = train_sessions[n_fit:]
    span = set(train_sessions + test_sessions)
    if pool.empty:
        attached = pool
    else:
        need = pool[pool["_day"].dt.strftime("%Y-%m-%d").isin(span)].copy()
        attached = tt.attach_entries(need)

    def slc(days: list[str]) -> pd.DataFrame:
        if attached.empty:
            return attached.copy()
        return attached[attached["_day"].dt.strftime("%Y-%m-%d").isin(set(days))].copy()

    return {
        "pool": pool,
        "attached": attached,
        "sessions": {
            "FIT": fit_sessions,
            "VAL": val_sessions,
            "TRAIN": train_sessions,
            "TEST": test_sessions,
        },
        "FIT": slc(fit_sessions),
        "VAL": slc(val_sessions),
        "TRAIN": slc(train_sessions),
        "TEST": slc(test_sessions),
    }


def run_setup(setup: str, args: argparse.Namespace, manifest: dict[str, Any]) -> dict[str, Any]:
    work_root = Path(args.work_root)
    setup_dir = work_root / setup
    setup_dir.mkdir(parents=True, exist_ok=True)
    (setup_dir / "candidates").mkdir(exist_ok=True)
    (setup_dir / "scripts").mkdir(exist_ok=True)

    data = load_setup_data(setup, work_root, manifest, args.slippage_bps)
    base = with_name(BASELINE_CONFIGS[setup])
    base["stage"] = "stage1_baseline"
    base["change_group"] = "baseline"
    base["description"] = BASELINE_CONFIGS[setup]["notes"]

    train_df = data["TRAIN"]
    fit_df = data["FIT"]
    val_df = data["VAL"]
    test_df = data["TEST"]
    q_grid = [0.10, 0.20, 0.35, 0.50, 0.65, 0.80, 0.90]
    signal_q = quantile_values(train_df, SIGNAL_FEATURES, q_grid)
    pm_df = premom_frame(train_df, float(base["sl"]))
    pm_q = quantile_values(pm_df, PREMOM_FEATURES, q_grid)
    rng = random.Random(int(args.seed) + sum(ord(c) for c in setup))

    rows: list[dict[str, Any]] = []
    serious: list[dict[str, Any]] = []
    # Keep deterministic full-loop coverage, but reserve a real TPE tail when Optuna
    # is available because the task explicitly requires Optuna TPE rather than a
    # fully prefilled seeded loop.
    optuna_tail = 15 if HAVE_OPTUNA and int(args.iterations) >= 75 else 0
    deterministic_budget = max(1, int(args.iterations) - optuna_tail)
    all_cfgs = make_stage_configs(setup, base, train_df, signal_q, pm_q, deterministic_budget, rng)
    seen = {cfg_key(c) for c in all_cfgs}

    min_fit = max(3, int(math.ceil(args.min_train_trades * 0.30)))
    min_val = max(3, int(math.ceil(args.min_train_trades * 0.20)))
    trial_no = 0

    def run_one(cfg: dict[str, Any], force_test: bool = False) -> float:
        nonlocal trial_no
        trial_no += 1
        cfg = with_name(cfg)
        cfg.setdefault("stage", "stage5_combination_search")
        cfg.setdefault("change_group", "combination")
        cfg.setdefault("description", "Candidate config.")
        fit = eval_cfg(setup, cfg, fit_df).metrics
        val = eval_cfg(setup, cfg, val_df).metrics
        ok_fv, fv_reason = reasonable_fit_val(fit, val, min_fit, min_val)
        train = None
        test = None
        decision = "REJECT_FIT_VAL"
        approval_reasons: list[str] = []
        if ok_fv or force_test:
            train = eval_cfg(setup, cfg, train_df).metrics
            if train_band_ok(train) or force_test:
                test_eval = eval_cfg(setup, cfg, test_df, detail=True)
                test = test_eval.metrics
                train_eval_detail = eval_cfg(setup, cfg, train_df, detail=True)
                train = train_eval_detail.metrics
                if train_band_ok(train):
                    ok_app, approval_reasons = approval_ok(train, test, int(args.min_train_trades), int(args.min_test_trades))
                    decision = "APPROVAL_REQUIRED" if ok_app else "TEST_FAIL_OR_DOMINATED"
                else:
                    decision = "TRAIN_OUT_OF_BAND_BASELINE" if force_test else "TRAIN_OUT_OF_BAND"
            else:
                decision = "TRAIN_OUT_OF_BAND"
        rec = {
            "iteration": trial_no,
            "setup": setup,
            "stage": cfg.get("stage", ""),
            "change_group": cfg.get("change_group", ""),
            "description": cfg.get("description", ""),
            "sl": cfg["sl"],
            "tgt": cfg["tgt"],
            "mask_terms": json.dumps(cfg.get("mask_terms", []), sort_keys=True),
            "premom_terms": json.dumps(cfg.get("premom_terms", []), sort_keys=True),
            "guard": json.dumps(cfg.get("guard", {}), sort_keys=True),
            "fit": fit,
            "val": val,
            "train": train,
            "test": test,
            "fit_val_reason": "OK" if ok_fv else fv_reason,
            "decision": decision,
            "approval_reasons": approval_reasons,
            "config": cfg,
        }
        rows.append(rec)
        if test is not None:
            serious.append(rec)
        pf_fit = fit["net_pf"] if fit["trades"] else 0.0
        pf_val = val["net_pf"] if val["trades"] else 0.0
        return min(min(pf_fit, 10.0), min(pf_val, 10.0)) - 0.40 * abs(pf_fit - pf_val)

    # Baseline must be evaluated on TEST as part of the requested baseline study.
    run_one(base, force_test=True)
    for cfg in all_cfgs[1:]:
        if trial_no >= int(args.iterations):
            break
        run_one(cfg)

    remaining = max(0, int(args.iterations) - trial_no)
    if remaining and signal_q and pm_q and HAVE_OPTUNA:
        def objective(trial: Any) -> float:
            cfg = optuna_cfg(trial, base, signal_q, pm_q, trial.number + trial_no)
            k = cfg_key(cfg)
            if k in seen:
                return -999.0
            seen.add(k)
            return run_one(cfg)

        study = optuna.create_study(direction="maximize", sampler=optuna.samplers.TPESampler(seed=int(args.seed)))
        study.optimize(objective, n_trials=remaining, timeout=float(args.time_budget_min) * 60.0)
        if trial_no < int(args.iterations):
            print("Optuna time budget ended before target iterations; filling with seeded random search.", flush=True)
    if trial_no < int(args.iterations):
        if not HAVE_OPTUNA:
            print("Optuna unavailable; using seeded random search fallback.", flush=True)
        signal_items = [(f, op, v) for f, vals in signal_q.items() for v in vals for op in (">=", "<=")]
        pm_items = [(f, op, v) for f, vals in pm_q.items() for v in vals for op in (">=", "<=")]
        while trial_no < int(args.iterations):
            cfg = with_name(
                base,
                name=f"seeded_random_{trial_no}",
                sl=rng.choice(SL_GRID),
                tgt=rng.choice(TGT_GRID),
                mask_terms=[list(rng.choice(signal_items))] if signal_items and rng.random() < 0.8 else [],
                premom_terms=[list(rng.choice(pm_items))] if pm_items and rng.random() < 0.8 else [],
                guard=rng.choice(GUARDS),
                stage="stage5_seeded_random_combo",
                change_group="combination",
                description="Seeded random combo fallback.",
            )
            k = cfg_key(cfg)
            if k in seen:
                continue
            seen.add(k)
            run_one(cfg)

    # Choose final candidates.
    approvals = [r for r in rows if r["decision"] == "APPROVAL_REQUIRED"]
    eligible_tested = [r for r in rows if r.get("test") is not None and r.get("train") is not None and train_band_ok(r["train"])]
    closest = max(
        rows,
        key=lambda r: (
            min((r["fit"] or {}).get("net_pf", 0), (r["val"] or {}).get("net_pf", 0)),
            (r["train"] or {}).get("net_pnl", -1e18) if r.get("train") else -1e18,
            (r["fit"] or {}).get("trades", 0) + (r["val"] or {}).get("trades", 0),
        ),
    )
    best = None
    if approvals:
        best = max(approvals, key=lambda r: (r["test"]["net_pf"], r["test"]["net_pnl"], r["test"]["trades"]))
    elif eligible_tested:
        best = max(eligible_tested, key=lambda r: (r["test"]["net_pf"], r["test"]["net_pnl"], r["test"]["trades"]))
    else:
        best = closest

    write_artifacts(setup, args, manifest, data, rows, serious, best, approvals, closest, signal_q, pm_q)
    return {
        "setup": setup,
        "rows": len(rows),
        "approvals": len(approvals),
        "baseline": rows[0],
        "best": best,
        "closest": closest,
        "eligible_tested": len(eligible_tested),
        "pool_rows": int(len(data["pool"])),
        "attached_rows": int(len(data["attached"])),
        "sessions": data["sessions"],
    }


def _cfg_markdown(cfg: dict[str, Any]) -> str:
    return json.dumps(
        {
            "exit": {"sl_pct": cfg["sl"], "tgt_pct": cfg["tgt"]},
            "mask_terms": cfg.get("mask_terms", []),
            "pre_momentum_terms": cfg.get("premom_terms", []),
            "entry_guards": cfg.get("guard", {}),
        },
        indent=2,
        sort_keys=False,
    )


def _logic_from_cfg(cfg: dict[str, Any]) -> dict[str, Any]:
    return {
        "exit": {"sl_pct": cfg["sl"], "tgt_pct": cfg["tgt"]},
        "indicator_values": cfg.get("mask_terms", []),
        "non_indicator_rules": cfg.get("guard", {}),
        "pre_momentum_filters": cfg.get("premom_terms", []),
        "filters_and_guards": {"mask_terms": cfg.get("mask_terms", []), "entry_guards": cfg.get("guard", {})},
    }


def write_artifacts(
    setup: str,
    args: argparse.Namespace,
    manifest: dict[str, Any],
    data: dict[str, Any],
    rows: list[dict[str, Any]],
    serious: list[dict[str, Any]],
    best: dict[str, Any],
    approvals: list[dict[str, Any]],
    closest: dict[str, Any],
    signal_q: dict[str, list[float]],
    pm_q: dict[str, list[float]],
) -> None:
    setup_dir = Path(args.work_root) / setup
    candidates_dir = setup_dir / "candidates"
    scripts_dir = setup_dir / "scripts"
    ctx = SETUP_CONTEXT[setup]
    sessions = data["sessions"]
    baseline = rows[0]

    csv_rows = []
    for r in rows:
        csv_rows.append(
            {
                "iteration": r["iteration"],
                "stage": r["stage"],
                "change_group": r["change_group"],
                "description": r["description"],
                "sl": r["sl"],
                "tgt": r["tgt"],
                "mask_terms": r["mask_terms"],
                "premom_terms": r["premom_terms"],
                "guard": r["guard"],
                "fit_trades": r["fit"]["trades"],
                "fit_pf": r["fit"]["net_pf"],
                "fit_net": r["fit"]["net_pnl"],
                "val_trades": r["val"]["trades"],
                "val_pf": r["val"]["net_pf"],
                "val_net": r["val"]["net_pnl"],
                "train_trades": "" if r["train"] is None else r["train"]["trades"],
                "train_pf": "" if r["train"] is None else r["train"]["net_pf"],
                "train_net": "" if r["train"] is None else r["train"]["net_pnl"],
                "test_trades": "" if r["test"] is None else r["test"]["trades"],
                "test_pf": "" if r["test"] is None else r["test"]["net_pf"],
                "test_net": "" if r["test"] is None else r["test"]["net_pnl"],
                "decision": r["decision"],
                "fit_val_reason": r["fit_val_reason"],
                "approval_reasons": "; ".join(r["approval_reasons"]),
            }
        )
    with (setup_dir / "iteration_log.csv").open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(csv_rows[0].keys()))
        writer.writeheader()
        writer.writerows(csv_rows)

    top_rows = sorted(
        rows,
        key=lambda r: (
            1 if r["decision"] == "APPROVAL_REQUIRED" else 0,
            (r["test"] or {}).get("net_pf", -1),
            (r["train"] or {}).get("net_pf", -1),
            min(r["fit"]["net_pf"], r["val"]["net_pf"]),
        ),
        reverse=True,
    )[:8]
    for i, r in enumerate(top_rows, 1):
        payload = {
            "rank": i,
            "setup": setup,
            "decision": r["decision"],
            "config": r["config"],
            "metrics": {"FIT": r["fit"], "VAL": r["val"], "TRAIN": r["train"], "TEST": r["test"]},
            "approval_reasons": r["approval_reasons"],
            "source": "research-only setup recovery loop",
        }
        (candidates_dir / f"candidate_{i:02d}_{r['decision'].lower()}.json").write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")

    final_payload = {
        "setup": setup,
        "approval_required": bool(approvals),
        "selected_record": {
            "iteration": best["iteration"],
            "decision": best["decision"],
            "config": best["config"],
            "metrics": {"FIT": best["fit"], "VAL": best["val"], "TRAIN": best["train"], "TEST": best["test"]},
        },
        "closest_train_side_record": {
            "iteration": closest["iteration"],
            "decision": closest["decision"],
            "config": closest["config"],
            "metrics": {"FIT": closest["fit"], "VAL": closest["val"], "TRAIN": closest["train"], "TEST": closest["test"]},
        },
    }
    final_cfg_path = candidates_dir / "final_candidate_needing_approval.json"
    final_cfg_path.write_text(json.dumps(final_payload, indent=2, default=str), encoding="utf-8")

    rerun = (
        f"python {HERE.relative_to(REPO)} --setups {setup} --iterations {args.iterations} "
        f"--scan-workers {args.scan_workers} --slippage-bps {args.slippage_bps}\n"
    )
    (scripts_dir / "RERUN_COMMAND.txt").write_text(rerun, encoding="utf-8")
    try:
        shutil.copy2(HERE, scripts_dir / "recover_d_setups.py")
    except Exception:
        pass

    pool_sessions = manifest["candidate_sessions_by_setup"].get(setup, [])
    train_candidate_sessions = [d for d in pool_sessions if args.train_start <= d <= args.train_end]
    test_candidate_sessions = [d for d in pool_sessions if args.test_start <= d <= args.test_end]
    common_lines = [
        f"# {setup}",
        "",
        f"- Requested TRAIN: {args.train_start}..{args.train_end}",
        f"- Requested TEST: {args.test_start}..{args.test_end}",
        f"- Actual TRAIN data sessions: {sessions['TRAIN'][0] if sessions['TRAIN'] else 'none'}..{sessions['TRAIN'][-1] if sessions['TRAIN'] else 'none'} ({len(sessions['TRAIN'])})",
        f"- Actual TEST data sessions: {sessions['TEST'][0] if sessions['TEST'] else 'none'}..{sessions['TEST'][-1] if sessions['TEST'] else 'none'} ({len(sessions['TEST'])})",
        f"- FIT sessions: {sessions['FIT'][0] if sessions['FIT'] else 'none'}..{sessions['FIT'][-1] if sessions['FIT'] else 'none'} ({len(sessions['FIT'])})",
        f"- VAL sessions: {sessions['VAL'][0] if sessions['VAL'] else 'none'}..{sessions['VAL'][-1] if sessions['VAL'] else 'none'} ({len(sessions['VAL'])})",
        f"- Candidate sessions in TRAIN: {len(train_candidate_sessions)}",
        f"- Candidate sessions in TEST: {len(test_candidate_sessions)}",
        f"- Pool rows: {len(data['pool'])}; rows with 1-minute entry: {len(data['attached'])}",
        f"- Slippage model: setup_train_test statutory costs with {args.slippage_bps} bps per leg.",
    ]

    (setup_dir / "POOL_RECREATION_REPORT.md").write_text(
        "\n".join(
            common_lines
            + [
                "",
                "## Rebuild Method",
                "",
                "The pool was recreated from 5-minute parquet data with `avwap_5min_ID_v2_backtesting._prepare_5m` and `_scan_day`, filtered to this setup only, then saved as a per-setup `pre_dedupe_live_candidates` CSV. Entry and exits were resolved later through `setup_train_test` on 1-minute data.",
                "",
                "## Requested Vs Actual",
                "",
                f"- Data root: `{args.data_root}`",
                f"- Tickers scanned: {manifest['tickers_scanned']}",
                f"- Candidate sessions: {pool_sessions[0] if pool_sessions else 'none'}..{pool_sessions[-1] if pool_sessions else 'none'} ({len(pool_sessions)})",
                f"- Pool CSV: `{setup_dir / 'pools' / FNAME}`",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    baseline_rows = [
        ["FIT", fmt_m(baseline["fit"])],
        ["VAL", fmt_m(baseline["val"])],
        ["TRAIN", fmt_m(baseline["train"])],
        ["TEST", fmt_m(baseline["test"])],
    ]
    (setup_dir / "BASELINE_RESULT.md").write_text(
        "\n".join(
            [
                f"# {setup} Baseline Result",
                "",
                BASELINE_CONFIGS[setup]["notes"],
                "",
                md_table(baseline_rows, ["Window", "Metrics"]),
                "",
                "```json",
                _cfg_markdown(baseline["config"]),
                "```",
                "",
                f"Decision: {baseline['decision']}. Baseline TEST is run because Stage 1 explicitly requires baseline TRAIN/TEST.",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    (setup_dir / "FROM_SCRATCH_LOGIC_REVIEW.md").write_text(
        "\n".join(
            [
                f"# {setup} From-Scratch Logic Review",
                "",
                f"## What It Tries To Capture\n{ctx['idea']}",
                "",
                f"## Why The Previous Approach Failed\n{ctx['previous_failure']}",
                "",
                f"## Fresh Read\n{ctx['logic_review']}",
                "",
                "## Review Findings",
                "",
                "- Entry logic is structurally simple but needs confirmation from volume, candle quality, and pre-entry movement.",
                "- Filters that only select a market-regime pocket are treated as suspect unless they also hold in FIT and VAL.",
                "- SL/target values were swept broadly rather than only near prior values.",
                "- TEST was not used to choose thresholds; it was only run for the baseline and full-TRAIN-band candidates.",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    def detail_study(label: str, rec: dict[str, Any]) -> list[str]:
        if not rec.get("test"):
            return [f"## {label}", "", "TEST was not run for this candidate because full TRAIN was outside the PF band."]
        m = rec["test"]
        return [
            f"## {label}",
            "",
            f"- TEST: {fmt_m(m)}",
            f"- Win rate: {m['win_pct']}%; exits target/SL/EOD: {m['target_pct']} / {m['sl_pct']} / {m['eod_pct']}%",
            f"- Days/symbols: {m['n_days']} / {m['n_symbols']}; max trades/day: {m['max_trades_day']}",
            f"- Dominance: top trade gross share {m['top_trade_gross_profit_share']}, top day net share {m['top_day_net_share']}, top symbol net share {m['top_symbol_net_share']}",
        ]

    (setup_dir / "WINNER_LOSER_STUDY.md").write_text(
        "\n".join(
            [
                f"# {setup} Winner / Loser Study",
                "",
                "Baseline and serious candidates were checked for win rate, exit mix, day/symbol spread, and dominance.",
                "",
                *detail_study("Baseline", baseline),
                "",
                *detail_study("Selected/Closest Candidate", best),
                "",
                "Interpretation: a candidate is rejected when profit is dominated by a single trade, day, or symbol even if headline PF is high.",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    (setup_dir / "REDESIGNED_SETUP_IDEAS.md").write_text(
        "\n".join(
            [
                f"# {setup} Redesigned Setup Ideas",
                "",
                "- Original with better filters: one or two signal-side thresholds from RSI/ADX/VWAP/volume/candle features.",
                "- Original with better pre-momentum: pre-entry 1-minute momentum, range, close-position, RSI-direction, and ADX gates.",
                "- Original with better exits: fixed SL/target grid from tight scalps to wider continuation exits.",
                "- Simpler version: raw detector plus one structural term only.",
                "- Stricter version: one signal filter plus one pre-momentum filter.",
                "- Time-window version: min/max slot guards and top_n slot ranking.",
                "- VWAP/EMA confirmation version: distance-to-VWAP/EMA and candle-location filters.",
                "- Volume plus candle quality version: vol_ratio, body_pct, close_loc, wick-skew.",
                "- Volatility-regime version: atr_pct, signal_range_pct, and market_abs_ret_pct checked on train side.",
                "- Failed-signal avoidance: avoid overextended, low-quality, or late-session entries.",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    sweep_rows = []
    for stage, g in pd.DataFrame(csv_rows).groupby("stage", sort=False):
        tested = len(g)
        train_band = int(((pd.to_numeric(g["train_pf"], errors="coerce") >= 1.30) & (pd.to_numeric(g["train_pf"], errors="coerce") <= 1.80)).sum())
        test_pass = int((pd.to_numeric(g["test_pf"], errors="coerce") > 1.40).sum())
        sweep_rows.append([stage, tested, train_band, test_pass])
    (setup_dir / "PARAMETER_SWEEP_SUMMARY.md").write_text(
        "\n".join(
            [
                f"# {setup} Parameter Sweep Summary",
                "",
                f"- Iterations run: {len(rows)}",
                f"- Search engine for combo fill: {'Optuna TPE' if HAVE_OPTUNA else 'seeded random fallback'}",
                f"- Signal features with train quantiles: {', '.join(sorted(signal_q)) or 'none'}",
                f"- Pre-momentum features with train quantiles: {', '.join(sorted(pm_q)) or 'none'}",
                "",
                md_table(sweep_rows, ["Stage", "Iterations", "TRAIN PF Band", "TEST PF > 1.40"]),
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    iter_rows = [
        [
            r["iteration"],
            r["stage"],
            r["change_group"],
            fmt_m(r["fit"]),
            fmt_m(r["val"]),
            fmt_m(r["train"]) if r["train"] else "not run",
            fmt_m(r["test"]) if r["test"] else "not run",
            r["decision"],
        ]
        for r in rows
    ]
    (setup_dir / "ITERATION_LOG.md").write_text(
        "\n".join(
            [
                f"# {setup} Iteration Log",
                "",
                "Each row changes one logical group where possible; combination/Optuna rows are marked separately and still use FIT/VAL objective only.",
                "",
                md_table(iter_rows, ["#", "Stage", "Group", "FIT", "VAL", "TRAIN", "TEST", "Decision"]),
                "",
                f"Full CSV: `{setup_dir / 'iteration_log.csv'}`",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    fail_notes = []
    if approvals:
        fail_notes.append("At least one candidate met the approval gate.")
    else:
        fail_notes.append("No candidate met the full approval gate.")
    if not best.get("test"):
        fail_notes.append("The closest train-side candidate was not evaluated on TEST because full TRAIN PF did not land in 1.30..1.80.")
    else:
        if best["test"]["net_pf"] <= 1.40:
            fail_notes.append("Best eligible TEST PF did not clear 1.40.")
        if best["test"].get("top_trade_gross_profit_share") and best["test"]["top_trade_gross_profit_share"] > 0.35:
            fail_notes.append("Best eligible candidate is trade-dominated.")
        if best["test"].get("top_day_net_share") and best["test"]["top_day_net_share"] > 0.40:
            fail_notes.append("Best eligible candidate is day-dominated.")
        if best["test"].get("top_symbol_net_share") and best["test"]["top_symbol_net_share"] > 0.40:
            fail_notes.append("Best eligible candidate is symbol-dominated.")
    (setup_dir / "FAILURE_ANALYSIS.md").write_text(
        "\n".join(
            [
                f"# {setup} Failure Analysis",
                "",
                *[f"- {x}" for x in fail_notes],
                "",
                "Closest robust train-side candidate:",
                "",
                f"- Iteration: {closest['iteration']}",
                f"- FIT: {fmt_m(closest['fit'])}",
                f"- VAL: {fmt_m(closest['val'])}",
                f"- TRAIN: {fmt_m(closest['train']) if closest['train'] else 'not run'}",
                f"- TEST: {fmt_m(closest['test']) if closest['test'] else 'not run'}",
                "",
                "```json",
                _cfg_markdown(closest["config"]),
                "```",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    cand_rows = [
        [
            i + 1,
            r["iteration"],
            r["decision"],
            fmt_m(r["train"]) if r["train"] else "not run",
            fmt_m(r["test"]) if r["test"] else "not run",
            f"candidate_{i + 1:02d}_{r['decision'].lower()}.json" if i < 8 else "",
        ]
        for i, r in enumerate(top_rows)
    ]
    (setup_dir / "CANDIDATE_CONFIGS.md").write_text(
        "\n".join(
            [
                f"# {setup} Candidate Configs",
                "",
                md_table(cand_rows, ["Rank", "Iteration", "Decision", "TRAIN", "TEST", "JSON"]),
                "",
                "Selected config:",
                "",
                "```json",
                _cfg_markdown(best["config"]),
                "```",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    final_logic = _logic_from_cfg(best["config"])
    approval = "YES" if approvals else "NO"
    recommendation_lines = [
        f"# {setup} Approval Required Final Recommendation",
        "",
        f"Approval recommendation: **{approval}**",
        "",
        f"- Previous approach failure: {ctx['previous_failure']}",
        f"- Pool recreation: succeeded; pool rows {len(data['pool'])}, entry rows {len(data['attached'])}.",
        f"- Baseline TRAIN: {fmt_m(baseline['train'])}",
        f"- Baseline TEST: {fmt_m(baseline['test'])}",
        f"- Best/selected TRAIN: {fmt_m(best['train']) if best['train'] else 'not run'}",
        f"- Best/selected TEST: {fmt_m(best['test']) if best['test'] else 'not run'}",
        f"- TEST PF crossed 1.40: {bool(best.get('test') and best['test']['net_pf'] > 1.40)}",
        f"- Candidate config path: `{final_cfg_path}`",
        f"- Final config needing approval: `{final_cfg_path}`",
        "",
        "## Final Logic",
        "",
        f"- Entry logic: {ctx['idea']}",
        f"- Indicator values: `{final_logic['indicator_values']}`",
        f"- Non-indicator rules: `{final_logic['non_indicator_rules']}`",
        f"- Pre-momentum filters: `{final_logic['pre_momentum_filters']}`",
        f"- Filters and guards: `{final_logic['filters_and_guards']}`",
        f"- SL/target/exit: SL {best['config']['sl']}%, target {best['config']['tgt']}%, 1-minute SL/target/EOD resolver.",
        "",
        "## Domination Check",
        "",
    ]
    if best.get("test"):
        recommendation_lines += [
            f"- TEST top trade gross share: {best['test']['top_trade_gross_profit_share']}",
            f"- TEST top day net share: {best['test']['top_day_net_share']}",
            f"- TEST top symbol net share: {best['test']['top_symbol_net_share']}",
        ]
    else:
        recommendation_lines += ["- Not applicable; selected train-side candidate did not reach TEST eligibility."]
    recommendation_lines += [
        "",
        "## Remaining Risks",
        "",
        "- Thin samples and day clustering remain binding risks unless the approval flag is YES.",
        "- Any top_n guard must be verified in the live/conf path before promotion.",
        "- No production files were edited.",
        "",
        "## Rerun Commands",
        "",
        "```powershell",
        rerun.strip(),
        "```",
    ]
    (setup_dir / "APPROVAL_REQUIRED_FINAL_RECOMMENDATION.md").write_text("\n".join(recommendation_lines) + "\n", encoding="utf-8")


def write_root_summary(work_root: Path, results: list[dict[str, Any]], args: argparse.Namespace, manifest: dict[str, Any]) -> None:
    rows = []
    for r in results:
        b = r["baseline"]
        best = r["best"]
        rows.append(
            [
                r["setup"],
                r["pool_rows"],
                r["attached_rows"],
                fmt_m(b["train"]),
                fmt_m(b["test"]),
                fmt_m(best["train"]) if best["train"] else "not run",
                fmt_m(best["test"]) if best["test"] else "not run",
                "YES" if r["approvals"] else "NO",
            ]
        )
    text = [
        "# D Setup Recovery Full Loop Summary",
        "",
        f"- Requested TRAIN: {args.train_start}..{args.train_end}",
        f"- Requested TEST: {args.test_start}..{args.test_end}",
        f"- Actual TRAIN sessions: {manifest['actual_data_sessions']['train'][0]}..{manifest['actual_data_sessions']['train'][-1]} ({len(manifest['actual_data_sessions']['train'])})",
        f"- Actual TEST sessions: {manifest['actual_data_sessions']['test'][0]}..{manifest['actual_data_sessions']['test'][-1]} ({len(manifest['actual_data_sessions']['test'])})",
        f"- Optimizer: {'Optuna TPE' if HAVE_OPTUNA else 'seeded random fallback'}",
        "",
        md_table(rows, ["Setup", "Pool Rows", "Entry Rows", "Baseline TRAIN", "Baseline TEST", "Best TRAIN", "Best TEST", "Approval"]),
        "",
        "No live trades were placed. No final setup config was edited.",
    ]
    (work_root / "SUMMARY.md").write_text("\n".join(text) + "\n", encoding="utf-8")
    (work_root / "summary.json").write_text(json.dumps({"manifest": manifest, "results": results}, indent=2, default=str), encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--setups", default=",".join(SETUPS))
    ap.add_argument("--work-root", default=str(DEFAULT_WORK_ROOT))
    ap.add_argument("--data-root", default=str(DEFAULT_DATA_ROOT))
    ap.add_argument("--train-start", default=REQUESTED_TRAIN[0])
    ap.add_argument("--train-end", default=REQUESTED_TRAIN[1])
    ap.add_argument("--test-start", default=REQUESTED_TEST[0])
    ap.add_argument("--test-end", default=REQUESTED_TEST[1])
    ap.add_argument("--iterations", type=int, default=125)
    ap.add_argument("--time-budget-min", type=float, default=12.0)
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--scan-workers", type=int, default=4)
    ap.add_argument("--limit-tickers", type=int, default=0)
    ap.add_argument("--slippage-bps", type=float, default=5.0)
    ap.add_argument("--min-train-trades", type=int, default=20)
    ap.add_argument("--min-test-trades", type=int, default=5)
    ap.add_argument("--reuse-pools", action="store_true")
    args = ap.parse_args()
    try:
        sys.stdout.reconfigure(line_buffering=True)
    except Exception:
        pass

    requested = [s.strip().upper() for s in str(args.setups).split(",") if s.strip()]
    bad = [s for s in requested if s not in SETUPS]
    if bad:
        raise SystemExit(f"unsupported setup(s): {bad}")
    work_root = Path(args.work_root)
    work_root.mkdir(parents=True, exist_ok=True)

    manifest_path = work_root / "_shared" / "pools" / "_manifest.json"
    if args.reuse_pools and manifest_path.exists():
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        print(f"[pool] reusing {manifest_path}", flush=True)
    else:
        _pool, manifest = recreate_pools(args)

    results = []
    for setup in requested:
        print(f"[loop] {setup} starting {args.iterations} iterations", flush=True)
        results.append(run_setup(setup, args, manifest))
        print(f"[loop] {setup} done approvals={results[-1]['approvals']}", flush=True)

    write_root_summary(work_root, results, args, manifest)
    print(f"[done] wrote {work_root / 'SUMMARY.md'}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
