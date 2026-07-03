r"""Research-only G/D v2 candidate layer.

This script does not modify live scanner code or final_setup_conf.py. It takes the
current unified raw candidate pool, loads the surrounding 5-minute bars, and emits
new sequence-aware G/D research setup names into a separate pool:

    C:\TradingData\eqidv2\outputs_ID_v11_gd_v2_pool

The goal is to test whether explicit sequence mechanics help before touching the
production candidate scanner.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

_HERE = Path(__file__).resolve().parent
_REPO_ROOT = _HERE.parent
for _p in (str(_REPO_ROOT), str(_HERE)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import avwap_5min_ID_v11_backtesting as v11  # noqa: E402

ROOT = Path(r"C:\TradingData\eqidv2")
DEFAULT_IN = ROOT / "outputs_ID_v11_unified_pool" / "historical_all_available_pre_dedupe_live_candidates.csv"
DEFAULT_OUT = ROOT / "outputs_ID_v11_gd_v2_pool"
HIST_5M = ROOT / "stocks_indicators_5min_eq_live2"
LIVE_5M = ROOT / "stocks_indicators_5min_eq_live"

KEY_COLS = ["ticker", "side", "setup", "signal_time_ist"]
BASE_SETUPS = {"G_HIGHER_HIGH_BREAK", "D_AVWAP_LOSE_REVERSAL"}


def _num(value: Any, default: float = np.nan) -> float:
    try:
        out = float(value)
    except Exception:
        return default
    return out if np.isfinite(out) else default


def _safe_div(a: float, b: float) -> float:
    return float(a / b) if np.isfinite(a) and np.isfinite(b) and b != 0 else float("nan")


@lru_cache(maxsize=4096)
def _load_5m(ticker: str) -> pd.DataFrame | None:
    name = f"{str(ticker).upper()}_stocks_indicators_5min.parquet"
    frames: list[pd.DataFrame] = []
    for root in (HIST_5M, LIVE_5M):
        path = root / name
        if not path.exists():
            continue
        try:
            df = pd.read_parquet(path)
        except Exception:
            continue
        if df is None or df.empty or "date" not in df.columns:
            continue
        df = df.copy()
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df = df.dropna(subset=["date"])
        if df.empty:
            continue
        if getattr(df["date"].dt, "tz", None) is None:
            df["date"] = df["date"].dt.tz_localize("Asia/Kolkata")
        else:
            df["date"] = df["date"].dt.tz_convert("Asia/Kolkata")
        frames.append(df)
    if not frames:
        return None
    out = pd.concat(frames, ignore_index=True, sort=False)
    out = out.dropna(subset=["date"]).sort_values("date")
    out = out.drop_duplicates(subset=["date"], keep="last").reset_index(drop=True)
    for col in ("open", "high", "low", "close", "volume", "VWAP", "ATR", "ADX", "RSI", "EMA_20"):
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")
    return out


def _locate_signal(bars: pd.DataFrame, ts: pd.Timestamp) -> int | None:
    if ts.tzinfo is None:
        ts = ts.tz_localize("Asia/Kolkata")
    else:
        ts = ts.tz_convert("Asia/Kolkata")
    hits = bars.index[bars["date"].dt.floor("min").eq(ts.floor("min"))].to_list()
    return int(hits[-1]) if hits else None


def _close_loc(open_px: float, high: float, low: float, close: float) -> tuple[float, float, float, float]:
    rng = high - low
    close_loc = _safe_div(close - low, rng)
    body_pct = _safe_div(abs(close - open_px), rng)
    body_top = max(open_px, close)
    body_bottom = min(open_px, close)
    upper_wick_pct = _safe_div(high - body_top, close) * 100.0
    lower_wick_pct = _safe_div(body_bottom - low, close) * 100.0
    return close_loc, body_pct, upper_wick_pct, lower_wick_pct


def _features_for_row(row: pd.Series) -> dict[str, float | bool]:
    ticker = str(row.get("ticker", "")).upper().strip()
    ts = v11._normalise_ts(row.get("signal_time_ist"))
    if pd.isna(ts):
        return {}
    bars = _load_5m(ticker)
    if bars is None or bars.empty:
        return {}
    idx = _locate_signal(bars, ts)
    if idx is None or idx < 12:
        return {}

    cur = bars.iloc[idx]
    prev = bars.iloc[max(0, idx - 12):idx].copy()
    prev3 = bars.iloc[max(0, idx - 3):idx].copy()
    prev6 = bars.iloc[max(0, idx - 6):idx].copy()
    prev10 = bars.iloc[max(0, idx - 10):idx].copy()
    last6 = bars.iloc[max(0, idx - 5):idx + 1].copy()

    open_px = _num(cur.get("open"))
    high = _num(cur.get("high"))
    low = _num(cur.get("low"))
    close = _num(cur.get("close"))
    vwap = _num(cur.get("VWAP"))
    row_vwap_dist_atr = _num(row.get("vwap_dist_atr"))
    atr = _num(cur.get("ATR"))
    adx = _num(cur.get("ADX"))
    ema20 = _num(cur.get("EMA_20"))
    volume = _num(cur.get("volume"))
    if not all(np.isfinite(x) for x in (open_px, high, low, close, atr)) or atr <= 0:
        return {}

    close_loc, body_pct, upper_wick_pct, lower_wick_pct = _close_loc(open_px, high, low, close)
    prev_high10 = _num(pd.to_numeric(prev10.get("high"), errors="coerce").max())
    prev_low10 = _num(pd.to_numeric(prev10.get("low"), errors="coerce").min())
    min_low6 = _num(pd.to_numeric(prev6.get("low"), errors="coerce").min())
    max_high6 = _num(pd.to_numeric(prev6.get("high"), errors="coerce").max())
    prev_close1 = _num(prev.iloc[-1].get("close")) if not prev.empty else np.nan
    prev_close3 = _num(prev3.iloc[0].get("close")) if len(prev3) >= 3 else np.nan
    prev_vol10 = _num(pd.to_numeric(prev10.get("volume"), errors="coerce").mean())
    prev_vwap_max6 = _num(pd.to_numeric(prev6.get("VWAP"), errors="coerce").max())
    prev_vwap_min6 = _num(pd.to_numeric(prev6.get("VWAP"), errors="coerce").min())
    prev_close_above_vwap6 = bool(
        ((pd.to_numeric(prev6.get("close"), errors="coerce") > pd.to_numeric(prev6.get("VWAP"), errors="coerce")).fillna(False)).any()
    ) if "VWAP" in prev6 else False
    prev_close_below_vwap6 = bool(
        ((pd.to_numeric(prev6.get("close"), errors="coerce") < pd.to_numeric(prev6.get("VWAP"), errors="coerce")).fillna(False)).any()
    ) if "VWAP" in prev6 else False
    adx_prev5 = _num(bars.iloc[idx - 5].get("ADX")) if idx >= 5 else np.nan
    ema_prev5 = _num(bars.iloc[idx - 5].get("EMA_20")) if idx >= 5 else np.nan

    vwap_dist_atr = _safe_div(close - vwap, atr)
    if not np.isfinite(vwap_dist_atr):
        vwap_dist_atr = row_vwap_dist_atr
    ema20_dist_atr = _safe_div(close - ema20, atr)
    pullback_depth_atr = _safe_div(prev_high10 - min_low6, atr)
    break_margin_atr = _safe_div(close - prev_high10, atr)
    breakdown_margin_atr = _safe_div(prev_low10 - close, atr)
    momentum_3_atr = _safe_div(close - prev_close3, atr)
    vol_expansion = _safe_div(volume, prev_vol10)
    adx_slope_5 = adx - adx_prev5 if np.isfinite(adx) and np.isfinite(adx_prev5) else np.nan
    ema_slope_5_atr = _safe_div(ema20 - ema_prev5, atr)
    extension_from_pullback_low_atr = _safe_div(close - min_low6, atr)
    extension_from_pullback_high_atr = _safe_div(max_high6 - close, atr)
    signal_range_atr = _safe_div(high - low, atr)
    high_touched_vwap_band = bool(np.isfinite(vwap) and np.isfinite(high) and high >= vwap - 0.20 * atr)
    low_touched_vwap_band = bool(np.isfinite(vwap) and np.isfinite(low) and low <= vwap + 0.20 * atr)

    return {
        "seq_close_loc": close_loc,
        "seq_body_pct": body_pct,
        "seq_upper_wick_pct": upper_wick_pct,
        "seq_lower_wick_pct": lower_wick_pct,
        "seq_vwap_dist_atr": vwap_dist_atr,
        "seq_ema20_dist_atr": ema20_dist_atr,
        "seq_pullback_depth_atr": pullback_depth_atr,
        "seq_break_margin_atr": break_margin_atr,
        "seq_breakdown_margin_atr": breakdown_margin_atr,
        "seq_momentum_3_atr": momentum_3_atr,
        "seq_vol_expansion": vol_expansion,
        "seq_adx": adx,
        "seq_adx_slope_5": adx_slope_5,
        "seq_ema_slope_5_atr": ema_slope_5_atr,
        "seq_extension_from_pullback_low_atr": extension_from_pullback_low_atr,
        "seq_extension_from_pullback_high_atr": extension_from_pullback_high_atr,
        "seq_signal_range_atr": signal_range_atr,
        "seq_prev_close_above_vwap6": prev_close_above_vwap6,
        "seq_prev_close_below_vwap6": prev_close_below_vwap6,
        "seq_high_touched_vwap_band": high_touched_vwap_band,
        "seq_low_touched_vwap_band": low_touched_vwap_band,
        "seq_prev_vwap_max6": prev_vwap_max6,
        "seq_prev_vwap_min6": prev_vwap_min6,
        "seq_current_close": close,
        "seq_prev_close1": prev_close1,
    }


def _emit_variant(row: pd.Series, setup: str, reason: str, feats: dict[str, Any]) -> dict[str, Any]:
    out = row.to_dict()
    out["source_setup"] = row.get("setup")
    out["setup"] = setup
    out["side"] = "LONG" if setup.startswith("GDV2_G_") else "SHORT"
    out["candidate_family"] = "GDV2_RESEARCH"
    out["selection_mode"] = "gd_v2_research_sequence"
    out["_basis"] = "gd_v2_sequence"
    out["reason"] = reason
    out["status"] = "CANDIDATE"
    out["candidate_id"] = f"{out.get('ticker')}|{out.get('side')}|{setup}|{out.get('signal_time_ist')}"
    diag = {}
    try:
        diag = json.loads(str(row.get("diagnostics_json") or "{}"))
    except Exception:
        diag = {}
    diag["gd_v2_reason"] = reason
    diag["gd_v2_source_setup"] = row.get("setup")
    for k, v in feats.items():
        if isinstance(v, (bool, str)):
            diag[k] = v
        else:
            try:
                fv = float(v)
                if np.isfinite(fv):
                    diag[k] = round(fv, 6)
            except Exception:
                pass
    out["diagnostics_json"] = json.dumps(diag, default=str)
    for k, v in feats.items():
        out[k] = v
    return out


def _g_variants(row: pd.Series, feats: dict[str, Any]) -> list[dict[str, Any]]:
    rs = _num(row.get("rs_pct"), 0.0)
    vol = _num(feats.get("seq_vol_expansion"))
    close_loc = _num(feats.get("seq_close_loc"))
    body = _num(feats.get("seq_body_pct"))
    upper = _num(feats.get("seq_upper_wick_pct"))
    vdist = _num(feats.get("seq_vwap_dist_atr"))
    pull = _num(feats.get("seq_pullback_depth_atr"))
    brk = _num(feats.get("seq_break_margin_atr"))
    mom = _num(feats.get("seq_momentum_3_atr"))
    adx = _num(feats.get("seq_adx"))
    adx_slope = _num(feats.get("seq_adx_slope_5"))
    ema_slope = _num(feats.get("seq_ema_slope_5_atr"))
    ext = _num(feats.get("seq_extension_from_pullback_low_atr"))
    touched_vwap = bool(feats.get("seq_low_touched_vwap_band"))
    prev_below_vwap = bool(feats.get("seq_prev_close_below_vwap6"))

    base = (
        vdist >= 0.0 and vdist <= 3.2
        and pull >= 0.45 and pull <= 3.2
        and brk >= -0.05
        and mom >= 0.10
        and close_loc >= 0.55
        and body >= 0.30
        and upper <= 0.45
        and vol >= 1.0
        and rs >= -0.25
        and ext <= 4.2
    )
    lite = (
        vdist >= -0.10 and vdist <= 4.5
        and pull >= 0.25 and pull <= 4.8
        and brk >= -0.35
        and mom >= -0.05
        and close_loc >= 0.45
        and body >= 0.18
        and upper <= 0.80
        and vol >= 0.65
        and rs >= -1.25
        and ext <= 6.0
    )
    strict = (
        base
        and touched_vwap
        and prev_below_vwap
        and adx >= 20.0
        and adx_slope >= -2.0
        and ema_slope >= -0.05
        and rs >= 0.0
    )
    trend = (
        base
        and adx >= 24.0
        and adx_slope >= 0.0
        and ema_slope >= 0.0
        and vdist <= 2.4
        and ext <= 3.4
    )
    out = []
    if lite:
        out.append(_emit_variant(row, "GDV2_G_HIGHER_HIGH_BREAK_LITE", "hh_break_sequence_lite_broad_holdout_probe", feats))
    if base:
        out.append(_emit_variant(row, "GDV2_G_HIGHER_HIGH_BREAK_BASE", "hh_break_after_controlled_pullback_base", feats))
    if strict:
        out.append(_emit_variant(row, "GDV2_G_HIGHER_HIGH_BREAK_RETEST", "hh_break_retest_or_vwap_touch_then_accept", feats))
    if trend:
        out.append(_emit_variant(row, "GDV2_G_HIGHER_HIGH_BREAK_TREND", "hh_break_trend_adx_ema_confirmed", feats))
    return out


def _d_variants(row: pd.Series, feats: dict[str, Any]) -> list[dict[str, Any]]:
    rs = _num(row.get("rs_pct"), 0.0)
    vol = _num(feats.get("seq_vol_expansion"))
    close_loc = _num(feats.get("seq_close_loc"))
    body = _num(feats.get("seq_body_pct"))
    lower = _num(feats.get("seq_lower_wick_pct"))
    vdist = _num(feats.get("seq_vwap_dist_atr"))
    edist = _num(feats.get("seq_ema20_dist_atr"))
    breakdown = _num(feats.get("seq_breakdown_margin_atr"))
    mom = _num(feats.get("seq_momentum_3_atr"))
    adx = _num(feats.get("seq_adx"))
    adx_slope = _num(feats.get("seq_adx_slope_5"))
    ema_slope = _num(feats.get("seq_ema_slope_5_atr"))
    ext_down = _num(feats.get("seq_extension_from_pullback_high_atr"))
    touched_vwap = bool(feats.get("seq_high_touched_vwap_band"))
    prev_above_vwap = bool(feats.get("seq_prev_close_above_vwap6"))

    base = (
        vdist <= -0.15 and vdist >= -3.5
        and close_loc <= 0.45
        and body >= 0.25
        and lower <= 0.35
        and vol >= 1.0
        and rs <= 0.35
        and mom <= -0.05
        and ext_down <= 4.0
    )
    lite = (
        vdist <= -0.05 and vdist >= -5.0
        and close_loc <= 0.60
        and body >= 0.12
        and lower <= 0.75
        and vol >= 0.65
        and rs <= 1.25
        and mom <= 0.15
        and ext_down <= 6.0
    )
    fail_reclaim = (
        base
        and prev_above_vwap
        and touched_vwap
        and edist <= 0.15
        and adx >= 18.0
        and adx_slope >= -2.0
    )
    breakdown_confirm = (
        base
        and breakdown >= -0.05
        and adx >= 22.0
        and ema_slope <= 0.05
        and close_loc <= 0.35
        and vdist >= -2.6
    )
    out = []
    if lite:
        out.append(_emit_variant(row, "GDV2_D_AVWAP_LOSE_REVERSAL_LITE", "avwap_lose_sequence_lite_broad_holdout_probe", feats))
    if base:
        out.append(_emit_variant(row, "GDV2_D_AVWAP_LOSE_REVERSAL_BASE", "avwap_lose_pressure_base_proxy", feats))
    if fail_reclaim:
        out.append(_emit_variant(row, "GDV2_D_AVWAP_LOSE_REVERSAL_FAIL_RECLAIM", "prior_above_vwap_then_failed_reclaim_reject", feats))
    if breakdown_confirm:
        out.append(_emit_variant(row, "GDV2_D_AVWAP_LOSE_REVERSAL_BREAKDOWN", "vwap_loss_with_breakdown_and_trend_pressure", feats))
    return out


def build_layer(in_csv: Path, out_dir: Path) -> pd.DataFrame:
    use = pd.read_csv(in_csv, nrows=0).columns.tolist()
    df = pd.read_csv(in_csv, low_memory=False)
    for col in KEY_COLS:
        if col not in df.columns:
            raise SystemExit(f"input pool missing {col}")
    df = df[df["setup"].astype(str).isin(BASE_SETUPS)].copy()
    df = df.drop_duplicates(subset=KEY_COLS, keep="first").reset_index(drop=True)
    rows: list[dict[str, Any]] = []
    misses = 0
    for i, row in df.iterrows():
        feats = _features_for_row(row)
        if not feats:
            misses += 1
            continue
        setup = str(row.get("setup"))
        if setup == "G_HIGHER_HIGH_BREAK":
            rows.extend(_g_variants(row, feats))
        elif setup == "D_AVWAP_LOSE_REVERSAL":
            rows.extend(_d_variants(row, feats))
        if (i + 1) % 1000 == 0:
            print(f"[gd_v2] processed {i + 1:,}/{len(df):,} source rows -> {len(rows):,} v2 rows", flush=True)

    out = pd.DataFrame(rows)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_csv = out_dir / "historical_all_available_pre_dedupe_live_candidates.csv"
    out.to_csv(out_csv, index=False)

    if not out.empty:
        ts = pd.to_datetime(out["signal_time_ist"], errors="coerce", utc=True).dt.tz_convert("Asia/Kolkata").dt.tz_localize(None)
        rows_by_setup = out["setup"].astype(str).value_counts().to_dict()
        rows_by_basis = out["_basis"].astype(str).value_counts().to_dict()
        date_min = str(ts.min())
        date_max = str(ts.max())
    else:
        rows_by_setup = {}
        rows_by_basis = {}
        date_min = ""
        date_max = ""
    manifest = {
        "built_utc": datetime.now(timezone.utc).isoformat(),
        "source_pool": str(in_csv),
        "out_file": str(out_csv),
        "research_only": True,
        "source_rows": int(len(df)),
        "feature_misses": int(misses),
        "rows_total": int(len(out)),
        "date_min": date_min,
        "date_max": date_max,
        "rows_by_basis": rows_by_basis,
        "rows_by_setup": rows_by_setup,
        "notes": "GDV2 sequence/state research candidates; not production scanner output.",
    }
    (out_dir / "_manifest.json").write_text(json.dumps(manifest, indent=2, default=str), encoding="utf-8")
    print(json.dumps(manifest, indent=2, default=str))
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", default=str(DEFAULT_IN))
    ap.add_argument("--out", default=str(DEFAULT_OUT))
    args = ap.parse_args()
    build_layer(Path(args.input), Path(args.out))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
