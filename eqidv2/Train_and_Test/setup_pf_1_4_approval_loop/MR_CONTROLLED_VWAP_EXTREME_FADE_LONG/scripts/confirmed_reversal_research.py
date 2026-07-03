from __future__ import annotations

import argparse
import json
import math
import sys
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

HERE = Path(__file__).resolve().parent
TT_DIR = HERE
for _ in range(6):
    TT_DIR = TT_DIR.parent
    if (TT_DIR / "setup_train_test.py").exists():
        break
REPO = TT_DIR.parent
for p in (str(REPO), str(TT_DIR)):
    if p not in sys.path:
        sys.path.insert(0, p)

import avwap_5min_ID_v11_backtesting as v11  # noqa: E402
import setup_train_test as tt  # noqa: E402


SETUP = "MR_CONTROLLED_VWAP_EXTREME_FADE_LONG"
SIDE = "LONG"
PF_TRAIN_LO = 1.30
PF_TRAIN_HI = 1.70
PF_TEST_MIN = 1.40
DOM_CAP = 0.40


def clean(obj: Any) -> Any:
    if isinstance(obj, dict):
        return {k: clean(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [clean(v) for v in obj]
    if isinstance(obj, tuple):
        return [clean(v) for v in obj]
    if isinstance(obj, (np.integer,)):
        return int(obj)
    if isinstance(obj, (np.floating, float)):
        f = float(obj)
        return None if not math.isfinite(f) else round(f, 6)
    return obj


def load_pool(pool_dir: Path) -> pd.DataFrame:
    csv_path = pool_dir / "historical_all_available_pre_dedupe_live_candidates.csv"
    if not csv_path.exists():
        raise SystemExit(f"Missing pool CSV: {csv_path}")
    df = pd.read_csv(csv_path, low_memory=False)
    df = df[df["setup"].astype(str).eq(SETUP)].copy()
    if df.empty:
        raise SystemExit(f"No {SETUP} rows in {csv_path}")
    for c in ("ticker", "side", "setup", "signal_time_ist"):
        if c not in df.columns:
            df[c] = ""
    df = df.drop_duplicates(subset=["ticker", "side", "setup", "signal_time_ist"], keep="first")
    df["setup"] = df["setup"].astype(str).str.strip()
    df["side"] = df["side"].astype(str).str.upper().str.strip()
    df["ticker"] = df["ticker"].astype(str).str.upper().str.strip()
    df["tt_sig_ts"] = df["signal_time_ist"].map(v11._normalise_ts)
    df = df.dropna(subset=["tt_sig_ts"]).copy()
    df["_day"] = df["tt_sig_ts"].dt.normalize().dt.tz_localize(None)
    df["_slot"] = df["tt_sig_ts"].map(v11._fmt_ist)
    return v11._selected_strategy_features(df).reset_index(drop=True)


def reset_tt(pool_dir: Path, slippage_bps: float) -> None:
    tt.POOL_DIRS = [pool_dir]
    tt.POOL_DIR = pool_dir
    tt.SLIPPAGE_BPS = float(slippage_bps)
    tt.ENTRY_LATENCY_MIN = 0
    tt.REGIME_ALIGN = False
    tt.REGIME_BAND = 0.0
    tt._entry.cache_clear()
    tt._resolve_full.cache_clear()
    tt._premom.cache_clear()


def set_entry(out: pd.DataFrame, idx: Any, entry_ts: pd.Timestamp, raw_open: float) -> None:
    fill = round(float(raw_open) * (1.0 + tt.SLIPPAGE_BPS / 1e4), 2)
    qty = max(1, int(v11.V7_SIGNAL_NOTIONAL_RS / float(raw_open)))
    out.at[idx, "tt_entry_ok"] = True
    out.at[idx, "tt_entry_iso"] = entry_ts.isoformat()
    out.at[idx, "tt_fill"] = fill
    out.at[idx, "tt_qty"] = int(qty)
    out.at[idx, "notional"] = fill * int(qty)


def first_entry_after(bars: pd.DataFrame, ts: pd.Timestamp) -> tuple[pd.Timestamp, float] | None:
    ent = v11._first_1m_entry(bars, ts, max_delay_minutes=v11.V7_ENTRY_SEARCH_MAX_DELAY_MIN)
    if ent is None:
        return None
    entry_ts, raw_open = ent
    if raw_open <= 0:
        return None
    return pd.Timestamp(entry_ts), float(raw_open)


def build_entry_mode(df: pd.DataFrame, mode: str) -> pd.DataFrame:
    if mode == "base_next_1m":
        out = tt.attach_entries(df.copy())
        out["confirm_mode"] = mode
        out["confirm_wait_min"] = 1.0
        out["confirm_path_low_ok"] = True
        return out

    out = df.copy()
    out["tt_entry_ok"] = False
    out["tt_entry_iso"] = None
    out["tt_fill"] = np.nan
    out["tt_qty"] = 0
    out["notional"] = np.nan
    out["confirm_mode"] = mode
    out["confirm_wait_min"] = np.nan
    out["confirm_path_low_ok"] = False
    out["confirm_green_count"] = np.nan
    out["confirm_close_vs_signal_pct"] = np.nan

    for r in out.itertuples():
        bars = v11._load_1m_with_open(r.ticker)
        if bars is None or bars.empty:
            continue
        sig_ts = pd.Timestamp(r.tt_sig_ts)
        sig_high = float(getattr(r, "signal_high", np.nan))
        sig_low = float(getattr(r, "signal_low", np.nan))
        sig_close = float(getattr(r, "signal_close", np.nan))
        if not all(np.isfinite(x) for x in (sig_high, sig_low, sig_close)):
            continue
        look_m = 10 if "10m" in mode else 15
        post = bars[(bars.index > sig_ts) & (bars.index <= sig_ts + pd.Timedelta(minutes=look_m))]
        if post.empty:
            continue

        trigger_ts: pd.Timestamp | None = None
        if mode.startswith("confirm_break_high"):
            for ts, b in post.iterrows():
                path = post.loc[:ts]
                low_ok = float(path["low"].min()) >= sig_low
                green_count = int((path["close"] > path["open"]).sum())
                close_break = float(b["close"]) > sig_high
                bar_green = float(b["close"]) > float(b["open"])
                if close_break and bar_green and ("higherlow" not in mode or low_ok) and ("twogreen" not in mode or green_count >= 2):
                    trigger_ts = pd.Timestamp(ts)
                    out.at[r.Index, "confirm_path_low_ok"] = bool(low_ok)
                    out.at[r.Index, "confirm_green_count"] = green_count
                    out.at[r.Index, "confirm_close_vs_signal_pct"] = (float(b["close"]) / sig_close - 1.0) * 100.0
                    break
        elif mode.startswith("confirm_next5m"):
            post5 = bars[(bars.index > sig_ts) & (bars.index <= sig_ts + pd.Timedelta(minutes=5))]
            if len(post5) >= 3:
                low_ok = float(post5["low"].min()) >= sig_low
                close_last = float(post5["close"].iloc[-1])
                open_first = float(post5["open"].iloc[0])
                green_count = int((post5["close"] > post5["open"]).sum())
                follow = close_last > sig_close and close_last > open_first and green_count >= 3
                if follow and ("higherlow" not in mode or low_ok):
                    trigger_ts = pd.Timestamp(post5.index[-1])
                    out.at[r.Index, "confirm_path_low_ok"] = bool(low_ok)
                    out.at[r.Index, "confirm_green_count"] = green_count
                    out.at[r.Index, "confirm_close_vs_signal_pct"] = (close_last / sig_close - 1.0) * 100.0
        else:
            raise ValueError(f"Unknown entry mode: {mode}")

        if trigger_ts is None:
            continue
        ent = first_entry_after(bars, trigger_ts)
        if ent is None:
            continue
        entry_ts, raw_open = ent
        set_entry(out, r.Index, entry_ts, raw_open)
        out.at[r.Index, "confirm_wait_min"] = (entry_ts - sig_ts).total_seconds() / 60.0

    return out[out["tt_entry_ok"]].reset_index(drop=True)


def pf_from_net(net: np.ndarray) -> float:
    net = np.asarray(net, dtype=float)
    net = net[np.isfinite(net)]
    gp = float(net[net > 0].sum())
    gl = float(net[net <= 0].sum())
    if abs(gl) < 1e-9:
        return float("inf") if gp > 0 else 0.0
    return gp / abs(gl)


def eval_cfg(cfg: dict[str, Any], df: pd.DataFrame, detail: bool = False) -> tuple[dict[str, Any], pd.DataFrame]:
    tt.MAX_POSITIONS = int(cfg.get("max_positions", 20))
    tt.DAILY_LOSS_RS = float(cfg.get("daily_loss_rs", 0.0))
    tt.REGIME_ALIGN = bool(cfg.get("regime_align", False))
    tt.REGIME_BAND = float(cfg.get("regime_band", 0.0))
    fam = tt.eval_family({SETUP: cfg}, df)
    det = tt.book_detail(fam["book"], {SETUP: (cfg["sl"], cfg["tgt"])}) if detail and fam["trades"] else pd.DataFrame()
    m: dict[str, Any] = {
        "n": int(fam["trades"]),
        "pf": round(float(fam["net_pf"]), 4) if math.isfinite(float(fam["net_pf"])) else float("inf"),
        "net": round(float(fam["net_pnl"]), 2),
        "day_block_p": None if not math.isfinite(float(fam["day_block_p"])) else round(float(fam["day_block_p"]), 4),
        "win_pct": 0.0,
    }
    net = np.asarray(fam.get("net", []), dtype=float)
    if len(net):
        m["win_pct"] = round(float((net > 0).mean()) * 100.0, 2)
    if det.empty:
        m.update({"target_n": 0, "sl_n": 0, "eod_n": 0, "trade_dom": None, "day_dom": None, "sym_dom": None, "max_dd": 0.0})
        return m, det
    dnet = det["net_pnl_rs"].to_numpy(dtype=float)
    wins = dnet[dnet > 0]
    cum = np.cumsum(dnet)
    dd = float((cum - np.maximum.accumulate(cum)).min()) if len(cum) else 0.0
    total = float(dnet.sum())
    gp = float(wins.sum())
    day_net = det.groupby("trade_date")["net_pnl_rs"].sum()
    sym_net = det.groupby("ticker")["net_pnl_rs"].sum()
    m.update(
        {
            "target_n": int((det["outcome"].astype(str) == "TARGET").sum()),
            "sl_n": int((det["outcome"].astype(str) == "SL").sum()),
            "eod_n": int((det["outcome"].astype(str) == "EOD").sum()),
            "trade_dom": round(float(dnet.max()) / gp, 3) if gp > 0 else None,
            "day_dom": round(float(day_net.max()) / total, 3) if total > 0 else None,
            "sym_dom": round(float(sym_net.max()) / total, 3) if total > 0 else None,
            "max_dd": round(dd, 2),
        }
    )
    return m, det


def metric_line(m: dict[str, Any] | None) -> str:
    if not m:
        return "not run"
    return f"n={m['n']} PF={m['pf']} net=Rs{m['net']:,.0f} win={m.get('win_pct', 0)}% t/s/e={m.get('target_n', 0)}/{m.get('sl_n', 0)}/{m.get('eod_n', 0)} dbp={m.get('day_block_p')}"


def dominance_ok(m: dict[str, Any]) -> bool:
    for k in ("trade_dom", "day_dom", "sym_dom"):
        v = m.get(k)
        if v is None or float(v) > DOM_CAP:
            return False
    return True


def config_text(cfg: dict[str, Any]) -> str:
    mask = "; ".join(f"{a}{o}{b}" for a, o, b in cfg.get("mask_terms", [])) or "-"
    pm = "; ".join(f"{a}{o}{b}" for a, o, b in cfg.get("premom_terms", [])) or "-"
    guard = json.dumps(cfg.get("guard") or {}, sort_keys=True)
    return f"{cfg['entry_mode']} SL={cfg['sl']} TGT={cfg['tgt']} mask=[{mask}] premom=[{pm}] guard={guard}"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--pool", default=r"C:\TradingData\eqidv2\setup_pools_2026_06_29\MR_CONTROLLED_VWAP_EXTREME_FADE_LONG")
    ap.add_argument("--train_start", default="2025-06-03")
    ap.add_argument("--test_start", default="2026-04-01")
    ap.add_argument("--slippage_bps", type=float, default=15.0)
    args = ap.parse_args()

    outdir = HERE.parent
    pool_dir = Path(args.pool)
    reset_tt(pool_dir, args.slippage_bps)
    raw = load_pool(pool_dir)
    train_start = pd.Timestamp(args.train_start)
    test_start = pd.Timestamp(args.test_start)
    sessions = sorted(pd.Series(raw["_day"].dropna().unique()))
    train_days = [d for d in sessions if train_start <= pd.Timestamp(d) < test_start]
    test_days = [d for d in sessions if pd.Timestamp(d) >= test_start]
    fit_days = train_days[: len(train_days) // 2]
    val_days = train_days[len(train_days) // 2 :]
    span = set(map(pd.Timestamp, train_days + test_days))
    raw = raw[raw["_day"].isin(span)].copy().reset_index(drop=True)

    # Keep this intentionally small: the previous broad exit/filter optimizer already
    # failed. This script is testing the structural fix, not mining every pocket.
    entry_modes = [
        "base_next_1m",
        "confirm_break_high_10m",
        "confirm_break_high_10m_higherlow",
        "confirm_break_high_10m_higherlow_twogreen",
        "confirm_next5m_follow_higherlow",
    ]
    mode_frames = {mode: build_entry_mode(raw, mode) for mode in entry_modes}
    print("[confirmed] entry modes built: " + ", ".join(f"{k}={len(v)}" for k, v in mode_frames.items()), flush=True)

    def slice_days(df: pd.DataFrame, days: list[pd.Timestamp]) -> pd.DataFrame:
        return df[df["_day"].isin(set(map(pd.Timestamp, days)))].copy().reset_index(drop=True)

    masks: list[tuple[str, list[tuple[Any, Any, Any]]]] = [
        ("none", []),
        ("no_bear", [("regime", "!=", "BEAR")]),
        ("neutral_only", [("regime", "==", "NEUTRAL")]),
        ("market_ge_-0.30", [("market_ret_pct", ">=", -0.30)]),
        ("market_green", [("market_ret_pct", ">=", 0.0)]),
        ("rs_not_deep_negative", [("rs_pct", ">=", -1.0)]),
        ("not_extreme_below_vwap", [("vwap_dist_atr", ">=", -5.0)]),
        ("not_panic_volume", [("vol_ratio", "<=", 3.0)]),
        ("strong_close", [("close_loc", ">=", 0.60)]),
        ("small_body_rejection", [("body_pct", "<=", 0.35)]),
        ("no_bear_market_ge_-0.30", [("regime", "!=", "BEAR"), ("market_ret_pct", ">=", -0.30)]),
        ("market_rs_stable", [("market_ret_pct", ">=", -0.30), ("rs_pct", ">=", -1.0)]),
        ("not_panic_and_rs", [("vol_ratio", "<=", 3.0), ("rs_pct", ">=", -1.0)]),
    ]
    premoms: list[tuple[str, list[tuple[Any, Any, Any]]]] = [
        ("none", []),
        ("rsi_dir_up", [("sig5_rsi_dir", ">=", 50.0)]),
        ("pre_mom_nonneg", [("pre2_mom_r", ">=", 0.0)]),
    ]
    guards: list[tuple[str, dict[str, Any] | None]] = [
        ("none", None),
        ("before_1200", {"max_slot": "12:00"}),
        ("before_1300_top1", {"max_slot": "13:00", "top_n": 1}),
    ]
    exits = [(0.70, 0.80), (0.90, 0.80), (1.20, 1.00), (1.20, 1.25), (0.90, 1.25)]

    rows: list[dict[str, Any]] = []
    passing: list[dict[str, Any]] = []
    iter_n = 0

    total_configs = len(entry_modes) * len(exits) * len(masks) * len(premoms) * len(guards)
    print(f"[confirmed] evaluating {total_configs} compact configs", flush=True)
    for mode, frame in mode_frames.items():
        fit = slice_days(frame, fit_days)
        val = slice_days(frame, val_days)
        train = slice_days(frame, train_days)
        test = slice_days(frame, test_days)
        for sl, tgt in exits:
            for mask_name, mask_terms in masks:
                for pm_name, pm_terms in premoms:
                    for guard_name, guard in guards:
                        iter_n += 1
                        cfg = {
                            "entry_mode": mode,
                            "sl": sl,
                            "tgt": tgt,
                            "mask_terms": mask_terms,
                            "premom_terms": pm_terms,
                            "guard": guard,
                            "status": "OK",
                            "max_positions": 20,
                            "daily_loss_rs": 0.0,
                        }
                        mf, _ = eval_cfg(cfg, fit, detail=False)
                        mv, _ = eval_cfg(cfg, val, detail=False)
                        min_fv = min(float(mf["pf"]), float(mv["pf"]))
                        train_m = None
                        test_m = None
                        status = "FIT/VAL weak"
                        if mf["n"] >= 5 and mv["n"] >= 5 and min_fv >= 0.75:
                            # Fast TRAIN check first; compute detail only for band candidates.
                            train_m, _ = eval_cfg(cfg, train, detail=False)
                            if PF_TRAIN_LO <= float(train_m["pf"]) <= PF_TRAIN_HI:
                                train_m, _train_det = eval_cfg(cfg, train, detail=True)
                                test_m, _test_det = eval_cfg(cfg, test, detail=True)
                                if test_m["n"] >= 5 and float(test_m["pf"]) > PF_TEST_MIN and dominance_ok(test_m):
                                    status = "PASS"
                                    passing.append({"config": clean(cfg), "fit": clean(mf), "val": clean(mv), "train": clean(train_m), "test": clean(test_m)})
                                else:
                                    status = "TEST fail"
                            else:
                                status = "TRAIN out of band"
                        rows.append(
                            {
                                "iteration": iter_n,
                                "mode": mode,
                                "mask": mask_name,
                                "premom": pm_name,
                                "guard": guard_name,
                                "sl": sl,
                                "tgt": tgt,
                                "fit": clean(mf),
                                "val": clean(mv),
                                "train": clean(train_m),
                                "test": clean(test_m),
                                "status": status,
                                "config_text": config_text(cfg),
                            }
                        )

    # Baseline and redesigned family summaries with detail.
    baseline_cfg = {
        "entry_mode": "base_next_1m",
        "sl": 0.70,
        "tgt": 0.80,
        "mask_terms": [],
        "premom_terms": [],
        "guard": None,
        "status": "OK",
        "max_positions": 20,
        "daily_loss_rs": 0.0,
    }
    baseline_train, _ = eval_cfg(baseline_cfg, slice_days(mode_frames["base_next_1m"], train_days), detail=True)
    baseline_test, _ = eval_cfg(baseline_cfg, slice_days(mode_frames["base_next_1m"], test_days), detail=True)

    # Pick best train-side and best confirmed entry family even if no pass.
    scored = []
    for r in rows:
        tr = r.get("train")
        te = r.get("test")
        fit_pf = float(r["fit"]["pf"] or 0.0)
        val_pf = float(r["val"]["pf"] or 0.0)
        score = min(fit_pf, val_pf) - 0.25 * abs(fit_pf - val_pf)
        if tr:
            score += min(float(tr["pf"]), 2.0)
        if te:
            score += min(float(te["pf"]), 2.0)
        scored.append((score, r))
    scored.sort(key=lambda x: x[0], reverse=True)
    best_overall = scored[0][1] if scored else None
    confirmed_rows = [x for x in scored if x[1]["mode"] != "base_next_1m"]
    best_confirmed = confirmed_rows[0][1] if confirmed_rows else None

    passing.sort(key=lambda x: (float(x["test"]["pf"]), float(x["test"]["net"]), float(x["train"]["n"])), reverse=True)
    cand_dir = outdir / "candidates"
    cand_dir.mkdir(exist_ok=True)
    for i, cand in enumerate(passing, 1):
        cpath = cand_dir / f"{SETUP}_confirmed_reversal_candidate_{i:03d}.json"
        cand["candidate_path"] = str(cpath)
        cpath.write_text(json.dumps(clean(cand), indent=2, default=str), encoding="utf-8")

    summary = {
        "setup": SETUP,
        "pool": str(pool_dir),
        "split": {
            "fit": [str(pd.Timestamp(d).date()) for d in fit_days],
            "val": [str(pd.Timestamp(d).date()) for d in val_days],
            "train": [str(pd.Timestamp(d).date()) for d in train_days],
            "test": [str(pd.Timestamp(d).date()) for d in test_days],
        },
        "entry_mode_rows": {mode: int(len(frame)) for mode, frame in mode_frames.items()},
        "baseline": {"config": clean(baseline_cfg), "train": clean(baseline_train), "test": clean(baseline_test)},
        "best_overall": clean(best_overall),
        "best_confirmed": clean(best_confirmed),
        "passing": clean(passing),
        "iterations": clean(rows),
    }
    (outdir / "CONFIRMED_REVERSAL_RUN_SUMMARY.json").write_text(json.dumps(summary, indent=2, default=str), encoding="utf-8")

    report = [
        f"# {SETUP} - Confirmed Reversal Redesign",
        "",
        "Research-only run. No final config edit and no live execution.",
        "",
        "## Change Tested",
        "- Delayed entry until post-signal confirmation instead of buying the first next-minute open.",
        "- Confirmation variants: break above signal high within 10m, optional higher-low path, optional two green 1m bars, and next-5m follow-through.",
        "- Additional simple filters tested: market not weak, RS not deeply negative, no BEAR regime, volume/panic filters, close/body quality, premomentum, time/top-N guards.",
        "",
        "## Split",
        f"- FIT: {pd.Timestamp(fit_days[0]).date()}..{pd.Timestamp(fit_days[-1]).date()} ({len(fit_days)} sessions)",
        f"- VAL: {pd.Timestamp(val_days[0]).date()}..{pd.Timestamp(val_days[-1]).date()} ({len(val_days)} sessions)",
        f"- TRAIN: {pd.Timestamp(train_days[0]).date()}..{pd.Timestamp(train_days[-1]).date()} ({len(train_days)} sessions)",
        f"- TEST: {pd.Timestamp(test_days[0]).date()}..{pd.Timestamp(test_days[-1]).date()} ({len(test_days)} sessions)",
        "",
        "## Entry Mode Survival",
    ]
    for mode, n in summary["entry_mode_rows"].items():
        report.append(f"- `{mode}`: {n} candidate entries")
    report += [
        "",
        "## Baseline",
        f"- Config: `{config_text(baseline_cfg)}`",
        f"- TRAIN: {metric_line(baseline_train)}",
        f"- TEST: {metric_line(baseline_test)}",
        "",
        "## Best Overall Search Row",
        f"- Config: `{best_overall['config_text'] if best_overall else 'none'}`",
        f"- FIT: {metric_line(best_overall['fit']) if best_overall else 'none'}",
        f"- VAL: {metric_line(best_overall['val']) if best_overall else 'none'}",
        f"- TRAIN: {metric_line(best_overall.get('train')) if best_overall else 'none'}",
        f"- TEST: {metric_line(best_overall.get('test')) if best_overall else 'none'}",
        f"- Status: {best_overall['status'] if best_overall else 'none'}",
        "",
        "## Best Confirmed-Entry Row",
        f"- Config: `{best_confirmed['config_text'] if best_confirmed else 'none'}`",
        f"- FIT: {metric_line(best_confirmed['fit']) if best_confirmed else 'none'}",
        f"- VAL: {metric_line(best_confirmed['val']) if best_confirmed else 'none'}",
        f"- TRAIN: {metric_line(best_confirmed.get('train')) if best_confirmed else 'none'}",
        f"- TEST: {metric_line(best_confirmed.get('test')) if best_confirmed else 'none'}",
        f"- Status: {best_confirmed['status'] if best_confirmed else 'none'}",
        "",
        "## Passing Candidates",
    ]
    if passing:
        for i, p in enumerate(passing, 1):
            report += [
                f"### Candidate {i:03d}",
                f"- Config: `{config_text(p['config'])}`",
                f"- TRAIN: {metric_line(p['train'])}",
                f"- TEST: {metric_line(p['test'])}",
                f"- Path: `{p.get('candidate_path', '')}`",
            ]
    else:
        report.append("- None passed TRAIN PF 1.30-1.70 + TEST PF > 1.40 + dominance checks.")
    report += [
        "",
        "## Recommendation",
        "NO APPROVAL CANDIDATE" if not passing else "APPROVAL REQUIRED - review candidate JSON before any promotion.",
        "",
        "DO NOT MOVE TO FINAL CONFIG UNTIL USER APPROVES",
    ]
    (outdir / "CONFIRMED_REVERSAL_REDESIGN.md").write_text("\n".join(report), encoding="utf-8")

    print(f"[confirmed] baseline TRAIN {metric_line(baseline_train)} | TEST {metric_line(baseline_test)}")
    print(f"[confirmed] entry survival: {summary['entry_mode_rows']}")
    if best_confirmed:
        print(f"[confirmed] best confirmed: {best_confirmed['config_text']} | TRAIN {metric_line(best_confirmed.get('train'))} | TEST {metric_line(best_confirmed.get('test'))} | {best_confirmed['status']}")
    print(f"[confirmed] passing candidates={len(passing)}")
    print(f"[confirmed] wrote {outdir / 'CONFIRMED_REVERSAL_REDESIGN.md'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
