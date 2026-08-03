"""Causal ORB break -> retest/hold -> second-breakout research.

This is a materially different event definition from the rejected ordinary
ORB. It writes only beneath outputs/orb_retest_research.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

import pullback_refinement as common
import research as r


OUT = r.OUT / "orb_retest_research"
RNG = np.random.default_rng(20260730)
CANONICAL_QUALITY = r.OUT / "data_quality.json"
REGIME_REFERENCE = r.OUT / "candidate_features.parquet"


@dataclass(frozen=True)
class SignalConfig:
    name: str
    variant: str
    initial_end_minute: int
    regime: str
    min_traded_value: float
    atr_low: float
    atr_high: float
    initial_extension_max: float
    initial_relative_volume_min: float
    retest_bars_max: int
    retest_depth_min: float
    retest_depth_max: float
    retest_close_buffer_min: float
    confirmation_bars_max: int
    confirmation_extension_max: float
    max_avwap_extension: float
    max_range_atr: float
    max_upper_wick: float
    adx_low: float
    adx_high: float
    rsi_low: float
    rsi_high: float
    relative_volume_min: float
    score_min: int
    top_n_per_slot: int


ANCHOR_EXITS = [
    r.ExitConfig(target, stop, hold)
    for target, stop, hold in (
        (.25, .35, 15), (.25, .50, 20), (.35, .40, 20),
        (.35, .55, 30), (.45, .45, 20), (.45, .60, 30),
        (.45, .75, 40), (.55, .55, 30), (.55, .70, 40),
        (.65, .60, 30), (.65, .75, 40), (.75, .75, 40),
        (.75, .90, 50), (.85, .90, 60),
    )
]

FULL_EXITS = [
    r.ExitConfig(target, stop, hold)
    for target in (.25, .35, .45, .55, .65, .75, .85)
    for stop in (.35, .45, .55, .65, .75, .85, .95)
    for hold in (15, 20, 30, 40, 50, 60)
]


def _sequence_candidates(d: pd.DataFrame, ticker: str, wanted: set[pd.Timestamp]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    broad_base = (
        d["session"].isin(wanted)
        & d["valid"]
        & d["close"].ge(50.0)
        & d["traded_value"].ge(500_000.0)
        & d["atr_pct"].between(.10, 1.20)
        & d["close"].ge(d["avwap"])
        & d["ema9"].gt(d["ema20"])
        & d["ema20_slope3"].gt(0)
        & d["avwap_ext"].between(-.02, 1.20)
        & d["range_atr"].le(3.0)
        & d["upper_wick_frac"].le(.65)
    )
    for variant, cutoff in (("or15", 570), ("or20", 575), ("or30", 585)):
        level_series = d[variant]
        extension = (d["close"] / level_series - 1.0) * 100.0
        initial_mask = (
            broad_base
            & d["minute"].between(cutoff + 5, 660)
            & extension.between(0.0, 1.0)
        )
        for initial_idx in d.index[initial_mask]:
            initial = d.loc[initial_idx]
            level = float(initial[variant])
            session = initial["session"]
            later = d[
                (d.index > initial_idx)
                & d["session"].eq(session)
                & d["valid"]
            ].head(8)
            if later.empty:
                continue
            for retest_pos, (retest_idx, retest) in enumerate(later.head(4).iterrows(), 1):
                retest_depth = (float(retest["low"]) / level - 1.0) * 100.0
                retest_close_buffer = (
                    float(retest["close"]) / level - 1.0
                ) * 100.0
                if not (-.50 <= retest_depth <= .35 and retest_close_buffer >= -.05):
                    continue
                after_retest = d[
                    (d.index > retest_idx)
                    & d["session"].eq(session)
                    & d["valid"]
                ].head(3)
                for confirm_pos, (confirm_idx, confirm) in enumerate(after_retest.iterrows(), 1):
                    confirm_extension = (
                        float(confirm["close"]) / float(retest["high"]) - 1.0
                    ) * 100.0
                    if (
                        float(confirm["close"]) > float(retest["high"])
                        and float(confirm["close"]) > level
                        and float(confirm["close_loc"]) >= .50
                        and int(confirm["minute"]) <= 840
                    ):
                        rec = {key: confirm.get(key, np.nan) for key in r.KEEP}
                        rec.update({
                            "ticker": ticker,
                            "strategy": "orb",
                            "variant": f"retest_{variant}",
                            "break_level": level,
                            "initial_time": initial["date"],
                            "initial_minute": int(initial["minute"]),
                            "initial_extension": float(extension.loc[initial_idx]),
                            "initial_rel_volume": float(initial["rel_volume"]),
                            "initial_close_loc": float(initial["close_loc"]),
                            "retest_time": retest["date"],
                            "retest_bars": retest_pos,
                            "retest_depth": retest_depth,
                            "retest_close_buffer": retest_close_buffer,
                            "retest_close_loc": float(retest["close_loc"]),
                            "confirmation_bars": confirm_pos,
                            "confirmation_extension": confirm_extension,
                            "sequence_id": (
                                f"{ticker}|{initial['date']}|{retest['date']}|"
                                f"{confirm['date']}|{variant}"
                            ),
                        })
                        rows.append(rec)
                        break
    if not rows:
        return pd.DataFrame()
    out = pd.DataFrame(rows)
    return out.sort_values(
        ["ticker", "date", "initial_time", "retest_time"]
    ).drop_duplicates(["ticker", "date", "variant"], keep="first")


def study_sessions() -> list[pd.Timestamp]:
    """Reuse the original research freeze instead of a moving live-data tail."""
    quality = json.loads(CANONICAL_QUALITY.read_text(encoding="utf-8"))
    sessions = [pd.Timestamp(value).normalize() for value in quality["sessions"]]
    if len(sessions) != 60:
        raise RuntimeError(f"expected 60 canonical sessions, found {len(sessions)}")
    return sessions


def attach_market_regime(candidates: pd.DataFrame) -> pd.DataFrame:
    """Attach the same point-in-time breadth/NIFTY regime used by the base study."""
    if "market_regime" in candidates.columns:
        return candidates
    reference = pd.read_parquet(
        REGIME_REFERENCE,
        columns=["date", "breadth", "nifty_up", "market_regime"],
    ).drop_duplicates("date")
    out = candidates.merge(reference, on="date", how="left", validate="many_to_one")
    missing = int(out["market_regime"].isna().sum())
    if missing:
        raise RuntimeError(
            f"market regime unavailable for {missing} candidate rows; "
            "refusing to impute a causal filter"
        )
    return out


def build_candidates(rebuild: bool = False) -> pd.DataFrame:
    cache = OUT / "sequence_candidates.parquet"
    metadata_path = OUT / "candidate_build.json"
    sessions = study_sessions()
    wanted = set(sessions)
    cached = pd.DataFrame()
    scanned: set[pd.Timestamp] = set()
    if cache.exists() and metadata_path.exists() and not rebuild:
        cached = pd.read_parquet(cache)
        metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
        scanned = {
            pd.Timestamp(value).normalize()
            for value in metadata.get("sessions", [])
        }
        cached = cached[cached["session"].isin(wanted)].copy()
    scan_wanted = wanted - scanned
    if not scan_wanted:
        out = attach_market_regime(cached).reset_index(drop=True)
        out.to_parquet(cache, index=False)
        return out

    five, _ = r.symbol_files()
    frames: list[pd.DataFrame] = []
    excluded = 0
    for n, (ticker, path) in enumerate(five.items(), 1):
        try:
            raw = r._read(path, r.FIVE_COLS)
            raw["date"] = r._dt(raw["date"])
            coverage = int(
                raw["date"].dt.normalize().isin(wanted)
                .groupby(raw["date"].dt.normalize()).any().sum()
            )
            if coverage < 48:
                excluded += 1
                continue
            raw = raw[
                raw["date"] >= min(scan_wanted) - pd.Timedelta(days=45)
            ]
            d = r.add_features(raw)
            seq = _sequence_candidates(d, ticker, scan_wanted)
            if not seq.empty:
                frames.append(seq)
        except Exception:
            excluded += 1
        if n % 100 == 0:
            print(f"[sequence-build] {n}/{len(five)} parts={len(frames)}")
    pieces = [value for value in (cached, *frames) if not value.empty]
    out = pd.concat(pieces, ignore_index=True) if pieces else pd.DataFrame()
    if not out.empty:
        out = out.sort_values(
            ["ticker", "date", "initial_time", "retest_time"]
        ).drop_duplicates(["ticker", "date", "variant"], keep="first")
        out["signal_id"] = out["sequence_id"]
        out = attach_market_regime(out).reset_index(drop=True)
    out.to_parquet(cache, index=False)
    metadata_path.write_text(
        json.dumps({
            "tickers_scanned": len(five),
            "excluded": excluded,
            "sequence_candidates": len(out),
            "sessions": [str(value.date()) for value in sessions],
        }, indent=2),
        encoding="utf-8",
    )
    return out


def configs(count: int = 2500) -> list[SignalConfig]:
    out: list[SignalConfig] = []
    for i in range(count):
        atr_low = float(RNG.choice((.15, .20, .25, .30)))
        atr_high = float(RNG.choice((.55, .65, .75, .90, 1.05)))
        if atr_high <= atr_low + .20:
            atr_high = min(1.05, atr_low + .40)
        adx_low = float(RNG.choice((10, 12, 14, 16, 18, 20)))
        adx_high = float(RNG.choice((28, 32, 36, 40, 44, 48)))
        if adx_high <= adx_low + 8:
            adx_high = adx_low + 12
        rsi_low = float(RNG.choice((46, 48, 50, 52, 54)))
        rsi_high = float(RNG.choice((66, 68, 70, 72, 74, 78)))
        if rsi_high <= rsi_low + 10:
            rsi_high = rsi_low + 14
        depth_min = float(RNG.choice((-.50, -.35, -.20, -.10, 0.0)))
        depth_max = float(RNG.choice((.05, .10, .15, .25, .35)))
        out.append(SignalConfig(
            name=f"orb_retest_{i:04d}",
            variant=str(RNG.choice(("retest_or15", "retest_or20", "retest_or30"))),
            initial_end_minute=int(RNG.choice((615, 630, 645, 660))),
            regime=str(RNG.choice(("all", "not_bearish", "bullish"))),
            min_traded_value=float(RNG.choice((1_000_000, 2_500_000, 5_000_000, 10_000_000))),
            atr_low=atr_low,
            atr_high=atr_high,
            initial_extension_max=float(RNG.choice((.20, .30, .40, .55, .75))),
            initial_relative_volume_min=float(RNG.choice((.50, .70, .90, 1.10, 1.30))),
            retest_bars_max=int(RNG.choice((1, 2, 3, 4))),
            retest_depth_min=depth_min,
            retest_depth_max=depth_max,
            retest_close_buffer_min=float(RNG.choice((-.05, 0.0, .03, .06, .10))),
            confirmation_bars_max=int(RNG.choice((1, 2, 3))),
            confirmation_extension_max=float(RNG.choice((.10, .20, .30, .45, .60))),
            max_avwap_extension=float(RNG.choice((.30, .45, .60, .80, 1.00))),
            max_range_atr=float(RNG.choice((.80, 1.10, 1.40, 1.80, 2.20))),
            max_upper_wick=float(RNG.choice((.10, .20, .30, .40, .50))),
            adx_low=adx_low,
            adx_high=adx_high,
            rsi_low=rsi_low,
            rsi_high=rsi_high,
            relative_volume_min=float(RNG.choice((.50, .70, .90, 1.10, 1.30, 1.50))),
            score_min=int(RNG.choice((4, 5, 6, 7))),
            top_n_per_slot=int(RNG.choice((0, 5, 10, 20))),
        ))
    return out


def selected_indices(entries: pd.DataFrame, cfg: SignalConfig) -> np.ndarray:
    x = entries
    score = (
        x["adx"].between(cfg.adx_low, cfg.adx_high).astype(np.int8)
        + x["adx_inc2"].astype(np.int8)
        + x["rsi"].between(cfg.rsi_low, cfg.rsi_high).astype(np.int8)
        + x["rsi_inc2"].astype(np.int8)
        + (
            (x["stoch_k"] > x["stoch_d"])
            & x["stoch_k"].between(25, 90)
        ).astype(np.int8)
        + x["rel_volume"].ge(cfg.relative_volume_min).astype(np.int8)
        + x["obv_up5"].astype(np.int8)
        + x["close_loc"].ge(.60).astype(np.int8)
    )
    mask = (
        x["variant"].eq(cfg.variant)
        & x["initial_minute"].le(cfg.initial_end_minute)
        & x["traded_value"].ge(cfg.min_traded_value)
        & x["atr_pct"].between(cfg.atr_low, cfg.atr_high)
        & x["initial_extension"].le(cfg.initial_extension_max)
        & x["initial_rel_volume"].ge(cfg.initial_relative_volume_min)
        & x["retest_bars"].le(cfg.retest_bars_max)
        & x["retest_depth"].between(cfg.retest_depth_min, cfg.retest_depth_max)
        & x["retest_close_buffer"].ge(cfg.retest_close_buffer_min)
        & x["confirmation_bars"].le(cfg.confirmation_bars_max)
        & x["confirmation_extension"].between(0.0, cfg.confirmation_extension_max)
        & x["avwap_ext"].between(-.02, cfg.max_avwap_extension)
        & x["range_atr"].le(cfg.max_range_atr)
        & x["upper_wick_frac"].le(cfg.max_upper_wick)
        & score.ge(cfg.score_min)
    )
    if cfg.regime == "not_bearish":
        mask &= x["market_regime"].ne("bearish")
    elif cfg.regime == "bullish":
        mask &= x["market_regime"].eq("bullish")
    chosen = x.loc[mask].copy()
    if chosen.empty:
        return np.array([], dtype=int)
    chosen["_rank"] = (
        score.loc[chosen.index].astype(float)
        + chosen["rel_volume"].clip(upper=3.0).fillna(0) * .5
        + chosen["retest_close_buffer"].clip(lower=0, upper=.3).fillna(0)
        - chosen["confirmation_extension"].clip(lower=0).fillna(0)
        - chosen["range_atr"].clip(lower=0).fillna(0) * .15
    )
    chosen = chosen.sort_values(["ticker", "session", "date"])
    chosen = chosen.drop_duplicates(["ticker", "session"], keep="first")
    if cfg.top_n_per_slot > 0:
        chosen = chosen.sort_values(
            ["date", "_rank", "traded_value", "ticker"],
            ascending=[True, False, False, True],
        ).groupby("date", sort=False, as_index=False).head(cfg.top_n_per_slot)
    return chosen.index.to_numpy(dtype=int)


def main() -> int:
    OUT.mkdir(parents=True, exist_ok=True)
    candidates = build_candidates()
    if candidates.empty:
        raise SystemExit("no ORB retest candidates")
    entry_cache = OUT / "valid_entries.parquet"
    rebuild_entries = True
    if entry_cache.exists():
        cached_columns = pd.read_parquet(entry_cache).columns
        rebuild_entries = "market_regime" not in cached_columns
    _, one = r.symbol_files()
    original_out = r.OUT
    try:
        r.OUT = OUT
        entries, paths, rejects = r.resolve_entries(
            candidates, one, rebuild=rebuild_entries
        )
    finally:
        r.OUT = original_out
    print(
        f"[orb-retest] candidates={len(candidates)} entries={len(entries)} "
        f"rejects={sum(rejects.values())}"
    )
    sessions = study_sessions()
    dev_a, dev_b = set(sessions[:20]), set(sessions[20:40])
    validation, test = set(sessions[40:50]), set(sessions[50:60])
    all_idx = entries.index.to_numpy(dtype=int)
    anchor_pnl = common.pnl_arrays(entries, paths, all_idx, ANCHOR_EXITS)
    generated = configs()
    index_cache: dict[str, np.ndarray] = {}
    stage1: list[dict[str, Any]] = []
    for n, cfg in enumerate(generated, 1):
        idx = selected_indices(entries, cfg)
        index_cache[cfg.name] = idx
        a_idx = common.fold_indices(entries, idx, dev_a)
        b_idx = common.fold_indices(entries, idx, dev_b)
        if len(a_idx) < 12 or len(b_idx) < 12:
            continue
        best: dict[str, Any] | None = None
        for ex in ANCHOR_EXITS:
            a = common.fast_metrics(anchor_pnl[ex.key], a_idx)
            b = common.fast_metrics(anchor_pnl[ex.key], b_idx)
            score = min(a["pf"], b["pf"]) - .15 * abs(
                np.log(max(a["pf"], .01) / max(b["pf"], .01))
            )
            row = {
                "config": cfg.name, **asdict(ex),
                "dev_a_trades": a["trades"], "dev_a_pf": a["pf"],
                "dev_a_expectancy": a["expectancy"], "dev_a_net": a["net"],
                "dev_b_trades": b["trades"], "dev_b_pf": b["pf"],
                "dev_b_expectancy": b["expectancy"], "dev_b_net": b["net"],
                "development_score": score,
            }
            if best is None or score > best["development_score"]:
                best = row
        if best is not None:
            stage1.append(best)
        if n % 250 == 0:
            print(f"[signal-search] {n}/{len(generated)} retained={len(stage1)}")
    if not stage1:
        raise SystemExit("no configurations met minimum development counts")
    stage1_df = pd.DataFrame(stage1).sort_values(
        ["development_score", "dev_a_trades", "dev_b_trades"],
        ascending=False,
    )
    stage1_df.to_csv(OUT / "development_signal_search.csv", index=False)

    full_pnl = common.pnl_arrays(entries, paths, all_idx, FULL_EXITS)
    config_map = {cfg.name: cfg for cfg in generated}
    stage2: list[dict[str, Any]] = []
    for name in stage1_df.head(100)["config"]:
        idx = index_cache[name]
        a_idx = common.fold_indices(entries, idx, dev_a)
        b_idx = common.fold_indices(entries, idx, dev_b)
        for ex in FULL_EXITS:
            a = common.fast_metrics(full_pnl[ex.key], a_idx)
            b = common.fast_metrics(full_pnl[ex.key], b_idx)
            if min(a["trades"], b["trades"]) < 12:
                continue
            score = min(a["pf"], b["pf"]) - .18 * abs(
                np.log(max(a["pf"], .01) / max(b["pf"], .01))
            )
            stage2.append({
                "config": name, **asdict(ex),
                "dev_a_trades": a["trades"], "dev_a_pf": a["pf"],
                "dev_a_expectancy": a["expectancy"], "dev_a_net": a["net"],
                "dev_b_trades": b["trades"], "dev_b_pf": b["pf"],
                "dev_b_expectancy": b["expectancy"], "dev_b_net": b["net"],
                "development_score": score,
            })
    stage2_df = pd.DataFrame(stage2).sort_values(
        ["development_score", "dev_a_trades", "dev_b_trades"],
        ascending=False,
    )
    stage2_df.to_csv(OUT / "development_exit_search.csv", index=False)

    validation_rows: list[dict[str, Any]] = []
    for _, row in stage2_df.head(250).iterrows():
        idx = index_cache[str(row["config"])]
        v_idx = common.fold_indices(entries, idx, validation)
        ex = r.ExitConfig(
            float(row["target_pct"]), float(row["stop_pct"]), int(row["hold_min"])
        )
        vm = common.fast_metrics(full_pnl[ex.key], v_idx)
        validation_rows.append({
            **row.to_dict(),
            "validation_trades": vm["trades"],
            "validation_pf": vm["pf"],
            "validation_expectancy": vm["expectancy"],
            "validation_net": vm["net"],
            "freeze_score": min(
                float(row["dev_a_pf"]), float(row["dev_b_pf"]), vm["pf"]
            ),
        })
    validation_df = pd.DataFrame(validation_rows).sort_values(
        ["freeze_score", "validation_expectancy", "validation_trades"],
        ascending=False,
    )
    validation_df.to_csv(OUT / "validation_freeze.csv", index=False)
    eligible = validation_df[
        (validation_df["dev_a_pf"] > 1.05)
        & (validation_df["dev_b_pf"] > 1.05)
        & (validation_df["validation_pf"] > 1.05)
        & (validation_df["dev_a_expectancy"] > 0)
        & (validation_df["dev_b_expectancy"] > 0)
        & (validation_df["validation_expectancy"] > 0)
        & (validation_df["validation_trades"] >= 6)
    ]
    pool = eligible if not eligible.empty else validation_df
    frozen = pool.iloc[0]
    cfg = config_map[str(frozen["config"])]
    ex = r.ExitConfig(
        float(frozen["target_pct"]), float(frozen["stop_pct"]), int(frozen["hold_min"])
    )
    idx = index_cache[cfg.name]
    test_idx = common.fold_indices(entries, idx, test)
    trades = r.simulate(entries, paths, test_idx, ex)
    test_metrics = r.metrics(trades)
    stress = r.metrics(r.simulate(entries, paths, test_idx, ex, 1.5))
    trades.to_csv(OUT / "frozen_test_trades.csv", index=False)
    payload = {
        "selection_used_test": False,
        "test_is_confirmation_not_pristine": True,
        "sequence_candidates": len(candidates),
        "valid_entries": len(entries),
        "entry_reject_counts": rejects,
        "searched_signal_configs": len(generated),
        "development_exit_combinations": len(stage2_df),
        "eligible_three_fold_candidates": len(eligible),
        "signal_config": asdict(cfg),
        "exit_config": asdict(ex),
        "dev_a": {
            "trades": int(frozen["dev_a_trades"]),
            "profit_factor": float(frozen["dev_a_pf"]),
            "expectancy": float(frozen["dev_a_expectancy"]),
            "net_profit": float(frozen["dev_a_net"]),
        },
        "dev_b": {
            "trades": int(frozen["dev_b_trades"]),
            "profit_factor": float(frozen["dev_b_pf"]),
            "expectancy": float(frozen["dev_b_expectancy"]),
            "net_profit": float(frozen["dev_b_net"]),
        },
        "validation": {
            "trades": int(frozen["validation_trades"]),
            "profit_factor": float(frozen["validation_pf"]),
            "expectancy": float(frozen["validation_expectancy"]),
            "net_profit": float(frozen["validation_net"]),
        },
        "test": test_metrics,
        "test_150pct_slippage": stress,
    }
    payload["classification"] = (
        "PROFITABLE_BUT_INSUFFICIENT_SAMPLE"
        if len(eligible) > 0
        and test_metrics["profit_factor"] > 1.0
        and test_metrics["expectancy"] > 0
        else "REJECT"
    )
    (OUT / "result.json").write_text(
        json.dumps(payload, indent=2, default=str), encoding="utf-8"
    )
    print(json.dumps(payload, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
