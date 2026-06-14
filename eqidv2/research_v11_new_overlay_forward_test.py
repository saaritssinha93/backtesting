from __future__ import annotations

import math
from pathlib import Path

import numpy as np
import pandas as pd

import avwap_5min_ID_v11_backtesting as v11


OLD_TRADES = Path(
    r"C:\TradingData\eqidv2\outputs_ID_v11_all_setups_with_ab_probe_from_v10_raw_q250\trades.csv"
)
CLEAN_POOL = Path(r"C:\TradingData\eqidv2\outputs_ID_v11_cleanpool")
OUT_DIR = Path(r"C:\TradingData\eqidv2\outputs_ID_v11_new_overlay_forward_test")

LONG_SETUP = "N_HIGH_RS_EMA_BOUNCE_LONG"
SHORT_SETUP = "N_MORNING_ZERO_WICK_SHORT"

SHORT_SOURCE_SETUPS = {
    "S_BB_SQUEEZE_SHORT",
    "E_ORB_BREAKOUT_SHORT",
    "D_EMA20_REJECTION",
    "E_VWAP_BAND_FADE",
}

EXIT_RULES = (
    (0.70, 1.00),
    (0.80, 1.20),
    (0.90, 1.50),
    (1.00, 2.00),
)

TRAIN_END = pd.Timestamp("2026-01-31")
VALID_START = pd.Timestamp("2026-02-01")
VALID_END = pd.Timestamp("2026-03-31")
HOLDOUT_START = pd.Timestamp("2026-04-01")
HOLDOUT_END = pd.Timestamp("2026-05-29")
FORWARD_START = pd.Timestamp("2026-06-01")
FORWARD_END = pd.Timestamp("2026-06-10")


def _normalise_candidates(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    out["signal_time_ist"] = pd.to_datetime(out["signal_time_ist"], errors="coerce")
    out = out.dropna(subset=["signal_time_ist", "ticker", "side", "setup"])
    out["signal_minute"] = (
        out["signal_time_ist"].dt.hour * 60 + out["signal_time_ist"].dt.minute
    )
    for col in (
        "signal_open",
        "signal_high",
        "signal_low",
        "signal_close",
        "body_pct",
        "rs_pct",
        "quality_score",
        "ranker_score",
    ):
        out[col] = pd.to_numeric(out.get(col), errors="coerce")
    close = out["signal_close"].replace(0, np.nan)
    body_bottom = pd.concat([out["signal_open"], out["signal_close"]], axis=1).min(axis=1)
    out["lower_wick_price_pct"] = (body_bottom - out["signal_low"]) / close * 100.0
    return out


def _load_old_candidates() -> pd.DataFrame:
    frame = pd.read_csv(OLD_TRADES, low_memory=False)
    return _normalise_candidates(frame)


def _load_forward_candidates() -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for path in sorted(CLEAN_POOL.glob("chunk_202606/historical_all_available_raw_candidates.csv")):
        frame = pd.read_csv(path, low_memory=False)
        frames.append(frame)
    if not frames:
        return pd.DataFrame()
    out = _normalise_candidates(pd.concat(frames, ignore_index=True, sort=False))
    dates = out["signal_time_ist"].dt.tz_localize(None).dt.normalize()
    return out.loc[(dates >= FORWARD_START) & (dates <= FORWARD_END)].copy()


def _relabel(
    frame: pd.DataFrame,
    *,
    setup: str,
    side: str,
    mask: pd.Series,
    reason: str,
) -> pd.DataFrame:
    out = frame.loc[mask].copy()
    if out.empty:
        return out
    out["source_setup"] = out["setup"].astype(str)
    out["setup"] = setup
    out["side"] = side
    out["reason"] = reason
    out["candidate_family"] = "new_overlay_forward_test"
    out["selection_mode"] = "new_overlay_forward_test"
    out["candidate_id"] = (
        out["ticker"].astype(str)
        + "|"
        + side
        + "|"
        + setup
        + "|"
        + out["signal_time_ist"].astype(str)
    )
    out["_day"] = out["signal_time_ist"].dt.tz_localize(None).dt.strftime("%Y-%m-%d")
    out["_score"] = pd.to_numeric(
        out.get("ranker_score", out.get("quality_score")),
        errors="coerce",
    ).fillna(0.0)
    out = (
        out.sort_values(["ticker", "_day", "_score"], ascending=[True, True, False])
        .drop_duplicates(["ticker", "_day"], keep="first")
        .drop(columns=["_day", "_score"])
        .reset_index(drop=True)
    )
    return out


def _build_overlays(frame: pd.DataFrame) -> pd.DataFrame:
    long_mask = (
        frame["setup"].astype(str).eq("D_EMA20_BOUNCE")
        & frame["side"].astype(str).str.upper().eq("LONG")
        & (frame["body_pct"] >= 0.60)
        & (frame["rs_pct"] >= 4.0)
    )
    short_mask = (
        frame["setup"].astype(str).isin(SHORT_SOURCE_SETUPS)
        & frame["side"].astype(str).str.upper().eq("SHORT")
        & frame["signal_minute"].between(601, 690)
        & (frame["lower_wick_price_pct"] <= 0.01)
        & (frame["quality_score"] <= 100.0)
    )
    long = _relabel(
        frame,
        setup=LONG_SETUP,
        side="LONG",
        mask=long_mask,
        reason="high_relative_strength_ema20_bounce_with_directional_body",
    )
    short = _relabel(
        frame,
        setup=SHORT_SETUP,
        side="SHORT",
        mask=short_mask,
        reason="morning_bearish_signal_closes_at_low_without_lower_wick",
    )
    return pd.concat([long, short], ignore_index=True, sort=False)


def _resolve(candidates: pd.DataFrame) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for setup in (LONG_SETUP, SHORT_SETUP):
        group = candidates.loc[candidates["setup"].astype(str).eq(setup)].copy()
        for sl_pct, target_pct in EXIT_RULES:
            v11.v6.SETUP_EXIT_RULES[setup] = (sl_pct, target_pct)
            signals, raw_entries, rejects = v11._build_v7_entry_engine_signals(group)
            raw_entries.to_csv(
                OUT_DIR / f"raw_entries_{setup}_{sl_pct:.2f}_{target_pct:.2f}.csv",
                index=False,
            )
            rejects.to_csv(
                OUT_DIR / f"rejects_{setup}_{sl_pct:.2f}_{target_pct:.2f}.csv",
                index=False,
            )
            if signals.empty:
                continue
            trades = v11._resolve_v7_entry_engine_signals(
                signals,
                label=f"new_overlay_forward_{setup}_{sl_pct:.2f}_{target_pct:.2f}",
                entry_fill_model="ltp_on_signal_1m_open",
                selected_strategy_profile="none",
            )
            if trades.empty:
                continue
            trades["exit_key"] = f"{sl_pct:.2f}/{target_pct:.2f}"
            trades["probe_sl_pct"] = sl_pct
            trades["probe_target_pct"] = target_pct
            trades["date"] = pd.to_datetime(trades["trade_date"], errors="coerce")
            trades["pnl"] = pd.to_numeric(trades["v6_net_pnl_rs"], errors="coerce").fillna(0.0)
            frames.append(trades)
    return pd.concat(frames, ignore_index=True, sort=False) if frames else pd.DataFrame()


def _profit_factor(values: pd.Series) -> float:
    pnl = pd.to_numeric(values, errors="coerce").fillna(0.0)
    gains = float(pnl[pnl > 0].sum())
    losses = float(-pnl[pnl < 0].sum())
    if losses <= 0:
        return math.inf if gains > 0 else 0.0
    return gains / losses


def _metrics(frame: pd.DataFrame) -> dict[str, float | int]:
    if frame.empty:
        return {
            "trades": 0,
            "days": 0,
            "pnl": 0.0,
            "pf": 0.0,
            "win_pct": 0.0,
            "avg_trade": 0.0,
        }
    pnl = pd.to_numeric(frame["pnl"], errors="coerce").fillna(0.0)
    return {
        "trades": int(len(frame)),
        "days": int(frame["date"].dt.date.nunique()),
        "pnl": float(pnl.sum()),
        "pf": float(_profit_factor(pnl)),
        "win_pct": float((pnl > 0).mean() * 100.0),
        "avg_trade": float(pnl.mean()),
    }


def _period(frame: pd.DataFrame, name: str) -> pd.DataFrame:
    dates = frame["date"].dt.normalize()
    if name == "train":
        return frame.loc[dates <= TRAIN_END]
    if name == "valid":
        return frame.loc[(dates >= VALID_START) & (dates <= VALID_END)]
    if name == "holdout":
        return frame.loc[(dates >= HOLDOUT_START) & (dates <= HOLDOUT_END)]
    if name == "forward":
        return frame.loc[(dates >= FORWARD_START) & (dates <= FORWARD_END)]
    raise ValueError(name)


def _bootstrap(frame: pd.DataFrame, n_bootstrap: int = 10000) -> float:
    daily = frame.groupby(frame["date"].dt.normalize())["pnl"].sum().to_numpy(dtype=float)
    if len(daily) < 5:
        return 1.0
    rng = np.random.default_rng(20260612)
    sampled = daily[rng.integers(0, len(daily), size=(n_bootstrap, len(daily)))].mean(axis=1)
    return float((sampled <= 0).mean())


def _summarise(trades: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict] = []
    for (setup, exit_key), group in trades.groupby(["setup", "exit_key"], sort=True):
        row: dict[str, object] = {"setup": setup, "exit_key": exit_key}
        for period in ("train", "valid", "holdout", "forward"):
            period_frame = _period(group, period)
            for key, value in _metrics(period_frame).items():
                row[f"{period}_{key}"] = value
            row[f"{period}_bootstrap_p"] = _bootstrap(period_frame)
        rows.append(row)
    return pd.DataFrame(rows)


def _choose(summary: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict] = []
    for setup in (LONG_SETUP, SHORT_SETUP):
        pool = summary.loc[
            summary["setup"].eq(setup)
            & (summary["train_trades"] >= 20)
            & (summary["train_pf"] >= 1.30)
            & (summary["train_pnl"] > 0)
            & (summary["valid_trades"] >= 6)
            & (summary["valid_pf"] >= 1.10)
            & (summary["valid_pnl"] > 0)
        ].copy()
        if pool.empty:
            rows.append({"setup": setup, "decision": "REJECT_DEVELOPMENT"})
            continue
        pool["dev_score"] = (
            np.minimum(pool["train_pf"], 3.0) * 100.0
            + np.minimum(pool["valid_pf"], 3.0) * 120.0
            + np.log1p(pool["train_trades"] + pool["valid_trades"]) * 15.0
        )
        best = pool.sort_values(["dev_score", "train_trades"], ascending=[False, False]).iloc[0].to_dict()
        holdout_ok = (
            int(best["holdout_trades"]) >= 10
            and float(best["holdout_pf"]) >= 1.10
            and float(best["holdout_pnl"]) > 0
        )
        forward_ok = (
            int(best["forward_trades"]) >= 3
            and float(best["forward_pf"]) >= 1.10
            and float(best["forward_pnl"]) > 0
        )
        if holdout_ok and forward_ok:
            decision = "PROMISING_PROBATION"
        elif int(best["forward_trades"]) >= 3 and not forward_ok:
            decision = "REJECT_FORWARD"
        elif holdout_ok:
            decision = "WAIT_MORE_FORWARD_DATA"
        else:
            decision = "REJECT_HOLDOUT"
        best["decision"] = decision
        rows.append(best)
    return pd.DataFrame(rows)


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    old = _build_overlays(_load_old_candidates())
    forward = _build_overlays(_load_forward_candidates())
    candidates = pd.concat([old, forward], ignore_index=True, sort=False)
    candidates = candidates.drop_duplicates("candidate_id", keep="last").reset_index(drop=True)
    candidates.to_csv(OUT_DIR / "new_overlay_candidates.csv", index=False)
    candidates.groupby(["setup", "source_setup"]).size().reset_index(name="candidates").to_csv(
        OUT_DIR / "new_overlay_candidate_counts.csv", index=False
    )
    print(
        f"[candidates] old={len(old):,} forward={len(forward):,} combined={len(candidates):,}",
        flush=True,
    )

    trades = _resolve(candidates)
    trades.to_csv(OUT_DIR / "new_overlay_resolved_trades.csv", index=False)
    summary = _summarise(trades)
    summary.to_csv(OUT_DIR / "new_overlay_exit_summary.csv", index=False)
    selected = _choose(summary)
    selected.to_csv(OUT_DIR / "new_overlay_selected.csv", index=False)

    print(
        selected[
            [
                col
                for col in (
                    "setup",
                    "decision",
                    "exit_key",
                    "train_trades",
                    "train_pf",
                    "valid_trades",
                    "valid_pf",
                    "holdout_trades",
                    "holdout_pf",
                    "forward_trades",
                    "forward_pf",
                    "forward_pnl",
                )
                if col in selected.columns
            ]
        ].to_string(index=False),
        flush=True,
    )


if __name__ == "__main__":
    main()
