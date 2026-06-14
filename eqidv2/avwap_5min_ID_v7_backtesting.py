"""
AVWAP ID 5-min v7 backtester.

v7 makes the accepted v6 research flow permanent:
  1. Run the v5 full native all-setups scanner from raw data by default.
  2. Re-resolve the v5 selected trades on 1-minute bars with setup-specific
     exits from v6.
  3. Re-resolve unused raw native candidates for the PF>2 setup family.
  4. Apply the accepted PF-band expansion miner internally.

Default output:
  C:\\TradingData\\eqidv2\\outputs_ID_v7_5min
"""

from __future__ import annotations

import argparse
import math
import os
import time
from pathlib import Path

import numpy as np
import pandas as pd

import avwap_5min_ID_v5_backtesting as v5
import avwap_5min_ID_v6_backtesting as v6
import walkforward_gate


OUT_ROOT = Path(r"C:\TradingData\eqidv2\outputs_ID_v7_5min")
_GATE_NOTIONAL_RS = 100_000.0

# P1-5: gate only evaluates the TRADED universe — SHORT setups + any explicitly
# exempted LONG setups (mirrors EQIDV2_SIGNAL_DISCOVERY_V7_SHORT_FOCUS_EXEMPT_SETUPS
# from the live scanner bat).  This ensures validated == traded.
_GATE_SHORT_FOCUS_EXEMPT_LONGS: frozenset[str] = frozenset(
    s.strip()
    for s in os.getenv(
        "EQIDV2_SIGNAL_DISCOVERY_V7_SHORT_FOCUS_EXEMPT_SETUPS",
        "A_MOD_BREAK_C1_HIGH",
    ).split(",")
    if s.strip()
)
PNL_COL = "v6_net_pnl_rs"
EXCLUDED_SETUPS = {
    "B_HUGE_PULLBACK_HOLD_BREAK",
    "S_LIQUIDITY_SWEEP_REVERSAL",
}


def _pf(values: pd.Series) -> float:
    s = pd.to_numeric(values, errors="coerce").fillna(0.0)
    gains = float(s[s > 0].sum())
    losses = float(-s[s < 0].sum())
    if losses <= 0:
        return math.inf if gains > 0 else 0.0
    return gains / losses


def _quick_summary(trades: pd.DataFrame) -> dict:
    pnl = pd.to_numeric(trades[PNL_COL], errors="coerce").fillna(0.0)
    return {
        "trades": int(len(trades)),
        "pf": float(_pf(pnl)),
        "pnl": float(pnl.sum()),
        "win": float((pnl > 0).mean() * 100.0) if len(trades) else 0.0,
    }


def _trade_key(df: pd.DataFrame) -> pd.Series:
    cols = [
        c for c in [
            "ticker",
            "side",
            "setup",
            "entry_time_v6",
            "entry_price_v6",
        ] if c in df.columns
    ]
    if not cols:
        return pd.Series(dtype=str)
    tmp = df[cols].copy()
    for c in cols:
        tmp[c] = tmp[c].astype(str)
    return tmp.agg("|".join, axis=1)


def _resolve_trades(trades: pd.DataFrame, cost_bps: float, label: str) -> pd.DataFrame:
    normalised = v6._normalise_trades(trades)
    normalised = normalised.loc[~normalised["setup"].astype(str).isin(EXCLUDED_SETUPS)].copy()
    missing = sorted(set(normalised["setup"].astype(str)) - set(v6.SETUP_EXIT_RULES))
    if missing:
        raise SystemExit(f"[v7] missing setup exit rules for {label}: {missing}")

    print(f"[v7] resolving {len(normalised):,} {label} trades on 1-min exits")
    rows = []
    misses = 0
    t0 = time.time()
    for i, (_, row) in enumerate(normalised.iterrows(), 1):
        rec = v6._resolve_trade(row, cost_bps=cost_bps)
        if rec is None:
            misses += 1
        else:
            rows.append(rec)
        if i % 500 == 0 or i == len(normalised):
            print(f"  [v7 {label} {i:5d}/{len(normalised)}] resolved={len(rows)} misses={misses} elapsed={time.time()-t0:.1f}s")

    if not rows:
        raise SystemExit(f"[v7] no {label} trades resolved")
    out = pd.DataFrame(rows)
    out["v7_resolution_source"] = label
    return out


def _prep_features(cur: pd.DataFrame, extra: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, list[str]]:
    cur = cur.copy()
    extra = extra.copy()
    numeric = [
        PNL_COL,
        "quality_score",
        "rs_pct",
        "market_ret_pct",
        "vol_ratio",
        "atr_pct",
        "close_loc",
        "body_pct",
        "vwap_dist_atr",
        "day_value_so_far_rs",
        "v6_bars_held",
        "bars_held",
        "entry_price_v6",
    ]
    for df in (cur, extra):
        for c in numeric:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce")
        if "_signal_hour" in df.columns:
            df["_signal_hour"] = pd.to_numeric(df["_signal_hour"], errors="coerce")
        else:
            src = "entry_time_v6" if "entry_time_v6" in df.columns else "entry_ts"
            df["_signal_hour"] = pd.to_datetime(df[src], errors="coerce").dt.hour

    features = [
        c for c in [
            "quality_score",
            "rs_pct",
            "market_ret_pct",
            "vol_ratio",
            "atr_pct",
            "close_loc",
            "body_pct",
            "vwap_dist_atr",
            "day_value_so_far_rs",
            "v6_bars_held",
            "bars_held",
            "entry_price_v6",
            "_signal_hour",
        ] if c in extra.columns
    ]
    return cur, extra, features


def _masks_for(rem: pd.DataFrame, features: list[str]) -> list[tuple[str, np.ndarray]]:
    quantiles = [
        0.02, 0.05, 0.10, 0.15, 0.20, 0.25, 0.30, 0.35, 0.40, 0.45,
        0.50, 0.55, 0.60, 0.65, 0.70, 0.75, 0.80, 0.85, 0.90, 0.95, 0.98,
    ]
    masks: list[tuple[str, np.ndarray]] = []
    for feature in features:
        s = rem[feature]
        if s.notna().sum() == 0:
            continue
        values = []
        for q in quantiles:
            v = s.quantile(q)
            if pd.notna(v):
                values.append(float(v))
        for v in sorted(set(values)):
            masks.append((f"{feature} >= {v:.6g}", (s >= v).fillna(False).to_numpy()))
            masks.append((f"{feature} <= {v:.6g}", (s <= v).fillna(False).to_numpy()))

    if "_signal_hour" in rem.columns:
        for hour in sorted(rem["_signal_hour"].dropna().unique()):
            masks.append((f"_signal_hour == {int(hour)}", (rem["_signal_hour"] == hour).fillna(False).to_numpy()))
    if "regime" in rem.columns:
        for regime in rem["regime"].dropna().unique():
            masks.append((f"regime == {regime}", (rem["regime"] == regime).fillna(False).to_numpy()))
    return masks


def _mine_positive_pool(cur: pd.DataFrame, extra: pd.DataFrame, target_setups: list[str]) -> pd.DataFrame:
    cur, extra, features = _prep_features(cur, extra)
    accepted_parts = []
    final_parts = [cur.copy()]

    for setup in target_setups:
        base = cur[cur["setup"] == setup].copy()
        rem = extra[extra["setup"] == setup].copy()
        if rem.empty:
            continue
        base_values = pd.to_numeric(base[PNL_COL], errors="coerce").fillna(0.0)
        min_n = 1 if len(base) < 20 else 3 if len(base) < 100 else 5
        chosen = []

        for round_no in range(1, 6):
            candidates = []
            singles = []

            def consider(desc: str, mask: np.ndarray) -> None:
                n = int(mask.sum())
                if n < min_n:
                    return
                group = rem.loc[mask]
                group_values = pd.to_numeric(group[PNL_COL], errors="coerce").fillna(0.0)
                group_pnl = float(group_values.sum())
                if group_pnl < 0.0:
                    return
                combined_values = pd.concat([base_values, group_values], ignore_index=True)
                combined_pf = _pf(combined_values)
                if combined_pf <= 2.0:
                    return
                cand = {
                    "desc": desc,
                    "mask": mask,
                    "n": n,
                    "group_pf": _pf(group_values),
                    "group_pnl": group_pnl,
                    "combined_pf": combined_pf,
                    "score": (n, combined_pf, group_pnl),
                }
                candidates.append(cand)
                singles.append(cand)

            for desc, mask in _masks_for(rem, features):
                consider(desc, mask)
            top = sorted(singles, key=lambda x: (x["n"], x["combined_pf"]), reverse=True)[:80]
            for i in range(len(top)):
                for j in range(i + 1, len(top)):
                    consider(f"{top[i]['desc']} AND {top[j]['desc']}", top[i]["mask"] & top[j]["mask"])

            if not candidates:
                break
            best = max(candidates, key=lambda x: x["score"])
            group = rem.loc[best["mask"]].copy()
            group["v7_positive_rule"] = best["desc"]
            group["v7_positive_round"] = round_no
            chosen.append(group)
            accepted_parts.append({
                "stage": "positive_pool",
                "setup": setup,
                "round": round_no,
                "rule": best["desc"],
                "added": best["n"],
                "group_pf": best["group_pf"],
                "group_pnl": best["group_pnl"],
                "setup_pf_after": best["combined_pf"],
            })
            base = pd.concat([base, group], ignore_index=True)
            base_values = pd.to_numeric(base[PNL_COL], errors="coerce").fillna(0.0)
            rem = rem.loc[~best["mask"]].copy()

        if chosen:
            final_parts.append(pd.concat(chosen, ignore_index=True))

    positive = pd.concat(final_parts, ignore_index=True, sort=False)
    positive.attrs["accepted"] = pd.DataFrame(accepted_parts)
    return positive


def _mine_strict_pool(cur: pd.DataFrame, extra: pd.DataFrame, target_setups: list[str]) -> pd.DataFrame:
    cur, extra, features = _prep_features(cur, extra)
    accepted_parts = []
    final_parts = [cur.copy()]

    for setup in target_setups:
        base = cur[cur["setup"] == setup].copy()
        rem = extra[extra["setup"] == setup].copy()
        if rem.empty:
            continue
        base_values = pd.to_numeric(base[PNL_COL], errors="coerce").fillna(0.0)
        min_n = 1 if len(base) < 20 else 3 if len(base) < 100 else 5
        chosen = []

        for round_no in range(1, 8):
            candidates = []
            singles = []

            def consider(desc: str, mask: np.ndarray) -> None:
                n = int(mask.sum())
                if n < min_n:
                    return
                group = rem.loc[mask]
                group_values = pd.to_numeric(group[PNL_COL], errors="coerce").fillna(0.0)
                group_pf = _pf(group_values)
                group_pnl = float(group_values.sum())
                if group_pf <= 2.0 or group_pnl <= 0.0:
                    return
                combined_values = pd.concat([base_values, group_values], ignore_index=True)
                combined_pf = _pf(combined_values)
                if combined_pf <= 2.0:
                    return
                cand = {
                    "desc": desc,
                    "mask": mask,
                    "n": n,
                    "group_pf": group_pf,
                    "group_pnl": group_pnl,
                    "combined_pf": combined_pf,
                    "score": (n, group_pf, group_pnl, combined_pf),
                }
                candidates.append(cand)
                singles.append(cand)

            for desc, mask in _masks_for(rem, features):
                consider(desc, mask)
            top = sorted(singles, key=lambda x: (x["n"], x["group_pf"]), reverse=True)[:100]
            for i in range(len(top)):
                for j in range(i + 1, len(top)):
                    consider(f"{top[i]['desc']} AND {top[j]['desc']}", top[i]["mask"] & top[j]["mask"])

            if not candidates:
                break
            best = max(candidates, key=lambda x: x["score"])
            group = rem.loc[best["mask"]].copy()
            group["v7_strict_rule"] = best["desc"]
            group["v7_strict_round"] = round_no
            chosen.append(group)
            accepted_parts.append({
                "stage": "strict_seed",
                "setup": setup,
                "round": round_no,
                "rule": best["desc"],
                "added": best["n"],
                "group_pf": best["group_pf"],
                "group_pnl": best["group_pnl"],
                "setup_pf_after": best["combined_pf"],
            })
            base = pd.concat([base, group], ignore_index=True)
            base_values = pd.to_numeric(base[PNL_COL], errors="coerce").fillna(0.0)
            rem = rem.loc[~best["mask"]].copy()

        if chosen:
            final_parts.append(pd.concat(chosen, ignore_index=True))

    strict = pd.concat(final_parts, ignore_index=True, sort=False)
    strict.attrs["accepted"] = pd.DataFrame(accepted_parts)
    return strict


def _plain(df: pd.DataFrame) -> pd.DataFrame:
    """Return a shallow copy without pandas attrs so concat cannot compare DataFrame attrs."""
    out = df.copy()
    out.attrs = {}
    return out


def _keyed_difference(left: pd.DataFrame, right: pd.DataFrame) -> pd.DataFrame:
    right_keys = set(_trade_key(right))
    return left.loc[~_trade_key(left).isin(right_keys)].copy()


def _apply_v7_expansion(base: pd.DataFrame, extra: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    base = base.loc[~base["setup"].astype(str).isin(EXCLUDED_SETUPS)].copy()
    extra = extra.loc[~extra["setup"].astype(str).isin(EXCLUDED_SETUPS)].copy()
    target_setups = []
    for setup, group in base.groupby("setup"):
        if _pf(group[PNL_COL]) > 2.0:
            target_setups.append(str(setup))
    print(f"[v7] PF>2 setup family={len(target_setups)}")

    extra = extra[extra["setup"].isin(target_setups)].copy()
    positive = _mine_positive_pool(base, extra, target_setups)
    strict = _mine_strict_pool(base, extra, target_setups)

    positive_extra = positive[positive.get("v7_positive_rule").notna()].copy()
    remaining = _keyed_difference(_plain(positive_extra), _plain(strict))

    rules = []
    for key, group in remaining.groupby(["setup", "v7_positive_round", "v7_positive_rule"], dropna=False):
        values = pd.to_numeric(group[PNL_COL], errors="coerce").fillna(0.0)
        rules.append({
            "key": key,
            "setup": key[0],
            "n": int(len(group)),
            "pf": _pf(values),
            "pnl": float(values.sum()),
            "df": group,
        })

    final = _plain(strict)
    selected = []
    remaining_rules = rules.copy()
    while True:
        best = None
        for rule in remaining_rules:
            test = pd.concat([_plain(final), _plain(rule["df"])], ignore_index=True, sort=False)
            summary = _quick_summary(test)
            if summary["pf"] <= 2.0:
                continue
            score = (rule["pnl"], rule["n"], summary["pf"])
            if best is None or score > best["score"]:
                best = {**rule, "score": score, "summary_after": summary}
        if best is None:
            break
        selected.append(best)
        final = pd.concat([_plain(final), _plain(best["df"])], ignore_index=True, sort=False)
        remaining_rules = [r for r in remaining_rules if r["key"] != best["key"]]

    accepted = []
    strict_acc = strict.attrs.get("accepted", pd.DataFrame())
    positive_acc = positive.attrs.get("accepted", pd.DataFrame())
    if len(strict_acc):
        accepted.append(strict_acc)
    if selected:
        accepted.append(pd.DataFrame([
            {
                "stage": "extra_full_pf_band",
                "setup": r["setup"],
                "round": r["key"][1],
                "rule": r["key"][2],
                "added": r["n"],
                "group_pf": r["pf"],
                "group_pnl": r["pnl"],
                "full_pf_after": r["summary_after"]["pf"],
            }
            for r in selected
        ]))
    if len(positive_acc):
        positive_acc = positive_acc.copy()
        positive_acc["stage"] = "positive_pool_candidate"
        accepted.append(positive_acc)

    accepted_df = pd.concat(accepted, ignore_index=True, sort=False) if accepted else pd.DataFrame()
    return final, accepted_df


def _gate_filter_accepted(
    trades: pd.DataFrame,
    accepted: pd.DataFrame,
    out_dir: Path,
) -> pd.DataFrame:
    """Run the walk-forward gate; return accepted filtered to PROMOTE setups only."""
    required = {"entry_time_v6", "entry_price_v6", "v6_exit_price", "setup", "side"}
    if not required.issubset(trades.columns) or trades.empty:
        print("[v7 gate] skipping: required columns missing or empty trades")
        return accepted

    gate_df = pd.DataFrame({
        "date": pd.to_datetime(trades["entry_time_v6"], errors="coerce"),
        "setup": trades["setup"].astype(str),
        "side": trades["side"].astype(str),
        "entry_price": pd.to_numeric(trades["entry_price_v6"], errors="coerce"),
        "exit_price": pd.to_numeric(trades["v6_exit_price"], errors="coerce"),
    })
    gate_df["qty"] = (
        (_GATE_NOTIONAL_RS / gate_df["entry_price"]).round().clip(lower=1)
    )
    gate_df = gate_df.dropna(subset=["entry_price", "exit_price"]).reset_index(drop=True)

    if gate_df.empty:
        print("[v7 gate] skipping: no valid rows after dropna")
        return accepted

    # P1-5: restrict gate to the traded universe (SHORT + exempt LONGs).
    traded_mask = (gate_df["side"] == "SHORT") | (
        (gate_df["side"] == "LONG") & gate_df["setup"].isin(_GATE_SHORT_FOCUS_EXEMPT_LONGS)
    )
    gate_df = gate_df[traded_mask].reset_index(drop=True)
    if gate_df.empty:
        print("[v7 gate] skipping: no traded-subset rows after SHORT_FOCUS filter")
        return accepted
    excluded_sides = (~traded_mask).sum()
    if excluded_sides:
        print(f"[v7 gate] P1-5 side filter: excluded {excluded_sides} non-traded rows")

    try:
        report = walkforward_gate.run_gate(gate_df)
    except Exception as exc:
        print(f"[v7 gate] gate error {exc!r}; writing unfiltered accepted_rules.csv")
        return accepted

    report.to_csv(out_dir / "gate_decision_report.csv", index=False)
    promoted = set(report.loc[report["decision"] == "PROMOTE", "setup"])
    n_before = len(accepted)
    if not accepted.empty and "setup" in accepted.columns:
        filtered = accepted[accepted["setup"].isin(promoted)].reset_index(drop=True)
    else:
        filtered = accepted
    n_after = len(filtered)
    promote_count = len(promoted)
    all_count = report["setup"].nunique()
    print(
        f"[v7 gate] {promote_count}/{all_count} setups PROMOTE; "
        f"accepted rows {n_before} → {n_after}"
    )
    return filtered


def _write_outputs(out_dir: Path, trades: pd.DataFrame, accepted: pd.DataFrame, source: str) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    trades.to_csv(out_dir / "trades.csv", index=False)
    summary, daily, by_setup = v6._metrics(trades)
    daily.rename(columns={"_pnl": "net_pnl_rs"}).to_csv(out_dir / "daily.csv", index=False)
    by_setup.to_csv(out_dir / "by_setup.csv", index=False)
    gated_accepted = _gate_filter_accepted(trades, accepted, out_dir)
    gated_accepted.to_csv(out_dir / "accepted_rules.csv", index=False)
    pd.DataFrame(
        [{"setup": k, "sl_pct": v[0], "target_pct": v[1]} for k, v in sorted(v6.SETUP_EXIT_RULES.items())]
    ).to_csv(out_dir / "setup_exit_rules.csv", index=False)

    text = v6._summary_text(summary, by_setup, Path(source))
    text = text.replace("AVWAP ID 5-min v6 backtest", "AVWAP ID 5-min v7 backtest")
    text = text.replace(
        "v5 trades re-resolved on 1-minute bars with setup-specific SL/target exits",
        "full v5 raw scan + v6 1-minute exits + internal PF-band expansion miner",
    )
    print(text)
    (out_dir / "summary.txt").write_text(text + "\n", encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser(description="AVWAP ID 5-min v7 full scanner with permanent PF-band expansion")
    ap.add_argument("--out", type=str, default=str(OUT_ROOT))
    ap.add_argument("--workers", type=int, default=v5.v2.DEFAULT_WORKERS)
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--cost_bps", type=float, default=v6.DEFAULT_COST_BPS)
    # 2026-05-19 (temporary): daily caps disabled for v7 — emit ALL valid
    # candidates instead of top-24 by score. Restore by reverting these two
    # defaults to v5.v2.DEFAULT_MAX_TRADES_PER_DAY (24) and
    # v5.v2.DEFAULT_MAX_SAME_SIDE_PER_DAY (16). v2's globals are untouched so
    # other backtesters keep the original caps.
    ap.add_argument("--max_trades_per_day", type=int, default=999_999)
    ap.add_argument("--max_same_side_per_day", type=int, default=999_999)
    ap.add_argument("--refresh_baseline", action="store_true")
    ap.add_argument("--use_v5_cache", action="store_true", help="reuse the v7/v5_stage CSVs instead of rescanning raw data")
    args = ap.parse_args()

    out_dir = Path(args.out)
    stage_dir = out_dir / "v5_stage"

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
        raise SystemExit(f"[v7] missing v5 stage files under {stage_dir}; rerun without --use_v5_cache")

    base = _resolve_trades(pd.read_csv(v5_trades, low_memory=False), float(args.cost_bps), "base")
    raw = pd.read_csv(raw_trades, low_memory=False)
    raw_norm = v6._normalise_trades(raw)
    raw_norm = raw_norm.loc[~raw_norm["setup"].astype(str).isin(EXCLUDED_SETUPS)].copy()
    raw_norm = raw_norm.loc[~_trade_key(raw_norm).isin(set(_trade_key(base)))].copy()

    pf2_setups = [str(setup) for setup, group in base.groupby("setup") if _pf(group[PNL_COL]) > 2.0]
    raw_norm = raw_norm[raw_norm["setup"].isin(pf2_setups)].copy()
    extra = _resolve_trades(raw_norm, float(args.cost_bps), "extra_pf2_pool")
    extra.to_csv(out_dir / "extra_pf2_setup_candidates_resolved.csv", index=False)

    final, accepted = _apply_v7_expansion(base, extra)
    _write_outputs(out_dir, final, accepted, str(v5_trades))
    (out_dir / "inputs.txt").write_text(
        f"v5_trades={v5_trades}\nraw_trades={raw_trades}\nfull_scan={not args.use_v5_cache}\n",
        encoding="utf-8",
    )
    print(f"[v7] wrote {out_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
