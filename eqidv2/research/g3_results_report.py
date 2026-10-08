"""Validate and summarize a completed six-arm G-3 research replay."""
from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import fno_v13_v10_g_2_0925_comparison as comp
import fno_v13_v10_g_2_backtest as g2
from research.g3_six_confirmation_variants import EXTENSION_DAYS, REPAIR_MINUTE, REPAIR_SHA


def _recover_provenance(folder: Path, days: list[str]) -> None:
    if (folder / "provenance.json").exists():
        return
    bundle = g2.load_bundle(g2.DEFAULT_SOURCE_BUNDLE, g2.DEFAULT_G_CONFIG)
    verified: dict = {}
    _, _, historical_snapshot = comp.snapshot_for(date(2026, 9, 25), verified)
    extensions = []
    for day in EXTENSION_DAYS:
        run_path, result, snapshot = comp.snapshot_for(day, verified)
        extensions.append(dict(day=str(day), run=str(run_path), snapshot=str(snapshot),
                               result_state=result["state"]))
    if g2.sha256(REPAIR_MINUTE) != REPAIR_SHA:
        raise RuntimeError("DALBHARAT immutable repair minute changed")
    payload = dict(source_bundle=str(bundle["source"]),
        frozen_g_config=str(bundle["g_config_path"]),
        source_bundle_manifest_sha256=g2.sha256(bundle["source"] / "bundle_manifest.json"),
        source_config_sha256=g2.sha256(bundle["g_config_path"]),
        historical_snapshot=str(historical_snapshot), repair_minute=str(REPAIR_MINUTE),
        repair_sha256=REPAIR_SHA, verified_snapshot_manifest_hashes=verified,
        extensions=extensions, days=days,
        protocol="First completed qualifying LONG minute within +1/+2/+3 after existing five-minute slots; native core-first ranking and slot quota; SHORT unchanged; G-2 targets, 1.25%-to-1.00% staged stop after 120 minutes, sizing, 10-minute post-confirmation entry expiry, costs and portfolio constraints unchanged",
        status="EXPLORATORY_REUSED_HISTORY_NOT_OUT_OF_SAMPLE",
        missing_oct1="Excluded: incomplete session snapshot; not synthesized")
    (folder / "provenance.json").write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")


def _clear_inherited_one_minute_fields(path: Path) -> None:
    """Remove unused +1 ancillary fields from delayed-trade exports.

    The already-completed replay used only body, wick and volume from the
    accepted candle. This mechanical export correction does not change an
    order, fill, exit or P&L.
    """
    frame = pd.read_csv(path)
    if "g3_confirmation_offset_minutes" not in frame:
        return
    delayed = pd.to_numeric(frame.g3_confirmation_offset_minutes, errors="coerce").gt(1)
    if not delayed.any():
        return
    consumed = {"v9_1m_body_ratio", "v9_1m_upper_wick_ratio",
                "v9_1m_volume_ratio", "v9_1m_feature_ts"}
    stale = [name for name in frame if name.startswith("v9_1m_") and name not in consumed]
    if stale:
        frame.loc[delayed, stale] = np.nan
    if "v9_feature_available_ts" in frame:
        frame.loc[delayed, "v9_feature_available_ts"] = frame.loc[delayed, "confirmation_ts"]
    frame.to_csv(path, index=False)


def run(folder: Path) -> None:
    summary = pd.read_csv(folder / "variant_summary.csv")
    daily = pd.read_csv(folder / "daywise_comparison.csv")
    expected = {"G2"} | {f"G3_W{window}_V{volume}" for window in (1, 2, 3)
                         for volume in ("1p2", "1p1")}
    if set(summary.variant) != expected or daily.date.nunique() != 46:
        raise ValueError("Incomplete six-arm or 46-session results")
    _recover_provenance(folder, sorted(daily.date.unique().tolist()))
    for name in expected:
        _clear_inherited_one_minute_fields(folder / f"trades_{name}.csv")
    _clear_inherited_one_minute_fields(folder / "added_removed_trades.csv")
    ledgers = {name: pd.read_csv(folder / f"trades_{name}.csv") for name in expected}
    audit = {"sessions": int(daily.date.nunique()), "arms": sorted(expected), "checks": []}
    for name, trades in ledgers.items():
        row = summary.loc[summary.variant.eq(name)].iloc[0]
        executed = trades.loc[trades.portfolio_executed.eq(True)].copy()
        if len(executed) != int(row.trades):
            raise ValueError(f"Trade count mismatch: {name}")
        if abs(executed.portfolio_net_profit_rupees.sum()-float(row.net_pnl)) > 1e-5:
            raise ValueError(f"Net P&L mismatch: {name}")
        if abs(executed.portfolio_cost_rupees.sum()-float(row.cost)) > 1e-5:
            raise ValueError(f"Cost mismatch: {name}")
        if abs(daily.loc[daily.variant.eq(name), "net_pnl"].sum()-float(row.net_pnl)) > 1e-5:
            raise ValueError(f"Daywise P&L mismatch: {name}")
        if len(executed) and not np.allclose(executed.portfolio_cost_rupees, 250., atol=1e-8, rtol=0):
            raise ValueError(f"Unexpected per-trade cost: {name}")
        audit["checks"].append(f"{name}: trade/cost/daywise totals reconcile")
    def selected_keys(name: str, side: str | None = None) -> set:
        x = ledgers[name]
        if side:
            x = x.loc[x.side.eq(side)]
        return set(zip(x.day.astype(str), x.setup_id.astype(str), x.tradingsymbol.astype(str)))
    baseline = ledgers["G2"]
    control = ledgers["G3_W1_V1p2"]
    fields = ["day", "setup_id", "tradingsymbol", "side", "filled", "portfolio_executed",
              "portfolio_net_profit_rupees"]
    a = baseline[fields].sort_values(fields[:4]).reset_index(drop=True)
    b = control[fields].sort_values(fields[:4]).reset_index(drop=True)
    if not a.equals(b):
        raise ValueError("G-2 and 1-minute/1.20× control do not match")
    audit["checks"].append("Exact-next-minute 1.20× control reproduces G-2 trade outcomes")
    shorts = selected_keys("G2", "SHORT")
    identity = ["day", "setup_id", "tradingsymbol"]
    base_clocks = baseline[identity + ["confirmation_ts"]].drop_duplicates(identity)
    for name in expected - {"G2"}:
        if selected_keys(name, "SHORT") != shorts:
            raise ValueError(f"SHORT selection changed: {name}")
        overlap = base_clocks.merge(
            ledgers[name][identity + ["confirmation_ts"]].drop_duplicates(identity),
            on=identity, suffixes=("_g2", "_g3"), validate="one_to_one")
        if overlap.confirmation_ts_g2.ne(overlap.confirmation_ts_g3).any():
            raise ValueError(f"Same stock/setup retimed relative to G-2: {name}")
    audit["checks"].append("All six arms preserve SHORT order selection identity")
    audit["checks"].append("No retained G-2 stock/setup entry was retimed; added/removed trade counts are unambiguous")
    for volume in ("1p2", "1p1"):
        one = selected_keys(f"G3_W1_V{volume}")
        two = selected_keys(f"G3_W2_V{volume}")
        three = selected_keys(f"G3_W3_V{volume}")
        if not (one <= two and two <= three):
            raise ValueError(f"Confirmation window selections are not prefix-preserving: {volume}")
    audit["checks"].append("Window 2/3 selections retain all earlier-window selected orders")
    audit["status"] = "PASS"
    (folder / "validation.json").write_text(json.dumps(audit, indent=2), encoding="utf-8")

    rows = []
    for row in summary.itertuples(index=False):
        name = row.variant
        executed = ledgers[name].loc[ledgers[name].portfolio_executed.eq(True)].copy()
        net_at_10bps = float(row.net_pnl-row.cost)
        pnl_10 = executed.portfolio_net_profit_rupees-executed.portfolio_cost_rupees
        pf10 = (float(pnl_10[pnl_10>0].sum() / -pnl_10[pnl_10<0].sum())
                if pnl_10.lt(0).any() else np.nan)
        rows.append(dict(variant=name, trades=int(row.trades), net_5bps=float(row.net_pnl),
            cost_5bps=float(row.cost), net_10bps=net_at_10bps,
            profit_factor_10bps=pf10, win_rate_10bps=float(100*pnl_10.gt(0).mean()) if len(pnl_10) else np.nan,
            net_delta_10bps_vs_g2=np.nan))
    stress = pd.DataFrame(rows)
    base10 = float(stress.loc[stress.variant.eq("G2"), "net_10bps"].iloc[0])
    stress["net_delta_10bps_vs_g2"] = stress.net_10bps-base10
    stress.to_csv(folder / "cost_stress.csv", index=False)

    display = summary.copy().set_index("variant")
    order = ["G2", "G3_W1_V1p2", "G3_W2_V1p2", "G3_W3_V1p2",
             "G3_W1_V1p1", "G3_W2_V1p1", "G3_W3_V1p1"]
    labels = {"G2":"Frozen G-2", "G3_W1_V1p2":"1m / 1.20×",
        "G3_W2_V1p2":"2m / 1.20×", "G3_W3_V1p2":"3m / 1.20×",
        "G3_W1_V1p1":"1m / 1.10×", "G3_W2_V1p1":"2m / 1.10×",
        "G3_W3_V1p1":"3m / 1.10×"}
    lines = ["# V13-V10-G-3 six-arm LONG confirmation research", "",
        "All 46 available complete sessions through 2026-10-07 IST; October 1 is excluded because its replay is incomplete. No frozen G/G-2 files or live broker rules were changed.", "",
        "| Arm | Executed | Win % | Net P&L | Δ vs G-2 | Cost | PF | Closed-trade DD |",
        "|---|---:|---:|---:|---:|---:|---:|---:|"]
    for name in order:
        r = display.loc[name]
        lines.append(f"| {labels[name]} | {int(r.trades)} | {r.win_rate_pct:.2f} | ₹{r.net_pnl:,.0f} | ₹{r.net_delta_vs_g2:+,.0f} | ₹{r.cost:,.0f} | {r.profit_factor:.3f} | ₹{r.closed_trade_drawdown:,.0f} |")
    lines += ["", "The one-minute 1.20× arm is the exact G-2 control. Window 2/3 means the first qualifying completed confirmation minute among +1/+2 or +1/+2/+3, not the most profitable future candle. No separate intervening-minute cancellation rule is imposed; the accepted candle must still close beyond the five-minute signal close. The 10-minute entry-trigger expiry is measured from the accepted confirmation; this makes its absolute end time up to 1–2 minutes later. All other 5-minute gates, SHORT selection, targets, 1.25%→1.00% staged stop after 120 minutes from fill, sizing and 5-bps trade cost are unchanged.", "",
        "The 10-bps table is an arithmetic cost-only stress on the same fills; it does not model changed fill probability or extra slippage. This is reused-history research, not untouched out-of-sample validation. Even a higher historical net P&L is not approval for live deployment.", "",
        "See `daywise_comparison.csv`, each `trades_*.csv`, `added_removed_trades.csv`, `confirmation_candidate_audit.csv`, `cost_stress.csv`, `validation.json`, and `provenance.json`."]
    (folder / "report.md").write_text("\n".join(lines)+"\n", encoding="utf-8")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("folder", type=Path)
    run(parser.parse_args().folder)
