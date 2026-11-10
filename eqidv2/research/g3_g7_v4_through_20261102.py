"""Extend the verified G3 V17 and G7 V4 Lock comparison through Nov 2.

This is an isolated research report. It does not replace the official G3
historical pointer, whose pinned October 13 source is currently unavailable.
"""
from __future__ import annotations

import argparse
from datetime import date
import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd

import fno_v13_replay_source as source
import fno_v13_v10_g_3_contract as production
from research import g3_target_cutoff_contract as g3
from research.g3_backtest_profile import contract as g3_current
from research import g7_g6_replica_backtest as g7
from research import g7_v4_lock_backtest as lock
from research import g7_v5_today_partial as engine


ROOT = Path(__file__).resolve().parents[1]
OLD = ROOT / "outputs/full_backtest_20261029/shared_replay"
V18 = ROOT / "g3_v18_20261102/results_20261102_complete"
TODAY = Path("C:/TradingData/eqidv2/g3_g7_v4_through_20261102/g3_daily_20261102")
VALIDATION = Path("C:/TradingData/eqidv2/backtesting_result_v13_v10_g_3/production/runs/2026-10-27/20261027T171521886383_0a615989")
LATER = ("2026-10-28", "2026-10-29", "2026-10-30", "2026-11-02")


def sha(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def verified_file(root: Path, name: str) -> Path:
    manifest = json.loads((root / "artifact_manifest.json" if root == OLD else root / "manifest.json").read_text(encoding="utf-8"))
    path = root / name
    if sha(path) != manifest["files"][name]:
        raise ValueError(f"Historical artifact changed: {path}")
    return path


def later_sources() -> dict[str, dict]:
    provenance_path = verified_file(V18, "source_provenance.json")
    records = json.loads(provenance_path.read_text(encoding="utf-8"))["sessions"]
    chosen = {item["day"]: item for item in records if item["day"] in LATER}
    if set(chosen) != set(LATER) - {"2026-11-02"}:
        raise ValueError("The three post-October-27 source days are unavailable")
    for item in chosen.values():
        if sha(Path(item["feature_path"])) != item["features_sha256"]:
            raise ValueError("Late G3 feature source changed")
        if sha(Path(item["snapshot_manifest"])) != item["snapshot_manifest_sha256"]:
            raise ValueError("Late snapshot manifest changed")
    today_result = json.loads((TODAY / "replay_result.json").read_text(encoding="utf-8"))
    if not today_result["complete"] or today_result["strategy_version"] != g3_current.STRATEGY_VERSION:
        raise ValueError("Today G3 daily replay is not the completed V17 contract")
    today_manifest = json.loads((TODAY / "run_manifest.json").read_text(encoding="utf-8"))
    for item in today_manifest["artifacts"].values():
        path = TODAY / item["path"]
        if sha(path) != item["sha256"]:
            raise ValueError(f"Today G3 artifact changed: {path.name}")
    today_source = json.loads((TODAY / "source_manifest.json").read_text(encoding="utf-8"))
    today_snapshot = Path(today_source["snapshot_manifest"])
    if sha(today_snapshot) != today_source["snapshot_manifest_sha256"]:
        raise ValueError("Today snapshot manifest changed")
    chosen["2026-11-02"] = dict(day="2026-11-02", feature_path=str(TODAY / "feature_ledger.csv"),
                                 snapshot_manifest=str(today_snapshot),
                                 snapshot_manifest_sha256=sha(today_snapshot),
                                 candidate_signals=str(TODAY / "candidate_signals.csv"),
                                 daily_result=str(TODAY / "replay_result.json"))
    return chosen


def register(day: str, item: dict) -> Path:
    snapshot = Path(item["snapshot_manifest"]).parent
    manifest = json.loads((snapshot / "snapshot_manifest.json").read_text(encoding="utf-8"))
    source._verify_input_snapshot(snapshot, manifest)
    return snapshot


def signals_for(day: str, item: dict) -> pd.DataFrame:
    if "candidate_signals" in item:
        return pd.read_csv(item["candidate_signals"])
    feature = pd.read_csv(item["feature_path"])
    contexts = pd.to_numeric(feature.nifty_first_bar_return_pct, errors="coerce").dropna().unique()
    if len(contexts) != 1:
        raise ValueError(f"Ambiguous Nifty context: {day}")
    return source._strict_signals(feature, float(contexts[0]))


def g7_day(day: str, item: dict) -> tuple[pd.DataFrame, dict]:
    snapshot = register(day, item)
    signals = signals_for(day, item)
    pool = g3.selection_audit(signals)
    ema = pd.to_numeric(pool.v9_1m_ema20, errors="coerce")
    atr = pd.to_numeric(pool.v9_1m_atr14, errors="coerce")
    close = pd.to_numeric(pool.confirmation_close, errors="coerce")
    if not (np.isfinite(ema).all() and np.isfinite(atr).all() and atr.gt(0).all()
            and np.isfinite(close).all()):
        raise ValueError(f"Sealed confirmation EMA/ATR unavailable: {day}")
    direction = np.where(pool.side.eq("LONG"), 1., -1.)
    pool["research_ema20"] = ema
    pool["research_atr14"] = atr
    pool["research_extension_atr"] = direction * (close - ema) / atr
    pool["research_feature_source"] = item["feature_path"]
    config = g7.configuration()
    baseline = g7.reselect(pool, config)
    prior_ids = {(str(row.day)[:10], "0926_SHORT", row.tradingsymbol): int(row.sid)
                 for row in baseline.loc[baseline.setup_id.eq("0926_SHORT")].itertuples()}
    short_pool = pool.loc[pool.setup_id.eq("0926_SHORT")]
    if len(short_pool):
        replacement, _ = lock.select_variant(short_pool, "G7_V4_LOCK2",
                                              {"variant": "wick_ge_0p20", "wick_ratio_min": .2},
                                              production, prior_ids)
    else:
        replacement = baseline.iloc[:0].copy()
    selected = pd.concat((baseline.loc[~baseline.setup_id.eq("0926_SHORT")], replacement),
                         ignore_index=True, sort=False)
    selected, _, rejected = g7.apply_slot_entry_filters(selected, config)
    if selected.duplicated(["setup_id", "tradingsymbol"]).any():
        raise ValueError(f"Duplicate G7 selected setup/symbol: {day}")
    selected = selected.sort_values(["confirmation_ts", "setup_id", "tradingsymbol"]).reset_index(drop=True)
    selected["sid"] = np.arange(780_000_000, 780_000_000 + len(selected), dtype=np.int64)
    selected["signal_id"] = [f"g7v4extension:{day}:{setup}:{symbol}"
                             for setup, symbol in zip(selected.setup_id, selected.tradingsymbol)]
    book, proof = engine.replay(selected, {"snapshot": str(snapshot)},
                                pd.Timestamp(day + " 15:15", tz="Asia/Kolkata"))
    if book.position_state.eq("OPEN").any() or not np.isclose(
            book.portfolio_net_profit_rupees.loc[book.portfolio_executed].sum(),
            book.realized_net_rs.sum(), atol=1e-5):
        raise ValueError(f"Full-day G7 exits did not settle: {day}")
    return book, dict(day=day, strict_signals=len(signals), prequota_pool=len(pool),
                      slot_entry_rejections=len(rejected), snapshot=str(snapshot),
                      snapshot_manifest_sha256=sha(snapshot / "snapshot_manifest.json"), **proof)


def flags(series: pd.Series) -> pd.Series:
    return series.astype(str).str.lower().eq("true")


def stats(book: pd.DataFrame, day: str) -> dict:
    subset = book.loc[book.day.astype(str).str[:10].eq(day)]
    done = subset.loc[flags(subset.portfolio_executed)]
    pnl = pd.to_numeric(done.portfolio_net_profit_rupees, errors="raise")
    gross = pd.to_numeric(done.portfolio_gross_profit_rupees, errors="raise")
    cost = pd.to_numeric(done.portfolio_cost_rupees, errors="raise")
    if not np.isclose((gross-cost).sum(), pnl.sum(), atol=1e-5):
        raise ValueError(f"P&L accounting mismatch: {day}")
    return dict(day=day, trades=len(done), wins=int(pnl.gt(0).sum()),
                losses=int(pnl.lt(0).sum()), win_rate_pct=100*float(pnl.gt(0).mean()) if len(done) else 0.,
                net_pnl_rs=float(pnl.sum()), gross_pnl_rs=float(gross.sum()),
                cost_rs=float(cost.sum()), return_on_10l_capital_pct=float(pnl.sum())/10_000.)


def run(output: Path) -> dict:
    if output.exists():
        raise FileExistsError(output)
    old_g3 = pd.read_csv(verified_file(OLD, "g3_portfolio_trades.csv"), low_memory=False)
    old_g7 = pd.read_csv(verified_file(OLD, "g7_v4_lock2_portfolio_trades.csv"), low_memory=False)
    v18_control = pd.read_csv(verified_file(V18, "control_trades.csv"), low_memory=False)
    old_days = sorted(old_g7.day.astype(str).unique())
    if len(old_days) != 59 or old_days[-1] != "2026-10-27":
        raise ValueError("Pinned G7 V4 old calendar changed")
    if set(old_g3.day.astype(str)) != set(old_days):
        raise ValueError("Old G3 and G7 calendars differ")
    control_old = v18_control.loc[v18_control.day.astype(str).isin(old_days)]
    for day in old_days:
        a, b = stats(old_g3, day), stats(control_old, day)
        for field in ("trades", "wins", "losses", "net_pnl_rs"):
            if not np.isclose(a[field], b[field], atol=1e-5):
                raise ValueError(f"G3 V17 control does not reproduce saved G3 on {day}: {field}")
    validation_source = json.loads((VALIDATION / "source_manifest.json").read_text(encoding="utf-8"))
    validation_snapshot = Path(validation_source["snapshot_manifest"])
    if sha(validation_snapshot) != validation_source["snapshot_manifest_sha256"]:
        raise ValueError("October 27 G7 parity snapshot changed")
    validation_book, _ = g7_day("2026-10-27", dict(
        day="2026-10-27", feature_path=str(VALIDATION / "feature_ledger.csv"),
        snapshot_manifest=str(validation_snapshot),
        candidate_signals=str(VALIDATION / "candidate_signals.csv")))
    expected = old_g7.loc[old_g7.day.astype(str).eq("2026-10-27")]
    comparable = validation_book.merge(expected, on=["setup_id", "tradingsymbol"],
                                       suffixes=("_fresh", "_saved"), validate="one_to_one")
    if (len(comparable) != len(expected) or len(validation_book) != len(expected)
            or not flags(comparable.portfolio_executed_fresh).equals(
                flags(comparable.portfolio_executed_saved))
            or not np.allclose(comparable.portfolio_net_profit_rupees_fresh,
                               comparable.portfolio_net_profit_rupees_saved, atol=1e-5, rtol=0)):
        raise ValueError("G7 V4 selection/execution does not reproduce saved October 27")
    validation_proof = dict(day="2026-10-27", selected_orders=len(validation_book),
                            executed_entries=int(flags(validation_book.portfolio_executed).sum()),
                            net_pnl_rs=float(comparable.portfolio_net_profit_rupees_fresh.sum()),
                            saved_net_pnl_rs=float(comparable.portfolio_net_profit_rupees_saved.sum()),
                            passed=True)
    source_records = later_sources()
    extension = []
    proofs = []
    for day in LATER:
        print(f"REPLAY G7 V4 {day}", flush=True)
        book, proof = g7_day(day, source_records[day])
        extension.append(book)
        proofs.append(proof)
    g7_later = pd.concat(extension, ignore_index=True, sort=False)
    if set(g7_later.day.astype(str)) != set(LATER):
        raise ValueError("G7 extension lost a session")
    g3_today = pd.read_csv(TODAY / "portfolio_trades.csv", low_memory=False)
    g3_all = pd.concat((v18_control, g3_today), ignore_index=True, sort=False)
    g7_all = pd.concat((old_g7, g7_later), ignore_index=True, sort=False)
    days = old_days + list(LATER)
    if len(days) != 63 or days != sorted(set(days)):
        raise ValueError("Unexpected complete-session calendar")
    daily = pd.DataFrame([{**{"strategy": strategy}, **stats(book, day)}
                          for strategy, book in (("G3_V17", g3_all), ("G7_V4_LOCK2", g7_all))
                          for day in days])
    for strategy in ("G3_V17", "G7_V4_LOCK2"):
        mask = daily.strategy.eq(strategy)
        daily.loc[mask, "cumulative_net_rs"] = daily.loc[mask, "net_pnl_rs"].cumsum().to_numpy()
    wide = daily.pivot(index="day", columns="strategy")
    paired = pd.DataFrame({"day": days})
    for strategy in ("G3_V17", "G7_V4_LOCK2"):
        for field in ("trades", "wins", "losses", "win_rate_pct", "net_pnl_rs", "gross_pnl_rs",
                      "cost_rs", "return_on_10l_capital_pct", "cumulative_net_rs"):
            paired[f"{strategy}_{field}"] = wide[(field, strategy)].reindex(days).to_numpy()
    paired["g7_minus_g3_net_rs"] = paired.G7_V4_LOCK2_net_pnl_rs - paired.G3_V17_net_pnl_rs
    paired["g7_minus_g3_cumulative_rs"] = paired.g7_minus_g3_net_rs.cumsum()
    overall = []
    for strategy, book in (("G3_V17", g3_all), ("G7_V4_LOCK2", g7_all)):
        chosen = daily.loc[daily.strategy.eq(strategy)]
        total_trades = int(chosen.trades.sum()); wins = int(chosen.wins.sum()); losses = int(chosen.losses.sum())
        cumulative = chosen.net_pnl_rs.cumsum()
        drawdown = cumulative.cummax().clip(lower=0).sub(cumulative).max()
        overall.append(dict(strategy=strategy, sessions=len(days), first_date=days[0], last_date=days[-1],
                            trades=total_trades, wins=wins, losses=losses,
                            win_rate_pct=100*wins/total_trades if total_trades else 0.,
                            profitable_days=int(chosen.net_pnl_rs.gt(0).sum()),
                            losing_days=int(chosen.net_pnl_rs.lt(0).sum()),
                            net_pnl_rs=float(chosen.net_pnl_rs.sum()),
                            gross_pnl_rs=float(chosen.gross_pnl_rs.sum()),
                            cost_rs=float(chosen.cost_rs.sum()),
                            return_on_10l_capital_pct=float(chosen.net_pnl_rs.sum())/10_000.,
                            daily_close_drawdown_rs=float(drawdown)))
    output.mkdir(parents=True)
    daily.to_csv(output / "daywise_long.csv", index=False)
    paired.to_csv(output / "daywise_comparison.csv", index=False)
    pd.DataFrame(overall).to_csv(output / "overall_comparison.csv", index=False)
    g7_later.to_csv(output / "g7_extension_trades.csv", index=False)
    proof = dict(scope="BACKTEST_ONLY", execution_authority=False, days=days,
                 excluded_session="2026-10-01", restricted_session="2026-10-28",
                 g3_reference=str(V18 / "control_trades.csv"),
                 g3_today=str(TODAY / "replay_result.json"),
                 g7_reference=str(OLD / "g7_v4_lock2_portfolio_trades.csv"),
                 reconstruction_validation=validation_proof,
                 g7_extension=proofs,
                 official_g3_history_publish="UNAVAILABLE_MISSING_PINNED_2026_10_13_MANIFEST")
    (output / "source_provenance.json").write_text(json.dumps(proof, indent=2, default=str), encoding="utf-8")
    return dict(overall=overall, last_four=paired.tail(4).to_dict("records"), output=str(output))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(run(args.output), indent=2, default=str))


if __name__ == "__main__":
    main()
