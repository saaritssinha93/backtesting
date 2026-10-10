"""Research-only V13-V10-G-3: six causal LONG confirmation-window arms.

The frozen G and G-2 configurations are never written. SHORT selection and all
five-minute LONG gates remain unchanged. A window of N means try completed
minutes +1 through +N in order, retaining the first available native selection
for each setup/day. There is no additional intervening-minute invalidation
rule; each accepted minute must still close beyond the 5m signal close.
This is an exploratory, reused-history comparison.
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import fno_oi_hybrid_data as hybrid
import fno_v13_v10_g_2_0925_comparison as comp
import fno_v13_v10_g_2_backtest as g2
import fno_v13_v10_g_2_extend_results as ext
import fno_v13_v10_g_daily_replay as replay
from research import g2_session_forensic_audit as audit

IST = "Asia/Kolkata"
EXTENSION_DAYS = [date(2026, 9, x) for x in (24, 25, 28, 29, 30)] + [date(2026, 10, x) for x in (5, 6, 7)]
REPAIR_MINUTE = Path(r"C:\TradingData\eqidv2\fno_oi\strategy_research\fno_historical_repair_20260827\historical_snapshots\snapshot_20260827T223607687461+0530_41hgqjpi\equity_1m\DALBHARAT_stocks_indicators_1min.parquet")
REPAIR_SHA = "031a0673d765172855593bc2b64403f17316a99d7e2298f0e255d26bb3bb0bd0"
SETUPS = [s for s in g2.g.v9.v5.profile_setups(g2.g.v9.v5.PROFILES["higher_frequency"]) if s.side == "LONG"]


def _save_csv(path: Path, frame: pd.DataFrame) -> None:
    frame.to_csv(path, index=False)


def _minute(path: Path, day: date) -> pd.DataFrame:
    problems: list = []
    minute = replay._load_minute(path, day, problems, path.stem)
    if problems:
        raise RuntimeError(f"Minute quality failure {path} {day}: {problems}")
    volume = pd.to_numeric(minute.volume, errors="coerce")
    denominator = volume.shift(1).rolling(20, min_periods=5).mean()
    minute["v9_1m_volume_ratio"] = volume.div(denominator.where(denominator.gt(0)))
    # Retain only requested-day candles after the past-only rolling feature is
    # computed. Execution never reads an earlier day's bars.
    return minute.loc[minute.ts.dt.date.eq(day)].reset_index(drop=True)


def _historical_minute_path(symbol: str, snapshot: Path) -> Path:
    if symbol == "DALBHARAT":
        if g2.sha256(REPAIR_MINUTE) != REPAIR_SHA:
            raise RuntimeError("DALBHARAT immutable repair snapshot changed")
        return REPAIR_MINUTE
    return hybrid.equity_one_minute_path(symbol, snapshot / "equity_1m")


def _long_five_minute_pool(frame: pd.DataFrame) -> pd.DataFrame:
    x = frame.copy()
    for c in ("signal_ts", "confirmation_ts"):
        x[c] = pd.to_datetime(x[c], utc=True).dt.tz_convert(IST)
    x["day"] = x.signal_ts.dt.date
    x["hhmm_int"] = x.signal_ts.dt.strftime("%H%M").astype(int)
    five = (x.oi.gt(x.prev_oi) & x.oi_change_pct.between(.05, 1.)
            & x.volume_ratio.ge(.8) & x.ema9.gt(x.ema20)
            & x.ema20.gt(x.ema50) & x.price_change_pct.ge(.1))
    if "source_1m_count" in x:
        five &= x.source_1m_count.eq(5)
    parts = []
    for setup in SETUPS:
        core, expanded = g2.g.setup_pair(setup, g2.g.SelectionChange())
        assert core == expanded
        test = (five & x.hhmm_int.eq(int(setup.signal_end.replace(":", "")))
                & x.price_change_pct.ge(core.price_change_pct)
                & x.oi_change_pct.ge(core.oi_change_pct)
                & x.volume_ratio.ge(core.volume_ratio)
                & x.traded_value.ge(core.min_traded_value))
        selected = x.loc[test].copy()
        selected["setup_id"] = setup.setup_id
        parts.append(selected)
    out = pd.concat(parts, ignore_index=True)
    if out.duplicated(["day", "tradingsymbol", "setup_id"]).any():
        raise RuntimeError("Duplicate five-minute LONG candidate")
    return out


def _minute_confirmation(five: pd.DataFrame, minute_by_pair: dict[tuple[date, str], pd.DataFrame],
                         offset: int, start_sid: int) -> tuple[pd.DataFrame, pd.DataFrame]:
    candidates = []
    coverage = []
    for i, row in enumerate(five.itertuples(index=False)):
        stamp = pd.Timestamp(row.signal_ts) + pd.Timedelta(minutes=offset)
        m = minute_by_pair[(row.day, str(row.tradingsymbol))]
        found = m.loc[m.ts.eq(stamp)]
        if len(found) != 1:
            raise RuntimeError(f"Missing exact +{offset}m candle: {row.day} {row.tradingsymbol} {stamp}")
        candle = found.iloc[0]
        o, h, lo, c = (float(candle[k]) for k in ("open", "high", "low", "close"))
        volratio = float(candle.v9_1m_volume_ratio)
        valid = (np.isfinite([o, h, lo, c, volratio]).all() and o > 0 and lo > 0
                 and h > lo and h >= max(o, c) and lo <= min(o, c)
                 and volratio >= 0)
        for flag in ("gap_filled", "opening_snapshot", "provisional_stale"):
            if flag in candle.index and str(candle[flag]).strip().lower() in ("true", "1", "yes", "on"):
                valid = False
        body = abs(c-o)/(h-lo) if h > lo else np.nan
        wick = (h-max(o, c))/(h-lo) if h > lo else np.nan
        direction = c > o and c > float(row.signal_close)
        coverage.append(dict(day=str(row.day), tradingsymbol=row.tradingsymbol,
                             setup_id=row.setup_id, offset_minutes=offset,
                             confirmation_ts=stamp, valid_bar=bool(valid), directional=bool(direction),
                             body_ratio=body, upper_wick_ratio=wick,
                             volume_ratio=volratio))
        if not (valid and direction):
            continue
        record = row._asdict()
        # Source 5m rows carry their original +1-minute ancillary features.
        # They cannot be reported as if observed on a +2/+3 decision.
        for field in tuple(record):
            if field.startswith("v9_1m_"):
                record[field] = np.nan
        record.update(sid=start_sid+i, side="LONG", confirmation_ts=stamp,
                      confirmation_open=o, confirmation_high=h,
                      confirmation_low=lo, confirmation_close=c,
                      confirmation_volume=float(candle.volume), body_ratio=body,
                      wick_ratio=wick, v9_1m_upper_wick_ratio=wick,
                      v9_1m_body_ratio=body, v9_1m_volume_ratio=volratio,
                      v9_1m_feature_ts=stamp, v9_exact_confirmation_present=True,
                      v9_feature_available_ts=stamp,
                      confirmation_source_flagged=False, trigger=h,
                      g3_confirmation_offset_minutes=offset)
        candidates.append(record)
    return pd.DataFrame(candidates), pd.DataFrame(coverage)


def _select(signals: pd.DataFrame, bundle: dict, threshold: float) -> pd.DataFrame:
    if signals.empty:
        return bundle["orders"].iloc[:0].copy()
    work = signals.copy()
    work["research_original_1m_volume_ratio"] = work.v9_1m_volume_ratio
    if threshold != 1.2:
        long = work.side.eq("LONG")
        work.loc[long, "v9_1m_volume_ratio"] *= 1.2 / threshold
    selected = g2.g.select_orders(work, bundle["v9_config"],
        g2.g.SelectionChange(**bundle["source_g"]["selection_change"]), core_first=True)
    if len(selected):
        selected["v9_1m_volume_ratio"] = selected.research_original_1m_volume_ratio
    return selected


def _select_window(original: pd.DataFrame, delayed: dict[int, pd.DataFrame],
                   bundle: dict, threshold: float, window: int) -> pd.DataFrame:
    first = _select(original, bundle, threshold)
    first["g3_confirmation_offset_minutes"] = 1
    parts = [first]
    occupied = set(zip(first.day.astype(str), first.setup_id.astype(str)))
    for offset in range(2, window+1):
        if delayed[offset].empty:
            continue
        chosen = _select(delayed[offset], bundle, threshold)
        if chosen.empty:
            continue
        vacant = ~pd.Series(list(zip(chosen.day.astype(str), chosen.setup_id.astype(str))),
                            index=chosen.index).isin(occupied)
        chosen = chosen.loc[vacant].copy()
        parts.append(chosen)
        occupied.update(zip(chosen.day.astype(str), chosen.setup_id.astype(str)))
    return pd.concat(parts, ignore_index=True)


def _paths_from_archive(source: Path, needed: set[int]) -> dict:
    paths: dict[int, dict] = {}
    with np.load(source / "dataset/paths.npz", allow_pickle=False) as archive:
        for name in archive.files:
            sid_text, field = name.split("_", 1)
            sid = int(sid_text)
            if sid in needed:
                paths.setdefault(sid, {})[field] = archive[name]
    if set(paths) != needed:
        raise RuntimeError(f"Missing sealed paths for {len(needed-set(paths))} original signals")
    return paths


def _simulate(orders: pd.DataFrame, paths: dict, source_g: dict) -> pd.DataFrame:
    if orders.empty:
        return pd.DataFrame()
    g2.g.v9.validate_paths(orders, paths)
    trades = g2.simulate_staged(ext._apply_retained_g_exits(orders, source_g), paths,
        cost_bps=source_g["cost_bps"], max_entry_delay_minutes=10)
    base = ext._portfolio_config(source_g)
    trades = g2.g.v9.v5.apply_fixed_capital_model(trades,
        base.capital_per_entry_rupees, base.leverage_factor)
    return g2.g.v9.v6.apply_portfolio_constraints(trades, base.portfolio_config())[0]


def _extension_pool(day: date, run: Path, snapshot: Path) -> pd.DataFrame:
    ledger_path = run / "feature_ledger.csv"
    if ledger_path.exists():
        meta = g2.read_json(run / "feature_ledger.csv.manifest.json")
        if g2.sha256(ledger_path) != meta["artifact_sha256"]:
            raise RuntimeError(f"Feature ledger drift: {ledger_path}")
        return audit.normalize(pd.read_csv(ledger_path))
    from research.g3_sep24_raw_pool import rebuild_sep24
    if day != date(2026, 9, 24):
        raise RuntimeError(f"No feature ledger for {day}")
    return rebuild_sep24(day, run, snapshot)


def _daily_and_summary(ledgers: dict[str, pd.DataFrame], days: list[date]) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    base = ledgers["G2"]
    baseline = audit.metrics(base)
    daily, rows, changes = [], [], []
    def key(frame):
        e = frame.loc[frame.portfolio_executed.eq(True)]
        return set(zip(e.day.astype(str), e.setup_id.astype(str), e.tradingsymbol.astype(str)))
    bk = key(base)
    for name, ledger in ledgers.items():
        metric = audit.metrics(ledger)
        ek = key(ledger)
        added, removed = ek-bk, bk-ek
        executed = ledger.loc[ledger.portfolio_executed.eq(True)].copy()
        executed["_key"] = list(zip(executed.day.astype(str), executed.setup_id.astype(str),
                                   executed.tradingsymbol.astype(str)))
        added_rows = executed.loc[executed._key.isin(added)]
        metric.update(variant=name, sessions=len(days), first_day=str(min(days)),
                      last_day=str(max(days)), net_delta_vs_g2=metric["net_pnl"]-baseline["net_pnl"],
                      added_trades=len(added), added_winners=int(added_rows.portfolio_net_profit_rupees.gt(0).sum()),
                      added_losers=int(added_rows.portfolio_net_profit_rupees.lt(0).sum()),
                      removed_g2_trades=len(removed), selected_orders=len(ledger),
                      filled_orders=int(ledger.filled.eq(True).sum()))
        rows.append(metric)
        for day in days:
            subset = ledger.loc[ledger.day.astype(str).eq(str(day))]
            daily.append(dict(date=str(day), variant=name, **audit.metrics(subset)))
        if added:
            changes.append(added_rows.assign(change_type="ADDED", variant=name).drop(columns="_key"))
        if removed:
            old = base.loc[base.portfolio_executed.eq(True)].copy()
            old["_key"] = list(zip(old.day.astype(str), old.setup_id.astype(str), old.tradingsymbol.astype(str)))
            changes.append(old.loc[old._key.isin(removed)].assign(change_type="REMOVED", variant=name).drop(columns="_key"))
    d = pd.DataFrame(daily)
    reference = d.loc[d.variant.eq("G2")].set_index("date").net_pnl
    d["net_delta_vs_g2"] = d.net_pnl-d.date.map(reference)
    return pd.DataFrame(rows), d, pd.concat(changes, ignore_index=True) if changes else pd.DataFrame()


def run(out: Path) -> None:
    if out.exists() and any(out.iterdir()):
        raise FileExistsError(f"Output directory must be new or empty: {out}")
    out.mkdir(parents=True, exist_ok=True)
    print("Verify sealed G-2 bundle", flush=True)
    bundle = g2.load_bundle(g2.DEFAULT_SOURCE_BUNDLE, g2.DEFAULT_G_CONFIG)
    verified: dict = {}
    _, _, historical_snapshot = comp.snapshot_for(date(2026, 9, 25), verified)
    historical_raw = pd.read_parquet(bundle["source"] / "dataset/all_5m_features.parquet")
    five = _long_five_minute_pool(historical_raw)
    assert len(five) == 316, "Frozen five-minute LONG pool changed"
    print(f"Historical five-minute LONG candidates: {len(five)}", flush=True)
    hist_minutes = {}
    for (day, symbol), _ in five.groupby(["day", "tradingsymbol"], sort=False):
        path = _historical_minute_path(str(symbol), historical_snapshot)
        hist_minutes[(day, str(symbol))] = _minute(path, day)
    hist_delayed, coverage = {}, []
    for offset in (1, 2, 3):
        frame, checks = _minute_confirmation(five, hist_minutes, offset,
                                              2_000_000 + offset*100_000)
        hist_delayed[offset] = frame
        coverage.append(checks.assign(segment="SEALED_38"))
    # Prove the reconstructed +1 source candles and ratio match sealed features.
    one = pd.DataFrame([dict(day=r.day, tradingsymbol=r.tradingsymbol, setup_id=r.setup_id,
         observed_open=float(hist_minutes[(r.day,str(r.tradingsymbol))].loc[
             lambda m: m.ts.eq(pd.Timestamp(r.signal_ts)+pd.Timedelta(minutes=1)), "open"].iloc[0]),
         source_open=float(r.confirmation_open)) for r in five.itertuples(index=False)])
    if not np.allclose(one.observed_open, one.source_open, rtol=0, atol=1e-9):
        raise RuntimeError("Sealed historical +1 minute parity failed")
    original = bundle["signals"].copy()
    original["day"] = pd.to_datetime(original.day).dt.date
    for c in ("signal_ts", "confirmation_ts", "v9_1m_feature_ts"):
        original[c] = pd.to_datetime(original[c], utc=True).dt.tz_convert(IST)
    names = {"G2": (1.2, 1)}
    for threshold in (1.2, 1.1):
        for window in (1, 2, 3):
            names[f"G3_W{window}_V{str(threshold).replace('.', 'p')}"] = (threshold, window)
    hist_orders = {name: _select_window(original, hist_delayed, bundle, threshold, window)
                   for name, (threshold, window) in names.items()}
    actual = set(zip(bundle["orders"].sid.astype(int), bundle["orders"].setup_id.astype(str)))
    reconstructed = set(zip(hist_orders["G2"].sid.astype(int), hist_orders["G2"].setup_id.astype(str)))
    if actual != reconstructed:
        raise RuntimeError("G-2 historical selections do not match sealed baseline")
    needed_original = set(int(sid) for frame in hist_orders.values() for sid in frame.loc[frame.sid.lt(2_000_000), "sid"])
    historical_paths = _paths_from_archive(bundle["source"], needed_original)
    selected_delayed = pd.concat(hist_orders.values()).loc[lambda x: x.sid.ge(2_000_000)].drop_duplicates("sid")
    for (day, _), group in selected_delayed.groupby(["day", "tradingsymbol"]):
        minute = hist_minutes[(day, str(group.tradingsymbol.iloc[0]))]
        for row in group.itertuples(index=False):
            path = minute.loc[minute.ts.gt(pd.Timestamp(row.confirmation_ts)) & minute.ts.le(replay._cutoff(day))]
            historical_paths[int(row.sid)] = dict(timestamp_ns=path.ts.astype("int64").to_numpy(),
                **{field: path[field].to_numpy(float) for field in ("open", "high", "low", "close")})
    ledgers: dict[str, list[pd.DataFrame]] = {name: [] for name in names}
    for name, orders in hist_orders.items():
        result = _simulate(orders, historical_paths, bundle["source_g"])
        ledgers[name].append(result)
        print("Sealed", name, audit.metrics(result), flush=True)
    provenance = []
    days = list(bundle["days"])
    for extension_number, day in enumerate(EXTENSION_DAYS):
        run_path, result, snapshot = comp.snapshot_for(day, verified)
        raw = _extension_pool(day, run_path, snapshot)
        first_return = float(raw.nifty_first_bar_return_pct.dropna().iloc[0])
        strict = replay._strict_signals(raw, first_return)
        strict["sid"] = strict.sid.astype(int) + 1_000_000 + extension_number*10_000
        five_day = _long_five_minute_pool(raw)
        mins = {}
        for (_, symbol), _ in five_day.groupby(["day", "tradingsymbol"], sort=False):
            mins[(day, str(symbol))] = _minute(hybrid.equity_one_minute_path(
                str(symbol), snapshot / "equity_1m"), day)
        later = {}
        for offset in (1, 2, 3):
            later[offset], checks = _minute_confirmation(five_day, mins, offset,
                3_000_000 + extension_number*100_000 + offset*10_000)
            coverage.append(checks.assign(segment=str(day)))
        chosen = {name: _select_window(strict, later, bundle, threshold, window)
                  for name, (threshold, window) in names.items()}
        union = pd.concat(chosen.values()).drop_duplicates("sid")
        if len(union):
            union["confirmation_ts"] = pd.to_datetime(
                union.confirmation_ts, utc=True).dt.tz_convert(IST)
        new_paths = ext._selected_paths(union, day, snapshot) if len(union) else {}
        for name, orders in chosen.items():
            if len(orders):
                ledgers[name].append(_simulate(orders, new_paths, bundle["source_g"]))
        days.append(day)
        provenance.append(dict(day=str(day), run=str(run_path), snapshot=str(snapshot),
                               result_state=result["state"], five_minute_candidates=len(five_day),
                               original_signals=len(strict)))
        print("Extension", day, {name: len(frame) for name, frame in chosen.items()}, flush=True)
    whole = {name: pd.concat(items, ignore_index=True) for name, items in ledgers.items()}
    summary, daywise, changes = _daily_and_summary(whole, days)
    for name, frame in whole.items():
        _save_csv(out / f"trades_{name}.csv", frame)
    _save_csv(out / "variant_summary.csv", summary)
    _save_csv(out / "daywise_comparison.csv", daywise)
    _save_csv(out / "added_removed_trades.csv", changes)
    _save_csv(out / "confirmation_candidate_audit.csv", pd.concat(coverage, ignore_index=True))
    provenance_record = dict(source_bundle=str(bundle["source"]),
        frozen_g_config=str(bundle["g_config_path"]),
        source_bundle_manifest_sha256=g2.sha256(bundle["source"] / "bundle_manifest.json"),
        source_config_sha256=g2.sha256(bundle["g_config_path"]),
        historical_snapshot=str(historical_snapshot), repair_minute=str(REPAIR_MINUTE),
        repair_sha256=REPAIR_SHA, verified_snapshot_manifest_hashes=verified,
        extensions=provenance, days=[str(d) for d in days],
        protocol="N completed minutes after each existing LONG five-minute signal, first passing minute fills vacant native setup/day slot; SHORT unchanged; native G2 ranking, targets, staged 1.25-to-1.00% stop after 120m, sizing, expiry, costs and portfolio constraints unchanged",
        status="EXPLORATORY_REUSED_HISTORY_NOT_OUT_OF_SAMPLE",
        missing_oct1="Excluded: incomplete session snapshot; not synthesized")
    (out / "provenance.json").write_text(json.dumps(provenance_record, indent=2, default=str), encoding="utf-8")
    print(summary.to_string(index=False), flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, required=True)
    run(parser.parse_args().output_dir)
