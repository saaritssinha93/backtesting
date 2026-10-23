"""Causal G6 entry-filter comparisons on the pinned 551-order baseline.

Run: python -m research.g6_direction_refinements
All candidates are fixed in advance. No default or baseline pointer is changed.
"""
from __future__ import annotations

import argparse
from datetime import datetime
import html
import json
from pathlib import Path
import secrets
import shutil

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

from research import g6_g4_replica_backtest as g6
from research import g6_primary_index_direction as primary
from research import g4_bounded_exit_backtest as bounded
from research import g4_g3_mirror_backtest as base
from research.g4_entry_veto_research import EvidencePrices
from research.g4_extended_target_sweep import read_frozen_csv
from research.g4_vwap_rsi_features import _check_flags

VERSION = "G6-DIRECTION-REFINEMENTS-V1"
CONFIG = base.ROOT / "configs/research/g6_direction_refinements_v1.json"
RESEARCH_ROOT = g6.G6_ROOT / "research"
IST = "Asia/Kolkata"
METRICS = ("trades", "wins", "losses", "losing_days", "net_pnl_rs",
           "negative_trade_sum_abs_rs", "daily_close_drawdown_rs", "stop_exits")
POLICIES = {
    "unfiltered": "Original G6, no additional index veto",
    "nifty_futures": "Previous Nifty-futures slot AND session veto",
    "nifty50_cash": "Nifty-50 cash slot AND session veto",
    "primary_current": "Current primary-index slot AND session veto",
    "both_indices_veto": "Reject only when Nifty futures AND primary index each veto",
    "either_index_veto": "Reject when Nifty futures OR primary index vetoes",
    "primary_fixed_002": "Primary opposition exceeds 0.02% on both measures",
    "primary_fixed_005": "Primary opposition exceeds 0.05% on both measures",
    "stock_vwap": "Primary veto AND stock adverse to session VWAP",
    "stock_break3": "Primary veto AND stock breaks prior three-minute support/resistance",
    "stock_confirmed": "Primary veto AND adverse stock VWAP AND support/resistance break",
    "persistent2": "Primary veto AND two consecutive adverse index candle bodies",
    "persistent3": "Primary veto AND three consecutive adverse index candle bodies",
    "volatility025": "Both primary moves exceed 0.25 times prior index ATR percentage",
    "volatility050": "Both primary moves exceed 0.50 times prior index ATR percentage",
    "volatility100": "Both primary moves exceed 1.00 times prior index ATR percentage",
    "stock_and_persistence": "Stock-confirmed veto AND two-candle persistence",
    "stock_and_volatility": "Stock-confirmed veto AND 0.50 ATR threshold",
    "persistence_and_volatility": "Two-candle persistence AND 0.50 ATR threshold",
    "all_three": "Stock-confirmed veto AND two-candle persistence AND 0.50 ATR threshold",
}
REFERENCES = {"unfiltered": "g6_unfiltered", "nifty_futures": g6.NIFTY_POLICY,
              "nifty50_cash": g6.NIFTY50_SPOT_POLICY, "primary_current": g6.PRIMARY_POLICY}


class PreservedPrices(EvidencePrices):
    """Use hash-verified immutable history if a rolling file has been updated.

    New stock features may use a later preserved historical snapshot, never an
    unverified replacement for a baseline file. Every confirmation OHLC is also
    reconciled to the original selected order in stock_features().
    """
    def __init__(self, summary, expected, through):
        super().__init__(summary, expected)
        self.through = pd.Timestamp(through).date()
        self.resolved = {}

    def frame(self, day, symbol):
        directory = self.snapshots.get(day)
        original = ((directory / "equity_1m") if directory else
                    base.source.DataRoots().equity_1m) / f"{symbol}_stocks_indicators_1min.parquet"
        original_key = str(original.resolve())
        if original_key not in self.resolved:
            digest = base.sha256(original)
            proof = self.baseline_proof.get(original_key, {})
            if digest == proof.get("sha256"):
                self.resolved[original_key] = (original, proof)
            elif directory:
                raise ValueError("Immutable baseline input changed: " + original_key)
            else:
                candidates = [(d, folder / "equity_1m" / original.name)
                              for d, folder in self.snapshots.items() if day <= d <= self.through]
                candidates.sort(reverse=True)
                for snapshot_day, path in candidates:
                    key = str(path.resolve())
                    if key in self.expected and path.is_file():
                        saved_digest = base.sha256(path)
                        if saved_digest != self.expected[key]:
                            raise ValueError("Fallback snapshot hash differs: " + key)
                        self.resolved[original_key] = (path, dict(
                            sha256=saved_digest, source_kind="PRESERVED_HISTORICAL_SNAPSHOT",
                            snapshot_date=str(snapshot_day), replaced_rolling_path=original_key,
                            baseline_rolling_sha256=proof.get("sha256"),
                            observed_updated_rolling_sha256=digest,
                            limitation="New feature input from preserved history; original rolling bytes unavailable. Confirmation OHLC must match baseline."))
                        break
                else:
                    raise ValueError("No preserved historical input for " + original_key)
        path, proof = self.resolved[original_key]
        key = str(path.resolve())
        if key not in self.cache:
            available = set(pq.ParquetFile(path).schema.names)
            columns = ["date", "open", "high", "low", "close", "volume"]
            columns += [x for x in ("gap_filled", "opening_snapshot", "provisional_stale") if x in available]
            frame = pd.read_parquet(path, columns=columns)
            frame["ts"] = base.source.hybrid._to_ist(frame.date).dt.as_unit("ns")
            if frame.ts.isna().any():
                raise ValueError("Invalid price timestamps")
            self.cache[key] = frame.sort_values("ts", kind="stable").reset_index(drop=True)
            while len(self.cache) > 8:
                self.cache.popitem(last=False)
        self.proof[key] = proof
        self.cache.move_to_end(key)
        return self.cache[key], key


def dump(path, value):
    Path(path).write_text(json.dumps(value, indent=2, default=str, allow_nan=False) + "\n",
                          encoding="utf-8")


def checked_stamp(value):
    stamp = pd.Timestamp(value)
    if pd.isna(stamp) or stamp.tzinfo is None:
        raise ValueError("A timezone-aware confirmation is required")
    stamp = stamp.tz_convert(IST)
    if stamp != stamp.floor("min"):
        raise ValueError("Confirmation must be an exact completed minute")
    return stamp


def stock_features(raw, row):
    """Strictly causal session VWAP and prior-three-minute structure break."""
    stamp = checked_stamp(row["confirmation_ts"])
    start = stamp.normalize() + pd.Timedelta(hours=9, minutes=16)
    window = raw.loc[raw.ts.ge(start) & raw.ts.le(stamp)].sort_values("ts").copy()
    if not np.array_equal(pd.DatetimeIndex(window.ts).as_unit("ns").asi8,
                          pd.date_range(start, stamp, freq="min").as_unit("ns").asi8):
        raise ValueError(f"Missing or duplicate stock minute: {row['tradingsymbol']} {stamp}")
    _check_flags(window)
    values = window[["open", "high", "low", "close", "volume"]].to_numpy(float)
    if (not np.isfinite(values).all() or (values[:, :4] <= 0).any()
            or (values[:, 4] < 0).any() or values[:, 4].sum() <= 0):
        raise ValueError("Invalid stock OHLCV")
    if ((values[:, 1] < values[:, [0, 2, 3]].max(axis=1)).any()
            or (values[:, 2] > values[:, [0, 1, 3]].min(axis=1)).any()):
        raise ValueError("Stock OHLC ordering differs")
    for i, field in enumerate(("open", "high", "low", "close")):
        if not np.isclose(values[-1, i], float(row["confirmation_" + field]), rtol=0, atol=.001):
            raise ValueError("Stock confirmation changed: " + field)
    if len(window) < 4:
        raise ValueError("Three preceding completed stock candles required")
    if row["side"] not in ("LONG", "SHORT"):
        raise ValueError("Invalid trade side")
    sign = 1 if row["side"] == "LONG" else -1
    vwap = float(np.average(values[:, [1, 2, 3]].mean(axis=1), weights=values[:, 4]))
    close = float(values[-1, 3])
    low, high = float(values[-4:-1, 2].min()), float(values[-4:-1, 1].max())
    adverse_vwap = sign * (close - vwap) < 0
    adverse_break = close < low if sign == 1 else close > high
    result = dict(stock_feature_ts=str(stamp), stock_bars=len(window), stock_close=close,
        stock_vwap=vwap, stock_prior3_low=low, stock_prior3_high=high,
        stock_adverse_vwap=bool(adverse_vwap), stock_adverse_break3=bool(adverse_break),
        stock_confirmed=bool(adverse_vwap and adverse_break))
    return result, window[["ts", "open", "high", "low", "close", "volume"]]


def index_features(frame, row):
    """Same-session persistence and ATR computed strictly before the current bar."""
    stamp = checked_stamp(row["confirmation_ts"])
    end = stamp.floor("5min")
    start = stamp.normalize() + pd.Timedelta(hours=9, minutes=20)
    window = frame.loc[frame.ts.ge(start) & frame.ts.le(end)].sort_values("ts").copy()
    if not np.array_equal(pd.DatetimeIndex(window.ts).as_unit("ns").asi8,
                          pd.date_range(start, end, freq="5min").as_unit("ns").asi8):
        raise ValueError("Missing or duplicate index context candle")
    data = window[["open", "high", "low", "close"]].to_numpy(float)
    if len(data) == 0 or not np.isfinite(data).all() or (data <= 0).any():
        raise ValueError("Invalid index OHLC")
    if ((data[:, 1] < data[:, [0, 2, 3]].max(axis=1)).any()
            or (data[:, 2] > data[:, [0, 1, 3]].min(axis=1)).any()):
        raise ValueError("Index OHLC ordering differs")
    if row["side"] not in ("LONG", "SHORT"):
        raise ValueError("Invalid trade side")
    sign = 1 if row["side"] == "LONG" else -1
    slot = float((data[-1, 3] / data[-1, 0] - 1) * 100)
    session = float((data[-1, 3] / data[0, 0] - 1) * 100)
    veto = bool(sign * slot < 0 and sign * session < 0)
    result = dict(index_feature_ts=str(end), index_bars=len(window),
        primary_slot_pct=slot, primary_session_pct=session, primary_veto=veto)
    for n in (2, 3):
        enough = len(data) >= n
        adverse = bool(np.all(sign * (data[-n:, 3] - data[-n:, 0]) < 0)) if enough else False
        result[f"persistence{n}_ready"] = enough
        result[f"persistent{n}_veto"] = veto and (adverse if enough else True)
    prior = data[:-1]
    ready = len(prior) >= 5
    atr = np.nan
    if ready:
        previous = np.r_[prior[0, 0], prior[:-1, 3]]
        tr = np.maximum.reduce([prior[:, 1] - prior[:, 2],
            np.abs(prior[:, 1] - previous), np.abs(prior[:, 2] - previous)])
        atr = float(tr[-14:].mean() / prior[-1, 3] * 100)
        if not np.isfinite(atr) or atr < 0:
            raise ValueError("Invalid prior index ATR")
    result.update(atr_ready=ready, atr_prior_bars=min(14, len(prior)),
                  index_atr_pct=atr, atr_last_bar_ts=str(window.ts.iloc[-2]) if ready else "")
    for multiplier, suffix in ((.25, "025"), (.5, "050"), (1., "100")):
        result["volatility" + suffix + "_veto"] = bool(
            sign * slot < -multiplier * atr and sign * session < -multiplier * atr) if ready else veto
    return result


def decisions(features):
    """No outcome columns participate in any entry decision."""
    p, n = features.primary_veto.astype(bool), features.nifty_veto.astype(bool)
    v = features.stock_adverse_vwap.astype(bool)
    b = features.stock_adverse_break3.astype(bool)
    stock = p & v & b
    two = features.persistent2_veto.astype(bool)
    vol = features.volatility050_veto.astype(bool)
    sign = features.side.map({"LONG": 1, "SHORT": -1})
    if sign.isna().any():
        raise ValueError("Invalid direction side")
    result = dict(unfiltered=pd.Series(False, index=features.index), nifty_futures=n,
        nifty50_cash=features.nifty50_cash_veto.astype(bool), primary_current=p,
        both_indices_veto=p & n, either_index_veto=p | n,
        primary_fixed_002=(sign * features.primary_slot_pct < -.02) &
                          (sign * features.primary_session_pct < -.02),
        primary_fixed_005=(sign * features.primary_slot_pct < -.05) &
                          (sign * features.primary_session_pct < -.05),
        stock_vwap=p & v, stock_break3=p & b, stock_confirmed=stock,
        persistent2=two, persistent3=features.persistent3_veto.astype(bool),
        volatility025=features.volatility025_veto.astype(bool), volatility050=vol,
        volatility100=features.volatility100_veto.astype(bool),
        stock_and_persistence=stock & two, stock_and_volatility=stock & vol,
        persistence_and_volatility=two & vol, all_three=stock & two & vol)
    if list(result) != list(POLICIES):
        raise ValueError("Policy manifest differs")
    return pd.DataFrame(result).astype(bool)


def allocate(original, veto, config, policy, cost_bps=5):
    """Reuse the verified fill/exit paths and replay sizing/portfolio allocation."""
    if len(veto) != len(original):
        raise ValueError("Decision count differs")
    work = original.copy()
    work["cost_pct"] = float(cost_bps) / 100
    work["net_return_pct"] = work.gross_return_pct - work.cost_pct
    audit = pd.DataFrame(dict(sid=work.sid.to_numpy(), both_counter_allowed=~np.asarray(veto, bool)))
    settings = {**config, "model_cost_bps_round_trip": float(cost_bps)}
    book, _ = primary.apply_filter(work, audit, settings)
    book["g6_direction_filter_mode"] = policy
    book["research_policy"] = policy
    book["research_veto"] = np.asarray(veto, bool)
    book["strategy_version"] = VERSION
    book["strategy_fingerprint"] = base.fingerprint(dict(version=VERSION, policy=policy,
                                                          cost_bps=cost_bps))
    book.loc[np.asarray(veto, bool), "portfolio_status"] = "G6_REFINEMENT_BLOCKED"
    book.loc[np.asarray(veto, bool), "portfolio_reject_reason"] = policy
    return book


def extra_stats(book, days):
    value = bounded.stats(book, days)
    part = book.loc[book.day.isin(days)]
    trades = part.loc[part.portfolio_executed.eq(True)]
    daily = trades.groupby("day").portfolio_net_profit_rupees.sum().reindex(days, fill_value=0.)
    pnl = trades.portfolio_net_profit_rupees
    value.update(worst_day_rs=float(daily.min()),
        losing_day_sum_abs_rs=float(-daily.loc[daily.lt(0)].sum()),
        gross_winner_profit_rs=float(pnl.loc[pnl.gt(0)].sum()),
        profit_factor=float(pnl.loc[pnl.gt(0)].sum() / -pnl.loc[pnl.lt(0)].sum())
                      if pnl.lt(0).any() else 0., vetoed_orders=int(part.research_veto.sum()))
    return value


def compare_metrics(books, days, period):
    metrics = {name: extra_stats(book, days) for name, book in books.items()}
    rows = []
    for name, value in metrics.items():
        row = dict(policy=name, description=POLICIES.get(name, "Chronological selection"),
                   period=period, **value)
        for ref in ("nifty_futures", "primary_current", "unfiltered"):
            baseline = metrics[ref]
            row.update({"delta_vs_" + ref + "_" + key: value[key] - baseline[key]
                        for key in METRICS})
        rows.append(row)
    return rows


def verify_package(root):
    manifest = base.read(root / "manifest.json")
    for name, digest in manifest["files"].items():
        path = (root / name).resolve()
        if not path.is_relative_to(root.resolve()) or base.sha256(path) != digest:
            raise ValueError("Artifact changed: " + name)
    return manifest


def prepare_features(source, original, summary, output):
    source_g3 = Path(summary["current_g3_source_run"])
    if base.sha256(source_g3 / "manifest.json") != summary["current_g3_manifest_sha256"]:
        raise ValueError("G3 price provenance changed")
    prices = PreservedPrices(base.read(source_g3 / "summary.json"), summary["price_sources"], summary["last_date"])
    audit = pd.read_csv(source / "primary_index_direction_audit.csv").set_index("sid")
    nifty = pd.read_csv(source / "nifty_direction_audit.csv").set_index("sid")
    cash = pd.read_csv(source / "nifty50_spot_direction_audit.csv").set_index("sid")
    records, contexts, index_cache = [], [], {}
    for count, row in enumerate(original.sort_values(["tradingsymbol", "day", "confirmation_ts"]).to_dict("records"), 1):
        sid, day = int(row["sid"]), row["day"]
        raw, key = prices.frame(pd.Timestamp(day).date(), row["tradingsymbol"])
        stock, window = stock_features(raw, row)
        contexts.append(window.assign(sid=sid))
        mapped = audit.loc[sid]
        index_name = mapped.mapped_primary_index
        cache_key = (index_name, day)
        if cache_key not in index_cache:
            slug = primary.INDEX_SPECS[index_name]["slug"]
            index_cache[cache_key] = pd.read_parquet(source / "primary_index_inputs" / slug / (day + ".parquet"))
        context = index_features(index_cache[cache_key], row)
        if (context["primary_veto"] != (not bool(mapped.both_counter_allowed))
                or not np.isclose(context["primary_slot_pct"], mapped.slot_change_pct, atol=1e-10, rtol=0)
                or not np.isclose(context["primary_session_pct"], mapped.session_change_pct, atol=1e-10, rtol=0)):
            raise ValueError("Primary direction differs from sealed source")
        records.append(dict(sid=sid, day=day, setup_id=row["setup_id"],
            tradingsymbol=row["tradingsymbol"], side=row["side"], confirmation_ts=str(row["confirmation_ts"]),
            primary_index=index_name, mapping_regime=mapped.mapping_regime,
            price_source=key, price_source_sha256=prices.proof[key]["sha256"],
            nifty_veto=not bool(nifty.loc[sid, "both_counter_allowed"]),
            nifty50_cash_veto=not bool(cash.loc[sid, "both_counter_allowed"]), **stock, **context))
        if count % 100 == 0:
            print(f"Causal feature coverage: {count}/{len(original)}", flush=True)
    features = pd.DataFrame(records).set_index("sid").loc[original.sid].reset_index()
    if features.sid.tolist() != original.sid.tolist():
        raise ValueError("Feature order identity changed")
    pd.concat(contexts, ignore_index=True).to_parquet(output / "stock_confirmation_inputs.parquet", index=False)
    rows = [frame.assign(primary_index=name) for (name, _), frame in index_cache.items()]
    pd.concat(rows, ignore_index=True).to_parquet(output / "index_confirmation_inputs.parquet", index=False)
    features.to_csv(output / "features.csv", index=False)
    dump(output / "stock_price_source_proof.json", prices.proof)
    for path, proof in prices.proof.items():
        if base.sha256(Path(path)) != proof["sha256"]:
            raise ValueError("Stock prices changed during feature preparation")
    return features


def chronological_selection(books, days):
    """Select on earlier dates only, then freeze for each next block."""
    choices, segments = [], []
    candidates = [name for name in POLICIES if name not in ("unfiltered", "nifty50_cash")]
    for start in range(20, len(days), 10):
        training, testing = days[:start], days[start:start + 10]
        scores = {name: extra_stats(books[name], training) for name in candidates}
        ref = scores["nifty_futures"]
        eligible = [name for name, s in scores.items() if all(
            s[key] <= ref[key] + 1e-8 for key in
            ("losing_days", "negative_trade_sum_abs_rs", "daily_close_drawdown_rs"))]
        selected = max(eligible, key=lambda name: (scores[name]["net_pnl_rs"], -candidates.index(name)))
        choices.append(dict(train_start=training[0], train_end=training[-1],
            test_start=testing[0], test_end=testing[-1], policy=selected,
            eligible_policies=";".join(eligible),
            training_profit_rs=scores[selected]["net_pnl_rs"],
            **{"test_" + key: value for key, value in extra_stats(books[selected], testing).items()}))
        segments.append(books[selected].loc[books[selected].day.isin(testing)].copy())
    return pd.concat(segments, ignore_index=True), pd.DataFrame(choices)


def report_html(output, summary, comparison, daily, coverage, walk):
    all_rows = comparison.loc[comparison.period.eq("all54")].copy()
    fields = ["policy", "trades", "wins", "losses", "losing_days", "net_pnl_rs",
              "negative_trade_sum_abs_rs", "daily_close_drawdown_rs", "worst_day_rs",
              "delta_vs_nifty_futures_net_pnl_rs"]
    later = comparison.loc[comparison.period.eq("later19")]
    display = lambda frame: '<div class="scroll">' + frame.to_html(index=False, border=0,
        float_format=lambda x: f"{x:,.2f}") + '</div>'
    parts = ['<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">',
        '<title>G6 direction refinements</title><style>body{font:15px/1.5 system-ui;margin:28px;background:#101927;color:#edf4ff}h1,h2{color:#e5f4ff}a{color:#8ed6ff}table{border-collapse:collapse;font-size:13px}td,th{padding:8px;border-bottom:1px solid #405269;text-align:right;white-space:nowrap}th{background:#26364e}td:first-child,th:first-child{text-align:left}.scroll{overflow:auto;margin:18px 0}.note{padding:15px;background:#1b2c43;border-radius:8px}</style></head><body>',
        '<h1>G6: compare 20 entry-filter variants</h1>',
        f'<p>{summary["first_date"]} to {summary["last_date"]}: {summary["sessions"]} sessions, {summary["orders"]} selected orders, 51 setups. No entry delay. User selection pending.</p>',
        '<p class="note">Amounts are INR after 5bps round-trip model cost. Capital is INR 100,000 per entry with 5x exposure and INR 1,000,000 total portfolio capital. Losing-trade amount and daily-close drawdown are positive loss magnitudes; lower is better. This is historical research, including dates previously studied.</p>',
        '<h2>Full history</h2>', display(all_rows[fields]),
        '<h2>Later 19 sessions: 21 September to 19 October</h2>', display(later[fields]),
        '<h2>Feature coverage and overlap with existing entry rules</h2>', display(coverage),
        '<p>Stock features use verified baseline files or hash-verified preserved historical snapshots where rolling files have since changed. All confirmation OHLC matches the original orders. Original rolling bytes are not recoverable for those replacements; source details are saved in stock_price_source_proof.json.</p>',
        '<h2>Chronological selection</h2><p>Each block uses a policy selected only from preceding dates. Eligibility requires no worse training loss amount, losing days or drawdown than Nifty; then select highest training profit. These dates are not an untouched holdout.</p>',
        display(walk[["train_end", "test_start", "test_end", "policy", "test_net_pnl_rs", "test_losing_days", "test_losses"]]),
        '<h2>Exact policy definitions</h2>', display(pd.DataFrame([dict(policy=k, rule=v) for k,v in POLICIES.items()])),
        '<p>Persistence with insufficient same-session candles retains the original primary-index veto. ATR is the mean true range of up to 14 prior same-session five-minute candles, excluding the decision candle, with at least five required; early entries retain the original veto. A structural break is the confirmation close crossing the prior three-minute low for longs or high for shorts. Pair and triple combinations require all named conditions to veto.</p>',
        '<h2>Results files</h2><ul>']
    links = {"comparison.csv":"All periods and baseline deltas", "daily_comparison.csv":"Day-wise results",
        "slot_side_comparison.csv":"All 51 slots and sides", "side_comparison.csv":"Long and short totals",
        "index_side_comparison.csv":"Primary index and side", "cost_sensitivity.csv":"5/10/15bps costs",
        "changed_trades.csv":"Trade changes against each reference", "decisions.csv":"All entry decisions",
        "features.csv":"Causal features", "pareto_choices.csv":"Undominated historical choices",
        "walk_forward_choices.csv":"Chronological choices", "walk_forward_comparison.csv":"Chronological results",
        "config.json":"Frozen research definitions", "summary.json":"Run evidence"}
    parts.extend('<li><a href="'+name+'">'+label+'</a></li>' for name,label in links.items())
    parts.append('</ul><p>All policy ledgers are under policies/&lt;policy&gt;/portfolio_trades.csv. Current G6 defaults and baseline pointer have not been changed.</p></body></html>')
    (output / "report.html").write_text("".join(parts), encoding="utf-8")


def run(output, *, feature_cache=None):
    spec = base.read(CONFIG)
    if spec["version"] != VERSION or spec["execution_authority"] is not False:
        raise ValueError("Unexpected research configuration")
    source = (g6.G6_ROOT / spec["source_run"]).resolve()
    if base.sha256(source / "manifest.json") != spec["source_manifest_sha256"]:
        raise ValueError("Pinned G6 baseline changed")
    verify_package(source)
    summary = base.read(source / "summary.json")
    config = base.read(source / "config.json")
    original = read_frozen_csv(source / "g6_unfiltered/portfolio_trades.csv")
    days = summary["days"]
    if len(days) != 54 or len(original) != 551 or original.sid.duplicated().any():
        raise ValueError("Unexpected pinned cohort")
    output = Path(output).resolve()
    if output.exists() or not output.is_relative_to(RESEARCH_ROOT.resolve()):
        raise ValueError("Require fresh isolated G6 research directory")
    pointer = g6.G6_ROOT / "latest/bounded_exit.json"
    untouched = {str(path): base.sha256(path) for path in (pointer, g6.DEFAULT_CONFIG)}
    code_paths = (Path(__file__), CONFIG, Path(primary.__file__), Path(bounded.__file__),
                  Path(base.__file__), base.ROOT / "research/g4_entry_veto_research.py",
                  base.ROOT / "research/g4_vwap_rsi_features.py")
    code = {str(path.resolve()): base.sha256(path) for path in code_paths}
    output.mkdir(parents=True)
    dump(output / "config.json", spec)
    print("Verified pinned baseline: 54 sessions, 551 orders; preparing confirmation features", flush=True)
    if feature_cache:
        feature_cache = Path(feature_cache).resolve()
        verify_package(feature_cache)
        for name in ("features.csv", "stock_confirmation_inputs.parquet",
                     "index_confirmation_inputs.parquet", "stock_price_source_proof.json"):
            shutil.copy2(feature_cache / name, output / name)
        features = pd.read_csv(output / "features.csv")
    else:
        features = prepare_features(source, original, summary, output)
    if features.sid.tolist() != original.sid.tolist():
        raise ValueError("Cached feature cohort differs")
    vetoes = decisions(features)
    decision_output = pd.concat([features[["sid", "day", "setup_id", "tradingsymbol", "side"]],
                                vetoes.add_prefix("veto_")], axis=1)
    decision_output.to_csv(output / "decisions.csv", index=False)
    books = {}
    for policy in POLICIES:
        book = allocate(original, vetoes[policy], config, policy)
        if policy in REFERENCES:
            saved = read_frozen_csv(source / REFERENCES[policy] / "portfolio_trades.csv")
            bounded.history._same_allocation(book, saved)
        books[policy] = book
        folder = output / "policies" / policy
        folder.mkdir(parents=True)
        book.to_csv(folder / "portfolio_trades.csv", index=False)
        print(f"{policy}: {bounded.stats(book, days)['net_pnl_rs']:,.2f}", flush=True)
    periods = {"all54": days, "first35": days[:35], "later19": days[35:],
               "october": [d for d in days if d >= "2026-10-01"],
               "october15": ["2026-10-15"], "october16": ["2026-10-16"], "last_day": days[-1:]}
    comparison = pd.DataFrame([row for period, cohort in periods.items()
                              for row in compare_metrics(books, cohort, period)])
    comparison.to_csv(output / "comparison.csv", index=False)
    daily = pd.DataFrame([dict(day=day, **row) for day in days
                          for row in compare_metrics(books, [day], "day")])
    daily.to_csv(output / "daily_comparison.csv", index=False)
    feature_columns = ["stock_adverse_vwap", "stock_adverse_break3", "stock_confirmed",
        "persistence2_ready", "persistence3_ready", "atr_ready"]
    coverage = pd.DataFrame([dict(feature=field, true_orders=int(features[field].sum()),
        false_orders=int((~features[field].astype(bool)).sum()),
        primary_veto_true_orders=int((features[field].astype(bool) & features.primary_veto).sum()))
        for field in feature_columns])
    coverage.to_csv(output / "feature_coverage.csv", index=False)
    setup_rows, side_rows, index_rows = [], [], []
    for setup in config["active_setups"]:
        grouped = {p: b.loc[b.setup_id.eq(setup["setup_id"])] for p,b in books.items()}
        setup_rows.extend(dict(setup_id=setup["setup_id"], signal_end=setup["signal_end"],
            side=setup["side"], **row) for row in compare_metrics(grouped, days, "all54"))
    for side in ("LONG", "SHORT"):
        grouped = {p: b.loc[b.side.eq(side)] for p,b in books.items()}
        side_rows.extend(dict(side=side, **row) for row in compare_metrics(grouped, days, "all54"))
    for (index, side), group in features.groupby(["primary_index", "side"]):
        grouped = {p: b.loc[b.sid.isin(group.sid)] for p,b in books.items()}
        index_rows.extend(dict(primary_index=index, side=side, **row)
                          for row in compare_metrics(grouped, days, "all54"))
    pd.DataFrame(setup_rows).to_csv(output / "slot_side_comparison.csv", index=False)
    pd.DataFrame(side_rows).to_csv(output / "side_comparison.csv", index=False)
    pd.DataFrame(index_rows).to_csv(output / "index_side_comparison.csv", index=False)
    changes = []
    for policy, book in books.items():
        for ref in ("nifty_futures", "primary_current"):
            before = books[ref].set_index("sid").loc[book.sid]
            after = book.set_index("sid")
            changed = before.portfolio_executed.ne(after.portfolio_executed) | ~np.isclose(
                before.portfolio_net_profit_rupees, after.portfolio_net_profit_rupees, rtol=0, atol=1e-8)
            part = after.loc[changed, ["day", "setup_id", "tradingsymbol", "side"]].copy()
            part["policy"], part["reference"] = policy, ref
            part["before_executed"] = before.loc[changed, "portfolio_executed"]
            part["after_executed"] = after.loc[changed, "portfolio_executed"]
            part["before_net_pnl_rs"] = before.loc[changed, "portfolio_net_profit_rupees"]
            part["after_net_pnl_rs"] = after.loc[changed, "portfolio_net_profit_rupees"]
            part["net_change_rs"] = part.after_net_pnl_rs - part.before_net_pnl_rs
            changes.append(part.reset_index())
    pd.concat(changes, ignore_index=True).to_csv(output / "changed_trades.csv", index=False)
    cost_rows = []
    for cost in (5, 10, 15):
        cost_books = {name: allocate(original, vetoes[name], config, name, cost) for name in POLICIES}
        for period in ("all54", "later19"):
            cost_rows.extend(dict(cost_bps=cost, **row)
                             for row in compare_metrics(cost_books, periods[period], period))
    pd.DataFrame(cost_rows).to_csv(output / "cost_sensitivity.csv", index=False)
    walk_book, choices = chronological_selection(books, days)
    choices.to_csv(output / "walk_forward_choices.csv", index=False)
    walk_books = {name: book.loc[book.day.isin(days[20:])] for name,book in books.items() if name in REFERENCES}
    walk_books["chronological_selection"] = walk_book
    pd.DataFrame(compare_metrics(walk_books, days[20:], "walk_forward34")).to_csv(
        output / "walk_forward_comparison.csv", index=False)
    walk_book.to_csv(output / "walk_forward_portfolio.csv", index=False)
    all_rows = comparison.loc[comparison.period.eq("all54")].copy()
    dominated = []
    for _, row in all_rows.iterrows():
        other = all_rows.loc[all_rows.policy.ne(row.policy)]
        weak = (other.net_pnl_rs >= row.net_pnl_rs - 1e-8)
        strict = other.net_pnl_rs > row.net_pnl_rs + 1e-8
        for field in ("losing_days", "negative_trade_sum_abs_rs", "daily_close_drawdown_rs"):
            weak &= other[field] <= row[field] + 1e-8
            strict |= other[field] < row[field] - 1e-8
        dominated.append(bool((weak & strict).any()))
    all_rows["dominated"] = dominated
    all_rows.to_csv(output / "pareto_choices.csv", index=False)
    aliases = {}
    for policy in POLICIES:
        earlier = list(POLICIES)[:list(POLICIES).index(policy)]
        aliases[policy] = next((p for p in earlier if vetoes[p].equals(vetoes[policy])), policy)
    result = dict(version=VERSION, execution_authority=False, default_changed=False,
        source_run=str(source), source_manifest_sha256=spec["source_manifest_sha256"],
        sessions=len(days), orders=len(original), first_date=days[0], last_date=days[-1], days=days,
        policies=POLICIES, identical_decision_policy=aliases, feature_coverage=coverage.to_dict("records"),
        no_post_confirmation_data=True, reference_allocation_parity=True,
        chronological_dates_previously_researched=True, input_policy=spec,
        code_sha256=code, unchanged_defaults=untouched)
    dump(output / "summary.json", result)
    report_html(output, result, comparison, daily, coverage, choices)
    for name, digest in {**untouched, **code}.items():
        if base.sha256(Path(name)) != digest:
            raise ValueError("Default or source code changed during research: " + name)
    dump(output / "manifest.json", dict(version=VERSION, code_sha256=code,
        files={p.relative_to(output).as_posix(): base.sha256(p)
               for p in output.rglob("*") if p.is_file()}))
    verify(output)
    print("VERIFIED", output, flush=True)
    return result


def verify(output):
    output = Path(output).resolve()
    verify_package(output)
    summary = base.read(output / "summary.json")
    if summary["version"] != VERSION or summary["execution_authority"] is not False:
        raise ValueError("Unexpected research identity")
    source = Path(summary["source_run"])
    if base.sha256(source / "manifest.json") != summary["source_manifest_sha256"]:
        raise ValueError("Pinned source identity differs")
    verify_package(source)
    features = pd.read_csv(output / "features.csv")
    selected = read_frozen_csv(Path(summary["source_run"]) / "g6_unfiltered/portfolio_trades.csv")
    if features.sid.tolist() != selected.sid.tolist():
        raise ValueError("Saved feature identities differ")
    stocks = pd.read_parquet(output / "stock_confirmation_inputs.parquet")
    indexes = pd.read_parquet(output / "index_confirmation_inputs.parquet")
    stock_groups = {int(sid): part for sid,part in stocks.groupby("sid")}
    index_groups = {(name, day): part for (name,day),part in indexes.groupby(["primary_index", "day"])}
    by_sid = features.set_index("sid")
    for row in selected.to_dict("records"):
        sid = int(row["sid"])
        f = by_sid.loc[sid]
        stock, _ = stock_features(stock_groups[sid], row)
        index = index_features(index_groups[(f.primary_index, row["day"])], row)
        for key, value in {**stock, **index}.items():
            saved = f[key]
            if isinstance(value, (bool, np.bool_)):
                if bool(saved) != value:
                    raise ValueError("Saved feature flag differs: " + key)
            elif isinstance(value, (int, float, np.number)):
                if not np.isclose(value, saved, rtol=0, atol=1e-9, equal_nan=True):
                    raise ValueError("Saved numeric feature differs: " + key)
            elif (str(saved) if pd.notna(saved) else "") != value:
                raise ValueError("Saved feature timestamp differs: " + key)
    vetoes = decisions(features)
    saved_decisions = pd.read_csv(output / "decisions.csv")
    for policy in POLICIES:
        if not saved_decisions["veto_" + policy].equals(vetoes[policy]):
            raise ValueError("Saved decisions differ: " + policy)
    config = base.read(Path(summary["source_run"]) / "config.json")
    comparison = pd.read_csv(output / "comparison.csv")
    daily = pd.read_csv(output / "daily_comparison.csv")
    books = {}
    periods = {"all54": summary["days"], "first35": summary["days"][:35],
        "later19": summary["days"][35:],
        "october": [d for d in summary["days"] if d >= "2026-10-01"],
        "october15": ["2026-10-15"], "october16": ["2026-10-16"],
        "last_day": summary["days"][-1:]}
    for policy in POLICIES:
        book = read_frozen_csv(output / "policies" / policy / "portfolio_trades.csv")
        books[policy] = book
        rebuilt = allocate(selected, vetoes[policy], config, policy)
        bounded.history._same_allocation(book, rebuilt)
        if (book.portfolio_executed & vetoes[policy]).any():
            raise ValueError("A vetoed trade received capital")
        for period, cohort in periods.items():
            row = comparison.loc[comparison.policy.eq(policy) & comparison.period.eq(period)].iloc[0]
            for key, value in extra_stats(book, cohort).items():
                if not np.isclose(row[key], value, rtol=0, atol=1e-6):
                    raise ValueError("Summary differs: " + policy + " " + key)
        for day in summary["days"]:
            saved = daily.loc[daily.policy.eq(policy) & daily.day.eq(day)].iloc[0]
            for key, value in extra_stats(book, [day]).items():
                if not np.isclose(saved[key], value, rtol=0, atol=1e-6):
                    raise ValueError("Daily accounting differs")
    for field in ("slot_side_comparison.csv", "side_comparison.csv", "index_side_comparison.csv"):
        grouped = pd.read_csv(output / field).groupby("policy").net_pnl_rs.sum()
        all_profit = comparison.loc[comparison.period.eq("all54")].set_index("policy").net_pnl_rs
        np.testing.assert_allclose(grouped.loc[all_profit.index], all_profit, rtol=0, atol=1e-6)
    costs = pd.read_csv(output / "cost_sensitivity.csv")
    for cost in (5, 10, 15):
        for policy in POLICIES:
            replay = allocate(selected, vetoes[policy], config, policy, cost)
            for period in ("all54", "later19"):
                row = costs.loc[costs.cost_bps.eq(cost) & costs.policy.eq(policy)
                                & costs.period.eq(period)].iloc[0]
                for key, value in extra_stats(replay, periods[period]).items():
                    if not np.isclose(row[key], value, rtol=0, atol=1e-6):
                        raise ValueError("Cost replay differs")
    walk, choices = chronological_selection(books, summary["days"])
    bounded.history._same_allocation(walk, read_frozen_csv(output / "walk_forward_portfolio.csv"))
    pd.testing.assert_frame_equal(choices, pd.read_csv(output / "walk_forward_choices.csv"),
                                  check_dtype=False, check_exact=False, atol=1e-6, rtol=0)
    return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--verify", type=Path)
    parser.add_argument("--feature-cache", type=Path)
    args = parser.parse_args()
    if args.verify:
        print(json.dumps({"verified": str(args.verify), "policies": len(verify(args.verify)["policies"])}))
        return
    stamp = datetime.now().strftime("%Y%m%dT%H%M%S")
    output = args.output_dir or RESEARCH_ROOT / (stamp + "_" + secrets.token_hex(4) + "_direction_refinements")
    run(output, feature_cache=args.feature_cache)


if __name__ == "__main__":
    main()
