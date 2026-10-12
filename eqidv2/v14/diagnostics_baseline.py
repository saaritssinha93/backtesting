"""Read-only numerical reproduction audit against sealed G3 source and inputs.

Writes diagnostic evidence only. It does not select a strategy, refresh data,
change a profile, or import a live trading policy.
"""
from __future__ import annotations

import ast
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pandas as pd

from . import baseline, features

ARCHIVE = Path("C:/TradingData/eqidv2/fno_oi/strategy_research/v13_corrected_v10_g_3/frozen_20261008_long110_nextminute_v1")
OUTPUT = Path("C:/TradingData/eqidv2/v14/diagnostics/20261010_improvement_audit")
V14_RUN = Path("C:/TradingData/eqidv2/v14/runs/20261010_participation_v2_observed")


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def _extract(path, names, namespace):
    tree = ast.parse(Path(path).read_text(encoding="utf-8-sig"))
    selected = [node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name in names]
    if {node.name for node in selected} != set(names):
        raise ValueError("Missing sealed numerical function")
    code = ast.Module(body=selected, type_ignores=[])
    exec(compile(code, str(path), "exec"), namespace)


def native_functions():
    baseline.verify_snapshot()
    namespace = dict(pd=pd, np=np, EMA_SPANS=(9, 20, 50), __builtins__=__builtins__,
                     common=SimpleNamespace(IST="Asia/Kolkata"))
    source = baseline.SNAPSHOT / "source/fno_oi_hybrid_data.py"
    _extract(source, {"_to_ist", "completed_real_equity_five_minute_bars",
                     "aggregate_equity_one_minute_to_five_minute", "add_equity_five_minute_features"}, namespace)
    namespace["hybrid"] = SimpleNamespace(_to_ist=namespace["_to_ist"])
    _extract(baseline.SNAPSHOT / "source/fno_v13_v9_data.py", {"causal_bar_features"}, namespace)
    return namespace


def compare(left, right, columns, *, symbol, comparison):
    joined = left.merge(right, on="signal_ts", suffixes=("_v14", "_g3"), how="inner", validate="one_to_one")
    rows = []
    for v14, g3 in columns.items():
        akey = v14 + "_v14" if v14 in right else v14
        bkey = g3 + "_g3" if g3 in left else g3
        a = pd.to_numeric(joined[akey], errors="coerce").to_numpy(float)
        b = pd.to_numeric(joined[bkey], errors="coerce").to_numpy(float)
        finite = np.isfinite(a) & np.isfinite(b)
        match = np.isclose(a, b, atol=1e-9, rtol=1e-10, equal_nan=True)
        rows.append(dict(symbol=symbol, comparison=comparison, feature=v14, native_feature=g3,
                         rows=len(joined), unequal_rows=int((~match).sum()),
                         finite_rows=int(finite.sum()),
                         max_absolute_difference=float(np.abs(a[finite] - b[finite]).max()) if finite.any() else None))
    return rows


def profit_metrics(trades, days):
    executed = trades.loc[trades.portfolio_executed.astype(str).str.lower().eq("true")].copy()
    net = executed.portfolio_net_profit_rupees
    daily = executed.groupby("day").portfolio_net_profit_rupees.sum().reindex(days, fill_value=0.)
    equity = np.r_[0., daily.cumsum().to_numpy(float)]
    return dict(days=len(days), selected=len(trades), executed=len(executed), wins=int(net.gt(0).sum()),
                win_rate_pct=float(net.gt(0).mean() * 100), net_pnl=float(net.sum()),
                gross_pnl=float(executed.portfolio_gross_profit_rupees.sum()),
                cost=float(executed.portfolio_cost_rupees.sum()), net_per_trade=float(net.mean()),
                daily_close_drawdown=float((np.maximum.accumulate(equity) - equity).max()),
                aggregate_trade_exposure=float(executed.portfolio_trade_exposure_rupees.sum()),
                mean_trade_exposure=float(executed.portfolio_trade_exposure_rupees.mean()))


def profitability(output=OUTPUT, *, run=V14_RUN):
    """Comparable executed trade metrics, retaining the G3 full-period context."""
    output, run = Path(output), Path(run)
    output.mkdir(parents=True, exist_ok=True)
    g3 = pd.read_csv(ARCHIVE / "trades.csv")
    cash = pd.read_csv(run / "trades_no_oi_control.csv")
    days = json.loads((run / "evaluation_coverage.json").read_text())["evaluated_days"]
    g3_days = pd.read_csv(ARCHIVE / "daily_results.csv").day.astype(str).tolist()
    groups = {"G3_ALL_ARCHIVED": (g3, g3_days),
              "G3_MATCHED_V14_DATES": (g3.loc[g3.day.isin(days)], days),
              "V14_NO_OI_CONTROL_MATCHED": (cash.loc[cash.day.isin(days)], days)}
    summary, detail = [], []
    for label, (ledger, dates) in groups.items():
        summary.append(dict(comparison=label, **profit_metrics(ledger, dates)))
        executed = ledger.loc[ledger.portfolio_executed.astype(str).str.lower().eq("true")]
        for column in ("side", "setup_id", "exit_reason"):
            for key, rows in executed.groupby(column):
                detail.append(dict(comparison=label, grouping=column, category=key, trades=len(rows),
                                   net_pnl=float(rows.portfolio_net_profit_rupees.sum()),
                                   wins=int(rows.portfolio_net_profit_rupees.gt(0).sum()),
                                   stops=int(rows.exit_reason.isin(["STOP", "TIGHTENED_STOP"]).sum()),
                                   targets=int(rows.exit_reason.eq("TARGET").sum())))
    pd.DataFrame(summary).to_csv(output / "g3_v14_profit_comparison.csv", index=False)
    pd.DataFrame(detail).to_csv(output / "g3_v14_side_setup_exits.csv", index=False)
    (output / "g3_v14_comparison_provenance.json").write_text(json.dumps(dict(
        source_g3=str(ARCHIVE), source_v14=str(run), evaluated_days=days,
        g3_trades_sha256=sha(ARCHIVE / "trades.csv"), v14_trades_sha256=sha(run / "trades_no_oi_control.csv"),
        note="Aggregate exposure sums each trade's modeled notional, not simultaneous exposure. Drawdown uses daily closes and includes initial zero. Same-date comparison still changes stock universe and OI gates together; not a causal OI ablation."), indent=2), encoding="utf-8")
    return summary


def run(output=OUTPUT, *, max_symbols=12):
    output = Path(output)
    output.mkdir(parents=True, exist_ok=True)
    provenance = json.loads((ARCHIVE / "source_study_provenance.json").read_text())
    bundle = Path(provenance["source_bundle"])
    annotated_path = bundle / "dataset/annotated.parquet"
    manifest = json.loads((bundle / "bundle_manifest.json").read_text())
    expected = manifest["artifacts"]["dataset/annotated.parquet"]["sha256"]
    if sha(annotated_path) != expected:
        raise ValueError("Sealed annotated feature hash changed")
    annotated = pd.read_parquet(annotated_path)
    annotated = annotated.loc[annotated.hhmm_int.isin(features.SIGNAL_TIMES)].copy()
    trades = pd.read_csv(ARCHIVE / "trades.csv")
    executed = trades.portfolio_executed.astype(str).str.lower().eq("true")
    # Declared deterministic sample: most frequently executed symbols in G3.
    symbols = trades.loc[executed].tradingsymbol.value_counts().head(max_symbols).index.tolist()
    snapshot = Path(provenance["historical_snapshot"])
    snapshot_manifest = json.loads((snapshot / "snapshot_manifest.json").read_text())
    if sha(snapshot / "snapshot_manifest.json") != provenance["verified_snapshot_manifest_hashes"][str(snapshot / "snapshot_manifest.json")]:
        raise ValueError("Sealed physical snapshot hash changed")
    records = {r["snapshot_relative_path"]: r for r in snapshot_manifest["sources"] if r.get("captured")}
    native = native_functions()
    audit, sources = [], []
    cash_columns = {name: name for name in ("open", "high", "low", "close", "volume", "price_change_pct", "volume_ratio", "traded_value", "ema9", "ema20", "ema50")}
    archive_columns = {name: name for name in ("price_change_pct", "volume_ratio", "traded_value", "signal_close", "body_ratio", "confirmation_open", "confirmation_high", "confirmation_low", "confirmation_close", "v9_1m_volume_ratio", "v9_1m_upper_wick_ratio", "v9_1m_lower_wick_ratio", "v9_5m_ema_bull", "v9_5m_ema_bear")}
    archive_columns.update({f"ema{x}": f"v9_5m_ema{x}" for x in (9, 20, 50)})
    for symbol in symbols:
        relative = f"equity_1m/{symbol}_stocks_indicators_1min.parquet"
        path = snapshot / relative
        if not path.exists() or relative not in records:
            sources.append(dict(symbol=symbol, status="MISSING_SEALED_MINUTE_INPUT"))
            continue
        if sha(path) != records[relative]["sha256"]:
            raise ValueError(f"Sealed minute input changed: {symbol}")
        minute = pd.read_parquet(path)
        minute["ts"] = native["_to_ist"](minute.date)
        original_five = native["add_equity_five_minute_features"](native["aggregate_equity_one_minute_to_five_minute"](minute))
        clean = features.clean_minutes(minute)
        copied_five = features.aggregate_five(clean)
        original_five["signal_ts"], copied_five["signal_ts"] = original_five.ts, copied_five.ts
        audit.extend(compare(copied_five, original_five, cash_columns, symbol=symbol, comparison="IDENTICAL_SEALED_INPUT_ALL_FIVE_MINUTE_BARS"))
        rebuilt = features.build_features(minute, symbol)
        native_minute = native["causal_bar_features"](minute, "v9_1m_")
        native_minute["signal_ts"] = native_minute.ts - pd.Timedelta(minutes=1)
        audit.extend(compare(rebuilt, native_minute, {
            "v9_1m_volume_ratio": "v9_1m_volume_ratio", "body_ratio": "v9_1m_body_ratio",
            "v9_1m_upper_wick_ratio": "v9_1m_upper_wick_ratio", "v9_1m_lower_wick_ratio": "v9_1m_lower_wick_ratio"},
            symbol=symbol, comparison="IDENTICAL_SEALED_INPUT_CONFIRMATION"))
        prior = annotated.loc[annotated.tradingsymbol.eq(symbol)].copy()
        prior = prior.drop_duplicates("signal_ts")
        audit.extend(compare(rebuilt, prior, archive_columns, symbol=symbol, comparison="ARCHIVED_G3_ANNOTATED_VALUES"))
        sources.append(dict(symbol=symbol, status="AUDITED", source=str(path), source_sha256=sha(path),
                            raw_rows=len(minute), clean_rows=len(clean), native_five_rows=len(original_five),
                            v14_five_rows=len(copied_five), archived_signal_rows=len(prior)))
        print(f"Feature audit: {symbol}, {len(original_five)} five-minute rows, {len(prior)} archived candidates", flush=True)
    result = pd.DataFrame(audit)
    result.to_csv(output / "g3_v14_feature_parity.csv", index=False)
    pd.DataFrame(sources).to_csv(output / "g3_v14_feature_sources.csv", index=False)
    summary = dict(sample_policy="12 most frequently executed frozen G3 symbols, or max_symbols argument", symbols=symbols,
                   features_source_sha256=sha(Path(features.__file__)),
                   annotated_source=str(annotated_path), annotated_sha256=expected,
                   snapshot=str(snapshot), audit_rows=len(result),
                   groups=result.groupby("comparison").agg(feature_checks=("feature", "size"), value_comparisons=("rows", "sum"), unequal_values=("unequal_rows", "sum")).reset_index().to_dict("records"),
                   note="Identical-input comparisons isolate formulas. Archived values additionally compare different sealed source capture dates; differences require lineage inspection, not automatic attribution to formulas.")
    (output / "g3_v14_feature_summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    return summary


if __name__ == "__main__":
    print(json.dumps(run(), indent=2))
    print(json.dumps(profitability(), indent=2))
