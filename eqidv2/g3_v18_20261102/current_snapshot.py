"""Descriptive current-day native-5m snapshot; no signals, orders or P&L.

Reads one immutable byte image per source, verifies real contiguous bars,
and selects the newest exact Nifty/stock clock with >=90% dated-universe
coverage. It does not synthesize minute counts or patch historical stores.
"""
from __future__ import annotations

import argparse
from datetime import date, datetime
import hashlib
import html
import io
import json
from pathlib import Path
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

from g3_v18_20261102.run import PACKAGE, clean, dump, guard_writes, sha

DATA = Path("C:/TradingData/eqidv2")
CASH = DATA / "stocks_indicators_5min_eq_live"
SPOT = CASH / "NIFTY50_INDEX_stocks_indicators_5min.parquet"
IST = ZoneInfo("Asia/Kolkata")
FLAGS = ("gap_filled", "opening_snapshot", "provisional_stale", "is_synthetic", "synthetic")


def read_image(path: Path) -> tuple[pd.DataFrame, dict]:
    """Hash exactly the bytes parsed, even if the live filename is replaced."""
    content = path.read_bytes()
    return pd.read_parquet(io.BytesIO(content)), dict(path=str(path), sha256=hashlib.sha256(content).hexdigest(),
        bytes=len(content), read_at_ist=datetime.now(IST).isoformat())


def true_flags(values: pd.Series) -> np.ndarray:
    return values.astype(str).str.strip().str.lower().isin(("1", "1.0", "true", "yes", "on")).to_numpy()


def current_bars(frame: pd.DataFrame, day: str, captured_at: datetime, *, stock: bool) -> pd.DataFrame:
    """Native completed five-minute features; invalid prefixes stay invalid.

    ``source_1m_count`` is checked only when actually supplied by the source;
    no minute rows/counts are fabricated. Native bars are not a substitute for
    the separate one-minute paths needed to replay G3 entry/exit execution.
    """
    f = frame.copy()
    f["bar_end_ist"] = pd.to_datetime(f.date, utc=True).dt.tz_convert(IST)
    start = pd.Timestamp(f"{day} 09:20", tz=IST)
    end = min(pd.Timestamp(captured_at), pd.Timestamp(f"{day} 15:30", tz=IST))
    f = f.loc[f.bar_end_ist.between(start, end)].sort_values("bar_end_ist", kind="stable").reset_index(drop=True)
    if f.empty:
        return f
    expected = pd.date_range(start, periods=len(f), freq="5min")
    stamps = pd.DatetimeIndex(f.bar_end_ist)
    exact = stamps.asi8 == expected.asi8
    exact &= ~f.bar_end_ist.duplicated(keep=False).to_numpy()
    values = f[["open", "high", "low", "close"]].apply(pd.to_numeric, errors="coerce").to_numpy(float)
    o, high, low, close = values.T
    valid = exact & np.isfinite(values).all(axis=1) & (values > 0).all(axis=1)
    valid &= (high >= np.maximum(o, close)) & (low <= np.minimum(o, close))
    flagged = np.zeros(len(f), dtype=bool)
    for key in FLAGS:
        if key in f:
            flagged |= true_flags(f[key])
    valid &= ~flagged
    if stock:
        volume = pd.to_numeric(f.volume, errors="coerce").to_numpy(float)
        valid &= np.isfinite(volume) & (volume >= 0)
        if "source_1m_count" in f:
            reported = pd.to_numeric(f.source_1m_count, errors="coerce")
            # A populated count explicitly contradicting a complete 5m bar
            # invalidates it; absent counts remain absent native provenance.
            valid &= (reported.isna() | reported.eq(5)).to_numpy()
    ready = np.logical_and.accumulate(valid)
    f["valid_contiguous_prefix"] = ready
    f["invalid_bar_flag"] = flagged
    f["return_from_open_pct"] = np.where(ready, 100 * (close / o[0] - 1), np.nan)
    if stock:
        cumulative_volume = np.cumsum(np.nan_to_num(volume, nan=0.))
        numerator = np.cumsum(np.nan_to_num((high + low + close) / 3 * volume, nan=0.))
        vwap = np.divide(numerator, cumulative_volume, out=np.full(len(f), np.nan), where=cumulative_volume > 0)
        ready &= cumulative_volume > 0
        f["valid_contiguous_prefix"] = ready
        vwap[~ready] = np.nan
        f["session_vwap_5m_approx"] = vwap
        f["vwap_distance_pct"] = 100 * (close / vwap - 1)
        f["vwap_slope_5m_pct"] = 100 * (vwap / pd.Series(vwap).shift().to_numpy() - 1)
        location = np.divide(2 * close - high - low, high - low, out=np.zeros(len(f)), where=high > low)
        weighted = pd.Series(location * volume).rolling(6, min_periods=1).sum().to_numpy()
        denominator = pd.Series(volume).rolling(6, min_periods=1).sum().to_numpy()
        pressure = np.divide(weighted, denominator, out=np.full(len(f), np.nan), where=denominator > 0)
        pressure[~ready] = np.nan
        f["volume_close_location_pressure_6"] = pressure
        f["cumulative_observed_volume"] = cumulative_volume
    f["source_timeframe"] = "NATIVE_OBSERVED_5M_END_LABELLED"
    return f


def render(output: Path, summary: dict, stocks: pd.DataFrame, unavailable: pd.DataFrame, coverage: pd.DataFrame):
    def table(frame):
        return '<div class="scroll">' + frame.to_html(index=False, border=0, na_rep="Unavailable", float_format=lambda x: f"{x:,.3f}") + '</div>'
    columns = ["symbol", "last_close", "return_from_open_pct", "relative_strength_vs_nifty_pct",
               "vwap_distance_pct", "vwap_slope_5m_pct", "volume_close_location_pressure_6"]
    if len(stocks):
        gainers = stocks.sort_values(["return_from_open_pct", "symbol"], ascending=[False, True]).head(10)[columns]
        losers = stocks.sort_values(["return_from_open_pct", "symbol"], ascending=[True, True]).head(10)[columns]
        detail = f'<h2>Ten largest gains from today’s open</h2>{table(gainers)}<h2>Ten weakest returns from today’s open</h2>{table(losers)}'
    else:
        detail = '<p>No clock met the required coverage; no ranked snapshot is shown.</p>'
    headline = f"{summary['common_bar_end_ist']}" if summary.get("common_bar_end_ist") else "No qualifying common clock"
    page = f'''<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>G3 V18 — November 2 current snapshot</title><style>
body{{margin:0;background:#0c131e;color:#e8edf4;font:15px/1.6 system-ui,sans-serif}}main{{max-width:1280px;margin:auto;padding:36px 28px}}h1{{font-size:32px}}h2{{margin-top:32px}}a{{color:#78c9ff}}.muted{{color:#a7b6ca}}.notice{{background:#16273a;border-left:4px solid #eab96c;padding:18px}}.cards{{display:flex;gap:16px;flex-wrap:wrap}}.card{{background:#17263b;padding:18px;border-radius:8px;min-width:180px}}.card strong{{display:block;font-size:23px}}.scroll{{overflow:auto}}table{{border-collapse:collapse;white-space:nowrap;width:100%;font-size:13px}}th,td{{padding:10px;border-bottom:1px solid #2a3a50}}th{{background:#1c2d43;text-align:left}}tr:nth-child(even){{background:#122033}}code{{color:#9ad9ce}}</style></head><body><main>
<p class="muted">G3 V18 · DESCRIPTIVE MARKET SNAPSHOT · NO EXECUTION AUTHORITY</p><h1>Today’s observed market and stock movement</h1><p>Session {summary['session_date']} · Latest qualifying completed common bar: <strong>{html.escape(headline)}</strong></p>
<p class="notice">These are observed indicators, not new entry recommendations or order signals. Today’s G3 backtest P&amp;L remains unavailable because complete one-minute execution inputs are not sealed. Native five-minute bars support this descriptive snapshot only. No stale stock values are carried forward.</p>
<div class="cards"><div class="card">Dated F&amp;O stock universe<strong>{summary['universe_stocks']}</strong></div><div class="card">Exact-clock valid stocks<strong>{summary.get('snapshot_stocks',0)}</strong></div><div class="card">Coverage<strong>{summary.get('coverage_pct',0):.1f}%</strong></div><div class="card">Nifty from today’s open<strong>{summary.get('nifty_from_open_pct') if summary.get('nifty_from_open_pct') is not None else 'Unavailable'}</strong></div></div>
<h2>Participation at the common clock</h2><p>{summary.get('advances',0)} stocks above their session opening price, {summary.get('declines',0)} below, and {summary.get('flat',0)} unchanged. Stocks above open: {summary.get('breadth_above_open_pct',0):.2f}%; stocks below open: {summary.get('breadth_below_open_pct',0):.2f}%. This is the dated F&amp;O cohort, not all NSE stocks, and uses today’s open rather than yesterday’s close.</p>
{detail}<h2>Coverage by completed Nifty clock</h2>{table(coverage)}<h2>Unavailable at the chosen clock</h2>{table(unavailable)}
<h2>Feature definitions and provenance</h2><p>Relative strength subtracts the true Nifty 50 spot return since today’s opening price from the stock’s return over the same period. Session VWAP is an approximation using completed five-minute typical price multiplied by volume. Slope compares consecutive completed VWAP observations; the first bar has no slope. Pressure is the volume-weighted close location over the latest six bars, ranging from −1 to +1; it is not observed bid/ask order flow.</p>
<p>Every usable series has an exact, continuous prefix from 09:20. Flagged, synthetic, incomplete, duplicate or noncontiguous input bars invalidate the corresponding prefix. Each source is read once as bytes and hashed from those same bytes. Files can have different read times; the snapshot nevertheless requires an exact shared completed bar and at least 90% universe coverage. No one-minute counts are invented.</p>
<p class="muted">Captured {html.escape(summary['captured_at_ist'])}. Snapshot age at capture: {summary.get('age_minutes_at_capture','Unavailable')} minutes. This file is a saved snapshot, not a continuously updating live dashboard.</p>
<p><a href="all_stocks_snapshot.csv">All valid stocks</a> · <a href="market_snapshot.csv">Market snapshot</a> · <a href="coverage_by_clock.csv">Coverage</a> · <a href="unavailable_stocks.csv">Unavailable stocks</a> · <a href="summary.json">Summary</a> · <a href="source_provenance.json">Source hashes</a> · <a href="manifest.json">Manifest</a></p></main></body></html>'''
    (output / "report.html").write_text(page, encoding="utf-8")


def run(day: str, output: Path):
    output = output.resolve()
    if not output.is_relative_to(PACKAGE) or output == PACKAGE:
        raise ValueError("Snapshot output must be a new folder under the new package")
    if output.exists():
        raise FileExistsError(output)
    guard_writes(PACKAGE)
    captured_at = datetime.now(IST)
    if day > captured_at.date().isoformat():
        raise ValueError("Cannot build a future snapshot")
    universe_path = DATA / f"fno_oi/universe/near_month_{day}.parquet"
    universe, universe_proof = read_image(universe_path)
    symbols = sorted(universe.loc[~universe.is_index_future.fillna(False).astype(bool), "equity_symbol"].dropna().astype(str).unique())
    if not symbols:
        raise ValueError("Empty dated stock universe")
    nifty, nifty_proof = read_image(SPOT)
    nifty = current_bars(nifty, day, captured_at, stock=False)
    eligible_nifty = nifty.loc[nifty.get("valid_contiguous_prefix", pd.Series(False, index=nifty.index)).eq(True)].copy()
    frames, proofs, status = {}, [], {}
    for i, symbol in enumerate(symbols, 1):
        path = CASH / f"{symbol}_stocks_indicators_5min.parquet"
        if not path.is_file():
            status[symbol] = dict(symbol=symbol, reason="MISSING_NATIVE_5M_FILE")
            continue
        try:
            source, proof = read_image(path)
            bars = current_bars(source, day, captured_at, stock=True)
            proofs.append(dict(symbol=symbol, **proof))
            frames[symbol] = bars
            status[symbol] = dict(symbol=symbol, latest_observed_bar_ist=str(bars.bar_end_ist.max()) if len(bars) else None,
                today_rows=len(bars), valid_prefix_rows=int(bars.valid_contiguous_prefix.sum()) if len(bars) else 0,
                reason="NO_TODAY_COMPLETED_BARS" if bars.empty else "NO_EXACT_VALID_PREFIX_AT_COMMON_CLOCK")
        except (ValueError, OSError, KeyError) as error:
            status[symbol] = dict(symbol=symbol, reason="UNREADABLE_OR_INVALID_NATIVE_5M", detail=str(error))
        if i % 50 == 0:
            print(f"Snapshot read {i}/{len(symbols)} native five-minute sources", flush=True)
    coverage = []
    for stamp in eligible_nifty.bar_end_ist:
        count = sum(int((bars.bar_end_ist.eq(stamp) & bars.valid_contiguous_prefix).sum()) for bars in frames.values() if len(bars))
        coverage.append(dict(bar_end_ist=str(stamp), valid_stocks=count, universe_stocks=len(symbols),
                             coverage_pct=100*count/len(symbols), minimum_90pct_pass=count/len(symbols)>=.9))
    coverage_frame = pd.DataFrame(coverage, columns=["bar_end_ist", "valid_stocks", "universe_stocks", "coverage_pct", "minimum_90pct_pass"])
    qualifying = coverage_frame.loc[coverage_frame.minimum_90pct_pass.eq(True)]
    records, common = [], None
    summary = dict(version="G3-V18-CURRENT-DESCRIPTIVE-SNAPSHOT", scope="DESCRIPTIVE_ONLY", execution_authority=False,
        order_signals=False, pnl_available=False, pnl_status="PENDING_COMPLETE_SEALED_ONE_MINUTE_EXECUTION_INPUTS",
        session_date=day, captured_at_ist=captured_at.isoformat(), universe_stocks=len(symbols),
        minimum_universe_coverage_pct=90., common_bar_end_ist=None,
        source_timeframe="NATIVE_OBSERVED_5M_END_LABELLED", minute_counts_fabricated=False,
        latest_nifty_observed_bar_ist=str(nifty.bar_end_ist.max()) if len(nifty) else None)
    if len(qualifying):
        chosen = qualifying.iloc[-1]
        common = pd.Timestamp(chosen.bar_end_ist)
        market = eligible_nifty.loc[eligible_nifty.bar_end_ist.eq(common)].iloc[0]
        market_return = float(market.return_from_open_pct)
        for symbol, bars in frames.items():
            if bars.empty:
                continue
            part = bars.loc[bars.bar_end_ist.eq(common) & bars.valid_contiguous_prefix]
            if len(part) != 1:
                continue
            row = part.iloc[0]
            records.append(dict(symbol=symbol, bar_end_ist=str(common), last_close=float(row.close),
                return_from_open_pct=float(row.return_from_open_pct), relative_strength_vs_nifty_pct=float(row.return_from_open_pct)-market_return,
                session_vwap_5m_approx=float(row.session_vwap_5m_approx), vwap_distance_pct=float(row.vwap_distance_pct),
                vwap_slope_5m_pct=float(row.vwap_slope_5m_pct), volume_close_location_pressure_6=float(row.volume_close_location_pressure_6),
                cumulative_observed_volume=float(row.cumulative_observed_volume),
                source_reported_1m_count=row.get("source_1m_count", None), source_timeframe="NATIVE_OBSERVED_5M_END_LABELLED"))
        stocks = pd.DataFrame(records).sort_values("symbol").reset_index(drop=True)
        advances, declines, flat = (int(op(stocks.return_from_open_pct,0).sum()) for op in (np.greater,np.less,np.equal))
        summary.update(common_bar_end_ist=str(common), snapshot_stocks=len(stocks), coverage_pct=100*len(stocks)/len(symbols),
            nifty_from_open_pct=market_return, nifty_last_close=float(market.close), advances=advances, declines=declines, flat=flat,
            breadth_above_open_pct=100*advances/len(stocks), breadth_below_open_pct=100*declines/len(stocks),
            age_minutes_at_capture=round((pd.Timestamp(captured_at)-common).total_seconds()/60,2), state="COMMON_CLOCK_COVERAGE_PASSED")
    else:
        stocks = pd.DataFrame(columns=["symbol", "bar_end_ist"])
        summary.update(snapshot_stocks=0,coverage_pct=0.,state="NO_CLOCK_WITH_90_PERCENT_CONTIGUOUS_REAL_STOCK_COVERAGE")
    included = set(stocks.symbol)
    unavailable = pd.DataFrame([value for symbol,value in status.items() if symbol not in included],
        columns=["symbol","reason","latest_observed_bar_ist","today_rows","valid_prefix_rows","detail"])
    output.mkdir(parents=True)
    stocks.to_csv(output / "all_stocks_snapshot.csv", index=False)
    pd.DataFrame([clean(summary)]).to_csv(output / "market_snapshot.csv", index=False)
    coverage_frame.to_csv(output / "coverage_by_clock.csv", index=False)
    unavailable.to_csv(output / "unavailable_stocks.csv", index=False)
    dump(output / "summary.json", summary)
    dump(output / "source_provenance.json", dict(universe=universe_proof, nifty_spot=nifty_proof, cash_sources=proofs,
        source_file_read_policy="ONE_BYTE_IMAGE_PER_SOURCE_HASHED_AND_PARSED_FROM_IDENTICAL_BYTES",
        unavailable=list(unavailable.to_dict("records")), code_sha256=sha(Path(__file__))))
    render(output, summary, stocks, unavailable, coverage_frame)
    dump(output / "manifest.json", dict(version=summary["version"], execution_authority=False,
        files={p.name:sha(p) for p in output.iterdir() if p.is_file()}))
    print(json.dumps(clean(dict(output=str(output), **summary)), indent=2), flush=True)
    return summary


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--day", type=date.fromisoformat, default=date(2026,11,2))
    parser.add_argument("--output",type=Path,default=PACKAGE/"current_20261102")
    args=parser.parse_args()
    run(str(args.day),args.output)


if __name__=="__main__":
    main()
