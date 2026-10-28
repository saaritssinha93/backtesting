"""Read-only normalization of pinned same-version research books for diagnosis."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path('C:/TradingData/eqidv2')
OUT = Path(__file__).resolve().parents[1] / 'outputs/october_regime_diagnosis_20261023'
SPECS = {
    'G3': (3, 'backtests/runs/2026-10-23/20261023T172057919135_0b57cd86_current_full_history', 'portfolio_trades.csv', []),
    'G4': (4, 'runs/2026-10-21/20261021T192354896814_1ce271c2_late_slots', 'g4_bounded_exit/portfolio_trades.csv', [
        ('daily/runs/2026-10-22/20261022T213111340459_24761024_g4_bounded_exit', 'portfolio_trades.csv'),
        ('daily/runs/2026-10-23/20261023_compare_6b96653a_g4', 'portfolio_trades.csv')]),
    'G6': (6, 'runs/2026-10-21/20261021T190634213322_1fc497f3_g6_nifty_futures', 'g6_nifty_futures_both_counter/portfolio_trades.csv', [
        ('daily/runs/2026-10-22/20261022T213111274162_4b42c575_g6_nifty_futures', 'g6_nifty_futures_both_counter/portfolio_trades.csv'),
        ('daily/runs/2026-10-23/20261023T183148480808_985a0e9e_g6_nifty_futures', 'g6_nifty_futures_both_counter/portfolio_trades.csv')]),
    'G7V2': (7, 'runs/2026-10-22/20261022T160000000000_g7_v2_restored_targets_slot_filters', 'g7_nifty_futures_both_counter/portfolio_trades.csv', [
        ('daily/runs/2026-10-22/20261022T213736388117_6f019403_g7_nifty_futures', 'g7_nifty_futures_both_counter/portfolio_trades.csv'),
        ('daily/runs/2026-10-23/20261023T183148514804_bbcb21e1_g7_nifty_futures', 'g7_nifty_futures_both_counter/portfolio_trades.csv')]),
    'G7V3': (7, 'runs/2026-10-22/20261022T171000000000_g7_v3_two_stage_exit', 'g7_nifty_futures_both_counter/portfolio_trades.csv', [
        ('daily/runs/2026-10-22/20261022T213111278672_4da7b1fa_g7_nifty_futures', 'g7_nifty_futures_both_counter/portfolio_trades.csv'),
        ('daily/runs/2026-10-23/20261023T183213971224_05933c99_g7_nifty_futures', 'g7_nifty_futures_both_counter/portfolio_trades.csv')]),
}


def read_json(path):
    return json.loads(path.read_text(encoding='utf-8-sig'))


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def load_all():
    books, provenance, calendars = [], [], {}
    for model, (number, historical, filename, extras) in SPECS.items():
        base = ROOT / f'backtesting_result_v13_v10_g_{number}'
        reference_config = read_json(base / historical / 'config.json')
        frames, dates = [], []
        for i, (relative, trade_file) in enumerate([(historical, filename)] + extras):
            run = base / relative
            summary = read_json(run / 'summary.json')
            config = read_json(run / 'config.json')
            assert config == reference_config, f'Config changed: {model} {run}'
            assert i == 0 or summary['complete'] is True
            days = summary.get('days', [summary.get('session_date')])
            assert days and all(days)
            assert not set(dates).intersection(days), f'Overlapping source sessions: {model}'
            dates.extend(days)
            manifest = read_json(run / 'manifest.json')
            for name in [trade_file, 'summary.json', 'config.json']:
                assert digest(run / name) == manifest['files'][name], f'Hash mismatch: {run / name}'
            frame = pd.read_csv(run / trade_file, low_memory=False)
            assert frame.day.astype(str).isin(days).all()
            frame['model'] = model
            frame['analysis_source_path'] = str(run / trade_file)
            frame['analysis_config_sha256'] = digest(run / 'config.json')
            frame['analysis_version'] = summary.get('strategy_version', summary.get('variant_id'))
            frame['month'] = frame.day.astype(str).str[:7]
            frame['win'] = frame.portfolio_net_profit_rupees.gt(0)
            frames.append(frame)
            provenance.append(dict(model=model, source=str(run / trade_file), source_sha256=digest(run / trade_file),
                                   manifest_sha256=digest(run / 'manifest.json'), version=frame.analysis_version.iloc[0],
                                   config_sha256=digest(run / 'config.json'), first_day=min(days), last_day=max(days), sessions=len(days),
                                   selected_rows=len(frame), config_payload_equal_to_historical=True,
                                   row_strategy_versions=sorted(frame.get('strategy_version', pd.Series(dtype=str)).dropna().unique().tolist()),
                                   row_strategy_fingerprints=sorted(frame.get('strategy_fingerprint', pd.Series(dtype=str)).dropna().unique().tolist())))
        book = pd.concat(frames, ignore_index=True)
        assert not book.duplicated(['day', 'setup_id', 'tradingsymbol', 'side']).any(), f'Duplicate trade keys: {model}'
        assert np.allclose(book.loc[book.portfolio_executed, 'portfolio_gross_profit_rupees'] - book.loc[book.portfolio_executed, 'portfolio_cost_rupees'],
                           book.loc[book.portfolio_executed, 'portfolio_net_profit_rupees'])
        books.append(book)
        calendars[model] = sorted(dates)
    assert len(set(tuple(x) for x in calendars.values())) == 1, 'Different session universes'
    return pd.concat(books, ignore_index=True), provenance, calendars


def stats(trades, dates=None):
    pnl = trades.portfolio_net_profit_rupees.astype(float)
    wins, losses = pnl[pnl > 0], pnl[pnl < 0]
    day_pnl = trades.groupby('day').portfolio_net_profit_rupees.sum()
    if dates is not None:
        day_pnl = day_pnl.reindex(dates, fill_value=0)
    return dict(trades=len(trades), wins=len(wins), losses=len(losses), breakeven=int(pnl.eq(0).sum()),
                win_rate_pct=100*pnl.gt(0).mean(), net_pnl_rs=pnl.sum(), profit_factor=wins.sum()/abs(losses.sum()) if len(losses) else np.nan,
                average_win_rs=wins.mean(), average_loss_rs=losses.mean(), average_trade_rs=pnl.mean(),
                stop_count=int(trades.exit_reason.astype(str).str.contains('STOP', case=False).sum()),
                stop_rate_pct=100*trades.exit_reason.astype(str).str.contains('STOP', case=False).mean(),
                stop_loss_count=int((trades.exit_reason.astype(str).str.contains('STOP', case=False) & pnl.lt(0)).sum()),
                sessions=len(day_pnl), losing_days=int(day_pnl.lt(0).sum()), losing_days_pnl_rs=day_pnl[day_pnl.lt(0)].sum(),
                gross_pnl_rs=trades.portfolio_gross_profit_rupees.sum(), cost_rs=trades.portfolio_cost_rupees.sum(),
                cost_bps_median=pd.to_numeric(trades.cost_pct, errors='coerce').median()*100)


def main():
    OUT.mkdir(exist_ok=True, parents=True)
    books, sources, calendars = load_all()
    books.to_csv(OUT / 'normalized_selected_books_g3_g7.csv', index=False)
    executed = books.loc[books.portfolio_executed].copy()
    executed.to_csv(OUT / 'normalized_executed_books_g3_g7.csv', index=False)
    pd.DataFrame(sources).to_csv(OUT / 'book_source_provenance.csv', index=False)
    rows = []
    for (model, month), group in executed.groupby(['model', 'month']):
        rows.append(dict(model=model, month=month, **stats(group, [d for d in calendars[model] if d.startswith(month)])))
    monthly = pd.DataFrame(rows)
    monthly.to_csv(OUT / 'monthly_results_g3_g7.csv', index=False)
    setup_rows = [dict(model=model, month=month, setup_id=setup, side=side, **stats(group))
                  for (model, month, setup, side), group in executed.groupby(['model', 'month', 'setup_id', 'side'])]
    pd.DataFrame(setup_rows).to_csv(OUT / 'monthly_setup_side_g3_g7.csv', index=False)
    pd.DataFrame([dict(day=d) for d in next(iter(calendars.values()))]).to_csv(OUT / 'common_calendar_g3_g7.csv', index=False)
    print(monthly.round(2).to_string(index=False))
    print('Output:', OUT)


if __name__ == '__main__':
    main()
