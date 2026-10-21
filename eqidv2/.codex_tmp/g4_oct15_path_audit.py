from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pandas as pd
from g3_one_minute_extension import last_features, extension_atr

ROOT = Path(r'C:\TradingData\eqidv2\backtesting_result_v13_v10_g_4\runs\2026-10-16\20261016T125839866379_53348fb1_late_slots')
SNAP = Path(r'C:\TradingData\eqidv2\backtesting_result_v13_v10_g_3\production\runs\2026-10-15\20261015T170545010772_39928700\input_snapshots\31d412ab5cc6555b3ee3843af1b03168f5482ddbc031b1ebb2fbb19d1d5694cc')
orders = pd.read_csv(ROOT / 'g4_bounded_exit' / 'portfolio_trades.csv')
orders = orders.loc[orders.day.eq('2026-10-15') & orders.filled.eq(True)]
nifty = pd.read_parquet(SNAP / 'futures_5m' / 'NIFTY26OCTFUT_5minute.parquet')
nifty['timestamp'] = pd.to_datetime(nifty.timestamp)
nifty = nifty.loc[nifty.timestamp.dt.date.eq(pd.Timestamp('2026-10-15').date())]
initial_nifty = float(nifty.iloc[0].open)
print('NIFTY FUT first open', initial_nifty, '15:15 close', float(nifty.loc[nifty.timestamp.eq(pd.Timestamp('2026-10-15 15:15', tz='Asia/Kolkata')), 'close'].iloc[0]))

for row in orders.sort_values('entry_ts').itertuples():
    frame = pd.read_parquet(SNAP / 'equity_1m' / f'{row.tradingsymbol}_stocks_indicators_1min.parquet')
    frame['date'] = pd.to_datetime(frame.date)
    confirm = pd.Timestamp(row.confirmation_ts)
    confirm_bar = frame.loc[frame.date.eq(confirm)].iloc[0]
    ema, atr = last_features(frame.loc[frame.date.lt(confirm)], confirm_bar.to_dict())
    extension = extension_atr(confirm_bar.close, ema, atr, row.side)
    frame = frame.loc[frame.date.dt.date.eq(pd.Timestamp('2026-10-15').date())]
    entry = pd.Timestamp(row.entry_ts)
    exit = pd.Timestamp(row.exit_ts)
    sign = 1 if row.side == 'LONG' else -1
    prior = frame.loc[frame.date.lt(entry)]
    during = frame.loc[frame.date.ge(entry) & frame.date.le(exit)]
    after = frame.loc[frame.date.gt(exit) & frame.date.le(pd.Timestamp('2026-10-15 15:15', tz='Asia/Kolkata'))]
    def ext(part):
        if part.empty:
            return None
        favorable = part.high if sign == 1 else part.low
        adverse = part.low if sign == 1 else part.high
        f = sign * (favorable / row.entry_price - 1) * 100
        a = sign * (adverse / row.entry_price - 1) * 100
        return dict(best_pct=round(float(f.max()), 3), best_time=str(part.loc[f.idxmax(), 'date']),
                    worst_pct=round(float(a.min()), 3), worst_time=str(part.loc[a.idxmin(), 'date']))
    latest = nifty.loc[nifty.timestamp.le(entry)].iloc[-1]
    later = nifty.loc[nifty.timestamp.le(exit)].iloc[-1]
    first_close = frame.iloc[0].close
    setup_return = sign * (row.entry_price / first_close - 1) * 100
    eod_return = sign * (float(frame.loc[frame.date.eq(pd.Timestamp('2026-10-15 15:15', tz='Asia/Kolkata')), 'close'].iloc[0]) / row.entry_price - 1) * 100
    print('\n', row.setup_id, row.tradingsymbol, row.side, 'entry', entry, 'exit', exit, row.exit_reason)
    print('confirmation_extension_atr', round(extension,3), 'confirmation_high_low',row.confirmation_high,row.confirmation_low)
    print('entry_vs_first_1m_close_pct', round(setup_return, 3), 'nifty_from_open_entry_pct', round(100*(float(latest.close)/initial_nifty-1),3),
          'nifty_from_open_exit_pct', round(100*(float(later.close)/initial_nifty-1),3))
    print('during', ext(during), 'after', ext(after), 'eod_if_held_pct', round(eod_return,3))
    print('prior 5 closes', prior.tail(5)[['date','close','volume']].to_dict('records'))
