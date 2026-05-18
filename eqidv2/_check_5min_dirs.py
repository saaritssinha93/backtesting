import pandas as pd
import os

for d in [
    r'C:\TradingData\eqidv2\stocks_indicators_5min_eq',
    r'C:\TradingData\eqidv2\stocks_indicators_5min_eq_live',
    r'C:\TradingData\eqidv2\stocks_indicators_5min_eq_live2',
    r'C:\TradingData\eqidv2\stocks_indicators_5min_eq_pending',
]:
    f = os.path.join(d, 'RELIANCE_stocks_indicators_5min.parquet')
    if os.path.exists(f):
        df = pd.read_parquet(f, columns=['date'])
        days = df['date'].dt.date.nunique()
        print(f"{d}: {len(df)} rows, days={days}, range={df['date'].min()} -> {df['date'].max()}")
    else:
        print(f"{d}: missing")
