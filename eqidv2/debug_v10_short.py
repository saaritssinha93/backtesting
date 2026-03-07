# debug_v10_short.py - test SHORT scan on a skipped ticker
import sys
import traceback
sys.path.insert(0, r'C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2')

try:
    from avwap_v11_refactored.avwap_short_strategy_v10_sweep import scan_all_days_for_ticker
    from avwap_v11_refactored.avwap_common_v7_sweep import default_short_config, read_15m_parquet
    import os

    cfg = default_short_config()
    for ticker in ["BAJFINANCE", "RELIANCE", "CIPLA", "TITAN"]:
        try:
            path = os.path.join(cfg.dir_15m, ticker + cfg.end_15m)
            df = read_15m_parquet(path, cfg.parquet_engine)
            print(f"{ticker}: df.shape={df.shape}, cols={list(df.columns[:8])}")
            result = scan_all_days_for_ticker(ticker, df, cfg)
            print(f"  -> trades={len(result)}")
        except Exception as e:
            print(f"  -> EXCEPTION for {ticker}:")
            traceback.print_exc()
        print()
except Exception as e:
    traceback.print_exc()
