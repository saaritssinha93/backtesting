"""Audited, read-only broker downloads for the authorized dashboard repair."""
import json
import logging
import shutil
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
import pandas as pd
import fno_oi_common as common
import fno_v13_v5_derivative_data as derivative
import trading_data_continous_run_historical_alltf_v3_parquet_stocksonly_1min as fetcher

out = Path(__file__).resolve().parent
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("dashboard-repair")
universe = pd.read_parquet(common.UNIVERSE_DIR / "near_month_2026-09-15.parquet")
client = common.make_kite_client(common.discover_kite_credentials()[0])
audit = []
for symbol in ("IDEA", "LTM"):
    row = universe.loc[universe.equity_symbol.eq(symbol)].iloc[0]
    path = Path(fetcher.DIRS["1min"]["out"]) / f"{symbol}_stocks_indicators_1min.parquet"
    backup = out / path.name
    if not backup.exists():
        shutil.copy2(path, backup)
    result = fetcher.process_ticker(
        "1min", symbol, int(row.equity_instrument_token), client,
        fetcher.IST_TZ.localize(datetime(2026, 9, 1, 9, 15)),
        fetcher.IST_TZ.localize(datetime(2026, 9, 15, 15, 30)),
        logger, common.load_holidays(), False, "end", str(out), False, 0,
    )
    audit.append({"symbol": symbol, "result": vars(result), "backup": str(backup)})
    if result.status == "failed":
        raise RuntimeError(f"Repair failed for {symbol}")
for symbol in ("KAYNES", "PIDILITIND"):
    row = universe.loc[universe.equity_symbol.eq(symbol)].iloc[0]
    records = client.historical_data(int(row.instrument_token), "2026-09-15 09:15:00", "2026-09-15 09:30:00", "5minute", oi=True)
    audit.append({"symbol": symbol, "broker_records": records, "note": "No synthetic opening candle inserted"})
(out / "input_repair_audit.json").write_text(json.dumps(audit, default=str, indent=2))
source = common.FNO_ROOT / "strategy_research/v13_corrected_v5/higher_frequency/fno_v13_corrected_v5_higher_frequency_trades.csv"
trades = derivative.load_filled_trades(source)
trades = trades.loc[trades.day.astype(str).between("2026-09-09", "2026-09-11")]
trades.to_csv(out / "v5_missing_options_source_trades.csv", index=False)
print("Prepared missing option inputs:", len(trades), flush=True)
