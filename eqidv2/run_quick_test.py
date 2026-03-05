"""Quick sanity test with all improvements: ATR targets + smart exit + wick>=1.0."""
import sys, os, warnings
warnings.filterwarnings('ignore')
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import pandas as pd
from eqidv5_strategy_common import default_long_config, default_short_config, compute_backtest_metrics, print_metrics
from eqidv5_combined_runner import _run_side_parallel, _add_notional_pnl


def _pnl_rs(df):
    if df.empty or 'pnl_rs' not in df.columns:
        return 0.0
    return float(pd.to_numeric(df['pnl_rs'], errors='coerce').fillna(0).sum())


if __name__ == "__main__":
    BASE = dict(
        dir_15m='stocks_indicators_15min_eq',
        dir_5m='stocks_indicators_5min_eq',
        require_5m_confirmation=False,
        require_adx_filter=False,
        require_ema_alignment=False,
        require_rsi_filter=False,
        sweep_vol_ratio=1.80,
        min_quality_score=5,
    )

    print("=== Testing optimized config: wick>=1.0 + cq + slope (500 tickers per side) ===")
    cfg_l = default_long_config(**BASE, sweep_min_wick_atr_mult=1.0,
                                 require_close_quality=True, close_quality_min_pct=0.55,
                                 require_avwap_slope=True)
    cfg_s = default_short_config(**BASE, sweep_min_wick_atr_mult=1.0,
                                  require_close_quality=True, close_quality_min_pct=0.55,
                                  require_avwap_slope=True)

    dl = _run_side_parallel('LONG',  cfg_l, 2, 500)
    ds = _run_side_parallel('SHORT', cfg_s, 2, 500)

    parts = [p for p in [dl, ds] if not p.empty]
    if parts:
        if not dl.empty:
            dl = _add_notional_pnl(dl)
        if not ds.empty:
            ds = _add_notional_pnl(ds)
        df = pd.concat([p for p in [dl, ds] if not p.empty], ignore_index=True)
        m = compute_backtest_metrics(df)
        print_metrics("C7_optimized_500t", m)
        print(f"  Notional Rs: LONG={int(_pnl_rs(dl)):+,}  SHORT={int(_pnl_rs(ds)):+,}")
        print(f"  Long: {len(dl) if not dl.empty else 0} trades  |  Short: {len(ds) if not ds.empty else 0} trades")
    else:
        print("No trades")
