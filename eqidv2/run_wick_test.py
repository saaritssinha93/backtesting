"""Quick wick threshold comparison test."""
import sys, os, warnings
warnings.filterwarnings('ignore')
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from eqidv5_strategy_common import default_long_config, default_short_config, compute_backtest_metrics
from eqidv5_combined_runner import _run_side_parallel, _add_notional_pnl
import pandas as pd

NTICKERS = 400  # per side

def test(name, **overrides):
    cfg_l = default_long_config(dir_15m='stocks_indicators_15min_eq', dir_5m='stocks_indicators_5min_eq', **overrides)
    cfg_s = default_short_config(dir_15m='stocks_indicators_15min_eq', dir_5m='stocks_indicators_5min_eq', **overrides)
    dl = _run_side_parallel('LONG',  cfg_l, 2, NTICKERS)
    ds = _run_side_parallel('SHORT', cfg_s, 2, NTICKERS)
    parts = [p for p in [dl, ds] if not p.empty]
    if not parts:
        print(f'{name}: No trades')
        return
    df = pd.concat(parts, ignore_index=True)
    df = _add_notional_pnl(df)
    m = compute_backtest_metrics(df)
    print(
        f'{name}: L={len(dl) if not dl.empty else 0}+S={len(ds) if not ds.empty else 0}'
        f'={m.total_trades}t, tgt={m.hit_rate_pct:.1f}%, sl={m.sl_rate_pct:.1f}%, '
        f'be={m.be_rate_pct:.1f}%, pnl/t={m.avg_pnl_pct:.3f}%, PF={m.profit_factor:.3f}'
    )

if __name__ == "__main__":
    BASE = dict(
        require_5m_confirmation=False, require_adx_filter=False,
        require_ema_alignment=False, require_rsi_filter=False,
        sweep_vol_ratio=1.80, min_quality_score=5,
    )

    print("=== Wick threshold tests ===")
    test('base(wick>=0.35)',    **BASE)
    test('wick>=0.7',           **BASE, sweep_min_wick_atr_mult=0.7)
    test('wick>=1.0',           **BASE, sweep_min_wick_atr_mult=1.0)
    test('wick>=1.2',           **BASE, sweep_min_wick_atr_mult=1.2)
    test('wick>=1.0+slope',     **BASE, sweep_min_wick_atr_mult=1.0, require_avwap_slope=True)
    test('wick>=1.0+cq60',      **BASE, sweep_min_wick_atr_mult=1.0, require_close_quality=True, close_quality_min_pct=0.60)
    test('wick>=1.0+cq65',      **BASE, sweep_min_wick_atr_mult=1.0, require_close_quality=True, close_quality_min_pct=0.65)
    test('wick>=1.0+slope+cq',  **BASE, sweep_min_wick_atr_mult=1.0, require_avwap_slope=True,
         require_close_quality=True, close_quality_min_pct=0.60)
    test('wick>=1.0+vp',        **BASE, sweep_min_wick_atr_mult=1.0, require_vp_confluence=True)
    test('wick>=1.0+q6',        **BASE, sweep_min_wick_atr_mult=1.0, min_quality_score=6)
    test('wick>=1.0+q7',        **BASE, sweep_min_wick_atr_mult=1.0, min_quality_score=7)
    print("=== Done ===")
