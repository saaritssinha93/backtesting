"""Wrapper: runs deep optimizer on all tickers. Stdout redirected by bat file."""
import sys, os
_base = os.path.dirname(os.path.abspath(__file__))
os.chdir(_base)
sys.path.insert(0, _base)

from eqidv5_deep_optimizer import run_deep_sweep
run_deep_sweep(
    dir15m="stocks_indicators_15min_eq",
    workers=6,
    max_tickers=0,   # ALL tickers
    out_csv=os.path.join(_base, "outputs_eqidv5", "deep_optimizer_results.csv"),
)
