# EQIDV2 — AVWAP + Meta-Label ML Filter

This folder is the `eqidv2` upgrade of `eqidv1` with deployable ML gating and confidence sizing.

## What's new
- `ml_meta_filter.py`: fast `p_win` inference layer with model/fallback heuristic.
- `avwap_live_signal_generator.py`: emits `p_win`, `ml_threshold`, `confidence_multiplier`; skips weak trades.
- `avwap_trade_execution_PAPER_TRADE_TRUE.py`: records ML fields in paper-trade logs.
- `eqidv2_ml_backtest.py`: filters historical trade CSVs using the same ML decision logic.

## Example commands
```bash
python eqidv2/avwap_live_signal_generator.py --ml-threshold 0.62
python eqidv2/avwap_trade_execution_PAPER_TRADE_TRUE.py
python eqidv2/eqidv2_ml_backtest.py --input-csv eqidv1/outputs/avwap_longshort_trades_ALL_DAYS_20260213_145432.csv
```

## Optional training
If you have a labeled candidate-trade dataset (`label` column, triple-barrier outcome):
```python
from eqidv2.ml_meta_filter import train_logreg_meta_model
train_logreg_meta_model(
    dataset_csv="candidate_trades.csv",
    out_model_path="eqidv2/models/meta_model.pkl",
    out_features_path="eqidv2/models/meta_features.json",
)
```
