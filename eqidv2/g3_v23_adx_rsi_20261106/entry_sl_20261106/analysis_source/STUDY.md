# Entry-relative stop data study

Only the first44 sessions (29 July–5 October2026) supply outcomes for this calibration. The next15 and final7 sessions are excluded from fitting. All history was previously observed in other research, so this is retrospective, not an untouched holdout.

Training sample: 496 trades, 356 native winners and 140 losers.

Native winner adverse excursion q95 is 0.868438%, q97.5 is 0.985388%. Upward0.05% rounding gives 0.90% and 1.00% fixed entry stop hypotheses.

The training ATR median is 0.12557993% of entry. Below/above this boundary, rounded winnerq95 stops are 0.70% / 1.00%. Training winnerMAE/ATRq95 is 6.220816, rounded upward to 6.25 ATR, with0.50% floor/1.25% cap.

These are hypotheses, not optimized profitable rules. They must be replayed against every baseline entry. Winner labels are used only to fit prior calibration quantiles; no current-trade outcome determines its stop.

## Excursion bounds

Exact intrabar MAE/MFE cannot be recovered from one-minute OHLC. The lower bound uses wholly post-entry/pre-exit bars plus the known exit price. The upper bound also includes entry/exit bar extremes; those extremes may precede entry or follow exit. Calibration uses the conservative upper bound to avoid understating the space a winner needed. Native stops censor loser excursions; no claim is made about their unrestricted future path.

## Entry features

One-minute simple rolling14 true range, inherited from native v9_1m_atr14; raw Nov4 clocks use the same G3/G7 formula. ATR is in price units; atr14_pct divides by actual entry price.

LONG: completed confirmation low; SHORT: completed confirmation high. Distance measured from actual native fill, available when fill occurs.

All790 entries have finitepositiveATR and completed confirmationOHLC.755 records come from verified native checkpoints,19 from the sealed Nov5 native source,9 from the Nov4 sealed feature ledger and7 from hashverified Nov4 stock prefixes.

[Training quantiles](training_quantiles.csv) · [Training excursion rows](training_excursions.csv) · [All entry-time features](entry_features.csv) · [Frozen fit](fit.json) · [Coverage](coverage.json)
