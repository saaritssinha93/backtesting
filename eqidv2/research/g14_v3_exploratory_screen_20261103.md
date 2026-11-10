# V14 six-field V3 exploratory screen: no candidate to promote

Reviewed 2026-11-03. Source: the completed [V14 V2 full-pool run](C:/TradingData/eqidv2/backtesting_result_v13_v10_g_14/runs/2026-11-03/six_feature_full_pool_v2_20261103/README.md), 63 sessions from 2026-07-29 through 2026-11-02. This document is a **negative screening result**, not a V3 backtest or a new production configuration. V14 V1 and V2 are unchanged.

## Full V2 replay, 9 bps round-trip costs

| Rule | July–Sep trades | July–Sep win % | July–Sep net ₹ | July–Sep max daily-close drawdown ₹ | Oct–Nov 2 trades | Oct–Nov 2 win % | Oct–Nov 2 net ₹ | Oct–Nov 2 max daily-close drawdown ₹ |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| V14 V1 comparator | 51 | 78.4 | +104,908 | 16,872 | 22 | 72.7 | +26,526 | 9,683 |
| A direction only | 351 | 63.2 | +26,893 | 152,963 | 166 | 48.8 | −212,455 | 220,992 |
| B with gap | 320 | 62.8 | +1,674 | 112,906 | 161 | 54.0 | −140,623 | 169,175 |
| C three-path OR | 352 | 63.1 | +23,091 | 152,963 | 166 | 48.8 | −212,455 | 220,992 |
| D strict OR | 329 | 64.1 | +27,284 | 136,210 | 156 | 50.6 | −176,144 | 184,681 |

The 20-session assessment remains negative at both 5 and 15 bps for every V2 six-field rule. At 5 bps the rule losses range from ₹108,500 to ₹179,348; at 15 bps they range from ₹181,756 to ₹243,165. The V14 V1 comparator remains positive at both costs. This is a large degradation in both profit and drawdown, with many more trades than the comparator.

## Development-only exploratory score-floor screen

I inspected **July–September executed V2 trades only** at 9 bps using fixed score floors `{0, 0.40, 0.50, 0.60, 0.70, 0.80, 0.90, 0.95, 0.99, 1.00}`. This was an outcome-informed exploratory screen, **not** the pre-frozen V2 protocol. It merely removes executed trades below a floor from the existing V2 book. It does **not** replay freed capital, position conflicts or the two-loss lock, and cannot be reported as a valid strategy result.

No screened floor produced both at least 30 retained development trades and 70% observed wins. The strongest simple directional example, A with score ≥0.80, retained **275** development trades, **66.2%** wins and **+₹67,508** in this non-replayed 9 bps proxy. It still trailed V14 V1's +₹104,908 and 78.4% on development. A B-rule floor of 1.00 retained 43 development trades, 69.8% wins and only +₹2,214. More seriously, a score of 1.00 is **mathematically impossible at 09:20** because opening OI-day change is zero and has 10% score weight; such a floor silently removes the opening slot.

After identifying these two illustrative floors from development, a fixed October–November 2 **non-replayed diagnostic** showed A≥0.80 retaining 143 executed V2 trades, 49.7% wins and −₹182,357. B≥1.00 retained 21, 61.9% and +₹1,778. Neither supports a six-field replacement for the V14 V1 comparator. The diagnostic amounts are deliberately **not** called V3 backtest P&L, since a true portfolio replay can change entries and exits.

## Decision

Do not create or adopt a V3 score-floor strategy from this screen. The development proxy does not reach 70% with a meaningful trade count, the V2 chronological assessment is substantially worse than V14 V1, and a floor capable of making the book very small can disable a requested slot by construction. A fresh six-field hypothesis would need a new frozen rule and a full minute/portfolio replay, then future sessions for a genuinely new assessment. The existing V14 rules and live/paper settings remain unchanged.

The October history and strategy family have already been studied repeatedly, so even the chronological October assessment here is **not a virgin holdout**. The 70% objective cannot be guaranteed by any threshold, particularly when raising the observed win percentage discards many profitable trades.
