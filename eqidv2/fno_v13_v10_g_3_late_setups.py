"""The six late setups explicitly selected for the G3 V4 promotion.

Short price thresholds are already effective G4 thresholds: do not apply the
legacy 0.65 expansion multiplier to these rows. Core and expanded rules are
identical for the six additions, preserving the G4 liquidity ranking.
"""
VERSION = "V13-V10-G-3-PRODUCTION-V4"
ACCEPTED_VARIANT = "G3_W1_V1p1_0950_LONG_T075_0920_LONG_T300_LATE_SIX"
# signal, side, quota, picker, price, OI, 5m volume, body, wick, target
ROWS = (
    ("10:05", "LONG", 1, "max_liquidity", .10, .05, 1.3, .70, .50, .56),
    ("10:10", "SHORT", 1, "max_liquidity", .13, .05, 1., .40, .60, 1.56),
    ("10:15", "LONG", 1, "max_liquidity", .30, .05, 1.3, .50, .50, .96),
    ("10:15", "SHORT", 1, "max_liquidity", .13, .05, 1., .40, .60, .68),
    ("10:25", "SHORT", 1, "max_liquidity", .13, .05, 1., .40, .60, 2.85),
    ("10:30", "SHORT", 1, "max_liquidity", .13, .05, 1., .40, .60, 1.66),
)
SETUP_IDS = frozenset({"1006_LONG", "1011_SHORT", "1016_LONG", "1016_SHORT", "1026_SHORT", "1031_SHORT"})
CONFIRMATION_VOLUME = {"1006_LONG": .60, "1016_LONG": .60}
TARGETS = dict(zip(("1006_LONG", "1011_SHORT", "1016_LONG", "1016_SHORT", "1026_SHORT", "1031_SHORT"),
                   (.56, 1.56, .96, .68, 2.85, 1.66)))
