"""Ten researched additions promoted unchanged to all G3 decision paths.

Prices below are effective thresholds, including SHORT rows. No further
legacy SHORT multiplier applies. Confirmation remains the exact next minute.
"""
VERSION = "V13-V10-G-3-PRODUCTION-V5"
ACCEPTED_VARIANT = "G3_W1_V1p1_0950_LONG_T075_0920_LONG_T300_LATE_SIX_SELECTED_TEN"
# signal, side, quota, picker, price, OI, 5m volume, 1m body, 1m wick, target
ROWS = (
    ("10:35", "LONG", 1, "max_liquidity", .10, .05, 1.3, .50, .50, 2.12),
    ("10:35", "SHORT", 1, "max_liquidity", .13, .05, 1., .30, .60, 2.86),
    ("10:40", "LONG", 1, "max_liquidity", .10, .05, 1.5, .50, .50, 1.71),
    ("10:45", "LONG", 1, "max_liquidity", .10, .05, 1.5, .50, .50, .79),
    ("10:45", "SHORT", 1, "max_liquidity", .13, .05, .8, .0, .60, 1.01),
    ("10:50", "LONG", 1, "max_liquidity", .10, .05, .8, .0, .50, .77),
    ("10:55", "SHORT", 1, "max_liquidity", .13, .05, 1., .0, 1., 1.53),
    ("11:10", "LONG", 1, "max_liquidity", .10, .15, 1.3, .50, .50, 1.76),
    ("11:10", "SHORT", 1, "max_liquidity", .13, .05, 1., .30, .60, 1.84),
    ("11:25", "SHORT", 1, "max_liquidity", .13, .05, 1., .0, 1., 4.20),
)
CONFIRMATION_VOLUME = {
    "1036_LONG": .6, "1036_SHORT": .4, "1041_LONG": .6,
    "1046_LONG": .6, "1046_SHORT": .8, "1051_LONG": .4,
    "1056_SHORT": .8, "1111_LONG": .6, "1111_SHORT": .8, "1126_SHORT": .4,
}
SETUP_IDS = frozenset(CONFIRMATION_VOLUME)
TARGETS = dict(zip(CONFIRMATION_VOLUME, (2.12, 2.86, 1.71, .79, 1.01, .77, 1.53, 1.76, 1.84, 4.20)))
