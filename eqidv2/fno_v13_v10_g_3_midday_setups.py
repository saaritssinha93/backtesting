"""The twelve approved midday setups shared by all G3 decision paths.

Thresholds are effective values; SHORT prices receive no legacy multiplier.
Importing this module performs no file, clock, broker or network access.
"""
VERSION = "V13-V10-G-3-PRODUCTION-V6"
ACCEPTED_VARIANT = "G3_W1_V1p1_0950_LONG_T075_0920_LONG_T300_LATE_SIX_SELECTED_TEN_MIDDAY_TWELVE"
# signal, side, quota, picker, price, OI, 5m volume, 1m body, 1m wick, target
ROWS = (
    ('11:35', 'LONG', 1, 'max_liquidity', 0.1, 0.05, 1.3, 0.5, 0.5, 7.15),
    ('11:35', 'SHORT', 1, 'max_liquidity', 0.13, 0.05, 1.0, 0.15, 0.6, 0.87),
    ('11:40', 'LONG', 1, 'max_liquidity', 0.1, 0.05, 1.3, 0.5, 0.5, 4.03),
    ('11:40', 'SHORT', 1, 'max_liquidity', 0.13, 0.05, 1.0, 0.0, 1.0, 1.19),
    ('11:45', 'SHORT', 1, 'max_liquidity', 0.13, 0.05, 1.0, 0.0, 0.6, 4.95),
    ('12:05', 'LONG', 1, 'max_liquidity', 0.1, 0.05, 1.3, 0.7, 0.5, 1.4),
    ('12:10', 'SHORT', 1, 'max_liquidity', 0.13, 0.05, 1.0, 0.3, 0.6, 0.79),
    ('12:15', 'LONG', 1, 'max_liquidity', 0.1, 0.05, 1.3, 0.5, 0.5, 1.24),
    ('12:15', 'SHORT', 1, 'max_liquidity', 0.1, 0.05, 1.0, 0.15, 0.75, 3.49),
    ('12:25', 'SHORT', 1, 'max_liquidity', 0.13, 0.05, 1.0, 0.15, 0.6, 2.13),
    ('12:30', 'LONG', 1, 'max_liquidity', 0.1, 0.05, 1.3, 0.5, 0.5, 2.53),
    ('12:30', 'SHORT', 1, 'max_liquidity', 0.13, 0.05, 1.0, 0.0, 0.75, 1.85),
)
CONFIRMATION_VOLUME = {
    '1136_LONG': 1.0,
    '1136_SHORT': 0.4,
    '1141_LONG': 0.0,
    '1141_SHORT': 1.2,
    '1146_SHORT': 0.0,
    '1206_LONG': 0.6,
    '1211_SHORT': 0.0,
    '1216_LONG': 1.0,
    '1216_SHORT': 0.0,
    '1226_SHORT': 0.0,
    '1231_LONG': 0.6,
    '1231_SHORT': 0.4,
}
TARGETS = {
    '1136_LONG': 7.15,
    '1136_SHORT': 0.87,
    '1141_LONG': 4.03,
    '1141_SHORT': 1.19,
    '1146_SHORT': 4.95,
    '1206_LONG': 1.4,
    '1211_SHORT': 0.79,
    '1216_LONG': 1.24,
    '1216_SHORT': 3.49,
    '1226_SHORT': 2.13,
    '1231_LONG': 2.53,
    '1231_SHORT': 1.85,
}
STAGES = {
    '1136_LONG': 'strict',
    '1136_SHORT': 'relax_1m',
    '1141_LONG': 'relax_1m',
    '1141_SHORT': 'relax_1m',
    '1146_SHORT': 'relax_1m',
    '1206_LONG': 'strict',
    '1211_SHORT': 'relax_1m',
    '1216_LONG': 'strict',
    '1216_SHORT': 'relax_5m',
    '1226_SHORT': 'relax_1m',
    '1231_LONG': 'strict',
    '1231_SHORT': 'relax_1m',
}
SETUP_IDS = frozenset(CONFIRMATION_VOLUME)
