"""The fifteen approved afternoon setups shared by all G3 decision paths.

Effective thresholds and targets are copied from the sealed G4 dual-70 result.
SHORT prices receive no legacy multiplier. Import performs no I/O.
"""
VERSION = 'V13-V10-G-3-PRODUCTION-V7'
ACCEPTED_VARIANT = 'G3_W1_V1p1_0950_LONG_T075_0920_LONG_T300_LATE_SIX_SELECTED_TEN_MIDDAY_TWELVE_AFTERNOON_FIFTEEN'
SOURCE_CONFIG_SHA256 = '5f244666ad68fd2d2d6ac6243f328a324b9363f676b30d7ae5a177246843b434'
# signal, side, quota, picker, price, OI, 5m volume, 1m body, 1m wick, target
ROWS = (
    ('12:35', 'LONG', 1, 'max_liquidity', 0.1, 0.05, 1.3, 0.3, 0.5, 0.37),
    ('12:35', 'SHORT', 1, 'max_liquidity', 0.1, 0.05, 0.9, 0.15, 1.0, 1.24),
    ('12:45', 'LONG', 1, 'max_liquidity', 0.1, 0.05, 1.3, 0.0, 0.75, 0.23),
    ('12:45', 'SHORT', 1, 'max_liquidity', 0.13, 0.05, 1.0, 0.15, 0.75, 0.49),
    ('12:50', 'LONG', 1, 'max_liquidity', 0.1, 0.05, 1.3, 0.0, 0.5, 0.26),
    ('12:50', 'SHORT', 1, 'max_liquidity', 0.1, 0.05, 0.9, 0.0, 0.75, 0.14),
    ('12:55', 'SHORT', 1, 'max_liquidity', 0.13, 0.05, 1.0, 0.15, 0.6, 0.14),
    ('13:00', 'SHORT', 1, 'max_liquidity', 0.1, 0.05, 0.8, 0.0, 0.75, 0.88),
    ('13:05', 'LONG', 1, 'max_liquidity', 0.1, 0.05, 1.0, 0.3, 0.75, 0.18),
    ('13:15', 'LONG', 1, 'max_liquidity', 0.1, 0.05, 1.0, 0.0, 0.5, 0.17),
    ('13:15', 'SHORT', 1, 'max_liquidity', 0.18, 0.07, 1.2, 0.5, 0.5, 0.33),
    ('13:20', 'SHORT', 1, 'max_liquidity', 0.13, 0.05, 1.0, 0.15, 0.6, 1.01),
    ('13:25', 'LONG', 1, 'max_liquidity', 0.1, 0.05, 1.3, 0.0, 0.75, 0.41),
    ('13:25', 'SHORT', 1, 'max_liquidity', 0.13, 0.05, 1.0, 0.4, 0.6, 0.76),
    ('13:30', 'LONG', 1, 'max_liquidity', 0.15, 0.07, 1.4, 0.55, 0.45, 1.17),
)
CONFIRMATION_VOLUME = {
    '1236_LONG': 0.2,
    '1236_SHORT': 0.0,
    '1246_LONG': 0.0,
    '1246_SHORT': 0.8,
    '1251_LONG': 0.6,
    '1251_SHORT': 0.8,
    '1256_SHORT': 0.4,
    '1301_SHORT': 0.8,
    '1306_LONG': 0.0,
    '1316_LONG': 0.6,
    '1316_SHORT': 1.3,
    '1321_SHORT': 0.0,
    '1326_LONG': 0.2,
    '1326_SHORT': 1.2,
    '1331_LONG': 0.8,
}
TARGETS = {
    '1236_LONG': 0.37,
    '1236_SHORT': 1.24,
    '1246_LONG': 0.23,
    '1246_SHORT': 0.49,
    '1251_LONG': 0.26,
    '1251_SHORT': 0.14,
    '1256_SHORT': 0.14,
    '1301_SHORT': 0.88,
    '1306_LONG': 0.18,
    '1316_LONG': 0.17,
    '1316_SHORT': 0.33,
    '1321_SHORT': 1.01,
    '1326_LONG': 0.41,
    '1326_SHORT': 0.76,
    '1331_LONG': 1.17,
}
STAGES = {
    '1236_LONG': 'relax_1m',
    '1236_SHORT': 'relax_5m',
    '1246_LONG': 'relax_1m',
    '1246_SHORT': 'relax_1m',
    '1251_LONG': 'relax_1m',
    '1251_SHORT': 'relax_5m',
    '1256_SHORT': 'relax_1m',
    '1301_SHORT': 'relax_5m',
    '1306_LONG': 'relax_5m',
    '1316_LONG': 'relax_5m',
    '1316_SHORT': 'strict',
    '1321_SHORT': 'relax_1m',
    '1326_LONG': 'relax_1m',
    '1326_SHORT': 'strict',
    '1331_LONG': 'strict',
}
SETUP_IDS = frozenset(CONFIRMATION_VOLUME)
