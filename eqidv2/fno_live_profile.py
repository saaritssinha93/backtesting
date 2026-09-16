"""Select the strategy behind stable FnO runtime transport generations."""
from __future__ import annotations

import importlib
import os

G_PROFILE = 'V13_V10_G'
PROFILE_ENV = 'FNO_V6_STRATEGY_PROFILE'


def config_for_generation(generation: str):
    generation = str(generation).strip().lower()
    if generation not in ('v5', 'v6'):
        raise ValueError(f'Unsupported FnO transport generation: {generation}')
    profile = os.getenv(PROFILE_ENV, '').strip().upper()
    if generation == 'v6' and profile:
        if profile != G_PROFILE:
            raise ValueError(f'Unsupported FnO V6 strategy profile: {profile}')
        return importlib.import_module('fno_v13_v10_g_live_config')
    return importlib.import_module(f'fno_{generation}_live_config')


def is_g_config(config) -> bool:
    return getattr(config, 'STRATEGY_PROFILE', '') == G_PROFILE
