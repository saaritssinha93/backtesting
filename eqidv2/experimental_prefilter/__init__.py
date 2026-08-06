"""Standalone experimental universe pre-filter.

This package is deliberately independent of the V7 live and V11 backtesting
packages.  It consumes read-only market snapshots and emits research-only
rankings; importing it has no runtime side effects.
"""

from .config import PrefilterConfig
from .engine import build_features, rank_universe, select_candidates

__all__ = [
    "PrefilterConfig",
    "build_features",
    "rank_universe",
    "select_candidates",
]

__version__ = "0.1.0"
