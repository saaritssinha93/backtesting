from __future__ import annotations

import json
from dataclasses import asdict, dataclass, replace
from pathlib import Path
from typing import Any, Iterable


DEFAULT_BUDGET_GRID = (100, 150, 200, 250, 300, 350, 400)


@dataclass(frozen=True)
class PrefilterConfig:
    """Configuration for the deterministic shadow ranker.

    Defaults are intentionally recall-oriented.  They are research defaults,
    not approved production thresholds.
    """

    budget: int = 400
    budget_grid: tuple[int, ...] = DEFAULT_BUDGET_GRID
    lookback_bars: int = 48
    min_bars: int = 1
    feature_min_observations: int = 8
    max_staleness_seconds: float = 420.0
    min_price_rs: float = 1.0
    min_median_traded_value_rs: float = 0.0
    long_stream_fraction: float = 0.45
    short_stream_fraction: float = 0.45
    activity_stream_fraction: float = 0.10
    carryover_fraction: float = 0.0
    exploration_fraction: float = 0.0

    def validate(self) -> "PrefilterConfig":
        if self.budget <= 0:
            raise ValueError("budget must be positive")
        if not self.budget_grid or any(int(k) <= 0 for k in self.budget_grid):
            raise ValueError("budget_grid must contain positive integers")
        if self.lookback_bars < self.min_bars:
            raise ValueError("lookback_bars must be >= min_bars")
        if self.min_bars < 1:
            raise ValueError("min_bars must be >= 1")
        if self.feature_min_observations < self.min_bars:
            raise ValueError("feature_min_observations must be >= min_bars")
        if self.max_staleness_seconds < 0:
            raise ValueError("max_staleness_seconds must be non-negative")
        fractions = (
            self.long_stream_fraction,
            self.short_stream_fraction,
            self.activity_stream_fraction,
            self.carryover_fraction,
            self.exploration_fraction,
        )
        if any(value < 0 or value > 1 for value in fractions):
            raise ValueError("allocation fractions must be between 0 and 1")
        stream_total = (
            self.long_stream_fraction
            + self.short_stream_fraction
            + self.activity_stream_fraction
        )
        if not 0.999 <= stream_total <= 1.001:
            raise ValueError("long/short/activity stream fractions must total 1.0")
        if self.carryover_fraction + self.exploration_fraction > 0.20:
            raise ValueError("carryover plus exploration must total <= 0.20")
        return self

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["budget_grid"] = list(self.budget_grid)
        return payload

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "PrefilterConfig":
        values = dict(payload)
        if "budget_grid" in values:
            values["budget_grid"] = _normalise_budgets(values["budget_grid"])
        return cls(**values).validate()

    @classmethod
    def from_json(cls, path: str | Path) -> "PrefilterConfig":
        with Path(path).open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
        if not isinstance(payload, dict):
            raise ValueError("config JSON must contain an object")
        return cls.from_dict(payload)

    def with_overrides(
        self,
        *,
        budget: int | None = None,
        budget_grid: Iterable[int] | None = None,
    ) -> "PrefilterConfig":
        updated = replace(
            self,
            budget=self.budget if budget is None else int(budget),
            budget_grid=(
                self.budget_grid
                if budget_grid is None
                else _normalise_budgets(budget_grid)
            ),
        )
        return updated.validate()


def _normalise_budgets(values: Iterable[int]) -> tuple[int, ...]:
    budgets = sorted({int(value) for value in values if int(value) > 0})
    if not budgets:
        raise ValueError("at least one positive budget is required")
    return tuple(budgets)
