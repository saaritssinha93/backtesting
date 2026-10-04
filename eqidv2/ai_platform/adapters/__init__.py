"""Side-effect-free readers for existing V13-V10-G artifacts."""

from .daily_replay import read_daily_replay
from .equity_orders import preferred_filled_equity, read_equity_orders
from .evidence import list_evidence, select_evidence
from .options_orders import read_option_orders
from .strategy import StrategyContract, read_strategy_contract
from .snapshot import read_coherent_snapshot
from .status import inspect_status

__all__ = [
    "StrategyContract",
    "list_evidence",
    "preferred_filled_equity",
    "read_daily_replay",
    "read_equity_orders",
    "read_option_orders",
    "read_strategy_contract",
    "read_coherent_snapshot",
    "inspect_status",
    "select_evidence",
]
