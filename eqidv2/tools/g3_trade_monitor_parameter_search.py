"""CLI entry point for isolated G3 monitor parameter research."""
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from research.g3_trade_monitor_parameter_search import main


if __name__ == "__main__":
    raise SystemExit(main())
