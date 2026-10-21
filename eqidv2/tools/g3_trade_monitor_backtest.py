"""Run isolated fixed-policy G3 advisory-exit research."""
from pathlib import Path
import sys

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from research.g3_trade_monitor_backtest import main


if __name__ == "__main__":
    raise SystemExit(main())
