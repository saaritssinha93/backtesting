r'''run_recovery.py — thin wrapper; heavy logic in _shared/run_setup_recovery.py.
Run from repo root:
  py -3.12 Train_and_Test\\setup_from_scratch_recovery_loop\\B_AVWAP_RECLAIM_REVERSAL\\scripts\\run_recovery.py
'''
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / '_shared'))
import run_setup_recovery
raise SystemExit(run_setup_recovery.main('B_AVWAP_RECLAIM_REVERSAL'))
