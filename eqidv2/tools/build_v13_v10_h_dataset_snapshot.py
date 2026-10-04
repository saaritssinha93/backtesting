"""Build a cutoff-scoped V13-v9 dataset from a stable futures archive copy.

The wrapper changes only this process's source directory pointers. It leaves
the live futures directory and all original G outputs untouched.
"""
from __future__ import annotations

import argparse
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import fno_oi_common as common
import fno_v13_corrected_v5_backtest as v5
import fno_v13_v9_data as data


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--futures-snapshot', type=Path, required=True)
    parser.add_argument('--output-dir', type=Path, required=True)
    parser.add_argument('--through-day', required=True)
    args = parser.parse_args()
    snapshot = args.futures_snapshot.resolve()
    if not snapshot.is_dir() or not any(snapshot.glob('NIFTY*FUT_5minute.parquet')):
        raise ValueError('Snapshot lacks NIFTY futures history')
    common.RAW_CONTRACT_DIR = snapshot
    v5.v13_v3.NIFTY_ROOT = snapshot
    result = data.build_dataset(args.output_dir, args.through_day)
    print(f"Sealed dataset through {args.through_day}: {result['output_dir']}; "
          f"{len(result['days'])} eligible sessions")
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
