"""Current V23 CLI: adopted 80%-target breakeven baseline through 5 November."""
import argparse
from pathlib import Path

from .breakeven_baseline_20261106.run import OUTPUT, run


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, default=OUTPUT,
                        help='Fresh output directory within breakeven_baseline_20261106')
    run(parser.parse_args().output)


if __name__ == '__main__':
    main()
