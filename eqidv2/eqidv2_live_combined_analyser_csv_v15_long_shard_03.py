# -*- coding: utf-8 -*-
from __future__ import annotations

from eqidv2_live_combined_analyser_csv_v15_long_sharded import (
    configure_v15_long_shard,
    run_v15_long_shard_main,
)


SHARD_ID = 3
SHARD_COUNT = 10


def configure():
    return configure_v15_long_shard(SHARD_ID, SHARD_COUNT)


def main() -> None:
    run_v15_long_shard_main(SHARD_ID, SHARD_COUNT)


if __name__ == '__main__':
    main()
