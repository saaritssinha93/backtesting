"""V2-owned port of the v16/v17 5-minute AVWAP cascade.

The modules in this package are copied from the old root-level v17 cascade and
rewired to import each other locally.  This lets v2 evolve the cascade without
depending on or mutating the original v17 files.
"""
