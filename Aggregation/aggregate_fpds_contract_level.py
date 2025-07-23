#!/usr/bin/env python3
"""
aggregate_fpds_contract_level.py
--------------------------------
Convert FPDS *contract‑action* shards to a *contract‑level* dataset.

Quick start
-----------
$ python aggregate_fpds_contract_level.py

Outputs fpds_contract_level.parquet in the current directory.
"""

import glob, os, sys
import pandas as pd
from tqdm import tqdm

# ======== CUSTOMISE HERE ===========================================
INPUT_DIR   = "/data/fpds_data"                 # where monthly shards live
OUTPUT_FILE = "/data/fpds_contract_level.parquet"

GROUP_KEYS  = ["content.ID.ContractID.PIID"]  # define a “contract”
NUM_FCN     = "sum"                       # sum | mean | max | min | median
OBJ_FCN     = "first"                     # first | last | mode
# ===================================================================

def main():
    shards = sorted(glob.glob(os.path.join(INPUT_DIR, "*.parquet")))
    if not shards:
        sys.exit(f"No Parquet files found in {INPUT_DIR}")

    # Peek at first file for dtypes
    sample = pd.read_parquet(shards[0])
    numeric_cols = [
        c for c, dt in sample.dtypes.items()
        if pd.api.types.is_numeric_dtype(dt) and c not in GROUP_KEYS
    ]
    object_cols = [c for c in sample.columns
                   if c not in GROUP_KEYS + numeric_cols]

    agg_map = {c: NUM_FCN for c in numeric_cols}
    agg_map.update({c: OBJ_FCN for c in object_cols})

    partials = []
    for shard in tqdm(shards, desc="Aggregating shards"):
        cols_needed = GROUP_KEYS + numeric_cols + object_cols
        df = pd.read_parquet(shard, columns=cols_needed)
        partials.append(df.groupby(GROUP_KEYS).agg(agg_map))

    combined = pd.concat(partials).groupby(level=list(range(len(GROUP_KEYS)))).agg(agg_map)
    combined.to_parquet(OUTPUT_FILE)
    print(f"Wrote {len(combined):,} contracts → {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
