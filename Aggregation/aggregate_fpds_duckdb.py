#!/usr/bin/env python3
"""
aggregate_fpds_duckdb.py
------------------------
Aggregates FPDS contract-action-level data to the contract level using DuckDB.
Handles complex nested columns including '#text'.

This script reads all monthly Parquet shards using DuckDB, groups rows by 
`content.ID.ContractID.PIID`, and:
- Sums numeric fields (e.g., content.totalDollarValues.totalObligatedAmount)
- Selects the first non-null value for categorical fields
- Performs a final aggregation across all PIIDs to ensure one row per contract

Output: /data/fpds_contract_level.parquet
"""

import duckdb
import os
import time

# ========== CONFIGURATION ==========
PARQUET_DIR = "/data/fpds_data/*.parquet"
OUTPUT_FILE = "/data/fpds_contract_level.parquet"

GROUP_KEY = "content.ID.ContractID.PIID"

NUM_COLS = [
    "content.totalDollarValues.totalObligatedAmount"
]

OBJ_COLS = [
    "content.contractData.typeOfContractPricing.#text",
    "content.vendor.vendorHeader.vendorName"
]
# ===================================

def q(col: str) -> str:
    """Ensure complex dotted column names are quoted properly."""
    return f'"{col}"'

def main():
    start = time.time()
    con = duckdb.connect(database=":memory:")
    con.execute(f"PRAGMA threads={os.cpu_count()}")

    # Build aggregation expressions
    agg_exprs = [
        f"SUM(TRY_CAST({q(c)} AS DOUBLE)) AS {c.split('.')[-1]}_sum"
        for c in NUM_COLS
    ]
    agg_exprs += [
        f"FIRST({q(c)}) AS {c.split('.')[-1].replace('#', '_')}" for c in OBJ_COLS
    ]

    # Aggregate all shards in one query and perform final grouping across PIIDs
    query = f"""
    COPY (
        SELECT
            {q(GROUP_KEY)} AS piid,
            {', '.join(agg_exprs)}
        FROM read_parquet('{PARQUET_DIR}')
        GROUP BY piid
    )
    TO '{OUTPUT_FILE}' (FORMAT PARQUET, COMPRESSION 'SNAPPY');
    """

    con.execute(query)

    # Count resulting rows
    rows = con.execute(
        f"SELECT COUNT(*) FROM read_parquet('{OUTPUT_FILE}')"
    ).fetchone()[0]

    print(f"\n✅ Wrote {rows:,} contracts → {OUTPUT_FILE}")
    print(f"⏱️  Finished in {(time.time() - start)/60:.1f} minutes\n")

if __name__ == "__main__":
    main()
