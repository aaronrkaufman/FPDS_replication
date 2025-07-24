To make the data usable at the contract—rather than contract‑action—level we supply two alternative aggregation scripts and the Parquet file they generate.

aggregate_fpds_contract_level.py processes each monthly shard in sequence, groups by content.ID.ContractID.PIID, sums numeric fields, keeps the first non‑null categorical values and writes a single file, fpds_contract_level.parquet. On a laptop with ≤ 16 GB RAM it completes in 25–40 minutes.

aggregate_fpds_duckdb.py (the recommended, multithreaded DuckDB rewrite) performs the same operations but does the grouping inside DuckDB, collapses duplicates across all shards in one pass, and finishes in about a minute on an SSD.

Both scripts output a contract‑level Parquet, which is available in Figshare. Users can tweak the grouping key or aggregation functions by editing the short configuration block at the top of either script. The 
