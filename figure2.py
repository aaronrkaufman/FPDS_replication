import pandas as pd
import glob
import matplotlib.pyplot as plt

# Change this path if your Parquet files are in a different location
FILES = glob.glob("/data/fpds_data/*.parquet")

all_counts = []

# 1. Loop through each file and group by Contract ID
for f in FILES:
    try:
        # Load only the Contract ID column to save memory
        df = pd.read_parquet(f, columns=["content.ID.ContractID.PIID"])
        # Group and count how many rows belong to each Contract ID
        counts_this_file = df.groupby("content.ID.ContractID.PIID").size()
        all_counts.append(counts_this_file)
        print(f"Processed {f}")
    except Exception as e:
        print(f"Error processing {f}: {e}")

# 2. Combine counts from all files
#    We'll sum the partial counts across files to get total counts per Contract ID
if not all_counts:
    print("No files or no data loaded!")
    exit()

full_counts = pd.concat(all_counts).groupby(level=0).sum()

# 3. Convert to a simple array of row counts
counts_array = full_counts.values

# 4. Make a histogram
#    If you have an extreme range, you can choose bins carefully or switch to a log scale
plt.figure(figsize=(8, 6))
# For example, we can set bins=100 or a range
plt.hist(counts_array, bins=range(1,101), edgecolor='black')

plt.title("Histogram of the Number of Rows per Contract ID\n(FPDS Dataset)")
plt.xlabel("Rows per Contract ID (truncated at 100)")
plt.ylabel("Frequency (count of Contract IDs)")

# If you suspect many IDs have more than 100 actions, the last bin gets truncated
# Alternatively, remove 'bins=range(1,101)' to let matplotlib auto-bin fully
# or do: bins=[1,2,3,4,5,10,20,50,100,500,1000,5000,10000]

plt.yscale('log')  # Optional: log-scale the y-axis if the distribution is skewed

plt.tight_layout()
plt.savefig("/data/histogram_rows_per_id.png")
plt.close()

print("Done! Wrote /data/histogram_rows_per_id.png")
print("Number of unique Contract IDs:", len(full_counts))
print("Max # of rows for a single Contract ID:", counts_array.max())
