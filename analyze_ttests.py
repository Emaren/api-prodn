import pandas as pd
from scipy.stats import ttest_ind

# Load CSV file
df = pd.read_csv("parsed_blocks_with_deltas.csv")

# Split into anomalous and normal blocks
anomalous = df[df["int1_minus_int4"] != 0]
normal = df[df["int1_minus_int4"] == 0]

# Fields to test
fields = ["int1", "int2", "float1", "float2", "byte1", "byte2", "int3", "int4", "float3"]

# Compare fields with t-tests
for field in fields:
    a_values = anomalous[field].dropna()
    n_values = normal[field].dropna()

    if not a_values.empty and not n_values.empty:
        t_stat, p_val = ttest_ind(a_values, n_values, equal_var=False)
        print(f"\n📌 {field}:\n"
              f"  Anomalous mean: {a_values.mean():.4e}\n"
              f"  Normal mean:    {n_values.mean():.4e}\n"
              f"  p-value:        {p_val:.4e} {'(Significant)' if p_val < 0.05 else ''}")

