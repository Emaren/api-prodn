# analyze_anomalies.py
import pandas as pd

df = pd.read_csv("parsed_blocks_with_deltas.csv")

# Filter where int1 ≠ int4
anomalies = df[df["int1"] != df["int4"]]

# Print all anomalous rows
print("\n🔍 Anomalous Blocks (int1 ≠ int4):\n")
print(anomalies.to_string(index=False))

# Optional: Save to CSV for deeper analysis
anomalies.to_csv("anomalous_blocks.csv", index=False)
print("\n✅ Exported anomalous blocks to anomalous_blocks.csv")

