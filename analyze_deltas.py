import pandas as pd

df = pd.read_csv("parsed_blocks.csv")

# Compute differences and equality checks
df["int1_minus_int4"] = df["int1"] - df["int4"]
df["byte1_equals_byte2"] = df["byte1"] == df["byte2"]

# Summary
print("\n🔍 Delta Analysis:\n")

print("📌 int1 - int4:")
print(df["int1_minus_int4"].value_counts().head(10))

print("\n📌 byte1 == byte2:")
print(df["byte1_equals_byte2"].value_counts())

# Optional: export for inspection
df.to_csv("parsed_blocks_with_deltas.csv", index=False)
print("\n✅ Exported to parsed_blocks_with_deltas.csv")
