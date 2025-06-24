import pandas as pd

# Load parsed data
df = pd.read_csv("parsed_blocks.csv")

# Drop non-numeric or irrelevant columns if needed
df = df.drop(columns=["offset"], errors="ignore")

# Compute Pearson correlation
correlation_matrix = df.corr(numeric_only=True)

# Print full matrix
print("\n📊 Correlation Matrix (Pearson):")
print(correlation_matrix.round(2))

# Highlight strong correlations
print("\n🔍 Strong Correlations (|r| ≥ 0.8):")
threshold = 0.8
for col in correlation_matrix.columns:
    for row in correlation_matrix.index:
        if row != col:
            r = correlation_matrix.loc[row, col]
            if abs(r) >= threshold:
                print(f"{row} ⟷ {col}: {r:.2f}")

