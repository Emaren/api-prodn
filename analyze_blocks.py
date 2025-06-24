import pandas as pd
import matplotlib.pyplot as plt

# Load the CSV file (assumed to be in your project root)
csv_file = "parsed_blocks.csv"
df = pd.read_csv(csv_file)

# Print descriptive statistics for each numeric column
print("Descriptive Statistics:")
print(df.describe())

# List the columns we want to analyze
columns = ["int1", "int2", "float1", "float2", "byte1", "byte2", "int3", "int4", "float3"]

# Plot a histogram for each column (each plot in its own figure)
for col in columns:
    plt.figure()
    df[col].hist(bins=10)
    plt.title(f"Histogram of {col}")
    plt.xlabel(col)
    plt.ylabel("Frequency")
    plt.show()
