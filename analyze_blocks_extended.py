import pandas as pd

df = pd.read_csv("parsed_blocks.csv")

def summarize_column(col):
    unique_vals = df[col].nunique()
    print(f"\n📌 {col}")
    print(f"Unique values: {unique_vals}")
    
    if unique_vals == 1:
        print("⚠️  Likely constant value")
    elif unique_vals <= 3:
        print("🧩 Possibly flags/enums")
    
    print("Top 5 most common:")
    print(df[col].value_counts().head(5))

def main():
    print("📊 Extended Field Analysis:")
    for column in df.columns:
        if column != "offset":
            summarize_column(column)

if __name__ == "__main__":
    main()
