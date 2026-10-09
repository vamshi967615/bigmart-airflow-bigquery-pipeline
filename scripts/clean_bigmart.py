import pandas as pd

df = pd.read_csv("dags/data/BigMart_Sales.csv")

# 1. Fix inconsistent fat-content labels
df["Item_Fat_Content"] = df["Item_Fat_Content"].replace(
    {"LF": "Low Fat", "low fat": "Low Fat", "reg": "Regular"}
)

# 2. Fill missing weights: item average first, then overall average
df["Item_Weight"] = df.groupby("Item_Identifier")["Item_Weight"].transform(
    lambda s: s.fillna(s.mean())
)
df["Item_Weight"] = df["Item_Weight"].fillna(df["Item_Weight"].mean())

# 3. Fill missing outlet size
df["Outlet_Size"] = df["Outlet_Size"].fillna("Unknown")

# 4. New column: outlet age
df["Outlet_Age"] = 2026 - df["Outlet_Establishment_Year"]

print("Rows:", len(df))
df.to_csv("dags/data/train_clean.csv", index=False)
print("Saved dags/data/train_clean.csv")