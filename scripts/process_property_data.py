"""
process_property_data.py
------------------------
Cleans and processes the Perth property sales dataset
for Power BI dashboard consumption.

Outputs:
    data/processed/suburb_summary.csv       - Median prices by suburb
    data/processed/yearly_trends.csv        - Price trends over time
    data/processed/property_clean.csv       - Full cleaned dataset
"""
import pandas as pd
import os

print("Loading data...")
df = pd.read_csv("./data/raw/all_perth_310121.csv")

# ── 1. CLEAN ──────────────────────────────────────────────────────────────────

# Parse date and extract year/month
df["DATE_SOLD"] = pd.to_datetime(df["DATE_SOLD"], dayfirst=True, errors="coerce")
df["YEAR"] = df["DATE_SOLD"].dt.year
df["MONTH"] = df["DATE_SOLD"].dt.month
df["YEAR_MONTH"] = df["DATE_SOLD"].dt.to_period("M").astype(str)

# Drop rows with missing critical fields
df = df.dropna(subset=["DATE_SOLD", "PRICE", "SUBURB"])

# Remove outliers — prices below $100k or above $3M
df = df[(df["PRICE"] >= 100000) & (df["PRICE"] <= 3000000)]

# Fill missing values
df["GARAGE"] = df["GARAGE"].fillna(0)
df["BUILD_YEAR"] = df["BUILD_YEAR"].fillna(df["BUILD_YEAR"].median())
df["NEAREST_SCH_RANK"] = df["NEAREST_SCH_RANK"].fillna(df["NEAREST_SCH_RANK"].median())

print(f"Clean dataset: {len(df)} records")
print(f"Date range: {df['DATE_SOLD'].min()} to {df['DATE_SOLD'].max()}")
print(f"Suburbs: {df['SUBURB'].nunique()}")

# ── 2. SUBURB SUMMARY ─────────────────────────────────────────────────────────

suburb_summary = df.groupby("SUBURB").agg(
    Median_Price=("PRICE", "median"),
    Mean_Price=("PRICE", "mean"),
    Total_Sales=("PRICE", "count"),
    Avg_Bedrooms=("BEDROOMS", "mean"),
    Avg_Land_Area=("LAND_AREA", "mean"),
    Avg_CBD_Dist=("CBD_DIST", "mean"),
    Avg_STN_Dist=("NEAREST_STN_DIST", "mean"),
    Avg_SCH_Rank=("NEAREST_SCH_RANK", "mean"),
    Latitude=("LATITUDE", "mean"),
    Longitude=("LONGITUDE", "mean"),
).reset_index()

suburb_summary["Price_Category"] = pd.cut(
    suburb_summary["Median_Price"],
    bins=[0, 400000, 600000, 800000, 1200000, 9999999],
    labels=["Under $400K", "$400K-$600K", "$600K-$800K", "$800K-$1.2M", "Over $1.2M"]
)

print(f"\nSuburbs summarised: {len(suburb_summary)}")
print("\nTop 10 most expensive suburbs:")
print(suburb_summary.nlargest(10, "Median_Price")[["SUBURB", "Median_Price", "Total_Sales"]])

# ── 3. YEARLY TRENDS ──────────────────────────────────────────────────────────

yearly_trends = df.groupby(["SUBURB", "YEAR"]).agg(
    Median_Price=("PRICE", "median"),
    Total_Sales=("PRICE", "count"),
).reset_index()

# Calculate year-on-year price change
yearly_trends = yearly_trends.sort_values(["SUBURB", "YEAR"])
yearly_trends["Prev_Year_Price"] = yearly_trends.groupby("SUBURB")["Median_Price"].shift(1)
yearly_trends["YoY_Change_Pct"] = (
    (yearly_trends["Median_Price"] - yearly_trends["Prev_Year_Price"])
    / yearly_trends["Prev_Year_Price"] * 100
).round(2)

print(f"\nYearly trend records: {len(yearly_trends)}")

# ── 4. MONTHLY TRENDS ─────────────────────────────────────────────────────────

monthly_trends = df.groupby("YEAR_MONTH").agg(
    Median_Price=("PRICE", "median"),
    Total_Sales=("PRICE", "count"),
).reset_index()

monthly_trends = monthly_trends.sort_values("YEAR_MONTH")

print(f"Monthly trend records: {len(monthly_trends)}")

# ── 5. SAVE OUTPUTS ───────────────────────────────────────────────────────────

os.makedirs("./data/processed", exist_ok=True)

df.to_csv("./data/processed/property_clean.csv", index=False)
suburb_summary.to_csv("./data/processed/suburb_summary.csv", index=False)
yearly_trends.to_csv("./data/processed/yearly_trends.csv", index=False)
monthly_trends.to_csv("./data/processed/monthly_trends.csv", index=False)

print("\nSaved:")
print("  data/processed/property_clean.csv")
print("  data/processed/suburb_summary.csv")
print("  data/processed/yearly_trends.csv")
print("  data/processed/monthly_trends.csv")
print("\nDone.")