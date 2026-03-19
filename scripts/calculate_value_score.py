"""
calculate_value_score.py
------------------------
Calculates a composite Value Score for each Perth suburb
based on price, location, and amenity factors.

The Value Score helps identify undervalued suburbs —
those with good amenities and location at relatively
lower prices.

Scoring logic:
    - Lower median price        = higher score
    - Closer to CBD             = higher score
    - Closer to train station   = higher score
    - Better school rank        = higher score
    - More sales volume         = higher confidence

Output:
    data/processed/suburb_value_scores.csv
"""
import pandas as pd
import numpy as np
import os

print("Loading suburb summary...")
df = pd.read_csv("./data/processed/suburb_summary.csv")

# Remove suburbs with fewer than 10 sales — not enough data to be reliable
df = df[df["Total_Sales"] >= 10].copy()
print(f"Suburbs with 10+ sales: {len(df)}")

# ── NORMALISE EACH FACTOR TO 0-100 SCALE ──────────────────────────────────────
# Normalisation formula: (value - min) / (max - min) * 100
# This puts all factors on the same scale so they can be combined fairly

def normalise(series, invert=False):
    """
    Normalise a series to 0-100.
    invert=True means lower raw value = higher score
    (used for price, CBD distance — lower is better)
    """
    min_val = series.min()
    max_val = series.max()
    normalised = (series - min_val) / (max_val - min_val) * 100
    if invert:
        return 100 - normalised
    return normalised

# Price score — lower price = higher score (more affordable)
df["Price_Score"] = normalise(df["Median_Price"], invert=True)

# CBD distance score — closer = higher score
df["CBD_Score"] = normalise(df["Avg_CBD_Dist"], invert=True)

# Station distance score — closer = higher score
df["STN_Score"] = normalise(df["Avg_STN_Dist"], invert=True)

# School rank score — lower rank number = better school = higher score
# Fill any remaining NaN with median before scoring
df["Avg_SCH_Rank"] = df["Avg_SCH_Rank"].fillna(df["Avg_SCH_Rank"].median())
df["SCH_Score"] = normalise(df["Avg_SCH_Rank"], invert=True)

# ── COMPOSITE VALUE SCORE ─────────────────────────────────────────────────────
# Weighted combination of all factors
# Weights reflect what Perth buyers typically prioritise
df["Value_Score"] = (
    df["Price_Score"]   * 0.40 +   # Price is most important
    df["CBD_Score"]     * 0.25 +   # CBD proximity second
    df["STN_Score"]     * 0.20 +   # Public transport third
    df["SCH_Score"]     * 0.15     # School quality fourth
).round(2)

# ── VALUE TIER ────────────────────────────────────────────────────────────────
df["Value_Tier"] = pd.qcut(
    df["Value_Score"],
    q=4,
    labels=["Low Value", "Average Value", "Good Value", "Best Value"]
)

# ── RESULTS ───────────────────────────────────────────────────────────────────
print("\nTop 15 Best Value Suburbs:")
top15 = df.nlargest(15, "Value_Score")[
    ["SUBURB", "Value_Score", "Median_Price", "Avg_CBD_Dist",
     "Avg_STN_Dist", "Total_Sales", "Value_Tier"]
]
print(top15.to_string(index=False))

print("\nTop 15 Most Expensive Suburbs:")
top15_exp = df.nlargest(15, "Median_Price")[
    ["SUBURB", "Median_Price", "Value_Score", "Total_Sales"]
]
print(top15_exp.to_string(index=False))

print(f"\nValue Tier distribution:")
print(df["Value_Tier"].value_counts())

# ── SAVE ──────────────────────────────────────────────────────────────────────
os.makedirs("./data/processed", exist_ok=True)
df.to_csv("./data/processed/suburb_value_scores.csv", index=False)
print("\nSaved: data/processed/suburb_value_scores.csv")
print("\nDone.")
