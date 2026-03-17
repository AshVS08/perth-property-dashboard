"""
get_abs_data.py
---------------
Fetches Perth LGA population data from the ABS API
and saves clean CSVs for Power BI.

Output:
    data/raw/abs_population_all.csv  - All Australian LGAs
    data/raw/abs_population_wa.csv   - Perth LGAs only
"""
import requests
import pandas as pd
import os

ABS_URL = "https://api.data.abs.gov.au/data/ABS,ERP_LGA2023,1.0.0/all"

params = {
    "startPeriod": "2018",
    "endPeriod": "2023",
    "format": "jsondata"
}

headers = {
    "Accept": "application/vnd.sdmx.data+json;version=1.0"
}

PERTH_LGAS = [
    "Armadale", "Bassendean", "Bayswater", "Belmont", "Cambridge",
    "Canning", "Claremont", "Cockburn", "Cottesloe", "East Fremantle",
    "Fremantle", "Gosnells", "Joondalup", "Kalamunda", "Kwinana",
    "Mandurah", "Melville", "Mosman Park", "Mundaring", "Nedlands",
    "Peppermint Grove", "Perth", "Rockingham", "Serpentine-Jarrahdale",
    "South Perth", "Stirling", "Subiaco", "Swan", "Victoria Park",
    "Vincent", "Wanneroo", "Western Australia"
]

print("Fetching ABS population data...")
response = requests.get(ABS_URL, params=params, headers=headers, timeout=120)
data = response.json()

structure = data["data"]["structure"]

# Extract dimension labels
region_dim = next(d for d in structure["dimensions"]["series"] if d["id"] == "REGION")
region_labels = {str(i): v["name"] for i, v in enumerate(region_dim["values"])}
region_ids = {str(i): v["id"] for i, v in enumerate(region_dim["values"])}

time_dim = next(d for d in structure["dimensions"]["observation"] if d["id"] == "TIME_PERIOD")
time_labels = {str(i): v["id"] for i, v in enumerate(time_dim["values"])}

# Parse observations
series = data["data"]["dataSets"][0]["series"]
records = []

for series_key, series_data in series.items():
    region_idx = series_key.split(":")[2]
    region_name = region_labels.get(region_idx, "Unknown")
    region_id = region_ids.get(region_idx, "Unknown")

    for obs_key, obs_value in series_data["observations"].items():
        year = time_labels.get(obs_key, "Unknown")
        population = obs_value[0]
        records.append({
            "Region_ID": region_id,
            "Region": region_name,
            "Year": year,
            "Population": population
        })

df = pd.DataFrame(records)
wa_df = df[df["Region"].isin(PERTH_LGAS)]

# Save outputs
os.makedirs(os.path.join(".", "data", "raw"), exist_ok=True)
df.to_csv(os.path.join(".", "data", "raw", "abs_population_all.csv"), index=False)
wa_df.to_csv(os.path.join(".", "data", "raw", "abs_population_wa.csv"), index=False)

print(f"Total records: {len(df)}")
print(f"Perth LGA records: {len(wa_df)}")
print(f"Perth LGAs found: {sorted(wa_df['Region'].unique())}")
print("\nSaved:")
print("  data/raw/abs_population_all.csv")
print("  data/raw/abs_population_wa.csv")
print("\nDone.")