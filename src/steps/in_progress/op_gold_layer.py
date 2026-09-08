from pathlib import Path
import os
import time
import pandas as pd
import numpy as np

# ==============================================================================
# 1. DYNAMIC PATH CONFIGURATION (Pathlib)
# ==============================================================================

script_dir = Path(__file__).resolve().parent
project_root = script_dir.parent.parent

data_dir = project_root / "data"

# Input path (Silver operational dataset)
silver_dir = data_dir / "silver" / "operational" / "2026"
silver_file = silver_dir / "operational_2026_sanitized_unified.parquet"

# Output path (Gold operational dataset)
gold_dir = data_dir / "gold" / "operational" / "2026"
gold_dir.mkdir(parents=True, exist_ok=True)

output_parquet = gold_dir / "operational_2026_gold.parquet"
output_xlsx = gold_dir / "operational_2026_gold.xlsx"

if not silver_file.exists():
    raise FileNotFoundError(f"Input Silver file not found: {silver_file}")

overall_start_time = time.time()

# ==============================================================================
# 2. PROCESSING OPERATIONAL INDICATORS
# ==============================================================================

print("\n" + "=" * 70)
print("STARTING GOLD LAYER PROCESSING: OPERATIONAL INDICATORS")
print("=" * 70)

print(f"Reading Silver database from: {silver_file.name}...")
df_silver = pd.read_parquet(silver_file)
df_silver.columns = df_silver.columns.str.strip().str.lower()

# Ensure correct numeric data types for calculations
df_silver["reference_year"] = pd.to_numeric(df_silver["reference_year"], errors="coerce").fillna(0).astype(int)
df_silver["reference_month"] = pd.to_numeric(df_silver["reference_month"], errors="coerce").fillna(0).astype(int)
df_silver["bodywork_year"] = pd.to_numeric(df_silver["bodywork_year"], errors="coerce")
df_silver["total_line_km"] = pd.to_numeric(df_silver["total_line_km"], errors="coerce").fillna(0.0)

# Calculate vehicle age safely handling NA values
valid_bodywork_mask = df_silver["bodywork_year"].fillna(0) > 1900
df_silver["vehicle_age"] = np.where(
    valid_bodywork_mask,
    df_silver["reference_year"] - df_silver["bodywork_year"],
    np.nan
)

print("Extracting operational KPIs by Company, Year, and Month...")

group_cols = ["sgti_owner_code", "sgti_owner_name", "reference_year", "reference_month"]

# 1. Distinct fleet age per vehicle used in the month
df_unique_fleet = (
    df_silver[df_silver["normalized_plate"].astype(bool) & df_silver["vehicle_age"].notna()]
    .drop_duplicates(subset=group_cols + ["normalized_plate"])
    .groupby(group_cols)["vehicle_age"]
    .mean()
    .reset_index()
    .rename(columns={"vehicle_age": "avg_fleet_age"})
)

# 2. Main KPI Aggregations
df_kpis = (
    df_silver.groupby(group_cols)
    .agg(
        vehicles_used_count=("normalized_plate", lambda x: x[x != ""].nunique()),
        trips_performed_count=("service_id", "count"),
        total_km_produced=("total_line_km", "sum"),
        services_operated_count=("service_id", lambda x: x[x != ""].nunique())
    )
    .reset_index()
)

# Merge aggregated metrics with average fleet age
df_gold = pd.merge(df_kpis, df_unique_fleet, on=group_cols, how="left")

# Format and round indicators
df_gold["avg_fleet_age"] = df_gold["avg_fleet_age"].fillna(0.0).round(2)
df_gold["total_km_produced"] = df_gold["total_km_produced"].round(2)

# Rename final schema to clear English standards
df_gold = df_gold.rename(
    columns={
        "sgti_owner_code": "company_code",
        "sgti_owner_name": "company_name",
        "reference_year": "year",
        "reference_month": "month",
    }
)

# Reorder columns
ordered_cols = [
    "company_code",
    "company_name",
    "year",
    "month",
    "vehicles_used_count",
    "avg_fleet_age",
    "trips_performed_count",
    "total_km_produced",
    "services_operated_count",
]
df_gold = df_gold[ordered_cols]

# ==============================================================================
# 3. EXPORTING GOLD DATASET
# ==============================================================================

print("\nExporting Gold dataset...")
df_gold.to_parquet(output_parquet, index=False)

with pd.ExcelWriter(output_xlsx, engine="openpyxl") as writer:
    df_gold.to_excel(writer, sheet_name="Gold_Operational", index=False)

execution_time = time.time() - overall_start_time

print("\n" + "=" * 70)
print("GOLD OPERATIONAL DATASET GENERATED SUCCESSFULLY!")
print(f"Parquet Output : {output_parquet}")
print(f"Excel Output   : {output_xlsx}")
print(f"Total Rows     : {len(df_gold):,}")
print(f"Execution Time : {execution_time:.2f} seconds")
print("=" * 70)