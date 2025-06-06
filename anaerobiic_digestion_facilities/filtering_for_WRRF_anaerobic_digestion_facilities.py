import pandas as pd

# Input path
main_file = "/home/kyl29/CSE_MSE_RXF131/staging/casf/kyl29/anaerobic_digestion_facilities/AnaerobicDigestionFacilities.csv"

# Load and filter the main file
adf = pd.read_csv(main_file, engine="python")

# Filter by facility type
adf_filtered = adf[adf["Facility Type"].str.strip().str.upper() == "WATER RESOURCE RECOVERY FACILITY"]

# Save the filtered data to a new CSV file
filtered_output = "WRRF_anaerobic_digestion_facilities.csv"
adf_filtered.to_csv(filtered_output, index=False)
print(f"Filtered data saved to {filtered_output}")