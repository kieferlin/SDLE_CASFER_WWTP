import pandas as pd

# input path
main_file = "/home/kyl29/CSE_MSE_RXF131/staging/casf/kyl29/anaerobic_digestion_facilities/1_filtering_WRRF_adf.csv"
adf = pd.read_csv(main_file, engine="python")

# filter for only water resource recovery facilities
adf_filtered = adf[adf["Facility Type"].str.strip().str.upper() == "WATER RESOURCE RECOVERY FACILITY"]

# save  filtered data to a new CSV
filtered_output = "WRRF_anaerobic_digestion_facilities.csv"
adf_filtered.to_csv(filtered_output, index=False)
print(f"Filtered data saved to {filtered_output}")