import os
import pandas as pd
from rapidfuzz import fuzz
import logging

# Set up logging
logging.basicConfig(filename='facility_matching.log', level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')

# input paths
main_file = "/home/kyl29/CSE_MSE_RXF131/staging/casf/kyl29/anaerobic_digestion_facilities/WRRF_anaerobic_digestion_facilities.csv"
facility_dir = "/home/kyl29/CSE_MSE_RXF131/staging/casf/kyl29/facility_data/"

# Load WWRF filtered facilities
logging.info("Loading WWRF filtered facilities...")
adf = pd.read_csv(main_file, engine="python")
logging.info(f"Loaded {len(adf)} facilities.")

# Output
match_results = []

# Loop through each state directory
logging.info("Beginning to process state directories...")
for state_dir in os.listdir(facility_dir):
    state_path = os.path.join(facility_dir, state_dir)
    if os.path.isdir(state_path):
        logging.info(f"Processing state directory: {state_dir}")
        
        # Loop through CSV files in each state directory
        for file in os.listdir(state_path):
            if file.startswith("2025_") and file.endswith(".csv"):
                path = os.path.join(state_path, file)
                logging.info(f"Processing file: {path}")
                
                # Read CSV, skipping metadata lines
                if os.path.getsize(path) > 0:
                    df = pd.read_csv(path, engine='python', skiprows=3)
                    logging.info(f"Loaded {len(df)} entries from {file}.")
                    
                    coord_threshold = 0.003
                    address_threshold = 80

                    for idx, row in adf.iterrows():
                        logging.info(f"Matching ADF entry {idx + 1}/{len(adf)}: {row['Name']}")
                        
                        address = str(row.get("Address", "")).upper()
                        city = str(row.get("City", "")).upper()
                        state = str(row.get("State", "")).upper()
                        target_address = f"{address}, {city}, {state}"

                        target_lat = float(row.get("Latitude", 0))
                        target_lon = float(row.get("Longitude", 0))

                        for idx2, row2 in df.iterrows():
                            address_to_compare = (str(row2.get("Street Address", "")).upper() + ', ' + 
                                                  str(row2.get("City", "")).upper() + ', ' + 
                                                  str(row2.get("State", "")).upper())
                            lat_diff = abs(target_lat - float(row2.get("Facility Latitude", 0)))
                            lon_diff = abs(target_lon - float(row2.get("Facility Longitude", 0)))
                            address_score = fuzz.ratio(target_address, address_to_compare)
                            
                            if lat_diff < coord_threshold and lon_diff < coord_threshold and address_score > address_threshold:
                                name_score = fuzz.token_sort_ratio(str(row['Name']).upper(), str(row2['Facility Name']).upper())
                                logging.info(f"Match found: {row['Name']} with {row2['Facility Name']} (Score: {name_score})")
                                
                                match_results.append({
                                    "ADF Name": row["Name"],
                                    "ADF Address": target_address,
                                    "ADF Zip": row.get("Zip Code", ""),
                                    "ADF Latitude": row.get("Latitude", ""),
                                    "ADF Longitude": row.get("Longitude", ""),
                                    
                                    "Matched Name": row2["Facility Name"],
                                    "Matched Address": address_to_compare,
                                    "Matched Zip": row2.get("ZIP Code", ""),
                                    "Matched Latitude": row2.get("Facility Latitude", ""),
                                    "Matched Longitude": row2.get("Facility Longitude", ""),
                                    
                                    "Name Fuzzy Score": name_score,
                                    "Address Fuzzy Score": address_score,
                                    "Source File": file
                                })
                else:
                    logging.info(f"Skipped empty file: {path}")

# Save matches to CSV
logging.info("Saving match results to CSV...")
results_df = pd.DataFrame(match_results)
results_df.to_csv("wrrf_fuzzy_matches.csv", index=False)
logging.info("Matching complete. Results saved to wrrf_fuzzy_matches.csv")