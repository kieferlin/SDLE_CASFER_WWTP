import os
import sys
import pandas as pd
from rapidfuzz import fuzz
import logging
import shutil # used for copying the file

# find root logger and remove any handlers it already has
root_logger = logging.getLogger()
for handler in root_logger.handlers[:]:
    root_logger.removeHandler(handler)

# configure logging
logging.basicConfig(filename='2_wrrf_fuzzy_matching.log', 
                    level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')

try:
    # fuzzy matching threshold scores
    COORD_THRESHOLD = 0.1 
    ADDRESS_THRESHOLD = 60
    NAME_THRESHOLD = 65
    ADDRESS_FUNCTION = fuzz.token_set_ratio

    # input paths
    main_file = "/home/kyl29/CSE_MSE_RXF131/staging/casf/kyl29/anaerobic_digestion_facilities/1_filtering_WRRF_adf.csv"
    facility_dir = "/home/kyl29/CSE_MSE_RXF131/staging/casf/kyl29/1_facility_download/"
    output_csv_path = "2_wrrf_fuzzy_matching.csv"

    # directory containing  index.html file
    web_dir_path = "/home/kyl29/CSE_MSE_RXF131/staging/casf/kyl29/leaflet/"

    # load and read anaerobic digestion facilities CSV
    logging.info(f"Loading WWRF filtered facilities from: {main_file}")
    adf = pd.read_csv(main_file, engine="python")
    
    # convert lat/long coordinate columns to numbers in the main 'adf' file
    adf['Latitude'] = pd.to_numeric(adf['Latitude'], errors='coerce').fillna(0)
    adf['Longitude'] = pd.to_numeric(adf['Longitude'], errors='coerce').fillna(0)

    # fill all other empty cells with empty strings
    adf = adf.fillna('') 
    logging.info(f"Loaded and cleaned {len(adf)} facilities.")

    # matching logic
    match_results = []
    matched_adf_indices = set()

    logging.info(f"Beginning to process state directories in: {facility_dir}")
    for state_dir in os.listdir(facility_dir):
        state_path = os.path.join(facility_dir, state_dir)
        if os.path.isdir(state_path):
            logging.info(f"Processing state directory: {state_dir}")
            
            for file in os.listdir(state_path):
                if file.startswith("2025_") and file.endswith(".csv"):
                    path = os.path.join(state_path, file)
                    logging.info(f"Processing file: {path}")
                    
                    if os.path.getsize(path) == 0:
                        logging.warning(f"Skipped empty file: {path}")
                        continue

                    try:
                        # load state data files
                        df = pd.read_csv(path, engine='python', skiprows=3)

                        # convert coordinate columns to numbers, turning errors into NaN, then filling with 0
                        df['Facility Latitude'] = pd.to_numeric(df.get('Facility Latitude'), errors='coerce').fillna(0)
                        df['Facility Longitude'] = pd.to_numeric(df.get('Facility Longitude'), errors='coerce').fillna(0)
                        
                        # fill all other empty cells with empty strings
                        df = df.fillna('')
                        logging.info(f"Loaded and cleaned {len(df)} entries from {file}.")

                    except (pd.errors.EmptyDataError, KeyError) as e:
                        logging.warning(f"Skipped file with no data or missing columns: {path}. Error: {e}")
                        continue

                    # matching loop
                    for idx, row in adf.iterrows():
                        if idx in matched_adf_indices:
                            continue

                        adf_name = str(row['Name']).upper()
                        if not adf_name: continue

                        target_address = f"{str(row.get('Address', '')).upper()}, {str(row.get('City', '')).upper()}, {str(row.get('State', '')).upper()}"
                        has_valid_address = bool(row.get('Address', ''))

                        target_lat = row['Latitude']
                        target_lon = row['Longitude']

                        for idx2, row2 in df.iterrows():
                            # ensure the NPDES column exists before trying to access it
                            if "NPDES Permit Number" not in row2:
                                continue

                            lat_diff = abs(target_lat - row2['Facility Latitude'])
                            lon_diff = abs(target_lon - row2['Facility Longitude'])

                            if lat_diff < COORD_THRESHOLD and lon_diff < COORD_THRESHOLD:
                                name_score = fuzz.token_sort_ratio(adf_name, str(row2['Facility Name']).upper())
                                
                                if not has_valid_address:
                                    if name_score > 95:
                                        address_score = 0
                                    else:
                                        continue
                                else:
                                    address_to_compare = f"{str(row2.get('Street Address', '')).upper()}, {str(row2.get('City', '')).upper()}, {str(row2.get('State', '')).upper()}"
                                    address_score = ADDRESS_FUNCTION(target_address, address_to_compare)

                                if address_score >= ADDRESS_THRESHOLD and name_score >= NAME_THRESHOLD:
                                    logging.info(f"Match found: {row['Name']} WITH {row2['Facility Name']} (Name: {name_score}, Addr: {address_score})")
                                    
                                    match_results.append({
                                        "npdes": row2["NPDES Permit Number"],
                                        "ADF Name": row["Name"], "ADF Address": target_address, "ADF Zip": row.get("Zip Code", ""),
                                        "ADF Latitude": row["Latitude"], "ADF Longitude": row["Longitude"],
                                        "Matched Name": row2["Facility Name"], "Matched Address": row2.get("Street Address", ""),
                                        "Matched Zip": row2.get("ZIP Code", ""), "Matched Latitude": row2["Facility Latitude"],
                                        "Matched Longitude": row2["Facility Longitude"], "Name Fuzzy Score": name_score,
                                        "Address Fuzzy Score": address_score, "Source File": file
                                    })
                                    matched_adf_indices.add(idx)
                                    break

    # save final output
    logging.info(f"Saving {len(match_results)} match results to {output_csv_path}...")
    results_df = pd.DataFrame(match_results)
    results_df.to_csv(output_csv_path, index=False)
    
    logging.info(f"Matching complete. Results saved to {output_csv_path}")

    # copy the generated CSV to index.html directory
    if os.path.exists(web_dir_path):
        destination_file_path = os.path.join(web_dir_path, output_csv_path)
        logging.info(f"Copying '{output_csv_path}' to '{destination_file_path}'...")
        shutil.copy(output_csv_path, destination_file_path)
        logging.info("File copied to web directory successfully.")
    else:
        logging.error(f"Destination web directory does not exist: '{web_dir_path}'. Could not copy file. Please check the 'web_dir_path' variable.")


except FileNotFoundError as e:
    logging.error(f"CRITICAL ERROR: A file was not found. Please check paths. Details: {e}")
    sys.exit(1)
except Exception as e:
    # catch any other unexpected errors and log them
    logging.error(f"An unexpected error occurred: {e}", exc_info=True)
    sys.exit(1)

logging.info("Script Finished Successfully")