import csv
import json
import os
import sys  
from datetime import datetime
from collections import defaultdict

# get the state from the sbatch script - ensure a state abbreviation is provided
if len(sys.argv) < 2:
    print("Error: Please provide a state abbreviation as a command-line argument.")
    sys.exit(1)

# the state to process is the first argument from the command line
state = sys.argv[1]

# input and outputh paths
dmr_base = "/home/kyl29/CSE_MSE_RXF131/staging/casf/kyl29/2_dmr_download_checkpoints"
facility_base = "/home/kyl29/CSE_MSE_RXF131/staging/casf/kyl29/1_facility_download"
output_base = "/home/kyl29/CSE_MSE_RXF131/staging/casf/kyl29/leaflet/1_csv_to_json"

# main loop for each year
for year in range(2007, 2026):
    # create a separate folder for each year
    year_output_dir = os.path.join(output_base, str(year))
    os.makedirs(year_output_dir, exist_ok=True)

    # build paths to input facility CSV and DMR directory
    facility_csv = os.path.join(facility_base, state, f"{year}_{state}.csv")
    dmr_dir = os.path.join(dmr_base, str(year), state)

    # skip if either file is missing
    if not os.path.isfile(facility_csv) or not os.path.isdir(dmr_dir):
        print(f"Skipping {state} {year}: missing files")
        continue

    # load facility coordinates from the facility CSV
    facility_coords = {}
    with open(facility_csv, "r", encoding="utf-8", errors="replace") as raw_f:
        # skip header and safely handle short files
        for _ in range(3):
            try:
                next(raw_f)
            except StopIteration:
                print(f"Skipping short facility file: {facility_csv}")
                facility_coords = {}
                break
        else:
            cleaned = (line.replace('\x00', '') for line in raw_f)
            reader = csv.DictReader(cleaned)
            for row in reader:
                npdes = row["NPDES Permit Number"].strip()
                lat = row["Facility Latitude"].strip()
                lon = row["Facility Longitude"].strip()
                if lat and lon:
                    facility_coords[npdes] = {
                        "lat": float(lat),
                        "lon": float(lon)
                    }

    # if facility data is empty or file was short, skip this state-year
    if not facility_coords:
        continue

    # initialize data structure to group DMR entries
    grouped_data = defaultdict(lambda: {
        "npdes": "",
        "pollutant": "",
        "unit": "",
        "lat": None,
        "lon": None,
        "measurements": []
    })

    # process each DMR file in the state's DMR folder
    for file in os.listdir(dmr_dir):
        if not file.endswith(".csv"):
            continue  # skip non-CSV files

        filepath = os.path.join(dmr_dir, file)
        with open(filepath, "r", encoding="utf-8", errors="replace") as raw_f:
            # skip header and safely handle short files
            for _ in range(4):
                try:
                    next(raw_f)
                except StopIteration:
                    print(f"Skipping short DMR file: {filepath}")
                    continue  # skip this file
            cleaned = (line.replace('\x00', '') for line in raw_f)
            reader = csv.DictReader(cleaned)
            for row in reader:
                # extract key pollutant and location info
                npdes = row["NPDES Permit Number"].strip()
                pollutant = row["Parameter Description"].strip()
                value = row["DMR Value"].strip()
                unit = row["DMR Value Unit"].strip()
                date_str = row["Monitoring Period Date"].strip()

                # skip rows with no data or invalid date
                if value.startswith("NODI:"):
                    continue
                # A slightly more precise version
                try:
                    datetime.strptime(date_str, "%m/%d/%Y")
                except ValueError: # Catch the specific error
                    continue

                # get latitude/longitude from facility metadata
                lat = facility_coords.get(npdes, {}).get("lat")
                lon = facility_coords.get(npdes, {}).get("lon")
                if lat is None or lon is None:
                    continue

                # use (npdes, pollutant) as the grouping key
                key = (npdes, pollutant)
                grouped_data[key]["npdes"] = npdes
                grouped_data[key]["pollutant"] = pollutant
                grouped_data[key]["unit"] = unit
                grouped_data[key]["lat"] = lat
                grouped_data[key]["lon"] = lon
                grouped_data[key]["measurements"].append({
                    "date": date_str,
                    "value": value
                })

    # write grouped output to JSON in the year subfolder
    # only write a file if there is data to prevent empty JSON files
    if grouped_data:
        final = list(grouped_data.values())
        output_path = os.path.join(year_output_dir, f"{state}_{year}.json")
        with open(output_path, "w") as f:
            json.dump(final, f, indent=2)

        print(f"Created file: {output_path}")
    else:
        print(f"No data found for {state} {year}, skipping file creation.")