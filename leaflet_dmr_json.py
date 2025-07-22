#!/usr/bin/env python3

# ==============================================================================
# Script: leaflet_dmr_json.py
#
# Description:
#   Processes a single year's (or a specific label's) worth of EPA ECHO DMR data.
#   It joins the DMR measurement data with facility location data (latitude and
#   longitude) and aggregates all measurements for each unique facility-pollutant
#   pair.
#
#   The final output is a set of JSON files, organized by state and year,
#   in a format optimized for visualization with Leaflet.js. Each JSON file
#   contains a list of facilities, where each facility object includes its
#   location, the pollutant measured, and a time-series of measurements.
#
# Usage:
#   python3 leaflet_dmr_json.py <input_dir> <output_dir> <year_or_label>
#
#   Example:
#   python3 leaflet_dmr_json.py ./download_echo_data ./leaflet_dmr_json 2015
#   python3 leaflet_dmr_json.py ./download_echo_data ./leaflet_dmr_json PREFY2009
#
# Dependencies:
#   - Python 3
# ==============================================================================

import csv
import json
import os
import sys
import argparse
import logging
from datetime import datetime
from collections import defaultdict

# Configure basic logging to display info-level messages.
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def load_facility_data(facility_csv_path):
    """
    Loads facility location data from ICIS_FACILITIES.csv into a dictionary
    for fast, efficient lookups. This prevents re-reading the large facility
    file for every row in the DMR data.

    Args:
        facility_csv_path (str): The full path to the facility CSV file.

    Returns:
        dict: A dictionary mapping an NPDES ID to its state, latitude, and longitude.
              Example: {'TX000001': {'lat': 30.2, 'lon': -97.7, 'state': 'TX'}}
    """
    facility_lookup = {}
    logging.info(f"Loading facility data from {facility_csv_path}...")
    try:
        # Use 'errors="replace"' to handle potential encoding issues in the source file.
        with open(facility_csv_path, "r", encoding="utf-8", errors="replace") as f:
            reader = csv.DictReader(f)
            for row in reader:
                npdes_id = row.get("NPDES_ID", "").strip()
                lat = row.get("GEOCODE_LATITUDE", "").strip()
                lon = row.get("GEOCODE_LONGITUDE", "").strip()
                state = row.get("STATE_CODE", "").strip()

                # Only include facilities with a valid ID and complete location data.
                if npdes_id and lat and lon and state:
                    try:
                        # Store data in the lookup dictionary.
                        facility_lookup[npdes_id] = {
                            "lat": float(lat),
                            "lon": float(lon),
                            "state": state
                        }
                    except (ValueError, TypeError):
                        # Gracefully skip rows where latitude/longitude are not valid numbers.
                        continue
    except FileNotFoundError:
        logging.error(f"FATAL: Facility file not found at {facility_csv_path}")
        sys.exit(1)
    
    logging.info(f"Loaded location data for {len(facility_lookup)} facilities.")
    return facility_lookup


def process_dmr_data(input_dir, output_dir, year_or_label):
    """
    Main processing function. It reads a specific DMR data file, joins it with
    facility locations, aggregates the measurements, and writes the output
    to structured JSON files.
    """
    # --- 1. Determine DMR filename and path ---
    # The ECHO dataset uses a different naming convention for data before fiscal year 2009.
    if year_or_label == "PREFY2009":
        dmr_filename = "NPDES_DMRS_PREFY2009.csv"
    else:
        dmr_filename = f"NPDES_DMRS_FY{year_or_label}.csv"
    
    dmr_csv_path = os.path.join(input_dir, dmr_filename)

    # Exit gracefully if the target DMR file doesn't exist. This is expected
    # in some cases and allows the SLURM array job to continue.
    if not os.path.isfile(dmr_csv_path):
        logging.warning(f"Skipping {year_or_label}: DMR file not found at {dmr_csv_path}")
        sys.exit(0)

    # --- 2. Load Facility Data ---
    # This is done once per script run for efficiency.
    facility_locations_path = os.path.join(input_dir, "ICIS_FACILITIES.csv")
    facility_data = load_facility_data(facility_locations_path)

    # --- 3. Process the Annual DMR File ---
    # defaultdict simplifies aggregation. It automatically creates nested dictionaries
    # as new states or facility-pollutant keys are encountered.
    data_by_state = defaultdict(lambda: defaultdict(lambda: {
        "npdes": "", "pollutant": "", "unit": "", "lat": None, "lon": None, "measurements": []
    }))

    logging.info(f"Processing DMR file for {year_or_label}: {dmr_csv_path}")
    with open(dmr_csv_path, "r", encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f)
        for row in reader:
            npdes = row.get("EXTERNAL_PERMIT_NMBR", "").strip()
            
            # Retrieve facility info (state, lat, lon) using the NPDES ID.
            facility_info = facility_data.get(npdes)
            if not facility_info:
                continue  # Skip DMR entries for facilities without location data.

            state = facility_info["state"]
            lat = facility_info["lat"]
            lon = facility_info["lon"]

            pollutant = row.get("PARAMETER_DESC", "").strip()
            value = row.get("DMR_VALUE_NMBR", "").strip()
            unit = row.get("DMR_UNIT_DESC", "").strip()
            date_str = row.get("MONITORING_PERIOD_END_DATE", "").strip()
            nodi_code = row.get("NODI_CODE", "").strip()

            # Skip rows that have no reported value. NODI (No Data Indicator) codes
            # signify reasons why data is missing (e.g., "Plant not in operation").
            if nodi_code or not value:
                continue
            
            # Ensure the date string is in the expected format before using it.
            try:
                datetime.strptime(date_str, "%m/%d/%Y")
            except ValueError:
                continue # Skip rows with malformed dates.

            # A tuple of (NPDES ID, Pollutant) serves as a unique key for aggregation.
            key = (npdes, pollutant)

            # Populate the data structure for this facility-pollutant pair.
            group = data_by_state[state][key]
            group["npdes"] = npdes
            group["pollutant"] = pollutant
            group["unit"] = unit
            group["lat"] = lat
            group["lon"] = lon
            group["measurements"].append({"date": date_str, "value": value})

    # --- 4. Write Output Files ---
    logging.info(f"Finished processing. Writing JSON files for {year_or_label}...")
    if not data_by_state:
        logging.info(f"No valid data found for {year_or_label}. No output files will be created.")
        return

    for state, grouped_data in data_by_state.items():
        # Create an output directory for the year/label, and a subdirectory for the state.
        # e.g., /path/to/output/2015/TX/
        state_output_dir = os.path.join(output_dir, str(year_or_label), state)
        os.makedirs(state_output_dir, exist_ok=True)
        
        # Convert the inner dictionary of {key: data} to a flat list [data, data, ...].
        final_list_for_json = list(grouped_data.values())
        
        output_path = os.path.join(state_output_dir, f"{state}_{year_or_label}.json")
        try:
            with open(output_path, "w") as f:
                json.dump(final_list_for_json, f, indent=2)
            logging.info(f"Successfully created file: {output_path}")
        except IOError as e:
            logging.error(f"Could not write to file {output_path}: {e}")


if __name__ == "__main__":
    # Set up the argument parser for a robust command-line interface.
    parser = argparse.ArgumentParser(
        description="Process one year of ECHO DMR data into a location-aware JSON format."
    )
    # Define the command-line arguments the script accepts.
    parser.add_argument("input_dir", type=str, help="The directory containing source CSV files.")
    parser.add_argument("output_dir", type=str, help="The root directory for the output JSON files.")
    parser.add_argument("year_or_label", type=str, help="The specific year or label (e.g., 2015 or PREFY2009) to process.")
    
    args = parser.parse_args()

    # Run the main processing function with the parsed arguments.
    process_dmr_data(args.input_dir, args.output_dir, args.year_or_label)