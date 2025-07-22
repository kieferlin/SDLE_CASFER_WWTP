#!/usr/bin/env python3

# ==============================================================================
# Script: leaflet_json_pollutant_list.py
#
# Description:
#   This script transforms the previously generated JSON files (organized by
#   year/state) into a new structure organized by year/pollutant. This regrouping
#   is essential for creating map visualizations where users can select a
#   specific pollutant to view across all locations.
#
#   It has special logic for the 'PREFY2009' dataset: it reads the measurement
#   dates within the files to split the data into its actual historical years
#   (e.g., 1995, 1996), creating separate output directories for each. For all
#   other years, it simply aggregates the data by pollutant.
#
# Usage:
#   python3 leaflet_json_pollutant_list.py <input_dir> <output_dir> <year_or_label>
#
#   Example:
#   python3 leaflet_json_pollutant_list.py ./leaflet_dmr_json ./leaflet_json_pollutant_list 2015
#
# Dependencies:
#   - Python 3
# ==============================================================================

import json
import os
import sys
import argparse
import logging
from collections import defaultdict
from datetime import datetime

# Configure basic logging for clear, standardized output.
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def process_pollutant_data(input_dir_root, output_dir_root, year_or_label):
    """
    Main function to orchestrate the regrouping of JSON data by pollutant.
    """
    # Construct the specific input directory path for the given year/label.
    year_input_dir = os.path.join(input_dir_root, year_or_label)

    # Verify that the input directory for the year exists before proceeding.
    if not os.path.isdir(year_input_dir):
        logging.error(f"Input directory for {year_or_label} not found at {year_input_dir}")
        sys.exit(1)

    logging.info(f"Starting job for year/label: {year_or_label}...")

    # --- Conditional Logic: Special handling for consolidated pre-2009 data ---
    if year_or_label == "PREFY2009":
        logging.info("PREFY2009 label detected. Splitting data by actual measurement year.")
        
        # This nested dictionary will store data extracted from the PREFY2009 files,
        # re-sorted by the actual year of measurement.
        # Structure: { "1992": { "pH": { "AK0000086": { facility_data... } } } }
        data_by_actual_year = defaultdict(lambda: defaultdict(dict))

        # Walk through the PREFY2009 directory tree (e.g., .../PREFY2009/TX/).
        for root, dirs, files in os.walk(year_input_dir):
            for filename in sorted(files):
                if not filename.endswith(".json"):
                    continue

                input_path = os.path.join(root, filename)
                logging.info(f"  Processing {input_path}")
                with open(input_path, "r") as f:
                    data = json.load(f)

                # Iterate through each facility entry in the state's JSON file.
                for facility_entry in data:
                    npdes = facility_entry["npdes"]
                    pollutant = facility_entry["pollutant"]
                    lat = facility_entry["lat"]
                    lon = facility_entry["lon"]

                    # For each measurement, extract its actual year and regroup it.
                    for measurement in facility_entry["measurements"]:
                        try:
                            # Extract year from the date string (format 'mm/dd/yyyy').
                            measurement_date = datetime.strptime(measurement["date"], "%m/%d/%Y")
                            actual_year = str(measurement_date.year)
                        except ValueError:
                            continue # Skip records with malformed dates.

                        # Get or create the facility record for the actual year and pollutant.
                        facility_record = data_by_actual_year[actual_year][pollutant].setdefault(npdes, {
                            "npdes": npdes, "pollutant": pollutant, "lat": lat, "lon": lon, "measurements": []
                        })
                        # Append the measurement to the correct year/pollutant/facility list.
                        facility_record["measurements"].append(measurement)

        # Write the reconstructed yearly files from the aggregated PREFY2009 data.
        logging.info("Writing reconstructed yearly files from PREFY2009 data...")
        for year, pollutants_data in data_by_actual_year.items():
            year_output_dir = os.path.join(output_dir_root, year)
            os.makedirs(year_output_dir, exist_ok=True)
            
            for pollutant, facilities_data in pollutants_data.items():
                # Create a filesystem-safe name for the pollutant.
                safe_name = pollutant.replace("/", "-").replace(" ", "_").replace(",", "") or "unnamed_pollutant"
                output_file = os.path.join(year_output_dir, f"{safe_name}.json")
                
                # Convert the dictionary of facilities back to a list for JSON output.
                final_list = list(facilities_data.values())
                with open(output_file, "w") as f:
                    json.dump(final_list, f, indent=2)
            logging.info(f"  Wrote {len(pollutants_data)} pollutant files for year {year}")

    # --- Standard Logic for individual fiscal years (2009 and later) ---
    else:
        logging.info(f"Processing as a single year: {year_or_label}")
        # Dictionary to hold all data for the year, keyed by pollutant.
        year_data_by_pollutant = defaultdict(list)

        # Walk through the year's directory (e.g., .../2015/TX/, .../2015/CA/).
        for root, dirs, files in os.walk(year_input_dir):
            for filename in sorted(files):
                if not filename.endswith(".json"):
                    continue
                input_path = os.path.join(root, filename)
                with open(input_path, "r") as f:
                    data = json.load(f)
                
                # Append each facility entry to the list for its specific pollutant.
                for entry in data:
                    pollutant = entry["pollutant"]
                    year_data_by_pollutant[pollutant].append(entry)
                logging.info(f"  Aggregated {os.path.basename(root)}/{filename}")
            
        if not year_data_by_pollutant:
            logging.warning(f"No data found for {year_or_label}. No output files will be created.")
            sys.exit(0)

        # Create the output directory for this year.
        year_output_dir = os.path.join(output_dir_root, year_or_label)
        os.makedirs(year_output_dir, exist_ok=True)

        # Write one JSON file for each pollutant found in that year.
        for pollutant, entries in year_data_by_pollutant.items():
            safe_name = pollutant.replace("/", "-").replace(" ", "_").replace(",", "") or "unnamed_pollutant"
            output_file = os.path.join(year_output_dir, f"{safe_name}.json")
            with open(output_file, "w") as f:
                json.dump(entries, f, indent=2)

        logging.info(f"Year {year_or_label} complete. Wrote {len(year_data_by_pollutant)} pollutant files to {year_output_dir}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Regroup state-based DMR JSON files into pollutant-based files."
    )
    parser.add_argument("input_dir", type=str, help="The root directory containing the input JSON files (from the previous step).")
    parser.add_argument("output_dir", type=str, help="The root directory for the new pollutant-based JSON files.")
    parser.add_argument("year_or_label", type=str, help="The specific year or label (e.g., 2015 or PREFY2009) to process.")
    
    args = parser.parse_args()

    process_pollutant_data(args.input_dir, args.output_dir, args.year_or_label)
