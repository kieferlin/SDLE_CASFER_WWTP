#!/usr/bin/env python3

# ==============================================================================
# Script: comprehensive_nutrient_list.py
#
# Description:
#   This script scans a specified directory structure to find all unique
#   pollutant data files. It expects a structure where a base data directory
#   contains subdirectories named by year (e.g., "2020", "2021"), and each
#   year directory contains JSON files named after pollutants
#   (e.g., "Ammonia.json").
#
#   It compiles a single, alphabetized list of all unique pollutant names
#   found across all years and saves it to a text file.
#
# Usage:
#   python3 comprehensive_nutrient_list.py
#
#
# Dependencies:
#   - Python 3
# ==============================================================================

import os

# --- Configuration ---

# Dynamically determine the directory where this script is located.
SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

# Define the directory where the year-based pollutant data is stored.
DATA_DIR = os.path.join(SCRIPT_DIR, "leaflet_json_pollutant_list")

# The name of the output file that will store the comprehensive list.
# This file will be created in the same directory as the script.
OUTPUT_FILENAME = "comprehensive_nutrient_list.txt"


# --- Main Script ---

def discover_pollutants(base_path):
    """
    Scans a directory structure to find and list all unique pollutant files.

    The expected structure is:
    base_path/
    ├── 2020/
    │   ├── PollutantA.json
    │   └── PollutantB.json
    └── 2021/
        ├── PollutantA.json
        └── PollutantC.json

    Args:
        base_path (str): The root directory containing year folders.

    Returns:
        set: A set containing the names of all unique pollutants found (e.g., {'PollutantA', 'PollutantB', 'PollutantC'}).
             Returns None if the base directory doesn't exist.
    """
    # A 'set' is used to automatically store only unique pollutant names.
    # If "Ammonia" is found in 2020 and 2021, it will only be added once.
    all_unique_pollutants = set()

    # First, check if the data directory actually exists.
    if not os.path.isdir(base_path):
        print(f"Error: Base data directory not found at '{base_path}'")
        return None

    print(f"Scanning directory: {base_path}\n")

    try:
        # Get a list of all items in the base directory (expecting year folders).
        # We sort it to process years in a consistent order.
        year_folders = sorted(os.listdir(base_path))
    except FileNotFoundError:
        print(f"Error: Cannot access directory '{base_path}'. Please check the path and permissions.")
        return None

    # Iterate through each item found in the data directory.
    for year_str in year_folders:
        year_path = os.path.join(base_path, year_str)
        # Process the item only if it's a directory with a 4-digit name (like a year).
        if os.path.isdir(year_path) and year_str.isdigit() and len(year_str) == 4:
            # Now, list all files inside the valid year folder.
            for filename in os.listdir(year_path):
                # We only care about files ending in '.json'.
                if filename.endswith(".json"):
                    # Extract the filename without the '.json' extension to get the pollutant name.
                    pollutant_name = os.path.splitext(filename)[0]
                    all_unique_pollutants.add(pollutant_name)

    return all_unique_pollutants

def save_list_to_file(pollutant_list, output_path):
    """
    Saves a list of items to a text file, with one item per line.

    Args:
        pollutant_list (list): The list of pollutants to save.
        output_path (str): The full path of the file to save the list to.
    """
    try:
        # Open the file in 'w' (write) mode. This will create the file if it
        # doesn't exist or overwrite it if it does.
        with open(output_path, 'w') as f:
            for pollutant in pollutant_list:
                f.write(f"{pollutant}\n")

        # Get the full, absolute path of the created file for clear user feedback.
        full_path = os.path.abspath(output_path)
        print("\n" + "="*60)
        print(f"Successfully created the comprehensive pollutant list.")
        print(f"Total unique pollutants found: {len(pollutant_list)}")
        print(f"File saved to: {full_path}")
        print("="*60)

    except IOError as e:
        print(f"\nError: Could not write to file '{output_path}'. Reason: {e}")

# --- Execution ---
# The __name__ == "__main__" block ensures this code only runs when the script
# is executed directly (not when imported as a module into another script).
if __name__ == "__main__":
    # 1. Discover all pollutants from the file structure defined in DATA_DIR.
    found_pollutants = discover_pollutants(DATA_DIR)

    # Proceed only if pollutants were found and no errors occurred.
    if found_pollutants:
        # 2. Convert the set to a list and sort it alphabetically for consistent,
        #    ordered output every time the script is run.
        sorted_pollutants = sorted(list(found_pollutants))

        # 3. Save the sorted list to the specified output file.
        save_list_to_file(sorted_pollutants, OUTPUT_FILENAME)
    else:
        print("\nNo pollutants were found. Please check the following:")
        print(f"  1. Ensure your data directory exists at: {DATA_DIR}")
        print("  2. Ensure it contains year folders (e.g., '2020', '2021').")
        print("  3. Ensure those folders contain '.json' files.")