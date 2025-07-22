#!/usr/bin/env python3

# ==============================================================================
# Script: download_echo_viewer.py
#
# Description:
#   A utility script to quickly inspect the contents of any CSV file downloaded
#   from the EPA ECHO database. The user provides the filename as an argument.
#
#   It prints the first few lines of the specified file to the console.
#
# Usage:
#   python3 download_echo_viewer.py <filename.csv> [--lines N]
#
# Dependencies:
#   - Python 3
# ==============================================================================

import os
import argparse

# --- Configuration & Argument Parsing ---

# Set up the argument parser to define what command-line arguments we accept.
parser = argparse.ArgumentParser(
    description="A utility to preview the first few lines of a downloaded EPA ECHO CSV file."
)

# Define a mandatory, positional argument: the filename.
parser.add_argument(
    "filename",
    type=str,
    help="The name of the CSV file to preview (e.g., 'NPDES_DMRS_FY2009.csv')."
)

# Define an optional argument '-n' or '--lines' to specify the number of lines to show.
parser.add_argument(
    "-n", "--lines",
    type=int,
    default=4, # If the user doesn't provide this, it will default to 4.
    help="The number of lines to display, including the header. Default is 4."
)

# Parse the arguments provided by the user when they run the script.
args = parser.parse_args()


# --- Path Setup ---

# Set the base directory dynamically to the script's location.
BASE_DIR = os.path.dirname(os.path.realpath(__file__))

# Construct the full path to the target CSV file using the user-provided filename.
csv_path = os.path.join(BASE_DIR, "download_echo_data", args.filename)


# --- Script Logic (Core functionality is unchanged) ---

print(f"--- Attempting to preview the first {args.lines} lines of: {csv_path} ---")

# Use a try...except block to gracefully handle the case where the file doesn't exist.
try:
    with open(csv_path, "r", encoding="utf-8") as f:
        # Loop through the file, getting both the line number (i) and content (line).
        for i, line in enumerate(f):
            # Stop reading after the number of lines specified by the user.
            if i >= args.lines:
                break
            # .strip() removes any leading/trailing whitespace.
            print(line.strip())

# If the file cannot be found, this block will execute.
except FileNotFoundError:
    print(f"\nERROR: File not found at the specified path.")
    print("Please check the following:")
    print("  1. Have you run the `./download_echo.sh` script first?")
    print(f"  2. Does the file '{args.filename}' exist inside the 'data' directory?")
    print("  3. Did you spell the filename correctly?")

print("--- Preview finished ---")