#!/bin/bash

# ==============================================================================
# download_echo.sh
#
# Description:
#   This script downloads and unzips data files from the US EPA's ECHO
#   (Enforcement and Compliance History Online) database. It is designed to
#   be efficient by using wget's timestamping feature (-N), which only
#   downloads files if the version on the server is newer than the local copy.
#
#   All downloaded files and a detailed log are stored in a 'data'
#   subdirectory created in the same location as this script.
#
# Dependencies:
#   - wget
#   - unzip
# ==============================================================================


# --- Configuration ---

# Determine the absolute path of the directory where this script is located.
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

# Define the data directory relative to the script's location.
# All downloads and unzipped files will be placed here.
DATA_DIR="${SCRIPT_DIR}/download_echo_data"

# Define the log file path. All output will be logged here for debugging.
LOG_FILE="${DATA_DIR}/download_echo.log"


# --- URL Lists ---
# This section defines all the files to be downloaded from the EPA ECHO website.

URLS_TO_DOWNLOAD=()
DMR_BASE_URL="https://echo.epa.gov/files/echodownloads"

# Add Discharge Monitoring Report (DMR) data for years prior to FY2009.
URLS_TO_DOWNLOAD+=("${DMR_BASE_URL}/npdes_dmrs_prefy2009.zip")

# Dynamically generate and add DMR data URLs for Fiscal Years 2009 through 2025.
for YEAR in {2009..2025}; do
  URLS_TO_DOWNLOAD+=("${DMR_BASE_URL}/npdes_dmrs_fy${YEAR}.zip")
done

# Add other key NPDES (National Pollutant Discharge Elimination System) and
# FRS (Facility Registry Service) data files.
URLS_TO_DOWNLOAD+=(
  "https://echo.epa.gov/files/echodownloads/npdes_downloads.zip"
  "https://echo.epa.gov/files/echodownloads/npdes_eff_downloads.zip"
  "https://echo.epa.gov/files/echodownloads/frs_downloads.zip"
)


# --- Script Logic ---

# Create the data directory if it doesn't already exist.
mkdir -p "$DATA_DIR"

# Log the start time of the script execution.
echo "----------------------------------------" >> "$LOG_FILE"
echo "Starting ECHO data check/download at $(date)" >> "$LOG_FILE"
echo "Checking ${#URLS_TO_DOWNLOAD[@]} total files..." | tee -a "$LOG_FILE"

# Loop through each URL in the list to download and process it.
for URL in "${URLS_TO_DOWNLOAD[@]}"; do
  # Extract the filename (e.g., "npdes_dmrs_fy2025.zip") from the full URL.
  FILENAME=$(basename "$URL")
  echo "Checking ${FILENAME}..." | tee -a "$LOG_FILE"
  
  # Use wget to download the file. The command is configured for robustness and efficiency.
  wget \
    -N \
    -P "$DATA_DIR" \
    --limit-rate=5m \
    --random-wait \
    --tries=3 \
    --waitretry=10 \
    -nv \
    "$URL" >> "$LOG_FILE" 2>&1
  
  # --- Explanation of wget flags ---
  
  # -N: Enables timestamping. Wget will only download the file if the remote
  #     version is newer than the local one, or if the local file doesn't exist.
  # -P "$DATA_DIR": Specifies the directory prefix where files will be saved.
  # --limit-rate=5m: Limits the download speed to 5 MB/s to be courteous to the server.
  # --random-wait: Waits a random amount of time (up to 2 seconds) between downloads.
  # --tries=3: Retries the download up to 3 times if it fails.
  # --waitretry=10: Waits 10 seconds between retries.
  # -nv: "no-verbose" mode. Prints less output, keeping the log clean.
  # >> "$LOG_FILE" 2>&1: Redirects both standard output (stdout) and standard
  #                     error (stderr) to the log file.
  
  # After the download attempt, check if the zip file exists locally.
  if [ -f "${DATA_DIR}/${FILENAME}" ]; then
    # If the file exists, unzip it.
    echo "Unzipping ${FILENAME} to ensure contents are up-to-date..." | tee -a "$LOG_FILE"
    # -o: Overwrites existing files without prompting.
    # -d: Specifies the destination directory for the unzipped files.
    unzip -o "${DATA_DIR}/${FILENAME}" -d "${DATA_DIR}/" >> "$LOG_FILE"
  else
    # This block will execute if wget failed to download the file after all retries.
    echo "ERROR: File ${FILENAME} not found after check/download attempt." | tee -a "$LOG_FILE"
  fi
done

# Log the completion time of the script.
echo "Download check finished at $(date)" >> "$LOG_FILE"
echo "----------------------------------------" >> "$LOG_FILE"
