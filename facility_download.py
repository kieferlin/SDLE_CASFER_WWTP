import requests
import time
import os
import csv
import logging
import random
import sys
import signal
from datetime import datetime
from urllib.parse import urlencode

# Setup logging
def setup_logging(output_dir):
    log_dir = os.path.join(output_dir, 'logs')
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, f'facility_data_download_{datetime.now().strftime("%Y-%m-%d")}.log')

    logging.basicConfig(
        filename=log_file,
        filemode='a',
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        level=logging.INFO
    )
    logger = logging.getLogger("Facility-Downloader")
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    logger.addHandler(console_handler)
    
    return logger

# Random wait
def random_wait(min_seconds, max_seconds, logger, message):
    wait_time = random.uniform(min_seconds, max_seconds)
    logger.info(f"Random wait for {wait_time:.1f} seconds: {message}")
    time.sleep(wait_time)

# Build facility query URL
def build_facility_query_url(year, state, ip_address):
    params = {
        'p_lod': 'ann',
        'p_year': year,
        'p_st': state,
        'p_loads_data': 'dmr',
        'p_nd': 'zero',
        'p_est': 'Y',
        'p_param_group': 'N',
        'p_nutrient_agg': 'N',
        'p_ipaddr': ip_address,
        'output': 'CSV',
        'qcolumns': ','.join(str(i) for i in range(1, 81))  # Columns 1-80
    }
    query_str = urlencode(params)
    return f"https://echodata.epa.gov/echo/dmr_rest_services.get_custom_data_annual?{query_str}"

# Download facility data for a given year and state
def download_facility_data(year, state, ip_address, output_dir, logger, checkpoint_file):
    state_dir = os.path.join(output_dir, state)
    os.makedirs(state_dir, exist_ok=True)

    file_path = os.path.join(state_dir, f"{year}_{state}.csv")

    # Check if file already exists
    if os.path.exists(file_path):
        logger.info(f"Skipping Year {year}, State {state}: file already exists.")
        return

    query_url = build_facility_query_url(year, state, ip_address)
    logger.info(f"Requesting URL: {query_url}")

    random_wait(3, 10, logger, "Pre-request delay")

    try:
        response = requests.get(query_url)
        response.raise_for_status()

        with open(file_path, "w") as file:
            file.write(response.text)

        logger.info(f"Saved facility data to {file_path}")

        # Save progress
        with open(checkpoint_file, "a") as f:
            f.write(f"{year}\n")

        random_wait(10, 30, logger, "Post-download delay")

    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to download Year {year}, State {state}: {e}")
        time.sleep(30)

# Process all years for a state
def process_years_for_state(state, years, ip_address, output_dir, logger, checkpoint_file):
    completed_years = set()
    if os.path.exists(checkpoint_file):
        with open(checkpoint_file, "r") as f:
            completed_years = set(line.strip() for line in f)

    for year in years:
        if str(year) in completed_years:
            logger.info(f"Skipping {state} {year}: already downloaded.")
            continue
        download_facility_data(year, state, ip_address, output_dir, logger, checkpoint_file)

    logger.info(f"Completed all years for {state}")

# Handle SIGUSR1 for SLURM requeue
def handle_sigusr1(signum, frame):
    logger.info("Received SIGUSR1: Saving progress and exiting...")
    sys.exit(0)

# Register signal handler
signal.signal(signal.SIGUSR1, handle_sigusr1)

# Main function
def main(state):
    output_dir = "/home/kyl29/CSE_MSE_RXF131/staging/casf/kyl29/facility_data"
    ip_address = "129.22.1.25"
    years = range(2000, 2026)

    logger = setup_logging(output_dir)

    logger.info(f"Starting facility data download for {state}")

    checkpoint_file = f"/home/kyl29/CSE_MSE_RXF131/staging/casf/kyl29/checkpoints/{state}_facility_progress.txt"
    os.makedirs(os.path.dirname(checkpoint_file), exist_ok=True)

    process_years_for_state(state, years, ip_address, output_dir, logger, checkpoint_file)

    logger.info(f"Finished processing for {state}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python download_facility.py <STATE>")
        sys.exit(1)

    state = sys.argv[1]
    main(state)
