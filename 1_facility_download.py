import requests
import time
import os
import logging
import random
import sys
import signal
from datetime import datetime
from urllib.parse import urlencode

# set up logging to both file and console
def setup_logging(output_dir):
    log_dir = os.path.join(output_dir, 'logs')
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, f'facility_data_download_{datetime.now().strftime("%Y-%m-%d")}.log')

    # configure logging to write to file
    logging.basicConfig(
        filename=log_file,
        filemode='a',
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        level=logging.INFO
    )

    # set up logger to also log to the console
    logger = logging.getLogger("Facility-Downloader")
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    logger.addHandler(console_handler)
    
    return logger

# sleep for a random time to avoid overloading API
def random_wait(min_seconds, max_seconds, logger, message):
    wait_time = random.uniform(min_seconds, max_seconds)
    logger.info(f"Random wait for {wait_time:.1f} seconds: {message}")
    time.sleep(wait_time)

# create facility query URL with the given year, state, and IP
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
        'qcolumns': ','.join(str(i) for i in range(1, 81))
    }
    query_str = urlencode(params)
    return f"https://echodata.epa.gov/echo/dmr_rest_services.get_custom_data_annual?{query_str}"

# download EPA data for a specific year and state
def download_facility_data(year, state, ip_address, output_dir, logger):
    state_dir = os.path.join(output_dir, state)
    os.makedirs(state_dir, exist_ok=True)

    file_path = os.path.join(state_dir, f"{year}_{state}.csv")
    
    query_url = build_facility_query_url(year, state, ip_address)
    logger.info(f"Requesting URL for {year} {state}: {query_url}")

    random_wait(3, 10, logger, "Pre-request delay")

    try:
        response = requests.get(query_url)
        response.raise_for_status()

        #save the CSV response to disk
        with open(file_path, "w", encoding="utf-8") as file:
            file.write(response.text)

        logger.info(f"SUCCESS: Saved data to {file_path}")
        random_wait(10, 30, logger, "Post-download delay")
        return True

    except requests.exceptions.RequestException as e:
        logger.error(f"FAILED to download Year {year}, State {state}: {e}")
        time.sleep(30)
        return False

# signal handler
def handle_sigusr1(signum, frame):
    logging.info("Received SIGUSR1: Exiting cleanly...")
    sys.exit(0)

# register signal handler
signal.signal(signal.SIGUSR1, handle_sigusr1)

# main function
def main(state):
    # output directory where CSVs are saved
    output_dir = "/home/kyl29/CSE_MSE_RXF131/staging/casf/kyl29/1_facility_download"
    ip_address = "129.22.1.25"
    # download data for years 2000–2025
    years = range(2000, 2026)

    logger = setup_logging(output_dir)

    # directory for progress tracking logs (checkpoints)
    checkpoints_dir = "/home/kyl29/CSE_MSE_RXF131/staging/casf/kyl29/1_facility_download_logs"
    os.makedirs(checkpoints_dir, exist_ok=True)

    # file to track completed years for this state
    checkpoint_file = os.path.join(checkpoints_dir, f"{state}_progress.txt")

    # read the checkpoint file to see what's already done
    completed_years = set()
    if os.path.exists(checkpoint_file):
        with open(checkpoint_file, 'r') as f:
            try:
                completed_years = {int(line.strip()) for line in f if line.strip().isdigit()}
            except ValueError:
                logger.warning(f"Warning: Non-integer value found in checkpoint file {checkpoint_file}. Ignoring invalid lines.")
                completed_years = set() # reset to be safe
    
    logger.info(f"Starting facility data download for {state}. Found {len(completed_years)} completed years in checkpoint file.")

    # main loop downloads each year unless already completed
    for year in years:
        if year in completed_years:
            logger.info(f"Skipping Year {year} for State {state}, already completed.")
            continue

        success = download_facility_data(year, state, ip_address, output_dir, logger)

        if success:
            # record completed year to checkpoint file
            with open(checkpoint_file, 'a') as f:
                f.write(f"{year}\n")
            logger.info(f"CHECKPOINT: Marked Year {year} for State {state} as complete.")

    logger.info(f"Finished processing all years for {state}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python 1_facility_download.py <STATE>")
        sys.exit(1)

    state = sys.argv[1]
    main(state)