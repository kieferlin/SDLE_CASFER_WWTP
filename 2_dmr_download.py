import os
import csv
import requests
import time
import logging
import random
import sys
import signal
from datetime import datetime
from urllib.parse import urlencode

# setup logging to both file and console
def setup_logging(output_dir):
    log_dir = os.path.join(output_dir, 'logs')
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, f'epa_data_download_{datetime.now().strftime("%Y-%m-%d")}.log')

    logging.basicConfig(
        filename=log_file,
        filemode='a',
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        level=logging.INFO
    )
    logger = logging.getLogger("EPA-Downloader")
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    logger.addHandler(console_handler)

    return logger

# random delay between requests to reduce load and avoid blocking
def random_wait(min_seconds, max_seconds, logger, message):
    wait_time = random.uniform(min_seconds, max_seconds)
    logger.info(f"Random wait for {wait_time:.1f} seconds: {message}")
    time.sleep(wait_time)

# read NPDES IDs from a CSV file, skipping headers and handling null bytes
def read_npdes_file(file_path):
    npdes_ids = []
    with open(file_path, 'r', encoding='utf-8', errors='ignore') as file:
        for _ in range(3):  # skip header lines
            next(file)
        clean_lines = (line.replace('\x00', '') for line in file)
        reader = csv.DictReader(clean_lines)
        for row in reader:
            npdes_id = row.get('NPDES Permit Number', '').strip()
            if npdes_id:
                npdes_ids.append(npdes_id)
    return npdes_ids

# build the EPA DMR data URL using the provided parameters
def build_query_url(start_date, end_date, npdes_id, ip_address):
    params = {
        'p_start_date': start_date,
        'p_end_date': end_date,
        'p_npdes_id': npdes_id,
        'p_ipaddr': ip_address,
        'output': 'CSV'
    }
    query_str = urlencode(params)
    return f"https://echodata.epa.gov/echo/dmr_rest_services.get_monitoring_data_csv?{query_str}"

# download and save the CSV file for one NPDES ID
def download_data_for_npdes(npdes_id, start_date, end_date, ip_address, output_dir, logger, state, year, checkpoint_file):
    state_folder = os.path.join(output_dir, 'EPA-WWTP', str(year), state)
    os.makedirs(state_folder, exist_ok=True)

    file_path = os.path.join(state_folder, f"{npdes_id}.csv")

    # skip if already downloaded
    if os.path.exists(file_path):
        logger.info(f"Skipping NPDES ID {npdes_id}, data already downloaded.")
        return

    query_url = build_query_url(start_date, end_date, npdes_id, ip_address)
    logger.info(f"Request URL: {query_url}")

    random_wait(3, 10, logger, "Pre-request delay")

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36'
    }

    try:
        response = requests.get(query_url, headers=headers)
        response.raise_for_status()

        # check if response is an error page instead of CSV
        if "FILE OUTPUT FAILED" in response.text or "<html" in response.text.lower():
            logger.warning(f"EPA rejected {npdes_id} for {state} in {year} — likely no DMR data.")

            # save error HTML for review
            error_dir = os.path.join(output_dir, 'EPA-WWTP', 'error_html', str(year), state)
            os.makedirs(error_dir, exist_ok=True)
            error_file = os.path.join(error_dir, f"{npdes_id}.html")
            with open(error_file, "w", encoding="utf-8") as f:
                f.write(response.text)
            return

        # save valid CSV
        with open(file_path, "w", encoding="utf-8") as file:
            file.write(response.text)

        logger.info(f"Saved data for NPDES ID {npdes_id}")

        # log to checkpoint
        with open(checkpoint_file, "a") as f:
            f.write(f"{npdes_id}\n")

        random_wait(10, 30, logger, "Interval delay between NPDES IDs")

    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to download data for NPDES ID {npdes_id}: {e}")
        time.sleep(30)

# process all NPDES IDs for a specific state and year
def process_npdes_ids(npdes_ids, start_date, end_date, ip_address, output_dir, state, year, logger, checkpoint_file):
    logger.info(f"Processing {len(npdes_ids)} NPDES IDs for {state} in {year}")

    completed_ids = set()
    if os.path.exists(checkpoint_file):
        with open(checkpoint_file, "r") as f:
            completed_ids = set(line.strip() for line in f)

    for npdes_id in npdes_ids:
        if npdes_id in completed_ids:
            logger.info(f"Skipping {npdes_id}, already processed.")
            continue
        download_data_for_npdes(npdes_id, start_date, end_date, ip_address, output_dir, logger, state, year, checkpoint_file)

    logger.info(f"Completed processing for {state} in {year}")

# gracefully exit and checkpoint if SLURM sends SIGUSR1
def handle_sigusr1(signum, frame):
    logger.info("Received SIGUSR1: Saving progress and exiting...")
    sys.exit(0)

signal.signal(signal.SIGUSR1, handle_sigusr1)

# main driver function
def main(state):
    test_folder = "/home/kyl29/CSE_MSE_RXF131/staging/casf/kyl29/facility_data"
    output_dir = "/home/kyl29/CSE_MSE_RXF131/staging/casf/kyl29"
    ip_address = "129.22.1.25"

    logger = setup_logging(output_dir)

    state_folder = os.path.join(test_folder, state)
    if not os.path.isdir(state_folder):
        logger.error(f"State folder {state_folder} does not exist.")
        return

    logger.info(f"Processing state: {state}")

    for filename in os.listdir(state_folder):
        if filename.endswith(f"_{state}.csv"):
            file_path = os.path.join(state_folder, filename)
            year = filename.split("_")[0]  # e.g., 2009_OH.csv → "2009"

            if not year.isdigit():
                logger.warning(f"Skipping {filename}, invalid year format.")
                continue

            # Fixed MM/YYYY format
            start_date = f"01/{year}"
            end_date = f"12/{year}"

            try:
                npdes_ids = read_npdes_file(file_path)
            except csv.Error as e:
                logger.error(f"Skipping file due to CSV error in {filename}: {e}")
                continue
            except Exception as e:
                logger.error(f"Unexpected error reading {filename}: {e}")
                continue

            if npdes_ids:
                checkpoint_file = f"/home/kyl29/CSE_MSE_RXF131/staging/casf/kyl29/checkpoints/{state}_{year}_progress.txt"
                os.makedirs(os.path.dirname(checkpoint_file), exist_ok=True)
                process_npdes_ids(npdes_ids, start_date, end_date, ip_address, output_dir, state, year, logger, checkpoint_file)
            else:
                logger.warning(f"No NPDES IDs found in {filename}")

    # mark state as complete
    state_checkpoint_file = f"/home/kyl29/CSE_MSE_RXF131/staging/casf/kyl29/checkpoints/{state}_progress.txt"
    with open(state_checkpoint_file, "w") as f:
        f.write(f"Finished processing {state}")

# entry point for SLURM or CLI
if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python script.py <STATE>")
        sys.exit(1)

    state = sys.argv[1]
    main(state)
