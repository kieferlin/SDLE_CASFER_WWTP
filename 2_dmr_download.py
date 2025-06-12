import os
import csv
import requests
import time
import logging
import logging.handlers  # import handlers for logging rotation
import random
import sys
import signal
from datetime import datetime
from urllib.parse import urlencode


# robust rotating logger
def setup_logging(output_dir):
    """
    Sets up logging to a rotating file and the console.
    This prevents "Stale file handle" errors in long-running jobs on network filesystems.
    """
    log_dir = os.path.join(output_dir, 'logs')
    os.makedirs(log_dir, exist_ok=True)
    # the handler will add dates during rotation
    log_file = os.path.join(log_dir, 'epa_data_download.log')

    # get the named logger
    logger = logging.getLogger("EPA-Downloader")
    logger.setLevel(logging.INFO)

    # prevent messages from being propagated to the root logger, avoiding duplicate logs
    logger.propagate = False

    # add handlers only if they don't already exist
    if not logger.handlers:
        # create a timed rotating file handler (rotates daily, keeps 30 days of logs)
        file_handler = logging.handlers.TimedRotatingFileHandler(
            log_file, when='D', interval=1, backupCount=30
        )
        file_handler.setFormatter(
            logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        )
        logger.addHandler(file_handler)

        # create a console handler for real-time output
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(
            logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        )
        logger.addHandler(console_handler)

    return logger


def random_wait(min_seconds, max_seconds, logger, message):
    """Random delay between requests to reduce load and avoid blocking."""
    wait_time = random.uniform(min_seconds, max_seconds)
    logger.info(f"Random wait for {wait_time:.1f} seconds: {message}")
    time.sleep(wait_time)


def read_npdes_file(file_path):
    """Read NPDES IDs from a CSV file, skipping headers and handling null bytes."""
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


def build_query_url(start_date, end_date, npdes_id, ip_address):
    """Build the EPA DMR data URL using the provided parameters."""
    params = {
        'p_start_date': start_date,
        'p_end_date': end_date,
        'p_npdes_id': npdes_id,
        'p_ipaddr': ip_address,
        'output': 'CSV'
    }
    query_str = urlencode(params)
    return f"https://echodata.epa.gov/echo/dmr_rest_services.get_monitoring_data_csv?{query_str}"


def download_data_for_npdes(npdes_id, start_date, end_date, ip_address, output_dir, logger, state, year, checkpoint_file):
    """Download and save the CSV file for one NPDES ID."""
    state_folder = os.path.join(output_dir, '2_dmr_download', str(year), state)
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
            error_dir = os.path.join(output_dir, '2_dmr_download', 'error_html', str(year), state)
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


def process_npdes_ids(npdes_ids, start_date, end_date, ip_address, output_dir, state, year, logger, checkpoint_file):
    """Process all NPDES IDs for a specific state and year."""
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


# define logger at global scope for signal handler access 
logger = None

def handle_sigusr1(signum, frame):
    """Gracefully exit if SLURM sends SIGUSR1."""
    if logger:
        logger.info("Received SIGUSR1 from SLURM: Exiting cleanly for requeue...")
    else:
        # fallback if logger hasn't been initialized yet
        print("Received SIGUSR1 from SLURM: Exiting cleanly for requeue...")
    sys.exit(0)


# register the signal handler
signal.signal(signal.SIGUSR1, handle_sigusr1)


def main(state):
    """Main driver function."""
    # use the global logger variable
    global logger

    facility_data_root = "/home/kyl29/CSE_MSE_RXF131/staging/casf/kyl29/1_facility_download"
    output_dir = "/home/kyl29/CSE_MSE_RXF131/staging/casf/kyl29"
    ip_address = "129.22.1.25"

    logger = setup_logging(output_dir)

    state_folder = os.path.join(facility_data_root, state)
    if not os.path.isdir(state_folder):
        logger.error(f"State folder {state_folder} does not exist.")
        return

    logger.info(f"Processing state: {state}")

    for filename in os.listdir(state_folder):
        if filename.endswith(f"_{state}.csv"):
            file_path = os.path.join(state_folder, filename)
            year = filename.split("_")[0]

            if not year.isdigit():
                logger.warning(f"Skipping {filename}, invalid year format.")
                continue

            start_date = f"01/01/{year}"
            end_date = f"12/31/{year}"

            try:
                npdes_ids = read_npdes_file(file_path)
            except csv.Error as e:
                logger.error(f"Skipping file due to CSV error in {filename}: {e}")
                continue
            except Exception as e:
                logger.error(f"Unexpected error reading {filename}: {e}")
                continue

            if npdes_ids:
                checkpoint_dir = f"/home/kyl29/CSE_MSE_RXF131/staging/casf/kyl29/2_dmr_download_checkpoints"
                os.makedirs(checkpoint_dir, exist_ok=True)
                checkpoint_file = os.path.join(checkpoint_dir, f"{state}_{year}_progress.txt")
                process_npdes_ids(npdes_ids, start_date, end_date, ip_address, output_dir, state, year, logger, checkpoint_file)
            else:
                logger.warning(f"No NPDES IDs found in {filename}")

    logger.info(f"Completed all available years for state: {state}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python 2_dmr_download.py <STATE_ABBREVIATION>")
        sys.exit(1)

    state_arg = sys.argv[1]
    main(state_arg)