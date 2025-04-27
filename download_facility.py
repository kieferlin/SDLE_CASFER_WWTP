import requests
import time
from datetime import datetime
import os
import csv
from urllib.parse import urlencode
import logging
from logging.handlers import RotatingFileHandler
import random

# Setup logging for the script
def setup_logging(output_dir, date):
    year = date.strftime("%Y")
    day = date.strftime("%Y-%m-%d")
    log_dir = os.path.join(output_dir, 'logs', year, day)
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, f'facility_data_download_{day}.log')
    
    file_handler = RotatingFileHandler(log_file, maxBytes=10*1024*1024, backupCount=5)
    file_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(file_formatter)
    
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(file_formatter)
    
    logger = logging.getLogger(f"EPA_{day}")
    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    logger.setLevel(logging.INFO)
    
    return logger

# Wait with a fixed delay
def wait_with_message(seconds, logger, message):
    logger.info(f"Waiting {seconds} seconds: {message}")
    time.sleep(seconds)

# Random wait
def random_wait(min_seconds, max_seconds, logger, message):
    wait_time = random.uniform(min_seconds, max_seconds)
    logger.info(f"Random wait for {wait_time:.1f} seconds: {message}")
    time.sleep(wait_time)

# Build query URL for facility data
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
        'qcolumns': ','.join(str(i) for i in range(1, 81))  # Columns 1 to 80
    }
    query_str = urlencode(params)
    return f"https://echodata.epa.gov/echo/dmr_rest_services.get_custom_data_annual?{query_str}"

# Download data for a specific year and state
def download_data_for_facility(year, state, ip_address, output_dir, logger):
    query_url = build_facility_query_url(year, state, ip_address)
    logger.info(f"Request URL: {query_url}")
    
    random_wait(3, 10, logger, "Pre-request delay")
    
    try:
        response = requests.get(query_url)
        response.raise_for_status()
        
        wait_with_message(5, logger, "Post-request delay")
        
        lines = response.text.splitlines()
        num_rows = len(lines) - 1
        
        logger.info(f"Downloaded data for Year: {year}, State: {state}, Rows: {num_rows}")
        
        state_dir = os.path.join(output_dir, state)
        os.makedirs(state_dir, exist_ok=True)
        file_path = os.path.join(state_dir, f"{year}_{state}.csv")
        
        with open(file_path, "w") as file:
            file.write(response.text)
        logger.info(f"Saved data to {file_path}")
        
        random_wait(10, 30, logger, "Interval delay between requests")
    
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to download data for Year: {year}, State: {state} - Error: {str(e)}")
        wait_with_message(30, logger, "Error recovery delay")

# Process all states and years
def process_states_and_years(states, years, ip_address, output_dir):
    logger = setup_logging(output_dir, datetime.now())
    logger.info(f"Starting processing for States: {len(states)}, Years: {len(years)}")
    
    for state in states:
        for year in years:
            download_data_for_facility(year, state, ip_address, output_dir, logger)
    
    logger.info("Completed processing for all states and years")

# Main function
def main():
    states = ["AL", "AR", "AZ", "CA", "CO", "CT", "DC", "DE", "FL", "GA", "IA", "ID", "IL", "IN", "KS", 
                "KY", "LA", "MA", "MD", "ME", "MI", "MN", "MO", "MS", "MT", "NC", "ND", "NE", "NH", "NJ", 
                "NM", "NV", "NY", "OH", "OK", "OR", "PA", "RI", "SC", "SD", "TN", "TX", "UT", "VA", "VT", 
                "WA", "WI", "WV", "WY"]
    # states = ["TX", "UT", "VA", "VT", "WA", "WI", "WV", "WY"]
    # years = range(2014, 2016)
    years = range(2000, 2026) 
    output_dir = "/home/kyl29/CSE_MSE_RXF131/staging/casf/kyl29/facility_data"
    ip_address = "129.22.1.25"
    
    print(f"Processing data for {len(states)} states and {len(years)} years")
    process_states_and_years(states, years, ip_address, output_dir)
    print("Completed processing.")

if __name__ == "__main__":
    main()