#!/usr/bin/env python3

# ==============================================================================
# Script: parquet_npdes_dmrs.py
#
# Description:
#   A batch processing script to convert raw EPA ECHO DMR (Discharge Monitoring
#   Report) data from CSV format to a partitioned Parquet dataset. The script
#   iterates over a user-defined range of years, processing each year's data file.
#
#   Key transformations include:
#   - Deriving a 'state' column from the permit number for partitioning.
#   - Adding a 'year' column, also for partitioning.
#   - Filtering the consolidated pre-2009 data file to extract a specific year.
#
#   The final output is a single, query-optimized Parquet dataset partitioned by
#   'year' and 'state', which dramatically improves the performance of subsequent
#   data analysis compared to working with individual large CSV files.
#
# Usage:
#   python3 parquet_npdes_dmrs.py <input_dir> <output_dir> --start_year YYYY --end_year YYYY
#
# Dependencies:
#   - Python 3
#   - pyarrow
# ==============================================================================

import os
import logging
import argparse
import pyarrow as pa
from pyarrow import csv as pv_csv
from pyarrow import parquet as pq
from pyarrow import compute as pc

# --- SCRIPT SETUP ---

# Configure basic logging to display info-level messages.
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# --- CORE PROCESSING FUNCTION & SCRIPT ORCHESTRATOR ---

def aggregate_data_for_year():
    """
    Main function to orchestrate the batch processing of ECHO DMR data.

    It parses command-line arguments to get the source/destination directories
    and a year range. It then loops through each year, finds the corresponding
    CSV file, processes it, and writes the result to a partitioned Parquet dataset.
    """
    # Set up the argument parser to make the script a flexible command-line tool.
    parser = argparse.ArgumentParser(
        description="Run a batch job to process multiple years of ECHO DMR data."
    )
    # Define the command-line arguments the script accepts.
    parser.add_argument("input_dir", type=str, help="The directory containing the source NPDES_DMRS_*.csv files.")
    parser.add_argument("output_dir", type=str, help="The root directory for the output Parquet dataset.")
    parser.add_argument("--start_year", type=int, required=True, help="The first year in the range to process (inclusive).")
    parser.add_argument("--end_year", type=int, required=True, help="The last year in the range to process (inclusive).")
    
    # Parse the arguments provided by the user.
    args = parser.parse_args()

    # Ensure the output directory exists before trying to write to it.
    os.makedirs(args.output_dir, exist_ok=True)
    
    # --- Batch Processing Loop ---
    # Loop through the specified range of years, from start_year to end_year.
    for year in range(args.start_year, args.end_year + 1):
        specific_year = str(year)
        logging.info(f"--- Starting processing for YEAR={specific_year} ---")

        # --- File Discovery Logic ---
        # The ECHO dataset uses a different naming convention for data before fiscal year 2009.
        year_int = int(specific_year)
        if year_int < 2009:
            csv_filename = 'NPDES_DMRS_PREFY2009.csv'
            logging.info(f"Year {specific_year} is before 2009. Using consolidated file: {csv_filename}")
        else:
            csv_filename = f'NPDES_DMRS_FY{specific_year}.csv'

        # Construct the full path to the source CSV file.
        file_to_process = os.path.join(args.input_dir, csv_filename)

        # Robustly handle cases where a file for a specific year might be missing.
        if not os.path.exists(file_to_process):
            logging.warning(f"CSV file not found for year {specific_year} at: {file_to_process}. Skipping to next year.")
            continue # Move to the next iteration of the loop.

        logging.info(f"Found source file: {file_to_process}")

        # --- Schema Definition for ECHO DMR Data ---
        # Defining a schema upfront is a crucial optimization for PyArrow. It avoids
        # slow and potentially error-prone type inference and ensures data consistency.
        # All columns are read as strings for robustness, as source CSVs can have
        # mixed types or formatting issues in numeric/date columns.
        expected_schema = pa.schema([
             pa.field("ACTIVITY_ID", pa.string()),
            pa.field("EXTERNAL_PERMIT_NMBR", pa.string()),
            pa.field("VERSION_NMBR", pa.string()),
            pa.field("PERM_FEATURE_ID", pa.string()),
            pa.field("PERM_FEATURE_NMBR", pa.string()),
            pa.field("PERM_FEATURE_TYPE_CODE", pa.string()),
            pa.field("LIMIT_SET_ID", pa.string()),
            pa.field("LIMIT_SET_DESIGNATOR", pa.string()),
            pa.field("LIMIT_SET_SCHEDULE_ID", pa.string()),
            pa.field("LIMIT_ID", pa.string()),
            pa.field("LIMIT_BEGIN_DATE", pa.string()),
            pa.field("LIMIT_END_DATE", pa.string()),
            pa.field("NMBR_OF_SUBMISSION", pa.string()),
            pa.field("NMBR_OF_REPORT", pa.string()),
            pa.field("PARAMETER_CODE", pa.string()),
            pa.field("PARAMETER_DESC", pa.string()),
            pa.field("MONITORING_LOCATION_CODE", pa.string()),
            pa.field("STAY_TYPE_CODE", pa.string()),
            pa.field("LIMIT_VALUE_ID", pa.string()),
            pa.field("LIMIT_VALUE_TYPE_CODE", pa.string()),
            pa.field("LIMIT_VALUE_NMBR", pa.string()),
            pa.field("LIMIT_UNIT_CODE", pa.string()),
            pa.field("LIMIT_UNIT_DESC", pa.string()),
            pa.field("STANDARD_UNIT_CODE", pa.string()),
            pa.field("STANDARD_UNIT_DESC", pa.string()),
            pa.field("LIMIT_VALUE_STANDARD_UNITS", pa.string()),
            pa.field("STATISTICAL_BASE_CODE", pa.string()),
            pa.field("STATISTICAL_BASE_TYPE_CODE", pa.string()),
            pa.field("LIMIT_VALUE_QUALIFIER_CODE", pa.string()),
            pa.field("OPTIONAL_MONITORING_FLAG", pa.string()),
            pa.field("LIMIT_SAMPLE_TYPE_CODE", pa.string()),
            pa.field("LIMIT_FREQ_OF_ANALYSIS_CODE", pa.string()),
            pa.field("STAY_VALUE_NMBR", pa.string()),
            pa.field("LIMIT_TYPE_CODE", pa.string()),
            pa.field("DMR_EVENT_ID", pa.string()),
            pa.field("MONITORING_PERIOD_END_DATE", pa.string()),
            pa.field("DMR_SAMPLE_TYPE_CODE", pa.string()),
            pa.field("DMR_FREQ_OF_ANALYSIS_CODE", pa.string()),
            pa.field("REPORTED_EXCURSION_NMBR", pa.string()),
            pa.field("DMR_FORM_VALUE_ID", pa.string()),
            pa.field("VALUE_TYPE_CODE", pa.string()),
            pa.field("DMR_VALUE_ID", pa.string()),
            pa.field("DMR_VALUE_NMBR", pa.string()),
            pa.field("DMR_UNIT_CODE", pa.string()),
            pa.field("DMR_UNIT_DESC", pa.string()),
            pa.field("DMR_VALUE_STANDARD_UNITS", pa.string()),
            pa.field("DMR_VALUE_QUALIFIER_CODE", pa.string()),
            pa.field("VALUE_RECEIVED_DATE", pa.string()),
            pa.field("DAYS_LATE", pa.string()),
            pa.field("NODI_CODE", pa.string()),
            pa.field("EXCEEDENCE_PCT", pa.string()),
            pa.field("NPDES_VIOLATION_ID", pa.string()),
            pa.field("VIOLATION_CODE", pa.string()),
            pa.field("RNC_DETECTION_CODE", pa.string()),
            pa.field("RNC_DETECTION_DATE", pa.string()),
            pa.field("RNC_RESOLUTION_CODE", pa.string()),
            pa.field("RNC_RESOLUTION_DATE", pa.string())
        ])

        # --- PyArrow CSV Reading Options ---
        # skip_rows=1 to ignore the header row in the CSV file.
        # column_names are taken from our predefined schema.
        read_options = pv_csv.ReadOptions(skip_rows=1, column_names=expected_schema.names)
        # Enforce the data types defined in our schema during conversion.
        convert_options = pv_csv.ConvertOptions(column_types=expected_schema)
        # Standard CSV parsing options.
        parse_options = pv_csv.ParseOptions(delimiter=',', quote_char='"')

        # Use a try/except block to gracefully handle file I/O or processing errors.
        # A failure for one year will halt the entire batch job to prevent partial results.
        try:
            logging.info("Reading CSV file into an Arrow Table...")
            arrow_table = pv_csv.read_csv(
                file_to_process,
                read_options=read_options,
                parse_options=parse_options,
                convert_options=convert_options
            )
            logging.info(f"Successfully read {arrow_table.num_rows} rows from {csv_filename}.")

            # --- Data Transformation ---

            # 1. Derive the 'state' column from the permit number for partitioning.
            # The state code is the first two characters of the permit number.
            permit_numbers_col = arrow_table.column("EXTERNAL_PERMIT_NMBR")
            state_col = pc.utf8_slice_codeunits(permit_numbers_col, start=0, stop=2)
            arrow_table = arrow_table.append_column('state', state_col)

            # 2. Add the 'year' column, also for partitioning.
            year_col = pa.array([specific_year] * len(arrow_table), type=pa.string())
            arrow_table = arrow_table.append_column('year', year_col)
            logging.info("Added 'state' and 'year' columns for partitioning.")
            
            # 3. Handle the special case for the consolidated pre-2009 data file.
            # This file contains data for multiple years, so we must filter it.
            if year_int < 2009:
                logging.info(f"Filtering PREFY2009 data to keep only records from year {specific_year}...")
                # Extract the year from the 'MONITORING_PERIOD_END_DATE' column (format MM/DD/YYYY).
                date_col = arrow_table.column('MONITORING_PERIOD_END_DATE')
                year_from_date = pc.utf8_slice_codeunits(date_col, start=-4)
                # Create a boolean mask to filter the table.
                filter_mask = pc.equal(year_from_date, specific_year)
                arrow_table = arrow_table.filter(filter_mask)
                logging.info(f"Filtered down to {arrow_table.num_rows} rows for year {specific_year}.")

            # --- Write Data to Parquet ---
            logging.info(f"Writing data to partitioned Parquet dataset at {args.output_dir}")
            pq.write_to_dataset(
                arrow_table,
                root_path=args.output_dir,
                # Partitioning creates a /year=YYYY/state=SS/ directory structure. This
                # enables query engines to read only the necessary data - predicate pushdown - 
                # to dramatically speeding up queries.
                partition_cols=['year', 'state'],
                compression='snappy', 
                # This makes the script idempotent. Re-running for the same year will safely
                # overwrite existing data for that year/state without side effects or errors.
                existing_data_behavior='overwrite_or_ignore'
            )
            logging.info(f"Successfully wrote data for year {specific_year}.")

        except Exception as e:
            # If any error occurs during the processing of a single year, log it
            # and exit the script with a non-zero status code to signal failure.
            logging.error(f"A critical error occurred while processing year {specific_year}. Halting script.", exc_info=True)
            exit(1)

if __name__ == "__main__":
    aggregate_data_for_year()