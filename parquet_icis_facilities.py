#!/usr/bin/env python3

# ==============================================================================
# Script: parquet_icis_facilities.py
#
# Description:
#   This script performs an ETL (Extract, Transform, Load) process on the
#   EPA ECHO 'ICIS_FACILITIES.csv' file. It reads the raw CSV data, performs
#   transformations, and writes the output to a partitioned Parquet dataset.
#
#   Key transformations:
#   - Renames 'STATE_CODE' to 'state' for consistency across the data package.
#   - Enriches the data by filling in missing 'state' values. If 'state' is
#     blank, it is backfilled using the first two characters of the 'NPDES_ID',
#     a common data quality issue in the source file. This ensures that
#     records are partitioned correctly by their actual state.
#
#   The final output is a query-optimized Parquet dataset partitioned by 'state'.
#   This structure allows analytics engines to efficiently filter and read only
#   the data for specific states, significantly improving query performance.
#
# Usage:
#   python3 parquet_icis_facilities.py <input_dir> <output_dir>
#
# Dependencies:
#   - Python 3
#   - pyarrow
#   - pandas
# ==============================================================================

import os
import logging
import argparse
import pandas as pd
import pyarrow as pa
from pyarrow import csv as pv_csv
from pyarrow import parquet as pq

# --- SCRIPT SETUP ---

# Configure basic logging to display info-level messages in a standard format.
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# --- CORE PROCESSING FUNCTION ---

def process_icis_facilities(input_dir: str, output_dir: str):
    """
    Reads the ICIS Facilities CSV, enriches missing state data, and writes
    the result into a partitioned Parquet dataset.

    This function encapsulates the entire ETL logic for the facilities data.

    Args:
        input_dir (str): The directory containing the 'ICIS_FACILITIES.csv' file.
        output_dir (str): The root directory where the partitioned Parquet data
                          will be written.
    """
    csv_filename = 'ICIS_FACILITIES.csv'
    # Construct the full path to the source file. This makes the script portable
    # and independent of where it's run from.
    file_to_process = os.path.join(input_dir, csv_filename)

    logging.info(f"Starting processing for {csv_filename}")

    # A crucial check to ensure the source file exists before proceeding.
    if not os.path.exists(file_to_process):
        logging.error(f"CSV file not found at: {file_to_process}. Exiting.")
        exit(1)

    logging.info(f"Found file to process: {file_to_process}")

    # --- Schema Definition for ICIS_FACILITIES.csv ---
    # Defining a schema explicitly is a best practice. It's faster than type
    # inference and prevents errors from unexpected data types in the source CSV.
    expected_schema = pa.schema([
        pa.field("ICIS_FACILITY_INTEREST_ID", pa.string()),
        pa.field("NPDES_ID", pa.string()),
        pa.field("FACILITY_UIN", pa.string()),
        pa.field("FACILITY_TYPE_CODE", pa.string()),
        pa.field("FACILITY_NAME", pa.string()),
        pa.field("LOCATION_ADDRESS", pa.string()),
        pa.field("SUPPLEMENTAL_ADDRESS_TEXT", pa.string()),
        pa.field("CITY", pa.string()),
        pa.field("COUNTY_CODE", pa.string()),
        pa.field("STATE_CODE", pa.string()),
        pa.field("ZIP", pa.string()),
        pa.field("GEOCODE_LATITUDE", pa.float64()),
        pa.field("GEOCODE_LONGITUDE", pa.float64()),
        pa.field("IMPAIRED_WATERS", pa.string())
    ])

    # --- PyArrow CSV Reading Options ---
    # These options tell PyArrow how to correctly interpret the CSV file.
    read_options = pv_csv.ReadOptions(
        skip_rows=1,  # Skip the header row in the CSV file.
        column_names=expected_schema.names  # Use the column names from our schema.
    )
    # This enforces the data types defined in our schema during conversion.
    convert_options = pv_csv.ConvertOptions(column_types=expected_schema)
    # Standard parsing options for a comma-separated, double-quoted file.
    parse_options = pv_csv.ParseOptions(delimiter=',', quote_char='"')

    try:
        logging.info("Reading the CSV file into an Arrow Table...")
        # Read the entire CSV file into memory as a high-performance PyArrow Table.
        arrow_table = pv_csv.read_csv(
            file_to_process,
            read_options=read_options,
            parse_options=parse_options,
            convert_options=convert_options
        )
        logging.info(f"Successfully read {arrow_table.num_rows} rows from {csv_filename}.")

        # --- Data Transformation: Rename Column ---
        # To maintain consistency across all datasets in this package, we rename
        # 'STATE_CODE' to 'state'. This makes joining data later much simpler.
        new_column_names = list(expected_schema.names)
        state_code_index = new_column_names.index('STATE_CODE')
        new_column_names[state_code_index] = 'state'
        arrow_table = arrow_table.rename_columns(new_column_names)
        logging.info("Renamed column 'STATE_CODE' to 'state' for consistency.")

        # This block addresses a known data quality issue where 'STATE_CODE' is
        # missing but can be inferred from the 'NPDES_ID'.
        logging.info("Converting to Pandas DataFrame to fix missing state codes...")
        df = arrow_table.to_pandas()

        # Create a boolean 'mask' to identify rows where 'state' is null or an empty string.
        mask_missing_state = (df['state'].isnull()) | (df['state'] == '')

        num_missing = mask_missing_state.sum()
        if num_missing > 0:
            logging.info(f"Found {num_missing} rows with missing state codes. Attempting to fix...")
            # For only the rows identified by the mask, update the 'state' column
            # by slicing the first two characters from the 'NPDES_ID' column.
            df.loc[mask_missing_state, 'state'] = df.loc[mask_missing_state, 'NPDES_ID'].str[:2]

            # Report on the outcome of the fix. 
            still_missing = ((df['state'].isnull()) | (df['state'] == '')).sum()
            logging.info(f"Fixed {num_missing - still_missing} rows. {still_missing} rows may still have no state.")
        else:
            logging.info("No missing state codes found.")

        # After manipulation, convert the cleaned Pandas DataFrame back to a PyArrow Table.
        # Explicitly apply the final schema to ensure data types are correct.
        # `preserve_index=False` prevents Pandas' index from being added as a column.
        final_schema = arrow_table.schema
        arrow_table = pa.Table.from_pandas(df, schema=final_schema, preserve_index=False)

        # --- Write Data to Parquet ---
        logging.info(f"Writing data to partitioned Parquet dataset at {output_dir}")
        # Write the final Arrow Table to a Parquet dataset.
        pq.write_to_dataset(
            arrow_table,
            root_path=output_dir,
            # Partitioning by 'state' creates a directory structure like /state=CA/, /state=NY/ (predicate pushdown)
            partition_cols=['state'],
            # Snappy is a fast and efficient compression algorithm, good for general use.
            compression='snappy',
            # This makes the script idempotent. If you run it again, it will safely
            # overwrite existing data for each partition without failing.
            existing_data_behavior='overwrite_or_ignore'
        )
        logging.info(f"Successfully wrote partitioned data to {output_dir}")

    except Exception as e:
        # If any error occurs, log it with a traceback and exit with a non-zero
        # status code to signal that the pipeline failed.
        logging.error(f"A critical error occurred while processing {csv_filename}.", exc_info=True)
        exit(1)

# --- SCRIPT ORCHESTRATOR ---

def main():
    """
    Main function to parse command-line arguments and run the processing script.
    This makes the script a reusable command-line tool.
    """
    parser = argparse.ArgumentParser(
        description="Convert the ICIS Facilities CSV file into a partitioned Parquet dataset."
    )
    # Define command-line arguments for input and output directories. This avoids
    # hardcoding paths and makes the script flexible.
    parser.add_argument("input_dir", type=str, help="The input directory containing the ICIS_FACILITIES.csv file.")
    parser.add_argument("output_dir", type=str, help="The output directory for the Parquet dataset.")

    args = parser.parse_args()

    # Ensure the output directory exists before the script tries to write to it.
    os.makedirs(args.output_dir, exist_ok=True)

    # Call the core processing function with the parsed arguments.
    process_icis_facilities(args.input_dir, args.output_dir)

if __name__ == "__main__":
    main()