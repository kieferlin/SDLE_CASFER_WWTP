#!/usr/bin/env python3

# ==============================================================================
# Script: parquet_icis_permits.py
#
# Description:
#   This script processes the 'ICIS_PERMITS.csv' file from a specified input
#   directory. It converts the CSV data into the highly efficient and compressed
#   Apache Parquet format. To optimize for future queries, the data is
#   partitioned into subdirectories based on the 'PERMIT_TYPE_CODE' column.
#   This partitioning allows data systems to read only the relevant permit
#   type data, significantly improving performance.
#
# Usage:
#   python3 parquet_icis_permits.py <input_directory> <output_directory>
#
#   Example:
#   python3 parquet_icis_permits.py ./download_echo_data ./parquet_icis_permits
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

# --- Script Setup ---

# Configure basic logging for standardized and informative output during script execution.
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def process_icis_permits(input_dir: str, output_dir: str):
    """
    Reads the ICIS_PERMITS.csv file, applies a predefined schema, and writes
    the data to a Parquet dataset partitioned by the PERMIT_TYPE_CODE column.

    Args:
        input_dir (str): The directory where ICIS_PERMITS.csv is located.
        output_dir (str): The root directory where the partitioned Parquet
                          dataset will be written.
    """
    # This script is specifically designed to process this file.
    csv_filename = 'ICIS_PERMITS.csv'
    file_to_process = os.path.join(input_dir, csv_filename)

    logging.info(f"Starting processing for {csv_filename}")

    # --- 1. Input File Validation ---
    if not os.path.exists(file_to_process):
        logging.error(f"FATAL: CSV file not found at '{file_to_process}'. Please check the path.")
        exit(1)

    # --- 2. Schema Definition ---
    # Defining a schema is a best practice for performance and data integrity.
    # It tells PyArrow the exact data type for each column, avoiding the need for
    # type inference and preventing potential parsing errors.
    expected_schema = pa.schema([
        pa.field("ACTIVITY_ID", pa.string()),
        pa.field("EXTERNAL_PERMIT_NMBR", pa.string()),
        pa.field("VERSION_NMBR", pa.string()),
        pa.field("FACILITY_TYPE_INDICATOR", pa.string()),
        pa.field("PERMIT_TYPE_CODE", pa.string()), # This column will be used for partitioning.
        pa.field("MAJOR_MINOR_STATUS_FLAG", pa.string()),
        pa.field("PERMIT_STATUS_CODE", pa.string()),
        pa.field("TOTAL_DESIGN_FLOW_NMBR", pa.float64()),
        pa.field("ACTUAL_AVERAGE_FLOW_NMBR", pa.float64()),
        pa.field("STATE_WATER_BODY", pa.string()),
        pa.field("STATE_WATER_BODY_NAME", pa.string()),
        pa.field("PERMIT_NAME", pa.string()),
        pa.field("AGENCY_TYPE_CODE", pa.string()),
        pa.field("ORIGINAL_ISSUE_DATE", pa.string()),
        pa.field("ISSUE_DATE", pa.string()),
        pa.field("ISSUING_AGENCY", pa.string()),
        pa.field("EFFECTIVE_DATE", pa.string()),
        pa.field("EXPIRATION_DATE", pa.string()),
        pa.field("RETIREMENT_DATE", pa.string()),
        pa.field("TERMINATION_DATE", pa.string()),
        pa.field("PERMIT_COMP_STATUS_FLAG", pa.string()),
        pa.field("DMR_NON_RECEIPT_FLAG", pa.string()),
        pa.field("RNC_TRACKING_FLAG", pa.string()),
        pa.field("MASTER_EXTERNAL_PERMIT_NMBR", pa.string()),
        pa.field("TMDL_INTERFACE_FLAG", pa.string()),
        pa.field("EDMR_AUTHORIZATION_FLAG", pa.string()),
        pa.field("PRETREATMENT_INDICATOR_CODE", pa.string()),
        pa.field("RAD_WBD_HUC12S", pa.string())
    ])

    # --- 3. CSV Reading Configuration ---
    # These options control how the CSV file is read into memory.
    read_options = pv_csv.ReadOptions(
        skip_rows=1,  # The source CSV has a header row that we need to skip.
        column_names=expected_schema.names  # Apply our defined column names.
    )
    convert_options = pv_csv.ConvertOptions(
        column_types=expected_schema  # Enforce the data types from our schema.
    )
    parse_options = pv_csv.ParseOptions(
        delimiter=',',  # Standard comma delimiter.
        quote_char='"', # Standard double-quote character for quoted fields.
    )

    try:
        # --- 4. Read CSV and Write to Parquet ---
        logging.info("Reading the CSV file into an Arrow Table...")
        arrow_table = pv_csv.read_csv(
            file_to_process,
            read_options=read_options,
            parse_options=parse_options,
            convert_options=convert_options
        )
        logging.info(f"Successfully read {arrow_table.num_rows} rows from {csv_filename}")

        logging.info(f"Writing partitioned Parquet data to: {output_dir}")
        # write_to_dataset is used for creating partitioned datasets.
        pq.write_to_dataset(
            arrow_table,
            root_path=output_dir,
            # This is the key to partitioning. It creates a directory structure like:
            # /output_dir/PERMIT_TYPE_CODE=NPD/data.parquet
            # /output_dir/PERMIT_TYPE_CODE=GPC/data.parquet
            partition_cols=["PERMIT_TYPE_CODE"],
            # Snappy is a fast compressor, offering a good balance between file size and speed.
            compression='snappy',
            # This makes the script idempotent. If run again, it won't fail if the
            # output directories already exist; it will overwrite the data.
            existing_data_behavior='overwrite_or_ignore'
        )
        logging.info("Parquet write operation completed successfully.")

    except Exception:
        # Log the full traceback for easier debugging without crashing the script.
        logging.error("An unexpected error occurred during file processing.", exc_info=True)
        exit(1)

def main():
    """
    Parses command-line arguments and orchestrates the conversion process.
    """
    parser = argparse.ArgumentParser(
        description="Convert ICIS_PERMITS.csv to a partitioned Parquet dataset."
    )
    parser.add_argument("input_dir", type=str, help="Directory containing the source ICIS_PERMITS.csv file.")
    parser.add_argument("output_dir", type=str, help="Directory to write the partitioned Parquet files to.")
    args = parser.parse_args()

    # For user convenience, create the output directory if it doesn't already exist.
    os.makedirs(args.output_dir, exist_ok=True)

    # Execute the main processing function with the provided paths.
    process_icis_permits(args.input_dir, args.output_dir)

if __name__ == "__main__":
    main()