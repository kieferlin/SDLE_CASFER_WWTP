#!/usr/bin/env python3

# ==============================================================================
# Script: parquet_npdes_naics.py
#
# Description:
#   A data processing script that converts the NPDES_NAICS.csv file into a
#   partitioned Apache Parquet dataset. This file links NPDES permit IDs to
#   their North American Industry Classification System (NAICS) codes, which
#   is essential for analyzing data by industrial sector.
#
#   Key transformations include:
#   - Reading the source CSV with a predefined schema for robustness.
#   - Deriving a 'state' column from the NPDES permit ID.
#   - Writing the data to a Parquet dataset partitioned by 'state'. This
#     optimization significantly speeds up downstream joins with other
#     state-partitioned datasets.
#
# Usage:
#   python3 parquet_npdes_naics.py <input_dir> <output_dir>
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
# Configure basic logging to display info-level messages in a standard format.
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# --- CORE PROCESSING FUNCTION ---

def process_npdes_naics(input_dir: str, output_dir: str):
    """
    Reads the NPDES NAICS CSV file, adds a 'state' column derived from the
    NPDES ID, and writes the result into a partitioned Parquet dataset.

    Args:
        input_dir (str): Directory containing the NPDES_NAICS.csv file.
        output_dir (str): Root directory for the output partitioned Parquet data.
    """
    # Define the expected filename for the NAICS data.
    csv_filename = 'NPDES_NAICS.csv'
    file_to_process = os.path.join(input_dir, csv_filename)

    logging.info(f"Starting processing for {csv_filename}")

    # Fail fast if the required input file does not exist. This provides a clear
    # error message instead of letting the script fail later.
    if not os.path.exists(file_to_process):
        logging.error(f"FATAL: CSV file not found at: {file_to_process}. Exiting.")
        exit(1)

    logging.info(f"Found file to process: {file_to_process}")

    # --- Schema definition for NPDES_NAICS.csv ---
    # Defining a schema upfront is faster and safer than type inference.
    # NAICS_CODE is treated as a string because it's an identifier, not a number for calculation.
    expected_schema = pa.schema([
        pa.field("NPDES_ID", pa.string()),
        pa.field("NAICS_CODE", pa.string()),
        pa.field("NAICS_DESC", pa.string()),
        pa.field("PRIMARY_INDICATOR_FLAG", pa.string())
    ])

    # --- PyArrow CSV Reading Options ---
    read_options = pv_csv.ReadOptions(
        skip_rows=1, # Ignore the header row in the CSV
        column_names=expected_schema.names
    )
    convert_options = pv_csv.ConvertOptions(column_types=expected_schema)
    parse_options = pv_csv.ParseOptions(delimiter=',', quote_char='"')

    try:
        logging.info("Reading the CSV file into an Arrow Table...")
        arrow_table = pv_csv.read_csv(
            file_to_process,
            read_options=read_options,
            parse_options=parse_options,
            convert_options=convert_options
        )
        logging.info(f"Successfully read {arrow_table.num_rows} rows from {csv_filename}.")

        # --- Data Transformation: Add 'state' column ---
        # The first two characters of the NPDES_ID are the state code. This column
        # is added to enable efficient partitioning.
        logging.info("Deriving 'state' column from 'NPDES_ID'.")
        npdes_id_col = arrow_table.column("NPDES_ID")
        state_col = pc.utf8_slice_codeunits(npdes_id_col, start=0, stop=2)
        arrow_table = arrow_table.append_column('state', state_col)
        logging.info("Successfully added 'state' column.")

        # --- Write to a partitioned Parquet dataset ---
        logging.info(f"Writing data to partitioned Parquet dataset at {output_dir}")
        pq.write_to_dataset(
            arrow_table,
            root_path=output_dir,
            # Partitioning by 'state' creates a directory structure like /state=AL/, /state=AK/, etc.
            # This allows query engines like Spark to only read the data for the states
            # relevant to a query, dramatically improving performance.
            partition_cols=['state'],
            compression='snappy',
            # This makes the script idempotent. Re-running it will safely overwrite
            # existing data without causing errors.
            existing_data_behavior='overwrite_or_ignore'
        )
        logging.info(f"Successfully wrote partitioned data to {output_dir}")

    except Exception as e:
        logging.error(f"A critical error occurred while processing {csv_filename}.", exc_info=True)
        exit(1)

# --- SCRIPT ENTRYPOINT ---
def main():
    """Parses command-line arguments and orchestrates the file processing."""
    parser = argparse.ArgumentParser(
        description="Convert the NPDES NAICS CSV file into a partitioned Parquet dataset."
    )
    parser.add_argument("input_dir", type=str, help="The input directory containing the CSV.")
    parser.add_argument("output_dir", type=str, help="The output directory for the Parquet dataset.")
    args = parser.parse_args()

    # Ensure the output directory exists before writing to it.
    os.makedirs(args.output_dir, exist_ok=True)
    process_npdes_naics(args.input_dir, args.output_dir)

if __name__ == "__main__":
    main()