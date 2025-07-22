#!/usr/bin/env python3

# ==============================================================================
# Script: read_parquet.py
#
# Description:
#   A general-purpose command-line tool for inspecting and querying
#   partitioned Parquet datasets. It allows users to view the schema,
#   select columns, filter rows, and limit the number of results displayed.
#
#   This tool is invaluable for quick data exploration and validation of the
#   ETL pipeline outputs.
#
# Usage:
#   See the examples at the end of this file or run with --help:
#   python3 read_parquet.py --help
#
# Dependencies:
#   - Python 3
#   - pyarrow
#   - pandas
# ==============================================================================

import pyarrow.parquet as pq
import pandas as pd
import argparse

def main():
    """Parses arguments and executes the Parquet data query."""
    parser = argparse.ArgumentParser(
        description="A general-purpose tool to query and view Parquet datasets.",
        # This formatter preserves newline characters in help text for better readability.
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument("path", type=str, help="The root path to the Parquet dataset directory.")
    parser.add_argument(
        "--columns", "-c", nargs='+',
        help="Space-separated list of columns to display. If not provided, shows all."
    )
    parser.add_argument(
        "--filter", "-f", nargs=3, action='append',
        metavar=('COLUMN', 'OPERATOR', 'VALUE'),
        help="Filter to apply. Can be used multiple times.\n"
             "Example: --filter state = OH --filter year = 2025"
    )
    parser.add_argument(
        "--rows", "-n", type=int, default=10,
        help="Number of rows to display (default: 10)."
    )
    parser.add_argument(
        "--schema", "-s", action='store_true',
        help="Only display the dataset schema and exit."
    )
    args = parser.parse_args()

    # --- Configure Pandas for better terminal display ---
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 120)
    pd.set_option('display.max_colwidth', 40)

    try:
        # pq.ParquetDataset reads a directory of Parquet files as a single logical table.
        dataset = pq.ParquetDataset(args.path)
        print(f"--- Schema for dataset at: {args.path} ---")
        # Partitioning columns are automatically included in the schema.
        print(f"Available columns: {dataset.schema.names}\n")

        # If the user only wants the schema, we exit early.
        if args.schema:
            return

        # =========================================================================
        # || PROCESS FILTERS: CONVERT TYPES FOR PYARROW                        ||
        # =========================================================================
        # Command-line arguments are always strings. PyArrow's filtering requires
        # values to have the correct data type (e.g., int, float) to match the
        # column's schema. This block intelligently converts filter values.
        processed_filters = []
        if args.filter:
            for f_col, f_op, f_val_str in args.filter:
                try:
                    # First, try to convert the value to an integer.
                    value = int(f_val_str)
                except ValueError:
                    try:
                        # If that fails, try to convert it to a float.
                        value = float(f_val_str)
                    except ValueError:
                        # If it's not a number, keep it as a string.
                        value = f_val_str
                processed_filters.append((f_col, f_op, value))
        else:
            processed_filters = None # No filters to process.

        # =========================================================================

        print(f"Applying processed filters: {processed_filters if processed_filters else 'None'}")

        # `read_table` with the `filters` argument is highly efficient. It performs
        # "predicate pushdown," meaning it only reads the data from disk that
        # matches the filter, dramatically speeding up queries.
        table = pq.read_table(args.path, filters=processed_filters)

        print(f"\nSuccessfully read {table.num_rows} rows matching the filter.")

        if table.num_rows == 0:
            print("No data to display.")
            return

        # Convert to a Pandas DataFrame for easy manipulation and display.
        df = table.to_pandas()

        if args.columns:
            # Provide a user-friendly experience by validating requested columns.
            existing_cols = [col for col in args.columns if col in df.columns]
            missing_cols = [col for col in args.columns if col not in df.columns]
            if missing_cols:
                print(f"\nWarning: The following requested columns were not found: {missing_cols}")
            if not existing_cols:
                print("Error: None of the requested columns exist in the data. Aborting.")
                return
            # Subset the DataFrame to only the requested (and existing) columns.
            df = df[existing_cols]

        print(f"\n--- Displaying first {min(args.rows, len(df))} rows of selected data ---")
        print(df.head(args.rows))

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()