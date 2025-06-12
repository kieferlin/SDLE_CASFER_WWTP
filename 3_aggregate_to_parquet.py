import os
import pandas as pd
import logging
import sys
from pyarrow import csv as pv_csv
from pyarrow import parquet as pq

# basic logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def aggregate_data(base_input_dir, base_output_dir):
    """
    Reads all individual NPDES CSV files and writes them into a partitioned
    Parquet dataset optimized for Spark
    """
    logging.info(f"Starting aggregation from {base_input_dir} to {base_output_dir}")

    # structure is /base_input_dir/{year}/{state}/{npdes_id}.csv
    all_csv_files = []
    for year_dir in os.listdir(base_input_dir):
        year_path = os.path.join(base_input_dir, year_dir)
        if not os.path.isdir(year_path):
            continue
        for state_dir in os.listdir(year_path):
            state_path = os.path.join(year_path, state_dir)
            if not os.path.isdir(state_path):
                continue
            
            logging.info(f"Scanning for files in {state_path}...")
            for csv_file in os.listdir(state_path):
                if csv_file.endswith('.csv'):
                    all_csv_files.append(os.path.join(state_path, csv_file))

    if not all_csv_files:
        logging.warning("No CSV files found to aggregate. Exiting.")
        return

    logging.info(f"Found {len(all_csv_files)} total CSV files to process.")

    # read all CSVs into a single Pandas DataFrame using pyarrow's CSV reader
    try:
        sample_df = pd.read_csv(all_csv_files[0], nrows=0)
        convert_options = pv_csv.ConvertOptions()
        parse_options = pv_csv.ParseOptions(delimiter=',')
        read_options = pv_csv.ReadOptions(
            skip_rows=4, # skip header lines
            autogenerate_column_names=False, # use the header from the file
            column_names=sample_df.columns
        )
    except Exception as e:
        logging.error(f"Could not determine headers from sample file. Error: {e}")
        return

    # use PyArrow to read all files into a single table for efficiency
    logging.info("Reading all CSVs into memory using PyArrow...")
    arrow_table = pv_csv.read_csv(
        all_csv_files,
        read_options=read_options,
        parse_options=parse_options,
        convert_options=convert_options
    )
    logging.info("Successfully read CSVs into an Arrow Table.")

    # add 'year' and 'state' columns for partitioning
    df = arrow_table.to_pandas()
    df['year'] = df['MONITORING_PERIOD_END_DATE'].str.split('/').str[2]
    df['state'] = df['PERM_STATE_CODE']
    
    # write to a partitioned Parquet dataset
    # create a directory structure like: /output_dir/year=2023/state=CA/part-0.parquet
    logging.info(f"Writing partitioned Parquet dataset to {base_output_dir}...")
    df.to_parquet(
        base_output_dir,
        engine='pyarrow',
        compression='snappy',
        partition_cols=['year', 'state']
    )

    logging.info("Aggregation to Parquet complete.")

def main():
    # directory where your 2_dmr_download.py saves its files
    input_dir = "/home/kyl29/CSE_MSE_RXF131/staging/casf/kyl29/2_dmr_download"
    # new directory to store the Parquet data
    output_dir = "/home/kyl29/CSE_MSE_RXF131/staging/casf/kyl29/3_dmr_aggregated_parquet"
    
    os.makedirs(output_dir, exist_ok=True)
    
    aggregate_data(input_dir, output_dir)

if __name__ == "__main__":
    main()