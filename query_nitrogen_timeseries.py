#!/usr/bin/env python3

# ==============================================================================
# Script: query_nitrogen_timeseries.py
#
# Description:
#   An Apache Spark script to calculate a time series of the total annual
#   nitrogen load for a user-specified group of states. This is used to
#   analyze regional pollution trends over many years.
#
#   The script performs the following steps:
#   1. Reads the complete, partitioned DMR (Discharge Monitoring Report) dataset.
#   2. Filters the data to include only the states specified by the user. This
#      is a highly efficient operation due to the dataset's partitioning.
#   3. Caches the state-filtered data to speed up subsequent operations.
#   4. Isolates 'Total Nitrogen' concentration and 'Flow' measurements from the
#      filtered data.
#   5. Joins these measurements by facility and date to calculate daily load (kg/day).
#   6. Aggregates all daily loads by year and state to produce the final time series.
#   7. Prints a summary report to the console and saves the results to a CSV file.
#
# Usage:
#   python3 query_nitrogen_timeseries.py <dmr_path> --states <ST1> <ST2> ... --output_path <file.csv>
#
# Dependencies:
#   - Python 3
#   - pyspark
# ==============================================================================

import argparse
import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

# --- SCRIPT SETUP ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Define a constant for converting units for clarity and maintainability.
MG_L_MGD_TO_KG_DAY = 3.78541

# --- CORE ANALYSIS FUNCTION ---

def analyze_regional_load(dmr_path: str, states: list, output_path: str):
    """
    Analyzes the time series of total annual nitrogen load for a given list of states.
    """
    spark = None
    dmr_filtered_by_state = None # Define here for access in finally block
    try:
        logging.info("Initializing SparkSession...")
        # NOTE: While setting memory here works, the best practice is to configure
        # memory via the submission script. This makes the Python code more
        # generic and reusable for different job sizes.
        spark = SparkSession.builder \
            .appName(f"RegionalLoad-Nitrogen-{'_'.join(states)}") \
            .config("spark.driver.memory", "100g") \
            .config("spark.executor.memory", "100g") \
            .getOrCreate()

        logging.info(f"Reading DMR data from: file://{dmr_path}")
        dmr_df = spark.read.parquet(f"file://{dmr_path}")

        logging.info(f"Analyzing Total Nitrogen load for states: {', '.join(states)}")
        
        # This initial filter is very fast because the data is partitioned by state.
        # Spark will only read the directories for the states in the list.
        dmr_filtered_by_state = dmr_df.filter(F.col("state").isin(states))
        
        # --- OPTIMIZATION: Cache the filtered dataframe ---
        # We will access this state-filtered data twice (once for nitrogen, once for flow).
        # Caching it in memory avoids re-reading and re-filtering from the source.
        dmr_filtered_by_state.cache()
        # Trigger an action to materialize the cache and get a count.
        logging.info(f"Cached state-filtered data. Count: {dmr_filtered_by_state.count()}")

        # --- Filter for specific parameters from the cached data ---
        nitrogen_df = dmr_filtered_by_state.filter(
            (F.col("PARAMETER_DESC") == 'Nitrogen, total [as N]') &
            (F.col("DMR_VALUE_STANDARD_UNITS").isNotNull())
        ).select(
            "EXTERNAL_PERMIT_NMBR", "state", "year", "MONITORING_PERIOD_END_DATE",
            F.col("DMR_VALUE_STANDARD_UNITS").cast(DoubleType()).alias("nitrogen_concentration_mg_L")
        )

        flow_df = dmr_filtered_by_state.filter(
            (F.col("PARAMETER_DESC") == 'Flow, in conduit or thru treatment plant') &
            (F.col("DMR_VALUE_STANDARD_UNITS").isNotNull())
        ).select(
            "EXTERNAL_PERMIT_NMBR", "MONITORING_PERIOD_END_DATE",
            F.col("DMR_VALUE_STANDARD_UNITS").cast(DoubleType()).alias("flow_MGD")
        )
        
        # --- Join and Calculate Load ---
        # This is the memory-intensive step where paired measurements are found.
        logging.info("Joining Nitrogen and Flow data on matching dates...")
        daily_load_df = nitrogen_df.join(
            flow_df,
            on=["EXTERNAL_PERMIT_NMBR", "MONITORING_PERIOD_END_DATE"],
            how="inner"
        ).withColumn(
            "nitrogen_load_kg_day",
            F.col("nitrogen_concentration_mg_L") * F.col("flow_MGD") * F.lit(MG_L_MGD_TO_KG_DAY)
        )

        # --- Aggregate by Year and State for the Final Time Series ---
        annual_state_load_df = daily_load_df.groupBy("year", "state").agg(
            F.sum("nitrogen_load_kg_day").alias("total_annual_load_kg"),
            F.count("*").alias("num_paired_measurements")
        )

        # Select and format the final columns for the report.
        final_df = annual_state_load_df.select(
            "year", "state",
            F.round("total_annual_load_kg", 0).alias("total_annual_load_kg"),
            "num_paired_measurements"
        ).orderBy("state", "year")

        # --- Display and Save Results ---
        if final_df.rdd.isEmpty():
            print("\n" + "="*80)
            print(f"No paired 'Nitrogen, total [as N]' and 'Flow' data found for states {', '.join(states)}.")
            print("="*80 + "\n")
            return

        print("\n" + "="*80)
        print(f"Time Series for Total Annual Nitrogen Load (kg)")
        print("="*80)
        # WARNING: .collect() brings all results to the driver node's memory.
        # This is fine for aggregated results, but can cause crashes with very large outputs.
        results = final_df.collect()
        for row in results:
            print(row)
        print("="*80 + "\n")

        logging.info(f"Writing load data to {output_path}...")
        final_df.repartition(1).write.csv(
            f"file://{output_path}", header=True, mode="overwrite"
        )
        logging.info(f"Analysis complete. Output file saved to {output_path}")

    except Exception as e:
        logging.error("An error occurred during the Spark job.", exc_info=True)
    finally:
        if spark:
            logging.info("Stopping SparkSession.")
            # It's good practice to explicitly unpersist cached data when done.
            if dmr_filtered_by_state:
                dmr_filtered_by_state.unpersist()
            spark.stop()

def main():
    parser = argparse.ArgumentParser(description="Analyze time series trends of total pollutant load.")
    parser.add_argument("dmr_path", help="Root directory of the DMR Parquet dataset.")
    # 'nargs=+' allows the user to provide one or more state codes.
    parser.add_argument("--states", required=True, nargs='+', help="One or more state codes (e.g., CA TX FL).")
    parser.add_argument("--output_path", required=True, help="Path to save the output CSV file.")
    args = parser.parse_args()
    
    analyze_regional_load(args.dmr_path, args.states, args.output_path)

if __name__ == "__main__":
    main()