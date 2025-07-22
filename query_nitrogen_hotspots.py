#!/usr/bin/env python3

# ==============================================================================
# Script: query_nitrogen_hotspots.py
#
# Description:
#   An Apache Spark script to identify facilities with the highest nitrogen
#   discharge loads for a given year. This analysis is crucial for pinpointing
#   significant sources of nutrient pollution in waterways.
#
#   The script performs the following steps:
#   1. Reads the partitioned DMR (Discharge Monitoring Report) and facility
#      information Parquet datasets.
#   2. Filters the massive DMR dataset for two very specific parameters:
#      - 'Nitrogen, total [as N]' for concentration measurements.
#      - 'Flow, in conduit or thru treatment plant' for effluent flow rates.
#   3. Performs an INNER JOIN on the nitrogen and flow data. This is the most
#      critical step, as it ensures that load is only calculated for days where
#      BOTH concentration and flow were reported by the same facility.
#   4. Calculates the daily nitrogen load in kilograms (kg/day) for each
#      concurrent measurement.
#   5. Aggregates these daily loads to find the average daily load for each
#      facility over the entire year.
#   6. Joins the results with the facility dataset to add descriptive information
#      like facility name and location.
#   7. Prints a summary report to the console and saves the top results to a CSV file.
#
# Usage:
#   python3 query_nitrogen_hotspots.py <dmr_path> <facilities_path> --year YYYY --output_path <file.csv>
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

# Configure basic logging to display info-level messages during the Spark job execution.
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Define a constant for converting units. This improves readability and maintainability.
# Formula: (mg/L) * (Million Gallons/Day) * (3.78541 L/Gallon) * (1,000,000 Gallons / 1 Million Gallons) * (1 kg / 1,000,000 mg)
# This simplifies to: (mg/L * MGD) * 3.78541 = kg/day
MG_L_MGD_TO_KG_DAY = 3.78541

# --- CORE ANALYSIS FUNCTION ---

def analyze_nitrogen_load(dmr_path: str, facilities_path: str, year: str, output_path: str):
    """
    Calculates the average daily nitrogen load for facilities by joining specific
    nitrogen concentration and flow measurements from the same monitoring day.

    Args:
        dmr_path (str): Path to the root of the partitioned DMR Parquet dataset.
        facilities_path (str): Path to the root of the partitioned ICIS facilities Parquet dataset.
        year (str): The target year for analysis (e.g., "2023").
        output_path (str): The path where the output CSV report will be saved.
    """
    spark = None
    try:
        # Initialize a SparkSession, which is the entry point to any Spark functionality.
        logging.info("Initializing SparkSession...")
        spark = SparkSession.builder.appName(f"NitrogenLoadAnalysis-{year}").getOrCreate()

        # The `file://` prefix is best practice to ensure Spark looks for the path on the
        # local filesystem of the driver and executors.
        logging.info(f"Reading DMR data from: file://{dmr_path}")
        dmr_df = spark.read.parquet(f"file://{dmr_path}")
        
        logging.info(f"Reading facilities data from: file://{facilities_path}")
        facilities_df = spark.read.parquet(f"file://{facilities_path}")

        logging.info(f"Filtering for specific Nitrogen and Flow parameters in year {year}...")

        # --- STEP 1: Isolate SPECIFIC Nitrogen concentration data ---
        # Filter for the exact parameter description for 'total nitrogen'. This precision
        # is vital to avoid including other nitrogen forms (e.g., ammonia, nitrate).
        # We also cast the value column to DoubleType to prepare for mathematical operations.
        nitrogen_df = dmr_df.filter(
            (F.col("year") == year) &
            (F.col("PARAMETER_DESC") == 'Nitrogen, total [as N]') &
            (F.col("DMR_VALUE_STANDARD_UNITS").isNotNull())
        ).select(
            "EXTERNAL_PERMIT_NMBR",
            "MONITORING_PERIOD_END_DATE",
            F.col("DMR_VALUE_STANDARD_UNITS").cast(DoubleType()).alias("nitrogen_concentration_mg_L")
        )

        # --- STEP 2: Isolate SPECIFIC Flow data ---
        # Similarly, filter for the exact flow parameter. Load cannot be calculated
        # without a corresponding flow measurement (Load = Concentration × Flow).
        flow_df = dmr_df.filter(
            (F.col("year") == year) &
            (F.col("PARAMETER_DESC") == 'Flow, in conduit or thru treatment plant') &
            (F.col("DMR_VALUE_STANDARD_UNITS").isNotNull())
        ).select(
            "EXTERNAL_PERMIT_NMBR",
            "MONITORING_PERIOD_END_DATE",
            F.col("DMR_VALUE_STANDARD_UNITS").cast(DoubleType()).alias("flow_MGD")
        )

        # --- STEP 3: Join on facility AND date to get paired measurements ---
        # This 'inner' join is the core of the analysis. It creates a new DataFrame
        # containing only rows where a facility reported BOTH nitrogen and flow
        # for the exact same monitoring period, ensuring valid load calculations.
        logging.info("Joining Nitrogen and Flow data on matching dates...")
        paired_measurements_df = nitrogen_df.join(
            flow_df,
            on=["EXTERNAL_PERMIT_NMBR", "MONITORING_PERIOD_END_DATE"],
            how="inner"
        )

        # --- STEP 4: Calculate the daily load for each paired measurement ---
        # With paired data, we can now apply the conversion formula to each row.
        daily_load_df = paired_measurements_df.withColumn(
            "nitrogen_load_kg_day",
            F.col("nitrogen_concentration_mg_L") * F.col("flow_MGD") * F.lit(MG_L_MGD_TO_KG_DAY)
        )

        # --- STEP 5: Aggregate to find the average daily load per facility ---
        # Group by facility permit number to calculate the mean of all their daily load
        # measurements for the year. We also count the measurements to understand
        # the robustness of the average for each facility.
        logging.info("Aggregating to find average daily load per facility...")
        avg_facility_load_df = daily_load_df.groupBy("EXTERNAL_PERMIT_NMBR").agg(
            F.mean("nitrogen_load_kg_day").alias("avg_daily_load_kg"),
            F.count("*").alias("num_paired_measurements")
        )
        
        # --- STEP 6: Join with facility info and format for output ---
        # Join the aggregated load data with the facilities dataset to add human-readable
        # context like the facility's name and city. Then, select and format the final columns.
        final_report_df = avg_facility_load_df.join(
            facilities_df,
            avg_facility_load_df["EXTERNAL_PERMIT_NMBR"] == facilities_df["NPDES_ID"]
        ).select(
            "FACILITY_NAME", "CITY", "state",
            F.round("avg_daily_load_kg", 2).alias("average_daily_nitrogen_load_kg"),
            "num_paired_measurements"
        ).orderBy(F.col("avg_daily_load_kg").desc())

        # --- STEP 7: Display and Save Results ---
        # Check if the final DataFrame is empty. If so, no paired data was found for the year.
        if final_report_df.rdd.isEmpty():
            print("\n" + "="*80)
            print(f"No paired 'Nitrogen, total [as N]' and 'Flow' data found for year {year}.")
            print("="*80 + "\n")
            return

        # Print the top results to the console for immediate user feedback.
        print("\n" + "="*80)
        print(f"Top Facilities by Average Daily Nitrogen Load (kg/day) for {year}")
        print("="*80)
        final_report_df.show(50, truncate=False)
        print("="*80 + "\n")
        
        # Write the top 200 results to a single CSV file.
        # `.repartition(1)` ensures the output is one file, not multiple part-files.
        logging.info(f"Writing report to {output_path}...")
        final_report_df.limit(200).repartition(1).write.csv(
            f"file://{output_path}", header=True, mode="overwrite"
        )
        logging.info("Analysis complete and output file saved.")

    except Exception as e:
        logging.error("An error occurred during the Spark job.", exc_info=True)
    finally:
        # It's critical to stop the SparkSession to release cluster resources.
        if spark:
            logging.info("Stopping SparkSession.")
            spark.stop()

# --- SCRIPT ENTRYPOINT ---
def main():
    """Parses command-line arguments and calls the main analysis function."""
    parser = argparse.ArgumentParser(description="Calculate average daily nitrogen load for facilities.")
    parser.add_argument("dmr_path", help="Root directory of the DMR Parquet dataset.")
    parser.add_argument("facilities_path", help="Root directory of the ICIS facilities Parquet dataset.")
    parser.add_argument("--year", required=True, help="Target year for analysis.")
    parser.add_argument("--output_path", required=True, help="Path to save the output CSV file.")
    args = parser.parse_args()
    
    analyze_nitrogen_load(args.dmr_path, args.facilities_path, args.year, args.output_path)

if __name__ == "__main__":
    main()