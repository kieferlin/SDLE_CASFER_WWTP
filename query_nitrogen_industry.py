#!/usr/bin/env python3

# ==============================================================================
# Script: query_nitrogen_industry.py
#
# Description:
#   An Apache Spark script to analyze and aggregate total nitrogen discharge
#   loads by industrial sector for a given year. This helps identify which
#   industries contribute the most to nitrogen pollution.
#
#   The script performs the following steps:
#   1. Reads the partitioned DMR (Discharge Monitoring Report) and the NPDES-NAICS
#      (industrial classification) linkage datasets.
#   2. Filters the DMR data for the target year and caches it for efficient reuse.
#   3. Isolates 'Total Nitrogen' concentration and 'Flow' measurements.
#   4. Joins these measurements by facility and date to calculate daily load (kg/day).
#   5. Joins the calculated daily loads with the NAICS data to associate each
#      discharge report with an industrial sector description.
#   6. Aggregates all daily loads by industrial sector to calculate the total
#      annual load, average load, and the number of measurements per sector.
#   7. Prints a summary report to the console and saves the full results to a CSV.
#
# Usage:
#   python3 query_nitrogen_industry.py <dmr_path> <naics_path> --year YYYY --output_path <file.csv>
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
# (mg/L * MGD) * 3.78541 = kg/day
MG_L_MGD_TO_KG_DAY = 3.78541

# --- CORE ANALYSIS FUNCTION ---

def analyze_industry_nitrogen_load(dmr_path: str, naics_path: str, year: str, output_path: str):
    """
    Analyzes which industrial sectors are the largest contributors to the total
    annual nitrogen load, calculated by joining concentration and flow data.

    Args:
        dmr_path (str): Path to the root of the DMR Parquet dataset.
        naics_path (str): Path to the root of the NPDES NAICS linkage Parquet dataset.
        year (str): The target year for analysis.
        output_path (str): Path to save the output CSV report.
    """
    spark = None
    try:
        logging.info("Initializing SparkSession...")
        # NOTE: While hardcoding memory settings works, the best practice is to
        # configure memory via the submission script (e.g., using --conf in
        # spark-submit or SLURM parameters). This makes the script more flexible.
        spark = SparkSession.builder \
            .appName(f"IndustryNitrogenLoad-{year}") \
            .config("spark.driver.memory", "100g") \
            .config("spark.executor.memory", "100g") \
            .getOrCreate()

        logging.info(f"Reading DMR data from: file://{dmr_path}")
        dmr_df = spark.read.parquet(f"file://{dmr_path}")
        
        logging.info(f"Reading NAICS data from: file://{naics_path}")
        naics_df = spark.read.parquet(f"file://{naics_path}")

        # --- 1. Prepare clean NAICS data for the final join ---
        # Select only the needed columns and get distinct rows to avoid ambiguity
        # when joining later. A facility can have multiple NAICS codes listed.
        naics_info_df = naics_df.select("NPDES_ID", "NAICS_DESC").distinct()

        # --- 2. Filter DMR data for the target year first ---
        # This is a key optimization: reduce the size of the main DataFrame as
        # early as possible before performing more complex operations.
        dmr_year_df = dmr_df.filter(F.col("year") == year)
        # Cache the year-filtered DataFrame in memory because we will use it twice
        # (once for nitrogen, once for flow). This avoids re-reading from disk.
        dmr_year_df.cache()
        
        # --- 3. Isolate SPECIFIC Nitrogen concentration and Flow data ---
        logging.info("Filtering for 'Nitrogen, total [as N]' and Flow parameters...")
        nitrogen_df = dmr_year_df.filter(F.col("PARAMETER_DESC") == 'Nitrogen, total [as N]') \
            .select("EXTERNAL_PERMIT_NMBR", "MONITORING_PERIOD_END_DATE",
                    F.col("DMR_VALUE_STANDARD_UNITS").cast(DoubleType()).alias("nitrogen_concentration_mg_L"))

        flow_df = dmr_year_df.filter(F.col("PARAMETER_DESC") == 'Flow, in conduit or thru treatment plant') \
            .select("EXTERNAL_PERMIT_NMBR", "MONITORING_PERIOD_END_DATE",
                    F.col("DMR_VALUE_STANDARD_UNITS").cast(DoubleType()).alias("flow_MGD"))

        # --- 4. Join measurements and calculate daily load per report ---
        logging.info("Joining Nitrogen and Flow data to calculate daily load...")
        # The inner join is critical to ensure we only use reports where both
        # concentration and flow were measured on the same day.
        daily_load_by_report_df = nitrogen_df.join(
            flow_df,
            on=["EXTERNAL_PERMIT_NMBR", "MONITORING_PERIOD_END_DATE"],
            how="inner"
        ).withColumn(
            "nitrogen_load_kg_day",
            F.col("nitrogen_concentration_mg_L") * F.col("flow_MGD") * F.lit(MG_L_MGD_TO_KG_DAY)
        )

        # --- 5. Join calculated load with industry information ---
        # This join enriches our load data with the industrial sector for each facility.
        logging.info("Joining load data with NAICS industry codes...")
        industry_load_df = daily_load_by_report_df.join(
            naics_info_df,
            daily_load_by_report_df["EXTERNAL_PERMIT_NMBR"] == naics_info_df["NPDES_ID"]
        )

        # --- 6. Aggregate by industry to find total load and other stats ---
        logging.info("Aggregating total load by industrial sector...")
        # This is the final aggregation. We group by the industry description and
        # calculate the sum of all daily loads to get the total for the year.
        final_df = industry_load_df.groupBy("NAICS_DESC").agg(
            F.sum("nitrogen_load_kg_day").alias("total_annual_load_kg"),
            F.avg("nitrogen_load_kg_day").alias("avg_daily_load_per_report_kg"),
            F.count("*").alias("num_paired_measurements")
        ).select(
            "NAICS_DESC",
            F.round("total_annual_load_kg", 0).alias("total_annual_load_kg"),
            F.round("avg_daily_load_per_report_kg", 0).alias("avg_daily_load_per_report_kg"),
            "num_paired_measurements"
        ).orderBy(F.col("total_annual_load_kg").desc())

        # --- 7. Display and Save Results ---
        if final_df.rdd.isEmpty():
            print("\n" + "="*80)
            print(f"No paired 'Nitrogen, total [as N]' and 'Flow' data found for any industry in year {year}.")
            print("="*80 + "\n")
            return

        print("\n" + "="*80)
        print(f"Industrial Sector Contribution to Total Nitrogen Load (kg) in {year}")
        print("="*80)
        final_df.show(50, truncate=False)
        print("="*80 + "\n")

        logging.info(f"Writing report to {output_path}...")
        # Final result into a single CSV file for easy use.
        final_df.repartition(1).write.csv(
            f"file://{output_path}", header=True, mode="overwrite"
        )
        logging.info(f"Analysis complete. Output file saved to {output_path}")

    except Exception as e:
        logging.error("An error occurred during the Spark job.", exc_info=True)
    finally:
        if spark:
            logging.info("Stopping SparkSession.")
            # Clear the cache before stopping the session.
            dmr_year_df.unpersist()
            spark.stop()

def main():
    parser = argparse.ArgumentParser(description="Analyze total nitrogen load contribution by industrial sector.")
    parser.add_argument("dmr_path", help="Root directory of the DMR Parquet dataset.")
    parser.add_argument("naics_path", help="Root directory of the NPDES NAICS Parquet dataset.")
    parser.add_argument("--year", required=True, help="Target year for analysis.")
    parser.add_argument("--output_path", required=True, help="Path to save the output CSV report.")
    args = parser.parse_args()
    
    analyze_industry_nitrogen_load(args.dmr_path, args.naics_path, args.year, args.output_path)

if __name__ == "__main__":
    main()