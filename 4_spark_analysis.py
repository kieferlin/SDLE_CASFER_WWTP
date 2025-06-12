from pyspark.sql import SparkSession
import pyspark.sql.functions as F

def analyze_dmr_data(spark, input_path, output_path):
    """
    Reads the partitioned Parquet data and performs an example analysis.
    """
    print(f"Reading Parquet data from {input_path}")
    
    # spark automatically discovers the partitions (year, state)
    df = spark.read.parquet(input_path)
    
    print("Data schema:")
    df.printSchema()
    
    print(f"Total records found: {df.count()}")
    
    # --- EXAMPLE ANALYSIS ---
    # Find the top 10 facilities with the most measurements (DMRs) in California for 2022
    
    print("Performing analysis: Top 10 facilities by measurement count in CA for 2022")
    
    analysis_df = (df
                   .filter((F.col('year') == '2022') & (F.col('state') == 'CA'))
                   .groupBy("NPDES_ID", "FACILITY_NAME")
                   .count()
                   .orderBy(F.col("count").desc())
                   .limit(10)
                  )
                  
    print("Analysis complete. Showing results:")
    analysis_df.show(truncate=False)
    
    # Save the results to a CSV file
    # .coalesce(1) combines the output into a single file for easy viewing
    print(f"Saving analysis results to {output_path}")
    analysis_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_path)


def main():
    # The SparkSession is the entry point to any Spark functionality.
    spark = (SparkSession.builder
             .appName("DMR_Analysis")
             .getOrCreate())

    input_path = "/home/kyl29/CSE_MSE_RXF131/staging/casf/kyl29/3_dmr_aggregated_parquet"
    output_path = "/home/kyl29/CSE_MSE_RXF131/staging/casf/kyl29/4_spark_analysis_results"

    analyze_dmr_data(spark, input_path, output_path)

    spark.stop()

if __name__ == "__main__":
    main()