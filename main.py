"""
Main script to call out functions
"""

from mylib.calculator import (
    log_output,
    create_spark_session,
    extract_data,
    transform_data,
    load_data,
    query_data,
)

if __name__ == "__main__":
    # Main ETL process
    spark = create_spark_session("PySparkDrinks")

    # Extract
    extract_data()

    # Load
    df = load_data(spark)

    # Query
    query_data(spark, df)

    # Transform
    transform_data(df)

    # Stop the Spark session
    spark.stop()
    log_output("Spark session stopped.")
