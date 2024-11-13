"""
library functions for PySpark ETL
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
import os
import requests

# Set up markdown logging
log_file_path = "pyspark_process.md"

# Check if the markdown log file exists and create a new one
if os.path.exists(log_file_path):
    os.remove(log_file_path)

with open(log_file_path, "w") as log_file:
    log_file.write("# ETL Process Log\n\n")


def log_output(operation, output=None):
    with open(log_file_path, "a") as file:
        file.write(f"{operation}\n\n")
        if output is not None:
            file.write("The truncated output is: \n\n")
            file.write(output)
            file.write("\n\n")


def create_spark_session(appName):
    """Create and return a Spark session."""
    spark = SparkSession.builder.appName(appName).getOrCreate()
    log_output("Spark session created")
    return spark


def extract_data(
    url="https://raw.githubusercontent.com/fivethirtyeight/data/refs/heads/master/alcohol-consumption/drinks.csv",
    file_path="data/drinks.csv",
):
    """ "Extract a url to a file path"""
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    response = requests.get(url)

    # Check if response status is valid
    if response.status_code == 200:
        # Save the content to the specified file path
        with open(file_path, "wb") as f:
            f.write(response.content)
        print(f"File successfully downloaded to {file_path}")
    else:
        print(f"Failed to retrieve the file. Status Code: {response.status_code}")

    return file_path

def transform_data(df):
    """Transform the DataFrame by adding a new column."""
    transformed_df = df.withColumn(
        "beer_percentage",
        (
            col("beer_servings")
            / (col("beer_servings") + col("wine_servings") + col("spirit_servings"))
        )
        * 100,
    )
    log_output("Data transformed: added 'beer_percentage' columns", 
               transformed_df.limit(15).toPandas().to_markdown())
    return transformed_df


def load_data(spark, data="data/drinks.csv"):
    """Load the DataFrame"""
    schema = StructType(
        [
            StructField("country", StringType(), True),
            StructField("beer_servings", IntegerType(), True),
            StructField("spirit_servings", IntegerType(), True),
            StructField("wine_servings", IntegerType(), True),
            StructField("total_litres_of_pure_alcohol", IntegerType(), True),
        ]
    )

    df = spark.read.option("header", "true").schema(schema).csv(data)

    log_output("Load data successfully", df.limit(10).toPandas().to_markdown())

    return df


def query_data(spark, df):
    df.createOrReplaceTempView("drinks")
    result = spark.sql("SELECT * FROM drinks WHERE beer_servings > 100")
    log_output("Query data", result.limit(10).toPandas().to_markdown())
    return result

