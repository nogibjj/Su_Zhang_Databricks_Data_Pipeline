# Databricks notebook source
# Edited further so that this could pass CI/CD

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round, when


def extract_load(
    url="https://raw.githubusercontent.com/fivethirtyeight/data/refs/heads/master/alcohol-consumption/drinks.csv",
    filepath="data/sz324_drinks_delta",
):
    """Extract to file path"""
    spark = SparkSession.builder.appName("Extract Data").getOrCreate()

    df = pd.read_csv(url)
    print(df.head())
    pollingplaces_df = spark.createDataFrame(df)

    if spark.catalog.tableExists("sz324_drinks_delta"):
        print("Table 'sz324_drinks_delta' already exists. Skipping append.")
    else:
        # Write to Delta table if it doesn't exist
        pollingplaces_df.write.format("delta").mode("append").saveAsTable(
            "sz324_drinks_delta"
        )
        print("Dataframe saved to table")

    return filepath


extract_load()


# COMMAND ----------


def transform_data():
    """Transform data by adding beer_percentage column and save to a new Delta table"""
    # Initialize Spark session
    spark = SparkSession.builder.appName("Transform Data").getOrCreate()

    # Define the source Delta table
    source_table_name = "ids706_data_engineering.default.sz324_drinks_delta"

    # Define the target Delta table
    target_table_name = "ids706_data_engineering.default.sz324_drinks_delta_transformed"

    # Read the source Delta table
    df = spark.table(source_table_name)

    # Add the beer_percentage column
    transformed_df = df.withColumn(
        "beer_percentage",
        round(
            when(
                (col("beer_servings") + col("wine_servings") + col("spirit_servings"))
                > 0,
                col("beer_servings")
                / (
                    col("beer_servings") + col("wine_servings") + col("spirit_servings")
                ),
            ).otherwise(None),
            3,
        ),
    )

    # Write the transformed data to a new Delta table
    transformed_df.write.format("delta").mode("overwrite").saveAsTable(
        target_table_name
    )
    print(
        f"Transformed data with 'beer_percentage' column "
        f"saved to table '{target_table_name}'"
    )


# Transform the data
transform_data()


# COMMAND ----------

LOG_FILE = "query_log.md"


def log_query(query, result="none"):
    """adds to a query markdown file"""
    with open(LOG_FILE, "a") as file:
        file.write(f"```SQL\n{query}\n```\n\n")
        file.write(f"```{result}\n```\n\n")


def query_data(query):
    spark = SparkSession.builder.appName("Query Data").getOrCreate()
    delta_table_name = "ids706_data_engineering.default.sz324_drinks_delta_transformed"

    log_query(query, result="Query received, executing query.")
    print(f"Executing SQL query on table {delta_table_name}")
    # Execute the query on the table
    result_df = spark.sql(query)
    pandas_df = result_df.toPandas()
    result_str = pandas_df.to_markdown(index=False)
    log_query(query, result=result_str)

    return result_df


query_df = query_data(
    query="""
    SELECT 
        country,
        beer_servings,
        wine_servings,
        spirit_servings,
        beer_percentage,
        CASE 
            WHEN beer_percentage > 0.5 THEN 'High Beer Consumption'
            ELSE 'Moderate/Low Beer Consumption'
        END AS beer_consumption_category
    FROM ids706_data_engineering.default.sz324_drinks_delta_transformed
    WHERE beer_percentage IS NOT NULL
    ORDER BY beer_percentage DESC
    LIMIT 20;
    """
)

query_df.head(20)
