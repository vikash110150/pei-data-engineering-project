# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer - Data Ingestion
# MAGIC ## Task 1: Create raw tables for each source dataset
# MAGIC
# MAGIC This notebook ingests data from various sources into the Bronze layer:
# MAGIC - **Orders**: CSV format
# MAGIC - **Products**: JSON format
# MAGIC - **Customers**: excel Format
# MAGIC
# MAGIC All data is stored as Delta tables in the Bronze layer.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Import Libraries

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Configuration

# COMMAND ----------

RAW_BASE_PATH = "/Workspace/Users/vikash110150@gmail.com/raw_data/raw"

ORDERS_FILE = f"{RAW_BASE_PATH}/Orders.json"
PRODUCTS_FILE = f"{RAW_BASE_PATH}/Products.csv"
CUSTOMERS_FILE = f"{RAW_BASE_PATH}/Customer.xlsx"


# COMMAND ----------

def clean_column_name(col):
    return (col.strip()
              .replace(" ", "_")
              .replace(",", "")
              .replace(";", "")
              .replace("{", "")
              .replace("}", "")
              .replace("(", "")
              .replace(")", "")
              .replace("\n", "")
              .replace("\t", "")
              .replace("=", "")
           )


# COMMAND ----------

from pyspark.sql import functions as F

def ingest_csv_to_bronze(source_path, entity_name):
    """
    Reads CSV → Cleans columns → Adds metadata → Saves to workspace.default.bronze_<entity>.
    """

    table_name = f"workspace.default.bronze_{entity_name.lower()}"
    print(f"Loading Bronze Table: {table_name}")

    df = (spark.read.format("csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .load(source_path))

    # Clean column names
    df = df.toDF(*[clean_column_name(c) for c in df.columns])

    # Add metadata
    df_bronze = (
        df.withColumn("ingestion_timestamp", F.current_timestamp())
          .withColumn("source_file", F.lit(source_path))
    )

    # Print schema on notebook
    df_bronze.printSchema()

    # Write table
    (df_bronze.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(table_name))

    print(f"✓ Bronze table created: {table_name}")
    return df_bronze


# COMMAND ----------

df_bronze_products = ingest_csv_to_bronze(PRODUCTS_FILE, "products")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Helper Functions

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ingest Orders

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ingest Products

# COMMAND ----------

import pandas as pd
from pyspark.sql import functions as F

def ingest_excel_to_bronze(source_path, entity_name):
    """
    Reads Excel → Converts to Spark → Cleans columns → Adds metadata 
    → Saves to workspace.default.bronze_<entity>.
    """

    table_name = f"workspace.default.bronze_{entity_name.lower()}"
    print(f" Ingesting Excel file into Bronze: {table_name}")

    # --- 1. Read Excel using Pandas ---
    df_pd = pd.read_excel(source_path)

    # Convert phone column if present
    if "phone" in df_pd.columns:
        df_pd["phone"] = df_pd["phone"].astype(str)

    # --- 2. Convert Pandas → Spark ---
    df = spark.createDataFrame(df_pd)

    # --- 3. Clean column names ---
    df = df.toDF(*[clean_column_name(c) for c in df.columns])

    # --- 4. Add metadata ---
    df_bronze = (
        df.withColumn("ingestion_timestamp", F.current_timestamp())
          .withColumn("source_file", F.lit(source_path))
    )

    df_bronze.printSchema()

    # --- 5. Save as Table ---
    (df_bronze.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(table_name))

    print(f"✓ Bronze table created: {table_name}\n")

    return df_bronze


# COMMAND ----------

df_bronze_customers = ingest_excel_to_bronze(
    CUSTOMERS_FILE,
    "customers"
)


# COMMAND ----------



def ingest_json_to_bronze(source_path, entity_name):
    """
    Reads JSON → Cleans column names → Adds metadata 
    → Saves to workspace.default.bronze_<entity>.
    """

    table_name = f"workspace.default.bronze_{entity_name.lower()}"
    print(f"Ingesting JSON file into Bronze: {table_name}")

    # --- 1. Read JSON file ---
    df = (spark.read.format("json")
            .option("multiLine", "true")  # in case JSON contains objects spanning lines
            .load(source_path))

    # --- 2. Clean column names ---
    df = df.toDF(*[clean_column_name(c) for c in df.columns])

    # --- 3. Add metadata columns ---
    df_bronze = (
        df.withColumn("ingestion_timestamp", F.current_timestamp())
          .withColumn("source_file", F.lit(source_path))
    )

    df_bronze.printSchema()

    # --- 4. Save as Delta Table ---
    (df_bronze.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(table_name))

    print(f"✓ Bronze table created: {table_name}\n")

    return df_bronze


# COMMAND ----------

df_bronze_orders = ingest_json_to_bronze(
    ORDERS_FILE,
    "orders"
)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Ingest Customers
