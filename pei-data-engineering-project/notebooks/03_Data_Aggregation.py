# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer - Data Aggregation
# MAGIC ## Task 3: Create aggregate table with rounded upto 2 decimal place

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------


def read_delta(source, by_table=True):
    if by_table:
        return spark.table(source)
    else:
        return spark.read.format("delta").load(source)

# COMMAND ----------

# Read Silver tables
orders_silver = read_delta("workspace.default.silver_orders", by_table=True)
customers_silver = read_delta("workspace.default.silver_customers", by_table=True)
products_silver = read_delta("workspace.default.silver_products", by_table=True)

# Build GOLD enriched table
gold_enriched = (
    orders_silver
        # Join with Customers using Customer_ID
        .join(customers_silver.select("Customer_ID", "Customer_Name", "Country"),
              on="Customer_ID", how="left")

        # Join with Products using Product_ID
        .join(products_silver.select("Product_ID", "Category", "Sub_Category"),
              on="Product_ID", how="left")

        # Round profit to 2 decimals
        .withColumn("Profit", F.round(F.col("Profit"), 2))

        # Add gold processing timestamp
        .withColumn("processing_timestamp", F.current_timestamp())
)
# Drop Bronze ingestion metadata (NOT needed in Gold)
if "source_file" in gold_enriched.columns:
    gold_enriched = gold_enriched.drop("source_file")

# Save to GOLD layer
gold_enriched.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("workspace.default.gold_orders")

display(gold_enriched.limit(10))


# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 4: Create profit aggregates by Year, Category, SubCategory, Customer

# COMMAND ----------

from pyspark.sql import functions as F

# Read Gold table
gold_orders = read_delta("workspace.default.gold_orders", by_table=True)

# Correct date parsing using SQL expression
gold_orders = gold_orders.withColumn(
    "order_year",
    F.year(F.expr("try_to_date(Order_Date, 'd/M/yyyy')"))
)

# Build aggregate table
profit_agg = (
    gold_orders
        .groupBy(
            "order_year",
            "Category",
            "Sub_Category",
            "Customer_Name"
        )
        .agg(
            F.round(F.sum("Profit"), 2).alias("total_profit"),
            F.count("Order_ID").alias("total_orders")
        )
        .withColumn("processing_timestamp", F.current_timestamp())
)

# Save to Gold
profit_agg.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("workspace.default.gold_profit")



