# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer - Data Transformation
# MAGIC ## Tasks 2 & 3: Create enriched tables

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# Configuration
BRONZE_BASE_PATH = "/Workspace/Users/vikash110150@gmail.com/pei-data-engineering/bronze"
SILVER_BASE_PATH = "/Workspace/Users/vikash110150@gmail.com/pei-data-engineering/silver"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 2: Create enriched customers and products

# COMMAND ----------


def read_delta(source, by_table=True):
    if by_table:
        return spark.table(source)
    else:
        return spark.read.format("delta").load(source)



# COMMAND ----------



def clean_string_column(df, column_name):
    """
    Cleans a string column by:
    - Removing special characters
    - Collapsing multiple spaces
    - Trimming whitespace
    - Converting to Title Case
    """
    
    return (
        df.withColumn(
            column_name,
            F.initcap(                                                  # Title Case
                F.trim(
                    F.regexp_replace(                                   # collapse spaces
                        F.regexp_replace(F.col(column_name), 
                                         r"[^A-Za-z0-9 ]", " "),        # remove special chars
                        r"\s+", " "                                     # collapse multiple spaces
                    )
                )
            )
        )
    )


# COMMAND ----------

from pyspark.sql import functions as F

def clean_phone_column(df, column_name):
    """
    Safely clean phone numbers:
    - Extract digits
    - Extract extension
    - Validate length
    - Return NULL for invalid values
    - Keep original formatting if valid
    """
    
    col = F.col(column_name)

    # 1. NULL out obvious errors
    df = df.withColumn(
        column_name,
        F.when(col.rlike("#ERROR!"), None)
         .when(F.trim(col) == "", None)
         .otherwise(col)
    )

    # 2. Extract digits only (for validation)
    digits = F.regexp_replace(col, r"[^0-9]", "")

    # 3. Extract extension
    ext = F.regexp_extract(col, r"[xX](\d+)", 1)

    # 4. Validate: if digits < 7 → NULL
    # WHY: real phone numbers are usually 10–12 digits, but some countries have 7-digit local numbers
    valid_number = F.when(F.length(digits) >= 7, col).otherwise(None)

    # 5. If valid and has extension → keep original + normalize extension
    final_val = F.when(
        valid_number.isNotNull() & (ext != ""),
        F.concat(valid_number, F.lit(" x"), ext)
    ).otherwise(valid_number)

    return df.withColumn(column_name, final_val)


# COMMAND ----------

customers = read_delta("workspace.default.bronze_customers", by_table=True)

customers_enriched = (
    customers
        .dropDuplicates(["customer_id"])
)

# Clean customer_name and country with one reusable line each
customers_enriched = clean_string_column(customers_enriched, "customer_name")
customers_enriched = clean_string_column(customers_enriched, "country")
customers_enriched = clean_phone_column(customers_enriched, "phone")

# Add processing timestamp
customers_enriched = customers_enriched.withColumn(
    "processing_timestamp", F.current_timestamp()
)

# Save to Silver
customers_enriched.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("workspace.default.silver_customers")




# COMMAND ----------

# Read Bronze Products
products = read_delta("workspace.default.bronze_products", by_table=True)

products_enriched = (
    products
        .dropDuplicates(["product_id"])
        .withColumnRenamed("Sub-Category", "Sub_Category")
)

# Clean category and sub_category — using reusable cleaning function
products_enriched = clean_string_column(products_enriched, "Product_Name")

# Add processing timestamp
products_enriched = products_enriched.withColumn(
    "processing_timestamp", F.current_timestamp()
)

# Save to Silver Layer
products_enriched.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("workspace.default.silver_products")



# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 3: Create enriched orders with profit and joins

# COMMAND ----------

# Read Bronze Orders
orders = read_delta("workspace.default.bronze_orders", by_table=True)

orders_enriched = (
    orders
        .dropDuplicates(["order_id"])
)

# Add processing timestamp
orders_enriched = orders_enriched.withColumn(
    "processing_timestamp", F.current_timestamp()
)

# Save to Silver Layer
orders_enriched.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("workspace.default.silver_orders")



