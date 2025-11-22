from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from utils.string_cleaners import StringCleaner
from typing import Dict
import logging

logger = logging.getLogger(__name__)
class SilverTransformation:

    def __init__(self, spark):
        self.spark = spark
    def _lowercase_columns(self, df):
        """Convert all column names to lowercase."""
        return df.toDF(*[c.lower() for c in df.columns])
    
    def customers(self, bronze_path, silver_table):
        df = self.spark.table(bronze_path)
        df = self._lowercase_columns(df)
        df = StringCleaner.clean_string_column(df, "customer_name")
        df = StringCleaner.clean_phone_column(df, "phone")
        df = (
            df.dropDuplicates(["customer_id"])
              .withColumn("country", F.initcap(F.trim(F.col("country"))))
              .withColumn("processing_timestamp", F.current_timestamp())
        )
        

        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(silver_table)
        return df

    def products(self, bronze_path, silver_table):
        df = self.spark.table(bronze_path)
        df = self._lowercase_columns(df)
        df = (
            df.dropDuplicates(["product_id"])
              .withColumn("product_name", F.initcap(F.trim(F.col("product_name"))))
              .withColumn("processing_timestamp", F.current_timestamp())
              .withColumnRenamed("state", "product_state")
              .withColumnRenamed("sub-category", "sub_category")
        )
        

        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(silver_table)
        return df

    def orders(self, bronze_path, silver_customers, silver_products, silver_table):
        orders = self.spark.table(bronze_path)
        customers = self.spark.table(silver_customers)
        products = self.spark.table(silver_products)

        # Lowercase bronze orders columns
        orders = self._lowercase_columns(orders)

        # 1️ Clean and enrich only order fields
        df = (
            orders.dropDuplicates(["order_id"])
                    .withColumn("order_date_clean", F.expr("try_to_date(order_date, 'd/M/yyyy')"))
                    .withColumn("year", F.year("order_date_clean"))
                    .withColumn("processing_timestamp", F.current_timestamp())
        )

        # 2️ Select only needed customer fields
        customers_sel = customers.select(
            "customer_id",
            "customer_name",
            "country"
        )

        # 3️ Select only needed product fields
        products_sel = products.select(
            "product_id",
            F.col("category").alias("product_category"),
            F.col("sub_category").alias("product_sub_category")
        )

        # Save table
        df.write.format("delta")\
            .mode("overwrite")\
            .option("overwriteSchema", "true")\
            .saveAsTable(silver_table)

        return df


    def transform_all(self, config):
        bronze = config['tables']['bronze']
        silver = config['tables']['silver']

        return {
            "customers": self.customers(bronze['customers'], silver['customers']),
            "products": self.products(bronze['products'], silver['products']),
            "orders": self.orders(bronze['orders'], silver['customers'], silver['products'], silver['orders'])
        }
