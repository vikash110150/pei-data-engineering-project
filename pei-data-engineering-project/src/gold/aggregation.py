from pyspark.sql import functions as F
import logging

logger = logging.getLogger(__name__)

class GoldAggregation:

    def __init__(self, spark):
        self.spark = spark

    # -------------------------------------------------
    # GOLD ORDERS
    # -------------------------------------------------
    def gold_orders(self, config):
        silver = config['tables']['silver']
        gold_orders_table = config['tables']['gold']['orders']

        orders = self.spark.table(silver['orders'])
        customers = self.spark.table(silver['customers'])
        products = self.spark.table(silver['products'])

        df_gold = (
            orders
                .join(
                    customers.select("customer_id", "customer_name", "country"),
                    on="customer_id",
                    how="left"
                )
                .join(
                    products.select("product_id", "category", "sub_category"),
                    on="product_id",
                    how="left"
                )
                .withColumn("profit", F.round(F.col("profit"), 2))
                .withColumn("processing_timestamp", F.current_timestamp())
        )

        # Remove metadata if exists
        if "source_file" in df_gold.columns:
            df_gold = df_gold.drop("source_file")

        df_gold.write \
            .format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable(gold_orders_table)

        return df_gold

    # -------------------------------------------------
    # GOLD PROFIT
    # -------------------------------------------------
    def gold_profit(self, config):
        gold_orders_table = config['tables']['gold']['orders']
        gold_profit_table = config['tables']['gold']['profit']

        df = self.spark.table(gold_orders_table)

        # Convert order date properly
        df = df.withColumn(
            "order_year",
            F.year(F.expr("try_to_date(order_date, 'd/M/yyyy')"))
        )

        df_agg = (
            df.groupBy("order_year", "category", "sub_category", "customer_name")
              .agg(
                  F.round(F.sum("Profit"), 2).alias("total_profit"),
                  F.count("order_id").alias("total_orders")
              )
              .withColumn("processing_timestamp", F.current_timestamp())
        )

        df_agg.write \
            .format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable(gold_profit_table)

        return df_agg

    # -------------------------------------------------
    # RUN EVERYTHING
    # -------------------------------------------------
    def aggregate_all(self, config):
        df_gold_orders = self.gold_orders(config)
        df_gold_profit = self.gold_profit(config)

        return {
            "gold_orders": df_gold_orders,
            "gold_profit": df_gold_profit
        }
