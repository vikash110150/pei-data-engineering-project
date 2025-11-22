from pyspark.sql import functions as F
import logging

logger = logging.getLogger(__name__)


class GoldAggregation:

    def __init__(self, spark):
        self.spark = spark

    # -------------------------------------------------
    # 1) GOLD ORDERS (Enriched Orders Join)
    # -------------------------------------------------
    def gold_orders(self, config):
        silver = config['tables']['silver']
        target = config['tables']['gold']['orders']

        orders = self.spark.table(silver['orders'])
        customers = self.spark.table(silver['customers'])
        products = self.spark.table(silver['products'])

        df_gold = (
            orders
            .join(customers.select("customer_id", "customer_name", "country"),
                  on="customer_id", how="left")
            .join(products.select("product_id", "category", "sub_category"),
                  on="product_id", how="left")
            .withColumn("profit", F.round(F.col("profit"), 2))
            .withColumn("processing_timestamp", F.current_timestamp())
        )

        if "source_file" in df_gold.columns:
            df_gold = df_gold.drop("source_file")

        df_gold.write.format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable(target)

        return df_gold

    # -------------------------------------------------
    # 2) GOLD PROFIT (Main Aggregate)
    # -------------------------------------------------
    def gold_profit(self, config):
        gold_orders = config['tables']['gold']['orders']
        target = config['tables']['gold']['profit']

        df = self.spark.table(gold_orders)

        df = df.withColumn(
            "order_year",
            F.year(F.expr("try_to_date(order_date, 'd/M/yyyy')"))
        )

        df_agg = (
            df.groupBy("order_year", "customer_name", "category", "sub_category")
              .agg(
                  F.round(F.sum("profit"), 2).alias("total_profit"),
                  F.count("order_id").alias("total_orders")
              )
              .withColumn("processing_timestamp", F.current_timestamp())
        )

        df_agg.write.format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable(target)

        return df_agg

    # -------------------------------------------------
    # 3) SQL AGGREGATES — REQUIRED TASKS
    # -------------------------------------------------

    def sql_profit_by_year(self, table):
        query = f"""
            SELECT
                year(order_date_clean) AS year,
                ROUND(SUM(profit),2) AS total_profit
            FROM {table}
            GROUP BY year(order_date_clean)
            ORDER BY year
        """
        return self.spark.sql(query)

    def sql_profit_by_year_category(self, table):
        query = f"""
            SELECT
                year(order_date_clean) AS year,
                category,
                ROUND(SUM(profit),2) AS total_profit
            FROM {table}
            GROUP BY year(order_date_clean), category
            ORDER BY year, category
        """
        return self.spark.sql(query)

    def sql_profit_by_customer(self, table):
        query = f"""
            SELECT
                customer_id,
                customer_name,
                ROUND(SUM(profit),2) AS total_profit
            FROM {table}
            GROUP BY customer_id, customer_name
            ORDER BY total_profit DESC
        """
        return self.spark.sql(query)

    def sql_profit_by_customer_year(self, table):
        query = f"""
            SELECT
                customer_id,
                customer_name,
                year(order_date_clean) AS year,
                ROUND(SUM(profit),2) AS total_profit
            FROM {table}
            GROUP BY customer_id, customer_name, year(order_date_clean)
            ORDER BY customer_name, year
        """
        return self.spark.sql(query)

    # -------------------------------------------------
    # 4) RUN EVERYTHING (writes all aggregate tables)
    # -------------------------------------------------
    def aggregate_all(self, config):

        # Core gold outputs
        df_gold_orders = self.gold_orders(config)
        df_gold_profit = self.gold_profit(config)

        gold_orders_table = config['tables']['gold']['orders']

        # SQL output table names from config
        t_year = config['tables']['gold']['profit_by_year']
        t_year_cat = config['tables']['gold']['profit_by_year_category']
        t_cust = config['tables']['gold']['profit_by_customer']
        t_cust_year = config['tables']['gold']['profit_by_customer_year']

        # Run SQL queries
        df_year = self.sql_profit_by_year(gold_orders_table)
        df_year_cat = self.sql_profit_by_year_category(gold_orders_table)
        df_customer = self.sql_profit_by_customer(gold_orders_table)
        df_customer_year = self.sql_profit_by_customer_year(gold_orders_table)

        # Save SQL outputs
        df_year.write.format("delta").mode("overwrite").saveAsTable(t_year)
        df_year_cat.write.format("delta").mode("overwrite").saveAsTable(t_year_cat)
        df_customer.write.format("delta").mode("overwrite").saveAsTable(t_cust)
        df_customer_year.write.format("delta").mode("overwrite").saveAsTable(t_cust_year)

        return {
            "gold_orders": df_gold_orders,
            "gold_profit": df_gold_profit,
            "profit_by_year": df_year,
            "profit_by_year_category": df_year_cat,
            "profit_by_customer": df_customer,
            "profit_by_customer_year": df_customer_year
        }
