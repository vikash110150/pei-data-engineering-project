
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from typing import Dict
import logging

logger = logging.getLogger(__name__)
class BronzeIngestion:

    def __init__(self, spark):
        self.spark = spark

    def _clean_columns(self, df):
        return df.toDF(*[
            c.strip()
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
            for c in df.columns
        ])

    def ingest_orders(self, source_path, table_name):
        from pyspark.sql import functions as F
        
        df = (
            self.spark.read.format("json")
            .option("multiLine", "true")
            .load(source_path)
        )
        df = self._clean_columns(df)
        df = df.withColumn("ingestion_timestamp", F.current_timestamp()) \
               .withColumn("source_file", F.lit(source_path))

        df.write.format("delta").mode("overwrite").saveAsTable(table_name)
        return df

    def ingest_products(self, source_path, table_name):
        df = (
            self.spark.read.format("csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .load(source_path)
        )
        df = self._clean_columns(df)
        df = df.withColumn("ingestion_timestamp", F.current_timestamp()) \
               .withColumn("source_file", F.lit(source_path))

        df.write.format("delta").mode("overwrite").saveAsTable(table_name)
        return df

    def ingest_customers(self, source_path, table_name):
        import pandas as pd
        try:
            import openpyxl
        except ImportError:
            import subprocess, sys
            subprocess.check_call([sys.executable, "-m", "pip", "install", "openpyxl"])
            import openpyxl  # load after installation
        pdf = pd.read_excel(source_path)

        # FIX: convert phone to string ALWAYS
        if "phone" in pdf.columns:
            pdf["phone"] = pdf["phone"].astype(str)

        df = self.spark.createDataFrame(pdf)
        df = self._clean_columns(df)

        df = df.withColumn("ingestion_timestamp", F.current_timestamp()) \
            .withColumn("source_file", F.lit(source_path))

        df.write.format("delta").mode("overwrite").saveAsTable(table_name)
        return df



    def ingest_all(self, config):
        raw = config['data_paths']['raw_base_path']
        files = config['source_files']
        bronze = config['tables']['bronze']

        return {
            "orders": self.ingest_orders(f"{raw}/{files['orders']}", bronze['orders']),
            "products": self.ingest_products(f"{raw}/{files['products']}", bronze['products']),
            "customers": self.ingest_customers(f"{raw}/{files['customers']}", bronze['customers'])
        }
