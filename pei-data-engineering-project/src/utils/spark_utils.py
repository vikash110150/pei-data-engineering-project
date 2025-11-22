"""
Spark utility functions for common operations.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType
from typing import Optional, List, Dict
import logging

logger = logging.getLogger(__name__)


class SparkUtils:
    """Utility class for Spark operations."""

    @staticmethod
    def get_or_create_spark() -> SparkSession:
        """
        Get or create Spark session.

        Returns:
            SparkSession instance
        """
        return SparkSession.builder \
            .appName("PEI Data Engineering") \
            .getOrCreate()

    @staticmethod
    def read_csv(
        spark: SparkSession,
        path: str,
        header: bool = True,
        infer_schema: bool = True,
        schema: Optional[StructType] = None
    ) -> DataFrame:
        """
        Read CSV file with error handling.

        Args:
            spark: SparkSession
            path: Path to CSV file
            header: Whether first row is header
            infer_schema: Whether to infer schema
            schema: Explicit schema (optional)

        Returns:
            DataFrame
        """
        try:
            reader = spark.read.format("csv") \
                .option("header", str(header).lower()) \
                .option("inferSchema", str(infer_schema).lower())

            if schema:
                reader = reader.schema(schema)

            df = reader.load(path)
            logger.info(f"Successfully read CSV from {path}. Rows: {df.count()}")
            return df

        except Exception as e:
            logger.error(f"Error reading CSV from {path}: {str(e)}")
            raise

    @staticmethod
    def read_json(
        spark: SparkSession,
        path: str,
        multiline: bool = True
    ) -> DataFrame:
        """
        Read JSON file with error handling.

        Args:
            spark: SparkSession
            path: Path to JSON file
            multiline: Whether JSON is multiline

        Returns:
            DataFrame
        """
        try:
            df = spark.read.format("json") \
                .option("multiLine", str(multiline).lower()) \
                .load(path)

            logger.info(f"Successfully read JSON from {path}. Rows: {df.count()}")
            return df

        except Exception as e:
            logger.error(f"Error reading JSON from {path}: {str(e)}")
            raise

    @staticmethod
    def read_excel(
        spark: SparkSession,
        path: str,
        sheet_name: str = "Sheet1",
        header: bool = True
    ) -> DataFrame:
        """
        Read Excel file using spark-excel library.

        Args:
            spark: SparkSession
            path: Path to Excel file
            sheet_name: Sheet name to read
            header: Whether first row is header

        Returns:
            DataFrame

        Note:
            Requires com.crealytics:spark-excel library installed on cluster
        """
        try:
            df = spark.read.format("com.crealytics.spark.excel") \
                .option("useHeader", str(header).lower()) \
                .option("inferSchema", "true") \
                .option("dataAddress", f"'{sheet_name}'!A1") \
                .load(path)

            logger.info(f"Successfully read Excel from {path}. Rows: {df.count()}")
            return df

        except Exception as e:
            logger.error(f"Error reading Excel from {path}: {str(e)}")
            logger.info("Hint: Ensure spark-excel library is installed on cluster")
            raise

    @staticmethod
    def write_delta(
        df: DataFrame,
        path: str,
        mode: str = "overwrite",
        partition_by: Optional[List[str]] = None
    ) -> None:
        """
        Write DataFrame as Delta table.

        Args:
            df: DataFrame to write
            path: Output path
            mode: Write mode (overwrite, append, etc.)
            partition_by: Columns to partition by
        """
        try:
            writer = df.write.format("delta").mode(mode)

            if partition_by:
                writer = writer.partitionBy(*partition_by)

            writer.save(path)
            logger.info(f"Successfully wrote Delta table to {path}")

        except Exception as e:
            logger.error(f"Error writing Delta table to {path}: {str(e)}")
            raise

    @staticmethod
    def create_or_replace_temp_view(
        df: DataFrame,
        view_name: str
    ) -> None:
        """
        Create or replace temporary view.

        Args:
            df: DataFrame
            view_name: Name of the temporary view
        """
        try:
            df.createOrReplaceTempView(view_name)
            logger.info(f"Created temp view: {view_name}")
        except Exception as e:
            logger.error(f"Error creating temp view {view_name}: {str(e)}")
            raise

    @staticmethod
    def optimize_delta_table(
        spark: SparkSession,
        table_path: str,
        z_order_cols: Optional[List[str]] = None
    ) -> None:
        """
        Optimize Delta table with optional Z-ordering.

        Args:
            spark: SparkSession
            table_path: Path to Delta table
            z_order_cols: Columns for Z-ordering
        """
        try:
            optimize_cmd = f"OPTIMIZE delta.`{table_path}`"

            if z_order_cols:
                z_cols = ", ".join(z_order_cols)
                optimize_cmd += f" ZORDER BY ({z_cols})"

            spark.sql(optimize_cmd)
            logger.info(f"Optimized table at {table_path}")

        except Exception as e:
            logger.error(f"Error optimizing table at {table_path}: {str(e)}")
            raise

    @staticmethod
    def get_data_quality_stats(df: DataFrame) -> Dict:
        """
        Get basic data quality statistics.

        Args:
            df: DataFrame to analyze

        Returns:
            Dictionary with statistics
        """
        stats = {
            'row_count': df.count(),
            'column_count': len(df.columns),
            'columns': df.columns,
            'null_counts': {}
        }

        # Calculate null counts for each column
        for col in df.columns:
            null_count = df.filter(F.col(col).isNull()).count()
            stats['null_counts'][col] = null_count

        return stats
