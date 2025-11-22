import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    # Use the **existing Databricks SparkSession**
    return SparkSession.builder.getOrCreate()
    
@pytest.fixture(scope="session")
def test_config():
    return {
        "bronze": {
            "orders": "/tmp/bronze_orders_test",
            "products": "/tmp/bronze_products_test",
            "customers": "/tmp/bronze_customers_test",
        },
        "silver": {
            "orders": "/tmp/silver_orders_test",
            "products": "/tmp/silver_products_test",
            "customers": "/tmp/silver_customers_test",
        },
        "gold": {
            "orders": "/tmp/gold_orders_test",
            "profit": "/tmp/gold_profit_test",
        },
        "raw": "tests/sample_data"
    }