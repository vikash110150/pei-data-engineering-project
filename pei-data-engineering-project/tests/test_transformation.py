from src.silver.transformation import SilverTransformation

def test_silver_customers(spark):
    transformer = SilverTransformation(spark)

    df = transformer.customers(
        bronze_path="workspace.default.test_bronze_customers",
        silver_table="workspace.default.test_silver_customers"
    )

    assert df.count() > 0
    assert "customer_name" in df.columns
    assert "processing_timestamp" in df.columns


def test_silver_products(spark):
    transformer = SilverTransformation(spark)

    df = transformer.products(
        bronze_path="workspace.default.test_bronze_products",
        silver_table="workspace.default.test_silver_products"
    )

    assert df.count() > 0
    assert "product_name" in df.columns


def test_silver_orders(spark):
    transformer = SilverTransformation(spark)

    df = transformer.orders(
        bronze_path="workspace.default.test_bronze_orders",
        silver_customers="workspace.default.test_silver_customers",
        silver_products="workspace.default.test_silver_products",
        silver_table="workspace.default.test_silver_orders"
    )

    assert df.count() > 0
    assert "year" in df.columns
    assert "processing_timestamp" in df.columns
