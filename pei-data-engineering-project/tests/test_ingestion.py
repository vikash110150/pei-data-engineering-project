from src.bronze.ingestion import BronzeIngestion

RAW = "/Workspace/Users/vikash110150@gmail.com/raw_data/raw"

def test_ingest_orders(spark):
    ingestion = BronzeIngestion(spark)

    df = ingestion.ingest_orders(
        source_path=f"{RAW}/Orders.json",
        table_name="workspace.default.test_bronze_orders"
    )

    assert df.count() > 0
    assert "Order_ID" in df.columns
    assert "ingestion_timestamp" in df.columns


def test_ingest_products(spark):
    ingestion = BronzeIngestion(spark)

    df = ingestion.ingest_products(
        source_path=f"{RAW}/Products.csv",
        table_name="workspace.default.test_bronze_products"
    )

    assert df.count() > 0
    assert "Product_ID" in df.columns


def test_ingest_customers(spark):
    ingestion = BronzeIngestion(spark)

    df = ingestion.ingest_customers(
        source_path="/tmp/raw_test_data/Customer.xlsx",
        table_name="workspace.default.test_bronze_customers"
    )

    assert df.count() > 0
    assert "Customer_ID" in df.columns
