from src.gold.aggregation import GoldAggregation

def test_gold_orders(spark):
    gold = GoldAggregation(spark)

    config = {
        "tables": {
            "silver": {
                "orders": "workspace.default.test_silver_orders",
                "customers": "workspace.default.test_silver_customers",
                "products": "workspace.default.test_silver_products"
            },
            "gold": {
                "orders": "workspace.default.test_gold_orders",
                "profit": "workspace.default.test_gold_profit"
            }
        }
    }

    df = gold.gold_orders(config)

    assert df.count() > 0
    assert "profit" in df.columns
    assert "processing_timestamp" in df.columns


def test_gold_profit(spark):
    gold = GoldAggregation(spark)

    config = {
        "tables": {
            "gold": {
                "orders": "workspace.default.test_gold_orders",
                "profit": "workspace.default.test_gold_profit"
            }
        }
    }

    df = gold.gold_profit(config)

    assert df.count() > 0
    assert "total_profit" in df.columns
    assert "processing_timestamp" in df.columns
