# PEI Data Engineering Project

### Project Overview
This project implements a comprehensive data engineering solution for processing e-commerce sales data using Databricks, PySpark, and the Medallion Architecture pattern. The solution handles multiple data formats (CSV, JSON, XLSX) and provides robust data transformations with comprehensive unit testing.
---

## 🏗️ Architecture

### Medallion Architecture (Bronze → Silver → Gold)

```
┌─────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                             │
│  JSON (Orders)  │  CSV (Products)  │  XLSX (Customer)           │
└────────────────┬────────────────────┬───────────────────────────┘
                 │                    │
                 ▼                    ▼
┌─────────────────────────────────────────────────────────────────┐
│                      BRONZE LAYER (Raw Data)                    │
│  • bronze_orders   • bronze_products   • bronze_customers       │
│  • No transformations  • Data as-is from source                 │
└────────────────┬────────────────────┬───────────────────────────┘
                 │                    │
                 ▼                    ▼
┌─────────────────────────────────────────────────────────────────┐
│                   SILVER LAYER (Cleansed Data)                  │
│  • silver_customers                                             │
│  • silver_products                                              │
│  • silver_orders                                                │
│                                                                 │
│  ✔ Standardized schemas                                         │
│  ✔ Data quality checks                                          │
│  ✔ Cleaned + validated fields                                   │
│  ✔ Duplication removed                                          │
└───────────────────────┬───────────────────────┬─────────────────┘
                        │                       │
                        ▼                       ▼
┌─────────────────────────────────────────────────────────────────┐
│                         GOLD LAYER (Curated)                    │
│                                                                 │
│ 1 gold_orders                                                   │
│     • Enriched fact table combining orders + customers +products│
│     • Profit rounded to 2 decimals                              │
│     • Cleaned + analytics-ready                                 │
│     • Includes processing timestamp                             │
│                                                                 │
│ 2 gold_profit                                                   │
│     • Yearly aggregated profit                                  │
│     • Grouped by: Year, Customer, Category, Sub-Category        │
│     • Metrics: SUM(profit), COUNT(order_id)                     │
│     • All profit values rounded to 2 decimals                   │
│                                                                 │
│ 3  SQL Aggregates (Materialized Tables)                         │
│     • profit_by_year                                            │
│     • profit_by_year_category                                   │
│     • profit_by_customer                                        │
│     • profit_by_customer_year                                   │
│                                                                 │
│  ✔ Business-ready data                                          │
│  ✔ Optimized for dashboards & reporting                         │
│  ✔ Delivered as Delta tables (ACID, versioned)                  │
└─────────────────────────────────────────────────────────────────┘

##  Project Structure

```

```
pei-data-engineering-project/
│
├── config/
│   └── config.yaml                    # Configuration file
│
├── src/                                # Source code modules
│   ├── bronze/
│   │   ├── __init__.py
│   │   └── ingestion.py               # Bronze layer ingestion logic
│   ├── silver/
│   │   ├── __init__.py
│   │   └── transformation.py          # Silver layer transformation logic
│   ├── gold/
│   │   ├── __init__.py
│   │   └── aggregation.py             # Gold layer aggregation logic
│   └── utils/
│       ├── __init__.py
│       └── string_cleaners.py         # Helper functions
│
├── tests/                             # Unit tests
│   ├── __init__.py
│   ├── conftest.py                    # Pytest configuration
│   ├── test_ingestion.py              # Bronze layer tests
│   ├── test_transformation.py         # Silver layer tests
│   └── test_aggregation.py            # Gold layer tests
│
├── main()                             # End-to-end pipeline runner (Bronze → Silver → Gold)
├── run_pytests                        # Notebook to execute all Pytest tests
│
├── data/
│   └── raw/                           # Sample/test data
│
├── libs/                              # Python library to read Excel file
│
├── requirements.txt                   # Python dependencies
├── setup.py                           # Package setup
├── pytest.ini                         # Pytest configuration
├── README.md                          # This file
│
└── notebooks/                         # Databricks notebooks
    ├── 01_Data_Ingestion.py           # Test Bronze ingestion
    ├── 02_Data_Transformation.py      # Test Silver transformation
    ├── 03_Data_Aggregation.py         # Test Gold aggregation
    └── 04_Analytics_Queries.py        # Test SQL analytics queries
```

```

The entire project is version-controlled using Git.

Repository Structure:
• Git folder created to store all source code, notebooks, and configs
• Code is committed and pushed regularly for version tracking
• Suitable for collaborative development and CI/CD workflows

Pipeline Overview — pei-data-engineering-pipeline
This pipeline runs the complete end-to-end data flow:
Raw Data → Bronze → Silver → Gold → pytest
```

```
## 🚀 Getting Started

### Prerequisites
- Databricks workspace
- Databricks Runtime 12.2 LTS or higher
- Python 3.9+
- Access to Google Drive data source

### Installation Steps

#### 1. Download Data from Google Drive
# Downloaded the datasets from:
# https://drive.google.com/drive/folders/1eWxfGcFwJJKAK0Nj4zZeCVx6gagPEEVc?usp=sharing

# Expected files:
# - orders.json
# - products.csv
# - customers.xlsx
```

```
#### 3. Upload Data Files

1. In Databricks, open the left sidebar.
2. Go to **Workspace** → navigate to your project directory.
3. Right-click and create a folder named: raw_data 
4. Right-click the raw_data folder → **Upload**
5. Upload the 3 source files:

   • orders.json  
   • products.csv  
   • customers.xlsx  



```
**Python Libraries**
```python
# Run in notebook
%pip install pandas openpyxl pytest pyyaml
```

#### 5. Update Configuration

Edit `config/config.yaml` with your paths:
```yaml
data_paths:
  raw_base_path: "/Workspace/Users/vikash110150@gmail.com/de_project/raw_data/raw"

tables:
  bronze:
    orders: "workspace.default.bronze_orders"
    products: "workspace.default.bronze_products"
    customers: "workspace.default.bronze_customers"

  silver:
    orders: "workspace.default.silver_orders"
    products: "workspace.default.silver_products"
    customers: "workspace.default.silver_customers"

  gold:
    orders: "workspace.default.gold_orders"
    profit: "workspace.default.gold_profit"

source_files:
  orders: "Orders.json"
  products: "Products.csv"
  customers: "Customer.xlsx"
```


## How to Run the Project
Pipeline Overview — pei-data-engineering-pipeline
The pipeline runs automatically (scheduled job)
It can also be triggered manually from the Databricks UI
Databricks Workspace → Jobs → pei-data-engineering-pipeline → Run Now
This pipeline runs the complete end-to-end data flow:

## 1. Run the Entire Pipeline (`main()`)
The `main()` notebook/script runs the full ETL pipeline:
- Bronze ingestion
- Silver cleansing
- Gold aggregation
- SQL materialization
 **Path:** `main()`
### 2. Run All Tests (`run_pytests`)
The `run_pytests` notebook automatically discovers and executes all tests located under the `tests/` folder using Pytest.

 **Path:** `run_pytests`

**Test Coverage:**
- Bronze layer ingestion tests
- Silver layer transformation tests
- Gold layer aggregation tests
- File converter tests
- Data quality validation tests

---

## 📋 Task Requirements Implementation

### ✅ Task 1: Create raw tables for each source dataset
- **Implemented in:** `ingestion.py`
- **Tables:** `bronze_orders`, `bronze_products`, `bronze_customers`
- **Format:** Delta tables with full schema inference

### ✅ Task 2: Create enriched table for customers and products
- **Implemented in:** `transformation.py`
- **Tables:** `silver_customers_enriched`, `silver_products_enriched`
- **Features:** Data cleansing, deduplication, schema standardization

### ✅ Task 3: Create enriched table with order information


### ✅ Task 4: Create aggregate table showing profit by dimensions
Implemented in: aggregation.py
Task 1: Create enriched Gold Orders table
Includes:
Orders joined with customers + products
Profit rounded to 2 decimals
Customer name, country
Product category, sub-category
Task 2: Create aggregated gold profit table
Dimensions:Year,Customer,Category,Sub-Category
Metrics:
Total Profit (rounded)
Total Orders
Task 3: Generate SQL-based analytics tables
Implemented in: aggregation.py
Tables:
profit_by_year
profit_by_year_category
profit_by_customer
profit_by_customer_year


## 🛡️ Data Quality & Error Handling

### Implemented Checks:
1. **Schema Validation:** Ensures correct data types and required columns
2. **Duplicate Detection:** Removes duplicate records
3. **Data Type Conversion:** Proper casting of numeric and date fields
4. **Business Rule Validation:** Profit calculation validation

### Error Handling:
- Try-catch blocks for file operations
- Logging of errors and warnings
- Graceful degradation for missing data

---

## 📝 Assumptions & Design Decisions

1. **Date Format:** Order dates assumed to be in ISO format (YYYY-MM-DD)
2. **Deduplication:** Based on primary keys (order_id, customer_id, product_id)
3. **Year Extraction:** From order_date field
4. **Schema Evolution:** Enabled for all Delta tables

---


## 📞 Support & Contact

For questions or issues:
- Create an issue in the project repository
- Contact: vikash110150@gmail.com
- Documentation: [Databricks Documentation](https://docs.databricks.com/)

---

## 📄 License

This project is created for educational and assessment purposes.

---

---

**Happy Data Engineering!
