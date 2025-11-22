# PEI Data Engineering Project
## E-commerce Sales Data Processing with Databricks

### Project Overview
This project implements a comprehensive data engineering solution for processing e-commerce sales data using Databricks, PySpark, and the Medallion Architecture pattern. The solution handles multiple data formats (CSV, JSON, XLSX) and provides robust data transformations with comprehensive unit testing.

---

## 🏗️ Architecture

### Medallion Architecture (Bronze → Silver → Gold)

```
┌─────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                              │
│  CSV (Orders)  │  JSON (Products)  │  XLSX (Customers)          │
└────────────────┬────────────────────┬────────────────────────────┘
                 │                    │
                 ▼                    ▼
┌─────────────────────────────────────────────────────────────────┐
│                      BRONZE LAYER (Raw Data)                     │
│  • bronze_orders   • bronze_products   • bronze_customers        │
│  • No transformations  • Data as-is from source                  │
└────────────────┬────────────────────┬────────────────────────────┘
                 │                    │
                 ▼                    ▼
┌─────────────────────────────────────────────────────────────────┐
│                   SILVER LAYER (Cleansed Data)                   │
│  • silver_customers                                              │
│  • silver_products                                               │
│  • silver_orders(with profit, customer, product info)            │
│  • Data quality checks  • Deduplication  • Schema enforcement    │
└────────────────┬────────────────────┬────────────────────────────┘
                 │                    │
                 ▼                    ▼
┌─────────────────────────────────────────────────────────────────┐
│                     GOLD LAYER (Aggregated)                      │
│  • gold_profit_aggregates (Year, Category, SubCategory, Customer)│
│  • Business-ready data  • Optimized for analytics                │
└─────────────────────────────────────────────────────────────────┘
```

---

## 📁 Project Structure

```
pei-data-engineering-project/
│
├── notebooks/                          # Databricks notebooks
│   ├── 01_Data_Ingestion.py           # Bronze layer - data ingestion
│   ├── 02_Data_Transformation.py      # Silver layer - enrichment
│   ├── 03_Data_Aggregation.py         # Gold layer - aggregations
│   └── 04_Analytics_Queries.py        # SQL analytics queries
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
│       ├── config.py                  # Configuration management
│       ├── file_converter.py          # XLSX to CSV converter
│       └── spark_utils.py             # Spark utility functions
│
├── tests/                              # Unit tests
│   ├── __init__.py
│   ├── conftest.py                    # Pytest configuration
│   ├── test_ingestion.py          # Bronze layer tests
│   ├── test_transformation.py     # Silver layer tests
│   ├── test_aggregation.py        # Gold layer tests
│   └── test_file_converter.py     # File converter tests
│
├── config/
│   └── config.yaml                    # Configuration file
│
├── data/
│   └── raw/                           # Sample/test data
│
├── requirements.txt                    # Python dependencies
├── setup.py                           # Package setup
├── pytest.ini                         # Pytest configuration
└── README.md                          # This file
```

---

## 🚀 Getting Started

### Prerequisites
- Databricks workspace
- Databricks Runtime 12.2 LTS or higher
- Python 3.9+
- Access to Google Drive data source

### Installation Steps

#### 1. Download Data from Google Drive
```bash
# Download the datasets from:
# https://drive.google.com/drive/folders/1eWxfGcFwJJKAK0Nj4zZeCVx6gagPEEVc?usp=sharing

# Expected files:
# - orders.csv
# - products.json
# - customers.xlsx
```


#### 3. Upload Data Files

**Method 1: DBFS File Upload (UI)**
```
1. Go to Databricks workspace
2. Click 'Data' in the left sidebar
3. Click 'DBFS' → 'Upload'
4. Upload your files to: /FileStore/pei-data-engineering/raw/
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
  raw_base_path: "/Workspace/Users/vikash110150@gmail.com/raw_data/raw"

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

---

## Data Pipeline Execution

### Step-by-Step Execution


#### Step 2: Bronze Layer - Data Ingestion
```python
# Run: notebooks/01_Data_Ingestion.py
# Creates: bronze_orders, bronze_products, bronze_customers
```

#### Step 3: Silver Layer - Data Transformation
```python
# Run: notebooks/02_Data_Transformation.py
# Creates: silver_customers_enriched, silver_products_enriched, silver_orders_enriched
```

#### Step 4: Gold Layer - Data Aggregation
```python
# Run: notebooks/03_Data_Aggregation.py
# Creates: gold_profit_aggregates
```

#### Step 5: Analytics Queries
```python
# Run: notebooks/04_Analytics_Queries.py
# Generates: Profit by Year, by Category, by Customer, etc.
```

---

## 🧪 Testing

### Running Unit Tests

**In Databricks Notebook:**
```python
# Install pytest
%pip install pytest

# Run all tests
!pytest /Workspace/Users/vikash110150@gmail.com/pei-data-engineering-project/tests/ -v

# Run specific test module
!pytest /Workspace/Users/vikash110150@gmail.com/pei-data-engineering-project/tests/unit/test_ingestion.py -v

# Run with coverage
!pytest /Workspace/Users/vikash110150@gmail.com/pei-data-engineering-project/tests/ --cov=src --cov-report=html
```

**Test Coverage:**
- Bronze layer ingestion tests
- Silver layer transformation tests
- Gold layer aggregation tests
- File converter tests
- Data quality validation tests

---

## 📋 Task Requirements Implementation

### ✅ Task 1: Create raw tables for each source dataset
- **Implemented in:** `notebooks/01_Data_Ingestion.py`
- **Tables:** `bronze_orders`, `bronze_products`, `bronze_customers`
- **Format:** Delta tables with full schema inference

### ✅ Task 2: Create enriched table for customers and products
- **Implemented in:** `notebooks/02_Data_Transformation.py`
- **Tables:** `silver_customers_enriched`, `silver_products_enriched`
- **Features:** Data cleansing, deduplication, schema standardization

### ✅ Task 3: Create enriched table with order information
- **Implemented in:** `notebooks/02_Data_Transformation.py`
- **Table:** `silver_orders_enriched`
- **Includes:**
  - Order information with profit (rounded to 2 decimals)
  - Customer name and country
  - Product category and sub-category

### ✅ Task 4: Create aggregate table showing profit by dimensions
- **Implemented in:** `notebooks/03_Data_Aggregation.py`
- **Table:** `gold_profit_aggregates`
- **Dimensions:** Year, Product Category, Product Sub-Category, Customer

### ✅ Task 5: SQL aggregates
- **Implemented in:** `notebooks/04_Analytics_Queries.py`
- **Queries:**
  - Profit by Year
  - Profit by Year + Product Category
  - Profit by Customer
  - Profit by Customer + Year

---

## 🛡️ Data Quality & Error Handling

### Implemented Checks:
1. **Schema Validation:** Ensures correct data types and required columns
2. **Null Handling:** Identifies and handles missing values
3. **Duplicate Detection:** Removes duplicate records
4. **Data Type Conversion:** Proper casting of numeric and date fields
5. **Business Rule Validation:** Profit calculation validation

### Error Handling:
- Try-catch blocks for file operations
- Logging of errors and warnings
- Graceful degradation for missing data
- Transaction rollback on failures

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

## 🎯 Next Steps

1. **Monitor Pipeline:** Set up job monitoring and alerting
2. **Add More Tests:** Increase test coverage to 90%+
3. **Implement CI/CD:** Automate testing and deployment
4. **Add Data Quality Checks:** More comprehensive validation
5. **Performance Tuning:** Optimize for larger datasets
6. **Documentation:** Add more inline code comments

---

**Happy Data Engineering!
