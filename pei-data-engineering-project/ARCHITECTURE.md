# Architecture Design Document

## Overview
This document outlines the architecture for the PEI Data Engineering Project using the Medallion Architecture pattern on Databricks.

## System Architecture

### Medallion Architecture Layers

#### 1. Bronze Layer (Raw Data)
- **Purpose**: Store raw, unprocessed data exactly as received from source systems
- **Tables**:
  - `bronze_orders`: Raw orders data from CSV
  - `bronze_products`: Raw products data from JSON  
  - `bronze_customers`: Raw customers data from CSV (converted from XLSX)
- **Characteristics**:
  - No transformations applied
  - Metadata columns added (ingestion_timestamp, source_file)
  - Delta format for ACID transactions
  - Immutable once written

#### 2. Silver Layer (Cleansed & Enriched)
- **Purpose**: Provide cleansed, validated, and enriched data
- **Tables**:
  - `silver_customers_enriched`: Deduplicated customers with standardized fields
  - `silver_products_enriched`: Deduplicated products with standardized categories
  - `silver_orders_enriched`: Orders with calculated profit, joined with customer and product details
- **Transformations**:
  - Data cleansing (trim, uppercase, null handling)
  - Deduplication based on primary keys
  - Profit calculation: `sales - (cost * quantity)`
  - Year extraction from order_date
  - Joins with dimension tables
- **Partitioning**: Orders partitioned by year

#### 3. Gold Layer (Business-Ready Aggregates)
- **Purpose**: Provide aggregated, business-ready data for analytics
- **Tables**:
  - `gold_profit_aggregates`: Profit aggregated by multiple dimensions
- **Aggregation Dimensions**:
  - Year
  - Product Category
  - Product Sub-Category  
  - Customer
- **Metrics**:
  - Total profit (rounded to 2 decimals)
  - Total sales
  - Total quantity
  - Order count
  - Average profit per order
- **Partitioning**: By year for query performance

## Data Flow

```
Source Files (CSV, JSON, XLSX)
    ↓
[XLSX → CSV Conversion]
    ↓
Bronze Layer (Raw Ingestion)
    ↓
Silver Layer (Transformation & Enrichment)
    ↓
Gold Layer (Aggregation)
    ↓
Analytics & Reporting
```

## Technology Stack

- **Platform**: Databricks
- **Processing Engine**: Apache Spark (PySpark)
- **Storage Format**: Delta Lake
- **Languages**: Python, SQL
- **Testing**: pytest
- **File Formats**: CSV, JSON, XLSX

## Data Quality Framework

### Validation Rules
1. **Schema Validation**: Ensure correct data types
2. **Null Checks**: Identify and handle missing values
3. **Duplicate Detection**: Remove duplicates based on primary keys
4. **Business Rule Validation**: 
   - Profit must be calculated correctly
   - Dates must be valid
   - Quantities must be positive

### Error Handling
- Try-catch blocks for all file operations
- Logging at each layer
- Transaction rollback on failures
- Data quality statistics tracking

## Performance Optimizations

1. **Partitioning**: 
   - Orders by year
   - Aggregates by year

2. **Z-Ordering**:
   - Orders: order_id, customer_id
   - Aggregates: year, product_category

3. **Delta Lake Features**:
   - ACID transactions
   - Time travel
   - Schema enforcement & evolution
   - OPTIMIZE command for compaction

4. **Caching Strategy**:
   - Cache frequently accessed dimension tables
   - Broadcast small lookup tables

## Security & Governance

- **Access Control**: Table-level permissions via Databricks ACLs
- **Audit Logging**: Track all data modifications
- **Data Lineage**: Full traceability from source to gold
- **Encryption**: At-rest and in-transit encryption

## Deployment Strategy

1. **Development**: Local testing with pytest
2. **Staging**: Databricks dev workspace
3. **Production**: Databricks prod workspace with job scheduling

## Monitoring & Alerting

- Job execution monitoring
- Data quality metrics
- Performance metrics
- Error tracking and alerting

## Future Enhancements

1. Implement Change Data Capture (CDC)
2. Add real-time streaming ingestion
3. Implement data quality framework (Great Expectations)
4. Add ML models for forecasting
5. Implement CI/CD pipeline
