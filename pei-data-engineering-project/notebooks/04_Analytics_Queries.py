# Databricks notebook source
# MAGIC %md
# MAGIC # Analytics Queries
# MAGIC ## Task 5: SQL Aggregates

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 5a: Profit by Year

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     YEAR(try_to_date(Order_Date, 'd/M/yyyy')) AS order_year,
# MAGIC     ROUND(SUM(Profit), 2) AS total_profit
# MAGIC FROM workspace.default.gold_orders
# MAGIC GROUP BY order_year
# MAGIC ORDER BY order_year;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 5b: Profit by Year + Product Category

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     YEAR(try_to_date(Order_Date, 'd/M/yyyy')) AS order_year,
# MAGIC     Category,
# MAGIC     ROUND(SUM(Profit), 2) AS total_profit
# MAGIC FROM workspace.default.gold_orders
# MAGIC GROUP BY order_year, Category
# MAGIC ORDER BY order_year, Category;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 5c: Profit by Customer

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     Customer_Name,
# MAGIC     ROUND(SUM(Profit), 2) AS total_profit
# MAGIC FROM workspace.default.gold_orders
# MAGIC GROUP BY Customer_Name
# MAGIC ORDER BY total_profit DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 5d: Profit by Customer + Year

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     Customer_Name,
# MAGIC     YEAR(try_to_date(Order_Date, 'd/M/yyyy')) AS order_year,
# MAGIC     ROUND(SUM(Profit), 2) AS total_profit
# MAGIC FROM workspace.default.gold_orders
# MAGIC GROUP BY Customer_Name, order_year
# MAGIC ORDER BY Customer_Name, order_year;

# COMMAND ----------

# MAGIC %md
# MAGIC ✅ Task 5 Completed - All SQL aggregates generated

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     o.Customer_ID, o.Customer_Name,
# MAGIC     COUNT(*) AS missing_orders
# MAGIC FROM workspace.default.gold_orders o
# MAGIC WHERE o.Customer_Name IS NULL
# MAGIC GROUP BY o.Customer_ID, o.Customer_Name
# MAGIC ORDER BY missing_orders DESC;
# MAGIC
