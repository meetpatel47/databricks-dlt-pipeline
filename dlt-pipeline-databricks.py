# Databricks notebook source
# DBTITLE 1,Bronze Layer
# MAGIC %sql
# MAGIC CREATE OR REFRESH STREAMING LIVE TABLE sales_raw
# MAGIC COMMENT "The raw sales data,ingested from sales-raw"
# MAGIC AS SELECT * FROM cloud_files("${datasets.path}/sales-raw", "csv")

# COMMAND ----------

# DBTITLE 1,Bronze Layer
# MAGIC %sql
# MAGIC CREATE OR REFRESH LIVE TABLE customers
# MAGIC COMMENT "The customers lookup table, ingested from customers-csv"
# MAGIC AS SELECT * FROM csv.`${datasets.path}/customers-csv`

# COMMAND ----------

# DBTITLE 1,Silver Layer
# MAGIC %sql
# MAGIC CREATE OR REFRESH STREAMING LIVE TABLE sales_cleaned (
# MAGIC   CONSTRAINT valid_sales_number EXPECT (sales_id IS NOT NULL) ON VIOLATION DROP ROW
# MAGIC )
# MAGIC COMMENT "The cleaned sales orders with valid sales_id"
# MAGIC AS
# MAGIC   SELECT c.sale_id, c.quantity, c.customer_id,c.first_name as f_name, c.last_name as l_name,s.products,
# MAGIC          c.country as country
# MAGIC   FROM STREAM(LIVE.sales_raw) s
# MAGIC   LEFT JOIN LIVE.customers c
# MAGIC     ON s.customer_id = c.customer_id

# COMMAND ----------

# DBTITLE 1,Gold Layer
# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REFRESH LIVE TABLE sales_products
# MAGIC COMMENT "Average products per customer in Singapore"
# MAGIC AS
# MAGIC   SELECT customer_id,f_name,l_name,avg(products) avg_products
# MAGIC   FROM LIVE.sales_cleaned
# MAGIC   WHERE country = "singapore"
# MAGIC   GROUP BY customer_id

# COMMAND ----------

# DBTITLE 1,Gold Layer
# MAGIC %sql
# MAGIC CREATE OR REFRESH LIVE TABLE sales_product
# MAGIC COMMENT "Number of products per customer country wise"
# MAGIC AS
# MAGIC   SELECT customer_id, f_name, l_name,dense_rank()over(partition by country order by products desc) number_of_products
# MAGIC   FROM LIVE.sales_cleaned
# MAGIC   GROUP BY customer_id
# MAGIC
