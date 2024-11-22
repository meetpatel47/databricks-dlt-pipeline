-- Databricks notebook source
-- DBTITLE 1,Bronze Layer
CREATE OR REFRESH STREAMING LIVE TABLE sales_raw
COMMENT "The raw sales data,ingested from sales-raw"
AS SELECT * FROM cloud_files("${datasets.path}/sales-raw", "csv")

-- COMMAND ----------

-- DBTITLE 1,Bronze Layer
CREATE OR REFRESH LIVE TABLE customers
COMMENT "The customers lookup table, ingested from customers-csv"
AS SELECT * FROM csv.`${datasets.path}/customers-csv`

-- COMMAND ----------

-- DBTITLE 1,Silver Layer
CREATE OR REFRESH STREAMING LIVE TABLE sales_cleaned (
  CONSTRAINT valid_sales_number EXPECT (sales_id IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT "The cleaned sales orders with valid sales_id"
AS
  SELECT c.sale_id, c.quantity, c.customer_id,c.first_name as f_name, c.last_name as l_name,s.products,
         c.country as country
  FROM STREAM(LIVE.sales_raw) s
  LEFT JOIN LIVE.customers c
    ON s.customer_id = c.customer_id

-- COMMAND ----------

-- DBTITLE 1,Gold Layer

CREATE OR REFRESH LIVE TABLE sales_products
COMMENT "Average products per customer in Singapore"
AS
  SELECT customer_id,f_name,l_name,avg(products) avg_products
  FROM LIVE.sales_cleaned
  WHERE country = "singapore"
  GROUP BY customer_id

-- COMMAND ----------

-- DBTITLE 1,Gold Layer
CREATE OR REFRESH LIVE TABLE sales_product
COMMENT "Number of products per customer country wise"
AS
  SELECT customer_id, f_name, l_name,dense_rank()over(partition by country order by products desc) number_of_products
  FROM LIVE.sales_cleaned
  GROUP BY customer_id
