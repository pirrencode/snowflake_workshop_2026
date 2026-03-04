-- Snowflake Classwork: End-to-end setup, load, transform, and analysis
-- This script is designed to be run in Snowflake Worksheets or SnowSQL.

/* =========================================================
   1) Setup Snowflake Environment
   ========================================================= */

-- Create and use the warehouse (optional: adjust size as needed)
CREATE WAREHOUSE IF NOT EXISTS CLASSWORK_WH
  WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE;

USE WAREHOUSE CLASSWORK_WH;

-- Create database and schemas requested
CREATE DATABASE IF NOT EXISTS SalesDW;
USE DATABASE SalesDW;

CREATE SCHEMA IF NOT EXISTS RawData;
CREATE SCHEMA IF NOT EXISTS Analytics;

/* =========================================================
   2) Data Modeling and Table Creation (RawData)
   ========================================================= */

USE SCHEMA RawData;

-- Product catalog (music products)
CREATE OR REPLACE TABLE DimProduct (
    product_id         NUMBER,
    product_name       STRING,
    product_category   STRING,   -- e.g., Album, Single, Merchandise
    artist_name        STRING,
    format_id          NUMBER,
    unit_price         NUMBER(12,2),
    active_flag        BOOLEAN,
    created_at         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Sales format dimension (CD, Vinyl, Digital Download, Streaming, etc.)
CREATE OR REPLACE TABLE DimFormat (
    format_id          NUMBER,
    format_name        STRING,
    channel_type       STRING,   -- Physical / Digital / Streaming
    created_at         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Customer dimension
CREATE OR REPLACE TABLE DimCustomer (
    customer_id        NUMBER,
    customer_name      STRING,
    email              STRING,
    country            STRING,
    state_province     STRING,
    city               STRING,
    created_at         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Raw sales fact table
CREATE OR REPLACE TABLE FactSalesRaw (
    sales_id           NUMBER,
    sales_date         DATE,
    customer_id        NUMBER,
    product_id         NUMBER,
    quantity           NUMBER,
    unit_price         NUMBER(12,2),
    discount_pct       NUMBER(5,2),
    payment_method     STRING,
    order_channel      STRING,
    loaded_at          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

/* =========================================================
   3) Data Loading
   ========================================================= */

-- Option A: Load from CSV files using internal stage + COPY INTO
-- 1) Create a file format for CSV files.
CREATE OR REPLACE FILE FORMAT ff_csv_sales
  TYPE = CSV
  SKIP_HEADER = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  EMPTY_FIELD_AS_NULL = TRUE
  NULL_IF = ('NULL', 'null', '');

-- 2) Create a named stage.
CREATE OR REPLACE STAGE stg_sales_data
  FILE_FORMAT = ff_csv_sales;

-- 3) Upload files via SnowSQL:
-- PUT file://<local_path>/dim_format.csv @stg_sales_data AUTO_COMPRESS=TRUE;
-- PUT file://<local_path>/dim_product.csv @stg_sales_data AUTO_COMPRESS=TRUE;
-- PUT file://<local_path>/dim_customer.csv @stg_sales_data AUTO_COMPRESS=TRUE;
-- PUT file://<local_path>/fact_sales_raw.csv @stg_sales_data AUTO_COMPRESS=TRUE;

-- 4) Load staged files into tables.
COPY INTO DimFormat (format_id, format_name, channel_type)
FROM (
  SELECT
    $1::NUMBER,
    $2::STRING,
    $3::STRING
  FROM @stg_sales_data/dim_format.csv
)
ON_ERROR = 'CONTINUE';

COPY INTO DimProduct (product_id, product_name, product_category, artist_name, format_id, unit_price, active_flag)
FROM (
  SELECT
    $1::NUMBER,
    $2::STRING,
    $3::STRING,
    $4::STRING,
    $5::NUMBER,
    $6::NUMBER(12,2),
    $7::BOOLEAN
  FROM @stg_sales_data/dim_product.csv
)
ON_ERROR = 'CONTINUE';

COPY INTO DimCustomer (customer_id, customer_name, email, country, state_province, city)
FROM (
  SELECT
    $1::NUMBER,
    $2::STRING,
    $3::STRING,
    $4::STRING,
    $5::STRING,
    $6::STRING
  FROM @stg_sales_data/dim_customer.csv
)
ON_ERROR = 'CONTINUE';

COPY INTO FactSalesRaw (sales_id, sales_date, customer_id, product_id, quantity, unit_price, discount_pct, payment_method, order_channel)
FROM (
  SELECT
    $1::NUMBER,
    TO_DATE($2::STRING, 'YYYY-MM-DD'),
    $3::NUMBER,
    $4::NUMBER,
    $5::NUMBER,
    $6::NUMBER(12,2),
    $7::NUMBER(5,2),
    $8::STRING,
    $9::STRING
  FROM @stg_sales_data/fact_sales_raw.csv
)
ON_ERROR = 'CONTINUE';

-- Option B (UI): Use Snowflake UI -> Data -> Databases -> SalesDW -> RawData -> Load Data.
-- Option C (Snowpipe): Create a pipe to auto-ingest from cloud storage for continuous loading.

/* =========================================================
   4) Data Transformation + Aggregated Table (Analytics)
   ========================================================= */

USE SCHEMA Analytics;

CREATE OR REPLACE TABLE SalesSummary (
    sales_date             DATE,
    year_num               NUMBER,
    month_num              NUMBER,
    customer_id            NUMBER,
    customer_name          STRING,
    country                STRING,
    product_id             NUMBER,
    product_name           STRING,
    product_category       STRING,
    artist_name            STRING,
    format_name            STRING,
    channel_type           STRING,
    total_units_sold       NUMBER,
    gross_sales_value      NUMBER(14,2),
    discount_value         NUMBER(14,2),
    net_sales_value        NUMBER(14,2),
    avg_selling_price      NUMBER(14,2),
    last_refresh_ts        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Transform and load (clean + join + aggregate)
INSERT OVERWRITE INTO SalesSummary (
    sales_date,
    year_num,
    month_num,
    customer_id,
    customer_name,
    country,
    product_id,
    product_name,
    product_category,
    artist_name,
    format_name,
    channel_type,
    total_units_sold,
    gross_sales_value,
    discount_value,
    net_sales_value,
    avg_selling_price,
    last_refresh_ts
)
WITH cleaned_sales AS (
    SELECT
      s.sales_id,
      s.sales_date,
      s.customer_id,
      s.product_id,
      COALESCE(s.quantity, 0) AS quantity,
      COALESCE(NULLIF(s.unit_price, 0), p.unit_price, 0) AS unit_price,
      COALESCE(s.discount_pct, 0) AS discount_pct
    FROM SalesDW.RawData.FactSalesRaw s
    LEFT JOIN SalesDW.RawData.DimProduct p
      ON s.product_id = p.product_id
    WHERE s.sales_date IS NOT NULL
      AND COALESCE(s.quantity, 0) >= 0
),
joined_data AS (
    SELECT
      cs.sales_date,
      YEAR(cs.sales_date) AS year_num,
      MONTH(cs.sales_date) AS month_num,
      cs.customer_id,
      c.customer_name,
      c.country,
      cs.product_id,
      p.product_name,
      p.product_category,
      p.artist_name,
      f.format_name,
      f.channel_type,
      cs.quantity,
      cs.unit_price,
      (cs.quantity * cs.unit_price) AS gross_amount,
      (cs.quantity * cs.unit_price) * (cs.discount_pct / 100) AS discount_amount,
      (cs.quantity * cs.unit_price) * (1 - (cs.discount_pct / 100)) AS net_amount
    FROM cleaned_sales cs
    LEFT JOIN SalesDW.RawData.DimCustomer c
      ON cs.customer_id = c.customer_id
    LEFT JOIN SalesDW.RawData.DimProduct p
      ON cs.product_id = p.product_id
    LEFT JOIN SalesDW.RawData.DimFormat f
      ON p.format_id = f.format_id
)
SELECT
  sales_date,
  year_num,
  month_num,
  customer_id,
  customer_name,
  country,
  product_id,
  product_name,
  product_category,
  artist_name,
  format_name,
  channel_type,
  SUM(quantity) AS total_units_sold,
  ROUND(SUM(gross_amount), 2) AS gross_sales_value,
  ROUND(SUM(discount_amount), 2) AS discount_value,
  ROUND(SUM(net_amount), 2) AS net_sales_value,
  ROUND(SUM(net_amount) / NULLIF(SUM(quantity), 0), 2) AS avg_selling_price,
  CURRENT_TIMESTAMP() AS last_refresh_ts
FROM joined_data
GROUP BY
  sales_date,
  year_num,
  month_num,
  customer_id,
  customer_name,
  country,
  product_id,
  product_name,
  product_category,
  artist_name,
  format_name,
  channel_type;

/* =========================================================
   5) Querying and Analysis
   ========================================================= */

-- Q1: Total sales revenue per product category
SELECT
  product_category,
  ROUND(SUM(net_sales_value), 2) AS total_revenue
FROM SalesDW.Analytics.SalesSummary
GROUP BY product_category
ORDER BY total_revenue DESC;

-- Q2: Top 10 customers by sales volume
SELECT
  customer_id,
  customer_name,
  ROUND(SUM(total_units_sold), 0) AS total_units,
  ROUND(SUM(net_sales_value), 2) AS total_revenue
FROM SalesDW.Analytics.SalesSummary
GROUP BY customer_id, customer_name
ORDER BY total_revenue DESC
LIMIT 10;

-- Q3: Sales trends over time (monthly)
SELECT
  TO_DATE(TO_CHAR(year_num) || '-' || LPAD(TO_CHAR(month_num), 2, '0') || '-01') AS month_start,
  ROUND(SUM(net_sales_value), 2) AS monthly_revenue,
  ROUND(SUM(total_units_sold), 0) AS monthly_units
FROM SalesDW.Analytics.SalesSummary
GROUP BY month_start
ORDER BY month_start;
