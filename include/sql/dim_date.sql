CREATE OR REPLACE TABLE gold.dim_date AS
WITH dates AS (
    SELECT DISTINCT order_date AS date FROM silver.orders_silver
    UNION
    SELECT DISTINCT ship_date AS date FROM silver.orders_silver
)
SELECT
    CAST(strftime(date, '%Y%m%d') AS INTEGER) AS date_key,
    date,
    year(date)  AS year,
    month(date) AS month,
    quarter(date) AS quarter,
    day(date)   AS day,
    strftime(date, '%w') AS day_of_week,
    strftime(date, '%A') AS day_name
FROM dates;
