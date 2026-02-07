CREATE OR REPLACE TABLE gold.dim_customer AS
SELECT
    row_number() OVER () AS customer_key,
    customer_id,
    customer_name,
    segment
FROM (
    SELECT DISTINCT
        customer_id,
        customer_name,
        segment
    FROM silver.orders_silver
);
