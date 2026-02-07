CREATE OR REPLACE TABLE gold.dim_product AS
SELECT
    row_number() OVER () AS product_key,
    product_id,
    product_name,
    category,
    sub_category
FROM (
    SELECT DISTINCT
        product_id,
        product_name,
        category,
        sub_category
    FROM silver.orders_silver
);
