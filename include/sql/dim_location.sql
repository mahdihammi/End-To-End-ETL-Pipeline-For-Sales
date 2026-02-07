CREATE OR REPLACE TABLE gold.dim_location AS
SELECT
    row_number() OVER () AS location_key,
    country,
    region,
    state,
    city,
    postal_code
FROM (
    SELECT DISTINCT
        country,
        region,
        state,
        city,
        postal_code
    FROM silver.orders_silver
);
