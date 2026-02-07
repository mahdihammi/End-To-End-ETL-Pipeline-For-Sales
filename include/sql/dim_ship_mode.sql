CREATE OR REPLACE TABLE gold.dim_ship_mode AS
SELECT
    row_number() OVER () AS ship_mode_key,
    ship_mode,
    ship_mode_priority
FROM (
    SELECT DISTINCT
        ship_mode,
        ship_mode_priority
    FROM silver.orders_silver
);
