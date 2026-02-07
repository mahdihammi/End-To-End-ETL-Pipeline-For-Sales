CREATE OR REPLACE TABLE gold.fact_sales AS
SELECT
    o.row_id,                 -- grain anchor
    o.order_id,               -- degenerate dimension

    od.date_key AS order_date_key,
    sd.date_key AS ship_date_key,

    c.customer_key,
    p.product_key,
    l.location_key,
    s.ship_mode_key,

    -- atomic measures ONLY
    o.sales,
    o.quantity,
    o.discount,
    o.profit
FROM silver.orders_silver o

JOIN gold.dim_customer c
    ON o.customer_id = c.customer_id

JOIN gold.dim_product p
    ON o.product_id = p.product_id

JOIN gold.dim_location l
    ON o.country = l.country
   AND o.region  = l.region
   AND o.state   = l.state
   AND o.city    = l.city
   AND o.postal_code = l.postal_code

JOIN gold.dim_ship_mode s
    ON o.ship_mode = s.ship_mode

JOIN gold.dim_date od
    ON o.order_date = od.date

JOIN gold.dim_date sd
    ON o.ship_date = sd.date;
