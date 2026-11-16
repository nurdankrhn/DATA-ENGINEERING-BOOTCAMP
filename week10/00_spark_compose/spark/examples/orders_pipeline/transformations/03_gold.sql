-- Gold: what the business reads. SQL and Python datasets live in the same pipeline;
-- the dependency graph (orders_live -> orders_current -> orders_cdc_raw) is resolved by name.

CREATE MATERIALIZED VIEW orders_by_status
COMMENT 'Live orders per status'
AS
SELECT status,
       COUNT(*)              AS orders,
       SUM(amount)           AS revenue,
       MAX(updated_at)       AS last_change
FROM   orders_live
GROUP  BY status;

CREATE MATERIALIZED VIEW customer_revenue
COMMENT 'Revenue per customer, live orders only'
AS
SELECT customer_id,
       COUNT(*)    AS orders,
       SUM(amount) AS revenue
FROM   orders_live
GROUP  BY customer_id;
