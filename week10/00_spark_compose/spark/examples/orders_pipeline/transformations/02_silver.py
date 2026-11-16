"""
Silver: one row per order, the latest change wins, deletes become a flag.

The imperative way is a MERGE with dedup and an updated_at guard. There is no MERGE here:
the materialized view is *defined* as "latest change per order"; the engine recomputes it,
so loading a file twice or out of order cannot change the result.

Upstream datasets are referenced by name inside SQL text (spark.sql). Spark 4.1.4 gotcha: a
DataFrame built with spark.read.table("orders_cdc_raw") is analysed when the pipeline is
registered, before the upstream flow has written anything in this run, and keeps that (empty)
snapshot. On a rerun the materialized view then comes out empty. SQL text is resolved when the
flow runs, so it sees the fresh upstream. The same holds for SQL files (03_gold.sql).
"""
from pyspark import pipelines as dp
from pyspark.sql import SparkSession

spark = SparkSession.active()


@dp.materialized_view(comment="Current state of every order: latest updated_at wins, op='D' -> is_deleted")
def orders_current():
    return spark.sql("""
        SELECT order_id, customer_id, status, amount, updated_at, op = 'D' AS is_deleted
        FROM (
            SELECT *, row_number() OVER (PARTITION BY order_id ORDER BY updated_at DESC) AS rn
            FROM orders_cdc_raw
        )
        WHERE rn = 1
    """)


@dp.temporary_view(comment="Only live orders; a helper for the gold layer, not stored")
def orders_live():
    return spark.sql("SELECT * FROM orders_current WHERE NOT is_deleted")
