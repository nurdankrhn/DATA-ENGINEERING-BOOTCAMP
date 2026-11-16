"""
Bronze: the raw CDC files, as they arrive.

Two ways to read the same folder, side by side:
- `orders_cdc_raw`    - a MATERIALIZED VIEW: recomputed from scratch on every run (batch).
- `orders_cdc_stream` - a STREAMING TABLE: only files not seen before are processed (incremental).
"""
from pyspark import pipelines as dp
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DecimalType, TimestampType

spark = SparkSession.active()

SOURCE = "file:///opt/examples/datasets/orders_cdc"

# Explicit schema: a pipeline never infers; the schema is the contract with the source.
CDC_SCHEMA = StructType([
    StructField("order_id", IntegerType()),
    StructField("customer_id", IntegerType()),
    StructField("status", StringType()),
    StructField("amount", DecimalType(10, 2)),
    StructField("updated_at", TimestampType()),
    StructField("op", StringType()),
])


@dp.materialized_view(comment="All CDC rows, batch read, recomputed on every run")
def orders_cdc_raw():
    return (spark.read.schema(CDC_SCHEMA)
            .option("header", "true")
            .csv(SOURCE)
            .withColumn("source_file", F.col("_metadata.file_name")))


@dp.table(comment="All CDC rows, read as a stream: a file is processed once, new files are appended")
def orders_cdc_stream():
    return (spark.readStream.schema(CDC_SCHEMA)
            .option("header", "true")
            .csv(SOURCE)
            .withColumn("source_file", F.col("_metadata.file_name")))

