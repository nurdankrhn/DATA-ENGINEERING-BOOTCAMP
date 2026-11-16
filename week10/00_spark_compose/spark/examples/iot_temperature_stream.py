from getpass import getpass

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType
)

# RustFS credentials
RUSTFS_ACCESS_KEY = "dataops"

RUSTFS_SECRET_KEY = "Ankara06"

# Spark session with S3A / RustFS configuration
spark = (
    SparkSession.builder
    .appName("IoTTemperatureStreaming")
    .config("spark.hadoop.fs.s3a.endpoint", "http://rustfs:9000")
    .config("spark.hadoop.fs.s3a.endpoint.region", "us-east-1")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
    )
    .config("spark.hadoop.fs.s3a.access.key", RUSTFS_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key", RUSTFS_SECRET_KEY)
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# S3 paths
SOURCE_PATH = "s3a://datasets/iot-temp-input/"
CHECKPOINT_PATH = "s3a://datasets/iot-temp-checkpoints/console"

# Explicit schema
schema = StructType([
    StructField("id", StringType(), True),
    StructField("room_id/id", StringType(), True),
    StructField("noted_date", StringType(), True),
    StructField("temp", IntegerType(), True),
    StructField("out/in", StringType(), True),
    StructField("event_time", StringType(), True)
])

# 1. Read streaming CSV files
df = (
    spark.readStream
    .format("csv")
    .option("header", "true")
    .option("maxFilesPerTrigger", 1)
    .schema(schema)
    .load(SOURCE_PATH)
)

# 2. Transform data
df_transformed = (
    df
    .withColumnRenamed("room_id/id", "room_id_id")
    .withColumnRenamed("out/in", "out_in")
    .withColumnRenamed("event_time", "gentime_time")
    .withColumn(
        "noted_date_ts",
        F.to_timestamp(F.col("noted_date"), "dd-MM-yyyy HH:mm")
    )
    .withColumn("event_year", F.year("noted_date_ts"))
    .withColumn("event_month", F.month("noted_date_ts"))
    .withColumn("event_day", F.dayofweek("noted_date_ts"))
)

# 3. Select output columns
df_result = df_transformed.select(
    "id",
    "room_id_id",
    "noted_date",
    "temp",
    "out_in",
    "gentime_time",
    "noted_date_ts",
    "event_year",
    "event_month",
    "event_day"
)

# 4. Write to console sink
query = (
    df_result.writeStream
    .format("console")
    .outputMode("append")
    .trigger(processingTime="5 seconds")
    .option("numRows", 10)
    .option("truncate", False)
    .option("checkpointLocation", CHECKPOINT_PATH)
    .start()
)

query.awaitTermination()