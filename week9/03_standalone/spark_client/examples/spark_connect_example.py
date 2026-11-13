"""
Spark Connect client example.

The driver is NOT in this process: it runs inside the spark-connect container.
This script only sends a logical plan over gRPC and receives Arrow batches back.

From the host (thin client, no JVM needed):
    pip install "pyspark-client==4.1.4"
    python spark_connect_example.py                      # sc://localhost:15002

From the spark-client container:
    docker exec spark-client python3 /opt/examples/spark_connect_example.py sc://spark-connect:15002

Before running: create the `datasets` bucket on RustFS and upload iris.csv (README step 3).
S3 credentials and endpoint are configured on the SERVER (see spark-connect in docker-compose.yaml),
not here - a Connect client has no SparkContext / hadoopConfiguration to set.
"""
import sys
from pyspark.sql import SparkSession, functions as F

remote = sys.argv[1] if len(sys.argv) > 1 else "sc://spark-connect:15002"

spark = SparkSession.builder.remote(remote).appName("Spark Connect Example").getOrCreate()
print("connected to", remote, "- server Spark version:", spark.version)

df = (spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("s3a://datasets/iris.csv"))

df.printSchema()
df.show(5)

(df.groupBy("Species")
   .agg(F.count("*").alias("n"),
        F.round(F.avg("SepalLengthCm"), 2).alias("avg_sepal_length"))
   .orderBy("Species")
   .show())

# RDD API is not available over Spark Connect:
try:
    spark.sparkContext
except Exception as e:
    print("sparkContext is not available over Spark Connect:", type(e).__name__)

spark.stop()
