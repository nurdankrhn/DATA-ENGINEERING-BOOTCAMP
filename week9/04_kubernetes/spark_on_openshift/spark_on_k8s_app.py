from pyspark.sql import SparkSession, functions as F
import time

spark = SparkSession.builder \
.appName("Spark on OpenShift") \
.getOrCreate()

df = spark.range(100000).withColumn("plus_10", F.col("id") + 10).withColumn("plus_20", F.col("id") + 20)

print("*******************************")

print(df.count())

df.printSchema()

df.show(100)

# 120 s instead of 30 s: time to open the Spark UI (step 8)
time.sleep(120)

print("Spark is shutting down.")

spark.stop()
