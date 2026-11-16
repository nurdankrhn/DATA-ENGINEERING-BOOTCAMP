from pyspark.sql import SparkSession, functions as F

accessKeyId='dataops'
secretAccessKey='Ankara06'

spark = (SparkSession.builder.appName("Streaming Intro").master("local[2]")
         .config("spark.hadoop.fs.s3a.access.key", accessKeyId)
        .config("spark.hadoop.fs.s3a.secret.key", secretAccessKey)
        .config("spark.hadoop.fs.s3a.path.style.access", True)
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint", "http://rustfs:9000")
         .getOrCreate())

spark.sparkContext.setLogLevel("ERROR")

# source
df = spark.readStream.format("text").load("s3a://datasets/iris_stream_text_input/")


# stream processing
df_filtered = df.filter(F.split(F.col("value"), ",")[4] == "Iris-virginica")



checkpointDir = "s3a://stream-states/iris_stream_state2"
# Sink
query = (
    df_filtered.writeStream
    .format("console")
    .outputMode("append")
    .trigger(processingTime="5 second")
    .option("numRows",4)
    .option("truncate", False)
    .option("checkpointLocation", checkpointDir)
    .start()
)

query.awaitTermination()