from pyspark.sql import SparkSession, functions as F

accessKeyId='dataops'
secretAccessKey='Ankara06'
bucket_name = "datasets"
stream_source_text_directory = "/iot_stream_input"
stream_sink_directory = "/iot_stream_output"

# spark session
spark = (SparkSession.builder
         .appName("File Source CSV Stateless")
        .config("spark.hadoop.fs.s3a.access.key", accessKeyId)
        .config("spark.hadoop.fs.s3a.secret.key", secretAccessKey)
        .config("spark.hadoop.fs.s3a.path.style.access", True)
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
         .getOrCreate())

# set log level
spark.sparkContext.setLogLevel('ERROR')
"""
1594512094.3859746,b8:27:eb:bf:9d:51,51.0,False,0.0076508222705571,False,0.0204112701224129,22.7,2025-11-21 07:22:06.536708
1594512094.7355676,00:0f:00:70:91:0a,76.0,False,0.005114383400977,False,0.0132748367048515,19.700000762939453,2025-11-21 07:22:07.037370
1594512098.0735729,b8:27:eb:bf:9d:51,50.9,False,0.007673227406398,False,0.0204751255761782,22.6,2025-11-21 07:22:07.538115
1594512099.589146,1c:bf:ce:15:ec:4d,76.80000305175781,True,0.0070233371458773,False,0.0186282253770188,27.0,2025-11-21 07:22:08.038878
1594512101.761235,b8:27:eb:bf:9d:51,50.9,False,0.0076635772823724,False,0.0204476208102336,22.6,2025-11-21 07:22:08.539637
"""
iot_schema = """ts         DOUBLE,
device     STRING,
humidity   DOUBLE,
light      BOOLEAN,
lpg        DOUBLE,
motion     BOOLEAN,
smoke      DOUBLE,
temp       DOUBLE,
event_time TIMESTAMP"""

# read data from csv schema
lines = (spark
         .readStream.format("csv")
         .schema(iot_schema)
         .option("header", True)
         .option("maxFilesPerTrigger", 1)
         .load(f"s3a://{bucket_name}{stream_source_text_directory}"))

"""
+--------------------+-----------------+-----------------+-----+------------------+------+------------------+------------------+--------------------------+
|ts                  |device           |humidity         |light|lpg               |motion|smoke             |temp              |event_time                |
+--------------------+-----------------+-----------------+-----+------------------+------+------------------+------------------+--------------------------+
|1.5945120943859746E9|b8:27:eb:bf:9d:51|51.0             |false|0.0076508222705571|false |0.0204112701224129|22.7              |2025-11-21 07:43:47.501831|
|1.5945120947355676E9|00:0f:00:70:91:0a|76.0             |false|0.005114383400977 |false |0.0132748367048515|19.700000762939453|2025-11-21 07:43:48.002609|
|1.5945120980735729E9|b8:27:eb:bf:9d:51|50.9             |false|0.007673227406398 |false |0.0204751255761782|22.6              |2025-11-21 07:43:48.503267|
|1.594512099589146E9 |1c:bf:ce:15:ec:4d|76.80000305175781|true |0.0070233371458773|false |0.0186282253770188|27.0              |2025-11-21 07:43:49.003988|
|1.594512101761235E9 |b8:27:eb:bf:9d:51|50.9             |false|0.0076635772823724|false |0.0204476208102336|22.6              |2025-11-21 07:43:49.504575|
|1.5945121044684107E9|1c:bf:ce:15:ec:4d|77.9000015258789 |true |0.0070094585431387|false |0.0185889075400507|27.0              |2025-11-21 07:43:50.005235|
|1.5945121054488635E9|b8:27:eb:bf:9d:51|50.9             |false|0.0076732416602977|false |0.0204751662043622|22.6              |2025-11-21 07:43:50.505886|
|1.594512106869076E9 |00:0f:00:70:91:0a|76.0             |false|0.0052414818417311|false |0.0136275211320191|19.700000762939453|2025-11-21 07:43:51.006534|
|1.5945121082753816E9|1c:bf:ce:15:ec:4d|77.9000015258789 |true |0.0069568023772355|false |0.0184397819021168|27.0              |2025-11-21 07:43:51.507127|
|1.5945121091366868E9|b8:27:eb:bf:9d:51|50.9             |false|0.0076668047981169|false |0.0204568196070641|22.6              |2025-11-21 07:43:52.007763|
+--------------------+-----------------+-----------------+-----+------------------+------+------------------+------------------+--------------------------+
"""



ten_min_avg = lines.groupBy(
        F.window(F.col("event_time"), "10 minutes"), F.col("device")
        ).agg( F.count("*"), F.mean("temp"), F.mean("humidity"))

checkpointDir = f"s3a://{bucket_name}/checkpoint/iotCountCheckpoint"

streamingQuery = (ten_min_avg
                  .writeStream
                  .format("console")
                  .outputMode("update")
                  .trigger(processingTime="10 second")
                  .option("numRows", 10)
                  .option("truncate", False)
                  .option("checkpointLocation", checkpointDir)
                  .start())

streamingQuery.awaitTermination()