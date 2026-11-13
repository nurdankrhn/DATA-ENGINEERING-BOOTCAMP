
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col


def main():
    spark = (
        SparkSession.builder
        .appName("Spark on K8s HW")
        .getOrCreate()
    )

    print("Starting Spark on Kubernetes homework...", flush=True)

    # Generate 1,000 rows
    df = spark.range(1000)

    # Add two derived columns
    df = (
        df.withColumn("plus_10", col("id") + 10)
          .withColumn("double_id", col("id") * 2)
    )

    # Display the first 50 rows in the driver logs
    print("Displaying the first 50 rows:", flush=True)
    df.show(50)

    # Print student information
    print("Student: NURDAN KARAHAN", flush=True)

    # Keep the Spark application alive for screenshots
    print("Waiting 30 seconds before stopping...", flush=True)
    time.sleep(30)

    spark.stop()
    print("Spark application completed successfully.", flush=True)


if __name__ == "__main__":
    main()
