# Spark Standalone Cluster

Stack: official `spark:4.1.4-java21-python3` image (master, worker, client, Spark Connect server) + RustFS (S3-compatible object store).

## 1. Bring up the cluster
```bash
docker compose up --scale spark-worker=2 -d
```

Verify everything is running:
```bash
docker compose ps

NAME                           SERVICE        STATUS
03_standalone-spark-worker-1   spark-worker   Up
03_standalone-spark-worker-2   spark-worker   Up
spark-master                   spark-master   Up
spark-client                   spark-client   Up
spark-connect                  spark-connect  Up
rustfs                         rustfs         Up
```

## 2. Spark Master UI
http://localhost:8080/ — you should see 2 registered workers in `ALIVE` state.

Worker UIs are exposed on host ports `8081-8089` (one per replica), driver UI at http://localhost:4041 while a job is running on the client.

## 3. RustFS Console
http://localhost:9001

Login with `dataops` / `Ankara06`. Create a `datasets` bucket and upload `iris.csv` from there.

Dataset URL: https://raw.githubusercontent.com/erkansirin78/datasets/master/iris.csv

## 4. Submit a sample job
The base Spark image is bare — no `hadoop-aws` jar bundled. Use `--packages` so Spark pulls it at submit time. The image bundles `hadoop-client-{api,runtime}-3.4.2.jar`, so **`hadoop-aws` must be `3.4.2`** (must match the bundled hadoop-client version exactly).

The image's `spark` user has `HOME=/nonexistent`, so Ivy can't write its default cache — point it at `/tmp` with `spark.jars.ivy`.

```bash
docker exec spark-client /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.driver.host=spark-client \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --packages org.apache.hadoop:hadoop-aws:3.4.2 \
  /opt/examples/simple_submit.py
```

`spark.driver.host=spark-client` is required so executors on the workers can reach the driver back over the `vbo` network.

> **Note:** `simple_submit.py` was written against MinIO. Update `fs.s3a.endpoint` to `http://rustfs:9000` (or pass `--conf spark.hadoop.fs.s3a.endpoint=http://rustfs:9000` to `spark-submit`) before it will read from the RustFS bucket.

Quick sanity check (no S3 needed, computes Pi — `spark.jars.ivy` not required since no `--packages`):
```bash
docker exec spark-client /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.driver.host=spark-client \
  /opt/spark/examples/src/main/python/pi.py 50
```

## 5. Interactive PySpark shell
```bash
docker exec -it spark-client /opt/spark/bin/pyspark \
  --master spark://spark-master:7077 \
  --conf spark.driver.host=spark-client
```

## 6. Spark Connect
Steps 4 and 5 run the **driver inside `spark-client`**: the Python process starts a JVM, the JVM
plans the query and talks to the executors. With **Spark Connect** the driver moves into a long-running
server (`spark-connect` in the compose file, started with `start-connect-server.sh`), and the client
is a thin gRPC client that only sends a logical plan and receives Arrow batches — no JVM, no Spark
install, no `spark.driver.host` on the client side. This is how notebooks, IDEs and Databricks connect to
a cluster.

```text
python / Jupyter / IDE  ──gRPC :15002──▶  spark-connect (driver)  ──▶  spark-master ──▶ workers
```

Server-side facts, visible in `docker-compose.yaml`:
- `SPARK_NO_DAEMONIZE=true` — the start script normally forks and exits; the container would stop with it.
- `--packages` and the `fs.s3a.*` settings are given **to the server**. A Connect client has no
  `sparkContext` / `hadoopConfiguration` to set, so every dependency and every Hadoop config lives on the server.
- Same `spark.driver.host` rule as in step 4: executors call the driver back, and the driver is now `spark-connect`.
- Server Spark UI: http://localhost:4042. Startup takes ~1 minute the first time (`hadoop-aws` is downloaded);
  `docker logs -f spark-connect` ends with `SparkConnectServer: Spark Connect server started`.

### 6.1 From the spark-client
`pyspark-client` is the JVM-free client package (a few MB). Use a Python version close to the server's
```bash
docker exec -it -u root spark-client bash
apt update
apt install python3.10-venv
exit


docker exec -it  spark-client bash
cd /tmp/
python3 -m venv .venv && source .venv/bin/activate
pip install "pyspark-client==4.1.4"
cd /opt/examples/
python spark_connect_example.py 
```

Or interactively:
```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.remote("sc://spark-connect:15002").getOrCreate()
spark.range(10).selectExpr("sum(id)").show()
```

### 6.2 From the `spark-client` container
The image ships PySpark but not the Connect client dependencies (`grpcio`, `pyarrow`, `pandas`). Install them once:
```bash
docker exec -u root spark-client pip install --break-system-packages "pyspark-client==4.1.4"
```
Then:
```bash
docker exec spark-client python3 /opt/examples/spark_connect_example.py sc://spark-connect:15002

docker exec -it spark-client /opt/spark/bin/pyspark --remote sc://spark-connect:15002
```
Note what is missing compared to step 5: no `--master`, no `--conf spark.driver.host`. The client does not
know or care where the cluster is.

### 6.3 What does not work over Connect
- `spark.sparkContext`, RDDs, `sc.parallelize`, `_jsc` / `_jvm` tricks — there is no JVM on the client.
  `simple_submit.py` sets S3 credentials through `_jsc.hadoopConfiguration()`; over Connect that has to be server config.
- `spark.conf.set` for static / Hadoop settings — only runtime SQL confs can be changed per session.
- Python UDFs need the same (or compatible) Python on client and server.

Spark Connect is also the transport of `spark-pipelines`, the Declarative Pipelines CLI that came with
Spark 4.1: see [../../08_declarative_pipelines](../../08_declarative_pipelines).

## 7. Shutdown
```bash
docker compose down
```

Add `-v` to also drop the `rustfs_data` volume.
