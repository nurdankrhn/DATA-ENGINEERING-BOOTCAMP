- In 00_spark_compose


- Compose up
```bash
docker-compose up -d
```

- Open Jupyterlab
```bash
docker exec -it spark jupyter lab --ip 0.0.0.0 --port 8888 --allow-root
```

- Create directory for example datasets
```bash
docker exec -it spark mkdir -p /opt/examples/datasets
```

## Spark image
`spark` service uses `veribilimiokulu/pyspark:4.1.3_python-3.12_java21`, built from
[image/Dockerfile](image/Dockerfile): `python:3.12` + OpenJDK 21 + Spark 4.1.3, plus S3A jars for
RustFS, the PostgreSQL JDBC driver and the Python packages in [image/requirements.txt](image/requirements.txt).

- Build it locally
```bash
docker compose build spark
```


## Iceberg catalog: Apache Polaris
`polaris` is an Iceberg REST catalog. Its own state is kept in `postgresql` (schema `polaris_schema`),
table files are written to the `warehouse` bucket in RustFS. Three one-time services prepare it on
`docker compose up` and then exit: `polaris-db-init` (schema), `polaris-bootstrap` (root user) and
`polaris-setup` (bucket + catalog `lakehouse`). They are safe to re-run.

- Catalog URI: `http://polaris:8181/api/catalog`, warehouse `lakehouse`, credential `root:s3cr3t`
- Spark config:
```python
.config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.12.0,org.apache.iceberg:iceberg-aws-bundle:1.12.0")
.config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
.config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
.config("spark.sql.catalog.lakehouse.type", "rest")
.config("spark.sql.catalog.lakehouse.uri", "http://polaris:8181/api/catalog")
.config("spark.sql.catalog.lakehouse.warehouse", "lakehouse")
.config("spark.sql.catalog.lakehouse.credential", "root:s3cr3t")
.config("spark.sql.catalog.lakehouse.scope", "PRINCIPAL_ROLE:ALL")
.config("spark.sql.catalog.lakehouse.s3.endpoint", "http://rustfs:9000")
.config("spark.sql.catalog.lakehouse.s3.path-style-access", "true")
.config("spark.sql.catalog.lakehouse.s3.access-key-id", "dataops")
.config("spark.sql.catalog.lakehouse.s3.secret-access-key", "Ankara06")
.config("spark.sql.catalog.lakehouse.client.region", "us-east-1")
```

- What the catalog actually stores: one pointer per table to its current metadata file
```bash
docker exec -it postgresql psql -U postgres -c "select name, entity_version, internal_properties::jsonb->>'metadata-location' as metadata_location from polaris_schema.entities where type_code = 7"
```
