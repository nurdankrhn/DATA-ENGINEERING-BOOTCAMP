# Week 8 Homework: Idempotent CDC Ingestion with Iceberg MERGE and Polaris

**Notebook:** [Week 8 homework](week8_iceberg_idempotent_merge.ipynb)

## Overview

This homework implements a **repeatable, order-independent batch CDC ingestion pipeline** with Apache Spark, Apache Iceberg, Apache Polaris, and RustFS. Daily order change files may contain duplicates, multiple updates to one order, late-arriving changes, and delete events. The pipeline preserves the **latest known state per `order_id`**, regardless of the order in which the supplied files are ingested or how often they are replayed.

**Idempotency:** Processing the same input again must not change the resulting table contents.

## Technology Stack

| Component | Purpose |
|---|---|
| Apache Spark 4.1.x (PySpark) | Batch ingestion, transformations, Window functions, and SQL `MERGE INTO` |
| Apache Iceberg | Transactional tables, snapshots, and time travel |
| Apache Polaris | REST catalog for `lakehouse` tables and metadata references |
| RustFS | S3-compatible object storage for Iceberg metadata and data files |
| PostgreSQL | Backing store for Polaris catalog state |
| Docker Compose / JupyterLab | Local development and execution environment |

The supplied Docker image reports Spark **4.1.3** in this environment. Connector versions and catalog settings are defined in `03_pyspark/docker/00_spark_compose/README.md`.

## Data and CDC Rules

Input files are located under `data/`:

- `batch_2026-01-01.csv`
- `batch_2026-01-02.csv`
- `batch_2026-01-03.csv`

CSV columns: `order_id` (INT), `customer_id` (INT), `status` (STRING), `amount` (DECIMAL(10,2)), `updated_at` (TIMESTAMP), and `op` (`I`, `U`, or `D`).

The target Iceberg table is `lakehouse.homework.orders` with columns:

```sql
order_id INT,
customer_id INT,
status STRING,
amount DECIMAL(10,2),
updated_at TIMESTAMP,
is_deleted BOOLEAN
```

CDC handling rules:

1. Read each batch using an **explicit schema**.
2. Deduplicate within the batch using `row_number()` over `PARTITION BY order_id ORDER BY updated_at DESC`.
3. `MERGE` on `order_id`. Update a matched order **only when** `source.updated_at > target.updated_at`.
4. Insert a previously unseen `order_id`.
5. Convert `op = 'D'` into **soft deletion** (`is_deleted = true`), without physically deleting the row.
6. Ignore late or repeated changes whose timestamps are older than or equal to the stored timestamp.

The supplied files contain identical duplicate events at equal timestamps. For a general production CDC feed with conflicting events at the same timestamp, an additional deterministic sequence or event identifier would be required.

## Architecture

```text
Daily CDC CSV batches
        |
        v
Spark: explicit schema + window deduplication
        |
        v
SQL MERGE INTO (timestamp guard + soft delete)
        |
        v
Apache Iceberg: lakehouse.homework.orders
        |
        +---- Apache Polaris REST catalog (table metadata pointers)
        |
        +---- RustFS / S3 (Iceberg metadata + Parquet data files)
```

## Environment Setup

From the Docker Compose directory:

```bash
cd 03_pyspark/docker/00_spark_compose
docker compose up -d

docker exec spark mkdir -p /opt/examples/datasets/orders_cdc
# Adjust the source path if the homework directory is elsewhere.
docker cp ../../week8/homework/iceberg_idempotent_merge/data/. spark:/opt/examples/datasets/orders_cdc/

docker exec -it spark jupyter lab --ip 0.0.0.0 --port 8888 --allow-root
```

Open JupyterLab using the URL/token displayed in the terminal (through the published port, if enabled). Configure the SparkSession using the **`lakehouse` catalog configuration from the Compose README**. Avoid committing Polaris or RustFS credentials to the notebook or repository.

## Implementation

### 1. Create namespace and Iceberg table

```sql
CREATE NAMESPACE IF NOT EXISTS lakehouse.homework;

CREATE TABLE IF NOT EXISTS lakehouse.homework.orders (
    order_id INT,
    customer_id INT,
    status STRING,
    amount DECIMAL(10,2),
    updated_at TIMESTAMP,
    is_deleted BOOLEAN
) USING iceberg;
```

For a fresh, top-to-bottom test run, start with **empty homework test tables**. `CREATE TABLE IF NOT EXISTS` alone does not clear data from a previous run. Do not reset unrelated tables.

### 2. Implement `ingest_batch(batch_date, table)`

For each input file, create a deduplicated temporary view and use a guarded MERGE:

```sql
MERGE INTO lakehouse.homework.orders AS target
USING cdc_updates AS source
ON target.order_id = source.order_id
WHEN MATCHED AND source.updated_at > target.updated_at THEN
  UPDATE SET
    customer_id = source.customer_id,
    status = source.status,
    amount = source.amount,
    updated_at = source.updated_at,
    is_deleted = (source.op = 'D')
WHEN NOT MATCHED THEN
  INSERT (order_id, customer_id, status, amount, updated_at, is_deleted)
  VALUES (
    source.order_id, source.customer_id, source.status,
    source.amount, source.updated_at, (source.op = 'D')
  );
```

The notebook's function parameterizes the destination table, allowing the same ingestion logic to be tested against `orders` and `orders_replay`. Its temporary view must contain exactly one selected event per `order_id` for normal ingestion.

### 3. Validate chronological ingestion

Ingest `2026-01-01`, `2026-01-02`, and `2026-01-03` in order. The expected final table is:

| order_id | customer_id | status | amount | updated_at | is_deleted |
|---:|---:|---|---:|---|---|
| 1001 | 1 | SHIPPED | 100.00 | 2026-01-02 15:00:00 | false |
| 1002 | 2 | SHIPPED | 250.50 | 2026-01-03 09:00:00 | false |
| 1003 | 3 | CANCELLED | 75.00 | 2026-01-02 11:00:00 | true |
| 1004 | 4 | PAID | 320.00 | 2026-01-03 09:30:00 | false |
| 1005 | 5 | NEW | 40.00 | 2026-01-01 09:30:00 | false |
| 1006 | 6 | NEW | 60.00 | 2026-01-02 11:30:00 | false |
| 1007 | 7 | NEW | 90.00 | 2026-01-03 10:00:00 | false |

Required assertions: **7 total rows**, **6 active rows**, **1 soft-deleted row**, and exact expected field values.

### 4. Prove idempotency

**a. Re-ingestion:** Collect sorted `orders` rows, ingest `batch_2026-01-02.csv` again, and assert the before/after row lists are equal.

**b. Out-of-order replay:** Create a fresh `lakehouse.homework.orders_replay` table and ingest files in this order:

```text
2026-01-03 -> 2026-01-01 -> 2026-01-02 -> 2026-01-03
```

Assert that both directions of the multiset difference are empty:

```python
assert orders_df.exceptAll(replay_df).count() == 0
assert replay_df.exceptAll(orders_df).count() == 0
```

### 5. Verify Iceberg time travel

Capture the `snapshot_id` immediately after ingesting the **first** CSV file, then read the table at that snapshot:

```python
old_df = (
    spark.read.format("iceberg")
    .option("versionAsOf", str(first_snapshot_id))
    .load("lakehouse.homework.orders")
)
assert old_df.count() == 5
```

The historical snapshot must contain the five original `NEW` orders (`1001`–`1005`) with their `2026-01-01` timestamps and `is_deleted = false`. Assert the **full values**, not only the row count. Iceberg uses snapshot IDs rather than Delta Lake-style sequential version numbers.

### 6. Demonstrate failure cases

**a. Naive MERGE (`orders_naive`):** Deduplicate each file, but remove the timestamp condition from `WHEN MATCHED`. Load the three batches chronologically. Assert the intentionally incorrect outcomes:

| order_id | Incorrect status | Incorrect updated_at | is_deleted |
|---:|---|---|---|
| 1001 | PAID | 2026-01-02 12:00:00 | false |
| 1003 | NEW | 2026-01-01 09:10:00 | false |

This demonstrates that late source events can overwrite newer target states and resurrect deleted orders.

**b. No deduplication (`orders_nodedup`):** Load `batch_2026-01-02.csv` twice without deduplication. Assert **6 rows after the first load**. The second MERGE must raise `MERGE_CARDINALITY_VIOLATION`; catch the exception, assert that the expected error occurred, and confirm that the table remains at 6 rows.

A cardinality violation arises when a target row matches multiple source rows in the same MERGE, leaving an ambiguous update. Deduplicating the source to one event per business key avoids that failure for this dataset.

## Validation Checklist

- [ ] Polaris catalog connection succeeds.
- [ ] Namespace and Iceberg tables are created.
- [ ] Chronological ingestion produces exactly 7 rows, including 6 active rows.
- [ ] Expected order details match all six target columns.
- [ ] Re-ingesting the second batch leaves table contents unchanged.
- [ ] Out-of-order replay produces the identical multiset of rows.
- [ ] Time travel returns the five original rows using the first snapshot ID.
- [ ] Naive MERGE reproduces the two expected stale values.
- [ ] No-dedup test raises and catches `MERGE_CARDINALITY_VIOLATION`.
- [ ] The notebook runs from top to bottom on a clean homework-table state.

## Key Takeaways

- **Deduplication** prevents multiple source rows from competing to update a target row.
- **Timestamp-guarded MERGE** prevents late or equal-timestamp events from corrupting current state.
- **Soft deletes** preserve deletion timestamps and stop older events from recreating records.
- **Iceberg snapshots** enable historical queries; **Polaris** manages catalog references, while **RustFS** stores the table files.
- **Idempotency describes final table contents**, not necessarily an unchanged snapshot or commit history.

## References

- Week 8 course notebooks: `04b_write_iceberg.ipynb` and `04d_iceberg_polaris.ipynb`
- Local stack instructions: `03_pyspark/docker/00_spark_compose/README.md`
- Homework specification: *Idempotent ingestion with Iceberg MERGE and the Polaris catalog*
