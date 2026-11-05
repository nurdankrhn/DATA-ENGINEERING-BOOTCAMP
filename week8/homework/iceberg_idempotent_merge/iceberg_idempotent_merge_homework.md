# Idempotent ingestion with Iceberg MERGE and the Polaris catalog

An order system sends you one change file (CDC) per day. Pipelines fail and get rerun, and files can arrive late or more than once. Build an ingestion that gives the **same table** no matter how many times, or in which order, the files are loaded.

**Ingestion is idempotent when running it again does not change the result.**

## Environment
Use the stack in `03_pyspark/docker/00_spark_compose`: Spark 4.1, Polaris and RustFS. Its README has the Spark config for the `lakehouse` catalog.

```bash
cd 03_pyspark/docker/00_spark_compose
docker compose up -d
docker exec spark mkdir -p /opt/examples/datasets/orders_cdc
docker cp ../../spark_homework/week8/iceberg_idempotent_merge/data/. spark:/opt/examples/datasets/orders_cdc/
docker exec -it spark jupyter lab --ip 0.0.0.0 --port 8888 --allow-root
```

## Data
Three files are in [data/](data/):

| column | type | meaning |
|---|---|---|
| `order_id` | int | primary key |
| `customer_id` | int | customer |
| `status` | string | `NEW`, `PAID`, `SHIPPED`, `CANCELLED` |
| `amount` | decimal(10,2) | order amount |
| `updated_at` | timestamp | when this change happened in the source system |
| `op` | string | `I` insert, `U` update, `D` delete |

The files are deliberately messy. Read them before you write any code:

- The same change can appear twice in one file.
- One order can change several times in one file.
- A late event can arrive after a newer change was already loaded.
- An old event can arrive for an order that was already deleted.

## Tasks

### 1. Create the table
Connect Spark to Polaris. Create the namespace `lakehouse.homework` and the Iceberg table `lakehouse.homework.orders` with these columns:

`order_id INT, customer_id INT, status STRING, amount DECIMAL(10,2), updated_at TIMESTAMP, is_deleted BOOLEAN`

### 2. Write `ingest_batch(batch_date, table)`
The function reads one file and loads it with **`MERGE INTO`**. It must:

- read the file with an explicit schema
- keep only the latest change per `order_id` in the file
- never let an older or equal `updated_at` overwrite the table
- **not delete rows**: `op = 'D'` sets `is_deleted = true`

### 3. Load the three files in date order
`orders` must be exactly this. It must also have 7 rows, 6 of them with `is_deleted = false`:

```
+--------+-----------+---------+------+-------------------+----------+
|order_id|customer_id|status   |amount|updated_at         |is_deleted|
+--------+-----------+---------+------+-------------------+----------+
|1001    |1          |SHIPPED  |100.00|2026-01-02 15:00:00|false     |
|1002    |2          |SHIPPED  |250.50|2026-01-03 09:00:00|false     |
|1003    |3          |CANCELLED|75.00 |2026-01-02 11:00:00|true      |
|1004    |4          |PAID     |320.00|2026-01-03 09:30:00|false     |
|1005    |5          |NEW      |40.00 |2026-01-01 09:30:00|false     |
|1006    |6          |NEW      |60.00 |2026-01-02 11:30:00|false     |
|1007    |7          |NEW      |90.00 |2026-01-03 10:00:00|false     |
+--------+-----------+---------+------+-------------------+----------+
```

### 4. Prove that it is idempotent
Both checks must pass as `assert` statements:

- **a.** Collect the rows of `orders`, load `batch_2026-01-02.csv` again and collect them again. The two sorted lists must be equal.
- **b.** Create `orders_replay` and load the files in this order: `01-03, 01-01, 01-02, 01-03`. Both of these must be 0:
  - `orders.exceptAll(orders_replay).count()`
  - `orders_replay.exceptAll(orders).count()`

### 5. Time travel
Read `orders` as it was right after the first file was loaded. Use `versionAsOf` with that snapshot's ID. The result must be:

```
+--------+-----------+------+------+-------------------+----------+
|order_id|customer_id|status|amount|updated_at         |is_deleted|
+--------+-----------+------+------+-------------------+----------+
|1001    |1          |NEW   |100.00|2026-01-01 09:00:00|false     |
|1002    |2          |NEW   |250.50|2026-01-01 09:05:00|false     |
|1003    |3          |NEW   |75.00 |2026-01-01 09:10:00|false     |
|1004    |4          |NEW   |300.00|2026-01-01 09:20:00|false     |
|1005    |5          |NEW   |40.00 |2026-01-01 09:30:00|false     |
+--------+-----------+------+------+-------------------+----------+
```

### 6. Break it on purpose
- **a.** Create `orders_naive` and load the three files in date order, with deduplication but no `updated_at` condition in the MERGE. These two rows must be the result:

```
+--------+-----------+------+------+-------------------+----------+
|order_id|customer_id|status|amount|updated_at         |is_deleted|
+--------+-----------+------+------+-------------------+----------+
|1001    |1          |PAID  |100.00|2026-01-02 12:00:00|false     |
|1003    |3          |NEW   |75.00 |2026-01-01 09:10:00|false     |
+--------+-----------+------+------+-------------------+----------+
```

- **b.** Create `orders_nodedup` and load `batch_2026-01-02.csv` twice without deduplication:
  - After the first load the table must have 6 rows.
  - The second load must fail with `MERGE_CARDINALITY_VIOLATION`. Catch the error and print it.

## Deliver
One notebook that runs from top to bottom without errors, with an `assert` for the row counts and values in tasks 3 to 6.
