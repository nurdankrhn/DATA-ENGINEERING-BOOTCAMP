# Week 9 Homework — Spark Structured Streaming: File Source → Console Sink

## Objective
Build a Spark Structured Streaming application that continuously reads IoT temperature CSV files landing in an S3-compatible object store (RustFS), enriches each record with event-time columns, and writes the running result to the console sink.

## Environment
- **Stack**: the `00_spark_compose` environment (Spark + RustFS).
- **Dataset**: [IOT-temp.csv.zip](https://github.com/erkansirin78/datasets/raw/master/IOT-temp.csv.zip)
- **Source**: S3 file source — RustFS, bucket `datasets`, directory `iot-temp-input/`.
- **Sink**: console (`format("console")`), output mode `append`.

## Tasks

### 1. Land streaming data into RustFS
From a local terminal, use the **data-generator** to write the `IOT-temp.csv` rows into the RustFS `datasets` bucket under the `iot-temp-input/` directory. The generator should drop new files over time so the stream has something to pick up batch by batch.

### 2. Build the streaming application
Develop a Spark Structured Streaming app that:
- Reads the **file source** from `s3a://datasets/iot-temp-input/` (CSV, with header).
- Sanitises the column names that contain `/`:
  - `room_id/id` → `room_id_id`
  - `out/in` → `out_in`
- Parses `noted_date` (format `dd-MM-yyyy HH:mm`) into a real timestamp column `noted_date_ts`.
- Adds event-time columns derived from `noted_date_ts`:
  - `event_year`
  - `event_month`
  - `event_day` (day of week)
- Keeps the data-generator's ingestion timestamp column (`gentime_time`).
- Writes the stream to the **console** sink.

## Sample input
```text
id,room_id/id,noted_date,temp,out/in
__export__.temp_log_196134_bd201015,Room Admin,08-12-2018 09:30,29,In
__export__.temp_log_196131_7bca51bc,Room Admin,08-12-2018 09:30,29,In
__export__.temp_log_196127_522915e3,Room Admin,08-12-2018 09:29,41,Out
__export__.temp_log_196128_be0919cf,Room Admin,08-12-2018 09:29,41,Out
```

## Expected console output
```text
-------------------------------------------
Batch: 5
-------------------------------------------
+--------------------+----------+----------------+----+------+--------------------+-------------------+----------+-----------+---------+
|                  id|room_id_id|      noted_date|temp|out_in|        gentime_time|      noted_date_ts|event_year|event_month|event_day|
+--------------------+----------+----------------+----+------+--------------------+-------------------+----------+-----------+---------+
|__export__.temp_l...|Room Admin|08-12-2018 07:17|  29|    In|2025-02-24 18:33:...|2018-12-08 07:17:00|      2018|         12|        7|
|__export__.temp_l...|Room Admin|08-12-2018 07:17|  39|   Out|2025-02-24 18:33:...|2018-12-08 07:17:00|      2018|         12|        7|
|__export__.temp_l...|Room Admin|08-12-2018 07:15|  38|   Out|2025-02-24 18:33:...|2018-12-08 07:15:00|      2018|         12|        7|
|__export__.temp_l...|Room Admin|08-12-2018 07:08|  39|   Out|2025-02-24 18:33:...|2018-12-08 07:08:00|      2018|         12|        7|
|__export__.temp_l...|Room Admin|08-12-2018 07:06|  29|    In|2025-02-24 18:33:...|2018-12-08 07:06:00|      2018|         12|        7|
|__export__.temp_l...|Room Admin|08-12-2018 07:06|  40|   Out|2025-02-24 18:33:...|2018-12-08 07:06:00|      2018|         12|        7|
|__export__.temp_l...|Room Admin|08-12-2018 07:03|  29|    In|2025-02-24 18:33:...|2018-12-08 07:03:00|      2018|         12|        7|
|__export__.temp_l...|Room Admin|08-12-2018 06:57|  29|    In|2025-02-24 18:33:...|2018-12-08 06:57:00|      2018|         12|        7|
|__export__.temp_l...|Room Admin|08-12-2018 06:57|  39|   Out|2025-02-24 18:33:...|2018-12-08 06:57:00|      2018|         12|        7|
|__export__.temp_l...|Room Admin|08-12-2018 06:56|  39|   Out|2025-02-24 18:33:...|2018-12-08 06:56:00|      2018|         12|        7|
+--------------------+----------+----------------+----+------+--------------------+-------------------+----------+-----------+---------+
```

## Deliverables
Submit a single Markdown file `simple_stream_homework_solution.md` containing, in order:

1. The exact data-generator command(s) used to push data into the RustFS `iot-temp-input/` directory.
2. The full source of your Spark Structured Streaming application.
3. The `spark-submit` (or run) command, including the S3A / RustFS connection configs you used.
4. A screenshot or copy of at least one streamed micro-batch on the console, showing the sanitised columns and the derived `event_year`, `event_month`, `event_day`.

## Grading checklist
- [ ] Data-generator writes files into RustFS bucket `datasets`, directory `iot-temp-input/`.
- [ ] App reads via the **file source** (streaming `readStream`), not a batch `read`.
- [ ] `room_id/id` and `out/in` are renamed to `room_id_id` and `out_in`.
- [ ] `noted_date` is parsed to a timestamp (`noted_date_ts`).
- [ ] `event_year`, `event_month`, `event_day` derived from the event timestamp (not from processing time).
- [ ] Output appears on the console sink across multiple batches.