# Week 9 Homework — Run a Spark Application on Kubernetes

## Objective
Submit a PySpark application to a local Kubernetes cluster (Docker Desktop) and verify that the driver and executor pods run successfully.

## Environment
- **Cluster**: Kubernetes built into **Docker Desktop**.
- **Spark image**: `spark:3.5.3-java17`
- **Submission method**: choose **one** of the two:
  1. `spark-submit` directly against the Kubernetes API server, **or**
  2. The **Spark Operator** (Helm chart) with a `SparkApplication` custom resource.

The lesson notes for both methods are next to this file:
- [01_spark-submit.md](01_spark-submit.md)
- [02_operator_helm.md](02_operator_helm.md)

## Tasks

### 1. Cluster prep
- Start the Kubernetes cluster on Docker Desktop.
- Confirm `kubectl get nodes` returns a `Ready` node.
- Create the namespace and service account / RBAC required by your chosen submission method (see lesson notes).

### 2. PySpark application
Write a single file `spark_on_k8s_hw.py` that:
- Creates a SparkSession with `appName="Spark on K8s HW"`.
- Builds a DataFrame from `spark.range(...)` of at least 1,000 rows, with **two derived columns** of your choice (e.g. `id + 10`, `id * 2`, etc.).
- Calls `df.show(50)` so 50 rows are printed in the driver log.
- Prints **your full name and surname** to stdout (e.g. `print("ERKAN SIRIN")`).
- Sleeps 30 seconds before stopping the session (so executor pods stay alive long enough for you to take a screenshot).
- Calls `spark.stop()` at the end.

### 3. Submission
Run the application against the cluster using your chosen method.

While the driver is running, in a second terminal capture:
- `kubectl get pods -w` output showing the driver pod and **at least 2 executor pods** in `Running` state.
- The driver-pod logs (`kubectl logs <driver-pod>`) showing both the `df.show(50)` table **and** your printed name and surname.

### 4. Cleanup
- After the app finishes, confirm `kubectl get pods` shows the driver pod as `Completed` (and executors gone).
- Stop the local cluster if you don't need it afterwards.

## Deliverables
Submit a single Markdown file `spark_on_k8s_hw_solution.md` (similar in structure to [01_spark-submit.md](01_spark-submit.md)) containing, in order:

1. The exact commands you ran to start the cluster and prepare RBAC.
2. The full source of `spark_on_k8s_hw.py`.
3. The exact `spark-submit` command (or `SparkApplication` YAML) you used.
4. The driver-side output showing the 50-row table.
5. A screenshot (`.png` embedded with `![alt](path)`) of the driver log section that prints your name and surname.
6. The `kubectl get pods` output showing the executor pods running, then completed.

## Grading checklist
- [ ] App ran on a real Kubernetes cluster (not local mode).
- [ ] At least 2 executor pods visible in `kubectl get pods`.
- [ ] 50 rows shown from a DataFrame built with `spark.range`.
- [ ] Student's name and surname clearly visible in the driver log screenshot.
- [ ] All commands and code reproducible from the submitted Markdown file.

## Hints
- **`spark-submit` master URL via `kubectl proxy`** — easiest path on Docker Desktop. In a separate terminal run:
  ```bash
  kubectl proxy --port=8001
  ```
  Leave it running, then use `--master k8s://http://127.0.0.1:8001` in your `spark-submit` command. This avoids dealing with the API server's TLS certs and bearer-token auth: the proxy uses your existing `kubectl` credentials and exposes the API over plain HTTP on localhost. (If you'd rather hit the API server directly, the URL is usually `k8s://https://kubernetes.docker.internal:6443` and you'll need to wire up the CA cert and a service-account token.)
- If executor pods crash with image-pull errors, pre-pull the image: `docker pull spark:3.5.3-java17` (Docker Desktop shares the daemon with the cluster).
- The Spark client and the cluster image **must match versions** — use `spark-3.5.3-bin-hadoop3` on the client side.
- If `kubectl logs <driver-pod>` returns nothing, the driver may have already terminated — that's why the 30 s sleep is required.