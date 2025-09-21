# Food Delivery Pipeline 

---

## Table of Contents

1. [Overview](#overview)
2. [Problem statement & Objectives](#problem-statement--objectives)

---

## 1. Overview <a name="overview"></a>

### 1.1 Executive summary

This project is a dockerized, event-aware data pipeline that simulates and ingests online food-delivery orders, validates and transforms them, stores them in a Postgres analytics database, and surfaces KPIs in Metabase. It was built with following production-grade patterns: event-driven ingestion, strict schema validation, idempotent upserts, file-level audit trails, and clear observability.

### 1.2 High-level components

* **Data generator** — produces synthetic CSV files that represent incoming orders (used for testing & demo).
* **MinIO (object store)** — three buckets/areas: `raw` (incoming), `processed` (accepted/archived), `rejected` (validation failures). MinIO stands in for S3 during local development.
* **Airflow (orchestrator)** — DAG `data_pipeline` orchestrates the flow: wait/sensor → generate  → validate → transform → upsert → archive. The DAG includes branching for validation outcome and small no-op nodes to make the control flow explicit.
* **Validator (Pandera)** — enforces schema, types, ranges, and business rules; returns structured validation results to `file_registry`.
* **Transformer** — normalizes columns, casts types, computes derived fields (e.g., delivery\_duration), and writes a canonical transformed CSV.
* **Upsert (DB loader)** — idempotent upsert logic (`INSERT ... ON CONFLICT DO UPDATE` or `MERGE`) into `orders` table and writes file-level metadata into `file_registry`.
* **Postgres (orders\_db)** — final data store for analytics; contains `orders` and `file_registry` tables.
* **Metabase** — analytic UI for dashboards and ad-hoc queries.
* **Observability** — Airflow logs, simple metrics, and Slack/webhook alerts for critical failures.

### 1.3 Data flow 

1. A new CSV lands in MinIO `raw/` (in production via the producer or upstream partner).
2. Airflow **sensor** detects the object (see Sensor notes below) and triggers the DAG run.
3. The DAG calls the Validator on the newest file.

   * If validation **passes** → Transformer → Upsert → Archive (move to `processed`) → mark file success in `file_registry`.
   * If validation **fails** → move file to `rejected`, write details to `file_registry`, and terminate the run gracefully (no exception-bubble).
4. Metabase queries Postgres for dashboards; analysts review KPIs.

> Note: for demo/test runs, the DAG includes a `generate_data` task to produce sample CSVs — this is intentionally part of the DAG to make end-to-end demos simple. The DAG is written so the **sensor-first** (wait) behavior remains the canonical event-driven pattern while the generator can be used as an emulation step in local testing.

### 1.4 Why sensor-first (event-driven) and implementation note

I standardised on a **sensor-first** approach to make the pipeline event-driven and reduce wasted runs. The rationale:

* **Event-driven** pipelines minimize latency — process as soon as data appears.
* **Cost and load**: sensors (especially deferrable sensors) are cheaper than high-frequency scheduled DAGs.
* **Operational clarity**: a sensor shows an explicit wait for external data in the DAG graph, which is easier for reviewers and auditors to understand.

Implementation options and recommended pattern:

* **Deferrable S3/MinIO sensor** (preferred): for Airflow 2.2+, implement a deferrable sensor (S3KeySensor-like) or a custom `MinioKeySensor` which uses the deferrable sensor base. Deferrable sensors yield the worker slot while polling, greatly reducing scheduler/workload costs.
* **Polling sensor**: simple implementation using `PythonSensor`  with a sensible poke interval (e.g., `poke_interval=30s`, `timeout=12h`). Works for local demos but not ideal for scale.
* **Push-based alternative (recommended for production at scale)**: use MinIO/S3 event notifications (webhook or message to a queue) to trigger a DAG run via the Airflow REST API or use a lightweight function that writes a record to a trigger table. This is more real-time and eliminates polling entirely.


### 1.5 Key engineering patterns applied

* **Idempotency**: ingestion uses a staging area and de-duplication logic before upsert; `orders.order_id` is the primary key and upsert uses safe merge semantics to avoid duplicates on replays.
* **Schema-as-code**: Pandera schemas live in code and are version-controlled; any schema change is a code change that must pass CI.
* **File-level audit trail (`file_registry`)**: every file processed writes an entry with `file_name`, `status`, `validation_ok`, `rows_processed`, `errors`, `processed_at`, and `run_id`. This enables traceability and replay.
* **Fail fast vs graceful handling**: validation failures do not throw unhandled exceptions that crash the DAG run; instead they branch to a rejection path so downstream jobs are not left in an inconsistent state.
* **Separation of concerns**: generator (test-only) is separate from production ingestion; transforms and upserts are isolated modules for easier testing.
* **Local-dev parity**: Docker Compose mirrors core production pieces (S3/MinIO, Postgres, Airflow) so behaviors are validated locally.


---

## 2. Problem statement & Objectives <a name="problem-statement--objectives"></a>

### 2.1 Problem statement

Modern analytics teams must quickly convert operational events (here: food delivery orders) into reliable, queryable datasets for business stakeholders. The primary problems we solve with this pipeline are:

1. **Unreliable input sources**: incoming CSVs may be malformed, contain invalid types, or carry business-rule violations (e.g., negative amounts, impossible distances). The pipeline must detect, quarantine, and surface these in an auditable way.
2. **Non-idempotent ingestion**: upstream systems may re-deliver files or re-run exports. The pipeline must be safe to re-run without creating duplicates or corrupting analytics.
3. **Lack of observability**: without structured logging and file-level metadata, it’s hard to track what was processed, when, and why failures occurred.
4. **Developer/deployment friction**: developers need a reproducible local setup for debugging, validation, and demos that resembles production behaviors.
5. **Operational cost and responsiveness**: naive schedule-based processing either delays data or wastes compute. The system needs to be event-aware and efficient.

### 2.2 Primary objectives (technical & acceptance criteria)

Below are the objectives I aimed to fulfil in this project, framed with measurable acceptance criteria where applicable.

**Objective A — Reliable, schema-validated ingestion**

* Implement Pandera-based validation that checks column types, nullability rules, allowed ranges (e.g., `distance_km` between 1 and 20), and rating constraints.
* Acceptance: invalid files are routed to `rejected` bucket and a `file_registry` row contains failure details; no downstream processing occurs for that file.

**Objective B — Idempotent, auditable upsert**

* Use staging + dedupe + upsert pattern. Implement upsert using safe DB semantics (`ON CONFLICT` / `MERGE`) and transactions. Record per-file metadata in `file_registry`.
* Acceptance: repeated ingestion of the same file should not create duplicate `order_id` rows; the `orders` table must remain consistent and `file_registry` must reflect successful/duplicate runs.

**Objective C — Event-driven orchestration with graceful branching**

* Use a sensor (or deferrable sensor) to detect new objects in MinIO and trigger processing; implement BranchPythonOperator for validation to route to success or rejection paths.
* Acceptance: when a file fails validation the DAG finishes cleanly without unhandled exceptions and the rejection path is observable in Airflow UI and `file_registry`.

**Objective D — Developer-friendly reproducibility**

* Provide a single-command local startup (`docker compose up -d --build`), a demo generator for synthetic data, and explicit demo scripts for replaying the full flow.
* Acceptance: a new developer can reproduce an end-to-end run (generator → Airflow run → data in Postgres → Metabase dashboard) using the README.

**Objective E — Observability & minimal alerting**

* Logs per-task in Airflow, structured `file_registry` records, and a basic Slack/webhook alert for critical failures.
* Acceptance: critical validation failures send a Slack message and file-level errors are queryable.

**Objective F — Clear road for production hardening**

* Document the migration path to production: deferrable sensors, S3 event notifications, secret management, migration scripts (Alembic), and CI that runs core DAG tasks in a sandbox.
* Acceptance: README contains a "Next steps for production" checklist and CI job templates.

### 2.3 KPIs & SLAs (recommended)

* **Data freshness SLA**: process new file within 5 minutes of arrival (tunable based on business need).
* **Validation coverage**: 100% of files processed must go through Pandera validation prior to upsert.
* **Idempotency**: 0 duplicates per run (validated by `count(distinct order_id)`).
* **Observability**: 100% of failed files must have an entry in `file_registry` with errors captured.

---


