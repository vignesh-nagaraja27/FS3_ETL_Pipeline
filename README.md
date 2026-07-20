# FS3 ETL Data Pipeline

A production-grade, configuration-driven ETL platform for importing coordinate measuring machine (CMM) measurement data into the FS3 PostgreSQL database.

The service monitors machine output folders, validates and transforms incoming Excel or CSV files, loads measurement data atomically, tracks every pipeline execution, recovers interrupted work, and routes failures through a Dead Letter Queue (DLQ). A Streamlit-based Mission Control dashboard provides operational visibility and retry controls.

## Table of Contents

- [Key Features](#key-features)
- [Supported Machines](#supported-machines)
- [Architecture](#architecture)
- [Project Structure](#project-structure)
- [Quick Start](#quick-start)
- [Configuration](#configuration)
- [Processing Lifecycle](#processing-lifecycle)
- [Machine-Specific Parsing](#machine-specific-parsing)
- [Data Integrity and Reliability](#data-integrity-and-reliability)
- [Database Objects](#database-objects)
- [Dead Letter Queue and Retry](#dead-letter-queue-and-retry)
- [Logging and Monitoring](#logging-and-monitoring)
- [Troubleshooting](#troubleshooting)

## Key Features

- Automated monitoring of multiple machine output folders
- Excel and CSV ingestion (`.xls`, `.xlsx`, `.xlsm`, and `.csv`)
- Centralized runtime configuration through `configs/machines.json`
- Per-machine parser definitions and worker limits
- Concurrent processing with dynamically calculated executor capacity
- File stability checks before processing
- Duplicate and concurrent-processing prevention
- Filename-prefix exclusion rules
- Atomic PostgreSQL transactions with automatic rollback
- Crash recovery for interrupted pipeline runs
- Dead Letter Queue with operator-controlled retry
- Structured, contextual logging
- Mission Control dashboard for status, health, and failure visibility
- PostgreSQL connection pooling

## Supported Machines

| Machine | Pipeline | Version | Workers | Special behavior |
|---|---|---:|---:|---|
| `KTSF-0453` | `3D_HD` | `04530000` | 4 | HD measurement processing |
| `KTS-7106` | `3D_BL` | `07106000` | 2 | BL parsing, `measure_point`, and ignored filename prefixes |

Machine details are configuration values, not hard-coded application behavior. Machines can be added, disabled, or tuned in `configs/machines.json`.

## Architecture

```text
Machine output folders
          │
          ▼
     Job Runner
  folder watchers + worker pool
          │
          ▼
      Dispatcher
  pipeline classification
          │
          ▼
      Pipeline3D
          │
          ├── Copy       validate, deduplicate, route to _processing
          ├── Transform  parse, clean, normalize
          ├── Load       resolve master + insert measurements atomically
          └── Finalize   route to completed or _NG
          │
          ▼
  FS3 PostgreSQL database
          │
          ├── Pipeline state and audit records
          ├── Measurement master and transaction data
          └── Dead Letter Queue
```

## Project Structure

```text
ETL_Pipeline/
├── main.py                     # ETL service entry point
├── dashboard.py                # Streamlit Mission Control dashboard
├── config_loader.py            # Loads and validates runtime configuration
├── configs/
│   ├── machines.json           # Machines, parsers, workers, and file rules
│   └── db_config.json          # PostgreSQL connection configuration
├── orchestrator/
│   ├── job_runner.py           # Watchers, worker pool, recovery, DLQ retry
│   └── dispatcher.py           # Routes files to the appropriate pipeline
├── pipelines/
│   └── pipeline_3d.py          # Copy → Transform → Load → Finalize workflow
├── steps/
│   ├── copy.py                 # Stability checks, duplicates, file routing
│   ├── transform.py            # Excel/CSV parsing and normalization
│   ├── load.py                 # Master resolution and measurement inserts
│   └── finalize.py             # Success and failure file handling
├── db/
│   └── db_handler.py           # Thread-safe PostgreSQL connection pool
├── state/
│   └── status_manager.py       # Pipeline state and retry management
├── utils/
│   └── logger.py               # Structured application logging
└── logs/
    └── etl_pipeline.log        # Rotating runtime log
```

> `parts_3d.json` and `parts_checksheet.json` belong to the previous architecture and are no longer used. Runtime machine and parser behavior now comes from `configs/machines.json`.

## Quick Start

### Prerequisites

- Python 3
- Access to the FS3 PostgreSQL database
- Required FS3 control and measurement tables
- Read/write access to configured machine source folders

### 1. Install dependencies

```bash
pip install psycopg2-binary pandas openpyxl streamlit
```

### 2. Configure PostgreSQL

Update `configs/db_config.json`:

```json
{
  "host": "localhost",
  "port": 5432,
  "database": "FS3",
  "user": "postgres",
  "password": "replace-with-your-password"
}
```

> Do not commit real credentials. Keep production secrets outside version control and provide only a safe example configuration in the repository.

### 3. Configure machines

Define each machine in `configs/machines.json`. Confirm that its source path exists and that the service account can read and move files there.

### 4. Start the ETL service

From the project root:

```bash
python main.py
```

### 5. Start Mission Control

In a separate terminal:

```bash
streamlit run dashboard.py
```

## Configuration

### Single source of runtime behavior

The current architecture requires only two configuration files:

```text
configs/
├── machines.json
└── db_config.json
```

`machines.json` defines:

- Machine identity and source folder
- Pipeline type and version
- Whether the watcher is enabled
- Per-machine concurrency
- Ignored filename prefixes
- File parsing rules and column locations

### Machine example

```json
{
  "machine_id": "KTS-7106",
  "source_path": "C:\\FS3\\CMM\\Mitutoyo\\KTS-7106",
  "pipeline_type": "3D_BL",
  "version": "07106000",
  "max_workers": 2,
  "enabled": true,
  "ignore_file_prefixes": [
    "E-NR-BLOCK"
  ],
  "parser": {
    "file_type": "excel",
    "header_row": null,
    "data_start_row": 8,
    "evaluation_item1_column_idx": 0,
    "specification_column_idx": 2,
    "upper_limit_column_idx": 3,
    "lower_limit_column_idx": 4,
    "value_column_idx": 5,
    "excel_sheet_index": -1
  }
}
```

Restart the ETL service after changing configuration.

### Worker scaling

Each machine defines its concurrency independently:

```json
"max_workers": 4
```

At startup, the Job Runner calculates the shared executor capacity:

```text
total_workers = sum(machine.max_workers)
```

With the current configuration:

```text
KTSF-0453: 4 workers
KTS-7106:  2 workers
Total:     6 workers
```

Worker counts can therefore be tuned without changing Python code. Choose values that reflect machine volume, database capacity, and available server resources.

### Ignored filename prefixes

A machine can exclude files before classification and processing:

```json
"ignore_file_prefixes": [
  "E-NR-BLOCK"
]
```

| Filename | Result |
|---|---|
| `E-NR-BLOCK_001.xls` | Ignored |
| `E-NR-BLOCK_RC.xlsx` | Ignored |
| `RC_001.xls` | Processed |
| `GT3_001.xls` | Processed |

## Processing Lifecycle

### 1. Detect

The Job Runner monitors every enabled machine source folder for supported files. Ignore-prefix rules are applied before the file is classified or submitted to a worker.

### 2. Copy

The Copy step:

- Waits until the file is stable and no longer being written
- Checks whether the file has already been imported or claimed
- Registers the pipeline run
- Routes the claimed file into `_processing`

### 3. Transform

The Transform step:

- Reads the configured Excel sheet or CSV content
- Applies machine-specific row and column settings
- Removes invalid or empty input
- Parses measurement identities and values
- Produces normalized row dictionaries for the Load step

Conceptual output:

```python
{
    "evaluation_item1": "...",
    "measure_item": "...",
    "measure_point": "...",
    "measure_value": "...",
    "upper_limit": "...",
    "lower_limit": "..."
}
```

The exact fields depend on the configured pipeline and source format.

### 4. Load

The Load step resolves or creates the measurement master record and inserts the associated transaction values. All database writes for the file share one connection and one transaction.

### 5. Finalize

- Successful files are moved to the completed location and marked `COMPLETED`.
- Failed files are moved to `_NG`, marked `FAILED`, and registered in the DLQ for operator review.

## Machine-Specific Parsing

### KTS-7106 `measure_point`

KTS-7106 separates a source measurement label into three identity fields.

```text
Input:
右デッキ面ﾎﾞｱ裏面取り_直径_1

Normalized:
evaluation_item1 = 右デッキ面ﾎﾞｱ裏面取り
measure_item     = 直径
measure_point    = 1
```

`measure_point` participates in master-record resolution for KTS-7106. Measurements such as the following therefore resolve to distinct master records:

```text
直径_1
直径_2
直径_3
直径_4
```

This prevents separate physical measurement points from being merged under one master definition.

## Data Integrity and Reliability

### Atomic load transaction

Every file is loaded within one PostgreSQL transaction:

```sql
BEGIN;
-- Resolve or insert master records
-- Insert transaction measurements
COMMIT;
```

If any row or database operation fails, the complete transaction is rolled back:

```sql
ROLLBACK;
```

The guarantee is:

```text
All writes for the file are committed
OR
No writes for the file are committed
```

This prevents ghost master records in `fs3.pre_m_cmm` without corresponding values in `fs3.pre_t_cmm`.

### Concurrent claim protection

Pipeline registration uses the database as the authority for active file claims. This prevents multiple watcher threads from processing the same active file concurrently.

### Connection pooling

Database connections come from a thread-safe shared pool instead of opening a new connection on every polling cycle or load stage. Connections are returned to the pool after use.

### Crash recovery

At startup, `_cleanup_stale_tasks()` resets interrupted `PROCESSING` records to `PENDING`. This allows abandoned work to re-enter the normal processing lifecycle instead of remaining permanently stuck.

## Database Objects

### Control and observability

| Object | Purpose |
|---|---|
| `fs3.etl_pipeline_log` | Per-file processing state, current step, and failure details |
| `fs3.etl_dlq` | Failed-file review and retry state |
| `fs3.etl_machine_registry` | Machine registration, heartbeat, and health |
| `fs3.etl_audit_log` | Structured pipeline events and audit history |

### Measurement data

| Object | Purpose |
|---|---|
| `fs3.pre_f_3d_hd` | Staging data for 3D HD measurements |
| `fs3.pre_f_3d_bl` | Staging data for 3D BL measurements |
| `fs3.pre_m_cmm` | Measurement master definitions |
| `fs3.pre_t_cmm` | Imported measurement transaction values |
| `fs3.pre_w_3d_hd` | HD conversion-mapping staging data |

## Dead Letter Queue and Retry

When processing fails, the file is moved to `_NG` and an entry is created in `fs3.etl_dlq`.

```text
FAILED
   │
   ▼
AWAITING_REVIEW
   │ operator requests retry
   ▼
RETRYING
   │ retry watcher claims the work
   ▼
PROCESSING
   ├──► COMPLETED
   └──► FAILED
```

A retry returns the physical file to the configured machine source path, resets its pipeline state, and resubmits it through the normal workflow. Retry limits protect the service from uncontrolled failure loops.

To inspect the current queue:

```sql
SELECT
    file_name,
    machine_id,
    failed_step,
    error_category,
    retry_count,
    dlq_status,
    queued_at
FROM fs3.etl_dlq
ORDER BY queued_at DESC;
```

To inspect recent failed pipeline runs:

```sql
SELECT
    file_name,
    machine_id,
    vehicle_model,
    parts_code,
    op_no,
    current_step,
    error_message,
    updated_at
FROM fs3.etl_pipeline_log
WHERE status = 'FAILED'
ORDER BY updated_at DESC;
```

## Logging and Monitoring

Logs include operational context such as:

- Machine ID
- Vehicle model
- Part code
- Operation number
- Pipeline log ID
- Current processing step
- Error details when applicable

Example:

```text
[2026-07-20 08:30:20] [INFO]
[MC:KTS-7106] [MODEL:RC] [PART:BL] [OP:OP020] [LOG_ID:42]
LOAD STEP STARTED
```

Runtime logs are written to `logs/etl_pipeline.log`. Mission Control reads the control tables to provide pipeline status, machine health, throughput, and DLQ actions.

## Troubleshooting

| Symptom | Check | Likely cause or action |
|---|---|---|
| File is not detected | `source_path`, `enabled`, extension | Incorrect folder, disabled machine, or unsupported file type |
| File is intentionally skipped | `ignore_file_prefixes` | Filename starts with a configured excluded prefix |
| File remains in `_processing` | Startup recovery and logs | Restart the service and confirm stale-task cleanup runs |
| File is moved to `_NG` | `etl_pipeline.log` and `fs3.etl_dlq` | Review `failed_step`, `error_category`, and error details |
| Retry does not start | DLQ status and watcher logs | Confirm the entry reaches `RETRYING` and its source path is valid |
| Database connection fails | `db_config.json` and PostgreSQL availability | Verify host, port, credentials, network, and database service |
| Connection pool is exhausted | Worker totals and pool settings | Reduce workers or increase pool capacity within DB limits |
| Wrong columns are parsed | Machine `parser` settings | Verify row offsets, column indexes, and Excel sheet index |
| Service fails at startup | Both configuration files | Check for missing files, malformed JSON, or unavailable database |

## Current Architecture

**Edition:** Machine-Configuration Edition  
**Runtime source of truth:** `configs/machines.json`

Vehicle-model mapping files are no longer required. New machine behavior should be introduced through machine configuration and its parser definition wherever the existing pipeline supports it.
