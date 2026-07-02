# FS3 ETL Data Pipeline — Elite Edition

A configuration-driven, production-grade ETL system for processing 3D CMM measurement
files into the FS3 PostgreSQL database.

Built to replace a fragile multi-script system prone to silent failures, connection leaks,
and unreadable file naming conventions. Now hardened with Elite-Tier reliability fixes.

---

## Quick Start

```bash
# 1. Fill in your database credentials
notepad ETL_Pipeline/configs/db_config.json

# 2. Apply the database schema (run ONCE on your PostgreSQL server)
psql -h YOUR_HOST -U YOUR_USER -d YOUR_DB -f ETL_Pipeline/schema_mission_control.sql

# 3. Run the system
python ETL_Pipeline/main.py

# 4. Open the Mission Control Dashboard (separate terminal)
streamlit run ETL_Pipeline/dashboard.py
```

The system starts immediately and monitors all machine folders defined in `configs/machines.json`.

---

## What Problem Does This Solve?

The previous system (`3D_HD_01` through `3D_HD_05`) had four critical flaws:

| Problem | Symptom | Root Cause |
|---|---|---|
| **RC Model OP010 never saved** | Data visible in staging but missing from `pre_m_cmm` | `sync_pre_m_1to1_from_pre_f()` silently skipped new records |
| **Connection exhaustion** | Database server slowdowns / connection refused | New `psycopg2.connect()` opened on every loop tick |
| **Cryptic file naming** | `♡file.csv` → `★file.csv` → `●file.csv` | State tracked by renaming files with Japanese symbols |
| **Regex crash for RC files** | Files moved to `並び替えNG` | `parse_keys_from_filename()` split on `_` producing just `"RC"` with no R/L digit match |

All four are resolved in this redesign.

---

## Elite-Tier Reliability Fixes (v2)

This version passed a rigorous 6-scenario QA audit. Four critical issues were found and fixed.

### Fix 1 — Atomic Concurrency Lock (Scenario 1: "Burst" Test)

**Problem:** The old `_is_file_processed()` → `register()` pattern was a TOCTOU race condition.
Two machine watcher threads could both read "file not processed," both pass the check, and both
attempt to insert the same file simultaneously — causing duplicate DB rows and double processing.

**Fix:** `StatusManager.register()` now uses a **PostgreSQL partial unique index** as an
atomic server-side lock:

```sql
-- Required index (included in schema_mission_control.sql):
CREATE UNIQUE INDEX idx_pipeline_active_file
    ON fs3.etl_pipeline_log(file_name)
    WHERE (status IN ('PENDING', 'PROCESSING'));

-- The atomic INSERT in register():
INSERT INTO etl_pipeline_log (...) VALUES (...)
ON CONFLICT (file_name) WHERE (status IN ('PENDING', 'PROCESSING'))
DO NOTHING
RETURNING id;
```

If two threads race: only ONE gets an `id` back. The other gets `NULL` and skips.
**The database enforces the lock — not application code.**

---

### Fix 2 — ACID Ghost Data Elimination (Scenario 3: "Ghost Data" Test)

**Problem:** `load.py` opened **three separate connections** — one per stage (staging,
master, transaction). Each committed independently. If `_insert_transaction()` failed after
`_upsert_master()` succeeded, a **Ghost Master Record** was created: `pre_m_cmm` had a
definition row but `pre_t_cmm` had no measurement values.

**Fix:** The entire LOAD step now runs inside **one atomic transaction** owned by `pipeline_3d.py`:

```python
# pipeline_3d.py — single transaction boundary:
with self._db.get_connection() as conn:       # ← ONE connection
    committed = load_step.run(..., conn=conn)  # ← all 3 stages share this cursor
# conn.commit() if success │ conn.rollback() if ANY exception
```

`load.py` now receives a cursor and executes SQL only — **it never calls commit or rollback**.
The `ConnectionContext.__exit__` in `db_handler.py` handles the commit/rollback atomically.

**Result:** All rows for a file commit together, or **none** do. Zero ghost records.

---

### Fix 3 — Startup Self-Healing (Scenario 5: "Double Process" Test)

**Problem:** If the server crashed mid-run, rows stayed at `status='PROCESSING'` in
`etl_pipeline_log` forever. The watcher's `_is_file_processed()` saw them as "active"
and never re-queued them. Files were permanently stuck without operator intervention.

**Fix:** `JobRunner.start()` calls `_cleanup_stale_tasks()` before launching any watcher threads:

```python
UPDATE fs3.etl_pipeline_log
SET status = 'PENDING', current_step = 'STALE_RESET', updated_at = NOW()
WHERE status = 'PROCESSING'
RETURNING id, file_name;
```

On every restart, stale files are automatically reset to `PENDING` and re-enter the queue
within one poll cycle. **The system heals itself.**

Additionally, `_is_file_processed()` now uses a 30-minute timeout: a `PROCESSING` entry older
than 30 minutes is treated as a crash victim and allowed back into the queue immediately.

---

### Fix 4 — Complete DLQ Retry (Scenario 6: "Dead Letter" Test)

**Problem:** The original "🔁 Retry" button only set `dlq_status='RETRYING'` in the database.
It did **not** move the file, reset the pipeline log, or trigger any re-processing. Clicking
Retry was effectively a no-op.

**Fix:** The Retry button now executes a 5-step sequence:

```
Step 1: Guard — enforce max_retries limit
Step 2: Read machines.json to find the machine's source_path
Step 3: shutil.move(ng_path → source_path)   ← PHYSICAL FILE MOVE
Step 4: UPDATE etl_pipeline_log SET status='PENDING'  ← DB RESET
Step 5: UPDATE etl_dlq SET dlq_status='RETRYING'     ← TRIGGER FLAG
```

A new dedicated background thread in `job_runner.py` (`_watch_dlq_retrying`) polls for
`RETRYING` entries every 15 seconds, claims them atomically, and re-submits to the pipeline
executor using the **original log_id** (no duplicate log rows).

---

## Architecture Overview

```
ETL_Pipeline/
│
├── main.py                     # Entry point. Run this to start everything.
│
├── config_loader.py            # Reads all JSON configs at startup. Central config API.
│
├── configs/                    # The "brain" — edit these, not Python files.
│   ├── db_config.json          # Database credentials (fill this in before running).
│   ├── machines.json           # Which CMM machines to watch and how.
│   ├── parts_3d.json           # Rules: filename → vehicle model + part + parser settings.
│   └── parts_checksheet.json   # Same but for checksheet files.
│
├── orchestrator/               # The supervisors.
│   ├── job_runner.py           # Starts watcher threads. Routes files. Manages thread pool.
│   │                           # v2: Startup cleanup + DLQ retry watcher thread.
│   └── dispatcher.py           # Classifies a file: 3D_HD? 3D_BL? CHECKSHEET? UNKNOWN?
│
├── pipelines/                  # Assembly lines — step sequencing logic.
│   └── pipeline_3d.py          # COPY → TRANSFORM → LOAD (single txn) → FINALIZE.
│                               # v2: Atomic transaction wrapper + DLQ retry path.
│
├── steps/                      # Atomic workers. Each does one job only.
│   ├── copy.py                 # Waits for file stability, checks for duplicates, moves file.
│   ├── transform.py            # Reads CSV/Excel using config parser settings. Returns clean rows.
│   ├── load.py                 # v2: Single-cursor executor (no internal commits).
│   └── finalize.py             # Moves file to _completed or _NG based on outcome.
│
├── state/
│   └── status_manager.py       # v2: Atomic register() with ON CONFLICT partial index.
│                               # New reset_for_retry() for DLQ retry path.
│
├── db/
│   └── db_handler.py           # Singleton connection pool (psycopg2.ThreadedConnectionPool).
│
├── utils/
│   └── logger.py               # Structured logger. Logs include [MC] [MODEL] [PART] [OP].
│
├── dashboard.py                # Mission Control Dashboard (Streamlit).
│                               # v2: Retry button physically moves file + resets pipeline log.
│
└── schema_mission_control.sql  # All CREATE TABLE + CREATE INDEX statements.
                                # v2: Includes idx_pipeline_active_file partial unique index.
```

---

## How Data Flows (Step by Step)

```
CMM Machine Folder
       │
       │  (file lands: 193E_R10_連続測定_..._20251126.xlsx)
       ▼
[1] Job Runner polls folder every 5 seconds (case-insensitive extension check)
       │
       ▼
[2] Dispatcher checks filename against parts_3d.json (re.IGNORECASE)
       │   match_pattern: "^193E_R" → vehicle_model: RC, op_no: OP010
       ▼
[3] Pipeline3D.run() starts
       │
       ├─[REGISTER]─────────  StatusManager.register() — ATOMIC DB CLAIM
       │                      ON CONFLICT (file_name) WHERE PENDING/PROCESSING
       │                      → returns log_id or None (another thread got it)
       │
       ├─[COPY STEP]────────  Wait for file stability (size polling)
       │                      Duplicate check (pre_l_3d_dldate)
       │                      Move to _processing/
       │
       ├─[TRANSFORM STEP]───  Read Excel/CSV using parser config (header_row, etc.)
       │                      Clean NaN rows, parse measure_no and measure_value
       │
       ├─[LOAD STEP]─────────────────────────────────────────────────────────────
       │  ┌─ SINGLE ATOMIC TRANSACTION ─────────────────────────────────────────┐
       │  │  with db.get_connection() as conn:                                  │
       │  │      for each row:                                                  │
       │  │          INSERT into pre_f_3d_hd  (staging)     ─ no commit        │
       │  │          UPSERT into pre_m_cmm    (master)      ─ no commit        │
       │  │          INSERT into pre_t_cmm    (transaction) ─ no commit        │
       │  │  ↓ all succeed → conn.COMMIT() ← one atomic write                  │
       │  │  ↓ any fail   → conn.ROLLBACK() ← zero partial data                │
       │  └─────────────────────────────────────────────────────────────────────┘
       │
       └─[FINALIZE STEP]────  Move to _completed/ (success)
                              OR _NG/ (failure → DLQ entry created)
```

---

## The State Machine (Replacing the Symbols)

Every pipeline run is tracked in the `fs3.etl_pipeline_log` table.

```
PENDING ──► PROCESSING ──► COMPLETED
                  │
                  └────────► FAILED ──► DLQ (AWAITING_REVIEW)
                                              │
                                    [Operator clicks Retry]
                                              │
                                    ┌─────────────────────┐
                                    │ Dashboard:           │
                                    │ 1. Move file _NG→src │
                                    │ 2. Reset log→PENDING │
                                    │ 3. DLQ→RETRYING      │
                                    └──────────┬──────────┘
                                               │
                                    [DLQ Watcher Thread]
                                               │
                                               ▼
                                          PROCESSING ──► COMPLETED
```

To check what failed and why:
```sql
-- Show all failures in the last 24 hours
SELECT file_name, vehicle_model, parts_code, op_no, current_step, error_message, updated_at
FROM fs3.etl_pipeline_log
WHERE status = 'FAILED'
  AND updated_at > NOW() - INTERVAL '24 hours'
ORDER BY updated_at DESC;

-- What's in the Dead Letter Queue right now?
SELECT file_name, machine_id, failed_step, error_category, retry_count, queued_at
FROM fs3.etl_dlq
WHERE dlq_status = 'AWAITING_REVIEW'
ORDER BY queued_at DESC;
```

---

## Database Schema — Required Objects

Run `schema_mission_control.sql` **once** on your PostgreSQL server. It creates:

| Object | Type | Purpose |
|---|---|---|
| `fs3.etl_pipeline_log` | TABLE | Per-file state machine (PENDING → COMPLETED/FAILED) |
| `fs3.etl_audit_log` | TABLE | Structured event log for all pipeline activity |
| `fs3.etl_dlq` | TABLE | Dead Letter Queue — failed files awaiting retry |
| `fs3.etl_machine_registry` | TABLE | CMM machine heartbeat and health tracking |
| `idx_pipeline_active_file` | **UNIQUE PARTIAL INDEX** | **Critical:** enables atomic file claim in `register()` |
| `fs3.v_etl_today_kpis` | VIEW | Pre-built KPI view for the dashboard Overview pillar |
| `fs3.v_etl_hourly_throughput` | VIEW | 12-hour throughput chart data |
| `fs3.v_machine_health` | VIEW | Machine ONLINE/IDLE/WARNING status |

### Measurement Tables (pre-existing in FS3)

| Table | Schema | Purpose |
|---|---|---|
| `pre_l_vehicle_model` | `fs3` | Lookup: filename prefix → vehicle model code |
| `pre_l_3d_dldate` | `fs3` | Duplicate detection: records filename + file size per import |
| `pre_f_3d_hd` | `fs3` | Staging table for 3D HD measurement rows |
| `pre_f_3d_bl` | `fs3` | Staging table for 3D BL measurement rows |
| `pre_m_cmm` | `fs3` | **Master table**: measurement definitions per model/op |
| `pre_t_cmm` | `fs3` | **Transaction table**: actual measurement values |
| `pre_w_3d_hd` | `fs3` | Conversion mapping staging (populated by conversion import) |

---

## How to Add a New Vehicle Model or Part

You do **not** need to touch any Python files.

**Step 1:** Open `ETL_Pipeline/configs/parts_3d.json`

**Step 2:** Add a new object to the `"mappings"` array:

```json
{
  "match_pattern": "^NEW_MODEL_R",
  "description": "New Vehicle Model X - Right Hand Side - OP030",
  "vehicle_model": "X",
  "parts_code": "HD-RH",
  "side": "R",
  "op_no": "OP030",
  "vpo_code": "VPO-X-R30",
  "parser": {
    "file_type": "excel",
    "header_row": 10,
    "data_start_row": 11,
    "value_column_idx": 7,
    "measure_no_col_idx": 12,
    "excel_sheet_index": -1
  }
}
```

**Step 3:** Save the file and restart the service. Done.

> **Tip:** The `match_pattern` is a regular expression matched **case-insensitively** against the filename. This means `.CSV`, `.XLSX`, Japanese characters in filenames (e.g. `193E_R10_連続_9999_20260702.xlsx`) all work correctly.

---

## How to Add a New Machine

Open `ETL_Pipeline/configs/machines.json` and add to `"watch_directories"`:

```json
{
  "machine_id": "NEW-MACHINE-001",
  "description": "New Zeiss CMM - Hall E, Line 2",
  "source_path": "C:/CMM/Zeiss/NEW-MACHINE-001/Output",
  "pipeline_type": "3D_HD",
  "poll_interval_sec": 5,
  "stable_wait_sec": 2,
  "stable_required_count": 3,
  "max_workers": 2,
  "enabled": true
}
```

Restart the service. The new machine watcher starts automatically.

---

## Log Format

Every log line tells you exactly where and why something happened:

```
[2026-07-02 17:10:03] [ERROR] [PID:4821] [MC: KTSF-0453] [MODEL: RC] [PART: HD-RH] [OP: OP010]
 - [LOG_ID:42] [ROW:1/200] [MEASURE_NO:3] LOAD STEP FAILED.
   ALL 0 previously staged rows will ROLLBACK automatically.
   Root cause: DataError: invalid input syntax for type numeric
```

Log files rotate at 10 MB with 5 backups kept in `ETL_Pipeline/logs/etl_pipeline.log`.

---

## Python Dependencies

```
psycopg2-binary
pandas
openpyxl
streamlit
```

Install with:
```bash
pip install psycopg2-binary pandas openpyxl streamlit
```

---

## Troubleshooting Common Issues

| Symptom | Where to Look | What to Check |
|---|---|---|
| File not picked up | `etl_pipeline.log` | Is filename matched by any `match_pattern` in `parts_3d.json`? |
| RC/OP010 still not in master | `etl_pipeline_log` table | Check `current_step` and `error_message` columns |
| "Connection pool exhausted" | `etl_pipeline.log` | Increase `max_conn` in `db_config.json` |
| File stuck in `_processing` | System restart | `_cleanup_stale_tasks()` resets it automatically on next start |
| Retry button does nothing | `etl_pipeline.log` | Check DLQ watcher logs — `[DLQ-WATCHER]` prefix |
| Ghost master records | Schema check | Ensure `idx_pipeline_active_file` partial index exists in DB |
| "Config file missing" on startup | Terminal output | Run `notepad configs/db_config.json` and fill in credentials |
| File at _NG, retry fails | Dashboard error | Confirm `file_path` in DLQ matches actual file location |
