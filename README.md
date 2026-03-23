# Alexandria

A distributed asynchronous job processing platform. Clients submit files via a REST API, receive a job ID instantly, and poll for results while Alexandria routes work to the appropriate worker in the background.

The core system is fully generic — you wire in your own task modules and routing rules to handle any workload at scale.

---

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [Architecture](#2-architecture)
3. [Prerequisites](#3-prerequisites)
4. [Environment Setup](#4-environment-setup)
5. [Configuration Checklist](#5-configuration-checklist)
6. [Booting the Platform](#6-booting-the-platform)
7. [How to Use the API](#7-how-to-use-the-api)
8. [Checking Job Status](#8-checking-job-status)
9. [Output Files](#9-output-files)
10. [Adding Your Own Workers](#10-adding-your-own-workers)
11. [Shutting Down](#11-shutting-down)
12. [v1.0 Architecture Highlights](#12-v10-architecture-highlights)

---

## 1. Project Overview

Alexandria decouples file ingestion from processing. Instead of making a client wait for a long-running task to complete, the API accepts the file, stamps it with a unique job ID, publishes a lightweight message to a queue, and returns immediately. One or more workers consume from that queue and execute whatever logic you plug in.

**Core design principles:**

- **Non-blocking ingestion** — the API never waits for a task to finish. Every job returns a `job_id` in milliseconds.
- **Content-based routing** — the file's MIME type determines which queue (and therefore which worker) handles it. No client-side configuration required.
- **Claim Check pattern** — files over 1 MB are saved to disk. Only a reference path travels through the message queue, keeping broker memory usage flat regardless of file size.
- **Pluggable workers** — task logic lives in isolated modules. Swapping or extending what Alexandria can process requires no changes to the core platform.

---

## 2. Architecture

Alexandria is a hybrid system. The control plane (API, broker, state store) runs in Docker. Workers run natively on the host machine, giving them direct access to whatever tools, runtimes, or hardware the task requires.

```
  Client
    │
    ▼
┌─────────────────────┐
│   FastAPI Gateway   │  (Docker) — Accepts uploads, issues job IDs
└──────────┬──────────┘
           │ publishes message
           ▼
┌─────────────────────┐
│      RabbitMQ       │  (Docker) — Routes messages by MIME type
└──────────┬──────────┘
           │ consumes message
           ▼
┌─────────────────────┐
│   Python Workers    │  (Native host) — Executes your task modules
└──────────┬──────────┘
           │ writes status
           ▼
┌─────────────────────┐
│       Redis         │  (Docker) — Job state ledger
└─────────────────────┘
```

| Component | Runtime | Role |
|---|---|---|
| `main.py` | Docker | FastAPI gateway — accepts uploads, publishes to RabbitMQ |
| `worker.py` | Native host | Consumes queue messages, dispatches to task modules |
| `modules/` | Native host | Your task logic — one module per job type |
| Redis | Docker | Stores `job_id → status` for client polling |
| RabbitMQ | Docker | Message broker with a built-in management UI |

---

## 3. Prerequisites

| Dependency | Notes |
|---|---|
| **Docker Desktop** | Must be running before you start Alexandria |
| **Python 3.9+** | Used to run the native workers |
| **Any native tools your tasks need** | e.g. compilers, runtimes, CLIs — install these on the host as required by your modules |

---

## 4. Environment Setup

Create a `.env` file in the project root. Docker Compose and the native workers both read from this file.

```bash
# .env

# --- RabbitMQ ---
RABBITMQ_USERNAME=guest
RABBITMQ_PASSWORD=guest
RABBITMQ_HOST=localhost
RABBITMQ_PORT=5672
RABBITMQ_EXCHANGE=your_exchange_name

# --- Redis ---
REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_USERNAME=default
REDIS_PASSWORD=your_redis_password
```

**Variable reference:**

| Variable | Description |
|---|---|
| `RABBITMQ_USERNAME` | RabbitMQ login username |
| `RABBITMQ_PASSWORD` | RabbitMQ login password |
| `RABBITMQ_HOST` | Broker hostname — use `localhost` for native workers |
| `RABBITMQ_PORT` | AMQP port (default: `5672`) |
| `RABBITMQ_EXCHANGE` | The exchange your queues are bound to |
| `REDIS_HOST` | Redis hostname — use `localhost` for native workers |
| `REDIS_PORT` | Redis port (default: `6379`) |
| `REDIS_USERNAME` | Redis username (default: `default`) |
| `REDIS_PASSWORD` | Must match the password set in Docker Compose |

> **Note:** `RABBITMQ_HOST` and `REDIS_HOST` are automatically overridden to their internal Docker service names for the API container. The values in your `.env` are used only by the native workers.

---

## 5. Configuration Checklist

Before booting Alexandria for the first time, make sure every item below matches your target RabbitMQ architecture. Each location is marked with a `*** CHANGE THIS ***` comment in the source file to make them easy to find.

---

### `.env` — credentials and connection details

```bash
RABBITMQ_USERNAME=guest       # ← your broker username
RABBITMQ_PASSWORD=guest       # ← your broker password
RABBITMQ_EXCHANGE=my_exchange # ← the exchange your queues are bound to
REDIS_PASSWORD=your_password  # ← must match the value Docker Compose uses
```

Change all four of these to real values before running anything. The defaults (`guest`/`guest`) are RabbitMQ's built-in test credentials and should not be used in any shared or production environment.

---

### `main.py` — MIME type → routing key map

```python
# *** CHANGE THIS TO MATCH YOUR RABBITMQ BINDING ***
ROUTING_MAP = {
    "application/json": "process.json",
    "application/vnd.android.package-archive": "process.apk",
    "application/zip": "build.unity",
    "text/plain": "process.logs"
}
```

Each entry maps an incoming file's MIME type to a RabbitMQ routing key. The routing key must match a binding on your exchange. Add, remove, or rename entries to reflect the file types your system actually handles.

---

### `worker.py` — routing key → module map

```python
# *** CHANGE THIS TO MATCH YOUR MODULES ***
ROUTING_MAP = {
    "process.json": handling_json,
    "process.log":  handling_log
}
```

Each entry maps a routing key (as published by the API) to the Python module that handles it. The keys here must match the routing keys you defined in `main.py`. See [Adding Your Own Workers](#10-adding-your-own-workers) for how to create and register a new module.

---

### `start.sh` — queue names per worker process

```bash
# *** CHANGE THIS TO MATCH YOUR RMQ QUEUES ***
export TASK_QUEUE="data_processing"
venv/bin/python worker.py &

export TASK_QUEUE="io_tasks"
venv/bin/python worker.py &
```

Each `worker.py` process subscribes to the queue named by `TASK_QUEUE`. These queue names must exist in your RabbitMQ setup and must be bound to the exchange and routing keys you configured above. Add or remove worker launch lines here to match the number of queues in your architecture.

---

> **Summary:** there are four places to configure — `.env`, `main.py`, `worker.py`, and `start.sh`. They form a chain: credentials and exchange live in `.env`, MIME types map to routing keys in `main.py`, routing keys map to modules in `worker.py`, and queue subscriptions are set per-worker in `start.sh`. All four must be consistent with each other and with your RabbitMQ topology.

---

## 6. Booting the Platform

### First-time setup

Make the start script executable (one-time):

```bash
chmod +x start.sh
```

### Start

```bash
./start.sh
```

The script runs the following steps automatically:

1. **Virtual environment** — creates `venv/` and installs `requirements.txt` if it doesn't already exist.
2. **Docker Compose** — starts RabbitMQ, Redis, and the FastAPI gateway as detached containers.
3. **Initialization wait** — pauses 5 seconds for RabbitMQ and Redis to finish booting.
4. **Native workers** — spawns one worker process per queue in the background:
   - Worker 1 → `data_processing` queue
   - Worker 2 → `io_tasks` queue
5. **Stays alive** — holds the terminal open so CTRL+C gracefully stops all workers.

When you see `✅ Alexandria is fully operational!` the system is accepting jobs.

---

## 7. How to Use the API

### Submit a job

```
POST http://localhost:8000/api/jobs
```

Send a `multipart/form-data` request with your file attached under the key **`upload_file`**. Alexandria reads the file's MIME type and routes it to the correct worker automatically.

**Default routing map** (configure in `main.py`):

| MIME Type | Routing Key |
|---|---|
| `application/json` | `process.json` |
| `text/plain` | `process.logs` |
| `application/zip` | `build.unity` |
| `application/vnd.android.package-archive` | `process.apk` |

Files with an unrecognized MIME type are rejected with a `400` error.

---

### cURL

```bash
curl -X POST http://localhost:8000/api/jobs \
  -F "upload_file=@/path/to/your/file.json;type=application/json"
```

The `;type=application/json` suffix explicitly sets the MIME type on the upload. Adjust it to match your file type.

---

### Postman

1. Method: **POST** — URL: `http://localhost:8000/api/jobs`
2. **Body** tab → select **form-data**
3. Add key: `upload_file` — change the field type from **Text** to **File**
4. Choose your file
5. **Send**

---

### Response

```json
{
  "job_id": "a3f1c2d4-e5b6-7890-abcd-ef1234567890",
  "routed_to": "process.json",
  "filename": "file.json"
}
```

Store the `job_id` to poll for results.

---

## 8. Checking Job Status

```
GET http://localhost:8000/api/jobs/{job_id}
```

```bash
curl http://localhost:8000/api/jobs/a3f1c2d4-e5b6-7890-abcd-ef1234567890
```

**In progress:**
```json
{ "status": "processing", "task_type": "process.json", "last_update": "..." }
```

**Completed:**
```json
{ "status": "completed", "task_type": "process.json", "last_update": "..." }
```

**Failed:**
```json
{ "status": "failed", "error": "...", "task_type": "process.json", "last_update": "..." }
```

---

## 9. Output Files

Alexandria organizes files into categorized subfolders under `temp/alexandria_assets/`. Your modules are responsible for writing output to the appropriate subfolder. The examples below reflect the default module structure:

```
temp/
└── alexandria_assets/
    ├── jsons/
    │   └── {job_id}_blueprint.json     ← input data saved by the JSON module
    ├── apks/
    │   └── {job_id}_game.apk           ← compiled output written by the JSON module
    └── logs/
        └── {job_id}_build.log          ← log files written by the log module
```

For large files routed via the Claim Check path (≥ 1 MB), the raw uploaded file is also staged under `temp/alexandria_assets/` during processing and automatically deleted by the worker's `finally` block once the job completes or fails.

---

## 10. Adding Your Own Workers

Alexandria's task logic is entirely contained in the `modules/` directory. The core platform (`main.py`, `worker.py`) does not need to change.

### Step 1 — Create your module

Add a file to `modules/` that exposes a `run(message, ch)` function:

```python
# modules/my_task.py

def run(message, ch):
    data = message.get("data")        # direct payload (small files)
    file_path = message.get("file_location")  # claim check path (large files)
    job_id = message.get("job_id")

    # Your task logic here

    # If your module writes a temporary file, inject its path back into the
    # message dict before returning (see the Janitor rule below).
    message["file_location"] = my_output_path
```

The `message` dict always contains:

| Key | Description |
|---|---|
| `job_id` | Unique identifier for this job |
| `task_type` | The routing key that dispatched this message |
| `payload_type` | `"direct"` (data in message) or `"ticket"` (data on disk) |
| `data` | File contents as a string — present when `payload_type` is `"direct"` |
| `file_location` | Path to the staged file — present when `payload_type` is `"ticket"` |

The `ch` parameter is the live RabbitMQ channel, available if your module needs to publish chained events downstream.

#### The Janitor rule

The `finally` block in `worker.py` automatically deletes any file referenced by `message["file_location"]` after every job, whether it succeeded or failed. This keeps the host disk clean without any cleanup code in your module.

**If your module generates a temporary file during execution, you must inject its path back into the message dict before the function returns:**

```python
message["file_location"] = my_new_path
```

This tells the worker's `finally` block exactly where to find the file so it can delete it. If you skip this step, the file will be left on disk permanently.

### Step 2 — Register the MIME type in `main.py`

```python
ROUTING_MAP = {
    "application/json": "process.json",
    "text/plain":       "process.logs",
    "your/mime-type":   "your.routing.key",   # add this
}
```

### Step 3 — Register the routing key in `worker.py`

```python
from modules import my_task

ROUTING_MAP = {
    "process.json": handling_json,
    "process.log":  handling_log,
    "your.routing.key": my_task,              # add this
}
```

### Step 4 — Add a queue for the new worker

In `start.sh`, export the queue name and launch a worker process:

```bash
export TASK_QUEUE="your_queue_name"
venv/bin/python worker.py &
```

---

## 11. Shutting Down

### Stop the native workers

Press `CTRL+C` in the terminal running `start.sh`.

### Stop the Docker control plane

```bash
docker-compose down
```

To also wipe persisted broker and cache data:

```bash
docker-compose down -v
```

### RabbitMQ Management UI

Inspect queues, bindings, and message rates while the platform is running:

```
http://localhost:15672
```

Log in with your `RABBITMQ_USERNAME` and `RABBITMQ_PASSWORD` from `.env`.

---

## 12. v1.0 Architecture Highlights

Alexandria is designed from the ground up to support large file payloads and long-running, resource-intensive tasks without bottlenecking the API or dropping broker connections.

---

### Hybrid Docker / Native Architecture

The control plane (FastAPI, Redis, RabbitMQ) runs in isolated Docker containers while Python workers execute natively on the host machine. This gives you the reliability and portability of container orchestration for infrastructure services, while allowing workers direct access to native OS binaries and hardware that cannot run inside a container.

---

### Multipart Form-Data Ingestion

The API gateway processes `multipart/form-data` exclusively, standardized on the `upload_file` field. This lets the API ingest large files — blueprints, assets, binaries — without loading the entire payload into application memory.

---

### The Claim Check Pattern

Files at or above the 1 MB threshold are never passed through the message broker. Instead, Alexandria saves the file to organized local storage under `temp/alexandria_assets/` (in categorized subfolders: `jsons/`, `apks/`, `logs/`) and publishes only a lightweight reference ticket to RabbitMQ. The worker resolves the file from disk using the path in the ticket. This keeps broker memory usage flat regardless of payload size.

---

### Extended Heartbeat Tolerance

Pika connection parameters are configured with `heartbeat=3600`, keeping the RabbitMQ connection alive through tasks that run for 10+ minutes. Without this, the broker closes idle connections mid-task and raises a `StreamLostError`, killing the worker process.

---

### Automated Janitor (Generic `finally` Block)

Every worker features a queue-agnostic `finally` block that runs after every job regardless of outcome. Modules inject the path of any file they generate into `message["file_location"]` before returning. The `finally` block reads that path and deletes the file from disk automatically, preventing accumulation of temporary build artifacts and log files over time.

---

### Environment Variable Fallbacks

`os.getenv()` calls include default fallback values throughout the codebase. This ensures workers can boot and connect to the broker cleanly even if specific variables are absent from the `.env` file, avoiding silent crashes on startup due to missing configuration.
