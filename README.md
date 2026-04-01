# Lufthansa Data Intelligence Pipeline

A production-grade data pipeline that extracts operational and reference data from the Lufthansa OpenAPI and processes it through a full Bronze → Silver → Gold medallion architecture on Databricks, managed with Terraform.

---

## Architecture Overview

The system is split into two distinct concerns: **ingestion** (Python scripts that call the API and land raw JSON) and **transformation** (Spark Declarative Pipelines that process data through the medallion layers).

```
Lufthansa API
      │
      ▼
┌─────────────────┐
│   Ingestion     │  src/ingestion/  — Python scripts, LufthansaClient
│  (Landing Zone) │  Unity Catalog Volume: /Volumes/main/lufthansa/landing_zone/
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│     Bronze      │  src/bronze/     — Auto Loader (cloudFiles) → DLT tables
│  (Raw JSON)     │  Catalog: main.lufthansa_bronze
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│     Silver      │  src/silver/     — Flatten, type-cast, deduplicate, quarantine
│  (Typed/Clean)  │  Catalog: main.lufthansa_silver
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│      Gold       │  src/gold/       — Enriched fact table (joined dimensions)
│  (Analytics)    │  Catalog: main.lufthansa_gold
└─────────────────┘
```

Infrastructure is fully managed by **Terraform** (`terraform/`), which provisions Databricks Jobs and Spark Declarative Pipelines.

---

## Project Structure

```
DataIntelligenceLHIND/
├── config.yaml                          # Local dev credentials (git-ignored)
├── pyproject.toml                       # Python project config (uv)
├── src/
│   ├── ingestion/
│   │   ├── fetch_departures_from_airport.py  # Hub departure sweeper (primary ops script)
│   │   ├── fetch_all_references.py           # Toggleable reference data ingestion
│   │   └── fetch_flights_on_route.py         # Ad-hoc route-based flight lookup
│   ├── bronze/
│   │   ├── ops_ingestion.py             # Auto Loader → ops_flights DLT table
│   │   └── ref_ingestion.py             # Auto Loader → ref_* DLT tables (5 types)
│   ├── silver/
│   │   ├── silver_operations.py         # Flatten + deduplicate flights; quarantine
│   │   └── silver_references.py         # Flatten + deduplicate reference data; quarantine
│   ├── gold/
│   │   └── gold_flight_analytics.py     # Enriched fact table (flights + dimensions)
│   └── utils/
│       └── helpers.py                   # LufthansaClient — auth, pagination, retry, save
└── terraform/
    ├── main.tf                          # Workspace file uploads
    ├── jobs.tf                          # Databricks Job definitions
    ├── pipelines.tf                     # Spark Declarative Pipeline definitions
    ├── provider.tf
    ├── variables.tf
    └── outputs.tf
```

---

## Setup

### Prerequisites & Secrets

**Local Development:** Create a `config.yaml` in the project root:

```yaml
client_id: "your_client_id"
client_secret: "your_client_secret"
```

**Databricks:** Configure a Secret Scope named `lufthansa` with keys `client_id` and `client_secret`.

### Running Ingestion Scripts Locally

**Hub departure sweep** (primary operational ingestion):
```bash
uv run src/ingestion/fetch_departures_from_airport.py
```
Sweeps the five Lufthansa hubs (FRA, MUC, ZRH, VIE, BRU) for all departing flights from the current time.

**Reference data ingestion:**
```bash
uv run src/ingestion/fetch_all_references.py
```
Toggle individual endpoints by setting `enabled: True/False` in the `REFERENCES_CONFIG` dict within the script.

**Ad-hoc route lookup:**
```bash
uv run src/ingestion/fetch_flights_on_route.py
```
Edit `routes` in the script to specify origin/destination pairs and date.

---

## Personal deployment

1. Configure Terraform to use your Databricks workspace and personal access token.
   ```bash
   export TF_VAR_databricks_host="https://<your-workspace>.cloud.databricks.com"
   export TF_VAR_databricks_token="<your-databricks-personal-access-token>"
   export TF_VAR_catalog="main"    # optional, default is main
   ```

2. Initialize and apply Terraform from the `terraform/` directory.
   ```bash
   cd terraform
   terraform init
   terraform apply
   ```

3. Terraform will provision the Databricks jobs, workspace files, and Spark Declarative Pipelines for the Lufthansa pipeline.
   - `Ingest reference data (Monthly)`
   - `Utility Operational Ingestion`
   - `Ingest Operational Data (Daily)`

4. If you want to trigger the first run manually instead of waiting for schedule, use the Databricks CLI.
   - List jobs and find the job ID:
     ```bash
     databricks jobs list
     ```
   - Trigger the `Utility Operational Ingestion` job once:
     ```bash
     databricks jobs run-now --job-id <JOB_ID>
     ```

   Alternatively, trigger the daily pipeline job by using its job ID instead.

---

## Data Storage

All raw API responses are stored as JSON in Unity Catalog Volumes with an ingestion envelope wrapping the original payload.

**Base Volume:** `/Volumes/main/lufthansa/landing_zone/`

### Partitioning Strategy

**Operational data** — daily partitions:
```
landing_zone/ops/flights/{YYYY-MM-DD}/
└── flights_{HUB}_{YYYYMMDD}_{HHmmss}_offset{N}.json
```

**Reference data** — monthly partitions:
```
landing_zone/ref/{entity_type}/{YYYY-MM}/
└── {entity_type}_{YYYYMMDD}_offset{N}.json
```

### Ingestion Envelope

Every saved file is wrapped with metadata for traceability:
```json
{
  "ingestion_metadata": {
    "ingested_at": "...",
    "batch_id": "...",
    "category": "ops|ref",
    "entity": "...",
    "script_name": "helpers.py",
    "offset": 0,
    "limit": 50,
    "endpoint": "..."
  },
  "payload": { ... }
}
```

---

## Medallion Pipeline

Pipelines are defined using **Spark Declarative Pipelines (SDP)** and orchestrated via Databricks Jobs.

### Bronze (`main.lufthansa_bronze`)

Auto Loader streams new JSON files from the landing zone into Delta tables as they arrive.

| Table | Source |
|---|---|
| `ops_flights` | `landing_zone/ops/flights/` |
| `ref_airports_bronze` | `landing_zone/ref/airports/` |
| `ref_airlines_bronze` | `landing_zone/ref/airlines/` |
| `ref_aircraft_bronze` | `landing_zone/ref/aircraft/` |
| `ref_cities_bronze` | `landing_zone/ref/cities/` |
| `ref_countries_bronze` | `landing_zone/ref/countries/` |

### Silver (`main.lufthansa_silver`)

Explodes nested JSON, casts types, deduplicates on primary key (latest ingestion wins), and routes bad records to quarantine tables.

**Operations pipeline (`silver_operations.py`):**
- `flights_staged` — staging view that explodes `FlightStatusResource.Flights.Flight` and flattens all fields (carrier, route, status, UTC/local timestamps)
- `ops_flights_silver` — deduplicated on `flight_id` (airline + number + departure date); drops records missing flight number or route
- `ops_flights_quarantine` — records that failed quality checks

**References pipeline (`silver_references.py`):**

Each reference type follows the same pattern (staged view → silver table → quarantine):

| Silver Table | Primary Key | Quality Rules |
|---|---|---|
| `ref_airports_silver` | `airport_code` | 3-char IATA code, non-null name |
| `ref_airlines_silver` | `airline_id` | non-null ID and name |
| `ref_aircraft_silver` | `aircraft_code` | non-null code and name |
| `ref_cities_silver` | `city_code` | non-null code and name |
| `ref_countries_silver` | `country_code` | non-null code and name |

### Gold (`main.lufthansa_gold`)

`gold_fact_flights_master` — enriched fact table joining flights with all reference dimensions:

| Dimension | Join Key |
|---|---|
| Airlines | `op_airline_id → airline_id` |
| Origin Airport | `origin_iata → airport_code` |
| Destination Airport | `dest_iata → airport_code` |
| Aircraft | `aircraft_code → aircraft_code` |
| Countries | `dst.country_code → country_code` |

Adds derived fields: `dep_hour`, `operational_status` (On Time / Delayed / In Flight / Cancelled).

---

## Scheduled Jobs

Three Databricks Jobs are provisioned by Terraform:

### Job 1 — Ingest Reference Data (Monthly)
- **Schedule:** 01:55 UTC on the 3rd of every month
- **Task:** `fetch_all_references.py`

### Job 2 — Utility Operational Ingestion (Ad-hoc)
- **Schedule:** None — triggered manually as needed
- **Task:** `fetch_departures_from_airport.py`

### Job 3 — Ingest Operational Data (Daily)
- **Schedule:** Every 4 hours UTC, skipping midnight — 04:00, 08:00, 12:00, 16:00, 20:00
- **Tasks (sequential):**
  1. `fetch_api_data` — calls `fetch_departures_from_airport.py`
  2. `refresh_bronze` — triggers the Bronze SDP pipeline
  3. `refresh_silver` — triggers the Silver SDP pipeline
  4. `refresh_gold` — triggers the Gold SDP pipeline

---

## Key Implementation Details

### Pagination (`helpers.py`)
`ingest_paginated` fetches pages at a fixed `limit` and advances the offset. The primary stopping condition is `TotalCount` from the API's `Meta` block — this correctly handles cases where the total record count is an exact multiple of the page size. Falls back to `record_count < limit` for endpoints that do not return `TotalCount`.

### Poison Pill Handling
When a paginated request returns HTTP 404 mid-sweep, a binary search recursively halves the batch to isolate the single bad offset, saves all clean sub-batches, then resumes ingestion past the problematic record.

### Retry Logic
`fetch_with_retry` applies exponential backoff on HTTP 429 and 5xx responses (up to 5 retries). HTTP 404 is treated as a signal (poison pill), not a transient error.

### Single-Record Normalization
The Lufthansa API returns a single record as a `dict` instead of a one-element `list`. `_normalize_single_objects_to_lists` corrects this before saving, ensuring Auto Loader always infers a consistent `ARRAY<STRUCT>` schema.

### Environment Detection
`LufthansaClient` automatically reads credentials from `config.yaml` locally and from Databricks Secret Scopes (`lufthansa`) when running in a Databricks Runtime environment.
