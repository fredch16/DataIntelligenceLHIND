# Lufthansa Data Ingestion Engine

This repository contains a modular, professional-grade data ingestion pipeline designed to extract operational and reference data from the Lufthansa OpenAPI. The system is engineered to handle large-scale JSON data extraction and store it in raw format within Unity Catalog Volumes on the Databricks Lakehouse.

---

## 🏗️ Architecture Overview

The codebase follows a **Modular Design Pattern**. By separating the core logic (authentication, retrying, pagination) from the execution scripts, the system ensures maintainability and scalability.

### Core Components

- **`utils/helpers.py`**: Contains the `LufthansaClient` class. This is the engine of the project, managing:
  - **Environment‑Aware Configuration**: Automatically switches between local `config.yaml` and Databricks Secret Scopes.
  - **Universal Ingestion**: Support for both single‑endpoint calls and paginated reference data.
  - **Robustness**: Exponential backoff retry logic for 429 (Rate Limit) and 5xx (Server) errors.

- **`scripts/operations/`**: Focused on time‑sensitive, daily operational flight data.
- **`scripts/references/`**: Focused on stable, monthly reference data (Airports, Aircraft, etc.).

---

## 📂 Project Structure

```
DataIntelligenceLHIND/
├── config.yaml             # Local development credentials (git-ignored)
├── utils/
│   ├── __init__.py         # Package identifier
│   └── helpers.py          # Centralized LufthansaClient logic
├── scripts/
│   ├── operations/
│   │   └── get_flights_daily.py
│   ├── references/
│   │   ├── ingest_all_references.py     # Consolidated reference ingestion (toggleable)
│   │   ├── get_aircraft.py
│   │   ├── get_airlines.py
│   │   ├── get_airports.py
│   │   ├── get_cities.py
│   │   └── get_countries.py
│   └── singular/
│       └── get_flight_by_route_on_day.py
└── README.md
```

---

## 🛠️ Setup & Execution

### 🔐 Prerequisites & Secrets
The system is designed to run seamlessly in two environments:

**Local Environment:** Ensure a `config.yaml` exists in the root directory:

```yaml
password: "your_proxy_credential"
```

**Databricks Environment:** Ensure a Secret Scope named `lufthansa_scope` is configured with the key `client_secret`.

### 🚀 Running the Pipeline

To execute ingestion tasks, run the corresponding scripts:

**Daily Flight Ingestion:**
```bash
python scripts/operations/get_flights_daily.py
```

**Reference Data Ingestion (Consolidated):**
```bash
python scripts/references/ingest_all_references.py
```
Toggle specific endpoints by editing `REFERENCES_CONFIG` in `ingest_all_references.py` and setting `enabled: True/False`.

**Individual Reference Scripts:**
Each reference data type can still be ingested independently:
```bash
python scripts/references/get_airlines.py
python scripts/references/get_airports.py
# ... etc
```

**Ad-hoc Flight Lookup:**
```bash
python scripts/singular/get_flight_by_route_on_day.py
```
Edit `departure_airport`, `arrival_airport`, and `date` parameters in the script as needed.

---

## 📊 Data Governance & Storage

All extracted data is persisted as raw JSON in the Bronze Layer, preserving complete API responses with metadata and link blocks.

**Base Volume Paths:**
- **Databricks:** `/Volumes/main/lufthansa/landing_zone/`
- **Local Development:** `outputs/`

### Hybrid Partitioning Strategy

Data is organized by category and type with intelligent partitioning:

**Reference Data** (Monthly Partitioning):
```
{base_volume}/ref/{entity_type}/{YYYY-MM}/
├── airlines/2026-03/
│   ├── 20260316_airlines_offset0.json
│   ├── 20260316_airlines_offset100.json
│   └── ...
├── airports/2026-03/
├── aircraft/2026-03/
├── cities/2026-03/
└── countries/2026-03/
```

**Operational Data** (Daily Partitioning):
```
{base_volume}/ops/flights/{YYYY-MM-DD}/
├── 2026-03-16/
│   ├── 20260316_flights_LHR_STR.json
│   ├── 20260316_flights_STR_LHR.json
│   ├── 20260316_flights_FRA_JFK.json
│   └── ...
```

**Filename Convention:** `YYYYMMDD_{entity_type}_[offset{N}|route].json`

---

## ✈️ Operational Coverage
The pipeline tracks high‑frequency routes including:

- **LHR ↔ STR** (Regional connectivity)
- **FRA ↔ JFK** (Transatlantic hub)
- **FRA ↔ SIN** (South East Asia hub)
- **FRA ↔ MUC** (Domestic backbone)
- **FRA ↔ DXB / MUC ↔ DEL** (Major international corridors)

---

## ✅ Technical Achievements

- **Rate Limit Management:** Implemented precise `time.sleep` intervals to respect the 3 requests per second limit.
- **Pagination Logic:** Custom logic to handle Lufthansa’s unique “link‑based” pagination and “ghost record” termination.
- **Data Standardization:** All JSON responses are re‑wrapped into a consistent schema to simplify downstream Spark transformations in the Silver layer.
- **Automated Triggers:** Configured Databricks Jobs with Cron schedules for daily operational and monthly reference refreshes.

