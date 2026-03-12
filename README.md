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
│   └── references/
│       ├── get_aircraft.py
│       ├── get_airlines.py
│       ├── get_airports.py
│       ├── get_cities.py
│       └── get_countries.py
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
To execute a specific ingestion task, run the corresponding script. The `sys.path` logic ensures all modules are resolved regardless of execution depth.

```bash
python scripts/operations/get_flights_daily.py
```

---

## 📊 Data Governance
All extracted data is persisted as raw JSON in the Bronze Layer of the Lakehouse.

**Volume Path:** `/Volumes/main/lufthansa/landing_zone/`

**Subfolders:**

- `/operation/`: Daily flight status by route.
- `/reference/`: Monthly snapshots of master data.

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

