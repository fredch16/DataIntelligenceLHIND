import time
import logging
from datetime import datetime
import sys
import os
import inspect

# --- DATABRICKS-SAFE PATH BOILERPLATE ---
# 1. Try normal __file__ first (works locally and in standard Python jobs)
if "__file__" in globals():
    current_file = os.path.abspath(__file__)
else:
    # 2. Databricks Fallback: Extract the filename straight from the compiler frame
    current_file = os.path.abspath(inspect.getfile(inspect.currentframe()))

current_dir = os.path.dirname(current_file)

# 3. Navigate up one directory from 'ingestion' to 'src'
src_root = os.path.abspath(os.path.join(current_dir, '..'))

# 4. Add 'src' to the system path so Python can find 'utils'
if src_root not in sys.path:
    sys.path.append(src_root)

from utils.helpers import LufthansaClient
logger = logging.getLogger("ingest_all_references")

# Toggleable reference data configuration
# Comment out any endpoint to skip it during ingestion
REFERENCES_CONFIG = {
	"airlines": {
		"endpoint": "/v1/references/airlines",
		"resource_key": "AirlineResource",
		"enabled": False
	},
	"countries": {
		"endpoint": "/v1/references/countries",
		"resource_key": "CountryResource",
		"enabled": False
	},
	"airports": {
		"endpoint": "/v1/references/airports",
		"resource_key": "AirportResource",
		"enabled": True
	},
	"aircraft": {
		"endpoint": "/v1/references/aircraft",
		"resource_key": "AircraftResource",
		"enabled": False
	},
	"cities": {
		"endpoint": "/v1/references/cities",
		"resource_key": "CityResource",
		"enabled": False
	}
}

def ingest_all_references():
	start_time = time.time()
	client = LufthansaClient(scope_name="lufthansa")
	enabled_refs = {k: v for k, v in REFERENCES_CONFIG.items() if v["enabled"]}
	
	logger.info(f"Starting Reference Data Ingestion")
	logger.info(f"Enabled Endpoints: {', '.join(enabled_refs.keys())}")
	
	for entity_type, config in enabled_refs.items():
		logger.info(f"Processing: {entity_type.upper()}")
		client.ingest_paginated(
			endpoint=config["endpoint"],
			resource_key=config["resource_key"],
			category="ref",
			entity_type=entity_type,
			limit=100
		)
	
	duration = (time.time() - start_time) / 60
	logger.info(f"All ingestions complete! Time: {duration:.2f} minutes")

if __name__ == "__main__":
	ingest_all_references()
