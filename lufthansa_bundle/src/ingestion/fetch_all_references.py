import time
import logging
from datetime import datetime
import sys
import os

# Get the absolute path of the directory containing THIS script (src/ingestion)
current_dir = os.getcwd()

# Go up one level to reach 'src'
# This ensures that 'import utils.helpers' will work correctly
src_root = os.path.abspath(os.path.join(current_dir, '..'))

if src_root not in sys.path:
    sys.path.append(src_root)

# Now you can import your utils
from utils.helpers import LufthansaClient

logger = logging.getLogger("ingest_all_references")

# Toggleable reference data configuration
# Comment out any endpoint to skip it during ingestion
REFERENCES_CONFIG = {
	"airlines": {
		"endpoint": "/v1/references/airlines",
		"resource_key": "AirlineResource",
		"enabled": True
	},
	"countries": {
		"endpoint": "/v1/references/countries",
		"resource_key": "CountryResource",
		"enabled": True
	},
	"airports": {
		"endpoint": "/v1/references/airports",
		"resource_key": "AirportResource",
		"enabled": True
	},
	"aircraft": {
		"endpoint": "/v1/references/aircraft",
		"resource_key": "AircraftResource",
		"enabled": True
	},
	"cities": {
		"endpoint": "/v1/references/cities",
		"resource_key": "CityResource",
		"enabled": True
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
			entity_type=entity_type
		)
	
	duration = (time.time() - start_time) / 60
	logger.info(f"All ingestions complete! Time: {duration:.2f} minutes")

if __name__ == "__main__":
	ingest_all_references()
