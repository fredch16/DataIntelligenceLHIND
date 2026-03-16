import sys
import os
import time
import logging
from datetime import datetime

try:
	# Local Mode: __file__ exists
	base_dir = os.path.dirname(os.path.abspath(__file__))
	project_root = os.path.abspath(os.path.join(base_dir, '../..'))
	if project_root not in sys.path:
		sys.path.append(project_root)
except NameError:
	# Databricks Mode: __file__ is not defined
	# Databricks usually adds the current repo to the path automatically,
	# but we can explicitly add the workspace root if needed:
	project_root = os.getcwd() 
	if project_root not in sys.path:
		sys.path.append(project_root)

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
		"enabled": False
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
	client = LufthansaClient(scope_name="lufthansa_scope")
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
