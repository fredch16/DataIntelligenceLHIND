import sys
import os
import time
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from utils.helpers import LufthansaClient

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
	
	print(f"Starting Reference Data Ingestion")
	print(f"📋 Endpoints: {', '.join(enabled_refs.keys())}\n")
	
	for entity_type, config in enabled_refs.items():
		print(f"\n{'='*50}")
		print(f"Ingesting: {entity_type.upper()}")
		print(f"{'='*50}")
		client.ingest_paginated(
			endpoint=config["endpoint"],
			resource_key=config["resource_key"],
			category="ref",
			entity_type=entity_type
		)
	
	duration = (time.time() - start_time) / 60
	print(f"\n{'='*50}")
	print(f"All ingestions complete! Time: {duration:.2f} minutes\n")

if __name__ == "__main__":
	ingest_all_references()
