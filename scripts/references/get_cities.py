import sys
import os
import time
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from utils.helpers import LufthansaClient

def run_cities_ingestion():
	start_time = time.time()
	client = LufthansaClient(scope_name="lufthansa_scope")
	print(f"🚀 Starting Cities Reference Ingestion")
	client.ingest_paginated(
		endpoint="/v1/references/cities",
		resource_key="CityResource",
		category="ref",
		entity_type="cities"
	)
	duration = (time.time() - start_time) / 60
	print(f"🏁 Finished! Time: {duration:.2f} minutes\n")

if __name__ == "__main__":
	run_cities_ingestion()