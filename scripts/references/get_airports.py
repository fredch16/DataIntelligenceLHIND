import sys
import os
import time
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from utils.helpers import LufthansaClient

def run_airport_ingestion():
	start_time = time.time()
	client = LufthansaClient(scope_name="lufthansa_scope")
	print(f"🚀 Starting Airport Reference Ingestion")
	client.ingest_paginated(
		endpoint="/v1/references/airports",
		resource_key="AirportResource",
		category="ref",
		entity_type="airports"
	)
	duration = (time.time() - start_time) / 60
	print(f"🏁 Finished! Time: {duration:.2f} minutes\n")

if __name__ == "__main__":
	run_airport_ingestion()