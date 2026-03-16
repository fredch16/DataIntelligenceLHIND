import sys
import os
import time
sys.path.append(os.path.abspath(os.path.join(os.getcwd(), '../..')))
from utils.helpers import LufthansaClient

def run_aircraft_ingestion():
	start_time = time.time()
	client = LufthansaClient(scope_name="lufthansa_scope")
	print(f"🚀 Starting Aircraft Reference Ingestion")
	client.ingest_paginated(
		endpoint="/v1/references/aircraft",
		resource_key="AircraftResource",
		category="ref",
		entity_type="aircraft"
	)
	duration = (time.time() - start_time) / 60
	print(f"🏁 Finished! Time: {duration:.2f} minutes\n")

if __name__ == "__main__":
	run_aircraft_ingestion()