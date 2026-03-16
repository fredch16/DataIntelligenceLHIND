import sys
import os
import time
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from utils.helpers import LufthansaClient

def run_countries_ingestion():
	start_time = time.time()
	client = LufthansaClient(scope_name="lufthansa_scope")
	print(f"🚀 Starting Countries Reference Ingestion")
	client.ingest_paginated(
		endpoint="/v1/references/countries",
		resource_key="CountryResource",
		category="ref",
		entity_type="countries"
	)
	duration = (time.time() - start_time) / 60
	print(f"🏁 Finished! Time: {duration:.2f} minutes\n")

if __name__ == "__main__":
	run_countries_ingestion()