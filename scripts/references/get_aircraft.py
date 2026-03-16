import sys
import os
import time
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