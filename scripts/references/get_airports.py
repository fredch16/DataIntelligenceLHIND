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

logger = logging.getLogger("get_airports")

def run_airport_ingestion():
	start_time = time.time()
	client = LufthansaClient(scope_name="lufthansa_scope")
	logger.info("Starting Airport Reference Ingestion")
	client.ingest_paginated(
		endpoint="/v1/references/airports",
		resource_key="AirportResource",
		category="ref",
		entity_type="airports"
	)
	duration = (time.time() - start_time) / 60
	logger.info(f"Finished! Time: {duration:.2f} minutes")

if __name__ == "__main__":
	run_airport_ingestion()