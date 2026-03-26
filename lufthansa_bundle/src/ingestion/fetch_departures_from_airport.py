import time
import logging
from datetime import datetime
import sys
import os

# --- PATH BOILERPLATE ---
if "__file__" in globals():
	current_dir = os.path.dirname(os.path.abspath(__file__))
else:
	current_dir = os.getcwd()

src_root = os.path.abspath(os.path.join(current_dir, '..'))
if src_root not in sys.path:
	sys.path.append(src_root)

from utils.helpers import LufthansaClient

# --- CONFIGURATION ---
logger = logging.getLogger("ingest_hub_flights")
logging.basicConfig(level=logging.INFO)

# We target the 'Power 5' hubs for maximum volume
HUBS = ["FRA", "MUC", "ZRH", "VIE", "BRU"]

def ingest_hub_flights():
	"""
	Sweeps the major Lufthansa hubs for all departing flights starting from 'now'.
	Pagination is handled automatically by the LufthansaClient.
	"""
	start_time = time.time()
	client = LufthansaClient(scope_name="lufthansa")
	now_str = datetime.now().strftime("%Y-%m-%dT%H:%M")
	logger.info(f"🚀 Starting Hub Flight Ingestion at {now_str}")
	logger.info(f"Targeting Hubs: {', '.join(HUBS)}")
	for hub in HUBS:
		logger.info(f"--- Processing Hub: {hub} ---")
		# Example: /v1/operations/flightstatus/departures/FRA/2026-03-20T16:45
		hub_endpoint = f"/v1/operations/flightstatus/departures/{hub}/{now_str}"
		try:
			client.ingest_paginated(
				endpoint=hub_endpoint,
				resource_key="FlightStatusResource",
				category="ops",        # Keeps it in the 'ops' folder in your Volume
				entity_type=f"flights",  # Targets the 'ops_flights' bronze table
				limit=50
			)
		except Exception as e:
			logger.error(f"❌ Failed to ingest hub {hub}: {str(e)}")
			continue # Move to the next hub even if one fails
	duration = (time.time() - start_time) / 60
	logger.info(f"✅ Hub ingestion complete! Total time: {duration:.2f} minutes")

if __name__ == "__main__":
	ingest_hub_flights()