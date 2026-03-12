import sys
import os
import time
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from utils.helpers import LufthansaClient
from datetime import datetime

def run_airport_ingestion():
	start_time = time.time()
	client = LufthansaClient(scope_name="lufthansa_scope")
	today = datetime.now().strftime('%Y-%m-%d')
	print(f"🚀 Starting Airport Reference Ingestion")
	
	data = client.fetch_all_pages(
		endpoint="/v1/references/airports",
		resource_key="AirportResource",
		nested_keys=["Airports", "Airport"]
	)
	
	if data:
		file_name = f"ref_airports_{today}.json"
		client.save_json(data, "reference", file_name)
	
	duration = (time.time() - start_time) / 60
	print(f"🏁 Finished! Time: {duration:.2f} minutes\n")

if __name__ == "__main__":
	run_airport_ingestion()