import sys
import os
import time
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from utils.helpers import LufthansaClient
from datetime import datetime

def run_airline_ingestion():
	start_time = time.time()
	client = LufthansaClient(scope_name="lufthansa_scope")
	today = datetime.now().strftime('%Y-%m-%d')
	print(f"🚀 Starting Airline Reference Ingestion")
	
	data = client.fetch_all_pages(
		endpoint="/v1/references/airlines",
		resource_key="AirlineResource",
		nested_keys=["Airlines", "Airline"]
	)
	
	if data:
		file_name = f"ref_airlines_{today}.json"
		client.save_json(data, "reference", file_name)
	
	duration = (time.time() - start_time) / 60
	print(f"🏁 Finished! Time: {duration:.2f} minutes\n")

if __name__ == "__main__":
	run_airline_ingestion()