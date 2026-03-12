import sys
import os
import time
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from utils.helpers import LufthansaClient
from datetime import datetime

def run_countries_ingestion():
	start_time = time.time()
	client = LufthansaClient(scope_name="lufthansa_scope")
	today = datetime.now().strftime('%Y-%m-%d')
	print(f"🚀 Starting Countries Reference Ingestion")
	
	data = client.fetch_all_pages(
		endpoint="/v1/references/countries",
		resource_key="CountryResource",
		nested_keys=["Countries", "Country"]
	)
	
	if data:
		file_name = f"ref_countries_{today}.json"
		client.save_json(data, "reference", file_name)
	
	duration = (time.time() - start_time) / 60
	print(f"🏁 Finished! Time: {duration:.2f} minutes\n")

if __name__ == "__main__":
	run_countries_ingestion()