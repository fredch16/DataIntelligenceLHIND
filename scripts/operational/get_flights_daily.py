import sys
import os

# Add the project root to the Python path so it can find the 'utils' folder
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

# NOW you can import it
from utils.helpers import LufthansaClient
from datetime import datetime
import time

def run_flight_ingestion():
	start_time = time.time()
	
	# 1. Initialize the Modular Client
	# It automatically handles Secrets (Local vs Databricks) and Paths
	client = LufthansaClient(scope_name="lufthansa_scope")
	
	# 2. Configuration
	today = datetime.now().strftime('%Y-%m-%d')
	service_type = "passenger"
	
	# The Arkadia Five (plus returns)
	routes = [
		("LHR", "STR"), ("STR", "LHR"),
		("FRA", "JFK"), ("JFK", "FRA"),
		("FRA", "LHR"), ("LHR", "FRA"),
		("FRA", "MUC"), ("MUC", "FRA"),
		("FRA", "SIN"), ("SIN", "FRA"),
		("FRA", "DXB"), ("DXB", "FRA")
	]
	
	print(f"🚀 Starting Daily Flight Ingestion for: {today}")
	print("-" * 50)

	total_ingested = 0

	# 3. Execution Loop
	for origin, destination in routes:
		print(f"✈️ Processing: {origin} -> {destination}")
		
		# Build Endpoint
		endpoint = f"/v1/operations/flightstatus/route/{origin}/{destination}/{today}?serviceType={service_type}&limit=100"
		
		# Use the Modular Helper for fetching (Handles Retries & Status Codes)
		data = client.fetch_with_retry(endpoint)
		
		if not data:
			print(f"⚠️ Skipping {origin}->{destination} due to fetch error.")
			continue

		# 4. Data Parsing & Standardization
		res_key = 'FlightStatusResource'
		records = data.get(res_key, {}).get('Flights', {}).get('Flight', [])

		# Fix the "Single Flight" dictionary bug
		if isinstance(records, dict):
			records = [records]

		if not records:
			print(f"⚠️ No flights found for {origin} -> {destination} on {today}.")
		else:
			flight_count = len(records)
			total_ingested += flight_count
			
			# Wrap for consistent Bronze schema
			final_output = {
				res_key: {
					"Flights": {
						"Flight": records
					}
				}
			}

			# 5. Save using the Modular Helper (Handles Volume vs Local paths)
			file_name = f"ops_flights_{origin}_{destination}_{today}.json"
			client.save_json(final_output, "operation", file_name)
			
			print(f"✅ Saved |{flight_count}| flights to {file_name}")

	# 6. Final Summary
	end_time = time.time()
	duration_mins = (end_time - start_time) / 60
	print("-" * 50)
	print(f"🏁 Finished! Total flights ingested: {total_ingested}")
	print(f"⏱️ Total Ingestion Time: {duration_mins:.2f} minutes\n")

if __name__ == "__main__":
	run_flight_ingestion()