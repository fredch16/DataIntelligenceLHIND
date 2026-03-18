import sys
import os
import time
import logging
from datetime import datetime

try:
	base_dir = os.path.dirname(os.path.abspath(__file__))
	project_root = os.path.abspath(os.path.join(base_dir, '../..'))
	if project_root not in sys.path:
		sys.path.append(project_root)
except NameError:
	project_root = os.getcwd() 
	if project_root not in sys.path:
		sys.path.append(project_root)

# from utils.helpers import MockLufthansaClient as LufthansaClient in case of API being down
from utils.helpers import LufthansaClient

logger = logging.getLogger("get_flights_daily")

def run_flight_ingestion():
	start_time = time.time()
	client = LufthansaClient(scope_name="lufthansa_app_own")
	today = datetime.now().strftime('%Y-%m-%d')
	today_str = datetime.now().strftime('%Y%m%d')
	service_type = "passenger"
	routes = [
		("LHR", "STR"), ("STR", "LHR"),
		("FRA", "JFK"), ("JFK", "FRA"),
		("FRA", "LHR"), ("LHR", "FRA"),
		("FRA", "MUC"), ("MUC", "FRA"),
		("FRA", "SIN"), ("SIN", "FRA"),
	]
	logger.info(f"Starting Daily Flight Ingestion for: {today}")
	total_ingested = 0
	for origin, destination in routes:
		logger.info(f"Processing: {origin} -> {destination}")
		endpoint = f"/v1/operations/flightstatus/route/{origin}/{destination}/{today}?serviceType={service_type}&limit=100"
		data = client.fetch_with_retry(endpoint)
		if not data:
			logger.warning(f"Skipping {origin}->{destination} due to fetch error.")
			continue
		res_key = 'FlightStatusResource'
		records = data.get(res_key, {}).get('Flights', {}).get('Flight', [])
		if isinstance(records, dict):
			records = [records]
		if not records:
			logger.warning(f"No flights found for {origin} -> {destination} on {today}.")
		else:
			flight_count = len(records)
			total_ingested += flight_count
			filename = f"{today_str}_flights_{origin}_{destination}.json"
			client.save_json(data, category="ops", entity_type="flights", filename=filename)
			logger.info(f"Saved {flight_count} flights to {filename}")
	end_time = time.time()
	duration_mins = (end_time - start_time) / 60
	logger.info(f"Finished! Total flights ingested: {total_ingested}")
	logger.info(f"Total Ingestion Time: {duration_mins:.2f} minutes")

if __name__ == "__main__":
	run_flight_ingestion()