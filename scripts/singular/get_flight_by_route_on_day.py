import sys
import os
import time
from datetime import datetime

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from utils.helpers import LufthansaClient

# NOTE: This script is intended for ad-hoc lookups and is not part of the daily ingestion schedule.
# It uses the same configuration resolution logic as the other ingestion scripts.

def run_single_route_lookup(departure_airport: str, arrival_airport: str, date: str):
	client = LufthansaClient(scope_name="lufthansa_scope")

	route_for_output = f"{departure_airport}-{arrival_airport}"
	endpoint = f"/v1/operations/flightstatus/route/{departure_airport}/{arrival_airport}/{date}"

	print(f"🚀 Fetching flight status for {departure_airport} → {arrival_airport} on {date}")
	data = client.fetch_with_retry(endpoint)
	if not data:
		print("⚠️ Request failed. See previous logs for details.")
		return

	file_name = f"get_flight_on_route_{route_for_output}_{date}_output.json"
	client.save_json(data, "operation", file_name)

if __name__ == "__main__":
	# Change these values as needed
	departure_airport = "FRA"
	arrival_airport = "JFK"
	date = datetime.now().strftime("%Y-%m-%d")

	run_single_route_lookup(departure_airport, arrival_airport, date)
