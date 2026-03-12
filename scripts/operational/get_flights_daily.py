import requests
import json
import os
import yaml
import time
from pathlib import Path
from datetime import datetime


# Databricks Behaviour
if "DATABRICKS_RUNTIME_VERSION" in os.environ:
	from pyspark.sql import SparkSession
	from pyspark.dbutils import DBUtils

	spark = SparkSession.builder.getOrCreate()
	dbutils = DBUtils(spark)
	proxy_pass = dbutils.secrets.get(scope="lufthansa_scope", key="client_secret")
	HEADERS = {"password": proxy_pass}

	# File storage
	ROOT_PATH = "/Volumes/main/lufthansa/landing_zone"
	BASE_VOLUME = f"{ROOT_PATH}/operation"
	os.makedirs(BASE_VOLUME, exist_ok=True)

else: # Local Testing Behaviour
	# 1. Get the directory where THIS script is saved
	script_dir = os.path.dirname(os.path.abspath(__file__))
	
	# 2. Go to the root of your project (usually one or two levels up from 'scripts/operations')
	# This ensures it always stays inside 'DataIntelligenceLHIND'
	project_root = os.path.dirname(os.path.dirname(script_dir))
	
	config_path = os.path.join(project_root, "config.yaml")
	
	with open(config_path, 'r') as f:
		config = yaml.safe_load(f)
	HEADERS = {"password": config["password"]}

	# 3. Create 'outputs' inside your project folder
	ROOT_PATH = os.path.join(project_root, "outputs")
	BASE_VOLUME = os.path.join(ROOT_PATH, "operation")
	
	# This should now work without Permission Errors
	os.makedirs(BASE_VOLUME, exist_ok=True)
	print(f"\n💻 Local path set to: {BASE_VOLUME}\n")


### ACTUAL SCRIPT STARTS HERE

start_time = time.time()
BASE_URL = "https://lh-proxy.onrender.com"


today = datetime.now().strftime('%Y-%m-%d')
print(f"📅 Target Date: {today}")
routes = [
	("LHR", "STR"),
	("STR", "LHR"),
	("FRA", "JFK"),
	("JFK", "FRA"),
	("FRA", "LHR"),
	("LHR", "FRA"),
	("FRA", "MUC"),
	("MUC", "FRA"),
	("FRA", "SIN"),
	("SIN", "FRA")
	("FRA", "DXB"),
	("DXB", "FRA")
]
serviceType = "passenger"
limit = 100

for origin, destination in routes:
	print(f"✈️ Processing Route: {origin} -> {destination}")
	ENDPOINT = f"/v1/operations/flightstatus/route/{origin}/{destination}/{today}?serviceType={serviceType}&limit=100"

	max_retries = 5
	retry_count = 0
	success = False
	response = None # Initialize so it's available outside the retry loop

	# --- RETRY LOOP ---
	while retry_count < max_retries and not success:
		try:
			response = requests.get(f"{BASE_URL}{ENDPOINT}", headers=HEADERS, timeout=30)
			if response.status_code == 200:
				success = True
			elif response.status_code in [429, 500, 502, 503, 504]:
				wait_time = (2 ** retry_count)
				print(f"⚠️ Server error {response.status_code}. Retrying in {wait_time}s... (Attempt {retry_count + 1}/{max_retries})")
				time.sleep(wait_time)
				retry_count += 1
			else:
				raise Exception(f"❌ Permanent Error: {response.status_code}")
		except requests.exceptions.RequestException as e:
			wait_time = (2 ** retry_count) + 2
			print(f"📡 Connection issue: {e}. Retrying in {wait_time}s...")
			time.sleep(wait_time)
			retry_count += 1

	# --- SUCCESS CHECK ---
	if not success:
		raise Exception(f"🚨 Failed to fetch data after {max_retries} attempts. Ingestion aborted.")

	# --- DATA PARSING ---
	data = response.json()
	records = data.get('FlightStatusResource', {}).get('Flights', {}).get('Flight', [])

	# Wrap single dictionary in a list if necessary
	if isinstance(records, dict):
		records = [records]

	if not records:
		print(f"⚠️ No flights found for {origin} -> {destination} on {today}.")
	else: #if records
		flight_count = len(records)
		final_output = {
			"FlightStatusResource": {
				"Flights": {
					"Flight": records
				}
			}
		}

		file_name = f"ops_flights_{origin}_{destination}_{today}.json"
		file_path = f"{BASE_VOLUME}/{file_name}"

		with open(file_path, "w") as f:
			# Use final_output here, NOT response.json()
			json.dump(final_output, f, indent=2)
		print(f"Saved |{flight_count}| flights for route: {origin}->{destination} to {file_name}")

end_time = time.time()
duration_mins = (end_time - start_time) / 60
print(f"\nINFO: Total Ingestion Time: {duration_mins:.2f} minutes\n")