import requests
import json
import os
import yaml
import time
from pathlib import Path
from datetime import datetime
today = datetime.now().strftime('%Y-%m-%d')


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
	BASE_VOLUME = f"{ROOT_PATH}/reference"
	os.makedirs(BASE_VOLUME, exist_ok=True)

else: # Local Testing Behaviour
	# 1. Get the directory where THIS script is saved
	script_dir = os.path.dirname(os.path.abspath(__file__))
	
	# 2. Go to the root of your project (usually one or two levels up from 'scripts/references')
	# This ensures it always stays inside 'DataIntelligenceLHIND'
	project_root = os.path.dirname(os.path.dirname(script_dir))
	
	config_path = os.path.join(project_root, "config.yaml")
	
	with open(config_path, 'r') as f:
		config = yaml.safe_load(f)
	HEADERS = {"password": config["password"]}

	# 3. Create 'outputs' inside your project folder
	ROOT_PATH = os.path.join(project_root, "outputs")
	BASE_VOLUME = os.path.join(ROOT_PATH, "reference")
	
	# This should now work without Permission Errors
	os.makedirs(BASE_VOLUME, exist_ok=True)
	print(f"💻 Local path set to: {BASE_VOLUME}")


### ACTUAL SCRIPT STARTS HERE

BASE_URL = "https://lh-proxy.onrender.com"

start_time = time.time()

all_cities = []
limit = 100
offset = 0
keep_going = True

while keep_going:
	PAGINATED_ENDPOINT = f"/v1/references/cities?limit={limit}&offset={offset}&lang=EN"
	max_retries = 5
	retry_count = 0
	success = False
	response = None # Initialize so it's available outside the retry loop

	# --- RETRY LOOP ---
	while retry_count < max_retries and not success:
		try:
			print(f"Fetching records {offset} to {offset + limit}...")
			response = requests.get(f"{BASE_URL}{PAGINATED_ENDPOINT}", headers=HEADERS, timeout=30)
			
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
		raise Exception(f"🚨 Failed to fetch offset {offset} after {max_retries} attempts. Ingestion aborted.")

	# --- DATA PARSING ---
	data = response.json()
	
	# Handle the 'error' key specifically (since Lufthansa sometimes returns 200 with an error body)
	if "error" in data or "ProcessingErrors" in data:
		print(f"🏁 End of data reached at offset {offset}.")
		keep_going = False
		continue

	records = data.get('CityResource', {}).get('Cities', {}).get('City', [])

	# Wrap single dictionary in a list if necessary
	if isinstance(records, dict):
		records = [records]

	if not records:
		keep_going = False
	else:
		all_cities.extend(records)
		print(f"📥 Added {len(records)} cities. Total: {len(all_cities)}")
		offset += limit
		time.sleep(0.4) # Respect the 3 req/sec limit


final_output = {
	"CityResource": {
		"Cities": {
			"City": all_cities
		}
	}
}

file_path = f"{BASE_VOLUME}/ref_cities_{today}.json"

with open(file_path, "w") as f:
	# Use final_output here, NOT response.json()
	json.dump(final_output, f, indent=2)

print(f"✅ Success! Saved {len(all_cities)} cities to {file_path}")
end_time = time.time()
duration_mins = (end_time - start_time) / 60
print(f"⏱️ Total Ingestion Time: {duration_mins:.2f} minutes")