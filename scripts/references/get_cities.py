import requests
import json
import os
import yaml
import time
from pathlib import Path


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


	## 
	max_retries = 5
	retry_count = 0
	success = False

	while retry_count < max_retries and not success:
		try:
			print(f"Fetching records {offset} to {offset + limit}...")
			response = requests.get(f"{BASE_URL}{PAGINATED_ENDPOINT}", headers=HEADERS, timeout=30)
			
			if response.status_code == 200:
				success = True
			elif response.status_code in [429, 500, 502, 503, 504]:
				# Exponential backoff: 2^retry_count + random jitter
				wait_time = (2 ** retry_count)
				print(f"⚠️ Server error {response.status_code}. Retrying in {wait_time:.2f}s... (Attempt {retry_count + 1}/{max_retries})")
				time.sleep(wait_time)
				retry_count += 1
			else:
				# If it's a 404 or 401, retrying won't help
				raise Exception(f"❌ Permanent Error: {response.status_code}")
		
		except requests.exceptions.RequestException as e:
			wait_time = (2 ** retry_count) + 2
			print(f"📡 Connection issue: {e}. Retrying in {wait_time}s...")
			time.sleep(wait_time)
			retry_count += 1

	if not success:
		raise Exception(f"🚨 Failed to fetch offset {offset} after {max_retries} attempts. Ingestion aborted to prevent partial data.")

final_output = {
	"CityResource": {
		"Cities": {
			"City": all_cities
		}
	}
}

file_path = f"{BASE_VOLUME}/ref_cities.json"

with open(file_path, "w") as f:
	# Use final_output here, NOT response.json()
	json.dump(final_output, f, indent=2)

print(f"✅ Success! Saved {len(all_cities)} cities to {file_path}")
end_time = time.time()
duration_mins = (end_time - start_time) / 60
print(f"⏱️ Total Ingestion Time: {duration_mins:.2f} minutes")