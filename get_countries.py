import requests
import json
import os
import yaml
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
	ROOT_PATH = "Volumes/main/lufthansa/landing_zone"
	BASE_VOLUME = f"{ROOT_PATH}/reference"
	os.makedirs(BASE_VOLUME, exist_ok=True)

else: # Local Testing Behaviour

	config_path = Path(__file__).parent / "config.yaml"
	with open(config_path, 'r') as f:
		config = yaml.safe_load(f)
	HEADERS = {"password": config["password"]}

	# File Storage Local
	ROOT_PATH = "./outputs"
	BASE_VOLUME = f"{ROOT_PATH}/reference"
	os.makedirs(BASE_VOLUME, exist_ok=True)

### ACTUAL SCRIPT STARTS HERE

BASE_URL = "https://lh-proxy.onrender.com"
ENDPOINT = "/v1/references/countries?limit=5&lang=EN"

all_countries = []
limit = 100
offset = 0
keep_going = True

while keep_going:
	PAGINATED_ENDPOINT = f"/v1/references/countries?limit={limit}&offset={offset}&lang=EN"

	print(f"Fetching records {offset} to {offset + limit}...")
	response = requests.get(f"{BASE_URL}{PAGINATED_ENDPOINT}", headers=HEADERS)

	if response.status_code == 200:
		data = response.json()
		records = data.get('CountryResource', {}).get('Countries', {}).get('Country', [])

		if not records:
			keep_going = False
		else:
			all_countries.extend(records)
			offset += limit
	else:
		raise Exception (f"❌ Error during pagination at offset {offset}. API Reponse: {response.status_code}")


final_output = {
	"CountryResource": {
		"Countries": {
			"Country": all_countries
		}
	}
}

file_path = f"{BASE_VOLUME}/ref_countries.json"

with open(file_path, "w") as f:
	# Use final_output here, NOT response.json()
	json.dump(final_output, f, indent=2)

print(f"✅ Success! Saved {len(all_countries)} countries to {file_path}")