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
	ROOT_PATH = "/Volumes/main/lufthansa/landing_zone"
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

response = requests.get(f"{BASE_URL}{ENDPOINT}", headers=HEADERS)

if response.status_code == 200:
	file_path = f"{BASE_VOLUME}/ref_countries.json"

	with open(file_path, "w") as f:
		json.dump(response.json(), f, indent=2)
	print(f"✅ Success! Saved to {file_path}")
else:
	print(f"❌ Failed! Status: {response.status_code}")