import requests
import json
import os
import yaml
from pathlib import Path


# Check if we are running on Databricks
if "DATABRICKS_RUNTIME_VERSION" in os.environ:
	from pyspark.sql import SparkSession
	from pyspark.dbutils import DBUtils

	spark = SparkSession.builder.getOrCreate()
	dbutils = DBUtils(spark)
	proxy_pass = dbutils.secrets.get(scope="lufthansa_scope", key="client_secret")
	HEADERS = {"password": proxy_pass}
else:
	config_path = Path(__file__).parent / "config.yaml"
	with open(config_path, 'r') as f:
		config = yaml.safe_load(f)
	HEADERS = {"password": config["password"]}

### ACTUAL SCRIPT STARTS HERE

BASE_URL = "https://lh-proxy.onrender.com"
ENDPOINT = "/v1/references/countries?limit=5&lang=EN"

response = requests.get(f"{BASE_URL}{ENDPOINT}", headers=HEADERS)

# 3. Print the result
if response.status_code == 200:
	print(json.dumps(response.json(), indent=2))
else:
	print(f"Failed! Status: {response.status_code}")
	print(response.text)