import os
import yaml
import time
import requests
import json

class LufthansaClient:
	def __init__(self, scope_name="lufthansa_scope"):
		self.base_url = "https://lh-proxy.onrender.com"

		self.client_secret = self._get_credentials(scope_name)

		self.base_volume = self._get_base_volume()

		self.headers = {"password": self.client_secret}

	def _get_credentials(self, scope):
		if "DATABRICKS_RUNTIME_VERSION" in os.environ:
			from pyspark.sql import SparkSession
			from pyspark.dbutils import DBUtils

			spark = SparkSession.builder.getOrCreate()
			dbutils = DBUtils(spark)
			# Best practice: use the 'scope' argument passed to the function
			proxy_pass = dbutils.secrets.get(scope=scope, key="client_secret")
			return proxy_pass
		else:
			# 1. Get directory of helpers.py (e.g., .../DataIntelligenceLHIND/utils)
			utils_dir = os.path.dirname(os.path.abspath(__file__))
			
			# 2. Get Project Root (e.g., .../DataIntelligenceLHIND)
			project_root = os.path.dirname(utils_dir)
			
			# 3. Join with config.yaml
			config_path = os.path.join(project_root, "config.yaml")
			
			if not os.path.exists(config_path):
				raise FileNotFoundError(f"❌ Could not find config.yaml at: {config_path}")
				
			with open(config_path, 'r') as f:
				config = yaml.safe_load(f)
			return config["password"]
		
	def _get_base_volume(self):
		if "DATABRICKS_RUNTIME_VERSION" in os.environ:
			return "/Volumes/main/lufthansa/landing_zone"
		script_dir = os.path.dirname(os.path.abspath(__file__))
		project_root = os.path.dirname(script_dir)
		return os.path.join(project_root, "outputs")


	def fetch_with_retry(self, endpoint, max_retries=5):
			retry_count = 0
			while retry_count < max_retries:
				try:
					response = requests.get(f"{self.base_url}{endpoint}", headers=self.headers, timeout=30)
					if response.status_code == 200:
						return response.json()
					elif response.status_code in [429, 500, 502, 503, 504]:
						wait = 2 ** retry_count
						print(f"⚠️ {response.status_code} Error. Retrying in {wait}s...")
						time.sleep(wait)
						retry_count += 1
					else:
						print(f"❌ Permanent Error: {response.status_code}")
						return None
				except Exception as e:
					time.sleep(2)
					retry_count += 1
			return None

	def save_json(self, data, subfolder, filename):
			full_path = f"{self.base_volume}/{subfolder}"
			os.makedirs(full_path, exist_ok=True)
			
			target_file = f"{full_path}/{filename}"
			with open(target_file, "w") as f:
				json.dump(data, f, indent=2)
			print(f"💾 Saved to {target_file}")

	def fetch_all_pages(self, endpoint, resource_key, nested_keys):
		"""
		Generic pagination handler for Reference data.
		resource_key: e.g., 'CountryResource'
		nested_keys: list of keys to get to the data, e.g., ['Countries', 'Country']
		"""
		all_records = []
		offset = 0
		limit = 100
		keep_going = True

		while keep_going:
			# Build the paginated URL
			sep = "&" if "?" in endpoint else "?"
			paginated_endpoint = f"{endpoint}{sep}limit={limit}&offset={offset}"
			print(f"📄 Fetching offset {offset}...")
			
			data = self.fetch_with_retry(paginated_endpoint)
			
			if not data:
				break

			# Navigate the nested JSON using the provided keys
			# e.g., data['CountryResource']['Countries']['Country']
			try:
				records = data.get(resource_key, {})
				for key in nested_keys:
					records = records.get(key, {})
				
				if isinstance(records, dict):
					records = [records]

	# ... inside while keep_going loop ...
				if not records:
					keep_going = False
				# ADD THIS CHECK: 
				# If we requested 100 but got back something tiny (like 1 or 0), 
				# we've reached the end of the line.
				elif len(records) < limit:
					all_records.extend(records)
					print(f"📥 Added final {len(records)} records. Total: {len(all_records)}")
					keep_going = False
				else:
					all_records.extend(records)
					print(f"📥 Added {len(records)} records. Total: {len(all_records)}")
					offset += limit
					time.sleep(0.4)
			except Exception as e:
				print(f"⚠️ Parsing error: {e}")
				keep_going = False

		# Return in the original Lufthansa structure
		return {resource_key: self._wrap_records(nested_keys, all_records)}

	def _wrap_records(self, keys, records):
		"""Helper to rebuild the nested structure for saving."""
		# This reverses ['Countries', 'Country'] into {'Countries': {'Country': records}}
		structure = records
		for key in reversed(keys):
			structure = {key: structure}
		return structure