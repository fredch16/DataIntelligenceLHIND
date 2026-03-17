import os
import uuid
import yaml
import time
import requests
import json
import logging
from datetime import datetime

class LufthansaClient:
	def __init__(self, scope_name="lufthansa_scope"):
		self.base_url = "https://lh-proxy.onrender.com"
		self.client_secret = self._get_credentials(scope_name)
		self.base_volume = self._get_base_volume()
		self._setup_logger()
		self.logger = logging.getLogger(self.__class__.__name__)
		self.headers = {"password": self.client_secret}

	def _setup_logger(self):
		"""
		ULTRA-STABLE VERSION: Console Only.
		Removes all file-based logging to eliminate 'Illegal seek' / 'I/O' errors.
		"""
		import logging
		import sys

		# Get the root logger
		root_logger = logging.getLogger()
		
		# 1. Force remove EVERY existing handler
		while root_logger.handlers:
			root_logger.removeHandler(root_logger.handlers[0])

		# 2. Create a clean formatter
		formatter = logging.Formatter(
			"%(asctime)s | %(levelname)s | %(name)s | %(message)s", 
			datefmt="%H:%M:%S"
		)

		# 3. Use ONLY StreamHandler (Standard Output)
		# sys.stdout is much more stable in notebooks than the default stream
		console_h = logging.StreamHandler(sys.stdout)
		console_h.setFormatter(formatter)
		
		root_logger.addHandler(console_h)
		root_logger.setLevel(logging.INFO)

		# 4. Silence other noisy libraries
		logging.getLogger("urllib3").setLevel(logging.WARNING)
		logging.getLogger("requests").setLevel(logging.WARNING)
		
		self.logger = logging.getLogger(self.__class__.__name__)
		self.logger.info("Logger initialized in Ultra-Stable mode (Console only).")


	def _get_credentials(self, scope):
		if "DATABRICKS_RUNTIME_VERSION" in os.environ:
			from pyspark.sql import SparkSession
			from pyspark.dbutils import DBUtils
			spark = SparkSession.builder.getOrCreate()
			dbutils = DBUtils(spark)
			proxy_pass = dbutils.secrets.get(scope=scope, key="client_secret")
			return proxy_pass
		else:
			utils_dir = os.path.dirname(os.path.abspath(__file__))
			project_root = os.path.dirname(utils_dir)
			config_path = os.path.join(project_root, "config.yaml")
			if not os.path.exists(config_path):
				raise FileNotFoundError(f"Could not find config.yaml at: {config_path}")
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
					try:
						self.logger.warning(f"HTTP {response.status_code} Error. Retrying in {wait}s...")
					except Exception as log_err:
						print(f"Log warning failed: {log_err}")
					time.sleep(wait)
					retry_count += 1
				else:
					try:
						self.logger.error(f"Permanent Error: HTTP {response.status_code}")
					except Exception as log_err:
						print(f"Log error failed: {log_err}")
					return None
			except Exception as e:
				try:
					self.logger.error(f"Request exception at retry {retry_count}: {type(e).__name__}: {str(e)}")
				except Exception as log_err:
					print(f"Log error failed: {log_err}")
				time.sleep(2)
				retry_count += 1
		try:
			self.logger.warning(f"Max retries ({max_retries}) exceeded. Returning None.")
		except Exception as log_err:
			print(f"Log warning failed: {log_err}")
		return None

	def save_json(self, data, category, entity_type, filename, metadata=None):
		"""
		Save JSON with hybrid partitioning.
		category: 'ops' or 'ref'
		entity_type: e.g., 'flights', 'airlines', 'airports'
		Path: {base_volume}/{category}/{entity_type}/{partition}/{filename}
		Partitioning: 'ref' uses YYYY-MM (monthly), 'ops' uses YYYY-MM-DD (daily)
		"""
		today = datetime.now()
		partition = today.strftime("%Y-%m") if category == "ref" else today.strftime("%Y-%m-%d")
		full_path = f"{self.base_volume}/{category}/{entity_type}/{partition}"
		os.makedirs(full_path, exist_ok=True)

	# Construct the Envelope
		envelope = {
			"ingestion_metadata": {
				"ingested_at": today.strftime("%Y-%m-%d %H:%M:%S"),
				"batch_id": str(uuid.uuid4()), # Unique ID for every single file save
				"category": category,
				"entity": entity_type,
				"script_name": os.path.basename(__file__)
			},
			"payload": data # The raw Lufthansa JSON
		}

		# If the paginated method sends specific metadata (like offset), merge it in
		if metadata:
			envelope["ingestion_metadata"].update(metadata)

		target_file = f"{full_path}/{filename}"
		with open(target_file, "w") as f:
			json.dump(envelope, f, indent=2)
		try:
			self.logger.info(f"Saved to {target_file}")
		except Exception as log_err:
			print(f"Saved to {target_file}")

	def ingest_paginated(self, endpoint, resource_key, category, entity_type):
		"""
		High-volume ingestion: saves each API response as separate file.
		Preserves raw metadata and link blocks for downstream processing.
		Stops when records < limit or no records returned.
		Filename format: YYYYMMDD_{entity_type}_offsetN.json
		"""
		offset = 0
		limit = 100
		today_str = datetime.now().strftime("%Y%m%d")
		total_records = 0
		while True:
			# Handle poison pill records for airports at specific indices
			requests_to_make = []
			if entity_type == "airports":
				if offset == 1500:
					requests_to_make = [(1500, 51), (1552, 48)]
					next_offset = 1600
				elif offset == 3500:
					requests_to_make = [(3500, 88), (3589, 12)]
					next_offset = 3600
			
			if requests_to_make:
				for current_offset, current_limit in requests_to_make:
					sep = "&" if "?" in endpoint else "?"
					paginated_endpoint = f"{endpoint}{sep}limit={current_limit}&offset={current_offset}&lang=EN"
					try:
						self.logger.info(f"Fetching offset {current_offset} with limit {current_limit} (poison pill skip)...")
					except Exception as log_err:
						print(f"Fetching offset {current_offset} with limit {current_limit} (poison pill skip)...")
					data = self.fetch_with_retry(paginated_endpoint)
					if not data:
						try:
							self.logger.warning(f"Request failed at offset {current_offset}. Stopping.")
						except Exception as log_err:
							print(f"Request failed at offset {current_offset}. Stopping.")
						break
					try:
						filename = f"{today_str}_{entity_type}_offset{current_offset}.json"
						meta_extras = {
							"offset": current_offset,
							"limit": current_limit,
							"endpoint": endpoint,
							"poison_pill_skip": True
						}
						self.save_json(data, category, entity_type, filename, metadata=meta_extras)
						records = self._find_records_in_response(data, resource_key)
						if records:
							record_count = len(records)
							total_records += record_count
							try:
								self.logger.info(f"Saved {record_count} records (total: {total_records})")
							except Exception as log_err:
								print(f"Saved {record_count} records (total: {total_records})")
						time.sleep(0.4)
					except Exception as e:
						try:
							self.logger.error(f"Error at offset {current_offset}: {e}")
						except Exception as log_err:
							print(f"Error at offset {current_offset}: {e}")
						break
				offset = next_offset
				time.sleep(0.4)
				continue
			
			# Normal pagination flow
			sep = "&" if "?" in endpoint else "?"
			paginated_endpoint = f"{endpoint}{sep}limit={limit}&offset={offset}&lang=EN"
			try:
				self.logger.info(f"Fetching offset {offset}...")
			except Exception as log_err:
				print(f"Fetching offset {offset}...")
			data = self.fetch_with_retry(paginated_endpoint)
			if not data:
				try:
					self.logger.warning(f"Request failed at offset {offset}. Stopping.")
				except Exception as log_err:
					print(f"Request failed at offset {offset}. Stopping.")
				break
			try:
				filename = f"{today_str}_{entity_type}_offset{offset}.json"
				meta_extras = {
					"offset": offset,
					"limit": limit,
					"endpoint": endpoint
				}
				self.save_json(data, category, entity_type, filename, metadata=meta_extras)
				records = self._find_records_in_response(data, resource_key)
				if not records:
					try:
						self.logger.info("No records found. Ingestion complete.")
					except Exception as log_err:
						print("No records found. Ingestion complete.")
					break
				record_count = len(records)
				total_records += record_count
				try:
					self.logger.info(f"Saved {record_count} records (total: {total_records})")
				except Exception as log_err:
					print(f"Saved {record_count} records (total: {total_records})")
				if record_count < limit:
					try:
						self.logger.info(f"Final batch has {record_count} records (< {limit}). Stopping.")
					except Exception as log_err:
						print(f"Final batch has {record_count} records (< {limit}). Stopping.")
					break
				offset += limit
				time.sleep(0.4)
			except Exception as e:
				try:
					self.logger.error(f"Error at offset {offset}: {e}")
				except Exception as log_err:
					print(f"Error at offset {offset}: {e}")
				break
		try:
			self.logger.info(f"Total records ingested: {total_records}")
		except Exception as log_err:
			print(f"Total records ingested: {total_records}")

	def _find_records_in_response(self, data, resource_key):
		"""Auto-detect records in Lufthansa response structure."""
		resource_data = data.get(resource_key, {})
		if not isinstance(resource_data, dict):
			return []
		for val1 in resource_data.values():
			if isinstance(val1, list):
				return val1
			if isinstance(val1, dict):
				for val2 in val1.values():
					if isinstance(val2, list):
						return val2
					elif isinstance(val2, dict):
						return [val2]
		return []

	def fetch_all_pages(self, endpoint, resource_key, nested_keys):
		"""
		Legacy pagination handler for Reference data (backward compatibility).
		Aggregates all records into single response structure.
		"""
		all_records = []
		offset = 0 
		limit = 100
		while True:
			sep = "&" if "?" in endpoint else "?"
			paginated_endpoint = f"{endpoint}{sep}limit={limit}&offset={offset}"
			self.logger.info(f"Fetching offset {offset}...")
			data = self.fetch_with_retry(paginated_endpoint)
			if not data:
				break
			try:
				records = data.get(resource_key, {})
				for key in nested_keys:
					records = records.get(key, {})
				if isinstance(records, dict):
					records = [records]
				if not records:
					break
				if len(records) < limit:
					all_records.extend(records)
					self.logger.info(f"Added final {len(records)} records. Total: {len(all_records)}")
					break
				all_records.extend(records)
				self.logger.info(f"Added {len(records)} records. Total: {len(all_records)}")
				offset += limit
				time.sleep(0.4)
			except Exception as e:
				self.logger.error(f"Parsing error: {e}")
				break
		structure = all_records
		for key in reversed(nested_keys):
			structure = {key: structure}
		return {resource_key: structure}

class MockLufthansaClient(LufthansaClient):
	def fetch_with_retry(self, endpoint, max_retries=5):
		self.logger.info("MOCK MODE: Returning hardcoded sample data")
		
		# Return a standard Lufthansa-structured dictionary
		if "countries" in endpoint:
			return {
				"CountryResource": {
					"Countries": {
						"Country": [
							{"CountryCode": "DE", "Names": {"Name": {"$": "Germany"}}},
							{"CountryCode": "US", "Names": {"Name": {"$": "United States"}}}
						]
					}
				}
			}
		
		if "flightstatus" in endpoint:
			return {
				"FlightStatusResource": {
					"Flights": {
						"Flight": [
							{"OperatingCarrier": {"AirlineID": "LH", "FlightNumber": "400"}}
						]
					}
				}
			}
		return None