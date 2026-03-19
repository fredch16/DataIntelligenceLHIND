import os
import uuid
import yaml
import time
import requests
import json
import logging
from datetime import datetime

class LufthansaClient:
	def __init__(self, scope_name="lufthansa_app_own"):
		self.base_url = "https://api.lufthansa.com"
		self.client_id, self.client_secret = self._get_credentials(scope_name)
		self.token = self._authenticate()
		self.base_volume = self._get_base_volume()
		self._setup_logger()
		self.logger = logging.getLogger(self.__class__.__name__)
		self.headers = {
			"Authorization": f"Bearer {self.token}",
			"Accept": "application/json"
		}

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
			cid = dbutils.secrets.get(scope=scope, key="client_id")
			csec = dbutils.secrets.get(scope=scope, key="client_secret")
			return cid, csec
		else:
			project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
			config_path = os.path.join(project_root, "config.yaml")
			if not os.path.exists(config_path):
				raise FileNotFoundError(f"Could not find config at: {config_path}")
			with open(config_path, 'r') as f:
				import yaml
				config = yaml.safe_load(f)
			return config["client_id"], config["client_secret"]

	def _authenticate(self):
		auth_url = f"{self.base_url}/v1/oauth/token"

		payload = {
			"client_id": self.client_id,
			"client_secret": self.client_secret,
			"grant_type" : "client_credentials"
		}

		response = requests.post(auth_url, data=payload)

		if response.status_code != 200:
			raise Exception(f"Auth Failed: {response.status_code} - {response.text}")

		return response.json()["access_token"]

	def _get_base_volume(self):
		if "DATABRICKS_RUNTIME_VERSION" in os.environ:
			return "/Volumes/main/lufthansa/landing_zone"
		script_dir = os.path.dirname(os.path.abspath(__file__))
		project_root = os.path.dirname(script_dir)
		return os.path.join(project_root, "outputs")

	def fetch_with_retry(self, endpoint, max_retries=5):
		"""Fetch with retry logic. Returns (data, status_code) tuple."""
		retry_count = 0
		while retry_count < max_retries:
			try:
				response = requests.get(f"{self.base_url}{endpoint}", headers=self.headers, timeout=30)
				if response.status_code == 200:
					return response.json(), 200
				elif response.status_code == 404:
					try:
						self.logger.warning(f"HTTP 404 Not Found at {endpoint}")
					except Exception as log_err:
						print(f"HTTP 404 Not Found at {endpoint}")
					return None, 404
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
					return None, response.status_code
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
		return None, None

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
		When poison pill (404) detected, binary search finds it, skips it, resumes at full speed.
		Stops when records < limit or no records returned.
		Filename format: YYYYMMDD_{entity_type}_offsetN.json
		"""
		offset = 0
		limit = 100
		today_str = datetime.now().strftime("%Y%m%d")
		total_records = 0
		
		while True:
			sep = "&" if "?" in endpoint else "?"
			paginated_endpoint = f"{endpoint}{sep}limit={limit}&offset={offset}&lang=EN"
			
			try:
				self.logger.info(f"Fetching offset {offset} with limit {limit}...")
			except Exception as log_err:
				print(f"Fetching offset {offset} with limit {limit}...")
			
			data, status_code = self.fetch_with_retry(paginated_endpoint)
			
			# Check for poison pill (HTTP 404) FIRST - before checking if data is None
			if status_code == 404:
				try:
					self.logger.warning(f"Poison pill (404) detected at offset {offset}. Starting binary search to isolate...")
				except Exception as log_err:
					print(f"Poison pill (404) detected at offset {offset}. Starting binary search to isolate...")
				
				# Binary search finds the exact offset that fails AND saves all clean batches found
				result = self._find_poison_pill_offset(endpoint, offset, limit, resource_key, category, entity_type, today_str)
				poison_pill_offset = result["poison_offset"]
				clean_batches = result["clean_batches"]
				
				# Save all clean batches found during binary search
				for batch_offset, batch_limit, batch_data in clean_batches:
					try:
						filename = f"{today_str}_{entity_type}_offset{batch_offset}.json"
						meta_extras = {
							"offset": batch_offset,
							"limit": batch_limit,
							"endpoint": endpoint,
							"poison_pill_recovered": True
						}
						self.save_json(batch_data, category, entity_type, filename, metadata=meta_extras)
						
						records = self._find_records_in_response(batch_data, resource_key)
						if records:
							record_count = len(records)
							total_records += record_count
							try:
								self.logger.info(f"Saved {record_count} records from binary search batch (total: {total_records})")
							except Exception as log_err:
								print(f"Saved {record_count} records from binary search batch (total: {total_records})")
					except Exception as e:
						try:
							self.logger.error(f"Error saving binary search batch at offset {batch_offset}: {e}")
						except Exception as log_err:
							print(f"Error saving binary search batch at offset {batch_offset}: {e}")
					
					time.sleep(0.2)
				
				if poison_pill_offset is not None:
					try:
						self.logger.info(f"Poison pill isolated at offset {poison_pill_offset}. Skipping and resuming at {poison_pill_offset + 1}.")
					except Exception as log_err:
						print(f"Poison pill isolated at offset {poison_pill_offset}. Skipping and resuming at {poison_pill_offset + 1}.")
					# Skip the bad record and resume at next offset
					offset = poison_pill_offset + 1
				else:
					try:
						self.logger.warning(f"Could not isolate poison pill. Moving past offset {offset + limit}.")
					except Exception as log_err:
						print(f"Could not isolate poison pill. Moving past offset {offset + limit}.")
					offset += limit
				
				time.sleep(0.4)
				continue
			
			# Check for other failures (data is None but not 404)
			if not data:
				try:
					self.logger.warning(f"Request failed at offset {offset} (status: {status_code}). Stopping.")
				except Exception as log_err:
					print(f"Request failed at offset {offset} (status: {status_code}). Stopping.")
				break
			
			# Normal flow - no poison detected
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

	def _find_poison_pill_offset(self, endpoint, start_offset, limit, resource_key, category, entity_type, today_str):
		"""
		Binary search to find the single offset that causes HTTP 404.
		Returns dict with:
		  - poison_offset: the exact offset that fails, or None if not found
		  - clean_batches: list of (offset, limit, data) tuples for all clean batches found
		Saves clean batches as they are discovered.
		"""
		clean_batches = []
		result = self._binary_search_recursive(endpoint, start_offset, limit, clean_batches)
		return {
			"poison_offset": result,
			"clean_batches": clean_batches
		}
	
	def _binary_search_recursive(self, endpoint, current_offset, current_limit, clean_batches):
		"""Recursive helper for binary search. Accumulates clean_batches list."""
		if current_limit <= 0:
			return None
		
		sep = "&" if "?" in endpoint else "?"
		test_endpoint = f"{endpoint}{sep}limit={current_limit}&offset={current_offset}&lang=EN"
		
		try:
			self.logger.info(f"Binary search: testing offset {current_offset}, limit {current_limit}")
		except:
			print(f"Binary search: testing offset {current_offset}, limit {current_limit}")
		
		test_data, status_code = self.fetch_with_retry(test_endpoint)
		
		# If this batch is clean (200), save it to accumulate list and return None
		if status_code == 200:
			try:
				self.logger.info(f"Binary search: Found clean batch at offset {current_offset}, limit {current_limit}")
			except:
				print(f"Binary search: Found clean batch at offset {current_offset}, limit {current_limit}")
			# Add to accumulation list (with data)
			clean_batches.append((current_offset, current_limit, test_data))
			return None
		
		# If we found a 404 and limit is 1, this is the poison pill
		if status_code == 404 and current_limit == 1:
			try:
				self.logger.warning(f"Binary search: Poison pill found at offset {current_offset}")
			except:
				print(f"Binary search: Poison pill found at offset {current_offset}")
			return current_offset
		
		# If we have a 404 and limit > 1, split in half and search both sides
		if status_code == 404:
			try:
				self.logger.info(f"Binary search: 404 found, splitting batch at offset {current_offset}, limit {current_limit}")
			except:
				print(f"Binary search: 404 found, splitting batch at offset {current_offset}, limit {current_limit}")
			
			half = current_limit // 2
			# Search left half first
			left_result = self._binary_search_recursive(endpoint, current_offset, half, clean_batches)
			if left_result is not None:
				return left_result
			# If left half is clean, search right half
			right_result = self._binary_search_recursive(endpoint, current_offset + half, current_limit - half, clean_batches)
			return right_result
		else:
			# Some other error - can't proceed
			try:
				self.logger.warning(f"Binary search: Unexpected status {status_code} at offset {current_offset}")
			except:
				print(f"Binary search: Unexpected status {status_code} at offset {current_offset}")
			return None

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
			data, status_code = self.fetch_with_retry(paginated_endpoint)
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
		
		# Return a standard Lufthansa-structured dictionary with (data, status_code) tuple
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
			}, 200
		
		if "flightstatus" in endpoint:
			return {
				"FlightStatusResource": {
					"Flights": {
						"Flight": [
							{"OperatingCarrier": {"AirlineID": "LH", "FlightNumber": "400"}}
						]
					}
				}
			}, 200
		return None, 404